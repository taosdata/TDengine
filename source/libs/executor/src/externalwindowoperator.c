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

#include "cmdnodes.h"
#include "executorInt.h"
#include "filter.h"
#include "operator.h"
#include "querytask.h"
#include "stream.h"
#include "tdatablock.h"
#include "windowfunc.h"

#define EXT_WIN_RES_ROWS_ALLOC_SIZE 10
#define EXT_WIN_BROADCAST_INMEM_BYTES (10 * 1024 * 1024)
#define EXT_WIN_TYPE_STR(_isMAExtW) ((_isMAExtW) ? "mergeAligned" : "")

typedef struct SBlockList {
  const SSDataBlock* pSrcBlock;
  SList*             pBlocks;
  int32_t            blockRowNumThreshold;
} SBlockList;


typedef int32_t (*extWinGetWinFp)(SOperatorInfo*, int64_t*, int32_t*, SDataBlockInfo*, SExtWinTimeWindow**, int32_t*);

typedef struct SExtWinPayloadSum {
  uint64_t low;
  uint64_t high;
} SExtWinPayloadSum;

#if __STDC_VERSION__ >= 201112LL
static_assert(sizeof(size_t) <= sizeof(uint64_t), "broadcast payload sum requires at most a 64-bit address space");
#endif

typedef struct SExtWindowStat {
  int64_t resBlockCreated;
  int64_t resBlockDestroyed;
  int64_t resBlockRecycled;
  int64_t resBlockReused;
  int64_t resBlockAppend;
  int64_t           broadcastScanBlocks;
  int64_t           broadcastScanRows;
  int64_t           broadcastReplayBlocks;
  int64_t           broadcastReplayRows;
  int64_t           broadcastStoreBudget;
  SExtWinPayloadSum broadcastPayloadCurrent;
  int64_t           broadcastPayloadPeak;
  int64_t           broadcastFlushBytes;
  int64_t           broadcastLoadBytes;
#if defined(BUILD_TEST)
  int64_t windowConditionEvals;
#endif
} SExtWindowStat;

typedef struct SExtWinBroadcastState {
  SWindowInputStore* pStore;
  int32_t            replayWinIdx;
  int32_t            replayPage;
  bool               inputLoaded;
  SArray*            pPageMeta;  // SArray<SExtWinBroadcastPageMeta>, one entry per store page
  int64_t            appendedSerializedBytes;
  SExtWindowStat*    pStat;
} SExtWinBroadcastState;

typedef struct SExtWinBroadcastPageMeta {
  SBlockID    id;
  STimeWindow window;
  int64_t     version;
  int16_t     dataLoad;
  uint8_t     scanFlag;
} SExtWinBroadcastPageMeta;

#if defined(BUILD_TEST)
typedef struct SExtWinBroadcastReplayObservation {
  int32_t     pageIndex;
  SBlockID    id;
  STimeWindow window;
  int64_t     version;
  int16_t     dataLoad;
  uint8_t     scanFlag;
} SExtWinBroadcastReplayObservation;
#endif

typedef struct SExtWinCalcGrpCtx {
  uint64_t           groupId;
  SArray*            pWins;           // SArray<SExtWinTimeWindow>
  int32_t            curIdx; // for pesudo func calculation

  bool               blkWinStartSet;
  int32_t            blkWinStartIdx;
  int32_t            blkWinIdx;
  int32_t            blkRowStartIdx;
  int32_t            blkScanFlag;

  SArray*            outWinBufIdx;
  int32_t            outWinTotalNum;      // agg: total output win num
  int32_t            outWinNum;           // already output win num
  int32_t            outWinIdx;           // current output win idx
  int32_t            outWinLastIdx;
  
  int32_t            lastWinIdx;
  int64_t            lastSKey;
  int64_t            lastEKey;
  int32_t            lastWinId;

  // Cached representative row with source data, used for group-key fallback in fill
  // paths. Set on first search, reused for all empty windows in this group.
  bool               anyRowCached;
  SResultRow*        pAnyRow;

  SExtWinBroadcastState broadcast;
  SArray*               pWindowConditionResults;  // SArray<int8_t>: 0 skip, 1 pass
  bool                  windowDecisionsReady;
  int32_t               settledThroughWinIdx;
} SExtWinCalcGrpCtx;

typedef struct SExtWinTrigGrpCtx {
  SExtWinCalcGrpCtx* pCCtx;
  int32_t            lastCtxIter;
  void*              pLastCGrpIter;

  SSHashObj* pCGCtxs;  // calc group id => operator-owned SExtWinCalcGrpCtx*
} SExtWinTrigGrpCtx;


typedef struct SExtWinResultRows {
  int32_t             resRowsSize;
  int32_t             resRowsIdx;
  int32_t             resRowSize;
  int32_t             resRowIdx;
  int32_t             rowSizeBytes;
  int64_t             resRowAllcNum;
  SResultRow**        pResultRows;
} SExtWinResultRows;

typedef struct SExternalWindowOperator {
  SOptrBasicInfo     binfo;
  SExprSupp          scalarSupp;
  SExprSupp          projSupp;
  int32_t            primaryTsIndex;
  EExtWinMode        mode;
  EFillMode          fillMode;
  bool               multiTableMode;
  bool               inputHasOrder;
  bool               needGroupSort;
  bool               extWinSplit;
  bool               calcWithPartition;
  bool               matchPartitionBy;
  EExtWinInputMode   inputMode;
  SNode*             pWindowConditions;  // borrowed from physical node
  SArray*            pPseudoColInfo;  
  STimeRangeNode*    timeRangeExpr;
  
  extWinGetWinFp     getWinFp;

  uint64_t           lastTGrpId;
  uint64_t           lastCGrpId;
  SExtWinTrigGrpCtx* pTGrpCtx;
  SSHashObj*          pGroupRegistry;  // trigger group id => operator-owned SExtWinTrigGrpCtx*
  SWindowInputPool*   pBroadcastPool;
  SArray*             pBroadcastContextViews;  // SArray<SStreamCalcContextGroupView>
  SDiskbasedBufStatis broadcastCollectedStatis;
  SArray*            pGrpIds;       // single calc or trig group ids
  SArray*            pCTGrpIds;
  
  // for project&indefRows
  SList*             pFreeBlocks;    // SList<SSDatablock*+SAarray*>
  SArray*            pOutputBlocks;  // SArray<SList*>, for each window, we have a list of blocks
  SListNode*         pLastBlkNode; 
  SSDataBlock*       pTmpBlock;
  SSDataBlock*       pProjTmpBlock;  // reusable tmp block for extWinApplyAggPostProjection
  
  // for agg
  SAggSupporter      aggSup;
  STimeWindowAggSupp twAggSup;

  SExtWinResultRows  resultRows;

  // for output
  SSTriggerGroupCalcInfo* pLastOutput;     // non-stream compatibility path
  int32_t                 lastOutputIter;  // non-stream compatibility path
  int32_t                 lastGrpIdx;    // index in pGrpIds or pCTGrpIds
  int32_t                 groupOrderIdx;
  uint64_t                activeBaseGId;
  bool                    groupEogPending;
  SSHashObj*              pFinalizedGrpIds;
  bool                    groupScanDone;
  bool                    groupOutputComplete;
  bool                    activeGroupPublished;
  bool                    boundaryReady;
  bool                    nonAggInputDone;
  bool                    mergeInputDone;
  bool                    mergeGroupComplete;

  // Back-pointer so the row-state cleanup hook can reach pOperator->exprSupp; the hook receives
  // only pOperator->info.
  SOperatorInfo*     pOperator;

  int32_t            outWinIdx;
  int32_t            resWinIdx;        // for result win allocation
  SSDataBlock*       pEmptyInputBlock;
  bool               hasCountFunc;
  SNodeList*         pFillExprs;
  SNode*             pFillValues;
  SColMatchInfo      fillMatchInfo;
  SExtWindowStat     stat;
  SArray*            pWinRowIdx;
  bool               isMergeAlignedExtW;

  // for vtable window query
  bool               isDynWindow;
  SArray*            pNullOutputSlots;  // SArray<int32_t>, reset for every dynamic execution unit
  int32_t            orgTableVgId;
  tb_uid_t           orgTableUid;
  STimeWindow        orgTableTimeRange;

#if defined(BUILD_TEST)
  bool    testAliasClearedOnOpen;
  SArray* pBroadcastReplayObservations;  // SArray<SExtWinBroadcastReplayObservation>
#endif
} SExternalWindowOperator;

static int32_t extWinCreateEmptyInputBlock(SOperatorInfo* pOperator, SSDataBlock** ppBlock);

typedef struct SExtWinBlockSnapshot {
  SList*       pList;
  SListNode*   pNode;
  SListNode*   pPrev;
  SSDataBlock* pBlock;
  SArray*      pIdx;
  int64_t      blockRows;
  uint32_t     blockCapacity;
} SExtWinBlockSnapshot;

static int32_t extWinTakeReusableBlock(SList* pFreeBlocks, SList* pTargetBlocks, int32_t rows, SSDataBlock** ppBlock,
                                       SArray** ppIdx) {
  SListNode* pNode = listHead(pFreeBlocks);
  if (NULL == pNode) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SSDataBlock* pBlock = *(SSDataBlock**)pNode->data;
  int32_t      code = blockDataEnsureCapacity(pBlock, TMAX(rows, 4096));
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  pNode = tdListPopHead(pFreeBlocks);
  tdListAppendNode(pTargetBlocks, pNode);
  *ppBlock = pBlock;
  *ppIdx = *(SArray**)((SSDataBlock**)pNode->data + 1);
  return TSDB_CODE_SUCCESS;
}

static bool extWinRangesOverlap(const void* pLeft, uint64_t leftLen, const void* pRight, uint64_t rightLen) {
  if (NULL == pLeft || 0 == leftLen || NULL == pRight || 0 == rightLen) {
    return false;
  }

  uintptr_t left = (uintptr_t)pLeft;
  uintptr_t right = (uintptr_t)pRight;
  return (left <= right) ? ((uint64_t)(right - left) < leftLen) : ((uint64_t)(left - right) < rightLen);
}

static bool extWinBlockNodeInvariantHolds(SList* pList, SListNode* pNode, SListNode* pExpectedPrev, SSDataBlock* pBlock,
                                          SArray* pIdx, int32_t appendRows, int32_t* pOverlapCol) {
  if (NULL != pOverlapCol) {
    *pOverlapCol = -1;
  }

  if (NULL == pList || NULL == pNode || NULL == pBlock || NULL == pIdx || listTail(pList) != pNode ||
      pNode->dl_prev_ != pExpectedPrev || NULL != pNode->dl_next_ || *(SSDataBlock**)pNode->data != pBlock ||
      *(SArray**)((SSDataBlock**)pNode->data + 1) != pIdx || NULL == pBlock->pDataBlock || pBlock->info.rows < 0 ||
      appendRows < 0 || pBlock->info.rows > pBlock->info.capacity ||
      appendRows > pBlock->info.capacity - pBlock->info.rows) {
    return false;
  }

  uint64_t nodeBytes = sizeof(SListNode) + (uint64_t)pList->eleSize;
  int32_t  numCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (NULL == pCol) {
      return false;
    }

    uint64_t dataBytes =
        IS_VAR_DATA_TYPE(pCol->info.type) ? pCol->varmeta.allocLen : (uint64_t)pBlock->info.capacity * pCol->info.bytes;
    void*    pAuxData = IS_VAR_DATA_TYPE(pCol->info.type) ? (void*)pCol->varmeta.offset : pCol->nullbitmap;
    uint64_t auxBytes = IS_VAR_DATA_TYPE(pCol->info.type) ? (uint64_t)pBlock->info.capacity * sizeof(int32_t)
                                                          : BitmapLen(pBlock->info.capacity);
    if (extWinRangesOverlap(pNode, nodeBytes, pCol->pData, dataBytes) ||
        extWinRangesOverlap(pNode, nodeBytes, pAuxData, auxBytes)) {
      if (NULL != pOverlapCol) {
        *pOverlapCol = i;
      }
      return false;
    }
  }

  return true;
}

static void extWinLogBlockInvariantFailure(SOperatorInfo* pOperator, const SExtWinBlockSnapshot* pSnapshot,
                                           const SExtWinTimeWindow* pWin, int32_t startPos, int32_t rows,
                                           int32_t overlapCol, const char* pStage) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SExtWinCalcGrpCtx*       pCCtx = pExtW->pTGrpCtx == NULL ? NULL : pExtW->pTGrpCtx->pCCtx;
  SListNode*               pNode = pSnapshot->pNode;
  SListNode*               pPrev = NULL == pNode ? NULL : pNode->dl_prev_;
  SListNode*               pNext = NULL == pNode ? NULL : pNode->dl_next_;
  SSDataBlock*             pNodeBlock = NULL == pNode ? NULL : *(SSDataBlock**)pNode->data;
  SArray*                  pNodeIdx = NULL == pNode ? NULL : *(SArray**)((SSDataBlock**)pNode->data + 1);

  qError("%s external window block invariant failed at %s, win:[%" PRId64 ",%" PRId64
         "], resWinIdx:%d, outWinIdx:%d, lastWinIdx:%d, blkWinStartIdx:%d, blkWinIdx:%d, startPos:%d, "
         "inputRows:%d, list:%p, listBlocks:%d, freeBlocks:%d, node:%p, prev:%p, expectedPrev:%p, next:%p, "
         "block:%p, nodeBlock:%p, idx:%p, nodeIdx:%p, beforeRows:%" PRId64 ", beforeCapacity:%u, rows:%" PRId64
         ", capacity:%u, overlapCol:%d, created:%" PRId64 ", destroyed:%" PRId64 ", recycled:%" PRId64
         ", reused:%" PRId64 ", appended:%" PRId64,
         pOperator->pTaskInfo->id.str, pStage, pWin->tw.skey, pWin->tw.ekey, pWin->resWinIdx, pExtW->outWinIdx,
         pCCtx == NULL ? -1 : pCCtx->lastWinIdx, pCCtx == NULL ? -1 : pCCtx->blkWinStartIdx,
         pCCtx == NULL ? -1 : pCCtx->blkWinIdx, startPos, rows, pSnapshot->pList,
         NULL == pSnapshot->pList ? -1 : listNEles(pSnapshot->pList), listNEles(pExtW->pFreeBlocks), pNode, pPrev,
         pSnapshot->pPrev, pNext, pSnapshot->pBlock, pNodeBlock, pSnapshot->pIdx, pNodeIdx, pSnapshot->blockRows,
         pSnapshot->blockCapacity, pSnapshot->pBlock->info.rows, pSnapshot->pBlock->info.capacity, overlapCol,
         pExtW->stat.resBlockCreated, pExtW->stat.resBlockDestroyed, pExtW->stat.resBlockRecycled,
         pExtW->stat.resBlockReused, pExtW->stat.resBlockAppend);
}

#if defined(BUILD_TEST)
int32_t extWinTestTakeReusableBlock(SList* pFreeBlocks, SList* pTargetBlocks, int32_t rows, SSDataBlock** ppBlock,
                                    SArray** ppIdx) {
  return extWinTakeReusableBlock(pFreeBlocks, pTargetBlocks, rows, ppBlock, ppIdx);
}

bool extWinTestBlockNodeInvariantHolds(SList* pList, SListNode* pNode, SListNode* pExpectedPrev, SSDataBlock* pBlock,
                                       SArray* pIdx, int32_t appendRows, int32_t* pOverlapCol) {
  return extWinBlockNodeInvariantHolds(pList, pNode, pExpectedPrev, pBlock, pIdx, appendRows, pOverlapCol);
}
#endif

static char* extWinModeStr(EExtWinMode mode) {
  switch (mode) {
    case EEXT_MODE_SCALAR:
      return "scalar";
    case EEXT_MODE_AGG:
      return "agg";
    case EEXT_MODE_INDEFR_FUNC:
      return "indefRows";
    default:
      break;
  }

  return "unknown";
}


static int extWinGrpIdCompare(const void* p1, const void* p2) {
  uint64_t* gId1 = (uint64_t*)p1;
  uint64_t* gId2 = (uint64_t*)p2;

  if (*gId1 == *gId2) {
    return 0;
  }
  
  return (*gId1) < (*gId2) ? -1 : 1;
}

static int extWinCalcTriggerGrpIdCompare(const void* p1, const void* p2) {
  const uint64_t* pFirst = p1;
  const uint64_t* pSecond = p2;

  if (pFirst[1] != pSecond[1]) {
    return pFirst[1] < pSecond[1] ? -1 : 1;
  }
  if (pFirst[0] == pSecond[0]) {
    return 0;
  }
  return pFirst[0] < pSecond[0] ? -1 : 1;
}

static void extWinCollectBroadcastPoolStatis(SExternalWindowOperator* pExtW) {
  if (pExtW == NULL || pExtW->pBroadcastPool == NULL) {
    return;
  }

  SDiskbasedBufStatis current = winInputPoolGetStatis(pExtW->pBroadcastPool);
  pExtW->stat.broadcastFlushBytes += current.flushBytes - pExtW->broadcastCollectedStatis.flushBytes;
  pExtW->stat.broadcastLoadBytes += current.loadBytes - pExtW->broadcastCollectedStatis.loadBytes;
  pExtW->broadcastCollectedStatis = current;
}

static int64_t extWinAddPositiveI64Saturating(int64_t lhs, int64_t rhs) {
  if (rhs > 0 && lhs > INT64_MAX - rhs) {
    return INT64_MAX;
  }
  return lhs + rhs;
}

static int64_t extWinMultiplyPositiveI64Saturating(int64_t lhs, int64_t rhs) {
  if (lhs <= 0 || rhs <= 0) {
    return 0;
  }
  return lhs > INT64_MAX / rhs ? INT64_MAX : lhs * rhs;
}

/*
 * Each live contribution is at most INT64_MAX and requires a separately allocated calc-group context. On the
 * supported at-most-64-bit address spaces, fewer than 2^64 contexts can be live, so their sum is below 2^127 and
 * fits exactly in these two limbs.
 */
static void extWinPayloadSumAdd(SExtWinPayloadSum* pSum, int64_t value) {
  if (value <= 0) {
    return;
  }
  uint64_t oldLow = pSum->low;
  pSum->low += (uint64_t)value;
  pSum->high += pSum->low < oldLow;
}

static void extWinPayloadSumSubtract(SExtWinPayloadSum* pSum, int64_t value) {
  if (value <= 0) {
    return;
  }
  uint64_t unsignedValue = (uint64_t)value;
  uint64_t borrow = pSum->low < unsignedValue;
  pSum->low -= unsignedValue;
  pSum->high -= borrow;
}

static int64_t extWinPayloadSumClamp(const SExtWinPayloadSum* pSum) {
  return pSum->high != 0 || pSum->low > INT64_MAX ? INT64_MAX : (int64_t)pSum->low;
}

static void extWinAccountBroadcastPayloadAppend(SExtWinBroadcastState* pState, int64_t serializedBytes) {
  pState->appendedSerializedBytes = extWinAddPositiveI64Saturating(pState->appendedSerializedBytes, serializedBytes);
  if (pState->pStat != NULL) {
    extWinPayloadSumAdd(&pState->pStat->broadcastPayloadCurrent, serializedBytes);
    int64_t current = extWinPayloadSumClamp(&pState->pStat->broadcastPayloadCurrent);
    int64_t capped = TMIN(current, pState->pStat->broadcastStoreBudget);
    pState->pStat->broadcastPayloadPeak = TMAX(pState->pStat->broadcastPayloadPeak, capped);
  }
}

static void extWinDestroyBroadcastState(SExtWinBroadcastState* pState) {
  if (pState == NULL) {
    return;
  }

  if (pState->pStat != NULL) {
    extWinPayloadSumSubtract(&pState->pStat->broadcastPayloadCurrent, pState->appendedSerializedBytes);
  }
  winInputStoreDestroy(pState->pStore);
  taosArrayDestroy(pState->pPageMeta);
  memset(pState, 0, sizeof(*pState));
}

static void extWinDestroyCGrpCtx(void* param) {
  if (NULL == param) {
    return;
  }

  SExtWinCalcGrpCtx* pCtx = (SExtWinCalcGrpCtx*)param;

  extWinDestroyBroadcastState(&pCtx->broadcast);
  taosArrayDestroy(pCtx->pWindowConditionResults);
  taosArrayDestroy(pCtx->pWins);
}

static void extWinDestroyTGrpCtx(void* param) {
  if (NULL == param) {
    return;
  }

  SExtWinTrigGrpCtx* pCtx = (SExtWinTrigGrpCtx*)param;

  int32_t             iter = 0;
  SExtWinCalcGrpCtx** ppCCtx = tSimpleHashIterate(pCtx->pCGCtxs, NULL, &iter);
  while (ppCCtx != NULL) {
    extWinDestroyCGrpCtx(*ppCCtx);
    taosMemoryFree(*ppCCtx);
    ppCCtx = tSimpleHashIterate(pCtx->pCGCtxs, ppCCtx, &iter);
  }
  tSimpleHashCleanup(pCtx->pCGCtxs);
}

void qCleanupExternalWindowGroupCtx(SSHashObj* pGroupCalcInfos) {
  if (pGroupCalcInfos == NULL) {
    return;
  }

  int32_t                 iter = 0;
  SSTriggerGroupCalcInfo* pGroup = tSimpleHashIterate(pGroupCalcInfos, NULL, &iter);
  while (pGroup != NULL) {
    if (pGroup->pRunnerGrpCtx != NULL) {
      qError("external-window runner group context must remain NULL");
    }
    pGroup = tSimpleHashIterate(pGroupCalcInfos, pGroup, &iter);
  }
}

static void extWinClearGroupRegistry(SExternalWindowOperator* pExtW) {
  if (pExtW == NULL) {
    return;
  }

  extWinCollectBroadcastPoolStatis(pExtW);

  int32_t             iter = 0;
  SExtWinTrigGrpCtx** ppTCtx = tSimpleHashIterate(pExtW->pGroupRegistry, NULL, &iter);
  while (ppTCtx != NULL) {
    extWinDestroyTGrpCtx(*ppTCtx);
    taosMemoryFree(*ppTCtx);
    ppTCtx = tSimpleHashIterate(pExtW->pGroupRegistry, ppTCtx, &iter);
  }
  tSimpleHashCleanup(pExtW->pGroupRegistry);
  winInputPoolDestroy(pExtW->pBroadcastPool);
  tDestroyStreamCalcContextGroupViews(&pExtW->pBroadcastContextViews);
  pExtW->pBroadcastPool = NULL;
  memset(&pExtW->broadcastCollectedStatis, 0, sizeof(pExtW->broadcastCollectedStatis));
  pExtW->pGroupRegistry = NULL;
  pExtW->pTGrpCtx = NULL;
  pExtW->lastTGrpId = 0;
  pExtW->lastCGrpId = 0;
}

static SExtWinCalcGrpCtx* extWinLookupCalcGroup(SExtWinTrigGrpCtx* pTCtx, uint64_t calcGroupId) {
  if (pTCtx == NULL) {
    return NULL;
  }
  SExtWinCalcGrpCtx** ppCCtx = tSimpleHashGet(pTCtx->pCGCtxs, &calcGroupId, sizeof(calcGroupId));
  return ppCCtx == NULL ? NULL : *ppCCtx;
}

static SExtWinTrigGrpCtx* extWinLookupTriggerGroup(SExternalWindowOperator* pExtW, uint64_t triggerGroupId) {
  if (pExtW == NULL) {
    return NULL;
  }
  SExtWinTrigGrpCtx** ppTCtx = tSimpleHashGet(pExtW->pGroupRegistry, &triggerGroupId, sizeof(triggerGroupId));
  return ppTCtx == NULL ? NULL : *ppTCtx;
}

static SExtWinCalcGrpCtx* extWinIterateCalcGroup(SExtWinTrigGrpCtx* pTCtx, void** ppIter, int32_t* pIter) {
  *ppIter = tSimpleHashIterate(pTCtx == NULL ? NULL : pTCtx->pCGCtxs, *ppIter, pIter);
  return *ppIter == NULL ? NULL : *(SExtWinCalcGrpCtx**)*ppIter;
}

static bool extWinGroupIsFinalized(SExternalWindowOperator* pExtW, uint64_t groupId) {
  return pExtW->pFinalizedGrpIds != NULL && tSimpleHashGet(pExtW->pFinalizedGrpIds, &groupId, sizeof(groupId)) != NULL;
}

static int32_t extWinFinalizeGroup(SExternalWindowOperator* pExtW, uint64_t groupId) {
  if (groupId == 0 || extWinGroupIsFinalized(pExtW, groupId)) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  if (pExtW->pFinalizedGrpIds == NULL) {
    pExtW->pFinalizedGrpIds = tSimpleHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
    if (pExtW->pFinalizedGrpIds == NULL) {
      return terrno;
    }
  }

  int8_t finalized = 1;
  return tSimpleHashPut(pExtW->pFinalizedGrpIds, &groupId, sizeof(groupId), &finalized, sizeof(finalized));
}

static void extWinClearGroupProgression(SExternalWindowOperator* pExtW) {
  tSimpleHashCleanup(pExtW->pFinalizedGrpIds);
  pExtW->pFinalizedGrpIds = NULL;
  pExtW->pLastOutput = NULL;
  pExtW->lastOutputIter = 0;
  pExtW->groupOrderIdx = 0;
  pExtW->activeBaseGId = 0;
  pExtW->groupEogPending = false;
  pExtW->groupScanDone = false;
  pExtW->groupOutputComplete = false;
  pExtW->activeGroupPublished = false;
  pExtW->boundaryReady = false;
  pExtW->mergeInputDone = false;
  pExtW->mergeGroupComplete = false;
}

static void extWinAssignBlockGrpId(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, SBlockID* pId);

static int32_t extWinBlockListAddBlock(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, SList* pList,
                                       int32_t rows, SSDataBlock** ppBlock, SArray** ppIdx) {
  SSDataBlock* pRes = NULL;
  int32_t code = 0, lino = 0;

  if (listNEles(pExtW->pFreeBlocks) > 0) {
    TAOS_CHECK_EXIT(extWinTakeReusableBlock(pExtW->pFreeBlocks, pList, rows, ppBlock, ppIdx));
    pExtW->stat.resBlockReused++;
  } else {
    TAOS_CHECK_EXIT(createOneDataBlock(pExtW->binfo.pRes, false, &pRes));
    TAOS_CHECK_EXIT(blockDataEnsureCapacity(pRes, TMAX(rows, 4096)));
    SArray* pIdx = taosArrayInit(10, sizeof(int64_t));
    TSDB_CHECK_NULL(pIdx, code, lino, _exit, terrno);
    void* res[2] = {pRes, pIdx};
    TAOS_CHECK_EXIT(tdListAppend(pList, res));

    *ppBlock = pRes;
    *ppIdx = pIdx;
    pExtW->stat.resBlockCreated++;
  }

  extWinAssignBlockGrpId(pOperator, pExtW, &(*ppBlock)->info.id);

_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(pRes);
  }
  
  return code;
}

static void extWinBindStreamGroupRuntime(SStreamRuntimeFuncInfo* pStream, SSTriggerGroupCalcInfo* pGroup,
                                         uint64_t groupId) {
  pStream->curGrpCalc = pGroup;
  pStream->groupId = groupId;
  pStream->createTable = &pGroup->createTable;
  pStream->pStreamPesudoFuncVals = pGroup->pParams;
  pStream->pStreamPartColVals = pGroup->pGroupColVals;
  pStream->rollupTbCount = pGroup->rollupTbCount;
}

static bool extWinUsesStrictMultiGroupProtocol(const SOperatorInfo* pOperator) {
  return pOperator != NULL && pOperator->pTaskInfo != NULL &&
         pOperator->pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM && isStaticMultiGroupJoinQualified(pOperator) &&
         pOperator->pTaskInfo->pStreamRuntimeInfo != NULL &&
         pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.isMultiGroupCalc;
}

static bool extWinUsesStreamingBroadcast(const SOperatorInfo* pOperator, const SExternalWindowOperator* pExtW) {
  return pExtW != NULL && pExtW->inputMode == EXT_WIN_INPUT_BROADCAST &&
         (pExtW->mode == EEXT_MODE_SCALAR || pExtW->mode == EEXT_MODE_INDEFR_FUNC) && !pExtW->calcWithPartition &&
         extWinUsesStrictMultiGroupProtocol(pOperator);
}

static int32_t extWinValidateMultiGroupProtocolMode(const SOperatorInfo* pOperator) {
  if (pOperator == NULL || pOperator->pTaskInfo == NULL || pOperator->pTaskInfo->execModel != OPTR_EXEC_MODEL_STREAM ||
      !isStaticMultiGroupJoinQualified(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  if (pOperator->pTaskInfo->pStreamRuntimeInfo == NULL ||
      !pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.isMultiGroupCalc) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinGetExpectedGroup(const SStreamRuntimeFuncInfo* pStream, int32_t orderIdx, uint64_t* pGid,
                                      SSTriggerGroupCalcInfo** ppGroup) {
  if (pStream == NULL || pGid == NULL || ppGroup == NULL || orderIdx < 0 || pStream->pGroupCalcOrder == NULL ||
      pStream->pGroupCalcInfos == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  *pGid = 0;
  *ppGroup = NULL;
  if (orderIdx >= taosArrayGetSize(pStream->pGroupCalcOrder)) {
    return TSDB_CODE_SUCCESS;
  }

  uint64_t* pExpectedGid = taosArrayGet(pStream->pGroupCalcOrder, orderIdx);
  if (pExpectedGid == NULL || *pExpectedGid == 0) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  SSTriggerGroupCalcInfo* pGroup = tSimpleHashGet(pStream->pGroupCalcInfos, pExpectedGid, sizeof(*pExpectedGid));
  if (pGroup == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  *pGid = *pExpectedGid;
  *ppGroup = pGroup;
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinInitCGrpCtx(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo,
                                 SSTriggerGroupCalcInfo* pGroup, uint64_t triggerGroupId, SExtWinCalcGrpCtx* pCtx);
static int32_t extWinPrepareWindowDecisions(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                            SExtWinCalcGrpCtx* pCCtx);

static int32_t extWinActivateGroup(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                   SSTriggerGroupCalcInfo* pGroup, uint64_t triggerGroupId, uint64_t calcGroupId,
                                   bool recordOrder) {
  if (pOperator == NULL || pExtW == NULL || pOperator->pTaskInfo == NULL ||
      pOperator->pTaskInfo->pStreamRuntimeInfo == NULL || pGroup == NULL || triggerGroupId == 0 ||
      pGroup->pParams == NULL || taosArrayGetSize(pGroup->pParams) <= 0 || pGroup->pRunnerGrpCtx != NULL ||
      extWinGroupIsFinalized(pExtW, triggerGroupId)) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  bool               newTrigger = false;
  bool               newCalc = false;
  bool               allocatedTrigger = false;
  bool               allocatedCalc = false;
  SExtWinTrigGrpCtx* pTCtx = NULL;
  SExtWinCalcGrpCtx* pCCtx = NULL;

  if (pExtW->pGroupRegistry == NULL) {
    pExtW->pGroupRegistry = tSimpleHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
    TSDB_CHECK_NULL(pExtW->pGroupRegistry, code, lino, _exit, terrno);
  }

  SExtWinTrigGrpCtx** ppTCtx = tSimpleHashGet(pExtW->pGroupRegistry, &triggerGroupId, sizeof(triggerGroupId));
  if (ppTCtx == NULL) {
    pTCtx = taosMemoryCalloc(1, sizeof(*pTCtx));
    TSDB_CHECK_NULL(pTCtx, code, lino, _exit, terrno);
    allocatedTrigger = true;
    TAOS_CHECK_EXIT(
        tSimpleHashPut(pExtW->pGroupRegistry, &triggerGroupId, sizeof(triggerGroupId), &pTCtx, sizeof(pTCtx)));
    allocatedTrigger = false;
    newTrigger = true;
  } else {
    pTCtx = *ppTCtx;
  }

  if (pTCtx->pCGCtxs == NULL) {
    pTCtx->pCGCtxs = tSimpleHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
    TSDB_CHECK_NULL(pTCtx->pCGCtxs, code, lino, _exit, terrno);
  }

  SExtWinCalcGrpCtx** ppCCtx = tSimpleHashGet(pTCtx->pCGCtxs, &calcGroupId, sizeof(calcGroupId));
  if (ppCCtx == NULL) {
    pCCtx = taosMemoryCalloc(1, sizeof(*pCCtx));
    TSDB_CHECK_NULL(pCCtx, code, lino, _exit, terrno);
    allocatedCalc = true;
    pCCtx->groupId = calcGroupId;
    TAOS_CHECK_EXIT(extWinInitCGrpCtx(pExtW, pOperator->pTaskInfo, pGroup, triggerGroupId, pCCtx));
    pCCtx->groupId = calcGroupId;
    TAOS_CHECK_EXIT(tSimpleHashPut(pTCtx->pCGCtxs, &calcGroupId, sizeof(calcGroupId), &pCCtx, sizeof(pCCtx)));
    newCalc = true;
    allocatedCalc = false;
  } else {
    pCCtx = *ppCCtx;
  }

  if (recordOrder && newCalc && (pExtW->needGroupSort || pExtW->inputMode == EXT_WIN_INPUT_BROADCAST)) {
    SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
    if (pStream->isMultiGroupCalc && pExtW->calcWithPartition) {
      if (pExtW->pCTGrpIds == NULL) {
        pExtW->pCTGrpIds = taosArrayInit(16, sizeof(uint64_t) * 2);
        TSDB_CHECK_NULL(pExtW->pCTGrpIds, code, lino, _exit, terrno);
      }
      uint64_t identity[2] = {calcGroupId, triggerGroupId};
      TSDB_CHECK_NULL(taosArrayPush(pExtW->pCTGrpIds, identity), code, lino, _exit, terrno);
    } else if (newTrigger || !pStream->isMultiGroupCalc) {
      if (pExtW->pGrpIds == NULL) {
        pExtW->pGrpIds = taosArrayInit(16, sizeof(uint64_t));
        TSDB_CHECK_NULL(pExtW->pGrpIds, code, lino, _exit, terrno);
      }
      uint64_t orderId = pStream->isMultiGroupCalc ? triggerGroupId : calcGroupId;
      TSDB_CHECK_NULL(taosArrayPush(pExtW->pGrpIds, &orderId), code, lino, _exit, terrno);
    }
  }

  pTCtx->pCCtx = pCCtx;
  pExtW->pTGrpCtx = pTCtx;
  pExtW->lastTGrpId = triggerGroupId;
  pExtW->lastCGrpId = calcGroupId;
  if (pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.isMultiGroupCalc) {
    extWinBindStreamGroupRuntime(&pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo, pGroup, triggerGroupId);
  }
  TAOS_CHECK_EXIT(extWinPrepareWindowDecisions(pOperator, pExtW, pCCtx));

_exit:
  if (code != TSDB_CODE_SUCCESS && allocatedTrigger) {
    taosMemoryFree(pTCtx);
  }
  if (code != TSDB_CODE_SUCCESS && allocatedCalc) {
    extWinDestroyCGrpCtx(pCCtx);
    taosMemoryFree(pCCtx);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pOperator->pTaskInfo), __func__, lino, tstrerror(code));
  }
  return code;
}

static void extWinPostUpdateStreamRt(SStreamRuntimeFuncInfo* pStream, SOperatorInfo* pOperator,
                                     SExternalWindowOperator* pExtW) {
  if (pStream->isMultiGroupCalc && pStream->curGrpCalc != NULL) {
    extWinBindStreamGroupRuntime(pStream, pStream->curGrpCalc, pExtW->lastTGrpId);
  }

  qDebug("%s streamRt updated, groupId %" PRIu64, GET_TASKID(pOperator->pTaskInfo), pStream->groupId);
}

static void extWinAssignBlockGrpId(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, SBlockID* pId) {
  SStreamRuntimeFuncInfo* pStream =
      pOperator->pTaskInfo->pStreamRuntimeInfo == NULL ? NULL : &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  bool     multiGroupCalc = pStream != NULL && pStream->isMultiGroupCalc;
  uint64_t currentCGrpId = (pExtW->pTGrpCtx != NULL && pExtW->pTGrpCtx->pCCtx != NULL)
                               ? pExtW->pTGrpCtx->pCCtx->groupId
                               : pExtW->lastCGrpId;

  if (pExtW->calcWithPartition && !multiGroupCalc) {
    // For partitioned non-multi-group output, the effective merge group is the
    // current calc-group id. If we keep using lastTGrpId here, all output
    // blocks collapse to group 0 and the downstream merge path may concatenate
    // different partition groups without re-ordering by _group_id.
    pId->groupId = currentCGrpId;
    pId->baseGId = 0;
  } else if (pExtW->extWinSplit) {
    pId->groupId = currentCGrpId;
    pId->baseGId = pExtW->lastTGrpId;
  } else if (multiGroupCalc) {
    pId->groupId = pExtW->lastTGrpId;
    pId->baseGId = pExtW->lastTGrpId;
  } else {
    pId->groupId = pExtW->lastTGrpId;
    pId->baseGId = 0;
  }

  qDebug("%s extWin res block assigned groupId %" PRIu64 " baseGid %" PRIu64, GET_TASKID(pOperator->pTaskInfo), pId->groupId, pId->baseGId);
}

static FORCE_INLINE void extWinResetBlockCalcState(SExtWinCalcGrpCtx* pCCtx) {
  if (pCCtx == NULL) {
    return;
  }

  pCCtx->blkWinIdx = -1;
  pCCtx->blkWinStartSet = false;
  pCCtx->blkRowStartIdx = 0;
}

static int32_t extWinGetLastBlockFromList(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, SList* pList, int32_t rows, SSDataBlock** ppBlock, SArray** ppIdx) {
  int32_t    code = 0, lino = 0;
  SSDataBlock* pRes = NULL;

  SListNode* pNode = TD_DLIST_TAIL(pList);
  if (NULL == pNode) {
    TAOS_CHECK_EXIT(extWinBlockListAddBlock(pOperator, pExtW, pList, rows, ppBlock, ppIdx));
    return code;
  }

  pRes = *(SSDataBlock**)pNode->data;
  SBlockID expectedId = pRes->info.id;
  extWinAssignBlockGrpId(pOperator, pExtW, &expectedId);
  if ((pRes->info.rows + rows) > pRes->info.capacity || pRes->info.id.groupId != expectedId.groupId ||
      pRes->info.id.baseGId != expectedId.baseGId) {
    TAOS_CHECK_EXIT(extWinBlockListAddBlock(pOperator, pExtW, pList, rows, ppBlock, ppIdx));
    return code;
  }

  extWinAssignBlockGrpId(pOperator, pExtW, &pRes->info.id);
  
  *ppIdx = *(SArray**)((SSDataBlock**)pNode->data + 1);
  *ppBlock = pRes;
  pExtW->stat.resBlockAppend++;

_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  
  return code;
}

static void extWinDestroyBlockList(void* p) {
  if (NULL == p) {
    return;
  }

  SListNode* pTmp = NULL;
  SList** ppList = (SList**)p;
  if ((*ppList) && TD_DLIST_NELES(*ppList) > 0) {
    SListNode* pNode = TD_DLIST_HEAD(*ppList);
    while (pNode) {
      SSDataBlock* pBlock = *(SSDataBlock**)pNode->data;
      blockDataDestroy(pBlock);
      SArray* pIdx = *(SArray**)((SArray**)pNode->data + 1);
      taosArrayDestroy(pIdx);
      pTmp = pNode;
      pNode = pNode->dl_next_;
      taosMemoryFree(pTmp);
    }
  }
  taosMemoryFree(*ppList);
}


static void extWinRecycleBlkNode(SExternalWindowOperator* pExtW, SListNode** ppNode) {
  if (NULL == ppNode || NULL == *ppNode) {
    return;
  }

  SSDataBlock* pBlock = *(SSDataBlock**)(*ppNode)->data;
  SArray* pIdx = *(SArray**)((SArray**)(*ppNode)->data + 1);
  
  if (listNEles(pExtW->pFreeBlocks) >= 10) {
    blockDataDestroy(pBlock);
    taosArrayDestroy(pIdx);
    taosMemoryFreeClear(*ppNode);
    pExtW->stat.resBlockDestroyed++;
    return;
  }
  
  blockDataCleanup(pBlock);
  taosArrayClear(pIdx);
  tdListPrependNode(pExtW->pFreeBlocks, *ppNode);
  *ppNode = NULL;
  pExtW->stat.resBlockRecycled++;
}

static void extWinRecycleOutputBlkNode(SExternalWindowOperator* pExtW, SArray** ppRuntimeIdx, SListNode** ppNode) {
  if (NULL != ppRuntimeIdx && NULL != ppNode && NULL != *ppNode) {
    SArray* pIdx = *(SArray**)((SArray**)(*ppNode)->data + 1);
    if (*ppRuntimeIdx == pIdx) {
      *ppRuntimeIdx = NULL;
    }
  }

  extWinRecycleBlkNode(pExtW, ppNode);
}

static bool extWinOwnsRuntimeIdxAlias(SExternalWindowOperator* pExtW, SStreamRuntimeFuncInfo* pRuntime) {
  if (pExtW == NULL || pRuntime == NULL || pRuntime->pStreamBlkWinIdx == NULL) {
    return false;
  }
  if (pRuntime->pStreamBlkWinIdx == pExtW->pWinRowIdx) {
    return true;
  }
  if (pExtW->pLastBlkNode == NULL) {
    return false;
  }

  SArray* pLastIdx = *(SArray**)((SArray**)pExtW->pLastBlkNode->data + 1);
  return pRuntime->pStreamBlkWinIdx == pLastIdx;
}

static void extWinDetachPublishedRuntimeAliases(SExecTaskInfo* pTaskInfo, SExternalWindowOperator* pExtW) {
  if (pTaskInfo == NULL || pTaskInfo->pStreamRuntimeInfo == NULL) {
    return;
  }

  SStreamRuntimeFuncInfo* pRuntime = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  if (!extWinOwnsRuntimeIdxAlias(pExtW, pRuntime) && !(pRuntime->pStreamBlkWinIdx == NULL && pExtW->groupScanDone)) {
    return;
  }

  pRuntime->pStreamBlkWinIdx = NULL;
  if (!pRuntime->isMultiGroupCalc) {
    return;
  }
  pRuntime->curGrpCalc = NULL;
  pRuntime->pStreamPesudoFuncVals = NULL;
  pRuntime->pStreamPartColVals = NULL;
  pRuntime->groupId = 0;
  pRuntime->createTable = NULL;
  pRuntime->rollupTbCount = 0;
  tClearStreamCurrentGroupRead(pRuntime);
}

static void extWinRecycleBlockList(SExternalWindowOperator* pExtW, void* p) {
  if (NULL == p) {
    return;
  }

  SListNode* pTmp = NULL;
  SList** ppList = (SList**)p;
  if ((*ppList) && TD_DLIST_NELES(*ppList) > 0) {
    SListNode* pNode = TD_DLIST_HEAD(*ppList);
    while (pNode) {
      pTmp = pNode;
      pNode = pNode->dl_next_;
      extWinRecycleBlkNode(pExtW, &pTmp);
    }
  }
  taosMemoryFree(*ppList);
  *ppList = NULL;
}
static void extWinDestroyBlkNode(SExternalWindowOperator* pInfo, SListNode* pNode) {
  if (NULL == pNode) {
    return;
  }

  SSDataBlock* pBlock = *(SSDataBlock**)pNode->data;
  SArray* pIdx = *(SArray**)((SArray**)pNode->data + 1);
  
  blockDataDestroy(pBlock);
  taosArrayDestroy(pIdx);

  taosMemoryFree(pNode);

  pInfo->stat.resBlockDestroyed++;
}


// Invoked by destroyOperator() before cleanupExprSupp(), the only point where both the expression
// supp and the result rows are still valid. Doing this from the closeFn below is too late: the supp
// has been memset by then, so numOfExprs is 0 and the loop below would silently iterate nothing,
// leaking every DISTINCT dedup set still attached to an unemitted row (early cancel, LIMIT short-circuit).
static FORCE_INLINE SResultRow* extWinGetResultRowByIdx(SExternalWindowOperator* pExtW, int32_t resWinIdx,
                                                        int32_t resultRowSize);

static void extWinCleanupResultRowsState(SExternalWindowOperator* pExtW) {
  if (pExtW == NULL || pExtW->mode != EEXT_MODE_AGG ||
      pExtW->resultRows.pResultRows == NULL || pExtW->pOperator == NULL) {
    return;
  }
  SExprSupp* pSupp = &pExtW->pOperator->exprSupp;
  SExtWinResultRows* pRows = &pExtW->resultRows;
  int64_t rowCount = (int64_t)pRows->resRowsIdx * pRows->resRowSize + pRows->resRowIdx;
  if (pRows->rowSizeBytes <= 0) {
    return;
  }
  for (int64_t i = 0; i < rowCount; ++i) {
    SResultRow* pRow = extWinGetResultRowByIdx(pExtW, (int32_t)i, pRows->rowSizeBytes);
    if (pRow == NULL) {
      continue;
    }
    cleanupResultRowFunctionState(pSupp, pRow);
  }
}

static void cleanupExternalWindowRowState(void* param) {
  // destroyOperator() invokes this callback before destroyExternalWindowOperatorInfo().
  // The ordering keeps exprSupp and resultRows alive while row state is released.
  extWinCleanupResultRowsState((SExternalWindowOperator*)param);
}

static void destroyExternalWindowOperatorInfo(void* param) {
  if (NULL == param) {
    return;
  }
  SExternalWindowOperator* pInfo = (SExternalWindowOperator*)param;
  cleanupBasicInfo(&pInfo->binfo);

  taosArrayDestroyEx(pInfo->pOutputBlocks, extWinDestroyBlockList);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  taosArrayDestroy(pInfo->pWinRowIdx);
  
  taosArrayDestroy(pInfo->pPseudoColInfo);
  taosArrayDestroy(pInfo->fillMatchInfo.pList);
  blockDataDestroy(pInfo->pTmpBlock);
  blockDataDestroy(pInfo->pProjTmpBlock);
  blockDataDestroy(pInfo->pEmptyInputBlock);

  extWinDestroyBlkNode(pInfo, pInfo->pLastBlkNode);
  if (pInfo->pFreeBlocks) {
    SListNode *node;
    while ((node = TD_DLIST_HEAD(pInfo->pFreeBlocks)) != NULL) {
      TD_DLIST_POP(pInfo->pFreeBlocks, node);
      extWinDestroyBlkNode(pInfo, node);
    }
    taosMemoryFree(pInfo->pFreeBlocks);
  }

  taosArrayDestroy(pInfo->pGrpIds);
  taosArrayDestroy(pInfo->pCTGrpIds);
  taosArrayDestroy(pInfo->pNullOutputSlots);
#if defined(BUILD_TEST)
  taosArrayDestroy(pInfo->pBroadcastReplayObservations);
#endif

  extWinClearGroupRegistry(pInfo);
  extWinClearGroupProgression(pInfo);

  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSupp);
  cleanupExprSupp(&pInfo->projSupp);
  for (int32_t i = 0; i < pInfo->resultRows.resRowsSize; ++i) {
    if (pInfo->resultRows.pResultRows && pInfo->resultRows.pResultRows[i]) {
      taosMemoryFreeClear(pInfo->resultRows.pResultRows[i]);
    }
  }
  taosMemoryFreeClear(pInfo->resultRows.pResultRows);
  
  pInfo->binfo.resultRowInfo.openWindow = tdListFree(pInfo->binfo.resultRowInfo.openWindow);

  qDebug("ext window stat at destroy, created:%" PRId64 ", destroyed:%" PRId64 ", recycled:%" PRId64 ", reused:%" PRId64
         ", append:%" PRId64 ", broadcast scan blocks:%" PRId64 ", rows:%" PRId64 ", replay blocks:%" PRId64
         ", rows:%" PRId64 ", store budget:%" PRId64 ", payload peak estimate:%" PRId64 ", flush bytes:%" PRId64
         ", load bytes:%" PRId64,
         pInfo->stat.resBlockCreated, pInfo->stat.resBlockDestroyed, pInfo->stat.resBlockRecycled,
         pInfo->stat.resBlockReused, pInfo->stat.resBlockAppend, pInfo->stat.broadcastScanBlocks,
         pInfo->stat.broadcastScanRows, pInfo->stat.broadcastReplayBlocks, pInfo->stat.broadcastReplayRows,
         pInfo->stat.broadcastStoreBudget, pInfo->stat.broadcastPayloadPeak, pInfo->stat.broadcastFlushBytes,
         pInfo->stat.broadcastLoadBytes);

  taosMemoryFreeClear(pInfo);
}

static int32_t extWinOpen(SOperatorInfo* pOperator);
static int32_t extWinNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);
static int32_t mergeAlignExtWinNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);
static int32_t extWinEnsureDynParamOpenCtx(SExternalWindowOperator* pExtW);
static int32_t getTimeWindowOfBlock(SSDataBlock* pBlock, col_id_t tsSlotId,
                                    int64_t* startTs, int64_t* endTs);

static FORCE_INLINE void extWinIntersectTimeRange(STimeWindow* pDst, const STimeWindow* pRange) {
  if (pDst == NULL || pRange == NULL) {
    return;
  }

  pDst->skey = TMAX(pDst->skey, pRange->skey);
  pDst->ekey = TMIN(pDst->ekey, pRange->ekey);
}

static void extWinApplyTimeRangeToTableScan(SOperatorInfo* pScanOp, const STimeWindow* pTimeRange) {
  if (pScanOp == NULL || pScanOp->info == NULL || pTimeRange == NULL) {
    return;
  }

  STableScanInfo* pScanInfo = (STableScanInfo*)pScanOp->info;
  extWinIntersectTimeRange(&pScanInfo->base.cond.twindows, pTimeRange);
  extWinIntersectTimeRange(&pScanInfo->base.orgCond.twindows, pTimeRange);
}

static void extWinApplyTimeRangeToExchangeParam(SOperatorParam* pParam, const STimeWindow* pTimeRange) {
  if (pParam == NULL || pParam->value == NULL || pTimeRange == NULL) {
    return;
  }

  SExchangeOperatorParam* pExcParam = (SExchangeOperatorParam*)pParam->value;
  if (pExcParam->multiParams) {
    return;
  }

  if (pExcParam->basic.paramType == 0) {
    pExcParam->basic.paramType = DYN_TYPE_EXCHANGE_PARAM;
  }
  extWinIntersectTimeRange(&pExcParam->basic.window, pTimeRange);
}

static int32_t extWinBuildFederatedRangeParam(SOperatorParam** ppParam, STimeWindow window) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SOperatorParam* pParam = NULL;

  QUERY_CHECK_NULL(ppParam, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppParam = NULL;
  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno);
  pParam->value = taosMemoryCalloc(1, sizeof(SDynQueryCtrlOperatorParam));
  QUERY_CHECK_NULL(pParam->value, code, lino, _return, terrno);
  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN;
  // Installed after parent routing is complete; -1 distinguishes this from
  // VStb's normal FederatedScan dispatch param.
  pParam->downstreamIdx = -1;
  ((SDynQueryCtrlOperatorParam*)pParam->value)->window = window;
  *ppParam = pParam;
  return TSDB_CODE_SUCCESS;

_return:
  if (NULL != pParam) {
    taosMemoryFree(pParam->value);
    taosMemoryFree(pParam);
  }
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static bool extWinApplyRangeToFederatedBatchParam(SOperatorParam* pParam, STimeWindow window) {
  if (pParam == NULL || pParam->opType != QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN ||
      pParam->downstreamIdx == -1 || pParam->pChildren != NULL || pParam->value == NULL) {
    return false;
  }

  SForeignScanOperatorParam* pFsParam = pParam->value;
  if (pFsParam->paramType != FOREIGN_SCAN_PARAM_BATCH) {
    return false;
  }

  pFsParam->window = window;
  pFsParam->isNewParam = true;
  return true;
}

static void extWinSetFederatedRuntimeRange(SOperatorInfo* pOperator, STimeWindow window) {
  if (NULL == pOperator ||
      QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN != pOperator->operatorType ||
      NULL == pOperator->info) {
    return;
  }
  SFederatedScanOperatorInfo* pInfo = pOperator->info;
  if (pInfo->pQueryHandle) {
    extConnectorCloseQuery(pInfo->pQueryHandle);
    pInfo->pQueryHandle = NULL;
  }
  if (pInfo->pConnHandle) {
    extConnectorClose(pInfo->pConnHandle);
    pInfo->pConnHandle = NULL;
  }
  pInfo->runtimeTimeRange = window;
  pInfo->hasRuntimeTimeRange = true;
  pInfo->queryStarted = false;
  pInfo->queryFinished = false;
}

static int32_t extWinResetCompletedDownstream(SOperatorInfo* pDownstream) {
  if (pDownstream == NULL || pDownstream->status != OP_EXEC_DONE ||
      pDownstream->fpSet.resetStateFn == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  return pDownstream->fpSet.resetStateFn(pDownstream);
}

static int32_t extWinApplyNonStreamTimeRangeToOperatorTree(SOperatorInfo* pOperator, const STimeWindow* pTimeRange,
                                                           int32_t* pScanAppliedNum, SExecTaskInfo* pRootTaskInfo) {
  if (pOperator == NULL || pTimeRange == NULL || pScanAppliedNum == NULL || pRootTaskInfo == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pOperator->pTaskInfo != pRootTaskInfo) {
    return TSDB_CODE_SUCCESS;
  }

  if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    extWinApplyTimeRangeToTableScan(pOperator, pTimeRange);
    ++(*pScanAppliedNum);
  }

  if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
    extWinApplyTimeRangeToExchangeParam(pOperator->pOperatorGetParam, pTimeRange);
  }

  if (pOperator->numOfDownstream <= 0 || pOperator->pDownstream == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pChild = pOperator->pDownstream[i];
    if (pChild == NULL) {
      continue;
    }

    if (pOperator->pDownstreamGetParams != NULL &&
        pChild->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
      extWinApplyTimeRangeToExchangeParam(pOperator->pDownstreamGetParams[i], pTimeRange);
    }

    int32_t code = extWinApplyNonStreamTimeRangeToOperatorTree(pChild, pTimeRange, pScanAppliedNum, pRootTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extWinApplyNonStreamTimeRangeToDownstream(SOperatorInfo* pOperator, const STimeWindow* pTimeRange) {
  if (pOperator == NULL || pTimeRange == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pTimeRange->skey == INT64_MAX || pTimeRange->ekey == INT64_MIN || pTimeRange->skey > pTimeRange->ekey) {
    return TSDB_CODE_SUCCESS;
  }

  if (pOperator->numOfDownstream <= 0 || pOperator->pDownstream == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t scanAppliedNum = 0;
  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (pDownstream == NULL) {
      continue;
    }

    int32_t code = extWinApplyNonStreamTimeRangeToOperatorTree(pDownstream, pTimeRange, &scanAppliedNum,
                                                                pOperator->pTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  qInfo("%s apply non-stream extWin timerange:[%" PRId64 ", %" PRId64 "] to downstream tree, tableScanCnt:%d",
        GET_TASKID(pOperator->pTaskInfo), pTimeRange->skey, pTimeRange->ekey, scanAppliedNum);

  return TSDB_CODE_SUCCESS;
}

typedef struct SMergeAlignedExternalWindowOperator {
  SExternalWindowOperator* pExtW;
  int64_t                  curTs;
  SResultRow*              pResultRow;
  SSDataBlock*             pNewGroup;
  // Nested external windows share one task, so the task-global stream cursor
  // cannot identify this operator's current window reliably.
  int32_t curWinIdx;
  // Tracks the last emitted window index when filling empty-window gaps.
  int32_t lastFinalizedWinIdx;
} SMergeAlignedExternalWindowOperator;

static void cleanupMergeAlignedExternalWindowRowState(void* param) {
  if (param == NULL) {
    return;
  }
  SMergeAlignedExternalWindowOperator* pMAExtW =
      (SMergeAlignedExternalWindowOperator*)param;
  extWinCleanupResultRowsState(pMAExtW->pExtW);
}

static int32_t extWinInitNonStreamWindowDataFromBlock(SExternalWindowPhysiNode* pPhynode, SExecTaskInfo* pTaskInfo,
                                                       STimeWindow* pTimeRange);
static int32_t extWinInitExprSupp(SExprSupp* pExprSupp, SNodeList* pNodeList, SExecTaskInfo* pTaskInfo);

static void destroyMergeAlignedExternalWindowOperator(void* pOperator) {
  SMergeAlignedExternalWindowOperator* pMAExtW = (SMergeAlignedExternalWindowOperator*)pOperator;
  destroyExternalWindowOperatorInfo(pMAExtW->pExtW);
  taosMemoryFreeClear(pMAExtW);
}

static int64_t* extWinExtractTsCol(SSDataBlock* pBlock, int32_t primaryTsIndex, SExecTaskInfo* pTaskInfo) {
  TSKEY* tsCols = NULL;

  if (pBlock->pDataBlock != NULL && pBlock->info.dataLoad) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, primaryTsIndex);
    if (!pColDataInfo) {
      pTaskInfo->code = terrno;
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    tsCols = (int64_t*)pColDataInfo->pData;
    if (pBlock->info.window.skey == 0 && pBlock->info.window.ekey == 0) {
      int32_t code = blockDataUpdateTsWindow(pBlock, primaryTsIndex);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
    }
  }

  return tsCols;
}

static FORCE_INLINE SExternalWindowOperator* extWinGetCoreInfo(SOperatorInfo* pOperator) {
  if (pOperator == NULL) {
    return NULL;
  }

  // getNextFn is an execution-record wrapper; _nextFn is the real operator callback.
  if (pOperator->fpSet._nextFn == mergeAlignExtWinNext) {
    SMergeAlignedExternalWindowOperator* pMAExtW = pOperator->info;
    return (pMAExtW != NULL) ? pMAExtW->pExtW : NULL;
  }

  return pOperator->info;
}

static FORCE_INLINE SExtWinCalcGrpCtx* extWinGetScopedCalcGrpCtx(SOperatorInfo* pOperator) {
  SExternalWindowOperator* pExtW = extWinGetCoreInfo(pOperator);
  if (pExtW == NULL || pExtW->pTGrpCtx == NULL || pExtW->pTGrpCtx->pCCtx == NULL) {
    return NULL;
  }

  // In partitioned external-window queries, different calc-groups may share
  // the same trigger-group. Their window cursors must stay isolated per
  // calc-group instead of falling back to the task-global cursor.
  if (pExtW->calcWithPartition) {
    return pExtW->pTGrpCtx->pCCtx;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pTaskInfo != NULL && pTaskInfo->pStreamRuntimeInfo != NULL) {
    SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
    if (pInfo->isMultiGroupCalc) {
      return pExtW->pTGrpCtx->pCCtx;
    }
  }

  return NULL;
}

static int32_t extWinGetCurWinIdx(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (!pTaskInfo->pStreamRuntimeInfo) {
    return 0;
  }
  SExtWinCalcGrpCtx* pCCtx = extWinGetScopedCalcGrpCtx(pOperator);
  if (pCCtx != NULL) {
    return pCCtx->curIdx;
  }

  return pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx;
}

static void extWinSetCurWinIdx(SOperatorInfo* pOperator, int32_t idx) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pTaskInfo->pStreamRuntimeInfo) {
    SExtWinCalcGrpCtx* pCCtx = extWinGetScopedCalcGrpCtx(pOperator);
    if (pCCtx != NULL) {
      pCCtx->curIdx = idx;
    }
    pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx = idx;
  }
}

static int32_t extWinPrepareWindowDecisions(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                            SExtWinCalcGrpCtx* pCCtx) {
  if (pOperator == NULL || pExtW == NULL || pCCtx == NULL || pCCtx->pWins == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (pCCtx->windowDecisionsReady) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t winCount = (int32_t)taosArrayGetSize(pCCtx->pWins);
  SArray* pDecisions = taosArrayInit_s(sizeof(int8_t), winCount);
  if (pDecisions == NULL) {
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t savedWinIdx = extWinGetCurWinIdx(pOperator);
  if (pExtW->pWindowConditions == NULL) {
    memset(pDecisions->pData, 1, (size_t)winCount * sizeof(int8_t));
  } else {
    for (int32_t i = 0; i < winCount; ++i) {
      extWinSetCurWinIdx(pOperator, i);
      SScalarParam output = {0};
      output.columnData = taosMemoryCalloc(1, sizeof(SColumnInfoData));
      if (output.columnData == NULL) {
        code = terrno;
        break;
      }
      output.columnData->info.type = TSDB_DATA_TYPE_BOOL;
      output.columnData->info.bytes = sizeof(bool);
      output.colAlloced = true;
      code = colInfoDataEnsureCapacity(output.columnData, 1, true);
      if (code == TSDB_CODE_SUCCESS) {
        SScalarExtraInfo extra = {.pStreamInfo = GET_STM_RTINFO(pOperator->pTaskInfo), .isStream = true};
#if defined(BUILD_TEST)
        pExtW->stat.windowConditionEvals++;
#endif
        code = scalarCalculate(pExtW->pWindowConditions, NULL, &output, &extra);
      }
      if (code == TSDB_CODE_SUCCESS &&
          (output.columnData == NULL || output.columnData->info.type != TSDB_DATA_TYPE_BOOL || output.numOfRows != 1)) {
        code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      }
      if (code == TSDB_CODE_SUCCESS) {
        int8_t* pDecision = taosArrayGet(pDecisions, i);
        if (pDecision == NULL) {
          code = terrno;
        } else {
          *pDecision = !colDataIsNull_s(output.columnData, 0) && *(bool*)colDataGetData(output.columnData, 0);
        }
      }
      sclFreeParam(&output);
      if (code != TSDB_CODE_SUCCESS) {
        break;
      }
    }
  }
  extWinSetCurWinIdx(pOperator, savedWinIdx);

  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(pDecisions);
    return code;
  }
  taosArrayDestroy(pCCtx->pWindowConditionResults);
  pCCtx->pWindowConditionResults = pDecisions;
  pCCtx->windowDecisionsReady = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinGetWindowDecision(const SExtWinCalcGrpCtx* pCCtx, int32_t winIdx, bool* pPass) {
  if (pCCtx == NULL || pPass == NULL || !pCCtx->windowDecisionsReady || pCCtx->pWindowConditionResults == NULL ||
      winIdx < 0 || winIdx >= (int32_t)taosArrayGetSize(pCCtx->pWindowConditionResults)) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  int8_t* pDecision = taosArrayGet(pCCtx->pWindowConditionResults, winIdx);
  if (pDecision == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  *pPass = *pDecision != 0;
  return TSDB_CODE_SUCCESS;
}

static void extWinPrepareBlockScan(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SExternalWindowOperator* pExtW = extWinGetCoreInfo(pOperator);
  SExtWinCalcGrpCtx*       pCCtx = pExtW == NULL || pExtW->pTGrpCtx == NULL ? NULL : pExtW->pTGrpCtx->pCCtx;
  if (pCCtx == NULL || pCCtx->blkScanFlag == pBlock->info.scanFlag) {
    return;
  }

  if (pCCtx->blkScanFlag == PRE_SCAN && pBlock->info.scanFlag == MAIN_SCAN) {
    extWinSetCurWinIdx(pOperator, pCCtx->outWinIdx);
    pCCtx->lastWinId = pCCtx->outWinIdx - 1;
    pCCtx->lastSKey = INT64_MIN;
    pCCtx->lastEKey = INT64_MIN;
  }

  pCCtx->blkScanFlag = pBlock->info.scanFlag;
  int32_t nextWinIdx = TMAX(pCCtx->outWinIdx, pCCtx->lastWinId);
  if (nextWinIdx < 0) {
    nextWinIdx = 0;
  }
  extWinSetCurWinIdx(pOperator, nextWinIdx);
  pCCtx->blkWinStartIdx = nextWinIdx;
  pCCtx->blkRowStartIdx = 0;
}

static void extWinIncCurWinOutIdx(SOperatorInfo* pOperator) {
  if (pOperator == NULL || pOperator->pTaskInfo->pStreamRuntimeInfo == NULL) {
    return;
  }

  SExtWinCalcGrpCtx* pCCtx = extWinGetScopedCalcGrpCtx(pOperator);
  if (pCCtx != NULL) {
    pCCtx->curIdx++;
  }
  pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx++;
}

static int32_t extWinAppendWinIdx(SExecTaskInfo* pTaskInfo, SArray* pIdx, SSDataBlock* pBlock, int32_t currWinIdx,
                                  int32_t rows) {
  int32_t  code = 0, lino = 0;
  int64_t* lastRes = taosArrayGetLast(pIdx);
  int32_t* lastWinIdx = (int32_t*)lastRes;
  int32_t* lastRowIdx = lastWinIdx ? (lastWinIdx + 1) : NULL;
  int64_t  res = 0;
  int32_t* pWinIdx = (int32_t*)&res;
  int32_t* pRowIdx = pWinIdx + 1;

  if (lastWinIdx && *lastWinIdx == currWinIdx) {
    return code;
  }

  *pWinIdx = currWinIdx;
  *pRowIdx = pBlock->info.rows - rows;

  TSDB_CHECK_NULL(taosArrayPush(pIdx, &res), code, lino, _exit, terrno);

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", pTaskInfo->id.str, __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinRebuildWinIdxByFilter(SExecTaskInfo* pTaskInfo, SArray* pIdx, int32_t rowsBeforeFilter,
                                           SColumnInfoData* pFilterRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pIdx == NULL || pFilterRes == NULL || rowsBeforeFilter <= 0 || taosArrayGetSize(pIdx) == 0) {
    return code;
  }

  int32_t idxSize = (int32_t)taosArrayGetSize(pIdx);
  SArray* pNewIdx = taosArrayInit(idxSize, sizeof(int64_t));
  TSDB_CHECK_NULL(pNewIdx, code, lino, _exit, terrno);

  int8_t* pIndicator = (int8_t*)pFilterRes->pData;
  int32_t newRowStart = 0;
  for (int32_t i = 0; i < idxSize; ++i) {
    int64_t cur = *(int64_t*)taosArrayGet(pIdx, i);
    int32_t* pCurWinIdx = (int32_t*)&cur;
    int32_t* pCurRowIdx = pCurWinIdx + 1;

    int32_t startRow = *pCurRowIdx;
    int32_t endRow = rowsBeforeFilter;
    if (i + 1 < idxSize) {
      int64_t next = *(int64_t*)taosArrayGet(pIdx, i + 1);
      int32_t* pNextWinIdx = (int32_t*)&next;
      int32_t* pNextRowIdx = pNextWinIdx + 1;
      endRow = *pNextRowIdx;
    }

    startRow = TMIN(TMAX(startRow, 0), rowsBeforeFilter);
    endRow = TMIN(TMAX(endRow, 0), rowsBeforeFilter);
    if (endRow <= startRow) {
      continue;
    }

    int32_t survivedRows = 0;
    for (int32_t r = startRow; r < endRow; ++r) {
      if (pIndicator[r]) {
        survivedRows++;
      }
    }

    if (survivedRows <= 0) {
      continue;
    }

    int64_t out = 0;
    int32_t* pOutWinIdx = (int32_t*)&out;
    int32_t* pOutRowIdx = pOutWinIdx + 1;
    *pOutWinIdx = *pCurWinIdx;
    *pOutRowIdx = newRowStart;
    TSDB_CHECK_NULL(taosArrayPush(pNewIdx, &out), code, lino, _exit, terrno);

    newRowStart += survivedRows;
  }

  taosArrayClear(pIdx);
  int32_t newSize = (int32_t)taosArrayGetSize(pNewIdx);
  if (newSize > 0) {
    void* dest = taosArrayReserve(pIdx, newSize);
    TSDB_CHECK_NULL(dest, code, lino, _exit, terrno);
    memcpy(dest, pNewIdx->pData, (size_t)newSize * sizeof(int64_t));
  }

_exit:
  taosArrayDestroy(pNewIdx);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", pTaskInfo->id.str, __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t extWinInitCGrpCtx(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo,
                                 SSTriggerGroupCalcInfo* pGroup, uint64_t triggerGroupId, SExtWinCalcGrpCtx* pCtx) {
  int32_t code = 0, lino = 0;

  pCtx->groupId = 0;
  pCtx->curIdx = 0;
  pCtx->lastSKey = INT64_MIN;
  pCtx->lastEKey = INT64_MAX;
  pCtx->lastWinId = -1;
  pCtx->lastWinIdx = -1;
  pCtx->blkWinIdx = -1;
  pCtx->blkScanFlag = -1;
  pCtx->blkWinStartSet = false;
  pCtx->blkWinStartIdx = 0;
  pCtx->blkRowStartIdx = 0;
  pCtx->outWinIdx = 0;
  pCtx->outWinLastIdx = -1;
  pCtx->outWinTotalNum = 0;
  pCtx->outWinNum = 0;
  pCtx->settledThroughWinIdx = -1;
  pCtx->windowDecisionsReady = false;
  pCtx->pWins = NULL;
  pCtx->outWinBufIdx = NULL;
  pCtx->broadcast.pStat = &pExtW->stat;

  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  SStreamRuntimeFuncInfo  initInfo = *pInfo;
  initInfo.curGrpCalc = pGroup;
  initInfo.pStreamPesudoFuncVals = pGroup->pParams;
  initInfo.pStreamPartColVals = pGroup->pGroupColVals;
  if (initInfo.isMultiGroupCalc) {
    initInfo.groupId = triggerGroupId;
  }

  size_t size = taosArrayGetSize(pGroup->pParams);
  pCtx->pWins = taosArrayInit_s(sizeof(SExtWinTimeWindow), size);
  TSDB_CHECK_NULL(pCtx->pWins, code, lino, _exit, terrno);
  //pCtx->outWinBufIdx = taosArrayInit_s(sizeof(int32_t), size);
  //TSDB_CHECK_NULL(pCtx->outWinBufIdx, code, lino, _exit, terrno);

  SExtWinTimeWindow* pWin = taosArrayGet(pCtx->pWins, 0);

  if (pExtW->isMergeAlignedExtW) {
    for (int32_t i = 0; i < size; ++i) {
      SSTriggerCalcParam* pParam = taosArrayGet(pGroup->pParams, i);

      pWin[i].tw.skey = pParam->wstart;
      pWin[i].tw.ekey = pParam->wstart + 1;
      pWin[i].resWinIdx = -1;
    }
  } else if (pExtW->timeRangeExpr && pExtW->timeRangeExpr->needCalc) {
    TAOS_CHECK_EXIT(scalarCalculateExtWinsTimeRange(pExtW->timeRangeExpr, &initInfo, pWin));
    for (int32_t i = 0; i < size; ++i) {
      pWin[i].resWinIdx = -1;
    }
    if (qDebugFlag & DEBUG_DEBUG) {
      for (int32_t i = 0; i < size; ++i) {
        qDebug("%s the %d/%d ext window calced initialized, TR[%" PRId64 ", %" PRId64 ")", 
            pTaskInfo->id.str, i, (int32_t)size, pWin[i].tw.skey, pWin[i].tw.ekey);
      }
    }
  } else {
    for (int32_t i = 0; i < size; ++i) {
      SSTriggerCalcParam* pParam = taosArrayGet(pGroup->pParams, i);

      pWin[i].tw.skey = pParam->wstart;
      pWin[i].tw.ekey = pParam->wend + ((pInfo->triggerType != STREAM_TRIGGER_SLIDING) ? 1 : 0);
      pWin[i].resWinIdx = -1;

      qDebug("%s the %d/%d ext window initialized, TR[%" PRId64 ", %" PRId64 ")", 
          pTaskInfo->id.str, i, (int32_t)size, pWin[i].tw.skey, pWin[i].tw.ekey);
    }
  }

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pTaskInfo), __func__, lino, tstrerror(code));
  }
  
  return code;
}

static void extWinInitDynParamCGrpCtx(SExtWinCalcGrpCtx* pCtx) {
  pCtx->groupId = 0;
  pCtx->curIdx = 0;
  pCtx->lastSKey = INT64_MIN;
  pCtx->lastWinId = -1;
  pCtx->lastWinIdx = -1;
  pCtx->blkWinIdx = -1;
  pCtx->blkScanFlag = -1;
  pCtx->blkWinStartSet = false;
  pCtx->blkWinStartIdx = 0;
  pCtx->blkRowStartIdx = 0;
  pCtx->outWinIdx = 0;
  pCtx->outWinLastIdx = -1;
  pCtx->outWinTotalNum = 0;
  pCtx->outWinNum = 0;
  pCtx->settledThroughWinIdx = -1;
  pCtx->windowDecisionsReady = false;
  pCtx->pWins = NULL;
  pCtx->outWinBufIdx = NULL;
  pCtx->pWindowConditionResults = NULL;
  pCtx->anyRowCached = false;
  pCtx->pAnyRow = NULL;
}

static FORCE_INLINE bool extWinNeedResolvePartitionBlockId(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo,
                                                           const SBlockID* pId) {
  if (pId == NULL || pTaskInfo == NULL || pTaskInfo->pStreamRuntimeInfo == NULL) {
    return false;
  }

  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  return pTaskInfo->execModel != OPTR_EXEC_MODEL_STREAM && pInfo->isMultiGroupCalc && pExtW->calcWithPartition &&
         pInfo->pGroupCalcInfos != NULL;
}

static void extWinResolveBaseGroupIdForPartition(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo,
                                                 SBlockID* pId) {
  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;

  if (pExtW->matchPartitionBy && pId->groupId != 0 && pId->baseGId != pId->groupId &&
      tSimpleHashGet(pInfo->pGroupCalcInfos, &pId->groupId, sizeof(pId->groupId)) != NULL) {
    pId->baseGId = pId->groupId;
    qDebug("%s %s normalize baseGId <- groupId %" PRIu64 " for matched partition keys", GET_TASKID(pTaskInfo),
           EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW), pId->baseGId);
    return;
  }

  if (pId->baseGId != 0) {
    return;
  }

  if (pId->groupId != 0 &&
      tSimpleHashGet(pInfo->pGroupCalcInfos, &pId->groupId, sizeof(pId->groupId)) != NULL) {
    pId->baseGId = pId->groupId;
    qDebug("%s %s normalize baseGId <- groupId %" PRIu64,
           GET_TASKID(pTaskInfo), EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW), pId->groupId);
    return;
  }

  // Compatibility path: some non-stream partitioned external-window plans may
  // still send blocks with only `groupId` or with both ids unset on the outer
  // side, while runtime has exactly one trigger-group from the subquery. In
  // that case we can safely recover `baseGId` from the singleton trigger-group.
  //
  // Typical SQL shape:
  //   select tbname, cast(_wstart as bigint) as ws, cast(ts as bigint) as ts64
  //   from ext_cx_src partition by tbname
  //   external_window((select ts, endtime, mark from ext_cx_win) w);
  //
  // Here the outer query is partitioned (`partition by tbname`), but the
  // subquery has no partition/group clause, so upstream may not fully carry
  // `baseGId` even though there is only one trigger-group to bind against.
  int32_t size = tSimpleHashGetSize(pInfo->pGroupCalcInfos);
  if (size == 1) {
    int32_t iter = 0;
    SSTriggerGroupCalcInfo* pOne = tSimpleHashIterate(pInfo->pGroupCalcInfos, NULL, &iter);
    if (pOne != NULL) {
      pId->baseGId = *(uint64_t*)tSimpleHashGetKey(pOne, NULL);
      qDebug("%s %s normalize baseGId <- single gid %" PRIu64,
             GET_TASKID(pTaskInfo), EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW), pId->baseGId);
    }
  }
}

static void extWinResolveCalcGroupIdForPartition(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo,
                                                 SBlockID* pId) {
  if (pId->groupId == 0 && pId->baseGId != 0) {
    pId->groupId = pId->baseGId;
    qDebug("%s %s normalize groupId <- baseGId %" PRIu64,
           GET_TASKID(pTaskInfo), EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW), pId->groupId);
  }
}

static void extWinResolveBlockIdForPartition(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo, SBlockID* pId) {
  if (!extWinNeedResolvePartitionBlockId(pExtW, pTaskInfo, pId)) {
    return;
  }

  extWinResolveBaseGroupIdForPartition(pExtW, pTaskInfo, pId);
  extWinResolveCalcGroupIdForPartition(pExtW, pTaskInfo, pId);
}

static bool extWinShouldSkipNonStreamPartitionBlock(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo,
                                                    SBlockID* pId) {
  if (!extWinNeedResolvePartitionBlockId(pExtW, pTaskInfo, pId)) {
    return false;
  }

  extWinResolveBlockIdForPartition(pExtW, pTaskInfo, pId);
  return pId->groupId != 0 && (pId->baseGId == 0 || pId->baseGId != pId->groupId);
}

static int32_t extWinSwitchInitCtxs(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, SBlockID* pId) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pTaskInfo->pStreamRuntimeInfo == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  extWinResolveBlockIdForPartition(pExtW, pTaskInfo, pId);
  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  SSTriggerGroupCalcInfo  standalone = {.pParams = pInfo->pStreamPesudoFuncVals,
                                        .pGroupColVals = pInfo->pStreamPartColVals};
  SSTriggerGroupCalcInfo* pGroup = &standalone;
  uint64_t                triggerGroupId = pId->baseGId;
  if (pInfo->isMultiGroupCalc) {
    if (pInfo->pGroupCalcInfos == NULL || triggerGroupId == 0 ||
        (pId->groupId == 0 && pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM && pExtW->calcWithPartition)) {
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
    pGroup = tSimpleHashGet(pInfo->pGroupCalcInfos, &triggerGroupId, sizeof(triggerGroupId));
    if (pGroup == NULL) {
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
  } else if (triggerGroupId == 0) {
    triggerGroupId = UINT64_MAX;
  }

  uint64_t calcGroupId = pExtW->calcWithPartition ? pId->groupId : 0;
  int32_t  code = extWinActivateGroup(pOperator, pExtW, pGroup, triggerGroupId, calcGroupId, true);
  if (!pInfo->isMultiGroupCalc && code == TSDB_CODE_SUCCESS) {
    pExtW->lastTGrpId = pId->baseGId;
  }
  return code;
}

static int32_t extWinBindOutputGroup(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, uint64_t groupId,
                                     uint64_t calcGroupId) {
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  SSTriggerGroupCalcInfo* pGroup = tSimpleHashGet(pStream->pGroupCalcInfos, &groupId, sizeof(groupId));
  if (pGroup == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  int32_t code = extWinActivateGroup(pOperator, pExtW, pGroup, groupId, calcGroupId, false);
  if (code != TSDB_CODE_SUCCESS) return code;

  pExtW->activeGroupPublished = false;
  pExtW->groupOutputComplete = false;
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinBindNextHashOutputGroup(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, bool* pFound) {
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  bool                    strictMultiGroup = extWinUsesStrictMultiGroupProtocol(pOperator);
  *pFound = false;

  if (pExtW->groupScanDone || (strictMultiGroup && (pExtW->groupEogPending || pExtW->activeBaseGId != 0))) {
    return TSDB_CODE_SUCCESS;
  }

  if (!strictMultiGroup) {
    while (true) {
      SSTriggerGroupCalcInfo* pGroup =
          tSimpleHashIterate(pStream->pGroupCalcInfos, pExtW->pLastOutput, &pExtW->lastOutputIter);
      if (pGroup == NULL) {
        pExtW->groupScanDone = true;
        pStream->curGrpCalc = NULL;
        return TSDB_CODE_SUCCESS;
      }

      pExtW->pLastOutput = pGroup;
      uint64_t groupId = *(uint64_t*)tSimpleHashGetKey(pGroup, NULL);
      if (extWinGroupIsFinalized(pExtW, groupId)) {
        continue;
      }

      uint64_t           calcGroupId = 0;
      SExtWinTrigGrpCtx* pTCtx = extWinLookupTriggerGroup(pExtW, groupId);
      if (pExtW->calcWithPartition) {
        calcGroupId = pTCtx == NULL || pTCtx->pCCtx == NULL ? groupId : pTCtx->pCCtx->groupId;
      }
      int32_t code = extWinBindOutputGroup(pOperator, pExtW, groupId, calcGroupId);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      *pFound = true;
      return TSDB_CODE_SUCCESS;
    }
  }

  if (pStream->pGroupCalcOrder == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  uint64_t                groupId = 0;
  SSTriggerGroupCalcInfo* pGroup = NULL;
  int32_t                 code = extWinGetExpectedGroup(pStream, pExtW->groupOrderIdx, &groupId, &pGroup);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pGroup == NULL) {
    pExtW->groupScanDone = true;
    pStream->curGrpCalc = NULL;
    return TSDB_CODE_SUCCESS;
  }

  uint64_t           calcGroupId = 0;
  SExtWinTrigGrpCtx* pTCtx = extWinLookupTriggerGroup(pExtW, groupId);
  if (pExtW->calcWithPartition) {
    calcGroupId = pTCtx == NULL || pTCtx->pCCtx == NULL ? groupId : pTCtx->pCCtx->groupId;
  }
  code = extWinBindOutputGroup(pOperator, pExtW, groupId, calcGroupId);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pExtW->activeBaseGId = groupId;
  *pFound = true;
  return TSDB_CODE_SUCCESS;
}

static void extWinPrepareLegacySettlementBoundary(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                                  SSDataBlock* pBlock) {
  blockDataCleanup(pBlock);
  taosArrayClear(pExtW->pWinRowIdx);
  pBlock->info.version = pOperator->pTaskInfo->version;
  pBlock->info.dataLoad = 1;
  extWinAssignBlockGrpId(pOperator, pExtW, &pBlock->info.id);
  extWinDetachPublishedRuntimeAliases(pOperator->pTaskInfo, pExtW);
  pExtW->boundaryReady = true;
}

static int32_t extWinFinishLegacyOutputGroup(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                             SSDataBlock* pBlock, SSDataBlock** ppRes) {
  if (pBlock->info.rows > 0) {
    pExtW->activeGroupPublished = true;
  }

  if (!pExtW->groupOutputComplete) {
    *ppRes = pBlock->info.rows > 0 ? pBlock : NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = extWinFinalizeGroup(pExtW, pExtW->lastTGrpId);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pOperator->pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM && pBlock->info.rows == 0 &&
      !pExtW->activeGroupPublished) {
    extWinPrepareLegacySettlementBoundary(pOperator, pExtW, pBlock);
    *ppRes = pBlock;
  } else {
    *ppRes = pBlock->info.rows > 0 ? pBlock : NULL;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinFinishOutputGroup(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, SSDataBlock* pBlock,
                                       uint64_t baseGId) {
  if (pOperator == NULL || pExtW == NULL || pBlock == NULL || baseGId == 0 || pExtW->activeBaseGId != baseGId ||
      !pExtW->groupEogPending) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  uint64_t                expectedGid = 0;
  SSTriggerGroupCalcInfo* pGroup = NULL;
  int32_t                 code = extWinGetExpectedGroup(pStream, pExtW->groupOrderIdx, &expectedGid, &pGroup);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pGroup == NULL || expectedGid != baseGId) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  pStream->pStreamBlkWinIdx = NULL;
  extWinBindStreamGroupRuntime(pStream, pGroup, baseGId);
  code = qSetMultiGroupEogBlock(pBlock, baseGId);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pBlock->info.version = pOperator->pTaskInfo->version;
  code = extWinFinalizeGroup(pExtW, baseGId);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  ++pExtW->groupOrderIdx;
  pExtW->activeBaseGId = 0;
  pExtW->groupEogPending = false;
  pExtW->groupOutputComplete = false;
  pExtW->activeGroupPublished = false;
  pExtW->boundaryReady = true;
  if (pExtW->groupOrderIdx >= taosArrayGetSize(pStream->pGroupCalcOrder)) {
    pExtW->groupScanDone = true;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinEmitPendingGroupEog(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, SSDataBlock* pBlock,
                                         SSDataBlock** ppRes, bool* pEmitted) {
  *pEmitted = false;
  if (!pExtW->groupEogPending) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = extWinFinishOutputGroup(pOperator, pExtW, pBlock, pExtW->activeBaseGId);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  *ppRes = pBlock;
  *pEmitted = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinPublishGroupOutput(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, SSDataBlock* pBlock,
                                        SSDataBlock** ppRes) {
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  uint64_t                expectedGid = 0;
  SSTriggerGroupCalcInfo* pGroup = NULL;
  int32_t                 code = extWinGetExpectedGroup(pStream, pExtW->groupOrderIdx, &expectedGid, &pGroup);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pGroup == NULL || expectedGid != pExtW->activeBaseGId) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  extWinBindStreamGroupRuntime(pStream, pGroup, expectedGid);

  if (pBlock->info.rows > 0) {
    pExtW->activeGroupPublished = true;
    if (pExtW->groupOutputComplete) {
      pExtW->groupEogPending = true;
    }
    *ppRes = pBlock;
    return TSDB_CODE_SUCCESS;
  }

  if (!pExtW->groupOutputComplete) {
    *ppRes = NULL;
    return TSDB_CODE_SUCCESS;
  }

  pExtW->groupEogPending = true;
  code = extWinFinishOutputGroup(pOperator, pExtW, pBlock, pExtW->activeBaseGId);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  *ppRes = pBlock;
  return TSDB_CODE_SUCCESS;
}

static void extWinResetResultRows(SExternalWindowOperator* pExtW) {
  SExtWinResultRows* pRows = &pExtW->resultRows;

  extWinCleanupResultRowsState(pExtW);

  // The chunks themselves are NOT freed here: they are reused across resets and are owned by
  // destroyExternalWindowOperatorInfo(). Only the read/write cursors are rewound.
  pRows->resRowsIdx = 0;
  pRows->resRowIdx = 0;
}

static int32_t mergeAlignExtWinSetOutputBuf(SOperatorInfo* pOperator, SResultRowInfo* pResultRowInfo,
                                            const STimeWindow* pWin, SResultRow** pResult,
                                            SExprSupp* pExprSup, SAggSupporter* pAggSup) {
  SMergeAlignedExternalWindowOperator* pMAExtW = pOperator->info;
  if (*pResult == NULL) {
    *pResult = getNewResultRow(pAggSup->pResultBuf, &pAggSup->currentPageId, pAggSup->resultRowSize);
    if (!*pResult) {
      qError("get new resultRow failed, err:%s", tstrerror(terrno));
      return terrno;
    }
    pResultRowInfo->cur = (SResultRowPosition){.pageId = (*pResult)->pageId, .offset = (*pResult)->offset};
  }

  (*pResult)->win = *pWin;
  (*pResult)->winIdx = pMAExtW->curWinIdx;

  return setResultRowInitCtx((*pResult), pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
}

static int32_t mergeAlignExtWinGetWinFromTs(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                            TSKEY ts, STimeWindow** ppWin) {
  SMergeAlignedExternalWindowOperator* pMAExtW = pOperator->info;
  int32_t                              blkWinIdx = TMAX(pMAExtW->curWinIdx, 0);

  // TODO handle desc order
  for (int32_t i = blkWinIdx; i < pExtW->pTGrpCtx->pCCtx->pWins->size; ++i) {
    STimeWindow* pWin = taosArrayGet(pExtW->pTGrpCtx->pCCtx->pWins, i);
    if (ts == pWin->skey) {
      pMAExtW->curWinIdx = i;
      extWinSetCurWinIdx(pOperator, i);
      *ppWin = pWin;
      return TSDB_CODE_SUCCESS;
    } else if (ts < pWin->skey) {
      qError("invalid ts %" PRId64 " for tgrp %" PRIu64 " current window idx %d skey %" PRId64, ts, pExtW->lastTGrpId, i, pWin->skey);
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
  }

  qError("invalid ts %" PRId64 " to find tgrp %" PRIu64 " merge aligned ext window, size:%d", ts,
         pExtW->lastTGrpId, (int32_t)pExtW->pTGrpCtx->pCCtx->pWins->size);

  return TSDB_CODE_STREAM_INTERNAL_ERROR;
}

static int32_t extWinPatchWindowPseudoCols(SExprSupp* pSup, SSDataBlock* pBlock,
                                           const STimeWindow* pWin, int32_t startRow,
                                           int32_t numOfRows) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  for (int32_t i = 0; i < pSup->numOfExprs; ++i) {
    SqlFunctionCtx* pCtx = &pSup->pCtx[i];
    int64_t value = 0;
    int32_t funcType = pCtx->pExpr->pExpr->_function.functionType;
    // Dynamically cloned physical templates do not always retain the
    // isPseudoFunc flag, so identify window pseudo columns by function type.
    if (FUNCTION_TYPE_WSTART == funcType || FUNCTION_TYPE_TWSTART == funcType) {
      value = pWin->skey;
    } else if (FUNCTION_TYPE_WEND == funcType || FUNCTION_TYPE_TWEND == funcType) {
      value = pWin->ekey;
    } else if (FUNCTION_TYPE_WDURATION == funcType || FUNCTION_TYPE_TWDURATION == funcType) {
      value = pWin->ekey - pWin->skey;
    } else {
      continue;
    }

    int32_t          slotId = pSup->pExprInfo[i].base.resSchema.slotId;
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
    QUERY_CHECK_NULL(pCol, code, lino, _return, terrno);
    for (int32_t row = 0; row < numOfRows; ++row) {
      code = colDataSetVal(pCol, startRow + row, (const char*)&value, false);
      QUERY_CHECK_CODE(code, lino, _return);
    }
  }

_return:
  return code;
}

static int32_t mergeAlignExtWinFinalizeResult(SOperatorInfo* pOperator, SResultRowInfo* pResultRowInfo,
                                              SSDataBlock* pResultBlock) {
  int32_t                              code = 0;
  int32_t                              lino = 0;
  SMergeAlignedExternalWindowOperator* pMAExtW = pOperator->info;
  SExternalWindowOperator*             pExtW = pMAExtW->pExtW;
  SExprSupp*                           pSup = &pOperator->exprSupp;
  int32_t                              resultRows = pResultBlock->info.rows;

  finalizeResultRows(pExtW->aggSup.pResultBuf, &pResultRowInfo->cur, pSup, pResultBlock, pOperator->pTaskInfo);
  resultRows = pResultBlock->info.rows - resultRows;

  if (resultRows > 0) {
    TAOS_CHECK_EXIT(extWinAppendWinIdx(pOperator->pTaskInfo, pExtW->pWinRowIdx, pResultBlock,
                                      pMAExtW->curWinIdx, resultRows));
  }

_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

// For vtable COLS merge (isDynWindow): fill skipped windows [fromIdx, toIdx) with NULL rows.
// Each COLS merge source must output exactly numOfWins rows so columns align positionally.
static int32_t mergeAlignExtWinFillEmptyWins(SOperatorInfo* pOperator, SResultRowInfo* pResultRowInfo,
                                              SSDataBlock* pResultBlock, int32_t fromIdx, int32_t toIdx) {
  SMergeAlignedExternalWindowOperator* pMAExtW = pOperator->info;
  SExternalWindowOperator*             pExtW = pMAExtW->pExtW;
  SExprSupp*                           pSup = &pOperator->exprSupp;
  int32_t                              code = 0, lino = 0;

  for (int32_t i = fromIdx; i < toIdx; ++i) {
    STimeWindow* pWin = taosArrayGet(pExtW->pTGrpCtx->pCCtx->pWins, i);
    if (pWin == NULL) continue;

    pMAExtW->curWinIdx = i;
    extWinSetCurWinIdx(pOperator, i);
    TAOS_CHECK_EXIT(mergeAlignExtWinSetOutputBuf(pOperator, pResultRowInfo, pWin, &pMAExtW->pResultRow, pSup, &pExtW->aggSup));
    int32_t resultStart = pResultBlock->info.rows;
    TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, pResultRowInfo, pResultBlock));
    // Empty source windows never run pseudo functions, so their intermediate
    // values remain zero unless the finalized placeholder row is patched here.
    TAOS_CHECK_EXIT(extWinPatchWindowPseudoCols(pSup, pResultBlock, pWin, resultStart,
                                                pResultBlock->info.rows - resultStart));
    resetResultRow(pMAExtW->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));
    pMAExtW->lastFinalizedWinIdx = i;
  }

_exit:
  if (code) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pOperator->pTaskInfo), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t mergeAlignExtWinAggDo(SOperatorInfo* pOperator, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock, SSDataBlock* pResultBlock) {
  SMergeAlignedExternalWindowOperator* pMAExtW = pOperator->info;
  SExternalWindowOperator*             pExtW = pMAExtW->pExtW;

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*     pSup = &pOperator->exprSupp;
  int32_t        code = 0, lino = 0;
  STimeWindow *pWin = NULL;

  int32_t startPos = 0;
  int64_t* tsCols = extWinExtractTsCol(pBlock, pExtW->primaryTsIndex, pTaskInfo);
  TSKEY ts = getStartTsKey(&pBlock->info.window, tsCols);

  pMAExtW->curWinIdx = 0;
  extWinSetCurWinIdx(pOperator, 0);
  code = mergeAlignExtWinGetWinFromTs(pOperator, pExtW, ts, &pWin);
  if (code) {
    qError("failed to get time window for tgrp %" PRIu64 " ts:%" PRId64 ", prim ts index:%d, error:%s", 
      pExtW->lastTGrpId, ts, pExtW->primaryTsIndex, tstrerror(code));
    TAOS_CHECK_EXIT(code);
  }

  int32_t newWinIdx = pMAExtW->curWinIdx;

  if (pMAExtW->curTs != INT64_MIN && pMAExtW->curTs != pWin->skey) {
    TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, pResultRowInfo, pResultBlock));
    pMAExtW->lastFinalizedWinIdx = pMAExtW->curWinIdx;
    resetResultRow(pMAExtW->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));
  }

  if (pExtW->isDynWindow && pMAExtW->lastFinalizedWinIdx + 1 < newWinIdx) {
    TAOS_CHECK_EXIT(mergeAlignExtWinFillEmptyWins(pOperator, pResultRowInfo, pResultBlock, pMAExtW->lastFinalizedWinIdx + 1, newWinIdx));
    extWinSetCurWinIdx(pOperator, newWinIdx);
  }

  TAOS_CHECK_EXIT(mergeAlignExtWinSetOutputBuf(pOperator, pResultRowInfo, pWin, &pMAExtW->pResultRow, pSup, &pExtW->aggSup));

  int32_t currPos = startPos;
  pMAExtW->curTs = pWin->skey;
  
  while (++currPos < pBlock->info.rows) {
    if (tsCols[currPos] == pMAExtW->curTs) continue;

    qDebug("current ts:%" PRId64 ", startPos:%d, currPos:%d, tsCols[currPos]:%" PRId64,
      pMAExtW->curTs, startPos, currPos, tsCols[currPos]); 
    TAOS_CHECK_EXIT(applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pExtW->twAggSup.timeWindowData, startPos,
                                           currPos - startPos, pBlock->info.rows, pSup->numOfExprs));

    TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, pResultRowInfo, pResultBlock));
    pMAExtW->lastFinalizedWinIdx = pMAExtW->curWinIdx;
    resetResultRow(pMAExtW->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));

    TAOS_CHECK_EXIT(mergeAlignExtWinGetWinFromTs(pOperator, pExtW, tsCols[currPos], &pWin));
    newWinIdx = pMAExtW->curWinIdx;

    if (pExtW->isDynWindow && pMAExtW->lastFinalizedWinIdx + 1 < newWinIdx) {
      TAOS_CHECK_EXIT(mergeAlignExtWinFillEmptyWins(pOperator, pResultRowInfo, pResultBlock, pMAExtW->lastFinalizedWinIdx + 1, newWinIdx));
      extWinSetCurWinIdx(pOperator, newWinIdx);
    }

    qDebug("ext window align2 start:%" PRId64 ", end:%" PRId64, pWin->skey, pWin->ekey);
    startPos = currPos;
    
    TAOS_CHECK_EXIT(mergeAlignExtWinSetOutputBuf(pOperator, pResultRowInfo, pWin, &pMAExtW->pResultRow, pSup, &pExtW->aggSup));

    pMAExtW->curTs = pWin->skey;
  }

  code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pExtW->twAggSup.timeWindowData, startPos,
                                         currPos - startPos, pBlock->info.rows, pSup->numOfExprs);

_exit:

  if (code != 0) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
  
  return code;
}

static int32_t mergeAlignExtWinBuildWinRowIdx(SOperatorInfo* pOperator, SSDataBlock* pInput, SSDataBlock* pResult) {
  SExternalWindowOperator* pExtW = extWinGetCoreInfo(pOperator);
  int64_t* tsCols = extWinExtractTsCol(pInput, pExtW->primaryTsIndex, pOperator->pTaskInfo);
  STimeWindow* pWin = NULL;
  int32_t code = 0, lino = 0;
  int64_t prevTs = INT64_MIN;
  
  for (int32_t i = 0; i < pInput->info.rows; ++i) {
    if (prevTs == tsCols[i]) {
      continue;
    }
    
    TAOS_CHECK_EXIT(mergeAlignExtWinGetWinFromTs(pOperator, pExtW, tsCols[i], &pWin));
    TAOS_CHECK_EXIT(extWinAppendWinIdx(pOperator->pTaskInfo, pExtW->pWinRowIdx, pResult, extWinGetCurWinIdx(pOperator), pInput->info.rows - i));

    prevTs = tsCols[i];
  }

_exit:

  if (code != 0) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;  
}

static int32_t mergeAlignExtWinProjectDo(SOperatorInfo* pOperator, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock,
                                            SSDataBlock* pResultBlock) {
  SExternalWindowOperator* pExtW = extWinGetCoreInfo(pOperator);
  SExprSupp*               pExprSup = &pExtW->scalarSupp;
  int32_t                  code = 0, lino = 0;
  
  TAOS_CHECK_EXIT(projectApplyFunctions(pExprSup->pExprInfo, pResultBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL,
                        GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo));

  TAOS_CHECK_EXIT(mergeAlignExtWinBuildWinRowIdx(pOperator, pBlock, pResultBlock));

_exit:

  if (code != 0) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static void mergeAlignExtWinDo(SOperatorInfo* pOperator) {
  SExecTaskInfo*                       pTaskInfo = pOperator->pTaskInfo;
  SMergeAlignedExternalWindowOperator* pMAExtW = pOperator->info;
  SExternalWindowOperator*             pExtW = pMAExtW->pExtW;
  SResultRow*                          pResultRow = NULL;
  int32_t                              code = 0;
  SSDataBlock*                         pRes = pExtW->binfo.pRes;
  SExprSupp*                           pSup = &pOperator->exprSupp;
  SOperatorInfo*                       pDownstream = pOperator->pDownstream[0];
  int32_t                              lino = 0;
  SStreamRuntimeFuncInfo*              pStream = NULL;
  // Ordinary batch queries do not allocate stream runtime state.
  if (pTaskInfo->pStreamRuntimeInfo != NULL) {
    pStream = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  }

  taosArrayClear(pExtW->pWinRowIdx);
  blockDataCleanup(pRes);
  pExtW->mergeGroupComplete = false;

  SSDataBlock* pBlock = NULL;
  while (1) {
    if (pMAExtW->pNewGroup != NULL) {
      pBlock = pMAExtW->pNewGroup;
      pMAExtW->pNewGroup = NULL;
    } else {
      pBlock = QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN == pDownstream->operatorType
                   ? getNextBlockFromDownstream(pOperator, 0)
                   : getNextBlockFromDownstreamRemainDetach(pOperator, 0);
    }

    if (pBlock == NULL) {
      // close last time window
      if (pMAExtW->curTs != INT64_MIN && EEXT_MODE_AGG == pExtW->mode) {
        TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, &pExtW->binfo.resultRowInfo, pRes));
        pMAExtW->lastFinalizedWinIdx = pMAExtW->curWinIdx;
        resetResultRow(pMAExtW->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));
      }
      // fill remaining empty windows with NULL rows (only for virtual table COLS merge)
      if (pExtW->isDynWindow && EEXT_MODE_AGG == pExtW->mode && pExtW->pTGrpCtx && pExtW->pTGrpCtx->pCCtx && pExtW->pTGrpCtx->pCCtx->pWins) {
        int32_t totalWins = taosArrayGetSize(pExtW->pTGrpCtx->pCCtx->pWins);
        if (pMAExtW->lastFinalizedWinIdx + 1 < totalWins) {
          TAOS_CHECK_EXIT(mergeAlignExtWinFillEmptyWins(pOperator, &pExtW->binfo.resultRowInfo, pRes, pMAExtW->lastFinalizedWinIdx + 1, totalWins));
        }
      }
      pExtW->mergeInputDone = true;
      pExtW->mergeGroupComplete = pExtW->lastTGrpId != 0;
      break;
    }

    if (pExtW->isDynWindow) {
      TSKEY skey = pBlock->info.window.skey;
      TSKEY ekey = pBlock->info.window.ekey;
      // A sorted partial-window merge exposes window starts as row timestamps;
      // use its propagated raw-input range so the last window is not truncated.
      if (QUERY_NODE_PHYSICAL_PLAN_MERGE != pDownstream->operatorType || skey > ekey) {
        TAOS_CHECK_EXIT(getTimeWindowOfBlock(pBlock, pExtW->primaryTsIndex, &skey, &ekey));
      }
      pExtW->orgTableTimeRange.skey =
          TMIN(pExtW->orgTableTimeRange.skey, skey);
      pExtW->orgTableTimeRange.ekey =
          TMAX(pExtW->orgTableTimeRange.ekey, ekey);
    }

    extWinResolveBlockIdForPartition(pExtW, pTaskInfo, &pBlock->info.id);
    if (extWinUsesStrictMultiGroupProtocol(pOperator) &&
        (pBlock->info.id.baseGId == 0 || (pExtW->calcWithPartition && pBlock->info.id.groupId == 0) ||
         tSimpleHashGet(pStream->pGroupCalcInfos, &pBlock->info.id.baseGId, sizeof(pBlock->info.id.baseGId)) == NULL)) {
      TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
    }

    bool triggerGroupChanged = pStream != NULL && pStream->isMultiGroupCalc && pExtW->lastTGrpId != 0 &&
                               pExtW->lastTGrpId != pBlock->info.id.baseGId &&
                               !extWinGroupIsFinalized(pExtW, pExtW->lastTGrpId);
    bool calcGroupChanged = pExtW->lastCGrpId != pBlock->info.id.groupId;
    if (triggerGroupChanged || calcGroupChanged) {
      if (pMAExtW->curTs != INT64_MIN && EEXT_MODE_AGG == pExtW->mode) {
        TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, &pExtW->binfo.resultRowInfo, pRes));

        // Group boundary: always clear row state and curTs after finalize to avoid
        // duplicate finalization and cross-group state reuse on next block.

        if (pMAExtW->pResultRow != NULL) {
          resetResultRow(pMAExtW->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));
        }

        pMAExtW->curTs = INT64_MIN;
      }

      if (triggerGroupChanged || pRes->info.rows > 0) {
        pMAExtW->pNewGroup = pBlock;
        pMAExtW->curTs = INT64_MIN;
        pExtW->mergeGroupComplete = triggerGroupChanged;
        break;
      }
    }

    TAOS_CHECK_EXIT(extWinSwitchInitCtxs(pOperator, pExtW, &pBlock->info.id));

    pRes->info.scanFlag = pBlock->info.scanFlag;
    pRes->info.id.groupId = pBlock->info.id.baseGId;  // only keep TGroup ID
    pRes->info.id.baseGId = 0;

    code = setInputDataBlock(pSup, pBlock, pExtW->binfo.inputTsOrder, pBlock->info.scanFlag, true);
    QUERY_CHECK_CODE(code, lino, _exit);

    printDataBlock(pBlock, __func__, "externalwindowAlign", pTaskInfo->id.queryId);

    if (EEXT_MODE_SCALAR == pExtW->mode) {
      TAOS_CHECK_EXIT(mergeAlignExtWinProjectDo(pOperator, &pExtW->binfo.resultRowInfo, pBlock, pRes));
    } else {
      TAOS_CHECK_EXIT(mergeAlignExtWinAggDo(pOperator, &pExtW->binfo.resultRowInfo, pBlock, pRes));
    }

    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      break;
    }
  }

  if (pStream != NULL) {
    pStream->pStreamBlkWinIdx = pExtW->pWinRowIdx;
    extWinPostUpdateStreamRt(pStream, pOperator, pExtW);
  }

_exit:

  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static int32_t mergeAlignExtWinNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SExecTaskInfo*                       pTaskInfo = pOperator->pTaskInfo;
  SMergeAlignedExternalWindowOperator* pMAExtW = pOperator->info;
  SExternalWindowOperator*             pExtW = pMAExtW->pExtW;
  int32_t                              code = 0;
  int32_t lino = 0;
  SColumnInfoData*                     pFilterRes = NULL;
  bool                                 runtimeMultiGroup =
      pTaskInfo->pStreamRuntimeInfo != NULL && pTaskInfo->pStreamRuntimeInfo->funcInfo.isMultiGroupCalc;
  bool strictMultiGroup = extWinUsesStrictMultiGroupProtocol(pOperator);

  TAOS_CHECK_EXIT(extWinValidateMultiGroupProtocolMode(pOperator));

  if (!runtimeMultiGroup || pExtW->lastTGrpId == 0 || extWinGroupIsFinalized(pExtW, pExtW->lastTGrpId)) {
    extWinDetachPublishedRuntimeAliases(pTaskInfo, pExtW);
  }
  pExtW->boundaryReady = false;

  if (pOperator->status == OP_EXEC_DONE && !pOperator->pOperatorGetParam) {
    (*ppRes) = NULL;
    return TSDB_CODE_SUCCESS;
  }

  if (pOperator->pOperatorGetParam) {
    SOperatorParam* pParam = pOperator->pOperatorGetParam;
    SExternalWindowOperatorParam* pExtParam = pParam->value;
    TAOS_CHECK_EXIT(extWinEnsureDynParamOpenCtx(pExtW));
    if (pExtW->pTGrpCtx->pCCtx->pWins) {
      taosArrayDestroy(pExtW->pTGrpCtx->pCCtx->pWins);
    }
    extWinInitDynParamCGrpCtx(pExtW->pTGrpCtx->pCCtx);
    pExtW->pTGrpCtx->pCCtx->pWins = pExtParam->ExtWins;
    pExtParam->ExtWins = NULL;
    taosArrayDestroy(pExtW->pNullOutputSlots);
    pExtW->pNullOutputSlots = pExtParam->pNullOutputSlots;
    pExtParam->pNullOutputSlots = NULL;
    pExtW->isDynWindow = true;
    pExtW->orgTableTimeRange.skey = INT64_MAX;
    pExtW->orgTableTimeRange.ekey = INT64_MIN;
    if (pExtW->hasCountFunc && pExtW->pEmptyInputBlock == NULL) {
      TAOS_CHECK_EXIT(extWinCreateEmptyInputBlock(pOperator, &pExtW->pEmptyInputBlock));
    }
    pMAExtW->curTs = INT64_MIN;
    pMAExtW->curWinIdx = -1;
    pMAExtW->lastFinalizedWinIdx = -1;
    if (pMAExtW->pResultRow != NULL) {
      resetResultRow(pMAExtW->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));
    }
    pOperator->status = OP_NOT_OPENED;

    SOperatorInfo* pDownstream = pOperator->pDownstream[0];
    code = extWinResetCompletedDownstream(pDownstream);
    QUERY_CHECK_CODE(code, lino, _exit);
    if (QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN == pDownstream->operatorType) {
      SExtWinTimeWindow* pFirstWin = taosArrayGet(pExtW->pTGrpCtx->pCCtx->pWins, 0);
      SExtWinTimeWindow* pLastWin = taosArrayGetLast(pExtW->pTGrpCtx->pCCtx->pWins);
      QUERY_CHECK_NULL(pFirstWin, code, lino, _exit, terrno)
      QUERY_CHECK_NULL(pLastWin, code, lino, _exit, terrno)
      STimeWindow window = {.skey = pFirstWin->tw.skey, .ekey = pLastWin->tw.ekey};
      if (!extWinApplyRangeToFederatedBatchParam(pOperator->pDownstreamGetParams[0], window)) {
        freeOperatorParam(pOperator->pDownstreamGetParams[0], OP_GET_PARAM);
        pOperator->pDownstreamGetParams[0] = NULL;
        TAOS_CHECK_EXIT(extWinBuildFederatedRangeParam(&pOperator->pDownstreamGetParams[0], window));
      }
      extWinSetFederatedRuntimeRange(
          pDownstream, window);
    }

    freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
    pOperator->pOperatorGetParam = NULL;
  }

  SSDataBlock* pRes = pExtW->binfo.pRes;
  bool         emitted = false;

  if (strictMultiGroup) {
    TAOS_CHECK_EXIT(extWinEmitPendingGroupEog(pOperator, pExtW, pRes, ppRes, &emitted));
    if (emitted) {
      goto _exit;
    }
  }

  while (true) {
    blockDataCleanup(pRes);
    taosArrayClear(pExtW->pWinRowIdx);

    if (runtimeMultiGroup && pExtW->mergeInputDone) {
      bool found = false;
      TAOS_CHECK_EXIT(extWinBindNextHashOutputGroup(pOperator, pExtW, &found));
      if (!found) {
        setOperatorCompleted(pOperator);
        *ppRes = NULL;
        break;
      }

      pExtW->groupOutputComplete = true;
      if (strictMultiGroup) {
        TAOS_CHECK_EXIT(extWinPublishGroupOutput(pOperator, pExtW, pRes, ppRes));
      } else {
        TAOS_CHECK_EXIT(extWinFinishLegacyOutputGroup(pOperator, pExtW, pRes, ppRes));
      }
      break;
    }

    if (strictMultiGroup && pExtW->activeBaseGId == 0) {
      bool found = false;
      TAOS_CHECK_EXIT(extWinBindNextHashOutputGroup(pOperator, pExtW, &found));
      if (!found) {
        if (pMAExtW->pNewGroup != NULL) {
          TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
        }
        setOperatorCompleted(pOperator);
        *ppRes = NULL;
        break;
      }
    }

    mergeAlignExtWinDo(pOperator);

    if (pRes->info.rows > 0 && pOperator->exprSupp.pFilterInfo != NULL) {
      int32_t        rowsBeforeFilter = pRes->info.rows;
      SColMatchInfo* pMatchInfo = pExtW->fillMatchInfo.pList != NULL ? &pExtW->fillMatchInfo : NULL;
      TAOS_CHECK_EXIT(doFilter(pRes, pOperator->exprSupp.pFilterInfo, pMatchInfo, &pFilterRes));
      if (pRes->info.rows < rowsBeforeFilter) {
        if (pFilterRes != NULL) {
          TAOS_CHECK_EXIT(extWinRebuildWinIdxByFilter(pTaskInfo, pExtW->pWinRowIdx, rowsBeforeFilter, pFilterRes));
        } else {
          taosArrayClear(pExtW->pWinRowIdx);
        }
      }
    }

    if (!runtimeMultiGroup) {
      *ppRes = pRes->info.rows == 0 ? NULL : pRes;
      if (pExtW->mergeInputDone) {
        setOperatorCompleted(pOperator);
      }
      break;
    }

    if (!strictMultiGroup) {
      pRes->info.id.groupId = pExtW->lastTGrpId;
      pRes->info.id.baseGId = 0;
      pRes->info.version = pTaskInfo->version;
      pRes->info.dataLoad = 1;
      pExtW->groupOutputComplete = pExtW->mergeGroupComplete;
      TAOS_CHECK_EXIT(extWinFinishLegacyOutputGroup(pOperator, pExtW, pRes, ppRes));
      if (*ppRes != NULL) {
        break;
      }

      colDataDestroy(pFilterRes);
      taosMemoryFreeClear(pFilterRes);
      continue;
    }

    if (pExtW->activeBaseGId == 0) {
      bool found = false;
      TAOS_CHECK_EXIT(extWinBindNextHashOutputGroup(pOperator, pExtW, &found));
      if (!found) {
        if (pRes->info.rows > 0) {
          TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
        }
        setOperatorCompleted(pOperator);
        *ppRes = NULL;
        break;
      }
    }
    if (pExtW->lastTGrpId != 0 && pExtW->lastTGrpId != pExtW->activeBaseGId) {
      TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
    }

    extWinAssignBlockGrpId(pOperator, pExtW, &pRes->info.id);
    pRes->info.version = pTaskInfo->version;
    pRes->info.dataLoad = 1;
    pExtW->groupOutputComplete = pExtW->mergeGroupComplete || (pExtW->mergeInputDone && pExtW->lastTGrpId == 0);
    TAOS_CHECK_EXIT(extWinPublishGroupOutput(pOperator, pExtW, pRes, ppRes));
    if (*ppRes != NULL) {
      break;
    }

    colDataDestroy(pFilterRes);
    taosMemoryFreeClear(pFilterRes);
  }

  if (*ppRes != NULL && pExtW->isDynWindow) {
    size_t rows = (*ppRes)->info.rows;
    for (int32_t i = 0; rows > 0 && pExtW->pNullOutputSlots != NULL &&
                        i < taosArrayGetSize(pExtW->pNullOutputSlots); ++i) {
      int32_t* pSlotId = taosArrayGet(pExtW->pNullOutputSlots, i);
      if (pSlotId != NULL && *pSlotId >= 0 && *pSlotId < taosArrayGetSize((*ppRes)->pDataBlock)) {
        SColumnInfoData* pCol = taosArrayGet((*ppRes)->pDataBlock, *pSlotId);
        if (pCol != NULL) {
          colDataSetNNULL(pCol, 0, rows);
        }
      }
    }
    (*ppRes)->info.window = pExtW->orgTableTimeRange;
  }

  pExtW->boundaryReady = false;

_exit:
  colDataDestroy(pFilterRes);
  taosMemoryFree(pFilterRes);

  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static int32_t mergeAlignExtWinNextExt(SOperatorInfo* pOperator, SOperatorParam* pParam, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (NULL != pParam) {
    pParam->opType = pOperator->operatorType;
  }
  code = optrDefaultGetNextExtFn(pOperator, pParam, ppRes);
  QUERY_CHECK_CODE(code, lino, _return);
  return code;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t resetMergeAlignedExtWinOperator(SOperatorInfo* pOperator) {
  int32_t                              code = 0, lino = 0;
  SMergeAlignedExternalWindowOperator* pMAExtW = pOperator->info;
  SExternalWindowOperator*             pExtW = pMAExtW->pExtW;
  SExecTaskInfo*                       pTaskInfo = pOperator->pTaskInfo;
  SExternalWindowPhysiNode*            pPhynode = (SExternalWindowPhysiNode*)pOperator->pPhyNode;
  pOperator->status = OP_NOT_OPENED;
  extWinDetachPublishedRuntimeAliases(pTaskInfo, pExtW);
  extWinClearGroupRegistry(pExtW);

  pMAExtW->pResultRow = NULL;
  pMAExtW->pNewGroup = NULL;
  pMAExtW->curTs = INT64_MIN;
  pMAExtW->curWinIdx = -1;
  pMAExtW->lastFinalizedWinIdx = -1;
  pExtW->outWinIdx = 0;
  if (pExtW->pGrpIds != NULL) taosArrayClear(pExtW->pGrpIds);
  if (pExtW->pCTGrpIds != NULL) taosArrayClear(pExtW->pCTGrpIds);
  pExtW->lastGrpIdx = 0;
  extWinClearGroupProgression(pExtW);
  extWinResetResultRows(pExtW);
  pExtW->resWinIdx = 0;
  resetBasicOperatorState(&pExtW->binfo);

  if (pExtW->mode == EEXT_MODE_AGG) {
    TAOS_CHECK_EXIT(resetAggSup(&pOperator->exprSupp, &pExtW->aggSup, pTaskInfo, pPhynode->window.pFuncs, NULL,
                                sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, NULL,
                                &pTaskInfo->storageAPI.functionStore));
    pOperator->exprSupp.hasWindowOrGroup = true;
    pOperator->exprSupp.hasWindow = true;
    for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
      pOperator->exprSupp.pCtx[i].hasWindowOrGroup = true;
      pOperator->exprSupp.pCtx[i].hasWindow = true;
    }
  } else {
    TAOS_CHECK_EXIT(resetExprSupp(&pExtW->scalarSupp, pTaskInfo, pPhynode->window.pProjs, NULL,
                                  &pTaskInfo->storageAPI.functionStore));
  }

  colDataDestroy(&pExtW->twAggSup.timeWindowData);
  TAOS_CHECK_EXIT(initExecTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pTaskInfo->window));

_exit:

  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
  }
  
  return code;
}

#if defined(BUILD_TEST)
int32_t extWinTestGetMergeAlignedAggBufferState(SOperatorInfo* pOperator, int32_t* pPageCount, int32_t* pCurrentPageId,
                                                int32_t* pCurrentPageUsed) {
  if (pOperator == NULL || pOperator->fpSet._nextFn != mergeAlignExtWinNext || pPageCount == NULL ||
      pCurrentPageId == NULL || pCurrentPageUsed == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SMergeAlignedExternalWindowOperator* pMAExtW = pOperator->info;
  SExternalWindowOperator*             pExtW = pMAExtW != NULL ? pMAExtW->pExtW : NULL;
  if (pExtW == NULL || pExtW->mode != EEXT_MODE_AGG || pExtW->aggSup.pResultBuf == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SArray* pPages = getDataBufPagesIdList(pExtW->aggSup.pResultBuf);
  *pPageCount = pPages != NULL ? taosArrayGetSize(pPages) : 0;
  *pCurrentPageId = pExtW->aggSup.currentPageId;
  *pCurrentPageUsed = 0;
  if (*pCurrentPageId >= 0) {
    SFilePage* pPage = getBufPage(pExtW->aggSup.pResultBuf, *pCurrentPageId);
    if (pPage == NULL) {
      return terrno;
    }
    *pCurrentPageUsed = pPage->num;
    releaseBufPage(pExtW->aggSup.pResultBuf, pPage);
  }

  return TSDB_CODE_SUCCESS;
}
#endif

static int32_t extWinBuildFillMatchInfo(SColMatchInfo* pMatchInfo, SNodeList* pFillExprs) {
  if (pFillExprs == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t numFillExprs = LIST_LENGTH(pFillExprs);
  pMatchInfo->matchType = COL_MATCH_FROM_SLOT_ID;
  pMatchInfo->pList = taosArrayInit(numFillExprs, sizeof(SColMatchItem));
  if (pMatchInfo->pList == NULL) {
    return terrno;
  }

  SNode* pFNode = NULL;
  FOREACH(pFNode, pFillExprs) {
    int16_t   dstSlot = -1;
    SDataType dtype = {0};
    int16_t   colId = 0;
    bool      isPk = false;
    bool      found = false;

    if (nodeType(pFNode) == QUERY_NODE_TARGET) {
      STargetNode* pTarget = (STargetNode*)pFNode;
      dstSlot = pTarget->slotId;
      if (nodeType(pTarget->pExpr) == QUERY_NODE_COLUMN) {
        SColumnNode* pCol = (SColumnNode*)pTarget->pExpr;
        dtype = pCol->node.resType;
        colId = pCol->colId;
        isPk = pCol->isPk;
        found = true;
      } else if (pTarget->pExpr != NULL) {
        dtype = ((SExprNode*)pTarget->pExpr)->resType;
        found = true;
      }
    } else if (nodeType(pFNode) == QUERY_NODE_COLUMN) {
      SColumnNode* pCol = (SColumnNode*)pFNode;
      dstSlot = pCol->slotId;
      dtype = pCol->node.resType;
      colId = pCol->colId;
      isPk = pCol->isPk;
      found = true;
    }

    if (found) {
      SColMatchItem c = {.needOutput = true,
                         .colId = colId,
                         .srcSlotId = dstSlot,
                         .dstSlotId = dstSlot,
                         .isPk = isPk,
                         .dataType = dtype};
      void* tmp = taosArrayPush(pMatchInfo->pList, &c);
      if (tmp == NULL) {
        return terrno;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static EFillMode extWinResolveFillMode(EFillMode fillMode, SExecTaskInfo* pTaskInfo) {
  SStreamRuntimeFuncInfo* pStream = GET_STM_RTINFO(pTaskInfo);
  return fillMode == 0 && pStream != NULL &&
                 (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM || pStream->isMultiGroupCalc)
             ? FILL_MODE_NONE
             : fillMode;
}

static bool extWinNeedGroupSort(const SExternalWindowPhysiNode* pPhynode, const SExecTaskInfo* pTaskInfo) {
  const SStreamRuntimeFuncInfo* pStream = GET_STM_RTINFO(pTaskInfo);
  return pPhynode->needGroupSort || (pStream != NULL && pStream->isMultiGroupCalc);
}

int32_t createMergeAlignedExternalWindowOperator(SOperatorInfo* pDownstream, SPhysiNode* pNode,
                                                 SExecTaskInfo* pTaskInfo, SOperatorInfo** ppOptrOut) {
  SExternalWindowPhysiNode* pPhynode = (SExternalWindowPhysiNode*)pNode;
  STimeWindow nonStreamExtWinRange = {.skey = INT64_MAX, .ekey = INT64_MIN};
  int32_t code = 0;
  int32_t lino = 0;
  SMergeAlignedExternalWindowOperator* pMAExtW = taosMemoryCalloc(1, sizeof(SMergeAlignedExternalWindowOperator));
  SOperatorInfo*                       pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  if (pTaskInfo->pStreamRuntimeInfo != NULL){
    pTaskInfo->pStreamRuntimeInfo->funcInfo.withExternalWindow = true;
  }
  pOperator->pPhyNode = pNode;
  if (!pMAExtW || !pOperator) {
    code = terrno;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  pMAExtW->pExtW = taosMemoryCalloc(1, sizeof(SExternalWindowOperator));
  if (!pMAExtW->pExtW) {
    code = terrno;
    goto _error;
  }

  SExternalWindowOperator* pExtW = pMAExtW->pExtW;
  SExprSupp* pSup = &pOperator->exprSupp;
  pSup->hasWindowOrGroup = true;
  pSup->hasWindow = true;
  pMAExtW->curTs = INT64_MIN;
  pMAExtW->curWinIdx = -1;
  pMAExtW->lastFinalizedWinIdx = -1;

  pExtW->primaryTsIndex = ((SColumnNode*)pPhynode->window.pTspk)->slotId;
  pExtW->mode = pPhynode->window.pFuncs ? EEXT_MODE_AGG : EEXT_MODE_SCALAR;
  pPhynode->window.node.inputTsOrder = TSDB_ORDER_ASC;
  pPhynode->window.node.outputTsOrder = pPhynode->window.node.inputTsOrder;
  setOptrBasicInfoOrder(&pExtW->binfo, &pPhynode->window.node);
  pExtW->needGroupSort = extWinNeedGroupSort(pPhynode, pTaskInfo);
  pExtW->calcWithPartition = pPhynode->calcWithPartition;
  pExtW->extWinSplit = pPhynode->extWinSplit;
  pExtW->fillMode = extWinResolveFillMode(pPhynode->extFill.mode, pTaskInfo);
  pExtW->pFillExprs = pPhynode->extFill.pFillExprs;
  pExtW->pFillValues = pPhynode->extFill.pFillValues;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  int32_t num = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pPhynode->window.pFuncs, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pTaskInfo->execModel != OPTR_EXEC_MODEL_STREAM) {
    code = extWinInitNonStreamWindowDataFromBlock(pPhynode, pTaskInfo, &nonStreamExtWinRange);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  if (pExtW->mode == EEXT_MODE_AGG) {
    code = initAggSup(pSup, &pExtW->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str, NULL,
                      &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  } else {
    code = extWinInitExprSupp(&pExtW->scalarSupp, pPhynode->window.pProjs, pTaskInfo);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pNode->pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  code = extWinBuildFillMatchInfo(&pExtW->fillMatchInfo, pPhynode->extFill.pFillExprs);
  QUERY_CHECK_CODE(code, lino, _error);

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhynode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pExtW->binfo, pResBlock);

  pExtW->pWinRowIdx = taosArrayInit(4096, sizeof(int64_t));
  TSDB_CHECK_NULL(pExtW->pWinRowIdx, code, lino, _error, terrno);

  initResultRowInfo(&pExtW->binfo.resultRowInfo);
  code = blockDataEnsureCapacity(pExtW->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  pExtW->isMergeAlignedExtW = true;
  pExtW->pOperator = pOperator;
  
  setOperatorInfo(pOperator, "MergeAlignedExternalWindowOperator", QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW, false, OP_NOT_OPENED, pMAExtW, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, mergeAlignExtWinNext, NULL,
                                         destroyMergeAlignedExternalWindowOperator, optrDefaultBufFn, NULL,
                                         mergeAlignExtWinNextExt, NULL);
  if (pExtW->mode == EEXT_MODE_AGG) {
    setOperatorRowStateCleanupFn(pOperator, cleanupMergeAlignedExternalWindowRowState);
  }

  setOperatorResetStateFn(pOperator, resetMergeAlignedExtWinOperator);

  code = appendDownstream(pOperator, &pDownstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);
  *ppOptrOut = pOperator;
  return code;
  
_error:
  if (pMAExtW) destroyMergeAlignedExternalWindowOperator(pMAExtW);
  destroyOperatorAndDownstreams(pOperator, &pDownstream, 1);
  pTaskInfo->code = code;
  return code;
}

static void extWinResetResultRows(SExternalWindowOperator* pExtW);

static int32_t extWinInitExprSupp(SExprSupp* pExprSupp, SNodeList* pNodeList, SExecTaskInfo* pTaskInfo) {
  if (pNodeList == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t    num = 0;
  SExprInfo* pExprInfo = NULL;
  int32_t    code = createExprInfo(pNodeList, NULL, &pExprInfo, &num);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return initExprSupp(pExprSupp, pExprInfo, num, &pTaskInfo->storageAPI.functionStore);
}

static int32_t resetExternalWindowExprSupp(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo,
                                           SExternalWindowPhysiNode* pPhynode) {
  int32_t code = 0, lino = 0;
  SNodeList* pNodeList = NULL;
  if (pExtW->mode == EEXT_MODE_SCALAR) {
    pNodeList = pPhynode->window.pProjs;
  } else {
    pNodeList = pPhynode->window.pExprs;
  }

  code = resetExprSupp(&pExtW->scalarSupp, pTaskInfo, pNodeList, NULL, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);
  if (pExtW->mode == EEXT_MODE_AGG) {
    code =
        resetExprSupp(&pExtW->projSupp, pTaskInfo, pPhynode->window.pProjs, NULL, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }
  QUERY_CHECK_CODE(code, lino, _error);
  pExtW->lastGrpIdx = INT32_MAX;
  extWinResetResultRows(pExtW);
  return code;
_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    pTaskInfo->code = code;
  }
  return code;
}

static void extWinLogAndResetRequestStat(SExternalWindowOperator* pExtW, const char* pTaskId) {
  qDebug("%s ext window request stat at reset, broadcast scan blocks:%" PRId64 ", rows:%" PRId64
         ", replay blocks:%" PRId64 ", rows:%" PRId64 ", store budget:%" PRId64 ", payload peak estimate:%" PRId64
         ", flush bytes:%" PRId64 ", load bytes:%" PRId64,
         pTaskId, pExtW->stat.broadcastScanBlocks, pExtW->stat.broadcastScanRows, pExtW->stat.broadcastReplayBlocks,
         pExtW->stat.broadcastReplayRows, pExtW->stat.broadcastStoreBudget, pExtW->stat.broadcastPayloadPeak,
         pExtW->stat.broadcastFlushBytes, pExtW->stat.broadcastLoadBytes);

  pExtW->stat.broadcastScanBlocks = 0;
  pExtW->stat.broadcastScanRows = 0;
  pExtW->stat.broadcastReplayBlocks = 0;
  pExtW->stat.broadcastReplayRows = 0;
  pExtW->stat.broadcastStoreBudget = 0;
  memset(&pExtW->stat.broadcastPayloadCurrent, 0, sizeof(pExtW->stat.broadcastPayloadCurrent));
  pExtW->stat.broadcastPayloadPeak = 0;
  pExtW->stat.broadcastFlushBytes = 0;
  pExtW->stat.broadcastLoadBytes = 0;
#if defined(BUILD_TEST)
  pExtW->stat.windowConditionEvals = 0;
#endif
}

static int32_t resetExternalWindowOperator(SOperatorInfo* pOperator) {
  int32_t code = 0, lino = 0;
  SExternalWindowOperator* pExtW = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SExternalWindowPhysiNode* pPhynode = (SExternalWindowPhysiNode*)pOperator->pPhyNode;
  pOperator->status = OP_NOT_OPENED;
  qClearStreamFetchRuntime(pTaskInfo->pStreamRuntimeInfo);

  extWinDetachPublishedRuntimeAliases(pTaskInfo, pExtW);

  SArray** ppRuntimeIdx = NULL;
  if (NULL != pTaskInfo->pStreamRuntimeInfo) {
    ppRuntimeIdx = &pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx;
  }
  extWinRecycleOutputBlkNode(pExtW, ppRuntimeIdx, &pExtW->pLastBlkNode);
  extWinClearGroupRegistry(pExtW);
  extWinLogAndResetRequestStat(pExtW, pTaskInfo->id.str);
#if defined(BUILD_TEST)
  taosArrayClear(pExtW->pBroadcastReplayObservations);
#endif

  // Must precede "resWinIdx = 0": that counter is the bound for the per-row state release, so
  // zeroing it first would leave every DISTINCT set attached to the reused rows unfreed.
  extWinResetResultRows(pExtW);

  pExtW->resWinIdx = 0;
  pExtW->outWinIdx = 0;
  if (pExtW->pGrpIds != NULL) taosArrayClear(pExtW->pGrpIds);
  if (pExtW->pCTGrpIds != NULL) taosArrayClear(pExtW->pCTGrpIds);
  pExtW->lastGrpIdx = 0;
  extWinClearGroupProgression(pExtW);
  pExtW->nonAggInputDone = false;
  pExtW->isDynWindow = false;
  resetBasicOperatorState(&pExtW->binfo);

  if (pExtW->mode == EEXT_MODE_AGG) {
    TAOS_CHECK_EXIT(resetAggSup(&pOperator->exprSupp, &pExtW->aggSup, pTaskInfo, pPhynode->window.pFuncs, NULL,
                                sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, NULL,
                                &pTaskInfo->storageAPI.functionStore));
  }
  TAOS_CHECK_EXIT(resetExternalWindowExprSupp(pExtW, pTaskInfo, pPhynode));
  colDataDestroy(&pExtW->twAggSup.timeWindowData);
  TAOS_CHECK_EXIT(initExecTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pTaskInfo->window));

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", pTaskInfo->id.str, __func__, lino, tstrerror(code));
  }
  
  return code;
}

static EDealRes extWinHasCountLikeFunc(SNode* pNode, void* res) {
  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (fmIsCountLikeFunc(pFunc->funcId) || (pFunc->hasOriginalFunc && fmIsCountLikeFunc(pFunc->originalFuncId))) {
      *(bool*)res = true;
      return DEAL_RES_END;
    }
  }
  return DEAL_RES_CONTINUE;
}


static int32_t extWinCreateEmptyInputBlock(SOperatorInfo* pOperator, SSDataBlock** ppBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SSDataBlock* pBlock = NULL;
  SExternalWindowOperator* pExtW = pOperator->info;

  code = createDataBlock(&pBlock);
  if (code) {
    return code;
  }

  pBlock->info.rows = 1;
  pBlock->info.capacity = 0;

  SExprSupp* pSupps[] = {&pOperator->exprSupp, &pExtW->scalarSupp};
  for (int32_t s = 0; s < 2; ++s) {
    SExprSupp* pSupp = pSupps[s];
    if (pSupp == NULL || pSupp->pExprInfo == NULL) {
      continue;
    }

    for (int32_t i = 0; i < pSupp->numOfExprs; ++i) {
      SColumnInfoData colInfo = {0};
      colInfo.hasNull = true;
      colInfo.info.type = TSDB_DATA_TYPE_NULL;
      colInfo.info.bytes = 1;

      SExprInfo* pOneExpr = &pSupp->pExprInfo[i];
      for (int32_t j = 0; j < pOneExpr->base.numOfParams; ++j) {
        SFunctParam* pFuncParam = &pOneExpr->base.pParam[j];
        if (pFuncParam->type != FUNC_PARAM_TYPE_COLUMN) {
          continue;
        }

        int32_t slotId = pFuncParam->pCol->slotId;
        int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
        if (slotId < numOfCols) {
          continue;
        }

        code = taosArrayEnsureCap(pBlock->pDataBlock, slotId + 1);
        QUERY_CHECK_CODE(code, lino, _end);

        for (int32_t k = numOfCols; k < slotId + 1; ++k) {
          void* tmp = taosArrayPush(pBlock->pDataBlock, &colInfo);
          QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
        }
      }
    }
  }

  code = blockDataEnsureCapacity(pBlock, pBlock->info.rows);
  QUERY_CHECK_CODE(code, lino, _end);

  for (int32_t i = 0; i < blockDataGetNumOfCols(pBlock); ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, 0);
  }
  *ppBlock = pBlock;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
  return code;
}



static int extWinTsWinCompare(const void* pLeft, const void* pRight) {
  int64_t ts = *(int64_t*)pLeft;
  SExtWinTimeWindow* pWin = (SExtWinTimeWindow*)pRight;
  if (ts < pWin->tw.skey) {
    return -1;
  }
  if (ts >= pWin->tw.ekey) {
    return 1;
  }

  return 0;
}


static int32_t extWinGetMultiTbWinFromTs(SOperatorInfo* pOperator, SArray* pWins, int64_t* tsCol, int64_t rowNum, int32_t* startPos) {
  int32_t idx = taosArraySearchIdx(pWins, tsCol, extWinTsWinCompare, TD_EQ);
  if (idx >= 0) {
    *startPos = 0;
    return idx;
  }

  SExtWinTimeWindow* pWin = NULL;
  int32_t w = 0;
  for (int64_t i = 1; i < rowNum; ++i) {
    for (; w < pWins->size; ++w) {
      pWin = TARRAY_GET_ELEM(pWins, w);
      if (tsCol[i] < pWin->tw.skey) {
        break;
      }
      
      if (tsCol[i] < pWin->tw.ekey) {
        *startPos = i;
        return w;
      }
    }
  }

  return -1;
}

static int32_t extWinGetNumOfRows(SExternalWindowOperator* pExtW,
                                  SDataBlockInfo* pInfo, int64_t* tsCol,
                                  int32_t startPos, TSKEY ekey) {
  if (pExtW->inputHasOrder) {
    return getNumOfRowsInTimeWindow(
        pInfo, tsCol, startPos, ekey, binarySearchForKey, NULL,
        pExtW->binfo.inputTsOrder);
  }

  // Timeline fallback consumes an unordered block sequentially. Stop at the
  // first timestamp beyond the current window instead of binary searching it.
  int32_t endPos = startPos + 1;
  while (endPos < pInfo->rows && tsCol[endPos] <= ekey) {
    ++endPos;
  }

  return endPos - startPos;
}

static int32_t extWinGetNoOvlpWin(SOperatorInfo* pOperator, int64_t* tsCol, int32_t* startPos, SDataBlockInfo* pInfo, SExtWinTimeWindow** ppWin, int32_t* winRows) {
  SExternalWindowOperator* pExtW = pOperator->info;
  if ((*startPos) >= pInfo->rows) {
    qDebug("%s %s blk rowIdx %d reach the end, size: %d, skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, *startPos, (int32_t)pInfo->rows);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SExtWinCalcGrpCtx* pCCtx = pExtW->pTGrpCtx->pCCtx;
  
  if (pCCtx->blkWinIdx < 0) {
    pCCtx->blkWinIdx = extWinGetCurWinIdx(pOperator);
  } else {
    pCCtx->blkWinIdx++;
  }

  if (pCCtx->blkWinIdx >= pCCtx->pWins->size) {
    qDebug("%s %s ext win blk idx %d reach the end, size: %d, skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, (int32_t)pCCtx->pWins->size);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }
  
  SExtWinTimeWindow* pWin = taosArrayGet(pCCtx->pWins, pCCtx->blkWinIdx);
  if (tsCol[pInfo->rows - 1] < pWin->tw.skey) {
    qDebug("%s %s block end ts %" PRId64 " is small than curr win %d skey %" PRId64 ", skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[pInfo->rows - 1], pCCtx->blkWinIdx, pWin->tw.skey);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t r = *startPos;

  qDebug("%s %s start to get novlp win from winIdx %d rowIdx %d", GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, r);

  // TODO handle desc order
  for (; pCCtx->blkWinIdx < pCCtx->pWins->size; ++pCCtx->blkWinIdx) {
    pWin = taosArrayGet(pCCtx->pWins, pCCtx->blkWinIdx);
    for (; r < pInfo->rows; ++r) {
      if (tsCol[r] < pWin->tw.skey) {
        continue;
      }

      if (tsCol[r] < pWin->tw.ekey) {
        extWinSetCurWinIdx(pOperator, pCCtx->blkWinIdx);
        *ppWin = pWin;
        *startPos = r;
        *winRows =
            extWinGetNumOfRows(pExtW, pInfo, tsCol, r, pWin->tw.ekey - 1);

        qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
            GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, pWin->tw.skey, pWin->tw.ekey, *winRows, r, tsCol[r], tsCol[r + *winRows - 1]);
        
        return TSDB_CODE_SUCCESS;
      }

      if (!pOperator->pTaskInfo->pStreamRuntimeInfo && tsCol[r] >= pWin->tw.ekey) {
        extWinSetCurWinIdx(pOperator, pCCtx->blkWinIdx);
        *ppWin = pWin;
        *startPos = r;
        *winRows =
            extWinGetNumOfRows(pExtW, pInfo, tsCol, r, pWin->tw.ekey - 1);

        qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk",
               GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, pWin->tw.skey, pWin->tw.ekey, *winRows, r, tsCol[r], tsCol[r + *winRows - 1]);

        return TSDB_CODE_SUCCESS;
      }

      break;
    }

    if (r == pInfo->rows) {
      break;
    }
  }

  qDebug("%s %s no more ext win in block, TR[%" PRId64 ", %" PRId64 "), skip it", 
      GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[0], tsCol[pInfo->rows - 1]);

  *ppWin = NULL;
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinGetOvlpWin(SOperatorInfo* pOperator, int64_t* tsCol, int32_t* startPos, SDataBlockInfo* pInfo, SExtWinTimeWindow** ppWin, int32_t* winRows) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SExtWinCalcGrpCtx* pCCtx = pExtW->pTGrpCtx->pCCtx;

  if (pCCtx->blkWinIdx < 0) {
    pCCtx->blkWinIdx = pCCtx->blkWinStartIdx;
  } else {
    pCCtx->blkWinIdx++;
  }

  if (pCCtx->blkWinIdx >= pCCtx->pWins->size) {
    qDebug("%s %s ext win blk idx %d reach the end, size: %d, skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, (int32_t)pCCtx->pWins->size);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }
  
  SExtWinTimeWindow* pWin = taosArrayGet(pCCtx->pWins, pCCtx->blkWinIdx);
  if (tsCol[pInfo->rows - 1] < pWin->tw.skey) {
    qDebug("%s %s block end ts %" PRId64 " is small than curr win %d skey %" PRId64 ", skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[pInfo->rows - 1], pCCtx->blkWinIdx, pWin->tw.skey);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int64_t r = 0;

  qDebug("%s %s start to get ovlp win from winIdx %d rowIdx %d", GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, pCCtx->blkRowStartIdx);
  
  // TODO handle desc order
  for (; pCCtx->blkWinIdx < pCCtx->pWins->size; ++pCCtx->blkWinIdx) {
    pWin = taosArrayGet(pCCtx->pWins, pCCtx->blkWinIdx);
    for (r = pCCtx->blkRowStartIdx; r < pInfo->rows; ++r) {
      if (tsCol[r] < pWin->tw.skey) {
        pCCtx->blkRowStartIdx = r + 1;
        continue;
      }

      if (tsCol[r] < pWin->tw.ekey) {
        extWinSetCurWinIdx(pOperator, pCCtx->blkWinIdx);
        *ppWin = pWin;
        *startPos = r;
        *winRows =
            extWinGetNumOfRows(pExtW, pInfo, tsCol, r, pWin->tw.ekey - 1);

        qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
            GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, pWin->tw.skey, pWin->tw.ekey, *winRows, (int32_t)r, tsCol[r], tsCol[r + *winRows - 1]);
        
        if ((r + *winRows) < pInfo->rows) {
          pCCtx->blkWinStartIdx = pCCtx->blkWinIdx + 1;
          pCCtx->blkWinStartSet = true;
        }
        
        return TSDB_CODE_SUCCESS;
      }

      break;
    }

    if (r >= pInfo->rows) {
      if (!pCCtx->blkWinStartSet) {
        pCCtx->blkWinStartIdx = pCCtx->blkWinIdx;
      }
      
      break;
    }
  }

  qDebug("%s %s no more ext win in block, TR[%" PRId64 ", %" PRId64 "), skip it", 
      GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[0], tsCol[pInfo->rows - 1]);

  *ppWin = NULL;
  return TSDB_CODE_SUCCESS;
}


static int32_t extWinGetMultiTbNoOvlpWin(SOperatorInfo* pOperator, int64_t* tsCol, int32_t* startPos, SDataBlockInfo* pInfo, SExtWinTimeWindow** ppWin, int32_t* winRows) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SExtWinCalcGrpCtx* pCCtx = pExtW->pTGrpCtx->pCCtx;

  if ((*startPos) >= pInfo->rows) {
    qDebug("%s %s blk rowIdx %d reach the end, size: %d, skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, *startPos, (int32_t)pInfo->rows);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }
  
  if (pCCtx->blkWinIdx < 0) {
    pCCtx->blkWinIdx = extWinGetMultiTbWinFromTs(pOperator, pCCtx->pWins, tsCol, pInfo->rows, startPos);
    if (pCCtx->blkWinIdx < 0) {
      qDebug("%s %s blk TR[%" PRId64 ", %" PRId64 ") not in any win, skip block", 
          GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[0], tsCol[pInfo->rows - 1]);
      *ppWin = NULL;
      return TSDB_CODE_SUCCESS;
    }

    extWinSetCurWinIdx(pOperator, pCCtx->blkWinIdx);
    *ppWin = taosArrayGet(pCCtx->pWins, pCCtx->blkWinIdx);
    *winRows = extWinGetNumOfRows(pExtW, pInfo, tsCol, *startPos,
                                  (*ppWin)->tw.ekey - 1);

    qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
        GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, (*ppWin)->tw.skey, (*ppWin)->tw.ekey, *winRows, *startPos, tsCol[*startPos], tsCol[*startPos + *winRows - 1]);
    
    return TSDB_CODE_SUCCESS;
  } else {
    pCCtx->blkWinIdx++;
  }

  if (pCCtx->blkWinIdx >= pCCtx->pWins->size) {
    qDebug("%s %s ext win blk idx %d reach the end, size: %d, skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, (int32_t)pCCtx->pWins->size);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }
  
  SExtWinTimeWindow* pWin = taosArrayGet(pCCtx->pWins, pCCtx->blkWinIdx);
  if (tsCol[pInfo->rows - 1] < pWin->tw.skey) {
    qDebug("%s %s block end ts %" PRId64 " is small than curr win %d skey %" PRId64 ", skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[pInfo->rows - 1], pCCtx->blkWinIdx, pWin->tw.skey);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t r = *startPos;

  qDebug("%s %s start to get mnovlp win from winIdx %d rowIdx %d", GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, r);

  // TODO handle desc order
  for (; pCCtx->blkWinIdx < pCCtx->pWins->size; ++pCCtx->blkWinIdx) {
    pWin = taosArrayGet(pCCtx->pWins, pCCtx->blkWinIdx);
    for (; r < pInfo->rows; ++r) {
      if (tsCol[r] < pWin->tw.skey) {
        continue;
      }

      if (tsCol[r] < pWin->tw.ekey) {
        extWinSetCurWinIdx(pOperator, pCCtx->blkWinIdx);
        *ppWin = pWin;
        *startPos = r;
        *winRows =
            extWinGetNumOfRows(pExtW, pInfo, tsCol, r, pWin->tw.ekey - 1);

        qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
            GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, pWin->tw.skey, pWin->tw.ekey, *winRows, r, tsCol[r], tsCol[r + *winRows - 1]);
        
        return TSDB_CODE_SUCCESS;
      }

      if (!pOperator->pTaskInfo->pStreamRuntimeInfo && tsCol[r] >= pWin->tw.ekey) {
        extWinSetCurWinIdx(pOperator, pCCtx->blkWinIdx);
        *ppWin = pWin;
        *startPos = r;
        *winRows =
            extWinGetNumOfRows(pExtW, pInfo, tsCol, r, pWin->tw.ekey - 1);

        qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk",
               GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, pWin->tw.skey, pWin->tw.ekey, *winRows, r, tsCol[r], tsCol[r + *winRows - 1]);

        return TSDB_CODE_SUCCESS;
      }

      break;
    }

    if (r == pInfo->rows) {
      break;
    }
  }

  qDebug("%s %s no more ext win in block, TR[%" PRId64 ", %" PRId64 "), skip it", 
      GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[0], tsCol[pInfo->rows - 1]);

  *ppWin = NULL;
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinGetFirstWinFromTs(SOperatorInfo* pOperator, SArray* pWins, int64_t* tsCol,
                                       int64_t rowNum, int32_t* startPos) {
  SExtWinTimeWindow* pWin = NULL;
  int32_t            idx = taosArraySearchIdx(pWins, tsCol, extWinTsWinCompare, TD_EQ);
  if (idx >= 0) {
    for (int i = idx - 1; i >= 0; --i) {
      pWin = TARRAY_GET_ELEM(pWins, i);
      if (extWinTsWinCompare(tsCol, pWin) == 0) {
        idx = i;
      } else {
        break;
      }
    }
    *startPos = 0;
    return idx;
  }

  pWin = NULL;
  int32_t w = 0;
  for (int64_t i = 1; i < rowNum; ++i) {
    for (; w < pWins->size; ++w) {
      pWin = TARRAY_GET_ELEM(pWins, w);
      if (tsCol[i] < pWin->tw.skey) {
        break;
      }

      if (tsCol[i] < pWin->tw.ekey) {
        *startPos = i;
        return w;
      }
    }
  }

  return -1;
}

static int32_t extWinGetMultiTbOvlpWin(SOperatorInfo* pOperator, int64_t* tsCol, int32_t* startPos, SDataBlockInfo* pInfo, SExtWinTimeWindow** ppWin, int32_t* winRows) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SExtWinCalcGrpCtx* pCCtx = pExtW->pTGrpCtx->pCCtx;

  if (pCCtx->blkWinIdx < 0) {
    pCCtx->blkWinIdx = extWinGetFirstWinFromTs(pOperator, pCCtx->pWins, tsCol, pInfo->rows, startPos);
    if (pCCtx->blkWinIdx < 0) {
      qDebug("%s %s blk TR[%" PRId64 ", %" PRId64 ") not in any win, skip block", 
          GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[0], tsCol[pInfo->rows - 1]);
      *ppWin = NULL;
      return TSDB_CODE_SUCCESS;
    }

    extWinSetCurWinIdx(pOperator, pCCtx->blkWinIdx);
    *ppWin = taosArrayGet(pCCtx->pWins, pCCtx->blkWinIdx);
    *winRows = extWinGetNumOfRows(pExtW, pInfo, tsCol, *startPos,
                                  (*ppWin)->tw.ekey - 1);
    
    qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
        GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, (*ppWin)->tw.skey, (*ppWin)->tw.ekey, *winRows, *startPos, tsCol[*startPos], tsCol[*startPos + *winRows - 1]);
    
    return TSDB_CODE_SUCCESS;
  } else {
    pCCtx->blkWinIdx++;
  }

  if (pCCtx->blkWinIdx >= pCCtx->pWins->size) {
    qDebug("%s %s ext win blk idx %d reach the end, size: %d, skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, (int32_t)pCCtx->pWins->size);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }
  
  SExtWinTimeWindow* pWin = taosArrayGet(pCCtx->pWins, pCCtx->blkWinIdx);
  if (tsCol[pInfo->rows - 1] < pWin->tw.skey) {
    qDebug("%s %s block end ts %" PRId64 " is small than curr win %d skey %" PRId64 ", skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[pInfo->rows - 1], pCCtx->blkWinIdx, pWin->tw.skey);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int64_t r = 0;

  qDebug("%s %s start to get movlp win from winIdx %d rowIdx %d", GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, pCCtx->blkRowStartIdx);

  // TODO handle desc order
  for (; pCCtx->blkWinIdx < pCCtx->pWins->size; ++pCCtx->blkWinIdx) {
    pWin = taosArrayGet(pCCtx->pWins, pCCtx->blkWinIdx);
    for (r = pCCtx->blkRowStartIdx; r < pInfo->rows; ++r) {
      if (tsCol[r] < pWin->tw.skey) {
        pCCtx->blkRowStartIdx = r + 1;
        continue;
      }

      if (tsCol[r] < pWin->tw.ekey) {
        extWinSetCurWinIdx(pOperator, pCCtx->blkWinIdx);
        *ppWin = pWin;
        *startPos = r;
        *winRows =
            extWinGetNumOfRows(pExtW, pInfo, tsCol, r, pWin->tw.ekey - 1);

        qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
            GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, pWin->tw.skey, pWin->tw.ekey, *winRows, (int32_t)r, tsCol[r], tsCol[r + *winRows - 1]);
        
        return TSDB_CODE_SUCCESS;
      }

      break;
    }

    if (r >= pInfo->rows) {
      break;
    }
  }

  qDebug("%s %s no more ext win in block, TR[%" PRId64 ", %" PRId64 "), skip it", 
      GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[0], tsCol[pInfo->rows - 1]);

  *ppWin = NULL;
  return TSDB_CODE_SUCCESS;
}

static const char* extWinGetWinFpName(SExternalWindowOperator* pExtW) {
  if (pExtW->getWinFp == extWinGetOvlpWin) {
    return "single_overlap";
  }
  if (pExtW->getWinFp == extWinGetNoOvlpWin) {
    return "single_no_overlap";
  }
  if (pExtW->getWinFp == extWinGetMultiTbOvlpWin) {
    return "multi_overlap";
  }
  if (pExtW->getWinFp == extWinGetMultiTbNoOvlpWin) {
    return "multi_no_overlap";
  }
  return "unknown";
}


static int32_t extWinGetResultRow(SExecTaskInfo* pTaskInfo, SExternalWindowOperator* pExtW, int32_t winIdx, int32_t resultRowSize, SResultRow** ppRes) {
  int32_t code = 0, lino = 0;
  SExtWinResultRows* pRows = &pExtW->resultRows;
  if (pExtW->mode == EEXT_MODE_AGG && resultRowSize != pExtW->aggSup.resultRowSize) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  pRows->rowSizeBytes = pExtW->aggSup.resultRowSize;
  while (true) {
    if (pRows->pResultRows[pRows->resRowsIdx] && pRows->resRowIdx < pRows->resRowSize) {
      break;
    }

    if (NULL == pRows->pResultRows[pRows->resRowsIdx]) {
      // Zero every slot in a newly allocated chunk. The allocation cursor advances before the
      // caller initializes the row, and teardown may run on an error path in that gap. Zeroed
      // entries make cleanup of those not-yet-initialized slots a harmless no-op.
      pRows->pResultRows[pRows->resRowsIdx] =
          taosMemoryCalloc(pRows->resRowSize, resultRowSize);
      TSDB_CHECK_NULL(pRows->pResultRows[pRows->resRowsIdx], code, lino, _exit, terrno);
      pRows->resRowAllcNum += pRows->resRowSize;
      continue;
    }

    // pRows->resRowIdx >= pRows->resRowSize
    
    pRows->resRowIdx = 0;
    pRows->resRowsIdx++;

    if (pRows->resRowsIdx >= pRows->resRowsSize) {
      int32_t oldSize = pRows->resRowsSize;
      pRows->resRowsSize += EXT_WIN_RES_ROWS_ALLOC_SIZE;
      pRows->pResultRows = taosMemoryRealloc(pRows->pResultRows, pRows->resRowsSize * POINTER_BYTES);
      TSDB_CHECK_NULL(pRows->pResultRows, code, lino, _exit, terrno);
      memset(pRows->pResultRows + oldSize, 0, (pRows->resRowsSize - oldSize) * POINTER_BYTES);
    }
  }

  pExtW->pTGrpCtx->pCCtx->outWinTotalNum++;
  //TSDB_CHECK_NULL(taosArrayPush(pExtW->pTGrpCtx->outWinBufIdx, &winIdx), code, lino, _exit, terrno);

  *ppRes = (SResultRow*)((char*)pRows->pResultRows[pRows->resRowsIdx] + pRows->resRowIdx++ * resultRowSize);

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pTaskInfo), __func__, lino, tstrerror(code));
  }

  return code;
}

static FORCE_INLINE SResultRow* extWinGetResultRowByIdx(SExternalWindowOperator* pExtW, int32_t resWinIdx,
                                                         int32_t resultRowSize) {
  SExtWinResultRows* pRows = &pExtW->resultRows;
  int32_t            resRowsIdx = resWinIdx / pRows->resRowSize;
  if (resRowsIdx >= pRows->resRowsSize || pRows->pResultRows[resRowsIdx] == NULL) {
    return NULL;
  }

  return (SResultRow*)((char*)pRows->pResultRows[resRowsIdx] + (resWinIdx % pRows->resRowSize) * resultRowSize);
}

static FORCE_INLINE bool extWinRowHasSourceData(const SResultRow* pRow) {
  return pRow != NULL && pRow->nOrigRows > 0;
}

static bool extWinHasNaturalRows(SExternalWindowOperator* pExtW, SExtWinCalcGrpCtx* pCCtx) {
  if (pCCtx == NULL || pCCtx->pWins == NULL) {
    return false;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pCCtx->pWins); ++i) {
    SExtWinTimeWindow* pWin = TARRAY_GET_ELEM(pCCtx->pWins, i);
    if (pWin->resWinIdx < 0) {
      continue;
    }

    SResultRow* pRow = extWinGetResultRowByIdx(pExtW, pWin->resWinIdx, pExtW->aggSup.resultRowSize);
    if (extWinRowHasSourceData(pRow)) {
      return true;
    }
  }

  return false;
}

static bool extWinShouldEmitAllWindows(SExternalWindowOperator* pExtW, SExtWinCalcGrpCtx* pCCtx) {
  if (pExtW->isDynWindow) {
    return true;
  }

  switch (pExtW->fillMode) {
    case FILL_MODE_NONE:
      return false;
    case FILL_MODE_NULL_F:
    case FILL_MODE_VALUE_F:
      return true;
    case FILL_MODE_PREV:
    case FILL_MODE_NEXT:
    case FILL_MODE_NULL:
    case FILL_MODE_VALUE:
      return extWinHasNaturalRows(pExtW, pCCtx);
    default:
      return false;
  }
}

static FORCE_INLINE bool extWinUsesValueFillMode(EFillMode fillMode) {
  return fillMode == FILL_MODE_VALUE || fillMode == FILL_MODE_VALUE_F;
}

static SResultRow* extWinFindAdjacentFillRow(SExternalWindowOperator* pExtW, SExtWinCalcGrpCtx* pCCtx, int32_t winIdx,
                                             bool next) {
  int32_t start = next ? (winIdx + 1) : (winIdx - 1);
  int32_t end = next ? taosArrayGetSize(pCCtx->pWins) : -1;
  int32_t step = next ? 1 : -1;

  for (int32_t i = start; i != end; i += step) {
    SExtWinTimeWindow* pWin = TARRAY_GET_ELEM(pCCtx->pWins, i);
    if (pWin->resWinIdx < 0) {
      continue;
    }

    SResultRow* pRow = extWinGetResultRowByIdx(pExtW, pWin->resWinIdx, pExtW->aggSup.resultRowSize);
    if (extWinRowHasSourceData(pRow)) {
      return pRow;
    }
  }

  return NULL;
}

static int32_t extWinApplyValueFill(SExternalWindowOperator* pExtW, SSDataBlock* pBlock, int32_t startRow,
                                    int32_t numOfRows) {
  if (pExtW->pFillValues == NULL || pExtW->fillMatchInfo.pList == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeListNode* pValueList = (SNodeListNode*)pExtW->pFillValues;
  int32_t matchNum = taosArrayGetSize(pExtW->fillMatchInfo.pList);
  for (int32_t i = 0; i < matchNum; ++i) {
    SColMatchItem* pItem = taosArrayGet(pExtW->fillMatchInfo.pList, i);
    if (pItem == NULL || !pItem->needOutput || pItem->dstSlotId < 0) {
      continue;
    }

    SValueNode* pValue = (SValueNode*)nodesListGetNode(pValueList->pNodeList, i);
    if (pValue == NULL) {
      continue;
    }

    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pItem->dstSlotId);
    if (pCol == NULL) {
      return terrno;
    }

    void* pData = nodesGetValueFromNode(pValue);
    for (int32_t row = 0; row < numOfRows; ++row) {
      int32_t code = colDataSetValOrCover(pCol, startRow + row, pData, pValue->isNull);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extWinApplyAggPostProjection(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                            SSDataBlock* pBlock, int32_t startRow, int32_t numOfRows) {
  if (pExtW->projSupp.pExprInfo == NULL || pBlock == NULL || pBlock->info.rows <= 0 || numOfRows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (startRow < 0 || startRow + numOfRows > pBlock->info.rows) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Lazy-init a reusable block with the same schema as pBlock to avoid
  // allocating a new block on every call (which happens once per filled row).
  if (pExtW->pProjTmpBlock == NULL) {
    TAOS_CHECK_EXIT(createOneDataBlock(pBlock, false, &pExtW->pProjTmpBlock));
  }
  blockDataCleanup(pExtW->pProjTmpBlock);
  TAOS_CHECK_EXIT(blockDataEnsureCapacity(pExtW->pProjTmpBlock, numOfRows));
  TAOS_CHECK_EXIT(blockDataMergeNRows(pExtW->pProjTmpBlock, pBlock, startRow, numOfRows));

  SSDataBlock* pSlice = pExtW->pProjTmpBlock;
  TAOS_CHECK_EXIT(projectApplyFunctions(pExtW->projSupp.pExprInfo, pSlice, pSlice, pExtW->projSupp.pCtx,
                                        pExtW->projSupp.numOfExprs, NULL,
                                        GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo));

  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  // TODO(perf): only copy back the slots actually written by projSupp, not all columns.
  // Passthrough columns (_wstart, group keys, etc.) are unchanged in pSlice and
  // do not need to be re-written to pBlock.
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, i);
    SColumnInfoData* pSrcCol = taosArrayGet(pSlice->pDataBlock, i);
    if (pDstCol == NULL || pSrcCol == NULL) {
      continue;
    }

    TAOS_CHECK_EXIT(colDataAssignNRows(pDstCol, startRow, pSrcCol, 0, numOfRows));
  }

_exit:
  return code;
}

static int32_t extWinAppendAggFilledRow(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                        SExtWinCalcGrpCtx* pCCtx, SExtWinTimeWindow* pWin, SResultRow* pSrcRow) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SSDataBlock*    pBlock = pExtW->binfo.pRes;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SExprInfo*      pExprInfo = pOperator->exprSupp.pExprInfo;
  int32_t         numOfExprs = pOperator->exprSupp.numOfExprs;
  int32_t*        rowEntryOffset = pOperator->exprSupp.rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;

  // Only take the NULL path when there is no source row at all.
  // Do NOT check pSrcRow->numOfRows here: for NEXT fill, the source row is
  // from a forward window whose doUpdateNumOfRows hasn't been called yet,
  // so numOfRows is still 0 even though the row has valid aggregation results.
  if (pSrcRow == NULL) {
    int32_t startRow = pBlock->info.rows;
    if (pBlock->info.rows + 1 > pBlock->info.capacity) {
      TAOS_CHECK_EXIT(blockDataEnsureCapacity(pBlock, pBlock->info.rows + 1));
    }
    for (int32_t j = 0; j < taosArrayGetSize(pBlock->pDataBlock); ++j) {
      SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, j);
      if (pColInfo != NULL) {
        colDataSetNULL(pColInfo, pBlock->info.rows);
      }
    }

    // Patch pseudo columns (_wstart/_twstart, _wend/_twend) with original window timestamps.
    // Also patch _group_key columns with values from any existing result row in this partition.
    SResultRow* pAnyRow = NULL;
    int32_t     groupKeyIdx = 0;
    for (int32_t j = 0; j < numOfExprs; ++j) {
      int32_t slotId = pExprInfo[j].base.resSchema.slotId;
      SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, slotId);
      if (pColInfo == NULL) continue;

      if (pCtx[j].isPseudoFunc) {
        int32_t funcType = pCtx[j].pExpr->pExpr->_function.functionType;
        if (funcType == FUNCTION_TYPE_WSTART || funcType == FUNCTION_TYPE_TWSTART) {
          TAOS_CHECK_EXIT(colDataSetVal(pColInfo, pBlock->info.rows, (const char*)&pWin->tw.skey, false));
        } else if (funcType == FUNCTION_TYPE_WEND || funcType == FUNCTION_TYPE_TWEND) {
          TAOS_CHECK_EXIT(colDataSetVal(pColInfo, pBlock->info.rows, (const char*)&pWin->tw.ekey, false));
        } else if (funcType == FUNCTION_TYPE_WDURATION || funcType == FUNCTION_TYPE_TWDURATION) {
          int64_t dur = pWin->tw.ekey - pWin->tw.skey;
          TAOS_CHECK_EXIT(colDataSetVal(pColInfo, pBlock->info.rows, (const char*)&dur, false));
        } else if (funcType == FUNCTION_TYPE_EXTERNAL_WINDOW_COLUMN && pTaskInfo->pStreamRuntimeInfo != NULL) {
          // w.mark is window metadata — always show the current window's own value.
          SStreamRuntimeFuncInfo* pStreamInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
          SNodeList* pParamList = pCtx[j].pExpr->base.pParamList;
          if (pParamList != NULL && LIST_LENGTH(pParamList) >= 2) {
            SNode* pSecond = nodesListGetNode(pParamList, 1);
            if (pSecond != NULL && nodeType(pSecond) == QUERY_NODE_VALUE) {
              int32_t placeholderNo = ((SValueNode*)pSecond)->placeholderNo;
              SSTriggerCalcParam* pCurParam = taosArrayGet(pStreamInfo->pStreamPesudoFuncVals, pCCtx->outWinIdx);
              if (pCurParam != NULL && pCurParam->pExternalWindowData != NULL &&
                  placeholderNo >= 0 && placeholderNo < (int32_t)taosArrayGetSize(pCurParam->pExternalWindowData)) {
                SStreamGroupValue* pVal = taosArrayGet(pCurParam->pExternalWindowData, placeholderNo);
                if (pVal != NULL) {
                  if (pVal->isNull) {
                    colDataSetNULL(pColInfo, pBlock->info.rows);
                  } else if (IS_VAR_DATA_TYPE(pVal->data.type) || pVal->data.type == TSDB_DATA_TYPE_DECIMAL) {
                    TAOS_CHECK_EXIT(colDataSetVal(pColInfo, pBlock->info.rows, (const char*)pVal->data.pData, false));
                  } else {
                    TAOS_CHECK_EXIT(colDataSetVal(pColInfo, pBlock->info.rows, (const char*)&pVal->data.val, false));
                  }
                }
              }
            }
          }
        }
      } else if (fmIsGroupKeyFunc(pCtx[j].functionId) || fmisSelectGroupConstValueFunc(pCtx[j].functionId)) {
        bool patched = false;

        if (fmIsGroupKeyFunc(pCtx[j].functionId) && pTaskInfo->pStreamRuntimeInfo != NULL) {
          SStreamRuntimeFuncInfo* pStreamInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
          SArray* pVals = pStreamInfo->pStreamPartColVals;
          if (pStreamInfo->curGrpCalc != NULL && pStreamInfo->curGrpCalc->pGroupColVals != NULL) {
            pVals = pStreamInfo->curGrpCalc->pGroupColVals;
          }

          if (pVals != NULL && groupKeyIdx < taosArrayGetSize(pVals)) {
            SStreamGroupValue* pValue = taosArrayGet(pVals, groupKeyIdx);
            if (pValue != NULL) {
              if (pValue->isNull) {
                colDataSetNULL(pColInfo, pBlock->info.rows);
              } else if (IS_VAR_DATA_TYPE(pValue->data.type) || pValue->data.type == TSDB_DATA_TYPE_DECIMAL) {
                TAOS_CHECK_EXIT(colDataSetVal(pColInfo, pBlock->info.rows, (const char*)pValue->data.pData, false));
              } else {
                TAOS_CHECK_EXIT(colDataSetVal(pColInfo, pBlock->info.rows, (const char*)&pValue->data.val, false));
              }
              patched = true;
            }
          }

          ++groupKeyIdx;
        }

        if (!patched && pAnyRow == NULL) {
          if (!pCCtx->anyRowCached) {
            int32_t numWins = taosArrayGetSize(pCCtx->pWins);
            for (int32_t w = 0; w < numWins; ++w) {
              SExtWinTimeWindow* pW = TARRAY_GET_ELEM(pCCtx->pWins, w);
              if (pW->resWinIdx >= 0) {
                SResultRow* pR = extWinGetResultRowByIdx(pExtW, pW->resWinIdx, pExtW->aggSup.resultRowSize);
                if (pR != NULL && extWinRowHasSourceData(pR)) {
                  pCCtx->pAnyRow = pR;
                  break;
                }
              }
            }
            pCCtx->anyRowCached = true;
          }
          pAnyRow = pCCtx->pAnyRow;
        }

        if (!patched && pAnyRow != NULL) {
          const void* pGroupKeyData = NULL;
          bool        isNull = true;
          if (resultRowGetGroupKeyResult(pAnyRow, j, rowEntryOffset, &pGroupKeyData, &isNull) && !isNull) {
            TAOS_CHECK_EXIT(colDataSetVal(pColInfo, pBlock->info.rows, pGroupKeyData, false));
          }
        }
      }
    }

    pBlock->info.rows += 1;
    TAOS_CHECK_EXIT(extWinApplyAggPostProjection(pOperator, pExtW, pBlock, startRow, 1));
    if (extWinUsesValueFillMode(pExtW->fillMode)) {
      TAOS_CHECK_EXIT(extWinApplyValueFill(pExtW, pBlock, startRow, 1));
    }
    TAOS_CHECK_EXIT(extWinAppendWinIdx(pTaskInfo, pExtW->pWinRowIdx, pBlock, pCCtx->outWinIdx, 1));
    return code;
  }

  // The source row may not have had doUpdateNumOfRows called yet (e.g. NEXT
  // fill copies from a forward window whose numOfRows hasn't been finalized).
  // Finalize the source row directly. Never memcpy a result row here: DISTINCT
  // aggregates own spill sets through their result-row state, and copying that
  // state would create a second cleanup owner and replay the set twice.
  doUpdateNumOfRows(pCtx, pSrcRow, numOfExprs, rowEntryOffset);

  if (pBlock->info.rows + pSrcRow->numOfRows > pBlock->info.capacity) {
    TAOS_CHECK_EXIT(blockDataEnsureCapacity(pBlock, pBlock->info.rows + pSrcRow->numOfRows));
  }

  int32_t startRow = pBlock->info.rows;
  updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pWin->tw, 0);
  TAOS_CHECK_EXIT(copyResultrowToDataBlock(pExprInfo, numOfExprs, pSrcRow, pCtx, pBlock, rowEntryOffset, pTaskInfo));

  pBlock->info.rows += pSrcRow->numOfRows;

  // Window metadata belongs to the filled window, not to the adjacent source.
  for (int32_t j = 0; j < numOfExprs; ++j) {
    if (!pCtx[j].isPseudoFunc) continue;
    int32_t funcType = pCtx[j].pExpr->pExpr->_function.functionType;
    int64_t value = 0;
    bool patch = true;
    if (funcType == FUNCTION_TYPE_WSTART || funcType == FUNCTION_TYPE_TWSTART) {
      value = pWin->tw.skey;
    } else if (funcType == FUNCTION_TYPE_WEND || funcType == FUNCTION_TYPE_TWEND) {
      value = pWin->tw.ekey;
    } else if (funcType == FUNCTION_TYPE_WDURATION || funcType == FUNCTION_TYPE_TWDURATION) {
      value = pWin->tw.ekey - pWin->tw.skey;
    } else if (funcType == FUNCTION_TYPE_EXTERNAL_WINDOW_COLUMN && pTaskInfo->pStreamRuntimeInfo != NULL) {
      patch = false;
      SNodeList* pParamList = pCtx[j].pExpr->base.pParamList;
      if (pParamList != NULL && LIST_LENGTH(pParamList) >= 2) {
        SNode* pSecond = nodesListGetNode(pParamList, 1);
        if (pSecond != NULL && nodeType(pSecond) == QUERY_NODE_VALUE) {
          int32_t placeholderNo = ((SValueNode*)pSecond)->placeholderNo;
          SStreamRuntimeFuncInfo* pStreamInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
          SSTriggerCalcParam* pCurParam = taosArrayGet(pStreamInfo->pStreamPesudoFuncVals, pCCtx->outWinIdx);
          if (pCurParam != NULL && pCurParam->pExternalWindowData != NULL && placeholderNo >= 0 &&
              placeholderNo < (int32_t)taosArrayGetSize(pCurParam->pExternalWindowData)) {
            SStreamGroupValue* pVal = taosArrayGet(pCurParam->pExternalWindowData, placeholderNo);
            SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pExprInfo[j].base.resSchema.slotId);
            if (pVal != NULL && pCol != NULL) {
              const char* pData = IS_VAR_DATA_TYPE(pVal->data.type) || pVal->data.type == TSDB_DATA_TYPE_DECIMAL
                                      ? (const char*)pVal->data.pData
                                      : (const char*)&pVal->data.val;
              for (int32_t row = startRow; row < startRow + pSrcRow->numOfRows; ++row) {
                TAOS_CHECK_EXIT(colDataSetValOrCover(pCol, row, pData, pVal->isNull));
              }
            }
          }
        }
      }
    } else {
      patch = false;
    }
    if (patch) {
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pExprInfo[j].base.resSchema.slotId);
      if (pCol != NULL) {
        for (int32_t row = startRow; row < startRow + pSrcRow->numOfRows; ++row) {
          TAOS_CHECK_EXIT(colDataSetValOrCover(pCol, row, (const char*)&value, false));
        }
      }
    }
  }

  TAOS_CHECK_EXIT(extWinApplyAggPostProjection(pOperator, pExtW, pBlock, startRow, pSrcRow->numOfRows));
  if (extWinUsesValueFillMode(pExtW->fillMode)) {
    TAOS_CHECK_EXIT(extWinApplyValueFill(pExtW, pBlock, startRow, pSrcRow->numOfRows));
  }

  TAOS_CHECK_EXIT(extWinAppendWinIdx(pTaskInfo, pExtW->pWinRowIdx, pBlock, pCCtx->outWinIdx, pSrcRow->numOfRows));

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t extWinInitResRows(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo) {
  int32_t code = 0, lino = 0;
  SExtWinResultRows* pRows = &pExtW->resultRows;

  pRows->resRowsSize = EXT_WIN_RES_ROWS_ALLOC_SIZE;
  pRows->resRowSize = 4096;
  pRows->pResultRows = taosMemoryCalloc(pRows->resRowsSize, POINTER_BYTES);
  TSDB_CHECK_NULL(pRows->pResultRows, code, lino, _exit, terrno);

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pTaskInfo), __func__, lino, tstrerror(code));
  }

  return code;
}


static int32_t extWinAggSetWinOutputBuf(SOperatorInfo* pOperator, SExtWinTimeWindow* win, SExprSupp* pSupp, 
                                     SAggSupporter* pAggSup, SExecTaskInfo* pTaskInfo) {
  int32_t code = 0, lino = 0;
  SResultRow* pResultRow = NULL;
  SExternalWindowOperator* pExtW = (SExternalWindowOperator*)pOperator->info;

  if (win->resWinIdx >= 0) {
    pResultRow = extWinGetResultRowByIdx(pExtW, win->resWinIdx, pAggSup->resultRowSize);
    TSDB_CHECK_NULL(pResultRow, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  } else {
    win->resWinIdx = pExtW->resWinIdx++;
    
    qDebug("set window [%" PRId64 ", %" PRId64 "] outIdx:%d", win->tw.skey, win->tw.ekey, win->resWinIdx);

    TAOS_CHECK_EXIT(extWinGetResultRow(pTaskInfo, pExtW, win->resWinIdx, pAggSup->resultRowSize, &pResultRow));
    
    memset(pResultRow, 0, pAggSup->resultRowSize);

    pResultRow->winIdx = extWinGetCurWinIdx(pOperator);
    TAOS_SET_POBJ_ALIGNED(&pResultRow->win, &win->tw);
  }

  // set time window for current result
  TAOS_CHECK_EXIT(setResultRowInitCtx(pResultRow, pSupp->pCtx, pSupp->numOfExprs, pSupp->rowEntryInfoOffset));

_exit:
  
  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinAggDo(SOperatorInfo* pOperator, int32_t startPos, int32_t forwardRows,
                                  SSDataBlock* pInputBlock) {
  if (pOperator->pTaskInfo->pStreamRuntimeInfo && forwardRows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SExprSupp*               pSup = &pOperator->exprSupp;
  SExternalWindowOperator* pExtW = pOperator->info;
  return applyAggFunctionOnPartialTuples(pOperator->pTaskInfo, pSup->pCtx, &pExtW->twAggSup.timeWindowData, startPos,
                                         forwardRows, pInputBlock->info.rows, pSup->numOfExprs);

}

static int32_t extWinLastWinClosed(SExternalWindowOperator* pExtW, const char* pTaskId, bool* pClosed) {
  *pClosed = false;

  if (pExtW->resWinIdx <= 0 || (pExtW->multiTableMode && !pExtW->inputHasOrder) || pExtW->pTGrpCtx == NULL ||
      pExtW->pTGrpCtx->pCCtx == NULL || pExtW->pTGrpCtx->pCCtx->lastWinIdx < 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pExtW->timeRangeExpr || !pExtW->timeRangeExpr->needCalc) {
    *pClosed = true;
    return TSDB_CODE_SUCCESS;
  }

  int32_t lastWinIdx = pExtW->pTGrpCtx->pCCtx->lastWinIdx;
  int32_t outputBlocks = NULL == pExtW->pOutputBlocks ? 0 : taosArrayGetSize(pExtW->pOutputBlocks);
  if (lastWinIdx >= outputBlocks) {
    qError("%s external window last output index is invalid, lastWinIdx:%d, outputBlocks:%d, blkWinStartIdx:%d",
           pTaskId, lastWinIdx, outputBlocks, pExtW->pTGrpCtx->pCCtx->blkWinStartIdx);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SList* pList = taosArrayGetP(pExtW->pOutputBlocks, lastWinIdx);
  if (NULL == pList) {
    qError("%s external window last output list is null, lastWinIdx:%d, outputBlocks:%d, blkWinStartIdx:%d", pTaskId,
           lastWinIdx, outputBlocks, pExtW->pTGrpCtx->pCCtx->blkWinStartIdx);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  if (0 == listNEles(pList)) {
    *pClosed = true;
    return TSDB_CODE_SUCCESS;
  }

  SListNode* pNode = listTail(pList);
  SArray*    pBlkWinIdx = NULL == pNode ? NULL : *((SArray**)pNode->data + 1);
  if (NULL == pNode || NULL == pBlkWinIdx) {
    qError(
        "%s external window last output block is invalid, lastWinIdx:%d, outputBlocks:%d, list:%p, "
        "listBlocks:%d, node:%p, idx:%p, blkWinStartIdx:%d",
        pTaskId, lastWinIdx, outputBlocks, pList, listNEles(pList), pNode, pBlkWinIdx,
        pExtW->pTGrpCtx->pCCtx->blkWinStartIdx);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int64_t* pIdx = taosArrayGetLast(pBlkWinIdx);
  if (pIdx && *(int32_t*)pIdx < pExtW->pTGrpCtx->pCCtx->blkWinStartIdx) {
    *pClosed = true;
  }

  return TSDB_CODE_SUCCESS;
}

#if defined(BUILD_TEST)
int32_t extWinTestLastWinClosed(SList* pOutputBlocks, bool* pClosed) {
  SExternalWindowOperator extW = {0};
  SExtWinTrigGrpCtx       tCtx = {0};
  SExtWinCalcGrpCtx       cCtx = {0};
  STimeRangeNode          timeRange = {0};
  SArray*                 pLists = taosArrayInit(1, sizeof(SList*));
  if (NULL == pLists) {
    return terrno;
  }

  timeRange.needCalc = true;
  cCtx.lastWinIdx = 0;
  tCtx.pCCtx = &cCtx;
  extW.resWinIdx = 1;
  extW.pTGrpCtx = &tCtx;
  extW.timeRangeExpr = &timeRange;
  extW.pOutputBlocks = pLists;
  if (NULL == taosArrayPush(pLists, &pOutputBlocks)) {
    taosArrayDestroy(pLists);
    return terrno;
  }

  int32_t code = extWinLastWinClosed(&extW, "test", pClosed);
  taosArrayDestroy(pLists);
  return code;
}
#endif

static SList** extWinReserveGetBlockList(SExternalWindowOperator* pExtW, SArray* pOutputBlocks, int32_t winIdx) {
  SList** ppList = NULL;
  if (taosArrayGetSize(pOutputBlocks) > winIdx) {
    ppList = taosArrayGet(pOutputBlocks, winIdx);
    extWinRecycleBlockList(pExtW, ppList);
  } else {
    ppList = taosArrayReserve(pOutputBlocks, 1);
  }

  return ppList;
}

static int32_t extWinEnsureBlockList(SExternalWindowOperator* pExtW, int32_t winIdx, SList** ppList) {
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;

  *ppList = taosArrayGetP(pExtW->pOutputBlocks, winIdx);
  if (*ppList != NULL) {
    return code;
  }

  SList** ppSlot = extWinReserveGetBlockList(pExtW, pExtW->pOutputBlocks, winIdx);
  TSDB_CHECK_NULL(ppSlot, code, lino, _exit, terrno);

  *ppList = tdListNew(POINTER_BYTES * 2);
  TSDB_CHECK_NULL(*ppList, code, lino, _exit, terrno);

  *ppSlot = *ppList;

_exit:
  return code;
}

static int32_t extWinGetWinResBlock(SOperatorInfo* pOperator, int32_t rows, SExtWinTimeWindow* pWin, SSDataBlock** ppRes, SArray** ppIdx) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SList*                   pList = NULL;
  int32_t                  code = TSDB_CODE_SUCCESS, lino = 0;
  bool                     lastWinClosed = false;

  if (pWin->resWinIdx >= 0) {
    TAOS_CHECK_EXIT(extWinEnsureBlockList(pExtW, pWin->resWinIdx, &pList));
  } else {
    TAOS_CHECK_EXIT(extWinLastWinClosed(pExtW, pOperator->pTaskInfo->id.str, &lastWinClosed));
    if (lastWinClosed) {
      pWin->resWinIdx = pExtW->pTGrpCtx->pCCtx->lastWinIdx;
      TAOS_CHECK_EXIT(extWinEnsureBlockList(pExtW, pWin->resWinIdx, &pList));
    } else {
      pWin->resWinIdx = pExtW->resWinIdx++;
      pList = tdListNew(POINTER_BYTES * 2);
      TSDB_CHECK_NULL(pList, code, lino, _exit, terrno);
      SList** ppList = extWinReserveGetBlockList(pExtW, pExtW->pOutputBlocks, pWin->resWinIdx);
      TSDB_CHECK_NULL(ppList, code, lino, _exit, terrno);
      *ppList = pList;
    }
  }

  pExtW->pTGrpCtx->pCCtx->lastWinIdx = pWin->resWinIdx;
  
  TAOS_CHECK_EXIT(extWinGetLastBlockFromList(pOperator, pExtW, pList, rows, ppRes, ppIdx));

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", pOperator->pTaskInfo->id.str, __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinProjectDo(SOperatorInfo* pOperator, SSDataBlock* pInputBlock, int32_t startPos, int32_t rows, SExtWinTimeWindow* pWin) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SExprSupp*               pExprSup = &pExtW->scalarSupp;
  SSDataBlock*             pResBlock = NULL;
  SArray*                  pIdx = NULL;
  SExtWinBlockSnapshot     snapshot = {0};
  int32_t                  overlapCol = -1;
  int32_t                  code = TSDB_CODE_SUCCESS, lino = 0;
  
  TAOS_CHECK_EXIT(extWinGetWinResBlock(pOperator, rows, pWin, &pResBlock, &pIdx));

  snapshot.pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->resWinIdx);
  snapshot.pNode = NULL == snapshot.pList ? NULL : listTail(snapshot.pList);
  snapshot.pPrev = NULL == snapshot.pNode ? NULL : snapshot.pNode->dl_prev_;
  snapshot.pBlock = pResBlock;
  snapshot.pIdx = pIdx;
  snapshot.blockRows = pResBlock->info.rows;
  snapshot.blockCapacity = pResBlock->info.capacity;
  if (!extWinBlockNodeInvariantHolds(snapshot.pList, snapshot.pNode, snapshot.pPrev, pResBlock, pIdx, rows,
                                     &overlapCol)) {
    extWinLogBlockInvariantFailure(pOperator, &snapshot, pWin, startPos, rows, overlapCol, "before-project");
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    lino = __LINE__;
    goto _exit;
  }

  qDebug("%s %s win[%" PRId64 ", %" PRId64 "] got res block %p winRowIdx %p, winOutIdx:%d, capacity:%d", 
      pOperator->pTaskInfo->id.str, __func__, pWin->tw.skey, pWin->tw.ekey, pResBlock, pIdx, pWin->resWinIdx, pResBlock->info.capacity);
  
  if (!pExtW->pTmpBlock) {
    TAOS_CHECK_EXIT(createOneDataBlock(pInputBlock, false, &pExtW->pTmpBlock));
  } else {
    blockDataCleanup(pExtW->pTmpBlock);
  }
  
  TAOS_CHECK_EXIT(blockDataEnsureCapacity(pExtW->pTmpBlock, TMAX(1, rows)));

  qDebug("%s %s start to copy %d rows to tmp blk", pOperator->pTaskInfo->id.str, __func__, rows);
  TAOS_CHECK_EXIT(blockDataMergeNRows(pExtW->pTmpBlock, pInputBlock, startPos, rows));

  qDebug("%s %s start to apply project to tmp blk", pOperator->pTaskInfo->id.str, __func__);
  TAOS_CHECK_EXIT(projectApplyFunctionsWithSelect(pExprSup->pExprInfo, pResBlock, pExtW->pTmpBlock, pExprSup->pCtx,
                                                  pExprSup->numOfExprs, NULL, GET_STM_RTINFO(pOperator->pTaskInfo),
                                                  true, pExprSup->hasIndefRowsFunc, pOperator->pTaskInfo));

  if (!extWinBlockNodeInvariantHolds(snapshot.pList, snapshot.pNode, snapshot.pPrev, pResBlock, pIdx, 0, &overlapCol)) {
    extWinLogBlockInvariantFailure(pOperator, &snapshot, pWin, startPos, rows, overlapCol, "after-project");
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    lino = __LINE__;
    goto _exit;
  }

  // propagate both ids so downstream (e.g., non-agg output/filters) can align with trigger group
  pResBlock->info.id.groupId = pInputBlock->info.id.groupId;
  pResBlock->info.id.baseGId = pInputBlock->info.id.baseGId;

  TAOS_CHECK_EXIT(extWinAppendWinIdx(pOperator->pTaskInfo, pIdx, pResBlock, extWinGetCurWinIdx(pOperator), rows));

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", pOperator->pTaskInfo->id.str, __func__, lino, tstrerror(code));
  } else {
    qDebug("%s %s project succeed", pOperator->pTaskInfo->id.str, __func__);
  }
  
  return code;
}

static int32_t extWinProjectOpen(SOperatorInfo* pOperator, SSDataBlock* pInputBlock) {
  SExternalWindowOperator* pExtW = pOperator->info;
  int64_t*                 tsCol = extWinExtractTsCol(pInputBlock, pExtW->primaryTsIndex, pOperator->pTaskInfo);
  SExtWinTimeWindow*       pWin = NULL;
  bool                     ascScan = pExtW->binfo.inputTsOrder == TSDB_ORDER_ASC;
  int32_t                  startPos = 0, winRows = 0;
  int32_t                  code = TSDB_CODE_SUCCESS, lino = 0;
  
  while (true) {
    TAOS_CHECK_EXIT((*pExtW->getWinFp)(pOperator, tsCol, &startPos, &pInputBlock->info, &pWin, &winRows));
    if (pWin == NULL) {
      break;
    }

    bool pass = true;
    TAOS_CHECK_EXIT(extWinGetWindowDecision(pExtW->pTGrpCtx->pCCtx, extWinGetCurWinIdx(pOperator), &pass));
    if (!pass) {
      startPos += winRows;
      continue;
    }

    qDebug("%s ext window [%" PRId64 ", %" PRId64 ") tgrp %" PRId64 " project start, ascScan:%d, startPos:%d, winRows:%d",
           GET_TASKID(pOperator->pTaskInfo), pWin->tw.skey, pWin->tw.ekey, pExtW->lastTGrpId, ascScan, startPos, winRows);        
    
    TAOS_CHECK_EXIT(extWinProjectDo(pOperator, pInputBlock, startPos, winRows, pWin));
    
    startPos += winRows;
  }
  
_exit:

  if (code) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinIndefRowsDoImpl(SOperatorInfo* pOperator, SSDataBlock* pRes, SSDataBlock* pBlock) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SOptrBasicInfo*     pInfo = &pExtW->binfo;
  SExprSupp*          pSup = &pOperator->exprSupp;
  SExecTaskInfo*      pTaskInfo = pOperator->pTaskInfo;
  int32_t order = pInfo->inputTsOrder;
  int32_t scanFlag = pBlock->info.scanFlag;
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;

  SExprSupp* pScalarSup = &pExtW->scalarSupp;
  if (pScalarSup->pExprInfo != NULL) {
    TAOS_CHECK_EXIT(projectApplyFunctions(pScalarSup->pExprInfo, pBlock, pBlock, pScalarSup->pCtx, pScalarSup->numOfExprs,
                                 pExtW->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo));
  }

  TAOS_CHECK_EXIT(setInputDataBlock(pSup, pBlock, order, scanFlag, false));

  TAOS_CHECK_EXIT(blockDataEnsureCapacity(pRes, pRes->info.rows + pBlock->info.rows));

  TAOS_CHECK_EXIT(projectApplyFunctions(pSup->pExprInfo, pRes, pBlock, pSup->pCtx, pSup->numOfExprs,
                               pExtW->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo));

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", pOperator->pTaskInfo->id.str, __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinIndefRowsSetWinOutputBuf(SExternalWindowOperator* pExtW, SExtWinTimeWindow* win, SExprSupp* pSupp, 
                                     SAggSupporter* pAggSup, SExecTaskInfo* pTaskInfo, bool reset) {
  int32_t code = 0, lino = 0;
  SResultRow* pResultRow = NULL;

  TAOS_CHECK_EXIT(extWinGetResultRow(pTaskInfo, pExtW, win->resWinIdx, pAggSup->resultRowSize, &pResultRow));
  
  qDebug("set indefRows tgrp %" PRIu64 " window [%" PRId64 ", %" PRId64 "] outIdx:%d", pExtW->lastTGrpId, win->tw.skey, win->tw.ekey, win->resWinIdx);

  if (reset) {
    memset(pResultRow, 0, pAggSup->resultRowSize);
  }
  for (int32_t k = 0; k < pSupp->numOfExprs; ++k) {
    SqlFunctionCtx* pCtx = &pSupp->pCtx[k];
    pCtx->pOutput = NULL;
  }

  TAOS_SET_POBJ_ALIGNED(&pResultRow->win, &win->tw);

  // set time window for current result
  TAOS_CHECK_EXIT(setResultRowInitCtx(pResultRow, pSupp->pCtx, pSupp->numOfExprs, pSupp->rowEntryInfoOffset));

_exit:
  
  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinGetSetWinResBlockBuf(SOperatorInfo* pOperator, int32_t rows, SExtWinTimeWindow* pWin, SSDataBlock** ppRes, SArray** ppIdx) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SList*                   pList = NULL;
  int32_t                  code = TSDB_CODE_SUCCESS, lino = 0;
  bool                     lastWinClosed = false;

  if (pWin->resWinIdx >= 0) {
    TAOS_CHECK_EXIT(extWinEnsureBlockList(pExtW, pWin->resWinIdx, &pList));
    TAOS_CHECK_EXIT(extWinIndefRowsSetWinOutputBuf(pExtW, pWin, &pOperator->exprSupp, &pExtW->aggSup, pOperator->pTaskInfo, false));
  } else {
    TAOS_CHECK_EXIT(extWinLastWinClosed(pExtW, pOperator->pTaskInfo->id.str, &lastWinClosed));
    if (lastWinClosed) {
      pWin->resWinIdx = pExtW->pTGrpCtx->pCCtx->lastWinIdx;
      TAOS_CHECK_EXIT(extWinEnsureBlockList(pExtW, pWin->resWinIdx, &pList));
    } else {
      pWin->resWinIdx = pExtW->resWinIdx++;
      pList = tdListNew(POINTER_BYTES * 2);
      TSDB_CHECK_NULL(pList, code, lino, _exit, terrno);
      SList** ppList = extWinReserveGetBlockList(pExtW, pExtW->pOutputBlocks, pWin->resWinIdx);
      TSDB_CHECK_NULL(ppList, code, lino, _exit, terrno);
      *ppList = pList;
    }
    TAOS_CHECK_EXIT(extWinIndefRowsSetWinOutputBuf(pExtW, pWin, &pOperator->exprSupp, &pExtW->aggSup, pOperator->pTaskInfo, true));
  }

  pExtW->pTGrpCtx->pCCtx->lastWinIdx = pWin->resWinIdx;
  
  TAOS_CHECK_EXIT(extWinGetLastBlockFromList(pOperator, pExtW, pList, rows, ppRes, ppIdx));

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", pOperator->pTaskInfo->id.str, __func__, lino, tstrerror(code));
  }

  return code;
}


static int32_t extWinIndefRowsDo(SOperatorInfo* pOperator, SSDataBlock* pInputBlock, int32_t startPos, int32_t rows, SExtWinTimeWindow* pWin) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SSDataBlock*             pResBlock = NULL;
  SArray*                  pIdx = NULL;
  SExtWinBlockSnapshot     snapshot = {0};
  int64_t                  rowsBefore = 0;
  int32_t                  overlapCol = -1;
  int32_t                  code = TSDB_CODE_SUCCESS, lino = 0;
  
  TAOS_CHECK_EXIT(extWinGetSetWinResBlockBuf(pOperator, rows, pWin, &pResBlock, &pIdx));

  snapshot.pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->resWinIdx);
  snapshot.pNode = NULL == snapshot.pList ? NULL : listTail(snapshot.pList);
  snapshot.pPrev = NULL == snapshot.pNode ? NULL : snapshot.pNode->dl_prev_;
  snapshot.pBlock = pResBlock;
  snapshot.pIdx = pIdx;
  snapshot.blockRows = pResBlock->info.rows;
  snapshot.blockCapacity = pResBlock->info.capacity;
  if (!extWinBlockNodeInvariantHolds(snapshot.pList, snapshot.pNode, snapshot.pPrev, pResBlock, pIdx, rows,
                                     &overlapCol)) {
    extWinLogBlockInvariantFailure(pOperator, &snapshot, pWin, startPos, rows, overlapCol, "before-indef-rows");
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    lino = __LINE__;
    goto _exit;
  }

  rowsBefore = pResBlock->info.rows;

  if (!pExtW->pTmpBlock) {
    TAOS_CHECK_EXIT(createOneDataBlock(pInputBlock, false, &pExtW->pTmpBlock));
  } else {
    blockDataCleanup(pExtW->pTmpBlock);
  }
  
  TAOS_CHECK_EXIT(blockDataEnsureCapacity(pExtW->pTmpBlock, TMAX(1, rows)));

  TAOS_CHECK_EXIT(blockDataMergeNRows(pExtW->pTmpBlock, pInputBlock, startPos, rows));
  TAOS_CHECK_EXIT(extWinIndefRowsDoImpl(pOperator, pResBlock, pExtW->pTmpBlock));

  if (!extWinBlockNodeInvariantHolds(snapshot.pList, snapshot.pNode, snapshot.pPrev, pResBlock, pIdx, 0, &overlapCol)) {
    extWinLogBlockInvariantFailure(pOperator, &snapshot, pWin, startPos, rows, overlapCol, "after-indef-rows");
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    lino = __LINE__;
    goto _exit;
  }

  pResBlock->info.id.groupId = pInputBlock->info.id.groupId;
  pResBlock->info.id.baseGId = pInputBlock->info.id.baseGId;

  TAOS_CHECK_EXIT(extWinAppendWinIdx(pOperator->pTaskInfo, pIdx, pResBlock, extWinGetCurWinIdx(pOperator),
                                     (int32_t)(pResBlock->info.rows - rowsBefore)));

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", pOperator->pTaskInfo->id.str, __func__, lino, tstrerror(code));
  }
  
  return code;
}


static int32_t extWinIndefRowsOpen(SOperatorInfo* pOperator, SSDataBlock* pInputBlock) {
  SExternalWindowOperator* pExtW = pOperator->info;
  int64_t*                 tsCol = extWinExtractTsCol(pInputBlock, pExtW->primaryTsIndex, pOperator->pTaskInfo);
  SExtWinTimeWindow*       pWin = NULL;
  bool                     ascScan = pExtW->binfo.inputTsOrder == TSDB_ORDER_ASC;
  int32_t                  startPos = 0, winRows = 0;
  int32_t                  code = TSDB_CODE_SUCCESS, lino = 0;
  
  while (true) {
    TAOS_CHECK_EXIT((*pExtW->getWinFp)(pOperator, tsCol, &startPos, &pInputBlock->info, &pWin, &winRows));
    if (pWin == NULL) {
      break;
    }

    bool pass = true;
    TAOS_CHECK_EXIT(extWinGetWindowDecision(pExtW->pTGrpCtx->pCCtx, extWinGetCurWinIdx(pOperator), &pass));
    if (!pass) {
      startPos += winRows;
      continue;
    }

    qDebug("%s ext window [%" PRId64 ", %" PRId64 ") tgrp %" PRId64 " indefRows start, ascScan:%d, startPos:%d, winRows:%d",
           GET_TASKID(pOperator->pTaskInfo), pWin->tw.skey, pWin->tw.ekey, pExtW->lastTGrpId, ascScan, startPos, winRows);        
    
    TAOS_CHECK_EXIT(extWinIndefRowsDo(pOperator, pInputBlock, startPos, winRows, pWin));
    
    startPos += winRows;
  }
  
_exit:

  if (code) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
  }

  return code;
}


static int32_t extWinNonAggOutputSingleGrpRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, SSDataBlock** ppRes) {
  SExtWinCalcGrpCtx*  pCCtx = pExtW->pTGrpCtx->pCCtx;
  int32_t         numOfWin = taosArrayGetSize(pCCtx->pWins);
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SSDataBlock*    pRes = NULL;

  for (; pCCtx->outWinIdx < numOfWin;) {
    SExtWinTimeWindow* pWin = TARRAY_GET_ELEM(pCCtx->pWins, pCCtx->outWinIdx);
    if (pWin->resWinIdx < 0) {
      ++pCCtx->outWinIdx;
      extWinIncCurWinOutIdx(pOperator);
      continue;
    }

    pCCtx->outWinLastIdx = pWin->resWinIdx;
    SList* pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->resWinIdx);
    if (listNEles(pList) <= 0) {
      ++pCCtx->outWinIdx;
      extWinIncCurWinOutIdx(pOperator);
      continue;
    }

    SListNode* pNode = tdListPopHead(pList);
    pRes = *(SSDataBlock**)pNode->data;
    pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx = *(SArray**)((SArray**)pNode->data + 1);
    pExtW->pLastBlkNode = pNode;

    if (listNEles(pList) <= 0) {
      pCCtx->outWinIdx++;
      extWinIncCurWinOutIdx(pOperator);
    }

    break;
  }

_exit:

  *ppRes = pRes;

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  
  return code;
}

static int32_t extWinRebindCanonicalNonAggOutputGroup(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  uint64_t                expectedGid = 0;
  SSTriggerGroupCalcInfo* pGroup = NULL;
  int32_t                 code = extWinGetExpectedGroup(pStream, pExtW->groupOrderIdx, &expectedGid, &pGroup);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pGroup == NULL || expectedGid == 0 || pExtW->activeBaseGId != expectedGid) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  SExtWinTrigGrpCtx* pTCtx = extWinLookupTriggerGroup(pExtW, expectedGid);
  SExtWinCalcGrpCtx* pCCtx = extWinLookupCalcGroup(pTCtx, 0);
  if (pCCtx == NULL || pCCtx->groupId != 0) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  if (pExtW->pTGrpCtx != pTCtx || pTCtx->pCCtx != pCCtx || pExtW->lastTGrpId != expectedGid || pExtW->lastCGrpId != 0 ||
      pStream->curGrpCalc != pGroup || pStream->groupId != expectedGid) {
    return extWinActivateGroup(pOperator, pExtW, pGroup, expectedGid, 0, false);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinNonAggOutputMultiGrpRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                             SSDataBlock** ppRes) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  SSDataBlock*            pBlock = NULL;
  bool                    emitted = false;

  TAOS_CHECK_EXIT(extWinEmitPendingGroupEog(pOperator, pExtW, pExtW->binfo.pRes, ppRes, &emitted));
  if (emitted) {
    goto _exit;
  }

  if ((pExtW->needGroupSort || pExtW->inputMode == EXT_WIN_INPUT_BROADCAST) && pExtW->calcWithPartition) {
    int32_t groupNum = taosArrayGetSize(pExtW->pCTGrpIds);
    if (pExtW->activeBaseGId == 0) {
      bool found = false;
      TAOS_CHECK_EXIT(extWinBindNextHashOutputGroup(pOperator, pExtW, &found));
      if (!found) {
        *ppRes = NULL;
        goto _exit;
      }
    }

    while (pExtW->lastGrpIdx < groupNum &&
           ((uint64_t*)taosArrayGet(pExtW->pCTGrpIds, pExtW->lastGrpIdx))[1] < pExtW->activeBaseGId) {
      ++pExtW->lastGrpIdx;
    }
    if (pExtW->lastGrpIdx >= groupNum ||
        ((uint64_t*)taosArrayGet(pExtW->pCTGrpIds, pExtW->lastGrpIdx))[1] != pExtW->activeBaseGId) {
      pExtW->groupOutputComplete = true;
    }

    while (pExtW->lastGrpIdx < groupNum) {
      uint64_t* pPair = taosArrayGet(pExtW->pCTGrpIds, pExtW->lastGrpIdx);
      if (pPair[1] != pExtW->activeBaseGId) {
        pExtW->groupOutputComplete = true;
        break;
      }
      if (pExtW->pTGrpCtx == NULL || pExtW->lastTGrpId != pPair[1] || pExtW->lastCGrpId != pPair[0]) {
        TAOS_CHECK_EXIT(extWinBindOutputGroup(pOperator, pExtW, pPair[1], pPair[0]));
      }

      TAOS_CHECK_EXIT(extWinNonAggOutputSingleGrpRes(pOperator, pExtW, &pBlock));
      SExtWinCalcGrpCtx* pCCtx = pExtW->pTGrpCtx->pCCtx;
      bool               calcGroupDone = pCCtx->outWinIdx >= taosArrayGetSize(pCCtx->pWins);
      if (calcGroupDone) {
        ++pExtW->lastGrpIdx;
      }

      bool triggerGroupDone =
          calcGroupDone && (pExtW->lastGrpIdx >= groupNum ||
                            ((uint64_t*)taosArrayGet(pExtW->pCTGrpIds, pExtW->lastGrpIdx))[1] != pPair[1]);
      pExtW->groupOutputComplete = triggerGroupDone;
      if (pBlock != NULL || !calcGroupDone || triggerGroupDone) {
        break;
      }
    }

    if (pBlock == NULL) {
      pBlock = pExtW->binfo.pRes;
      blockDataCleanup(pBlock);
    }
    extWinAssignBlockGrpId(pOperator, pExtW, &pBlock->info.id);
    TAOS_CHECK_EXIT(extWinPublishGroupOutput(pOperator, pExtW, pBlock, ppRes));
    extWinPostUpdateStreamRt(pStream, pOperator, pExtW);
    goto _exit;
  }

  if (pExtW->activeBaseGId == 0) {
    bool found = false;
    TAOS_CHECK_EXIT(extWinBindNextHashOutputGroup(pOperator, pExtW, &found));
    if (!found) {
      goto _exit;
    }
  }

  if (extWinUsesStrictMultiGroupProtocol(pOperator) && pExtW->inputMode == EXT_WIN_INPUT_WINDOW_BOUND &&
      pExtW->mode == EEXT_MODE_SCALAR && !pExtW->calcWithPartition) {
    TAOS_CHECK_EXIT(extWinRebindCanonicalNonAggOutputGroup(pOperator, pExtW));
  }

  TAOS_CHECK_EXIT(extWinNonAggOutputSingleGrpRes(pOperator, pExtW, &pBlock));
  SExtWinCalcGrpCtx* pCCtx = pExtW->pTGrpCtx == NULL ? NULL : pExtW->pTGrpCtx->pCCtx;
  int32_t            numOfWin = pCCtx == NULL ? 0 : taosArrayGetSize(pCCtx->pWins);
  pExtW->groupOutputComplete = pExtW->nonAggInputDone && (pCCtx == NULL || pCCtx->outWinIdx >= numOfWin);

  if (pBlock == NULL) {
    pBlock = pExtW->binfo.pRes;
    blockDataCleanup(pBlock);
  }
  extWinAssignBlockGrpId(pOperator, pExtW, &pBlock->info.id);
  TAOS_CHECK_EXIT(extWinPublishGroupOutput(pOperator, pExtW, pBlock, ppRes));
  extWinPostUpdateStreamRt(pStream, pOperator, pExtW);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    *ppRes = NULL;
  }

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinNonAggOutputLegacyMultiGrpRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                                   SSDataBlock** ppRes) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  SSDataBlock*            pBlock = NULL;

  if ((pExtW->needGroupSort || pExtW->inputMode == EXT_WIN_INPUT_BROADCAST) && pExtW->calcWithPartition) {
    int32_t groupNum = taosArrayGetSize(pExtW->pCTGrpIds);
    if (pExtW->lastGrpIdx >= groupNum) {
      pExtW->groupScanDone = true;
      pStream->curGrpCalc = NULL;
      *ppRes = NULL;
      goto _exit;
    }
    while (pExtW->lastGrpIdx < groupNum) {
      uint64_t* pPair = taosArrayGet(pExtW->pCTGrpIds, pExtW->lastGrpIdx);
      if (pExtW->pTGrpCtx == NULL || pExtW->lastTGrpId != pPair[1] || pExtW->lastCGrpId != pPair[0]) {
        TAOS_CHECK_EXIT(extWinBindOutputGroup(pOperator, pExtW, pPair[1], pPair[0]));
      }

      TAOS_CHECK_EXIT(extWinNonAggOutputSingleGrpRes(pOperator, pExtW, &pBlock));
      SExtWinCalcGrpCtx* pCCtx = pExtW->pTGrpCtx->pCCtx;
      bool               calcGroupDone = pCCtx->outWinIdx >= taosArrayGetSize(pCCtx->pWins);
      if (calcGroupDone) {
        ++pExtW->lastGrpIdx;
      }

      bool triggerGroupDone =
          calcGroupDone && (pExtW->lastGrpIdx >= groupNum ||
                            ((uint64_t*)taosArrayGet(pExtW->pCTGrpIds, pExtW->lastGrpIdx))[1] != pPair[1]);
      pExtW->groupOutputComplete = triggerGroupDone;
      if (pBlock != NULL || !calcGroupDone || triggerGroupDone) {
        break;
      }
    }

    if (pBlock == NULL) {
      pBlock = pExtW->binfo.pRes;
      blockDataCleanup(pBlock);
    }
    extWinAssignBlockGrpId(pOperator, pExtW, &pBlock->info.id);
    TAOS_CHECK_EXIT(extWinFinishLegacyOutputGroup(pOperator, pExtW, pBlock, ppRes));
    extWinPostUpdateStreamRt(pStream, pOperator, pExtW);
    goto _exit;
  }

  if (pStream->curGrpCalc == NULL || extWinGroupIsFinalized(pExtW, pExtW->lastTGrpId)) {
    bool found = false;
    TAOS_CHECK_EXIT(extWinBindNextHashOutputGroup(pOperator, pExtW, &found));
    if (!found) {
      goto _exit;
    }
  }

  TAOS_CHECK_EXIT(extWinNonAggOutputSingleGrpRes(pOperator, pExtW, &pBlock));
  SExtWinCalcGrpCtx* pCCtx = pExtW->pTGrpCtx == NULL ? NULL : pExtW->pTGrpCtx->pCCtx;
  int32_t            numOfWin = pCCtx == NULL ? 0 : taosArrayGetSize(pCCtx->pWins);
  pExtW->groupOutputComplete = pCCtx == NULL || pCCtx->outWinIdx >= numOfWin;

  if (pBlock == NULL) {
    pBlock = pExtW->binfo.pRes;
    blockDataCleanup(pBlock);
  }
  extWinAssignBlockGrpId(pOperator, pExtW, &pBlock->info.id);
  TAOS_CHECK_EXIT(extWinFinishLegacyOutputGroup(pOperator, pExtW, pBlock, ppRes));
  extWinPostUpdateStreamRt(pStream, pOperator, pExtW);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    *ppRes = NULL;
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t extWinNonAggOutputRes(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SExternalWindowOperator* pExtW = pOperator->info;
  int32_t                  numOfWin = pExtW->resWinIdx;
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSDataBlock*             pRes = NULL;
  SStreamRuntimeFuncInfo*  pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;

  if (extWinUsesStrictMultiGroupProtocol(pOperator)) {
    TAOS_CHECK_EXIT(extWinNonAggOutputMultiGrpRes(pOperator, pExtW, &pRes));
  } else if (pStream->isMultiGroupCalc) {
    TAOS_CHECK_EXIT(extWinNonAggOutputLegacyMultiGrpRes(pOperator, pExtW, &pRes));
  } else {
    for (; pExtW->outWinIdx < numOfWin; pExtW->outWinIdx++, extWinIncCurWinOutIdx(pOperator)) {
      SList* pList = taosArrayGetP(pExtW->pOutputBlocks, pExtW->outWinIdx);
      if (listNEles(pList) <= 0) {
        continue;
      }

      SListNode* pNode = tdListPopHead(pList);
      pRes = *(SSDataBlock**)pNode->data;
      pStream->pStreamBlkWinIdx = *(SArray**)((SArray**)pNode->data + 1);
      pExtW->pLastBlkNode = pNode;

      if (listNEles(pList) <= 0) {
        pExtW->outWinIdx++;
        extWinIncCurWinOutIdx(pOperator);
      }

      break;
    }
  }

  if (pRes) {
    qDebug("%s result generated, rows:%" PRId64 , GET_TASKID(pOperator->pTaskInfo), pRes->info.rows);
    pRes->info.version = pOperator->pTaskInfo->version;
    pRes->info.dataLoad = 1;
  } else {
    pStream->pStreamBlkWinIdx = NULL;
    qDebug("%s ext window done", GET_TASKID(pOperator->pTaskInfo));
  }

  *ppRes = pStream->isMultiGroupCalc ? pRes : ((pRes && pRes->info.rows > 0) ? pRes : NULL);

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinAggSettleThrough(SOperatorInfo* pOperator, int32_t endWinIdx, SSDataBlock* pRestoreInput) {
  if (pOperator == NULL || pOperator->info == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  SExternalWindowOperator* pExtW = pOperator->info;
  SExtWinCalcGrpCtx*       pCCtx = pExtW->pTGrpCtx == NULL ? NULL : pExtW->pTGrpCtx->pCCtx;
  if (pCCtx == NULL || pCCtx->pWins == NULL || !pCCtx->windowDecisionsReady) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  int32_t winCount = (int32_t)taosArrayGetSize(pCCtx->pWins);
  if (endWinIdx < -1 || endWinIdx >= winCount) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (endWinIdx <= pCCtx->settledThroughWinIdx) {
    return pRestoreInput == NULL ? TSDB_CODE_SUCCESS
                                 : setInputDataBlock(&pOperator->exprSupp, pRestoreInput, pExtW->binfo.inputTsOrder,
                                                     pRestoreInput->info.scanFlag, true);
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t savedWinIdx = extWinGetCurWinIdx(pOperator);
  bool    emptyInputBound = false;
  for (int32_t i = pCCtx->settledThroughWinIdx + 1; i <= endWinIdx; ++i) {
    bool pass = true;
    code = extWinGetWindowDecision(pCCtx, i, &pass);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }

    SExtWinTimeWindow* pWin = taosArrayGet(pCCtx->pWins, i);
    if (pWin == NULL) {
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      break;
    }
    SResultRow* pRow =
        pWin->resWinIdx < 0 ? NULL : extWinGetResultRowByIdx(pExtW, pWin->resWinIdx, pExtW->aggSup.resultRowSize);
    bool naturalInput = extWinRowHasSourceData(pRow);

    if (!pass || !naturalInput) {
      if (pExtW->fillMode == FILL_MODE_NONE && pExtW->pEmptyInputBlock != NULL) {
        if (!emptyInputBound) {
          code = setInputDataBlock(&pOperator->exprSupp, pExtW->pEmptyInputBlock, pExtW->binfo.inputTsOrder, MAIN_SCAN,
                                   true);
          if (code != TSDB_CODE_SUCCESS) {
            break;
          }
          emptyInputBound = true;
        }
        extWinSetCurWinIdx(pOperator, i);
        code = extWinAggSetWinOutputBuf(pOperator, pWin, &pOperator->exprSupp, &pExtW->aggSup, pOperator->pTaskInfo);
        if (code != TSDB_CODE_SUCCESS) {
          break;
        }
        updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pWin->tw, 1);
        code = extWinAggDo(pOperator, 0, 1, pExtW->pEmptyInputBlock);
        if (code != TSDB_CODE_SUCCESS) {
          break;
        }
      }
    }

    pCCtx->lastWinId = i;
    pCCtx->settledThroughWinIdx = i;
  }

  extWinSetCurWinIdx(pOperator, savedWinIdx);
  if (pRestoreInput != NULL) {
    int32_t restoreCode = setInputDataBlock(&pOperator->exprSupp, pRestoreInput, pExtW->binfo.inputTsOrder,
                                            pRestoreInput->info.scanFlag, true);
    if (code == TSDB_CODE_SUCCESS) {
      code = restoreCode;
    }
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed since %s", pOperator->pTaskInfo->id.str, __func__, tstrerror(code));
  }
  return code;
}

static int32_t extWinAggHandleEmptyWins(SOperatorInfo* pOperator, SSDataBlock* pBlock, bool allRemains,
                                        SExtWinTimeWindow* pWin) {
  (void)pWin;
  SExternalWindowOperator* pExtW = pOperator->info;
  SExtWinCalcGrpCtx*       pCCtx = pExtW->pTGrpCtx == NULL ? NULL : pExtW->pTGrpCtx->pCCtx;
  if (pCCtx == NULL || pCCtx->pWins == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  int32_t endWinIdx = allRemains ? (int32_t)taosArrayGetSize(pCCtx->pWins) - 1 : extWinGetCurWinIdx(pOperator) - 1;
  return extWinAggSettleThrough(pOperator, endWinIdx, pBlock);
}

static int32_t extWinAggOpen(SOperatorInfo* pOperator, SSDataBlock* pInputBlock) {
  SExternalWindowOperator* pExtW = (SExternalWindowOperator*)pOperator->info;
  int32_t                  startPos = 0, winRows = 0;
  int64_t*                 tsCol = extWinExtractTsCol(pInputBlock, pExtW->primaryTsIndex, pOperator->pTaskInfo);
  bool                     ascScan = pExtW->binfo.inputTsOrder == TSDB_ORDER_ASC;
  int32_t                  code = 0, lino = 0;
  SExtWinTimeWindow*       pWin = NULL;
  bool                     scalarCalc = false;

  while (true) {
    TAOS_CHECK_EXIT((*pExtW->getWinFp)(pOperator, tsCol, &startPos, &pInputBlock->info, &pWin, &winRows));
    if (pWin == NULL) {
      break;
    }

    TAOS_CHECK_EXIT(extWinAggHandleEmptyWins(pOperator, pInputBlock, false, pWin));

    bool pass = true;
    TAOS_CHECK_EXIT(extWinGetWindowDecision(pExtW->pTGrpCtx->pCCtx, extWinGetCurWinIdx(pOperator), &pass));
    if (!pass) {
      startPos += winRows;
      continue;
    }

    qDebug("%s ext window [%" PRId64 ", %" PRId64 ") tgrp %" PRIu64 " cgrp %" PRIu64 " agg start, ascScan:%d, startPos:%d, winRows:%d",
           GET_TASKID(pOperator->pTaskInfo), pWin->tw.skey, pWin->tw.ekey, pExtW->lastTGrpId, pExtW->lastCGrpId, ascScan, startPos, winRows);        

    if (!scalarCalc) {
      if (pExtW->scalarSupp.pExprInfo) {
        SExprSupp* pScalarSup = &pExtW->scalarSupp;
        TAOS_CHECK_EXIT(projectApplyFunctions(pScalarSup->pExprInfo, pInputBlock, pInputBlock, pScalarSup->pCtx, pScalarSup->numOfExprs,
                                     pExtW->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo));
      }

      scalarCalc = true;
    }

    TAOS_CHECK_EXIT(
        extWinAggSetWinOutputBuf(pOperator, pWin, &pOperator->exprSupp, &pExtW->aggSup, pOperator->pTaskInfo));

    updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pWin->tw, 1);
    TAOS_CHECK_EXIT(extWinAggDo(pOperator, startPos, winRows, pInputBlock));

    SResultRow* pCurRow = extWinGetResultRowByIdx(pExtW, pWin->resWinIdx, pExtW->aggSup.resultRowSize);
    if (pCurRow) {
      pCurRow->nOrigRows += winRows;
    }
    
    pExtW->pTGrpCtx->pCCtx->lastSKey = pWin->tw.skey;
    pExtW->pTGrpCtx->pCCtx->lastEKey = pWin->tw.ekey;
    pExtW->pTGrpCtx->pCCtx->lastWinId = extWinGetCurWinIdx(pOperator);
    startPos += winRows;
  }

_exit:

  if (code) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinAggOutputSingleCGrpRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, bool* grpDone) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExprInfo*         pExprInfo = pOperator->exprSupp.pExprInfo;
  int32_t            numOfExprs = pOperator->exprSupp.numOfExprs;
  int32_t*           rowEntryOffset = pOperator->exprSupp.rowEntryInfoOffset;
  SqlFunctionCtx*    pCtx = pOperator->exprSupp.pCtx;
  SSDataBlock*       pBlock = pExtW->binfo.pRes;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SExtWinTrigGrpCtx* pTGrpCtx = pExtW->pTGrpCtx;
  if (!pExtW->pTGrpCtx) {
    return TSDB_CODE_SUCCESS;
  }
  SExtWinCalcGrpCtx* pCCtx = pTGrpCtx->pCCtx;
  int32_t            numOfWin = taosArrayGetSize(pCCtx->pWins);

  if (pExtW->fillMode == FILL_MODE_NONE) {
    for (; pCCtx->outWinIdx < numOfWin && (pExtW->isDynWindow || pCCtx->outWinNum < pCCtx->outWinTotalNum);
         pCCtx->outWinIdx += 1) {
      SExtWinTimeWindow* pWin = TARRAY_GET_ELEM(pCCtx->pWins, pCCtx->outWinIdx);
      bool               emptyWin = false;

      if (pWin->resWinIdx < 0) {
        emptyWin = true;
      } else {
        SResultRow* pRow = extWinGetResultRowByIdx(pExtW, pWin->resWinIdx, pExtW->aggSup.resultRowSize);
        doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);
        if (pRow->numOfRows == 0) {
          emptyWin = true;
        }
      }

      if (emptyWin) {
        if (!pExtW->isDynWindow) {
          continue;
        }
        if (pBlock->info.rows + 1 > pBlock->info.capacity) {
          uint32_t newSize = pBlock->info.rows + 1 + numOfWin - pCCtx->outWinIdx;
          TAOS_CHECK_EXIT(blockDataEnsureCapacity(pBlock, newSize));
        }
        for (int32_t j = 0; j < taosArrayGetSize(pBlock->pDataBlock); ++j) {
          SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, j);
          if (pColInfo) {
            colDataSetNULL(pColInfo, pBlock->info.rows);
          }
        }
        TAOS_CHECK_EXIT(extWinPatchWindowPseudoCols(&pOperator->exprSupp, pBlock, &pWin->tw,
                                                    pBlock->info.rows, 1));
        pBlock->info.rows += 1;
        pCCtx->outWinNum++;
        TAOS_CHECK_EXIT(extWinAppendWinIdx(pOperator->pTaskInfo, pExtW->pWinRowIdx, pBlock, pCCtx->outWinIdx, 1));

        if (pBlock->info.rows >= pOperator->resultInfo.threshold) {
          ++pCCtx->outWinIdx;
          break;
        }
        continue;
      }

      pCCtx->outWinNum++;
      SResultRow* pRow = extWinGetResultRowByIdx(pExtW, pWin->resWinIdx, pExtW->aggSup.resultRowSize);
      if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
        uint32_t newSize =
            pBlock->info.rows + pRow->numOfRows + (pExtW->isDynWindow ? numOfWin - pCCtx->outWinIdx
                                                                       : pCCtx->outWinTotalNum - pCCtx->outWinNum);
        TAOS_CHECK_EXIT(blockDataEnsureCapacity(pBlock, newSize));
        qDebug("datablock capacity not sufficient, expand to required:%d, current capacity:%d, %s", newSize,
               pBlock->info.capacity, GET_TASKID(pTaskInfo));
      }

      updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pRow->win, 0);
      TAOS_CHECK_EXIT(copyResultrowToDataBlock(pExprInfo, numOfExprs, pRow, pCtx, pBlock, rowEntryOffset, pTaskInfo));

      int32_t startRow = pBlock->info.rows;
      pBlock->info.rows += pRow->numOfRows;
      TAOS_CHECK_EXIT(extWinApplyAggPostProjection(pOperator, pExtW, pBlock, startRow, pRow->numOfRows));
      TAOS_CHECK_EXIT(extWinAppendWinIdx(pOperator->pTaskInfo, pExtW->pWinRowIdx, pBlock, pRow->winIdx, pRow->numOfRows));

      if (pBlock->info.rows >= pOperator->resultInfo.threshold) {
        ++pCCtx->outWinIdx;
        break;
      }
    }

    if (grpDone) {
      *grpDone = pExtW->isDynWindow ? (pCCtx->outWinIdx >= numOfWin)
                                    : (pCCtx->outWinIdx >= numOfWin || pCCtx->outWinNum >= pCCtx->outWinTotalNum);
    }

    return code;
  }

  bool               emitAllWins = extWinShouldEmitAllWindows(pExtW, pCCtx);

  // For vtable COLS merge (isDynWindow), iterate all windows including empty ones to output NULL placeholder rows;
  // for normal queries, stop at outWinTotalNum (windows with actual results).
  for (; pCCtx->outWinIdx < numOfWin && (emitAllWins || pCCtx->outWinNum < pCCtx->outWinTotalNum); pCCtx->outWinIdx += 1) {
    SExtWinTimeWindow* pWin = TARRAY_GET_ELEM(pCCtx->pWins, pCCtx->outWinIdx);
    bool emptyWin = false;
    SResultRow* pRow = NULL;

    if (pWin->resWinIdx < 0) {
      emptyWin = true;
    } else {
      pRow = extWinGetResultRowByIdx(pExtW, pWin->resWinIdx, pExtW->aggSup.resultRowSize);
      doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);
      if (!extWinRowHasSourceData(pRow)) {
        emptyWin = true;
      }
    }

    if (emptyWin) {
      if (!emitAllWins) {
        continue;
      }

      SResultRow* pFillRow = NULL;
      if (pExtW->fillMode == FILL_MODE_PREV) {
        pFillRow = extWinFindAdjacentFillRow(pExtW, pCCtx, pCCtx->outWinIdx, false);
      } else if (pExtW->fillMode == FILL_MODE_NEXT) {
        pFillRow = extWinFindAdjacentFillRow(pExtW, pCCtx, pCCtx->outWinIdx, true);
      }
      TAOS_CHECK_EXIT(extWinAppendAggFilledRow(pOperator, pExtW, pCCtx, pWin, pFillRow));
      pCCtx->outWinNum++;

      if (pBlock->info.rows >= pOperator->resultInfo.threshold) {
        ++pCCtx->outWinIdx;
        break;
      }
      continue;
    }

    pCCtx->outWinNum++;

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      uint32_t newSize = pBlock->info.rows + pRow->numOfRows + ((emitAllWins || pExtW->isDynWindow) ? (numOfWin - pCCtx->outWinIdx) : (pCCtx->outWinTotalNum - pCCtx->outWinNum));
      TAOS_CHECK_EXIT(blockDataEnsureCapacity(pBlock, newSize));
      qDebug("datablock capacity not sufficient, expand to required:%d, current capacity:%d, %s", newSize,
             pBlock->info.capacity, GET_TASKID(pTaskInfo));
    }

    updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pRow->win, 0);
    TAOS_CHECK_EXIT(copyResultrowToDataBlock(pExprInfo, numOfExprs, pRow, pCtx, pBlock, rowEntryOffset, pTaskInfo));

    int32_t startRow = pBlock->info.rows;
    pBlock->info.rows += pRow->numOfRows;
    TAOS_CHECK_EXIT(extWinApplyAggPostProjection(pOperator, pExtW, pBlock, startRow, pRow->numOfRows));

    TAOS_CHECK_EXIT(extWinAppendWinIdx(pOperator->pTaskInfo, pExtW->pWinRowIdx, pBlock, pRow->winIdx, pRow->numOfRows));

    if (pBlock->info.rows >= pOperator->resultInfo.threshold) {
      ++pCCtx->outWinIdx;
      break;
    }
  }

  if (grpDone) {
    // isDynWindow: done when all windows (including empty) are emitted; otherwise done when result count is met
    *grpDone = (emitAllWins || pExtW->isDynWindow) ? (pCCtx->outWinIdx >= numOfWin)
                                                   : (pCCtx->outWinIdx >= numOfWin || pCCtx->outWinNum >= pCCtx->outWinTotalNum);
  }

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  
  return code;
}

static int32_t extWinAggOutputMulNoOrderCGrpsRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                                 bool* pTriggerGroupDone) {
  int32_t code = 0, lino = 0;
  SExtWinTrigGrpCtx* pTGrpCtx = pExtW->pTGrpCtx;
  SSDataBlock*    pBlock = pExtW->binfo.pRes;
  bool               calcGroupDone = false;

  if (pTriggerGroupDone != NULL) {
    *pTriggerGroupDone = false;
  }

  if (pTGrpCtx == NULL) {
    if (pTriggerGroupDone != NULL) {
      *pTriggerGroupDone = true;
    }
    return TSDB_CODE_SUCCESS;
  }

  if (pTGrpCtx->pCCtx) {
    pExtW->lastCGrpId = pTGrpCtx->pCCtx->groupId;
    TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, &calcGroupDone));
    if (pBlock->info.rows > 0) {
      goto _exit;
    }
  }

  if (pTGrpCtx->pCCtx == NULL || calcGroupDone) {
    pTGrpCtx->pCCtx = extWinIterateCalcGroup(pTGrpCtx, &pTGrpCtx->pLastCGrpIter, &pTGrpCtx->lastCtxIter);
    while (pTGrpCtx->pCCtx != NULL) {
      pExtW->lastCGrpId = pTGrpCtx->pCCtx->groupId;
      calcGroupDone = false;
      TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, &calcGroupDone));
      if (pBlock->info.rows > 0) {
        break;
      }

      if (!calcGroupDone) {
        break;
      }
      pTGrpCtx->pCCtx = extWinIterateCalcGroup(pTGrpCtx, &pTGrpCtx->pLastCGrpIter, &pTGrpCtx->lastCtxIter);
    }
  }

  if (pTriggerGroupDone != NULL) {
    *pTriggerGroupDone = pTGrpCtx->pCCtx == NULL;
  }

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pOperator->pTaskInfo), __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinAggOutputMulNoOrderTGrpsRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  bool                    triggerGroupDone = false;

  bool needBind = extWinUsesStrictMultiGroupProtocol(pOperator)
                      ? pExtW->activeBaseGId == 0
                      : pStream->curGrpCalc == NULL || extWinGroupIsFinalized(pExtW, pExtW->lastTGrpId);
  if (needBind) {
    bool found = false;
    TAOS_CHECK_EXIT(extWinBindNextHashOutputGroup(pOperator, pExtW, &found));
    if (!found) {
      goto _exit;
    }
  }

  if (pExtW->calcWithPartition) {
    TAOS_CHECK_EXIT(extWinAggOutputMulNoOrderCGrpsRes(pOperator, pExtW, &triggerGroupDone));
  } else {
    TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, &triggerGroupDone));
  }
  pExtW->groupOutputComplete = triggerGroupDone;
  extWinPostUpdateStreamRt(pStream, pOperator, pExtW);

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pOperator->pTaskInfo), __func__, lino, tstrerror(code));
  }

  return code;
}


static int32_t extWinAggOutputMulOrderCGrpsRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  int32_t code = 0, lino = 0;
  SExtWinTrigGrpCtx* pTGrpCtx = pExtW->pTGrpCtx;
  SSDataBlock*    pBlock = pExtW->binfo.pRes;
  int32_t grpNum = taosArrayGetSize(pExtW->pGrpIds);
  bool grpDone = false;

  if (pTGrpCtx == NULL || pTGrpCtx->pCGCtxs == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  for (; pExtW->lastGrpIdx < grpNum; ++pExtW->lastGrpIdx) {
    uint64_t *grpId = taosArrayGet(pExtW->pGrpIds, pExtW->lastGrpIdx);
    pTGrpCtx->pCCtx = extWinLookupCalcGroup(pTGrpCtx, *grpId);
    TSDB_CHECK_NULL(pTGrpCtx->pCCtx, code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, &grpDone));
    if (pBlock->info.rows > 0) {
      if (grpDone) {
        pExtW->lastGrpIdx++;
      }
      
      break;
    }
  }

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pOperator->pTaskInfo), __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinAggOutputMulOrderTGrpsRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  int32_t code = 0, lino = 0;
  int32_t grpNum = taosArrayGetSize(pExtW->pGrpIds);
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  bool grpDone = false;

  bool strictMultiGroup = extWinUsesStrictMultiGroupProtocol(pOperator);
  bool needBind = strictMultiGroup ? pExtW->activeBaseGId == 0
                                   : pStream->curGrpCalc == NULL || extWinGroupIsFinalized(pExtW, pExtW->lastTGrpId);
  if (needBind) {
    bool found = false;
    while (!strictMultiGroup && pExtW->lastGrpIdx < grpNum) {
      uint64_t* pGroupId = taosArrayGet(pExtW->pGrpIds, pExtW->lastGrpIdx++);
      if (extWinGroupIsFinalized(pExtW, *pGroupId)) {
        continue;
      }

      TAOS_CHECK_EXIT(extWinBindOutputGroup(pOperator, pExtW, *pGroupId, 0));
      found = true;
      break;
    }

    if (found) {
      goto _output;
    }
    TAOS_CHECK_EXIT(extWinBindNextHashOutputGroup(pOperator, pExtW, &found));
    if (!found) {
      goto _exit;
    }
  }

_output:
  TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, &grpDone));
  pExtW->groupOutputComplete = grpDone;
  extWinPostUpdateStreamRt(pStream, pOperator, pExtW);

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pOperator->pTaskInfo), __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinAggOutputMulOrderTCGrpsRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  int32_t code = 0, lino = 0;
  SSDataBlock*            pBlock = pExtW->binfo.pRes;
  int32_t                 grpNum = taosArrayGetSize(pExtW->pCTGrpIds);
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  bool                    calcGroupDone = false;
  bool                    strictMultiGroup = extWinUsesStrictMultiGroupProtocol(pOperator);

  bool needBind = strictMultiGroup ? pExtW->activeBaseGId == 0
                                   : pStream->curGrpCalc == NULL || extWinGroupIsFinalized(pExtW, pExtW->lastTGrpId);
  if (needBind) {
    bool found = false;
    while (!strictMultiGroup && pExtW->lastGrpIdx < grpNum) {
      uint64_t* pPair = taosArrayGet(pExtW->pCTGrpIds, pExtW->lastGrpIdx);
      if (extWinGroupIsFinalized(pExtW, pPair[1])) {
        ++pExtW->lastGrpIdx;
        continue;
      }

      TAOS_CHECK_EXIT(extWinBindOutputGroup(pOperator, pExtW, pPair[1], pPair[0]));
      found = true;
      break;
    }

    if (found) {
      goto _bound;
    }
    TAOS_CHECK_EXIT(extWinBindNextHashOutputGroup(pOperator, pExtW, &found));
    if (!found) {
      goto _exit;
    }
  }

_bound:
  if (strictMultiGroup) {
    while (pExtW->lastGrpIdx < grpNum &&
           ((uint64_t*)taosArrayGet(pExtW->pCTGrpIds, pExtW->lastGrpIdx))[1] < pExtW->activeBaseGId) {
      ++pExtW->lastGrpIdx;
    }

    if (pExtW->lastGrpIdx >= grpNum ||
        ((uint64_t*)taosArrayGet(pExtW->pCTGrpIds, pExtW->lastGrpIdx))[1] != pExtW->activeBaseGId) {
      pExtW->groupOutputComplete = true;
      goto _publish;
    }
  } else if (pExtW->lastGrpIdx >= grpNum) {
    bool triggerGroupDone = false;
    TAOS_CHECK_EXIT(extWinAggOutputMulNoOrderCGrpsRes(pOperator, pExtW, &triggerGroupDone));
    pExtW->groupOutputComplete = triggerGroupDone;
    goto _publish;
  }

  while (pExtW->lastGrpIdx < grpNum) {
    uint64_t* pPair = taosArrayGet(pExtW->pCTGrpIds, pExtW->lastGrpIdx);
    if (pPair[1] != pExtW->lastTGrpId) {
      pExtW->groupOutputComplete = true;
      break;
    }

    pExtW->pTGrpCtx->pCCtx = extWinLookupCalcGroup(pExtW->pTGrpCtx, pPair[0]);
    TSDB_CHECK_NULL(pExtW->pTGrpCtx->pCCtx, code, lino, _exit, TSDB_CODE_STREAM_INTERNAL_ERROR);
    pExtW->lastCGrpId = pPair[0];
    calcGroupDone = false;
    TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, &calcGroupDone));
    if (calcGroupDone) {
      ++pExtW->lastGrpIdx;
      if (pExtW->lastGrpIdx >= grpNum) {
        pExtW->groupOutputComplete = true;
      } else {
        uint64_t* pNextPair = taosArrayGet(pExtW->pCTGrpIds, pExtW->lastGrpIdx);
        pExtW->groupOutputComplete = pNextPair[1] != pExtW->lastTGrpId;
      }
    }

    if (pBlock->info.rows > 0 || !calcGroupDone || pExtW->groupOutputComplete) {
      break;
    }
  }

_publish:
  extWinPostUpdateStreamRt(pStream, pOperator, pExtW);
  
_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pOperator->pTaskInfo), __func__, lino, tstrerror(code));
  }

  return code;
}


static int32_t extWinAggOutputRes(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*    pBlock = pExtW->binfo.pRes;
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SStreamRuntimeFuncInfo*  pStream = pTaskInfo->pStreamRuntimeInfo ? &pTaskInfo->pStreamRuntimeInfo->funcInfo : NULL;
  SColumnInfoData*         pFilterRes = NULL;
  bool                     emitted = false;
  bool                     useRecordedGroupOrder = pExtW->needGroupSort || pExtW->inputMode == EXT_WIN_INPUT_BROADCAST;
  const char*              outputPath = useRecordedGroupOrder
                                            ? ((pStream && pStream->isMultiGroupCalc)
                                                   ? (pExtW->calcWithPartition ? "mul_order_tcalc" : "mul_order_tgroup")
                                                   : "mul_order_cgroup")
                                            : ((pStream && pStream->isMultiGroupCalc)
                                                   ? "mul_noorder_tgroup"
                                                   : (pExtW->calcWithPartition ? "mul_noorder_cgroup" : "single_cgroup"));

  pBlock->info.version = pTaskInfo->version;
  blockDataCleanup(pBlock);
  taosArrayClear(pExtW->pWinRowIdx);
  pExtW->groupOutputComplete = false;

  if (extWinUsesStrictMultiGroupProtocol(pOperator)) {
    TAOS_CHECK_EXIT(extWinEmitPendingGroupEog(pOperator, pExtW, pBlock, ppRes, &emitted));
    if (emitted) {
      goto _exit;
    }
  }

  qDebug("%s %s output-path:%s, needGroupSort:%d, calcWithPartition:%d, multiGroupCalc:%d, inputHasOrder:%d",
         GET_TASKID(pTaskInfo), __func__, outputPath, pExtW->needGroupSort, pExtW->calcWithPartition,
         (pStream && pStream->isMultiGroupCalc) ? 1 : 0, pExtW->inputHasOrder);

  if (useRecordedGroupOrder) {
    if (pStream && pStream->isMultiGroupCalc) {
      if (pExtW->calcWithPartition) {
        TAOS_CHECK_EXIT(extWinAggOutputMulOrderTCGrpsRes(pOperator, pExtW));
      } else {
        TAOS_CHECK_EXIT(extWinAggOutputMulOrderTGrpsRes(pOperator, pExtW));
      }
    } else {
      TAOS_CHECK_EXIT(extWinAggOutputMulOrderCGrpsRes(pOperator, pExtW));
    }
  } else {
    if (pStream && pStream->isMultiGroupCalc) {
      TAOS_CHECK_EXIT(extWinAggOutputMulNoOrderTGrpsRes(pOperator, pExtW));
    } else if (pExtW->calcWithPartition) {
      TAOS_CHECK_EXIT(extWinAggOutputMulNoOrderCGrpsRes(pOperator, pExtW, NULL));
    } else {
      TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, NULL));
    }
  }

  qDebug("%s result generated, rows:%" PRId64 ", groupId:%" PRIu64, GET_TASKID(pTaskInfo), pBlock->info.rows,
         pBlock->info.id.groupId);

  int32_t rowsBeforeFilter = pBlock->info.rows;
  SColMatchInfo* pMatchInfo = pExtW->fillMatchInfo.pList != NULL ? &pExtW->fillMatchInfo : NULL;
  TAOS_CHECK_EXIT(doFilter(pBlock, pOperator->exprSupp.pFilterInfo, pMatchInfo, &pFilterRes));
  if (pBlock->info.rows < rowsBeforeFilter) {
    if (pFilterRes != NULL) {
      TAOS_CHECK_EXIT(extWinRebuildWinIdxByFilter(pTaskInfo, pExtW->pWinRowIdx, rowsBeforeFilter, pFilterRes));
    } else {
      // no indicator means all rows are filtered out by short-circuit path
      taosArrayClear(pExtW->pWinRowIdx);
    }
  }

  pBlock->info.dataLoad = 1;
  extWinAssignBlockGrpId(pOperator, pExtW, &pBlock->info.id);

  qDebug("%s result generated, rows:%" PRId64 ", cGrpId:%" PRIu64 " baseGid:%" PRIu64, 
    GET_TASKID(pTaskInfo), pBlock->info.rows, pBlock->info.id.groupId, pBlock->info.id.baseGId);

  if (extWinUsesStrictMultiGroupProtocol(pOperator) && pExtW->activeBaseGId != 0) {
    TAOS_CHECK_EXIT(extWinPublishGroupOutput(pOperator, pExtW, pBlock, ppRes));
  } else if (pStream != NULL && pStream->isMultiGroupCalc && pStream->curGrpCalc != NULL) {
    TAOS_CHECK_EXIT(extWinFinishLegacyOutputGroup(pOperator, pExtW, pBlock, ppRes));
  } else {
    *ppRes = (pBlock->info.rows > 0) ? pBlock : NULL;
  }

  if (*ppRes) {
    (*ppRes)->info.window.skey = pExtW->orgTableTimeRange.skey;
    (*ppRes)->info.window.ekey = pExtW->orgTableTimeRange.ekey;
  }
  if (pOperator->pTaskInfo->pStreamRuntimeInfo && !pExtW->boundaryReady) {
    pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx = pExtW->pWinRowIdx;
  }

_exit:
  colDataDestroy(pFilterRes);
  taosMemoryFree(pFilterRes);

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static void extWinFreeResultRow(SExternalWindowOperator* pExtW) {
  if (pExtW->resultRows.resRowAllcNum * pExtW->aggSup.resultRowSize >= 1048576) {
    int32_t i = 1;
    while (i < pExtW->resultRows.resRowsSize && pExtW->resultRows.pResultRows[i]) {
      taosMemoryFreeClear(pExtW->resultRows.pResultRows[i]);
      pExtW->resultRows.resRowAllcNum -= pExtW->resultRows.resRowSize;
      i++;
    }
  }
  
  if (pExtW->binfo.pRes && pExtW->binfo.pRes->info.rows * pExtW->aggSup.resultRowSize >= 1048576) {
    blockDataFreeCols(pExtW->binfo.pRes);
  }
}

static bool extWinLegacyNonAggOutputReady(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  if (pOperator->pTaskInfo->pStreamRuntimeInfo != NULL &&
      pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.isMultiGroupCalc) {
    qDebug("%s %s fast-path disabled by multi-group progression", GET_TASKID(pOperator->pTaskInfo), __func__);
    return false;
  }

  if (pExtW->calcWithPartition) {
    qDebug("%s %s fast-path disabled by calcWithPartition", GET_TASKID(pOperator->pTaskInfo), __func__);
    return false;
  }

  if ((pExtW->multiTableMode && !pExtW->inputHasOrder) || pExtW->needGroupSort) {
    qDebug("%s %s fast-path disabled, multiTableNoOrder:%d, needGroupSort:%d", GET_TASKID(pOperator->pTaskInfo),
           __func__, (pExtW->multiTableMode && !pExtW->inputHasOrder) ? 1 : 0, pExtW->needGroupSort);
    return false;
  }
  int32_t remainWin = pExtW->resWinIdx - pExtW->outWinIdx;
  if (remainWin > 1 && (NULL == pExtW->timeRangeExpr || !pExtW->timeRangeExpr->needCalc)) {
    qDebug("%s %s fast-path enabled by remainWin:%d", GET_TASKID(pOperator->pTaskInfo), __func__, remainWin);
    return true;
  }
  
  SList* pList = taosArrayGetP(pExtW->pOutputBlocks, pExtW->outWinIdx);
  if (!pList || listNEles(pList) <= 0) {
    return false;
  }
  if (listNEles(pList) > 1) {
    qDebug("%s %s fast-path enabled by blockListSize:%d", GET_TASKID(pOperator->pTaskInfo), __func__,
           listNEles(pList));
    return true;
  }

  SListNode* pNode = listHead(pList);
  SArray* pIdx = *(SArray**)((SArray**)pNode->data + 1);
  int32_t* winIdx = taosArrayGetLast(pIdx);
  if (winIdx && *winIdx < pExtW->pTGrpCtx->pCCtx->blkWinStartIdx) {
    qDebug("%s %s fast-path enabled by winIdx:%d blkWinStartIdx:%d", GET_TASKID(pOperator->pTaskInfo), __func__,
           *winIdx, pExtW->pTGrpCtx->pCCtx->blkWinStartIdx);
    return true;
  }

  return false;
}

static int32_t extWinCalcGroupOutputReady(SExternalWindowOperator* pExtW, SExtWinCalcGrpCtx* pCCtx, bool* pReady) {
  if (pExtW == NULL || pCCtx == NULL || pReady == NULL || pCCtx->pWins == NULL || pExtW->pOutputBlocks == NULL ||
      pCCtx->outWinIdx < 0) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  *pReady = false;
  int32_t numWins = taosArrayGetSize(pCCtx->pWins);
  if (pCCtx->outWinIdx >= numWins) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t pendingWinIdx = -1;
  SList*  pList = NULL;
  for (int32_t i = pCCtx->outWinIdx; i < numWins; ++i) {
    SExtWinTimeWindow* pWin = TARRAY_GET_ELEM(pCCtx->pWins, i);
    if (pWin->resWinIdx < 0) {
      continue;
    }
    if (pWin->resWinIdx >= taosArrayGetSize(pExtW->pOutputBlocks)) {
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }

    pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->resWinIdx);
    if (pList == NULL) {
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
    if (listNEles(pList) > 0) {
      pendingWinIdx = i;
      break;
    }
  }
  if (pendingWinIdx < 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (listNEles(pList) > 1) {
    *pReady = true;
    return TSDB_CODE_SUCCESS;
  }

  if (pExtW->timeRangeExpr == NULL || !pExtW->timeRangeExpr->needCalc) {
    for (int32_t i = pendingWinIdx + 1; i < numWins; ++i) {
      SExtWinTimeWindow* pWin = TARRAY_GET_ELEM(pCCtx->pWins, i);
      if (pWin->resWinIdx < 0) {
        continue;
      }
      if (pWin->resWinIdx >= taosArrayGetSize(pExtW->pOutputBlocks)) {
        return TSDB_CODE_STREAM_INTERNAL_ERROR;
      }
      *pReady = true;
      return TSDB_CODE_SUCCESS;
    }
  }

  SListNode* pNode = listHead(pList);
  SArray*    pIdx = pNode == NULL ? NULL : *(SArray**)((SArray**)pNode->data + 1);
  int64_t*   pPackedIdx = pIdx == NULL ? NULL : taosArrayGetLast(pIdx);
  if (pNode == NULL || pIdx == NULL || pPackedIdx == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  int32_t winIdx = *(int32_t*)pPackedIdx;
  *pReady = winIdx < pCCtx->blkWinStartIdx;
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinNonAggOutputReady(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, bool* pReady) {
  if (pOperator == NULL || pExtW == NULL || pReady == NULL || pOperator->pTaskInfo == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pReady = false;
  if (!extWinUsesStrictMultiGroupProtocol(pOperator)) {
    *pReady = extWinLegacyNonAggOutputReady(pOperator, pExtW);
    return TSDB_CODE_SUCCESS;
  }

  if (pExtW->inputMode != EXT_WIN_INPUT_WINDOW_BOUND || pExtW->mode != EEXT_MODE_SCALAR || pExtW->calcWithPartition ||
      (pExtW->multiTableMode && !pExtW->inputHasOrder)) {
    return TSDB_CODE_SUCCESS;
  }

  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  uint64_t                expectedGid = 0;
  SSTriggerGroupCalcInfo* pGroup = NULL;
  int32_t                 code = extWinGetExpectedGroup(pStream, pExtW->groupOrderIdx, &expectedGid, &pGroup);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pGroup == NULL || expectedGid == 0) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  SExtWinTrigGrpCtx* pTCtx = extWinLookupTriggerGroup(pExtW, expectedGid);
  if (pTCtx == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SExtWinCalcGrpCtx* pCCtx = extWinLookupCalcGroup(pTCtx, 0);
  if (pCCtx == NULL || pCCtx->groupId != 0) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  return extWinCalcGroupOutputReady(pExtW, pCCtx, pReady);
}

static int32_t getTimeWindowOfBlock(SSDataBlock *pBlock, col_id_t tsSlotId, int64_t *startTs, int64_t *endTs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t tsIndex = -1;
  for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++) {
    SColumnInfoData *pCol = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i);
    QUERY_CHECK_NULL(pCol, code, lino, _return, terrno)
    if (pCol->info.colId == tsSlotId) {
      tsIndex = i;
      break;
    }
  }

  if (tsIndex == -1) {
    tsIndex = (int32_t)taosArrayGetSize(pBlock->pDataBlock) - 1;
  }

  SColumnInfoData *pColData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, tsIndex);
  QUERY_CHECK_NULL(pColData, code, lino, _return, terrno)

  GET_TYPED_DATA(*startTs, int64_t, TSDB_DATA_TYPE_TIMESTAMP, colDataGetNumData(pColData, 0), 0);
  GET_TYPED_DATA(*endTs, int64_t, TSDB_DATA_TYPE_TIMESTAMP, colDataGetNumData(pColData, pBlock->info.rows - 1), 0);

  return code;
_return:
  qError("failed to get time window of block, %s code:%s, line:%d", __func__, tstrerror(code), lino);
  return code;
}

static void extWinEndClearCtxs(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo) {
  if (pTaskInfo == NULL || pTaskInfo->pStreamRuntimeInfo == NULL) {
    return;
  }

  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  if (!pInfo->isMultiGroupCalc) {
    return;
  }

  pInfo->pStreamPesudoFuncVals = NULL;
  pInfo->pStreamPartColVals = NULL;
}

static int32_t extWinPrepareStaticEofGroups(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pTaskInfo->pStreamRuntimeInfo == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SStreamRuntimeFuncInfo* pStream = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  if (!pStream->isMultiGroupCalc) {
    if (pExtW->pTGrpCtx == NULL) {
      SBlockID id = {0};
      return extWinSwitchInitCtxs(pOperator, pExtW, &id);
    }
    return extWinPrepareWindowDecisions(pOperator, pExtW, pExtW->pTGrpCtx->pCCtx);
  }
  if (pStream->pGroupCalcInfos == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  if (!extWinUsesStrictMultiGroupProtocol(pOperator)) {
    int32_t                 iter = 0;
    SSTriggerGroupCalcInfo* pGroup = tSimpleHashIterate(pStream->pGroupCalcInfos, NULL, &iter);
    while (pGroup != NULL) {
      uint64_t groupId = *(uint64_t*)tSimpleHashGetKey(pGroup, NULL);
      uint64_t calcGroupId = 0;
      if (pExtW->calcWithPartition) {
        SExtWinTrigGrpCtx* pTCtx = extWinLookupTriggerGroup(pExtW, groupId);
        calcGroupId = pTCtx == NULL || pTCtx->pCCtx == NULL ? groupId : pTCtx->pCCtx->groupId;
      }
      int32_t code = extWinActivateGroup(pOperator, pExtW, pGroup, groupId, calcGroupId, false);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      pGroup = tSimpleHashIterate(pStream->pGroupCalcInfos, pGroup, &iter);
    }
    return TSDB_CODE_SUCCESS;
  }

  if (pStream->pGroupCalcOrder == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  int32_t groupCount = taosArrayGetSize(pStream->pGroupCalcOrder);
  for (int32_t orderIdx = 0; orderIdx < groupCount; ++orderIdx) {
    uint64_t                groupId = 0;
    SSTriggerGroupCalcInfo* pGroup = NULL;
    int32_t                 code = extWinGetExpectedGroup(pStream, orderIdx, &groupId, &pGroup);
    if (code != TSDB_CODE_SUCCESS || pGroup == NULL) {
      return code == TSDB_CODE_SUCCESS ? TSDB_CODE_STREAM_INTERNAL_ERROR : code;
    }
    uint64_t calcGroupId = 0;
    if (pExtW->calcWithPartition) {
      SExtWinTrigGrpCtx* pTCtx = extWinLookupTriggerGroup(pExtW, groupId);
      calcGroupId = pTCtx == NULL || pTCtx->pCCtx == NULL ? groupId : pTCtx->pCCtx->groupId;
    }
    code = extWinActivateGroup(pOperator, pExtW, pGroup, groupId, calcGroupId, false);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinAggHandleMultiTGrpEmptyWins(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  SStreamRuntimeFuncInfo* pStream = &pTaskInfo->pStreamRuntimeInfo->funcInfo;

  bool    canonicalOrder = extWinUsesStrictMultiGroupProtocol(pOperator);
  int32_t groupCount =
      canonicalOrder ? taosArrayGetSize(pStream->pGroupCalcOrder) : tSimpleHashGetSize(pStream->pGroupCalcInfos);
  int32_t                 iter = 0;
  SSTriggerGroupCalcInfo* pIterGroup = NULL;
  for (int32_t orderIdx = 0; orderIdx < groupCount; ++orderIdx) {
    uint64_t                groupId = 0;
    SSTriggerGroupCalcInfo* pGroup = NULL;
    if (canonicalOrder) {
      TAOS_CHECK_EXIT(extWinGetExpectedGroup(pStream, orderIdx, &groupId, &pGroup));
    } else {
      pIterGroup = tSimpleHashIterate(pStream->pGroupCalcInfos, pIterGroup, &iter);
      QUERY_CHECK_NULL(pIterGroup, code, lino, _exit, TSDB_CODE_STREAM_INTERNAL_ERROR);
      pGroup = pIterGroup;
      groupId = *(uint64_t*)tSimpleHashGetKey(pGroup, NULL);
    }
    QUERY_CHECK_NULL(pGroup, code, lino, _exit, TSDB_CODE_STREAM_INTERNAL_ERROR);
    uint64_t           calcGroupId = 0;
    SExtWinTrigGrpCtx* pTCtx = extWinLookupTriggerGroup(pExtW, groupId);
    if (pExtW->calcWithPartition) {
      calcGroupId = pTCtx == NULL || pTCtx->pCCtx == NULL ? groupId : pTCtx->pCCtx->groupId;
    }
    TAOS_CHECK_EXIT(extWinBindOutputGroup(pOperator, pExtW, groupId, calcGroupId));

    SExtWinTrigGrpCtx* pTGrpCtx = pExtW->pTGrpCtx;
    if (pTGrpCtx->pCGCtxs == NULL) {
      TAOS_CHECK_EXIT(extWinAggHandleEmptyWins(pOperator, NULL, true, NULL));
    } else {
      SExtWinCalcGrpCtx* pSavedCCtx = pTGrpCtx->pCCtx;
      uint64_t           savedCGrpId = pExtW->lastCGrpId;
      int32_t            cIter = 0;
      void*              pCIter = NULL;
      int32_t            emptyCode = TSDB_CODE_SUCCESS;
      SExtWinCalcGrpCtx* pCCtx = extWinIterateCalcGroup(pTGrpCtx, &pCIter, &cIter);
      while (pCCtx != NULL) {
        pTGrpCtx->pCCtx = pCCtx;
        pExtW->lastCGrpId = pCCtx->groupId;
        emptyCode = extWinAggHandleEmptyWins(pOperator, NULL, true, NULL);
        if (emptyCode != TSDB_CODE_SUCCESS) {
          break;
        }
        pCCtx = extWinIterateCalcGroup(pTGrpCtx, &pCIter, &cIter);
      }
      pTGrpCtx->pCCtx = pSavedCCtx;
      pExtW->lastCGrpId = savedCGrpId;
      TAOS_CHECK_EXIT(emptyCode);
    }
  }

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static void extWinPrepareForOutput(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  extWinClearGroupProgression(pExtW);
  pExtW->lastGrpIdx = 0;
  pExtW->lastTGrpId = 0;
  pExtW->lastCGrpId = 0;

  if (pOperator == NULL || pOperator->pTaskInfo == NULL || pOperator->pTaskInfo->pStreamRuntimeInfo == NULL) {
    return;
  }

  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;

  pStream->curGrpCalc = NULL;
  tClearStreamCurrentGroupRead(pStream);

  if (pExtW->needGroupSort) {
    if (pStream->isMultiGroupCalc && pExtW->calcWithPartition) {
      if (pExtW->pCTGrpIds) {
        taosArraySort(pExtW->pCTGrpIds, extWinCalcTriggerGrpIdCompare);
      }
    } else {
      if (pExtW->pGrpIds) {
        taosArraySort(pExtW->pGrpIds, extWinGrpIdCompare);
      }
    }
  }

  // In multi-group output mode, always restart from iterator state instead of
  // inheriting the calc phase's current group/cgrp cursor.
  if (pStream->isMultiGroupCalc) {
    pExtW->pTGrpCtx = NULL;
  }
}

static int32_t extWinEnsureDynParamOpenCtx(SExternalWindowOperator* pExtW) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  uint64_t           triggerGroupId = UINT64_MAX;
  uint64_t           calcGroupId = 0;
  SExtWinTrigGrpCtx* pNewTCtx = NULL;
  SExtWinCalcGrpCtx* pNewCCtx = NULL;

  if (pExtW->pGroupRegistry == NULL) {
    pExtW->pGroupRegistry = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
    TSDB_CHECK_NULL(pExtW->pGroupRegistry, code, lino, _exit, terrno);
  }

  SExtWinTrigGrpCtx** ppTCtx = tSimpleHashGet(pExtW->pGroupRegistry, &triggerGroupId, sizeof(triggerGroupId));
  if (ppTCtx == NULL) {
    pNewTCtx = taosMemoryCalloc(1, sizeof(*pNewTCtx));
    TSDB_CHECK_NULL(pNewTCtx, code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(
        tSimpleHashPut(pExtW->pGroupRegistry, &triggerGroupId, sizeof(triggerGroupId), &pNewTCtx, sizeof(pNewTCtx)));
    pNewTCtx = NULL;
    ppTCtx = tSimpleHashGet(pExtW->pGroupRegistry, &triggerGroupId, sizeof(triggerGroupId));
    TSDB_CHECK_NULL(ppTCtx, code, lino, _exit, terrno);
  }
  pExtW->pTGrpCtx = *ppTCtx;
  if (pExtW->pTGrpCtx->pCGCtxs == NULL) {
    pExtW->pTGrpCtx->pCGCtxs = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
    TSDB_CHECK_NULL(pExtW->pTGrpCtx->pCGCtxs, code, lino, _exit, terrno);
  }
  SExtWinCalcGrpCtx** ppCCtx = tSimpleHashGet(pExtW->pTGrpCtx->pCGCtxs, &calcGroupId, sizeof(calcGroupId));
  if (ppCCtx == NULL) {
    pNewCCtx = taosMemoryCalloc(1, sizeof(*pNewCCtx));
    TSDB_CHECK_NULL(pNewCCtx, code, lino, _exit, terrno);
    extWinInitDynParamCGrpCtx(pNewCCtx);
    pNewCCtx->broadcast.pStat = &pExtW->stat;
    TAOS_CHECK_EXIT(
        tSimpleHashPut(pExtW->pTGrpCtx->pCGCtxs, &calcGroupId, sizeof(calcGroupId), &pNewCCtx, sizeof(pNewCCtx)));
    pNewCCtx = NULL;
    ppCCtx = tSimpleHashGet(pExtW->pTGrpCtx->pCGCtxs, &calcGroupId, sizeof(calcGroupId));
    TSDB_CHECK_NULL(ppCCtx, code, lino, _exit, terrno);
  }
  pExtW->pTGrpCtx->pCCtx = *ppCCtx;

_exit:
  taosMemoryFree(pNewTCtx);
  taosMemoryFree(pNewCCtx);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinEnsureBroadcastPool(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                         SSDataBlock* pBlock) {
  uint32_t pageSize = 0;
  int64_t  defaultBufferSize = 0;
  int32_t  code = getBufferPgSize(pBlock->info.rowSize, &pageSize, &defaultBufferSize);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pExtW->pBroadcastPool != NULL) {
    return winInputPoolGetPageSize(pExtW->pBroadcastPool) == (int32_t)pageSize ? TSDB_CODE_SUCCESS
                                                                               : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int64_t actualBudget = TMAX((int64_t)EXT_WIN_BROADCAST_INMEM_BYTES, (int64_t)pageSize * 4);
  code = winInputPoolCreate((int32_t)pageSize, actualBudget, pOperator->pTaskInfo->id.str, &pExtW->pBroadcastPool);
  if (code == TSDB_CODE_SUCCESS) {
    pExtW->stat.broadcastStoreBudget = actualBudget;
  }
  return code;
}

static int32_t extWinBroadcastAppendBlock(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                          SExtWinCalcGrpCtx* pCCtx, SSDataBlock* pBlock) {
  if (pOperator == NULL || pExtW == NULL || pCCtx == NULL || pBlock == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  if (pBlock->info.rows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t                code = TSDB_CODE_SUCCESS;
  SExtWinBroadcastState* pState = &pCCtx->broadcast;
  if (pState->inputLoaded) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  if (pState->pStore == NULL) {
    code = extWinEnsureBroadcastPool(pOperator, pExtW, pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = winInputStoreCreate(pBlock, pExtW->pBroadcastPool, &pState->pStore);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pState->pPageMeta = taosArrayInit(4, sizeof(SExtWinBroadcastPageMeta));
    if (pState->pPageMeta == NULL) {
      return terrno;
    }
    pState->pStat = &pExtW->stat;
  }

  int32_t firstNewPage = winInputStoreGetPageCount(pState->pStore);
  code = winInputStoreAppendBlock(pState->pStore, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  int32_t                  pageCount = winInputStoreGetPageCount(pState->pStore);
  SExtWinBroadcastPageMeta meta = {.id = pBlock->info.id,
                                   .window = pBlock->info.window,
                                   .version = pBlock->info.version,
                                   .dataLoad = pBlock->info.dataLoad,
                                   .scanFlag = pBlock->info.scanFlag};
  for (int32_t page = firstNewPage; page < pageCount; ++page) {
    if (taosArrayPush(pState->pPageMeta, &meta) == NULL) {
      return terrno;
    }
  }

  size_t  blockBytes = blockDataGetSize(pBlock);
  int64_t serializedBytes = blockBytes > INT64_MAX ? INT64_MAX : (int64_t)blockBytes;
  serializedBytes = extWinAddPositiveI64Saturating(serializedBytes, sizeof(int32_t));
  int64_t columnHeaderBytes =
      extWinMultiplyPositiveI64Saturating(taosArrayGetSize(pBlock->pDataBlock), sizeof(int32_t));
  serializedBytes = extWinAddPositiveI64Saturating(serializedBytes, columnHeaderBytes);
  extWinAccountBroadcastPayloadAppend(pState, serializedBytes);
  pExtW->stat.broadcastScanBlocks++;
  pExtW->stat.broadcastScanRows += pBlock->info.rows;
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinBroadcastProcessPage(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, SSDataBlock* pBlock,
                                          SExtWinTimeWindow* pWin) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (pExtW->mode) {
    case EEXT_MODE_SCALAR:
      return extWinProjectDo(pOperator, pBlock, 0, (int32_t)pBlock->info.rows, pWin);
    case EEXT_MODE_INDEFR_FUNC:
      return extWinIndefRowsDo(pOperator, pBlock, 0, (int32_t)pBlock->info.rows, pWin);
    case EEXT_MODE_AGG: {
      if (pExtW->scalarSupp.pExprInfo != NULL) {
        SExprSupp* pScalarSup = &pExtW->scalarSupp;
        code = projectApplyFunctions(pScalarSup->pExprInfo, pBlock, pBlock, pScalarSup->pCtx, pScalarSup->numOfExprs,
                                     pExtW->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      code = setInputDataBlock(&pOperator->exprSupp, pBlock, pExtW->binfo.inputTsOrder, pBlock->info.scanFlag, true);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      code = extWinAggSettleThrough(pOperator, extWinGetCurWinIdx(pOperator) - 1, pBlock);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      if (pWin->resWinIdx < 0) {
        code = extWinAggSetWinOutputBuf(pOperator, pWin, &pOperator->exprSupp, &pExtW->aggSup, pOperator->pTaskInfo);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pWin->tw, 1);
      code = extWinAggDo(pOperator, 0, (int32_t)pBlock->info.rows, pBlock);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      SResultRow* pResult = extWinGetResultRowByIdx(pExtW, pWin->resWinIdx, pExtW->aggSup.resultRowSize);
      if (pResult != NULL) {
        pResult->nOrigRows += pBlock->info.rows;
      }
      SExtWinCalcGrpCtx* pCCtx = pExtW->pTGrpCtx->pCCtx;
      pCCtx->lastSKey = pWin->tw.skey;
      pCCtx->lastEKey = pWin->tw.ekey;
      pCCtx->lastWinId = extWinGetCurWinIdx(pOperator);
      return TSDB_CODE_SUCCESS;
    }
    default:
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
}

static int32_t extWinBroadcastRestoreNullSummaries(SSDataBlock* pBlock) {
  if (pBlock == NULL || pBlock->pDataBlock == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t columns = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t col = 0; col < columns; ++col) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, col);
    if (pCol == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    pCol->hasNull = false;
    for (int32_t row = 0; row < pBlock->info.rows; ++row) {
      bool isNull = IS_VAR_DATA_TYPE(pCol->info.type) ? colDataIsNull_var(pCol, row) : colDataIsNull_f(pCol, row);
      if (isNull) {
        pCol->hasNull = true;
        break;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

#if defined(BUILD_TEST)
static int32_t extWinTestObserveBroadcastReplay(SExternalWindowOperator* pExtW, int32_t pageIndex,
                                                SSDataBlock* pReplay) {
  if (pExtW->pBroadcastReplayObservations == NULL) {
    pExtW->pBroadcastReplayObservations = taosArrayInit(4, sizeof(SExtWinBroadcastReplayObservation));
    if (pExtW->pBroadcastReplayObservations == NULL) {
      return terrno;
    }
  }

  SExtWinBroadcastReplayObservation observation = {
      .pageIndex = pageIndex,
      .id = pReplay->info.id,
      .window = pReplay->info.window,
      .version = pReplay->info.version,
      .dataLoad = pReplay->info.dataLoad,
      .scanFlag = pReplay->info.scanFlag,
  };
  if (taosArrayPush(pExtW->pBroadcastReplayObservations, &observation) == NULL) {
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}
#endif

static int32_t extWinBroadcastReplayPage(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                         SExtWinCalcGrpCtx* pCCtx, int32_t winIdx) {
  if (pOperator == NULL || pExtW == NULL || pCCtx == NULL || pCCtx->pWins == NULL || winIdx < 0 ||
      winIdx >= (int32_t)taosArrayGetSize(pCCtx->pWins)) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SExtWinTimeWindow* pWin = taosArrayGet(pCCtx->pWins, winIdx);
  if (pWin == NULL) {
    return terrno;
  }
  SExtWinBroadcastState* pState = &pCCtx->broadcast;
  int32_t                pageCount = winInputStoreGetPageCount(pState->pStore);
  if (pState->replayPage < 0 || pState->replayPage >= pageCount) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SSDataBlock* pStored = NULL;
  int32_t      code = winInputStoreGetBlock(pState->pStore, pState->replayPage, &pStored);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SExtWinBroadcastPageMeta* pMeta = taosArrayGet(pState->pPageMeta, pState->replayPage);
  if (pStored == NULL || pMeta == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SSDataBlock* pReplay = NULL;
  code = createOneDataBlock(pStored, true, &pReplay);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pReplay->info.id = pMeta->id;
  pReplay->info.window = pMeta->window;
  pReplay->info.version = pMeta->version;
  pReplay->info.dataLoad = pMeta->dataLoad;
  pReplay->info.scanFlag = pMeta->scanFlag;
  code = extWinBroadcastRestoreNullSummaries(pReplay);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pReplay);
    return code;
  }
#if defined(BUILD_TEST)
  code = extWinTestObserveBroadcastReplay(pExtW, pState->replayPage, pReplay);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pReplay);
    return code;
  }
#endif
  code = extWinBroadcastProcessPage(pOperator, pExtW, pReplay, pWin);
  pExtW->stat.broadcastReplayBlocks++;
  pExtW->stat.broadcastReplayRows += pReplay->info.rows;
  blockDataDestroy(pReplay);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  ++pState->replayPage;
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinBroadcastReplayWindow(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                           SExtWinCalcGrpCtx* pCCtx, int32_t winIdx) {
  SExtWinBroadcastState* pState = &pCCtx->broadcast;
  pState->replayPage = 0;
  int32_t pageCount = winInputStoreGetPageCount(pState->pStore);
  while (pState->replayPage < pageCount) {
    int32_t code = extWinBroadcastReplayPage(pOperator, pExtW, pCCtx, winIdx);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinBroadcastReplayCalcGroup(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                              SExtWinCalcGrpCtx* pCCtx) {
  if (pCCtx == NULL || pCCtx->pWins == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  pExtW->pTGrpCtx->pCCtx = pCCtx;
  pExtW->lastCGrpId = pCCtx->groupId;
  pCCtx->broadcast.inputLoaded = true;
  for (int32_t winIdx = 0; winIdx < (int32_t)taosArrayGetSize(pCCtx->pWins); ++winIdx) {
    SExtWinTimeWindow* pWin = taosArrayGet(pCCtx->pWins, winIdx);
    if (pWin == NULL) {
      return terrno;
    }
    extWinSetCurWinIdx(pOperator, winIdx);
    bool    pass = true;
    int32_t code = extWinGetWindowDecision(pCCtx, winIdx, &pass);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (pass && pCCtx->broadcast.pStore != NULL) {
      code = extWinBroadcastReplayWindow(pOperator, pExtW, pCCtx, winIdx);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinBroadcastReplayTriggerGroup(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  SExtWinTrigGrpCtx* pTCtx = pExtW->pTGrpCtx;
  if (pTCtx == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  if (pTCtx->pCGCtxs == NULL) {
    return extWinBroadcastReplayCalcGroup(pOperator, pExtW, pTCtx->pCCtx);
  }

  SExtWinCalcGrpCtx* pSaved = pTCtx->pCCtx;
  int32_t            iter = 0;
  void*              pIter = NULL;
  int32_t            code = TSDB_CODE_SUCCESS;
  SExtWinCalcGrpCtx* pCCtx = extWinIterateCalcGroup(pTCtx, &pIter, &iter);
  while (pCCtx != NULL) {
    code = extWinBroadcastReplayCalcGroup(pOperator, pExtW, pCCtx);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
    pCCtx = extWinIterateCalcGroup(pTCtx, &pIter, &iter);
  }
  pTCtx->pCCtx = pSaved;
  return code;
}

static int32_t extWinBroadcastReplayAll(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  SStreamRuntimeFuncInfo* pStream = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  if (!pStream->isMultiGroupCalc) {
    if (pExtW->pTGrpCtx == NULL) {
      SBlockID id = {0};
      int32_t  code = extWinSwitchInitCtxs(pOperator, pExtW, &id);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    return extWinBroadcastReplayTriggerGroup(pOperator, pExtW);
  }

  bool strictMultiGroup = extWinUsesStrictMultiGroupProtocol(pOperator);
  if (!strictMultiGroup && !pExtW->calcWithPartition && taosArrayGetSize(pExtW->pGrpIds) > 0) {
    int32_t groupCount = taosArrayGetSize(pExtW->pGrpIds);
    for (int32_t i = 0; i < groupCount; ++i) {
      uint64_t* pGroupId = taosArrayGet(pExtW->pGrpIds, i);
      if (pGroupId == NULL) {
        return terrno;
      }
      SSTriggerGroupCalcInfo* pGroup = tSimpleHashGet(pStream->pGroupCalcInfos, pGroupId, sizeof(*pGroupId));
      if (pGroup == NULL) {
        return TSDB_CODE_STREAM_INTERNAL_ERROR;
      }
      int32_t code = extWinActivateGroup(pOperator, pExtW, pGroup, *pGroupId, 0, false);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      code = extWinBroadcastReplayTriggerGroup(pOperator, pExtW);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  if (strictMultiGroup && !pExtW->calcWithPartition) {
    if (pStream->pGroupCalcOrder == NULL) {
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
    int32_t groupCount = taosArrayGetSize(pStream->pGroupCalcOrder);
    for (int32_t orderIdx = 0; orderIdx < groupCount; ++orderIdx) {
      uint64_t                groupId = 0;
      SSTriggerGroupCalcInfo* pGroup = NULL;
      int32_t                 code = extWinGetExpectedGroup(pStream, orderIdx, &groupId, &pGroup);
      if (code != TSDB_CODE_SUCCESS || pGroup == NULL) {
        return code == TSDB_CODE_SUCCESS ? TSDB_CODE_STREAM_INTERNAL_ERROR : code;
      }
      code = extWinActivateGroup(pOperator, pExtW, pGroup, groupId, 0, false);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      code = extWinBroadcastReplayTriggerGroup(pOperator, pExtW);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  if (taosArrayGetSize(pExtW->pCTGrpIds) > 0) {
    int32_t pairCount = taosArrayGetSize(pExtW->pCTGrpIds);
    for (int32_t i = 0; i < pairCount; ++i) {
      uint64_t* pPair = taosArrayGet(pExtW->pCTGrpIds, i);
      if (pPair == NULL) {
        return terrno;
      }
      SSTriggerGroupCalcInfo* pGroup = tSimpleHashGet(pStream->pGroupCalcInfos, &pPair[1], sizeof(pPair[1]));
      if (pGroup == NULL) {
        return TSDB_CODE_STREAM_INTERNAL_ERROR;
      }
      int32_t code = extWinActivateGroup(pOperator, pExtW, pGroup, pPair[1], pPair[0], false);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      code = extWinBroadcastReplayCalcGroup(pOperator, pExtW, pExtW->pTGrpCtx->pCCtx);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  if (!strictMultiGroup) {
    int32_t                 iter = 0;
    SSTriggerGroupCalcInfo* pGroup = tSimpleHashIterate(pStream->pGroupCalcInfos, NULL, &iter);
    while (pGroup != NULL) {
      uint64_t groupId = *(uint64_t*)tSimpleHashGetKey(pGroup, NULL);
      uint64_t calcGroupId = pExtW->calcWithPartition ? groupId : 0;
      int32_t  code = extWinActivateGroup(pOperator, pExtW, pGroup, groupId, calcGroupId, false);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      code = extWinBroadcastReplayTriggerGroup(pOperator, pExtW);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      pGroup = tSimpleHashIterate(pStream->pGroupCalcInfos, pGroup, &iter);
    }
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_STREAM_INTERNAL_ERROR;
}

static int32_t extWinGetBroadcastInputBlock(SOperatorInfo* pOperator, SSDataBlock** ppBlock) {
  if (pOperator == NULL || ppBlock == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppBlock = getNextBlockFromDownstreamRemainDetach(pOperator, 0);
  return TSDB_CODE_SUCCESS;
}

static const SStreamCalcContextGroupView* extWinFindBroadcastContextView(const SArray* pViews, int64_t gid) {
  int32_t low = 0;
  int32_t high = taosArrayGetSize(pViews) - 1;
  while (low <= high) {
    int32_t                            mid = low + (high - low) / 2;
    const SStreamCalcContextGroupView* pView = taosArrayGet(pViews, mid);
    if (pView == NULL || pView->gid == gid) {
      return pView;
    }
    if (pView->gid < gid) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  return NULL;
}

static int32_t extWinBroadcastLoadActiveGroup(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                              bool resetDownstream) {
  if (pExtW->pTGrpCtx == NULL || pExtW->pTGrpCtx->pCCtx == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t                 code = TSDB_CODE_SUCCESS;
  SStreamRuntimeInfo*     pRuntime = pOperator->pTaskInfo->pStreamRuntimeInfo;
  SStreamRuntimeFuncInfo* pStream = pRuntime == NULL ? NULL : &pRuntime->funcInfo;
  if (pRuntime != NULL) {
    pRuntime->fetchInfo.funcInfo = pRuntime->funcInfo;
    pRuntime->fetchInfo.reuseReader = resetDownstream;
    pRuntime->fetchInfo.active = true;
  }

  if (resetDownstream) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[0];
    if (pDownstream == NULL || pDownstream->fpSet.resetStateFn == NULL) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _exit;
    }
    code = pDownstream->fpSet.resetStateFn(pDownstream);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }
  }

  if (pStream != NULL && pStream->isMultiGroupCalc) {
    const SStreamCalcContextGroupView* pView = NULL;
    if (pStream->pContextPolicy != NULL || pStream->pAncestorContext != NULL) {
      if (pExtW->pBroadcastContextViews == NULL) {
        code = tProjectStreamCalcContextForAllGroups(pStream->pContextPolicy, pStream->pAncestorContext,
                                                     &pExtW->pBroadcastContextViews);
        if (code != TSDB_CODE_SUCCESS) {
          goto _exit;
        }
      }
      pView = extWinFindBroadcastContextView(pExtW->pBroadcastContextViews, pStream->groupId);
      if (pView == NULL) {
        code = TSDB_CODE_INVALID_PARA;
        goto _exit;
      }
    }
    pRuntime->fetchInfo.funcInfo.isMultiGroupCalc = false;
    pRuntime->fetchInfo.funcInfo.pContextPolicy = pView == NULL ? NULL : pView->pContextPolicy;
    pRuntime->fetchInfo.funcInfo.pAncestorContext = pView == NULL ? NULL : pView->pAncestorContext;
  }

  while (true) {
    SSDataBlock* pBlock = NULL;
    code = extWinGetBroadcastInputBlock(pOperator, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }
    if (pBlock == NULL) {
      break;
    }
    if (pExtW->isDynWindow && pBlock->info.rows > 0) {
      TSKEY skey = 0;
      TSKEY ekey = 0;
      code = getTimeWindowOfBlock(pBlock, pExtW->primaryTsIndex, &skey, &ekey);
      if (code != TSDB_CODE_SUCCESS) {
        goto _exit;
      }
      pExtW->orgTableTimeRange.skey = TMIN(pExtW->orgTableTimeRange.skey, skey);
      pExtW->orgTableTimeRange.ekey = TMAX(pExtW->orgTableTimeRange.ekey, ekey);
    }
    code = extWinBroadcastAppendBlock(pOperator, pExtW, pExtW->pTGrpCtx->pCCtx, pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }
  }
  pExtW->pTGrpCtx->pCCtx->broadcast.inputLoaded = true;

_exit:
  qClearStreamFetchRuntime(pRuntime);
  return code;
}

static int32_t extWinBroadcastLoadAllTriggerGroups(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  if (pStream->pGroupCalcInfos == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  bool resetDownstream = false;
  if (extWinUsesStrictMultiGroupProtocol(pOperator)) {
    if (pStream->pGroupCalcOrder == NULL) {
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
    int32_t groupCount = taosArrayGetSize(pStream->pGroupCalcOrder);
    for (int32_t orderIdx = 0; orderIdx < groupCount; ++orderIdx) {
      uint64_t                groupId = 0;
      SSTriggerGroupCalcInfo* pGroup = NULL;
      int32_t                 code = extWinGetExpectedGroup(pStream, orderIdx, &groupId, &pGroup);
      if (code != TSDB_CODE_SUCCESS || pGroup == NULL) {
        return code == TSDB_CODE_SUCCESS ? TSDB_CODE_STREAM_INTERNAL_ERROR : code;
      }
      code = extWinActivateGroup(pOperator, pExtW, pGroup, groupId, 0, true);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      code = extWinBroadcastLoadActiveGroup(pOperator, pExtW, resetDownstream);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      resetDownstream = true;
    }
    return TSDB_CODE_SUCCESS;
  }

  if (taosArrayGetSize(pExtW->pGrpIds) > 0) {
    int32_t groupCount = taosArrayGetSize(pExtW->pGrpIds);
    for (int32_t i = 0; i < groupCount; ++i) {
      uint64_t* pGroupId = taosArrayGet(pExtW->pGrpIds, i);
      if (pGroupId == NULL) {
        return terrno;
      }
      SSTriggerGroupCalcInfo* pGroup = tSimpleHashGet(pStream->pGroupCalcInfos, pGroupId, sizeof(*pGroupId));
      if (pGroup == NULL) {
        return TSDB_CODE_STREAM_INTERNAL_ERROR;
      }
      int32_t code = extWinActivateGroup(pOperator, pExtW, pGroup, *pGroupId, 0, true);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      code = extWinBroadcastLoadActiveGroup(pOperator, pExtW, resetDownstream);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      resetDownstream = true;
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t                 iter = 0;
  SSTriggerGroupCalcInfo* pGroup = tSimpleHashIterate(pStream->pGroupCalcInfos, NULL, &iter);
  while (pGroup != NULL) {
    uint64_t groupId = *(uint64_t*)tSimpleHashGetKey(pGroup, NULL);
    int32_t  code = extWinActivateGroup(pOperator, pExtW, pGroup, groupId, 0, true);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = extWinBroadcastLoadActiveGroup(pOperator, pExtW, resetDownstream);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    resetDownstream = true;
    pGroup = tSimpleHashIterate(pStream->pGroupCalcInfos, pGroup, &iter);
  }
  return TSDB_CODE_SUCCESS;
}

static void extWinBroadcastReleaseGroupInput(SExternalWindowOperator* pExtW, SExtWinCalcGrpCtx* pCCtx) {
  if (pExtW == NULL || pCCtx == NULL) {
    return;
  }

  extWinDestroyBroadcastState(&pCCtx->broadcast);
  pCCtx->broadcast.pStat = &pExtW->stat;
}

static int32_t extWinBroadcastLoadStreamingGroup(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                                 bool* pLoaded) {
  if (pOperator == NULL || pExtW == NULL || pLoaded == NULL || pOperator->pTaskInfo == NULL ||
      pOperator->pTaskInfo->pStreamRuntimeInfo == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  *pLoaded = false;
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  uint64_t                groupId = 0;
  SSTriggerGroupCalcInfo* pGroup = NULL;
  int32_t                 code = extWinGetExpectedGroup(pStream, pExtW->groupOrderIdx, &groupId, &pGroup);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pGroup == NULL) {
    pExtW->groupScanDone = true;
    pStream->curGrpCalc = NULL;
    return TSDB_CODE_SUCCESS;
  }

  code = extWinActivateGroup(pOperator, pExtW, pGroup, groupId, 0, false);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = extWinBroadcastLoadActiveGroup(pOperator, pExtW, pExtW->groupOrderIdx > 0);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  bool found = false;
  code = extWinBindNextHashOutputGroup(pOperator, pExtW, &found);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (!found || pExtW->activeBaseGId != groupId || pExtW->pTGrpCtx == NULL || pExtW->pTGrpCtx->pCCtx == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  SExtWinCalcGrpCtx* pCCtx = pExtW->pTGrpCtx->pCCtx;
  pCCtx->broadcast.replayWinIdx = 0;
  pCCtx->broadcast.replayPage = 0;
  pCCtx->outWinIdx = 0;
  *pLoaded = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinBroadcastPopStreamingBlock(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                                SExtWinCalcGrpCtx* pCCtx, SSDataBlock** ppBlock) {
  *ppBlock = NULL;
  if (pCCtx->broadcast.replayWinIdx < 0 || pCCtx->broadcast.replayWinIdx >= (int32_t)taosArrayGetSize(pCCtx->pWins)) {
    return TSDB_CODE_SUCCESS;
  }

  SExtWinTimeWindow* pWin = taosArrayGet(pCCtx->pWins, pCCtx->broadcast.replayWinIdx);
  if (pWin == NULL) {
    return terrno;
  }
  if (pWin->resWinIdx < 0) {
    return TSDB_CODE_SUCCESS;
  }

  SList* pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->resWinIdx);
  if (pList == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  SListNode* pNode = tdListPopHead(pList);
  if (pNode == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  *ppBlock = *(SSDataBlock**)pNode->data;
  pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx = *(SArray**)((SArray**)pNode->data + 1);
  pExtW->pLastBlkNode = pNode;
  return TSDB_CODE_SUCCESS;
}

static void extWinBroadcastAdvanceStreamingWindow(SExtWinCalcGrpCtx* pCCtx) {
  ++pCCtx->broadcast.replayWinIdx;
  pCCtx->broadcast.replayPage = 0;
  pCCtx->outWinIdx = pCCtx->broadcast.replayWinIdx;
}

static int32_t extWinBroadcastStreamingNext(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                            SSDataBlock** ppRes) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  *ppRes = NULL;

  if (pExtW->groupEogPending) {
    SExtWinCalcGrpCtx* pCompleted = pExtW->pTGrpCtx == NULL ? NULL : pExtW->pTGrpCtx->pCCtx;
    bool               emitted = false;
    TAOS_CHECK_EXIT(extWinEmitPendingGroupEog(pOperator, pExtW, pExtW->binfo.pRes, ppRes, &emitted));
    if (!emitted || *ppRes == NULL) {
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      lino = __LINE__;
      goto _exit;
    }
    extWinBroadcastReleaseGroupInput(pExtW, pCompleted);
    goto _exit;
  }

  while (!pExtW->groupScanDone) {
    if (pExtW->activeBaseGId == 0) {
      bool loaded = false;
      TAOS_CHECK_EXIT(extWinBroadcastLoadStreamingGroup(pOperator, pExtW, &loaded));
      if (!loaded) {
        goto _exit;
      }
    }

    SExtWinCalcGrpCtx* pCCtx = pExtW->pTGrpCtx == NULL ? NULL : pExtW->pTGrpCtx->pCCtx;
    if (pCCtx == NULL || pCCtx->pWins == NULL || !pCCtx->broadcast.inputLoaded) {
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      lino = __LINE__;
      goto _exit;
    }

    int32_t winCount = (int32_t)taosArrayGetSize(pCCtx->pWins);
    while (pCCtx->broadcast.replayWinIdx < winCount) {
      SSDataBlock* pBlock = NULL;
      TAOS_CHECK_EXIT(extWinBroadcastPopStreamingBlock(pOperator, pExtW, pCCtx, &pBlock));
      if (pBlock != NULL) {
        SExtWinTimeWindow* pWin = taosArrayGet(pCCtx->pWins, pCCtx->broadcast.replayWinIdx);
        SList*             pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->resWinIdx);
        int32_t pageCount = pCCtx->broadcast.pStore == NULL ? 0 : winInputStoreGetPageCount(pCCtx->broadcast.pStore);
        if (listNEles(pList) == 0 && pCCtx->broadcast.replayPage >= pageCount) {
          extWinBroadcastAdvanceStreamingWindow(pCCtx);
        }

        pExtW->groupOutputComplete = pCCtx->broadcast.replayWinIdx >= winCount;
        pBlock->info.version = pOperator->pTaskInfo->version;
        pBlock->info.dataLoad = 1;
        extWinAssignBlockGrpId(pOperator, pExtW, &pBlock->info.id);
        TAOS_CHECK_EXIT(extWinPublishGroupOutput(pOperator, pExtW, pBlock, ppRes));
        extWinPostUpdateStreamRt(pStream, pOperator, pExtW);
        goto _exit;
      }

      bool pass = true;
      TAOS_CHECK_EXIT(extWinGetWindowDecision(pCCtx, pCCtx->broadcast.replayWinIdx, &pass));
      int32_t pageCount = pCCtx->broadcast.pStore == NULL ? 0 : winInputStoreGetPageCount(pCCtx->broadcast.pStore);
      if (!pass || pCCtx->broadcast.replayPage >= pageCount) {
        extWinBroadcastAdvanceStreamingWindow(pCCtx);
        continue;
      }

      extWinSetCurWinIdx(pOperator, pCCtx->broadcast.replayWinIdx);
      TAOS_CHECK_EXIT(extWinBroadcastReplayPage(pOperator, pExtW, pCCtx, pCCtx->broadcast.replayWinIdx));
    }

    pExtW->groupOutputComplete = true;
    SSDataBlock* pBoundary = pExtW->binfo.pRes;
    blockDataCleanup(pBoundary);
    extWinAssignBlockGrpId(pOperator, pExtW, &pBoundary->info.id);
    SExtWinCalcGrpCtx* pCompleted = pCCtx;
    TAOS_CHECK_EXIT(extWinPublishGroupOutput(pOperator, pExtW, pBoundary, ppRes));
    extWinPostUpdateStreamRt(pStream, pOperator, pExtW);
    if (*ppRes == NULL || !qIsMultiGroupEogBlock(*ppRes)) {
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      lino = __LINE__;
      goto _exit;
    }
    extWinBroadcastReleaseGroupInput(pExtW, pCompleted);
    goto _exit;
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    *ppRes = NULL;
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t extWinBroadcastOpen(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  SStreamRuntimeFuncInfo* pStream = pTaskInfo->pStreamRuntimeInfo ? &pTaskInfo->pStreamRuntimeInfo->funcInfo : NULL;
  if (pStream != NULL && pStream->isMultiGroupCalc && !pExtW->calcWithPartition) {
    int32_t code = extWinBroadcastLoadAllTriggerGroups(pOperator, pExtW);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else {
    while (true) {
      SSDataBlock* pBlock = NULL;
      int32_t      code = extWinGetBroadcastInputBlock(pOperator, &pBlock);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      if (pBlock == NULL) {
        break;
      }
      if (pExtW->isDynWindow && pBlock->info.rows > 0) {
        TSKEY skey = 0;
        TSKEY ekey = 0;
        code = getTimeWindowOfBlock(pBlock, pExtW->primaryTsIndex, &skey, &ekey);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        pExtW->orgTableTimeRange.skey = TMIN(pExtW->orgTableTimeRange.skey, skey);
        pExtW->orgTableTimeRange.ekey = TMAX(pExtW->orgTableTimeRange.ekey, ekey);
      }
      if (extWinShouldSkipNonStreamPartitionBlock(pExtW, pTaskInfo, &pBlock->info.id)) {
        continue;
      }
      code = extWinSwitchInitCtxs(pOperator, pExtW, &pBlock->info.id);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      code = extWinBroadcastAppendBlock(pOperator, pExtW, pExtW->pTGrpCtx->pCCtx, pBlock);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  int32_t code = extWinBroadcastReplayAll(pOperator, pExtW);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pExtW->mode == EEXT_MODE_AGG) {
    SStreamRuntimeFuncInfo* pStream = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
    return pStream->isMultiGroupCalc ? extWinAggHandleMultiTGrpEmptyWins(pOperator, pExtW)
                                     : extWinAggHandleEmptyWins(pOperator, NULL, true, NULL);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinOpen(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator) && !pOperator->pOperatorGetParam) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t                  code = 0;
  int32_t                  lino = 0;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SOperatorInfo*           pDownstream = pOperator->pDownstream[0];
  SExternalWindowOperator* pExtW = pOperator->info;
  SExprSupp*               pSup = &pOperator->exprSupp;
  SStreamRuntimeFuncInfo*  pInfo = pTaskInfo->pStreamRuntimeInfo ? &pTaskInfo->pStreamRuntimeInfo->funcInfo : NULL;

  if (pOperator->pOperatorGetParam) {
    SOperatorParam*               pParam = (SOperatorParam*)(pOperator->pOperatorGetParam);
    SOperatorParam*               pDownParam = (SOperatorParam*)(pOperator->pDownstreamGetParams[0]);
    SExchangeOperatorParam*       pExecParam = NULL;
    SExternalWindowOperatorParam* pExtPram = (SExternalWindowOperatorParam*)pParam->value;

    TAOS_CHECK_EXIT(extWinEnsureDynParamOpenCtx(pExtW));

    if (pExtW->pTGrpCtx->pCCtx->pWins) {
      taosArrayDestroy(pExtW->pTGrpCtx->pCCtx->pWins);
    }
    taosArrayDestroy(pExtW->pTGrpCtx->pCCtx->pWindowConditionResults);
    extWinDestroyBroadcastState(&pExtW->pTGrpCtx->pCCtx->broadcast);

    extWinInitDynParamCGrpCtx(pExtW->pTGrpCtx->pCCtx);
    pExtW->pTGrpCtx->pCCtx->broadcast.pStat = &pExtW->stat;
    pExtW->pTGrpCtx->pCCtx->pWins = pExtPram->ExtWins;

    pExtPram->ExtWins = NULL;
    taosArrayDestroy(pExtW->pNullOutputSlots);
    pExtW->pNullOutputSlots = pExtPram->pNullOutputSlots;
    pExtPram->pNullOutputSlots = NULL;
    TAOS_CHECK_EXIT(extWinPrepareWindowDecisions(pOperator, pExtW, pExtW->pTGrpCtx->pCCtx));
    pExtW->outWinIdx = 0;
    pExtW->isDynWindow = true;
    if (pExtW->hasCountFunc && pExtW->pEmptyInputBlock == NULL) {
      TAOS_CHECK_EXIT(extWinCreateEmptyInputBlock(pOperator, &pExtW->pEmptyInputBlock));
    }
    pExtW->orgTableTimeRange.skey = INT64_MAX;
    pExtW->orgTableTimeRange.ekey = INT64_MIN;

    QUERY_CHECK_CONDITION(pOperator->numOfDownstream == 1, code, lino, _exit, TSDB_CODE_INVALID_PARA)

    code = extWinResetCompletedDownstream(pDownstream);
    QUERY_CHECK_CODE(code, lino, _exit);

    if (QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN == pDownstream->operatorType) {
      SExtWinTimeWindow* pFirstWin = taosArrayGet(pExtW->pTGrpCtx->pCCtx->pWins, 0);
      SExtWinTimeWindow* pLastWin = taosArrayGetLast(pExtW->pTGrpCtx->pCCtx->pWins);
      QUERY_CHECK_NULL(pFirstWin, code, lino, _exit, terrno)
      QUERY_CHECK_NULL(pLastWin, code, lino, _exit, terrno)
      STimeWindow window = {.skey = pFirstWin->tw.skey, .ekey = pLastWin->tw.ekey};
      if (!extWinApplyRangeToFederatedBatchParam(pDownParam, window)) {
        SOperatorParam* pNewParam = NULL;
        TAOS_CHECK_EXIT(extWinBuildFederatedRangeParam(&pNewParam, window));
        freeOperatorParam(pDownParam, OP_GET_PARAM);
        pDownParam = pNewParam;
      }
      pOperator->pDownstreamGetParams[0] = pDownParam;
    }

    switch (pDownParam->opType) {
      case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE: {
        pExecParam = (SExchangeOperatorParam*)((SOperatorParam*)(pOperator->pDownstreamGetParams[0]))->value;
        if (!pExecParam->multiParams) {
          pExecParam->basic.vgId = pExtW->orgTableVgId;
          taosArrayClear(pExecParam->basic.uidList);
          QUERY_CHECK_NULL(taosArrayPush(pExecParam->basic.uidList, &pExtW->orgTableUid), code, lino, _exit, terrno)
        }
        break;
      }
      default:
        break;
    }

    freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
    pOperator->pOperatorGetParam = NULL;
  }

  if (pTaskInfo->execModel != OPTR_EXEC_MODEL_STREAM && pInfo != NULL && !pInfo->isMultiGroupCalc &&
      (pInfo->pStreamPesudoFuncVals == NULL || taosArrayGetSize(pInfo->pStreamPesudoFuncVals) == 0)) {
    goto _input_done;
  }

  if (pExtW->inputMode == EXT_WIN_INPUT_BROADCAST) {
    if (!extWinUsesStreamingBroadcast(pOperator, pExtW)) {
      TAOS_CHECK_EXIT(extWinBroadcastOpen(pOperator, pExtW));
    }
    goto _input_done;
  }

  while (1) {
    SSDataBlock* pBlock = QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN == pDownstream->operatorType
                              ? getNextBlockFromDownstream(pOperator, 0)
                              : getNextBlockFromDownstreamRemainDetach(pOperator, 0);
    if (pBlock == NULL) {
      if (!pExtW->isDynWindow) {
        TAOS_CHECK_EXIT(extWinPrepareStaticEofGroups(pOperator, pExtW));
      }
      if (EEXT_MODE_AGG == pExtW->mode) {
        if (pInfo && pInfo->isMultiGroupCalc) {
          TAOS_CHECK_EXIT(extWinAggHandleMultiTGrpEmptyWins(pOperator, pExtW));
        } else {
          TAOS_CHECK_EXIT(extWinAggHandleEmptyWins(pOperator, pBlock, true, NULL));
        }
      }
      if (pExtW->inputMode == EXT_WIN_INPUT_WINDOW_BOUND &&
          (pExtW->mode == EEXT_MODE_SCALAR || pExtW->mode == EEXT_MODE_INDEFR_FUNC)) {
        pExtW->nonAggInputDone = true;
      }

      break;
    }

    if (pExtW->isDynWindow) {
      TSKEY skey = 0;
      TSKEY ekey = 0;
      code = getTimeWindowOfBlock(pBlock, pExtW->primaryTsIndex, &skey, &ekey);
      QUERY_CHECK_CODE(code, lino, _exit);
      pExtW->orgTableTimeRange.skey = TMIN(pExtW->orgTableTimeRange.skey, skey);
      pExtW->orgTableTimeRange.ekey = TMAX(pExtW->orgTableTimeRange.ekey, ekey);
    }

    printDataBlock(pBlock, __func__, pTaskInfo->id.str, pTaskInfo->id.queryId);

    qInfo("%s ext window mode:%s baseGrp:%" PRIu64 " grp:%" PRIu64 " got %" PRId64 " rows from downstream",
        GET_TASKID(pTaskInfo), extWinModeStr(pExtW->mode), pBlock->info.id.baseGId, pBlock->info.id.groupId, pBlock->info.rows);

    if (extWinShouldSkipNonStreamPartitionBlock(pExtW, pTaskInfo, &pBlock->info.id)) {
      qDebug("%s skip unmatched non-stream partition block, baseGrp:%" PRIu64 " grp:%" PRIu64, GET_TASKID(pTaskInfo),
             pBlock->info.id.baseGId, pBlock->info.id.groupId);
      continue;
    }

    TAOS_CHECK_EXIT(extWinSwitchInitCtxs(pOperator, pExtW, &pBlock->info.id));

    // Reset block-local traversal state for each new input block, but preserve
    // blkWinStartIdx so a window spanning multiple blocks can continue from the
    // same logical window on the next block.
    extWinResetBlockCalcState(pExtW->pTGrpCtx ? pExtW->pTGrpCtx->pCCtx : NULL);
    extWinPrepareBlockScan(pOperator, pBlock);

    bool nonAggOutputReady = false;
    switch (pExtW->mode) {
      case EEXT_MODE_SCALAR:
        TAOS_CHECK_EXIT(extWinProjectOpen(pOperator, pBlock));
        TAOS_CHECK_EXIT(extWinNonAggOutputReady(pOperator, pExtW, &nonAggOutputReady));
        if (nonAggOutputReady) {
          goto _exit;
        }
        break;
      case EEXT_MODE_AGG:
        TAOS_CHECK_EXIT(extWinAggOpen(pOperator, pBlock));
        break;
      case EEXT_MODE_INDEFR_FUNC:
        TAOS_CHECK_EXIT(extWinIndefRowsOpen(pOperator, pBlock));
        TAOS_CHECK_EXIT(extWinNonAggOutputReady(pOperator, pExtW, &nonAggOutputReady));
        if (nonAggOutputReady) {
          goto _exit;
        }
        break;
      default:
        break;
    }
  }

_input_done:
  if (pOperator->pOperatorGetParam) {
    freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
    pOperator->pOperatorGetParam = NULL;
  }
  OPTR_SET_OPENED(pOperator);

  extWinPrepareForOutput(pOperator, pExtW);

_exit:

  extWinEndClearCtxs(pExtW, pTaskInfo);

  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  return code;
}

static int32_t extWinNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                  code = 0;
  int32_t                  lino = 0;
  SExternalWindowOperator* pExtW = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  bool                     multiGroupCalc =
      pTaskInfo->pStreamRuntimeInfo != NULL && pTaskInfo->pStreamRuntimeInfo->funcInfo.isMultiGroupCalc;
  TAOS_CHECK_EXIT(extWinValidateMultiGroupProtocolMode(pOperator));

  if (pOperator->status == OP_EXEC_DONE && !pOperator->pOperatorGetParam) {
    *ppRes = NULL;
    return code;
  }

  if (pOperator->pOperatorGetParam) {
    if (pOperator->status == OP_EXEC_DONE) {
      pOperator->status = OP_NOT_OPENED;
    }
  }

  if (!multiGroupCalc || pExtW->lastTGrpId == 0 || extWinGroupIsFinalized(pExtW, pExtW->lastTGrpId)) {
    extWinDetachPublishedRuntimeAliases(pTaskInfo, pExtW);
  }
  SArray** ppRuntimeIdx = NULL;
  if (NULL != pTaskInfo->pStreamRuntimeInfo) {
    ppRuntimeIdx = &pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx;
  }
  extWinRecycleOutputBlkNode(pExtW, ppRuntimeIdx, &pExtW->pLastBlkNode);

  while (1) {
    if (pOperator->status == OP_NOT_OPENED) {
      TAOS_CHECK_EXIT(pOperator->fpSet._openFn(pOperator));
    }

    if (pExtW->mode == EEXT_MODE_SCALAR || pExtW->mode == EEXT_MODE_INDEFR_FUNC) {
      if (extWinUsesStreamingBroadcast(pOperator, pExtW)) {
        TAOS_CHECK_EXIT(extWinBroadcastStreamingNext(pOperator, pExtW, ppRes));
      } else {
        TAOS_CHECK_EXIT(extWinNonAggOutputRes(pOperator, ppRes));
      }
      if (NULL == *ppRes) {
        if (multiGroupCalc && !pExtW->groupScanDone) {
          continue;
        }
        setOperatorCompleted(pOperator);
        extWinFreeResultRow(pExtW);
      }
    } else {
      TAOS_CHECK_EXIT(extWinAggOutputRes(pOperator, ppRes));
      if (NULL != *ppRes) {
        break;
      }

      if (multiGroupCalc) {
        if (!pExtW->groupScanDone) {
          continue;
        }
        setOperatorCompleted(pOperator);
        extWinFreeResultRow(pExtW);
        break;
      }

      if (pExtW->pTGrpCtx == NULL || pExtW->pTGrpCtx->pCCtx == NULL ||
          (pExtW->isDynWindow ? (pExtW->pTGrpCtx->pCCtx->outWinIdx >= taosArrayGetSize(pExtW->pTGrpCtx->pCCtx->pWins))
                              : (pExtW->pTGrpCtx->pCCtx->outWinNum >= pExtW->pTGrpCtx->pCCtx->outWinTotalNum))) {
        setOperatorCompleted(pOperator);
        if (pTaskInfo->pStreamRuntimeInfo) {
          extWinFreeResultRow(pExtW);
        }
        break;
      }
    }
    break;
  }

  if (*ppRes && (*ppRes)->info.rows > 0) {
    for (int32_t i = 0; pExtW->pNullOutputSlots != NULL &&
                        i < taosArrayGetSize(pExtW->pNullOutputSlots); ++i) {
      int32_t* pSlotId = taosArrayGet(pExtW->pNullOutputSlots, i);
      if (pSlotId != NULL && *pSlotId >= 0 && *pSlotId < taosArrayGetSize((*ppRes)->pDataBlock)) {
        SColumnInfoData* pCol = taosArrayGet((*ppRes)->pDataBlock, *pSlotId);
        if (pCol != NULL) {
          colDataSetNNULL(pCol, 0, (*ppRes)->info.rows);
        }
      }
    }
    qDebug("%s tgrp %" PRIu64 " cgrp %" PRIu64 " ext window return block with %" PRId64 " rows", 
      GET_TASKID(pTaskInfo), pExtW->lastTGrpId, pExtW->lastCGrpId, (*ppRes)->info.rows);
        
    pOperator->resultInfo.totalRows += (*ppRes)->info.rows;
    printDataBlock(*ppRes, __func__, GET_TASKID(pTaskInfo), pTaskInfo->id.queryId);
  }

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pTaskInfo), __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  if ((*ppRes) && (*ppRes)->info.rows <= 0 && !pExtW->boundaryReady) {
    *ppRes = NULL;
  }
  pExtW->boundaryReady = false;

  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM && (*ppRes)) {
    printDataBlock(*ppRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo), pTaskInfo->id.queryId);
  }

  return code;
}

static int32_t extWinNextExt(SOperatorInfo* pOperator, SOperatorParam* pParam, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (NULL != pParam) {
    pParam->opType = pOperator->operatorType;
  }
  code = optrDefaultGetNextExtFn(pOperator, pParam, ppRes);
  QUERY_CHECK_CODE(code, lino, _return);
  return code;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

#if defined(BUILD_TEST)
int32_t extWinTestGetBroadcastStat(SOperatorInfo* pOperator, int64_t* pScanBlocks, int64_t* pScanRows,
                                   int64_t* pReplayBlocks, int64_t* pReplayRows, int64_t* pActualBudget,
                                   int64_t* pPayloadPeak, int64_t* pFlushBytes, int64_t* pLoadBytes,
                                   int64_t* pPredicateEvals) {
  if (pOperator == NULL || pScanBlocks == NULL || pScanRows == NULL || pReplayBlocks == NULL || pReplayRows == NULL ||
      pActualBudget == NULL || pPayloadPeak == NULL || pFlushBytes == NULL || pLoadBytes == NULL ||
      pPredicateEvals == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SExternalWindowOperator* pExtW = extWinGetCoreInfo(pOperator);
  if (pExtW == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pScanBlocks = pExtW->stat.broadcastScanBlocks;
  *pScanRows = pExtW->stat.broadcastScanRows;
  *pReplayBlocks = pExtW->stat.broadcastReplayBlocks;
  *pReplayRows = pExtW->stat.broadcastReplayRows;
  *pActualBudget = pExtW->stat.broadcastStoreBudget;
  *pPayloadPeak = pExtW->stat.broadcastPayloadPeak;
  *pFlushBytes = pExtW->stat.broadcastFlushBytes;
  *pLoadBytes = pExtW->stat.broadcastLoadBytes;
  if (pExtW->pBroadcastPool != NULL) {
    SDiskbasedBufStatis current = winInputPoolGetStatis(pExtW->pBroadcastPool);
    *pFlushBytes += current.flushBytes - pExtW->broadcastCollectedStatis.flushBytes;
    *pLoadBytes += current.loadBytes - pExtW->broadcastCollectedStatis.loadBytes;
  }
  *pPredicateEvals = pExtW->stat.windowConditionEvals;
  return TSDB_CODE_SUCCESS;
}

int32_t extWinTestGetBroadcastRegisteredPageCount(SOperatorInfo* pOperator, int32_t* pRegisteredPages) {
  if (pOperator == NULL || pRegisteredPages == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SExternalWindowOperator* pExtW = extWinGetCoreInfo(pOperator);
  if (pExtW == NULL || pExtW->pBroadcastPool == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *pRegisteredPages = winInputPoolTestGetRegisteredPageCount(pExtW->pBroadcastPool);
  return TSDB_CODE_SUCCESS;
}

int32_t extWinTestReplaceActiveBroadcastState(SOperatorInfo* pOperator, SSDataBlock* pBlock,
                                              int32_t* pRegisteredPages) {
  if (pOperator == NULL || pBlock == NULL || pRegisteredPages == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SExternalWindowOperator* pExtW = extWinGetCoreInfo(pOperator);
  if (pExtW == NULL || pExtW->pTGrpCtx == NULL || pExtW->pTGrpCtx->pCCtx == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SExtWinCalcGrpCtx* pCCtx = pExtW->pTGrpCtx->pCCtx;
  extWinDestroyBroadcastState(&pCCtx->broadcast);
  pCCtx->broadcast.pStat = &pExtW->stat;
  int32_t code = extWinBroadcastAppendBlock(pOperator, pExtW, pCCtx, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  *pRegisteredPages = winInputPoolTestGetRegisteredPageCount(pExtW->pBroadcastPool);
  return TSDB_CODE_SUCCESS;
}

int32_t extWinTestGetBroadcastPageMeta(SOperatorInfo* pOperator, uint64_t triggerGroupId, uint64_t calcGroupId,
                                       int32_t pageIndex, int32_t* pPageCount, SBlockID* pId, STimeWindow* pWindow,
                                       int64_t* pVersion, int16_t* pDataLoad, uint8_t* pScanFlag) {
  if (pOperator == NULL || pageIndex < 0 || pPageCount == NULL || pId == NULL || pWindow == NULL || pVersion == NULL ||
      pDataLoad == NULL || pScanFlag == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SExternalWindowOperator* pExtW = extWinGetCoreInfo(pOperator);
  SExtWinTrigGrpCtx*       pTCtx = extWinLookupTriggerGroup(pExtW, triggerGroupId);
  SExtWinCalcGrpCtx*       pCCtx = extWinLookupCalcGroup(pTCtx, calcGroupId);
  if (pCCtx == NULL || pCCtx->broadcast.pPageMeta == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  *pPageCount = taosArrayGetSize(pCCtx->broadcast.pPageMeta);
  SExtWinBroadcastPageMeta* pMeta = taosArrayGet(pCCtx->broadcast.pPageMeta, pageIndex);
  if (pMeta == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  *pId = pMeta->id;
  *pWindow = pMeta->window;
  *pVersion = pMeta->version;
  *pDataLoad = pMeta->dataLoad;
  *pScanFlag = pMeta->scanFlag;
  return TSDB_CODE_SUCCESS;
}

int32_t extWinTestGetBroadcastReplayObservation(SOperatorInfo* pOperator, int32_t observationIndex,
                                                int32_t* pObservationCount, int32_t* pPageIndex, SBlockID* pId,
                                                STimeWindow* pWindow, int64_t* pVersion, int16_t* pDataLoad,
                                                uint8_t* pScanFlag) {
  if (pOperator == NULL || observationIndex < 0 || pObservationCount == NULL || pPageIndex == NULL || pId == NULL ||
      pWindow == NULL || pVersion == NULL || pDataLoad == NULL || pScanFlag == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SExternalWindowOperator* pExtW = extWinGetCoreInfo(pOperator);
  if (pExtW == NULL || pExtW->pBroadcastReplayObservations == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  *pObservationCount = taosArrayGetSize(pExtW->pBroadcastReplayObservations);
  SExtWinBroadcastReplayObservation* pObservation = taosArrayGet(pExtW->pBroadcastReplayObservations, observationIndex);
  if (pObservation == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  *pPageIndex = pObservation->pageIndex;
  *pId = pObservation->id;
  *pWindow = pObservation->window;
  *pVersion = pObservation->version;
  *pDataLoad = pObservation->dataLoad;
  *pScanFlag = pObservation->scanFlag;
  return TSDB_CODE_SUCCESS;
}

int32_t extWinTestRunBroadcastPayloadTransition(int64_t firstContribution, int64_t secondContribution,
                                                int64_t* pCurrentAfterFirst, int64_t* pCurrentAfterSecond,
                                                int64_t* pCurrentAfterSecondDestroy, int64_t* pPeakAfterSecondDestroy) {
  if (firstContribution < 0 || secondContribution < 0 || pCurrentAfterFirst == NULL || pCurrentAfterSecond == NULL ||
      pCurrentAfterSecondDestroy == NULL || pPeakAfterSecondDestroy == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SExtWindowStat        stat = {.broadcastStoreBudget = INT64_MAX};
  SExtWinBroadcastState first = {.pStat = &stat};
  SExtWinBroadcastState second = {.pStat = &stat};
  extWinAccountBroadcastPayloadAppend(&first, firstContribution);
  *pCurrentAfterFirst = extWinPayloadSumClamp(&stat.broadcastPayloadCurrent);
  extWinAccountBroadcastPayloadAppend(&second, secondContribution);
  *pCurrentAfterSecond = extWinPayloadSumClamp(&stat.broadcastPayloadCurrent);
  extWinDestroyBroadcastState(&second);
  *pCurrentAfterSecondDestroy = extWinPayloadSumClamp(&stat.broadcastPayloadCurrent);
  *pPeakAfterSecondDestroy = stat.broadcastPayloadPeak;
  extWinDestroyBroadcastState(&first);
  return TSDB_CODE_SUCCESS;
}

int32_t extWinTestGetWindowConditionEvals(SOperatorInfo* pOperator, int64_t* pEvaluations) {
  if (pOperator == NULL || pEvaluations == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SExternalWindowOperator* pExtW = extWinGetCoreInfo(pOperator);
  if (pExtW == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *pEvaluations = pExtW->stat.windowConditionEvals;
  return TSDB_CODE_SUCCESS;
}

int32_t extWinTestGetWindowDecisionState(SOperatorInfo* pOperator, int32_t* pCalcContexts, int32_t* pReadyContexts,
                                         int64_t* pTotalWindows, int64_t* pSettledWindows) {
  if (pOperator == NULL || pCalcContexts == NULL || pReadyContexts == NULL || pTotalWindows == NULL ||
      pSettledWindows == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SExternalWindowOperator* pExtW = extWinGetCoreInfo(pOperator);
  if (pExtW == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pCalcContexts = 0;
  *pReadyContexts = 0;
  *pTotalWindows = 0;
  *pSettledWindows = 0;
  int32_t             triggerIter = 0;
  SExtWinTrigGrpCtx** ppTCtx = tSimpleHashIterate(pExtW->pGroupRegistry, NULL, &triggerIter);
  while (ppTCtx != NULL) {
    int32_t            calcIter = 0;
    void*              pCalcIter = NULL;
    SExtWinCalcGrpCtx* pCCtx = extWinIterateCalcGroup(*ppTCtx, &pCalcIter, &calcIter);
    while (pCCtx != NULL) {
      ++*pCalcContexts;
      *pReadyContexts += pCCtx->windowDecisionsReady ? 1 : 0;
      *pTotalWindows += taosArrayGetSize(pCCtx->pWins);
      *pSettledWindows += pCCtx->settledThroughWinIdx + 1;
      pCCtx = extWinIterateCalcGroup(*ppTCtx, &pCalcIter, &calcIter);
    }
    ppTCtx = tSimpleHashIterate(pExtW->pGroupRegistry, ppTCtx, &triggerIter);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t extWinTestObserveAliasOnOpen(SOperatorInfo* pOperator) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;

  pExtW->testAliasClearedOnOpen = NULL == pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx;
  OPTR_SET_OPENED(pOperator);
  return TSDB_CODE_SUCCESS;
}

int32_t extWinTestOutputAliasLifecycle(SList* pOutputList, SList* pFreeBlocks, bool reset, bool* pAliasEstablished,
                                       bool* pAliasCleared) {
  SExternalWindowOperator  extW = {0};
  SExtWinTrigGrpCtx        tCtx = {0};
  SExtWinCalcGrpCtx        cCtx = {0};
  SOperatorInfo            operatorInfo = {0};
  SExecTaskInfo            taskInfo = {0};
  SStreamRuntimeInfo       runtimeInfo = {0};
  SExternalWindowPhysiNode physiNode = {0};
  SSDataBlock*             pRes = NULL;
  SArray*                  pOutputBlocks = taosArrayInit(1, sizeof(SList*));
  SArray*                  pWins = taosArrayInit(1, sizeof(SExtWinTimeWindow));
  int32_t                  code = TSDB_CODE_SUCCESS;
  SExtWinTimeWindow        win = {.resWinIdx = 0};

  if (NULL == pOutputBlocks || NULL == pWins || NULL == taosArrayPush(pOutputBlocks, &pOutputList) ||
      NULL == taosArrayPush(pWins, &win)) {
    code = terrno;
    goto _exit;
  }

  SListNode* pNode = listHead(pOutputList);
  SArray*    pIdx = NULL == pNode ? NULL : *(SArray**)((SArray**)pNode->data + 1);
  cCtx.pWins = pWins;
  cCtx.lastWinIdx = 0;
  cCtx.outWinLastIdx = -1;
  tCtx.pCCtx = &cCtx;
  extW.mode = EEXT_MODE_SCALAR;
  extW.resWinIdx = 1;
  extW.pOutputBlocks = pOutputBlocks;
  extW.pFreeBlocks = pFreeBlocks;
  extW.pTGrpCtx = &tCtx;

  taskInfo.pStreamRuntimeInfo = &runtimeInfo;
  operatorInfo.info = &extW;
  operatorInfo.pTaskInfo = &taskInfo;
  operatorInfo.pPhyNode = &physiNode;

  code = extWinNonAggOutputRes(&operatorInfo, &pRes);
  if (TSDB_CODE_SUCCESS != code) {
    goto _exit;
  }

  *pAliasEstablished = runtimeInfo.funcInfo.pStreamBlkWinIdx == pIdx && NULL != extW.pLastBlkNode;
  if (reset) {
    code = resetExternalWindowOperator(&operatorInfo);
    *pAliasCleared = NULL == runtimeInfo.funcInfo.pStreamBlkWinIdx && NULL == extW.pLastBlkNode;
  } else {
    operatorInfo.status = OP_NOT_OPENED;
    operatorInfo.fpSet._openFn = extWinTestObserveAliasOnOpen;
    code = extWinNext(&operatorInfo, &pRes);
    *pAliasCleared = extW.testAliasClearedOnOpen && NULL == extW.pLastBlkNode;
  }

_exit:
  cleanupExprSuppWithoutFilter(&extW.scalarSupp);
  colDataDestroy(&extW.twAggSup.timeWindowData);
  taosArrayDestroy(pWins);
  taosArrayDestroy(pOutputBlocks);
  return code;
}
#endif
static int32_t extWinInitNonStreamWindowDataFromBlock(SExternalWindowPhysiNode* pPhynode, SExecTaskInfo* pTaskInfo, STimeWindow* pTimeRange);
static int32_t extWinValidateNonStreamBlock(SSDataBlock* pBlock, SColumnInfoData** ppStartCol,
                                            SColumnInfoData** ppEndCol, int32_t* pNumRows, int32_t* pNumCols);
static int32_t extWinCheckMonotonicWstart(bool hasPrevStart, int64_t prevStart, int64_t currStart, int32_t row);
static int32_t extWinBuildExternalWindowDataForRow(SSDataBlock* pBlock, int32_t numCols, int32_t row,
                                                   SArray** ppExternalData);
static int32_t extWinBuildTriggerParamForRow(SSDataBlock* pBlock, SColumnInfoData* pStartCol, SColumnInfoData* pEndCol,
                                             int32_t numCols, int32_t row, SSTriggerCalcParam* pParam);

int32_t createExternalWindowOperator(SOperatorInfo* pDownstream, SPhysiNode* pNode, SExecTaskInfo* pTaskInfo,
                                     SOperatorInfo** pOptrOut) {
  SExternalWindowPhysiNode* pPhynode = (SExternalWindowPhysiNode*)pNode;
  QRY_PARAM_CHECK(pOptrOut);
  int32_t                  code = 0;
  int32_t                  lino = 0;
  bool                     isInStream = true;
  STimeWindow              nonStreamExtWinRange = {.skey = INT64_MAX, .ekey = INT64_MIN};
  SExternalWindowOperator* pExtW = taosMemoryCalloc(1, sizeof(SExternalWindowOperator));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  pOperator->pPhyNode = pNode;
  if (!pExtW || !pOperator) {
    code = terrno;
    lino = __LINE__;
    goto _error;
  }
  initOperatorCostInfo(pOperator);
  pExtW->needGroupSort = extWinNeedGroupSort(pPhynode, pTaskInfo);
  // In non-stream (batch) mode, temporarily disable calcWithPartition to tolerate
  // upstream that does not provide distinct C-group ids (groupId==baseGId).
  // Caller may decide to turn it back on once upstream is ready.
  pExtW->calcWithPartition = pPhynode->calcWithPartition;
  pExtW->matchPartitionBy = pPhynode->matchPartitionBy;
  // pExtW->calcWithPartition = (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM) ? pPhynode->calcWithPartition : false;
  pExtW->extWinSplit = pPhynode->extWinSplit;
  pExtW->inputMode = pPhynode->extWinInputMode;
  pExtW->pWindowConditions = pPhynode->pWindowConditions;
  setOperatorInfo(pOperator, "ExternalWindowOperator", QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW, true, OP_NOT_OPENED,
                  pExtW, pTaskInfo);
                  
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhynode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pExtW->binfo, pResBlock);

  pExtW->primaryTsIndex = ((SColumnNode*)pPhynode->window.pTspk)->slotId;
  pExtW->mode = pPhynode->window.indefRowsFunc ? EEXT_MODE_INDEFR_FUNC : (pPhynode->window.pFuncs ? EEXT_MODE_AGG : EEXT_MODE_SCALAR);
  pExtW->fillMode = extWinResolveFillMode(pPhynode->extFill.mode, pTaskInfo);
  pExtW->pFillExprs = pPhynode->extFill.pFillExprs;
  pExtW->pFillValues = pPhynode->extFill.pFillValues;
  pPhynode->window.node.inputTsOrder = TSDB_ORDER_ASC;
  pPhynode->window.node.outputTsOrder = pPhynode->window.node.inputTsOrder;
  setOptrBasicInfoOrder(&pExtW->binfo, &pPhynode->window.node);
  pExtW->isDynWindow = false;

  qDebug("%s create extWin operator, execModel:%d, phySubquery:%p", GET_TASKID(pTaskInfo), pTaskInfo->execModel,
        pPhynode->pSubquery);

  if (pTaskInfo->pStreamRuntimeInfo != NULL){
    pTaskInfo->pStreamRuntimeInfo->funcInfo.withExternalWindow = true;
  }
  
  // pExtW->limitInfo = (SLimitInfo){0};
  // initLimitInfo(pPhynode->window.node.pLimit, pPhynode->window.node.pSlimit, &pExtW->limitInfo);

  if (pTaskInfo->execModel != OPTR_EXEC_MODEL_STREAM) {
    isInStream = false;
    // If pre-init has not been performed, initialize from subquery blocks here.
    if (pTaskInfo->pStreamRuntimeInfo == NULL ||
        pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcInfos == NULL) {
      code = extWinInitNonStreamWindowDataFromBlock(pPhynode, pTaskInfo, &nonStreamExtWinRange);
      if (code != TSDB_CODE_SUCCESS) {
        lino = __LINE__;
        goto _error;
      }
    }
  }

  if (pExtW->mode == EEXT_MODE_SCALAR) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pPhynode->window.pProjs, NULL, &pScalarExprInfo, &numOfScalarExpr);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pExtW->scalarSupp, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);

    pExtW->pOutputBlocks = taosArrayInit(STREAM_CALC_REQ_MAX_WIN_NUM, POINTER_BYTES);
    if (!pExtW->pOutputBlocks) QUERY_CHECK_CODE(terrno, lino, _error);

    pExtW->pFreeBlocks = tdListNew(POINTER_BYTES * 2);
    QUERY_CHECK_NULL(pExtW->pFreeBlocks, code, lino, _error, terrno);
  } else if (pExtW->mode == EEXT_MODE_AGG) {
    if (pPhynode->window.pExprs != NULL) {
      int32_t    num = 0;
      SExprInfo* pSExpr = NULL;
      code = createExprInfo(pPhynode->window.pExprs, NULL, &pSExpr, &num);
      QUERY_CHECK_CODE(code, lino, _error);
    
      code = initExprSupp(&pExtW->scalarSupp, pSExpr, num, &pTaskInfo->storageAPI.functionStore);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }
      checkIndefRowsFuncs(&pExtW->scalarSupp);
    }

    if (pPhynode->window.pProjs != NULL) {
      code = extWinInitExprSupp(&pExtW->projSupp, pPhynode->window.pProjs, pTaskInfo);
      QUERY_CHECK_CODE(code, lino, _error);
    }
    
    size_t keyBufSize = sizeof(int64_t) * 2 + POINTER_BYTES;
    initResultSizeInfo(&pOperator->resultInfo, 4096);
    //code = blockDataEnsureCapacity(pExtW->binfo.pRes, pOperator->resultInfo.capacity);
    //QUERY_CHECK_CODE(code, lino, _error);

    pExtW->pWinRowIdx = taosArrayInit(4096, sizeof(int64_t));
    TSDB_CHECK_NULL(pExtW->pWinRowIdx, code, lino, _error, terrno);
    
    int32_t num = 0;
    SExprInfo* pExprInfo = NULL;
    code = createExprInfo(pPhynode->window.pFuncs, NULL, &pExprInfo, &num);
    QUERY_CHECK_CODE(code, lino, _error);
    pOperator->exprSupp.hasWindow = true;
    pOperator->exprSupp.hasWindowOrGroup = true;
    code = initAggSup(&pOperator->exprSupp, &pExtW->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str, 0, 0);
    QUERY_CHECK_CODE(code, lino, _error);

    code = filterInitFromNode((SNode*)pNode->pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                              pTaskInfo->pStreamRuntimeInfo);
    QUERY_CHECK_CODE(code, lino, _error);

    code = extWinBuildFillMatchInfo(&pExtW->fillMatchInfo, pPhynode->extFill.pFillExprs);
    QUERY_CHECK_CODE(code, lino, _error);

    if (pExtW->fillMode != FILL_MODE_NONE) {
    }

    nodesWalkExprs(pPhynode->window.pFuncs, extWinHasCountLikeFunc, &pExtW->hasCountFunc);
    if (pExtW->fillMode != FILL_MODE_NONE || (pExtW->hasCountFunc && pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM)) {
      code = extWinCreateEmptyInputBlock(pOperator, &pExtW->pEmptyInputBlock);
      QUERY_CHECK_CODE(code, lino, _error);
      qDebug("%s ext window prepared empty input block, fillMode:%d", pOperator->pTaskInfo->id.str, pExtW->fillMode);
    } else {
      qDebug("%s ext window have CountLikeFunc: %d. IsInStream:%d", pOperator->pTaskInfo->id.str, pExtW->hasCountFunc,
             pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM);
    }

    code = initExecTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pTaskInfo->window);
    QUERY_CHECK_CODE(code, lino, _error);
  } else {
    size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
    
    if (pPhynode->window.pExprs != NULL) {
      int32_t    num = 0;
      SExprInfo* pSExpr = NULL;
      code = createExprInfo(pPhynode->window.pExprs, NULL, &pSExpr, &num);
      QUERY_CHECK_CODE(code, lino, _error);
    
      code = initExprSupp(&pExtW->scalarSupp, pSExpr, num, &pTaskInfo->storageAPI.functionStore);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }
    }
    
    int32_t    numOfExpr = 0;
    SExprInfo* pExprInfo = NULL;
    code = createExprInfo(pPhynode->window.pFuncs, NULL, &pExprInfo, &numOfExpr);
    TSDB_CHECK_CODE(code, lino, _error);
    
    code = initAggSup(&pOperator->exprSupp, &pExtW->aggSup, pExprInfo, numOfExpr, keyBufSize, pTaskInfo->id.str,
                              NULL, &pTaskInfo->storageAPI.functionStore);
    TSDB_CHECK_CODE(code, lino, _error);
    pOperator->exprSupp.hasWindowOrGroup = false;
    
    code = filterInitFromNode((SNode*)pNode->pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                              pTaskInfo->pStreamRuntimeInfo);
    TSDB_CHECK_CODE(code, lino, _error);
    
    setOptrBasicInfoOrder(&pExtW->binfo, pNode);
    code = setRowTsColumnOutputInfo(pOperator->exprSupp.pCtx, numOfExpr, &pExtW->pPseudoColInfo);
    TSDB_CHECK_CODE(code, lino, _error);

    pExtW->pOutputBlocks = taosArrayInit(STREAM_CALC_REQ_MAX_WIN_NUM, POINTER_BYTES);
    if (!pExtW->pOutputBlocks) QUERY_CHECK_CODE(terrno, lino, _error);

    pExtW->pFreeBlocks = tdListNew(POINTER_BYTES * 2);
    QUERY_CHECK_NULL(pExtW->pFreeBlocks, code, lino, _error, terrno);  
  }

  pExtW->timeRangeExpr = (STimeRangeNode*)pPhynode->pTimeRange;
  if (pExtW->timeRangeExpr) {
    QUERY_CHECK_NULL(pExtW->timeRangeExpr->pStart, code, lino, _error, TSDB_CODE_STREAM_INTERNAL_ERROR);
    QUERY_CHECK_NULL(pExtW->timeRangeExpr->pEnd, code, lino, _error, TSDB_CODE_STREAM_INTERNAL_ERROR);
  }

  if (pPhynode->isSingleTable) {
    if (!isInStream) {
      pExtW->getWinFp = extWinGetOvlpWin;
      pExtW->multiTableMode = false;
    } else {
      pExtW->getWinFp =
          (pExtW->timeRangeExpr && (pExtW->timeRangeExpr->needCalc ||
                                    (pTaskInfo->pStreamRuntimeInfo->funcInfo.addOptions & CALC_SLIDING_OVERLAP)))
              ? extWinGetOvlpWin
              : extWinGetNoOvlpWin;
      pExtW->multiTableMode = false;
    }
  } else {
    if (!isInStream) {
      pExtW->getWinFp = extWinGetMultiTbOvlpWin;
      pExtW->multiTableMode = true;
    } else {
      pExtW->getWinFp =
          (pExtW->timeRangeExpr && (pExtW->timeRangeExpr->needCalc ||
                                    (pTaskInfo->pStreamRuntimeInfo->funcInfo.addOptions & CALC_SLIDING_OVERLAP)))
              ? extWinGetMultiTbOvlpWin
              : extWinGetMultiTbNoOvlpWin;
      pExtW->multiTableMode = true;
    }
  }
  pExtW->inputHasOrder = pPhynode->inputHasOrder;
  pExtW->orgTableUid = pPhynode->orgTableUid;
  pExtW->orgTableVgId = pPhynode->orgTableVgId;

  code = extWinInitResRows(pExtW, pTaskInfo);
  TSDB_CHECK_CODE(code, lino, _error);

  pExtW->pOperator = pOperator;

  pOperator->fpSet = createOperatorFpSet(extWinOpen, extWinNext, NULL, destroyExternalWindowOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  if (pExtW->mode == EEXT_MODE_AGG) {
    setOperatorRowStateCleanupFn(pOperator, cleanupExternalWindowRowState);
  }

  setOperatorResetStateFn(pOperator, resetExternalWindowOperator);
  code = appendDownstream(pOperator, &pDownstream, 1);
  if (code != 0) {
    goto _error;
  }

  if (!isInStream) {
    code = extWinApplyNonStreamTimeRangeToDownstream(pOperator, &nonStreamExtWinRange);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  *pOptrOut = pOperator;

  qDebug("%s extWin operator created, mode:%s, multiTableMnode:%d, inputHasOrder:%d, inputTsOrder:%d, inputDataOrder:%d, "
    "getWinFp:%s, hasTimeRangeExpr:%d, timeRangeNeedCalc:%d needGroupSort:%d, extWinSplit:%d, calcWithPartition:%d",
    GET_TASKID(pTaskInfo), extWinModeStr(pExtW->mode), pExtW->multiTableMode, pExtW->inputHasOrder, pExtW->binfo.inputTsOrder,
    pExtW->binfo.inputDataOrder, extWinGetWinFpName(pExtW), pExtW->timeRangeExpr ? 1 : 0,
    pExtW->timeRangeExpr ? pExtW->timeRangeExpr->needCalc : -1, pExtW->needGroupSort, pExtW->extWinSplit, pExtW->calcWithPartition);
  
  return code;

_error:

  if (pExtW != NULL) {
    destroyExternalWindowOperatorInfo(pExtW);
  }

  destroyOperatorAndDownstreams(pOperator, &pDownstream, 1);
  pTaskInfo->code = code;
  qError("error happens at %s %d, code:%s", __func__, lino, tstrerror(code));
  return code;
}

// Pre-initialize external-window runtime from subquery results before building children.
// This enables scan operators to see isMultiGroupCalc/curGrpRead and build tablelist with baseGId.
int32_t extWinPreInitFromSubquery(SPhysiNode* pNode, SExecTaskInfo* pTaskInfo) {
  if (pNode == NULL || pTaskInfo == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM) {
    return TSDB_CODE_SUCCESS;  // stream path unaffected
  }
  // If already initialized, skip.
  if (pTaskInfo->pStreamRuntimeInfo &&
      pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcInfos != NULL) {
    return TSDB_CODE_SUCCESS;
  }
  SExternalWindowPhysiNode* pPhynode = (SExternalWindowPhysiNode*)pNode;
  STimeWindow tmpRange = {.skey = INT64_MAX, .ekey = INT64_MIN};
  return extWinInitNonStreamWindowDataFromBlock(pPhynode, pTaskInfo, &tmpRange);
}

static int32_t extWinValidateNonStreamBlock(SSDataBlock* pBlock, SColumnInfoData** ppStartCol,
                                            SColumnInfoData** ppEndCol, int32_t* pNumRows, int32_t* pNumCols) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pBlock, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlock->pDataBlock, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(ppStartCol, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(ppEndCol, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pNumRows, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pNumCols, code, lino, _exit, TSDB_CODE_INVALID_PARA);

  int32_t numRows = pBlock->info.rows;
  int32_t numCols = taosArrayGetSize(pBlock->pDataBlock);
  if (numCols < 2) {
    qError("%s invalid external-window block: expected at least 2 columns, got %d", __func__, numCols);
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
  }

  SColumnInfoData* pStartCol = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, 0);
  SColumnInfoData* pEndCol = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, 1);
  TSDB_CHECK_NULL(pStartCol, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pEndCol, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  if (pStartCol == NULL || pEndCol == NULL || pStartCol->info.type != TSDB_DATA_TYPE_TIMESTAMP ||
      pEndCol->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
    qError("%s invalid external-window block: first two columns must be timestamp, got (%d, %d)", __func__,
           pStartCol ? pStartCol->info.type : -1, pEndCol ? pEndCol->info.type : -1);
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
  }
  *ppStartCol = pStartCol;
  *ppEndCol = pEndCol;
  *pNumRows = numRows;
  *pNumCols = numCols;

_exit:
  return code;
}

static int32_t extWinCheckMonotonicWstart(bool hasPrevStart, int64_t prevStart, int64_t currStart, int32_t row) {
  if (hasPrevStart && currStart < prevStart) {
    qError("%s invalid external-window block: wstart must be monotonic non-decreasing, row:%d, prev:%" PRId64
           ", curr:%" PRId64,
           __func__, row, prevStart, currStart);
    return TSDB_CODE_EXT_WIN_SUB_UNORDERED;
  }

  return TSDB_CODE_SUCCESS;
}

static void extWinDestroyExternalDataValue(void* ptr) {
  SValue* pVal = (SValue*)ptr;
  if (pVal != NULL && (IS_VAR_DATA_TYPE(pVal->type) || pVal->type == TSDB_DATA_TYPE_DECIMAL)) {
    valueClearDatum(pVal, pVal->type);
  }
}

static int32_t extWinBuildExternalWindowDataForRow(SSDataBlock* pBlock, int32_t numCols, int32_t row,
                                                   SArray** ppExternalData) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pExternalData = taosArrayInit(numCols - 2, sizeof(SStreamGroupValue));
  TSDB_CHECK_NULL(pBlock, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlock->pDataBlock, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(ppExternalData, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pExternalData, code, lino, _exit, terrno);

  for (int32_t col = 2; col < numCols; ++col) {
    SColumnInfoData* pColData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, col);
    if (pColData == NULL) {
      continue;
    }

    SStreamGroupValue data = {0};
    SValue* pVal = &data.data;

    pVal->type = pColData->info.type;

    if (!colDataIsNull_s(pColData, row)) {
      void* pData = colDataGetData(pColData, row);
      if (IS_VAR_DATA_TYPE(pVal->type) || pVal->type == TSDB_DATA_TYPE_DECIMAL) {
        int32_t datumLen = IS_VAR_DATA_TYPE(pVal->type) ? calcStrBytesByType(pVal->type, (char*)pData) : pColData->info.bytes;
        void* pDataCopy = taosMemoryMalloc(datumLen);
        TSDB_CHECK_NULL(pDataCopy, code, lino, _exit, terrno);
        memcpy(pDataCopy, pData, datumLen);
        valueSetDatum(pVal, pVal->type, pDataCopy, datumLen);
      } else {
        valueSetDatum(pVal, pVal->type, pData, pColData->info.bytes);
      }
    } else {
      data.isNull = true;
    }

    TSDB_CHECK_NULL(taosArrayPush(pExternalData, &data), code, lino, _exit, terrno);
  }

  *ppExternalData = pExternalData;

_exit:
  if (code) {
    taosArrayDestroyEx(pExternalData, extWinDestroyExternalDataValue);
  }
  return code;
}

static int32_t extWinBuildTriggerParamForRow(SSDataBlock* pBlock, SColumnInfoData* pStartCol, SColumnInfoData* pEndCol,
                                             int32_t numCols, int32_t row, SSTriggerCalcParam* pParam) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  TSDB_CHECK_NULL(pBlock, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pStartCol, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pEndCol, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pParam, code, lino, _exit, TSDB_CODE_INVALID_PARA);

  *pParam = (SSTriggerCalcParam){0};

  if (!colDataIsNull_s(pStartCol, row)) {
    pParam->wstart = *(int64_t*)colDataGetData(pStartCol, row);
  }

  if (!colDataIsNull_s(pEndCol, row)) {
    pParam->wend = *(int64_t*)colDataGetData(pEndCol, row);
  }

  pParam->wduration = pParam->wend - pParam->wstart;
  TAOS_CHECK_EXIT(extWinBuildExternalWindowDataForRow(pBlock, numCols, row, &pParam->pExternalWindowData));

_exit:
  return code;
}

#if defined(BUILD_TEST)
static SArray* extWinGetSSDataBlocksInTest(SExternalWindowPhysiNode* pPhynode);
#endif

static uint64_t extWinGetRemoteResultGroupId(const SSDataBlock* pBlock) {
  if (pBlock == NULL) {
    return 0;
  }

  return (pBlock->info.id.baseGId != 0) ? pBlock->info.id.baseGId : pBlock->info.id.groupId;
}

static bool hasGroupedRemoteResult(SArray* pBlocks) {
  if (pBlocks == NULL) {
    return false;
  }

  int32_t blockNum = taosArrayGetSize(pBlocks);
  for (int32_t blockIdx = 0; blockIdx < blockNum; ++blockIdx) {
    SSDataBlock** ppOne = taosArrayGet(pBlocks, blockIdx);
    if (ppOne == NULL || *ppOne == NULL) {
      continue;
    }

    if (extWinGetRemoteResultGroupId(*ppOne) != 0) {
      return true;
    }
  }

  return false;
}

static int32_t extWinAppendNonGroupedCalcParam(SStreamRuntimeFuncInfo* pRt, SSTriggerCalcParam* pParam,
                                               bool* pHasPrevStart, int64_t* pPrevStart, int32_t row) {
  int32_t code = TSDB_CODE_SUCCESS;

  code = extWinCheckMonotonicWstart(*pHasPrevStart, *pPrevStart, pParam->wstart, row);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  *pPrevStart = pParam->wstart;
  *pHasPrevStart = true;

  void* pRet = taosArrayPush(pRt->pStreamPesudoFuncVals, pParam);
  if (pRet == NULL) {
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extWinGetOrCreateGroupedCalcInfo(SStreamRuntimeFuncInfo* pRt, uint64_t groupId,
                                                SSTriggerGroupCalcInfo** ppGroupInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SSTriggerGroupCalcInfo* pGroupInfo = tSimpleHashGet(pRt->pGroupCalcInfos, &groupId, sizeof(groupId));

  if (pGroupInfo == NULL) {
    SSTriggerGroupCalcInfo info = {0};
    info.pParams = taosArrayInit(4, sizeof(SSTriggerCalcParam));
    TSDB_CHECK_NULL(info.pParams, code, lino, _exit, terrno);

    TAOS_CHECK_EXIT(tSimpleHashPut(pRt->pGroupCalcInfos, &groupId, sizeof(groupId), &info, sizeof(info)));

    pGroupInfo = tSimpleHashGet(pRt->pGroupCalcInfos, &groupId, sizeof(groupId));
    TSDB_CHECK_NULL(pGroupInfo, code, lino, _exit, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }

  *ppGroupInfo = pGroupInfo;

_exit:
  return code;
}

static int32_t extWinAppendGroupedCalcParam(SStreamRuntimeFuncInfo* pRt, uint64_t groupId, SSTriggerCalcParam* pParam,
                                            int32_t row) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SSTriggerGroupCalcInfo* pGroupInfo = NULL;
  bool groupHasPrevStart = false;
  int64_t groupPrevStart = 0;

  TAOS_CHECK_EXIT(extWinGetOrCreateGroupedCalcInfo(pRt, groupId, &pGroupInfo));

  groupHasPrevStart = (taosArrayGetSize(pGroupInfo->pParams) > 0);
  if (groupHasPrevStart) {
    SSTriggerCalcParam* pLast = taosArrayGetLast(pGroupInfo->pParams);
    TSDB_CHECK_NULL(pLast, code, lino, _exit, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    groupPrevStart = pLast->wstart;
  }

  code = extWinCheckMonotonicWstart(groupHasPrevStart, groupPrevStart, pParam->wstart, row);
  if (code != TSDB_CODE_SUCCESS) {
    TAOS_CHECK_EXIT(code);
  }

  TSDB_CHECK_NULL(taosArrayPush(pGroupInfo->pParams, pParam), code, lino, _exit, terrno);

_exit:
  return code;
}

static int32_t extWinBuildNonGroupedCalcInfosFromBlocks(SArray* pBlocks, SStreamRuntimeFuncInfo* pRt,
                                                        STimeWindow* pExtWinTimeRange) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SColumnInfoData* pStartCol = NULL;
  SColumnInfoData* pEndCol = NULL;
  int32_t numRows = 0;
  int32_t numCols = 0;
  bool hasPrevStart = false;
  int64_t prevStart = 0;
  uint64_t prevGroupId = UINT64_MAX;
  int32_t blockNum = taosArrayGetSize(pBlocks);

  for (int32_t blockIdx = 0; blockIdx < blockNum; ++blockIdx) {
    SSDataBlock** ppOne = taosArrayGet(pBlocks, blockIdx);
    TSDB_CHECK_NULL(ppOne, code, lino, _exit, TSDB_CODE_INVALID_PARA);
    SSDataBlock* pBlock = *ppOne;
    TSDB_CHECK_NULL(pBlock, code, lino, _exit, TSDB_CODE_INVALID_PARA);

    TAOS_CHECK_EXIT(extWinValidateNonStreamBlock(pBlock, &pStartCol, &pEndCol, &numRows, &numCols));

    uint64_t groupId = extWinGetRemoteResultGroupId(pBlock);
    if (prevGroupId != groupId) {
      hasPrevStart = false;
      prevGroupId = groupId;
    }

    for (int32_t row = 0; row < numRows; ++row) {
      SSTriggerCalcParam param = {0};
      TAOS_CHECK_EXIT(extWinBuildTriggerParamForRow(pBlock, pStartCol, pEndCol, numCols, row, &param));

      pExtWinTimeRange->skey = TMIN(pExtWinTimeRange->skey, param.wstart);
      pExtWinTimeRange->ekey = TMAX(pExtWinTimeRange->ekey, param.wend);

      code = extWinAppendNonGroupedCalcParam(pRt, &param, &hasPrevStart, &prevStart, row);
      if (code != TSDB_CODE_SUCCESS) {
        tDestroySSTriggerCalcParam(&param);
        TAOS_CHECK_EXIT(code);
      }
    }
  }

_exit:
  return code;
}

static int32_t extWinBuildGroupedCalcInfosFromBlocks(SArray* pBlocks, SStreamRuntimeFuncInfo* pRt,
                                                     STimeWindow* pExtWinTimeRange) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SColumnInfoData* pStartCol = NULL;
  SColumnInfoData* pEndCol = NULL;
  int32_t numRows = 0;
  int32_t numCols = 0;
  int32_t blockNum = taosArrayGetSize(pBlocks);

  for (int32_t blockIdx = 0; blockIdx < blockNum; ++blockIdx) {
    SSDataBlock** ppOne = taosArrayGet(pBlocks, blockIdx);
    TSDB_CHECK_NULL(ppOne, code, lino, _exit, TSDB_CODE_INVALID_PARA);
    SSDataBlock* pBlock = *ppOne;
    TSDB_CHECK_NULL(pBlock, code, lino, _exit, TSDB_CODE_INVALID_PARA);

    TAOS_CHECK_EXIT(extWinValidateNonStreamBlock(pBlock, &pStartCol, &pEndCol, &numRows, &numCols));

    uint64_t groupId = extWinGetRemoteResultGroupId(pBlock);
    for (int32_t row = 0; row < numRows; ++row) {
      SSTriggerCalcParam param = {0};
      TAOS_CHECK_EXIT(extWinBuildTriggerParamForRow(pBlock, pStartCol, pEndCol, numCols, row, &param));

      pExtWinTimeRange->skey = TMIN(pExtWinTimeRange->skey, param.wstart);
      pExtWinTimeRange->ekey = TMAX(pExtWinTimeRange->ekey, param.wend);

      code = extWinAppendGroupedCalcParam(pRt, groupId, &param, row);
      if (code != TSDB_CODE_SUCCESS) {
        tDestroySSTriggerCalcParam(&param);
        TAOS_CHECK_EXIT(code);
      }
    }
  }

_exit:
  return code;
}

static int32_t extWinInitNonStreamWindowDataFromBlock(SExternalWindowPhysiNode* pPhynode, SExecTaskInfo* pTaskInfo, STimeWindow* pTimeRange) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SArray*     pBlocks = NULL;
  STimeWindow extWinTimeRange = {.skey = INT64_MAX, .ekey = INT64_MIN};

  if (NULL == pPhynode->pSubquery || nodeType(pPhynode->pSubquery) != QUERY_NODE_REMOTE_TABLE) {
    qDebug("invalid subquery in external window, pSubquery:%p, type:%d", pPhynode->pSubquery,
           pPhynode->pSubquery ? nodeType(pPhynode->pSubquery) : -1);
    return TSDB_CODE_SUCCESS;
  }
  TSDB_CHECK_NULL(pTaskInfo, code, lino, _exit, TSDB_CODE_INVALID_PARA);

  if (pTaskInfo->pStreamRuntimeInfo == NULL) {
    pTaskInfo->pStreamRuntimeInfo = (SStreamRuntimeInfo*)taosMemoryCalloc(1, sizeof(SStreamRuntimeInfo));
    TSDB_CHECK_NULL(pTaskInfo->pStreamRuntimeInfo, code, lino, _exit, terrno);
    pTaskInfo->ownStreamRtInfo = true;
  }

  // Initialize basic runtime function parameters.
  SStreamRuntimeFuncInfo* pRt = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  pRt->withExternalWindow = true;
  pRt->isWindowTrigger = true;
  pRt->triggerType = STREAM_TRIGGER_SESSION;
  pRt->precision = 0;
  pRt->curIdx = 0;

#if defined(BUILD_TEST)
  // Test-only path:
  // 1) route by subquery source db name and build mock external-window blocks;
  // 2) non-test db names are treated as unsupported since production interface is not implemented yet.
  pBlocks = extWinGetSSDataBlocksInTest(pPhynode);
  TSDB_CHECK_NULL(pBlocks, code, lino, _exit, terrno);
#else
  // get the block with external window values from subquery, for now just return error since this code
  // path is only for non-stream query which is not supported yet.
  
  SRemoteTableNode* pRemote = (SRemoteTableNode*)pPhynode->pSubquery;
  TAOS_CHECK_EXIT(qFetchRemoteNode(gTaskScalarExtra.pSubJobCtx, pRemote->subQIdx, (SNode*)pRemote));

  pBlocks = pRemote->pResBlks;
#endif
  // For non-stream external_window, whether trigger windows should be built as
  // grouped calc infos must follow the OUTER query partition semantics instead of
  // raw remote block ids. Remote subquery result blocks may carry non-zero ids
  // even when the subquery is semantically non-grouped, which would incorrectly
  // split one logical external window into multiple TGrps.
  bool groupedRemoteResult = pPhynode->calcWithPartition && hasGroupedRemoteResult(pBlocks);
  pRt->isMultiGroupCalc = groupedRemoteResult ? 1 : 0;
  pRt->curGrpCalc = NULL;
  pRt->groupId = 0;

  // Initialize/reset pseudo function values.
  if (pRt->pStreamPesudoFuncVals == NULL) {
    pRt->pStreamPesudoFuncVals = taosArrayInit(4, sizeof(SSTriggerCalcParam));
    TSDB_CHECK_NULL(pRt->pStreamPesudoFuncVals, code, lino, _exit, terrno);
  } else {
    taosArrayClearEx(pRt->pStreamPesudoFuncVals, tDestroySSTriggerCalcParam);
  }

  if (pRt->pGroupCalcInfos != NULL) {
    tSimpleHashCleanup(pRt->pGroupCalcInfos);
    pRt->pGroupCalcInfos = NULL;
  }

  if (groupedRemoteResult) {
    pRt->pGroupCalcInfos = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
    TSDB_CHECK_NULL(pRt->pGroupCalcInfos, code, lino, _exit, terrno);
    tSimpleHashSetFreeFp(pRt->pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  }
  
  if (groupedRemoteResult) {
    TAOS_CHECK_EXIT(extWinBuildGroupedCalcInfosFromBlocks(pBlocks, pRt, &extWinTimeRange));
    // The grouped path uses per-group pParams (in pGroupCalcInfos) instead of
    // pStreamPesudoFuncVals.  extWinInitCGrpCtx later overwrites the pointer,
    // orphaning the original SArray.  Free it now while we still can.
    taosArrayDestroy(pRt->pStreamPesudoFuncVals);
    pRt->pStreamPesudoFuncVals = NULL;
  } else {
    TAOS_CHECK_EXIT(extWinBuildNonGroupedCalcInfosFromBlocks(pBlocks, pRt, &extWinTimeRange));
  }

  if (pTimeRange != NULL) {
    *pTimeRange = extWinTimeRange;
  }

  if (!groupedRemoteResult && taosArrayGetSize(pRt->pStreamPesudoFuncVals) > 0) {
    qInfo("%s non-stream extWin mock initialized from block, winNum:%d, firstWin:[%" PRId64 ", %" PRId64 "], wholeRange:[%" PRId64 ", %" PRId64 "]",
          GET_TASKID(pTaskInfo), (int32_t)taosArrayGetSize(pRt->pStreamPesudoFuncVals),
          ((SSTriggerCalcParam*)taosArrayGet(pRt->pStreamPesudoFuncVals, 0))->wstart,
          ((SSTriggerCalcParam*)taosArrayGet(pRt->pStreamPesudoFuncVals, 0))->wend,
          extWinTimeRange.skey, extWinTimeRange.ekey);
  } else if (groupedRemoteResult) {
    qInfo("%s non-stream extWin initialized from grouped remote result, groupNum:%d, wholeRange:[%" PRId64 ", %" PRId64 "]",
          GET_TASKID(pTaskInfo), pRt->pGroupCalcInfos ? tSimpleHashGetSize(pRt->pGroupCalcInfos) : 0,
          extWinTimeRange.skey, extWinTimeRange.ekey);
  }

_exit:
  if (pBlocks) {
    for (int32_t blockIdx = 0; blockIdx < taosArrayGetSize(pBlocks); ++blockIdx) {
      SSDataBlock** ppOne = taosArrayGet(pBlocks, blockIdx);
      if (ppOne && *ppOne) {
        blockDataDestroy(*ppOne);
      }
    }
    taosArrayDestroy(pBlocks);
  }
  return code;
}

#if defined(BUILD_TEST)
// mockSSDataBlock is a helper function to create a sample SSDataBlock for testing purposes.
static SSDataBlock* mockSSDataBlock() {
  SSDataBlock* pBlock = NULL;
  int32_t      code = createDataBlock(&pBlock);
  if (code != TSDB_CODE_SUCCESS || pBlock == NULL) {
    return NULL;
  }

  // Add 4 columns: timestamp, timestamp, int, bigint.
  SColumnInfoData col1 = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, 8, 1);
  code = blockDataAppendColInfo(pBlock, &col1);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return NULL;
  }

  SColumnInfoData col2 = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, 8, 2);
  code = blockDataAppendColInfo(pBlock, &col2);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return NULL;
  }

  SColumnInfoData col3 = createColumnInfoData(TSDB_DATA_TYPE_INT, 4, 3);
  code = blockDataAppendColInfo(pBlock, &col3);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return NULL;
  }

  SColumnInfoData col4 = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, 8, 4);
  code = blockDataAppendColInfo(pBlock, &col4);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return NULL;
  }

  // Ensure capacity for all rows.
  code = blockDataEnsureCapacity(pBlock, 2);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return NULL;
  }

  // Get column data pointers.
  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, 1);
  SColumnInfoData* pCol3 = taosArrayGet(pBlock->pDataBlock, 2);
  SColumnInfoData* pCol4 = taosArrayGet(pBlock->pDataBlock, 3);
  if (pCol1 == NULL || pCol2 == NULL || pCol3 == NULL || pCol4 == NULL) {
    blockDataDestroy(pBlock);
    return NULL;
  }

  // Row 1 values.
  int64_t ts1 = 1589335200000;
  int64_t ts2 = 1589338140000;
  int32_t intVal1 = 100;
  int64_t bigintVal1 = 1000000LL;

  code = colDataSetVal(pCol1, 0, (const char*)&ts1, false);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return NULL;
  }
  code = colDataSetVal(pCol2, 0, (const char*)&ts2, false);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return NULL;
  }
  code = colDataSetVal(pCol3, 0, (const char*)&intVal1, false);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return NULL;
  }
  code = colDataSetVal(pCol4, 0, (const char*)&bigintVal1, false);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return NULL;
  }
  pBlock->info.rows++;

  // Row 2 values.
  int64_t ts3 = 1589338140001;
  int64_t ts4 = 1589340110000;
  int32_t intVal2 = 200;
  int64_t bigintVal2 = 2000000LL;

  code = colDataSetVal(pCol1, 1, (const char*)&ts3, false);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return NULL;
  }
  code = colDataSetVal(pCol2, 1, (const char*)&ts4, false);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return NULL;
  }
  code = colDataSetVal(pCol3, 1, (const char*)&intVal2, false);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return NULL;
  }
  code = colDataSetVal(pCol4, 1, (const char*)&bigintVal2, false);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return NULL;
  }
  pBlock->info.rows++;

  pBlock->info.id.groupId = 0;
  pBlock->info.id.baseGId = pBlock->info.id.groupId;

  return pBlock;
}

static int32_t extWinMockSSDataBlocksWithGroups(SArray** ppBlocks) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SArray*      pBlocks = NULL;
  SSDataBlock* pBlock1 = NULL;
  SSDataBlock* pBlock2 = NULL;

  TSDB_CHECK_NULL(ppBlocks, code, lino, _exit, TSDB_CODE_INVALID_PARA);

  pBlocks = taosArrayInit(2, POINTER_BYTES);
  TSDB_CHECK_NULL(pBlocks, code, lino, _exit, terrno);

  pBlock1 = mockSSDataBlock();
  TSDB_CHECK_NULL(pBlock1, code, lino, _exit, terrno);
  pBlock1->info.id.groupId = 1001;
  pBlock1->info.id.baseGId = pBlock1->info.id.groupId;

  pBlock2 = mockSSDataBlock();
  TSDB_CHECK_NULL(pBlock2, code, lino, _exit, terrno);
  pBlock2->info.id.groupId = 1002;
  pBlock2->info.id.baseGId = pBlock2->info.id.groupId;

  SColumnInfoData* pG2Start = taosArrayGet(pBlock2->pDataBlock, 0);
  SColumnInfoData* pG2End = taosArrayGet(pBlock2->pDataBlock, 1);
  SColumnInfoData* pG2Int = taosArrayGet(pBlock2->pDataBlock, 2);
  SColumnInfoData* pG2BigInt = taosArrayGet(pBlock2->pDataBlock, 3);
  TSDB_CHECK_NULL(pG2Start, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pG2End, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pG2Int, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pG2BigInt, code, lino, _exit, TSDB_CODE_INVALID_PARA);

  for (int32_t row = 0; row < pBlock2->info.rows; ++row) {
    int64_t startTs = *(int64_t*)colDataGetData(pG2Start, row);
    int64_t endTs = *(int64_t*)colDataGetData(pG2End, row);
    int32_t intVal = *(int32_t*)colDataGetData(pG2Int, row);
    int64_t bigIntVal = *(int64_t*)colDataGetData(pG2BigInt, row);

    startTs += 3600000;
    endTs += 3600000;
    intVal += 100;
    bigIntVal += 1000000;

    TAOS_CHECK_EXIT(colDataSetVal(pG2Start, row, (const char*)&startTs, false));
    TAOS_CHECK_EXIT(colDataSetVal(pG2End, row, (const char*)&endTs, false));
    TAOS_CHECK_EXIT(colDataSetVal(pG2Int, row, (const char*)&intVal, false));
    TAOS_CHECK_EXIT(colDataSetVal(pG2BigInt, row, (const char*)&bigIntVal, false));
  }

  TSDB_CHECK_NULL(taosArrayPush(pBlocks, &pBlock1), code, lino, _exit, terrno);
  TSDB_CHECK_NULL(taosArrayPush(pBlocks, &pBlock2), code, lino, _exit, terrno);

  *ppBlocks = pBlocks;
  return code;

_exit:
  if (pBlock1) {
    blockDataDestroy(pBlock1);
  }
  if (pBlock2) {
    blockDataDestroy(pBlock2);
  }
  if (pBlocks) {
    taosArrayDestroy(pBlocks);
  }
  return code;
}

typedef enum {
  EXT_WIN_TEST_MOCK_UNSUPPORTED = 0,
  EXT_WIN_TEST_MOCK_SINGLE_BLOCK,
  EXT_WIN_TEST_MOCK_GROUP_BLOCKS,
} EExtWinTestMockMode;

typedef struct {
  EExtWinTestMockMode mode;
} SExtWinTestMockCtx;

static bool extWinDetectTestMockModeFromPhysiNode(SPhysiNode* pNode, SExtWinTestMockCtx* pCtx) {
  if (pNode == NULL || pCtx == NULL) {
    return false;
  }

  if (nodeType(pNode) == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanPhysiNode* pScan = (STableScanPhysiNode*)pNode;
    const char*          pDbName = tNameGetDbNameP(&pScan->scan.tableName);
    if (pDbName == NULL) {
      pCtx->mode = EXT_WIN_TEST_MOCK_UNSUPPORTED;
      return true;
    }

    if (0 == strcasecmp(pDbName, "external_window_test_single_block")) {
      pCtx->mode = EXT_WIN_TEST_MOCK_SINGLE_BLOCK;
    } else if (0 == strcasecmp(pDbName, "external_window_test_group_blocks")) {
      pCtx->mode = EXT_WIN_TEST_MOCK_GROUP_BLOCKS;
    } else {
      pCtx->mode = EXT_WIN_TEST_MOCK_UNSUPPORTED;
    }

    return true;
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (extWinDetectTestMockModeFromPhysiNode((SPhysiNode*)pChild, pCtx)) {
      return true;
    }
  }

  return false;
}

static SArray* extWinGetSSDataBlocksInTest(SExternalWindowPhysiNode* pPhynode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pPhynode == NULL || pPhynode->pSubquery == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  SExtWinTestMockCtx ctx = {.mode = EXT_WIN_TEST_MOCK_UNSUPPORTED};
  SNode*             pChild = NULL;
  FOREACH(pChild, pPhynode->window.node.pChildren) {
    if (extWinDetectTestMockModeFromPhysiNode((SPhysiNode*)pChild, &ctx)) {
      break;
    }
  }

  if (ctx.mode == EXT_WIN_TEST_MOCK_SINGLE_BLOCK) {
    SArray*      pBlocks = taosArrayInit(1, POINTER_BYTES);
    SSDataBlock* pBlock = NULL;
    if (pBlocks == NULL) {
      return NULL;
    }

    pBlock = mockSSDataBlock();
    if (pBlock == NULL) {
      taosArrayDestroy(pBlocks);
      return NULL;
    }

    if (taosArrayPush(pBlocks, &pBlock) == NULL) {
      blockDataDestroy(pBlock);
      taosArrayDestroy(pBlocks);
      return NULL;
    }

    return pBlocks;
  }

  if (ctx.mode == EXT_WIN_TEST_MOCK_GROUP_BLOCKS) {
    SArray* pBlocks = NULL;
    int32_t code = extWinMockSSDataBlocksWithGroups(&pBlocks);
    TAOS_CHECK_EXIT(code);
    return pBlocks;
  }

    if (NULL == pPhynode->pSubquery || nodeType(pPhynode->pSubquery) != QUERY_NODE_REMOTE_TABLE) {
    qError("invalid subquery in external window, pSubquery:%p, type:%d", pPhynode->pSubquery, pPhynode->pSubquery ? nodeType(pPhynode->pSubquery) : -1);
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    TAOS_CHECK_EXIT(code);
  }
  
  SRemoteTableNode* pRemote = (SRemoteTableNode*)pPhynode->pSubquery;
  TAOS_CHECK_EXIT(qFetchRemoteNode(gTaskScalarExtra.pSubJobCtx, pRemote->subQIdx, (SNode*)pRemote));

  return pRemote->pResBlks;
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    qError("%s : %d error code:%s", __func__, lino, tstrerror(code));
  }

  return NULL;
}
#endif
