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
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"
#include "stream.h"
#include "filter.h"
#include "cmdnodes.h"
#include "../../function/inc/functionResInfoInt.h"

#define EXT_WIN_RES_ROWS_ALLOC_SIZE 10
#define EXT_WIN_CALC_GROUP_SIZE 1000
#define EXT_WIN_TYPE_STR(_isMAExtW) ((_isMAExtW) ? "mergeAligned" : "")

typedef struct SBlockList {
  const SSDataBlock* pSrcBlock;
  SList*             pBlocks;
  int32_t            blockRowNumThreshold;
} SBlockList;


typedef int32_t (*extWinGetWinFp)(SOperatorInfo*, int64_t*, int32_t*, SDataBlockInfo*, SExtWinTimeWindow**, int32_t*);

typedef struct SExtWindowStat {
  int64_t resBlockCreated;
  int64_t resBlockDestroyed;
  int64_t resBlockRecycled;
  int64_t resBlockReused;
  int64_t resBlockAppend;
} SExtWindowStat;


typedef struct SExtWinCalcGrpCtx {
  uint64_t           groupId;
  SArray*            pWins;           // SArray<SExtWinTimeWindow>
  int32_t            curIdx; // for pesudo func calculation

  bool               blkWinStartSet;
  int32_t            blkWinStartIdx;
  int32_t            blkWinIdx;
  int32_t            blkRowStartIdx;

  SArray*            outWinBufIdx;
  int32_t            outWinTotalNum;      // agg: total output win num
  int32_t            outWinNum;           // already output win num
  int32_t            outWinIdx;           // current output win idx
  int32_t            outWinLastIdx;
  
  int32_t            lastWinIdx;
  int64_t            lastSKey;
  int64_t            lastEKey;
  int32_t            lastWinId;  
} SExtWinCalcGrpCtx;

typedef struct SExtWinTrigGrpCtx {
  SExtWinCalcGrpCtx* pCCtx;
  int32_t            lastCtxIter;

  SSHashObj*         pCGCtxs; // calc groups ctxs, groupId => SExtWinCalcGrpCtx
} SExtWinTrigGrpCtx;


typedef struct SExtWinResultRows {
  int32_t             resRowsSize;
  int32_t             resRowsIdx;
  int32_t             resRowSize;
  int32_t             resRowIdx;
  int64_t             resRowAllcNum;
  SResultRow**        pResultRows;
} SExtWinResultRows;

typedef struct SExternalWindowOperator {
  SOptrBasicInfo     binfo;
  SExprSupp          scalarSupp;
  int32_t            primaryTsIndex;
  EExtWinMode        mode;
  EFillMode          fillMode;
  bool               multiTableMode;
  bool               inputHasOrder;
  bool               needGroupSort;
  bool               extWinSplit;
  bool               calcWithPartition;
  SArray*            pPseudoColInfo;  
  STimeRangeNode*    timeRangeExpr;
  
  extWinGetWinFp     getWinFp;

  uint64_t           lastTGrpId;
  uint64_t           lastCGrpId;
  SExtWinTrigGrpCtx* pTGrpCtx;
  bool               ownTGrpCtx;
  SArray*            pGrpIds;       // single calc or trig group ids
  SArray*            pCTGrpIds;
  
  // for project&indefRows
  SList*             pFreeBlocks;    // SList<SSDatablock*+SAarray*>
  SArray*            pOutputBlocks;  // SArray<SList*>, for each window, we have a list of blocks
  SListNode*         pLastBlkNode; 
  SSDataBlock*       pTmpBlock;
  
  // for agg
  SAggSupporter      aggSup;
  STimeWindowAggSupp twAggSup;

  SExtWinResultRows  resultRows;

  // for output
  SSTriggerGroupCalcInfo* pLastOutput;
  int32_t                 lastOutputIter;
  int32_t                 lastGrpIdx;    // index in pGrpIds or pCTGrpIds

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
  int32_t            orgTableVgId;
  tb_uid_t           orgTableUid;
  STimeWindow        orgTableTimeRange;

  SExecTaskInfo*     pTaskInfo;  // for pRunnerGrpCtx cleanup in destroy
} SExternalWindowOperator;

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

static void extWinDestroyCGrpCtx(void* param) {
  if (NULL == param) {
    return;
  }

  SExtWinCalcGrpCtx* pCtx = (SExtWinCalcGrpCtx*)param;

  taosArrayDestroy(pCtx->pWins);
}

static void extWinDestroyTGrpCtx(void* param) {
  if (NULL == param) {
    return;
  }

  SExtWinTrigGrpCtx* pCtx = (SExtWinTrigGrpCtx*)param;

  if (pCtx->pCGCtxs) {
    tSimpleHashSetFreeFp(pCtx->pCGCtxs, extWinDestroyCGrpCtx);
    tSimpleHashCleanup(pCtx->pCGCtxs);
  } else if (pCtx->pCCtx) {
    extWinDestroyCGrpCtx(pCtx->pCCtx);
    taosMemoryFree(pCtx->pCCtx);
  }
}

static int32_t extWinBlockListAddBlock(SExternalWindowOperator* pExtW, SList* pList, int32_t rows, SSDataBlock** ppBlock, SArray** ppIdx) {
  SSDataBlock* pRes = NULL;
  int32_t code = 0, lino = 0;

  if (listNEles(pExtW->pFreeBlocks) > 0) {
    SListNode* pNode = tdListPopHead(pExtW->pFreeBlocks);
    *ppBlock = *(SSDataBlock**)pNode->data;
    *ppIdx = *(SArray**)((SArray**)pNode->data + 1);
    tdListAppendNode(pList, pNode);
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

  (*ppBlock)->info.id.groupId = pExtW->lastCGrpId;
  // ensure projection path blocks also carry the matching T-group id
  (*ppBlock)->info.id.baseGId = pExtW->lastTGrpId;

  
_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(pRes);
  }
  
  return code;
}

static void extWinPostUpdateStreamRt(SStreamRuntimeFuncInfo* pStream, SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  if (pStream->curGrpCalc) {
    pStream->createTable = &pStream->curGrpCalc->createTable;
    pStream->pStreamPesudoFuncVals = pStream->curGrpCalc->pParams;
    pStream->pStreamPartColVals = pStream->curGrpCalc->pGroupColVals;
  }
  
  pStream->groupId = pExtW->lastTGrpId;

  qDebug("%s streamRt updated, groupId %" PRIu64, GET_TASKID(pOperator->pTaskInfo), pStream->groupId);
}

static void extWinAssignBlockGrpId(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, SBlockID* pId) {
  uint64_t currentCGrpId = (pExtW->pTGrpCtx != NULL && pExtW->pTGrpCtx->pCCtx != NULL)
                               ? pExtW->pTGrpCtx->pCCtx->groupId
                               : pExtW->lastCGrpId;

  if (pExtW->calcWithPartition && !pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.isMultiGroupCalc) {
    // For partitioned non-multi-group output, the effective merge group is the
    // current calc-group id. If we keep using lastTGrpId here, all output
    // blocks collapse to group 0 and the downstream merge path may concatenate
    // different partition groups without re-ordering by _group_id.
    pId->groupId = currentCGrpId;
    pId->baseGId = 0;
  } else if (pExtW->extWinSplit) {
    pId->groupId = currentCGrpId;
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
    TAOS_CHECK_EXIT(extWinBlockListAddBlock(pExtW, pList, rows, ppBlock, ppIdx));
    return code;
  }

  pRes = *(SSDataBlock**)pNode->data;
  if ((pRes->info.rows + rows) > pRes->info.capacity) {
    TAOS_CHECK_EXIT(extWinBlockListAddBlock(pExtW, pList, rows, ppBlock, ppIdx));
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

  if (pInfo->ownTGrpCtx && pInfo->pTGrpCtx) {
    extWinDestroyTGrpCtx(pInfo->pTGrpCtx);
    taosMemoryFree(pInfo->pTGrpCtx);
    pInfo->pTGrpCtx = NULL;
  }

  // Free executor-specific pRunnerGrpCtx stored inside grouped calc info entries.
  // These contain SExtWinTrigGrpCtx which must be cleaned up here (before
  // doDestroyTask calls tDestroyStRtFuncInfo) since extWinDestroyTGrpCtx is
  // only accessible from this translation unit.
  if (pInfo->pTaskInfo != NULL && pInfo->pTaskInfo->pStreamRuntimeInfo != NULL) {
    SStreamRuntimeFuncInfo* pRt = &pInfo->pTaskInfo->pStreamRuntimeInfo->funcInfo;
    if (pRt->pGroupCalcInfos != NULL) {
      int32_t iter = 0;
      void*   pIter = NULL;
      while ((pIter = tSimpleHashIterate(pRt->pGroupCalcInfos, pIter, &iter)) != NULL) {
        SSTriggerGroupCalcInfo* pGrpCalc = (SSTriggerGroupCalcInfo*)pIter;
        if (pGrpCalc->pRunnerGrpCtx != NULL) {
          extWinDestroyTGrpCtx(pGrpCalc->pRunnerGrpCtx);
          taosMemoryFree(pGrpCalc->pRunnerGrpCtx);
          pGrpCalc->pRunnerGrpCtx = NULL;
        }
      }
    }
  }

  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSupp);
  for (int32_t i = 0; i < pInfo->resultRows.resRowsSize; ++i) {
    if (pInfo->resultRows.pResultRows && pInfo->resultRows.pResultRows[i]) {
      taosMemoryFreeClear(pInfo->resultRows.pResultRows[i]);
    }
  }
  taosMemoryFreeClear(pInfo->resultRows.pResultRows);
  
  pInfo->binfo.resultRowInfo.openWindow = tdListFree(pInfo->binfo.resultRowInfo.openWindow);

  qDebug("ext window stat at destroy, created:%" PRId64 ", destroyed:%" PRId64 ", recycled:%" PRId64 ", reused:%" PRId64 ", append:%" PRId64, 
      pInfo->stat.resBlockCreated, pInfo->stat.resBlockDestroyed, pInfo->stat.resBlockRecycled, 
      pInfo->stat.resBlockReused, pInfo->stat.resBlockAppend);

  taosMemoryFreeClear(pInfo);
}

static int32_t extWinOpen(SOperatorInfo* pOperator);
static int32_t extWinNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);
static int32_t mergeAlignExtWinNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);

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
  int64_t curTs;
  SResultRow*  pResultRow;
  SSDataBlock* pNewGroup;
  int32_t lastFinalizedWinIdx;  // tracks last emitted window index for filling empty-window gaps (vtable COLS merge)
} SMergeAlignedExternalWindowOperator;

static int32_t extWinInitNonStreamWindowDataFromBlock(SExternalWindowPhysiNode* pPhynode, SExecTaskInfo* pTaskInfo,
                                                       STimeWindow* pTimeRange);

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


static int32_t extWinAppendWinIdx(SExecTaskInfo*       pTaskInfo, SArray* pIdx, SSDataBlock* pBlock, int32_t currWinIdx, int32_t rows) {
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


static int32_t extWinInitCGrpCtx(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo, SExtWinCalcGrpCtx* pCtx) {
  int32_t code = 0, lino = 0;

  pCtx->groupId = 0;
  pCtx->curIdx = 0;
  pCtx->lastSKey = INT64_MIN;
  pCtx->lastEKey = INT64_MAX;
  pCtx->lastWinId = -1;
  pCtx->lastWinIdx = -1;
  pCtx->blkWinIdx = -1;
  pCtx->blkWinStartSet = false;
  pCtx->blkWinStartIdx = 0;
  pCtx->blkRowStartIdx = 0;
  pCtx->outWinIdx = 0;
  pCtx->outWinLastIdx = -1;
  pCtx->outWinTotalNum = 0;
  pCtx->outWinNum = 0;
  pCtx->pWins = NULL;
  pCtx->outWinBufIdx = NULL;
  
  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;

  if (pInfo->isMultiGroupCalc) {
    pInfo->pStreamPesudoFuncVals = pInfo->curGrpCalc->pParams;
    pInfo->pStreamPartColVals = pInfo->curGrpCalc->pGroupColVals;
  }

  size_t size = taosArrayGetSize(pInfo->pStreamPesudoFuncVals);
  pCtx->pWins = taosArrayInit_s(sizeof(SExtWinTimeWindow), size);
  TSDB_CHECK_NULL(pCtx->pWins, code, lino, _exit, terrno);
  //pCtx->outWinBufIdx = taosArrayInit_s(sizeof(int32_t), size);
  //TSDB_CHECK_NULL(pCtx->outWinBufIdx, code, lino, _exit, terrno);

  SExtWinTimeWindow* pWin = taosArrayGet(pCtx->pWins, 0);

  if (pExtW->isMergeAlignedExtW) {
    for (int32_t i = 0; i < size; ++i) {
      SSTriggerCalcParam* pParam = taosArrayGet(pInfo->pStreamPesudoFuncVals, i);

      pWin[i].tw.skey = pParam->wstart;
      pWin[i].tw.ekey = pParam->wstart + 1;
      pWin[i].resWinIdx = -1;
    }
  } else if (pExtW->timeRangeExpr && pExtW->timeRangeExpr->needCalc) {
    TAOS_CHECK_EXIT(scalarCalculateExtWinsTimeRange(pExtW->timeRangeExpr, pInfo, pWin));
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
      SSTriggerCalcParam* pParam = taosArrayGet(pInfo->pStreamPesudoFuncVals, i);

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
  pCtx->blkWinStartSet = false;
  pCtx->blkWinStartIdx = 0;
  pCtx->blkRowStartIdx = 0;
  pCtx->outWinIdx = 0;
  pCtx->outWinLastIdx = -1;
  pCtx->outWinTotalNum = 0;
  pCtx->outWinNum = 0;
  pCtx->pWins = NULL;
  pCtx->outWinBufIdx = NULL;
}

static int32_t extWinSwitchInitTGrpCtx(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo, SBlockID* pId) {
  int32_t code = 0, lino = 0;
  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;

  if ((!pInfo->isMultiGroupCalc && NULL != pExtW->pTGrpCtx) ||
      (pInfo->isMultiGroupCalc && NULL != pExtW->pTGrpCtx && pId->baseGId == pExtW->lastTGrpId)) {
    goto _exit;
  }
  
  if (pInfo->isMultiGroupCalc) {
    pInfo->curGrpCalc = tSimpleHashGet(pInfo->pGroupCalcInfos, &pId->baseGId, sizeof(pId->baseGId));
    if (NULL == pInfo->curGrpCalc) {
      qError("%s %s failed to get %s extWin tgrp %" PRIu64 " calc info", 
        GET_TASKID(pTaskInfo), __func__, EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW), pId->baseGId);
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }

    if (NULL == pInfo->curGrpCalc->pRunnerGrpCtx) {
      pInfo->curGrpCalc->pRunnerGrpCtx = taosMemoryCalloc(1, sizeof(SExtWinTrigGrpCtx));
      TSDB_CHECK_NULL(pInfo->curGrpCalc->pRunnerGrpCtx, code, lino, _exit, terrno);
      
      if (!pExtW->calcWithPartition && pExtW->needGroupSort) {
        if (NULL == pExtW->pGrpIds) {
          pExtW->pGrpIds = taosArrayInit(1024, sizeof(uint64_t));
          TSDB_CHECK_NULL(pExtW->pGrpIds, code, lino, _exit, terrno);
        }
        
        TSDB_CHECK_NULL(taosArrayPush(pExtW->pGrpIds, &pId->baseGId), code, lino, _exit, terrno);
      }
    } else {
      pInfo->pStreamPesudoFuncVals = pInfo->curGrpCalc->pParams;
      pInfo->pStreamPartColVals = pInfo->curGrpCalc->pGroupColVals;
    }
    
    pInfo->groupId = pId->baseGId;
    pExtW->lastTGrpId = pId->baseGId;
    pExtW->pTGrpCtx = pInfo->curGrpCalc->pRunnerGrpCtx;
    pExtW->ownTGrpCtx = false;

    qDebug("%s %s extWin switch to tgrp %" PRIu64, 
      GET_TASKID(pTaskInfo), EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW), pId->baseGId);

    goto _exit;
  }

  pExtW->pTGrpCtx = taosMemoryCalloc(1, sizeof(SExtWinTrigGrpCtx));
  TSDB_CHECK_NULL(pExtW->pTGrpCtx, code, lino, _exit, terrno);
  pExtW->ownTGrpCtx = true;

_exit:

  if (code) {
    qError("%s %s %s extWin failed at line %d since %s", 
      GET_TASKID(pTaskInfo), __func__, EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW), lino, tstrerror(code));
  }
  
  return code;
}

static FORCE_INLINE bool extWinNeedResolvePartitionBlockId(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo,
                                                           const SBlockID* pId) {
  if (pId == NULL || pTaskInfo == NULL || pTaskInfo->pStreamRuntimeInfo == NULL) {
    return false;
  }

  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  return pInfo->isMultiGroupCalc && pExtW->calcWithPartition && pInfo->pGroupCalcInfos != NULL;
}

static void extWinResolveBaseGroupIdForPartition(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo,
                                                 SBlockID* pId) {
  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;

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


static int32_t extWinSwitchInitCGrpCtx(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo, SBlockID* pId) {
  int32_t code = 0, lino = 0;
  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  SExtWinTrigGrpCtx* pTCtx = pExtW->pTGrpCtx;

  if (pTCtx == NULL) {
    qError("%s %s invalid tgrp ctx for %s extWin, baseGrp:%" PRIu64 " grp:%" PRIu64,
           GET_TASKID(pTaskInfo), __func__, EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW),
           pId->baseGId, pId->groupId);
    TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
  }

  // In partitioned multi-group mode, only blocks whose C-group (groupId)
  // matches the current T-group (baseGId) should proceed. Treat mismatches
  // like "no cgrp" here; extWinOpen has an early filter to skip such blocks.
  if (0 == pId->groupId || (pInfo->isMultiGroupCalc && (pId->baseGId != pId->groupId))) {
    if (NULL != pTCtx->pCGCtxs) {
      qError("%s plan or ctx conflict, pCGCtxs:%p baseGrp:%" PRIu64 " grp:%" PRIu64
             " lastCGrp:%" PRIu64 " isMulti:%d calcWithPart:%d",
             GET_TASKID(pTaskInfo), pTCtx->pCGCtxs, pId->baseGId, pId->groupId,
             pExtW->lastCGrpId, pInfo->isMultiGroupCalc, pExtW->calcWithPartition);
      TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
    }

    if (NULL == pTCtx->pCCtx) {
      pTCtx->pCCtx = taosMemoryCalloc(1, sizeof(*pTCtx->pCCtx));
      TSDB_CHECK_NULL(pTCtx->pCCtx, code, lino, _exit, terrno);
      TAOS_CHECK_EXIT(extWinInitCGrpCtx(pExtW, pTaskInfo, pTCtx->pCCtx));
    }

    qDebug("%s ext win switch to no cgrp", EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW));
    
    goto _exit;
  }

  if (pId->groupId == pExtW->lastCGrpId) {
    if (pTCtx->pCCtx != NULL) {
      pTCtx->pCCtx->groupId = pId->groupId;
    }
    qDebug("%s ext win continue cgrp %" PRIu64, EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW), pId->groupId);
    goto _exit;
  }

  pExtW->lastCGrpId = pId->groupId;
  
  if (NULL == pTCtx->pCGCtxs) {
    if (NULL != pTCtx->pCCtx) {
      TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
    }
    
    pTCtx->pCGCtxs = tSimpleHashInit(EXT_WIN_CALC_GROUP_SIZE, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
    TSDB_CHECK_NULL(pTCtx->pCGCtxs, code, lino, _exit, terrno);
  }

  pTCtx->pCCtx = tSimpleHashGet(pTCtx->pCGCtxs, &pId->groupId, sizeof(pId->groupId));
  if (NULL == pTCtx->pCCtx) {
    SExtWinCalcGrpCtx tmp = {0};
    TAOS_CHECK_EXIT(tSimpleHashPut(pTCtx->pCGCtxs, &pId->groupId, sizeof(pId->groupId), &tmp, sizeof(tmp)));
    pTCtx->pCCtx = tSimpleHashGet(pTCtx->pCGCtxs, &pId->groupId, sizeof(pId->groupId));
    TAOS_CHECK_EXIT(extWinInitCGrpCtx(pExtW, pTaskInfo, pTCtx->pCCtx));

    if (pExtW->needGroupSort) {
      if (pInfo->isMultiGroupCalc) {
        if (NULL == pExtW->pCTGrpIds) {
          pExtW->pCTGrpIds = taosArrayInit(1024, sizeof(uint64_t) * 2);
          TSDB_CHECK_NULL(pExtW->pCTGrpIds, code, lino, _exit, terrno);
        }
        
        TSDB_CHECK_NULL(taosArrayPush(pExtW->pCTGrpIds, &pId->groupId), code, lino, _exit, terrno);
      } else {
        if (NULL == pExtW->pGrpIds) {
          pExtW->pGrpIds = taosArrayInit(1024, sizeof(uint64_t));
          TSDB_CHECK_NULL(pExtW->pGrpIds, code, lino, _exit, terrno);
        }
        
        TSDB_CHECK_NULL(taosArrayPush(pExtW->pGrpIds, &pId->groupId), code, lino, _exit, terrno);
      }
    }
  }
  if (pTCtx->pCCtx != NULL) {
    pTCtx->pCCtx->groupId = pId->groupId;
  }
  
  qDebug("%s ext win switch to cgrp %" PRIu64, EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW), pId->groupId);

_exit:

  if (code) {
    qError("%s %s %s ext win failed at line %d since %s", 
      GET_TASKID(pTaskInfo), __func__, EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW), lino, tstrerror(code));
  }
  
  return code;
}

static int32_t extWinSwitchInitCtxs(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo, SBlockID* pId) {
  int32_t code = 0, lino = 0;

  if (pTaskInfo->pStreamRuntimeInfo == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  extWinResolveBlockIdForPartition(pExtW, pTaskInfo, pId);
  TAOS_CHECK_EXIT(extWinSwitchInitTGrpCtx(pExtW, pTaskInfo, pId));
  TAOS_CHECK_EXIT(extWinSwitchInitCGrpCtx(pExtW, pTaskInfo, pId));

_exit:

  if (code) {
    qError("%s %s %s ext win failed at line %d since %s", 
      GET_TASKID(pTaskInfo), __func__, EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW), lino, tstrerror(code));
  }

  return code;
}


static void extWinResetResultRows(SExtWinResultRows* pRows) {
  pRows->resRowsIdx = 0;
  pRows->resRowIdx = 0;
}

static int32_t mergeAlignExtWinSetOutputBuf(SOperatorInfo* pOperator, SResultRowInfo* pResultRowInfo, const STimeWindow* pWin, SResultRow** pResult,
                                       SExprSupp* pExprSup, SAggSupporter* pAggSup) {
  if (*pResult == NULL) {
    *pResult = getNewResultRow(pAggSup->pResultBuf, &pAggSup->currentPageId, pAggSup->resultRowSize);
    if (!*pResult) {
      qError("get new resultRow failed, err:%s", tstrerror(terrno));
      return terrno;
    }
    pResultRowInfo->cur = (SResultRowPosition){.pageId = (*pResult)->pageId, .offset = (*pResult)->offset};
  }
  
  (*pResult)->win = *pWin;
  (*pResult)->winIdx = extWinGetCurWinIdx(pOperator);
  
  return setResultRowInitCtx((*pResult), pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
}


static int32_t mergeAlignExtWinGetWinFromTs(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, TSKEY ts, STimeWindow** ppWin) {
  int32_t blkWinIdx = extWinGetCurWinIdx(pOperator);
  
  // TODO handle desc order
  for (int32_t i = blkWinIdx; i < pExtW->pTGrpCtx->pCCtx->pWins->size; ++i) {
    STimeWindow* pWin = taosArrayGet(pExtW->pTGrpCtx->pCCtx->pWins, i);
    if (ts == pWin->skey) {
      extWinSetCurWinIdx(pOperator, i);
      *ppWin = pWin;
      return TSDB_CODE_SUCCESS;
    } else if (ts < pWin->skey) {
      qError("invalid ts %" PRId64 " for tgrp %" PRIu64 " current window idx %d skey %" PRId64, ts, pExtW->lastTGrpId, i, pWin->skey);
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
  }
  
  qError("invalid ts %" PRId64 " to find tgrp %" PRIu64" merge aligned ext window, size:%d", ts, pExtW->lastTGrpId, (int32_t)pExtW->pTGrpCtx->pCCtx->pWins->size);
  
  return TSDB_CODE_STREAM_INTERNAL_ERROR;
}

static int32_t mergeAlignExtWinFinalizeResult(SOperatorInfo* pOperator, SResultRowInfo* pResultRowInfo, SSDataBlock* pResultBlock) {
  int32_t        code = 0, lino = 0;
  SMergeAlignedExternalWindowOperator* pMAExtW = pOperator->info;
  SExternalWindowOperator*             pExtW = pMAExtW->pExtW;
  SExprSupp*     pSup = &pOperator->exprSupp;
  SResultRow*  pResultRow = pMAExtW->pResultRow;
  
  finalizeResultRows(pExtW->aggSup.pResultBuf, &pResultRowInfo->cur, pSup, pResultBlock, pOperator->pTaskInfo);

  if (pResultRow->numOfRows > 0) {
    TAOS_CHECK_EXIT(extWinAppendWinIdx(pOperator->pTaskInfo, pExtW->pWinRowIdx, pResultBlock, pResultRow->winIdx, pResultRow->numOfRows));
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

    extWinSetCurWinIdx(pOperator, i);
    TAOS_CHECK_EXIT(mergeAlignExtWinSetOutputBuf(pOperator, pResultRowInfo, pWin, &pMAExtW->pResultRow, pSup, &pExtW->aggSup));
    TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, pResultRowInfo, pResultBlock));
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

  extWinSetCurWinIdx(pOperator, 0);
  code = mergeAlignExtWinGetWinFromTs(pOperator, pExtW, ts, &pWin);
  if (code) {
    qError("failed to get time window for tgrp %" PRIu64 " ts:%" PRId64 ", prim ts index:%d, error:%s", 
      pExtW->lastTGrpId, ts, pExtW->primaryTsIndex, tstrerror(code));
    TAOS_CHECK_EXIT(code);
  }

  int32_t newWinIdx = extWinGetCurWinIdx(pOperator);

  if (pMAExtW->curTs != INT64_MIN && pMAExtW->curTs != pWin->skey) {
    TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, pResultRowInfo, pResultBlock));
    pMAExtW->lastFinalizedWinIdx = pMAExtW->pResultRow->winIdx;
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
    pMAExtW->lastFinalizedWinIdx = pMAExtW->pResultRow->winIdx;
    resetResultRow(pMAExtW->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));

    TAOS_CHECK_EXIT(mergeAlignExtWinGetWinFromTs(pOperator, pExtW, tsCols[currPos], &pWin));
    newWinIdx = extWinGetCurWinIdx(pOperator);

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
  SExternalWindowOperator* pExtW = pOperator->info;
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
  SExternalWindowOperator* pExtW = pOperator->info;
  SExprSupp*               pExprSup = &pExtW->scalarSupp;
  int32_t                  code = 0, lino = 0;
  
  TAOS_CHECK_EXIT(projectApplyFunctions(pExprSup->pExprInfo, pResultBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL,
                        GET_STM_RTINFO(pOperator->pTaskInfo)));

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
  int32_t                              lino = 0;
  SStreamRuntimeFuncInfo*              pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;

  taosArrayClear(pExtW->pWinRowIdx);
  blockDataCleanup(pRes);

  SSDataBlock* pBlock = NULL;
  while (1) {
    if (pMAExtW->pNewGroup != NULL) {
      pBlock = pMAExtW->pNewGroup;
      pMAExtW->pNewGroup = NULL;
    } else {
      pBlock = getNextBlockFromDownstream(pOperator, 0);
    }

    if (pBlock == NULL) {
      // close last time window
      if (pMAExtW->curTs != INT64_MIN && EEXT_MODE_AGG == pExtW->mode) {
        TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, &pExtW->binfo.resultRowInfo, pRes));
        pMAExtW->lastFinalizedWinIdx = pMAExtW->pResultRow->winIdx;
      }
      // fill remaining empty windows with NULL rows (only for virtual table COLS merge)
      if (pExtW->isDynWindow && EEXT_MODE_AGG == pExtW->mode && pExtW->pTGrpCtx && pExtW->pTGrpCtx->pCCtx && pExtW->pTGrpCtx->pCCtx->pWins) {
        int32_t totalWins = taosArrayGetSize(pExtW->pTGrpCtx->pCCtx->pWins);
        if (pMAExtW->lastFinalizedWinIdx + 1 < totalWins) {
          TAOS_CHECK_EXIT(mergeAlignExtWinFillEmptyWins(pOperator, &pExtW->binfo.resultRowInfo, pRes, pMAExtW->lastFinalizedWinIdx + 1, totalWins));
        }
      }
      setOperatorCompleted(pOperator);
      break;
    }

    extWinResolveBlockIdForPartition(pExtW, pTaskInfo, &pBlock->info.id);

    if (pExtW->lastCGrpId != pBlock->info.id.groupId) {
      if (pMAExtW->curTs != INT64_MIN && EEXT_MODE_AGG == pExtW->mode) {
        TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, &pExtW->binfo.resultRowInfo, pRes));

        // Group boundary: always clear row state and curTs after finalize to avoid
        // duplicate finalization and cross-group state reuse on next block.

        if (pMAExtW->pResultRow != NULL) {
          resetResultRow(pMAExtW->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));
        }

        pMAExtW->curTs = INT64_MIN;
      }

      if (pRes->info.rows > 0) {
        pMAExtW->pNewGroup = pBlock;
        pMAExtW->curTs = INT64_MIN;
        break;
      }
    }

    TAOS_CHECK_EXIT(extWinSwitchInitCtxs(pExtW, pTaskInfo, &pBlock->info.id));

    pRes->info.scanFlag = pBlock->info.scanFlag;
    pRes->info.id.groupId = pBlock->info.id.baseGId;  // only keep TGroup ID

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

  pStream->pStreamBlkWinIdx = pExtW->pWinRowIdx;

  extWinPostUpdateStreamRt(pStream, pOperator, pExtW);
  
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

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SSDataBlock* pRes = pExtW->binfo.pRes;
  blockDataCleanup(pRes);

  mergeAlignExtWinDo(pOperator);

  if (pRes->info.rows > 0 && pOperator->exprSupp.pFilterInfo != NULL) {
    SColumnInfoData* pFilterRes = NULL;
    SColMatchInfo* pMatchInfo = pExtW->fillMatchInfo.pList != NULL ? &pExtW->fillMatchInfo : NULL;
    TAOS_CHECK_EXIT(doFilter(pRes, pOperator->exprSupp.pFilterInfo, pMatchInfo, &pFilterRes));
    colDataDestroy(pFilterRes);
    taosMemoryFree(pFilterRes);
  }

  size_t rows = pRes->info.rows;
  (*ppRes) = (rows == 0) ? NULL : pRes;

_exit:

  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static int32_t resetMergeAlignedExtWinOperator(SOperatorInfo* pOperator) {
  int32_t code = 0, lino = 0;
  SMergeAlignedExternalWindowOperator* pMAExtW = pOperator->info;
  SExternalWindowOperator*             pExtW = pMAExtW->pExtW;
  SExecTaskInfo*                       pTaskInfo = pOperator->pTaskInfo;
  SMergeAlignedIntervalPhysiNode * pPhynode = (SMergeAlignedIntervalPhysiNode*)pOperator->pPhyNode;
  pOperator->status = OP_NOT_OPENED;

  //resetBasicOperatorState(&pExtW->binfo);
  //pMAExtW->pResultRow = NULL;

  initResultRowInfo(&pExtW->binfo.resultRowInfo);
  pMAExtW->curTs = INT64_MIN;
  pMAExtW->lastFinalizedWinIdx = -1;

  extWinResetResultRows(&pExtW->resultRows);

/*
  int32_t code = resetAggSup(&pOperator->exprSupp, &pExtW->aggSup, pTaskInfo, pPhynode->window.pFuncs, NULL,
                             sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, NULL,
                             &pTaskInfo->storageAPI.functionStore);
*/                             

  colDataDestroy(&pExtW->twAggSup.timeWindowData);
  TAOS_CHECK_EXIT(initExecTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pTaskInfo->window));

  pExtW->outWinIdx = 0;
  pExtW->lastTGrpId = 0;
  pExtW->lastCGrpId = 0;
  // ownTGrpCtx==true: this operator owns the allocation; free before reset.
  // ownTGrpCtx==false: borrowed pointer into pGroupCalcInfos; must not free.
  if (pExtW->ownTGrpCtx && pExtW->pTGrpCtx) {
    extWinDestroyTGrpCtx(pExtW->pTGrpCtx);
    taosMemoryFree(pExtW->pTGrpCtx);
  }
  pExtW->pTGrpCtx = NULL;
  pExtW->ownTGrpCtx = false;

_exit:

  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
  }
  
  return code;
}

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
      } else if (nodeType(pTarget->pExpr) == QUERY_NODE_FUNCTION) {
        SFunctionNode* pFunc = (SFunctionNode*)pTarget->pExpr;
        dtype = pFunc->node.resType;
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
  pExtW->pTaskInfo = pTaskInfo;
  SExprSupp* pSup = &pOperator->exprSupp;
  pSup->hasWindowOrGroup = true;
  pSup->hasWindow = true;
  pMAExtW->curTs = INT64_MIN;
  pMAExtW->lastFinalizedWinIdx = -1;

  pExtW->primaryTsIndex = ((SColumnNode*)pPhynode->window.pTspk)->slotId;
  pExtW->mode = pPhynode->window.pProjs ? EEXT_MODE_SCALAR : EEXT_MODE_AGG;
  pExtW->binfo.inputTsOrder = pPhynode->window.node.inputTsOrder = TSDB_ORDER_ASC;
  pExtW->binfo.outputTsOrder = pExtW->binfo.inputTsOrder;
  pExtW->needGroupSort = pPhynode->needGroupSort;
  pExtW->calcWithPartition = pPhynode->calcWithPartition;
  pExtW->extWinSplit = pPhynode->extWinSplit;
  pExtW->fillMode = pPhynode->extFill.mode;
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
  
  setOperatorInfo(pOperator, "MergeAlignedExternalWindowOperator", QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW, false, OP_NOT_OPENED, pMAExtW, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, mergeAlignExtWinNext, NULL,
                                         destroyMergeAlignedExternalWindowOperator, optrDefaultBufFn, NULL,
                                         optrDefaultGetNextExtFn, NULL);
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

static void extWinResetResultRows(SExtWinResultRows* pRows);
static int32_t resetExternalWindowExprSupp(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo,
                                           SExternalWindowPhysiNode* pPhynode) {
  int32_t    code = 0, lino = 0, num = 0;
  SExprInfo* pExprInfo = NULL;
  cleanupExprSuppWithoutFilter(&pExtW->scalarSupp);

  SNodeList* pNodeList = NULL;
  if (pPhynode->window.pProjs) {
    pNodeList = pPhynode->window.pProjs;
  } else {
    pNodeList = pPhynode->window.pExprs;
  }

  code = createExprInfo(pNodeList, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);
  code = initExprSupp(&pExtW->scalarSupp, pExprInfo, num, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);
  pExtW->lastGrpIdx = INT32_MAX;
  extWinResetResultRows(&pExtW->resultRows);
  return code;
_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    pTaskInfo->code = code;
  }
  return code;
}


static int32_t resetExternalWindowOperator(SOperatorInfo* pOperator) {
  int32_t code = 0, lino = 0;
  SExternalWindowOperator* pExtW = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SExternalWindowPhysiNode* pPhynode = (SExternalWindowPhysiNode*)pOperator->pPhyNode;
  pOperator->status = OP_NOT_OPENED;

  //resetBasicOperatorState(&pExtW->binfo);
  initResultRowInfo(&pExtW->binfo.resultRowInfo);

  extWinRecycleBlkNode(pExtW, &pExtW->pLastBlkNode);

/*
  int32_t code = blockDataEnsureCapacity(pExtW->binfo.pRes, pOperator->resultInfo.capacity);
  if (code == 0) {
    code = resetAggSup(&pOperator->exprSupp, &pExtW->aggSup, pTaskInfo, pPhynode->window.pFuncs, NULL,
                       sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, pTaskInfo->streamInfo.pState,
                       &pTaskInfo->storageAPI.functionStore);
  }
*/
  TAOS_CHECK_EXIT(resetExternalWindowExprSupp(pExtW, pTaskInfo, pPhynode));
  colDataDestroy(&pExtW->twAggSup.timeWindowData);
  TAOS_CHECK_EXIT(initExecTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pTaskInfo->window));

  pExtW->resWinIdx = 0;
  pExtW->lastOutputIter = 0;
  pExtW->outWinIdx = 0;
  pExtW->lastTGrpId = 0;
  pExtW->lastCGrpId = 0;
  // ownTGrpCtx==true: this operator owns the allocation; free before reset.
  // ownTGrpCtx==false: borrowed pointer into pGroupCalcInfos; must not free.
  if (pExtW->ownTGrpCtx && pExtW->pTGrpCtx) {
    extWinDestroyTGrpCtx(pExtW->pTGrpCtx);
    taosMemoryFree(pExtW->pTGrpCtx);
  }
  pExtW->pTGrpCtx = NULL;
  pExtW->ownTGrpCtx = false;
  pExtW->isDynWindow = false;

  extWinResetResultRows(&pExtW->resultRows);
  
  qDebug("%s ext window stat at reset, created:%" PRId64 ", destroyed:%" PRId64 ", recycled:%" PRId64 ", reused:%" PRId64 ", append:%" PRId64, 
      pTaskInfo->id.str, pExtW->stat.resBlockCreated, pExtW->stat.resBlockDestroyed, pExtW->stat.resBlockRecycled, 
      pExtW->stat.resBlockReused, pExtW->stat.resBlockAppend);

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
        *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, r, pWin->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);

        qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
            GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, pWin->tw.skey, pWin->tw.ekey, *winRows, r, tsCol[r], tsCol[r + *winRows - 1]);
        
        return TSDB_CODE_SUCCESS;
      }

      if (!pOperator->pTaskInfo->pStreamRuntimeInfo && tsCol[r] >= pWin->tw.ekey) {
        extWinSetCurWinIdx(pOperator, pCCtx->blkWinIdx);
        *ppWin = pWin;
        *startPos = r;
        *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, r, pWin->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);

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
        *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, r, pWin->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);

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
    *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, *startPos, (*ppWin)->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);

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
        *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, r, pWin->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);

        qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
            GET_TASKID(pOperator->pTaskInfo), __func__, pCCtx->blkWinIdx, pWin->tw.skey, pWin->tw.ekey, *winRows, r, tsCol[r], tsCol[r + *winRows - 1]);
        
        return TSDB_CODE_SUCCESS;
      }

      if (!pOperator->pTaskInfo->pStreamRuntimeInfo && tsCol[r] >= pWin->tw.ekey) {
        extWinSetCurWinIdx(pOperator, pCCtx->blkWinIdx);
        *ppWin = pWin;
        *startPos = r;
        *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, r, pWin->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);

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
    *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, *startPos, (*ppWin)->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);
    
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
        *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, r, pWin->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);

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


static int32_t extWinGetResultRow(SExecTaskInfo* pTaskInfo, SExternalWindowOperator* pExtW, int32_t winIdx, int32_t resultRowSize, SResultRow** ppRes) {
  int32_t code = 0, lino = 0;
  SExtWinResultRows* pRows = &pExtW->resultRows;
  while (true) {
    if (pRows->pResultRows[pRows->resRowsIdx] && pRows->resRowIdx < pRows->resRowSize) {
      break;
    }

    if (NULL == pRows->pResultRows[pRows->resRowsIdx]) {
      pRows->pResultRows[pRows->resRowsIdx] = taosMemoryMalloc(pRows->resRowSize * resultRowSize);
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

static int32_t extWinAppendAggFilledRow(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW,
                                        SExtWinCalcGrpCtx* pCCtx, SExtWinTimeWindow* pWin, SResultRow* pSrcRow) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  char*           pTmpBuf = NULL;
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
          int32_t numWins = taosArrayGetSize(pCCtx->pWins);
          for (int32_t w = 0; w < numWins; ++w) {
            SExtWinTimeWindow* pW = TARRAY_GET_ELEM(pCCtx->pWins, w);
            if (pW->resWinIdx >= 0) {
              SResultRow* pR = extWinGetResultRowByIdx(pExtW, pW->resWinIdx, pExtW->aggSup.resultRowSize);
              if (pR != NULL && extWinRowHasSourceData(pR)) {
                pAnyRow = pR;
                break;
              }
            }
          }
        }

        if (!patched && pAnyRow != NULL) {
          SResultRowEntryInfo* pEntryInfo = getResultEntryInfo(pAnyRow, j, rowEntryOffset);
          SGroupKeyInfo* pGKInfo = GET_ROWCELL_INTERBUF(pEntryInfo);
          if (pGKInfo->hasResult && !pGKInfo->isNull) {
            TAOS_CHECK_EXIT(colDataSetVal(pColInfo, pBlock->info.rows, pGKInfo->data, false));
          }
        }
      }
    }

    if (extWinUsesValueFillMode(pExtW->fillMode)) {
      TAOS_CHECK_EXIT(extWinApplyValueFill(pExtW, pBlock, pBlock->info.rows, 1));
    }

    pBlock->info.rows += 1;
    TAOS_CHECK_EXIT(extWinAppendWinIdx(pTaskInfo, pExtW->pWinRowIdx, pBlock, pCCtx->outWinIdx, 1));
    return code;
  }

  pTmpBuf = taosMemoryMalloc(pExtW->aggSup.resultRowSize);
  TSDB_CHECK_NULL(pTmpBuf, code, lino, _exit, terrno);

  memcpy(pTmpBuf, pSrcRow, pExtW->aggSup.resultRowSize);
  SResultRow* pTmpRow = (SResultRow*)pTmpBuf;
  pTmpRow->win = pWin->tw;
  pTmpRow->winIdx = pCCtx->outWinIdx;

  // The source row may not have had doUpdateNumOfRows called yet (e.g. NEXT
  // fill copies from a forward window whose numOfRows hasn't been finalized).
  doUpdateNumOfRows(pCtx, pTmpRow, numOfExprs, rowEntryOffset);

  // Patch window pseudo column results (_wstart/_twstart, _wend/_twend) in the
  // copied row so they reflect the current window, not the source (adjacent) window.
  for (int32_t j = 0; j < numOfExprs; ++j) {
    if (!pCtx[j].isPseudoFunc) continue;
    int32_t funcType = pCtx[j].pExpr->pExpr->_function.functionType;
    if (funcType == FUNCTION_TYPE_WSTART || funcType == FUNCTION_TYPE_TWSTART) {
      SResultRowEntryInfo* pEntryInfo = getResultEntryInfo(pTmpRow, j, rowEntryOffset);
      *(int64_t*)GET_ROWCELL_INTERBUF(pEntryInfo) = pWin->tw.skey;
    } else if (funcType == FUNCTION_TYPE_WEND || funcType == FUNCTION_TYPE_TWEND) {
      SResultRowEntryInfo* pEntryInfo = getResultEntryInfo(pTmpRow, j, rowEntryOffset);
      *(int64_t*)GET_ROWCELL_INTERBUF(pEntryInfo) = pWin->tw.ekey;
    } else if (funcType == FUNCTION_TYPE_WDURATION || funcType == FUNCTION_TYPE_TWDURATION) {
      SResultRowEntryInfo* pEntryInfo = getResultEntryInfo(pTmpRow, j, rowEntryOffset);
      *(int64_t*)GET_ROWCELL_INTERBUF(pEntryInfo) = pWin->tw.ekey - pWin->tw.skey;
    } else if (funcType == FUNCTION_TYPE_EXTERNAL_WINDOW_COLUMN && pTaskInfo->pStreamRuntimeInfo != NULL) {
      // w.mark is window metadata — always show the current window's own value,
      // not the adjacent (source) window's value copied by memcpy above.
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
              SResultRowEntryInfo* pEntryInfo = getResultEntryInfo(pTmpRow, j, rowEntryOffset);
              char* dest = GET_ROWCELL_INTERBUF(pEntryInfo);
              if (pVal->isNull) {
                pEntryInfo->isNullRes = 1;
              } else {
                pEntryInfo->isNullRes = 0;
                if (IS_VAR_DATA_TYPE(pVal->data.type)) {
                  memcpy(dest, pVal->data.pData, pVal->data.nData);
                } else {
                  memcpy(dest, &pVal->data.val, pExprInfo[j].base.resSchema.bytes);
                }
              }
            }
          }
        }
      }
    }
  }

  // For empty windows (nOrigRows == 0) filled from an adjacent row, the group key
  // in the copied result row was never populated by aggregation.  Patch it from any
  // result row that actually received source data in this partition (calc group).
  if (pTmpRow->nOrigRows == 0) {
    SResultRow* pAnyRow = NULL;
    for (int32_t j = 0; j < numOfExprs; ++j) {
      if (!(fmIsGroupKeyFunc(pCtx[j].functionId) || fmisSelectGroupConstValueFunc(pCtx[j].functionId))) {
        continue;
      }

      SResultRowEntryInfo* pTmpEntry = getResultEntryInfo(pTmpRow, j, rowEntryOffset);
      SGroupKeyInfo* pTmpGK = GET_ROWCELL_INTERBUF(pTmpEntry);
      if (pTmpGK->hasResult && !pTmpGK->isNull) {
        continue;  // already valid (e.g., _group_const_value from the source row)
      }

      // Find any result row with source data in this calc group
      if (pAnyRow == NULL) {
        int32_t numWins = taosArrayGetSize(pCCtx->pWins);
        for (int32_t w = 0; w < numWins; ++w) {
          SExtWinTimeWindow* pW = TARRAY_GET_ELEM(pCCtx->pWins, w);
          if (pW->resWinIdx >= 0) {
            SResultRow* pR = extWinGetResultRowByIdx(pExtW, pW->resWinIdx, pExtW->aggSup.resultRowSize);
            if (pR != NULL && extWinRowHasSourceData(pR)) {
              pAnyRow = pR;
              break;
            }
          }
        }
      }

      if (pAnyRow != NULL) {
        SResultRowEntryInfo* pSrcEntry = getResultEntryInfo(pAnyRow, j, rowEntryOffset);
        SGroupKeyInfo* pSrcGK = GET_ROWCELL_INTERBUF(pSrcEntry);
        if (pSrcGK->hasResult) {
          // Copy the group key intermediate buffer from the source row
          int32_t interBufSize = pCtx[j].resDataInfo.interBufSize;
          memcpy(pTmpGK, pSrcGK, interBufSize);
          pTmpEntry->numOfRes = 1;
          pTmpEntry->isNullRes = pSrcGK->isNull ? 1 : 0;
        }
      }
    }
  }

  if (pBlock->info.rows + pTmpRow->numOfRows > pBlock->info.capacity) {
    TAOS_CHECK_EXIT(blockDataEnsureCapacity(pBlock, pBlock->info.rows + pTmpRow->numOfRows));
  }

  int32_t startRow = pBlock->info.rows;
  updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pTmpRow->win, 0);
  TAOS_CHECK_EXIT(copyResultrowToDataBlock(pExprInfo, numOfExprs, pTmpRow, pCtx, pBlock, rowEntryOffset, pTaskInfo));

  if (extWinUsesValueFillMode(pExtW->fillMode)) {
    TAOS_CHECK_EXIT(extWinApplyValueFill(pExtW, pBlock, startRow, pTmpRow->numOfRows));
  }

  pBlock->info.rows += pTmpRow->numOfRows;
  TAOS_CHECK_EXIT(extWinAppendWinIdx(pTaskInfo, pExtW->pWinRowIdx, pBlock, pCCtx->outWinIdx, pTmpRow->numOfRows));

_exit:
  taosMemoryFree(pTmpBuf);
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

static bool extWinLastWinClosed(SExternalWindowOperator* pExtW) {
  if (pExtW->resWinIdx <= 0 || (pExtW->multiTableMode && !pExtW->inputHasOrder)) {
    return false;
  }

  if (pExtW->pTGrpCtx == NULL || pExtW->pTGrpCtx->pCCtx == NULL || pExtW->pTGrpCtx->pCCtx->lastWinIdx < 0) {
    return false;
  }

  if (NULL == pExtW->timeRangeExpr || !pExtW->timeRangeExpr->needCalc) {
    return true;
  }

  SList* pList = taosArrayGetP(pExtW->pOutputBlocks, pExtW->pTGrpCtx->pCCtx->lastWinIdx);
  if (pList == NULL) {
    return false;
  }
  if (0 == listNEles(pList)) {
    return true;
  }

  SListNode* pNode = listTail(pList);
  SArray* pBlkWinIdx = *((SArray**)pNode->data + 1);
  int64_t* pIdx = taosArrayGetLast(pBlkWinIdx);
  if (pIdx && *(int32_t*)pIdx < pExtW->pTGrpCtx->pCCtx->blkWinStartIdx) {
    return true;
  }

  return false;
}

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
  
  if (pWin->resWinIdx >= 0) {
    TAOS_CHECK_EXIT(extWinEnsureBlockList(pExtW, pWin->resWinIdx, &pList));
  } else {
    if (extWinLastWinClosed(pExtW)) {
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
  int32_t                  code = TSDB_CODE_SUCCESS, lino = 0;
  
  TAOS_CHECK_EXIT(extWinGetWinResBlock(pOperator, rows, pWin, &pResBlock, &pIdx));

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
                                                  true, pExprSup->hasIndefRowsFunc));

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
                                 pExtW->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo)));
  }

  TAOS_CHECK_EXIT(setInputDataBlock(pSup, pBlock, order, scanFlag, false));

  TAOS_CHECK_EXIT(blockDataEnsureCapacity(pRes, pRes->info.rows + pBlock->info.rows));

  TAOS_CHECK_EXIT(projectApplyFunctions(pSup->pExprInfo, pRes, pBlock, pSup->pCtx, pSup->numOfExprs,
                               pExtW->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo)));

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
    for (int32_t k = 0; k < pSupp->numOfExprs; ++k) {
      SqlFunctionCtx* pCtx = &pSupp->pCtx[k];
      pCtx->pOutput = NULL;
    }
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
  
  if (pWin->resWinIdx >= 0) {
    TAOS_CHECK_EXIT(extWinEnsureBlockList(pExtW, pWin->resWinIdx, &pList));
    TAOS_CHECK_EXIT(extWinIndefRowsSetWinOutputBuf(pExtW, pWin, &pOperator->exprSupp, &pExtW->aggSup, pOperator->pTaskInfo, false));
  } else {
    if (extWinLastWinClosed(pExtW)) {
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
  int32_t                  code = TSDB_CODE_SUCCESS, lino = 0;
  
  TAOS_CHECK_EXIT(extWinGetSetWinResBlockBuf(pOperator, rows, pWin, &pResBlock, &pIdx));
  
  if (!pExtW->pTmpBlock) {
    TAOS_CHECK_EXIT(createOneDataBlock(pInputBlock, false, &pExtW->pTmpBlock));
  } else {
    blockDataCleanup(pExtW->pTmpBlock);
  }
  
  TAOS_CHECK_EXIT(blockDataEnsureCapacity(pExtW->pTmpBlock, TMAX(1, rows)));

  TAOS_CHECK_EXIT(blockDataMergeNRows(pExtW->pTmpBlock, pInputBlock, startPos, rows));
  TAOS_CHECK_EXIT(extWinIndefRowsDoImpl(pOperator, pResBlock, pExtW->pTmpBlock));

  pResBlock->info.id.groupId = pInputBlock->info.id.groupId;

  TAOS_CHECK_EXIT(extWinAppendWinIdx(pOperator->pTaskInfo, pIdx, pResBlock, extWinGetCurWinIdx(pOperator), rows));

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

  for (; pCCtx->outWinIdx < numOfWin && pCCtx->outWinLastIdx < pCCtx->lastWinIdx; pCCtx->outWinIdx += 1, extWinIncCurWinOutIdx(pOperator)) {
    SExtWinTimeWindow* pWin = TARRAY_GET_ELEM(pCCtx->pWins, pCCtx->outWinIdx);
    if (pWin->resWinIdx < 0 || pWin->resWinIdx == pCCtx->outWinLastIdx) {
      continue;
    }

    pCCtx->outWinLastIdx = pWin->resWinIdx;
    SList* pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->resWinIdx);
    if (listNEles(pList) <= 0) {
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

static int32_t extWinNonAggOutputMultiGrpRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, SSDataBlock** ppRes) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  SSDataBlock*    pBlock = NULL;

  if (pStream->curGrpCalc) {
    TAOS_CHECK_EXIT(extWinNonAggOutputSingleGrpRes(pOperator, pExtW, &pBlock));
  }
  if (pBlock == NULL || pBlock->info.rows == 0) {
    pStream->curGrpCalc = tSimpleHashIterate(pStream->pGroupCalcInfos, pStream->curGrpCalc, &pExtW->lastOutputIter);
    while (pStream->curGrpCalc != NULL) {
      if (pStream->curGrpCalc->pRunnerGrpCtx) {
        pExtW->lastTGrpId = *(uint64_t*)tSimpleHashGetKey(pStream->curGrpCalc, NULL);
        pExtW->pTGrpCtx = pStream->curGrpCalc->pRunnerGrpCtx;
        pExtW->ownTGrpCtx = false;
        
        TAOS_CHECK_EXIT(extWinNonAggOutputSingleGrpRes(pOperator, pExtW, &pBlock));
        if (pBlock != NULL && pBlock->info.rows > 0) {
          break;
        }
      }
      
      pStream->curGrpCalc = tSimpleHashIterate(pStream->pGroupCalcInfos, pStream->curGrpCalc, &pExtW->lastOutputIter);
    }
  }

  extWinPostUpdateStreamRt(pStream, pOperator, pExtW);

_exit:

  *ppRes = pBlock;

  if (code != TSDB_CODE_SUCCESS) {
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

  if (pStream->isMultiGroupCalc) {
    TAOS_CHECK_EXIT(extWinNonAggOutputMultiGrpRes(pOperator, pExtW, &pRes));
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

  *ppRes = (pRes && pRes->info.rows > 0) ? pRes : NULL;

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinAggHandleEmptyWins(SOperatorInfo* pOperator, SSDataBlock* pBlock, bool allRemains, SExtWinTimeWindow* pWin) {
  int32_t code = 0, lino = 0;
  SExternalWindowOperator* pExtW = (SExternalWindowOperator*)pOperator->info;
  SExprSupp* pSup = &pOperator->exprSupp;
  int32_t currIdx = extWinGetCurWinIdx(pOperator);

  if (NULL == pExtW->pEmptyInputBlock || NULL == pExtW->pTGrpCtx || NULL == pExtW->pTGrpCtx->pCCtx ||
      NULL == pExtW->pTGrpCtx->pCCtx->pWins) {
    goto _exit;
  }

  if (pWin && pWin->tw.skey == pExtW->pTGrpCtx->pCCtx->lastSKey &&
      pWin->tw.ekey == pExtW->pTGrpCtx->pCCtx->lastEKey) {
    goto _exit;
  }

  bool ascScan = pExtW->binfo.inputTsOrder == TSDB_ORDER_ASC;
  int32_t endIdx = allRemains ? (pExtW->pTGrpCtx->pCCtx->pWins->size - 1) : (currIdx - 1);
  SResultRowInfo* pResultRowInfo = &pExtW->binfo.resultRowInfo;
  SSDataBlock* pInput = pExtW->pEmptyInputBlock;

  if ((pExtW->pTGrpCtx->pCCtx->lastWinId + 1) <= endIdx) {
    TAOS_CHECK_EXIT(setInputDataBlock(pSup, pExtW->pEmptyInputBlock, pExtW->binfo.inputTsOrder, MAIN_SCAN, true));
  }
  
  for (int32_t i = pExtW->pTGrpCtx->pCCtx->lastWinId + 1; i <= endIdx; ++i) {
    SExtWinTimeWindow* pWin = taosArrayGet(pExtW->pTGrpCtx->pCCtx->pWins, i);

    extWinSetCurWinIdx(pOperator, i);
    qDebug("%s tgrp %" PRIu64 " cgrp %" PRIu64 " %dth ext empty window start:%" PRId64 ", end:%" PRId64 ", ascScan:%d",
           GET_TASKID(pOperator->pTaskInfo), pExtW->lastTGrpId, pExtW->lastCGrpId, i, pWin->tw.skey, pWin->tw.ekey, ascScan);

    TAOS_CHECK_EXIT(extWinAggSetWinOutputBuf(pOperator, pWin, pSup, &pExtW->aggSup, pOperator->pTaskInfo));

    updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pWin->tw, 1);
    code = extWinAggDo(pOperator, 0, 1, pInput);
    pExtW->pTGrpCtx->pCCtx->lastWinId = i;  
    TAOS_CHECK_EXIT(code);
  }

  
_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", pOperator->pTaskInfo->id.str, __FUNCTION__, lino, tstrerror(code));
  } else {
    if (pBlock) {
      TAOS_CHECK_EXIT(setInputDataBlock(pSup, pBlock, pExtW->binfo.inputTsOrder, pBlock->info.scanFlag, true));
    }

    if (!allRemains) {
      extWinSetCurWinIdx(pOperator, currIdx);  
    }
  }

  return code;
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

    qDebug("%s ext window [%" PRId64 ", %" PRId64 ") tgrp %" PRIu64 " cgrp %" PRIu64 " agg start, ascScan:%d, startPos:%d, winRows:%d",
           GET_TASKID(pOperator->pTaskInfo), pWin->tw.skey, pWin->tw.ekey, pExtW->lastTGrpId, pExtW->lastCGrpId, ascScan, startPos, winRows);        

    if (!scalarCalc) {
      if (pExtW->scalarSupp.pExprInfo) {
        SExprSupp* pScalarSup = &pExtW->scalarSupp;
        TAOS_CHECK_EXIT(projectApplyFunctions(pScalarSup->pExprInfo, pInputBlock, pInputBlock, pScalarSup->pCtx, pScalarSup->numOfExprs,
                                     pExtW->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo)));
      }
      
      scalarCalc = true;
    }

    if (pWin->tw.skey != pExtW->pTGrpCtx->pCCtx->lastSKey || pWin->tw.ekey != pExtW->pTGrpCtx->pCCtx->lastEKey ||
        pWin->tw.skey == INT64_MIN) {
      TAOS_CHECK_EXIT(
          extWinAggSetWinOutputBuf(pOperator, pWin, &pOperator->exprSupp, &pExtW->aggSup, pOperator->pTaskInfo));
    }

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

      pBlock->info.rows += pRow->numOfRows;
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

    pBlock->info.rows += pRow->numOfRows;

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

static int32_t extWinAggOutputMulNoOrderCGrpsRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  int32_t code = 0, lino = 0;
  SExtWinTrigGrpCtx* pTGrpCtx = pExtW->pTGrpCtx;
  SSDataBlock*    pBlock = pExtW->binfo.pRes;

  if (pTGrpCtx == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (pTGrpCtx->pCCtx) {
    pExtW->lastCGrpId = pTGrpCtx->pCCtx->groupId;
    TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, NULL));
  }
  if (0 == pBlock->info.rows) {
    pTGrpCtx->pCCtx = tSimpleHashIterate(pTGrpCtx->pCGCtxs, pTGrpCtx->pCCtx, &pTGrpCtx->lastCtxIter);
    while (pTGrpCtx->pCCtx != NULL) {
      pExtW->lastCGrpId = pTGrpCtx->pCCtx->groupId;
      TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, NULL));
      if (pBlock->info.rows > 0) {
        break;
      }
      
      pTGrpCtx->pCCtx = tSimpleHashIterate(pTGrpCtx->pCGCtxs, pTGrpCtx->pCCtx, &pTGrpCtx->lastCtxIter);
    }
  }

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pOperator->pTaskInfo), __func__, lino, tstrerror(code));
  }

  return code;
}



static int32_t extWinAggOutputMulNoOrderTGrpsRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  SSDataBlock*    pBlock = pExtW->binfo.pRes;

  if (pStream->curGrpCalc) {
    pExtW->lastTGrpId = *(uint64_t*)tSimpleHashGetKey(pStream->curGrpCalc, NULL);
    pExtW->pTGrpCtx = pStream->curGrpCalc->pRunnerGrpCtx;
    pExtW->ownTGrpCtx = false;
    if (pExtW->calcWithPartition) {
      TAOS_CHECK_EXIT(extWinAggOutputMulNoOrderCGrpsRes(pOperator, pExtW));
    } else {
      TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, NULL));
    }
  }
  if (0 == pBlock->info.rows) {
    pStream->curGrpCalc = tSimpleHashIterate(pStream->pGroupCalcInfos, pStream->curGrpCalc, &pExtW->lastOutputIter);
    while (pStream->curGrpCalc != NULL) {
      if (pStream->curGrpCalc->pRunnerGrpCtx) {
        pExtW->lastTGrpId = *(uint64_t*)tSimpleHashGetKey(pStream->curGrpCalc, NULL);
        pExtW->pTGrpCtx = pStream->curGrpCalc->pRunnerGrpCtx;
        pExtW->ownTGrpCtx = false;
        pExtW->lastCGrpId = 0;

        if (pExtW->calcWithPartition) {
          // Same ownership rule as in extWinPrepareForOutput (see comment there).
          if (pExtW->pTGrpCtx->pCCtx && pExtW->pTGrpCtx->pCGCtxs == NULL) {
            extWinDestroyCGrpCtx(pExtW->pTGrpCtx->pCCtx);
            taosMemoryFree(pExtW->pTGrpCtx->pCCtx);
          }
          pExtW->pTGrpCtx->pCCtx = NULL;
          TAOS_CHECK_EXIT(extWinAggOutputMulNoOrderCGrpsRes(pOperator, pExtW));
        } else {
          TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, NULL));
        }
        
        if (pBlock->info.rows > 0) {
          break;
        }
      }
      
      pStream->curGrpCalc = tSimpleHashIterate(pStream->pGroupCalcInfos, pStream->curGrpCalc, &pExtW->lastOutputIter);
    }
  }

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
    pTGrpCtx->pCCtx = tSimpleHashGet(pTGrpCtx->pCGCtxs, grpId, sizeof(*grpId));
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
  SExtWinTrigGrpCtx* pTGrpCtx = pExtW->pTGrpCtx;
  SSDataBlock*    pBlock = pExtW->binfo.pRes;
  int32_t grpNum = taosArrayGetSize(pExtW->pGrpIds);
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  bool grpDone = false;

  for (; pExtW->lastGrpIdx < grpNum; ++pExtW->lastGrpIdx) {
    uint64_t *grpId = taosArrayGet(pExtW->pGrpIds, pExtW->lastGrpIdx);

    pStream->curGrpCalc = tSimpleHashGet(pStream->pGroupCalcInfos, grpId, sizeof(*grpId));
    TSDB_CHECK_NULL(pStream->curGrpCalc, code, lino, _exit, terrno);
    pExtW->pTGrpCtx = pStream->curGrpCalc->pRunnerGrpCtx;
    pExtW->ownTGrpCtx = false;

    TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, &grpDone));
    if (pBlock->info.rows > 0) {
      if (grpDone) {
        pExtW->lastGrpIdx++;
      }

      break;
    }
  }

  extWinPostUpdateStreamRt(pStream, pOperator, pExtW);

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pOperator->pTaskInfo), __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinAggOutputMulOrderTCGrpsRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  int32_t code = 0, lino = 0;
  SExtWinTrigGrpCtx* pTGrpCtx = pExtW->pTGrpCtx;
  SSDataBlock*    pBlock = pExtW->binfo.pRes;
  int32_t grpNum = taosArrayGetSize(pExtW->pCTGrpIds);
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  bool grpDone = false;

  for (; pExtW->lastGrpIdx < grpNum; ++pExtW->lastGrpIdx) {
    uint64_t* cGrpId = taosArrayGet(pExtW->pCTGrpIds, pExtW->lastGrpIdx);
    uint64_t* tGrpId = cGrpId + 1;

    pStream->curGrpCalc = tSimpleHashGet(pStream->pGroupCalcInfos, tGrpId, sizeof(*tGrpId));
    TSDB_CHECK_NULL(pStream->curGrpCalc, code, lino, _exit, terrno);
    pExtW->pTGrpCtx = pStream->curGrpCalc->pRunnerGrpCtx;
    pExtW->ownTGrpCtx = false;

    pExtW->pTGrpCtx->pCCtx = tSimpleHashGet(pExtW->pTGrpCtx->pCGCtxs, cGrpId, sizeof(*cGrpId));
    TSDB_CHECK_NULL(pExtW->pTGrpCtx->pCCtx, code, lino, _exit, terrno);

    TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, &grpDone));
    if (pBlock->info.rows > 0) {
      if (grpDone) {
        pExtW->lastGrpIdx++;
      }
      
      break;
    }
  }

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

  pBlock->info.version = pTaskInfo->version;
  blockDataCleanup(pBlock);
  taosArrayClear(pExtW->pWinRowIdx);

  if (pExtW->needGroupSort) {
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
      TAOS_CHECK_EXIT(extWinAggOutputMulNoOrderCGrpsRes(pOperator, pExtW));
    } else {
      TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, NULL));
    }
  }
  
  qDebug("%s result generated, rows:%" PRId64 ", groupId:%" PRIu64, GET_TASKID(pTaskInfo), pBlock->info.rows,
         pBlock->info.id.groupId);

  int32_t rowsBeforeFilter = pBlock->info.rows;
  SColumnInfoData* pFilterRes = NULL;
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

  *ppRes = (pBlock->info.rows > 0) ? pBlock : NULL;

  if (*ppRes) {
    (*ppRes)->info.window.skey = pExtW->orgTableTimeRange.skey;
    (*ppRes)->info.window.ekey = pExtW->orgTableTimeRange.ekey;
  }
  if (pOperator->pTaskInfo->pStreamRuntimeInfo) {
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

static bool extWinNonAggGotResBlock(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  if (pExtW->calcWithPartition) {
    return false;
  }

  if ((pExtW->multiTableMode && !pExtW->inputHasOrder) || pExtW->needGroupSort) {
    return false;
  }
  int32_t remainWin = pExtW->resWinIdx - pExtW->outWinIdx;
  if (remainWin > 1 && (NULL == pExtW->timeRangeExpr || !pExtW->timeRangeExpr->needCalc)) {
    return true;
  }
  
  SList* pList = taosArrayGetP(pExtW->pOutputBlocks, pExtW->outWinIdx);
  if (!pList || listNEles(pList) <= 0) {
    return false;
  }
  if (listNEles(pList) > 1) {
    return true;
  }

  SListNode* pNode = listHead(pList);
  SArray* pIdx = *(SArray**)((SArray**)pNode->data + 1);
  int32_t* winIdx = taosArrayGetLast(pIdx);
  if (winIdx && *winIdx < pExtW->pTGrpCtx->pCCtx->blkWinStartIdx) {
    return true;
  }

  return false;
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


static int32_t extWinAggHandleMultiTGrpEmptyWins(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  int32_t         iter = 0;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStreamRuntimeFuncInfo* pStream = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  SSTriggerGroupCalcInfo* pGroup = NULL;

  if (NULL == pExtW->pEmptyInputBlock) {
    goto _exit;
  }
  
  pGroup = tSimpleHashIterate(pStream->pGroupCalcInfos, NULL, &iter);
  while (pGroup != NULL) {
    pExtW->lastTGrpId = *(uint64_t*)tSimpleHashGetKey(pGroup, NULL);

    if (NULL == pGroup->pRunnerGrpCtx) {
      pStream->curGrpCalc = pGroup;

      SExtWinTrigGrpCtx* pTGCtx = taosMemoryCalloc(1, sizeof(SExtWinTrigGrpCtx));
      TSDB_CHECK_NULL(pTGCtx, code, lino, _exit, terrno);

      pTGCtx->pCCtx = taosMemoryCalloc(1, sizeof(*pTGCtx->pCCtx));
      TSDB_CHECK_NULL(pTGCtx->pCCtx, code, lino, _exit, terrno);
      TAOS_CHECK_EXIT(extWinInitCGrpCtx(pExtW, pOperator->pTaskInfo, pTGCtx->pCCtx));
      pGroup->pRunnerGrpCtx = pTGCtx;
    }

    pExtW->pTGrpCtx = pGroup->pRunnerGrpCtx;
    pExtW->ownTGrpCtx = false;
    TAOS_CHECK_EXIT(extWinAggHandleEmptyWins(pOperator, NULL, true, NULL));
    
    pGroup = tSimpleHashIterate(pStream->pGroupCalcInfos, pGroup, &iter);
  }

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static void extWinPrepareForOutput(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW) {
  pExtW->lastGrpIdx = 0;
  pExtW->lastOutputIter = 0;
  pExtW->lastTGrpId = 0;
  pExtW->lastCGrpId = 0;

  if (pOperator == NULL || pOperator->pTaskInfo == NULL || pOperator->pTaskInfo->pStreamRuntimeInfo == NULL) {
    return;
  }

  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;

  pStream->curGrpCalc = NULL;
  pStream->curGrpRead = NULL;

  if (pExtW->needGroupSort) {
    if (pStream->isMultiGroupCalc && pExtW->calcWithPartition) {
      if (pExtW->pCTGrpIds) {
        taosArraySort(pExtW->pCTGrpIds, extWinGrpIdCompare);
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
    pExtW->ownTGrpCtx = false;
  }

  if ((!pStream->isMultiGroupCalc) && pExtW->calcWithPartition && pExtW->pTGrpCtx) {
    // pCGCtxs==NULL: pCCtx is a standalone taosMemoryCalloc allocation owned
    //   by pTGrpCtx; free it before clearing or extWinDestroyTGrpCtx will miss it.
    // pCGCtxs!=NULL: pCCtx is a raw pointer into hash-table inline storage;
    //   must not be freed here — tSimpleHashCleanup releases it.
    if (pExtW->pTGrpCtx->pCCtx && pExtW->pTGrpCtx->pCGCtxs == NULL) {
      extWinDestroyCGrpCtx(pExtW->pTGrpCtx->pCCtx);
      taosMemoryFree(pExtW->pTGrpCtx->pCCtx);
    }
    pExtW->pTGrpCtx->pCCtx = NULL;
  }
}

static int32_t extWinEnsureDynParamOpenCtx(SExternalWindowOperator* pExtW) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  if (pExtW->pTGrpCtx == NULL) {
    pExtW->pTGrpCtx = taosMemoryCalloc(1, sizeof(*pExtW->pTGrpCtx));
    TSDB_CHECK_NULL(pExtW->pTGrpCtx, code, lino, _exit, terrno);
    pExtW->ownTGrpCtx = true;
  }

  if (pExtW->pTGrpCtx->pCCtx == NULL) {
    pExtW->pTGrpCtx->pCCtx = taosMemoryCalloc(1, sizeof(*pExtW->pTGrpCtx->pCCtx));
    TSDB_CHECK_NULL(pExtW->pTGrpCtx->pCCtx, code, lino, _exit, terrno);
    extWinInitDynParamCGrpCtx(pExtW->pTGrpCtx->pCCtx);
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
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

    extWinInitDynParamCGrpCtx(pExtW->pTGrpCtx->pCCtx);
    pExtW->pTGrpCtx->pCCtx->pWins = pExtPram->ExtWins;

    pExtPram->ExtWins = NULL;
    pExtW->outWinIdx = 0;
    pExtW->isDynWindow = true;
    pExtW->orgTableTimeRange.skey = INT64_MAX;
    pExtW->orgTableTimeRange.ekey = INT64_MIN;

    QUERY_CHECK_CONDITION(pOperator->numOfDownstream == 1, code, lino, _exit, TSDB_CODE_INVALID_PARA)

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

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstreamRemainDetach(pOperator, 0);
    if (pBlock == NULL) {
      if (EEXT_MODE_AGG == pExtW->mode) {
        if (pInfo && pInfo->isMultiGroupCalc) {
          TAOS_CHECK_EXIT(extWinAggHandleMultiTGrpEmptyWins(pOperator, pExtW));
        } else {
          if (pExtW->pEmptyInputBlock && pExtW->pTGrpCtx == NULL) {
            pExtW->pTGrpCtx = taosMemoryCalloc(1, sizeof(*pExtW->pTGrpCtx));
            TSDB_CHECK_NULL(pExtW->pTGrpCtx, code, lino, _exit, terrno);
            pExtW->ownTGrpCtx = true;
            pExtW->pTGrpCtx->pCCtx = taosMemoryCalloc(1, sizeof(*pExtW->pTGrpCtx->pCCtx));
            TSDB_CHECK_NULL(pExtW->pTGrpCtx->pCCtx, code, lino, _exit, terrno);
            TAOS_CHECK_EXIT(extWinInitCGrpCtx(pExtW, pOperator->pTaskInfo, pExtW->pTGrpCtx->pCCtx));
          }
          TAOS_CHECK_EXIT(extWinAggHandleEmptyWins(pOperator, pBlock, true, NULL));
        }
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

    // Fallback for non-stream grouped external-window:
    // 1) if outer calc is partitioned and downstream only carries groupId,
    //    treat groupId as the corresponding trigger-group id;
    // 2) otherwise, if there is exactly one trigger group from subquery,
    //    use that singleton gid.
    if (pBlock->info.id.baseGId == 0 && pTaskInfo->pStreamRuntimeInfo &&
        pTaskInfo->pStreamRuntimeInfo->funcInfo.isMultiGroupCalc &&
        pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcInfos) {
      if (pExtW->calcWithPartition && pBlock->info.id.groupId != 0 &&
          tSimpleHashGet(pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcInfos,
                         &pBlock->info.id.groupId, sizeof(pBlock->info.id.groupId)) != NULL) {
        pBlock->info.id.baseGId = pBlock->info.id.groupId;
        qDebug("%s extWin fallback baseGId <- matched groupId %" PRIu64,
               GET_TASKID(pTaskInfo), pBlock->info.id.groupId);
      } else {
        int32_t __iter = 0;
        int32_t __size = tSimpleHashGetSize(pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcInfos);
        if (__size == 1) {
          SSTriggerGroupCalcInfo* __one = tSimpleHashIterate(pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcInfos, NULL, &__iter);
          if (__one) {
            uint64_t __gid = *(uint64_t*)tSimpleHashGetKey(__one, NULL);
            pBlock->info.id.baseGId = __gid;
            qDebug("%s extWin fallback baseGId set to single gid %" PRIu64, GET_TASKID(pTaskInfo), __gid);
          }
        }
      }
    }

    // Equality gating in partitioned multi-group mode:
    // process the block only when baseGId == groupId; otherwise skip it.
    if (pTaskInfo->pStreamRuntimeInfo &&
        pTaskInfo->pStreamRuntimeInfo->funcInfo.isMultiGroupCalc &&
        pExtW->calcWithPartition &&
        pBlock->info.id.groupId != 0 && pBlock->info.id.baseGId == 0) {
      qDebug("%s skip block: no matched trigger group for groupId %" PRIu64,
             GET_TASKID(pTaskInfo), pBlock->info.id.groupId);
      continue;
    }

    if (pTaskInfo->pStreamRuntimeInfo &&
        pTaskInfo->pStreamRuntimeInfo->funcInfo.isMultiGroupCalc &&
        pExtW->calcWithPartition &&
        pBlock->info.id.groupId != 0 && pBlock->info.id.baseGId != 0 &&
        pBlock->info.id.groupId != pBlock->info.id.baseGId) {
      qDebug("%s skip block: baseGId %" PRIu64 " != groupId %" PRIu64,
             GET_TASKID(pTaskInfo), pBlock->info.id.baseGId, pBlock->info.id.groupId);
      continue;
    }

    TAOS_CHECK_EXIT(extWinSwitchInitCtxs(pExtW, pTaskInfo, &pBlock->info.id));

    // Reset block-local traversal state for each new input block, but preserve
    // blkWinStartIdx so a window spanning multiple blocks can continue from the
    // same logical window on the next block.
    extWinResetBlockCalcState(pExtW->pTGrpCtx ? pExtW->pTGrpCtx->pCCtx : NULL);

    switch (pExtW->mode) {
      case EEXT_MODE_SCALAR:
        TAOS_CHECK_EXIT(extWinProjectOpen(pOperator, pBlock));
        if (extWinNonAggGotResBlock(pOperator, pExtW)) {
          goto _exit;
        }
        break;
      case EEXT_MODE_AGG:
        TAOS_CHECK_EXIT(extWinAggOpen(pOperator, pBlock));
        break;
      case EEXT_MODE_INDEFR_FUNC:
        TAOS_CHECK_EXIT(extWinIndefRowsOpen(pOperator, pBlock));
        if (extWinNonAggGotResBlock(pOperator, pExtW)) {
          goto _exit;
        }
        break;
      default:
        break;
    }
  }

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
  if (pOperator->status == OP_EXEC_DONE && !pOperator->pOperatorGetParam) {
    *ppRes = NULL;
    return code;
  }

  if (pOperator->pOperatorGetParam) {
    if (pOperator->status == OP_EXEC_DONE) {
      pOperator->status = OP_NOT_OPENED;
    }
  }

  extWinRecycleBlkNode(pExtW, &pExtW->pLastBlkNode);

  while (1) {
    if (pOperator->status == OP_NOT_OPENED) {
      TAOS_CHECK_EXIT(pOperator->fpSet._openFn(pOperator));
    }

    if (pExtW->mode == EEXT_MODE_SCALAR || pExtW->mode == EEXT_MODE_INDEFR_FUNC) {
      TAOS_CHECK_EXIT(extWinNonAggOutputRes(pOperator, ppRes));
      if (NULL == *ppRes) {
        setOperatorCompleted(pOperator);
        extWinFreeResultRow(pExtW);
      }
    } else {
      TAOS_CHECK_EXIT(extWinAggOutputRes(pOperator, ppRes));
      if (NULL != *ppRes) {
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

  if ((*ppRes) && (*ppRes)->info.rows <= 0) {
    *ppRes = NULL;
  }

  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM && (*ppRes)) {
    printDataBlock(*ppRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo), pTaskInfo->id.queryId);
  }

  return code;
}

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
  pExtW->pTaskInfo = pTaskInfo;
  pExtW->needGroupSort = pPhynode->needGroupSort;
  // In non-stream (batch) mode, temporarily disable calcWithPartition to tolerate
  // upstream that does not provide distinct C-group ids (groupId==baseGId).
  // Caller may decide to turn it back on once upstream is ready.
  pExtW->calcWithPartition = pPhynode->calcWithPartition;
  // pExtW->calcWithPartition = (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM) ? pPhynode->calcWithPartition : false;
  pExtW->extWinSplit = pPhynode->extWinSplit;
  
  setOperatorInfo(pOperator, "ExternalWindowOperator", QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW, true, OP_NOT_OPENED,
                  pExtW, pTaskInfo);
                  
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhynode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pExtW->binfo, pResBlock);

  pExtW->primaryTsIndex = ((SColumnNode*)pPhynode->window.pTspk)->slotId;
  pExtW->mode = pPhynode->window.pProjs ? EEXT_MODE_SCALAR : (pPhynode->window.indefRowsFunc ? EEXT_MODE_INDEFR_FUNC : EEXT_MODE_AGG);
  pExtW->fillMode = pPhynode->extFill.mode;
  pExtW->pFillExprs = pPhynode->extFill.pFillExprs;
  pExtW->pFillValues = pPhynode->extFill.pFillValues;
  pExtW->binfo.inputTsOrder = pPhynode->window.node.inputTsOrder = TSDB_ORDER_ASC;
  pExtW->binfo.outputTsOrder = pExtW->binfo.inputTsOrder;
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

  if (pPhynode->window.pProjs) {
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
    
    pExtW->binfo.inputTsOrder = pNode->inputTsOrder;
    pExtW->binfo.outputTsOrder = pNode->outputTsOrder;
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

  pOperator->fpSet = createOperatorFpSet(extWinOpen, extWinNext, NULL, destroyExternalWindowOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
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

  qDebug("%s extWin operator created, mode:%s, multiTableMnode:%d, inputHasOrder:%d, hasTimeRangeExpr:%d, timeRangeNeedCalc:%d "
    "needGroupSort:%d, extWinSplit:%d, calcWithPartition:%d",
    GET_TASKID(pTaskInfo), extWinModeStr(pExtW->mode), pExtW->multiTableMode, pExtW->inputHasOrder, pExtW->timeRangeExpr ? 1 : 0,
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
