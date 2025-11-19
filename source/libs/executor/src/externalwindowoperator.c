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
  SExtWindowStat     stat;
  SArray*            pWinRowIdx;
  bool               isMergeAlignedExtW;
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

void extWinDestroyCGrpCtx(void* param) {
  if (NULL == param) {
    return;
  }

  SExtWinCalcGrpCtx* pCtx = (SExtWinCalcGrpCtx*)param;

  taosArrayDestroy(pCtx->pWins);
}

void extWinDestroyTGrpCtx(void* param) {
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
  if (pExtW->extWinSplit) {
    pId->groupId = pExtW->lastCGrpId;
    pId->baseGId = pExtW->lastTGrpId;
  } else {
    pId->groupId = pExtW->lastTGrpId;
    pId->baseGId = 0;
  }

  qDebug("%s extWin res block assigned groupId %" PRIu64 " baseGid %" PRIu64, GET_TASKID(pOperator->pTaskInfo), pId->groupId, pId->baseGId);
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


void destroyExternalWindowOperatorInfo(void* param) {
  if (NULL == param) {
    return;
  }
  SExternalWindowOperator* pInfo = (SExternalWindowOperator*)param;
  cleanupBasicInfo(&pInfo->binfo);

  taosArrayDestroyEx(pInfo->pOutputBlocks, extWinDestroyBlockList);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  taosArrayDestroy(pInfo->pWinRowIdx);
  
  taosArrayDestroy(pInfo->pPseudoColInfo);
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
  
  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSupp);
  for (int32_t i = 0; i < pInfo->resultRows.resRowsSize && NULL != pInfo->resultRows.pResultRows[i]; ++i) {
    taosMemoryFreeClear(pInfo->resultRows.pResultRows[i]);
  }
  
  pInfo->binfo.resultRowInfo.openWindow = tdListFree(pInfo->binfo.resultRowInfo.openWindow);

  qDebug("ext window stat at destroy, created:%" PRId64 ", destroyed:%" PRId64 ", recycled:%" PRId64 ", reused:%" PRId64 ", append:%" PRId64, 
      pInfo->stat.resBlockCreated, pInfo->stat.resBlockDestroyed, pInfo->stat.resBlockRecycled, 
      pInfo->stat.resBlockReused, pInfo->stat.resBlockAppend);

  taosMemoryFreeClear(pInfo);
}

static int32_t extWinOpen(SOperatorInfo* pOperator);
static int32_t extWinNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);

typedef struct SMergeAlignedExternalWindowOperator {
  SExternalWindowOperator* pExtW;
  int64_t curTs;
  SResultRow*  pResultRow;
  SSDataBlock* pNewGroup;
} SMergeAlignedExternalWindowOperator;

void destroyMergeAlignedExternalWindowOperator(void* pOperator) {
  SMergeAlignedExternalWindowOperator* pMAExtW = (SMergeAlignedExternalWindowOperator*)pOperator;
  destroyExternalWindowOperatorInfo(pMAExtW->pExtW);
  taosMemoryFreeClear(pMAExtW);
}

int64_t* extWinExtractTsCol(SSDataBlock* pBlock, int32_t primaryTsIndex, SExecTaskInfo* pTaskInfo) {
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

static int32_t extWinGetCurWinIdx(SExecTaskInfo* pTaskInfo) {
  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  return pInfo->isMultiGroupCalc ? ((SExtWinTrigGrpCtx*)pInfo->curGrpCalc->pRunnerGrpCtx)->pCCtx->curIdx : pInfo->curIdx;
}

static void extWinIncCurWinIdx(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  pInfo->isMultiGroupCalc ? ((SExtWinTrigGrpCtx*)pInfo->curGrpCalc->pRunnerGrpCtx)->pCCtx->curIdx++ : pInfo->curIdx++;
}

static void extWinSetCurWinIdx(SOperatorInfo* pOperator, int32_t idx) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  if (pInfo->isMultiGroupCalc) {
    ((SExtWinTrigGrpCtx*)pInfo->curGrpCalc->pRunnerGrpCtx)->pCCtx->curIdx = idx;
  } else {
    pInfo->curIdx = idx;
  }
}


static void extWinIncCurWinOutIdx(SStreamRuntimeInfo* pStreamRuntimeInfo) {
  if (pStreamRuntimeInfo->funcInfo.isMultiGroupCalc) {
    //((SExtWinTrigGrpCtx*)pStreamRuntimeInfo->funcInfo.curGrpCalc->pRunnerGrpCtx)->pCCtx->curIdx++;
  } else {
    pStreamRuntimeInfo->funcInfo.curOutIdx++;
  }
}


static const STimeWindow* extWinGetNextWin(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo) {
  int32_t curIdx = extWinGetCurWinIdx(pTaskInfo);
  if (curIdx + 1 >= pExtW->pTGrpCtx->pCCtx->pWins->size) return NULL;
  return taosArrayGet(pExtW->pTGrpCtx->pCCtx->pWins, curIdx + 1);
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


static int32_t extWinInitCGrpCtx(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo, SExtWinCalcGrpCtx* pCtx) {
  int32_t code = 0, lino = 0;

  pCtx->lastSKey = INT64_MIN;
  pCtx->lastWinId = -1;
  pCtx->blkWinIdx = -1;
  pCtx->blkWinStartSet = false;
  pCtx->blkRowStartIdx = 0;
  
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
    }
  } else if (pExtW->timeRangeExpr && pExtW->timeRangeExpr->needCalc) {
    TAOS_CHECK_EXIT(scalarCalculateExtWinsTimeRange(pExtW->timeRangeExpr, pInfo, pWin));
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

static int32_t extWinSwitchInitTGrpCtx(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo, SBlockID* pId) {
  int32_t code = 0, lino = 0;
  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;

  if ((!pInfo->isMultiGroupCalc && NULL != pExtW->pTGrpCtx) ||
      (pInfo->isMultiGroupCalc && pId->baseGId == pExtW->lastTGrpId)) {
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

    qDebug("%s %s extWin switch to tgrp %" PRIu64, 
      GET_TASKID(pTaskInfo), EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW), pId->baseGId);

    goto _exit;
  }

  pExtW->pTGrpCtx = taosMemoryCalloc(1, sizeof(SExtWinTrigGrpCtx));
  TSDB_CHECK_NULL(pExtW->pTGrpCtx, code, lino, _exit, terrno);

_exit:

  if (code) {
    qError("%s %s %s extWin failed at line %d since %s", 
      GET_TASKID(pTaskInfo), __func__, EXT_WIN_TYPE_STR(pExtW->isMergeAlignedExtW), lino, tstrerror(code));
  }
  
  return code;
}


static int32_t extWinSwitchInitCGrpCtx(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo, SBlockID* pId) {
  int32_t code = 0, lino = 0;
  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  SExtWinTrigGrpCtx* pTCtx = pExtW->pTGrpCtx;

  if (0 == pId->groupId || (pInfo->isMultiGroupCalc && (pId->baseGId == pId->groupId))) {
    if (NULL != pTCtx->pCGCtxs || pExtW->calcWithPartition) {
      qError("%s plan or ctx conflict, pCGCtxs:%p, calcWithPartition:%d", GET_TASKID(pTaskInfo), pTCtx->pCGCtxs, pExtW->calcWithPartition);
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
  (*pResult)->winIdx = extWinGetCurWinIdx(pOperator->pTaskInfo);
  
  return setResultRowInitCtx((*pResult), pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
}


static int32_t mergeAlignExtWinGetWinFromTs(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, TSKEY ts, STimeWindow** ppWin) {
  int32_t blkWinIdx = extWinGetCurWinIdx(pOperator->pTaskInfo);
  
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
  
  code = mergeAlignExtWinGetWinFromTs(pOperator, pExtW, ts, &pWin);
  if (code) {
    qError("failed to get time window for tgrp %" PRIu64 " ts:%" PRId64 ", prim ts index:%d, error:%s", 
      pExtW->lastTGrpId, ts, pExtW->primaryTsIndex, tstrerror(code));
    TAOS_CHECK_EXIT(code);
  }

  if (pMAExtW->curTs != INT64_MIN && pMAExtW->curTs != pWin->skey) {
    TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, pResultRowInfo, pResultBlock));
    resetResultRow(pMAExtW->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));
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
    resetResultRow(pMAExtW->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));

    TAOS_CHECK_EXIT(mergeAlignExtWinGetWinFromTs(pOperator, pExtW, tsCols[currPos], &pWin));
    
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
    TAOS_CHECK_EXIT(extWinAppendWinIdx(pOperator->pTaskInfo, pExtW->pWinRowIdx, pResult, extWinGetCurWinIdx(pOperator->pTaskInfo), pInput->info.rows - i));

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

void mergeAlignExtWinDo(SOperatorInfo* pOperator) {
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

  while (1) {
    SSDataBlock* pBlock = (pMAExtW->pNewGroup) ? pMAExtW->pNewGroup : getNextBlockFromDownstream(pOperator, 0);

    if (pBlock == NULL) {
      // close last time window
      if (pMAExtW->curTs != INT64_MIN && EEXT_MODE_AGG == pExtW->mode) {
        TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, &pExtW->binfo.resultRowInfo, pRes));
      }
      setOperatorCompleted(pOperator);
      break;
    }

    if (pExtW->lastCGrpId != pBlock->info.id.groupId) {
      if (pMAExtW->curTs != INT64_MIN && EEXT_MODE_AGG == pExtW->mode) {
        TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, &pExtW->binfo.resultRowInfo, pRes));
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
    qDebug("ext windowpExtWAlign->scalarMode:%d", pExtW->mode);

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
  
  size_t rows = pRes->info.rows;
  pOperator->resultInfo.totalRows += rows;
  (*ppRes) = (rows == 0) ? NULL : pRes;

_exit:

  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

int32_t resetMergeAlignedExtWinOperator(SOperatorInfo* pOperator) {
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

  extWinResetResultRows(&pExtW->resultRows);

/*
  int32_t code = resetAggSup(&pOperator->exprSupp, &pExtW->aggSup, pTaskInfo, pPhynode->window.pFuncs, NULL,
                             sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, pTaskInfo->streamInfo.pState,
                             &pTaskInfo->storageAPI.functionStore);
*/                             

  colDataDestroy(&pExtW->twAggSup.timeWindowData);
  TAOS_CHECK_EXIT(initExecTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pTaskInfo->window));

  pExtW->outWinIdx = 0;
  pExtW->lastTGrpId = 0;
  pExtW->lastCGrpId = 0;
  
_exit:

  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
  }
  
  return code;
}

int32_t createMergeAlignedExternalWindowOperator(SOperatorInfo* pDownstream, SPhysiNode* pNode,
                                                 SExecTaskInfo* pTaskInfo, SOperatorInfo** ppOptrOut) {
  SMergeAlignedIntervalPhysiNode* pPhynode = (SMergeAlignedIntervalPhysiNode*)pNode;
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

  pMAExtW->pExtW = taosMemoryCalloc(1, sizeof(SExternalWindowOperator));
  if (!pMAExtW->pExtW) {
    code = terrno;
    goto _error;
  }

  SExternalWindowOperator* pExtW = pMAExtW->pExtW;
  SExprSupp* pSup = &pOperator->exprSupp;
  pSup->hasWindowOrGroup = true;
  pMAExtW->curTs = INT64_MIN;

  pExtW->primaryTsIndex = ((SColumnNode*)pPhynode->window.pTspk)->slotId;
  pExtW->mode = pPhynode->window.pProjs ? EEXT_MODE_SCALAR : EEXT_MODE_AGG;
  pExtW->binfo.inputTsOrder = pPhynode->window.node.inputTsOrder = TSDB_ORDER_ASC;
  pExtW->binfo.outputTsOrder = pExtW->binfo.inputTsOrder;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  int32_t num = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pPhynode->window.pFuncs, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pExtW->mode == EEXT_MODE_AGG) {
    code = initAggSup(pSup, &pExtW->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str, pTaskInfo->streamInfo.pState,
                      &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

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
  pExtW->pTGrpCtx = NULL;

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
  if (!tsCountAlwaysReturnValue) {
    return TSDB_CODE_SUCCESS;
  }

  SExternalWindowOperator* pExtW = pOperator->info;

  if (!pExtW->hasCountFunc) {
    return TSDB_CODE_SUCCESS;
  }

  code = createDataBlock(&pBlock);
  if (code) {
    return code;
  }

  pBlock->info.rows = 1;
  pBlock->info.capacity = 0;

  for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
    SColumnInfoData colInfo = {0};
    colInfo.hasNull = true;
    colInfo.info.type = TSDB_DATA_TYPE_NULL;
    colInfo.info.bytes = 1;

    SExprInfo* pOneExpr = &pOperator->exprSupp.pExprInfo[i];
    for (int32_t j = 0; j < pOneExpr->base.numOfParams; ++j) {
      SFunctParam* pFuncParam = &pOneExpr->base.pParam[j];
      if (pFuncParam->type == FUNC_PARAM_TYPE_COLUMN) {
        int32_t slotId = pFuncParam->pCol->slotId;
        int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
        if (slotId >= numOfCols) {
          code = taosArrayEnsureCap(pBlock->pDataBlock, slotId + 1);
          QUERY_CHECK_CODE(code, lino, _end);

          for (int32_t k = numOfCols; k < slotId + 1; ++k) {
            void* tmp = taosArrayPush(pBlock->pDataBlock, &colInfo);
            QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
          }
        }
      } else if (pFuncParam->type == FUNC_PARAM_TYPE_VALUE) {
        // do nothing
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
    pCCtx->blkWinIdx = extWinGetCurWinIdx(pOperator->pTaskInfo);
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


static int32_t extWinGetWinStartPos(STimeWindow win, const SDataBlockInfo* pBlockInfo, int32_t lastEndPos, int32_t order, int32_t* nextPos, int64_t* tsCol) {
  bool ascQuery = order == TSDB_ORDER_ASC;

  if (win.ekey <= pBlockInfo->window.skey && ascQuery) {
    return -2;
  }
//if (win.skey > pBlockInfo->window.ekey && !ascQuery) return -2;

  if (win.skey > pBlockInfo->window.ekey && ascQuery) return -1;
//if (win.ekey < pBlockInfo->window.skey && !ascQuery) return -1;

  while (true) {
    if (win.ekey <= tsCol[lastEndPos + 1] && ascQuery) return -2;
    if (win.skey <= tsCol[lastEndPos + 1] && ascQuery) break;
    lastEndPos++;
  }

  *nextPos = lastEndPos + 1;
  return 0;
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
      pRows->resRowsSize += EXT_WIN_RES_ROWS_ALLOC_SIZE;
      pRows->pResultRows = taosMemoryRealloc(pRows->pResultRows, pRows->resRowsSize * POINTER_BYTES);
      TSDB_CHECK_NULL(pRows->pResultRows, code, lino, _exit, terrno);    
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
    TAOS_CHECK_EXIT(extWinGetResultRow(pTaskInfo, pExtW, win->resWinIdx, pAggSup->resultRowSize, &pResultRow));
  } else {
    win->resWinIdx = pExtW->resWinIdx++;
    
    qDebug("set window [%" PRId64 ", %" PRId64 "] outIdx:%d", win->tw.skey, win->tw.ekey, win->resWinIdx);

    TAOS_CHECK_EXIT(extWinGetResultRow(pTaskInfo, pExtW, win->resWinIdx, pAggSup->resultRowSize, &pResultRow));
    
    memset(pResultRow, 0, pAggSup->resultRowSize);

    pResultRow->winIdx = extWinGetCurWinIdx(pOperator->pTaskInfo);
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
  if (forwardRows == 0) return 0;
  SExprSupp*               pSup = &pOperator->exprSupp;
  SExternalWindowOperator* pExtW = pOperator->info;
  return applyAggFunctionOnPartialTuples(pOperator->pTaskInfo, pSup->pCtx, &pExtW->twAggSup.timeWindowData, startPos,
                                         forwardRows, pInputBlock->info.rows, pSup->numOfExprs);

}

static bool extWinLastWinClosed(SExternalWindowOperator* pExtW) {
  if (pExtW->resWinIdx <= 0 || (pExtW->multiTableMode && !pExtW->inputHasOrder)) {
    return false;
  }

  if (NULL == pExtW->timeRangeExpr || !pExtW->timeRangeExpr->needCalc) {
    return true;
  }

  SList* pList = taosArrayGetP(pExtW->pOutputBlocks, pExtW->pTGrpCtx->pCCtx->lastWinIdx);
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

static int32_t extWinGetWinResBlock(SOperatorInfo* pOperator, int32_t rows, SExtWinTimeWindow* pWin, SSDataBlock** ppRes, SArray** ppIdx) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SList*                   pList = NULL;
  int32_t                  code = TSDB_CODE_SUCCESS, lino = 0;
  
  if (pWin->resWinIdx >= 0) {
    pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->resWinIdx);
  } else {
    if (extWinLastWinClosed(pExtW)) {
      pWin->resWinIdx = pExtW->pTGrpCtx->pCCtx->lastWinIdx;
      pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->resWinIdx);
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
  TAOS_CHECK_EXIT(projectApplyFunctionsWithSelect(pExprSup->pExprInfo, pResBlock, pExtW->pTmpBlock, pExprSup->pCtx, pExprSup->numOfExprs,
        NULL, GET_STM_RTINFO(pOperator->pTaskInfo), true, pExprSup->hasIndefRowsFunc));

  TAOS_CHECK_EXIT(extWinAppendWinIdx(pOperator->pTaskInfo, pIdx, pResBlock, extWinGetCurWinIdx(pOperator->pTaskInfo), rows));

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
    pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->resWinIdx);
    TAOS_CHECK_EXIT(extWinIndefRowsSetWinOutputBuf(pExtW, pWin, &pOperator->exprSupp, &pExtW->aggSup, pOperator->pTaskInfo, false));
  } else {
    if (extWinLastWinClosed(pExtW)) {
      pWin->resWinIdx = pExtW->pTGrpCtx->pCCtx->lastWinIdx;
      pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->resWinIdx);
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

  TAOS_CHECK_EXIT(extWinAppendWinIdx(pOperator->pTaskInfo, pIdx, pResBlock, extWinGetCurWinIdx(pOperator->pTaskInfo), rows));

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

  for (; pCCtx->outWinIdx < numOfWin && pCCtx->outWinLastIdx < pCCtx->lastWinIdx; pCCtx->outWinIdx += 1, extWinIncCurWinOutIdx(pOperator->pTaskInfo->pStreamRuntimeInfo)) {
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
      extWinIncCurWinOutIdx(pOperator->pTaskInfo->pStreamRuntimeInfo);
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
  if (0 == pBlock->info.rows) {
    pStream->curGrpCalc = tSimpleHashIterate(pStream->pGroupCalcInfos, pStream->curGrpCalc, &pExtW->lastOutputIter);
    while (pStream->curGrpCalc != NULL) {
      if (pStream->curGrpCalc->pRunnerGrpCtx) {
        pExtW->lastTGrpId = *(uint64_t*)tSimpleHashGetKey(pStream->curGrpCalc, NULL);
        pExtW->pTGrpCtx = pStream->curGrpCalc->pRunnerGrpCtx;
        
        TAOS_CHECK_EXIT(extWinNonAggOutputSingleGrpRes(pOperator, pExtW, &pBlock));
        if (pBlock->info.rows > 0) {
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
    for (; pExtW->outWinIdx < numOfWin; pExtW->outWinIdx++, extWinIncCurWinOutIdx(pOperator->pTaskInfo->pStreamRuntimeInfo)) {
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
        extWinIncCurWinOutIdx(pOperator->pTaskInfo->pStreamRuntimeInfo);
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
  int32_t currIdx = extWinGetCurWinIdx(pOperator->pTaskInfo);

  if (NULL == pExtW->pEmptyInputBlock || (pWin && pWin->tw.skey == pExtW->pTGrpCtx->pCCtx->lastSKey)) {
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
      TAOS_CHECK_EXIT(setInputDataBlock(pSup, pBlock, pExtW->binfo.inputTsOrder, MAIN_SCAN, true));
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

    if (pWin->tw.skey != pExtW->pTGrpCtx->pCCtx->lastSKey) {
      TAOS_CHECK_EXIT(extWinAggSetWinOutputBuf(pOperator, pWin, &pOperator->exprSupp, &pExtW->aggSup, pOperator->pTaskInfo));
    }
    
    updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pWin->tw, 1);
    TAOS_CHECK_EXIT(extWinAggDo(pOperator, startPos, winRows, pInputBlock));
    
    pExtW->pTGrpCtx->pCCtx->lastSKey = pWin->tw.skey;
    pExtW->pTGrpCtx->pCCtx->lastWinId = extWinGetCurWinIdx(pOperator->pTaskInfo);
    startPos += winRows;
  }

_exit:

  if (code) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinAggOutputSingleCGrpRes(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, bool* grpDone) {
  SExprInfo*      pExprInfo = pOperator->exprSupp.pExprInfo;
  int32_t         numOfExprs = pOperator->exprSupp.numOfExprs;
  int32_t*        rowEntryOffset = pOperator->exprSupp.rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;
  SSDataBlock*    pBlock = pExtW->binfo.pRes;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SExtWinTrigGrpCtx* pTGrpCtx = pExtW->pTGrpCtx;
  SExtWinCalcGrpCtx* pCCtx = pTGrpCtx->pCCtx;
  int32_t         numOfWin = taosArrayGetSize(pCCtx->pWins);
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;

  for (; pCCtx->outWinIdx < numOfWin && pCCtx->outWinNum < pCCtx->outWinTotalNum; pCCtx->outWinIdx += 1) {
    SExtWinTimeWindow* pWin = TARRAY_GET_ELEM(pCCtx->pWins, pCCtx->outWinIdx);
    if (pWin->resWinIdx < 0) {
      continue;
    }

    pCCtx->outWinNum++;
    SResultRow* pRow = (SResultRow*)((char*)pExtW->resultRows.pResultRows[pWin->resWinIdx / pExtW->resultRows.resRowSize] + (pWin->resWinIdx % pExtW->resultRows.resRowSize) * pExtW->aggSup.resultRowSize);
    
    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);

    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      continue;
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      uint32_t newSize = pBlock->info.rows + pRow->numOfRows + pCCtx->outWinTotalNum - pCCtx->outWinNum;
      TAOS_CHECK_EXIT(blockDataEnsureCapacity(pBlock, newSize));
      qDebug("datablock capacity not sufficient, expand to required:%d, current capacity:%d, %s", newSize,
             pBlock->info.capacity, GET_TASKID(pTaskInfo));
    }

    TAOS_CHECK_EXIT(copyResultrowToDataBlock(pExprInfo, numOfExprs, pRow, pCtx, pBlock, rowEntryOffset, pTaskInfo));

    pBlock->info.rows += pRow->numOfRows;

    TAOS_CHECK_EXIT(extWinAppendWinIdx(pOperator->pTaskInfo, pExtW->pWinRowIdx, pBlock, pRow->winIdx, pRow->numOfRows));

    if (pBlock->info.rows >= pOperator->resultInfo.threshold) {
      break;
    }
  }

  if (grpDone) {
    *grpDone = pCCtx->outWinIdx >= numOfWin || pCCtx->outWinNum >= pCCtx->outWinTotalNum;
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

  if (pTGrpCtx->pCCtx) {
    TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, NULL));
  }
  if (0 == pBlock->info.rows) {
    pTGrpCtx->pCCtx = tSimpleHashIterate(pTGrpCtx->pCGCtxs, pTGrpCtx->pCCtx, &pTGrpCtx->lastCtxIter);
    while (pTGrpCtx->pCCtx != NULL) {
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
    TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, NULL));
  }
  if (0 == pBlock->info.rows) {
    pStream->curGrpCalc = tSimpleHashIterate(pStream->pGroupCalcInfos, pStream->curGrpCalc, &pExtW->lastOutputIter);
    while (pStream->curGrpCalc != NULL) {
      if (pStream->curGrpCalc->pRunnerGrpCtx) {
        pExtW->lastTGrpId = *(uint64_t*)tSimpleHashGetKey(pStream->curGrpCalc, NULL);
        pExtW->pTGrpCtx = pStream->curGrpCalc->pRunnerGrpCtx;

        if (pExtW->calcWithPartition) {
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
  SStreamRuntimeFuncInfo* pStream = &pTaskInfo->pStreamRuntimeInfo->funcInfo;

  pBlock->info.version = pTaskInfo->version;
  blockDataCleanup(pBlock);
  taosArrayClear(pExtW->pWinRowIdx);

  if (pExtW->needGroupSort) {
    if (pStream->isMultiGroupCalc) {
      if (pExtW->calcWithPartition) {
        TAOS_CHECK_EXIT(extWinAggOutputMulOrderTCGrpsRes(pOperator, pExtW));
      } else {
        TAOS_CHECK_EXIT(extWinAggOutputMulOrderTGrpsRes(pOperator, pExtW));
      }
    } else {
      TAOS_CHECK_EXIT(extWinAggOutputMulOrderCGrpsRes(pOperator, pExtW));
    }
  } else {
    if (pStream->isMultiGroupCalc) {
      TAOS_CHECK_EXIT(extWinAggOutputMulNoOrderTGrpsRes(pOperator, pExtW));
    } else if (pExtW->calcWithPartition) {
      TAOS_CHECK_EXIT(extWinAggOutputMulNoOrderCGrpsRes(pOperator, pExtW));
    } else {
      TAOS_CHECK_EXIT(extWinAggOutputSingleCGrpRes(pOperator, pExtW, NULL));
    }
  }
  
  pBlock->info.dataLoad = 1;
  extWinAssignBlockGrpId(pOperator, pExtW, &pBlock->info.id);

  qDebug("%s result generated, rows:%" PRId64 ", cGrpId:%" PRIu64 " baseGid:%" PRIu64, 
    GET_TASKID(pTaskInfo), pBlock->info.rows, pBlock->info.id.groupId, pBlock->info.id.baseGId);

  *ppRes = (pBlock->info.rows > 0) ? pBlock : NULL;
  pStream->pStreamBlkWinIdx = pExtW->pWinRowIdx;

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static void extWinFreeResultRow(SExternalWindowOperator* pExtW) {
  if (pExtW->resultRows.resRowAllcNum * pExtW->aggSup.resultRowSize >= 1048576) {
    int32_t i = 1;
    while (i < pExtW->resultRows.resRowSize && pExtW->resultRows.pResultRows[i]) {
      taosMemoryFreeClear(pExtW->resultRows.pResultRows[i]);
      pExtW->resultRows.resRowAllcNum -= pExtW->resultRows.resRowSize;
      i++;
    }
  }
  
  if (pExtW->binfo.pRes && pExtW->binfo.pRes->info.rows * pExtW->aggSup.resultRowSize >= 1048576) {
    blockDataFreeCols(pExtW->binfo.pRes);
  }
}

static bool extWinNonAggGotResBlock(SExternalWindowOperator* pExtW) {
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


static void extWinEndClearCtxs(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo) {
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
  SStreamRuntimeFuncInfo* pStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;

  pStream->curGrpCalc = NULL;
  pStream->curGrpRead = NULL;
  
  pExtW->lastGrpIdx = 0;

  if (pExtW->needGroupSort) {
    if (pStream->isMultiGroupCalc && pExtW->calcWithPartition) {
      taosArraySort(pExtW->pCTGrpIds, extWinGrpIdCompare);
    } else {
      taosArraySort(pExtW->pGrpIds, extWinGrpIdCompare);
    }
  }
    
  if ((!pStream->isMultiGroupCalc) && pExtW->calcWithPartition && pExtW->pTGrpCtx) {
    pExtW->pTGrpCtx->pCCtx = NULL;
  }
}

static int32_t extWinOpen(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }
  
  int32_t                  code = 0;
  int32_t                  lino = 0;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SOperatorInfo*           pDownstream = pOperator->pDownstream[0];
  SExternalWindowOperator* pExtW = pOperator->info;
  SExprSupp*               pSup = &pOperator->exprSupp;
  SStreamRuntimeFuncInfo*  pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      if (EEXT_MODE_AGG == pExtW->mode) {
        TAOS_CHECK_EXIT(pInfo->isMultiGroupCalc ? extWinAggHandleMultiTGrpEmptyWins(pOperator, pExtW) : extWinAggHandleEmptyWins(pOperator, pBlock, true, NULL));
      }
      
      break;
    }

    printDataBlock(pBlock, __func__, pTaskInfo->id.str, pTaskInfo->id.queryId);

    qInfo("%s ext window mode:%s baseGrp:%" PRIu64 " grp:%" PRIu64 " got %" PRId64 " rows from downstream", 
        GET_TASKID(pTaskInfo), extWinModeStr(pExtW->mode), pBlock->info.id.baseGId, pBlock->info.id.groupId, pBlock->info.rows);
    
    TAOS_CHECK_EXIT(extWinSwitchInitCtxs(pExtW, pTaskInfo, &pBlock->info.id));

    switch (pExtW->mode) {
      case EEXT_MODE_SCALAR:
        TAOS_CHECK_EXIT(extWinProjectOpen(pOperator, pBlock));
        if (extWinNonAggGotResBlock(pExtW)) {
          goto _exit;
        }
        break;
      case EEXT_MODE_AGG:
        TAOS_CHECK_EXIT(extWinAggOpen(pOperator, pBlock));
        break;
      case EEXT_MODE_INDEFR_FUNC:
        TAOS_CHECK_EXIT(extWinIndefRowsOpen(pOperator, pBlock));
        if (extWinNonAggGotResBlock(pExtW)) {
          goto _exit;
        }
        break;
      default:
        break;
    }
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

  if (pOperator->status == OP_EXEC_DONE) {
    *ppRes = NULL;
    return code;
  }

  extWinRecycleBlkNode(pExtW, &pExtW->pLastBlkNode);

  TAOS_CHECK_EXIT(pOperator->fpSet._openFn(pOperator));

  if (pExtW->mode == EEXT_MODE_SCALAR || pExtW->mode == EEXT_MODE_INDEFR_FUNC) {
    TAOS_CHECK_EXIT(extWinNonAggOutputRes(pOperator, ppRes));
    if (NULL == *ppRes) {
      setOperatorCompleted(pOperator);
      extWinFreeResultRow(pExtW);
    }
  } else {
    TAOS_CHECK_EXIT(extWinAggOutputRes(pOperator, ppRes));
    if (NULL == *ppRes || (*ppRes)->info.rows <= 0) {
      setOperatorCompleted(pOperator);
      extWinFreeResultRow(pExtW);
    }
  }

  if (*ppRes && (*ppRes)->info.rows > 0) {
    qDebug("%s tgrp %" PRIu64 " cgrp %" PRIu64 " ext window return block with %" PRId64 " rows", 
      GET_TASKID(pTaskInfo), pExtW->lastTGrpId, pExtW->lastCGrpId, (*ppRes)->info.rows);
        
    pOperator->resultInfo.totalRows += (*ppRes)->info.rows;
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

int32_t createExternalWindowOperator(SOperatorInfo* pDownstream, SPhysiNode* pNode, SExecTaskInfo* pTaskInfo,
                                     SOperatorInfo** pOptrOut) {
  SExternalWindowPhysiNode* pPhynode = (SExternalWindowPhysiNode*)pNode;
  QRY_PARAM_CHECK(pOptrOut);
  int32_t                  code = 0;
  int32_t                  lino = 0;
  SExternalWindowOperator* pExtW = taosMemoryCalloc(1, sizeof(SExternalWindowOperator));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  pOperator->pPhyNode = pNode;
  if (!pExtW || !pOperator) {
    code = terrno;
    lino = __LINE__;
    goto _error;
  }

  pExtW->needGroupSort = pPhynode->needGroupSort;
  pExtW->calcWithPartition = pPhynode->calcWithPartition;
  pExtW->extWinSplit = pPhynode->extWinSplit;
  
  setOperatorInfo(pOperator, "ExternalWindowOperator", QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW, true, OP_NOT_OPENED,
                  pExtW, pTaskInfo);
                  
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhynode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pExtW->binfo, pResBlock);

  pExtW->primaryTsIndex = ((SColumnNode*)pPhynode->window.pTspk)->slotId;
  pExtW->mode = pPhynode->window.pProjs ? EEXT_MODE_SCALAR : (pPhynode->window.indefRowsFunc ? EEXT_MODE_INDEFR_FUNC : EEXT_MODE_AGG);
  pExtW->binfo.inputTsOrder = pPhynode->window.node.inputTsOrder = TSDB_ORDER_ASC;
  pExtW->binfo.outputTsOrder = pExtW->binfo.inputTsOrder;

  if (pTaskInfo->pStreamRuntimeInfo != NULL){
    pTaskInfo->pStreamRuntimeInfo->funcInfo.withExternalWindow = true;
  }
  
  // pExtW->limitInfo = (SLimitInfo){0};
  // initLimitInfo(pPhynode->window.node.pLimit, pPhynode->window.node.pSlimit, &pExtW->limitInfo);

  if (pPhynode->window.pProjs) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pPhynode->window.pProjs, NULL, &pScalarExprInfo, &numOfScalarExpr);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pExtW->scalarSupp, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);

  //if (pExtW->multiTableMode) {
    pExtW->pOutputBlocks = taosArrayInit(STREAM_CALC_REQ_MAX_WIN_NUM, POINTER_BYTES);
    if (!pExtW->pOutputBlocks) QUERY_CHECK_CODE(terrno, lino, _error);
  //}
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
    code = initAggSup(&pOperator->exprSupp, &pExtW->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str, 0, 0);
    QUERY_CHECK_CODE(code, lino, _error);

    nodesWalkExprs(pPhynode->window.pFuncs, extWinHasCountLikeFunc, &pExtW->hasCountFunc);
    if (pExtW->hasCountFunc) {
      code = extWinCreateEmptyInputBlock(pOperator, &pExtW->pEmptyInputBlock);
      QUERY_CHECK_CODE(code, lino, _error);
      qDebug("%s ext window has CountLikeFunc", pOperator->pTaskInfo->id.str);
    } else {
      qDebug("%s ext window doesn't have CountLikeFunc", pOperator->pTaskInfo->id.str);
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
                              pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
    TSDB_CHECK_CODE(code, lino, _error);
    pOperator->exprSupp.hasWindowOrGroup = false;
    
    //code = setFunctionResultOutput(pOperator, &pExtW->binfo, &pExtW->aggSup, MAIN_SCAN, numOfExpr);
    //TSDB_CHECK_CODE(code, lino, _error);
    
    code = filterInitFromNode((SNode*)pNode->pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                              pTaskInfo->pStreamRuntimeInfo);
    TSDB_CHECK_CODE(code, lino, _error);
    
    pExtW->binfo.inputTsOrder = pNode->inputTsOrder;
    pExtW->binfo.outputTsOrder = pNode->outputTsOrder;
    code = setRowTsColumnOutputInfo(pOperator->exprSupp.pCtx, numOfExpr, &pExtW->pPseudoColInfo);
    TSDB_CHECK_CODE(code, lino, _error);

  //if (pExtW->multiTableMode) {
    pExtW->pOutputBlocks = taosArrayInit(STREAM_CALC_REQ_MAX_WIN_NUM, POINTER_BYTES);
    if (!pExtW->pOutputBlocks) QUERY_CHECK_CODE(terrno, lino, _error);
  //}
    pExtW->pFreeBlocks = tdListNew(POINTER_BYTES * 2);
    QUERY_CHECK_NULL(pExtW->pFreeBlocks, code, lino, _error, terrno);  
  }

  pExtW->timeRangeExpr = (STimeRangeNode*)pPhynode->pTimeRange;
  if (pExtW->timeRangeExpr) {
    QUERY_CHECK_NULL(pExtW->timeRangeExpr->pStart, code, lino, _error, TSDB_CODE_STREAM_INTERNAL_ERROR);
    QUERY_CHECK_NULL(pExtW->timeRangeExpr->pEnd, code, lino, _error, TSDB_CODE_STREAM_INTERNAL_ERROR);
  }

  if (pPhynode->isSingleTable) {
    pExtW->getWinFp = (pExtW->timeRangeExpr && (pExtW->timeRangeExpr->needCalc || (pTaskInfo->pStreamRuntimeInfo->funcInfo.addOptions & CALC_SLIDING_OVERLAP))) ? extWinGetOvlpWin : extWinGetNoOvlpWin;
    pExtW->multiTableMode = false;
  } else {
    pExtW->getWinFp = (pExtW->timeRangeExpr && (pExtW->timeRangeExpr->needCalc || (pTaskInfo->pStreamRuntimeInfo->funcInfo.addOptions & CALC_SLIDING_OVERLAP))) ? extWinGetMultiTbOvlpWin : extWinGetMultiTbNoOvlpWin;
    pExtW->multiTableMode = true;
  }
  pExtW->inputHasOrder = pPhynode->inputHasOrder;

  code = extWinInitResRows(pExtW, pTaskInfo);
  TSDB_CHECK_CODE(code, lino, _error);

  pOperator->fpSet = createOperatorFpSet(extWinOpen, extWinNext, NULL, destroyExternalWindowOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetExternalWindowOperator);
  code = appendDownstream(pOperator, &pDownstream, 1);
  if (code != 0) {
    goto _error;
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


