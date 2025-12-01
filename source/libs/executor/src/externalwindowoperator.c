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

typedef struct SExternalWindowOperator {
  SOptrBasicInfo     binfo;
  SExprSupp          scalarSupp;
  int32_t            primaryTsIndex;
  EExtWinMode        mode;
  bool               multiTableMode;
  bool               inputHasOrder;
  SArray*            pWins;           // SArray<SExtWinTimeWindow>
  SArray*            pPseudoColInfo;  
  STimeRangeNode*    timeRangeExpr;
  
  extWinGetWinFp     getWinFp;

  bool               blkWinStartSet;
  int32_t            blkWinStartIdx;
  int32_t            blkWinIdx;
  int32_t            blkRowStartIdx;
  int32_t            outputWinId;
  int32_t            outputWinNum;
  int32_t            outWinIdx;

  // for project&indefRows
  SList*             pFreeBlocks;    // SList<SSDatablock*+SAarray*>
  SArray*            pOutputBlocks;  // SArray<SList*>, for each window, we have a list of blocks
  SListNode*         pLastBlkNode; 
  SSDataBlock*       pTmpBlock;
  
  // for agg
  SAggSupporter      aggSup;
  STimeWindowAggSupp twAggSup;

  int32_t            resultRowCapacity;
  SResultRow*        pResultRow;

  int64_t            lastSKey;
  int32_t            lastWinId;
  SSDataBlock*       pEmptyInputBlock;
  bool               hasCountFunc;
  SExtWindowStat     stat;
  SArray*            pWinRowIdx;
} SExternalWindowOperator;


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
  
_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(pRes);
  }
  
  return code;
}

static int32_t extWinGetLastBlockFromList(SExternalWindowOperator* pExtW, SList* pList, int32_t rows, SSDataBlock** ppBlock, SArray** ppIdx) {
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
  taosArrayDestroy(pInfo->pWins);
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
  taosMemoryFreeClear(pInfo->pResultRow);

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
} SMergeAlignedExternalWindowOperator;

void destroyMergeAlignedExternalWindowOperator(void* pOperator) {
  SMergeAlignedExternalWindowOperator* pMlExtInfo = (SMergeAlignedExternalWindowOperator*)pOperator;
  destroyExternalWindowOperatorInfo(pMlExtInfo->pExtW);
  taosMemoryFreeClear(pMlExtInfo);
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
  return pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx;
}

static void extWinIncCurWinIdx(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx++;
}

static void extWinSetCurWinIdx(SOperatorInfo* pOperator, int32_t idx) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx = idx;
}


static void extWinIncCurWinOutIdx(SStreamRuntimeInfo* pStreamRuntimeInfo) {
  pStreamRuntimeInfo->funcInfo.curOutIdx++;
}


static const STimeWindow* extWinGetNextWin(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo) {
  int32_t curIdx = extWinGetCurWinIdx(pTaskInfo);
  if (curIdx + 1 >= pExtW->pWins->size) return NULL;
  return taosArrayGet(pExtW->pWins, curIdx + 1);
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
  for (int32_t i = blkWinIdx; i < pExtW->pWins->size; ++i) {
    STimeWindow* pWin = taosArrayGet(pExtW->pWins, i);
    if (ts == pWin->skey) {
      extWinSetCurWinIdx(pOperator, i);
      *ppWin = pWin;
      return TSDB_CODE_SUCCESS;
    } else if (ts < pWin->skey) {
      qError("invalid ts %" PRId64 " for current window idx %d skey %" PRId64, ts, i, pWin->skey);
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
  }
  
  qError("invalid ts %" PRId64 " to find merge aligned ext window, size:%d", ts, (int32_t)pExtW->pWins->size);
  return TSDB_CODE_STREAM_INTERNAL_ERROR;
}

static int32_t mergeAlignExtWinFinalizeResult(SOperatorInfo* pOperator, SResultRowInfo* pResultRowInfo, SSDataBlock* pResultBlock) {
  int32_t        code = 0, lino = 0;
  SMergeAlignedExternalWindowOperator* pMlExtInfo = pOperator->info;
  SExternalWindowOperator*             pExtW = pMlExtInfo->pExtW;
  SExprSupp*     pSup = &pOperator->exprSupp;
  SResultRow*  pResultRow = pMlExtInfo->pResultRow;
  
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
  SMergeAlignedExternalWindowOperator* pMlExtInfo = pOperator->info;
  SExternalWindowOperator*             pExtW = pMlExtInfo->pExtW;

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*     pSup = &pOperator->exprSupp;
  int32_t        code = 0, lino = 0;
  STimeWindow *pWin = NULL;

  int32_t startPos = 0;
  int64_t* tsCols = extWinExtractTsCol(pBlock, pExtW->primaryTsIndex, pTaskInfo);
  TSKEY ts = getStartTsKey(&pBlock->info.window, tsCols);
  
  code = mergeAlignExtWinGetWinFromTs(pOperator, pExtW, ts, &pWin);
  if (code) {
    qError("failed to get time window for ts:%" PRId64 ", prim ts index:%d, error:%s", ts, pExtW->primaryTsIndex, tstrerror(code));
    TAOS_CHECK_EXIT(code);
  }

  if (pMlExtInfo->curTs != INT64_MIN && pMlExtInfo->curTs != pWin->skey) {
    TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, pResultRowInfo, pResultBlock));
    resetResultRow(pMlExtInfo->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));
  }
  
  TAOS_CHECK_EXIT(mergeAlignExtWinSetOutputBuf(pOperator, pResultRowInfo, pWin, &pMlExtInfo->pResultRow, pSup, &pExtW->aggSup));

  int32_t currPos = startPos;
  pMlExtInfo->curTs = pWin->skey;
  
  while (++currPos < pBlock->info.rows) {
    if (tsCols[currPos] == pMlExtInfo->curTs) continue;

    qDebug("current ts:%" PRId64 ", startPos:%d, currPos:%d, tsCols[currPos]:%" PRId64,
      pMlExtInfo->curTs, startPos, currPos, tsCols[currPos]); 
    TAOS_CHECK_EXIT(applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pExtW->twAggSup.timeWindowData, startPos,
                                           currPos - startPos, pBlock->info.rows, pSup->numOfExprs));

    TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, pResultRowInfo, pResultBlock));
    resetResultRow(pMlExtInfo->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));

    TAOS_CHECK_EXIT(mergeAlignExtWinGetWinFromTs(pOperator, pExtW, tsCols[currPos], &pWin));
    
    qDebug("ext window align2 start:%" PRId64 ", end:%" PRId64, pWin->skey, pWin->ekey);
    startPos = currPos;
    
    TAOS_CHECK_EXIT(mergeAlignExtWinSetOutputBuf(pOperator, pResultRowInfo, pWin, &pMlExtInfo->pResultRow, pSup, &pExtW->aggSup));

    pMlExtInfo->curTs = pWin->skey;
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
  SMergeAlignedExternalWindowOperator* pMlExtInfo = pOperator->info;
  SExternalWindowOperator*             pExtW = pMlExtInfo->pExtW;
  SResultRow*                          pResultRow = NULL;
  int32_t                              code = 0;
  SSDataBlock*                         pRes = pExtW->binfo.pRes;
  SExprSupp*                           pSup = &pOperator->exprSupp;
  int32_t                              lino = 0;

  taosArrayClear(pExtW->pWinRowIdx);
  blockDataCleanup(pRes);

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);

    if (pBlock == NULL) {
      // close last time window
      if (pMlExtInfo->curTs != INT64_MIN && EEXT_MODE_AGG == pExtW->mode) {
        TAOS_CHECK_EXIT(mergeAlignExtWinFinalizeResult(pOperator, &pExtW->binfo.resultRowInfo, pRes));
      }
      setOperatorCompleted(pOperator);
      break;
    }

    pRes->info.scanFlag = pBlock->info.scanFlag;
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

  pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx = pExtW->pWinRowIdx;
  
_exit:

  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static int32_t mergeAlignExtWinNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SExecTaskInfo*                       pTaskInfo = pOperator->pTaskInfo;
  SMergeAlignedExternalWindowOperator* pMlExtInfo = pOperator->info;
  SExternalWindowOperator*             pExtW = pMlExtInfo->pExtW;
  int32_t                              code = 0;
  int32_t lino = 0;

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SSDataBlock* pRes = pExtW->binfo.pRes;
  blockDataCleanup(pRes);

  if (taosArrayGetSize(pExtW->pWins) <= 0) {
    size_t size = taosArrayGetSize(pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamPesudoFuncVals);
    STimeWindow* pWin = taosArrayReserve(pExtW->pWins, size);
    TSDB_CHECK_NULL(pWin, code, lino, _exit, terrno);

    for (int32_t i = 0; i < size; ++i) {
      SSTriggerCalcParam* pParam = taosArrayGet(pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamPesudoFuncVals, i);
      pWin[i].skey = pParam->wstart;
      pWin[i].ekey = pParam->wstart + 1;
    }
    
    pExtW->outputWinId = pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx;
  }

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
  SMergeAlignedExternalWindowOperator* pMlExtInfo = pOperator->info;
  SExternalWindowOperator*             pExtW = pMlExtInfo->pExtW;
  SExecTaskInfo*                       pTaskInfo = pOperator->pTaskInfo;
  SMergeAlignedIntervalPhysiNode * pPhynode = (SMergeAlignedIntervalPhysiNode*)pOperator->pPhyNode;
  pOperator->status = OP_NOT_OPENED;

  taosArrayClear(pExtW->pWins);

  resetBasicOperatorState(&pExtW->binfo);
  pMlExtInfo->pResultRow = NULL;
  pMlExtInfo->curTs = INT64_MIN;

  int32_t code = resetAggSup(&pOperator->exprSupp, &pExtW->aggSup, pTaskInfo, pPhynode->window.pFuncs, NULL,
                             sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, pTaskInfo->streamInfo.pState,
                             &pTaskInfo->storageAPI.functionStore);
  if (code == 0) {
    colDataDestroy(&pExtW->twAggSup.timeWindowData);
    code = initExecTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pTaskInfo->window);
  }
  return code;
}

int32_t createMergeAlignedExternalWindowOperator(SOperatorInfo* pDownstream, SPhysiNode* pNode,
                                                 SExecTaskInfo* pTaskInfo, SOperatorInfo** ppOptrOut) {
  SMergeAlignedIntervalPhysiNode* pPhynode = (SMergeAlignedIntervalPhysiNode*)pNode;
  int32_t code = 0;
  int32_t lino = 0;
  SMergeAlignedExternalWindowOperator* pMlExtInfo = taosMemoryCalloc(1, sizeof(SMergeAlignedExternalWindowOperator));
  SOperatorInfo*                       pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  if (pTaskInfo->pStreamRuntimeInfo != NULL){
    pTaskInfo->pStreamRuntimeInfo->funcInfo.withExternalWindow = true;
  }
  pOperator->pPhyNode = pNode;
  if (!pMlExtInfo || !pOperator) {
    code = terrno;
    goto _error;
  }

  pMlExtInfo->pExtW = taosMemoryCalloc(1, sizeof(SExternalWindowOperator));
  if (!pMlExtInfo->pExtW) {
    code = terrno;
    goto _error;
  }

  SExternalWindowOperator* pExtW = pMlExtInfo->pExtW;
  SExprSupp* pSup = &pOperator->exprSupp;
  pSup->hasWindowOrGroup = true;
  pSup->hasWindow = true;
  pMlExtInfo->curTs = INT64_MIN;

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

  pExtW->pWins = taosArrayInit(4096, sizeof(STimeWindow));
  if (!pExtW->pWins) QUERY_CHECK_CODE(terrno, lino, _error);

  pExtW->pWinRowIdx = taosArrayInit(4096, sizeof(int64_t));
  TSDB_CHECK_NULL(pExtW->pWinRowIdx, code, lino, _error, terrno);

  initResultRowInfo(&pExtW->binfo.resultRowInfo);
  code = blockDataEnsureCapacity(pExtW->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);
  setOperatorInfo(pOperator, "MergeAlignedExternalWindowOperator", QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW, false, OP_NOT_OPENED, pMlExtInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, mergeAlignExtWinNext, NULL,
                                         destroyMergeAlignedExternalWindowOperator, optrDefaultBufFn, NULL,
                                         optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetMergeAlignedExtWinOperator);

  code = appendDownstream(pOperator, &pDownstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);
  *ppOptrOut = pOperator;
  return code;
  
_error:
  if (pMlExtInfo) destroyMergeAlignedExternalWindowOperator(pMlExtInfo);
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

  pExtW->outputWinId = 0;
  pExtW->lastWinId = -1;
  pExtW->outputWinNum = 0;
  taosArrayClear(pExtW->pWins);
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

  pExtW->outWinIdx = 0;
  pExtW->lastSKey = INT64_MIN;

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


static int32_t extWinGetMultiTbWinFromTs(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, int64_t* tsCol, int64_t rowNum, int32_t* startPos) {
  int32_t idx = taosArraySearchIdx(pExtW->pWins, tsCol, extWinTsWinCompare, TD_EQ);
  if (idx >= 0) {
    *startPos = 0;
    return idx;
  }

  SExtWinTimeWindow* pWin = NULL;
  int32_t w = 0;
  for (int64_t i = 1; i < rowNum; ++i) {
    for (; w < pExtW->pWins->size; ++w) {
      pWin = TARRAY_GET_ELEM(pExtW->pWins, w);
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
  
  if (pExtW->blkWinIdx < 0) {
    pExtW->blkWinIdx = extWinGetCurWinIdx(pOperator->pTaskInfo);
  } else {
    pExtW->blkWinIdx++;
  }

  if (pExtW->blkWinIdx >= pExtW->pWins->size) {
    qDebug("%s %s ext win blk idx %d reach the end, size: %d, skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, (int32_t)pExtW->pWins->size);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }
  
  SExtWinTimeWindow* pWin = taosArrayGet(pExtW->pWins, pExtW->blkWinIdx);
  if (tsCol[pInfo->rows - 1] < pWin->tw.skey) {
    qDebug("%s %s block end ts %" PRId64 " is small than curr win %d skey %" PRId64 ", skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[pInfo->rows - 1], pExtW->blkWinIdx, pWin->tw.skey);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t r = *startPos;

  qDebug("%s %s start to get novlp win from winIdx %d rowIdx %d", GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, r);

  // TODO handle desc order
  for (; pExtW->blkWinIdx < pExtW->pWins->size; ++pExtW->blkWinIdx) {
    pWin = taosArrayGet(pExtW->pWins, pExtW->blkWinIdx);
    for (; r < pInfo->rows; ++r) {
      if (tsCol[r] < pWin->tw.skey) {
        continue;
      }

      if (tsCol[r] < pWin->tw.ekey) {
        extWinSetCurWinIdx(pOperator, pExtW->blkWinIdx);
        *ppWin = pWin;
        *startPos = r;
        *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, r, pWin->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);

        qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
            GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, pWin->tw.skey, pWin->tw.ekey, *winRows, r, tsCol[r], tsCol[r + *winRows - 1]);
        
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
  if (pExtW->blkWinIdx < 0) {
    pExtW->blkWinIdx = pExtW->blkWinStartIdx;
  } else {
    pExtW->blkWinIdx++;
  }

  if (pExtW->blkWinIdx >= pExtW->pWins->size) {
    qDebug("%s %s ext win blk idx %d reach the end, size: %d, skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, (int32_t)pExtW->pWins->size);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }
  
  SExtWinTimeWindow* pWin = taosArrayGet(pExtW->pWins, pExtW->blkWinIdx);
  if (tsCol[pInfo->rows - 1] < pWin->tw.skey) {
    qDebug("%s %s block end ts %" PRId64 " is small than curr win %d skey %" PRId64 ", skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[pInfo->rows - 1], pExtW->blkWinIdx, pWin->tw.skey);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int64_t r = 0;

  qDebug("%s %s start to get ovlp win from winIdx %d rowIdx %d", GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, pExtW->blkRowStartIdx);
  
  // TODO handle desc order
  for (; pExtW->blkWinIdx < pExtW->pWins->size; ++pExtW->blkWinIdx) {
    pWin = taosArrayGet(pExtW->pWins, pExtW->blkWinIdx);
    for (r = pExtW->blkRowStartIdx; r < pInfo->rows; ++r) {
      if (tsCol[r] < pWin->tw.skey) {
        pExtW->blkRowStartIdx = r + 1;
        continue;
      }

      if (tsCol[r] < pWin->tw.ekey) {
        extWinSetCurWinIdx(pOperator, pExtW->blkWinIdx);
        *ppWin = pWin;
        *startPos = r;
        *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, r, pWin->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);

        qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
            GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, pWin->tw.skey, pWin->tw.ekey, *winRows, (int32_t)r, tsCol[r], tsCol[r + *winRows - 1]);
        
        if ((r + *winRows) < pInfo->rows) {
          pExtW->blkWinStartIdx = pExtW->blkWinIdx + 1;
          pExtW->blkWinStartSet = true;
        }
        
        return TSDB_CODE_SUCCESS;
      }

      break;
    }

    if (r >= pInfo->rows) {
      if (!pExtW->blkWinStartSet) {
        pExtW->blkWinStartIdx = pExtW->blkWinIdx;
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
  if ((*startPos) >= pInfo->rows) {
    qDebug("%s %s blk rowIdx %d reach the end, size: %d, skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, *startPos, (int32_t)pInfo->rows);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }
  
  if (pExtW->blkWinIdx < 0) {
    pExtW->blkWinIdx = extWinGetMultiTbWinFromTs(pOperator, pExtW, tsCol, pInfo->rows, startPos);
    if (pExtW->blkWinIdx < 0) {
      qDebug("%s %s blk TR[%" PRId64 ", %" PRId64 ") not in any win, skip block", 
          GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[0], tsCol[pInfo->rows - 1]);
      *ppWin = NULL;
      return TSDB_CODE_SUCCESS;
    }

    extWinSetCurWinIdx(pOperator, pExtW->blkWinIdx);
    *ppWin = taosArrayGet(pExtW->pWins, pExtW->blkWinIdx);
    *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, *startPos, (*ppWin)->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);

    qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
        GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, (*ppWin)->tw.skey, (*ppWin)->tw.ekey, *winRows, *startPos, tsCol[*startPos], tsCol[*startPos + *winRows - 1]);
    
    return TSDB_CODE_SUCCESS;
  } else {
    pExtW->blkWinIdx++;
  }

  if (pExtW->blkWinIdx >= pExtW->pWins->size) {
    qDebug("%s %s ext win blk idx %d reach the end, size: %d, skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, (int32_t)pExtW->pWins->size);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }
  
  SExtWinTimeWindow* pWin = taosArrayGet(pExtW->pWins, pExtW->blkWinIdx);
  if (tsCol[pInfo->rows - 1] < pWin->tw.skey) {
    qDebug("%s %s block end ts %" PRId64 " is small than curr win %d skey %" PRId64 ", skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[pInfo->rows - 1], pExtW->blkWinIdx, pWin->tw.skey);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t r = *startPos;

  qDebug("%s %s start to get mnovlp win from winIdx %d rowIdx %d", GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, r);

  // TODO handle desc order
  for (; pExtW->blkWinIdx < pExtW->pWins->size; ++pExtW->blkWinIdx) {
    pWin = taosArrayGet(pExtW->pWins, pExtW->blkWinIdx);
    for (; r < pInfo->rows; ++r) {
      if (tsCol[r] < pWin->tw.skey) {
        continue;
      }

      if (tsCol[r] < pWin->tw.ekey) {
        extWinSetCurWinIdx(pOperator, pExtW->blkWinIdx);
        *ppWin = pWin;
        *startPos = r;
        *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, r, pWin->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);

        qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
            GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, pWin->tw.skey, pWin->tw.ekey, *winRows, r, tsCol[r], tsCol[r + *winRows - 1]);
        
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

static int32_t extWinGetFirstWinFromTs(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, int64_t* tsCol,
                                       int64_t rowNum, int32_t* startPos) {
  SExtWinTimeWindow* pWin = NULL;
  int32_t            idx = taosArraySearchIdx(pExtW->pWins, tsCol, extWinTsWinCompare, TD_EQ);
  if (idx >= 0) {
    for (int i = idx - 1; i >= 0; --i) {
      pWin = TARRAY_GET_ELEM(pExtW->pWins, i);
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
    for (; w < pExtW->pWins->size; ++w) {
      pWin = TARRAY_GET_ELEM(pExtW->pWins, w);
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
  if (pExtW->blkWinIdx < 0) {
    pExtW->blkWinIdx = extWinGetFirstWinFromTs(pOperator, pExtW, tsCol, pInfo->rows, startPos);
    if (pExtW->blkWinIdx < 0) {
      qDebug("%s %s blk TR[%" PRId64 ", %" PRId64 ") not in any win, skip block", 
          GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[0], tsCol[pInfo->rows - 1]);
      *ppWin = NULL;
      return TSDB_CODE_SUCCESS;
    }

    extWinSetCurWinIdx(pOperator, pExtW->blkWinIdx);
    *ppWin = taosArrayGet(pExtW->pWins, pExtW->blkWinIdx);
    *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, *startPos, (*ppWin)->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);
    
    qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
        GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, (*ppWin)->tw.skey, (*ppWin)->tw.ekey, *winRows, *startPos, tsCol[*startPos], tsCol[*startPos + *winRows - 1]);
    
    return TSDB_CODE_SUCCESS;
  } else {
    pExtW->blkWinIdx++;
  }

  if (pExtW->blkWinIdx >= pExtW->pWins->size) {
    qDebug("%s %s ext win blk idx %d reach the end, size: %d, skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, (int32_t)pExtW->pWins->size);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }
  
  SExtWinTimeWindow* pWin = taosArrayGet(pExtW->pWins, pExtW->blkWinIdx);
  if (tsCol[pInfo->rows - 1] < pWin->tw.skey) {
    qDebug("%s %s block end ts %" PRId64 " is small than curr win %d skey %" PRId64 ", skip block", 
        GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[pInfo->rows - 1], pExtW->blkWinIdx, pWin->tw.skey);
    *ppWin = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int64_t r = 0;

  qDebug("%s %s start to get movlp win from winIdx %d rowIdx %d", GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, pExtW->blkRowStartIdx);

  // TODO handle desc order
  for (; pExtW->blkWinIdx < pExtW->pWins->size; ++pExtW->blkWinIdx) {
    pWin = taosArrayGet(pExtW->pWins, pExtW->blkWinIdx);
    for (r = pExtW->blkRowStartIdx; r < pInfo->rows; ++r) {
      if (tsCol[r] < pWin->tw.skey) {
        pExtW->blkRowStartIdx = r + 1;
        continue;
      }

      if (tsCol[r] < pWin->tw.ekey) {
        extWinSetCurWinIdx(pOperator, pExtW->blkWinIdx);
        *ppWin = pWin;
        *startPos = r;
        *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, r, pWin->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);

        qDebug("%s %s the %dth ext win TR[%" PRId64 ", %" PRId64 ") got %d rows rowStartidx %d ts[%" PRId64 ", %" PRId64 "] in blk", 
            GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, pWin->tw.skey, pWin->tw.ekey, *winRows, (int32_t)r, tsCol[r], tsCol[r + *winRows - 1]);
        
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

static int32_t extWinAggSetWinOutputBuf(SOperatorInfo* pOperator, SExtWinTimeWindow* win, SExprSupp* pSupp, 
                                     SAggSupporter* pAggSup, SExecTaskInfo* pTaskInfo) {
  int32_t code = 0, lino = 0;
  SResultRow* pResultRow = NULL;
  SExternalWindowOperator* pExtW = (SExternalWindowOperator*)pOperator->info;
  
#if 0
  SResultRow* pResultRow = doSetResultOutBufByKey(pAggSup->pResultBuf, pResultRowInfo, (char*)&win->skey, TSDB_KEYSIZE,
                                                  true, tableGroupId, pTaskInfo, true, pAggSup, true);
  if (pResultRow == NULL) {
    qError("failed to set result output buffer, error:%s", tstrerror(pTaskInfo->code));
    return pTaskInfo->code;
  }

  qDebug("current result rows num:%d", tSimpleHashGetSize(pAggSup->pResultRowHashTable));

#else
  if (win->winOutIdx >= 0) {
    pResultRow = (SResultRow*)((char*)pExtW->pResultRow + win->winOutIdx * pAggSup->resultRowSize);
  } else {
    win->winOutIdx = pExtW->outWinIdx++;
    
    qDebug("set window [%" PRId64 ", %" PRId64 "] outIdx:%d", win->tw.skey, win->tw.ekey, win->winOutIdx);

    pResultRow = (SResultRow*)((char*)pExtW->pResultRow + win->winOutIdx * pAggSup->resultRowSize);
    
    memset(pResultRow, 0, pAggSup->resultRowSize);

    pResultRow->winIdx = extWinGetCurWinIdx(pOperator->pTaskInfo);
    TAOS_SET_POBJ_ALIGNED(&pResultRow->win, &win->tw);
  }
#endif

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
  if (pExtW->outWinIdx <= 0 || (pExtW->multiTableMode && !pExtW->inputHasOrder)) {
    return false;
  }

  if (NULL == pExtW->timeRangeExpr || !pExtW->timeRangeExpr->needCalc) {
    return true;
  }

  SList* pList = taosArrayGetP(pExtW->pOutputBlocks, pExtW->outWinIdx - 1);
  if (0 == listNEles(pList)) {
    return true;
  }

  SListNode* pNode = listTail(pList);
  SArray* pBlkWinIdx = *((SArray**)pNode->data + 1);
  int64_t* pIdx = taosArrayGetLast(pBlkWinIdx);
  if (pIdx && *(int32_t*)pIdx < pExtW->blkWinStartIdx) {
    return true;
  }

  return false;
}

static int32_t extWinGetWinResBlock(SOperatorInfo* pOperator, int32_t rows, SExtWinTimeWindow* pWin, SSDataBlock** ppRes, SArray** ppIdx) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SList*                   pList = NULL;
  int32_t                  code = TSDB_CODE_SUCCESS, lino = 0;
  
  if (pWin->winOutIdx >= 0) {
    pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->winOutIdx);
  } else {
    if (extWinLastWinClosed(pExtW)) {
      pWin->winOutIdx = pExtW->outWinIdx - 1;
      pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->winOutIdx);
    } else {
      pWin->winOutIdx = pExtW->outWinIdx++;
      pList = tdListNew(POINTER_BYTES * 2);
      TSDB_CHECK_NULL(pList, code, lino, _exit, terrno);
      SList** ppList = taosArrayGet(pExtW->pOutputBlocks, pWin->winOutIdx);
      extWinRecycleBlockList(pExtW, ppList);
      *ppList = pList;
    }
  }
  
  TAOS_CHECK_EXIT(extWinGetLastBlockFromList(pExtW, pList, rows, ppRes, ppIdx));

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
      pOperator->pTaskInfo->id.str, __func__, pWin->tw.skey, pWin->tw.ekey, pResBlock, pIdx, pWin->winOutIdx, pResBlock->info.capacity);
  
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

    qDebug("%s ext window [%" PRId64 ", %" PRId64 ") project start, ascScan:%d, startPos:%d, winRows:%d",
           GET_TASKID(pOperator->pTaskInfo), pWin->tw.skey, pWin->tw.ekey, ascScan, startPos, winRows);        
    
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

  pResultRow = (SResultRow*)((char*)pExtW->pResultRow + win->winOutIdx * pAggSup->resultRowSize);
  
  qDebug("set window [%" PRId64 ", %" PRId64 "] outIdx:%d", win->tw.skey, win->tw.ekey, win->winOutIdx);

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
  
  if (pWin->winOutIdx >= 0) {
    pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->winOutIdx);
    TAOS_CHECK_EXIT(extWinIndefRowsSetWinOutputBuf(pExtW, pWin, &pOperator->exprSupp, &pExtW->aggSup, pOperator->pTaskInfo, false));
  } else {
    if (extWinLastWinClosed(pExtW)) {
      pWin->winOutIdx = pExtW->outWinIdx - 1;
      pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->winOutIdx);
    } else {
      pWin->winOutIdx = pExtW->outWinIdx++;
      pList = tdListNew(POINTER_BYTES * 2);
      TSDB_CHECK_NULL(pList, code, lino, _exit, terrno);
      SList** ppList = taosArrayGet(pExtW->pOutputBlocks, pWin->winOutIdx);
      extWinRecycleBlockList(pExtW, ppList);
      *ppList = pList;
    }
    TAOS_CHECK_EXIT(extWinIndefRowsSetWinOutputBuf(pExtW, pWin, &pOperator->exprSupp, &pExtW->aggSup, pOperator->pTaskInfo, true));
  }
  
  TAOS_CHECK_EXIT(extWinGetLastBlockFromList(pExtW, pList, rows, ppRes, ppIdx));

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

    qDebug("%s ext window [%" PRId64 ", %" PRId64 ") indefRows start, ascScan:%d, startPos:%d, winRows:%d",
           GET_TASKID(pOperator->pTaskInfo), pWin->tw.skey, pWin->tw.ekey, ascScan, startPos, winRows);        
    
    TAOS_CHECK_EXIT(extWinIndefRowsDo(pOperator, pInputBlock, startPos, winRows, pWin));
    
    startPos += winRows;
  }
  
_exit:

  if (code) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinNonAggOutputRes(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SExternalWindowOperator* pExtW = pOperator->info;
  int32_t                  numOfWin = pExtW->outWinIdx;
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSDataBlock*             pRes = NULL;

  for (; pExtW->outputWinId < numOfWin; pExtW->outputWinId++, extWinIncCurWinOutIdx(pOperator->pTaskInfo->pStreamRuntimeInfo)) {
    SList* pList = taosArrayGetP(pExtW->pOutputBlocks, pExtW->outputWinId);
    if (listNEles(pList) <= 0) {
      continue;
    }

    SListNode* pNode = tdListPopHead(pList);
    pRes = *(SSDataBlock**)pNode->data;
    pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx = *(SArray**)((SArray**)pNode->data + 1);
    pExtW->pLastBlkNode = pNode;

    if (listNEles(pList) <= 0) {
      pExtW->outputWinId++;
      extWinIncCurWinOutIdx(pOperator->pTaskInfo->pStreamRuntimeInfo);
    }

    break;
  }

  if (pRes) {
    qDebug("%s result generated, rows:%" PRId64 , GET_TASKID(pOperator->pTaskInfo), pRes->info.rows);
    pRes->info.version = pOperator->pTaskInfo->version;
    pRes->info.dataLoad = 1;
  } else {
    pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx = NULL;
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

  if (NULL == pExtW->pEmptyInputBlock || (pWin && pWin->tw.skey == pExtW->lastSKey)) {
    goto _exit;
  }

  bool ascScan = pExtW->binfo.inputTsOrder == TSDB_ORDER_ASC;
  int32_t endIdx = allRemains ? (pExtW->pWins->size - 1) : (currIdx - 1);
  SResultRowInfo* pResultRowInfo = &pExtW->binfo.resultRowInfo;
  SSDataBlock* pInput = pExtW->pEmptyInputBlock;

  if ((pExtW->lastWinId + 1) <= endIdx) {
    TAOS_CHECK_EXIT(setInputDataBlock(pSup, pExtW->pEmptyInputBlock, pExtW->binfo.inputTsOrder, MAIN_SCAN, true));
  }
  
  for (int32_t i = pExtW->lastWinId + 1; i <= endIdx; ++i) {
    SExtWinTimeWindow* pWin = taosArrayGet(pExtW->pWins, i);

    extWinSetCurWinIdx(pOperator, i);
    qDebug("%s %dth ext empty window start:%" PRId64 ", end:%" PRId64 ", ascScan:%d",
           GET_TASKID(pOperator->pTaskInfo), i, pWin->tw.skey, pWin->tw.ekey, ascScan);

    TAOS_CHECK_EXIT(extWinAggSetWinOutputBuf(pOperator, pWin, pSup, &pExtW->aggSup, pOperator->pTaskInfo));

    updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pWin->tw, 1);
    code = extWinAggDo(pOperator, 0, 1, pInput);
    pExtW->lastWinId = i;  
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

    qDebug("%s ext window [%" PRId64 ", %" PRId64 ") agg start, ascScan:%d, startPos:%d, winRows:%d",
           GET_TASKID(pOperator->pTaskInfo), pWin->tw.skey, pWin->tw.ekey, ascScan, startPos, winRows);        

    if (!scalarCalc) {
      if (pExtW->scalarSupp.pExprInfo) {
        SExprSupp* pScalarSup = &pExtW->scalarSupp;
        TAOS_CHECK_EXIT(projectApplyFunctions(pScalarSup->pExprInfo, pInputBlock, pInputBlock, pScalarSup->pCtx, pScalarSup->numOfExprs,
                                     pExtW->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo)));
      }
      
      scalarCalc = true;
    }

    if (pWin->tw.skey != pExtW->lastSKey) {
      TAOS_CHECK_EXIT(extWinAggSetWinOutputBuf(pOperator, pWin, &pOperator->exprSupp, &pExtW->aggSup, pOperator->pTaskInfo));
    }
    
    updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pWin->tw, 1);
    TAOS_CHECK_EXIT(extWinAggDo(pOperator, startPos, winRows, pInputBlock));
    
    pExtW->lastSKey = pWin->tw.skey;
    pExtW->lastWinId = extWinGetCurWinIdx(pOperator->pTaskInfo);
    startPos += winRows;
  }

_exit:

  if (code) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinAggOutputRes(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*             pBlock = pExtW->binfo.pRes;
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SExprInfo*               pExprInfo = pOperator->exprSupp.pExprInfo;
  int32_t                  numOfExprs = pOperator->exprSupp.numOfExprs;
  int32_t*                 rowEntryOffset = pOperator->exprSupp.rowEntryInfoOffset;
  SqlFunctionCtx*          pCtx = pOperator->exprSupp.pCtx;
  int32_t                  numOfWin = pExtW->outWinIdx;

  pBlock->info.version = pTaskInfo->version;
  blockDataCleanup(pBlock);
  taosArrayClear(pExtW->pWinRowIdx);

  for (; pExtW->outputWinId < pExtW->pWins->size; pExtW->outputWinId += 1) {
    SExtWinTimeWindow* pWin = taosArrayGet(pExtW->pWins, pExtW->outputWinId);
    int32_t            winIdx = pWin->winOutIdx;
    if (winIdx < 0) {
      continue;
    }

    pExtW->outputWinNum++;
    SResultRow* pRow = (SResultRow*)((char*)pExtW->pResultRow + winIdx * pExtW->aggSup.resultRowSize);

    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);

    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      continue;
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      uint32_t newSize = pBlock->info.rows + pRow->numOfRows + numOfWin - pExtW->outputWinNum;
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

  qDebug("%s result generated, rows:%" PRId64 ", groupId:%" PRIu64, GET_TASKID(pTaskInfo), pBlock->info.rows,
         pBlock->info.id.groupId);
         
  pBlock->info.dataLoad = 1;

  *ppRes = (pBlock->info.rows > 0) ? pBlock : NULL;
  pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx = pExtW->pWinRowIdx;

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinInitResultRow(SExecTaskInfo*        pTaskInfo, SExternalWindowOperator* pExtW, int32_t winNum) {
  if (EEXT_MODE_SCALAR == pExtW->mode) {
    return TSDB_CODE_SUCCESS;
  }

  if (winNum <= pExtW->resultRowCapacity) {
    return TSDB_CODE_SUCCESS;
  }
  
  taosMemoryFreeClear(pExtW->pResultRow);
  pExtW->resultRowCapacity = -1;

  int32_t code = 0, lino = 0;
  
  pExtW->pResultRow = taosMemoryCalloc(winNum, pExtW->aggSup.resultRowSize);
  QUERY_CHECK_NULL(pExtW->pResultRow, code, lino, _exit, terrno);

  pExtW->resultRowCapacity = winNum;

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", pTaskInfo->id.str, __func__, lino, tstrerror(code));
  }

  return code;
}

static void extWinFreeResultRow(SExternalWindowOperator* pExtW) {
  if (pExtW->resultRowCapacity * pExtW->aggSup.resultRowSize >= 1048576) {
    taosMemoryFreeClear(pExtW->pResultRow);
    pExtW->resultRowCapacity = -1;
  }
  if (pExtW->binfo.pRes && pExtW->binfo.pRes->info.rows * pExtW->aggSup.resultRowSize >= 1048576) {
    blockDataFreeCols(pExtW->binfo.pRes);
  }
}

static int32_t extWinInitWindowList(SExternalWindowOperator* pExtW, SExecTaskInfo*        pTaskInfo) {
  if (taosArrayGetSize(pExtW->pWins) > 0) {
    return TSDB_CODE_SUCCESS;
  }
  
  int32_t code = 0, lino = 0;
  SStreamRuntimeFuncInfo* pInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
  size_t size = taosArrayGetSize(pInfo->pStreamPesudoFuncVals);
  SExtWinTimeWindow* pWin = taosArrayReserve(pExtW->pWins, size);
  TSDB_CHECK_NULL(pWin, code, lino, _exit, terrno);

  TAOS_CHECK_EXIT(extWinInitResultRow(pTaskInfo, pExtW, size));

  if (pExtW->timeRangeExpr && pExtW->timeRangeExpr->needCalc) {
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
      pWin[i].winOutIdx = -1;

      qDebug("%s the %d/%d ext window initialized, TR[%" PRId64 ", %" PRId64 ")", 
          pTaskInfo->id.str, i, (int32_t)size, pWin[i].tw.skey, pWin[i].tw.ekey);
    }
  }
  
  pExtW->outputWinId = pInfo->curIdx;
  pExtW->lastWinId = -1;
  pExtW->blkWinStartIdx = pInfo->curIdx;

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", pTaskInfo->id.str, __func__, lino, tstrerror(code));
  }

  return code;
}

static bool extWinNonAggGotResBlock(SExternalWindowOperator* pExtW) {
  if (pExtW->multiTableMode && !pExtW->inputHasOrder) {
    return false;
  }
  int32_t remainWin = pExtW->outWinIdx - pExtW->outputWinId;
  if (remainWin > 1 && (NULL == pExtW->timeRangeExpr || !pExtW->timeRangeExpr->needCalc)) {
    return true;
  }
  
  SList* pList = taosArrayGetP(pExtW->pOutputBlocks, pExtW->outputWinId);
  if (!pList || listNEles(pList) <= 0) {
    return false;
  }
  if (listNEles(pList) > 1) {
    return true;
  }

  SListNode* pNode = listHead(pList);
  SArray* pIdx = *(SArray**)((SArray**)pNode->data + 1);
  int32_t* winIdx = taosArrayGetLast(pIdx);
  if (winIdx && *winIdx < pExtW->blkWinStartIdx) {
    return true;
  }

  return false;
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
  
  TAOS_CHECK_EXIT(extWinInitWindowList(pExtW, pTaskInfo));

  while (1) {
    pExtW->blkWinIdx = -1;
    pExtW->blkWinStartSet = false;
    pExtW->blkRowStartIdx = 0;
    
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      if (EEXT_MODE_AGG == pExtW->mode) {
        TAOS_CHECK_EXIT(extWinAggHandleEmptyWins(pOperator, pBlock, true, NULL));
      }
      pExtW->blkWinStartIdx = pExtW->pWins->size;
      break;
    }

    printDataBlock(pBlock, __func__, pTaskInfo->id.str, pTaskInfo->id.queryId);

    qDebug("ext window mode:%d got %" PRId64 " rows from downstream", pExtW->mode, pBlock->info.rows);
    
    switch (pExtW->mode) {
      case EEXT_MODE_SCALAR:
        TAOS_CHECK_EXIT(extWinProjectOpen(pOperator, pBlock));
        if (extWinNonAggGotResBlock(pExtW)) {
          return code;
        }
        break;
      case EEXT_MODE_AGG:
        TAOS_CHECK_EXIT(extWinAggOpen(pOperator, pBlock));
        break;
      case EEXT_MODE_INDEFR_FUNC:
        TAOS_CHECK_EXIT(extWinIndefRowsOpen(pOperator, pBlock));
        if (extWinNonAggGotResBlock(pExtW)) {
          return code;
        }
        break;
      default:
        break;
    }
  }

  OPTR_SET_OPENED(pOperator);

#if 0
  if (pExtW->mode == EEXT_MODE_AGG) {
    qDebug("ext window before dump final rows num:%d", tSimpleHashGetSize(pExtW->aggSup.pResultRowHashTable));

    code = initGroupedResultInfo(&pExtW->groupResInfo, pExtW->aggSup.pResultRowHashTable, pExtW->binfo.inputTsOrder);
    QUERY_CHECK_CODE(code, lino, _exit);

    qDebug("ext window after dump final rows num:%d", tSimpleHashGetSize(pExtW->aggSup.pResultRowHashTable));
  }
#endif

_exit:

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
#if 0    
    doBuildResultDatablock(pOperator, &pExtW->binfo, &pExtW->groupResInfo, pExtW->aggSup.pResultBuf);
    bool hasRemain = hasRemainResults(&pExtW->groupResInfo);
    if (!hasRemain) {
      setOperatorCompleted(pOperator);
      break;
    }
    if (pExtW->binfo.pRes->info.rows > 0) break;
#else
    TAOS_CHECK_EXIT(extWinAggOutputRes(pOperator, ppRes));
    setOperatorCompleted(pOperator);
    extWinFreeResultRow(pExtW);
#endif      
  }

  if (*ppRes) {
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
    pExtW->pOutputBlocks = taosArrayInit_s(POINTER_BYTES, STREAM_CALC_REQ_MAX_WIN_NUM);
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

    pExtW->lastSKey = INT64_MIN;
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
    pExtW->pOutputBlocks = taosArrayInit_s(POINTER_BYTES, STREAM_CALC_REQ_MAX_WIN_NUM);
    if (!pExtW->pOutputBlocks) QUERY_CHECK_CODE(terrno, lino, _error);
  //}
    pExtW->pFreeBlocks = tdListNew(POINTER_BYTES * 2);
    QUERY_CHECK_NULL(pExtW->pFreeBlocks, code, lino, _error, terrno);  
  }

  pExtW->pWins = taosArrayInit(4096, sizeof(SExtWinTimeWindow));
  if (!pExtW->pWins) QUERY_CHECK_CODE(terrno, lino, _error);
  
  //initResultRowInfo(&pExtW->binfo.resultRowInfo);

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

  pOperator->fpSet = createOperatorFpSet(extWinOpen, extWinNext, NULL, destroyExternalWindowOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetExternalWindowOperator);
  code = appendDownstream(pOperator, &pDownstream, 1);
  if (code != 0) {
    goto _error;
  }

  *pOptrOut = pOperator;
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


