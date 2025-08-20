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

typedef struct SBlockList {
  const SSDataBlock* pSrcBlock;
  SList*             pBlocks;
  int32_t            blockRowNumThreshold;
} SBlockList;

typedef struct SExtWinTimeWindow {
  STimeWindow tw;
  int32_t     winOutIdx;
} SExtWinTimeWindow;

typedef int32_t (*extWinGetWinFp)(SOperatorInfo*, int64_t*, int32_t*, SDataBlockInfo*, SExtWinTimeWindow**, int32_t*);

typedef struct SExternalWindowOperator {
  SOptrBasicInfo     binfo;
  SExprSupp          scalarSupp;
  int32_t            primaryTsIndex;
  EExtWinMode        mode;
  bool               multiTableMode;
  SArray*            pWins;           // SArray<SExtWinTimeWindow>
  // SArray*            pOffsetList;  // for each window
  SArray*            pPseudoColInfo;  
  // SLimitInfo         limitInfo;  // limit info for each window

  extWinGetWinFp     getWinFp;

  bool               blkWinStartSet;
  int32_t            blkWinStartIdx;
  int32_t            blkWinIdx;
  int32_t            blkRowStartIdx;
  int32_t            outputWinId;
  int32_t            outWinIdx;

  // for project&indefRows
  SArray*            pOutputBlocks;  // SArray<SList*>, for each window, we have a list of blocks
  SListNode*         pOutputBlockListNode;  // block index in block array used for output
  SSDataBlock*       pTmpBlock;
  
  // for agg
  SAggSupporter      aggSup;
  STimeWindowAggSupp twAggSup;
  SResultRow*        pResultRow;
  int64_t            lastSKey;
  int32_t            lastWinId;
  SSDataBlock*       pEmptyInputBlock;
  bool               hasCountFunc;
} SExternalWindowOperator;


static int32_t extWinBlockListAddBlock(SExternalWindowOperator* pExtW, SList* pList, int32_t rows, SSDataBlock** ppBlock) {
  SSDataBlock* pRes = NULL;
  int32_t code = 0, lino = 0;
  
  TAOS_CHECK_EXIT(createOneDataBlock(pExtW->binfo.pRes, false, &pRes));
  TAOS_CHECK_EXIT(blockDataEnsureCapacity(pRes, TMAX(rows, 4096)));
  TAOS_CHECK_EXIT(tdListAppend(pList, &pRes));

  *ppBlock = pRes;

_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(pRes);
  }
  
  return code;
}

static int32_t extWinGetLastBlockFromList(SExternalWindowOperator* pExtW, SList* pList, int32_t rows, SSDataBlock** ppBlock) {
  int32_t    code = 0, lino = 0;
  SSDataBlock* pRes = NULL;

  SListNode* pNode = TD_DLIST_TAIL(pList);
  if (NULL == pNode) {
    TAOS_CHECK_EXIT(extWinBlockListAddBlock(pExtW, pList, rows, ppBlock));
    return code;
  }

  pRes = *(SSDataBlock**)pNode->data;
  if ((pRes->info.rows + rows) > pRes->info.capacity) {
    TAOS_CHECK_EXIT(extWinBlockListAddBlock(pExtW, pList, rows, ppBlock));
    return code;
  }

  *ppBlock = pRes;

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
      pTmp = pNode;
      pNode = pNode->dl_next_;
      taosMemoryFree(pTmp);
    }
  }
  taosMemoryFree(*ppList);
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
 
  taosArrayDestroy(pInfo->pPseudoColInfo);
  blockDataDestroy(pInfo->pTmpBlock);
  blockDataDestroy(pInfo->pEmptyInputBlock);

  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSupp);
  taosMemoryFreeClear(pInfo->pResultRow);

  pInfo->binfo.resultRowInfo.openWindow = tdListFree(pInfo->binfo.resultRowInfo.openWindow);

  taosMemoryFreeClear(pInfo);
}

static int32_t extWinOpen(SOperatorInfo* pOperator);
static int32_t extWinNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);

typedef struct SMergeAlignedExternalWindowOperator {
  SExternalWindowOperator* pExtW;
  int64_t curTs;
  SSDataBlock* pPrefetchedBlock;
  SResultRow*  pResultRow;
} SMergeAlignedExternalWindowOperator;

void destroyMergeAlignedExternalWindowOperator(void* pOperator) {
  SMergeAlignedExternalWindowOperator* pMlExtInfo = (SMergeAlignedExternalWindowOperator*)pOperator;
  destroyExternalWindowOperatorInfo(pMlExtInfo->pExtW);
}

void doMergeAlignExternalWindow(SOperatorInfo* pOperator);

static int32_t setSingleOutputTupleBuf(SResultRowInfo* pResultRowInfo, const STimeWindow* pWin, SResultRow** pResult,
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
  return setResultRowInitCtx((*pResult), pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
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


static void extWinIncCurWinOutIdx(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  pTaskInfo->pStreamRuntimeInfo->funcInfo.curOutIdx++;
}


static const STimeWindow* extWinGetNextWin(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo) {
  int32_t curIdx = extWinGetCurWinIdx(pTaskInfo);
  if (curIdx + 1 >= pExtW->pWins->size) return NULL;
  return taosArrayGet(pExtW->pWins, curIdx + 1);
}


static const STimeWindow* extWinGetWinFromTs(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, TSKEY ts) {
  // TODO handle desc order
  for (int32_t i = 0; i < pExtW->pWins->size; ++i) {
    const STimeWindow* pWin = taosArrayGet(pExtW->pWins, i);
    if (ts >= pWin->skey && ts < pWin->ekey) {
      extWinSetCurWinIdx(pOperator, i);
      return pWin;
    }
  }
  return NULL;
}

static int32_t doMergeAlignExtWindowAgg(SOperatorInfo* pOperator, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock, SSDataBlock* pResultBlock) {
  SMergeAlignedExternalWindowOperator* pMlExtInfo = pOperator->info;
  SExternalWindowOperator*             pExtW = pMlExtInfo->pExtW;

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*     pSup = &pOperator->exprSupp;
  int32_t        code = 0;

  int32_t startPos = 0;
  int64_t* tsCols = extWinExtractTsCol(pBlock, pExtW->primaryTsIndex, pTaskInfo);
  TSKEY ts = getStartTsKey(&pBlock->info.window, tsCols);

  const STimeWindow *pWin = extWinGetWinFromTs(pOperator, pExtW, ts);
  if (pWin == NULL) {
    qError("failed to get time window for ts:%" PRId64 ", index:%d, error:%s", ts, pExtW->primaryTsIndex, tstrerror(terrno));
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_INVALID_PARA);
  }
  code = setSingleOutputTupleBuf(pResultRowInfo, pWin, &pMlExtInfo->pResultRow, pSup, &pExtW->aggSup);
  if (code != 0 || pMlExtInfo->pResultRow == NULL) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  int32_t currPos = startPos;
  pMlExtInfo->curTs = pWin->skey;
  while (++currPos < pBlock->info.rows) {
    if (tsCols[currPos] == pMlExtInfo->curTs) continue;

    qDebug("current ts:%" PRId64 ", startPos:%d, currPos:%d, tsCols[currPos]:%" PRId64,
      pMlExtInfo->curTs, startPos, currPos, tsCols[currPos]); 
    code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pExtW->twAggSup.timeWindowData, startPos,
                                           currPos - startPos, pBlock->info.rows, pSup->numOfExprs);
    if (code != 0) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    finalizeResultRows(pExtW->aggSup.pResultBuf, &pResultRowInfo->cur, pSup, pResultBlock, pTaskInfo);
    resetResultRow(pMlExtInfo->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));

    pWin = extWinGetNextWin(pExtW, pTaskInfo);
    if (!pWin) break;
    qDebug("ext window align2 start:%" PRId64 ", end:%" PRId64, pWin->skey, pWin->ekey);
    extWinIncCurWinIdx(pOperator);
    startPos = currPos;
    code = setSingleOutputTupleBuf(pResultRowInfo, pWin, &pMlExtInfo->pResultRow, pSup, &pExtW->aggSup);
    if (code != 0 || pMlExtInfo->pResultRow == NULL) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
    pMlExtInfo->curTs = pWin->skey;
  }

  code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pExtW->twAggSup.timeWindowData, startPos,
                                         currPos - startPos, pBlock->info.rows, pSup->numOfExprs);
  if (code != 0) {
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static int32_t doMergeAlignExtWindowProject(SOperatorInfo* pOperator, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock,
                                            SSDataBlock* pResultBlock) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SExprSupp*               pExprSup = &pExtW->scalarSupp;
  int32_t code = projectApplyFunctions(pExprSup->pExprInfo, pResultBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL,
                        GET_STM_RTINFO(pOperator->pTaskInfo));
  return code;
}

void doMergeAlignExternalWindow(SOperatorInfo* pOperator) {
  SExecTaskInfo*                       pTaskInfo = pOperator->pTaskInfo;
  SMergeAlignedExternalWindowOperator* pMlExtInfo = pOperator->info;
  SExternalWindowOperator*             pExtW = pMlExtInfo->pExtW;
  SResultRow*                          pResultRow = NULL;
  int32_t                              code = 0;
  SSDataBlock*                         pRes = pExtW->binfo.pRes;
  SExprSupp*                           pSup = &pOperator->exprSupp;
  int32_t                              lino = 0;

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);

    if (pBlock == NULL) {
      // close last time window
      if (pMlExtInfo->curTs != INT64_MIN && EEXT_MODE_AGG == pExtW->mode) {
        finalizeResultRows(pMlExtInfo->pExtW->aggSup.pResultBuf, &pExtW->binfo.resultRowInfo.cur, pSup, pRes, pTaskInfo);
        resetResultRow(pMlExtInfo->pResultRow,pExtW->aggSup.resultRowSize - sizeof(SResultRow));
      }
      setOperatorCompleted(pOperator);
      break;
    }

    pRes->info.scanFlag = pBlock->info.scanFlag;
    code = setInputDataBlock(pSup, pBlock, pExtW->binfo.inputTsOrder, pBlock->info.scanFlag, true);
    QUERY_CHECK_CODE(code, lino, _end);

    printDataBlock(pBlock, __func__, "externalwindowAlign");
    qDebug("ext windowpExtWAlign->scalarMode:%d", pExtW->mode);


    if (EEXT_MODE_SCALAR == pExtW->mode) {
      code = doMergeAlignExtWindowProject(pOperator, &pExtW->binfo.resultRowInfo, pBlock, pRes);
    } else {
      code = doMergeAlignExtWindowAgg(pOperator, &pExtW->binfo.resultRowInfo, pBlock, pRes);
    }

    // int32_t status = handleLimitOffset(pOperator, pLimitInfo, pBlock);
    // if (status == EXTERNAL_RETRIEVE_CONTINUE) {
    //   continue;
    // } else if (status == EXTERNAL_RETRIEVE_NEXT_WINDOW) {
    //   pExtW->pOutputBlockListNode = NULL;
    //   pExtW->outputWinId++;
    //   incExtWinOutIdx(pOperator);
    //   continue;
    // } else if (status == EXTERNAL_RETRIEVE_DONE) {
    //   // do nothing, just break
    // }
  }
_end:
  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static int32_t mergeAlignedExternalWindowNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
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

  if (!pExtW->pWins) {
    size_t size = taosArrayGetSize(pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamPesudoFuncVals);
    pExtW->pWins = taosArrayInit(size, sizeof(STimeWindow));
    if (!pExtW->pWins) QUERY_CHECK_CODE(terrno, lino, _end);
    // pExtW->pOffsetList = taosArrayInit(size, sizeof(SLimitInfo));
    // if (!pExtW->pOffsetList) QUERY_CHECK_CODE(terrno, lino, _end);

    for (int32_t i = 0; i < size; ++i) {
      SSTriggerCalcParam* pParam = taosArrayGet(pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamPesudoFuncVals, i);
      STimeWindow win = {.skey = pParam->wstart, .ekey = pParam->wend};
      if (pTaskInfo->pStreamRuntimeInfo->funcInfo.triggerType != 1){  // 1 meams STREAM_TRIGGER_SLIDING
        win.ekey++;
      }
      TSDB_CHECK_NULL(taosArrayPush(pExtW->pWins, &win), code, lino, _end, terrno);
    }
    pExtW->outputWinId = pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx;
  }

  doMergeAlignExternalWindow(pOperator);
  size_t rows = pRes->info.rows;
  pOperator->resultInfo.totalRows += rows;
  (*ppRes) = (rows == 0) ? NULL : pRes;

_end:
  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

int32_t resetMergeAlignedExternalWindowOperator(SOperatorInfo* pOperator) {
  SMergeAlignedExternalWindowOperator* pMlExtInfo = pOperator->info;
  SExternalWindowOperator*             pExtW = pMlExtInfo->pExtW;
  SExecTaskInfo*                       pTaskInfo = pOperator->pTaskInfo;
  SMergeAlignedIntervalPhysiNode * pPhynode = (SMergeAlignedIntervalPhysiNode*)pOperator->pPhyNode;
  pOperator->status = OP_NOT_OPENED;

  taosArrayDestroy(pExtW->pWins);
  pExtW->pWins = NULL;

  resetBasicOperatorState(&pExtW->binfo);
  pMlExtInfo->pResultRow = NULL;
  pMlExtInfo->curTs = INT64_MIN;
  if (pMlExtInfo->pPrefetchedBlock) blockDataCleanup(pMlExtInfo->pPrefetchedBlock);

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
  pMlExtInfo->curTs = INT64_MIN;

  pExtW->primaryTsIndex = ((SColumnNode*)pPhynode->window.pTspk)->slotId;
  pExtW->mode = pPhynode->window.pProjs ? EEXT_MODE_SCALAR : EEXT_MODE_AGG;
  pExtW->binfo.inputTsOrder = pPhynode->window.node.inputTsOrder = TSDB_ORDER_ASC;
  pExtW->binfo.outputTsOrder = pExtW->binfo.inputTsOrder;

  // pExtW->limitInfo = (SLimitInfo){0};
  // initLimitInfo(pPhynode->window.node.pLimit, pPhynode->window.node.pSlimit, &pExtW->limitInfo);

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 100000);

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

  initResultRowInfo(&pExtW->binfo.resultRowInfo);
  code = blockDataEnsureCapacity(pExtW->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);
  setOperatorInfo(pOperator, "MergeAlignedExternalWindowOperator", QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW, false, OP_NOT_OPENED, pMlExtInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, mergeAlignedExternalWindowNext, NULL,
                                         destroyMergeAlignedExternalWindowOperator, optrDefaultBufFn, NULL,
                                         optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetMergeAlignedExternalWindowOperator);

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

static int32_t resetExternalWindowOperator(SOperatorInfo* pOperator) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SExternalWindowPhysiNode* pPhynode = (SExternalWindowPhysiNode*)pOperator->pPhyNode;
  pOperator->status = OP_NOT_OPENED;

  resetBasicOperatorState(&pExtW->binfo);
  pExtW->outputWinId = 0;
  pExtW->lastWinId = -1;
  taosArrayDestroyEx(pExtW->pOutputBlocks, extWinDestroyBlockList);
  // taosArrayDestroy(pExtW->pOffsetList);
  taosArrayDestroy(pExtW->pWins);
  pExtW->pWins = NULL;
  pExtW->pOutputBlockListNode = NULL;
  pExtW->pOutputBlocks = NULL;
  // pExtW->pOffsetList = NULL;
  int32_t code = blockDataEnsureCapacity(pExtW->binfo.pRes, pOperator->resultInfo.capacity);
  if (code == 0) {
    code = resetAggSup(&pOperator->exprSupp, &pExtW->aggSup, pTaskInfo, pPhynode->window.pFuncs, NULL,
                       sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, pTaskInfo->streamInfo.pState,
                       &pTaskInfo->storageAPI.functionStore);
  }
  if (code == 0) {
    code = resetExprSupp(&pExtW->scalarSupp, pTaskInfo, pPhynode->window.pProjs, NULL,
                         &pTaskInfo->storageAPI.functionStore);
  }
  if (code == 0) {
    colDataDestroy(&pExtW->twAggSup.timeWindowData);
    code = initExecTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pTaskInfo->window);
  }
  blockDataDestroy(pExtW->pTmpBlock);
  pExtW->pTmpBlock = NULL;

  pExtW->outWinIdx = 0;
  pExtW->lastSKey = INT64_MIN;
  
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
  if (ts > pWin->tw.ekey) {
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
      
      if (tsCol[i] <= pWin->tw.ekey) {
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

  qDebug("%s %s start to get win from winIdx %d rowIdx %d", GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, r);

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
        
        return TSDB_CODE_SUCCESS;
      }

      break;
    }

    if (r == pInfo->rows) {
      break;
    }
  }

  qDebug("%s %s no more ext win in block, time range[%" PRId64 ", %" PRId64 "], skip it", 
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

  qDebug("%s %s start to get win from winIdx %d rowIdx %d", GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, pExtW->blkRowStartIdx);
  
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
        
        if ((r + *winRows) >= pInfo->rows) {
          pExtW->blkWinStartIdx = pExtW->blkWinIdx;
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

  qDebug("%s %s no more ext win in block, time range[%" PRId64 ", %" PRId64 "], skip it", 
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
      qDebug("%s %s blk time range[%" PRId64 ", %" PRId64 "] not in any win, skip block", 
          GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[0], tsCol[pInfo->rows - 1]);
      *ppWin = NULL;
      return TSDB_CODE_SUCCESS;
    }

    extWinSetCurWinIdx(pOperator, pExtW->blkWinIdx);
    *ppWin = taosArrayGet(pExtW->pWins, pExtW->blkWinIdx);
    *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, *startPos, (*ppWin)->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);
    
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

  qDebug("%s %s start to get win from winIdx %d rowIdx %d", GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, r);

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
        
        return TSDB_CODE_SUCCESS;
      }

      break;
    }

    if (r == pInfo->rows) {
      break;
    }
  }

  qDebug("%s %s no more ext win in block, time range[%" PRId64 ", %" PRId64 "], skip it", 
      GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[0], tsCol[pInfo->rows - 1]);

  *ppWin = NULL;
  return TSDB_CODE_SUCCESS;
}


static int32_t extWinGetMultiTbOvlpWin(SOperatorInfo* pOperator, int64_t* tsCol, int32_t* startPos, SDataBlockInfo* pInfo, SExtWinTimeWindow** ppWin, int32_t* winRows) {
  SExternalWindowOperator* pExtW = pOperator->info;
  if (pExtW->blkWinIdx < 0) {
    pExtW->blkWinIdx = extWinGetMultiTbWinFromTs(pOperator, pExtW, tsCol, pInfo->rows, startPos);
    if (pExtW->blkWinIdx < 0) {
      qDebug("%s %s blk time range[%" PRId64 ", %" PRId64 "] not in any win, skip block", 
          GET_TASKID(pOperator->pTaskInfo), __func__, tsCol[0], tsCol[pInfo->rows - 1]);
      *ppWin = NULL;
      return TSDB_CODE_SUCCESS;
    }

    extWinSetCurWinIdx(pOperator, pExtW->blkWinIdx);
    *ppWin = taosArrayGet(pExtW->pWins, pExtW->blkWinIdx);
    *winRows = getNumOfRowsInTimeWindow(pInfo, tsCol, *startPos, (*ppWin)->tw.ekey - 1, binarySearchForKey, NULL, pExtW->binfo.inputTsOrder);
    
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

  qDebug("%s %s start to get win from winIdx %d rowIdx %d", GET_TASKID(pOperator->pTaskInfo), __func__, pExtW->blkWinIdx, pExtW->blkRowStartIdx);

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
        
        return TSDB_CODE_SUCCESS;
      }

      break;
    }

    if (r >= pInfo->rows) {
      break;
    }
  }

  qDebug("%s %s no more ext win in block, time range[%" PRId64 ", %" PRId64 "], skip it", 
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

static int32_t extWinAggSetWinOutputBuf(SExternalWindowOperator* pExtW, SExtWinTimeWindow* win, SExprSupp* pSupp, 
                                     SAggSupporter* pAggSup, SExecTaskInfo* pTaskInfo) {
  int32_t code = 0, lino = 0;
  SResultRow* pResultRow = NULL;
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

static int32_t extWinGetWinResBlock(SOperatorInfo* pOperator, int32_t rows, SExtWinTimeWindow* pWin, SSDataBlock** ppRes) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SSDataBlock*             pBlock = NULL;
  SList*                   pList = NULL;
  int32_t                  code = TSDB_CODE_SUCCESS, lino = 0;
  
  if (pWin->winOutIdx >= 0) {
    pList = taosArrayGetP(pExtW->pOutputBlocks, pWin->winOutIdx);
  } else {
    pWin->winOutIdx = pExtW->outWinIdx++;
    pList = tdListNew(sizeof(SSDataBlock*));
    TSDB_CHECK_NULL(pList, code, lino, _exit, terrno);
    SList** ppList = taosArrayGet(pExtW->pOutputBlocks, pWin->winOutIdx);
    *ppList = pList;
  }
  
  TAOS_CHECK_EXIT(extWinGetLastBlockFromList(pExtW, pList, rows, &pBlock));

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
  int32_t                  code = TSDB_CODE_SUCCESS, lino = 0;
  
  TAOS_CHECK_EXIT(extWinGetWinResBlock(pOperator, rows, pWin, &pResBlock));

  if (!pExtW->pTmpBlock) {
    TAOS_CHECK_EXIT(createOneDataBlock(pInputBlock, false, &pExtW->pTmpBlock));
  } else {
    blockDataCleanup(pExtW->pTmpBlock);
  }
  
  TAOS_CHECK_EXIT(blockDataEnsureCapacity(pExtW->pTmpBlock, TMAX(1, rows)));

  TAOS_CHECK_EXIT(blockDataMergeNRows(pExtW->pTmpBlock, pInputBlock, startPos, rows));
  TAOS_CHECK_EXIT(projectApplyFunctionsWithSelect(pExprSup->pExprInfo, pResBlock, pExtW->pTmpBlock, pExprSup->pCtx, pExprSup->numOfExprs,
        NULL, GET_STM_RTINFO(pOperator->pTaskInfo), true));

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", pOperator->pTaskInfo->id.str, __func__, lino, tstrerror(code));
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

    qDebug("%s ext window [%" PRId64 ", %" PRId64 "] project start, ascScan:%d, startPos:%d, winRows:%d",
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


static int32_t extWinIndefRowsDo(SOperatorInfo* pOperator, SSDataBlock* pInputBlock, int32_t startPos, int32_t rows, SExtWinTimeWindow* pWin) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SSDataBlock*             pResBlock = NULL;
  int32_t                  code = TSDB_CODE_SUCCESS, lino = 0;
  
  TAOS_CHECK_EXIT(extWinGetWinResBlock(pOperator, rows, pWin, &pResBlock));
  
  if (!pExtW->pTmpBlock) {
    TAOS_CHECK_EXIT(createOneDataBlock(pInputBlock, false, &pExtW->pTmpBlock));
  } else {
    blockDataCleanup(pExtW->pTmpBlock);
  }
  
  TAOS_CHECK_EXIT(blockDataEnsureCapacity(pExtW->pTmpBlock, TMAX(1, rows)));

  TAOS_CHECK_EXIT(blockDataMergeNRows(pExtW->pTmpBlock, pInputBlock, startPos, rows));
  TAOS_CHECK_EXIT(extWinIndefRowsDoImpl(pOperator, pResBlock, pExtW->pTmpBlock));

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

    qDebug("%s ext window [%" PRId64 ", %" PRId64 "] indefRows start, ascScan:%d, startPos:%d, winRows:%d",
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

  for (; pExtW->outputWinId < numOfWin; ) {
    SList* pList = taosArrayGetP(pExtW->pOutputBlocks, pExtW->outputWinId);
    if (pExtW->pOutputBlockListNode == NULL) {
      pExtW->pOutputBlockListNode = tdListGetHead(pList);
    } else {
      if (!pExtW->pOutputBlockListNode->dl_next_) {
        pExtW->pOutputBlockListNode = NULL;
        extWinIncCurWinOutIdx(pOperator);
        pExtW->outputWinId++;
        continue;
      }
      pExtW->pOutputBlockListNode = pExtW->pOutputBlockListNode->dl_next_;
    }

    if (pExtW->pOutputBlockListNode) {
      pRes = *(SSDataBlock**)pExtW->pOutputBlockListNode->data;
      if (pRes && pRes->info.rows > 0) {
        break;
      }

      continue;
    }
    
    extWinIncCurWinOutIdx(pOperator);
    pExtW->outputWinId++;
  }

  if (pRes) {
    qDebug("%s result generated, rows:%" PRId64 , GET_TASKID(pOperator->pTaskInfo), pRes->info.rows);
    pRes->info.version = pOperator->pTaskInfo->version;
    pRes->info.dataLoad = 1;
  }

  *ppRes = (pRes && pRes->info.rows > 0) ? pRes : NULL;

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t extWinAggHandleEmptyWins(SOperatorInfo* pOperator, SSDataBlock* pBlock, bool allRemains) {
  int32_t code = 0, lino = 0;
  SExternalWindowOperator* pExtW = (SExternalWindowOperator*)pOperator->info;
  bool ascScan = pExtW->binfo.inputTsOrder == TSDB_ORDER_ASC;
  int32_t currIdx = extWinGetCurWinIdx(pOperator->pTaskInfo);
  SExprSupp* pSup = &pOperator->exprSupp;

  if (NULL == pExtW->pEmptyInputBlock) {
    goto _exit;
  }

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

    TAOS_CHECK_EXIT(extWinAggSetWinOutputBuf(pExtW, pWin, pSup, &pExtW->aggSup, pOperator->pTaskInfo));

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

    if (pWin->tw.skey != pExtW->lastSKey) {
      TAOS_CHECK_EXIT(extWinAggHandleEmptyWins(pOperator, pInputBlock, false));
    }

    qDebug("%s ext window [%" PRId64 ", %" PRId64 "] agg start, ascScan:%d, startPos:%d, winRows:%d",
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
      TAOS_CHECK_EXIT(extWinAggSetWinOutputBuf(pExtW, pWin, &pOperator->exprSupp, &pExtW->aggSup, pOperator->pTaskInfo));
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
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*    pBlock = pExtW->binfo.pRes;
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SExprInfo*      pExprInfo = pOperator->exprSupp.pExprInfo;
  int32_t         numOfExprs = pOperator->exprSupp.numOfExprs;
  int32_t*        rowEntryOffset = pOperator->exprSupp.rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;
  int32_t         numOfWin = pExtW->outWinIdx;

  pBlock->info.version = pTaskInfo->version;
  blockDataCleanup(pBlock);

  for (; pExtW->outputWinId < numOfWin; pExtW->outputWinId += 1) {
    SResultRow* pRow = (SResultRow*)((char*)pExtW->pResultRow + pExtW->outputWinId * pExtW->aggSup.resultRowSize);

    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);

    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      continue;
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      uint32_t newSize = pBlock->info.rows + pRow->numOfRows + numOfWin - pExtW->outputWinId;
      TAOS_CHECK_EXIT(blockDataEnsureCapacity(pBlock, newSize));
      qDebug("datablock capacity not sufficient, expand to required:%d, current capacity:%d, %s", newSize,
             pBlock->info.capacity, GET_TASKID(pTaskInfo));
    }

    TAOS_CHECK_EXIT(copyResultrowToDataBlock(pExprInfo, numOfExprs, pRow, pCtx, pBlock, rowEntryOffset, pTaskInfo));

    pBlock->info.rows += pRow->numOfRows;
    if (pBlock->info.rows >= pOperator->resultInfo.threshold) {
      break;
    }
  }

  qDebug("%s result generated, rows:%" PRId64 ", groupId:%" PRIu64, GET_TASKID(pTaskInfo), pBlock->info.rows,
         pBlock->info.id.groupId);
         
  pBlock->info.dataLoad = 1;

  *ppRes = (pBlock->info.rows > 0) ? pBlock : NULL;

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}
static int32_t extWinInitWindowList(SExternalWindowOperator* pExtW, SExecTaskInfo*        pTaskInfo) {
  int32_t code = 0, lino = 0;
  size_t size = taosArrayGetSize(pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamPesudoFuncVals);
  for (int32_t i = 0; i < size; ++i) {
    SSTriggerCalcParam* pParam = taosArrayGet(pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamPesudoFuncVals, i);
    SExtWinTimeWindow* pWin = taosArrayReserve(pExtW->pWins, 1);
    TSDB_CHECK_NULL(pWin, code, lino, _exit, terrno);
    
    pWin->tw.skey = pParam->wstart;
    pWin->tw.ekey = pParam->wend;
    if (pTaskInfo->pStreamRuntimeInfo->funcInfo.triggerType != STREAM_TRIGGER_SLIDING) {
      pWin->tw.ekey++;
    }
    pWin->winOutIdx = -1;

    if (/*pExtW->multiTableMode &&*/ (EEXT_MODE_SCALAR == pExtW->mode || EEXT_MODE_INDEFR_FUNC == pExtW->mode)) {
      SList** ppList = taosArrayGet(pExtW->pOutputBlocks, i);
      extWinDestroyBlockList(ppList);
      *ppList = NULL;
    }
  }
  
  pExtW->outputWinId = pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx;
  pExtW->lastWinId = -1;
  pExtW->blkWinStartIdx = pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx;

_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", pTaskInfo->id.str, __func__, lino, tstrerror(code));
  }

  return code;
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
  
  pTaskInfo->pStreamRuntimeInfo->funcInfo.extWinProjMode = (pExtW->mode != EEXT_MODE_AGG);

  TAOS_CHECK_EXIT(extWinInitWindowList(pExtW, pTaskInfo));

  while (1) {
    pExtW->blkWinIdx = -1;
    pExtW->blkWinStartSet = false;
    pExtW->blkRowStartIdx = 0;
    
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      if (EEXT_MODE_AGG == pExtW->mode) {
        TAOS_CHECK_EXIT(extWinAggHandleEmptyWins(pOperator, pBlock, true));
      }
      break;
    }

    printDataBlock(pBlock, __func__, pTaskInfo->id.str);

    qDebug("ext window mode:%d got %" PRId64 " rows from downstream", pExtW->mode, pBlock->info.rows);
    
    switch (pExtW->mode) {
      case EEXT_MODE_SCALAR:
        TAOS_CHECK_EXIT(extWinProjectOpen(pOperator, pBlock));
        break;
      case EEXT_MODE_AGG:
        TAOS_CHECK_EXIT(extWinAggOpen(pOperator, pBlock));
        break;
      case EEXT_MODE_INDEFR_FUNC:
        TAOS_CHECK_EXIT(extWinIndefRowsOpen(pOperator, pBlock));
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

  TAOS_CHECK_EXIT(pOperator->fpSet._openFn(pOperator));

  if (pExtW->mode == EEXT_MODE_SCALAR || pExtW->mode == EEXT_MODE_INDEFR_FUNC) {
    TAOS_CHECK_EXIT(extWinNonAggOutputRes(pOperator, ppRes));
    if (NULL == *ppRes) {
      setOperatorCompleted(pOperator);
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
#endif      
  }

  pOperator->resultInfo.totalRows += pExtW->binfo.pRes->info.rows;
  
_exit:

  if (code) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pTaskInfo), __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM && (*ppRes)) {
    printDataBlock(*ppRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
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
                  
  if (pTaskInfo->pStreamRuntimeInfo != NULL){
    pTaskInfo->pStreamRuntimeInfo->funcInfo.withExternalWindow = true;
  }
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhynode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pExtW->binfo, pResBlock);

  pExtW->primaryTsIndex = ((SColumnNode*)pPhynode->window.pTspk)->slotId;
  pExtW->mode = pPhynode->window.pProjs ? EEXT_MODE_SCALAR : (pPhynode->window.indefRowsFunc ? EEXT_MODE_INDEFR_FUNC : EEXT_MODE_AGG);
  pExtW->binfo.inputTsOrder = pPhynode->window.node.inputTsOrder = TSDB_ORDER_ASC;
  pExtW->binfo.outputTsOrder = pExtW->binfo.inputTsOrder;

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
    }
    
    size_t keyBufSize = sizeof(int64_t) * 2 + POINTER_BYTES;
    initResultSizeInfo(&pOperator->resultInfo, 100000);
    code = blockDataEnsureCapacity(pExtW->binfo.pRes, pOperator->resultInfo.capacity);
    QUERY_CHECK_CODE(code, lino, _error);

    int32_t num = 0;
    SExprInfo* pExprInfo = NULL;
    code = createExprInfo(pPhynode->window.pFuncs, NULL, &pExprInfo, &num);
    QUERY_CHECK_CODE(code, lino, _error);
    code = initAggSup(&pOperator->exprSupp, &pExtW->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str, 0, 0);
    QUERY_CHECK_CODE(code, lino, _error);

    pExtW->pResultRow = taosMemoryCalloc(STREAM_CALC_REQ_MAX_WIN_NUM, pExtW->aggSup.resultRowSize);
    QUERY_CHECK_NULL(pExtW->pResultRow, code, lino, _error, terrno);

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
    
    pOperator->resultInfo.capacity = STREAM_CALC_REQ_MAX_WIN_NUM;
    pOperator->resultInfo.threshold = STREAM_CALC_REQ_MAX_WIN_NUM;
    pOperator->resultInfo.totalRows = 0;
    code = blockDataEnsureCapacity(pExtW->binfo.pRes, pOperator->resultInfo.capacity);
    TSDB_CHECK_CODE(code, lino, _error);
    
    int32_t    numOfExpr = 0;
    SExprInfo* pExprInfo = NULL;
    code = createExprInfo(pPhynode->window.pFuncs, NULL, &pExprInfo, &numOfExpr);
    TSDB_CHECK_CODE(code, lino, _error);
    
    code = initAggSup(&pOperator->exprSupp, &pExtW->aggSup, pExprInfo, numOfExpr, keyBufSize, pTaskInfo->id.str,
                              pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
    TSDB_CHECK_CODE(code, lino, _error);
    pOperator->exprSupp.hasWindowOrGroup = false;
    
    code = setFunctionResultOutput(pOperator, &pExtW->binfo, &pExtW->aggSup, MAIN_SCAN, numOfExpr);
    TSDB_CHECK_CODE(code, lino, _error);
    
    code = filterInitFromNode((SNode*)pNode->pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                              pTaskInfo->pStreamRuntimeInfo);
    TSDB_CHECK_CODE(code, lino, _error);
    
    pExtW->binfo.pRes = pResBlock;
    pExtW->binfo.inputTsOrder = pNode->inputTsOrder;
    pExtW->binfo.outputTsOrder = pNode->outputTsOrder;
    code = setRowTsColumnOutputInfo(pOperator->exprSupp.pCtx, numOfExpr, &pExtW->pPseudoColInfo);
    TSDB_CHECK_CODE(code, lino, _error);

  //if (pExtW->multiTableMode) {
    pExtW->pOutputBlocks = taosArrayInit(STREAM_CALC_REQ_MAX_WIN_NUM, POINTER_BYTES);
    if (!pExtW->pOutputBlocks) QUERY_CHECK_CODE(terrno, lino, _error);
  //}
  }

  pExtW->pWins = taosArrayInit(STREAM_CALC_REQ_MAX_WIN_NUM, sizeof(SExtWinTimeWindow));
  if (!pExtW->pWins) QUERY_CHECK_CODE(terrno, lino, _error);
  
  initResultRowInfo(&pExtW->binfo.resultRowInfo);

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


