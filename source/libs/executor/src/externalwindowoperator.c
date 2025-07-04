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

typedef struct SBlockList {
  const SSDataBlock* pSrcBlock;
  SList*             pBlocks;
  int32_t            blockRowNumThreshold;
} SBlockList;


typedef struct SExternalWindowOperator {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSupp;
  STimeWindowAggSupp twAggSup;
  SGroupResInfo      groupResInfo;
  int32_t            primaryTsIndex;
  bool               scalarMode;
  SArray*            pWins;
  SArray*            pOutputBlocks;  // for each window, we have a list of blocks
  int32_t            outputWinId;
  SListNode*         pOutputBlockListNode;  // block index in block array used for output
  SSDataBlock*       pTmpBlock;
} SExternalWindowOperator;



static int32_t blockListInit(SBlockList* pBlockList, int32_t threshold) {
  pBlockList->pBlocks = tdListNew(sizeof(SSDataBlock*));
  if (!pBlockList->pBlocks) {
    return terrno;
  }
  return 0;
}

static int32_t blockListAddBlock(SBlockList* pBlockList) {
  SSDataBlock* pRes = NULL;
  int32_t      code = createOneDataBlock(pBlockList->pSrcBlock, false, &pRes);
  if (code != 0) {
    return code;
  }
  code = blockDataEnsureCapacity(pRes, pBlockList->blockRowNumThreshold);
  if (code != 0) {
    blockDataDestroy(pRes);
    return code;
  }
  code = tdListAppend(pBlockList->pBlocks, &pRes);
  if (code != 0) {
    blockDataDestroy(pRes);
    return code;
  }
  return 0;
}

static int32_t blockListGetLastBlock(SBlockList* pBlockList, SSDataBlock** ppBlock) {
  SListNode* pNode = TD_DLIST_TAIL(pBlockList->pBlocks);
  int32_t    code = 0;
  *ppBlock = NULL;
  code = blockListAddBlock(pBlockList);
  if (0 == code) {
    pNode = TD_DLIST_TAIL(pBlockList->pBlocks);
    *ppBlock = *(SSDataBlock**)pNode->data;
  }
  return code;
}

static void blockListDestroy(void* p) {
  SBlockList* pBlockList = (SBlockList*)p;
  if (TD_DLIST_NELES(pBlockList->pBlocks) > 0) {
    SListNode* pNode = TD_DLIST_HEAD(pBlockList->pBlocks);
    while (pNode) {
      SSDataBlock* pBlock = *(SSDataBlock**)pNode->data;
      blockDataDestroy(pBlock);
      pNode = pNode->dl_next_;
    }
  }
}



void destroyExternalWindowOperatorInfo(void* param) {
  if (NULL == param) {
    return;
  }
  SExternalWindowOperator* pInfo = (SExternalWindowOperator*)param;
  cleanupBasicInfo(&pInfo->binfo);

  taosArrayDestroyEx(pInfo->pOutputBlocks, blockListDestroy);
  taosArrayDestroy(pInfo->pWins);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  cleanupGroupResInfo(&pInfo->groupResInfo);
 
  blockDataDestroy(pInfo->pTmpBlock);

  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSupp);

  pInfo->binfo.resultRowInfo.openWindow = tdListFree(pInfo->binfo.resultRowInfo.openWindow);

  taosMemoryFreeClear(pInfo);
}


static int32_t doOpenExternalWindow(SOperatorInfo* pOperator);
static int32_t externalWindowNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);

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
    for (int32_t i = 0; i < size; ++i) {
      SSTriggerCalcParam* pParam = taosArrayGet(pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamPesudoFuncVals, i);
      STimeWindow win = {.skey = pParam->wstart, .ekey = pParam->wend};
      if (pTaskInfo->pStreamRuntimeInfo->funcInfo.triggerType != 1){  // 1 meams STREAM_TRIGGER_SLIDING
        win.ekey++;
      }
      (void)taosArrayPush(pExtW->pWins, &win);

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

  resetBasicOperatorState(&pExtW->binfo);
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
  pExtW->scalarMode = pPhynode->window.pProjs;
  pExtW->binfo.inputTsOrder = pPhynode->window.node.inputTsOrder = TSDB_ORDER_ASC;
  pExtW->binfo.outputTsOrder = pExtW->binfo.inputTsOrder;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 512);

  int32_t num = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pPhynode->window.pFuncs, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  bool scalarMode = false;

  if (scalarMode) {

  } else {
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
  taosArrayDestroyEx(pExtW->pOutputBlocks, blockListDestroy);
  taosArrayDestroy(pExtW->pWins);
  pExtW->pWins = NULL;
  pExtW->pOutputBlockListNode = NULL;
  pExtW->pOutputBlocks = NULL;
  initResultSizeInfo(&pOperator->resultInfo, 512);
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
  if (code == 0) {
    cleanupGroupResInfo(&pExtW->groupResInfo);
  }
  blockDataDestroy(pExtW->pTmpBlock);
  pExtW->pTmpBlock = NULL;
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
  if (pTaskInfo->pStreamRuntimeInfo != NULL){
    pTaskInfo->pStreamRuntimeInfo->funcInfo.withExternalWindow = true;
  }
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhynode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pExtW->binfo, pResBlock);

  pExtW->primaryTsIndex = ((SColumnNode*)pPhynode->window.pTspk)->slotId;
  pExtW->scalarMode = pPhynode->window.pProjs;
  pExtW->binfo.inputTsOrder = pPhynode->window.node.inputTsOrder = TSDB_ORDER_ASC;
  pExtW->binfo.outputTsOrder = pExtW->binfo.inputTsOrder;

  if (pExtW->scalarMode) {
  } else {
    size_t keyBufSize = sizeof(int64_t) * 2 + POINTER_BYTES;
    initResultSizeInfo(&pOperator->resultInfo, 512);
    code = blockDataEnsureCapacity(pExtW->binfo.pRes, pOperator->resultInfo.capacity);
    QUERY_CHECK_CODE(code, lino, _error);

    int32_t num = 0;
    SExprInfo* pExprInfo = NULL;
    code = createExprInfo(pPhynode->window.pFuncs, NULL, &pExprInfo, &num);
    QUERY_CHECK_CODE(code, lino, _error);
    code = initAggSup(&pOperator->exprSupp, &pExtW->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str, 0, 0);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  if (pPhynode->window.pProjs) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pPhynode->window.pProjs, NULL, &pScalarExprInfo, &numOfScalarExpr);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pExtW->scalarSupp, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  QUERY_CHECK_CODE(code, lino, _error);

  code = initExecTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultRowInfo(&pExtW->binfo.resultRowInfo);
  setOperatorInfo(pOperator, "ExternalWindowOperator", QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW, true, OP_NOT_OPENED,
                  pExtW, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(doOpenExternalWindow, externalWindowNext, NULL, destroyExternalWindowOperatorInfo,
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

int64_t* extractTsCol(SSDataBlock* pBlock, int32_t primaryTsIndex, SExecTaskInfo* pTaskInfo) {
  TSKEY* tsCols = NULL;

  if (pBlock->pDataBlock != NULL && pBlock->info.dataLoad) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, primaryTsIndex);
    if (!pColDataInfo) {
      pTaskInfo->code = terrno;
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    tsCols = (int64_t*)pColDataInfo->pData;
    if (tsCols[0] == 0) {
      qWarn("%s at line %d.block start ts:%" PRId64 ",end ts:%" PRId64, __func__, __LINE__, tsCols[0],
            tsCols[pBlock->info.rows - 1]);
    }

    if (tsCols[0] != 0 && (pBlock->info.window.skey == 0 && pBlock->info.window.ekey == 0)) {
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

static int32_t getExtWinCurIdx(SExecTaskInfo* pTaskInfo) {
  return pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx;
}

static void incExtWinCurIdx(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx++;
}

static void incExtWinOutIdx(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  pTaskInfo->pStreamRuntimeInfo->funcInfo.curOutIdx++;
}

static const STimeWindow* getExtWindow(SExternalWindowOperator* pExtW, TSKEY ts) {
  // TODO handle desc order
  for (int32_t i = 0; i < pExtW->pWins->size; ++i) {
    const STimeWindow* pWin = taosArrayGet(pExtW->pWins, i);
    if (ts >= pWin->skey && ts < pWin->ekey) {
      return pWin;
    }
  }
  return NULL;
}

static const STimeWindow* getExtNextWindow(SExternalWindowOperator* pExtW, SExecTaskInfo* pTaskInfo) {
  int32_t curIdx = getExtWinCurIdx(pTaskInfo);
  if (curIdx + 1 >= pExtW->pWins->size) return NULL;
  return taosArrayGet(pExtW->pWins, curIdx + 1);
}

static int32_t getNextStartPos(STimeWindow win, const SDataBlockInfo* pBlockInfo, int32_t lastEndPos, int32_t order) {
  bool ascQuery = order == TSDB_ORDER_ASC;

  if (win.skey > pBlockInfo->window.ekey && ascQuery) return -1;
  if (win.ekey < pBlockInfo->window.skey && !ascQuery) return -1;
  return lastEndPos + 1;
}

static int32_t setExtWindowOutputBuf(SResultRowInfo* pResultRowInfo, STimeWindow* win,                                      SResultRow** pResult, int64_t tableGroupId, SqlFunctionCtx* pCtx,
                                     int32_t numOfOutput, int32_t* rowEntryInfoOffset, SAggSupporter* pAggSup,
                                     SExecTaskInfo* pTaskInfo) {
  SResultRow* pResultRow = doSetResultOutBufByKey(pAggSup->pResultBuf, pResultRowInfo, (char*)&win->skey, TSDB_KEYSIZE,
                                                  true, tableGroupId, pTaskInfo, true, pAggSup, true);

  if (pResultRow == NULL || pTaskInfo->code != 0) {
    *pResult = NULL;
    qError("failed to set result output buffer, error:%s", tstrerror(pTaskInfo->code));
    return pTaskInfo->code;
  }

  // set time window for current result
  TAOS_SET_POBJ_ALIGNED(&pResultRow->win, win);
  *pResult = pResultRow;
  return setResultRowInitCtx(pResultRow, pCtx, numOfOutput, rowEntryInfoOffset);
}

static int32_t extWindowDoHashAgg(SOperatorInfo* pOperator, int32_t startPos, int32_t forwardRows,
                                  SSDataBlock* pInputBlock) {
  SExprSupp*               pSup = &pOperator->exprSupp;
  SExternalWindowOperator* pExtW = pOperator->info;
  return applyAggFunctionOnPartialTuples(pOperator->pTaskInfo, pSup->pCtx, &pExtW->twAggSup.timeWindowData, startPos,
                                         forwardRows, pInputBlock->info.rows, pSup->numOfExprs);
}

static SSDataBlock* extWindowGetOutputBlock(SOperatorInfo* pOperator, int32_t winIdx) {
  SExternalWindowOperator* pExtW = pOperator->info;
  SSDataBlock*             pBlock = NULL;
  SBlockList*              pBlockList = taosArrayGet(pExtW->pOutputBlocks, winIdx);
  int32_t                  code = blockListGetLastBlock(pBlockList, &pBlock);
  if (code != 0) terrno = code;
  return pBlock;
}

static int32_t extWindowCopyRows(SOperatorInfo* pOperator, SSDataBlock* pInputBlock, int32_t startPos,
                                 int32_t forwardRows) {
  if (forwardRows == 0) return 0;
  SExternalWindowOperator* pExtW = pOperator->info;
  SSDataBlock*             pResBlock = extWindowGetOutputBlock(pOperator, getExtWinCurIdx(pOperator->pTaskInfo));
  SExprSupp*               pExprSup = &pExtW->scalarSupp;

  if (!pResBlock) {
    qError("failed to get output block for ext window:%s", tstrerror(terrno));
    return terrno;
  }
  int32_t rowsToCopy = forwardRows;
  int32_t code = 0;
  if (!pExtW->pTmpBlock)
    code = createOneDataBlock(pInputBlock, false, &pExtW->pTmpBlock);
  else
    blockDataCleanup(pExtW->pTmpBlock);
  if (code) {
    qError("failed to create datablock:%s", tstrerror(terrno));
    return code;
  }
  code = blockDataEnsureCapacity(pExtW->pTmpBlock, TMAX(1, rowsToCopy));
  if (code) {
    qError("failed to ensure capacity:%s", tstrerror(terrno));
    return code;
  }
  if (rowsToCopy > 0) {
    code = blockDataMergeNRows(pExtW->pTmpBlock, pInputBlock, startPos, rowsToCopy);
    if (code == 0) {
      code = projectApplyFunctions(pExprSup->pExprInfo, pResBlock, pExtW->pTmpBlock, pExprSup->pCtx, pExprSup->numOfExprs,
          NULL, GET_STM_RTINFO(pOperator->pTaskInfo));
    }
  }

  if (code != 0) return code;

  return code;
}

static int32_t hashExternalWindowProject(SOperatorInfo* pOperator, SSDataBlock* pInputBlock) {
  SExprSupp*               pSup = &pOperator->exprSupp;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SExternalWindowOperator* pExtW = pOperator->info;
  SqlFunctionCtx*          pCtx = NULL;
  int64_t*                 tsCol = extractTsCol(pInputBlock, pExtW->primaryTsIndex, pTaskInfo);
  TSKEY                    ts = getStartTsKey(&pInputBlock->info.window, tsCol);
  const STimeWindow*       pWin = getExtWindow(pExtW, ts);
  bool                     ascScan = pExtW->binfo.inputTsOrder == TSDB_ORDER_ASC;
  int32_t                  tsOrder = pExtW->binfo.inputTsOrder;
  int32_t startPos = 0;

  if (!pWin) return 0;
  TSKEY   ekey = ascScan ? pWin->ekey : pWin->skey;
  int32_t forwardRows =
      getNumOfRowsInTimeWindow(&pInputBlock->info, tsCol, startPos, ekey-1, binarySearchForKey, NULL, tsOrder);

  int32_t code = extWindowCopyRows(pOperator, pInputBlock, startPos, forwardRows);

  while (code == 0 && pInputBlock->info.rows > startPos + forwardRows) {
    pWin = getExtNextWindow(pExtW, pTaskInfo);
    if (!pWin) break;
    incExtWinCurIdx(pOperator);

    startPos = startPos + forwardRows;
    ekey = ascScan ? pWin->ekey : pWin->skey;
    forwardRows =
        getNumOfRowsInTimeWindow(&pInputBlock->info, tsCol, startPos, ekey-1, binarySearchForKey, NULL, tsOrder);
    code = extWindowCopyRows(pOperator, pInputBlock, startPos, forwardRows);
  }
  return code;
}

static void hashExternalWindowAgg(SOperatorInfo* pOperator, SSDataBlock* pInputBlock) {
  SExternalWindowOperator* pExtW = (SExternalWindowOperator*)pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*               pSup = &pOperator->exprSupp;
  SResultRowInfo*          pResultRowInfo = &pExtW->binfo.resultRowInfo;
  int32_t                  startPos = 0;
  int32_t                  numOfOutput = pSup->numOfExprs;
  int64_t*                 tsCols = extractTsCol(pInputBlock, pExtW->primaryTsIndex, pTaskInfo);
  bool                     ascScan = pExtW->binfo.inputTsOrder == TSDB_ORDER_ASC;
  TSKEY                    ts = getStartTsKey(&pInputBlock->info.window, tsCols);
  SResultRow*              pResult = NULL;
  int32_t                  ret = 0;

  const STimeWindow* pWin = getExtWindow(pExtW, ts);
  if (pWin == NULL) {
    qError("failed to get time window for ts:%" PRId64 ", error:%s", ts, tstrerror(terrno));
    return;
  }
  qDebug("ext window1 start:%" PRId64 ", end:%" PRId64 ", ts:%" PRId64 ", ascScan:%d",
         pWin->skey, pWin->ekey, ts, ascScan);
  STimeWindow win = *pWin;
  ret = setExtWindowOutputBuf(pResultRowInfo, &win, &pResult, pInputBlock->info.id.groupId, pSup->pCtx,
                              numOfOutput, pSup->rowEntryInfoOffset, &pExtW->aggSup, pTaskInfo);
  if (ret != 0 || !pResult) T_LONG_JMP(pTaskInfo->env, ret);
  TSKEY   ekey = ascScan ? win.ekey : win.skey;
  int32_t forwardRows = getNumOfRowsInTimeWindow(&pInputBlock->info, tsCols, startPos, ekey - 1, binarySearchForKey, NULL,
                                                 pExtW->binfo.inputTsOrder);

  updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &win, 1);
  ret = extWindowDoHashAgg(pOperator, startPos, forwardRows, pInputBlock);

  if (ret != 0) {
    T_LONG_JMP(pTaskInfo->env, ret);
  }

  while (1) {
    int32_t prevEndPos = forwardRows + startPos - 1;
    pWin = getExtNextWindow(pExtW, pTaskInfo);
    if (!pWin)
      break;
    else
      win = *pWin;
    qDebug("ext window2 start:%" PRId64 ", end:%" PRId64 ", ts:%" PRId64 ", ascScan:%d",
           win.skey, win.ekey, ts, ascScan);
    startPos = getNextStartPos(win, &pInputBlock->info, prevEndPos, pExtW->binfo.inputTsOrder);
    if (startPos < 0) break;
    incExtWinCurIdx(pOperator);

    ekey = ascScan ? win.ekey : win.skey;
    forwardRows = getNumOfRowsInTimeWindow(&pInputBlock->info, tsCols, startPos, ekey - 1, binarySearchForKey, NULL,
                                           pExtW->binfo.inputTsOrder);

    ret = setExtWindowOutputBuf(pResultRowInfo, &win, &pResult, pInputBlock->info.id.groupId, pSup->pCtx,
                                numOfOutput, pSup->rowEntryInfoOffset, &pExtW->aggSup, pTaskInfo);
    if (ret != 0 || !pResult) T_LONG_JMP(pTaskInfo->env, ret);

    updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &win, 1);
    ret = extWindowDoHashAgg(pOperator, startPos, forwardRows, pInputBlock);
    if (ret != 0) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }
  }
}

static int32_t doOpenExternalWindow(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) return TSDB_CODE_SUCCESS;
  int32_t                  code = 0;
  int32_t                  lino = 0;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SOperatorInfo*           pDownstream = pOperator->pDownstream[0];
  SExternalWindowOperator* pExtW = pOperator->info;
  SExprSupp*               pSup = &pOperator->exprSupp;
  pTaskInfo->pStreamRuntimeInfo->funcInfo.extWinProjMode = pExtW->scalarMode;

  int32_t scanFlag = MAIN_SCAN;
  int64_t st = taosGetTimestampUs();

  if (!pExtW->pWins) {
    size_t size = taosArrayGetSize(pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamPesudoFuncVals);
    pExtW->pWins = taosArrayInit(size, sizeof(STimeWindow));
    if (!pExtW->pWins) QUERY_CHECK_CODE(terrno, lino, _end);
    pExtW->pOutputBlocks = taosArrayInit(size, sizeof(SBlockList));
    if (!pExtW->pOutputBlocks) QUERY_CHECK_CODE(terrno, lino, _end);
    for (int32_t i = 0; i < size; ++i) {
      SSTriggerCalcParam* pParam = taosArrayGet(pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamPesudoFuncVals, i);
      STimeWindow win = {.skey = pParam->wstart, .ekey = pParam->wend};
      if (pTaskInfo->pStreamRuntimeInfo->funcInfo.triggerType != 1){  // 1 meams STREAM_TRIGGER_SLIDING
        win.ekey++;
      }
      (void)taosArrayPush(pExtW->pWins, &win);

      SBlockList bl = {.pSrcBlock = pExtW->binfo.pRes, .pBlocks = 0, .blockRowNumThreshold = 4096};
      code = blockListInit(&bl, 4096);
      if (code != 0) QUERY_CHECK_CODE(code, lino, _end);
      (void)taosArrayPush(pExtW->pOutputBlocks, &bl);
    }
    pExtW->outputWinId = pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx;
  }

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) break;

    code = setInputDataBlock(pSup, pBlock, pExtW->binfo.inputTsOrder, scanFlag, true);
    QUERY_CHECK_CODE(code, lino, _end);

    printDataBlock(pBlock, __func__, "externalwindow");

    qDebug("ext windowpExtW->scalarMode:%d", pExtW->scalarMode);
    if (!pExtW->scalarMode) {
      hashExternalWindowAgg(pOperator, pBlock);
    } else {
      // scalar mode, no need to do agg, just output rows partitioned by window
      code = hashExternalWindowProject(pOperator, pBlock);
      if (code != 0) goto _end;
    }

    OPTR_SET_OPENED(pOperator);
  }

  code = initGroupedResultInfo(&pExtW->groupResInfo, pExtW->aggSup.pResultRowHashTable, pExtW->binfo.inputTsOrder);
  QUERY_CHECK_CODE(code, lino, _end);
_end:
  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static int32_t externalWindowNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                  code = 0;
  int32_t                  lino = 0;
  SExternalWindowOperator* pExtW = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*               pSup = &pOperator->exprSupp;

  if (pOperator->status == OP_EXEC_DONE) {
    *ppRes = NULL;
    return code;
  }

  SSDataBlock* pBlock = pExtW->binfo.pRes;
  blockDataCleanup(pBlock);
  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

  while (1) {
    if (pExtW->scalarMode) {
      if (pExtW->outputWinId >= taosArrayGetSize(pExtW->pOutputBlocks)) {
        pBlock->info.rows = 0;
        break;
      }
      SBlockList* pList = taosArrayGet(pExtW->pOutputBlocks, pExtW->outputWinId);
      if (pExtW->pOutputBlockListNode == NULL) {
        pExtW->pOutputBlockListNode = tdListGetHead(pList->pBlocks);
      } else {
        if (!pExtW->pOutputBlockListNode->dl_next_) {
          pExtW->pOutputBlockListNode = NULL;
          pExtW->outputWinId++;
          incExtWinOutIdx(pOperator);
          continue;
        }
        pExtW->pOutputBlockListNode = pExtW->pOutputBlockListNode->dl_next_;
      }

      if (pExtW->pOutputBlockListNode) {
        code = blockDataMerge(pBlock, *(SSDataBlock**)pExtW->pOutputBlockListNode->data);
        if (code != 0) goto _end;
      } else {
        pExtW->outputWinId++;
        incExtWinOutIdx(pOperator);
        continue;
      }
      break;
    } else {
      doBuildResultDatablock(pOperator, &pExtW->binfo, &pExtW->groupResInfo, pExtW->aggSup.pResultBuf);
      bool hasRemain = hasRemainResults(&pExtW->groupResInfo);
      if (!hasRemain) {
        setOperatorCompleted(pOperator);
        break;
      }
      if (pExtW->binfo.pRes->info.rows > 0) break;
    }
  }

  pOperator->resultInfo.totalRows += pExtW->binfo.pRes->info.rows;
_end:
  if (code != 0) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) = (pExtW->binfo.pRes->info.rows == 0) ? NULL : pExtW->binfo.pRes;
  return code;
}

static int32_t setSingleOutputTupleBuf(SResultRowInfo* pResultRowInfo, const STimeWindow* pWin, SResultRow** pResult,
                                       SExprSupp* pExprSup, SAggSupporter* pAggSup) {
  if (*pResult == NULL) {
    *pResult = getNewResultRow(pAggSup->pResultBuf, &pAggSup->currentPageId, pAggSup->resultRowSize);
    if (!*pResult) {
      return terrno;
    }
    pResultRowInfo->cur = (SResultRowPosition){.pageId = (*pResult)->pageId, .offset = (*pResult)->offset};
  }
  (*pResult)->win = *pWin;
  return setResultRowInitCtx((*pResult), pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
}

static int32_t doMergeAlignExtWindowAgg(SOperatorInfo* pOperator, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock, SSDataBlock* pResultBlock) {
  SMergeAlignedExternalWindowOperator* pMlExtInfo = pOperator->info;
  SExternalWindowOperator*             pExtW = pMlExtInfo->pExtW;

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*     pSup = &pOperator->exprSupp;
  int32_t        code = 0;

  int32_t startPos = 0;
  int64_t* tsCols = extractTsCol(pBlock, pExtW->primaryTsIndex, pTaskInfo);
  TSKEY ts = getStartTsKey(&pBlock->info.window, tsCols);

  const STimeWindow *pWin = getExtWindow(pExtW, ts);
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

    pWin = getExtNextWindow(pExtW, pTaskInfo);
    if (!pWin) break;
    qDebug("ext window align2 start:%" PRId64 ", end:%" PRId64, pWin->skey, pWin->ekey);
    incExtWinCurIdx(pOperator);
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
      if (pMlExtInfo->curTs != INT64_MIN && !pExtW->scalarMode) {
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
    qDebug("ext windowpExtWAlign->scalarMode:%d", pExtW->scalarMode);


    if (pExtW->scalarMode) {
      code = doMergeAlignExtWindowProject(pOperator, &pExtW->binfo.resultRowInfo, pBlock, pRes);
    } else {
      code = doMergeAlignExtWindowAgg(pOperator, &pExtW->binfo.resultRowInfo, pBlock, pRes);
    }
  }
_end:
  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}
