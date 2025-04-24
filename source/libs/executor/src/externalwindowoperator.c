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

static int32_t blockListinit(SBlockList* pBlockList, int32_t threshold) {
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
  if (pNode) {
    SSDataBlock* pBlock = *(SSDataBlock**)pNode->data;
    if (pBlock->info.rows < pBlockList->blockRowNumThreshold) {
      *ppBlock = pBlock;
    } else {
      code = blockListAddBlock(pBlockList);
    }
  } else {
    code = blockListAddBlock(pBlockList);
  }
  if (*ppBlock == NULL && 0 == code) {
    code = blockListGetLastBlock(pBlockList, ppBlock);
  }
  return code;
}

typedef struct SExternalWindowOperator {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSupp;
  STimeWindowAggSupp twAggSup;
  SGroupResInfo      groupResInfo;
  int32_t            primaryTsIndex;
  bool               scalarMode;
  SArray*            pWins;
  int32_t            winCurIdx;
  SArray*            pOutputBlocks;  // for each window, we have a list of blocks
} SExternalWindowOperator;

typedef struct SMergeAlignedExternalWindowOperator {
  SExternalWindowOperator* pExtW;
  int64_t curTs;
  SSDataBlock* pPrefetchedBlock;
  SResultRow*  pResultRow;
} SMergeAlignedExternalWindowOperator;

void destroyMergeAlignedExternalWindowOperator(void* pOperator) {

}

void doMergeAlignExternalWindow(SOperatorInfo* pOperator);

static int32_t mergeAlignedExternalWindowNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SExecTaskInfo*                       pTaskInfo = pOperator->pTaskInfo;
  SMergeAlignedExternalWindowOperator* pMlExtInfo = pOperator->info;
  SExternalWindowOperator*             pExtW = pMlExtInfo->pExtW;
  int32_t                              code = 0;

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SSDataBlock* pRes = pExtW->binfo.pRes;
  blockDataCleanup(pRes);

  doMergeAlignExternalWindow(pOperator);
  size_t rows = pRes->info.rows;
  pOperator->resultInfo.totalRows += rows;
  (*ppRes) = (rows == 0) ? NULL : pRes;
  return code;
}


int32_t createMergeAlignedExternalWindowOperator(SOperatorInfo* pDownstream, SMergeAlignedIntervalPhysiNode* pPhynode,
                                                 SExecTaskInfo* pTaskInfo, SOperatorInfo** ppOptrOut) {
  int32_t code = 0;
  int32_t lino = 0;
  SMergeAlignedExternalWindowOperator* pMlExtInfo = taosMemoryCalloc(1, sizeof(SMergeAlignedExternalWindowOperator));
  SOperatorInfo*                       pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

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

  // filterInfo???

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
  setOperatorInfo(pOperator, "MergeAlignedExternalWindowOperator", 0, false, OP_NOT_OPENED, pMlExtInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, 0 /*next fn*/, NULL, destroyMergeAlignedExternalWindowOperator,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

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

static int32_t doOpenExternalWindow(SOperatorInfo* pOperator);
static void    destroyExternalWindowOperator(void* pOperator);

int32_t createExternalWindowOperator(SOperatorInfo* pDownstream, SIntervalPhysiNode* pPhynode, SExecTaskInfo* pTaskInfo,
                                     SOperatorInfo** pOptrOut) {
  QRY_PARAM_CHECK(pOptrOut);
  int32_t                  code = 0;
  int32_t                  lino = 0;
  SExternalWindowOperator* pExtW = taosMemoryCalloc(1, sizeof(SExternalWindowOperator));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (!pExtW || !pOperator) {
    code = terrno;
    lino = __LINE__;
    goto _error;
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhynode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pExtW->binfo, pResBlock);

  // pExtW->primaryTsIndex = slotId;

  if (pExtW->scalarMode) {
  } else {
    size_t keyBufSize = sizeof(int64_t) * 2 + POINTER_BYTES;
    initResultSizeInfo(&pOperator->resultInfo, 512);
    code = blockDataEnsureCapacity(pExtW->binfo.pRes, pOperator->resultInfo.capacity);
    QUERY_CHECK_CODE(code, lino, _error);
    // create agg funcs expr info
    // init agg sup
  }

  QUERY_CHECK_CODE(code, lino, _error);

  initResultRowInfo(&pExtW->binfo.resultRowInfo);
  setOperatorInfo(pOperator, "ExternalWindowOperator", 0 /*TODO wjm set type*/, true, OP_NOT_OPENED, pExtW, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(doOpenExternalWindow, 0 /*next fn*/, NULL, destroyExternalWindowOperator,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &pDownstream, 1);
  if (code != 0) {
    goto _error;
  }

  *pOptrOut = pOperator;
  return code;
_error:
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

static const STimeWindow* getExtWindow(SExternalWindowOperator* pExtW, TSKEY ts) {
  // TODO wjm handle desc order
  for (int32_t i = 0; i < pExtW->pWins->size; ++i) {
    const STimeWindow* pWin = taosArrayGet(pExtW->pWins, i);
    if (ts >= pWin->skey && ts < pWin->ekey) {
      pExtW->winCurIdx = i;
      return pWin;
    }
  }
  return NULL;
}

static const STimeWindow* getExtNextWindow(SOperatorInfo* pOperator) {
  SExternalWindowOperator* pExtW = pOperator->info;
  if (pExtW->winCurIdx + 1 >= pExtW->pWins->size) return NULL;
  pExtW->winCurIdx++;
  return taosArrayGet(pExtW->pWins, pExtW->winCurIdx);
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
  SExternalWindowOperator* pExtW = pOperator->info;
  SSDataBlock*             pResBlock = extWindowGetOutputBlock(pOperator, pExtW->winCurIdx);
  if (!pResBlock) {
    qError("failed to get output block for ext window:%s", tstrerror(terrno));
    return terrno;
  }
  int32_t rowsToCopy = pResBlock->info.capacity - pResBlock->info.rows;
  int32_t code = 0;
  if (rowsToCopy > 0) {
    code = blockDataMergeNRows(pResBlock, pInputBlock, startPos, rowsToCopy);
    if (code != 0) return code;
  }

  if (forwardRows > rowsToCopy) {
    code = extWindowCopyRows(pOperator, pInputBlock, startPos = rowsToCopy, forwardRows - rowsToCopy);
  }

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
  int32_t startPos = 0;  // TODO wjm filter out somerows not in current window, and do it for hashExternalWindowAgg

  TSKEY   ekey = ascScan ? pWin->ekey : pWin->skey;
  int32_t forwardRows =
      getNumOfRowsInTimeWindow(&pInputBlock->info, tsCol, startPos, ekey, binarySearchForKey, NULL, tsOrder);

  int32_t code = extWindowCopyRows(pOperator, pInputBlock, startPos, forwardRows);

  while (code == 0 && pInputBlock->info.rows > startPos + forwardRows) {
    pWin = getExtNextWindow(pOperator);
    if (!pWin) break;

    startPos = startPos + forwardRows + 1;  // TODO wjm filter out some rows not in current window
    ekey = ascScan ? pWin->ekey : pWin->skey;
    forwardRows =
        getNumOfRowsInTimeWindow(&pInputBlock->info, tsCol, startPos, ekey, binarySearchForKey, NULL, tsOrder);
    code = extWindowCopyRows(pOperator, pInputBlock, startPos, forwardRows);
  }
  return code;
}

static bool hashExternalWindowAgg(SOperatorInfo* pOperator, SSDataBlock* pInputBlock) {
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
  // TODO wjm handle limit, test desc order

  const STimeWindow* pWin = getExtWindow(pExtW, ts);
  if (pWin) {
    // TODO wjm handle null
  }
  STimeWindow win = *pWin;
  ret = setExtWindowOutputBuf(pResultRowInfo, &win, &pResult, pInputBlock->info.id.groupId, pSup->pCtx,
                              numOfOutput, pSup->rowEntryInfoOffset, &pExtW->aggSup, pTaskInfo);
  if (ret != 0 || !pResult) T_LONG_JMP(pTaskInfo->env, ret);

  TSKEY   ekey = ascScan ? win.ekey : win.skey;
  int32_t forwardRows = getNumOfRowsInTimeWindow(&pInputBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL,
                                                 pExtW->binfo.inputTsOrder);

  updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &win, 1);
  ret = extWindowDoHashAgg(pOperator, startPos, forwardRows, pInputBlock);

  if (ret != 0) {
    T_LONG_JMP(pTaskInfo->env, ret);
  }

  while (1) {
    int32_t prevEndPos = forwardRows - 1 + startPos;
    pWin = getExtNextWindow(pOperator);
    if (!pWin)
      break;
    else
      win = *pWin;
    startPos = getNextStartPos(win, &pInputBlock->info, prevEndPos, pExtW->binfo.inputTsOrder);
    if (startPos < 0) break;

    ekey = ascScan ? win.ekey : win.skey;
    forwardRows = getNumOfRowsInTimeWindow(&pInputBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL,
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
  return true;
}

static int32_t doOpenExternalWindow(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) return TSDB_CODE_SUCCESS;
  int32_t                  code = 0;
  int32_t                  lino = 0;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SOperatorInfo*           pDownstream = pOperator->pDownstream[0];
  SExternalWindowOperator* pExtW = pOperator->info;
  SExprSupp*               pSup = &pOperator->exprSupp;

  int32_t scanFlag = MAIN_SCAN;
  int64_t st = taosGetTimestampUs();

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) break;

    if (pExtW->scalarSupp.pExprInfo) {
      SExprSupp* pExprSup = &pExtW->scalarSupp;
      code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = setInputDataBlock(pSup, pBlock, pExtW->binfo.inputTsOrder, scanFlag, true);
    QUERY_CHECK_CODE(code, lino, _end);

    if (!pExtW->scalarMode) {
      if (hashExternalWindowAgg(pOperator, pBlock)) break;
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

void destroyExternalWindowOperator(void* pOperator) {}

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
  if (!pWin) {
    // TODO wjm
  }

  code = setSingleOutputTupleBuf(pResultRowInfo, pWin, &pMlExtInfo->pResultRow, pSup, &pExtW->aggSup);
  if (code != 0 || pMlExtInfo->pResultRow == NULL) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  int32_t currPos = startPos;

  while (++currPos < pBlock->info.rows) {

  }

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
      break;
    }

    pRes->info.scanFlag = pBlock->info.scanFlag;
    code = setInputDataBlock(pSup, pBlock, pExtW->binfo.inputTsOrder, pBlock->info.scanFlag, true);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pExtW->scalarMode) {

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