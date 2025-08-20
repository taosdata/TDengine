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

typedef struct SExternalWindowOperator {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSupp;
  STimeWindowAggSupp twAggSup;
  SGroupResInfo      groupResInfo;
  int32_t            primaryTsIndex;
  EExtWinMode        mode;
  SArray*            pWins;
  SArray*            pOutputBlocks;  // for each window, we have a list of blocks
  // SArray*            pOffsetList;  // for each window
  int32_t            lastWinId;
  int32_t            outputWinId;
  SListNode*         pOutputBlockListNode;  // block index in block array used for output
  SSDataBlock*       pTmpBlock;
  SSDataBlock*       pEmptyInputBlock;
  SArray*            pPseudoColInfo;  
  bool               hasCountFunc;
  // SLimitInfo         limitInfo;  // limit info for each window
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
  taosMemoryFree(pBlockList->pBlocks);
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
 
  taosArrayDestroy(pInfo->pPseudoColInfo);
  blockDataDestroy(pInfo->pTmpBlock);
  blockDataDestroy(pInfo->pEmptyInputBlock);

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
  initResultSizeInfo(&pOperator->resultInfo, 512);

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
  taosArrayDestroyEx(pExtW->pOutputBlocks, blockListDestroy);
  // taosArrayDestroy(pExtW->pOffsetList);
  taosArrayDestroy(pExtW->pWins);
  pExtW->pWins = NULL;
  pExtW->pOutputBlockListNode = NULL;
  pExtW->pOutputBlocks = NULL;
  // pExtW->pOffsetList = NULL;
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

static EDealRes extWindowHasCountLikeFunc(SNode* pNode, void* res) {
  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (fmIsCountLikeFunc(pFunc->funcId) || (pFunc->hasOriginalFunc && fmIsCountLikeFunc(pFunc->originalFuncId))) {
      *(bool*)res = true;
      return DEAL_RES_END;
    }
  }
  return DEAL_RES_CONTINUE;
}


static int32_t extWindowCreateEmptyInputBlock(SOperatorInfo* pOperator, SSDataBlock** ppBlock) {
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
    initResultSizeInfo(&pOperator->resultInfo, 512);
    code = blockDataEnsureCapacity(pExtW->binfo.pRes, pOperator->resultInfo.capacity);
    QUERY_CHECK_CODE(code, lino, _error);

    int32_t num = 0;
    SExprInfo* pExprInfo = NULL;
    code = createExprInfo(pPhynode->window.pFuncs, NULL, &pExprInfo, &num);
    QUERY_CHECK_CODE(code, lino, _error);
    code = initAggSup(&pOperator->exprSupp, &pExtW->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str, 0, 0);
    QUERY_CHECK_CODE(code, lino, _error);

    nodesWalkExprs(pPhynode->window.pFuncs, extWindowHasCountLikeFunc, &pExtW->hasCountFunc);
    if (pExtW->hasCountFunc) {
      code = extWindowCreateEmptyInputBlock(pOperator, &pExtW->pEmptyInputBlock);
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
    
    initResultSizeInfo(&pOperator->resultInfo, 512);
    code = blockDataEnsureCapacity(pResBlock, 512);
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
  }

  initResultRowInfo(&pExtW->binfo.resultRowInfo);

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

static void setExtWinCurIdx(SOperatorInfo* pOperator, int32_t idx) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx = idx;
}


static void incExtWinOutIdx(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  pTaskInfo->pStreamRuntimeInfo->funcInfo.curOutIdx++;
}

static const STimeWindow* getExtWindow(SOperatorInfo* pOperator, SExternalWindowOperator* pExtW, TSKEY ts) {
  // TODO handle desc order
  for (int32_t i = 0; i < pExtW->pWins->size; ++i) {
    const STimeWindow* pWin = taosArrayGet(pExtW->pWins, i);
    if (ts >= pWin->skey && ts < pWin->ekey) {
      setExtWinCurIdx(pOperator, i);
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

static int32_t getNextStartPos(STimeWindow win, const SDataBlockInfo* pBlockInfo, int32_t lastEndPos, int32_t order, int32_t* nextPos, int64_t* tsCol) {
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

static int32_t setExtWindowOutputBuf(SResultRowInfo* pResultRowInfo, const STimeWindow* win, SResultRow** pResult, int64_t tableGroupId, SqlFunctionCtx* pCtx,
                                     int32_t numOfOutput, int32_t* rowEntryInfoOffset, SAggSupporter* pAggSup,
                                     SExecTaskInfo* pTaskInfo) {
  SResultRow* pResultRow = doSetResultOutBufByKey(pAggSup->pResultBuf, pResultRowInfo, (char*)&win->skey, TSDB_KEYSIZE,
                                                  true, tableGroupId, pTaskInfo, true, pAggSup, true);

  if (pResultRow == NULL || pTaskInfo->code != 0) {
    *pResult = NULL;
    qError("failed to set result output buffer, error:%s", tstrerror(pTaskInfo->code));
    return pTaskInfo->code;
  }

  qDebug("current result rows num:%d", tSimpleHashGetSize(pAggSup->pResultRowHashTable));

  // set time window for current result
  TAOS_SET_POBJ_ALIGNED(&pResultRow->win, win);
  *pResult = pResultRow;
  pTaskInfo->code = setResultRowInitCtx(pResultRow, pCtx, numOfOutput, rowEntryInfoOffset);
  if (pTaskInfo->code) {
    *pResult = NULL;
    qError("failed to set result row ctx, error:%s", tstrerror(pTaskInfo->code));
    return pTaskInfo->code;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extWindowDoHashAgg(SOperatorInfo* pOperator, int32_t startPos, int32_t forwardRows,
                                  SSDataBlock* pInputBlock) {
  if (forwardRows == 0) return 0;
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
    qError("%s failed to get output block for ext window:%s", GET_TASKID(pOperator->pTaskInfo), tstrerror(terrno));
    return terrno;
  }
  int32_t rowsToCopy = forwardRows;
  int32_t code = 0;
  if (!pExtW->pTmpBlock)
    code = createOneDataBlock(pInputBlock, false, &pExtW->pTmpBlock);
  else
    blockDataCleanup(pExtW->pTmpBlock);
  if (code) {
    qError("%s failed to create datablock:%s", GET_TASKID(pOperator->pTaskInfo), tstrerror(terrno));
    return code;
  }
  code = blockDataEnsureCapacity(pExtW->pTmpBlock, TMAX(1, rowsToCopy));
  if (code) {
    qError("%s failed to ensure capacity:%s", GET_TASKID(pOperator->pTaskInfo), tstrerror(terrno));
    return code;
  }
  if (rowsToCopy > 0) {
    code = blockDataMergeNRows(pExtW->pTmpBlock, pInputBlock, startPos, rowsToCopy);
    if (code == 0) {
      code = projectApplyFunctionsWithSelect(pExprSup->pExprInfo, pResBlock, pExtW->pTmpBlock, pExprSup->pCtx, pExprSup->numOfExprs,
          NULL, GET_STM_RTINFO(pOperator->pTaskInfo), true);
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
  const STimeWindow*       pWin = getExtWindow(pOperator, pExtW, ts);
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

static int32_t extWindowDoIndefRows(SOperatorInfo* pOperator, SSDataBlock* pRes, SSDataBlock* pBlock) {
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


static int32_t extWindowHandleIndefRows(SOperatorInfo* pOperator, SSDataBlock* pInputBlock, int32_t startPos,
                                 int32_t forwardRows) {
  if (forwardRows == 0) return 0;
  SExternalWindowOperator* pExtW = pOperator->info;
  SSDataBlock*             pResBlock = extWindowGetOutputBlock(pOperator, getExtWinCurIdx(pOperator->pTaskInfo));

  if (!pResBlock) {
    qError("%s failed to get output block for ext window:%s", GET_TASKID(pOperator->pTaskInfo), tstrerror(terrno));
    return terrno;
  }
  
  int32_t rowsToCopy = forwardRows;
  int32_t code = 0;
  if (!pExtW->pTmpBlock)
    code = createOneDataBlock(pInputBlock, false, &pExtW->pTmpBlock);
  else
    blockDataCleanup(pExtW->pTmpBlock);
    
  if (code) {
    qError("%s failed to create datablock:%s", GET_TASKID(pOperator->pTaskInfo), tstrerror(terrno));
    return code;
  }
  code = blockDataEnsureCapacity(pExtW->pTmpBlock, TMAX(1, rowsToCopy));
  if (code) {
    qError("%s failed to ensure capacity:%s", GET_TASKID(pOperator->pTaskInfo), tstrerror(terrno));
    return code;
  }
  
  if (rowsToCopy > 0) {
    code = blockDataMergeNRows(pExtW->pTmpBlock, pInputBlock, startPos, rowsToCopy);
    if (code == 0) {
      code = extWindowDoIndefRows(pOperator, pResBlock, pExtW->pTmpBlock);
    }
  }

  return code;
}


static int32_t hashExternalWindowIndefRows(SOperatorInfo* pOperator, SSDataBlock* pInputBlock) {
  SExprSupp*               pSup = &pOperator->exprSupp;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SExternalWindowOperator* pExtW = pOperator->info;
  SqlFunctionCtx*          pCtx = NULL;
  int64_t*                 tsCol = extractTsCol(pInputBlock, pExtW->primaryTsIndex, pTaskInfo);
  TSKEY                    ts = getStartTsKey(&pInputBlock->info.window, tsCol);
  const STimeWindow*       pWin = getExtWindow(pOperator, pExtW, ts);
  bool                     ascScan = pExtW->binfo.inputTsOrder == TSDB_ORDER_ASC;
  int32_t                  tsOrder = pExtW->binfo.inputTsOrder;
  int32_t                  startPos = 0;

  if (!pWin) return 0;
  TSKEY   ekey = ascScan ? pWin->ekey : pWin->skey;
  int32_t forwardRows =
      getNumOfRowsInTimeWindow(&pInputBlock->info, tsCol, startPos, ekey-1, binarySearchForKey, NULL, tsOrder);

  int32_t code = extWindowHandleIndefRows(pOperator, pInputBlock, startPos, forwardRows);

  while (code == 0 && pInputBlock->info.rows > startPos + forwardRows) {
    pWin = getExtNextWindow(pExtW, pTaskInfo);
    if (!pWin) break;
    incExtWinCurIdx(pOperator);

    startPos = startPos + forwardRows;
    ekey = ascScan ? pWin->ekey : pWin->skey;
    forwardRows =
        getNumOfRowsInTimeWindow(&pInputBlock->info, tsCol, startPos, ekey-1, binarySearchForKey, NULL, tsOrder);
    code = extWindowHandleIndefRows(pOperator, pInputBlock, startPos, forwardRows);
  }
  
  return code;
}


static int32_t extWindowProcessEmptyWins(SOperatorInfo* pOperator, SSDataBlock* pBlock, bool allRemains) {
  int32_t code = 0, lino = 0;
  SExternalWindowOperator* pExtW = (SExternalWindowOperator*)pOperator->info;
  bool ascScan = pExtW->binfo.inputTsOrder == TSDB_ORDER_ASC;
  int32_t currIdx = getExtWinCurIdx(pOperator->pTaskInfo);
  SExprSupp* pSup = &pOperator->exprSupp;

  if (NULL == pExtW->pEmptyInputBlock) {
    goto _exit;
  }

  int32_t endIdx = allRemains ? (pExtW->pWins->size - 1) : (currIdx - 1);
  SResultRowInfo* pResultRowInfo = &pExtW->binfo.resultRowInfo;
  SResultRow* pResult = NULL;
  SSDataBlock* pInput = pExtW->pEmptyInputBlock;

  if ((pExtW->lastWinId + 1) <= endIdx) {
    TAOS_CHECK_EXIT(setInputDataBlock(pSup, pExtW->pEmptyInputBlock, pExtW->binfo.inputTsOrder, MAIN_SCAN, true));
  }
  
  for (int32_t i = pExtW->lastWinId + 1; i <= endIdx; ++i) {
    const STimeWindow* pWin = taosArrayGet(pExtW->pWins, i);

    setExtWinCurIdx(pOperator, i);
    qDebug("%s %dth ext empty window start:%" PRId64 ", end:%" PRId64 ", ascScan:%d",
           GET_TASKID(pOperator->pTaskInfo), i, pWin->skey, pWin->ekey, ascScan);

    TAOS_CHECK_EXIT(setExtWindowOutputBuf(pResultRowInfo, pWin, &pResult, pInput->info.id.groupId, pSup->pCtx, pSup->numOfExprs,
                                pSup->rowEntryInfoOffset, &pExtW->aggSup, pOperator->pTaskInfo));

    updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, pWin, 1);
    code = extWindowDoHashAgg(pOperator, 0, 1, pInput);
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
      setExtWinCurIdx(pOperator, currIdx);  
    }
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

  const STimeWindow* pWin = getExtWindow(pOperator, pExtW, ts);
  if (pWin == NULL) {
    qError("%s failed to get time window for ts:%" PRId64 ", error:%s", GET_TASKID(pOperator->pTaskInfo), ts, tstrerror(terrno));
    return;
  }

  ret = extWindowProcessEmptyWins(pOperator, pInputBlock, false);
  if (ret != 0) {
    T_LONG_JMP(pTaskInfo->env, ret);
  }
  
  qDebug("%s ext window1 start:%" PRId64 ", end:%" PRId64 ", ts:%" PRId64 ", ascScan:%d",
         GET_TASKID(pOperator->pTaskInfo), pWin->skey, pWin->ekey, ts, ascScan);        

  SExprSupp* pScalarSup = &pExtW->scalarSupp;
  if (pScalarSup->pExprInfo != NULL) {
    ret = projectApplyFunctions(pScalarSup->pExprInfo, pInputBlock, pInputBlock, pScalarSup->pCtx, pScalarSup->numOfExprs,
                                 pExtW->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo));
    if (ret != 0) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }
  }

  STimeWindow win = *pWin;
  ret = setExtWindowOutputBuf(pResultRowInfo, &win, &pResult, pInputBlock->info.id.groupId, pSup->pCtx, numOfOutput,
                              pSup->rowEntryInfoOffset, &pExtW->aggSup, pTaskInfo);
  if (ret != 0 || !pResult) {
    T_LONG_JMP(pTaskInfo->env, ret);
  }
  TSKEY   ekey = ascScan ? win.ekey : win.skey;
  int32_t forwardRows = getNumOfRowsInTimeWindow(&pInputBlock->info, tsCols, startPos, ekey - 1, binarySearchForKey, NULL,
                                                 pExtW->binfo.inputTsOrder);

  updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &win, 1);
  ret = extWindowDoHashAgg(pOperator, startPos, forwardRows, pInputBlock);
  pExtW->lastWinId = getExtWinCurIdx(pTaskInfo);

  if (ret != 0) {
    T_LONG_JMP(pTaskInfo->env, ret);
  }

  int32_t nextPosGot = 0;
  while (1) {
    int32_t prevEndPos = forwardRows + startPos - 1;
    if (prevEndPos >= pInputBlock->info.rows) {
      break;
    }
    
    pWin = getExtNextWindow(pExtW, pTaskInfo);
    if (!pWin)
      break;
    else
      win = *pWin;
      
    qDebug("%s ext window2 start:%" PRId64 ", end:%" PRId64 ", ts:%" PRId64 ", ascScan:%d",
           GET_TASKID(pOperator->pTaskInfo), win.skey, win.ekey, ts, ascScan);
           
    nextPosGot = getNextStartPos(win, &pInputBlock->info, prevEndPos, pExtW->binfo.inputTsOrder, &startPos, tsCols);
    if (-1 == nextPosGot) {
      qDebug("%s ignore current block", GET_TASKID(pOperator->pTaskInfo));
      break;
    }
    if (-2 == nextPosGot) {
      qDebug("%s skip current window", GET_TASKID(pOperator->pTaskInfo));
      incExtWinCurIdx(pOperator);
      continue;
    }
    
    incExtWinCurIdx(pOperator);

    ret = extWindowProcessEmptyWins(pOperator, pInputBlock, false);
    if (ret != 0) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }

    ekey = ascScan ? win.ekey : win.skey;
    forwardRows = getNumOfRowsInTimeWindow(&pInputBlock->info, tsCols, startPos, ekey - 1, binarySearchForKey, NULL,
                                           pExtW->binfo.inputTsOrder);

    ret = setExtWindowOutputBuf(pResultRowInfo, &win, &pResult, pInputBlock->info.id.groupId, pSup->pCtx,
                                numOfOutput, pSup->rowEntryInfoOffset, &pExtW->aggSup, pTaskInfo);
    if (ret != 0 || !pResult) T_LONG_JMP(pTaskInfo->env, ret);

    updateTimeWindowInfo(&pExtW->twAggSup.timeWindowData, &win, 1);
    ret = extWindowDoHashAgg(pOperator, startPos, forwardRows, pInputBlock);
    pExtW->lastWinId = getExtWinCurIdx(pTaskInfo);  
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
  pTaskInfo->pStreamRuntimeInfo->funcInfo.extWinProjMode = (pExtW->mode != EEXT_MODE_AGG);

  int32_t scanFlag = MAIN_SCAN;
  int64_t st = taosGetTimestampUs();

  if (!pExtW->pWins) {
    size_t size = taosArrayGetSize(pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamPesudoFuncVals);
    pExtW->pWins = taosArrayInit(size, sizeof(STimeWindow));
    if (!pExtW->pWins) QUERY_CHECK_CODE(terrno, lino, _exit);
    pExtW->pOutputBlocks = taosArrayInit(size, sizeof(SBlockList));
    if (!pExtW->pOutputBlocks) QUERY_CHECK_CODE(terrno, lino, _exit);
    // pExtW->pOffsetList = taosArrayInit(size, sizeof(SLimitInfo));
    // if (!pExtW->pOffsetList) QUERY_CHECK_CODE(terrno, lino, _end);
    for (int32_t i = 0; i < size; ++i) {
      SSTriggerCalcParam* pParam = taosArrayGet(pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamPesudoFuncVals, i);
      STimeWindow win = {.skey = pParam->wstart, .ekey = pParam->wend};
      if (pTaskInfo->pStreamRuntimeInfo->funcInfo.triggerType != STREAM_TRIGGER_SLIDING) {
        win.ekey++;
      }
      TSDB_CHECK_NULL(taosArrayPush(pExtW->pWins, &win), code, lino, _exit, terrno);

      SBlockList bl = {.pSrcBlock = pExtW->binfo.pRes, .pBlocks = 0, .blockRowNumThreshold = 4096};
      code = blockListInit(&bl, 4096);
      if (code != 0) QUERY_CHECK_CODE(code, lino, _exit);
      TSDB_CHECK_NULL(taosArrayPush(pExtW->pOutputBlocks, &bl), code, lino, _exit, terrno);
    }
    pExtW->outputWinId = pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx;
    pExtW->lastWinId = -1;
  }

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      if (EEXT_MODE_AGG == pExtW->mode) {
        TAOS_CHECK_EXIT(extWindowProcessEmptyWins(pOperator, pBlock, true));
      }
      break;
    }

    printDataBlock(pBlock, __func__, "externalwindow");

    qDebug("ext windowpExtW->mode:%d", pExtW->mode);
    switch (pExtW->mode) {
      case EEXT_MODE_SCALAR:
        TAOS_CHECK_EXIT(hashExternalWindowProject(pOperator, pBlock));
        break;
      case EEXT_MODE_AGG:
        hashExternalWindowAgg(pOperator, pBlock);
        break;
      case EEXT_MODE_INDEFR_FUNC:
        TAOS_CHECK_EXIT(hashExternalWindowIndefRows(pOperator, pBlock));
        break;
      default:
        break;
    }
  }

  OPTR_SET_OPENED(pOperator);

  if (pExtW->mode == EEXT_MODE_AGG) {
    qDebug("ext window before dump final rows num:%d", tSimpleHashGetSize(pExtW->aggSup.pResultRowHashTable));

    code = initGroupedResultInfo(&pExtW->groupResInfo, pExtW->aggSup.pResultRowHashTable, pExtW->binfo.inputTsOrder);
    QUERY_CHECK_CODE(code, lino, _exit);

    qDebug("ext window after dump final rows num:%d", tSimpleHashGetSize(pExtW->aggSup.pResultRowHashTable));
  }

_exit:

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
    if (pExtW->mode == EEXT_MODE_SCALAR || pExtW->mode == EEXT_MODE_INDEFR_FUNC) {
      if (pExtW->outputWinId >= taosArrayGetSize(pExtW->pOutputBlocks)) {
        pBlock->info.rows = 0;
        break;
      }
      SBlockList* pList = taosArrayGet(pExtW->pOutputBlocks, pExtW->outputWinId);
      // SLimitInfo* pLimitInfo = taosArrayGet(pExtW->pOffsetList, pExtW->outputWinId);
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

  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM && (*ppRes)) {
    printDataBlock(*ppRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
  }
  
  return code;
}

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

static int32_t doMergeAlignExtWindowAgg(SOperatorInfo* pOperator, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock, SSDataBlock* pResultBlock) {
  SMergeAlignedExternalWindowOperator* pMlExtInfo = pOperator->info;
  SExternalWindowOperator*             pExtW = pMlExtInfo->pExtW;

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*     pSup = &pOperator->exprSupp;
  int32_t        code = 0;

  int32_t startPos = 0;
  int64_t* tsCols = extractTsCol(pBlock, pExtW->primaryTsIndex, pTaskInfo);
  TSKEY ts = getStartTsKey(&pBlock->info.window, tsCols);

  const STimeWindow* pWin = getExtWindow(pOperator, pExtW, ts);
  if (pWin == NULL) {
    qError("failed to get time window for ts:%" PRId64 ", index:%d, error:%s", ts, pExtW->primaryTsIndex,
           tstrerror(terrno));
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_INVALID_PARA);
  }
  if (pMlExtInfo->curTs != INT64_MIN && pMlExtInfo->curTs != pWin->skey) {
    finalizeResultRows(pExtW->aggSup.pResultBuf, &pExtW->binfo.resultRowInfo.cur, pSup, pExtW->binfo.pRes, pTaskInfo);
    resetResultRow(pMlExtInfo->pResultRow, pExtW->aggSup.resultRowSize - sizeof(SResultRow));
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
