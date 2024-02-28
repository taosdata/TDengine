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
#include "functionMgt.h"
#include "operator.h"
#include "querytask.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "ttime.h"

typedef struct SCountWindowResult {
  int32_t     winRows;
  SResultRow  row;
} SCountWindowResult;

typedef struct SCountWindowSupp {
  SArray*            pWinStates;
  int32_t            stateIndex;
} SCountWindowSupp;

typedef struct SCountWindowOperatorInfo {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSup;
  int32_t            tsSlotId;  // primary timestamp column slot id
  STimeWindowAggSupp twAggSup;
  uint64_t           groupId;  // current group id, used to identify the data block from different groups
  SResultRow*        pRow;
  int32_t            windowCount;
  int32_t            windowSliding;
  SCountWindowSupp   countSup;
} SCountWindowOperatorInfo;

void destroyCountWindowOperatorInfo(void* param) {
  SCountWindowOperatorInfo* pInfo = (SCountWindowOperatorInfo*)param;
  if (pInfo == NULL) {
    return;
  }
  cleanupBasicInfo(&pInfo->binfo);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);

  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSup);
  taosArrayDestroy(pInfo->countSup.pWinStates);
  taosMemoryFreeClear(param);
}

static void clearWinStateBuff(SCountWindowResult* pBuff) {
  pBuff->winRows = 0;
}

static SCountWindowResult* getCountWinStateInfo(SCountWindowSupp* pCountSup) {
  SCountWindowResult* pBuffInfo = taosArrayGet(pCountSup->pWinStates, pCountSup->stateIndex);
  pCountSup->stateIndex = (pCountSup->stateIndex + 1) % taosArrayGetSize(pCountSup->pWinStates);
  return pBuffInfo;
}

static SCountWindowResult* setCountWindowOutputBuff(SExprSupp* pExprSup, SCountWindowSupp* pCountSup, SResultRow** pResult) {
  SCountWindowResult* pBuff = getCountWinStateInfo(pCountSup);
  (*pResult) = &pBuff->row;
  setResultRowInitCtx(*pResult, pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
  return pBuff;
}

static int32_t updateCountWindowInfo(int32_t start, int32_t blockRows, int32_t countWinRows, int32_t* pCurrentRows) {
  int32_t rows = TMIN(countWinRows - (*pCurrentRows), blockRows - start);
  (*pCurrentRows) += rows;
  return rows;
}

int32_t doCountWindowAggImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                pExprSup = &pOperator->exprSupp;
  SCountWindowOperatorInfo* pInfo = pOperator->info;
  SSDataBlock*              pRes = pInfo->binfo.pRes;
  SColumnInfoData*          pColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
  TSKEY*                    tsCols = (TSKEY*)pColInfoData->pData;
  int32_t                   code = TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < pBlock->info.rows;) {
    int32_t             step = pInfo->windowSliding;
    SCountWindowResult* pBuffInfo = setCountWindowOutputBuff(pExprSup, &pInfo->countSup, &pInfo->pRow);
    int32_t prevRows = pBuffInfo->winRows;
    int32_t num = updateCountWindowInfo(i, pBlock->info.rows, pInfo->windowCount, &pBuffInfo->winRows);
    if (prevRows == 0) {
      pInfo->pRow->win.skey = tsCols[i];
    }
    pInfo->pRow->win.ekey = tsCols[num + i - 1];

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pInfo->pRow->win, 0);
    applyAggFunctionOnPartialTuples(pTaskInfo, pExprSup->pCtx, &pInfo->twAggSup.timeWindowData, i, num,
                                    pBlock->info.rows, pExprSup->numOfExprs);
    if (pBuffInfo->winRows == pInfo->windowCount) {
      doUpdateNumOfRows(pExprSup->pCtx, pInfo->pRow, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
      copyResultrowToDataBlock(pExprSup->pExprInfo, pExprSup->numOfExprs, pInfo->pRow, pExprSup->pCtx, pRes,
                               pExprSup->rowEntryInfoOffset, pTaskInfo);
      pRes->info.rows += pInfo->pRow->numOfRows;
      clearWinStateBuff(pBuffInfo);
      clearResultRowInitFlag(pExprSup->pCtx, pExprSup->numOfExprs);
    }
    if (pInfo->windowCount != pInfo->windowSliding) {
      if (prevRows <= pInfo->windowSliding) {
        if (pBuffInfo->winRows > pInfo->windowSliding) {
          step = pInfo->windowSliding - prevRows;
        }
      } else {
        step = 0;
      }
    }
    i += step;
  }

  return code;
}

static void buildCountResult(SExprSupp* pExprSup, SCountWindowSupp* pCountSup, SExecTaskInfo* pTaskInfo, SFilterInfo* pFilterInfo, SSDataBlock* pBlock) {
  SResultRow* pResultRow = NULL;
  for (int32_t i = 0; i < taosArrayGetSize(pCountSup->pWinStates); i++) {
    SCountWindowResult* pBuff = setCountWindowOutputBuff(pExprSup, pCountSup, &pResultRow);
    if (pBuff->winRows == 0) {
      continue;;
    }
    doUpdateNumOfRows(pExprSup->pCtx, pResultRow, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
    copyResultrowToDataBlock(pExprSup->pExprInfo, pExprSup->numOfExprs, pResultRow, pExprSup->pCtx, pBlock,
                             pExprSup->rowEntryInfoOffset, pTaskInfo);
    pBlock->info.rows += pResultRow->numOfRows;
    clearWinStateBuff(pBuff);
    clearResultRowInitFlag(pExprSup->pCtx, pExprSup->numOfExprs);
  }
  doFilter(pBlock, pFilterInfo, NULL);
}

static SSDataBlock* countWindowAggregate(SOperatorInfo* pOperator) {
  SCountWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                pExprSup = &pOperator->exprSupp;
  int32_t                   order = pInfo->binfo.inputTsOrder;
  SSDataBlock*              pRes = pInfo->binfo.pRes;
  SOperatorInfo*            downstream = pOperator->pDownstream[0];

  blockDataCleanup(pRes);

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    pRes->info.scanFlag = pBlock->info.scanFlag;
    setInputDataBlock(pExprSup, pBlock, order, MAIN_SCAN, true);
    blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      pTaskInfo->code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                              pInfo->scalarSup.numOfExprs, NULL);
      if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
      }
    }

    if (pInfo->groupId == 0) {
      pInfo->groupId = pBlock->info.id.groupId;
    } else if (pInfo->groupId != pBlock->info.id.groupId) {
      buildCountResult(pExprSup, &pInfo->countSup, pTaskInfo, pOperator->exprSupp.pFilterInfo, pRes);
      pInfo->groupId = pBlock->info.id.groupId;
    }

    doCountWindowAggImpl(pOperator, pBlock);
    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      return pRes;
    }
  }

  buildCountResult(pExprSup, &pInfo->countSup, pTaskInfo, pOperator->exprSupp.pFilterInfo, pRes);
  return pRes->info.rows == 0 ? NULL : pRes;
}

SOperatorInfo* createCountwindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode,
                                             SExecTaskInfo* pTaskInfo) {
  SCountWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SCountWindowOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SCountWinodwPhysiNode* pCountWindowNode = (SCountWinodwPhysiNode*)physiNode;

  pInfo->tsSlotId = ((SColumnNode*)pCountWindowNode->window.pTspk)->slotId;

  if (pCountWindowNode->window.pExprs != NULL) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pCountWindowNode->window.pExprs, NULL, &numOfScalarExpr);
    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  size_t keyBufSize = 0;
  int32_t    num = 0;
  SExprInfo* pExprInfo = createExprInfo(pCountWindowNode->window.pFuncs, NULL, &num);
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                    pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pCountWindowNode->window.node.pOutputDataBlockDesc);
  blockDataEnsureCapacity(pResBlock, pOperator->resultInfo.capacity);

  initBasicInfo(&pInfo->binfo, pResBlock);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  pInfo->binfo.inputTsOrder = physiNode->inputTsOrder;
  pInfo->binfo.outputTsOrder = physiNode->outputTsOrder;
  pInfo->windowCount = pCountWindowNode->windowCount;
  pInfo->windowSliding = pCountWindowNode->windowSliding;
  //sizeof(SCountWindowResult)
  int32_t itemSize = sizeof(int32_t) + pInfo->aggSup.resultRowSize;
  int32_t numOfItem = 1;
  if (pInfo->windowCount != pInfo->windowSliding) {
    numOfItem = pInfo->windowCount / pInfo->windowSliding + 1;
  }
  pInfo->countSup.pWinStates = taosArrayInit_s(itemSize, numOfItem);
  if (!pInfo->countSup.pWinStates) {
    goto _error;
  }

  pInfo->countSup.stateIndex = 0;

  code = filterInitFromNode((SNode*)pCountWindowNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  setOperatorInfo(pOperator, "CountWindowOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_COUNT, true, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, countWindowAggregate, NULL, destroyCountWindowOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyCountWindowOperatorInfo(pInfo);
  }

  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}
