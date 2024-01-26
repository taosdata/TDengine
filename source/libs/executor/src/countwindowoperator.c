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

typedef struct SCountWindowOperatorInfo {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSup;
  SWindowRowsSup     winSup;
  int32_t            tsSlotId;  // primary timestamp column slot id
  STimeWindowAggSupp twAggSup;
  uint64_t           groupId;  // current group id, used to identify the data block from different groups
  SResultRow*        pRow;
} SCountWindowOperatorInfo;

void destroyCountWindowOperatorInfo(void* param) {
  SCountWindowOperatorInfo* pInfo = (SCountWindowOperatorInfo*)param;
  if (pInfo == NULL) {
    return;
  }

  if (pInfo->pRow != NULL) {
    taosMemoryFree(pInfo->pRow);
  }

  cleanupBasicInfo(&pInfo->binfo);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);

  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSup);
  taosMemoryFreeClear(param);
}

int32_t doCountWindowAggImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                pSup = &pOperator->exprSupp;
  SCountWindowOperatorInfo* pInfo = pOperator->info;
  SSDataBlock*              pRes = pInfo->binfo.pRes;
  int64_t                   groupId = pBlock->info.id.groupId;
  SColumnInfoData*          pColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
  TSKEY*                    tsCols = (TSKEY*)pColInfoData->pData;
  SWindowRowsSup*           pRowSup = &pInfo->winSup;
  int32_t                   rowIndex = 0;
  int32_t                   code = TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    // 1.如果group id发生变化，获取新group id上一次的window的缓存，并把旧group id的信息存入缓存。
    // 2.计算 当前需要合并的行数
    // 3.做聚集计算。
    // 4.达到行数，将结果存入pInfo->res中。
  }

  return code;
}

static SSDataBlock* countWindowAggregate(SOperatorInfo* pOperator) {
  SCountWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                pSup = &pOperator->exprSupp;
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
    setInputDataBlock(pSup, pBlock, order, MAIN_SCAN, true);
    blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      pTaskInfo->code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                              pInfo->scalarSup.numOfExprs, NULL);
      if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
      }
    }

    doCountWindowAggImpl(pOperator, pBlock);
    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      return pRes;
    }
  }

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

  code = filterInitFromNode((SNode*)pCountWindowNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

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

  pInfo->twAggSup = (STimeWindowAggSupp){.waterMark = pCountWindowNode->window.watermark,
                                         .calTrigger = pCountWindowNode->window.triggerType};

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



static int32_t setSingleOutputTupleBufv1(SResultRowInfo* pResultRowInfo, STimeWindow* win, SResultRow** pResult,
                                         SExprSupp* pExprSup, SAggSupporter* pAggSup) {
  if (*pResult == NULL) {
    SResultRow* p = taosMemoryCalloc(1, pAggSup->resultRowSize);
    pResultRowInfo->cur = (SResultRowPosition){.pageId = p->pageId, .offset = p->offset};
    *pResult = p;
  }

  (*pResult)->win = *win;

  clearResultRowInitFlag(pExprSup->pCtx, pExprSup->numOfExprs);
  setResultRowInitCtx(*pResult, pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
  return TSDB_CODE_SUCCESS;
}
