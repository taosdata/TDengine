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

static int32_t eventWindowAggregateNext(SOperatorInfo* pOperator, SSDataBlock** pRes);
static void    destroyEWindowOperatorInfo(void* param);
static int32_t eventWindowAggImpl(SOperatorInfo* pOperator, SEventWindowOperatorInfo* pInfo, SSDataBlock* pBlock);
void cleanupResultInfoInEventWindow(SOperatorInfo* pOperator, SEventWindowOperatorInfo* pInfo);

static int32_t resetEventWindowOperState(SOperatorInfo* pOper) {
  SEventWindowOperatorInfo* pEvent = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  SEventWinodwPhysiNode* pPhynode = (SEventWinodwPhysiNode*)pOper->pPhyNode;
  pOper->status = OP_NOT_OPENED;

  resetBasicOperatorState(&pEvent->binfo);
  taosMemoryFreeClear(pEvent->pRow);

  pEvent->groupId = 0;
  pEvent->pPreDataBlock = NULL;
  pEvent->inWindow = false;

  colDataDestroy(&pEvent->twAggSup.timeWindowData);
  int32_t code = initExecTimeWindowInfo(&pEvent->twAggSup.timeWindowData, &pTaskInfo->window);
  cleanupResultInfoInEventWindow(pOper, pEvent);

  if (code == 0) {
    code = resetAggSup(&pOper->exprSupp, &pEvent->aggSup, pTaskInfo, pPhynode->window.pFuncs, NULL,
                       sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, pTaskInfo->streamInfo.pState,
                       &pTaskInfo->storageAPI.functionStore);
  }
  if (code == 0) {
    code = resetExprSupp(&pEvent->scalarSup, pTaskInfo, pPhynode->window.pExprs, NULL,
                         &pTaskInfo->storageAPI.functionStore);
  }
  return code;
}

int32_t createEventwindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode,
                                             SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SEventWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SEventWindowOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = physiNode;
  pOperator->exprSupp.hasWindowOrGroup = true;

  SEventWinodwPhysiNode* pEventWindowNode = (SEventWinodwPhysiNode*)physiNode;

  int32_t tsSlotId = ((SColumnNode*)pEventWindowNode->window.pTspk)->slotId;
  code = filterInitFromNode((SNode*)pEventWindowNode->pStartCond, &pInfo->pStartCondInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  code = filterInitFromNode((SNode*)pEventWindowNode->pEndCond, &pInfo->pEndCondInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pEventWindowNode->window.pExprs != NULL) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = NULL;

    code = createExprInfo(pEventWindowNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalarExpr);
    QUERY_CHECK_CODE(code, lino, _error);
    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pEventWindowNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  int32_t    num = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pEventWindowNode->window.pFuncs, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                    pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pEventWindowNode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pInfo->binfo, pResBlock);

  code = blockDataEnsureCapacity(pResBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  pInfo->binfo.inputTsOrder = physiNode->inputTsOrder;
  pInfo->binfo.outputTsOrder = physiNode->outputTsOrder;

  pInfo->twAggSup = (STimeWindowAggSupp){.waterMark = pEventWindowNode->window.watermark,
                                         .calTrigger = pEventWindowNode->window.triggerType};

  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->tsSlotId = tsSlotId;
  pInfo->pPreDataBlock = NULL;
  pInfo->pOperator = pOperator;
  pInfo->trueForLimit = pEventWindowNode->trueForLimit;

  setOperatorInfo(pOperator, "EventWindowOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT, true, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, eventWindowAggregateNext, NULL, destroyEWindowOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  setOperatorResetStateFn(pOperator, resetEventWindowOperState);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    destroyEWindowOperatorInfo(pInfo);
  }

  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

void cleanupResultInfoInEventWindow(SOperatorInfo* pOperator, SEventWindowOperatorInfo* pInfo) {
  if (pInfo == NULL || pInfo->pRow == NULL || pOperator == NULL) {
    return;
  }
  SExprSupp*       pSup = &pOperator->exprSupp;
  for (int32_t j = 0; j < pSup->numOfExprs; ++j) {
    pSup->pCtx[j].resultInfo = getResultEntryInfo(pInfo->pRow, j, pSup->rowEntryInfoOffset);
    if (pSup->pCtx[j].fpSet.cleanup) {
      pSup->pCtx[j].fpSet.cleanup(&pSup->pCtx[j]);
    }
  }
}

void destroyEWindowOperatorInfo(void* param) {
  SEventWindowOperatorInfo* pInfo = (SEventWindowOperatorInfo*)param;
  if (pInfo == NULL) {
    return;
  }

  if (pInfo->pRow != NULL) {
    taosMemoryFree(pInfo->pRow);
  }

  if (pInfo->pStartCondInfo != NULL) {
    filterFreeInfo(pInfo->pStartCondInfo);
    pInfo->pStartCondInfo = NULL;
  }

  if (pInfo->pEndCondInfo != NULL) {
    filterFreeInfo(pInfo->pEndCondInfo);
    pInfo->pEndCondInfo = NULL;
  }

  cleanupBasicInfo(&pInfo->binfo);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);

  cleanupResultInfoInEventWindow(pInfo->pOperator, pInfo);
  pInfo->pOperator = NULL;
  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSup);
  taosMemoryFreeClear(param);
}

static int32_t eventWindowAggregateNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SEventWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;

  SExprSupp* pSup = &pOperator->exprSupp;
  int32_t    order = pInfo->binfo.inputTsOrder;

  SSDataBlock* pRes = pInfo->binfo.pRes;

  blockDataCleanup(pRes);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    SSDataBlock* pBlock = NULL;
    if (pInfo->pPreDataBlock == NULL) {
      pBlock = getNextBlockFromDownstream(pOperator, 0);
    } else {
      pBlock = pInfo->pPreDataBlock;
      pInfo->pPreDataBlock = NULL;
    }

    if (pBlock == NULL) {
      break;
    }

    pRes->info.scanFlag = pBlock->info.scanFlag;
    code = setInputDataBlock(pSup, pBlock, order, MAIN_SCAN, true);
    QUERY_CHECK_CODE(code, lino, _end);

    code = blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);
    QUERY_CHECK_CODE(code, lino, _end);

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                   pInfo->scalarSup.numOfExprs, NULL, GET_STM_RTINFO(pOperator->pTaskInfo));
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = eventWindowAggImpl(pOperator, pInfo, pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doFilter(pRes, pSup->pFilterInfo, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pRes->info.rows >= pOperator->resultInfo.threshold ||
        (pRes->info.id.groupId != pInfo->groupId && pRes->info.rows > 0)) {
      (*ppRes) = pRes;
      return code;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) =  pRes->info.rows == 0 ? NULL : pRes;
  return code;
}

static int32_t setSingleOutputTupleBufv1(SResultRowInfo* pResultRowInfo, STimeWindow* win, SResultRow** pResult,
                                         SExprSupp* pExprSup, SAggSupporter* pAggSup) {
  if (*pResult == NULL) {
    SResultRow* p = taosMemoryCalloc(1, pAggSup->resultRowSize);
    if (!p) {
      return terrno;
    }
    pResultRowInfo->cur = (SResultRowPosition){.pageId = p->pageId, .offset = p->offset};
    *pResult = p;
  }

  (*pResult)->win = *win;

  return setResultRowInitCtx(*pResult, pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
}

static int32_t doEventWindowAggImpl(SEventWindowOperatorInfo* pInfo, SExprSupp* pSup, int32_t startIndex,
                                    int32_t endIndex, const SSDataBlock* pBlock, int64_t* tsList,
                                    SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  SWindowRowsSup* pRowSup = &pInfo->winSup;

  int32_t numOfOutput = pSup->numOfExprs;
  int32_t numOfRows = endIndex - startIndex + 1;

  doKeepTuple(pRowSup, tsList[endIndex], pBlock->info.id.groupId);

  code = setSingleOutputTupleBufv1(&pInfo->binfo.resultRowInfo, &pRowSup->win, &pInfo->pRow, pSup, &pInfo->aggSup);
  if (code != TSDB_CODE_SUCCESS) {  // null data, too many state code
    qError("failed to set single output tuple buffer, code:%d", code);
    return code;
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, 0);
  code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startIndex, numOfRows,
                                         pBlock->info.rows, numOfOutput);
  return code;
}

int32_t eventWindowAggImpl(SOperatorInfo* pOperator, SEventWindowOperatorInfo* pInfo, SSDataBlock* pBlock) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*       pSup = &pOperator->exprSupp;
  SSDataBlock*     pRes = pInfo->binfo.pRes;
  int64_t          gid = pBlock->info.id.groupId;
  SColumnInfoData *ps = NULL, *pe = NULL;
  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _return, terrno);
  TSKEY*           tsList = (TSKEY*)pColInfoData->pData;
  SWindowRowsSup*  pRowSup = &pInfo->winSup;
  int32_t          rowIndex = 0;
  int64_t          minWindowSize = getMinWindowSize(pOperator);

  pRowSup->numOfRows = 0;
  if (pInfo->groupId == 0) {
    pInfo->groupId = gid;
  } else if (pInfo->groupId != gid) {
    // this is a new group, reset the info
    pInfo->inWindow = false;
    pInfo->groupId = gid;
    pInfo->pPreDataBlock = pBlock;
    goto _return;
  }
  pRes->info.id.groupId = pInfo->groupId;

  SFilterColumnParam param1 = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};

  code = filterSetDataFromSlotId(pInfo->pStartCondInfo, &param1);
  QUERY_CHECK_CODE(code, lino, _return);

  int32_t status1 = 0;
  code = filterExecute(pInfo->pStartCondInfo, pBlock, &ps, NULL, param1.numOfCols, &status1);
  QUERY_CHECK_CODE(code, lino, _return);

  SFilterColumnParam param2 = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
  code = filterSetDataFromSlotId(pInfo->pEndCondInfo, &param2);
  QUERY_CHECK_CODE(code, lino, _return);

  int32_t status2 = 0;
  code = filterExecute(pInfo->pEndCondInfo, pBlock, &pe, NULL, param2.numOfCols, &status2);
  QUERY_CHECK_CODE(code, lino, _return);

  int32_t startIndex = pInfo->inWindow ? 0 : -1;
  while (rowIndex < pBlock->info.rows) {
    if (pInfo->inWindow) {  // let's find the first end value
      for (rowIndex = startIndex; rowIndex < pBlock->info.rows; ++rowIndex) {
        if (((bool*)pe->pData)[rowIndex]) {
          break;
        }
      }

      if (rowIndex < pBlock->info.rows) {
        code = doEventWindowAggImpl(pInfo, pSup, startIndex, rowIndex, pBlock, tsList, pTaskInfo);
        QUERY_CHECK_CODE(code, lino, _return);
        doUpdateNumOfRows(pSup->pCtx, pInfo->pRow, pSup->numOfExprs, pSup->rowEntryInfoOffset);

        if (pRowSup->win.ekey - pRowSup->win.skey < minWindowSize) {
          qDebug("skip small window, groupId: %" PRId64 ", windowSize: %" PRId64 ", minWindowSize: %" PRId64,
                 pInfo->groupId, pRowSup->win.ekey - pRowSup->win.skey, minWindowSize);
        } else {
          // check buffer size
          if (pRes->info.rows + pInfo->pRow->numOfRows >= pRes->info.capacity) {
            int32_t newSize = pRes->info.rows + pInfo->pRow->numOfRows;
            code = blockDataEnsureCapacity(pRes, newSize);
            QUERY_CHECK_CODE(code, lino, _return);
          }

          code = copyResultrowToDataBlock(pSup->pExprInfo, pSup->numOfExprs, pInfo->pRow, pSup->pCtx, pRes,
                                          pSup->rowEntryInfoOffset, pTaskInfo);
          QUERY_CHECK_CODE(code, lino, _return);

          pRes->info.rows += pInfo->pRow->numOfRows;
        }
        pInfo->pRow->numOfRows = 0;

        pInfo->inWindow = false;
        rowIndex += 1;
      } else {
        code = doEventWindowAggImpl(pInfo, pSup, startIndex, pBlock->info.rows - 1, pBlock, tsList, pTaskInfo);
        QUERY_CHECK_CODE(code, lino, _return);
      }
    } else {  // find the first start value that is fulfill for the start condition
      for (; rowIndex < pBlock->info.rows; ++rowIndex) {
        if (((bool*)ps->pData)[rowIndex]) {
          doKeepNewWindowStartInfo(pRowSup, tsList, rowIndex, gid);
          pInfo->inWindow = true;
          startIndex = rowIndex;
          if (pInfo->pRow != NULL) {
            clearResultRowInitFlag(pSup->pCtx, pSup->numOfExprs);
          }
          break;
        }
      }

      if (pInfo->inWindow) {
        continue;  // try to find the end position
      } else {
        break;  // no valid start position, quit
      }
    }
  }

_return:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  colDataDestroy(ps);
  taosMemoryFree(ps);
  colDataDestroy(pe);
  taosMemoryFree(pe);

  return code;
}
