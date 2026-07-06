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

static int32_t trimNullTimelineRows(SSDataBlock* pBlock, int32_t tsSlotId) {
  if (pBlock == NULL || pBlock->info.rows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, tsSlotId);
  if (pTsCol == NULL) {
    return terrno;
  }
  if (!pTsCol->hasNull) {
    return TSDB_CODE_SUCCESS;
  }

  bool* pKeep = taosMemoryCalloc(pBlock->info.rows, sizeof(bool));
  if (pKeep == NULL) {
    return terrno;
  }

  int32_t keepRows = 0;
  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    if (!colDataIsNull_f(pTsCol, i)) {
      pKeep[i] = true;
      ++keepRows;
    }
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (keepRows < pBlock->info.rows) {
    code = trimDataBlock(pBlock, pBlock->info.rows, pKeep);
  }
  taosMemoryFree(pKeep);
  return code;
}

static int32_t resetEventWindowOperState(SOperatorInfo* pOper) {
  SEventWindowOperatorInfo* pEvent = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  SEventWinodwPhysiNode* pPhynode = (SEventWinodwPhysiNode*)pOper->pPhyNode;
  pOper->status = OP_NOT_OPENED;

  resetBasicOperatorState(&pEvent->binfo);
  cleanupResultInfoInEventWindow(pOper, pEvent);
  taosMemoryFreeClear(pEvent->pRow);

  pEvent->groupId = 0;
  pEvent->pPreDataBlock = NULL;
  pEvent->inWindow = false;
  pEvent->startCondCount   = 0;
  pEvent->startCondFirstTs = INT64_MIN;
  pEvent->endCondCount     = 0;
  pEvent->endCondFirstTs   = INT64_MIN;
  pEvent->winSup.lastTs = INT64_MIN;
  resetIndefRowsRuntime(&pEvent->indefRows, pOper);

  colDataDestroy(&pEvent->twAggSup.timeWindowData);
  int32_t code = initExecTimeWindowInfo(&pEvent->twAggSup.timeWindowData, &pTaskInfo->window);

  if (code == 0) {
    code = resetAggSup(&pOper->exprSupp, &pEvent->aggSup, pTaskInfo, pPhynode->window.pFuncs, NULL,
                       sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, NULL,
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
  initOperatorCostInfo(pOperator);

  pOperator->pPhyNode = physiNode;
  pOperator->exprSupp.hasWindowOrGroup = true;
  pOperator->exprSupp.hasWindow = true;

  SEventWinodwPhysiNode* pEventWindowNode = (SEventWinodwPhysiNode*)physiNode;

  int32_t tsSlotId = ((SColumnNode*)pEventWindowNode->window.pTspk)->slotId;
  code = filterInitFromNode((SNode*)pEventWindowNode->pStartCond, &pInfo->pStartCondInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);
  filterSetExecContext(pInfo->pStartCondInfo, pTaskInfo, isTaskKilled);

  code = filterInitFromNode((SNode*)pEventWindowNode->pEndCond, &pInfo->pEndCondInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);
  filterSetExecContext(pInfo->pEndCondInfo, pTaskInfo, isTaskKilled);

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
  filterSetExecContext(pOperator->exprSupp.pFilterInfo, pTaskInfo, isTaskKilled);

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  int32_t    num = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pEventWindowNode->window.pFuncs, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                    NULL, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->indefRowsMode = pEventWindowNode->window.indefRowsFunc;
  if (pInfo->indefRowsMode) {
    code = initIndefRowsRuntime(&pInfo->indefRows, pOperator->exprSupp.pCtx, num, pOperator->resultInfo.capacity,
                               pEventWindowNode->window.pProjs, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pEventWindowNode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pInfo->binfo, pResBlock);

  code = blockDataEnsureCapacity(pResBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  setOptrBasicInfoOrder(&pInfo->binfo, physiNode);
  pInfo->winSup.lastTs = INT64_MIN;

  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->tsSlotId = tsSlotId;
  pInfo->pPreDataBlock = NULL;
  pInfo->pOperator = pOperator;
  pInfo->trueForInfo.trueForType = pEventWindowNode->trueForType;
  pInfo->trueForInfo.count = pEventWindowNode->trueForCount;
  pInfo->trueForInfo.duration = pEventWindowNode->trueForDuration;
  pInfo->startTrueForInfo.trueForType = pEventWindowNode->startTrueForType;
  pInfo->startTrueForInfo.count       = pEventWindowNode->startTrueForCount;
  pInfo->startTrueForInfo.duration    = pEventWindowNode->startTrueForDuration;
  pInfo->endTrueForInfo.trueForType   = pEventWindowNode->endTrueForType;
  pInfo->endTrueForInfo.count         = pEventWindowNode->endTrueForCount;
  pInfo->endTrueForInfo.duration      = pEventWindowNode->endTrueForDuration;
  pInfo->startCondCount   = 0;
  pInfo->startCondFirstTs = INT64_MIN;
  pInfo->endCondCount     = 0;
  pInfo->endCondFirstTs   = INT64_MIN;

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

  // First cleanup function contexts that may reference result buffers/state.
  // This must happen before freeing any buffers that those cleanups might touch.
  cleanupResultInfoInEventWindow(pInfo->pOperator, pInfo);
  cleanupIndefRowsRuntime(&pInfo->indefRows, pInfo->pOperator);

  if (pInfo->pRow != NULL) {
    taosMemoryFree(pInfo->pRow);
    pInfo->pRow = NULL;
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

  if (pOperator->status == OP_EXEC_DONE) {
    *ppRes = NULL;
    return code;
  }

  SExprSupp* pSup = &pOperator->exprSupp;
  int32_t    order = pInfo->binfo.inputTsOrder;

  SSDataBlock* pRes = pInfo->binfo.pRes;

  if (pInfo->indefRowsMode) {
    (*ppRes) = getNextIndefRowsResultBlock(&pInfo->indefRows, pOperator);
    if ((*ppRes) != NULL) {
      return code;
    }
  }

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
    pRes->info.dataLoad = 1;
    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                   pInfo->scalarSup.numOfExprs, NULL, GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = trimNullTimelineRows(pBlock, pInfo->tsSlotId);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pBlock->info.rows == 0) {
      continue;
    }

    code = setInputDataBlock(pSup, pBlock, order, pBlock->info.scanFlag, true);
    QUERY_CHECK_CODE(code, lino, _end);

    code = blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);
    QUERY_CHECK_CODE(code, lino, _end);

    code = eventWindowAggImpl(pOperator, pInfo, pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pInfo->indefRowsMode) {
      (*ppRes) = getNextIndefRowsResultBlock(&pInfo->indefRows, pOperator);
      if ((*ppRes) != NULL) {
        return code;
      }
      continue;
    }

    code = doFilter(pRes, pSup->pFilterInfo, NULL, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pRes->info.rows >= pOperator->resultInfo.threshold ||
        (pRes->info.id.groupId != pInfo->groupId && pRes->info.rows > 0)) {
      (*ppRes) = pRes;
      return code;
    }
  }

  if (pInfo->indefRowsMode) {
    dropAllIndefRowsWindowStates(pOperator, &pInfo->indefRows);
    pInfo->inWindow = false;
    (*ppRes) = getNextIndefRowsResultBlock(&pInfo->indefRows, pOperator);
    if ((*ppRes) == NULL) {
      setOperatorCompleted(pOperator);
    }
    return code;
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

  doKeepTuple(pRowSup, tsList[endIndex], endIndex, pBlock->info.id.groupId);

  if (pInfo->indefRowsMode) {
    SIndefRowsWindowState* pState = NULL;
    return applyIndefRowsFuncOnWindowState(pInfo->pOperator, &pInfo->indefRows, &pState, pInfo->binfo.pRes,
                                           pBlock->info.id.groupId, &pRowSup->win, (SSDataBlock*)pBlock, startIndex,
                                           numOfRows, pInfo->binfo.inputTsOrder, pInfo->aggSup.resultRowSize);
  }

  code = setSingleOutputTupleBufv1(&pInfo->binfo.resultRowInfo, &pRowSup->win, &pInfo->pRow, pSup, &pInfo->aggSup);
  if (code != TSDB_CODE_SUCCESS) {  // null data, too many state code
    qError("failed to set single output tuple buffer, code:%d", code);
    return code;
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, 0);
  pInfo->pRow->nOrigRows += numOfRows;
  code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startIndex, numOfRows,
                                         pBlock->info.rows, numOfOutput);
  return code;
}

static FORCE_INLINE bool isTrueForRuleConfigured(const STrueForInfo* pTrueForInfo) {
  return pTrueForInfo != NULL && (pTrueForInfo->duration > 0 || pTrueForInfo->count > 0);
}

static FORCE_INLINE bool isTrueForSatisfiedFast(STrueForInfo* pTrueForInfo, int64_t skey, int64_t ekey, int64_t count) {
  if (!isTrueForRuleConfigured(pTrueForInfo)) {
    return true;
  }
  return isTrueForSatisfied(pTrueForInfo, skey, ekey, count);
}

static int32_t emitOrDropEventWindowResult(SOperatorInfo* pOperator, SEventWindowOperatorInfo* pInfo, SExprSupp* pSup,
                                           SSDataBlock* pRes, STrueForInfo* pTrueForInfo, SExecTaskInfo* pTaskInfo) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SWindowRowsSup* pRowSup = &pInfo->winSup;

  if (pInfo->indefRowsMode) {
    SIndefRowsWindowState* pState = findIndefRowsWindowState(&pInfo->indefRows, pInfo->groupId, pRowSup->win.skey);
    if (pState == NULL) {
      return TSDB_CODE_QRY_WINDOW_STATE_NOT_EXIST;
    }

    if (!isTrueForSatisfied(pTrueForInfo, pState->win.skey, pState->win.ekey, pState->pRow->nOrigRows)) {
      qDebug("skip small window, groupId: %" PRId64 ", skey: %" PRId64 ", ekey: %" PRId64 ", nrows: %u", pInfo->groupId,
             pState->win.skey, pState->win.ekey, pState->pRow->nOrigRows);
      dropIndefRowsWindowState(pOperator, &pInfo->indefRows, pState);
    } else {
      code = closeIndefRowsWindowState(pOperator, &pInfo->indefRows, pState);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    return TSDB_CODE_SUCCESS;
  }

  doUpdateNumOfRows(pSup->pCtx, pInfo->pRow, pSup->numOfExprs, pSup->rowEntryInfoOffset);

  if (!isTrueForSatisfied(pTrueForInfo, pRowSup->win.skey, pRowSup->win.ekey, pInfo->pRow->nOrigRows)) {
    qDebug("skip small window, groupId: %" PRId64 ", skey: %" PRId64 ", ekey: %" PRId64 ", nrows: %u", pInfo->groupId,
           pRowSup->win.skey, pRowSup->win.ekey, pInfo->pRow->nOrigRows);
  } else {
    if (pRes->info.rows + pInfo->pRow->numOfRows >= pRes->info.capacity) {
      int32_t newSize = pRes->info.rows + pInfo->pRow->numOfRows;
      code = blockDataEnsureCapacity(pRes, newSize);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    code = copyResultrowToDataBlock(pSup->pExprInfo, pSup->numOfExprs, pInfo->pRow, pSup->pCtx, pRes,
                                    pSup->rowEntryInfoOffset, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    pRes->info.rows += pInfo->pRow->numOfRows;
  }

  pInfo->pRow->numOfRows = 0;
  pInfo->pRow->nOrigRows = 0;
  return TSDB_CODE_SUCCESS;
}

static int32_t closeWindowOnEndStreak(SOperatorInfo* pOperator, SEventWindowOperatorInfo* pInfo, SExprSupp* pSup,
                                      SSDataBlock* pRes, STrueForInfo* pTrueForInfo, const SSDataBlock* pBlock,
                                      TSKEY* tsList, int32_t startIndex, int32_t rowIndex, SExecTaskInfo* pTaskInfo) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SWindowRowsSup* pRowSup = &pInfo->winSup;

  int32_t endStreakCount = pInfo->endCondCount;  // saved before clear
  TSKEY   endFirstTs = pInfo->endCondFirstTs;    // saved before clear
  pInfo->endCondCount = 0;
  pInfo->endCondFirstTs = INT64_MIN;

  int32_t endRowIndex = rowIndex - (endStreakCount - 1);
  // Aggregate rows in [startIndex, endRowIndex] of this block only when the
  // end streak's first row resides in THIS block (i.e. endRowIndex >= startIndex).
  // If the streak started in a prior block (endRowIndex < startIndex), every row
  // from startIndex through rowIndex is part of the end streak (matched end_cond)
  // and lies past endFirstTs, so the window must not include any of them.
  // NOTE: rows in the prior block that lay after endFirstTs were already aggregated
  // by the trailing "aggregate rest of block" path; eliminating that residual
  // over-aggregation requires holding streak-tail rows back per block, which is
  // a non-trivial refactor tracked separately.
  if (endRowIndex >= startIndex) {
    code = doEventWindowAggImpl(pInfo, pSup, startIndex, endRowIndex, pBlock, tsList, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  // Override ekey with the first-row timestamp of the end streak.
  pRowSup->win.ekey = endFirstTs;
  code = emitOrDropEventWindowResult(pOperator, pInfo, pSup, pRes, pTrueForInfo, pTaskInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pInfo->inWindow = false;
  pInfo->endCondCount = 0;
  pInfo->endCondFirstTs = INT64_MIN;
  return TSDB_CODE_SUCCESS;
}

static int32_t handleStartSatisfiedAndOverlap(SOperatorInfo* pOperator, SEventWindowOperatorInfo* pInfo,
                                              SExprSupp* pSup, SSDataBlock* pRes, STrueForInfo* pTrueForInfo,
                                              TSKEY* tsList, int32_t rowIndex, bool overlapOnRow, int32_t* startIndex,
                                              bool* closedOnStartOverlap, SExecTaskInfo* pTaskInfo) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SWindowRowsSup* pRowSup = &pInfo->winSup;

  TSKEY streakFirstTs = pInfo->startCondFirstTs;
  pInfo->startCondCount = 0;
  pInfo->startCondFirstTs = INT64_MIN;
  pInfo->inWindow = true;
  pInfo->endCondCount = 0;
  pInfo->endCondFirstTs = INT64_MIN;

  // All streak rows are already aggregated tentatively (in pInfo->pRow
  // for !indefRowsMode, or in the indefRows window state for
  // indefRowsMode). Resume from the next row to avoid double-counting.
  pRowSup->win.skey = streakFirstTs;
  *startIndex = rowIndex + 1;

  // Handle boundary case where start and end conditions overlap on the
  // same row: this row should also participate in end streak detection.
  if (!overlapOnRow) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->endCondFirstTs = tsList[rowIndex];
  pInfo->endCondCount = 1;
  if (isTrueForSatisfiedFast(&pInfo->endTrueForInfo, pInfo->endCondFirstTs, tsList[rowIndex], pInfo->endCondCount)) {
    TSKEY endFirstTs = pInfo->endCondFirstTs;
    pInfo->endCondCount = 0;
    pInfo->endCondFirstTs = INT64_MIN;
    pRowSup->win.ekey = endFirstTs;

    code = emitOrDropEventWindowResult(pOperator, pInfo, pSup, pRes, pTrueForInfo, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    pInfo->inWindow = false;
    *closedOnStartOverlap = true;
  }

  return TSDB_CODE_SUCCESS;
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
  STrueForInfo*    pTrueForInfo = getTrueForInfo(pOperator);

  pRowSup->numOfRows = 0;
  if (pInfo->groupId == 0) {
    pInfo->groupId = gid;
  } else if (pInfo->groupId != gid) {
    // this is a new group, reset the info
    if (pInfo->indefRowsMode) {
      dropAllIndefRowsWindowStates(pOperator, &pInfo->indefRows);
    }
    pInfo->inWindow = false;
    pInfo->groupId = gid;
    pInfo->winSup.lastTs = INT64_MIN;
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

  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    if (pColInfoData->hasNull && colDataIsNull_f(pColInfoData, i)) {
      continue;
    }

    if (pBlock->info.scanFlag != PRE_SCAN) {
      if (pInfo->winSup.lastTs == INT64_MIN) {
        pInfo->winSup.lastTs = tsList[i];
      } else {
        if (tsList[i] != pInfo->winSup.lastTs) {
          pInfo->winSup.lastTs = tsList[i];
        }
      }
    }
  }
  int32_t startIndex = pInfo->inWindow ? 0 : -1;
  while (rowIndex < pBlock->info.rows) {
    while (rowIndex < pBlock->info.rows && pColInfoData->hasNull && colDataIsNull_f(pColInfoData, rowIndex)) {
      rowIndex++;
    }
    if (rowIndex >= pBlock->info.rows) {
      break;
    }

    if (pInfo->inWindow) {  // find enough consecutive end-condition rows to satisfy end_limit
      for (rowIndex = startIndex; rowIndex < pBlock->info.rows; ++rowIndex) {
        if (((bool*)pe->pData)[rowIndex]) {
          // End condition satisfied: accumulate streak.
          if (pInfo->endCondCount == 0) {
            pInfo->endCondFirstTs = tsList[rowIndex];
          }
          pInfo->endCondCount++;
          if (isTrueForSatisfiedFast(&pInfo->endTrueForInfo, pInfo->endCondFirstTs, tsList[rowIndex],
                                     pInfo->endCondCount)) {
            // End threshold met: will close at FIRST row of end streak (handled after loop).
            break;
          }
        } else {
          // End condition interrupted: reset streak.
          pInfo->endCondCount   = 0;
          pInfo->endCondFirstTs = INT64_MIN;
        }
      }

      if (rowIndex < pBlock->info.rows) {
        code = closeWindowOnEndStreak(pOperator, pInfo, pSup, pRes, pTrueForInfo, pBlock, tsList, startIndex, rowIndex,
                                      pTaskInfo);
        QUERY_CHECK_CODE(code, lino, _return);
        rowIndex += 1;
      } else {
        // Guard against startIndex past the last row (happens when the start streak was
        // satisfied at the final row of this block in a prior loop iteration).
        if (startIndex < pBlock->info.rows) {
          code = doEventWindowAggImpl(pInfo, pSup, startIndex, pBlock->info.rows - 1, pBlock, tsList, pTaskInfo);
          QUERY_CHECK_CODE(code, lino, _return);
        }
      }
    } else {  // find the first start value satisfying start_limit threshold
      bool closedOnStartOverlap = false;
      for (; rowIndex < pBlock->info.rows; ++rowIndex) {
        if (((bool*)ps->pData)[rowIndex]) {
          // Start condition satisfied for this row: accumulate streak.
          if (pInfo->startCondCount == 0) {
            pInfo->startCondFirstTs = tsList[rowIndex];
            // Set up window start for tentative aggregation across all blocks.
            doKeepNewWindowStartInfo(pRowSup, tsList, rowIndex, gid);
            pRowSup->win.skey = pInfo->startCondFirstTs;
            if (!pInfo->indefRowsMode && pInfo->pRow != NULL) {
              // pRow already exists from a previous window: reset its agg state.
              clearResultRowInitFlag(pSup->pCtx, pSup->numOfExprs);
              pInfo->pRow->nOrigRows = 0;
            }
          }
          pInfo->startCondCount++;
          // Aggregate this streak row eagerly so it lands in pInfo->pRow (or the
          // indefRows window state) regardless of which block it arrived in.
          // For indefRowsMode: creates/updates the pending window state keyed on
          // startCondFirstTs.  For !indefRowsMode: allocates pInfo->pRow on first
          // call (when pRow is NULL) and accumulates into it on subsequent calls.
          code = doEventWindowAggImpl(pInfo, pSup, rowIndex, rowIndex, pBlock, tsList, pTaskInfo);
          QUERY_CHECK_CODE(code, lino, _return);
          if (isTrueForSatisfiedFast(&pInfo->startTrueForInfo, pInfo->startCondFirstTs, tsList[rowIndex],
                                     pInfo->startCondCount)) {
            bool overlapOnRow = ((bool*)pe->pData)[rowIndex];
            code = handleStartSatisfiedAndOverlap(pOperator, pInfo, pSup, pRes, pTrueForInfo, tsList, rowIndex,
                                                  overlapOnRow, &startIndex, &closedOnStartOverlap, pTaskInfo);
            QUERY_CHECK_CODE(code, lino, _return);
            if (closedOnStartOverlap) {
              rowIndex += 1;
            }
            break;
          }
        } else {
          // Start condition interrupted: reset streak and discard tentative agg.
          if (pInfo->startCondCount > 0) {
            if (!pInfo->indefRowsMode && pInfo->pRow != NULL) {
              clearResultRowInitFlag(pSup->pCtx, pSup->numOfExprs);
              pInfo->pRow->nOrigRows = 0;
            } else if (pInfo->indefRowsMode) {
              // Drop the tentative indefRows window state for the broken streak.
              SIndefRowsWindowState* pPendingState =
                  findIndefRowsWindowState(&pInfo->indefRows, pInfo->groupId, pInfo->startCondFirstTs);
              if (pPendingState != NULL) {
                dropIndefRowsWindowState(pOperator, &pInfo->indefRows, pPendingState);
              }
            }
          }
          pInfo->startCondCount   = 0;
          pInfo->startCondFirstTs = INT64_MIN;
        }
      }

      if (closedOnStartOverlap) {
        continue;
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
