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

typedef struct SEventWindowOperatorInfo {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSup;
  SWindowRowsSup     winSup;
  int32_t            tsSlotId;  // primary timestamp column slot id
  STimeWindowAggSupp twAggSup;
  uint64_t           groupId;  // current group id, used to identify the data block from different groups
  SFilterInfo*       pStartCondInfo;
  SFilterInfo*       pEndCondInfo;
  bool               inWindow;
  SResultRow*        pRow;
} SEventWindowOperatorInfo;

static SSDataBlock* eventWindowAggregate(SOperatorInfo* pOperator);
static void         destroyEWindowOperatorInfo(void* param);
static int32_t      eventWindowAggImpl(SOperatorInfo* pOperator, SEventWindowOperatorInfo* pInfo, SSDataBlock* pBlock);

// todo : move to  util
static void doKeepNewWindowStartInfo(SWindowRowsSup* pRowSup, const int64_t* tsList, int32_t rowIndex,
                                     uint64_t groupId) {
  pRowSup->startRowIndex = rowIndex;
  pRowSup->numOfRows = 0;
  pRowSup->win.skey = tsList[rowIndex];
  pRowSup->groupId = groupId;
}

static void doKeepTuple(SWindowRowsSup* pRowSup, int64_t ts, uint64_t groupId) {
  pRowSup->win.ekey = ts;
  pRowSup->prevTs = ts;
  pRowSup->numOfRows += 1;
  pRowSup->groupId = groupId;
}

SOperatorInfo* createEventwindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode,
                                             SExecTaskInfo* pTaskInfo) {
  SEventWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SEventWindowOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SEventWinodwPhysiNode* pEventWindowNode = (SEventWinodwPhysiNode*)physiNode;

  int32_t tsSlotId = ((SColumnNode*)pEventWindowNode->window.pTspk)->slotId;
  int32_t code = filterInitFromNode((SNode*)pEventWindowNode->pStartCond, &pInfo->pStartCondInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = filterInitFromNode((SNode*)pEventWindowNode->pEndCond, &pInfo->pEndCondInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (pEventWindowNode->window.pExprs != NULL) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pEventWindowNode->window.pExprs, NULL, &numOfScalarExpr);
    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  code = filterInitFromNode((SNode*)pEventWindowNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  int32_t    num = 0;
  SExprInfo* pExprInfo = createExprInfo(pEventWindowNode->window.pFuncs, NULL, &num);
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                    pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pEventWindowNode->window.node.pOutputDataBlockDesc);
  blockDataEnsureCapacity(pResBlock, pOperator->resultInfo.capacity);

  initBasicInfo(&pInfo->binfo, pResBlock);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  pInfo->binfo.inputTsOrder = physiNode->inputTsOrder;
  pInfo->binfo.outputTsOrder = physiNode->outputTsOrder;

  pInfo->twAggSup = (STimeWindowAggSupp){.waterMark = pEventWindowNode->window.watermark,
                                         .calTrigger = pEventWindowNode->window.triggerType};

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->tsSlotId = tsSlotId;

  setOperatorInfo(pOperator, "EventWindowOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE, true, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, eventWindowAggregate, NULL, destroyEWindowOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyEWindowOperatorInfo(pInfo);
  }

  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
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

  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSup);
  taosMemoryFreeClear(param);
}

static SSDataBlock* eventWindowAggregate(SOperatorInfo* pOperator) {
  SEventWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;

  SExprSupp* pSup = &pOperator->exprSupp;
  int32_t    order = pInfo->binfo.inputTsOrder;

  SSDataBlock* pRes = pInfo->binfo.pRes;

  blockDataCleanup(pRes);

  SOperatorInfo* downstream = pOperator->pDownstream[0];
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

    eventWindowAggImpl(pOperator, pInfo, pBlock);
    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      return pRes;
    }
  }

  return pRes->info.rows == 0 ? NULL : pRes;
}

static int32_t setSingleOutputTupleBufv1(SResultRowInfo* pResultRowInfo, STimeWindow* win, SResultRow** pResult,
                                         SExprSupp* pExprSup, SAggSupporter* pAggSup) {
  if (*pResult == NULL) {
    SResultRow* p = taosMemoryCalloc(1, pAggSup->resultRowSize);
    pResultRowInfo->cur = (SResultRowPosition){.pageId = p->pageId, .offset = p->offset};
    *pResult = p;
  }

  (*pResult)->win = *win;

  setResultRowInitCtx(*pResult, pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
  return TSDB_CODE_SUCCESS;
}

static void doEventWindowAggImpl(SEventWindowOperatorInfo* pInfo, SExprSupp* pSup, int32_t startIndex, int32_t endIndex,
                                 const SSDataBlock* pBlock, int64_t* tsList, SExecTaskInfo* pTaskInfo) {
  SWindowRowsSup* pRowSup = &pInfo->winSup;

  int32_t numOfOutput = pSup->numOfExprs;
  int32_t numOfRows = endIndex - startIndex + 1;

  doKeepTuple(pRowSup, tsList[endIndex], pBlock->info.id.groupId);

  int32_t ret =
      setSingleOutputTupleBufv1(&pInfo->binfo.resultRowInfo, &pRowSup->win, &pInfo->pRow, pSup, &pInfo->aggSup);
  if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_APP_ERROR);
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, 0);
  applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startIndex, numOfRows,
                                  pBlock->info.rows, numOfOutput);
}

int32_t eventWindowAggImpl(SOperatorInfo* pOperator, SEventWindowOperatorInfo* pInfo, SSDataBlock* pBlock) {
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*       pSup = &pOperator->exprSupp;
  SSDataBlock*     pRes = pInfo->binfo.pRes;
  int64_t          gid = pBlock->info.id.groupId;
  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
  TSKEY*           tsList = (TSKEY*)pColInfoData->pData;
  SWindowRowsSup*  pRowSup = &pInfo->winSup;
  SColumnInfoData *ps = NULL, *pe = NULL;
  int32_t          rowIndex = 0;

  pRowSup->numOfRows = 0;
  if (pInfo->groupId == 0) {
    pInfo->groupId = gid;
  } else if (pInfo->groupId != gid) {
    // this is a new group, reset the info
    pInfo->inWindow = false;
    pInfo->groupId = gid;
  }

  SFilterColumnParam param1 = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};

  int32_t code = filterSetDataFromSlotId(pInfo->pStartCondInfo, &param1);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t status1 = 0;
  filterExecute(pInfo->pStartCondInfo, pBlock, &ps, NULL, param1.numOfCols, &status1);

  SFilterColumnParam param2 = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
  code = filterSetDataFromSlotId(pInfo->pEndCondInfo, &param2);
  if (code != TSDB_CODE_SUCCESS) {
    goto _return;
  }

  int32_t status2 = 0;
  filterExecute(pInfo->pEndCondInfo, pBlock, &pe, NULL, param2.numOfCols, &status2);

  int32_t startIndex = pInfo->inWindow ? 0 : -1;
  while (rowIndex < pBlock->info.rows) {
    if (pInfo->inWindow) {  // let's find the first end value
      for (rowIndex = startIndex; rowIndex < pBlock->info.rows; ++rowIndex) {
        if (((bool*)pe->pData)[rowIndex]) {
          break;
        }
      }

      if (rowIndex < pBlock->info.rows) {
        doEventWindowAggImpl(pInfo, pSup, startIndex, rowIndex, pBlock, tsList, pTaskInfo);
        doUpdateNumOfRows(pSup->pCtx, pInfo->pRow, pSup->numOfExprs, pSup->rowEntryInfoOffset);

        // check buffer size
        if (pRes->info.rows + pInfo->pRow->numOfRows >= pRes->info.capacity) {
          int32_t newSize = pRes->info.rows + pInfo->pRow->numOfRows;
          blockDataEnsureCapacity(pRes, newSize);
        }

        copyResultrowToDataBlock(pSup->pExprInfo, pSup->numOfExprs, pInfo->pRow, pSup->pCtx, pRes,
                                 pSup->rowEntryInfoOffset, pTaskInfo);

        pRes->info.rows += pInfo->pRow->numOfRows;
        pInfo->pRow->numOfRows = 0;

        pInfo->inWindow = false;
        rowIndex += 1;
      } else {
        doEventWindowAggImpl(pInfo, pSup, startIndex, pBlock->info.rows - 1, pBlock, tsList, pTaskInfo);
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

  colDataDestroy(ps);
  taosMemoryFree(ps);
  colDataDestroy(pe);
  taosMemoryFree(pe);

  return code;
}
