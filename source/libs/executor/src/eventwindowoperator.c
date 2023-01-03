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

#include "executorimpl.h"
#include "filter.h"
#include "function.h"
#include "functionMgt.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "ttime.h"

typedef struct SEventWindowOperatorInfo {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSup;
  SGroupResInfo      groupResInfo;
  SWindowRowsSup     winSup;
  SColumn            stateCol;  // start row index
  bool               hasKey;
  SStateKeys         stateKey;
  int32_t            tsSlotId;  // primary timestamp column slot id
  STimeWindowAggSupp twAggSup;
} SEventWindowOperatorInfo;

static int32_t openEventWindowAggOptr(SOperatorInfo* pOperator);
static void destroyEWindowOperatorInfo(void* param);
static void doEventWindowAggImpl(SOperatorInfo* pOperator, SEventWindowOperatorInfo* pInfo, SSDataBlock* pBlock);
static SSDataBlock* doEventWindowAgg(SOperatorInfo* pOperator);

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

static void updateTimeWindowInfo(SColumnInfoData* pColData, STimeWindow* pWin, bool includeEndpoint) {
  int64_t* ts = (int64_t*)pColData->pData;
  int32_t  delta = includeEndpoint ? 1 : 0;

  int64_t duration = pWin->ekey - pWin->skey + delta;
  ts[2] = duration;            // set the duration
  ts[3] = pWin->skey;          // window start key
  ts[4] = pWin->ekey + delta;  // window end key
}

SOperatorInfo* createEventwindowOperatorInfo(SOperatorInfo* downstream, SEventWinodwPhysiNode* pStateNode,
                                             SExecTaskInfo* pTaskInfo) {
  SEventWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SEventWindowOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  int32_t      tsSlotId = ((SColumnNode*)pStateNode->window.pTspk)->slotId;
  SColumnNode* pColNode = (SColumnNode*)((STargetNode*)pStateNode->pStartCond)->pExpr;

  if (pStateNode->window.pExprs != NULL) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pStateNode->window.pExprs, NULL, &numOfScalarExpr);
    int32_t    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->stateCol = extractColumnFromColumnNode(pColNode);
  pInfo->stateKey.type = pInfo->stateCol.type;
  pInfo->stateKey.bytes = pInfo->stateCol.bytes;
  pInfo->stateKey.pData = taosMemoryCalloc(1, pInfo->stateCol.bytes);
  if (pInfo->stateKey.pData == NULL) {
    goto _error;
  }

  int32_t code = filterInitFromNode((SNode*)pStateNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  int32_t    num = 0;
  SExprInfo* pExprInfo = createExprInfo(pStateNode->window.pFuncs, NULL, &num);
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pStateNode->window.node.pOutputDataBlockDesc);
  initBasicInfo(&pInfo->binfo, pResBlock);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pInfo->twAggSup =
      (STimeWindowAggSupp){.waterMark = pStateNode->window.watermark, .calTrigger = pStateNode->window.triggerType};

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->tsSlotId = tsSlotId;

  setOperatorInfo(pOperator, "StateWindowOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE, true, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(openEventWindowAggOptr, doEventWindowAgg, NULL, destroyEWindowOperatorInfo,
                                         optrDefaultBufFn, NULL);

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

  cleanupBasicInfo(&pInfo->binfo);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);

  cleanupAggSup(&pInfo->aggSup);
  cleanupGroupResInfo(&pInfo->groupResInfo);
  taosMemoryFreeClear(param);
}

static int32_t openEventWindowAggOptr(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  SEventWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;

  SExprSupp* pSup = &pOperator->exprSupp;
  int32_t    order = TSDB_ORDER_ASC;
  int64_t    st = taosGetTimestampUs();

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

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

    doEventWindowAggImpl(pOperator, pInfo, pBlock);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, TSDB_ORDER_ASC);
  pOperator->status = OP_RES_TO_RETURN;

  return TSDB_CODE_SUCCESS;
}

void doEventWindowAggImpl(SOperatorInfo* pOperator, SEventWindowOperatorInfo* pInfo, SSDataBlock* pBlock) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*     pSup = &pOperator->exprSupp;

  SColumnInfoData* pStateColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->stateCol.slotId);
  int64_t          gid = pBlock->info.id.groupId;

  bool    masterScan = true;
  int32_t numOfOutput = pOperator->exprSupp.numOfExprs;
  int16_t bytes = pStateColInfoData->info.bytes;

  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
  TSKEY*           tsList = (TSKEY*)pColInfoData->pData;

  SWindowRowsSup* pRowSup = &pInfo->winSup;
  pRowSup->numOfRows = 0;

  struct SColumnDataAgg* pAgg = NULL;
  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    pAgg = (pBlock->pBlockAgg != NULL) ? pBlock->pBlockAgg[pInfo->stateCol.slotId] : NULL;
    if (colDataIsNull(pStateColInfoData, pBlock->info.rows, j, pAgg)) {
      continue;
    }

    char* val = colDataGetData(pStateColInfoData, j);

    if (gid != pRowSup->groupId || !pInfo->hasKey) {
      // todo extract method
      if (IS_VAR_DATA_TYPE(pInfo->stateKey.type)) {
        varDataCopy(pInfo->stateKey.pData, val);
      } else {
        memcpy(pInfo->stateKey.pData, val, bytes);
      }

      pInfo->hasKey = true;

      doKeepNewWindowStartInfo(pRowSup, tsList, j, gid);
      doKeepTuple(pRowSup, tsList[j], gid);
    } else if (compareVal(val, &pInfo->stateKey)) {
      doKeepTuple(pRowSup, tsList[j], gid);
      if (j == 0 && pRowSup->startRowIndex != 0) {
        pRowSup->startRowIndex = 0;
      }
    } else {  // a new state window started
      SResultRow* pResult = NULL;

      // keep the time window for the closed time window.
      STimeWindow window = pRowSup->win;

      pRowSup->win.ekey = pRowSup->win.skey;
      int32_t ret = setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &window, masterScan, &pResult, gid, pSup->pCtx,
                                           numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_APP_ERROR);
      }

      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &window, false);
      applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                                      pRowSup->numOfRows, pBlock->info.rows, numOfOutput);

      // here we start a new session window
      doKeepNewWindowStartInfo(pRowSup, tsList, j, gid);
      doKeepTuple(pRowSup, tsList[j], gid);

      // todo extract method
      if (IS_VAR_DATA_TYPE(pInfo->stateKey.type)) {
        varDataCopy(pInfo->stateKey.pData, val);
      } else {
        memcpy(pInfo->stateKey.pData, val, bytes);
      }
    }
  }

  SResultRow* pResult = NULL;
  pRowSup->win.ekey = tsList[pBlock->info.rows - 1];
  int32_t ret = setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &pRowSup->win, masterScan, &pResult, gid,
                                       pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_APP_ERROR);
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, false);
  applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex, pRowSup->numOfRows,
                                  pBlock->info.rows, numOfOutput);
}

SSDataBlock* doEventWindowAgg(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SEventWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SOptrBasicInfo*           pBInfo = &pInfo->binfo;

  pTaskInfo->code = pOperator->fpSet._openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    setOperatorCompleted(pOperator);
    return NULL;
  }

  blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);
  while (1) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    doFilter(pBInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);

    bool hasRemain = hasRemainResults(&pInfo->groupResInfo);
    if (!hasRemain) {
      setOperatorCompleted(pOperator);
      break;
    }

    if (pBInfo->pRes->info.rows > 0) {
      break;
    }
  }

  pOperator->resultInfo.totalRows += pBInfo->pRes->info.rows;
  return (pBInfo->pRes->info.rows == 0) ? NULL : pBInfo->pRes;
}
