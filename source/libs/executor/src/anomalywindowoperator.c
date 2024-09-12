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

typedef struct {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSup;
  int32_t            tsSlotId;  // primary timestamp column slot id //
  STimeWindowAggSupp twAggSup;
  SGroupResInfo      groupResInfo;  //
  SWindowRowsSup     winSup;
  SColumn            anomalyCol;  // start row index
  bool               hasKey;
  SStateKeys         anomalyKey;
} SAnomalyWindowOperatorInfo;

static void doAnomalyWindowAggImpl(SOperatorInfo* pOperator, SAnomalyWindowOperatorInfo* pInfo, SSDataBlock* pBlock) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*     pSup = &pOperator->exprSupp;

  SColumnInfoData* pAnomalyColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->anomalyCol.slotId);
  if (!pAnomalyColInfoData) {
    pTaskInfo->code = terrno;
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  int64_t gid = pBlock->info.id.groupId;
  bool    masterScan = true;
  int32_t numOfOutput = pOperator->exprSupp.numOfExprs;
  int32_t bytes = pAnomalyColInfoData->info.bytes;

  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
  if (!pColInfoData) {
    pTaskInfo->code = terrno;
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  TSKEY*          tsList = (TSKEY*)pColInfoData->pData;
  SWindowRowsSup* pRowSup = &pInfo->winSup;
  pRowSup->numOfRows = 0;
  pRowSup->startRowIndex = 0;

  struct SColumnDataAgg* pAgg = NULL;
  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    pAgg = (pBlock->pBlockAgg != NULL) ? &pBlock->pBlockAgg[pInfo->anomalyCol.slotId] : NULL;
    if (colDataIsNull(pAnomalyColInfoData, pBlock->info.rows, j, pAgg)) {
      continue;
    }

    char* val = colDataGetData(pAnomalyColInfoData, j);

    if (gid != pRowSup->groupId || !pInfo->hasKey) {
      // todo extract method
      if (IS_VAR_DATA_TYPE(pInfo->anomalyKey.type)) {
        varDataCopy(pInfo->anomalyKey.pData, val);
      } else {
        memcpy(pInfo->anomalyKey.pData, val, bytes);
      }

      pInfo->hasKey = true;

      doKeepNewWindowStartInfo(pRowSup, tsList, j, gid);
      doKeepTuple(pRowSup, tsList[j], gid);
    } else if (compareVal(val, &pInfo->anomalyKey)) {
      doKeepTuple(pRowSup, tsList[j], gid);
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

      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &window, 0);
      applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                                      pRowSup->numOfRows, pBlock->info.rows, numOfOutput);

      // here we start a new session window
      doKeepNewWindowStartInfo(pRowSup, tsList, j, gid);
      doKeepTuple(pRowSup, tsList[j], gid);

      // todo extract method
      if (IS_VAR_DATA_TYPE(pInfo->anomalyKey.type)) {
        varDataCopy(pInfo->anomalyKey.pData, val);
      } else {
        memcpy(pInfo->anomalyKey.pData, val, bytes);
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

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, 0);
  applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                                  pRowSup->numOfRows, pBlock->info.rows, numOfOutput);
}

static int32_t openAnomalyWindowAggOptr(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SAnomalyWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;

  SExprSupp* pSup = &pOperator->exprSupp;
  int32_t    order = pInfo->binfo.inputTsOrder;
  int64_t    st = taosGetTimestampUs();

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    pInfo->binfo.pRes->info.scanFlag = pBlock->info.scanFlag;
    code = setInputDataBlock(pSup, pBlock, order, MAIN_SCAN, true);
    QUERY_CHECK_CODE(code, lino, _end);

    code = blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);
    QUERY_CHECK_CODE(code, lino, _end);

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      pTaskInfo->code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                              pInfo->scalarSup.numOfExprs, NULL);
      if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
      }
    }

    doAnomalyWindowAggImpl(pOperator, pInfo, pBlock);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  code = initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, TSDB_ORDER_ASC);
  QUERY_CHECK_CODE(code, lino, _end);

  pOperator->status = OP_RES_TO_RETURN;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static void destroyAnomalyWindowOperatorInfo(void* param) {
  SAnomalyWindowOperatorInfo* pInfo = (SAnomalyWindowOperatorInfo*)param;
  if (pInfo == NULL) return;

  cleanupBasicInfo(&pInfo->binfo);
  taosMemoryFreeClear(pInfo->anomalyKey.pData);
  cleanupExprSupp(&pInfo->scalarSup);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  cleanupAggSup(&pInfo->aggSup);
  cleanupGroupResInfo(&pInfo->groupResInfo);

  taosMemoryFreeClear(param);
}

static int32_t doAnomalyWindowAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SAnomalyWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SOptrBasicInfo*             pBInfo = &pInfo->binfo;

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

  code = blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  while (1) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    code = doFilter(pBInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) = (pBInfo->pRes->info.rows == 0) ? NULL : pBInfo->pRes;
  return code;
}

int32_t createAnomalywindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo,
                                        SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SAnomalyWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SAnomalyWindowOperatorInfo));
  SOperatorInfo*              pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SStateWinodwPhysiNode*      pAnomalyNode = (SStateWinodwPhysiNode*)physiNode;
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->exprSupp.hasWindowOrGroup = true;
  int32_t      tsSlotId = ((SColumnNode*)pAnomalyNode->window.pTspk)->slotId;
  SColumnNode* pColNode = (SColumnNode*)(pAnomalyNode->pStateKey);

  if (pAnomalyNode->window.pExprs != NULL) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pAnomalyNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalarExpr);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->anomalyCol = extractColumnFromColumnNode(pColNode);
  pInfo->anomalyKey.type = pInfo->anomalyCol.type;
  pInfo->anomalyKey.bytes = pInfo->anomalyCol.bytes;
  pInfo->anomalyKey.pData = taosMemoryCalloc(1, pInfo->anomalyCol.bytes);
  if (pInfo->anomalyKey.pData == NULL) {
    goto _error;
  }

  pInfo->binfo.inputTsOrder = pAnomalyNode->window.node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pAnomalyNode->window.node.outputTsOrder;

  code = filterInitFromNode((SNode*)pAnomalyNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;  //

  int32_t    num = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pAnomalyNode->window.pFuncs, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                    pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pAnomalyNode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pInfo->binfo, pResBlock);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pInfo->twAggSup =
      (STimeWindowAggSupp){.waterMark = pAnomalyNode->window.watermark, .calTrigger = pAnomalyNode->window.triggerType};

  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->tsSlotId = tsSlotId;

  setOperatorInfo(pOperator, "AnomalyWindowOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_ANOMALY, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet =
      createOperatorFpSet(openAnomalyWindowAggOptr, doAnomalyWindowAggNext, NULL, destroyAnomalyWindowOperatorInfo,
                          optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    destroyAnomalyWindowOperatorInfo(pInfo);
  }

  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}
