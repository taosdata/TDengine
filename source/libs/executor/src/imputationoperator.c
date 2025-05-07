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
#include "tanalytics.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tjson.h"
#include "ttime.h"

#ifdef USE_ANALYTICS

typedef struct {
 SArray*     blocks;   // SSDataBlock*
 SArray*     windows;  // STimeWindow
 uint64_t    groupId;
 int64_t     cachedRows;
 int32_t     curWinIndex;
 STimeWindow curWin;
 SResultRow* pResultRow;
 char*       algoOpt;
} SImputationSupp;

typedef struct {
 SOptrBasicInfo     binfo;
 SExprSupp          scalarSup;
 int32_t            tsSlotId;
 char               algoName[TSDB_ANALYTIC_ALGO_NAME_LEN];
 char               algoUrl[TSDB_ANALYTIC_ALGO_URL_LEN];
 char               imputatOpt[TSDB_ANALYTIC_ALGO_OPTION_LEN];
 int64_t            timeout;
 int8_t             wncheck;
 SImputationSupp    imputatSup;
 SSDataBlock*       pRes;
// SColumn            anomalyCol;
// SStateKeys         anomalyKey;
} SImputationOperatorInfo;

static void    imputatDestroyOperatorInfo(void* param);
static int32_t imputationNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);
//static int32_t anomalyAggregateBlocks(SOperatorInfo* pOperator);
//static int32_t anomalyCacheBlock(SImputationOperatorInfo* pInfo, SSDataBlock* pBlock);

int32_t createImputationOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo,
                                     SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  size_t                  keyBufSize = 0;
  int32_t                 num = 0;
  SExprInfo*              pExprInfo = NULL;
  int32_t                 numOfExprs = 0;
  const char*             id = GET_TASKID(pTaskInfo);
  SHashObj*               pHashMap = NULL;
  SForecastFuncPhysiNode* pForecastPhyNode = (SForecastFuncPhysiNode*)physiNode;
  SExprSupp*              pExprSup = NULL;
  SImputationSupp*        pSupp = NULL;

  SImputationOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SImputationOperatorInfo));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL || pInfo == NULL) {
    code = terrno;
    goto _error;
  }

  pSupp = &pInfo->imputatSup;
  pForecastPhyNode = (SForecastFuncPhysiNode*)physiNode;
  pExprSup = &pOperator->exprSupp;

  code = createExprInfo(pForecastPhyNode->pFuncs, NULL, &pExprInfo, &numOfExprs);
  QUERY_CHECK_CODE(code, lino, _error);
  
  code = initExprSupp(pExprSup, pExprInfo, numOfExprs, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pForecastPhyNode->pExprs != NULL) {
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pForecastPhyNode->pExprs, NULL, &pScalarExprInfo, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, num, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pForecastPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

#if 0
  code = forecastParseInput(pSupp, pForecastPhyNode->pFuncs, id);
  QUERY_CHECK_CODE(code, lino, _error);

  code = forecastParseOutput(pSupp, pExprSup);
  QUERY_CHECK_CODE(code, lino, _error);

  code = forecastParseOpt(pSupp, id);
  QUERY_CHECK_CODE(code, lino, _error);

  code = forecastCreateBuf(pSupp);
  QUERY_CHECK_CODE(code, lino, _error);
#endif
  
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pInfo->pRes = createDataBlockFromDescNode(physiNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);

  setOperatorInfo(pOperator, "ImputationOperator", QUERY_NODE_PHYSICAL_PLAN_FORECAST_FUNC, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, imputationNext, NULL, imputatDestroyOperatorInfo, optrDefaultBufFn,
                                         NULL, optrDefaultGetNextExtFn, NULL);

  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;

  qDebug("%s forecast env is initialized, option:%s", id, pSupp->algoOpt);
  return TSDB_CODE_SUCCESS;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", id, __func__, lino, tstrerror(code));
  }
  
  if (pInfo != NULL) imputatDestroyOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

static int32_t imputationNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SImputationOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SOptrBasicInfo*          pBInfo = &pInfo->binfo;
  SImputationSupp*         pSupp = &pInfo->imputatSup;
  SSDataBlock*             pRes = pInfo->binfo.pRes;
  int64_t                  st = taosGetTimestampUs();
  int32_t                  numOfBlocks = taosArrayGetSize(pSupp->blocks);
  const char*              idstr = GET_TASKID(pTaskInfo);

  blockDataCleanup(pRes);

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    if (pSupp->groupId == 0 || pSupp->groupId == pBlock->info.id.groupId) {
      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks++;
      pSupp->cachedRows += pBlock->info.rows;
      qDebug("group:%" PRId64 ", blocks:%d, rows:%" PRId64 ", total rows:%" PRId64, pSupp->groupId, numOfBlocks,
             pBlock->info.rows, pSupp->cachedRows);
//      code = anomalyCacheBlock(pInfo, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      qDebug("group:%" PRId64 ", read finish for new group coming, blocks:%d", pSupp->groupId, numOfBlocks);
//      code = anomalyAggregateBlocks(pOperator);
      QUERY_CHECK_CODE(code, lino, _end);

      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks = 1;
      pSupp->cachedRows = pBlock->info.rows;
      qDebug("group:%" PRId64 ", new group, rows:%" PRId64 ", total rows:%" PRId64, pSupp->groupId, pBlock->info.rows,
             pSupp->cachedRows);
//      code = anomalyCacheBlock(pInfo, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pRes->info.rows > 0) {
      (*ppRes) = pRes;
      qDebug("group:%" PRId64 ", return to upstream, blocks:%d", pRes->info.id.groupId, numOfBlocks);
      return code;
    }
  }

  if (numOfBlocks > 0) {
    qDebug("group:%" PRId64 ", read finish, blocks:%d", pInfo->imputatSup.groupId, numOfBlocks);
//    code = anomalyAggregateBlocks(pOperator);
  }

  int64_t cost = taosGetTimestampUs() - st;
  qDebug("%s all groups finished, cost:%" PRId64 "us", idstr, cost);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", idstr, __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  (*ppRes) = (pBInfo->pRes->info.rows == 0) ? NULL : pBInfo->pRes;
  return code;
}

static void imputatDestroyOperatorInfo(void* param) {
  SImputationOperatorInfo* pInfo = (SImputationOperatorInfo*)param;
  if (pInfo == NULL) return;

  cleanupBasicInfo(&pInfo->binfo);
  cleanupExprSupp(&pInfo->scalarSup);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->imputatSup.blocks); ++i) {
    SSDataBlock* pBlock = taosArrayGetP(pInfo->imputatSup.blocks, i);
    blockDataDestroy(pBlock);
  }

  taosArrayDestroy(pInfo->imputatSup.blocks);
  taosArrayDestroy(pInfo->imputatSup.windows);
  taosMemoryFreeClear(pInfo->imputatSup.pResultRow);

  taosMemoryFreeClear(param);
}

#if 0
static int32_t anomalyCacheBlock(SAnomalyWindowOperatorInfo* pInfo, SSDataBlock* pSrc) {
  if (pInfo->imputatSup.cachedRows > ANALY_ANOMALY_WINDOW_MAX_ROWS) {
    return TSDB_CODE_ANA_ANODE_TOO_MANY_ROWS;
  }

  SSDataBlock* pDst = NULL;
  int32_t      code = createOneDataBlock(pSrc, true, &pDst);

  if (code != 0) return code;
  if (pDst == NULL) return code;
  if (taosArrayPush(pInfo->imputatSup.blocks, &pDst) == NULL) return terrno;

  return 0;
}
#endif

static int32_t anomalyFindWindow(SImputationSupp* pSupp, TSKEY key) {
  for (int32_t i = pSupp->curWinIndex; i < taosArrayGetSize(pSupp->windows); ++i) {
    STimeWindow* pWindow = taosArrayGet(pSupp->windows, i);
    if (key >= pWindow->skey && key < pWindow->ekey) {
      pSupp->curWin = *pWindow;
      pSupp->curWinIndex = i;
      return 0;
    }
  }
  return -1;
}

static int32_t anomalyParseJson(SJson* pJson, SArray* pWindows, const char* pId) {
  int32_t     code = 0;
  int32_t     rows = 0;
  STimeWindow win = {0};

  taosArrayClear(pWindows);

  tjsonGetInt32ValueFromDouble(pJson, "rows", rows, code);
  if (code < 0) {
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }

  if (rows < 0) {
    char pMsg[1024] = {0};
    code = tjsonGetStringValue(pJson, "msg", pMsg);
    if (code) {
      qError("%s failed to get error msg from rsp, unknown error", pId);
    } else {
      qError("%s failed to exec forecast, msg:%s", pId, pMsg);
    }

    return TSDB_CODE_ANA_ANODE_RETURN_ERROR;
  } else if (rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SJson* res = tjsonGetObjectItem(pJson, "res");
  if (res == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

  int32_t ressize = tjsonGetArraySize(res);
  if (ressize != rows) return TSDB_CODE_INVALID_JSON_FORMAT;

  for (int32_t i = 0; i < rows; ++i) {
    SJson* row = tjsonGetArrayItem(res, i);
    if (row == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

    int32_t colsize = tjsonGetArraySize(row);
    if (colsize != 2) return TSDB_CODE_INVALID_JSON_FORMAT;

    SJson* start = tjsonGetArrayItem(row, 0);
    SJson* end = tjsonGetArrayItem(row, 1);
    if (start == NULL || end == NULL) {
      qError("%s invalid res from analytic sys, code:%s", pId, tstrerror(TSDB_CODE_INVALID_JSON_FORMAT));
      return TSDB_CODE_INVALID_JSON_FORMAT;
    }

    tjsonGetObjectValueBigInt(start, &win.skey);
    tjsonGetObjectValueBigInt(end, &win.ekey);

    if (win.skey >= win.ekey) {
      win.ekey = win.skey + 1;
    }

    if (taosArrayPush(pWindows, &win) == NULL) {
      qError("%s out of memory in generating anomaly_window", pId);
      return TSDB_CODE_OUT_OF_BUFFER;
    }
  }

  int32_t numOfWins = taosArrayGetSize(pWindows);
  qDebug("%s anomaly window recevied, total:%d", pId, numOfWins);
  for (int32_t i = 0; i < numOfWins; ++i) {
    STimeWindow* pWindow = taosArrayGet(pWindows, i);
    qDebug("%s anomaly win:%d [%" PRId64 ", %" PRId64 ")", pId, i, pWindow->skey, pWindow->ekey);
  }

  return code;
}

# if 0
static int32_t anomalyAnalysisWindow(SOperatorInfo* pOperator) {
  SImputationOperatorInfo* pInfo = pOperator->info;
  SImputationSupp*            pSupp = &pInfo->imputatSup;
  SJson*                      pJson = NULL;
  SAnalyticBuf                analyBuf = {.bufType = ANALYTICS_BUF_TYPE_JSON};
  char                        dataBuf[64] = {0};
  int32_t                     code = 0;
  int64_t                     ts = taosGetTimestampMs();
  int32_t                     lino = 0;
  const char*                 pId = GET_TASKID(pOperator->pTaskInfo);

  snprintf(analyBuf.fileName, sizeof(analyBuf.fileName), "%s/tdengine-anomaly-%" PRId64 "-%" PRId64, tsTempDir, ts,
           pSupp->groupId);
  code = tsosAnalyBufOpen(&analyBuf, 2);
  QUERY_CHECK_CODE(code, lino, _OVER);

  const char* prec = TSDB_TIME_PRECISION_MILLI_STR;
//  if (pInfo->anomalyCol.precision == TSDB_TIME_PRECISION_MICRO) prec = TSDB_TIME_PRECISION_MICRO_STR;
//  if (pInfo->anomalyCol.precision == TSDB_TIME_PRECISION_NANO) prec = TSDB_TIME_PRECISION_NANO_STR;

  code = taosAnalyBufWriteColMeta(&analyBuf, 0, TSDB_DATA_TYPE_TIMESTAMP, "ts");
  QUERY_CHECK_CODE(code, lino, _OVER);

  code = taosAnalyBufWriteColMeta(&analyBuf, 1, pInfo->anomalyCol.type, "val");
  QUERY_CHECK_CODE(code, lino, _OVER);

  code = taosAnalyBufWriteDataBegin(&analyBuf);
  QUERY_CHECK_CODE(code, lino, _OVER);

  int32_t numOfBlocks = (int32_t)taosArrayGetSize(pSupp->blocks);

  // timestamp
  code = taosAnalyBufWriteColBegin(&analyBuf, 0);
  QUERY_CHECK_CODE(code, lino, _OVER);

  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock* pBlock = taosArrayGetP(pSupp->blocks, i);
    if (pBlock == NULL) break;
    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
    if (pTsCol == NULL) break;
    for (int32_t j = 0; j < pBlock->info.rows; ++j) {
      code = taosAnalyBufWriteColData(&analyBuf, 0, TSDB_DATA_TYPE_TIMESTAMP, &((TSKEY*)pTsCol->pData)[j]);
      QUERY_CHECK_CODE(code, lino, _OVER);
    }
  }

  code = taosAnalyBufWriteColEnd(&analyBuf, 0);
  QUERY_CHECK_CODE(code, lino, _OVER);

  // data
  code = taosAnalyBufWriteColBegin(&analyBuf, 1);
  QUERY_CHECK_CODE(code, lino, _OVER);

  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock* pBlock = taosArrayGetP(pSupp->blocks, i);
    if (pBlock == NULL) break;
    SColumnInfoData* pValCol = taosArrayGet(pBlock->pDataBlock, pInfo->anomalyCol.slotId);
    if (pValCol == NULL) break;

    for (int32_t j = 0; j < pBlock->info.rows; ++j) {
      code = taosAnalyBufWriteColData(&analyBuf, 1, pValCol->info.type, colDataGetData(pValCol, j));
      QUERY_CHECK_CODE(code, lino, _OVER);
    }
  }
  code = taosAnalyBufWriteColEnd(&analyBuf, 1);
  QUERY_CHECK_CODE(code, lino, _OVER);

  code = taosAnalyBufWriteDataEnd(&analyBuf);
  QUERY_CHECK_CODE(code, lino, _OVER);

  code = taosAnalyBufWriteOptStr(&analyBuf, "option", pInfo->anomalyOpt);
  QUERY_CHECK_CODE(code, lino, _OVER);

  code = taosAnalyBufWriteOptStr(&analyBuf, "algo", pInfo->algoName);
  QUERY_CHECK_CODE(code, lino, _OVER);

  code = taosAnalyBufWriteOptStr(&analyBuf, "prec", prec);
  QUERY_CHECK_CODE(code, lino, _OVER);

  code = taosAnalyBufWriteOptInt(&analyBuf, "wncheck", pInfo->wncheck);
  QUERY_CHECK_CODE(code, lino, _OVER);

  code = taosAnalyBufClose(&analyBuf);
  QUERY_CHECK_CODE(code, lino, _OVER);

  pJson = taosAnalySendReqRetJson(pInfo->algoUrl, ANALYTICS_HTTP_TYPE_POST, &analyBuf, pInfo->timeout * 1000);
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

  code = anomalyParseJson(pJson, pSupp->windows, pId);

_OVER:
  if (code != 0) {
    qError("%s failed to analysis window since %s, lino:%d", pId, tstrerror(code), lino);
  }

  taosAnalyBufDestroy(&analyBuf);
  if (pJson != NULL) tjsonDelete(pJson);
  return code;
}

static int32_t anomalyAggregateRows(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SImputationOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                  pExprSup = &pOperator->exprSupp;
  SImputationSupp*            pSupp = &pInfo->imputatSup;
  SWindowRowsSup*             pRowSup = &pInfo->anomalyWinRowSup;
  SResultRow*                 pResRow = pSupp->pResultRow;
  int32_t                     numOfOutput = pOperator->exprSupp.numOfExprs;

  int32_t code = setResultRowInitCtx(pResRow, pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
  if (code == 0) {
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pSupp->curWin, 0);
    code = applyAggFunctionOnPartialTuples(pTaskInfo, pExprSup->pCtx, &pInfo->twAggSup.timeWindowData,
                                           pRowSup->startRowIndex, pRowSup->numOfRows, pBlock->info.rows, numOfOutput);
  }

  return code;
}

static int32_t anomalyBuildResult(SOperatorInfo* pOperator) {
  SImputationOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                  pExprSup = &pOperator->exprSupp;
  SSDataBlock*                pRes = pInfo->binfo.pRes;
  SResultRow*                 pResRow = pInfo->imputatSup.pResultRow;

  doUpdateNumOfRows(pExprSup->pCtx, pResRow, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
  int32_t code = copyResultrowToDataBlock(pExprSup->pExprInfo, pExprSup->numOfExprs, pResRow, pExprSup->pCtx, pRes,
                                          pExprSup->rowEntryInfoOffset, pTaskInfo);
  if (code == 0) {
    pRes->info.rows += pResRow->numOfRows;
  }

  clearResultRowInitFlag(pExprSup->pCtx, pExprSup->numOfExprs);
  return code;
}

static int32_t anomalyAggregateBlocks(SOperatorInfo* pOperator) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SImputationOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                  pExprSup = &pOperator->exprSupp;
  SSDataBlock*                pRes = pInfo->binfo.pRes;
  SImputationSupp*            pSupp = &pInfo->imputatSup;
//  SWindowRowsSup*             pRowSup = &pInfo->anomalyWinRowSup;
  SResultRow*                 pResRow = pSupp->pResultRow;
  int32_t                     numOfOutput = pOperator->exprSupp.numOfExprs;
  int32_t                     rowsInWin = 0;
  int32_t                     rowsInBlock = 0;
  const int64_t               gid = pSupp->groupId;
  const int32_t               order = pInfo->binfo.inputTsOrder;

  int32_t numOfBlocks = (int32_t)taosArrayGetSize(pSupp->blocks);
  if (numOfBlocks == 0) goto _OVER;

  qDebug("group:%" PRId64 ", aggregate blocks, blocks:%d", pSupp->groupId, numOfBlocks);
  pRes->info.id.groupId = pSupp->groupId;

  code = anomalyAnalysisWindow(pOperator);
  QUERY_CHECK_CODE(code, lino, _OVER);

  int32_t numOfWins = taosArrayGetSize(pSupp->windows);
  qDebug("group:%" PRId64 ", wins:%d, rows:%" PRId64, pSupp->groupId, numOfWins, pSupp->cachedRows);
  for (int32_t w = 0; w < numOfWins; ++w) {
    STimeWindow* pWindow = taosArrayGet(pSupp->windows, w);
    if (w == 0) {
      pSupp->curWin = *pWindow;
//      pRowSup->win.skey = pSupp->curWin.skey;
    }
    qDebug("group:%" PRId64 ", win:%d [%" PRId64 ", %" PRId64 ")", pSupp->groupId, w, pWindow->skey, pWindow->ekey);
  }

  if (numOfWins <= 0) goto _OVER;
  if (numOfWins > pRes->info.capacity) {
    code = blockDataEnsureCapacity(pRes, numOfWins);
    QUERY_CHECK_CODE(code, lino, _OVER);
  }

  for (int32_t b = 0; b < numOfBlocks; ++b) {
    SSDataBlock* pBlock = taosArrayGetP(pSupp->blocks, b);
    if (pBlock == NULL) break;

    pRes->info.scanFlag = pBlock->info.scanFlag;
    code = setInputDataBlock(pExprSup, pBlock, order, MAIN_SCAN, true);
    if (code != 0) break;

    code = blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);
    if (code != 0) break;

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                   pInfo->scalarSup.numOfExprs, NULL);
      if (code != 0) break;
    }

    SColumnInfoData* pValCol = taosArrayGet(pBlock->pDataBlock, pInfo->anomalyCol.slotId);
    if (pValCol == NULL) break;
    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
    if (pTsCol == NULL) break;
    TSKEY* tsList = (TSKEY*)pTsCol->pData;
    bool   lastBlock = (b == numOfBlocks - 1);

    qTrace("group:%" PRId64 ", block:%d win:%d, riwin:%d riblock:%d, rows:%" PRId64, pSupp->groupId, b,
           pSupp->curWinIndex, rowsInWin, rowsInBlock, pBlock->info.rows);

    for (int32_t r = 0; r < pBlock->info.rows; ++r) {
      TSKEY key = tsList[r];
      bool  keyInWin = (key >= pSupp->curWin.skey && key <= pSupp->curWin.ekey);
      bool  lastRow = (r == pBlock->info.rows - 1);

      if (keyInWin) {
        if (r < 5) {
          qTrace("group:%" PRId64 ", block:%d win:%d, row:%d ts:%" PRId64 ", riwin:%d riblock:%d", pSupp->groupId, b,
                 pSupp->curWinIndex, r, key, rowsInWin, rowsInBlock);
        }
        if (rowsInBlock == 0) {
          doKeepNewWindowStartInfo(pRowSup, tsList, r, gid);
        }
        doKeepTuple(pRowSup, tsList[r], gid);
        rowsInBlock++;
        rowsInWin++;
      } else {
        if (rowsInBlock > 0) {
          qTrace("group:%" PRId64 ", block:%d win:%d, row:%d ts:%" PRId64 ", riwin:%d riblock:%d, agg", pSupp->groupId,
                 b, pSupp->curWinIndex, r, key, rowsInWin, rowsInBlock);
          code = anomalyAggregateRows(pOperator, pBlock);
          QUERY_CHECK_CODE(code, lino, _OVER);
          rowsInBlock = 0;
        }
        if (rowsInWin > 0) {
          qTrace("group:%" PRId64 ", block:%d win:%d, row:%d ts:%" PRId64 ", riwin:%d riblock:%d, build result",
                 pSupp->groupId, b, pSupp->curWinIndex, r, key, rowsInWin, rowsInBlock);
          code = anomalyBuildResult(pOperator);
          QUERY_CHECK_CODE(code, lino, _OVER);
          rowsInWin = 0;
        }
        if (anomalyFindWindow(pSupp, tsList[r]) == 0) {
          qTrace("group:%" PRId64 ", block:%d win:%d, row:%d ts:%" PRId64 ", riwin:%d riblock:%d, new window detect",
                 pSupp->groupId, b, pSupp->curWinIndex, r, key, rowsInWin, rowsInBlock);
          doKeepNewWindowStartInfo(pRowSup, tsList, r, gid);
          doKeepTuple(pRowSup, tsList[r], gid);
          rowsInBlock = 1;
          rowsInWin = 1;
        } else {
          qTrace("group:%" PRId64 ", block:%d win:%d, row:%d ts:%" PRId64 ", riwin:%d riblock:%d, window not found",
                 pSupp->groupId, b, pSupp->curWinIndex, r, key, rowsInWin, rowsInBlock);
          rowsInBlock = 0;
          rowsInWin = 0;
        }
      }

      if (lastRow && rowsInBlock > 0) {
        qTrace("group:%" PRId64 ", block:%d win:%d, row:%d ts:%" PRId64 ", riwin:%d riblock:%d, agg since lastrow",
               pSupp->groupId, b, pSupp->curWinIndex, r, key, rowsInWin, rowsInBlock);
        code = anomalyAggregateRows(pOperator, pBlock);
        QUERY_CHECK_CODE(code, lino, _OVER);
        rowsInBlock = 0;
      }
    }

    if (lastBlock && rowsInWin > 0) {
      qTrace("group:%" PRId64 ", block:%d win:%d, riwin:%d riblock:%d, build result since lastblock", pSupp->groupId, b,
             pSupp->curWinIndex, rowsInWin, rowsInBlock);
      code = anomalyBuildResult(pOperator);
      QUERY_CHECK_CODE(code, lino, _OVER);
      rowsInWin = 0;
    }
  }

  code = doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
  QUERY_CHECK_CODE(code, lino, _OVER);

_OVER:
  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock* pBlock = taosArrayGetP(pSupp->blocks, i);
    qDebug("%s, clear block, pBlock:%p pBlock->pDataBlock:%p", __func__, pBlock, pBlock->pDataBlock);
    blockDataDestroy(pBlock);
  }

  taosArrayClear(pSupp->blocks);
  taosArrayClear(pSupp->windows);
  pSupp->cachedRows = 0;
  pSupp->curWin.ekey = 0;
  pSupp->curWin.skey = 0;
  pSupp->curWinIndex = 0;

  return code;
}
#endif

#else

int32_t createImputationOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo,
                                        SOperatorInfo** pOptrInfo) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}
void imputatDestroyOperatorInfo(void* param) {}

#endif