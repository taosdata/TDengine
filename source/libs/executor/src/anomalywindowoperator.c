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
#include "tanal.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tjson.h"
#include "ttime.h"

#ifdef USE_ANAL

typedef struct {
  SArray*     blocks;   // SSDataBlock*
  SArray*     windows;  // STimeWindow
  uint64_t    groupId;
  int64_t     numOfRows;
  int32_t     curWinIndex;
  STimeWindow curWin;
  SResultRow* pResultRow;
} SAnomalyWindowSupp;

typedef struct {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSup;
  int32_t            tsSlotId;
  STimeWindowAggSupp twAggSup;
  char               algoName[TSDB_ANAL_ALGO_NAME_LEN];
  char               algoUrl[TSDB_ANAL_ALGO_URL_LEN];
  char               anomalyOpt[TSDB_ANAL_ALGO_OPTION_LEN];
  SAnomalyWindowSupp anomalySup;
  SWindowRowsSup     anomalyWinRowSup;
  SColumn            anomalyCol;
  SStateKeys         anomalyKey;
} SAnomalyWindowOperatorInfo;

static void    anomalyDestroyOperatorInfo(void* param);
static int32_t anomalyAggregateNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);
static void    anomalyAggregateBlocks(SOperatorInfo* pOperator);
static int32_t anomalyCacheBlock(SAnomalyWindowOperatorInfo* pInfo, SSDataBlock* pBlock);

int32_t createAnomalywindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo,
                                        SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SAnomalyWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SAnomalyWindowOperatorInfo));
  SOperatorInfo*              pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SAnomalyWindowPhysiNode*    pAnomalyNode = (SAnomalyWindowPhysiNode*)physiNode;
  SColumnNode*                pColNode = (SColumnNode*)(pAnomalyNode->pAnomalyKey);
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  if (!taosAnalGetOptStr(pAnomalyNode->anomalyOpt, "algo", pInfo->algoName, sizeof(pInfo->algoName))) {
    qError("failed to get anomaly_window algorithm name from %s", pAnomalyNode->anomalyOpt);
    code = TSDB_CODE_ANAL_ALGO_NOT_FOUND;
    goto _error;
  }
  if (taosAnalGetAlgoUrl(pInfo->algoName, ANAL_ALGO_TYPE_ANOMALY_DETECT, pInfo->algoUrl, sizeof(pInfo->algoUrl)) != 0) {
    qError("failed to get anomaly_window algorithm url from %s", pInfo->algoName);
    code = TSDB_CODE_ANAL_ALGO_NOT_LOAD;
    goto _error;
  }

  pOperator->exprSupp.hasWindowOrGroup = true;
  pInfo->tsSlotId = ((SColumnNode*)pAnomalyNode->window.pTspk)->slotId;
  strncpy(pInfo->anomalyOpt, pAnomalyNode->anomalyOpt, sizeof(pInfo->anomalyOpt));

  if (pAnomalyNode->window.pExprs != NULL) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pAnomalyNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalarExpr);
    QUERY_CHECK_CODE(code, lino, _error);
    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  size_t     keyBufSize = 0;
  int32_t    num = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pAnomalyNode->window.pFuncs, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                    pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pAnomalyNode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pInfo->binfo, pResBlock);

  code = blockDataEnsureCapacity(pResBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  pInfo->binfo.inputTsOrder = pAnomalyNode->window.node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pAnomalyNode->window.node.outputTsOrder;

  pInfo->anomalyCol = extractColumnFromColumnNode(pColNode);
  pInfo->anomalyKey.type = pInfo->anomalyCol.type;
  pInfo->anomalyKey.bytes = pInfo->anomalyCol.bytes;
  pInfo->anomalyKey.pData = taosMemoryCalloc(1, pInfo->anomalyCol.bytes);
  if (pInfo->anomalyKey.pData == NULL) {
    goto _error;
  }

  int32_t itemSize = sizeof(int32_t) + pInfo->aggSup.resultRowSize + pInfo->anomalyKey.bytes;
  pInfo->anomalySup.pResultRow = taosMemoryCalloc(1, itemSize);
  pInfo->anomalySup.blocks = taosArrayInit(16, sizeof(SSDataBlock*));
  pInfo->anomalySup.windows = taosArrayInit(16, sizeof(STimeWindow));

  if (pInfo->anomalySup.windows == NULL || pInfo->anomalySup.blocks == NULL || pInfo->anomalySup.pResultRow == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  code = filterInitFromNode((SNode*)pAnomalyNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  setOperatorInfo(pOperator, "AnomalyWindowOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_ANOMALY, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, anomalyAggregateNext, NULL, anomalyDestroyOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;

  qInfo("anomaly_window operator is created, algo:%s url:%s opt:%s", pInfo->algoName, pInfo->algoUrl, pInfo->anomalyOpt);
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    anomalyDestroyOperatorInfo(pInfo);
  }

  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  qError("failed to create anomaly_window operator, algo:%s code:0x%x", pInfo->algoName, code);
  return code;
}

static int32_t anomalyAggregateNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SAnomalyWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SOptrBasicInfo*             pBInfo = &pInfo->binfo;
  SAnomalyWindowSupp*         pSupp = &pInfo->anomalySup;
  SSDataBlock*                pRes = pInfo->binfo.pRes;
  int64_t                     st = taosGetTimestampUs();
  int32_t                     numOfBlocks = taosArrayGetSize(pSupp->blocks);

  blockDataCleanup(pRes);

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    if (pSupp->groupId == 0 || pSupp->groupId == pBlock->info.id.groupId) {
      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks++;
      qDebug("group:%" PRId64 ", blocks:%d, cache block rows:%" PRId64, pSupp->groupId, numOfBlocks, pBlock->info.rows);
      code = anomalyCacheBlock(pInfo, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      qInfo("group:%" PRId64 ", read finish for new group coming, blocks:%d", pSupp->groupId, numOfBlocks);
      anomalyAggregateBlocks(pOperator);
      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks = 1;
      qDebug("group:%" PRId64 ", new group, cache block rows:%" PRId64, pSupp->groupId, pBlock->info.rows);
      code = anomalyCacheBlock(pInfo, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pRes->info.rows > 0) {
      (*ppRes) = pRes;
      qInfo("group:%" PRId64 ", return to upstream, blocks:%d", pRes->info.id.groupId, numOfBlocks);
      return code;
    }
  }

  if (numOfBlocks > 0) {
    qInfo("group:%" PRId64 ", read finish, blocks:%d", pInfo->anomalySup.groupId, numOfBlocks);
    anomalyAggregateBlocks(pOperator);
  }

  int64_t cost = taosGetTimestampUs() - st;
  qInfo("all groups finished, cost:%" PRId64 "us", cost);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) = (pBInfo->pRes->info.rows == 0) ? NULL : pBInfo->pRes;
  return code;
}

static void anomalyDestroyOperatorInfo(void* param) {
  SAnomalyWindowOperatorInfo* pInfo = (SAnomalyWindowOperatorInfo*)param;
  if (pInfo == NULL) return;

  qInfo("anomaly_window operator is destroyed, algo:%s", pInfo->algoName);

  cleanupBasicInfo(&pInfo->binfo);
  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSup);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->anomalySup.blocks); ++i) {
    SSDataBlock* pBlock = taosArrayGetP(pInfo->anomalySup.blocks, i);
    blockDataDestroy(pBlock);
  }
  taosArrayDestroy(pInfo->anomalySup.blocks);
  taosArrayDestroy(pInfo->anomalySup.windows);
  taosMemoryFreeClear(pInfo->anomalySup.pResultRow);
  taosMemoryFreeClear(pInfo->anomalyKey.pData);

  taosMemoryFreeClear(param);
}

static int32_t anomalyCacheBlock(SAnomalyWindowOperatorInfo* pInfo, SSDataBlock* pSrc) {
  SSDataBlock* pDst = NULL;
  int32_t      code = createOneDataBlock(pSrc, true, &pDst);

  if (code != 0) return code;
  if (pDst == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  if (taosArrayPush(pInfo->anomalySup.blocks, &pDst) == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  return 0;
}

static int32_t anomalyFindWindow(SAnomalyWindowSupp* pSupp, TSKEY key) {
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

static int32_t anomalyParseJson(SJson* pJson, SArray* pWindows) {
  int32_t     code = 0;
  int32_t     rows = 0;
  STimeWindow win = {0};

  taosArrayClear(pWindows);

  tjsonGetInt32ValueFromDouble(pJson, "rows", rows, code);
  if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
  if (rows <= 0) return 0;

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
    if (start == NULL || end == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

    tjsonGetObjectValueBigInt(start, &win.skey);
    tjsonGetObjectValueBigInt(end, &win.ekey);

    if (win.skey >= win.ekey) {
      win.ekey = win.skey + 1;
    }

    if (taosArrayPush(pWindows, &win) == NULL) return TSDB_CODE_OUT_OF_BUFFER;
  }

  int32_t numOfWins = taosArrayGetSize(pWindows);
  qInfo("anomaly window recevied, total:%d", numOfWins);
  for (int32_t i = 0; i < numOfWins; ++i) {
    STimeWindow* pWindow = taosArrayGet(pWindows, i);
    qInfo("anomaly win:%d [%" PRId64 ", %" PRId64 ")", i, pWindow->skey, pWindow->ekey);
  }

  return 0;
}

static int32_t anomalyAnalysisWindow(SOperatorInfo* pOperator) {
  SAnomalyWindowOperatorInfo* pInfo = pOperator->info;
  SAnomalyWindowSupp*         pSupp = &pInfo->anomalySup;
  SJson*                      pJson = NULL;
  SAnalBuf                    analBuf = {.bufType = ANAL_BUF_TYPE_JSON};
  char                        dataBuf[64] = {0};
  int32_t                     code = 0;

  int64_t ts = 0;
  // int64_t ts = taosGetTimestampMs();
  snprintf(analBuf.fileName, sizeof(analBuf.fileName), "%s/tdengine-anomaly-%" PRId64 "-%" PRId64, tsTempDir, ts,
           pSupp->groupId);
  code = tsosAnalBufOpen(&analBuf, 2);
  if (code != 0) goto _OVER;

  const char* prec = TSDB_TIME_PRECISION_MILLI_STR;
  if (pInfo->anomalyCol.precision == TSDB_TIME_PRECISION_MICRO) prec = TSDB_TIME_PRECISION_MICRO_STR;
  if (pInfo->anomalyCol.precision == TSDB_TIME_PRECISION_NANO) prec = TSDB_TIME_PRECISION_NANO_STR;

  code = taosAnalBufWriteOptStr(&analBuf, "algo", pInfo->algoName);
  if (code != 0) goto _OVER;

  code = taosAnalBufWriteOptStr(&analBuf, "prec", prec);
  if (code != 0) goto _OVER;

  code = taosAnalBufWriteColMeta(&analBuf, 0, TSDB_DATA_TYPE_TIMESTAMP, "ts");
  if (code != 0) goto _OVER;

  code = taosAnalBufWriteColMeta(&analBuf, 1, pInfo->anomalyCol.type, "val");
  if (code != 0) goto _OVER;

  code = taosAnalBufWriteDataBegin(&analBuf);
  if (code != 0) goto _OVER;

  int32_t numOfBlocks = (int32_t)taosArrayGetSize(pSupp->blocks);

  // timestamp
  code = taosAnalBufWriteColBegin(&analBuf, 0);
  if (code != 0) goto _OVER;
  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock* pBlock = taosArrayGetP(pSupp->blocks, i);
    if (pBlock == NULL) break;
    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
    if (pTsCol == NULL) break;
    for (int32_t j = 0; j < pBlock->info.rows; ++j) {
      code = taosAnalBufWriteColData(&analBuf, 0, TSDB_DATA_TYPE_TIMESTAMP, &((TSKEY*)pTsCol->pData)[j]);
      if (code != 0) goto _OVER;
    }
  }
  code = taosAnalBufWriteColEnd(&analBuf, 0);
  if (code != 0) goto _OVER;

  // data
  code = taosAnalBufWriteColBegin(&analBuf, 1);
  if (code != 0) goto _OVER;
  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock* pBlock = taosArrayGetP(pSupp->blocks, i);
    if (pBlock == NULL) break;
    SColumnInfoData* pValCol = taosArrayGet(pBlock->pDataBlock, pInfo->anomalyCol.slotId);
    if (pValCol == NULL) break;

    for (int32_t j = 0; j < pBlock->info.rows; ++j) {
      code = taosAnalBufWriteColData(&analBuf, 1, pValCol->info.type, colDataGetData(pValCol, j));
      if (code != 0) goto _OVER;
      if (code != 0) goto _OVER;
    }
  }
  code = taosAnalBufWriteColEnd(&analBuf, 1);
  if (code != 0) goto _OVER;

  code = taosAnalBufWriteDataEnd(&analBuf);
  if (code != 0) goto _OVER;

  code = taosAnalBufWriteOptStr(&analBuf, "option", pInfo->anomalyOpt);
  if (code != 0) goto _OVER;

  code = taosAnalBufClose(&analBuf);
  if (code != 0) goto _OVER;

  pJson = taosAnalSendReqRetJson(pInfo->algoUrl, ANAL_HTTP_TYPE_POST, &analBuf);
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

  code = anomalyParseJson(pJson, pSupp->windows);
  if (code != 0) goto _OVER;

_OVER:
  if (code != 0) {
    qError("failed to analysis window since %s", tstrerror(code));
  }
  taosAnalBufDestroy(&analBuf);
  if (pJson != NULL) tjsonDelete(pJson);
  return code;
}

static void anomalyAggregateRows(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SAnomalyWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                  pExprSup = &pOperator->exprSupp;
  SAnomalyWindowSupp*         pSupp = &pInfo->anomalySup;
  SWindowRowsSup*             pRowSup = &pInfo->anomalyWinRowSup;
  SResultRow*                 pResRow = pSupp->pResultRow;
  int32_t                     numOfOutput = pOperator->exprSupp.numOfExprs;

  if (setResultRowInitCtx(pResRow, pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset) == 0) {
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pSupp->curWin, 0);
    applyAggFunctionOnPartialTuples(pTaskInfo, pExprSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                                    pRowSup->numOfRows, pBlock->info.rows, numOfOutput);
  }
}

static void anomalyBuildResult(SOperatorInfo* pOperator) {
  SAnomalyWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                  pExprSup = &pOperator->exprSupp;
  SSDataBlock*                pRes = pInfo->binfo.pRes;
  SResultRow*                 pResRow = pInfo->anomalySup.pResultRow;

  doUpdateNumOfRows(pExprSup->pCtx, pResRow, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
  copyResultrowToDataBlock(pExprSup->pExprInfo, pExprSup->numOfExprs, pResRow, pExprSup->pCtx, pRes,
                           pExprSup->rowEntryInfoOffset, pTaskInfo);
  pRes->info.rows += pResRow->numOfRows;
  clearResultRowInitFlag(pExprSup->pCtx, pExprSup->numOfExprs);
}

static void anomalyAggregateBlocks(SOperatorInfo* pOperator) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SAnomalyWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                  pExprSup = &pOperator->exprSupp;
  SSDataBlock*                pRes = pInfo->binfo.pRes;
  SAnomalyWindowSupp*         pSupp = &pInfo->anomalySup;
  SWindowRowsSup*             pRowSup = &pInfo->anomalyWinRowSup;
  SResultRow*                 pResRow = pSupp->pResultRow;
  int32_t                     numOfOutput = pOperator->exprSupp.numOfExprs;
  int32_t                     rowsInWin = 0;
  int32_t                     rowsInBlock = 0;
  const int64_t               gid = pSupp->groupId;
  const int32_t               order = pInfo->binfo.inputTsOrder;

  int32_t numOfBlocks = (int32_t)taosArrayGetSize(pSupp->blocks);
  if (numOfBlocks == 0) goto _OVER;

  qInfo("group:%" PRId64 ", aggregate blocks, blocks:%d", pSupp->groupId, numOfBlocks);
  pRes->info.id.groupId = pSupp->groupId;

  code = anomalyAnalysisWindow(pOperator);
  QUERY_CHECK_CODE(code, lino, _OVER);

  int32_t numOfWins = taosArrayGetSize(pSupp->windows);
  qInfo("group:%" PRId64 ", wins:%d, rows:%" PRId64, pSupp->groupId, numOfWins, pSupp->numOfRows);
  for (int32_t w = 0; w < numOfWins; ++w) {
    STimeWindow* pWindow = taosArrayGet(pSupp->windows, w);
    if (w == 0) {
      pSupp->curWin = *pWindow;
      pRowSup->win.skey = pSupp->curWin.skey;
    }
    qInfo("group:%" PRId64 ", win:%d [%" PRId64 ", %" PRId64 ")", pSupp->groupId, w, pWindow->skey, pWindow->ekey);
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
      bool  keyInWin = (key >= pSupp->curWin.skey && key < pSupp->curWin.ekey);
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
          anomalyAggregateRows(pOperator, pBlock);
          rowsInBlock = 0;
        }
        if (rowsInWin > 0) {
          qTrace("group:%" PRId64 ", block:%d win:%d, row:%d ts:%" PRId64 ", riwin:%d riblock:%d, build result",
                pSupp->groupId, b, pSupp->curWinIndex, r, key, rowsInWin, rowsInBlock);
          anomalyBuildResult(pOperator);
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
        anomalyAggregateRows(pOperator, pBlock);
        rowsInBlock = 0;
      }
    }

    if (lastBlock && rowsInWin > 0) {
      qTrace("group:%" PRId64 ", block:%d win:%d, riwin:%d riblock:%d, build result since lastblock", pSupp->groupId, b,
            pSupp->curWinIndex, rowsInWin, rowsInBlock);
      anomalyBuildResult(pOperator);
      rowsInWin = 0;
    }
  }

  code = doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
  QUERY_CHECK_CODE(code, lino, _OVER);

_OVER:
  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock* pBlock = taosArrayGetP(pSupp->blocks, i);
    qInfo("%s, clear block, pBlock:%p pBlock->pDataBlock:%p", __func__, pBlock, pBlock->pDataBlock);
    blockDataDestroy(pBlock);
  }

  taosArrayClear(pSupp->blocks);
  taosArrayClear(pSupp->windows);
  pSupp->numOfRows = 0;
  pSupp->curWin.ekey = 0;
  pSupp->curWin.skey = 0;
  pSupp->curWinIndex = 0;
}

#else

int32_t createAnomalywindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo,
                                        SOperatorInfo** pOptrInfo){
  return TSDB_CODE_OPS_NOT_SUPPORT;
}
void destroyForecastInfo(void* param) {}

#endif