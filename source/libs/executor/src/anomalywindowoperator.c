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
#include "taoserror.h"
#include "tarray.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tjson.h"

#ifdef USE_ANALYTICS

typedef struct {
  SArray*     blocks;   // SSDataBlock*
  SArray*     windows;  // STimeWindow
  SArray*     pMaskList; // anomaly mask for each window
  uint64_t    groupId;
  int64_t     cachedRows;
  int32_t     curWinIndex;
  int32_t     curMask;
  STimeWindow curWin;
  SResultRow* pResultRow;
} SAnomalyWindowSupp;

typedef struct {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSup;
  int32_t            tsSlotId;
  int32_t            resMarkSlotId;
  STimeWindowAggSupp twAggSup;
  char               algoName[TSDB_ANALYTIC_ALGO_NAME_LEN];
  char               algoUrl[TSDB_ANALYTIC_ALGO_URL_LEN];
  char               anomalyOpt[TSDB_ANALYTIC_ALGO_OPTION_LEN];
  int64_t            timeout;
  int8_t             wncheck;

  SAnomalyWindowSupp anomalySup;
  SWindowRowsSup     anomalyWinRowSup;
  SColumn            anomalyCol;
  SStateKeys         anomalyKey;
} SAnomalyWindowOperatorInfo;

static void    anomalyDestroyOperatorInfo(void* param);
static int32_t anomalyAggregateNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);
static int32_t anomalyAggregateBlocks(SOperatorInfo* pOperator);
static int32_t anomalyCacheBlock(SAnomalyWindowOperatorInfo* pInfo, SSDataBlock* pBlock);
static int32_t initOptions(SAnomalyWindowOperatorInfo* pInfo, SAnomalyWindowPhysiNode* pAnomalyNode, const char* id);
static int32_t resetAnomalyWindowOperState(SOperatorInfo* pOper);
static void setResSlot(SAnomalyWindowOperatorInfo* pInfo, SAnomalyWindowPhysiNode* pAnomalyNode, SExprInfo* pExprInfo,
                int32_t num);

static int32_t resetAnomalyWindowOperState(SOperatorInfo* pOper) {
  int32_t code = 0, lino = 0;
  SAnomalyWindowOperatorInfo* pInfo = pOper->info;
  SAnomalyWindowPhysiNode*    pAnomalyNode = (SAnomalyWindowPhysiNode*)pOper->pPhyNode;
  SExecTaskInfo* pTaskInfo = pOper->pTaskInfo;
  SExprInfo*  pExprInfo = NULL;
  size_t      keyBufSize = 0;
  int32_t     num = 0;
  const char* id = GET_TASKID(pTaskInfo);

  pOper->status = OP_NOT_OPENED;

  resetBasicOperatorState(&pInfo->binfo);

  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSup);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->anomalySup.blocks); ++i) {
    SSDataBlock* pBlock = taosArrayGetP(pInfo->anomalySup.blocks, i);
    blockDataDestroy(pBlock);
  }

  taosArrayClear(pInfo->anomalySup.blocks);
  taosArrayClear(pInfo->anomalySup.windows);
  taosMemoryFreeClear(pInfo->anomalySup.pResultRow);
  pInfo->anomalySup.groupId = 0;
  pInfo->anomalySup.cachedRows = 0;
  pInfo->anomalySup.curWin.ekey = 0;
  pInfo->anomalySup.curWin.skey = 0;
  pInfo->anomalySup.curWinIndex = 0;

  memset(&pInfo->anomalyWinRowSup, 0, sizeof(pInfo->anomalyWinRowSup));

  if (pAnomalyNode->window.pExprs != NULL) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = NULL;
    TAOS_CHECK_EXIT(createExprInfo(pAnomalyNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalarExpr));

    TAOS_CHECK_EXIT(initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore));
  }

  TAOS_CHECK_EXIT(createExprInfo(pAnomalyNode->window.pFuncs, NULL, &pExprInfo, &num));

  initResultSizeInfo(&pOper->resultInfo, 4096);

  TAOS_CHECK_EXIT(initAggSup(&pOper->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, id, pTaskInfo->streamInfo.pState,
                    &pTaskInfo->storageAPI.functionStore));

  int32_t itemSize = sizeof(int32_t) + pInfo->aggSup.resultRowSize + pInfo->anomalyKey.bytes;
  pInfo->anomalySup.pResultRow = taosMemoryCalloc(1, itemSize);
  TSDB_CHECK_NULL(pInfo->anomalySup.pResultRow, code, lino, _exit, terrno);

  TAOS_CHECK_EXIT(filterInitFromNode((SNode*)pAnomalyNode->window.node.pConditions, &pOper->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo));

  TAOS_CHECK_EXIT(initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window));

_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;  
}


int32_t createAnomalywindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo,
                                        SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  size_t      keyBufSize = 0;
  int32_t     num = 0;
  SExprInfo*  pExprInfo = NULL;
  const char* id = GET_TASKID(pTaskInfo);

  SAnomalyWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SAnomalyWindowOperatorInfo));
  SOperatorInfo*              pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SAnomalyWindowPhysiNode*    pAnomalyNode = (SAnomalyWindowPhysiNode*)physiNode;
  SColumnNode*                pColNode = (SColumnNode*)(pAnomalyNode->pAnomalyKey);
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = physiNode;
  pOperator->exprSupp.hasWindowOrGroup = true;

  code = initOptions(pInfo, pAnomalyNode, id);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pAnomalyNode->window.pExprs != NULL) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pAnomalyNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalarExpr);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = createExprInfo(pAnomalyNode->window.pFuncs, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  tstrncpy(pInfo->anomalyOpt, pAnomalyNode->anomalyOpt, sizeof(pInfo->anomalyOpt));
  setResSlot(pInfo, pAnomalyNode, pExprInfo, num);

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, id, pTaskInfo->streamInfo.pState,
                    &pTaskInfo->storageAPI.functionStore);
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
  QUERY_CHECK_NULL(pInfo->anomalyKey.pData, code, lino, _error, terrno)

  int32_t itemSize = sizeof(int32_t) + pInfo->aggSup.resultRowSize + pInfo->anomalyKey.bytes;
  pInfo->anomalySup.pResultRow = taosMemoryCalloc(1, itemSize);
  QUERY_CHECK_NULL(pInfo->anomalySup.pResultRow, code, lino, _error, terrno)

  pInfo->anomalySup.blocks = taosArrayInit(16, sizeof(SSDataBlock*));
  QUERY_CHECK_NULL(pInfo->anomalySup.blocks, code, lino, _error, terrno)

  pInfo->anomalySup.windows = taosArrayInit(16, sizeof(STimeWindow));
  QUERY_CHECK_NULL(pInfo->anomalySup.windows, code, lino, _error, terrno)

  pInfo->anomalySup.pMaskList = taosArrayInit(16, sizeof(int32_t));
  QUERY_CHECK_NULL(pInfo->anomalySup.pMaskList, code, lino, _error, terrno)

  code = filterInitFromNode((SNode*)pAnomalyNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  setOperatorInfo(pOperator, "AnomalyWindowOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_ANOMALY, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, anomalyAggregateNext, NULL, anomalyDestroyOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  setOperatorResetStateFn(pOperator, resetAnomalyWindowOperState);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;

  qDebug("%s anomaly_window operator is created, algo:%s url:%s opt:%s", id, pInfo->algoName, pInfo->algoUrl,
         pInfo->anomalyOpt);

  return TSDB_CODE_SUCCESS;

_error:
  qError("%s failed to create anomaly_window operator, line:%d algo:%s code:%s", id, lino, pAnomalyNode->anomalyOpt,
         tstrerror(code));

  if (pInfo != NULL) {
    anomalyDestroyOperatorInfo(pInfo);
  }

  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

int32_t initOptions(SAnomalyWindowOperatorInfo* pInfo, SAnomalyWindowPhysiNode* pAnomalyNode, const char* id) {
  SHashObj* pHashMap = NULL;
  int32_t   code = taosAnalyGetOpts(pAnomalyNode->anomalyOpt, &pHashMap);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed to get options for creating anomaly_window operator, code:%s", id, tstrerror(code));
    taosHashCleanup(pHashMap);
    return code;
  }

  code = taosAnalysisParseAlgo(pAnomalyNode->anomalyOpt, pInfo->algoName, pInfo->algoUrl,
                               ANALY_ALGO_TYPE_ANOMALY_DETECT, tListLen(pInfo->algoUrl), pHashMap, id);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed to parsing options for anomaly_window operator, code:%s", id, tstrerror(code));
    taosHashCleanup(pHashMap);
    return code;
  }

  pInfo->timeout = taosAnalysisParseTimout(pHashMap, id);
  pInfo->wncheck = taosAnalysisParseWncheck(pHashMap, id);

  taosHashCleanup(pHashMap);
  return code;
}

void setResSlot(SAnomalyWindowOperatorInfo* pInfo, SAnomalyWindowPhysiNode* pAnomalyNode, SExprInfo* pExprInfo,
                int32_t num) {
  pInfo->tsSlotId = ((SColumnNode*)pAnomalyNode->window.pTspk)->slotId;
  for (int32_t j = 0; j < num; ++j) {
    SExprInfo* p1 = &pExprInfo[j];
    int32_t    dstSlot = p1->base.resSchema.slotId;
    if (p1->pExpr->_function.functionType == FUNCTION_TYPE_ANOMALY_MARK) {
      pInfo->resMarkSlotId = dstSlot;
    }
  }
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
  const char*                 idstr = GET_TASKID(pTaskInfo);

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
      code = anomalyCacheBlock(pInfo, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      qDebug("group:%" PRId64 ", read finish for new group coming, blocks:%d", pSupp->groupId, numOfBlocks);
      code = anomalyAggregateBlocks(pOperator);
      QUERY_CHECK_CODE(code, lino, _end);

      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks = 1;
      pSupp->cachedRows = pBlock->info.rows;
      qDebug("group:%" PRId64 ", new group, rows:%" PRId64 ", total rows:%" PRId64, pSupp->groupId,
             pBlock->info.rows, pSupp->cachedRows);
      code = anomalyCacheBlock(pInfo, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pRes->info.rows > 0) {
      (*ppRes) = pRes;
      qDebug("group:%" PRId64 ", return to upstream, blocks:%d", pRes->info.id.groupId, numOfBlocks);
      return code;
    }
  }

  if (numOfBlocks > 0) {
    qDebug("group:%" PRId64 ", read finish, blocks:%d", pInfo->anomalySup.groupId, numOfBlocks);
    code = anomalyAggregateBlocks(pOperator);
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

static void anomalyDestroyOperatorInfo(void* param) {
  SAnomalyWindowOperatorInfo* pInfo = (SAnomalyWindowOperatorInfo*)param;
  if (pInfo == NULL) return;

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
  taosArrayDestroy(pInfo->anomalySup.pMaskList);
  taosMemoryFreeClear(pInfo->anomalySup.pResultRow);
  taosMemoryFreeClear(pInfo->anomalyKey.pData);

  taosMemoryFreeClear(param);
}

static int32_t anomalyCacheBlock(SAnomalyWindowOperatorInfo* pInfo, SSDataBlock* pSrc) {
  if (pInfo->anomalySup.cachedRows > ANALY_ANOMALY_WINDOW_MAX_ROWS) {
    return TSDB_CODE_ANA_ANODE_TOO_MANY_ROWS;
  }

  SSDataBlock* pDst = NULL;
  int32_t      code = createOneDataBlock(pSrc, true, &pDst);

  if (code != 0) return code;
  if (pDst == NULL) return code;
  if (taosArrayPush(pInfo->anomalySup.blocks, &pDst) == NULL) return terrno;

  return 0;
}

static int32_t anomalyFindWindow(SAnomalyWindowSupp* pSupp, TSKEY key) {
  for (int32_t i = pSupp->curWinIndex; i < taosArrayGetSize(pSupp->windows); ++i) {
    STimeWindow* pWindow = taosArrayGet(pSupp->windows, i);
    if (key >= pWindow->skey && key < pWindow->ekey) {
      pSupp->curWin = *pWindow;
      pSupp->curWinIndex = i;

      int32_t* p = taosArrayGet(pSupp->pMaskList, i);
      if (p != NULL) {
        pSupp->curMask = *p;
      } else {
        pSupp->curMask = -1; // the TDgpt may not return the mask value 
      }

      return 0;
    }
  }
  return -1;
}

static int32_t anomalyParseJson(SJson* pJson, SArray* pWindows, SArray* pMasks, const char* pId) {
  int32_t     code = 0;
  int32_t     rows = 0;
  STimeWindow win = {0};

  taosArrayClear(pWindows);

  tjsonGetInt32ValueFromDouble(pJson, "rows", rows, code);
  if (code < 0) {
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }

  if (rows < 0 && code == 0) {  // error happens, parse the error msg and return to client
    code = parseErrorMsgFromAnalyticServer(pJson, pId);
    tjsonDelete(pJson);
    return code;
  } else if (rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SJson* res = tjsonGetObjectItem(pJson, "res");
  if (res == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

  SJson* pMaskObj = tjsonGetObjectItem(pJson, "mask");
  if (pMaskObj != NULL) {
    if (tjsonGetArraySize(pMaskObj) != rows) {
      qError("%s num in mask list not equals to window number", pId);
      return TSDB_CODE_INVALID_JSON_FORMAT;
    }
  }

  int32_t ressize = tjsonGetArraySize(res);
  if (ressize != rows) {
    qError("%s result in res not equals to window number", pId);
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }

  for (int32_t i = 0; i < rows; ++i) {
    SJson* pRow = tjsonGetArrayItem(res, i);
    if (pRow == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

    int32_t colsize = tjsonGetArraySize(pRow);
    if (colsize != 2) return TSDB_CODE_INVALID_JSON_FORMAT;

    SJson* start = tjsonGetArrayItem(pRow, 0);
    SJson* end = tjsonGetArrayItem(pRow, 1);
    if (start == NULL || end == NULL) {
      qError("%s invalid res from analytic sys, code:%s", pId, tstrerror(TSDB_CODE_INVALID_JSON_FORMAT));
      return TSDB_CODE_INVALID_JSON_FORMAT;
    }

    tjsonGetObjectValueBigInt(start, &win.skey);
    tjsonGetObjectValueBigInt(end, &win.ekey);

    if (win.skey >= win.ekey) {
      win.ekey = win.skey + 1;
    }

    if (pMaskObj != NULL) {
      SJson* pOneMask = tjsonGetArrayItem(pMaskObj, i);
      int64_t mask = 0;
      tjsonGetObjectValueBigInt(pOneMask, &mask);

      int32_t m = mask;
      void* p = taosArrayPush(pMasks, &m);
      if (p == NULL) {
        qError("%s failed to put mask into result list, code:%s", pId, tstrerror(terrno));
        return terrno;
      }
    }

    if (taosArrayPush(pWindows, &win) == NULL) {
      qError("%s out of memory in generating anomaly_window", pId);
      return TSDB_CODE_OUT_OF_BUFFER;
    }
  }

  int32_t numOfWins = taosArrayGetSize(pWindows);
  qDebug("%s anomaly window received, total:%d", pId, numOfWins);
  for (int32_t i = 0; i < numOfWins; ++i) {
    STimeWindow* pWindow = taosArrayGet(pWindows, i);
    qDebug("%s anomaly win:%d [%" PRId64 ", %" PRId64 ")", pId, i, pWindow->skey, pWindow->ekey);
  }

  return code;
}

static int32_t anomalyAnalysisWindow(SOperatorInfo* pOperator) {
  SAnomalyWindowOperatorInfo* pInfo = pOperator->info;
  SAnomalyWindowSupp*         pSupp = &pInfo->anomalySup;
  SJson*                      pJson = NULL;
  SAnalyticBuf                analyBuf = {.bufType = ANALYTICS_BUF_TYPE_JSON};
  char                        dataBuf[64] = {0};
  int32_t                     code = 0;
  int64_t                     ts = taosGetTimestampNs();
  int32_t                     lino = 0;
  const char*                 pId = GET_TASKID(pOperator->pTaskInfo);

  if(pSupp->cachedRows < ANALY_ANOMALY_WINDOW_MIN_ROWS) {
    qError("%s input rows for anomaly check not enough, min required:%d, current:%" PRId64, pId, ANALY_ANOMALY_WINDOW_MIN_ROWS,
           pSupp->cachedRows);
    return TSDB_CODE_ANA_ANODE_NOT_ENOUGH_ROWS;
  }

  snprintf(analyBuf.fileName, sizeof(analyBuf.fileName), "%s/tdengine-anomaly-%" PRId64 "-%p-%" PRId64, tsTempDir, ts,
           pSupp, pSupp->groupId);
  code = tsosAnalyBufOpen(&analyBuf, 2, pId);
  QUERY_CHECK_CODE(code, lino, _OVER);

  const char* prec = TSDB_TIME_PRECISION_MILLI_STR;
  if (pInfo->anomalyCol.precision == TSDB_TIME_PRECISION_MICRO) prec = TSDB_TIME_PRECISION_MICRO_STR;
  if (pInfo->anomalyCol.precision == TSDB_TIME_PRECISION_NANO) prec = TSDB_TIME_PRECISION_NANO_STR;

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
    if (pBlock == NULL) {
      break;
    }

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

  pJson = taosAnalySendReqRetJson(pInfo->algoUrl, ANALYTICS_HTTP_TYPE_POST, &analyBuf, pInfo->timeout, pId);
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

  code = anomalyParseJson(pJson, pSupp->windows, pSupp->pMaskList, pId);

_OVER:
  if (code != 0) {
    qError("%s failed to analysis window since %s, lino:%d", pId, tstrerror(code), lino);
  }

  taosAnalyBufDestroy(&analyBuf);
  if (pJson != NULL) tjsonDelete(pJson);
  return code;
}

static int32_t anomalyAggregateRows(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SAnomalyWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                  pExprSup = &pOperator->exprSupp;
  SAnomalyWindowSupp*         pSupp = &pInfo->anomalySup;
  SWindowRowsSup*             pRowSup = &pInfo->anomalyWinRowSup;
  SResultRow*                 pResRow = pSupp->pResultRow;
  int32_t                     numOfOutput = pOperator->exprSupp.numOfExprs;

  int32_t code = setResultRowInitCtx(pResRow, pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
  if (code == 0) {
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pSupp->curWin, 0);

    // NOTE:the sixth row is the mask value
    int64_t mask = pSupp->curMask;
    colDataSetInt64(&pInfo->twAggSup.timeWindowData, 5, &mask);
    code = applyAggFunctionOnPartialTuples(pTaskInfo, pExprSup->pCtx, &pInfo->twAggSup.timeWindowData,
                                           pRowSup->startRowIndex, pRowSup->numOfRows, pBlock->info.rows, numOfOutput);
  }

  return code;
}

static int32_t anomalyBuildResult(SOperatorInfo* pOperator) {
  SAnomalyWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                  pExprSup = &pOperator->exprSupp;
  SSDataBlock*                pRes = pInfo->binfo.pRes;
  SResultRow*                 pResRow = pInfo->anomalySup.pResultRow;

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
      pRowSup->win.skey = pSupp->curWin.skey;
      pSupp->curWinIndex = w;
      if (pSupp->pMaskList != NULL && taosArrayGetSize(pSupp->pMaskList) > 0) {
        void*p = taosArrayGet(pSupp->pMaskList, w);
        if (p != NULL) {
          pSupp->curMask = *(int32_t*) p;
        } else {
          pSupp->curMask = -1;
        }
      }
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
                                   pInfo->scalarSup.numOfExprs, NULL, GET_STM_RTINFO(pOperator->pTaskInfo));
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
        doKeepTuple(pRowSup, tsList[r], r, gid);
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
          doKeepTuple(pRowSup, tsList[r], r, gid);
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

  code = doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
  QUERY_CHECK_CODE(code, lino, _OVER);

_OVER:
  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock* pBlock = taosArrayGetP(pSupp->blocks, i);
    qDebug("%s, clear block, pBlock:%p pBlock->pDataBlock:%p", __func__, pBlock, pBlock->pDataBlock);
    blockDataDestroy(pBlock);
  }

  taosArrayClear(pSupp->blocks);
  taosArrayClear(pSupp->windows);
  taosArrayClear(pSupp->pMaskList);

  pSupp->cachedRows = 0;
  pSupp->curWin.ekey = 0;
  pSupp->curWin.skey = 0;
  pSupp->curWinIndex = 0;

  return code;
}

#else

int32_t createAnomalywindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo,
                                        SOperatorInfo** pOptrInfo) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}
void destroyForecastInfo(void* param) {}

#endif

