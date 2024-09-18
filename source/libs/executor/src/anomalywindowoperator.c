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
  SArray*     blocks;   // SSDataBlock
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

  pOperator->exprSupp.hasWindowOrGroup = true;
  pInfo->tsSlotId = ((SColumnNode*)pAnomalyNode->window.pTspk)->slotId;

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
  pInfo->anomalySup.blocks = taosArrayInit(16, sizeof(SSDataBlock));
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
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    anomalyDestroyOperatorInfo(pInfo);
  }

  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
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
  int32_t                     numOfBlocks = 0;

  blockDataCleanup(pRes);

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    if (pSupp->groupId == 0 || pSupp->groupId == pBlock->info.id.groupId) {
      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks++;
      uInfo("group:%" PRId64 ", cache block, numOfBlocks:%d", pSupp->groupId, numOfBlocks);
      code = anomalyCacheBlock(pInfo, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      uInfo("group:%" PRId64 ", read finish for new group coming, numOfBlocks:%d", pSupp->groupId, numOfBlocks);
      anomalyAggregateBlocks(pOperator);
      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks = 1;
      uInfo("group:%" PRId64 ", new group", pSupp->groupId);
      code = anomalyCacheBlock(pInfo, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pRes->info.rows > 0) {
      (*ppRes) = pRes;
      uInfo("group:%" PRId64 ", return to upstream, numOfBlocks:%d", pRes->info.id.groupId, numOfBlocks);
      return code;
    }
  }

  if (numOfBlocks > 0) {
    uInfo("group:%" PRId64 ", read finish, numOfBlocks:%d", pInfo->anomalySup.groupId, numOfBlocks);
    anomalyAggregateBlocks(pOperator);
  }

  int64_t cost = taosGetTimestampUs() - st;
  uInfo("all groups finished, numOfBlocks:%d cost:%" PRId64 "us", numOfBlocks, cost);

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

  cleanupBasicInfo(&pInfo->binfo);
  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSup);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->anomalySup.blocks); ++i) {
    SSDataBlock* pBlock = taosArrayGet(pInfo->anomalySup.blocks, i);
    blockDataFreeRes(pBlock);
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
  if (taosArrayPush(pInfo->anomalySup.blocks, pDst) == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  return 0;
}

static void anomalyCacheRow(SAnomalyWindowSupp* pSupp, int64_t ts, char* val, int8_t valType) {
  char    buf[32];
  int32_t bufLen = 0;

  switch (valType) {
    case TSDB_DATA_TYPE_BOOL:
      bufLen = snprintf(buf, sizeof(buf), "%" PRId64 ",%d", ts, (*((int8_t*)val) == 1) ? 1 : 0);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      bufLen = snprintf(buf, sizeof(buf), "%" PRId64 ",%d", ts, *(int8_t*)val);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      bufLen = snprintf(buf, sizeof(buf), "%" PRId64 ",%u", ts, *(uint8_t*)val);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      bufLen = snprintf(buf, sizeof(buf), "%" PRId64 ",%d", ts, *(int16_t*)val);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      bufLen = snprintf(buf, sizeof(buf), "%" PRId64 ",%u", ts, *(uint16_t*)val);
      break;
    case TSDB_DATA_TYPE_INT:
      bufLen = snprintf(buf, sizeof(buf), "%" PRId64 ",%d", ts, *(int32_t*)val);
      break;
    case TSDB_DATA_TYPE_UINT:
      bufLen = snprintf(buf, sizeof(buf), "%" PRId64 ",%u", ts, *(uint32_t*)val);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      bufLen = snprintf(buf, sizeof(buf), "%" PRId64 ",%" PRId64, ts, *(int64_t*)val);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      bufLen = snprintf(buf, sizeof(buf), "%" PRId64 ",%" PRIu64, ts, *(uint64_t*)val);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      bufLen = snprintf(buf, sizeof(buf), "%" PRId64 ",%f", ts, GET_FLOAT_VAL(val));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      bufLen = snprintf(buf, sizeof(buf), "%" PRId64 ",%f", ts, GET_DOUBLE_VAL(val));
      break;
    default:
      buf[0] = '\0';
      break;
  }

  pSupp->numOfRows++;
  // uInfo("rows:%" PRId64 ", buf:%s type:%d", pSupp->numOfRows, buf, valType);
}

static int32_t anomalyFindWindow(SAnomalyWindowSupp* pSupp, TSKEY key) {
  int32_t numOfWIndows = (int32_t)taosArrayGetSize(pSupp->windows);
  for (int32_t i = pSupp->curWinIndex; i < numOfWIndows; ++i) {
    STimeWindow* pWindow = taosArrayGet(pSupp->windows, i);
    if (key >= pWindow->skey && key < pWindow->ekey) {
      pSupp->curWin = *pWindow;
      pSupp->curWinIndex = i;
      return 0;
    }
  }
  return -1;
}

static int32_t anomalyAnalysisWindow(SOperatorInfo* pOperator) {
  SAnomalyWindowOperatorInfo* pInfo = pOperator->info;
  SAnomalyWindowSupp*         pSupp = &pInfo->anomalySup;

  int32_t numOfBlocks = (int32_t)taosArrayGetSize(pSupp->blocks);
  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock* pBlock = taosArrayGet(pSupp->blocks, i);
    if (pBlock == NULL) break;
    SColumnInfoData* pValCol = taosArrayGet(pBlock->pDataBlock, pInfo->anomalyCol.slotId);
    if (pValCol == NULL) break;
    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
    if (pTsCol == NULL) break;

    for (int32_t j = 0; j < pBlock->info.rows; ++j) {
      SColumnDataAgg* pAgg = (pBlock->pBlockAgg != NULL) ? &pBlock->pBlockAgg[pInfo->anomalyCol.slotId] : NULL;
      if (colDataIsNull(pValCol, pBlock->info.rows, j, pAgg)) continue;
      anomalyCacheRow(pSupp, ((TSKEY*)pTsCol->pData)[j], colDataGetData(pValCol, j), pValCol->info.type);
    }
  }

#if 0
  STimeWindow win1 = {.skey = 1726056351057, .ekey = 1726056354116};
  STimeWindow win2 = {.skey = 1726056354116, .ekey = 1726126062004};
  STimeWindow win3 = {.skey = 1726126062004, .ekey = INT64_MAX};
  taosArrayPush(pSupp->windows, &win1);
  taosArrayPush(pSupp->windows, &win2);
  taosArrayPush(pSupp->windows, &win3);
#else
  STimeWindow win1 = {.skey = 1577808000000, .ekey = 1577808400000};
  STimeWindow win2 = {.skey = 1577808400000, .ekey = 1577808600000};
  taosArrayPush(pSupp->windows, &win1);
  taosArrayPush(pSupp->windows, &win2);
#endif

  return 0;
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

  uInfo("group:%" PRId64 ", aggregate blocks, numOfBlocks:%d", pSupp->groupId, numOfBlocks);
  pRes->info.id.groupId = pSupp->groupId;

  code = anomalyAnalysisWindow(pOperator);
  QUERY_CHECK_CODE(code, lino, _OVER);

  int32_t numOfWins = taosArrayGetSize(pSupp->windows);
  uInfo("group:%" PRId64 ", numOfWins:%d, numOfRows:%" PRId64, pSupp->groupId, numOfWins, pSupp->numOfRows);
  for (int32_t w = 0; w < numOfWins; ++w) {
    STimeWindow* pWindow = taosArrayGet(pSupp->windows, w);
    if (w == 0) {
      pSupp->curWin = *pWindow;
      pRowSup->win.skey = pSupp->curWin.skey;
    }
    uInfo("group:%" PRId64 ", win:%d [%" PRId64 ", %" PRId64 ")", pSupp->groupId, w, pWindow->skey, pWindow->ekey);
  }

  if (numOfWins <= 0) goto _OVER;
  if (numOfWins > pRes->info.capacity) {
    code = blockDataEnsureCapacity(pRes, numOfWins);
    QUERY_CHECK_CODE(code, lino, _OVER);
  }

  for (int32_t b = 0; b < numOfBlocks; ++b) {
    SSDataBlock* pBlock = taosArrayGet(pSupp->blocks, b);
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

    uInfo("group:%" PRId64 ", block:%d win:%d, riwin:%d riblock:%d, rows:%" PRId64, pSupp->groupId, b,
          pSupp->curWinIndex, rowsInWin, rowsInBlock, pBlock->info.rows);

    for (int32_t r = 0; r < pBlock->info.rows; ++r) {
      TSKEY key = tsList[r];
      bool  keyInWin = (key >= pSupp->curWin.skey && key < pSupp->curWin.ekey);
      bool  lastRow = (r == pBlock->info.rows - 1);

      if (keyInWin) {
        if (r < 5) {
          uInfo("group:%" PRId64 ", block:%d win:%d, row:%d ts:%" PRId64 ", riwin:%d riblock:%d", pSupp->groupId, b,
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
          uInfo("group:%" PRId64 ", block:%d win:%d, row:%d ts:%" PRId64 ", riwin:%d riblock:%d, agg", pSupp->groupId,
                b, pSupp->curWinIndex, r, key, rowsInWin, rowsInBlock);
          anomalyAggregateRows(pOperator, pBlock);
          rowsInBlock = 0;
        }
        if (rowsInWin > 0) {
          uInfo("group:%" PRId64 ", block:%d win:%d, row:%d ts:%" PRId64 ", riwin:%d riblock:%d, build result",
                pSupp->groupId, b, pSupp->curWinIndex, r, key, rowsInWin, rowsInBlock);
          anomalyBuildResult(pOperator);
          rowsInWin = 0;
        }
        if (anomalyFindWindow(pSupp, tsList[r]) == 0) {
          uInfo("group:%" PRId64 ", block:%d win:%d, row:%d ts:%" PRId64 ", riwin:%d riblock:%d, new window detect",
                pSupp->groupId, b, pSupp->curWinIndex, r, key, rowsInWin, rowsInBlock);
          doKeepNewWindowStartInfo(pRowSup, tsList, r, gid);
          doKeepTuple(pRowSup, tsList[r], gid);
          rowsInBlock = 1;
          rowsInWin = 1;
        } else {
          uInfo("group:%" PRId64 ", block:%d win:%d, row:%d ts:%" PRId64 ", riwin:%d riblock:%d, window not found",
                pSupp->groupId, b, pSupp->curWinIndex, r, key, rowsInWin, rowsInBlock);
          rowsInBlock = 0;
          rowsInWin = 0;
        }
      }

      if (lastRow && rowsInBlock > 0) {
        uInfo("group:%" PRId64 ", block:%d win:%d, row:%d ts:%" PRId64 ", riwin:%d riblock:%d, agg since lastrow",
              pSupp->groupId, b, pSupp->curWinIndex, r, key, rowsInWin, rowsInBlock);
        anomalyAggregateRows(pOperator, pBlock);
        rowsInBlock = 0;
      }
    }

    if (lastBlock && rowsInWin > 0) {
      uInfo("group:%" PRId64 ", block:%d win:%d, riwin:%d riblock:%d, build result since lastblock", pSupp->groupId, b,
            pSupp->curWinIndex, rowsInWin, rowsInBlock);
      anomalyBuildResult(pOperator);
      rowsInWin = 0;
    }
  }

  code = doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
  QUERY_CHECK_CODE(code, lino, _OVER);

_OVER:
  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock* pBlock = taosArrayGet(pSupp->blocks, i);
    uInfo("%s, clear block, pBlock:%p pBlock->pDataBlock:%p", __func__, pBlock, pBlock->pDataBlock);
    blockDataFreeRes(pBlock);
  }

  taosArrayClear(pSupp->blocks);
  taosArrayClear(pSupp->windows);
  pSupp->numOfRows = 0;
  pSupp->curWin.ekey = 0;
  pSupp->curWin.skey = 0;
  pSupp->curWinIndex = 0;
}
