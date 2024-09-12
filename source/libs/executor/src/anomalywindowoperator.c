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
  STimeWindow curWin;
  SResultRow  resultRow;
} SAnomalyWindowSupp;

typedef struct {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSup;
  int32_t            tsSlotId;  // primary timestamp column slot id
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

  pInfo->anomalySup.blocks = taosArrayInit(16, sizeof(SSDataBlock));
  pInfo->anomalySup.windows = taosArrayInit(16, sizeof(STimeWindow));
  if (pInfo->anomalySup.windows == NULL || pInfo->anomalySup.blocks == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pInfo->anomalyCol = extractColumnFromColumnNode(pColNode);
  pInfo->anomalyKey.type = pInfo->anomalyCol.type;
  pInfo->anomalyKey.bytes = pInfo->anomalyCol.bytes;
  pInfo->anomalyKey.pData = taosMemoryCalloc(1, pInfo->anomalyCol.bytes);
  if (pInfo->anomalyKey.pData == NULL) {
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
  SExprSupp*                  pExprSup = &pOperator->exprSupp;
  int32_t                     order = pInfo->binfo.inputTsOrder;
  SOptrBasicInfo*             pBInfo = &pInfo->binfo;
  SAnomalyWindowSupp*         pSupp = &pInfo->anomalySup;
  SSDataBlock*                pRes = pInfo->binfo.pRes;
  int64_t                     st = taosGetTimestampUs();
  int32_t                     numOfBlocks = 0;
  int32_t                     numOfGroups = 0;

  blockDataCleanup(pRes);

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      uInfo("group:%" PRId64 ", last group", pInfo->anomalySup.groupId);
      anomalyAggregateBlocks(pOperator);
      break;
    }

    numOfBlocks++;

    pRes->info.scanFlag = pBlock->info.scanFlag;
    code = setInputDataBlock(pExprSup, pBlock, order, MAIN_SCAN, true);
    QUERY_CHECK_CODE(code, lino, _end);

    code = blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);
    QUERY_CHECK_CODE(code, lino, _end);

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                   pInfo->scalarSup.numOfExprs, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pSupp->groupId == 0) {
      pSupp->groupId = pBlock->info.id.groupId;
      numOfGroups++;
      uInfo("group:%" PRId64 ", first group", pSupp->groupId);
      code = anomalyCacheBlock(pInfo, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (pSupp->groupId == pBlock->info.id.groupId) {
      uInfo("group:%" PRId64 ", continue read", pSupp->groupId);
      code = anomalyCacheBlock(pInfo, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (pSupp->groupId != pBlock->info.id.groupId) {
      uInfo("group:%" PRId64 ", read finish", pSupp->groupId);
      anomalyAggregateBlocks(pOperator);
      pSupp->groupId = pBlock->info.id.groupId;
      numOfGroups++;
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

  int64_t cost = taosGetTimestampUs() - st;
  uInfo("all groups finished, numOfBlocks:%d cost:%" PRId64, numOfBlocks, cost);

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

  taosArrayDestroy(pInfo->anomalySup.blocks);
  taosArrayDestroy(pInfo->anomalySup.windows);
  taosMemoryFreeClear(pInfo->anomalyKey.pData);

  taosMemoryFreeClear(param);
}

static int32_t anomalyCacheBlock(SAnomalyWindowOperatorInfo* pInfo, SSDataBlock* pBlock) {
  SSDataBlock cacheBlock = {0};

  int32_t code = copyDataBlock(&cacheBlock, pBlock);
  if (code != TSDB_CODE_SUCCESS) return code;

  cacheBlock.pDataBlock = pBlock->pDataBlock;
  cacheBlock.pBlockAgg = pBlock->pBlockAgg;
  pBlock->pDataBlock = NULL;
  pBlock->pBlockAgg = NULL;

  SSDataBlock* pCacheBlock = taosArrayPush(pInfo->anomalySup.blocks, &cacheBlock);
  if (pCacheBlock == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  return TSDB_CODE_SUCCESS;
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

  // make simluate windows for test
  if (pSupp->numOfRows == 0) {
    pSupp->curWin.skey = ts;
  } else if (pSupp->numOfRows % 4 == 0) {
    pSupp->curWin.ekey = ts;
    taosArrayPush(pSupp->windows, &pSupp->curWin);
    pSupp->curWin.skey = ts;
  } else {
  }

  pSupp->numOfRows++;
  uInfo("rows:%" PRId64 ", buf:%s type:%d", pSupp->numOfRows, buf, valType);
}

static int32_t anomalyFindWindow(SAnomalyWindowSupp* pSupp, TSKEY key) {
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pSupp->windows); ++i) {
    STimeWindow* pWindow = taosArrayGet(pSupp->windows, i);
    if (key >= pWindow->skey && key < pWindow->skey) {
      pSupp->curWin = *pWindow;
      return 0;
    }
  }
  return -1;
}

static void anomalyWindowBuildResult(SOperatorInfo* pOperator, SSDataBlock** ppRes) {}

static void anomalyAnalysisWindow(SOperatorInfo* pOperator) {}

static void anomalyAggregateBlocks(SOperatorInfo* pOperator) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SAnomalyWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                  pExprSup = &pOperator->exprSupp;
  SSDataBlock*                pRes = pInfo->binfo.pRes;
  SAnomalyWindowSupp*         pSupp = &pInfo->anomalySup;
  SWindowRowsSup*             pRowSup = &pInfo->anomalyWinRowSup;
  SSDataBlock*                pBlock = NULL;
  SColumnInfoData*            pValCol = NULL;
  SColumnInfoData*            pTsCol = NULL;
  struct SColumnDataAgg*      pAgg = NULL;
  int32_t                     numOfOutput = pOperator->exprSupp.numOfExprs;
  int32_t                     numOfBlocks = (int32_t)taosArrayGetSize(pSupp->blocks);
  int32_t                     winRows = 0;
  const bool                  masterScan = true;
  const int64_t               gid = pSupp->groupId;

  uInfo("group:%" PRId64 ", aggregate blocks, numOfBlocks:%d", pSupp->groupId, numOfBlocks);
  pRes->info.id.groupId = pSupp->groupId;

  for (int32_t i = 0; i < numOfBlocks; ++i) {
    pBlock = taosArrayGet(pSupp->blocks, i);
    if (pBlock == NULL) break;
    pValCol = taosArrayGet(pBlock->pDataBlock, pInfo->anomalyCol.slotId);
    if (pValCol == NULL) break;
    pTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
    if (pTsCol == NULL) break;

    for (int32_t j = 0; j < pBlock->info.rows; ++j) {
      pAgg = (pBlock->pBlockAgg != NULL) ? &pBlock->pBlockAgg[pInfo->anomalyCol.slotId] : NULL;
      if (colDataIsNull(pValCol, pBlock->info.rows, j, pAgg)) continue;
      anomalyCacheRow(pSupp, ((TSKEY*)pTsCol->pData)[j], colDataGetData(pValCol, j), pValCol->info.type);
    }
  }

  anomalyAnalysisWindow(pOperator);
  int32_t numOfWindows = taosArrayGetSize(pSupp->windows);
  uInfo("group:%" PRId64 ", analysis anomaly window:%d, rows:%" PRId64, pSupp->groupId, numOfWindows, pSupp->numOfRows);
  if (numOfWindows <= 0) goto _OVER;

  if (numOfWindows > pBlock->info.capacity) {
    code = blockDataEnsureCapacity(pRes, numOfWindows);
    QUERY_CHECK_CODE(code, lino, _OVER);
  }

  STimeWindow* pWindow = taosArrayGet(pSupp->windows, 0);
  pSupp->curWin = *pWindow;
  pRowSup->win.skey = pSupp->curWin.skey;

  for (int32_t i = 0; i < numOfBlocks; ++i) {
    pBlock = taosArrayGet(pSupp->blocks, i);
    if (pBlock == NULL) break;
    pValCol = taosArrayGet(pBlock->pDataBlock, pInfo->anomalyCol.slotId);
    if (pValCol == NULL) break;
    pTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
    if (pTsCol == NULL) break;
    TSKEY* tsList = (TSKEY*)pTsCol->pData;

    for (int32_t j = 0; j < pBlock->info.rows; ++j) {
      TSKEY key = tsList[j];
      if (key >= pSupp->curWin.skey && key < pSupp->curWin.ekey) {
        if (winRows == 0) {
          doKeepNewWindowStartInfo(pRowSup, tsList, j, gid);
        }
        doKeepTuple(pRowSup, tsList[j], gid);
        winRows++;
      } else {
        if (winRows > 0) {
          winRows = 0;

          code = setResultRowInitCtx(&pSupp->resultRow, pExprSup->pCtx, pExprSup->numOfExprs,
                                     pExprSup->rowEntryInfoOffset);
          QUERY_CHECK_CODE(code, lino, _OVER);

          updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pSupp->curWin, 0);
          applyAggFunctionOnPartialTuples(pTaskInfo, pExprSup->pCtx, &pInfo->twAggSup.timeWindowData,
                                          pRowSup->startRowIndex, pRowSup->numOfRows, pBlock->info.rows, numOfOutput);

          doUpdateNumOfRows(pExprSup->pCtx, &pSupp->resultRow, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
          copyResultrowToDataBlock(pExprSup->pExprInfo, pExprSup->numOfExprs, &pSupp->resultRow, pExprSup->pCtx, pRes,
                                   pExprSup->rowEntryInfoOffset, pTaskInfo);
          pRes->info.rows += pSupp->resultRow.numOfRows;
          memset(pRowSup, 0, sizeof(SWindowRowsSup));
        }
        if (anomalyFindWindow(pSupp, tsList[j]) == 0) {
          j--;
        } else {
          uInfo("group:%" PRId64 ", anomaly window not found for ts:%" PRId64, pSupp->groupId, tsList[j]);
        }
      }
    }
  }

  code = doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
  QUERY_CHECK_CODE(code, lino, _OVER);

_OVER:
  taosArrayClear(pSupp->blocks);
  taosArrayClear(pSupp->windows);
  pSupp->numOfRows = 0;
  pSupp->curWin.ekey = 0;
  pSupp->curWin.skey = 0;
  memset(pRowSup, 0, sizeof(SWindowRowsSup));
}
