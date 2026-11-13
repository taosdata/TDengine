/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License
 * for more details.
 *
 * TIME_ALIGN execution stage shared by the scan-internal (TableScan) and
 * Projection hosts.  It evaluates fill_forward()/fill_next() over the align
 * output columns of a data block stream and, for NEXT, holds rows whose
 * successor is not known yet.  State is kept per series (uid) so interleaved
 * blocks of a table merge scan never leak values across tables.
 */

#include "alignstage.h"
#include "executorInt.h"
#include "functionMgt.h"
#include "nodes.h"
#include "os.h"
#include "querytask.h"
#include "taoserror.h"
#include "tcommon.h"
#include "tdatablock.h"

// Upper bound on rows held while a NEXT successor is unknown.  A series with a
// longer all-NULL tail fails the query instead of growing without bound.
#define ALIGN_STAGE_DEFAULT_MAX_HELD_ROWS (1000 * 1000LL)

typedef struct SAlignSeriesState {
  uint64_t uid;
  int8_t   phase;
  SExprSupp sup;      // per-series function contexts and result buffers
  char*     pRowBuf;  // backing storage for sup.pCtx[i].resultInfo
  SSDataBlock* pHold;
  bool         holdPublished;
  bool         holdUnpublished;
  struct SAlignSeriesState* pNext;
} SAlignSeriesState;

struct SAlignStage {
  SExprInfo* pExprInfo;
  int32_t    numOfExprs;
  SAlignStageCol* pCols;
  int32_t    numOfCols;
  int32_t    tsSlotId;
  int32_t    spanTsSlotId;  // time-axis slot for the optional range parameter
  char       tzName[TD_TIMEZONE_LEN];
  bool       mono;
  int8_t     order;
  bool       desc;
  bool       hasNext;
  SExecTaskInfo* pTaskInfo;
  SHashObj*  pSeries;       // key: uid -> SAlignSeriesState*
  SAlignSeriesState* pSeriesHead;
  SAlignSeriesState* pFlushCursor;
  int64_t    heldRows;
  int64_t    maxHeldRows;
};

static SNode* makeAlignStageTinyIntValue(int8_t value) {
  SValueNode* pVal = NULL;
  if (TSDB_CODE_SUCCESS != nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal)) {
    return NULL;
  }
  pVal->node.resType =
      (SDataType){.type = TSDB_DATA_TYPE_TINYINT, .bytes = tDataTypes[TSDB_DATA_TYPE_TINYINT].bytes};
  if (TSDB_CODE_SUCCESS != nodesSetValueNodeValue(pVal, &value)) {
    nodesDestroyNode((SNode*)pVal);
    return NULL;
  }
  pVal->translate = true;
  pVal->isNull = false;
  return (SNode*)pVal;
}

// Append fill function args 2..6 for the optional time-range parameter:
// ts column, spanValue, spanUnit, timezone name and monotonic flag.
static int32_t appendTimeAlignSpanParams(SAlignStage* pStage, SFunctionNode* pFunc, const SAlignStageCol* pCol) {
  if (!pCol->hasSpan) return TSDB_CODE_SUCCESS;
  if (pStage->spanTsSlotId < 0) {
    qError("TIME_ALIGN: no timestamp slot for the time range of column '%s'", pCol->name);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SColumnNode* pTs = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pTs);
  if (TSDB_CODE_SUCCESS != code) return code;
  pTs->node.resType = (SDataType){.type = TSDB_DATA_TYPE_TIMESTAMP,
                                  .bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes,
                                  .precision = TSDB_TIME_PRECISION_MILLI};
  pTs->slotId = pStage->spanTsSlotId;
  pTs->colId = 0;
  code = nodesListMakeStrictAppend(&pFunc->pParameterList, (SNode*)pTs);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pTs);
    return code;
  }

  SNode* pNode = NULL;
  code = nodesMakeValueNodeFromInt64(pCol->spanValue, &pNode);
  if (TSDB_CODE_SUCCESS != code) return code;
  code = nodesListMakeStrictAppend(&pFunc->pParameterList, pNode);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNode);
    return code;
  }

  pNode = makeAlignStageTinyIntValue(pCol->spanUnit);
  if (pNode == NULL) return terrno;
  code = nodesListMakeStrictAppend(&pFunc->pParameterList, pNode);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNode);
    return code;
  }

  SValueNode* pTz = NULL;
  code = nodesMakeValueNodeFromString(pStage->tzName, &pTz);
  if (TSDB_CODE_SUCCESS != code) return code;
  code = nodesListMakeStrictAppend(&pFunc->pParameterList, (SNode*)pTz);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pTz);
    return code;
  }

  // Bit 0 = monotonic delivery, bit 1 = descending timestamp order.
  pNode = makeAlignStageTinyIntValue((int8_t)((pStage->mono ? 0x1 : 0) | (pStage->desc ? 0x2 : 0)));
  if (pNode == NULL) return terrno;
  code = nodesListMakeStrictAppend(&pFunc->pParameterList, pNode);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNode);
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildTimeAlignExprInfo(SAlignStage* pStage) {
  SNodeList* pTargets = NULL;
  int32_t    code = nodesMakeList(&pTargets);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  for (int32_t i = 0; i < pStage->numOfCols; ++i) {
    const SAlignStageCol* pCol = &pStage->pCols[i];

    SColumnNode* pParam = NULL;
    code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pParam);
    if (TSDB_CODE_SUCCESS != code) goto _return;
    pParam->node.resType = pCol->type;
    pParam->slotId = pCol->srcSlotId;
    pParam->colId = 0;

    SFunctionNode* pFunc = NULL;
    code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pParam);
      goto _return;
    }
    // A descending stream delivers later timestamps first, so the value a row
    // must take comes from the other side of the stream: PREV is resolved with
    // the pending-row filler and NEXT with the running-state filler.
    int8_t method = pCol->method;
    if (pStage->desc) {
      method = (method == ALIGN_METHOD_NEXT) ? ALIGN_METHOD_PREV : ALIGN_METHOD_NEXT;
    }
    tstrncpy(pFunc->functionName, (method == ALIGN_METHOD_NEXT) ? "fill_next" : "fill_forward",
             TSDB_FUNC_NAME_LEN);
    code = nodesListMakeStrictAppend(&pFunc->pParameterList, (SNode*)pParam);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pParam);
      nodesDestroyNode((SNode*)pFunc);
      goto _return;
    }
    pParam = NULL;
    code = appendTimeAlignSpanParams(pStage, pFunc, pCol);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pFunc);
      goto _return;
    }
    pFunc->node.resType = pCol->type;
    tstrncpy(pFunc->node.aliasName, pCol->name, TSDB_COL_NAME_LEN);
    code = fmGetFuncInfo(pFunc, NULL, 0);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pFunc);
      goto _return;
    }

    STargetNode* pTarget = NULL;
    code = nodesMakeNode(QUERY_NODE_TARGET, (SNode**)&pTarget);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pFunc);
      goto _return;
    }
    pTarget->pExpr = (SNode*)pFunc;
    pTarget->slotId = pCol->outSlotId;
    code = nodesListMakeStrictAppend(&pTargets, (SNode*)pTarget);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pFunc);
      nodesDestroyNode((SNode*)pTarget);
      goto _return;
    }
  }

  code = createExprInfo(pTargets, NULL, &pStage->pExprInfo, &pStage->numOfExprs);
  if (TSDB_CODE_SUCCESS != code) goto _return;

  // The expression infos now own the function nodes; detach them from the
  // target wrappers before dropping the wrappers.
  SNode* pNode = NULL;
  FOREACH(pNode, pTargets) { ((STargetNode*)pNode)->pExpr = NULL; }

_return:
  nodesDestroyList(pTargets);
  return code;
}

static void destroyAlignSeries(SAlignStage* pStage, SAlignSeriesState* pState) {
  if (pState == NULL) return;
  if (pState->sup.pCtx != NULL) {
    destroySqlFunctionCtx(pState->sup.pCtx, pStage->pExprInfo, pStage->numOfExprs);
    pState->sup.pCtx = NULL;
  }
  taosMemoryFreeClear(pState->sup.rowEntryInfoOffset);
  taosMemoryFreeClear(pState->pRowBuf);
  blockDataDestroy(pState->pHold);
  pState->pHold = NULL;
  taosMemoryFree(pState);
}

int32_t alignStageCreate(SAlignStage** ppStage, const SAlignStageCol* pCols, int32_t numOfCols,
                         int32_t tsSlotId, int32_t spanTsSlotId, const char* tzName, bool mono,
                         int8_t order, SExecTaskInfo* pTaskInfo) {
  *ppStage = NULL;
  if (pCols == NULL || numOfCols <= 0) return TSDB_CODE_SUCCESS;

  SAlignStage* pStage = taosMemoryCalloc(1, sizeof(SAlignStage));
  if (pStage == NULL) return terrno;

  pStage->numOfCols = numOfCols;
  pStage->tsSlotId = tsSlotId;
  pStage->spanTsSlotId = spanTsSlotId;
  pStage->mono = mono;
  pStage->order = order;
  pStage->desc = (order == TSDB_ORDER_DESC);
  if (tzName != NULL) {
    tstrncpy(pStage->tzName, tzName, sizeof(pStage->tzName));
  }
  pStage->pTaskInfo = pTaskInfo;
  pStage->maxHeldRows = ALIGN_STAGE_DEFAULT_MAX_HELD_ROWS;
  pStage->pCols = taosMemoryMalloc(sizeof(SAlignStageCol) * numOfCols);
  if (pStage->pCols == NULL) {
    taosMemoryFree(pStage);
    return terrno;
  }
  for (int32_t i = 0; i < numOfCols; ++i) {
    pStage->pCols[i] = pCols[i];
    // The pending-row host path is needed by whichever method maps to
    // fill_next after the stream direction is applied.
    int8_t method = pCols[i].method;
    if (pStage->desc) {
      method = (method == ALIGN_METHOD_NEXT) ? ALIGN_METHOD_PREV : ALIGN_METHOD_NEXT;
    }
    if (method == ALIGN_METHOD_NEXT) {
      pStage->hasNext = true;
    }
  }

  int32_t code = buildTimeAlignExprInfo(pStage);
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(pStage->pCols);
    taosMemoryFree(pStage);
    return code;
  }

  pStage->pSeries = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pStage->pSeries == NULL) {
    code = terrno;
    alignStageDestroy(pStage);
    return code;
  }

  *ppStage = pStage;
  return TSDB_CODE_SUCCESS;
}

void alignStageSetMaxHeldRows(SAlignStage* pStage, int64_t maxHeldRows) {
  if (pStage != NULL && maxHeldRows > 0) {
    pStage->maxHeldRows = maxHeldRows;
  }
}

bool alignStageHasNext(const SAlignStage* pStage) { return pStage != NULL && pStage->hasNext; }

static SAlignSeriesState* findOrCreateSeries(SAlignStage* pStage, uint64_t uid, int8_t phase, int32_t* pCode) {
  SAlignSeriesState** ppState = taosHashGet(pStage->pSeries, &uid, sizeof(uid));
  SAlignSeriesState*  pState = (ppState != NULL) ? *ppState : NULL;
  if (pState != NULL) {
    if (pState->phase != phase) {
      // Two-pass scans restart the stream: drop the old phase's contexts so
      // the new pass starts without predecessor state.
      if (pState->holdUnpublished) {
        *pCode = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        qError("TIME_ALIGN: series %" PRIu64 " changed scan phase with unpublished rows", uid);
        return NULL;
      }
      blockDataDestroy(pState->pHold);
      pState->pHold = NULL;
      pState->holdPublished = false;
      pState->holdUnpublished = false;
      for (int32_t i = 0; i < pState->sup.numOfExprs; ++i) {
        SqlFunctionCtx* pCtx = &pState->sup.pCtx[i];
        if (pCtx->resultInfo != NULL && pCtx->fpSet.cleanup != NULL) {
          pCtx->fpSet.cleanup(pCtx);
        }
        if (pCtx->resultInfo != NULL) {
          pCtx->resultInfo->initialized = false;
        }
      }
      pState->phase = phase;
      return pState;
    }
    return pState;
  }

  pState = taosMemoryCalloc(1, sizeof(SAlignSeriesState));
  if (pState == NULL) {
    *pCode = terrno;
    return NULL;
  }
  pState->uid = uid;
  pState->phase = phase;
  pState->sup.pExprInfo = pStage->pExprInfo;
  pState->sup.numOfExprs = pStage->numOfExprs;
  pState->sup.pCtx = createSqlFunctionCtx(pStage->pExprInfo, pStage->numOfExprs,
                                          &pState->sup.rowEntryInfoOffset,
                                          &pStage->pTaskInfo->storageAPI.functionStore);
  if (pState->sup.pCtx == NULL) {
    *pCode = terrno;
    destroyAlignSeries(pStage, pState);
    return NULL;
  }

  int64_t totalSize = 0;
  for (int32_t i = 0; i < pStage->numOfExprs; ++i) {
    totalSize += sizeof(SResultRowEntryInfo) + pState->sup.pCtx[i].resDataInfo.interBufSize;
  }
  pState->pRowBuf = taosMemoryCalloc(1, totalSize);
  if (pState->pRowBuf == NULL) {
    *pCode = terrno;
    destroyAlignSeries(pStage, pState);
    return NULL;
  }
  int64_t off = 0;
  for (int32_t i = 0; i < pStage->numOfExprs; ++i) {
    pState->sup.pCtx[i].resultInfo = (SResultRowEntryInfo*)(pState->pRowBuf + off);
    pState->sup.pCtx[i].resultInfo->initialized = false;
    off += sizeof(SResultRowEntryInfo) + pState->sup.pCtx[i].resDataInfo.interBufSize;
  }

  *pCode = taosHashPut(pStage->pSeries, &uid, sizeof(uid), &pState, POINTER_BYTES);
  if (TSDB_CODE_SUCCESS != *pCode) {
    destroyAlignSeries(pStage, pState);
    return NULL;
  }

  pState->pNext = pStage->pSeriesHead;
  pStage->pSeriesHead = pState;
  return pState;
}

static bool seriesAnyPending(const SAlignSeriesState* pState) {
  for (int32_t i = 0; i < pState->sup.numOfExprs; ++i) {
    SqlFunctionCtx* pCtx = &pState->sup.pCtx[i];
    if (pCtx->fpSet.hasPending != NULL && pCtx->fpSet.hasPending(pCtx)) {
      return true;
    }
  }
  return false;
}

static int32_t appendBlockToHold(SAlignStage* pStage, SAlignSeriesState* pState, SSDataBlock* pIn) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pState->pHold == NULL) {
    code = createOneDataBlock(pIn, false, &pState->pHold);
    if (TSDB_CODE_SUCCESS != code) return code;
  } else if (pState->holdPublished) {
    blockDataCleanup(pState->pHold);
    pState->holdPublished = false;
  }

  if (pStage->heldRows + pIn->info.rows > pStage->maxHeldRows) {
    qError("TIME_ALIGN: held rows exceed the limit (%" PRId64 "), series:%" PRIu64,
           pStage->maxHeldRows, pState->uid);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t rowsBefore = pState->pHold->info.rows;
  code = blockDataEnsureCapacity(pState->pHold, rowsBefore + pIn->info.rows);
  if (TSDB_CODE_SUCCESS != code) return code;

  pState->pHold->info.id = pIn->info.id;
  pState->pHold->info.scanFlag = pIn->info.scanFlag;
  if (pIn->info.parTbName[0] != '\0') {
    tstrncpy(pState->pHold->info.parTbName, pIn->info.parTbName, TSDB_TABLE_NAME_LEN);
  }

  int32_t capacity = (int32_t)pState->pHold->info.capacity;
  int32_t numOfCols = (int32_t)taosArrayGetSize(pState->pHold->pDataBlock);
  if (numOfCols > (int32_t)taosArrayGetSize(pIn->pDataBlock)) {
    numOfCols = (int32_t)taosArrayGetSize(pIn->pDataBlock);
  }
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDst = taosArrayGet(pState->pHold->pDataBlock, i);
    SColumnInfoData* pSrc = taosArrayGet(pIn->pDataBlock, i);
    if (pDst == NULL || pSrc == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    code = colDataMergeCol(pDst, rowsBefore, &capacity, pSrc, pIn->info.rows);
    if (TSDB_CODE_SUCCESS != code) return code;
  }
  pState->holdUnpublished = true;
  pStage->heldRows += pIn->info.rows;
  return TSDB_CODE_SUCCESS;
}

static int32_t applyAlignToHold(SAlignStage* pStage, SAlignSeriesState* pState, SSDataBlock* pIn) {
  int32_t code = setInputDataBlock(&pState->sup, pIn,
                                   (pStage->order == TSDB_ORDER_DESC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC,
                                   pIn->info.scanFlag, false);
  if (TSDB_CODE_SUCCESS != code) return code;

  for (int32_t i = 0; i < pStage->numOfExprs; ++i) {
    pState->sup.pCtx[i].pOutput = NULL;
  }
  code = projectApplyFunctions(pStage->pExprInfo, pState->pHold, pIn, pState->sup.pCtx, pStage->numOfExprs,
                               NULL, NULL, pStage->pTaskInfo);
  if (TSDB_CODE_SUCCESS != code) return code;

  if (pStage->tsSlotId >= 0) {
    code = blockDataUpdateTsWindow(pState->pHold, pStage->tsSlotId);
  }
  return code;
}

int32_t alignStageFeed(SAlignStage* pStage, uint64_t uid, int8_t phase, SSDataBlock* pIn,
                       SSDataBlock** ppReady) {
  *ppReady = NULL;
  if (pStage == NULL || pIn == NULL || pIn->info.rows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t            code = TSDB_CODE_SUCCESS;
  SAlignSeriesState* pState = findOrCreateSeries(pStage, uid, phase, &code);
  if (pState == NULL) return code;

  code = appendBlockToHold(pStage, pState, pIn);
  if (TSDB_CODE_SUCCESS != code) return code;

  code = applyAlignToHold(pStage, pState, pIn);
  if (TSDB_CODE_SUCCESS != code) return code;

  if (pStage->hasNext && seriesAnyPending(pState)) {
    return TSDB_CODE_SUCCESS;
  }

  pState->holdPublished = true;
  pState->holdUnpublished = false;
  pStage->heldRows -= pState->pHold->info.rows;
  *ppReady = pState->pHold;
  return TSDB_CODE_SUCCESS;
}

int32_t alignStageFlushSeries(SAlignStage* pStage, uint64_t uid, SSDataBlock** ppBlock) {
  *ppBlock = NULL;
  if (pStage == NULL) return TSDB_CODE_SUCCESS;

  SAlignSeriesState** ppState = taosHashGet(pStage->pSeries, &uid, sizeof(uid));
  SAlignSeriesState*  pState = (ppState != NULL) ? *ppState : NULL;
  if (pState == NULL || !pState->holdUnpublished) {
    return TSDB_CODE_SUCCESS;
  }

  pState->holdPublished = true;
  pState->holdUnpublished = false;
  pStage->heldRows -= pState->pHold->info.rows;
  *ppBlock = pState->pHold;
  return TSDB_CODE_SUCCESS;
}

int32_t alignStageFlushNext(SAlignStage* pStage, SSDataBlock** ppBlock) {
  *ppBlock = NULL;
  if (pStage == NULL) return TSDB_CODE_SUCCESS;

  if (pStage->pFlushCursor == NULL) {
    pStage->pFlushCursor = pStage->pSeriesHead;
  }
  while (pStage->pFlushCursor != NULL) {
    SAlignSeriesState* pState = pStage->pFlushCursor;
    pStage->pFlushCursor = pState->pNext;
    if (!pState->holdUnpublished) {
      continue;
    }
    pState->holdPublished = true;
    pState->holdUnpublished = false;
    pStage->heldRows -= pState->pHold->info.rows;
    *ppBlock = pState->pHold;
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t alignStageReset(SAlignStage* pStage) {
  if (pStage == NULL) return TSDB_CODE_SUCCESS;
  SAlignSeriesState* pState = pStage->pSeriesHead;
  while (pState != NULL) {
    SAlignSeriesState* pNext = pState->pNext;
    destroyAlignSeries(pStage, pState);
    pState = pNext;
  }
  pStage->pSeriesHead = NULL;
  pStage->pFlushCursor = NULL;
  pStage->heldRows = 0;
  if (pStage->pSeries != NULL) {
    taosHashClear(pStage->pSeries);
  }
  return TSDB_CODE_SUCCESS;
}

void alignStageDestroy(SAlignStage* pStage) {
  if (pStage == NULL) return;
  alignStageReset(pStage);
  taosHashCleanup(pStage->pSeries);
  if (pStage->pExprInfo != NULL) {
    destroyExprInfo(pStage->pExprInfo, pStage->numOfExprs);
    taosMemoryFreeClear(pStage->pExprInfo);
  }
  taosMemoryFreeClear(pStage->pCols);
  taosMemoryFree(pStage);
}
