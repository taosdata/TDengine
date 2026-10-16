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
#include "osLz4.h"
#include "query.h"
#include "querytask.h"
#include "taoserror.h"
#include "tchecksum.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "tglobal.h"
#include "thash.h"
#include "tlog.h"
#include "ttime.h"

typedef enum SResultTsInterpType {
  RESULT_ROW_START_INTERP = 1,
  RESULT_ROW_END_INTERP = 2,
} SResultTsInterpType;

typedef struct SOpenWindowInfo {
  SResultRowPosition pos;
  uint64_t           groupId;
} SOpenWindowInfo;

#define FAST_INTERVAL_LOCAL_RESULT_BATCH_ROWS 256

typedef enum EExIntervalCellType {
  EX_INTERVAL_CELL_WSTART = 1,
  EX_INTERVAL_CELL_WEND,
  EX_INTERVAL_CELL_WDURATION,
  EX_INTERVAL_CELL_COUNT,
  EX_INTERVAL_CELL_SUM,
  EX_INTERVAL_CELL_AVG,
} EExIntervalCellType;

typedef struct SExIntervalCell {
  double  sum;
  int64_t count;
} SExIntervalCell;

typedef struct SExIntervalEntry {
  int32_t          errCode;
  int32_t          ready;
  int32_t          numOfWindows;
  int32_t          numOfAggs;
  int32_t          activeCount;
  int32_t          minActiveWindow;
  int32_t          maxActiveWindow;
  int64_t          firstWindowStart;
  int64_t          interval;
  int64_t*         pRows;
  SExIntervalCell* pCells;
  int16_t          slotIds[EX_INTERVAL_MAX_FUNCTIONS];
  int8_t           cellTypes[EX_INTERVAL_MAX_FUNCTIONS];
} SExIntervalEntry;

typedef struct SExIntervalInfo {
  SExIntervalEntry* pEntry;
  double*           pLocalSums;
  int64_t*          pLocalRows;
  int64_t*          pLocalCounts;
  int32_t*          pLocalWindowIndexes;
  int32_t*          pLocalWindows;
  int32_t            localCapacity;
  int32_t            numOfLocalWindows;
  int32_t            activeCount;
  int32_t            minActiveWindow;
  int32_t            maxActiveWindow;
  void*             pHashEntry;
  uint64_t          queryId;
  int8_t            localSumIndex[EX_INTERVAL_MAX_FUNCTIONS];
  int8_t            localCountIndex[EX_INTERVAL_MAX_FUNCTIONS];
  int8_t            mergeCellIndex[EX_INTERVAL_MAX_FUNCTIONS];
  int8_t            numOfLocalSums;
  int8_t            numOfLocalCounts;
  bool              root;
  bool              finalized;
} SExIntervalInfo;

static SHashObj*   gExIntervalHash = NULL;
static TdThreadOnce gExIntervalHashOnce = PTHREAD_ONCE_INIT;

static bool exIntervalAddInt64(int64_t lhs, int64_t rhs, int64_t* pResult) {
  if (NULL == pResult || (rhs > 0 && lhs > INT64_MAX - rhs) || (rhs < 0 && lhs < INT64_MIN - rhs)) {
    return false;
  }

  *pResult = lhs + rhs;
  return true;
}

static bool exIntervalMulInt64(int64_t lhs, int64_t rhs, int64_t* pResult) {
  if (NULL == pResult) {
    return false;
  }
  if (0 == lhs || 0 == rhs) {
    *pResult = 0;
    return true;
  }

  if ((lhs > 0 && ((rhs > 0 && lhs > INT64_MAX / rhs) || (rhs < 0 && rhs < INT64_MIN / lhs))) ||
      (lhs < 0 && ((rhs > 0 && lhs < INT64_MIN / rhs) || (rhs < 0 && lhs < INT64_MAX / rhs)))) {
    return false;
  }

  *pResult = lhs * rhs;
  return true;
}

static void destroyExIntervalEntry(SExIntervalEntry* pEntry) {
  if (NULL == pEntry) {
    return;
  }
  taosMemoryFreeClear(pEntry->pRows);
  taosMemoryFreeClear(pEntry->pCells);
  taosMemoryFree(pEntry);
}

static void exIntervalAtomicMin(int32_t* pValue, int32_t value) {
  int32_t current = atomic_load_32(pValue);
  while (value < current) {
    int32_t previous = atomic_val_compare_exchange_32(pValue, current, value);
    if (previous == current) {
      return;
    }
    current = previous;
  }
}

static void exIntervalAtomicMax(int32_t* pValue, int32_t value) {
  int32_t current = atomic_load_32(pValue);
  while (value > current) {
    int32_t previous = atomic_val_compare_exchange_32(pValue, current, value);
    if (previous == current) {
      return;
    }
    current = previous;
  }
}

static void exIntervalPublishLocalRange(SExIntervalInfo* pExInfo) {
  if (pExInfo == NULL || pExInfo->pEntry == NULL || pExInfo->activeCount <= 0) {
    return;
  }
  atomic_add_fetch_32(&pExInfo->pEntry->activeCount, pExInfo->activeCount);
  exIntervalAtomicMin(&pExInfo->pEntry->minActiveWindow, pExInfo->minActiveWindow);
  exIntervalAtomicMax(&pExInfo->pEntry->maxActiveWindow, pExInfo->maxActiveWindow);
}

static void destroyExIntervalHashEntry(void* data) {
  if (NULL != data) {
    destroyExIntervalEntry(*(SExIntervalEntry**)data);
  }
}

static void cleanupExIntervalHash(void) {
  taosHashCleanup(gExIntervalHash);
  gExIntervalHash = NULL;
}

static void initExIntervalHash(void) {
  gExIntervalHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == gExIntervalHash) {
    qError("EX_INTERVAL failed to initialize shared result hash, code:%s", tstrerror(terrno));
    return;
  }
  taosHashSetFreeFp(gExIntervalHash, destroyExIntervalHashEntry);
  if (0 != atexit(cleanupExIntervalHash)) {
    qError("EX_INTERVAL failed to register shared result hash cleanup");
    cleanupExIntervalHash();
  }
}

static void releaseExIntervalInfo(void** ppInfo) {
  SExIntervalInfo** ppExInfo = (SExIntervalInfo**)ppInfo;
  if (NULL == ppExInfo || NULL == *ppExInfo) {
    return;
  }
  SExIntervalInfo* pExInfo = *ppExInfo;
  if (NULL != pExInfo->pHashEntry && NULL != gExIntervalHash) {
    int32_t code = taosHashRemove(gExIntervalHash, &pExInfo->queryId, sizeof(pExInfo->queryId));
    if (TSDB_CODE_SUCCESS != code && TSDB_CODE_NOT_FOUND != code) {
      qError("EX_INTERVAL failed to discard shared result, qId:%" PRIu64 ", code:%s", pExInfo->queryId,
             tstrerror(code));
    }
  }
  if (NULL != pExInfo->pHashEntry) {
    taosHashRelease(gExIntervalHash, pExInfo->pHashEntry);
    pExInfo->pHashEntry = NULL;
  }
  taosMemoryFreeClear(pExInfo->pLocalSums);
  taosMemoryFreeClear(pExInfo->pLocalRows);
  taosMemoryFreeClear(pExInfo->pLocalCounts);
  taosMemoryFreeClear(pExInfo->pLocalWindowIndexes);
  taosMemoryFreeClear(pExInfo->pLocalWindows);
  taosMemoryFreeClear(*ppExInfo);
}

static int32_t acquireExIntervalEntry(SExIntervalInfo* pExInfo) {
  if (NULL == pExInfo || NULL == gExIntervalHash) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (NULL != pExInfo->pHashEntry && NULL != pExInfo->pEntry) {
    return TSDB_CODE_SUCCESS;
  }

  pExInfo->pHashEntry = taosHashAcquire(gExIntervalHash, &pExInfo->queryId, sizeof(pExInfo->queryId));
  if (NULL == pExInfo->pHashEntry || NULL == *(SExIntervalEntry**)pExInfo->pHashEntry) {
    if (NULL != pExInfo->pHashEntry) {
      taosHashRelease(gExIntervalHash, pExInfo->pHashEntry);
      pExInfo->pHashEntry = NULL;
    }
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  pExInfo->pEntry = *(SExIntervalEntry**)pExInfo->pHashEntry;
  return TSDB_CODE_SUCCESS;
}

static void setExIntervalError(SExIntervalEntry* pEntry, int32_t code) {
  if (NULL != pEntry && TSDB_CODE_SUCCESS != code) {
    (void)atomic_val_compare_exchange_32(&pEntry->errCode, TSDB_CODE_SUCCESS, code);
  }
}

static int32_t exIntervalGetFuncType(const SqlFunctionCtx* pCtx, int32_t* pFuncType) {
  if (NULL == pCtx || NULL == pFuncType || NULL == pCtx->pExpr || NULL == pCtx->pExpr->pExpr ||
      NULL == pCtx->pExpr->pExpr->_function.pFunctNode) {
    return TSDB_CODE_INVALID_PARA;
  }
  *pFuncType = pCtx->pExpr->pExpr->_function.pFunctNode->funcType;
  return TSDB_CODE_SUCCESS;
}

static int32_t exIntervalGetCellType(const SqlFunctionCtx* pCtx, int8_t* pType) {
  if (NULL == pType) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t funcType = 0;
  int32_t code = exIntervalGetFuncType(pCtx, &funcType);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  switch (funcType) {
    case FUNCTION_TYPE_WSTART:
      *pType = EX_INTERVAL_CELL_WSTART;
      break;
    case FUNCTION_TYPE_WEND:
      *pType = EX_INTERVAL_CELL_WEND;
      break;
    case FUNCTION_TYPE_WDURATION:
      *pType = EX_INTERVAL_CELL_WDURATION;
      break;
    case FUNCTION_TYPE_COUNT:
      *pType = EX_INTERVAL_CELL_COUNT;
      break;
    case FUNCTION_TYPE_SUM:
      *pType = EX_INTERVAL_CELL_SUM;
      break;
    case FUNCTION_TYPE_AVG:
    case FUNCTION_TYPE_AVG_PARTIAL:
    case FUNCTION_TYPE_AVG_MERGE:
      *pType = EX_INTERVAL_CELL_AVG;
      break;
    default:
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t exIntervalGetMergeSourceSlot(const SExprInfo* pExpr, int16_t* pSlot) {
  if (NULL == pExpr || NULL == pSlot) {
    return TSDB_CODE_INVALID_PARA;
  }

  for (int32_t i = 0; i < pExpr->base.numOfParams; ++i) {
    const SFunctParam* pParam = &pExpr->base.pParam[i];
    if (FUNC_PARAM_TYPE_COLUMN == pParam->type && NULL != pParam->pCol) {
      *pSlot = pParam->pCol->slotId;
      return TSDB_CODE_SUCCESS;
    }
  }

  *pSlot = pExpr->base.resSchema.slotId;
  return TSDB_CODE_SUCCESS;
}

static int32_t exIntervalInitCells(SExIntervalEntry* pEntry, SExprSupp* pSup) {
  if (NULL == pEntry || NULL == pSup || pSup->numOfExprs <= 0 || pSup->numOfExprs > EX_INTERVAL_MAX_FUNCTIONS) {
    return TSDB_CODE_INVALID_PARA;
  }
  pEntry->numOfAggs = pSup->numOfExprs;
  size_t cellCount = (size_t)pEntry->numOfWindows * pEntry->numOfAggs;
  if (cellCount > SIZE_MAX / sizeof(SExIntervalCell)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pEntry->pCells = taosMemoryCalloc(cellCount, sizeof(SExIntervalCell));
  pEntry->pRows = taosMemoryCalloc((size_t)pEntry->numOfWindows, sizeof(int64_t));
  if (NULL == pEntry->pCells || NULL == pEntry->pRows) {
    return terrno;
  }
  for (int32_t i = 0; i < pEntry->numOfAggs; ++i) {
    int8_t type = 0;
    int32_t code = exIntervalGetCellType(&pSup->pCtx[i], &type);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    pEntry->slotIds[i] = pSup->pExprInfo[i].base.resSchema.slotId;
    pEntry->cellTypes[i] = type;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t exIntervalInitEntry(SExIntervalEntry* pEntry, uint64_t count, int64_t firstWindowStart,
                                   int64_t interval, SExprSupp* pSup) {
  if (NULL == pEntry || count > EX_INTERVAL_MAX_WINDOWS || interval <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  pEntry->numOfWindows = (int32_t)count;
  pEntry->firstWindowStart = firstWindowStart;
  pEntry->interval = interval;
  pEntry->activeCount = 0;
  pEntry->minActiveWindow = pEntry->numOfWindows;
  pEntry->maxActiveWindow = -1;
  int32_t code = exIntervalInitCells(pEntry, pSup);
  if (TSDB_CODE_SUCCESS == code) {
    atomic_store_32(&pEntry->ready, 1);
  }
  return code;
}

static int32_t exIntervalInitLocalSums(SExIntervalInfo* pExInfo) {
  if (NULL == pExInfo || NULL == pExInfo->pEntry) {
    return TSDB_CODE_INVALID_PARA;
  }
  SExIntervalEntry* pEntry = pExInfo->pEntry;
  TAOS_MEMSET(pExInfo->localSumIndex, -1, sizeof(pExInfo->localSumIndex));
  TAOS_MEMSET(pExInfo->localCountIndex, -1, sizeof(pExInfo->localCountIndex));
  for (int32_t expr = 0; expr < pEntry->numOfAggs; ++expr) {
    int8_t type = pEntry->cellTypes[expr];
    if (EX_INTERVAL_CELL_SUM == type || EX_INTERVAL_CELL_AVG == type) {
      pExInfo->localSumIndex[expr] = pExInfo->numOfLocalSums++;
    }
    if (EX_INTERVAL_CELL_COUNT == type || EX_INTERVAL_CELL_SUM == type || EX_INTERVAL_CELL_AVG == type) {
      pExInfo->localCountIndex[expr] = pExInfo->numOfLocalCounts++;
    }
  }
  pExInfo->pLocalWindowIndexes = taosMemoryCalloc((size_t)pEntry->numOfWindows, sizeof(int32_t));
  return NULL == pExInfo->pLocalWindowIndexes ? terrno : TSDB_CODE_SUCCESS;
}

static int32_t exIntervalEnsureLocalCapacity(SExIntervalInfo* pExInfo, int32_t required) {
  if (NULL == pExInfo || NULL == pExInfo->pEntry || required <= 0) return TSDB_CODE_INVALID_PARA;
  if (required <= pExInfo->localCapacity) return TSDB_CODE_SUCCESS;

  int32_t capacity = pExInfo->localCapacity > 0 ? pExInfo->localCapacity : 64;
  while (capacity < required) {
    if (capacity > pExInfo->pEntry->numOfWindows / 2) {
      capacity = pExInfo->pEntry->numOfWindows;
      break;
    }
    capacity *= 2;
  }
  if (capacity < required) return TSDB_CODE_OUT_OF_MEMORY;

  int32_t* pWindows = taosMemoryRealloc(pExInfo->pLocalWindows, (size_t)capacity * sizeof(int32_t));
  if (NULL == pWindows) return terrno;
  pExInfo->pLocalWindows = pWindows;
  int64_t* pRows = taosMemoryRealloc(pExInfo->pLocalRows, (size_t)capacity * sizeof(int64_t));
  if (NULL == pRows) return terrno;
  pExInfo->pLocalRows = pRows;
  if (pExInfo->numOfLocalSums > 0) {
    if ((size_t)capacity > SIZE_MAX / pExInfo->numOfLocalSums / sizeof(double)) return TSDB_CODE_OUT_OF_MEMORY;
    double* pSums = taosMemoryRealloc(pExInfo->pLocalSums,
                                      (size_t)capacity * pExInfo->numOfLocalSums * sizeof(double));
    if (NULL == pSums) return terrno;
    pExInfo->pLocalSums = pSums;
  }
  if (pExInfo->numOfLocalCounts > 0) {
    if ((size_t)capacity > SIZE_MAX / pExInfo->numOfLocalCounts / sizeof(int64_t)) return TSDB_CODE_OUT_OF_MEMORY;
    int64_t* pCounts = taosMemoryRealloc(pExInfo->pLocalCounts,
                                         (size_t)capacity * pExInfo->numOfLocalCounts * sizeof(int64_t));
    if (NULL == pCounts) return terrno;
    pExInfo->pLocalCounts = pCounts;
  }
  pExInfo->localCapacity = capacity;
  return TSDB_CODE_SUCCESS;
}

static int32_t exIntervalGetLocalWindow(SExIntervalInfo* pExInfo, int32_t window, int32_t* pLocalIndex) {
  if (NULL == pExInfo || NULL == pExInfo->pEntry || NULL == pExInfo->pLocalWindowIndexes || NULL == pLocalIndex ||
      window < 0 || window >= pExInfo->pEntry->numOfWindows) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t localIndex = pExInfo->pLocalWindowIndexes[window] - 1;
  if (localIndex >= 0) {
    *pLocalIndex = localIndex;
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = exIntervalEnsureLocalCapacity(pExInfo, pExInfo->numOfLocalWindows + 1);
  if (TSDB_CODE_SUCCESS != code) return code;
  localIndex = pExInfo->numOfLocalWindows++;
  pExInfo->pLocalWindowIndexes[window] = localIndex + 1;
  pExInfo->pLocalWindows[localIndex] = window;
  pExInfo->pLocalRows[localIndex] = 0;
  if (pExInfo->numOfLocalSums > 0) {
    TAOS_MEMSET(&pExInfo->pLocalSums[(size_t)localIndex * pExInfo->numOfLocalSums], 0,
                (size_t)pExInfo->numOfLocalSums * sizeof(double));
  }
  if (pExInfo->numOfLocalCounts > 0) {
    TAOS_MEMSET(&pExInfo->pLocalCounts[(size_t)localIndex * pExInfo->numOfLocalCounts], 0,
                (size_t)pExInfo->numOfLocalCounts * sizeof(int64_t));
  }
  pExInfo->activeCount = pExInfo->numOfLocalWindows;
  if (window < pExInfo->minActiveWindow) pExInfo->minActiveWindow = window;
  if (window > pExInfo->maxActiveWindow) pExInfo->maxActiveWindow = window;
  *pLocalIndex = localIndex;
  return TSDB_CODE_SUCCESS;
}

static void exIntervalMergeLocalWindow(SExIntervalInfo* pExInfo, int32_t localIndex) {
  SExIntervalEntry* pEntry = pExInfo->pEntry;
  int32_t window = pExInfo->pLocalWindows[localIndex];
  int64_t localRows = pExInfo->pLocalRows[localIndex];
  if (localRows <= 0) return;
  atomic_add_fetch_64(&pEntry->pRows[window], localRows);
  for (int32_t expr = 0; expr < pEntry->numOfAggs; ++expr) {
    SExIntervalCell* pCell = &pEntry->pCells[(size_t)window * pEntry->numOfAggs + expr];
    int8_t countIndex = pExInfo->localCountIndex[expr];
    if (countIndex >= 0) {
      int64_t count = pExInfo->pLocalCounts[(size_t)localIndex * pExInfo->numOfLocalCounts + countIndex];
      if (count > 0) atomic_add_fetch_64(&pCell->count, count);
    }
    int8_t sumIndex = pExInfo->localSumIndex[expr];
    if (sumIndex >= 0) {
      double sum = pExInfo->pLocalSums[(size_t)localIndex * pExInfo->numOfLocalSums + sumIndex];
      if (sum != 0) atomic_fetch_add_double(&pCell->sum, sum);
    }
  }
}

static int32_t exIntervalMergeLocalSums(SExIntervalInfo* pExInfo) {
  if (NULL == pExInfo || NULL == pExInfo->pEntry ||
      (pExInfo->numOfLocalWindows > 0 && (NULL == pExInfo->pLocalWindows || NULL == pExInfo->pLocalRows)) ||
      (pExInfo->numOfLocalWindows > 0 && pExInfo->numOfLocalSums > 0 && NULL == pExInfo->pLocalSums) ||
      (pExInfo->numOfLocalWindows > 0 && pExInfo->numOfLocalCounts > 0 && NULL == pExInfo->pLocalCounts)) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pExInfo->activeCount <= 0) return TSDB_CODE_SUCCESS;
  for (int32_t localIndex = 0; localIndex < pExInfo->numOfLocalWindows; ++localIndex) {
    exIntervalMergeLocalWindow(pExInfo, localIndex);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t exIntervalInitMergeCellIndexes(SExIntervalInfo* pExInfo, const SExprSupp* pSup) {
  if (NULL == pExInfo || NULL == pExInfo->pEntry || NULL == pSup || pSup->numOfExprs <= 0 ||
      pSup->numOfExprs > EX_INTERVAL_MAX_FUNCTIONS) {
    return TSDB_CODE_INVALID_PARA;
  }

  TAOS_MEMSET(pExInfo->mergeCellIndex, -1, sizeof(pExInfo->mergeCellIndex));
  if (pExInfo->pEntry->numOfAggs < pSup->numOfExprs) {
    qError("EX_INTERVAL has fewer vnode expressions:%d than merge expressions:%d, qId:%" PRIu64,
           pExInfo->pEntry->numOfAggs, pSup->numOfExprs, pExInfo->queryId);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  for (int32_t expr = 0; expr < pSup->numOfExprs; ++expr) {
    int8_t expectedType = 0;
    int32_t code = exIntervalGetCellType(&pSup->pCtx[expr], &expectedType);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    int16_t expectedSlot = 0;
    code = exIntervalGetMergeSourceSlot(&pSup->pExprInfo[expr], &expectedSlot);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    int32_t cell = 0;
    for (; cell < pExInfo->pEntry->numOfAggs; ++cell) {
      if (pExInfo->pEntry->slotIds[cell] == expectedSlot) {
        break;
      }
    }
    if (cell >= pExInfo->pEntry->numOfAggs) {
      qError("EX_INTERVAL failed to map merge expression:%d, inputSlot:%d, type:%d, qId:%" PRIu64, expr,
             expectedSlot, expectedType, pExInfo->queryId);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    pExInfo->mergeCellIndex[expr] = (int8_t)cell;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initExIntervalInfo(SIntervalAggOperatorInfo* pInfo, SOperatorInfo* pOperator, bool root) {
  if (NULL == pInfo || NULL == pOperator || NULL == pOperator->pTaskInfo) return TSDB_CODE_INVALID_PARA;
  int32_t code = taosThreadOnce(&gExIntervalHashOnce, initExIntervalHash);
  if (TSDB_CODE_SUCCESS != code || NULL == gExIntervalHash) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  SExIntervalInfo* pExInfo = taosMemoryCalloc(1, sizeof(SExIntervalInfo));
  if (NULL == pExInfo) return terrno;
  pExInfo->queryId = pOperator->pTaskInfo->id.queryId;
  pExInfo->root = root;

  if (root) {
    code = acquireExIntervalEntry(pExInfo);
    if (TSDB_CODE_SUCCESS != code) {
      taosMemoryFree(pExInfo);
      return code;
    }
    pInfo->pExInterval = pExInfo;
    return TSDB_CODE_SUCCESS;
  }

  SExIntervalEntry* pNewEntry = taosMemoryCalloc(1, sizeof(SExIntervalEntry));
  if (NULL == pNewEntry) {
    taosMemoryFree(pExInfo);
    return terrno;
  }

  SResultRowInfo dummy = {0};
  dummy.cur.pageId = -1;
  STimeWindow first = getActiveTimeWindow(NULL, &dummy, pInfo->interval.timeRange.skey, &pInfo->interval, TSDB_ORDER_ASC);
  STimeWindow last = getActiveTimeWindow(NULL, &dummy, pInfo->interval.timeRange.ekey, &pInfo->interval, TSDB_ORDER_ASC);
  if (pInfo->interval.interval <= 0 || last.skey < first.skey) {
    destroyExIntervalEntry(pNewEntry);
    taosMemoryFree(pExInfo);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  uint64_t count = ((uint64_t)last.skey - (uint64_t)first.skey) / (uint64_t)pInfo->interval.interval + 1;
  if (count > EX_INTERVAL_MAX_WINDOWS) {
    destroyExIntervalEntry(pNewEntry);
    taosMemoryFree(pExInfo);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  bool entryInserted = false;
  code = exIntervalInitEntry(pNewEntry, count, first.skey, pInfo->interval.interval, &pOperator->exprSupp);
  if (TSDB_CODE_SUCCESS != code) { destroyExIntervalEntry(pNewEntry); taosMemoryFree(pExInfo); return code; }
  code = taosHashPut(gExIntervalHash, &pExInfo->queryId, sizeof(pExInfo->queryId), &pNewEntry, sizeof(pNewEntry));
  if (TSDB_CODE_SUCCESS == code) {
    entryInserted = true;
  } else if (HASH_NODE_EXIST(code)) {
    destroyExIntervalEntry(pNewEntry);
  } else {
    destroyExIntervalEntry(pNewEntry);
    taosMemoryFree(pExInfo);
    return code;
  }

  code = acquireExIntervalEntry(pExInfo);
  if (TSDB_CODE_SUCCESS != code) {
    if (entryInserted) {
      int32_t removeCode = taosHashRemove(gExIntervalHash, &pExInfo->queryId, sizeof(pExInfo->queryId));
      if (TSDB_CODE_SUCCESS != removeCode && TSDB_CODE_NOT_FOUND != removeCode) {
        qError("EX_INTERVAL failed to discard entry after acquire failure, qId:%" PRIu64 ", code:%s", pExInfo->queryId,
               tstrerror(removeCode));
      }
    }
    taosMemoryFree(pExInfo);
    return code;
  }
  if (0 == atomic_load_32(&pExInfo->pEntry->ready)) {
    code = exIntervalInitEntry(pExInfo->pEntry, count, first.skey, pInfo->interval.interval, &pOperator->exprSupp);
    if (TSDB_CODE_SUCCESS != code) {
      setExIntervalError(pExInfo->pEntry, code);
      releaseExIntervalInfo((void**)&pExInfo);
      return code;
    }
  }
  code = exIntervalInitLocalSums(pExInfo);
  if (TSDB_CODE_SUCCESS != code) {
    releaseExIntervalInfo((void**)&pExInfo);
    return code;
  }
  pExInfo->minActiveWindow = pExInfo->pEntry->numOfWindows;
  pExInfo->maxActiveWindow = -1;
  pInfo->pExInterval = pExInfo;
  return TSDB_CODE_SUCCESS;
}

static int32_t exIntervalGetNumericValue(const SColumnInfoData* pCol, int32_t row, double* pValue) {
  if (NULL == pCol || NULL == pValue) return TSDB_CODE_INVALID_PARA;
  const void* pData = colDataGetData(pCol, row);
  if (NULL == pData) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  if (TSDB_DATA_TYPE_FLOAT == pCol->info.type) {
    *pValue = *(const float*)pData;
  } else if (TSDB_DATA_TYPE_DOUBLE == pCol->info.type) {
    *pValue = *(const double*)pData;
  } else {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t processExIntervalBlock(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SIntervalAggOperatorInfo* pInfo = pOperator->info;
  SExIntervalInfo* pExInfo = pInfo->pExInterval;
  SExIntervalEntry* pEntry = NULL == pExInfo ? NULL : pExInfo->pEntry;
  SExprSupp* pSup = &pOperator->exprSupp;
  if (NULL == pExInfo || NULL == pEntry || NULL == pBlock ||
      NULL == pExInfo->pLocalWindowIndexes) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  int32_t code = atomic_load_32(&pEntry->errCode);
  if (TSDB_CODE_SUCCESS != code) return code;
  if (NULL != pInfo->scalarSupp.pExprInfo) {
    code = projectApplyFunctions(pInfo->scalarSupp.pExprInfo, pBlock, pBlock, pInfo->scalarSupp.pCtx,
                                 pInfo->scalarSupp.numOfExprs, NULL, GET_STM_RTINFO(pOperator->pTaskInfo),
                                 pOperator->pTaskInfo);
    if (TSDB_CODE_SUCCESS != code) return code;
  }
  code = setInputDataBlock(pSup, pBlock, pInfo->binfo.inputTsOrder, pBlock->info.scanFlag, true);
  if (TSDB_CODE_SUCCESS != code) return code;
  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
  if (NULL == pTsCol || TSDB_DATA_TYPE_TIMESTAMP != pTsCol->info.type) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  for (int32_t row = 0; row < pBlock->info.rows; ++row) {
    int64_t ts = *(int64_t*)colDataGetData(pTsCol, row);
    if (ts < pEntry->firstWindowStart) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    uint64_t windowIndex = ((uint64_t)ts - (uint64_t)pEntry->firstWindowStart) / (uint64_t)pEntry->interval;
    if (windowIndex >= (uint64_t)pEntry->numOfWindows) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    int32_t localIndex = -1;
    code = exIntervalGetLocalWindow(pExInfo, (int32_t)windowIndex, &localIndex);
    if (TSDB_CODE_SUCCESS != code) return code;
    pExInfo->pLocalRows[localIndex] += 1;
    for (int32_t expr = 0; expr < pSup->numOfExprs; ++expr) {
      int8_t type = pEntry->cellTypes[expr];
      if (EX_INTERVAL_CELL_WSTART == type || EX_INTERVAL_CELL_WEND == type || EX_INTERVAL_CELL_WDURATION == type) continue;
      SqlFunctionCtx* pCtx = &pSup->pCtx[expr];
      SColumnInfoData* pCol = pCtx->input.numOfInputCols > 0 ? pCtx->input.pData[0] : NULL;
      bool isNull = NULL == pCol || colDataIsNull(pCol, pBlock->info.rows, row, NULL);
      if (EX_INTERVAL_CELL_COUNT == type) {
        if (!isNull) {
          int8_t countIndex = pExInfo->localCountIndex[expr];
          if (countIndex < 0) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
          pExInfo->pLocalCounts[(size_t)localIndex * pExInfo->numOfLocalCounts + countIndex] += 1;
        }
        continue;
      }
      if (isNull) continue;
      double value = 0;
      code = exIntervalGetNumericValue(pCol, row, &value);
      if (TSDB_CODE_SUCCESS != code) return code;
      int8_t sumIndex = pExInfo->localSumIndex[expr];
      int8_t countIndex = pExInfo->localCountIndex[expr];
      if (sumIndex < 0 || countIndex < 0) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      pExInfo->pLocalSums[(size_t)localIndex * pExInfo->numOfLocalSums + sumIndex] += value;
      pExInfo->pLocalCounts[(size_t)localIndex * pExInfo->numOfLocalCounts + countIndex] += 1;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t processExIntervalInput(SOperatorInfo* pOperator) {
  SIntervalAggOperatorInfo* pInfo = pOperator->info;
  SExIntervalInfo* pExInfo = pInfo->pExInterval;
  if (NULL == pExInfo || NULL == pExInfo->pEntry) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  while (!isTaskKilled(pOperator->pTaskInfo)) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (NULL == pBlock) break;
    int32_t code = processExIntervalBlock(pOperator, pBlock);
    if (TSDB_CODE_SUCCESS != code) {
      setExIntervalError(pExInfo->pEntry, code);
      return code;
    }
  }
  if (isTaskKilled(pOperator->pTaskInfo)) {
    int32_t code = pOperator->pTaskInfo->code;
    setExIntervalError(pExInfo->pEntry, code);
    return code;
  }
  exIntervalPublishLocalRange(pExInfo);
  int32_t code = exIntervalMergeLocalSums(pExInfo);
  if (TSDB_CODE_SUCCESS != code) setExIntervalError(pExInfo->pEntry, code);
  else {
    taosMemoryFreeClear(pExInfo->pLocalSums);
    taosMemoryFreeClear(pExInfo->pLocalRows);
    taosMemoryFreeClear(pExInfo->pLocalCounts);
    taosMemoryFreeClear(pExInfo->pLocalWindowIndexes);
    taosMemoryFreeClear(pExInfo->pLocalWindows);
  }
  return code;
}

// Tracks monotonic table/source progress and exposes the global minimum at the heap root.
typedef struct SFastWatermarkHeap {
  int64_t*  pValues;
  uint8_t*  pSeen;
  int32_t*  pHeap;
  int32_t*  pHeapPos;
  int32_t   numOfSlots;
  int32_t   numOfSeen;
} SFastWatermarkHeap;

typedef struct SFastIntervalInfo {
  char*              pRows;
  int32_t*           pRowIndexes;
  uint64_t*          pActiveBitmap;
  uint64_t*          pGroupIds;
  SFastWatermarkHeap tableWatermarks;
  int64_t            firstWindowStart;
  int64_t            interval;
  int64_t            watermark;
  int32_t            numOfWindows;
  int32_t            activeBitmapWords;
  int32_t            activeCount;
  int32_t            groupCapacity;
  int32_t            numOfGroups;
  int32_t            currentGroupSlot;
  uint64_t           currentGroupId;
  int32_t            rowCapacity;
  int32_t            minActiveWindow;
  int32_t            maxActiveWindow;
  bool               denseMode;
  int32_t            nextResult;
  int32_t            sealedWindowExclusive;
  bool               grouped;
  bool               inputCompleted;
} SFastIntervalInfo;

typedef struct SFastMergeIntervalInfo {
  SSDataBlock*       pCachedRows;
  int32_t*           pWindowRowCounts;
  uint64_t*          pActiveBitmap;
  SFastWatermarkHeap sourceProgress;
  SResultRow*        pResultRow;
  SResultRow*        pResultRows;
  int32_t*           pActiveWindows;
  int32_t            resultRowCapacity;
  int64_t            firstWindowStart;
  int64_t            interval;
  int32_t            numOfWindows;
  int32_t            numOfSources;
  int32_t            activeBitmapWords;
  int32_t            activeCount;
  int32_t            minActiveWindow;
  int32_t            maxActiveWindow;
  bool               denseMode;
  int32_t            nextWindow;
  int32_t            sealedWindowExclusive;
  bool               inputCompleted;
  bool               resultRowActive;
  bool               batchProcess;
} SFastMergeIntervalInfo;

static void cleanupFastWatermarkHeap(SFastWatermarkHeap* pHeap) {
  if (pHeap == NULL) {
    return;
  }
  taosMemoryFreeClear(pHeap->pValues);
  taosMemoryFreeClear(pHeap->pSeen);
  taosMemoryFreeClear(pHeap->pHeap);
  taosMemoryFreeClear(pHeap->pHeapPos);
  pHeap->numOfSlots = 0;
  pHeap->numOfSeen = 0;
}

static int32_t initFastWatermarkHeap(SFastWatermarkHeap* pHeap, int32_t numOfSlots) {
  if (pHeap == NULL || numOfSlots <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  pHeap->pValues = taosMemoryCalloc((size_t)numOfSlots, sizeof(int64_t));
  pHeap->pSeen = taosMemoryCalloc((size_t)numOfSlots, sizeof(uint8_t));
  pHeap->pHeap = taosMemoryCalloc((size_t)numOfSlots, sizeof(int32_t));
  pHeap->pHeapPos = taosMemoryMalloc((size_t)numOfSlots * sizeof(int32_t));
  if (pHeap->pValues == NULL || pHeap->pSeen == NULL || pHeap->pHeap == NULL || pHeap->pHeapPos == NULL) {
    cleanupFastWatermarkHeap(pHeap);
    return terrno;
  }

  for (int32_t i = 0; i < numOfSlots; ++i) {
    pHeap->pHeapPos[i] = -1;
  }
  pHeap->numOfSlots = numOfSlots;
  return TSDB_CODE_SUCCESS;
}

static bool fastWatermarkHeapLess(const SFastWatermarkHeap* pHeap, int32_t left, int32_t right) {
  int64_t leftValue = pHeap->pValues[left];
  int64_t rightValue = pHeap->pValues[right];
  return leftValue < rightValue || (leftValue == rightValue && left < right);
}

static void fastWatermarkHeapSwap(SFastWatermarkHeap* pHeap, int32_t left, int32_t right) {
  int32_t leftSlot = pHeap->pHeap[left];
  pHeap->pHeap[left] = pHeap->pHeap[right];
  pHeap->pHeap[right] = leftSlot;
  pHeap->pHeapPos[pHeap->pHeap[left]] = left;
  pHeap->pHeapPos[pHeap->pHeap[right]] = right;
}

static void fastWatermarkHeapSiftUp(SFastWatermarkHeap* pHeap, int32_t pos) {
  while (pos > 0) {
    int32_t parent = (pos - 1) / 2;
    if (!fastWatermarkHeapLess(pHeap, pHeap->pHeap[pos], pHeap->pHeap[parent])) {
      break;
    }
    fastWatermarkHeapSwap(pHeap, pos, parent);
    pos = parent;
  }
}

static void fastWatermarkHeapSiftDown(SFastWatermarkHeap* pHeap, int32_t pos) {
  while (true) {
    int32_t left = pos * 2 + 1;
    if (left >= pHeap->numOfSeen) {
      return;
    }
    int32_t right = left + 1;
    int32_t smallest = left;
    if (right < pHeap->numOfSeen && fastWatermarkHeapLess(pHeap, pHeap->pHeap[right], pHeap->pHeap[left])) {
      smallest = right;
    }
    if (!fastWatermarkHeapLess(pHeap, pHeap->pHeap[smallest], pHeap->pHeap[pos])) {
      return;
    }
    fastWatermarkHeapSwap(pHeap, pos, smallest);
    pos = smallest;
  }
}

static int32_t updateFastWatermarkHeap(SFastWatermarkHeap* pHeap, int32_t slot, int64_t value) {
  if (pHeap == NULL || slot < 0 || slot >= pHeap->numOfSlots) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  if (!pHeap->pSeen[slot]) {
    pHeap->pSeen[slot] = 1;
    pHeap->pValues[slot] = value;
    pHeap->pHeap[pHeap->numOfSeen] = slot;
    pHeap->pHeapPos[slot] = pHeap->numOfSeen;
    pHeap->numOfSeen += 1;
    fastWatermarkHeapSiftUp(pHeap, pHeap->pHeapPos[slot]);
    return TSDB_CODE_SUCCESS;
  }

  if (value > pHeap->pValues[slot]) {
    pHeap->pValues[slot] = value;
    fastWatermarkHeapSiftDown(pHeap, pHeap->pHeapPos[slot]);
  }
  return TSDB_CODE_SUCCESS;
}

static bool getFastWatermarkHeapMin(const SFastWatermarkHeap* pHeap, int64_t* pWatermark) {
  if (pHeap == NULL || pWatermark == NULL || pHeap->numOfSeen != pHeap->numOfSlots || pHeap->numOfSeen == 0) {
    return false;
  }
  *pWatermark = pHeap->pValues[pHeap->pHeap[0]];
  return true;
}

static void destroyFastIntervalInfo(SFastIntervalInfo* pFast) {
  if (pFast == NULL) {
    return;
  }
  taosMemoryFreeClear(pFast->pRows);
  taosMemoryFreeClear(pFast->pRowIndexes);
  taosMemoryFreeClear(pFast->pActiveBitmap);
  taosMemoryFreeClear(pFast->pGroupIds);
  cleanupFastWatermarkHeap(&pFast->tableWatermarks);
  taosMemoryFree(pFast);
}

static int32_t ensureFastIntervalRowCapacity(SFastIntervalInfo* pFast, int32_t required, int32_t rowSize) {
  if (NULL == pFast || required <= 0 || rowSize <= 0) return TSDB_CODE_INVALID_PARA;
  if (required <= pFast->rowCapacity) return TSDB_CODE_SUCCESS;

  int32_t maxCapacity = pFast->groupCapacity * pFast->numOfWindows;
  int32_t capacity = pFast->rowCapacity > 0 ? pFast->rowCapacity : FAST_INTERVAL_LOCAL_RESULT_BATCH_ROWS;
  while (capacity < required) {
    if (capacity > maxCapacity / 2) {
      capacity = maxCapacity;
      break;
    }
    capacity *= 2;
  }
  if (capacity < required || (size_t)capacity > SIZE_MAX / rowSize) return TSDB_CODE_OUT_OF_MEMORY;
  char* pRows = taosMemoryRealloc(pFast->pRows, (size_t)capacity * rowSize);
  if (NULL == pRows) return terrno;
  pFast->pRows = pRows;
  pFast->rowCapacity = capacity;
  return TSDB_CODE_SUCCESS;
}

static int32_t fastIntervalGetGroupSlot(SFastIntervalInfo* pFast, uint64_t groupId, int32_t* pGroupSlot) {
  if (pFast == NULL || pGroupSlot == NULL) return TSDB_CODE_INVALID_PARA;
  if (pFast->currentGroupSlot >= 0 && pFast->currentGroupId == groupId) {
    *pGroupSlot = pFast->currentGroupSlot;
    return TSDB_CODE_SUCCESS;
  }
  for (int32_t i = 0; i < pFast->numOfGroups; ++i) {
    if (pFast->pGroupIds[i] == groupId) {
      pFast->currentGroupId = groupId;
      pFast->currentGroupSlot = i;
      *pGroupSlot = i;
      return TSDB_CODE_SUCCESS;
    }
  }
  if (pFast->numOfGroups >= pFast->groupCapacity) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  int32_t slot = pFast->numOfGroups++;
  pFast->pGroupIds[slot] = groupId;
  pFast->currentGroupId = groupId;
  pFast->currentGroupSlot = slot;
  *pGroupSlot = slot;
  return TSDB_CODE_SUCCESS;
}

static int32_t fastIntervalTotalSlots(const SFastIntervalInfo* pFast) {
  return pFast == NULL ? 0 : pFast->numOfGroups * pFast->numOfWindows;
}

static bool fastIntervalGrouped(const SFastIntervalInfo* pFast) {
  return pFast != NULL && pFast->grouped;
}

static int32_t initFastIntervalAggSup(SExprSupp* pSup, SAggSupporter* pAggSup, SExprInfo* pExprInfo,
                                      int32_t numOfExprs, SFunctionStateStore* pStore) {
  int32_t code = initExprSupp(pSup, pExprInfo, numOfExprs, pStore);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pAggSup->currentPageId = -1;
  pAggSup->resultRowSize = getResultRowSize(pSup->pCtx, numOfExprs);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    pSup->pCtx[i].hasWindowOrGroup = true;
    pSup->pCtx[i].hasWindow = true;
    pSup->pCtx[i].saveHandle.pBuf = NULL;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t resetFastIntervalAggSup(SExprSupp* pSup, SAggSupporter* pAggSup, SNodeList* pFuncs,
                                       SFunctionStateStore* pStore) {
  cleanupExprSuppWithoutFilter(pSup);
  cleanupAggSup(pAggSup);

  int32_t    numOfExprs = 0;
  SExprInfo* pExprInfo = NULL;
  int32_t    code = createExprInfo(pFuncs, NULL, &pExprInfo, &numOfExprs);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pSup->hasWindowOrGroup = true;
  pSup->hasWindow = true;
  return initFastIntervalAggSup(pSup, pAggSup, pExprInfo, numOfExprs, pStore);
}

static int32_t getFastIntervalRow(SAggSupporter* pAggSup, const STimeWindow* pWin, uint64_t groupId,
                                  SResultRowInfo* pResultRowInfo, SResultRow** ppRow) {
  SFastIntervalInfo* pFast = pAggSup->pFastInterval;
  if (pFast == NULL || pWin == NULL || ppRow == NULL || pWin->skey < pFast->firstWindowStart) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int64_t delta = pWin->skey - pFast->firstWindowStart;
  if (pFast->interval <= 0 || delta % pFast->interval != 0) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int64_t windowIndex = delta / pFast->interval;
  if (windowIndex < 0 || windowIndex >= pFast->numOfWindows) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t groupSlot = -1;
  int32_t code = fastIntervalGetGroupSlot(pFast, groupId, &groupSlot);
  if (code != TSDB_CODE_SUCCESS) return code;
  int32_t index = groupSlot * pFast->numOfWindows + (int32_t)windowIndex;

  int32_t rowIndex = pFast->pRowIndexes[index] - 1;
  bool newRow = rowIndex < 0;
  if (rowIndex < 0) {
    code = ensureFastIntervalRowCapacity(pFast, pFast->activeCount + 1, pAggSup->resultRowSize);
    if (TSDB_CODE_SUCCESS != code) return code;
    rowIndex = pFast->activeCount;
    pFast->pRowIndexes[index] = rowIndex + 1;
    ++pFast->activeCount;
  }
  SResultRow* pRow = (SResultRow*)(pFast->pRows + (size_t)rowIndex * pAggSup->resultRowSize);
  if (newRow) {
    memset(pRow, 0, pAggSup->resultRowSize);
    pRow->pageId = 0;
    pRow->offset = (int32_t)index;
    TAOS_SET_POBJ_ALIGNED(&pRow->win, pWin);
    if (index < pFast->minActiveWindow) pFast->minActiveWindow = (int32_t)index;
    if (index > pFast->maxActiveWindow) pFast->maxActiveWindow = (int32_t)index;
    if (!pFast->denseMode) {
      pFast->pActiveBitmap[index >> 6] |= 1ULL << (index & 63);
      if ((int64_t)pFast->activeCount * 5 >= (int64_t)pFast->groupCapacity * pFast->numOfWindows) {
        pFast->denseMode = true;
      }
    }
  }
  pResultRowInfo->cur = (SResultRowPosition){.pageId = 0, .offset = (int32_t)index};
  *ppRow = pRow;
  return TSDB_CODE_SUCCESS;
}

static int32_t initFastIntervalInfo(SIntervalAggOperatorInfo* pInfo, SOperatorInfo* pDownstream) {
  if (pInfo == NULL || pDownstream == NULL || pDownstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  STableScanInfo* pScanInfo = pDownstream->info;
  if (pScanInfo == NULL || !pScanInfo->fastInterval || pScanInfo->base.pTableListInfo == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t numOfTables = 0;
  int32_t code = tableListGetSize(pScanInfo->base.pTableListInfo, &numOfTables);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SFastIntervalInfo* pFast = taosMemoryCalloc(1, sizeof(SFastIntervalInfo));
  if (pFast == NULL) {
    return terrno;
  }

  SResultRowInfo dummy = {0};
  dummy.cur.pageId = -1;
  STimeWindow first = getActiveTimeWindow(NULL, &dummy, pInfo->interval.timeRange.skey, &pInfo->interval, TSDB_ORDER_ASC);
  STimeWindow last = getActiveTimeWindow(NULL, &dummy, pInfo->interval.timeRange.ekey, &pInfo->interval, TSDB_ORDER_ASC);
  if (last.skey < first.skey || pInfo->interval.interval <= 0) {
    taosMemoryFree(pFast);
    return TSDB_CODE_INVALID_PARA;
  }

  int64_t delta = last.skey - first.skey;
  if (delta < 0 || delta % pInfo->interval.interval != 0) {
    taosMemoryFree(pFast);
    return TSDB_CODE_INVALID_PARA;
  }

  bool    grouped = pScanInfo->hasGroupByTag;
  int32_t groupCapacity = grouped ? TMAX(numOfTables, 1) : 1;
  int64_t numOfWindows = delta / pInfo->interval.interval + 1;
  int64_t totalSlots = numOfWindows * groupCapacity;
  if (numOfWindows <= 0 || numOfWindows > EX_INTERVAL_MAX_WINDOWS || totalSlots > EX_INTERVAL_MAX_WINDOWS ||
      numOfWindows > INT64_MAX / pInfo->aggSup.resultRowSize) {
    taosMemoryFree(pFast);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pFast->groupCapacity = groupCapacity;
  pFast->pGroupIds = taosMemoryCalloc((size_t)pFast->groupCapacity, sizeof(uint64_t));
  pFast->pRowIndexes = taosMemoryCalloc((size_t)totalSlots, sizeof(int32_t));
  pFast->activeBitmapWords = (int32_t)(((uint64_t)totalSlots + 63) / 64);
  pFast->pActiveBitmap = taosMemoryCalloc((size_t)pFast->activeBitmapWords, sizeof(uint64_t));
  if (numOfTables > 0 && !grouped) {
    code = initFastWatermarkHeap(&pFast->tableWatermarks, numOfTables);
  }
  if (pFast->pGroupIds == NULL || pFast->pRowIndexes == NULL || pFast->pActiveBitmap == NULL ||
      code != TSDB_CODE_SUCCESS) {
    destroyFastIntervalInfo(pFast);
    return code != TSDB_CODE_SUCCESS ? code : terrno;
  }

  pFast->firstWindowStart = first.skey;
  pFast->interval = pInfo->interval.interval;
  pFast->numOfWindows = (int32_t)numOfWindows;
  pFast->grouped = grouped;
  pFast->minActiveWindow = (int32_t)totalSlots;
  pFast->maxActiveWindow = -1;
  pFast->currentGroupSlot = -1;
  pFast->watermark = INT64_MIN;
  if (0 == numOfTables) {
    // An empty vnode contributes no partial rows but must not fail the fast merge.
    pFast->inputCompleted = true;
    pFast->sealedWindowExclusive = pFast->numOfWindows;
  }
  pInfo->aggSup.pFastInterval = pFast;
  return TSDB_CODE_SUCCESS;
}

static void cleanupFastIntervalRows(SIntervalAggOperatorInfo* pInfo) {
  SFastIntervalInfo* pFast = pInfo->aggSup.pFastInterval;
  if (pFast == NULL) {
    return;
  }

  SExprSupp* pSup = &pInfo->pOperator->exprSupp;
  int32_t totalSlots = fastIntervalTotalSlots(pFast);
  bool sparse = !pFast->denseMode && pFast->activeCount > 0 &&
                (int64_t)pFast->activeCount * 5 < (int64_t)(pFast->maxActiveWindow - pFast->minActiveWindow + 1);
  if (sparse) {
    for (int32_t word = pFast->minActiveWindow >> 6; word < pFast->activeBitmapWords; ++word) {
      uint64_t bits = pFast->pActiveBitmap[word];
      if (word == (pFast->minActiveWindow >> 6)) bits &= (~0ULL << (pFast->minActiveWindow & 63));
      while (bits != 0) {
        int32_t i = (word << 6) + BUILDIN_CTZL(bits);
        bits &= bits - 1;
        if (i > pFast->maxActiveWindow || i >= totalSlots) break;
        int32_t rowIndex = pFast->pRowIndexes[i] - 1;
        if (rowIndex < 0) continue;
        SResultRow* pRow = (SResultRow*)(pFast->pRows + (size_t)rowIndex * pInfo->aggSup.resultRowSize);
        cleanupResultRowFunctionState(pSup, pRow);
      }
    }
  } else {
    for (int32_t i = pFast->minActiveWindow; i <= pFast->maxActiveWindow && i < totalSlots; ++i) {
      int32_t rowIndex = pFast->pRowIndexes[i] - 1;
      if (rowIndex < 0) continue;
      SResultRow* pRow = (SResultRow*)(pFast->pRows + (size_t)rowIndex * pInfo->aggSup.resultRowSize);
      cleanupResultRowFunctionState(pSup, pRow);
    }
  }
  destroyFastIntervalInfo(pFast);
  pInfo->aggSup.pFastInterval = NULL;
}

static int32_t fastIntervalNextActiveWindow(const SFastIntervalInfo* pFast, int32_t start) {
  int32_t totalSlots = fastIntervalTotalSlots(pFast);
  if (pFast == NULL || pFast->pActiveBitmap == NULL || start < 0 || start >= totalSlots) {
    return pFast == NULL ? -1 : totalSlots;
  }
  int32_t word = start >> 6;
  uint64_t bits = pFast->pActiveBitmap[word] & (~0ULL << (start & 63));
  while (true) {
    if (bits != 0) {
      int32_t index = (word << 6) + BUILDIN_CTZL(bits);
      return index < totalSlots ? index : totalSlots;
    }
    if (++word >= pFast->activeBitmapWords) return totalSlots;
    bits = pFast->pActiveBitmap[word];
  }
}

static void cleanupFastMergeIntervalResultRow(SFastMergeIntervalInfo* pFast, SExprSupp* pSup) {
  if (pFast == NULL || pFast->pResultRow == NULL || pSup == NULL || !pFast->resultRowActive) {
    return;
  }

  for (int32_t i = 0; i < pSup->numOfExprs; ++i) {
    pSup->pCtx[i].resultInfo = getResultEntryInfo(pFast->pResultRow, i, pSup->rowEntryInfoOffset);
    if (pSup->pCtx[i].fpSet.cleanup != NULL) {
      pSup->pCtx[i].fpSet.cleanup(&pSup->pCtx[i]);
    }
    pSup->pCtx[i].resultInfo = NULL;
  }
  pFast->resultRowActive = false;
}

static void cleanupFastMergeResultRow(SResultRow* pResultRow, SExprSupp* pSup, const int32_t* rowEntryInfoOffset) {
  if (pResultRow == NULL || pSup == NULL || rowEntryInfoOffset == NULL) {
    return;
  }
  for (int32_t i = 0; i < pSup->numOfExprs; ++i) {
    pSup->pCtx[i].resultInfo = getResultEntryInfo(pResultRow, i, rowEntryInfoOffset);
    if (pSup->pCtx[i].fpSet.cleanup != NULL) {
      pSup->pCtx[i].fpSet.cleanup(&pSup->pCtx[i]);
    }
    pSup->pCtx[i].resultInfo = NULL;
  }
}

static void cleanupFastMergeIntervalInfo(SMergeAlignedIntervalAggOperatorInfo* pMiaInfo) {
  if (pMiaInfo == NULL || pMiaInfo->pFastInterval == NULL) {
    return;
  }

  SFastMergeIntervalInfo* pFast = pMiaInfo->pFastInterval;
  if (pMiaInfo->intervalAggOperatorInfo != NULL && pMiaInfo->intervalAggOperatorInfo->pOperator != NULL) {
    cleanupFastMergeIntervalResultRow(pFast, &pMiaInfo->intervalAggOperatorInfo->pOperator->exprSupp);
  }
  blockDataDestroy(pFast->pCachedRows);
  taosMemoryFreeClear(pFast->pWindowRowCounts);
  taosMemoryFreeClear(pFast->pActiveBitmap);
  cleanupFastWatermarkHeap(&pFast->sourceProgress);
  taosMemoryFreeClear(pFast->pResultRow);
  taosMemoryFreeClear(pFast->pResultRows);
  taosMemoryFreeClear(pFast->pActiveWindows);
  taosMemoryFree(pFast);
  pMiaInfo->pFastInterval = NULL;
}

static int64_t* extractTsCol(SSDataBlock* pBlock, const SIntervalAggOperatorInfo* pInfo, SExecTaskInfo* pTaskInfo);

static SResultRowPosition addToOpenWindowList(SResultRowInfo* pResultRowInfo, const SResultRow* pResult,
                                              uint64_t groupId, SExecTaskInfo* pTaskInfo);
static void doCloseWindow(SResultRowInfo* pResultRowInfo, const SIntervalAggOperatorInfo* pInfo, SResultRow* pResult);
static int32_t applyIndefRowsWindowSegment(SOperatorInfo* pOperator, SIndefRowsRuntime* pRuntime,
                                           SSDataBlock* pResultTemplate, int32_t resultRowSize,
                                           uint64_t groupId, const STimeWindow* pWin,
                                           SSDataBlock* pInputBlock, int32_t startRow, int32_t numRows,
                                           int32_t inputTsOrder, bool closeWindow) {
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  SIndefRowsWindowState* pState = NULL;

  code = applyIndefRowsFuncOnWindowState(pOperator, pRuntime, &pState, pResultTemplate, groupId, pWin,
                                                 pInputBlock, startRow, numRows, inputTsOrder, resultRowSize);
  QUERY_CHECK_CODE(code, lino, _return);

  if (!closeWindow) {
    return code;
  }
  
  code = closeIndefRowsWindowState(pOperator, pRuntime, pState);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t setTimeWindowOutputBuf(SResultRowInfo* pResultRowInfo, STimeWindow* win, bool masterscan,
                                      SResultRow** pResult, int64_t tableGroupId, SqlFunctionCtx* pCtx,
                                      int32_t numOfOutput, int32_t* rowEntryInfoOffset, SAggSupporter* pAggSup,
                                      SExecTaskInfo* pTaskInfo) {
  SResultRow* pResultRow = NULL;
  int32_t     code = TSDB_CODE_SUCCESS;
  if (pAggSup->pFastInterval != NULL) {
    code = getFastIntervalRow(pAggSup, win, tableGroupId, pResultRowInfo, &pResultRow);
  } else {
    pResultRow = doSetResultOutBufByKey(pAggSup->pResultBuf, pResultRowInfo, (char*)&win->skey, TSDB_KEYSIZE,
                                         masterscan, tableGroupId, pTaskInfo, true, pAggSup, true);
  }

  if (code != TSDB_CODE_SUCCESS || pResultRow == NULL || pTaskInfo->code != 0) {
    *pResult = NULL;
    qError("failed to set result output buffer, error:%s", tstrerror(code != TSDB_CODE_SUCCESS ? code : pTaskInfo->code));
    return code != TSDB_CODE_SUCCESS ? code : pTaskInfo->code;
  }

  // set time window for current result
  TAOS_SET_POBJ_ALIGNED(&pResultRow->win, win);
  *pResult = pResultRow;
  return setResultRowInitCtx(pResultRow, pCtx, numOfOutput, rowEntryInfoOffset);
}

static FORCE_INLINE int32_t setFastIntervalOutputBuf(SResultRowInfo* pResultRowInfo, STimeWindow* win,
                                                      uint64_t groupId, SResultRow** pResult, SqlFunctionCtx* pCtx,
                                                      int32_t numOfOutput, int32_t* rowEntryInfoOffset,
                                                      SAggSupporter* pAggSup) {
  SResultRow* pResultRow = NULL;
  int32_t     code = getFastIntervalRow(pAggSup, win, groupId, pResultRowInfo, &pResultRow);
  if (code != TSDB_CODE_SUCCESS || pResultRow == NULL) {
    *pResult = NULL;
    return code != TSDB_CODE_SUCCESS ? code : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  TAOS_SET_POBJ_ALIGNED(&pResultRow->win, win);
  *pResult = pResultRow;
  return setResultRowInitCtx(pResultRow, pCtx, numOfOutput, rowEntryInfoOffset);
}

static FORCE_INLINE bool fastIntervalSingleRow(const SFastIntervalInfo* pFast, const SSDataBlock* pBlock,
                                               const TSKEY* tsCols, int32_t startPos, TSKEY ekey, bool ascScan) {
  if (pFast == NULL || pBlock == NULL || tsCols == NULL || startPos < 0 || startPos >= pBlock->info.rows) {
    return false;
  }
  if (startPos + 1 >= pBlock->info.rows) {
    return true;
  }
  return ascScan ? tsCols[startPos + 1] > ekey : tsCols[startPos + 1] < ekey;
}

static FORCE_INLINE bool fastIntervalNextWindow(const SFastIntervalInfo* pFast, const SSDataBlock* pBlock,
                                                const TSKEY* tsCols, int32_t nextPos, const STimeWindow* pWin,
                                                bool ascScan) {
  if (pFast == NULL || pBlock == NULL || tsCols == NULL || pWin == NULL || nextPos < 0 ||
      nextPos >= pBlock->info.rows) {
    return false;
  }
  return ascScan ? tsCols[nextPos] > pWin->ekey : tsCols[nextPos] < pWin->skey;
}

void doKeepTuple(SWindowRowsSup* pRowSup, int64_t ts, int32_t rowIndex, uint64_t groupId) {
  pRowSup->win.ekey = ts;
  pRowSup->prevTs = ts;
  pRowSup->groupId = groupId;
  pRowSup->numOfRows += 1;
  if (hasContinuousNullRows(pRowSup)) {
    // rows having null state col are wrapped by rows of same state
    // these rows can be counted into current window
    pRowSup->numOfRows += pRowSup->numNullRows;
    resetNumNullRows(pRowSup);
  }
}

void doKeepNewWindowStartInfo(SWindowRowsSup* pRowSup, const int64_t* tsList, int32_t rowIndex, uint64_t groupId) {
  pRowSup->startRowIndex = rowIndex;
  pRowSup->numOfRows = 0;
  pRowSup->win.skey = tsList[rowIndex];
  pRowSup->groupId = groupId;
  resetNumNullRows(pRowSup);
}

FORCE_INLINE int32_t getForwardStepsInBlock(int32_t numOfRows, __block_search_fn_t searchFn, TSKEY ekey, int32_t pos,
                                            int32_t order, int64_t* pData) {
  int32_t forwardRows = 0;

  if (order == TSDB_ORDER_ASC) {
    int32_t end = searchFn((char*)&pData[pos], numOfRows - pos, ekey, order);
    if (end >= 0) {
      forwardRows = end;

      while (pData[end + pos] == ekey) {
        forwardRows += 1;
        ++pos;
      }
    }
  } else {
    int32_t end = searchFn((char*)&pData[pos], numOfRows - pos, ekey, order);
    if (end >= 0) {
      forwardRows = end;

      while (pData[end + pos] == ekey) {
        forwardRows += 1;
        ++pos;
      }
    }
    //    int32_t end = searchFn((char*)pData, pos + 1, ekey, order);
    //    if (end >= 0) {
    //      forwardRows = pos - end;
    //
    //      if (pData[end] == ekey) {
    //        forwardRows += 1;
    //      }
    //    }
  }

  return forwardRows;
}

int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order) {
  if (num <= 0) {
    return -1;
  }

  TSKEY* keyList = (TSKEY*)pValue;
  int32_t firstPos = 0;
  int32_t lastPos = num;

  // Return the leftmost qualifying position.  A timestamp block may contain
  // duplicate keys after multiple ordered sources are merged; returning an
  // arbitrary equal position would skip rows at a window boundary.
  while (firstPos < lastPos) {
    int32_t midPos = firstPos + ((lastPos - firstPos) >> 1);
    bool beforeKey = order == TSDB_ORDER_DESC ? keyList[midPos] > key : keyList[midPos] < key;
    if (beforeKey) {
      firstPos = midPos + 1;
    } else {
      lastPos = midPos;
    }
  }

  return firstPos < num ? firstPos : -1;
}

int32_t getNumOfRowsInTimeWindow(SDataBlockInfo* pDataBlockInfo, TSKEY* pPrimaryColumn, int32_t startPos, TSKEY ekey,
                                 __block_search_fn_t searchFn, STableQueryInfo* item, int32_t order) {
  int32_t num = -1;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(order);

  if (order == TSDB_ORDER_ASC) {
    if (ekey < pDataBlockInfo->window.ekey && pPrimaryColumn) {
      num = getForwardStepsInBlock(pDataBlockInfo->rows, searchFn, ekey, startPos, order, pPrimaryColumn);
      if (item != NULL) {
        item->lastKey = pPrimaryColumn[startPos + (num - 1)] + step;
      }
    } else {
      num = pDataBlockInfo->rows - startPos;
      if (item != NULL) {
        item->lastKey = pDataBlockInfo->window.ekey + step;
      }
    }
  } else {  // desc
    if (ekey > pDataBlockInfo->window.skey && pPrimaryColumn) {
      num = getForwardStepsInBlock(pDataBlockInfo->rows, searchFn, ekey, startPos, order, pPrimaryColumn);
      if (item != NULL) {
        item->lastKey = pPrimaryColumn[startPos + (num - 1)] + step;
      }
    } else {
      num = pDataBlockInfo->rows - startPos;
      if (item != NULL) {
        item->lastKey = pDataBlockInfo->window.ekey + step;
      }
    }
  }

  return num;
}

void doTimeWindowInterpolation(SArray* pPrevValues, SArray* pDataBlock, TSKEY prevTs, int32_t prevRowIndex, TSKEY curTs,
                               int32_t curRowIndex, TSKEY windowKey, int32_t type, SExprSupp* pSup) {
  SqlFunctionCtx* pCtx = pSup->pCtx;

  int32_t index = 1;
  for (int32_t k = 0; k < pSup->numOfExprs; ++k) {
    if (!fmIsIntervalInterpoFunc(pCtx[k].functionId)) {
      pCtx[k].start.key = INT64_MIN;
      continue;
    }

    SFunctParam*     pParam = &pCtx[k].param[0];
    SColumnInfoData* pColInfo = taosArrayGet(pDataBlock, pParam->pCol->slotId);

    double v1 = 0, v2 = 0, v = 0;
    if (prevRowIndex == -1) {
      SGroupKeys* p = taosArrayGet(pPrevValues, index);
      GET_TYPED_DATA(v1, double, pColInfo->info.type, p->pData, typeGetTypeModFromColInfo(&pColInfo->info));
    } else {
      GET_TYPED_DATA(v1, double, pColInfo->info.type, colDataGetData(pColInfo, prevRowIndex),
                     typeGetTypeModFromColInfo(&pColInfo->info));
    }

    GET_TYPED_DATA(v2, double, pColInfo->info.type, colDataGetData(pColInfo, curRowIndex),
                   typeGetTypeModFromColInfo(&pColInfo->info));

#if 0
    if (functionId == FUNCTION_INTERP) {
      if (type == RESULT_ROW_START_INTERP) {
        pCtx[k].start.key = prevTs;
        pCtx[k].start.val = v1;

        pCtx[k].end.key = curTs;
        pCtx[k].end.val = v2;

        if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_VARBINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR ||
            pColInfo->info.type == TSDB_DATA_TYPE_GEOMETRY) {
          if (prevRowIndex == -1) {
            //            pCtx[k].start.ptr = (char*)pRuntimeEnv->prevRow[index];
          } else {
            pCtx[k].start.ptr = (char*)pColInfo->pData + prevRowIndex * pColInfo->info.bytes;
          }

          pCtx[k].end.ptr = (char*)pColInfo->pData + curRowIndex * pColInfo->info.bytes;
        }
      }
    } else if (functionId == FUNCTION_TWA) {
#endif

    SPoint point1 = (SPoint){.key = prevTs, .val = &v1};
    SPoint point2 = (SPoint){.key = curTs, .val = &v2};
    SPoint point = (SPoint){.key = windowKey, .val = &v};

    if (fmIsElapsedFunc(pCtx[k].functionId)) {
      // elapsed only needs the boundary key, not an interpolated value.
    } else if (fmIsTwLocfFunc(pCtx[k].functionId)) {
      // LOCF (last observation carried forward): hold the previous point's value at the
      // window boundary instead of linearly interpolating between the two neighbors.
      v = v1;
    } else {
      taosGetLinearInterpolationVal(&point, TSDB_DATA_TYPE_DOUBLE, &point1, &point2, TSDB_DATA_TYPE_DOUBLE, 0);
    }

    if (type == RESULT_ROW_START_INTERP) {
      pCtx[k].start.key = point.key;
      pCtx[k].start.val = v;
    } else {
      pCtx[k].end.key = point.key;
      pCtx[k].end.val = v;
    }

    index += 1;
  }
#if 0
  }
#endif
}

static void setNotInterpoWindowKey(SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t type) {
  if (type == RESULT_ROW_START_INTERP) {
    for (int32_t k = 0; k < numOfOutput; ++k) {
      pCtx[k].start.key = INT64_MIN;
    }
  } else {
    for (int32_t k = 0; k < numOfOutput; ++k) {
      pCtx[k].end.key = INT64_MIN;
    }
  }
}

static bool setTimeWindowInterpolationStartTs(SIntervalAggOperatorInfo* pInfo, int32_t pos, SSDataBlock* pBlock,
                                              const TSKEY* tsCols, STimeWindow* win, SExprSupp* pSup) {
  bool ascQuery = (pInfo->binfo.inputTsOrder == TSDB_ORDER_ASC);

  TSKEY curTs = tsCols[pos];

  SGroupKeys* pTsKey = taosArrayGet(pInfo->pPrevValues, 0);
  TSKEY       lastTs = *(int64_t*)pTsKey->pData;

  // lastTs == INT64_MIN and pos == 0 means this is the first time window, interpolation is not needed.
  // start exactly from this point, no need to do interpolation
  TSKEY key = ascQuery ? win->skey : win->ekey;
  if (key == curTs) {
    setNotInterpoWindowKey(pSup->pCtx, pSup->numOfExprs, RESULT_ROW_START_INTERP);
    return true;
  }

  // it is the first time window, no need to do interpolation
  if (pTsKey->isNull && pos == 0) {
    setNotInterpoWindowKey(pSup->pCtx, pSup->numOfExprs, RESULT_ROW_START_INTERP);
  } else {
    TSKEY prevTs = ((pos == 0) ? lastTs : tsCols[pos - 1]);
    doTimeWindowInterpolation(pInfo->pPrevValues, pBlock->pDataBlock, prevTs, pos - 1, curTs, pos, key,
                              RESULT_ROW_START_INTERP, pSup);
  }

  return true;
}

static int32_t setTimeWindowInterpolationEndTs(SIntervalAggOperatorInfo* pInfo, SExprSupp* pSup, int32_t endRowIndex,
                                               int32_t nextRowIndex, SArray* pDataBlock, const TSKEY* tsCols,
                                               TSKEY blockEkey, STimeWindow* win, bool* pRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t order = pInfo->binfo.inputTsOrder;

  TSKEY actualEndKey = tsCols[endRowIndex];
  TSKEY key = (order == TSDB_ORDER_ASC) ? win->ekey : win->skey;

  // not ended in current data block, do not invoke interpolation
  if ((key > blockEkey && (order == TSDB_ORDER_ASC)) || (key < blockEkey && (order == TSDB_ORDER_DESC))) {
    setNotInterpoWindowKey(pSup->pCtx, pSup->numOfExprs, RESULT_ROW_END_INTERP);
    (*pRes) = false;
    return code;
  }

  // there is actual end point of current time window, no interpolation needs
  if (key == actualEndKey) {
    setNotInterpoWindowKey(pSup->pCtx, pSup->numOfExprs, RESULT_ROW_END_INTERP);
    (*pRes) = true;
    return code;
  }

  if (nextRowIndex < 0) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR));
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  TSKEY nextKey = tsCols[nextRowIndex];
  doTimeWindowInterpolation(pInfo->pPrevValues, pDataBlock, actualEndKey, endRowIndex, nextKey, nextRowIndex, key,
                            RESULT_ROW_END_INTERP, pSup);
  (*pRes) = true;
  return code;
}

bool inCalSlidingWindow(const SInterval* pInterval, const STimeWindow* pWin, TSKEY calStart, TSKEY calEnd) {
  if (pInterval->interval != pInterval->sliding && (pWin->ekey < calStart || pWin->skey > calEnd)) {
    return false;
  }

  return true;
}

bool inSlidingWindow(const SInterval* pInterval, const STimeWindow* pWin, const SDataBlockInfo* pBlockInfo) {
  return inCalSlidingWindow(pInterval, pWin, pBlockInfo->calWin.skey, pBlockInfo->calWin.ekey);
}

int32_t getNextQualifiedWindow(SInterval* pInterval, STimeWindow* pNext, SDataBlockInfo* pDataBlockInfo,
                               TSKEY* primaryKeys, int32_t prevPosition, int32_t order) {
  bool ascQuery = (order != TSDB_ORDER_DESC);

  int32_t precision = pInterval->precision;
  getNextTimeWindow(pInterval, pNext, order);

  // next time window is not in current block
  if ((pNext->skey > pDataBlockInfo->window.ekey && order == TSDB_ORDER_ASC) ||
      (pNext->ekey < pDataBlockInfo->window.skey && order == TSDB_ORDER_DESC)) {
    return -1;
  }

  if (!inSlidingWindow(pInterval, pNext, pDataBlockInfo) && order == TSDB_ORDER_ASC) {
    return -1;
  }

  TSKEY   skey = ascQuery ? pNext->skey : pNext->ekey;
  int32_t startPos = 0;

  // tumbling time window query, a special case of sliding time window query
  if (pInterval->sliding == pInterval->interval && prevPosition != -1) {
    startPos = prevPosition + 1;
  } else {
    if ((skey <= pDataBlockInfo->window.skey && ascQuery) || (skey >= pDataBlockInfo->window.ekey && !ascQuery)) {
      startPos = 0;
    } else {
      startPos = binarySearchForKey((char*)primaryKeys, pDataBlockInfo->rows, skey, order);
    }
  }
  if(startPos < 0 || startPos >= pDataBlockInfo->rows) {
    return -1;
  }

  /* interp query with fill should not skip time window */
  //  if (pQueryAttr->pointInterpQuery && pQueryAttr->fillType != TSDB_FILL_NONE) {
  //    return startPos;
  //  }

  /*
   * This time window does not cover any data, try next time window,
   * this case may happen when the time window is too small
   */
  if (primaryKeys != NULL) {
    if (ascQuery && primaryKeys[startPos] > pNext->ekey) {
      TSKEY next = primaryKeys[startPos];
      if (IS_CALENDAR_TIME_DURATION(pInterval->intervalUnit) ||
          IS_CALENDAR_DAY_DURATION(pInterval->intervalUnit)) {
        pNext->skey = taosTimeTruncate(next, pInterval);
        pNext->ekey = taosTimeGetIntervalEnd(pNext->skey, pInterval);
      } else {
        pNext->ekey += ((next - pNext->ekey + pInterval->sliding - 1) / pInterval->sliding) * pInterval->sliding;
        pNext->skey = pNext->ekey - pInterval->interval + 1;
      }
    } else if ((!ascQuery) && primaryKeys[startPos] < pNext->skey) {
      TSKEY next = primaryKeys[startPos];
      if (IS_CALENDAR_TIME_DURATION(pInterval->intervalUnit) ||
          IS_CALENDAR_DAY_DURATION(pInterval->intervalUnit)) {
        pNext->skey = taosTimeTruncate(next, pInterval);
        pNext->ekey = taosTimeGetIntervalEnd(pNext->skey, pInterval);
      } else {
        pNext->skey -= ((pNext->skey - next + pInterval->sliding - 1) / pInterval->sliding) * pInterval->sliding;
        pNext->ekey = pNext->skey + pInterval->interval - 1;
      }
    }
  }

  return startPos;
}

static bool isResultRowInterpolated(SResultRow* pResult, SResultTsInterpType type) {
  if (type == RESULT_ROW_START_INTERP) {
    return pResult->startInterp == true;
  } else {
    return pResult->endInterp == true;
  }
}

static void setResultRowInterpo(SResultRow* pResult, SResultTsInterpType type) {
  if (type == RESULT_ROW_START_INTERP) {
    pResult->startInterp = true;
  } else {
    pResult->endInterp = true;
  }
}

static int32_t doWindowBorderInterpolation(SIntervalAggOperatorInfo* pInfo, SSDataBlock* pBlock, SResultRow* pResult,
                                           STimeWindow* win, int32_t startPos, int32_t forwardRows, SExprSupp* pSup) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (!pInfo->timeWindowInterpo) {
    return code;
  }

  if (pBlock == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    return code;
  }

  if (pBlock->pDataBlock == NULL) {
    return code;
  }

  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);

  TSKEY* tsCols = (TSKEY*)(pColInfo->pData);
  bool   done = isResultRowInterpolated(pResult, RESULT_ROW_START_INTERP);
  if (!done) {  // it is not interpolated, now start to generated the interpolated value
    bool interp = setTimeWindowInterpolationStartTs(pInfo, startPos, pBlock, tsCols, win, pSup);
    if (interp) {
      setResultRowInterpo(pResult, RESULT_ROW_START_INTERP);
    }
  } else {
    setNotInterpoWindowKey(pSup->pCtx, pSup->numOfExprs, RESULT_ROW_START_INTERP);
  }

  // point interpolation does not require the end key time window interpolation.
  // interpolation query does not generate the time window end interpolation
  done = isResultRowInterpolated(pResult, RESULT_ROW_END_INTERP);
  if (!done) {
    int32_t endRowIndex = startPos + forwardRows - 1;
    int32_t nextRowIndex = endRowIndex + 1;

    // duplicated ts row does not involve in the interpolation of end value for current time window
    int32_t x = endRowIndex;
    while (x > 0) {
      if (tsCols[x] == tsCols[x - 1]) {
        x -= 1;
      } else {
        endRowIndex = x;
        break;
      }
    }

    TSKEY endKey = (pInfo->binfo.inputTsOrder == TSDB_ORDER_ASC) ? pBlock->info.window.ekey : pBlock->info.window.skey;
    bool  interp = false;
    code = setTimeWindowInterpolationEndTs(pInfo, pSup, endRowIndex, nextRowIndex, pBlock->pDataBlock, tsCols, endKey,
                                           win, &interp);
    QUERY_CHECK_CODE(code, lino, _end);
    if (interp) {
      setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
    }
  } else {
    setNotInterpoWindowKey(pSup->pCtx, pSup->numOfExprs, RESULT_ROW_END_INTERP);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t saveDataBlockLastRow(SArray* pPrevKeys, const SSDataBlock* pBlock, SArray* pCols) {
  if (pBlock->pDataBlock == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  size_t num = taosArrayGetSize(pPrevKeys);
  for (int32_t k = 0; k < num; ++k) {
    SColumn* pc = taosArrayGet(pCols, k);

    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, pc->slotId);

    SGroupKeys* pkey = taosArrayGet(pPrevKeys, k);
    for (int32_t i = pBlock->info.rows - 1; i >= 0; --i) {
      if (colDataIsNull_s(pColInfo, i)) {
        continue;
      }

      char* val = colDataGetData(pColInfo, i);
      if (IS_VAR_DATA_TYPE(pkey->type)) {
        // The cell buffer is initially sized by the interpolation parameter's declared
        // width; a virtual table column may carry a value wider than declared (the full
        // source value is returned by design), so grow the cell instead of overrunning it.
        // calcStrBytesByType dispatches the json/blob/varstr headers (varDataTLen
        // would misparse the json envelope as a varstr prefix).
        int32_t bytes = calcStrBytesByType(pkey->type, val);
        // A negative length (truncated blob header) would bypass the bound and become
        // a huge size_t in memcpy -- refuse it.
        if (bytes < 0) {
          qError("invalid varlen interp cell length %d", bytes);
          return TSDB_CODE_QRY_NOT_ENOUGH_BUFFER;
        }
        if (bytes > pkey->bytes) {
          char* tmp = taosMemoryRealloc(pkey->pData, bytes);
          if (NULL == tmp) {
            qError("failed to grow interp cell to %d bytes", bytes);
            return terrno;
          }
          pkey->pData = tmp;
          pkey->bytes = bytes;
        }
        memcpy(pkey->pData, val, bytes);
      } else {
        memcpy(pkey->pData, val, pkey->bytes);
      }

      break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void doInterpUnclosedTimeWindow(SOperatorInfo* pOperatorInfo, int32_t numOfExprs, SResultRowInfo* pResultRowInfo,
                                       SSDataBlock* pBlock, int32_t scanFlag, int64_t* tsCols, SResultRowPosition* p) {
  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;

  SIntervalAggOperatorInfo* pInfo = (SIntervalAggOperatorInfo*)pOperatorInfo->info;
  SExprSupp*                pSup = &pOperatorInfo->exprSupp;

  int32_t startPos = 0;
  int32_t numOfOutput = pSup->numOfExprs;

  SResultRow* pResult = NULL;

  while (1) {
    SListNode*          pn = tdListGetHead(pResultRowInfo->openWindow);
    SOpenWindowInfo*    pOpenWin = (SOpenWindowInfo*)pn->data;
    uint64_t            groupId = pOpenWin->groupId;
    SResultRowPosition* p1 = &pOpenWin->pos;
    if (p->pageId == p1->pageId && p->offset == p1->offset) {
      break;
    }

    SResultRow* pr = getResultRowByPos(pInfo->aggSup.pResultBuf, p1, false);
    if (NULL == pr) {
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    if (!(pr->offset == p1->offset && pr->pageId == p1->pageId)) {
      pTaskInfo->code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    if (pr->closed) {
      if (!(isResultRowInterpolated(pr, RESULT_ROW_START_INTERP) &&
            isResultRowInterpolated(pr, RESULT_ROW_END_INTERP))) {
        pTaskInfo->code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        T_LONG_JMP(pTaskInfo->env, terrno);
      }
      SListNode* pNode = tdListPopHead(pResultRowInfo->openWindow);
      taosMemoryFree(pNode);
      continue;
    }

    STimeWindow w = pr->win;
    if (groupId == pBlock->info.id.groupId) {
      TSKEY curTs = pBlock->info.window.skey;
      if (tsCols != NULL) {
        curTs = tsCols[startPos];
      }

      if ((pInfo->binfo.inputTsOrder == TSDB_ORDER_ASC && w.ekey >= curTs) ||
          (pInfo->binfo.inputTsOrder == TSDB_ORDER_DESC && w.skey <= curTs)) {
        break;
      }
    }

    int32_t     ret = setTimeWindowOutputBuf(pResultRowInfo, &w, (scanFlag == MAIN_SCAN), &pResult, groupId, pSup->pCtx,
                                             numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }

    if (isResultRowInterpolated(pResult, RESULT_ROW_END_INTERP)) {
      pTaskInfo->code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    SGroupKeys* pTsKey = taosArrayGet(pInfo->pPrevValues, 0);
    if (!pTsKey) {
      pTaskInfo->code = terrno;
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    int64_t prevTs = *(int64_t*)pTsKey->pData;
    if (groupId == pBlock->info.id.groupId) {
      TSKEY curTs = pBlock->info.window.skey;
      if (tsCols != NULL) {
        curTs = tsCols[startPos];
      }
      doTimeWindowInterpolation(pInfo->pPrevValues, pBlock->pDataBlock, prevTs, -1, curTs, startPos, w.ekey,
                                RESULT_ROW_END_INTERP, pSup);
    }

    setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
    setNotInterpoWindowKey(pSup->pCtx, numOfExprs, RESULT_ROW_START_INTERP);

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &w, 1);
    ret = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, 0,
                                          pBlock->info.rows, numOfExprs);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }

    if (isResultRowInterpolated(pResult, RESULT_ROW_END_INTERP)) {
      closeResultRow(pr);
      SListNode* pNode = tdListPopHead(pResultRowInfo->openWindow);
      taosMemoryFree(pNode);
    } else {  // the remains are can not be closed yet.
      break;
    }
  }
}

static bool tsKeyCompFn(void* l, void* r, void* param) {
  TSKEY*                    lTS = (TSKEY*)l;
  TSKEY*                    rTS = (TSKEY*)r;
  SIntervalAggOperatorInfo* pInfo = param;
  return pInfo->binfo.outputTsOrder == TSDB_ORDER_ASC ? *lTS < *rTS : *lTS > *rTS;
}

static bool isCalculatedWin(SIntervalAggOperatorInfo* pInfo, const STimeWindow* win, uint64_t tableGroupId) {
  if (pInfo->aggSup.pFastInterval != NULL) {
    SFastIntervalInfo* pFast = pInfo->aggSup.pFastInterval;
    if (win->skey < pFast->firstWindowStart) {
      return false;
    }
    int64_t delta = win->skey - pFast->firstWindowStart;
    int64_t windowIndex = delta / pFast->interval;
    if (delta % pFast->interval != 0 || windowIndex < 0 || windowIndex >= pFast->numOfWindows) {
      return false;
    }
    int32_t groupSlot = 0;
    if (fastIntervalGrouped(pFast)) {
      groupSlot = -1;
      for (int32_t i = 0; i < pFast->numOfGroups; ++i) {
        if (pFast->pGroupIds[i] == tableGroupId) {
          groupSlot = i;
          break;
        }
      }
      if (groupSlot < 0) return false;
    }
    int32_t index = groupSlot * pFast->numOfWindows + (int32_t)windowIndex;
    return pFast->pRowIndexes[index] > 0;
  }
  char keyBuf[sizeof(TSKEY) + sizeof(uint64_t)] = {0};
  SET_RES_WINDOW_KEY(keyBuf, (char*)&win->skey, sizeof(TSKEY), tableGroupId);
  return tSimpleHashGet(pInfo->aggSup.pResultRowHashTable, keyBuf, GET_RES_WINDOW_KEY_LEN(sizeof(TSKEY))) != NULL;
}

/**
 * @brief check if cur window should be filtered out by limit info
 * @retval true if should be filtered out
 * @retval false if not filtering out
 * @note If no limit info, we skip filtering.
 *       If input/output ts order mismatch, we skip filtering too.
 *       eg. input ts order: desc, and output ts order: asc, limit: 10
 *       IntervalOperator should output the first 10 windows, however, we can't find the first 10 windows until we scan
 *       every tuple in every block.
 *       And the boundedQueue keeps refreshing all records with smaller ts key.
 */
static bool filterWindowWithLimit(SIntervalAggOperatorInfo* pOperatorInfo, STimeWindow* win, uint64_t groupId,
                                  SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (!pOperatorInfo->limited  // if no limit info, no filter will be applied
      || pOperatorInfo->binfo.inputTsOrder != pOperatorInfo->binfo.outputTsOrder
      // if input/output ts order mismatch, no filter
  ) {
    return false;
  }

  if (pOperatorInfo->limit == 0) return true;

  if (pOperatorInfo->pBQ == NULL) {
    pOperatorInfo->pBQ = createBoundedQueue(pOperatorInfo->limit - 1, tsKeyCompFn, taosAutoMemoryFree, pOperatorInfo);
    QUERY_CHECK_NULL(pOperatorInfo->pBQ, code, lino, _end, terrno);
  }

  bool shouldFilter = false;
  // if BQ has been full, compare it with top of BQ
  if (taosBQSize(pOperatorInfo->pBQ) == taosBQMaxSize(pOperatorInfo->pBQ) + 1) {
    PriorityQueueNode* top = taosBQTop(pOperatorInfo->pBQ);
    shouldFilter = tsKeyCompFn(top->data, &win->skey, pOperatorInfo);
  }
  if (shouldFilter) {
    return true;
  } else if (isCalculatedWin(pOperatorInfo, win, groupId)) {
    return false;
  }

  // cur win not been filtered out and not been pushed into BQ yet, push it into BQ
  PriorityQueueNode node = {.data = taosMemoryMalloc(sizeof(TSKEY))};
  QUERY_CHECK_NULL(node.data, code, lino, _end, terrno);

  *((TSKEY*)node.data) = win->skey;

  if (NULL == taosBQPush(pOperatorInfo->pBQ, &node)) {
    taosMemoryFree(node.data);
    return true;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return false;
}

int32_t getNumOfRowsInTimeWinUnsorted(SDataBlockInfo* pDataBlockInfo, TSKEY* pPrimaryColumn, STimeWindow* win,
                                      int32_t startPos) {
  int32_t rows = pDataBlockInfo->rows;
  for (int32_t i = startPos; i < pDataBlockInfo->rows; ++i) {
    if (pPrimaryColumn[i] >= win->skey && pPrimaryColumn[i] <= win->ekey) {
      continue;
    } else {
      return i - startPos;
    }
  }
  return rows - startPos;
}

static bool hashIntervalAgg(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock,
                            int32_t scanFlag) {
  SIntervalAggOperatorInfo* pInfo = (SIntervalAggOperatorInfo*)pOperatorInfo->info;

  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*     pSup = &pOperatorInfo->exprSupp;

  int32_t          startPos = 0;
  int32_t          numOfOutput = pSup->numOfExprs;
  int64_t*         tsCols = extractTsCol(pBlock, pInfo, pTaskInfo);
  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
  uint64_t         tableGroupId = pBlock->info.id.groupId;
  bool             ascScan = (pInfo->binfo.inputTsOrder == TSDB_ORDER_ASC);
  SResultRow*      pResult = NULL;
  bool             sorted = optrHasOrderedInput(&pInfo->binfo, DATA_ORDER_LEVEL_IN_BLOCK) || tsCols == NULL;
  qTrace("%s %s ordered-check, groupId:%" PRIu64 ", sorted:%d, tsColsNull:%d, inputTsOrder:%d, inputDataOrder:%d, "
         "minDataOrder:%d, rows:%" PRId64 ", scanFlag:%d",
         GET_TASKID(pTaskInfo), __func__, tableGroupId, sorted, tsCols == NULL, pInfo->binfo.inputTsOrder,
         pInfo->binfo.inputDataOrder, DATA_ORDER_LEVEL_IN_BLOCK, pBlock->info.rows, scanFlag);
  while (pTsCol != NULL && startPos < pBlock->info.rows && pTsCol->hasNull && colDataIsNull_f(pTsCol, startPos)) {
    ++startPos;
  }
  if (startPos >= pBlock->info.rows) {
    return false;
  }

  TSKEY       ts = (tsCols == NULL) ? pBlock->info.window.skey : tsCols[startPos];
  int32_t     ret = TSDB_CODE_SUCCESS;

  if (tableGroupId != pInfo->curGroupId) {
    if (pInfo->indefRowsMode) {
      ret = closeAllIndefRowsWindowStates(pOperatorInfo, &pInfo->indefRows);
      if (ret != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, ret);
      }
    }

    pInfo->handledGroupNum += 1;
    if (pInfo->slimited && pInfo->handledGroupNum > pInfo->slimit) {
      return true;
    } else {
      pInfo->curGroupId = tableGroupId;
      destroyBoundedQueue(pInfo->pBQ);
      pInfo->pBQ = NULL;
    }
  }

  SResultRowInfo fastDummy = {0};
  fastDummy.cur.pageId = -1;
  SFastIntervalInfo* pFast = pInfo->aggSup.pFastInterval;
  int64_t     seekStartUs = pFast == NULL ? taosGetTimestampUs() : 0;
  STimeWindow win = getActiveTimeWindow(pInfo->aggSup.pFastInterval != NULL ? NULL : pInfo->aggSup.pResultBuf,
                                        pInfo->aggSup.pFastInterval != NULL ? &fastDummy : pResultRowInfo, ts,
                                        &pInfo->interval, pInfo->binfo.inputTsOrder);
  if (pFast == NULL) {
    pInfo->timing.hashSeekCalls += 1;
    pInfo->timing.hashSeekElapsedUs += taosGetTimestampUs() - seekStartUs;
  }
  if (filterWindowWithLimit(pInfo, &win, tableGroupId, pTaskInfo)) return false;

  TSKEY   ekey = ascScan ? win.ekey : win.skey;
  if (tsCols == NULL &&
      ((ascScan && ekey < pBlock->info.window.ekey) || (!ascScan && ekey > pBlock->info.window.skey))) {
    qError("%s failed at line %d since timestamp column data is not loaded and block spans multiple windows",
           __func__, __LINE__);
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }

  int32_t forwardRows = sorted && fastIntervalSingleRow(pFast, pBlock, tsCols, startPos, ekey, ascScan)
                            ? 1
                            : (sorted ? getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey,
                                                                 binarySearchForKey, NULL, pInfo->binfo.inputTsOrder)
                                      : getNumOfRowsInTimeWinUnsorted(&pBlock->info, tsCols, &win, startPos));

  if (pInfo->indefRowsMode) {
    bool closeWindow = sorted && (startPos + forwardRows < pBlock->info.rows);
    ret = applyIndefRowsWindowSegment(pOperatorInfo, &pInfo->indefRows, pInfo->binfo.pRes, pInfo->aggSup.resultRowSize,
                                      tableGroupId, &win, pBlock, startPos, forwardRows, pInfo->binfo.inputTsOrder,
                                      closeWindow);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }
  } else {
    int64_t outputStartUs = pFast == NULL ? taosGetTimestampUs() : 0;
    ret = pFast != NULL
              ? setFastIntervalOutputBuf(pResultRowInfo, &win, tableGroupId, &pResult, pSup->pCtx, numOfOutput,
                                          pSup->rowEntryInfoOffset, &pInfo->aggSup)
              : setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId,
                                       pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (pFast == NULL) {
      pInfo->timing.hashOutputCalls += 1;
      pInfo->timing.hashOutputElapsedUs += taosGetTimestampUs() - outputStartUs;
    }
    if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }

    // prev time window not interpolation yet.
    if (pInfo->timeWindowInterpo) {
      SResultRowPosition pos = addToOpenWindowList(pResultRowInfo, pResult, tableGroupId, pTaskInfo);
      doInterpUnclosedTimeWindow(pOperatorInfo, numOfOutput, pResultRowInfo, pBlock, scanFlag, tsCols, &pos);

      // restore current time window
      ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pSup->pCtx,
                                   numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
      if (ret != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, ret);
      }

      // window start key interpolation
      ret = doWindowBorderInterpolation(pInfo, pBlock, pResult, &win, startPos, forwardRows, pSup);
      if (ret != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, ret);
      }
    }
    // qDebug("hashIntervalAgg1 window skey: %lld, ekey:%lld, startPos: %d, forwardRows: %d",
    //   win.skey, win.ekey, startPos, forwardRows);
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &win, 1);
    int64_t applyStartUs = pFast == NULL ? taosGetTimestampUs() : 0;
    ret = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows,
                                          pBlock->info.rows, numOfOutput);
    if (pFast == NULL) {
      pInfo->timing.hashApplyCalls += 1;
      pInfo->timing.hashApplyElapsedUs += taosGetTimestampUs() - applyStartUs;
    }
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }

    doCloseWindow(pResultRowInfo, pInfo, pResult);
  }

  STimeWindow nextWin = win;
  int32_t rows = pBlock->info.rows;

  while (startPos < pBlock->info.rows) {
    seekStartUs = pFast == NULL ? taosGetTimestampUs() : 0;
    if (sorted) {
      int32_t nextPos = startPos + forwardRows;
      if (pFast != NULL && fastIntervalNextWindow(pFast, pBlock, tsCols, nextPos, &nextWin, ascScan)) {
        startPos = nextPos;
        getInitialStartTimeWindow(&pInfo->interval, tsCols[startPos], &nextWin, ascScan);
      } else if (pFast != NULL && nextPos >= pBlock->info.rows) {
        break;
      } else {
        startPos = getNextQualifiedWindow(&pInfo->interval, &nextWin, &pBlock->info, tsCols,
                                          forwardRows - 1 + startPos, pInfo->binfo.inputTsOrder);
        if (startPos < 0) {
          break;
        }
      }
    } else {
      pBlock->info.rows = forwardRows;
      int32_t newStartOff = forwardRows >= 1
                                ? getNextQualifiedWindow(&pInfo->interval, &nextWin, &pBlock->info, tsCols + startPos,
                                                         forwardRows - 1, pInfo->binfo.inputTsOrder)
                                : -1;
      pBlock->info.rows = rows;
      if (newStartOff >= 0) {
        startPos += newStartOff;
      } else if ((startPos += forwardRows) < pBlock->info.rows) {
        getInitialStartTimeWindow(&pInfo->interval, tsCols[startPos], &nextWin, true);
      }
      if (startPos >= pBlock->info.rows) {
        break;
      }
    }
    if (pFast == NULL) {
      pInfo->timing.hashSeekCalls += 1;
      pInfo->timing.hashSeekElapsedUs += taosGetTimestampUs() - seekStartUs;
    }

    if (filterWindowWithLimit(pInfo, &nextWin, tableGroupId, pTaskInfo)) {
      break;
    }

    ekey = ascScan ? nextWin.ekey : nextWin.skey;
    forwardRows = sorted && fastIntervalSingleRow(pFast, pBlock, tsCols, startPos, ekey, ascScan)
                      ? 1
                      : (sorted ? getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey,
                                                           NULL, pInfo->binfo.inputTsOrder)
                                : getNumOfRowsInTimeWinUnsorted(&pBlock->info, tsCols, &nextWin, startPos));
    if (forwardRows == 0) continue;

    if (pInfo->indefRowsMode) {
      bool closeWindow = sorted && (startPos + forwardRows < pBlock->info.rows);
      ret = applyIndefRowsWindowSegment(pOperatorInfo, &pInfo->indefRows, pInfo->binfo.pRes, pInfo->aggSup.resultRowSize,
                                        tableGroupId, &nextWin, pBlock, startPos, forwardRows,
                                        pInfo->binfo.inputTsOrder, closeWindow);
      if (ret != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, ret);
      }
    } else {
      // null data, failed to allocate more memory buffer
      int64_t outputStartUs = pFast == NULL ? taosGetTimestampUs() : 0;
      int32_t code = pFast != NULL
                         ? setFastIntervalOutputBuf(pResultRowInfo, &nextWin, tableGroupId, &pResult, pSup->pCtx,
                                                     numOfOutput,
                                                     pSup->rowEntryInfoOffset, &pInfo->aggSup)
                         : setTimeWindowOutputBuf(pResultRowInfo, &nextWin, (scanFlag == MAIN_SCAN), &pResult,
                                                  tableGroupId, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset,
                                                  &pInfo->aggSup, pTaskInfo);
      if (pFast == NULL) {
        pInfo->timing.hashOutputCalls += 1;
        pInfo->timing.hashOutputElapsedUs += taosGetTimestampUs() - outputStartUs;
      }
      if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
        T_LONG_JMP(pTaskInfo->env, code);
      }

      // window start(end) key interpolation
      code = doWindowBorderInterpolation(pInfo, pBlock, pResult, &nextWin, startPos, forwardRows, pSup);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }
      // TODO: add to open window? how to close the open windows after input blocks exhausted?
#if 0
      if ((ascScan && ekey <= pBlock->info.window.ekey) ||
          (!ascScan && ekey >= pBlock->info.window.skey)) {
        // window start(end) key interpolation
        doWindowBorderInterpolation(pInfo, pBlock, pResult, &nextWin, startPos, forwardRows, pSup);
      } else if (pInfo->timeWindowInterpo) {
        addToOpenWindowList(pResultRowInfo, pResult, tableGroupId);
      }
#endif
      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &nextWin, 1);
      int64_t applyStartUs = pFast == NULL ? taosGetTimestampUs() : 0;
      ret = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos,
                                            forwardRows, pBlock->info.rows, numOfOutput);
      if (pFast == NULL) {
        pInfo->timing.hashApplyCalls += 1;
        pInfo->timing.hashApplyElapsedUs += taosGetTimestampUs() - applyStartUs;
      }
      if (ret != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, ret);
      }
      doCloseWindow(pResultRowInfo, pInfo, pResult);
    }
  }

  if (pInfo->timeWindowInterpo) {
    ret = saveDataBlockLastRow(pInfo->pPrevValues, pBlock, pInfo->pInterpCols);
    if (TSDB_CODE_SUCCESS != ret) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }
  }
  return false;
}

void doCloseWindow(SResultRowInfo* pResultRowInfo, const SIntervalAggOperatorInfo* pInfo, SResultRow* pResult) {
  // current result is done in computing final results.
  if (pInfo->timeWindowInterpo && isResultRowInterpolated(pResult, RESULT_ROW_END_INTERP)) {
    closeResultRow(pResult);
    SListNode* pNode = tdListPopHead(pResultRowInfo->openWindow);
    taosMemoryFree(pNode);
  }
}

SResultRowPosition addToOpenWindowList(SResultRowInfo* pResultRowInfo, const SResultRow* pResult, uint64_t groupId,
                                       SExecTaskInfo* pTaskInfo) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SOpenWindowInfo openWin = {0};
  openWin.pos.pageId = pResult->pageId;
  openWin.pos.offset = pResult->offset;
  openWin.groupId = groupId;
  SListNode* pn = tdListGetTail(pResultRowInfo->openWindow);
  if (pn == NULL) {
    code = tdListAppend(pResultRowInfo->openWindow, &openWin);
    QUERY_CHECK_CODE(code, lino, _end);
    return openWin.pos;
  }

  SOpenWindowInfo* px = (SOpenWindowInfo*)pn->data;
  if (px->pos.pageId != openWin.pos.pageId || px->pos.offset != openWin.pos.offset || px->groupId != openWin.groupId) {
    code = tdListAppend(pResultRowInfo->openWindow, &openWin);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return openWin.pos;
}

int64_t* extractTsCol(SSDataBlock* pBlock, const SIntervalAggOperatorInfo* pInfo, SExecTaskInfo* pTaskInfo) {
  TSKEY* tsCols = NULL;

  if (pBlock->pDataBlock != NULL && pBlock->info.dataLoad) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
    if (!pColDataInfo) {
      pTaskInfo->code = terrno;
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    tsCols = (int64_t*)pColDataInfo->pData;
    if (tsCols != NULL && pBlock->info.window.skey == 0 && pBlock->info.window.ekey == 0) {
      int32_t code = blockDataUpdateTsWindow(pBlock, pInfo->primaryTsIndex);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
    }

    if (pColDataInfo->pData != NULL && pColDataInfo->hasNull) {
      bool   found = false;
      int64_t minTs = INT64_MAX;
      int64_t maxTs = INT64_MIN;
      for (int32_t i = 0; i < pBlock->info.rows; ++i) {
        if (colDataIsNull_f(pColDataInfo, i)) {
          continue;
        }

        int64_t ts = tsCols[i];
        minTs = TMIN(minTs, ts);
        maxTs = TMAX(maxTs, ts);
        found = true;
      }

      if (found) {
        pBlock->info.window.skey = minTs;
        pBlock->info.window.ekey = maxTs;
      }
    }
  }

  return tsCols;
}

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

static int32_t doOpenIntervalAgg(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  SIntervalAggOperatorInfo* pInfo = pOperator->info;
  SExprSupp*                pSup = &pOperator->exprSupp;
  int64_t                   openStartUs = taosGetTimestampUs();

  if (pSup->pFilterInfo != NULL) {
    filterSetExecContext(pSup->pFilterInfo, pTaskInfo, isTaskKilled);
  }

  int32_t scanFlag = MAIN_SCAN;

  pInfo->cleanGroupResInfo = false;
  if (pInfo->aggSup.pFastInterval != NULL) {
    // FAST_INTERVAL is pulled incrementally from doBuildIntervalResultNext().
    OPTR_SET_OPENED(pOperator);
    return TSDB_CODE_SUCCESS;
  }

  while (1) {
    int64_t downstreamStartUs = taosGetTimestampUs();
    SSDataBlock* pBlock = getNextBlockFromDownstreamRemainDetach(pOperator, 0);
    pInfo->timing.downstreamCalls += 1;
    pInfo->timing.downstreamElapsedUs += taosGetTimestampUs() - downstreamStartUs;
    if (pBlock == NULL) {
      break;
    }

    pInfo->binfo.pRes->info.scanFlag = scanFlag = pBlock->info.scanFlag;

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL,
                                   GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = trimNullTimelineRows(pBlock, pInfo->primaryTsIndex);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pBlock->info.rows == 0) {
      continue;
    }

    // the pDataBlock are always the same one, no need to call this again
    int64_t setInputStartUs = taosGetTimestampUs();
    code = setInputDataBlock(pSup, pBlock, pInfo->binfo.inputTsOrder, scanFlag, true);
    pInfo->timing.setInputCalls += 1;
    pInfo->timing.setInputElapsedUs += taosGetTimestampUs() - setInputStartUs;
    QUERY_CHECK_CODE(code, lino, _end);
    int64_t hashStartUs = taosGetTimestampUs();
    bool    stop = hashIntervalAgg(pOperator, &pInfo->binfo.resultRowInfo, pBlock, scanFlag);
    pInfo->timing.hashCalls += 1;
    pInfo->timing.hashElapsedUs += taosGetTimestampUs() - hashStartUs;
    if (stop) break;
  }

  if (pInfo->indefRowsMode) {
    code = closeAllIndefRowsWindowStates(pOperator, &pInfo->indefRows);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    int64_t initResultStartUs = taosGetTimestampUs();
    code = initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, pInfo->binfo.outputTsOrder);
    pInfo->timing.initResultCalls += 1;
    pInfo->timing.initResultElapsedUs += taosGetTimestampUs() - initResultStartUs;
    QUERY_CHECK_CODE(code, lino, _end);
    pInfo->cleanGroupResInfo = true;
  }

  OPTR_SET_OPENED(pOperator);

_end:
  pInfo->timing.openCalls += 1;
  pInfo->timing.openElapsedUs += taosGetTimestampUs() - openStartUs;
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static int32_t updateFastIntervalWatermark(SFastIntervalInfo* pFast, const SSDataBlock* pBlock) {
  if (pFast == NULL || pBlock == NULL || pBlock->info.window.ekey == INT64_MIN ||
      pBlock->info.id.uid > INT32_MAX) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t code = updateFastWatermarkHeap(&pFast->tableWatermarks, (int32_t)pBlock->info.id.uid,
                                         pBlock->info.window.ekey);
  if (code != TSDB_CODE_SUCCESS) {
    qError("FAST_INTERVAL invalid table index:%" PRIu64 ", tables:%d", pBlock->info.id.uid,
           pFast->tableWatermarks.numOfSlots);
    return code;
  }

  int64_t watermark = INT64_MIN;
  if (getFastWatermarkHeapMin(&pFast->tableWatermarks, &watermark)) {
    pFast->watermark = watermark;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t processFastIntervalInput(SOperatorInfo* pOperator) {
  SIntervalAggOperatorInfo* pInfo = pOperator->info;
  SFastIntervalInfo*        pFast = pInfo->aggSup.pFastInterval;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                pSup = &pOperator->exprSupp;
  if (pFast == NULL || pFast->inputCompleted) {
    return TSDB_CODE_SUCCESS;
  }

  SSDataBlock* pBlock = getNextBlockFromDownstreamRemainDetach(pOperator, 0);
  if (pBlock == NULL) {
    pFast->inputCompleted = true;
    pFast->sealedWindowExclusive = fastIntervalTotalSlots(pFast);
    return TSDB_CODE_SUCCESS;
  }

  pInfo->binfo.pRes->info.scanFlag = pBlock->info.scanFlag;
  if (pInfo->scalarSupp.pExprInfo != NULL) {
    SExprSupp* pExprSup = &pInfo->scalarSupp;
    int32_t code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx,
                                         pExprSup->numOfExprs, NULL, GET_STM_RTINFO(pTaskInfo), pTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  int32_t code = trimNullTimelineRows(pBlock, pInfo->primaryTsIndex);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  code = blockDataUpdateTsWindow(pBlock, pInfo->primaryTsIndex);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = setInputDataBlock(pSup, pBlock, pInfo->binfo.inputTsOrder, pBlock->info.scanFlag, true);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (hashIntervalAgg(pOperator, &pInfo->binfo.resultRowInfo, pBlock, pBlock->info.scanFlag)) {
    pFast->inputCompleted = true;
    pFast->sealedWindowExclusive = fastIntervalTotalSlots(pFast);
    return TSDB_CODE_SUCCESS;
  }

  if (fastIntervalGrouped(pFast)) {
    return TSDB_CODE_SUCCESS;
  }

  code = updateFastIntervalWatermark(pFast, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pFast->watermark != INT64_MIN) {
    int64_t delta = pFast->watermark - pFast->firstWindowStart;
    if (delta > 0) {
      int64_t exclusive = delta / pFast->interval;
      if (exclusive > pFast->numOfWindows) {
        exclusive = pFast->numOfWindows;
      }
      if (exclusive > pFast->sealedWindowExclusive) {
        pFast->sealedWindowExclusive = (int32_t)exclusive;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

// start a new state window and record the start info
void doKeepNewStateWindowStartInfo(SWindowRowsSup* pRowSup, const int64_t* tsList,
  int32_t rowIndex, uint64_t groupId, const EStateWinExtendOption* extendOption, bool hasPrevWin) {
  pRowSup->groupId = groupId;
  if (*extendOption == STATE_WIN_EXTEND_OPTION_DEFAULT ||
      *extendOption == STATE_WIN_EXTEND_OPTION_BACKWARD) {
    pRowSup->win.skey = tsList[rowIndex];
    pRowSup->startRowIndex = rowIndex;
    pRowSup->numOfRows = 0;  // does not include the current row yet
  } else {
    pRowSup->startRowIndex = hasContinuousNullRows(pRowSup) ?
      rowIndex - pRowSup->numNullRows : rowIndex;
    pRowSup->win.skey = hasPrevWin ?
                        pRowSup->win.ekey + 1 : tsList[pRowSup->startRowIndex];
    pRowSup->numOfRows = rowIndex - pRowSup->startRowIndex;
  }
  resetNumNullRows(pRowSup);
}

// close a state window and record its end info
// this functions is called when a new state row appears
// @param rowIndex the index of the first row of next window
void doKeepCurStateWindowEndInfo(SWindowRowsSup* pRowSup, const int64_t* tsList, 
                                 int32_t rowIndex,
                                 const EStateWinExtendOption* extendOption,
                                 bool hasNextWin) {
  /*
   * For deferred rows between two windows:
   * - EXTEND(0): follow single-col default null behavior.
   *   unresolved rows are dropped when state changes.
   * - EXTEND(1): unresolved rows belong to old window.
   * - EXTEND(2): unresolved rows belong to new window
   *   (handled by doKeepNewStateWindowStartInfo).
   */
  if (*extendOption == STATE_WIN_EXTEND_OPTION_DEFAULT) {
      /*
       * Partial-NULL rows must belong to a window.
       * Under EXTEND(0), unresolved all-NULL rows are still handled by the
       * default null behavior, while deferred partial-NULL rows are merged
       * into the previous window.
       */
      if (pRowSup->numDeferredPartialNull > 0) {
        pRowSup->win.ekey = pRowSup->lastDeferredPartialNullTs;
        if (pRowSup->numNullRows < pRowSup->numDeferredPartialNull) {
          qError("%s:%d numNullRows(%u) < numDeferredPartialNull(%u), clamping",
                 __func__, __LINE__, pRowSup->numNullRows,
                 pRowSup->numDeferredPartialNull);
          pRowSup->numNullRows = 0;
        } else {
          pRowSup->numNullRows -= pRowSup->numDeferredPartialNull;
        }
      }
      pRowSup->numOfRows += pRowSup->numDeferredPartialNull;
      pRowSup->numDeferredPartialNull = 0;
      pRowSup->lastDeferredPartialNullTs = INT64_MIN;
      if (hasNextWin) {
        /*
         * State changed: drop unresolved deferred rows
         * (all-NULL + dual-side compatible partial-NULL)
         * between windows.
         */
        resetNumNullRows(pRowSup);
      }
      /*
       * End of block without state change (!hasNextWin): preserve
       * numNullRows so trailing unresolved rows survive cross-block
       * and can be resolved by rows in the next block.
       * Their physical data stays in the unfinished block
       * (numPartialCalcRows excludes them), so aggregation in the
       * next block can read their column values correctly.
       */
  } else if (*extendOption == STATE_WIN_EXTEND_OPTION_BACKWARD) {
      pRowSup->win.ekey = hasNextWin?
                          tsList[rowIndex] - 1 : pRowSup->prevTs;
      pRowSup->numOfRows += hasContinuousNullRows(pRowSup) ?
        pRowSup->numNullRows : 0;
      resetNumNullRows(pRowSup);
  } else if (*extendOption == STATE_WIN_EXTEND_OPTION_FORWARD) {
      /*
       * EXTEND(2) uses deferred rows to extend the next window when a cut
       * happens.
       *
       * At block tail (!hasNextWin), only deferred partial-NULL rows may
       * need to be preserved into current window. Pure all-NULL tail rows
       * must not be merged into the current window; they are unresolved and
       * should follow default tail behavior (typically dropped).
       */
      if (!hasNextWin && pRowSup->numDeferredPartialNull > 0) {
        pRowSup->win.ekey = pRowSup->lastDeferredPartialNullTs;
        pRowSup->numOfRows += pRowSup->numDeferredPartialNull;
        if (pRowSup->numNullRows < pRowSup->numDeferredPartialNull) {
          qError("%s:%d numNullRows(%u) < numDeferredPartialNull(%u), clamping",
                 __func__, __LINE__, pRowSup->numNullRows,
                 pRowSup->numDeferredPartialNull);
          pRowSup->numNullRows = 0;
        } else {
          pRowSup->numNullRows -= pRowSup->numDeferredPartialNull;
        }
        pRowSup->numDeferredPartialNull = 0;
        pRowSup->lastDeferredPartialNullTs = INT64_MIN;
        pRowSup->firstDeferredPartialRowIndex = -1;
      }
  }
}

void doKeepStateWindowNullInfo(SWindowRowsSup* pRowSup, TSKEY nullRowTs) {
  pRowSup->numNullRows += 1;
  pRowSup->prevTs = nullRowTs;
}

static FORCE_INLINE void absorbDeferredTailAllNull(SWindowRowsSup* pRowSup) {
  if (pRowSup->numDeferredTailAllNull == 0) {
    return;
  }

  pRowSup->numDeferredPartialNull += pRowSup->numDeferredTailAllNull;
  pRowSup->numDeferredTailAllNull = 0;
}

static void resetStateKeysUndefined(SStateWindowOperatorInfo* pInfo) {
  int32_t keyNum = taosArrayGetSize(pInfo->stateKeys);
  for (int32_t i = 0; i < keyNum; ++i) {
    SStateKeys* pKey = taosArrayGet(pInfo->stateKeys, i);
    if (pKey != NULL) pKey->isNull = true;
  }
}

static bool stateWindowKeysAllDefined(const SStateWindowOperatorInfo* pInfo) {
  int32_t keyNum = taosArrayGetSize(pInfo->stateKeys);
  for (int32_t i = 0; i < keyNum; ++i) {
    SStateKeys* pKey = taosArrayGet(pInfo->stateKeys, i);
    if (pKey == NULL || pKey->isNull) {
      return false;
    }
  }
  return true;
}

/**
  @brief Process the closed state window and do aggregation on the tuples
  within the window. Partial results are stored in the output buffer. If window
  has no valid rows, return success.
*/
static int32_t processClosedStateWindow(SStateWindowOperatorInfo* pInfo,
                                        SWindowRowsSup* pRowSup,
                                        SSDataBlock* pBlock,
                                        SExecTaskInfo* pTaskInfo,
                                        SExprSupp* pSup,
                                        int32_t numOfOutput,
                                        bool closeWindow) {
  if (pRowSup->numOfRows == 0) {
    // no valid rows in the window
    return TSDB_CODE_SUCCESS;
  }

  if (pInfo->indefRowsMode) {
    return applyIndefRowsWindowSegment(pInfo->pOperator, &pInfo->indefRows, pInfo->binfo.pRes,
                                       pInfo->aggSup.resultRowSize, pRowSup->groupId, &pRowSup->win, pBlock,
                                       pRowSup->startRowIndex, pRowSup->numOfRows, pInfo->binfo.inputTsOrder,
                                       closeWindow);
  }

  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SResultRow* pResult = NULL;
  code = setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &pRowSup->win,
    true, &pResult, pRowSup->groupId, pSup->pCtx, numOfOutput,
    pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
  QUERY_CHECK_CODE(code, lino, _return);

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, 0);
  pResult->nOrigRows += pRowSup->numOfRows;
  code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx,
    &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
    pRowSup->numOfRows, 0, numOfOutput);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

/*
 * Return the per-column aggregate pointer for slotId, or NULL when
 * the block has no aggregates or slotId is out of range.
 */
static FORCE_INLINE struct SColumnDataAgg* getBlockAggForSlot(
    const SSDataBlock* pBlock, int32_t slotId) {
  if (pBlock->pBlockAgg == NULL) {
    return NULL;
  }
  int32_t numCols = taosArrayGetSize(pBlock->pDataBlock);
  if (slotId < 0 || slotId >= numCols) {
    qError("%s pBlockAgg slotId out of bounds, slotId:%d numCols:%d",
           __func__, slotId, numCols);
    return NULL;
  }
  return &pBlock->pBlockAgg[slotId];
}

/*
 * Check NULL status of all state key columns for the given row.
 *   *pAllNull  = true  when every column is NULL.
 *   *pHasNull  = true  when at least one column is NULL
 *                       (includes the all-NULL case).
 */
static int32_t stateWindowRowNullCheck(
    SStateWindowOperatorInfo* pInfo, SSDataBlock* pBlock,
    int32_t rowIndex, bool* pAllNull, bool* pHasNull) {
  int32_t keyNum = taosArrayGetSize(pInfo->stateCols);

  bool anyNull = false;
  bool anyNonNull = false;
  for (int32_t i = 0; i < keyNum; ++i) {
    SColumn* pStateCol = taosArrayGet(pInfo->stateCols, i);
    SColumnInfoData* pColData =
        taosArrayGet(pBlock->pDataBlock, pStateCol->slotId);
    struct SColumnDataAgg* pAgg = getBlockAggForSlot(pBlock, pStateCol->slotId);
    if (pColData == NULL) {
      qError("%s invalid state key column, slotId:%d is missing",
             __func__, pStateCol->slotId);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    if (colDataIsNull(pColData, pBlock->info.rows, rowIndex, pAgg)) {
      anyNull = true;
    } else {
      anyNonNull = true;
    }
  }
  *pAllNull = !anyNonNull;
  *pHasNull = anyNull;
  return TSDB_CODE_SUCCESS;
}

/*
 * A virtual table state column may carry a value wider than the declared width (the full
 * source value is returned by design).  The key buffers are operator-owned heap memory, so
 * grow them to fit instead of overflowing.
 */
static int32_t ensureStateKeyCapacity(SStateKeys* pKey, int32_t needBytes) {
  // A negative length (truncated blob header) would satisfy the capacity test and
  // later become a huge size_t in memcpy -- refuse it.
  if (needBytes < 0) {
    qError("invalid state key length %d", needBytes);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  if (needBytes <= pKey->bytes) {
    return TSDB_CODE_SUCCESS;
  }
  char* p = taosMemoryRealloc(pKey->pData, needBytes);
  if (NULL == p) {
    return terrno;
  }
  pKey->pData = p;
  pKey->bytes = needBytes;
  return TSDB_CODE_SUCCESS;
}

/*
 * Assign state key values from the given row.  Only non-NULL
 * columns are copied; NULL columns keep their previous value
 * (or stay "undefined" if isNull is still true).
 */
static int32_t assignStateWindowKeys(
    SStateWindowOperatorInfo* pInfo, SSDataBlock* pBlock,
    int32_t rowIndex) {
  int32_t keyNum = taosArrayGetSize(pInfo->stateCols);
  for (int32_t i = 0; i < keyNum; ++i) {
    SColumn* pStateCol = taosArrayGet(pInfo->stateCols, i);
    SStateKeys* pKey = taosArrayGet(pInfo->stateKeys, i);
    SColumnInfoData* pColData =
        taosArrayGet(pBlock->pDataBlock, pStateCol->slotId);
    if (pColData == NULL || pColData->pData == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    struct SColumnDataAgg* pAgg = getBlockAggForSlot(pBlock, pStateCol->slotId);
    if (colDataIsNull(pColData, pBlock->info.rows, rowIndex, pAgg)) {
      continue;
    }
    char* pVal = colDataGetData(pColData, rowIndex);
    if (IS_VAR_DATA_TYPE(pKey->type)) {
      int32_t code = ensureStateKeyCapacity(pKey, calcStrBytesByType(pKey->type, pVal));
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
    assignVal(pKey->pData, pVal, pColData->info.bytes, pKey->type);
    pKey->isNull = false;
  }
  return TSDB_CODE_SUCCESS;
}

/*
 * Copy pendingKeys from stateKeys.  Called when a new window
 * opens or after pending rows are flushed.
 */
static int32_t syncPendingKeysFromState(SStateWindowOperatorInfo* pInfo) {
  int32_t keyNum = taosArrayGetSize(pInfo->stateKeys);
  for (int32_t i = 0; i < keyNum; ++i) {
    SStateKeys* pSrc = taosArrayGet(pInfo->stateKeys, i);
    SStateKeys* pDst = taosArrayGet(pInfo->pendingKeys, i);
    if (pSrc == NULL || pDst == NULL) continue;
    if (!pSrc->isNull) {
      // buffers may have been grown independently by ensureStateKeyCapacity
      int32_t code = ensureStateKeyCapacity(pDst, pSrc->bytes);
      if (TSDB_CODE_SUCCESS != code) {
        // Fail the sync instead of leaving pDst claiming a defined value with stale data.
        qError("syncPendingKeysFromState: failed to grow key buffer to %d bytes", pSrc->bytes);
        return code;
      }
      memcpy(pDst->pData, pSrc->pData, pSrc->bytes);
    }
    pDst->isNull = pSrc->isNull;
  }
  memset(pInfo->pendingColTouched, 0, sizeof(bool) * keyNum);
  pInfo->hasPendingPartialNull = false;
  return TSDB_CODE_SUCCESS;
}

/*
 * Copy stateKeys from pendingKeys.  Called when flushing
 * pending rows into the current window (all-non-NULL confirm).
 */
static int32_t syncStateKeysFromPending(SStateWindowOperatorInfo* pInfo) {
  int32_t keyNum = taosArrayGetSize(pInfo->stateKeys);
  for (int32_t i = 0; i < keyNum; ++i) {
    SStateKeys* pSrc = taosArrayGet(pInfo->pendingKeys, i);
    SStateKeys* pDst = taosArrayGet(pInfo->stateKeys, i);
    if (pSrc == NULL || pDst == NULL) continue;
    if (!pSrc->isNull) {
      // buffers may have been grown independently by ensureStateKeyCapacity
      int32_t code = ensureStateKeyCapacity(pDst, pSrc->bytes);
      if (TSDB_CODE_SUCCESS != code) {
        // Fail the sync instead of leaving pDst claiming a defined value with stale data.
        qError("syncStateKeysFromPending: failed to grow key buffer to %d bytes", pSrc->bytes);
        return code;
      }
      memcpy(pDst->pData, pSrc->pData, pSrc->bytes);
    }
    pDst->isNull = pSrc->isNull;
  }
  return TSDB_CODE_SUCCESS;
}

static void resetPendingState(SStateWindowOperatorInfo* pInfo) {
  int32_t keyNum = taosArrayGetSize(pInfo->stateKeys);
  memset(pInfo->pendingColTouched, 0, sizeof(bool) * keyNum);
  pInfo->hasPendingPartialNull = false;
}

/*
 * Read-only comparison against pendingKeys.  Returns *pEqual =
 * false if any column is judged "changed".
 */
static int32_t checkPendingKeysCompatible(
    SStateWindowOperatorInfo* pInfo, SSDataBlock* pBlock,
    int32_t rowIndex, bool* pEqual) {
  int32_t keyNum = taosArrayGetSize(pInfo->stateCols);
  *pEqual = true;
  for (int32_t i = 0; i < keyNum; ++i) {
    SColumn* pStateCol = taosArrayGet(pInfo->stateCols, i);
    SStateKeys* pKey = taosArrayGet(pInfo->pendingKeys, i);
    SColumnInfoData* pColData =
        taosArrayGet(pBlock->pDataBlock, pStateCol->slotId);
    if (pColData == NULL || pColData->pData == NULL
        || pKey == NULL || pKey->pData == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    struct SColumnDataAgg* pAgg = getBlockAggForSlot(pBlock, pStateCol->slotId);
    if (colDataIsNull(pColData, pBlock->info.rows, rowIndex, pAgg)) {
      continue;
    }
    if (pKey->isNull) {
      continue;  /* init: not a change */
    }
    if (!compareVal(colDataGetData(pColData, rowIndex), pKey)) {
      *pEqual = false;
      return TSDB_CODE_SUCCESS;
    }
  }
  return TSDB_CODE_SUCCESS;
}

/*
 * Update pendingKeys from a deferred partial-NULL row and mark
 * touched columns.
 */
static int32_t updatePendingKeysFromRow(
    SStateWindowOperatorInfo* pInfo, SSDataBlock* pBlock,
    int32_t rowIndex) {
  int32_t keyNum = taosArrayGetSize(pInfo->stateCols);
  for (int32_t i = 0; i < keyNum; ++i) {
    SColumn* pStateCol = taosArrayGet(pInfo->stateCols, i);
    SStateKeys* pKey = taosArrayGet(pInfo->pendingKeys, i);
    SColumnInfoData* pColData =
        taosArrayGet(pBlock->pDataBlock, pStateCol->slotId);
    if (pColData == NULL || pColData->pData == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    struct SColumnDataAgg* pAgg = getBlockAggForSlot(pBlock, pStateCol->slotId);
    if (colDataIsNull(pColData, pBlock->info.rows, rowIndex, pAgg)) {
      continue;
    }
    if (pKey->isNull) {
      char* pVal = colDataGetData(pColData, rowIndex);
      if (IS_VAR_DATA_TYPE(pKey->type)) {
        int32_t code = ensureStateKeyCapacity(pKey, calcStrBytesByType(pKey->type, pVal));
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
      }
      assignVal(pKey->pData, pVal, pColData->info.bytes, pKey->type);
      pKey->isNull = false;
    }
    pInfo->pendingColTouched[i] = true;
  }
  pInfo->hasPendingPartialNull = true;
  return TSDB_CODE_SUCCESS;
}

/*
 * At cut time, check whether the pending partial-NULL segment
 * is dual-side compatible with the new window's first row.
 *
 * For each column touched by a pending partial-NULL row, check
 * if the value in pendingKeys is compatible with the new row:
 *   - new row col is NULL          -> compatible (can init)
 *   - pending col undefined        -> compatible
 *   - new row col equals pending   -> compatible
 *   - otherwise                    -> NOT compatible
 */
static int32_t checkPendingDualSideCompatible(
    SStateWindowOperatorInfo* pInfo, SSDataBlock* pBlock,
    int32_t newRowIndex, bool* pCompatible) {
  int32_t keyNum = taosArrayGetSize(pInfo->stateCols);
  *pCompatible = true;

  if (!pInfo->hasPendingPartialNull) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < keyNum; ++i) {
    if (!pInfo->pendingColTouched[i]) continue;
    SColumn* pStateCol = taosArrayGet(pInfo->stateCols, i);
    SStateKeys* pPendKey = taosArrayGet(pInfo->pendingKeys, i);
    SColumnInfoData* pColData =
        taosArrayGet(pBlock->pDataBlock, pStateCol->slotId);
    if (pColData == NULL || pPendKey == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    struct SColumnDataAgg* pAgg = getBlockAggForSlot(pBlock, pStateCol->slotId);
    if (colDataIsNull(pColData, pBlock->info.rows, newRowIndex, pAgg)) {
      continue;
    }
    if (pPendKey->isNull) {
      continue;
    }
    if (!compareVal(colDataGetData(pColData, newRowIndex), pPendKey)) {
      *pCompatible = false;
      return TSDB_CODE_SUCCESS;
    }
  }
  return TSDB_CODE_SUCCESS;
}

/*
 * Commit pending rows to the current (old) window: add
 * numNullRows to numOfRows, copy pendingKeys -> stateKeys.
 */
static int32_t commitPendingToOldWindow(
    SStateWindowOperatorInfo* pInfo, SWindowRowsSup* pRowSup) {
  /*
   * Partial-NULL rows must belong to a window (cannot be dropped).
   * Add them to numOfRows now.  Remaining all-NULL rows in
   * numNullRows are left for doKeepCurStateWindowEndInfo/EXTEND.
   */
  if (pRowSup->numDeferredPartialNull > 0) {
    pRowSup->win.ekey = pRowSup->lastDeferredPartialNullTs;
  }
  pRowSup->numOfRows += pRowSup->numDeferredPartialNull;
  if (pRowSup->numNullRows < pRowSup->numDeferredPartialNull) {
    qError("%s:%d numNullRows(%u) < numDeferredPartialNull(%u), clamping",
           __func__, __LINE__, pRowSup->numNullRows,
           pRowSup->numDeferredPartialNull);
    pRowSup->numNullRows = 0;
  } else {
    pRowSup->numNullRows -= pRowSup->numDeferredPartialNull;
  }
  pRowSup->numDeferredPartialNull = 0;
  pRowSup->firstDeferredPartialRowIndex = -1;
  pRowSup->lastDeferredPartialNullTs = INT64_MIN;

  /* Only sync columns touched by deferred partial-NULL rows */
  int32_t keyNum = taosArrayGetSize(pInfo->stateKeys);
  for (int32_t i = 0; i < keyNum; ++i) {
    if (!pInfo->pendingColTouched[i]) continue;
    SStateKeys* pSrc = taosArrayGet(pInfo->pendingKeys, i);
    SStateKeys* pDst = taosArrayGet(pInfo->stateKeys, i);
    if (pSrc == NULL || pDst == NULL) continue;
    if (!pSrc->isNull) {
      // buffers may have been grown independently by ensureStateKeyCapacity
      int32_t code = ensureStateKeyCapacity(pDst, pSrc->bytes);
      if (TSDB_CODE_SUCCESS != code) {
        // Fail the commit instead of leaving pDst claiming a defined value with stale data.
        qError("%s:%d failed to grow key buffer to %d bytes", __func__, __LINE__, pSrc->bytes);
        return code;
      }
      memcpy(pDst->pData, pSrc->pData, pSrc->bytes);
    }
    pDst->isNull = pSrc->isNull;
  }
  resetPendingState(pInfo);
  return TSDB_CODE_SUCCESS;
}

static bool shouldSplitDeferredPartialStandalone(const SStateWindowOperatorInfo* pInfo,
             const SWindowRowsSup* pRowSup,
             bool dualSide,
             EStateWinExtendOption extendOption) {
  return (!dualSide && extendOption == STATE_WIN_EXTEND_OPTION_FORWARD &&
          pRowSup->numDeferredPartialNull > 0 &&
    pRowSup->firstDeferredPartialRowIndex >= 0 &&
    stateWindowKeysAllDefined(pInfo));
}

static int32_t processStandaloneDeferredPartialWindow(
    SStateWindowOperatorInfo* pInfo, SWindowRowsSup* pRowSup, SSDataBlock* pBlock,
    SExecTaskInfo* pTaskInfo, SExprSupp* pExprSup, int32_t numOfOutput) {
  if (pRowSup->numDeferredPartialNull == 0 ||
      pRowSup->firstDeferredPartialRowIndex < 0) {
    return TSDB_CODE_SUCCESS;
  }

  SWindowRowsSup partialWin = {0};
  partialWin.groupId = pRowSup->groupId;
  partialWin.startRowIndex = pRowSup->firstDeferredPartialRowIndex;
  partialWin.numOfRows = (int32_t)pRowSup->numDeferredPartialNull;
  partialWin.win.skey = pRowSup->win.ekey + 1;
  partialWin.win.ekey = pRowSup->lastDeferredPartialNullTs;

  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
  if (pTsCol == NULL || pTsCol->pData == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t code = processClosedStateWindow(
      pInfo, &partialWin, pBlock, pTaskInfo, pExprSup, numOfOutput, true);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  /*
   * Keep window continuity for EXTEND(2): after emitting a standalone
   * deferred partial-NULL window, the next window start is derived from
   * previous ekey + 1. Update ekey here so the next window starts right
   * after the standalone window instead of after the old window.
   */
  pRowSup->win.ekey = partialWin.win.ekey;

  if (pRowSup->numNullRows < pRowSup->numDeferredPartialNull) {
    qError("%s:%d numNullRows(%u) < numDeferredPartialNull(%u), clamping",
           __func__, __LINE__, pRowSup->numNullRows,
           pRowSup->numDeferredPartialNull);
    pRowSup->numNullRows = 0;
  } else {
    pRowSup->numNullRows -= pRowSup->numDeferredPartialNull;
  }
  pRowSup->numDeferredPartialNull = 0;
  pRowSup->firstDeferredPartialRowIndex = -1;
  pRowSup->lastDeferredPartialNullTs = INT64_MIN;
  resetPendingState(pInfo);
  return TSDB_CODE_SUCCESS;
}

/*
 * Two-phase comparison against pendingKeys.
 *
 * Phase 1 (read-only):
 *   - ri is NULL          -> skip
 *   - ri non-NULL, pi undefined -> not a change
 *   - ri non-NULL, pi defined   -> compare; ri != pi => changed
 *
 * Phase 2 (commit):
 *   - only when no column changed, initialize undefined pi with ri.
 *
 * This ensures "undefined -> defined" does not force a cut-window by itself.
 */
static int32_t compareStateWindowKeys(
    SStateWindowOperatorInfo* pInfo, SSDataBlock* pBlock,
    int32_t rowIndex, bool* pEqual) {
  int32_t keyNum = taosArrayGetSize(pInfo->stateCols);
  *pEqual = true;
  for (int32_t i = 0; i < keyNum; ++i) {
    SColumn* pStateCol = taosArrayGet(pInfo->stateCols, i);
    SStateKeys* pKey = taosArrayGet(pInfo->pendingKeys, i);
    SColumnInfoData* pColData =
        taosArrayGet(pBlock->pDataBlock, pStateCol->slotId);
    if (pColData == NULL || pColData->pData == NULL
        || pKey == NULL || pKey->pData == NULL) {
      qError("%s invalid state key at slotId:%d",
             __func__, pStateCol->slotId);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    struct SColumnDataAgg* pAgg = getBlockAggForSlot(pBlock, pStateCol->slotId);

    if (colDataIsNull(pColData, pBlock->info.rows, rowIndex, pAgg)) {
      continue;
    }

    if (pKey->isNull) {
      continue;
    }

    if (!compareVal(colDataGetData(pColData, rowIndex), pKey)) {
      *pEqual = false;
      return TSDB_CODE_SUCCESS;
    }
  }

  /* phase 2: no change, initialize undefined columns */
  for (int32_t i = 0; i < keyNum; ++i) {
    SColumn* pStateCol = taosArrayGet(pInfo->stateCols, i);
    SStateKeys* pKey = taosArrayGet(pInfo->pendingKeys, i);
    SColumnInfoData* pColData =
        taosArrayGet(pBlock->pDataBlock, pStateCol->slotId);
    if (pColData == NULL || pColData->pData == NULL
        || pKey == NULL || pKey->pData == NULL) {
      qError("%s invalid state key at slotId:%d",
             __func__, pStateCol->slotId);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    if (!pKey->isNull) {
      continue;
    }

    struct SColumnDataAgg* pAgg = getBlockAggForSlot(pBlock, pStateCol->slotId);
    if (colDataIsNull(pColData, pBlock->info.rows, rowIndex, pAgg)) {
      continue;
    }

    char* pVal = colDataGetData(pColData, rowIndex);
    if (IS_VAR_DATA_TYPE(pKey->type)) {
      int32_t code = ensureStateKeyCapacity(pKey, calcStrBytesByType(pKey->type, pVal));
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
    assignVal(pKey->pData, pVal, pColData->info.bytes, pKey->type);
    pKey->isNull = false;
  }
  return TSDB_CODE_SUCCESS;
}

// process a data block for state window aggregation
// scan from startIndex to endIndex
// numPartialCalcRows returns the number of rows that have been
// partially calculated within the block
static void doStateWindowAggImpl(SOperatorInfo* pOperator,
                                 SStateWindowOperatorInfo* pInfo,
                                 SSDataBlock* pBlock, int32_t* startIndex,
                                 int32_t* endIndex,
                                 int32_t* numPartialCalcRows) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*     pExprSup = &pOperator->exprSupp;
  uint64_t gid = pBlock->info.id.groupId;
  int32_t numOfOutput = pOperator->exprSupp.numOfExprs;

  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock,
                                               pInfo->tsSlotId);
  if (NULL == pColInfoData) {
    pTaskInfo->code = terrno;
    T_LONG_JMP(pTaskInfo->env, terrno);
  }
  TSKEY* tsList = (TSKEY*)pColInfoData->pData;

  EStateWinExtendOption  extendOption = pInfo->extendOption;
  SWindowRowsSup*        pRowSup = &pInfo->winSup;

  if (pRowSup->groupId != gid) {
    /*
      group changed, process the previous group's unclosed state window first
    */
    doKeepCurStateWindowEndInfo(pRowSup, tsList, 0, &extendOption, false);
    int32_t code = processClosedStateWindow(pInfo, pRowSup, pBlock, pTaskInfo,
                                            pExprSup, numOfOutput, true);
    if (TSDB_CODE_SUCCESS != code) T_LONG_JMP(pTaskInfo->env, code);
    *numPartialCalcRows = pRowSup->startRowIndex + pRowSup->numOfRows;

    /*
      unhandled null rows should be ignored, since they belong to previous group
    */
    *numPartialCalcRows += pRowSup->numNullRows;

    /*
      reset state window info for new group
    */
    pInfo->hasKey = false;
    resetStateKeysUndefined(pInfo);
    resetPendingState(pInfo);
    resetWindowRowsSup(pRowSup);
  }

  for (int32_t j = *startIndex; j < *endIndex; ++j) {
    if (pColInfoData->hasNull && colDataIsNull_f(pColInfoData, j)) {
      continue;
    }

    if (pBlock->info.scanFlag != PRE_SCAN) {
      if (pInfo->winSup.lastTs == INT64_MIN || gid != pRowSup->groupId || !pInfo->hasKey) {
        pInfo->winSup.lastTs = tsList[j];
      } else {
        if (tsList[j] != pInfo->winSup.lastTs) {
          pInfo->winSup.lastTs = tsList[j];
        }
      }
    }
    bool    allNull = false;
    bool    hasNull = false;
    int32_t code = stateWindowRowNullCheck(pInfo, pBlock, j, &allNull, &hasNull);
    if (TSDB_CODE_SUCCESS != code) {
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }

    if (allNull) {
      doKeepStateWindowNullInfo(pRowSup, tsList[j]);
      if (pRowSup->numDeferredPartialNull > 0) {
        pRowSup->numDeferredTailAllNull++;
      }
      continue;
    }

    if (!pInfo->hasKey) {
      code = assignStateWindowKeys(pInfo, pBlock, j);
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
      code = syncPendingKeysFromState(pInfo);
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
      pInfo->hasKey = true;
      doKeepNewStateWindowStartInfo(
        pRowSup, tsList, j, gid, &extendOption, false);
      doKeepTuple(pRowSup, tsList[j], j, gid);
    } else if (hasNull) {
      /*
       * Partial NULL row: check compatibility against pendingKeys
       * (read-only).  If compatible, defer like an all-NULL row
       * so that EXTEND decides its final window assignment.
       */
      bool keysEqual = false;
      code = checkPendingKeysCompatible(pInfo, pBlock, j, &keysEqual);
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
      if (keysEqual) {
        absorbDeferredTailAllNull(pRowSup);
        code = updatePendingKeysFromRow(pInfo, pBlock, j);
        if (TSDB_CODE_SUCCESS != code) {
          pTaskInfo->code = code;
          T_LONG_JMP(pTaskInfo->env, code);
        }
        doKeepStateWindowNullInfo(pRowSup, tsList[j]);
        if (pRowSup->numDeferredPartialNull == 0) {
          pRowSup->firstDeferredPartialRowIndex = j;
        }
        pRowSup->numDeferredPartialNull++;
        pRowSup->lastDeferredPartialNullTs = tsList[j];
        continue;
      }
      /* not compatible  ->  resolve pending, then cut */
      bool dualSide = false;
      code = checkPendingDualSideCompatible(pInfo, pBlock, j, &dualSide);
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
      bool splitStandalone =
          shouldSplitDeferredPartialStandalone(pInfo, pRowSup, dualSide, extendOption);
      if (!dualSide && !splitStandalone) {
        code = commitPendingToOldWindow(pInfo, pRowSup);
        if (TSDB_CODE_SUCCESS != code) {
          pTaskInfo->code = code;
          T_LONG_JMP(pTaskInfo->env, code);
        }
      }
      /* numNullRows left for EXTEND when dualSide==true */
      doKeepCurStateWindowEndInfo(pRowSup, tsList, j, &extendOption, true);
      code = processClosedStateWindow(pInfo, pRowSup, pBlock, pTaskInfo,
                                              pExprSup, numOfOutput, true);
      if (TSDB_CODE_SUCCESS != code) {
        T_LONG_JMP(pTaskInfo->env, code);
      }
      *numPartialCalcRows = pRowSup->startRowIndex + pRowSup->numOfRows;
      if (splitStandalone) {
        code = processStandaloneDeferredPartialWindow(
            pInfo, pRowSup, pBlock, pTaskInfo, pExprSup, numOfOutput);
        if (TSDB_CODE_SUCCESS != code) {
          T_LONG_JMP(pTaskInfo->env, code);
        }
      }

      doKeepNewStateWindowStartInfo(pRowSup, tsList, j, gid,
                                    &extendOption, true);
      doKeepTuple(pRowSup, tsList[j], j, gid);
      resetStateKeysUndefined(pInfo);
      code = assignStateWindowKeys(pInfo, pBlock, j);
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
      code = syncPendingKeysFromState(pInfo);
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
    } else {
      /* all non-NULL row */
      bool keysEqual = false;
      code = compareStateWindowKeys(pInfo, pBlock, j, &keysEqual);
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
      if (keysEqual) {
        /*
         * Flush pending: sync committed stateKeys from pendingKeys
         * (deferred rows may have initialized undefined columns),
         * then doKeepTuple absorbs numNullRows (all-NULL + partial-NULL).
         */
        code = syncStateKeysFromPending(pInfo);
        if (TSDB_CODE_SUCCESS != code) {
          pTaskInfo->code = code;
          T_LONG_JMP(pTaskInfo->env, code);
        }
        doKeepTuple(pRowSup, tsList[j], j, gid);
        pRowSup->numDeferredPartialNull = 0;
        pRowSup->firstDeferredPartialRowIndex = -1;
        resetPendingState(pInfo);
        continue;
      }
      /* not compatible  ->  resolve pending, then cut */
      bool dualSide = false;
      code = checkPendingDualSideCompatible(pInfo, pBlock, j, &dualSide);
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
      bool splitStandalone =
          shouldSplitDeferredPartialStandalone(pInfo, pRowSup, dualSide, extendOption);
      if (!dualSide && !splitStandalone) {
        code = commitPendingToOldWindow(pInfo, pRowSup);
        if (TSDB_CODE_SUCCESS != code) {
          pTaskInfo->code = code;
          T_LONG_JMP(pTaskInfo->env, code);
        }
      }
      doKeepCurStateWindowEndInfo(pRowSup, tsList, j, &extendOption, true);
      code = processClosedStateWindow(pInfo, pRowSup, pBlock, pTaskInfo,
                                              pExprSup, numOfOutput, true);
      if (TSDB_CODE_SUCCESS != code) {
        T_LONG_JMP(pTaskInfo->env, code);
      }
      *numPartialCalcRows = pRowSup->startRowIndex + pRowSup->numOfRows;
      if (splitStandalone) {
        code = processStandaloneDeferredPartialWindow(
            pInfo, pRowSup, pBlock, pTaskInfo, pExprSup, numOfOutput);
        if (TSDB_CODE_SUCCESS != code) {
          T_LONG_JMP(pTaskInfo->env, code);
        }
      }

      doKeepNewStateWindowStartInfo(pRowSup, tsList, j, gid,
                                    &extendOption, true);
      doKeepTuple(pRowSup, tsList[j], j, gid);
      resetStateKeysUndefined(pInfo);
      code = assignStateWindowKeys(pInfo, pBlock, j);
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
      code = syncPendingKeysFromState(pInfo);
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
    }
  }

  if (!pInfo->hasKey && extendOption != STATE_WIN_EXTEND_OPTION_FORWARD) {
    /*
      No valid state rows within the block and we don't care about
      null rows before valid state window, mark them as processed and drop them
    */
    *numPartialCalcRows = pBlock->info.rows;
    resetNumNullRows(pRowSup);
    return;
  }
  if (pRowSup->numOfRows == 0 &&
      extendOption != STATE_WIN_EXTEND_OPTION_BACKWARD) {
    /*
      If no valid state window or we don't know the belonging of
      null rows in the end of the block, handle them with next block
    */
    return;
  }
  doKeepCurStateWindowEndInfo(pRowSup, tsList, *endIndex, &extendOption, false);
  int32_t code = processClosedStateWindow(pInfo, pRowSup, pBlock, pTaskInfo,
                                          pExprSup, numOfOutput, false);
  if (TSDB_CODE_SUCCESS != code) {
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  *numPartialCalcRows = pRowSup->startRowIndex + pRowSup->numOfRows;
  // reset part of pRowSup after doing agg calculation
  pRowSup->startRowIndex = 0;
  pRowSup->numOfRows = 0;
}

static int32_t openStateWindowAggOptr(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SStateWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;

  SExprSupp* pSup = &pOperator->exprSupp;
  int32_t    order = pInfo->binfo.inputTsOrder;

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  pInfo->cleanGroupResInfo = false;

  SSDataBlock* pUnfinishedBlock = NULL;
  int32_t      startIndex = 0;
  int32_t      endIndex = 0;
  int32_t      numPartialCalcRows = 0;
  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      if (pUnfinishedBlock != NULL) {
        blockDataDestroy(pUnfinishedBlock);
        pUnfinishedBlock = NULL;
        resetWindowRowsSup(&pInfo->winSup);
      }
      break;
    }
    
    // mark whether pUnfinishedBlock is a reference to pBlock
    bool isRef = false;
    startIndex = 0;
    if (pUnfinishedBlock != NULL) {
      startIndex = pUnfinishedBlock->info.rows;
      // merge unfinished block with current block
      code = blockDataMerge(pUnfinishedBlock, pBlock);
      // reset id to current block id
      pUnfinishedBlock->info.id = pBlock->info.id;
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      pUnfinishedBlock = pBlock;
      isRef = true;
    }
    endIndex = pUnfinishedBlock->info.rows;

    pInfo->binfo.pRes->info.scanFlag = pUnfinishedBlock->info.scanFlag;
    pInfo->binfo.pRes->info.dataLoad = 1;
    code = setInputDataBlock(
      pSup, pUnfinishedBlock, order, pUnfinishedBlock->info.scanFlag, true);
    QUERY_CHECK_CODE(code, lino, _end);

    code = blockDataUpdateTsWindow(pUnfinishedBlock, pInfo->tsSlotId);
    QUERY_CHECK_CODE(code, lino, _end);

    // there is an scalar expression that 
    // needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      pTaskInfo->code = projectApplyFunctions(pInfo->scalarSup.pExprInfo,
        pUnfinishedBlock, pUnfinishedBlock, pInfo->scalarSup.pCtx,
        pInfo->scalarSup.numOfExprs, NULL,
        GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo);
      if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
      }
    }

    doStateWindowAggImpl(pOperator, pInfo, pUnfinishedBlock, 
      &startIndex, &endIndex, &numPartialCalcRows);
    if (numPartialCalcRows < pUnfinishedBlock->info.rows) {
      // save unfinished block for next round processing
      if (isRef) {
        code = createOneDataBlock(pBlock, true, &pUnfinishedBlock);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = blockDataTrimFirstRows(pUnfinishedBlock, numPartialCalcRows);
      QUERY_CHECK_NULL(pUnfinishedBlock, code, lino, _end, terrno);
    } else {
      if (!isRef) {
        blockDataDestroy(pUnfinishedBlock);
      }
      pUnfinishedBlock = NULL;
    }
    numPartialCalcRows = 0;
  }

  if (pInfo->indefRowsMode) {
    code = closeAllIndefRowsWindowStates(pOperator, &pInfo->indefRows);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    code = initGroupedResultInfo(
      &pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, TSDB_ORDER_ASC);
    QUERY_CHECK_CODE(code, lino, _end);
    pInfo->cleanGroupResInfo = true;
  }
  pOperator->status = OP_RES_TO_RETURN;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static int32_t doStateWindowAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SStateWindowOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*           pBInfo = &pInfo->binfo;

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pInfo->indefRowsMode) {
    (*ppRes) = getNextIndefRowsResultBlock(&pInfo->indefRows, pOperator);
    if ((*ppRes) == NULL) {
      setOperatorCompleted(pOperator);
    }
    return code;
  }

  code = blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  while (1) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    code = doFilterWithTaskCorrelation(pBInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL,
                                       pOperator->pTaskInfo);
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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) = (pBInfo->pRes->info.rows == 0) ? NULL : pBInfo->pRes;
  return code;
}

static int32_t buildFastIntervalResultBlock(SOperatorInfo* pOperator, SIntervalAggOperatorInfo* pInfo) {
  int32_t            code = TSDB_CODE_SUCCESS;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*         pSup = &pOperator->exprSupp;
  SSDataBlock*       pBlock = pInfo->binfo.pRes;
  SFastIntervalInfo* pFast = pInfo->aggSup.pFastInterval;
  int32_t            resultBatchRows = TMIN(pOperator->resultInfo.threshold, FAST_INTERVAL_LOCAL_RESULT_BATCH_ROWS);

  blockDataCleanup(pBlock);
  pBlock->info.id.groupId = 0;
  pBlock->info.version = pTaskInfo->version;
  pBlock->info.dataLoad = 1;
  int32_t totalSlots = fastIntervalTotalSlots(pFast);
  if (pFast->inputCompleted && pFast->activeCount == 0) {
    pFast->nextResult = totalSlots;
    setOperatorCompleted(pOperator);
    return TSDB_CODE_SUCCESS;
  }
  if (fastIntervalGrouped(pFast) && !pFast->inputCompleted) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t sealedWindowExclusive = fastIntervalGrouped(pFast) ? totalSlots : pFast->sealedWindowExclusive;
  bool sparse = !pFast->denseMode && pFast->activeCount > 0 &&
                (int64_t)pFast->activeCount * 5 < (int64_t)(pFast->maxActiveWindow - pFast->minActiveWindow + 1);
  while (pFast->nextResult < sealedWindowExclusive && pBlock->info.rows < resultBatchRows) {
    int32_t index = pFast->nextResult;
    if (sparse) {
      index = fastIntervalNextActiveWindow(pFast, index);
      if (index >= sealedWindowExclusive) {
        pFast->nextResult = sealedWindowExclusive;
        break;
      }
    }
    int32_t groupSlot = index / pFast->numOfWindows;
    if (pBlock->info.rows > 0 && pBlock->info.id.groupId != pFast->pGroupIds[groupSlot]) {
      break;
    }
    pBlock->info.id.groupId = pFast->pGroupIds[groupSlot];
    pFast->nextResult = index + 1;
    int32_t rowIndex = pFast->pRowIndexes[index] - 1;
    if (rowIndex < 0) continue;

    SResultRow* pRow = (SResultRow*)(pFast->pRows + (size_t)rowIndex * pInfo->aggSup.resultRowSize);
    code = setResultRowInitCtx(pRow, pSup->pCtx, pSup->numOfExprs, pSup->rowEntryInfoOffset);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    doUpdateNumOfRows(pSup->pCtx, pRow, pSup->numOfExprs, pSup->rowEntryInfoOffset);
    if (pRow->numOfRows == 0) {
      continue;
    }
    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      code = blockDataEnsureCapacity(pBlock, pBlock->info.rows + pRow->numOfRows);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRow->win, 0);
    code = copyResultrowToDataBlock(pSup->pExprInfo, pSup->numOfExprs, pRow, pSup->pCtx, pBlock,
                                    pSup->rowEntryInfoOffset, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pBlock->info.rows += pRow->numOfRows;
  }
  if (pBlock->info.rows > 0) {
    code = blockDataUpdateTsWindow(pBlock, 0);
  }
  return code;
}

static int32_t doBuildIntervalResultNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SIntervalAggOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE && !pOperator->pOperatorGetParam) {
    (*ppRes) = NULL;
    return code;
  }

  if (pOperator->pOperatorGetParam) {
    if (pOperator->status == OP_EXEC_DONE && pOperator->fpSet.resetStateFn) {
      code = pOperator->fpSet.resetStateFn(pOperator);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
    pOperator->pOperatorGetParam = NULL;
  }

  if (pTaskInfo->localFetch.directBlock && pOperator->status != OP_EXEC_DONE && pInfo->binfo.pRes != NULL &&
      pInfo->binfo.pRes->info.rows > 0) {
    SSDataBlock* pNextBlock = NULL;
    code = createOneDataBlock(pInfo->binfo.pRes, false, &pNextBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    code = blockDataEnsureCapacity(pNextBlock, pOperator->resultInfo.capacity);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pNextBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    pInfo->binfo.pRes = pNextBlock;
  }

  if (pInfo->pExInterval != NULL) {
    code = processExIntervalInput(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);
    setOperatorCompleted(pOperator);
    *ppRes = NULL;
    return code;
  }

  SSDataBlock* pBlock = pInfo->binfo.pRes;
  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pInfo->indefRowsMode) {
    (*ppRes) = getNextIndefRowsResultBlock(&pInfo->indefRows, pOperator);
    if ((*ppRes) == NULL) {
      setOperatorCompleted(pOperator);
    }
    return code;
  }

  if (pInfo->aggSup.pFastInterval != NULL) {
    while (1) {
      SFastIntervalInfo* pFast = pInfo->aggSup.pFastInterval;
      if (pFast->nextResult >= pFast->sealedWindowExclusive && !pFast->inputCompleted) {
        code = processFastIntervalInput(pOperator);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      code = buildFastIntervalResultBlock(pOperator, pInfo);
      QUERY_CHECK_CODE(code, lino, _end);
      code = doFilterWithTaskCorrelation(pBlock, pOperator->exprSupp.pFilterInfo, NULL, NULL,
                                         pOperator->pTaskInfo);
      QUERY_CHECK_CODE(code, lino, _end);
      pFast = pInfo->aggSup.pFastInterval;
      if (pFast->inputCompleted && pFast->nextResult >= fastIntervalTotalSlots(pFast)) {
        setOperatorCompleted(pOperator);
      }
      if (pBlock->info.rows > 0 || pOperator->status == OP_EXEC_DONE) {
        break;
      }
    }
    (*ppRes) = (pBlock->info.rows == 0) ? NULL : pBlock;
    if (*ppRes != NULL && pTaskInfo->localFetch.directBlock) {
      if (pOperator->status == OP_EXEC_DONE) {
        pInfo->binfo.pRes = NULL;
      } else {
        SSDataBlock* pNextBlock = NULL;
        code = createOneDataBlock(pBlock, false, &pNextBlock);
        QUERY_CHECK_CODE(code, lino, _end);
        code = blockDataEnsureCapacity(pNextBlock, pOperator->resultInfo.capacity);
        if (code != TSDB_CODE_SUCCESS) {
          blockDataDestroy(pNextBlock);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        pInfo->binfo.pRes = pNextBlock;
      }
    }
    return code;
  }

  while (1) {
    int64_t buildStartUs = taosGetTimestampUs();
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    pInfo->timing.buildCalls += 1;
    pInfo->timing.buildElapsedUs += taosGetTimestampUs() - buildStartUs;
    int64_t filterStartUs = taosGetTimestampUs();
    code = doFilterWithTaskCorrelation(pBlock, pOperator->exprSupp.pFilterInfo, NULL, NULL,
                                       pOperator->pTaskInfo);
    pInfo->timing.filterCalls += 1;
    pInfo->timing.filterElapsedUs += taosGetTimestampUs() - filterStartUs;
    QUERY_CHECK_CODE(code, lino, _end);

    bool hasRemain = hasRemainResults(&pInfo->groupResInfo);
    if (!hasRemain) {
      setOperatorCompleted(pOperator);
      break;
    }

    if (pBlock->info.rows > 0) {
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) = (pBlock->info.rows == 0) ? NULL : pBlock;
  if (*ppRes != NULL && pTaskInfo->localFetch.directBlock) {
    if (pOperator->status == OP_EXEC_DONE) {
      pInfo->binfo.pRes = NULL;
    } else {
      SSDataBlock* pNextBlock = NULL;
      code = createOneDataBlock(pBlock, false, &pNextBlock);
      QUERY_CHECK_CODE(code, lino, _end);
      code = blockDataEnsureCapacity(pNextBlock, pOperator->resultInfo.capacity);
      if (code != TSDB_CODE_SUCCESS) {
        blockDataDestroy(pNextBlock);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      pInfo->binfo.pRes = pNextBlock;
    }
  }
  return code;
}

static void destroyStateWindowOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }
  SStateWindowOperatorInfo* pInfo = (SStateWindowOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  cleanupIndefRowsRuntime(&pInfo->indefRows, pInfo->pOperator);
  if (pInfo->stateKeys != NULL) {
    int32_t keyNum = taosArrayGetSize(pInfo->stateKeys);
    for (int32_t i = 0; i < keyNum; ++i) {
      SStateKeys* pKey = taosArrayGet(pInfo->stateKeys, i);
      taosMemoryFreeClear(pKey->pData);
    }
  }
  taosArrayDestroy(pInfo->stateKeys);
  if (pInfo->pendingKeys != NULL) {
    int32_t keyNum = taosArrayGetSize(pInfo->pendingKeys);
    for (int32_t i = 0; i < keyNum; ++i) {
      SStateKeys* pKey = taosArrayGet(pInfo->pendingKeys, i);
      taosMemoryFreeClear(pKey->pData);
    }
  }
  taosArrayDestroy(pInfo->pendingKeys);
  taosMemoryFreeClear(pInfo->pendingColTouched);
  taosArrayDestroy(pInfo->stateCols);
  if (pInfo->pOperator) {
    cleanupResultInfo(pInfo->pOperator->pTaskInfo, &pInfo->pOperator->exprSupp, &pInfo->groupResInfo, &pInfo->aggSup,
                      pInfo->cleanGroupResInfo);
    pInfo->pOperator = NULL;
  }

  cleanupExprSupp(&pInfo->scalarSup);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  cleanupAggSup(&pInfo->aggSup);
  cleanupGroupResInfo(&pInfo->groupResInfo);

  taosMemoryFreeClear(param);
}

static void freeItem(void* param) {
  SGroupKeys* pKey = (SGroupKeys*)param;
  taosMemoryFree(pKey->pData);
}

void destroyIntervalOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }

  SIntervalAggOperatorInfo* pInfo = (SIntervalAggOperatorInfo*)param;

  if (pInfo->pOperator != NULL && pInfo->pOperator->pTaskInfo != NULL) {
    SIntervalTimingInfo* pTiming = &pInfo->timing;
    qInfo("%s interval timing open:%" PRId64 "/%.3fms downstream:%" PRId64 "/%.3fms "
          "set_input:%" PRId64 "/%.3fms hash:%" PRId64 "/%.3fms output:%" PRId64 "/%.3fms "
          "apply:%" PRId64 "/%.3fms seek:%" PRId64 "/%.3fms init:%" PRId64 "/%.3fms "
          "build:%" PRId64 "/%.3fms filter:%" PRId64 "/%.3fms",
          GET_TASKID(pInfo->pOperator->pTaskInfo), pTiming->openCalls, pTiming->openElapsedUs / 1000.0,
          pTiming->downstreamCalls, pTiming->downstreamElapsedUs / 1000.0, pTiming->setInputCalls,
          pTiming->setInputElapsedUs / 1000.0, pTiming->hashCalls, pTiming->hashElapsedUs / 1000.0,
          pTiming->hashOutputCalls, pTiming->hashOutputElapsedUs / 1000.0, pTiming->hashApplyCalls,
          pTiming->hashApplyElapsedUs / 1000.0, pTiming->hashSeekCalls, pTiming->hashSeekElapsedUs / 1000.0,
          pTiming->initResultCalls, pTiming->initResultElapsedUs / 1000.0, pTiming->buildCalls,
          pTiming->buildElapsedUs / 1000.0, pTiming->filterCalls, pTiming->filterElapsedUs / 1000.0);
  }

  releaseExIntervalInfo(&pInfo->pExInterval);
  cleanupBasicInfo(&pInfo->binfo);
  cleanupIndefRowsRuntime(&pInfo->indefRows, pInfo->pOperator);
  // Per-result-row state is released by cleanupIntervalRowState(), invoked from destroyOperator()
  // before cleanupExprSupp(). Doing it here would be too late: the supp is memset by then, so
  // numOfExprs is 0 and cleanupResultInfo() walks nothing -- silently leaking the DISTINCT dedup
  // set, spill pages and temp file of every row never emitted (early cancel, LIMIT short-circuit).
  pInfo->pOperator = NULL;

  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSupp);

  pInfo->binfo.resultRowInfo.openWindow = tdListFree(pInfo->binfo.resultRowInfo.openWindow);

  taosArrayDestroy(pInfo->pInterpCols);
  pInfo->pInterpCols = NULL;

  taosArrayDestroyEx(pInfo->pPrevValues, freeItem);
  pInfo->pPrevValues = NULL;

  cleanupGroupResInfo(&pInfo->groupResInfo);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  destroyBoundedQueue(pInfo->pBQ);
  taosMemoryFreeClear(param);
}

// Runs from destroyOperator() before cleanupExprSupp(), the only point where both the expression
// supp and the result rows are still valid.
static void cleanupIntervalRowState(void* param) {
  SIntervalAggOperatorInfo* pInfo = (SIntervalAggOperatorInfo*)param;
  if (pInfo == NULL || pInfo->pOperator == NULL) {
    return;
  }
  if (pInfo->aggSup.pFastInterval != NULL) {
    cleanupFastIntervalRows(pInfo);
  } else {
    cleanupResultInfo(pInfo->pOperator->pTaskInfo, &pInfo->pOperator->exprSupp, &pInfo->groupResInfo, &pInfo->aggSup,
                      pInfo->cleanGroupResInfo);
  }
}

static int32_t initWindowInterpPrevVal(SIntervalAggOperatorInfo* pInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   tmp = NULL;

  pInfo->pInterpCols = taosArrayInit(4, sizeof(SColumn));
  QUERY_CHECK_NULL(pInfo->pInterpCols, code, lino, _end, terrno);

  pInfo->pPrevValues = taosArrayInit(4, sizeof(SGroupKeys));
  QUERY_CHECK_NULL(pInfo->pPrevValues, code, lino, _end, terrno);

  {  // ts column
    SColumn c = {0};
    c.colId = 1;
    c.slotId = pInfo->primaryTsIndex;
    c.type = TSDB_DATA_TYPE_TIMESTAMP;
    c.bytes = sizeof(int64_t);
    tmp = taosArrayPush(pInfo->pInterpCols, &c);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

    SGroupKeys key;
    key.bytes = c.bytes;
    key.type = c.type;
    key.isNull = true;  // to denote no value is assigned yet
    key.pData = taosMemoryCalloc(1, c.bytes);
    QUERY_CHECK_NULL(key.pData, code, lino, _end, terrno);

    tmp = taosArrayPush(pInfo->pPrevValues, &key);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t timeWindowinterpNeeded(SqlFunctionCtx* pCtx, int32_t numOfCols, SIntervalAggOperatorInfo* pInfo,
                                      bool* pRes) {
  // the primary timestamp column
  bool    needed = false;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   tmp = NULL;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SExprInfo* pExpr = pCtx[i].pExpr;
    if (fmIsIntervalInterpoFunc(pCtx[i].functionId)) {
      needed = true;
      break;
    }
  }

  if (needed) {
    code = initWindowInterpPrevVal(pInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SExprInfo* pExpr = pCtx[i].pExpr;

    if (fmIsIntervalInterpoFunc(pCtx[i].functionId)) {
      SFunctParam* pParam = &pExpr->base.pParam[0];

      SColumn c = *pParam->pCol;
      tmp = taosArrayPush(pInfo->pInterpCols, &c);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

      SGroupKeys key = {0};
      key.bytes = c.bytes;
      key.type = c.type;
      key.isNull = false;
      key.pData = taosMemoryCalloc(1, c.bytes);
      QUERY_CHECK_NULL(key.pData, code, lino, _end, terrno);

      tmp = taosArrayPush(pInfo->pPrevValues, &key);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  *pRes = needed;
  return code;
}

static int32_t resetInterval(SOperatorInfo* pOper, SIntervalAggOperatorInfo* pIntervalInfo){
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  SIntervalPhysiNode* pPhynode = (SIntervalPhysiNode*)pOper->pPhyNode;
  bool fastInterval = (pIntervalInfo->aggSup.pFastInterval != NULL);
  bool exInterval = (pIntervalInfo->pExInterval != NULL);
  pOper->status = OP_NOT_OPENED;

  if (exInterval) {
    releaseExIntervalInfo(&pIntervalInfo->pExInterval);
  }

  resetBasicOperatorState(&pIntervalInfo->binfo);
  if (fastInterval) {
    cleanupFastIntervalRows(pIntervalInfo);
  } else {
    cleanupResultInfo(pIntervalInfo->pOperator->pTaskInfo, &pIntervalInfo->pOperator->exprSupp,
                      &pIntervalInfo->groupResInfo, &pIntervalInfo->aggSup, pIntervalInfo->cleanGroupResInfo);
  }

  colDataDestroy(&pIntervalInfo->twAggSup.timeWindowData);
  int32_t code = initExecTimeWindowInfo(&pIntervalInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  if (code == 0) {
    code = fastInterval
               ? resetFastIntervalAggSup(&pOper->exprSupp, &pIntervalInfo->aggSup, pPhynode->window.pFuncs,
                                         &pTaskInfo->storageAPI.functionStore)
               : resetAggSup(&pOper->exprSupp, &pIntervalInfo->aggSup, pTaskInfo, pPhynode->window.pFuncs, NULL,
                             sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, NULL,
                             &pTaskInfo->storageAPI.functionStore);
  }
  if (code == 0) {
    code = resetExprSupp(&pIntervalInfo->scalarSupp, pTaskInfo, pPhynode->window.pExprs, NULL,
                         &pTaskInfo->storageAPI.functionStore);
  }
  if (code == TSDB_CODE_SUCCESS && fastInterval) {
    SOperatorInfo* pDownstream = pOper->numOfDownstream > 0 ? pOper->pDownstream[0] : NULL;
    code = initFastIntervalInfo(pIntervalInfo, pDownstream);
  }
  if (code == TSDB_CODE_SUCCESS && exInterval) {
    code = initExIntervalInfo(pIntervalInfo, pOper, false);
  }

  if (pIntervalInfo->binfo.resultRowInfo.openWindow != NULL){
    tdListEmpty(pIntervalInfo->binfo.resultRowInfo.openWindow);
  }

  if (pPhynode->window.node.pSlimit && ((SLimitNode*)pPhynode->window.node.pSlimit)->limit) {
    pIntervalInfo->curGroupId = UINT64_MAX;
  }

  pIntervalInfo->cleanGroupResInfo = false;
  pIntervalInfo->handledGroupNum = 0;
  memset(&pIntervalInfo->timing, 0, sizeof(pIntervalInfo->timing));
  setOptrBasicInfoOrder(&pIntervalInfo->binfo, &pPhynode->window.node);

  taosArrayDestroy(pIntervalInfo->pInterpCols);
  pIntervalInfo->pInterpCols = NULL;

  if (pIntervalInfo->pPrevValues != NULL) {
    taosArrayDestroyEx(pIntervalInfo->pPrevValues, freeItem);
    pIntervalInfo->pPrevValues = NULL;
    code = initWindowInterpPrevVal(pIntervalInfo);
  }

  resetIndefRowsRuntime(&pIntervalInfo->indefRows, pIntervalInfo->pOperator);

  cleanupGroupResInfo(&pIntervalInfo->groupResInfo);
  destroyBoundedQueue(pIntervalInfo->pBQ);
  pIntervalInfo->pBQ = NULL;
  return code;
}

static int32_t resetIntervalOperState(SOperatorInfo* pOper) {
  SIntervalAggOperatorInfo* pInfo = pOper->info;
  return resetInterval(pOper, pInfo);
}

int32_t createIntervalOperatorInfo(SOperatorInfo* downstream, SIntervalPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                   SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SIntervalAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SIntervalAggOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    lino = __LINE__;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  pOperator->pPhyNode = pPhyNode;
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pInfo->binfo, pResBlock);

  SExprSupp* pSup = &pOperator->exprSupp;
  pSup->hasWindowOrGroup = true;
  pSup->hasWindow = true;

  pInfo->primaryTsIndex = ((SColumnNode*)pPhyNode->window.pTspk)->slotId;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 512);
  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  int32_t    num = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pPhyNode->window.pFuncs, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  code = pPhyNode->fastInterval
             ? initFastIntervalAggSup(pSup, &pInfo->aggSup, pExprInfo, num,
                                      &pTaskInfo->storageAPI.functionStore)
             : initAggSup(pSup, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str, NULL,
                          &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->indefRowsMode = pPhyNode->window.indefRowsFunc;
  if (pInfo->indefRowsMode) {
    code = initIndefRowsRuntime(&pInfo->indefRows, pOperator->exprSupp.pCtx, num, pOperator->resultInfo.capacity,
                               pPhyNode->window.pProjs, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  SInterval interval = {.interval = pPhyNode->interval,
                        .sliding = pPhyNode->sliding,
                        .intervalUnit = pPhyNode->intervalUnit,
                        .slidingUnit = pPhyNode->slidingUnit,
                        .offset = pPhyNode->offset,
                        .precision = ((SColumnNode*)pPhyNode->window.pTspk)->node.resType.precision,
                        .timeRange = pPhyNode->timeRange,
                        .firstDayOfWeek = pPhyNode->firstDayOfWeek,
                        .timezone = pPhyNode->timezone};
  calcIntervalAutoOffset(&interval);

  STimeWindowAggSupp as = {
      .maxTs = INT64_MIN,
  };

  pInfo->win = pTaskInfo->window;
  setOptrBasicInfoOrder(&pInfo->binfo, &pPhyNode->window.node);
  if (downstream != NULL && downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN) {
    pInfo->binfo.inputDataOrder = DATA_ORDER_LEVEL_NONE;
  }
  pInfo->interval = interval;
  pInfo->twAggSup = as;
  pInfo->binfo.mergeResultBlock = pPhyNode->window.mergeDataBlock;
  if (pPhyNode->window.node.pLimit && ((SLimitNode*)pPhyNode->window.node.pLimit)->limit) {
    SLimitNode* pLimit = (SLimitNode*)pPhyNode->window.node.pLimit;
    pInfo->limited = true;
    pInfo->limit = pLimit->limit->datum.i + (pLimit->offset ? pLimit->offset->datum.i : 0);
  }
  if (pPhyNode->window.node.pSlimit && ((SLimitNode*)pPhyNode->window.node.pSlimit)->limit) {
    SLimitNode* pLimit = (SLimitNode*)pPhyNode->window.node.pSlimit;
    pInfo->slimited = true;
    pInfo->slimit = pLimit->limit->datum.i + (pLimit->offset ? pLimit->offset->datum.i : 0);
    pInfo->curGroupId = UINT64_MAX;
  }

  if (pPhyNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pPhyNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalar);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  code = filterInitFromNode((SNode*)pPhyNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pInfo->win);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->timeWindowInterpo = false;
  code = timeWindowinterpNeeded(pSup->pCtx, num, pInfo, &pInfo->timeWindowInterpo);
  QUERY_CHECK_CODE(code, lino, _error);
  if (pInfo->timeWindowInterpo) {
    pInfo->binfo.resultRowInfo.openWindow = tdListNew(sizeof(SOpenWindowInfo));
    if (pInfo->binfo.resultRowInfo.openWindow == NULL) {
      goto _error;
    }
  }

  pInfo->pOperator = pOperator;
  pInfo->cleanGroupResInfo = false;
  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  setOperatorInfo(pOperator, "TimeIntervalAggOperator", QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  if (pPhyNode->fastInterval) {
    code = initFastIntervalInfo(pInfo, downstream);
    QUERY_CHECK_CODE(code, lino, _error);
  }
  if (pPhyNode->exInterval) {
    code = initExIntervalInfo(pInfo, pOperator, false);
    QUERY_CHECK_CODE(code, lino, _error);
  }
  if (downstream != NULL && downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pTableScanInfo = downstream->info;
    pTableScanInfo->base.pdInfo.interval = pInfo->interval;
    pTableScanInfo->base.pdInfo.pExprSup = &pOperator->exprSupp;
    pTableScanInfo->base.pdInfo.pAggSup = &pInfo->aggSup;
  }

  pOperator->fpSet = createOperatorFpSet(doOpenIntervalAgg, doBuildIntervalResultNext, NULL, destroyIntervalOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorRowStateCleanupFn(pOperator, cleanupIntervalRowState);
  setOperatorResetStateFn(pOperator, resetIntervalOperState);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    destroyIntervalOperatorInfo(pInfo);
  }

  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  qError("error happens at %s %d, code:%s", __func__, lino, tstrerror(code));
  return code;
}

// todo handle multiple timeline cases. assume no timeline interweaving
static void doSessionWindowAggImpl(SOperatorInfo* pOperator, SSessionAggOperatorInfo* pInfo, SSDataBlock* pBlock) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*     pSup = &pOperator->exprSupp;

  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
  if (!pColInfoData) {
    pTaskInfo->code = terrno;
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  bool    masterScan = true;
  int32_t numOfOutput = pOperator->exprSupp.numOfExprs;
  int64_t gid = pBlock->info.id.groupId;

  int64_t gap = pInfo->gap;

  if (!pInfo->reptScan) {
    pInfo->reptScan = true;
    pInfo->winSup.prevTs = INT64_MIN;
  }

  SWindowRowsSup* pRowSup = &pInfo->winSup;
  pRowSup->numOfRows = 0;
  pRowSup->startRowIndex = 0;

  // In case of ascending or descending order scan data, only one time window needs to be kepted for each table.
  TSKEY* tsList = (TSKEY*)pColInfoData->pData;
  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    if (pColInfoData->hasNull && colDataIsNull_f(pColInfoData, j)) {
      continue;
    }

    if (gid != pRowSup->groupId || pInfo->winSup.prevTs == INT64_MIN) {
      doKeepNewWindowStartInfo(pRowSup, tsList, j, gid);
      doKeepTuple(pRowSup, tsList[j], j, gid);
    } else if (tsList[j] == pRowSup->prevTs) {
      // Duplicate timestamp - keep in current window
      doKeepTuple(pRowSup, tsList[j], j, gid);
    } else {
      int64_t absDiff = (tsList[j] > pRowSup->prevTs) ? (tsList[j] - pRowSup->prevTs) : (pRowSup->prevTs - tsList[j]);
      if (absDiff <= gap) {
        // Gap within threshold - belongs to current session window
        doKeepTuple(pRowSup, tsList[j], j, gid);
      } else {  // start a new session window
        if (pRowSup->numOfRows > 0) {  // handled data that belongs to the previous session window
          // keep the time window for the closed time window.
          STimeWindow window = pRowSup->win;

          int32_t ret = TSDB_CODE_SUCCESS;
          if (pInfo->indefRowsMode) {
            ret = applyIndefRowsWindowSegment(pOperator, &pInfo->indefRows, pInfo->binfo.pRes,
                                              pInfo->aggSup.resultRowSize, gid, &window, pBlock,
                                              pRowSup->startRowIndex, pRowSup->numOfRows, pInfo->binfo.inputTsOrder,
                                              true);
          } else {
            SResultRow* pResult = NULL;
            ret =
                setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &window, masterScan, &pResult, gid, pSup->pCtx,
                                       numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
            if (ret == TSDB_CODE_SUCCESS) {
              // pInfo->numOfRows data belong to the current session window
              updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &window, 0);
              ret = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData,
                                                    pRowSup->startRowIndex, pRowSup->numOfRows, pBlock->info.rows,
                                                    numOfOutput);
            }
          }
          if (ret != TSDB_CODE_SUCCESS) {
            T_LONG_JMP(pTaskInfo->env, ret);
          }
        }

        // here we start a new session window
        doKeepNewWindowStartInfo(pRowSup, tsList, j, gid);
        doKeepTuple(pRowSup, tsList[j], j, gid);
      }
    }
  }

  pRowSup->win.ekey = tsList[pBlock->info.rows - 1];
  int32_t ret = TSDB_CODE_SUCCESS;
  if (pInfo->indefRowsMode) {
    ret = applyIndefRowsWindowSegment(pOperator, &pInfo->indefRows, pInfo->binfo.pRes, pInfo->aggSup.resultRowSize,
                                      gid, &pRowSup->win, pBlock, pRowSup->startRowIndex, pRowSup->numOfRows,
                                      pInfo->binfo.inputTsOrder, false);
  } else {
    SResultRow* pResult = NULL;
    ret = setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &pRowSup->win, masterScan, &pResult, gid,
                                 pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (ret == TSDB_CODE_SUCCESS) {
      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, 0);
      ret = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                                            pRowSup->numOfRows, pBlock->info.rows, numOfOutput);
    }
  }
  if (ret != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, ret);
  }
}

static int32_t doSessionWindowAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*          pBInfo = &pInfo->binfo;
  SExprSupp*               pSup = &pOperator->exprSupp;

  if (pOperator->status == OP_RES_TO_RETURN) {
    if (pInfo->indefRowsMode) {
      (*ppRes) = getNextIndefRowsResultBlock(&pInfo->indefRows, pOperator);
      if ((*ppRes) == NULL) {
        setOperatorCompleted(pOperator);
      }
      return code;
    }

    while (1) {
      doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
      code = doFilterWithTaskCorrelation(pBInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL,
                                         pOperator->pTaskInfo);
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
    (*ppRes) = (pBInfo->pRes->info.rows == 0) ? NULL : pBInfo->pRes;
    return code;
  }

  int32_t order = pInfo->binfo.inputTsOrder;

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  pInfo->cleanGroupResInfo = false;
  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    pBInfo->pRes->info.scanFlag = pBlock->info.scanFlag;
    pBInfo->pRes->info.dataLoad = 1;
    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL,
                                   GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    // the pDataBlock are always the same one, no need to call this again
    code = setInputDataBlock(pSup, pBlock, order, pBlock->info.scanFlag, true);
    QUERY_CHECK_CODE(code, lino, _end);

    code = blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);
    QUERY_CHECK_CODE(code, lino, _end);

    doSessionWindowAggImpl(pOperator, pInfo, pBlock);
  }

  if (pInfo->indefRowsMode) {
    code = closeAllIndefRowsWindowStates(pOperator, &pInfo->indefRows);
    QUERY_CHECK_CODE(code, lino, _end);
    pOperator->status = OP_RES_TO_RETURN;
    (*ppRes) = getNextIndefRowsResultBlock(&pInfo->indefRows, pOperator);
    if ((*ppRes) == NULL) {
      setOperatorCompleted(pOperator);
    }
    return code;
  }

  // restore the value
  pOperator->status = OP_RES_TO_RETURN;

  code = initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, TSDB_ORDER_ASC);
  QUERY_CHECK_CODE(code, lino, _end);
  pInfo->cleanGroupResInfo = true;

  code = blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);
  while (1) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    code = doFilterWithTaskCorrelation(pBInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL,
                                       pOperator->pTaskInfo);
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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) = (pBInfo->pRes->info.rows == 0) ? NULL : pBInfo->pRes;
  return code;
}

static int32_t resetStatewindowOperState(SOperatorInfo* pOper) {
  SStateWindowOperatorInfo* pInfo = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  SStateWindowPhysiNode* pPhynode = (SStateWindowPhysiNode*)pOper->pPhyNode;
  pOper->status = OP_NOT_OPENED;

  resetBasicOperatorState(&pInfo->binfo);
  cleanupResultInfo(pInfo->pOperator->pTaskInfo, &pInfo->pOperator->exprSupp, &pInfo->groupResInfo, &pInfo->aggSup,
                    pInfo->cleanGroupResInfo);

  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  int32_t code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  if (code == 0) {
    code = resetAggSup(&pOper->exprSupp, &pInfo->aggSup, pTaskInfo, pPhynode->window.pFuncs, NULL,
                       sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, NULL,
                       &pTaskInfo->storageAPI.functionStore);
  }
  if (code == 0) {
    code = resetExprSupp(&pInfo->scalarSup, pTaskInfo, pPhynode->window.pExprs, NULL,
                         &pTaskInfo->storageAPI.functionStore);
  }

  pInfo->cleanGroupResInfo = false;
  pInfo->hasKey = false;
  pInfo->winSup.lastTs = INT64_MIN;
  resetIndefRowsRuntime(&pInfo->indefRows, pInfo->pOperator);
  cleanupGroupResInfo(&pInfo->groupResInfo);
  int32_t keyNum = taosArrayGetSize(pInfo->stateKeys);
  for (int32_t i = 0; i < keyNum; ++i) {
    SStateKeys* pKey = taosArrayGet(pInfo->stateKeys, i);
    memset(pKey->pData, 0, pKey->bytes);
    pKey->isNull = true;
    SStateKeys* pPend = taosArrayGet(pInfo->pendingKeys, i);
    if (pPend != NULL) {
      memset(pPend->pData, 0, pPend->bytes);
      pPend->isNull = true;
    }
  }
  resetPendingState(pInfo);
  return code;
}

// todo make this as an non-blocking operator
int32_t createStatewindowOperatorInfo(SOperatorInfo* downstream, SStateWindowPhysiNode* pStateNode,
                                      SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SStateWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStateWindowOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  pOperator->pPhyNode = pStateNode;
  pOperator->exprSupp.hasWindowOrGroup = true;
  pOperator->exprSupp.hasWindow = true;
  int32_t      tsSlotId = ((SColumnNode*)pStateNode->window.pTspk)->slotId;

  if (pStateNode->window.pExprs != NULL) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pStateNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalarExpr);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  int32_t keyNum = LIST_LENGTH(pStateNode->pStateKeys);
  pInfo->stateCols = taosArrayInit(keyNum, sizeof(SColumn));
  pInfo->stateKeys = taosArrayInit(keyNum, sizeof(SStateKeys));
  pInfo->pendingKeys = taosArrayInit(keyNum, sizeof(SStateKeys));
  pInfo->pendingColTouched = taosMemoryCalloc(keyNum, sizeof(bool));
  pInfo->hasPendingPartialNull = false;
  if (pInfo->stateCols == NULL || pInfo->stateKeys == NULL
      || pInfo->pendingKeys == NULL || pInfo->pendingColTouched == NULL) {
    goto _error;
  }
  for (int32_t i = 0; i < keyNum; ++i) {
    SColumnNode* pColNode = (SColumnNode*)nodesListGetNode(pStateNode->pStateKeys, i);
    SColumn      stateCol = extractColumnFromColumnNode(pColNode);
    SStateKeys   stateKey = {.type = stateCol.type, .bytes = stateCol.bytes, .isNull = true, .pData = taosMemoryCalloc(1, stateCol.bytes)};
    SStateKeys   pendKey  = {.type = stateCol.type, .bytes = stateCol.bytes, .isNull = true, .pData = taosMemoryCalloc(1, stateCol.bytes)};
    if (stateKey.pData == NULL || pendKey.pData == NULL
        || taosArrayPush(pInfo->stateCols, &stateCol) == NULL
        || taosArrayPush(pInfo->stateKeys, &stateKey) == NULL
        || taosArrayPush(pInfo->pendingKeys, &pendKey) == NULL) {
      taosMemoryFreeClear(stateKey.pData);
      taosMemoryFreeClear(pendKey.pData);
      goto _error;
    }
  }
  setOptrBasicInfoOrder(&pInfo->binfo, &pStateNode->window.node);

  code = filterInitFromNode((SNode*)pStateNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  int32_t    num = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pStateNode->window.pFuncs, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                    NULL, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->indefRowsMode = pStateNode->window.indefRowsFunc;
  if (pInfo->indefRowsMode) {
    code = initIndefRowsRuntime(&pInfo->indefRows, pOperator->exprSupp.pCtx, num, pOperator->resultInfo.capacity,
                               pStateNode->window.pProjs, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pStateNode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pInfo->binfo, pResBlock);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->tsSlotId = tsSlotId;
  pInfo->pOperator = pOperator;
  pInfo->cleanGroupResInfo = false;
  pInfo->extendOption = pStateNode->extendOption;
  pInfo->trueForInfo.trueForType = pStateNode->trueForType;
  pInfo->trueForInfo.count = pStateNode->trueForCount;
  pInfo->trueForInfo.duration = pStateNode->trueForDuration;
  pInfo->winSup.lastTs = INT64_MIN;

  setOperatorInfo(pOperator, "StateWindowOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE, true, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(openStateWindowAggOptr, doStateWindowAggNext, NULL, destroyStateWindowOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetStatewindowOperState);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    destroyStateWindowOperatorInfo(pInfo);
  }

  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

void destroySWindowOperatorInfo(void* param) {
  SSessionAggOperatorInfo* pInfo = (SSessionAggOperatorInfo*)param;
  if (pInfo == NULL) {
    return;
  }

  cleanupBasicInfo(&pInfo->binfo);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  cleanupIndefRowsRuntime(&pInfo->indefRows, pInfo->pOperator);
  if (pInfo->pOperator) {
    cleanupResultInfo(pInfo->pOperator->pTaskInfo, &pInfo->pOperator->exprSupp, &pInfo->groupResInfo, &pInfo->aggSup,
                      pInfo->cleanGroupResInfo);
    pInfo->pOperator = NULL;
  }

  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSupp);

  cleanupGroupResInfo(&pInfo->groupResInfo);
  taosMemoryFreeClear(param);
}

static int32_t resetSessionAggOperState(SOperatorInfo* pOper) {
  SSessionAggOperatorInfo* pInfo = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  SSessionWinodwPhysiNode* pPhynode = (SSessionWinodwPhysiNode*)pOper->pPhyNode;
  pOper->status = OP_NOT_OPENED;

  resetBasicOperatorState(&pInfo->binfo);
  cleanupResultInfo(pInfo->pOperator->pTaskInfo, &pInfo->pOperator->exprSupp, &pInfo->groupResInfo, &pInfo->aggSup,
                    pInfo->cleanGroupResInfo);

  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  int32_t code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  if (code == 0) {
    code = resetAggSup(&pOper->exprSupp, &pInfo->aggSup, pTaskInfo, pPhynode->window.pFuncs, NULL,
                       sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, NULL,
                       &pTaskInfo->storageAPI.functionStore);
  }
  if (code == 0) {
    code = resetExprSupp(&pInfo->scalarSupp, pTaskInfo, pPhynode->window.pExprs, NULL,
                         &pTaskInfo->storageAPI.functionStore);
  }

  pInfo->cleanGroupResInfo = false;
  pInfo->winSup = (SWindowRowsSup){0};
  pInfo->winSup.prevTs = INT64_MIN;
  pInfo->reptScan = false;
  resetIndefRowsRuntime(&pInfo->indefRows, pInfo->pOperator);

  cleanupGroupResInfo(&pInfo->groupResInfo);
  return code;
}

int32_t createSessionAggOperatorInfo(SOperatorInfo* downstream, SSessionWinodwPhysiNode* pSessionNode,
                                     SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SSessionAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SSessionAggOperatorInfo));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  pOperator->pPhyNode = pSessionNode;
  pOperator->exprSupp.hasWindowOrGroup = true;
  pOperator->exprSupp.hasWindow = true;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pSessionNode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pInfo->binfo, pResBlock);

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pSessionNode->window.pFuncs, NULL, &pExprInfo, &numOfCols);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str,
                    NULL, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->indefRowsMode = pSessionNode->window.indefRowsFunc;
  if (pInfo->indefRowsMode) {
    code = initIndefRowsRuntime(&pInfo->indefRows, pOperator->exprSupp.pCtx, numOfCols, pOperator->resultInfo.capacity,
                               pSessionNode->window.pProjs, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  pInfo->gap = pSessionNode->gap;

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->tsSlotId = ((SColumnNode*)pSessionNode->window.pTspk)->slotId;
  pInfo->binfo.pRes = pResBlock;
  pInfo->winSup.prevTs = INT64_MIN;
  pInfo->reptScan = false;
  setOptrBasicInfoOrder(&pInfo->binfo, &pSessionNode->window.node);

  if (pSessionNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pSessionNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalar);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pSessionNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->pOperator = pOperator;
  pInfo->cleanGroupResInfo = false;
  setOperatorInfo(pOperator, "SessionWindowAggOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doSessionWindowAggNext, NULL, destroySWindowOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  pOperator->pTaskInfo = pTaskInfo;
  setOperatorResetStateFn(pOperator, resetSessionAggOperState);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) destroySWindowOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

static int32_t setSingleOutputTupleBuf(SResultRowInfo* pResultRowInfo, STimeWindow* win, SResultRow** pResult,
                                       SExprSupp* pExprSup, SAggSupporter* pAggSup);

static int32_t initFastMergeIntervalInfo(SMergeAlignedIntervalAggOperatorInfo* pMiaInfo,
                                         const SMergeAlignedIntervalPhysiNode* pNode) {
  if (pMiaInfo == NULL || pNode == NULL || pMiaInfo->intervalAggOperatorInfo == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SPhysiNode* pChild = (SPhysiNode*)nodesListGetNode(pNode->window.node.pChildren, 0);
  if (pChild == NULL || pChild->type != QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
    qError("FAST_INTERVAL requires one root exchange child");
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SExchangePhysiNode* pExchange = (SExchangePhysiNode*)pChild;
  int32_t             numOfSources = LIST_LENGTH(pExchange->pSrcEndPoints);
  if (numOfSources <= 0 || pExchange->seqRecvData || !pExchange->fastInterval ||
      pExchange->node.pOutputDataBlockDesc == NULL) {
    qError("FAST_INTERVAL requires a non-sequential exchange with source endpoints");
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SIntervalAggOperatorInfo* pIaInfo = pMiaInfo->intervalAggOperatorInfo;
  SInterval*                pInterval = &pIaInfo->interval;
  SResultRowInfo            dummy = {0};
  dummy.cur.pageId = -1;
  STimeWindow first = getActiveTimeWindow(NULL, &dummy, pInterval->timeRange.skey, pInterval, TSDB_ORDER_ASC);
  STimeWindow last = getActiveTimeWindow(NULL, &dummy, pInterval->timeRange.ekey, pInterval, TSDB_ORDER_ASC);
  if (pInterval->interval <= 0 || last.skey < first.skey) {
    qError("FAST_INTERVAL invalid interval range, interval:%" PRId64 ", first:%" PRId64 ", last:%" PRId64,
           pInterval->interval, first.skey, last.skey);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int64_t delta = last.skey - first.skey;
  if (delta % pInterval->interval != 0) {
    qError("FAST_INTERVAL unaligned interval range, delta:%" PRId64 ", interval:%" PRId64, delta,
           pInterval->interval);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int64_t numOfWindows = delta / pInterval->interval + 1;
  if (numOfWindows <= 0 || numOfWindows > EX_INTERVAL_MAX_WINDOWS || numOfWindows > INT32_MAX / numOfSources) {
    qError("FAST_INTERVAL invalid cache shape, windows:%" PRId64 ", sources:%d", numOfWindows, numOfSources);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t                   code = TSDB_CODE_SUCCESS;
  SFastMergeIntervalInfo* pFast = taosMemoryCalloc(1, sizeof(SFastMergeIntervalInfo));
  if (pFast == NULL) {
    return terrno;
  }

  pFast->pCachedRows = createDataBlockFromDescNode(pExchange->node.pOutputDataBlockDesc);
  if (pFast->pCachedRows == NULL) {
    code = terrno;
    goto _error;
  }

  int32_t totalRows = (int32_t)(numOfWindows * numOfSources);
  code = blockDataEnsureCapacity(pFast->pCachedRows, totalRows);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pFast->pWindowRowCounts = taosMemoryCalloc((size_t)numOfWindows, sizeof(int32_t));
  pFast->activeBitmapWords = (int32_t)(((uint64_t)numOfWindows + 63) / 64);
  pFast->pActiveBitmap = taosMemoryCalloc((size_t)pFast->activeBitmapWords, sizeof(uint64_t));
  code = initFastWatermarkHeap(&pFast->sourceProgress, numOfSources);
  if (pFast->pWindowRowCounts == NULL || pFast->pActiveBitmap == NULL || code != TSDB_CODE_SUCCESS) {
    if (code == TSDB_CODE_SUCCESS) {
      code = terrno;
    }
    goto _error;
  }

  pFast->pResultRow = taosMemoryCalloc(1, pIaInfo->aggSup.resultRowSize);
  if (pFast->pResultRow == NULL) {
    code = terrno;
    goto _error;
  }

  pFast->firstWindowStart = first.skey;
  pFast->interval = pInterval->interval;
  pFast->numOfWindows = (int32_t)numOfWindows;
  pFast->numOfSources = numOfSources;
  pFast->minActiveWindow = pFast->numOfWindows;
  pFast->maxActiveWindow = -1;
  pFast->resultRowCapacity = pIaInfo->pOperator->resultInfo.threshold;
  pFast->pResultRows = taosMemoryCalloc((size_t)pFast->resultRowCapacity, pIaInfo->aggSup.resultRowSize);
  pFast->pActiveWindows = taosMemoryCalloc((size_t)pFast->resultRowCapacity, sizeof(int32_t));
  if (pFast->pResultRows == NULL || pFast->pActiveWindows == NULL) {
    code = terrno;
    goto _error;
  }
  pFast->batchProcess = true;
  for (int32_t i = 0; i < pIaInfo->pOperator->exprSupp.numOfExprs; ++i) {
    SqlFunctionCtx* pCtx = &pIaInfo->pOperator->exprSupp.pCtx[i];
    if (pCtx->functionId != -1 &&
        (pCtx->isPseudoFunc || fmIsPlaceHolderFunc(pCtx->functionId) || pCtx->fpSet.process == NULL)) {
      pFast->batchProcess = false;
      break;
    }
  }
  pMiaInfo->pFastInterval = pFast;
  return TSDB_CODE_SUCCESS;

_error:
  blockDataDestroy(pFast->pCachedRows);
  taosMemoryFreeClear(pFast->pWindowRowCounts);
  taosMemoryFreeClear(pFast->pActiveBitmap);
  cleanupFastWatermarkHeap(&pFast->sourceProgress);
  taosMemoryFreeClear(pFast->pResultRow);
  taosMemoryFreeClear(pFast->pResultRows);
  taosMemoryFreeClear(pFast->pActiveWindows);
  taosMemoryFree(pFast);
  return code;
}

static int32_t processFastPartialFunction(SExecTaskInfo* pTaskInfo, SqlFunctionCtx* pCtx, SResultRow* pResultRow,
                                          int32_t exprIndex, int32_t offset,
                                          int32_t numOfRows, int32_t numOfTotal, const int32_t* rowEntryInfoOffset) {
  bool    savedHasAgg = pCtx->input.colDataSMAIsSet;
  int32_t savedNumOfRows = pCtx->input.numOfRows;
  int32_t savedStartOffset = pCtx->input.startRowIndex;
  pCtx->resultInfo = getResultEntryInfo(pResultRow, exprIndex, rowEntryInfoOffset);
  pCtx->input.startRowIndex = offset;
  pCtx->input.numOfRows = numOfRows;
  if (pCtx->input.colDataSMAIsSet && numOfRows < numOfTotal) {
    pCtx->input.colDataSMAIsSet = false;
  }

  if (pCtx->functionId == -1) {
    SColumnInfoData* pColInfoData = pCtx->input.pData[0];
    int32_t rowIndex = offset < numOfTotal ? offset : 0;
    if (pColInfoData == NULL || rowIndex < 0 || rowIndex >= numOfTotal) {
      pCtx->resultInfo->isNullRes = 1;
    } else if (colDataIsNull(pColInfoData, numOfTotal, rowIndex, NULL)) {
      pCtx->resultInfo->isNullRes = 1;
    } else {
      char* dest = GET_ROWCELL_INTERBUF(pCtx->resultInfo);
      char* data = colDataGetData(pColInfoData, rowIndex);
      if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
        // The interbuf cell is sized by the expression's declared result width; a
        // virtual table column may carry a value wider than declared (the full source
        // value is returned by design) -- refuse to overrun it.
        int32_t code = copyVarLenCellChecked(dest, pColInfoData->info.type, data,
                                             (int32_t)pCtx->resDataInfo.interBufSize, NULL);
        if (TSDB_CODE_SUCCESS != code) {
          // Restore the saved input state like every other return path in this function.
          pCtx->input.colDataSMAIsSet = savedHasAgg;
          pCtx->input.numOfRows = savedNumOfRows;
          pCtx->input.startRowIndex = savedStartOffset;
          return code;
        }
      } else {
        memcpy(dest, data, pColInfoData->info.bytes);
      }
      pCtx->resultInfo->isNullRes = 0;
    }
    pCtx->resultInfo->numOfRes = 1;
    pCtx->input.colDataSMAIsSet = savedHasAgg;
    pCtx->input.numOfRows = savedNumOfRows;
    pCtx->input.startRowIndex = savedStartOffset;
    return TSDB_CODE_SUCCESS;
  }

  if (functionNeedToExecute(pCtx) && pCtx->input.pData[0] == NULL) {
    qError("%s FAST_INTERVAL batch aggregate input is NULL, expr:%d", GET_TASKID(pTaskInfo), exprIndex);
    pCtx->input.colDataSMAIsSet = savedHasAgg;
    pCtx->input.numOfRows = savedNumOfRows;
    pCtx->input.startRowIndex = savedStartOffset;
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  if (functionNeedToExecute(pCtx)) {
    int32_t code = pCtx->fpSet.process(pCtx);
    if (code != TSDB_CODE_SUCCESS) {
      if (pCtx->fpSet.cleanup != NULL) {
        pCtx->fpSet.cleanup(pCtx);
      }
      pCtx->input.colDataSMAIsSet = savedHasAgg;
      pCtx->input.numOfRows = savedNumOfRows;
      pCtx->input.startRowIndex = savedStartOffset;
      return code;
    }
  }
  pCtx->input.colDataSMAIsSet = savedHasAgg;
  pCtx->input.numOfRows = savedNumOfRows;
  pCtx->input.startRowIndex = savedStartOffset;
  return TSDB_CODE_SUCCESS;
}

static int32_t fastMergeIntervalNextActiveWindow(const SFastMergeIntervalInfo* pFast, int32_t start);

static int32_t buildFastMergeIntervalBatch(SOperatorInfo* pOperator, SIntervalAggOperatorInfo* pIaInfo,
                                            SFastMergeIntervalInfo* pFast) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SExprSupp*     pSup = &pOperator->exprSupp;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*   pRes = pIaInfo->binfo.pRes;
  int32_t startWindow = pFast->nextWindow;
  int32_t endWindow = TMIN(pFast->sealedWindowExclusive, startWindow + pOperator->resultInfo.threshold);
  int32_t numOfRows = endWindow - startWindow;
  if (numOfRows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SResultRow* pRows = pFast->pResultRows;
  int32_t     initializedRows = 0;
  if (pRows == NULL || numOfRows > pFast->resultRowCapacity) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  memset(pRows, 0, (size_t)numOfRows * pIaInfo->aggSup.resultRowSize);

  for (int32_t i = 0; i < numOfRows; ++i) {
    SResultRow* pRow = (SResultRow*)((char*)pRows + (size_t)i * pIaInfo->aggSup.resultRowSize);
    pRow->win.skey = pFast->firstWindowStart + (int64_t)(startWindow + i) * pFast->interval;
    pRow->win.ekey = pRow->win.skey + pFast->interval - 1;
    code = setResultRowInitCtx(pRow, pSup->pCtx, pSup->numOfExprs, pSup->rowEntryInfoOffset);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
    initializedRows += 1;
  }

  for (int32_t expr = 0; expr < pSup->numOfExprs; ++expr) {
    SqlFunctionCtx* pCtx = &pSup->pCtx[expr];
    for (int32_t i = 0; i < numOfRows; ++i) {
      int32_t windowIndex = startWindow + i;
      int32_t partialRows = pFast->pWindowRowCounts[windowIndex];
      if (partialRows == 0) {
        continue;
      }
      SResultRow* pRow = (SResultRow*)((char*)pRows + (size_t)i * pIaInfo->aggSup.resultRowSize);
      code = processFastPartialFunction(pTaskInfo, pCtx, pRow, expr, windowIndex * pFast->numOfSources, partialRows,
                                         pFast->pCachedRows->info.rows, pSup->rowEntryInfoOffset);
      if (code != TSDB_CODE_SUCCESS) {
        goto _cleanup;
      }
    }
  }

  for (int32_t i = 0; i < numOfRows; ++i) {
    int32_t windowIndex = startWindow + i;
    if (pFast->pWindowRowCounts[windowIndex] == 0) {
      continue;
    }
    SResultRow* pRow = (SResultRow*)((char*)pRows + (size_t)i * pIaInfo->aggSup.resultRowSize);
    doUpdateNumOfRows(pSup->pCtx, pRow, pSup->numOfExprs, pSup->rowEntryInfoOffset);
    if (pRow->numOfRows == 0) {
      continue;
    }
    updateTimeWindowInfo(&pIaInfo->twAggSup.timeWindowData, &pRow->win, 0);
    code = copyResultrowToDataBlock(pSup->pExprInfo, pSup->numOfExprs, pRow, pSup->pCtx, pRes,
                                    pSup->rowEntryInfoOffset, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
    pRes->info.rows += pRow->numOfRows;
  }
  pFast->nextWindow = endWindow;

_cleanup:
  for (int32_t i = 0; i < initializedRows; ++i) {
    SResultRow* pRow = (SResultRow*)((char*)pRows + (size_t)i * pIaInfo->aggSup.resultRowSize);
    cleanupFastMergeResultRow(pRow, pSup, pSup->rowEntryInfoOffset);
  }
  return code;
}

static int32_t buildFastMergeIntervalSparseBatch(SOperatorInfo* pOperator, SIntervalAggOperatorInfo* pIaInfo,
                                                  SFastMergeIntervalInfo* pFast) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SExprSupp*     pSup = &pOperator->exprSupp;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*   pRes = pIaInfo->binfo.pRes;
  SResultRow*    pRows = pFast->pResultRows;
  int32_t*       pWindows = pFast->pActiveWindows;
  int32_t        numOfRows = 0;
  int32_t        initializedRows = 0;

  if (NULL == pRows || NULL == pWindows || pFast->resultRowCapacity <= 0) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t nextWindow = pFast->nextWindow;
  while (numOfRows < pFast->resultRowCapacity) {
    int32_t windowIndex = fastMergeIntervalNextActiveWindow(pFast, nextWindow);
    if (windowIndex >= pFast->sealedWindowExclusive) {
      pFast->nextWindow = pFast->sealedWindowExclusive;
      break;
    }
    pWindows[numOfRows++] = windowIndex;
    nextWindow = windowIndex + 1;
    pFast->nextWindow = nextWindow;
  }
  if (0 == numOfRows) return TSDB_CODE_SUCCESS;

  TAOS_MEMSET(pRows, 0, (size_t)numOfRows * pIaInfo->aggSup.resultRowSize);
  for (int32_t i = 0; i < numOfRows; ++i) {
    int32_t windowIndex = pWindows[i];
    SResultRow* pRow = (SResultRow*)((char*)pRows + (size_t)i * pIaInfo->aggSup.resultRowSize);
    pRow->win.skey = pFast->firstWindowStart + (int64_t)windowIndex * pFast->interval;
    pRow->win.ekey = pRow->win.skey + pFast->interval - 1;
    code = setResultRowInitCtx(pRow, pSup->pCtx, pSup->numOfExprs, pSup->rowEntryInfoOffset);
    if (TSDB_CODE_SUCCESS != code) goto _cleanup;
    ++initializedRows;
  }

  for (int32_t expr = 0; expr < pSup->numOfExprs; ++expr) {
    SqlFunctionCtx* pCtx = &pSup->pCtx[expr];
    for (int32_t i = 0; i < numOfRows; ++i) {
      int32_t windowIndex = pWindows[i];
      int32_t partialRows = pFast->pWindowRowCounts[windowIndex];
      SResultRow* pRow = (SResultRow*)((char*)pRows + (size_t)i * pIaInfo->aggSup.resultRowSize);
      code = processFastPartialFunction(pTaskInfo, pCtx, pRow, expr, windowIndex * pFast->numOfSources, partialRows,
                                        pFast->pCachedRows->info.rows, pSup->rowEntryInfoOffset);
      if (TSDB_CODE_SUCCESS != code) goto _cleanup;
    }
  }

  for (int32_t i = 0; i < numOfRows; ++i) {
    SResultRow* pRow = (SResultRow*)((char*)pRows + (size_t)i * pIaInfo->aggSup.resultRowSize);
    doUpdateNumOfRows(pSup->pCtx, pRow, pSup->numOfExprs, pSup->rowEntryInfoOffset);
    if (0 == pRow->numOfRows) continue;
    updateTimeWindowInfo(&pIaInfo->twAggSup.timeWindowData, &pRow->win, 0);
    code = copyResultrowToDataBlock(pSup->pExprInfo, pSup->numOfExprs, pRow, pSup->pCtx, pRes,
                                    pSup->rowEntryInfoOffset, pTaskInfo);
    if (TSDB_CODE_SUCCESS != code) goto _cleanup;
    pRes->info.rows += pRow->numOfRows;
  }

_cleanup:
  for (int32_t i = 0; i < initializedRows; ++i) {
    SResultRow* pRow = (SResultRow*)((char*)pRows + (size_t)i * pIaInfo->aggSup.resultRowSize);
    cleanupFastMergeResultRow(pRow, pSup, pSup->rowEntryInfoOffset);
  }
  return code;
}

static int32_t buildFastMergeIntervalSparseRows(SOperatorInfo* pOperator, SIntervalAggOperatorInfo* pIaInfo,
                                                 SFastMergeIntervalInfo* pFast) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SExprSupp*     pSup = &pOperator->exprSupp;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*   pRes = pIaInfo->binfo.pRes;
  SResultRow*    pRows = pFast->pResultRows;
  int32_t*       pWindows = pFast->pActiveWindows;
  int32_t        numOfRows = 0;
  int32_t        initializedRows = 0;

  if (NULL == pRows || NULL == pWindows || pFast->resultRowCapacity <= 0) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t nextWindow = pFast->nextWindow;
  while (numOfRows < pFast->resultRowCapacity) {
    int32_t windowIndex = fastMergeIntervalNextActiveWindow(pFast, nextWindow);
    if (windowIndex >= pFast->sealedWindowExclusive) {
      pFast->nextWindow = pFast->sealedWindowExclusive;
      break;
    }
    pWindows[numOfRows++] = windowIndex;
    nextWindow = windowIndex + 1;
    pFast->nextWindow = nextWindow;
  }
  if (0 == numOfRows) return TSDB_CODE_SUCCESS;

  TAOS_MEMSET(pRows, 0, (size_t)numOfRows * pIaInfo->aggSup.resultRowSize);
  for (int32_t i = 0; i < numOfRows; ++i) {
    int32_t windowIndex = pWindows[i];
    SResultRow* pRow = (SResultRow*)((char*)pRows + (size_t)i * pIaInfo->aggSup.resultRowSize);
    STimeWindow win = {.skey = pFast->firstWindowStart + (int64_t)windowIndex * pFast->interval,
                       .ekey = pFast->firstWindowStart + (int64_t)(windowIndex + 1) * pFast->interval - 1};
    pRow->win = win;
    code = setResultRowInitCtx(pRow, pSup->pCtx, pSup->numOfExprs, pSup->rowEntryInfoOffset);
    if (TSDB_CODE_SUCCESS != code) goto _cleanup;
    ++initializedRows;

    updateTimeWindowInfo(&pIaInfo->twAggSup.timeWindowData, &win, 1);
    code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pIaInfo->twAggSup.timeWindowData,
                                           windowIndex * pFast->numOfSources, pFast->pWindowRowCounts[windowIndex],
                                           pFast->pCachedRows->info.rows, pSup->numOfExprs);
    if (TSDB_CODE_SUCCESS != code) goto _cleanup;
  }

  for (int32_t i = 0; i < numOfRows; ++i) {
    SResultRow* pRow = (SResultRow*)((char*)pRows + (size_t)i * pIaInfo->aggSup.resultRowSize);
    doUpdateNumOfRows(pSup->pCtx, pRow, pSup->numOfExprs, pSup->rowEntryInfoOffset);
    if (0 == pRow->numOfRows) continue;
    updateTimeWindowInfo(&pIaInfo->twAggSup.timeWindowData, &pRow->win, 0);
    code = copyResultrowToDataBlock(pSup->pExprInfo, pSup->numOfExprs, pRow, pSup->pCtx, pRes,
                                    pSup->rowEntryInfoOffset, pTaskInfo);
    if (TSDB_CODE_SUCCESS != code) goto _cleanup;
    pRes->info.rows += pRow->numOfRows;
  }

_cleanup:
  for (int32_t i = 0; i < initializedRows; ++i) {
    SResultRow* pRow = (SResultRow*)((char*)pRows + (size_t)i * pIaInfo->aggSup.resultRowSize);
    cleanupFastMergeResultRow(pRow, pSup, pSup->rowEntryInfoOffset);
  }
  return code;
}

static int32_t cacheFastMergeIntervalRow(SFastMergeIntervalInfo* pFast, const SSDataBlock* pSource,
                                         int32_t sourceRow, int32_t targetRow) {
  int32_t sourceCols = taosArrayGetSize(pSource->pDataBlock);
  int32_t targetCols = taosArrayGetSize(pFast->pCachedRows->pDataBlock);
  if (sourceCols != targetCols || sourceRow < 0 || sourceRow >= pSource->info.rows || targetRow < 0 ||
      targetRow >= pFast->numOfWindows * pFast->numOfSources) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  for (int32_t i = 0; i < sourceCols; ++i) {
    SColumnInfoData* pSourceCol = taosArrayGet(pSource->pDataBlock, i);
    SColumnInfoData* pTargetCol = taosArrayGet(pFast->pCachedRows->pDataBlock, i);
    if (pSourceCol == NULL || pTargetCol == NULL || pSourceCol->info.type != pTargetCol->info.type ||
        pSourceCol->info.bytes != pTargetCol->info.bytes) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    bool isNull = colDataIsNull(pSourceCol, pSource->info.rows, sourceRow, NULL);
    int32_t code = colDataSetVal(pTargetCol, targetRow, isNull ? NULL : colDataGetData(pSourceCol, sourceRow), isNull);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t collectFastMergeIntervalRows(SOperatorInfo* pOperator, bool* pWatermarkAdvanced) {
  SMergeAlignedIntervalAggOperatorInfo* pMiaInfo = pOperator->info;
  SIntervalAggOperatorInfo*             pIaInfo = pMiaInfo->intervalAggOperatorInfo;
  SFastMergeIntervalInfo*               pFast = pMiaInfo->pFastInterval;
  if (pFast == NULL || pWatermarkAdvanced == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  *pWatermarkAdvanced = false;

  SSDataBlock* pBlock = getNextBlockFromDownstreamRemainDetach(pOperator, 0);
  if (pBlock == NULL) {
    pFast->inputCompleted = true;
    *pWatermarkAdvanced = pFast->sealedWindowExclusive < pFast->numOfWindows;
    pFast->sealedWindowExclusive = pFast->numOfWindows;
    return TSDB_CODE_SUCCESS;
  }

  SColumnInfoData* pWStartCol = taosArrayGet(pBlock->pDataBlock, pIaInfo->primaryTsIndex);
  if (pWStartCol == NULL || pWStartCol->pData == NULL || pWStartCol->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
    qError("FAST_INTERVAL received a block without _wstart at slot:%d", pIaInfo->primaryTsIndex);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  if (pBlock->info.id.uid > INT32_MAX) {
    qError("FAST_INTERVAL received an invalid source index:%" PRIu64, pBlock->info.id.uid);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  int32_t sourceSlot = (int32_t)pBlock->info.id.uid;
  if (sourceSlot < 0 || sourceSlot >= pFast->numOfSources) {
    qError("FAST_INTERVAL received source index:%d outside [0,%d)", sourceSlot, pFast->numOfSources);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int64_t maxWindowIndex = -1;

  for (int32_t row = 0; row < pBlock->info.rows; ++row) {
    if (colDataIsNull(pWStartCol, pBlock->info.rows, row, NULL)) {
      qError("FAST_INTERVAL received NULL _wstart at row:%d", row);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    int64_t wstart = *(int64_t*)colDataGetData(pWStartCol, row);
    int64_t delta = wstart - pFast->firstWindowStart;
    if (delta < 0 || delta % pFast->interval != 0) {
      qError("FAST_INTERVAL received unaligned _wstart:%" PRId64, wstart);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    int64_t windowIndex = delta / pFast->interval;
    if (windowIndex >= pFast->numOfWindows) {
      qError("FAST_INTERVAL received _wstart outside configured range:%" PRId64, wstart);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    maxWindowIndex = TMAX(maxWindowIndex, windowIndex);
    int32_t sourceIndex = pFast->pWindowRowCounts[windowIndex]++;
    if (sourceIndex == 0) {
      ++pFast->activeCount;
      if (windowIndex < pFast->minActiveWindow) pFast->minActiveWindow = (int32_t)windowIndex;
      if (windowIndex > pFast->maxActiveWindow) pFast->maxActiveWindow = (int32_t)windowIndex;
      if (!pFast->denseMode) {
        pFast->pActiveBitmap[windowIndex >> 6] |= 1ULL << (windowIndex & 63);
        if ((int64_t)pFast->activeCount * 5 >= pFast->numOfWindows) {
          pFast->denseMode = true;
        }
      }
    }
    if (sourceIndex >= pFast->numOfSources) {
      qError("FAST_INTERVAL received too many partial rows for window:%" PRId64, wstart);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    int32_t targetRow = (int32_t)windowIndex * pFast->numOfSources + sourceIndex;
    int32_t code = cacheFastMergeIntervalRow(pFast, pBlock, row, targetRow);
    if (code != TSDB_CODE_SUCCESS) {
      qError("FAST_INTERVAL failed to cache partial row, window:%" PRId64 ", source:%d, code:%s", wstart,
             sourceIndex, tstrerror(code));
      return code;
    }
  }

  int64_t sourceProgress = maxWindowIndex + 1;
  int32_t code = updateFastWatermarkHeap(&pFast->sourceProgress, sourceSlot, sourceProgress);
  if (code != TSDB_CODE_SUCCESS) {
    qError("FAST_INTERVAL failed to update source:%d progress:%" PRId64, sourceSlot, sourceProgress);
    return code;
  }

  int64_t sealedWindowExclusive = 0;
  if (getFastWatermarkHeapMin(&pFast->sourceProgress, &sealedWindowExclusive)) {
    if (sealedWindowExclusive > pFast->sealedWindowExclusive) {
      pFast->sealedWindowExclusive = (int32_t)TMIN(sealedWindowExclusive, pFast->numOfWindows);
      *pWatermarkAdvanced = true;
    }
  }

  pFast->pCachedRows->info.rows = pFast->numOfWindows * pFast->numOfSources;
  pFast->pCachedRows->info.dataLoad = 1;
  pFast->pCachedRows->info.scanFlag = MAIN_SCAN;
  return TSDB_CODE_SUCCESS;
}

static int32_t fastMergeIntervalNextActiveWindow(const SFastMergeIntervalInfo* pFast, int32_t start) {
  if (pFast == NULL || pFast->pActiveBitmap == NULL || start < 0 || start >= pFast->numOfWindows) {
    return pFast == NULL ? -1 : pFast->numOfWindows;
  }
  int32_t word = start >> 6;
  uint64_t bits = pFast->pActiveBitmap[word] & (~0ULL << (start & 63));
  while (true) {
    if (bits != 0) {
      int32_t index = (word << 6) + BUILDIN_CTZL(bits);
      return index < pFast->numOfWindows ? index : pFast->numOfWindows;
    }
    if (++word >= pFast->activeBitmapWords) return pFast->numOfWindows;
    bits = pFast->pActiveBitmap[word];
  }
}

static int32_t buildFastMergeIntervalResultBlock(SOperatorInfo* pOperator) {
  int32_t                               code = TSDB_CODE_SUCCESS;
  SMergeAlignedIntervalAggOperatorInfo* pMiaInfo = pOperator->info;
  SIntervalAggOperatorInfo*             pIaInfo = pMiaInfo->intervalAggOperatorInfo;
  SFastMergeIntervalInfo*               pFast = pMiaInfo->pFastInterval;
  SExprSupp*                            pSup = &pOperator->exprSupp;
  SExecTaskInfo*                        pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*                          pRes = pIaInfo->binfo.pRes;

  blockDataCleanup(pRes);
  pRes->info.version = pTaskInfo->version;
  pRes->info.dataLoad = 1;
  if (pFast->batchProcess) {
    bool sparse = !pFast->denseMode && pFast->activeCount > 0 &&
                  (int64_t)pFast->activeCount * 5 < (int64_t)(pFast->maxActiveWindow - pFast->minActiveWindow + 1);
    if (pFast->activeCount == 0) {
      pFast->nextWindow = pFast->numOfWindows;
      setOperatorCompleted(pOperator);
      return TSDB_CODE_SUCCESS;
    }
    if (sparse) {
      code = buildFastMergeIntervalSparseBatch(pOperator, pIaInfo, pFast);
    } else {
      code = buildFastMergeIntervalBatch(pOperator, pIaInfo, pFast);
    }
    if (code != TSDB_CODE_SUCCESS) return code;
    if (pFast->inputCompleted && pFast->nextWindow >= pFast->numOfWindows) {
      setOperatorCompleted(pOperator);
    }
    return doFilterWithTaskCorrelation(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL,
                                       pOperator->pTaskInfo);
  }
  bool sparse = !pFast->denseMode && pFast->activeCount > 0 &&
                (int64_t)pFast->activeCount * 5 < (int64_t)(pFast->maxActiveWindow - pFast->minActiveWindow + 1);
  if (sparse) {
    code = buildFastMergeIntervalSparseRows(pOperator, pIaInfo, pFast);
    if (TSDB_CODE_SUCCESS != code) return code;
    if (pFast->inputCompleted && pFast->nextWindow >= pFast->numOfWindows) {
      setOperatorCompleted(pOperator);
    }
    return doFilterWithTaskCorrelation(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL,
                                       pOperator->pTaskInfo);
  }
  while (pFast->nextWindow < pFast->sealedWindowExclusive && pRes->info.rows < pOperator->resultInfo.threshold) {
    int32_t windowIndex = pFast->nextWindow;
    if (sparse) {
      windowIndex = fastMergeIntervalNextActiveWindow(pFast, windowIndex);
      if (windowIndex >= pFast->sealedWindowExclusive) {
        pFast->nextWindow = pFast->sealedWindowExclusive;
        break;
      }
    }
    pFast->nextWindow = windowIndex + 1;
    int32_t numOfRows = pFast->pWindowRowCounts[windowIndex];
    if (numOfRows == 0) {
      continue;
    }

    STimeWindow win = {.skey = pFast->firstWindowStart + (int64_t)windowIndex * pFast->interval,
                       .ekey = pFast->firstWindowStart + (int64_t)(windowIndex + 1) * pFast->interval - 1};
    SResultRow* pResultRow = pFast->pResultRow;
    if (pResultRow == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    pResultRow->win = win;
    pFast->resultRowActive = true;
    code = setResultRowInitCtx(pResultRow, pSup->pCtx, pSup->numOfExprs, pSup->rowEntryInfoOffset);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }

    updateTimeWindowInfo(&pIaInfo->twAggSup.timeWindowData, &win, 1);
    int32_t startRow = windowIndex * pFast->numOfSources;
    code = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pIaInfo->twAggSup.timeWindowData, startRow,
                                           numOfRows, pFast->pCachedRows->info.rows, pSup->numOfExprs);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }

    doUpdateNumOfRows(pSup->pCtx, pResultRow, pSup->numOfExprs, pSup->rowEntryInfoOffset);
    if (pResultRow->numOfRows > 0) {
      code = blockDataEnsureCapacity(pRes, pRes->info.rows + pResultRow->numOfRows);
      if (code != TSDB_CODE_SUCCESS) {
        goto _cleanup;
      }

      updateTimeWindowInfo(&pIaInfo->twAggSup.timeWindowData, &win, 0);
      code = copyResultrowToDataBlock(pSup->pExprInfo, pSup->numOfExprs, pResultRow, pSup->pCtx, pRes,
                                      pSup->rowEntryInfoOffset, pTaskInfo);
      if (code == TSDB_CODE_SUCCESS) {
        pRes->info.rows += pResultRow->numOfRows;
      }
    }

_cleanup:
    cleanupFastMergeIntervalResultRow(pFast, pSup);
    resetResultRow(pResultRow, pIaInfo->aggSup.resultRowSize - sizeof(SResultRow));
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if (pFast->nextWindow >= pFast->numOfWindows) {
    setOperatorCompleted(pOperator);
  }
  return doFilterWithTaskCorrelation(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL,
                                     pOperator->pTaskInfo);
}

void destroyMAIOperatorInfo(void* param) {
  SMergeAlignedIntervalAggOperatorInfo* miaInfo = (SMergeAlignedIntervalAggOperatorInfo*)param;
  if (miaInfo == NULL) {
    return;
  }
  cleanupFastMergeIntervalInfo(miaInfo);
  releaseExIntervalInfo(&miaInfo->pExInterval);
  if (miaInfo->intervalAggOperatorInfo != NULL) {
    destroyIntervalOperatorInfo(miaInfo->intervalAggOperatorInfo);
  }
  taosMemoryFreeClear(param);
}

static int32_t exIntervalSetOutputValue(SColumnInfoData* pCol, int32_t row, int8_t type,
                                        const SExIntervalCell* pCell, int64_t wstart, int64_t wend, int64_t wduration) {
  if (NULL == pCol || NULL == pCell) return TSDB_CODE_INVALID_PARA;
  if (EX_INTERVAL_CELL_WSTART == type) return colDataSetVal(pCol, row, (const char*)&wstart, false);
  if (EX_INTERVAL_CELL_WEND == type) return colDataSetVal(pCol, row, (const char*)&wend, false);
  if (EX_INTERVAL_CELL_WDURATION == type) return colDataSetVal(pCol, row, (const char*)&wduration, false);
  int64_t count = atomic_load_64((int64_t*)&pCell->count);
  if (EX_INTERVAL_CELL_COUNT == type) return colDataSetVal(pCol, row, (const char*)&count, false);
  if (0 == count) return colDataSetVal(pCol, row, NULL, true);
  double value = EX_INTERVAL_CELL_AVG == type ? pCell->sum / count : pCell->sum;
  if (TSDB_DATA_TYPE_FLOAT == pCol->info.type) {
    float result = (float)value;
    return colDataSetVal(pCol, row, (const char*)&result, false);
  }
  if (TSDB_DATA_TYPE_DOUBLE == pCol->info.type) return colDataSetVal(pCol, row, (const char*)&value, false);
  return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
}

static int32_t buildExMergeIntervalResultBlock(SOperatorInfo* pOperator) {
  SMergeAlignedIntervalAggOperatorInfo* pMiaInfo = pOperator->info;
  SIntervalAggOperatorInfo* pIaInfo = pMiaInfo->intervalAggOperatorInfo;
  SExIntervalInfo* pExInfo = pMiaInfo->pExInterval;
  if (NULL == pIaInfo || NULL == pExInfo || pExInfo->finalized) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == pExInfo->pHashEntry || NULL == pExInfo->pEntry) {
    code = acquireExIntervalEntry(pExInfo);
  }
  if (TSDB_CODE_SUCCESS != code || NULL == pExInfo->pHashEntry || NULL == pExInfo->pEntry) {
    if (TSDB_CODE_SUCCESS == code) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    goto _return;
  }
  SExIntervalEntry* pEntry = pExInfo->pEntry;
  int32_t entryCode = atomic_load_32(&pEntry->errCode);
  if (0 == atomic_load_32(&pEntry->ready) || TSDB_CODE_SUCCESS != entryCode) {
    qError("EX_INTERVAL shared result is invalid, qId:%" PRIu64 ", ready:%d, errCode:%s, aggs:%d",
           pExInfo->queryId, atomic_load_32(&pEntry->ready), tstrerror(entryCode), pEntry->numOfAggs);
    code = TSDB_CODE_SUCCESS == entryCode ? TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR : entryCode;
    goto _return;
  }
  code = exIntervalInitMergeCellIndexes(pExInfo, &pOperator->exprSupp);
  if (TSDB_CODE_SUCCESS != code) {
    goto _return;
  }
  SSDataBlock* pRes = pIaInfo->binfo.pRes;
  blockDataCleanup(pRes);
  int32_t activeCount = atomic_load_32(&pEntry->activeCount);
  int32_t minWindow = atomic_load_32(&pEntry->minActiveWindow);
  int32_t maxWindow = atomic_load_32(&pEntry->maxActiveWindow);
  if (activeCount <= 0 || minWindow > maxWindow) {
    code = TSDB_CODE_SUCCESS;
    goto _finalize;
  }
  code = blockDataEnsureCapacity(pRes, activeCount);
  if (TSDB_CODE_SUCCESS != code) goto _return;
  for (int32_t window = minWindow; window <= maxWindow; ++window) {
    if (0 == atomic_load_64(&pEntry->pRows[window])) continue;
    int32_t resultRow = pRes->info.rows;
    int64_t windowOffset = 0;
    int64_t wstart = 0;
    int64_t wend = 0;
    if (!exIntervalMulInt64((int64_t)window, pEntry->interval, &windowOffset) ||
        !exIntervalAddInt64(pEntry->firstWindowStart, windowOffset, &wstart) ||
        !exIntervalAddInt64(wstart, pEntry->interval, &wend)) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _return;
    }
    for (int32_t expr = 0; expr < pOperator->exprSupp.numOfExprs; ++expr) {
      SColumnInfoData* pCol = taosArrayGet(pRes->pDataBlock, pOperator->exprSupp.pExprInfo[expr].base.resSchema.slotId);
      if (NULL == pCol) { code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR; goto _return; }
      int8_t cellIndex = pExInfo->mergeCellIndex[expr];
      if (cellIndex < 0 || cellIndex >= pEntry->numOfAggs) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        goto _return;
      }
      SExIntervalCell* pCell = &pEntry->pCells[(size_t)window * pEntry->numOfAggs + cellIndex];
      code = exIntervalSetOutputValue(pCol, resultRow, pEntry->cellTypes[cellIndex], pCell, wstart, wend,
                                      pEntry->interval);
      if (TSDB_CODE_SUCCESS != code) goto _return;
    }
    pRes->info.rows += 1;
  }
  pRes->info.dataLoad = 1;
  pRes->info.version = pOperator->pTaskInfo->version;
  SColumnInfoData* pTsCol = taosArrayGet(pRes->pDataBlock, pIaInfo->primaryTsIndex);
  if (pRes->info.rows > 0 && NULL != pTsCol && TSDB_DATA_TYPE_TIMESTAMP == pTsCol->info.type) {
    code = blockDataUpdateTsWindow(pRes, pIaInfo->primaryTsIndex);
    if (TSDB_CODE_SUCCESS != code) goto _return;
  }
_finalize:
  code = taosHashRemove(gExIntervalHash, &pExInfo->queryId, sizeof(pExInfo->queryId));
  if (TSDB_CODE_SUCCESS != code && TSDB_CODE_NOT_FOUND != code) goto _return;
  code = TSDB_CODE_SUCCESS;
  pExInfo->pEntry = NULL;
  pExInfo->finalized = true;
  code = doFilterWithTaskCorrelation(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL,
                                     pOperator->pTaskInfo);
_return:
  if (NULL != pExInfo->pHashEntry) {
    int32_t removeCode = taosHashRemove(gExIntervalHash, &pExInfo->queryId, sizeof(pExInfo->queryId));
    if (TSDB_CODE_SUCCESS != removeCode && TSDB_CODE_NOT_FOUND != removeCode && TSDB_CODE_SUCCESS == code) {
      code = removeCode;
    }
    taosHashRelease(gExIntervalHash, pExInfo->pHashEntry);
    pExInfo->pHashEntry = NULL;
  }
  pExInfo->pEntry = NULL;
  return code;
}

static SResultRow* doSetSingleOutputTupleBuf(SResultRowInfo* pResultRowInfo, SAggSupporter* pSup) {
  SResultRow* pResult = getNewResultRow(pSup->pResultBuf, &pSup->currentPageId, pSup->resultRowSize);
  if (NULL == pResult) {
    return pResult;
  }
  pResultRowInfo->cur = (SResultRowPosition){.pageId = pResult->pageId, .offset = pResult->offset};
  return pResult;
}

static int32_t setSingleOutputTupleBuf(SResultRowInfo* pResultRowInfo, STimeWindow* win, SResultRow** pResult,
                                       SExprSupp* pExprSup, SAggSupporter* pAggSup) {
  if (*pResult == NULL) {
    *pResult = doSetSingleOutputTupleBuf(pResultRowInfo, pAggSup);
    if (*pResult == NULL) {
      return terrno;
    }
  }

  // set time window for current result
  (*pResult)->win = (*win);
  return setResultRowInitCtx((*pResult), pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
}

static void doMergeAlignedIntervalAggImpl(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo,
                                          SSDataBlock* pBlock, SSDataBlock* pResultBlock) {
  SMergeAlignedIntervalAggOperatorInfo* miaInfo = pOperatorInfo->info;
  SIntervalAggOperatorInfo*             iaInfo = miaInfo->intervalAggOperatorInfo;

  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*     pSup = &pOperatorInfo->exprSupp;
  SInterval*     pInterval = &iaInfo->interval;

  int32_t  startPos = 0;
  int64_t* tsCols = extractTsCol(pBlock, iaInfo, pTaskInfo);

  TSKEY ts = getStartTsKey(&pBlock->info.window, tsCols);

  // there is an result exists
  if (miaInfo->curTs != INT64_MIN) {
    if (ts != miaInfo->curTs) {
      finalizeResultRows(iaInfo->aggSup.pResultBuf, &pResultRowInfo->cur, pSup, pResultBlock, pTaskInfo);
      resetResultRow(miaInfo->pResultRow, iaInfo->aggSup.resultRowSize - sizeof(SResultRow));
      miaInfo->curTs = ts;
    }
  } else {
    miaInfo->curTs = ts;
  }

  STimeWindow win = {0};
  win.skey = miaInfo->curTs;
  win.ekey = taosTimeGetIntervalEnd(win.skey, pInterval);

  int32_t ret = setSingleOutputTupleBuf(pResultRowInfo, &win, &miaInfo->pResultRow, pSup, &iaInfo->aggSup);
  if (ret != TSDB_CODE_SUCCESS || miaInfo->pResultRow == NULL) {
    T_LONG_JMP(pTaskInfo->env, ret);
  }

  int32_t currPos = startPos;

  STimeWindow currWin = win;
  while (++currPos < pBlock->info.rows) {
    if (tsCols[currPos] == miaInfo->curTs) {
      continue;
    }

    updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &currWin, 1);
    ret = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &iaInfo->twAggSup.timeWindowData, startPos,
                                          currPos - startPos, pBlock->info.rows, pSup->numOfExprs);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }

    finalizeResultRows(iaInfo->aggSup.pResultBuf, &pResultRowInfo->cur, pSup, pResultBlock, pTaskInfo);
    resetResultRow(miaInfo->pResultRow, iaInfo->aggSup.resultRowSize - sizeof(SResultRow));
    miaInfo->curTs = tsCols[currPos];

    currWin.skey = miaInfo->curTs;
    currWin.ekey = taosTimeGetIntervalEnd(currWin.skey, pInterval);

    startPos = currPos;
    ret = setSingleOutputTupleBuf(pResultRowInfo, &win, &miaInfo->pResultRow, pSup, &iaInfo->aggSup);
    if (ret != TSDB_CODE_SUCCESS || miaInfo->pResultRow == NULL) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }

    miaInfo->curTs = currWin.skey;
  }

  updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &currWin, 1);
  ret = applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &iaInfo->twAggSup.timeWindowData, startPos,
                                        currPos - startPos, pBlock->info.rows, pSup->numOfExprs);
  if (ret != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, ret);
  }
}

static void cleanupAfterGroupResultGen(SMergeAlignedIntervalAggOperatorInfo* pMiaInfo, SSDataBlock* pRes) {
  pRes->info.id.groupId = pMiaInfo->groupId;
  pMiaInfo->curTs = INT64_MIN;
  pMiaInfo->groupId = 0;
}

static void doMergeAlignedIntervalAgg(SOperatorInfo* pOperator) {
  SExecTaskInfo*                        pTaskInfo = pOperator->pTaskInfo;
  int32_t                               code = TSDB_CODE_SUCCESS;
  int32_t                               lino = 0;
  SMergeAlignedIntervalAggOperatorInfo* pMiaInfo = pOperator->info;
  SIntervalAggOperatorInfo*             pIaInfo = pMiaInfo->intervalAggOperatorInfo;

  SExprSupp*      pSup = &pOperator->exprSupp;
  SSDataBlock*    pRes = pIaInfo->binfo.pRes;
  SResultRowInfo* pResultRowInfo = &pIaInfo->binfo.resultRowInfo;
  SOperatorInfo*  downstream = pOperator->pDownstream[0];

  while (1) {
    SSDataBlock* pBlock = NULL;
    if (pMiaInfo->prefetchedBlock == NULL) {
      pBlock = getNextBlockFromDownstreamRemainDetach(pOperator, 0);
    } else {
      pBlock = pMiaInfo->prefetchedBlock;
      pMiaInfo->prefetchedBlock = NULL;

      pMiaInfo->groupId = pBlock->info.id.groupId;
    }

    // no data exists, all query processing is done
    if (pBlock == NULL) {
      // close last unclosed time window
      if (pMiaInfo->curTs != INT64_MIN) {
        finalizeResultRows(pIaInfo->aggSup.pResultBuf, &pResultRowInfo->cur, pSup, pRes, pTaskInfo);
        resetResultRow(pMiaInfo->pResultRow, pIaInfo->aggSup.resultRowSize - sizeof(SResultRow));
        cleanupAfterGroupResultGen(pMiaInfo, pRes);
        code = doFilterWithTaskCorrelation(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL,
                                           pOperator->pTaskInfo);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      setOperatorCompleted(pOperator);
      break;
    }

    if (pMiaInfo->groupId == 0) {
      if (pMiaInfo->groupId != pBlock->info.id.groupId) {
        pMiaInfo->groupId = pBlock->info.id.groupId;
        pRes->info.id.groupId = pMiaInfo->groupId;
      }
    } else {
      if (pMiaInfo->groupId != pBlock->info.id.groupId) {
        // if there are unclosed time window, close it firstly.
        if (pMiaInfo->curTs == INT64_MIN) {
          pTaskInfo->code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
          T_LONG_JMP(pTaskInfo->env, terrno);
        }
        finalizeResultRows(pIaInfo->aggSup.pResultBuf, &pResultRowInfo->cur, pSup, pRes, pTaskInfo);
        resetResultRow(pMiaInfo->pResultRow, pIaInfo->aggSup.resultRowSize - sizeof(SResultRow));

        pMiaInfo->prefetchedBlock = pBlock;
        cleanupAfterGroupResultGen(pMiaInfo, pRes);
        code = doFilterWithTaskCorrelation(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL,
                                           pOperator->pTaskInfo);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pRes->info.rows == 0) {
          // After filtering for last group, the result is empty, so we need to continue to process next group
          continue;
        } else {
          break;
        }
      } else {
        // continue
        pRes->info.id.groupId = pMiaInfo->groupId;
      }
    }

    pRes->info.scanFlag = pBlock->info.scanFlag;

    if (pIaInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pIaInfo->scalarSupp;
      code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL,
                                   GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = setInputDataBlock(pSup, pBlock, pIaInfo->binfo.inputTsOrder, pBlock->info.scanFlag, true);
    QUERY_CHECK_CODE(code, lino, _end);

    doMergeAlignedIntervalAggImpl(pOperator, &pIaInfo->binfo.resultRowInfo, pBlock, pRes);

    code = doFilterWithTaskCorrelation(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL,
                                       pOperator->pTaskInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pRes->info.rows >= pOperator->resultInfo.capacity) {
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static int32_t mergeAlignedIntervalAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SExecTaskInfo*                        pTaskInfo = pOperator->pTaskInfo;
  int32_t                               code = TSDB_CODE_SUCCESS;
  SMergeAlignedIntervalAggOperatorInfo* pMiaInfo = pOperator->info;
  SIntervalAggOperatorInfo*             iaInfo = pMiaInfo->intervalAggOperatorInfo;
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  SSDataBlock* pRes = iaInfo->binfo.pRes;
  if (pMiaInfo->pExInterval != NULL) {
    code = acquireExIntervalEntry(pMiaInfo->pExInterval);
    if (TSDB_CODE_SUCCESS != code) {
      pTaskInfo->code = code;
      return code;
    }
    while (true) {
      SSDataBlock* pBlock = NULL;
      code = getNextBlockFromDownstreamChecked(pOperator, 0, false,
                                               &pBlock);
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        return code;
      }
      if (NULL == pBlock) break;
    }
    code = buildExMergeIntervalResultBlock(pOperator);
    if (TSDB_CODE_SUCCESS != code) {
      pTaskInfo->code = code;
      return code;
    }
    setOperatorCompleted(pOperator);
    pOperator->resultInfo.totalRows += pRes->info.rows;
    *ppRes = pRes->info.rows == 0 ? NULL : pRes;
    return code;
  }
  if (pMiaInfo->pFastInterval != NULL) {
    SFastMergeIntervalInfo* pFast = pMiaInfo->pFastInterval;
    while (1) {
      bool watermarkAdvanced = pFast->nextWindow < pFast->sealedWindowExclusive;
      while (!watermarkAdvanced && !pFast->inputCompleted) {
        code = collectFastMergeIntervalRows(pOperator, &watermarkAdvanced);
        if (code != TSDB_CODE_SUCCESS) {
          pTaskInfo->code = code;
          return code;
        }
      }

      if (!watermarkAdvanced && pFast->nextWindow >= pFast->sealedWindowExclusive) {
        setOperatorCompleted(pOperator);
        break;
      }

      code = setInputDataBlock(&pOperator->exprSupp, pFast->pCachedRows, iaInfo->binfo.inputTsOrder, MAIN_SCAN, true);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        return code;
      }
      code = buildFastMergeIntervalResultBlock(pOperator);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        return code;
      }
      if (pRes->info.rows > 0 || pOperator->status == OP_EXEC_DONE) {
        break;
      }
    }
    *ppRes = (pRes->info.rows == 0) ? NULL : pRes;
    return code;
  }

  blockDataCleanup(pRes);

  if (iaInfo->binfo.mergeResultBlock) {
    while (1) {
      if (pOperator->status == OP_EXEC_DONE) {
        break;
      }

      if (pRes->info.rows >= pOperator->resultInfo.threshold) {
        break;
      }

      doMergeAlignedIntervalAgg(pOperator);
    }
  } else {
    doMergeAlignedIntervalAgg(pOperator);
  }

  (*ppRes) = (pRes->info.rows == 0) ? NULL : pRes;
  return code;
}

static int32_t resetMergeAlignedIntervalOperState(SOperatorInfo* pOper) {
  SMergeAlignedIntervalAggOperatorInfo* pInfo = pOper->info;
  
  uint64_t     groupId;  // current groupId
  int64_t      curTs;    // current ts
  SSDataBlock* prefetchedBlock;
  SResultRow*  pResultRow;

  pInfo->groupId = 0;
  pInfo->curTs = INT64_MIN;
  pInfo->prefetchedBlock = NULL;
  pInfo->pResultRow = NULL;

  bool fastInterval = (pInfo->pFastInterval != NULL);
  bool exInterval = (pInfo->pExInterval != NULL);
  if (exInterval) {
    releaseExIntervalInfo(&pInfo->pExInterval);
  }
  cleanupFastMergeIntervalInfo(pInfo);
  int32_t code = resetInterval(pOper, pInfo->intervalAggOperatorInfo);
  if (code != TSDB_CODE_SUCCESS || !fastInterval) {
    if (code != TSDB_CODE_SUCCESS || !exInterval) return code;
    code = initExIntervalInfo(pInfo->intervalAggOperatorInfo, pOper, true);
    if (TSDB_CODE_SUCCESS == code) {
      pInfo->pExInterval = pInfo->intervalAggOperatorInfo->pExInterval;
      pInfo->intervalAggOperatorInfo->pExInterval = NULL;
    }
    return code;
  }
  return initFastMergeIntervalInfo(pInfo, (SMergeAlignedIntervalPhysiNode*)pOper->pPhyNode);
}

int32_t createMergeAlignedIntervalOperatorInfo(SOperatorInfo* downstream, SMergeAlignedIntervalPhysiNode* pNode,
                                               SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                               code = TSDB_CODE_SUCCESS;
  int32_t                               lino = 0;
  SMergeAlignedIntervalAggOperatorInfo* miaInfo = taosMemoryCalloc(1, sizeof(SMergeAlignedIntervalAggOperatorInfo));
  SOperatorInfo*                        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (miaInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  pOperator->pPhyNode = pNode;
  miaInfo->intervalAggOperatorInfo = taosMemoryCalloc(1, sizeof(SIntervalAggOperatorInfo));
  if (miaInfo->intervalAggOperatorInfo == NULL) {
    code = terrno;
    goto _error;
  }

  SInterval interval = {.interval = pNode->interval,
                        .sliding = pNode->sliding,
                        .intervalUnit = pNode->intervalUnit,
                        .slidingUnit = pNode->slidingUnit,
                        .offset = pNode->offset,
                        .precision = ((SColumnNode*)pNode->window.pTspk)->node.resType.precision,
                        .timeRange = pNode->timeRange,
                        .firstDayOfWeek = pNode->firstDayOfWeek,
                        .timezone = pNode->timezone};
  calcIntervalAutoOffset(&interval);

  SIntervalAggOperatorInfo* iaInfo = miaInfo->intervalAggOperatorInfo;
  SExprSupp*                pSup = &pOperator->exprSupp;
  pSup->hasWindowOrGroup = true;
  pSup->hasWindow = true;

  code = filterInitFromNode((SNode*)pNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  miaInfo->curTs = INT64_MIN;
  iaInfo->win = pTaskInfo->window;
  setOptrBasicInfoOrder(&iaInfo->binfo, &pNode->window.node);
  iaInfo->interval = interval;
  iaInfo->primaryTsIndex = ((SColumnNode*)pNode->window.pTspk)->slotId;
  iaInfo->binfo.mergeResultBlock = pNode->window.mergeDataBlock;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 512);

  int32_t    num = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pNode->window.pFuncs, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initAggSup(&pOperator->exprSupp, &iaInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                    NULL, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalar);
    QUERY_CHECK_CODE(code, lino, _error);
    code = initExprSupp(&iaInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pNode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&iaInfo->binfo, pResBlock);
  code = initExecTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &iaInfo->win);
  QUERY_CHECK_CODE(code, lino, _error);

  iaInfo->timeWindowInterpo = false;
  code = timeWindowinterpNeeded(pSup->pCtx, num, iaInfo, &iaInfo->timeWindowInterpo);
  QUERY_CHECK_CODE(code, lino, _error);
  if (iaInfo->timeWindowInterpo) {
    iaInfo->binfo.resultRowInfo.openWindow = tdListNew(sizeof(SOpenWindowInfo));
  }

  initResultRowInfo(&iaInfo->binfo.resultRowInfo);
  code = blockDataEnsureCapacity(iaInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);
  iaInfo->pOperator = pOperator;
  setOperatorInfo(pOperator, "TimeMergeAlignedIntervalAggOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL,
                  false, OP_NOT_OPENED, miaInfo, pTaskInfo);
  if (pNode->fastInterval) {
    code = initFastMergeIntervalInfo(miaInfo, pNode);
    QUERY_CHECK_CODE(code, lino, _error);
  }
  if (pNode->exInterval) {
    code = initExIntervalInfo(iaInfo, pOperator, true);
    QUERY_CHECK_CODE(code, lino, _error);
    miaInfo->pExInterval = iaInfo->pExInterval;
    iaInfo->pExInterval = NULL;
  }
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, mergeAlignedIntervalAggNext, NULL, destroyMAIOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetMergeAlignedIntervalOperState);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (miaInfo != NULL) destroyMAIOperatorInfo(miaInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

//=====================================================================================================================
// merge interval operator
typedef struct SMergeIntervalAggOperatorInfo {
  SIntervalAggOperatorInfo intervalAggOperatorInfo;
  SList*                   groupIntervals;
  SListIter                groupIntervalsIter;
  bool                     hasGroupId;
  uint64_t                 groupId;
  SSDataBlock*             prefetchedBlock;
  bool                     inputBlocksFinished;
} SMergeIntervalAggOperatorInfo;

typedef struct SGroupTimeWindow {
  uint64_t    groupId;
  STimeWindow window;
} SGroupTimeWindow;

void destroyMergeIntervalOperatorInfo(void* param) {
  SMergeIntervalAggOperatorInfo* miaInfo = (SMergeIntervalAggOperatorInfo*)param;
  miaInfo->groupIntervals = tdListFree(miaInfo->groupIntervals);
  destroyIntervalOperatorInfo(&miaInfo->intervalAggOperatorInfo);

  taosMemoryFreeClear(param);
}

static int32_t outputPrevIntervalResult(SOperatorInfo* pOperatorInfo, uint64_t tableGroupId, SSDataBlock* pResultBlock,
                                        STimeWindow* newWin) {
  SMergeIntervalAggOperatorInfo* miaInfo = pOperatorInfo->info;
  SIntervalAggOperatorInfo*      iaInfo = &miaInfo->intervalAggOperatorInfo;
  bool                           ascScan = (iaInfo->binfo.inputTsOrder == TSDB_ORDER_ASC);

  SGroupTimeWindow groupTimeWindow = {.groupId = tableGroupId, .window = *newWin};
  int32_t          code = tdListAppend(miaInfo->groupIntervals, &groupTimeWindow);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }

  SListIter iter = {0};
  tdListInitIter(miaInfo->groupIntervals, &iter, TD_LIST_FORWARD);
  SListNode* listNode = NULL;
  while ((listNode = tdListNext(&iter)) != NULL) {
    SGroupTimeWindow* prevGrpWin = (SGroupTimeWindow*)listNode->data;
    if (prevGrpWin->groupId != tableGroupId) {
      continue;
    }

    STimeWindow* prevWin = &prevGrpWin->window;
    if ((ascScan && newWin->skey > prevWin->ekey) || ((!ascScan) && newWin->skey < prevWin->ekey)) {
      SListNode* tmp = tdListPopNode(miaInfo->groupIntervals, listNode);
      taosMemoryFreeClear(tmp);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void doMergeIntervalAggImpl(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock,
                                   int32_t scanFlag, SSDataBlock* pResultBlock) {
  SMergeIntervalAggOperatorInfo* miaInfo = pOperatorInfo->info;
  SIntervalAggOperatorInfo*      iaInfo = &miaInfo->intervalAggOperatorInfo;

  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*     pExprSup = &pOperatorInfo->exprSupp;

  int32_t     startPos = 0;
  int32_t     numOfOutput = pExprSup->numOfExprs;
  int64_t*    tsCols = extractTsCol(pBlock, iaInfo, pTaskInfo);
  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, iaInfo->primaryTsIndex);
  uint64_t    tableGroupId = pBlock->info.id.groupId;
  bool        ascScan = (iaInfo->binfo.inputTsOrder == TSDB_ORDER_ASC);
  SResultRow* pResult = NULL;

  while (pTsCol != NULL && startPos < pBlock->info.rows && pTsCol->hasNull && colDataIsNull_f(pTsCol, startPos)) {
    ++startPos;
  }
  if (startPos >= pBlock->info.rows) {
    return;
  }

  TSKEY blockStartTs = tsCols[startPos];

  STimeWindow win = getActiveTimeWindow(iaInfo->aggSup.pResultBuf, pResultRowInfo, blockStartTs, &iaInfo->interval,
                                        iaInfo->binfo.inputTsOrder);

  int32_t ret =
      setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pExprSup->pCtx,
                             numOfOutput, pExprSup->rowEntryInfoOffset, &iaInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
    T_LONG_JMP(pTaskInfo->env, ret);
  }

  TSKEY   ekey = ascScan ? win.ekey : win.skey;
  int32_t forwardRows = getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL,
                                                 iaInfo->binfo.inputTsOrder);
  if (forwardRows <= 0) {
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }

  // prev time window not interpolation yet.
  if (iaInfo->timeWindowInterpo) {
    SResultRowPosition pos = addToOpenWindowList(pResultRowInfo, pResult, tableGroupId, pTaskInfo);
    doInterpUnclosedTimeWindow(pOperatorInfo, numOfOutput, pResultRowInfo, pBlock, scanFlag, tsCols, &pos);

    // restore current time window
    ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pExprSup->pCtx,
                                 numOfOutput, pExprSup->rowEntryInfoOffset, &iaInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }

    // window start key interpolation
    ret = doWindowBorderInterpolation(iaInfo, pBlock, pResult, &win, startPos, forwardRows, pExprSup);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }
  }

  updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &win, 1);
  ret = applyAggFunctionOnPartialTuples(pTaskInfo, pExprSup->pCtx, &iaInfo->twAggSup.timeWindowData, startPos,
                                        forwardRows, pBlock->info.rows, numOfOutput);
  if (ret != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, ret);
  }
  doCloseWindow(pResultRowInfo, iaInfo, pResult);

  // output previous interval results after this interval (&win) is closed
  int32_t code = outputPrevIntervalResult(pOperatorInfo, tableGroupId, pResultBlock, &win);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }

  STimeWindow nextWin = win;
  while (1) {
    int32_t prevEndPos = forwardRows - 1 + startPos;
    startPos = getNextQualifiedWindow(&iaInfo->interval, &nextWin, &pBlock->info, tsCols, prevEndPos,
                                      iaInfo->binfo.inputTsOrder);
    if (startPos < 0) {
      break;
    }

    // null data, failed to allocate more memory buffer
    code =
        setTimeWindowOutputBuf(pResultRowInfo, &nextWin, (scanFlag == MAIN_SCAN), &pResult, tableGroupId,
                               pExprSup->pCtx, numOfOutput, pExprSup->rowEntryInfoOffset, &iaInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    ekey = ascScan ? nextWin.ekey : nextWin.skey;
    forwardRows = getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL,
                                           iaInfo->binfo.inputTsOrder);

    // window start(end) key interpolation
    code = doWindowBorderInterpolation(iaInfo, pBlock, pResult, &nextWin, startPos, forwardRows, pExprSup);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &nextWin, 1);
    code = applyAggFunctionOnPartialTuples(pTaskInfo, pExprSup->pCtx, &iaInfo->twAggSup.timeWindowData, startPos,
                                           forwardRows, pBlock->info.rows, numOfOutput);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
    doCloseWindow(pResultRowInfo, iaInfo, pResult);

    // output previous interval results after this interval (&nextWin) is closed
    code = outputPrevIntervalResult(pOperatorInfo, tableGroupId, pResultBlock, &nextWin);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }

  if (iaInfo->timeWindowInterpo) {
    code = saveDataBlockLastRow(iaInfo->pPrevValues, pBlock, iaInfo->pInterpCols);
    if (TSDB_CODE_SUCCESS != code) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }
}

static int32_t doMergeIntervalAggNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SMergeIntervalAggOperatorInfo* miaInfo = pOperator->info;
  SIntervalAggOperatorInfo*      iaInfo = &miaInfo->intervalAggOperatorInfo;
  SExprSupp*                     pExpSupp = &pOperator->exprSupp;

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  SSDataBlock* pRes = iaInfo->binfo.pRes;
  blockDataCleanup(pRes);
  code = blockDataEnsureCapacity(pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  if (!miaInfo->inputBlocksFinished) {
    SOperatorInfo* downstream = pOperator->pDownstream[0];
    while (1) {
      SSDataBlock* pBlock = NULL;
      if (miaInfo->prefetchedBlock == NULL) {
        pBlock = getNextBlockFromDownstream(pOperator, 0);
      } else {
        pBlock = miaInfo->prefetchedBlock;
        miaInfo->groupId = pBlock->info.id.groupId;
        miaInfo->prefetchedBlock = NULL;
      }

      if (pBlock == NULL) {
        tdListInitIter(miaInfo->groupIntervals, &miaInfo->groupIntervalsIter, TD_LIST_FORWARD);
        miaInfo->inputBlocksFinished = true;
        break;
      }

      if (!miaInfo->hasGroupId) {
        miaInfo->hasGroupId = true;
        miaInfo->groupId = pBlock->info.id.groupId;
      } else if (miaInfo->groupId != pBlock->info.id.groupId) {
        miaInfo->prefetchedBlock = pBlock;
        break;
      }

      pRes->info.scanFlag = pBlock->info.scanFlag;
      code = setInputDataBlock(pExpSupp, pBlock, iaInfo->binfo.inputTsOrder, pBlock->info.scanFlag, true);
      QUERY_CHECK_CODE(code, lino, _end);

      doMergeIntervalAggImpl(pOperator, &iaInfo->binfo.resultRowInfo, pBlock, pBlock->info.scanFlag, pRes);

      if (pRes->info.rows >= pOperator->resultInfo.threshold) {
        break;
      }
    }

    pRes->info.id.groupId = miaInfo->groupId;
  }

  if (miaInfo->inputBlocksFinished) {
    SListNode* listNode = tdListNext(&miaInfo->groupIntervalsIter);

    if (listNode != NULL) {
      SGroupTimeWindow* grpWin = (SGroupTimeWindow*)(listNode->data);
      pRes->info.id.groupId = grpWin->groupId;
    }
  }

  if (pRes->info.rows == 0) {
    setOperatorCompleted(pOperator);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) = (pRes->info.rows == 0) ? NULL : pRes;
  return code;
}

static int32_t resetMergeIntervalOperState(SOperatorInfo* pOper) {
  SMergeIntervalAggOperatorInfo* pInfo = pOper->info;

  pInfo->hasGroupId = false;
  pInfo->groupId = 0;
  pInfo->prefetchedBlock = NULL;
  pInfo->inputBlocksFinished = false;
  tdListEmpty(pInfo->groupIntervals);
  
  SIntervalAggOperatorInfo* pIntervalInfo = &pInfo->intervalAggOperatorInfo;
  return resetInterval(pOper, pIntervalInfo);
}

int32_t createMergeIntervalOperatorInfo(SOperatorInfo* downstream, SMergeIntervalPhysiNode* pIntervalPhyNode,
                                        SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SMergeIntervalAggOperatorInfo* pMergeIntervalInfo = taosMemoryCalloc(1, sizeof(SMergeIntervalAggOperatorInfo));
  SOperatorInfo*                 pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pMergeIntervalInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  pOperator->pPhyNode = pIntervalPhyNode;
  SInterval interval = {.interval = pIntervalPhyNode->interval,
                        .sliding = pIntervalPhyNode->sliding,
                        .intervalUnit = pIntervalPhyNode->intervalUnit,
                        .slidingUnit = pIntervalPhyNode->slidingUnit,
                        .offset = pIntervalPhyNode->offset,
                        .precision = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->node.resType.precision,
                        .timeRange = pIntervalPhyNode->timeRange,
                        .firstDayOfWeek = pIntervalPhyNode->firstDayOfWeek,
                        .timezone = pIntervalPhyNode->timezone};
  calcIntervalAutoOffset(&interval);

  pMergeIntervalInfo->groupIntervals = tdListNew(sizeof(SGroupTimeWindow));

  SIntervalAggOperatorInfo* pIntervalInfo = &pMergeIntervalInfo->intervalAggOperatorInfo;
  pIntervalInfo->win = pTaskInfo->window;
  setOptrBasicInfoOrder(&pIntervalInfo->binfo, &pIntervalPhyNode->window.node);
  pIntervalInfo->interval = interval;
  pIntervalInfo->binfo.mergeResultBlock = pIntervalPhyNode->window.mergeDataBlock;
  pIntervalInfo->primaryTsIndex = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->slotId;

  SExprSupp* pExprSupp = &pOperator->exprSupp;
  pExprSupp->hasWindowOrGroup = true;
  pExprSupp->hasWindow = true;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  int32_t    num = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initAggSup(pExprSupp, &pIntervalInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                    NULL, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pIntervalPhyNode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pIntervalInfo->binfo, pResBlock);
  code = initExecTimeWindowInfo(&pIntervalInfo->twAggSup.timeWindowData, &pIntervalInfo->win);
  QUERY_CHECK_CODE(code, lino, _error);

  pIntervalInfo->timeWindowInterpo = false;
  code = timeWindowinterpNeeded(pExprSupp->pCtx, num, pIntervalInfo, &pIntervalInfo->timeWindowInterpo);
  QUERY_CHECK_CODE(code, lino, _error);
  if (pIntervalInfo->timeWindowInterpo) {
    pIntervalInfo->binfo.resultRowInfo.openWindow = tdListNew(sizeof(SOpenWindowInfo));
    if (pIntervalInfo->binfo.resultRowInfo.openWindow == NULL) {
      goto _error;
    }
  }

  pIntervalInfo->pOperator = pOperator;
  initResultRowInfo(&pIntervalInfo->binfo.resultRowInfo);
  setOperatorInfo(pOperator, "TimeMergeIntervalAggOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL, false,
                  OP_NOT_OPENED, pMergeIntervalInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doMergeIntervalAggNext, NULL, destroyMergeIntervalOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetMergeIntervalOperState);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;
_error:
  if (pMergeIntervalInfo != NULL) {
    destroyMergeIntervalOperatorInfo(pMergeIntervalInfo);
  }
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}
