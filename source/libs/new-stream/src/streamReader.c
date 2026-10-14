/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include "streamReader.h"
#include <stdint.h>
#include <tdef.h>
#include "osMemPool.h"
#include "osMemory.h"
#include "streamInt.h"
#include "streamTaskStats.h"
#include "executor.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdef.h"
#include "thash.h"
#include "tsimplehash.h"
#include "tcommon.h"
#include "tmsg.h"

void stReaderCaptureFetchState(SStreamReaderFetchState* pState, const SResFetchReq* pReq, int32_t actualNodeId) {
  *pState = (SStreamReaderFetchState){.enabled = true,
                                      .id = pReq->readerFetchId,
                                      .queryId = pReq->queryId,
                                      .taskId = pReq->taskId,
                                      .execId = pReq->execId,
                                      .nodeId = actualNodeId,
                                      .streamGen = pReq->pStRtFuncInfo->streamGen,
                                      .sessionId = pReq->pStRtFuncInfo->sessionId,
                                      .dynTbname = pReq->dynTbname,
                                      .readerGroupMode = pReq->readerGroupMode,
                                      .readerDimensionBatch = pReq->readerDimensionBatch};
}

int32_t stReaderCheckFetchContinuation(const SStreamTriggerReaderCalcInfo* pCalc, const SResFetchReq* pReq,
                                       int32_t actualNodeId) {
  if (pCalc == NULL || pReq == NULL || pReq->pStRtFuncInfo == NULL) return TSDB_CODE_INVALID_MSG;
  const SStreamReaderFetchState* pState = &pCalc->fetchState;
  const SStreamRuntimeFuncInfo*  pRt = pReq->pStRtFuncInfo;
  if (!pState->enabled || pState->id == 0 || pState->id != pReq->readerFetchId || pState->queryId != pReq->queryId ||
      pState->taskId != pReq->taskId || pState->execId != pReq->execId || pState->nodeId != actualNodeId ||
      pState->streamGen != pRt->streamGen || pState->sessionId != pRt->sessionId ||
      pState->dynTbname != pReq->dynTbname || pState->readerGroupMode != pReq->readerGroupMode ||
      pState->readerDimensionBatch != pReq->readerDimensionBatch ||
      (pReq->readerDimensionBatch && (!pReq->readerGroupMode || pReq->dynTbname)) || !pRt->isMultiGroupCalc ||
      pReq->reset || pReq->reuseReader || pReq->pOpParam != NULL || pRt->pGroupCalcInfos != NULL ||
      pRt->pGroupReadInfos != NULL || pRt->curGrpRead != NULL || pRt->pContextPolicy != NULL ||
      pRt->pAncestorContext != NULL || pReq->pContextFrames != NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  return TSDB_CODE_SUCCESS;
}

void stReaderInvalidateFetchState(SStreamTriggerReaderCalcInfo* pCalc) {
  if (pCalc != NULL) pCalc->fetchState.id = 0;
}

int32_t stReaderBindDimensionGroup(SStreamTriggerReaderCalcInfo* pCalc, uint64_t gid) {
  if (pCalc == NULL || !pCalc->dimensionBatch.installed) return TSDB_CODE_INVALID_PARA;
  SStreamReaderDimensionBatch* pBatch = &pCalc->dimensionBatch;
  SSTriggerGroupCalcInfo*      pGroup = tSimpleHashGet(pBatch->runtime.pGroupCalcInfos, &gid, sizeof(gid));
  if (pGroup == NULL || taosArrayGetSize(pGroup->pParams) == 0) return TSDB_CODE_INVALID_PARA;
  SStreamRuntimeFuncInfo view = pBatch->runtime;
  view.isMultiGroupCalc = false;
  view.pGroupCalcInfos = NULL;
  view.pGroupCalcOrder = NULL;
  view.groupId = gid;
  view.curIdx = 0;
  view.curGrpCalc = pGroup;
  view.pStreamPesudoFuncVals = pGroup->pParams;
  view.pStreamPartColVals = pGroup->pGroupColVals;
  view.createTable = &pGroup->createTable;
  view.rollupTbCount = pGroup->rollupTbCount;
  view.hasPlaceHolder = pCalc->hasPlaceHolder;
  view.pContextPolicy = NULL;
  view.pAncestorContext = NULL;
  int32_t lo = 0, hi = taosArrayGetSize(pBatch->pContextViews);
  while (lo < hi) {
    int32_t                      mid = lo + (hi - lo) / 2;
    SStreamCalcContextGroupView* pView = taosArrayGet(pBatch->pContextViews, mid);
    if ((uint64_t)pView->gid < gid)
      lo = mid + 1;
    else
      hi = mid;
  }
  if (lo < taosArrayGetSize(pBatch->pContextViews)) {
    SStreamCalcContextGroupView* pView = taosArrayGet(pBatch->pContextViews, lo);
    if ((uint64_t)pView->gid == gid) {
      view.pContextPolicy = pView->pContextPolicy;
      view.pAncestorContext = pView->pAncestorContext;
    }
  }
  if (pCalc->requiresContextPolicy && view.pContextPolicy == NULL) return TSDB_CODE_INVALID_PARA;
  pCalc->rtInfo.funcInfo = view;
  return TSDB_CODE_SUCCESS;
}

void stReaderClearDimensionBatch(SStreamTriggerReaderCalcInfo* pCalc) {
  if (pCalc == NULL || !pCalc->dimensionBatch.installed) return;
  // A scan can retain pointers into the current group until reset or destruction.
  qDestroyTask(pCalc->pTaskInfo);
  pCalc->pTaskInfo = NULL;
  memset(&pCalc->rtInfo.funcInfo, 0, sizeof(pCalc->rtInfo.funcInfo));
  tDestroyStreamCalcContextGroupViews(&pCalc->dimensionBatch.pContextViews);
  taosArrayDestroy(pCalc->dimensionBatch.pGroupOrder);
  tDestroyStRtFuncInfo(&pCalc->dimensionBatch.runtime);
  memset(&pCalc->dimensionBatch, 0, sizeof(pCalc->dimensionBatch));
}

int32_t stReaderSetGroupOutputMode(SStreamTriggerReaderCalcInfo* pCalc, const SResFetchReq* pReq) {
  if (pCalc == NULL || pReq == NULL) return TSDB_CODE_INVALID_PARA;
  if (!pReq->reset) {
    if (!pCalc->readerGroupModeSet || pReq->readerGroupMode != pCalc->readerGroupRequested ||
        (pCalc->readerGroupAccepted && pReq->pOpParam != NULL))
      return TSDB_CODE_INVALID_PARA;
    return TSDB_CODE_SUCCESS;
  }
  pCalc->readerGroupModeSet = false;
  pCalc->readerGroupAccepted = false;
  int32_t code = qEnableReaderGroupOutput(pCalc->pTaskInfo, pReq->readerGroupMode && pReq->pOpParam == NULL,
                                          &pCalc->readerGroupAccepted);
  if (code != TSDB_CODE_SUCCESS) return code;
  pCalc->readerGroupRequested = pReq->readerGroupMode;
  pCalc->readerGroupModeSet = true;
  return TSDB_CODE_SUCCESS;
}

#define STREAM_READER_PERIOD_LOG_BUFFER_SIZE 2048
// Six int64 values, one int32, the longest status, eight uint64 values, two rates, ten optional values, and a bool.
#define STREAM_READER_PERIOD_LOG_VALUE_BYTES (6 * 21 + 11 + 10 + 8 * 20 + 2 * 32 + 10 * 32 + 5)
#define STREAM_READER_PERIOD_LOG_FORMAT                                                                              \
  "record=task_period stream_id=%" PRId64 " task_id=%" PRId64 " serious_id=%" PRId64                                 \
  " node_id=%d task_type=reader status=%s stats_start_at=%" PRId64 " uptime_ms=%" PRId64 " stats_window_ms=%" PRId64 \
  " pull_count=%" PRIu64 " success_count=%" PRIu64 " no_data_count=%" PRIu64 " no_context_count=%" PRIu64            \
  " failure_count=%" PRIu64 " data_rows=%" PRIu64 " data_blocks=%" PRIu64                                            \
  " data_rows_per_sec=%.3f data_blocks_per_sec=%.3f"                                                                 \
  " scan_duration_samples=%" PRIu64                                                                                  \
  " scan_duration_avg_ms=%s scan_duration_max_ms=%s scan_duration_lifetime_max_ms=%s"                                \
  " scan_duration_lifetime_max_at=%s last_returned_wal_ver=%s last_success_at=%s"                                    \
  " active_scan_contexts=%s table_count=%s cache_entries=%s reader_shallow_memory_bytes_estimate=%s stats_overflow=%s"

enum {
  STREAM_READER_PERIOD_LOG_BUFFER_CHECK =
      1 / (int)(sizeof(STREAM_READER_PERIOD_LOG_FORMAT) + STREAM_READER_PERIOD_LOG_VALUE_BYTES <=
                STREAM_READER_PERIOD_LOG_BUFFER_SIZE)
};

static bool stReaderAddMemoryBytes(int64_t *pTotal, size_t bytes) {
  if (bytes > INT64_MAX || *pTotal > INT64_MAX - (int64_t)bytes) return false;
  *pTotal += (int64_t)bytes;
  return true;
}

static bool stReaderAddArrayMemory(int64_t *pTotal, const SArray *pArray) {
  if (pArray == NULL) return true;
  if (pArray->elemSize != 0 && pArray->capacity > SIZE_MAX / pArray->elemSize) return false;
  return stReaderAddMemoryBytes(pTotal, sizeof(*pArray) + (size_t)pArray->capacity * pArray->elemSize);
}

static bool stReaderAddBlockMemory(int64_t *pTotal, const SSDataBlock *pBlock) {
  return pBlock == NULL || stReaderAddMemoryBytes(pTotal, blockDataGetMemorySize(pBlock));
}

bool stReaderTaskGetShallowMemoryEstimate(const SStreamReaderTask *pTask, int64_t *pBytes) {
  if (pTask == NULL || pTask->info == NULL || pBytes == NULL ||
      (pTask->task.flags & STREAM_FLAG_REF_EXT_SOURCE) != 0) {
    return false;
  }

  if (pTask->triggerReader != 1) {
    const SArray *pCalcInfos = (const SArray *)pTask->info;
    int64_t       total = 0;
    if (!stReaderAddArrayMemory(&total, pCalcInfos) || pCalcInfos->elemSize != POINTER_BYTES) return false;
    for (size_t i = 0; i < pCalcInfos->size; ++i) {
      const SStreamTriggerReaderCalcInfo *pCalcInfo = *(SStreamTriggerReaderCalcInfo **)TARRAY_GET_ELEM(pCalcInfos, i);
      if (pCalcInfo != NULL && !stReaderAddMemoryBytes(&total, sizeof(*pCalcInfo))) return false;
    }
    *pBytes = total;
    return true;
  }

  const SStreamTriggerReaderInfo *pInfo = (const SStreamTriggerReaderInfo *)pTask->info;
  int64_t total = sizeof(*pInfo);
  const SHashObj *hashes[] = {pInfo->streamTaskMap, pInfo->groupIdMap, pInfo->pTableMetaCacheTrigger,
                              pInfo->pTableMetaCacheCalc, pInfo->triggerTableSchemaMapVTable};
  const SSHashObj *simpleHashes[] = {pInfo->uidHashTrigger, pInfo->uidHashCalc};
  const SArray *arrays[] = {pInfo->tableList.pTableList, pInfo->vSetTableList.pTableList, pInfo->tsSchemas};
  const SSDataBlock *blocks[] = {pInfo->triggerResBlock, pInfo->triggerBlock, pInfo->calcResBlock,
                                 pInfo->calcBlock, pInfo->metaBlock, pInfo->tsBlock};

  for (size_t i = 0; i < sizeof(hashes) / sizeof(hashes[0]); ++i) {
    if (!stReaderAddMemoryBytes(&total, taosHashGetMemSize(hashes[i]))) return false;
  }
  for (size_t i = 0; i < sizeof(simpleHashes) / sizeof(simpleHashes[0]); ++i) {
    if (!stReaderAddMemoryBytes(&total, tSimpleHashGetMemSize(simpleHashes[i]))) return false;
  }
  for (size_t i = 0; i < sizeof(arrays) / sizeof(arrays[0]); ++i) {
    if (!stReaderAddArrayMemory(&total, arrays[i])) return false;
  }
  for (size_t i = 0; i < sizeof(blocks) / sizeof(blocks[0]); ++i) {
    if (!stReaderAddBlockMemory(&total, blocks[i])) return false;
  }
  if (pInfo->vtbCache != NULL) {
    if (!stReaderAddMemoryBytes(&total, sizeof(*pInfo->vtbCache)) ||
        !stReaderAddArrayMemory(&total, pInfo->vtbCache->reqColCids) ||
        !stReaderAddArrayMemory(&total, pInfo->vtbCache->reqTagCids) ||
        !stReaderAddArrayMemory(&total, pInfo->vtbCache->uidSlice) ||
        !stReaderAddMemoryBytes(&total, tSimpleHashGetMemSize(pInfo->vtbCache->uid2Result)) ||
        !stReaderAddMemoryBytes(&total, taosHashGetMemSize(pInfo->vtbCache->dbVgInfo)) ||
        !stReaderAddMemoryBytes(&total, taosHashGetMemSize(pInfo->vtbCache->tblRefCache))) {
      return false;
    }
  }
  *pBytes = total;
  return true;
}

static int32_t stReaderFormatOptionalI64(char* pBuffer, int32_t bufferSize, bool valid, int64_t value) {
  int32_t len = valid ? snprintf(pBuffer, bufferSize, "%" PRId64, value) : snprintf(pBuffer, bufferSize, "NA");
  return len < 0 || len >= bufferSize ? TSDB_CODE_OUT_OF_BUFFER : TSDB_CODE_SUCCESS;
}

static int32_t stReaderFormatOptionalMs(char* pBuffer, int32_t bufferSize, bool valid, double valueMs) {
  int32_t len = valid ? snprintf(pBuffer, bufferSize, "%.3f", valueMs) : snprintf(pBuffer, bufferSize, "NA");
  return len < 0 || len >= bufferSize ? TSDB_CODE_OUT_OF_BUFFER : TSDB_CODE_SUCCESS;
}

int32_t stReaderTaskLogStats(SStreamTask* pTask, const SStreamTaskPeriodSnapshot* pSnapshot) {
  if (pTask == NULL || pSnapshot == NULL || pTask->type != STREAM_READER_TASK ||
      pSnapshot->taskType != STREAM_READER_TASK || pSnapshot->statsWindowMs <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  const SStreamReaderPeriodStats*   pPeriod = &pSnapshot->period.reader;
  const SStreamReaderPeriodStats*   pCumulative = &pSnapshot->cumulative.reader;
  const SStreamReaderGaugeSnapshot* pGauges = &pSnapshot->readerGauges;
  char                              scanAvgMs[32] = {0};
  char                              scanMaxMs[32] = {0};
  char                              scanLifetimeMaxMs[32] = {0};
  char                              scanLifetimeMaxAt[32] = {0};
  char                              lastReturnedWalVer[32] = {0};
  char                              lastSuccessAt[32] = {0};
  char                              activeScanContexts[32] = {0};
  char                              tableCount[32] = {0};
  char                              cacheEntries[32] = {0};
  char                              shallowMemoryBytesEstimate[32] = {0};

  int32_t code = stReaderFormatOptionalMs(
      scanAvgMs, sizeof(scanAvgMs), pPeriod->scanDuration.samples > 0,
      pPeriod->scanDuration.samples > 0
          ? (double)pPeriod->scanDuration.totalUs / (double)pPeriod->scanDuration.samples / 1000.0
          : 0.0);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = stReaderFormatOptionalI64(
      shallowMemoryBytesEstimate, sizeof(shallowMemoryBytesEstimate),
      (pGauges->validMask & STREAM_READER_GAUGE_SHALLOW_MEMORY) != 0, pGauges->shallowMemoryBytesEstimate);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = stReaderFormatOptionalMs(scanMaxMs, sizeof(scanMaxMs), pPeriod->scanDuration.samples > 0,
                                  (double)pPeriod->scanDuration.maxUs / 1000.0);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = stReaderFormatOptionalMs(scanLifetimeMaxMs, sizeof(scanLifetimeMaxMs), pCumulative->scanDuration.samples > 0,
                                  (double)pCumulative->scanDuration.maxUs / 1000.0);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = stReaderFormatOptionalI64(scanLifetimeMaxAt, sizeof(scanLifetimeMaxAt), pCumulative->scanDuration.samples > 0,
                                   pCumulative->scanDuration.maxAtMs);
  if (code != TSDB_CODE_SUCCESS) return code;
  code =
      stReaderFormatOptionalI64(lastReturnedWalVer, sizeof(lastReturnedWalVer),
                                (pGauges->validMask & STREAM_READER_GAUGE_LAST_WAL) != 0, pGauges->lastReturnedWalVer);
  if (code != TSDB_CODE_SUCCESS) return code;
  code =
      stReaderFormatOptionalI64(lastSuccessAt, sizeof(lastSuccessAt),
                                (pGauges->validMask & STREAM_READER_GAUGE_LAST_SUCCESS) != 0, pGauges->lastSuccessAtMs);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = stReaderFormatOptionalI64(activeScanContexts, sizeof(activeScanContexts),
                                   (pGauges->validMask & STREAM_READER_GAUGE_ACTIVE_CONTEXTS) != 0,
                                   pGauges->activeScanContexts);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = stReaderFormatOptionalI64(tableCount, sizeof(tableCount),
                                   (pGauges->validMask & STREAM_READER_GAUGE_TABLE_COUNT) != 0, pGauges->tableCount);
  if (code != TSDB_CODE_SUCCESS) return code;
  code =
      stReaderFormatOptionalI64(cacheEntries, sizeof(cacheEntries),
                                (pGauges->validMask & STREAM_READER_GAUGE_CACHE_ENTRIES) != 0, pGauges->cacheEntries);
  if (code != TSDB_CODE_SUCCESS) return code;

  const double rowsPerSec = (double)pPeriod->dataRows * 1000.0 / (double)pSnapshot->statsWindowMs;
  const double blocksPerSec = (double)pPeriod->dataBlocks * 1000.0 / (double)pSnapshot->statsWindowMs;
  const char*  pStatus = pTask->status >= STREAM_STATUS_UNDEPLOYED && pTask->status <= STREAM_STATUS_DROPPING
                             ? gStreamStatusStr[pTask->status]
                             : "Unknown";
  char         line[STREAM_READER_PERIOD_LOG_BUFFER_SIZE] = {0};
  int32_t      len = snprintf(line, sizeof(line), STREAM_READER_PERIOD_LOG_FORMAT, pTask->streamId, pTask->taskId,
                              pTask->seriousId, pTask->nodeId, pStatus, pSnapshot->statsStartAtMs, pSnapshot->uptimeMs,
                              pSnapshot->statsWindowMs, pPeriod->pullCount, pPeriod->successCount, pPeriod->noDataCount,
                              pPeriod->noContextCount, pPeriod->failureCount, pPeriod->dataRows, pPeriod->dataBlocks,
                              rowsPerSec, blocksPerSec, pPeriod->scanDuration.samples, scanAvgMs, scanMaxMs,
                              scanLifetimeMaxMs, scanLifetimeMaxAt, lastReturnedWalVer, lastSuccessAt, activeScanContexts,
                              tableCount, cacheEntries, shallowMemoryBytesEstimate, pSnapshot->statsOverflow ? "true" : "false");
  if (len < 0 || len >= sizeof(line)) return TSDB_CODE_OUT_OF_BUFFER;

  ST_TASK_DLOG("%s", line);
  return TSDB_CODE_SUCCESS;
}

static const SStreamContextPolicyEntry* stFindReaderContextPolicyEntry(const SStreamContextPolicy* pPolicy, int64_t gid,
                                                                       int32_t paramIndex) {
  int32_t low = 0;
  int32_t high = taosArrayGetSize(pPolicy == NULL ? NULL : pPolicy->pEntries) - 1;
  while (low <= high) {
    const int32_t                    mid = low + (high - low) / 2;
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pPolicy->pEntries, mid);
    if (pEntry == NULL) return NULL;
    if (pEntry->gid == gid && pEntry->paramIndex == paramIndex) return pEntry;
    if (pEntry->gid < gid || (pEntry->gid == gid && pEntry->paramIndex < paramIndex)) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  return NULL;
}

typedef struct {
  int32_t layerIndex;
  int8_t  triggerType;
  int8_t  reserved[3];
  TSKEY   openingTs;
  int64_t nativeDiscriminator;
} SStreamReaderLineageScopeKey;

typedef struct {
  int64_t                      gid;
  int32_t                      scopeCount;
  int32_t                      reserved;
  SStreamReaderLineageScopeKey scopes[STREAM_WINDOW_MAX_LAYERS - 1];
} SStreamReaderLineageKey;

typedef struct {
  int32_t vgId;
  int32_t readInfoIndex;
} SStreamReaderReadKey;

struct SStreamReaderCalcProjection {
  const SStreamRuntimeFuncInfo* pSource;
  int32_t                       actualNodeId;
  SSHashObj*                    pLineageParams;
  SSHashObj*                    pReadBindings;
  SSHashObj*                    pAncestorGids;
};

static int32_t stMakeReaderLineageKey(int64_t gid, const SWindowLineage* pLineage, SStreamReaderLineageKey* pKey) {
  if (pKey == NULL) return TSDB_CODE_INVALID_PARA;
  memset(pKey, 0, sizeof(*pKey));
  pKey->gid = gid;
  pKey->scopeCount = taosArrayGetSize(pLineage == NULL ? NULL : pLineage->pScopes);
  if (pKey->scopeCount <= 0 || pKey->scopeCount >= STREAM_WINDOW_MAX_LAYERS) return TSDB_CODE_INVALID_PARA;
  for (int32_t i = 0; i < pKey->scopeCount; ++i) {
    const SScopeInstanceId* pScope = taosArrayGet(pLineage->pScopes, i);
    if (pScope == NULL) return TSDB_CODE_INVALID_PARA;
    pKey->scopes[i].layerIndex = pScope->layerIndex;
    pKey->scopes[i].triggerType = pScope->triggerType;
    pKey->scopes[i].openingTs = pScope->openingTs;
    pKey->scopes[i].nativeDiscriminator = pScope->nativeDiscriminator;
  }
  return TSDB_CODE_SUCCESS;
}

void stDestroyReaderCalcProjection(SStreamReaderCalcProjection** ppProjection) {
  if (ppProjection == NULL || *ppProjection == NULL) return;
  SStreamReaderCalcProjection* pProjection = *ppProjection;
  tSimpleHashCleanup(pProjection->pLineageParams);
  tSimpleHashCleanup(pProjection->pReadBindings);
  tSimpleHashCleanup(pProjection->pAncestorGids);
  taosMemoryFreeClear(*ppProjection);
}

int32_t stPrepareReaderCalcProjection(const SStreamRuntimeFuncInfo* pSource, int32_t actualNodeId,
                                      SStreamReaderCalcProjection** ppProjection) {
  if (pSource == NULL || ppProjection == NULL || actualNodeId <= 0 || !pSource->isMultiGroupCalc ||
      taosArrayGetSize(pSource->curGrpRead) <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppProjection = NULL;
  const bool sourceHasContext = pSource->pContextPolicy != NULL || pSource->pAncestorContext != NULL;
  int32_t    code = tAdmitStreamContext(pSource->pContextPolicy, pSource->pAncestorContext, sourceHasContext);
  if (code != TSDB_CODE_SUCCESS) return code;

  SStreamReaderCalcProjection* pProjection = taosMemoryCalloc(1, sizeof(*pProjection));
  if (pProjection == NULL) return terrno;
  pProjection->pSource = pSource;
  pProjection->actualNodeId = actualNodeId;
  const int32_t paramCount =
      taosArrayGetSize(pSource->pAncestorContext == NULL ? NULL : pSource->pAncestorContext->pParamContexts);
  const int32_t bindingCount =
      taosArrayGetSize(pSource->pAncestorContext == NULL ? NULL : pSource->pAncestorContext->pReadScopeBindings);
  pProjection->pLineageParams = tSimpleHashInit(TMAX(paramCount, 1), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  pProjection->pReadBindings =
      tSimpleHashInit(TMAX(bindingCount, 1), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  pProjection->pAncestorGids = tSimpleHashInit(TMAX(paramCount, 1), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (pProjection->pLineageParams == NULL || pProjection->pReadBindings == NULL || pProjection->pAncestorGids == NULL) {
    code = terrno;
    goto _exit;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pSource->pContextPolicy == NULL ? NULL : pSource->pContextPolicy->pEntries);
       ++i) {
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pSource->pContextPolicy->pEntries, i);
    if (pEntry->contextPolicy != STREAM_CONTEXT_POLICY_ANCESTOR) continue;
    const bool present = true;
    code = tSimpleHashPut(pProjection->pAncestorGids, &pEntry->gid, sizeof(pEntry->gid), &present, sizeof(present));
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_DUP_KEY) goto _exit;
  }

  for (int32_t i = 0; i < paramCount; ++i) {
    const SStreamAncestorParamContext* pParam = taosArrayGet(pSource->pAncestorContext->pParamContexts, i);
    SStreamReaderLineageKey            key = {0};
    code = stMakeReaderLineageKey(pParam->leafIdentity.gid, &pParam->leafIdentity.lineage, &key);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    int32_t* pCurrent = tSimpleHashGet(pProjection->pLineageParams, &key, sizeof(key));
    if (pCurrent == NULL) {
      code = tSimpleHashPut(pProjection->pLineageParams, &key, sizeof(key), &i, sizeof(i));
      if (code != TSDB_CODE_SUCCESS) goto _exit;
    } else {
      const SStreamAncestorParamContext* pExisting = taosArrayGet(pSource->pAncestorContext->pParamContexts, *pCurrent);
      if (pExisting == NULL) {
        code = TSDB_CODE_INVALID_PARA;
        goto _exit;
      }
      if (pParam->paramIndex < pExisting->paramIndex) *pCurrent = i;
    }
  }

  for (int32_t i = 0; i < bindingCount; ++i) {
    const SStreamReadScopeBinding* pBinding = taosArrayGet(pSource->pAncestorContext->pReadScopeBindings, i);
    SStreamReaderReadKey           key = {.vgId = pBinding->vgId, .readInfoIndex = pBinding->readInfoIndex};
    code = tSimpleHashPut(pProjection->pReadBindings, &key, sizeof(key), &i, sizeof(i));
    if (code != TSDB_CODE_SUCCESS) goto _exit;
  }

  *ppProjection = pProjection;
  return TSDB_CODE_SUCCESS;

_exit:
  stDestroyReaderCalcProjection(&pProjection);
  return code;
}

static int32_t stCloneReaderProjectedContext(const SStreamAncestorParamContext* pParam,
                                             const SStreamReadScopeBinding*     pBinding,
                                             SStreamAncestorContext**           ppProjected) {
  SStreamAncestorContext selected = {0};
  selected.pParamContexts = taosArrayInit(1, sizeof(SStreamAncestorParamContext));
  selected.pReadScopeBindings = taosArrayInit(1, sizeof(SStreamReadScopeBinding));
  if (selected.pParamContexts == NULL || selected.pReadScopeBindings == NULL) {
    taosArrayDestroy(selected.pParamContexts);
    taosArrayDestroy(selected.pReadScopeBindings);
    return terrno;
  }
  SStreamAncestorParamContext projectedParam = *pParam;
  projectedParam.paramIndex = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  if (taosArrayPush(selected.pParamContexts, &projectedParam) == NULL ||
      taosArrayPush(selected.pReadScopeBindings, pBinding) == NULL) {
    code = terrno;
  } else {
    code = tCloneStreamAncestorContext(&selected, ppProjected);
  }
  taosArrayDestroy(selected.pParamContexts);
  taosArrayDestroy(selected.pReadScopeBindings);
  return code;
}

int32_t stProjectPreparedReaderCalcContext(const SStreamReaderCalcProjection* pProjection, int32_t readInfoIndex,
                                           SStreamRuntimeFuncInfo* pTarget) {
  if (pProjection == NULL || pTarget == NULL || readInfoIndex < 0 ||
      taosArrayGetSize(pTarget->pStreamPesudoFuncVals) != 1) {
    return TSDB_CODE_INVALID_PARA;
  }
  const SStreamRuntimeFuncInfo* pSource = pProjection->pSource;
  const SSTriggerGroupReadInfo* pReadInfo = taosArrayGet(pSource->curGrpRead, readInfoIndex);
  if (pReadInfo == NULL) return TSDB_CODE_INVALID_PARA;

  SStreamReaderReadKey readKey = {.vgId = pProjection->actualNodeId, .readInfoIndex = readInfoIndex};
  const int32_t*       pBindingIndex = tSimpleHashGet(pProjection->pReadBindings, &readKey, sizeof(readKey));
  const bool hasAncestor = tSimpleHashGet(pProjection->pAncestorGids, &pReadInfo->gid, sizeof(pReadInfo->gid)) != NULL;
  if (pBindingIndex == NULL && hasAncestor) return TSDB_CODE_INVALID_PARA;

  const SStreamContextPolicyEntry* pSourceEntry = NULL;
  SStreamAncestorContext*          pProjectedContext = NULL;
  int32_t                          code = TSDB_CODE_SUCCESS;
  if (pBindingIndex != NULL) {
    const SStreamReadScopeBinding* pBinding =
        taosArrayGet(pSource->pAncestorContext->pReadScopeBindings, *pBindingIndex);
    if (pBinding == NULL || pBinding->scope.gid != pReadInfo->gid) return TSDB_CODE_INVALID_PARA;
    SStreamReaderLineageKey lineageKey = {0};
    code = stMakeReaderLineageKey(pBinding->scope.gid, &pBinding->scope.lineage, &lineageKey);
    if (code != TSDB_CODE_SUCCESS) return code;
    const int32_t* pParamIndex = tSimpleHashGet(pProjection->pLineageParams, &lineageKey, sizeof(lineageKey));
    const SStreamAncestorParamContext* pParam =
        pParamIndex == NULL ? NULL : taosArrayGet(pSource->pAncestorContext->pParamContexts, *pParamIndex);
    if (pParam == NULL) return TSDB_CODE_INVALID_PARA;
    pSourceEntry = stFindReaderContextPolicyEntry(pSource->pContextPolicy, pReadInfo->gid, pParam->paramIndex);
    if (pSourceEntry == NULL || pSourceEntry->contextPolicy != STREAM_CONTEXT_POLICY_ANCESTOR) {
      return TSDB_CODE_INVALID_PARA;
    }
    code = stCloneReaderProjectedContext(pParam, pBinding, &pProjectedContext);
    if (code != TSDB_CODE_SUCCESS) return code;
  }

  SStreamContextPolicy* pProjectedPolicy = NULL;
  if (pSourceEntry != NULL) {
    pProjectedPolicy = taosMemoryCalloc(1, sizeof(*pProjectedPolicy));
    if (pProjectedPolicy == NULL) {
      code = terrno;
      goto _exit;
    }
    pProjectedPolicy->pEntries = taosArrayInit(1, sizeof(SStreamContextPolicyEntry));
    if (pProjectedPolicy->pEntries == NULL) {
      code = terrno;
      goto _exit;
    }
    SStreamContextPolicyEntry projectedEntry = *pSourceEntry;
    projectedEntry.paramIndex = 0;
    if (taosArrayPush(pProjectedPolicy->pEntries, &projectedEntry) == NULL) {
      code = terrno;
      goto _exit;
    }
    code = tAdmitStreamContext(pProjectedPolicy, pProjectedContext, true);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
  }

  tDestroyStreamContextPolicy(&pTarget->pContextPolicy);
  tDestroyStreamAncestorContext(&pTarget->pAncestorContext);
  pTarget->isMultiGroupCalc = false;
  pTarget->groupId = pReadInfo->gid;
  pTarget->curIdx = 0;
  pTarget->addOptions = pSource->addOptions;
  pTarget->pContextPolicy = pProjectedPolicy;
  pProjectedPolicy = NULL;
  pTarget->pAncestorContext = pProjectedContext;
  pProjectedContext = NULL;

_exit:
  tDestroyStreamContextPolicy(&pProjectedPolicy);
  tDestroyStreamAncestorContext(&pProjectedContext);
  return code;
}

int32_t stProjectReaderCalcContext(const SStreamRuntimeFuncInfo* pSource, int32_t actualNodeId, int32_t readInfoIndex,
                                   int32_t sourceParamIndex, SStreamRuntimeFuncInfo* pTarget) {
  if (pSource == NULL || pTarget == NULL || taosArrayGetSize(pTarget->pStreamPesudoFuncVals) != 1) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pSource->isMultiGroupCalc) {
    if (sourceParamIndex >= 0) return TSDB_CODE_INVALID_PARA;
    SStreamReaderCalcProjection* pProjection = NULL;
    int32_t                      code = stPrepareReaderCalcProjection(pSource, actualNodeId, &pProjection);
    if (code == TSDB_CODE_SUCCESS) {
      code = stProjectPreparedReaderCalcContext(pProjection, readInfoIndex, pTarget);
    }
    stDestroyReaderCalcProjection(&pProjection);
    return code;
  }

  const bool sourceHasContext = pSource->pContextPolicy != NULL || pSource->pAncestorContext != NULL;
  int32_t    code = tAdmitStreamContext(pSource->pContextPolicy, pSource->pAncestorContext, sourceHasContext);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int64_t                          gid = pSource->groupId;
  const SStreamContextPolicyEntry* pSourceEntry = NULL;
  SStreamAncestorContext*          pProjectedContext = NULL;
  if (!pSource->isMultiGroupCalc) {
    if (readInfoIndex >= 0 || sourceParamIndex < 0 ||
        sourceParamIndex >= taosArrayGetSize(pSource->pStreamPesudoFuncVals)) {
      return TSDB_CODE_INVALID_PARA;
    }
    if (pSource->pContextPolicy != NULL) {
      pSourceEntry = stFindReaderContextPolicyEntry(pSource->pContextPolicy, gid, sourceParamIndex);
      if (pSourceEntry == NULL) {
        return TSDB_CODE_INVALID_PARA;
      }
      if (pSourceEntry->contextPolicy == STREAM_CONTEXT_POLICY_ANCESTOR) {
        code = tProjectStreamAncestorContext(pSource->pAncestorContext, gid, sourceParamIndex, 0, &pProjectedContext);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
    }
  }

  SStreamContextPolicy* pProjectedPolicy = NULL;
  if (pSourceEntry != NULL) {
    pProjectedPolicy = taosMemoryCalloc(1, sizeof(*pProjectedPolicy));
    if (pProjectedPolicy == NULL) {
      code = terrno;
      goto _exit;
    }
    pProjectedPolicy->pEntries = taosArrayInit(1, sizeof(SStreamContextPolicyEntry));
    if (pProjectedPolicy->pEntries == NULL) {
      code = terrno;
      goto _exit;
    }
    SStreamContextPolicyEntry projectedEntry = *pSourceEntry;
    projectedEntry.paramIndex = 0;
    if (taosArrayPush(pProjectedPolicy->pEntries, &projectedEntry) == NULL) {
      code = terrno;
      goto _exit;
    }
    code = tAdmitStreamContext(pProjectedPolicy, pProjectedContext, true);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }
  }

  tDestroyStreamContextPolicy(&pTarget->pContextPolicy);
  tDestroyStreamAncestorContext(&pTarget->pAncestorContext);
  pTarget->isMultiGroupCalc = false;
  pTarget->groupId = gid;
  pTarget->curIdx = 0;
  pTarget->addOptions = pSource->addOptions;
  pTarget->pContextPolicy = pProjectedPolicy;
  pProjectedPolicy = NULL;
  pTarget->pAncestorContext = pProjectedContext;
  pProjectedContext = NULL;

_exit:
  tDestroyStreamContextPolicy(&pProjectedPolicy);
  tDestroyStreamAncestorContext(&pProjectedContext);
  return code;
}

int32_t stValidateNodeGroupReadBindings(const SStreamRuntimeFuncInfo* pSource, int32_t actualNodeId) {
  if (pSource == NULL || actualNodeId <= 0 || !pSource->isMultiGroupCalc ||
      taosArrayGetSize(pSource->curGrpRead) <= 0 || pSource->pGroupCalcInfos == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  const bool sourceHasContext = pSource->pContextPolicy != NULL || pSource->pAncestorContext != NULL;
  int32_t    code = tAdmitStreamContext(pSource->pContextPolicy, pSource->pAncestorContext, sourceHasContext);
  if (code != TSDB_CODE_SUCCESS) return code;

  SSHashObj*                    pBorrowedNodeReads = NULL;
  SSHashObj*                    pAncestorGids = NULL;
  SSHashObj*                    pGroupReadInfos = pSource->pGroupReadInfos;
  SArray*                       pNodeBindings = NULL;
  SStreamAncestorContext        nodeContext = {0};
  const SStreamAncestorContext* pValidationContext = pSource->pAncestorContext;
  if (pGroupReadInfos == NULL) {
    pBorrowedNodeReads = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    if (pBorrowedNodeReads == NULL) return terrno;
    code = tSimpleHashPut(pBorrowedNodeReads, &actualNodeId, sizeof(actualNodeId), &pSource->curGrpRead, POINTER_BYTES);
    if (code != TSDB_CODE_SUCCESS) {
      tSimpleHashCleanup(pBorrowedNodeReads);
      return code;
    }
    pGroupReadInfos = pBorrowedNodeReads;
    if (pSource->pAncestorContext != NULL) {
      nodeContext = *pSource->pAncestorContext;
      nodeContext.pParamIndex = NULL;
      const int32_t bindingCount = taosArrayGetSize(pSource->pAncestorContext->pReadScopeBindings);
      pNodeBindings = taosArrayInit(bindingCount, sizeof(SStreamReadScopeBinding));
      if (pNodeBindings == NULL) {
        code = terrno;
        goto _exit;
      }
      for (int32_t i = 0; i < bindingCount; ++i) {
        const SStreamReadScopeBinding* pBinding = taosArrayGet(pSource->pAncestorContext->pReadScopeBindings, i);
        if (pBinding != NULL && pBinding->vgId == actualNodeId && taosArrayPush(pNodeBindings, pBinding) == NULL) {
          code = terrno;
          goto _exit;
        }
      }
      nodeContext.pReadScopeBindings = pNodeBindings;
      pValidationContext = &nodeContext;
      if (taosArrayGetSize(pNodeBindings) == 0) {
        const int32_t policyCount = taosArrayGetSize(pSource->pContextPolicy->pEntries);
        pAncestorGids = tSimpleHashInit(policyCount, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
        if (pAncestorGids == NULL) {
          code = terrno;
          goto _exit;
        }
        const bool present = true;
        for (int32_t i = 0; i < policyCount; ++i) {
          const SStreamContextPolicyEntry* pEntry = taosArrayGet(pSource->pContextPolicy->pEntries, i);
          if (pEntry != NULL && pEntry->contextPolicy == STREAM_CONTEXT_POLICY_ANCESTOR &&
              tSimpleHashGet(pAncestorGids, &pEntry->gid, sizeof(pEntry->gid)) == NULL) {
            code = tSimpleHashPut(pAncestorGids, &pEntry->gid, sizeof(pEntry->gid), &present, sizeof(present));
            if (code != TSDB_CODE_SUCCESS) goto _exit;
          }
        }
        for (int32_t i = 0; i < taosArrayGetSize(pSource->curGrpRead); ++i) {
          const SSTriggerGroupReadInfo* pRead = taosArrayGet(pSource->curGrpRead, i);
          if (pRead != NULL && tSimpleHashGet(pAncestorGids, &pRead->gid, sizeof(pRead->gid)) != NULL) {
            code = TSDB_CODE_INVALID_PARA;
            goto _exit;
          }
        }
      }
    }
  }

  if (pGroupReadInfos != NULL) {
    SSTriggerCalcRequest request = {
        .isMultiGroupCalc = 1,
        .pGroupCalcInfos = pSource->pGroupCalcInfos,
        .pGroupReadInfos = pGroupReadInfos,
        .pContextPolicy = pSource->pContextPolicy,
        .pAncestorContext = (SStreamAncestorContext*)pValidationContext,
    };
    code = tValidateSTriggerCalcRequest(&request);
    if (code != TSDB_CODE_SUCCESS) goto _exit;

    if (pSource->pGroupReadInfos == NULL) {
      code = TSDB_CODE_SUCCESS;
      goto _exit;
    }
    if (pSource->curNodeId != actualNodeId) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
    SArray** ppNodeReads = tSimpleHashGet(pSource->pGroupReadInfos, &actualNodeId, sizeof(actualNodeId));
    if (ppNodeReads == NULL || *ppNodeReads == NULL ||
        taosArrayGetSize(*ppNodeReads) != taosArrayGetSize(pSource->curGrpRead)) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
    for (int32_t readInfoIndex = 0; readInfoIndex < taosArrayGetSize(pSource->curGrpRead); ++readInfoIndex) {
      const SSTriggerGroupReadInfo* pRead = taosArrayGet(pSource->curGrpRead, readInfoIndex);
      const SSTriggerGroupReadInfo* pSourceRead = taosArrayGet(*ppNodeReads, readInfoIndex);
      if (pRead == NULL || pSourceRead == NULL || pSourceRead->gid != pRead->gid ||
          memcmp(&pSourceRead->firstParam, &pRead->firstParam, offsetof(SSTriggerCalcParam, notifyType)) != 0 ||
          memcmp(&pSourceRead->lastParam, &pRead->lastParam, offsetof(SSTriggerCalcParam, notifyType)) != 0) {
        code = TSDB_CODE_INVALID_PARA;
        goto _exit;
      }
    }
    code = TSDB_CODE_SUCCESS;
    goto _exit;
  }

_exit:
  tSimpleHashCleanup(pAncestorGids);
  taosArrayDestroy(pNodeBindings);
  tSimpleHashCleanup(pBorrowedNodeReads);
  return code;
}

static void freeUidMapElementList(void* pData) {
  if (pData == NULL) return;
  SArray* elements = *(SArray**)pData;
  taosArrayDestroy(elements);
}

void qStreamDestroyTableInfo(StreamTableListInfo* pTableListInfo) { 
  if (pTableListInfo == NULL) return;
  taosArrayDestroyP(pTableListInfo->pTableList, taosMemFree);
  pTableListInfo->pTableList = NULL;
  taosHashCancelIterate(pTableListInfo->gIdMap, pTableListInfo->pIter);
  taosHashCleanup(pTableListInfo->gIdMap);
  stDebug("release gIdMap:%p", pTableListInfo->gIdMap);
  pTableListInfo->pIter = NULL;
  pTableListInfo->gIdMap = NULL;
  taosHashCleanup(pTableListInfo->uIdMap);
  pTableListInfo->uIdMap = NULL;
  pTableListInfo->uIdMapMode = UIDMAP_SINGLE;
}

void qStreamClearTableInfo(StreamTableListInfo* pTableListInfo){
  if (pTableListInfo->pTableList) {
    taosArrayClearP(pTableListInfo->pTableList, taosMemFree);
  }

  if (pTableListInfo->gIdMap) {
    taosHashCancelIterate(pTableListInfo->gIdMap, pTableListInfo->pIter);
    taosHashClear(pTableListInfo->gIdMap);
    pTableListInfo->pIter = NULL;
  }

  if (pTableListInfo->uIdMap) {
    taosHashClear(pTableListInfo->uIdMap);
  }
}

static int32_t removeList(SHashObj* idMap, SStreamTableKeyInfo* table, uint64_t key){
  int32_t code = 0;
  int32_t lino = 0;
  SStreamTableList* list = taosHashGet(idMap, &key, LONG_BYTES);
  if (list == NULL) {
    stError("stream reader remove table list failed, groupId does not exist, key:%"PRIu64, key);
    code = TSDB_CODE_NOT_FOUND;
    goto end;
  } 
  if (list->head == table && list->tail == table) {
    // only one element
    list->head = NULL;
    list->tail = NULL;
    list->size = 0;
    code = taosHashRemove(idMap, &key, LONG_BYTES);
    if (code != 0) {
      stError("stream reader remove table list failed, remove groupId failed, key:%"PRIu64, key);
      goto end;
    }
  } else if (list->head == table) {
    // first element
    list->head = table->next;
    list->head->prev = NULL;
    list->size -= 1;
  } else if (list->tail == table) {
    // last element
    list->tail = table->prev;
    list->tail->next = NULL;
    list->size -= 1;
  } else {
    // middle element
    table->prev->next = table->next;
    table->next->prev = table->prev;
    list->size -= 1;
  }
end:
  return code;
}

static int32_t addList(SHashObj* idMap, SStreamTableKeyInfo* table, uint64_t key){
  int32_t code = 0;
  int32_t lino = 0;

  SStreamTableList* list = taosHashGet(idMap, &key, LONG_BYTES);
  if (list == NULL) {
    SStreamTableList tmp  = {.head = table, .tail = table, .size = 1};
    STREAM_CHECK_RET_GOTO(taosHashPut(idMap, &key, LONG_BYTES, &tmp, sizeof(SStreamTableList)));
  } else {
    list->tail->next = table;
    table->prev = list->tail;
    list->tail = table;
    list->size += 1;
  }

end:
  return code;
}

int32_t initStreamTableListInfo(StreamTableListInfo* pTableListInfo, EUidMapMode uIdMapMode) {
  int32_t                   code = 0;
  int32_t                   lino = 0;
  if (pTableListInfo->uIdMap != NULL) {
    STREAM_CHECK_CONDITION_GOTO(pTableListInfo->uIdMapMode != uIdMapMode, TSDB_CODE_INVALID_PARA);
  } else {
    pTableListInfo->uIdMapMode = uIdMapMode;
  }

  if (pTableListInfo->pTableList == NULL) {
    pTableListInfo->pTableList = taosArrayInit(4, POINTER_BYTES);
    STREAM_CHECK_NULL_GOTO(pTableListInfo->pTableList, terrno);
  }
  if (pTableListInfo->gIdMap == NULL) {
    pTableListInfo->gIdMap = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    STREAM_CHECK_NULL_GOTO(pTableListInfo->gIdMap, terrno);
  }
  if (pTableListInfo->uIdMap == NULL) {
    pTableListInfo->uIdMap = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    STREAM_CHECK_NULL_GOTO(pTableListInfo->uIdMap, terrno);
    if (pTableListInfo->uIdMapMode == UIDMAP_MULTI) {
      taosHashSetFreeFp(pTableListInfo->uIdMap, freeUidMapElementList);
    }
  }

end:
  return code;
}

int32_t  qStreamSetTableList(StreamTableListInfo* pTableListInfo, int64_t uid, uint64_t gid){
  int32_t code = 0;
  int32_t lino = 0;

  stDebug("stream reader set table list, uid:%"PRIu64", gid:%"PRIu64, uid, gid);
  STREAM_CHECK_RET_GOTO(initStreamTableListInfo(pTableListInfo, pTableListInfo->uIdMapMode));
  SStreamTableKeyInfo* keyInfo = taosMemoryCalloc(1, sizeof(SStreamTableKeyInfo));
  STREAM_CHECK_NULL_GOTO(keyInfo, terrno);
  *keyInfo = (SStreamTableKeyInfo){.uid = uid, .groupId = gid, .markedDeleted = false, .prev = NULL, .next = NULL};
  if (taosArrayPush(pTableListInfo->pTableList, &keyInfo) == NULL) {
    taosMemoryFreeClear(keyInfo);
    code = terrno;
    goto end;
  }

  STREAM_CHECK_RET_GOTO(addList(pTableListInfo->gIdMap, keyInfo, gid));

  SStreamTableMapElement element = {.table = keyInfo, .index = taosArrayGetSize(pTableListInfo->pTableList) - 1};
  if (pTableListInfo->uIdMapMode == UIDMAP_MULTI) {
    SArray** pElements = taosHashGet(pTableListInfo->uIdMap, &uid, LONG_BYTES);
    if (pElements == NULL) {
      SArray* elements = taosArrayInit(1, sizeof(SStreamTableMapElement));
      STREAM_CHECK_NULL_GOTO(elements, terrno);
      if (taosArrayPush(elements, &element) == NULL) {
        code = terrno;
        taosArrayDestroy(elements);
        goto end;
      }
      STREAM_CHECK_RET_GOTO(taosHashPut(pTableListInfo->uIdMap, &uid, LONG_BYTES, &elements, sizeof(elements)));
    } else {
      STREAM_CHECK_NULL_GOTO(taosArrayPush(*pElements, &element), terrno);
    }
  } else {
    STREAM_CHECK_RET_GOTO(taosHashPut(pTableListInfo->uIdMap, &uid, LONG_BYTES, &element, sizeof(element)));
  }

end:
  return code;
}

int32_t  qStreamRemoveTableList(StreamTableListInfo* pTableListInfo, int64_t uid){
  int32_t code = 0;
  int32_t lino = 0;

  STREAM_CHECK_NULL_GOTO(pTableListInfo->pTableList, terrno);
  STREAM_CHECK_NULL_GOTO(pTableListInfo->gIdMap, terrno);
  STREAM_CHECK_NULL_GOTO(pTableListInfo->uIdMap, terrno);
  if (pTableListInfo->uIdMapMode == UIDMAP_MULTI) {
    SArray** pElements = taosHashGet(pTableListInfo->uIdMap, &uid, LONG_BYTES);
    if (pElements == NULL) {
      goto end;
    }

    int32_t numOfElements = taosArrayGetSize(*pElements);
    for (int32_t i = 0; i < numOfElements; ++i) {
      SStreamTableMapElement* info = taosArrayGet(*pElements, i);
      STREAM_CHECK_NULL_GOTO(info, terrno);
      STREAM_CHECK_RET_GOTO(removeList(pTableListInfo->gIdMap, info->table, info->table->groupId));
      SStreamTableKeyInfo* tmp = taosArrayGetP(pTableListInfo->pTableList, info->index);
      if (tmp != NULL) {
        tmp->markedDeleted = true;
      }
    }
  } else {
    SStreamTableMapElement* info = taosHashGet(pTableListInfo->uIdMap, &uid, LONG_BYTES);
    if (info == NULL) {
      goto end;
    }

    STREAM_CHECK_RET_GOTO(removeList(pTableListInfo->gIdMap, info->table, info->table->groupId));

    SStreamTableKeyInfo* tmp = taosArrayGetP(pTableListInfo->pTableList, info->index);
    if (tmp != NULL) {
      tmp->markedDeleted = true;
    }
  }
  code = taosHashRemove(pTableListInfo->uIdMap, &uid, LONG_BYTES);
  
end:
  return code;
}

static void* copyTableInfo(void* p) {
  SStreamTableKeyInfo* src = (SStreamTableKeyInfo*)p;
  SStreamTableKeyInfo* dst = taosMemoryMalloc(sizeof(SStreamTableKeyInfo));
  if (dst != NULL) {
    *dst = *src;
    dst->prev = NULL;
    dst->next = NULL;
  }
  return dst;
}

static bool uidMapHasTable(StreamTableListInfo* pTableListInfo, SStreamTableKeyInfo* table) {
  if (pTableListInfo->uIdMapMode == UIDMAP_MULTI) {
    SArray** pElements = taosHashGet(pTableListInfo->uIdMap, &table->uid, LONG_BYTES);
    if (pElements == NULL) {
      return false;
    }

    int32_t numOfElements = taosArrayGetSize(*pElements);
    for (int32_t i = 0; i < numOfElements; ++i) {
      SStreamTableMapElement* element = taosArrayGet(*pElements, i);
      if (element != NULL && element->table == table) {
        return true;
      }
    }
    return false;
  }

  SStreamTableMapElement* element = taosHashGet(pTableListInfo->uIdMap, &table->uid, LONG_BYTES);
  return element != NULL && element->table == table;
}

int32_t  qStreamCopyTableInfo(SStreamTriggerReaderInfo* sStreamReaderInfo, StreamTableListInfo* dst){
  int32_t code = 0;
  int32_t lino = 0;
  taosRLockLatch(&sStreamReaderInfo->lock);
  StreamTableListInfo* src = sStreamReaderInfo->isVtableStream ? &sStreamReaderInfo->vSetTableList : &sStreamReaderInfo->tableList;
  STREAM_CHECK_RET_GOTO(initStreamTableListInfo(dst, src->uIdMapMode));
  int32_t totalSize = taosArrayGetSize(src->pTableList);
  for (int32_t i = 0; i < totalSize; ++i) {
    SStreamTableKeyInfo* info = taosArrayGetP(src->pTableList, i);
    if (info == NULL) {
      continue;
    }
    if (info->markedDeleted || !uidMapHasTable(src, info)) {
      continue;
    }
    STREAM_CHECK_RET_GOTO(qStreamSetTableList(dst, info->uid, info->groupId));
  }
end:
   taosRUnLockLatch(&sStreamReaderInfo->lock);
  return code;
}

SArray* qStreamGetTableArrayList(SStreamTriggerReaderInfo* sStreamReaderInfo) { 
  taosRLockLatch(&sStreamReaderInfo->lock);
  SArray* pTableList = taosArrayDup(sStreamReaderInfo->tableList.pTableList, copyTableInfo);
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return pTableList;
}

int32_t  qStreamGetTableListNum(SStreamTriggerReaderInfo* sStreamReaderInfo){
  taosRLockLatch(&sStreamReaderInfo->lock);
  StreamTableListInfo* tmp = sStreamReaderInfo->isVtableStream ? &sStreamReaderInfo->vSetTableList : &sStreamReaderInfo->tableList;
  int32_t num = taosArrayGetSize(tmp->pTableList);
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return num;
}

int32_t  qStreamGetTableListGroupNum(SStreamTriggerReaderInfo* sStreamReaderInfo){
  taosRLockLatch(&sStreamReaderInfo->lock);
  StreamTableListInfo* tmp = sStreamReaderInfo->isVtableStream ? &sStreamReaderInfo->vSetTableList : &sStreamReaderInfo->tableList;
  int32_t num = taosHashGetSize(tmp->gIdMap);
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return num;
}

bool isRollupMultiReader(SStreamTriggerReaderInfo* sStreamReaderInfo) {
  return sStreamReaderInfo->isRollupReader && !sStreamReaderInfo->isVtableStream;
}

int32_t qStreamGetGroupTableCount(SStreamTriggerReaderInfo* sStreamReaderInfo, uint64_t gid) {
  int32_t num = 0;

  taosRLockLatch(&sStreamReaderInfo->lock);
  if (isRollupMultiReader(sStreamReaderInfo)) {
    SStreamTableList* list = taosHashGet(sStreamReaderInfo->tableList.gIdMap, &gid, LONG_BYTES);
    if (list != NULL) {
      num = list->size;
    }
  }
  taosRUnLockLatch(&sStreamReaderInfo->lock);

  return num;
}

static uint64_t qStreamGetGroupId(StreamTableListInfo* tmp, int64_t uid){
  uint64_t groupId = -1;
  if (tmp->uIdMapMode == UIDMAP_MULTI) {
    SArray** pElements = taosHashGet(tmp->uIdMap, &uid, LONG_BYTES);
    if (pElements != NULL) {
      int32_t numOfElements = taosArrayGetSize(*pElements);
      for (int32_t i = 0; i < numOfElements; ++i) {
        SStreamTableMapElement* element = taosArrayGet(*pElements, i);
        if (element == NULL || element->table == NULL || element->table->markedDeleted) {
          continue;
        }
        groupId = element->table->groupId;
        break;
      }
    }
  } else {
    SStreamTableMapElement* info = taosHashGet(tmp->uIdMap, &uid, LONG_BYTES);
    if (info != NULL) {
      groupId = info->table->groupId;
    }
  }
  return groupId;
}

uint64_t qStreamGetGroupIdFromOrigin(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t uid){
  StreamTableListInfo* tmp = &sStreamReaderInfo->tableList;
  uint64_t groupId = qStreamGetGroupId(tmp, uid);
  return groupId;
}

uint64_t qStreamGetGroupIdFromSet(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t uid){
  uint64_t groupId = uid;
  taosRLockLatch(&sStreamReaderInfo->lock);
  if (!sStreamReaderInfo->isVtableStream){
    groupId = qStreamGetGroupId(&sStreamReaderInfo->tableList, uid);
  }
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return groupId;
}

static int32_t buildTableListFromList(STableKeyInfo** pKeyInfo, int32_t* size, SStreamTableList* list){
  *size = list->size;
  *pKeyInfo = taosMemoryCalloc(*size, sizeof(STableKeyInfo));
  if (*pKeyInfo == NULL) {
    return terrno;
  }
  SStreamTableKeyInfo* iter = list->head;
  STableKeyInfo* kInfo = *pKeyInfo;
  while (iter != NULL) {
    stDebug("stream reader get table list, uid:%"PRIu64", gid:%"PRIu64, iter->uid, iter->groupId);
    kInfo->uid = iter->uid;
    kInfo->groupId = iter->groupId;
    iter = iter->next;
    kInfo++;
  }
  return 0;
}

static int32_t buildTableListFromArray(STableKeyInfo** pKeyInfo, int32_t* size, SArray* pTableList){
  int32_t totalSize = taosArrayGetSize(pTableList);
  *size = totalSize;
  *pKeyInfo = taosMemoryCalloc(*size, sizeof(STableKeyInfo));
  if (*pKeyInfo == NULL) {
    return terrno;
  }
  STableKeyInfo* kInfo = *pKeyInfo;
  for (int32_t i = 0; i < totalSize; ++i) {
    SStreamTableKeyInfo* info = taosArrayGetP(pTableList, i);
    if (info == NULL || info->markedDeleted) {
      continue;
    }
    kInfo->uid = info->uid;
    kInfo->groupId = info->groupId;
    kInfo++;
  }
  return 0;
}

int32_t qStreamGetTableList(SStreamTriggerReaderInfo* sStreamReaderInfo, uint64_t gid, STableKeyInfo** pKeyInfo, int32_t* size) {
  int32_t      code = 0;
  int32_t      lino = 0;
  if (pKeyInfo == NULL || size == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  void* pTask = sStreamReaderInfo->pTask;
  *size = 0;
  *pKeyInfo = NULL;
  taosRLockLatch(&sStreamReaderInfo->lock);
  StreamTableListInfo* tmp = sStreamReaderInfo->isVtableStream ? &sStreamReaderInfo->vSetTableList : &sStreamReaderInfo->tableList;
  if (gid == 0) {   // return all tables
    STREAM_CHECK_RET_GOTO(buildTableListFromArray(pKeyInfo, size, tmp->pTableList));
    goto end;
  }
  SStreamTableList* list = taosHashGet(tmp->gIdMap, &gid, LONG_BYTES);
  if (list == NULL) {
    ST_TASK_DLOG("%s not found gid:%"PRId64, __func__, gid);
    goto end;
  }

  STREAM_CHECK_RET_GOTO(buildTableListFromList(pKeyInfo, size, list));
end:
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return code;
}

int32_t qStreamIterTableList(StreamTableListInfo* tableInfo, STableKeyInfo** pKeyInfo, int32_t* size, int64_t* suid) {
  int32_t      code = 0;
  int32_t      lino = 0;
  if (pKeyInfo == NULL || size == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *size = 0;
  *pKeyInfo = NULL;
  tableInfo->pIter = taosHashIterate(tableInfo->gIdMap, tableInfo->pIter);
  STREAM_CHECK_NULL_GOTO(tableInfo->pIter, code);

  int64_t* key = (int64_t*)taosHashGetKey(tableInfo->pIter, NULL);
  *suid = *key;
  stDebug("stream reader iter table list, suid:%"PRId64, *suid);
  SStreamTableList* list = (SStreamTableList*)(tableInfo->pIter);
  STREAM_CHECK_RET_GOTO(buildTableListFromList(pKeyInfo, size, list));
end:
  return code;
}

int32_t qBuildVTableList(SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t      lino = 0;
  int32_t iter = 0;
  void* pTask = sStreamReaderInfo->pTask;
  void*   px = tSimpleHashIterate(sStreamReaderInfo->uidHashTrigger, NULL, &iter);
  while (px != NULL) {
    int64_t* id = tSimpleHashGetKey(px, NULL);
    STREAM_CHECK_RET_GOTO(qStreamSetTableList(&sStreamReaderInfo->vSetTableList, *(id+1), *id));
    px = tSimpleHashIterate(sStreamReaderInfo->uidHashTrigger, px, &iter);
    ST_TASK_DLOG("%s build tablelist for vtable, suid:%"PRId64" uid:%"PRId64, __func__, *id, *(id+1));
  }
  
end:
  return code;
}

void releaseStreamTask(void* p) {
  if (p == NULL) return;
  SStreamReaderTaskInner* pTask = *((SStreamReaderTaskInner**)p);
  if (pTask == NULL) return;
  blockDataDestroy(pTask->pResBlock);
  blockDataDestroy(pTask->pResBlockDst);
  pTask->storageApi->tsdReader.tsdReaderClose(pTask->pReader);
  cleanupQueryTableDataCond(&pTask->cond);
  tSimpleHashCleanup(pTask->pRollupMetaByUid);
  tSimpleHashCleanup(pTask->pRollupMetaCount);

  taosMemoryFree(pTask);
}

int32_t createDataBlockForStream(SArray* schemas, SSDataBlock** pBlockRet) {
  int32_t      code = 0;
  int32_t      lino = 0;
  int32_t      numOfCols = taosArrayGetSize(schemas);
  SSDataBlock* pBlock = NULL;
  STREAM_CHECK_RET_GOTO(createDataBlock(&pBlock));

  for (int32_t i = 0; i < numOfCols; ++i) {
    SSchema* pSchema = taosArrayGet(schemas, i);
    STREAM_CHECK_NULL_GOTO(pSchema, terrno);
    SColumnInfoData idata = createColumnInfoData(pSchema->type, pSchema->bytes, pSchema->colId);

    STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
  }
  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, STREAM_RETURN_ROWS_NUM));

end:
  // STREAM_PRINT_LOG_END(code, lino)
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    pBlock = NULL;
  }
  *pBlockRet = pBlock;
  return code;
}

int32_t createDataBlockForTs(SSDataBlock** pBlockRet) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SSDataBlock* pBlock = NULL;
  STREAM_CHECK_RET_GOTO(createDataBlock(&pBlock));
  SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID);
  STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, STREAM_RETURN_ROWS_NUM));

end:
  STREAM_PRINT_LOG_END(code, lino)
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    pBlock = NULL;
  }
  *pBlockRet = pBlock;
  return code;
}

int32_t qStreamInitQueryTableDataCond(SQueryTableDataCond* pCond, int32_t order, void* schemas, bool isSchema,
                                      STimeWindow twindows, uint64_t suid, int64_t ver, int32_t** pSlotList) {
  int32_t code = 0;
  int32_t lino = 0;

  memset(pCond, 0, sizeof(*pCond));

  pCond->order = order;
  pCond->numOfCols = isSchema ? taosArrayGetSize((SArray*)schemas) : LIST_LENGTH((SNodeList*)schemas);
  pCond->pSlotList = pSlotList != NULL ? *pSlotList : taosMemoryMalloc(sizeof(int32_t) * pCond->numOfCols);
  STREAM_CHECK_NULL_GOTO(pCond->pSlotList, terrno);

  pCond->colList = taosMemoryCalloc(pCond->numOfCols, sizeof(SColumnInfo));
  STREAM_CHECK_NULL_GOTO(pCond->colList, terrno);
  

  pCond->twindows = twindows;
  pCond->suid = suid;
  pCond->type = TIMEWINDOW_RANGE_CONTAINED;
  pCond->startVersion = -1;
  pCond->endVersion = ver;
  //  pCond->skipRollup = readHandle->skipRollup;

  pCond->notLoadData = false;

  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    SColumnInfo* pColInfo = &pCond->colList[i];
    if (isSchema) {
      SSchema* pSchema = taosArrayGet((SArray*)schemas, i);
      pCond->colList[i].type = pSchema->type;
      pCond->colList[i].bytes = pSchema->bytes;
      pCond->colList[i].colId = pSchema->colId;
      pCond->colList[i].pk = pSchema->flags & COL_IS_KEY;

      if (pSlotList == NULL ) pCond->pSlotList[i] = i;
    } else {
      STargetNode* pNode = (STargetNode*)nodesListGetNode((SNodeList*)schemas, i);
      STREAM_CHECK_NULL_GOTO(pNode, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

      SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;
      STREAM_CHECK_NULL_GOTO(pColNode, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

      pCond->colList[i].type = pColNode->node.resType.type;
      pCond->colList[i].bytes = pColNode->node.resType.bytes;
      pCond->colList[i].precision = pColNode->node.resType.precision;
      pCond->colList[i].scale = pColNode->node.resType.scale;
      pCond->colList[i].colId = pColNode->colId;
      pCond->colList[i].pk = pColNode->isPk;

      if (pSlotList == NULL)  pCond->pSlotList[i] = pNode->slotId;
    }
  }

end:
  STREAM_PRINT_LOG_END(code, lino);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pCond->colList);
    taosMemoryFree(pCond->pSlotList);
    pCond->colList = NULL;
    pCond->pSlotList = NULL;
  }
  if (pSlotList != NULL) *pSlotList = NULL;
  return code;
}

int32_t createStreamTask(void* pVnode, SStreamOptions* options, SStreamReaderTaskInner** ppTask,
                         SSDataBlock* pResBlock, STableKeyInfo* pList, int32_t pNum, SStorageAPI* storageApi) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTaskInner = taosMemoryCalloc(1, sizeof(SStreamReaderTaskInner));

  STREAM_CHECK_NULL_GOTO(pTaskInner, terrno);
  pTaskInner->options = options;
  pTaskInner->storageApi = storageApi;
  if (pResBlock != NULL) {
    STREAM_CHECK_RET_GOTO(createOneDataBlock(pResBlock, false, &pTaskInner->pResBlock));
  } else {
    STREAM_CHECK_RET_GOTO(createDataBlockForStream(pTaskInner->options->schemas, &pTaskInner->pResBlock));
  }

  cleanupQueryTableDataCond(&pTaskInner->cond);
  STREAM_CHECK_RET_GOTO(qStreamInitQueryTableDataCond(&pTaskInner->cond, options->order, options->schemas, options->isSchema,
                                                    options->twindows, options->suid, options->ver, options->pSlotList));
  STREAM_CHECK_RET_GOTO(pTaskInner->storageApi->tsdReader.tsdReaderOpen(pVnode, &pTaskInner->cond, pList, pNum, pTaskInner->pResBlock,
                                                          (void**)&pTaskInner->pReader, pTaskInner->idStr, NULL));
  *ppTask = pTaskInner;
  pTaskInner = NULL;

end:
  releaseStreamTask(&pTaskInner);
  return code;
}

int32_t createStreamTaskForTs(SStreamOptions* options, SStreamReaderTaskInner** ppTask, SStorageAPI* storageApi) {
  SStreamReaderTaskInner* pTaskInner = taosMemoryCalloc(1, sizeof(SStreamReaderTaskInner));
  if (pTaskInner == NULL) 
    return terrno;
  
  pTaskInner->options = options;
  pTaskInner->storageApi = storageApi;
  *ppTask = pTaskInner;
  return 0;
}

static void destroyCondition(SNode* pCond) {
  if (pCond == NULL) return;
  nodesDestroyNode(pCond);
}

static void destroyBlock(void* data) {
  if (data == NULL) return;
  blockDataDestroy(*(SSDataBlock**)data);
}

static void tblRefCacheItemFree(void *param) {
  SVTableRefResolveRspItem *p = (SVTableRefResolveRspItem *)param;
  if (p) {
    taosMemoryFreeClear(p->tagData);
    taosMemoryFreeClear(p->nextRef.tagCondJson);
  }
}

int32_t streamVTableInfoCacheInit(SStreamVTableInfoCache *pCache) {
  if (pCache == NULL) return TSDB_CODE_INVALID_PARA;
  taosInitRWLatch(&pCache->lock);
  // reqColCids / reqTagCids are NULL until the first commit. NULL means
  // "resolve all columns" downstream; an empty array would mean "resolve
  // zero columns" and produce a false diff on recheck.
  pCache->reqColCids  = NULL;
  pCache->reqTagCids  = NULL;
  pCache->uid2Result  = tSimpleHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  pCache->dbVgInfo    = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  pCache->tblRefCache = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  pCache->uidSlice    = taosArrayInit(64, sizeof(int64_t));
  pCache->sliceCursor = 0;
  pCache->lastCheckMs = 0;
  pCache->valid       = false;
  if (!pCache->uid2Result || !pCache->dbVgInfo || !pCache->uidSlice || !pCache->tblRefCache) {
    streamVTableInfoCacheDestroy(pCache);
    return terrno;
  }
  tSimpleHashSetFreeFp(pCache->uid2Result, streamVTableResolveResultDestroy);
  taosHashSetFreeFp(pCache->tblRefCache, tblRefCacheItemFree);
  return 0;
}

void streamVTableResolveResultDestroy(void *ptr) {
  if (!ptr) return;
  SVTableResolveResult *pRes = *(SVTableResolveResult **)ptr;
  if (pRes == NULL) return;
  tSimpleHashCleanup(pRes->colMap);
  tSimpleHashCleanup(pRes->tagMap);
  taosMemoryFree(pRes);
}

void streamVTableInfoCacheDestroy(SStreamVTableInfoCache *pCache) {
  if (!pCache) return;
  tSimpleHashCleanup(pCache->uid2Result);
  taosArrayDestroy(pCache->reqColCids);
  taosArrayDestroy(pCache->reqTagCids);
  taosArrayDestroy(pCache->uidSlice);
  if (pCache->dbVgInfo) {
    void *iter = taosHashIterate(pCache->dbVgInfo, NULL);
    while (iter != NULL) {
      tFreeSUsedbRsp((SUseDbRsp *)iter);
      iter = taosHashIterate(pCache->dbVgInfo, iter);
    }
    taosHashCleanup(pCache->dbVgInfo);
  }
  taosHashCleanup(pCache->tblRefCache);
  pCache->uid2Result  = NULL;
  pCache->reqColCids  = NULL;
  pCache->reqTagCids  = NULL;
  pCache->dbVgInfo    = NULL;
  pCache->tblRefCache = NULL;
  pCache->uidSlice    = NULL;
  pCache->sliceCursor = 0;
  pCache->valid       = false;
}

static void releaseStreamReaderInfo(void* p) {
  if (p == NULL) return;
  SStreamTriggerReaderInfo* pInfo = (SStreamTriggerReaderInfo*)p;
  taosHashCleanup(pInfo->streamTaskMap);
  taosHashCleanup(pInfo->groupIdMap);
  pInfo->streamTaskMap = NULL;

  nodesDestroyNode((SNode*)(pInfo->triggerAst));
  nodesDestroyNode((SNode*)(pInfo->calcAst));
  
  nodesDestroyList(pInfo->partitionCols);
  nodesDestroyList(pInfo->pRollupTagCols);
  blockDataDestroy(pInfo->triggerResBlock);
  blockDataDestroy(pInfo->calcResBlock);
  blockDataDestroy(pInfo->tsBlock);
  taosArrayDestroy(pInfo->tsSchemas);
  destroyExprInfo(pInfo->pExprInfoTriggerTag, pInfo->numOfExprTriggerTag);
  taosMemoryFreeClear(pInfo->pExprInfoTriggerTag);
  destroyExprInfo(pInfo->pExprInfoCalcTag, pInfo->numOfExprCalcTag);
  taosMemoryFreeClear(pInfo->pExprInfoCalcTag);
  tSimpleHashCleanup(pInfo->uidHashTrigger);
  tSimpleHashCleanup(pInfo->uidHashCalc);
  qStreamDestroyTableInfo(&pInfo->tableList);
  qStreamDestroyTableInfo(&pInfo->vSetTableList);
  filterFreeInfo(pInfo->pFilterInfo);
  pInfo->pFilterInfo = NULL;
  blockDataDestroy(pInfo->triggerBlock);
  pInfo->triggerBlock = NULL;
  blockDataDestroy(pInfo->calcBlock);
  pInfo->calcBlock = NULL;
  blockDataDestroy(pInfo->metaBlock);
  pInfo->metaBlock = NULL;
  taosHashCleanup(pInfo->triggerTableSchemaMapVTable);
  taosMemoryFreeClear(pInfo->triggerTableSchema);
  taosHashCleanup(pInfo->pTableMetaCacheTrigger);
  taosHashCleanup(pInfo->pTableMetaCacheCalc);
  if (pInfo->vtbCache) {
    streamVTableInfoCacheDestroy(pInfo->vtbCache);
    taosMemoryFreeClear(pInfo->vtbCache);
  }
  taosMemoryFreeClear(pInfo->extraErrMsg);
  taosMemoryFree(pInfo);
}

static void releaseStreamReaderCalcInfo(void* p) {
  if (p == NULL) return;
  SStreamTriggerReaderCalcInfo* pInfo = (SStreamTriggerReaderCalcInfo*)p;
  if (pInfo == NULL) return;
  nodesDestroyNode((SNode*)(pInfo->calcAst));
  taosMemoryFreeClear(pInfo->calcScanPlan);
  qDestroyTask(pInfo->pTaskInfo);
  pInfo->pTaskInfo = NULL;
  nodesDestroyNode((SNode*)pInfo->tsConditions);
  filterFreeInfo(pInfo->pFilterInfo);

  stReaderClearDimensionBatch(pInfo);
  tDestroyStRtFuncInfo(&pInfo->rtInfo.funcInfo);
  tDestroyStreamContextPolicy(&pInfo->tmpRtFuncInfo.pContextPolicy);
  tDestroyStreamAncestorContext(&pInfo->tmpRtFuncInfo.pAncestorContext);
  taosArrayDestroy(pInfo->tmpRtFuncInfo.pStreamPesudoFuncVals);
  taosMemoryFree(pInfo);
}

int32_t qStreamBuildSchema(SArray* schemas, int8_t type, int32_t bytes, col_id_t colId) {
  SSchema* pSchema = taosArrayReserve(schemas, 1);
  if (pSchema == NULL) {
    return terrno;
  }
  pSchema->type = type;
  pSchema->bytes = bytes;
  pSchema->colId = colId;
  return 0;
}

static void releaseGroupIdMap(void* p) {
  if (p == NULL) return;
  SArray* gInfo = *((SArray**)p);
  if (gInfo == NULL) return;
  taosArrayDestroyEx(gInfo, tDestroySStreamGroupValue);
}

static int32_t setColIdForCalcResBlock(SNodeList* colList, SArray* pDataBlock){
  int32_t  code = 0;
  int32_t  lino = 0;
  SNode*  nodeItem = NULL;
  FOREACH(nodeItem, colList) {
    SNode*           pNode = ((STargetNode*)nodeItem)->pExpr;
    int32_t          slotId = ((STargetNode*)nodeItem)->slotId;
    SColumnInfoData* pColData = taosArrayGet(pDataBlock, slotId);
    STREAM_CHECK_NULL_GOTO(pColData, terrno);

    if (nodeType(pNode) == QUERY_NODE_FUNCTION){
      SFunctionNode* pFuncNode = (SFunctionNode*)pNode;
      STREAM_CHECK_CONDITION_GOTO(pFuncNode->funcType != FUNCTION_TYPE_TBNAME, TSDB_CODE_INVALID_PARA);
      pColData->info.colId = -1;
    } else if (nodeType(pNode) == QUERY_NODE_COLUMN) {
      SColumnNode*     valueNode = (SColumnNode*)(pNode);
      pColData->info.colId = valueNode->colId;
    } else {
      code = TSDB_CODE_INVALID_PARA;
      goto end;
    }
  }
end:
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

static void freeTagCache(void* pData){
  if (pData == NULL) return;
  SArray* tagCache = *(SArray**)pData;
  taosArrayDestroyP(tagCache, taosMemFree);
}

static void freeSchema(void* pData){
  if (pData == NULL) return;
  STSchema* schema = *(STSchema**)pData;
  taosMemoryFree(schema);
}

static bool groupbyTbname(SNodeList* pGroupList) {
  bool   bytbname = false;
  SNode* pNode = NULL;
  FOREACH(pNode, pGroupList) {
    if (pNode->type == QUERY_NODE_FUNCTION) {
      bytbname = (strcmp(((struct SFunctionNode*)pNode)->functionName, "tbname") == 0);
      break;
    }
  }
  return bytbname;
}

static SStreamTriggerReaderInfo* createStreamReaderInfo(void* pTask, const SStreamReaderDeployMsg* pMsg) {
  int32_t    code = 0;
  int32_t    lino = 0;

  SStreamTriggerReaderInfo* sStreamReaderInfo = taosMemoryCalloc(1, sizeof(SStreamTriggerReaderInfo));
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  sStreamReaderInfo->lock = 0;
  sStreamReaderInfo->pTask = pTask;
  sStreamReaderInfo->tableType = pMsg->msg.trigger.triggerTblType;
  sStreamReaderInfo->isVtableStream = pMsg->msg.trigger.isTriggerTblVirt;

  sStreamReaderInfo->suid = pMsg->msg.trigger.triggerTblSuid;
  sStreamReaderInfo->uid = pMsg->msg.trigger.triggerTblUid;

  ST_TASK_DLOG("pMsg->msg.trigger.deleteReCalc: %d", pMsg->msg.trigger.deleteReCalc);
  sStreamReaderInfo->deleteReCalc = pMsg->msg.trigger.deleteReCalc;
  sStreamReaderInfo->deleteOutTbl = pMsg->msg.trigger.deleteOutTbl;
  // process triggerScanPlan
  STREAM_CHECK_RET_GOTO(
      nodesStringToNode(pMsg->msg.trigger.triggerScanPlan, (SNode**)(&sStreamReaderInfo->triggerAst)));
  if (sStreamReaderInfo->triggerAst != NULL) {
    STREAM_CHECK_CONDITION_GOTO(
        QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN != nodeType(sStreamReaderInfo->triggerAst->pNode) &&
            QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN != nodeType(sStreamReaderInfo->triggerAst->pNode),
        TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
    sStreamReaderInfo->pTagCond = sStreamReaderInfo->triggerAst->pTagCond;
    sStreamReaderInfo->pTagIndexCond = sStreamReaderInfo->triggerAst->pTagIndexCond;
    sStreamReaderInfo->pConditions = sStreamReaderInfo->triggerAst->pNode->pConditions;
    STREAM_CHECK_RET_GOTO(filterInitFromNode(sStreamReaderInfo->pConditions, &sStreamReaderInfo->pFilterInfo, 0, NULL));
    STREAM_CHECK_RET_GOTO(nodesStringToList(pMsg->msg.trigger.partitionCols, &sStreamReaderInfo->partitionCols));
    sStreamReaderInfo->twindows = ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scanRange;
    sStreamReaderInfo->triggerCols = ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scan.pScanCols;
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerCols, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
    SDataBlockDescNode* pDescNode =
        ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scan.node.pOutputDataBlockDesc;
    sStreamReaderInfo->triggerResBlock = createDataBlockFromDescNode(pDescNode);
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerResBlock, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

    // SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, LONG_BYTES, -1); // uid
    // STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(sStreamReaderInfo->triggerResBlockNew, &idata));
    // idata = createColumnInfoData(TSDB_DATA_TYPE_UBIGINT, LONG_BYTES, -1); // gid
    // STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(sStreamReaderInfo->triggerResBlockNew, &idata));

    // STREAM_CHECK_RET_GOTO(buildSTSchemaForScanData(&sStreamReaderInfo->triggerSchema, sStreamReaderInfo->triggerCols));
    sStreamReaderInfo->triggerPseudoCols = ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scan.pScanPseudoCols;
    if (sStreamReaderInfo->triggerPseudoCols != NULL) {
      STREAM_CHECK_RET_GOTO(
          createExprInfo(sStreamReaderInfo->triggerPseudoCols, NULL, &sStreamReaderInfo->pExprInfoTriggerTag, &sStreamReaderInfo->numOfExprTriggerTag));
    }
    STREAM_CHECK_RET_GOTO(setColIdForCalcResBlock(sStreamReaderInfo->triggerPseudoCols, sStreamReaderInfo->triggerResBlock->pDataBlock));
    STREAM_CHECK_RET_GOTO(setColIdForCalcResBlock(sStreamReaderInfo->triggerCols, sStreamReaderInfo->triggerResBlock->pDataBlock));
    sStreamReaderInfo->groupByTbname = groupbyTbname(sStreamReaderInfo->partitionCols);
    if (pMsg->msg.trigger.rollupTagCols != NULL) {
      SNodeList* pList = NULL;
      STREAM_CHECK_RET_GOTO(nodesStringToList(pMsg->msg.trigger.rollupTagCols, &pList));
      if (LIST_LENGTH(pList) != 1 || nodesListGetNode(pList, 0) == NULL) {
        nodesDestroyList(pList);
        code = TSDB_CODE_INVALID_PARA;
        lino = __LINE__;
        goto end;
      }
      sStreamReaderInfo->pRollupTagCols = pList;
      nodesListGetNode(sStreamReaderInfo->pRollupTagCols, 0)->type = QUERY_NODE_COLUMN;
      sStreamReaderInfo->isRollupReader = true;
      sStreamReaderInfo->tableList.uIdMapMode = UIDMAP_MULTI;
      ST_TASK_ILOG("rollup reader deployed, tag:%s",
                   ((SColumnNode*)nodesListGetNode(sStreamReaderInfo->pRollupTagCols, 0))->colName);
    }
  }

  // process calcCacheScanPlan
  STREAM_CHECK_RET_GOTO(nodesStringToNode(pMsg->msg.trigger.calcCacheScanPlan, (SNode**)(&sStreamReaderInfo->calcAst)));
  if (sStreamReaderInfo->calcAst != NULL) {
    STREAM_CHECK_CONDITION_GOTO(
        QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN != nodeType(sStreamReaderInfo->calcAst->pNode) &&
            QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN != nodeType(sStreamReaderInfo->calcAst->pNode),
        TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
    
    SDataBlockDescNode* pDescNode =
        ((STableScanPhysiNode*)(sStreamReaderInfo->calcAst->pNode))->scan.node.pOutputDataBlockDesc;
    sStreamReaderInfo->calcResBlock = createDataBlockFromDescNode(pDescNode);
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->calcResBlock, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
    

    SNodeList* pseudoCols = ((STableScanPhysiNode*)(sStreamReaderInfo->calcAst->pNode))->scan.pScanPseudoCols;
    if (pseudoCols != NULL) {
      STREAM_CHECK_RET_GOTO(
          createExprInfo(pseudoCols, NULL, &sStreamReaderInfo->pExprInfoCalcTag, &sStreamReaderInfo->numOfExprCalcTag));
    }
    SNodeList* pScanCols = ((STableScanPhysiNode*)(sStreamReaderInfo->calcAst->pNode))->scan.pScanCols;
    STREAM_CHECK_RET_GOTO(setColIdForCalcResBlock(pseudoCols, sStreamReaderInfo->calcResBlock->pDataBlock));
    STREAM_CHECK_RET_GOTO(setColIdForCalcResBlock(pScanCols, sStreamReaderInfo->calcResBlock->pDataBlock));
    STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->calcResBlock, false, &sStreamReaderInfo->calcBlock));
    SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, LONG_BYTES, INT16_MIN); // ver
    STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(sStreamReaderInfo->calcBlock, &idata));

    sStreamReaderInfo->pTableMetaCacheCalc = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->pTableMetaCacheCalc, terrno);
    taosHashSetFreeFp(sStreamReaderInfo->pTableMetaCacheCalc, freeTagCache);
  }

  sStreamReaderInfo->tsSchemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->tsSchemas, terrno)
  STREAM_CHECK_RET_GOTO(
      qStreamBuildSchema(sStreamReaderInfo->tsSchemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID))  // first ts
  STREAM_CHECK_RET_GOTO(createDataBlockForTs(&sStreamReaderInfo->tsBlock));
  sStreamReaderInfo->groupIdMap =
      taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->groupIdMap, terrno);
  taosHashSetFreeFp(sStreamReaderInfo->groupIdMap, releaseGroupIdMap);

  sStreamReaderInfo->streamTaskMap =
      taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->streamTaskMap, terrno);
  taosHashSetFreeFp(sStreamReaderInfo->streamTaskMap, releaseStreamTask);

  sStreamReaderInfo->pTableMetaCacheTrigger = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->pTableMetaCacheTrigger, terrno);
  taosHashSetFreeFp(sStreamReaderInfo->pTableMetaCacheTrigger, freeTagCache);

  sStreamReaderInfo->triggerTableSchemaMapVTable = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerTableSchemaMapVTable, terrno);
  taosHashSetFreeFp(sStreamReaderInfo->triggerTableSchemaMapVTable, freeSchema);

  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->triggerResBlock, false, &sStreamReaderInfo->triggerBlock));
  SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, LONG_BYTES, INT16_MIN); // ver
  STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(sStreamReaderInfo->triggerBlock, &idata));

  sStreamReaderInfo->vtbCache = taosMemoryCalloc(1, sizeof(SStreamVTableInfoCache));
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->vtbCache, terrno);
  STREAM_CHECK_RET_GOTO(streamVTableInfoCacheInit(sStreamReaderInfo->vtbCache));

end:
  STREAM_PRINT_LOG_END(code, lino);

  if (code != 0) {
    releaseStreamReaderInfo(sStreamReaderInfo);
    sStreamReaderInfo = NULL;
  }
  return sStreamReaderInfo;
}

static EDealRes checkPlaceHolderColumn(SNode* pNode, void* pContext) {
  if (QUERY_NODE_FUNCTION != nodeType((pNode))) {
    return DEAL_RES_CONTINUE;
  }
  SFunctionNode* pFuncNode = (SFunctionNode*)(pNode);
  if (fmIsStreamPesudoColVal(pFuncNode->funcId)) {
    *(bool*)pContext = true;
  }

  return DEAL_RES_CONTINUE;
}

static SStreamTriggerReaderCalcInfo* createStreamReaderCalcInfo(void* pTask, const SStreamReaderDeployMsg* pMsg, SNode* pPlan,
                                                               bool keepPlan, bool* pPlanTaken) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SNodeList* triggerCols = NULL;
  if (pPlanTaken != NULL) {
    *pPlanTaken = false;
  }

  SStreamTriggerReaderCalcInfo* sStreamReaderCalcInfo = taosMemoryCalloc(1, sizeof(SStreamTriggerReaderCalcInfo));
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo, terrno);

  sStreamReaderCalcInfo->pTask = pTask;
  if (keepPlan) {
    sStreamReaderCalcInfo->calcAst = (SSubplan*)pPlan;
    if (pPlanTaken != NULL) {
      *pPlanTaken = true;
    }
  } else {
    STREAM_CHECK_RET_GOTO(nodesCloneNode(pPlan, (SNode**)&sStreamReaderCalcInfo->calcAst));
  }
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo->calcAst, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
  sStreamReaderCalcInfo->requiresContextPolicy = sStreamReaderCalcInfo->calcAst->requiresAncestorContext;
  if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == nodeType(sStreamReaderCalcInfo->calcAst->pNode) ||
      QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN == nodeType(sStreamReaderCalcInfo->calcAst->pNode)){
    SNodeList* pScanCols = ((STableScanPhysiNode*)(sStreamReaderCalcInfo->calcAst->pNode))->scan.pScanCols;
    SNode*     nodeItem = NULL;
    FOREACH(nodeItem, pScanCols) {
      SColumnNode* valueNode = (SColumnNode*)((STargetNode*)nodeItem)->pExpr;
      if (valueNode->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        sStreamReaderCalcInfo->pTargetNodeTs = (STargetNode*)nodeItem;
      }
    }
  }
  
  bool hasPlaceHolderColumn = false;
  nodesWalkExpr(sStreamReaderCalcInfo->calcAst->pTagCond, checkPlaceHolderColumn, (void*)&hasPlaceHolderColumn);
  sStreamReaderCalcInfo->hasPlaceHolder = hasPlaceHolderColumn;
  sStreamReaderCalcInfo->calcScanPlan = taosStrdup(pMsg->msg.calc.calcScanPlan);
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo->calcScanPlan, terrno);
  sStreamReaderCalcInfo->pTaskInfo = NULL;

  sStreamReaderCalcInfo->tmpRtFuncInfo.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo->tmpRtFuncInfo.pStreamPesudoFuncVals, terrno);

end:
  STREAM_PRINT_LOG_END(code, lino);

  if (code != 0) {
    releaseStreamReaderCalcInfo(sStreamReaderCalcInfo);
    sStreamReaderCalcInfo = NULL;
  }
  return sStreamReaderCalcInfo;
}

/* Forward declarations to bridge ext-source reader lifecycle without
 * including streamReaderExt.h (which redefines SStreamTriggerReaderInfo in
 * this translation unit, causing a redefinition error).  The functions are
 * implemented in streamReaderExt.c via void* to avoid the type clash. */
extern int32_t stExtReaderOpen(void *pSpec, const SStreamTask *pTask, void **ppInfo);
extern void    stExtReaderClose(void *pInfo);

static bool stExtFedScanMatchesSource(const SFederatedScanPhysiNode *pFedScan, const char *sourceName) {
  if (pFedScan == NULL) {
    return false;
  }
  if (sourceName == NULL) {
    return true;
  }
  if (sourceName[0] == '\0') {
    return false;
  }
  if (pFedScan->pExtTable == NULL || nodeType(pFedScan->pExtTable) != QUERY_NODE_EXTERNAL_TABLE) {
    return false;
  }

  const SExtTableNode *pExtTable = (const SExtTableNode *)pFedScan->pExtTable;
  return strcmp(pExtTable->sourceName, sourceName) == 0;
}

static const SScanLogicNode *stExtFindLogicScanInPlan(const SNode *pNode, const char *sourceName) {
  if (pNode == NULL || nodeType(pNode) < QUERY_NODE_LOGIC_PLAN_SCAN ||
      nodeType(pNode) >= QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN) {
    return NULL;
  }

  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_SCAN) {
    const SScanLogicNode *pScan = (const SScanLogicNode *)pNode;
    if (pScan->scanType != SCAN_TYPE_EXTERNAL || pScan->pExtTableNode == NULL ||
        nodeType(pScan->pExtTableNode) != QUERY_NODE_EXTERNAL_TABLE) {
      return NULL;
    }
    const SExtTableNode *pExtTable = (const SExtTableNode *)pScan->pExtTableNode;
    if (sourceName != NULL && (sourceName[0] == '\0' || strcmp(pExtTable->sourceName, sourceName) != 0)) {
      return NULL;
    }
    return pScan;
  }

  const SLogicNode *pLogic = (const SLogicNode *)pNode;
  SNode *pChild = NULL;
  FOREACH(pChild, pLogic->pChildren) {
    const SScanLogicNode *pScan = stExtFindLogicScanInPlan(pChild, sourceName);
    if (pScan != NULL) {
      return pScan;
    }
  }
  return NULL;
}

static SFederatedScanPhysiNode *stExtFindFedScanInPlan(SNode *pNode, const char *sourceName, bool requireExtTable) {
  if (pNode == NULL) {
    return NULL;
  }

  if (nodeType(pNode) == QUERY_NODE_PHYSICAL_SUBPLAN) {
    return stExtFindFedScanInPlan((SNode *)((SSubplan *)pNode)->pNode, sourceName, requireExtTable);
  }

  if (nodeType(pNode) == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) {
    SFederatedScanPhysiNode *pFedScan = (SFederatedScanPhysiNode *)pNode;
    if (pFedScan->pRemotePlan != NULL) {
      stDebug("ext: skip remote logical plan during physical namespace lookup, nodeType:%d",
              (int)nodeType(pFedScan->pRemotePlan));
    }
    if ((!requireExtTable || pFedScan->pExtTable != NULL) && stExtFedScanMatchesSource(pFedScan, sourceName)) {
      return pFedScan;
    }
  }

  SPhysiNode *pPhysi = (SPhysiNode *)pNode;
  SNode      *pChild = NULL;
  FOREACH(pChild, pPhysi->pChildren) {
    SFederatedScanPhysiNode *pFedScan = stExtFindFedScanInPlan(pChild, sourceName, requireExtTable);
    if (pFedScan != NULL) {
      return pFedScan;
    }
  }

  return NULL;
}

static SNodeList *stExtFindCalcScanColsInPlan(SNode *pNode, const char *sourceName,
                                              SFederatedScanPhysiNode **ppFedScan,
                                              ENodeType *pScanType) {
  if (pNode == NULL) {
    return NULL;
  }

  ENodeType ntype = nodeType(pNode);
  if (ntype == QUERY_NODE_PHYSICAL_SUBPLAN) {
    SSubplan *pSubplan = (SSubplan *)pNode;
    return stExtFindCalcScanColsInPlan((SNode *)pSubplan->pNode, sourceName, ppFedScan, pScanType);
  }

  if (ntype == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN ||
      ntype == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
    if (ppFedScan != NULL) {
      *ppFedScan = NULL;
    }
    if (pScanType != NULL) {
      *pScanType = ntype;
    }
    return ((STableScanPhysiNode *)pNode)->scan.pScanCols;
  }

  if (ntype == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) {
    SFederatedScanPhysiNode *pFedScan = (SFederatedScanPhysiNode *)pNode;
    if (stExtFedScanMatchesSource(pFedScan, sourceName)) {
      if (ppFedScan != NULL) {
        *ppFedScan = pFedScan;
      }
      if (pScanType != NULL) {
        *pScanType = ntype;
      }
      return pFedScan->pScanCols;
    }
    if (pFedScan->pRemotePlan != NULL) {
      const SScanLogicNode *pRemoteScan =
          stExtFindLogicScanInPlan(pFedScan->pRemotePlan, sourceName);
      if (pRemoteScan != NULL) {
        if (ppFedScan != NULL) {
          *ppFedScan = NULL;
        }
        if (pScanType != NULL) {
          *pScanType = QUERY_NODE_LOGIC_PLAN_SCAN;
        }
        stDebug("ext: found remote logical scan for source=%s", sourceName != NULL ? sourceName : "(any)");
        return pRemoteScan->pScanCols;
      }
      stDebug("ext: remote logical plan has no matching external scan, nodeType:%d",
              (int)nodeType(pFedScan->pRemotePlan));
    }
  }

  SPhysiNode *pPhysi = (SPhysiNode *)pNode;
  SNode      *pChild = NULL;
  FOREACH(pChild, pPhysi->pChildren) {
    SNodeList *pScanCols = stExtFindCalcScanColsInPlan(pChild, sourceName, ppFedScan, pScanType);
    if (pScanCols != NULL) {
      return pScanCols;
    }
  }

  return NULL;
}

static void stExtOverrideNamespaceFromFedScan(SStreamReaderTask *pTask, SStreamExtTriggerSpec *pExtSpec,
                                              const SFederatedScanPhysiNode *pFedScan) {
  if (pFedScan == NULL || pFedScan->pExtTable == NULL ||
      nodeType(pFedScan->pExtTable) != QUERY_NODE_EXTERNAL_TABLE) {
    return;
  }

  const SExtTableNode *pExtTable = (const SExtTableNode *)pFedScan->pExtTable;
  const SScanLogicNode *pRemoteScan =
      stExtFindLogicScanInPlan(pFedScan->pRemotePlan, pExtSpec->sourceName);
  if (pRemoteScan != NULL && pRemoteScan->pExtTableNode != NULL &&
      nodeType(pRemoteScan->pExtTableNode) == QUERY_NODE_EXTERNAL_TABLE) {
    pExtTable = (const SExtTableNode *)pRemoteScan->pExtTableNode;
  }
  if (strcmp(pExtSpec->sourceName, pExtTable->sourceName) != 0) {
    ST_TASK_DLOG("ext: skip namespace from source=%s for spec source=%s", pExtTable->sourceName,
                 pExtSpec->sourceName);
    return;
  }

  if (pExtTable->table.dbName[0] != '\0'){
    tstrncpy(pExtSpec->extDb, pExtTable->table.dbName, sizeof(pExtSpec->extDb));
  }
  if (pExtTable->schemaName[0] != '\0'){
    tstrncpy(pExtSpec->extSchema, pExtTable->schemaName, sizeof(pExtSpec->extSchema));
  }
}

static int32_t stExtOverrideNamespaceFromPlan(SStreamReaderTask *pTask, SStreamExtTriggerSpec *pExtSpec,
                                              const char *planStr) {
  if (planStr == NULL) {
    ST_TASK_ELOG("%s", "ext: cannot resolve reader namespace from NULL scan plan");
    return TSDB_CODE_INVALID_PARA;
  }

  SSubplan *pSubplan = NULL;
  int32_t code = nodesStringToNode(planStr, (SNode **)&pSubplan);
  if (code != TSDB_CODE_SUCCESS || pSubplan == NULL) {
    ST_TASK_ELOG("ext: failed to parse scan plan for reader namespace, code=%d", code);
    nodesDestroyNode((SNode *)pSubplan);
    return code != TSDB_CODE_SUCCESS ? code : TSDB_CODE_INVALID_PARA;
  }

  SPhysiNode *pPlan = nodeType((SNode *)pSubplan) == QUERY_NODE_PHYSICAL_SUBPLAN
                         ? (SPhysiNode *)pSubplan->pNode
                         : (SPhysiNode *)pSubplan;
  SFederatedScanPhysiNode *pFedScan =
      stExtFindFedScanInPlan((SNode *)pPlan, pExtSpec == NULL ? NULL : pExtSpec->sourceName, true);
  if (pFedScan != NULL) {
    ST_TASK_DLOG("ext: resolved reader namespace from federated scan under node type=%d",
                 pPlan != NULL ? (int)nodeType(pPlan) : -1);
    stExtOverrideNamespaceFromFedScan(pTask, pExtSpec, pFedScan);
  } else if (pExtSpec != NULL && pExtSpec->extDb[0] != '\0' && pExtSpec->extTable[0] != '\0' &&
             pExtSpec->tsColumn[0] != '\0') {
    ST_TASK_DLOG("ext: using reader namespace from ext spec, source:%s db:%s schema:%s table:%s ts:%s planType:%d",
                 pExtSpec->sourceName, pExtSpec->extDb, pExtSpec->extSchema, pExtSpec->extTable,
                 pExtSpec->tsColumn, pPlan != NULL ? (int)nodeType(pPlan) : -1);
  } else {
    ST_TASK_ELOG("ext: cannot resolve reader namespace from scan node type=%d",
                 pPlan != NULL ? (int)nodeType(pPlan) : -1);
    code = TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN;
  }

  nodesDestroyNode((SNode *)pSubplan);
  return code;
}

/* ---------------------------------------------------------------------------
 * stExtBuildTriggerColumns -- parse triggerCols node-list and populate
 * pExtSpec->triggerColumns + pExtSpec->pColMappings so that fetchDataForUid
 * can build a precise SELECT list instead of SELECT *.
 * --------------------------------------------------------------------------- */
static int32_t stExtBuildTriggerColumns(SStreamReaderTask *pTask,
                                        const SStreamReaderDeployMsg *pMsg) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList *pTrigCols = NULL;

  if (pMsg->pExtSpec->triggerColumns != NULL && pMsg->pExtSpec->pColMappings != NULL &&
      pMsg->pExtSpec->numColMappings > 0) {
    ST_TASK_DLOG("ext: reuse deploy triggerColumns count=%d", pMsg->pExtSpec->numColMappings);
    return code;
  }

  if (pMsg->msg.trigger.triggerCols == NULL) {
    ST_TASK_DLOG("%s", "ext: triggerCols empty, fetchDataForUid will use SELECT *");
    return code;
  }

  int32_t trigParseCode = nodesStringToList(
      (const char *)pMsg->msg.trigger.triggerCols, &pTrigCols);
  if (trigParseCode != TSDB_CODE_SUCCESS) {
    ST_TASK_DLOG("ext: triggerCols parse failed code=%d, fetchDataForUid will use SELECT *",
                 trigParseCode);
    return code;
  }
  if (pTrigCols == NULL || LIST_LENGTH(pTrigCols) == 0) {
    ST_TASK_DLOG("%s", "ext: triggerCols empty, fetchDataForUid will use SELECT *");
    nodesDestroyList(pTrigCols);
    return code;
  }

  int32_t nCols = LIST_LENGTH(pTrigCols);
  if (pMsg->pExtSpec->triggerColumns == NULL) {
    pMsg->pExtSpec->triggerColumns = taosArrayInit(nCols, TSDB_COL_NAME_LEN);
  }
  if (pMsg->pExtSpec->pColMappings == NULL) {
    pMsg->pExtSpec->pColMappings =
        (SExtColTypeMapping *)taosMemoryCalloc(nCols, sizeof(SExtColTypeMapping));
  }
  if (pMsg->pExtSpec->triggerColumns == NULL || pMsg->pExtSpec->pColMappings == NULL) {
    ST_TASK_ELOG("%s", "ext: OOM allocating triggerColumns / pColMappings");
    nodesDestroyList(pTrigCols);
    return terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t colIdx = 0;
  SNode  *pNode = NULL;
  FOREACH(pNode, pTrigCols) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN) continue;
    SColumnNode *pCol = (SColumnNode *)pNode;
    if (taosArrayPush(pMsg->pExtSpec->triggerColumns, pCol->colName) == NULL) {
      ST_TASK_ELOG("ext: OOM pushing triggerColumn '%s'", pCol->colName);
      code = terrno;
      break;
    }
    SExtColTypeMapping *pMap = &pMsg->pExtSpec->pColMappings[colIdx];
    pMap->tdType = ((SExprNode *)pNode)->resType;
    tstrncpy(pMap->colName, pCol->colName, sizeof(pMap->colName));
    ST_TASK_DLOG("ext: triggerColumns[%d] col='%s' type=%d bytes=%d",
                 colIdx, pCol->colName, pMap->tdType.type, pMap->tdType.bytes);
    colIdx++;
  }
  pMsg->pExtSpec->numColMappings = colIdx;
  ST_TASK_DLOG("ext: triggerColumns built count=%d numColMappings=%d", colIdx, colIdx);

  nodesDestroyList(pTrigCols);
  return code;
}

/* ---------------------------------------------------------------------------
 * stExtCollectScanCols -- iterate a pScanCols node-list and fill
 * pExtSpec->calcColumns + pExtSpec->pCalcMappings.
 *
 * pFedScan != NULL   ->   federated-scan path (unwrap STargetNode, use
 *                      pColTypeMappings when available).
 * pFedScan == NULL   ->   table-scan path (raw SColumnNode entries).
 * --------------------------------------------------------------------------- */
static int32_t stExtCollectScanCols(SStreamReaderTask *pTask,
                                    SStreamExtTriggerSpec *pExtSpec,
                                    SNodeList *pScanCols,
                                    SFederatedScanPhysiNode *pFedScan) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t nCalc = LIST_LENGTH(pScanCols);
  const char *tag = pFedScan ? "(federated)" : "";

  pExtSpec->calcColumns =
      taosArrayInit(nCalc, TSDB_COL_NAME_LEN);
  pExtSpec->pCalcMappings =
      (SExtColTypeMapping *)taosMemoryCalloc(nCalc, sizeof(SExtColTypeMapping));
  if (pExtSpec->calcColumns == NULL || pExtSpec->pCalcMappings == NULL) {
    ST_TASK_ELOG("ext: OOM allocating calcColumns/pCalcMappings %s", tag);
    return terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t calcIdx = 0;
  int32_t nodeIdx = 0;
  SNode  *pCalcNode = NULL;
  FOREACH(pCalcNode, pScanCols) {
    int32_t scanIdx = nodeIdx++;
    SNode *pExpr = pCalcNode;
    if (nodeType(pCalcNode) == QUERY_NODE_TARGET) {
      pExpr = ((STargetNode *)pCalcNode)->pExpr;
    }
    if (pExpr == NULL || nodeType(pExpr) != QUERY_NODE_COLUMN) {
      ST_TASK_DLOG("ext: calcScanCols[%d] nodeType=%d exprType=%d skipped %s",
                   scanIdx, (int)nodeType(pCalcNode),
                   pExpr ? (int)nodeType(pExpr) : -1, tag);
      continue;
    }
    SColumnNode *pCol = (SColumnNode *)pExpr;
    if (taosArrayPush(pExtSpec->calcColumns, pCol->colName) == NULL) {
      ST_TASK_ELOG("ext: OOM pushing calcColumn '%s' %s", pCol->colName, tag);
      return terrno;
    }
    SExtColTypeMapping *pMap = &pExtSpec->pCalcMappings[calcIdx];
    if (pFedScan && pFedScan->pColTypeMappings != NULL &&
        calcIdx < pFedScan->numColTypeMappings) {
      pMap->tdType = pFedScan->pColTypeMappings[calcIdx].tdType;
    } else {
      pMap->tdType = ((SExprNode *)pExpr)->resType;
    }
    if (pMap->tdType.type == TSDB_DATA_TYPE_TIMESTAMP && pMap->tdType.precision != pExtSpec->precision) {
      ST_TASK_DLOG("ext: normalize calc mapping timestamp precision col='%s' precision:%d->%d %s",
                   pCol->colName, pMap->tdType.precision, pExtSpec->precision, tag);
      pMap->tdType.precision = pExtSpec->precision;
    }
    tstrncpy(pMap->colName, pCol->colName, sizeof(pMap->colName));
    ST_TASK_DLOG("ext: calcColumns[%d] col='%s' type=%d bytes=%d precision=%d %s",
                 calcIdx, pCol->colName, pMap->tdType.type, pMap->tdType.bytes, pMap->tdType.precision, tag);
    calcIdx++;
  }
  pExtSpec->numCalcMappings = calcIdx;
  ST_TASK_DLOG("ext: calcColumns built count=%d numCalcMappings=%d %s", calcIdx, calcIdx, tag);
  return code;
}

/* ---------------------------------------------------------------------------
 * stExtBuildCalcColumnsFromPlan -- parse a serialized scan plan string and
 * populate pExtSpec->calcColumns + pExtSpec->pCalcMappings.
 *
 * Called by:
 *   - trigger reader deploy: planStr = pMsg->msg.trigger.calcCacheScanPlan
 *   - calc reader deploy:    planStr = pMsg->msg.calc.calcScanPlan
 * --------------------------------------------------------------------------- */
static int32_t stExtBuildCalcColumnsFromPlan(SStreamReaderTask *pTask,
                                             SStreamExtTriggerSpec *pExtSpec,
                                             const char *planStr,
                                             bool updateSpecPrecision) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pExtSpec != NULL && pExtSpec->calcColumns != NULL && pExtSpec->pCalcMappings != NULL &&
      pExtSpec->numCalcMappings > 0) {
    ST_TASK_DLOG("ext: reuse deploy calcColumns count=%d", pExtSpec->numCalcMappings);
    return code;
  }

  if (planStr == NULL) {
    ST_TASK_DLOG("%s", "ext: calcScanPlan is NULL, calcColumns empty");
    return code;
  }

  SSubplan *pCalcSubplan = NULL;
  int32_t calcParseCode = nodesStringToNode(planStr, (SNode**)&pCalcSubplan);
  if (calcParseCode != TSDB_CODE_SUCCESS || pCalcSubplan == NULL) {
    ST_TASK_DLOG("ext: calcCacheScanPlan parse failed code=%d, calcColumns will be empty",
                 calcParseCode);
    return code;
  }

  // extract the physical scan node from the subplan
  SPhysiNode *pCalcPlan = NULL;
  if (nodeType((SNode*)pCalcSubplan) == QUERY_NODE_PHYSICAL_SUBPLAN) {
    pCalcPlan = (pCalcSubplan->pNode != NULL) ? (SPhysiNode *)pCalcSubplan->pNode : NULL;
  } else {
    pCalcPlan = (SPhysiNode *)pCalcSubplan;
  }

  ENodeType ntype = (pCalcPlan != NULL) ? nodeType(pCalcPlan) : (ENodeType)0;
  ST_TASK_DLOG("ext: calcCacheScanPlan subplan pNode type=%d", (int)ntype);

  if (updateSpecPrecision && pExtSpec != NULL && pCalcPlan != NULL &&
      pCalcPlan->pOutputDataBlockDesc != NULL) {
    int8_t calcPrecision = (int8_t)pCalcPlan->pOutputDataBlockDesc->precision;
    if (calcPrecision >= TSDB_MIN_PRECISION && calcPrecision <= TSDB_MAX_PRECISION) {
      if (pExtSpec->precision != calcPrecision) {
        ST_TASK_DLOG("ext: update calc reader precision from plan precision:%d->%d",
                     pExtSpec->precision, calcPrecision);
      }
      pExtSpec->precision = calcPrecision;
    } else {
      ST_TASK_WLOG("ext: invalid calc plan precision:%d, keep spec precision:%d",
                   calcPrecision, pExtSpec->precision);
    }
  }

  SNodeList *pScanCols = NULL;
  SFederatedScanPhysiNode *pFedScan = NULL;
  ENodeType scanType = (ENodeType)0;
  pScanCols = stExtFindCalcScanColsInPlan((SNode *)pCalcPlan, pExtSpec == NULL ? NULL : pExtSpec->sourceName,
                                          &pFedScan, &scanType);
  if (pScanCols != NULL) {
    ST_TASK_DLOG("ext: calcCacheScanPlan found scan node type=%d under root node type=%d",
                 (int)scanType, (int)ntype);
  } else {
    ST_TASK_DLOG("ext: calcCacheScanPlan root node type=%d has no TABLE_SCAN/FEDERATED_SCAN, calcColumns empty",
                 (int)ntype);
  }

  if (pScanCols != NULL && LIST_LENGTH(pScanCols) > 0) {
    code = stExtCollectScanCols(pTask, pExtSpec, pScanCols, pFedScan);
  } else if (pScanCols != NULL) {
    const char *tag = pFedScan ? "federated " : "";
    ST_TASK_DLOG("ext: %scalcScanPlan has no scan cols, calcColumns empty", tag);
  }

  nodesDestroyNode((SNode*)pCalcSubplan);
  return code;
}

/* Convenience wrapper for the trigger reader deploy path. */
static int32_t stExtBuildCalcColumns(SStreamReaderTask *pTask,
                                     const SStreamReaderDeployMsg *pMsg) {
  return stExtBuildCalcColumnsFromPlan(pTask, pMsg->pExtSpec,
                                      (const char *)pMsg->msg.trigger.calcCacheScanPlan,
                                      false);
}

/* ---------------------------------------------------------------------------
 * stReaderTaskDeploy -- deploy a reader task (trigger, ext-trigger, or calc).
 * --------------------------------------------------------------------------- */
int32_t stReaderTaskDeploy(SStreamReaderTask* pTask, const SStreamReaderDeployMsg* pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SNode*  pPlan = NULL;
  bool    pPlanMoved = false;
  STREAM_CHECK_NULL_GOTO(pTask, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_NULL_GOTO(pMsg, TSDB_CODE_INVALID_PARA);
  TAOS_CHECK_GOTO(streamTaskStatsInit(&pTask->task, &pTask->pStats), &lino, end);

  pTask->triggerReader = pMsg->triggerReader;
  if (pMsg->triggerReader == 1) {
    if (pMsg->pExtSpec != NULL) {
      /* External-source trigger reader: initialize via streamReaderExtOpen
       * instead of createStreamReaderInfo, which requires a TABLE_SCAN plan
       * node unavailable for federated scans. */
      ST_TASK_DLOG("ext trigger reader deploy: pExtSpec=%p", pMsg->pExtSpec);

      TAOS_CHECK_GOTO(stExtOverrideNamespaceFromPlan(pTask, pMsg->pExtSpec,
                                                    (const char *)pMsg->msg.trigger.triggerScanPlan),
                      &lino, end);
      TAOS_CHECK_GOTO(stExtBuildTriggerColumns(pTask, pMsg), &lino, end);
      TAOS_CHECK_GOTO(stExtBuildCalcColumns(pTask, pMsg), &lino, end);

      void *pExtInfo = NULL;
      TAOS_CHECK_GOTO(stExtReaderOpen((void *)pMsg->pExtSpec, &pTask->task, &pExtInfo), &lino, end);
      pTask->info = pExtInfo;
      pTask->task.flags |= STREAM_FLAG_REF_EXT_SOURCE;
      ST_TASK_DLOG("ext trigger reader opened: pInfo=%p flags=%" PRId64,
                   pTask->info, pTask->task.flags);
    } else {
      ST_TASK_DLOGL("triggerScanPlan:%s", (char*)(pMsg->msg.trigger.triggerScanPlan));
      ST_TASK_DLOGL("calcCacheScanPlan:%s", (char*)(pMsg->msg.trigger.calcCacheScanPlan));
      pTask->info = createStreamReaderInfo(pTask, pMsg);
      STREAM_CHECK_NULL_GOTO(pTask->info, terrno);
    }
  } else {
    if (pMsg->pExtSpec != NULL) {
      /* External-source calc/runner reader: build calcColumns from the calc
       * scan plan, then initialize via stExtReaderOpen so handleExtFetchReq
       * can use pTask->info as SStreamExtReaderInfo. */
      ST_TASK_DLOG("ext calc reader deploy: pExtSpec=%p calcScanPlan=%p",
                   pMsg->pExtSpec, pMsg->msg.calc.calcScanPlan);

      /* Multi-source federated calc: each calc reader scans its own external
       * table, but the mnode-supplied ext spec only carries the source-level
       * connection.  Override the per-scan table identity delivered in the
       * deploy msg so streamReaderExtFetchData targets the correct table/ts col. */
      if (pMsg->msg.calc.extTable[0] != '\0') {
        tstrncpy(pMsg->pExtSpec->extTable, pMsg->msg.calc.extTable, sizeof(pMsg->pExtSpec->extTable));
      }
      if (pMsg->msg.calc.tsColumn[0] != '\0') {
        tstrncpy(pMsg->pExtSpec->tsColumn, pMsg->msg.calc.tsColumn, sizeof(pMsg->pExtSpec->tsColumn));
      }

      TAOS_CHECK_GOTO(stExtOverrideNamespaceFromPlan(pTask, pMsg->pExtSpec,
                                                    (const char *)pMsg->msg.calc.calcScanPlan),
                      &lino, end);
      TAOS_CHECK_GOTO(stExtBuildCalcColumnsFromPlan(pTask, pMsg->pExtSpec,
                                                   (const char *)pMsg->msg.calc.calcScanPlan,
                                                   true),
                      &lino, end);

      void *pExtInfo = NULL;
      TAOS_CHECK_GOTO(stExtReaderOpen((void *)pMsg->pExtSpec, &pTask->task, &pExtInfo), &lino, end);
      pTask->info = pExtInfo;
      pTask->task.flags |= STREAM_FLAG_REF_EXT_SOURCE;
      ST_TASK_DLOG("ext calc reader opened: pInfo=%p flags=%" PRId64,
                   pTask->info, pTask->task.flags);
    } else {
      SNode* pPlan = NULL;
      ST_TASK_DLOGL("calcScanPlan:%s", (char*)(pMsg->msg.calc.calcScanPlan));
      pTask->info = taosArrayInit(pMsg->msg.calc.execReplica, POINTER_BYTES);
      STREAM_CHECK_NULL_GOTO(pTask->info, terrno);
      STREAM_CHECK_RET_GOTO(nodesStringToNode(pMsg->msg.calc.calcScanPlan, &pPlan));
      
      for (int32_t i = 0; i < pMsg->msg.calc.execReplica; ++i) {
        bool pPlanTaken = false;
        SStreamTriggerReaderCalcInfo* pCalcInfo = createStreamReaderCalcInfo(pTask, pMsg, pPlan, 0 == i, &pPlanTaken);
        if (pPlanTaken) {
          pPlanMoved = true;
        }
        STREAM_CHECK_NULL_GOTO(pCalcInfo, terrno);
        if (NULL == taosArrayPush(pTask->info, &pCalcInfo)) {
          releaseStreamReaderCalcInfo(pCalcInfo);
          STREAM_CHECK_NULL_GOTO(NULL, terrno);
        }
      }
    }
    if (!pPlanMoved) {
      nodesDestroyNode(pPlan);
    }
    pPlan = NULL;
  }
  ST_TASK_DLOG("stReaderTaskDeploy: stream %" PRIx64 " task %" PRIx64 " vgId:%d pTask:%p, info:%p", pTask->task.streamId,
         pTask->task.taskId, pTask->task.nodeId, pTask, pTask->info);

  pTask->task.status = STREAM_STATUS_INIT;

end:

  STREAM_PRINT_LOG_END(code, lino);

  if (code) {
    streamTaskStatsHandleLifecycle(&pTask->pStats, STREAM_TASK_STATS_DEPLOY_FAILED);
    if (!pPlanMoved) {
      nodesDestroyNode(pPlan);
    }
    if (pTask->triggerReader == 1) {
      releaseStreamReaderInfo(pTask->info);
    } else {
      taosArrayDestroyP(pTask->info, releaseStreamReaderCalcInfo);
    }
    pTask->info = NULL;
    pTask->task.status = STREAM_STATUS_FAILED;
  }

  return code;
}

int32_t stReaderTaskUndeployImpl(SStreamReaderTask** ppTask, const SStreamUndeployTaskMsg* pMsg, taskUndeplyCallback cb) {
  int32_t code = 0;
  int32_t lino = 0;
  STREAM_CHECK_NULL_GOTO(ppTask, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_NULL_GOTO(pMsg, TSDB_CODE_INVALID_PARA);
  SStreamReaderTask* pTask = *ppTask;
  if ((*ppTask)->triggerReader == 1) {
    if (STREAM_IS_REF_EXT_SOURCE((*ppTask)->task.flags)) {
      /* Ext-source reader: release the SStreamTriggerReaderInfo (ext version)
       * allocated by streamReaderExtOpen via streamReaderExtClose. */
      ST_TASK_ILOG("release ext reader info:%p flags:%" PRId64, (*ppTask)->info, (*ppTask)->task.flags);
      stExtReaderClose((*ppTask)->info);
    } else {
      ST_TASK_ILOG("release stream reader info:%p", (*ppTask)->info);
      releaseStreamReaderInfo((*ppTask)->info);
    }
  } else {
    if (STREAM_IS_REF_EXT_SOURCE((*ppTask)->task.flags)) {
      ST_TASK_ILOG("release ext calc reader info:%p flags:%" PRId64, (*ppTask)->info, (*ppTask)->task.flags);
      stExtReaderClose((*ppTask)->info);
    } else {
      taosArrayDestroyP((*ppTask)->info, releaseStreamReaderCalcInfo);
    }
  }
  (*ppTask)->info = NULL;
  streamTaskStatsHandleLifecycle(&(*ppTask)->pStats, STREAM_TASK_STATS_UNDEPLOYED);

end:
  STREAM_PRINT_LOG_END(code, lino);
  (*cb)(ppTask);

  return code;
}


int32_t stReaderTaskUndeploy(SStreamReaderTask** ppTask, bool force) {
  int32_t            code = TSDB_CODE_SUCCESS;
  SStreamReaderTask *pTask = *ppTask;
  
  if (!force && taosWTryForceLockLatch(&pTask->task.entryLock)) {
    ST_TASK_DLOG("ignore undeploy reader task since working, entryLock:%x", pTask->task.entryLock);
    return code;
  }

  return stReaderTaskUndeployImpl(ppTask, &pTask->task.undeployMsg, pTask->task.undeployCb);
}
// int32_t stReaderTaskExecute(SStreamReaderTask* pTask, SStreamMsg* pMsg);
// void qStreamSetGroupId(void* pTableListInfo, SSDataBlock* pBlock) {
//   pBlock->ino.id.groupId = tableListGetTableGroupId(pTableListInfo, pBlock->info.id.uid);
// }

void* qStreamGetReaderInfo(int64_t streamId, int64_t taskId, void** taskAddr) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SStreamTask* pTask = NULL;
  STREAM_CHECK_RET_GOTO(streamAcquireTask(streamId, taskId, &pTask, taskAddr));

  pTask->status = STREAM_STATUS_RUNNING;

end:
  STREAM_PRINT_LOG_END(code, lino);
  if (code == TSDB_CODE_SUCCESS) {
    ST_TASK_DLOG("qStreamGetReaderInfo, pTask:%p, info:%p", pTask, ((SStreamReaderTask*)pTask)->info);
    return ((SStreamReaderTask*)pTask)->info;
  }
  terrno = code;
  return NULL;
}


int32_t streamBuildFetchRsp(SArray* pResList, bool hasNext, void** data, size_t* size, int8_t precision) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;

  int32_t blockNum = 0;
  size_t  dataEncodeBufSize = sizeof(SRetrieveTableRsp);
  for(size_t i = 0; i < taosArrayGetSize(pResList); i++){
    SSDataBlock* pBlock = taosArrayGetP(pResList, i);
    if (pBlock == NULL || pBlock->info.rows == 0) continue;
    int32_t blockSize = blockGetInternalEncodeSize(pBlock);
    dataEncodeBufSize += (INT_BYTES * 2 + blockSize);
    blockNum++;
  }
  buf = rpcMallocCont(dataEncodeBufSize);
  STREAM_CHECK_NULL_GOTO(buf, terrno);

  SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)buf;
  pRetrieve->version = 0;
  pRetrieve->precision = precision;
  pRetrieve->compressed = 0;
  pRetrieve->completed = hasNext ? 0 : 1;
  pRetrieve->numOfRows = 0;
  pRetrieve->numOfBlocks = htonl(blockNum);

  char* dataBuf = (char*)(pRetrieve->data);
  for(size_t i = 0; i < taosArrayGetSize(pResList); i++){
    SSDataBlock* pBlock = taosArrayGetP(pResList, i);
    if (pBlock == NULL || pBlock->info.rows == 0) continue;
    int32_t blockSize = blockGetInternalEncodeSize(pBlock);
    *((int32_t*)(dataBuf)) = blockSize;
    *((int32_t*)(dataBuf + INT_BYTES)) = blockSize;
    pRetrieve->numOfRows += pBlock->info.rows;
    int32_t actualLen =
        blockEncodeInternal(pBlock, dataBuf + INT_BYTES * 2, blockSize, taosArrayGetSize(pBlock->pDataBlock));
    STREAM_CHECK_CONDITION_GOTO(actualLen < 0, terrno);
    dataBuf += (INT_BYTES * 2 + actualLen);
  }
  stDebug("stream fetch get result blockNum:%d, rows:%" PRId64, blockNum, pRetrieve->numOfRows);

  pRetrieve->numOfRows = htobe64(pRetrieve->numOfRows);
  
  *data = buf;
  *size = dataEncodeBufSize;
  buf = NULL;

end:
  rpcFreeCont(buf);
  return code;
}

static int32_t encodeReaderFetchBatch(SEncoder* pEncoder, const SArray* pBlocks, int32_t* pBlockCount,
                                      int64_t* pRowCount) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  size_t  itemCount = pBlocks == NULL ? 0 : taosArrayGetSize(pBlocks);
  if (itemCount > STREAM_CALC_REQ_MAX_WIN_NUM || itemCount > INT32_MAX) return TSDB_CODE_INVALID_PARA;

  TAOS_CHECK_EXIT(tEncodeU8(pEncoder, STREAM_READER_FETCH_BATCH_VERSION));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, (int32_t)itemCount));
  for (size_t i = 0; i < itemCount; ++i) {
    const SSDataBlock* pBlock = taosArrayGetP(pBlocks, i);
    if (pBlock == NULL || pBlock->info.id.baseGId == 0) TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);

    const bool isEog = qIsMultiGroupEogBlock(pBlock);
    if (!isEog && pBlock->info.rows <= 0) TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
    TAOS_CHECK_EXIT(tEncodeU8(pEncoder, isEog ? STREAM_RUNNER_FETCH_EOG : STREAM_RUNNER_FETCH_DATA));
    TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pBlock->info.id.baseGId));
    if (isEog) {
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, NULL, 0));
      continue;
    }

    int32_t blockSize = blockGetInternalEncodeSize(pBlock);
    if (blockSize <= 0) TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
    TAOS_CHECK_EXIT(tEncodeU32v(pEncoder, (uint32_t)blockSize));
    if (pEncoder->data != NULL) {
      if ((size_t)blockSize > TD_CODER_REMAIN_CAPACITY(pEncoder)) TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_RANGE);
      int32_t actualLen = blockEncodeInternal(pBlock, (char*)TD_CODER_CURRENT(pEncoder), blockSize,
                                              taosArrayGetSize(pBlock->pDataBlock));
      if (actualLen != blockSize) TAOS_CHECK_EXIT(actualLen < 0 ? terrno : TSDB_CODE_INVALID_MSG);
    }
    pEncoder->pos += blockSize;
    if (pBlockCount != NULL) ++*pBlockCount;
    if (pRowCount != NULL) *pRowCount += pBlock->info.rows;
  }

_exit:
  return code;
}

int32_t streamBuildGroupFetchRsp(const SArray* pBlocks, bool hasNext, void** ppData, size_t* pSize, int8_t precision) {
  SEncoder encoder = {0};
  void*    pBuffer = NULL;
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  blockCount = 0;
  int64_t  rowCount = 0;

  if (ppData == NULL || pSize == NULL) return TSDB_CODE_INVALID_PARA;
  *ppData = NULL;
  *pSize = 0;

  tEncoderInit(&encoder, NULL, 0);
  code = encodeReaderFetchBatch(&encoder, pBlocks, &blockCount, &rowCount);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  if (encoder.pos > INT32_MAX - sizeof(SRetrieveTableRsp)) {
    code = TSDB_CODE_OUT_OF_RANGE;
    goto _exit;
  }
  const int32_t payloadLen = (int32_t)encoder.pos;
  const size_t  totalSize = sizeof(SRetrieveTableRsp) + (size_t)payloadLen;
  tEncoderClear(&encoder);

  pBuffer = rpcMallocCont(totalSize);
  if (pBuffer == NULL) {
    code = terrno;
    goto _exit;
  }
  memset(pBuffer, 0, sizeof(SRetrieveTableRsp));
  SRetrieveTableRsp* pRsp = pBuffer;
  pRsp->version = RETRIEVE_TABLE_RSP_VERSION;
  pRsp->completed = hasNext ? 0 : 1;
  pRsp->precision = precision;
  pRsp->streamBlockType = STREAM_READER_FETCH_BATCH;
  pRsp->payloadLen = htonl(payloadLen);
  pRsp->compLen = htonl(payloadLen);
  pRsp->numOfBlocks = htonl(blockCount);
  pRsp->numOfRows = htobe64(rowCount);

  tEncoderInit(&encoder, pRsp->data, payloadLen);
  code = encodeReaderFetchBatch(&encoder, pBlocks, NULL, NULL);
  if (code != TSDB_CODE_SUCCESS || encoder.pos != (size_t)payloadLen) {
    if (code == TSDB_CODE_SUCCESS) code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  *ppData = pBuffer;
  *pSize = totalSize;
  pBuffer = NULL;

_exit:
  tEncoderClear(&encoder);
  rpcFreeCont(pBuffer);
  return code;
}
