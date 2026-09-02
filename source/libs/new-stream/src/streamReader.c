#include "streamReader.h"
#include <stdint.h>
#include <tdef.h>
#include "osMemPool.h"
#include "osMemory.h"
#include "streamInt.h"
#include "executor.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdef.h"
#include "thash.h"
#include "tsimplehash.h"
#include "tcommon.h"
#include "tmsg.h"

#define STREAM_READER_PERIOD_LOG_BUFFER_SIZE 2048
// Six int64 values, one int32, the longest status, eight uint64 values, two rates, nine optional values, and a bool.
#define STREAM_READER_PERIOD_LOG_VALUE_BYTES (6 * 21 + 11 + 10 + 8 * 20 + 2 * 32 + 9 * 32 + 5)
#define STREAM_READER_PERIOD_LOG_FORMAT                                                                              \
  "record=task_period stream_id=%" PRId64 " task_id=%" PRId64 " serious_id=%" PRId64                                 \
  " node_id=%d task_type=reader status=%s stats_start_at=%" PRId64 " uptime_ms=%" PRId64 " stats_window_ms=%" PRId64 \
  " pull_count=%" PRIu64 " success_count=%" PRIu64 " no_data_count=%" PRIu64 " no_context_count=%" PRIu64            \
  " failure_count=%" PRIu64 " data_rows=%" PRIu64 " data_blocks=%" PRIu64                                            \
  " data_rows_per_sec=%.3f data_blocks_per_sec=%.3f"                                                                 \
  " scan_duration_samples=%" PRIu64                                                                                  \
  " scan_duration_avg_ms=%s scan_duration_max_ms=%s scan_duration_lifetime_max_ms=%s"                                \
  " scan_duration_lifetime_max_at=%s last_returned_wal_ver=%s last_success_at=%s"                                    \
  " active_scan_contexts=%s table_count=%s cache_entries=%s stats_overflow=%s"

enum {
  STREAM_READER_PERIOD_LOG_BUFFER_CHECK =
      1 / (int)(sizeof(STREAM_READER_PERIOD_LOG_FORMAT) + STREAM_READER_PERIOD_LOG_VALUE_BYTES <=
                STREAM_READER_PERIOD_LOG_BUFFER_SIZE)
};

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

  int32_t code = stReaderFormatOptionalMs(
      scanAvgMs, sizeof(scanAvgMs), pPeriod->scanDuration.samples > 0,
      pPeriod->scanDuration.samples > 0
          ? (double)pPeriod->scanDuration.totalUs / (double)pPeriod->scanDuration.samples / 1000.0
          : 0.0);
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
                              tableCount, cacheEntries, pSnapshot->statsOverflow ? "true" : "false");
  if (len < 0 || len >= sizeof(line)) return TSDB_CODE_OUT_OF_BUFFER;

  ST_TASK_DLOG("%s", line);
  return TSDB_CODE_SUCCESS;
}

static const SStreamContextPolicyEntry* stFindReaderContextPolicyEntry(const SStreamContextPolicy* pPolicy, int64_t gid,
                                                                       int32_t paramIndex) {
  const int32_t count = taosArrayGetSize(pPolicy == NULL ? NULL : pPolicy->pEntries);
  for (int32_t i = 0; i < count; ++i) {
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pPolicy->pEntries, i);
    if (pEntry->gid == gid && pEntry->paramIndex == paramIndex) {
      return pEntry;
    }
  }
  return NULL;
}

static int32_t stCountReaderContextBindings(const SStreamAncestorContext* pContext, int32_t actualNodeId,
                                            int32_t readInfoIndex) {
  int32_t       matches = 0;
  const int32_t count = taosArrayGetSize(pContext == NULL ? NULL : pContext->pReadScopeBindings);
  for (int32_t i = 0; i < count; ++i) {
    const SStreamReadScopeBinding* pBinding = taosArrayGet(pContext->pReadScopeBindings, i);
    if (pBinding->vgId == actualNodeId && pBinding->readInfoIndex == readInfoIndex) {
      ++matches;
    }
  }
  return matches;
}

static int32_t stCountReaderAncestorPolicies(const SStreamContextPolicy* pPolicy, int64_t gid) {
  int32_t       matches = 0;
  const int32_t count = taosArrayGetSize(pPolicy == NULL ? NULL : pPolicy->pEntries);
  for (int32_t i = 0; i < count; ++i) {
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pPolicy->pEntries, i);
    if (pEntry->gid == gid && pEntry->contextPolicy == STREAM_CONTEXT_POLICY_ANCESTOR) {
      ++matches;
    }
  }
  return matches;
}

int32_t stProjectReaderCalcContext(const SStreamRuntimeFuncInfo* pSource, int32_t actualNodeId, int32_t readInfoIndex,
                                   int32_t sourceParamIndex, SStreamRuntimeFuncInfo* pTarget) {
  if (pSource == NULL || pTarget == NULL || taosArrayGetSize(pTarget->pStreamPesudoFuncVals) != 1) {
    return TSDB_CODE_INVALID_PARA;
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
  } else {
    if (readInfoIndex < 0 || sourceParamIndex >= 0) {
      return TSDB_CODE_INVALID_PARA;
    }
    const SSTriggerGroupReadInfo* pReadInfo = taosArrayGet(pSource->curGrpRead, readInfoIndex);
    if (pReadInfo == NULL) {
      return TSDB_CODE_INVALID_PARA;
    }
    gid = pReadInfo->gid;
    const int32_t bindingCount = stCountReaderContextBindings(pSource->pAncestorContext, actualNodeId, readInfoIndex);
    if (bindingCount > 1) {
      return TSDB_CODE_INVALID_PARA;
    }
    if (bindingCount == 0 && stCountReaderAncestorPolicies(pSource->pContextPolicy, gid) > 0) {
      return TSDB_CODE_INVALID_PARA;
    }
    if (bindingCount == 1) {
      if (pSource->pContextPolicy == NULL) {
        return TSDB_CODE_INVALID_PARA;
      }
      const int32_t count = taosArrayGetSize(pSource->pContextPolicy->pEntries);
      for (int32_t i = 0; i < count; ++i) {
        const SStreamContextPolicyEntry* pCandidate = taosArrayGet(pSource->pContextPolicy->pEntries, i);
        if (pCandidate->gid != gid || pCandidate->contextPolicy != STREAM_CONTEXT_POLICY_ANCESTOR) {
          continue;
        }
        SStreamAncestorContext* pCandidateContext = NULL;
        code = tProjectStreamAncestorContext(pSource->pAncestorContext, gid, pCandidate->paramIndex, 0,
                                             &pCandidateContext);
        if (code != TSDB_CODE_SUCCESS) {
          tDestroyStreamAncestorContext(&pProjectedContext);
          return code;
        }
        if (stCountReaderContextBindings(pCandidateContext, actualNodeId, readInfoIndex) == 1) {
          if (pSourceEntry != NULL) {
            tDestroyStreamAncestorContext(&pCandidateContext);
            tDestroyStreamAncestorContext(&pProjectedContext);
            return TSDB_CODE_INVALID_PARA;
          }
          pSourceEntry = pCandidate;
          pProjectedContext = pCandidateContext;
        } else {
          tDestroyStreamAncestorContext(&pCandidateContext);
        }
      }
      if (pSourceEntry == NULL) {
        return TSDB_CODE_INVALID_PARA;
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
    stError("stream reader remove table list failed, groupId not exist, key:%"PRIu64, key);
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
  if (p) taosMemoryFreeClear(p->tagData);
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

static void stExtOverrideNamespaceFromFedScan(SStreamReaderTask *pTask, SStreamExtTriggerSpec *pExtSpec,
                                              const SFederatedScanPhysiNode *pFedScan) {
  if (pFedScan == NULL || pFedScan->pExtTable == NULL ||
      nodeType(pFedScan->pExtTable) != QUERY_NODE_EXTERNAL_TABLE) {
    return;
  }

  const SExtTableNode *pExtTable = (const SExtTableNode *)pFedScan->pExtTable;
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
  if (pPlan != NULL && nodeType(pPlan) == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) {
    stExtOverrideNamespaceFromFedScan(pTask, pExtSpec, (SFederatedScanPhysiNode *)pPlan);
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
 * pFedScan != NULL  →  federated-scan path (unwrap STargetNode, use
 *                      pColTypeMappings when available).
 * pFedScan == NULL  →  table-scan path (raw SColumnNode entries).
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
  SNode  *pCalcNode = NULL;
  FOREACH(pCalcNode, pScanCols) {
    // for federated scan, unwrap STargetNode if present
    SNode *pExpr = pCalcNode;
    if (pFedScan && nodeType(pCalcNode) == QUERY_NODE_TARGET) {
      pExpr = ((STargetNode *)pCalcNode)->pExpr;
    }
    if (pExpr == NULL || nodeType(pExpr) != QUERY_NODE_COLUMN) {
      ST_TASK_DLOG("ext: calcScanCols[%d] nodeType=%d exprType=%d skipped %s",
                   calcIdx, (int)nodeType(pCalcNode),
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
    tstrncpy(pMap->colName, pCol->colName, sizeof(pMap->colName));
    ST_TASK_DLOG("ext: calcColumns[%d] col='%s' type=%d bytes=%d %s",
                 calcIdx, pCol->colName, pMap->tdType.type, pMap->tdType.bytes, tag);
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
                                             const char *planStr) {
  int32_t code = TSDB_CODE_SUCCESS;

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

  SNodeList *pScanCols = NULL;
  SFederatedScanPhysiNode *pFedScan = NULL;

  if (ntype == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN ||
      ntype == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
    pScanCols = ((STableScanPhysiNode *)pCalcPlan)->scan.pScanCols;
  } else if (ntype == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) {
    pFedScan = (SFederatedScanPhysiNode *)pCalcPlan;
    pScanCols = pFedScan->pScanCols;
  } else {
    ST_TASK_DLOG("ext: calcCacheScanPlan node type=%d (not TABLE_SCAN/FEDERATED_SCAN), calcColumns empty",
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
                                      (const char *)pMsg->msg.trigger.calcCacheScanPlan);
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
                                                   (const char *)pMsg->msg.calc.calcScanPlan),
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
  if ((*ppTask)->triggerReader == 1) {
    if (STREAM_IS_REF_EXT_SOURCE((*ppTask)->task.flags)) {
      /* Ext-source reader: release the SStreamTriggerReaderInfo (ext version)
       * allocated by streamReaderExtOpen via streamReaderExtClose. */
      stInfo("release ext reader info:%p flags:%" PRId64, (*ppTask)->info, (*ppTask)->task.flags);
      stExtReaderClose((*ppTask)->info);
    } else {
      stInfo("release stream reader info:%p", (*ppTask)->info);
      releaseStreamReaderInfo((*ppTask)->info);
    }
  } else {
    if (STREAM_IS_REF_EXT_SOURCE((*ppTask)->task.flags)) {
      stInfo("release ext calc reader info:%p flags:%" PRId64, (*ppTask)->info, (*ppTask)->task.flags);
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
