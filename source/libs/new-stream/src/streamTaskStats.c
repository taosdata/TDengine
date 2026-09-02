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

#include "streamTaskStats.h"

#include "os.h"
#include "stream.h"

#define STREAM_STATS_BUCKET_EMPTY_SECOND INT64_MIN
#define STREAM_STATS_KNOWN_METRICS                                                               \
  (STREAM_METRIC_PHYSICAL_INPUT | STREAM_METRIC_LOGICAL_INPUT | STREAM_METRIC_DELIVERED_OUTPUT | \
   STREAM_METRIC_RESULT_LATENCY | STREAM_METRIC_REALTIME_LAG | STREAM_METRIC_HISTORY_PROGRESS |  \
   STREAM_METRIC_RECALCULATES)

typedef struct SStreamStatsBucket {
  int64_t  second;
  uint64_t physicalInputRows;
  uint64_t logicalInputRows;
  uint64_t deliveredOutputRows;
  uint64_t resultLatencyUs;
  uint64_t resultLatencySamples;
} SStreamStatsBucket;

struct SStreamTaskStats {
  SRWLatch                   lock;
  EStreamTaskType            taskType;
  uint64_t                   applicableMask;
  int64_t                    startMonoUs;
  int64_t                    startWallMs;
  int64_t                    periodStartMonoUs;
  bool                       overflow;
  bool                       realtimeLagValid;
  int64_t                    realtimeLagMs;
  SStreamReaderGaugeSnapshot readerGauges;
  SStreamRunnerGaugeSnapshot runnerGauges;
  SStreamStatsBucket         buckets[STREAM_STATS_BUCKET_COUNT];
  SStreamReaderPeriodStats   readerPeriod;
  SStreamReaderPeriodStats   readerCumulative;
  SStreamTriggerPeriodStats  triggerPeriod;
  SStreamTriggerPeriodStats  triggerCumulative;
  SStreamRunnerPeriodStats   runnerPeriod;
  SStreamRunnerPeriodStats   runnerCumulative;
};

static uint64_t streamStatsSaturatingAdd(uint64_t current, uint64_t delta, bool *pOverflow) {
  if (UINT64_MAX - current < delta) {
    *pOverflow = true;
    return UINT64_MAX;
  }
  return current + delta;
}

static void streamStatsAdd(SStreamTaskStats *pStats, uint64_t *pCurrent, uint64_t delta) {
  *pCurrent = streamStatsSaturatingAdd(*pCurrent, delta, &pStats->overflow);
}

static void streamStatsRecordDuration(SStreamTaskStats *pStats, SStreamDurationStats *pDuration, uint64_t durationUs,
                                      int64_t nowWallMs) {
  bool firstSample = pDuration->samples == 0;
  streamStatsAdd(pStats, &pDuration->samples, 1);
  streamStatsAdd(pStats, &pDuration->totalUs, durationUs);
  if (firstSample || durationUs > pDuration->maxUs) {
    pDuration->maxUs = durationUs;
    pDuration->maxAtMs = nowWallMs;
  }
}

static SStreamStatsBucket *streamStatsGetBucket(SStreamTaskStats *pStats, int64_t nowMonoUs) {
  if (nowMonoUs < pStats->startMonoUs) return NULL;

  int64_t             second = (nowMonoUs - pStats->startMonoUs) / STREAM_STATS_BUCKET_US;
  int32_t             index = (int32_t)(second % STREAM_STATS_BUCKET_COUNT);
  SStreamStatsBucket *pBucket = &pStats->buckets[index];
  if (pBucket->second == STREAM_STATS_BUCKET_EMPTY_SECOND || pBucket->second < second) {
    memset(pBucket, 0, sizeof(*pBucket));
    pBucket->second = second;
  } else if (pBucket->second > second) {
    return NULL;
  }
  return pBucket;
}

static void streamStatsRecordBucket(SStreamTaskStats *pStats, int64_t nowMonoUs, uint64_t physicalRows,
                                    uint64_t logicalRows, uint64_t outputRows, uint64_t latencyUs,
                                    uint64_t latencySamples) {
  SStreamStatsBucket *pBucket = streamStatsGetBucket(pStats, nowMonoUs);
  if (pBucket == NULL) return;

  streamStatsAdd(pStats, &pBucket->physicalInputRows, physicalRows);
  streamStatsAdd(pStats, &pBucket->logicalInputRows, logicalRows);
  streamStatsAdd(pStats, &pBucket->deliveredOutputRows, outputRows);
  streamStatsAdd(pStats, &pBucket->resultLatencyUs, latencyUs);
  streamStatsAdd(pStats, &pBucket->resultLatencySamples, latencySamples);
}

static bool streamStatsIsTask(const SStreamTaskStats *pStats, EStreamTaskType taskType) {
  return pStats != NULL && pStats->taskType == taskType;
}

static bool streamStatsHasValidTime(const SStreamTaskStats *pStats, int64_t nowMonoUs) {
  return pStats != NULL && nowMonoUs >= pStats->startMonoUs;
}

static uint64_t streamStatsAllowedMetrics(EStreamTaskType taskType) {
  switch (taskType) {
    case STREAM_READER_TASK:
      return STREAM_METRIC_PHYSICAL_INPUT;
    case STREAM_TRIGGER_TASK:
      return STREAM_METRIC_LOGICAL_INPUT | STREAM_METRIC_REALTIME_LAG | STREAM_METRIC_HISTORY_PROGRESS |
             STREAM_METRIC_RECALCULATES;
    case STREAM_RUNNER_TASK:
      return STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY;
  }
  return 0;
}

static bool streamStatsIsValidReaderResult(EStreamReaderResult result) {
  return result >= STREAM_READER_RESULT_SUCCESS && result <= STREAM_READER_RESULT_FAILURE;
}

static bool streamStatsIsValidTriggerEvent(EStreamTriggerEvent event) {
  return event >= STREAM_TRIGGER_EVENT_LOGICAL_WINDOW && event <= STREAM_TRIGGER_EVENT_INVALID_WAL_TIME;
}

static bool streamStatsIsValidRunnerFailure(EStreamRunnerFailure failure) {
  return failure >= STREAM_RUNNER_FAILURE_CALC && failure <= STREAM_RUNNER_FAILURE_NOTIFY;
}

static int64_t streamStatsElapsedUs(int64_t startMonoUs, int64_t nowMonoUs) {
  return nowMonoUs > startMonoUs ? nowMonoUs - startMonoUs : 0;
}

int32_t stTaskStatsCreate(EStreamTaskType taskType, uint64_t applicableMask, int64_t startMonoUs, int64_t startWallMs,
                          SStreamTaskStats **ppStats) {
  if (ppStats == NULL) return TSDB_CODE_INVALID_PARA;
  *ppStats = NULL;
  if (taskType < STREAM_READER_TASK || taskType > STREAM_RUNNER_TASK || startMonoUs < 0 || startWallMs < 0 ||
      (applicableMask & ~STREAM_STATS_KNOWN_METRICS) != 0 ||
      (applicableMask & ~streamStatsAllowedMetrics(taskType)) != 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppStats = taosMemoryCalloc(1, sizeof(**ppStats));
  if (*ppStats == NULL) return terrno;

  taosInitRWLatch(&(*ppStats)->lock);
  (*ppStats)->taskType = taskType;
  (*ppStats)->applicableMask = applicableMask;
  (*ppStats)->startMonoUs = startMonoUs;
  (*ppStats)->startWallMs = startWallMs;
  (*ppStats)->periodStartMonoUs = startMonoUs;
  for (int32_t i = 0; i < STREAM_STATS_BUCKET_COUNT; ++i) {
    (*ppStats)->buckets[i].second = STREAM_STATS_BUCKET_EMPTY_SECOND;
  }
  return TSDB_CODE_SUCCESS;
}

void stTaskStatsDestroy(SStreamTaskStats **ppStats) {
  if (ppStats == NULL || *ppStats == NULL) return;
  taosMemoryFreeClear(*ppStats);
}

int32_t stTaskStatsGetStartWallMs(SStreamTaskStats *pStats, int64_t *pStartWallMs) {
  if (pStats == NULL || pStartWallMs == NULL) return TSDB_CODE_INVALID_PARA;

  taosRLockLatch(&pStats->lock);
  *pStartWallMs = pStats->startWallMs;
  taosRUnLockLatch(&pStats->lock);
  return TSDB_CODE_SUCCESS;
}

void stTaskStatsRecordReaderData(SStreamTaskStats *pStats, uint64_t rows, uint64_t blocks, int64_t nowMonoUs) {
  if (!streamStatsIsTask(pStats, STREAM_READER_TASK) || !streamStatsHasValidTime(pStats, nowMonoUs)) return;

  taosWLockLatch(&pStats->lock);
  streamStatsAdd(pStats, &pStats->readerPeriod.dataRows, rows);
  streamStatsAdd(pStats, &pStats->readerCumulative.dataRows, rows);
  streamStatsAdd(pStats, &pStats->readerPeriod.dataBlocks, blocks);
  streamStatsAdd(pStats, &pStats->readerCumulative.dataBlocks, blocks);
  streamStatsRecordBucket(pStats, nowMonoUs, rows, 0, 0, 0, 0);
  taosWUnLockLatch(&pStats->lock);
}

void stTaskStatsRecordReaderResult(SStreamTaskStats *pStats, EStreamReaderResult result, uint64_t durationUs,
                                   int64_t nowMonoUs, int64_t nowWallMs) {
  if (!streamStatsIsTask(pStats, STREAM_READER_TASK) || !streamStatsHasValidTime(pStats, nowMonoUs) ||
      !streamStatsIsValidReaderResult(result)) {
    return;
  }

  taosWLockLatch(&pStats->lock);
  streamStatsAdd(pStats, &pStats->readerPeriod.pullCount, 1);
  streamStatsAdd(pStats, &pStats->readerCumulative.pullCount, 1);
  switch (result) {
    case STREAM_READER_RESULT_SUCCESS:
      streamStatsAdd(pStats, &pStats->readerPeriod.successCount, 1);
      streamStatsAdd(pStats, &pStats->readerCumulative.successCount, 1);
      break;
    case STREAM_READER_RESULT_NO_DATA:
      streamStatsAdd(pStats, &pStats->readerPeriod.noDataCount, 1);
      streamStatsAdd(pStats, &pStats->readerCumulative.noDataCount, 1);
      break;
    case STREAM_READER_RESULT_NO_CONTEXT:
      streamStatsAdd(pStats, &pStats->readerPeriod.noContextCount, 1);
      streamStatsAdd(pStats, &pStats->readerCumulative.noContextCount, 1);
      break;
    case STREAM_READER_RESULT_FAILURE:
      streamStatsAdd(pStats, &pStats->readerPeriod.failureCount, 1);
      streamStatsAdd(pStats, &pStats->readerCumulative.failureCount, 1);
      break;
  }
  streamStatsRecordDuration(pStats, &pStats->readerPeriod.scanDuration, durationUs, nowWallMs);
  streamStatsRecordDuration(pStats, &pStats->readerCumulative.scanDuration, durationUs, nowWallMs);
  taosWUnLockLatch(&pStats->lock);
}

void stTaskStatsSetReaderGauges(SStreamTaskStats *pStats, int64_t lastReturnedWalVer, int64_t lastSuccessAtMs,
                                int64_t activeScanContexts, int64_t tableCount, int64_t cacheEntries,
                                uint64_t validGaugeMask) {
  if (!streamStatsIsTask(pStats, STREAM_READER_TASK)) return;

  const uint64_t knownMask = STREAM_READER_GAUGE_LAST_WAL | STREAM_READER_GAUGE_LAST_SUCCESS |
                             STREAM_READER_GAUGE_ACTIVE_CONTEXTS | STREAM_READER_GAUGE_TABLE_COUNT |
                             STREAM_READER_GAUGE_CACHE_ENTRIES;
  validGaugeMask &= knownMask;
  taosWLockLatch(&pStats->lock);
  if ((validGaugeMask & STREAM_READER_GAUGE_LAST_WAL) != 0) {
    pStats->readerGauges.lastReturnedWalVer = lastReturnedWalVer;
  }
  if ((validGaugeMask & STREAM_READER_GAUGE_LAST_SUCCESS) != 0) {
    pStats->readerGauges.lastSuccessAtMs = lastSuccessAtMs;
  }
  if ((validGaugeMask & STREAM_READER_GAUGE_ACTIVE_CONTEXTS) != 0) {
    pStats->readerGauges.activeScanContexts = activeScanContexts;
  }
  if ((validGaugeMask & STREAM_READER_GAUGE_TABLE_COUNT) != 0) {
    pStats->readerGauges.tableCount = tableCount;
  }
  if ((validGaugeMask & STREAM_READER_GAUGE_CACHE_ENTRIES) != 0) {
    pStats->readerGauges.cacheEntries = cacheEntries;
  }
  pStats->readerGauges.validMask |= validGaugeMask;
  taosWUnLockLatch(&pStats->lock);
}

void stReaderResponseStatsSetWalData(SStreamReaderResponseStats *pResponse, const SSTriggerWalNewRsp *pWalResponse) {
  if (pResponse == NULL || pWalResponse == NULL) return;

  pResponse->dataRows = 0;
  pResponse->dataBlocks = 0;
  const SSDataBlock *pBlock = pWalResponse->dataBlock;
  if (pBlock == NULL || pBlock->info.rows <= 0) return;

  pResponse->dataRows = (uint64_t)pBlock->info.rows;
  pResponse->dataBlocks = 1;
}

void stReaderTaskRecordPullResult(SStreamReaderTask *pTask, const SStreamReaderResponseStats *pResponse, int32_t code,
                                  int64_t nowMonoUs, int64_t nowWallMs) {
  if (pTask == NULL || pResponse == NULL || pTask->pStats == NULL) return;

  EStreamReaderResult result = STREAM_READER_RESULT_FAILURE;
  uint64_t            validGaugeMask = 0;
  if (pResponse->activeScanContextsValid) validGaugeMask |= STREAM_READER_GAUGE_ACTIVE_CONTEXTS;
  if (pResponse->tableCountValid) validGaugeMask |= STREAM_READER_GAUGE_TABLE_COUNT;
  if (code == TSDB_CODE_SUCCESS) {
    result = STREAM_READER_RESULT_SUCCESS;
    if (pResponse->dataRows != 0 || pResponse->dataBlocks != 0) {
      stTaskStatsRecordReaderData(pTask->pStats, pResponse->dataRows, pResponse->dataBlocks, nowMonoUs);
    }
    if (pResponse->lastReturnedWalVerValid) {
      validGaugeMask |= STREAM_READER_GAUGE_LAST_WAL | STREAM_READER_GAUGE_LAST_SUCCESS;
    }
  } else {
    if (code == TSDB_CODE_STREAM_NO_DATA) {
      result = STREAM_READER_RESULT_NO_DATA;
    } else if (code == TSDB_CODE_STREAM_NO_CONTEXT) {
      result = STREAM_READER_RESULT_NO_CONTEXT;
    }
  }

  stTaskStatsSetReaderGauges(pTask->pStats, pResponse->lastReturnedWalVer, nowWallMs, pResponse->activeScanContexts,
                             pResponse->tableCount, 0, validGaugeMask);
  uint64_t durationUs =
      nowMonoUs > pResponse->requestStartMonoUs ? (uint64_t)(nowMonoUs - pResponse->requestStartMonoUs) : 0;
  stTaskStatsRecordReaderResult(pTask->pStats, result, durationUs, nowMonoUs, nowWallMs);
}

void stTaskStatsRecordTriggerInput(SStreamTaskStats *pStats, uint64_t rows, int64_t nowMonoUs) {
  if (!streamStatsIsTask(pStats, STREAM_TRIGGER_TASK) || !streamStatsHasValidTime(pStats, nowMonoUs)) return;

  taosWLockLatch(&pStats->lock);
  streamStatsRecordBucket(pStats, nowMonoUs, 0, rows, 0, 0, 0);
  taosWUnLockLatch(&pStats->lock);
}

void stTaskStatsRecordTriggerCheck(SStreamTaskStats *pStats, bool history, uint64_t durationUs, int64_t nowMonoUs,
                                   int64_t nowWallMs) {
  if (!streamStatsIsTask(pStats, STREAM_TRIGGER_TASK) || !streamStatsHasValidTime(pStats, nowMonoUs)) return;

  taosWLockLatch(&pStats->lock);
  if (history) {
    streamStatsAdd(pStats, &pStats->triggerPeriod.historyCheckCount, 1);
    streamStatsAdd(pStats, &pStats->triggerCumulative.historyCheckCount, 1);
    streamStatsRecordDuration(pStats, &pStats->triggerPeriod.historyDuration, durationUs, nowWallMs);
    streamStatsRecordDuration(pStats, &pStats->triggerCumulative.historyDuration, durationUs, nowWallMs);
  } else {
    streamStatsAdd(pStats, &pStats->triggerPeriod.realtimeCheckCount, 1);
    streamStatsAdd(pStats, &pStats->triggerCumulative.realtimeCheckCount, 1);
    streamStatsRecordDuration(pStats, &pStats->triggerPeriod.realtimeDuration, durationUs, nowWallMs);
    streamStatsRecordDuration(pStats, &pStats->triggerCumulative.realtimeDuration, durationUs, nowWallMs);
  }
  taosWUnLockLatch(&pStats->lock);
}

void stTaskStatsRecordTriggerEvent(SStreamTaskStats *pStats, EStreamTriggerEvent event, uint64_t count,
                                   int64_t nowMonoUs) {
  if (!streamStatsIsTask(pStats, STREAM_TRIGGER_TASK) || !streamStatsHasValidTime(pStats, nowMonoUs) ||
      !streamStatsIsValidTriggerEvent(event)) {
    return;
  }

  uint64_t *pPeriod = NULL;
  uint64_t *pCumulative = NULL;
  taosWLockLatch(&pStats->lock);
  switch (event) {
    case STREAM_TRIGGER_EVENT_LOGICAL_WINDOW:
      pPeriod = &pStats->triggerPeriod.logicalWindowCount;
      pCumulative = &pStats->triggerCumulative.logicalWindowCount;
      break;
    case STREAM_TRIGGER_EVENT_CALC_REQUEST:
      pPeriod = &pStats->triggerPeriod.calcRequestCount;
      pCumulative = &pStats->triggerCumulative.calcRequestCount;
      break;
    case STREAM_TRIGGER_EVENT_READER_RETRY:
      pPeriod = &pStats->triggerPeriod.readerPullRetryCount;
      pCumulative = &pStats->triggerCumulative.readerPullRetryCount;
      break;
    case STREAM_TRIGGER_EVENT_RUNNER_RETRY:
      pPeriod = &pStats->triggerPeriod.runnerCalcRetryCount;
      pCumulative = &pStats->triggerCumulative.runnerCalcRetryCount;
      break;
    case STREAM_TRIGGER_EVENT_NOTIFY:
      pPeriod = &pStats->triggerPeriod.notifyCount;
      pCumulative = &pStats->triggerCumulative.notifyCount;
      break;
    case STREAM_TRIGGER_EVENT_DROP:
      pPeriod = &pStats->triggerPeriod.dropCount;
      pCumulative = &pStats->triggerCumulative.dropCount;
      break;
    case STREAM_TRIGGER_EVENT_FAILURE:
      pPeriod = &pStats->triggerPeriod.failureCount;
      pCumulative = &pStats->triggerCumulative.failureCount;
      break;
    case STREAM_TRIGGER_EVENT_INVALID_WAL_TIME:
      pPeriod = &pStats->triggerPeriod.invalidWalTimeCount;
      pCumulative = &pStats->triggerCumulative.invalidWalTimeCount;
      break;
  }
  if (pPeriod != NULL) {
    streamStatsAdd(pStats, pPeriod, count);
    streamStatsAdd(pStats, pCumulative, count);
  }
  taosWUnLockLatch(&pStats->lock);
}

void stTaskStatsSetRealtimeLag(SStreamTaskStats *pStats, bool valid, int64_t lagMs) {
  if (!streamStatsIsTask(pStats, STREAM_TRIGGER_TASK)) return;

  taosWLockLatch(&pStats->lock);
  pStats->realtimeLagValid = valid;
  pStats->realtimeLagMs = lagMs;
  taosWUnLockLatch(&pStats->lock);
}

void stTaskStatsRecordRunnerRequest(SStreamTaskStats *pStats, uint64_t logicalWindows, int64_t nowMonoUs,
                                    int64_t nowWallMs) {
  if (!streamStatsIsTask(pStats, STREAM_RUNNER_TASK) || !streamStatsHasValidTime(pStats, nowMonoUs)) return;

  taosWLockLatch(&pStats->lock);
  streamStatsAdd(pStats, &pStats->runnerPeriod.calcRequestCount, 1);
  streamStatsAdd(pStats, &pStats->runnerCumulative.calcRequestCount, 1);
  streamStatsAdd(pStats, &pStats->runnerPeriod.logicalWindowCount, logicalWindows);
  streamStatsAdd(pStats, &pStats->runnerCumulative.logicalWindowCount, logicalWindows);
  if (nowWallMs > pStats->runnerGauges.lastCalcAtMs) pStats->runnerGauges.lastCalcAtMs = nowWallMs;
  taosWUnLockLatch(&pStats->lock);
}

void stTaskStatsRecordRunnerInput(SStreamTaskStats *pStats, uint64_t rows, uint64_t blocks, int64_t nowMonoUs) {
  if (!streamStatsIsTask(pStats, STREAM_RUNNER_TASK) || !streamStatsHasValidTime(pStats, nowMonoUs)) return;

  taosWLockLatch(&pStats->lock);
  streamStatsAdd(pStats, &pStats->runnerPeriod.inputRows, rows);
  streamStatsAdd(pStats, &pStats->runnerCumulative.inputRows, rows);
  streamStatsAdd(pStats, &pStats->runnerPeriod.inputBlocks, blocks);
  streamStatsAdd(pStats, &pStats->runnerCumulative.inputBlocks, blocks);
  taosWUnLockLatch(&pStats->lock);
}

void stTaskStatsRecordRunnerCalcDuration(SStreamTaskStats *pStats, uint64_t durationUs, int64_t nowMonoUs,
                                         int64_t nowWallMs) {
  if (!streamStatsIsTask(pStats, STREAM_RUNNER_TASK) || !streamStatsHasValidTime(pStats, nowMonoUs)) return;

  taosWLockLatch(&pStats->lock);
  streamStatsRecordDuration(pStats, &pStats->runnerPeriod.calcDuration, durationUs, nowWallMs);
  streamStatsRecordDuration(pStats, &pStats->runnerCumulative.calcDuration, durationUs, nowWallMs);
  taosWUnLockLatch(&pStats->lock);
}

void stTaskStatsRecordRunnerWindow(SStreamTaskStats *pStats, bool hasResult, uint64_t latencyUs, int64_t nowMonoUs,
                                   int64_t nowWallMs) {
  if (!streamStatsIsTask(pStats, STREAM_RUNNER_TASK) || !streamStatsHasValidTime(pStats, nowMonoUs)) return;

  taosWLockLatch(&pStats->lock);
  streamStatsRecordDuration(pStats, &pStats->runnerPeriod.resultLatency, latencyUs, nowWallMs);
  streamStatsRecordDuration(pStats, &pStats->runnerCumulative.resultLatency, latencyUs, nowWallMs);
  streamStatsRecordBucket(pStats, nowMonoUs, 0, 0, 0, latencyUs, 1);
  if (nowWallMs > pStats->runnerGauges.lastResultAtMs) pStats->runnerGauges.lastResultAtMs = nowWallMs;
  if (!hasResult) {
    streamStatsAdd(pStats, &pStats->runnerPeriod.noResultWindowCount, 1);
    streamStatsAdd(pStats, &pStats->runnerCumulative.noResultWindowCount, 1);
  }
  taosWUnLockLatch(&pStats->lock);
}

void stTaskStatsRecordRunnerOutput(SStreamTaskStats *pStats, uint64_t rows, uint64_t blocks, int64_t nowMonoUs,
                                   int64_t nowWallMs) {
  if (!streamStatsIsTask(pStats, STREAM_RUNNER_TASK) || !streamStatsHasValidTime(pStats, nowMonoUs)) return;

  taosWLockLatch(&pStats->lock);
  streamStatsAdd(pStats, &pStats->runnerPeriod.outputRows, rows);
  streamStatsAdd(pStats, &pStats->runnerCumulative.outputRows, rows);
  streamStatsAdd(pStats, &pStats->runnerPeriod.outputBlocks, blocks);
  streamStatsAdd(pStats, &pStats->runnerCumulative.outputBlocks, blocks);
  streamStatsRecordBucket(pStats, nowMonoUs, 0, 0, rows, 0, 0);
  if (nowWallMs > pStats->runnerGauges.lastOutputAtMs) pStats->runnerGauges.lastOutputAtMs = nowWallMs;
  taosWUnLockLatch(&pStats->lock);
}

void stTaskStatsRecordRunnerFailure(SStreamTaskStats *pStats, EStreamRunnerFailure failure, int64_t nowMonoUs) {
  if (!streamStatsIsTask(pStats, STREAM_RUNNER_TASK) || !streamStatsHasValidTime(pStats, nowMonoUs) ||
      !streamStatsIsValidRunnerFailure(failure)) {
    return;
  }

  uint64_t *pPeriod = NULL;
  uint64_t *pCumulative = NULL;
  taosWLockLatch(&pStats->lock);
  switch (failure) {
    case STREAM_RUNNER_FAILURE_CALC:
      pPeriod = &pStats->runnerPeriod.calcFailureCount;
      pCumulative = &pStats->runnerCumulative.calcFailureCount;
      break;
    case STREAM_RUNNER_FAILURE_SINK:
      pPeriod = &pStats->runnerPeriod.sinkFailureCount;
      pCumulative = &pStats->runnerCumulative.sinkFailureCount;
      break;
    case STREAM_RUNNER_FAILURE_NOTIFY:
      pPeriod = &pStats->runnerPeriod.notifyFailureCount;
      pCumulative = &pStats->runnerCumulative.notifyFailureCount;
      break;
  }
  if (pPeriod != NULL) {
    streamStatsAdd(pStats, pPeriod, 1);
    streamStatsAdd(pStats, pCumulative, 1);
  }
  taosWUnLockLatch(&pStats->lock);
}

void stTaskStatsSetRunnerGauges(SStreamTaskStats *pStats, int64_t lastCalcAtMs, int64_t lastResultAtMs,
                                int64_t lastOutputAtMs) {
  if (!streamStatsIsTask(pStats, STREAM_RUNNER_TASK)) return;

  taosWLockLatch(&pStats->lock);
  if (lastCalcAtMs > pStats->runnerGauges.lastCalcAtMs) pStats->runnerGauges.lastCalcAtMs = lastCalcAtMs;
  if (lastResultAtMs > pStats->runnerGauges.lastResultAtMs) pStats->runnerGauges.lastResultAtMs = lastResultAtMs;
  if (lastOutputAtMs > pStats->runnerGauges.lastOutputAtMs) pStats->runnerGauges.lastOutputAtMs = lastOutputAtMs;
  taosWUnLockLatch(&pStats->lock);
}

int32_t stTaskStatsSnapshot1m(SStreamTaskStats *pStats, int64_t nowMonoUs, SStreamTaskMetricsSnapshot *pSnapshot) {
  if (pStats == NULL || pSnapshot == NULL) return TSDB_CODE_INVALID_PARA;

  memset(pSnapshot, 0, sizeof(*pSnapshot));
  if (!streamStatsHasValidTime(pStats, nowMonoUs)) return TSDB_CODE_INVALID_PARA;
  taosWLockLatch(&pStats->lock);
  pSnapshot->applicableMask = pStats->applicableMask;
  pSnapshot->windowReady =
      streamStatsElapsedUs(pStats->startMonoUs, nowMonoUs) >= STREAM_STATS_BUCKET_COUNT * STREAM_STATS_BUCKET_US;
  if (pSnapshot->windowReady) {
    int64_t nowSecond = (nowMonoUs - pStats->startMonoUs) / STREAM_STATS_BUCKET_US;
    for (int32_t i = 0; i < STREAM_STATS_BUCKET_COUNT; ++i) {
      int64_t second = nowSecond - STREAM_STATS_BUCKET_COUNT + i;
      if (second < 0) continue;
      SStreamStatsBucket *pBucket = &pStats->buckets[second % STREAM_STATS_BUCKET_COUNT];
      if (pBucket->second != second) continue;
      streamStatsAdd(pStats, &pSnapshot->physicalInputRows1m, pBucket->physicalInputRows);
      streamStatsAdd(pStats, &pSnapshot->logicalInputRows1m, pBucket->logicalInputRows);
      streamStatsAdd(pStats, &pSnapshot->deliveredOutputRows1m, pBucket->deliveredOutputRows);
      streamStatsAdd(pStats, &pSnapshot->resultLatencyUs1m, pBucket->resultLatencyUs);
      streamStatsAdd(pStats, &pSnapshot->resultLatencySamples1m, pBucket->resultLatencySamples);
    }
    pSnapshot->validMask = pStats->applicableMask & (STREAM_METRIC_PHYSICAL_INPUT | STREAM_METRIC_LOGICAL_INPUT |
                                                     STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY);
  }
  if ((pStats->applicableMask & STREAM_METRIC_REALTIME_LAG) != 0 && pStats->realtimeLagValid) {
    pSnapshot->validMask |= STREAM_METRIC_REALTIME_LAG;
    pSnapshot->realtimeLagMs = pStats->realtimeLagMs;
  }
  taosWUnLockLatch(&pStats->lock);
  return TSDB_CODE_SUCCESS;
}

int32_t stTaskStatsRotatePeriod(SStreamTaskStats *pStats, int64_t nowMonoUs, SStreamTaskPeriodSnapshot *pSnapshot,
                                bool *pRotated) {
  if (pStats == NULL || pSnapshot == NULL || pRotated == NULL) return TSDB_CODE_INVALID_PARA;

  memset(pSnapshot, 0, sizeof(*pSnapshot));
  *pRotated = false;
  if (!streamStatsHasValidTime(pStats, nowMonoUs)) return TSDB_CODE_INVALID_PARA;
  taosWLockLatch(&pStats->lock);
  int64_t windowUs = streamStatsElapsedUs(pStats->periodStartMonoUs, nowMonoUs);
  if (windowUs < STREAM_STATS_PERIOD_US) {
    taosWUnLockLatch(&pStats->lock);
    return TSDB_CODE_SUCCESS;
  }

  pSnapshot->taskType = pStats->taskType;
  pSnapshot->statsStartAtMs = pStats->startWallMs;
  pSnapshot->uptimeMs = streamStatsElapsedUs(pStats->startMonoUs, nowMonoUs) / 1000;
  pSnapshot->statsWindowMs = windowUs / 1000;
  pSnapshot->statsOverflow = pStats->overflow;
  switch (pStats->taskType) {
    case STREAM_READER_TASK:
      pSnapshot->readerGauges = pStats->readerGauges;
      pSnapshot->period.reader = pStats->readerPeriod;
      pSnapshot->cumulative.reader = pStats->readerCumulative;
      memset(&pStats->readerPeriod, 0, sizeof(pStats->readerPeriod));
      break;
    case STREAM_TRIGGER_TASK:
      pSnapshot->period.trigger = pStats->triggerPeriod;
      pSnapshot->cumulative.trigger = pStats->triggerCumulative;
      memset(&pStats->triggerPeriod, 0, sizeof(pStats->triggerPeriod));
      break;
    case STREAM_RUNNER_TASK:
      pSnapshot->runnerGauges = pStats->runnerGauges;
      pSnapshot->period.runner = pStats->runnerPeriod;
      pSnapshot->cumulative.runner = pStats->runnerCumulative;
      memset(&pStats->runnerPeriod, 0, sizeof(pStats->runnerPeriod));
      break;
  }
  pStats->periodStartMonoUs = nowMonoUs;
  *pRotated = true;
  taosWUnLockLatch(&pStats->lock);
  return TSDB_CODE_SUCCESS;
}
