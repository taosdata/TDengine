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

#ifndef TDENGINE_STREAM_TASK_STATS_H
#define TDENGINE_STREAM_TASK_STATS_H

#include "streamMsg.h"

#ifdef __cplusplus
extern "C" {
#endif

#define STREAM_STATS_BUCKET_COUNT 60
#define STREAM_STATS_BUCKET_US    1000000LL
#define STREAM_STATS_PERIOD_US    180000000LL

typedef enum EStreamReaderResult {
  STREAM_READER_RESULT_SUCCESS = 0,
  STREAM_READER_RESULT_NO_DATA = 1,
  STREAM_READER_RESULT_NO_CONTEXT = 2,
  STREAM_READER_RESULT_FAILURE = 3,
} EStreamReaderResult;

typedef enum EStreamReaderGauge {
  STREAM_READER_GAUGE_LAST_WAL = 1ULL << 0,
  STREAM_READER_GAUGE_LAST_SUCCESS = 1ULL << 1,
  STREAM_READER_GAUGE_ACTIVE_CONTEXTS = 1ULL << 2,
  STREAM_READER_GAUGE_TABLE_COUNT = 1ULL << 3,
  STREAM_READER_GAUGE_CACHE_ENTRIES = 1ULL << 4,
} EStreamReaderGauge;

typedef enum EStreamTriggerEvent {
  STREAM_TRIGGER_EVENT_LOGICAL_WINDOW = 0,
  STREAM_TRIGGER_EVENT_CALC_REQUEST = 1,
  STREAM_TRIGGER_EVENT_READER_RETRY = 2,
  STREAM_TRIGGER_EVENT_RUNNER_RETRY = 3,
  STREAM_TRIGGER_EVENT_NOTIFY = 4,
  STREAM_TRIGGER_EVENT_DROP = 5,
  STREAM_TRIGGER_EVENT_FAILURE = 6,
  STREAM_TRIGGER_EVENT_INVALID_WAL_TIME = 7,
} EStreamTriggerEvent;

typedef enum EStreamRunnerFailure {
  STREAM_RUNNER_FAILURE_CALC = 0,
  STREAM_RUNNER_FAILURE_SINK = 1,
  STREAM_RUNNER_FAILURE_NOTIFY = 2,
} EStreamRunnerFailure;

typedef enum EStreamTaskStatsLifecycle {
  STREAM_TASK_STATS_DEPLOY_FAILED,
  STREAM_TASK_STATS_UNDEPLOYED,
  STREAM_TASK_STATS_REMOVED,
  STREAM_TASK_STATS_OWNER_DESTROYED,
} EStreamTaskStatsLifecycle;

typedef struct SStreamDurationStats {
  uint64_t samples;
  uint64_t totalUs;
  uint64_t maxUs;
  int64_t  maxAtMs;
} SStreamDurationStats;

typedef struct SStreamReaderPeriodStats {
  uint64_t             pullCount;
  uint64_t             successCount;
  uint64_t             noDataCount;
  uint64_t             noContextCount;
  uint64_t             failureCount;
  uint64_t             dataRows;
  uint64_t             dataBlocks;
  SStreamDurationStats scanDuration;
} SStreamReaderPeriodStats;

typedef struct SStreamReaderGaugeSnapshot {
  int64_t  lastReturnedWalVer;
  int64_t  lastSuccessAtMs;
  int64_t  activeScanContexts;
  int64_t  tableCount;
  int64_t  cacheEntries;
  uint64_t validMask;
} SStreamReaderGaugeSnapshot;

typedef struct SStreamTriggerPeriodStats {
  uint64_t             realtimeCheckCount;
  uint64_t             historyCheckCount;
  uint64_t             logicalWindowCount;
  uint64_t             calcRequestCount;
  uint64_t             readerPullRetryCount;
  uint64_t             runnerCalcRetryCount;
  uint64_t             notifyCount;
  uint64_t             dropCount;
  uint64_t             failureCount;
  uint64_t             invalidWalTimeCount;
  SStreamDurationStats realtimeDuration;
  SStreamDurationStats historyDuration;
} SStreamTriggerPeriodStats;

typedef struct SStreamRunnerPeriodStats {
  uint64_t             calcRequestCount;
  uint64_t             logicalWindowCount;
  uint64_t             inputRows;
  uint64_t             inputBlocks;
  uint64_t             outputRows;
  uint64_t             outputBlocks;
  uint64_t             noResultWindowCount;
  uint64_t             calcFailureCount;
  uint64_t             sinkFailureCount;
  uint64_t             notifyFailureCount;
  SStreamDurationStats calcDuration;
  SStreamDurationStats resultLatency;
} SStreamRunnerPeriodStats;

typedef struct SStreamRunnerGaugeSnapshot {
  int64_t lastCalcAtMs;
  int64_t lastResultAtMs;
  int64_t lastOutputAtMs;
} SStreamRunnerGaugeSnapshot;

typedef struct SStreamTaskPeriodSnapshot {
  EStreamTaskType            taskType;
  int64_t                    statsStartAtMs;
  int64_t                    uptimeMs;
  int64_t                    statsWindowMs;
  bool                       statsOverflow;
  SStreamReaderGaugeSnapshot readerGauges;
  SStreamRunnerGaugeSnapshot runnerGauges;
  union {
    SStreamReaderPeriodStats  reader;
    SStreamTriggerPeriodStats trigger;
    SStreamRunnerPeriodStats  runner;
  } period;
  union {
    SStreamReaderPeriodStats  reader;
    SStreamTriggerPeriodStats trigger;
    SStreamRunnerPeriodStats  runner;
  } cumulative;
} SStreamTaskPeriodSnapshot;

typedef struct SStreamTaskStats SStreamTaskStats;
struct SStreamRunnerTask;

int32_t stTaskStatsCreate(EStreamTaskType taskType, uint64_t applicableMask, int64_t startMonoUs, int64_t startWallMs,
                          SStreamTaskStats **ppStats);
void    stTaskStatsDestroy(SStreamTaskStats **ppStats);
int32_t stTaskStatsGetStartWallMs(SStreamTaskStats *pStats, int64_t *pStartWallMs);
void    streamTaskStatsHandleLifecycle(SStreamTaskStats **ppStats, EStreamTaskStatsLifecycle event);
void    stTaskStatsRecordReaderData(SStreamTaskStats *pStats, uint64_t rows, uint64_t blocks, int64_t nowMonoUs);
void    stTaskStatsRecordReaderResult(SStreamTaskStats *pStats, EStreamReaderResult result, uint64_t durationUs,
                                      int64_t nowMonoUs, int64_t nowWallMs);
void    stTaskStatsSetReaderGauges(SStreamTaskStats *pStats, int64_t lastReturnedWalVer, int64_t lastSuccessAtMs,
                                   int64_t activeScanContexts, int64_t tableCount, int64_t cacheEntries,
                                   uint64_t validGaugeMask);
void    stTaskStatsRecordTriggerInput(SStreamTaskStats *pStats, uint64_t rows, int64_t nowMonoUs);
void    stTaskStatsRecordTriggerCheck(SStreamTaskStats *pStats, bool history, uint64_t durationUs, int64_t nowMonoUs,
                                      int64_t nowWallMs);
void    stTaskStatsRecordTriggerEvent(SStreamTaskStats *pStats, EStreamTriggerEvent event, uint64_t count,
                                      int64_t nowMonoUs);
void    stTaskStatsSetRealtimeLag(SStreamTaskStats *pStats, bool valid, int64_t lagMs);
void    stTaskStatsRecordRunnerRequest(SStreamTaskStats *pStats, uint64_t logicalWindows, int64_t nowMonoUs,
                                       int64_t nowWallMs);
void    stTaskStatsRecordRunnerInput(SStreamTaskStats *pStats, uint64_t rows, uint64_t blocks, int64_t nowMonoUs);
void    stTaskStatsRecordRunnerCalcDuration(SStreamTaskStats *pStats, uint64_t durationUs, int64_t nowMonoUs,
                                            int64_t nowWallMs);
void    stTaskStatsRecordRunnerWindow(SStreamTaskStats *pStats, bool hasResult, uint64_t latencyUs, int64_t nowMonoUs,
                                      int64_t nowWallMs);
void    stTaskStatsRecordRunnerOutput(SStreamTaskStats *pStats, uint64_t rows, uint64_t blocks, int64_t nowMonoUs,
                                      int64_t nowWallMs);
void    stTaskStatsRecordRunnerFailure(SStreamTaskStats *pStats, EStreamRunnerFailure failure, int64_t nowMonoUs);
/** Set positive Runner wall-clock gauges without regression; zero leaves that gauge unchanged. */
void    stTaskStatsSetRunnerGauges(SStreamTaskStats *pStats, int64_t lastCalcAtMs, int64_t lastResultAtMs,
                                   int64_t lastOutputAtMs);
int32_t stTaskStatsSnapshot1m(SStreamTaskStats *pStats, int64_t nowMonoUs, SStreamTaskMetricsSnapshot *pSnapshot);
int32_t stTaskStatsRotatePeriod(SStreamTaskStats *pStats, int64_t nowMonoUs, SStreamTaskPeriodSnapshot *pSnapshot,
                                bool *pRotated);
int32_t stReaderTaskLogStats(SStreamTask *pTask, const SStreamTaskPeriodSnapshot *pSnapshot);
int32_t stRunnerTaskLogStats(struct SStreamRunnerTask *pTask, const SStreamTaskPeriodSnapshot *pSnapshot);

#ifdef __cplusplus
}
#endif

#endif
