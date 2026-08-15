#include <gtest/gtest.h>

#include <atomic>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "stub.h"

extern "C" {
#include "streamInt.h"
#include "streamTaskStats.h"

uint64_t streamTaskMetricMask(const SStreamTask *pTask);
int32_t  streamTaskStatsInit(SStreamTask *pTask, SStreamTaskStats **ppStats);
int32_t  stmHbAddTaskStatus(int64_t streamId, SStreamHbMsg *pMsg, SStreamTask *pTask, SStreamTaskStats *pStats);
int32_t  stmBuildHbStreamsStatusReq(SStreamHbMsg *pMsg);
}

static int32_t                   gLifecycleCallCount = 0;
static EStreamTaskStatsLifecycle gLastLifecycleEvent = STREAM_TASK_STATS_DEPLOY_FAILED;
static bool                      gStatsCreateCalled = false;
static uint64_t                  gStatsCreateApplicableMask = 0;
static int32_t                   gReaderStatsLogCallCount = 0;
static int64_t                   gMonotonicUs = 0;
static std::vector<std::string>  gCapturedLogs;

static void captureStatsLifecycle(SStreamTaskStats **, EStreamTaskStatsLifecycle event) {
  ++gLifecycleCallCount;
  gLastLifecycleEvent = event;
}

static void noOpUndeployCallback(void *) {}

static int32_t captureStatsCreate(EStreamTaskType, uint64_t applicableMask, int64_t, int64_t, SStreamTaskStats **) {
  gStatsCreateCalled = true;
  gStatsCreateApplicableMask = applicableMask;
  return TSDB_CODE_FAILED;
}

static int32_t failStatsSnapshot(SStreamTaskStats *, int64_t, SStreamTaskMetricsSnapshot *) { return TSDB_CODE_FAILED; }

static int32_t failReaderStatsLog(SStreamTask *, const SStreamTaskPeriodSnapshot *) { return TSDB_CODE_FAILED; }

static int32_t countReaderStatsLog(SStreamTask *, const SStreamTaskPeriodSnapshot *) {
  ++gReaderStatsLogCallCount;
  return TSDB_CODE_SUCCESS;
}

static int64_t fixedMonotonicUs() { return gMonotonicUs; }

static void captureTaosPrintLog(const char *, int32_t, int32_t, const char *format, ...) {
  char    buffer[4096] = {0};
  va_list args;
  va_start(args, format);
  int32_t len = vsnprintf(buffer, sizeof(buffer), format, args);
  va_end(args);
  if (len >= 0 && len < sizeof(buffer)) gCapturedLogs.emplace_back(buffer);
}

class ScopedStreamDebugLogCapture {
 public:
  ScopedStreamDebugLogCapture() : previousDebugFlag_(stDebugFlag) {
    gCapturedLogs.clear();
    stub_.set(taosPrintLog, captureTaosPrintLog);
    stDebugFlag = previousDebugFlag_ | DEBUG_DEBUG | DEBUG_FILE;
  }

  ~ScopedStreamDebugLogCapture() {
    stDebugFlag = previousDebugFlag_;
    gCapturedLogs.clear();
  }

 private:
  Stub    stub_;
  int32_t previousDebugFlag_;
};

class StreamTaskStatsDebugTest : public ::testing::Test {
 protected:
  static SStreamTaskPeriodSnapshot emptyReaderSnapshot() {
    SStreamTaskPeriodSnapshot snapshot = {};
    snapshot.taskType = STREAM_READER_TASK;
    snapshot.statsStartAtMs = 1000;
    snapshot.uptimeMs = 180000;
    snapshot.statsWindowMs = 180000;
    return snapshot;
  }

  static SStreamTask readerTask() {
    SStreamTask task = {};
    task.type = STREAM_READER_TASK;
    task.streamId = 1;
    task.taskId = 2;
    task.seriousId = 3;
    task.nodeId = 4;
    task.status = STREAM_STATUS_RUNNING;
    return task;
  }

  static std::string formatReaderPeriod(const SStreamTaskPeriodSnapshot &snapshot) {
    ScopedStreamDebugLogCapture capture;
    SStreamTask                 task = readerTask();
    EXPECT_EQ(stReaderTaskLogStats(&task, &snapshot), TSDB_CODE_SUCCESS);
    EXPECT_EQ(gCapturedLogs.size(), 1);
    return gCapturedLogs.empty() ? std::string{} : gCapturedLogs.front();
  }
};

TEST(StreamTaskStatsTest, OneMinuteNeedsSixtyClosedBuckets) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_LOGICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordTriggerInput(stats, 120, 500000);
  SStreamTaskMetricsSnapshot snapshot = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(stats, 59999999, &snapshot), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(snapshot.windowReady);
  ASSERT_EQ(stTaskStatsSnapshot1m(stats, 60000000, &snapshot), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(snapshot.windowReady);
  EXPECT_EQ(snapshot.logicalInputRows1m, 120);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, EmptyClosedMinuteIsValidZero) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  SStreamTaskMetricsSnapshot snapshot = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(stats, 60000000, &snapshot), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(snapshot.windowReady);
  EXPECT_EQ(snapshot.physicalInputRows1m, 0);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, NonAlignedStartNeedsSixtyClosedBuckets) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_LOGICAL_INPUT, 500000, 1000, &stats),
            TSDB_CODE_SUCCESS);
  stTaskStatsRecordTriggerInput(stats, 8, 60250000);

  SStreamTaskMetricsSnapshot snapshot = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(stats, 60499999, &snapshot), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(snapshot.windowReady);
  ASSERT_EQ(stTaskStatsSnapshot1m(stats, 60500000, &snapshot), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(snapshot.windowReady);
  EXPECT_EQ(snapshot.logicalInputRows1m, 8);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, CurrentSecondIsExcludedFromMinuteSnapshot) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_LOGICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordTriggerInput(stats, 7, 60500000);

  SStreamTaskMetricsSnapshot snapshot = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(stats, 60999999, &snapshot), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(snapshot.windowReady);
  EXPECT_EQ(snapshot.logicalInputRows1m, 0);
  ASSERT_EQ(stTaskStatsSnapshot1m(stats, 61000000, &snapshot), TSDB_CODE_SUCCESS);
  EXPECT_EQ(snapshot.logicalInputRows1m, 7);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, OldSlotUpdateCannotReplaceNewBucket) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_LOGICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordTriggerInput(stats, 11, 1000000);
  stTaskStatsRecordTriggerInput(stats, 22, 61000000);
  stTaskStatsRecordTriggerInput(stats, 99, 1000000);

  SStreamTaskMetricsSnapshot snapshot = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(stats, 62000000, &snapshot), TSDB_CODE_SUCCESS);
  EXPECT_EQ(snapshot.logicalInputRows1m, 22);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, LateRotationProducesOneRealLongPeriod) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_RUNNER_TASK, STREAM_METRIC_DELIVERED_OUTPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordRunnerOutput(stats, 9, 1, 1000000, 2000);
  SStreamTaskPeriodSnapshot period = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(stats, 240000000, &period, &rotated), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(rotated);
  EXPECT_EQ(period.statsWindowMs, 240000);
  EXPECT_EQ(period.period.runner.outputRows, 9);
  ASSERT_EQ(stTaskStatsRotatePeriod(stats, 240600000, &period, &rotated), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(rotated);
  stTaskStatsDestroy(&stats);
}

TEST_F(StreamTaskStatsDebugTest, DebugOffStillRotatesAndDropsCompletedPeriod) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordReaderResult(stats, STREAM_READER_RESULT_SUCCESS, 1000, 1000, 2000);

  SStreamTask                 task = readerTask();
  ScopedStreamDebugLogCapture capture;
  gReaderStatsLogCallCount = 0;
  {
    Stub stub;
    stub.set(stReaderTaskLogStats, countReaderStatsLog);
    ASSERT_EQ(stmMaybeRotateTaskStats(&task, stats, STREAM_STATS_PERIOD_US, false), TSDB_CODE_SUCCESS);
  }
  EXPECT_EQ(gReaderStatsLogCallCount, 0);
  EXPECT_TRUE(gCapturedLogs.empty());
  ASSERT_EQ(stmMaybeRotateTaskStats(&task, stats, 2 * STREAM_STATS_PERIOD_US, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(gCapturedLogs.size(), 1);
  EXPECT_NE(gCapturedLogs.front().find("pull_count=0"), std::string::npos);
  EXPECT_NE(gCapturedLogs.front().find("stats_window_ms=180000"), std::string::npos);
  stTaskStatsDestroy(&stats);
}

TEST_F(StreamTaskStatsDebugTest, DebugEnabledMidWindowPrintsWholeWindow) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordReaderData(stats, 360, 90, 1000);
  stTaskStatsRecordReaderResult(stats, STREAM_READER_RESULT_SUCCESS, 2000, 1000, 2000);

  SStreamTask                 task = readerTask();
  ScopedStreamDebugLogCapture capture;
  ASSERT_EQ(stmMaybeRotateTaskStats(&task, stats, STREAM_STATS_PERIOD_US / 2, false), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(gCapturedLogs.empty());
  ASSERT_EQ(stmMaybeRotateTaskStats(&task, stats, STREAM_STATS_PERIOD_US, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(gCapturedLogs.size(), 1);
  EXPECT_NE(gCapturedLogs.front().find("pull_count=1"), std::string::npos);
  EXPECT_NE(gCapturedLogs.front().find("data_rows=360"), std::string::npos);
  EXPECT_NE(gCapturedLogs.front().find("data_rows_per_sec=2.000"), std::string::npos);
  EXPECT_NE(gCapturedLogs.front().find("data_blocks_per_sec=0.500"), std::string::npos);
  stTaskStatsDestroy(&stats);
}

TEST_F(StreamTaskStatsDebugTest, DebugDisabledMidWindowSkipsOnlyOutput) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordReaderData(stats, 7, 1, 1000);

  SStreamTask                 task = readerTask();
  ScopedStreamDebugLogCapture capture;
  ASSERT_EQ(stmMaybeRotateTaskStats(&task, stats, STREAM_STATS_PERIOD_US / 2, true), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(gCapturedLogs.empty());
  ASSERT_EQ(stmMaybeRotateTaskStats(&task, stats, STREAM_STATS_PERIOD_US, false), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(gCapturedLogs.empty());
  ASSERT_EQ(stmMaybeRotateTaskStats(&task, stats, 2 * STREAM_STATS_PERIOD_US, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(gCapturedLogs.size(), 1);
  EXPECT_NE(gCapturedLogs.front().find("data_rows=0"), std::string::npos);
  stTaskStatsDestroy(&stats);
}

TEST_F(StreamTaskStatsDebugTest, IdleReaderPrintsZeroAndNa) {
  const std::string line = formatReaderPeriod(emptyReaderSnapshot());
  EXPECT_NE(line.find("pull_count=0"), std::string::npos);
  EXPECT_NE(line.find("data_rows_per_sec=0.000"), std::string::npos);
  EXPECT_NE(line.find("scan_duration_samples=0"), std::string::npos);
  EXPECT_NE(line.find("scan_duration_avg_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("scan_duration_max_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("scan_duration_lifetime_max_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("scan_duration_lifetime_max_at=NA"), std::string::npos);
  EXPECT_NE(line.find("last_returned_wal_ver=NA"), std::string::npos);
  EXPECT_NE(line.find("last_success_at=NA"), std::string::npos);
  EXPECT_NE(line.find("active_scan_contexts=NA"), std::string::npos);
  EXPECT_NE(line.find("table_count=NA"), std::string::npos);
  EXPECT_NE(line.find("cache_entries=NA"), std::string::npos);
}

TEST_F(StreamTaskStatsDebugTest, ReaderPeriodContainsEveryCommonIdentityField) {
  SStreamTaskPeriodSnapshot snapshot = emptyReaderSnapshot();
  snapshot.statsOverflow = true;
  snapshot.period.reader = {
      .pullCount = 7,
      .successCount = 3,
      .noDataCount = 1,
      .noContextCount = 1,
      .failureCount = 2,
      .dataRows = 360,
      .dataBlocks = 90,
      .scanDuration = {.samples = 2, .totalUs = 3000, .maxUs = 2000, .maxAtMs = 3000},
  };
  snapshot.cumulative.reader.scanDuration = {.samples = 5, .totalUs = 9000, .maxUs = 5000, .maxAtMs = 4444};
  snapshot.readerGauges = {
      .lastReturnedWalVer = 11,
      .lastSuccessAtMs = 2222,
      .activeScanContexts = 3,
      .tableCount = 4,
      .cacheEntries = 5,
      .validMask = STREAM_READER_GAUGE_LAST_WAL | STREAM_READER_GAUGE_LAST_SUCCESS |
                   STREAM_READER_GAUGE_ACTIVE_CONTEXTS | STREAM_READER_GAUGE_TABLE_COUNT |
                   STREAM_READER_GAUGE_CACHE_ENTRIES,
  };

  const std::string line = formatReaderPeriod(snapshot);
  const char *const requiredFields[] = {
      "record=task_period",
      "stream_id=1",
      "task_id=2",
      "serious_id=3",
      "node_id=4",
      "task_type=reader",
      "status=Running",
      "stats_start_at=1000",
      "uptime_ms=180000",
      "stats_window_ms=180000",
      "pull_count=7",
      "success_count=3",
      "no_data_count=1",
      "no_context_count=1",
      "failure_count=2",
      "data_rows=360",
      "data_blocks=90",
      "data_rows_per_sec=2.000",
      "data_blocks_per_sec=0.500",
      "scan_duration_samples=2",
      "scan_duration_avg_ms=1.500",
      "scan_duration_max_ms=2.000",
      "scan_duration_lifetime_max_ms=5.000",
      "scan_duration_lifetime_max_at=4444",
      "last_returned_wal_ver=11",
      "last_success_at=2222",
      "active_scan_contexts=3",
      "table_count=4",
      "cache_entries=5",
      "stats_overflow=true",
  };
  for (const char *field : requiredFields) {
    EXPECT_NE(line.find(field), std::string::npos) << field;
  }
}

TEST_F(StreamTaskStatsDebugTest, ReaderPeriodDoesNotContainRetryOrQueueDepth) {
  const std::string line = formatReaderPeriod(emptyReaderSnapshot());
  EXPECT_EQ(line.find("queue_depth="), std::string::npos);
  EXPECT_EQ(line.find("retry_count="), std::string::npos);
}

TEST_F(StreamTaskStatsDebugTest, ReaderPeriodKeepsFractionalMicrosecondsInAverage) {
  SStreamTaskPeriodSnapshot snapshot = emptyReaderSnapshot();
  snapshot.period.reader.scanDuration = {.samples = 4, .totalUs = 6003, .maxUs = 2000, .maxAtMs = 3000};
  const std::string line = formatReaderPeriod(snapshot);
  EXPECT_NE(line.find("scan_duration_avg_ms=1.501"), std::string::npos);
}

TEST_F(StreamTaskStatsDebugTest, ReaderPeriodUsesActualLongWindowForRates) {
  SStreamTaskPeriodSnapshot snapshot = emptyReaderSnapshot();
  snapshot.statsWindowMs = 240000;
  snapshot.period.reader.dataRows = 480;
  snapshot.period.reader.dataBlocks = 120;
  const std::string line = formatReaderPeriod(snapshot);
  EXPECT_NE(line.find("data_rows_per_sec=2.000"), std::string::npos);
  EXPECT_NE(line.find("data_blocks_per_sec=0.500"), std::string::npos);
}

TEST_F(StreamTaskStatsDebugTest, ReaderZeroDurationSampleKeepsPeakTime) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordReaderResult(stats, STREAM_READER_RESULT_SUCCESS, 0, 1000, 2345);

  SStreamTaskPeriodSnapshot snapshot = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(stats, STREAM_STATS_PERIOD_US, &snapshot, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(snapshot.period.reader.scanDuration.samples, 1);
  EXPECT_EQ(snapshot.period.reader.scanDuration.maxUs, 0);
  EXPECT_EQ(snapshot.period.reader.scanDuration.maxAtMs, 2345);
  EXPECT_EQ(snapshot.cumulative.reader.scanDuration.maxAtMs, 2345);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, ConcurrentUpdateAndSnapshotKeepsEveryRow) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_RUNNER_TASK, STREAM_METRIC_DELIVERED_OUTPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);

  std::atomic<bool>        start{false};
  std::atomic<bool>        snapshotLoopStarted{false};
  std::atomic<int>         writersReady{0};
  std::atomic<int>         writersEntered{0};
  std::atomic<int>         snapshotCalls{0};
  std::atomic<int>         finished{0};
  std::vector<std::thread> writers;
  for (int i = 0; i < 4; ++i) {
    writers.emplace_back([&] {
      writersReady.fetch_add(1, std::memory_order_release);
      while (!start.load(std::memory_order_acquire)) {
      }
      while (!snapshotLoopStarted.load(std::memory_order_acquire)) {
      }
      writersEntered.fetch_add(1, std::memory_order_release);
      while (snapshotCalls.load(std::memory_order_acquire) == 0) {
      }
      for (int n = 0; n < 10000; ++n) {
        stTaskStatsRecordRunnerOutput(stats, 1, 0, 1000000, 2000);
      }
      finished.fetch_add(1, std::memory_order_release);
    });
  }
  std::thread snapshotter([&] {
    while (!start.load(std::memory_order_acquire)) {
    }
    snapshotLoopStarted.store(true, std::memory_order_release);
    while (writersEntered.load(std::memory_order_acquire) != 4) {
    }
    SStreamTaskMetricsSnapshot snapshot = {};
    while (finished.load(std::memory_order_acquire) != 4) {
      snapshotCalls.fetch_add(1, std::memory_order_release);
      ASSERT_EQ(stTaskStatsSnapshot1m(stats, 61000000, &snapshot), TSDB_CODE_SUCCESS);
    }
  });

  while (writersReady.load(std::memory_order_acquire) != 4) {
  }
  start.store(true, std::memory_order_release);
  for (auto &writer : writers) writer.join();
  snapshotter.join();

  SStreamTaskMetricsSnapshot snapshot = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(stats, 61000000, &snapshot), TSDB_CODE_SUCCESS);
  EXPECT_EQ(snapshot.deliveredOutputRows1m, 40000);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, ResultLatencyKeepsTotalAndSamples) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_RUNNER_TASK, STREAM_METRIC_RESULT_LATENCY, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordRunnerWindow(stats, true, 3, 1000000, 1001);
  stTaskStatsRecordRunnerWindow(stats, true, 5, 2000000, 1002);

  SStreamTaskMetricsSnapshot snapshot = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(stats, 60000000, &snapshot), TSDB_CODE_SUCCESS);
  EXPECT_EQ(snapshot.resultLatencyUs1m, 8);
  EXPECT_EQ(snapshot.resultLatencySamples1m, 2);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, RunnerRequestAndWindowsDoNotDoubleCountAndEmptyResultHasLatency) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_RUNNER_TASK, STREAM_METRIC_RESULT_LATENCY, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordRunnerRequest(stats, 2, 1000000, 2000);
  stTaskStatsRecordRunnerWindow(stats, true, 3, 1000000, 1001);
  stTaskStatsRecordRunnerWindow(stats, false, 5, 2000000, 1002);

  SStreamTaskMetricsSnapshot snapshot = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(stats, 60000000, &snapshot), TSDB_CODE_SUCCESS);
  EXPECT_EQ(snapshot.resultLatencyUs1m, 8);
  EXPECT_EQ(snapshot.resultLatencySamples1m, 2);

  SStreamTaskPeriodSnapshot period = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(stats, STREAM_STATS_PERIOD_US, &period, &rotated), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(rotated);
  EXPECT_EQ(period.period.runner.logicalWindowCount, 2);
  EXPECT_EQ(period.period.runner.noResultWindowCount, 1);
  EXPECT_EQ(period.period.runner.resultLatency.totalUs, 8);
  EXPECT_EQ(period.period.runner.resultLatency.samples, 2);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, RunnerEventsAndLastGaugesShareOneSnapshot) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_RUNNER_TASK, STREAM_METRIC_DELIVERED_OUTPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordRunnerRequest(stats, 2, 1000000, 2000);
  stTaskStatsRecordRunnerWindow(stats, true, 3, 1000000, 3000);
  stTaskStatsRecordRunnerOutput(stats, 9, 1, 1000000, 4000);

  SStreamTaskPeriodSnapshot snapshot = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(stats, STREAM_STATS_PERIOD_US, &snapshot, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(snapshot.period.runner.calcRequestCount, 1);
  EXPECT_EQ(snapshot.period.runner.logicalWindowCount, 2);
  EXPECT_EQ(snapshot.period.runner.resultLatency.samples, 1);
  EXPECT_EQ(snapshot.period.runner.outputRows, 9);
  EXPECT_EQ(snapshot.runnerGauges.lastCalcAtMs, 2000);
  EXPECT_EQ(snapshot.runnerGauges.lastResultAtMs, 3000);
  EXPECT_EQ(snapshot.runnerGauges.lastOutputAtMs, 4000);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, CounterOverflowSaturatesAndSetsFlag) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_RUNNER_TASK, STREAM_METRIC_DELIVERED_OUTPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordRunnerOutput(stats, UINT64_MAX, 0, 1000000, 2000);
  stTaskStatsRecordRunnerOutput(stats, 1, 0, 1000000, 2001);

  SStreamTaskPeriodSnapshot period = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(stats, STREAM_STATS_PERIOD_US, &period, &rotated), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(rotated);
  EXPECT_TRUE(period.statsOverflow);
  EXPECT_EQ(period.period.runner.outputRows, UINT64_MAX);
  EXPECT_EQ(period.cumulative.runner.outputRows, UINT64_MAX);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, ReaderResultClassificationDoesNotInventData) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);

  stTaskStatsRecordReaderResult(stats, STREAM_READER_RESULT_SUCCESS, 500, 3000, 4000);
  stTaskStatsRecordReaderResult(stats, STREAM_READER_RESULT_NO_DATA, 1000, 4000, 5000);
  stTaskStatsRecordReaderResult(stats, STREAM_READER_RESULT_NO_CONTEXT, 2000, 5000, 7000);
  stTaskStatsRecordReaderData(stats, 7, 1, 7000);

  SStreamTaskPeriodSnapshot snapshot = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(stats, STREAM_STATS_PERIOD_US, &snapshot, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(snapshot.period.reader.pullCount, 3);
  EXPECT_EQ(snapshot.period.reader.successCount, 1);
  EXPECT_EQ(snapshot.period.reader.noDataCount, 1);
  EXPECT_EQ(snapshot.period.reader.noContextCount, 1);
  EXPECT_EQ(snapshot.period.reader.dataRows, 7);
  EXPECT_EQ(snapshot.period.reader.dataBlocks, 1);
  EXPECT_EQ(snapshot.period.reader.scanDuration.samples, 3);
  EXPECT_EQ(snapshot.period.reader.scanDuration.totalUs, 3500);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, ReaderResponseBoundaryCountsDuplicateDataExactly) {
  SStreamReaderTask task = {};
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &task.pStats),
            TSDB_CODE_SUCCESS);

  const SStreamReaderResponseStats response = {
      .requestStartMonoUs = 1000,
      .dataRows = 11,
      .dataBlocks = 2,
      .lastReturnedWalVer = 23,
      .activeScanContexts = 3,
      .tableCount = 41,
      .lastReturnedWalVerValid = true,
      .activeScanContextsValid = true,
      .tableCountValid = true,
  };
  stReaderTaskRecordPullResult(&task, &response, TSDB_CODE_SUCCESS, 4000, 5000);
  stReaderTaskRecordPullResult(&task, &response, TSDB_CODE_SUCCESS, 7000, 8000);

  SStreamTaskPeriodSnapshot snapshot = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(task.pStats, STREAM_STATS_PERIOD_US, &snapshot, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(snapshot.period.reader.pullCount, 2);
  EXPECT_EQ(snapshot.period.reader.successCount, 2);
  EXPECT_EQ(snapshot.period.reader.dataRows, 22);
  EXPECT_EQ(snapshot.period.reader.dataBlocks, 4);
  EXPECT_EQ(snapshot.period.reader.scanDuration.samples, 2);
  EXPECT_EQ(snapshot.period.reader.scanDuration.totalUs, 9000);
  EXPECT_EQ(snapshot.readerGauges.lastReturnedWalVer, 23);
  EXPECT_EQ(snapshot.readerGauges.lastSuccessAtMs, 8000);
  EXPECT_EQ(snapshot.readerGauges.activeScanContexts, 3);
  EXPECT_EQ(snapshot.readerGauges.tableCount, 41);
  EXPECT_EQ(snapshot.readerGauges.validMask, STREAM_READER_GAUGE_LAST_WAL | STREAM_READER_GAUGE_LAST_SUCCESS |
                                                 STREAM_READER_GAUGE_ACTIVE_CONTEXTS | STREAM_READER_GAUGE_TABLE_COUNT);
  stTaskStatsDestroy(&task.pStats);
}

TEST(StreamTaskStatsTest, ReaderResponseBoundaryClassifiesEveryResult) {
  SStreamReaderTask task = {};
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &task.pStats),
            TSDB_CODE_SUCCESS);

  SStreamReaderResponseStats response = {
      .requestStartMonoUs = 1000,
      .dataRows = 3,
      .dataBlocks = 1,
  };
  stReaderTaskRecordPullResult(&task, &response, TSDB_CODE_SUCCESS, 2000, 3000);
  response.requestStartMonoUs = 2000;
  response.dataRows = 99;
  response.dataBlocks = 9;
  stReaderTaskRecordPullResult(&task, &response, TSDB_CODE_STREAM_NO_DATA, 4000, 5000);
  response.requestStartMonoUs = 4000;
  stReaderTaskRecordPullResult(&task, &response, TSDB_CODE_STREAM_NO_CONTEXT, 7000, 8000);
  response.requestStartMonoUs = 7000;
  stReaderTaskRecordPullResult(&task, &response, TSDB_CODE_FAILED, 11000, 12000);

  SStreamTaskPeriodSnapshot snapshot = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(task.pStats, STREAM_STATS_PERIOD_US, &snapshot, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(snapshot.period.reader.pullCount, 4);
  EXPECT_EQ(snapshot.period.reader.successCount, 1);
  EXPECT_EQ(snapshot.period.reader.noDataCount, 1);
  EXPECT_EQ(snapshot.period.reader.noContextCount, 1);
  EXPECT_EQ(snapshot.period.reader.failureCount, 1);
  EXPECT_EQ(snapshot.period.reader.dataRows, 3);
  EXPECT_EQ(snapshot.period.reader.dataBlocks, 1);
  EXPECT_EQ(snapshot.period.reader.scanDuration.samples, 4);
  EXPECT_EQ(snapshot.period.reader.scanDuration.totalUs, 10000);
  stTaskStatsDestroy(&task.pStats);
}

TEST(StreamTaskStatsTest, ReaderGaugeSnapshotKeepsIndependentValidityAcrossRotations) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);

  stTaskStatsSetReaderGauges(stats, 17, 2000, 3, 41, 999,
                             STREAM_READER_GAUGE_LAST_WAL | STREAM_READER_GAUGE_LAST_SUCCESS |
                                 STREAM_READER_GAUGE_ACTIVE_CONTEXTS | STREAM_READER_GAUGE_TABLE_COUNT);

  SStreamTaskPeriodSnapshot first = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(stats, STREAM_STATS_PERIOD_US, &first, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(first.readerGauges.validMask, STREAM_READER_GAUGE_LAST_WAL | STREAM_READER_GAUGE_LAST_SUCCESS |
                                              STREAM_READER_GAUGE_ACTIVE_CONTEXTS | STREAM_READER_GAUGE_TABLE_COUNT);
  EXPECT_EQ(first.readerGauges.lastReturnedWalVer, 17);
  EXPECT_EQ(first.readerGauges.lastSuccessAtMs, 2000);
  EXPECT_EQ(first.readerGauges.activeScanContexts, 3);
  EXPECT_EQ(first.readerGauges.tableCount, 41);
  EXPECT_EQ(first.readerGauges.cacheEntries, 0);
  EXPECT_EQ(first.readerGauges.validMask & STREAM_READER_GAUGE_CACHE_ENTRIES, 0);

  stTaskStatsSetReaderGauges(stats, 0, 0, 5, 0, 0, STREAM_READER_GAUGE_ACTIVE_CONTEXTS);
  SStreamTaskPeriodSnapshot second = {};
  ASSERT_EQ(stTaskStatsRotatePeriod(stats, 2 * STREAM_STATS_PERIOD_US, &second, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(second.readerGauges.lastReturnedWalVer, 17);
  EXPECT_EQ(second.readerGauges.lastSuccessAtMs, 2000);
  EXPECT_EQ(second.readerGauges.activeScanContexts, 5);
  EXPECT_EQ(second.readerGauges.tableCount, 41);
  EXPECT_EQ(second.readerGauges.validMask, first.readerGauges.validMask);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, FailedReaderResponseDoesNotAdvanceWalGauge) {
  SStreamReaderTask task = {};
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &task.pStats),
            TSDB_CODE_SUCCESS);

  SStreamReaderResponseStats response = {
      .requestStartMonoUs = 1000,
      .lastReturnedWalVer = 0,
      .lastReturnedWalVerValid = true,
  };
  stReaderTaskRecordPullResult(&task, &response, TSDB_CODE_SUCCESS, 2000, 3000);
  response.requestStartMonoUs = 2000;
  response.lastReturnedWalVer = 99;
  stReaderTaskRecordPullResult(&task, &response, TSDB_CODE_FAILED, 4000, 5000);

  SStreamTaskPeriodSnapshot snapshot = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(task.pStats, STREAM_STATS_PERIOD_US, &snapshot, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(snapshot.readerGauges.lastReturnedWalVer, 0);
  EXPECT_EQ(snapshot.readerGauges.lastSuccessAtMs, 3000);
  EXPECT_EQ(snapshot.readerGauges.validMask & (STREAM_READER_GAUGE_LAST_WAL | STREAM_READER_GAUGE_LAST_SUCCESS),
            STREAM_READER_GAUGE_LAST_WAL | STREAM_READER_GAUGE_LAST_SUCCESS);
  stTaskStatsDestroy(&task.pStats);
}

TEST(StreamTaskStatsTest, ReaderWalResponseBoundaryUsesFinalFilteredBlockRows) {
  SStreamReaderTask task = {};
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &task.pStats),
            TSDB_CODE_SUCCESS);

  SSDataBlock finalBlock = {};
  finalBlock.info.rows = 4;
  const SSTriggerWalNewRsp walResponse = {
      .dataBlock = &finalBlock,
      .totalDataRows = 9,
  };
  SStreamReaderResponseStats response = {
      .requestStartMonoUs = 1000,
  };
  stReaderResponseStatsSetWalData(&response, &walResponse);
  stReaderTaskRecordPullResult(&task, &response, TSDB_CODE_SUCCESS, 2000, 3000);

  SStreamTaskPeriodSnapshot snapshot = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(task.pStats, STREAM_STATS_PERIOD_US, &snapshot, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(snapshot.period.reader.dataRows, 4);
  EXPECT_EQ(snapshot.period.reader.dataBlocks, 1);
  stTaskStatsDestroy(&task.pStats);
}

TEST(StreamTaskStatsTest, NewInstanceStartsEmpty) {
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_RUNNER_TASK, STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY, 0,
                              1000, &stats),
            TSDB_CODE_SUCCESS);

  SStreamTaskMetricsSnapshot snapshot = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(stats, 60000000, &snapshot), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(snapshot.windowReady);
  EXPECT_EQ(snapshot.deliveredOutputRows1m, 0);
  EXPECT_EQ(snapshot.resultLatencyUs1m, 0);
  EXPECT_EQ(snapshot.resultLatencySamples1m, 0);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, InvalidRoleMaskIsRejected) {
  SStreamTaskStats *stats = nullptr;
  EXPECT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_LOGICAL_INPUT, 0, 1000, &stats),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(stats, nullptr);
  EXPECT_EQ(stTaskStatsCreate(STREAM_RUNNER_TASK, 1ULL << 63, 0, 1000, &stats), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(stats, nullptr);
  EXPECT_EQ(stTaskStatsCreate(STREAM_RUNNER_TASK, STREAM_METRIC_DELIVERED_OUTPUT, -1, 1000, &stats),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(stats, nullptr);
  EXPECT_EQ(stTaskStatsCreate(STREAM_RUNNER_TASK, STREAM_METRIC_DELIVERED_OUTPUT, 0, -1, &stats),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(stats, nullptr);
}

TEST(StreamTaskStatsTest, InvalidEventsAndEarlyEventsLeaveStatsEmpty) {
  SStreamTaskStats *reader = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 10, 1000, &reader), TSDB_CODE_SUCCESS);
  stTaskStatsRecordReaderResult(reader, static_cast<EStreamReaderResult>(99), 3, 10, 1001);
  stTaskStatsRecordReaderResult(reader, STREAM_READER_RESULT_SUCCESS, 3, 9, 1001);
  stTaskStatsRecordReaderData(reader, 7, 2, 9);
  SStreamTaskPeriodSnapshot period = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(reader, STREAM_STATS_PERIOD_US + 10, &period, &rotated), TSDB_CODE_SUCCESS);
  EXPECT_EQ(period.period.reader.pullCount, 0);
  EXPECT_EQ(period.period.reader.successCount, 0);
  EXPECT_EQ(period.period.reader.dataRows, 0);
  EXPECT_EQ(period.period.reader.scanDuration.samples, 0);
  stTaskStatsDestroy(&reader);

  SStreamTaskStats *trigger = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_LOGICAL_INPUT, 10, 1000, &trigger), TSDB_CODE_SUCCESS);
  stTaskStatsRecordTriggerEvent(trigger, static_cast<EStreamTriggerEvent>(99), 3, 10);
  stTaskStatsRecordTriggerInput(trigger, 7, 9);
  SStreamTaskMetricsSnapshot triggerSnapshot = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(trigger, 60000010, &triggerSnapshot), TSDB_CODE_SUCCESS);
  EXPECT_EQ(triggerSnapshot.logicalInputRows1m, 0);
  stTaskStatsDestroy(&trigger);

  SStreamTaskStats *runner = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_RUNNER_TASK, STREAM_METRIC_DELIVERED_OUTPUT, 10, 1000, &runner),
            TSDB_CODE_SUCCESS);
  stTaskStatsRecordRunnerFailure(runner, static_cast<EStreamRunnerFailure>(99), 10);
  stTaskStatsRecordRunnerOutput(runner, 7, 2, 9, 2000);
  ASSERT_EQ(stTaskStatsRotatePeriod(runner, STREAM_STATS_PERIOD_US + 10, &period, &rotated), TSDB_CODE_SUCCESS);
  EXPECT_EQ(period.period.runner.outputRows, 0);
  EXPECT_EQ(period.period.runner.outputBlocks, 0);
  EXPECT_EQ(period.period.runner.calcFailureCount, 0);
  stTaskStatsDestroy(&runner);
}

TEST(StreamTaskStatsTest, TaskMetricApplicabilityMatchesRole) {
  SStreamTask reader = {};
  reader.type = STREAM_READER_TASK;

  SStreamTask trigger = {};
  trigger.type = STREAM_TRIGGER_TASK;

  SStreamRunnerTask topRunner = {};
  topRunner.task.type = STREAM_RUNNER_TASK;
  topRunner.topTask = true;

  SStreamRunnerTask nonTopRunner = {};
  nonTopRunner.task.type = STREAM_RUNNER_TASK;

  EXPECT_EQ(streamTaskMetricMask(&reader), STREAM_METRIC_PHYSICAL_INPUT);
  EXPECT_EQ(streamTaskMetricMask(&trigger), STREAM_METRIC_LOGICAL_INPUT | STREAM_METRIC_REALTIME_LAG |
                                                STREAM_METRIC_HISTORY_PROGRESS | STREAM_METRIC_RECALCULATES);
  EXPECT_EQ(streamTaskMetricMask(&topRunner.task), STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY);
  EXPECT_EQ(streamTaskMetricMask(&nonTopRunner.task), 0);
}

TEST(StreamTaskStatsTest, RuntimeTopIdentityControlsRunnerMetrics) {
  SStreamRunnerTask finalRunner = {};
  finalRunner.task.type = STREAM_RUNNER_TASK;
  finalRunner.task.flags = STREAM_FLAG_REDEPLOY_RUNNER;
  finalRunner.topTask = true;

  ASSERT_EQ(streamTaskStatsInit(&finalRunner.task, &finalRunner.pStats), TSDB_CODE_SUCCESS);
  int64_t nowMonoUs = streamTaskGetMonotonicUs();
  stTaskStatsRecordRunnerOutput(finalRunner.pStats, 17, 1, nowMonoUs, 2000);
  stTaskStatsRecordRunnerWindow(finalRunner.pStats, true, 5000, nowMonoUs, 1000);

  SStreamTaskMetricsSnapshot finalSnapshot = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(finalRunner.pStats, nowMonoUs + 60000000, &finalSnapshot), TSDB_CODE_SUCCESS);
  EXPECT_EQ(finalSnapshot.applicableMask, STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY);
  EXPECT_EQ(finalSnapshot.validMask, STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY);
  EXPECT_EQ(finalSnapshot.deliveredOutputRows1m, 17);
  EXPECT_EQ(finalSnapshot.resultLatencyUs1m, 5000);
  EXPECT_EQ(finalSnapshot.resultLatencySamples1m, 1);

  SStreamRunnerTask scalarRoot = {};
  scalarRoot.task.type = STREAM_RUNNER_TASK;
  scalarRoot.task.flags = STREAM_FLAG_TOP_RUNNER;
  scalarRoot.topTask = false;

  ASSERT_EQ(streamTaskStatsInit(&scalarRoot.task, &scalarRoot.pStats), TSDB_CODE_SUCCESS);
  nowMonoUs = streamTaskGetMonotonicUs();
  stTaskStatsRecordRunnerOutput(scalarRoot.pStats, 23, 1, nowMonoUs, 2000);
  stTaskStatsRecordRunnerWindow(scalarRoot.pStats, true, 7000, nowMonoUs, 1000);

  SStreamTaskMetricsSnapshot scalarSnapshot = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(scalarRoot.pStats, nowMonoUs + 60000000, &scalarSnapshot), TSDB_CODE_SUCCESS);
  EXPECT_EQ(scalarSnapshot.applicableMask, 0);
  EXPECT_EQ(scalarSnapshot.validMask, 0);

  stTaskStatsDestroy(&finalRunner.pStats);
  stTaskStatsDestroy(&scalarRoot.pStats);
}

TEST(StreamTaskStatsTest, RunnerDeploySetsTopIdentityBeforeStatsInit) {
  SStreamRunnerTask task = {};
  task.task.type = STREAM_RUNNER_TASK;
  task.task.flags = STREAM_FLAG_REDEPLOY_RUNNER;

  char                   name[] = "stream";
  char                   plan[] = "plan";
  char                   db[] = "db";
  char                   table[] = "table";
  SStreamRunnerDeployMsg deploy = {};
  deploy.streamName = name;
  deploy.pPlan = plan;
  deploy.outDBFName = db;
  deploy.outTblName = table;
  deploy.topPlan = true;

  gStatsCreateCalled = false;
  gStatsCreateApplicableMask = 0;
  {
    Stub stub;
    stub.set(stTaskStatsCreate, captureStatsCreate);
    EXPECT_EQ(stRunnerTaskDeploy(&task, &deploy), TSDB_CODE_FAILED);
  }
  EXPECT_TRUE(gStatsCreateCalled);
  EXPECT_TRUE(task.topTask);
  EXPECT_EQ(gStatsCreateApplicableMask, STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY);
  EXPECT_EQ(task.pStats, nullptr);
}

TEST(StreamTaskStatsTest, LifecycleDefersCleanupUntilOwnerRemoval) {
  SStreamTask reader = {};
  reader.type = STREAM_READER_TASK;
  SStreamTask trigger = {};
  trigger.type = STREAM_TRIGGER_TASK;
  SStreamRunnerTask runner = {};
  runner.task.type = STREAM_RUNNER_TASK;
  runner.topTask = true;
  SStreamTask      *tasks[] = {&reader, &trigger, &runner.task};
  SStreamTaskStats *stats[3] = {};
  const uint64_t    expectedMasks[] = {
      STREAM_METRIC_PHYSICAL_INPUT,
      STREAM_METRIC_LOGICAL_INPUT | STREAM_METRIC_REALTIME_LAG | STREAM_METRIC_HISTORY_PROGRESS |
          STREAM_METRIC_RECALCULATES,
      STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY,
  };

  for (int32_t i = 0; i < 3; ++i) {
    ASSERT_EQ(streamTaskStatsInit(tasks[i], &stats[i]), TSDB_CODE_SUCCESS);
    ASSERT_NE(stats[i], nullptr);

    SStreamTaskMetricsSnapshot snapshot = {};
    ASSERT_EQ(stTaskStatsSnapshot1m(stats[i], INT64_MAX, &snapshot), TSDB_CODE_SUCCESS);
    EXPECT_EQ(snapshot.applicableMask, expectedMasks[i]);
  }

  streamTaskStatsHandleLifecycle(&stats[0], STREAM_TASK_STATS_DEPLOY_FAILED);
  EXPECT_EQ(stats[0], nullptr);

  streamTaskStatsHandleLifecycle(&stats[1], STREAM_TASK_STATS_UNDEPLOYED);
  ASSERT_NE(stats[1], nullptr);
  streamTaskStatsHandleLifecycle(&stats[1], STREAM_TASK_STATS_REMOVED);
  EXPECT_EQ(stats[1], nullptr);

  streamTaskStatsHandleLifecycle(&stats[2], STREAM_TASK_STATS_UNDEPLOYED);
  ASSERT_NE(stats[2], nullptr);
  streamTaskStatsHandleLifecycle(&stats[2], STREAM_TASK_STATS_OWNER_DESTROYED);
  EXPECT_EQ(stats[2], nullptr);
  streamTaskStatsHandleLifecycle(&stats[2], STREAM_TASK_STATS_OWNER_DESTROYED);
}

TEST(StreamTaskStatsTest, ReaderDeployFailureCleansRealOwnerStats) {
  SStreamReaderTask task = {};
  task.task.type = STREAM_READER_TASK;

  char                   invalidPlan[] = "{";
  SStreamReaderDeployMsg deploy = {};
  deploy.msg.calc.execReplica = 1;
  deploy.msg.calc.calcScanPlan = invalidPlan;

  EXPECT_EQ(stReaderTaskDeploy(&task, &deploy), TSDB_CODE_FAILED);
  EXPECT_EQ(task.task.status, STREAM_STATUS_FAILED);
  EXPECT_EQ(task.info, nullptr);
  EXPECT_EQ(task.pStats, nullptr);
}

TEST(StreamTaskStatsTest, ReaderUndeployDefersStatsUntilRealListRemoval) {
  constexpr int64_t streamId = 41;
  constexpr int64_t taskId = 42;
  constexpr int64_t seriousId = 43;

  SStreamInfo stream = {};
  stream.taskNum = 2;
  stream.readerList = tdListNew(sizeof(SStreamReaderTask));
  ASSERT_NE(stream.readerList, nullptr);

  auto *task = static_cast<SStreamReaderTask *>(tdListReserve(stream.readerList));
  ASSERT_NE(task, nullptr);
  task->task.type = STREAM_READER_TASK;
  task->task.streamId = streamId;
  task->task.taskId = taskId;
  task->task.seriousId = seriousId;
  task->task.undeployCb = noOpUndeployCallback;
  ASSERT_EQ(streamTaskStatsInit(&task->task, &task->pStats), TSDB_CODE_SUCCESS);
  ASSERT_NE(task->pStats, nullptr);

  SStreamReaderTask *owner = task;
  gLifecycleCallCount = 0;
  {
    Stub stub;
    stub.set(streamTaskStatsHandleLifecycle, captureStatsLifecycle);
    ASSERT_EQ(stReaderTaskUndeploy(&owner, true), TSDB_CODE_SUCCESS);
  }
  EXPECT_EQ(owner, task);
  EXPECT_EQ(gLifecycleCallCount, 1);
  EXPECT_EQ(gLastLifecycleEvent, STREAM_TASK_STATS_UNDEPLOYED);
  ASSERT_NE(task->pStats, nullptr);

  stream.undeployReaders = taosArrayInit(1, sizeof(int64_t) * 2);
  ASSERT_NE(stream.undeployReaders, nullptr);
  int64_t identity[2] = {taskId, seriousId};
  ASSERT_NE(taosArrayPush(stream.undeployReaders, identity), nullptr);

  taosWLockLatch(&stream.lock);
  smHandleRemovedTask(&stream, streamId, 0, STREAM_READER_TASK, stream.undeployReaders, stream.readerList);
  taosWUnLockLatch(&stream.lock);

  EXPECT_EQ(TD_DLIST_NELES(stream.readerList), 0);
  EXPECT_EQ(taosArrayGetSize(stream.undeployReaders), 0);
  EXPECT_EQ(stream.taskNum, 1);

  taosArrayDestroy(stream.undeployReaders);
  stream.readerList = static_cast<SList *>(tdListFree(stream.readerList));
}

TEST(StreamTaskStatsTest, HeartbeatMetricsAreInitializedBeforeTraversal) {
  SStreamHbMsg heartbeat = {};
  ASSERT_EQ(stmBuildHbStreamsStatusReq(&heartbeat), TSDB_CODE_SUCCESS);
  EXPECT_EQ(heartbeat.observabilityVersion, STREAM_HB_OBSERVABILITY_VERSION_V1);
  ASSERT_NE(heartbeat.pTaskMetrics, nullptr);
  EXPECT_EQ(taosArrayGetSize(heartbeat.pTaskMetrics), 0);
  tCleanupStreamHbMsg(&heartbeat, true);
}

TEST_F(StreamTaskStatsDebugTest, HeartbeatRotatesBeforeMetricSnapshotFailure) {
  SStreamTask       task = readerTask();
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordReaderData(stats, 7, 1, 1000);

  SStreamHbMsg heartbeat = {};
  heartbeat.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(heartbeat.pStreamStatus, nullptr);
  {
    Stub stub;
    stub.set(stTaskStatsSnapshot1m, failStatsSnapshot);
    EXPECT_EQ(stmHbAddTaskStatus(task.streamId, &heartbeat, &task, stats), TSDB_CODE_SUCCESS);
  }
  EXPECT_EQ(taosArrayGetSize(heartbeat.pStreamStatus), 1);
  EXPECT_EQ(taosArrayGetSize(heartbeat.pTaskMetrics), 0);

  SStreamTaskPeriodSnapshot period = {};
  bool                      rotated = true;
  ASSERT_EQ(stTaskStatsRotatePeriod(stats, streamTaskGetMonotonicUs(), &period, &rotated), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(rotated);

  tCleanupStreamHbMsg(&heartbeat, true);
  stTaskStatsDestroy(&stats);
}

TEST_F(StreamTaskStatsDebugTest, HeartbeatUsesRealDebugFlagToggleWithoutChangingRotation) {
  SStreamTask       task = readerTask();
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordReaderData(stats, 7, 1, 1000);

  ScopedStreamDebugLogCapture capture;
  Stub                        stub;
  stub.set(streamTaskGetMonotonicUs, fixedMonotonicUs);
  stDebugFlag &= ~DEBUG_DEBUG;
  gMonotonicUs = STREAM_STATS_PERIOD_US;

  SStreamHbMsg heartbeat = {};
  heartbeat.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(heartbeat.pStreamStatus, nullptr);
  ASSERT_EQ(stmHbAddTaskStatus(task.streamId, &heartbeat, &task, stats), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(gCapturedLogs.empty());
  tCleanupStreamHbMsg(&heartbeat, true);

  stTaskStatsRecordReaderData(stats, 8, 2, STREAM_STATS_PERIOD_US + 1);
  stDebugFlag |= DEBUG_DEBUG;
  gMonotonicUs = 2 * STREAM_STATS_PERIOD_US;
  heartbeat.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(heartbeat.pStreamStatus, nullptr);
  ASSERT_EQ(stmHbAddTaskStatus(task.streamId, &heartbeat, &task, stats), TSDB_CODE_SUCCESS);
  ASSERT_EQ(gCapturedLogs.size(), 1);
  EXPECT_NE(gCapturedLogs.front().find("data_rows=8"), std::string::npos);
  EXPECT_NE(gCapturedLogs.front().find("stats_window_ms=180000"), std::string::npos);

  tCleanupStreamHbMsg(&heartbeat, true);
  stTaskStatsDestroy(&stats);
}

TEST_F(StreamTaskStatsDebugTest, HeartbeatLogFailureKeepsStatusAndTaskState) {
  SStreamTask       task = readerTask();
  SStreamTaskStats *stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_READER_TASK, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  stTaskStatsRecordReaderData(stats, 7, 1, 1000);

  SStreamHbMsg heartbeat = {};
  heartbeat.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(heartbeat.pStreamStatus, nullptr);
  ScopedStreamDebugLogCapture capture;
  {
    Stub stub;
    stub.set(stReaderTaskLogStats, failReaderStatsLog);
    EXPECT_EQ(stmHbAddTaskStatus(task.streamId, &heartbeat, &task, stats), TSDB_CODE_SUCCESS);
  }
  ASSERT_EQ(gCapturedLogs.size(), 1);
  EXPECT_NE(gCapturedLogs.front().find("failed to rotate or log task statistics"), std::string::npos);
  ASSERT_EQ(taosArrayGetSize(heartbeat.pStreamStatus), 1);
  EXPECT_EQ(taosArrayGetSize(heartbeat.pTaskMetrics), 1);
  const auto *status = static_cast<const SStmTaskStatusMsg *>(taosArrayGet(heartbeat.pStreamStatus, 0));
  ASSERT_NE(status, nullptr);
  EXPECT_EQ(status->streamId, task.streamId);
  EXPECT_EQ(status->taskId, task.taskId);
  EXPECT_EQ(status->seriousId, task.seriousId);
  EXPECT_EQ(status->status, STREAM_STATUS_RUNNING);
  EXPECT_EQ(task.status, STREAM_STATUS_RUNNING);

  tCleanupStreamHbMsg(&heartbeat, true);
  stTaskStatsDestroy(&stats);
}

TEST(StreamTaskStatsTest, HeartbeatContainsOneEntryForEveryTaskStatus) {
  SStreamTask reader = {};
  reader.type = STREAM_READER_TASK;
  reader.streamId = 1;
  reader.taskId = 11;
  reader.seriousId = 111;
  SStreamTaskStats *readerStats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(reader.type, STREAM_METRIC_PHYSICAL_INPUT, 0, 1000, &readerStats), TSDB_CODE_SUCCESS);

  SStreamTriggerTask trigger = {};
  trigger.task.type = STREAM_TRIGGER_TASK;
  trigger.task.streamId = 1;
  trigger.task.taskId = 12;
  trigger.task.seriousId = 112;
  taosInitRWLatch(&trigger.readerProgressLock);
  trigger.pReaderProgressSnapshots = taosArrayInit(0, sizeof(SStreamReaderProgressSnapshot));
  ASSERT_NE(trigger.pReaderProgressSnapshots, nullptr);
  ASSERT_EQ(stTaskStatsCreate(trigger.task.type,
                              STREAM_METRIC_LOGICAL_INPUT | STREAM_METRIC_REALTIME_LAG |
                                  STREAM_METRIC_HISTORY_PROGRESS | STREAM_METRIC_RECALCULATES,
                              0, 1000, &trigger.pStats),
            TSDB_CODE_SUCCESS);

  SStreamTask nonTopRunner = {};
  nonTopRunner.type = STREAM_RUNNER_TASK;
  nonTopRunner.streamId = 1;
  nonTopRunner.taskId = 13;
  nonTopRunner.seriousId = 113;
  SStreamTaskStats *nonTopRunnerStats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(nonTopRunner.type, 0, 0, 1000, &nonTopRunnerStats), TSDB_CODE_SUCCESS);

  SStreamTask topRunner = {};
  topRunner.type = STREAM_RUNNER_TASK;
  topRunner.streamId = 1;
  topRunner.taskId = 14;
  topRunner.seriousId = 114;
  topRunner.flags = 1 << 1;
  SStreamTaskStats *topRunnerStats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(topRunner.type, STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY, 0, 1000,
                              &topRunnerStats),
            TSDB_CODE_SUCCESS);

  SStreamHbMsg heartbeat = {};
  heartbeat.pStreamStatus = taosArrayInit(4, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(heartbeat.pStreamStatus, nullptr);
  ASSERT_EQ(stmHbAddTaskStatus(1, &heartbeat, &reader, readerStats), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stmHbAddTaskStatus(1, &heartbeat, &trigger.task, trigger.pStats), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stmHbAddTaskStatus(1, &heartbeat, &nonTopRunner, nonTopRunnerStats), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stmHbAddTaskStatus(1, &heartbeat, &topRunner, topRunnerStats), TSDB_CODE_SUCCESS);

  ASSERT_EQ(taosArrayGetSize(heartbeat.pStreamStatus), 4);
  ASSERT_EQ(taosArrayGetSize(heartbeat.pTaskMetrics), 4);
  EXPECT_EQ(heartbeat.observabilityVersion, STREAM_HB_OBSERVABILITY_VERSION_V1);
  for (int32_t i = 0; i < 4; ++i) {
    const auto *entry = static_cast<const SStreamTaskMetricsEntry *>(taosArrayGet(heartbeat.pTaskMetrics, i));
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->taskStatusIndex, i);
  }
  const int64_t expectedTaskIds[] = {11, 12, 13, 14};
  const int64_t expectedSeriousIds[] = {111, 112, 113, 114};
  for (int32_t i = 0; i < 4; ++i) {
    const auto *entry = static_cast<const SStreamTaskMetricsEntry *>(taosArrayGet(heartbeat.pTaskMetrics, i));
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->streamId, 1);
    EXPECT_EQ(entry->taskId, expectedTaskIds[i]);
    EXPECT_EQ(entry->seriousId, expectedSeriousIds[i]);
  }
  const auto *nonTopEntry = static_cast<const SStreamTaskMetricsEntry *>(taosArrayGet(heartbeat.pTaskMetrics, 2));
  ASSERT_NE(nonTopEntry, nullptr);
  EXPECT_EQ(nonTopEntry->snapshot.applicableMask, 0);

  tCleanupStreamHbMsg(&heartbeat, true);
  stTaskStatsDestroy(&readerStats);
  stTaskStatsDestroy(&trigger.pStats);
  taosArrayDestroy(trigger.pReaderProgressSnapshots);
  stTaskStatsDestroy(&nonTopRunnerStats);
  stTaskStatsDestroy(&topRunnerStats);
}

TEST(StreamTaskStatsTest, TriggerHeartbeatRefreshesRealtimeLagFromReaderProgress) {
  SStreamTriggerTask trigger = {};
  trigger.task.type = STREAM_TRIGGER_TASK;
  trigger.task.streamId = 1;
  trigger.task.taskId = 12;
  trigger.task.seriousId = 112;
  taosInitRWLatch(&trigger.readerProgressLock);
  trigger.pReaderProgressSnapshots = taosArrayInit(1, sizeof(SStreamReaderProgressSnapshot));
  ASSERT_NE(trigger.pReaderProgressSnapshots, nullptr);
  ASSERT_EQ(stTaskStatsCreate(trigger.task.type, STREAM_METRIC_REALTIME_LAG, 0, 1000, &trigger.pStats),
            TSDB_CODE_SUCCESS);
  stTaskStatsSetRealtimeLag(trigger.pStats, true, 1);

  const int64_t                       nowMs = taosGetTimestampMs();
  const SStreamReaderProgressSnapshot progress = {
      .taskId = 21,
      .nodeId = 2,
      .verTime = (nowMs - 4000) * 1000,
  };
  ASSERT_NE(taosArrayPush(trigger.pReaderProgressSnapshots, &progress), nullptr);

  SStreamHbMsg heartbeat = {};
  heartbeat.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(heartbeat.pStreamStatus, nullptr);
  ASSERT_EQ(stmHbAddTaskStatus(1, &heartbeat, &trigger.task, trigger.pStats), TSDB_CODE_SUCCESS);

  ASSERT_EQ(taosArrayGetSize(heartbeat.pTaskMetrics), 1);
  const auto *entry = static_cast<const SStreamTaskMetricsEntry *>(taosArrayGet(heartbeat.pTaskMetrics, 0));
  ASSERT_NE(entry, nullptr);
  EXPECT_GE(entry->snapshot.realtimeLagMs, 4000);
  EXPECT_LT(entry->snapshot.realtimeLagMs, 10000);

  tCleanupStreamHbMsg(&heartbeat, true);
  stTaskStatsDestroy(&trigger.pStats);
  taosArrayDestroy(trigger.pReaderProgressSnapshots);
}

static SArray *failReaderProgressCopy(const SArray *, __array_item_dup_fn_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

TEST(StreamTaskStatsTest, TriggerProgressCopyFailureKeepsCoreStatusAndSkipsMetricEntry) {
  Stub stub;
  stub.set(taosArrayDup, failReaderProgressCopy);

  SStreamTriggerTask trigger = {};
  trigger.task.type = STREAM_TRIGGER_TASK;
  trigger.task.streamId = 7;
  trigger.task.taskId = 71;
  trigger.task.seriousId = 711;
  trigger.task.status = STREAM_STATUS_RUNNING;
  taosInitRWLatch(&trigger.readerProgressLock);
  trigger.pReaderProgressSnapshots = taosArrayInit(0, sizeof(SStreamReaderProgressSnapshot));
  ASSERT_NE(trigger.pReaderProgressSnapshots, nullptr);
  ASSERT_EQ(stTaskStatsCreate(trigger.task.type, STREAM_METRIC_REALTIME_LAG, 0, 1000, &trigger.pStats),
            TSDB_CODE_SUCCESS);

  SStreamHbMsg heartbeat = {};
  heartbeat.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(heartbeat.pStreamStatus, nullptr);

  EXPECT_EQ(stmHbAddTaskStatus(trigger.task.streamId, &heartbeat, &trigger.task, trigger.pStats), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosArrayGetSize(heartbeat.pStreamStatus), 1);
  EXPECT_EQ(taosArrayGetSize(heartbeat.pTaskMetrics), 0);
  const auto *status = static_cast<const SStmTaskStatusMsg *>(taosArrayGet(heartbeat.pStreamStatus, 0));
  ASSERT_NE(status, nullptr);
  EXPECT_EQ(status->streamId, 7);
  EXPECT_EQ(status->taskId, 71);
  EXPECT_EQ(status->seriousId, 711);
  EXPECT_EQ(status->status, STREAM_STATUS_RUNNING);
  EXPECT_EQ(trigger.task.status, STREAM_STATUS_RUNNING);

  tCleanupStreamHbMsg(&heartbeat, true);
  taosArrayDestroy(trigger.pReaderProgressSnapshots);
  stTaskStatsDestroy(&trigger.pStats);
}

TEST(StreamTaskStatsTest, SnapshotFailureKeepsCoreStatusAndSkipsMetricEntry) {
  SStreamTask reader = {};
  reader.type = STREAM_READER_TASK;
  reader.streamId = 7;
  reader.taskId = 71;
  reader.seriousId = 711;
  reader.status = STREAM_STATUS_RUNNING;
  SStreamTaskStats *readerStats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(reader.type, STREAM_METRIC_PHYSICAL_INPUT, INT64_MAX, 1000, &readerStats),
            TSDB_CODE_SUCCESS);

  SStreamHbMsg heartbeat = {};
  heartbeat.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(heartbeat.pStreamStatus, nullptr);

  EXPECT_EQ(stmHbAddTaskStatus(reader.streamId, &heartbeat, &reader, readerStats), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosArrayGetSize(heartbeat.pStreamStatus), 1);
  EXPECT_EQ(taosArrayGetSize(heartbeat.pTaskMetrics), 0);
  const auto *status = static_cast<const SStmTaskStatusMsg *>(taosArrayGet(heartbeat.pStreamStatus, 0));
  ASSERT_NE(status, nullptr);
  EXPECT_EQ(status->streamId, 7);
  EXPECT_EQ(status->taskId, 71);
  EXPECT_EQ(status->seriousId, 711);
  EXPECT_EQ(status->status, STREAM_STATUS_RUNNING);
  EXPECT_EQ(reader.streamId, 7);
  EXPECT_EQ(reader.taskId, 71);
  EXPECT_EQ(reader.seriousId, 711);
  EXPECT_EQ(reader.status, STREAM_STATUS_RUNNING);

  tCleanupStreamHbMsg(&heartbeat, true);
  tCleanupStreamHbMsg(&heartbeat, true);
  stTaskStatsDestroy(&readerStats);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
