#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cmdnodes.h"
#include "dataSinkMgt.h"
#include "stream.h"
#include "streamInt.h"
#include "streamRunner.h"
#include "streamTaskStats.h"
#include "stub.h"
#include "tdatablock.h"

extern "C" {
#include "tcurl.h"

int32_t stmHbAddTaskStatus(int64_t streamId, SStreamHbMsg *pMsg, SStreamTask *pTask, SStreamTaskStats *pStats);
}

namespace {

struct RunnerCallState {
  int32_t                                  resetCalls = 0;
  int32_t                                  executeCalls = 0;
  int32_t                                  forceOutputCalls = 0;
  int32_t                                  sinkCalls = 0;
  int32_t                                  notifyCalls = 0;
  int32_t                                  notifyTargets = 0;
  int32_t                                  sinkCode = TSDB_CODE_SUCCESS;
  int32_t                                  notifyCode = TSDB_CODE_SUCCESS;
  int32_t                                  notifyBuildCode = TSDB_CODE_SUCCESS;
  int32_t                                  forceOutputCode = TSDB_CODE_SUCCESS;
  bool                                     sinkBlockIsNull = false;
  bool                                     autoCreateTable = false;
  int64_t                                  groupId = 0;
  int64_t                                  monotonicUs = 1000;
  char                                     tableName[TSDB_TABLE_NAME_LEN] = {0};
  std::vector<int32_t>                     executeCodes;
  std::vector<int64_t>                     executeReadyTimes;
  std::vector<bool>                        executeFinished;
  std::vector<SSDataBlock *>               executeBlocks;
  std::vector<int32_t>                     executeOutIndexes;
  SStreamRuntimeInfo                      *pRuntimeInfo = nullptr;
  std::vector<std::pair<int64_t, int64_t>> executeBlockingIntervals;
};

RunnerCallState          gCalls;
std::vector<std::string> gRunnerDebugLogs;
std::atomic<bool>        gBlockRunnerPeriodLog{false};
std::atomic<bool>        gRunnerPeriodLogEntered{false};
std::atomic<int32_t>     gRunnerUndeployCalls{0};

struct RunnerRotationInterleave {
  int32_t                   calls = 0;
  int32_t                   code = TSDB_CODE_SUCCESS;
  bool                      rotated = false;
  SStreamTaskPeriodSnapshot snapshot = {};
};

RunnerRotationInterleave gRunnerRotationInterleave;

void captureRunnerDebugLog(const char *, int32_t, int32_t, const char *format, ...) {
  char    buffer[4096] = {0};
  va_list args;
  va_start(args, format);
  int32_t len = vsnprintf(buffer, sizeof(buffer), format, args);
  va_end(args);
  if (len >= 0 && len < sizeof(buffer)) gRunnerDebugLogs.emplace_back(buffer);
  if (strstr(buffer, "record=task_period task_type=runner") != nullptr) {
    gRunnerPeriodLogEntered.store(true);
    while (gBlockRunnerPeriodLog.load()) std::this_thread::yield();
  }
}

void captureRunnerUndeploy(void *) { ++gRunnerUndeployCalls; }

void rotateOnRunnerGaugeUpdate(SStreamTaskStats *pStats, int64_t, int64_t, int64_t) {
  ++gRunnerRotationInterleave.calls;
  gRunnerRotationInterleave.code = stTaskStatsRotatePeriod(
      pStats, STREAM_STATS_PERIOD_US + 1, &gRunnerRotationInterleave.snapshot, &gRunnerRotationInterleave.rotated);
}

int32_t failRunnerStatsLog(SStreamRunnerTask *, const SStreamTaskPeriodSnapshot *) { return TSDB_CODE_FAILED; }

class ScopedRunnerDebugLogCapture {
 public:
  ScopedRunnerDebugLogCapture() : previousDebugFlag_(stDebugFlag) {
    gRunnerDebugLogs.clear();
    stub_.set(taosPrintLog, captureRunnerDebugLog);
    stDebugFlag = previousDebugFlag_ | DEBUG_DEBUG | DEBUG_FILE;
  }

  ~ScopedRunnerDebugLogCapture() {
    stDebugFlag = previousDebugFlag_;
    gRunnerDebugLogs.clear();
  }

 private:
  Stub    stub_;
  int32_t previousDebugFlag_;
};

int32_t mockStreamClearStatesForOperators(qTaskInfo_t) {
  ++gCalls.resetCalls;
  return TSDB_CODE_SUCCESS;
}

int32_t mockStreamExecuteTask(qTaskInfo_t, SSDataBlock **ppBlock, bool *pFinished) {
  const size_t index = static_cast<size_t>(gCalls.executeCalls++);
  if (gCalls.pRuntimeInfo != nullptr && gCalls.pRuntimeInfo->blockingStatsFp != nullptr) {
    for (const auto &interval : gCalls.executeBlockingIntervals) {
      gCalls.monotonicUs = interval.first;
      gCalls.pRuntimeInfo->blockingStatsFp(gCalls.pRuntimeInfo->pBlockingStatsParam, true);
      gCalls.monotonicUs = interval.second;
      gCalls.pRuntimeInfo->blockingStatsFp(gCalls.pRuntimeInfo->pBlockingStatsParam, false);
    }
  }
  if (index < gCalls.executeReadyTimes.size()) {
    gCalls.monotonicUs = gCalls.executeReadyTimes[index];
  }
  if (gCalls.pRuntimeInfo != nullptr && index < gCalls.executeOutIndexes.size()) {
    gCalls.pRuntimeInfo->funcInfo.curOutIdx = gCalls.executeOutIndexes[index];
  }
  *ppBlock = index < gCalls.executeBlocks.size() ? gCalls.executeBlocks[index] : nullptr;
  *pFinished = index < gCalls.executeFinished.size() ? gCalls.executeFinished[index] : true;
  return index < gCalls.executeCodes.size() ? gCalls.executeCodes[index] : TSDB_CODE_SUCCESS;
}

int32_t mockStreamForceOutput(qTaskInfo_t, SSDataBlock **ppBlock, int32_t) {
  ++gCalls.forceOutputCalls;
  *ppBlock = nullptr;
  return gCalls.forceOutputCode;
}

int32_t mockDsPutDataBlock(DataSinkHandle, const SInputData *pInput, bool *pContinue) {
  ++gCalls.sinkCalls;
  gCalls.sinkBlockIsNull = pInput->pData == nullptr;
  gCalls.autoCreateTable = pInput->pStreamDataInserterInfo->isAutoCreateTable;
  gCalls.groupId = pInput->pStreamDataInserterInfo->groupId;
  tstrncpy(gCalls.tableName, pInput->pStreamDataInserterInfo->tbName, sizeof(gCalls.tableName));
  *pContinue = false;
  return gCalls.sinkCode;
}

int32_t mockStreamSendNotifyContent(SStreamTask *, const char *, const char *, int32_t, int64_t,
                                    const SArray *pNotifyAddrUrls, int32_t, const SSTriggerCalcParam *, int32_t) {
  ++gCalls.notifyCalls;
  gCalls.notifyTargets = static_cast<int32_t>(taosArrayGetSize(pNotifyAddrUrls));
  return gCalls.notifyCode;
}

int32_t mockStreamSendNotifyContentWithResult(SStreamTask *, const char *, const char *, int32_t, int64_t,
                                              const SArray *pNotifyAddrUrls, int32_t, const SSTriggerCalcParam *,
                                              int32_t, bool *pAttempted, bool *pDelivered) {
  ++gCalls.notifyCalls;
  gCalls.notifyTargets = static_cast<int32_t>(taosArrayGetSize(pNotifyAddrUrls));
  *pAttempted = true;
  *pDelivered = gCalls.notifyCode == TSDB_CODE_SUCCESS;
  return TSDB_CODE_SUCCESS;
}

int32_t mockTcurlConnectFailure(CURL **, const char *) { return TSDB_CODE_FAILED; }

int32_t mockTcurlConnectSuccess(CURL **ppConn, const char *) {
  *ppConn = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t mockTcurlSendFailure(SCURL *, const void *, size_t, size_t *pSent, curl_off_t, unsigned int) {
  *pSent = 0;
  return TSDB_CODE_FAILED;
}

int32_t mockStreamBuildBlockResultNotifyContent(const SStreamRunnerTask *, const SSDataBlock *, char **ppContent,
                                                const SArray *, int32_t, int32_t, bool *pHasNotifyRows) {
  if (gCalls.notifyBuildCode != TSDB_CODE_SUCCESS) return gCalls.notifyBuildCode;
  *ppContent = taosStrdup("{}");
  *pHasNotifyRows = true;
  return *ppContent == nullptr ? terrno : TSDB_CODE_SUCCESS;
}

int64_t mockStreamTaskGetMonotonicUs() { return gCalls.monotonicUs; }

void destroyRuntimeInfoInList(SList *pList) {
  SListNode *pNode = tdListGetHead(pList);
  while (pNode != nullptr) {
    auto *pExec = reinterpret_cast<SStreamRunnerTaskExecution *>(pNode->data);
    tDestroyStRtFuncInfo(&pExec->runtimeInfo.funcInfo);
    pNode = pNode->dl_next_;
  }
}

class StreamRunnerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gCalls = {};
    gBlockRunnerPeriodLog.store(false);
    gRunnerPeriodLogEntered.store(false);
    gRunnerUndeployCalls.store(0);
    stub_.set(streamClearStatesForOperators, mockStreamClearStatesForOperators);
    stub_.set(streamExecuteTask, mockStreamExecuteTask);
    stub_.set(streamForceOutput, mockStreamForceOutput);
    stub_.set(dsPutDataBlock, mockDsPutDataBlock);
    stub_.set(streamSendNotifyContent, mockStreamSendNotifyContent);
    stub_.set(streamSendNotifyContentWithResult, mockStreamSendNotifyContentWithResult);
    stub_.set(streamBuildBlockResultNotifyContent, mockStreamBuildBlockResultNotifyContent);
    stub_.set(streamTaskGetMonotonicUs, mockStreamTaskGetMonotonicUs);

    ASSERT_EQ(taosThreadMutexInit(&task_.execMgr.lock, nullptr), TSDB_CODE_SUCCESS);
    task_.execMgr.lockInited = true;
    task_.execMgr.pFreeExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
    task_.execMgr.pRunningExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
    ASSERT_NE(task_.execMgr.pFreeExecs, nullptr);
    ASSERT_NE(task_.execMgr.pRunningExecs, nullptr);

    auto *pExec = static_cast<SStreamRunnerTaskExecution *>(tdListReserve(task_.execMgr.pFreeExecs));
    ASSERT_NE(pExec, nullptr);
    pExec->pExecutor = &executorSentinel_;
    pExec->pSinkHandle = &sinkSentinel_;
    pExec->runtimeInfo.execId = 7;
    tstrncpy(pExec->tbname, "out_zero_window", sizeof(pExec->tbname));
    pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
    ASSERT_NE(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, nullptr);
    SSTriggerCalcParam staleWindow = {.wstart = 1000, .wend = 2000};
    ASSERT_NE(taosArrayPush(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, &staleWindow), nullptr);

    task_.topTask = true;
    task_.task.type = STREAM_RUNNER_TASK;
    task_.output.outTblType = TSDB_NORMAL_TABLE;
    task_.task.streamId = 0x1234;
    task_.task.taskId = 23;
    task_.task.seriousId = 45;
    task_.task.nodeId = 67;
    task_.task.status = STREAM_STATUS_RUNNING;
    task_.parallelExecutionNun = 3;
    task_.notification.pNotifyAddrUrls = taosArrayInit(1, sizeof(char *));
    ASSERT_NE(task_.notification.pNotifyAddrUrls, nullptr);
    char *notifyUrl = notifyUrl_;
    ASSERT_NE(taosArrayPush(task_.notification.pNotifyAddrUrls, &notifyUrl), nullptr);

    request_.brandNew = true;
    request_.execId = -1;
    request_.createTable = 1;
    request_.gid = 42;
    request_.sessionId = 1;

    ASSERT_EQ(stTaskStatsCreate(STREAM_RUNNER_TASK, STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY, 1,
                                1000, &task_.pStats),
              TSDB_CODE_SUCCESS);

    ASSERT_EQ(gStreamMgmt.taskMap, nullptr);
    gStreamMgmt.taskMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    ASSERT_NE(gStreamMgmt.taskMap, nullptr);
    SStreamTask *pTask = &task_.task;
    ASSERT_EQ(taosHashPut(gStreamMgmt.taskMap, &task_.task.streamId,
                          sizeof(task_.task.streamId) + sizeof(task_.task.taskId), &pTask, POINTER_BYTES),
              TSDB_CODE_SUCCESS);
  }

  void TearDown() override {
    taosHashCleanup(gStreamMgmt.taskMap);
    gStreamMgmt.taskMap = nullptr;
    tDestroySTriggerCalcRequest(&request_);
    if (task_.execMgr.pRunningExecs != nullptr) {
      destroyRuntimeInfoInList(task_.execMgr.pRunningExecs);
      task_.execMgr.pRunningExecs = static_cast<SList *>(tdListFree(task_.execMgr.pRunningExecs));
    }
    if (task_.execMgr.pFreeExecs != nullptr) {
      destroyRuntimeInfoInList(task_.execMgr.pFreeExecs);
      task_.execMgr.pFreeExecs = static_cast<SList *>(tdListFree(task_.execMgr.pFreeExecs));
    }
    if (task_.execMgr.lockInited) {
      EXPECT_EQ(taosThreadMutexDestroy(&task_.execMgr.lock), TSDB_CODE_SUCCESS);
    }
    taosArrayDestroy(task_.notification.pNotifyAddrUrls);
    stTaskStatsDestroy(&task_.pStats);
    for (SSDataBlock *pBlock : ownedBlocks_) {
      blockDataDestroy(pBlock);
    }
  }

  void SetWindows(int32_t count) {
    request_.params = taosArrayInit(count, sizeof(SSTriggerCalcParam));
    ASSERT_NE(request_.params, nullptr);
    for (int32_t i = 0; i < count; ++i) {
      SSTriggerCalcParam param = {.wstart = i * 1000, .wend = (i + 1) * 1000};
      param.notifyType = STRIGGER_EVENT_WINDOW_OPEN;
      ASSERT_NE(taosArrayPush(request_.params, &param), nullptr);
    }
    request_.createTable = 0;
  }

  SSDataBlock *NewBlock(int64_t rows) {
    SSDataBlock *pBlock = nullptr;
    EXPECT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
    if (pBlock != nullptr) {
      pBlock->info.rows = rows;
      ownedBlocks_.push_back(pBlock);
    }
    return pBlock;
  }

  SStreamRunnerPeriodStats RotateRunnerStats() { return RotateRunnerSnapshot().period.runner; }

  SStreamTaskPeriodSnapshot RotateRunnerSnapshot() {
    SStreamTaskPeriodSnapshot snapshot = {};
    bool                      rotated = false;
    EXPECT_EQ(stTaskStatsRotatePeriod(task_.pStats, STREAM_STATS_PERIOD_US + 1, &snapshot, &rotated),
              TSDB_CODE_SUCCESS);
    EXPECT_TRUE(rotated);
    return snapshot;
  }

  SStreamTaskPeriodSnapshot EmptyRunnerSnapshot() const {
    SStreamTaskPeriodSnapshot snapshot = {};
    snapshot.taskType = STREAM_RUNNER_TASK;
    snapshot.statsStartAtMs = 1000;
    snapshot.uptimeMs = 180000;
    snapshot.statsWindowMs = 180000;
    return snapshot;
  }

  std::string EmitRunnerDebugPeriod(const SStreamTaskPeriodSnapshot &snapshot) {
    ScopedRunnerDebugLogCapture capture;
    EXPECT_EQ(stRunnerTaskLogStats(&task_, &snapshot), TSDB_CODE_SUCCESS);
    EXPECT_EQ(gRunnerDebugLogs.size(), 1);
    return gRunnerDebugLogs.empty() ? std::string{} : gRunnerDebugLogs.front();
  }

  SStreamRunnerTaskExecution *Exec() {
    EXPECT_EQ(listNEles(task_.execMgr.pFreeExecs), 1);
    SListNode *pNode = tdListGetHead(task_.execMgr.pFreeExecs);
    return pNode == nullptr ? nullptr : reinterpret_cast<SStreamRunnerTaskExecution *>(pNode->data);
  }

  Stub                       stub_;
  char                       executorSentinel_ = 0;
  char                       sinkSentinel_ = 0;
  char                       notifyUrl_[32] = "ws://unused/zero-window";
  SStreamRunnerTask          task_ = {};
  SSTriggerCalcRequest       request_ = {};
  std::vector<SSDataBlock *> ownedBlocks_;
};

TEST_F(StreamRunnerTest, RunnerPeriodUsesSameCountsAsMinuteSnapshot) {
  stTaskStatsRecordRunnerRequest(task_.pStats, 2, 1000001, 2000);
  stTaskStatsRecordRunnerInput(task_.pStats, 400, 20, 1000001);
  stTaskStatsRecordRunnerCalcDuration(task_.pStats, 6003, 1000001, 2000);
  stTaskStatsRecordRunnerWindow(task_.pStats, true, 5000, 1000001, 3000);
  stTaskStatsRecordRunnerOutput(task_.pStats, 100, 10, 1000001, 4000);

  SStreamTaskMetricsSnapshot minute = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(task_.pStats, STREAM_STATS_BUCKET_COUNT * STREAM_STATS_BUCKET_US + 1, &minute),
            TSDB_CODE_SUCCESS);
  ASSERT_TRUE(minute.windowReady);

  const SStreamTaskPeriodSnapshot period = RotateRunnerSnapshot();
  EXPECT_EQ(period.period.runner.outputRows, minute.deliveredOutputRows1m);
  EXPECT_EQ(period.period.runner.resultLatency.totalUs, minute.resultLatencyUs1m);
  EXPECT_EQ(period.period.runner.resultLatency.samples, minute.resultLatencySamples1m);

  const std::string line = EmitRunnerDebugPeriod(period);
  EXPECT_NE(line.find("calc_request_count=1"), std::string::npos);
  EXPECT_NE(line.find("logical_window_count=2"), std::string::npos);
  EXPECT_NE(line.find("input_rows=400"), std::string::npos);
  EXPECT_NE(line.find("input_blocks=20"), std::string::npos);
  EXPECT_NE(line.find("output_rows=100"), std::string::npos);
  EXPECT_NE(line.find("output_blocks=10"), std::string::npos);
  EXPECT_NE(line.find("input_rows_per_sec=2.222"), std::string::npos);
  EXPECT_NE(line.find("input_blocks_per_sec=0.111"), std::string::npos);
  EXPECT_NE(line.find("output_rows_per_sec=0.556"), std::string::npos);
  EXPECT_NE(line.find("output_blocks_per_sec=0.056"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_avg_ms=6.003"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_max_ms=6.003"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_lifetime_max_ms=6.003"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_lifetime_max_at=2000"), std::string::npos);
  EXPECT_NE(line.find("result_latency_avg_ms=5.000"), std::string::npos);
  EXPECT_NE(line.find("result_latency_max_ms=5.000"), std::string::npos);
  EXPECT_NE(line.find("result_latency_lifetime_max_ms=5.000"), std::string::npos);
  EXPECT_NE(line.find("result_latency_lifetime_max_at=3000"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerPeriodPrintsNoResultAndThreeFailureClasses) {
  SStreamTaskPeriodSnapshot snapshot = EmptyRunnerSnapshot();
  snapshot.period.runner.noResultWindowCount = 7;
  snapshot.period.runner.calcFailureCount = 2;
  snapshot.period.runner.sinkFailureCount = 3;
  snapshot.period.runner.notifyFailureCount = 5;

  const std::string line = EmitRunnerDebugPeriod(snapshot);
  EXPECT_NE(line.find("no_result_window_count=7"), std::string::npos);
  EXPECT_NE(line.find("calc_failure_count=2"), std::string::npos);
  EXPECT_NE(line.find("sink_failure_count=3"), std::string::npos);
  EXPECT_NE(line.find("notify_failure_count=5"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerPeriodUsesActualWindowForRates) {
  SStreamTaskPeriodSnapshot snapshot = EmptyRunnerSnapshot();
  snapshot.statsWindowMs = 200000;
  snapshot.period.runner.inputRows = 400;
  snapshot.period.runner.inputBlocks = 20;
  snapshot.period.runner.outputRows = 100;
  snapshot.period.runner.outputBlocks = 10;

  const std::string line = EmitRunnerDebugPeriod(snapshot);
  EXPECT_NE(line.find("input_rows_per_sec=2.000"), std::string::npos);
  EXPECT_NE(line.find("input_blocks_per_sec=0.100"), std::string::npos);
  EXPECT_NE(line.find("output_rows_per_sec=0.500"), std::string::npos);
  EXPECT_NE(line.find("output_blocks_per_sec=0.050"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerPeriodNoSamplesPrintsNa) {
  const std::string line = EmitRunnerDebugPeriod(EmptyRunnerSnapshot());
  EXPECT_NE(line.find("calc_duration_avg_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_max_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_lifetime_max_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_lifetime_max_at=NA"), std::string::npos);
  EXPECT_NE(line.find("result_latency_avg_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("result_latency_max_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("result_latency_lifetime_max_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("result_latency_lifetime_max_at=NA"), std::string::npos);
  EXPECT_NE(line.find("last_calc_at=NA"), std::string::npos);
  EXPECT_NE(line.find("last_result_at=NA"), std::string::npos);
  EXPECT_NE(line.find("last_output_at=NA"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerExecCountsAreReadUnderLock) {
  ScopedRunnerDebugLogCapture capture;
  SStreamTaskPeriodSnapshot   snapshot = EmptyRunnerSnapshot();
  std::atomic<bool>           entered{false};
  std::atomic<bool>           finished{false};
  int32_t                     logCode = TSDB_CODE_FAILED;

  ASSERT_EQ(taosThreadMutexLock(&task_.execMgr.lock), TSDB_CODE_SUCCESS);
  std::thread logger([&] {
    entered.store(true);
    logCode = stRunnerTaskLogStats(&task_, &snapshot);
    finished.store(true);
  });
  while (!entered.load()) std::this_thread::yield();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(finished.load());

  SStreamRunnerTaskExecution secondExec = {};
  EXPECT_EQ(tdListAppend(task_.execMgr.pFreeExecs, &secondExec), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosThreadMutexUnlock(&task_.execMgr.lock), TSDB_CODE_SUCCESS);
  logger.join();

  ASSERT_EQ(logCode, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(finished.load());
  ASSERT_EQ(gRunnerDebugLogs.size(), 1);
  EXPECT_NE(gRunnerDebugLogs.front().find("free_exec_count=2"), std::string::npos);
  EXPECT_NE(gRunnerDebugLogs.front().find("running_exec_count=0"), std::string::npos);
  EXPECT_NE(gRunnerDebugLogs.front().find("parallel_execution_limit=3"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerPeriodUninitializedExecManagerDoesNotFabricateCounts) {
  ScopedRunnerDebugLogCapture capture;
  SStreamTaskPeriodSnapshot   snapshot = EmptyRunnerSnapshot();
  snapshot.period.runner.calcRequestCount = 7;
  task_.execMgr.lockInited = false;
  const int32_t logCode = stRunnerTaskLogStats(&task_, &snapshot);
  task_.execMgr.lockInited = true;

  EXPECT_EQ(logCode, TSDB_CODE_SUCCESS);
  ASSERT_EQ(gRunnerDebugLogs.size(), 1);
  EXPECT_NE(gRunnerDebugLogs.front().find("calc_request_count=7"), std::string::npos);
  EXPECT_NE(gRunnerDebugLogs.front().find("free_exec_count=NA"), std::string::npos);
  EXPECT_NE(gRunnerDebugLogs.front().find("running_exec_count=NA"), std::string::npos);
  EXPECT_NE(gRunnerDebugLogs.front().find("parallel_execution_limit=NA"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerPeriodEntryWriterSkipsPoisonedExecManagerAndPrintsNa) {
  ScopedRunnerDebugLogCapture capture;
  SStreamTaskPeriodSnapshot   snapshot = EmptyRunnerSnapshot();
  SList                      *pFreeExecs = task_.execMgr.pFreeExecs;
  SList                      *pRunningExecs = task_.execMgr.pRunningExecs;

  ASSERT_EQ(taosWTryForceLockLatch(&task_.task.entryLock), TSDB_CODE_SUCCESS);
  task_.execMgr.pFreeExecs = reinterpret_cast<SList *>(static_cast<uintptr_t>(1));
  task_.execMgr.pRunningExecs = reinterpret_cast<SList *>(static_cast<uintptr_t>(1));

  const int32_t logCode = stRunnerTaskLogStats(&task_, &snapshot);
  std::string   line;
  int32_t       periodLogs = 0;
  for (const std::string &log : gRunnerDebugLogs) {
    if (log.find("record=task_period task_type=runner") != std::string::npos) {
      ++periodLogs;
      line = log;
    }
  }

  task_.execMgr.pFreeExecs = pFreeExecs;
  task_.execMgr.pRunningExecs = pRunningExecs;
  taosWUnLockLatch(&task_.task.entryLock);

  EXPECT_EQ(logCode, TSDB_CODE_SUCCESS);
  ASSERT_EQ(periodLogs, 1);
  EXPECT_NE(line.find("free_exec_count=NA"), std::string::npos);
  EXPECT_NE(line.find("running_exec_count=NA"), std::string::npos);
  EXPECT_NE(line.find("parallel_execution_limit=NA"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerLoggerEntryReadPreventsTeardownUntilExecSnapshotCompletes) {
  ScopedRunnerDebugLogCapture capture;
  SStreamRunnerTask           runner = {};
  runner.task.type = STREAM_RUNNER_TASK;
  runner.task.streamId = 0x5678;
  runner.task.taskId = 89;
  runner.task.status = STREAM_STATUS_RUNNING;
  runner.task.undeployCb = captureRunnerUndeploy;
  runner.parallelExecutionNun = 2;
  ASSERT_EQ(taosThreadMutexInit(&runner.execMgr.lock, nullptr), TSDB_CODE_SUCCESS);
  runner.execMgr.lockInited = true;
  runner.execMgr.pFreeExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  runner.execMgr.pRunningExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  ASSERT_NE(runner.execMgr.pFreeExecs, nullptr);
  ASSERT_NE(runner.execMgr.pRunningExecs, nullptr);
  SStreamTask *pRegisteredTask = &runner.task;
  ASSERT_EQ(taosHashPut(gStreamMgmt.taskMap, &runner.task.streamId,
                        sizeof(runner.task.streamId) + sizeof(runner.task.taskId), &pRegisteredTask, POINTER_BYTES),
            TSDB_CODE_SUCCESS);

  SStreamTaskPeriodSnapshot snapshot = EmptyRunnerSnapshot();
  int32_t                   logCode = TSDB_CODE_FAILED;
  gBlockRunnerPeriodLog.store(true);
  std::thread logger([&] { logCode = stRunnerTaskLogStats(&runner, &snapshot); });
  const auto  deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!gRunnerPeriodLogEntered.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  bool    entered = gRunnerPeriodLogEntered.load();
  int32_t undeployCode = TSDB_CODE_FAILED;
  if (entered) {
    EXPECT_EQ(taosHashRemove(gStreamMgmt.taskMap, &runner.task.streamId,
                             sizeof(runner.task.streamId) + sizeof(runner.task.taskId)),
              TSDB_CODE_SUCCESS);
    SStreamRunnerTask *pRunner = &runner;
    undeployCode = stRunnerTaskUndeploy(&pRunner, false);
    EXPECT_EQ(gRunnerUndeployCalls.load(), 0);
    EXPECT_TRUE(taosHasRWWFlag(&runner.task.entryLock));
  }

  gBlockRunnerPeriodLog.store(false);
  logger.join();

  EXPECT_TRUE(entered);
  EXPECT_EQ(undeployCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(logCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(gRunnerUndeployCalls.load(), 1);
  if (!entered) {
    TAOS_UNUSED(taosHashRemove(gStreamMgmt.taskMap, &runner.task.streamId,
                               sizeof(runner.task.streamId) + sizeof(runner.task.taskId)));
    tdListFreeP(runner.execMgr.pRunningExecs, nullptr);
    tdListFreeP(runner.execMgr.pFreeExecs, nullptr);
    TAOS_UNUSED(taosThreadMutexDestroy(&runner.execMgr.lock));
  }
}

TEST_F(StreamRunnerTest, RunnerPeriodDoesNotContainWriteRetryOrQueueDepth) {
  const std::string line = EmitRunnerDebugPeriod(EmptyRunnerSnapshot());
  EXPECT_EQ(line.find("write_retry_count="), std::string::npos);
  EXPECT_EQ(line.find("queue_depth="), std::string::npos);
  EXPECT_EQ(line.find("invalid_result_latency_count="), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerPeriodContainsCommonIdentity) {
  SStreamTaskPeriodSnapshot snapshot = EmptyRunnerSnapshot();
  snapshot.statsOverflow = true;
  const std::string line = EmitRunnerDebugPeriod(snapshot);

  EXPECT_NE(line.find("record=task_period task_type=runner"), std::string::npos);
  EXPECT_NE(line.find("stream_id=4660"), std::string::npos);
  EXPECT_NE(line.find("task_id=23"), std::string::npos);
  EXPECT_NE(line.find("serious_id=45"), std::string::npos);
  EXPECT_NE(line.find("node_id=67"), std::string::npos);
  EXPECT_NE(line.find("task_type=runner"), std::string::npos);
  EXPECT_NE(line.find("status=Running"), std::string::npos);
  EXPECT_NE(line.find("stats_start_at=1000"), std::string::npos);
  EXPECT_NE(line.find("uptime_ms=180000"), std::string::npos);
  EXPECT_NE(line.find("stats_window_ms=180000"), std::string::npos);
  EXPECT_NE(line.find("stats_overflow=true"), std::string::npos);

  const char *requiredFields[] = {
      "calc_request_count=",
      "logical_window_count=",
      "input_rows=",
      "input_blocks=",
      "output_rows=",
      "output_blocks=",
      "no_result_window_count=",
      "calc_failure_count=",
      "sink_failure_count=",
      "notify_failure_count=",
      "input_rows_per_sec=",
      "input_blocks_per_sec=",
      "output_rows_per_sec=",
      "output_blocks_per_sec=",
      "calc_duration_samples=",
      "calc_duration_avg_ms=",
      "calc_duration_max_ms=",
      "calc_duration_lifetime_max_ms=",
      "calc_duration_lifetime_max_at=",
      "result_latency_samples=",
      "result_latency_avg_ms=",
      "result_latency_max_ms=",
      "result_latency_lifetime_max_ms=",
      "result_latency_lifetime_max_at=",
      "free_exec_count=",
      "running_exec_count=",
      "parallel_execution_limit=",
      "last_calc_at=",
      "last_result_at=",
      "last_output_at=",
  };
  for (const char *field : requiredFields) EXPECT_NE(line.find(field), std::string::npos) << field;
}

TEST_F(StreamRunnerTest, RunnerLastEventGaugesUseWallClockAtNaturalBoundaries) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  task_.lowLatencyCalc = true;

  const int64_t beforeWallMs = taosGetTimestampMs();
  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);
  const int64_t afterWallMs = taosGetTimestampMs();

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_GE(snapshot.runnerGauges.lastCalcAtMs, beforeWallMs);
  EXPECT_LE(snapshot.runnerGauges.lastCalcAtMs, afterWallMs);
  EXPECT_GE(snapshot.runnerGauges.lastResultAtMs, beforeWallMs);
  EXPECT_LE(snapshot.runnerGauges.lastResultAtMs, afterWallMs);
  EXPECT_GE(snapshot.runnerGauges.lastOutputAtMs, beforeWallMs);
  EXPECT_LE(snapshot.runnerGauges.lastOutputAtMs, afterWallMs);
  EXPECT_NE(snapshot.runnerGauges.lastCalcAtMs, gCalls.monotonicUs);

  const std::string line = EmitRunnerDebugPeriod(snapshot);
  EXPECT_NE(line.find("last_calc_at=" + std::to_string(snapshot.runnerGauges.lastCalcAtMs)), std::string::npos);
  EXPECT_NE(line.find("last_result_at=" + std::to_string(snapshot.runnerGauges.lastResultAtMs)), std::string::npos);
  EXPECT_NE(line.find("last_output_at=" + std::to_string(snapshot.runnerGauges.lastOutputAtMs)), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerGaugeSetterDoesNotRegressOnOutOfOrderCallbacks) {
  stTaskStatsSetRunnerGauges(task_.pStats, 100, 200, 300);
  stTaskStatsSetRunnerGauges(task_.pStats, 90, 0, 250);
  stTaskStatsSetRunnerGauges(task_.pStats, 110, 0, 0);

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_EQ(snapshot.runnerGauges.lastCalcAtMs, 110);
  EXPECT_EQ(snapshot.runnerGauges.lastResultAtMs, 200);
  EXPECT_EQ(snapshot.runnerGauges.lastOutputAtMs, 300);
}

TEST_F(StreamRunnerTest, RunnerNoResultAdvancesResultGaugeButNotOutputGauge) {
  SetWindows(1);
  gCalls.executeReadyTimes = {6000};

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_GT(snapshot.runnerGauges.lastCalcAtMs, 0);
  EXPECT_GT(snapshot.runnerGauges.lastResultAtMs, 0);
  EXPECT_EQ(snapshot.runnerGauges.lastOutputAtMs, 0);
}

TEST_F(StreamRunnerTest, RunnerSinkFailureDoesNotAdvanceOutputGauge) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  gCalls.sinkCode = TSDB_CODE_INVALID_PARA;
  task_.lowLatencyCalc = true;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_EQ(snapshot.runnerGauges.lastOutputAtMs, 0);
}

TEST_F(StreamRunnerTest, RunnerCalcFailureBeforeReadyDoesNotAdvanceResultGauge) {
  SetWindows(1);
  gCalls.executeCodes = {TSDB_CODE_INVALID_PARA};
  gCalls.executeReadyTimes = {6000};

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_GT(snapshot.runnerGauges.lastCalcAtMs, 0);
  EXPECT_EQ(snapshot.runnerGauges.lastResultAtMs, 0);
  EXPECT_EQ(snapshot.runnerGauges.lastOutputAtMs, 0);
}

TEST_F(StreamRunnerTest, RunnerNotifyFailureDoesNotAdvanceOutputGauge) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  gCalls.notifyCode = TSDB_CODE_INVALID_PARA;
  task_.notification.calcNotifyOnly = true;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_EQ(snapshot.runnerGauges.lastOutputAtMs, 0);
}

TEST_F(StreamRunnerTest, RunnerSinkAndNotifyRecordOutputOnce) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  task_.lowLatencyCalc = true;

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);
  ASSERT_EQ(gCalls.sinkCalls, 1);
  ASSERT_EQ(gCalls.notifyCalls, 1);

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_EQ(snapshot.period.runner.outputRows, 7);
  EXPECT_EQ(snapshot.period.runner.outputBlocks, 1);
  EXPECT_GT(snapshot.runnerGauges.lastOutputAtMs, 0);
}

TEST_F(StreamRunnerTest, RunnerRequestAndCalcGaugeCannotSplitAcrossRotation) {
  SetWindows(1);
  gCalls.executeCodes = {TSDB_CODE_INVALID_PARA};
  gRunnerRotationInterleave = {};
  {
    Stub gaugeStub;
    gaugeStub.set(stTaskStatsSetRunnerGauges, rotateOnRunnerGaugeUpdate);
    EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);
  }

  if (gRunnerRotationInterleave.calls == 0) {
    gRunnerRotationInterleave.code =
        stTaskStatsRotatePeriod(task_.pStats, STREAM_STATS_PERIOD_US + 1, &gRunnerRotationInterleave.snapshot,
                                &gRunnerRotationInterleave.rotated);
  }
  ASSERT_EQ(gRunnerRotationInterleave.code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(gRunnerRotationInterleave.rotated);
  EXPECT_EQ(gRunnerRotationInterleave.snapshot.period.runner.calcRequestCount, 1);
  EXPECT_GT(gRunnerRotationInterleave.snapshot.runnerGauges.lastCalcAtMs, 0);
}

TEST_F(StreamRunnerTest, RunnerPeriodLoggerFailureDoesNotAffectHeartbeat) {
  gCalls.monotonicUs = STREAM_STATS_PERIOD_US + 1;
  SStreamHbMsg heartbeat = {};
  heartbeat.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(heartbeat.pStreamStatus, nullptr);
  ScopedRunnerDebugLogCapture capture;
  {
    Stub failLogStub;
    failLogStub.set(stRunnerTaskLogStats, failRunnerStatsLog);
    EXPECT_EQ(stmHbAddTaskStatus(task_.task.streamId, &heartbeat, &task_.task, task_.pStats), TSDB_CODE_SUCCESS);
  }

  ASSERT_EQ(taosArrayGetSize(heartbeat.pStreamStatus), 1);
  EXPECT_EQ(taosArrayGetSize(heartbeat.pTaskMetrics), 1);
  EXPECT_EQ(task_.task.status, STREAM_STATUS_RUNNING);
  ASSERT_EQ(gRunnerDebugLogs.size(), 1);
  EXPECT_NE(gRunnerDebugLogs.front().find("failed to rotate or log task statistics"), std::string::npos);
  tCleanupStreamHbMsg(&heartbeat, true);
}

TEST_F(StreamRunnerTest, BrandNewZeroWindowRequestOnReusedExecCreatesTableWithoutExecutingOrNotifying) {
  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  EXPECT_EQ(gCalls.resetCalls, 1);
  EXPECT_EQ(gCalls.executeCalls, 0);
  EXPECT_EQ(gCalls.forceOutputCalls, 0);
  EXPECT_EQ(gCalls.sinkCalls, 1);
  EXPECT_TRUE(gCalls.sinkBlockIsNull);
  EXPECT_TRUE(gCalls.autoCreateTable);
  EXPECT_EQ(gCalls.groupId, 42);
  EXPECT_STREQ(gCalls.tableName, "out_zero_window");
  EXPECT_EQ(gCalls.notifyCalls, 0);

  ASSERT_EQ(listNEles(task_.execMgr.pFreeExecs), 1);
  ASSERT_EQ(listNEles(task_.execMgr.pRunningExecs), 0);
  auto *pExec = reinterpret_cast<SStreamRunnerTaskExecution *>(tdListGetHead(task_.execMgr.pFreeExecs)->data);
  EXPECT_EQ(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, nullptr);
  ASSERT_NE(request_.params, nullptr);
  EXPECT_EQ(taosArrayGetSize(request_.params), 1);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.calcRequestCount, 1);
  EXPECT_EQ(stats.logicalWindowCount, 0);
  EXPECT_EQ(stats.resultLatency.samples, 0);
}

TEST_F(StreamRunnerTest, ExplicitRequestStartDrivesBatchWindowLatency) {
  SetWindows(2);
  gCalls.executeReadyTimes = {6000, 10000};

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.calcRequestCount, 1);
  EXPECT_EQ(stats.logicalWindowCount, 2);
  EXPECT_EQ(stats.resultLatency.samples, 2);
  EXPECT_EQ(stats.resultLatency.totalUs, 5000 + 9000);
  EXPECT_EQ(stats.noResultWindowCount, 2);
}

TEST_F(StreamRunnerTest, MultiGroupRequestCountsWindowsFromEveryGroup) {
  request_.isMultiGroupCalc = true;
  request_.createTable = 0;
  request_.pGroupCalcInfos = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request_.pGroupCalcInfos, nullptr);
  tSimpleHashSetFreeFp(request_.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);

  const int32_t groupWindowCounts[] = {2, 1};
  for (int64_t gid = 0; gid < 2; ++gid) {
    SSTriggerGroupCalcInfo info = {};
    info.pParams = taosArrayInit(groupWindowCounts[gid], sizeof(SSTriggerCalcParam));
    ASSERT_NE(info.pParams, nullptr);
    for (int32_t i = 0; i < groupWindowCounts[gid]; ++i) {
      SSTriggerCalcParam param = {};
      ASSERT_NE(taosArrayPush(info.pParams, &param), nullptr);
    }
    ASSERT_EQ(tSimpleHashPut(request_.pGroupCalcInfos, &gid, sizeof(gid), &info, sizeof(info)), TSDB_CODE_SUCCESS);
  }

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.calcRequestCount, 1);
  EXPECT_EQ(stats.logicalWindowCount, 3);
  EXPECT_EQ(gCalls.executeCalls, 0);
}

TEST_F(StreamRunnerTest, EmptySuccessfulWindowHasLatencySample) {
  SetWindows(1);
  gCalls.executeReadyTimes = {7000};

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 2000), TSDB_CODE_SUCCESS);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.resultLatency.totalUs, 5000);
  EXPECT_EQ(stats.noResultWindowCount, 1);
  EXPECT_EQ(stats.outputRows, 0);
  EXPECT_EQ(stats.outputBlocks, 0);
}

TEST_F(StreamRunnerTest, MultipleBlocksForOneWindowRecordOneReadySample) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(3), NewBlock(4), nullptr};
  gCalls.executeReadyTimes = {4000, 6000, 8000};
  gCalls.executeFinished = {false, false, true};
  task_.lowLatencyCalc = true;

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  EXPECT_EQ(gCalls.executeCalls, 3);
  EXPECT_EQ(gCalls.sinkCalls, 2);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.resultLatency.totalUs, 3000);
  EXPECT_EQ(stats.outputRows, 7);
  EXPECT_EQ(stats.outputBlocks, 2);
}

TEST_F(StreamRunnerTest, LaterBatchFailureKeepsEarlierLatencySample) {
  SetWindows(2);
  gCalls.executeCodes = {TSDB_CODE_SUCCESS, TSDB_CODE_INVALID_PARA};
  gCalls.executeReadyTimes = {6000, 10000};

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.resultLatency.totalUs, 5000);
  EXPECT_EQ(stats.calcFailureCount, 1);
  EXPECT_EQ(stats.calcDuration.samples, 1);
  EXPECT_EQ(stats.calcDuration.totalUs, 9000);
}

TEST_F(StreamRunnerTest, ExecutorBlockingIntervalsAreExcludedFromCalcDuration) {
  SetWindows(1);
  gCalls.executeReadyTimes = {10000};
  gCalls.executeBlockingIntervals = {{2000, 4000}, {5000, 8000}};
  SStreamRunnerTaskExecution *pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  gCalls.pRuntimeInfo = &pExec->runtimeInfo;

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.calcDuration.samples, 1);
  EXPECT_EQ(stats.calcDuration.totalUs, 4000);
}

TEST_F(StreamRunnerTest, SinkFailureDoesNotCountOutputOrLoseReadyLatency) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  gCalls.sinkCode = TSDB_CODE_INVALID_PARA;
  task_.lowLatencyCalc = true;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.outputRows, 0);
  EXPECT_EQ(stats.outputBlocks, 0);
  EXPECT_EQ(stats.sinkFailureCount, 1);
  EXPECT_EQ(stats.calcFailureCount, 0);
}

TEST_F(StreamRunnerTest, NotificationTargetsDoNotMultiplyCalcNotifyOnlyOutput) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  task_.notification.calcNotifyOnly = true;
  char  secondNotifyUrl[] = "ws://unused/second";
  char *pSecondNotifyUrl = secondNotifyUrl;
  ASSERT_NE(taosArrayPush(task_.notification.pNotifyAddrUrls, &pSecondNotifyUrl), nullptr);

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  EXPECT_EQ(gCalls.sinkCalls, 0);
  EXPECT_EQ(gCalls.notifyCalls, 1);
  EXPECT_EQ(gCalls.notifyTargets, 2);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.outputRows, 7);
  EXPECT_EQ(stats.outputBlocks, 1);
}

TEST_F(StreamRunnerTest, SuccessfulTopSinkAndNotificationCountLogicalOutputOnce) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  task_.lowLatencyCalc = true;

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  EXPECT_EQ(gCalls.sinkCalls, 1);
  EXPECT_EQ(gCalls.notifyCalls, 1);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.outputRows, 7);
  EXPECT_EQ(stats.outputBlocks, 1);
}

TEST_F(StreamRunnerTest, DroppedNotificationFailureCountsFailureButNotOutput) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  gCalls.notifyCode = TSDB_CODE_INVALID_PARA;
  task_.notification.calcNotifyOnly = true;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.notifyFailureCount, 1);
  EXPECT_EQ(stats.outputRows, 0);
  EXPECT_EQ(stats.calcFailureCount, 0);
}

TEST_F(StreamRunnerTest, DroppedNotifyFailureDoesNotHideLaterCalcFailure) {
  SetWindows(2);
  gCalls.executeBlocks = {NewBlock(7), nullptr};
  gCalls.executeCodes = {TSDB_CODE_SUCCESS, TSDB_CODE_INVALID_PARA};
  gCalls.executeReadyTimes = {6000, 10000};
  gCalls.notifyCode = TSDB_CODE_INVALID_PARA;
  task_.notification.calcNotifyOnly = true;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.notifyFailureCount, 1);
  EXPECT_EQ(stats.calcFailureCount, 1);
  EXPECT_EQ(stats.outputRows, 0);
}

TEST_F(StreamRunnerTest, ExternalWindowIndexesRecordDataAndGapWindowsExactlyOnce) {
  SetWindows(3);
  SSDataBlock *pBlock = NewBlock(3);
  gCalls.executeBlocks = {pBlock};
  gCalls.executeReadyTimes = {6000};
  task_.lowLatencyCalc = true;
  SStreamRunnerTaskExecution *pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  pExec->runtimeInfo.funcInfo.withExternalWindow = true;
  SArray *pIndexes = taosArrayInit(2, sizeof(int64_t));
  ASSERT_NE(pIndexes, nullptr);
  int64_t first = 0;
  int64_t third = (INT64_C(2) << 32) | 2;
  ASSERT_NE(taosArrayPush(pIndexes, &first), nullptr);
  ASSERT_NE(taosArrayPush(pIndexes, &third), nullptr);
  pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx = pIndexes;

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx = nullptr;
  taosArrayDestroy(pIndexes);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.logicalWindowCount, 3);
  EXPECT_EQ(stats.resultLatency.samples, 3);
  EXPECT_EQ(stats.noResultWindowCount, 1);
}

TEST_F(StreamRunnerTest, ExternalWindowContinuationBlocksRecordOneReadySample) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(3), NewBlock(4)};
  gCalls.executeReadyTimes = {4000, 6000};
  gCalls.executeFinished = {false, false};
  task_.lowLatencyCalc = true;
  SStreamRunnerTaskExecution *pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  gCalls.pRuntimeInfo = &pExec->runtimeInfo;
  gCalls.executeOutIndexes = {0, 1};
  pExec->runtimeInfo.funcInfo.withExternalWindow = true;
  SArray *pIndexes = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(pIndexes, nullptr);
  int64_t first = 0;
  ASSERT_NE(taosArrayPush(pIndexes, &first), nullptr);
  pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx = pIndexes;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx = nullptr;
  taosArrayDestroy(pIndexes);
  EXPECT_EQ(gCalls.executeCalls, 2);
  EXPECT_EQ(gCalls.sinkCalls, 2);
  EXPECT_EQ(gCalls.notifyCalls, 2);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.resultLatency.totalUs, 3000);
  EXPECT_EQ(stats.outputRows, 7);
  EXPECT_EQ(stats.outputBlocks, 2);
  EXPECT_EQ(stats.notifyFailureCount, 0);
}

TEST_F(StreamRunnerTest, ExternalReadySampleSurvivesNotificationPreparationFailure) {
  SetWindows(1);
  SSDataBlock *pBlock = NewBlock(1);
  gCalls.executeBlocks = {pBlock};
  gCalls.executeReadyTimes = {6000};
  gCalls.notifyBuildCode = TSDB_CODE_INVALID_PARA;
  task_.addOptions = NOTIFY_ON_FAILURE_PAUSE;
  SStreamRunnerTaskExecution *pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  pExec->runtimeInfo.funcInfo.withExternalWindow = true;
  SArray *pIndexes = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(pIndexes, nullptr);
  int64_t first = 0;
  ASSERT_NE(taosArrayPush(pIndexes, &first), nullptr);
  pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx = pIndexes;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);

  pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx = nullptr;
  taosArrayDestroy(pIndexes);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.resultLatency.totalUs, 5000);
  EXPECT_EQ(stats.notifyFailureCount, 1);
  EXPECT_EQ(stats.calcFailureCount, 0);
}

TEST_F(StreamRunnerTest, NonTopRunnerCountsReturnedBlock) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(5)};
  gCalls.executeReadyTimes = {6000};
  task_.topTask = false;

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  ASSERT_NE(request_.pOutBlock, nullptr);
  EXPECT_EQ(static_cast<SSDataBlock *>(request_.pOutBlock)->info.rows, 5);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.outputRows, 5);
  EXPECT_EQ(stats.outputBlocks, 1);
}

TEST_F(StreamRunnerTest, RuntimeInputCallbackRecordsRemoteAndFilteredLocalRows) {
  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  ASSERT_EQ(listNEles(task_.execMgr.pFreeExecs), 1);
  auto *pExec = reinterpret_cast<SStreamRunnerTaskExecution *>(tdListGetHead(task_.execMgr.pFreeExecs)->data);
  ASSERT_NE(pExec->runtimeInfo.inputStatsFp, nullptr);
  pExec->runtimeInfo.inputStatsFp(pExec->runtimeInfo.pInputStatsParam, 11, 1);
  pExec->runtimeInfo.inputStatsFp(pExec->runtimeInfo.pInputStatsParam, 3, 1);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.inputRows, 14);
  EXPECT_EQ(stats.inputBlocks, 2);
}

TEST_F(StreamRunnerTest, DropNotificationReportsRawTargetFailureWithoutChangingBusinessCode) {
  stub_.reset(streamSendNotifyContentWithResult);
  stub_.set(tcurlConnect, mockTcurlConnectFailure);
  SSTriggerCalcParam param = {};
  param.notifyType = STRIGGER_EVENT_WINDOW_OPEN;
  bool attempted = false;
  bool delivered = true;

  EXPECT_EQ(streamSendNotifyContentWithResult(&task_.task, "db.stream", "out", STREAM_TRIGGER_SESSION, 42,
                                              task_.notification.pNotifyAddrUrls, 0, &param, 1, &attempted, &delivered),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(attempted);
  EXPECT_FALSE(delivered);
}

TEST_F(StreamRunnerTest, DropNotificationReportsRawSendFailureWithoutChangingBusinessCode) {
  stub_.reset(streamSendNotifyContentWithResult);
  stub_.set(tcurlConnect, mockTcurlConnectSuccess);
  stub_.set(tcurlSend, mockTcurlSendFailure);
  SSTriggerCalcParam param = {};
  param.notifyType = STRIGGER_EVENT_WINDOW_OPEN;
  bool attempted = false;
  bool delivered = true;

  EXPECT_EQ(streamSendNotifyContentWithResult(&task_.task, "db.stream", "out", STREAM_TRIGGER_SESSION, 42,
                                              task_.notification.pNotifyAddrUrls, 0, &param, 1, &attempted, &delivered),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(attempted);
  EXPECT_FALSE(delivered);
}

}  // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
