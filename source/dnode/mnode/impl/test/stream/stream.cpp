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

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include "nodes.h"
#include "planner.h"
#include "stub.h"
#include "systable.h"
#include "tdatablock.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include <libs/transport/trpc.h>
#include "../../inc/mndSnode.h"
#include "../../inc/mndStream.h"

extern "C" int32_t msmBuildTriggerDeployInfo(SMnode *pMnode, SStmStatus *pInfo, SStmTaskDeploy *pDeploy,
                                             SStreamObj *pStream);
extern "C" void    msmStopStreamByError(int64_t streamId, SStmStatus *pStatus, int32_t errCode, int64_t currTs);
extern "C" int32_t msmNormalHandleHbMsg(SStmGrpCtx *pCtx);
extern "C" int32_t msmNormalHandleStatusUpdate(SStmGrpCtx *pCtx);
extern "C" int32_t mstSetStreamTaskResBlock(SStreamObj *pStream, SStmTaskStatus *pTask, SSDataBlock *pBlock,
                                            int32_t numOfRows);
extern "C" void    monReportStreamFailure(int64_t ts, int64_t streamId, const char *streamName, int32_t errorCode);

namespace {

struct CapturedStreamFailure {
  int32_t     calls = 0;
  int64_t     ts = 0;
  int64_t     streamId = 0;
  std::string streamName;
  int32_t     errorCode = 0;
};

CapturedStreamFailure gCapturedStreamFailure;

void captureStreamFailure(int64_t ts, int64_t streamId, const char *streamName, int32_t errorCode) {
  ++gCapturedStreamFailure.calls;
  gCapturedStreamFailure.ts = ts;
  gCapturedStreamFailure.streamId = streamId;
  gCapturedStreamFailure.streamName = streamName;
  gCapturedStreamFailure.errorCode = errorCode;
}

class StreamFailureStubGuard {
 public:
  StreamFailureStubGuard() {
    gCapturedStreamFailure = {};
    stub_.set(monReportStreamFailure, captureStreamFailure);
  }

  ~StreamFailureStubGuard() { stub_.reset(monReportStreamFailure); }

 private:
  Stub stub_;
};

int8_t *gExpectedStopped = nullptr;
int32_t gStoppedCompareExchangeCalls = 0;

int8_t failStoppedCompareExchange(int8_t volatile *ptr, int8_t oldval, int8_t newval) {
  if (ptr == gExpectedStopped) {
    ++gStoppedCompareExchangeCalls;
    return 1;
  }
  return oldval;
}

class StoppedCompareExchangeFailureGuard {
 public:
  explicit StoppedCompareExchangeFailureGuard(int8_t *stopped) {
    gExpectedStopped = stopped;
    gStoppedCompareExchangeCalls = 0;
    stub_.set(atomic_val_compare_exchange_8, failStoppedCompareExchange);
  }

  ~StoppedCompareExchangeFailureGuard() {
    stub_.reset(atomic_val_compare_exchange_8);
    gExpectedStopped = nullptr;
  }

 private:
  Stub stub_;
};

TEST(MndStreamFailureReportTest, ReportsOnceUntilRedeployResetsStoppedState) {
  constexpr int64_t kStreamId = 0x1234;
  constexpr int64_t kFirstTs = 1787068800123;
  constexpr int64_t kSecondTs = 1787068810123;
  char              streamName[] = "1.db.stream_a";
  SStmStatus        status = {};
  status.streamName = streamName;

  StreamFailureStubGuard guard;

  msmStopStreamByError(kStreamId, &status, TSDB_CODE_MND_STREAM_TASK_LOST, kFirstTs);
  EXPECT_EQ(atomic_load_8(&status.stopped), 1);
  EXPECT_EQ(gCapturedStreamFailure.calls, 1);
  EXPECT_EQ(gCapturedStreamFailure.ts, kFirstTs);
  EXPECT_EQ(gCapturedStreamFailure.streamId, kStreamId);
  EXPECT_EQ(gCapturedStreamFailure.streamName, streamName);
  EXPECT_EQ(gCapturedStreamFailure.errorCode, TSDB_CODE_MND_STREAM_TASK_LOST);

  msmStopStreamByError(kStreamId, &status, TSDB_CODE_MND_STREAM_VGROUP_LOST, kFirstTs + 1);
  EXPECT_EQ(gCapturedStreamFailure.calls, 1);
  EXPECT_EQ(gCapturedStreamFailure.errorCode, TSDB_CODE_MND_STREAM_TASK_LOST);

  atomic_store_8(&status.stopped, 0);
  msmStopStreamByError(kStreamId, &status, TSDB_CODE_MND_STREAM_VGROUP_LOST, kSecondTs);
  EXPECT_EQ(gCapturedStreamFailure.calls, 2);
  EXPECT_EQ(gCapturedStreamFailure.ts, kSecondTs);
  EXPECT_EQ(gCapturedStreamFailure.errorCode, TSDB_CODE_MND_STREAM_VGROUP_LOST);
}

TEST(MndStreamFailureReportTest, DoesNotReportWhenStoppedCompareExchangeFails) {
  constexpr int64_t kStreamId = 0x5678;
  char              streamName[] = "1.db.stream_cas_failure";
  SStmStatus        status = {};
  status.streamName = streamName;

  EXPECT_EQ(atomic_load_8(&status.stopped), 0);
  StreamFailureStubGuard             reportGuard;
  StoppedCompareExchangeFailureGuard compareExchangeGuard(&status.stopped);

  msmStopStreamByError(kStreamId, &status, TSDB_CODE_MND_STREAM_TASK_LOST, 1787068820123);

  EXPECT_EQ(gStoppedCompareExchangeCalls, 1);
  EXPECT_EQ(atomic_load_8(&status.stopped), 0);
  EXPECT_EQ(gCapturedStreamFailure.calls, 0);
}

STrans *createTestTrans() {
  STrans *pTrans = (STrans *)taosMemoryCalloc(1, sizeof(STrans));
  if (pTrans == nullptr) {
    return nullptr;
  }

  (void)taosThreadMutexInit(&pTrans->mutex, nullptr);
  pTrans->prepareActions = taosArrayInit(1, sizeof(STransAction));
  pTrans->redoActions = taosArrayInit(1, sizeof(STransAction));
  pTrans->undoActions = taosArrayInit(1, sizeof(STransAction));
  pTrans->commitActions = taosArrayInit(1, sizeof(STransAction));
  if (pTrans->prepareActions == nullptr || pTrans->redoActions == nullptr || pTrans->undoActions == nullptr ||
      pTrans->commitActions == nullptr) {
    mndTransDrop(pTrans);
    return nullptr;
  }

  return pTrans;
}

void destroyRecoveredRunnerLists(SStmStatus *pStatus) {
  for (int32_t i = 0; i < MND_STREAM_RUNNER_DEPLOY_NUM; ++i) {
    taosArrayDestroy(pStatus->runners[i]);
    pStatus->runners[i] = nullptr;
  }
}

class MndStreamActionQueueTest : public ::testing::Test {
 protected:
  void SetUp() override {
    queue_.head = static_cast<SStmQNode *>(taosMemoryCalloc(1, sizeof(SStmQNode)));
    ASSERT_NE(queue_.head, nullptr);
    queue_.tail = queue_.head;
  }

  void TearDown() override {
    SStmQNode *pNode = nullptr;
    while (mndStreamActionDequeue(&queue_, &pNode)) {
    }
    taosMemoryFreeClear(queue_.head);
    queue_.tail = nullptr;
  }

  SStmActionQ queue_ = {};
};

class MndStreamHeartbeatTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedActionQ_ = mStreamMgmt.actionQ;
    savedStreamMap_ = mStreamMgmt.streamMap;
    savedDnodeMap_ = mStreamMgmt.dnodeMap;
    savedActionQLock_ = mStreamMgmt.actionQLock;
    savedToDeployVgTaskNum_ = mStreamMgmt.toDeployVgTaskNum;
    savedToDeploySnodeTaskNum_ = mStreamMgmt.toDeploySnodeTaskNum;

    queue_.head = static_cast<SStmQNode *>(taosMemoryCalloc(1, sizeof(SStmQNode)));
    ASSERT_NE(queue_.head, nullptr);
    queue_.tail = queue_.head;

    mStreamMgmt.actionQ = &queue_;
    mStreamMgmt.actionQLock = 0;
    mStreamMgmt.toDeployVgTaskNum = 0;
    mStreamMgmt.toDeploySnodeTaskNum = 0;
    mStreamMgmt.streamMap = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    ASSERT_NE(mStreamMgmt.streamMap, nullptr);
    mStreamMgmt.dnodeMap = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    ASSERT_NE(mStreamMgmt.dnodeMap, nullptr);
  }

  void TearDown() override {
    SStmQNode *pNode = nullptr;
    while (mndStreamActionDequeue(&queue_, &pNode)) {
    }
    taosMemoryFreeClear(queue_.head);
    queue_.tail = nullptr;

    taosHashCleanup(mStreamMgmt.dnodeMap);
    taosHashCleanup(mStreamMgmt.streamMap);
    mStreamMgmt.actionQ = savedActionQ_;
    mStreamMgmt.streamMap = savedStreamMap_;
    mStreamMgmt.dnodeMap = savedDnodeMap_;
    mStreamMgmt.actionQLock = savedActionQLock_;
    mStreamMgmt.toDeployVgTaskNum = savedToDeployVgTaskNum_;
    mStreamMgmt.toDeploySnodeTaskNum = savedToDeploySnodeTaskNum_;
  }

  SStmActionQ queue_ = {};

 private:
  SStmActionQ *savedActionQ_ = nullptr;
  SHashObj    *savedStreamMap_ = nullptr;
  SHashObj    *savedDnodeMap_ = nullptr;
  SRWLatch     savedActionQLock_ = 0;
  int32_t      savedToDeployVgTaskNum_ = 0;
  int32_t      savedToDeploySnodeTaskNum_ = 0;
};

SStreamTaskMetricsEntry makeMetricEntry(int32_t index, int64_t streamId, int64_t taskId, int64_t seriousId,
                                        uint64_t outputRows = 7) {
  SStreamTaskMetricsEntry entry = {};
  entry.taskStatusIndex = index;
  entry.streamId = streamId;
  entry.taskId = taskId;
  entry.seriousId = seriousId;
  entry.snapshot.applicableMask = STREAM_METRIC_DELIVERED_OUTPUT;
  entry.snapshot.validMask = STREAM_METRIC_DELIVERED_OUTPUT;
  entry.snapshot.windowReady = true;
  entry.snapshot.deliveredOutputRows1m = outputRows;
  return entry;
}

SStmTaskStatusMsg makeStatusMessage(int64_t streamId, int64_t taskId, int64_t seriousId) {
  SStmTaskStatusMsg msg = {};
  msg.type = STREAM_RUNNER_TASK;
  msg.streamId = streamId;
  msg.taskId = taskId;
  msg.seriousId = seriousId;
  msg.nodeId = 1;
  msg.status = STREAM_STATUS_RUNNING;
  return msg;
}

SArray *failTaosArrayDup(const SArray *, __array_item_dup_fn_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

class TaosArrayDupFailureGuard {
 public:
  TaosArrayDupFailureGuard() : savedTerrno_(terrno) { stub_.set(taosArrayDup, failTaosArrayDup); }

  ~TaosArrayDupFailureGuard() {
    stub_.reset(taosArrayDup);
    terrno = savedTerrno_;
  }

 private:
  int32_t savedTerrno_ = TSDB_CODE_SUCCESS;
  Stub    stub_;
};

int32_t gTaosArrayDupCalls = 0;

SArray *failSecondTaosArrayDup(const SArray *pSrc, __array_item_dup_fn_t fn) {
  if (++gTaosArrayDupCalls == 2) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  if (fn != nullptr) {
    terrno = TSDB_CODE_INVALID_PARA;
    return nullptr;
  }
  return taosArrayFromList(pSrc->pData, pSrc->size, pSrc->elemSize);
}

class SecondTaosArrayDupFailureGuard {
 public:
  SecondTaosArrayDupFailureGuard() : savedTerrno_(terrno) {
    gTaosArrayDupCalls = 0;
    stub_.set(taosArrayDup, failSecondTaosArrayDup);
  }

  ~SecondTaosArrayDupFailureGuard() {
    stub_.reset(taosArrayDup);
    terrno = savedTerrno_;
  }

 private:
  int32_t savedTerrno_ = TSDB_CODE_SUCCESS;
  Stub    stub_;
};

int32_t gRecalcRecordArrayGetCalls = 0;

void *countRecalcRecordArrayGet(const SArray *pArray, size_t index) {
  if (pArray == nullptr || index >= pArray->size) return nullptr;
  if (pArray->elemSize == sizeof(SStmRecalcRecord)) ++gRecalcRecordArrayGetCalls;
  return TARRAY_GET_ELEM(pArray, index);
}

class RecalcRecordArrayGetGuard {
 public:
  RecalcRecordArrayGetGuard() {
    gRecalcRecordArrayGetCalls = 0;
    stub_.set(taosArrayGet, countRecalcRecordArrayGet);
  }

  ~RecalcRecordArrayGetGuard() { stub_.reset(taosArrayGet); }

 private:
  Stub stub_;
};

void   *gExpectedAtomicStoreTarget = nullptr;
int32_t gExpectedAtomicStoreCalls = 0;

void recordAtomicStorePtr(void *ptr, void *val) {
  if (ptr == gExpectedAtomicStoreTarget) ++gExpectedAtomicStoreCalls;
  *static_cast<void **>(ptr) = val;
}

class AtomicStorePtrGuard {
 public:
  explicit AtomicStorePtrGuard(void *pTarget) {
    gExpectedAtomicStoreTarget = pTarget;
    gExpectedAtomicStoreCalls = 0;
    stub_.set(atomic_store_ptr, recordAtomicStorePtr);
  }

  ~AtomicStorePtrGuard() {
    stub_.reset(atomic_store_ptr);
    gExpectedAtomicStoreTarget = nullptr;
  }

 private:
  Stub stub_;
};

SStmTaskStatus *appendTask(SArray **ppTasks, EStreamTaskType type, int64_t flags) {
  if (*ppTasks == nullptr) {
    *ppTasks = taosArrayInit(1, sizeof(SStmTaskStatus));
  }
  if (*ppTasks == nullptr) {
    return nullptr;
  }

  SStmTaskStatus task = {};
  task.type = type;
  task.flags = flags;
  return static_cast<SStmTaskStatus *>(taosArrayPush(*ppTasks, &task));
}

SSDataBlock *createSystemTableBlock(const char *tableName, int32_t capacity) {
  const SSysTableMeta *pMeta = getSysTableMeta("information_schema", tableName);
  if (pMeta == nullptr) {
    return nullptr;
  }

  SSDataBlock *pBlock = nullptr;
  if (createDataBlock(&pBlock) != TSDB_CODE_SUCCESS) {
    return nullptr;
  }

  for (int32_t i = 0; i < pMeta->colNum; ++i) {
    SColumnInfoData column = createColumnInfoData(pMeta->schema[i].type, pMeta->schema[i].bytes, i + 1);
    if (blockDataAppendColInfo(pBlock, &column) != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pBlock);
      return nullptr;
    }
  }

  if (blockDataEnsureCapacity(pBlock, capacity) != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return nullptr;
  }
  return pBlock;
}

double getDoubleValue(const SSDataBlock *pBlock, int32_t column, int32_t row) {
  const SColumnInfoData *pColumn = static_cast<const SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, column));
  double                 value = 0;
  std::memcpy(&value, colDataGetData(pColumn, row), sizeof(value));
  return value;
}

int64_t getInt64Value(const SSDataBlock *pBlock, int32_t column, int32_t row) {
  const SColumnInfoData *pColumn = static_cast<const SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, column));
  int64_t                value = 0;
  std::memcpy(&value, colDataGetData(pColumn, row), sizeof(value));
  return value;
}

int32_t getInt32Value(const SSDataBlock *pBlock, int32_t column, int32_t row) {
  const SColumnInfoData *pColumn = static_cast<const SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, column));
  int32_t                value = 0;
  std::memcpy(&value, colDataGetData(pColumn, row), sizeof(value));
  return value;
}

std::string getVarCharValue(const SSDataBlock *pBlock, int32_t column, int32_t row) {
  const SColumnInfoData *pColumn = static_cast<const SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, column));
  const char            *pValue = colDataGetData(pColumn, row);
  return std::string(varDataVal(pValue), varDataLen(pValue));
}

bool isNullValue(const SSDataBlock *pBlock, int32_t column, int32_t row, int32_t rows) {
  const SColumnInfoData *pColumn = static_cast<const SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, column));
  return colDataIsNull(pColumn, rows, row, nullptr);
}

SSnodeObj *returnNoSnode(SMnode *, int32_t) { return nullptr; }

void ignoreSnodeRelease(SMnode *, SSnodeObj *) {}

class StreamMapGuard {
 public:
  StreamMapGuard() : saved_(mStreamMgmt.streamMap) {
    mStreamMgmt.streamMap = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  }

  ~StreamMapGuard() {
    taosHashCleanup(mStreamMgmt.streamMap);
    mStreamMgmt.streamMap = saved_;
  }

  SHashObj *get() const { return mStreamMgmt.streamMap; }

 private:
  SHashObj *saved_ = nullptr;
};

class SnodeLookupStubGuard {
 public:
  SnodeLookupStubGuard() {
    stub_.set(mndAcquireSnode, returnNoSnode);
    stub_.set(mndReleaseSnode, ignoreSnodeRelease);
  }

  ~SnodeLookupStubGuard() {
    stub_.reset(mndReleaseSnode);
    stub_.reset(mndAcquireSnode);
  }

 private:
  Stub stub_;
};

class MndStreamTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedTaskMap_ = mStreamMgmt.taskMap;
    mStreamMgmt.taskMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    ASSERT_NE(mStreamMgmt.taskMap, nullptr);
  }

  void TearDown() override {
    taosHashCleanup(mStreamMgmt.taskMap);
    mStreamMgmt.taskMap = savedTaskMap_;
  }

  int32_t registerTask(const SStmTaskStatusMsg &msg, SStmTaskStatus *pStatus) {
    SStmTaskStatus *pStored = pStatus;
    return taosHashPut(mStreamMgmt.taskMap, &msg.streamId, sizeof(msg.streamId) + sizeof(msg.taskId), &pStored,
                       POINTER_BYTES);
  }

  int32_t handleHeartbeat(SStmTaskStatusMsg *pMsg, int32_t version, SArray *pMetrics) {
    SStreamHbMsg req = {};
    req.observabilityVersion = version;
    req.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
    if (req.pStreamStatus == nullptr || taosArrayPush(req.pStreamStatus, pMsg) == nullptr) {
      taosArrayDestroy(req.pStreamStatus);
      return terrno;
    }
    req.pTaskMetrics = pMetrics;

    SStmGrpCtx ctx = {};
    ctx.currTs = 100;
    ctx.pReq = &req;
    const int32_t code = msmNormalHandleStatusUpdate(&ctx);

    req.pTaskMetrics = nullptr;
    tCleanupStreamHbMsg(&req, true);
    return code;
  }

 private:
  SHashObj *savedTaskMap_ = nullptr;
};

int64_t gRecalcNowMs = 0;

int32_t returnRecalcTimeOfDay(struct timeval *pTime) {
  pTime->tv_sec = gRecalcNowMs / 1000;
  pTime->tv_usec = gRecalcNowMs % 1000 * 1000;
  return TSDB_CODE_SUCCESS;
}

class MndStreamRecalcTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedStreamMap_ = mStreamMgmt.streamMap;
    mStreamMgmt.streamMap = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    ASSERT_NE(mStreamMgmt.streamMap, nullptr);
    timeStub_.set(taosGetTimeOfDay, returnRecalcTimeOfDay);
    gRecalcNowMs = 1000;
    create_.name = streamName_;
    create_.streamDB = streamDb_;
    create_.sql = sql_;
    create_.streamId = 42;
    create_.fillHistory = true;
    tstrncpy(stream_.name, streamName_, sizeof(stream_.name));
    stream_.pCreate = &create_;

    trigger_.type = STREAM_TRIGGER_TASK;
    trigger_.status = STREAM_STATUS_RUNNING;
    trigger_.id.taskId = 20;
    trigger_.id.seriousId = 30;
    trigger_.id.nodeId = 1;
    status_.triggerTask = &trigger_;
    status_.pCreate = &create_;
    ASSERT_EQ(
        taosHashPut(mStreamMgmt.streamMap, &create_.streamId, sizeof(create_.streamId), &status_, sizeof(status_)),
        TSDB_CODE_SUCCESS);
    stored_ =
        static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &create_.streamId, sizeof(create_.streamId)));
    ASSERT_NE(stored_, nullptr);
    trigger_.pStream = stored_;
  }

  void TearDown() override {
    mstDestroySStmTaskStatus(&trigger_);
    taosArrayDestroy(stored_->userRecalcList);
    stored_->userRecalcList = nullptr;
    taosArrayDestroy(stored_->recalcRecords);
    stored_->recalcRecords = nullptr;
    timeStub_.reset(taosGetTimeOfDay);
    taosHashCleanup(mStreamMgmt.streamMap);
    mStreamMgmt.streamMap = savedStreamMap_;
  }

  int32_t Apply(int64_t recalcId, EStreamRecalcStatus status, int32_t progress, TSKEY start = 100, TSKEY end = 200,
                bool historyValid = false, int32_t historyProgress = 0) {
    SStreamTaskMetricsEntry entry = {};
    entry.taskStatusIndex = 0;
    entry.streamId = create_.streamId;
    entry.taskId = trigger_.id.taskId;
    entry.seriousId = trigger_.id.seriousId;
    entry.snapshot.applicableMask = STREAM_METRIC_HISTORY_PROGRESS | STREAM_METRIC_RECALCULATES;
    entry.snapshot.validMask = STREAM_METRIC_RECALCULATES;
    entry.snapshot.historyProgressValid = historyValid;
    entry.snapshot.historyProgressPct = historyProgress;
    if (historyValid) entry.snapshot.validMask |= STREAM_METRIC_HISTORY_PROGRESS;
    entry.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
    if (entry.snapshot.pRecalculates == nullptr) return terrno;
    SStreamRecalcSnapshot recalc = {};
    recalc.recalcId = recalcId;
    recalc.start = start;
    recalc.end = end;
    recalc.progressPct = progress;
    recalc.status = status;
    if (taosArrayPush(entry.snapshot.pRecalculates, &recalc) == nullptr) {
      const int32_t code = terrno;
      taosArrayDestroy(entry.snapshot.pRecalculates);
      return code;
    }
    const int32_t code = mstApplyTaskMetrics(&trigger_, 0, create_.streamId, &entry);
    taosArrayDestroy(entry.snapshot.pRecalculates);
    return code;
  }

  SSDataBlock *QueryRecalculates(int32_t capacity = 128) {
    SSDataBlock *pBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAM_RECALCULATES, capacity);
    if (pBlock == nullptr) return nullptr;
    int32_t rows = 0;
    if (mstSetStreamRecalculatesResBlock(&stream_, pBlock, &rows, capacity) != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pBlock);
      return nullptr;
    }
    return pBlock;
  }

  int32_t FindRecalcRow(const SSDataBlock *pBlock, int64_t recalcId) {
    char id[32] = {0};
    snprintf(id, sizeof(id), "%" PRIx64, recalcId);
    for (int32_t row = 0; row < pBlock->info.rows; ++row) {
      if (getVarCharValue(pBlock, 2, row) == id) return row;
    }
    return -1;
  }

  char               streamName_[32] = "test.stream";
  char               streamDb_[16] = "test";
  char               sql_[16] = "select 1";
  SCMCreateStreamReq create_ = {};
  SStreamObj         stream_ = {};
  SStmTaskStatus     trigger_ = {};
  SStmStatus         status_ = {};
  SStmStatus        *stored_ = nullptr;

 private:
  SHashObj *savedStreamMap_ = nullptr;
  Stub      timeStub_;
};

}  // namespace

TEST_F(MndStreamTest, StreamMetricsKeepIndependentValidity) {
  SStmStatus stream = {};
  stream.runnerDeploys = 1;
  stream.triggerTask = static_cast<SStmTaskStatus *>(taosMemoryCalloc(1, sizeof(SStmTaskStatus)));
  ASSERT_NE(stream.triggerTask, nullptr);
  stream.triggerTask->type = STREAM_TRIGGER_TASK;
  stream.triggerTask->metricsValid = true;
  stream.triggerTask->metrics.windowReady = true;
  stream.triggerTask->metrics.validMask = STREAM_METRIC_LOGICAL_INPUT | STREAM_METRIC_REALTIME_LAG;
  stream.triggerTask->metrics.logicalInputRows1m = 600;
  stream.triggerTask->metrics.realtimeLagMs = 1234;

  auto *runner0 = appendTask(&stream.runners[0], STREAM_RUNNER_TASK, STREAM_FLAG_TOP_RUNNER);
  auto *runner1 = appendTask(&stream.runners[0], STREAM_RUNNER_TASK, STREAM_FLAG_TOP_RUNNER);
  ASSERT_NE(runner0, nullptr);
  ASSERT_NE(runner1, nullptr);
  runner0 = static_cast<SStmTaskStatus *>(taosArrayGet(stream.runners[0], 0));
  runner1 = static_cast<SStmTaskStatus *>(taosArrayGet(stream.runners[0], 1));
  runner0->metricsValid = true;
  runner0->metrics.windowReady = true;
  runner0->metrics.validMask = STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY;
  runner0->metrics.deliveredOutputRows1m = 120;
  runner0->metrics.resultLatencyUs1m = 600000;
  runner0->metrics.resultLatencySamples1m = 3;
  runner1->metricsValid = false;

  SStreamMetricView view = {};
  ASSERT_EQ(mstBuildStreamMetricView(&stream, &view), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(view.inputRateValid);
  EXPECT_DOUBLE_EQ(view.inputRowsPerSec1m, 10.0);
  EXPECT_TRUE(view.realtimeLagValid);
  EXPECT_EQ(view.realtimeLagMs, 1234);
  EXPECT_FALSE(view.outputRateValid);
  EXPECT_FALSE(view.resultLatencyValid);

  mstClearTaskMetrics(stream.triggerTask);
  taosMemoryFree(stream.triggerTask);
  taosArrayDestroy(stream.runners[0]);
}

TEST_F(MndStreamTest, StreamMetricsTolerateMissingRunnerDeployments) {
  SStmStatus stream = {};
  stream.runnerDeploys = 1;

  SStreamMetricView view = {};
  ASSERT_EQ(mstBuildStreamMetricView(&stream, &view), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(view.outputRateValid);
  EXPECT_FALSE(view.resultLatencyValid);
}

TEST_F(MndStreamTest, RunnerLatencyUsesRawWeightedTotalsAcrossDeployments) {
  SStmStatus stream = {};
  stream.runnerDeploys = 2;
  auto *runner0 = appendTask(&stream.runners[0], STREAM_RUNNER_TASK, STREAM_FLAG_TOP_RUNNER);
  auto *runner1 = appendTask(&stream.runners[1], STREAM_RUNNER_TASK, STREAM_FLAG_TOP_RUNNER);
  auto *nonTop = appendTask(&stream.runners[2], STREAM_RUNNER_TASK, 0);
  ASSERT_NE(runner0, nullptr);
  ASSERT_NE(runner1, nullptr);
  ASSERT_NE(nonTop, nullptr);

  for (SStmTaskStatus *runner : {runner0, runner1, nonTop}) {
    runner->metricsValid = true;
    runner->metrics.windowReady = true;
    runner->metrics.validMask = STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY;
  }
  runner0->metrics.deliveredOutputRows1m = 60;
  runner1->metrics.deliveredOutputRows1m = 60;
  runner0->metrics.resultLatencyUs1m = 1000000;
  runner0->metrics.resultLatencySamples1m = 1;
  runner1->metrics.resultLatencyUs1m = 9000000;
  runner1->metrics.resultLatencySamples1m = 9;
  nonTop->metrics.deliveredOutputRows1m = (std::numeric_limits<uint64_t>::max)();
  nonTop->metrics.resultLatencyUs1m = (std::numeric_limits<uint64_t>::max)();
  nonTop->metrics.resultLatencySamples1m = 1;

  SStreamMetricView view = {};
  ASSERT_EQ(mstBuildStreamMetricView(&stream, &view), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(view.outputRateValid);
  EXPECT_DOUBLE_EQ(view.outputRowsPerSec1m, 2.0);
  ASSERT_TRUE(view.resultLatencyValid);
  EXPECT_DOUBLE_EQ(view.resultLatencyAvg1mMs, 1000.0);

  taosArrayDestroy(stream.runners[0]);
  taosArrayDestroy(stream.runners[1]);
  taosArrayDestroy(stream.runners[2]);
}

TEST_F(MndStreamTest, StreamMetricsHandleEmptyWindowsAndMissingLag) {
  SStmStatus stream = {};
  stream.runnerDeploys = 2;
  stream.triggerTask = static_cast<SStmTaskStatus *>(taosMemoryCalloc(1, sizeof(SStmTaskStatus)));
  ASSERT_NE(stream.triggerTask, nullptr);
  stream.triggerTask->type = STREAM_TRIGGER_TASK;
  stream.triggerTask->metricsValid = true;
  stream.triggerTask->metrics.validMask = STREAM_METRIC_LOGICAL_INPUT;

  auto *runner0 = appendTask(&stream.runners[0], STREAM_RUNNER_TASK, STREAM_FLAG_TOP_RUNNER);
  auto *runner1 = appendTask(&stream.runners[1], STREAM_RUNNER_TASK, STREAM_FLAG_TOP_RUNNER);
  ASSERT_NE(runner0, nullptr);
  ASSERT_NE(runner1, nullptr);
  for (SStmTaskStatus *runner : {runner0, runner1}) {
    runner->metricsValid = true;
    runner->metrics.windowReady = true;
    runner->metrics.validMask = STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY;
  }

  SStreamMetricView view = {};
  ASSERT_EQ(mstBuildStreamMetricView(&stream, &view), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(view.realtimeLagValid);
  ASSERT_TRUE(view.outputRateValid);
  EXPECT_DOUBLE_EQ(view.outputRowsPerSec1m, 0.0);
  EXPECT_FALSE(view.resultLatencyValid);

  mstClearTaskMetrics(stream.triggerTask);
  taosMemoryFree(stream.triggerTask);
  taosArrayDestroy(stream.runners[0]);
  taosArrayDestroy(stream.runners[1]);
}

TEST_F(MndStreamTest, StreamMetricTotalsDoNotWrap) {
  SStmStatus stream = {};
  stream.runnerDeploys = 2;
  auto *runner0 = appendTask(&stream.runners[0], STREAM_RUNNER_TASK, STREAM_FLAG_TOP_RUNNER);
  auto *runner1 = appendTask(&stream.runners[1], STREAM_RUNNER_TASK, STREAM_FLAG_TOP_RUNNER);
  ASSERT_NE(runner0, nullptr);
  ASSERT_NE(runner1, nullptr);
  for (SStmTaskStatus *runner : {runner0, runner1}) {
    runner->metricsValid = true;
    runner->metrics.windowReady = true;
    runner->metrics.validMask = STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY;
    runner->metrics.deliveredOutputRows1m = (std::numeric_limits<uint64_t>::max)();
    runner->metrics.resultLatencyUs1m = (std::numeric_limits<uint64_t>::max)();
    runner->metrics.resultLatencySamples1m = 1;
  }

  SStreamMetricView view = {};
  ASSERT_EQ(mstBuildStreamMetricView(&stream, &view), TSDB_CODE_SUCCESS);
  const double maxCounter = static_cast<double>((std::numeric_limits<uint64_t>::max)());
  ASSERT_TRUE(view.outputRateValid);
  EXPECT_DOUBLE_EQ(view.outputRowsPerSec1m, maxCounter / 30.0);
  ASSERT_TRUE(view.resultLatencyValid);
  EXPECT_DOUBLE_EQ(view.resultLatencyAvg1mMs, maxCounter / 1000.0);

  taosArrayDestroy(stream.runners[0]);
  taosArrayDestroy(stream.runners[1]);
}

TEST_F(MndStreamTest, RuntimeMetricSchemasAppendExpectedColumns) {
  const SSysTableMeta *pStreams = getSysTableMeta("information_schema", TSDB_INS_TABLE_STREAMS);
  ASSERT_NE(pStreams, nullptr);
  ASSERT_GE(pStreams->colNum, 5);
  const int32_t streamBase = pStreams->colNum - 5;
  EXPECT_STREQ(pStreams->schema[streamBase].name, "realtime_lag_ms");
  EXPECT_EQ(pStreams->schema[streamBase].type, TSDB_DATA_TYPE_BIGINT);
  EXPECT_STREQ(pStreams->schema[streamBase + 1].name, "input_rows_per_sec_1m");
  EXPECT_EQ(pStreams->schema[streamBase + 1].type, TSDB_DATA_TYPE_DOUBLE);
  EXPECT_STREQ(pStreams->schema[streamBase + 2].name, "output_rows_per_sec_1m");
  EXPECT_EQ(pStreams->schema[streamBase + 2].type, TSDB_DATA_TYPE_DOUBLE);
  EXPECT_STREQ(pStreams->schema[streamBase + 3].name, "runner_result_latency_avg_1m_ms");
  EXPECT_EQ(pStreams->schema[streamBase + 3].type, TSDB_DATA_TYPE_DOUBLE);
  EXPECT_STREQ(pStreams->schema[streamBase + 4].name, "history_progress_pct");
  EXPECT_EQ(pStreams->schema[streamBase + 4].type, TSDB_DATA_TYPE_INT);

  const SSysTableMeta *pTasks = getSysTableMeta("information_schema", TSDB_INS_TABLE_STREAM_TASKS);
  ASSERT_NE(pTasks, nullptr);
  ASSERT_GE(pTasks->colNum, 3);
  const int32_t taskBase = pTasks->colNum - 3;
  EXPECT_STREQ(pTasks->schema[taskBase].name, "input_rows_per_sec_1m");
  EXPECT_EQ(pTasks->schema[taskBase].type, TSDB_DATA_TYPE_DOUBLE);
  EXPECT_STREQ(pTasks->schema[taskBase + 1].name, "output_rows_per_sec_1m");
  EXPECT_EQ(pTasks->schema[taskBase + 1].type, TSDB_DATA_TYPE_DOUBLE);
  EXPECT_STREQ(pTasks->schema[taskBase + 2].name, "runner_result_latency_avg_1m_ms");
  EXPECT_EQ(pTasks->schema[taskBase + 2].type, TSDB_DATA_TYPE_DOUBLE);
}

TEST_F(MndStreamRecalcTest, RuntimeProgressSchemasAppendExpectedColumns) {
  const SSysTableMeta *pStreams = getSysTableMeta("information_schema", TSDB_INS_TABLE_STREAMS);
  ASSERT_NE(pStreams, nullptr);
  EXPECT_STREQ(pStreams->schema[pStreams->colNum - 1].name, "history_progress_pct");
  EXPECT_EQ(pStreams->schema[pStreams->colNum - 1].type, TSDB_DATA_TYPE_INT);

  const SSysTableMeta *pRecalculates = getSysTableMeta("information_schema", TSDB_INS_TABLE_STREAM_RECALCULATES);
  ASSERT_NE(pRecalculates, nullptr);
  EXPECT_STREQ(pRecalculates->schema[pRecalculates->colNum - 1].name, "status");
  EXPECT_EQ(pRecalculates->schema[pRecalculates->colNum - 1].type, TSDB_DATA_TYPE_VARCHAR);
}

TEST_F(MndStreamRecalcTest, AcceptedRecalcIsImmediatelyPending) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(mstAppendNewRecalcRange(create_.streamId, stored_, &range), TSDB_CODE_SUCCESS);
  ASSERT_NE(stored_->userRecalcList, nullptr);
  ASSERT_NE(stored_->recalcRecords, nullptr);
  ASSERT_EQ(taosArrayGetSize(stored_->userRecalcList), 1);
  ASSERT_EQ(taosArrayGetSize(stored_->recalcRecords), 1);
  const auto *request = static_cast<const SStreamRecalcReq *>(taosArrayGet(stored_->userRecalcList, 0));
  const auto *record = static_cast<const SStmRecalcRecord *>(taosArrayGet(stored_->recalcRecords, 0));
  ASSERT_NE(request, nullptr);
  ASSERT_NE(record, nullptr);
  EXPECT_EQ(record->snapshot.recalcId, request->recalcId);
  EXPECT_EQ(record->snapshot.start, request->start);
  EXPECT_EQ(record->snapshot.end, request->end);
  EXPECT_EQ(record->snapshot.progressPct, 0);
  EXPECT_EQ(record->snapshot.status, STREAM_RECALC_STATUS_PENDING);
  EXPECT_TRUE(record->typedStatusKnown);

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 1);
  EXPECT_EQ(getVarCharValue(pBlock, 5, 0), "0%");
  EXPECT_EQ(getVarCharValue(pBlock, 6, 0), "Pending");
  blockDataDestroy(pBlock);
}

TEST_F(MndStreamRecalcTest, AcceptedRecalcUsesAtomicDispatchPublication) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  {
    AtomicStorePtrGuard guard(&stored_->userRecalcList);
    ASSERT_EQ(mstAppendNewRecalcRange(create_.streamId, stored_, &range), TSDB_CODE_SUCCESS);
    EXPECT_EQ(gExpectedAtomicStoreCalls, 1);
  }

  auto *published = static_cast<SArray *>(atomic_load_ptr(&stored_->userRecalcList));
  ASSERT_NE(published, nullptr);
  EXPECT_EQ(taosArrayGetSize(published), 1);
  taosWLockLatch(&stored_->userRecalcLock);
  auto *dispatched = stored_->userRecalcList;
  stored_->userRecalcList = nullptr;
  taosWUnLockLatch(&stored_->userRecalcLock);
  EXPECT_EQ(dispatched, published);
  EXPECT_EQ(atomic_load_ptr(&stored_->userRecalcList), nullptr);
  taosArrayDestroy(dispatched);
}

TEST_F(MndStreamRecalcTest, PendingAndDispatchPublicationSurvivesSecondAllocationFailure) {
  STimeWindow firstRange = {};
  firstRange.skey = 100;
  firstRange.ekey = 200;
  ASSERT_EQ(mstAppendNewRecalcRange(create_.streamId, stored_, &firstRange), TSDB_CODE_SUCCESS);
  SArray     *oldRequests = stored_->userRecalcList;
  SArray     *oldRecords = stored_->recalcRecords;
  const auto *oldRequest = static_cast<const SStreamRecalcReq *>(taosArrayGet(oldRequests, 0));
  const auto *oldRecord = static_cast<const SStmRecalcRecord *>(taosArrayGet(oldRecords, 0));
  ASSERT_NE(oldRequest, nullptr);
  ASSERT_NE(oldRecord, nullptr);
  const int64_t oldRecalcId = oldRequest->recalcId;

  STimeWindow secondRange = {};
  secondRange.skey = 300;
  secondRange.ekey = 400;
  int32_t     code = TSDB_CODE_SUCCESS;
  {
    SecondTaosArrayDupFailureGuard guard;
    code = mstAppendNewRecalcRange(create_.streamId, stored_, &secondRange);
  }

  EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(stored_->userRecalcList, oldRequests);
  EXPECT_EQ(stored_->recalcRecords, oldRecords);
  ASSERT_EQ(taosArrayGetSize(stored_->userRecalcList), 1);
  ASSERT_EQ(taosArrayGetSize(stored_->recalcRecords), 1);
  EXPECT_EQ(static_cast<const SStreamRecalcReq *>(taosArrayGet(stored_->userRecalcList, 0))->recalcId, oldRecalcId);
  EXPECT_EQ(static_cast<const SStmRecalcRecord *>(taosArrayGet(stored_->recalcRecords, 0))->snapshot.recalcId,
            oldRecalcId);
}

TEST_F(MndStreamRecalcTest, ProgressAndStatusReplaceAtomically) {
  ASSERT_EQ(Apply(7, STREAM_RECALC_STATUS_RUNNING, 42), TSDB_CODE_SUCCESS);
  ASSERT_EQ(Apply(7, STREAM_RECALC_STATUS_FINISHED, 100), TSDB_CODE_SUCCESS);

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  const int32_t row = FindRecalcRow(pBlock, 7);
  ASSERT_GE(row, 0);
  EXPECT_EQ(getVarCharValue(pBlock, 5, row), "100%");
  EXPECT_EQ(getVarCharValue(pBlock, 6, row), "Finished");
  blockDataDestroy(pBlock);
}

TEST_F(MndStreamRecalcTest, TypedUpdateAllocationFailurePreservesOldRecordAndSnapshot) {
  ASSERT_EQ(Apply(70, STREAM_RECALC_STATUS_RUNNING, 25), TSDB_CODE_SUCCESS);
  SArray *oldRecords = stored_->recalcRecords;
  SArray *oldSnapshot = trigger_.metrics.pRecalculates;

  int32_t code = TSDB_CODE_SUCCESS;
  {
    SecondTaosArrayDupFailureGuard guard;
    code = Apply(70, STREAM_RECALC_STATUS_FINISHED, 100);
  }

  EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(stored_->recalcRecords, oldRecords);
  EXPECT_EQ(trigger_.metrics.pRecalculates, oldSnapshot);
  ASSERT_EQ(taosArrayGetSize(stored_->recalcRecords), 1);
  const auto *record = static_cast<const SStmRecalcRecord *>(taosArrayGet(stored_->recalcRecords, 0));
  ASSERT_NE(record, nullptr);
  EXPECT_EQ(record->snapshot.progressPct, 25);
  EXPECT_EQ(record->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
}

TEST_F(MndStreamRecalcTest, OneHundredPercentCannotRemainRunning) {
  ASSERT_EQ(Apply(8, STREAM_RECALC_STATUS_RUNNING, 25), TSDB_CODE_SUCCESS);
  EXPECT_EQ(Apply(8, STREAM_RECALC_STATUS_RUNNING, 100), TSDB_CODE_INVALID_PARA);

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  const int32_t row = FindRecalcRow(pBlock, 8);
  ASSERT_GE(row, 0);
  EXPECT_EQ(getVarCharValue(pBlock, 5, row), "25%");
  EXPECT_EQ(getVarCharValue(pBlock, 6, row), "Running");
  blockDataDestroy(pBlock);
}

TEST_F(MndStreamRecalcTest, TerminalStateCannotRegressOrRefreshRetentionTime) {
  ASSERT_EQ(Apply(9, STREAM_RECALC_STATUS_FINISHED, 100), TSDB_CODE_SUCCESS);
  const int64_t observedAt =
      static_cast<const SStmRecalcRecord *>(taosArrayGet(stored_->recalcRecords, 0))->terminalObservedAtMs;
  ASSERT_EQ(observedAt, 1000);
  gRecalcNowMs = 500;
  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_GE(FindRecalcRow(pBlock, 9), 0);
  blockDataDestroy(pBlock);

  gRecalcNowMs = 2000;
  ASSERT_EQ(Apply(9, STREAM_RECALC_STATUS_FINISHED, 100), TSDB_CODE_SUCCESS);
  ASSERT_EQ(static_cast<const SStmRecalcRecord *>(taosArrayGet(stored_->recalcRecords, 0))->terminalObservedAtMs,
            observedAt);
  EXPECT_EQ(Apply(9, STREAM_RECALC_STATUS_RUNNING, 50), TSDB_CODE_INVALID_MSG);

  gRecalcNowMs = 3601000;
  pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(FindRecalcRow(pBlock, 9), -1);
  EXPECT_EQ(pBlock->info.rows, 0);
  blockDataDestroy(pBlock);

  gRecalcNowMs = 3602000;
  mstInvalidateTaskMetrics(&trigger_);
  ASSERT_EQ(Apply(9, STREAM_RECALC_STATUS_FINISHED, 100), TSDB_CODE_SUCCESS);
  pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(FindRecalcRow(pBlock, 9), -1);
  EXPECT_EQ(pBlock->info.rows, 0);
  blockDataDestroy(pBlock);

  ASSERT_EQ(taosArrayGetSize(stored_->recalcRecords), 1);
  const auto *tombstone = static_cast<const SStmRecalcRecord *>(taosArrayGet(stored_->recalcRecords, 0));
  ASSERT_NE(tombstone, nullptr);
  EXPECT_TRUE(tombstone->hidden);
  EXPECT_EQ(tombstone->terminalObservedAtMs, 1000);

  ASSERT_EQ(Apply(10, STREAM_RECALC_STATUS_PENDING, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosArrayGetSize(stored_->recalcRecords), 1);
  const auto *pending = static_cast<const SStmRecalcRecord *>(taosArrayGet(stored_->recalcRecords, 0));
  ASSERT_NE(pending, nullptr);
  EXPECT_EQ(pending->snapshot.recalcId, 10);
  EXPECT_FALSE(pending->hidden);
  pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(FindRecalcRow(pBlock, 9), -1);
  EXPECT_GE(FindRecalcRow(pBlock, 10), 0);
  blockDataDestroy(pBlock);
}

TEST_F(MndStreamRecalcTest, OneHundredAndFirstTerminalEvictsOldest) {
  for (int64_t recalcId = 1000; recalcId <= 1100; ++recalcId) {
    gRecalcNowMs = recalcId;
    ASSERT_EQ(Apply(recalcId, STREAM_RECALC_STATUS_FINISHED, 100), TSDB_CODE_SUCCESS);
  }

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 100);
  EXPECT_EQ(FindRecalcRow(pBlock, 1000), -1);
  EXPECT_GE(FindRecalcRow(pBlock, 1001), 0);
  blockDataDestroy(pBlock);

  gRecalcNowMs = 1200;
  mstInvalidateTaskMetrics(&trigger_);
  ASSERT_EQ(Apply(1000, STREAM_RECALC_STATUS_FINISHED, 100), TSDB_CODE_SUCCESS);
  pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 100);
  EXPECT_EQ(FindRecalcRow(pBlock, 1000), -1);
  blockDataDestroy(pBlock);
}

TEST_F(MndStreamRecalcTest, EqualTerminalTimeEvictsSmallestRecalcId) {
  gRecalcNowMs = 5000;
  for (int64_t recalcId = 1; recalcId <= 101; ++recalcId) {
    ASSERT_EQ(Apply(recalcId, STREAM_RECALC_STATUS_FAILED, 50), TSDB_CODE_SUCCESS);
  }

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 100);
  EXPECT_EQ(FindRecalcRow(pBlock, 1), -1);
  EXPECT_GE(FindRecalcRow(pBlock, 2), 0);
  blockDataDestroy(pBlock);
}

TEST_F(MndStreamRecalcTest, ActiveRecordsIgnoreTerminalLimit) {
  for (int64_t recalcId = 1; recalcId <= 101; ++recalcId) {
    ASSERT_EQ(Apply(recalcId, STREAM_RECALC_STATUS_RUNNING, 1), TSDB_CODE_SUCCESS);
  }

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 101);
  blockDataDestroy(pBlock);
}

TEST_F(MndStreamRecalcTest, LargeTypedSnapshotDoesNotScanRecordsForEveryEntry) {
  constexpr int32_t       kRecalcCount = 64;
  SStreamTaskMetricsEntry entry = {};
  entry.taskStatusIndex = 0;
  entry.streamId = create_.streamId;
  entry.taskId = trigger_.id.taskId;
  entry.seriousId = trigger_.id.seriousId;
  entry.snapshot.applicableMask = STREAM_METRIC_RECALCULATES;
  entry.snapshot.validMask = STREAM_METRIC_RECALCULATES;
  entry.snapshot.pRecalculates = taosArrayInit(kRecalcCount, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(entry.snapshot.pRecalculates, nullptr);
  for (int32_t i = 0; i < kRecalcCount; ++i) {
    SStreamRecalcSnapshot recalc = {};
    recalc.recalcId = i + 1;
    recalc.start = 100;
    recalc.end = 200;
    recalc.progressPct = 1;
    recalc.status = STREAM_RECALC_STATUS_RUNNING;
    ASSERT_NE(taosArrayPush(entry.snapshot.pRecalculates, &recalc), nullptr);
  }

  int32_t code = TSDB_CODE_SUCCESS;
  {
    RecalcRecordArrayGetGuard guard;
    code = mstApplyTaskMetrics(&trigger_, 0, create_.streamId, &entry);
  }

  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_LE(gRecalcRecordArrayGetCalls, 3 * kRecalcCount);
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), kRecalcCount);
  taosArrayDestroy(entry.snapshot.pRecalculates);
}

TEST_F(MndStreamRecalcTest, SeriousIdChangeAndRemovalClearProgressState) {
  SSTriggerRuntimeStatus legacy = {};
  legacy.histroyProgress = 37;
  legacy.userRecalcs = taosArrayInit(1, sizeof(SSTriggerRecalcProgress));
  ASSERT_NE(legacy.userRecalcs, nullptr);
  SSTriggerRecalcProgress legacyRecalc = {};
  legacyRecalc.recalcId = 99;
  legacyRecalc.progress = 42;
  legacyRecalc.start = 100;
  legacyRecalc.end = 200;
  ASSERT_NE(taosArrayPush(legacy.userRecalcs, &legacyRecalc), nullptr);
  ASSERT_EQ(mstCopyTriggerRuntimeStatus(&trigger_, &legacy), TSDB_CODE_SUCCESS);
  taosArrayDestroy(legacy.userRecalcs);

  ASSERT_EQ(Apply(10, STREAM_RECALC_STATUS_RUNNING, 10, 100, 200, true, 40), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosArrayGetSize(stored_->recalcRecords), 1);
  EXPECT_TRUE(trigger_.metricsValid);
  EXPECT_EQ(mstBumpTaskSeriousId(&trigger_), 31);
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 0);
  EXPECT_FALSE(trigger_.metricsValid);
  EXPECT_EQ(trigger_.metrics.applicableMask, 0U);
  EXPECT_EQ(trigger_.metrics.validMask, 0U);
  EXPECT_EQ(trigger_.metrics.pRecalculates, nullptr);
  EXPECT_EQ(trigger_.detailStatus, nullptr);

  SStreamMetricView view = {};
  ASSERT_EQ(mstBuildStreamMetricView(stored_, &view), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(view.historyProgressValid);
  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 0);
  blockDataDestroy(pBlock);

  ASSERT_EQ(Apply(11, STREAM_RECALC_STATUS_RUNNING, 20), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosArrayGetSize(stored_->recalcRecords), 1);
  mstDestroySStmTaskStatus(&trigger_);
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 0);
  EXPECT_FALSE(trigger_.metricsValid);
  EXPECT_EQ(trigger_.metrics.applicableMask, 0U);
  EXPECT_EQ(trigger_.metrics.validMask, 0U);
  EXPECT_EQ(trigger_.metrics.pRecalculates, nullptr);
}

TEST_F(MndStreamRecalcTest, OldTriggerProgressFeedsLegacyColumnsWithNullStatus) {
  SSTriggerRuntimeStatus legacy = {};
  legacy.histroyProgress = 37;
  legacy.userRecalcs = taosArrayInit(2, sizeof(SSTriggerRecalcProgress));
  ASSERT_NE(legacy.userRecalcs, nullptr);
  SSTriggerRecalcProgress recalc = {};
  recalc.recalcId = 12;
  recalc.progress = 42;
  recalc.start = 100;
  recalc.end = 200;
  ASSERT_NE(taosArrayPush(legacy.userRecalcs, &recalc), nullptr);
  SSTriggerRecalcProgress sameAsTyped = {};
  sameAsTyped.recalcId = 13;
  sameAsTyped.progress = 42;
  sameAsTyped.start = 100;
  sameAsTyped.end = 200;
  ASSERT_NE(taosArrayPush(legacy.userRecalcs, &sameAsTyped), nullptr);
  ASSERT_EQ(mstCopyTriggerRuntimeStatus(&trigger_, &legacy), TSDB_CODE_SUCCESS);
  taosArrayDestroy(legacy.userRecalcs);

  SnodeLookupStubGuard snodeLookup;
  SMnode               mnode = {};
  SSDataBlock         *pStreamBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(pStreamBlock, nullptr);
  ASSERT_EQ(mstSetStreamAttrResBlock(&mnode, &stream_, pStreamBlock, 0), TSDB_CODE_SUCCESS);
  EXPECT_EQ(getInt32Value(pStreamBlock, taosArrayGetSize(pStreamBlock->pDataBlock) - 1, 0), 37);
  blockDataDestroy(pStreamBlock);

  ASSERT_EQ(Apply(13, STREAM_RECALC_STATUS_PENDING, 0), TSDB_CODE_SUCCESS);
  SSDataBlock *pRecalcBlock = QueryRecalculates();
  ASSERT_NE(pRecalcBlock, nullptr);
  const int32_t row = FindRecalcRow(pRecalcBlock, 12);
  ASSERT_GE(row, 0);
  EXPECT_EQ(getVarCharValue(pRecalcBlock, 5, row), "42%");
  EXPECT_TRUE(isNullValue(pRecalcBlock, 6, row, pRecalcBlock->info.rows));
  const int32_t typedRow = FindRecalcRow(pRecalcBlock, 13);
  ASSERT_GE(typedRow, 0);
  EXPECT_EQ(getVarCharValue(pRecalcBlock, 5, typedRow), "0%");
  EXPECT_EQ(getVarCharValue(pRecalcBlock, 6, typedRow), "Pending");
  blockDataDestroy(pRecalcBlock);
}

TEST_F(MndStreamRecalcTest, TypedHistoryInvalidityOverridesLegacyProgress) {
  SSTriggerRuntimeStatus legacy = {};
  legacy.histroyProgress = 37;
  ASSERT_EQ(mstCopyTriggerRuntimeStatus(&trigger_, &legacy), TSDB_CODE_SUCCESS);
  ASSERT_EQ(Apply(13, STREAM_RECALC_STATUS_RUNNING, 1, 100, 200, false, 0), TSDB_CODE_SUCCESS);

  SnodeLookupStubGuard snodeLookup;
  SMnode               mnode = {};
  SSDataBlock         *pBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(mstSetStreamAttrResBlock(&mnode, &stream_, pBlock, 0), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(isNullValue(pBlock, taosArrayGetSize(pBlock->pDataBlock) - 1, 0, 1));
  blockDataDestroy(pBlock);

  mstInvalidateTaskMetrics(&trigger_);
  pBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(mstSetStreamAttrResBlock(&mnode, &stream_, pBlock, 0), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(isNullValue(pBlock, taosArrayGetSize(pBlock->pDataBlock) - 1, 0, 1));
  blockDataDestroy(pBlock);
}

TEST_F(MndStreamTest, TaskMetricColumnsPreserveApplicability) {
  const SSysTableMeta *pMeta = getSysTableMeta("information_schema", TSDB_INS_TABLE_STREAM_TASKS);
  ASSERT_NE(pMeta, nullptr);
  SSDataBlock *pBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAM_TASKS, 3);
  ASSERT_NE(pBlock, nullptr);
  const int32_t inputColumn = pMeta->colNum - 3;
  const int32_t outputColumn = pMeta->colNum - 2;
  const int32_t latencyColumn = pMeta->colNum - 1;

  char               streamName[] = "test.stream";
  SCMCreateStreamReq createReq = {};
  createReq.streamId = 42;
  SStreamObj stream = {};
  tstrncpy(stream.name, streamName, sizeof(stream.name));
  stream.pCreate = &createReq;

  SStmStatus topology = {};
  topology.runnerDeploys = 1;
  auto *storedRunner = appendTask(&topology.runners[0], STREAM_RUNNER_TASK, 0);
  ASSERT_NE(storedRunner, nullptr);
  storedRunner->id.taskId = 10;
  storedRunner->id.seriousId = 20;
  storedRunner->id.deployId = 0;
  StreamMapGuard streamMap;
  ASSERT_NE(streamMap.get(), nullptr);
  ASSERT_EQ(
      taosHashPut(mStreamMgmt.streamMap, &createReq.streamId, sizeof(createReq.streamId), &topology, sizeof(topology)),
      TSDB_CODE_SUCCESS);
  auto *storedTopology =
      static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &createReq.streamId, sizeof(createReq.streamId)));
  ASSERT_NE(storedTopology, nullptr);
  storedRunner = static_cast<SStmTaskStatus *>(taosArrayGetLast(storedTopology->runners[0]));
  ASSERT_NE(storedRunner, nullptr);

  SStmTaskStatus reader = {};
  reader.type = STREAM_READER_TASK;
  reader.flags = STREAM_FLAG_TRIGGER_READER;
  reader.metricsValid = true;
  reader.metrics.windowReady = true;
  reader.metrics.validMask = STREAM_METRIC_PHYSICAL_INPUT;
  reader.metrics.physicalInputRows1m = 600;
  ASSERT_EQ(mstSetStreamTaskResBlock(&stream, &reader, pBlock, 0), TSDB_CODE_SUCCESS);
  EXPECT_DOUBLE_EQ(getDoubleValue(pBlock, inputColumn, 0), 10.0);
  EXPECT_TRUE(isNullValue(pBlock, outputColumn, 0, 3));
  EXPECT_TRUE(isNullValue(pBlock, latencyColumn, 0, 3));

  storedRunner->metricsValid = true;
  storedRunner->metrics.windowReady = true;
  storedRunner->metrics.validMask = STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY;
  storedRunner->metrics.deliveredOutputRows1m = 120;
  storedRunner->metrics.resultLatencyUs1m = 3000;
  storedRunner->metrics.resultLatencySamples1m = 2;
  ASSERT_EQ(mstSetStreamTaskResBlock(&stream, storedRunner, pBlock, 1), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(isNullValue(pBlock, inputColumn, 1, 3));
  EXPECT_DOUBLE_EQ(getDoubleValue(pBlock, outputColumn, 1), 2.0);
  EXPECT_DOUBLE_EQ(getDoubleValue(pBlock, latencyColumn, 1), 1.5);

  SStmTaskStatus nonTop = {};
  nonTop.type = STREAM_RUNNER_TASK;
  nonTop.metricsValid = true;
  nonTop.metrics.windowReady = true;
  nonTop.metrics.validMask = STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY;
  ASSERT_EQ(mstSetStreamTaskResBlock(&stream, &nonTop, pBlock, 2), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(isNullValue(pBlock, inputColumn, 2, 3));
  EXPECT_TRUE(isNullValue(pBlock, outputColumn, 2, 3));
  EXPECT_TRUE(isNullValue(pBlock, latencyColumn, 2, 3));

  taosArrayDestroy(topology.runners[0]);
  blockDataDestroy(pBlock);
}

TEST_F(MndStreamTest, StreamMetricColumnsPreserveIndependentNullability) {
  const SSysTableMeta *pMeta = getSysTableMeta("information_schema", TSDB_INS_TABLE_STREAMS);
  ASSERT_NE(pMeta, nullptr);
  SSDataBlock *pBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(pBlock, nullptr);

  char               streamName[] = "test.stream";
  char               streamDb[] = "test";
  char               sql[] = "select 1";
  SCMCreateStreamReq createReq = {};
  createReq.name = streamName;
  createReq.streamDB = streamDb;
  createReq.sql = sql;
  createReq.streamId = 42;
  SStreamObj stream = {};
  tstrncpy(stream.name, streamName, sizeof(stream.name));
  stream.pCreate = &createReq;

  SStmTaskStatus trigger = {};
  trigger.type = STREAM_TRIGGER_TASK;
  trigger.status = STREAM_STATUS_RUNNING;
  trigger.metricsValid = true;
  trigger.metrics.windowReady = true;
  trigger.metrics.validMask = STREAM_METRIC_LOGICAL_INPUT | STREAM_METRIC_REALTIME_LAG;
  trigger.metrics.logicalInputRows1m = 600;
  trigger.metrics.realtimeLagMs = 1234;
  SStmStatus status = {};
  status.triggerTask = &trigger;
  auto *runner = appendTask(&status.runners[7], STREAM_RUNNER_TASK, STREAM_FLAG_TOP_RUNNER);
  ASSERT_NE(runner, nullptr);
  runner->metricsValid = false;

  StreamMapGuard streamMap;
  ASSERT_NE(streamMap.get(), nullptr);
  ASSERT_EQ(
      taosHashPut(mStreamMgmt.streamMap, &createReq.streamId, sizeof(createReq.streamId), &status, sizeof(status)),
      TSDB_CODE_SUCCESS);

  SnodeLookupStubGuard snodeLookup;
  SMnode               mnode = {};
  ASSERT_EQ(mstSetStreamAttrResBlock(&mnode, &stream, pBlock, 0), TSDB_CODE_SUCCESS);

  const int32_t lagColumn = pMeta->colNum - 5;
  const int32_t inputColumn = pMeta->colNum - 4;
  const int32_t outputColumn = pMeta->colNum - 3;
  const int32_t latencyColumn = pMeta->colNum - 2;
  const int32_t historyColumn = pMeta->colNum - 1;
  EXPECT_EQ(getInt64Value(pBlock, lagColumn, 0), 1234);
  EXPECT_DOUBLE_EQ(getDoubleValue(pBlock, inputColumn, 0), 10.0);
  EXPECT_TRUE(isNullValue(pBlock, outputColumn, 0, 1));
  EXPECT_TRUE(isNullValue(pBlock, latencyColumn, 0, 1));
  EXPECT_TRUE(isNullValue(pBlock, historyColumn, 0, 1));

  taosArrayDestroy(status.runners[7]);
  blockDataDestroy(pBlock);
}

TEST_F(MndStreamTest, FinalRunnerTopologyDrivesStreamAndTaskMetrics) {
  const SSysTableMeta *pStreamMeta = getSysTableMeta("information_schema", TSDB_INS_TABLE_STREAMS);
  const SSysTableMeta *pTaskMeta = getSysTableMeta("information_schema", TSDB_INS_TABLE_STREAM_TASKS);
  ASSERT_NE(pStreamMeta, nullptr);
  ASSERT_NE(pTaskMeta, nullptr);
  SSDataBlock *pStreamBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  SSDataBlock *pTaskBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAM_TASKS, 4);
  ASSERT_NE(pStreamBlock, nullptr);
  ASSERT_NE(pTaskBlock, nullptr);

  char               streamName[] = "test.stream";
  char               streamDb[] = "test";
  char               sql[] = "select 1";
  SCMCreateStreamReq createReq = {};
  createReq.name = streamName;
  createReq.streamDB = streamDb;
  createReq.sql = sql;
  createReq.streamId = 43;
  SStreamObj stream = {};
  tstrncpy(stream.name, streamName, sizeof(stream.name));
  stream.pCreate = &createReq;

  SStmStatus status = {};
  status.runnerDeploys = 2;
  auto *intermediate = appendTask(&status.runners[0], STREAM_RUNNER_TASK, STREAM_FLAG_TOP_RUNNER);
  ASSERT_NE(intermediate, nullptr);
  ASSERT_NE(appendTask(&status.runners[0], STREAM_RUNNER_TASK, 0), nullptr);
  intermediate = static_cast<SStmTaskStatus *>(taosArrayGet(status.runners[0], 0));
  auto *final0 = static_cast<SStmTaskStatus *>(taosArrayGetLast(status.runners[0]));
  auto *final1 = appendTask(&status.runners[1], STREAM_RUNNER_TASK, 0);
  ASSERT_NE(final0, nullptr);
  ASSERT_NE(final1, nullptr);
  intermediate->id.taskId = 10;
  intermediate->id.seriousId = 20;
  intermediate->id.deployId = 0;
  final0->id.taskId = 11;
  final0->id.seriousId = 21;
  final0->id.deployId = 0;
  final1->id.taskId = 12;
  final1->id.seriousId = 22;
  final1->id.deployId = 1;
  for (SStmTaskStatus *runner : {intermediate, final0, final1}) {
    runner->metricsValid = true;
    runner->metrics.windowReady = true;
    runner->metrics.validMask = STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY;
  }
  intermediate->metrics.deliveredOutputRows1m = 6000;
  intermediate->metrics.resultLatencyUs1m = 6000000;
  intermediate->metrics.resultLatencySamples1m = 1;
  final0->metrics.deliveredOutputRows1m = 60;
  final0->metrics.resultLatencyUs1m = 1000000;
  final0->metrics.resultLatencySamples1m = 1;
  final1->metrics.deliveredOutputRows1m = 60;
  final1->metrics.resultLatencyUs1m = 9000000;
  final1->metrics.resultLatencySamples1m = 9;

  StreamMapGuard streamMap;
  ASSERT_NE(streamMap.get(), nullptr);
  ASSERT_EQ(
      taosHashPut(mStreamMgmt.streamMap, &createReq.streamId, sizeof(createReq.streamId), &status, sizeof(status)),
      TSDB_CODE_SUCCESS);
  auto *storedStatus =
      static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &createReq.streamId, sizeof(createReq.streamId)));
  ASSERT_NE(storedStatus, nullptr);
  auto *storedIntermediate = static_cast<SStmTaskStatus *>(taosArrayGet(storedStatus->runners[0], 0));
  auto *storedFinal0 = static_cast<SStmTaskStatus *>(taosArrayGetLast(storedStatus->runners[0]));
  auto *storedFinal1 = static_cast<SStmTaskStatus *>(taosArrayGetLast(storedStatus->runners[1]));
  ASSERT_NE(storedIntermediate, nullptr);
  ASSERT_NE(storedFinal0, nullptr);
  ASSERT_NE(storedFinal1, nullptr);

  SnodeLookupStubGuard snodeLookup;
  SMnode               mnode = {};
  ASSERT_EQ(mstSetStreamAttrResBlock(&mnode, &stream, pStreamBlock, 0), TSDB_CODE_SUCCESS);
  const int32_t streamOutputColumn = pStreamMeta->colNum - 3;
  const int32_t streamLatencyColumn = pStreamMeta->colNum - 2;
  EXPECT_DOUBLE_EQ(getDoubleValue(pStreamBlock, streamOutputColumn, 0), 2.0);
  EXPECT_DOUBLE_EQ(getDoubleValue(pStreamBlock, streamLatencyColumn, 0), 1000.0);

  ASSERT_EQ(mstSetStreamTaskResBlock(&stream, storedIntermediate, pTaskBlock, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(mstSetStreamTaskResBlock(&stream, storedFinal0, pTaskBlock, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(mstSetStreamTaskResBlock(&stream, storedFinal1, pTaskBlock, 2), TSDB_CODE_SUCCESS);
  const int32_t taskOutputColumn = pTaskMeta->colNum - 2;
  const int32_t taskLatencyColumn = pTaskMeta->colNum - 1;
  EXPECT_TRUE(isNullValue(pTaskBlock, taskOutputColumn, 0, 3));
  EXPECT_TRUE(isNullValue(pTaskBlock, taskLatencyColumn, 0, 3));
  EXPECT_DOUBLE_EQ(getDoubleValue(pTaskBlock, taskOutputColumn, 1), 1.0);
  EXPECT_DOUBLE_EQ(getDoubleValue(pTaskBlock, taskLatencyColumn, 1), 1000.0);
  EXPECT_DOUBLE_EQ(getDoubleValue(pTaskBlock, taskOutputColumn, 2), 1.0);
  EXPECT_DOUBLE_EQ(getDoubleValue(pTaskBlock, taskLatencyColumn, 2), 1000.0);

  taosArrayDestroy(status.runners[0]);
  taosArrayDestroy(status.runners[1]);
  blockDataDestroy(pStreamBlock);
  blockDataDestroy(pTaskBlock);
}

TEST_F(MndStreamTest, NormalHeartbeatWithoutTailInvalidatesMetrics) {
  constexpr int64_t streamId = 10;
  SStmStatus        stream = {};
  SStmTaskStatus    status = {};
  status.pStream = &stream;
  status.type = STREAM_RUNNER_TASK;
  status.id.taskId = 20;
  status.id.seriousId = 30;
  status.id.nodeId = 1;
  SStmTaskStatusMsg msg = makeStatusMessage(streamId, 20, 30);
  ASSERT_EQ(registerTask(msg, &status), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsEntry initial = makeMetricEntry(0, streamId, 20, 30);
  ASSERT_EQ(mstApplyTaskMetrics(&status, 0, streamId, &initial), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(status.metricsValid);

  EXPECT_EQ(handleHeartbeat(&msg, 0, nullptr), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(status.metricsValid);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, NormalHeartbeatWithUnknownTailInvalidatesMetrics) {
  constexpr int64_t streamId = 10;
  SStmStatus        stream = {};
  SStmTaskStatus    status = {};
  status.pStream = &stream;
  status.type = STREAM_RUNNER_TASK;
  status.id.taskId = 20;
  status.id.seriousId = 30;
  status.id.nodeId = 1;
  SStmTaskStatusMsg msg = makeStatusMessage(streamId, 20, 30);
  ASSERT_EQ(registerTask(msg, &status), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsEntry initial = makeMetricEntry(0, streamId, 20, 30);
  ASSERT_EQ(mstApplyTaskMetrics(&status, 0, streamId, &initial), TSDB_CODE_SUCCESS);
  SArray *metrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  ASSERT_NE(metrics, nullptr);
  ASSERT_NE(taosArrayPush(metrics, &initial), nullptr);

  EXPECT_EQ(handleHeartbeat(&msg, STREAM_HB_OBSERVABILITY_VERSION_V1 + 1, metrics), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(status.metricsValid);
  taosArrayDestroy(metrics);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, KnownTailMissingEntryLeavesMetricsInvalid) {
  constexpr int64_t streamId = 10;
  SStmStatus        stream = {};
  SStmTaskStatus    status = {};
  status.pStream = &stream;
  status.type = STREAM_RUNNER_TASK;
  status.id.taskId = 20;
  status.id.seriousId = 30;
  status.id.nodeId = 1;
  SStmTaskStatusMsg msg = makeStatusMessage(streamId, 20, 30);
  ASSERT_EQ(registerTask(msg, &status), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsEntry initial = makeMetricEntry(0, streamId, 20, 30);
  ASSERT_EQ(mstApplyTaskMetrics(&status, 0, streamId, &initial), TSDB_CODE_SUCCESS);
  SArray *metrics = taosArrayInit(0, sizeof(SStreamTaskMetricsEntry));
  ASSERT_NE(metrics, nullptr);

  EXPECT_EQ(handleHeartbeat(&msg, STREAM_HB_OBSERVABILITY_VERSION_V1, metrics), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(status.metricsValid);
  EXPECT_EQ(status.status, STREAM_STATUS_RUNNING);
  EXPECT_EQ(status.lastUpTs, 100);
  taosArrayDestroy(metrics);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, DuplicateMetricIndexLeavesMetricsInvalid) {
  constexpr int64_t streamId = 10;
  SStmStatus        stream = {};
  SStmTaskStatus    status = {};
  status.pStream = &stream;
  status.type = STREAM_RUNNER_TASK;
  status.id.taskId = 20;
  status.id.seriousId = 30;
  status.id.nodeId = 1;
  SStmTaskStatusMsg msg = makeStatusMessage(streamId, 20, 30);
  ASSERT_EQ(registerTask(msg, &status), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsEntry entry = makeMetricEntry(0, streamId, 20, 30);
  SArray                 *metrics = taosArrayInit(2, sizeof(SStreamTaskMetricsEntry));
  ASSERT_NE(metrics, nullptr);
  ASSERT_NE(taosArrayPush(metrics, &entry), nullptr);
  ASSERT_NE(taosArrayPush(metrics, &entry), nullptr);

  EXPECT_EQ(handleHeartbeat(&msg, STREAM_HB_OBSERVABILITY_VERSION_V1, metrics), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(status.metricsValid);
  EXPECT_EQ(status.status, STREAM_STATUS_RUNNING);
  EXPECT_EQ(status.lastUpTs, 100);
  taosArrayDestroy(metrics);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, MetricEntryDecodeErrorLeavesMetricsInvalid) {
  constexpr int64_t streamId = 10;
  SStmStatus        stream = {};
  SStmTaskStatus    status = {};
  status.pStream = &stream;
  status.type = STREAM_RUNNER_TASK;
  status.id.taskId = 20;
  status.id.seriousId = 30;
  status.id.nodeId = 1;
  SStmTaskStatusMsg msg = makeStatusMessage(streamId, 20, 30);
  ASSERT_EQ(registerTask(msg, &status), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsEntry entry = makeMetricEntry(0, streamId, 20, 30);
  entry.decodeCode = TSDB_CODE_INVALID_MSG;
  SArray *metrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  ASSERT_NE(metrics, nullptr);
  ASSERT_NE(taosArrayPush(metrics, &entry), nullptr);

  EXPECT_EQ(handleHeartbeat(&msg, STREAM_HB_OBSERVABILITY_VERSION_V1, metrics), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(status.metricsValid);
  EXPECT_EQ(status.status, STREAM_STATUS_RUNNING);
  EXPECT_EQ(status.lastUpTs, 100);
  taosArrayDestroy(metrics);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, AbnormalReaderHeartbeatsPreserveLastSuccessfulMetrics) {
  constexpr int64_t streamId = 10;
  SStmStatus        stream = {};
  SStmTaskStatus    status = {};
  status.pStream = &stream;
  status.type = STREAM_READER_TASK;
  status.id.taskId = 20;
  status.id.seriousId = 30;
  status.id.nodeId = 1;
  SStmTaskStatusMsg msg = makeStatusMessage(streamId, 20, 30);
  msg.type = STREAM_READER_TASK;
  ASSERT_EQ(registerTask(msg, &status), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsEntry initial = makeMetricEntry(0, streamId, 20, 30, 7);
  initial.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(initial.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot initialRecalc = {};
  initialRecalc.recalcId = 100;
  ASSERT_NE(taosArrayPush(initial.snapshot.pRecalculates, &initialRecalc), nullptr);
  ASSERT_EQ(mstApplyTaskMetrics(&status, 0, streamId, &initial), TSDB_CODE_SUCCESS);
  taosArrayDestroy(initial.snapshot.pRecalculates);
  initial.snapshot.pRecalculates = nullptr;
  SArray *oldRecalculates = status.metrics.pRecalculates;

  SStreamTaskMetricsEntry replacement = makeMetricEntry(0, streamId, 20, 30, 99);
  replacement.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(replacement.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot replacementRecalc = {};
  replacementRecalc.recalcId = 200;
  ASSERT_NE(taosArrayPush(replacement.snapshot.pRecalculates, &replacementRecalc), nullptr);

  SHashObj *actionStm = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  ASSERT_NE(actionStm, nullptr);
  taosHashSetFreeFp(actionStm, mstDestroySStmAction);
  SSdb                sdb = {};
  SMnode              mnode = {};
  const EStreamStatus abnormalStatuses[] = {
      STREAM_STATUS_STOPPED,
      STREAM_STATUS_FAILED,
      STREAM_STATUS_DROPPING,
      STREAM_STATUS_UNDEPLOYED,
  };
  mnode.pSdb = &sdb;

  for (EStreamStatus abnormalStatus : abnormalStatuses) {
    msg.status = abnormalStatus;
    SStreamHbMsg req = {};
    req.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
    req.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
    ASSERT_NE(req.pStreamStatus, nullptr);
    ASSERT_NE(taosArrayPush(req.pStreamStatus, &msg), nullptr);
    req.pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
    ASSERT_NE(req.pTaskMetrics, nullptr);
    ASSERT_NE(taosArrayPush(req.pTaskMetrics, &replacement), nullptr);

    SStmGrpCtx ctx = {};
    ctx.currTs = 100;
    ctx.pMnode = &mnode;
    ctx.pReq = &req;
    ctx.actionStm = actionStm;
    ctx.taskNum = 2;
    ASSERT_EQ(msmNormalHandleStatusUpdate(&ctx), TSDB_CODE_SUCCESS);

    EXPECT_EQ(status.status, abnormalStatus);
    EXPECT_EQ(status.lastUpTs, 100);
    EXPECT_TRUE(status.metricsValid);
    EXPECT_EQ(status.metrics.deliveredOutputRows1m, 7);
    EXPECT_EQ(status.metrics.pRecalculates, oldRecalculates);
    ASSERT_EQ(taosArrayGetSize(status.metrics.pRecalculates), 1);
    const auto *stored = static_cast<const SStreamRecalcSnapshot *>(taosArrayGet(status.metrics.pRecalculates, 0));
    ASSERT_NE(stored, nullptr);
    EXPECT_EQ(stored->recalcId, 100);

    SArray *metrics = req.pTaskMetrics;
    req.pTaskMetrics = nullptr;
    tCleanupStreamHbMsg(&req, true);
    taosArrayDestroy(metrics);
  }

  taosHashCleanup(actionStm);
  taosArrayDestroy(replacement.snapshot.pRecalculates);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, MetricEntryOutOfRangeLeavesTouchedMetricsInvalid) {
  constexpr int64_t streamId = 10;
  SStmStatus        stream = {};
  SStmTaskStatus    status = {};
  status.pStream = &stream;
  status.type = STREAM_RUNNER_TASK;
  status.id.taskId = 20;
  status.id.seriousId = 30;
  status.id.nodeId = 1;
  SStmTaskStatusMsg msg = makeStatusMessage(streamId, 20, 30);
  ASSERT_EQ(registerTask(msg, &status), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsEntry entry = makeMetricEntry(1, streamId, 20, 30);
  SArray                 *metrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  ASSERT_NE(metrics, nullptr);
  ASSERT_NE(taosArrayPush(metrics, &entry), nullptr);

  EXPECT_EQ(handleHeartbeat(&msg, STREAM_HB_OBSERVABILITY_VERSION_V1, metrics), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(status.metricsValid);
  taosArrayDestroy(metrics);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, ValidMetricEntryUpdatesMatchedTask) {
  constexpr int64_t streamId = 10;
  SStmStatus        stream = {};
  SStmTaskStatus    status = {};
  status.pStream = &stream;
  status.type = STREAM_RUNNER_TASK;
  status.id.taskId = 20;
  status.id.seriousId = 30;
  status.id.nodeId = 1;
  SStmTaskStatusMsg msg = makeStatusMessage(streamId, 20, 30);
  ASSERT_EQ(registerTask(msg, &status), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsEntry entry = makeMetricEntry(0, streamId, 20, 30, 123);
  SArray                 *metrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  ASSERT_NE(metrics, nullptr);
  ASSERT_NE(taosArrayPush(metrics, &entry), nullptr);

  EXPECT_EQ(handleHeartbeat(&msg, STREAM_HB_OBSERVABILITY_VERSION_V1, metrics), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(status.metricsValid);
  EXPECT_EQ(status.metrics.deliveredOutputRows1m, 123);
  taosArrayDestroy(metrics);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, InitHeartbeatAppliesMetricEntryAfterCoreStatusUpdate) {
  constexpr int64_t streamId = 10;
  StreamMapGuard    streamMap;
  SStmStatus        stream = {};
  stream.lastActionTs = INT64_MIN;
  ASSERT_EQ(taosHashPut(streamMap.get(), &streamId, sizeof(streamId), &stream, sizeof(stream)), TSDB_CODE_SUCCESS);

  SStmTaskStatus status = {};
  status.pStream = &stream;
  status.type = STREAM_RUNNER_TASK;
  status.id.taskId = 20;
  status.id.seriousId = 30;
  status.id.nodeId = 1;
  status.status = STREAM_STATUS_RUNNING;
  SStmTaskStatusMsg msg = makeStatusMessage(streamId, 20, 30);
  msg.status = STREAM_STATUS_INIT;
  ASSERT_EQ(registerTask(msg, &status), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsEntry entry = makeMetricEntry(0, streamId, 20, 30, 123);
  SArray                 *metrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  ASSERT_NE(metrics, nullptr);
  ASSERT_NE(taosArrayPush(metrics, &entry), nullptr);

  EXPECT_EQ(handleHeartbeat(&msg, STREAM_HB_OBSERVABILITY_VERSION_V1, metrics), TSDB_CODE_SUCCESS);
  EXPECT_EQ(status.status, STREAM_STATUS_INIT);
  EXPECT_EQ(status.lastUpTs, 100);
  ASSERT_TRUE(status.metricsValid);
  EXPECT_EQ(status.metrics.deliveredOutputRows1m, 123);
  taosArrayDestroy(metrics);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, InvalidInitMetricEntryInvalidatesStaleSnapshot) {
  constexpr int64_t streamId = 10;
  StreamMapGuard    streamMap;
  SStmStatus        stream = {};
  stream.lastActionTs = INT64_MIN;
  ASSERT_EQ(taosHashPut(streamMap.get(), &streamId, sizeof(streamId), &stream, sizeof(stream)), TSDB_CODE_SUCCESS);

  SStmTaskStatus status = {};
  status.pStream = &stream;
  status.type = STREAM_RUNNER_TASK;
  status.id.taskId = 20;
  status.id.seriousId = 30;
  status.id.nodeId = 1;
  status.status = STREAM_STATUS_RUNNING;
  SStmTaskStatusMsg msg = makeStatusMessage(streamId, 20, 30);
  msg.status = STREAM_STATUS_INIT;
  ASSERT_EQ(registerTask(msg, &status), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsEntry stale = makeMetricEntry(0, streamId, 20, 30, 7);
  ASSERT_EQ(mstApplyTaskMetrics(&status, 0, streamId, &stale), TSDB_CODE_SUCCESS);
  SStreamTaskMetricsEntry invalid = makeMetricEntry(0, streamId, 20, 30, 123);
  invalid.decodeCode = TSDB_CODE_INVALID_MSG;
  SArray *metrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  ASSERT_NE(metrics, nullptr);
  ASSERT_NE(taosArrayPush(metrics, &invalid), nullptr);

  EXPECT_EQ(handleHeartbeat(&msg, STREAM_HB_OBSERVABILITY_VERSION_V1, metrics), TSDB_CODE_SUCCESS);
  EXPECT_EQ(status.status, STREAM_STATUS_INIT);
  EXPECT_EQ(status.lastUpTs, 100);
  EXPECT_FALSE(status.metricsValid);
  taosArrayDestroy(metrics);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, MissingInitMetricEntryInvalidatesStaleSnapshot) {
  constexpr int64_t streamId = 10;
  StreamMapGuard    streamMap;
  SStmStatus        stream = {};
  stream.lastActionTs = INT64_MIN;
  ASSERT_EQ(taosHashPut(streamMap.get(), &streamId, sizeof(streamId), &stream, sizeof(stream)), TSDB_CODE_SUCCESS);

  SStmTaskStatus status = {};
  status.pStream = &stream;
  status.type = STREAM_RUNNER_TASK;
  status.id.taskId = 20;
  status.id.seriousId = 30;
  status.id.nodeId = 1;
  status.status = STREAM_STATUS_RUNNING;
  SStmTaskStatusMsg msg = makeStatusMessage(streamId, 20, 30);
  msg.status = STREAM_STATUS_INIT;
  ASSERT_EQ(registerTask(msg, &status), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsEntry stale = makeMetricEntry(0, streamId, 20, 30, 7);
  ASSERT_EQ(mstApplyTaskMetrics(&status, 0, streamId, &stale), TSDB_CODE_SUCCESS);
  SArray *metrics = taosArrayInit(0, sizeof(SStreamTaskMetricsEntry));
  ASSERT_NE(metrics, nullptr);

  EXPECT_EQ(handleHeartbeat(&msg, STREAM_HB_OBSERVABILITY_VERSION_V1, metrics), TSDB_CODE_SUCCESS);
  EXPECT_EQ(status.status, STREAM_STATUS_INIT);
  EXPECT_EQ(status.lastUpTs, 100);
  EXPECT_FALSE(status.metricsValid);
  taosArrayDestroy(metrics);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, MetricEntryRequiresIndexAndFullIdentity) {
  SStmTaskStatus status = {};
  status.id.taskId = 20;
  status.id.seriousId = 30;

  SStreamTaskMetricsEntry entry = makeMetricEntry(4, 10, 20, 30);
  EXPECT_EQ(mstApplyTaskMetrics(&status, 3, 10, &entry), TSDB_CODE_INVALID_MSG);
  EXPECT_FALSE(status.metricsValid);

  entry.taskStatusIndex = 4;
  entry.streamId = 11;
  EXPECT_EQ(mstApplyTaskMetrics(&status, 4, 10, &entry), TSDB_CODE_INVALID_MSG);
  EXPECT_FALSE(status.metricsValid);

  entry.streamId = 10;
  entry.taskId = 21;
  EXPECT_EQ(mstApplyTaskMetrics(&status, 4, 10, &entry), TSDB_CODE_INVALID_MSG);
  EXPECT_FALSE(status.metricsValid);

  entry.taskId = 20;
  entry.seriousId = 31;
  EXPECT_EQ(mstApplyTaskMetrics(&status, 4, 10, &entry), TSDB_CODE_INVALID_MSG);
  EXPECT_FALSE(status.metricsValid);

  entry.seriousId = 30;
  ASSERT_EQ(mstApplyTaskMetrics(&status, 4, 10, &entry), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(status.metricsValid);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, ReplacingSnapshotDeepCopiesRecalculations) {
  SStmTaskStatus status = {};
  status.id.taskId = 20;
  status.id.seriousId = 30;
  SStreamTaskMetricsEntry entry = makeMetricEntry(4, 10, 20, 30);
  entry.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(entry.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot recalc = {};
  recalc.recalcId = 100;
  recalc.progressPct = 5;
  recalc.status = STREAM_RECALC_STATUS_RUNNING;
  ASSERT_NE(taosArrayPush(entry.snapshot.pRecalculates, &recalc), nullptr);

  ASSERT_EQ(mstApplyTaskMetrics(&status, 4, 10, &entry), TSDB_CODE_SUCCESS);
  ASSERT_NE(status.metrics.pRecalculates, entry.snapshot.pRecalculates);
  taosArrayDestroy(entry.snapshot.pRecalculates);
  entry.snapshot.pRecalculates = nullptr;

  ASSERT_TRUE(status.metricsValid);
  ASSERT_EQ(taosArrayGetSize(status.metrics.pRecalculates), 1);
  const auto *stored = static_cast<const SStreamRecalcSnapshot *>(taosArrayGet(status.metrics.pRecalculates, 0));
  ASSERT_NE(stored, nullptr);
  EXPECT_EQ(stored->recalcId, 100);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, SnapshotCopyOutOfMemoryPreservesOldSnapshot) {
  SStmTaskStatus             status = {};
  SStreamTaskMetricsSnapshot initial = {};
  initial.deliveredOutputRows1m = 7;
  initial.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(initial.pRecalculates, nullptr);
  SStreamRecalcSnapshot initialRecalc = {};
  initialRecalc.recalcId = 100;
  ASSERT_NE(taosArrayPush(initial.pRecalculates, &initialRecalc), nullptr);
  ASSERT_EQ(mstCopyTaskMetrics(&status, &initial), TSDB_CODE_SUCCESS);
  taosArrayDestroy(initial.pRecalculates);
  initial.pRecalculates = nullptr;
  SArray *oldRecalculates = status.metrics.pRecalculates;

  SStreamTaskMetricsSnapshot replacement = {};
  replacement.deliveredOutputRows1m = 99;
  replacement.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(replacement.pRecalculates, nullptr);
  SStreamRecalcSnapshot replacementRecalc = {};
  replacementRecalc.recalcId = 200;
  ASSERT_NE(taosArrayPush(replacement.pRecalculates, &replacementRecalc), nullptr);

  int32_t code = TSDB_CODE_SUCCESS;
  terrno = TSDB_CODE_SUCCESS;
  {
    TaosArrayDupFailureGuard guard;
    code = mstCopyTaskMetrics(&status, &replacement);
  }

  EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(terrno, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(status.metricsValid);
  EXPECT_EQ(status.metrics.deliveredOutputRows1m, 7);
  EXPECT_EQ(status.metrics.pRecalculates, oldRecalculates);
  ASSERT_EQ(taosArrayGetSize(status.metrics.pRecalculates), 1);
  const auto *stored = static_cast<const SStreamRecalcSnapshot *>(taosArrayGet(status.metrics.pRecalculates, 0));
  ASSERT_NE(stored, nullptr);
  EXPECT_EQ(stored->recalcId, 100);

  taosArrayDestroy(replacement.pRecalculates);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, StaleSeriousIdHeartbeatPreservesCurrentSnapshot) {
  constexpr int64_t streamId = 10;
  SStmStatus        stream = {};
  SStmTaskStatus    status = {};
  status.pStream = &stream;
  status.type = STREAM_RUNNER_TASK;
  status.id.taskId = 20;
  status.id.seriousId = 31;
  status.id.nodeId = 1;
  status.status = STREAM_STATUS_RUNNING;
  status.lastUpTs = 99;
  SStmTaskStatusMsg current = makeStatusMessage(streamId, 20, 31);
  ASSERT_EQ(registerTask(current, &status), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsEntry initial = makeMetricEntry(0, streamId, 20, 31, 7);
  initial.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(initial.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot recalc = {};
  recalc.recalcId = 100;
  ASSERT_NE(taosArrayPush(initial.snapshot.pRecalculates, &recalc), nullptr);
  ASSERT_EQ(mstApplyTaskMetrics(&status, 0, streamId, &initial), TSDB_CODE_SUCCESS);
  taosArrayDestroy(initial.snapshot.pRecalculates);
  initial.snapshot.pRecalculates = nullptr;
  SArray *oldRecalculates = status.metrics.pRecalculates;

  SStmTaskStatusMsg stale = makeStatusMessage(streamId, 20, 30);
  SStreamHbMsg      req = {};
  req.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(req.pStreamStatus, nullptr);
  ASSERT_NE(taosArrayPush(req.pStreamStatus, &stale), nullptr);
  SHashObj *actionStm = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  ASSERT_NE(actionStm, nullptr);
  taosHashSetFreeFp(actionStm, mstDestroySStmAction);
  SSdb       sdb = {};
  SMnode     mnode = {};
  SStmGrpCtx ctx = {};
  mnode.pSdb = &sdb;
  ctx.currTs = 100;
  ctx.pMnode = &mnode;
  ctx.pReq = &req;
  ctx.actionStm = actionStm;
  ctx.taskNum = 1;

  ASSERT_EQ(msmNormalHandleStatusUpdate(&ctx), TSDB_CODE_SUCCESS);
  EXPECT_EQ(status.id.seriousId, 31);
  EXPECT_EQ(status.status, STREAM_STATUS_RUNNING);
  EXPECT_EQ(status.lastUpTs, 99);
  EXPECT_EQ(taosHashGetSize(actionStm), 1);
  EXPECT_TRUE(status.metricsValid);
  EXPECT_EQ(status.metrics.deliveredOutputRows1m, 7);
  EXPECT_EQ(status.metrics.pRecalculates, oldRecalculates);
  if (status.metrics.pRecalculates != nullptr) {
    EXPECT_EQ(taosArrayGetSize(status.metrics.pRecalculates), 1);
    const auto *stored = static_cast<const SStreamRecalcSnapshot *>(taosArrayGet(status.metrics.pRecalculates, 0));
    ASSERT_NE(stored, nullptr);
    EXPECT_EQ(stored->recalcId, 100);
  }

  taosHashCleanup(actionStm);
  tCleanupStreamHbMsg(&req, true);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, BumpingSeriousIdClearsOwnedSnapshot) {
  SStmTaskStatus status = {};
  status.id.taskId = 20;
  status.id.seriousId = 30;
  SStreamTaskMetricsEntry initial = makeMetricEntry(0, 10, 20, 30, 7);
  initial.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(initial.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot recalc = {};
  recalc.recalcId = 100;
  ASSERT_NE(taosArrayPush(initial.snapshot.pRecalculates, &recalc), nullptr);
  ASSERT_EQ(mstApplyTaskMetrics(&status, 0, 10, &initial), TSDB_CODE_SUCCESS);
  taosArrayDestroy(initial.snapshot.pRecalculates);
  initial.snapshot.pRecalculates = nullptr;

  EXPECT_EQ(mstBumpTaskSeriousId(&status), 31);
  EXPECT_EQ(status.id.seriousId, 31);
  EXPECT_FALSE(status.metricsValid);
  EXPECT_EQ(status.metrics.deliveredOutputRows1m, 0);
  EXPECT_EQ(status.metrics.pRecalculates, nullptr);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, ReplacingTriggerRuntimeStatusDeepCopiesUserRecalculations) {
  SStmTaskStatus         status = {};
  SSTriggerRuntimeStatus input = {};
  input.autoRecalcNum = 1;
  input.userRecalcs = taosArrayInit(1, sizeof(SSTriggerRecalcProgress));
  ASSERT_NE(input.userRecalcs, nullptr);
  SSTriggerRecalcProgress first = {};
  first.recalcId = 100;
  first.progress = 5;
  first.start = 10;
  first.end = 20;
  ASSERT_NE(taosArrayPush(input.userRecalcs, &first), nullptr);

  ASSERT_EQ(mstCopyTriggerRuntimeStatus(&status, &input), TSDB_CODE_SUCCESS);
  taosArrayDestroy(input.userRecalcs);
  input.userRecalcs = nullptr;

  auto *stored = static_cast<SSTriggerRuntimeStatus *>(status.detailStatus);
  ASSERT_NE(stored, nullptr);
  ASSERT_NE(stored->userRecalcs, nullptr);
  ASSERT_EQ(taosArrayGetSize(stored->userRecalcs), 1);
  const auto *storedFirst = static_cast<const SSTriggerRecalcProgress *>(taosArrayGet(stored->userRecalcs, 0));
  ASSERT_NE(storedFirst, nullptr);
  EXPECT_EQ(storedFirst->recalcId, 100);

  SSTriggerRuntimeStatus replacement = {};
  replacement.autoRecalcNum = 2;
  replacement.userRecalcs = taosArrayInit(1, sizeof(SSTriggerRecalcProgress));
  ASSERT_NE(replacement.userRecalcs, nullptr);
  SSTriggerRecalcProgress second = {};
  second.recalcId = 200;
  second.progress = 10;
  second.start = 30;
  second.end = 40;
  ASSERT_NE(taosArrayPush(replacement.userRecalcs, &second), nullptr);
  ASSERT_EQ(mstCopyTriggerRuntimeStatus(&status, &replacement), TSDB_CODE_SUCCESS);
  taosArrayDestroy(replacement.userRecalcs);
  replacement.userRecalcs = nullptr;

  stored = static_cast<SSTriggerRuntimeStatus *>(status.detailStatus);
  ASSERT_NE(stored, nullptr);
  EXPECT_EQ(stored->autoRecalcNum, 2);
  ASSERT_EQ(taosArrayGetSize(stored->userRecalcs), 1);
  const auto *storedSecond = static_cast<const SSTriggerRecalcProgress *>(taosArrayGet(stored->userRecalcs, 0));
  ASSERT_NE(storedSecond, nullptr);
  EXPECT_EQ(storedSecond->recalcId, 200);
  mstDestroySStmTaskStatus(&status);
}

TEST_F(MndStreamTest, TriggerStatusCopyOutOfMemoryPreservesOldSnapshot) {
  SStmTaskStatus         status = {};
  SSTriggerRuntimeStatus initial = {};
  initial.autoRecalcNum = 1;
  initial.userRecalcs = taosArrayInit(1, sizeof(SSTriggerRecalcProgress));
  ASSERT_NE(initial.userRecalcs, nullptr);
  SSTriggerRecalcProgress initialRecalc = {};
  initialRecalc.recalcId = 100;
  ASSERT_NE(taosArrayPush(initial.userRecalcs, &initialRecalc), nullptr);
  ASSERT_EQ(mstCopyTriggerRuntimeStatus(&status, &initial), TSDB_CODE_SUCCESS);
  taosArrayDestroy(initial.userRecalcs);
  initial.userRecalcs = nullptr;

  auto                  *oldStatus = static_cast<SSTriggerRuntimeStatus *>(status.detailStatus);
  SArray                *oldRecalculates = oldStatus->userRecalcs;
  SSTriggerRuntimeStatus replacement = {};
  replacement.autoRecalcNum = 2;
  replacement.userRecalcs = taosArrayInit(1, sizeof(SSTriggerRecalcProgress));
  ASSERT_NE(replacement.userRecalcs, nullptr);
  SSTriggerRecalcProgress replacementRecalc = {};
  replacementRecalc.recalcId = 200;
  ASSERT_NE(taosArrayPush(replacement.userRecalcs, &replacementRecalc), nullptr);

  int32_t code = TSDB_CODE_SUCCESS;
  terrno = TSDB_CODE_SUCCESS;
  {
    TaosArrayDupFailureGuard guard;
    code = mstCopyTriggerRuntimeStatus(&status, &replacement);
  }

  EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(terrno, TSDB_CODE_SUCCESS);
  EXPECT_EQ(status.detailStatus, oldStatus);
  auto *stored = static_cast<SSTriggerRuntimeStatus *>(status.detailStatus);
  ASSERT_NE(stored, nullptr);
  EXPECT_EQ(stored->autoRecalcNum, 1);
  EXPECT_EQ(stored->userRecalcs, oldRecalculates);
  ASSERT_EQ(taosArrayGetSize(stored->userRecalcs), 1);
  const auto *storedRecalc = static_cast<const SSTriggerRecalcProgress *>(taosArrayGet(stored->userRecalcs, 0));
  ASSERT_NE(storedRecalc, nullptr);
  EXPECT_EQ(storedRecalc->recalcId, 100);

  taosArrayDestroy(replacement.userRecalcs);
  mstDestroySStmTaskStatus(&status);
}

TEST_F(MndStreamTest, TaskRemovalCleansMetricsAndTriggerRuntimeStatus) {
  SStmTaskStatus status = {};
  status.extraErrMsg = taosStrdup("error");
  ASSERT_NE(status.extraErrMsg, nullptr);
  status.metricsValid = true;
  status.metrics.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(status.metrics.pRecalculates, nullptr);
  status.detailStatus = taosMemoryCalloc(1, sizeof(SSTriggerRuntimeStatus));
  ASSERT_NE(status.detailStatus, nullptr);
  auto *trigger = static_cast<SSTriggerRuntimeStatus *>(status.detailStatus);
  trigger->userRecalcs = taosArrayInit(1, sizeof(SSTriggerRecalcProgress));
  ASSERT_NE(trigger->userRecalcs, nullptr);

  mstDestroySStmTaskStatus(&status);
  EXPECT_EQ(status.extraErrMsg, nullptr);
  EXPECT_FALSE(status.metricsValid);
  EXPECT_EQ(status.metrics.pRecalculates, nullptr);
  EXPECT_EQ(status.detailStatus, nullptr);
  mstDestroySStmTaskStatus(&status);
}

TEST(MndStreamTransTest, AppendFailureDoesNotDropCallerOwnedTrans) {
  STrans *pTrans = createTestTrans();
  ASSERT_NE(pTrans, nullptr);

  char streamName[] = "test.stream";
  char streamDb[] = "test";
  char outTblName[] = "out";

  SCMCreateStreamReq createReq = {0};
  createReq.name = streamName;
  createReq.streamId = 1;
  createReq.streamDB = streamDb;
  createReq.outTblName = outTblName;

  SStreamObj stream = {0};
  tstrncpy(stream.name, "test.stream", sizeof(stream.name));
  stream.pCreate = &createReq;

  int32_t code = mndStreamTransAppend(&stream, pTrans, SDB_STATUS_INIT);
  ASSERT_NE(code, TSDB_CODE_SUCCESS);

  mndTransDrop(pTrans);
}

TEST(MndStreamWatchTest, InfersRunnerDeploysFromObservedIds) {
  SStmStatus status = {0};
  status.runnerNum = 2;
  status.runnerReplica = 20;

  ASSERT_EQ(mstRestoreRunnerDeploy(&status, 2), TSDB_CODE_SUCCESS);
  EXPECT_EQ(status.runnerDeploys, 3);
  EXPECT_EQ(status.runnerReplica, MND_STREAM_RUNNER_REPLICA_UNKNOWN);
  EXPECT_EQ(status.runners[0], nullptr);
  ASSERT_NE(status.runners[2], nullptr);
  EXPECT_EQ(taosArrayGetSize(status.runners[2]), 0);

  ASSERT_EQ(mstRestoreRunnerDeploy(&status, 0), TSDB_CODE_SUCCESS);
  EXPECT_EQ(status.runnerDeploys, 3);
  ASSERT_NE(status.runners[0], nullptr);

  ASSERT_EQ(mstRestoreRunnerDeploy(&status, 7), TSDB_CODE_SUCCESS);
  EXPECT_EQ(status.runnerDeploys, 8);
  ASSERT_NE(status.runners[7], nullptr);

  destroyRecoveredRunnerLists(&status);
}

TEST(MndStreamWatchTest, RejectsRunnerDeployIdsOutsideTheStaticCap) {
  SStmStatus status = {0};
  status.runnerNum = 1;

  EXPECT_EQ(mstRestoreRunnerDeploy(&status, -1), TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(mstRestoreRunnerDeploy(&status, MND_STREAM_RUNNER_DEPLOY_NUM), TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(status.runnerDeploys, 0);
}

TEST(MndStreamWatchTest, TaskPolicyOnlyRefreshesSnapshotConsumers) {
  SStmStatus status = {0};
  status.runnerNum = 1;
  status.runnerReplica = MND_STREAM_RUNNER_REPLICA_UNKNOWN;

  SStmTaskAction action = {0};
  action.type = STREAM_RUNNER_TASK;
  EXPECT_TRUE(mstTaskDeployNeedsRunnerSnapshot(&status, &action));

  action.type = STREAM_READER_TASK;
  action.flag = 0;
  EXPECT_TRUE(mstTaskDeployNeedsRunnerSnapshot(&status, &action));

  action.flag = STREAM_FLAG_TRIGGER_READER;
  EXPECT_FALSE(mstTaskDeployNeedsRunnerSnapshot(&status, &action));

  status.runnerReplica = 5;
  action.type = STREAM_RUNNER_TASK;
  action.flag = 0;
  EXPECT_FALSE(mstTaskDeployNeedsRunnerSnapshot(&status, &action));
}

TEST(MndStreamWatchTest, RunnerSnapshotClaimDistinguishesStates) {
  SStmStatus status = {0};
  status.runnerNum = 1;
  atomic_store_32(&status.runnerReplica, MND_STREAM_RUNNER_REPLICA_UNKNOWN);

  EXPECT_EQ(mstClaimRunnerSnapshotRedeploy(&status), MST_RUNNER_SNAPSHOT_CLAIM_ACQUIRED);
  EXPECT_EQ(atomic_load_8(&status.runnerSnapshotRedeployPending), 1);

  EXPECT_EQ(mstClaimRunnerSnapshotRedeploy(&status), MST_RUNNER_SNAPSHOT_CLAIM_ALREADY_PENDING);
  EXPECT_EQ(atomic_load_8(&status.runnerSnapshotRedeployPending), 1);

  atomic_store_8(&status.runnerSnapshotRedeployPending, 0);
  atomic_store_32(&status.runnerReplica, 5);
  EXPECT_EQ(mstClaimRunnerSnapshotRedeploy(&status), MST_RUNNER_SNAPSHOT_CLAIM_KNOWN);
  EXPECT_EQ(atomic_load_8(&status.runnerSnapshotRedeployPending), 0);
}

TEST_F(MndStreamActionQueueTest, RepeatedUnknownConsumersPostOneOwnedFullDeploy) {
  char       streamName[] = "test.runner_snapshot";
  SStmStatus status = {0};
  status.streamName = streamName;
  status.runnerNum = 1;
  status.runnerReplica = MND_STREAM_RUNNER_REPLICA_UNKNOWN;

  ASSERT_EQ(mstPostRunnerSnapshotRedeploy(&queue_, 42, &status), TSDB_CODE_SUCCESS);
  ASSERT_EQ(mstPostRunnerSnapshotRedeploy(&queue_, 42, &status), TSDB_CODE_SUCCESS);
  EXPECT_EQ(atomic_load_64(reinterpret_cast<volatile int64_t *>(&queue_.qRemainNum)), 1);
  EXPECT_EQ(atomic_load_8(&status.runnerSnapshotRedeployPending), 1);

  SStmQNode *pNode = nullptr;
  ASSERT_TRUE(mndStreamActionDequeue(&queue_, &pNode));
  ASSERT_NE(pNode, nullptr);
  EXPECT_EQ(pNode->type, STREAM_ACT_DEPLOY);
  EXPECT_TRUE(pNode->streamAct);
  EXPECT_EQ(pNode->action.stream.streamId, 42);
  EXPECT_STREQ(pNode->action.stream.streamName, streamName);
  EXPECT_TRUE(pNode->action.stream.runnerSnapshotRedeployOwner);
  EXPECT_EQ(atomic_load_64(reinterpret_cast<volatile int64_t *>(&queue_.qRemainNum)), 0);
}

TEST_F(MndStreamActionQueueTest, KnownSnapshotDoesNotPostFullDeploy) {
  char       streamName[] = "test.runner_snapshot";
  SStmStatus status = {0};
  status.streamName = streamName;
  status.runnerNum = 1;
  status.runnerReplica = 5;

  EXPECT_EQ(mstPostRunnerSnapshotRedeploy(&queue_, 42, &status), TSDB_CODE_SUCCESS);
  EXPECT_EQ(atomic_load_64(reinterpret_cast<volatile int64_t *>(&queue_.qRemainNum)), 0);
  EXPECT_EQ(atomic_load_8(&status.runnerSnapshotRedeployPending), 0);
}

TEST_F(MndStreamActionQueueTest, NormalStreamActionDoesNotOwnRunnerSnapshotLatch) {
  char streamName[] = "test.normal_deploy";

  ASSERT_EQ(mstPostStreamAction(&queue_, 42, streamName, nullptr, false, STREAM_ACT_DEPLOY), TSDB_CODE_SUCCESS);

  SStmQNode *pNode = nullptr;
  ASSERT_TRUE(mndStreamActionDequeue(&queue_, &pNode));
  ASSERT_NE(pNode, nullptr);
  EXPECT_FALSE(pNode->action.stream.runnerSnapshotRedeployOwner);
}

TEST(MndStreamWatchTest, CalcReaderBuilderRejectsUnknownReplica) {
  SStmStatus status = {0};
  status.runnerNum = 1;
  status.runnerReplica = MND_STREAM_RUNNER_REPLICA_UNKNOWN;
  SStmTaskDeploy deploy = {};

  EXPECT_EQ(msmBuildReaderDeployInfo(&deploy, nullptr, &status, false), TSDB_CODE_MND_STREAM_INTERNAL_ERROR);

  status.runnerDeploys = 3;
  status.runnerReplica = 5;
  ASSERT_EQ(msmBuildReaderDeployInfo(&deploy, nullptr, &status, false), TSDB_CODE_SUCCESS);
  EXPECT_EQ(deploy.msg.reader.msg.calc.execReplica, 15);
}

TEST(MndStreamWatchTest, TriggerTargetBuilderRejectsUnknownReplica) {
  SStmStatus status = {0};
  status.runnerNum = 1;
  status.runnerReplica = MND_STREAM_RUNNER_REPLICA_UNKNOWN;
  SArray *targets = nullptr;

  EXPECT_EQ(msmBuildTriggerRunnerTargets(nullptr, &status, 42, &targets), TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(targets, nullptr);
}

TEST(MndStreamWatchTest, TriggerTargetBuilderClearsOutputOnFailure) {
  SStmStatus status = {0};
  status.runnerNum = 1;
  status.runnerDeploys = 1;
  status.runnerReplica = 5;
  status.runners[0] = taosArrayInit(1, sizeof(SStmTaskStatus));
  ASSERT_NE(status.runners[0], nullptr);

  SArray *targets = nullptr;
  EXPECT_EQ(msmBuildTriggerRunnerTargets(nullptr, &status, 42, &targets), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(targets, nullptr);

  taosArrayDestroy(targets);
  taosArrayDestroy(status.runners[0]);
}

TEST(MndStreamWatchTest, RunnerBuilderRejectsUnknownReplica) {
  SStmStatus status = {0};
  status.runnerNum = 1;
  status.runnerReplica = MND_STREAM_RUNNER_REPLICA_UNKNOWN;
  SStmTaskDeploy deploy = {};

  EXPECT_EQ(msmBuildRunnerDeployInfo(&deploy, nullptr, nullptr, &status, false), TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
}

TEST(MndStreamWatchTest, TriggerDeployBuilderPropagatesRunnerTargetError) {
  SCMCreateStreamReq createReq = {0};
  createReq.streamId = 42;

  SSdb   sdb = {0};
  SMnode mnode = {0};
  mnode.pSdb = &sdb;

  SStmStatus status = {0};
  status.pCreate = &createReq;
  status.runnerNum = 1;
  atomic_store_32(&status.runnerReplica, MND_STREAM_RUNNER_REPLICA_UNKNOWN);
  status.trigReaders = taosArrayInit(1, sizeof(SStmTaskStatus));
  ASSERT_NE(status.trigReaders, nullptr);

  SStmTaskStatus reader = {0};
  reader.id.nodeId = 1;
  ASSERT_NE(taosArrayPush(status.trigReaders, &reader), nullptr);

  SStreamObj stream = {0};
  stream.pCreate = &createReq;

  SStmTaskDeploy deploy = {};
  EXPECT_EQ(msmBuildTriggerDeployInfo(&mnode, &status, &deploy, &stream), TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(deploy.msg.trigger.readerList, nullptr);
  EXPECT_EQ(deploy.msg.trigger.runnerList, nullptr);

  taosArrayDestroy(deploy.msg.trigger.readerList);
  taosArrayDestroy(deploy.msg.trigger.runnerList);
  taosArrayDestroy(status.trigReaders);
}

TEST_F(MndStreamHeartbeatTest, SnapshotDeployWriterWaitsForPostingHeartbeatReader) {
  constexpr int64_t streamId = 42;
  char              streamName[] = "test.runner_snapshot";

  SStmTaskStatus triggerTask = {0};
  triggerTask.id.taskId = 100;
  triggerTask.id.seriousId = 200;
  triggerTask.id.nodeId = 1;
  triggerTask.id.taskIdx = 0;

  SStmStatus status = {0};
  status.streamName = streamName;
  status.runnerNum = 1;
  status.triggerTask = &triggerTask;
  atomic_store_8(&status.stopped, 2);
  atomic_store_32(&status.runnerReplica, MND_STREAM_RUNNER_REPLICA_UNKNOWN);
  ASSERT_EQ(taosHashPut(mStreamMgmt.streamMap, &streamId, sizeof(streamId), &status, sizeof(status)),
            TSDB_CODE_SUCCESS);
  SStmStatus *pStatus = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId)));
  ASSERT_NE(pStatus, nullptr);

  SStreamRecalcReq recalcReq = {0};
  SStmAction       action = {0};
  action.actions = STREAM_ACT_UPDATE_TRIGGER | STREAM_ACT_RECALC | STREAM_ACT_START;
  action.recalc.recalcList = taosArrayInit(1, sizeof(SStreamRecalcReq));
  ASSERT_NE(action.recalc.recalcList, nullptr);
  ASSERT_NE(taosArrayPush(action.recalc.recalcList, &recalcReq), nullptr);
  action.start.triggerId = triggerTask.id;

  SHashObj *actionStm = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  ASSERT_NE(actionStm, nullptr);
  taosHashSetFreeFp(actionStm, mstDestroySStmAction);
  ASSERT_EQ(taosHashPut(actionStm, &streamId, sizeof(streamId), &action, sizeof(action)), TSDB_CODE_SUCCESS);

  SHashObj *deployStm = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  ASSERT_NE(deployStm, nullptr);

  constexpr int32_t dnodeId = 1;
  int64_t           lastUpTs = 0;
  ASSERT_EQ(taosHashPut(mStreamMgmt.dnodeMap, &dnodeId, sizeof(dnodeId), &lastUpTs, sizeof(lastUpTs)),
            TSDB_CODE_SUCCESS);

  SStreamHbMsg req = {0};
  req.dnodeId = dnodeId;
  SMStreamHbRspMsg rsp = {0};
  SStmGrpCtx       ctx = {0};
  ctx.currTs = 1;
  ctx.pReq = &req;
  ctx.pRsp = &rsp;
  ctx.actionStm = actionStm;
  ctx.deployStm = deployStm;

  std::atomic<int32_t> heartbeatCode{TSDB_CODE_SUCCESS};
  std::atomic<bool>    heartbeatFinished{false};

  SStmQNode *pNoopAction = static_cast<SStmQNode *>(taosMemoryCalloc(1, sizeof(SStmQNode)));
  ASSERT_NE(pNoopAction, nullptr);
  SHashObj *consumerActionStm = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  ASSERT_NE(consumerActionStm, nullptr);
  SHashObj *consumerDeployStm = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  ASSERT_NE(consumerDeployStm, nullptr);

  taosWLockLatch(&queue_.lock);
  std::thread heartbeat([&]() {
    heartbeatCode.store(msmNormalHandleHbMsg(&ctx));
    heartbeatFinished.store(true);
  });

  bool       posted = false;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < deadline) {
    if (1 == atomic_load_8(&pStatus->runnerSnapshotRedeployPending)) {
      posted = true;
      break;
    }
    if (heartbeatFinished.load()) {
      break;
    }
    std::this_thread::yield();
  }

  pNoopAction->next = nullptr;
  queue_.tail->next = pNoopAction;
  queue_.tail = pNoopAction;
  (void)atomic_add_fetch_64(reinterpret_cast<volatile int64_t *>(&queue_.qRemainNum), 1);

  SStreamHbMsg consumerReq = {0};
  consumerReq.dnodeId = dnodeId;
  SMStreamHbRspMsg consumerRsp = {0};
  SStmGrpCtx       consumerCtx = {0};
  consumerCtx.currTs = 2;
  consumerCtx.pReq = &consumerReq;
  consumerCtx.pRsp = &consumerRsp;
  consumerCtx.actionStm = consumerActionStm;
  consumerCtx.deployStm = consumerDeployStm;

  std::atomic<int32_t> consumerCode{TSDB_CODE_SUCCESS};
  std::atomic<bool>    consumerFinished{false};
  std::thread          consumer([&]() {
    consumerCode.store(msmNormalHandleHbMsg(&consumerCtx));
    consumerFinished.store(true);
  });

  bool       writerWaitedForReader = false;
  const auto writerDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < writerDeadline) {
    if (taosHasRWWFlag(&mStreamMgmt.actionQLock)) {
      writerWaitedForReader = !taosIsOnlyWLocked(&mStreamMgmt.actionQLock) && !consumerFinished.load();
      break;
    }
    if (consumerFinished.load()) {
      break;
    }
    std::this_thread::yield();
  }

  taosWUnLockLatch(&queue_.lock);
  heartbeat.join();
  consumer.join();

  EXPECT_TRUE(posted);
  EXPECT_TRUE(writerWaitedForReader);
  EXPECT_EQ(heartbeatCode.load(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(consumerCode.load(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(atomic_load_64(reinterpret_cast<volatile int64_t *>(&queue_.qRemainNum)), 0);
  ASSERT_NE(rsp.rsps.rspList, nullptr);
  ASSERT_EQ(taosArrayGetSize(rsp.rsps.rspList), 1);
  SStreamMgmtRsp *pRecalcRsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(rsp.rsps.rspList, 0));
  ASSERT_NE(pRecalcRsp, nullptr);
  EXPECT_EQ(pRecalcRsp->header.msgType, STREAM_MSG_USER_RECALC);
  EXPECT_EQ(taosArrayGetSize(pRecalcRsp->cont.recalcList), 1);
  ASSERT_NE(rsp.start.taskList, nullptr);
  ASSERT_EQ(taosArrayGetSize(rsp.start.taskList), 1);
  SStreamTaskStart *pStart = static_cast<SStreamTaskStart *>(taosArrayGet(rsp.start.taskList, 0));
  ASSERT_NE(pStart, nullptr);
  EXPECT_EQ(pStart->task.taskId, triggerTask.id.taskId);

  tFreeSMStreamHbRspMsg(&rsp);
  tFreeSMStreamHbRspMsg(&consumerRsp);
  taosHashCleanup(consumerDeployStm);
  taosHashCleanup(consumerActionStm);
  taosHashCleanup(deployStm);
  taosHashCleanup(actionStm);
}

// clang-format off
/*

namespace {

static int64_t defStreamId = 999;

SRpcMsg buildHbReq() {
  SStreamHbMsg msg = {0};
  msg.vgId = 1;
  msg.numOfTasks = 5;


  int32_t  tlen = 0;
  int32_t  code = 0;
  SEncoder encoder;
  void*    buf = NULL;
  SRpcMsg  msg1 = {0};
  msg1.info.noResp = 1;

  tEncodeSize(tEncodeStreamHbMsg, &msg, tlen, code);
  if (code < 0) {
    goto _end;
  }

  buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    goto _end;
  }

  tEncoderInit(&encoder, (uint8_t*)buf, tlen);
  if ((code = tEncodeStreamHbMsg(&encoder, &msg)) < 0) {
    rpcFreeCont(buf);
    tEncoderClear(&encoder);
    goto _end;
  }
  tEncoderClear(&encoder);

  {
    msg1.msgType = TDMT_MND_STREAM_HEARTBEAT;
    msg1.pCont = buf;
    msg1.contLen = tlen;
  }

  taosArrayDestroy(msg.pTaskStatus);
  return msg1;

_end:
  return msg1;
}

void setTask(SStreamTask* pTask, int32_t nodeId, int64_t streamId, int32_t taskId) {
  SStreamExecInfo* pExecNode = &execInfo;

  pTask->id.streamId = streamId;
  pTask->id.taskId = taskId;
  pTask->info.nodeId = nodeId;
}

void initStreamExecInfo() {
  SStreamExecInfo* pExecNode = &execInfo;

  SStreamTask task = {0};
  setTask(&task, 1, defStreamId, 1);
  setTask(&task, 1, defStreamId, 2);
  setTask(&task, 1, defStreamId, 3);
  setTask(&task, 1, defStreamId, 4);
  setTask(&task, 2, defStreamId, 5);
}

void initNodeInfo() {
  SNodeEntry entry = {0};
  entry.nodeId = 2;
  entry.stageUpdated = true;
  void* px = taosArrayPush(execInfo.pNodeList, &entry);
  ASSERT(px != NULL);
}
}  // namespace

class StreamTest : public testing::Test { // 继承了 testing::Test
 protected:

  static void SetUpTestSuite() {
    int32_t code = mndInitExecInfo();
    ASSERT(code == 0);

    initStreamExecInfo();
    initNodeInfo();

    (void) printf("setup env for streamTest suite");
  }

  static void TearDownTestSuite() {
    (void) printf("tearDown env for streamTest suite");
  }

  virtual void SetUp() override {
  }

  virtual void TearDown() override {
  }
};

TEST_F(StreamTest, handle_error_in_hb) {
  SRpcMsg msg = buildHbReq();
  int32_t code = mndProcessStreamHb(&msg);

  rpcFreeCont(msg.pCont);
}

TEST_F(StreamTest, plan_Test) {
  char* ast = "{\"NodeType\":\"101\",\"Name\":\"SelectStmt\",\"SelectStmt\":{\"Distinct\":false,\"Projections\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_1\",\"UserAlias\":\"_wstart\",\"Name\":\"_wstart\",\"Id\":\"89\",\"Type\":\"3505\",\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_2\",\"UserAlias\":\"sum(voltage)\",\"Name\":\"sum\",\"Id\":\"1\",\"Type\":\"14\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"voltage\",\"UserAlias\":\"voltage\",\"TableId\":\"6555383776122680534\",\"TableType\":\"1\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"meters\",\"TableAlias\":\"meters\",\"ColName\":\"voltage\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"#expr_3\",\"UserAlias\":\"groupid\",\"Name\":\"_group_key\",\"Id\":\"96\",\"Type\":\"3754\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"#expr_3\",\"UserAlias\":\"groupid\",\"TableId\":\"6555383776122680534\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"test\",\"TableName\":\"meters\",\"TableAlias\":\"meters\",\"ColName\":\"groupid\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}}],\"From\":{\"NodeType\":\"6\",\"Name\":\"RealTable\",\"RealTable\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"DbName\":\"test\",\"tableName\":\"meters\",\"tableAlias\":\"meters\",\"MetaSize\":\"475\",\"Meta\":{\"VgId\":\"0\",\"TableType\":\"1\",\"Uid\":\"6555383776122680534\",\"Suid\":\"6555383776122680534\",\"Sversion\":\"1\",\"Tversion\":\"1\",\"ComInfo\":{\"NumOfTags\":\"2\",\"Precision\":\"0\",\"NumOfColumns\":\"4\",\"RowSize\":\"20\"},\"ColSchemas\":[{\"Type\":\"9\",\"ColId\":\"1\",\"bytes\":\"8\",\"Name\":\"ts\"},{\"Type\":\"6\",\"ColId\":\"2\",\"bytes\":\"4\",\"Name\":\"current\"},{\"Type\":\"4\",\"ColId\":\"3\",\"bytes\":\"4\",\"Name\":\"voltage\"},{\"Type\":\"6\",\"ColId\":\"4\",\"bytes\":\"4\",\"Name\":\"phase\"},{\"Type\":\"4\",\"ColId\":\"5\",\"bytes\":\"4\",\"Name\":\"groupid\"},{\"Type\":\"8\",\"ColId\":\"6\",\"bytes\":\"26\",\"Name\":\"location\"}]},\"VgroupsInfoSize\":\"1340\",\"VgroupsInfo\":{\"Num\":\"2\",\"Vgroups\":[{\"VgId\":\"2\",\"HashBegin\":\"0\",\"HashEnd\":\"2147483646\",\"EpSet\":{\"InUse\":\"0\",\"NumOfEps\":\"1\",\"Eps\":[{\"Fqdn\":\"localhost\",\"Port\":\"6030\"}]},\"NumOfTable\":\"0\"},{\"VgId\":\"3\",\"HashBegin\":\"2147483647\",\"HashEnd\":\"4294967295\",\"EpSet\":{\"InUse\":\"0\",\"NumOfEps\":\"1\",\"Eps\":[{\"Fqdn\":\"localhost\",\"Port\":\"6030\"}]},\"NumOfTable\":\"0\"}]}}},\"PartitionBy\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"groupid\",\"UserAlias\":\"groupid\",\"TableId\":\"6555383776122680534\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"test\",\"TableName\":\"meters\",\"TableAlias\":\"meters\",\"ColName\":\"groupid\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"Window\":{\"NodeType\":\"14\",\"Name\":\"IntervalWindow\",\"IntervalWindow\":{\"Interval\":{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"115\",\"Bytes\":\"8\"},\"AliasName\":\"c804c3a15ebe05b5baf40ad5ee12be1f\",\"UserAlias\":\"2s\",\"LiteralSize\":\"2\",\"Literal\":\"2s\",\"Duration\":true,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"1
15\",\"Datum\":\"2000\"}},\"TsPk\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"TableId\":\"6555383776122680534\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"meters\",\"TableAlias\":\"meters\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}}},\"StmtName\":\"0x1580095ba\",\"HasAggFuncs\":true}}";
  //  char* ast = "{\"NodeType\":\"101\",\"Name\":\"SelectStmt\",\"SelectStmt\":{\"Distinct\":false,\"Projections\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_1\",\"UserAlias\":\"wstart\",\"Name\":\"_wstart\",\"Id\":\"89\",\"Type\":\"3505\",\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"#expr_2\",\"UserAlias\":\"min(c1)\",\"Name\":\"min\",\"Id\":\"2\",\"Type\":\"8\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"3\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"2\"},\"AliasName\":\"#expr_3\",\"UserAlias\":\"max(c2)\",\"Name\":\"max\",\"Id\":\"3\",\"Type\":\"7\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"3\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"2\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_4\",\"UserAlias\":\"sum(c3)\",\"Name\":\"sum\",\"Id\":\"1\",\"Type\":\"14\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c3\",\"UserAlias\":\"c3\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c3\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"LiteralSize\":\"0\",\"Duration\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_5\",\"UserAlias\":\"first(c4)\",\"Name\":\"first\",\"Id\":\"33\",\"Type\":\"504\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"c4\",\"UserAlias\":\"c4\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c4\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":
\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"11\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"#expr_6\",\"UserAlias\":\"last(c5)\",\"Name\":\"last\",\"Id\":\"36\",\"Type\":\"506\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"11\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"c5\",\"UserAlias\":\"c5\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c5\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"12\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"2\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_7\",\"UserAlias\":\"apercentile(c6, 50)\",\"Name\":\"apercentile\",\"Id\":\"12\",\"Type\":\"1\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"12\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"2\"},\"AliasName\":\"c6\",\"UserAlias\":\"c6\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"7\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c6\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"c0c7c76d30bd3dcaefc96f40275bdc0a\",\"UserAlias\":\"50\",\"LiteralSize\":\"2\",\"Literal\":\"50\",\"Duration\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"50\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"LiteralSize\":\"0\",\"Duration\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"13\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_8\",\"UserAlias\":\"avg(c7)\",\"Name\":\"avg\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"13\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c7\",\"UserAlias\":\"c7\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"8\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c7\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"Data
Type\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"LiteralSize\":\"0\",\"Duration\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"14\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_9\",\"UserAlias\":\"count(c8)\",\"Name\":\"count\",\"Id\":\"0\",\"Type\":\"3\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"14\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"c8\",\"UserAlias\":\"c8\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"9\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c8\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"LiteralSize\":\"0\",\"Duration\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"6\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"Node
  SNode *     pAst = NULL;
  SQueryPlan *pPlan = NULL;

  if (taosCreateLog("taoslog", 10, "/etc/taos", NULL, NULL, NULL, NULL, 1) != 0) {
    // ignore create log failed, only print
    (void) printf(" WARING: Create failed:%s. configDir\n", strerror(errno));
  }

  if (nodesStringToNode(ast, &pAst) < 0) {
    ASSERT(0);
  }

  SPlanContext cxt = {0};
  cxt.pAstRoot = pAst;
  cxt.topicQuery = false;
  cxt.streamQuery = true;
  cxt.triggerType = STREAM_TRIGGER_WINDOW_CLOSE;
  cxt.watermark = 1;
  cxt.igExpired = 1;
  cxt.deleteMark = 1;
  cxt.igCheckUpdate = 1;

  // using ast and param to build physical plan
  if (qCreateQueryPlan(&cxt, &pPlan, NULL) < 0) {
    ASSERT(0);
  }

  if (pAst != NULL) nodesDestroyNode(pAst);
  nodesDestroyNode((SNode*)pPlan);
}
*/
// clang-format on
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


#pragma GCC diagnostic pop
