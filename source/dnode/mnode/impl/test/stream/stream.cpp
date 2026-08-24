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
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "functionMgt.h"
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
#include "../../inc/mndDb.h"
#include "../../inc/mndExtSource.h"
#include "../../inc/mndPrivilege.h"
#include "../../inc/mndSnode.h"
#include "../../inc/mndStb.h"
#include "../../inc/mndStream.h"
#include "../../inc/mndStreamRecalc.h"
#include "../../inc/mndUser.h"
#include "../../inc/mndVgroup.h"
#include "sdb.h"

extern "C" int32_t msmBuildTriggerDeployInfo(SMnode *pMnode, SStmStatus *pInfo, SStmTaskDeploy *pDeploy,
                                             SStreamObj *pStream);
extern "C" void    msmStopStreamByError(int64_t streamId, SStmStatus *pStatus, int32_t errCode, int64_t currTs);
extern "C" int32_t msmNormalHandleHbMsg(SStmGrpCtx *pCtx);
extern "C" int32_t msmNormalHandleStatusUpdate(SStmGrpCtx *pCtx);
extern "C" void    msmChkHandleTriggerOperations(SStmGrpCtx *pCtx, SStmTaskStatusMsg *pTask, SStmTaskStatus *pStatus);
extern "C" int32_t msmWatchRecordNewTask(SStmGrpCtx *pCtx, SStmTaskStatusMsg *pTask);
extern "C" int32_t msmAssignTaskSnodeId(SMnode *pMnode, SStreamObj *pStream, bool isStatic);
extern "C" void    msmDestroyRuntimeInfo(SMnode *pMnode);
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

class StreamTest : public ::testing::Test {
 protected:
  static SStreamWindowPlan* makeSessionPlan() {
    auto* pPlan = static_cast<SStreamWindowPlan*>(taosMemoryCalloc(1, sizeof(SStreamWindowPlan)));
    if (pPlan == nullptr) return nullptr;
    pPlan->version = STREAM_WINDOW_PLAN_VERSION;
    pPlan->pLayers = taosArrayInit(2, sizeof(SStreamWindowLayerSpec));
    if (pPlan->pLayers == nullptr) {
      tDestroyStreamWindowPlan(&pPlan);
      return nullptr;
    }

    SStreamWindowLayerSpec outer = {};
    tstrncpy(outer.name, "outer", sizeof(outer.name));
    outer.triggerType = WINDOW_TYPE_SESSION;
    outer.trigger.session.sessionVal = 20;
    if (taosArrayPush(pPlan->pLayers, &outer) == nullptr) {
      tDestroyStreamWindowPlan(&pPlan);
      return nullptr;
    }

    SStreamWindowLayerSpec leaf = {};
    leaf.triggerType = WINDOW_TYPE_SESSION;
    leaf.trigger.session.sessionVal = 10;
    if (taosArrayPush(pPlan->pLayers, &leaf) == nullptr) {
      tDestroyStreamWindowPlan(&pPlan);
      return nullptr;
    }
    return pPlan;
  }
};

int32_t failWindowPlanClone(const SStreamWindowPlan*, SStreamWindowPlan** ppPlan) {
  *ppPlan = nullptr;
  return TSDB_CODE_OUT_OF_MEMORY;
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

struct FullArrayDupGateState {
  std::mutex              mutex;
  std::condition_variable cv;
  std::thread::id         writerThread;
  bool                    entered = false;
  bool                    release = false;
};

FullArrayDupGateState *gFullArrayDupGate = nullptr;

SArray *gateFullArrayDup(const SArray *pSource, __array_item_dup_fn_t fn) {
  if (fn != nullptr) {
    terrno = TSDB_CODE_INVALID_PARA;
    return nullptr;
  }

  SArray                *pCopy = taosArrayFromList(pSource->pData, pSource->size, pSource->elemSize);
  FullArrayDupGateState *pGate = gFullArrayDupGate;
  if (pCopy == nullptr || pGate == nullptr || std::this_thread::get_id() != pGate->writerThread) return pCopy;

  std::unique_lock<std::mutex> lock(pGate->mutex);
  if (!pGate->entered) {
    pGate->entered = true;
    pGate->cv.notify_all();
    pGate->cv.wait(lock, [pGate]() { return pGate->release; });
  }
  return pCopy;
}

class FullArrayDupGate {
 public:
  FullArrayDupGate() {
    gFullArrayDupGate = &state_;
    stub_.set(taosArrayDup, gateFullArrayDup);
  }

  ~FullArrayDupGate() {
    Release();
    stub_.reset(taosArrayDup);
    gFullArrayDupGate = nullptr;
  }

  void SetWriterThread() {
    std::lock_guard<std::mutex> lock(state_.mutex);
    state_.writerThread = std::this_thread::get_id();
  }

  bool WaitUntilEntered() {
    std::unique_lock<std::mutex> lock(state_.mutex);
    return state_.cv.wait_for(lock, std::chrono::seconds(5), [this]() { return state_.entered; });
  }

  void Release() {
    std::lock_guard<std::mutex> lock(state_.mutex);
    state_.release = true;
    state_.cv.notify_all();
  }

 private:
  FullArrayDupGateState state_;
  Stub                  stub_;
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
    if (progress < 0 || progress > 100 || status < STREAM_RECALC_STATUS_PENDING ||
        status > STREAM_RECALC_STATUS_FAILED || (status == STREAM_RECALC_STATUS_PENDING && progress != 0) ||
        (status == STREAM_RECALC_STATUS_RUNNING && progress >= 100) ||
        (status == STREAM_RECALC_STATUS_FINISHED && progress != 100) ||
        (status == STREAM_RECALC_STATUS_FAILED && progress >= 100)) {
      taosArrayDestroy(entry.snapshot.pRecalculates);
      return TSDB_CODE_INVALID_PARA;
    }

    int32_t code = mstApplyTaskMetrics(&trigger_, 0, create_.streamId, &entry);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(entry.snapshot.pRecalculates);
      return code;
    }

    taosWLockLatch(&stored_->userRecalcLock);
    SStmRecalcRecord *pRecord = nullptr;
    if (stored_->recalcRecords == nullptr) {
      stored_->recalcRecords = taosArrayInit(1, sizeof(SStmRecalcRecord));
      if (stored_->recalcRecords == nullptr) {
        code = terrno;
        goto _unlock;
      }
    }
    for (int32_t i = taosArrayGetSize(stored_->recalcRecords) - 1; i >= 0; --i) {
      const auto *pRecord = static_cast<const SStmRecalcRecord *>(taosArrayGet(stored_->recalcRecords, i));
      if (pRecord->hidden && pRecord->snapshot.recalcId != recalcId) {
        taosArrayRemove(stored_->recalcRecords, i);
      }
    }

    for (int32_t i = 0; i < taosArrayGetSize(stored_->recalcRecords); ++i) {
      auto *pCurrent = static_cast<SStmRecalcRecord *>(taosArrayGet(stored_->recalcRecords, i));
      if (pCurrent->snapshot.recalcId == recalcId) {
        pRecord = pCurrent;
        break;
      }
    }
    if (pRecord == nullptr) {
      SStmRecalcRecord record = {};
      record.snapshot = recalc;
      record.terminalObservedAtMs =
          status == STREAM_RECALC_STATUS_FINISHED || status == STREAM_RECALC_STATUS_FAILED ? gRecalcNowMs : 0;
      record.typedStatusKnown = true;
      record.visible = true;
      if (taosArrayPush(stored_->recalcRecords, &record) == nullptr) code = terrno;
      goto _unlock;
    }
    if (pRecord->snapshot.start != start || pRecord->snapshot.end != end || progress < pRecord->snapshot.progressPct ||
        ((pRecord->snapshot.status == STREAM_RECALC_STATUS_FINISHED ||
          pRecord->snapshot.status == STREAM_RECALC_STATUS_FAILED) &&
         (pRecord->snapshot.status != status || pRecord->snapshot.progressPct != progress)) ||
        (pRecord->snapshot.status == STREAM_RECALC_STATUS_RUNNING && status == STREAM_RECALC_STATUS_PENDING)) {
      code = TSDB_CODE_INVALID_MSG;
      goto _unlock;
    }
    if (pRecord->snapshot.status != STREAM_RECALC_STATUS_FINISHED &&
        pRecord->snapshot.status != STREAM_RECALC_STATUS_FAILED &&
        (status == STREAM_RECALC_STATUS_FINISHED || status == STREAM_RECALC_STATUS_FAILED)) {
      pRecord->terminalObservedAtMs = gRecalcNowMs;
    }
    pRecord->snapshot = recalc;
    pRecord->typedStatusKnown = true;

  _unlock:
    taosWUnLockLatch(&stored_->userRecalcLock);
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

class StreamRecalculateViewTest : public MndStreamRecalcTest {
 protected:
  SStmRecalcRecord *AddViewRecord(int64_t recalcId, EStreamRecalcStatus status, int32_t progress,
                                  int64_t requestTimeMs = 1710000000123) {
    if (stored_->recalcRecords == nullptr) {
      stored_->recalcRecords = taosArrayInit(2, sizeof(SStmRecalcRecord));
    }
    if (stored_->recalcRecords == nullptr) return nullptr;

    SStmRecalcRecord record = {};
    record.snapshot.recalcId = recalcId;
    record.snapshot.start = 100;
    record.snapshot.end = 200;
    record.snapshot.progressPct = progress;
    record.snapshot.status = status;
    record.requestTimeMs = requestTimeMs;
    record.typedStatusKnown = true;
    record.visible = true;
    return static_cast<SStmRecalcRecord *>(taosArrayPush(stored_->recalcRecords, &record));
  }

  SStmRecalcRecord *ViewRecord(int64_t recalcId) {
    for (int32_t i = 0; stored_->recalcRecords != nullptr && i < taosArrayGetSize(stored_->recalcRecords); ++i) {
      auto *pRecord = static_cast<SStmRecalcRecord *>(taosArrayGet(stored_->recalcRecords, i));
      if (pRecord != nullptr && pRecord->snapshot.recalcId == recalcId) return pRecord;
    }
    return nullptr;
  }

  int32_t ReportTypedRecalculations(int64_t firstRecalcId, int32_t count) {
    SSTriggerRuntimeStatus legacy = {};
    legacy.userRecalcs = taosArrayInit(count, sizeof(SSTriggerRecalcProgress));
    if (legacy.userRecalcs == nullptr) return terrno;

    SStreamTaskMetricsEntry entry = {};
    entry.taskStatusIndex = 0;
    entry.streamId = create_.streamId;
    entry.taskId = trigger_.id.taskId;
    entry.seriousId = trigger_.id.seriousId;
    entry.snapshot.applicableMask = STREAM_METRIC_RECALCULATES;
    entry.snapshot.validMask = STREAM_METRIC_RECALCULATES;
    entry.snapshot.pRecalculates = taosArrayInit(count, sizeof(SStreamRecalcSnapshot));
    if (entry.snapshot.pRecalculates == nullptr) {
      taosArrayDestroy(legacy.userRecalcs);
      return terrno;
    }

    int32_t code = TSDB_CODE_SUCCESS;
    for (int32_t i = 0; i < count; ++i) {
      const int64_t                 recalcId = firstRecalcId + i;
      const SSTriggerRecalcProgress legacyProgress = {
          .recalcId = recalcId,
          .progress = 100,
          .start = 100,
          .end = 200,
      };
      const SStreamRecalcSnapshot typedProgress = {
          .recalcId = recalcId,
          .start = 100,
          .end = 200,
          .progressPct = 100,
          .status = STREAM_RECALC_STATUS_FINISHED,
      };
      if (taosArrayPush(legacy.userRecalcs, &legacyProgress) == nullptr ||
          taosArrayPush(entry.snapshot.pRecalculates, &typedProgress) == nullptr) {
        code = terrno;
        goto _exit;
      }
    }

    code = mstCopyTriggerRuntimeStatus(&trigger_, &legacy);
    if (code == TSDB_CODE_SUCCESS) {
      code = mstApplyTaskMetrics(&trigger_, 0, create_.streamId, &entry);
    }

  _exit:
    taosArrayDestroy(legacy.userRecalcs);
    taosArrayDestroy(entry.snapshot.pRecalculates);
    return code;
  }
};

std::string Hex8(uint32_t value) {
  char text[9] = {0};
  snprintf(text, sizeof(text), "%08" PRIX32, value);
  return text;
}

const char *returnLongRecalcError(int32_t) {
  static const std::string error(400, 'x');
  return error.c_str();
}

SStreamObj BuildMinimalStreamObj() {
  SStreamObj stream = {};
  stream.pCreate = static_cast<SCMCreateStreamReq *>(taosMemoryCalloc(1, sizeof(*stream.pCreate)));
  if (stream.pCreate == nullptr) return stream;

  stream.pCreate->name = taosStrdup("1.test.stream");
  stream.pCreate->sql = taosStrdup("select 1");
  stream.pCreate->streamDB = taosStrdup("1.test");
  stream.pCreate->streamId = 1;
  stream.pCreate->triggerType = WINDOW_TYPE_INTERVAL;
  tstrncpy(stream.name, stream.pCreate->name, sizeof(stream.name));
  return stream;
}

std::vector<uint8_t> EncodeStreamObj(const SStreamObj &stream) {
  SEncoder encoder = {};
  tEncoderInit(&encoder, nullptr, 0);
  const int32_t size = tEncodeSStreamObj(&encoder, &stream);
  tEncoderClear(&encoder);
  if (size < 0) return {};

  std::vector<uint8_t> encoded(size);
  tEncoderInit(&encoder, encoded.data(), size);
  if (tEncodeSStreamObj(&encoder, &stream) < 0) encoded.clear();
  tEncoderClear(&encoder);
  return encoded;
}

SStreamObj DecodeStreamObj(const std::vector<uint8_t> &encoded, int32_t sver) {
  SStreamObj stream = {};
  SDecoder   decoder = {};
  tDecoderInit(&decoder, const_cast<uint8_t *>(encoded.data()), encoded.size());
  if (tDecodeSStreamObj(&decoder, &stream, sver) != TSDB_CODE_SUCCESS) tFreeStreamObj(&stream);
  tDecoderClear(&decoder);
  return stream;
}

SStreamObj RoundTripStreamObj(const SStreamObj &stream, int32_t sver) {
  return DecodeStreamObj(EncodeStreamObj(stream), sver);
}

SStreamObj DecodeStreamRaw(SSdbRaw *pRaw) {
  int32_t tlen = 0;
  if (pRaw == nullptr || sdbGetRawInt32(pRaw, 0, &tlen) != TSDB_CODE_SUCCESS || tlen <= 0) return {};
  std::vector<uint8_t> encoded(tlen);
  if (sdbGetRawBinary(pRaw, sizeof(tlen), reinterpret_cast<char *>(encoded.data()), tlen) != TSDB_CODE_SUCCESS) {
    return {};
  }
  const int32_t markerPos = sizeof(tlen) + tlen;
  int8_t        updateKind = MND_STREAM_RAW_UPDATE_FULL;
  if (pRaw->dataLen == markerPos + (int32_t)sizeof(updateKind) &&
      sdbGetRawInt8(pRaw, markerPos, &updateKind) != TSDB_CODE_SUCCESS) {
    return {};
  }
  if (updateKind == MND_STREAM_RAW_UPDATE_RECALC_PATCH) {
    SStreamObj stream = {};
    SDecoder   decoder = {};
    int32_t    requestNum = 0;
    int32_t    code = TSDB_CODE_SUCCESS;
    tDecoderInit(&decoder, encoded.data(), encoded.size());
    if ((code = tStartDecode(&decoder)) == TSDB_CODE_SUCCESS) code = tDecodeCStrTo(&decoder, stream.name);
    if (code == TSDB_CODE_SUCCESS) code = tDecodeU64(&decoder, &stream.recalcRevision);
    if (code == TSDB_CODE_SUCCESS) code = tDecodeI32(&decoder, &requestNum);
    if (code == TSDB_CODE_SUCCESS && requestNum > 0) {
      stream.pIncompleteRecalcs = taosArrayInit(requestNum, sizeof(SStreamRecalcPersistReq));
      if (stream.pIncompleteRecalcs == nullptr) code = terrno;
    }
    for (int32_t i = 0; code == TSDB_CODE_SUCCESS && i < requestNum; ++i) {
      SStreamRecalcPersistReq request = {};
      if ((code = tDecodeI64(&decoder, &request.recalcId)) == TSDB_CODE_SUCCESS) {
        code = tDecodeI64(&decoder, &request.start);
      }
      if (code == TSDB_CODE_SUCCESS) code = tDecodeI64(&decoder, &request.end);
      if (code == TSDB_CODE_SUCCESS) code = tDecodeI64(&decoder, &request.requestTimeMs);
      if (code == TSDB_CODE_SUCCESS && taosArrayPush(stream.pIncompleteRecalcs, &request) == nullptr) code = terrno;
    }
    tEndDecode(&decoder);
    tDecoderClear(&decoder);
    if (code != TSDB_CODE_SUCCESS || requestNum < 0) {
      tFreeStreamObj(&stream);
      return {};
    }
    stream.sdbRawUpdateKind = updateKind;
    return stream;
  }
  return DecodeStreamObj(encoded, MND_STREAM_VER_NUMBER);
}

struct StreamEncodeMallocGateState {
  std::thread::id   encoderThread;
  std::atomic<bool> entered{false};
  std::atomic<bool> release{false};
};

StreamEncodeMallocGateState *gStreamEncodeMallocGate = nullptr;

void *gateStreamEncodeMalloc(int64_t size) {
  StreamEncodeMallocGateState *pGate = gStreamEncodeMallocGate;
  if (pGate != nullptr && std::this_thread::get_id() == pGate->encoderThread && !pGate->entered.exchange(true)) {
    while (!pGate->release.load()) std::this_thread::yield();
  }
  return taosMemCalloc(1, size);
}

class StreamEncodeMallocGate {
 public:
  StreamEncodeMallocGate() {
    gStreamEncodeMallocGate = &state_;
    stub_.set(taosMemMalloc, gateStreamEncodeMalloc);
  }

  ~StreamEncodeMallocGate() {
    state_.release.store(true);
    stub_.reset(taosMemMalloc);
    gStreamEncodeMallocGate = nullptr;
  }

  void SetEncoderThread() { state_.encoderThread = std::this_thread::get_id(); }
  bool Entered() const { return state_.entered.load(); }
  void Release() { state_.release.store(true); }

 private:
  StreamEncodeMallocGateState state_;
  Stub                        stub_;
};

int32_t EncodeLegacyStreamObj(SEncoder *encoder, const SStreamObj *stream) {
  TAOS_CHECK_RETURN(tStartEncode(encoder));
  TAOS_CHECK_RETURN(tEncodeCStr(encoder, stream->name));
  TAOS_CHECK_RETURN(tSerializeSCMCreateStreamReqImpl(encoder, stream->pCreate));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, stream->mainSnodeId));
  TAOS_CHECK_RETURN(tEncodeI8(encoder, stream->userStopped));
  TAOS_CHECK_RETURN(tEncodeI64(encoder, stream->createTime));
  TAOS_CHECK_RETURN(tEncodeI64(encoder, stream->updateTime));
  TAOS_CHECK_RETURN(tEncodeCStr(encoder, stream->createUser));
  TAOS_CHECK_RETURN(tEncodeI64(encoder, stream->ownerId));
  TAOS_CHECK_RETURN(tEncodeU64(encoder, stream->flags));
  tEndEncode(encoder);
  return encoder->pos;
}

std::vector<uint8_t> EncodeStreamObjWithoutRecalcTail() {
  SStreamObj stream = BuildMinimalStreamObj();
  SEncoder   encoder = {};
  tEncoderInit(&encoder, nullptr, 0);
  const int32_t size = EncodeLegacyStreamObj(&encoder, &stream);
  tEncoderClear(&encoder);
  if (size < 0) {
    tFreeStreamObj(&stream);
    return {};
  }

  std::vector<uint8_t> encoded(size);
  tEncoderInit(&encoder, encoded.data(), size);
  const int32_t code = EncodeLegacyStreamObj(&encoder, &stream);
  tEncoderClear(&encoder);
  tFreeStreamObj(&stream);
  if (code < 0) encoded.clear();
  return encoded;
}

std::vector<uint8_t> EncodeStreamObjWithRecalcTail() {
  SStreamObj stream = BuildMinimalStreamObj();
  stream.recalcRevision = 1;
  stream.pIncompleteRecalcs = taosArrayInit(1, sizeof(SStreamRecalcPersistReq));
  SStreamRecalcPersistReq req = {};
  req.recalcId = 1;
  req.start = 100;
  req.end = 200;
  req.requestTimeMs = 300;
  if (stream.pIncompleteRecalcs == nullptr || taosArrayPush(stream.pIncompleteRecalcs, &req) == nullptr) {
    tFreeStreamObj(&stream);
    return {};
  }

  std::vector<uint8_t> encoded = EncodeStreamObj(stream);
  tFreeStreamObj(&stream);
  return encoded;
}

int32_t DecodeLegacyStreamObjAndConsumeOuterObject(const std::vector<uint8_t> &encoded) {
  SStreamObj stream = {};
  SDecoder   decoder = {};
  int32_t    code = TSDB_CODE_SUCCESS;
  tDecoderInit(&decoder, const_cast<uint8_t *>(encoded.data()), encoded.size());
  if ((code = tStartDecode(&decoder)) == TSDB_CODE_SUCCESS) code = tDecodeCStrTo(&decoder, stream.name);
  if (code == TSDB_CODE_SUCCESS) {
    stream.pCreate = static_cast<SCMCreateStreamReq *>(taosMemoryCalloc(1, sizeof(*stream.pCreate)));
    if (stream.pCreate == nullptr) code = terrno;
  }
  if (code == TSDB_CODE_SUCCESS) code = tDeserializeSCMCreateStreamReqImpl(&decoder, stream.pCreate);
  if (code == TSDB_CODE_SUCCESS) code = tDecodeI32(&decoder, &stream.mainSnodeId);
  if (code == TSDB_CODE_SUCCESS) code = tDecodeI8(&decoder, &stream.userStopped);
  if (code == TSDB_CODE_SUCCESS) code = tDecodeI64(&decoder, &stream.createTime);
  if (code == TSDB_CODE_SUCCESS) code = tDecodeI64(&decoder, &stream.updateTime);
  if (code == TSDB_CODE_SUCCESS) code = tDecodeCStrTo(&decoder, stream.createUser);
  if (code == TSDB_CODE_SUCCESS) code = tDecodeI64(&decoder, &stream.ownerId);
  if (code == TSDB_CODE_SUCCESS) code = tDecodeU64(&decoder, &stream.flags);
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  tFreeStreamObj(&stream);
  return code;
}

SStreamObj BuildStreamObjWithRequest(uint64_t revision, int64_t recalcId) {
  SStreamObj stream = BuildMinimalStreamObj();
  stream.recalcRevision = revision;
  stream.pIncompleteRecalcs = taosArrayInit(1, sizeof(SStreamRecalcPersistReq));
  SStreamRecalcPersistReq req = {};
  req.recalcId = recalcId;
  req.start = 100;
  req.end = 200;
  req.requestTimeMs = 300;
  if (stream.pIncompleteRecalcs == nullptr || taosArrayPush(stream.pIncompleteRecalcs, &req) == nullptr) {
    tFreeStreamObj(&stream);
  }
  return stream;
}

SArray *BuildRequests(std::initializer_list<int64_t> recalcIds) {
  SArray *requests = taosArrayInit(recalcIds.size(), sizeof(SStreamRecalcPersistReq));
  if (requests == nullptr) return nullptr;

  for (const int64_t recalcId : recalcIds) {
    const SStreamRecalcPersistReq request = {
        .recalcId = recalcId,
        .start = 100,
        .end = 200,
        .requestTimeMs = 300,
    };
    if (taosArrayPush(requests, &request) == nullptr) {
      taosArrayDestroy(requests);
      return nullptr;
    }
  }
  return requests;
}

SArray *initArrayWithInheritedTerrno(size_t size, size_t elemSize) {
  if (size < TARRAY_MIN_SIZE) size = TARRAY_MIN_SIZE;

  auto *array = static_cast<SArray *>(taosMemoryMalloc(sizeof(SArray)));
  if (array == nullptr) return nullptr;
  array->pData = taosMemoryCalloc(size, elemSize);
  if (array->pData == nullptr) {
    taosMemoryFree(array);
    return nullptr;
  }

  array->size = 0;
  array->capacity = static_cast<uint32_t>(size);
  array->elemSize = elemSize;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return array;
}

int64_t OnlyPersistedRecalcId(const SStreamObj &stream) {
  const auto *req = static_cast<const SStreamRecalcPersistReq *>(taosArrayGet(stream.pIncompleteRecalcs, 0));
  return req == nullptr ? 0 : req->recalcId;
}

class StreamObjSdbUpdateTest : public ::testing::Test {
 protected:
  void SetUp() override {
    SSdbOpt options = {};
    options.path = TD_TMP_DIR_PATH "stream_obj_sdb_update";
    options.pMnode = &mnode_;
    taosRemoveDir(options.path);

    pSdb_ = sdbInit(&options);
    ASSERT_NE(pSdb_, nullptr);
    mnode_.pSdb = pSdb_;
    ASSERT_EQ(mndInitStream(&mnode_), TSDB_CODE_SUCCESS);
  }

  void TearDown() override {
    if (pSdb_ != nullptr) sdbCleanup(pSdb_);
    pSdb_ = nullptr;
  }

  int32_t WriteStream(SStreamObj *stream, ESdbStatus status) {
    SSdbRaw *raw = mndStreamActionEncode(stream);
    if (raw == nullptr) return terrno;

    const int32_t code = sdbSetRawStatus(raw, status);
    if (code != TSDB_CODE_SUCCESS) {
      sdbFreeRaw(raw);
      return code;
    }
    return sdbWrite(pSdb_, raw);
  }

  SSdbRaw *EncodeRecalcPatch(SStreamObj *source, uint64_t revision, SArray *requests) {
    STrans *trans = createTestTrans();
    if (trans == nullptr) return nullptr;

    if (mndStreamTransAppendRecalcUpdate(source, revision, requests, trans, SDB_STATUS_READY) != TSDB_CODE_SUCCESS ||
        taosArrayGetSize(trans->commitActions) != 1) {
      mndTransDrop(trans);
      return nullptr;
    }

    auto *action = static_cast<STransAction *>(taosArrayGet(trans->commitActions, 0));
    if (action == nullptr) {
      mndTransDrop(trans);
      return nullptr;
    }

    SSdbRaw *raw = action->pRaw;
    action->pRaw = nullptr;
    mndTransDrop(trans);
    return raw;
  }

  SSdbRaw *EncodeRecalcPatchWithZeroRecalcId(SStreamObj *source, uint64_t revision, SArray *requests) {
    SSdbRaw *raw = EncodeRecalcPatch(source, revision, requests);
    if (raw == nullptr) return nullptr;

    int32_t tlen = 0;
    if (sdbGetRawInt32(raw, 0, &tlen) != TSDB_CODE_SUCCESS || tlen <= 0) {
      sdbFreeRaw(raw);
      return nullptr;
    }

    SDecoder decoder = {};
    char     streamName[TSDB_TABLE_FNAME_LEN] = {};
    uint64_t decodedRevision = 0;
    int32_t  requestNum = 0;
    tDecoderInit(&decoder, reinterpret_cast<uint8_t *>(raw->pData) + sizeof(tlen), tlen);
    int32_t code = tStartDecode(&decoder);
    if (code == TSDB_CODE_SUCCESS) code = tDecodeCStrTo(&decoder, streamName);
    if (code == TSDB_CODE_SUCCESS) code = tDecodeU64(&decoder, &decodedRevision);
    if (code == TSDB_CODE_SUCCESS) code = tDecodeI32(&decoder, &requestNum);
    uint8_t *recalcId = TD_CODER_CURRENT(&decoder);
    if (code == TSDB_CODE_SUCCESS && (requestNum != 1 || TD_CODER_REMAIN_CAPACITY(&decoder) < sizeof(int64_t))) {
      code = TSDB_CODE_INVALID_MSG;
    }
    if (code == TSDB_CODE_SUCCESS) memset(recalcId, 0, sizeof(int64_t));
    tEndDecode(&decoder);
    tDecoderClear(&decoder);
    if (code != TSDB_CODE_SUCCESS) {
      sdbFreeRaw(raw);
      return nullptr;
    }
    return raw;
  }

  int32_t WriteRecalcPatch(SStreamObj &source, uint64_t revision, SArray *requests) {
    SSdbRaw *raw = EncodeRecalcPatch(&source, revision, requests);
    if (raw == nullptr) return terrno;

    const int32_t code = sdbSetRawStatus(raw, SDB_STATUS_READY);
    if (code != TSDB_CODE_SUCCESS) {
      sdbFreeRaw(raw);
      return code;
    }
    return sdbWrite(pSdb_, raw);
  }

  int32_t ApplyRegisteredUpdate(SStreamObj *pStored, SStreamObj *pUpdate) {
    SdbUpdateFp update = pSdb_->updateFps[SDB_STREAM];
    return update == nullptr ? TSDB_CODE_INTERNAL_ERROR : update(pSdb_, pStored, pUpdate);
  }

  SStreamObj *AcquireStored() { return static_cast<SStreamObj *>(sdbAcquire(pSdb_, SDB_STREAM, "1.test.stream")); }

  template <typename Append>
  void CaptureAppendWhileRegisteredWriterWaits(SStreamObj *pPersisted, SStreamObj *pNewer, Append append,
                                               SStreamObj *pEncoded) {
    *pEncoded = {};
    STrans *pTrans = createTestTrans();
    ASSERT_NE(pTrans, nullptr);

    std::atomic<int32_t> appendCode{TSDB_CODE_INTERNAL_ERROR};
    std::atomic<int32_t> writerCode{TSDB_CODE_INTERNAL_ERROR};
    std::atomic<bool>    writerFinished{false};
    bool                 writerWaited = false;
    {
      StreamEncodeMallocGate gate;
      std::thread            encoder([&]() {
        gate.SetEncoderThread();
        appendCode.store(append(pTrans));
      });
      const auto             encodeDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      while (!gate.Entered() && std::chrono::steady_clock::now() < encodeDeadline) std::this_thread::yield();
      if (!gate.Entered()) {
        gate.Release();
        encoder.join();
        mndTransDrop(pTrans);
        ADD_FAILURE() << "stream append did not reach the second encode pass";
        return;
      }

      std::thread writer([&]() {
        writerCode.store(WriteStream(pNewer, SDB_STATUS_READY));
        writerFinished.store(true);
      });
      const auto  writerDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      while (!writerFinished.load() && !taosHasRWWFlag(&pPersisted->lock) &&
             std::chrono::steady_clock::now() < writerDeadline) {
        std::this_thread::yield();
      }
      writerWaited = taosHasRWWFlag(&pPersisted->lock) && !writerFinished.load();
      gate.Release();
      encoder.join();
      writer.join();
    }

    EXPECT_EQ(appendCode.load(), TSDB_CODE_SUCCESS);
    EXPECT_EQ(writerCode.load(), TSDB_CODE_SUCCESS);
    EXPECT_TRUE(writerWaited);
    if (taosArrayGetSize(pTrans->commitActions) == 1) {
      const auto *pAction = static_cast<const STransAction *>(taosArrayGet(pTrans->commitActions, 0));
      if (pAction != nullptr) *pEncoded = DecodeStreamRaw(pAction->pRaw);
    } else {
      ADD_FAILURE() << "stream append did not produce exactly one commit action";
    }
    mndTransDrop(pTrans);
  }

  SMnode mnode_ = {};
  SSdb  *pSdb_ = nullptr;
};

struct CapturedRecalcTransaction {
  STrans                     *key = nullptr;
  SStreamObj                 *sourceStream = nullptr;
  int64_t                     streamId = 0;
  uint64_t                    revision = 0;
  SArray                     *requests = nullptr;
  tmsg_t                      requestMsgType = 0;
  std::vector<SRpcHandleInfo> rpcHandles;
  std::vector<uint8_t>        callbackParam;
};

std::atomic<bool> *gRuntimeDestroyEntered = nullptr;

void captureRuntimeDestroy(SMnode *) { gRuntimeDestroyEntered->store(true); }

void ignoreRuntimeVnodeLeaderRemoval(int32_t) {}

class RuntimeDestroyProbe {
 public:
  RuntimeDestroyProbe() {
    gRuntimeDestroyEntered = &entered_;
    stub_.set(streamRemoveVnodeLeader, ignoreRuntimeVnodeLeaderRemoval);
    stub_.set(msmDestroyRuntimeInfo, captureRuntimeDestroy);
  }

  ~RuntimeDestroyProbe() {
    stub_.reset(msmDestroyRuntimeInfo);
    stub_.reset(streamRemoveVnodeLeader);
    gRuntimeDestroyEntered = nullptr;
  }

  bool Entered() const { return entered_.load(); }

 private:
  std::atomic<bool> entered_{false};
  Stub              stub_;
};

bool waitForRuntimeWriterOrDestroy(const RuntimeDestroyProbe &probe) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!probe.Entered() && !taosHasRWWFlag(&mStreamMgmt.runtimeLock) && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return probe.Entered() || taosHasRWWFlag(&mStreamMgmt.runtimeLock);
}

struct RecalcPersistenceHarness {
  int32_t                               createCalls = 0;
  ETrnConflct                           createConflict = TRN_CONFLICT_NOTHING;
  int32_t                               queueCode = TSDB_CODE_SUCCESS;
  const SArray                         *snapshotDupSource = nullptr;
  std::atomic<bool>                     snapshotDupEntered{false};
  std::atomic<bool>                     snapshotDupHasRuntimeRead{false};
  std::atomic<bool>                     releaseSnapshotDup{false};
  std::deque<int32_t>                   createCodes;
  std::deque<int32_t>                   appendCodes;
  std::deque<int32_t>                   prepareCodes;
  std::deque<CapturedRecalcTransaction> transactions;
  std::deque<SRpcMsg>                   queuedMessages;
  std::vector<int32_t>                  responseCodes;
  std::vector<void *>                   responseHandles;
  std::atomic<bool>                     responseWhileRuntimeRead{false};
  std::atomic<bool>                     scheduleWhileRuntimeRead{false};
  tmsg_t                                lastCreateMsgType = 0;
  std::vector<SRpcHandleInfo>           lastCreateRpcHandles;
  SStmStatus                           *responseProbeStatus = nullptr;
  int64_t                               responseProbeRecalcId = 0;
  bool                                  responseObservedPublished = false;
};

RecalcPersistenceHarness *gRecalcPersistenceHarness = nullptr;

bool runtimeReadPreventsWriter() {
  bool        prevented = false;
  std::thread writer([&]() {
    if (taosWTryLockLatch(&mStreamMgmt.runtimeLock) != 0) {
      prevented = true;
    } else {
      taosWUnLockLatch(&mStreamMgmt.runtimeLock);
    }
  });
  writer.join();
  return prevented;
}

struct RecalcCompletedNodeFreeProbeState {
  void             *target = nullptr;
  Stub             *stub = nullptr;
  std::atomic<bool> freed{false};
  std::atomic<bool> freedWhileRuntimeRead{false};
};

RecalcCompletedNodeFreeProbeState *gRecalcCompletedNodeFreeProbe = nullptr;

void captureRecalcCompletedNodeFree(void *pAllocation) {
  RecalcCompletedNodeFreeProbeState *pProbe = gRecalcCompletedNodeFreeProbe;
  if (pProbe != nullptr && pAllocation == pProbe->target) {
    pProbe->freed.store(true);
    if (runtimeReadPreventsWriter()) pProbe->freedWhileRuntimeRead.store(true);
  }
  pProbe->stub->reset(taosMemFree);
  taosMemFree(pAllocation);
  pProbe->stub->set(taosMemFree, captureRecalcCompletedNodeFree);
}

class RecalcCompletedNodeFreeProbe {
 public:
  explicit RecalcCompletedNodeFreeProbe(void *pTarget) {
    state_.target = pTarget;
    state_.stub = &stub_;
    gRecalcCompletedNodeFreeProbe = &state_;
    stub_.set(taosMemFree, captureRecalcCompletedNodeFree);
  }

  ~RecalcCompletedNodeFreeProbe() {
    stub_.reset(taosMemFree);
    gRecalcCompletedNodeFreeProbe = nullptr;
  }

  bool Freed() const { return state_.freed.load(); }

  bool FreedWhileRuntimeRead() const { return state_.freedWhileRuntimeRead.load(); }

 private:
  RecalcCompletedNodeFreeProbeState state_;
  Stub                              stub_;
};

SArray *gateRecalcSnapshotDup(const SArray *pSrc, __array_item_dup_fn_t fn) {
  if (fn != nullptr) {
    terrno = TSDB_CODE_INVALID_PARA;
    return nullptr;
  }

  SArray *pCopy = taosArrayFromList(pSrc->pData, pSrc->size, pSrc->elemSize);
  if (pCopy == nullptr || pSrc != gRecalcPersistenceHarness->snapshotDupSource) return pCopy;

  if (taosWTryLockLatch(&mStreamMgmt.runtimeLock) == 0) {
    taosWUnLockLatch(&mStreamMgmt.runtimeLock);
  } else {
    gRecalcPersistenceHarness->snapshotDupHasRuntimeRead.store(true);
  }
  gRecalcPersistenceHarness->snapshotDupEntered.store(true);
  while (!gRecalcPersistenceHarness->releaseSnapshotDup.load()) std::this_thread::yield();
  return pCopy;
}

int32_t gRecalcCallocFailureCalls = 0;

void *failRecalcCalloc(int64_t, int64_t) {
  ++gRecalcCallocFailureCalls;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

class RecalcCallocFailureGuard {
 public:
  RecalcCallocFailureGuard() : savedTerrno_(terrno) {
    gRecalcCallocFailureCalls = 0;
    stub_.set(taosMemCalloc, failRecalcCalloc);
  }

  ~RecalcCallocFailureGuard() {
    stub_.reset(taosMemCalloc);
    terrno = savedTerrno_;
  }

 private:
  int32_t savedTerrno_ = TSDB_CODE_SUCCESS;
  Stub    stub_;
};

std::atomic<int32_t> gRecalcCallocBarrierCalls{0};
std::atomic<bool>    gReleaseRecalcCallocBarrier{false};

void *gateRecalcCalloc(int64_t num, int64_t size) {
  gRecalcCallocBarrierCalls.fetch_add(1);
  while (!gReleaseRecalcCallocBarrier.load()) std::this_thread::yield();
  void *pMemory = taosMemMalloc(num * size);
  if (pMemory != nullptr) memset(pMemory, 0, num * size);
  return pMemory;
}

class RecalcCallocBarrierGuard {
 public:
  RecalcCallocBarrierGuard() {
    gRecalcCallocBarrierCalls.store(0);
    gReleaseRecalcCallocBarrier.store(false);
    stub_.set(taosMemCalloc, gateRecalcCalloc);
  }

  ~RecalcCallocBarrierGuard() {
    gReleaseRecalcCallocBarrier.store(true);
    stub_.reset(taosMemCalloc);
  }

  void Release() { gReleaseRecalcCallocBarrier.store(true); }

 private:
  Stub stub_;
};

int32_t popHarnessCode(std::deque<int32_t> *pCodes, int32_t defaultCode) {
  if (pCodes->empty()) return defaultCode;
  const int32_t code = pCodes->front();
  pCodes->pop_front();
  return code;
}

int32_t captureRecalcCreateTrans(SMnode *, SStreamObj *, SRpcMsg *pReq, ETrnConflct conflict, const char *,
                                 STrans **ppTrans) {
  ++gRecalcPersistenceHarness->createCalls;
  gRecalcPersistenceHarness->createConflict = conflict;
  gRecalcPersistenceHarness->lastCreateMsgType = 0;
  gRecalcPersistenceHarness->lastCreateRpcHandles.clear();
  const int32_t code = popHarnessCode(&gRecalcPersistenceHarness->createCodes, TSDB_CODE_SUCCESS);
  if (code != TSDB_CODE_SUCCESS) return code;
  *ppTrans = createTestTrans();
  if (*ppTrans == nullptr) return TSDB_CODE_OUT_OF_MEMORY;
  (*ppTrans)->pRpcArray = taosArrayInit(1, sizeof(SRpcHandleInfo));
  if ((*ppTrans)->pRpcArray == nullptr) {
    mndTransDrop(*ppTrans);
    *ppTrans = nullptr;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (pReq != nullptr) {
    (*ppTrans)->originRpcType = pReq->msgType;
    if (taosArrayPush((*ppTrans)->pRpcArray, &pReq->info) == nullptr) {
      mndTransDrop(*ppTrans);
      *ppTrans = nullptr;
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  gRecalcPersistenceHarness->lastCreateMsgType = (*ppTrans)->originRpcType;
  for (int32_t i = 0; i < taosArrayGetSize((*ppTrans)->pRpcArray); ++i) {
    const auto *pInfo = static_cast<const SRpcHandleInfo *>(taosArrayGet((*ppTrans)->pRpcArray, i));
    if (pInfo != nullptr) gRecalcPersistenceHarness->lastCreateRpcHandles.push_back(*pInfo);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t captureRecalcTransAppend(SStreamObj *pStream, uint64_t revision, SArray *pRequests, STrans *pTrans, int32_t) {
  const int32_t code = popHarnessCode(&gRecalcPersistenceHarness->appendCodes, TSDB_CODE_SUCCESS);
  if (code != TSDB_CODE_SUCCESS) return code;

  CapturedRecalcTransaction captured;
  captured.key = pTrans;
  captured.sourceStream = pStream;
  captured.streamId = pStream->pCreate->streamId;
  captured.revision = revision;
  captured.requests =
      pRequests == nullptr ? taosArrayInit(0, sizeof(SStreamRecalcPersistReq)) : taosArrayDup(pRequests, nullptr);
  if (captured.requests == nullptr) return terrno;
  captured.requestMsgType = pTrans->originRpcType;
  for (int32_t i = 0; i < taosArrayGetSize(pTrans->pRpcArray); ++i) {
    const auto *pInfo = static_cast<const SRpcHandleInfo *>(taosArrayGet(pTrans->pRpcArray, i));
    if (pInfo != nullptr) captured.rpcHandles.push_back(*pInfo);
  }
  gRecalcPersistenceHarness->transactions.push_back(captured);
  return TSDB_CODE_SUCCESS;
}

int32_t captureRecalcPrepare(SMnode *, STrans *pTrans) {
  const int32_t code = popHarnessCode(&gRecalcPersistenceHarness->prepareCodes, TSDB_CODE_SUCCESS);
  for (auto iter = gRecalcPersistenceHarness->transactions.begin();
       iter != gRecalcPersistenceHarness->transactions.end(); ++iter) {
    if (iter->key != pTrans) continue;
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
      taosArrayDestroy(iter->requests);
      gRecalcPersistenceHarness->transactions.erase(iter);
      return code;
    }
    iter->callbackParam.resize(pTrans->paramLen);
    if (pTrans->paramLen > 0) std::memcpy(iter->callbackParam.data(), pTrans->param, pTrans->paramLen);
    iter->key = nullptr;
    return code;
  }
  return TSDB_CODE_INTERNAL_ERROR;
}

void ignoreRecalcTransPullup(SMnode *) {}

int32_t captureRecalcResponse(const SRpcMsg *pRsp) {
  if (runtimeReadPreventsWriter()) gRecalcPersistenceHarness->responseWhileRuntimeRead.store(true);
  gRecalcPersistenceHarness->responseCodes.push_back(pRsp->code);
  gRecalcPersistenceHarness->responseHandles.push_back(pRsp->info.handle);
  SStmStatus *pStatus = gRecalcPersistenceHarness->responseProbeStatus;
  if (pStatus != nullptr) {
    taosRLockLatch(&pStatus->userRecalcLock);
    for (int32_t i = 0; i < taosArrayGetSize(pStatus->recalcRecords); ++i) {
      const auto *pRecord = static_cast<const SStmRecalcRecord *>(taosArrayGet(pStatus->recalcRecords, i));
      if (pRecord != nullptr && pRecord->snapshot.recalcId == gRecalcPersistenceHarness->responseProbeRecalcId) {
        gRecalcPersistenceHarness->responseObservedPublished =
            pRecord->visible && !pRecord->hidden && pRecord->snapshot.status == STREAM_RECALC_STATUS_PENDING;
        break;
      }
    }
    taosRUnLockLatch(&pStatus->userRecalcLock);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t captureRecalcQueue(void *, EQueueType qtype, SRpcMsg *pMsg) {
  if (runtimeReadPreventsWriter()) gRecalcPersistenceHarness->scheduleWhileRuntimeRead.store(true);
  EXPECT_EQ(qtype, WRITE_QUEUE);
  EXPECT_EQ(pMsg->msgType, TDMT_MND_TRANS_TIMER);
  if (gRecalcPersistenceHarness->queueCode != TSDB_CODE_SUCCESS) {
    return gRecalcPersistenceHarness->queueCode;
  }
  gRecalcPersistenceHarness->queuedMessages.push_back(*pMsg);
  return TSDB_CODE_SUCCESS;
}

class MndStreamRecalcPersistenceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedStreamMap_ = mStreamMgmt.streamMap;
    savedTaskMap_ = mStreamMgmt.taskMap;
    savedDnodeMap_ = mStreamMgmt.dnodeMap;
    savedActionQ_ = mStreamMgmt.actionQ;
    savedActionQLock_ = mStreamMgmt.actionQLock;
    savedThreadNum_ = mStreamMgmt.threadNum;
    savedThreadCtx_ = mStreamMgmt.tCtx;
    savedPullupPending_ = mStreamMgmt.recalcPullupPending;
    savedActive_ = mStreamMgmt.active;
    savedState_ = mStreamMgmt.state;
    savedInactiveTimes_ = mStreamMgmt.stat.inactiveTimes;
    savedToDeployVgTaskNum_ = mStreamMgmt.toDeployVgTaskNum;
    savedToDeploySnodeTaskNum_ = mStreamMgmt.toDeploySnodeTaskNum;
    mStreamMgmt.streamMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    ASSERT_NE(mStreamMgmt.streamMap, nullptr);
    mStreamMgmt.taskMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    ASSERT_NE(mStreamMgmt.taskMap, nullptr);
    mStreamMgmt.dnodeMap = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    ASSERT_NE(mStreamMgmt.dnodeMap, nullptr);
    heartbeatActionStm_ = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    ASSERT_NE(heartbeatActionStm_, nullptr);
    heartbeatDeployStm_ = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    ASSERT_NE(heartbeatDeployStm_, nullptr);
    taosHashSetFreeFp(heartbeatActionStm_, mstDestroySStmAction);
    taosHashSetFreeFp(heartbeatDeployStm_, tDeepFreeSStmStreamDeploy);
    heartbeatThreadCtx_.actionStm[0] = heartbeatActionStm_;
    heartbeatThreadCtx_.deployStm[0] = heartbeatDeployStm_;
    heartbeatActionQ_.head = static_cast<SStmQNode *>(taosMemoryCalloc(1, sizeof(SStmQNode)));
    ASSERT_NE(heartbeatActionQ_.head, nullptr);
    heartbeatActionQ_.tail = heartbeatActionQ_.head;
    mStreamMgmt.actionQ = &heartbeatActionQ_;
    mStreamMgmt.actionQLock = 0;
    mStreamMgmt.threadNum = 1;
    mStreamMgmt.tCtx = &heartbeatThreadCtx_;
    atomic_store_8(&mStreamMgmt.recalcPullupPending, 0);
    atomic_store_8(&mStreamMgmt.active, 1);
    atomic_store_8(&mStreamMgmt.state, MND_STM_STATE_NORMAL);
    atomic_store_32(&mStreamMgmt.toDeployVgTaskNum, 0);
    atomic_store_32(&mStreamMgmt.toDeploySnodeTaskNum, 0);
    const int32_t dnodeId = 1;
    const int64_t lastUpTs = 0;
    ASSERT_EQ(taosHashPut(mStreamMgmt.dnodeMap, &dnodeId, sizeof(dnodeId), &lastUpTs, sizeof(lastUpTs)),
              TSDB_CODE_SUCCESS);

    SSdbOpt options = {};
    options.path = TD_TMP_DIR_PATH "stream_recalc_persistence";
    options.pMnode = &mnode_;
    taosRemoveDir(options.path);
    pSdb_ = sdbInit(&options);
    ASSERT_NE(pSdb_, nullptr);
    mnode_.pSdb = pSdb_;
    ASSERT_EQ(mndInitStream(&mnode_), TSDB_CODE_SUCCESS);
    ASSERT_EQ(mndInitTrans(&mnode_), TSDB_CODE_SUCCESS);
    mnode_.msgCb.mgmt = &harness_;
    mnode_.msgCb.putToQueueFp = captureRecalcQueue;

    gRecalcPersistenceHarness = &harness_;
    stub_.set(mndStreamCreateTrans, captureRecalcCreateTrans);
    stub_.set(mndStreamTransAppendRecalcUpdate, captureRecalcTransAppend);
    stub_.set(mndTransPrepare, captureRecalcPrepare);
    stub_.set(mndTransPullup, ignoreRecalcTransPullup);
    stub_.set(rpcSendResponse, captureRecalcResponse);

    AddStream(kStreamA, "1.test.recalc_a");
    AddStream(kStreamB, "1.test.recalc_b");
  }

  void TearDown() override {
    stub_.reset(taosArrayDup);
    stub_.reset(rpcSendResponse);
    stub_.reset(mndTransPullup);
    stub_.reset(mndTransPrepare);
    stub_.reset(mndStreamTransAppendRecalcUpdate);
    stub_.reset(mndStreamCreateTrans);

    for (auto &message : harness_.queuedMessages) rpcFreeCont(message.pCont);
    for (auto &transaction : harness_.transactions) taosArrayDestroy(transaction.requests);
    for (const int64_t streamId : streamIds_) {
      auto *pStatus = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId)));
      if (pStatus != nullptr) mstDestroySStmStatus(pStatus);
    }
    SStmQNode *pActionNode = nullptr;
    while (mndStreamActionDequeue(&heartbeatActionQ_, &pActionNode)) {
    }
    taosMemoryFreeClear(heartbeatActionQ_.head);
    heartbeatActionQ_.tail = nullptr;
    taosHashCleanup(heartbeatDeployStm_);
    taosHashCleanup(heartbeatActionStm_);
    taosHashCleanup(mStreamMgmt.dnodeMap);
    taosHashCleanup(mStreamMgmt.taskMap);
    taosHashCleanup(mStreamMgmt.streamMap);
    mStreamMgmt.streamMap = savedStreamMap_;
    mStreamMgmt.taskMap = savedTaskMap_;
    mStreamMgmt.dnodeMap = savedDnodeMap_;
    mStreamMgmt.actionQ = savedActionQ_;
    mStreamMgmt.actionQLock = savedActionQLock_;
    mStreamMgmt.threadNum = savedThreadNum_;
    mStreamMgmt.tCtx = savedThreadCtx_;
    mStreamMgmt.stat.inactiveTimes = savedInactiveTimes_;
    atomic_store_8(&mStreamMgmt.recalcPullupPending, savedPullupPending_);
    atomic_store_8(&mStreamMgmt.active, savedActive_);
    atomic_store_8(&mStreamMgmt.state, savedState_);
    atomic_store_32(&mStreamMgmt.toDeployVgTaskNum, savedToDeployVgTaskNum_);
    atomic_store_32(&mStreamMgmt.toDeploySnodeTaskNum, savedToDeploySnodeTaskNum_);
    if (pSdb_ != nullptr) sdbCleanup(pSdb_);
    pSdb_ = nullptr;
    gRecalcPersistenceHarness = nullptr;
  }

  void AddStream(int64_t streamId, const char *name) {
    SStreamObj stream = BuildMinimalStreamObj();
    ASSERT_NE(stream.pCreate, nullptr);
    taosMemoryFreeClear(stream.pCreate->name);
    stream.pCreate->name = taosStrdup(name);
    stream.pCreate->streamId = streamId;
    tstrncpy(stream.name, name, sizeof(stream.name));
    SSdbRaw *pRaw = mndStreamActionEncode(&stream);
    ASSERT_NE(pRaw, nullptr);
    ASSERT_EQ(sdbSetRawStatus(pRaw, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
    ASSERT_EQ(sdbWrite(pSdb_, pRaw), TSDB_CODE_SUCCESS);
    tFreeStreamObj(&stream);

    SStmStatus status = {};
    ASSERT_EQ(taosHashPut(mStreamMgmt.streamMap, &streamId, sizeof(streamId), &status, sizeof(status)),
              TSDB_CODE_SUCCESS);
    streamIds_.push_back(streamId);
  }

  SSdbRaw *EncodeStreamUpdate(int64_t streamId, uint64_t revision, int64_t recalcId) {
    SStreamObj *pStream = nullptr;
    if (mndAcquireStreamById(&mnode_, streamId, &pStream) != TSDB_CODE_SUCCESS || pStream == nullptr) return nullptr;

    SArray                 *pRequests = taosArrayInit(1, sizeof(SStreamRecalcPersistReq));
    SStreamRecalcPersistReq request = {
        .recalcId = recalcId,
        .start = recalcId * 10,
        .end = recalcId * 10 + 9,
        .requestTimeMs = recalcId * 100,
    };
    if (pRequests == nullptr || taosArrayPush(pRequests, &request) == nullptr) {
      taosArrayDestroy(pRequests);
      mndReleaseStream(&mnode_, pStream);
      return nullptr;
    }

    SStreamObj updated = *pStream;
    updated.recalcRevision = revision;
    updated.pIncompleteRecalcs = pRequests;
    SSdbRaw *pRaw = mndStreamActionEncode(&updated);
    if (pRaw != nullptr && sdbSetRawStatus(pRaw, SDB_STATUS_READY) != TSDB_CODE_SUCCESS) {
      sdbFreeRaw(pRaw);
      pRaw = nullptr;
    }
    taosArrayDestroy(pRequests);
    mndReleaseStream(&mnode_, pStream);
    return pRaw;
  }

  int32_t WriteRaw(SSdbRaw *pRaw) { return sdbWrite(pSdb_, pRaw); }

  void GateSnapshotDup(const SArray *pSource) {
    harness_.snapshotDupSource = pSource;
    harness_.snapshotDupEntered.store(false);
    harness_.snapshotDupHasRuntimeRead.store(false);
    harness_.releaseSnapshotDup.store(false);
    stub_.set(taosArrayDup, gateRecalcSnapshotDup);
  }

  SStmTaskStatus *InstallTriggerTask(int64_t streamId = kStreamA) {
    SStmStatus *pStatus = Status(streamId);
    if (pStatus == nullptr) return nullptr;
    pStatus->triggerTask = static_cast<SStmTaskStatus *>(taosMemoryCalloc(1, sizeof(*pStatus->triggerTask)));
    if (pStatus->triggerTask == nullptr) return nullptr;
    pStatus->triggerTask->type = STREAM_TRIGGER_TASK;
    pStatus->triggerTask->pStream = pStatus;
    pStatus->triggerTask->id.taskId = 100;
    pStatus->triggerTask->id.seriousId = 200;
    pStatus->triggerTask->id.nodeId = 1;
    pStatus->triggerTask->status = STREAM_STATUS_RUNNING;
    SStmTaskStatus *pTrigger = pStatus->triggerTask;
    const int64_t   taskKey[2] = {streamId, pTrigger->id.taskId};
    if (taosHashPut(mStreamMgmt.taskMap, taskKey, sizeof(taskKey), &pTrigger, POINTER_BYTES) != TSDB_CODE_SUCCESS) {
      return nullptr;
    }
    return pStatus->triggerTask;
  }

  int32_t RemovePersistedRecalc(int64_t streamId = kStreamA) {
    SStreamObj *pStream = nullptr;
    int32_t     code = mndAcquireStreamById(&mnode_, streamId, &pStream);
    if (code != TSDB_CODE_SUCCESS || pStream == nullptr) return code;

    SArray *pRequests = taosArrayInit(0, sizeof(SStreamRecalcPersistReq));
    if (pRequests == nullptr) {
      mndReleaseStream(&mnode_, pStream);
      return terrno;
    }
    SStreamObj updated = *pStream;
    updated.recalcRevision = pStream->recalcRevision + 1;
    updated.pIncompleteRecalcs = pRequests;
    SSdbRaw *pRaw = mndStreamActionEncode(&updated);
    taosArrayDestroy(pRequests);
    mndReleaseStream(&mnode_, pStream);
    if (pRaw == nullptr) return terrno;
    code = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    if (code != TSDB_CODE_SUCCESS) {
      sdbFreeRaw(pRaw);
      return code;
    }
    return sdbWrite(pSdb_, pRaw);
  }

  SArray *BuildTerminalHeartbeat(int64_t recalcId, SStreamHbMsg *pHeartbeat) {
    pHeartbeat->streamGId = 0;
    pHeartbeat->dnodeId = 1;
    pHeartbeat->observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
    pHeartbeat->pVgLeaders = taosArrayInit(0, sizeof(int32_t));
    pHeartbeat->pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
    pHeartbeat->pStreamReq = taosArrayInit(0, sizeof(int32_t));
    pHeartbeat->pTriggerStatus = taosArrayInit(0, sizeof(SSTriggerRuntimeStatus));
    pHeartbeat->pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
    SArray *pSnapshots = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
    if (pHeartbeat->pVgLeaders == nullptr || pHeartbeat->pStreamStatus == nullptr ||
        pHeartbeat->pStreamReq == nullptr || pHeartbeat->pTriggerStatus == nullptr ||
        pHeartbeat->pTaskMetrics == nullptr || pSnapshots == nullptr) {
      taosArrayDestroy(pSnapshots);
      tCleanupStreamHbMsg(pHeartbeat, true);
      return nullptr;
    }

    SStmTaskStatusMsg task = {};
    task.type = STREAM_TRIGGER_TASK;
    task.streamId = kStreamA;
    task.taskId = 100;
    task.seriousId = 200;
    task.nodeId = 1;
    task.status = STREAM_STATUS_RUNNING;
    task.detailStatus = -1;
    SStreamRecalcSnapshot snapshot = {
        .recalcId = recalcId,
        .start = 100,
        .end = 200,
        .progressPct = 100,
        .status = STREAM_RECALC_STATUS_FINISHED,
    };
    SStreamTaskMetricsEntry entry = {};
    entry.taskStatusIndex = 0;
    entry.streamId = kStreamA;
    entry.taskId = 100;
    entry.seriousId = 200;
    entry.decodeCode = TSDB_CODE_SUCCESS;
    entry.recalcDetailState = STREAM_RECALC_DETAIL_ABSENT;
    entry.snapshot.applicableMask = STREAM_METRIC_RECALCULATES;
    entry.snapshot.validMask = STREAM_METRIC_RECALCULATES;
    entry.snapshot.pRecalculates = pSnapshots;
    if (taosArrayPush(pHeartbeat->pStreamStatus, &task) == nullptr || taosArrayPush(pSnapshots, &snapshot) == nullptr) {
      taosArrayDestroy(pSnapshots);
      tCleanupStreamHbMsg(pHeartbeat, true);
      return nullptr;
    }
    if (taosArrayPush(pHeartbeat->pTaskMetrics, &entry) == nullptr) {
      taosArrayDestroy(pSnapshots);
      tCleanupStreamHbMsg(pHeartbeat, true);
      return nullptr;
    }
    return pSnapshots;
  }

  int32_t HandleHeartbeat(SStreamHbMsg *pHeartbeat) {
    SRpcMsg request = {};
    SRpcMsg response = {};
    request.info.node = &mnode_;
    const int32_t code = msmHandleStreamHbMsg(&mnode_, 100, pHeartbeat, &request, &response);
    rpcFreeCont(response.pCont);
    return code;
  }

  void AddSelectiveClearRecords() {
    const SStmRecalcRecord agedLegacy = {
        .snapshot =
            {
                .recalcId = kAgedLegacyRecalcId,
                .start = 300,
                .end = 400,
                .progressPct = 100,
                .status = STREAM_RECALC_STATUS_FINISHED,
            },
        .terminalObservedAtMs = 1,
        .typedStatusKnown = true,
        .hidden = true,
    };
    const SStmRecalcRecord evictedLegacy = {
        .snapshot =
            {
                .recalcId = kEvictedLegacyRecalcId,
                .start = 500,
                .end = 600,
                .progressPct = 100,
                .status = STREAM_RECALC_STATUS_FINISHED,
            },
        .terminalObservedAtMs = 2,
        .typedStatusKnown = true,
        .hidden = true,
    };
    const SStmRecalcRecord visibleCoordinator = {
        .snapshot =
            {
                .recalcId = kVisibleCoordinatorRecalcId,
                .start = 700,
                .end = 800,
                .progressPct = 0,
                .status = STREAM_RECALC_STATUS_PENDING,
            },
        .requestTimeMs = 1000,
        .typedStatusKnown = true,
        .visible = true,
    };
    const SStmRecalcRecord terminalCoordinator = {
        .snapshot =
            {
                .recalcId = kTerminalCoordinatorRecalcId,
                .start = 900,
                .end = 1000,
                .progressPct = 100,
                .status = STREAM_RECALC_STATUS_FINISHED,
            },
        .requestTimeMs = 2000,
        .terminalObservedAtMs = 3000,
        .typedStatusKnown = true,
        .visible = true,
    };
    const SStmRecalcRecord persistingCoordinator = {
        .snapshot =
            {
                .recalcId = kPersistingCoordinatorRecalcId,
                .start = 1100,
                .end = 1200,
                .progressPct = 80,
                .status = STREAM_RECALC_STATUS_RUNNING,
            },
        .terminalCandidate =
            {
                .recalcId = kPersistingCoordinatorRecalcId,
                .start = 1100,
                .end = 1200,
                .progressPct = 100,
                .status = STREAM_RECALC_STATUS_FINISHED,
            },
        .requestTimeMs = 4000,
        .typedStatusKnown = true,
        .visible = true,
        .terminalPersisting = true,
        .terminalCandidateValid = true,
    };
    ASSERT_NE(taosArrayPush(Status()->recalcRecords, &agedLegacy), nullptr);
    ASSERT_NE(taosArrayPush(Status()->recalcRecords, &evictedLegacy), nullptr);
    ASSERT_NE(taosArrayPush(Status()->recalcRecords, &visibleCoordinator), nullptr);
    ASSERT_NE(taosArrayPush(Status()->recalcRecords, &terminalCoordinator), nullptr);
    ASSERT_NE(taosArrayPush(Status()->recalcRecords, &persistingCoordinator), nullptr);
  }

  SStmStatus *Status(int64_t streamId = kStreamA) {
    return static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId)));
  }

  int32_t Accept(int64_t streamId, const STimeWindow &range, intptr_t handle) {
    SStreamObj *pStream = nullptr;
    if (mndAcquireStreamById(&mnode_, streamId, &pStream) != TSDB_CODE_SUCCESS || pStream == nullptr) {
      return TSDB_CODE_MND_STREAM_NOT_EXIST;
    }
    auto *pStatus = static_cast<SStmStatus *>(taosHashAcquire(mStreamMgmt.streamMap, &streamId, sizeof(streamId)));
    if (pStatus == nullptr) {
      mndReleaseStream(&mnode_, pStream);
      return TSDB_CODE_MND_STREAM_NOT_EXIST;
    }
    SRpcMsg request = {};
    request.info.node = &mnode_;
    request.info.handle = reinterpret_cast<void *>(handle);
    const int32_t code = mndStreamRecalcAccept(&mnode_, pStream, pStatus, &range, &request);
    taosHashRelease(mStreamMgmt.streamMap, pStatus);
    mndReleaseStream(&mnode_, pStream);
    return code;
  }

  int32_t AcceptA(const STimeWindow &range, intptr_t handle = 1) { return Accept(kStreamA, range, handle); }

  SStmRecalcRecord *Record(int64_t streamId, int64_t recalcId) {
    SStmStatus *pStatus = Status(streamId);
    for (int32_t i = 0; pStatus != nullptr && i < taosArrayGetSize(pStatus->recalcRecords); ++i) {
      auto *pRecord = static_cast<SStmRecalcRecord *>(taosArrayGet(pStatus->recalcRecords, i));
      if (pRecord->snapshot.recalcId == recalcId) return pRecord;
    }
    return nullptr;
  }

  SStmRecalcRecord *OnlyRecord(int64_t streamId = kStreamA) {
    SStmStatus *pStatus = Status(streamId);
    return pStatus == nullptr || taosArrayGetSize(pStatus->recalcRecords) != 1
               ? nullptr
               : static_cast<SStmRecalcRecord *>(taosArrayGet(pStatus->recalcRecords, 0));
  }

  int32_t VisibleCount(int64_t streamId = kStreamA) {
    int32_t     count = 0;
    SStmStatus *pStatus = Status(streamId);
    for (int32_t i = 0; pStatus != nullptr && i < taosArrayGetSize(pStatus->recalcRecords); ++i) {
      const auto *pRecord = static_cast<const SStmRecalcRecord *>(taosArrayGet(pStatus->recalcRecords, i));
      if (pRecord->visible && !pRecord->hidden) ++count;
    }
    return count;
  }

  int32_t DispatchableCount(int64_t streamId = kStreamA) {
    int32_t     count = 0;
    SStmStatus *pStatus = Status(streamId);
    for (int32_t i = 0; pStatus != nullptr && i < taosArrayGetSize(pStatus->recalcRecords); ++i) {
      const auto *pRecord = static_cast<const SStmRecalcRecord *>(taosArrayGet(pStatus->recalcRecords, i));
      if (pRecord->visible && !pRecord->hidden && !pRecord->dispatchConfirmed && !pRecord->terminalPersisting) {
        ++count;
      }
    }
    return count;
  }

  int64_t FirstCapturedRecalcId() const {
    if (harness_.transactions.empty()) return 0;
    const auto *pRequest =
        static_cast<const SStreamRecalcPersistReq *>(taosArrayGet(harness_.transactions.front().requests, 0));
    return pRequest == nullptr ? 0 : pRequest->recalcId;
  }

  static bool CapturedRequestExists(const CapturedRecalcTransaction &captured, int64_t recalcId) {
    for (int32_t i = 0; i < taosArrayGetSize(captured.requests); ++i) {
      const auto *pRequest = static_cast<const SStreamRecalcPersistReq *>(taosArrayGet(captured.requests, i));
      if (pRequest != nullptr && pRequest->recalcId == recalcId) return true;
    }
    return false;
  }

  bool PersistedRequestExists(int64_t streamId, int64_t recalcId) {
    SStreamObj *pStream = nullptr;
    if (mndAcquireStreamById(&mnode_, streamId, &pStream) != TSDB_CODE_SUCCESS || pStream == nullptr) return false;
    bool found = false;
    for (int32_t i = 0; i < taosArrayGetSize(pStream->pIncompleteRecalcs); ++i) {
      const auto *pRequest = static_cast<const SStreamRecalcPersistReq *>(taosArrayGet(pStream->pIncompleteRecalcs, i));
      if (pRequest->recalcId == recalcId) found = true;
    }
    mndReleaseStream(&mnode_, pStream);
    return found;
  }

  void ApplyCapturedStream(const CapturedRecalcTransaction &captured) {
    SStreamObj *pStream = nullptr;
    ASSERT_EQ(mndAcquireStreamById(&mnode_, captured.streamId, &pStream), TSDB_CODE_SUCCESS);
    ASSERT_NE(pStream, nullptr);
    SStreamObj updated = *pStream;
    updated.recalcRevision = captured.revision;
    updated.pIncompleteRecalcs = captured.requests;
    SSdbRaw *pRaw = mndStreamActionEncode(&updated);
    ASSERT_NE(pRaw, nullptr);
    ASSERT_EQ(sdbSetRawStatus(pRaw, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
    ASSERT_EQ(sdbWrite(pSdb_, pRaw), TSDB_CODE_SUCCESS);
    mndReleaseStream(&mnode_, pStream);
  }

  void InvokeRegisteredStop(const CapturedRecalcTransaction &captured) {
    STrans *pTrans = mndTransCreate(&mnode_, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, nullptr, "recalc-test-stop");
    ASSERT_NE(pTrans, nullptr);
    pTrans->stage = TRN_STAGE_FINISH;
    void *pParam = taosMemoryMalloc(captured.callbackParam.size());
    ASSERT_NE(pParam, nullptr);
    std::memcpy(pParam, captured.callbackParam.data(), captured.callbackParam.size());
    mndTransSetCb(pTrans, static_cast<ETrnFunc>(0), TRANS_STOP_FUNC_STREAM_RECALC, pParam,
                  static_cast<int32_t>(captured.callbackParam.size()));
    const int32_t transId = pTrans->id;
    SSdbRaw      *pRaw = mndTransEncode(pTrans);
    mndTransDrop(pTrans);
    ASSERT_NE(pRaw, nullptr);
    ASSERT_EQ(sdbSetRawStatus(pRaw, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
    ASSERT_EQ(sdbWrite(pSdb_, pRaw), TSDB_CODE_SUCCESS);
    STrans *pHeld = mndAcquireTrans(&mnode_, transId);
    ASSERT_NE(pHeld, nullptr);
    mndTransRefresh(&mnode_, pHeld);
    mndReleaseTrans(&mnode_, pHeld);
  }

  void CommitNext(bool registeredStop = false) {
    ASSERT_FALSE(harness_.transactions.empty());
    CapturedRecalcTransaction captured = harness_.transactions.front();
    harness_.transactions.pop_front();
    ApplyCapturedStream(captured);
    if (registeredStop) {
      InvokeRegisteredStop(captured);
    } else {
      mndStreamRecalcTransStopped(&mnode_, captured.callbackParam.data(), captured.callbackParam.size());
    }
    taosArrayDestroy(captured.requests);
  }

  void StopNextWithoutCommit() {
    ASSERT_FALSE(harness_.transactions.empty());
    CapturedRecalcTransaction captured = harness_.transactions.front();
    harness_.transactions.pop_front();
    mndStreamRecalcTransStopped(&mnode_, captured.callbackParam.data(), captured.callbackParam.size());
    taosArrayDestroy(captured.requests);
  }

  void LoseNextAfterApplying() {
    ASSERT_FALSE(harness_.transactions.empty());
    CapturedRecalcTransaction captured = harness_.transactions.front();
    harness_.transactions.pop_front();
    ApplyCapturedStream(captured);
    taosWLockLatch(&Status()->userRecalcLock);
    Status()->recalcTransActive = false;
    taosWUnLockLatch(&Status()->userRecalcLock);
    taosArrayDestroy(captured.requests);
    atomic_store_8(&mStreamMgmt.recalcPullupPending, 1);
  }

  void DestroyStatus(int64_t streamId = kStreamA) {
    mstDestroySStmStatus(Status(streamId));
    streamIds_.erase(std::remove(streamIds_.begin(), streamIds_.end(), streamId), streamIds_.end());
  }

  STrans *PersistAndAcquireFinishedRecalc(const CapturedRecalcTransaction &captured, const SRpcMsg &request) {
    STrans *pTrans = mndTransCreate(&mnode_, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, nullptr, "held-recalc");
    if (pTrans == nullptr) return nullptr;
    pTrans->stage = TRN_STAGE_FINISH;
    void *pParam = taosMemoryMalloc(captured.callbackParam.size());
    if (pParam == nullptr) {
      mndTransDrop(pTrans);
      return nullptr;
    }
    std::memcpy(pParam, captured.callbackParam.data(), captured.callbackParam.size());
    mndTransSetCb(pTrans, static_cast<ETrnFunc>(0), TRANS_STOP_FUNC_STREAM_RECALC, pParam,
                  static_cast<int32_t>(captured.callbackParam.size()));

    const int32_t transId = pTrans->id;
    SSdbRaw      *pRaw = mndTransEncode(pTrans);
    mndTransDrop(pTrans);
    if (pRaw == nullptr) return nullptr;
    if (sdbSetRawStatus(pRaw, SDB_STATUS_READY) != TSDB_CODE_SUCCESS) {
      sdbFreeRaw(pRaw);
      return nullptr;
    }
    if (sdbWrite(pSdb_, pRaw) != TSDB_CODE_SUCCESS) return nullptr;

    STrans *pHeld = mndAcquireTrans(&mnode_, transId);
    if (pHeld == nullptr) return nullptr;
    if (pHeld->pRpcArray == nullptr) pHeld->pRpcArray = taosArrayInit(1, sizeof(SRpcHandleInfo));
    if (pHeld->pRpcArray == nullptr || taosArrayPush(pHeld->pRpcArray, &request.info) == nullptr) {
      mndReleaseTrans(&mnode_, pHeld);
      return nullptr;
    }
    pHeld->originRpcType = request.msgType;
    return pHeld;
  }

  void RunImmediateWake() {
    ASSERT_FALSE(harness_.queuedMessages.empty());
    SRpcMsg message = harness_.queuedMessages.front();
    harness_.queuedMessages.pop_front();
    MndMsgFp handler = mnode_.msgFp[TMSG_INDEX(TDMT_MND_TRANS_TIMER)];
    ASSERT_NE(handler, nullptr);
    ASSERT_EQ(handler(&message), TSDB_CODE_SUCCESS);
    rpcFreeCont(message.pCont);
  }

  void RunPeriodicTimer() {
    SRpcMsg message = {};
    message.msgType = TDMT_MND_TRANS_TIMER;
    message.info.node = &mnode_;
    MndMsgFp handler = mnode_.msgFp[TMSG_INDEX(TDMT_MND_TRANS_TIMER)];
    ASSERT_NE(handler, nullptr);
    ASSERT_EQ(handler(&message), TSDB_CODE_SUCCESS);
  }

  static constexpr int64_t kStreamA = 42;
  static constexpr int64_t kStreamB = 43;
  static constexpr int64_t kAgedLegacyRecalcId = 0x7001;
  static constexpr int64_t kEvictedLegacyRecalcId = 0x7002;
  static constexpr int64_t kVisibleCoordinatorRecalcId = 0x8001;
  static constexpr int64_t kTerminalCoordinatorRecalcId = 0x8002;
  static constexpr int64_t kPersistingCoordinatorRecalcId = 0x8003;
  RecalcPersistenceHarness harness_;
  SMnode                   mnode_ = {};

 private:
  Stub                 stub_;
  SSdb                *pSdb_ = nullptr;
  SHashObj            *savedStreamMap_ = nullptr;
  SHashObj            *savedTaskMap_ = nullptr;
  SHashObj            *savedDnodeMap_ = nullptr;
  SStmActionQ         *savedActionQ_ = nullptr;
  SRWLatch             savedActionQLock_ = 0;
  int32_t              savedThreadNum_ = 0;
  SStmThreadCtx       *savedThreadCtx_ = nullptr;
  int64_t              savedInactiveTimes_ = 0;
  int8_t               savedPullupPending_ = 0;
  int8_t               savedActive_ = 0;
  int8_t               savedState_ = 0;
  int32_t              savedToDeployVgTaskNum_ = 0;
  int32_t              savedToDeploySnodeTaskNum_ = 0;
  SStmActionQ          heartbeatActionQ_ = {};
  SStmThreadCtx        heartbeatThreadCtx_ = {};
  SHashObj            *heartbeatActionStm_ = nullptr;
  SHashObj            *heartbeatDeployStm_ = nullptr;
  std::vector<int64_t> streamIds_;
};

struct CapturedLifecycleTransaction {
  ETrnConflct          conflict = TRN_CONFLICT_NOTHING;
  std::string          dbname;
  std::string          objectName;
  int32_t              stopFunc = 0;
  int32_t              commitCount = 0;
  int8_t               rawStatus = SDB_STATUS_READY;
  SStreamObj           stream = {};
  std::vector<uint8_t> callbackParam;
};

struct LifecycleRuntimeAction {
  int64_t streamId = 0;
  int32_t action = 0;
  int8_t  committedUserStopped = -1;
};

struct LifecycleTransactionHarness {
  std::deque<int32_t>                      prepareCodes;
  std::deque<CapturedLifecycleTransaction> transactions;
  std::vector<LifecycleRuntimeAction>      runtimeActions;
  std::vector<int32_t>                     observedEvents;
  std::atomic<int32_t>                     callbackCount{0};
  std::atomic<int32_t>                     responseCount{0};
  std::mutex                               callbackMutex;
  std::condition_variable                  callbackCv;
  bool                                     blockCallback = false;
  bool                                     callbackEntered = false;
  bool                                     releaseCallback = false;
  bool                                     callbackWithoutTransLock = false;
};

enum ELifecycleObservedEvent {
  LIFECYCLE_RUNTIME_PUBLISHED = 1,
  LIFECYCLE_RESPONSE_SENT = 2,
};

LifecycleTransactionHarness *gLifecycleHarness = nullptr;
SUserObj                     gLifecycleUser = {};

struct FinishWriteFailureHarness {
  Stub   *stub = nullptr;
  int32_t calls = 0;
};

FinishWriteFailureHarness *gFinishWriteFailureHarness = nullptr;

struct RecalcPublicationGateState {
  const SArray     *target = nullptr;
  bool              probeRuntimeRead = true;
  std::atomic<bool> entered{false};
  std::atomic<bool> hasRuntimeRead{false};
  std::atomic<bool> release{false};
};

RecalcPublicationGateState *gRecalcPublicationGate = nullptr;

void *gateRecalcPublicationArrayGet(const SArray *pArray, size_t index) {
  RecalcPublicationGateState *pGate = gRecalcPublicationGate;
  if (pGate != nullptr && pArray == pGate->target && !pGate->entered.exchange(true)) {
    if (pGate->probeRuntimeRead) {
      if (taosWTryLockLatch(&mStreamMgmt.runtimeLock) == 0) {
        taosWUnLockLatch(&mStreamMgmt.runtimeLock);
      } else {
        pGate->hasRuntimeRead.store(true);
      }
    }
    while (!pGate->release.load()) std::this_thread::yield();
  }
  if (pArray == nullptr || index >= pArray->size) return nullptr;
  return TARRAY_GET_ELEM(pArray, index);
}

class RecalcPublicationGate {
 public:
  explicit RecalcPublicationGate(const SArray *pTarget, bool probeRuntimeRead = true) {
    state_.target = pTarget;
    state_.probeRuntimeRead = probeRuntimeRead;
    gRecalcPublicationGate = &state_;
    stub_.set(taosArrayGet, gateRecalcPublicationArrayGet);
  }

  ~RecalcPublicationGate() {
    Release();
    stub_.reset(taosArrayGet);
    gRecalcPublicationGate = nullptr;
  }

  bool WaitUntilEntered() const {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!state_.entered.load() && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
    return state_.entered.load();
  }

  bool HasRuntimeRead() const { return state_.hasRuntimeRead.load(); }
  void Release() { state_.release.store(true); }

 private:
  RecalcPublicationGateState state_;
  Stub                       stub_;
};

int32_t failNextFinishWrite(SSdb *, SSdbRaw *pRaw) {
  ++gFinishWriteFailureHarness->calls;
  gFinishWriteFailureHarness->stub->reset(sdbWrite);
  sdbFreeRaw(pRaw);
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return TSDB_CODE_OUT_OF_MEMORY;
}

struct TransCreateFailureHarness {
  Stub   *stub = nullptr;
  void   *transAllocation = nullptr;
  bool    transFreed = false;
  int32_t addBatchCalls = 0;
};

TransCreateFailureHarness *gTransCreateFailureHarness = nullptr;

void *captureTransCreateCalloc(int64_t num, int64_t size) {
  gTransCreateFailureHarness->stub->reset(taosMemCalloc);
  void *pAllocation = taosMemCalloc(num, size);
  gTransCreateFailureHarness->stub->set(taosMemCalloc, captureTransCreateCalloc);
  if (num == 1 && size == sizeof(STrans) && gTransCreateFailureHarness->transAllocation == nullptr) {
    gTransCreateFailureHarness->transAllocation = pAllocation;
  }
  return pAllocation;
}

void captureTransCreateFree(void *pAllocation) {
  if (pAllocation == gTransCreateFailureHarness->transAllocation) {
    gTransCreateFailureHarness->transFreed = true;
  }
  gTransCreateFailureHarness->stub->reset(taosMemFree);
  taosMemFree(pAllocation);
  gTransCreateFailureHarness->stub->set(taosMemFree, captureTransCreateFree);
}

void *failTransRpcAddBatch(SArray *pArray, const void *pData, int32_t num) {
  if (pArray->elemSize != sizeof(SRpcHandleInfo)) {
    gTransCreateFailureHarness->stub->reset(taosArrayAddBatch);
    void *pResult = taosArrayAddBatch(pArray, pData, num);
    gTransCreateFailureHarness->stub->set(taosArrayAddBatch, failTransRpcAddBatch);
    return pResult;
  }
  ++gTransCreateFailureHarness->addBatchCalls;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

int32_t allowLifecycleAcquireUser(SMnode *, const char *, SUserObj **ppUser) {
  *ppUser = &gLifecycleUser;
  return TSDB_CODE_SUCCESS;
}

void ignoreLifecycleReleaseUser(SMnode *, SUserObj *) {}

int32_t allowLifecycleDbPrivilege(SMnode *, const char *, const char *, EOperType, const char *, bool) {
  return TSDB_CODE_SUCCESS;
}

int32_t allowLifecycleObjectPrivilege(SMnode *, SUserObj *, EPrivType, EPrivObjType, int64_t, const char *,
                                      const char *) {
  return TSDB_CODE_SUCCESS;
}

int32_t captureLifecyclePrepare(SMnode *, STrans *pTrans) {
  CapturedLifecycleTransaction captured;
  captured.conflict = pTrans->conflict;
  captured.dbname = pTrans->dbname;
  captured.objectName = pTrans->stbname;
  captured.stopFunc = pTrans->stopFunc;
  captured.commitCount = taosArrayGetSize(pTrans->commitActions);
  if (captured.commitCount > 0) {
    const auto *pAction = static_cast<const STransAction *>(taosArrayGet(pTrans->commitActions, 0));
    if (pAction != nullptr && pAction->pRaw != nullptr) {
      captured.rawStatus = pAction->pRaw->status;
      captured.stream = DecodeStreamRaw(pAction->pRaw);
    }
  }
  captured.callbackParam.resize(pTrans->paramLen);
  if (pTrans->paramLen > 0) {
    std::memcpy(captured.callbackParam.data(), pTrans->param, pTrans->paramLen);
  }
  gLifecycleHarness->transactions.push_back(captured);
  return popHarnessCode(&gLifecycleHarness->prepareCodes, TSDB_CODE_ACTION_IN_PROGRESS);
}

void captureLifecycleUndeploy(SMnode *pMnode, int64_t streamId, char *) {
  ++gLifecycleHarness->callbackCount;
  {
    std::unique_lock<std::mutex> lock(gLifecycleHarness->callbackMutex);
    if (taosThreadRwlockTryWrlock(&pMnode->pSdb->locks[SDB_TRANS]) == 0) {
      gLifecycleHarness->callbackWithoutTransLock = true;
      taosThreadRwlockUnlock(&pMnode->pSdb->locks[SDB_TRANS]);
    }
    if (gLifecycleHarness->blockCallback) {
      gLifecycleHarness->callbackEntered = true;
      gLifecycleHarness->callbackCv.notify_all();
      gLifecycleHarness->callbackCv.wait(lock, []() { return gLifecycleHarness->releaseCallback; });
    }
  }

  LifecycleRuntimeAction captured;
  captured.streamId = streamId;
  captured.action = -1;
  SStreamObj *pStream = nullptr;
  if (mndAcquireStreamById(pMnode, streamId, &pStream) == TSDB_CODE_SUCCESS && pStream != nullptr) {
    captured.committedUserStopped = atomic_load_8(&pStream->userStopped);
    mndReleaseStream(pMnode, pStream);
  }
  gLifecycleHarness->runtimeActions.push_back(captured);
  gLifecycleHarness->observedEvents.push_back(LIFECYCLE_RUNTIME_PUBLISHED);
}

int32_t captureLifecyclePost(SStmActionQ *, int64_t streamId, char *, void *, bool, int32_t action) {
  LifecycleRuntimeAction captured;
  captured.streamId = streamId;
  captured.action = action;
  gLifecycleHarness->runtimeActions.push_back(captured);
  gLifecycleHarness->observedEvents.push_back(LIFECYCLE_RUNTIME_PUBLISHED);
  return TSDB_CODE_SUCCESS;
}

void captureLifecycleResponse(SRpcMsg *) {
  ++gLifecycleHarness->responseCount;
  gLifecycleHarness->observedEvents.push_back(LIFECYCLE_RESPONSE_SENT);
}

class StreamLifecycleTransactionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedActive_ = atomic_load_8(&mStreamMgmt.active);
    savedState_ = atomic_load_8(&mStreamMgmt.state);
    savedInactiveTimes_ = mStreamMgmt.stat.inactiveTimes;
    savedActionQ_ = mStreamMgmt.actionQ;
    savedStreamMap_ = mStreamMgmt.streamMap;
    atomic_store_8(&mStreamMgmt.active, 1);
    atomic_store_8(&mStreamMgmt.state, MND_STM_STATE_NORMAL);
    mStreamMgmt.actionQ = static_cast<SStmActionQ *>(taosMemoryCalloc(1, sizeof(SStmActionQ)));
    ASSERT_NE(mStreamMgmt.actionQ, nullptr);
    mStreamMgmt.actionQ->head = static_cast<SStmQNode *>(taosMemoryCalloc(1, sizeof(SStmQNode)));
    ASSERT_NE(mStreamMgmt.actionQ->head, nullptr);
    mStreamMgmt.actionQ->tail = mStreamMgmt.actionQ->head;
    mStreamMgmt.streamMap = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    ASSERT_NE(mStreamMgmt.streamMap, nullptr);

    savedAuditLevel_ = tsAuditLevel;
    tsAuditLevel = AUDIT_LEVEL_NONE;

    SSdbOpt options = {};
    options.path = TD_TMP_DIR_PATH "stream_lifecycle_transaction";
    options.pMnode = &mnode_;
    taosRemoveDir(options.path);
    pSdb_ = sdbInit(&options);
    ASSERT_NE(pSdb_, nullptr);
    mnode_.pSdb = pSdb_;
    ASSERT_EQ(mndInitStream(&mnode_), TSDB_CODE_SUCCESS);
    ASSERT_EQ(mndInitTrans(&mnode_), TSDB_CODE_SUCCESS);

    gLifecycleHarness = &harness_;
    stub_.set(mndAcquireUser, allowLifecycleAcquireUser);
    stub_.set(mndReleaseUser, ignoreLifecycleReleaseUser);
    stub_.set(mndCheckDbPrivilegeByName, allowLifecycleDbPrivilege);
    stub_.set(mndCheckObjPrivilegeRecF, allowLifecycleObjectPrivilege);
    stub_.set(mndTransPrepare, captureLifecyclePrepare);
    stub_.set(msmUndeployStream, captureLifecycleUndeploy);
    stub_.set(mstPostStreamAction, captureLifecyclePost);
    stub_.set(tmsgSendRsp, captureLifecycleResponse);

    ASSERT_TRUE(WriteStream(kStreamA, kStreamAName, 0));
    ASSERT_TRUE(WriteStream(kStreamB, kStreamBName, 0));
  }

  void TearDown() override {
    stub_.reset(tmsgSendRsp);
    stub_.reset(mstPostStreamAction);
    stub_.reset(msmUndeployStream);
    stub_.reset(mndTransPrepare);
    stub_.reset(mndCheckObjPrivilegeRecF);
    stub_.reset(mndCheckDbPrivilegeByName);
    stub_.reset(mndReleaseUser);
    stub_.reset(mndAcquireUser);
    for (auto &captured : harness_.transactions) tFreeStreamObj(&captured.stream);
    if (pSdb_ != nullptr) sdbCleanup(pSdb_);
    pSdb_ = nullptr;
    gLifecycleHarness = nullptr;
    tsAuditLevel = savedAuditLevel_;

    SStmQNode *pNode = nullptr;
    while (mStreamMgmt.actionQ != nullptr && mndStreamActionDequeue(mStreamMgmt.actionQ, &pNode)) {
    }
    if (mStreamMgmt.actionQ != nullptr) {
      taosMemoryFreeClear(mStreamMgmt.actionQ->head);
      taosMemoryFreeClear(mStreamMgmt.actionQ);
    }
    taosHashCleanup(mStreamMgmt.streamMap);
    mStreamMgmt.actionQ = savedActionQ_;
    mStreamMgmt.streamMap = savedStreamMap_;
    mStreamMgmt.stat.inactiveTimes = savedInactiveTimes_;
    atomic_store_8(&mStreamMgmt.state, savedState_);
    atomic_store_8(&mStreamMgmt.active, savedActive_);
  }

  bool WriteStream(int64_t streamId, const char *name, int8_t userStopped) {
    SStreamObj stream = BuildMinimalStreamObj();
    if (stream.pCreate == nullptr) return false;
    taosMemoryFreeClear(stream.pCreate->name);
    taosMemoryFreeClear(stream.pCreate->outTblName);
    stream.pCreate->name = taosStrdup(name);
    stream.pCreate->outTblName = taosStrdup(kSharedOutputName);
    stream.pCreate->streamId = streamId;
    stream.userStopped = userStopped;
    tstrncpy(stream.name, name, sizeof(stream.name));
    SSdbRaw *pRaw = mndStreamActionEncode(&stream);
    bool     ok = pRaw != nullptr && sdbSetRawStatus(pRaw, SDB_STATUS_READY) == TSDB_CODE_SUCCESS &&
              sdbWrite(pSdb_, pRaw) == TSDB_CODE_SUCCESS;
    if (pRaw != nullptr && !ok) sdbFreeRaw(pRaw);
    tFreeStreamObj(&stream);
    return ok;
  }

  int8_t UserStopped(int64_t streamId = kStreamA) {
    SStreamObj *pStream = nullptr;
    if (mndAcquireStreamById(&mnode_, streamId, &pStream) != TSDB_CODE_SUCCESS || pStream == nullptr) return -1;
    int8_t userStopped = atomic_load_8(&pStream->userStopped);
    mndReleaseStream(&mnode_, pStream);
    return userStopped;
  }

  int8_t UserDropped(int64_t streamId = kStreamA) {
    SStreamObj *pStream = nullptr;
    if (mndAcquireStreamById(&mnode_, streamId, &pStream) != TSDB_CODE_SUCCESS || pStream == nullptr) return -1;
    int8_t userDropped = atomic_load_8(&pStream->userDropped);
    mndReleaseStream(&mnode_, pStream);
    return userDropped;
  }

  bool StreamExists(int64_t streamId = kStreamA) {
    SStreamObj *pStream = nullptr;
    bool        exists = mndAcquireStreamById(&mnode_, streamId, &pStream) == TSDB_CODE_SUCCESS && pStream != nullptr;
    if (pStream != nullptr) mndReleaseStream(&mnode_, pStream);
    return exists;
  }

  int32_t SendPause(const char *name) {
    SMPauseStreamReq request = {};
    request.name = const_cast<char *>(name);
    return SendRequest(TDMT_MND_STOP_STREAM, tSerializeSMPauseStreamReq, &request);
  }

  int32_t SendResume(const char *name) {
    SMResumeStreamReq request = {};
    request.name = const_cast<char *>(name);
    return SendRequest(TDMT_MND_START_STREAM, tSerializeSMResumeStreamReq, &request);
  }

  int32_t SendDrop(std::initializer_list<const char *> names) {
    std::vector<char *> mutableNames;
    for (const char *name : names) mutableNames.push_back(const_cast<char *>(name));
    SMDropStreamReq request = {
        .name = mutableNames.data(),
        .count = static_cast<int32_t>(mutableNames.size()),
    };
    return SendRequest(TDMT_MND_DROP_STREAM, tSerializeSMDropStreamReq, &request);
  }

  template <typename Request>
  int32_t SendRequest(tmsg_t msgType, int32_t (*serializer)(void *, int32_t, const Request *), Request *pRequest) {
    const int32_t length = serializer(nullptr, 0, pRequest);
    if (length <= 0) return length;
    std::vector<uint8_t> content(length);
    if (serializer(content.data(), length, pRequest) != length) return TSDB_CODE_INVALID_MSG;
    SRpcMsg rpc = {};
    rpc.msgType = msgType;
    rpc.pCont = content.data();
    rpc.contLen = length;
    rpc.info.node = &mnode_;
    MndMsgFp handler = mnode_.msgFp[TMSG_INDEX(msgType)];
    return handler == nullptr ? TSDB_CODE_MSG_NOT_PROCESSED : handler(&rpc);
  }

  bool RegisterActiveLifecycle(const char *streamName) {
    STrans *pTrans = mndTransCreate(&mnode_, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, nullptr, "active-stream");
    if (pTrans == nullptr) return false;
    mndTransSetDbName(pTrans, kStreamDb, streamName);
    SSdbRaw *pRaw = mndTransEncode(pTrans);
    mndTransDrop(pTrans);
    if (pRaw == nullptr) return false;
    if (sdbSetRawStatus(pRaw, SDB_STATUS_READY) != TSDB_CODE_SUCCESS) {
      sdbFreeRaw(pRaw);
      return false;
    }
    return sdbWrite(pSdb_, pRaw) == TSDB_CODE_SUCCESS;
  }

  CapturedLifecycleTransaction TakeTransaction() {
    CapturedLifecycleTransaction captured = harness_.transactions.front();
    harness_.transactions.pop_front();
    return captured;
  }

  bool ApplyCommittedStream(const CapturedLifecycleTransaction &captured) {
    SSdbRaw *pRaw = mndStreamActionEncode(const_cast<SStreamObj *>(&captured.stream));
    if (pRaw == nullptr) return false;
    if (sdbSetRawStatus(pRaw, static_cast<ESdbStatus>(captured.rawStatus)) != TSDB_CODE_SUCCESS) {
      sdbFreeRaw(pRaw);
      return false;
    }
    return sdbWrite(pSdb_, pRaw) == TSDB_CODE_SUCCESS;
  }

  bool InvokeRegisteredLifecycleStop(const CapturedLifecycleTransaction &captured) {
    if (captured.callbackParam.empty()) return false;
    STrans *pTrans = mndTransCreate(&mnode_, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, nullptr, "lifecycle-stop");
    if (pTrans == nullptr) return false;
    pTrans->stage = TRN_STAGE_FINISH;
    void *pParam = taosMemoryMalloc(captured.callbackParam.size());
    if (pParam == nullptr) {
      mndTransDrop(pTrans);
      return false;
    }
    std::memcpy(pParam, captured.callbackParam.data(), captured.callbackParam.size());
    mndTransSetCb(pTrans, static_cast<ETrnFunc>(0), static_cast<ETrnFunc>(9), pParam,
                  static_cast<int32_t>(captured.callbackParam.size()));
    const int32_t transId = pTrans->id;
    SSdbRaw      *pRaw = mndTransEncode(pTrans);
    mndTransDrop(pTrans);
    if (pRaw == nullptr) return false;
    if (sdbSetRawStatus(pRaw, SDB_STATUS_READY) != TSDB_CODE_SUCCESS || sdbWrite(pSdb_, pRaw) != TSDB_CODE_SUCCESS) {
      return false;
    }
    STrans *pHeld = mndAcquireTrans(&mnode_, transId);
    if (pHeld == nullptr) return false;
    mndTransRefresh(&mnode_, pHeld);
    mndReleaseTrans(&mnode_, pHeld);
    return true;
  }

  STrans *PersistAndAcquireFinishedStop(const SRpcMsg *pRequest) {
    STrans *pTrans = mndTransCreate(&mnode_, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pRequest, "held-lifecycle");
    if (pTrans == nullptr) return nullptr;
    pTrans->stage = TRN_STAGE_FINISH;
    auto *pParam = static_cast<SStreamLifecycleTransParam *>(taosMemoryCalloc(1, sizeof(SStreamLifecycleTransParam)));
    if (pParam == nullptr) {
      mndTransDrop(pTrans);
      return nullptr;
    }
    pParam->streamId = kStreamA;
    pParam->action = MND_STREAM_LIFECYCLE_STOP;
    pParam->expectedUserStopped = 1;
    tstrncpy(pParam->streamName, kStreamAName, sizeof(pParam->streamName));
    mndTransSetCb(pTrans, static_cast<ETrnFunc>(0), TRANS_STOP_FUNC_STREAM_LIFECYCLE, pParam, sizeof(*pParam));

    const int32_t transId = pTrans->id;
    SSdbRaw      *pRaw = mndTransEncode(pTrans);
    mndTransDrop(pTrans);
    if (pRaw == nullptr) return nullptr;
    if (sdbSetRawStatus(pRaw, SDB_STATUS_READY) != TSDB_CODE_SUCCESS) {
      sdbFreeRaw(pRaw);
      return nullptr;
    }
    if (sdbWrite(pSdb_, pRaw) != TSDB_CODE_SUCCESS) return nullptr;
    return mndAcquireTrans(&mnode_, transId);
  }

  bool WaitForBlockedCallback() {
    std::unique_lock<std::mutex> lock(harness_.callbackMutex);
    return harness_.callbackCv.wait_for(lock, std::chrono::seconds(5), [this]() { return harness_.callbackEntered; });
  }

  void ReleaseBlockedCallback() {
    std::lock_guard<std::mutex> lock(harness_.callbackMutex);
    harness_.releaseCallback = true;
    harness_.callbackCv.notify_all();
  }

  static constexpr int64_t     kStreamA = 0x1001;
  static constexpr int64_t     kStreamB = 0x1002;
  static constexpr const char *kStreamDb = "1.test";
  static constexpr const char *kStreamAName = "1.test.lifecycle_a";
  static constexpr const char *kStreamBName = "1.test.lifecycle_b";
  static constexpr const char *kSharedOutputName = "1.output.shared";
  LifecycleTransactionHarness  harness_;
  SMnode                       mnode_ = {};

 private:
  Stub         stub_;
  SSdb        *pSdb_ = nullptr;
  int32_t      savedAuditLevel_ = AUDIT_LEVEL_NONE;
  int64_t      savedInactiveTimes_ = 0;
  SStmActionQ *savedActionQ_ = nullptr;
  SHashObj    *savedStreamMap_ = nullptr;
  int8_t       savedState_ = 0;
  int8_t       savedActive_ = 0;
};

constexpr int64_t     StreamLifecycleTransactionTest::kStreamA;
constexpr int64_t     StreamLifecycleTransactionTest::kStreamB;
constexpr const char *StreamLifecycleTransactionTest::kStreamDb;
constexpr const char *StreamLifecycleTransactionTest::kStreamAName;
constexpr const char *StreamLifecycleTransactionTest::kStreamBName;
constexpr const char *StreamLifecycleTransactionTest::kSharedOutputName;

}  // namespace

TEST_F(StreamLifecycleTransactionTest, UsesStreamNameInsteadOfOutputTableAsConflictKey) {
  ASSERT_EQ(SendPause(kStreamAName), TSDB_CODE_ACTION_IN_PROGRESS);
  ASSERT_EQ(harness_.transactions.size(), 1U);
  const CapturedLifecycleTransaction &captured = harness_.transactions.front();
  EXPECT_EQ(captured.conflict, TRN_CONFLICT_DB_INSIDE);
  EXPECT_EQ(captured.dbname, kStreamDb);
  EXPECT_EQ(captured.objectName, kStreamAName);
  EXPECT_NE(captured.objectName, kSharedOutputName);
}

TEST_F(StreamLifecycleTransactionTest, SameStreamLifecycleOperationsConflict) {
  ASSERT_TRUE(RegisterActiveLifecycle(kStreamAName));
  EXPECT_EQ(SendDrop({kStreamAName}), TSDB_CODE_MND_TRANS_CONFLICT);
  EXPECT_TRUE(harness_.transactions.empty());
}

TEST_F(StreamLifecycleTransactionTest, DifferentStreamsDoNotConflict) {
  ASSERT_TRUE(RegisterActiveLifecycle(kStreamAName));
  EXPECT_EQ(SendPause(kStreamBName), TSDB_CODE_ACTION_IN_PROGRESS);
  ASSERT_EQ(harness_.transactions.size(), 1U);
  EXPECT_EQ(harness_.transactions.front().conflict, TRN_CONFLICT_DB_INSIDE);
  EXPECT_EQ(harness_.transactions.front().objectName, kStreamBName);
}

TEST_F(StreamLifecycleTransactionTest, StopPrepareFailureKeepsLiveStreamRunning) {
  harness_.prepareCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(SendPause(kStreamAName), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(UserStopped(), 0);
  EXPECT_TRUE(harness_.runtimeActions.empty());
}

TEST_F(StreamLifecycleTransactionTest, StartPrepareFailureKeepsLiveStreamStopped) {
  ASSERT_TRUE(WriteStream(kStreamA, kStreamAName, 1));
  harness_.prepareCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(SendResume(kStreamAName), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(UserStopped(), 1);
  EXPECT_TRUE(harness_.runtimeActions.empty());
}

TEST_F(StreamLifecycleTransactionTest, RuntimeActionRunsOnlyAfterCommitCallback) {
  ASSERT_EQ(SendPause(kStreamAName), TSDB_CODE_ACTION_IN_PROGRESS);
  ASSERT_EQ(harness_.transactions.size(), 1U);
  CapturedLifecycleTransaction stop = TakeTransaction();
  EXPECT_TRUE(harness_.runtimeActions.empty());
  ASSERT_TRUE(InvokeRegisteredLifecycleStop(stop));
  EXPECT_TRUE(harness_.runtimeActions.empty());
  ASSERT_TRUE(ApplyCommittedStream(stop));
  ASSERT_TRUE(InvokeRegisteredLifecycleStop(stop));
  ASSERT_EQ(harness_.runtimeActions.size(), 1U);
  EXPECT_EQ(harness_.runtimeActions[0].streamId, kStreamA);
  EXPECT_EQ(harness_.runtimeActions[0].action, -1);
  tFreeStreamObj(&stop.stream);

  ASSERT_EQ(SendResume(kStreamAName), TSDB_CODE_ACTION_IN_PROGRESS);
  CapturedLifecycleTransaction start = TakeTransaction();
  EXPECT_EQ(harness_.runtimeActions.size(), 1U);
  ASSERT_TRUE(ApplyCommittedStream(start));
  ASSERT_TRUE(InvokeRegisteredLifecycleStop(start));
  ASSERT_EQ(harness_.runtimeActions.size(), 2U);
  EXPECT_EQ(harness_.runtimeActions[1].streamId, kStreamA);
  EXPECT_EQ(harness_.runtimeActions[1].action, STREAM_ACT_DEPLOY);
  tFreeStreamObj(&start.stream);

  ASSERT_EQ(SendDrop({kStreamAName}), TSDB_CODE_ACTION_IN_PROGRESS);
  CapturedLifecycleTransaction drop = TakeTransaction();
  EXPECT_EQ(harness_.runtimeActions.size(), 2U);
  EXPECT_EQ(UserDropped(), 0);
  EXPECT_TRUE(StreamExists());
  ASSERT_TRUE(ApplyCommittedStream(drop));
  EXPECT_FALSE(StreamExists());
  ASSERT_TRUE(InvokeRegisteredLifecycleStop(drop));
  ASSERT_EQ(harness_.runtimeActions.size(), 3U);
  EXPECT_EQ(harness_.runtimeActions[2].streamId, kStreamA);
  EXPECT_EQ(harness_.runtimeActions[2].action, -1);
  tFreeStreamObj(&drop.stream);
}

TEST_F(StreamLifecycleTransactionTest, CommittedLifecyclePublishesBeforeFrameworkResponseWithHeldTransReference) {
  ASSERT_TRUE(WriteStream(kStreamA, kStreamAName, 1));

  SRpcMsg request = {};
  request.msgType = TDMT_MND_STOP_STREAM;
  request.info.node = &mnode_;
  request.info.handle = reinterpret_cast<void *>(1);
  STrans *pHeld = PersistAndAcquireFinishedStop(&request);
  ASSERT_NE(pHeld, nullptr);
  if (pHeld->pRpcArray == nullptr) pHeld->pRpcArray = taosArrayInit(1, sizeof(SRpcHandleInfo));
  ASSERT_NE(pHeld->pRpcArray, nullptr);
  SRpcHandleInfo rpcInfo = request.info;
  ASSERT_NE(taosArrayPush(pHeld->pRpcArray, &rpcInfo), nullptr);
  EXPECT_EQ(pHeld->stopFunc, TRANS_STOP_FUNC_STREAM_LIFECYCLE);

  mndTransRefresh(&mnode_, pHeld);
  mndReleaseTrans(&mnode_, pHeld);

  EXPECT_EQ(harness_.observedEvents, std::vector<int32_t>({LIFECYCLE_RUNTIME_PUBLISHED, LIFECYCLE_RESPONSE_SENT}));
  ASSERT_EQ(harness_.runtimeActions.size(), 1U);
  EXPECT_EQ(harness_.runtimeActions[0].committedUserStopped, 1);
  EXPECT_EQ(harness_.responseCount.load(), 1);
}

TEST_F(StreamLifecycleTransactionTest, ConcurrentFinishRefreshesRunCallbackOnceBeforeSingleResponse) {
  ASSERT_TRUE(WriteStream(kStreamA, kStreamAName, 1));

  SRpcMsg request = {};
  request.msgType = TDMT_MND_STOP_STREAM;
  request.info.node = &mnode_;
  request.info.handle = reinterpret_cast<void *>(4);
  STrans *pFirst = PersistAndAcquireFinishedStop(&request);
  ASSERT_NE(pFirst, nullptr);
  if (pFirst->pRpcArray == nullptr) pFirst->pRpcArray = taosArrayInit(1, sizeof(SRpcHandleInfo));
  ASSERT_NE(pFirst->pRpcArray, nullptr);
  SRpcHandleInfo rpcInfo = request.info;
  ASSERT_NE(taosArrayPush(pFirst->pRpcArray, &rpcInfo), nullptr);
  STrans *pSecond = mndAcquireTrans(&mnode_, pFirst->id);
  ASSERT_NE(pSecond, nullptr);

  harness_.blockCallback = true;
  RuntimeDestroyProbe destroyProbe;

  std::thread first([&]() { mndTransRefresh(&mnode_, pFirst); });
  if (!WaitForBlockedCallback()) {
    ReleaseBlockedCallback();
    first.join();
    mndReleaseTrans(&mnode_, pSecond);
    mndReleaseTrans(&mnode_, pFirst);
    FAIL() << "lifecycle callback did not reach the deterministic barrier";
  }
  EXPECT_EQ(harness_.callbackCount.load(), 1);
  EXPECT_EQ(harness_.responseCount.load(), 0);

  std::atomic<bool> secondFinished{false};
  std::thread       second([&]() {
    mndTransRefresh(&mnode_, pSecond);
    secondFinished.store(true);
  });
  const auto        secondDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!secondFinished.load() && std::chrono::steady_clock::now() < secondDeadline) std::this_thread::yield();
  if (!secondFinished.load()) {
    ReleaseBlockedCallback();
    first.join();
    second.join();
    mndReleaseTrans(&mnode_, pSecond);
    mndReleaseTrans(&mnode_, pFirst);
    FAIL() << "second refresh did not finish its response-state check while the callback was blocked";
  }
  EXPECT_EQ(harness_.responseCount.load(), 0);

  std::thread teardown([&]() { msmHandleBecomeNotLeader(&mnode_); });
  if (!waitForRuntimeWriterOrDestroy(destroyProbe)) {
    ReleaseBlockedCallback();
    first.join();
    second.join();
    teardown.join();
    mndReleaseTrans(&mnode_, pSecond);
    mndReleaseTrans(&mnode_, pFirst);
    FAIL() << "runtime teardown did not reach the reader/writer barrier";
  }
  EXPECT_FALSE(destroyProbe.Entered());
  EXPECT_TRUE(harness_.callbackWithoutTransLock);
  EXPECT_EQ(harness_.responseCount.load(), 0);

  ReleaseBlockedCallback();
  first.join();
  second.join();
  teardown.join();
  EXPECT_EQ(harness_.callbackCount.load(), 1);
  EXPECT_EQ(harness_.responseCount.load(), 1);
  EXPECT_EQ(harness_.observedEvents, std::vector<int32_t>({LIFECYCLE_RUNTIME_PUBLISHED, LIFECYCLE_RESPONSE_SENT}));
  mndReleaseTrans(&mnode_, pSecond);
  mndReleaseTrans(&mnode_, pFirst);
}

TEST_F(StreamLifecycleTransactionTest, TeardownFirstSkipsCommittedLifecycleRuntimePublication) {
  ASSERT_TRUE(WriteStream(kStreamA, kStreamAName, 1));

  RuntimeDestroyProbe destroyProbe;
  msmHandleBecomeNotLeader(&mnode_);
  ASSERT_TRUE(destroyProbe.Entered());

  SStreamLifecycleTransParam param = {};
  param.streamId = kStreamA;
  param.action = MND_STREAM_LIFECYCLE_STOP;
  param.expectedUserStopped = 1;
  tstrncpy(param.streamName, kStreamAName, sizeof(param.streamName));
  mndStreamLifecycleTransStopped(&mnode_, &param, sizeof(param));
  EXPECT_EQ(harness_.callbackCount.load(), 0);
  EXPECT_TRUE(harness_.runtimeActions.empty());
}

TEST_F(StreamLifecycleTransactionTest, FinishDeleteFailureRetainsStreamRpcUntilRetry) {
  ASSERT_TRUE(WriteStream(kStreamA, kStreamAName, 1));

  SRpcMsg request = {};
  request.msgType = TDMT_MND_STOP_STREAM;
  request.info.node = &mnode_;
  request.info.handle = reinterpret_cast<void *>(2);
  STrans *pHeld = PersistAndAcquireFinishedStop(&request);
  ASSERT_NE(pHeld, nullptr);
  if (pHeld->pRpcArray == nullptr) pHeld->pRpcArray = taosArrayInit(1, sizeof(SRpcHandleInfo));
  ASSERT_NE(pHeld->pRpcArray, nullptr);
  SRpcHandleInfo rpcInfo = request.info;
  ASSERT_NE(taosArrayPush(pHeld->pRpcArray, &rpcInfo), nullptr);

  Stub                      writeStub;
  FinishWriteFailureHarness writeFailure;
  writeFailure.stub = &writeStub;
  gFinishWriteFailureHarness = &writeFailure;
  writeStub.set(sdbWrite, failNextFinishWrite);
  mndTransRefresh(&mnode_, pHeld);

  EXPECT_EQ(writeFailure.calls, 1);
  EXPECT_TRUE(harness_.observedEvents.empty());
  EXPECT_EQ(harness_.callbackCount.load(), 0);
  EXPECT_EQ(harness_.responseCount.load(), 0);
  EXPECT_EQ(taosArrayGetSize(pHeld->pRpcArray), 1);
  EXPECT_EQ(pHeld->stopFunc, TRANS_STOP_FUNC_STREAM_LIFECYCLE);

  mndTransRefresh(&mnode_, pHeld);
  EXPECT_EQ(harness_.observedEvents, std::vector<int32_t>({LIFECYCLE_RUNTIME_PUBLISHED, LIFECYCLE_RESPONSE_SENT}));
  EXPECT_EQ(harness_.callbackCount.load(), 1);
  EXPECT_EQ(harness_.responseCount.load(), 1);
  EXPECT_EQ(taosArrayGetSize(pHeld->pRpcArray), 0);
  EXPECT_EQ(pHeld->stopFunc, 0);
  mndReleaseTrans(&mnode_, pHeld);
  gFinishWriteFailureHarness = nullptr;
}

TEST_F(StreamLifecycleTransactionTest, CreateRpcPushFailureDropsTransactionResources) {
  SRpcMsg request = {};
  request.msgType = TDMT_MND_STOP_STREAM;
  request.info.node = &mnode_;
  request.info.handle = reinterpret_cast<void *>(3);

  Stub                      failureStub;
  TransCreateFailureHarness createFailure;
  createFailure.stub = &failureStub;
  gTransCreateFailureHarness = &createFailure;
  failureStub.set(taosMemCalloc, captureTransCreateCalloc);
  failureStub.set(taosMemFree, captureTransCreateFree);
  failureStub.set(taosArrayAddBatch, failTransRpcAddBatch);

  terrno = TSDB_CODE_SUCCESS;
  STrans *pTrans = mndTransCreate(&mnode_, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, &request, "rpc-push-oom");

  failureStub.reset(taosArrayAddBatch);
  failureStub.reset(taosMemFree);
  failureStub.reset(taosMemCalloc);
  gTransCreateFailureHarness = nullptr;
  EXPECT_EQ(pTrans, nullptr);
  EXPECT_EQ(terrno, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(createFailure.addBatchCalls, 1);
  EXPECT_NE(createFailure.transAllocation, nullptr);
  EXPECT_TRUE(createFailure.transFreed);
}

TEST_F(StreamLifecycleTransactionTest, StartChecksActiveStopBeforeNotStoppedEarlyReturn) {
  ASSERT_EQ(UserStopped(), 0);
  ASSERT_TRUE(RegisterActiveLifecycle(kStreamAName));
  EXPECT_EQ(SendResume(kStreamAName), TSDB_CODE_MND_TRANS_CONFLICT);
}

TEST_F(StreamLifecycleTransactionTest, MultiDropKeepsLegacyTransactionPath) {
  ASSERT_EQ(SendDrop({kStreamAName, kStreamBName}), TSDB_CODE_ACTION_IN_PROGRESS);
  ASSERT_EQ(harness_.transactions.size(), 1U);
  const CapturedLifecycleTransaction &captured = harness_.transactions.front();
  EXPECT_EQ(captured.conflict, TRN_CONFLICT_NOTHING);
  EXPECT_EQ(captured.stopFunc, 0);
  EXPECT_EQ(captured.commitCount, 2);
  EXPECT_EQ(harness_.runtimeActions.size(), 2U);
}

TEST_F(MndStreamRecalcPersistenceTest, AcceptedRecordIsHiddenBeforeCommit) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  ASSERT_NE(OnlyRecord(), nullptr);
  EXPECT_TRUE(OnlyRecord()->hidden);
  EXPECT_FALSE(OnlyRecord()->visible);
  EXPECT_EQ(VisibleCount(), 0);
  EXPECT_EQ(DispatchableCount(), 0);
  EXPECT_TRUE(harness_.responseCodes.empty());
  EXPECT_EQ(harness_.createConflict, TRN_CONFLICT_DB_INSIDE);
}

TEST_F(MndStreamRecalcPersistenceTest, ClaimedAcceptTransfersRpcToTransaction) {
  STimeWindow range = {100, 200};
  ASSERT_EQ(AcceptA(range, 11), TSDB_CODE_SUCCESS);
  ASSERT_EQ(harness_.transactions.size(), 1U);
  const CapturedRecalcTransaction &captured = harness_.transactions.front();
  EXPECT_EQ(captured.requestMsgType, TDMT_MND_RECALC_STREAM);
  ASSERT_EQ(captured.rpcHandles.size(), 1U);
  EXPECT_EQ(captured.rpcHandles.front().handle, reinterpret_cast<void *>(11));
  EXPECT_TRUE(harness_.responseCodes.empty());
}

TEST_F(MndStreamRecalcPersistenceTest, SynchronousPrepareFailureRepliesOnlyThroughHandler) {
  harness_.prepareCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);
  STimeWindow range = {100, 200};
  EXPECT_EQ(AcceptA(range), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(harness_.lastCreateMsgType, TDMT_MND_RECALC_STREAM);
  ASSERT_EQ(harness_.lastCreateRpcHandles.size(), 1U);
  EXPECT_EQ(harness_.lastCreateRpcHandles.front().handle, reinterpret_cast<void *>(1));
  EXPECT_EQ(taosArrayGetSize(Status()->recalcRecords), 0);
  EXPECT_TRUE(isListEmpty(&Status()->recalcPersistOps));
  EXPECT_TRUE(harness_.responseCodes.empty());
}

TEST_F(MndStreamRecalcPersistenceTest, TransactionCallbackPublishesWithoutManualReply) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range, 12), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  ASSERT_NE(recalcId, 0);
  EXPECT_EQ(VisibleCount(), 0);
  EXPECT_EQ(DispatchableCount(), 0);
  ASSERT_EQ(harness_.transactions.front().rpcHandles.size(), 1U);
  EXPECT_EQ(harness_.transactions.front().rpcHandles.front().handle, reinterpret_cast<void *>(12));

  CommitNext();

  ASSERT_EQ(VisibleCount(), 1);
  ASSERT_EQ(DispatchableCount(), 1);
  const SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  EXPECT_EQ(pRecord->snapshot.status, STREAM_RECALC_STATUS_PENDING);
  EXPECT_EQ(pRecord->snapshot.progressPct, 0);
  EXPECT_GT(pRecord->requestTimeMs, 0);
  EXPECT_TRUE(harness_.responseCodes.empty());
}

TEST_F(MndStreamRecalcPersistenceTest, CallbackPublishesBeforeFrameworkResponseWithHeldTransReference) {
  STimeWindow range = {100, 200};
  ASSERT_EQ(AcceptA(range, 13), TSDB_CODE_SUCCESS);
  ASSERT_EQ(harness_.transactions.size(), 1U);
  CapturedRecalcTransaction captured = harness_.transactions.front();
  harness_.transactions.pop_front();
  const int64_t recalcId = static_cast<const SStreamRecalcPersistReq *>(taosArrayGet(captured.requests, 0))->recalcId;

  SRpcMsg request = {};
  request.msgType = TDMT_MND_RECALC_STREAM;
  request.info.node = &mnode_;
  request.info.handle = reinterpret_cast<void *>(13);
  STrans *pHeld = PersistAndAcquireFinishedRecalc(captured, request);
  ASSERT_NE(pHeld, nullptr);
  ASSERT_EQ(taosArrayGetSize(pHeld->pRpcArray), 1);

  ApplyCapturedStream(captured);
  harness_.responseProbeStatus = Status();
  harness_.responseProbeRecalcId = recalcId;
  mndTransRefresh(&mnode_, pHeld);
  EXPECT_EQ(pHeld->stopFunc, 0);
  mndReleaseTrans(&mnode_, pHeld);

  EXPECT_TRUE(harness_.responseObservedPublished);
  ASSERT_EQ(harness_.responseCodes.size(), 1U);
  EXPECT_EQ(harness_.responseCodes.front(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(VisibleCount(), 1);
  EXPECT_TRUE(isListEmpty(&Status()->recalcPersistOps));
  taosArrayDestroy(captured.requests);
}

TEST_F(MndStreamRecalcPersistenceTest, PublicationRuntimeReaderBlocksTeardownUntilCommitIsPublished) {
  STimeWindow range = {100, 200};
  ASSERT_EQ(AcceptA(range, 14), TSDB_CODE_SUCCESS);
  ASSERT_EQ(harness_.transactions.size(), 1U);
  CapturedRecalcTransaction captured = harness_.transactions.front();
  harness_.transactions.pop_front();
  ApplyCapturedStream(captured);

  RecalcPublicationGate gate(Status()->recalcRecords);
  RuntimeDestroyProbe   destroyProbe;

  std::thread callback(
      [&]() { mndStreamRecalcTransStopped(&mnode_, captured.callbackParam.data(), captured.callbackParam.size()); });
  if (!gate.WaitUntilEntered()) {
    gate.Release();
    callback.join();
    taosArrayDestroy(captured.requests);
    FAIL() << "recalculation publication did not reach the deterministic barrier";
  }

  std::thread teardown([&]() { msmHandleBecomeNotLeader(&mnode_); });
  if (!waitForRuntimeWriterOrDestroy(destroyProbe)) {
    gate.Release();
    callback.join();
    teardown.join();
    taosArrayDestroy(captured.requests);
    FAIL() << "runtime teardown did not reach the publication reader";
  }
  EXPECT_TRUE(gate.HasRuntimeRead());
  EXPECT_FALSE(destroyProbe.Entered());

  gate.Release();
  callback.join();
  teardown.join();
  EXPECT_TRUE(destroyProbe.Entered());
  EXPECT_EQ(VisibleCount(), 1);
  EXPECT_TRUE(isListEmpty(&Status()->recalcPersistOps));
  taosArrayDestroy(captured.requests);
}

TEST_F(MndStreamRecalcPersistenceTest, TeardownFirstSkipsCommittedRecalcPublication) {
  STimeWindow range = {100, 200};
  ASSERT_EQ(AcceptA(range, 15), TSDB_CODE_SUCCESS);
  ASSERT_EQ(harness_.transactions.size(), 1U);
  CapturedRecalcTransaction captured = harness_.transactions.front();
  harness_.transactions.pop_front();
  const int64_t recalcId = static_cast<const SStreamRecalcPersistReq *>(taosArrayGet(captured.requests, 0))->recalcId;
  ApplyCapturedStream(captured);

  RuntimeDestroyProbe destroyProbe;
  msmHandleBecomeNotLeader(&mnode_);
  ASSERT_TRUE(destroyProbe.Entered());
  mndStreamRecalcTransStopped(&mnode_, captured.callbackParam.data(), captured.callbackParam.size());

  const SStmRecalcRecord *pRecord = OnlyRecord();
  ASSERT_NE(pRecord, nullptr);
  EXPECT_TRUE(pRecord->hidden);
  EXPECT_FALSE(pRecord->visible);
  EXPECT_TRUE(Status()->recalcTransActive);
  EXPECT_FALSE(isListEmpty(&Status()->recalcPersistOps));
  EXPECT_TRUE(PersistedRequestExists(kStreamA, recalcId));
  taosArrayDestroy(captured.requests);
}

TEST_F(MndStreamRecalcPersistenceTest, PullupRuntimeReaderBlocksTeardownThroughClaimedStart) {
  STimeWindow first = {100, 200};
  STimeWindow second = {300, 400};
  ASSERT_EQ(AcceptA(first, 16), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 17), TSDB_CODE_SUCCESS);
  CommitNext();
  ASSERT_EQ(harness_.createCalls, 1);

  SStreamObj *pStream = nullptr;
  ASSERT_EQ(mndAcquireStreamById(&mnode_, kStreamA, &pStream), TSDB_CODE_SUCCESS);
  ASSERT_NE(pStream, nullptr);
  GateSnapshotDup(pStream->pIncompleteRecalcs);
  mndReleaseStream(&mnode_, pStream);

  RuntimeDestroyProbe destroyProbe;
  std::thread         pullup([&]() { mndStreamRecalcPullup(&mnode_); });
  const auto          pullupDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!harness_.snapshotDupEntered.load() && std::chrono::steady_clock::now() < pullupDeadline) {
    std::this_thread::yield();
  }
  if (!harness_.snapshotDupEntered.load()) {
    harness_.releaseSnapshotDup.store(true);
    pullup.join();
    FAIL() << "recalculation pullup did not reach the deterministic barrier";
  }

  std::thread teardown([&]() { msmHandleBecomeNotLeader(&mnode_); });
  if (!waitForRuntimeWriterOrDestroy(destroyProbe)) {
    harness_.releaseSnapshotDup.store(true);
    pullup.join();
    teardown.join();
    FAIL() << "runtime teardown did not reach the pullup reader";
  }
  EXPECT_TRUE(harness_.snapshotDupHasRuntimeRead.load());
  EXPECT_FALSE(destroyProbe.Entered());

  harness_.releaseSnapshotDup.store(true);
  pullup.join();
  teardown.join();
  EXPECT_TRUE(destroyProbe.Entered());
  EXPECT_EQ(harness_.createCalls, 2);
}

TEST_F(MndStreamRecalcPersistenceTest, AlreadyCommittedHeartbeatFinishDoesNotRecursivelyReadRuntimeLockDuringTeardown) {
  STimeWindow range = {100, 200};
  ASSERT_EQ(AcceptA(range, 83), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  ASSERT_NE(recalcId, 0);
  CommitNext();
  ASSERT_NE(InstallTriggerTask(), nullptr);
  ASSERT_NE(Record(kStreamA, recalcId), nullptr);
  Record(kStreamA, recalcId)->dispatchConfirmed = true;
  ASSERT_EQ(RemovePersistedRecalc(), TSDB_CODE_SUCCESS);

  SStreamHbMsg heartbeat = {};
  SArray      *pSnapshots = BuildTerminalHeartbeat(recalcId, &heartbeat);
  ASSERT_NE(pSnapshots, nullptr);
  {
    RecalcPublicationGate gate(pSnapshots, false);
    RuntimeDestroyProbe   destroyProbe;
    EXPECT_EXIT(
        {
          std::atomic<bool>    heartbeatFinished{false};
          std::atomic<bool>    teardownFinished{false};
          std::atomic<int32_t> heartbeatCode{TSDB_CODE_INTERNAL_ERROR};
          std::thread          heartbeatThread([&]() {
            heartbeatCode.store(HandleHeartbeat(&heartbeat));
            heartbeatFinished.store(true);
          });
          if (!gate.WaitUntilEntered()) std::_Exit(2);

          std::thread teardown([&]() {
            msmHandleBecomeNotLeader(&mnode_);
            teardownFinished.store(true);
          });
          if (!waitForRuntimeWriterOrDestroy(destroyProbe)) std::_Exit(3);
          if (destroyProbe.Entered() || !taosHasRWWFlag(&mStreamMgmt.runtimeLock)) std::_Exit(4);

          gate.Release();
          const auto completionDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
          while ((!heartbeatFinished.load() || !teardownFinished.load()) &&
                 std::chrono::steady_clock::now() < completionDeadline) {
            std::this_thread::yield();
          }
          if (!heartbeatFinished.load() || !teardownFinished.load()) std::_Exit(5);
          heartbeatThread.join();
          teardown.join();
          if (heartbeatCode.load() != TSDB_CODE_SUCCESS) std::_Exit(6);
          std::_Exit(0);
        },
        ::testing::ExitedWithCode(0), "");
  }
  tCleanupStreamHbMsg(&heartbeat, true);
}

TEST_F(MndStreamRecalcPersistenceTest, HeartbeatDefersFinishCompletionAndScheduleUntilRuntimeUnlock) {
  STimeWindow range = {100, 200};
  ASSERT_EQ(AcceptA(range, 84), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  ASSERT_NE(recalcId, 0);
  CommitNext();
  ASSERT_NE(InstallTriggerTask(), nullptr);
  ASSERT_NE(Record(kStreamA, recalcId), nullptr);
  Record(kStreamA, recalcId)->dispatchConfirmed = true;
  ASSERT_EQ(RemovePersistedRecalc(), TSDB_CODE_SUCCESS);

  SStreamHbMsg heartbeat = {};
  ASSERT_NE(BuildTerminalHeartbeat(recalcId, &heartbeat), nullptr);
  ASSERT_EQ(HandleHeartbeat(&heartbeat), TSDB_CODE_SUCCESS);
  tCleanupStreamHbMsg(&heartbeat, true);

  EXPECT_FALSE(harness_.responseWhileRuntimeRead.load());
  EXPECT_FALSE(harness_.scheduleWhileRuntimeRead.load());
  EXPECT_TRUE(harness_.responseCodes.empty());
  ASSERT_EQ(harness_.queuedMessages.size(), 1U);
  EXPECT_EQ(listNEles(&Status()->recalcPersistOps), 1);
  EXPECT_FALSE(Status()->recalcTransActive);

  SListNode *pCompletedNode = listHead(&Status()->recalcPersistOps);
  ASSERT_NE(pCompletedNode, nullptr);
  RecalcCompletedNodeFreeProbe freeProbe(pCompletedNode);
  RunImmediateWake();

  EXPECT_TRUE(freeProbe.Freed());
  EXPECT_FALSE(freeProbe.FreedWhileRuntimeRead());
  EXPECT_TRUE(isListEmpty(&Status()->recalcPersistOps));
  EXPECT_FALSE(Status()->recalcTransActive);
  ASSERT_NE(Record(kStreamA, recalcId), nullptr);
  EXPECT_EQ(Record(kStreamA, recalcId)->snapshot.status, STREAM_RECALC_STATUS_FINISHED);
  EXPECT_TRUE(harness_.responseCodes.empty());
}

TEST_F(MndStreamRecalcPersistenceTest, AlreadyCommittedPullupDoesNotRecursivelyReadRuntimeLockDuringTeardown) {
  STimeWindow first = {100, 200};
  STimeWindow second = {300, 400};
  ASSERT_EQ(AcceptA(first, 81), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 82), TSDB_CODE_SUCCESS);
  LoseNextAfterApplying();

  SStreamObj *pStream = nullptr;
  ASSERT_EQ(mndAcquireStreamById(&mnode_, kStreamA, &pStream), TSDB_CODE_SUCCESS);
  ASSERT_NE(pStream, nullptr);
  GateSnapshotDup(pStream->pIncompleteRecalcs);
  mndReleaseStream(&mnode_, pStream);

  RuntimeDestroyProbe destroyProbe;
  EXPECT_EXIT(
      {
        std::atomic<bool> pullupFinished{false};
        std::atomic<bool> teardownFinished{false};
        std::thread       pullup([&]() {
          mndStreamRecalcPullup(&mnode_);
          pullupFinished.store(true);
        });
        const auto        pullupDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!harness_.snapshotDupEntered.load() && std::chrono::steady_clock::now() < pullupDeadline) {
          std::this_thread::yield();
        }
        if (!harness_.snapshotDupEntered.load()) std::_Exit(2);

        std::thread teardown([&]() {
          msmHandleBecomeNotLeader(&mnode_);
          teardownFinished.store(true);
        });
        if (!waitForRuntimeWriterOrDestroy(destroyProbe)) std::_Exit(3);
        if (destroyProbe.Entered() || !taosHasRWWFlag(&mStreamMgmt.runtimeLock)) std::_Exit(4);

        harness_.releaseSnapshotDup.store(true);
        const auto completionDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while ((!pullupFinished.load() || !teardownFinished.load()) &&
               std::chrono::steady_clock::now() < completionDeadline) {
          std::this_thread::yield();
        }
        if (!pullupFinished.load() || !teardownFinished.load()) std::_Exit(5);
        pullup.join();
        teardown.join();
        std::_Exit(0);
      },
      ::testing::ExitedWithCode(0), "");
}

TEST_F(MndStreamRecalcPersistenceTest, AlreadyCommittedPullupDefersResponseAndSchedulingUntilRuntimeUnlock) {
  STimeWindow first = {100, 200};
  STimeWindow second = {300, 400};
  ASSERT_EQ(AcceptA(first, 91), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 92), TSDB_CODE_SUCCESS);
  LoseNextAfterApplying();

  RunPeriodicTimer();

  ASSERT_EQ(harness_.responseCodes.size(), 1U);
  EXPECT_EQ(harness_.responseHandles.front(), reinterpret_cast<void *>(91));
  ASSERT_EQ(harness_.queuedMessages.size(), 1U);
  EXPECT_FALSE(harness_.responseWhileRuntimeRead.load());
  EXPECT_FALSE(harness_.scheduleWhileRuntimeRead.load());
}

TEST_F(MndStreamRecalcPersistenceTest, TeardownFirstSkipsPendingRecalcPullup) {
  STimeWindow first = {100, 200};
  STimeWindow second = {300, 400};
  ASSERT_EQ(AcceptA(first, 18), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 19), TSDB_CODE_SUCCESS);
  CommitNext();
  ASSERT_EQ(harness_.createCalls, 1);

  RuntimeDestroyProbe destroyProbe;
  msmHandleBecomeNotLeader(&mnode_);
  ASSERT_TRUE(destroyProbe.Entered());
  mndStreamRecalcPullup(&mnode_);

  EXPECT_EQ(harness_.createCalls, 1);
  EXPECT_FALSE(Status()->recalcTransActive);
  EXPECT_FALSE(isListEmpty(&Status()->recalcPersistOps));
}

TEST_F(MndStreamRecalcPersistenceTest, TwoAcceptsForOneStreamRunTransactionsSerially) {
  STimeWindow first = {};
  first.skey = 100;
  first.ekey = 200;
  STimeWindow second = {};
  second.skey = 300;
  second.ekey = 400;
  ASSERT_EQ(AcceptA(first, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 2), TSDB_CODE_SUCCESS);
  EXPECT_EQ(harness_.createCalls, 1);
  ASSERT_EQ(harness_.transactions.size(), 1U);
  EXPECT_EQ(listNEles(&Status()->recalcPersistOps), 2);

  CommitNext();
  EXPECT_EQ(harness_.createCalls, 1);
  ASSERT_EQ(harness_.queuedMessages.size(), 1U);
  RunImmediateWake();
  EXPECT_EQ(harness_.createCalls, 2);
  ASSERT_EQ(harness_.transactions.size(), 1U);
}

TEST_F(MndStreamRecalcPersistenceTest, QueuedAcceptTransfersRpcWhenPulledUp) {
  STimeWindow first = {100, 200};
  STimeWindow second = {300, 400};
  ASSERT_EQ(AcceptA(first, 21), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 22), TSDB_CODE_SUCCESS);

  CommitNext();
  ASSERT_EQ(harness_.queuedMessages.size(), 1U);
  RunImmediateWake();

  ASSERT_EQ(harness_.transactions.size(), 1U);
  const CapturedRecalcTransaction &captured = harness_.transactions.front();
  EXPECT_EQ(captured.requestMsgType, TDMT_MND_RECALC_STREAM);
  ASSERT_EQ(captured.rpcHandles.size(), 1U);
  EXPECT_EQ(captured.rpcHandles.front().handle, reinterpret_cast<void *>(22));
  EXPECT_TRUE(harness_.responseCodes.empty());
}

TEST_F(MndStreamRecalcPersistenceTest, DifferentStreamsDoNotShareTransactionQueue) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(Accept(kStreamA, range, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(Accept(kStreamB, range, 2), TSDB_CODE_SUCCESS);
  EXPECT_EQ(harness_.createCalls, 2);
  EXPECT_EQ(harness_.transactions.size(), 2U);
  EXPECT_TRUE(Status(kStreamA)->recalcTransActive);
  EXPECT_TRUE(Status(kStreamB)->recalcTransActive);
}

TEST_F(MndStreamRecalcPersistenceTest, PullupStartsQueuedTransactionsForEachStreamIndependently) {
  STimeWindow first = {100, 200};
  STimeWindow second = {300, 400};
  ASSERT_EQ(Accept(kStreamA, first, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(Accept(kStreamA, second, 2), TSDB_CODE_SUCCESS);
  ASSERT_EQ(Accept(kStreamB, first, 3), TSDB_CODE_SUCCESS);
  ASSERT_EQ(Accept(kStreamB, second, 4), TSDB_CODE_SUCCESS);
  ASSERT_EQ(harness_.createCalls, 2);
  ASSERT_EQ(harness_.transactions.size(), 2U);

  CommitNext();
  CommitNext();
  ASSERT_EQ(harness_.queuedMessages.size(), 2U);

  RunImmediateWake();

  EXPECT_EQ(harness_.createCalls, 4);
  EXPECT_EQ(harness_.transactions.size(), 2U);
  EXPECT_TRUE(Status(kStreamA)->recalcTransActive);
  EXPECT_TRUE(Status(kStreamB)->recalcTransActive);
}

TEST_F(MndStreamRecalcPersistenceTest, RequestSnapshotIsAnImmutableCopy) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  range.skey = 999;
  range.ekey = 1000;
  ASSERT_EQ(harness_.transactions.size(), 1U);
  const auto *pCaptured =
      static_cast<const SStreamRecalcPersistReq *>(taosArrayGet(harness_.transactions.front().requests, 0));
  ASSERT_NE(pCaptured, nullptr);
  EXPECT_EQ(pCaptured->start, 100);
  EXPECT_EQ(pCaptured->end, 200);
  CommitNext();

  SStreamObj *pStream = nullptr;
  ASSERT_EQ(mndAcquireStreamById(&mnode_, kStreamA, &pStream), TSDB_CODE_SUCCESS);
  ASSERT_NE(pStream, nullptr);
  SArray *pSnapshot = nullptr;
  ASSERT_EQ(mndStreamRecalcSnapshot(pStream, &pSnapshot), TSDB_CODE_SUCCESS);
  mndReleaseStream(&mnode_, pStream);
  ASSERT_EQ(taosArrayGetSize(pSnapshot), 1);
  auto *pMutable = static_cast<SStreamRecalcPersistReq *>(taosArrayGet(pSnapshot, 0));
  pMutable->start = 777;
  EXPECT_TRUE(PersistedRequestExists(kStreamA, pMutable->recalcId));
  pStream = nullptr;
  ASSERT_EQ(mndAcquireStreamById(&mnode_, kStreamA, &pStream), TSDB_CODE_SUCCESS);
  const auto *pPersisted = static_cast<const SStreamRecalcPersistReq *>(taosArrayGet(pStream->pIncompleteRecalcs, 0));
  ASSERT_NE(pPersisted, nullptr);
  EXPECT_EQ(pPersisted->start, 100);
  mndReleaseStream(&mnode_, pStream);
  taosArrayDestroy(pSnapshot);
}

TEST_F(MndStreamRecalcPersistenceTest, TransactionUsesRequestsAndRevisionFromOneLockedSnapshot) {
  constexpr int64_t kOldRequest = 101;
  constexpr int64_t kNewRequest = 202;
  SSdbRaw          *pInitial = EncodeStreamUpdate(kStreamA, 10, kOldRequest);
  ASSERT_NE(pInitial, nullptr);
  ASSERT_EQ(WriteRaw(pInitial), TSDB_CODE_SUCCESS);

  SStreamObj *pStored = nullptr;
  ASSERT_EQ(mndAcquireStreamById(&mnode_, kStreamA, &pStored), TSDB_CODE_SUCCESS);
  ASSERT_NE(pStored, nullptr);
  ASSERT_NE(pStored->pIncompleteRecalcs, nullptr);
  GateSnapshotDup(pStored->pIncompleteRecalcs);

  SSdbRaw *pConcurrent = EncodeStreamUpdate(kStreamA, 20, kNewRequest);
  ASSERT_NE(pConcurrent, nullptr);
  STimeWindow range = {};
  range.skey = 1000;
  range.ekey = 2000;
  std::atomic<int32_t> acceptCode{TSDB_CODE_INTERNAL_ERROR};
  std::atomic<int32_t> writerCode{TSDB_CODE_INTERNAL_ERROR};
  std::atomic<bool>    writerFinished{false};

  std::thread accept([&]() { acceptCode.store(AcceptA(range)); });
  const auto  snapshotDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!harness_.snapshotDupEntered.load() && std::chrono::steady_clock::now() < snapshotDeadline) {
    std::this_thread::yield();
  }
  if (!harness_.snapshotDupEntered.load()) {
    harness_.releaseSnapshotDup.store(true);
    accept.join();
    sdbFreeRaw(pConcurrent);
    mndReleaseStream(&mnode_, pStored);
    FAIL() << "recalculation snapshot did not reach the gated array copy";
  }

  std::thread writer([&]() {
    writerCode.store(WriteRaw(pConcurrent));
    writerFinished.store(true);
  });
  const auto  writerDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!writerFinished.load() && !taosHasRWWFlag(&pStored->lock) &&
         std::chrono::steady_clock::now() < writerDeadline) {
    std::this_thread::yield();
  }
  const bool writerReachedUpdate = writerFinished.load() || taosHasRWWFlag(&pStored->lock);
  harness_.releaseSnapshotDup.store(true);
  accept.join();
  writer.join();
  mndReleaseStream(&mnode_, pStored);

  ASSERT_TRUE(writerReachedUpdate);
  ASSERT_EQ(acceptCode.load(), TSDB_CODE_SUCCESS);
  ASSERT_EQ(writerCode.load(), TSDB_CODE_SUCCESS);
  ASSERT_EQ(harness_.transactions.size(), 1U);
  const CapturedRecalcTransaction &captured = harness_.transactions.front();
  const bool oldSnapshot = captured.revision == 11 && CapturedRequestExists(captured, kOldRequest) &&
                           !CapturedRequestExists(captured, kNewRequest);
  const bool newSnapshot = captured.revision == 21 && !CapturedRequestExists(captured, kOldRequest) &&
                           CapturedRequestExists(captured, kNewRequest);
  EXPECT_TRUE(oldSnapshot || newSnapshot);
}

TEST_F(MndStreamRecalcPersistenceTest, CoordinatorAppendUsesAcquiredLiveStreamInsteadOfCopiedLatch) {
  SStreamObj *pStored = nullptr;
  ASSERT_EQ(mndAcquireStreamById(&mnode_, kStreamA, &pStored), TSDB_CODE_SUCCESS);
  ASSERT_NE(pStored, nullptr);

  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  ASSERT_EQ(harness_.transactions.size(), 1U);
  EXPECT_EQ(harness_.transactions.front().sourceStream, pStored);

  mndReleaseStream(&mnode_, pStored);
}

TEST_F(MndStreamRecalcPersistenceTest, TerminalCandidateRemainsRunningUntilDeleteCommit) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  CommitNext();
  SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  pRecord->snapshot.progressPct = 50;

  SStreamRecalcTerminalCandidate candidate = {};
  candidate.snapshot = pRecord->snapshot;
  candidate.snapshot.status = STREAM_RECALC_STATUS_FAILED;
  candidate.snapshot.progressPct = 75;
  candidate.retryOrdinal = 3;
  candidate.errorCode = TSDB_CODE_RPC_TIMEOUT;
  candidate.errorText = tstrerror(candidate.errorCode);
  ASSERT_EQ(mndStreamRecalcFinish(&mnode_, kStreamA, &candidate), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pRecord->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(pRecord->snapshot.progressPct, 50);
  EXPECT_TRUE(pRecord->terminalPersisting);
  EXPECT_TRUE(PersistedRequestExists(kStreamA, recalcId));
}

TEST_F(MndStreamRecalcPersistenceTest, DeletePrepareFailureKeepsRunningAndRetries) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  CommitNext();
  SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  pRecord->snapshot.progressPct = 50;
  harness_.prepareCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);

  SStreamRecalcTerminalCandidate candidate = {};
  candidate.snapshot = pRecord->snapshot;
  candidate.snapshot.status = STREAM_RECALC_STATUS_FAILED;
  candidate.snapshot.progressPct = 75;
  candidate.retryOrdinal = 3;
  candidate.errorCode = TSDB_CODE_RPC_TIMEOUT;
  ASSERT_EQ(mndStreamRecalcFinish(&mnode_, kStreamA, &candidate), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pRecord->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_TRUE(pRecord->terminalPersisting);
  EXPECT_FALSE(Status()->recalcTransActive);
  EXPECT_EQ(harness_.transactions.size(), 0U);

  RunPeriodicTimer();
  EXPECT_TRUE(Status()->recalcTransActive);
  EXPECT_EQ(harness_.transactions.size(), 1U);
}

TEST_F(MndStreamRecalcPersistenceTest, FirstTerminalCandidateIsImmutable) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  CommitNext();
  SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  pRecord->snapshot.progressPct = 50;

  SStreamRecalcTerminalCandidate first = {};
  first.snapshot = pRecord->snapshot;
  first.snapshot.status = STREAM_RECALC_STATUS_FAILED;
  first.snapshot.progressPct = 75;
  first.retryOrdinal = 3;
  first.errorCode = TSDB_CODE_RPC_TIMEOUT;
  ASSERT_EQ(mndStreamRecalcFinish(&mnode_, kStreamA, &first), TSDB_CODE_SUCCESS);
  SStreamRecalcTerminalCandidate conflicting = first;
  conflicting.snapshot.status = STREAM_RECALC_STATUS_FINISHED;
  conflicting.snapshot.progressPct = 100;
  conflicting.errorCode = TSDB_CODE_SUCCESS;
  ASSERT_EQ(mndStreamRecalcFinish(&mnode_, kStreamA, &conflicting), TSDB_CODE_SUCCESS);

  EXPECT_EQ(pRecord->terminalCandidate.status, STREAM_RECALC_STATUS_FAILED);
  EXPECT_EQ(pRecord->terminalCandidate.progressPct, 75);
  EXPECT_EQ(pRecord->errorCode, TSDB_CODE_RPC_TIMEOUT);
}

TEST_F(MndStreamRecalcPersistenceTest, DuplicateTerminalCandidateIsIdempotent) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  CommitNext();
  SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;

  SStreamRecalcTerminalCandidate candidate = {};
  candidate.snapshot = pRecord->snapshot;
  candidate.snapshot.status = STREAM_RECALC_STATUS_FINISHED;
  candidate.snapshot.progressPct = 100;
  ASSERT_EQ(mndStreamRecalcFinish(&mnode_, kStreamA, &candidate), TSDB_CODE_SUCCESS);
  const int32_t createCalls = harness_.createCalls;
  ASSERT_EQ(mndStreamRecalcFinish(&mnode_, kStreamA, &candidate), TSDB_CODE_SUCCESS);
  EXPECT_EQ(harness_.createCalls, createCalls);
  EXPECT_EQ(listNEles(&Status()->recalcPersistOps), 1);
}

TEST_F(MndStreamRecalcPersistenceTest, DuplicateTerminalCandidateDoesNotAllocateUnderMemoryPressure) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  CommitNext();
  SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;

  SStreamRecalcTerminalCandidate candidate = {};
  candidate.snapshot = pRecord->snapshot;
  candidate.snapshot.status = STREAM_RECALC_STATUS_FINISHED;
  candidate.snapshot.progressPct = 100;
  ASSERT_EQ(mndStreamRecalcFinish(&mnode_, kStreamA, &candidate), TSDB_CODE_SUCCESS);
  ASSERT_EQ(listNEles(&Status()->recalcPersistOps), 1);
  const int32_t createCalls = harness_.createCalls;

  int32_t code = TSDB_CODE_SUCCESS;
  {
    RecalcCallocFailureGuard guard;
    code = mndStreamRecalcFinish(&mnode_, kStreamA, &candidate);
  }

  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(gRecalcCallocFailureCalls, 0);
  EXPECT_EQ(listNEles(&Status()->recalcPersistOps), 1);
  EXPECT_EQ(harness_.createCalls, createCalls);
}

TEST_F(MndStreamRecalcPersistenceTest, ConcurrentTerminalCandidatesEnqueueOneFinishOperation) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  CommitNext();
  SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;

  SStreamRecalcTerminalCandidate candidate = {};
  candidate.snapshot = pRecord->snapshot;
  candidate.snapshot.status = STREAM_RECALC_STATUS_FINISHED;
  candidate.snapshot.progressPct = 100;
  std::atomic<int32_t> firstCode{TSDB_CODE_INTERNAL_ERROR};
  std::atomic<int32_t> secondCode{TSDB_CODE_INTERNAL_ERROR};
  {
    RecalcCallocBarrierGuard guard;
    std::thread              first([&]() { firstCode.store(mndStreamRecalcFinish(&mnode_, kStreamA, &candidate)); });
    std::thread              second([&]() { secondCode.store(mndStreamRecalcFinish(&mnode_, kStreamA, &candidate)); });
    const auto               deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (gRecalcCallocBarrierCalls.load() < 2 && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::yield();
    }
    guard.Release();
    first.join();
    second.join();
  }

  EXPECT_GE(gRecalcCallocBarrierCalls.load(), 2);
  EXPECT_EQ(firstCode.load(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(secondCode.load(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(listNEles(&Status()->recalcPersistOps), 1);
  EXPECT_EQ(harness_.transactions.size(), 1U);
}

TEST_F(MndStreamRecalcPersistenceTest, PublishedTerminalRecordDoesNotAllocateUnderMemoryPressure) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  CommitNext();
  SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;

  SStreamRecalcTerminalCandidate candidate = {};
  candidate.snapshot = pRecord->snapshot;
  candidate.snapshot.status = STREAM_RECALC_STATUS_FINISHED;
  candidate.snapshot.progressPct = 100;
  ASSERT_EQ(mndStreamRecalcFinish(&mnode_, kStreamA, &candidate), TSDB_CODE_SUCCESS);
  CommitNext();
  ASSERT_TRUE(isListEmpty(&Status()->recalcPersistOps));

  int32_t code = TSDB_CODE_SUCCESS;
  {
    RecalcCallocFailureGuard guard;
    code = mndStreamRecalcFinish(&mnode_, kStreamA, &candidate);
  }

  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(gRecalcCallocFailureCalls, 0);
  EXPECT_TRUE(isListEmpty(&Status()->recalcPersistOps));
}

TEST_F(MndStreamRecalcPersistenceTest, DeleteCommitPublishesFinishedOrFailed) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  CommitNext();
  SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  pRecord->snapshot.progressPct = 50;

  SStreamRecalcTerminalCandidate candidate = {};
  candidate.snapshot = pRecord->snapshot;
  candidate.snapshot.status = STREAM_RECALC_STATUS_FAILED;
  candidate.snapshot.progressPct = 75;
  candidate.retryOrdinal = 3;
  candidate.errorCode = TSDB_CODE_RPC_TIMEOUT;
  ASSERT_EQ(mndStreamRecalcFinish(&mnode_, kStreamA, &candidate), TSDB_CODE_SUCCESS);
  CommitNext();

  pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  EXPECT_EQ(pRecord->snapshot.status, STREAM_RECALC_STATUS_FAILED);
  EXPECT_EQ(pRecord->snapshot.progressPct, 75);
  EXPECT_FALSE(pRecord->terminalPersisting);
  EXPECT_FALSE(PersistedRequestExists(kStreamA, recalcId));
}

TEST_F(MndStreamRecalcPersistenceTest, CallbackRechecksSdbMembershipBeforePublishing) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  StopNextWithoutCommit();
  ASSERT_NE(OnlyRecord(), nullptr);
  EXPECT_TRUE(OnlyRecord()->hidden);
  EXPECT_FALSE(OnlyRecord()->visible);
  EXPECT_FALSE(Status()->recalcTransActive);
  EXPECT_EQ(listNEles(&Status()->recalcPersistOps), 1);
  EXPECT_TRUE(harness_.responseCodes.empty());
}

TEST_F(MndStreamRecalcPersistenceTest, RegisteredSdbStopDefersNextTransactionToImmediateTimer) {
  STimeWindow first = {};
  first.skey = 100;
  first.ekey = 200;
  STimeWindow second = {};
  second.skey = 300;
  second.ekey = 400;
  ASSERT_EQ(AcceptA(first, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 2), TSDB_CODE_SUCCESS);
  ASSERT_EQ(harness_.createCalls, 1);

  CommitNext(true);

  EXPECT_EQ(harness_.createCalls, 1);
  ASSERT_EQ(harness_.queuedMessages.size(), 1U);
  RunImmediateWake();
  EXPECT_EQ(harness_.createCalls, 2);
}

TEST_F(MndStreamRecalcPersistenceTest, FinishCallbackWithoutDeleteCommitRetries) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  CommitNext();
  SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;

  SStreamRecalcTerminalCandidate candidate = {};
  candidate.snapshot = pRecord->snapshot;
  candidate.snapshot.status = STREAM_RECALC_STATUS_FINISHED;
  candidate.snapshot.progressPct = 100;
  ASSERT_EQ(mndStreamRecalcFinish(&mnode_, kStreamA, &candidate), TSDB_CODE_SUCCESS);
  const int32_t createCalls = harness_.createCalls;
  StopNextWithoutCommit();
  EXPECT_EQ(pRecord->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_TRUE(pRecord->terminalPersisting);
  EXPECT_FALSE(Status()->recalcTransActive);
  RunImmediateWake();
  EXPECT_EQ(harness_.createCalls, createCalls + 1);
}

TEST_F(MndStreamRecalcPersistenceTest, ImmediateWakeFailureFallsBackToPeriodicTimer) {
  STimeWindow first = {};
  first.skey = 100;
  first.ekey = 200;
  STimeWindow second = {};
  second.skey = 300;
  second.ekey = 400;
  ASSERT_EQ(AcceptA(first, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 2), TSDB_CODE_SUCCESS);
  harness_.queueCode = TSDB_CODE_OUT_OF_MEMORY;
  CommitNext();
  EXPECT_TRUE(harness_.queuedMessages.empty());
  EXPECT_EQ(harness_.createCalls, 1);

  harness_.queueCode = TSDB_CODE_SUCCESS;
  RunPeriodicTimer();
  EXPECT_EQ(harness_.createCalls, 2);
}

TEST_F(MndStreamRecalcPersistenceTest, DuplicateTimerWakeDoesNotStartTwice) {
  STimeWindow first = {};
  first.skey = 100;
  first.ekey = 200;
  STimeWindow second = {};
  second.skey = 300;
  second.ekey = 400;
  ASSERT_EQ(AcceptA(first, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 2), TSDB_CODE_SUCCESS);
  CommitNext();
  RunImmediateWake();
  ASSERT_EQ(harness_.createCalls, 2);
  RunPeriodicTimer();
  EXPECT_EQ(harness_.createCalls, 2);
}

TEST_F(MndStreamRecalcPersistenceTest, QueuedAcceptInitializationFailureRepliesAndRemovesReservation) {
  STimeWindow first = {};
  first.skey = 100;
  first.ekey = 200;
  STimeWindow second = {};
  second.skey = 300;
  second.ekey = 400;
  ASSERT_EQ(AcceptA(first, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 2), TSDB_CODE_SUCCESS);
  harness_.createCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);
  CommitNext();
  ASSERT_TRUE(harness_.responseCodes.empty());
  RunImmediateWake();

  EXPECT_EQ(VisibleCount(), 1);
  EXPECT_EQ(taosArrayGetSize(Status()->recalcRecords), 1);
  ASSERT_EQ(harness_.responseCodes.size(), 1U);
  EXPECT_EQ(harness_.responseCodes.back(), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_TRUE(isListEmpty(&Status()->recalcPersistOps));
}

TEST_F(MndStreamRecalcPersistenceTest, QueuedPrepareFailureRepliesExactlyOnce) {
  STimeWindow first = {100, 200};
  STimeWindow second = {300, 400};
  ASSERT_EQ(AcceptA(first, 31), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 32), TSDB_CODE_SUCCESS);
  harness_.prepareCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);

  CommitNext();
  ASSERT_TRUE(harness_.responseCodes.empty());
  RunImmediateWake();

  EXPECT_EQ(harness_.lastCreateMsgType, TDMT_MND_RECALC_STREAM);
  ASSERT_EQ(harness_.lastCreateRpcHandles.size(), 1U);
  EXPECT_EQ(harness_.lastCreateRpcHandles.front().handle, reinterpret_cast<void *>(32));
  ASSERT_EQ(harness_.responseCodes.size(), 1U);
  EXPECT_EQ(harness_.responseCodes.front(), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(harness_.responseHandles.front(), reinterpret_cast<void *>(32));
  EXPECT_TRUE(isListEmpty(&Status()->recalcPersistOps));
}

TEST_F(MndStreamRecalcPersistenceTest, DestroyRepliesToPendingAccepts) {
  STimeWindow first = {100, 200};
  STimeWindow second = {300, 400};
  STimeWindow third = {500, 600};
  ASSERT_EQ(AcceptA(first, 41), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 42), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(third, 43), TSDB_CODE_SUCCESS);

  SStmStatus *pStatus = Status();
  DestroyStatus();

  EXPECT_FALSE(pStatus->recalcPersistOpsInitialized);
  EXPECT_FALSE(pStatus->recalcTransActive);
  EXPECT_TRUE(isListEmpty(&pStatus->recalcPersistOps));
  ASSERT_EQ(harness_.responseCodes.size(), 2U);
  EXPECT_EQ(harness_.responseCodes[0], TSDB_CODE_MND_STREAM_NOT_AVAILABLE);
  EXPECT_EQ(harness_.responseCodes[1], TSDB_CODE_MND_STREAM_NOT_AVAILABLE);
  EXPECT_EQ(harness_.responseHandles,
            std::vector<void *>({reinterpret_cast<void *>(42), reinterpret_cast<void *>(43)}));
}

TEST_F(MndStreamRecalcPersistenceTest, DestroyDoesNotReplyForTransferredAccept) {
  STimeWindow range = {100, 200};
  ASSERT_EQ(AcceptA(range, 51), TSDB_CODE_SUCCESS);
  ASSERT_EQ(harness_.transactions.size(), 1U);
  ASSERT_EQ(harness_.transactions.front().rpcHandles.size(), 1U);
  EXPECT_EQ(harness_.transactions.front().rpcHandles.front().handle, reinterpret_cast<void *>(51));

  SStmStatus *pStatus = Status();
  DestroyStatus();

  EXPECT_FALSE(pStatus->recalcPersistOpsInitialized);
  EXPECT_FALSE(pStatus->recalcTransActive);
  EXPECT_TRUE(isListEmpty(&pStatus->recalcPersistOps));
  EXPECT_TRUE(harness_.responseCodes.empty());
}

TEST_F(MndStreamRecalcPersistenceTest, AlreadyCommittedAcceptPublishesAndRepliesOnce) {
  STimeWindow range = {100, 200};
  ASSERT_EQ(AcceptA(range, 61), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  ASSERT_NE(recalcId, 0);

  LoseNextAfterApplying();
  RunPeriodicTimer();

  const SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  EXPECT_TRUE(pRecord->visible);
  EXPECT_FALSE(pRecord->hidden);
  EXPECT_TRUE(isListEmpty(&Status()->recalcPersistOps));
  ASSERT_EQ(harness_.responseCodes.size(), 1U);
  EXPECT_EQ(harness_.responseCodes.front(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(harness_.responseHandles.front(), reinterpret_cast<void *>(61));
  EXPECT_EQ(harness_.createCalls, 1);
}

TEST_F(MndStreamRecalcPersistenceTest, InactiveLeaderDestroyLeavesPendingRepliesToConnectionTeardown) {
  STimeWindow first = {100, 200};
  STimeWindow second = {300, 400};
  ASSERT_EQ(AcceptA(first, 71), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 72), TSDB_CODE_SUCCESS);
  atomic_store_8(&mStreamMgmt.active, 0);

  SStmStatus *pStatus = Status();
  DestroyStatus();

  EXPECT_FALSE(pStatus->recalcPersistOpsInitialized);
  EXPECT_FALSE(pStatus->recalcTransActive);
  EXPECT_TRUE(isListEmpty(&pStatus->recalcPersistOps));
  EXPECT_TRUE(harness_.responseCodes.empty());
}

TEST_F(MndStreamRecalcPersistenceTest, TaskResetPreservesActiveAcceptThroughCommitPublication) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range, 1), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  ASSERT_NE(recalcId, 0);
  ASSERT_NE(InstallTriggerTask(), nullptr);
  AddSelectiveClearRecords();

  mstResetSStmStatus(Status());
  ASSERT_EQ(Status()->triggerTask, nullptr);
  EXPECT_EQ(Record(kStreamA, kAgedLegacyRecalcId), nullptr);
  EXPECT_EQ(Record(kStreamA, kEvictedLegacyRecalcId), nullptr);
  EXPECT_NE(Record(kStreamA, recalcId), nullptr);
  EXPECT_NE(Record(kStreamA, kVisibleCoordinatorRecalcId), nullptr);
  EXPECT_NE(Record(kStreamA, kTerminalCoordinatorRecalcId), nullptr);
  EXPECT_NE(Record(kStreamA, kPersistingCoordinatorRecalcId), nullptr);
  CommitNext(true);

  const SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  EXPECT_TRUE(pRecord->visible);
  EXPECT_FALSE(pRecord->hidden);
  EXPECT_EQ(pRecord->snapshot.status, STREAM_RECALC_STATUS_PENDING);
  EXPECT_EQ(pRecord->snapshot.progressPct, 0);
  EXPECT_TRUE(isListEmpty(&Status()->recalcPersistOps));
  EXPECT_TRUE(harness_.responseCodes.empty());
}

TEST_F(MndStreamRecalcPersistenceTest, SeriousIdBumpPreservesActiveAcceptThroughCommitPublication) {
  STimeWindow range = {};
  range.skey = 100;
  range.ekey = 200;
  ASSERT_EQ(AcceptA(range, 1), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  ASSERT_NE(recalcId, 0);
  SStmTaskStatus *pTrigger = InstallTriggerTask();
  ASSERT_NE(pTrigger, nullptr);
  AddSelectiveClearRecords();

  EXPECT_EQ(mstBumpTaskSeriousId(pTrigger), 201);
  EXPECT_EQ(Record(kStreamA, kAgedLegacyRecalcId), nullptr);
  EXPECT_EQ(Record(kStreamA, kEvictedLegacyRecalcId), nullptr);
  EXPECT_NE(Record(kStreamA, recalcId), nullptr);
  EXPECT_NE(Record(kStreamA, kVisibleCoordinatorRecalcId), nullptr);
  EXPECT_NE(Record(kStreamA, kTerminalCoordinatorRecalcId), nullptr);
  EXPECT_NE(Record(kStreamA, kPersistingCoordinatorRecalcId), nullptr);
  CommitNext(true);

  const SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  EXPECT_TRUE(pRecord->visible);
  EXPECT_FALSE(pRecord->hidden);
  EXPECT_EQ(pRecord->snapshot.status, STREAM_RECALC_STATUS_PENDING);
  EXPECT_EQ(pRecord->snapshot.progressPct, 0);
  EXPECT_TRUE(isListEmpty(&Status()->recalcPersistOps));
  EXPECT_TRUE(harness_.responseCodes.empty());
}

class MndStreamRecalcIntegrationTest : public MndStreamRecalcPersistenceTest {
 protected:
  static SArray *Snapshots(const SStreamRecalcSnapshot &snapshot) {
    SArray *pSnapshots = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
    if (pSnapshots != nullptr && taosArrayPush(pSnapshots, &snapshot) == nullptr) {
      taosArrayDestroy(pSnapshots);
      return nullptr;
    }
    return pSnapshots;
  }

  static SArray *ErrorDetails(int64_t recalcId, int32_t retryOrdinal, int32_t errorCode) {
    SArray *pDetails = taosArrayInit(1, sizeof(SStreamRecalcDetail));
    if (pDetails == nullptr) return nullptr;
    SStreamRecalcDetail detail = {};
    detail.recalcId = recalcId;
    detail.retryOrdinal = retryOrdinal;
    detail.errorCode = errorCode;
    detail.errorText = taosStrdup(tstrerror(errorCode));
    if (detail.errorText == nullptr || taosArrayPush(pDetails, &detail) == nullptr) {
      taosMemoryFreeClear(detail.errorText);
      taosArrayDestroy(pDetails);
      return nullptr;
    }
    return pDetails;
  }

  static void DestroyDetails(SArray *pDetails) {
    for (int32_t i = 0; i < taosArrayGetSize(pDetails); ++i) {
      auto *pDetail = static_cast<SStreamRecalcDetail *>(taosArrayGet(pDetails, i));
      taosMemoryFreeClear(pDetail->errorText);
    }
    taosArrayDestroy(pDetails);
  }
};

TEST_F(MndStreamRecalcIntegrationTest, AcceptedRequestResendsUntilSnapshotThenPublishesTerminalAfterDelete) {
  STimeWindow range = {100, 200};
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  ASSERT_NE(recalcId, 0);
  CommitNext();
  ASSERT_NE(InstallTriggerTask(), nullptr);

  SStmRecalcRecord *pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  EXPECT_EQ(pRecord->snapshot.status, STREAM_RECALC_STATUS_PENDING);
  for (int32_t heartbeat = 0; heartbeat < 2; ++heartbeat) {
    SArray *pDispatch = nullptr;
    ASSERT_EQ(mndStreamRecalcBuildDispatch(Status(), &pDispatch), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosArrayGetSize(pDispatch), 1);
    const auto *pRequest = static_cast<const SStreamRecalcReq *>(taosArrayGet(pDispatch, 0));
    ASSERT_NE(pRequest, nullptr);
    EXPECT_EQ(pRequest->recalcId, recalcId);
    EXPECT_EQ(pRequest->start, range.skey);
    EXPECT_EQ(pRequest->end, range.ekey);
    taosArrayDestroy(pDispatch);
  }

  SStreamRecalcSnapshot running = {
      .recalcId = recalcId,
      .start = range.skey,
      .end = range.ekey,
      .progressPct = 15,
      .status = STREAM_RECALC_STATUS_RUNNING,
  };
  SArray *pSnapshots = Snapshots(running);
  SArray *pDetails = ErrorDetails(recalcId, 1, TSDB_CODE_RPC_TIMEOUT);
  ASSERT_NE(pSnapshots, nullptr);
  ASSERT_NE(pDetails, nullptr);
  ASSERT_EQ(mndStreamRecalcApplySnapshot(&mnode_, kStreamA, Status(), 100, 200, true,
                                         STREAM_RECALC_DETAIL_RECOGNIZED_VALID, pSnapshots, pDetails),
            TSDB_CODE_SUCCESS);
  taosArrayDestroy(pSnapshots);
  DestroyDetails(pDetails);
  EXPECT_EQ(Record(kStreamA, recalcId)->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  SArray *pDispatch = nullptr;
  ASSERT_EQ(mndStreamRecalcBuildDispatch(Status(), &pDispatch), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosArrayGetSize(pDispatch), 0);
  taosArrayDestroy(pDispatch);

  harness_.prepareCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);
  SStreamRecalcSnapshot failed = running;
  failed.progressPct = 40;
  failed.status = STREAM_RECALC_STATUS_FAILED;
  pSnapshots = Snapshots(failed);
  pDetails = ErrorDetails(recalcId, 3, TSDB_CODE_RPC_TIMEOUT);
  ASSERT_NE(pSnapshots, nullptr);
  ASSERT_NE(pDetails, nullptr);
  ASSERT_EQ(mndStreamRecalcApplySnapshot(&mnode_, kStreamA, Status(), 100, 200, true,
                                         STREAM_RECALC_DETAIL_RECOGNIZED_VALID, pSnapshots, pDetails),
            TSDB_CODE_SUCCESS);
  taosArrayDestroy(pSnapshots);
  DestroyDetails(pDetails);
  EXPECT_EQ(Record(kStreamA, recalcId)->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_TRUE(PersistedRequestExists(kStreamA, recalcId));

  RunPeriodicTimer();
  ASSERT_EQ(harness_.transactions.size(), 1U);
  CommitNext();
  pRecord = Record(kStreamA, recalcId);
  ASSERT_NE(pRecord, nullptr);
  EXPECT_EQ(pRecord->snapshot.status, STREAM_RECALC_STATUS_FAILED);
  EXPECT_EQ(pRecord->snapshot.progressPct, 40);
  EXPECT_EQ(pRecord->retryOrdinal, 3);
  EXPECT_EQ(pRecord->errorCode, TSDB_CODE_RPC_TIMEOUT);
  EXPECT_STREQ(pRecord->errorText, tstrerror(TSDB_CODE_RPC_TIMEOUT));
  EXPECT_FALSE(PersistedRequestExists(kStreamA, recalcId));
}

class MndStreamRecalcDispatchTest : public ::testing::Test {
 protected:
  enum class DetailWireMutation {
    kNone,
    kUnknownVersion,
    kInvalidRetry,
  };

  void SetUp() override {
    savedStreamMap_ = mStreamMgmt.streamMap;
    savedTaskMap_ = mStreamMgmt.taskMap;
    mStreamMgmt.streamMap = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    mStreamMgmt.taskMap = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    actionStm_ = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    ASSERT_NE(mStreamMgmt.streamMap, nullptr);
    ASSERT_NE(mStreamMgmt.taskMap, nullptr);
    ASSERT_NE(actionStm_, nullptr);
    taosHashSetFreeFp(actionStm_, mstDestroySStmAction);

    create_.name = streamName_;
    create_.streamDB = streamDb_;
    create_.sql = sql_;
    create_.streamId = kStreamId;
    tstrncpy(stream_.name, streamName_, sizeof(stream_.name));
    stream_.pCreate = &create_;
    status_.pCreate = &create_;
    timeStub_.set(taosGetTimeOfDay, returnRecalcTimeOfDay);
    gRecalcNowMs = 200000;

    trigger_.type = STREAM_TRIGGER_TASK;
    trigger_.status = STREAM_STATUS_RUNNING;
    trigger_.id.taskId = kTriggerTaskId;
    trigger_.id.seriousId = kTriggerSeriousId;
    trigger_.id.nodeId = 1;
    status_.triggerTask = &trigger_;
    ASSERT_EQ(taosHashPut(mStreamMgmt.streamMap, &kStreamId, sizeof(kStreamId), &status_, sizeof(status_)),
              TSDB_CODE_SUCCESS);
    stored_ = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamId, sizeof(kStreamId)));
    ASSERT_NE(stored_, nullptr);
    trigger_.pStream = stored_;

    SStmTaskStatus *pTrigger = &trigger_;
    const int64_t   key[2] = {kStreamId, kTriggerTaskId};
    ASSERT_EQ(taosHashPut(mStreamMgmt.taskMap, key, sizeof(key), &pTrigger, POINTER_BYTES), TSDB_CODE_SUCCESS);

    taskMsg_.type = STREAM_TRIGGER_TASK;
    taskMsg_.streamId = kStreamId;
    taskMsg_.taskId = kTriggerTaskId;
    taskMsg_.seriousId = kTriggerSeriousId;
    taskMsg_.nodeId = 1;
    taskMsg_.status = STREAM_STATUS_RUNNING;
    taskMsg_.detailStatus = -1;
    ctx_.pMnode = &mnode_;
    ctx_.currTs = 100;
    ctx_.actionStm = actionStm_;
    ctx_.taskNum = 1;
  }

  void TearDown() override {
    droppedStub_.reset(mstIsStreamDropped);
    invalidateStub_.reset(mstInvalidateTaskMetrics);
    finishStub_.reset(mndStreamRecalcFinish);
    gFinishLockStatus = nullptr;
    gObservedViewStream = nullptr;
    taosHashCleanup(actionStm_);
    mstDestroySStmTaskStatus(&trigger_);
    if (stored_->recalcPersistOpsInitialized) tdListEmpty(&stored_->recalcPersistOps);
    taosArrayDestroy(stored_->recalcRecords);
    stored_->recalcRecords = nullptr;
    taosHashCleanup(mStreamMgmt.taskMap);
    taosHashCleanup(mStreamMgmt.streamMap);
    mStreamMgmt.taskMap = savedTaskMap_;
    mStreamMgmt.streamMap = savedStreamMap_;
    timeStub_.reset(taosGetTimeOfDay);
  }

  SStmRecalcRecord *AddRecord(int64_t recalcId = kRecalcId, TSKEY start = 100, TSKEY end = 200) {
    if (stored_->recalcRecords == nullptr) {
      stored_->recalcRecords = taosArrayInit(2, sizeof(SStmRecalcRecord));
    }
    if (stored_->recalcRecords == nullptr) return nullptr;
    SStmRecalcRecord record = {};
    record.snapshot.recalcId = recalcId;
    record.snapshot.start = start;
    record.snapshot.end = end;
    record.snapshot.status = STREAM_RECALC_STATUS_PENDING;
    record.requestTimeMs = 1710000000123;
    record.triggerTaskId = kTriggerTaskId;
    record.triggerSeriousId = kTriggerSeriousId;
    record.typedStatusKnown = true;
    record.visible = true;
    return static_cast<SStmRecalcRecord *>(taosArrayPush(stored_->recalcRecords, &record));
  }

  SStmRecalcRecord *Record(int64_t recalcId = kRecalcId) {
    for (int32_t i = 0; i < taosArrayGetSize(stored_->recalcRecords); ++i) {
      auto *pRecord = static_cast<SStmRecalcRecord *>(taosArrayGet(stored_->recalcRecords, i));
      if (pRecord != nullptr && pRecord->snapshot.recalcId == recalcId) return pRecord;
    }
    return nullptr;
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

  static int32_t FindRecalcRow(const SSDataBlock *pBlock, int64_t recalcId) {
    char id[32] = {0};
    snprintf(id, sizeof(id), "%" PRIx64, recalcId);
    for (int32_t row = 0; row < pBlock->info.rows; ++row) {
      if (getVarCharValue(pBlock, 2, row) == id) return row;
    }
    return -1;
  }

  void ObserveViewDuringMetricApply(int64_t recalcId) {
    gObservedViewStream = &stream_;
    gObservedRecalcId = recalcId;
    gObservedViewCode = TSDB_CODE_SUCCESS;
    gObservedViewRows = -1;
    gObservedRecalcRow = -1;
    invalidateStub_.set(mstInvalidateTaskMetrics, CaptureViewOnMetricsInvalidate);
  }

  const SArray *DispatchFromTriggerHeartbeat() {
    taosHashClear(actionStm_);
    SStreamHbMsg req = {};
    ctx_.pReq = &req;
    msmChkHandleTriggerOperations(&ctx_, &taskMsg_, &trigger_);
    const auto *pAction = static_cast<const SStmAction *>(taosHashGet(actionStm_, &kStreamId, sizeof(kStreamId)));
    return pAction == nullptr ? nullptr : pAction->recalc.recalcList;
  }

  int32_t ApplyHeartbeat(SArray *pSnapshots, SArray *pDetails = nullptr, int64_t taskId = kTriggerTaskId,
                         int64_t seriousId = kTriggerSeriousId, int32_t decodeCode = TSDB_CODE_SUCCESS) {
    taosHashClear(actionStm_);
    SStreamHbMsg req = {};
    req.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
    req.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
    req.pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
    if (req.pStreamStatus == nullptr || req.pTaskMetrics == nullptr ||
        taosArrayPush(req.pStreamStatus, &taskMsg_) == nullptr) {
      tCleanupStreamHbMsg(&req, true);
      return terrno;
    }

    SStreamTaskMetricsEntry entry = {};
    entry.taskStatusIndex = 0;
    entry.streamId = kStreamId;
    entry.taskId = taskId;
    entry.seriousId = seriousId;
    entry.decodeCode = decodeCode;
    entry.recalcDetailState = pDetails == nullptr ? STREAM_RECALC_DETAIL_ABSENT : STREAM_RECALC_DETAIL_RECOGNIZED_VALID;
    entry.snapshot.applicableMask = STREAM_METRIC_RECALCULATES;
    entry.snapshot.validMask = STREAM_METRIC_RECALCULATES;
    entry.snapshot.pRecalculates = pSnapshots;
    entry.snapshot.pRecalcDetails = pDetails;
    if (taosArrayPush(req.pTaskMetrics, &entry) == nullptr) {
      tCleanupStreamHbMsg(&req, true);
      return terrno;
    }

    ctx_.pReq = &req;
    const int32_t code = msmNormalHandleStatusUpdate(&ctx_);
    tCleanupStreamHbMsg(&req, true);
    return code;
  }

  static int32_t ReadI32(const std::vector<uint8_t> &bytes, size_t offset) {
    int32_t value = 0;
    if (offset + sizeof(value) <= bytes.size()) memcpy(&value, bytes.data() + offset, sizeof(value));
    return value;
  }

  static int64_t ReadI64(const std::vector<uint8_t> &bytes, size_t offset) {
    int64_t value = 0;
    if (offset + sizeof(value) <= bytes.size()) memcpy(&value, bytes.data() + offset, sizeof(value));
    return value;
  }

  static void WriteI32(std::vector<uint8_t> *pBytes, size_t offset, int32_t value) {
    ASSERT_NE(pBytes, nullptr);
    ASSERT_LE(offset + sizeof(value), pBytes->size());
    memcpy(pBytes->data() + offset, &value, sizeof(value));
  }

  static size_t FindDetailExtension(const std::vector<uint8_t> &bytes, int32_t recalcNum) {
    constexpr size_t kEntryHeaderSize = sizeof(int32_t) * 2 + sizeof(int64_t) * 3;
    constexpr size_t kFixedPayloadSize =
        sizeof(uint64_t) * 7 + sizeof(int8_t) * 2 + sizeof(int64_t) + sizeof(int32_t) * 2;
    constexpr size_t kRecalcWireSize = sizeof(int64_t) * 3 + sizeof(int32_t) * 2;
    for (size_t offset = 0; offset + kEntryHeaderSize + kFixedPayloadSize <= bytes.size(); ++offset) {
      if (ReadI32(bytes, offset) != 0 || ReadI64(bytes, offset + sizeof(int32_t)) != kStreamId ||
          ReadI64(bytes, offset + sizeof(int32_t) + sizeof(int64_t)) != kTriggerTaskId ||
          ReadI64(bytes, offset + sizeof(int32_t) + sizeof(int64_t) * 2) != kTriggerSeriousId) {
        continue;
      }
      const size_t payloadOffset = offset + kEntryHeaderSize;
      uint64_t     applicableMask = 0;
      uint64_t     validMask = 0;
      memcpy(&applicableMask, bytes.data() + payloadOffset, sizeof(applicableMask));
      memcpy(&validMask, bytes.data() + payloadOffset + sizeof(applicableMask), sizeof(validMask));
      if (applicableMask != STREAM_METRIC_RECALCULATES || validMask != STREAM_METRIC_RECALCULATES) continue;
      return payloadOffset + kFixedPayloadSize + recalcNum * kRecalcWireSize;
    }
    return bytes.size();
  }

  int32_t ApplySerializedHeartbeat(SArray *pSnapshots, SArray *pDetails,
                                   DetailWireMutation             mutation = DetailWireMutation::kNone,
                                   const SSTriggerRecalcProgress *pLegacyProgress = nullptr, bool includeMetrics = true,
                                   int32_t observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1,
                                   int64_t statusSeriousId = kTriggerSeriousId, int64_t metricsTaskId = kTriggerTaskId,
                                   uint64_t applicableMask = STREAM_METRIC_RECALCULATES,
                                   uint64_t validMask = STREAM_METRIC_RECALCULATES) {
    SStreamHbMsg input = {};
    input.dnodeId = 1;
    input.observabilityVersion = observabilityVersion;
    input.pVgLeaders = taosArrayInit(0, sizeof(int32_t));
    input.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
    input.pStreamReq = taosArrayInit(0, sizeof(int32_t));
    input.pTriggerStatus = taosArrayInit(0, sizeof(SSTriggerRuntimeStatus));
    input.pTaskMetrics = taosArrayInit(includeMetrics ? 1 : 0, sizeof(SStreamTaskMetricsEntry));
    if (input.pVgLeaders == nullptr || input.pStreamStatus == nullptr || input.pStreamReq == nullptr ||
        input.pTriggerStatus == nullptr || input.pTaskMetrics == nullptr) {
      tCleanupStreamHbMsg(&input, true);
      return terrno;
    }

    SStmTaskStatusMsg taskMsg = taskMsg_;
    taskMsg.seriousId = statusSeriousId;
    if (pLegacyProgress != nullptr) {
      SSTriggerRuntimeStatus legacy = {};
      legacy.userRecalcs = taosArrayInit(1, sizeof(SSTriggerRecalcProgress));
      if (legacy.userRecalcs == nullptr || taosArrayPush(legacy.userRecalcs, pLegacyProgress) == nullptr) {
        taosArrayDestroy(legacy.userRecalcs);
        tCleanupStreamHbMsg(&input, true);
        return terrno;
      }
      if (taosArrayPush(input.pTriggerStatus, &legacy) == nullptr) {
        taosArrayDestroy(legacy.userRecalcs);
        tCleanupStreamHbMsg(&input, true);
        return terrno;
      }
      taskMsg.detailStatus = 0;
    }
    if (taosArrayPush(input.pStreamStatus, &taskMsg) == nullptr) {
      tCleanupStreamHbMsg(&input, true);
      return terrno;
    }

    if (includeMetrics) {
      SStreamTaskMetricsEntry entry = {};
      entry.taskStatusIndex = 0;
      entry.streamId = kStreamId;
      entry.taskId = metricsTaskId;
      entry.seriousId = statusSeriousId;
      entry.snapshot.applicableMask = applicableMask;
      entry.snapshot.validMask = validMask;
      entry.snapshot.logicalInputRows1m = 600;
      entry.snapshot.pRecalculates = pSnapshots;
      entry.snapshot.pRecalcDetails = pDetails;
      if (taosArrayPush(input.pTaskMetrics, &entry) == nullptr) {
        tCleanupStreamHbMsg(&input, true);
        return terrno;
      }
    }

    SEncoder sizeEncoder = {};
    tEncoderInit(&sizeEncoder, nullptr, 0);
    int32_t encodedLength = tEncodeStreamHbMsg(&sizeEncoder, &input);
    tEncoderClear(&sizeEncoder);
    if (encodedLength <= 0) {
      tCleanupStreamHbMsg(&input, true);
      return encodedLength;
    }

    std::vector<uint8_t> bytes(encodedLength);
    SEncoder             encoder = {};
    tEncoderInit(&encoder, bytes.data(), encodedLength);
    int32_t code = tEncodeStreamHbMsg(&encoder, &input);
    tEncoderClear(&encoder);
    const int32_t recalcNum = taosArrayGetSize(pSnapshots);
    tCleanupStreamHbMsg(&input, true);
    if (code <= 0) return code;

    if (includeMetrics && mutation != DetailWireMutation::kNone) {
      size_t detailOffset = FindDetailExtension(bytes, recalcNum);
      if (detailOffset + sizeof(int32_t) > bytes.size()) return TSDB_CODE_INVALID_MSG;
      if (mutation == DetailWireMutation::kUnknownVersion) {
        WriteI32(&bytes, detailOffset, STREAM_HB_RECALC_DETAIL_VERSION_V1 + 1);
      } else {
        if (detailOffset + sizeof(int32_t) * 3 + sizeof(int64_t) > bytes.size()) return TSDB_CODE_INVALID_MSG;
        WriteI32(&bytes, detailOffset + sizeof(int32_t) * 3 + sizeof(int64_t), 4);
      }
    }

    SStreamHbMsg decoded = {};
    SDecoder     decoder = {};
    tDecoderInit(&decoder, bytes.data(), encodedLength);
    code = tDecodeStreamHbMsg(&decoder, &decoded);
    tDecoderClear(&decoder);
    if (code == TSDB_CODE_SUCCESS) {
      ctx_.pReq = &decoded;
      code = msmNormalHandleStatusUpdate(&ctx_);
    }
    tCleanupStreamHbMsg(&decoded, true);
    return code;
  }

  static SArray *Snapshots(const SStreamRecalcSnapshot *pSnapshot = nullptr) {
    SArray *pSnapshots = taosArrayInit(pSnapshot == nullptr ? 0 : 1, sizeof(SStreamRecalcSnapshot));
    if (pSnapshots != nullptr && pSnapshot != nullptr && taosArrayPush(pSnapshots, pSnapshot) == nullptr) {
      taosArrayDestroy(pSnapshots);
      return nullptr;
    }
    return pSnapshots;
  }

  static SArray *ErrorDetails(int64_t recalcId, int32_t retryOrdinal, int32_t errorCode) {
    SArray *pDetails = taosArrayInit(1, sizeof(SStreamRecalcDetail));
    if (pDetails == nullptr) return nullptr;
    SStreamRecalcDetail detail = {};
    detail.recalcId = recalcId;
    detail.retryOrdinal = retryOrdinal;
    detail.errorCode = errorCode;
    detail.errorText = errorCode == 0 ? nullptr : taosStrdup(tstrerror(errorCode));
    if ((errorCode != 0 && detail.errorText == nullptr) || taosArrayPush(pDetails, &detail) == nullptr) {
      taosMemoryFreeClear(detail.errorText);
      taosArrayDestroy(pDetails);
      return nullptr;
    }
    return pDetails;
  }

  static const SStreamRecalcReq *OnlyDispatch(const SArray *pDispatch) {
    return taosArrayGetSize(pDispatch) == 1 ? static_cast<const SStreamRecalcReq *>(taosArrayGet(pDispatch, 0))
                                            : nullptr;
  }

  static void CaptureViewOnMetricsInvalidate(SStmTaskStatus *) {
    SSDataBlock *pBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAM_RECALCULATES, 128);
    if (pBlock == nullptr) {
      gObservedViewCode = terrno;
      return;
    }
    int32_t rows = 0;
    gObservedViewCode = mstSetStreamRecalculatesResBlock(gObservedViewStream, pBlock, &rows, 128);
    if (gObservedViewCode == TSDB_CODE_SUCCESS) {
      gObservedViewRows = pBlock->info.rows;
      gObservedRecalcRow = FindRecalcRow(pBlock, gObservedRecalcId);
    }
    blockDataDestroy(pBlock);
  }

  static int32_t ReturnStreamNotDropped(SMnode *, int64_t, bool *pDropped) {
    *pDropped = false;
    return TSDB_CODE_SUCCESS;
  }

  static int32_t CaptureFinishUnlocked(SMnode *, int64_t, const SStreamRecalcTerminalCandidate *pCandidate) {
    gFinishCalled = true;
    gFinishRetryOrdinal = pCandidate->retryOrdinal;
    gFinishErrorCode = pCandidate->errorCode;
    tstrncpy(gFinishErrorText, pCandidate->errorText, sizeof(gFinishErrorText));
    if (gFinishLockStatus != nullptr && taosWTryLockLatch(&gFinishLockStatus->userRecalcLock) == 0) {
      gFinishCalledWithoutLock = true;
      taosWUnLockLatch(&gFinishLockStatus->userRecalcLock);
    }
    return TSDB_CODE_SUCCESS;
  }

  static constexpr int64_t kStreamId = 42;
  static constexpr int64_t kTriggerTaskId = 55;
  static constexpr int64_t kTriggerSeriousId = 66;
  static constexpr int64_t kRecalcId = 0x7788;
  static SStmStatus       *gFinishLockStatus;
  static bool              gFinishCalled;
  static bool              gFinishCalledWithoutLock;
  static int32_t           gFinishRetryOrdinal;
  static int32_t           gFinishErrorCode;
  static char              gFinishErrorText[MND_STREAM_RECALC_MESSAGE_LEN];
  static SStreamObj       *gObservedViewStream;
  static int64_t           gObservedRecalcId;
  static int32_t           gObservedViewCode;
  static int32_t           gObservedViewRows;
  static int32_t           gObservedRecalcRow;
  char                     streamName_[32] = "test.stream";
  char                     streamDb_[16] = "test";
  char                     sql_[16] = "select 1";
  SCMCreateStreamReq       create_ = {};
  SStreamObj               stream_ = {};
  SMnode                   mnode_ = {};
  SStmStatus               status_ = {};
  SStmStatus              *stored_ = nullptr;
  SStmTaskStatus           trigger_ = {};
  SStmTaskStatusMsg        taskMsg_ = {};
  SStmGrpCtx               ctx_ = {};
  SHashObj                *actionStm_ = nullptr;
  SHashObj                *savedStreamMap_ = nullptr;
  SHashObj                *savedTaskMap_ = nullptr;
  Stub                     finishStub_;
  Stub                     droppedStub_;
  Stub                     invalidateStub_;
  Stub                     timeStub_;
};

SStmStatus       *MndStreamRecalcDispatchTest::gFinishLockStatus = nullptr;
bool              MndStreamRecalcDispatchTest::gFinishCalled = false;
bool              MndStreamRecalcDispatchTest::gFinishCalledWithoutLock = false;
int32_t           MndStreamRecalcDispatchTest::gFinishRetryOrdinal = 0;
int32_t           MndStreamRecalcDispatchTest::gFinishErrorCode = 0;
char              MndStreamRecalcDispatchTest::gFinishErrorText[MND_STREAM_RECALC_MESSAGE_LEN] = {};
SStreamObj       *MndStreamRecalcDispatchTest::gObservedViewStream = nullptr;
int64_t           MndStreamRecalcDispatchTest::gObservedRecalcId = 0;
int32_t           MndStreamRecalcDispatchTest::gObservedViewCode = TSDB_CODE_SUCCESS;
int32_t           MndStreamRecalcDispatchTest::gObservedViewRows = -1;
int32_t           MndStreamRecalcDispatchTest::gObservedRecalcRow = -1;
constexpr int64_t MndStreamRecalcDispatchTest::kStreamId;
constexpr int64_t MndStreamRecalcDispatchTest::kTriggerTaskId;
constexpr int64_t MndStreamRecalcDispatchTest::kTriggerSeriousId;
constexpr int64_t MndStreamRecalcDispatchTest::kRecalcId;

class StreamRecalculateHeartbeatViewTest : public MndStreamRecalcDispatchTest {};

TEST_F(StreamRecalculateHeartbeatViewTest, FirstV1InvalidDetailCannotReviveAgePrunedLegacyMirror) {
  SStmRecalcRecord *pRecord = AddRecord();
  ASSERT_NE(pRecord, nullptr);
  pRecord->snapshot.status = STREAM_RECALC_STATUS_FINISHED;
  pRecord->snapshot.progressPct = 100;
  pRecord->terminalObservedAtMs = 1000;
  gRecalcNowMs = 3601000;

  const SSTriggerRecalcProgress legacy = {
      .recalcId = kRecalcId,
      .progress = 100,
      .start = 100,
      .end = 200,
  };
  SStreamRecalcSnapshot snapshot = {};
  snapshot.recalcId = kRecalcId;
  snapshot.start = 100;
  snapshot.end = 200;
  snapshot.progressPct = 100;
  snapshot.status = STREAM_RECALC_STATUS_FINISHED;
  SArray *pDetails = ErrorDetails(kRecalcId, 1, TSDB_CODE_OUT_OF_MEMORY);
  ASSERT_NE(pDetails, nullptr);
  ObserveViewDuringMetricApply(kRecalcId);

  ASSERT_EQ(ApplySerializedHeartbeat(Snapshots(&snapshot), pDetails, DetailWireMutation::kInvalidRetry, &legacy),
            TSDB_CODE_SUCCESS);

  EXPECT_EQ(gObservedViewCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(gObservedViewRows, 0);
  EXPECT_EQ(gObservedRecalcRow, -1);
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 0);
  EXPECT_FALSE(trigger_.metricsValid);
  EXPECT_NE(trigger_.metrics.applicableMask & STREAM_METRIC_RECALCULATES, 0U);
  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 0);
  EXPECT_EQ(FindRecalcRow(pBlock, kRecalcId), -1);
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateHeartbeatViewTest, FirstV1MissingEntryCannotExceedTerminalCapThroughLegacyMirror) {
  constexpr int32_t kTerminalCount = 101;
  for (int32_t i = 0; i < kTerminalCount; ++i) {
    SStmRecalcRecord *pRecord = AddRecord(kRecalcId + i, 100 + i, 200 + i);
    ASSERT_NE(pRecord, nullptr);
    pRecord->snapshot.status = STREAM_RECALC_STATUS_FINISHED;
    pRecord->snapshot.progressPct = 100;
    pRecord->terminalObservedAtMs = 1000 + i;
  }
  const SSTriggerRecalcProgress legacy = {
      .recalcId = kRecalcId,
      .progress = 100,
      .start = 100,
      .end = 200,
  };
  ObserveViewDuringMetricApply(kRecalcId);

  ASSERT_EQ(ApplySerializedHeartbeat(nullptr, nullptr, DetailWireMutation::kNone, &legacy, false), TSDB_CODE_SUCCESS);

  EXPECT_EQ(gObservedViewCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(gObservedViewRows, 100);
  EXPECT_EQ(gObservedRecalcRow, -1);
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 100);
  EXPECT_FALSE(trigger_.metricsValid);
  EXPECT_NE(trigger_.metrics.applicableMask & STREAM_METRIC_RECALCULATES, 0U);
  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 100);
  EXPECT_EQ(FindRecalcRow(pBlock, kRecalcId), -1);
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateHeartbeatViewTest, FirstV1InvalidFixedEntryCannotCreateUnknownLegacyRow) {
  constexpr int64_t             kUnknownRecalcId = kRecalcId + 1000;
  const SSTriggerRecalcProgress legacy = {
      .recalcId = kUnknownRecalcId,
      .progress = 100,
      .start = 100,
      .end = 200,
  };
  SStreamRecalcSnapshot snapshot = {};
  snapshot.recalcId = kUnknownRecalcId;
  snapshot.start = 100;
  snapshot.end = 200;
  snapshot.progressPct = 20;
  snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  ObserveViewDuringMetricApply(kUnknownRecalcId);

  ASSERT_EQ(ApplySerializedHeartbeat(Snapshots(&snapshot), nullptr, DetailWireMutation::kNone, &legacy, true,
                                     STREAM_HB_OBSERVABILITY_VERSION_V1, kTriggerSeriousId, kTriggerTaskId + 1),
            TSDB_CODE_SUCCESS);

  EXPECT_EQ(gObservedViewCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(gObservedViewRows, 0);
  EXPECT_EQ(gObservedRecalcRow, -1);
  EXPECT_FALSE(trigger_.metricsValid);
  EXPECT_NE(trigger_.metrics.applicableMask & STREAM_METRIC_RECALCULATES, 0U);
  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 0);
  EXPECT_EQ(FindRecalcRow(pBlock, kUnknownRecalcId), -1);
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateHeartbeatViewTest, FirstV1UnknownDetailCannotCreateUnknownLegacyRow) {
  constexpr int64_t             kUnknownRecalcId = kRecalcId + 1001;
  const SSTriggerRecalcProgress legacy = {
      .recalcId = kUnknownRecalcId,
      .progress = 20,
      .start = 100,
      .end = 200,
  };
  SStreamRecalcSnapshot snapshot = {};
  snapshot.recalcId = kUnknownRecalcId;
  snapshot.start = 100;
  snapshot.end = 200;
  snapshot.progressPct = 20;
  snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  SArray *pDetails = taosArrayInit(0, sizeof(SStreamRecalcDetail));
  ASSERT_NE(pDetails, nullptr);
  ObserveViewDuringMetricApply(kUnknownRecalcId);

  ASSERT_EQ(ApplySerializedHeartbeat(Snapshots(&snapshot), pDetails, DetailWireMutation::kUnknownVersion, &legacy),
            TSDB_CODE_SUCCESS);

  EXPECT_EQ(gObservedViewCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(gObservedViewRows, 0);
  EXPECT_EQ(gObservedRecalcRow, -1);
  EXPECT_TRUE(trigger_.metricsValid);
  EXPECT_NE(trigger_.metrics.applicableMask & STREAM_METRIC_RECALCULATES, 0U);
  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 0);
  EXPECT_EQ(FindRecalcRow(pBlock, kUnknownRecalcId), -1);
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateHeartbeatViewTest, V1NonRecalcMetricKeepsCapabilityLatchAndHidesLegacyMirror) {
  constexpr int64_t             kUnknownRecalcId = kRecalcId + 1500;
  const SSTriggerRecalcProgress legacy = {
      .recalcId = kUnknownRecalcId,
      .progress = 20,
      .start = 100,
      .end = 200,
  };
  ObserveViewDuringMetricApply(kUnknownRecalcId);

  ASSERT_EQ(ApplySerializedHeartbeat(nullptr, nullptr, DetailWireMutation::kNone, &legacy, true,
                                     STREAM_HB_OBSERVABILITY_VERSION_V1, kTriggerSeriousId, kTriggerTaskId,
                                     STREAM_METRIC_LOGICAL_INPUT, STREAM_METRIC_LOGICAL_INPUT),
            TSDB_CODE_SUCCESS);

  EXPECT_EQ(gObservedViewCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(gObservedViewRows, 0);
  EXPECT_EQ(gObservedRecalcRow, -1);
  ASSERT_TRUE(trigger_.metricsValid);
  EXPECT_EQ(trigger_.metrics.applicableMask,
            static_cast<uint64_t>(STREAM_METRIC_LOGICAL_INPUT | STREAM_METRIC_RECALCULATES));
  EXPECT_EQ(trigger_.metrics.validMask, static_cast<uint64_t>(STREAM_METRIC_LOGICAL_INPUT));
  EXPECT_EQ(trigger_.metrics.logicalInputRows1m, 600U);
  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 0);
  EXPECT_EQ(FindRecalcRow(pBlock, kUnknownRecalcId), -1);
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateHeartbeatViewTest, OldHeartbeatRetainsLegacyFallbackWithoutCapabilityLatch) {
  constexpr int64_t             kLegacyRecalcId = kRecalcId + 2000;
  const SSTriggerRecalcProgress legacy = {
      .recalcId = kLegacyRecalcId,
      .progress = 42,
      .start = 100,
      .end = 200,
  };
  ObserveViewDuringMetricApply(kLegacyRecalcId);

  ASSERT_EQ(ApplySerializedHeartbeat(nullptr, nullptr, DetailWireMutation::kNone, &legacy, false, 0),
            TSDB_CODE_SUCCESS);

  EXPECT_EQ(gObservedViewCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(gObservedViewRows, 1);
  EXPECT_GE(gObservedRecalcRow, 0);
  EXPECT_EQ(trigger_.metrics.applicableMask & STREAM_METRIC_RECALCULATES, 0U);
  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 1);
  EXPECT_GE(FindRecalcRow(pBlock, kLegacyRecalcId), 0);
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateHeartbeatViewTest, SeriousIdResetClearsLatchAndStaleIdentityCannotRestoreIt) {
  constexpr int64_t             kLegacyRecalcId = kRecalcId + 3000;
  const SSTriggerRecalcProgress legacy = {
      .recalcId = kLegacyRecalcId,
      .progress = 42,
      .start = 100,
      .end = 200,
  };
  ASSERT_EQ(ApplySerializedHeartbeat(nullptr, nullptr, DetailWireMutation::kNone, &legacy, false), TSDB_CODE_SUCCESS);
  ASSERT_NE(trigger_.metrics.applicableMask & STREAM_METRIC_RECALCULATES, 0U);

  const int64_t newSeriousId = mstBumpTaskSeriousId(&trigger_);
  ASSERT_EQ(newSeriousId, kTriggerSeriousId + 1);
  EXPECT_EQ(trigger_.metrics.applicableMask & STREAM_METRIC_RECALCULATES, 0U);
  EXPECT_EQ(trigger_.detailStatus, nullptr);

  droppedStub_.set(mstIsStreamDropped, ReturnStreamNotDropped);
  ASSERT_EQ(ApplySerializedHeartbeat(nullptr, nullptr, DetailWireMutation::kNone, &legacy, false,
                                     STREAM_HB_OBSERVABILITY_VERSION_V1, kTriggerSeriousId),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(trigger_.metrics.applicableMask & STREAM_METRIC_RECALCULATES, 0U);
  EXPECT_EQ(trigger_.detailStatus, nullptr);

  ASSERT_EQ(ApplySerializedHeartbeat(nullptr, nullptr, DetailWireMutation::kNone, &legacy, false,
                                     STREAM_HB_OBSERVABILITY_VERSION_V1, newSeriousId),
            TSDB_CODE_SUCCESS);
  EXPECT_NE(trigger_.metrics.applicableMask & STREAM_METRIC_RECALCULATES, 0U);
  EXPECT_NE(trigger_.detailStatus, nullptr);
  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 0);
  EXPECT_EQ(FindRecalcRow(pBlock, kLegacyRecalcId), -1);
  blockDataDestroy(pBlock);
}

TEST_F(MndStreamRecalcDispatchTest, UnconfirmedRequestIsIncludedInEveryHeartbeatResponse) {
  ASSERT_NE(AddRecord(), nullptr);

  SArray *pDirect = nullptr;
  ASSERT_EQ(mndStreamRecalcBuildDispatch(stored_, &pDirect), TSDB_CODE_SUCCESS);
  const SStreamRecalcReq *pBuilt = OnlyDispatch(pDirect);
  ASSERT_NE(pBuilt, nullptr);
  EXPECT_EQ(pBuilt->recalcId, kRecalcId);
  EXPECT_EQ(pBuilt->start, 100);
  EXPECT_EQ(pBuilt->end, 200);
  taosArrayDestroy(pDirect);

  const SStreamRecalcReq *pFirst = OnlyDispatch(DispatchFromTriggerHeartbeat());
  ASSERT_NE(pFirst, nullptr);
  EXPECT_EQ(pFirst->recalcId, kRecalcId);
  EXPECT_EQ(pFirst->start, 100);
  EXPECT_EQ(pFirst->end, 200);

  const SStreamRecalcReq *pSecond = OnlyDispatch(DispatchFromTriggerHeartbeat());
  ASSERT_NE(pSecond, nullptr);
  EXPECT_EQ(pSecond->recalcId, kRecalcId);

  SStreamRecalcSnapshot pending = {};
  pending.recalcId = kRecalcId;
  pending.start = 100;
  pending.end = 200;
  pending.status = STREAM_RECALC_STATUS_PENDING;
  SArray *pSnapshots = Snapshots(&pending);
  ASSERT_NE(pSnapshots, nullptr);
  ASSERT_EQ(mndStreamRecalcApplySnapshot(&mnode_, kStreamId, stored_, kTriggerTaskId, kTriggerSeriousId, true,
                                         STREAM_RECALC_DETAIL_ABSENT, pSnapshots, nullptr),
            TSDB_CODE_SUCCESS);
  taosArrayDestroy(pSnapshots);
  EXPECT_TRUE(Record()->dispatchConfirmed);
  EXPECT_EQ(Record()->snapshot.status, STREAM_RECALC_STATUS_PENDING);
  EXPECT_EQ(taosArrayGetSize(DispatchFromTriggerHeartbeat()), 0);
}

TEST_F(MndStreamRecalcDispatchTest, MatchingSnapshotConfirmsDispatch) {
  ASSERT_NE(AddRecord(), nullptr);
  SStreamRecalcSnapshot snapshot = {};
  snapshot.recalcId = kRecalcId;
  snapshot.start = 100;
  snapshot.end = 200;
  snapshot.progressPct = 10;
  snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  ASSERT_EQ(ApplyHeartbeat(Snapshots(&snapshot)), TSDB_CODE_SUCCESS);

  ASSERT_NE(Record(), nullptr);
  EXPECT_TRUE(Record()->dispatchConfirmed);
  EXPECT_EQ(Record()->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(Record()->snapshot.progressPct, 10);
  EXPECT_EQ(taosArrayGetSize(DispatchFromTriggerHeartbeat()), 0);

  snapshot.progressPct = 20;
  SArray *pDetails = taosArrayInit(1, sizeof(SStreamRecalcDetail));
  ASSERT_NE(pDetails, nullptr);
  SStreamRecalcDetail detail = {};
  detail.recalcId = kRecalcId;
  detail.retryOrdinal = 2;
  detail.errorCode = TSDB_CODE_OUT_OF_MEMORY;
  detail.errorText = taosStrdup(tstrerror(detail.errorCode));
  ASSERT_NE(detail.errorText, nullptr);
  ASSERT_NE(taosArrayPush(pDetails, &detail), nullptr);
  ASSERT_EQ(ApplyHeartbeat(Snapshots(&snapshot), pDetails), TSDB_CODE_SUCCESS);
  EXPECT_EQ(Record()->snapshot.progressPct, 20);
  EXPECT_EQ(Record()->retryOrdinal, 2);
  EXPECT_EQ(Record()->errorCode, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_STREQ(Record()->errorText, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
}

TEST_F(MndStreamRecalcDispatchTest, MatchingSnapshotRequiresCurrentTriggerIdentity) {
  ASSERT_NE(AddRecord(), nullptr);
  SStreamRecalcSnapshot snapshot = {};
  snapshot.recalcId = kRecalcId;
  snapshot.start = 100;
  snapshot.end = 200;
  snapshot.progressPct = 10;
  snapshot.status = STREAM_RECALC_STATUS_RUNNING;

  ASSERT_EQ(ApplyHeartbeat(Snapshots(&snapshot), nullptr, kTriggerTaskId - 1, kTriggerSeriousId), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(Record()->dispatchConfirmed);
  EXPECT_EQ(Record()->snapshot.status, STREAM_RECALC_STATUS_PENDING);

  ASSERT_EQ(ApplyHeartbeat(Snapshots(&snapshot), nullptr, kTriggerTaskId, kTriggerSeriousId - 1), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(Record()->dispatchConfirmed);
  EXPECT_EQ(Record()->snapshot.status, STREAM_RECALC_STATUS_PENDING);
}

TEST_F(MndStreamRecalcDispatchTest, RangeMismatchDoesNotConfirmOrUpdate) {
  ASSERT_NE(AddRecord(), nullptr);
  SStreamRecalcSnapshot snapshot = {};
  snapshot.recalcId = kRecalcId;
  snapshot.start = 101;
  snapshot.end = 200;
  snapshot.progressPct = 10;
  snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  ASSERT_EQ(ApplyHeartbeat(Snapshots(&snapshot)), TSDB_CODE_SUCCESS);

  EXPECT_FALSE(Record()->dispatchConfirmed);
  EXPECT_EQ(Record()->snapshot.status, STREAM_RECALC_STATUS_PENDING);
  EXPECT_EQ(Record()->snapshot.progressPct, 0);
}

TEST_F(MndStreamRecalcDispatchTest, ConfirmedRangeConflictRearmsExactDispatch) {
  ASSERT_NE(AddRecord(), nullptr);
  SStreamRecalcSnapshot snapshot = {};
  snapshot.recalcId = kRecalcId;
  snapshot.start = 100;
  snapshot.end = 200;
  snapshot.progressPct = 10;
  snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  ASSERT_EQ(ApplyHeartbeat(Snapshots(&snapshot)), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(Record()->dispatchConfirmed);

  snapshot.start = 101;
  snapshot.progressPct = 20;
  ASSERT_EQ(ApplyHeartbeat(Snapshots(&snapshot)), TSDB_CODE_SUCCESS);

  EXPECT_FALSE(Record()->dispatchConfirmed);
  const SStreamRecalcReq *pDispatch = OnlyDispatch(DispatchFromTriggerHeartbeat());
  ASSERT_NE(pDispatch, nullptr);
  EXPECT_EQ(pDispatch->recalcId, kRecalcId);
  EXPECT_EQ(pDispatch->start, 100);
  EXPECT_EQ(pDispatch->end, 200);
}

TEST_F(MndStreamRecalcDispatchTest, UnknownIdDoesNotCreateRuntimeRecord) {
  ASSERT_NE(AddRecord(), nullptr);
  SStreamRecalcSnapshot snapshot = {};
  snapshot.recalcId = kRecalcId + 1;
  snapshot.start = 100;
  snapshot.end = 200;
  snapshot.progressPct = 10;
  snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  ASSERT_EQ(ApplyHeartbeat(Snapshots(&snapshot)), TSDB_CODE_SUCCESS);

  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 1);
  EXPECT_EQ(Record(kRecalcId + 1), nullptr);
}

TEST_F(MndStreamRecalcDispatchTest, CompleteSnapshotAbsenceClearsConfirmation) {
  SStmRecalcRecord *pRecord = AddRecord();
  ASSERT_NE(pRecord, nullptr);
  pRecord->dispatchConfirmed = true;
  ASSERT_EQ(ApplyHeartbeat(Snapshots()), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(Record()->dispatchConfirmed);
  ASSERT_NE(OnlyDispatch(DispatchFromTriggerHeartbeat()), nullptr);
}

TEST_F(MndStreamRecalcDispatchTest, IncompleteOrInvalidSnapshotCannotClearConfirmation) {
  SStmRecalcRecord *pRecord = AddRecord();
  ASSERT_NE(pRecord, nullptr);
  pRecord->dispatchConfirmed = true;

  ASSERT_EQ(mndStreamRecalcApplySnapshot(&mnode_, kStreamId, stored_, kTriggerTaskId, kTriggerSeriousId, false,
                                         STREAM_RECALC_DETAIL_ABSENT, nullptr, nullptr),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(Record()->dispatchConfirmed);

  ASSERT_EQ(ApplyHeartbeat(nullptr, nullptr, kTriggerTaskId, kTriggerSeriousId, TSDB_CODE_INVALID_MSG),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(Record()->dispatchConfirmed);

  SStreamRecalcSnapshot invalid = {};
  invalid.recalcId = kRecalcId + 1;
  invalid.start = 100;
  invalid.end = 200;
  invalid.progressPct = 100;
  invalid.status = STREAM_RECALC_STATUS_RUNNING;
  SArray *pInvalid = Snapshots(&invalid);
  ASSERT_NE(pInvalid, nullptr);
  ASSERT_EQ(mndStreamRecalcApplySnapshot(&mnode_, kStreamId, stored_, kTriggerTaskId, kTriggerSeriousId, true,
                                         STREAM_RECALC_DETAIL_ABSENT, pInvalid, nullptr),
            TSDB_CODE_SUCCESS);
  taosArrayDestroy(pInvalid);
  EXPECT_TRUE(Record()->dispatchConfirmed);

  ASSERT_NE(AddRecord(kRecalcId + 1), nullptr);
  Record(kRecalcId + 1)->dispatchConfirmed = true;
  SStreamRecalcSnapshot valid = {};
  valid.recalcId = kRecalcId;
  valid.start = 100;
  valid.end = 200;
  valid.progressPct = 20;
  valid.status = STREAM_RECALC_STATUS_RUNNING;
  SArray *pValidSnapshots = Snapshots(&valid);
  SArray *pInvalidDetails = taosArrayInit(1, sizeof(SStreamRecalcDetail));
  ASSERT_NE(pValidSnapshots, nullptr);
  ASSERT_NE(pInvalidDetails, nullptr);
  SStreamRecalcDetail invalidDetail = {};
  invalidDetail.recalcId = kRecalcId;
  invalidDetail.retryOrdinal = 4;
  ASSERT_NE(taosArrayPush(pInvalidDetails, &invalidDetail), nullptr);
  ASSERT_EQ(mndStreamRecalcApplySnapshot(&mnode_, kStreamId, stored_, kTriggerTaskId, kTriggerSeriousId, true,
                                         STREAM_RECALC_DETAIL_INVALID, pValidSnapshots, pInvalidDetails),
            TSDB_CODE_SUCCESS);
  taosArrayDestroy(pInvalidDetails);
  taosArrayDestroy(pValidSnapshots);
  EXPECT_EQ(Record()->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(Record()->snapshot.progressPct, 20);
  EXPECT_TRUE(Record(kRecalcId + 1)->dispatchConfirmed);
}

TEST_F(MndStreamRecalcDispatchTest, InvalidSerializedDetailKeepsFixedAndCannotClearAbsence) {
  ASSERT_NE(AddRecord(), nullptr);
  ASSERT_NE(AddRecord(kRecalcId + 1), nullptr);
  Record()->retryOrdinal = 2;
  Record()->errorCode = TSDB_CODE_RPC_TIMEOUT;
  tstrncpy(Record()->errorText, tstrerror(Record()->errorCode), sizeof(Record()->errorText));
  Record(kRecalcId + 1)->dispatchConfirmed = true;
  SStreamRecalcSnapshot snapshot = {};
  snapshot.recalcId = kRecalcId;
  snapshot.start = 100;
  snapshot.end = 200;
  snapshot.progressPct = 20;
  snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  SArray *pDetails = ErrorDetails(kRecalcId, 1, TSDB_CODE_OUT_OF_MEMORY);
  ASSERT_NE(pDetails, nullptr);

  ASSERT_EQ(ApplySerializedHeartbeat(Snapshots(&snapshot), pDetails, DetailWireMutation::kInvalidRetry),
            TSDB_CODE_SUCCESS);

  EXPECT_EQ(Record()->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(Record()->snapshot.progressPct, 20);
  EXPECT_EQ(Record()->retryOrdinal, 2);
  EXPECT_EQ(Record()->errorCode, TSDB_CODE_RPC_TIMEOUT);
  EXPECT_STREQ(Record()->errorText, tstrerror(TSDB_CODE_RPC_TIMEOUT));
  EXPECT_TRUE(Record(kRecalcId + 1)->dispatchConfirmed);
}

TEST_F(MndStreamRecalcDispatchTest, UnknownSerializedDetailVersionCannotClearAbsence) {
  ASSERT_NE(AddRecord(), nullptr);
  ASSERT_NE(AddRecord(kRecalcId + 1), nullptr);
  Record()->retryOrdinal = 2;
  Record()->errorCode = TSDB_CODE_RPC_TIMEOUT;
  tstrncpy(Record()->errorText, tstrerror(Record()->errorCode), sizeof(Record()->errorText));
  Record(kRecalcId + 1)->dispatchConfirmed = true;
  SStreamRecalcSnapshot snapshot = {};
  snapshot.recalcId = kRecalcId;
  snapshot.start = 100;
  snapshot.end = 200;
  snapshot.progressPct = 20;
  snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  SArray *pDetails = taosArrayInit(0, sizeof(SStreamRecalcDetail));
  ASSERT_NE(pDetails, nullptr);

  ASSERT_EQ(ApplySerializedHeartbeat(Snapshots(&snapshot), pDetails, DetailWireMutation::kUnknownVersion),
            TSDB_CODE_SUCCESS);

  EXPECT_EQ(Record()->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(Record()->snapshot.progressPct, 20);
  EXPECT_EQ(Record()->retryOrdinal, 2);
  EXPECT_EQ(Record()->errorCode, TSDB_CODE_RPC_TIMEOUT);
  EXPECT_STREQ(Record()->errorText, tstrerror(TSDB_CODE_RPC_TIMEOUT));
  EXPECT_TRUE(Record(kRecalcId + 1)->dispatchConfirmed);
}

TEST_F(MndStreamRecalcDispatchTest, RecognizedDetailSetClearsStaleErrorAndFinishedCandidateIsSuccessful) {
  ASSERT_NE(AddRecord(), nullptr);
  SStreamRecalcSnapshot snapshot = {};
  snapshot.recalcId = kRecalcId;
  snapshot.start = 100;
  snapshot.end = 200;
  snapshot.progressPct = 10;
  snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  SArray *pErrorDetails = ErrorDetails(kRecalcId, 2, TSDB_CODE_OUT_OF_MEMORY);
  ASSERT_NE(pErrorDetails, nullptr);
  ASSERT_EQ(ApplySerializedHeartbeat(Snapshots(&snapshot), pErrorDetails), TSDB_CODE_SUCCESS);
  ASSERT_EQ(Record()->retryOrdinal, 2);
  ASSERT_EQ(Record()->errorCode, TSDB_CODE_OUT_OF_MEMORY);

  snapshot.progressPct = 20;
  ASSERT_EQ(ApplySerializedHeartbeat(Snapshots(&snapshot), nullptr), TSDB_CODE_SUCCESS);
  EXPECT_EQ(Record()->retryOrdinal, 2);
  EXPECT_EQ(Record()->errorCode, TSDB_CODE_OUT_OF_MEMORY);

  snapshot.progressPct = 30;
  SArray *pEmptyDetails = taosArrayInit(0, sizeof(SStreamRecalcDetail));
  ASSERT_NE(pEmptyDetails, nullptr);
  ASSERT_EQ(ApplySerializedHeartbeat(Snapshots(&snapshot), pEmptyDetails), TSDB_CODE_SUCCESS);
  EXPECT_EQ(Record()->retryOrdinal, 0);
  EXPECT_EQ(Record()->errorCode, 0);
  EXPECT_STREQ(Record()->errorText, "");

  gFinishLockStatus = stored_;
  gFinishCalled = false;
  gFinishRetryOrdinal = -1;
  gFinishErrorCode = TSDB_CODE_OUT_OF_MEMORY;
  tstrncpy(gFinishErrorText, tstrerror(gFinishErrorCode), sizeof(gFinishErrorText));
  finishStub_.set(mndStreamRecalcFinish, CaptureFinishUnlocked);
  snapshot.progressPct = 100;
  snapshot.status = STREAM_RECALC_STATUS_FINISHED;
  pEmptyDetails = taosArrayInit(0, sizeof(SStreamRecalcDetail));
  ASSERT_NE(pEmptyDetails, nullptr);
  ASSERT_EQ(ApplySerializedHeartbeat(Snapshots(&snapshot), pEmptyDetails), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(gFinishCalled);
  EXPECT_EQ(gFinishRetryOrdinal, 0);
  EXPECT_EQ(gFinishErrorCode, 0);
  EXPECT_STREQ(gFinishErrorText, "");
}

TEST_F(MndStreamRecalcDispatchTest, MalformedFixedDoesNotClearAbsenceAndValidSiblingApplies) {
  ASSERT_NE(AddRecord(), nullptr);
  ASSERT_NE(AddRecord(kRecalcId + 1), nullptr);
  Record(kRecalcId + 1)->dispatchConfirmed = true;
  SArray *pSnapshots = taosArrayInit(2, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(pSnapshots, nullptr);
  SStreamRecalcSnapshot valid = {};
  valid.recalcId = kRecalcId;
  valid.start = 100;
  valid.end = 200;
  valid.progressPct = 10;
  valid.status = STREAM_RECALC_STATUS_RUNNING;
  SStreamRecalcSnapshot malformed = valid;
  malformed.recalcId = kRecalcId + 2;
  malformed.start = 300;
  malformed.end = 300;
  ASSERT_NE(taosArrayPush(pSnapshots, &valid), nullptr);
  ASSERT_NE(taosArrayPush(pSnapshots, &malformed), nullptr);

  ASSERT_EQ(ApplySerializedHeartbeat(pSnapshots, nullptr), TSDB_CODE_SUCCESS);

  EXPECT_EQ(Record()->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(Record()->snapshot.progressPct, 10);
  EXPECT_TRUE(Record(kRecalcId + 1)->dispatchConfirmed);
}

TEST_F(MndStreamRecalcDispatchTest, DuplicateFixedIdDoesNotClearAbsenceAndValidSiblingApplies) {
  ASSERT_NE(AddRecord(), nullptr);
  ASSERT_NE(AddRecord(kRecalcId + 1), nullptr);
  Record(kRecalcId + 1)->dispatchConfirmed = true;
  SArray *pSnapshots = taosArrayInit(3, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(pSnapshots, nullptr);
  SStreamRecalcSnapshot valid = {};
  valid.recalcId = kRecalcId;
  valid.start = 100;
  valid.end = 200;
  valid.progressPct = 10;
  valid.status = STREAM_RECALC_STATUS_RUNNING;
  SStreamRecalcSnapshot duplicate = valid;
  duplicate.recalcId = kRecalcId + 2;
  duplicate.start = 300;
  duplicate.end = 400;
  ASSERT_NE(taosArrayPush(pSnapshots, &valid), nullptr);
  ASSERT_NE(taosArrayPush(pSnapshots, &duplicate), nullptr);
  duplicate.progressPct = 20;
  ASSERT_NE(taosArrayPush(pSnapshots, &duplicate), nullptr);

  ASSERT_EQ(ApplySerializedHeartbeat(pSnapshots, nullptr), TSDB_CODE_SUCCESS);

  EXPECT_EQ(Record()->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(Record()->snapshot.progressPct, 10);
  EXPECT_TRUE(Record(kRecalcId + 1)->dispatchConfirmed);
}

TEST_F(MndStreamRecalcDispatchTest, LargeSnapshotUsesBoundedRecordLookups) {
  constexpr int32_t kCount = 128;
  SArray           *pSnapshots = taosArrayInit(kCount, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(pSnapshots, nullptr);
  for (int32_t i = 0; i < kCount; ++i) {
    const int64_t recalcId = kRecalcId + i;
    const TSKEY   start = 100 + i * 10;
    ASSERT_NE(AddRecord(recalcId, start, start + 5), nullptr);
    SStreamRecalcSnapshot snapshot = {};
    snapshot.recalcId = recalcId;
    snapshot.start = start;
    snapshot.end = start + 5;
    snapshot.progressPct = 1;
    snapshot.status = STREAM_RECALC_STATUS_RUNNING;
    ASSERT_NE(taosArrayPush(pSnapshots, &snapshot), nullptr);
  }

  int32_t code = TSDB_CODE_SUCCESS;
  {
    RecalcRecordArrayGetGuard guard;
    code = ApplyHeartbeat(pSnapshots);
  }

  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_LE(gRecalcRecordArrayGetCalls, kCount * 6);
}

TEST_F(MndStreamRecalcDispatchTest, LargeTerminalSnapshotUsesBoundedRecordLookups) {
  constexpr int32_t kCount = 128;
  SArray           *pSnapshots = taosArrayInit(kCount, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(pSnapshots, nullptr);
  mndStreamRecalcInitStatus(stored_);
  stored_->recalcTransActive = true;
  for (int32_t i = 0; i < kCount; ++i) {
    const int64_t recalcId = kRecalcId + i;
    const TSKEY   start = 100 + i * 10;
    ASSERT_NE(AddRecord(recalcId, start, start + 5), nullptr);
    SStreamRecalcSnapshot snapshot = {};
    snapshot.recalcId = recalcId;
    snapshot.start = start;
    snapshot.end = start + 5;
    snapshot.progressPct = 100;
    snapshot.status = STREAM_RECALC_STATUS_FINISHED;
    ASSERT_NE(taosArrayPush(pSnapshots, &snapshot), nullptr);
  }

  int32_t code = TSDB_CODE_SUCCESS;
  {
    RecalcRecordArrayGetGuard guard;
    code = ApplyHeartbeat(pSnapshots);
  }

  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_LE(gRecalcRecordArrayGetCalls, kCount * 12);
  EXPECT_EQ(listNEles(&stored_->recalcPersistOps), kCount);
}

TEST_F(MndStreamRecalcDispatchTest, TerminalSnapshotCallsFinishWithoutRuntimeLock) {
  SStmRecalcRecord *pRecord = AddRecord();
  ASSERT_NE(pRecord, nullptr);
  pRecord->snapshot.status = STREAM_RECALC_STATUS_PENDING;
  gFinishLockStatus = stored_;
  gFinishCalled = false;
  gFinishCalledWithoutLock = false;
  finishStub_.set(mndStreamRecalcFinish, CaptureFinishUnlocked);

  SStreamRecalcSnapshot snapshot = {};
  snapshot.recalcId = kRecalcId;
  snapshot.start = 100;
  snapshot.end = 200;
  snapshot.progressPct = 100;
  snapshot.status = STREAM_RECALC_STATUS_FINISHED;
  SArray *pSnapshots = Snapshots(&snapshot);
  ASSERT_NE(pSnapshots, nullptr);
  ASSERT_EQ(mndStreamRecalcApplySnapshot(&mnode_, kStreamId, stored_, kTriggerTaskId, kTriggerSeriousId, true,
                                         STREAM_RECALC_DETAIL_ABSENT, pSnapshots, nullptr),
            TSDB_CODE_SUCCESS);
  taosArrayDestroy(pSnapshots);

  EXPECT_TRUE(gFinishCalled);
  EXPECT_TRUE(gFinishCalledWithoutLock);
  EXPECT_EQ(Record()->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
}

SStreamObj *gRecoveryStreams[2] = {nullptr, nullptr};

int32_t returnRecoveryStreamById(SMnode *, int64_t streamId, SStreamObj **ppStream) {
  for (SStreamObj *pStream : gRecoveryStreams) {
    if (pStream != nullptr && pStream->pCreate != nullptr && pStream->pCreate->streamId == streamId) {
      *ppStream = pStream;
      return TSDB_CODE_SUCCESS;
    }
  }
  *ppStream = nullptr;
  return TSDB_CODE_MND_STREAM_NOT_EXIST;
}

int32_t returnRecoveryStreamByName(SMnode *, char *streamName, SStreamObj **ppStream) {
  for (SStreamObj *pStream : gRecoveryStreams) {
    if (pStream != nullptr && strcmp(pStream->name, streamName) == 0) {
      *ppStream = pStream;
      return TSDB_CODE_SUCCESS;
    }
  }
  *ppStream = nullptr;
  return TSDB_CODE_MND_STREAM_NOT_EXIST;
}

void ignoreRecoveryStreamRelease(SMnode *, SStreamObj *) {}

int32_t returnRecoverySnodeId(SMnode *, SStreamObj *, bool) { return 1; }

class MndStreamRecalcRecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    SaveRuntime();
    InitRuntime();
    streams_[0] = BuildMinimalStreamObj();
    streams_[1] = BuildMinimalStreamObj();
    ASSERT_NE(streams_[0].pCreate, nullptr);
    ASSERT_NE(streams_[1].pCreate, nullptr);
    streams_[0].pCreate->streamId = kStreamA;
    streams_[1].pCreate->streamId = kStreamB;
    taosMemoryFreeClear(streams_[0].pCreate->name);
    taosMemoryFreeClear(streams_[1].pCreate->name);
    streams_[0].pCreate->name = taosStrdup("1.test.recovery_a");
    streams_[1].pCreate->name = taosStrdup("1.test.recovery_b");
    tstrncpy(streams_[0].name, streams_[0].pCreate->name, sizeof(streams_[0].name));
    tstrncpy(streams_[1].name, streams_[1].pCreate->name, sizeof(streams_[1].name));
    gRecoveryStreams[0] = &streams_[0];
    gRecoveryStreams[1] = &streams_[1];
    stub_.set(mndAcquireStreamById, returnRecoveryStreamById);
    stub_.set(mndAcquireStream, returnRecoveryStreamByName);
    stub_.set(mndReleaseStream, ignoreRecoveryStreamRelease);
    stub_.set(msmAssignTaskSnodeId, returnRecoverySnodeId);
  }

  void TearDown() override {
    stub_.reset(msmAssignTaskSnodeId);
    stub_.reset(mndReleaseStream);
    stub_.reset(mndAcquireStream);
    stub_.reset(mndAcquireStreamById);
    gRecoveryStreams[0] = nullptr;
    gRecoveryStreams[1] = nullptr;

    CleanupStatuses();
    taosHashCleanup(actionStm_);
    taosHashCleanup(deployStm_);
    taosHashCleanup(mStreamMgmt.toDeploySnodeMap);
    taosHashCleanup(mStreamMgmt.toDeployVgMap);
    taosHashCleanup(mStreamMgmt.dnodeMap);
    taosHashCleanup(mStreamMgmt.snodeMap);
    taosHashCleanup(mStreamMgmt.vgroupMap);
    taosHashCleanup(mStreamMgmt.taskMap);
    taosHashCleanup(mStreamMgmt.streamMap);
    DrainActionQueue();
    tFreeStreamObj(&streams_[0]);
    tFreeStreamObj(&streams_[1]);
    RestoreRuntime();
  }

  void SaveRuntime() {
    savedStreamMap_ = mStreamMgmt.streamMap;
    savedTaskMap_ = mStreamMgmt.taskMap;
    savedVgroupMap_ = mStreamMgmt.vgroupMap;
    savedSnodeMap_ = mStreamMgmt.snodeMap;
    savedDnodeMap_ = mStreamMgmt.dnodeMap;
    savedToDeployVgMap_ = mStreamMgmt.toDeployVgMap;
    savedToDeploySnodeMap_ = mStreamMgmt.toDeploySnodeMap;
    savedActionQ_ = mStreamMgmt.actionQ;
    savedActionQLock_ = mStreamMgmt.actionQLock;
    savedLastTaskId_ = mStreamMgmt.lastTaskId;
    savedActive_ = mStreamMgmt.active;
    savedState_ = mStreamMgmt.state;
    savedToDeployVgTaskNum_ = mStreamMgmt.toDeployVgTaskNum;
    savedToDeploySnodeTaskNum_ = mStreamMgmt.toDeploySnodeTaskNum;
  }

  void InitRuntime() {
    mStreamMgmt.streamMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    mStreamMgmt.taskMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    mStreamMgmt.vgroupMap = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    mStreamMgmt.snodeMap = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    mStreamMgmt.dnodeMap = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    mStreamMgmt.toDeployVgMap = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    mStreamMgmt.toDeploySnodeMap = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    actionStm_ = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    deployStm_ = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    ASSERT_NE(mStreamMgmt.streamMap, nullptr);
    ASSERT_NE(mStreamMgmt.taskMap, nullptr);
    ASSERT_NE(mStreamMgmt.vgroupMap, nullptr);
    ASSERT_NE(mStreamMgmt.snodeMap, nullptr);
    ASSERT_NE(mStreamMgmt.dnodeMap, nullptr);
    ASSERT_NE(mStreamMgmt.toDeployVgMap, nullptr);
    ASSERT_NE(mStreamMgmt.toDeploySnodeMap, nullptr);
    ASSERT_NE(actionStm_, nullptr);
    ASSERT_NE(deployStm_, nullptr);
    taosHashSetFreeFp(mStreamMgmt.vgroupMap, mstDestroySStmVgroupStatus);
    taosHashSetFreeFp(mStreamMgmt.snodeMap, mstDestroySStmSnodeStatus);
    taosHashSetFreeFp(mStreamMgmt.toDeployVgMap, mstDestroySStmVgTasksToDeploy);
    taosHashSetFreeFp(mStreamMgmt.toDeploySnodeMap, mstDestroySStmSnodeTasksDeploy);
    taosHashSetFreeFp(actionStm_, mstDestroySStmAction);
    taosHashSetFreeFp(deployStm_, tDeepFreeSStmStreamDeploy);

    actionQueue_.head = static_cast<SStmQNode *>(taosMemoryCalloc(1, sizeof(SStmQNode)));
    ASSERT_NE(actionQueue_.head, nullptr);
    actionQueue_.tail = actionQueue_.head;
    mStreamMgmt.actionQ = &actionQueue_;
    mStreamMgmt.actionQLock = 0;
    mStreamMgmt.lastTaskId = 100;
    mStreamMgmt.active = 1;
    mStreamMgmt.state = MND_STM_STATE_NORMAL;
    mStreamMgmt.toDeployVgTaskNum = 0;
    mStreamMgmt.toDeploySnodeTaskNum = 0;

    SStmSnodeStatus snode = {};
    const int32_t   snodeId = 1;
    ASSERT_EQ(taosHashPut(mStreamMgmt.snodeMap, &snodeId, sizeof(snodeId), &snode, sizeof(snode)), TSDB_CODE_SUCCESS);
    const int32_t dnodeId = 1;
    const int64_t lastUpTs = 0;
    ASSERT_EQ(taosHashPut(mStreamMgmt.dnodeMap, &dnodeId, sizeof(dnodeId), &lastUpTs, sizeof(lastUpTs)),
              TSDB_CODE_SUCCESS);
  }

  void RestoreRuntime() {
    mStreamMgmt.streamMap = savedStreamMap_;
    mStreamMgmt.taskMap = savedTaskMap_;
    mStreamMgmt.vgroupMap = savedVgroupMap_;
    mStreamMgmt.snodeMap = savedSnodeMap_;
    mStreamMgmt.dnodeMap = savedDnodeMap_;
    mStreamMgmt.toDeployVgMap = savedToDeployVgMap_;
    mStreamMgmt.toDeploySnodeMap = savedToDeploySnodeMap_;
    mStreamMgmt.actionQ = savedActionQ_;
    mStreamMgmt.actionQLock = savedActionQLock_;
    mStreamMgmt.lastTaskId = savedLastTaskId_;
    mStreamMgmt.active = savedActive_;
    mStreamMgmt.state = savedState_;
    mStreamMgmt.toDeployVgTaskNum = savedToDeployVgTaskNum_;
    mStreamMgmt.toDeploySnodeTaskNum = savedToDeploySnodeTaskNum_;
  }

  void DrainActionQueue() {
    SStmQNode *pNode = nullptr;
    while (mndStreamActionDequeue(&actionQueue_, &pNode)) {
    }
    taosMemoryFreeClear(actionQueue_.head);
    actionQueue_.tail = nullptr;
  }

  void CleanupStatuses() {
    const int64_t ids[] = {kStreamA, kStreamB};
    for (int64_t streamId : ids) {
      auto *pStatus = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId)));
      if (pStatus == nullptr) continue;
      mstResetSStmStatus(pStatus);
      taosArrayDestroy(pStatus->recalcRecords);
      pStatus->recalcRecords = nullptr;
      if (pStatus->recalcPersistOpsInitialized) tdListEmpty(&pStatus->recalcPersistOps);
      pStatus->recalcPersistOpsInitialized = false;
    }
  }

  SStmStatus *AddStatus(int64_t streamId) {
    SStreamObj *pStream = Stream(streamId);
    if (pStream == nullptr) return nullptr;
    SStmStatus status = {};
    status.streamName = pStream->name;
    status.pCreate = pStream->pCreate;
    if (taosHashPut(mStreamMgmt.streamMap, &streamId, sizeof(streamId), &status, sizeof(status)) != TSDB_CODE_SUCCESS) {
      return nullptr;
    }
    return static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId)));
  }

  SStreamObj *Stream(int64_t streamId) {
    for (SStreamObj &stream : streams_) {
      if (stream.pCreate != nullptr && stream.pCreate->streamId == streamId) return &stream;
    }
    return nullptr;
  }

  void AddPersisted(int64_t streamId, int64_t recalcId = kRecalcId, TSKEY start = 100, TSKEY end = 200,
                    int64_t requestTimeMs = kRequestTimeMs) {
    SStreamObj *pStream = Stream(streamId);
    ASSERT_NE(pStream, nullptr);
    if (pStream->pIncompleteRecalcs == nullptr) {
      pStream->pIncompleteRecalcs = taosArrayInit(2, sizeof(SStreamRecalcPersistReq));
    }
    ASSERT_NE(pStream->pIncompleteRecalcs, nullptr);
    SStreamRecalcPersistReq request = {};
    request.recalcId = recalcId;
    request.start = start;
    request.end = end;
    request.requestTimeMs = requestTimeMs;
    ASSERT_NE(taosArrayPush(pStream->pIncompleteRecalcs, &request), nullptr);
  }

  static SStmRecalcRecord *FindRecord(SStmStatus *pStatus, int64_t recalcId = kRecalcId) {
    for (int32_t i = 0; pStatus != nullptr && i < taosArrayGetSize(pStatus->recalcRecords); ++i) {
      auto *pRecord = static_cast<SStmRecalcRecord *>(taosArrayGet(pStatus->recalcRecords, i));
      if (pRecord != nullptr && pRecord->snapshot.recalcId == recalcId) return pRecord;
    }
    return nullptr;
  }

  static void AddRuntimeRecord(SStmStatus *pStatus, int64_t taskId, int64_t seriousId) {
    pStatus->recalcRecords = taosArrayInit(2, sizeof(SStmRecalcRecord));
    ASSERT_NE(pStatus->recalcRecords, nullptr);
    SStmRecalcRecord record = {};
    record.snapshot.recalcId = kRecalcId;
    record.snapshot.start = 100;
    record.snapshot.end = 200;
    record.snapshot.progressPct = 64;
    record.snapshot.status = STREAM_RECALC_STATUS_RUNNING;
    record.requestTimeMs = kRequestTimeMs;
    record.triggerTaskId = taskId;
    record.triggerSeriousId = seriousId;
    record.retryOrdinal = 2;
    record.errorCode = TSDB_CODE_OUT_OF_MEMORY;
    tstrncpy(record.errorText, tstrerror(record.errorCode), sizeof(record.errorText));
    record.typedStatusKnown = true;
    record.visible = true;
    record.dispatchConfirmed = true;
    ASSERT_NE(taosArrayPush(pStatus->recalcRecords, &record), nullptr);
  }

  static constexpr int64_t kStreamA = 42;
  static constexpr int64_t kStreamB = 43;
  static constexpr int64_t kRecalcId = 0x7788;
  static constexpr int64_t kRequestTimeMs = 1710000000123;
  SMnode                   mnode_ = {};
  SStreamObj               streams_[2] = {};
  SStmActionQ              actionQueue_ = {};
  SHashObj                *actionStm_ = nullptr;
  SHashObj                *deployStm_ = nullptr;
  SHashObj                *savedStreamMap_ = nullptr;
  SHashObj                *savedTaskMap_ = nullptr;
  SHashObj                *savedVgroupMap_ = nullptr;
  SHashObj                *savedSnodeMap_ = nullptr;
  SHashObj                *savedDnodeMap_ = nullptr;
  SHashObj                *savedToDeployVgMap_ = nullptr;
  SHashObj                *savedToDeploySnodeMap_ = nullptr;
  SStmActionQ             *savedActionQ_ = nullptr;
  SRWLatch                 savedActionQLock_ = 0;
  int64_t                  savedLastTaskId_ = 0;
  int8_t                   savedActive_ = 0;
  int8_t                   savedState_ = 0;
  int32_t                  savedToDeployVgTaskNum_ = 0;
  int32_t                  savedToDeploySnodeTaskNum_ = 0;
  Stub                     stub_;
};

constexpr int64_t MndStreamRecalcRecoveryTest::kStreamA;
constexpr int64_t MndStreamRecalcRecoveryTest::kStreamB;
constexpr int64_t MndStreamRecalcRecoveryTest::kRecalcId;
constexpr int64_t MndStreamRecalcRecoveryTest::kRequestTimeMs;

TEST_F(MndStreamRecalcRecoveryTest, NewRuntimeRestoresOriginalIdRangeAndRequestTime) {
  AddPersisted(kStreamA);
  SStmStatus status = {};
  ASSERT_EQ(mndStreamRecalcRestore(Stream(kStreamA), &status, 55, 66), TSDB_CODE_SUCCESS);

  const SStmRecalcRecord *pRecord = FindRecord(&status);
  ASSERT_NE(pRecord, nullptr);
  EXPECT_EQ(pRecord->snapshot.recalcId, kRecalcId);
  EXPECT_EQ(pRecord->snapshot.start, 100);
  EXPECT_EQ(pRecord->snapshot.end, 200);
  EXPECT_EQ(pRecord->snapshot.status, STREAM_RECALC_STATUS_PENDING);
  EXPECT_EQ(pRecord->snapshot.progressPct, 0);
  EXPECT_EQ(pRecord->requestTimeMs, kRequestTimeMs);
  EXPECT_FALSE(pRecord->dispatchConfirmed);
  taosArrayDestroy(status.recalcRecords);
}

TEST_F(MndStreamRecalcRecoveryTest, RedeployResetsProgressRetryAndDispatchConfirmation) {
  AddPersisted(kStreamA);
  SStmStatus *pStatus = AddStatus(kStreamA);
  ASSERT_NE(pStatus, nullptr);
  AddRuntimeRecord(pStatus, 11, 22);
  ASSERT_EQ(mstPostStreamAction(&actionQueue_, kStreamA, Stream(kStreamA)->name, nullptr, false, STREAM_ACT_DEPLOY),
            TSDB_CODE_SUCCESS);

  SStreamHbMsg req = {};
  req.dnodeId = 1;
  req.snodeId = 0;
  req.pVgLeaders = taosArrayInit(0, sizeof(int32_t));
  req.pStreamReq = taosArrayInit(0, sizeof(int32_t));
  req.pStreamStatus = taosArrayInit(0, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(req.pVgLeaders, nullptr);
  ASSERT_NE(req.pStreamReq, nullptr);
  ASSERT_NE(req.pStreamStatus, nullptr);
  SStmGrpCtx ctx = {};
  ctx.pMnode = &mnode_;
  ctx.currTs = 100;
  ctx.pReq = &req;
  ctx.actionStm = actionStm_;
  ctx.deployStm = deployStm_;
  ASSERT_EQ(msmNormalHandleHbMsg(&ctx), TSDB_CODE_SUCCESS);
  tCleanupStreamHbMsg(&req, true);

  pStatus = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(pStatus, nullptr);
  ASSERT_NE(pStatus->triggerTask, nullptr);
  const SStmRecalcRecord *pRecord = FindRecord(pStatus);
  ASSERT_NE(pRecord, nullptr);
  EXPECT_EQ(pRecord->snapshot.status, STREAM_RECALC_STATUS_PENDING);
  EXPECT_EQ(pRecord->snapshot.progressPct, 0);
  EXPECT_EQ(pRecord->retryOrdinal, 0);
  EXPECT_EQ(pRecord->errorCode, 0);
  EXPECT_STREQ(pRecord->errorText, "");
  EXPECT_FALSE(pRecord->dispatchConfirmed);
  EXPECT_EQ(pRecord->triggerTaskId, pStatus->triggerTask->id.taskId);
  EXPECT_EQ(pRecord->triggerSeriousId, pStatus->triggerTask->id.seriousId);
}

TEST_F(MndStreamRecalcRecoveryTest, ValidRuntimeForCurrentDeploymentIsPreserved) {
  AddPersisted(kStreamA);
  SStmStatus status = {};
  AddRuntimeRecord(&status, 55, 66);
  ASSERT_EQ(mndStreamRecalcRestore(Stream(kStreamA), &status, 55, 66), TSDB_CODE_SUCCESS);

  const SStmRecalcRecord *pRecord = FindRecord(&status);
  ASSERT_NE(pRecord, nullptr);
  EXPECT_EQ(pRecord->snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(pRecord->snapshot.progressPct, 64);
  EXPECT_EQ(pRecord->retryOrdinal, 2);
  EXPECT_EQ(pRecord->errorCode, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_TRUE(pRecord->dispatchConfirmed);
  taosArrayDestroy(status.recalcRecords);
}

TEST_F(MndStreamRecalcRecoveryTest, LargeRestoreUsesBoundedRecordLookups) {
  constexpr int32_t kCount = 128;
  SStmStatus        status = {};
  status.recalcRecords = taosArrayInit(kCount, sizeof(SStmRecalcRecord));
  ASSERT_NE(status.recalcRecords, nullptr);
  for (int32_t i = 0; i < kCount; ++i) {
    const int64_t recalcId = kRecalcId + i;
    const TSKEY   start = 100 + i * 10;
    AddPersisted(kStreamA, recalcId, start, start + 5, kRequestTimeMs + i);
    SStmRecalcRecord record = {};
    record.snapshot.recalcId = recalcId;
    record.snapshot.start = start;
    record.snapshot.end = start + 5;
    record.snapshot.status = STREAM_RECALC_STATUS_RUNNING;
    record.requestTimeMs = kRequestTimeMs + i;
    record.triggerTaskId = 55;
    record.triggerSeriousId = 66;
    record.typedStatusKnown = true;
    record.visible = true;
    ASSERT_NE(taosArrayPush(status.recalcRecords, &record), nullptr);
  }

  int32_t code = TSDB_CODE_SUCCESS;
  {
    RecalcRecordArrayGetGuard guard;
    code = mndStreamRecalcRestore(Stream(kStreamA), &status, 55, 66);
  }

  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_LE(gRecalcRecordArrayGetCalls, kCount * 5);
  EXPECT_EQ(taosArrayGetSize(status.recalcRecords), kCount);
  taosArrayDestroy(status.recalcRecords);
}

TEST_F(MndStreamRecalcRecoveryTest, WatchAdoptionRestoresAfterTriggerIdentityIsKnown) {
  AddPersisted(kStreamA);
  AddPersisted(kStreamB);
  SStmStatus *pUnknown = AddStatus(kStreamA);
  SStmStatus *pKnown = AddStatus(kStreamB);
  ASSERT_NE(pUnknown, nullptr);
  ASSERT_NE(pKnown, nullptr);
  SStmGrpCtx ctx = {};
  ctx.pMnode = &mnode_;
  ctx.currTs = 100;

  SStmTaskStatusMsg unknown = {};
  unknown.type = STREAM_TRIGGER_TASK;
  unknown.streamId = kStreamA;
  unknown.taskId = 55;
  unknown.seriousId = 0;
  unknown.nodeId = 1;
  unknown.status = STREAM_STATUS_RUNNING;
  ASSERT_EQ(msmWatchRecordNewTask(&ctx, &unknown), TSDB_CODE_SUCCESS);
  EXPECT_EQ(FindRecord(pUnknown), nullptr);

  SStmTaskStatusMsg known = unknown;
  known.streamId = kStreamB;
  known.taskId = 56;
  known.seriousId = 66;
  ASSERT_EQ(msmWatchRecordNewTask(&ctx, &known), TSDB_CODE_SUCCESS);
  const SStmRecalcRecord *pRecord = FindRecord(pKnown);
  ASSERT_NE(pRecord, nullptr);
  EXPECT_EQ(pRecord->triggerTaskId, 56);
  EXPECT_EQ(pRecord->triggerSeriousId, 66);
}

TEST_F(MndStreamRecalcRecoveryTest, UserStopKeepsPersistentRequests) {
  AddPersisted(kStreamA);
  SStmStatus *pStatus = AddStatus(kStreamA);
  ASSERT_NE(pStatus, nullptr);

  msmUndeployStream(&mnode_, kStreamA, Stream(kStreamA)->name);

  EXPECT_EQ(atomic_load_8(&pStatus->stopped), 2);
  ASSERT_EQ(taosArrayGetSize(Stream(kStreamA)->pIncompleteRecalcs), 1);
  const auto *pRequest =
      static_cast<const SStreamRecalcPersistReq *>(taosArrayGet(Stream(kStreamA)->pIncompleteRecalcs, 0));
  ASSERT_NE(pRequest, nullptr);
  EXPECT_EQ(pRequest->recalcId, kRecalcId);
  EXPECT_EQ(pRequest->requestTimeMs, kRequestTimeMs);
}

TEST_F(MndStreamRecalcRecoveryTest, StreamDropReleasesPersistentArrayWithObject) {
  SStreamObj stream = BuildMinimalStreamObj();
  ASSERT_NE(stream.pCreate, nullptr);
  stream.pIncompleteRecalcs = taosArrayInit(1, sizeof(SStreamRecalcPersistReq));
  ASSERT_NE(stream.pIncompleteRecalcs, nullptr);
  SStreamRecalcPersistReq request = {};
  request.recalcId = kRecalcId;
  request.start = 100;
  request.end = 200;
  request.requestTimeMs = kRequestTimeMs;
  ASSERT_NE(taosArrayPush(stream.pIncompleteRecalcs, &request), nullptr);

  tFreeStreamObj(&stream);

  EXPECT_EQ(stream.pIncompleteRecalcs, nullptr);
  EXPECT_EQ(stream.pCreate, nullptr);
}

TEST(StreamObjTest, RecalcTailRoundTripsAtVersionEight) {
  SStreamObj input = BuildMinimalStreamObj();
  ASSERT_NE(input.pCreate, nullptr);
  input.recalcRevision = 7;
  input.pIncompleteRecalcs = taosArrayInit(1, sizeof(SStreamRecalcPersistReq));
  SStreamRecalcPersistReq req = {};
  req.recalcId = 0x1234;
  req.start = 100;
  req.end = 200;
  req.requestTimeMs = 300;
  ASSERT_NE(taosArrayPush(input.pIncompleteRecalcs, &req), nullptr);

  SStreamObj output = RoundTripStreamObj(input, MND_STREAM_VER_NUMBER);
  EXPECT_EQ(output.recalcRevision, 7);
  ASSERT_EQ(taosArrayGetSize(output.pIncompleteRecalcs), 1);
  const auto *decoded = static_cast<const SStreamRecalcPersistReq *>(taosArrayGet(output.pIncompleteRecalcs, 0));
  EXPECT_EQ(decoded->recalcId, req.recalcId);
  EXPECT_EQ(decoded->start, req.start);
  EXPECT_EQ(decoded->end, req.end);
  EXPECT_EQ(decoded->requestTimeMs, req.requestTimeMs);
  tFreeStreamObj(&input);
  tFreeStreamObj(&output);
}

TEST(StreamObjTest, VersionEightObjectWithoutRecalcTailDecodesEmpty) {
  std::vector<uint8_t> legacy = EncodeStreamObjWithoutRecalcTail();
  ASSERT_FALSE(legacy.empty());
  SStreamObj output = DecodeStreamObj(legacy, MND_STREAM_VER_NUMBER);
  EXPECT_EQ(output.recalcRevision, 0);
  EXPECT_EQ(output.pIncompleteRecalcs, nullptr);
  tFreeStreamObj(&output);
}

TEST(StreamObjTest, OldDecoderSkipsVersionEightRecalcTail) {
  std::vector<uint8_t> current = EncodeStreamObjWithRecalcTail();
  ASSERT_FALSE(current.empty());
  EXPECT_EQ(DecodeLegacyStreamObjAndConsumeOuterObject(current), TSDB_CODE_SUCCESS);
}

TEST_F(StreamObjSdbUpdateTest, OlderRecalcRevisionCannotReplaceRequests) {
  SStreamObj stored = BuildStreamObjWithRequest(9, 0x91);
  SStreamObj stale = BuildStreamObjWithRequest(8, 0x81);
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  tFreeStreamObj(&stored);
  ASSERT_EQ(WriteStream(&stale, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  tFreeStreamObj(&stale);

  SStreamObj *persisted = AcquireStored();
  ASSERT_NE(persisted, nullptr);
  EXPECT_EQ(persisted->recalcRevision, 9);
  EXPECT_EQ(OnlyPersistedRecalcId(*persisted), 0x91);
  sdbRelease(pSdb_, persisted);
}

TEST_F(StreamObjSdbUpdateTest, StaleFullRevisionSkipsCopyAndCommitsLifecycle) {
  SStreamObj stored = BuildStreamObjWithRequest(12, 0xc1);
  stored.mainSnodeId = 17;
  stored.userStopped = 1;
  stored.ownerId = 23;
  stored.updateTime = 29;
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);

  SStreamObj stale = BuildStreamObjWithRequest(11, 0xb1);
  stale.mainSnodeId = 31;
  stale.userStopped = 0;
  stale.ownerId = 41;
  stale.updateTime = 51;
  {
    TaosArrayDupFailureGuard guard;
    EXPECT_EQ(WriteStream(&stale, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  }

  SStreamObj *persisted = AcquireStored();
  ASSERT_NE(persisted, nullptr);
  EXPECT_EQ(persisted->mainSnodeId, 31);
  EXPECT_EQ(persisted->userStopped, 0);
  EXPECT_EQ(persisted->ownerId, 41);
  EXPECT_EQ(persisted->updateTime, 51);
  EXPECT_EQ(persisted->recalcRevision, 12);
  EXPECT_EQ(OnlyPersistedRecalcId(*persisted), 0xc1);

  sdbRelease(pSdb_, persisted);
  tFreeStreamObj(&stale);
  tFreeStreamObj(&stored);
}

TEST_F(StreamObjSdbUpdateTest, NewerRecalcRevisionOwnsIndependentArray) {
  SStreamObj stored = BuildStreamObjWithRequest(9, 0x91);
  SStreamObj newer = BuildStreamObjWithRequest(10, 0xa1);
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  tFreeStreamObj(&stored);
  ASSERT_EQ(WriteStream(&newer, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  tFreeStreamObj(&newer);

  SStreamObj *persisted = AcquireStored();
  ASSERT_NE(persisted, nullptr);
  EXPECT_EQ(persisted->recalcRevision, 10);
  EXPECT_EQ(OnlyPersistedRecalcId(*persisted), 0xa1);
  sdbRelease(pSdb_, persisted);
}

TEST_F(StreamObjSdbUpdateTest, RecalcPatchCannotInsertMissingStream) {
  SStreamObj source = BuildStreamObjWithRequest(1, 0x11);
  SArray    *requests = BuildRequests({0x22});
  SSdbRaw   *raw = EncodeRecalcPatch(&source, 2, requests);
  ASSERT_NE(raw, nullptr);
  ASSERT_EQ(sdbSetRawStatus(raw, SDB_STATUS_READY), TSDB_CODE_SUCCESS);

  EXPECT_EQ(sdbWrite(pSdb_, raw), TSDB_CODE_SDB_OBJ_NOT_THERE);
  EXPECT_EQ(AcquireStored(), nullptr);

  taosArrayDestroy(requests);
  tFreeStreamObj(&source);
}

TEST_F(StreamObjSdbUpdateTest, RecalcPatchOnlyChangesRecalcTuple) {
  SStreamObj stored = BuildStreamObjWithRequest(10, 0xa1);
  stored.mainSnodeId = 17;
  stored.userStopped = 1;
  stored.ownerId = 23;
  stored.updateTime = 29;
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);

  SArray *requests = BuildRequests({0xb1});
  ASSERT_EQ(WriteRecalcPatch(stored, 11, requests), TSDB_CODE_SUCCESS);
  SStreamObj *persisted = AcquireStored();
  ASSERT_NE(persisted, nullptr);
  EXPECT_EQ(persisted->mainSnodeId, 17);
  EXPECT_EQ(persisted->userStopped, 1);
  EXPECT_EQ(persisted->ownerId, 23);
  EXPECT_EQ(persisted->updateTime, 29);
  EXPECT_EQ(persisted->recalcRevision, 11);
  EXPECT_EQ(OnlyPersistedRecalcId(*persisted), 0xb1);

  sdbRelease(pSdb_, persisted);
  taosArrayDestroy(requests);
  tFreeStreamObj(&stored);
}

TEST_F(StreamObjSdbUpdateTest, LegacyFullRawWithoutKindStillUpdatesFullObject) {
  SStreamObj stored = BuildStreamObjWithRequest(10, 0xa1);
  stored.mainSnodeId = 17;
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);

  SStreamObj updated = BuildStreamObjWithRequest(11, 0xb1);
  updated.mainSnodeId = 31;
  updated.userStopped = 1;
  updated.ownerId = 41;
  updated.updateTime = 51;
  SSdbRaw *raw = mndStreamActionEncode(&updated);
  ASSERT_NE(raw, nullptr);
  ASSERT_GT(raw->dataLen, (int32_t)sizeof(int8_t));
  ASSERT_EQ(sdbSetRawDataLen(raw, raw->dataLen - sizeof(int8_t)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(sdbSetRawStatus(raw, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  ASSERT_EQ(sdbWrite(pSdb_, raw), TSDB_CODE_SUCCESS);

  SStreamObj *persisted = AcquireStored();
  ASSERT_NE(persisted, nullptr);
  EXPECT_EQ(persisted->mainSnodeId, 31);
  EXPECT_EQ(persisted->userStopped, 1);
  EXPECT_EQ(persisted->ownerId, 41);
  EXPECT_EQ(persisted->updateTime, 51);
  EXPECT_EQ(persisted->recalcRevision, 11);
  EXPECT_EQ(OnlyPersistedRecalcId(*persisted), 0xb1);

  sdbRelease(pSdb_, persisted);
  tFreeStreamObj(&updated);
  tFreeStreamObj(&stored);
}

TEST_F(StreamObjSdbUpdateTest, FullRawWithZeroKindRoundTrips) {
  SStreamObj source = BuildStreamObjWithRequest(7, 0x71);
  source.mainSnodeId = 19;
  SSdbRaw *raw = mndStreamActionEncode(&source);
  ASSERT_NE(raw, nullptr);
  int8_t updateKind = -1;
  ASSERT_EQ(sdbGetRawInt8(raw, raw->dataLen - sizeof(updateKind), &updateKind), TSDB_CODE_SUCCESS);
  EXPECT_EQ(updateKind, MND_STREAM_RAW_UPDATE_FULL);
  ASSERT_EQ(sdbSetRawStatus(raw, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  ASSERT_EQ(sdbWrite(pSdb_, raw), TSDB_CODE_SUCCESS);

  SStreamObj *persisted = AcquireStored();
  ASSERT_NE(persisted, nullptr);
  EXPECT_EQ(persisted->mainSnodeId, 19);
  EXPECT_EQ(persisted->recalcRevision, 7);
  EXPECT_EQ(OnlyPersistedRecalcId(*persisted), 0x71);

  sdbRelease(pSdb_, persisted);
  tFreeStreamObj(&source);
}

TEST_F(StreamObjSdbUpdateTest, UnknownRawUpdateKindIsRejected) {
  SStreamObj source = BuildStreamObjWithRequest(7, 0x71);
  SSdbRaw   *raw = mndStreamActionEncode(&source);
  ASSERT_NE(raw, nullptr);
  ASSERT_EQ(sdbSetRawInt8(raw, raw->dataLen - sizeof(int8_t), 2), TSDB_CODE_SUCCESS);
  ASSERT_EQ(sdbSetRawStatus(raw, SDB_STATUS_READY), TSDB_CODE_SUCCESS);

  EXPECT_EQ(sdbWrite(pSdb_, raw), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(AcquireStored(), nullptr);

  tFreeStreamObj(&source);
}

TEST_F(StreamObjSdbUpdateTest, EqualRevisionPatchIsNoOp) {
  SStreamObj stored = BuildStreamObjWithRequest(10, 0xa1);
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  SArray *requests = BuildRequests({0xb1});
  ASSERT_NE(requests, nullptr);

  ASSERT_EQ(WriteRecalcPatch(stored, 10, requests), TSDB_CODE_SUCCESS);
  SStreamObj *persisted = AcquireStored();
  ASSERT_NE(persisted, nullptr);
  EXPECT_EQ(persisted->recalcRevision, 10);
  EXPECT_EQ(OnlyPersistedRecalcId(*persisted), 0xa1);

  sdbRelease(pSdb_, persisted);
  taosArrayDestroy(requests);
  tFreeStreamObj(&stored);
}

TEST_F(StreamObjSdbUpdateTest, OlderRevisionPatchIsNoOp) {
  SStreamObj stored = BuildStreamObjWithRequest(10, 0xa1);
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  SArray *requests = BuildRequests({0x91});
  ASSERT_NE(requests, nullptr);

  ASSERT_EQ(WriteRecalcPatch(stored, 9, requests), TSDB_CODE_SUCCESS);
  SStreamObj *persisted = AcquireStored();
  ASSERT_NE(persisted, nullptr);
  EXPECT_EQ(persisted->recalcRevision, 10);
  EXPECT_EQ(OnlyPersistedRecalcId(*persisted), 0xa1);

  sdbRelease(pSdb_, persisted);
  taosArrayDestroy(requests);
  tFreeStreamObj(&stored);
}

TEST_F(StreamObjSdbUpdateTest, PatchDeepCopiesRequestArray) {
  SStreamObj stored = BuildStreamObjWithRequest(10, 0xa1);
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  SArray *requests = BuildRequests({0xb1});
  ASSERT_NE(requests, nullptr);

  ASSERT_EQ(WriteRecalcPatch(stored, 11, requests), TSDB_CODE_SUCCESS);
  auto *request = static_cast<SStreamRecalcPersistReq *>(taosArrayGet(requests, 0));
  ASSERT_NE(request, nullptr);
  request->recalcId = 0xc1;

  SStreamObj *persisted = AcquireStored();
  ASSERT_NE(persisted, nullptr);
  EXPECT_EQ(persisted->recalcRevision, 11);
  EXPECT_EQ(OnlyPersistedRecalcId(*persisted), 0xb1);

  sdbRelease(pSdb_, persisted);
  taosArrayDestroy(requests);
  tFreeStreamObj(&stored);
}

TEST_F(StreamObjSdbUpdateTest, PatchAllocationFailureKeepsOldTuple) {
  SStreamObj stored = BuildStreamObjWithRequest(10, 0xa1);
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  SArray *requests = BuildRequests({0xb1});
  ASSERT_NE(requests, nullptr);

  {
    TaosArrayDupFailureGuard guard;
    EXPECT_EQ(WriteRecalcPatch(stored, 11, requests), TSDB_CODE_OUT_OF_MEMORY);
  }
  SStreamObj *persisted = AcquireStored();
  ASSERT_NE(persisted, nullptr);
  EXPECT_EQ(persisted->recalcRevision, 10);
  EXPECT_EQ(OnlyPersistedRecalcId(*persisted), 0xa1);

  sdbRelease(pSdb_, persisted);
  taosArrayDestroy(requests);
  tFreeStreamObj(&stored);
}

TEST_F(StreamObjSdbUpdateTest, FullAllocationFailureKeepsLifecycleAndRecalcTuple) {
  SStreamObj stored = BuildStreamObjWithRequest(10, 0xa1);
  stored.mainSnodeId = 17;
  stored.userStopped = 1;
  stored.ownerId = 23;
  stored.updateTime = 29;
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);

  SStreamObj updated = BuildStreamObjWithRequest(11, 0xb1);
  updated.mainSnodeId = 31;
  updated.userStopped = 0;
  updated.ownerId = 41;
  updated.updateTime = 51;
  {
    TaosArrayDupFailureGuard guard;
    EXPECT_EQ(WriteStream(&updated, SDB_STATUS_READY), TSDB_CODE_OUT_OF_MEMORY);
  }

  SStreamObj *persisted = AcquireStored();
  ASSERT_NE(persisted, nullptr);
  EXPECT_EQ(persisted->mainSnodeId, 17);
  EXPECT_EQ(persisted->userStopped, 1);
  EXPECT_EQ(persisted->ownerId, 23);
  EXPECT_EQ(persisted->updateTime, 29);
  EXPECT_EQ(persisted->recalcRevision, 10);
  EXPECT_EQ(OnlyPersistedRecalcId(*persisted), 0xa1);

  sdbRelease(pSdb_, persisted);
  tFreeStreamObj(&updated);
  tFreeStreamObj(&stored);
}

TEST_F(StreamObjSdbUpdateTest, NewerPatchWinsRecalcTupleWhileRacingFullCommitsLifecycle) {
  SStreamObj stored = BuildStreamObjWithRequest(10, 0xa1);
  stored.mainSnodeId = 17;
  stored.userStopped = 1;
  stored.ownerId = 23;
  stored.updateTime = 29;
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);

  SStreamObj full = BuildStreamObjWithRequest(11, 0xb1);
  full.mainSnodeId = 31;
  full.userStopped = 0;
  full.ownerId = 41;
  full.updateTime = 51;
  SArray *patchRequests = BuildRequests({0xc1});
  ASSERT_NE(patchRequests, nullptr);
  SStreamObj patch = {};
  patch.sdbRawUpdateKind = MND_STREAM_RAW_UPDATE_RECALC_PATCH;
  patch.recalcRevision = 12;
  patch.pIncompleteRecalcs = patchRequests;
  SStreamObj *persisted = AcquireStored();
  ASSERT_NE(persisted, nullptr);

  std::atomic<int32_t> fullCode{TSDB_CODE_INTERNAL_ERROR};
  FullArrayDupGate     gate;
  std::thread          fullWriter([&]() {
    gate.SetWriterThread();
    fullCode.store(ApplyRegisteredUpdate(persisted, &full));
  });
  if (!gate.WaitUntilEntered()) {
    gate.Release();
    fullWriter.join();
    sdbRelease(pSdb_, persisted);
    taosArrayDestroy(patchRequests);
    tFreeStreamObj(&full);
    tFreeStreamObj(&stored);
    FAIL() << "FULL update did not reach the array-copy barrier";
  }

  const int32_t patchCode = ApplyRegisteredUpdate(persisted, &patch);
  gate.Release();
  fullWriter.join();

  EXPECT_EQ(patchCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(fullCode.load(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(persisted->mainSnodeId, 31);
  EXPECT_EQ(persisted->userStopped, 0);
  EXPECT_EQ(persisted->ownerId, 41);
  EXPECT_EQ(persisted->updateTime, 51);
  EXPECT_EQ(persisted->recalcRevision, 12);
  EXPECT_EQ(OnlyPersistedRecalcId(*persisted), 0xc1);

  sdbRelease(pSdb_, persisted);
  taosArrayDestroy(patchRequests);
  tFreeStreamObj(&full);
  tFreeStreamObj(&stored);
}

TEST_F(StreamObjSdbUpdateTest, MalformedPatchIgnoresInheritedTerrno) {
  SStreamObj source = BuildStreamObjWithRequest(10, 0xa1);
  SArray    *requests = BuildRequests({0xb1});
  ASSERT_NE(requests, nullptr);
  SSdbRaw *raw = EncodeRecalcPatchWithZeroRecalcId(&source, 11, requests);
  ASSERT_NE(raw, nullptr);
  ASSERT_EQ(sdbSetRawStatus(raw, SDB_STATUS_READY), TSDB_CODE_SUCCESS);

  Stub inheritedTerrnoStub;
  inheritedTerrnoStub.set(taosArrayInit, initArrayWithInheritedTerrno);
  EXPECT_EQ(sdbWrite(pSdb_, raw), TSDB_CODE_INVALID_MSG);
  inheritedTerrnoStub.reset(taosArrayInit);

  taosArrayDestroy(requests);
  tFreeStreamObj(&source);
}

TEST_F(StreamObjSdbUpdateTest, RecalcPatchRejectsPayloadBeyondInt32BeforeReadingRequests) {
  SStreamObj source = BuildStreamObjWithRequest(1, 0x11);
  SArray     requests = {};
  requests.size = (size_t)INT32_MAX / (4 * sizeof(int64_t)) + 1;
  requests.elemSize = sizeof(SStreamRecalcPersistReq);
  STrans *trans = createTestTrans();
  ASSERT_NE(trans, nullptr);

  EXPECT_EQ(mndStreamTransAppendRecalcUpdate(&source, 2, &requests, trans, SDB_STATUS_READY), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(taosArrayGetSize(trans->commitActions), 0);

  mndTransDrop(trans);
  tFreeStreamObj(&source);
}

TEST_F(StreamObjSdbUpdateTest, RegisteredSdbWriterWaitsForStreamSnapshotReader) {
  SStreamObj stored = BuildStreamObjWithRequest(9, 0x91);
  SStreamObj newer = BuildStreamObjWithRequest(10, 0xa1);
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  tFreeStreamObj(&stored);

  SStreamObj *pPersisted = AcquireStored();
  ASSERT_NE(pPersisted, nullptr);
  taosRLockLatch(&pPersisted->lock);
  std::atomic<int32_t> writerCode{TSDB_CODE_INTERNAL_ERROR};
  std::atomic<bool>    writerStarted{false};
  std::atomic<bool>    writerFinished{false};
  std::thread          writer([&]() {
    writerStarted.store(true);
    writerCode.store(WriteStream(&newer, SDB_STATUS_READY));
    writerFinished.store(true);
  });

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!writerFinished.load() && !taosHasRWWFlag(&pPersisted->lock) && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  const bool writerWaited = writerStarted.load() && taosHasRWWFlag(&pPersisted->lock) && !writerFinished.load();
  taosRUnLockLatch(&pPersisted->lock);
  writer.join();

  EXPECT_TRUE(writerWaited);
  EXPECT_EQ(writerCode.load(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pPersisted->recalcRevision, 10);
  EXPECT_EQ(OnlyPersistedRecalcId(*pPersisted), 0xa1);
  sdbRelease(pSdb_, pPersisted);
  tFreeStreamObj(&newer);
}

TEST_F(StreamObjSdbUpdateTest, LiveTransactionAppendLocksBothEncodePassesAgainstRegisteredWriter) {
  SStreamObj stored = BuildStreamObjWithRequest(9, 0x91);
  SStreamObj newer = BuildStreamObjWithRequest(10, 0xa1);
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  tFreeStreamObj(&stored);

  SStreamObj *pPersisted = AcquireStored();
  ASSERT_NE(pPersisted, nullptr);
  SStreamObj encoded = {};
  CaptureAppendWhileRegisteredWriterWaits(
      pPersisted, &newer, [&](STrans *pTrans) { return mndStreamTransAppend(pPersisted, pTrans, SDB_STATUS_READY); },
      &encoded);
  ASSERT_NE(encoded.pCreate, nullptr);
  const bool oldTuple = encoded.recalcRevision == 9 && OnlyPersistedRecalcId(encoded) == 0x91;
  const bool newTuple = encoded.recalcRevision == 10 && OnlyPersistedRecalcId(encoded) == 0xa1;
  EXPECT_TRUE(oldTuple || newTuple);

  tFreeStreamObj(&encoded);
  sdbRelease(pSdb_, pPersisted);
  tFreeStreamObj(&newer);
}

TEST_F(StreamObjSdbUpdateTest, RecalcAppendEncodesPrivateTupleUnderLiveStreamLock) {
  SStreamObj stored = BuildStreamObjWithRequest(9, 0x91);
  SStreamObj newer = BuildStreamObjWithRequest(10, 0xa1);
  ASSERT_EQ(WriteStream(&stored, SDB_STATUS_READY), TSDB_CODE_SUCCESS);
  tFreeStreamObj(&stored);

  SStreamObj *pPersisted = AcquireStored();
  ASSERT_NE(pPersisted, nullptr);
  SArray *pRequests = taosArrayInit(1, sizeof(SStreamRecalcPersistReq));
  ASSERT_NE(pRequests, nullptr);
  const SStreamRecalcPersistReq request = {
      .recalcId = 0xb1,
      .start = 300,
      .end = 400,
      .requestTimeMs = 500,
  };
  ASSERT_NE(taosArrayPush(pRequests, &request), nullptr);

  SStreamObj encoded = {};
  CaptureAppendWhileRegisteredWriterWaits(
      pPersisted, &newer,
      [&](STrans *pTrans) {
        return mndStreamTransAppendRecalcUpdate(pPersisted, 11, pRequests, pTrans, SDB_STATUS_READY);
      },
      &encoded);
  EXPECT_EQ(encoded.sdbRawUpdateKind, MND_STREAM_RAW_UPDATE_RECALC_PATCH);
  EXPECT_STREQ(encoded.name, pPersisted->name);
  EXPECT_EQ(encoded.recalcRevision, 11);
  EXPECT_EQ(OnlyPersistedRecalcId(encoded), 0xb1);

  tFreeStreamObj(&encoded);
  taosArrayDestroy(pRequests);
  sdbRelease(pSdb_, pPersisted);
  tFreeStreamObj(&newer);
}

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
  EXPECT_STREQ(pRecalculates->schema[pRecalculates->colNum - 3].name, "status");
  EXPECT_EQ(pRecalculates->schema[pRecalculates->colNum - 3].type, TSDB_DATA_TYPE_VARCHAR);
  EXPECT_STREQ(pRecalculates->schema[pRecalculates->colNum - 2].name, "request_time");
  EXPECT_EQ(pRecalculates->schema[pRecalculates->colNum - 2].type, TSDB_DATA_TYPE_TIMESTAMP);
  EXPECT_STREQ(pRecalculates->schema[pRecalculates->colNum - 1].name, "message");
  EXPECT_EQ(pRecalculates->schema[pRecalculates->colNum - 1].type, TSDB_DATA_TYPE_VARCHAR);
}

TEST_F(StreamRecalculateViewTest, RequestTimeAndMessageColumnsAreAppended) {
  const SSysTableMeta *pMeta = getSysTableMeta("information_schema", TSDB_INS_TABLE_STREAM_RECALCULATES);
  ASSERT_NE(pMeta, nullptr);

  struct ExpectedColumn {
    const char *name;
    int8_t      type;
    int32_t     bytes;
  };
  const ExpectedColumn expected[] = {
      {"stream_name", TSDB_DATA_TYPE_VARCHAR, SYSTABLE_SCH_TABLE_NAME_LEN},
      {"stream_id", TSDB_DATA_TYPE_VARCHAR, 19 + VARSTR_HEADER_SIZE},
      {"recalc_id", TSDB_DATA_TYPE_VARCHAR, 19 + VARSTR_HEADER_SIZE},
      {"start", TSDB_DATA_TYPE_TIMESTAMP, 8},
      {"end", TSDB_DATA_TYPE_TIMESTAMP, 8},
      {"progress", TSDB_DATA_TYPE_VARCHAR, 20 + VARSTR_HEADER_SIZE},
      {"status", TSDB_DATA_TYPE_VARCHAR, 16 + VARSTR_HEADER_SIZE},
      {"request_time", TSDB_DATA_TYPE_TIMESTAMP, 8},
      {"message", TSDB_DATA_TYPE_VARCHAR, 256 + VARSTR_HEADER_SIZE},
  };
  ASSERT_EQ(pMeta->colNum, static_cast<int32_t>(sizeof(expected) / sizeof(expected[0])));
  for (int32_t i = 0; i < pMeta->colNum; ++i) {
    EXPECT_STREQ(pMeta->schema[i].name, expected[i].name);
    EXPECT_EQ(pMeta->schema[i].type, expected[i].type);
    EXPECT_EQ(pMeta->schema[i].bytes, expected[i].bytes);
  }
}

TEST_F(StreamRecalculateViewTest, PendingHasRequestTimeAndNullMessage) {
  constexpr int64_t kRequestTimeMs = 1710000000123;
  SStmRecalcRecord *pCurrent = AddViewRecord(1, STREAM_RECALC_STATUS_PENDING, 0, kRequestTimeMs);
  ASSERT_NE(pCurrent, nullptr);
  pCurrent->retryOrdinal = 2;
  pCurrent->errorCode = TSDB_CODE_RPC_TIMEOUT;
  ASSERT_NE(AddViewRecord(2, STREAM_RECALC_STATUS_PENDING, 0, 0), nullptr);

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(taosArrayGetSize(pBlock->pDataBlock), 9);
  const int32_t currentRow = FindRecalcRow(pBlock, 1);
  const int32_t legacyRow = FindRecalcRow(pBlock, 2);
  ASSERT_GE(currentRow, 0);
  ASSERT_GE(legacyRow, 0);
  EXPECT_EQ(getInt64Value(pBlock, 7, currentRow), kRequestTimeMs);
  EXPECT_TRUE(isNullValue(pBlock, 8, currentRow, pBlock->info.rows));
  EXPECT_TRUE(isNullValue(pBlock, 7, legacyRow, pBlock->info.rows));
  EXPECT_TRUE(isNullValue(pBlock, 8, legacyRow, pBlock->info.rows));
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateViewTest, NormalRunningHasNullMessage) {
  SStmRecalcRecord *pRecord = AddViewRecord(3, STREAM_RECALC_STATUS_RUNNING, 42);
  ASSERT_NE(pRecord, nullptr);
  pRecord->errorCode = TSDB_CODE_RPC_TIMEOUT;
  tstrncpy(pRecord->errorText, "untrusted stored text", sizeof(pRecord->errorText));
  pRecord = AddViewRecord(31, STREAM_RECALC_STATUS_RUNNING, 42);
  ASSERT_NE(pRecord, nullptr);
  pRecord->retryOrdinal = 2;
  pRecord = AddViewRecord(32, STREAM_RECALC_STATUS_RUNNING, 42);
  ASSERT_NE(pRecord, nullptr);
  pRecord->retryOrdinal = 4;
  pRecord->errorCode = TSDB_CODE_RPC_TIMEOUT;

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(taosArrayGetSize(pBlock->pDataBlock), 9);
  const int64_t normalIds[] = {3, 31, 32};
  for (int64_t recalcId : normalIds) {
    const int32_t row = FindRecalcRow(pBlock, recalcId);
    ASSERT_GE(row, 0);
    EXPECT_TRUE(isNullValue(pBlock, 8, row, pBlock->info.rows));
  }
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateViewTest, RetryingRunningFormatsOrdinalCodeAndError) {
  SStmRecalcRecord *pRecord = AddViewRecord(4, STREAM_RECALC_STATUS_RUNNING, 42);
  ASSERT_NE(pRecord, nullptr);
  pRecord->retryOrdinal = 2;
  pRecord->errorCode = TSDB_CODE_RPC_TIMEOUT;
  tstrncpy(pRecord->errorText, "untrusted stored text", sizeof(pRecord->errorText));

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(taosArrayGetSize(pBlock->pDataBlock), 9);
  const int32_t row = FindRecalcRow(pBlock, 4);
  ASSERT_GE(row, 0);
  EXPECT_EQ(getVarCharValue(pBlock, 8, row), "retrying 2/3: [0x" + Hex8(static_cast<uint32_t>(TSDB_CODE_RPC_TIMEOUT)) +
                                                 "] " + tstrerror(TSDB_CODE_RPC_TIMEOUT));
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateViewTest, FinishedForcesHundredPercentAndNullMessage) {
  SStmRecalcRecord *pRecord = AddViewRecord(5, STREAM_RECALC_STATUS_FINISHED, 63);
  ASSERT_NE(pRecord, nullptr);
  pRecord->retryOrdinal = 3;
  pRecord->errorCode = TSDB_CODE_RPC_TIMEOUT;
  pRecord->terminalObservedAtMs = gRecalcNowMs;

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(taosArrayGetSize(pBlock->pDataBlock), 9);
  const int32_t row = FindRecalcRow(pBlock, 5);
  ASSERT_GE(row, 0);
  EXPECT_EQ(getVarCharValue(pBlock, 5, row), "100%");
  EXPECT_TRUE(isNullValue(pBlock, 8, row, pBlock->info.rows));
  ASSERT_NE(ViewRecord(5), nullptr);
  EXPECT_EQ(ViewRecord(5)->snapshot.progressPct, 63);
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateViewTest, FailedFormatsFinalError) {
  SStmRecalcRecord *pRecord = AddViewRecord(6, STREAM_RECALC_STATUS_FAILED, 63);
  ASSERT_NE(pRecord, nullptr);
  pRecord->retryOrdinal = 3;
  pRecord->errorCode = TSDB_CODE_RPC_TIMEOUT;
  tstrncpy(pRecord->errorText, "untrusted stored text", sizeof(pRecord->errorText));
  pRecord->terminalObservedAtMs = gRecalcNowMs;

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(taosArrayGetSize(pBlock->pDataBlock), 9);
  const int32_t row = FindRecalcRow(pBlock, 6);
  ASSERT_GE(row, 0);
  EXPECT_EQ(getVarCharValue(pBlock, 5, row), "63%");
  EXPECT_EQ(getVarCharValue(pBlock, 8, row),
            "[0x" + Hex8(static_cast<uint32_t>(TSDB_CODE_RPC_TIMEOUT)) + "] " + tstrerror(TSDB_CODE_RPC_TIMEOUT));
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateViewTest, MessageIsSafelyTruncatedToVarcharCapacity) {
  SStmRecalcRecord *pRecord = AddViewRecord(7, STREAM_RECALC_STATUS_FAILED, 63);
  ASSERT_NE(pRecord, nullptr);
  pRecord->errorCode = TSDB_CODE_RPC_TIMEOUT;
  pRecord->terminalObservedAtMs = gRecalcNowMs;

  Stub errorStub;
  errorStub.set(tstrerror, returnLongRecalcError);
  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(taosArrayGetSize(pBlock->pDataBlock), 9);
  const int32_t row = FindRecalcRow(pBlock, 7);
  ASSERT_GE(row, 0);
  std::string expected = "[0x" + Hex8(static_cast<uint32_t>(TSDB_CODE_RPC_TIMEOUT)) + "] " + std::string(400, 'x');
  expected.resize(256);
  EXPECT_EQ(getVarCharValue(pBlock, 8, row), expected);
  blockDataDestroy(pBlock);
  errorStub.reset(tstrerror);
}

TEST_F(StreamRecalculateViewTest, TerminalRetentionStartsAtDeleteCommit) {
  SStmRecalcRecord *pRecord = AddViewRecord(8, STREAM_RECALC_STATUS_RUNNING, 61);
  ASSERT_NE(pRecord, nullptr);
  pRecord->terminalCandidate = pRecord->snapshot;
  pRecord->terminalCandidate.status = STREAM_RECALC_STATUS_FAILED;
  pRecord->terminalCandidateValid = true;
  pRecord->terminalPersisting = true;

  gRecalcNowMs = 7200000;
  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_GE(FindRecalcRow(pBlock, 8), 0);
  blockDataDestroy(pBlock);

  pRecord = ViewRecord(8);
  ASSERT_NE(pRecord, nullptr);
  pRecord->snapshot = pRecord->terminalCandidate;
  pRecord->terminalObservedAtMs = gRecalcNowMs;
  pRecord->terminalPersisting = false;

  gRecalcNowMs = 10799999;
  pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_GE(FindRecalcRow(pBlock, 8), 0);
  blockDataDestroy(pBlock);

  gRecalcNowMs = 10800000;
  pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(FindRecalcRow(pBlock, 8), -1);
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 0);
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateViewTest, TerminalRetentionKeepsAtMostOneHundredPerStream) {
  for (int32_t i = 0; i < 100; ++i) {
    SStmRecalcRecord *pRecord = AddViewRecord(1000 + i, STREAM_RECALC_STATUS_FINISHED, 100);
    ASSERT_NE(pRecord, nullptr);
    pRecord->terminalObservedAtMs = 1000 + i;
    pRecord->terminalCandidateValid = true;
  }
  gRecalcNowMs = 200000;

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 100);
  EXPECT_GE(FindRecalcRow(pBlock, 1000), 0);
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 100);
  blockDataDestroy(pBlock);

  SStmRecalcRecord *pNewest = AddViewRecord(1100, STREAM_RECALC_STATUS_FINISHED, 100);
  ASSERT_NE(pNewest, nullptr);
  pNewest->terminalObservedAtMs = 1100;
  pNewest->terminalCandidateValid = true;
  pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 100);
  EXPECT_EQ(FindRecalcRow(pBlock, 1000), -1);
  EXPECT_GE(FindRecalcRow(pBlock, 1001), 0);
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 100);
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateViewTest, ActiveRecordsAreNeverPrunedByTerminalLimit) {
  for (int32_t i = 0; i < 101; ++i) {
    SStmRecalcRecord *pRecord = AddViewRecord(2000 + i, STREAM_RECALC_STATUS_FAILED, 50);
    ASSERT_NE(pRecord, nullptr);
    pRecord->terminalObservedAtMs = 1000 + i;
    pRecord->terminalCandidateValid = true;
  }
  SStmRecalcRecord *pPending = AddViewRecord(3001, STREAM_RECALC_STATUS_PENDING, 0);
  SStmRecalcRecord *pRunning = AddViewRecord(3002, STREAM_RECALC_STATUS_RUNNING, 40);
  SStmRecalcRecord *pPersisting = AddViewRecord(3003, STREAM_RECALC_STATUS_FAILED, 60);
  SStmRecalcRecord *pHidden = AddViewRecord(3004, STREAM_RECALC_STATUS_FAILED, 70);
  ASSERT_NE(pPending, nullptr);
  ASSERT_NE(pRunning, nullptr);
  ASSERT_NE(pPersisting, nullptr);
  ASSERT_NE(pHidden, nullptr);
  pPending = ViewRecord(3001);
  pRunning = ViewRecord(3002);
  pPersisting = ViewRecord(3003);
  pHidden = ViewRecord(3004);
  ASSERT_NE(pPending, nullptr);
  ASSERT_NE(pRunning, nullptr);
  ASSERT_NE(pPersisting, nullptr);
  ASSERT_NE(pHidden, nullptr);
  pPending->terminalObservedAtMs = 1;
  pRunning->terminalObservedAtMs = 1;
  pPersisting->terminalObservedAtMs = 1;
  pPersisting->terminalPersisting = true;
  pHidden->terminalObservedAtMs = 1;
  pHidden->hidden = true;
  gRecalcNowMs = 200000;

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 103);
  EXPECT_GE(FindRecalcRow(pBlock, 3001), 0);
  EXPECT_GE(FindRecalcRow(pBlock, 3002), 0);
  EXPECT_GE(FindRecalcRow(pBlock, 3003), 0);
  EXPECT_EQ(FindRecalcRow(pBlock, 3004), -1);
  EXPECT_NE(ViewRecord(3004), nullptr);
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 104);
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateViewTest, AgePrunedTerminalIsNotRevivedByTypedLegacyMirror) {
  constexpr int64_t kRecalcId = 4000;
  SStmRecalcRecord *pRecord = AddViewRecord(kRecalcId, STREAM_RECALC_STATUS_FINISHED, 100);
  ASSERT_NE(pRecord, nullptr);
  pRecord->terminalObservedAtMs = 1000;
  ASSERT_EQ(ReportTypedRecalculations(kRecalcId, 1), TSDB_CODE_SUCCESS);

  gRecalcNowMs = 3601000;
  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 0);
  EXPECT_EQ(FindRecalcRow(pBlock, kRecalcId), -1);
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 0);
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateViewTest, LimitPrunedTerminalIsNotRevivedByTypedLegacyMirror) {
  constexpr int64_t kFirstRecalcId = 5000;
  constexpr int32_t kRecalcCount = 101;
  for (int32_t i = 0; i < kRecalcCount; ++i) {
    SStmRecalcRecord *pRecord = AddViewRecord(kFirstRecalcId + i, STREAM_RECALC_STATUS_FINISHED, 100);
    ASSERT_NE(pRecord, nullptr);
    pRecord->terminalObservedAtMs = 1000;
  }
  ASSERT_EQ(ReportTypedRecalculations(kFirstRecalcId, kRecalcCount), TSDB_CODE_SUCCESS);

  gRecalcNowMs = 200000;
  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 100);
  EXPECT_EQ(FindRecalcRow(pBlock, kFirstRecalcId), -1);
  EXPECT_GE(FindRecalcRow(pBlock, kFirstRecalcId + 1), 0);
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 100);
  blockDataDestroy(pBlock);
}

TEST_F(StreamRecalculateViewTest, UnknownTypedRecalculationDoesNotCreateViewRow) {
  constexpr int64_t kUnknownRecalcId = 6000;
  ASSERT_EQ(ReportTypedRecalculations(kUnknownRecalcId, 1), TSDB_CODE_SUCCESS);

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 0);
  EXPECT_EQ(FindRecalcRow(pBlock, kUnknownRecalcId), -1);
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 0);
  blockDataDestroy(pBlock);
}

TEST_F(MndStreamRecalcTest, RecalculateViewUsesMillisecondTimestamps) {
  create_.triggerPrec = TSDB_TIME_PRECISION_MICRO;
  ASSERT_EQ(Apply(7, STREAM_RECALC_STATUS_RUNNING, 42, 1735689600123456, 1735689600654321), TSDB_CODE_SUCCESS);
  ASSERT_EQ(Apply(8, STREAM_RECALC_STATUS_PENDING, 0, INT64_MIN, INT64_MAX), TSDB_CODE_SUCCESS);

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  const int32_t microRow = FindRecalcRow(pBlock, 7);
  ASSERT_GE(microRow, 0);
  EXPECT_EQ(getInt64Value(pBlock, 3, microRow), 1735689600123);
  EXPECT_EQ(getInt64Value(pBlock, 4, microRow), 1735689600654);
  const int32_t unboundedRow = FindRecalcRow(pBlock, 8);
  ASSERT_GE(unboundedRow, 0);
  EXPECT_EQ(getInt64Value(pBlock, 3, unboundedRow), INT64_MIN);
  EXPECT_EQ(getInt64Value(pBlock, 4, unboundedRow), INT64_MAX);
  blockDataDestroy(pBlock);
}

TEST_F(MndStreamRecalcTest, RecalculateViewConvertsNanosecondsToMilliseconds) {
  create_.triggerPrec = TSDB_TIME_PRECISION_NANO;
  ASSERT_EQ(Apply(7, STREAM_RECALC_STATUS_RUNNING, 42, 1735689600123456789, 1735689600654321789), TSDB_CODE_SUCCESS);

  SSDataBlock *pBlock = QueryRecalculates();
  ASSERT_NE(pBlock, nullptr);
  const int32_t row = FindRecalcRow(pBlock, 7);
  ASSERT_GE(row, 0);
  EXPECT_EQ(getInt64Value(pBlock, 3, row), 1735689600123);
  EXPECT_EQ(getInt64Value(pBlock, 4, row), 1735689600654);
  blockDataDestroy(pBlock);
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
    TaosArrayDupFailureGuard guard;
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
  ASSERT_EQ(taosArrayGetSize(stored_->recalcRecords), 0);

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
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 100);
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

TEST_F(MndStreamRecalcTest, LargeTypedSnapshotDoesNotCreateRuntimeRecords) {
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
  EXPECT_EQ(gRecalcRecordArrayGetCalls, 0);
  EXPECT_EQ(taosArrayGetSize(stored_->recalcRecords), 0);
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
  ASSERT_EQ(mstCopyTriggerRuntimeStatus(&trigger_, &legacy), TSDB_CODE_SUCCESS);
  taosArrayDestroy(legacy.userRecalcs);

  SnodeLookupStubGuard snodeLookup;
  SMnode               mnode = {};
  SSDataBlock         *pStreamBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(pStreamBlock, nullptr);
  ASSERT_EQ(mstSetStreamAttrResBlock(&mnode, &stream_, pStreamBlock, 0), TSDB_CODE_SUCCESS);
  EXPECT_EQ(getInt32Value(pStreamBlock, taosArrayGetSize(pStreamBlock->pDataBlock) - 1, 0), 37);
  blockDataDestroy(pStreamBlock);

  SSDataBlock *pRecalcBlock = QueryRecalculates();
  ASSERT_NE(pRecalcBlock, nullptr);
  const int32_t row = FindRecalcRow(pRecalcBlock, 12);
  ASSERT_GE(row, 0);
  EXPECT_EQ(getVarCharValue(pRecalcBlock, 5, row), "42%");
  EXPECT_TRUE(isNullValue(pRecalcBlock, 6, row, pRecalcBlock->info.rows));
  EXPECT_EQ(pRecalcBlock->info.rows, 1);
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

TEST_F(MndStreamTest, CopyPreservesOnlyExistingRecalcCapability) {
  SStmTaskStatus status = {};
  status.metrics.applicableMask = STREAM_METRIC_RECALCULATES | STREAM_METRIC_HISTORY_PROGRESS;
  SStreamTaskMetricsSnapshot replacement = {};
  replacement.applicableMask = STREAM_METRIC_LOGICAL_INPUT;
  replacement.validMask = STREAM_METRIC_LOGICAL_INPUT;

  ASSERT_EQ(mstCopyTaskMetrics(&status, &replacement), TSDB_CODE_SUCCESS);

  EXPECT_EQ(status.metrics.applicableMask,
            static_cast<uint64_t>(STREAM_METRIC_RECALCULATES | STREAM_METRIC_LOGICAL_INPUT));
  EXPECT_EQ(status.metrics.validMask, static_cast<uint64_t>(STREAM_METRIC_LOGICAL_INPUT));
  EXPECT_TRUE(status.metricsValid);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, CopyWithoutRecalcCapabilityDoesNotInventIt) {
  SStmTaskStatus status = {};
  status.metrics.applicableMask = STREAM_METRIC_HISTORY_PROGRESS;
  SStreamTaskMetricsSnapshot replacement = {};
  replacement.applicableMask = STREAM_METRIC_LOGICAL_INPUT;
  replacement.validMask = STREAM_METRIC_LOGICAL_INPUT;

  ASSERT_EQ(mstCopyTaskMetrics(&status, &replacement), TSDB_CODE_SUCCESS);

  EXPECT_EQ(status.metrics.applicableMask, static_cast<uint64_t>(STREAM_METRIC_LOGICAL_INPUT));
  EXPECT_EQ(status.metrics.applicableMask & STREAM_METRIC_RECALCULATES, 0U);
  EXPECT_TRUE(status.metricsValid);
  mstClearTaskMetrics(&status);
}

TEST_F(MndStreamTest, ReplacingSnapshotDeepCopiesRecalcDetailStrings) {
  SStreamHbMsg input = {};
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pVgLeaders = taosArrayInit(0, sizeof(int32_t));
  input.pStreamStatus = taosArrayInit(0, sizeof(SStmTaskStatusMsg));
  input.pStreamReq = taosArrayInit(0, sizeof(int32_t));
  input.pTriggerStatus = taosArrayInit(0, sizeof(SSTriggerRuntimeStatus));
  input.pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  ASSERT_NE(input.pVgLeaders, nullptr);
  ASSERT_NE(input.pStreamStatus, nullptr);
  ASSERT_NE(input.pStreamReq, nullptr);
  ASSERT_NE(input.pTriggerStatus, nullptr);
  ASSERT_NE(input.pTaskMetrics, nullptr);
  SStreamTaskMetricsEntry entry = makeMetricEntry(0, 10, 20, 30);
  entry.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(entry.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot recalc = {};
  recalc.recalcId = 100;
  ASSERT_NE(taosArrayPush(entry.snapshot.pRecalculates, &recalc), nullptr);
  entry.snapshot.pRecalcDetails = taosArrayInit(1, sizeof(SStreamRecalcDetail));
  ASSERT_NE(entry.snapshot.pRecalcDetails, nullptr);
  SStreamRecalcDetail detail = {};
  detail.recalcId = 100;
  detail.retryOrdinal = 2;
  detail.errorCode = TSDB_CODE_OUT_OF_MEMORY;
  detail.errorText = taosStrdup(tstrerror(detail.errorCode));
  ASSERT_NE(detail.errorText, nullptr);
  ASSERT_NE(taosArrayPush(entry.snapshot.pRecalcDetails, &detail), nullptr);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &entry), nullptr);

  SEncoder sizeEncoder = {};
  tEncoderInit(&sizeEncoder, nullptr, 0);
  int32_t encodedLength = tEncodeStreamHbMsg(&sizeEncoder, &input);
  ASSERT_GT(encodedLength, 0);
  tEncoderClear(&sizeEncoder);
  std::vector<char> bytes(encodedLength, 0);
  SEncoder          encoder = {};
  tEncoderInit(&encoder, reinterpret_cast<uint8_t *>(bytes.data()), encodedLength);
  ASSERT_EQ(tEncodeStreamHbMsg(&encoder, &input), encodedLength);
  tEncoderClear(&encoder);

  SStreamHbMsg decoded = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t *>(bytes.data()), encodedLength);
  ASSERT_EQ(tDecodeStreamHbMsg(&decoder, &decoded), TSDB_CODE_SUCCESS);
  tDecoderClear(&decoder);
  const auto *decodedEntry = static_cast<const SStreamTaskMetricsEntry *>(taosArrayGet(decoded.pTaskMetrics, 0));
  ASSERT_NE(decodedEntry, nullptr);

  SStmTaskStatus status = {};
  ASSERT_EQ(mstCopyTaskMetrics(&status, &decodedEntry->snapshot), TSDB_CODE_SUCCESS);
  tCleanupStreamHbMsg(&decoded, true);

  ASSERT_TRUE(status.metricsValid);
  ASSERT_NE(status.metrics.pRecalcDetails, nullptr);
  ASSERT_EQ(taosArrayGetSize(status.metrics.pRecalcDetails), 1);
  const auto *stored = static_cast<const SStreamRecalcDetail *>(taosArrayGet(status.metrics.pRecalcDetails, 0));
  ASSERT_NE(stored, nullptr);
  EXPECT_EQ(stored->recalcId, 100);
  EXPECT_EQ(stored->retryOrdinal, 2);
  EXPECT_EQ(stored->errorCode, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_STREQ(stored->errorText, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
  mstClearTaskMetrics(&status);
  tCleanupStreamHbMsg(&input, true);
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

TEST_F(StreamTest, StreamDeployWindowPlanDeepOwnsPersistedPlan) {
  SCMCreateStreamReq create = {};
  create.streamId = 42;
  create.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  create.pWindowPlan = makeSessionPlan();
  ASSERT_NE(nullptr, create.pWindowPlan);

  SStmStatus status = {};
  status.pCreate = &create;
  SStreamObj stream = {};
  stream.pCreate = &create;
  SStmTaskDeploy deploy = {};

  ASSERT_EQ(TSDB_CODE_SUCCESS, msmBuildTriggerDeployInfo(nullptr, &status, &deploy, &stream));
  ASSERT_NE(nullptr, deploy.msg.trigger.pWindowPlan);
  EXPECT_NE(create.pWindowPlan, deploy.msg.trigger.pWindowPlan);
  EXPECT_NE(create.pWindowPlan->pLayers, deploy.msg.trigger.pWindowPlan->pLayers);

  auto* pSourceOuter = static_cast<SStreamWindowLayerSpec*>(taosArrayGet(create.pWindowPlan->pLayers, 0));
  auto* pDeployOuter = static_cast<SStreamWindowLayerSpec*>(taosArrayGet(deploy.msg.trigger.pWindowPlan->pLayers, 0));
  pSourceOuter->trigger.session.sessionVal = 99;
  EXPECT_EQ(20, pDeployOuter->trigger.session.sessionVal);

  tDestroyStreamWindowPlan(&create.pWindowPlan);
  EXPECT_EQ(20, pDeployOuter->trigger.session.sessionVal);
  tDestroyStreamWindowPlan(&deploy.msg.trigger.pWindowPlan);
}

TEST_F(StreamTest, StreamDeployWindowPlanPropagatesRunnerCapability) {
  SCMCreateStreamReq create = {};
  create.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN | STREAM_OPTION_FLUSH_ON_OUTER_CLOSE;

  SStmStatus status = {};
  status.pCreate = &create;
  atomic_store_32(&status.runnerReplica, 1);
  SStreamObj stream = {};
  stream.pCreate = &create;
  SStmTaskDeploy deploy = {};

  ASSERT_EQ(TSDB_CODE_SUCCESS, msmBuildRunnerDeployInfo(&deploy, nullptr, &stream, &status, true));
  EXPECT_TRUE(BIT_FLAG_TEST_MASK(deploy.msg.runner.addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN));
  EXPECT_TRUE(BIT_FLAG_TEST_MASK(deploy.msg.runner.addOptions, STREAM_OPTION_FLUSH_ON_OUTER_CLOSE));
}

TEST_F(StreamTest, StreamDeployNonVirtualNestedTrowsPropagatesCacheScanPlans) {
  SCMCreateStreamReq create = {};
  create.streamId = 42;
  create.triggerTblType = TSDB_SUPER_TABLE;
  create.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  create.placeHolderBitmap = PLACE_HOLDER_PARTITION_ROWS;
  create.pWindowPlan = makeSessionPlan();
  create.triggerScanPlan = const_cast<char*>("trigger-plan");
  create.calcScanPlanList = taosArrayInit(1, sizeof(SStreamCalcScan));
  ASSERT_NE(nullptr, create.pWindowPlan);
  ASSERT_NE(nullptr, create.calcScanPlanList);
  SStreamCalcScan calcScan = {};
  calcScan.readFromCache = true;
  calcScan.scanPlan = const_cast<char *>("calc-plan");
  ASSERT_NE(nullptr, taosArrayPush(create.calcScanPlanList, &calcScan));

  SStmStatus status = {};
  status.pCreate = &create;
  SStreamObj stream = {};
  stream.pCreate = &create;
  SStmTaskDeploy deploy = {};

  ASSERT_EQ(TSDB_CODE_SUCCESS, msmBuildTriggerDeployInfo(nullptr, &status, &deploy, &stream));
  EXPECT_STREQ("trigger-plan", static_cast<const char*>(deploy.msg.trigger.triggerScanPlan));
  EXPECT_STREQ("calc-plan", static_cast<const char*>(deploy.msg.trigger.calcCacheScanPlan));

  tDestroyStreamWindowPlan(&deploy.msg.trigger.pWindowPlan);
  tDestroyStreamWindowPlan(&create.pWindowPlan);
  taosArrayDestroy(create.calcScanPlanList);
}

TEST_F(StreamTest, LocalTriggerWithExternalCalcSourceKeepsNestedCacheScanPlans) {
  SCMCreateStreamReq create = {};
  create.streamId = 42;
  create.triggerTblType = TSDB_SUPER_TABLE;
  create.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  create.placeHolderBitmap = PLACE_HOLDER_PARTITION_ROWS;
  create.pWindowPlan = makeSessionPlan();
  create.triggerScanPlan = const_cast<char *>("trigger-plan");
  create.calcScanPlanList = taosArrayInit(1, sizeof(SStreamCalcScan));
  ASSERT_NE(nullptr, create.pWindowPlan);
  ASSERT_NE(nullptr, create.calcScanPlanList);
  SStreamCalcScan calcScan = {};
  calcScan.readFromCache = true;
  calcScan.scanPlan = const_cast<char *>("calc-plan");
  ASSERT_NE(nullptr, taosArrayPush(create.calcScanPlanList, &calcScan));

  SStmStatus status = {};
  status.pCreate = &create;
  SStreamObj stream = {};
  stream.flags = STREAM_FLAG_REF_EXT_SOURCE;
  stream.pCreate = &create;
  SStmTaskDeploy deploy = {};
  deploy.task.flags = 0;

  ASSERT_EQ(TSDB_CODE_SUCCESS, msmBuildTriggerDeployInfo(nullptr, &status, &deploy, &stream));
  EXPECT_STREQ("trigger-plan", static_cast<const char *>(deploy.msg.trigger.triggerScanPlan));
  EXPECT_STREQ("calc-plan", static_cast<const char *>(deploy.msg.trigger.calcCacheScanPlan));

  tDestroyStreamWindowPlan(&deploy.msg.trigger.pWindowPlan);
  tDestroyStreamWindowPlan(&create.pWindowPlan);
  taosArrayDestroy(create.calcScanPlanList);
}

TEST_F(StreamTest, StreamDeployWindowPlanCloneFailureKeepsPersistedPlan) {
  SCMCreateStreamReq create = {};
  create.streamId = 42;
  create.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  create.pWindowPlan = makeSessionPlan();
  ASSERT_NE(nullptr, create.pWindowPlan);
  SStreamWindowPlan* pPersistedPlan = create.pWindowPlan;

  SStmStatus status = {};
  status.pCreate = &create;
  SStreamObj stream = {};
  stream.pCreate = &create;
  SStmTaskDeploy deploy = {};
  Stub           stub;
  stub.set(tCloneStreamWindowPlan, failWindowPlanClone);

  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, msmBuildTriggerDeployInfo(nullptr, &status, &deploy, &stream));
  EXPECT_EQ(pPersistedPlan, create.pWindowPlan);
  EXPECT_EQ(nullptr, deploy.msg.trigger.pWindowPlan);

  tDestroyStreamWindowPlan(&create.pWindowPlan);
}

TEST_F(StreamTest, StreamDeployWindowPlanUndeployedOwnerCleanup) {
  SStmTaskToDeployExt pending = {};
  pending.deploy.task.type = STREAM_TRIGGER_TASK;
  pending.deploy.msg.trigger.pWindowPlan = makeSessionPlan();
  ASSERT_NE(nullptr, pending.deploy.msg.trigger.pWindowPlan);

  mstDestroySStmTaskToDeployExt(&pending);
  EXPECT_EQ(nullptr, pending.deploy.msg.trigger.pWindowPlan);
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

*/
// clang-format on

namespace {

class MndStreamCreateMetadataTest;

MndStreamCreateMetadataTest* gCreateMetadataTest = nullptr;

int32_t testSdbSetTable(SSdb*, SSdbTable) { return TSDB_CODE_SUCCESS; }

int32_t testGrantCheck(EGrantType) { return TSDB_CODE_SUCCESS; }

int32_t testAssignSnode(SMnode*, int64_t) { return 1; }

void testBecomeNotLeader(SMnode*) {}

class MndStreamCreateMetadataTest : public testing::Test {
 public:
  static int32_t acquireStream(SMnode*, char*, SStreamObj** ppStream) {
    *ppStream = nullptr;
    return TSDB_CODE_MND_STREAM_NOT_EXIST;
  }

  static int32_t acquireUser(SMnode*, const char*, SUserObj** ppUser) {
    *ppUser = &gCreateMetadataTest->user_;
    return TSDB_CODE_SUCCESS;
  }

  static void releaseUser(SMnode*, SUserObj*) {}

  static int32_t checkDbPrivilege(SMnode*, const char*, const char*, EOperType, const char*, bool) {
    return TSDB_CODE_SUCCESS;
  }

  static int32_t getSdbSize(SSdb*, ESdbType) { return 0; }

  static SDbObj* acquireDb(SMnode*, const char*) {
    return gCreateMetadataTest->returnDb_ ? &gCreateMetadataTest->db_ : nullptr;
  }

  static void releaseDb(SMnode*, SDbObj*) {}

  static SStbObj* acquireStb(SMnode*, char*) {
    if (gCreateMetadataTest->blockStbLookup_) {
      std::unique_lock<std::mutex> lock(gCreateMetadataTest->stbLookupMutex_);
      gCreateMetadataTest->stbLookupEntered_ = true;
      gCreateMetadataTest->stbLookupCv_.notify_all();
      if (!gCreateMetadataTest->stbLookupCv_.wait_for(lock, std::chrono::seconds(5),
                                                      [] { return gCreateMetadataTest->releaseStbLookup_; })) {
        gCreateMetadataTest->stbLookupTimedOut_ = true;
      }
    }
    return gCreateMetadataTest->returnStb_ ? &gCreateMetadataTest->stb_ : nullptr;
  }

  static void releaseStb(SMnode*, SStbObj*) {}

  static SExtSourceObj* acquireExtSource(SMnode*, const char* sourceName) {
    gCreateMetadataTest->acquiredExtSources_.emplace_back(sourceName == nullptr ? "" : sourceName);
    return gCreateMetadataTest->returnExtSource_ ? &gCreateMetadataTest->extSource_ : nullptr;
  }

  static void releaseExtSource(SMnode*, SExtSourceObj*) {}

  static int32_t buildDbVgroups(SMnode*, SSHashObj** ppVgroups) {
    if (gCreateMetadataTest->blockVgroupLookup_) {
      std::unique_lock<std::mutex> lock(gCreateMetadataTest->vgroupLookupMutex_);
      gCreateMetadataTest->vgroupLookupEntered_ = true;
      gCreateMetadataTest->vgroupLookupCv_.notify_all();
      if (!gCreateMetadataTest->vgroupLookupCv_.wait_for(lock, std::chrono::seconds(5),
                                                         [] { return gCreateMetadataTest->releaseVgroupLookup_; })) {
        gCreateMetadataTest->vgroupLookupTimedOut_ = true;
      }
    }
    *ppVgroups = reinterpret_cast<SSHashObj*>(static_cast<uintptr_t>(0x2001));
    return TSDB_CODE_SUCCESS;
  }

  static void destroyDbVgroups(SSHashObj*) {}

  static int32_t getTableVgId(SSHashObj*, char*, char*, int32_t* pVgId) {
    *pVgId = gCreateMetadataTest->vgroup_.vgId;
    return TSDB_CODE_SUCCESS;
  }

  static SVgObj* acquireVgroup(SMnode*, int32_t vgId) {
    return vgId == gCreateMetadataTest->vgroup_.vgId ? &gCreateMetadataTest->vgroup_ : nullptr;
  }

  static void releaseVgroup(SMnode*, SVgObj*) {}

  static SEpSet getVgroupEpset(SMnode*, const SVgObj*) { return gCreateMetadataTest->epSet_; }

  static int32_t createTrans(SMnode*, SStreamObj* pStream, SRpcMsg* pReq, ETrnConflct, const char*, STrans** ppTrans) {
    ++gCreateMetadataTest->createTransCalls_;
    gCreateMetadataTest->transactionOrigin_ = pReq->msgType;
    if (pStream != nullptr && pStream->pCreate != nullptr) {
      gCreateMetadataTest->transactionTableType_ = pStream->pCreate->triggerTblType;
      gCreateMetadataTest->transactionTableUid_ = pStream->pCreate->triggerTblUid;
      gCreateMetadataTest->transactionTableSuid_ = pStream->pCreate->triggerTblSuid;
      gCreateMetadataTest->transactionVgId_ = pStream->pCreate->triggerTblVgId;
      gCreateMetadataTest->transactionPrecision_ = pStream->pCreate->triggerPrec;
      gCreateMetadataTest->transactionFlags_ = pStream->pCreate->flags;
      SNodeList* pPartition = nullptr;
      if (pStream->pCreate->partitionCols != nullptr &&
          nodesStringToList(static_cast<const char*>(pStream->pCreate->partitionCols), &pPartition) ==
              TSDB_CODE_SUCCESS &&
          LIST_LENGTH(pPartition) == 1 && nodeType(nodesListGetNode(pPartition, 0)) == QUERY_NODE_COLUMN) {
        auto* pColumn = reinterpret_cast<SColumnNode*>(nodesListGetNode(pPartition, 0));
        gCreateMetadataTest->transactionPartitionColType_ = pColumn->colType;
        gCreateMetadataTest->transactionPartitionDataType_ = pColumn->node.resType.type;
        gCreateMetadataTest->transactionPartitionBytes_ = pColumn->node.resType.bytes;
      }
      nodesDestroyList(pPartition);
    }
    *ppTrans = nullptr;
    return TSDB_CODE_ACTION_IN_PROGRESS;
  }

  static int32_t sendAsync(void*, SEpSet*, int64_t* pTransporterId, SMsgSendInfo* pSendInfo) {
    ++gCreateMetadataTest->asyncSendCalls_;
    if (gCreateMetadataTest->sendCode_ != TSDB_CODE_SUCCESS) {
      const int32_t code = gCreateMetadataTest->sendCode_;
      destroySendMsgInfo(pSendInfo);
      return code;
    }

    if (gCreateMetadataTest->callbackBeforeSendReturn_) {
      SDataBuf response = gCreateMetadataTest->makeCallbackResponse();
      pSendInfo->fp(pSendInfo->param, &response, gCreateMetadataTest->callbackCode_);
      destroySendMsgInfo(pSendInfo);
      *pTransporterId = gCreateMetadataTest->transporterId_;
      return TSDB_CODE_SUCCESS;
    }

    *pTransporterId = gCreateMetadataTest->transporterId_;
    gCreateMetadataTest->pendingSend_ = pSendInfo;
    return TSDB_CODE_SUCCESS;
  }

  static int32_t freeConnection(void*, int64_t transporterId) {
    ++gCreateMetadataTest->freeConnectionCalls_;
    gCreateMetadataTest->freedTransporterIds_.push_back(transporterId);
    return TSDB_CODE_SUCCESS;
  }

  static int32_t putToQueue(void*, EQueueType qtype, SRpcMsg* pMsg) {
    EXPECT_EQ(WRITE_QUEUE, qtype);
    ++gCreateMetadataTest->queueCalls_;
    if (gCreateMetadataTest->blockQueue_) {
      std::unique_lock<std::mutex> lock(gCreateMetadataTest->queueMutex_);
      gCreateMetadataTest->queueEntered_ = true;
      gCreateMetadataTest->queueCv_.notify_all();
      if (!gCreateMetadataTest->queueCv_.wait_for(lock, std::chrono::seconds(5),
                                                  [] { return gCreateMetadataTest->releaseQueue_; })) {
        gCreateMetadataTest->queueTimedOut_ = true;
      }
    }
    if (gCreateMetadataTest->queueCode_ != TSDB_CODE_SUCCESS) return gCreateMetadataTest->queueCode_;
    if (gCreateMetadataTest->processQueueInline_) {
      MndMsgFp continuation = gCreateMetadataTest->mnode_.msgFp[TMSG_INDEX(TDMT_MND_UNUSED1)];
      EXPECT_NE(nullptr, continuation);
      if (continuation == nullptr) return TSDB_CODE_INTERNAL_ERROR;
      pMsg->info.node = &gCreateMetadataTest->mnode_;
      EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, continuation(pMsg));
      rpcFreeCont(pMsg->pCont);
      pMsg->pCont = nullptr;
      return TSDB_CODE_SUCCESS;
    }
    EXPECT_EQ(nullptr, gCreateMetadataTest->queuedMsg_.pCont);
    gCreateMetadataTest->queuedMsg_ = *pMsg;
    pMsg->pCont = nullptr;
    return TSDB_CODE_SUCCESS;
  }

  static int32_t sendResponse(const SRpcMsg* pRsp) {
    ++gCreateMetadataTest->responseCalls_;
    gCreateMetadataTest->lastResponseCode_ = pRsp->code;
    return TSDB_CODE_SUCCESS;
  }

  static void* failRpcMallocCont(int64_t) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }

  static void* timerInit(int32_t, int32_t, int32_t, const char*) {
    return reinterpret_cast<void*>(static_cast<uintptr_t>(0x4001));
  }

  static void timerCleanup(void*) {}

  static tmr_h timerStart(TAOS_TMR_CALLBACK callback, int32_t delayMs, void* param, void*) {
    ++gCreateMetadataTest->timerStartCalls_;
    gCreateMetadataTest->timerDelayMs_ = delayMs;
    gCreateMetadataTest->timerCallback_ = callback;
    gCreateMetadataTest->timerParam_ = param;
    if (gCreateMetadataTest->blockTimerStart_) {
      std::unique_lock<std::mutex> lock(gCreateMetadataTest->timerStartMutex_);
      gCreateMetadataTest->timerStartEntered_ = true;
      gCreateMetadataTest->timerStartCv_.notify_all();
      if (!gCreateMetadataTest->timerStartCv_.wait_for(lock, std::chrono::seconds(5),
                                                       [] { return gCreateMetadataTest->releaseTimerStart_; })) {
        gCreateMetadataTest->timerStartTimedOut_ = true;
      }
    }
    return reinterpret_cast<tmr_h>(static_cast<uintptr_t>(0x4002));
  }

  static bool timerStopA(tmr_h* pTimer) {
    ++gCreateMetadataTest->timerStopCalls_;
    *pTimer = nullptr;
    return gCreateMetadataTest->timerCanStop_;
  }

 protected:
  void SetUp() override {
    gCreateMetadataTest = this;
    savedDisableStream_ = tsDisableStream;
    tsDisableStream = false;
#ifdef TD_ENTERPRISE
    savedFederatedQueryEnable_ = tsFederatedQueryEnable;
    tsFederatedQueryEnable = true;
#endif

    tstrncpy(user_.name, "root", sizeof(user_.name));
    tstrncpy(db_.name, "0.test", sizeof(db_.name));
    db_.uid = 700;
    db_.cfg.precision = TSDB_TIME_PRECISION_MILLI;
    vgroup_.vgId = 10;
    epSet_.numOfEps = 1;
    tstrncpy(epSet_.eps[0].fqdn, "localhost", sizeof(epSet_.eps[0].fqdn));
    epSet_.eps[0].port = 6030;
    extSource_.type = EXT_SOURCE_MYSQL;

    tstrncpy(stb_.name, "0.test.meters", sizeof(stb_.name));
    tstrncpy(stb_.db, "0.test", sizeof(stb_.db));
    stb_.uid = 900;
    stb_.dbUid = db_.uid;
    stb_.numOfColumns = 2;
    stb_.numOfTags = 1;
    stbColumns_[0] = {};
    stbColumns_[0].type = TSDB_DATA_TYPE_TIMESTAMP;
    stbColumns_[0].colId = PRIMARYKEY_TIMESTAMP_COL_ID;
    stbColumns_[0].bytes = 8;
    stbColumns_[1] = {};
    stbColumns_[1].type = TSDB_DATA_TYPE_INT;
    stbColumns_[1].colId = 2;
    stbColumns_[1].bytes = 4;
    stbTags_[0] = {};
    stbTags_[0].type = TSDB_DATA_TYPE_VARCHAR;
    stbTags_[0].colId = 3;
    stbTags_[0].bytes = 16;
    tstrncpy(stbColumns_[0].name, "ts", sizeof(stbColumns_[0].name));
    tstrncpy(stbColumns_[1].name, "value", sizeof(stbColumns_[1].name));
    tstrncpy(stbTags_[0].name, "tag_a", sizeof(stbTags_[0].name));
    stbCmpr_[0].id = stbColumns_[0].colId;
    stbCmpr_[1].id = stbColumns_[1].colId;
    stb_.pColumns = stbColumns_;
    stb_.pTags = stbTags_;
    stb_.pCmpr = stbCmpr_;
    taosInitRWLatch(&stb_.lock);

    stub_.set(sdbSetTable, testSdbSetTable);
    stub_.set(grantCheck, testGrantCheck);
    stub_.set(msmAssignRandomSnodeId, testAssignSnode);
    stub_.set(msmHandleBecomeNotLeader, testBecomeNotLeader);
    stub_.set(mndAcquireStream, acquireStream);
    stub_.set(mndAcquireUser, acquireUser);
    stub_.set(mndReleaseUser, releaseUser);
    stub_.set(mndCheckDbPrivilegeByName, checkDbPrivilege);
    stub_.set(sdbGetSize, getSdbSize);
    stub_.set(mndAcquireDb, acquireDb);
    stub_.set(mndReleaseDb, releaseDb);
    stub_.set(mndAcquireStb, acquireStb);
    stub_.set(mndReleaseStb, releaseStb);
    stub_.set(mndAcquireExtSource, acquireExtSource);
    stub_.set(mndReleaseExtSource, releaseExtSource);
    stub_.set(mstBuildDBVgroupsMap, buildDbVgroups);
    stub_.set(mstDestroyDbVgroupsHash, destroyDbVgroups);
    stub_.set(mstGetTableVgId, getTableVgId);
    stub_.set(mndAcquireVgroup, acquireVgroup);
    stub_.set(mndReleaseVgroup, releaseVgroup);
    stub_.set(mndGetVgroupEpset, getVgroupEpset);
    stub_.set(mndStreamCreateTrans, createTrans);
    stub_.set(asyncSendMsgToServer, sendAsync);
    stub_.set(asyncFreeConnById, freeConnection);
    stub_.set(rpcSendResponse, sendResponse);
    stub_.set(taosTmrInit, timerInit);
    stub_.set(taosTmrCleanUp, timerCleanup);
    stub_.set(taosTmrStart, timerStart);
    stub_.set(taosTmrStopA, timerStopA);

    mnode_.pSdb = &sdb_;
    mnode_.msgCb.clientRpc = reinterpret_cast<void*>(static_cast<uintptr_t>(0x3001));
    mnode_.msgCb.mgmt = this;
    mnode_.msgCb.putToQueueFp = putToQueue;
    ASSERT_EQ(TSDB_CODE_SUCCESS, mndInitStream(&mnode_));
    streamInitialized_ = true;
    handler_ = mnode_.msgFp[TMSG_INDEX(TDMT_MND_CREATE_STREAM)];
    ASSERT_NE(nullptr, handler_);
  }

  void TearDown() override {
    if (pendingSend_ != nullptr) {
      destroySendMsgInfo(pendingSend_);
      pendingSend_ = nullptr;
    }
    if (queuedMsg_.pCont != nullptr) {
      rpcFreeCont(queuedMsg_.pCont);
      queuedMsg_.pCont = nullptr;
    }
    if (streamInitialized_) mndCleanupStream(&mnode_);
    tsDisableStream = savedDisableStream_;
#ifdef TD_ENTERPRISE
    tsFederatedQueryEnable = savedFederatedQueryEnable_;
#endif
    gCreateMetadataTest = nullptr;
  }

  SStreamWindowPlan* makeSessionPlan() {
    SStreamWindowPlan* plan = static_cast<SStreamWindowPlan*>(taosMemoryCalloc(1, sizeof(*plan)));
    if (plan == nullptr) return nullptr;
    plan->version = STREAM_WINDOW_PLAN_VERSION;
    plan->pLayers = taosArrayInit(2, sizeof(SStreamWindowLayerSpec));
    if (plan->pLayers == nullptr) {
      tDestroyStreamWindowPlan(&plan);
      return nullptr;
    }

    SStreamWindowLayerSpec outer = {};
    outer.triggerType = WINDOW_TYPE_SESSION;
    tstrncpy(outer.name, "outer", sizeof(outer.name));
    outer.trigger.session.slotId = 0;
    outer.trigger.session.sessionVal = 10;
    if (taosArrayPush(plan->pLayers, &outer) == nullptr) {
      tDestroyStreamWindowPlan(&plan);
      return nullptr;
    }

    SStreamWindowLayerSpec leaf = {};
    leaf.triggerType = WINDOW_TYPE_SESSION;
    leaf.trigger.session.slotId = 1;
    leaf.trigger.session.sessionVal = 5;
    if (taosArrayPush(plan->pLayers, &leaf) == nullptr) {
      tDestroyStreamWindowPlan(&plan);
      return nullptr;
    }
    return plan;
  }

  SCMCreateStreamReq makeRequest() {
    SCMCreateStreamReq req = {};
    req.name = const_cast<char*>("0.test.nested_stream");
    req.streamId = 42;
    req.sql = const_cast<char*>("create stream nested_stream");
    req.streamDB = const_cast<char*>("0.test");
    req.triggerDB = const_cast<char*>("0.test");
    req.outDB = const_cast<char*>("0.test");
    req.triggerTblName = const_cast<char*>("meters");
    req.outTblName = const_cast<char*>("out");
    req.triggerType = WINDOW_TYPE_SESSION;
    req.trigger.session.slotId = 1;
    req.trigger.session.sessionVal = 5;
    req.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
    req.triggerTblType = TSDB_SUPER_TABLE;
    req.triggerTblUid = 111;
    req.triggerTblSuid = 222;
    req.triggerTblVgId = 333;
    req.triggerPrec = TSDB_TIME_PRECISION_NANO;
    req.calcTsSlotId = -1;
    req.triTsSlotId = -1;
    req.calcPkSlotId = -1;
    req.triPkSlotId = -1;
    req.pWindowPlan = makeSessionPlan();
    return req;
  }

  int32_t runCreate(SCMCreateStreamReq* pCreate) {
    const int32_t len = tSerializeSCMCreateStreamReq(nullptr, 0, pCreate);
    EXPECT_GT(len, 0);
    if (len <= 0) return len;
    std::vector<uint8_t> bytes(len);
    EXPECT_EQ(len, tSerializeSCMCreateStreamReq(bytes.data(), len, pCreate));

    SRpcMsg msg = {};
    msg.msgType = TDMT_MND_CREATE_STREAM;
    msg.pCont = bytes.data();
    msg.contLen = len;
    msg.info.node = &mnode_;
    msg.info.handle = reinterpret_cast<void*>(static_cast<uintptr_t>(0x1111));
    tstrncpy(msg.info.conn.user, "root", sizeof(msg.info.conn.user));
    return handler_(&msg);
  }

  std::vector<uint8_t> serializeMeta(const STableMetaRsp& source) {
    STableMetaRsp meta = source;
    const int32_t len = tSerializeSTableMetaRsp(nullptr, 0, &meta);
    EXPECT_GT(len, 0);
    if (len <= 0) return {};
    std::vector<uint8_t> bytes(len);
    EXPECT_EQ(len, tSerializeSTableMetaRsp(bytes.data(), len, &meta));
    return bytes;
  }

  STableMetaRsp makeNormalMeta() {
    STableMetaRsp meta = {};
    tstrncpy(meta.tbName, "meters", sizeof(meta.tbName));
    tstrncpy(meta.dbFName, "0.test", sizeof(meta.dbFName));
    meta.dbId = db_.uid;
    meta.numOfColumns = 2;
    meta.precision = TSDB_TIME_PRECISION_MILLI;
    meta.tableType = TSDB_NORMAL_TABLE;
    meta.tuid = 901;
    meta.vgId = vgroup_.vgId;
    meta.pSchemas = stbColumns_;
    meta.pSchemaExt = metaSchemaExt_;
    metaSchemaExt_[0].colId = stbColumns_[0].colId;
    metaSchemaExt_[1].colId = stbColumns_[1].colId;
    return meta;
  }

  SDataBuf makeCallbackResponse() {
    SDataBuf response = {};
    if (!callbackBytes_.empty()) {
      response.pData = taosMemoryMalloc(callbackBytes_.size());
      EXPECT_NE(nullptr, response.pData);
      if (response.pData != nullptr) memcpy(response.pData, callbackBytes_.data(), callbackBytes_.size());
      response.len = callbackBytes_.size();
    }
    return response;
  }

  void invokeCallback(int32_t code, const std::vector<uint8_t>& bytes, bool destroySend = true) {
    ASSERT_NE(nullptr, pendingSend_);
    SMsgSendInfo* send = pendingSend_;
    callbackBytes_ = bytes;
    SDataBuf response = makeCallbackResponse();
    send->fp(send->param, &response, code);
    if (destroySend) {
      destroySendMsgInfo(send);
      pendingSend_ = nullptr;
    }
  }

  int32_t runQueuedContinuation() {
    EXPECT_NE(nullptr, queuedMsg_.pCont);
    MndMsgFp continuation = mnode_.msgFp[TMSG_INDEX(TDMT_MND_UNUSED1)];
    EXPECT_NE(nullptr, continuation);
    if (queuedMsg_.pCont == nullptr || continuation == nullptr) return TSDB_CODE_INVALID_PARA;
    queuedMsg_.info.node = &mnode_;
    const int32_t code = continuation(&queuedMsg_);
    rpcFreeCont(queuedMsg_.pCont);
    queuedMsg_.pCont = nullptr;
    return code;
  }

  int32_t runContinuationBytes(const std::vector<uint8_t>& bytes) {
    MndMsgFp continuation = mnode_.msgFp[TMSG_INDEX(TDMT_MND_UNUSED1)];
    EXPECT_NE(nullptr, continuation);
    if (bytes.empty() || continuation == nullptr) return TSDB_CODE_INVALID_PARA;
    SRpcMsg msg = {};
    msg.msgType = TDMT_MND_UNUSED1;
    msg.pCont = const_cast<uint8_t*>(bytes.data());
    msg.contLen = static_cast<int32_t>(bytes.size());
    msg.info.node = &mnode_;
    return continuation(&msg);
  }

  void queueNormalContinuation(STableMetaRsp meta) {
    SCMCreateStreamReq req = makeRequest();
    ASSERT_NE(nullptr, req.pWindowPlan);
    ASSERT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
    ASSERT_NE(nullptr, pendingSend_);
    invokeCallback(TSDB_CODE_SUCCESS, serializeMeta(meta));
    ASSERT_NE(nullptr, queuedMsg_.pCont);
    freeRequestFixture(&req);
  }

  void fireTimer() {
    ASSERT_NE(nullptr, timerCallback_);
    TAOS_TMR_CALLBACK callback = timerCallback_;
    void*             param = timerParam_;
    timerCallback_ = nullptr;
    timerParam_ = nullptr;
    callback(param, reinterpret_cast<void*>(static_cast<uintptr_t>(0x4002)));
  }

  void addExtSpec(SCMCreateStreamReq* pReq, const char* sourceName, const char* extTable, const char* tsColumn) {
    if (pReq->extSpecs == nullptr) pReq->extSpecs = taosArrayInit(1, POINTER_BYTES);
    ASSERT_NE(nullptr, pReq->extSpecs);
    auto* spec = static_cast<SStreamExtTriggerSpec*>(taosMemoryCalloc(1, sizeof(SStreamExtTriggerSpec)));
    ASSERT_NE(nullptr, spec);
    tstrncpy(spec->sourceName, sourceName, sizeof(spec->sourceName));
    tstrncpy(spec->extTable, extTable, sizeof(spec->extTable));
    tstrncpy(spec->tsColumn, tsColumn, sizeof(spec->tsColumn));
    spec->sourceType = EXT_SOURCE_MYSQL;
    ASSERT_NE(nullptr, taosArrayPush(pReq->extSpecs, &spec));
    pReq->numOfExtSpecs = taosArrayGetSize(pReq->extSpecs);
  }

  void setColumnList(SCMCreateStreamReq* pReq, bool rollup, col_id_t colId, const char* colName, EColumnType colType,
                     int8_t dataType) {
    SNodeList* pList = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeList(&pList));
    SNode* pNode = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &pNode));
    auto* pCol = reinterpret_cast<SColumnNode*>(pNode);
    pCol->colId = colId;
    pCol->colType = colType;
    pCol->node.resType.type = dataType;
    tstrncpy(pCol->colName, colName, sizeof(pCol->colName));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListAppend(pList, pNode));
    char*   serialized = nullptr;
    int32_t serializedLen = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListToString(pList, false, &serialized, &serializedLen));
    nodesDestroyList(pList);
    if (rollup) {
      pReq->rollupTagCols = serialized;
    } else {
      pReq->partitionCols = serialized;
    }
  }

  void setPartitionFunction(SCMCreateStreamReq* pReq, const char* functionName, int32_t funcId, int32_t funcType) {
    SNodeList* pList = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeList(&pList));
    SNode* pNode = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_FUNCTION, &pNode));
    auto* pFunction = reinterpret_cast<SFunctionNode*>(pNode);
    tstrncpy(pFunction->functionName, functionName, sizeof(pFunction->functionName));
    pFunction->funcId = funcId;
    pFunction->funcType = funcType;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListAppend(pList, pNode));
    char*   serialized = nullptr;
    int32_t serializedLen = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListToString(pList, false, &serialized, &serializedLen));
    nodesDestroyList(pList);
    pReq->partitionCols = serialized;
  }

  SNode* makeFunction(const char* functionName, const std::vector<SNode*>& parameters = {}) {
    SNodeList* pParameterList = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeList(&pParameterList));
    if (pParameterList == nullptr) return nullptr;
    for (SNode* pParameter : parameters) {
      if (nodesListStrictAppend(pParameterList, pParameter) != TSDB_CODE_SUCCESS) {
        nodesDestroyList(pParameterList);
        return nullptr;
      }
    }
    SFunctionNode* pFunction = nullptr;
    const int32_t  code = ::createFunction(functionName, pParameterList, &pFunction);
    EXPECT_EQ(TSDB_CODE_SUCCESS, code);
    if (code != TSDB_CODE_SUCCESS) nodesDestroyList(pParameterList);
    return reinterpret_cast<SNode*>(pFunction);
  }

  void setWrappedTbnamePartition(SCMCreateStreamReq* pReq, const char* wrapperName) {
    SNode* pTbname = makeFunction("tbname");
    ASSERT_NE(nullptr, pTbname);
    std::vector<SNode*> parameters = {pTbname};
    if (strcmp(wrapperName, "substr") == 0) {
      SNode* pStart = nullptr;
      SNode* pLength = nullptr;
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeValueNodeFromInt32(1, &pStart));
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeValueNodeFromInt32(1, &pLength));
      parameters.push_back(pStart);
      parameters.push_back(pLength);
    }
    SNode* pWrapper = makeFunction(wrapperName, parameters);
    ASSERT_NE(nullptr, pWrapper);
    SNodeList* pList = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pList, pWrapper));
    char*   serialized = nullptr;
    int32_t serializedLen = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListToString(pList, false, &serialized, &serializedLen));
    nodesDestroyList(pList);
    pReq->partitionCols = serialized;
  }

  void setTbnameTagPartition(SCMCreateStreamReq* pReq) {
    SNodeList* pList = nullptr;
    SNode*     pTbname = makeFunction("tbname");
    ASSERT_NE(nullptr, pTbname);
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pList, pTbname));
    SNode* pTagNode = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &pTagNode));
    auto* pTag = reinterpret_cast<SColumnNode*>(pTagNode);
    pTag->colId = stbTags_[0].colId;
    pTag->colType = COLUMN_TYPE_COLUMN;
    pTag->node.resType.type = TSDB_DATA_TYPE_INT;
    tstrncpy(pTag->colName, stbTags_[0].name, sizeof(pTag->colName));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pList, pTagNode));
    char*   serialized = nullptr;
    int32_t serializedLen = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListToString(pList, false, &serialized, &serializedLen));
    nodesDestroyList(pList);
    pReq->partitionCols = serialized;
  }

  void useCountWindowPlan(SCMCreateStreamReq* pReq) {
    for (int32_t i = 0; i < taosArrayGetSize(pReq->pWindowPlan->pLayers); ++i) {
      auto* pLayer = static_cast<SStreamWindowLayerSpec*>(taosArrayGet(pReq->pWindowPlan->pLayers, i));
      pLayer->triggerType = WINDOW_TYPE_COUNT;
      pLayer->trigger = {};
      pLayer->trigger.count.countVal = 1;
      pLayer->trigger.count.sliding = 1;
    }
    pReq->triggerType = WINDOW_TYPE_COUNT;
    pReq->trigger = {};
    pReq->trigger.count.countVal = 1;
    pReq->trigger.count.sliding = 1;
  }

  void freeRequestFixture(SCMCreateStreamReq* pReq) {
    tDestroyStreamWindowPlan(&pReq->pWindowPlan);
    taosMemoryFreeClear(pReq->partitionCols);
    taosMemoryFreeClear(pReq->rollupTagCols);
    if (pReq->extSpecs != nullptr) {
      for (int32_t i = 0; i < taosArrayGetSize(pReq->extSpecs); ++i) {
        taosMemoryFree(*static_cast<SStreamExtTriggerSpec**>(taosArrayGet(pReq->extSpecs, i)));
      }
      taosArrayDestroy(pReq->extSpecs);
      pReq->extSpecs = nullptr;
    }
  }

  Stub                    stub_;
  SSdb                    sdb_ = {};
  SMnode                  mnode_ = {};
  SUserObj                user_ = {};
  SDbObj                  db_ = {};
  SStbObj                 stb_ = {};
  SVgObj                  vgroup_ = {};
  SEpSet                  epSet_ = {};
  SSchema                 stbColumns_[2] = {};
  SSchema                 stbTags_[1] = {};
  SColCmpr                stbCmpr_[2] = {};
  SSchemaExt              metaSchemaExt_[2] = {};
  SExtSourceObj           extSource_ = {};
  MndMsgFp                handler_ = nullptr;
  SMsgSendInfo*           pendingSend_ = nullptr;
  SRpcMsg                 queuedMsg_ = {};
  std::vector<uint8_t>    callbackBytes_;
  bool                    returnStb_ = false;
  bool                    returnDb_ = true;
  bool                    returnExtSource_ = true;
  bool                    savedDisableStream_ = false;
  bool                    streamInitialized_ = false;
  bool                    callbackBeforeSendReturn_ = false;
  bool                    timerCanStop_ = true;
  bool                    blockTimerStart_ = false;
  bool                    timerStartEntered_ = false;
  bool                    releaseTimerStart_ = false;
  bool                    timerStartTimedOut_ = false;
  std::mutex              timerStartMutex_;
  std::condition_variable timerStartCv_;
  bool                    blockVgroupLookup_ = false;
  bool                    vgroupLookupEntered_ = false;
  bool                    releaseVgroupLookup_ = false;
  bool                    vgroupLookupTimedOut_ = false;
  std::mutex              vgroupLookupMutex_;
  std::condition_variable vgroupLookupCv_;
  bool                    blockStbLookup_ = false;
  bool                    stbLookupEntered_ = false;
  bool                    releaseStbLookup_ = false;
  bool                    stbLookupTimedOut_ = false;
  std::mutex              stbLookupMutex_;
  std::condition_variable stbLookupCv_;
  bool                    blockQueue_ = false;
  bool                    processQueueInline_ = false;
  bool                    queueEntered_ = false;
  bool                    releaseQueue_ = false;
  bool                    queueTimedOut_ = false;
  std::mutex              queueMutex_;
  std::condition_variable queueCv_;
#ifdef TD_ENTERPRISE
  bool savedFederatedQueryEnable_ = false;
#endif
  int32_t                  sendCode_ = TSDB_CODE_SUCCESS;
  int32_t                  callbackCode_ = TSDB_CODE_SUCCESS;
  int32_t                  queueCode_ = TSDB_CODE_SUCCESS;
  int64_t                  transporterId_ = 55;
  int32_t                  timerDelayMs_ = 0;
  int32_t                  timerStartCalls_ = 0;
  int32_t                  timerStopCalls_ = 0;
  TAOS_TMR_CALLBACK        timerCallback_ = nullptr;
  void*                    timerParam_ = nullptr;
  int32_t                  asyncSendCalls_ = 0;
  int32_t                  freeConnectionCalls_ = 0;
  int32_t                  queueCalls_ = 0;
  int32_t                  responseCalls_ = 0;
  int32_t                  lastResponseCode_ = TSDB_CODE_SUCCESS;
  int32_t                  createTransCalls_ = 0;
  int32_t                  transactionOrigin_ = 0;
  int8_t                   transactionTableType_ = 0;
  uint64_t                 transactionTableUid_ = 0;
  uint64_t                 transactionTableSuid_ = 0;
  int32_t                  transactionVgId_ = 0;
  int8_t                   transactionPrecision_ = 0;
  int64_t                  transactionFlags_ = 0;
  int32_t                  transactionPartitionColType_ = -1;
  int32_t                  transactionPartitionDataType_ = -1;
  int32_t                  transactionPartitionBytes_ = -1;
  std::vector<std::string> acquiredExtSources_;
  std::vector<int64_t>     freedTransporterIds_;
};

TEST_F(MndStreamCreateMetadataTest, NormalTableUsesVnodePreflightBeforeTransaction) {
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  req.enableMultiGroupCalc = 1;

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  EXPECT_EQ(1, asyncSendCalls_);
  EXPECT_EQ(0, createTransCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, ExtCandidateUsesAuthoritativeSourceIdentity) {
  for (const auto& fixture : {std::pair<const char*, const char*>("0.ext_source", ""),
                              std::pair<const char*, const char*>("cluster.0.ext_source", "0.wrong")}) {
    SCOPED_TRACE(fixture.first);
    acquiredExtSources_.clear();
    asyncSendCalls_ = 0;
    createTransCalls_ = 0;

    SCMCreateStreamReq req = makeRequest();
    ASSERT_NE(nullptr, req.pWindowPlan);
    req.triggerDB = const_cast<char*>(fixture.second);
    addExtSpec(&req, fixture.first, "remote_meters", "ts");

    EXPECT_NE(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
    ASSERT_EQ(1U, acquiredExtSources_.size());
    EXPECT_EQ(fixture.first, acquiredExtSources_[0]);
    EXPECT_EQ(0, asyncSendCalls_);
    EXPECT_EQ(0, createTransCalls_);

    freeRequestFixture(&req);
  }
}

TEST_F(MndStreamCreateMetadataTest, ExtCandidateRequiresExistingAuthoritativeSourceType) {
  for (const bool sourceExists : {false, true}) {
    SCOPED_TRACE(sourceExists);
    returnExtSource_ = sourceExists;
    extSource_.type = sourceExists ? EXT_SOURCE_TDENGINE : EXT_SOURCE_MYSQL;
    SCMCreateStreamReq req = makeRequest();
    ASSERT_NE(nullptr, req.pWindowPlan);
    addExtSpec(&req, "cluster.0.ext_source", "remote_meters", "ts");

    EXPECT_NE(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
    EXPECT_EQ(0, asyncSendCalls_);
    EXPECT_EQ(0, createTransCalls_);
    freeRequestFixture(&req);
  }
}

TEST_F(MndStreamCreateMetadataTest, CalcOnlyExtSpecStillUsesLocalPreflight) {
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  addExtSpec(&req, "0.calc_source", "", "");

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  EXPECT_EQ(1, asyncSendCalls_);
  EXPECT_EQ(0, createTransCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, RejectsMalformedAndAmbiguousExtCandidates) {
  struct Fixture {
    const char* firstSource;
    const char* firstTable;
    const char* firstTs;
    const char* secondSource;
    const char* secondTable;
    const char* secondTs;
  };
  const Fixture fixtures[] = {
      {"", "remote", "ts", nullptr, nullptr, nullptr},        {"0.src", "remote", "", nullptr, nullptr, nullptr},
      {"0.src", "", "ts", nullptr, nullptr, nullptr},         {"", "", "", nullptr, nullptr, nullptr},
      {"0.src1", "remote1", "ts", "0.src2", "remote2", "ts"},
  };

  for (const auto& fixture : fixtures) {
    asyncSendCalls_ = 0;
    createTransCalls_ = 0;
    SCMCreateStreamReq req = makeRequest();
    ASSERT_NE(nullptr, req.pWindowPlan);
    addExtSpec(&req, fixture.firstSource, fixture.firstTable, fixture.firstTs);
    if (fixture.secondSource != nullptr) {
      addExtSpec(&req, fixture.secondSource, fixture.secondTable, fixture.secondTs);
    }

    EXPECT_NE(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
    EXPECT_EQ(0, asyncSendCalls_);
    EXPECT_EQ(0, createTransCalls_);
    freeRequestFixture(&req);
  }
}

TEST_F(MndStreamCreateMetadataTest, SuperTableMetadataNormalizesIdentityBeforeTransaction) {
  returnStb_ = true;
  stb_.virtualStb = 1;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  EXPECT_EQ(1, queueCalls_);
  EXPECT_EQ(0, asyncSendCalls_);
  EXPECT_EQ(0, createTransCalls_);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
  EXPECT_EQ(1, createTransCalls_);
  EXPECT_EQ(TDMT_MND_CREATE_STREAM, transactionOrigin_);
  EXPECT_EQ(TSDB_SUPER_TABLE, transactionTableType_);
  EXPECT_EQ(stb_.uid, transactionTableUid_);
  EXPECT_EQ(stb_.uid, transactionTableSuid_);
  EXPECT_EQ(0, transactionVgId_);
  EXPECT_EQ(TSDB_TIME_PRECISION_MILLI, transactionPrecision_);
  EXPECT_NE(0, transactionFlags_ & CREATE_STREAM_FLAG_TRIGGER_VIRTUAL_STB);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, SuperTablePayloadAllocationFailureRetainsHandlerResponseOwnership) {
  returnStb_ = true;
  stub_.set(rpcMallocCont, failRpcMallocCont);
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);

  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, runCreate(&req));
  EXPECT_EQ(0, queueCalls_);
  EXPECT_EQ(0, responseCalls_);
  EXPECT_EQ(0, createTransCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, SuperTableQueueFailureOwnsResponseAndPayload) {
  returnStb_ = true;
  queueCode_ = TSDB_CODE_APP_IS_STOPPING;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  EXPECT_EQ(1, queueCalls_);
  EXPECT_EQ(1, responseCalls_);
  EXPECT_EQ(queueCode_, lastResponseCode_);
  EXPECT_EQ(0, createTransCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, AuthoritativeCompositePrimaryKeyRejectsForgedColumnFlags) {
  returnStb_ = true;
  stbColumns_[1].flags = COL_IS_KEY;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_EQ(1, queueCalls_);
  EXPECT_NE(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
  EXPECT_EQ(0, createTransCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, PartitionColumnsResolveAgainstAuthoritativeTagSchema) {
  returnStb_ = true;
  for (const bool authoritativeTag : {true, false}) {
    SCOPED_TRACE(authoritativeTag);
    queueCalls_ = 0;
    createTransCalls_ = 0;
    SCMCreateStreamReq req = makeRequest();
    ASSERT_NE(nullptr, req.pWindowPlan);
    setColumnList(&req, false, authoritativeTag ? stbTags_[0].colId : stbColumns_[1].colId,
                  authoritativeTag ? stbTags_[0].name : stbColumns_[1].name, COLUMN_TYPE_TAG, TSDB_DATA_TYPE_VARCHAR);

    EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
    ASSERT_EQ(1, queueCalls_);
    const int32_t code = runQueuedContinuation();
    if (authoritativeTag) {
      EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, code);
      EXPECT_EQ(1, createTransCalls_);
    } else {
      EXPECT_NE(TSDB_CODE_ACTION_IN_PROGRESS, code);
      EXPECT_EQ(0, createTransCalls_);
    }
    freeRequestFixture(&req);
  }
}

TEST_F(MndStreamCreateMetadataTest, AuthoritativeTagTypeIsPersistedInPartitionAst) {
  returnStb_ = true;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  setColumnList(&req, false, stbTags_[0].colId, stbTags_[0].name, COLUMN_TYPE_COLUMN, TSDB_DATA_TYPE_INT);

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_EQ(1, queueCalls_);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
  EXPECT_EQ(1, createTransCalls_);
  EXPECT_EQ(COLUMN_TYPE_TAG, transactionPartitionColType_);
  EXPECT_EQ(stbTags_[0].type, transactionPartitionDataType_);
  EXPECT_EQ(stbTags_[0].bytes, transactionPartitionBytes_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, PartitionTbnameFactUsesCanonicalFunctionIdentity) {
  returnStb_ = true;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  useCountWindowPlan(&req);
  setPartitionFunction(&req, "timezone", fmGetFuncId("timezone"), FUNCTION_TYPE_TBNAME);

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_EQ(1, queueCalls_);
  EXPECT_NE(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
  EXPECT_EQ(0, createTransCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, WrappedTbnameDoesNotSatisfySuperTableDataDrivenPartitionRequirement) {
  returnStb_ = true;
  for (const char* wrapperName : {"length", "substr"}) {
    queueCalls_ = 0;
    createTransCalls_ = 0;
    SCMCreateStreamReq req = makeRequest();
    ASSERT_NE(nullptr, req.pWindowPlan);
    useCountWindowPlan(&req);
    setWrappedTbnamePartition(&req, wrapperName);

    EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
    EXPECT_EQ(1, queueCalls_);
    EXPECT_NE(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
    EXPECT_EQ(0, createTransCalls_);

    freeRequestFixture(&req);
  }
}

TEST_F(MndStreamCreateMetadataTest, BareTbnameWithTagSatisfiesSuperTableDataDrivenPartitionRequirement) {
  returnStb_ = true;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  useCountWindowPlan(&req);
  setTbnameTagPartition(&req);

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  EXPECT_EQ(1, queueCalls_);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
  EXPECT_EQ(1, createTransCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, CanonicalTbnameSatisfiesSuperTableCountPartitionRequirement) {
  returnStb_ = true;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  useCountWindowPlan(&req);
  const int32_t tbnameId = fmGetFuncId("tbname");
  setPartitionFunction(&req, "tbname", tbnameId, fmGetFuncTypeFromId(tbnameId));

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_EQ(1, queueCalls_);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
  EXPECT_EQ(1, createTransCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, PartitionCaseWhenScalarExpressionUsesAuthoritativeTags) {
  returnStb_ = true;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);

  SNodeList* pList = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeList(&pList));
  SNode* pCaseNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_CASE_WHEN, &pCaseNode));
  auto* pCase = reinterpret_cast<SCaseWhenNode*>(pCaseNode);
  pCase->node.resType.type = stbTags_[0].type;
  pCase->node.resType.bytes = stbTags_[0].bytes;

  SNode* pWhenThenNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_WHEN_THEN, &pWhenThenNode));
  auto* pWhenThen = reinterpret_cast<SWhenThenNode*>(pWhenThenNode);
  pWhenThen->node.resType = pCase->node.resType;
  SValueNode* pWhen = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeValueNodeFromBool(true, &pWhen));
  pWhenThen->pWhen = reinterpret_cast<SNode*>(pWhen);

  SNode* pTagNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &pTagNode));
  auto* pTag = reinterpret_cast<SColumnNode*>(pTagNode);
  pTag->colId = stbTags_[0].colId;
  pTag->colType = COLUMN_TYPE_COLUMN;
  pTag->node.resType.type = TSDB_DATA_TYPE_INT;
  tstrncpy(pTag->colName, stbTags_[0].name, sizeof(pTag->colName));
  pWhenThen->pThen = pTagNode;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pCase->pWhenThenList, pWhenThenNode));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListAppend(pList, pCaseNode));

  char*   serialized = nullptr;
  int32_t serializedLen = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListToString(pList, false, &serialized, &serializedLen));
  nodesDestroyList(pList);
  req.partitionCols = serialized;

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_EQ(1, queueCalls_);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
  EXPECT_EQ(1, createTransCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, PartitionInListScalarExpressionUsesAuthoritativeTags) {
  returnStb_ = true;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);

  SNodeList* pList = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeList(&pList));
  SNode* pOperatorNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_OPERATOR, &pOperatorNode));
  auto* pOperator = reinterpret_cast<SOperatorNode*>(pOperatorNode);
  pOperator->opType = OP_TYPE_IN;
  pOperator->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOperator->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;

  SNode* pTagNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &pTagNode));
  auto* pTag = reinterpret_cast<SColumnNode*>(pTagNode);
  pTag->colId = stbTags_[0].colId;
  pTag->colType = COLUMN_TYPE_COLUMN;
  pTag->node.resType.type = TSDB_DATA_TYPE_INT;
  tstrncpy(pTag->colName, stbTags_[0].name, sizeof(pTag->colName));
  pOperator->pLeft = pTagNode;

  SNode* pValuesNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_NODE_LIST, &pValuesNode));
  auto*       pValues = reinterpret_cast<SNodeListNode*>(pValuesNode);
  char        literal[] = "x";
  SValueNode* pValue = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeValueNodeFromString(literal, &pValue));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pValues->pNodeList, reinterpret_cast<SNode*>(pValue)));
  pOperator->pRight = pValuesNode;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListAppend(pList, pOperatorNode));

  char*   serialized = nullptr;
  int32_t serializedLen = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListToString(pList, false, &serialized, &serializedLen));
  nodesDestroyList(pList);
  req.partitionCols = serialized;

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_EQ(1, queueCalls_);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
  EXPECT_EQ(1, createTransCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, RollupRequiresAuthoritativeStringTag) {
  returnStb_ = true;
  for (const bool authoritativeStringTag : {true, false}) {
    SCOPED_TRACE(authoritativeStringTag);
    queueCalls_ = 0;
    createTransCalls_ = 0;
    SCMCreateStreamReq req = makeRequest();
    ASSERT_NE(nullptr, req.pWindowPlan);
    const SSchema& schema = authoritativeStringTag ? stbTags_[0] : stbColumns_[1];
    setColumnList(&req, true, schema.colId, schema.name, COLUMN_TYPE_TAG, TSDB_DATA_TYPE_VARCHAR);

    EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
    ASSERT_EQ(1, queueCalls_);
    const int32_t code = runQueuedContinuation();
    if (authoritativeStringTag) {
      EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, code);
      EXPECT_EQ(1, createTransCalls_);
    } else {
      EXPECT_NE(TSDB_CODE_ACTION_IN_PROGRESS, code);
      EXPECT_EQ(0, createTransCalls_);
    }
    freeRequestFixture(&req);
  }
}

TEST_F(MndStreamCreateMetadataTest, ColumnListsRejectMissingMismatchedAmbiguousAndMalformedEntries) {
  returnStb_ = true;
  enum class Fixture { missing, mismatched, ambiguous, malformed };
  for (const Fixture fixture : {Fixture::missing, Fixture::mismatched, Fixture::ambiguous, Fixture::malformed}) {
    SCOPED_TRACE(static_cast<int32_t>(fixture));
    queueCalls_ = 0;
    createTransCalls_ = 0;
    SCMCreateStreamReq req = makeRequest();
    ASSERT_NE(nullptr, req.pWindowPlan);
    if (fixture == Fixture::missing) {
      setColumnList(&req, false, 99, "missing", COLUMN_TYPE_TAG, TSDB_DATA_TYPE_VARCHAR);
    } else if (fixture == Fixture::mismatched) {
      setColumnList(&req, false, stbTags_[0].colId, stbColumns_[1].name, COLUMN_TYPE_TAG, TSDB_DATA_TYPE_VARCHAR);
    } else if (fixture == Fixture::ambiguous) {
      tstrncpy(stbColumns_[1].name, stbTags_[0].name, sizeof(stbColumns_[1].name));
      setColumnList(&req, false, stbTags_[0].colId, stbTags_[0].name, COLUMN_TYPE_TAG, TSDB_DATA_TYPE_VARCHAR);
    } else {
      req.partitionCols = taosStrdup("{");
      ASSERT_NE(nullptr, req.partitionCols);
    }

    EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
    ASSERT_EQ(1, queueCalls_);
    EXPECT_NE(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
    EXPECT_EQ(0, createTransCalls_);
    tstrncpy(stbColumns_[1].name, "value", sizeof(stbColumns_[1].name));
    freeRequestFixture(&req);
  }
}

TEST_F(MndStreamCreateMetadataTest, ChildMetadataNormalizesIdentityWithoutRejectingMultiGroupCreate) {
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  req.enableMultiGroupCalc = 1;
  STableMetaRsp meta = makeNormalMeta();
  meta.tableType = TSDB_CHILD_TABLE;
  meta.suid = 800;
  tstrncpy(meta.stbName, "meters_stb", sizeof(meta.stbName));

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  invokeCallback(TSDB_CODE_SUCCESS, serializeMeta(meta));
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
  EXPECT_EQ(1, createTransCalls_);
  EXPECT_EQ(TSDB_CHILD_TABLE, transactionTableType_);
  EXPECT_EQ(meta.tuid, transactionTableUid_);
  EXPECT_EQ(meta.suid, transactionTableSuid_);
  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, VnodeMetadataMustMatchRequestedTableAndAuthoritativeRoute) {
  enum class Fixture {
    tableName,
    databaseName,
    vgroup,
    tableType,
    normalSuid,
    normalStbName,
    childSameUid,
    precision,
  };
  for (const Fixture fixture :
       {Fixture::tableName, Fixture::databaseName, Fixture::vgroup, Fixture::tableType, Fixture::normalSuid,
        Fixture::normalStbName, Fixture::childSameUid, Fixture::precision}) {
    SCOPED_TRACE(static_cast<int32_t>(fixture));
    queueCalls_ = 0;
    createTransCalls_ = 0;
    SCMCreateStreamReq req = makeRequest();
    ASSERT_NE(nullptr, req.pWindowPlan);
    STableMetaRsp meta = makeNormalMeta();
    if (fixture == Fixture::tableName) tstrncpy(meta.tbName, "other", sizeof(meta.tbName));
    if (fixture == Fixture::databaseName) tstrncpy(meta.dbFName, "0.other", sizeof(meta.dbFName));
    if (fixture == Fixture::vgroup) meta.vgId = vgroup_.vgId + 1;
    if (fixture == Fixture::tableType) meta.tableType = TSDB_SUPER_TABLE;
    if (fixture == Fixture::normalSuid) meta.suid = 800;
    if (fixture == Fixture::normalStbName) tstrncpy(meta.stbName, "meters_stb", sizeof(meta.stbName));
    if (fixture == Fixture::childSameUid) {
      meta.tableType = TSDB_CHILD_TABLE;
      meta.suid = meta.tuid;
      tstrncpy(meta.stbName, "meters_stb", sizeof(meta.stbName));
    }
    if (fixture == Fixture::precision) meta.precision = TSDB_TIME_PRECISION_MICRO;

    EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
    invokeCallback(TSDB_CODE_SUCCESS, serializeMeta(meta));
    EXPECT_NE(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
    EXPECT_EQ(0, createTransCalls_);
    freeRequestFixture(&req);
  }
}

TEST_F(MndStreamCreateMetadataTest, SuccessfulVnodeResponseHandsOffOneContinuation) {
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_NE(nullptr, pendingSend_);

  const std::vector<uint8_t> metaBytes = serializeMeta(makeNormalMeta());
  invokeCallback(TSDB_CODE_SUCCESS, metaBytes, false);
  EXPECT_EQ(1, queueCalls_);
  EXPECT_EQ(0, responseCalls_);
  invokeCallback(TSDB_CODE_SUCCESS, metaBytes);
  EXPECT_EQ(1, queueCalls_);
  EXPECT_EQ(0, responseCalls_);
  EXPECT_EQ(1, freeConnectionCalls_);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
  EXPECT_EQ(1, createTransCalls_);
  EXPECT_EQ(TDMT_MND_CREATE_STREAM, transactionOrigin_);
  EXPECT_EQ(TSDB_NORMAL_TABLE, transactionTableType_);
  EXPECT_EQ(901U, transactionTableUid_);
  EXPECT_EQ(0U, transactionTableSuid_);
  EXPECT_EQ(vgroup_.vgId, transactionVgId_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, ContinuationCanClaimWhileQueuePublicationIsReturning) {
  processQueueInline_ = true;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);

  ASSERT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_NE(nullptr, pendingSend_);
  invokeCallback(TSDB_CODE_SUCCESS, serializeMeta(makeNormalMeta()));

  EXPECT_EQ(1, queueCalls_);
  EXPECT_EQ(1, createTransCalls_);
  EXPECT_EQ(nullptr, queuedMsg_.pCont);
  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, ContinuationTokenIsSingleUse) {
  queueNormalContinuation(makeNormalMeta());
  const auto*          pBytes = static_cast<const uint8_t*>(queuedMsg_.pCont);
  std::vector<uint8_t> continuation(pBytes, pBytes + queuedMsg_.contLen);

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
  EXPECT_EQ(1, createTransCalls_);
  EXPECT_NE(TSDB_CODE_ACTION_IN_PROGRESS, runContinuationBytes(continuation));
  EXPECT_EQ(1, createTransCalls_);
}

TEST_F(MndStreamCreateMetadataTest, RpcErrorRepliesOnceWithoutContinuation) {
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_NE(nullptr, pendingSend_);

  invokeCallback(TSDB_CODE_RPC_TIMEOUT, {});
  EXPECT_EQ(1, responseCalls_);
  EXPECT_EQ(TSDB_CODE_RPC_TIMEOUT, lastResponseCode_);
  EXPECT_EQ(0, queueCalls_);
  EXPECT_EQ(0, createTransCalls_);
  EXPECT_EQ(1, freeConnectionCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, ApplicationTimeoutRepliesOnceAndReleasesTransporter) {
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_NE(nullptr, pendingSend_);
  ASSERT_NE(nullptr, timerCallback_);
  EXPECT_EQ(tsStatusSRTimeoutMs, timerDelayMs_);

  fireTimer();
  EXPECT_EQ(1, responseCalls_);
  EXPECT_EQ(0, queueCalls_);
  EXPECT_EQ(0, createTransCalls_);
  EXPECT_EQ(1, freeConnectionCalls_);

  destroySendMsgInfo(pendingSend_);
  pendingSend_ = nullptr;
  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, TimeoutAndCallbackRaceHasOneTerminalOwner) {
  timerCanStop_ = false;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_NE(nullptr, pendingSend_);
  ASSERT_NE(nullptr, timerCallback_);

  SMsgSendInfo* send = pendingSend_;
  const auto    metaBytes = serializeMeta(makeNormalMeta());
  callbackBytes_ = metaBytes;
  SDataBuf          response = makeCallbackResponse();
  TAOS_TMR_CALLBACK timerCallback = timerCallback_;
  void*             timerParam = timerParam_;
  timerCallback_ = nullptr;
  timerParam_ = nullptr;

  std::mutex              raceMutex;
  std::condition_variable raceCv;
  int32_t                 ready = 0;
  bool                    start = false;
  std::atomic<bool>       startWaitTimedOut{false};
  auto                    waitForStart = [&] {
    std::unique_lock<std::mutex> lock(raceMutex);
    ++ready;
    raceCv.notify_all();
    if (!raceCv.wait_for(lock, std::chrono::seconds(5), [&] { return start; })) startWaitTimedOut.store(true);
  };
  std::thread callbackThread([&] {
    waitForStart();
    send->fp(send->param, &response, TSDB_CODE_SUCCESS);
  });
  std::thread timeoutThread([&] {
    waitForStart();
    timerCallback(timerParam, reinterpret_cast<void*>(static_cast<uintptr_t>(0x4002)));
  });
  bool        readyReached = false;
  {
    std::unique_lock<std::mutex> lock(raceMutex);
    readyReached = raceCv.wait_for(lock, std::chrono::seconds(5), [&] { return ready == 2; });
    start = true;
  }
  raceCv.notify_all();
  callbackThread.join();
  timeoutThread.join();
  ASSERT_TRUE(readyReached);
  EXPECT_FALSE(startWaitTimedOut.load());
  destroySendMsgInfo(send);
  pendingSend_ = nullptr;

  EXPECT_EQ(1, responseCalls_ + queueCalls_);
  EXPECT_EQ(1, freeConnectionCalls_);
  if (queueCalls_ == 1) {
    EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
    EXPECT_EQ(1, createTransCalls_);
  } else {
    EXPECT_EQ(0, createTransCalls_);
  }
  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, ImmediateSendFailureRepliesOnce) {
  sendCode_ = TSDB_CODE_RPC_BROKEN_LINK;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  EXPECT_EQ(1, asyncSendCalls_);
  EXPECT_EQ(1, responseCalls_);
  EXPECT_EQ(sendCode_, lastResponseCode_);
  EXPECT_EQ(0, queueCalls_);
  EXPECT_EQ(0, createTransCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, QueueFailureOwnsReplyAndPayload) {
  queueCode_ = TSDB_CODE_APP_IS_STOPPING;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_NE(nullptr, pendingSend_);

  invokeCallback(TSDB_CODE_SUCCESS, serializeMeta(makeNormalMeta()));
  EXPECT_EQ(1, queueCalls_);
  EXPECT_EQ(1, responseCalls_);
  EXPECT_EQ(queueCode_, lastResponseCode_);
  EXPECT_EQ(0, createTransCalls_);
  EXPECT_EQ(1, freeConnectionCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, MalformedMetadataFailsInContinuation) {
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_NE(nullptr, pendingSend_);

  invokeCallback(TSDB_CODE_SUCCESS, {1, 2, 3, 4});
  ASSERT_EQ(1, queueCalls_);
  EXPECT_NE(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
  EXPECT_EQ(0, createTransCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, ContinuationTokenRejectsInvalidReferenceNonceAndLength) {
  enum class Fixture { zero, invalidRefSet, invalidNonce, truncated, trailing };
  for (const Fixture fixture :
       {Fixture::zero, Fixture::invalidRefSet, Fixture::invalidNonce, Fixture::truncated, Fixture::trailing}) {
    SCOPED_TRACE(static_cast<int32_t>(fixture));
    queueCalls_ = 0;
    createTransCalls_ = 0;
    queueNormalContinuation(makeNormalMeta());
    ASSERT_GE(queuedMsg_.contLen, 8);
    auto* bytes = static_cast<uint8_t*>(queuedMsg_.pCont);
    if (fixture == Fixture::zero) memset(bytes, 0, queuedMsg_.contLen);
    if (fixture == Fixture::invalidRefSet) memset(bytes, 0xff, sizeof(int32_t));
    if (fixture == Fixture::invalidNonce) bytes[queuedMsg_.contLen - 1] ^= 0xff;
    if (fixture == Fixture::truncated) --queuedMsg_.contLen;
    if (fixture == Fixture::trailing) {
      void* expanded = rpcMallocCont(queuedMsg_.contLen + 1);
      ASSERT_NE(nullptr, expanded);
      memcpy(expanded, queuedMsg_.pCont, queuedMsg_.contLen);
      static_cast<uint8_t*>(expanded)[queuedMsg_.contLen] = 0;
      rpcFreeCont(queuedMsg_.pCont);
      queuedMsg_.pCont = expanded;
      ++queuedMsg_.contLen;
    }

    EXPECT_NE(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
    EXPECT_EQ(0, createTransCalls_);
  }
}

TEST_F(MndStreamCreateMetadataTest, CleanupRejectsCreateAlreadyPastInitialStoppingCheck) {
  blockVgroupLookup_ = true;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);

  std::atomic<int32_t> createCode{TSDB_CODE_SUCCESS};
  std::thread          createThread([&] { createCode.store(runCreate(&req)); });

  bool lookupEntered = false;
  {
    std::unique_lock<std::mutex> lock(vgroupLookupMutex_);
    lookupEntered = vgroupLookupCv_.wait_for(lock, std::chrono::seconds(5), [&] { return vgroupLookupEntered_; });
  }
  if (lookupEntered) {
    mndCleanupStream(&mnode_);
    streamInitialized_ = false;
  }
  {
    std::lock_guard<std::mutex> lock(vgroupLookupMutex_);
    releaseVgroupLookup_ = true;
  }
  vgroupLookupCv_.notify_all();
  createThread.join();

  ASSERT_TRUE(lookupEntered);
  EXPECT_FALSE(vgroupLookupTimedOut_);
  EXPECT_EQ(TSDB_CODE_APP_IS_STOPPING, createCode.load());
  EXPECT_EQ(0, asyncSendCalls_);
  EXPECT_EQ(0, responseCalls_);
  EXPECT_EQ(0, queueCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, CleanupWaitsForAdmittedPreflightInitialization) {
  blockTimerStart_ = true;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);

  std::atomic<int32_t> createCode{TSDB_CODE_SUCCESS};
  std::thread          createThread([&] { createCode.store(runCreate(&req)); });

  bool timerStartEntered = false;
  {
    std::unique_lock<std::mutex> lock(timerStartMutex_);
    timerStartEntered = timerStartCv_.wait_for(lock, std::chrono::seconds(5), [&] { return timerStartEntered_; });
  }

  std::mutex              cleanupMutex;
  std::condition_variable cleanupCv;
  bool                    cleanupDone = false;
  std::thread             cleanupThread;
  if (timerStartEntered) {
    cleanupThread = std::thread([&] {
      mndCleanupStream(&mnode_);
      {
        std::lock_guard<std::mutex> lock(cleanupMutex);
        cleanupDone = true;
      }
      cleanupCv.notify_all();
    });
    std::unique_lock<std::mutex> lock(cleanupMutex);
    EXPECT_FALSE(cleanupCv.wait_for(lock, std::chrono::milliseconds(100), [&] { return cleanupDone; }));
  }

  {
    std::lock_guard<std::mutex> lock(timerStartMutex_);
    releaseTimerStart_ = true;
  }
  timerStartCv_.notify_all();
  createThread.join();
  if (cleanupThread.joinable()) cleanupThread.join();

  ASSERT_TRUE(timerStartEntered);
  streamInitialized_ = false;
  EXPECT_FALSE(timerStartTimedOut_);
  EXPECT_TRUE(cleanupDone);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, createCode.load());
  EXPECT_EQ(1, asyncSendCalls_);
  EXPECT_EQ(1, responseCalls_);
  EXPECT_EQ(1, freeConnectionCalls_);

  if (pendingSend_ != nullptr) {
    destroySendMsgInfo(pendingSend_);
    pendingSend_ = nullptr;
  }
  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, CleanupRejectsSuperTableBeforeContinuationHandoff) {
  returnStb_ = true;
  blockStbLookup_ = true;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);

  std::atomic<int32_t> createCode{TSDB_CODE_SUCCESS};
  std::thread          createThread([&] { createCode.store(runCreate(&req)); });

  bool lookupEntered = false;
  {
    std::unique_lock<std::mutex> lock(stbLookupMutex_);
    lookupEntered = stbLookupCv_.wait_for(lock, std::chrono::seconds(5), [&] { return stbLookupEntered_; });
  }
  if (lookupEntered) {
    mndCleanupStream(&mnode_);
    streamInitialized_ = false;
  }
  {
    std::lock_guard<std::mutex> lock(stbLookupMutex_);
    releaseStbLookup_ = true;
  }
  stbLookupCv_.notify_all();
  createThread.join();

  ASSERT_TRUE(lookupEntered);
  EXPECT_FALSE(stbLookupTimedOut_);
  EXPECT_EQ(TSDB_CODE_APP_IS_STOPPING, createCode.load());
  EXPECT_EQ(0, queueCalls_);
  EXPECT_EQ(0, responseCalls_);
  EXPECT_EQ(0, createTransCalls_);

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, CleanupTerminatesPendingPreflightOnce) {
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_NE(nullptr, pendingSend_);

  mndCleanupStream(&mnode_);
  streamInitialized_ = false;
  EXPECT_EQ(1, responseCalls_);
  EXPECT_EQ(0, queueCalls_);
  EXPECT_EQ(0, createTransCalls_);
  EXPECT_EQ(1, freeConnectionCalls_);
  destroySendMsgInfo(pendingSend_);
  pendingSend_ = nullptr;

  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, LateCallbackAfterCleanupCannotReplyOrEnqueueAgain) {
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_NE(nullptr, pendingSend_);
  SMsgSendInfo* send = pendingSend_;

  mndCleanupStream(&mnode_);
  streamInitialized_ = false;
  EXPECT_EQ(1, responseCalls_);
  SDataBuf response = {};
  callbackBytes_ = serializeMeta(makeNormalMeta());
  response = makeCallbackResponse();
  send->fp(send->param, &response, TSDB_CODE_SUCCESS);
  EXPECT_EQ(1, responseCalls_);
  EXPECT_EQ(0, queueCalls_);
  EXPECT_EQ(0, createTransCalls_);
  EXPECT_EQ(1, freeConnectionCalls_);

  destroySendMsgInfo(send);
  pendingSend_ = nullptr;
  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, CleanupWaitsForEnqueueingCallbackTerminalCleanup) {
  blockQueue_ = true;
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  ASSERT_NE(nullptr, pendingSend_);
  SMsgSendInfo* send = pendingSend_;

  callbackBytes_ = serializeMeta(makeNormalMeta());
  SDataBuf    response = makeCallbackResponse();
  std::thread callbackThread([&] { send->fp(send->param, &response, TSDB_CODE_SUCCESS); });

  bool queueEntered = false;
  {
    std::unique_lock<std::mutex> lock(queueMutex_);
    queueEntered = queueCv_.wait_for(lock, std::chrono::seconds(5), [&] { return queueEntered_; });
  }

  std::mutex              cleanupMutex;
  std::condition_variable cleanupCv;
  bool                    cleanupDone = false;
  std::thread             cleanupThread;
  if (queueEntered) {
    cleanupThread = std::thread([&] {
      mndCleanupStream(&mnode_);
      {
        std::lock_guard<std::mutex> lock(cleanupMutex);
        cleanupDone = true;
      }
      cleanupCv.notify_all();
    });
    std::unique_lock<std::mutex> lock(cleanupMutex);
    EXPECT_FALSE(cleanupCv.wait_for(lock, std::chrono::milliseconds(100), [&] { return cleanupDone; }));
  }

  {
    std::lock_guard<std::mutex> lock(queueMutex_);
    releaseQueue_ = true;
  }
  queueCv_.notify_all();
  callbackThread.join();
  if (cleanupThread.joinable()) cleanupThread.join();
  ASSERT_TRUE(queueEntered);
  streamInitialized_ = false;
  EXPECT_FALSE(queueTimedOut_);
  EXPECT_TRUE(cleanupDone);
  EXPECT_EQ(1, queueCalls_);
  EXPECT_EQ(0, responseCalls_);
  EXPECT_EQ(1, freeConnectionCalls_);

  destroySendMsgInfo(send);
  pendingSend_ = nullptr;
  freeRequestFixture(&req);
}

TEST_F(MndStreamCreateMetadataTest, CallbackBeforeSendReturnReleasesPublishedTransporter) {
  callbackBeforeSendReturn_ = true;
  callbackBytes_ = serializeMeta(makeNormalMeta());
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);

  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runCreate(&req));
  EXPECT_EQ(1, asyncSendCalls_);
  EXPECT_EQ(1, queueCalls_);
  EXPECT_EQ(1, freeConnectionCalls_);
  ASSERT_EQ(1U, freedTransporterIds_.size());
  EXPECT_EQ(transporterId_, freedTransporterIds_[0]);
  EXPECT_EQ(TSDB_CODE_ACTION_IN_PROGRESS, runQueuedContinuation());
  EXPECT_EQ(1, createTransCalls_);

  freeRequestFixture(&req);
}

}  // namespace

// clang-format off
/*
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
