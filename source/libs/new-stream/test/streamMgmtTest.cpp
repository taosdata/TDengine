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

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <vector>

#include "stub.h"

extern "C" {
#include "extConnector.h"
#include "streamInt.h"
#include "streamTriggerTask.h"
#include "taoserror.h"
#include "tencode.h"
#include "tglobal.h"

int32_t streamHbBuildRequestMsg(SStreamHbMsg *pMsg, bool *skipHb);
void    streamHbStart(void *param, void *tmrId);
int32_t stmAddMgmtReq(int64_t streamId, SArray **ppReq, int32_t idx);
int32_t smUndeployTask(SStreamTaskUndeploy *pUndeploy, bool rmFromVg);
int32_t stmHbAddStreamStatus(SStreamHbMsg *pMsg, SStreamInfo *pStream, int64_t streamId, bool reportPeriod,
                             int32_t *pollError = nullptr);
}

namespace {

constexpr int64_t kStreamId = 0x1234;
constexpr int64_t kTaskId = 0x5678;
constexpr int64_t kSeriousId = 7;
constexpr int32_t kNodeId = 3;

int32_t gDeployCalls = 0;
int32_t gUndeployCalls = 0;
int32_t gDeployFailureCode = TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED;
bool    gSecondDeploySucceeds = false;
bool    gIncludeDiagnostic = true;

Stub                *gOwnerInsertStub = nullptr;
SStreamTriggerTask **gPendingUndeploy = nullptr;

int32_t failStreamOwnerInsert(SHashObj *pHash, const void *key, size_t keyLen, const void *data, size_t size) {
  if (pHash == gStreamMgmt.stmGrp[STREAM_GID(kStreamId)]) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  gOwnerInsertStub->reset(taosHashPut);
  int32_t code = taosHashPut(pHash, key, keyLen, data, size);
  gOwnerInsertStub->set(taosHashPut, failStreamOwnerInsert);
  return code;
}

int32_t deployTriggerWithStatistics(SStreamTriggerTask *pTask, SStreamTriggerDeployMsg *) {
  ++gDeployCalls;
  pTask->task.status = STREAM_STATUS_INIT;
  pTask->task.flags |= STREAM_FLAG_REF_EXT_SOURCE;
  return streamTaskStatsInit(&pTask->task, &pTask->pStats);
}

int32_t deferTriggerUndeploy(SStreamTriggerTask **ppTask, bool) {
  gPendingUndeploy = ppTask;
  return TSDB_CODE_SUCCESS;
}

int32_t deployTriggerForLifecycleTest(SStreamTriggerTask *pTask, SStreamTriggerDeployMsg *) {
  ++gDeployCalls;
  if (gSecondDeploySucceeds && gDeployCalls > 1) {
    pTask->task.status = STREAM_STATUS_INIT;
    pTask->task.errorCode = TSDB_CODE_SUCCESS;
    return TSDB_CODE_SUCCESS;
  }

  pTask->task.status = STREAM_STATUS_FAILED;
  pTask->task.errorCode = gDeployFailureCode;
  if (gIncludeDiagnostic) {
    pTask->task.extraErrMsg = taosStrdup("failed to load timezone 'Missing/Timezone'");
  }
  return gDeployFailureCode;
}

int32_t undeployTriggerForLifecycleTest(SStreamTriggerTask **ppTask, bool) {
  ++gUndeployCalls;
  EXPECT_NE((*ppTask)->task.undeployCb, nullptr);
  if ((*ppTask)->task.undeployCb != nullptr) {
    (*ppTask)->task.undeployCb(ppTask);
  }
  return TSDB_CODE_SUCCESS;
}

class StreamMgmtLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gid_ = STREAM_GID(kStreamId);
    savedTaskMap_ = gStreamMgmt.taskMap;
    savedGroup_ = gStreamMgmt.stmGrp[gid_];
    savedSnodeEnabled_ = gStreamMgmt.snodeEnabled;
    gStreamMgmt.taskMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    ASSERT_NE(gStreamMgmt.taskMap, nullptr);
    gStreamMgmt.stmGrp[gid_] = nullptr;
    gStreamMgmt.snodeEnabled = true;

    gDeployCalls = 0;
    gUndeployCalls = 0;
    gDeployFailureCode = TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED;
    gSecondDeploySucceeds = false;
    gIncludeDiagnostic = true;
    gPendingUndeploy = nullptr;
    stub_.set(stTriggerTaskDeploy, deployTriggerForLifecycleTest);
    stub_.set(stTriggerTaskUndeploy, undeployTriggerForLifecycleTest);
  }

  void TearDown() override {
    gPendingUndeploy = nullptr;
    taosHashCleanup(gStreamMgmt.taskMap);
    taosHashCleanup(gStreamMgmt.stmGrp[gid_]);
    gStreamMgmt.taskMap = savedTaskMap_;
    gStreamMgmt.stmGrp[gid_] = savedGroup_;
    gStreamMgmt.snodeEnabled = savedSnodeEnabled_;
  }

  int32_t TryDeployTrigger(int64_t streamId = kStreamId) {
    SStmTaskDeploy trigger = {};
    trigger.task.type = STREAM_TRIGGER_TASK;
    trigger.task.streamId = streamId;
    trigger.task.taskId = kTaskId;
    trigger.task.seriousId = kSeriousId + gDeployCalls;
    trigger.task.nodeId = kNodeId;
    trigger.task.taskIdx = 0;

    SStmStreamDeploy deploy = {};
    deploy.streamId = streamId;
    deploy.triggerTask = &trigger;
    return smDeployTasks(&deploy);
  }

  void DeployTrigger() { ASSERT_EQ(TryDeployTrigger(), TSDB_CODE_SUCCESS); }

  SStreamInfo *AcquireStream() {
    return static_cast<SStreamInfo *>(taosHashAcquire(gStreamMgmt.stmGrp[gid_], &kStreamId, sizeof(kStreamId)));
  }

  void ReleaseStream(SStreamInfo *pStream) { taosHashRelease(gStreamMgmt.stmGrp[gid_], pStream); }

  SStreamTriggerTask *AcquireTrigger(void **ppAddr) {
    SStreamTask *pTask = nullptr;
    if (streamAcquireTask(kStreamId, kTaskId, &pTask, ppAddr) != TSDB_CODE_SUCCESS) {
      return nullptr;
    }
    return reinterpret_cast<SStreamTriggerTask *>(pTask);
  }

  SStreamHbMsg RoundTripHeartbeat(const SStreamHbMsg &input) {
    SEncoder sizeEncoder = {};
    tEncoderInit(&sizeEncoder, nullptr, 0);
    int32_t encoded = tEncodeStreamHbMsg(&sizeEncoder, &input);
    EXPECT_GT(encoded, 0);
    tEncoderClear(&sizeEncoder);

    std::vector<uint8_t> bytes(static_cast<size_t>(encoded));
    SEncoder             encoder = {};
    tEncoderInit(&encoder, bytes.data(), static_cast<uint32_t>(bytes.size()));
    EXPECT_EQ(tEncodeStreamHbMsg(&encoder, &input), encoded);
    tEncoderClear(&encoder);

    SStreamHbMsg output = {};
    SDecoder     decoder = {};
    tDecoderInit(&decoder, bytes.data(), static_cast<uint32_t>(bytes.size()));
    EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_SUCCESS);
    tDecoderClear(&decoder);
    return output;
  }

  int32_t   gid_ = 0;
  SHashObj *savedTaskMap_ = nullptr;
  SHashObj *savedGroup_ = nullptr;
  bool      savedSnodeEnabled_ = false;
  Stub      stub_;
};

TEST_F(StreamMgmtLifecycleTest, RetainsTimezoneLoadFailureThroughHeartbeatAndDestruction) {
  DeployTrigger();

  SStreamInfo *pStream = AcquireStream();
  ASSERT_NE(pStream, nullptr);
  ASSERT_NE(pStream->triggerList, nullptr);
  EXPECT_EQ(TD_DLIST_NELES(pStream->triggerList), 1);
  EXPECT_EQ(pStream->taskNum, 1);

  void               *taskAddr = nullptr;
  SStreamTriggerTask *pTrigger = AcquireTrigger(&taskAddr);
  ASSERT_NE(pTrigger, nullptr);
  EXPECT_EQ(pTrigger->task.status, STREAM_STATUS_FAILED);
  EXPECT_EQ(pTrigger->task.errorCode, TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED);
  ASSERT_NE(pTrigger->task.extraErrMsg, nullptr);
  EXPECT_STREQ(pTrigger->task.extraErrMsg, "failed to load timezone 'Missing/Timezone'");
  streamReleaseTask(taskAddr);

  SStreamHbMsg heartbeat = {};
  heartbeat.streamGId = gid_;
  ASSERT_EQ(stmHbAddStreamStatus(&heartbeat, pStream, kStreamId, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosArrayGetSize(heartbeat.pStreamStatus), 1);
  const auto *status = static_cast<const SStmTaskStatusMsg *>(taosArrayGet(heartbeat.pStreamStatus, 0));
  ASSERT_NE(status, nullptr);
  EXPECT_EQ(status->status, STREAM_STATUS_FAILED);
  EXPECT_EQ(status->errorCode, TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED);
  ASSERT_NE(status->extraErrMsg, nullptr);

  SStreamHbMsg decoded = RoundTripHeartbeat(heartbeat);
  ASSERT_EQ(taosArrayGetSize(decoded.pStreamStatus), 1);
  const auto *decodedStatus = static_cast<const SStmTaskStatusMsg *>(taosArrayGet(decoded.pStreamStatus, 0));
  ASSERT_NE(decodedStatus, nullptr);
  EXPECT_EQ(decodedStatus->status, STREAM_STATUS_FAILED);
  EXPECT_EQ(decodedStatus->errorCode, TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED);
  EXPECT_STREQ(decodedStatus->extraErrMsg, "failed to load timezone 'Missing/Timezone'");
  tCleanupStreamHbMsg(&decoded, true);
  tCleanupStreamHbMsg(&heartbeat, true);
  ReleaseStream(pStream);

  int64_t taskKey[2] = {kStreamId, kTaskId};
  ASSERT_EQ(taosHashRemove(gStreamMgmt.taskMap, taskKey, sizeof(taskKey)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosHashRemove(gStreamMgmt.stmGrp[gid_], &kStreamId, sizeof(kStreamId)), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosHashGetSize(gStreamMgmt.taskMap), 0);
  EXPECT_EQ(taosHashGetSize(gStreamMgmt.stmGrp[gid_]), 0);
}

TEST_F(StreamMgmtLifecycleTest, HeartbeatPreservesTimezoneErrorWithoutDiagnosticMessage) {
  gIncludeDiagnostic = false;
  DeployTrigger();

  SStreamInfo *pStream = AcquireStream();
  ASSERT_NE(pStream, nullptr);
  SStreamHbMsg heartbeat = {};
  heartbeat.streamGId = gid_;
  ASSERT_EQ(stmHbAddStreamStatus(&heartbeat, pStream, kStreamId, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosArrayGetSize(heartbeat.pStreamStatus), 1);
  const auto *status = static_cast<const SStmTaskStatusMsg *>(taosArrayGet(heartbeat.pStreamStatus, 0));
  ASSERT_NE(status, nullptr);
  EXPECT_EQ(status->status, STREAM_STATUS_FAILED);
  EXPECT_EQ(status->errorCode, TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED);
  EXPECT_EQ(status->extraErrMsg, nullptr);

  SStreamHbMsg decoded = RoundTripHeartbeat(heartbeat);
  ASSERT_EQ(taosArrayGetSize(decoded.pStreamStatus), 1);
  const auto *decodedStatus = static_cast<const SStmTaskStatusMsg *>(taosArrayGet(decoded.pStreamStatus, 0));
  ASSERT_NE(decodedStatus, nullptr);
  EXPECT_EQ(decodedStatus->errorCode, TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED);
  EXPECT_EQ(decodedStatus->extraErrMsg, nullptr);
  tCleanupStreamHbMsg(&decoded, true);
  tCleanupStreamHbMsg(&heartbeat, true);
  ReleaseStream(pStream);
}

TEST_F(StreamMgmtLifecycleTest, OtherDeployFailureUsesExistingRemovalPath) {
  gDeployFailureCode = TSDB_CODE_INTERNAL_ERROR;
  gIncludeDiagnostic = false;
  DeployTrigger();

  SStreamInfo *pStream = AcquireStream();
  ASSERT_NE(pStream, nullptr);
  ASSERT_NE(pStream->triggerList, nullptr);
  EXPECT_EQ(TD_DLIST_NELES(pStream->triggerList), 0);
  EXPECT_EQ(pStream->taskNum, 0);
  EXPECT_EQ(taosHashGetSize(gStreamMgmt.taskMap), 0);

  SStreamHbMsg heartbeat = {};
  heartbeat.streamGId = gid_;
  EXPECT_EQ(stmHbAddStreamStatus(&heartbeat, pStream, kStreamId, false), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosArrayGetSize(heartbeat.pStreamStatus), 0);
  tCleanupStreamHbMsg(&heartbeat, true);
  ReleaseStream(pStream);
}

TEST_F(StreamMgmtLifecycleTest, RedeployReplacesRetainedTimezoneFailure) {
  gSecondDeploySucceeds = true;
  DeployTrigger();

  void               *failedAddr = nullptr;
  SStreamTriggerTask *failedTask = AcquireTrigger(&failedAddr);
  ASSERT_NE(failedTask, nullptr);
  streamReleaseTask(failedAddr);

  DeployTrigger();

  SStreamInfo *pStream = AcquireStream();
  ASSERT_NE(pStream, nullptr);
  EXPECT_EQ(gUndeployCalls, 1);
  ASSERT_NE(pStream->triggerList, nullptr);
  EXPECT_EQ(TD_DLIST_NELES(pStream->triggerList), 2);
  EXPECT_EQ(pStream->taskNum, 2);
  ASSERT_NE(pStream->undeployTriggers, nullptr);
  EXPECT_EQ(taosArrayGetSize(pStream->undeployTriggers), 1);

  SStreamHbMsg heartbeat = {};
  heartbeat.streamGId = gid_;
  ASSERT_EQ(stmHbAddStreamStatus(&heartbeat, pStream, kStreamId, false), TSDB_CODE_SUCCESS);
  EXPECT_EQ(TD_DLIST_NELES(pStream->triggerList), 1);
  EXPECT_EQ(pStream->taskNum, 1);
  ASSERT_EQ(taosArrayGetSize(heartbeat.pStreamStatus), 1);
  const auto *status = static_cast<const SStmTaskStatusMsg *>(taosArrayGet(heartbeat.pStreamStatus, 0));
  ASSERT_NE(status, nullptr);
  EXPECT_EQ(status->status, STREAM_STATUS_INIT);
  EXPECT_EQ(status->errorCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(status->seriousId, kSeriousId + 1);

  void               *runningAddr = nullptr;
  SStreamTriggerTask *runningTask = AcquireTrigger(&runningAddr);
  ASSERT_NE(runningTask, nullptr);
  EXPECT_NE(runningTask, failedTask);
  EXPECT_EQ(runningTask, reinterpret_cast<SStreamTriggerTask *>(TD_DLIST_HEAD(pStream->triggerList)->data));
  EXPECT_EQ(taosHashGetSize(gStreamMgmt.taskMap), 1);
  streamReleaseTask(runningAddr);

  tCleanupStreamHbMsg(&heartbeat, true);
  ReleaseStream(pStream);
}

TEST_F(StreamMgmtLifecycleTest, FailedStreamRegistrationDoesNotPublishTasks) {
  stub_.set(stTriggerTaskDeploy, deployTriggerWithStatistics);
  {
    Stub hashStub;
    gOwnerInsertStub = &hashStub;
    hashStub.set(taosHashPut, failStreamOwnerInsert);
    EXPECT_EQ(TryDeployTrigger(), TSDB_CODE_OUT_OF_MEMORY);
  }
  gOwnerInsertStub = nullptr;

  EXPECT_EQ(gDeployCalls, 0);
  EXPECT_EQ(gUndeployCalls, 0);
  EXPECT_EQ(taosHashGetSize(gStreamMgmt.taskMap), 0);
  EXPECT_EQ(taosHashGetSize(gStreamMgmt.stmGrp[gid_]), 0);
}

TEST_F(StreamMgmtLifecycleTest, DisabledMnodeRepliesUndeployPeerTaskAfterErrorTimeout) {
  stub_.set(stTriggerTaskDeploy, deployTriggerWithStatistics);
  DeployTrigger();
  ASSERT_EQ(taosHashGetSize(gStreamMgmt.taskMap), 1);
  SStreamHbInfo savedHeartbeat = {};
  std::swap(savedHeartbeat, gStreamMgmt.hb);

  constexpr int64_t start = 1000;
  constexpr int32_t disabled = TSDB_CODE_MSG_NOT_PROCESSED;
  EXPECT_EQ(streamHbHandleRspErr(disabled, start), 0);
  EXPECT_EQ(streamHbHandleRspErr(disabled, start + STREAM_HB_ERR_HANDLE_MAX_DELAY - 1), 0);
  EXPECT_EQ(gUndeployCalls, 0);
  EXPECT_EQ(taosHashGetSize(gStreamMgmt.taskMap), 1);
  EXPECT_EQ(streamHbHandleRspErr(disabled, start + STREAM_HB_ERR_HANDLE_MAX_DELAY), 0);
  EXPECT_EQ(gUndeployCalls, 1);
  EXPECT_EQ(taosHashGetSize(gStreamMgmt.taskMap), 0);
  std::swap(savedHeartbeat, gStreamMgmt.hb);

  SStreamInfo *stream = AcquireStream();
  ASSERT_NE(stream, nullptr);
  SStreamHbMsg hb = {};
  hb.streamGId = gid_;
  EXPECT_EQ(stmHbAddStreamStatus(&hb, stream, kStreamId, false), 0);
  EXPECT_EQ(taosArrayGetSize(hb.pStreamStatus), 0);
  tCleanupStreamHbMsg(&hb, true);
  ReleaseStream(stream);
}

TEST_F(StreamMgmtLifecycleTest, DeferredUndeployRetainsTaskUntilCallbackCompletes) {
  stub_.set(stTriggerTaskDeploy, deployTriggerWithStatistics);
  stub_.set(stTriggerTaskUndeploy, deferTriggerUndeploy);
  DeployTrigger();

  void               *taskAddr = nullptr;
  SStreamTriggerTask *pTrigger = AcquireTrigger(&taskAddr);
  ASSERT_NE(pTrigger, nullptr);
  ASSERT_NE(pTrigger->pStats, nullptr);
  SStreamTaskUndeploy undeploy = {};
  undeploy.task = pTrigger->task;
  streamReleaseTask(taskAddr);
  ASSERT_EQ(smUndeployTask(&undeploy, false), TSDB_CODE_SUCCESS);
  ASSERT_NE(gPendingUndeploy, nullptr);

  SStreamInfo *pStream = AcquireStream();
  ASSERT_NE(pStream, nullptr);
  SStreamHbMsg heartbeat = {};
  heartbeat.streamGId = gid_;
  EXPECT_EQ(stmHbAddStreamStatus(&heartbeat, pStream, kStreamId, false), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pStream->taskNum, 1);
  EXPECT_EQ(TD_DLIST_NELES(pStream->triggerList), 1);
  EXPECT_NE((*gPendingUndeploy)->pStats, nullptr);
  tCleanupStreamHbMsg(&heartbeat, true);

  (*gPendingUndeploy)->task.undeployCb(gPendingUndeploy);
  gPendingUndeploy = nullptr;
  heartbeat = {};
  heartbeat.streamGId = gid_;
  EXPECT_EQ(stmHbAddStreamStatus(&heartbeat, pStream, kStreamId, false), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pStream->taskNum, 0);
  EXPECT_EQ(TD_DLIST_NELES(pStream->triggerList), 0);
  EXPECT_EQ(taosArrayGetSize(heartbeat.pStreamStatus), 0);
  tCleanupStreamHbMsg(&heartbeat, true);
  ReleaseStream(pStream);
  EXPECT_EQ(taosHashGetSize(gStreamMgmt.taskMap), 0);
  EXPECT_EQ(taosHashGetSize(gStreamMgmt.stmGrp[gid_]), 0);
}

class StreamReaderNodeTest : public StreamMgmtLifecycleTest {
 protected:
  void SetUp() override {
    ASSERT_NO_FATAL_FAILURE(StreamMgmtLifecycleTest::SetUp());
    savedVgroups_ = gStreamMgmt.vgroupMap;
    gStreamMgmt.vgroupMap = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
    ASSERT_NE(nullptr, gStreamMgmt.vgroupMap);
    taosHashSetFreeFp(
        gStreamMgmt.vgroupMap,
        +[](void *value) { taosArrayDestroy(static_cast<SStreamVgReaderTasks *>(value)->taskList); });
    savedHbError_ = gStreamMgmt.hb.lastErrCode;
  }

  void TearDown() override {
    taosHashCleanup(gStreamMgmt.vgroupMap);
    gStreamMgmt.vgroupMap = savedVgroups_;
    gStreamMgmt.hb.lastErrCode = savedHbError_;
    StreamMgmtLifecycleTest::TearDown();
  }

 private:
  SHashObj *savedVgroups_ = nullptr;
  int32_t   savedHbError_ = 0;
};

int32_t                  gConnectorOpens = 0;
int32_t                  gConnectorCloses = 0;
std::function<int32_t()> gOnFirstConnectorOpen;

class StreamRemoteReaderDeliveryTest : public StreamReaderNodeTest {
 protected:
  void SetUp() override {
    ASSERT_NO_FATAL_FAILURE(StreamReaderNodeTest::SetUp());
    std::swap(savedHeartbeat_, gStreamMgmt.hb);
    streamHbResetReaderRole(&gStreamMgmt.hb, true);
    gConnectorOpens = gConnectorCloses = 0;
    stub_.set(
        extConnectorOpen, +[](const SExtSourceCfg *, SExtConnectorHandle **handle) -> int32_t {
          if (atomic_add_fetch_32(&gConnectorOpens, 1) == 1 && gOnFirstConnectorOpen) {
            int32_t code = gOnFirstConnectorOpen();
            if (code) return code;
          }
          *handle = reinterpret_cast<SExtConnectorHandle *>(&gConnectorOpens);
          return TSDB_CODE_SUCCESS;
        });
    stub_.set(extConnectorClose, +[](SExtConnectorHandle *) { ++gConnectorCloses; });

    response_.streamGId = gid_;
    response_.deploy.streamList = taosArrayInit_s(sizeof(SStmStreamDeploy), 1);
    ASSERT_NE(nullptr, response_.deploy.streamList);
    auto *stream = static_cast<SStmStreamDeploy *>(taosArrayGet(response_.deploy.streamList, 0));
    stream->streamId = kStreamId;
    stream->readerTasks = taosArrayInit_s(sizeof(SStmTaskDeploy), 1);
    ASSERT_NE(nullptr, stream->readerTasks);
    reader_ = static_cast<SStmTaskDeploy *>(taosArrayGet(stream->readerTasks, 0));
    reader_->task.type = STREAM_READER_TASK;
    reader_->task.flags = STREAM_FLAG_TRIGGER_READER | STREAM_FLAG_REF_EXT_SOURCE | STREAM_FLAG_REMOTE_READER_DEPLOY;
    reader_->task.streamId = kStreamId;
    reader_->task.taskId = kTaskId;
    reader_->task.seriousId = kSeriousId;
    reader_->task.nodeId = kNodeId;
    auto &msg = reader_->msg.reader;
    msg.triggerReader = 1;
    msg.pExtSpec = static_cast<SStreamExtTriggerSpec *>(taosMemoryCalloc(1, sizeof(SStreamExtTriggerSpec)));
    ASSERT_NE(nullptr, msg.pExtSpec);
    msg.pExtSpec->sourceType = EXT_SOURCE_MYSQL;
    tstrncpy(msg.pExtSpec->sourceName, "remote", sizeof(msg.pExtSpec->sourceName));
    tstrncpy(msg.pExtSpec->extDb, "sensors", sizeof(msg.pExtSpec->extDb));
    tstrncpy(msg.pExtSpec->extTable, "meters", sizeof(msg.pExtSpec->extTable));
    tstrncpy(msg.pExtSpec->tsColumn, "ts", sizeof(msg.pExtSpec->tsColumn));
    msg.pExtSpec->triggerColumns = taosArrayInit_s(TSDB_COL_NAME_LEN, 1);
    ASSERT_NE(nullptr, msg.pExtSpec->triggerColumns);
    tstrncpy(static_cast<char *>(taosArrayGet(msg.pExtSpec->triggerColumns, 0)), "ts", TSDB_COL_NAME_LEN);
    msg.pExtSpec->pColMappings = static_cast<SExtColTypeMapping *>(taosMemoryCalloc(1, sizeof(SExtColTypeMapping)));
    ASSERT_NE(nullptr, msg.pExtSpec->pColMappings);
    msg.pExtSpec->numColMappings = 1;
    auto &mapping = msg.pExtSpec->pColMappings[0];
    tstrncpy(mapping.colName, "ts", sizeof(mapping.colName));
    mapping.tdType.type = TSDB_DATA_TYPE_TIMESTAMP;
    mapping.tdType.bytes = sizeof(int64_t);

    SNode *raw = nullptr;
    ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &raw));
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> plan(raw, nodesDestroyNode);
    auto                                               *scan = reinterpret_cast<SSubplan *>(raw);
    scan->subplanType = SUBPLAN_TYPE_SCAN;
    ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, reinterpret_cast<SNode **>(&scan->pNode)));
    ASSERT_EQ(0, nodesNodeToString(raw, false, reinterpret_cast<char **>(&msg.msg.trigger.triggerScanPlan), nullptr));
  }

  void TearDown() override {
    gOnFirstConnectorOpen = {};
    smUndeployAllTasks();
    tDeepFreeSMStreamHbRspMsg(&response_);
    streamHbResetReaderRole(&gStreamMgmt.hb, false);
    std::swap(savedHeartbeat_, gStreamMgmt.hb);
    StreamReaderNodeTest::TearDown();
  }

  int64_t Poll() {
    SStreamHbMsg hb = {};
    hb.dnodeId = kNodeId;
    EXPECT_EQ(0, streamHbIssueReaderPoll(&gStreamMgmt.hb, now_, &hb));
    EXPECT_NE(0, hb.readerDeployEpoch);
    return hb.readerDeployEpoch;
  }

  void EncodeResponse(std::vector<uint8_t> &packet) {
    int32_t len = 0, code = 0;
    tEncodeSize(tEncodeStreamHbRsp, &response_, len, code);
    ASSERT_EQ(0, code);
    packet.resize(len);
    SEncoder encoder = {};
    tEncoderInit(&encoder, packet.data(), len);
    code = tEncodeStreamHbRsp(&encoder, &response_);
    tEncoderClear(&encoder);
    ASSERT_EQ(0, code);
  }

  void Deliver(std::vector<uint8_t> &packet) {
    SDecoder         decoder = {};
    SMStreamHbRspMsg hb = {};
    tDecoderInit(&decoder, packet.data(), packet.size());
    int32_t code = tDecodeStreamHbRsp(&decoder, &hb);
    tDecoderClear(&decoder);
    ASSERT_EQ(0, code);
    ASSERT_EQ(0, streamHbProcessRspMsg(&hb));
  }

  void DeliverResponse() {
    std::vector<uint8_t> packet;
    ASSERT_NO_FATAL_FAILURE(EncodeResponse(packet));
    ASSERT_NO_FATAL_FAILURE(Deliver(packet));
  }

  void AddCleanup(SMStreamHbRspMsg &hb, const SStreamTask &task, bool pairedPatch) {
    if (!hb.undeploy.taskList) hb.undeploy.taskList = taosArrayInit(1, sizeof(SStreamTaskUndeploy));
    ASSERT_NE(nullptr, hb.undeploy.taskList);
    SStreamTaskUndeploy cleanup = {};
    cleanup.task = task;
    if (!pairedPatch) cleanup.task.flags &= ~STREAM_FLAG_REMOTE_READER_DEPLOY;
    cleanup.undeployMsg.doCleanup = true;
    ASSERT_NE(nullptr, taosArrayPush(hb.undeploy.taskList, &cleanup));
  }

  void Unbind(const SStreamTask &task) {
    SMStreamHbRspMsg unbind = {};
    unbind.streamGId = gid_;
    unbind.readerDeployEpoch = Poll();
    ASSERT_NO_FATAL_FAILURE(AddCleanup(unbind, task, false));
    ASSERT_EQ(0, streamHbProcessRspMsg(&unbind));
  }

  void CheckHeartbeat(const SStreamTask *expected) {
    SStreamHbMsg heartbeat = {};
    heartbeat.streamGId = gid_;
    EXPECT_EQ(0, stmBuildHbStreamsStatusReq(&heartbeat));
    EXPECT_EQ(expected ? 1u : 0u, taosArrayGetSize(heartbeat.pStreamStatus));
    if (expected && taosArrayGetSize(heartbeat.pStreamStatus) == 1) {
      auto *status = static_cast<SStmTaskStatusMsg *>(taosArrayGet(heartbeat.pStreamStatus, 0));
      EXPECT_EQ(expected->taskId, status->taskId);
      EXPECT_EQ(expected->seriousId, status->seriousId);
      EXPECT_EQ(STREAM_STATUS_INIT, status->status);
    }
    tCleanupStreamHbMsg(&heartbeat, true);
  }

  SMStreamHbRspMsg response_ = {};
  SStmTaskDeploy  *reader_ = nullptr;
  SStreamHbInfo    savedHeartbeat_ = {};
  int64_t          now_ = 0;
};

TEST_F(StreamRemoteReaderDeliveryTest, RoleChangeDuringStatusCollectionDiscardsTheSnapshot) {
  response_.readerDeployEpoch = Poll();
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  const int64_t previousRole = gStreamMgmt.hb.readerRuntime.id;
  static Stub  *collector;
  collector = &stub_;
  stub_.set(
      stmBuildHbStreamsStatusReq, +[](SStreamHbMsg *msg) {
        collector->reset(stmBuildHbStreamsStatusReq);
        int32_t code = stmBuildHbStreamsStatusReq(msg);
        streamHbRenewReaderRuntime(&gStreamMgmt.hb);
        return code;
      });
  auto savedGetDnode = gStreamMgmt.getDnode;
  auto savedGroup = gStreamMgmt.stmGrpIdx;
  auto savedReported = gStreamMgmt.hbReported;
  gStreamMgmt.getDnode = +[](void *) { return kNodeId; };
  gStreamMgmt.stmGrpIdx = (gid_ + STREAM_MAX_GROUP_NUM - 1) % STREAM_MAX_GROUP_NUM;
  SStreamHbMsg packet = {};
  bool         skip = false;
  EXPECT_EQ(0, streamHbBuildRequestMsg(&packet, &skip));
  EXPECT_TRUE(skip);
  EXPECT_EQ(1u, taosArrayGetSize(packet.pStreamStatus));
  EXPECT_EQ(previousRole, packet.readerRuntime.id);
  EXPECT_NE(gStreamMgmt.hb.readerRuntime.id, packet.readerRuntime.id);
  EXPECT_EQ(0, gStreamMgmt.hb.readerPollSerial);
  tCleanupStreamHbMsg(&packet, true);
  collector = nullptr;
  gStreamMgmt.getDnode = savedGetDnode;
  gStreamMgmt.stmGrpIdx = savedGroup;
  gStreamMgmt.hbReported = savedReported;
}

TEST_F(StreamRemoteReaderDeliveryTest, IdleGroupCycleRegistersOnlyTheTransmittedPoll) {
  auto savedGetDnode = gStreamMgmt.getDnode;
  auto savedGetMnode = gStreamMgmt.getMnode;
  auto savedGroup = gStreamMgmt.stmGrpIdx;
  auto savedReported = gStreamMgmt.hbReported;
  gStreamMgmt.getDnode = +[](void *) { return kNodeId; };
  gStreamMgmt.getMnode = +[](void *, SEpSet *) {};
  gStreamMgmt.stmGrpIdx = 0;
  gStreamMgmt.hbReported = false;
  stub_.set(streamTmrStart, +[](TAOS_TMR_CALLBACK, int32_t, void *, void *, tmr_h *, const char *) {});
  static int sent;
  sent = 0;
  stub_.set(
      tmsgSendReq, +[](const SEpSet *, SRpcMsg *rpc) {
        ++sent;
        SStreamHbMsg msg = {};
        SDecoder     decoder = {};
        const auto   headerSize = sizeof(SStreamMsgGrpHeader);
        tDecoderInit(&decoder, static_cast<uint8_t *>(rpc->pCont) + headerSize, rpc->contLen - headerSize);
        int32_t code = tDecodeStreamHbMsg(&decoder, &msg);
        EXPECT_EQ(0, code);
        EXPECT_EQ(0, msg.streamGId);
        EXPECT_EQ(kNodeId, msg.snodeId);
        EXPECT_EQ(gStreamMgmt.hb.readerRuntime.id, msg.readerRuntime.id);
        EXPECT_NE(0, msg.readerDeployEpoch);
        tCleanupStreamHbMsg(&msg, true);
        tDecoderClear(&decoder);
        rpcFreeCont(rpc->pCont);
        return code;
      });
  for (int gid = 0; gid < STREAM_MAX_GROUP_NUM; ++gid) streamHbStart(nullptr, nullptr);
  EXPECT_EQ(1, sent);
  auto     &hb = gStreamMgmt.hb;
  const int tokens =
      tSimpleHashGetSize(hb.readerPollWindows[0].polls) + tSimpleHashGetSize(hb.readerPollWindows[1].polls);
  EXPECT_EQ(sent, tokens);
  RecordProperty("idle_cycle_sent", sent);
  RecordProperty("idle_cycle_registered_polls", tokens);
  gStreamMgmt.getDnode = savedGetDnode;
  gStreamMgmt.getMnode = savedGetMnode;
  gStreamMgmt.stmGrpIdx = savedGroup;
  gStreamMgmt.hbReported = savedReported;
}

class StreamGroupHeartbeatTest : public StreamMgmtLifecycleTest {
 protected:
  void SetUp() override {
    ASSERT_NO_FATAL_FAILURE(StreamMgmtLifecycleTest::SetUp());
    std::swap(savedHeartbeat_, gStreamMgmt.hb);
    streamHbResetReaderRole(&gStreamMgmt.hb, true);
    gStreamMgmt.getDnode = +[](void *) { return kNodeId; };
    gStreamMgmt.getMnode = +[](void *, SEpSet *) {};
    gStreamMgmt.hbReported = false;
    stub_.set(streamTmrStart, +[](TAOS_TMR_CALLBACK, int32_t, void *, void *, tmr_h *, const char *) {});
    static StreamGroupHeartbeatTest *test;
    test = this;
    stub_.set(
        tmsgSendReq, +[](const SEpSet *, SRpcMsg *rpc) {
          ++test->sent_;
          auto &packet = test->packet_;
          tCleanupStreamHbMsg(&packet, true);
          packet = {};
          SDecoder   decoder = {};
          const auto headerSize = sizeof(SStreamMsgGrpHeader);
          tDecoderInit(&decoder, static_cast<uint8_t *>(rpc->pCont) + headerSize, rpc->contLen - headerSize);
          int32_t code = tDecodeStreamHbMsg(&decoder, &packet);
          EXPECT_EQ(0, code);
          tDecoderClear(&decoder);
          rpcFreeCont(rpc->pCont);
          return code;
        });
  }

  void TearDown() override {
    tCleanupStreamHbMsg(&packet_, true);
    streamHbResetReaderRole(&gStreamMgmt.hb, false);
    std::swap(savedHeartbeat_, gStreamMgmt.hb);
    gStreamMgmt.getDnode = savedGetDnode_;
    gStreamMgmt.getMnode = savedGetMnode_;
    gStreamMgmt.stmGrpIdx = savedGroupIdx_;
    gStreamMgmt.hbReported = savedReported_;
    StreamMgmtLifecycleTest::TearDown();
  }

  void DeployPeers() {
    stub_.set(
        stTriggerTaskDeploy, +[](SStreamTriggerTask *task, SStreamTriggerDeployMsg *) {
          task->task.status = STREAM_STATUS_RUNNING;
          return 0;
        });
    ASSERT_EQ(0, TryDeployTrigger(kStreamId));
    ASSERT_EQ(0, TryDeployTrigger(kStreamId + STREAM_MAX_GROUP_NUM));
    auto *group = gStreamMgmt.stmGrp[gid_];
    void *iter = nullptr;
    while ((iter = taosHashIterate(group, iter))) {
      auto *stream = static_cast<SStreamInfo *>(iter);
      peers_.push_back(reinterpret_cast<SStreamTriggerTask *>(TD_DLIST_HEAD(stream->triggerList)->data));
    }
    ASSERT_EQ(2u, peers_.size());
  }

  void Tick() {
    gStreamMgmt.stmGrpIdx = (gid_ + STREAM_MAX_GROUP_NUM - 1) % STREAM_MAX_GROUP_NUM;
    streamHbStart(nullptr, nullptr);
  }

  void CheckSnapshotFailure(int failedIndex) {
    ASSERT_NO_FATAL_FAILURE(DeployPeers());
    auto &tracker = peers_[failedIndex]->pRecalcTracker;
    ASSERT_EQ(0, stRecalcTrackerCreate(&tracker));
    static Stub *copyStub;
    copyStub = &stub_;
    static bool failed;
    failed = false;
    stub_.set(
        stRecalcTrackerCopySnapshot,
        +[](SStreamRecalcTracker *tracker, bool *history, int32_t *progress, SArray **snapshots) {
          copyStub->reset(stRecalcTrackerCopySnapshot);
          Stub allocation;
          allocation.set(
              taosArrayInit, +[](size_t, size_t elementSize) -> SArray * {
                EXPECT_EQ(sizeof(SStreamRecalcSnapshot), elementSize);
                failed = true;
                terrno = TSDB_CODE_OUT_OF_MEMORY;
                return nullptr;
              });
          return stRecalcTrackerCopySnapshot(tracker, history, progress, snapshots);
        });

    // Group zero collects the periodic snapshot on alternate heartbeats.
    for (int i = 0; i < 2; ++i) {
      const int  previousSent = sent_;
      const bool alreadyFailed = failed;
      Tick();
      EXPECT_EQ(previousSent + 1, sent_);
      if (!alreadyFailed && failed && sent_ > previousSent) {
        CheckHealthyPeer(peers_[1 - failedIndex]);
        EXPECT_EQ(1u, taosArrayGetSize(packet_.pTriggerStatus));
      }
    }
    EXPECT_TRUE(failed);
    stRecalcTrackerDestroy(&tracker);
  }

  void CheckHealthyPeer(const SStreamTriggerTask *peer) {
    ASSERT_EQ(1u, taosArrayGetSize(packet_.pStreamStatus));
    const auto *status = static_cast<SStmTaskStatusMsg *>(taosArrayGet(packet_.pStreamStatus, 0));
    EXPECT_EQ(peer->task.streamId, status->streamId);
    EXPECT_EQ(peer->task.taskId, status->taskId);
    EXPECT_EQ(STREAM_STATUS_RUNNING, status->status);
    ASSERT_EQ(1u, taosArrayGetSize(packet_.pTaskMetrics));
    const auto *metrics = static_cast<SStreamTaskMetricsEntry *>(taosArrayGet(packet_.pTaskMetrics, 0));
    EXPECT_EQ(0, metrics->taskStatusIndex);
    EXPECT_EQ(peer->task.streamId, metrics->streamId);
    EXPECT_EQ(gStreamMgmt.hb.readerRuntime.id, packet_.readerRuntime.id);
    EXPECT_NE(0, packet_.readerDeployEpoch);
  }

  decltype(gStreamMgmt.getDnode)    savedGetDnode_ = gStreamMgmt.getDnode;
  decltype(gStreamMgmt.getMnode)    savedGetMnode_ = gStreamMgmt.getMnode;
  int8_t                            savedGroupIdx_ = gStreamMgmt.stmGrpIdx;
  bool                              savedReported_ = gStreamMgmt.hbReported;
  SStreamHbInfo                     savedHeartbeat_ = {};
  SStreamHbMsg                      packet_ = {};
  int                               sent_ = 0;
  std::vector<SStreamTriggerTask *> peers_;
};

TEST_F(StreamGroupHeartbeatTest, FirstSnapshotFailureStillSendsHealthyPeer) { CheckSnapshotFailure(0); }

TEST_F(StreamGroupHeartbeatTest, LastSnapshotFailureStillSendsHealthyPeer) { CheckSnapshotFailure(1); }

TEST_F(StreamGroupHeartbeatTest, PollRegistrationFailureAbortsGroupBeforeObservations) {
  ASSERT_NO_FATAL_FAILURE(DeployPeers());
  static Stub *pollStub;
  pollStub = &stub_;
  stub_.set(
      tSimpleHashPut, +[](SSHashObj *, const void *, size_t, const void *, size_t) {
        pollStub->reset(tSimpleHashPut);
        return TSDB_CODE_OUT_OF_MEMORY;
      });
  Tick();
  EXPECT_EQ(0, sent_);
  EXPECT_EQ(0, gStreamMgmt.hb.readerPollSerial);
  Tick();
  EXPECT_EQ(1, sent_);
  EXPECT_EQ(2u, taosArrayGetSize(packet_.pStreamStatus));
  EXPECT_NE(0, packet_.readerDeployEpoch);
}

TEST_F(StreamGroupHeartbeatTest, IncompleteRequestNeverReportsTriggerAsAcknowledged) {
  ASSERT_NO_FATAL_FAILURE(DeployPeers());
  SStreamMgmtReq pending = {};
  pending.reqId = 71;
  pending.type = STREAM_MGMT_REQ_TRIGGER_MIXED_ORIGTBL_READER;
  auto &trigger = peers_[0]->task;
  trigger.pMgmtReq = &pending;
  for (bool failIndex : {false, true}) {
    SCOPED_TRACE(failIndex);
    Stub failure;
    if (failIndex) {
      failure.set(stmAddMgmtReq, +[](int64_t, SArray **, int32_t) { return TSDB_CODE_OUT_OF_MEMORY; });
    } else {
      failure.set(tCloneSStreamMgmtReq, +[](SStreamMgmtReq *, SStreamMgmtReq **) { return TSDB_CODE_OUT_OF_MEMORY; });
    }
    const int previousSent = sent_;
    Tick();
    EXPECT_EQ(previousSent + 1, sent_);
    CheckHealthyPeer(peers_[1]);
    EXPECT_EQ(0u, taosArrayGetSize(packet_.pStreamReq));
    if (taosArrayGetSize(packet_.pStreamStatus) == 1) {
      auto *status = static_cast<SStmTaskStatusMsg *>(taosArrayGet(packet_.pStreamStatus, 0));
      EXPECT_EQ(status->detailStatus < 0 ? 0u : 1u, taosArrayGetSize(packet_.pTriggerStatus));
      EXPECT_LE(status->detailStatus, 0);
    }
  }
  Tick();
  EXPECT_EQ(2u, taosArrayGetSize(packet_.pStreamStatus));
  EXPECT_EQ(1u, taosArrayGetSize(packet_.pStreamReq));
  trigger.pMgmtReq = nullptr;
}

TEST_F(StreamRemoteReaderDeliveryTest, DelayedCreateAfterLastUnbindDoesNotReopenReader) {
  response_.readerDeployEpoch = Poll();
  std::vector<uint8_t> held, retry;
  ASSERT_NO_FATAL_FAILURE(EncodeResponse(held));
  response_.readerDeployEpoch = Poll();
  ASSERT_NO_FATAL_FAILURE(EncodeResponse(retry));
  ASSERT_NO_FATAL_FAILURE(Deliver(retry));
  ASSERT_EQ(1, taosHashGetSize(gStreamMgmt.taskMap));
  ASSERT_EQ(1, gConnectorOpens);
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(&reader_->task));

  // Unbinding the last column on the remote Trigger sends this standalone cleanup to the Reader.
  ASSERT_NO_FATAL_FAILURE(Unbind(reader_->task));
  ASSERT_EQ(0, taosHashGetSize(gStreamMgmt.taskMap));
  ASSERT_EQ(1, gConnectorCloses);
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(nullptr));

  ASSERT_NO_FATAL_FAILURE(Deliver(held));
  EXPECT_EQ(0, taosHashGetSize(gStreamMgmt.taskMap));
  EXPECT_EQ(1, gConnectorOpens);

  // After the next poll, the mnode can intentionally deploy a new incarnation.
  ++reader_->task.seriousId;
  response_.readerDeployEpoch = Poll();
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  EXPECT_EQ(1, taosHashGetSize(gStreamMgmt.taskMap));
  EXPECT_EQ(2, gConnectorOpens);
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(&reader_->task));
}

TEST_F(StreamRemoteReaderDeliveryTest, DelayedPatchSurvivesOtherStreamsUnbindingAndAppliesLegacyCleanup) {
  const auto original = reader_->task;
  response_.readerDeployEpoch = Poll();
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());

  // An older mnode sends an ordinary Reader deployment without an epoch or marker.
  ++reader_->task.taskId;
  reader_->task.flags &= ~STREAM_FLAG_REMOTE_READER_DEPLOY;
  const auto legacy = reader_->task;
  response_.readerDeployEpoch = 0;
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());

  reader_->task = original;
  ++reader_->task.seriousId;
  const auto replacement = reader_->task;
  response_.readerDeployEpoch = Poll();
  ASSERT_NO_FATAL_FAILURE(AddCleanup(response_, original, true));
  ASSERT_NO_FATAL_FAILURE(AddCleanup(response_, legacy, false));
  std::vector<uint8_t> held;
  ASSERT_NO_FATAL_FAILURE(EncodeResponse(held));

  auto *stream = static_cast<SStmStreamDeploy *>(taosArrayGet(response_.deploy.streamList, 0));
  taosArrayClear(response_.undeploy.taskList);
  // B deliberately shares A's task and instance numbers. Its repeated unbinds straddle a poll-window boundary.
  stream->streamId = reader_->task.streamId = kStreamId + STREAM_MAX_GROUP_NUM;
  for (int i = 0; i < 4; ++i) {
    if (i == 2) now_ += STREAM_HB_ERR_HANDLE_MAX_DELAY;
    response_.readerDeployEpoch = Poll();
    ASSERT_NO_FATAL_FAILURE(DeliverResponse());
    ASSERT_EQ(3, taosHashGetSize(gStreamMgmt.taskMap));
    ASSERT_NO_FATAL_FAILURE(Unbind(reader_->task));
    ASSERT_EQ(2, taosHashGetSize(gStreamMgmt.taskMap));
  }

  ASSERT_NO_FATAL_FAILURE(Deliver(held));
  EXPECT_EQ(1, taosHashGetSize(gStreamMgmt.taskMap));
  EXPECT_EQ(7, gConnectorOpens);
  EXPECT_EQ(6, gConnectorCloses);
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(&replacement));

  ASSERT_NO_FATAL_FAILURE(Unbind(replacement));
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(nullptr));
  ASSERT_NO_FATAL_FAILURE(Deliver(held));
  EXPECT_EQ(0, taosHashGetSize(gStreamMgmt.taskMap));
  EXPECT_EQ(7, gConnectorOpens);
}

TEST_F(StreamRemoteReaderDeliveryTest, FinalUnbindBeforeFirstInstallRejectsInFlightCreate) {
  response_.readerDeployEpoch = Poll();
  std::vector<uint8_t> held;
  ASSERT_NO_FATAL_FAILURE(EncodeResponse(held));
  ASSERT_NO_FATAL_FAILURE(Unbind(reader_->task));
  ASSERT_NO_FATAL_FAILURE(Deliver(held));
  EXPECT_EQ(0, gConnectorOpens);
  EXPECT_EQ(0, taosHashGetSize(gStreamMgmt.taskMap));

  ++reader_->task.seriousId;
  response_.readerDeployEpoch = Poll();
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(&reader_->task));
}

TEST_F(StreamRemoteReaderDeliveryTest, ExpiredPollCannotResurrectReaderAfterRetirementRecordIsReclaimed) {
  response_.readerDeployEpoch = Poll();
  std::vector<uint8_t> held;
  ASSERT_NO_FATAL_FAILURE(EncodeResponse(held));
  ASSERT_NO_FATAL_FAILURE(Deliver(held));
  ASSERT_NO_FATAL_FAILURE(Unbind(reader_->task));
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(nullptr));
  ASSERT_EQ(1, tSimpleHashGetSize(gStreamMgmt.hb.readerPollWindows[0].retired));

  now_ += 2 * STREAM_HB_ERR_HANDLE_MAX_DELAY;
  response_.readerDeployEpoch = Poll();
  EXPECT_EQ(0, tSimpleHashGetSize(gStreamMgmt.hb.readerPollWindows[0].retired));
  EXPECT_EQ(0, tSimpleHashGetSize(gStreamMgmt.hb.readerPollWindows[1].retired));
  ASSERT_NO_FATAL_FAILURE(Deliver(held));
  EXPECT_EQ(1, gConnectorOpens);
  EXPECT_EQ(0, taosHashGetSize(gStreamMgmt.taskMap));

  ++reader_->task.seriousId;
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(&reader_->task));
}

TEST_F(StreamRemoteReaderDeliveryTest, DuplicateDoesNotBlockOtherStreamWhileConnectorOpens) {
  response_.readerDeployEpoch = Poll();
  std::vector<uint8_t> initial, retry;
  ASSERT_NO_FATAL_FAILURE(EncodeResponse(initial));

  // Another heartbeat group redelivers the Reader along with a different stream's Trigger.
  response_.streamGId = (gid_ + 1) % STREAM_MAX_GROUP_NUM;
  response_.readerDeployEpoch = Poll();
  auto *peer = static_cast<SStmStreamDeploy *>(taosArrayReserve(response_.deploy.streamList, 1));
  ASSERT_NE(nullptr, peer);
  peer->streamId = kStreamId + STREAM_MAX_GROUP_NUM;
  peer->triggerTask = static_cast<SStmTaskDeploy *>(taosMemoryCalloc(1, sizeof(SStmTaskDeploy)));
  ASSERT_NE(nullptr, peer->triggerTask);
  peer->triggerTask->task.type = STREAM_TRIGGER_TASK;
  peer->triggerTask->task.streamId = peer->streamId;
  peer->triggerTask->task.taskId = kTaskId;
  peer->triggerTask->task.seriousId = kSeriousId;
  peer->triggerTask->task.nodeId = kNodeId;
  peer->triggerTask->msg.trigger.triggerType = WINDOW_TYPE_PERIOD;
  peer->triggerTask->msg.trigger.streamName = taosStrdup("other_stream");
  ASSERT_NE(nullptr, peer->triggerTask->msg.trigger.streamName);
  ASSERT_NO_FATAL_FAILURE(EncodeResponse(retry));
  stub_.set(stTriggerTaskDeploy, deployTriggerWithStatistics);

  std::mutex              mutex;
  std::condition_variable condition;
  bool                    opening = false, release = false;
  gOnFirstConnectorOpen = [&] {
    std::unique_lock<std::mutex> lock(mutex);
    opening = true;
    condition.notify_all();
    condition.wait(lock, [&] { return release; });
    return TSDB_CODE_SUCCESS;
  };
  auto firstWorker = std::async(std::launch::async, [&] { Deliver(initial); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(condition.wait_for(lock, std::chrono::seconds(5), [&] { return opening; }));
  }
  auto       secondWorker = std::async(std::launch::async, [&] { Deliver(retry); });
  const auto status = secondWorker.wait_for(std::chrono::seconds(5));
  if (status == std::future_status::ready) {
    EXPECT_EQ(1, gDeployCalls);
    EXPECT_EQ(1, atomic_load_32(&gConnectorOpens));
    EXPECT_EQ(1, taosHashGetSize(gStreamMgmt.taskMap));  // Only the peer is ready for use.
  }
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
  }
  condition.notify_all();
  firstWorker.get();
  secondWorker.get();
  EXPECT_EQ(std::future_status::ready, status);
  EXPECT_EQ(1, gConnectorOpens);
  EXPECT_EQ(2, taosHashGetSize(gStreamMgmt.taskMap));
}

TEST_F(StreamRemoteReaderDeliveryTest, CancelledInstallPreservesHealthyTaskAndHeartbeat) {
  stub_.set(stTriggerTaskDeploy, deployTriggerWithStatistics);
  DeployTrigger();
  ++reader_->task.taskId;
  response_.readerDeployEpoch = Poll();
  gOnFirstConnectorOpen = [&] {
    EXPECT_EQ(1, taosHashGetSize(gStreamMgmt.taskMap));
    auto *owner = AcquireStream();
    EXPECT_NE(nullptr, owner);
    if (!owner) return TSDB_CODE_FAILED;
    EXPECT_EQ(1, owner->taskNum);
    ReleaseStream(owner);
    SStreamTaskUndeploy cancel = {};
    cancel.task = reader_->task;
    cancel.task.flags &= ~STREAM_FLAG_REMOTE_READER_DEPLOY;
    cancel.undeployMsg.doCleanup = true;
    return smUndeployTask(&cancel, true);
  };
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  EXPECT_EQ(1, gConnectorOpens);
  EXPECT_EQ(1, gConnectorCloses);
  EXPECT_EQ(1, taosHashGetSize(gStreamMgmt.taskMap));
  auto *owner = AcquireStream();
  ASSERT_NE(nullptr, owner);
  EXPECT_EQ(1, owner->taskNum);
  EXPECT_EQ(0, TD_DLIST_NELES(owner->readerList));
  ReleaseStream(owner);
  SStreamTask healthy = {};
  healthy.taskId = kTaskId;
  healthy.seriousId = kSeriousId;
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(&healthy));
}

TEST_F(StreamRemoteReaderDeliveryTest, HeartbeatTimeoutExpiresPendingReaderInstallsAndAllowsFreshDeployment) {
  const int64_t previousRuntime = gStreamMgmt.hb.readerRuntime.id;
  const int64_t pendingPoll = Poll();
  response_.readerDeployEpoch = Poll();
  gOnFirstConnectorOpen = [&] {
    EXPECT_EQ(1, tSimpleHashGetSize(gStreamMgmt.hb.readerInstalls));
    EXPECT_EQ(0, streamHbHandleRspErr(TSDB_CODE_RPC_TIMEOUT, now_));
    EXPECT_EQ(0, streamHbHandleRspErr(TSDB_CODE_RPC_TIMEOUT, now_ + STREAM_HB_ERR_HANDLE_MAX_DELAY));
    EXPECT_EQ(nullptr, gStreamMgmt.hb.readerInstalls);
    return TSDB_CODE_SUCCESS;
  };
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  EXPECT_EQ(1, gConnectorOpens);
  EXPECT_EQ(1, gConnectorCloses);
  ASSERT_EQ(0, taosHashGetSize(gStreamMgmt.taskMap));

  response_.readerDeployEpoch = pendingPoll;
  response_.readerRuntime = {previousRuntime, 79};
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  EXPECT_EQ(1, gConnectorOpens);
  EXPECT_EQ(0, taosHashGetSize(gStreamMgmt.taskMap));

  now_ += STREAM_HB_ERR_HANDLE_MAX_DELAY;
  SStreamHbMsg recovered = {};
  recovered.dnodeId = kNodeId;
  ASSERT_EQ(0, streamHbIssueReaderPoll(&gStreamMgmt.hb, now_, &recovered));
  EXPECT_EQ(kNodeId, recovered.snodeId);
  EXPECT_EQ(0, recovered.readerRuntime.challenge);
  EXPECT_NE(previousRuntime, recovered.readerRuntime.id);
  ASSERT_NE(0, recovered.readerDeployEpoch);
  response_.readerRuntime = recovered.readerRuntime;
  response_.readerDeployEpoch = recovered.readerDeployEpoch;
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  EXPECT_EQ(2, gConnectorOpens);
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(&reader_->task));
}

TEST_F(StreamRemoteReaderDeliveryTest, RoleChangeRenewsPollsAndConfirmation) {
  const auto old = gStreamMgmt.hb.readerRuntime;
  const auto epoch = Poll();
  ASSERT_NE(0, old.id);
  ASSERT_NE(nullptr, tSimpleHashGet(gStreamMgmt.hb.readerPollWindows[0].polls, &epoch, sizeof(epoch)));

  streamSetSnodeDisabled(false);
  SStreamHbMsg hb = {};
  hb.dnodeId = kNodeId;
  ASSERT_EQ(0, streamHbIssueReaderPoll(&gStreamMgmt.hb, now_, &hb));
  EXPECT_EQ(0, hb.snodeId);
  EXPECT_EQ(0, hb.readerDeployEpoch);
  EXPECT_EQ(0, hb.readerRuntime.id);
  EXPECT_EQ(nullptr, gStreamMgmt.hb.readerPollWindows[0].polls);

  auto getDnode = gStreamMgmt.getDnode;
  gStreamMgmt.getDnode = +[](void *) { return kNodeId; };
  SMsgCb callbacks = gStreamMgmt.msgCb;
  streamSetSnodeEnabled(&callbacks);
  gStreamMgmt.getDnode = getDnode;
  SMStreamHbRspMsg previous = {};
  previous.readerRuntime = {old.id, 79};
  ASSERT_EQ(0, streamHbProcessRspMsg(&previous));
  ASSERT_EQ(0, streamHbIssueReaderPoll(&gStreamMgmt.hb, now_, &hb));
  EXPECT_EQ(kNodeId, hb.snodeId);
  EXPECT_NE(old.id, hb.readerRuntime.id);
  EXPECT_NE(0, hb.readerRuntime.id);
  EXPECT_EQ(0, hb.readerRuntime.challenge);
  EXPECT_NE(epoch, hb.readerDeployEpoch);
  EXPECT_EQ(nullptr, tSimpleHashGet(gStreamMgmt.hb.readerPollWindows[0].polls, &epoch, sizeof(epoch)));
}

TEST_F(StreamRemoteReaderDeliveryTest, FinishingOldInstallPreservesNextRolesClaim) {
  stub_.set(
      stReaderTaskDeploy, +[](SStreamReaderTask *reader, const SStreamReaderDeployMsg *) -> int32_t {
        auto         &hb = gStreamMgmt.hb;
        const int64_t key[] = {reader->task.streamId, reader->task.taskId, reader->task.seriousId};
        EXPECT_NE(nullptr, tSimpleHashGet(hb.readerInstalls, key, sizeof(key)));
        streamHbResetReaderRole(&hb, false);
        EXPECT_EQ(nullptr, hb.readerInstalls);
        streamHbResetReaderRole(&hb, true);
        SStreamHbMsg poll = {};
        poll.dnodeId = kNodeId;
        TAOS_CHECK_RETURN(streamHbIssueReaderPoll(&hb, 0, &poll));
        // A worker from the new role owns the same key while the old worker finishes.
        hb.readerInstalls = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
        if (!hb.readerInstalls) return terrno;
        return tSimpleHashPut(hb.readerInstalls, key, sizeof(key), &poll.readerDeployEpoch,
                              sizeof(poll.readerDeployEpoch));
      });
  auto *deploy = static_cast<SStmStreamDeploy *>(taosArrayGet(response_.deploy.streamList, 0));
  deploy->readerDeployEpoch = Poll();
  ASSERT_EQ(0, smDeployTasks(deploy));
  EXPECT_EQ(0, taosHashGetSize(gStreamMgmt.taskMap));
  EXPECT_EQ(0, taosHashGetSize(gStreamMgmt.stmGrp[gid_]));
  EXPECT_EQ(1, tSimpleHashGetSize(gStreamMgmt.hb.readerInstalls));
}

TEST_F(StreamRemoteReaderDeliveryTest, OldReaderReferenceOutlivesReinstallationWithSameIds) {
  stub_.set(
      stReaderTaskDeploy, +[](SStreamReaderTask *reader, const SStreamReaderDeployMsg *) -> int32_t {
        reader->task.status = STREAM_STATUS_INIT;
        return streamTaskStatsInit(&reader->task, &reader->pStats);
      });
  auto *deploy = static_cast<SStmStreamDeploy *>(taosArrayGet(response_.deploy.streamList, 0));
  deploy->readerDeployEpoch = Poll();
  ASSERT_EQ(0, smDeployTasks(deploy));
  SStreamTask *old = nullptr;
  void        *ref = nullptr;
  ASSERT_EQ(0, streamAcquireTask(kStreamId, kTaskId, &old, &ref));
  std::unique_ptr<void, decltype(&streamReleaseTask)> pin(ref, streamReleaseTask);

  streamSetSnodeDisabled(false);
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(nullptr));
  streamHbResetReaderRole(&gStreamMgmt.hb, true);
  deploy->readerDeployEpoch = Poll();
  ASSERT_EQ(0, smDeployTasks(deploy));
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(&reader_->task));

  SStreamTaskUndeploy remove = {};
  remove.task = reader_->task;
  remove.task.flags &= ~STREAM_FLAG_REMOTE_READER_DEPLOY;
  ASSERT_EQ(0, smUndeployTask(&remove, true));
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(nullptr));
  auto *owner = AcquireStream();
  ASSERT_NE(nullptr, owner);
  EXPECT_EQ(1, owner->taskNum);
  ASSERT_EQ(1, TD_DLIST_NELES(owner->readerList));
  EXPECT_EQ(static_cast<void *>(old), static_cast<void *>(TD_DLIST_HEAD(owner->readerList)->data));
  ReleaseStream(owner);
  pin.reset();
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(nullptr));
  EXPECT_EQ(0, taosHashGetSize(gStreamMgmt.stmGrp[gid_]));
}

TEST_F(StreamRemoteReaderDeliveryTest, RoleTeardownReleasesInstalledSnodeReader) {
  response_.readerDeployEpoch = Poll();
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  ASSERT_EQ(1, taosHashGetSize(gStreamMgmt.taskMap));
  streamSetSnodeDisabled(false);
  EXPECT_EQ(0, taosHashGetSize(gStreamMgmt.taskMap));
  EXPECT_EQ(1, gConnectorCloses);
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(nullptr));
}

TEST_F(StreamRemoteReaderDeliveryTest, FailedConnectorOpenReleasesClaimForSameInstance) {
  response_.readerDeployEpoch = Poll();
  gOnFirstConnectorOpen = [] { return TSDB_CODE_FAILED; };
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  ASSERT_EQ(1, gConnectorOpens);
  ASSERT_EQ(0, taosHashGetSize(gStreamMgmt.taskMap));
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  EXPECT_EQ(2, gConnectorOpens);
  EXPECT_EQ(1, taosHashGetSize(gStreamMgmt.taskMap));
  ASSERT_NO_FATAL_FAILURE(CheckHeartbeat(&reader_->task));
}

class StreamColocatedReaderDeliveryTest : public StreamRemoteReaderDeliveryTest,
                                          public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    ASSERT_NO_FATAL_FAILURE(StreamRemoteReaderDeliveryTest::SetUp());
    savedCb_ = gStreamMgmt.msgCb;
    gStreamMgmt.msgCb.putToQueueFp = [](void *, EQueueType, SRpcMsg *msg) {
      rpcFreeCont(msg->pCont);
      return 0;
    };
    response_.rsps.rspList = taosArrayInit_s(sizeof(SStreamMgmtRsp), 2);
    ASSERT_NE(nullptr, response_.rsps.rspList);
    for (int i = 0; i < 2; ++i) {
      auto &trigger = triggers_[i];
      trigger.task.type = STREAM_TRIGGER_TASK;
      trigger.task.streamId = kStreamId + i;
      trigger.task.taskId = kTaskId + 2;
      trigger.task.nodeId = kNodeId;
      trigger.task.seriousId = kSeriousId;
      trigger.task.status = STREAM_STATUS_RUNNING;
      trigger.task.ackedSchemaReqId = -1;
      trigger.waitingMgmtReqId = 21;
      trigger.task.pMgmtReq = static_cast<SStreamMgmtReq *>(taosMemoryCalloc(1, sizeof(SStreamMgmtReq)));
      ASSERT_NE(nullptr, trigger.task.pMgmtReq);
      trigger.task.pMgmtReq->reqId = trigger.waitingMgmtReqId;
      trigger.task.pMgmtReq->type = STREAM_MGMT_REQ_TRIGGER_MIXED_ORIGTBL_READER;
      trigger.pRealtimeContext = &contexts_[i];
      contexts_[i].pTask = &trigger;
      trigger.readerList = taosArrayInit(1, sizeof(SStreamTaskAddr));
      trigger.pExtOrigTableRefs = taosArrayInit_s(sizeof(SSTriggerExtOrigTableRef), 1);
      auto *ref = static_cast<SSTriggerExtOrigTableRef *>(taosArrayGet(trigger.pExtOrigTableRefs, 0));
      ASSERT_NE(nullptr, ref);
      ref->registryEntryId = 17;
      ref->sourceConfigVersion = 1;
      ref->bindings = taosArrayInit_s(sizeof(SSTriggerExtOrigTableBinding), 1);
      auto *binding = static_cast<SSTriggerExtOrigTableBinding *>(taosArrayGet(ref->bindings, 0));
      ASSERT_NE(nullptr, binding);
      binding->bindingId = 71;
      binding->kind = STRIGGER_EXT_BINDING_TRIGGER;
      const int64_t key[] = {trigger.task.streamId, trigger.task.taskId};
      auto         *ptr = &trigger.task;
      ASSERT_EQ(0, taosHashPut(gStreamMgmt.taskMap, key, sizeof(key), &ptr, sizeof(ptr)));

      auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(response_.rsps.rspList, i));
      reply->header.msgType = STREAM_MSG_MIXED_ORIGTBL_READER_INFO;
      reply->task = trigger.task;
      reply->task.pMgmtReq = nullptr;
      reply->reqId = trigger.waitingMgmtReqId;
      reply->cont.pExtOrigTblReaders = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderRsp), 1);
      auto *ext = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
      ASSERT_NE(nullptr, ext);
      ext->registryEntryId = ref->registryEntryId;
      ext->deploymentGeneration = ref->sourceConfigVersion;
      ext->protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
      ext->capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START;
      ext->readerTaskAddr.taskId = reader_->task.taskId;
      ext->readerTaskAddr.nodeId = kNodeId + i;
      ext->acceptedTriggerBindingCount = 1;
      ext->results = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderResult), 1);
      auto *result = static_cast<SStreamExtOrigTblReaderResult *>(taosArrayGet(ext->results, 0));
      ASSERT_NE(nullptr, result);
      result->binding.bindingId = binding->bindingId;
    }
    if (GetParam()) {
      response_.readerDeployEpoch = Poll();
    } else {
      reader_->task.flags &= ~STREAM_FLAG_REMOTE_READER_DEPLOY;
    }
  }

  void TearDown() override {
    for (auto &trigger : triggers_) {
      const int64_t key[] = {trigger.task.streamId, trigger.task.taskId};
      (void)taosHashRemove(gStreamMgmt.taskMap, key, sizeof(key));
      tFreeSStreamMgmtReq(trigger.task.pMgmtReq);
      taosMemoryFree(trigger.task.pMgmtReq);
      taosArrayDestroy(trigger.readerList);
      for (size_t i = 0; i < taosArrayGetSize(trigger.pExtOrigTableRefs); ++i) {
        auto *ref = static_cast<SSTriggerExtOrigTableRef *>(taosArrayGet(trigger.pExtOrigTableRefs, i));
        taosArrayDestroy(ref->bindings);
        taosArrayDestroy(ref->pReaderCleanupAddrs);
      }
      taosArrayDestroy(trigger.pExtOrigTableRefs);
      if (trigger.pRealtimeContext) tSimpleHashCleanup(trigger.pRealtimeContext->pReaderExtProgress);
    }
    gStreamMgmt.msgCb = savedCb_;
    StreamRemoteReaderDeliveryTest::TearDown();
  }

  SStreamTriggerTask       triggers_[2] = {};
  SSTriggerRealtimeContext contexts_[2] = {};
  SMsgCb                   savedCb_ = {};
};

TEST_P(StreamColocatedReaderDeliveryTest, ResultOnlyBeforeFirstInstallWaitsAndKeepsSnodeOwnership) {
  auto *pending = triggers_[0].task.pMgmtReq;
  auto *ref = static_cast<SSTriggerExtOrigTableRef *>(taosArrayGet(triggers_[0].pExtOrigTableRefs, 0));

  // The result-only retry can overtake the initial heartbeat that carries the install.
  SArray *actions = nullptr;
  std::swap(actions, response_.deploy.streamList);
  DeliverResponse();
  std::swap(actions, response_.deploy.streamList);
  EXPECT_EQ(0, gConnectorOpens);
  EXPECT_EQ(pending, triggers_[0].task.pMgmtReq);
  EXPECT_EQ(-1, triggers_[0].task.ackedSchemaReqId);
  EXPECT_EQ(0, ref->readerTaskAddr.taskId);
  EXPECT_EQ(STREAM_STATUS_RUNNING, triggers_[0].task.status);
  EXPECT_EQ(nullptr, triggers_[1].task.pMgmtReq);
  EXPECT_EQ(21, triggers_[1].task.ackedSchemaReqId);

  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  EXPECT_EQ(1, gConnectorOpens);
  EXPECT_EQ(nullptr, triggers_[0].task.pMgmtReq);
  EXPECT_EQ(21, triggers_[0].task.ackedSchemaReqId);
  EXPECT_EQ(kTaskId, ref->readerTaskAddr.taskId);
  EXPECT_EQ(kNodeId, ref->readerTaskAddr.nodeId);
  EXPECT_EQ(1, ref->acceptedBindingCount);

  // A separately deployed vnode Reader shares the numeric node ID.
  reader_->task.flags &= ~STREAM_FLAG_REMOTE_READER_DEPLOY;
  ++reader_->task.taskId;
  taosArrayClearEx(response_.rsps.rspList, tFreeSStreamMgmtRsp);
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  ASSERT_EQ(2, gConnectorOpens);
  const int64_t vnodeKey[] = {kStreamId, reader_->task.taskId};
  const int64_t snodeKey[] = {kStreamId, kTaskId};
  streamSetSnodeDisabled(false);
  EXPECT_EQ(1, gConnectorCloses);
  EXPECT_EQ(nullptr, taosHashGet(gStreamMgmt.taskMap, snodeKey, sizeof(snodeKey)));
  EXPECT_NE(nullptr, taosHashGet(gStreamMgmt.taskMap, vnodeKey, sizeof(vnodeKey)));
  smUndeployVgTasks(kNodeId, true);
  EXPECT_EQ(2, gConnectorCloses);
  EXPECT_EQ(nullptr, taosHashGet(gStreamMgmt.taskMap, vnodeKey, sizeof(vnodeKey)));
}

TEST_P(StreamColocatedReaderDeliveryTest, FailedCreateWithResultOnlyRetriesExpiresOnlyForLegacyMnode) {
  auto       *pending = triggers_[0].task.pMgmtReq;
  auto       *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(response_.rsps.rspList, 0));
  const auto *ext = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
  pending->cont.pExtOrigTblReaders = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderReq), 1);
  auto *create = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(pending->cont.pExtOrigTblReaders, 0));
  ASSERT_NE(nullptr, create);
  create->registryEntryId = ext->registryEntryId;
  create->capabilities = ext->capabilities;
  create->protocolVersion = ext->protocolVersion;
  create->deploymentGeneration = ext->deploymentGeneration;
  std::vector<uint8_t> install;
  ASSERT_NO_FATAL_FAILURE(EncodeResponse(install));
  gOnFirstConnectorOpen = [] { return TSDB_CODE_FAILED; };
  ASSERT_NO_FATAL_FAILURE(Deliver(install));
  EXPECT_EQ(1, gConnectorOpens);
  EXPECT_EQ(STREAM_STATUS_RUNNING, triggers_[0].task.status);
  EXPECT_EQ(-1, triggers_[0].task.ackedSchemaReqId);
  EXPECT_EQ(21, triggers_[1].task.ackedSchemaReqId);
  ASSERT_EQ(pending, triggers_[0].task.pMgmtReq);
  ASSERT_EQ(0, pending->legacyReaderDeadline);

  // The baseline mnode already registered this CREATE and only resends its result.
  taosArrayClearEx(response_.deploy.streamList, tDeepFreeSStmStreamDeploy);
  --reply->reqId;
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  ASSERT_EQ(0, pending->legacyReaderDeadline);
  ++reply->reqId;
  const int64_t earliestDeadline = taosGetMonoTimestampMs() + STREAM_HB_ERR_HANDLE_MAX_DELAY;
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  ASSERT_EQ(pending, triggers_[0].task.pMgmtReq);
  EXPECT_EQ(reply->reqId, pending->reqId);
  EXPECT_EQ(STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START, create->capabilities);
  const int64_t deadline = pending->legacyReaderDeadline;
  if (GetParam()) {
    EXPECT_EQ(0, deadline);
  } else {
    EXPECT_GE(deadline, earliestDeadline);
    EXPECT_LE(deadline, taosGetMonoTimestampMs() + STREAM_HB_ERR_HANDLE_MAX_DELAY);
  }
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  EXPECT_EQ(deadline, pending->legacyReaderDeadline);
  EXPECT_EQ(STREAM_STATUS_RUNNING, triggers_[0].task.status);
  EXPECT_EQ(-1, triggers_[0].task.ackedSchemaReqId);

  pending->legacyReaderDeadline = taosGetMonoTimestampMs() - 1;
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  if (GetParam()) {
    // A guarded mnode can replay the install for the original request.
    EXPECT_EQ(STREAM_STATUS_RUNNING, triggers_[0].task.status);
    ASSERT_NO_FATAL_FAILURE(Deliver(install));
    EXPECT_EQ(2, gConnectorOpens);
    EXPECT_EQ(21, triggers_[0].task.ackedSchemaReqId);
    EXPECT_EQ(nullptr, triggers_[0].task.pMgmtReq);
  } else {
    auto *ref = static_cast<SSTriggerExtOrigTableRef *>(taosArrayGet(triggers_[0].pExtOrigTableRefs, 0));
    EXPECT_EQ(1, gConnectorOpens);
    EXPECT_EQ(STREAM_STATUS_FAILED, triggers_[0].task.status);
    EXPECT_EQ(TSDB_CODE_STREAM_TASK_NOT_EXIST, triggers_[0].task.errorCode);
    EXPECT_EQ(-1, triggers_[0].task.ackedSchemaReqId);
    EXPECT_EQ(0, ref->acceptedBindingCount);
    EXPECT_TRUE(ref->readerCleanupPending);
    ASSERT_EQ(1u, taosArrayGetSize(ref->pReaderCleanupAddrs));
    auto *cleanup = static_cast<SStreamTaskAddr *>(taosArrayGet(ref->pReaderCleanupAddrs, 0));
    EXPECT_EQ(kNodeId, cleanup->nodeId);
    EXPECT_EQ(kTaskId, cleanup->taskId);
  }
}

TEST_P(StreamColocatedReaderDeliveryTest, TerminalEntryErrorReachesTriggerWhenAnotherReaderFailsToOpen) {
  auto                    &trigger = triggers_[0];
  gOnFirstConnectorOpen = [] { return TSDB_CODE_FAILED; };
  if (!GetParam()) {
    ASSERT_NO_FATAL_FAILURE(DeliverResponse());
    taosArrayClearEx(response_.deploy.streamList, tDeepFreeSStmStreamDeploy);
    ASSERT_NO_FATAL_FAILURE(DeliverResponse());
    ASSERT_NE(nullptr, trigger.task.pMgmtReq);
    ASSERT_GT(trigger.task.pMgmtReq->legacyReaderDeadline, 0);
    trigger.task.pMgmtReq->legacyReaderDeadline = taosGetMonoTimestampMs() - 1;
  }
  SSTriggerExtOrigTableRef rejected = {};
  rejected.registryEntryId = 18;
  rejected.bindings = taosArrayInit(1, sizeof(SSTriggerExtOrigTableBinding));
  ASSERT_NE(nullptr, rejected.bindings);
  ASSERT_NE(nullptr, taosArrayPush(trigger.pExtOrigTableRefs, &rejected));
  auto                      *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(response_.rsps.rspList, 0));
  SStreamExtOrigTblReaderRsp error = {};
  error.registryEntryId = rejected.registryEntryId;
  error.protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
  error.code = TSDB_CODE_EXT_COLUMN_NOT_EXIST;
  ASSERT_NE(nullptr, taosArrayPush(reply->cont.pExtOrigTblReaders, &error));

  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  EXPECT_EQ(1, gConnectorOpens);
  EXPECT_EQ(STREAM_STATUS_FAILED, trigger.task.status);
  EXPECT_EQ(TSDB_CODE_EXT_COLUMN_NOT_EXIST, trigger.task.errorCode);
  EXPECT_EQ(-1, trigger.task.ackedSchemaReqId);
  EXPECT_NE(nullptr, trigger.task.pMgmtReq);
  EXPECT_EQ(21, triggers_[1].task.ackedSchemaReqId);
}

TEST_P(StreamColocatedReaderDeliveryTest, StaleReplyDiscardingSameAddressCannotConfirmCurrentRequest) {
  // Install an older incarnation at the address requested by this Trigger.
  SArray *replies = nullptr;
  std::swap(replies, response_.rsps.rspList);
  --reader_->task.seriousId;
  DeliverResponse();
  ++reader_->task.seriousId;
  std::swap(replies, response_.rsps.rspList);
  ASSERT_EQ(1, gConnectorOpens);

  auto          *current = static_cast<SStreamMgmtRsp *>(taosArrayGet(response_.rsps.rspList, 0));
  SStreamMgmtRsp stale = *current;
  --stale.reqId;
  stale.cont.pExtOrigTblReaders = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderRsp), 1);
  auto *ext = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(stale.cont.pExtOrigTblReaders, 0));
  ASSERT_NE(nullptr, ext);
  const auto *active = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(current->cont.pExtOrigTblReaders, 0));
  *ext = *active;
  ext->results = taosArrayDup(active->results, nullptr);
  ASSERT_NE(nullptr, ext->results);
  ASSERT_NE(nullptr, taosArrayPush(response_.rsps.rspList, &stale));
  auto *pending = triggers_[0].task.pMgmtReq;
  ASSERT_NO_FATAL_FAILURE(DeliverResponse());
  EXPECT_EQ(1, gConnectorOpens);
  EXPECT_EQ(pending, triggers_[0].task.pMgmtReq);
  EXPECT_EQ(-1, triggers_[0].task.ackedSchemaReqId);
  EXPECT_EQ(21, triggers_[1].task.ackedSchemaReqId);
}

INSTANTIATE_TEST_SUITE_P(CurrentAndLegacySender, StreamColocatedReaderDeliveryTest, ::testing::Bool());

TEST_F(StreamRemoteReaderDeliveryTest, DisabledRuntimeRejectsCreateFromPreviousLifetime) {
  gStreamMgmt.hb.readerRuntime = {71, 17};
  response_.readerRuntime = {71, 18};
  response_.readerDeployEpoch = Poll();
  std::vector<uint8_t> held;
  ASSERT_NO_FATAL_FAILURE(EncodeResponse(held));

  const bool savedDisabled = tsDisableStream;
  tsDisableStream = true;
  const int32_t code = streamHbInit(&gStreamMgmt.hb);
  tsDisableStream = savedDisabled;
  ASSERT_EQ(0, code);

  ASSERT_NE(71, gStreamMgmt.hb.readerRuntime.id);
  ASSERT_NO_FATAL_FAILURE(Deliver(held));
  EXPECT_EQ(0, taosHashGetSize(gStreamMgmt.taskMap));
  EXPECT_EQ(0, gConnectorOpens);
  EXPECT_EQ(0, gStreamMgmt.hb.readerRuntime.challenge);
  SMStreamHbRspMsg current = {};
  current.readerRuntime = {gStreamMgmt.hb.readerRuntime.id, 19};
  ASSERT_EQ(0, streamHbProcessRspMsg(&current));
  EXPECT_EQ(19, gStreamMgmt.hb.readerRuntime.challenge);
}

enum class ReaderPeers { Legacy, LegacyMnodeWithGuardedTrigger, Guarded };

class StreamReaderReplayTest : public StreamReaderNodeTest, public ::testing::WithParamInterface<ReaderPeers> {
 protected:
  bool GuardedTrigger() const { return GetParam() != ReaderPeers::Legacy; }

  void SetUp() override {
    ASSERT_NO_FATAL_FAILURE(StreamReaderNodeTest::SetUp());
    stub_.set(stTriggerTaskDeploy, deployTriggerWithStatistics);
    stub_.set(
        stReaderTaskDeploy, +[](SStreamReaderTask *reader, const SStreamReaderDeployMsg *) -> int32_t {
          reader->task.status = STREAM_STATUS_INIT;
          return 0;
        });
    stub_.set(
        stReaderTaskUndeploy, +[](SStreamReaderTask **reader, bool) -> int32_t {
          (*reader)->task.undeployCb(reader);
          return 0;
        });
    // Model the Trigger consuming its request when the matching mixed response reaches task execution.
    stub_.set(
        stTriggerTaskExecute, +[](SStreamTriggerTask *trigger, const SStreamMsg *msg) -> int32_t {
          auto *reply = reinterpret_cast<const SStreamMgmtRsp *>(msg);
          auto *req = trigger->task.pMgmtReq;
          if (req && req->reqId == reply->reqId) {
            if (trigger->task.ackedSchemaReqId) trigger->task.ackedSchemaReqId = req->reqId;
            tFreeSStreamMgmtReq(req);
            taosMemoryFreeClear(trigger->task.pMgmtReq);
          }
          return 0;
        });
    ASSERT_NO_FATAL_FAILURE(DeployTrigger());
    trigger_ = AcquireTrigger(&ref_);
    ASSERT_NE(nullptr, trigger_);
    trigger_->task.ackedSchemaReqId = GuardedTrigger() ? -1 : 0;
  }

  void TearDown() override {
    if (trigger_) {
      tFreeSStreamMgmtReq(trigger_->task.pMgmtReq);
      taosMemoryFreeClear(trigger_->task.pMgmtReq);
      streamReleaseTask(ref_);
    }
    StreamReaderNodeTest::TearDown();
  }

  void Request(int64_t id) {
    ASSERT_EQ(nullptr, trigger_->task.pMgmtReq);
    auto *req = static_cast<SStreamMgmtReq *>(taosMemoryCalloc(1, sizeof(SStreamMgmtReq)));
    ASSERT_NE(nullptr, req);
    req->type = STREAM_MGMT_REQ_TRIGGER_MIXED_ORIGTBL_READER;
    req->reqId = id;
    trigger_->task.pMgmtReq = req;
  }

  enum ReaderAction {
    Create = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START,
    Cleanup = STREAM_EXT_ORIGTBL_READER_CAP_PATCH,
    Patch = Create | Cleanup,
  };

  enum ExtraAction { None, DeployPeer, CleanupPeer, DeploySameReader };

  void Deliver(int64_t id, ReaderAction action, ExtraAction extra = None) {
    SMStreamHbRspMsg hb = {};
    hb.streamGId = gid_;
    hb.rsps.rspList = taosArrayInit_s(sizeof(SStreamMgmtRsp), 1);
    ASSERT_NE(nullptr, hb.rsps.rspList);
    auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(hb.rsps.rspList, 0));
    reply->header.msgType = STREAM_MSG_MIXED_ORIGTBL_READER_INFO;
    reply->task.type = STREAM_TRIGGER_TASK;
    reply->task.streamId = kStreamId;
    reply->task.taskId = kTaskId;
    reply->task.seriousId = kSeriousId;
    reply->reqId = id;
    reply->cont.pExtOrigTblReaders = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderRsp), 1);
    ASSERT_NE(nullptr, reply->cont.pExtOrigTblReaders);
    auto *ext = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
    ext->registryEntryId = 17;
    ext->protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
    ext->capabilities = action;
    if (GuardedTrigger()) ext->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_GUARDED_DEPLOY;
    ext->readerTaskAddr.taskId = kTaskId + 1;
    ext->readerTaskAddr.nodeId = kNodeId;
    SStreamTask reader = {};
    reader.type = STREAM_READER_TASK;
    reader.flags = STREAM_FLAG_TRIGGER_READER | STREAM_FLAG_REF_EXT_SOURCE;
    reader.streamId = kStreamId;
    reader.taskId = ext->readerTaskAddr.taskId;
    reader.seriousId = 13;
    reader.nodeId = kNodeId;
    if (action != Create) {
      hb.undeploy.taskList = taosArrayInit_s(sizeof(SStreamTaskUndeploy), extra == CleanupPeer ? 2 : 1);
      ASSERT_NE(nullptr, hb.undeploy.taskList);
      for (size_t i = 0; i < taosArrayGetSize(hb.undeploy.taskList); ++i) {
        auto *undeploy = static_cast<SStreamTaskUndeploy *>(taosArrayGet(hb.undeploy.taskList, i));
        undeploy->task = reader;
        undeploy->task.taskId += i;
        undeploy->undeployMsg.doCleanup = true;
      }
    }
    if (action != Cleanup || extra == DeploySameReader) {
      hb.deploy.streamList = taosArrayInit_s(sizeof(SStmStreamDeploy), 1);
      ASSERT_NE(nullptr, hb.deploy.streamList);
      auto *stream = static_cast<SStmStreamDeploy *>(taosArrayGet(hb.deploy.streamList, 0));
      stream->streamId = kStreamId;
      stream->readerTasks = taosArrayInit_s(sizeof(SStmTaskDeploy), extra == DeployPeer ? 2 : 1);
      ASSERT_NE(nullptr, stream->readerTasks);
      for (size_t i = 0; i < taosArrayGetSize(stream->readerTasks); ++i) {
        auto *deploy = static_cast<SStmTaskDeploy *>(taosArrayGet(stream->readerTasks, i));
        deploy->task = reader;
        deploy->task.taskId += i;
        if (action == Patch && GetParam() == ReaderPeers::Guarded) ++deploy->task.seriousId;
      }
    }
    ASSERT_EQ(0, streamHbProcessRspMsg(&hb));
  }

  SStreamTask *Reader(int64_t id) {
    const int64_t key[] = {kStreamId, id};
    auto        **task = static_cast<SStreamTask **>(taosHashGet(gStreamMgmt.taskMap, key, sizeof(key)));
    return task ? *task : nullptr;
  }

  SStreamTriggerTask *trigger_ = nullptr;
  void               *ref_ = nullptr;
};

TEST_P(StreamReaderReplayTest, LateCreateKeepsCleanedReaderAbsentAndDeploysPeer) {
  ASSERT_NO_FATAL_FAILURE(Request(21));
  ASSERT_NO_FATAL_FAILURE(Deliver(21, Create));
  ASSERT_TRUE(Reader(kTaskId + 1));
  ASSERT_EQ(nullptr, trigger_->task.pMgmtReq);
  ASSERT_NO_FATAL_FAILURE(Request(22));
  ASSERT_NO_FATAL_FAILURE(Deliver(22, Cleanup));
  ASSERT_FALSE(Reader(kTaskId + 1));
  ASSERT_EQ(nullptr, trigger_->task.pMgmtReq);
  ASSERT_NO_FATAL_FAILURE(Deliver(21, Create, DeployPeer));
  EXPECT_FALSE(Reader(kTaskId + 1));
  EXPECT_TRUE(Reader(kTaskId + 2));
  EXPECT_EQ(GuardedTrigger() ? 22 : 0, trigger_->task.ackedSchemaReqId);

  streamReleaseTask(ref_);
  trigger_ = nullptr;
  ASSERT_NO_FATAL_FAILURE(DeployTrigger());
  trigger_ = AcquireTrigger(&ref_);
  ASSERT_NE(nullptr, trigger_);
  ASSERT_NE(kSeriousId, trigger_->task.seriousId);
  trigger_->task.ackedSchemaReqId = GuardedTrigger() ? -1 : 0;
  ASSERT_NO_FATAL_FAILURE(Request(21));
  auto *pending = trigger_->task.pMgmtReq;
  ASSERT_NO_FATAL_FAILURE(Deliver(21, Create));
  EXPECT_FALSE(Reader(kTaskId + 1));
  EXPECT_EQ(pending, trigger_->task.pMgmtReq);
}

TEST_P(StreamReaderReplayTest, ReplayedPatchKeepsReplacementAndCleansIndependentPeer) {
  ASSERT_NO_FATAL_FAILURE(Request(21));
  ASSERT_NO_FATAL_FAILURE(Deliver(21, Create, DeployPeer));
  auto *original = Reader(kTaskId + 1);
  ASSERT_NE(nullptr, original);
  ASSERT_NE(nullptr, Reader(kTaskId + 2));

  ASSERT_NO_FATAL_FAILURE(Request(22));
  ASSERT_NO_FATAL_FAILURE(Deliver(22, Patch));
  auto *replacement = Reader(kTaskId + 1);
  ASSERT_NE(nullptr, replacement);
  ASSERT_NE(original, replacement);
  ASSERT_EQ(nullptr, trigger_->task.pMgmtReq);

  ASSERT_NO_FATAL_FAILURE(Deliver(22, Patch, CleanupPeer));
  EXPECT_EQ(replacement, Reader(kTaskId + 1));
  EXPECT_EQ(nullptr, Reader(kTaskId + 2));
}

TEST_P(StreamReaderReplayTest, StaleCleanupStillRemovesReaderAlongsideQueuedDeployment) {
  ASSERT_NO_FATAL_FAILURE(Request(21));
  ASSERT_NO_FATAL_FAILURE(Deliver(21, Create));
  ASSERT_NE(nullptr, Reader(kTaskId + 1));

  ASSERT_NO_FATAL_FAILURE(Request(23));
  ASSERT_NO_FATAL_FAILURE(Deliver(22, Cleanup, DeploySameReader));
  EXPECT_EQ(nullptr, Reader(kTaskId + 1));
  ASSERT_NE(nullptr, trigger_->task.pMgmtReq);
  EXPECT_EQ(23, trigger_->task.pMgmtReq->reqId);
}

INSTANTIATE_TEST_SUITE_P(LegacyAndGuarded, StreamReaderReplayTest,
                         ::testing::Values(ReaderPeers::Legacy, ReaderPeers::LegacyMnodeWithGuardedTrigger,
                                           ReaderPeers::Guarded));

TEST_F(StreamMgmtLifecycleTest, HeartbeatSkipsDeployingOwnerAndReportsOtherGroup) {
  DeployTrigger();
  SStreamInfo *pReady = AcquireStream();
  ASSERT_NE(pReady, nullptr);

  // A newly registered owner stays read-locked while deployment opens its connections.
  SStreamInfo deploying = {};
  taosRLockLatch(&deploying.lock);
  SStreamHbMsg blockedHeartbeat = {};
  blockedHeartbeat.streamGId = STREAM_GID(kStreamId + 1);
  SStreamHbMsg readyHeartbeat = {};
  readyHeartbeat.streamGId = gid_;
  std::promise<void> started;
  auto               entered = started.get_future();
  auto               heartbeat = std::async(std::launch::async, [&] {
    started.set_value();
    EXPECT_EQ(stmHbAddStreamStatus(&blockedHeartbeat, &deploying, kStreamId + 1, false), TSDB_CODE_SUCCESS);
    EXPECT_EQ(stmHbAddStreamStatus(&readyHeartbeat, pReady, kStreamId, false), TSDB_CODE_SUCCESS);
  });
  entered.wait();
  const auto status = heartbeat.wait_for(std::chrono::seconds(5));
  taosRUnLockLatch(&deploying.lock);
  heartbeat.get();

  EXPECT_EQ(status, std::future_status::ready);
  EXPECT_EQ(taosArrayGetSize(blockedHeartbeat.pStreamStatus), 0);
  EXPECT_EQ(taosArrayGetSize(readyHeartbeat.pStreamStatus), 1);
  EXPECT_EQ(deploying.destroyed, 0);
  tCleanupStreamHbMsg(&blockedHeartbeat, true);
  tCleanupStreamHbMsg(&readyHeartbeat, true);
  ReleaseStream(pReady);
}

}  // namespace
