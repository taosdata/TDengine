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
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "stub.h"

extern "C" {
#include "streamInt.h"
#include "streamTriggerTask.h"
#include "taoserror.h"
#include "tencode.h"

int32_t smAddTasksToStreamMap(SStmStreamDeploy *pDeploy, SStreamInfo *pStream);
int32_t stmHbAddStreamStatus(SStreamHbMsg *pMsg, SStreamInfo *pStream, int64_t streamId, bool reportPeriod);
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
    stub_.set(stTriggerTaskDeploy, deployTriggerForLifecycleTest);
    stub_.set(stTriggerTaskUndeploy, undeployTriggerForLifecycleTest);
  }

  void TearDown() override {
    taosHashCleanup(gStreamMgmt.taskMap);
    taosHashCleanup(gStreamMgmt.stmGrp[gid_]);
    gStreamMgmt.taskMap = savedTaskMap_;
    gStreamMgmt.stmGrp[gid_] = savedGroup_;
    gStreamMgmt.snodeEnabled = savedSnodeEnabled_;
  }

  void DeployTrigger() {
    SStmTaskDeploy trigger = {};
    trigger.task.type = STREAM_TRIGGER_TASK;
    trigger.task.streamId = kStreamId;
    trigger.task.taskId = kTaskId;
    trigger.task.seriousId = kSeriousId + gDeployCalls;
    trigger.task.nodeId = kNodeId;
    trigger.task.taskIdx = 0;

    SStmStreamDeploy deploy = {};
    deploy.streamId = kStreamId;
    deploy.triggerTask = &trigger;
    ASSERT_EQ(smDeployTasks(&deploy), TSDB_CODE_SUCCESS);
  }

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

}  // namespace
