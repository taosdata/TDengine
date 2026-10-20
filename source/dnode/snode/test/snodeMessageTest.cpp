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
#include <string>
#include <vector>
#include "stub.h"

extern "C" {
#include "sndInt.h"
#include "streamInt.h"
#include "streamRunner.h"
#include "streamTriggerTask.h"
}

namespace {
constexpr int64_t kStreamId = 0x1234;
constexpr int64_t kTaskId = 0x5678;
int32_t           responseCode = 0;
int32_t           executions = 0;
std::vector<char> responseBody;

class SnodeMessageTest : public testing::Test {
 protected:
  SSnode             node = {};
  std::string        savedDataDir;
  std::string        dataDir;
  bool               savedSkipKeyCheck = false;
  SHashObj*          savedTaskMap = nullptr;
  SHashObj*          savedGroup = nullptr;
  SHashObj*          taskMap = nullptr;
  SHashObj*          group = nullptr;
  SStreamTriggerTask trigger = {};

  void SetUp() override {
    responseCode = executions = 0;
    responseBody.clear();
    savedTaskMap = gStreamMgmt.taskMap;
    savedGroup = gStreamMgmt.stmGrp[STREAM_GID(kStreamId)];
    savedSkipKeyCheck = tsSkipKeyCheckMode;
    tsSkipKeyCheckMode = true;
    savedDataDir = tsDataDir;
    dataDir = std::string(TD_TMP_DIR_PATH) + "snode-message-" + std::to_string(taosGetPId());
    tstrncpy(tsDataDir, dataDir.c_str(), PATH_MAX);
    ASSERT_EQ(taosMulMkDir((dataDir + "/snode/checkpoint").c_str()), 0);
    taskMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    ASSERT_NE(taskMap, nullptr);
    group = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    ASSERT_NE(group, nullptr);
    gStreamMgmt.taskMap = taskMap;
    gStreamMgmt.stmGrp[STREAM_GID(kStreamId)] = group;
    trigger.task.type = STREAM_TRIGGER_TASK;
    trigger.task.streamId = kStreamId;
    trigger.task.taskId = kTaskId + 1;
    SStreamTask* task = &trigger.task;
    int64_t      key[] = {kStreamId, trigger.task.taskId};
    ASSERT_EQ(taosHashPut(taskMap, key, sizeof(key), &task, sizeof(task)), 0);
    SStreamInfo stream = {};
    stream.triggerTaskId = trigger.task.taskId;
    ASSERT_EQ(taosHashPut(group, &kStreamId, sizeof(kStreamId), &stream, sizeof(stream)), 0);
  }

  void TearDown() override {
    gStreamMgmt.taskMap = savedTaskMap;
    gStreamMgmt.stmGrp[STREAM_GID(kStreamId)] = savedGroup;
    tstrncpy(tsDataDir, savedDataDir.c_str(), PATH_MAX);
    tsSkipKeyCheckMode = savedSkipKeyCheck;
    taosHashCleanup(taskMap);
    taosHashCleanup(group);
    taosRemoveDir(dataDir.c_str());
  }

  void addTask(SStreamTask* task) {
    task->streamId = kStreamId;
    task->taskId = kTaskId;
    int64_t key[] = {kStreamId, kTaskId};
    ASSERT_EQ(taosHashPut(gStreamMgmt.taskMap, key, sizeof(key), &task, sizeof(task)), 0);
  }

  int32_t process(int32_t type, void* body, int32_t length, int32_t error = 0) {
    SRpcMsg msg = {};
    msg.msgType = type;
    msg.pCont = body;
    msg.contLen = length;
    msg.code = error;
    return sndProcessStreamMsg(&node, nullptr, &msg);
  }

  int32_t fetch(int32_t type, SStreamRuntimeFuncInfo* runtime, bool reset = true) {
    SResFetchReq req = {};
    req.queryId = kStreamId;
    req.taskId = kTaskId;
    req.reset = reset;
    req.pStRtFuncInfo = runtime;
    int32_t length = tSerializeSResFetchReq(nullptr, 0, &req, true, false);
    EXPECT_GT(length, 0);
    if (length <= 0) return TSDB_CODE_INVALID_MSG;
    std::vector<char> body(length);
    int32_t           encoded = tSerializeSResFetchReq(body.data(), length, &req, true, false);
    EXPECT_EQ(encoded, length);
    if (encoded != length) return TSDB_CODE_INVALID_MSG;
    return process(type, body.data(), length);
  }
};

TEST_F(SnodeMessageTest, CheckpointMessagesRejectTruncatedBodies) {
  for (auto entry : {std::pair<int32_t, int32_t>{TDMT_STREAM_SYNC_CHECKPOINT, 24},
                     {TDMT_STREAM_SYNC_CHECKPOINT_RSP, 16},
                     {TDMT_STREAM_DELETE_CHECKPOINT, 16}}) {
    for (int32_t length = 0; length < entry.second; ++length) {
      SCOPED_TRACE(::testing::Message() << "type=" << entry.first << " length=" << length);
      void* body = taosMemoryCalloc(1, length > 0 ? length : 1);
      EXPECT_EQ(process(entry.first, body, length), TSDB_CODE_INVALID_MSG);
      taosMemoryFree(body);
    }
    EXPECT_EQ(process(entry.first, nullptr, entry.second), TSDB_CODE_INVALID_MSG);
  }
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_PENDING);
}

TEST_F(SnodeMessageTest, CheckpointErrorResponseDoesNotNeedABody) {
  EXPECT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, nullptr, 0, TSDB_CODE_RPC_BROKEN_LINK), TSDB_CODE_RPC_BROKEN_LINK);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_PENDING);
}

TEST_F(SnodeMessageTest, CheckpointSentinelAndDeleteKeepTheirWireLayouts) {
  int64_t body[3] = {};
  int32_t version = -1;
  memcpy(reinterpret_cast<char*>(body) + 4, &version, sizeof(version));
  memcpy(reinterpret_cast<char*>(body) + 8, &kStreamId, sizeof(kStreamId));
  EXPECT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, body, 16), 0);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_READY);
  EXPECT_EQ(process(TDMT_STREAM_DELETE_CHECKPOINT, body, 16), 0);
}

TEST_F(SnodeMessageTest, MissingCheckpointRequestReturnsSentinelWithoutCreatingFile) {
  int64_t body[3] = {};
  int32_t version = -1;
  memcpy(reinterpret_cast<char*>(body) + sizeof(SMsgHead) + INT_BYTES, &version, sizeof(version));
  memcpy(reinterpret_cast<char*>(body) + sizeof(SMsgHead) + 2 * INT_BYTES, &kStreamId, sizeof(kStreamId));
  ASSERT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT, body, sizeof(body)), 0);
  ASSERT_EQ(responseCode, 0);
  ASSERT_EQ(responseBody.size(), 16U);
  int32_t returnedVersion = 0;
  int64_t returnedStreamId = 0;
  memcpy(&returnedVersion, responseBody.data() + INT_BYTES, sizeof(returnedVersion));
  memcpy(&returnedStreamId, responseBody.data() + 2 * INT_BYTES, sizeof(returnedStreamId));
  EXPECT_EQ(returnedVersion, -1);
  EXPECT_EQ(returnedStreamId, kStreamId);
  EXPECT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, responseBody.data(), responseBody.size()), 0);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_READY);
  EXPECT_FALSE(taosCheckExistFile((dataDir + "/snode/checkpoint/1234.ck").c_str()));
}

TEST_F(SnodeMessageTest, CheckpointErrorRequiresCompleteSentinelForTaskCorrelation) {
  int32_t body[5] = {};
  body[1] = -1;
  memcpy(body + 2, &kStreamId, sizeof(kStreamId));
  const int32_t error = TAOS_SYSTEM_ERROR(ENOSPC);
  for (int32_t length = 0; length < 16; ++length) {
    EXPECT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, body, length, error), error);
    EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_PENDING);
  }
  EXPECT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, body, sizeof(body), error), error);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_PENDING);
  body[0] = 12;
  EXPECT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, body, 16, error), error);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_PENDING);
  body[0] = 0;
  body[1] = 2;
  EXPECT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, body, 16, error), error);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_PENDING);
}

static TdFilePtr failCheckpointOpen(const char*, int32_t) {
  terrno = TAOS_SYSTEM_ERROR(EACCES);
  return nullptr;
}

TEST_F(SnodeMessageTest, CheckpointReadPropagatesOpenFailure) {
  Stub stub;
  stub.set(taosOpenFile, failCheckpointOpen);
  void*   data = nullptr;
  int64_t length = 0;
  EXPECT_EQ(streamReadCheckPoint(kStreamId, &data, &length), static_cast<int32_t>(TAOS_SYSTEM_ERROR(EACCES)));
  EXPECT_EQ(data, nullptr);
  EXPECT_EQ(length, 0);
}

TEST_F(SnodeMessageTest, FetchRequiresRuntimeInfoBeforeTaskLookup) {
  for (int32_t type : {TDMT_STREAM_FETCH_FROM_RUNNER, TDMT_STREAM_FETCH_FROM_CACHE}) {
    EXPECT_EQ(fetch(type, nullptr), TSDB_CODE_INVALID_MSG);
    EXPECT_EQ(responseCode, TSDB_CODE_INVALID_MSG);
  }
  EXPECT_EQ(executions, 0);
}

TEST_F(SnodeMessageTest, RunnerMessagesRejectOtherTaskTypesBeforeWritingTaskState) {
  for (auto type : {STREAM_READER_TASK, STREAM_TRIGGER_TASK}) {
    SStreamRunnerTask task = {};
    task.task.type = type;
    addTask(&task.task);
    const auto             original = task;
    SStreamRuntimeFuncInfo runtime = {};
    EXPECT_EQ(fetch(TDMT_STREAM_FETCH_FROM_RUNNER, &runtime), TSDB_CODE_INVALID_MSG);

    SSTriggerCalcRequest calc = {};
    calc.streamId = kStreamId;
    calc.runnerTaskId = kTaskId;
    calc.createTable = 1;
    int32_t length = tSerializeSTriggerCalcRequest(nullptr, 0, &calc);
    ASSERT_GT(length, 0);
    std::vector<char> body(sizeof(SMsgHead) + length);
    ASSERT_EQ(tSerializeSTriggerCalcRequest(body.data() + sizeof(SMsgHead), length, &calc), length);
    EXPECT_EQ(process(TDMT_STREAM_TRIGGER_CALC, body.data(), body.size()), TSDB_CODE_INVALID_MSG);

    SSTriggerDropRequest drop = {};
    drop.streamId = kStreamId;
    drop.runnerTaskId = kTaskId;
    length = tSerializeSTriggerDropTableRequest(nullptr, 0, &drop);
    ASSERT_GT(length, 0);
    body.resize(sizeof(SMsgHead) + length);
    ASSERT_EQ(tSerializeSTriggerDropTableRequest(body.data() + sizeof(SMsgHead), length, &drop), length);
    EXPECT_EQ(process(TDMT_STREAM_TRIGGER_DROP, body.data(), body.size()), TSDB_CODE_INVALID_MSG);
    EXPECT_EQ(memcmp(&task, &original, sizeof(task)), 0);
    EXPECT_EQ(executions, 0);
    int64_t key[] = {kStreamId, kTaskId};
    EXPECT_EQ(taosHashRemove(gStreamMgmt.taskMap, key, sizeof(key)), 0);
  }
}

TEST_F(SnodeMessageTest, ValidRunnerStillReachesExecution) {
  SStreamRunnerTask task = {};
  task.task.type = STREAM_RUNNER_TASK;
  addTask(&task.task);
  SStreamRuntimeFuncInfo runtime = {};
  EXPECT_EQ(fetch(TDMT_STREAM_FETCH_FROM_RUNNER, &runtime), TSDB_CODE_APP_ERROR);
  EXPECT_EQ(fetch(TDMT_STREAM_FETCH_FROM_RUNNER, &runtime, false), TSDB_CODE_APP_ERROR);
  EXPECT_EQ(executions, 2);
}

TEST_F(SnodeMessageTest, TriggerProgressRejectsReaderAndRunnerTasks) {
  for (auto type : {STREAM_READER_TASK, STREAM_RUNNER_TASK}) {
    SStreamTask task = {};
    task.type = type;
    int64_t delay = 123;
    bool    finished = false;
    EXPECT_EQ(stTriggerTaskGetDelay(&task, &delay, &finished), TSDB_CODE_INVALID_MSG);
    EXPECT_EQ(delay, 123);
    EXPECT_FALSE(finished);
  }
}

TEST_F(SnodeMessageTest, ExternalFetchRejectsNonReaderEvenWithExternalSourceFlag) {
  for (auto type : {STREAM_TRIGGER_TASK, STREAM_RUNNER_TASK}) {
    SStreamTask task = {};
    task.type = type;
    task.flags = STREAM_FLAG_REF_EXT_SOURCE;
    addTask(&task);
    EXPECT_EQ(fetch(TDMT_STREAM_FETCH_EXT, nullptr), TSDB_CODE_INVALID_MSG);
    int64_t key[] = {kStreamId, kTaskId};
    EXPECT_EQ(taosHashRemove(gStreamMgmt.taskMap, key, sizeof(key)), 0);
  }
}

TEST_F(SnodeMessageTest, CheckpointSyncPersistsAndDeleteRemovesValidData) {
  int64_t body[3] = {};
  int32_t version = 1;
  memcpy(reinterpret_cast<char*>(body) + 12, &version, sizeof(version));
  memcpy(reinterpret_cast<char*>(body) + 16, &kStreamId, sizeof(kStreamId));
  EXPECT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT, body, 24), 0);
  EXPECT_EQ(responseCode, 0);
  void*   data = nullptr;
  int64_t length = 0;
  ASSERT_EQ(streamReadCheckPoint(kStreamId, &data, &length), 0);
  ASSERT_EQ(length, 16);
  ASSERT_NE(data, nullptr);
  EXPECT_EQ(memcmp(data, reinterpret_cast<char*>(body) + 8, 16), 0);
  taosMemoryFree(data);
  memcpy(reinterpret_cast<char*>(body) + 8, &kStreamId, sizeof(kStreamId));
  EXPECT_EQ(process(TDMT_STREAM_DELETE_CHECKPOINT, body, 16), 0);
  EXPECT_FALSE(taosCheckExistFile((dataDir + "/snode/checkpoint/1234.ck").c_str()));
}

TEST_F(SnodeMessageTest, CheckpointWriteFailureDoesNotMarkReadyOrAcknowledgeSuccess) {
  // A directory at the checkpoint filename forces the real file write to fail.
  ASSERT_EQ(taosMkDir((dataDir + "/snode/checkpoint/1234.ck").c_str()), 0);
  int64_t body[3] = {};
  int32_t version = 1;
  memcpy(reinterpret_cast<char*>(body) + 12, &version, sizeof(version));
  memcpy(reinterpret_cast<char*>(body) + 16, &kStreamId, sizeof(kStreamId));
  EXPECT_NE(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, reinterpret_cast<char*>(body) + 8, 16), 0);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_RETRY);
  EXPECT_NE(process(TDMT_STREAM_SYNC_CHECKPOINT, body, 24), 0);
  EXPECT_NE(responseCode, 0);

  // A later background sync failure must not restart an already ready task.
  trigger.checkpointSyncState = STREAM_CHECKPOINT_SYNC_READY;
  EXPECT_NE(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, reinterpret_cast<char*>(body) + 8, 16), 0);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_READY);
}

TEST_F(SnodeMessageTest, CheckpointWriteRejectsShortDataWithoutCreatingFile) {
  char body[16] = {};
  for (int32_t length = 0; length < 16; ++length) {
    EXPECT_EQ(streamWriteCheckPoint(kStreamId, body, length), TSDB_CODE_INVALID_PARA);
  }
  EXPECT_FALSE(taosCheckExistFile((dataDir + "/snode/checkpoint/1234.ck").c_str()));
}

static int64_t failCheckpointBodyWrite(TdFilePtr file, const void* data, int64_t length) {
  if (length == sizeof(int64_t)) return taosPWriteFile(file, data, length, 0);
  terrno = TAOS_SYSTEM_ERROR(ENOSPC);
  return -1;
}

TEST_F(SnodeMessageTest, FailedFirstWriteDoesNotPublishPartialCheckpoint) {
  int32_t checkpoint[7] = {};
  checkpoint[0] = sizeof(checkpoint) - INT_BYTES;
  checkpoint[1] = 2;
  checkpoint[4] = 1;
  memcpy(checkpoint + 2, &kStreamId, sizeof(kStreamId));
  Stub stub;
  stub.set(taosWriteFile, failCheckpointBodyWrite);
  EXPECT_EQ(streamWriteCheckPoint(kStreamId, checkpoint, sizeof(checkpoint)), TAOS_SYSTEM_ERROR(ENOSPC));
  EXPECT_FALSE(taosCheckExistFile((dataDir + "/snode/checkpoint/1234.ck").c_str()));
  stub.reset(taosWriteFile);

  ASSERT_EQ(streamWriteCheckPoint(kStreamId, checkpoint, sizeof(checkpoint)), 0);
  void*   data = nullptr;
  int64_t length = 0;
  ASSERT_EQ(streamReadCheckPoint(kStreamId, &data, &length), 0);
  ASSERT_EQ(length, sizeof(checkpoint));
  ASSERT_NE(data, nullptr);
  EXPECT_EQ(memcmp(data, checkpoint, sizeof(checkpoint)), 0);
  taosMemoryFree(data);
}

TEST_F(SnodeMessageTest, CheckpointWriteTruncatesStaleTempFile) {
  const auto temp = dataDir + "/snode/checkpoint/1234.tmp";
  TdFilePtr  fp = taosOpenFile(temp.c_str(), TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  ASSERT_NE(fp, nullptr);
  char stale[64] = {};
  ASSERT_EQ(taosWriteFile(fp, stale, sizeof(stale)), sizeof(stale));
  ASSERT_EQ(taosCloseFile(&fp), 0);
  int32_t checkpoint[7] = {};
  checkpoint[0] = sizeof(checkpoint) - INT_BYTES;
  checkpoint[1] = 2;
  checkpoint[4] = 1;
  memcpy(checkpoint + 2, &kStreamId, sizeof(kStreamId));
  ASSERT_EQ(streamWriteCheckPoint(kStreamId, checkpoint, sizeof(checkpoint)), 0);
  EXPECT_FALSE(taosCheckExistFile(temp.c_str()));
  void*   data = nullptr;
  int64_t length = 0;
  ASSERT_EQ(streamReadCheckPoint(kStreamId, &data, &length), 0);
  EXPECT_EQ(length, sizeof(checkpoint));
  ASSERT_NE(data, nullptr);
  if (length == sizeof(checkpoint)) EXPECT_EQ(memcmp(data, checkpoint, sizeof(checkpoint)), 0);
  taosMemoryFree(data);
}

TEST_F(SnodeMessageTest, CheckpointReadRejectsShortLegacyAndLengthPrefixedFiles) {
  const auto file = dataDir + "/snode/checkpoint/1234.ck";
  for (bool prefixed : {false, true}) {
    for (int64_t length = 0; length < 16; ++length) {
      SCOPED_TRACE(::testing::Message() << "prefixed=" << prefixed << " length=" << length);
      TdFilePtr fp = taosOpenFile(file.c_str(), TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
      ASSERT_NE(fp, nullptr);
      if (prefixed) ASSERT_EQ(taosWriteFile(fp, &length, sizeof(length)), sizeof(length));
      char body[16] = {};
      ASSERT_EQ(taosWriteFile(fp, body, length), length);
      ASSERT_EQ(taosCloseFile(&fp), 0);
      void*   data = nullptr;
      int64_t dataLen = 0;
      EXPECT_EQ(streamReadCheckPoint(kStreamId, &data, &dataLen), TSDB_CODE_FILE_CORRUPTED);
      EXPECT_EQ(data, nullptr);
      EXPECT_EQ(dataLen, 0);
      taosMemoryFree(data);
    }
  }
}

TEST_F(SnodeMessageTest, CheckpointReadPreservesValidLegacyFormat) {
  int64_t body[2] = {};
  int32_t frameLength = 12;
  int32_t version = 1;
  memcpy(body, &frameLength, sizeof(frameLength));
  memcpy(reinterpret_cast<char*>(body) + 4, &version, sizeof(version));
  memcpy(reinterpret_cast<char*>(body) + 8, &kStreamId, sizeof(kStreamId));
  TdFilePtr fp =
      taosOpenFile((dataDir + "/snode/checkpoint/1234.ck").c_str(), TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  ASSERT_NE(fp, nullptr);
  ASSERT_EQ(taosWriteFile(fp, body, sizeof(body)), sizeof(body));
  ASSERT_EQ(taosCloseFile(&fp), 0);
  void*   data = nullptr;
  int64_t length = 0;
  ASSERT_EQ(streamReadCheckPoint(kStreamId, &data, &length), 0);
  ASSERT_EQ(length, 16);
  ASSERT_NE(data, nullptr);
  EXPECT_EQ(memcmp(data, body, sizeof(body)), 0);
  taosMemoryFree(data);
}

static SEpSet                         checkpointEndpoint = {};
static std::vector<std::vector<char>> checkpointRequests;
static int32_t                        checkpointSendCode = 0;
static int32_t                        checkpointWaits = 0;

static SEpSet* getCheckpointEndpoint(int32_t) { return &checkpointEndpoint; }
static SEpSet* noCheckpointEndpoint(int32_t) { return nullptr; }

static int32_t captureCheckpointRequest(const SEpSet*, SRpcMsg* msg) {
  EXPECT_EQ(msg->msgType, TDMT_STREAM_SYNC_CHECKPOINT);
  auto* body = static_cast<char*>(msg->pCont);
  checkpointRequests.emplace_back(body, body + msg->contLen);
  rpcFreeCont(msg->pCont);
  return checkpointSendCode;
}

static int32_t captureCheckpointWait(SList*, const void*) {
  // Count scheduled waits; this fixture resumes the context explicitly without a timer.
  ++checkpointWaits;
  return 0;
}

class SnodeCheckpointRecoveryTest : public SnodeMessageTest {
 protected:
  Stub                     stub;
  getSynEpset_f            savedEndpoint = nullptr;
  SSTriggerRealtimeContext context = {};
  int64_t                  errorTaskId = 0;

  void SetUp() override {
    savedEndpoint = gStreamMgmt.getSynEpset;
    SnodeMessageTest::SetUp();
    if (HasFatalFailure()) return;
    checkpointRequests.clear();
    checkpointSendCode = checkpointWaits = 0;
    gStreamMgmt.getSynEpset = getCheckpointEndpoint;
    stub.set(tmsgSendReq, captureCheckpointRequest);
    stub.set(tdListAppend, captureCheckpointWait);
    trigger.task.status = STREAM_STATUS_RUNNING;
    trigger.triggerType = STREAM_TRIGGER_SLIDING;
    trigger.realtimeStarted = 1;
    trigger.historyCalcStarted = true;
    trigger.pRealtimeContext = &context;
    context.pTask = &trigger;
    context.sessionId = 1;  // Realtime session ID used by STRIGGER_CTRL_START.
  }

  void TearDown() override {
    stub.reset(tmsgSendReq);
    stub.reset(tdListAppend);
    gStreamMgmt.getSynEpset = savedEndpoint;
    tSimpleHashCleanup(trigger.pHistoryCutoffTime);
    SnodeMessageTest::TearDown();
  }

  int32_t resume() {
    SSTriggerCtrlRequest request = {};
    request.type = STRIGGER_CTRL_START;
    request.streamId = kStreamId;
    request.taskId = trigger.task.taskId;
    request.sessionId = context.sessionId;
    int32_t length = tSerializeSTriggerCtrlRequest(nullptr, 0, &request);
    EXPECT_GT(length, 0);
    if (length <= 0) return TSDB_CODE_INVALID_MSG;
    std::vector<char> body(length);
    int32_t           encoded = tSerializeSTriggerCtrlRequest(body.data(), length, &request);
    EXPECT_EQ(encoded, length);
    if (encoded != length) return TSDB_CODE_INVALID_MSG;
    SRpcMsg message = {};
    message.msgType = TDMT_STREAM_TRIGGER_CTRL;
    message.pCont = body.data();
    message.contLen = length;
    return stTriggerTaskProcessRsp(&trigger.task, &message, &errorTaskId);
  }
};

TEST_F(SnodeCheckpointRecoveryTest, FailedPersistenceRetriesAndBecomesReadyAfterStorageRecovers) {
  // Legacy v1 payload: frame length, version, stream ID, format, reader/group counts.
  int32_t checkpoint[7] = {};
  checkpoint[0] = sizeof(checkpoint) - INT_BYTES;
  checkpoint[4] = 1;
  int32_t version = 1;
  memcpy(reinterpret_cast<char*>(checkpoint) + INT_BYTES, &version, sizeof(version));
  memcpy(reinterpret_cast<char*>(checkpoint) + 2 * INT_BYTES, &kStreamId, sizeof(kStreamId));
  ASSERT_EQ(streamWriteCheckPoint(kStreamId, checkpoint, sizeof(checkpoint)), 0);
  const auto blockedTemp = dataDir + "/snode/checkpoint/1234.tmp";
  ASSERT_EQ(taosMkDir(blockedTemp.c_str()), 0);

  version = 2;
  memcpy(reinterpret_cast<char*>(checkpoint) + INT_BYTES, &version, sizeof(version));
  ASSERT_NE(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, checkpoint, sizeof(checkpoint)), 0);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_RETRY);
  ASSERT_EQ(resume(), 0);
  ASSERT_EQ(checkpointRequests.size(), 1U);
  EXPECT_EQ(checkpointWaits, 1);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_PENDING);

  // An in-flight retry must finish before another request is sent.
  ASSERT_EQ(resume(), 0);
  EXPECT_EQ(checkpointRequests.size(), 1U);
  EXPECT_EQ(checkpointWaits, 2);
  ASSERT_NE(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, checkpoint, sizeof(checkpoint)), 0);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_RETRY);

  taosRemoveDir(blockedTemp.c_str());
  ASSERT_EQ(resume(), 0);
  ASSERT_EQ(checkpointRequests.size(), 2U);
  EXPECT_EQ(checkpointWaits, 3);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_PENDING);
  for (const auto& request : checkpointRequests) {
    ASSERT_EQ(request.size(), sizeof(SMsgHead) + sizeof(checkpoint));
    int32_t sentVersion = 0;
    memcpy(&sentVersion, request.data() + sizeof(SMsgHead) + INT_BYTES, sizeof(sentVersion));
    EXPECT_EQ(sentVersion, 1);
  }

  ASSERT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, checkpoint, sizeof(checkpoint)), 0);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_READY);
  EXPECT_EQ(trigger.task.status, STREAM_STATUS_RUNNING);
  void*   data = nullptr;
  int64_t length = 0;
  ASSERT_EQ(streamReadCheckPoint(kStreamId, &data, &length), 0);
  ASSERT_EQ(length, sizeof(checkpoint));
  ASSERT_NE(data, nullptr);
  EXPECT_EQ(memcmp(data, checkpoint, sizeof(checkpoint)), 0);
  taosMemoryFree(data);
  ASSERT_EQ(resume(), 0);
  EXPECT_TRUE(context.haveReadCheckpoint);
  EXPECT_EQ(trigger.checkpointVersion, version);
  EXPECT_EQ(context.status, STRIGGER_CONTEXT_FETCH_META);
  EXPECT_EQ(checkpointRequests.size(), 2U);
}

TEST_F(SnodeCheckpointRecoveryTest, PendingResponseDoesNotResendCheckpoint) {
  ASSERT_EQ(resume(), 0);
  ASSERT_EQ(resume(), 0);
  EXPECT_TRUE(checkpointRequests.empty());
  EXPECT_EQ(checkpointWaits, 2);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_PENDING);
}

TEST_F(SnodeCheckpointRecoveryTest, PeerWriteFailureRetriesAfterStorageRecovers) {
  int32_t checkpoint[7] = {};
  checkpoint[0] = sizeof(checkpoint) - INT_BYTES;
  checkpoint[1] = 1;
  checkpoint[4] = 1;
  memcpy(checkpoint + 2, &kStreamId, sizeof(kStreamId));
  const auto peerDir = dataDir + "/peer";
  ASSERT_EQ(taosMulMkDir((peerDir + "/snode/checkpoint").c_str()), 0);
  tstrncpy(tsDataDir, peerDir.c_str(), PATH_MAX);
  ASSERT_EQ(streamWriteCheckPoint(kStreamId, checkpoint, sizeof(checkpoint)), 0);
  const auto blockedTemp = peerDir + "/snode/checkpoint/1234.tmp";
  ASSERT_EQ(taosMkDir(blockedTemp.c_str()), 0);

  tstrncpy(tsDataDir, dataDir.c_str(), PATH_MAX);
  checkpoint[1] = 2;
  ASSERT_EQ(streamWriteCheckPoint(kStreamId, checkpoint, sizeof(checkpoint)), 0);
  ASSERT_EQ(streamSyncWriteCheckpoint(kStreamId, &checkpointEndpoint, nullptr, 0), 0);
  ASSERT_EQ(checkpointRequests.size(), 1U);

  // Execute the peer's actual request handler, then deliver its actual error response.
  tstrncpy(tsDataDir, peerDir.c_str(), PATH_MAX);
  auto&         request = checkpointRequests.front();
  const int32_t error = process(TDMT_STREAM_SYNC_CHECKPOINT, request.data(), request.size());
  ASSERT_NE(error, 0);
  ASSERT_EQ(responseCode, error);
  EXPECT_EQ(responseBody.size(), 16U);
  tstrncpy(tsDataDir, dataDir.c_str(), PATH_MAX);
  EXPECT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, responseBody.empty() ? nullptr : responseBody.data(),
                    responseBody.size(), responseCode),
            error);
  ASSERT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_RETRY);
  ASSERT_EQ(resume(), 0);
  ASSERT_EQ(checkpointRequests.size(), 2U);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_PENDING);
  ASSERT_EQ(resume(), 0);
  EXPECT_EQ(checkpointRequests.size(), 2U);

  taosRemoveDir(blockedTemp.c_str());
  tstrncpy(tsDataDir, peerDir.c_str(), PATH_MAX);
  auto& retry = checkpointRequests.back();
  ASSERT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT, retry.data(), retry.size()), 0);
  ASSERT_EQ(responseCode, 0);
  ASSERT_EQ(responseBody.size(), 16U);
  void*   data = nullptr;
  int64_t length = 0;
  ASSERT_EQ(streamReadCheckPoint(kStreamId, &data, &length), 0);
  ASSERT_EQ(length, sizeof(checkpoint));
  ASSERT_NE(data, nullptr);
  EXPECT_EQ(memcmp(data, checkpoint, sizeof(checkpoint)), 0);
  taosMemoryFree(data);

  tstrncpy(tsDataDir, dataDir.c_str(), PATH_MAX);
  ASSERT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, responseBody.data(), responseBody.size(), responseCode), 0);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_READY);
  ASSERT_EQ(resume(), 0);
  EXPECT_TRUE(context.haveReadCheckpoint);
  EXPECT_EQ(trigger.checkpointVersion, 2);
  EXPECT_EQ(context.status, STRIGGER_CONTEXT_FETCH_META);
  EXPECT_EQ(checkpointRequests.size(), 2U);
}

TEST_F(SnodeCheckpointRecoveryTest, MissingRetryPeerPropagatesToTriggerErrorHandling) {
  trigger.checkpointSyncState = STREAM_CHECKPOINT_SYNC_RETRY;
  gStreamMgmt.getSynEpset = noCheckpointEndpoint;
  EXPECT_EQ(resume(), TSDB_CODE_SNODE_NOT_FOUND);
  EXPECT_TRUE(checkpointRequests.empty());
  EXPECT_EQ(checkpointWaits, 0);
  EXPECT_EQ(errorTaskId, trigger.task.taskId);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_RETRY);
}

TEST_F(SnodeCheckpointRecoveryTest, CorruptPeerCheckpointFailsStartupWithoutRetrying) {
  int32_t checkpoint[7] = {};
  checkpoint[0] = sizeof(checkpoint) - INT_BYTES;
  checkpoint[1] = 2;
  checkpoint[4] = 1;
  memcpy(checkpoint + 2, &kStreamId, sizeof(kStreamId));
  ASSERT_EQ(streamWriteCheckPoint(kStreamId, checkpoint, sizeof(checkpoint)), 0);
  ASSERT_EQ(streamSyncWriteCheckpoint(kStreamId, &checkpointEndpoint, nullptr, 0), 0);
  ASSERT_EQ(checkpointRequests.size(), 1U);

  const auto peerDir = dataDir + "/peer";
  ASSERT_EQ(taosMulMkDir((peerDir + "/snode/checkpoint").c_str()), 0);
  const auto peerFile = peerDir + "/snode/checkpoint/1234.ck";
  TdFilePtr  fp = taosOpenFile(peerFile.c_str(), TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  ASSERT_NE(fp, nullptr);
  int64_t originalLength = sizeof(checkpoint);
  ASSERT_EQ(taosWriteFile(fp, &originalLength, sizeof(originalLength)), sizeof(originalLength));
  ASSERT_EQ(taosCloseFile(&fp), 0);
  tstrncpy(tsDataDir, peerDir.c_str(), PATH_MAX);
  auto& request = checkpointRequests.front();
  ASSERT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT, request.data(), request.size()), TSDB_CODE_FILE_CORRUPTED);
  ASSERT_EQ(responseCode, TSDB_CODE_FILE_CORRUPTED);
  ASSERT_EQ(responseBody.size(), 16U);
  int64_t fileSize = 0;
  ASSERT_EQ(taosStatFile(peerFile.c_str(), &fileSize, nullptr, nullptr), 0);
  EXPECT_EQ(fileSize, sizeof(originalLength));

  tstrncpy(tsDataDir, dataDir.c_str(), PATH_MAX);
  EXPECT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, responseBody.data(), responseBody.size(), responseCode),
            TSDB_CODE_FILE_CORRUPTED);
  const int32_t error = resume();
  EXPECT_EQ(error, TSDB_CODE_FILE_CORRUPTED);
  EXPECT_EQ(errorTaskId, trigger.task.taskId);
  // A late success must not clear the terminal startup error.
  EXPECT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, responseBody.data(), responseBody.size()), 0);
  EXPECT_EQ(resume(), TSDB_CODE_FILE_CORRUPTED);
  // Apply the same task-error callback used by the Trigger worker on a failed resume.
  streamHandleTaskError(kStreamId, errorTaskId, error);
  EXPECT_EQ(trigger.task.status, STREAM_STATUS_FAILED);
  EXPECT_EQ(trigger.task.errorCode, TSDB_CODE_FILE_CORRUPTED);
  EXPECT_FALSE(context.haveReadCheckpoint);
  EXPECT_EQ(checkpointWaits, 0);
  EXPECT_EQ(checkpointRequests.size(), 1U);
}

TEST_F(SnodeCheckpointRecoveryTest, CorruptBackgroundCheckpointDoesNotInvalidateReadyTask) {
  trigger.checkpointSyncState = STREAM_CHECKPOINT_SYNC_READY;
  int32_t sentinel[4] = {0, -1, 0, 0};
  memcpy(sentinel + 2, &kStreamId, sizeof(kStreamId));
  EXPECT_EQ(process(TDMT_STREAM_SYNC_CHECKPOINT_RSP, sentinel, sizeof(sentinel), TSDB_CODE_FILE_CORRUPTED),
            TSDB_CODE_FILE_CORRUPTED);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_READY);
  EXPECT_EQ(trigger.task.status, STREAM_STATUS_RUNNING);
  EXPECT_EQ(trigger.task.errorCode, 0);
}

TEST_F(SnodeCheckpointRecoveryTest, RetrySendFailurePropagatesToTriggerErrorHandling) {
  trigger.checkpointSyncState = STREAM_CHECKPOINT_SYNC_RETRY;
  checkpointSendCode = TSDB_CODE_RPC_BROKEN_LINK;
  EXPECT_EQ(resume(), TSDB_CODE_RPC_BROKEN_LINK);
  EXPECT_EQ(checkpointRequests.size(), 1U);
  EXPECT_EQ(checkpointWaits, 0);
  EXPECT_EQ(errorTaskId, trigger.task.taskId);
  EXPECT_EQ(trigger.checkpointSyncState, STREAM_CHECKPOINT_SYNC_PENDING);
}
}  // namespace

extern "C" {
int32_t __wrap_rpcSendResponse(const SRpcMsg* msg) {
  responseCode = msg->code;
  responseBody.clear();
  if (msg->pCont != nullptr && msg->contLen > 0) {
    const auto* body = static_cast<const char*>(msg->pCont);
    responseBody.assign(body, body + msg->contLen);
  }
  rpcFreeCont(msg->pCont);
  return 0;
}
void    __wrap_tmsgSendRsp(SRpcMsg* msg) { (void)__wrap_rpcSendResponse(msg); }
int32_t __wrap_stRunnerTaskExecute(SStreamRunnerTask*, SSTriggerCalcRequest*, int64_t) {
  ++executions;
  return TSDB_CODE_APP_ERROR;
}
int32_t __wrap_stRunnerTaskDropTable(SStreamRunnerTask*, SSTriggerDropRequest*) {
  ++executions;
  return TSDB_CODE_APP_ERROR;
}
}
