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
#include <iostream>
#include <thread>
#include "nodes.h"
#include "planner.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include <libs/transport/trpc.h>
#include "../../inc/mndStream.h"

extern "C" int32_t msmBuildTriggerDeployInfo(SMnode *pMnode, SStmStatus *pInfo, SStmTaskDeploy *pDeploy,
                                             SStreamObj *pStream);
extern "C" int32_t msmNormalHandleHbMsg(SStmGrpCtx *pCtx);

namespace {

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

}  // namespace

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
