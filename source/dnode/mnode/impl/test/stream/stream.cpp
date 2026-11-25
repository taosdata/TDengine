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
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "cJSON.h"
#include "functionMgt.h"
#include "nodes.h"
#include "planner.h"
#include "stub.h"
#include "systable.h"
#include "tdatablock.h"
#include "tjson.h"
extern "C" {
#include "tdTestSeam.h"
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include <libs/transport/trpc.h>
#include "../../../../../../include/libs/new-stream/stream.h"
#include "../../inc/mndDb.h"
#include "../../inc/mndDnode.h"
#include "../../inc/mndExtSource.h"
#include "../../inc/mndPrivilege.h"
#include "../../inc/mndSnode.h"
#include "../../inc/mndStb.h"
#include "../../inc/mndStream.h"
#include "../../inc/mndStreamRecalc.h"
#include "../../inc/mndSync.h"
#include "../../inc/mndTrans.h"
#include "../../inc/mndTxn.h"
#include "../../inc/mndUser.h"
#include "../../inc/mndVgroup.h"
#include "sdb.h"
#ifdef TD_ENTERPRISE
#include "../../../../../libs/new-stream/inc/streamInt.h"
#include "../../../../../libs/new-stream/inc/streamReaderExt.h"
#include "libs/extconnector/extConnector.h"
#include "libs/qcom/extTypeMap.h"
#endif

extern "C" int32_t msmBuildTriggerDeployInfo(SMnode *pMnode, SStmStatus *pInfo, SStmTaskDeploy *pDeploy,
                                             SStreamObj *pStream);
extern "C" void    msmStopStreamByError(int64_t streamId, SStmStatus *pStatus, int32_t errCode, int64_t currTs);
extern "C" int32_t msmNormalHandleHbMsgImpl(SStmGrpCtx *pCtx, bool *recalcStartDeferred, SArray **inputs);
extern "C" void    msmDrainStreamActions(SMnode *mnode);
extern "C" int32_t msmHandleTaskMgmtReq(SStmGrpCtx *pCtx, SStmTaskStatusMsg *pTask);
#ifdef TD_ENTERPRISE
extern "C" int32_t msmBuildExtOrigTblReaderSpec(const SExtSourceObj *pSource,
                                                  const SStreamExtOrigTblReaderReq *pReq,
                                                  const SExtTableMeta *pMeta,
                                                  SStreamExtTriggerSpec **ppSpec,
                                                  SStreamExtOrigTblReaderRsp *pRsp);
extern "C" int32_t msmPrepareExtOrigTblReader(int64_t streamId, SCMCreateStreamReq *create, const SExtSourceObj *source,
                                              const SStreamExtOrigTblReaderReq *req, const SExtTableMeta *meta,
                                              SStmTaskToDeployExt *entry, SStreamExtOrigTblReaderRsp *rsp);
extern "C" SStmTaskStatus *msmFindExtOrigTblReader(SStmStatus *status, int64_t registryEntryId);
extern "C" int32_t         msmPublishExtOrigTblReader(SStmGrpCtx *ctx, SStmStatus *status, SStreamObj *stream,
                                                      const SStreamExtOrigTblReaderReq *req, SStmTaskToDeployExt *entry,
                                                      SStmTaskStatus **slot, SStreamExtOrigTblReaderRsp *rsp);

static int32_t DeployExtReaderFromMeta(SStmGrpCtx *ctx, SStmStatus *status, SStreamObj *stream,
                                       const SExtSourceObj *source, const SStreamExtOrigTblReaderReq *req,
                                       const SExtTableMeta *meta, SStreamExtOrigTblReaderRsp *rsp) {
  SStmTaskToDeployExt entry = {};
  int32_t code = msmPrepareExtOrigTblReader(stream->pCreate->streamId, status->pCreate, source, req, meta, &entry, rsp);
  if (!code) {
    auto *reader = msmFindExtOrigTblReader(status, req->registryEntryId);
    code = msmPublishExtOrigTblReader(ctx, status, stream, req, &entry, &reader, rsp);
  }
  mstDestroySStmTaskToDeployExt(&entry);
  return rsp->code = code;
}
#endif
extern "C" int32_t msmNormalHandleStatusUpdate(SStmGrpCtx *pCtx);
extern "C" int32_t msmCheckDeployTrigReader(SStmGrpCtx *pCtx, SStmStatus *pStatus, SStmTaskStatusMsg *pTask,
                                            int32_t vgId, SStreamObj *stream);
extern "C" void    msmChkHandleTriggerOperations(SStmGrpCtx *pCtx, SStmTaskStatusMsg *pTask, SStmTaskStatus *pStatus);
extern "C" int32_t msmWatchRecordNewTask(SStmGrpCtx *pCtx, SStmTaskStatusMsg *pTask);
extern "C" int32_t msmAssignTaskSnodeId(SMnode *pMnode, SStreamObj *pStream, bool isStatic);
extern "C" void    msmDestroyRuntimeInfo(SMnode *pMnode);
extern "C" void    msmCheckLoopStreamMap(SMnode *pMnode);
extern "C" void    monReportStreamFailure(int64_t ts, int64_t streamId, const char *streamName, int32_t errorCode);

namespace {

constexpr int32_t kStreamRealtimeLagColumn = 10;
constexpr int32_t kStreamInputRateColumn = 11;
constexpr int32_t kStreamOutputRateColumn = 12;
constexpr int32_t kStreamResultLatencyColumn = 13;
constexpr int32_t kStreamHistoryProgressColumn = 14;
constexpr int32_t kStreamTimezoneColumn = 15;
constexpr int32_t kStreamFirstDayOfWeekColumn = 16;

int32_t gStreamHbDecodeCalls = 0;
int32_t gStreamHbResponseCode = 0;

int32_t allowStreamHbGrant(EGrantType) { return TSDB_CODE_SUCCESS; }

int32_t captureStreamHbDecode(SDecoder *, SStreamHbMsg *) {
  ++gStreamHbDecodeCalls;
  return TSDB_CODE_SUCCESS;
}

int32_t ignoreStreamHbBody(SMnode *, int64_t, SStreamHbMsg *, SRpcMsg *, SRpcMsg *) { return TSDB_CODE_SUCCESS; }

void captureStreamHbResponse(SRpcMsg *pRsp) { gStreamHbResponseCode = pRsp->code; }

class MndStreamHbInputTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gStreamHbDecodeCalls = 0;
    gStreamHbResponseCode = 0;
    stub_.set(grantCheckExpire, allowStreamHbGrant);
    stub_.set(tDecodeStreamHbMsg, captureStreamHbDecode);
    stub_.set(msmHandleStreamHbMsg, ignoreStreamHbBody);
    stub_.set(tmsgSendRsp, captureStreamHbResponse);
  }

  void TearDown() override {
    stub_.reset(tmsgSendRsp);
    stub_.reset(msmHandleStreamHbMsg);
    stub_.reset(tDecodeStreamHbMsg);
    stub_.reset(grantCheckExpire);
  }

  Stub stub_;
};

TEST_F(MndStreamHbInputTest, RejectsMissingOrShortHeaderBeforeDecode) {
  SStreamMsgGrpHeader header = {};
  SRpcMsg             msg = {};
  msg.pCont = &header;

  for (int32_t length : {0, 1, static_cast<int32_t>(sizeof(header)) - 1}) {
    msg.contLen = length;
    gStreamHbResponseCode = 0;
    ASSERT_EQ(mndProcessStreamHb(&msg), TSDB_CODE_INVALID_MSG);
    EXPECT_EQ(gStreamHbDecodeCalls, 0);
    EXPECT_EQ(gStreamHbResponseCode, TSDB_CODE_INVALID_MSG);
  }

  msg.pCont = nullptr;
  msg.contLen = 0;
  EXPECT_EQ(mndProcessStreamHb(&msg), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(gStreamHbDecodeCalls, 0);
  EXPECT_EQ(gStreamHbResponseCode, TSDB_CODE_INVALID_MSG);
}

TEST_F(MndStreamHbInputTest, RejectsHeaderWithoutBody) {
  stub_.reset(tDecodeStreamHbMsg);
  SStreamMsgGrpHeader header = {};
  SRpcMsg             msg = {};
  msg.pCont = &header;
  msg.contLen = sizeof(header);

  EXPECT_EQ(mndProcessStreamHb(&msg), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(gStreamHbResponseCode, TSDB_CODE_INVALID_MSG);
}

TEST_F(MndStreamHbInputTest, DisabledMnodeRejectsHeartbeatFromPeerWithExistingTask) {
  SStreamHbMsg hb = {};
  hb.dnodeId = hb.snodeId = 2;
  hb.streamGId = 1;
  hb.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(hb.pStreamStatus, nullptr);
  SStmTaskStatusMsg task = {};
  task.streamId = 1;
  task.taskId = 17;
  task.nodeId = hb.snodeId;
  task.type = STREAM_RUNNER_TASK;
  task.status = STREAM_STATUS_RUNNING;
  ASSERT_NE(taosArrayPush(hb.pStreamStatus, &task), nullptr);

  int32_t length = 0, code = 0;
  tEncodeSize(tEncodeStreamHbMsg, &hb, length, code);
  ASSERT_EQ(code, 0);
  SStreamMsgGrpHeader  header = {hb.streamGId};
  std::vector<uint8_t> frame(sizeof(header) + length);
  memcpy(frame.data(), &header, sizeof(header));
  SEncoder encoder = {};
  tEncoderInit(&encoder, frame.data() + sizeof(header), length);
  EXPECT_EQ(tEncodeStreamHbMsg(&encoder, &hb), length);
  tEncoderClear(&encoder);
  taosArrayDestroy(hb.pStreamStatus);

  SMnode  mnode = {};
  SRpcMsg msg = {};
  msg.msgType = TDMT_MND_STREAM_HEARTBEAT;
  msg.info.node = &mnode;
  msg.contLen = static_cast<int32_t>(frame.size());
  msg.pCont = frame.data();
  const bool savedDisable = tsDisableStream;
  tsDisableStream = true;
  EXPECT_EQ(mndProcessStreamHb(&msg), TSDB_CODE_MSG_NOT_PROCESSED);
  tsDisableStream = savedDisable;
  EXPECT_EQ(gStreamHbResponseCode, TSDB_CODE_MSG_NOT_PROCESSED);
  EXPECT_EQ(gStreamHbDecodeCalls, 0);
}

#ifdef TD_ENTERPRISE
Stub            *gSchemaThreadStub = nullptr;
int              gSchemaThreadCreates = 0;
std::atomic<int> gSchemaThreadExits{0};
void *(*gSchemaThreadMain)(void *) = nullptr;

void *countSchemaThreadExit(void *arg) {
  void *result = gSchemaThreadMain(arg);
  ++gSchemaThreadExits;
  return result;
}

int32_t failSecondSchemaThread(TdThread *tid, const TdThreadAttr *attr, void *(*main)(void *), void *arg) {
  if (++gSchemaThreadCreates == 2) return TSDB_CODE_OUT_OF_MEMORY;
  if (gSchemaThreadCreates == 1) gSchemaThreadMain = main;
  gSchemaThreadStub->reset(taosThreadCreate);
  int32_t code = taosThreadCreate(tid, attr, countSchemaThreadExit, arg);
  gSchemaThreadStub->set(taosThreadCreate, failSecondSchemaThread);
  return code;
}

TEST(MndStreamSchemaWorkerTest, FailedSecondThreadIsJoinedBeforeReopen) {
  SMnode mnode = {};
  Stub   stub;
  gSchemaThreadStub = &stub;
  gSchemaThreadCreates = 0;
  gSchemaThreadExits = 0;
  stub.set(taosThreadCreate, failSecondSchemaThread);
  const auto savedThreads = tsNumOfMnodeStreamMgmtThreads;
  tsNumOfMnodeStreamMgmtThreads = 2;

  EXPECT_EQ(msmInitSchemaWorker(&mnode), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(gSchemaThreadCreates, 2);
  EXPECT_EQ(gSchemaThreadExits.load(), 1);
  EXPECT_EQ(mnode.streamSchemaWorker.pool, nullptr);
  EXPECT_EQ(mnode.streamSchemaWorker.queue, nullptr);
  msmCleanupSchemaWorker(&mnode);  // mndExecSteps rolls back the failed step too.

  EXPECT_EQ(msmInitSchemaWorker(&mnode), 0);
  EXPECT_NE(mnode.streamSchemaWorker.queue, nullptr);
  EXPECT_EQ(gSchemaThreadCreates, 4);
  msmCleanupSchemaWorker(&mnode);
  EXPECT_EQ(gSchemaThreadExits.load(), 3);
  tsNumOfMnodeStreamMgmtThreads = savedThreads;
  stub.reset(taosThreadCreate);
  gSchemaThreadStub = nullptr;
}
#endif

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

bool   *gPrepareSyncStarted = nullptr;
int32_t gPrepareProposalCalls = 0;

void ignorePrepareSdbLock(SSdb *, int32_t) {}

SSdbRaw *failPrepareRawAllocation(ESdbType, int8_t, int32_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

int32_t failPrepareProposal(SMnode *, SSdbRaw *raw, int32_t transId) {
  ++gPrepareProposalCalls;
  EXPECT_TRUE(*gPrepareSyncStarted);
  EXPECT_EQ(raw->type, SDB_TRANS);
  EXPECT_EQ(raw->status, SDB_STATUS_READY);
  int32_t encodedId = 0;
  EXPECT_EQ(sdbGetRawInt32(raw, 0, &encodedId), TSDB_CODE_SUCCESS);
  EXPECT_EQ(encodedId, transId);
  return TSDB_CODE_MND_TRANS_SYNC_TIMEOUT;
}

class MndTransPrepareStatusTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedErrno_ = terrno;
    trans_ = createTestTrans();
    ASSERT_NE(trans_, nullptr);
    trans_->conflict = TRN_CONFLICT_NOTHING;
    trans_->exec = TRN_EXEC_PARALLEL;
    gPrepareSyncStarted = &syncStarted_;
    gPrepareProposalCalls = 0;
    stub_.set(sdbWriteLock, ignorePrepareSdbLock);
    stub_.set(sdbUnLock, ignorePrepareSdbLock);
    stub_.set(mndSyncPropose, failPrepareProposal);
  }

  void TearDown() override {
    // Older cppstub clear() leaves null entries that its destructor visits again.
    stub_.reset(sdbAllocRaw);
    stub_.reset(mndSyncPropose);
    stub_.reset(sdbUnLock);
    stub_.reset(sdbWriteLock);
    gPrepareSyncStarted = nullptr;
    mndTransDrop(trans_);
    terrno = savedErrno_;
  }

  SMnode  node_ = {};
  STrans *trans_ = nullptr;
  bool    syncStarted_ = true;
  int32_t savedErrno_ = 0;
  Stub    stub_;
};

TEST_F(MndTransPrepareStatusTest, CommitValidationFailureDoesNotStartSync) {
  EXPECT_EQ(mndTransPrepareWithSyncStatus(&node_, trans_, &syncStarted_), TSDB_CODE_MND_TRANS_CLOG_IS_NULL);
  EXPECT_FALSE(syncStarted_);
  EXPECT_EQ(gPrepareProposalCalls, 0);
}

TEST_F(MndTransPrepareStatusTest, EncodeAllocationFailureDoesNotStartSync) {
  mndTransSetChangeless(trans_);
  stub_.set(sdbAllocRaw, failPrepareRawAllocation);
  EXPECT_EQ(mndTransPrepareWithSyncStatus(&node_, trans_, &syncStarted_), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_FALSE(syncStarted_);
  EXPECT_EQ(gPrepareProposalCalls, 0);
}

TEST_F(MndTransPrepareStatusTest, ProposalFailureStillMarksSyncAsStarted) {
  mndTransSetChangeless(trans_);
  syncStarted_ = false;
  EXPECT_EQ(mndTransPrepareWithSyncStatus(&node_, trans_, &syncStarted_), TSDB_CODE_MND_TRANS_SYNC_TIMEOUT);
  EXPECT_TRUE(syncStarted_);
  EXPECT_EQ(gPrepareProposalCalls, 1);
}

STrans *gGroupProgressTrans = nullptr;
int32_t gGroupProgressSyncCalls = 0;
int32_t gGroupProgressCommitCalls = 0;
int32_t gGroupProgressSyncCode = TSDB_CODE_SUCCESS;

bool groupProgressIsLeader(SMnode *) { return true; }

int32_t captureGroupProgressProposal(SMnode *, SSdbRaw *, int32_t) {
  EXPECT_NE(gGroupProgressTrans, nullptr);
  if (gGroupProgressTrans == nullptr) {
    return TSDB_CODE_INTERNAL_ERROR;
  }
  if (gGroupProgressTrans->stage == TRN_STAGE_COMMIT) {
    ++gGroupProgressCommitCalls;
    return TSDB_CODE_MND_TRANS_SYNC_TIMEOUT;
  }
  ++gGroupProgressSyncCalls;
  return gGroupProgressSyncCode;
}

class MndTransGroupProgressTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedErrno_ = terrno;
    trans_ = createTestTrans();
    ASSERT_NE(trans_, nullptr);
    trans_->id = 1;
    trans_->stage = TRN_STAGE_REDO_ACTION;
    trans_->exec = TRN_EXEC_GROUP_PARALLEL;
    trans_->policy = TRN_POLICY_RETRY;
    trans_->actionPos = 1;
    trans_->redoGroupActions = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    trans_->groupActionPos = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    ASSERT_NE(trans_->redoGroupActions, nullptr);
    ASSERT_NE(trans_->groupActionPos, nullptr);

    for (int32_t groupId : {2, 3}) {
      STransAction action = {};
      action.id = groupId - 2;
      action.groupId = groupId;
      action.stage = TRN_STAGE_REDO_ACTION;
      action.actionType = TRANS_ACTION_MSG;
      action.msgType = TDMT_VND_TXN_COMMIT;
      action.msgSent = 1;
      action.msgReceived = 1;
      ASSERT_NE(taosArrayPush(trans_->redoActions, &action), nullptr);
    }
    for (int32_t groupId : {2, 3}) {
      SArray *actions = taosArrayInit(1, sizeof(STransAction *));
      ASSERT_NE(actions, nullptr);
      ASSERT_EQ(taosHashPut(trans_->redoGroupActions, &groupId, sizeof(groupId), &actions, sizeof(actions)), 0);
      auto *action = static_cast<STransAction *>(taosArrayGet(trans_->redoActions, groupId - 2));
      ASSERT_NE(taosArrayPush(actions, &action), nullptr);
      int32_t position = groupId == 2 ? 1 : 0;
      ASSERT_EQ(taosHashPut(trans_->groupActionPos, &groupId, sizeof(groupId), &position, sizeof(position)), 0);
    }

    gGroupProgressTrans = trans_;
    gGroupProgressSyncCalls = 0;
    gGroupProgressCommitCalls = 0;
    gGroupProgressSyncCode = TSDB_CODE_SUCCESS;
    stub_.set(mndIsLeader, groupProgressIsLeader);
    stub_.set(mndSyncPropose, captureGroupProgressProposal);
  }

  void TearDown() override {
    stub_.reset(mndSyncPropose);
    stub_.reset(mndIsLeader);
    gGroupProgressTrans = nullptr;
    mndTransDrop(trans_);
    terrno = savedErrno_;
  }

  STransAction *pendingAction() { return static_cast<STransAction *>(taosArrayGet(trans_->redoActions, 1)); }

  SMnode node_ = {};
  STrans *trans_ = nullptr;
  int32_t savedErrno_ = 0;
  Stub stub_;
};

TEST_F(MndTransGroupProgressTest, LastGroupAcknowledgementStartsCommitWithoutPullup) {
  mndTransExecute(&node_, trans_, true);
  EXPECT_EQ(trans_->actionPos, 2);
  EXPECT_EQ(trans_->stage, TRN_STAGE_COMMIT);
  EXPECT_EQ(gGroupProgressSyncCalls, 1);
  EXPECT_EQ(gGroupProgressCommitCalls, 1);
}

TEST_F(MndTransGroupProgressTest, PendingAcknowledgementDoesNotStartCommit) {
  STransAction *action = pendingAction();
  ASSERT_NE(action, nullptr);
  action->msgReceived = 0;
  mndTransExecute(&node_, trans_, true);
  EXPECT_EQ(trans_->actionPos, 1);
  EXPECT_EQ(trans_->stage, TRN_STAGE_REDO_ACTION);
  EXPECT_EQ(gGroupProgressSyncCalls, 0);
  EXPECT_EQ(gGroupProgressCommitCalls, 0);
}

TEST_F(MndTransGroupProgressTest, FailedAcknowledgementDoesNotStartCommit) {
  STransAction *action = pendingAction();
  ASSERT_NE(action, nullptr);
  action->errCode = TSDB_CODE_VND_INVALID_TABLE_ACTION;
  mndTransExecute(&node_, trans_, true);
  EXPECT_EQ(trans_->actionPos, 1);
  EXPECT_EQ(trans_->stage, TRN_STAGE_REDO_ACTION);
  EXPECT_EQ(gGroupProgressCommitCalls, 0);
}

TEST_F(MndTransGroupProgressTest, FailedProgressPersistenceDoesNotStartCommit) {
  gGroupProgressSyncCode = TSDB_CODE_MND_TRANS_SYNC_TIMEOUT;
  mndTransExecute(&node_, trans_, true);
  EXPECT_EQ(trans_->actionPos, 1);
  EXPECT_EQ(trans_->stage, TRN_STAGE_REDO_ACTION);
  EXPECT_EQ(gGroupProgressSyncCalls, 1);
  EXPECT_EQ(gGroupProgressCommitCalls, 0);
}

TEST_F(MndTransGroupProgressTest, UnsentNextActionStillWaitsForPullup) {
  STransAction *action = pendingAction();
  ASSERT_NE(action, nullptr);
  action->msgSent = 0;
  action->msgReceived = 0;
  mndTransExecute(&node_, trans_, true);
  EXPECT_EQ(trans_->actionPos, 1);
  EXPECT_EQ(trans_->stage, TRN_STAGE_REDO_ACTION);
  EXPECT_EQ(action->msgSent, 0);
  EXPECT_EQ(gGroupProgressSyncCalls, 0);
  EXPECT_EQ(gGroupProgressCommitCalls, 0);
}

// ---------------------------------------------------------------------------------------------
// Commit / rollback trans of a user txn whose participant vgroup no longer exists.
//
// A participant's vgroup disappears when its database is dropped, possibly after the commit / rollback
// trans was prepared. Its TDMT_VND_TXN_COMMIT / ROLLBACK action would then be retried for ever
// (TRN_POLICY_RETRY). mndTransSendSingleMsg therefore completes such an action without sending it - but
// only for the commit / rollback trans of a user txn, and only when the *mnode's own SDB* says the vgroup
// is gone. A vnode that merely answers "closed or removed" (offline, still starting, CLOSE VNODE, failed to
// load) is still in the SDB and must keep being retried: it still holds the txn's uncommitted entries.
// ---------------------------------------------------------------------------------------------

int32_t gVgoneSendCalls = 0;
int32_t gVgoneAcquireCalls = 0;
int32_t gVgoneAcquireErrno = TSDB_CODE_MND_VGROUP_NOT_EXIST;  // what mndAcquireVgroup reports when it finds nothing
bool    gVgoneVgroupPresent = false;
SVgObj  gVgoneVgroup = {};

bool vgoneIsLeader(SMnode *) { return true; }

SVgObj *vgoneAcquireVgroup(SMnode *, int32_t) {
  ++gVgoneAcquireCalls;
  if (gVgoneVgroupPresent) return &gVgoneVgroup;
  terrno = gVgoneAcquireErrno;
  return nullptr;
}

void vgoneReleaseVgroup(SMnode *, SVgObj *) {}

int32_t vgoneSendReq(const SEpSet *, SRpcMsg *pMsg) {
  ++gVgoneSendCalls;
  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = nullptr;
  return TSDB_CODE_SUCCESS;
}

// The trans engine syncs the trans to the other mnodes after each finished action and before it enters
// the COMMIT stage. Fail the proposal so the engine stops right there: the test only looks at what the
// redo stage did with the action, and must not need a raft group (same trick as MndTransGroupProgressTest).
int32_t gVgoneSyncCalls = 0;
int32_t vgoneSyncPropose(SMnode *, SSdbRaw *, int32_t) {
  ++gVgoneSyncCalls;
  return TSDB_CODE_MND_TRANS_SYNC_TIMEOUT;
}

class MndTransVnodeGoneTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedErrno_ = terrno;
    gVgoneSendCalls = 0;
    gVgoneSyncCalls = 0;
    gVgoneAcquireCalls = 0;
    gVgoneAcquireErrno = TSDB_CODE_MND_VGROUP_NOT_EXIST;
    gVgoneVgroupPresent = false;
    stub_.set(mndIsLeader, vgoneIsLeader);
    stub_.set(mndAcquireVgroup, vgoneAcquireVgroup);
    stub_.set(mndReleaseVgroup, vgoneReleaseVgroup);
    stub_.set(tmsgSendReq, vgoneSendReq);
    stub_.set(mndSyncPropose, vgoneSyncPropose);
  }

  void TearDown() override {
    stub_.reset(mndSyncPropose);
    stub_.reset(tmsgSendReq);
    stub_.reset(mndReleaseVgroup);
    stub_.reset(mndAcquireVgroup);
    stub_.reset(mndIsLeader);
    if (trans_ != nullptr) mndTransDrop(trans_);
    terrno = savedErrno_;
  }

  // One redo action (a commit or rollback message to vgId 7) in a trans of the given kind. The commit trans
  // of a user txn runs GROUP_PARALLEL (one group per vgId), the rollback trans runs plain PARALLEL; both
  // executors must honour the skip, so the tests run each of them.
  void Build(EOperType oper, tmsg_t msgType, ETrnExec exec = TRN_EXEC_PARALLEL) {
    trans_ = createTestTrans();
    ASSERT_NE(trans_, nullptr);
    trans_->id = 1;
    trans_->stage = TRN_STAGE_REDO_ACTION;
    trans_->exec = exec;
    trans_->policy = TRN_POLICY_RETRY;
    trans_->oper = oper;
    STransAction action = {};
    action.id = 0;
    action.groupId = 7;
    action.stage = TRN_STAGE_REDO_ACTION;
    action.actionType = TRANS_ACTION_MSG;
    action.msgType = msgType;
    action.contLen = 8;
    action.pCont = taosMemoryCalloc(1, action.contLen);
    ASSERT_NE(action.pCont, nullptr);
    ASSERT_NE(taosArrayPush(trans_->redoActions, &action), nullptr);
    if (exec == TRN_EXEC_GROUP_PARALLEL) {
      trans_->redoGroupActions = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
      trans_->groupActionPos = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
      ASSERT_NE(trans_->redoGroupActions, nullptr);
      ASSERT_NE(trans_->groupActionPos, nullptr);
      int32_t groupId = 7;
      SArray *actions = taosArrayInit(1, sizeof(STransAction *));
      ASSERT_NE(actions, nullptr);
      ASSERT_EQ(taosHashPut(trans_->redoGroupActions, &groupId, sizeof(groupId), &actions, sizeof(actions)), 0);
      STransAction *first = Action();
      ASSERT_NE(taosArrayPush(actions, &first), nullptr);
      int32_t pos = 0;
      ASSERT_EQ(taosHashPut(trans_->groupActionPos, &groupId, sizeof(groupId), &pos, sizeof(pos)), 0);
      trans_->groupParallelNum = 1;
    }
  }

  STransAction *Action() { return static_cast<STransAction *>(taosArrayGet(trans_->redoActions, 0)); }

  SMnode node_ = {};
  STrans *trans_ = nullptr;
  int32_t savedErrno_ = 0;
  Stub stub_;
};

// The case this exists for: user COMMIT, participant vgroup gone from the SDB -> done, nothing is sent.
TEST_F(MndTransVnodeGoneTest, UserCommitSkipsVgroupGoneFromSdb) {
  Build(MND_OPER_COMMIT_TXN, TDMT_VND_TXN_COMMIT);
  mndTransExecute(&node_, trans_, false);
  EXPECT_EQ(gVgoneSendCalls, 0);
  EXPECT_EQ(Action()->msgSent, 1);
  EXPECT_EQ(Action()->msgReceived, 1);
  EXPECT_EQ(Action()->errCode, 0);
  EXPECT_EQ(trans_->stage, TRN_STAGE_COMMIT) << "done means the trans moves on instead of retrying for ever";
}

// Same, through the executor the real commit trans uses (GROUP_PARALLEL): a finished action is what makes
// the engine persist progress and move on, so it must see the skipped action as finished.
TEST_F(MndTransVnodeGoneTest, UserCommitSkipsVgroupGoneFromSdbGroupParallel) {
  Build(MND_OPER_COMMIT_TXN, TDMT_VND_TXN_COMMIT, TRN_EXEC_GROUP_PARALLEL);
  mndTransExecute(&node_, trans_, false);
  EXPECT_EQ(gVgoneSendCalls, 0);
  EXPECT_EQ(Action()->msgSent, 1);
  EXPECT_EQ(Action()->msgReceived, 1);
  EXPECT_EQ(Action()->errCode, 0);
  EXPECT_GE(gVgoneSyncCalls, 1) << "the finished action must be acknowledged (progress persisted), not retried";
}

TEST_F(MndTransVnodeGoneTest, VgroupStillInSdbIsSentGroupParallel) {
  Build(MND_OPER_COMMIT_TXN, TDMT_VND_TXN_COMMIT, TRN_EXEC_GROUP_PARALLEL);
  gVgoneVgroupPresent = true;
  mndTransExecute(&node_, trans_, false);
  EXPECT_EQ(gVgoneSendCalls, 1);
  EXPECT_EQ(Action()->msgReceived, 0);
  EXPECT_EQ(gVgoneSyncCalls, 0);
}

TEST_F(MndTransVnodeGoneTest, UserRollbackSkipsVgroupGoneFromSdb) {
  Build(MND_OPER_ROLLBACK_TXN, TDMT_VND_TXN_ROLLBACK);
  mndTransExecute(&node_, trans_, false);
  EXPECT_EQ(gVgoneSendCalls, 0);
  EXPECT_EQ(Action()->errCode, 0);
  EXPECT_EQ(trans_->stage, TRN_STAGE_COMMIT);
}

// A vgroup the DROP DATABASE is removing right now (DROPPING) cannot come back either.
TEST_F(MndTransVnodeGoneTest, VgroupBeingDroppedCountsAsGone) {
  Build(MND_OPER_COMMIT_TXN, TDMT_VND_TXN_COMMIT);
  gVgoneAcquireErrno = TSDB_CODE_SDB_OBJ_DROPPING;
  mndTransExecute(&node_, trans_, false);
  EXPECT_EQ(gVgoneSendCalls, 0);
  EXPECT_EQ(Action()->errCode, 0);
}

// The point of keying on the SDB: a vgroup still in the SDB (offline / starting / closed / failed vnode) is
// NOT skipped, however the vnode would answer - the message is sent, so the txn's entries are not lost.
TEST_F(MndTransVnodeGoneTest, VgroupStillInSdbIsSent) {
  Build(MND_OPER_COMMIT_TXN, TDMT_VND_TXN_COMMIT);
  gVgoneVgroupPresent = true;
  mndTransExecute(&node_, trans_, false);
  EXPECT_EQ(gVgoneSendCalls, 1);
  EXPECT_EQ(Action()->msgSent, 1);
  EXPECT_EQ(Action()->msgReceived, 0) << "still waiting for the vnode's answer";
  EXPECT_NE(trans_->stage, TRN_STAGE_COMMIT);
}

// A vgroup that cannot be acquired for another reason (e.g. still CREATING) is not "gone": keep trying.
TEST_F(MndTransVnodeGoneTest, VgroupNotReadyForOtherReasonIsSent) {
  Build(MND_OPER_COMMIT_TXN, TDMT_VND_TXN_COMMIT);
  gVgoneAcquireErrno = TSDB_CODE_SDB_OBJ_CREATING;
  mndTransExecute(&node_, trans_, false);
  EXPECT_EQ(gVgoneSendCalls, 1);
  EXPECT_EQ(Action()->msgReceived, 0);
}

// Nothing leaks to other trans: same message type, same missing vgroup, but not a user txn's trans
// (e.g. the orphan-txn clean-up trans, which carries no txn oper) -> sent as before.
TEST_F(MndTransVnodeGoneTest, OtherTransIsNeverSkipped) {
  Build(MND_OPER_BEGIN_TXN, TDMT_VND_TXN_COMMIT);
  mndTransExecute(&node_, trans_, false);
  EXPECT_EQ(gVgoneSendCalls, 1);
  EXPECT_EQ(Action()->msgReceived, 0);
  EXPECT_EQ(gVgoneAcquireCalls, 0) << "must not even look at the SDB for a trans that is not a user commit / rollback";
}

TEST_F(MndTransVnodeGoneTest, OtherMessageTypeOfAUserTxnTransIsNeverSkipped) {
  Build(MND_OPER_COMMIT_TXN, TDMT_VND_CREATE_STB);
  mndTransExecute(&node_, trans_, false);
  EXPECT_EQ(gVgoneSendCalls, 1);
  EXPECT_EQ(Action()->msgReceived, 0);
  EXPECT_EQ(gVgoneAcquireCalls, 0);
}

// notSend means "only record, do not send"; the skip must not run there (it would mark the action done).
TEST_F(MndTransVnodeGoneTest, NotSendLeavesActionUntouched) {
  Build(MND_OPER_COMMIT_TXN, TDMT_VND_TXN_COMMIT);
  mndTransExecute(&node_, trans_, true);
  EXPECT_EQ(gVgoneSendCalls, 0);
  EXPECT_EQ(Action()->msgSent, 0);
  EXPECT_EQ(Action()->msgReceived, 0);
}

STxnObj *gShadowRemovalTxn = nullptr;

void *acquireShadowRemovalTxn(SSdb *, ESdbType type, const void *key) {
  return type == SDB_TXN && *static_cast<const txn_id_t *>(key) == gShadowRemovalTxn->id ? gShadowRemovalTxn : nullptr;
}

void releaseShadowRemovalTxn(SSdb *, void *) {}

class MndStreamShadowRemovalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedErrno_ = terrno;
    txn_.id = static_cast<txn_id_t>(1) << 32;
    txn_.pShadowOps = taosArrayInit(3, sizeof(SMndShadowOp));
    ASSERT_NE(txn_.pShadowOps, nullptr);
    node_.txnMgmt.pStbConflictMap =
        taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    ASSERT_NE(node_.txnMgmt.pStbConflictMap, nullptr);
    gShadowRemovalTxn = &txn_;
    stub_.set(sdbAcquire, acquireShadowRemovalTxn);
    stub_.set(sdbRelease, releaseShadowRemovalTxn);
  }

  void TearDown() override {
    stub_.reset(sdbRelease);
    stub_.reset(sdbAcquire);
    gShadowRemovalTxn = nullptr;
    taosArrayDestroy(txn_.pShadowOps);
    taosHashCleanup(node_.txnMgmt.pStbConflictMap);
    terrno = savedErrno_;
  }

  SMnode  node_ = {};
  STxnObj txn_ = {};
  int32_t savedErrno_ = 0;
  Stub    stub_;
};

TEST_F(MndStreamShadowRemovalTest, RemovesLatestExactMatchAndPreservesOtherOwners) {
  SMndShadowOp op = {};
  op.opType = MND_SHADOW_OP_CREATE_STREAM;
  op.uid = 7;
  tstrncpy(op.name, "1.db.stream", sizeof(op.name));
  ASSERT_NE(taosArrayPush(txn_.pShadowOps, &op), nullptr);
  op.opType = MND_SHADOW_OP_CREATE_STB;
  ASSERT_NE(taosArrayPush(txn_.pShadowOps, &op), nullptr);
  op.opType = MND_SHADOW_OP_CREATE_STREAM;
  ASSERT_NE(taosArrayPush(txn_.pShadowOps, &op), nullptr);
  ASSERT_EQ(taosHashPut(node_.txnMgmt.pStbConflictMap, op.name, strlen(op.name), &txn_.id, sizeof(txn_.id)), 0);

  mndTxnRemoveShadowOp(&node_, txn_.id, op.opType, op.name, op.uid);
  ASSERT_EQ(taosArrayGetSize(txn_.pShadowOps), 2);
  EXPECT_EQ(static_cast<SMndShadowOp *>(taosArrayGet(txn_.pShadowOps, 0))->opType, MND_SHADOW_OP_CREATE_STREAM);
  EXPECT_EQ(static_cast<SMndShadowOp *>(taosArrayGet(txn_.pShadowOps, 1))->opType, MND_SHADOW_OP_CREATE_STB);
  auto *owner = static_cast<txn_id_t *>(taosHashGet(node_.txnMgmt.pStbConflictMap, op.name, strlen(op.name)));
  ASSERT_NE(owner, nullptr);
  EXPECT_EQ(*owner, txn_.id);

  mndTxnRemoveShadowOp(&node_, txn_.id, op.opType, op.name, op.uid + 1);
  EXPECT_EQ(taosArrayGetSize(txn_.pShadowOps), 2);
  mndTxnRemoveShadowOp(&node_, txn_.id, op.opType, op.name, op.uid);
  ASSERT_EQ(taosArrayGetSize(txn_.pShadowOps), 1);
  EXPECT_EQ(static_cast<SMndShadowOp *>(taosArrayGet(txn_.pShadowOps, 0))->opType, MND_SHADOW_OP_CREATE_STB);

  const txn_id_t otherOwner = txn_.id + 1;
  ASSERT_EQ(taosHashPut(node_.txnMgmt.pStbConflictMap, op.name, strlen(op.name), &otherOwner, sizeof(otherOwner)), 0);
  mndTxnRemoveShadowOp(&node_, txn_.id, MND_SHADOW_OP_CREATE_STB, op.name, op.uid);
  EXPECT_EQ(txn_.pShadowOps, nullptr);
  owner = static_cast<txn_id_t *>(taosHashGet(node_.txnMgmt.pStbConflictMap, op.name, strlen(op.name)));
  ASSERT_NE(owner, nullptr);
  EXPECT_EQ(*owner, otherOwner);
}

class MndTxnPendingStreamCreateTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedErrno_ = terrno;
    SSdbOpt options = {};
    options.path = TD_TMP_DIR_PATH "stream_create_pending";
    options.pMnode = &node_;
    taosRemoveDir(options.path);
    node_.pSdb = sdbInit(&options);
    ASSERT_NE(node_.pSdb, nullptr);
    ASSERT_EQ(mndInitTrans(&node_), TSDB_CODE_SUCCESS);
    ASSERT_EQ(mndInitTxn(&node_), TSDB_CODE_SUCCESS);
    ASSERT_EQ(mndInitStb(&node_), TSDB_CODE_SUCCESS);
    // Recovery scans stream markers without starting the stream runtime.
    SSdbTable table = {};
    table.sdbType = SDB_STREAM;
    table.keyType = SDB_KEY_BINARY;
    ASSERT_EQ(sdbSetTable(node_.pSdb, table), TSDB_CODE_SUCCESS);
  }

  void TearDown() override {
    if (node_.pSdb != nullptr) sdbCleanup(node_.pSdb);
    mndCleanupTxn(&node_);
    taosRemoveDir(TD_TMP_DIR_PATH "stream_create_pending");
    terrno = savedErrno_;
  }

  bool WriteRaw(SSdbRaw *raw, ESdbStatus status) {
    if (raw == nullptr) return false;
    if (sdbSetRawStatus(raw, status) != TSDB_CODE_SUCCESS) {
      sdbFreeRaw(raw);
      return false;
    }
    return sdbWrite(node_.pSdb, raw) == TSDB_CODE_SUCCESS;
  }

  bool WritePending(tmsg_t origin = TDMT_MND_CREATE_STREAM, ESdbStatus status = SDB_STATUS_READY) {
    STrans *trans = createTestTrans();
    if (trans == nullptr) return false;
    auto *pending = static_cast<SMndTxnStreamCreate *>(taosMemoryCalloc(1, sizeof(SMndTxnStreamCreate)));
    if (pending == nullptr) {
      mndTransDrop(trans);
      return false;
    }
    pending->txnId = txnId_;
    tstrncpy(pending->name, name_, sizeof(pending->name));
    trans->id = 7;
    trans->stage = TRN_STAGE_REDO_ACTION;
    trans->originRpcType = origin;
    mndTransSetUserData(trans, pending, sizeof(*pending));
    SSdbRaw *raw = mndTransEncode(trans);
    mndTransDrop(trans);
    // sdbWrite runs the registered real decoder and transfers ownership to SDB.
    return WriteRaw(raw, status);
  }

  bool WriteUserTxn() {
    STxnObj txn = {};
    txn.id = txnId_;
    txn.stage = UTXN_STAGE_ACTIVE;
    return WriteRaw(node_.pSdb->encodeFps[SDB_TXN](&txn), SDB_STATUS_READY);
  }

  void ExpectRecoveryState(bool deferred) {
    auto *txn = static_cast<STxnObj *>(sdbAcquire(node_.pSdb, SDB_TXN, &txnId_));
    ASSERT_NE(txn, nullptr);
    EXPECT_EQ(txn->stage, UTXN_STAGE_ACTIVE);
    EXPECT_EQ(txn->shadowRebuildPending, deferred);
    EXPECT_EQ(txn->pShadowOps, nullptr);
    sdbRelease(node_.pSdb, txn);
  }

  SMnode         node_ = {};
  int32_t        savedErrno_ = 0;
  const txn_id_t txnId_ = static_cast<txn_id_t>(1) << 32;
  const char    *name_ = "1.db.pending_stream";
};

TEST_F(MndTxnPendingStreamCreateTest, RoundTripPreservesBlockersWithoutRuntimeShadow) {
  ASSERT_TRUE(WriteUserTxn());
  ASSERT_TRUE(WritePending());
  STrans *stored = mndAcquireTrans(&node_, 7);
  ASSERT_NE(stored, nullptr);
  EXPECT_EQ(stored->originRpcType, TDMT_MND_CREATE_STREAM);
  EXPECT_EQ(stored->userDataLen, sizeof(SMndTxnStreamCreate));
  EXPECT_NE(stored->userData, nullptr);
  SMndTxnStreamCreate recovered = {};
  if (stored->userData != nullptr && stored->userDataLen == sizeof(recovered)) {
    memcpy(&recovered, stored->userData, sizeof(recovered));
  }
  mndReleaseTrans(&node_, stored);
  EXPECT_EQ(recovered.txnId, txnId_);
  EXPECT_STREQ(recovered.name, name_);
  ExpectRecoveryState(false);

  EXPECT_EQ(mndTxnCheckPendingStreamCreate(&node_, txnId_), TSDB_CODE_MND_TRANS_CONFLICT);
  EXPECT_EQ(mndTxnCheckPendingStreamCreate(&node_, txnId_ + 1), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mndTxnCheckStreamConflict(&node_, name_, txnId_), TSDB_CODE_TXN_RESOURCE_BUSY);
  EXPECT_EQ(mndTxnCheckStreamConflict(&node_, name_, txnId_ + 1), TSDB_CODE_TXN_RESOURCE_BUSY);
  EXPECT_EQ(mndTxnCheckStreamConflict(&node_, "1.db.other_stream", txnId_), TSDB_CODE_SUCCESS);
}

TEST_F(MndTxnPendingStreamCreateTest, IgnoresOtherRpcTypesAndClearsBlockersWhenPendingIsDeleted) {
  ASSERT_TRUE(WritePending(TDMT_MND_CREATE_STB));
  EXPECT_EQ(mndTxnCheckPendingStreamCreate(&node_, txnId_), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mndTxnCheckStreamConflict(&node_, name_, txnId_ + 1), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(WritePending(TDMT_MND_CREATE_STB, SDB_STATUS_DROPPED));

  ASSERT_TRUE(WritePending());
  EXPECT_EQ(mndTxnCheckPendingStreamCreate(&node_, txnId_), TSDB_CODE_MND_TRANS_CONFLICT);
  EXPECT_EQ(mndTxnCheckStreamConflict(&node_, name_, txnId_), TSDB_CODE_TXN_RESOURCE_BUSY);
  ASSERT_TRUE(WritePending(TDMT_MND_CREATE_STREAM, SDB_STATUS_DROPPED));
  EXPECT_EQ(mndTxnCheckPendingStreamCreate(&node_, txnId_), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mndTxnCheckStreamConflict(&node_, name_, txnId_), TSDB_CODE_SUCCESS);
}

TEST_F(MndTxnPendingStreamCreateTest, RestoreDefersShadowRebuildUntilPendingIsDeleted) {
  ASSERT_TRUE(WriteUserTxn());
  ASSERT_TRUE(WritePending());
  ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);
  ExpectRecoveryState(true);
  EXPECT_EQ(mndTxnCheckPendingStreamCreate(&node_, txnId_), TSDB_CODE_MND_TRANS_CONFLICT);

  ASSERT_TRUE(WritePending(TDMT_MND_CREATE_STREAM, SDB_STATUS_DROPPED));
  ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);
  ExpectRecoveryState(false);
  EXPECT_EQ(mndTxnCheckPendingStreamCreate(&node_, txnId_), TSDB_CODE_SUCCESS);
}

class MndTxnDeferredSchemaTest : public MndTxnPendingStreamCreateTest {
 protected:
  void SetUp() override {
    MndTxnPendingStreamCreateTest::SetUp();
    ASSERT_EQ(mndInitDb(&node_), TSDB_CODE_SUCCESS);
    SDbObj db = {};
    tstrncpy(db.name, "1.db", sizeof(db.name));
    db.uid = 100;
    ASSERT_TRUE(WriteRaw(mndDbActionEncode(&db), SDB_STATUS_READY));
    stub_.set(mndAcquireUser, AcquireUser);
    stub_.set(mndReleaseUser, ReleaseUser);
    // Keep validation, shadow registration and schema responses real; omit Raft submission.
    stub_.set(mndTransPrepare, PrepareTrans);
    ASSERT_TRUE(WriteUserTxn());
    ASSERT_TRUE(WriteAlterMarker());
    ASSERT_TRUE(WritePending());
    ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);
    ExpectRecoveryState(true);
  }

  void TearDown() override {
    stub_.reset(mndTransPrepare);
    stub_.reset(mndReleaseUser);
    stub_.reset(mndAcquireUser);
    MndTxnPendingStreamCreateTest::TearDown();
  }

  static int32_t AcquireUser(SMnode *, const char *, SUserObj **user) {
    static SUserObj root = {};
    root.superUser = 1;
    root.enable = 1;
    root.passwordLifeTime = -1;
    *user = &root;
    return TSDB_CODE_SUCCESS;
  }

  static void    ReleaseUser(SMnode *, SUserObj *) {}
  static int32_t PrepareTrans(SMnode *, STrans *) { return TSDB_CODE_SUCCESS; }

  bool WriteAlterMarker() {
    SSchema columns[2] = {};
    columns[0].type = TSDB_DATA_TYPE_TIMESTAMP;
    columns[0].bytes = 8;
    columns[0].colId = 1;
    tstrncpy(columns[0].name, "ts", sizeof(columns[0].name));
    columns[1].type = TSDB_DATA_TYPE_INT;
    columns[1].bytes = 4;
    columns[1].colId = 2;
    tstrncpy(columns[1].name, "v", sizeof(columns[1].name));
    SSchema tag = {};
    tag.type = TSDB_DATA_TYPE_INT;
    tag.bytes = 4;
    tag.colId = 3;
    tstrncpy(tag.name, "t", sizeof(tag.name));
    SColCmpr compression[2] = {};
    compression[0].id = 1;
    compression[1].id = 2;

    SMAlterStbReq alter = {};
    tstrncpy(alter.name, "1.db.stb", sizeof(alter.name));
    alter.alterType = TSDB_ALTER_TABLE_ADD_COLUMN;
    alter.numOfFields = 1;
    alter.txnId = txnId_;
    alter.securityLevel = -1;
    alter.pFields = taosArrayInit(1, sizeof(SField));
    if (alter.pFields == nullptr) return false;
    SField field = {};
    field.type = TSDB_DATA_TYPE_INT;
    field.bytes = 4;
    tstrncpy(field.name, "x", sizeof(field.name));
    taosArrayPush(alter.pFields, &field);
    const int32_t     length = tSerializeSMAlterStbReq(nullptr, 0, &alter);
    const int32_t     count = 1;
    std::vector<char> chain(2 * sizeof(int32_t) + length);
    memcpy(chain.data(), &count, sizeof(count));
    memcpy(chain.data() + sizeof(count), &length, sizeof(length));
    tSerializeSMAlterStbReq(chain.data() + 2 * sizeof(int32_t), length, &alter);
    tFreeSMAltertbReq(&alter);

    SStbObj stb = {};
    tstrncpy(stb.name, "1.db.stb", sizeof(stb.name));
    tstrncpy(stb.db, "1.db", sizeof(stb.db));
    stb.uid = 101;
    stb.dbUid = 100;
    stb.numOfColumns = 2;
    stb.numOfTags = 1;
    stb.nextColId = 4;
    stb.pColumns = columns;
    stb.pTags = &tag;
    stb.pCmpr = compression;
    stb.txnId = txnId_;
    stb.txnStatus = META_TXN_PRE_ALTER;
    stb.pTxnAlterReqs = chain.data();
    stb.txnAlterReqsLen = static_cast<int32_t>(chain.size());
    return WriteRaw(mndStbActionEncode(&stb), SDB_STATUS_READY);
  }

  template <typename Request>
  int32_t SendRequest(tmsg_t type, int32_t (*serialize)(void *, int32_t, Request *), Request *request,
                      STableMetaRsp *meta = nullptr) {
    const int32_t length = serialize(nullptr, 0, request);
    if (length <= 0) return TSDB_CODE_INVALID_MSG;
    std::vector<char> data(length);
    if (serialize(data.data(), length, request) != length) return TSDB_CODE_INVALID_MSG;
    SRpcMsg rpc = {};
    rpc.info.node = &node_;
    rpc.msgType = type;
    rpc.pCont = data.data();
    rpc.contLen = length;
    int32_t code = node_.msgFp[TMSG_INDEX(type)](&rpc);
    if (code == 0 && meta != nullptr) {
      code = tDeserializeSTableMetaRsp(rpc.info.rsp, rpc.info.rspLen, meta);
    }
    if (code != 0) EXPECT_EQ(rpc.info.rsp, nullptr);
    rpcFreeCont(rpc.info.rsp);
    return code;
  }

  int32_t ReadMeta(txn_id_t txnId, STableMetaRsp *meta) {
    STableInfoReq request = {};
    tstrncpy(request.dbFName, "1.db", sizeof(request.dbFName));
    tstrncpy(request.tbName, "stb", sizeof(request.tbName));
    request.txnId = txnId;
    return SendRequest(TDMT_MND_TABLE_META, tSerializeSTableInfoReq, &request, meta);
  }

  int32_t DropColumn() {
    SMAlterStbReq request = {};
    tstrncpy(request.name, "1.db.stb", sizeof(request.name));
    request.alterType = TSDB_ALTER_TABLE_DROP_COLUMN;
    request.numOfFields = 1;
    request.txnId = txnId_;
    request.securityLevel = -1;
    request.pFields = taosArrayInit(1, sizeof(SField));
    SField field = {};
    tstrncpy(field.name, "x", sizeof(field.name));
    taosArrayPush(request.pFields, &field);
    const int32_t code = SendRequest(TDMT_MND_ALTER_STB, tSerializeSMAlterStbReq, &request);
    tFreeSMAltertbReq(&request);
    return code;
  }

  Stub stub_;
};

TEST_F(MndTxnDeferredSchemaTest, GetterRejectsPendingAndRecoversPersistedAlter) {
  SArray *ops = nullptr;
  EXPECT_EQ(mndTxnGetAlterOpsForStb(&node_, txnId_, "1.db.stb", &ops), TSDB_CODE_MND_TRANS_CONFLICT);
  EXPECT_EQ(ops, nullptr);
  taosArrayDestroy(ops);
  ASSERT_TRUE(WritePending(TDMT_MND_CREATE_STREAM, SDB_STATUS_DROPPED));

  ASSERT_EQ(mndTxnGetAlterOpsForStb(&node_, txnId_, "1.db.stb", &ops), TSDB_CODE_SUCCESS);
  ASSERT_NE(ops, nullptr);
  EXPECT_EQ(taosArrayGetSize(ops), 1);
  auto         *op = static_cast<SMndShadowOp *>(taosArrayGet(ops, 0));
  SMAlterStbReq alter = {};
  EXPECT_EQ(tDeserializeSMAlterStbReq(op->pReqData, op->reqDataLen, &alter), 0);
  EXPECT_EQ(alter.alterType, TSDB_ALTER_TABLE_ADD_COLUMN);
  ASSERT_EQ(taosArrayGetSize(alter.pFields), 1);
  EXPECT_STREQ(static_cast<SField *>(taosArrayGet(alter.pFields, 0))->name, "x");
  tFreeSMAltertbReq(&alter);
  taosArrayDestroy(ops);
}

TEST_F(MndTxnDeferredSchemaTest, MetadataRejectsPendingThenReturnsRecoveredColumn) {
  STableMetaRsp meta = {};
  EXPECT_EQ(ReadMeta(txnId_, &meta), TSDB_CODE_MND_TRANS_CONFLICT);
  tFreeSTableMetaRsp(&meta);
  ASSERT_TRUE(WritePending(TDMT_MND_CREATE_STREAM, SDB_STATUS_DROPPED));

  ASSERT_EQ(ReadMeta(txnId_, &meta), TSDB_CODE_SUCCESS);
  EXPECT_EQ(meta.numOfColumns, 3);
  EXPECT_STREQ(meta.pSchemas[2].name, "x");
  tFreeSTableMetaRsp(&meta);
  ASSERT_EQ(ReadMeta(0, &meta), TSDB_CODE_SUCCESS);
  EXPECT_EQ(meta.numOfColumns, 2);
  tFreeSTableMetaRsp(&meta);
}

TEST_F(MndTxnDeferredSchemaTest, AlterValidationRecoversPriorColumn) {
  EXPECT_EQ(DropColumn(), TSDB_CODE_MND_TRANS_CONFLICT);
  ASSERT_TRUE(WritePending(TDMT_MND_CREATE_STREAM, SDB_STATUS_DROPPED));
  EXPECT_EQ(DropColumn(), TSDB_CODE_ACTION_IN_PROGRESS);

  STableMetaRsp meta = {};
  ASSERT_EQ(ReadMeta(txnId_, &meta), TSDB_CODE_SUCCESS);
  EXPECT_EQ(meta.numOfColumns, 2);
  tFreeSTableMetaRsp(&meta);
}

TEST_F(MndTxnDeferredSchemaTest, ConcurrentReadsRecoverOneCompleteAlterList) {
  ASSERT_TRUE(WritePending(TDMT_MND_CREATE_STREAM, SDB_STATUS_DROPPED));
  std::atomic<bool>        start(false);
  std::vector<std::thread> readers;
  for (int i = 0; i < 8; ++i) {
    readers.emplace_back([this, &start] {
      while (!start.load()) std::this_thread::yield();
      SArray *ops = nullptr;
      EXPECT_EQ(mndTxnGetAlterOpsForStb(&node_, txnId_, "1.db.stb", &ops), TSDB_CODE_SUCCESS);
      EXPECT_EQ(taosArrayGetSize(ops), 1);
      taosArrayDestroy(ops);
    });
  }
  start.store(true);
  for (auto &reader : readers) reader.join();

  auto *txn = static_cast<STxnObj *>(sdbAcquire(node_.pSdb, SDB_TXN, &txnId_));
  ASSERT_NE(txn, nullptr);
  EXPECT_FALSE(txn->shadowRebuildPending);
  EXPECT_EQ(taosArrayGetSize(txn->pShadowOps), 1);
  sdbRelease(node_.pSdb, txn);
}

void destroyRecoveredRunnerLists(SStmStatus *pStatus) {
  for (int32_t i = 0; i < MND_STREAM_RUNNER_DEPLOY_NUM; ++i) {
    taosArrayDestroy(pStatus->runners[i]);
    pStatus->runners[i] = nullptr;
  }
}

void destroyTrigOReaders(SStmStatus *pStatus) {
  if (pStatus == nullptr || pStatus->trigOReaders == nullptr) {
    return;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pStatus->trigOReaders); ++i) {
    SArray *readerList = static_cast<SArray *>(taosArrayGetP(pStatus->trigOReaders, i));
    taosArrayDestroy(readerList);
  }
  taosArrayDestroy(pStatus->trigOReaders);
  pStatus->trigOReaders = nullptr;
#ifdef TD_ENTERPRISE
  taosHashCleanup(pStatus->retiredExtReaders);
  pStatus->retiredExtReaders = nullptr;
#endif
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

static int32_t handleHeartbeatTaskMgmtReq(SStmGrpCtx *ctx, SStmTaskStatusMsg *task) {
  taosRLockLatch(&mStreamMgmt.heartbeatLock);
  taosRLockLatch(&mStreamMgmt.runtimeLock);
  taosWLockLatch(&mStreamMgmt.actionQLock);
  int32_t code = msmHandleTaskMgmtReq(ctx, task);
  taosWUnLockLatch(&mStreamMgmt.actionQLock);
  taosRUnLockLatch(&mStreamMgmt.runtimeLock);
  taosRUnLockLatch(&mStreamMgmt.heartbeatLock);
  return code;
}

class MndStreamHeartbeatTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedActionQ_ = mStreamMgmt.actionQ;
    savedStreamMap_ = mStreamMgmt.streamMap;
    savedDnodeMap_ = mStreamMgmt.dnodeMap;
    savedTaskMap_ = mStreamMgmt.taskMap;
    savedActionQLock_ = mStreamMgmt.actionQLock;
    savedToDeployVgTaskNum_ = mStreamMgmt.toDeployVgTaskNum;
    savedToDeploySnodeTaskNum_ = mStreamMgmt.toDeploySnodeTaskNum;
    savedCurrentTs_ = mStreamMgmt.hCtx.currentTs;

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
    mStreamMgmt.taskMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    ASSERT_NE(mStreamMgmt.taskMap, nullptr);
  }

  void TearDown() override {
    SStmQNode *pNode = nullptr;
    while (mndStreamActionDequeue(&queue_, &pNode)) {
    }
    taosMemoryFreeClear(queue_.head);
    queue_.tail = nullptr;

    taosHashCleanup(mStreamMgmt.dnodeMap);
    taosHashCleanup(mStreamMgmt.taskMap);
    taosHashCleanup(mStreamMgmt.streamMap);
    mStreamMgmt.actionQ = savedActionQ_;
    mStreamMgmt.streamMap = savedStreamMap_;
    mStreamMgmt.dnodeMap = savedDnodeMap_;
    mStreamMgmt.taskMap = savedTaskMap_;
    mStreamMgmt.actionQLock = savedActionQLock_;
    mStreamMgmt.toDeployVgTaskNum = savedToDeployVgTaskNum_;
    mStreamMgmt.toDeploySnodeTaskNum = savedToDeploySnodeTaskNum_;
    mStreamMgmt.hCtx.currentTs = savedCurrentTs_;
  }

  SStmActionQ queue_ = {};

 private:
  SStmActionQ *savedActionQ_ = nullptr;
  SHashObj    *savedStreamMap_ = nullptr;
  SHashObj    *savedDnodeMap_ = nullptr;
  SHashObj    *savedTaskMap_ = nullptr;
  SRWLatch     savedActionQLock_ = 0;
  int32_t      savedToDeployVgTaskNum_ = 0;
  int32_t      savedToDeploySnodeTaskNum_ = 0;
  int64_t      savedCurrentTs_ = 0;
};

#ifdef TD_ENTERPRISE
class MndStreamExtReaderRuntimeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedTaskMap_ = mStreamMgmt.taskMap;
    savedSnodeMap_ = mStreamMgmt.snodeMap;
    savedToDeploySnodeMap_ = mStreamMgmt.toDeploySnodeMap;
    savedToDeploySnodeTaskNum_ = mStreamMgmt.toDeploySnodeTaskNum;
    savedLastTaskId_ = mStreamMgmt.lastTaskId;

    mStreamMgmt.taskMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    ASSERT_NE(mStreamMgmt.taskMap, nullptr);
    mStreamMgmt.snodeMap = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    ASSERT_NE(mStreamMgmt.snodeMap, nullptr);
    taosHashSetFreeFp(mStreamMgmt.snodeMap, mstDestroySStmSnodeStatus);
    mStreamMgmt.toDeploySnodeMap =
        taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
    ASSERT_NE(mStreamMgmt.toDeploySnodeMap, nullptr);
    taosHashSetFreeFp(mStreamMgmt.toDeploySnodeMap, mstDestroySStmSnodeTasksDeploy);
    mStreamMgmt.toDeploySnodeTaskNum = 0;
    mStreamMgmt.lastTaskId = 1;

    SStmSnodeStatus snode = {};
    ASSERT_EQ(taosHashPut(mStreamMgmt.snodeMap, &snodeId_, sizeof(snodeId_), &snode, sizeof(snode)),
              TSDB_CODE_SUCCESS);
    }

  void TearDown() override {
    taosHashCleanup(mStreamMgmt.taskMap);
    taosHashCleanup(mStreamMgmt.snodeMap);
    taosHashCleanup(mStreamMgmt.toDeploySnodeMap);

    mStreamMgmt.taskMap = savedTaskMap_;
    mStreamMgmt.snodeMap = savedSnodeMap_;
    mStreamMgmt.toDeploySnodeMap = savedToDeploySnodeMap_;
    mStreamMgmt.toDeploySnodeTaskNum = savedToDeploySnodeTaskNum_;
    mStreamMgmt.lastTaskId = savedLastTaskId_;
  }

  int32_t snodeId_ = 7;

 private:
  SHashObj *savedTaskMap_ = nullptr;
  SHashObj *savedSnodeMap_ = nullptr;
  SHashObj *savedToDeploySnodeMap_ = nullptr;
  int32_t   savedToDeploySnodeTaskNum_ = 0;
  int64_t   savedLastTaskId_ = 0;
};
#endif

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

bool reportStreamExists(SSdb *, ESdbType, const void *) { return true; }

TEST_F(MndStreamHeartbeatTest, TimezoneLoadFailureKeepsFatalCodeAndPostsDeployAfterBackoff) {
  constexpr int64_t kStreamId = 0x1234;
  constexpr int64_t kTaskId = 0x5678;
  constexpr int64_t kSeriousId = 7;
  constexpr int64_t kHeartbeatTs = 1787068800123;
  char              streamName[] = "1.db.timezone_retry";

  SStmTaskStatus trigger = {};
  trigger.type = STREAM_TRIGGER_TASK;
  trigger.id.taskId = kTaskId;
  trigger.id.seriousId = kSeriousId;
  trigger.id.nodeId = 3;
  trigger.status = STREAM_STATUS_INIT;

  SStmStatus stream = {};
  stream.streamName = streamName;
  stream.triggerTask = &trigger;
  ASSERT_EQ(taosHashPut(mStreamMgmt.streamMap, &kStreamId, sizeof(kStreamId), &stream, sizeof(stream)),
            TSDB_CODE_SUCCESS);
  auto *stored = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamId, sizeof(kStreamId)));
  ASSERT_NE(stored, nullptr);
  trigger.pStream = stored;

  SStmTaskStatus *storedTrigger = &trigger;
  int64_t         taskKey[2] = {kStreamId, kTaskId};
  ASSERT_EQ(taosHashPut(mStreamMgmt.taskMap, taskKey, sizeof(taskKey), &storedTrigger, POINTER_BYTES),
            TSDB_CODE_SUCCESS);

  SStmTaskStatusMsg failed = {};
  failed.type = STREAM_TRIGGER_TASK;
  failed.streamId = kStreamId;
  failed.taskId = kTaskId;
  failed.seriousId = kSeriousId;
  failed.nodeId = trigger.id.nodeId;
  failed.status = STREAM_STATUS_FAILED;
  failed.detailStatus = -1;
  failed.errorCode = TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED;
  failed.extraErrMsg = taosStrdup("failed to load timezone 'Missing/Timezone'");
  ASSERT_NE(failed.extraErrMsg, nullptr);

  SStreamHbMsg request = {};
  request.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(request.pStreamStatus, nullptr);
  ASSERT_NE(taosArrayPush(request.pStreamStatus, &failed), nullptr);

  SStmGrpCtx ctx = {};
  ctx.currTs = kHeartbeatTs;
  ctx.pReq = &request;
  StreamFailureStubGuard reportGuard;
  ASSERT_EQ(msmNormalHandleStatusUpdate(&ctx), TSDB_CODE_SUCCESS);

  EXPECT_EQ(atomic_load_8(&stored->stopped), 1);
  EXPECT_EQ(stored->fatalError, TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED);
  EXPECT_EQ(stored->fatalRetryDuration, MST_ISOLATION_DURATION);
  EXPECT_EQ(stored->fatalRetryTs, kHeartbeatTs + MST_ISOLATION_DURATION);
  EXPECT_EQ(stored->stat.lastError, TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED);
  EXPECT_STREQ(stored->extraErrMsg, "failed to load timezone 'Missing/Timezone'");
  EXPECT_EQ(trigger.status, STREAM_STATUS_FAILED);
  EXPECT_EQ(trigger.errCode, TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED);
  EXPECT_EQ(gCapturedStreamFailure.calls, 1);
  EXPECT_EQ(gCapturedStreamFailure.errorCode, TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED);

  SSdb   sdb = {};
  SMnode mnode = {};
  mnode.pSdb = &sdb;
  Stub streamExistsStub;
  streamExistsStub.set(sdbCheckExists, reportStreamExists);

  mStreamMgmt.hCtx.currentTs = stored->fatalRetryTs - 1;
  msmCheckLoopStreamMap(&mnode);
  EXPECT_EQ(atomic_load_64(reinterpret_cast<volatile int64_t *>(&queue_.qRemainNum)), 0);

  mStreamMgmt.hCtx.currentTs = stored->fatalRetryTs;
  msmCheckLoopStreamMap(&mnode);
  ASSERT_EQ(atomic_load_64(reinterpret_cast<volatile int64_t *>(&queue_.qRemainNum)), 1);
  SStmQNode *pNode = nullptr;
  ASSERT_TRUE(mndStreamActionDequeue(&queue_, &pNode));
  ASSERT_NE(pNode, nullptr);
  EXPECT_EQ(pNode->type, STREAM_ACT_DEPLOY);
  EXPECT_TRUE(pNode->streamAct);
  EXPECT_EQ(pNode->action.stream.streamId, kStreamId);
  EXPECT_STREQ(pNode->action.stream.streamName, streamName);

  tCleanupStreamHbMsg(&request, true);
  taosMemoryFreeClear(trigger.extraErrMsg);
  taosMemoryFreeClear(stored->extraErrMsg);
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

class MndStreamOrigReaderReplyTest : public MndStreamHeartbeatTest {
 protected:
  void SetUp() override {
    ASSERT_NO_FATAL_FAILURE(MndStreamHeartbeatTest::SetUp());
    current_ = this;
    savedActive_ = mStreamMgmt.active;
    mStreamMgmt.active = 1;
    mStreamMgmt.lastTaskId = 100;
    mStreamMgmt.vgroupMap = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    mStreamMgmt.toDeployVgMap = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    ASSERT_NE(nullptr, mStreamMgmt.vgroupMap);
    ASSERT_NE(nullptr, mStreamMgmt.toDeployVgMap);
    taosHashSetFreeFp(mStreamMgmt.vgroupMap, mstDestroySStmVgroupStatus);
    taosHashSetFreeFp(mStreamMgmt.toDeployVgMap, mstDestroySStmVgTasksToDeploy);
    create_.streamId = streamId_;
    row_.pCreate = &create_;
    tstrncpy(row_.name, "1.db.stream", sizeof(row_.name));
    input_.refs = 1;
    input_.create = &create_;
    input_.name = row_.name;
    status_.deployInput = &input_;
    status_.pCreate = &create_;
    status_.streamName = row_.name;
    status_.triggerTask = &trigger_;
    deployCalls_ = 0;
    status_.lastTrigMgmtReqId = 17;
    status_.trigReaders = taosArrayInit(1, sizeof(SStmTaskStatus));
    status_.trigOReaders = taosArrayInit(1, POINTER_BYTES);
    readers_ = taosArrayInit(1, sizeof(SStmTaskStatus));
    ASSERT_NE(status_.trigReaders, nullptr);
    ASSERT_NE(status_.trigOReaders, nullptr);
    ASSERT_NE(readers_, nullptr);
    SStmTaskStatus reader = {};
    reader.id.nodeId = 5;
    reader.id.taskId = 0x42;
    ASSERT_NE(taosArrayPush(readers_, &reader), nullptr);
    ASSERT_NE(taosArrayPush(status_.trigOReaders, &readers_), nullptr);
    ASSERT_EQ(taosHashPut(mStreamMgmt.streamMap, &streamId_, sizeof(streamId_), &status_, sizeof(status_)), 0);
    stored_ = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &streamId_, sizeof(streamId_)));
    ASSERT_NE(stored_, nullptr);
    trigger_.pStream = stored_;
    trigger_.type = STREAM_TRIGGER_TASK;
    trigger_.id.taskId = 1;
    int64_t key[] = {streamId_, trigger_.id.taskId};
    auto   *trigger = &trigger_;
    ASSERT_EQ(0, taosHashPut(mStreamMgmt.taskMap, key, sizeof(key), &trigger, sizeof(trigger)));
    ctx_.pMnode = &mnode_;
    ctx_.pRsp = &heartbeat_;
    stub_.set(
        mndAcquireStream, +[](SMnode *, char *name, SStreamObj **row) -> int32_t {
          EXPECT_STREQ(current_->row_.name, name);
          ++current_->acquireCalls_;
          *row = &current_->row_;
          return 0;
        });
    stub_.set(
        mndAcquireStreamById, +[](SMnode *, int64_t, SStreamObj **) -> int32_t {
          ADD_FAILURE() << "A Trigger original-reader request scanned the stream catalog";
          return TSDB_CODE_MND_STREAM_NOT_EXIST;
        });
    stub_.set(
        mndReleaseStream, +[](SMnode *, SStreamObj *row) {
          EXPECT_EQ(row, &current_->row_);
          ++current_->releaseCalls_;
        });
    stub_.set(
        sdbGetTableVer,
        +[](SSdb *, ESdbType type) -> int64_t { return type == SDB_VGROUP ? current_->vgVersion_ : 0; });
    stub_.set(mstBuildDBVgroupsMap, buildDbMap);
    stub_.set(mndGetVgroupEpsetById, readerEpset);
  }

  void TearDown() override {
    tFreeSStreamMgmtReq(task_.pMgmtReq);
    taosMemoryFree(task_.pMgmtReq);
    taosArrayDestroyEx(heartbeat_.rsps.rspList, tFreeSStreamMgmtRsp);
    destroyTrigOReaders(&status_);
    taosArrayDestroy(status_.trigReaders);
    EXPECT_EQ(acquireCalls_, releaseCalls_);
    EXPECT_EQ(1, input_.refs);
    if (stored_) taosHashCleanup(stored_->registrations);
    taosHashCleanup(mStreamMgmt.toDeployVgMap);
    taosHashCleanup(mStreamMgmt.vgroupMap);
    mStreamMgmt.toDeployVgMap = savedToDeployVgMap_;
    mStreamMgmt.vgroupMap = savedVgroupMap_;
    mStreamMgmt.lastTaskId = savedLastTaskId_;
    mStreamMgmt.active = savedActive_;
    current_ = nullptr;
    MndStreamHeartbeatTest::TearDown();
  }

  static int32_t buildDbMap(SMnode *, SSHashObj **ppMap) {
    ++current_->mapCalls_;
    for (auto *lock : {&mStreamMgmt.heartbeatLock, &mStreamMgmt.runtimeLock, &mStreamMgmt.actionQLock}) {
      int32_t code = taosWTryLockLatch(lock);
      EXPECT_EQ(0, code);
      if (!code) taosWUnLockLatch(lock);
    }
    if (current_->duringScan_) current_->duringScan_();
    *ppMap = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    if (*ppMap == nullptr) return terrno;
    SDBVgHashInfo db = {};
    db.hashMethod = 1;
    db.vgArray = taosArrayInit(1, sizeof(SVGroupHashInfo));
    if (db.vgArray == nullptr) return terrno;
    SVGroupHashInfo vg = {};
    vg.vgId = 5;
    vg.hashEnd = UINT32_MAX;
    if (taosArrayPush(db.vgArray, &vg) == nullptr) {
      taosArrayDestroy(db.vgArray);
      return terrno;
    }
    const int32_t code = tSimpleHashPut(*ppMap, "1.db", sizeof("1.db"), &db, sizeof(db));
    if (code != 0) taosArrayDestroy(db.vgArray);
    return code;
  }

  static int32_t failDbMap(SMnode *, SSHashObj **) { return TSDB_CODE_MND_DB_NOT_EXIST; }

  static int32_t countDeployment(SStmGrpCtx *, SStmStatus *, SStmTaskStatusMsg *, int32_t, SStreamObj *row) {
    EXPECT_EQ(row, &current_->row_);
    ++deployCalls_;
    return 0;
  }

  static int32_t failDeployment(SStmGrpCtx *ctx, SStmStatus *status, SStmTaskStatusMsg *task, int32_t vgId,
                                SStreamObj *row) {
    countDeployment(ctx, status, task, vgId, row);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  static SArray *failReaderReplyAllocation(size_t size, size_t elemSize) {
    if (elemSize == sizeof(SStreamTaskAddr)) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return nullptr;
    }
    SArray *array = taosArrayInit_s(elemSize, (std::max)(size, static_cast<size_t>(TARRAY_MIN_SIZE)));
    if (array != nullptr) array->size = 0;
    return array;
  }

  static SEpSet readerEpset(SMnode *, int32_t) {
    SEpSet ep = {};
    ep.numOfEps = 1;
    ep.eps[0].port = 6030;
    tstrncpy(ep.eps[0].fqdn, "reader.test", sizeof(ep.eps[0].fqdn));
    return ep;
  }

  void request(bool mixed = false, int64_t cleanupRegistry = 0) {
    taosArrayDestroyEx(heartbeat_.rsps.rspList, tFreeSStreamMgmtRsp);
    heartbeat_.rsps.rspList = nullptr;
    task_.streamId = streamId_;
    task_.taskId = 1;
    task_.type = STREAM_TRIGGER_TASK;
    task_.pMgmtReq = static_cast<SStreamMgmtReq *>(taosMemoryCalloc(1, sizeof(SStreamMgmtReq)));
    ASSERT_NE(task_.pMgmtReq, nullptr);
    task_.pMgmtReq->reqId = 17;
    task_.pMgmtReq->type = STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER;
    task_.pMgmtReq->cont.pReqs = taosArrayInit(2, sizeof(SStreamDbTableName));
    ASSERT_NE(task_.pMgmtReq->cont.pReqs, nullptr);
    for (const char *table : {"physical_a", "physical_b"}) {
      SStreamDbTableName name = {};
      tstrncpy(name.dbFName, "1.db", sizeof(name.dbFName));
      tstrncpy(name.tbName, table, sizeof(name.tbName));
      ASSERT_NE(taosArrayPush(task_.pMgmtReq->cont.pReqs, &name), nullptr);
    }
    if (mixed) {
      task_.pMgmtReq->type = STREAM_MGMT_REQ_TRIGGER_MIXED_ORIGTBL_READER;
      task_.pMgmtReq->cont.pExtOrigTblReaders = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderReq), 1);
      ASSERT_NE(nullptr, task_.pMgmtReq->cont.pExtOrigTblReaders);
      if (cleanupRegistry) {
        auto *req = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(task_.pMgmtReq->cont.pExtOrigTblReaders, 0));
        req->registryEntryId = cleanupRegistry;
        req->capabilities = STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
        req->protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
      }
#ifdef TD_ENTERPRISE
      stub_.set(mndAcquireExtSource, +[](SMnode *, const char *) -> SExtSourceObj * { return nullptr; });
#endif
    }
    ASSERT_EQ(0, handleHeartbeatTaskMgmtReq(&ctx_, &task_));
    ASSERT_EQ(taosArrayGetSize(heartbeat_.rsps.rspList), 1);
  }

  static MndStreamOrigReaderReplyTest *current_;
  std::function<void()>                duringScan_;
  SHashObj                            *savedVgroupMap_ = mStreamMgmt.vgroupMap;
  SHashObj                            *savedToDeployVgMap_ = mStreamMgmt.toDeployVgMap;
  int64_t                              savedLastTaskId_ = mStreamMgmt.lastTaskId;
  int8_t                               savedActive_ = 0;
  int                                  mapCalls_ = 0;
  int                                  acquireCalls_ = 0;
  int                                  releaseCalls_ = 0;
  int64_t                              vgVersion_ = 0;
  SMnode                               mnode_ = {};
  SStmDeployInput                      input_ = {};
  SCMCreateStreamReq                   create_ = {};
  SStreamObj                           row_ = {};
  SStmTaskStatus                       trigger_ = {};
  Stub              stub_;
  int64_t           streamId_ = 0x7115431537;
  SStmStatus        status_ = {};
  SStmStatus       *stored_ = nullptr;
  SArray           *readers_ = nullptr;
  SStmTaskStatusMsg task_ = {};
  SMStreamHbRspMsg  heartbeat_ = {};
  SStmGrpCtx        ctx_ = {};
  static int        deployCalls_;
};

MndStreamOrigReaderReplyTest *MndStreamOrigReaderReplyTest::current_ = nullptr;
int MndStreamOrigReaderReplyTest::deployCalls_ = 0;

TEST_F(MndStreamOrigReaderReplyTest, MixedRequestResolvesLocalPlacementWithoutGlobalLocks) {
  stored_->lastTrigMgmtReqId = 0;
  taosArrayClear(readers_);
  ASSERT_NO_FATAL_FAILURE(request(true));
  const auto *rsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  EXPECT_EQ(STREAM_MSG_MIXED_ORIGTBL_READER_INFO, rsp->header.msgType);
  EXPECT_EQ(0, rsp->code);
  EXPECT_EQ(1, mapCalls_);
  EXPECT_EQ(17, stored_->lastTrigMgmtReqId);
  ASSERT_EQ(2, taosArrayGetSize(rsp->cont.vgIds));
  EXPECT_EQ(5, *static_cast<int32_t *>(taosArrayGet(rsp->cont.vgIds, 0)));
  ASSERT_EQ(1, taosArrayGetSize(rsp->cont.readerList));
  auto *addr = static_cast<SStreamTaskAddr *>(taosArrayGet(rsp->cont.readerList, 0));
  EXPECT_EQ(5, addr->nodeId);
  auto *queue =
      static_cast<SStmVgTasksToDeploy *>(taosHashGet(mStreamMgmt.toDeployVgMap, &addr->nodeId, sizeof(addr->nodeId)));
  ASSERT_NE(nullptr, queue);
  ASSERT_EQ(1, taosArrayGetSize(queue->taskList));
  auto *entry = static_cast<SStmTaskToDeployExt *>(taosArrayGet(queue->taskList, 0));
  EXPECT_EQ(addr->taskId, entry->deploy.task.taskId);
  ASSERT_EQ(1, taosArrayGetSize(rsp->cont.pExtOrigTblReaders));
  const auto *ext = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(rsp->cont.pExtOrigTblReaders, 0));
#ifdef TD_ENTERPRISE
  EXPECT_EQ(TSDB_CODE_EXT_SOURCE_NOT_FOUND, ext->code);
#else
  EXPECT_EQ(TSDB_CODE_OPS_NOT_SUPPORT, ext->code);
#endif
}

TEST_F(MndStreamOrigReaderReplyTest, FailedLocalRoutePreservesExternalReaderInMixedPatch) {
  stored_->lastTrigMgmtReqId = 0;
  SStmTaskStatus existing = {};
  existing.id.uid = 82;
  existing.id.nodeId = 1;
  existing.id.taskId = 102;
  existing.flags = STREAM_FLAG_REF_EXT_SOURCE;
  existing.type = STREAM_READER_TASK;
  existing.status = STREAM_STATUS_RUNNING;
  auto *reader = static_cast<SStmTaskStatus *>(taosArrayPush(readers_, &existing));
  ASSERT_NE(nullptr, reader);
  duringScan_ = [&] { ++vgVersion_; };
  stub_.set(mndAcquireVgroup, +[](SMnode *, int32_t) -> SVgObj * { return nullptr; });
  ASSERT_NO_FATAL_FAILURE(request(true, existing.id.uid));
  auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  ASSERT_NE(nullptr, reply);
  EXPECT_EQ(17, reply->reqId);
  EXPECT_EQ(TSDB_CODE_MND_STREAM_NOT_AVAILABLE, reply->code);
  auto *result = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
  ASSERT_NE(nullptr, result);
  EXPECT_EQ(existing.id.uid, result->registryEntryId);
  EXPECT_EQ(reply->code, result->code);
  EXPECT_EQ(0u, taosArrayGetSize(heartbeat_.undeploy.taskList));
  EXPECT_FALSE(reader->extReaderCleaned);
  EXPECT_EQ(existing.id.taskId, reader->id.taskId);
  EXPECT_EQ(STREAM_STATUS_RUNNING, reader->status);
}

TEST_F(MndStreamOrigReaderReplyTest, LocalRequestRejectsPlacementAndGenerationChangesDuringScan) {
  stored_->lastTrigMgmtReqId = 0;
  stub_.set(msmCheckDeployTrigReader, countDeployment);
  stub_.set(
      mndAcquireVgroup, +[](SMnode *, int32_t vgId) -> SVgObj * {
        EXPECT_EQ(5, vgId);
        return nullptr;
      });
  duringScan_ = [&] { ++vgVersion_; };
  ASSERT_NO_FATAL_FAILURE(request());
  const auto *rsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  EXPECT_EQ(TSDB_CODE_MND_STREAM_NOT_AVAILABLE, rsp->code);
  duringScan_ = [&] { ++stored_->buildVersion; };
  ASSERT_NO_FATAL_FAILURE(request());
  rsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  EXPECT_EQ(TSDB_CODE_MND_STREAM_NOT_RUNNING, rsp->code);
  EXPECT_EQ(2, mapCalls_);
  EXPECT_EQ(0, deployCalls_);
  EXPECT_EQ(0, stored_->lastTrigMgmtReqId);
}

TEST_F(MndStreamOrigReaderReplyTest, UnrelatedVgroupChangeKeepsLocalReaderDeployment) {
  stored_->lastTrigMgmtReqId = 0;
  taosArrayClear(readers_);
  duringScan_ = [&] { ++vgVersion_; };
  stub_.set(
      mndAcquireVgroup, +[](SMnode *, int32_t vgId) -> SVgObj * {
        static SVgObj vg = {};
        EXPECT_EQ(5, vgId);
        tstrncpy(vg.dbName, "1.db", sizeof(vg.dbName));
        vg.hashEnd = UINT32_MAX;
        return &vg;
      });
  stub_.set(mndReleaseVgroup, +[](SMnode *, SVgObj *) {});
  stub_.set(sdbReadLock, +[](SSdb *, int32_t) {});
  stub_.set(sdbUnLock, +[](SSdb *, int32_t) {});
  ASSERT_NO_FATAL_FAILURE(request());
  const auto *rsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  EXPECT_EQ(0, rsp->code);
  EXPECT_EQ(1, mapCalls_);
  EXPECT_EQ(17, stored_->lastTrigMgmtReqId);
  EXPECT_EQ(1, taosArrayGetSize(rsp->cont.readerList));
}

TEST_F(MndStreamOrigReaderReplyTest, ReplaysCompleteReplyWithoutRedeployingReaders) {
  // The first reply may be delayed or lost; every retry must carry usable reader information.
  for (int i = 0; i < 2; ++i) {
    ASSERT_NO_FATAL_FAILURE(request());
    const auto *rsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
    ASSERT_EQ(rsp->code, 0);
    EXPECT_EQ(rsp->reqId, 17);
    ASSERT_EQ(taosArrayGetSize(rsp->cont.vgIds), 2);
    EXPECT_EQ(*static_cast<int32_t *>(taosArrayGet(rsp->cont.vgIds, 0)), 5);
    EXPECT_EQ(*static_cast<int32_t *>(taosArrayGet(rsp->cont.vgIds, 1)), 5);
    ASSERT_EQ(taosArrayGetSize(rsp->cont.readerList), 1);
    const auto *reader = static_cast<SStreamTaskAddr *>(taosArrayGet(rsp->cont.readerList, 0));
    EXPECT_EQ(reader->nodeId, 5);
    EXPECT_EQ(reader->taskId, 0x42);
    EXPECT_EQ(reader->epset.eps[0].port, 6030);
    EXPECT_EQ(msmGetTrigOReaderSize(stored_->trigOReaders), 1);
    EXPECT_EQ(task_.pMgmtReq, nullptr);
  }
}

TEST_F(MndStreamOrigReaderReplyTest, ReturnsReaderAddressesAcrossPartiallyFilledBlocks) {
  SArray *second = taosArrayInit(1, sizeof(SStmTaskStatus));
  ASSERT_NE(nullptr, second);
  ASSERT_NE(nullptr, taosArrayPush(stored_->trigOReaders, &second));
  SStmTaskStatus peer = {};
  peer.id.nodeId = 6;
  peer.id.taskId = 0x43;
  ASSERT_NE(nullptr, taosArrayPush(second, &peer));

  ASSERT_NO_FATAL_FAILURE(request());
  const auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  ASSERT_EQ(0, reply->code);
  ASSERT_EQ(2u, taosArrayGetSize(reply->cont.readerList));
  const auto *first = static_cast<SStreamTaskAddr *>(taosArrayGet(reply->cont.readerList, 0));
  const auto *last = static_cast<SStreamTaskAddr *>(taosArrayGet(reply->cont.readerList, 1));
  EXPECT_EQ(0x42, first->taskId);
  EXPECT_EQ(5, first->nodeId);
  EXPECT_EQ(peer.id.taskId, last->taskId);
  EXPECT_EQ(peer.id.nodeId, last->nodeId);
}

TEST_F(MndStreamOrigReaderReplyTest, FailedRequestDoesNotSuppressRetry) {
  stored_->lastTrigMgmtReqId = 0;
  stub_.set(mstBuildDBVgroupsMap, failDbMap);
  ASSERT_NO_FATAL_FAILURE(request());
  const auto *rsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  EXPECT_EQ(rsp->code, TSDB_CODE_MND_DB_NOT_EXIST);
  EXPECT_EQ(stored_->lastTrigMgmtReqId, 0);

  stub_.set(mstBuildDBVgroupsMap, buildDbMap);
  SStmTaskStatus reader = {};
  reader.id.nodeId = 5;
  ASSERT_NE(taosArrayPush(stored_->trigReaders, &reader), nullptr);
  ASSERT_NO_FATAL_FAILURE(request());
  rsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  EXPECT_EQ(rsp->code, 0);
  EXPECT_EQ(taosArrayGetSize(rsp->cont.vgIds), 2);
  EXPECT_EQ(stored_->lastTrigMgmtReqId, 17);
}

TEST_F(MndStreamOrigReaderReplyTest, RepeatedRequestPreservesStoppedError) {
  stored_->stopped = 1;
  ASSERT_NO_FATAL_FAILURE(request());
  const auto *rsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  EXPECT_EQ(rsp->code, TSDB_CODE_MND_STREAM_STOPPED);
}

TEST_F(MndStreamOrigReaderReplyTest, PartialDeploymentFailureStopsStreamBeforeRetry) {
  // A reader is already recorded; failure in a later deployment must use whole-stream recovery.
  stored_->lastTrigMgmtReqId = 0;
  stub_.set(msmCheckDeployTrigReader, failDeployment);
  ASSERT_NO_FATAL_FAILURE(request());
  const auto *rsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  EXPECT_EQ(rsp->code, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(stored_->stopped, 1);
  EXPECT_EQ(stored_->fatalError, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(stored_->lastTrigMgmtReqId, 0);

  ASSERT_NO_FATAL_FAILURE(request());
  rsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  EXPECT_EQ(rsp->code, TSDB_CODE_MND_STREAM_STOPPED);
  EXPECT_EQ(deployCalls_, 1);
  EXPECT_EQ(msmGetTrigOReaderSize(stored_->trigOReaders), 1);
  EXPECT_EQ(static_cast<SStmTaskStatus *>(taosArrayGet(readers_, 0))->id.taskId, 0x42);
}

TEST_F(MndStreamOrigReaderReplyTest, ReplyAllocationFailureDoesNotRepeatCompletedDeployment) {
  stored_->lastTrigMgmtReqId = 0;
  stub_.set(msmCheckDeployTrigReader, countDeployment);
  stub_.set(taosArrayInit, failReaderReplyAllocation);
  ASSERT_NO_FATAL_FAILURE(request());
  const auto *rsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  EXPECT_EQ(rsp->code, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(stored_->lastTrigMgmtReqId, 17);

  stub_.reset(taosArrayInit);
  ASSERT_NO_FATAL_FAILURE(request());
  rsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  EXPECT_EQ(rsp->code, 0);
  EXPECT_EQ(taosArrayGetSize(rsp->cont.vgIds), 2);
  EXPECT_EQ(taosArrayGetSize(rsp->cont.readerList), 1);
  EXPECT_EQ(deployCalls_, 1);
}

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

class MndStreamTagRefTest;
MndStreamTagRefTest* gStreamTagRefTest = nullptr;

class MndStreamTagRefTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gStreamTagRefTest = this;
    stream_.pCreate = &create_;
    create_.triggerTblType = TSDB_SUPER_TABLE;
    create_.triggerTblSuid = 1;
    tags_[0].colId = 7;
    tags_[1].colId = 8;
    stub_.set(sdbGetSize, streamCount);
    stub_.set(sdbTraverse, visitStream);
  }

  void TearDown() override {
    stub_.reset(sdbTraverse);
    stub_.reset(sdbGetSize);
    nodesDestroyList(partition_);
    nodesDestroyList(injected_);
    taosMemoryFree(create_.partitionCols);
    taosMemoryFree(create_.durationTagCols);
    gStreamTagRefTest = nullptr;
  }

  static int32_t streamCount(SSdb*, ESdbType) { return 1; }

  static void visitStream(SSdb*, ESdbType type, sdbTraverseFp callback, void* p1, void* p2, void* p3) {
    EXPECT_EQ(SDB_STREAM, type);
    (void)callback(&gStreamTagRefTest->mnode_, &gStreamTagRefTest->stream_, p1, p2, p3);
  }

  static SNode* tagColumn() {
    SNode* node = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &node));
    if (node != nullptr) {
      auto* column = reinterpret_cast<SColumnNode*>(node);
      column->colId = 7;
      column->colType = COLUMN_TYPE_TAG;
      column->node.resType.type = TSDB_DATA_TYPE_INT;
      column->node.resType.bytes = sizeof(int32_t);
    }
    return node;
  }

  void setPartition(bool expression, bool injected) {
    SNode* tbname = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &tbname));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&partition_, tbname));
    auto* tbnameColumn = reinterpret_cast<SColumnNode*>(tbname);
    tbnameColumn->colId = -1;
    tbnameColumn->colType = COLUMN_TYPE_TBNAME;
    tbnameColumn->node.resType.type = TSDB_DATA_TYPE_VARCHAR;
    tbnameColumn->node.resType.bytes = TSDB_TABLE_NAME_LEN;
    if (expression) {
      SNode* node = nullptr;
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_OPERATOR, &node));
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&partition_, node));
      auto* op = reinterpret_cast<SOperatorNode*>(node);
      op->opType = OP_TYPE_ADD;
      op->node.resType.type = TSDB_DATA_TYPE_INT;
      op->node.resType.bytes = sizeof(int32_t);
      op->pLeft = tagColumn();
      ASSERT_NE(nullptr, op->pLeft);
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &op->pRight));
      auto* zero = reinterpret_cast<SValueNode*>(op->pRight);
      zero->node.resType.type = TSDB_DATA_TYPE_INT;
      zero->node.resType.bytes = sizeof(int32_t);
      zero->translate = true;
      int32_t value = 0;
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesSetValueNodeValue(zero, &value));
    }
    SNode* rawTag = tagColumn();
    ASSERT_NE(nullptr, rawTag);
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&partition_, rawTag));
    int32_t length = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesListToString(partition_, false, reinterpret_cast<char**>(&create_.partitionCols), &length));
    if (injected) {
      SNode* injectedTag = tagColumn();
      ASSERT_NE(nullptr, injectedTag);
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&injected_, injectedTag));
      ASSERT_EQ(TSDB_CODE_SUCCESS,
                nodesListToString(injected_, false, reinterpret_cast<char**>(&create_.durationTagCols), &length));
    }
  }

  SMnode mnode_ = {};
  SStreamObj stream_ = {};
  SCMCreateStreamReq create_ = {};
  SSchema tags_[2] = {};
  SNodeList* partition_ = nullptr;
  SNodeList* injected_ = nullptr;
  Stub stub_;
};

TEST_F(MndStreamTagRefTest, UserPartitionExpressionIsNotExemptedByInjectedTag) {
  setPartition(true, true);
  ASSERT_FALSE(HasFatalFailure());
  mndStreamUpdateTagsRefFlag(&mnode_, create_.triggerTblSuid, tags_, 2);
  EXPECT_NE(0, tags_[0].flags & COL_REF_BY_STM);
  EXPECT_EQ(0, tags_[1].flags & COL_REF_BY_STM);
}

TEST_F(MndStreamTagRefTest, InjectedOnlyTagRemainsSettable) {
  setPartition(false, true);
  ASSERT_FALSE(HasFatalFailure());
  mndStreamUpdateTagsRefFlag(&mnode_, create_.triggerTblSuid, tags_, 2);
  EXPECT_EQ(0, tags_[0].flags & COL_REF_BY_STM);
  EXPECT_EQ(0, tags_[1].flags & COL_REF_BY_STM);
}

TEST_F(MndStreamTagRefTest, ExplicitPartitionTagRemainsReadOnly) {
  setPartition(false, false);
  ASSERT_FALSE(HasFatalFailure());
  mndStreamUpdateTagsRefFlag(&mnode_, create_.triggerTblSuid, tags_, 2);
  EXPECT_NE(0, tags_[0].flags & COL_REF_BY_STM);
  EXPECT_EQ(0, tags_[1].flags & COL_REF_BY_STM);
}

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

// Fakes only the wall clock; the monotonic clock stays real.
int32_t returnRecalcClock(int clockId, struct timespec *pTime) {
  int64_t nowNs = gRecalcNowMs * NANOSECOND_PER_MSEC;
  if (clockId != CLOCK_REALTIME) {
    nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
  }
  pTime->tv_sec = static_cast<time_t>(nowNs / NANOSECOND_PER_SEC);
  pTime->tv_nsec = static_cast<long>(nowNs % NANOSECOND_PER_SEC);
  return TSDB_CODE_SUCCESS;
}

class MndStreamRecalcTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedStreamMap_ = mStreamMgmt.streamMap;
    mStreamMgmt.streamMap = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    ASSERT_NE(mStreamMgmt.streamMap, nullptr);
    timeStub_.set(taosClockGetTime, returnRecalcClock);
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
    timeStub_.reset(taosClockGetTime);
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
  int32_t                     transId = 0;
  SStreamObj                 *sourceStream = nullptr;
  int64_t                     streamId = 0;
  uint64_t                    revision = 0;
  SArray                     *requests = nullptr;
  tmsg_t                      requestMsgType = 0;
  std::vector<SRpcHandleInfo> rpcHandles;
  std::vector<uint8_t>        callbackParam;
};

// The streamTest binary never calls streamInit(), so the production guard
// streamMgmtInited() is permanently false here. Fixtures that model an ONLINE mnode
// (mStreamMgmt initialized, msmHandleBecomeNotLeader() expected to tear the runtime down)
// must stub it to true, otherwise the guard returns early and their probes never fire.
// The offline mini-mnode tests at the bottom of this file deliberately use the real
// implementation, which keeps the uninitialized path covered.
bool alwaysStreamMgmtInited() { return true; }

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
  std::function<void()>                 beforeCreate;
  std::function<void()>                 beforePrepare;
  int32_t                               createCalls = 0;
  std::vector<int64_t>                  createStreamIds;
  ETrnConflct                           createConflict = TRN_CONFLICT_NOTHING;
  int32_t                               queueCode = TSDB_CODE_SUCCESS;
  const SArray                         *snapshotDupSource = nullptr;
  std::atomic<bool>                     snapshotDupEntered{false};
  std::atomic<bool>                     snapshotDupHasRuntimeRead{false};
  std::atomic<bool>                     releaseSnapshotDup{false};
  std::atomic<bool>                     queryRuntimeWaiting{false};
  std::deque<int32_t>                   createCodes;
  std::deque<int32_t>                   appendCodes;
  std::deque<int32_t>                   prepareCodes;
  std::function<void()>                 onPrepare;
  std::deque<CapturedRecalcTransaction> transactions;
  std::deque<SRpcMsg>                   queuedMessages;
  std::vector<int32_t>                  responseCodes;
  std::vector<void *>                   responseHandles;
  std::atomic<bool>                     responseWhileRuntimeRead{false};
  std::atomic<bool>                     scheduleWhileRuntimeRead{false};
  std::atomic<bool>                     prepareWhileRuntimeRead{false};
  bool                                  persistPreparedTransactions = false;
  tmsg_t                                lastCreateMsgType = 0;
  std::vector<SRpcHandleInfo>           lastCreateRpcHandles;
  SStmStatus                           *responseProbeStatus = nullptr;
  int64_t                               responseProbeRecalcId = 0;
  bool                                  responseObservedPublished = false;
};

RecalcPersistenceHarness *gRecalcPersistenceHarness = nullptr;

bool observeRecalcRuntimeLockWait(SRWLatch *pLock, bool readLock) {
  if (readLock) {
    while (taosRTryLockLatch(pLock)) {
      if (pLock == &mStreamMgmt.runtimeLock) gRecalcPersistenceHarness->queryRuntimeWaiting.store(true);
      taosMsleep(1);
    }
  } else {
    taosWWaitLockLatch(pLock);
  }
  return true;
}

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

int32_t captureRecalcCreateTrans(SMnode *, SStreamObj *pStream, SRpcMsg *pReq, ETrnConflct conflict, const char *,
                                 STrans **ppTrans) {
  if (gRecalcPersistenceHarness->beforeCreate) {
    auto callback = std::move(gRecalcPersistenceHarness->beforeCreate);
    callback();
  }
  ++gRecalcPersistenceHarness->createCalls;
  gRecalcPersistenceHarness->createStreamIds.push_back(pStream->pCreate->streamId);
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
  captured.transId = pTrans->id;
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

int32_t captureRecalcPrepare(SMnode *pMnode, STrans *pTrans) {
  if (gRecalcPersistenceHarness->beforePrepare) {
    auto callback = std::move(gRecalcPersistenceHarness->beforePrepare);
    callback();
  }
  if (runtimeReadPreventsWriter()) gRecalcPersistenceHarness->prepareWhileRuntimeRead.store(true);
  const int32_t code = popHarnessCode(&gRecalcPersistenceHarness->prepareCodes, TSDB_CODE_SUCCESS);
  for (auto iter = gRecalcPersistenceHarness->transactions.begin();
       iter != gRecalcPersistenceHarness->transactions.end(); ++iter) {
    if (iter->key != pTrans) continue;
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
      taosArrayDestroy(iter->requests);
      gRecalcPersistenceHarness->transactions.erase(iter);
      if (gRecalcPersistenceHarness->onPrepare) gRecalcPersistenceHarness->onPrepare();
      return code;
    }
    iter->callbackParam.resize(pTrans->paramLen);
    if (pTrans->paramLen > 0) std::memcpy(iter->callbackParam.data(), pTrans->param, pTrans->paramLen);
    iter->key = nullptr;
    if (gRecalcPersistenceHarness->persistPreparedTransactions) {
      // Keep earlier transactions in SDB, as happens while commit is retrying.
      SSdbRaw *pRaw = mndTransEncode(pTrans);
      if (pRaw == nullptr) return terrno;
      int32_t persistCode = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
      if (persistCode != TSDB_CODE_SUCCESS) {
        sdbFreeRaw(pRaw);
        return persistCode;
      }
      persistCode = sdbWrite(pMnode->pSdb, pRaw);
      if (persistCode != TSDB_CODE_SUCCESS) return persistCode;
    }
    if (gRecalcPersistenceHarness->onPrepare) gRecalcPersistenceHarness->onPrepare();
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
    savedPullupCursorValid_ = mStreamMgmt.recalcPullupCursorValid;
    savedPullupCursor_ = mStreamMgmt.recalcPullupCursor;
    savedPullupRoundRemaining_ = mStreamMgmt.recalcPullupRoundRemaining;
    savedActive_ = mStreamMgmt.active;
    savedState_ = mStreamMgmt.state;
    savedInactiveTimes_ = mStreamMgmt.stat.inactiveTimes;
    savedActiveTimes_ = mStreamMgmt.stat.activeTimes;
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
    mStreamMgmt.recalcPullupCursorValid = 0;
    mStreamMgmt.recalcPullupCursor = 0;
    mStreamMgmt.recalcPullupRoundRemaining = 0;
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
    stub_.set(streamMgmtInited, alwaysStreamMgmtInited);

    AddStream(kStreamA, "1.test.recalc_a");
    AddStream(kStreamB, "1.test.recalc_b");
  }

  void TearDown() override {
    stub_.reset(streamMgmtInited);
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
    mStreamMgmt.stat.activeTimes = savedActiveTimes_;
    atomic_store_8(&mStreamMgmt.recalcPullupPending, savedPullupPending_);
    mStreamMgmt.recalcPullupCursorValid = savedPullupCursorValid_;
    mStreamMgmt.recalcPullupCursor = savedPullupCursor_;
    mStreamMgmt.recalcPullupRoundRemaining = savedPullupRoundRemaining_;
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

  int32_t HandleHeartbeat(SStreamHbMsg *pHeartbeat, int64_t now = 100) {
    SRpcMsg request = {};
    SRpcMsg response = {};
    request.info.node = &mnode_;
    const int32_t code = msmHandleStreamHbMsg(&mnode_, now, pHeartbeat, &request, &response);
    rpcFreeCont(response.pCont);
    return code;
  }

  void AddSelectiveClearRecords() {
    SStmRecalcRecord agedLegacy = {};
    agedLegacy.snapshot.recalcId = kAgedLegacyRecalcId;
    agedLegacy.snapshot.start = 300;
    agedLegacy.snapshot.end = 400;
    agedLegacy.snapshot.progressPct = 100;
    agedLegacy.snapshot.status = STREAM_RECALC_STATUS_FINISHED;
    agedLegacy.terminalObservedAtMs = 1;
    agedLegacy.typedStatusKnown = true;
    agedLegacy.hidden = true;

    SStmRecalcRecord evictedLegacy = {};
    evictedLegacy.snapshot.recalcId = kEvictedLegacyRecalcId;
    evictedLegacy.snapshot.start = 500;
    evictedLegacy.snapshot.end = 600;
    evictedLegacy.snapshot.progressPct = 100;
    evictedLegacy.snapshot.status = STREAM_RECALC_STATUS_FINISHED;
    evictedLegacy.terminalObservedAtMs = 2;
    evictedLegacy.typedStatusKnown = true;
    evictedLegacy.hidden = true;

    SStmRecalcRecord visibleCoordinator = {};
    visibleCoordinator.snapshot.recalcId = kVisibleCoordinatorRecalcId;
    visibleCoordinator.snapshot.start = 700;
    visibleCoordinator.snapshot.end = 800;
    visibleCoordinator.snapshot.progressPct = 0;
    visibleCoordinator.snapshot.status = STREAM_RECALC_STATUS_PENDING;
    visibleCoordinator.requestTimeMs = 1000;
    visibleCoordinator.typedStatusKnown = true;
    visibleCoordinator.visible = true;

    SStmRecalcRecord terminalCoordinator = {};
    terminalCoordinator.snapshot.recalcId = kTerminalCoordinatorRecalcId;
    terminalCoordinator.snapshot.start = 900;
    terminalCoordinator.snapshot.end = 1000;
    terminalCoordinator.snapshot.progressPct = 100;
    terminalCoordinator.snapshot.status = STREAM_RECALC_STATUS_FINISHED;
    terminalCoordinator.requestTimeMs = 2000;
    terminalCoordinator.terminalObservedAtMs = 3000;
    terminalCoordinator.typedStatusKnown = true;
    terminalCoordinator.visible = true;

    SStmRecalcRecord persistingCoordinator = {};
    persistingCoordinator.snapshot.recalcId = kPersistingCoordinatorRecalcId;
    persistingCoordinator.snapshot.start = 1100;
    persistingCoordinator.snapshot.end = 1200;
    persistingCoordinator.snapshot.progressPct = 80;
    persistingCoordinator.snapshot.status = STREAM_RECALC_STATUS_RUNNING;
    persistingCoordinator.terminalCandidate.recalcId = kPersistingCoordinatorRecalcId;
    persistingCoordinator.terminalCandidate.start = 1100;
    persistingCoordinator.terminalCandidate.end = 1200;
    persistingCoordinator.terminalCandidate.progressPct = 100;
    persistingCoordinator.terminalCandidate.status = STREAM_RECALC_STATUS_FINISHED;
    persistingCoordinator.requestTimeMs = 4000;
    persistingCoordinator.typedStatusKnown = true;
    persistingCoordinator.visible = true;
    persistingCoordinator.terminalPersisting = true;
    persistingCoordinator.terminalCandidateValid = true;

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

  bool ClearRuntimeStreams() {
    if (taosWTryLockLatch(&mStreamMgmt.runtimeLock) != 0) return false;
    atomic_store_8(&mStreamMgmt.active, 0);
    while (!streamIds_.empty()) DestroyStatus(streamIds_.back());
    taosHashCleanup(mStreamMgmt.streamMap);
    mStreamMgmt.streamMap = nullptr;
    taosWUnLockLatch(&mStreamMgmt.runtimeLock);
    return true;
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

  void KeepRealTransactionsInSdb() {
    stub_.reset(mndStreamCreateTrans);
    harness_.persistPreparedTransactions = true;
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
  int64_t              savedActiveTimes_ = 0;
  int8_t               savedPullupPending_ = 0;
  int8_t               savedPullupCursorValid_ = 0;
  int64_t              savedPullupCursor_ = 0;
  int32_t              savedPullupRoundRemaining_ = 0;
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

constexpr int64_t MndStreamRecalcPersistenceTest::kStreamA;

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

extern "C" void msmInvalidateStreamDeployment(int64_t streamId, char *streamName);
SMnode         *gLifecycleMnode = nullptr;

void captureLifecycleUndeploy(int64_t streamId, char *) {
  SMnode *pMnode = gLifecycleMnode;
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
    gLifecycleMnode = &mnode_;
    stub_.set(msmInvalidateStreamDeployment, captureLifecycleUndeploy);
    stub_.set(mstPostStreamAction, captureLifecyclePost);
    stub_.set(tmsgSendRsp, captureLifecycleResponse);
    stub_.set(streamMgmtInited, alwaysStreamMgmtInited);

    ASSERT_TRUE(WriteStream(kStreamA, kStreamAName, 0));
    ASSERT_TRUE(WriteStream(kStreamB, kStreamBName, 0));
  }

  void TearDown() override {
    stub_.reset(streamMgmtInited);
    stub_.reset(tmsgSendRsp);
    stub_.reset(mstPostStreamAction);
    stub_.reset(msmInvalidateStreamDeployment);
    gLifecycleMnode = nullptr;
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

extern "C" int64_t msmAssignTaskSeriousId();
int32_t            returnRecoverySnodeId(SMnode *, SStreamObj *, bool);

class PrivateBuildPause {
 public:
  explicit PrivateBuildPause(bool pauseAtTaskId = true) {
    entered_.store(false);
    released_.store(false);
    expired_.store(false);
    if (pauseAtTaskId) stub_.set(msmAssignTaskSeriousId, Pause);
  }
  bool Entered() {
    auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!entered_.load() && std::chrono::steady_clock::now() < end) std::this_thread::yield();
    return entered_.load();
  }
  void Release() {
    released_.store(true);
    EXPECT_FALSE(expired_.load());
  }

  static int64_t Pause() {
    entered_.store(true);
    auto end = std::chrono::steady_clock::now() + std::chrono::seconds(12);
    while (!released_.load() && std::chrono::steady_clock::now() < end) std::this_thread::yield();
    if (!released_.load()) expired_.store(true);
    return taosGetTimestampNs();
  }

 private:
  Stub                     stub_;
  static std::atomic<bool> entered_, released_, expired_;
};
std::atomic<bool> PrivateBuildPause::entered_{false};
std::atomic<bool> PrivateBuildPause::released_{false};
std::atomic<bool> PrivateBuildPause::expired_{false};

TEST_F(MndStreamOrigReaderReplyTest, TaskRowsSurviveResetWhileAnotherStreamAddsReaderAndStops) {
  SCMCreateStreamReq create = {};
  create.streamId = streamId_ + 1;
  SStreamObj stream = {};
  stream.pCreate = &create;
  tstrncpy(stream.name, "1.db.large", sizeof(stream.name));
  SStmStatus        status = {};
  constexpr int32_t taskCount = 512;
  for (int32_t i = 0; i < taskCount; ++i) {
    auto *task = appendTask(&status.trigReaders, STREAM_READER_TASK, STREAM_FLAG_TRIGGER_READER);
    ASSERT_NE(nullptr, task);
    task->id.taskId = i + 1;
  }
  auto *last = static_cast<SStmTaskStatus *>(taosArrayGetLast(status.trigReaders));
  last->status = STREAM_STATUS_FAILED;
  last->errCode = TSDB_CODE_OUT_OF_MEMORY;
  last->extraErrMsg = taosStrdup("reader detail");
  ASSERT_NE(nullptr, last->extraErrMsg);
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.streamMap, &create.streamId, sizeof(create.streamId), &status, sizeof(status)));
  auto *live = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &create.streamId, sizeof(create.streamId)));
  ASSERT_NE(nullptr, live);
  auto *block = createSystemTableBlock(TSDB_INS_TABLE_STREAM_TASKS, 1);
  ASSERT_NE(nullptr, block);

  PrivateBuildPause pause(false);
  static Stub      *gate;
  gate = &stub_;
  stub_.set(
      blockDataEnsureCapacity, +[](SSDataBlock *block, uint32_t rows) -> int32_t {
        gate->reset(blockDataEnsureCapacity);
        PrivateBuildPause::Pause();
        return blockDataEnsureCapacity(block, rows);
      });
  int32_t     rows = 0;
  std::thread query([&] { EXPECT_EQ(0, mstSetStreamTasksResBlock(&stream, block, &rows, 1)); });
  EXPECT_TRUE(pause.Entered());
  auto savedState = mStreamMgmt.state;
  mStreamMgmt.state = MND_STM_STATE_NORMAL;
  stored_->lastTrigMgmtReqId = 0;
  taosArrayClear(readers_);
  request();
  auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(heartbeat_.rsps.rspList, 0));
  EXPECT_NE(nullptr, reply);
  if (reply) {
    EXPECT_EQ(0, reply->code);
    EXPECT_EQ(1u, taosArrayGetSize(reply->cont.readerList));
  }
  EXPECT_EQ(1, msmGetTrigOReaderSize(stored_->trigOReaders));
  msmUndeployStream(&mnode_, streamId_, row_.name);
  EXPECT_EQ(2, stored_->stopped);
  taosWLockLatch(&mStreamMgmt.runtimeLock);
  mstResetSStmStatus(live);
  taosWUnLockLatch(&mStreamMgmt.runtimeLock);
  pause.Release();
  query.join();
  mStreamMgmt.state = savedState;

  EXPECT_EQ(taskCount, rows);
  EXPECT_EQ(rows, block->info.rows);
  if (rows == taskCount) {
    EXPECT_EQ("1", getVarCharValue(block, 2, 0));
    EXPECT_EQ("200", getVarCharValue(block, 2, taskCount - 1));
    EXPECT_NE(std::string::npos, getVarCharValue(block, 13, taskCount - 1).find("reader detail"));
  }
  blockDataDestroy(block);
}

TEST_F(MndStreamRecalcPersistenceTest, HealthAndQueriesFinishDuringPrivateRedeploy) {
  SStreamObj *stream = nullptr;
  ASSERT_EQ(0, mndAcquireStreamById(&mnode_, kStreamA, &stream));
  Status(kStreamA)->streamName = taosStrdup(stream->name);
  Status(kStreamB)->streamName = taosStrdup("1.test.recalc_b");
  ASSERT_EQ(0, tCloneStreamCreateDeployPointers(stream->pCreate, &Status(kStreamA)->pCreate));
  ASSERT_NE(InstallTriggerTask(), nullptr);
  SnodeLookupStubGuard snodeLookup;
  Stub                 placement;
  placement.set(msmAssignTaskSnodeId, returnRecoverySnodeId);
  auto *streams = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  auto *tasks = createSystemTableBlock(TSDB_INS_TABLE_STREAM_TASKS, 1);
  ASSERT_NE(nullptr, streams);
  ASSERT_NE(nullptr, tasks);
  ASSERT_EQ(0, mstPostStreamAction(mStreamMgmt.actionQ, kStreamA, stream->name, nullptr, false, STREAM_ACT_DEPLOY));
  PrivateBuildPause pause;
  SStreamHbMsg      request = {};
  request.dnodeId = 1;
  ASSERT_EQ(0, msmInitDeployWorker(&mnode_));
  EXPECT_EQ(0, HandleHeartbeat(&request));
  EXPECT_TRUE(pause.Entered());
  int64_t prior = mStreamMgmt.hCtx.currentTs;
  msmHealthCheck(&mnode_);
  EXPECT_GT(mStreamMgmt.hCtx.currentTs, prior);
  SStreamHbMsg other = {};
  other.dnodeId = 1;
  other.streamGId = 1;
  EXPECT_EQ(0, HandleHeartbeat(&other));
  EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, stream, streams, 0, 0));
  int32_t rows = 1;
  EXPECT_EQ(0, mstSetStreamTasksResBlock(stream, tasks, &rows, 1));
  EXPECT_EQ(2, rows);
  EXPECT_EQ(100, Status(kStreamA)->triggerTask->id.taskId);
  atomic_store_8(&stream->userStopped, 1);
  msmUndeployStream(&mnode_, kStreamA, stream->name);
  pause.Release();
  msmCleanupDeployWorker(&mnode_);
  EXPECT_EQ(2, Status(kStreamA)->stopped);
  EXPECT_EQ(100, Status(kStreamA)->triggerTask->id.taskId);
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.taskMap));
  blockDataDestroy(tasks);
  blockDataDestroy(streams);
  mndReleaseStream(&mnode_, stream);
}

extern "C" void msmCheckVgroupStatus(SMnode *);
extern "C" void msmCheckTaskListStatus(int64_t streamId, SStmTaskStatus **tasks, int32_t count);

TEST_F(MndStreamRecalcPersistenceTest, SameGroupReportsKeepHealthyTasksRunningDuringPrivateDeploy) {
  auto *healthy = Status(kStreamB);
  auto *trigger = InstallTriggerTask(kStreamB);
  ASSERT_NE(nullptr, trigger);
  healthy->runners[0] = taosArrayInit(1, sizeof(SStmTaskStatus));
  ASSERT_NE(nullptr, healthy->runners[0]);
  healthy->runnerDeploys = 1;
  SStmTaskStatus runner = {};
  runner.type = STREAM_RUNNER_TASK;
  runner.id.taskId = 101;
  runner.id.seriousId = 201;
  runner.id.nodeId = 1;
  runner.status = STREAM_STATUS_RUNNING;
  runner.pStream = healthy;
  auto *running = static_cast<SStmTaskStatus *>(taosArrayPush(healthy->runners[0], &runner));
  ASSERT_NE(nullptr, running);
  int64_t runnerKey[] = {kStreamB, 101};
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.taskMap, runnerKey, sizeof(runnerKey), &running, POINTER_BYTES));

  SStreamObj *stream = nullptr;
  ASSERT_EQ(0, mndAcquireStreamById(&mnode_, kStreamA, &stream));
  Status(kStreamA)->streamName = taosStrdup(stream->name);
  ASSERT_EQ(0, tCloneStreamCreateDeployPointers(stream->pCreate, &Status(kStreamA)->pCreate));
  ASSERT_EQ(0, mstPostStreamAction(mStreamMgmt.actionQ, kStreamA, stream->name, nullptr, false, STREAM_ACT_DEPLOY));
  Stub placement;
  placement.set(msmAssignTaskSnodeId, returnRecoverySnodeId);
  SStreamHbMsg heartbeat = {};
  heartbeat.dnodeId = 1;
  heartbeat.pStreamStatus = taosArrayInit(2, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(nullptr, heartbeat.pStreamStatus);
  SStmTaskStatus *tasks[] = {trigger, running};
  for (auto *task : tasks) {
    SStmTaskStatusMsg report = {};
    report.type = task->type;
    report.streamId = kStreamB;
    report.taskId = task->id.taskId;
    report.seriousId = task->id.seriousId;
    report.nodeId = 1;
    report.status = STREAM_STATUS_RUNNING;
    report.detailStatus = -1;
    ASSERT_NE(nullptr, taosArrayPush(heartbeat.pStreamStatus, &report));
  }

  PrivateBuildPause pause;
  ASSERT_EQ(0, msmInitDeployWorker(&mnode_));
  // The same heartbeat worker must return from the first report to process the next one.
  EXPECT_EQ(0, HandleHeartbeat(&heartbeat, 100));
  EXPECT_TRUE(pause.Entered());
  int64_t next = 100 + MST_ISOLATION_DURATION / 2;
  EXPECT_EQ(0, HandleHeartbeat(&heartbeat, next));
  int64_t saved = mStreamMgmt.hCtx.currentTs;
  mStreamMgmt.hCtx.currentTs = 100 + MST_ISOLATION_DURATION + 1;
  msmCheckTaskListStatus(kStreamB, tasks, 2);
  mStreamMgmt.hCtx.currentTs = saved;
  EXPECT_EQ(0, healthy->stopped);
  EXPECT_EQ(100, trigger->id.taskId);
  EXPECT_EQ(101, running->id.taskId);
  EXPECT_EQ(200, trigger->id.seriousId);
  EXPECT_EQ(201, running->id.seriousId);
  for (auto *task : tasks) {
    EXPECT_EQ(next, task->lastUpTs);
    EXPECT_EQ(STREAM_STATUS_RUNNING, task->status);
  }
  atomic_store_8(&stream->userStopped, 1);
  pause.Release();
  msmCleanupDeployWorker(&mnode_);
  tCleanupStreamHbMsg(&heartbeat, true);
  mndReleaseStream(&mnode_, stream);
}

class IsolatedStreamRuntime {
 public:
  explicit IsolatedStreamRuntime(SMnode *node) : saved_(mStreamMgmt), mnode_(node) { mStreamMgmt = {}; }
  ~IsolatedStreamRuntime() {
    msmDestroyRuntimeInfo(mnode_);
    mStreamMgmt = saved_;
  }

 private:
  SStmRuntime saved_;
  SMnode     *mnode_;
};

#ifdef TD_ENTERPRISE
static void PauseExternalReaderSchema(Stub &stub) {
  stub.set(
      mndAcquireExtSource, +[](SMnode *, const char *) -> SExtSourceObj * {
        static SExtSourceObj source = {};
        source.type = EXT_SOURCE_MYSQL;
        return &source;
      });
  stub.set(mndReleaseExtSource, +[](SMnode *, SExtSourceObj *) {});
  stub.set(extConnectorOpen, +[](const SExtSourceCfg *, SExtConnectorHandle **) -> int32_t { return 0; });
  stub.set(
      extConnectorGetTableSchema, +[](SExtConnectorHandle *, const SExtTableNode *, SExtTableMeta **) -> int32_t {
        PrivateBuildPause::Pause();
        return TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
      });
}

static void InitExternalReaderRequest(SStmTaskStatusMsg *task, int64_t streamId, const SStmTaskId &id) {
  task->streamId = streamId;
  task->type = STREAM_TRIGGER_TASK;
  task->taskId = id.taskId;
  task->seriousId = id.seriousId;
  task->nodeId = id.nodeId;
  task->pMgmtReq = static_cast<SStreamMgmtReq *>(taosMemoryCalloc(1, sizeof(SStreamMgmtReq)));
  ASSERT_NE(nullptr, task->pMgmtReq);
  task->pMgmtReq->type = STREAM_MGMT_REQ_TRIGGER_MIXED_ORIGTBL_READER;
  task->pMgmtReq->cont.pExtOrigTblReaders = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderReq), 1);
  auto *ext = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(task->pMgmtReq->cont.pExtOrigTblReaders, 0));
  ASSERT_NE(nullptr, ext);
  ext->registryEntryId = 1;
  ext->protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
  ext->capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START;
  tstrncpy(ext->identity.sourceName, "remote", sizeof(ext->identity.sourceName));
  tstrncpy(ext->identity.dbName, "metrics", sizeof(ext->identity.dbName));
  tstrncpy(ext->identity.tableName, "cpu", sizeof(ext->identity.tableName));
}

TEST_F(MndStreamRecalcPersistenceTest, LeadershipChangeDuringReaderRequestPreservesNewGroupState) {
  IsolatedStreamRuntime runtime(&mnode_);
  Stub                  topology;
  topology.set(streamAddVnodeLeader, ignoreRuntimeVnodeLeaderRemoval);
  topology.set(streamRemoveVnodeLeader, ignoreRuntimeVnodeLeaderRemoval);
  msmHandleBecomeLeader(&mnode_);
  ASSERT_EQ(1, mStreamMgmt.active);
  atomic_store_8(&mStreamMgmt.state, MND_STM_STATE_NORMAL);
  SStreamHbMsg heartbeat = {};
  heartbeat.dnodeId = 1;
  int64_t seen = 0;
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.dnodeMap, &heartbeat.dnodeId, sizeof(heartbeat.dnodeId), &seen, sizeof(seen)));
  SStmStatus status = {};
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA), &status, sizeof(status)));
  Status()->streamName = taosStrdup("1.test.recalc_a");
  ASSERT_NE(nullptr, Status()->streamName);
  SStreamObj *stream = nullptr;
  ASSERT_EQ(0, mndAcquireStream(&mnode_, Status()->streamName, &stream));
  ASSERT_EQ(0, tCloneStreamCreateDeployPointers(stream->pCreate, &Status()->pCreate));
  mndReleaseStream(&mnode_, stream);
  const int64_t sentinelId = kStreamB;
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.streamMap, &sentinelId, sizeof(sentinelId), &status, sizeof(status)));
  auto *healthy = InstallTriggerTask(kStreamB);
  ASSERT_NE(nullptr, healthy);
  auto *trigger = InstallTriggerTask();
  ASSERT_NE(nullptr, trigger);
  heartbeat.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 1);
  heartbeat.pStreamReq = taosArrayInit_s(sizeof(int32_t), 1);
  auto *task = static_cast<SStmTaskStatusMsg *>(taosArrayGet(heartbeat.pStreamStatus, 0));
  ASSERT_NE(nullptr, task);
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(task, kStreamA, trigger->id));
  SStmTaskStatusMsg other = {};
  other.type = STREAM_TRIGGER_TASK;
  other.streamId = kStreamB;
  other.taskId = healthy->id.taskId;
  other.seriousId = healthy->id.seriousId;
  other.nodeId = healthy->id.nodeId;
  other.status = STREAM_STATUS_RUNNING;
  other.detailStatus = -1;
  ASSERT_NE(nullptr, taosArrayPush(heartbeat.pStreamStatus, &other));
  task = static_cast<SStmTaskStatusMsg *>(taosArrayGet(heartbeat.pStreamStatus, 0));
  PrivateBuildPause pause(false);
  PauseExternalReaderSchema(topology);
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  task->status = STREAM_STATUS_RUNNING;
  EXPECT_EQ(0, HandleHeartbeat(&heartbeat));
  EXPECT_NE(nullptr, Status()->pendingSchema);
  EXPECT_TRUE(pause.Entered());
  // A retransmission on this same group must update both tasks while the provider is blocked.
  auto *pending = Status()->pendingSchema;
  InitExternalReaderRequest(task, kStreamA, trigger->id);
  int64_t next = 100 + MST_ISOLATION_DURATION / 2;
  EXPECT_EQ(0, HandleHeartbeat(&heartbeat, next));
  EXPECT_EQ(pending, Status()->pendingSchema);
  EXPECT_EQ(next, healthy->lastUpTs);
  mStreamMgmt.hCtx.currentTs = 100 + MST_ISOLATION_DURATION + 1;
  msmCheckTaskListStatus(kStreamB, &healthy, 1);
  EXPECT_EQ(0, Status(kStreamB)->stopped);
  EXPECT_EQ(other.taskId, healthy->id.taskId);
  EXPECT_EQ(other.seriousId, healthy->id.seriousId);
  msmHandleBecomeNotLeader(&mnode_);
  msmHandleBecomeLeader(&mnode_);
  auto         *newGroup = mStreamMgmt.tCtx[0].actionStm[0];
  SStmAction    action = {};
  EXPECT_EQ(0, taosHashPut(newGroup, &sentinelId, sizeof(sentinelId), &action, sizeof(action)));
  pause.Release();
  msmCleanupSchemaWorker(&mnode_);
  EXPECT_NE(nullptr, taosHashGet(newGroup, &sentinelId, sizeof(sentinelId)));
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
  tCleanupStreamHbMsg(&heartbeat, true);
}
#endif

TEST_F(MndStreamRecalcPersistenceTest, LeadershipChangeDuringBuildDiscardsOldTasks) {
  IsolatedStreamRuntime runtime(&mnode_);
  Stub topology;
  topology.set(streamAddVnodeLeader, ignoreRuntimeVnodeLeaderRemoval);
  topology.set(streamRemoveVnodeLeader, ignoreRuntimeVnodeLeaderRemoval);
  topology.set(msmAssignTaskSnodeId, returnRecoverySnodeId);
  msmHandleBecomeLeader(&mnode_);
  ASSERT_EQ(1, mStreamMgmt.active);
  atomic_store_8(&mStreamMgmt.state, MND_STM_STATE_NORMAL);
  int     node = 1;
  int64_t ts = 0;
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.dnodeMap, &node, sizeof(node), &ts, sizeof(ts)));
  char name[] = "1.test.recalc_a";
  ASSERT_EQ(0, mstPostStreamAction(mStreamMgmt.actionQ, kStreamA, name, nullptr, false, STREAM_ACT_DEPLOY));
  PrivateBuildPause pause;
  SStreamHbMsg      req = {};
  req.dnodeId = node;
  ASSERT_EQ(0, msmInitDeployWorker(&mnode_));
  EXPECT_EQ(0, HandleHeartbeat(&req));
  EXPECT_TRUE(pause.Entered());
  char waitingName[] = "1.test.recalc_b";
  EXPECT_EQ(0, mstPostStreamCreate(kStreamB, waitingName));
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.actionQ->queuedCreates));
  msmHandleBecomeNotLeader(&mnode_);
  msmHandleBecomeLeader(&mnode_);
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.actionQ->queuedCreates));
  EXPECT_EQ(0, mstPostStreamCreate(kStreamB, waitingName));
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.actionQ->queuedCreates));
  pause.Release();
  msmCleanupDeployWorker(&mnode_);
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
}

TEST_F(MndStreamRecalcPersistenceTest, FinishCommitAndRecalculateQueryCompleteWithRuntimeWriterWaiting) {
  STimeWindow range = {100, 200};
  ASSERT_EQ(AcceptA(range), TSDB_CODE_SUCCESS);
  const int64_t recalcId = FirstCapturedRecalcId();
  CommitNext();
  ASSERT_NE(InstallTriggerTask(), nullptr);
  Record(kStreamA, recalcId)->dispatchConfirmed = true;

  SStreamHbMsg heartbeat = {};
  ASSERT_NE(BuildTerminalHeartbeat(recalcId, &heartbeat), nullptr);
  ASSERT_EQ(HandleHeartbeat(&heartbeat), TSDB_CODE_SUCCESS);
  tCleanupStreamHbMsg(&heartbeat, true);
  ASSERT_TRUE(Record(kStreamA, recalcId)->terminalPersisting);

  SStreamObj *pStream = nullptr;
  ASSERT_EQ(mndAcquireStreamById(&mnode_, kStreamA, &pStream), TSDB_CODE_SUCCESS);
  ASSERT_NE(pStream, nullptr);
  GateSnapshotDup(pStream->pIncompleteRecalcs);
  mndReleaseStream(&mnode_, pStream);

  Stub waitProbe;
  waitProbe.set(mstWaitLock, observeRecalcRuntimeLockWait);
  EXPECT_EXIT(
      {
        // Preserve the real completion callback and the cross-thread wait that
        // the ordinary prepare stub omits. A pending writer must not deadlock it.
        const auto queryRecalculates = [&](bool finished) {
          SSDataBlock *pBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAM_RECALCULATES, 128);
          ASSERT_NE(pBlock, nullptr);
          SRpcMsg request = {};
          request.info.node = &mnode_;
          SShowObj   show = {};
          const auto retrieve = mnode_.showMgmt.retrieveFps[TSDB_MGMT_TABLE_STREAM_RECALCULATES];
          EXPECT_EQ(retrieve(&request, &show, pBlock, 128), 1);
          EXPECT_EQ(pBlock->info.rows, 1);
          if (pBlock->info.rows == 1) {
            EXPECT_EQ(getVarCharValue(pBlock, 0, 0), "recalc_a");
            if (finished) {
              EXPECT_EQ(getVarCharValue(pBlock, 5, 0), "100%");
              EXPECT_EQ(getVarCharValue(pBlock, 6, 0), "Finished");
            }
          }
          blockDataDestroy(pBlock);
        };
        harness_.onPrepare = [&]() {
          std::thread sync([&]() { CommitNext(true); });
          sync.join();
        };
        std::atomic<bool> pullupFinished{false};
        std::atomic<bool> writerFinished{false};
        std::atomic<bool> queryFinished{false};
        std::thread       pullup([&]() {
          RunImmediateWake();
          pullupFinished.store(true);
        });
        auto              deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!harness_.snapshotDupEntered.load() && std::chrono::steady_clock::now() < deadline) {
          std::this_thread::yield();
        }
        if (!harness_.snapshotDupEntered.load()) std::_Exit(2);
        std::thread writer([&]() {
          taosWWaitLockLatch(&mStreamMgmt.runtimeLock);
          taosWUnLockLatch(&mStreamMgmt.runtimeLock);
          writerFinished.store(true);
        });
        deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!taosHasRWWFlag(&mStreamMgmt.runtimeLock) && !writerFinished.load() &&
               std::chrono::steady_clock::now() < deadline) {
          std::this_thread::yield();
        }
        if (!taosHasRWWFlag(&mStreamMgmt.runtimeLock)) std::_Exit(3);
        // The status-table reader in defect 7153905841 waits behind this writer.
        std::thread query([&]() {
          queryRecalculates(false);
          queryFinished.store(true);
        });
        deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!harness_.queryRuntimeWaiting.load() && std::chrono::steady_clock::now() < deadline) {
          std::this_thread::yield();
        }
        if (!harness_.queryRuntimeWaiting.load()) std::_Exit(4);
        harness_.releaseSnapshotDup.store(true);
        deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while ((!pullupFinished.load() || !writerFinished.load() || !queryFinished.load()) &&
               std::chrono::steady_clock::now() < deadline) {
          std::this_thread::yield();
        }
        if (!pullupFinished.load() || !writerFinished.load() || !queryFinished.load()) std::_Exit(5);
        pullup.join();
        writer.join();
        query.join();
        EXPECT_FALSE(PersistedRequestExists(kStreamA, recalcId));
        EXPECT_EQ(Record(kStreamA, recalcId)->snapshot.status, STREAM_RECALC_STATUS_FINISHED);
        EXPECT_FALSE(Status()->recalcTransActive);
        EXPECT_TRUE(isListEmpty(&Status()->recalcPersistOps));
        queryRecalculates(true);
        std::_Exit(::testing::Test::HasFailure() ? 6 : 0);
      },
      ::testing::ExitedWithCode(0), "");
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

TEST_F(MndStreamRecalcPersistenceTest, PullupWrapIncludesCursorStream) {
  const STimeWindow range = {100, 200};
  ASSERT_EQ(Accept(kStreamA, range, 1), TSDB_CODE_SUCCESS);
  CommitNext();

  SStmRecalcRecord *pRecord = OnlyRecord(kStreamA);
  ASSERT_NE(pRecord, nullptr);
  pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  SStreamRecalcTerminalCandidate candidate = {};
  candidate.snapshot = pRecord->snapshot;
  candidate.snapshot.status = STREAM_RECALC_STATUS_FINISHED;
  candidate.snapshot.progressPct = 100;
  harness_.prepareCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);
  ASSERT_EQ(mndStreamRecalcFinish(&mnode_, kStreamA, &candidate), TSDB_CODE_SUCCESS);
  ASSERT_FALSE(Status(kStreamA)->recalcTransActive);

  harness_.createStreamIds.clear();
  mStreamMgmt.recalcPullupCursor = kStreamA;
  mStreamMgmt.recalcPullupCursorValid = 1;
  atomic_store_8(&mStreamMgmt.recalcPullupPending, 1);
  mndStreamRecalcPullup(&mnode_);

  ASSERT_EQ(harness_.createStreamIds.size(), 1U);
  EXPECT_EQ(harness_.createStreamIds.front(), kStreamA);
  EXPECT_TRUE(Status(kStreamA)->recalcTransActive);
}

static Stub* gRecalcPullupIterationStub = nullptr;
static int32_t gRecalcPullupIterations = 0;

static void* countRecalcPullupIterations(SHashObj* pHash, void* pIter) {
  if (pHash == mStreamMgmt.streamMap) ++gRecalcPullupIterations;
  gRecalcPullupIterationStub->reset(taosHashIterate);
  void* pNext = taosHashIterate(pHash, pIter);
  gRecalcPullupIterationStub->set(taosHashIterate, countRecalcPullupIterations);
  return pNext;
}

TEST_F(MndStreamRecalcPersistenceTest, PullupRotatesPastFailedBatchBeforeRetryingIt) {
  constexpr int32_t streamCount = 10;
  std::vector<int64_t> streamIds = {kStreamA, kStreamB};
  for (int32_t i = 2; i < streamCount; ++i) {
    const int64_t streamId = kStreamA + i;
    const std::string name = "1.test.recalc_fair_" + std::to_string(i);
    AddStream(streamId, name.c_str());
    streamIds.push_back(streamId);
  }

  const STimeWindow range = {100, 200};
  for (int32_t i = 0; i < streamCount; ++i) {
    ASSERT_EQ(Accept(streamIds[i], range, i + 1), TSDB_CODE_SUCCESS);
  }
  for (int32_t i = 0; i < streamCount; ++i) CommitNext();

  for (int32_t i = 0; i < streamCount; ++i) {
    SStmRecalcRecord* pRecord = OnlyRecord(streamIds[i]);
    ASSERT_NE(pRecord, nullptr);
    pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;
    SStreamRecalcTerminalCandidate candidate = {};
    candidate.snapshot = pRecord->snapshot;
    candidate.snapshot.status = STREAM_RECALC_STATUS_FINISHED;
    candidate.snapshot.progressPct = 100;
    harness_.prepareCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);
    ASSERT_EQ(mndStreamRecalcFinish(&mnode_, streamIds[i], &candidate), TSDB_CODE_SUCCESS);
    EXPECT_FALSE(Status(streamIds[i])->recalcTransActive);
  }

  harness_.createStreamIds.clear();
  for (int32_t i = 0; i < streamCount; ++i) harness_.prepareCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);
  atomic_store_8(&mStreamMgmt.recalcPullupPending, 1);
  mndStreamRecalcPullup(&mnode_);
  ASSERT_EQ(harness_.createStreamIds.size(), 8U);
  const std::vector<int64_t> firstBatch = harness_.createStreamIds;
  EXPECT_TRUE(harness_.transactions.empty());
  const size_t queuedAfterFirstBatch = harness_.queuedMessages.size();

  ASSERT_EQ(atomic_load_8(&mStreamMgmt.recalcPullupPending), 1);
  Stub iterationStub;
  gRecalcPullupIterations = 0;
  gRecalcPullupIterationStub = &iterationStub;
  iterationStub.set(taosHashIterate, countRecalcPullupIterations);
  mndStreamRecalcPullup(&mnode_);
  iterationStub.reset(taosHashIterate);
  gRecalcPullupIterationStub = nullptr;
  EXPECT_EQ(gRecalcPullupIterations, streamCount - static_cast<int32_t>(firstBatch.size()));
  ASSERT_EQ(harness_.createStreamIds.size(), static_cast<size_t>(streamCount));
  EXPECT_TRUE(harness_.transactions.empty());
  EXPECT_EQ(std::find(firstBatch.begin(), firstBatch.end(), harness_.createStreamIds[8]), firstBatch.end());
  EXPECT_EQ(std::find(firstBatch.begin(), firstBatch.end(), harness_.createStreamIds[9]), firstBatch.end());
  EXPECT_EQ(harness_.queuedMessages.size(), queuedAfterFirstBatch);
  EXPECT_EQ(mStreamMgmt.recalcPullupRoundRemaining, 0);
  EXPECT_EQ(atomic_load_8(&mStreamMgmt.recalcPullupPending), 1);
}

TEST_F(MndStreamRecalcPersistenceTest, PullupResumesWhenCursorWasRemoved) {
  const STimeWindow first = {100, 200};
  const STimeWindow second = {300, 400};
  ASSERT_EQ(Accept(kStreamA, first, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(Accept(kStreamA, second, 2), TSDB_CODE_SUCCESS);
  CommitNext();
  ASSERT_FALSE(Status(kStreamA)->recalcTransActive);
  mStreamMgmt.recalcPullupCursor = kStreamA + 1000;
  mStreamMgmt.recalcPullupCursorValid = 1;
  atomic_store_8(&mStreamMgmt.recalcPullupPending, 1);
  mndStreamRecalcPullup(&mnode_);
  EXPECT_TRUE(Status(kStreamA)->recalcTransActive);
  EXPECT_EQ(harness_.createCalls, 2);
}

TEST_F(MndStreamRecalcPersistenceTest, QueuedStreamsReserveDistinctTransactionIdsBeforeCompletion) {
  const STimeWindow first = {100, 200};
  const STimeWindow second = {300, 400};
  ASSERT_EQ(Accept(kStreamA, first, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(Accept(kStreamA, second, 2), TSDB_CODE_SUCCESS);
  ASSERT_EQ(Accept(kStreamB, first, 3), TSDB_CODE_SUCCESS);
  ASSERT_EQ(Accept(kStreamB, second, 4), TSDB_CODE_SUCCESS);
  CommitNext();
  CommitNext();
  ASSERT_TRUE(harness_.transactions.empty());

  // Use the real allocator; a pointer-only transaction stub hides ID reuse.
  KeepRealTransactionsInSdb();
  RunImmediateWake();

  ASSERT_EQ(harness_.transactions.size(), 2U);
  EXPECT_NE(harness_.transactions[0].transId, harness_.transactions[1].transId);
  EXPECT_EQ(sdbGetSize(mnode_.pSdb, SDB_TRANS), 2);
  EXPECT_FALSE(harness_.prepareWhileRuntimeRead.load());
  for (const auto &captured : harness_.transactions) {
    STrans *pStored = mndAcquireTrans(&mnode_, captured.transId);
    ASSERT_NE(pStored, nullptr);
    EXPECT_EQ(pStored->paramLen, captured.callbackParam.size());
    if (pStored->paramLen == captured.callbackParam.size()) {
      EXPECT_EQ(std::memcmp(pStored->param, captured.callbackParam.data(), pStored->paramLen), 0);
    }
    mndReleaseTrans(&mnode_, pStored);
  }
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

TEST_F(MndStreamRecalcPersistenceTest, QueuedTransactionPreparationDoesNotHoldRuntimeLock) {
  STimeWindow first = {100, 200};
  STimeWindow second = {300, 400};
  ASSERT_EQ(AcceptA(first, 101), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 102), TSDB_CODE_SUCCESS);
  CommitNext();
  RunImmediateWake();

  EXPECT_EQ(harness_.createCalls, 2);
  EXPECT_FALSE(harness_.prepareWhileRuntimeRead.load());
  CommitNext();
  EXPECT_EQ(VisibleCount(), 2);
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

TEST_F(MndStreamRecalcPersistenceTest, RemovedStatusDuringCreationFailureRepliesExactlyOnce) {
  ASSERT_EQ(AcceptA({100, 200}, 101), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA({300, 400}, 102), TSDB_CODE_SUCCESS);
  CommitNext();
  harness_.createCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);
  harness_.beforeCreate = [&]() {
    ASSERT_FALSE(runtimeReadPreventsWriter());
    taosWLockLatch(&mStreamMgmt.runtimeLock);
    DestroyStatus();
    const int64_t streamId = kStreamA;
    EXPECT_EQ(taosHashRemove(mStreamMgmt.streamMap, &streamId, sizeof(streamId)), TSDB_CODE_SUCCESS);
    taosWUnLockLatch(&mStreamMgmt.runtimeLock);
    EXPECT_TRUE(harness_.responseCodes.empty());  // RPC has already transferred.
  };
  RunImmediateWake();
  ASSERT_EQ(harness_.responseCodes.size(), 1U);
  EXPECT_EQ(harness_.responseCodes.front(), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(harness_.responseHandles.front(), reinterpret_cast<void *>(102));
  EXPECT_FALSE(harness_.responseWhileRuntimeRead.load());
  RunPeriodicTimer();
  EXPECT_EQ(harness_.responseCodes.size(), 1U);
}

TEST_F(MndStreamRecalcPersistenceTest, RemovedStatusDuringPreparationFailureRepliesExactlyOnce) {
  ASSERT_EQ(AcceptA({100, 200}, 101), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA({300, 400}, 102), TSDB_CODE_SUCCESS);
  CommitNext();
  harness_.prepareCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);
  harness_.beforePrepare = [&]() {
    ASSERT_FALSE(runtimeReadPreventsWriter());
    taosWLockLatch(&mStreamMgmt.runtimeLock);
    DestroyStatus();
    const int64_t streamId = kStreamA;
    EXPECT_EQ(taosHashRemove(mStreamMgmt.streamMap, &streamId, sizeof(streamId)), TSDB_CODE_SUCCESS);
    taosWUnLockLatch(&mStreamMgmt.runtimeLock);
    EXPECT_TRUE(harness_.responseCodes.empty());  // RPC has already transferred.
  };
  RunImmediateWake();
  ASSERT_EQ(harness_.responseCodes.size(), 1U);
  EXPECT_EQ(harness_.responseCodes.front(), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(harness_.responseHandles.front(), reinterpret_cast<void *>(102));
  EXPECT_FALSE(harness_.responseWhileRuntimeRead.load());
  RunPeriodicTimer();
  EXPECT_EQ(harness_.responseCodes.size(), 1U);
}

TEST_F(MndStreamRecalcPersistenceTest, PullupPrepareFailureAfterRuntimeRemovalRepliesOnce) {
  STimeWindow first = {100, 200};
  STimeWindow second = {300, 400};
  ASSERT_EQ(AcceptA(first, 21), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 22), TSDB_CODE_SUCCESS);
  CommitNext();

  harness_.prepareCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);
  harness_.onPrepare = [&]() { EXPECT_TRUE(ClearRuntimeStreams()); };
  RunImmediateWake();
  EXPECT_EQ(mStreamMgmt.streamMap, nullptr);
  ASSERT_EQ(harness_.responseCodes.size(), 1U);
  EXPECT_EQ(harness_.responseCodes.front(), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(harness_.responseHandles.front(), reinterpret_cast<void *>(22));
  RunPeriodicTimer();
  EXPECT_EQ(harness_.responseCodes.size(), 1U);
}

TEST_F(MndStreamRecalcPersistenceTest, PullupPrepareFailurePreservesReplacementRuntime) {
  STimeWindow first = {100, 200};
  STimeWindow second = {300, 400};
  STimeWindow replacement = {500, 600};
  ASSERT_EQ(AcceptA(first, 31), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 32), TSDB_CODE_SUCCESS);
  CommitNext();

  bool replaced = false;
  harness_.prepareCodes.push_back(TSDB_CODE_OUT_OF_MEMORY);
  harness_.onPrepare = [&]() {
    if (replaced) return;
    replaced = true;
    ASSERT_TRUE(ClearRuntimeStreams());
    taosWLockLatch(&mStreamMgmt.runtimeLock);
    mStreamMgmt.streamMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    ++mStreamMgmt.stat.activeTimes;
    taosWUnLockLatch(&mStreamMgmt.runtimeLock);
    ASSERT_NE(mStreamMgmt.streamMap, nullptr);
    AddStream(kStreamA, "1.test.recalc_a");
    atomic_store_8(&mStreamMgmt.active, 1);
    ASSERT_EQ(AcceptA(replacement, 33), TSDB_CODE_SUCCESS);
  };
  RunImmediateWake();
  ASSERT_TRUE(replaced);
  ASSERT_NE(Status(), nullptr);
  EXPECT_TRUE(Status()->recalcTransActive);
  EXPECT_EQ(listNEles(&Status()->recalcPersistOps), 1);
  ASSERT_EQ(harness_.transactions.size(), 1U);
  ASSERT_EQ(harness_.responseCodes.size(), 1U);
  EXPECT_EQ(harness_.responseCodes.front(), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(harness_.responseHandles.front(), reinterpret_cast<void *>(32));
  ASSERT_NE(OnlyRecord(), nullptr);
  const int64_t replacementId = OnlyRecord()->snapshot.recalcId;
  CommitNext();
  EXPECT_TRUE(PersistedRequestExists(kStreamA, replacementId));
  EXPECT_FALSE(Status()->recalcTransActive);
  ASSERT_NE(Record(kStreamA, replacementId), nullptr);
  EXPECT_TRUE(Record(kStreamA, replacementId)->visible);
}

TEST_F(MndStreamRecalcPersistenceTest, PullupSnapshotAllocationFailureRepliesAndRemovesReservation) {
  STimeWindow first = {100, 200};
  STimeWindow second = {300, 400};
  ASSERT_EQ(AcceptA(first, 41), TSDB_CODE_SUCCESS);
  ASSERT_EQ(AcceptA(second, 42), TSDB_CODE_SUCCESS);
  CommitNext();
  {
    // Bounded pullup traverses in place, so allocation fails while copying the
    // claimed request snapshot. The accepted RPC must receive its failure.
    RecalcCallocFailureGuard failure;
    RunImmediateWake();
  }
  EXPECT_EQ(harness_.createCalls, 1);
  EXPECT_FALSE(Status()->recalcTransActive);
  EXPECT_TRUE(isListEmpty(&Status()->recalcPersistOps));
  EXPECT_EQ(VisibleCount(), 1);
  EXPECT_EQ(taosArrayGetSize(Status()->recalcRecords), 1);
  ASSERT_EQ(harness_.responseCodes.size(), 1U);
  EXPECT_EQ(harness_.responseCodes.front(), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(harness_.responseHandles.front(), reinterpret_cast<void *>(42));
  EXPECT_FALSE(harness_.responseWhileRuntimeRead.load());
  RunPeriodicTimer();
  EXPECT_EQ(harness_.createCalls, 1);
  EXPECT_TRUE(harness_.transactions.empty());
  EXPECT_EQ(harness_.responseCodes.size(), 1U);
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
    timeStub_.set(taosClockGetTime, returnRecalcClock);
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
    timeStub_.reset(taosClockGetTime);
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
    stub_.set(sdbGetTableVer, +[](SSdb *, ESdbType) -> int64_t { return 0; });
  }

  void TearDown() override {
    stub_.reset(sdbGetTableVer);
    stub_.reset(msmAssignTaskSnodeId);
    stub_.reset(mndReleaseStream);
    stub_.reset(mndAcquireStream);
    stub_.reset(mndAcquireStreamById);
    gRecoveryStreams[0] = nullptr;
    gRecoveryStreams[1] = nullptr;

    taosHashCleanup(mStreamMgmt.watch.deferredStreams);
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
    savedWatch_ = mStreamMgmt.watch;
    savedErrorEvent_ = mStreamMgmt.lastTs[STM_EVENT_STM_TERR];
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
    mStreamMgmt.watch = {};
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
    taosHashSetFreeFp(deployStm_, tFreeSStmStreamDeploy);

    actionQueue_.head = static_cast<SStmQNode *>(taosMemoryCalloc(1, sizeof(SStmQNode)));
    ASSERT_NE(actionQueue_.head, nullptr);
    actionQueue_.tail = actionQueue_.head;
    actionQueue_.queuedCreates =
        taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    ASSERT_NE(nullptr, actionQueue_.queuedCreates);
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
    mStreamMgmt.lastTs[STM_EVENT_STM_TERR] = savedErrorEvent_;
    mStreamMgmt.watch = savedWatch_;
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
    taosHashCleanup(actionQueue_.queuedCreates);
    taosMemoryFreeClear(actionQueue_.head);
    actionQueue_.tail = nullptr;
  }

  void CleanupStatuses() {
    const int64_t ids[] = {kStreamA, kStreamB};
    for (int64_t streamId : ids) {
      auto *pStatus = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId)));
      if (pStatus == nullptr) continue;
      if (pStatus->deployInput && pStatus->pCreate == Stream(streamId)->pCreate) {
        // AddStatus borrows the fixture's SDB object.
        pStatus->deployInput->name = nullptr;
        pStatus->deployInput->create = nullptr;
        mstReleaseDeployInput(pStatus->deployInput);
        pStatus->deployInput = nullptr;
      }
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
  SStmWatchCtx             savedWatch_ = {};
  SStmLastTs               savedErrorEvent_ = {};
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

static thread_local int64_t heartbeatNowNs = 0;

class MndStreamDeployOwnershipTest : public MndStreamRecalcRecoveryTest {
 protected:
  void UseHeartbeatClock() {
    heartbeatNowNs = 0;
    stub_.set(
        taosClockGetTime, +[](int id, struct timespec *ts) -> int32_t {
          if (id != CLOCK_REALTIME) return returnRecalcClock(id, ts);
          // Distinct incarnations can be created within a single synthetic millisecond.
          const int64_t ns = ++heartbeatNowNs;
          ts->tv_sec = static_cast<time_t>(ns / NANOSECOND_PER_SEC);
          ts->tv_nsec = static_cast<long>(ns % NANOSECOND_PER_SEC);
          return 0;
        });
  }

  void CheckQueuedCreate(bool fail, bool scanFirst);
  void SetMetadataGroups(bool recovering);
  void CheckWatchDeployedScans(bool cache, bool empty, bool missingReader);
#ifdef BUILD_TEST_SEAM
  void CheckRunnerCleanupReplacement(bool rebuild);
#endif
#ifdef TD_ENTERPRISE
  void CheckSchemaWorkerStop(bool stopMnode);
#endif
  void TearDown() override {
    stub_.reset(taosHashPut);
    stub_.reset(tCloneStreamWindowPlan);
    for (int64_t id : {kStreamA, kStreamB}) {
      auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &id, sizeof(id)));
      if (status != nullptr && status->pCreate != Stream(id)->pCreate) {
        mstDestroySStmStatus(status);
      }
    }
    MndStreamRecalcRecoveryTest::TearDown();
  }

  void HandleActions() {
    atomic_store_8(&actionQueue_.draining, 1);
    msmDrainStreamActions(&mnode_);
  }

  void QueueDeploy(int64_t id) {
    ASSERT_EQ(mstPostStreamAction(&actionQueue_, id, Stream(kStreamA)->name, nullptr, false, STREAM_ACT_DEPLOY),
              TSDB_CODE_SUCCESS);
  }

  void ReadHeartbeatDeploys(SStreamHbMsg &hb, int32_t expectedCode,
                            const std::function<void(const SStmStreamDeploy &)> &visit, int64_t now = 101,
                            const std::function<void(const SMStreamHbRspMsg &)> &visitReply = {},
                            bool deliverToReceiver = false, int32_t encodeError = 0) {
    SStmThreadCtx threadCtx = {};
    threadCtx.actionStm[hb.streamGId] = actionStm_;
    threadCtx.deployStm[hb.streamGId] = deployStm_;
    auto *savedCtx = mStreamMgmt.tCtx;
    auto  savedThreads = mStreamMgmt.threadNum;
    mStreamMgmt.tCtx = &threadCtx;
    mStreamMgmt.threadNum = 1;
    SRpcMsg request = {};
    SRpcMsg response = {};
    // Some replay tests revisit an earlier millisecond. Keep the sub-millisecond sequence for distinct task IDs.
    heartbeatNowNs = now * NANOSECOND_PER_MSEC + heartbeatNowNs % NANOSECOND_PER_MSEC;
    int32_t code = msmHandleStreamHbMsg(&mnode_, now, &hb, &request, &response);
    mStreamMgmt.tCtx = savedCtx;
    mStreamMgmt.threadNum = savedThreads;
    EXPECT_EQ(expectedCode, code);
    EXPECT_EQ(encodeError ? encodeError : expectedCode, response.code);

    if (!expectedCode && !encodeError) EXPECT_NE(nullptr, response.pCont);
    if (expectedCode || encodeError) EXPECT_EQ(nullptr, response.pCont);
    if (response.pCont) {
      SMStreamHbRspMsg decoded = {};
      SDecoder         decoder;
      tDecoderInit(&decoder, static_cast<uint8_t *>(response.pCont) + sizeof(SStreamMsgGrpHeader),
                   response.contLen - sizeof(SStreamMsgGrpHeader));
      EXPECT_EQ(0, tDecodeStreamHbRsp(&decoder, &decoded));
      if (visitReply) visitReply(decoded);
      for (size_t i = 0; i < taosArrayGetSize(decoded.deploy.streamList); ++i) {
        auto *stream = static_cast<SStmStreamDeploy *>(taosArrayGet(decoded.deploy.streamList, i));
        visit(*stream);
      }
      if (deliverToReceiver) {
        EXPECT_EQ(0, streamHbProcessRspMsg(&decoded));
      } else {
        tDeepFreeSMStreamHbRspMsg(&decoded);
      }
      tDecoderClear(&decoder);
      rpcFreeCont(response.pCont);
    }
  }

  std::vector<int64_t> HeartbeatTriggerIds(int32_t expectedCode, std::vector<int64_t> *runnerIds = nullptr) {
    std::vector<int64_t> tasks;
    SStreamHbMsg         hb = {};
    hb.dnodeId = hb.snodeId = 1;
    ReadHeartbeatDeploys(hb, expectedCode, [&](const SStmStreamDeploy &stream) {
      if (stream.triggerTask) tasks.push_back(stream.triggerTask->task.taskId);
      if (runnerIds) {
        for (size_t j = 0; j < taosArrayGetSize(stream.runnerTasks); ++j) {
          auto *runner = static_cast<SStmTaskDeploy *>(taosArrayGet(stream.runnerTasks, j));
          runnerIds->push_back(runner->task.taskId);
        }
      }
    });
    return tasks;
  }

  void SetRunnerPlan(bool nested, int64_t streamId = kStreamA) {
    auto endpoint = +[](SMnode *, int32_t id) -> SEpSet {
      SEpSet ep = {};
      ep.numOfEps = 1;
      ep.eps[0].port = 6000 + id;
      tstrncpy(ep.eps[0].fqdn, id == 2 ? "node2" : "node1", sizeof(ep.eps[0].fqdn));
      return ep;
    };
    stub_.set(mndGetDnodeEpsetById, endpoint);
    if (nested) {
      stub_.set(mndGetVgroupEpsetById, endpoint);
      // Keep the fixture's endpoint and catalog fault controls independent.
      // The real combined lookup is covered by VgroupEndpointLookupPreservesStatusAndOrder.
      stub_.set(mndGetVgroupEpsetByIdWithStatus,
                +[](SMnode *mnode, int32_t vg, int32_t *lookupCode) -> SEpSet {
        SEpSet ep = mndGetVgroupEpsetById(mnode, vg);
        *lookupCode = 0;
        if (ep.numOfEps == 0) {
          SVgObj *row = mndAcquireVgroup(mnode, vg);
          *lookupCode = row ? 0 : terrno;
          if (row) mndReleaseVgroup(mnode, row);
        }
        return ep;
      });
      auto *create = Stream(streamId)->pCreate;
      create->calcScanPlanList = taosArrayInit(1, sizeof(SStreamCalcScan));
      ASSERT_NE(nullptr, create->calcScanPlanList);
      auto *scan = static_cast<SStreamCalcScan *>(taosArrayReserve(create->calcScanPlanList, 1));
      ASSERT_NE(nullptr, scan);
      const int32_t vg = 20;
      scan->vgList = taosArrayInit(1, sizeof(vg));
      ASSERT_NE(nullptr, scan->vgList);
      ASSERT_NE(nullptr, taosArrayPush(scan->vgList, &vg));
      SNode *raw = nullptr;
      ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &raw));
      std::unique_ptr<SNode, decltype(&nodesDestroyNode)> owner(raw, nodesDestroyNode);
      auto                                               *reader = reinterpret_cast<SSubplan *>(raw);
      reader->subplanType = SUBPLAN_TYPE_SCAN;
      reader->id.groupId = 3;
      reader->id.subplanId = 13;
      ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, reinterpret_cast<SNode **>(&reader->pNode)));
      reinterpret_cast<STableScanPhysiNode *>(reader->pNode)->scan.uid = 42;
      ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_REMOTE_VALUE, &reader->pNode->pConditions));
      ASSERT_EQ(0, nodesNodeToString(raw, false, reinterpret_cast<char **>(&scan->scanPlan), nullptr));
      ASSERT_NE(nullptr, scan->scanPlan);
    }
    SNode *root = nullptr;
    for (int i = 0; i <= (nested ? 1 : 0); ++i) {
      SNode *dag = nullptr, *level = nullptr, *sub = nullptr;
      ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, &dag));
      ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_NODE_LIST, &level));
      ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &sub));
      if (nested) {
        auto *plan = reinterpret_cast<SSubplan *>(sub);
        plan->id.groupId = i + 1;
        plan->id.subplanId = i + 11;
        if (i == 0) {
          ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, reinterpret_cast<SNode **>(&plan->pNode)));
          auto *exchange = reinterpret_cast<SExchangePhysiNode *>(plan->pNode);
          exchange->srcStartGroupId = exchange->srcEndGroupId = 3;
          ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_REMOTE_VALUE, &plan->pNode->pConditions));
          SNode *ref = nullptr;
          ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_VALUE, &ref));
          auto *value = reinterpret_cast<SValueNode *>(ref);
          value->translate = true;
          value->node.resType.type = TSDB_DATA_TYPE_BIGINT;
          value->datum.i = (INT64_C(3) << 32) | 13;
          ASSERT_EQ(0, nodesListMakeStrictAppend(&plan->pChildren, ref));
        } else {
          ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PROJECT, reinterpret_cast<SNode **>(&plan->pNode)));
        }
      }
      reinterpret_cast<SQueryPlan *>(dag)->numOfSubplans = 1;
      ASSERT_EQ(0, nodesListMakeStrictAppend(&reinterpret_cast<SNodeListNode *>(level)->pNodeList, sub));
      ASSERT_EQ(0, nodesListMakeStrictAppend(&reinterpret_cast<SQueryPlan *>(dag)->pSubplans, level));
      if (!root)
        root = dag;
      else
        ASSERT_EQ(0, nodesListMakeStrictAppend(&reinterpret_cast<SQueryPlan *>(root)->pChildren, dag));
    }
    char *plan = nullptr;
    ASSERT_EQ(0, nodesNodeToString(root, false, &plan, nullptr));
    if (plan == nullptr) nodesDestroyNode(root);
    ASSERT_NE(nullptr, plan);
    nodesDestroyNode(root);
    Stream(streamId)->pCreate->calcPlan = plan;
    Stream(streamId)->pCreate->numOfCalcSubplan = 1;
  }


  void SetMetadataCatalog(std::vector<int32_t> ids, int32_t tsmaId = 0) {
    static std::vector<SVgObj> rows;
    rows.clear();
    for (int32_t id : ids) {
      SVgObj row = {};
      row.vgId = id;
      rows.push_back(row);
    }
    if (tsmaId != 0) {
      SVgObj row = {};
      row.vgId = tsmaId;
      row.isTsma = true;
      rows.push_back(row);
    }
    stub_.set(sdbFetch, +[](SSdb *, ESdbType type, void *iter, void **row) -> void * {
      size_t index = reinterpret_cast<uintptr_t>(iter);
      if (type != SDB_VGROUP || index >= rows.size()) return nullptr;
      *row = &rows[index];
      return reinterpret_cast<void *>(index + 1);
    });
    stub_.set(sdbRelease, +[](SSdb *, void *) {});
    stub_.set(sdbCancelFetch, +[](SSdb *, void *) {});
    stub_.set(sdbTraverse, +[](SSdb *, ESdbType type, sdbTraverseFp callback, void *p1, void *p2, void *p3) {
      if (type != SDB_VGROUP) return;
      for (auto &row : rows) {
        if (!callback(nullptr, &row, p1, p2, p3)) break;
      }
    });
  }

  void SetLegacyMetadataPlan(int partitions = 3, bool oneScan = false, bool liveFirst = false,
                             bool metadataInSubquery = false) {
    ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(true));
    auto *create = Stream(kStreamA)->pCreate;
    auto *first = static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, 0));
    if (!metadataInSubquery) {
      taosMemoryFree(first->scanPlan);
      taosArrayDestroy(first->vgList);
      taosArrayClear(create->calcScanPlanList);
    }
    SNode *raw = nullptr;
    ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(create->calcPlan), &raw));
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> owner(raw, nodesDestroyNode);
    auto *dag = reinterpret_cast<SQueryPlan *>(raw);
    if (!metadataInSubquery) {
      nodesDestroyList(dag->pChildren);
      dag->pChildren = nullptr;
    }
    auto *level = reinterpret_cast<SNodeListNode *>(nodesListGetNode(dag->pSubplans, 0));
    auto *runner = reinterpret_cast<SSubplan *>(nodesListGetNode(level->pNodeList, 0));
    if (!metadataInSubquery) {
      nodesDestroyList(runner->pChildren);
      runner->pChildren = nullptr;
    } else {
      auto *ordinary = static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, 0));
      SNode *ordinaryNode = nullptr;
      ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(ordinary->scanPlan), &ordinaryNode));
      std::unique_ptr<SNode, decltype(&nodesDestroyNode)> ordinaryOwner(ordinaryNode, nodesDestroyNode);
      auto *ordinaryPlan = reinterpret_cast<SSubplan *>(ordinaryNode);
      ordinaryPlan->id.groupId = 4;
      ordinaryPlan->id.subplanId = 14;
      void *encodedOrdinary = nullptr;
      ASSERT_EQ(0, nodesNodeToString(ordinaryNode, false, reinterpret_cast<char **>(&encodedOrdinary), nullptr));
      ASSERT_NE(nullptr, encodedOrdinary);
      taosMemoryFree(ordinary->scanPlan);
      ordinary->scanPlan = encodedOrdinary;
      auto *exchange = reinterpret_cast<SExchangePhysiNode *>(runner->pNode);
      exchange->srcStartGroupId = exchange->srcEndGroupId = 4;
      auto *ordinaryRef = reinterpret_cast<SValueNode *>(nodesListGetNode(runner->pChildren, 0));
      ordinaryRef->datum.i = (INT64_C(4) << 32) | 14;
      auto *child = reinterpret_cast<SQueryPlan *>(nodesListGetNode(dag->pChildren, 0));
      auto *childLevel = reinterpret_cast<SNodeListNode *>(nodesListGetNode(child->pSubplans, 0));
      auto *childRunner = reinterpret_cast<SSubplan *>(nodesListGetNode(childLevel->pNodeList, 0));
      SNode *newExchange = nullptr;
      ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, &newExchange));
      nodesDestroyNode(reinterpret_cast<SNode *>(childRunner->pNode));
      childRunner->pNode = reinterpret_cast<SPhysiNode *>(newExchange);
      auto *childExchange = reinterpret_cast<SExchangePhysiNode *>(childRunner->pNode);
      childExchange->srcStartGroupId = childExchange->srcEndGroupId = 3;
    }
    for (int i = 0; i < partitions; ++i) {
      auto *scan = static_cast<SStreamCalcScan *>(taosArrayReserve(create->calcScanPlanList, 1));
      ASSERT_NE(nullptr, scan);
      const int32_t vg = i == 1 ? 20 : 21 + i;
      scan->vgList = taosArrayInit(1, sizeof(vg));
      ASSERT_NE(nullptr, scan->vgList);
      ASSERT_NE(nullptr, taosArrayPush(scan->vgList, &vg));
      SNode *sub = nullptr;
      ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &sub));
      std::unique_ptr<SNode, decltype(&nodesDestroyNode)> subOwner(sub, nodesDestroyNode);
      auto *plan = reinterpret_cast<SSubplan *>(sub);
      plan->subplanType = SUBPLAN_TYPE_SCAN;
      plan->id.groupId = 3;
      plan->id.subplanId = 13 + i;
      ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN,
                                reinterpret_cast<SNode **>(&plan->pNode)));
      auto *meta = reinterpret_cast<SSystemTableScanPhysiNode *>(plan->pNode);
      meta->scan.virtualStableScan = true;
      tstrncpy(meta->scan.tableName.dbname, TSDB_INFORMATION_SCHEMA_DB, sizeof(meta->scan.tableName.dbname));
      tstrncpy(meta->scan.tableName.tname, TSDB_INS_TABLE_VC_COLS, sizeof(meta->scan.tableName.tname));
      ASSERT_EQ(0, nodesNodeToString(sub, false, reinterpret_cast<char **>(&scan->scanPlan), nullptr));
      ASSERT_NE(nullptr, scan->scanPlan);
      SNode *ref = nullptr;
      ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_VALUE, &ref));
      auto *value = reinterpret_cast<SValueNode *>(ref);
      value->translate = true;
      value->node.resType.type = TSDB_DATA_TYPE_BIGINT;
      value->datum.i = (INT64_C(3) << 32) | (13 + i);
      auto *target = runner;
      if (metadataInSubquery) {
        auto *child = reinterpret_cast<SQueryPlan *>(nodesListGetNode(dag->pChildren, 0));
        auto *childLevel = reinterpret_cast<SNodeListNode *>(nodesListGetNode(child->pSubplans, 0));
        target = reinterpret_cast<SSubplan *>(nodesListGetNode(childLevel->pNodeList, 0));
      }
      ASSERT_EQ(0, nodesListMakeStrictAppend(&target->pChildren, ref));
    }
    char *encodedCalcPlan = nullptr;
    ASSERT_EQ(0, nodesNodeToString(raw, false, &encodedCalcPlan, nullptr));
    ASSERT_NE(nullptr, encodedCalcPlan);
    taosMemoryFree(create->calcPlan);
    create->calcPlan = encodedCalcPlan;
    if (oneScan) {
      auto *scan = static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, 0));
      int32_t live = 20;
      ASSERT_NE(nullptr, taosArrayPush(scan->vgList, &live));
      if (liveFirst) {
        std::swap(*static_cast<int32_t *>(taosArrayGet(scan->vgList, 0)),
                  *static_cast<int32_t *>(taosArrayGet(scan->vgList, 1)));
      }
    }
    stub_.set(mndAcquireVgroup, +[](SMnode *, int32_t vg) -> SVgObj * {
      static SVgObj existing = {};
      if (vg == 20) return &existing;
      terrno = TSDB_CODE_MND_VGROUP_NOT_EXIST;
      return nullptr;
    });
    stub_.set(mndGetVgroupEpsetById, +[](SMnode *, int32_t vg) -> SEpSet {
      SEpSet ep = {};
      if (vg == 20) {
        ep.numOfEps = 1;
        ep.eps[0].port = 6020;
        tstrncpy(ep.eps[0].fqdn, "node1", sizeof(ep.eps[0].fqdn));
      }
      return ep;
    });
    stub_.set(mndReleaseVgroup, +[](SMnode *, SVgObj *) {});
    SetMetadataCatalog({20});
  }

#if defined(BUILD_TEST_SEAM) && defined(TD_ENTERPRISE)
  void SetExternalReaders() {
    ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(true));
    auto *stream = Stream(kStreamA);
    auto *create = stream->pCreate;
    auto *scan = static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, 0));
    *static_cast<int32_t *>(taosArrayGet(scan->vgList, 0)) = SNODE_HANDLE;
    stream->flags |= STREAM_FLAG_REF_EXT_SOURCE;
    stream->mainSnodeId = 1;
    create->triggerTblType = TSDB_NORMAL_TABLE;
    create->triggerTblName = taosStrdup("meters");
    create->extSpecs = taosArrayInit_s(sizeof(SStreamExtTriggerSpec *), 1);
    ASSERT_NE(nullptr, create->extSpecs);
    auto *spec = static_cast<SStreamExtTriggerSpec *>(taosMemoryCalloc(1, sizeof(SStreamExtTriggerSpec)));
    ASSERT_NE(nullptr, spec);
    *static_cast<SStreamExtTriggerSpec **>(taosArrayGet(create->extSpecs, 0)) = spec;
    create->numOfExtSpecs = 1;
    tstrncpy(spec->sourceName, "1.test_pg", sizeof(spec->sourceName));
    tstrncpy(spec->extTable, "meters", sizeof(spec->extTable));
    spec->sourceType = EXT_SOURCE_POSTGRESQL;
    spec->numCalcMappings = 128;
    spec->pCalcMappings = static_cast<SExtColTypeMapping *>(taosMemoryCalloc(128, sizeof(SExtColTypeMapping)));
    ASSERT_NE(nullptr, spec->pCalcMappings);
    for (int i = 0; i < spec->numCalcMappings; ++i) {
      snprintf(spec->pCalcMappings[i].colName, sizeof(spec->pCalcMappings[i].colName), "value_%d", i);
    }
    stub_.set(
        mndAcquireExtSource, +[](SMnode *, const char *name) -> SExtSourceObj * {
          static SExtSourceObj source = {};
          source.encryptedPassword[0] = 1;
          tstrncpy(source.options, "first", sizeof(source.options));
          return strcmp(name, "1.test_pg") == 0 ? &source : nullptr;
        });
    stub_.set(mndReleaseExtSource, +[](SMnode *, SExtSourceObj *){});
    stub_.set(sdbReadLock, +[](SSdb *, int32_t){});
    stub_.set(sdbUnLock, +[](SSdb *, int32_t){});
    stub_.set(sdbFetch, +[](SSdb *, ESdbType, void *, void **) -> void * { return nullptr; });
  }
#endif

  void PrependWatchNonVgroupScans() {
    auto *create = Stream(kStreamA)->pCreate;
    if (!create->calcScanPlanList) create->calcScanPlanList = taosArrayInit(4, sizeof(SStreamCalcScan));
    ASSERT_NE(nullptr, create->calcScanPlanList);
    for (int mode = 0; mode < 4; ++mode) {
      SStreamCalcScan empty = {};
      auto *scan = static_cast<SStreamCalcScan *>(taosArrayInsert(create->calcScanPlanList, mode, &empty));
      ASSERT_NE(nullptr, scan);
      scan->scanPlan = taosStrdup("must not decode non-vgroup scan");
      ASSERT_NE(nullptr, scan->scanPlan);
      scan->readFromCache = mode == 0;
      if (mode != 1) {
        scan->vgList = taosArrayInit(1, sizeof(int32_t));
        ASSERT_NE(nullptr, scan->vgList);
        int32_t id = mode == 0 ? 20 : (mode == 2 ? MNODE_HANDLE : SNODE_HANDLE);
        ASSERT_NE(nullptr, taosArrayPush(scan->vgList, &id));
      }
    }
  }

  int32_t WatchTask() {
    SStmGrpCtx ctx = {};
    ctx.pMnode = &mnode_;
    SStmTaskStatusMsg task = {};
    task.streamId = kStreamA;
    task.type = STREAM_READER_TASK;
    return msmWatchRecordNewTask(&ctx, &task);
  }

  void FailClone() {
    Stream(kStreamA)->pCreate->pWindowPlan =
        static_cast<SStreamWindowPlan *>(taosMemoryCalloc(1, sizeof(SStreamWindowPlan)));
    ASSERT_NE(Stream(kStreamA)->pCreate->pWindowPlan, nullptr);
    stub_.set(tCloneStreamWindowPlan, failWindowPlanClone);
  }

  static int32_t FailHashPut(SHashObj *, const void *, size_t, const void *, size_t) {
    return terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
};

extern "C" int32_t msmQueueVgroupCalcReaders(SStmGrpCtx *, SStreamObj *, SStmStatus *, SNodeList *);
extern "C" int32_t msmGrpAddDeploySnodeTasks(SStmGrpCtx *, SArray **);
extern "C" void    msmCleanDeployedSnodeTasks(int32_t);

TEST_F(MndStreamDeployOwnershipTest, CalcReadersKeepOriginalPlanOrShareOneBindingAcrossVgroups) {
  SStreamObj *stream = Stream(kStreamA);
  SStmStatus *status = AddStatus(kStreamA);
  ASSERT_NE(status, nullptr);
  status->calcReaders = tdListNew(sizeof(SStmTaskStatus));
  ASSERT_NE(status->calcReaders, nullptr);
  status->pCreate->calcScanPlanList = taosArrayInit(2, sizeof(SStreamCalcScan));
  ASSERT_NE(status->pCreate->calcScanPlanList, nullptr);
  for (int i = 0; i < 2; ++i) {
    SNode *plan = nullptr;
    ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &plan));
    reinterpret_cast<SSubplan *>(plan)->id.groupId = 100 + i;
    SStreamCalcScan scan = {};
    char           *original = nullptr;
    ASSERT_EQ(0, nodesNodeToString(plan, true, &original, nullptr));
    scan.scanPlan = original;
    nodesDestroyNode(plan);
    ASSERT_NE(nullptr, taosArrayPush(status->pCreate->calcScanPlanList, &scan));
    for (int vg : {2, 3}) {
      SStmTaskStatus reader = {};
      reader.type = STREAM_READER_TASK;
      reader.id.nodeId = vg;
      reader.id.taskIdx = i;
      ASSERT_NE(nullptr, tdListAdd(status->calcReaders, &reader));
    }
  }
  static int serializations;
  serializations = 0;
  Stub counter;
  counter.set(
      tjsonToUnformattedString, +[](const SJson *json) -> char * {
        ++serializations;
        return cJSON_PrintUnformatted(reinterpret_cast<const cJSON *>(json));
      });
  ASSERT_EQ(0, msmQueueVgroupCalcReaders(nullptr, stream, status, nullptr));
  EXPECT_EQ(0, serializations);
  SNode *source = nullptr;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_DOWNSTREAM_SOURCE, &source));
  reinterpret_cast<SDownstreamSourceNode *>(source)->taskId = 73;
  SNodeList *endpoints = nullptr;
  ASSERT_EQ(0, nodesListMakeStrictAppend(&endpoints, source));
  int32_t code = msmQueueVgroupCalcReaders(nullptr, stream, status, endpoints);
  nodesDestroyList(endpoints);
  ASSERT_EQ(0, code);
  EXPECT_EQ(2, serializations);

  const char *firstBound[2] = {};
  for (int32_t vg : {2, 3}) {
    auto *queue = static_cast<SStmVgTasksToDeploy *>(taosHashGet(mStreamMgmt.toDeployVgMap, &vg, sizeof(vg)));
    ASSERT_NE(nullptr, queue);
    ASSERT_EQ(4u, taosArrayGetSize(queue->taskList));
    for (int i = 0; i < 2; ++i) {
      auto *scan = static_cast<SStreamCalcScan *>(taosArrayGet(status->pCreate->calcScanPlanList, i));
      auto *plain = static_cast<SStmTaskToDeployExt *>(taosArrayGet(queue->taskList, i));
      EXPECT_STREQ(static_cast<const char *>(scan->scanPlan),
                   static_cast<const char *>(plain->deploy.msg.reader.msg.calc.calcScanPlan));
      auto *patched = static_cast<SStmTaskToDeployExt *>(taosArrayGet(queue->taskList, 2 + i));
      auto *bound = static_cast<const char *>(patched->deploy.msg.reader.msg.calc.calcScanPlan);
      if (firstBound[i]) {
        EXPECT_NE(firstBound[i], bound);
        EXPECT_STREQ(firstBound[i], bound);
      }
      firstBound[i] = bound;
      SNode *decoded = nullptr;
      ASSERT_EQ(0, nodesStringToNode(bound, &decoded));
      auto *subplan = reinterpret_cast<SSubplan *>(decoded);
      EXPECT_EQ(100 + i, subplan->id.groupId);
      ASSERT_EQ(1, LIST_LENGTH(subplan->pSubQ));
      EXPECT_EQ(73, reinterpret_cast<SDownstreamSourceNode *>(nodesListGetNode(subplan->pSubQ, 0))->taskId);
      nodesDestroyNode(decoded);
    }
  }
}

extern "C" void msmCheckStreamsStatus(SMnode *);

void MndStreamDeployOwnershipTest::CheckQueuedCreate(bool fail, bool scanFirst) {
  // A occupies the worker while CREATE and the catalog scan both discover B.
  // A successful B must keep its original Trigger; a failed B must retain its first backoff.
  static bool failB;
  static int  buildsB;
  stub_.set(
      msmAssignTaskSnodeId, +[](SMnode *, SStreamObj *stream, bool) -> int32_t {
        if (stream->pCreate->streamId == gRecoveryStreams[1]->pCreate->streamId) {
          ++buildsB;
          if (failB) {
            terrno = TSDB_CODE_SNODE_NO_AVAILABLE_NODE;
            return 0;
          }
        }
        return 1;
      });
  stub_.set(
      sdbFetch, +[](SSdb *, ESdbType type, void *iter, void **row) -> void * {
        if (!iter && type == SDB_STREAM) return *row = gRecoveryStreams[1];
        return nullptr;
      });
  stub_.set(sdbRelease, +[](SSdb *, void *) {});
  stub_.set(sdbCheckExists, reportStreamExists);
  auto       savedHealth = mStreamMgmt.hCtx;
  SStmLastTs savedTimes[STM_EVENT_MAX_VALUE];
  memcpy(savedTimes, mStreamMgmt.lastTs, sizeof(savedTimes));
  failB = fail;
  buildsB = 0;
  QueueDeploy(kStreamA);
  PrivateBuildPause pause;
  std::thread       worker([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  if (!scanFirst) EXPECT_EQ(0, mstPostStreamCreate(kStreamB, Stream(kStreamB)->name));
  for (int i = 0; i < 4; ++i) {
    memset(mStreamMgmt.lastTs, 0, sizeof(savedTimes));
    mStreamMgmt.hCtx.currentTs = taosGetTimestampMs();
    msmCheckStreamsStatus(&mnode_);
    EXPECT_EQ(1u, actionQueue_.qRemainNum);
  }
  if (scanFirst) EXPECT_EQ(0, mstPostStreamCreate(kStreamB, Stream(kStreamB)->name));
  EXPECT_EQ(1u, actionQueue_.qRemainNum);
  EXPECT_EQ(1, taosHashGetSize(actionQueue_.queuedCreates));
  pause.Release();
  worker.join();
  HandleActions();  // Drain any actions left queued by a failed deployment.
  EXPECT_EQ(1, buildsB);
  EXPECT_EQ(0, taosHashGetSize(actionQueue_.queuedCreates));
  auto *b = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_NE(nullptr, b);
  EXPECT_EQ(0, b->deployTimes);
  EXPECT_EQ(fail ? 1 : 0, b->stopped);
  if (fail) {
    EXPECT_GT(b->fatalRetryTs, mStreamMgmt.hCtx.currentTs);
  } else {
    EXPECT_NE(nullptr, b->triggerTask);
  }
  memcpy(mStreamMgmt.lastTs, savedTimes, sizeof(savedTimes));
  mStreamMgmt.hCtx = savedHealth;
}

TEST_F(MndStreamDeployOwnershipTest, QueuedCreateAndHealthScanPublishOnce) { CheckQueuedCreate(false, false); }

TEST_F(MndStreamDeployOwnershipTest, QueuedCreateAndHealthScanKeepFirstBackoff) { CheckQueuedCreate(true, false); }

TEST_F(MndStreamDeployOwnershipTest, CatalogScanBeforeCreateCommitKeepsPublishedGeneration) {
  CheckQueuedCreate(false, true);
}

TEST_F(MndStreamDeployOwnershipTest, CatalogScanBeforeCreateCommitKeepsBackoff) { CheckQueuedCreate(true, true); }

TEST_F(MndStreamDeployOwnershipTest, FailedFirstDeploymentEnqueueCanBeRetried) {
  stub_.set(taosHashPut, FailHashPut);
  EXPECT_NE(0, mstPostStreamCreate(kStreamB, Stream(kStreamB)->name));
  stub_.reset(taosHashPut);
  EXPECT_EQ(0u, actionQueue_.qRemainNum);
  EXPECT_EQ(0, taosHashGetSize(actionQueue_.queuedCreates));
  EXPECT_EQ(0, mstPostStreamCreate(kStreamB, Stream(kStreamB)->name));
  EXPECT_EQ(1u, actionQueue_.qRemainNum);
}

TEST_F(MndStreamDeployOwnershipTest, ExternalPlacementScansCatalogOnceOutsideQueryLocks) {
  ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(false));
  Stream(kStreamA)->flags |= STREAM_FLAG_REF_EXT_SOURCE;
  ASSERT_NE(nullptr, AddStatus(kStreamB));

  static SStreamObj catalog[128] = {};
  static SSnodeObj  nodes[2] = {};
  static int        rowsRead;
  rowsRead = 0;
  nodes[0].id = 1;
  nodes[1].id = 2;
  for (auto &stream : catalog) {
    stream.flags = STREAM_FLAG_REF_EXT_SOURCE;
    stream.mainSnodeId = 1;
  }
  SStmSnodeStatus spare = {};
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.snodeMap, &nodes[1].id, sizeof(nodes[1].id), &spare, sizeof(spare)));
  stub_.set(
      sdbFetch, +[](SSdb *, ESdbType type, void *last, void **row) -> void * {
        if (type == SDB_STREAM) {
          auto *next = last ? static_cast<SStreamObj *>(last) + 1 : catalog;
          if (next == catalog + 128) return nullptr;
          if (next == catalog + 64) PrivateBuildPause::Pause();
          ++rowsRead;
          return *row = next;
        }
        if (type == SDB_SNODE) {
          auto *next = last ? static_cast<SSnodeObj *>(last) + 1 : nodes;
          if (next != nodes + 2) return *row = next;
        }
        return nullptr;
      });
  stub_.set(sdbRelease, +[](SSdb *, void *) {});
  auto *block = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(nullptr, block);
  SnodeLookupStubGuard snode;
  PrivateBuildPause    pause(false);
  QueueDeploy(kStreamA);
  std::thread builder([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, Stream(kStreamB), block, 0, 0));
  pause.Release();
  builder.join();
  blockDataDestroy(block);

  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  EXPECT_EQ(2, status->triggerTask->id.nodeId);
  ASSERT_GT(status->runnerDeploys, 0);
  for (int i = 0; i < status->runnerDeploys; ++i) {
    auto *runner = static_cast<SStmTaskStatus *>(taosArrayGet(status->runners[i], 0));
    ASSERT_NE(nullptr, runner);
    EXPECT_EQ(2, runner->id.nodeId);
  }
  EXPECT_EQ(128, rowsRead);
}

TEST_F(MndStreamDeployOwnershipTest, UnrelatedVgroupChangeKeepsSelectedPlacement) {
  Stream(kStreamA)->pCreate->triggerTblType = TSDB_NORMAL_TABLE;
  Stream(kStreamA)->pCreate->triggerTblVgId = 20;
  Stub addresses;
  addresses.set(
      mndGetVgroupEpsetById, +[](SMnode *, int32_t id) -> SEpSet {
        EXPECT_EQ(20, id);
        SEpSet ep = {};
        ep.numOfEps = 1;
        ep.eps[0].port = 6030;
        tstrncpy(ep.eps[0].fqdn, "node1", sizeof(ep.eps[0].fqdn));
        return ep;
      });
  static std::atomic<int64_t> vgVersion{0};
  vgVersion.store(0);
  stub_.set(
      sdbGetTableVer, +[](SSdb *, ESdbType type) -> int64_t { return type == SDB_VGROUP ? vgVersion.load() : 0; });
  QueueDeploy(kStreamA);
  HandleActions();
  PrivateBuildPause pause;
  QueueDeploy(kStreamA);
  std::thread builder([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  vgVersion.fetch_add(1);
  pause.Release();
  builder.join();

  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(0, status->stopped);
  EXPECT_EQ(1, status->deployTimes);
  ASSERT_NE(nullptr, status->triggerTask);
  EXPECT_EQ(2, taosHashGetSize(mStreamMgmt.taskMap));
}

TEST_F(MndStreamDeployOwnershipTest, SelectedVgroupEndpointDisappearsBeforePublication) {
  Stream(kStreamA)->pCreate->triggerTblType = TSDB_NORMAL_TABLE;
  Stream(kStreamA)->pCreate->triggerTblVgId = 20;
  static int versions, endpoints;
  versions = endpoints = 0;
  stub_.set(sdbGetTableVer, +[](SSdb *, ESdbType type) -> int64_t { return type == SDB_VGROUP ? ++versions : 0; });
  Stub addresses;
  addresses.set(
      mndGetVgroupEpsetById, +[](SMnode *, int32_t id) -> SEpSet {
        EXPECT_EQ(20, id);
        SEpSet ep = {};
        if (++endpoints == 1) {
          ep.numOfEps = 1;
          ep.eps[0].port = 6030;
          tstrncpy(ep.eps[0].fqdn, "node1", sizeof(ep.eps[0].fqdn));
        }
        return ep;
      });
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(TSDB_CODE_MND_STREAM_NOT_AVAILABLE, status->fatalError);
  EXPECT_EQ(2, endpoints);
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(0, mStreamMgmt.toDeployVgTaskNum);
}

TEST_F(MndStreamDeployOwnershipTest, LostSelectedSnodeRejectsDraft) {
  PrivateBuildPause pause;
  QueueDeploy(kStreamA);
  std::thread builder([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  const int32_t snode = 1;
  taosWLockLatch(&mStreamMgmt.runtimeLock);
  EXPECT_EQ(0, taosHashRemove(mStreamMgmt.snodeMap, &snode, sizeof(snode)));
  taosWUnLockLatch(&mStreamMgmt.runtimeLock);
  pause.Release();
  builder.join();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(TSDB_CODE_SNODE_NO_AVAILABLE_NODE, status->fatalError);
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
}

TEST_F(MndStreamDeployOwnershipTest, RedeployAndStatusReadFinishWhileUnrelatedQueueIsLocked) {
  QueueDeploy(kStreamA);
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
  HandleActions();
  auto *published = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, published);
  ASSERT_NE(nullptr, published->triggerTask);
  int64_t oldKey[2] = {kStreamA, published->triggerTask->id.taskId};

  constexpr int32_t lockedNode = 128;
  for (int32_t node = 2; node <= lockedNode; ++node) {
    SStmVgTasksToDeploy queue = {};
    queue.taskList = taosArrayInit(1, sizeof(SStmTaskToDeployExt));
    ASSERT_NE(nullptr, queue.taskList);
    SStmTaskToDeployExt entry = {};
    entry.deploy.task.type = STREAM_READER_TASK;
    entry.deploy.task.streamId = kStreamB + node;
    entry.deploy.task.taskId = node;
    ASSERT_NE(nullptr, taosArrayPush(queue.taskList, &entry));
    ASSERT_EQ(0, taosHashPut(mStreamMgmt.toDeployVgMap, &node, sizeof(node), &queue, sizeof(queue)));
    int64_t         key[2] = {entry.deploy.task.streamId, entry.deploy.task.taskId};
    SStmTaskStatus *unrelated = nullptr;
    ASSERT_EQ(0, taosHashPut(mStreamMgmt.taskMap, key, sizeof(key), &unrelated, sizeof(unrelated)));
    ++mStreamMgmt.toDeployVgTaskNum;
  }
  auto *locked =
      static_cast<SStmVgTasksToDeploy *>(taosHashGet(mStreamMgmt.toDeployVgMap, &lockedNode, sizeof(lockedNode)));
  ASSERT_NE(nullptr, locked);
  auto *block = createSystemTableBlock(TSDB_INS_TABLE_STREAM_TASKS, 1);
  ASSERT_NE(nullptr, block);

  QueueDeploy(kStreamA);
  std::mutex              mutex;
  std::condition_variable cv;
  bool                    done = false;
  taosWLockLatch(&locked->lock);
  std::thread worker([&] {
    HandleActions();
    int32_t rows = 0;
    EXPECT_EQ(0, mstSetStreamTasksResBlock(Stream(kStreamB), block, &rows, 1));
    EXPECT_EQ(1, rows);
    std::lock_guard<std::mutex> guard(mutex);
    done = true;
    cv.notify_one();
  });
  {
    std::unique_lock<std::mutex> guard(mutex);
    EXPECT_TRUE(cv.wait_for(guard, std::chrono::seconds(5), [&] { return done; }));
  }
  taosWUnLockLatch(&locked->lock);
  worker.join();
  blockDataDestroy(block);
  EXPECT_EQ(nullptr, taosHashGet(mStreamMgmt.taskMap, oldKey, sizeof(oldKey)));
  EXPECT_EQ(lockedNode + 1, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(0, locked->deployed);
}

TEST_F(MndStreamDeployOwnershipTest, NoAvailableNodeFailsBeforeGlobalTaskRollback) {
  static int taskMapScans;
  taskMapScans = 0;
  stub_.set(
      msmAssignTaskSnodeId, +[](SMnode *, SStreamObj *, bool) -> int32_t {
        terrno = TSDB_CODE_SNODE_NO_AVAILABLE_NODE;
        return 0;
      });
  Stub scan;
  scan.set(
      taosHashIterate, +[](SHashObj *map, void *) -> void * {
        if (map == mStreamMgmt.taskMap) ++taskMapScans;
        return nullptr;
      });
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(TSDB_CODE_SNODE_NO_AVAILABLE_NODE, status->fatalError);
  EXPECT_EQ(0, taskMapScans);
}

class FailPublishedTriggerPush {
 public:
  FailPublishedTriggerPush() {
    hook_ = &stub_;
    stub_.set(taosArrayAddBatch, Push);
  }

 private:
  static void *Push(SArray *array, const void *data, int32_t count) {
    const int32_t node = 1;
    auto *queue = static_cast<SStmSnodeTasksDeploy *>(taosHashGet(mStreamMgmt.toDeploySnodeMap, &node, sizeof(node)));
    if (queue && array == queue->triggerList) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return nullptr;
    }
    hook_->reset(taosArrayAddBatch);
    void *result = taosArrayAddBatch(array, data, count);
    hook_->set(taosArrayAddBatch, Push);
    return result;
  }
  Stub         stub_;
  static Stub *hook_;
};
Stub *FailPublishedTriggerPush::hook_;

TEST_F(MndStreamDeployOwnershipTest, FailedFullDeploymentDiscardsQueuedTasksBeforeHeartbeatCanCollectThem) {
  ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(false));
  FailPublishedTriggerPush fail;
  QueueDeploy(kStreamA);
  HandleActions();
  const int32_t snodeId = 1;
  auto         *queue =
      static_cast<SStmSnodeTasksDeploy *>(taosHashGet(mStreamMgmt.toDeploySnodeMap, &snodeId, sizeof(snodeId)));
  ASSERT_NE(nullptr, queue);
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, status->fatalError);
  ASSERT_GT(status->runnerDeploys, 0);
  ASSERT_EQ(status->runnerDeploys, taosArrayGetSize(queue->runnerList));
  for (int i = 0; i < status->runnerDeploys; ++i) {
    auto *runner = static_cast<SStmTaskToDeployExt *>(taosArrayGet(queue->runnerList, i));
    EXPECT_TRUE(runner->deployed);
  }
  EXPECT_EQ(status->runnerDeploys, queue->runnerDeployed);
  EXPECT_EQ(0u, taosArrayGetSize(queue->triggerList));
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  std::vector<int64_t> runners;
  EXPECT_TRUE(HeartbeatTriggerIds(0, &runners).empty());
  EXPECT_TRUE(runners.empty());
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
}

TEST_F(MndStreamDeployOwnershipTest, RollbackThenCleanupStillDeliversAnotherStreamsRunner) {
  const int32_t        node = 1;
  SStmSnodeTasksDeploy queued = {};
  queued.runnerList = taosArrayInit(1, sizeof(SStmTaskToDeployExt));
  queued.triggerList = taosArrayInit(2, sizeof(SStmTaskToDeployExt));
  SStmTaskToDeployExt runner = {};
  runner.deploy.task.type = STREAM_RUNNER_TASK;
  runner.deploy.task.streamId = kStreamA;
  SStmTaskToDeployExt trigger = {};
  trigger.deploy.task.type = STREAM_TRIGGER_TASK;
  trigger.deploy.task.streamId = kStreamB;
  ASSERT_NE(nullptr, taosArrayPush(queued.runnerList, &runner));
  ASSERT_NE(nullptr, taosArrayPush(queued.triggerList, &trigger));
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.toDeploySnodeMap, &node, sizeof(node), &queued, sizeof(queued)));
  mStreamMgmt.toDeploySnodeTaskNum = 2;
  ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(false));
  {
    FailPublishedTriggerPush fail;
    QueueDeploy(kStreamA);
    HandleActions();
  }
  auto *failed = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, failed);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, failed->fatalError);
  msmCleanDeployedSnodeTasks(node);
  auto *queue = static_cast<SStmSnodeTasksDeploy *>(taosHashGet(mStreamMgmt.toDeploySnodeMap, &node, sizeof(node)));
  ASSERT_NE(nullptr, queue);
  ASSERT_EQ(nullptr, queue->runnerList);
  ASSERT_EQ(0, queue->runnerDeployed);
  queue->runnerList = taosArrayInit(1, sizeof(runner));
  runner.deploy.task.streamId = kStreamB;
  runner.deploy.task.taskId = 83;
  ASSERT_NE(nullptr, taosArrayPush(queue->runnerList, &runner));
  ++mStreamMgmt.toDeploySnodeTaskNum;
  SStreamHbMsg req = {};
  req.snodeId = node;
  SStmGrpCtx ctx = {};
  ctx.pReq = &req;
  ctx.deployStm = deployStm_;
  ASSERT_EQ(0, msmGrpAddDeploySnodeTasks(&ctx, nullptr));
  auto *collected = static_cast<SStmStreamDeploy *>(taosHashGet(deployStm_, &kStreamB, sizeof(kStreamB)));
  ASSERT_NE(nullptr, collected);
  ASSERT_EQ(1u, taosArrayGetSize(collected->runnerTasks));
  EXPECT_EQ(83, static_cast<SStmTaskDeploy *>(taosArrayGet(collected->runnerTasks, 0))->task.taskId);
}

TEST_F(MndStreamDeployOwnershipTest, RunnerRepairSurvivesCollectionOfLastNodeTask) {
  ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(false));
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  auto *runner = static_cast<SStmTaskStatus *>(taosArrayGet(status->runners[0], 0));
  ASSERT_NE(nullptr, runner);
  const auto runnerId = runner->id.taskId;

  // Check that producers cannot enter between the cleanup's node lookup and hash removal.
  static Stub *guard;
  guard = &stub_;
  stub_.set(
      msmCleanDeployedSnodeTasks, +[](int32_t node) {
        bool publisherEntered = taosWTryLockLatch(&mStreamMgmt.actionQLock) == 0;
        if (publisherEntered) taosWUnLockLatch(&mStreamMgmt.actionQLock);
        EXPECT_FALSE(publisherEntered);
        guard->reset(msmCleanDeployedSnodeTasks);
        msmCleanDeployedSnodeTasks(node);
      });
  std::vector<int64_t> runners;
  EXPECT_EQ(std::vector<int64_t>{status->triggerTask->id.taskId}, HeartbeatTriggerIds(0, &runners));
  ASSERT_EQ(static_cast<size_t>(status->runnerDeploys), runners.size());
  EXPECT_EQ(runnerId, runners.front());
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.toDeploySnodeMap));

  SStmTaskAction repair = {};
  repair.streamId = kStreamA;
  repair.generation = status->triggerTask->id.taskId;
  repair.type = STREAM_RUNNER_TASK;
  repair.multiRunner = true;
  repair.deployNum = 1;
  mstPostTaskAction(&actionQueue_, &repair, STREAM_ACT_DEPLOY);
  HandleActions();
  runners.clear();
  EXPECT_TRUE(HeartbeatTriggerIds(0, &runners).empty());
  EXPECT_EQ(std::vector<int64_t>{runnerId}, runners);
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
}

TEST_F(MndStreamDeployOwnershipTest, RequestedCalcReadersUseOneScanWhileAnotherStreamCanBeQueried) {
  ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(true));
  QueueDeploy(kStreamA);
  HandleActions();
  auto *owner = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, owner);
  ASSERT_NE(nullptr, owner->triggerTask);
  auto *runner = static_cast<SStmTaskStatus *>(taosArrayGet(owner->runners[0], 0));
  ASSERT_NE(nullptr, runner);
  ASSERT_NE(nullptr, AddStatus(kStreamB));
  SnodeLookupStubGuard snode;
  auto                *block = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(nullptr, block);

  SStreamHbMsg hb = {};
  hb.dnodeId = 1;
  hb.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 1);
  hb.pStreamReq = taosArrayInit_s(sizeof(int32_t), 1);
  auto *task = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, 0));
  ASSERT_NE(nullptr, task);
  task->streamId = kStreamA;
  task->taskId = runner->id.taskId;
  task->nodeId = runner->id.nodeId;
  task->seriousId = runner->id.seriousId;
  task->type = STREAM_RUNNER_TASK;
  task->status = STREAM_STATUS_RUNNING;
  task->pMgmtReq = static_cast<SStreamMgmtReq *>(taosMemoryCalloc(1, sizeof(SStreamMgmtReq)));
  ASSERT_NE(nullptr, task->pMgmtReq);
  task->pMgmtReq->type = STREAM_MGMT_REQ_RUNNER_ORIGTBL_READER;
  const int32_t groups[][3] = {{21, 22, 21}, {22, 21, 22}};
  task->pMgmtReq->cont.pReqs = taosArrayInit_s(sizeof(SStreamOReaderDeployReq), 2);
  for (int i = 0; i < 2; ++i) {
    auto *req = static_cast<SStreamOReaderDeployReq *>(taosArrayGet(task->pMgmtReq->cont.pReqs, i));
    ASSERT_NE(nullptr, req);
    req->uid = 42;
    req->execId = 50 + i;
    req->vgIds = taosArrayInit(3, sizeof(int32_t));
    for (int32_t vg : groups[i]) ASSERT_NE(nullptr, taosArrayPush(req->vgIds, &vg));
  }

  SStreamMgmtReq *retry = nullptr;
  ASSERT_EQ(0, tCloneSStreamMgmtReq(task->pMgmtReq, &retry));
  static std::vector<int32_t> prepared;
  static bool                 retrying;
  retrying = false;
  prepared.clear();
  stub_.set(
      mndGetVgroupEpsetById, +[](SMnode *, int32_t vg) -> SEpSet {
        if (vg == 21 || vg == 22) {
          prepared.push_back(vg);
          if (retrying && prepared.size() == 1) PrivateBuildPause::Pause();
        }
        SEpSet ep = {};
        ep.numOfEps = 1;
        ep.eps[0].port = 6000 + vg;
        return ep;
      });

  PrivateBuildPause       pause(false);
  static std::atomic<int> lookups{0};
  lookups.store(0);
  stub_.set(
      mstGetScanUidFromPlan, +[](int64_t, void *, int64_t *uid) -> int32_t {
        if (lookups.fetch_add(1) == 0) PrivateBuildPause::Pause();
        *uid = 42;
        return 0;
      });
  int64_t     taskIds[2] = {};
  auto        receive = [&] {
    ReadHeartbeatDeploys(
        hb, 0, [](const SStmStreamDeploy &) {}, retrying ? 102 : 101,
        [&](const SMStreamHbRspMsg &decoded) {
          ASSERT_EQ(1u, taosArrayGetSize(decoded.rsps.rspList));
          auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(decoded.rsps.rspList, 0));
          ASSERT_EQ(0, reply->code);
          ASSERT_EQ(2u, taosArrayGetSize(reply->cont.execRspList));
          for (int i = 0; i < 2; ++i) {
            auto *exec = static_cast<SStreamOReaderDeployRsp *>(taosArrayGet(reply->cont.execRspList, i));
            EXPECT_EQ(50 + i, exec->execId);
            ASSERT_EQ(3u, taosArrayGetSize(exec->vgList));
            for (int j = 0; j < 3; ++j) {
              auto   *addr = static_cast<SStreamTaskAddr *>(taosArrayGet(exec->vgList, j));
              int32_t vg = groups[i][j];
              EXPECT_EQ(vg, addr->nodeId);
              EXPECT_EQ(1, addr->epset.numOfEps);
              EXPECT_EQ(6000 + vg, addr->epset.eps[0].port);
              if (!retrying && i == 0 && j < 2) taskIds[j] = addr->taskId;
              EXPECT_EQ(taskIds[vg - 21], addr->taskId);
            }
          }
        });
  };
  std::thread receiving(receive);
  EXPECT_TRUE(pause.Entered());
  EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, Stream(kStreamB), block, 0, 0));
  pause.Release();
  receiving.join();
  EXPECT_EQ(1, lookups.load());
  EXPECT_EQ((std::vector<int32_t>{21, 22}), prepared);
  EXPECT_NE(taskIds[0], taskIds[1]);

  // Retry the same execution slots after both Readers have been published.
  task->pMgmtReq = retry;
  retrying = true;
  prepared.clear();
  PrivateBuildPause firstLookup(false);
  std::thread       replaying(receive);
  EXPECT_TRUE(firstLookup.Entered());
  auto start = std::chrono::steady_clock::now();
  firstLookup.Release();
  EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, Stream(kStreamB), block, 0, 0));
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(5));
  replaying.join();
  EXPECT_EQ(1, lookups.load());
  EXPECT_EQ((std::vector<int32_t>{21, 22}), prepared);
  for (int32_t vg : {21, 22}) {
    auto *queue = static_cast<SStmVgTasksToDeploy *>(taosHashGet(mStreamMgmt.toDeployVgMap, &vg, sizeof(vg)));
    ASSERT_NE(nullptr, queue);
    ASSERT_EQ(1u, taosArrayGetSize(queue->taskList));
    auto *entry = static_cast<SStmTaskToDeployExt *>(taosArrayGet(queue->taskList, 0));
    EXPECT_EQ(taskIds[vg - 21], entry->deploy.task.taskId);
    EXPECT_EQ(kStreamA, entry->deploy.task.streamId);
    EXPECT_EQ(0, entry->deploy.task.taskIdx);
    EXPECT_NE(nullptr, entry->deploy.msg.reader.msg.calc.calcScanPlan);
  }
  blockDataDestroy(block);
  tCleanupStreamHbMsg(&hb, true);
}

#ifdef TD_ENTERPRISE
TEST_F(MndStreamDeployOwnershipTest, WideDynamicExternalCreateAndPatchAllowOtherStreamStatusReads) {
  Stream(kStreamA)->mainSnodeId = 1;
  QueueDeploy(kStreamA);
  HandleActions();
  auto *owner = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, owner);
  ASSERT_NE(nullptr, owner->triggerTask);
  ASSERT_NE(nullptr, AddStatus(kStreamB));
  auto *block = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(nullptr, block);
  SnodeLookupStubGuard snode;
  PauseExternalReaderSchema(stub_);
  stub_.set(mndGetDnodeEpsetById, +[](SMnode *, int32_t) -> SEpSet { return {}; });
  stub_.set(sdbReadLock, +[](SSdb *, int32_t) {});
  stub_.set(sdbUnLock, +[](SSdb *, int32_t) {});
  stub_.set(
      extConnectorGetTableSchema, +[](SExtConnectorHandle *, const SExtTableNode *, SExtTableMeta **out) -> int32_t {
        std::vector<SExtColumnDef> columns(129);
        tstrncpy(columns[0].colName, "ts", sizeof(columns[0].colName));
        tstrncpy(columns[0].extTypeName, "timestamp", sizeof(columns[0].extTypeName));
        columns[0].isPrimaryKey = true;
        for (size_t i = 1; i < columns.size(); ++i) {
          snprintf(columns[i].colName, sizeof(columns[i].colName), "value_%d", (int)i);
          tstrncpy(columns[i].extTypeName, "int", sizeof(columns[i].extTypeName));
        }
        SExtTableMeta meta = {};
        meta.numOfCols = (int32_t)columns.size();
        meta.pCols = columns.data();
        *out = extConnectorCloneTableSchema(&meta);
        return *out ? 0 : terrno;
      });
  stub_.set(
      extFindColumnIndex, +[](EExtSourceType, const SExtTableMeta *meta, const char *name) -> int32_t {
        if (strcmp(name, "value_32") == 0) PrivateBuildPause::Pause();
        for (int i = 0; i < meta->numOfCols; ++i)
          if (strcmp(meta->pCols[i].colName, name) == 0) return i;
        return -1;
      });
  int64_t originalId = 0, requestId = 0;
  auto   *source = mndAcquireExtSource(&mnode_, "remote");
  for (int width : {64, 128, 96}) {
    const bool        alterSourceDuringPatch = width == 96;
    PrivateBuildPause pause(false);
    SStreamHbMsg      hb = {};
    hb.dnodeId = hb.snodeId = 1;
    hb.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 1);
    hb.pStreamReq = taosArrayInit_s(sizeof(int32_t), 1);
    ASSERT_NE(nullptr, hb.pStreamStatus);
    ASSERT_NE(nullptr, hb.pStreamReq);
    auto &task = *static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, 0));
    ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(&task, kStreamA, owner->triggerTask->id));
    task.status = STREAM_STATUS_RUNNING;
    auto *req = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(task.pMgmtReq->cont.pExtOrigTblReaders, 0));
    task.ackedSchemaReqId = requestId ? requestId : -1;
    task.pMgmtReq->reqId = ++requestId;
    req->registryEntryId = 77;
    req->protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
    tstrncpy(req->identity.sourceName, "remote", sizeof(req->identity.sourceName));
    req->capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START;
    req->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_GUARDED_DEPLOY;
    if (originalId) req->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
    req->bindings = taosArrayInit(width, sizeof(SStreamExtOrigTblReaderBinding));
    ASSERT_NE(nullptr, req->bindings);
    for (int i = 1; i <= width; ++i) {
      SStreamExtOrigTblReaderBinding binding = {};
      binding.kind = STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER;
      binding.bindingId = i;
      binding.expectedType = TSDB_DATA_TYPE_INT;
      snprintf(binding.externalColumn, sizeof(binding.externalColumn), "value_%d", i);
      ASSERT_NE(nullptr, taosArrayPush(req->bindings, &binding));
    }
    SStreamMgmtReq *retry = nullptr, *duplicate = nullptr;
    ASSERT_EQ(0, tCloneSStreamMgmtReq(task.pMgmtReq, &retry));
    ASSERT_EQ(0, tCloneSStreamMgmtReq(task.pMgmtReq, &duplicate));
    ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
    auto skipDeploy = [](const SStmStreamDeploy &) {};
    ReadHeartbeatDeploys(hb, 0, skipDeploy, 101, [](const SMStreamHbRspMsg &response) {
      EXPECT_EQ(0u, taosArrayGetSize(response.rsps.rspList));
    });
    EXPECT_TRUE(pause.Entered());
    // This heartbeat uses the same group worker while value_32 is still being bound.
    SStreamHbMsg other = {};
    other.dnodeId = 1;
    ReadHeartbeatDeploys(other, 0, skipDeploy, 102);
    EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, Stream(kStreamB), block, 0, 0));
    if (alterSourceDuringPatch) ++source->metaVersion;
    pause.Release();
    msmCleanupSchemaWorker(&mnode_);
    task.pMgmtReq = retry;
    ReadHeartbeatDeploys(hb, 0, skipDeploy, 103, [&](const SMStreamHbRspMsg &response) {
      auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(response.rsps.rspList, 0));
      ASSERT_NE(nullptr, reply);
      auto *result = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
      ASSERT_NE(nullptr, result);
      if (alterSourceDuringPatch) {
        --source->metaVersion;
        EXPECT_EQ(TSDB_CODE_EXT_SOURCE_CHANGED, result->code);
        EXPECT_EQ(0u, taosArrayGetSize(result->results));
        EXPECT_EQ(0, result->acceptedTriggerBindingCount);
        EXPECT_EQ(0u, taosArrayGetSize(response.undeploy.taskList));
        EXPECT_EQ(0u, taosArrayGetSize(response.deploy.streamList));
        return;
      }
      ASSERT_EQ(0, result->code);
      ASSERT_EQ(width, result->acceptedTriggerBindingCount);
      ASSERT_EQ(width, taosArrayGetSize(result->results));
      if (originalId) {
        ASSERT_EQ(originalId, result->readerTaskAddr.taskId);
        ASSERT_EQ(1u, taosArrayGetSize(response.undeploy.taskList));
        auto *cleanup = static_cast<SStreamTaskUndeploy *>(taosArrayGet(response.undeploy.taskList, 0));
        EXPECT_EQ(originalId, cleanup->task.taskId);
        EXPECT_TRUE(cleanup->undeployMsg.doCleanup);
      } else {
        originalId = result->readerTaskAddr.taskId;
        EXPECT_NE(0, originalId);
        EXPECT_EQ(0u, taosArrayGetSize(response.undeploy.taskList));
      }
      ASSERT_EQ(1u, taosArrayGetSize(response.deploy.streamList));
      auto *stream = static_cast<SStmStreamDeploy *>(taosArrayGet(response.deploy.streamList, 0));
      ASSERT_EQ(1u, taosArrayGetSize(stream->readerTasks));
      auto *entry = static_cast<SStmTaskDeploy *>(taosArrayGet(stream->readerTasks, 0));
      ASSERT_EQ(originalId, entry->task.taskId);
      auto *spec = entry->msg.reader.pExtSpec;
      ASSERT_NE(nullptr, spec);
      ASSERT_EQ(width + 1, spec->numColMappings);
      for (int i = 1; i <= width; ++i) {
        auto *binding = static_cast<SStreamExtOrigTblReaderResult *>(taosArrayGet(result->results, i - 1));
        EXPECT_EQ(i, binding->binding.resultColumnSlot);
        EXPECT_STREQ(binding->binding.externalColumn, spec->pColMappings[i].colName);
      }
    });
    // The schema worker is stopped. A retransmission must still recover the same reply.
    ASSERT_EQ(nullptr, task.pMgmtReq);
    task.pMgmtReq = duplicate;
    ReadHeartbeatDeploys(hb, 0, skipDeploy, 104, [&](const SMStreamHbRspMsg &response) {
      ASSERT_EQ(1u, taosArrayGetSize(response.rsps.rspList));
      auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(response.rsps.rspList, 0));
      ASSERT_EQ(requestId, reply->reqId);
      auto *result = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
      ASSERT_NE(nullptr, result);
      EXPECT_EQ(alterSourceDuringPatch ? TSDB_CODE_EXT_SOURCE_CHANGED : 0, result->code);
      EXPECT_EQ(alterSourceDuringPatch ? 0 : width, result->acceptedTriggerBindingCount);
      if (!alterSourceDuringPatch) EXPECT_EQ(originalId, result->readerTaskAddr.taskId);
    });
    EXPECT_NE(nullptr, owner->pendingSchema);
    tFreeSStreamMgmtReq(task.pMgmtReq);
    taosMemoryFreeClear(task.pMgmtReq);
    taosArrayClear(hb.pStreamReq);
    if (alterSourceDuringPatch) {
      // This last result has been published and replayed but the trigger has not acknowledged it.
      EXPECT_EQ(TSDB_CODE_GRANT_STREAM_EXPIRED, msmHandleGrantExpired(&mnode_, TSDB_CODE_GRANT_STREAM_EXPIRED));
      EXPECT_TRUE(MST_IS_GRANT_STOPPED(owner->stopped));
      EXPECT_EQ(0, atomic_load_64(&mnode_.streamSchemaBatchBytes));
    } else {
      task.ackedSchemaReqId = requestId;
      ReadHeartbeatDeploys(hb, 0, skipDeploy, 105);
    }
    EXPECT_EQ(nullptr, owner->pendingSchema);
    EXPECT_EQ(0, mnode_.streamSchemaBatchCount);
    tCleanupStreamHbMsg(&hb, true);
  }
  blockDataDestroy(block);
}

extern "C" int32_t smUndeployTask(SStreamTaskUndeploy *undeploy, bool removeFromVgroup);

TEST_F(MndStreamDeployOwnershipTest, LostCreateAndPatchRespectReceiverReplayCapability) {
  UseHeartbeatClock();
  Stream(kStreamA)->mainSnodeId = 1;
  QueueDeploy(kStreamA);
  HandleActions();
  auto *owner = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_TRUE(owner && owner->triggerTask);
  SnodeLookupStubGuard snode;
  PauseExternalReaderSchema(stub_);
  stub_.set(mndGetDnodeEpsetById, +[](SMnode *, int32_t) -> SEpSet { return {}; });
  stub_.set(sdbReadLock, +[](SSdb *, int32_t) {});
  stub_.set(sdbUnLock, +[](SSdb *, int32_t) {});
  static int fetches;
  fetches = 0;
  stub_.set(
      extConnectorGetTableSchema, +[](SExtConnectorHandle *, const SExtTableNode *, SExtTableMeta **out) {
        ++fetches;
        SExtColumnDef col = {};
        tstrncpy(col.colName, "ts", sizeof(col.colName));
        tstrncpy(col.extTypeName, "timestamp", sizeof(col.extTypeName));
        col.isPrimaryKey = true;
        SExtTableMeta meta = {};
        meta.numOfCols = 1;
        meta.pCols = &col;
        *out = extConnectorCloneTableSchema(&meta);
        return *out ? 0 : terrno;
      });
  auto        skip = [](const SStmStreamDeploy &) {};
  SStreamTask previous = {};
  for (int64_t reqId : {1, 2, 3, 4}) {
    bool         patch = reqId == 2 || reqId == 3;
    bool         legacy = reqId >= 3;
    SStreamHbMsg hb = {};
    hb.dnodeId = hb.snodeId = 1;
    hb.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 1);
    hb.pStreamReq = taosArrayInit_s(sizeof(int32_t), 1);
    auto *trigger = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, 0));
    ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger, kStreamA, owner->triggerTask->id));
    trigger->status = STREAM_STATUS_RUNNING;
    trigger->ackedSchemaReqId = legacy ? 0 : -1;
    trigger->pMgmtReq->reqId = reqId;
    auto *ext = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(trigger->pMgmtReq->cont.pExtOrigTblReaders, 0));
    ext->registryEntryId = reqId == 4 ? 19 : 18;
    ext->protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
    ext->capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START;
    if (!legacy) ext->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_GUARDED_DEPLOY;
    if (patch) ext->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
    SStreamMgmtReq *retry = nullptr, *duplicate = nullptr;
    ASSERT_EQ(0, tCloneSStreamMgmtReq(trigger->pMgmtReq, &retry));
    ASSERT_EQ(0, tCloneSStreamMgmtReq(trigger->pMgmtReq, &duplicate));
    ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
    ReadHeartbeatDeploys(hb, 0, skip);
    msmCleanupSchemaWorker(&mnode_);
    SStreamTask         deployed = {};
    SStreamTaskUndeploy retired = {};
    for (auto *message : {retry, duplicate}) {
      trigger->pMgmtReq = message;
      if (legacy && message == duplicate) {
        auto noActions = [](const SStmStreamDeploy &) { ADD_FAILURE() << "Late legacy replay deployed a Reader"; };
        ReadHeartbeatDeploys(hb, 0, noActions, 111, [](const SMStreamHbRspMsg &rsp) {
          EXPECT_EQ(0u, taosArrayGetSize(rsp.rsps.rspList));
          EXPECT_EQ(0u, taosArrayGetSize(rsp.undeploy.taskList));
        });
        ReadHeartbeatDeploys(hb, 0, noActions, 110 + MST_ISOLATION_DURATION, [&](const SMStreamHbRspMsg &rsp) {
          ASSERT_EQ(1u, taosArrayGetSize(rsp.rsps.rspList));
          auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(rsp.rsps.rspList, 0));
          ASSERT_EQ(reqId, reply->reqId);
          auto *result = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
          ASSERT_NE(nullptr, result);
          EXPECT_EQ(TSDB_CODE_MND_STREAM_TASK_LOST, result->code);
          EXPECT_EQ(0u, taosArrayGetSize(rsp.undeploy.taskList));
        });
        deployed.status = STREAM_STATUS_RUNNING;
        ASSERT_NE(nullptr, taosArrayPush(hb.pStreamStatus, &deployed));
        trigger = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, 0));
        ReadHeartbeatDeploys(hb, 0, noActions, 111 + MST_ISOLATION_DURATION, [&](const SMStreamHbRspMsg &rsp) {
          ASSERT_EQ(1u, taosArrayGetSize(rsp.rsps.rspList));
          auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(rsp.rsps.rspList, 0));
          EXPECT_EQ(reqId, reply->reqId);
          auto *result = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
          ASSERT_NE(nullptr, result);
          EXPECT_EQ(0, result->code);
          EXPECT_EQ(deployed.taskId, result->readerTaskAddr.taskId);
          EXPECT_EQ(0u, taosArrayGetSize(rsp.undeploy.taskList));
        });
        taosArrayPop(hb.pStreamStatus);
        tFreeSStreamMgmtReq(trigger->pMgmtReq);
        taosMemoryFreeClear(trigger->pMgmtReq);
        continue;
      }
      ReadHeartbeatDeploys(
          hb, 0,
          [&](const SStmStreamDeploy &stream) {
            ASSERT_EQ(1u, taosArrayGetSize(stream.readerTasks));
            auto *reader = static_cast<SStmTaskDeploy *>(taosArrayGet(stream.readerTasks, 0));
            ASSERT_NE(nullptr, reader->msg.reader.pExtSpec);
            EXPECT_STREQ("ts", reader->msg.reader.pExtSpec->tsColumn);
            if (message == retry) deployed = reader->task;
            EXPECT_EQ(deployed.taskId, reader->task.taskId);
            EXPECT_EQ(deployed.seriousId, reader->task.seriousId);
          },
          110,
          [&](const SMStreamHbRspMsg &response) {
            ASSERT_EQ(1u, taosArrayGetSize(response.rsps.rspList));
            auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(response.rsps.rspList, 0));
            ASSERT_EQ(reqId, reply->reqId);
            auto *result = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
            ASSERT_NE(nullptr, result);
            ASSERT_EQ(0, result->code);
            EXPECT_EQ(patch ? 1u : 0u, taosArrayGetSize(response.undeploy.taskList));
            if (patch) {
              retired = *static_cast<SStreamTaskUndeploy *>(taosArrayGet(response.undeploy.taskList, 0));
              EXPECT_TRUE(retired.undeployMsg.doCleanup);
              EXPECT_EQ(previous.taskId, retired.task.taskId);
              EXPECT_EQ(previous.seriousId, retired.task.seriousId);
            }
          });
      // A retry consumes its prepared request. Replayed requests stay owned by the heartbeat.
      if (trigger->pMgmtReq) {
        tFreeSStreamMgmtReq(trigger->pMgmtReq);
        taosMemoryFreeClear(trigger->pMgmtReq);
      }
    }
    EXPECT_NE(0, deployed.taskId);
    EXPECT_EQ(reqId, fetches);
    if (patch) {
      EXPECT_NE(previous.seriousId, deployed.seriousId);
      EXPECT_EQ(!legacy, previous.taskId == deployed.taskId);
      // Execute the actual snode cleanup with only the replacement installed.
      static SStreamTask *installed;
      installed = &deployed;
      Stub receiver;
      receiver.set(
          taosHashAcquire, +[](SHashObj *, const void *key, size_t) -> void * {
            return static_cast<const int64_t *>(key)[1] == installed->taskId ? &installed : nullptr;
          });
      receiver.set(taosHashRelease, +[](SHashObj *, void *) {});
      EXPECT_EQ(0, smUndeployTask(&retired, true));
      EXPECT_EQ(0, deployed.deployed);
    }
    taosArrayClear(hb.pStreamReq);
    trigger->ackedSchemaReqId = legacy ? 0 : reqId;
    if (legacy) {
      // A previously built no-request report cannot acknowledge a legacy Reader deployment.
      ReadHeartbeatDeploys(hb, 0, skip);
      EXPECT_NE(nullptr, owner->pendingSchema);
      deployed.status = STREAM_STATUS_RUNNING;
      ASSERT_NE(nullptr, taosArrayPush(hb.pStreamStatus, &deployed));
    }
    ReadHeartbeatDeploys(hb, 0, skip);
    EXPECT_EQ(nullptr, owner->pendingSchema);
    EXPECT_EQ(0, atomic_load_32(&mnode_.streamSchemaBatchCount));
    previous = deployed;
    tCleanupStreamHbMsg(&hb, true);
  }
}

class MndStreamRemoteReaderTest : public MndStreamDeployOwnershipTest {
 protected:
  void SetUp() override {
    ASSERT_NO_FATAL_FAILURE(MndStreamDeployOwnershipTest::SetUp());
    UseHeartbeatClock();
    triggerHb_.snodeId = triggerHb_.dnodeId = 1;
    readerHb_.snodeId = readerHb_.dnodeId = 2;
    SStmSnodeStatus snode = {};
    int64_t         lastUp = 0;
    ASSERT_EQ(0, taosHashPut(mStreamMgmt.snodeMap, &readerHb_.snodeId, sizeof(int32_t), &snode, sizeof(snode)));
    ASSERT_EQ(0, taosHashPut(mStreamMgmt.dnodeMap, &readerHb_.dnodeId, sizeof(int32_t), &lastUp, sizeof(lastUp)));
    Stream(kStreamA)->mainSnodeId = readerHb_.snodeId;
    SNode *raw = nullptr;
    ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &raw));
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> plan(raw, nodesDestroyNode);
    auto                                               *subplan = reinterpret_cast<SSubplan *>(raw);
    subplan->subplanType = SUBPLAN_TYPE_SCAN;
    ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, reinterpret_cast<SNode **>(&subplan->pNode)));
    reinterpret_cast<STableScanPhysiNode *>(subplan->pNode)->scan.uid = 42;
    ASSERT_EQ(0, nodesNodeToString(raw, false, reinterpret_cast<char **>(&Stream(kStreamA)->pCreate->triggerScanPlan),
                                   nullptr));
    QueueDeploy(kStreamA);
    HandleActions();
    owner_ = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
    ASSERT_TRUE(owner_ && owner_->triggerTask);
    ASSERT_EQ(triggerHb_.snodeId, owner_->triggerTask->id.nodeId);

    PauseExternalReaderSchema(stub_);
    stub_.set(
        mstIsStreamDropped, +[](SMnode *, int64_t, bool *dropped) -> int32_t {
          *dropped = false;
          return 0;
        });
    stub_.set(mndGetDnodeEpsetById, +[](SMnode *, int32_t) -> SEpSet { return {}; });
    stub_.set(sdbReadLock, +[](SSdb *, int32_t) {});
    stub_.set(sdbUnLock, +[](SSdb *, int32_t) {});
    stub_.set(
        extConnectorGetTableSchema, +[](SExtConnectorHandle *, const SExtTableNode *, SExtTableMeta **out) {
          SExtColumnDef cols[2] = {};
          tstrncpy(cols[0].colName, "ts", sizeof(cols[0].colName));
          tstrncpy(cols[0].extTypeName, "timestamp", sizeof(cols[0].extTypeName));
          cols[0].isPrimaryKey = true;
          tstrncpy(cols[1].colName, "load", sizeof(cols[1].colName));
          tstrncpy(cols[1].extTypeName, "int", sizeof(cols[1].extTypeName));
          SExtTableMeta meta = {};
          meta.numOfCols = 2;
          meta.pCols = cols;
          *out = extConnectorCloneTableSchema(&meta);
          return *out ? 0 : terrno;
        });

    triggerHb_.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 1);
    triggerHb_.pStreamReq = taosArrayInit(1, sizeof(int32_t));
    readerHb_.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
    ASSERT_TRUE(triggerHb_.pStreamStatus && triggerHb_.pStreamReq && readerHb_.pStreamStatus);
    trigger_ = static_cast<SStmTaskStatusMsg *>(taosArrayGet(triggerHb_.pStreamStatus, 0));
  }

  void TearDown() override {
    tFreeSStreamMgmtReq(request_);
    taosMemoryFree(request_);
    tCleanupStreamHbMsg(&triggerHb_, true);
    tCleanupStreamHbMsg(&readerHb_, true);
    MndStreamDeployOwnershipTest::TearDown();
  }

  void PrepareRequest(bool guarded, bool patch = false) {
    tFreeSStreamMgmtReq(request_);
    taosMemoryFreeClear(request_);
    ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, streamId_, owner_->triggerTask->id));
    trigger_->status = STREAM_STATUS_RUNNING;
    trigger_->ackedSchemaReqId = guarded ? (patch ? 1 : -1) : 0;
    request_ = trigger_->pMgmtReq;
    trigger_->pMgmtReq = nullptr;
    request_->reqId = patch ? 2 : 1;
    auto *req = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(request_->cont.pExtOrigTblReaders, 0));
    if (guarded) req->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_GUARDED_DEPLOY;
    if (patch) {
      req->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
      req->bindings = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderBinding), 1);
      auto *binding = static_cast<SStreamExtOrigTblReaderBinding *>(taosArrayGet(req->bindings, 0));
      ASSERT_NE(nullptr, binding);
      binding->kind = STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER;
      binding->bindingId = 17;
      binding->expectedType = TSDB_DATA_TYPE_INT;
      tstrncpy(binding->externalColumn, "load", sizeof(binding->externalColumn));
    }
    ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
    TriggerHeartbeat(true);
    msmCleanupSchemaWorker(&mnode_);
    EXPECT_FALSE(success_);
  }

  void Submit(bool guarded, bool patch = false) {
    ASSERT_NO_FATAL_FAILURE(PrepareRequest(guarded, patch));
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));  // Publish the prepared result.
  }

  // Build a separate copy of the request so a second poll can arrive during response encoding.
  void PollLocalReader(int64_t when, SStreamTask *install, int32_t encodeError = 0) {
    *install = {};
    std::unique_ptr<SArray, decltype(&taosArrayDestroy)> reports(taosArrayInit(1, sizeof(SStmTaskStatusMsg)),
                                                                 taosArrayDestroy);
    std::unique_ptr<SArray, decltype(&taosArrayDestroy)> requests(taosArrayInit_s(sizeof(int32_t), 1),
                                                                  taosArrayDestroy);
    ASSERT_TRUE(reports && requests);
    SStreamHbMsg hb = triggerHb_;
    hb.pStreamStatus = reports.get();
    hb.pStreamReq = requests.get();
    auto *status = static_cast<SStmTaskStatusMsg *>(taosArrayPush(hb.pStreamStatus, trigger_));
    ASSERT_NE(nullptr, status);
    status->pMgmtReq = nullptr;
    ASSERT_EQ(0, tCloneSStreamMgmtReq(request_, &status->pMgmtReq));
    ReadHeartbeatDeploys(
        hb, 0,
        [&](const SStmStreamDeploy &s) {
          ASSERT_EQ(1u, taosArrayGetSize(s.readerTasks));
          *install = static_cast<SStmTaskDeploy *>(taosArrayGet(s.readerTasks, 0))->task;
        },
        when, {}, false, encodeError);
    tFreeSStreamMgmtReq(status->pMgmtReq);
    taosMemoryFree(status->pMgmtReq);
  }

  void TriggerHeartbeat(bool retry, int32_t expectedError = 0, const SStreamTask *secondReader = nullptr) {
    success_ = false;
    bool gotReply = false;
    if (retry) {
      int32_t idx = 0;
      ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamReq, &idx));
      ASSERT_EQ(0, tCloneSStreamMgmtReq(request_, &trigger_->pMgmtReq));
    }
    ReadHeartbeatDeploys(
        triggerHb_, 0, [](const SStmStreamDeploy &s) { EXPECT_EQ(0u, taosArrayGetSize(s.readerTasks)); }, now_++,
        [&](const SMStreamHbRspMsg &rsp) {
          EXPECT_EQ(0u, taosArrayGetSize(rsp.undeploy.taskList));
          size_t count = taosArrayGetSize(rsp.rsps.rspList);
          if (!count) return;
          ASSERT_TRUE(retry);
          ASSERT_EQ(1u, count);
          gotReply = true;
          auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(rsp.rsps.rspList, 0));
          EXPECT_EQ(request_->reqId, reply->reqId);
          ASSERT_EQ(0, reply->code);
          if (request_->type == STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER) {
            success_ = true;
            return;
          }
          auto *ext = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
          ASSERT_NE(nullptr, ext);
          ASSERT_EQ(expectedError, ext->code);
          if (expectedError) return;
          EXPECT_EQ(readerHb_.snodeId, ext->readerTaskAddr.nodeId);
          replyTask_ = ext->readerTaskAddr.taskId;
          if (secondReader) {
            ASSERT_EQ(2u, taosArrayGetSize(reply->cont.pExtOrigTblReaders));
            auto *second = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 1));
            EXPECT_EQ(0, second->code);
            EXPECT_EQ(secondReader->nodeId, second->readerTaskAddr.nodeId);
            EXPECT_EQ(secondReader->taskId, second->readerTaskAddr.taskId);
          }
          if (request_->reqId == 2) {
            ASSERT_EQ(1u, taosArrayGetSize(ext->results));
            auto *result = static_cast<SStreamExtOrigTblReaderResult *>(taosArrayGet(ext->results, 0));
            EXPECT_EQ(17, result->binding.bindingId);
            EXPECT_EQ(1, result->binding.resultColumnSlot);
          }
          success_ = true;
        });
    tFreeSStreamMgmtReq(trigger_->pMgmtReq);
    taosMemoryFreeClear(trigger_->pMgmtReq);
    taosArrayClear(triggerHb_.pStreamReq);
    if (expectedError) EXPECT_TRUE(gotReply);
  }

  struct Receiver {
    int64_t       triggerKey[2];
    SStreamHbInfo savedHeartbeat = {};
    bool          enabled = gStreamMgmt.snodeEnabled;
    Receiver(int64_t streamId, int64_t taskId) : triggerKey{streamId, taskId} {
      std::swap(savedHeartbeat, gStreamMgmt.hb);
      streamHbResetReaderRole(&gStreamMgmt.hb, true);
      gStreamMgmt.taskMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
      gStreamMgmt.vgroupMap = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
      taosHashSetFreeFp(
          gStreamMgmt.vgroupMap, +[](void *p) { taosArrayDestroy(static_cast<SStreamVgReaderTasks *>(p)->taskList); });
    }
    ~Receiver() {
      (void)taosHashRemove(gStreamMgmt.taskMap, triggerKey, sizeof(triggerKey));
      smUndeployAllTasks();
      taosHashCleanup(gStreamMgmt.taskMap);
      taosHashCleanup(gStreamMgmt.vgroupMap);
      taosHashCleanup(gStreamMgmt.stmGrp[STREAM_GID(triggerKey[0])]);
      gStreamMgmt.taskMap = gStreamMgmt.vgroupMap = gStreamMgmt.stmGrp[STREAM_GID(triggerKey[0])] = nullptr;
      streamHbResetReaderRole(&gStreamMgmt.hb, false);
      std::swap(savedHeartbeat, gStreamMgmt.hb);
      gStreamMgmt.snodeEnabled = enabled;
    }
  };

  void ReaderHeartbeat(const SStreamTask *installed = nullptr, EStreamStatus state = STREAM_STATUS_RUNNING) {
    if (deliverReader_) ASSERT_EQ(0, streamHbIssueReaderPoll(&gStreamMgmt.hb, now_, &readerHb_));
    taosArrayClear(readerHb_.pStreamStatus);
    if (installed) {
      auto *status = static_cast<SStmTaskStatusMsg *>(taosArrayPush(readerHb_.pStreamStatus, installed));
      ASSERT_NE(nullptr, status);
      status->status = state;
      if (state == STREAM_STATUS_FAILED) status->errorCode = TSDB_CODE_MND_STREAM_TASK_LOST;
    }
    deployment_ = {};
    cleanups_.clear();
    ReadHeartbeatDeploys(
        readerHb_, 0,
        [&](const SStmStreamDeploy &s) {
          ASSERT_EQ(1u, taosArrayGetSize(s.readerTasks));
          auto *reader = static_cast<SStmTaskDeploy *>(taosArrayGet(s.readerTasks, 0));
          EXPECT_EQ(0, deployment_.taskId);
          deployment_ = reader->task;
          EXPECT_EQ(readerHb_.readerDeployEpoch != 0, (deployment_.flags & STREAM_FLAG_REMOTE_READER_DEPLOY) != 0);
          EXPECT_EQ(streamId_, deployment_.streamId);
          EXPECT_EQ(readerHb_.snodeId, deployment_.nodeId);
          auto *spec = reader->msg.reader.pExtSpec;
          ASSERT_NE(nullptr, spec);
          EXPECT_STREQ("ts", spec->tsColumn);
          if (request_->reqId == 2) {
            ASSERT_EQ(2, spec->numColMappings);
            EXPECT_STREQ("load", spec->pColMappings[1].colName);
          } else {
            EXPECT_EQ(1, spec->numColMappings);
          }
        },
        now_++,
        [&](const SMStreamHbRspMsg &rsp) {
          EXPECT_EQ(0u, taosArrayGetSize(rsp.rsps.rspList));
          if (readerHb_.readerRuntime.id) {
            EXPECT_EQ(readerHb_.readerRuntime.id, rsp.readerRuntime.id);
            readerHb_.readerRuntime.challenge = rsp.readerRuntime.challenge;
          }
          if (taosArrayGetSize(rsp.deploy.streamList)) EXPECT_EQ(readerHb_.readerDeployEpoch, rsp.readerDeployEpoch);
          for (size_t i = 0; i < taosArrayGetSize(rsp.undeploy.taskList); ++i) {
            cleanups_.push_back(*static_cast<SStreamTaskUndeploy *>(taosArrayGet(rsp.undeploy.taskList, i)));
          }
        },
        deliverReader_);
  }

  void CheckReaderHealth() {
    Stub health;
    health.set(sdbGetSize, +[](SSdb *, ESdbType) -> int32_t { return 1; });
    health.set(mstEventPassIsolation, +[](int32_t, int32_t) { return false; });  // No catalog sweep is due.
    health.set(taosClockGetTime, returnRecalcClock);
    gRecalcNowMs = now_;
    const auto saved = mStreamMgmt.hCtx;
    mStreamMgmt.hCtx.slotIdx = readerHb_.snodeId % MND_STREAM_ISOLATION_PERIOD_NUM - 1;
    msmHealthCheck(&mnode_);
    EXPECT_EQ(now_, mStreamMgmt.hCtx.currentTs);
    mStreamMgmt.hCtx = saved;
  }

  void CheckReplacementTimeout(bool dispatch) {
    readerHb_.readerDeployEpoch = 17;
    ASSERT_NO_FATAL_FAILURE(Submit(true));
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
    const auto original = deployment_;
    ASSERT_NE(0, original.taskId);
    const auto lastReport = now_;
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&original, STREAM_STATUS_FAILED));
    int64_t key[] = {streamId_, original.taskId};
    auto  **live = static_cast<SStmTaskStatus **>(taosHashGet(mStreamMgmt.taskMap, key, sizeof(key)));
    ASSERT_NE(nullptr, live);

    now_ += MST_ISOLATION_DURATION;
    const auto queued = now_;
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
    readerHb_.readerDeployEpoch = 0;
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
    ASSERT_EQ(0, deployment_.taskId);
    CheckReaderHealth();  // The old report is stale, but the replacement has just been queued.
    ASSERT_EQ(0u, actionQueue_.qRemainNum);

    now_ = queued + MST_ISOLATION_DURATION - 4;
    if (dispatch) readerHb_.readerDeployEpoch = 18;
    const auto deadline = (dispatch ? now_ : queued) + MST_ISOLATION_DURATION;
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
    ASSERT_EQ(dispatch ? original.taskId : 0, deployment_.taskId);
    const auto replacement = (*live)->id.seriousId;
    ASSERT_NE(original.seriousId, replacement);
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));

    now_ = deadline - 2;
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());  // A poll just before expiry keeps the node online.
    if (dispatch) ASSERT_EQ(replacement, deployment_.seriousId);
    CheckReaderHealth();
    ASSERT_EQ(0u, actionQueue_.qRemainNum);
    EXPECT_EQ(lastReport, (*live)->lastUpTs);
    now_ = deadline;
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, TSDB_CODE_MND_STREAM_TASK_LOST));
    now_ = deadline;
    CheckReaderHealth();
    ASSERT_EQ(1u, actionQueue_.qRemainNum);
    EXPECT_NE(replacement, (*live)->id.seriousId);
    HandleActions();
    EXPECT_EQ(1, owner_->stopped);
    EXPECT_EQ(TSDB_CODE_MND_STREAM_TASK_LOST, owner_->fatalError);
  }

  void CheckReleased() {
    EXPECT_EQ(nullptr, owner_->pendingSchema);
    EXPECT_EQ(0, atomic_load_32(&mnode_.streamSchemaBatchCount));
    EXPECT_EQ(0, atomic_load_64(&mnode_.streamSchemaBatchBytes));
  }

  void CheckLegacyCreate(bool dropReply, int64_t *reservation = nullptr) {
    ASSERT_NO_FATAL_FAILURE(Submit(false));
    const bool firstSuccess = success_;
    const auto batchBytes = atomic_load_64(&mnode_.streamSchemaBatchBytes);
    ASSERT_GT(batchBytes, 0);
    if (reservation) *reservation = batchBytes;
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
    SStreamTask installed = deployment_;
    ASSERT_NE(0, installed.taskId);
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
    EXPECT_EQ(0, deployment_.taskId);  // A legacy Reader must never receive a second deployment.
    if (!firstSuccess) {
      ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
      ASSERT_TRUE(success_);
    }
    if (dropReply) {
      // Lose the first success to the Trigger while the Reader keeps reporting on its own node.
      now_ += MST_ISOLATION_DURATION;
      ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
      ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
      ASSERT_TRUE(success_);
    }
    EXPECT_EQ(installed.taskId, replyTask_);
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
    EXPECT_EQ(nullptr, owner_->pendingSchema);
    EXPECT_EQ(0, atomic_load_32(&mnode_.streamSchemaBatchCount));
    EXPECT_LT(atomic_load_64(&mnode_.streamSchemaBatchBytes), batchBytes);
    // An idle legacy report can have been built before publication and arrive out of order.
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
    ASSERT_TRUE(success_);
    EXPECT_EQ(installed.taskId, replyTask_);
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
    EXPECT_EQ(0, deployment_.taskId);
  }

  void AdvanceLegacyRequest() {
    int64_t next = request_->reqId + 1;
    tFreeSStreamMgmtReq(request_);
    *request_ = {};
    request_->type = STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER;
    request_->reqId = next;
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
    ASSERT_TRUE(success_);
    CheckReleased();
  }

  SnodeLookupStubGuard             snode_;
  SStmStatus                      *owner_ = nullptr;
  int64_t                          streamId_ = kStreamA;
  SStreamHbMsg                     readerHb_ = {}, triggerHb_ = {};
  SStmTaskStatusMsg               *trigger_ = nullptr;
  SStreamMgmtReq                  *request_ = nullptr;
  int64_t                          now_ = 100, replyTask_ = 0;
  bool                             success_ = false, deliverReader_ = false;
  SStreamTask                      deployment_ = {};
  std::vector<SStreamTaskUndeploy> cleanups_;
};

TEST_F(MndStreamRemoteReaderTest, OtherGroupCannotDrainColocatedReaderFromGuardedSuccess) {
  Stream(kStreamA)->mainSnodeId = triggerHb_.snodeId;
  triggerHb_.readerDeployEpoch = 17;
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, kStreamA, owner_->triggerTask->id));
  trigger_->status = STREAM_STATUS_RUNNING;
  trigger_->ackedSchemaReqId = -1;
  trigger_->pMgmtReq->reqId = 1;
  auto *entry = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(trigger_->pMgmtReq->cont.pExtOrigTblReaders, 0));
  entry->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_GUARDED_DEPLOY;
  ASSERT_EQ(0, tCloneSStreamMgmtReq(trigger_->pMgmtReq, &request_));
  int32_t index = 0;
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamReq, &index));
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  auto skip = [](const SStmStreamDeploy &) {};
  ReadHeartbeatDeploys(triggerHb_, 0, skip);
  msmCleanupSchemaWorker(&mnode_);

  ASSERT_EQ(0, tCloneSStreamMgmtReq(request_, &trigger_->pMgmtReq));
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
  HandleActions();
  auto *other = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_TRUE(other && other->triggerTask);
  SStmTaskStatusMsg second = {};
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(&second, kStreamB, other->triggerTask->id));
  second.status = STREAM_STATUS_RUNNING;
  second.pMgmtReq->reqId = 2;
  tFreeSStreamMgmtReq(second.pMgmtReq);
  second.pMgmtReq->cont = {};
  second.pMgmtReq->type = STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER;
  second.pMgmtReq->cont.pReqs = taosArrayInit_s(sizeof(SStreamDbTableName), 1);
  ASSERT_NE(nullptr, second.pMgmtReq->cont.pReqs);
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamStatus, &second));
  index = 1;
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamReq, &index));

  static std::function<void()> pollOtherGroup;
  bool                         polled = false;
  pollOtherGroup = [&] {
    SStreamHbMsg hb = {};
    hb.snodeId = hb.dnodeId = triggerHb_.snodeId;
    hb.streamGId = 1;
    hb.readerDeployEpoch = 19;
    ReadHeartbeatDeploys(hb, 0, [](const SStmStreamDeploy &stream) {
      EXPECT_EQ(0u, taosArrayGetSize(stream.readerTasks));
    });  // Drop this group's decoded reply.
    polled = true;
  };
  stub_.set(
      mstBuildDBVgroupsMap, +[](SMnode *, SSHashObj **) -> int32_t {
        pollOtherGroup();
        return TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
      });
  int64_t installed = 0, returned = 0;
  ReadHeartbeatDeploys(
      triggerHb_, 0,
      [&](const SStmStreamDeploy &stream) {
        if (stream.streamId != kStreamA) return;
        ASSERT_EQ(1u, taosArrayGetSize(stream.readerTasks));
        const auto &task = static_cast<SStmTaskDeploy *>(taosArrayGet(stream.readerTasks, 0))->task;
        EXPECT_NE(0, task.flags & STREAM_FLAG_REMOTE_READER_DEPLOY);
        installed = task.taskId;
      },
      now_++,
      [&](const SMStreamHbRspMsg &rsp) {
        ASSERT_EQ(2u, taosArrayGetSize(rsp.rsps.rspList));
        auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(rsp.rsps.rspList, 0));
        auto *ext = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
        ASSERT_NE(nullptr, ext);
        ASSERT_EQ(0, ext->code);
        returned = ext->readerTaskAddr.taskId;
      });
  pollOtherGroup = {};
  EXPECT_TRUE(polled);
  EXPECT_NE(0, installed);
  EXPECT_EQ(installed, returned);
}

TEST_F(MndStreamRemoteReaderTest, ConsecutiveLocalPatchesInstallFinalInstanceOnFirstDeliveryAndReplay) {
  Stream(kStreamA)->mainSnodeId = triggerHb_.snodeId;
  ASSERT_EQ(nullptr, gStreamMgmt.taskMap);
  ASSERT_EQ(nullptr, gStreamMgmt.vgroupMap);
  ASSERT_EQ(nullptr, gStreamMgmt.stmGrp[STREAM_GID(kStreamA)]);
  SStreamTask receiverTrigger = {};
  Receiver    receiver(kStreamA, owner_->triggerTask->id.taskId);
  receiverTrigger.type = STREAM_TRIGGER_TASK;
  receiverTrigger.streamId = kStreamA;
  receiverTrigger.taskId = owner_->triggerTask->id.taskId;
  receiverTrigger.seriousId = owner_->triggerTask->id.seriousId;
  SStreamTask *ptr = &receiverTrigger;
  ASSERT_EQ(0, taosHashPut(gStreamMgmt.taskMap, receiver.triggerKey, sizeof(receiver.triggerKey), &ptr, sizeof(ptr)));
  // Keep the Trigger request pending to simulate loss of its first acknowledgement.
  stub_.set(smHandleMgmtRsp, +[](SStreamMgmtRsps *) -> int32_t { return 0; });
  static int  opens, closes;
  static bool receiving;
  opens = closes = 0;
  receiving = false;
  stub_.set(
      extConnectorOpen, +[](const SExtSourceCfg *, SExtConnectorHandle **handle) -> int32_t {
        if (receiving) ++opens;
        *handle = reinterpret_cast<SExtConnectorHandle *>(&opens);
        return 0;
      });
  stub_.set(
      extConnectorClose, +[](SExtConnectorHandle *) {
        if (receiving) ++closes;
      });
  int32_t index = 0;
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamReq, &index));
  SStreamTask installed = {};
  for (int64_t id : {1, 2}) {
    ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, kStreamA, owner_->triggerTask->id));
    trigger_->ackedSchemaReqId = id == 1 ? -1 : 1;
    trigger_->status = STREAM_STATUS_RUNNING;
    trigger_->pMgmtReq->reqId = id;
    auto *tables = trigger_->pMgmtReq->cont.pExtOrigTblReaders;
    auto *item = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(tables, 0));
    item->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_GUARDED_DEPLOY;
    if (id == 2) {
      item->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
      SStreamExtOrigTblReaderReq last = *item;
      last.bindings = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderBinding), 1);
      auto *binding = static_cast<SStreamExtOrigTblReaderBinding *>(taosArrayGet(last.bindings, 0));
      ASSERT_NE(nullptr, binding);
      binding->kind = STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER;
      binding->bindingId = 17;
      binding->expectedType = TSDB_DATA_TYPE_INT;
      tstrncpy(binding->externalColumn, "load", sizeof(binding->externalColumn));
      ASSERT_NE(nullptr, taosArrayPush(tables, &last));
    }
    tFreeSStreamMgmtReq(request_);
    taosMemoryFreeClear(request_);
    ASSERT_EQ(0, tCloneSStreamMgmtReq(trigger_->pMgmtReq, &request_));
    receiverTrigger.pMgmtReq = request_;
    ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
    auto skip = [](const SStmStreamDeploy &) {};
    ReadHeartbeatDeploys(triggerHb_, 0, skip);
    msmCleanupSchemaWorker(&mnode_);
    const auto original = installed;
    for (int delivery = 0; delivery < id; ++delivery) {
      ASSERT_EQ(0, tCloneSStreamMgmtReq(request_, &trigger_->pMgmtReq));
      receiving = true;
      ReadHeartbeatDeploys(
          triggerHb_, 0,
          [&](const SStmStreamDeploy &stream) {
            ASSERT_EQ(1u, taosArrayGetSize(stream.readerTasks));
            auto *reader = static_cast<SStmTaskDeploy *>(taosArrayGet(stream.readerTasks, 0));
            ASSERT_NE(nullptr, reader->msg.reader.pExtSpec);
            EXPECT_EQ(id, reader->msg.reader.pExtSpec->numColMappings);
            if (delivery) EXPECT_EQ(installed.seriousId, reader->task.seriousId);
            installed = reader->task;
          },
          now_++,
          [&](const SMStreamHbRspMsg &rsp) {
            ASSERT_EQ(1u, taosArrayGetSize(rsp.rsps.rspList));
            ASSERT_EQ(1u, taosArrayGetSize(rsp.deploy.streamList));
            ASSERT_EQ(id - 1, taosArrayGetSize(rsp.undeploy.taskList));
            if (id == 2) {
              auto *cleanup = static_cast<SStreamTaskUndeploy *>(taosArrayGet(rsp.undeploy.taskList, 0));
              EXPECT_EQ(original.taskId, cleanup->task.taskId);
              EXPECT_EQ(original.seriousId, cleanup->task.seriousId);
            }
          },
          true);
      receiving = false;
      if (id == 2) {
        EXPECT_EQ(original.taskId, installed.taskId);
        EXPECT_NE(original.seriousId, installed.seriousId);
      }
      const int64_t key[] = {kStreamA, installed.taskId};
      auto        **live = static_cast<SStreamTask **>(taosHashGet(gStreamMgmt.taskMap, key, sizeof(key)));
      ASSERT_NE(nullptr, live);
      EXPECT_EQ(installed.seriousId, (*live)->seriousId);
      EXPECT_EQ(id, opens);
      EXPECT_EQ(id - 1, closes);
      tFreeSStreamMgmtReq(trigger_->pMgmtReq);
      taosMemoryFreeClear(trigger_->pMgmtReq);
    }
  }
}

TEST_F(MndStreamRemoteReaderTest, BulkPublishReusesFirstRegistrySlotAcrossBlocksAndCleanup) {
  constexpr int existing = MST_ORIGINAL_READER_LIST_SIZE, creates = 16;
  Stream(kStreamA)->mainSnodeId = triggerHb_.snodeId;
  ASSERT_EQ(0u, taosArrayGetSize(owner_->trigOReaders));
  if (!owner_->trigOReaders) owner_->trigOReaders = taosArrayInit(1, sizeof(SArray *));
  auto *block = taosArrayInit_s(sizeof(SStmTaskStatus), existing);
  ASSERT_NE(nullptr, block);
  ASSERT_NE(nullptr, taosArrayPush(owner_->trigOReaders, &block));
  // Two retired external slots share a registry ID. An internal slot with the same UID precedes both.
  for (int i : {0, 1, existing - 1}) {
    auto *slot = static_cast<SStmTaskStatus *>(taosArrayGet(block, i));
    slot->type = STREAM_READER_TASK;
    slot->id.uid = 1;
    slot->id.taskId = -(i + 1);
    if (i) {
      slot->flags = STREAM_FLAG_REF_EXT_SOURCE;
      slot->extReaderCleaned = 1;
    }
  }

  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
  HandleActions();
  auto *peer = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_TRUE(peer && peer->triggerTask);
  auto *statusBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(nullptr, statusBlock);
  SStreamHbMsg peerHb = {};
  peerHb.dnodeId = peerHb.snodeId = peer->triggerTask->id.nodeId;
  peerHb.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 1);
  auto *report = static_cast<SStmTaskStatusMsg *>(taosArrayGet(peerHb.pStreamStatus, 0));
  ASSERT_NE(nullptr, report);
  report->type = STREAM_TRIGGER_TASK;
  report->streamId = kStreamB;
  report->taskId = peer->triggerTask->id.taskId;
  report->seriousId = peer->triggerTask->id.seriousId;
  report->nodeId = peerHb.snodeId;
  report->status = STREAM_STATUS_RUNNING;

  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, kStreamA, owner_->triggerTask->id));
  trigger_->status = STREAM_STATUS_RUNNING;
  trigger_->ackedSchemaReqId = -1;
  trigger_->pMgmtReq->reqId = 1;
  auto      *tables = trigger_->pMgmtReq->cont.pExtOrigTblReaders;
  auto       table = *static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(tables, 0));
  const auto create = table.capabilities;
  for (int i = 2; i <= creates + 1; ++i) {
    table.registryEntryId = i;
    ASSERT_NE(nullptr, taosArrayPush(tables, &table));
  }
  table.capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
  ASSERT_NE(nullptr, taosArrayPush(tables, &table));  // PATCH the last CREATE in the new block.
  table.registryEntryId = 1;
  table.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
  ASSERT_NE(nullptr, taosArrayPush(tables, &table));
  table.capabilities = create;
  ASSERT_NE(nullptr, taosArrayPush(tables, &table));  // Recreate the cleaned Reader in its original slot.
  ASSERT_EQ(0, tCloneSStreamMgmtReq(trigger_->pMgmtReq, &request_));
  int32_t index = 0;
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamReq, &index));
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  auto skip = [](const SStmStreamDeploy &) {};
  ReadHeartbeatDeploys(triggerHb_, 0, skip);
  msmCleanupSchemaWorker(&mnode_);

  ASSERT_EQ(0, tCloneSStreamMgmtReq(request_, &trigger_->pMgmtReq));
  static int               slotReads;
  static thread_local bool publishing;
  static std::atomic<int>  waiting;
  slotReads = 0;
  waiting.store(0);
  PrivateBuildPause pause(false);
  Stub              count;
  count.set(
      mstWaitLock, +[](SRWLatch *lock, bool read) -> bool {
        if (read) {
          if (lock == &mStreamMgmt.actionQLock && !publishing) ++waiting;
          taosRLockLatch(lock);
        } else {
          taosWWaitLockLatch(lock);
        }
        return true;
      });
  count.set(
      taosArrayGet, +[](const SArray *array, size_t i) -> void * {
        if (!array || i >= array->size) return nullptr;
        if (publishing && array->elemSize == sizeof(SStmTaskStatus) && ++slotReads == existing / 2)
          PrivateBuildPause::Pause();
        return TARRAY_GET_ELEM(array, i);
      });
  int64_t     recreated = 0;
  std::thread publisher([&] {
    publishing = true;
    ReadHeartbeatDeploys(triggerHb_, 0, skip, now_++, [&](const SMStreamHbRspMsg &rsp) {
      ASSERT_EQ(1u, taosArrayGetSize(rsp.rsps.rspList));
      auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(rsp.rsps.rspList, 0));
      auto *results = reply->cont.pExtOrigTblReaders;
      ASSERT_EQ(creates + 4, taosArrayGetSize(results));
      auto result = [&](int i) { return static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(results, i)); };
      EXPECT_EQ(TSDB_CODE_MND_STREAM_TASK_LOST, result(0)->code);
      for (int i = 1; i < creates + 4; ++i) EXPECT_EQ(0, result(i)->code);
      EXPECT_EQ(result(creates)->readerTaskAddr.taskId, result(creates + 1)->readerTaskAddr.taskId);
      recreated = result(creates + 3)->readerTaskAddr.taskId;
      EXPECT_NE(0, recreated);
      EXPECT_NE(result(creates + 2)->readerTaskAddr.taskId, recreated);
    });
  });
  EXPECT_TRUE(pause.Entered());
  std::thread query([&] { EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, Stream(kStreamB), statusBlock, 0, 0)); });
  // Use a private heartbeat context. The fixture's RPC wrapper temporarily swaps the shared thread context.
  std::thread heartbeat([&] {
    SMStreamHbRspMsg reply = {};
    SStmGrpCtx       ctx = {};
    ctx.currTs = 123;
    ctx.pMnode = &mnode_;
    ctx.pReq = &peerHb;
    ctx.pRsp = &reply;
    bool deferred = false;
    taosRLockLatch(&mStreamMgmt.runtimeLock);
    EXPECT_EQ(0, msmNormalHandleHbMsgImpl(&ctx, &deferred, nullptr));
    taosRUnLockLatch(&mStreamMgmt.runtimeLock);
    tFreeSMStreamHbRspMsg(&reply);
  });
  const auto  deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (waiting.load() < 2 && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
  EXPECT_EQ(2, waiting.load());
  pause.Release();
  publisher.join();
  query.join();
  heartbeat.join();
  EXPECT_EQ(123, peer->triggerTask->lastUpTs);
  EXPECT_EQ(STREAM_STATUS_RUNNING, peer->triggerTask->status);
  blockDataDestroy(statusBlock);
  tCleanupStreamHbMsg(&peerHb, true);
  count.reset(taosArrayGet);
  RecordProperty("nonempty_publish_registry_slot_reads", slotReads);
  EXPECT_LE(slotReads, existing);
  EXPECT_EQ(2u, taosArrayGetSize(owner_->trigOReaders));
  EXPECT_EQ(existing + creates, msmGetTrigOReaderSize(owner_->trigOReaders));
  EXPECT_EQ(-1, static_cast<SStmTaskStatus *>(taosArrayGet(block, 0))->id.taskId);
  EXPECT_EQ(recreated, static_cast<SStmTaskStatus *>(taosArrayGet(block, 1))->id.taskId);
  EXPECT_EQ(-existing, static_cast<SStmTaskStatus *>(taosArrayGet(block, existing - 1))->id.taskId);
}

TEST_F(MndStreamRemoteReaderTest, BulkPatchAndRetirementUseExactReaderInstances) {
  constexpr int readers = 32;
  Stream(kStreamA)->mainSnodeId = triggerHb_.snodeId;
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, kStreamA, owner_->triggerTask->id));
  trigger_->status = STREAM_STATUS_RUNNING;
  trigger_->ackedSchemaReqId = -1;  // PATCH keeps the taskId and replaces its incarnation.
  trigger_->pMgmtReq->reqId = 1;
  auto *tables = trigger_->pMgmtReq->cont.pExtOrigTblReaders;
  auto  table = *static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(tables, 0));
  for (int id = 2; id <= readers; ++id) {
    table.registryEntryId = id;
    ASSERT_NE(nullptr, taosArrayPush(tables, &table));
  }
  table.capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
  for (int id = readers; id >= 1; --id) {
    table.registryEntryId = id;
    ASSERT_NE(nullptr, taosArrayPush(tables, &table));
  }
  ASSERT_EQ(0, tCloneSStreamMgmtReq(trigger_->pMgmtReq, &request_));
  int32_t index = 0;
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamReq, &index));
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  auto                   skip = [](const SStmStreamDeploy &) {};
  static const SArray   *schemas;
  static std::thread::id heartbeatThread;
  schemas = nullptr;
  heartbeatThread = std::this_thread::get_id();
  {
    Stub capture;
    capture.set(
        taosArrayInit_s, +[](size_t elem, size_t size) -> SArray * {
          auto *array = taosArrayInit(size, elem);
          if (array) array->size = size;
          if (size == 2 * readers && std::this_thread::get_id() != heartbeatThread) schemas = array;
          return array;
        });
    ReadHeartbeatDeploys(triggerHb_, 0, skip);
    msmCleanupSchemaWorker(&mnode_);
  }
  ASSERT_NE(nullptr, schemas);
  ASSERT_EQ(0, tCloneSStreamMgmtReq(request_, &trigger_->pMgmtReq));

  static int           batchReads, slotReads;
  static SStmStatus   *countedOwner;
  static const SArray *slots;
  batchReads = slotReads = 0;
  slots = nullptr;
  countedOwner = owner_;
  Stub           count;
  static int64_t incarnation;
  incarnation = 0;
  count.set(msmAssignTaskSeriousId, +[]() -> int64_t { return ++incarnation; });
  count.set(
      taosArrayGet, +[](const SArray *array, size_t i) -> void * {
        if (!array || i >= array->size) return nullptr;
        if (array == schemas) ++batchReads;
        // The first publication creates the slot block. Reading its pointer directly
        // avoids adding instrumented array reads to the measured path.
        if (!slots && countedOwner->trigOReaders && countedOwner->trigOReaders->size) {
          slots = *static_cast<SArray **>(TARRAY_GET_ELEM(countedOwner->trigOReaders, 0));
        }
        if (array == slots) ++slotReads;
        return TARRAY_GET_ELEM(array, i);
      });
  std::vector<SStreamTask> deploys;
  ReadHeartbeatDeploys(triggerHb_, 0, [&](const SStmStreamDeploy &s) {
    for (size_t i = 0; i < taosArrayGetSize(s.readerTasks); ++i)
      deploys.push_back(static_cast<SStmTaskDeploy *>(taosArrayGet(s.readerTasks, i))->task);
  });
  ASSERT_EQ(readers, deploys.size());
  ASSERT_EQ(1u, taosArrayGetSize(owner_->trigOReaders));
  RecordProperty("first_patch_registry_slot_reads", slotReads);
  // Only slots present at the start of publication need to be visited.
  EXPECT_EQ(slots, taosArrayGetP(owner_->trigOReaders, 0));
  EXPECT_LE(slotReads, readers);
  RecordProperty("first_patch_batch_reads", batchReads);
  EXPECT_LE(batchReads, 4 * 2 * readers);
  schemas = nullptr;

  auto *reports = triggerHb_.pStreamStatus;
  ASSERT_EQ(0, taosArrayEnsureCap(reports, readers + 4));
  trigger_ = static_cast<SStmTaskStatusMsg *>(taosArrayGet(reports, 0));
  for (int i = 0; i < readers; ++i) {
    auto &replacement = deploys[readers - 1 - i];
    replacement.status = STREAM_STATUS_RUNNING;
    auto reported = replacement;
    if (i == 0) --reported.seriousId;
    ASSERT_NE(nullptr, taosArrayPush(reports, &reported));
  }
  ASSERT_EQ(0, tCloneSStreamMgmtReq(request_, &trigger_->pMgmtReq));
  ReadHeartbeatDeploys(triggerHb_, 0, skip, now_++,
                       [](const SMStreamHbRspMsg &rsp) { EXPECT_EQ(0u, taosArrayGetSize(rsp.rsps.rspList)); });
  *static_cast<SStreamTask *>(taosArrayGet(reports, 1)) = deploys.back();
  ReadHeartbeatDeploys(triggerHb_, 0, skip, now_++, [&](const SMStreamHbRspMsg &rsp) {
    ASSERT_EQ(1u, taosArrayGetSize(rsp.rsps.rspList));
    auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(rsp.rsps.rspList, 0));
    EXPECT_EQ(2 * readers, taosArrayGetSize(reply->cont.pExtOrigTblReaders));
  });
  tFreeSStreamMgmtReq(trigger_->pMgmtReq);
  taosMemoryFreeClear(trigger_->pMgmtReq);
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, kStreamA, owner_->triggerTask->id));
  trigger_->ackedSchemaReqId = 1;
  trigger_->pMgmtReq->reqId = 2;
  tables = trigger_->pMgmtReq->cont.pExtOrigTblReaders;
  table.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
  taosArrayClear(tables);
  for (int id = 1; id <= readers; ++id) {
    table.registryEntryId = id;
    ASSERT_NE(nullptr, taosArrayPush(tables, &table));
  }
  taosArrayPopTailBatch(reports, readers);
  slotReads = 0;
  ReadHeartbeatDeploys(triggerHb_, 0, skip, now_++, [&](const SMStreamHbRspMsg &rsp) {
    ASSERT_EQ(readers, taosArrayGetSize(rsp.undeploy.taskList));
  });
  RecordProperty("cleanup_registry_slot_reads", slotReads);
  EXPECT_LE(slotReads, readers);
  ASSERT_EQ(readers, taosHashGetSize(owner_->retiredExtReaders));
  slots = static_cast<const SArray *>(taosArrayGetP(owner_->trigOReaders, 0));
  ASSERT_NE(nullptr, slots);
  taosArrayClear(triggerHb_.pStreamReq);
  for (auto &task : deploys) ASSERT_NE(nullptr, taosArrayPush(reports, &task));
  auto wrongNode = deploys.back();
  ++wrongNode.nodeId;
  ASSERT_NE(nullptr, taosArrayPush(reports, &wrongNode));
  auto stale = deploys.front();
  --stale.seriousId;
  ASSERT_NE(nullptr, taosArrayPush(reports, &stale));
  auto otherStream = deploys.back();
  otherStream.streamId = kStreamB;
  ASSERT_NE(nullptr, taosArrayPush(reports, &otherStream));
  for (int retry = 0; retry < 2; ++retry) {
    slotReads = 0;
    ReadHeartbeatDeploys(triggerHb_, 0, skip, now_++, [&](const SMStreamHbRspMsg &rsp) {
      ASSERT_GE(taosArrayGetSize(rsp.undeploy.taskList), readers);
      std::vector<int64_t> detached;
      for (size_t i = 0; i < taosArrayGetSize(rsp.undeploy.taskList); ++i) {
        auto *entry = static_cast<SStreamTaskUndeploy *>(taosArrayGet(rsp.undeploy.taskList, i));
        if (entry->undeployMsg.doCleanup) {
          EXPECT_FALSE(entry->undeployMsg.doCheckpoint);
          EXPECT_EQ(kStreamA, entry->task.streamId);
          EXPECT_EQ(triggerHb_.snodeId, entry->task.nodeId);
          detached.push_back(entry->task.seriousId);
        }
      }
      ASSERT_EQ(readers, detached.size());
      for (int i = 0; i < readers; ++i) EXPECT_EQ(deploys[i].seriousId, detached[i]);
    });
    RecordProperty("retired_heartbeat_slot_reads", slotReads);
    EXPECT_EQ(0, slotReads);
  }
}

TEST_F(MndStreamRemoteReaderTest, CoLocatedLegacyBatchConfirmsInLinearTimeAndAllowsOtherStreamWork) {
  constexpr int readers = 32;
  Stream(kStreamA)->mainSnodeId = triggerHb_.snodeId;
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, kStreamA, owner_->triggerTask->id));
  trigger_->status = STREAM_STATUS_RUNNING;
  trigger_->pMgmtReq->reqId = 1;
  auto *tables = trigger_->pMgmtReq->cont.pExtOrigTblReaders;
  auto  table = *static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(tables, 0));
  for (int i = 1; i < readers; ++i) {
    table.registryEntryId = i + 1;
    snprintf(table.identity.tableName, sizeof(table.identity.tableName), "cpu_%d", i);
    ASSERT_NE(nullptr, taosArrayPush(tables, &table));
  }
  ASSERT_EQ(0, tCloneSStreamMgmtReq(trigger_->pMgmtReq, &request_));
  int32_t index = 0;
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamReq, &index));
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  auto skip = [](const SStmStreamDeploy &) {};
  ReadHeartbeatDeploys(triggerHb_, 0, skip);
  msmCleanupSchemaWorker(&mnode_);
  ASSERT_EQ(0, tCloneSStreamMgmtReq(request_, &trigger_->pMgmtReq));
  std::vector<SStreamTask> deployed;
  ReadHeartbeatDeploys(triggerHb_, 0, [&](const SStmStreamDeploy &stream) {
    for (size_t i = 0; i < taosArrayGetSize(stream.readerTasks); ++i) {
      const auto *entry = static_cast<SStmTaskDeploy *>(taosArrayGet(stream.readerTasks, i));
      deployed.push_back(entry->task);
    }
  });
  ASSERT_EQ(readers, deployed.size());
  ASSERT_NE(nullptr, owner_->pendingSchema);
  auto *reports = triggerHb_.pStreamStatus;
  ASSERT_EQ(0, taosArrayEnsureCap(reports, readers + 8));
  trigger_ = static_cast<SStmTaskStatusMsg *>(taosArrayGet(reports, 0));
  taosArrayClear(triggerHb_.pStreamReq);
  for (int i = readers - 1; i > 0; --i) {
    deployed[i].status = i % 2 ? STREAM_STATUS_RUNNING : STREAM_STATUS_INIT;
    ASSERT_NE(nullptr, taosArrayPush(reports, &deployed[i]));
  }
  // All but one Reader are present. Each lookalike differs from the missing instance in just one field.
  SStreamTask missing = deployed[0];
  missing.status = STREAM_STATUS_RUNNING;
  SStreamTask impostors[] = {missing, missing, missing, missing, missing, missing};
  impostors[0].streamId = kStreamB;
  impostors[1].nodeId += 1;
  impostors[2].taskId += 100000;
  impostors[3].seriousId -= 1;
  impostors[4].type = STREAM_RUNNER_TASK;
  impostors[5].status = STREAM_STATUS_STOPPED;
  for (const auto &task : impostors) ASSERT_NE(nullptr, taosArrayPush(reports, &task));
  ReadHeartbeatDeploys(triggerHb_, 0, skip);
  ASSERT_NE(nullptr, owner_->pendingSchema);
  ASSERT_EQ(0, tCloneSStreamMgmtReq(request_, &trigger_->pMgmtReq));
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamReq, &index));
  ReadHeartbeatDeploys(triggerHb_, 0, skip, 102,
                       [](const SMStreamHbRspMsg &rsp) { EXPECT_EQ(0u, taosArrayGetSize(rsp.rsps.rspList)); });
  // An exact instance and a stale incarnation of the same address can coexist, in either order.
  taosArrayPop(reports);  // The exact Reader now reports healthy instead of STOPPED.
  ASSERT_NE(nullptr, taosArrayPush(reports, &missing));
  ASSERT_NE(nullptr, taosArrayPush(reports, &impostors[3]));

  static const SArray     *watched;
  static Stub             *failInsert;
  static std::atomic<int>  visits{0};
  static std::atomic<bool> gated{false};
  watched = reports;
  Stub count;
  count.set(
      taosArrayGet, +[](const SArray *array, size_t i) -> void * {
        if (array == watched) {
          ++visits;
          if (i == readers / 2 && !gated.exchange(true)) {
            if (failInsert)
              failInsert->set(taosHashPut, FailHashPut);
            else
              PrivateBuildPause::Pause();
          }
        }
        if (!array || i >= array->size) return nullptr;
        return TARRAY_GET_ELEM(array, i);
      });
  // Fail halfway through indexing, after the lost-task deadline. Keep the request for the next heartbeat.
  {
    Stub failure;
    failInsert = &failure;
    gated.store(false);
    ReadHeartbeatDeploys(triggerHb_, TSDB_CODE_OUT_OF_MEMORY, skip, 103 + MST_ISOLATION_DURATION);
    failInsert = nullptr;
  }
  ASSERT_TRUE(gated.load());
  ASSERT_NE(nullptr, trigger_->pMgmtReq);
  ASSERT_NE(nullptr, owner_->pendingSchema);
  auto *block = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(nullptr, block);
  for (bool retry : {true, false}) {
    visits.store(0);
    gated.store(false);
    PrivateBuildPause pause(false);
    std::thread       receiving([&] {
      ReadHeartbeatDeploys(triggerHb_, 0, skip, 104 + MST_ISOLATION_DURATION, [&](const SMStreamHbRspMsg &rsp) {
        ASSERT_EQ(retry ? 1u : 0u, taosArrayGetSize(rsp.rsps.rspList));
        if (retry) {
          auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(rsp.rsps.rspList, 0));
          ASSERT_EQ(readers, taosArrayGetSize(reply->cont.pExtOrigTblReaders));
          for (int i = 0; i < readers; ++i) {
            auto *result = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, i));
            EXPECT_EQ(0, result->code);
            EXPECT_EQ(deployed[i].taskId, result->readerTaskAddr.taskId);
          }
        }
      });
    });
    EXPECT_TRUE(pause.Entered());
    bool actionQueueAvailable = taosWTryLockLatch(&mStreamMgmt.actionQLock) == 0;
    if (actionQueueAvailable) {
      taosWUnLockLatch(&mStreamMgmt.actionQLock);
      if (retry) {
        EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, Stream(kStreamB), block, 0, 0));
      } else {
        EXPECT_EQ(
            0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
        HandleActions();
        auto *other = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
        EXPECT_TRUE(other && other->triggerTask);
      }
    }
    pause.Release();
    receiving.join();
    EXPECT_TRUE(actionQueueAvailable);
    // Includes the ordinary status/metric passes as well as the confirmation lookup.
    RecordProperty(retry ? "retry_report_reads" : "idle_report_reads", visits.load());
    EXPECT_LE(visits.load(), 6 * taosArrayGetSize(reports));
    tFreeSStreamMgmtReq(trigger_->pMgmtReq);
    taosMemoryFreeClear(trigger_->pMgmtReq);
    taosArrayClear(triggerHb_.pStreamReq);
  }
  CheckReleased();
  blockDataDestroy(block);
}

TEST_F(MndStreamRemoteReaderTest, MixedPatchRetriesOnTriggerNodeUntilBothReaderInstancesReport) {
  triggerHb_.readerDeployEpoch = 17;
  readerHb_.readerDeployEpoch = 23;
  readerHb_.readerRuntime.id = 7001;
  Stream(kStreamA)->mainSnodeId = triggerHb_.snodeId;
  auto submit = [&](int64_t id) {
    tFreeSStreamMgmtReq(request_);
    taosMemoryFreeClear(request_);
    ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, kStreamA, owner_->triggerTask->id));
    trigger_->status = STREAM_STATUS_RUNNING;
    trigger_->ackedSchemaReqId = id == 1 ? -1 : 1;
    request_ = trigger_->pMgmtReq;
    trigger_->pMgmtReq = nullptr;
    request_->reqId = id;
    auto *tables = request_->cont.pExtOrigTblReaders;
    auto *first = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(tables, 0));
    first->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_GUARDED_DEPLOY;
    if (id == 2) {
      auto create = *first;
      create.registryEntryId = 2;
      tstrncpy(create.identity.tableName, "memory", sizeof(create.identity.tableName));
      first->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
      ASSERT_NE(nullptr, taosArrayPush(tables, &create));
    }
    ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
    TriggerHeartbeat(true);
    msmCleanupSchemaWorker(&mnode_);
    EXPECT_FALSE(success_);
  };

  SStreamTask                  install = {};
  std::vector<SStreamTask>     cleanups;
  std::vector<SStreamTaskAddr> results;
  auto                         poll = [&](SStreamHbMsg &hb, bool retry) {
    install = {};
    cleanups.clear();
    results.clear();
    if (retry) {
      int32_t index = 0;
      ASSERT_NE(nullptr, taosArrayPush(hb.pStreamReq, &index));
      ASSERT_EQ(0, tCloneSStreamMgmtReq(request_, &trigger_->pMgmtReq));
    }
    ReadHeartbeatDeploys(
        hb, 0,
        [&](const SStmStreamDeploy &stream) {
          for (size_t i = 0; i < taosArrayGetSize(stream.readerTasks); ++i) {
            EXPECT_EQ(0, install.taskId);
            install = static_cast<SStmTaskDeploy *>(taosArrayGet(stream.readerTasks, i))->task;
            EXPECT_EQ(hb.snodeId, install.nodeId);
          }
        },
        now_++,
        [&](const SMStreamHbRspMsg &rsp) {
          EXPECT_EQ(hb.readerDeployEpoch, rsp.readerDeployEpoch);
          if (hb.readerRuntime.id) {
            EXPECT_EQ(hb.readerRuntime.id, rsp.readerRuntime.id);
            hb.readerRuntime.challenge = rsp.readerRuntime.challenge;
          }
          for (size_t i = 0; i < taosArrayGetSize(rsp.undeploy.taskList); ++i) {
            auto *entry = static_cast<SStreamTaskUndeploy *>(taosArrayGet(rsp.undeploy.taskList, i));
            if (entry->undeployMsg.doCleanup) cleanups.push_back(entry->task);
          }
          size_t count = taosArrayGetSize(rsp.rsps.rspList);
          if (!count) return;
          ASSERT_TRUE(retry);
          ASSERT_EQ(1u, count);
          auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(rsp.rsps.rspList, 0));
          ASSERT_EQ(request_->reqId, reply->reqId);
          ASSERT_EQ(0, reply->code);
          for (size_t i = 0; i < taosArrayGetSize(reply->cont.pExtOrigTblReaders); ++i) {
            auto *entry = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, i));
            EXPECT_EQ(i + 1, entry->registryEntryId);
            EXPECT_EQ(0, entry->code);
            results.push_back(entry->readerTaskAddr);
          }
        });
    if (retry) {
      tFreeSStreamMgmtReq(trigger_->pMgmtReq);
      taosMemoryFreeClear(trigger_->pMgmtReq);
      taosArrayClear(hb.pStreamReq);
    }
  };

  ASSERT_NO_FATAL_FAILURE(submit(1));
  ASSERT_NO_FATAL_FAILURE(poll(triggerHb_, true));
  SStreamTask previous = install;
  ASSERT_NE(0, previous.taskId);
  ASSERT_EQ(1u, results.size());
  EXPECT_EQ(previous.taskId, results[0].taskId);
  previous.status = STREAM_STATUS_RUNNING;
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamStatus, &previous));
  trigger_ = static_cast<SStmTaskStatusMsg *>(taosArrayGet(triggerHb_.pStreamStatus, 0));
  trigger_->ackedSchemaReqId = 1;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  ASSERT_NO_FATAL_FAILURE(CheckReleased());
  taosArrayPop(triggerHb_.pStreamStatus);

  // A new table uses the new main snode; PATCH keeps its established address on the Trigger's node.
  Stream(kStreamA)->mainSnodeId = readerHb_.snodeId;
  ASSERT_NO_FATAL_FAILURE(submit(2));
  ASSERT_NO_FATAL_FAILURE(poll(triggerHb_, true));
  SStreamTask replacement = install;
  ASSERT_EQ(previous.taskId, replacement.taskId);
  ASSERT_NE(previous.seriousId, replacement.seriousId);
  auto checkPatch = [&] {
    EXPECT_TRUE(results.empty());
    EXPECT_EQ(replacement.taskId, install.taskId);
    EXPECT_EQ(replacement.seriousId, install.seriousId);
    EXPECT_TRUE(install.flags & STREAM_FLAG_REMOTE_READER_DEPLOY);
    ASSERT_EQ(1u, cleanups.size());
    EXPECT_EQ(previous.nodeId, cleanups[0].nodeId);
    EXPECT_EQ(previous.taskId, cleanups[0].taskId);
    EXPECT_EQ(previous.seriousId, cleanups[0].seriousId);
    EXPECT_TRUE(cleanups[0].flags & STREAM_FLAG_REMOTE_READER_DEPLOY);
  };
  ASSERT_NO_FATAL_FAILURE(checkPatch());
  ASSERT_NO_FATAL_FAILURE(poll(readerHb_, false));
  SStreamTask remote = install;
  ASSERT_NE(0, remote.taskId);
  EXPECT_TRUE(remote.flags & STREAM_FLAG_REMOTE_READER_DEPLOY);
  EXPECT_TRUE(cleanups.empty());
  remote.status = STREAM_STATUS_RUNNING;
  ASSERT_NE(nullptr, taosArrayPush(readerHb_.pStreamStatus, &remote));
  ASSERT_NO_FATAL_FAILURE(poll(readerHb_, false));
  EXPECT_EQ(0, install.taskId);

  // Lose the original local PATCH. A missing or stale local instance must keep the result pending.
  ++triggerHb_.readerDeployEpoch;
  ASSERT_NO_FATAL_FAILURE(poll(triggerHb_, true));
  ASSERT_NO_FATAL_FAILURE(checkPatch());
  auto *reported = static_cast<SStreamTask *>(taosArrayPush(triggerHb_.pStreamStatus, &previous));
  ASSERT_NE(nullptr, reported);
  trigger_ = static_cast<SStmTaskStatusMsg *>(taosArrayGet(triggerHb_.pStreamStatus, 0));
  ASSERT_NO_FATAL_FAILURE(poll(triggerHb_, true));
  ASSERT_NO_FATAL_FAILURE(checkPatch());

  // The remote process restarts while the local replacement is still missing.
  // Even a delayed healthy report with its previous challenge cannot resurrect the acknowledgement.
  const auto oldRuntime = readerHb_.readerRuntime;
  ASSERT_NE(0, oldRuntime.challenge);
  taosArrayClear(readerHb_.pStreamStatus);
  readerHb_.readerRuntime = {7002, 0};
  ASSERT_NO_FATAL_FAILURE(poll(readerHb_, false));
  const auto restarted = readerHb_.readerRuntime;
  ASSERT_NE(0, restarted.challenge);
  ASSERT_NE(oldRuntime.challenge, restarted.challenge);
  readerHb_.readerRuntime = oldRuntime;
  ASSERT_NE(nullptr, taosArrayPush(readerHb_.pStreamStatus, &remote));
  ASSERT_NO_FATAL_FAILURE(poll(readerHb_, false));
  ASSERT_EQ(0, readerHb_.readerRuntime.challenge);  // Only the earlier sender must reintroduce itself.
  readerHb_.readerRuntime = restarted;

  *reported = replacement;
  reported->status = STREAM_STATUS_INIT;
  ASSERT_NO_FATAL_FAILURE(poll(triggerHb_, false));
  ASSERT_NO_FATAL_FAILURE(poll(triggerHb_, true));
  ASSERT_TRUE(results.empty());
  taosArrayClear(readerHb_.pStreamStatus);
  ASSERT_NO_FATAL_FAILURE(poll(readerHb_, false));
  EXPECT_EQ(restarted.challenge, readerHb_.readerRuntime.challenge);
  ASSERT_EQ(remote.taskId, install.taskId);
  ASSERT_EQ(remote.seriousId, install.seriousId);
  ASSERT_NE(nullptr, taosArrayPush(readerHb_.pStreamStatus, &remote));
  ASSERT_NO_FATAL_FAILURE(poll(readerHb_, false));
  // The previous local INIT and the remote report would authorize this retry on their own.
  // A failure in the Trigger's very same packet must withhold both addresses.
  reported->status = STREAM_STATUS_FAILED;
  reported->errorCode = TSDB_CODE_MND_STREAM_TASK_LOST;
  ASSERT_NO_FATAL_FAILURE(poll(triggerHb_, true));
  EXPECT_TRUE(results.empty());
  const SStreamTask retired = *reported;
  reported->status = STREAM_STATUS_RUNNING;
  reported->errorCode = 0;
  ASSERT_NO_FATAL_FAILURE(poll(triggerHb_, true));
  ASSERT_TRUE(results.empty());
  ASSERT_EQ(retired.taskId, install.taskId);
  ASSERT_NE(retired.seriousId, install.seriousId);
  *reported = install;
  reported->status = STREAM_STATUS_RUNNING;
  ASSERT_NO_FATAL_FAILURE(poll(triggerHb_, true));
  EXPECT_EQ(0, install.taskId);
  EXPECT_TRUE(cleanups.empty());
  ASSERT_EQ(2u, results.size());
  EXPECT_EQ(replacement.nodeId, results[0].nodeId);
  EXPECT_EQ(replacement.taskId, results[0].taskId);
  EXPECT_EQ(remote.nodeId, results[1].nodeId);
  EXPECT_EQ(remote.taskId, results[1].taskId);
  trigger_->ackedSchemaReqId = 2;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, LegacyTriggerRecoversLostSuccessAfterRemoteReaderReports) {
  readerHb_.readerDeployEpoch = 11;
  ASSERT_NO_FATAL_FAILURE(CheckLegacyCreate(true));
  AdvanceLegacyRequest();
}

TEST_F(MndStreamRemoteReaderTest, ReusedAndNewReaderBothConfirmBeforeFirstAndCachedSuccess) {
  readerHb_.readerRuntime.id = 501;
  ASSERT_NO_FATAL_FAILURE(CheckLegacyCreate(false));
  SStreamTask reused = *static_cast<SStreamTask *>(taosArrayGet(readerHb_.pStreamStatus, 0));
  auto       *tables = request_->cont.pExtOrigTblReaders;
  auto        table = *static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(tables, 0));
  table.registryEntryId = 2;
  tstrncpy(table.identity.tableName, "memory", sizeof(table.identity.tableName));
  ASSERT_NE(nullptr, taosArrayPush(tables, &table));
  request_->reqId = 3;
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  msmCleanupSchemaWorker(&mnode_);
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);

  readerHb_.readerRuntime = {502, 0};
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const SStreamTask added = deployment_;
  ASSERT_NE(0, added.taskId);
  ASSERT_NE(reused.taskId, added.taskId);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&added));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&reused));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, 0, &added));
  ASSERT_TRUE(success_);
  ASSERT_EQ(reused.taskId, replyTask_);
  ASSERT_EQ(0, atomic_load_32(&mnode_.streamSchemaBatchCount));

  readerHb_.readerRuntime = {503, 0};
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&added));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&reused));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, 0, &added));
  EXPECT_TRUE(success_);
  EXPECT_EQ(reused.taskId, replyTask_);
  AdvanceLegacyRequest();
}

TEST_F(MndStreamRemoteReaderTest, IdleCachedResultAddsNoReportVisitsOnUnrelatedNode) {
  ASSERT_NO_FATAL_FAILURE(CheckLegacyCreate(false));
  ASSERT_EQ(0, atomic_load_32(&mnode_.streamSchemaBatchCount));
  ASSERT_NE(nullptr, mnode_.streamSchemaResults);

  auto *reports = triggerHb_.pStreamStatus;
  auto  task = *trigger_;
  ASSERT_EQ(nullptr, task.pMgmtReq);
  for (int i = 1; i < 32; ++i) ASSERT_NE(nullptr, taosArrayPush(reports, &task));
  trigger_ = static_cast<SStmTaskStatusMsg *>(taosArrayGet(reports, 0));

  static const SArray *watched;
  static int           visits;
  watched = reports;
  Stub count;
  count.set(
      taosArrayGet, +[](const SArray *array, size_t i) -> void * {
        if (array == watched) ++visits;
        return array && i < array->size ? TARRAY_GET_ELEM(array, i) : nullptr;
      });
  visits = 0;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  const int cachedVisits = visits;
  ASSERT_NO_FATAL_FAILURE(AdvanceLegacyRequest());
  ASSERT_EQ(nullptr, mnode_.streamSchemaResults);
  visits = 0;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  EXPECT_GE(visits, taosArrayGetSize(reports));
  EXPECT_EQ(visits, cachedVisits);
  RecordProperty("cached_idle_report_visits", cachedVisits);
  RecordProperty("uncached_idle_report_visits", visits);
}

TEST_F(MndStreamRemoteReaderTest, TerminalFieldErrorReturnsBeforeUnconfirmedRemoteReader) {
  readerHb_.readerRuntime.id = 501;
  ASSERT_NO_FATAL_FAILURE(CheckLegacyCreate(false));
  const auto reused = *static_cast<SStreamTask *>(taosArrayGet(readerHb_.pStreamStatus, 0));
  readerHb_.readerRuntime = {502, 0};
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());  // The new process has not reported the reused address.

  auto *tables = request_->cont.pExtOrigTblReaders;
  auto  table = *static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(tables, 0));
  table.registryEntryId = 2;
  ASSERT_NE(nullptr, taosArrayPush(tables, &table));  // An independent install queued before the bad binding.
  table.registryEntryId = 3;
  table.bindings = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderBinding), 1);
  auto *binding = static_cast<SStreamExtOrigTblReaderBinding *>(taosArrayGet(table.bindings, 0));
  ASSERT_NE(nullptr, binding);
  binding->kind = STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER;
  binding->bindingId = 17;
  binding->expectedType = TSDB_DATA_TYPE_INT;
  tstrncpy(binding->externalColumn, "missing", sizeof(binding->externalColumn));
  ASSERT_NE(nullptr, taosArrayPush(tables, &table));
  request_->reqId = 3;

  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  msmCleanupSchemaWorker(&mnode_);

  int32_t i = 0;
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamReq, &i));
  auto    skip = [](const SStmStreamDeploy &) {};
  int64_t queued = 0;
  auto    checkError = [&] {
    ASSERT_EQ(0, tCloneSStreamMgmtReq(request_, &trigger_->pMgmtReq));
    ReadHeartbeatDeploys(triggerHb_, 0, skip, now_++, [&](const SMStreamHbRspMsg &rsp) {
      ASSERT_EQ(1u, taosArrayGetSize(rsp.rsps.rspList));
      auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(rsp.rsps.rspList, 0));
      ASSERT_EQ(request_->reqId, reply->reqId);
      ASSERT_EQ(0, reply->code);
      auto *entries = reply->cont.pExtOrigTblReaders;
      ASSERT_EQ(3u, taosArrayGetSize(entries));
      auto *original = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(entries, 0));
      auto *added = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(entries, 1));
      auto *bad = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(entries, 2));
      EXPECT_EQ(0, original->code);
      EXPECT_EQ(reused.taskId, original->readerTaskAddr.taskId);
      EXPECT_EQ(0, added->code);
      ASSERT_NE(0, added->readerTaskAddr.taskId);
      if (queued) EXPECT_EQ(queued, added->readerTaskAddr.taskId);
      queued = added->readerTaskAddr.taskId;
      EXPECT_EQ(3, bad->registryEntryId);
      EXPECT_EQ(TSDB_CODE_EXT_COLUMN_NOT_EXIST, bad->code);
    });
    tFreeSStreamMgmtReq(trigger_->pMgmtReq);
    taosMemoryFreeClear(trigger_->pMgmtReq);
  };
  ASSERT_NO_FATAL_FAILURE(checkError());
  now_ += MST_ISOLATION_DURATION;
  ASSERT_NO_FATAL_FAILURE(checkError());  // A lost terminal reply replays even without either healthy report.
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const auto added = deployment_;
  ASSERT_EQ(queued, added.taskId);

  // The Trigger can clean up both addresses it received in the rejected response.
  tFreeSStreamMgmtReq(request_);
  taosMemoryFreeClear(request_);
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, kStreamA, owner_->triggerTask->id));
  auto *cleanupReq = trigger_->pMgmtReq;
  cleanupReq->reqId = 4;
  auto *cleanup = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(cleanupReq->cont.pExtOrigTblReaders, 0));
  cleanup->capabilities = STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
  auto secondCleanup = *cleanup;
  secondCleanup.registryEntryId = 2;
  ASSERT_NE(nullptr, taosArrayPush(cleanupReq->cont.pExtOrigTblReaders, &secondCleanup));
  ASSERT_EQ(0, tCloneSStreamMgmtReq(cleanupReq, &request_));
  ReadHeartbeatDeploys(triggerHb_, 0, skip, now_++, [&](const SMStreamHbRspMsg &rsp) {
    ASSERT_EQ(1u, taosArrayGetSize(rsp.rsps.rspList));
    auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(rsp.rsps.rspList, 0));
    ASSERT_EQ(2u, taosArrayGetSize(reply->cont.pExtOrigTblReaders));
    for (size_t n = 0; n < 2; ++n) {
      auto *ext = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, n));
      EXPECT_EQ(0, ext->code);
      EXPECT_EQ(n == 0 ? reused.taskId : added.taskId, ext->readerTaskAddr.taskId);
    }
  });
  for (const auto &task : {reused, added}) {
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&task));
    ASSERT_EQ(1u, cleanups_.size());
    EXPECT_EQ(task.taskId, cleanups_[0].task.taskId);
    EXPECT_EQ(task.seriousId, cleanups_[0].task.seriousId);
  }
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, CollidingNodePollVisitsOneWaitEntryForManyQueuedReaders) {
  constexpr int readers = 32;
  readerHb_.snodeId = readerHb_.dnodeId = triggerHb_.snodeId + 32;
  SStmSnodeStatus node = {};
  int64_t         lastUp = 0;
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.snodeMap, &readerHb_.snodeId, sizeof(int32_t), &node, sizeof(node)));
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.dnodeMap, &readerHb_.dnodeId, sizeof(int32_t), &lastUp, sizeof(lastUp)));
  Stream(kStreamA)->mainSnodeId = readerHb_.snodeId;
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, kStreamA, owner_->triggerTask->id));
  trigger_->status = STREAM_STATUS_RUNNING;
  trigger_->ackedSchemaReqId = -1;
  trigger_->pMgmtReq->reqId = 1;
  auto *tables = trigger_->pMgmtReq->cont.pExtOrigTblReaders;
  auto  table = *static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(tables, 0));
  for (int i = 2; i <= readers; ++i) {
    table.registryEntryId = i;
    ASSERT_NE(nullptr, taosArrayPush(tables, &table));
  }
  request_ = trigger_->pMgmtReq;
  trigger_->pMgmtReq = nullptr;
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  msmCleanupSchemaWorker(&mnode_);
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  auto *target = static_cast<SStmSnodeTasksDeploy *>(
      taosHashGet(mStreamMgmt.toDeploySnodeMap, &readerHb_.snodeId, sizeof(int32_t)));
  ASSERT_NE(nullptr, target);
  ASSERT_EQ(readers, taosArrayGetSize(target->calcReaderList));

  static const SArray *bucket;
  static int           visits;
  bucket = mnode_.streamSchemaNodes[triggerHb_.snodeId % tListLen(mnode_.streamSchemaNodes)];
  ASSERT_EQ(1u, taosArrayGetSize(bucket));
  visits = 0;
  {
    Stub count;
    count.set(
        taosArrayGet, +[](const SArray *array, size_t index) -> void * {
          if (array == bucket) ++visits;
          return array && index < array->size ? TARRAY_GET_ELEM(array, index) : nullptr;
        });
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));  // Node 1 has status only. Node 33 owns every pending install.
  }
  EXPECT_EQ(1, visits);
  RecordProperty("queued_readers_on_colliding_node", readers);
  RecordProperty("unrelated_wait_entry_visits", visits);
  size_t deployed = 0;
  ReadHeartbeatDeploys(
      readerHb_, 0, [&](const SStmStreamDeploy &stream) { deployed += taosArrayGetSize(stream.readerTasks); }, now_++);
  EXPECT_EQ(readers, deployed);
  EXPECT_EQ(nullptr, mnode_.streamSchemaNodes[readerHb_.snodeId % tListLen(mnode_.streamSchemaNodes)]);
}

TEST_F(MndStreamRemoteReaderTest, UnrelatedStatusPollDuringMetadataFetchMatchesIdleWork) {
  ASSERT_NO_FATAL_FAILURE(CheckLegacyCreate(false));
  const SStreamTask reader = *static_cast<SStreamTask *>(taosArrayGet(readerHb_.pStreamStatus, 0));
  ASSERT_NO_FATAL_FAILURE(AdvanceLegacyRequest());
  for (int i = 1; i < 32; ++i) ASSERT_NE(nullptr, taosArrayPush(readerHb_.pStreamStatus, &reader));

  // A second stream's Trigger and external source request both belong to node 1.
  Stream(kStreamB)->mainSnodeId = triggerHb_.snodeId;
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
  HandleActions();
  auto *other = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_TRUE(other && other->triggerTask);
  tFreeSStreamMgmtReq(request_);
  taosMemoryFreeClear(request_);
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, kStreamB, other->triggerTask->id));
  trigger_->status = STREAM_STATUS_RUNNING;
  request_ = trigger_->pMgmtReq;
  request_->reqId = 1;
  trigger_->pMgmtReq = nullptr;
  auto skip = [](const SStmStreamDeploy &) {};
  ReadHeartbeatDeploys(readerHb_, 0, skip, now_++);

  static const SArray *watched;
  static int           visits, allocations;
  static SHashObj     *spare;
  watched = readerHb_.pStreamStatus;
  auto measure = [&] {
    // The poll can allocate at most one report index. Supply the same empty index if it asks for one.
    spare = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    ASSERT_NE(nullptr, spare);
    visits = allocations = 0;
    {
      Stub count;
      count.set(
          taosArrayGet, +[](const SArray *array, size_t i) -> void * {
            if (array == watched) ++visits;
            return array && i < array->size ? TARRAY_GET_ELEM(array, i) : nullptr;
          });
      count.set(
          taosHashInit, +[](size_t capacity, _hash_fn_t hash, bool update, SHashLockTypeE lock) -> SHashObj * {
            EXPECT_EQ(4u, capacity);
            EXPECT_EQ(taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), hash);
            EXPECT_TRUE(update);
            EXPECT_EQ(HASH_NO_LOCK, lock);
            ++allocations;
            auto *result = spare;
            spare = nullptr;
            return result;
          });
      ReadHeartbeatDeploys(readerHb_, 0, skip, now_++);
    }
    taosHashCleanup(spare);
  };

  ASSERT_NO_FATAL_FAILURE(measure());
  const int idleVisits = visits, idleAllocations = allocations;
  EXPECT_GE(idleVisits, 32);
  EXPECT_EQ(0, idleAllocations);
  PrivateBuildPause pause(false);
  Stub              blocked;
  PauseExternalReaderSchema(blocked);
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  TriggerHeartbeat(true);
  EXPECT_TRUE(pause.Entered());
  EXPECT_NE(nullptr, other->pendingSchema);
  EXPECT_EQ(1, atomic_load_32(&mnode_.streamSchemaBatchCount));
  measure();
  pause.Release();
  msmCleanupSchemaWorker(&mnode_);
  EXPECT_EQ(idleVisits, visits);
  EXPECT_EQ(idleAllocations, allocations);
  RecordProperty("idle_report_visits", idleVisits);
  RecordProperty("pending_lookup_report_visits", visits);
  RecordProperty("idle_report_index_allocations", idleAllocations);
  RecordProperty("pending_lookup_report_index_allocations", allocations);
}

TEST_F(MndStreamRemoteReaderTest, FailedReaderRejectsDelayedHealthyReportOnCachedLegacyRetry) {
  ASSERT_EQ(nullptr, gStreamMgmt.taskMap);
  ASSERT_EQ(nullptr, gStreamMgmt.vgroupMap);
  ASSERT_EQ(nullptr, gStreamMgmt.stmGrp[STREAM_GID(kStreamA)]);
  Receiver   receiver(kStreamA, owner_->triggerTask->id.taskId);
  static int closes;
  closes = 0;
  stub_.set(
      extConnectorOpen, +[](const SExtSourceCfg *, SExtConnectorHandle **out) -> int32_t {
        *out = reinterpret_cast<SExtConnectorHandle *>(&closes);
        return 0;
      });
  stub_.set(extConnectorClose, +[](SExtConnectorHandle *) { ++closes; });
  deliverReader_ = true;
  ASSERT_NO_FATAL_FAILURE(CheckLegacyCreate(false));
  const auto    installed = *static_cast<SStreamTask *>(taosArrayGet(readerHb_.pStreamStatus, 0));
  const auto    runtime = readerHb_.readerRuntime;
  const auto    epoch = readerHb_.readerDeployEpoch;
  const int64_t key[] = {kStreamA, installed.taskId};
  auto        **live = static_cast<SStreamTask **>(taosHashGet(gStreamMgmt.taskMap, key, sizeof(key)));
  ASSERT_NE(nullptr, live);
  ASSERT_EQ(installed.seriousId, (*live)->seriousId);
  (*live)->status = STREAM_STATUS_FAILED;
  const int before = closes;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed, STREAM_STATUS_FAILED));
  EXPECT_EQ(nullptr, taosHashGet(gStreamMgmt.taskMap, key, sizeof(key)));
  EXPECT_EQ(before + 1, closes);
  ASSERT_EQ(1u, cleanups_.size());
  EXPECT_EQ(installed.taskId, cleanups_[0].task.taskId);
  EXPECT_EQ(installed.seriousId, cleanups_[0].task.seriousId);
  deliverReader_ = false;
  readerHb_.readerDeployEpoch = epoch;
  readerHb_.readerRuntime = runtime;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  EXPECT_FALSE(success_);
  now_ += MST_ISOLATION_DURATION;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, TSDB_CODE_MND_STREAM_TASK_LOST));
  ASSERT_NO_FATAL_FAILURE(AdvanceLegacyRequest());
}

TEST_F(MndStreamRemoteReaderTest, PreviousRuntimeFailureCannotRemoveRestartedReader) {
  ASSERT_EQ(nullptr, gStreamMgmt.taskMap);
  ASSERT_EQ(nullptr, gStreamMgmt.vgroupMap);
  ASSERT_EQ(nullptr, gStreamMgmt.stmGrp[STREAM_GID(kStreamA)]);
  Receiver   receiver(kStreamA, owner_->triggerTask->id.taskId);
  static int handles[8], opens, closes, reads;
  opens = closes = reads = 0;
  stub_.set(
      extConnectorOpen, +[](const SExtSourceCfg *, SExtConnectorHandle **out) -> int32_t {
        if (opens == 8) return TSDB_CODE_FAILED;
        *out = reinterpret_cast<SExtConnectorHandle *>(&handles[opens++]);
        return 0;
      });
  stub_.set(extConnectorClose, +[](SExtConnectorHandle *) { ++closes; });
  deliverReader_ = true;
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const auto installed = deployment_;
  ASSERT_NE(0, installed.taskId);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  const auto previous = readerHb_.readerRuntime;
  const auto previousPoll = readerHb_.readerDeployEpoch;
  ASSERT_NE(0, previous.challenge);

  smUndeployAllTasks();  // Restart the target, keeping the management node and request alive.
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  ASSERT_NE(previous.id, readerHb_.readerRuntime.id);
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  ASSERT_EQ(installed.taskId, deployment_.taskId);
  ASSERT_EQ(installed.seriousId, deployment_.seriousId);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  const int before = closes;

  deliverReader_ = false;
  readerHb_.readerRuntime = previous;
  readerHb_.readerDeployEpoch = previousPoll;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed, STREAM_STATUS_FAILED));
  EXPECT_TRUE(cleanups_.empty());
  EXPECT_EQ(0, readerHb_.readerRuntime.challenge);
  // The same old heartbeat may also name a task no longer registered at the mnode.
  auto absent = installed;
  ++absent.taskId;
  readerHb_.readerRuntime = previous;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&absent, STREAM_STATUS_FAILED));
  EXPECT_TRUE(cleanups_.empty());
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  EXPECT_TRUE(success_);  // A packet from the stopped process cannot clear the current confirmation.
  deliverReader_ = true;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  EXPECT_TRUE(cleanups_.empty());
  EXPECT_EQ(before, closes);

  const int64_t key[] = {kStreamA, installed.taskId};
  auto        **live = static_cast<SStreamReaderTask **>(taosHashGet(gStreamMgmt.taskMap, key, sizeof(key)));
  ASSERT_NE(nullptr, live);
  ASSERT_EQ(installed.seriousId, (*live)->task.seriousId);
  stub_.set(
      extConnectorExecQuery,
      +[](SExtConnectorHandle *conn, const SFederatedScanPhysiNode *, const char *, const char *, SExtQueryHandle **out,
          SExtConnectorError *) -> int32_t {
        EXPECT_EQ(reinterpret_cast<SExtConnectorHandle *>(&handles[opens - 1]), conn);
        *out = reinterpret_cast<SExtQueryHandle *>(conn);
        return 0;
      });
  stub_.set(
      extConnectorFetchBlock,
      +[](SExtQueryHandle *, const SExtColTypeMapping *, int32_t, SSDataBlock **out, SExtConnectorError *) -> int32_t {
        *out = nullptr;
        if (++reads != 1) return 0;
        SSDataBlock *raw = nullptr;
        TAOS_CHECK_RETURN(createDataBlock(&raw));
        std::unique_ptr<SSDataBlock, decltype(&blockDataDestroy)> row(raw, blockDataDestroy);
        auto column = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1);
        TAOS_CHECK_RETURN(blockDataAppendColInfo(raw, &column));
        TAOS_CHECK_RETURN(blockDataEnsureCapacity(raw, 1));
        int64_t ts = 1234;
        colDataSetInt64(static_cast<SColumnInfoData *>(taosArrayGet(raw->pDataBlock, 0)), 0, &ts);
        raw->info.rows = 1;
        *out = row.release();
        return 0;
      });
  stub_.set(extConnectorCloseQuery, +[](SExtQueryHandle *) {});
  SSDataBlock *data = nullptr;
  ASSERT_EQ(0,
            streamReaderExtFetchData(static_cast<SStreamExtReaderInfo *>((*live)->info), INT64_MIN, INT64_MAX, &data));
  std::unique_ptr<SSDataBlock, decltype(&blockDataDestroy)> result(data, blockDataDestroy);
  ASSERT_NE(nullptr, data);
  EXPECT_EQ(2, reads);
  ASSERT_EQ(1, data->info.rows);
  EXPECT_EQ(1234, getInt64Value(data, 0, 0));

  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed, STREAM_STATUS_FAILED));
  EXPECT_EQ(1u, cleanups_.size());
  EXPECT_EQ(nullptr, taosHashGet(gStreamMgmt.taskMap, key, sizeof(key)));
  EXPECT_EQ(before + 1, closes);
  trigger_->ackedSchemaReqId = request_->reqId;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, ReplacementWaitsForTargetPollThenGetsTimeToConfirm) {
  readerHb_.readerDeployEpoch = 17;
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const auto original = deployment_;
  ASSERT_NE(0, original.taskId);
  now_ += MST_ISOLATION_DURATION;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&original, STREAM_STATUS_FAILED));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));  // Queue a fresh instance after the original batch deadline.
  ASSERT_FALSE(success_);
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  now_ += MST_ISOLATION_DURATION - 4;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const auto replacement = deployment_;
  ASSERT_EQ(original.taskId, replacement.taskId);
  ASSERT_NE(original.seriousId, replacement.seriousId);
  ASSERT_EQ(1u, cleanups_.size());
  EXPECT_EQ(original.seriousId, cleanups_[0].task.seriousId);

  now_ += 6;  // The first target poll gave this instance its own confirmation interval.
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());  // Retransmit without extending that interval.
  ASSERT_EQ(replacement.seriousId, deployment_.seriousId);
  CheckReaderHealth();
  HandleActions();
  ASSERT_EQ(0, owner_->stopped);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&replacement));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  EXPECT_EQ(replacement.taskId, replyTask_);
  trigger_->ackedSchemaReqId = request_->reqId;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, UnclaimedReplacementExpiresAtQueueDeadline) { CheckReplacementTimeout(false); }

TEST_F(MndStreamRemoteReaderTest, SilentReplacementExpiresAtFirstDispatchDeadline) { CheckReplacementTimeout(true); }

TEST_F(MndStreamRemoteReaderTest, ReplacementCannotBeClaimedByUnguardedPollAndExpiresFromQueueTime) {
  readerHb_.readerDeployEpoch = 17;
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const auto original = deployment_;
  ASSERT_NE(0, original.taskId);
  now_ += MST_ISOLATION_DURATION;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&original, STREAM_STATUS_FAILED));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  readerHb_.readerDeployEpoch = 0;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  EXPECT_EQ(0, deployment_.taskId);
  EXPECT_TRUE(cleanups_.empty());
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  EXPECT_FALSE(success_);
  now_ += MST_ISOLATION_DURATION;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, TSDB_CODE_MND_STREAM_TASK_LOST));

  readerHb_.readerDeployEpoch = 18;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const auto replacement = deployment_;
  ASSERT_EQ(original.taskId, replacement.taskId);
  ASSERT_NE(original.seriousId, replacement.seriousId);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&replacement));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  trigger_->ackedSchemaReqId = request_->reqId;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, CachedLegacyResultRequiresConfirmationFromRestartedTarget) {
  readerHb_.readerRuntime.id = 501;
  ASSERT_NO_FATAL_FAILURE(CheckLegacyCreate(false));
  SStreamTask installed = *static_cast<SStreamTask *>(taosArrayGet(readerHb_.pStreamStatus, 0));
  readerHb_.readerRuntime = {502, 0};
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  now_ += MST_ISOLATION_DURATION;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, TSDB_CODE_MND_STREAM_TASK_LOST));
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  AdvanceLegacyRequest();
}

TEST_F(MndStreamRemoteReaderTest, StaleChallengeCannotDisplaceConfirmedTargetAndLivePeerCanHandshakeAgain) {
  readerHb_.readerRuntime.id = 500;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const auto earlier = readerHb_.readerRuntime;
  readerHb_.readerRuntime = {501, 0};
  ASSERT_NO_FATAL_FAILURE(CheckLegacyCreate(false));
  const SStreamTask installed = *static_cast<SStreamTask *>(taosArrayGet(readerHb_.pStreamStatus, 0));
  readerHb_.readerRuntime = earlier;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  ASSERT_EQ(0, readerHb_.readerRuntime.challenge);
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);

  // A live peer with a challenge from another mnode can restart the handshake.
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  ASSERT_NE(0, readerHb_.readerRuntime.challenge);
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  EXPECT_EQ(installed.taskId, replyTask_);
  AdvanceLegacyRequest();
}

TEST_F(MndStreamRemoteReaderTest, LegacyHeartbeatCannotReuseConfirmationDuringTargetDowngrade) {
  readerHb_.readerRuntime.id = 501;
  ASSERT_NO_FATAL_FAILURE(CheckLegacyCreate(false));
  SStreamTask installed = *static_cast<SStreamTask *>(taosArrayGet(readerHb_.pStreamStatus, 0));
  readerHb_.readerRuntime = {};
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  now_ += MST_ISOLATION_DURATION;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  AdvanceLegacyRequest();
}

TEST_F(MndStreamRemoteReaderTest, DelayedLegacyPollLeavesRemoteDeployQueuedForCurrentTarget) {
  readerHb_.readerRuntime.id = 501;
  readerHb_.readerDeployEpoch = 17;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const auto current = readerHb_.readerRuntime;
  ASSERT_NE(0, current.challenge);
  ASSERT_NO_FATAL_FAILURE(Submit(true));

  readerHb_.readerRuntime = {};
  readerHb_.readerDeployEpoch = 0;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  ASSERT_EQ(0, deployment_.taskId);
  readerHb_.readerRuntime = current;
  readerHb_.readerDeployEpoch = 18;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  SStreamTask installed = deployment_;
  ASSERT_NE(0, installed.taskId);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  trigger_->ackedSchemaReqId = 1;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, IdleLegacyTriggerReleasesRemoteReaderBatch) {
  ASSERT_NO_FATAL_FAILURE(CheckLegacyCreate(false));
  AdvanceLegacyRequest();
}

TEST_F(MndStreamRemoteReaderTest, LegacyCachedReplyYieldsBudgetToNewStream) {
  int64_t reservation = 0;
  ASSERT_NO_FATAL_FAILURE(CheckLegacyCreate(false, &reservation));
  ASSERT_GT(atomic_load_64(&mnode_.streamSchemaBatchBytes), 0);
  auto *previousOwner = owner_;
  auto  previousTrigger = *trigger_;

  Stream(kStreamB)->mainSnodeId = readerHb_.snodeId;
  streamId_ = kStreamB;
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
  HandleActions();
  owner_ = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_TRUE(owner_ && owner_->triggerTask);
  // Model unrelated active batches. This equally sized request fits only after eviction of A's cached reply.
  const int64_t occupied = INT64_C(256) * 1024 * 1024 - reservation;
  (void)atomic_add_fetch_64(&mnode_.streamSchemaBatchBytes, occupied);
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  (void)atomic_sub_fetch_64(&mnode_.streamSchemaBatchBytes, occupied);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  SStreamTask installed = deployment_;
  ASSERT_NE(0, installed.taskId);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  trigger_->ackedSchemaReqId = 1;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();

  owner_ = previousOwner;
  streamId_ = kStreamA;
  *trigger_ = previousTrigger;
  auto *req = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(request_->cont.pExtOrigTblReaders, 0));
  req->capabilities &= ~STREAM_EXT_ORIGTBL_READER_CAP_GUARDED_DEPLOY;
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  TriggerHeartbeat(true, TSDB_CODE_MND_STREAM_NOT_AVAILABLE);
  msmCleanupSchemaWorker(&mnode_);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  EXPECT_EQ(0, deployment_.taskId);
  CheckReleased();
}

// Inject failure only when the outgoing list takes ownership of a Reader group.
static void *FailReaderGroupAppend(SArray *array, const void *data, int32_t count) {
  if (array->elemSize == sizeof(SStmStreamDeploy) &&
      taosArrayGetSize(static_cast<const SStmStreamDeploy *>(data)->readerTasks)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  void *slot = taosArrayReserve(array, count);
  return slot ? memcpy(slot, data, count * array->elemSize) : nullptr;
}

static void *FailReaderResponseAllocation(int64_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

TEST_F(MndStreamRemoteReaderTest, LegacyTargetRetriesOnlyUnsentReaderResponses) {
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  auto *slots = static_cast<SArray *>(taosArrayGetP(owner_->trigOReaders, 0));
  ASSERT_TRUE(slots && taosArrayGetSize(slots) == 1);
  auto      *reader = static_cast<SStmTaskStatus *>(taosArrayGet(slots, 0));
  const auto original = reader->id;
  const auto pickupDeadline = reader->readerRepairRetryTs;
  now_ += 1000;
  {
    Stub fail;
    fail.set(taosArrayAddBatch, FailReaderGroupAppend);
    ReadHeartbeatDeploys(readerHb_, TSDB_CODE_OUT_OF_MEMORY, [](const SStmStreamDeploy &) {}, now_++);
  }
  EXPECT_EQ(pickupDeadline, reader->readerRepairRetryTs);
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  {
    Stub fail;
    // A Trigger retry while the target response is being encoded cannot queue a duplicate.
    static std::function<void()> retry;
    retry = [&]() {
      fail.reset(rpcMallocCont);
      TriggerHeartbeat(true);
      EXPECT_FALSE(success_);
      ReaderHeartbeat();
      EXPECT_EQ(0, deployment_.taskId);
    };
    fail.set(
        rpcMallocCont, +[](int64_t size) -> void * {
          retry();
          return FailReaderResponseAllocation(size);
        });
    ReadHeartbeatDeploys(readerHb_, 0, [](const SStmStreamDeploy &) {}, now_++, {}, false, TSDB_CODE_OUT_OF_MEMORY);
    retry = {};
  }
  EXPECT_EQ(pickupDeadline, reader->readerRepairRetryTs);
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  const auto sent = now_;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const auto installed = deployment_;
  ASSERT_EQ(original.taskId, installed.taskId);
  ASSERT_EQ(original.seriousId, installed.seriousId);
  EXPECT_EQ(sent + MST_ISOLATION_DURATION, reader->readerRepairRetryTs);
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  EXPECT_EQ(0, deployment_.taskId);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  EXPECT_TRUE(success_);
  EXPECT_EQ(original.taskId, replyTask_);
}

TEST_F(MndStreamRemoteReaderTest, GuardedLocalReaderGetsFullWindowAfterFirstEncodeFails) {
  Stream(streamId_)->mainSnodeId = triggerHb_.snodeId;
  triggerHb_.readerDeployEpoch = 17;
  ASSERT_NO_FATAL_FAILURE(PrepareRequest(true));
  const auto  first = now_;
  SStreamTask install = {};
  {
    Stub                         failure;
    static std::function<void()> duringEncode;
    duringEncode = [&]() {
      failure.reset(rpcMallocCont);
      SStreamTask duplicate = {};
      PollLocalReader(first + 1, &duplicate);
      EXPECT_EQ(0, duplicate.taskId);
    };
    failure.set(
        rpcMallocCont, +[](int64_t size) -> void * {
          duringEncode();
          return FailReaderResponseAllocation(size);
        });
    PollLocalReader(first, &install, TSDB_CODE_OUT_OF_MEMORY);
    duringEncode = {};
  }
  ASSERT_EQ(0, install.taskId);
  auto *slots = static_cast<SArray *>(taosArrayGetP(owner_->trigOReaders, 0));
  ASSERT_TRUE(slots && taosArrayGetSize(slots) == 1);
  auto      *live = static_cast<SStmTaskStatus *>(taosArrayGet(slots, 0));
  const auto serial = live->id.seriousId;
  EXPECT_EQ(first + MST_ISOLATION_DURATION, live->readerRepairRetryTs);
  const auto sent = first + 25000;
  ASSERT_NO_FATAL_FAILURE(PollLocalReader(sent, &install));
  ASSERT_EQ(serial, install.seriousId);
  ASSERT_EQ(live->id.taskId, install.taskId);
  const auto deadline = sent + MST_ISOLATION_DURATION;
  RecordProperty("guarded_encoded_window_ms", static_cast<int>(live->readerRepairRetryTs - sent));
  EXPECT_EQ(deadline, live->readerRepairRetryTs);
  ASSERT_NO_FATAL_FAILURE(PollLocalReader(deadline - 2, &install));
  EXPECT_EQ(serial, install.seriousId);
  EXPECT_EQ(deadline, live->readerRepairRetryTs);
  auto saved = mStreamMgmt.hCtx.currentTs;
  mStreamMgmt.hCtx.currentTs = deadline - 1;
  msmCheckTaskListStatus(streamId_, &live, 1);
  mStreamMgmt.hCtx.currentTs = saved;
  ASSERT_EQ(0u, actionQueue_.qRemainNum);
  install.status = STREAM_STATUS_RUNNING;
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamStatus, &install));
  trigger_ = static_cast<SStmTaskStatusMsg *>(taosArrayGet(triggerHb_.pStreamStatus, 0));
  trigger_->ackedSchemaReqId = request_->reqId;
  now_ = deadline - 1;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  EXPECT_EQ(serial, live->id.seriousId);
  EXPECT_EQ(deadline - 1, live->lastUpTs);
  EXPECT_GT(live->remoteSchemaConfirmation, 0);
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, ColocatedLegacyRetryUsesFirstSuccessfulDispatchDeadline) {
  Stream(streamId_)->mainSnodeId = triggerHb_.snodeId;
  {
    Stub fail;
    fail.set(taosArrayAddBatch, FailReaderGroupAppend);
    ASSERT_NO_FATAL_FAILURE(Submit(false));
  }
  ASSERT_FALSE(success_);
  ASSERT_NE(nullptr, owner_->pendingSchema);
  const auto  retryAt = now_ + 25000;
  SStreamTask installed = {};
  {
    Stub fail;
    fail.set(rpcMallocCont, FailReaderResponseAllocation);
    ASSERT_NO_FATAL_FAILURE(PollLocalReader(retryAt - 10000, &installed, TSDB_CODE_OUT_OF_MEMORY));
    ASSERT_EQ(0, installed.taskId);
  }
  {
    Stub         delay;
    static Stub *encoding;
    encoding = &delay;
    delay.set(
        rpcMallocCont, +[](int64_t size) -> void * {
          encoding->reset(rpcMallocCont);
          heartbeatNowNs += 25000LL * NANOSECOND_PER_MSEC;
          return rpcMallocCont(size);
        });
    ASSERT_NO_FATAL_FAILURE(PollLocalReader(retryAt, &installed));
  }
  const auto sent = heartbeatNowNs / NANOSECOND_PER_MSEC;
  ASSERT_EQ(retryAt + 25000, sent);
  ASSERT_NE(0, installed.taskId);
  auto *slots = static_cast<SArray *>(taosArrayGetP(owner_->trigOReaders, 0));
  ASSERT_TRUE(slots && taosArrayGetSize(slots) == 1);
  auto *live = static_cast<SStmTaskStatus *>(taosArrayGet(slots, 0));
  RecordProperty("legacy_encoded_window_ms", static_cast<int>(live->readerRepairRetryTs - sent));
  EXPECT_EQ(sent + MST_ISOLATION_DURATION, live->readerRepairRetryTs);
  const auto saved = mStreamMgmt.hCtx.currentTs;
  mStreamMgmt.hCtx.currentTs = sent + MST_ISOLATION_DURATION - 1;
  msmCheckTaskListStatus(streamId_, &live, 1);
  mStreamMgmt.hCtx.currentTs = saved;
  EXPECT_EQ(0u, actionQueue_.qRemainNum);
  now_ = sent + 6000;  // The publication deadline passed, but the install can still be confirmed.
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  EXPECT_FALSE(success_);
  now_ = sent + MST_ISOLATION_DURATION - 1;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  EXPECT_FALSE(success_);
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, TSDB_CODE_MND_STREAM_TASK_LOST));
  installed.status = STREAM_STATUS_RUNNING;
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamStatus, &installed));
  trigger_ = static_cast<SStmTaskStatusMsg *>(taosArrayGet(triggerHb_.pStreamStatus, 0));
  readerHb_.snodeId = triggerHb_.snodeId;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  EXPECT_TRUE(success_);
  EXPECT_EQ(installed.taskId, replyTask_);
}

TEST_F(MndStreamRemoteReaderTest, LegacyTargetGetsOneCopyWhenModernTriggerRetries) {
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const SStreamTask original = deployment_;
  ASSERT_NE(0, original.taskId);

  // A legacy target cannot reject a delayed copy after removing the Reader.
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  EXPECT_FALSE(success_);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  EXPECT_EQ(0, deployment_.taskId);
  now_ += MST_ISOLATION_DURATION;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, TSDB_CODE_MND_STREAM_TASK_LOST));
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  EXPECT_EQ(0, deployment_.taskId);

  // If the original arrives late, its target's report makes the result available.
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&original));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  EXPECT_EQ(original.taskId, replyTask_);
  trigger_->ackedSchemaReqId = 1;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, DeferredRemotePatchDeliversCleanupWithItsReplacement) {
  readerHb_.readerDeployEpoch = 17;
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const auto previous = deployment_;
  ASSERT_NE(0, previous.taskId);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&previous));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  trigger_->ackedSchemaReqId = 1;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  ASSERT_NO_FATAL_FAILURE(Submit(true, true));

  stub_.set(msmAssignTaskSnodeId, +[](SMnode *, SStreamObj *, bool) -> int32_t { return 2; });
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
  HandleActions();
  auto *other = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_TRUE(other && other->triggerTask);
  ASSERT_EQ(readerHb_.snodeId, other->triggerTask->id.nodeId);
  auto *input = owner_->deployInput;
  owner_->deployInput = nullptr;
  std::vector<int64_t> triggers;
  ReadHeartbeatDeploys(
      readerHb_, 0,
      [&](const SStmStreamDeploy &stream) {
        EXPECT_EQ(0u, taosArrayGetSize(stream.readerTasks));
        if (stream.triggerTask) triggers.push_back(stream.triggerTask->task.taskId);
      },
      now_++, [](const SMStreamHbRspMsg &rsp) { EXPECT_EQ(0u, taosArrayGetSize(rsp.undeploy.taskList)); });
  owner_->deployInput = input;
  EXPECT_EQ(std::vector<int64_t>{other->triggerTask->id.taskId}, triggers);

  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&previous));
  EXPECT_NE(0, deployment_.taskId);
  EXPECT_NE(previous.seriousId, deployment_.seriousId);
  ASSERT_EQ(1u, cleanups_.size());
  EXPECT_EQ(previous.seriousId, cleanups_[0].task.seriousId);
  const auto replacement = deployment_;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&replacement));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  trigger_->ackedSchemaReqId = 2;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, ExpiringOneBatchMovesSharedQueueReadersAtMostOnce) {
  constexpr int readers = 16;
  Stream(kStreamB)->mainSnodeId = readerHb_.snodeId;
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
  HandleActions();
  auto *other = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_TRUE(other && other->triggerTask);
  ASSERT_EQ(triggerHb_.snodeId, other->triggerTask->id.nodeId);

  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, kStreamA, owner_->triggerTask->id));
  trigger_->status = STREAM_STATUS_RUNNING;
  trigger_->ackedSchemaReqId = -1;
  request_ = trigger_->pMgmtReq;
  trigger_->pMgmtReq = nullptr;
  request_->reqId = 1;
  auto *tables = request_->cont.pExtOrigTblReaders;
  auto *first = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(tables, 0));
  first->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_GUARDED_DEPLOY;
  auto table = *first;
  for (int i = 2; i <= readers; ++i) {
    table.registryEntryId = i;
    ASSERT_NE(nullptr, taosArrayPush(tables, &table));
  }
  auto publish = [&] {
    ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
    TriggerHeartbeat(true);
    msmCleanupSchemaWorker(&mnode_);
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
    ASSERT_FALSE(success_);
  };
  ASSERT_NO_FATAL_FAILURE(publish());
  const auto older = *trigger_;
  const auto expires = now_ - 1 + MST_ISOLATION_DURATION;
  now_ += MST_ISOLATION_DURATION / 2;
  trigger_->streamId = kStreamB;
  trigger_->taskId = other->triggerTask->id.taskId;
  trigger_->seriousId = other->triggerTask->id.seriousId;
  ASSERT_NO_FATAL_FAILURE(publish());
  const auto newer = *trigger_;

  auto *node = static_cast<SStmSnodeTasksDeploy *>(
      taosHashGet(mStreamMgmt.toDeploySnodeMap, &readerHb_.snodeId, sizeof(readerHb_.snodeId)));
  ASSERT_NE(nullptr, node);
  static const SArray *watched;
  static int           moved;
  watched = node->calcReaderList;
  ASSERT_EQ(2 * readers, taosArrayGetSize(watched));
  std::vector<int64_t> expected;
  for (int i = 0; i < 2 * readers; ++i) {
    const auto &task = static_cast<SStmTaskToDeployExt *>(taosArrayGet(watched, i))->deploy.task;
    ASSERT_EQ(i < readers ? kStreamA : kStreamB, task.streamId);
    if (i >= readers) expected.push_back(task.taskId);
  }

  *trigger_ = older;
  now_ = expires;
  moved = 0;
  {
    Stub count;
    count.set(
        taosArrayRemove, +[](SArray *a, size_t index) {
          if (a == watched) moved += static_cast<int>(a->size - index - 1);
          taosArrayRemoveBatch(a, index, 1, nullptr);
        });
    count.set(
        taosArraySet, +[](SArray *a, size_t index, void *entry) {
          if (a == watched) ++moved;
          if (index < a->size) memcpy(TARRAY_GET_ELEM(a, index), entry, a->elemSize);
        });
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, TSDB_CODE_MND_STREAM_TASK_LOST));
  }
  EXPECT_LE(moved, readers);
  RecordProperty("queue_entries_moved", moved);
  EXPECT_EQ(nullptr, owner_->pendingSchema);
  ASSERT_NE(nullptr, other->pendingSchema);
  EXPECT_EQ(1, atomic_load_32(&mnode_.streamSchemaBatchCount));
  EXPECT_EQ(readers, mStreamMgmt.toDeploySnodeTaskNum);
  EXPECT_EQ(0, node->calcReaderDeployed);

  std::vector<int64_t> delivered;
  readerHb_.readerDeployEpoch = 17;
  ReadHeartbeatDeploys(
      readerHb_, 0,
      [&](const SStmStreamDeploy &stream) {
        ASSERT_EQ(kStreamB, stream.streamId);
        for (size_t i = 0; i < taosArrayGetSize(stream.readerTasks); ++i) {
          auto *entry = static_cast<SStmTaskDeploy *>(taosArrayGet(stream.readerTasks, i));
          ASSERT_NE(nullptr, entry->msg.reader.pExtSpec);
          auto task = entry->task;
          delivered.push_back(task.taskId);
          task.status = STREAM_STATUS_RUNNING;
          ASSERT_NE(nullptr, taosArrayPush(readerHb_.pStreamStatus, &task));
        }
      },
      now_++);
  EXPECT_EQ(expected, delivered);
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
  EXPECT_EQ(nullptr, taosHashGet(mStreamMgmt.toDeploySnodeMap, &readerHb_.snodeId, sizeof(readerHb_.snodeId)));
  ReadHeartbeatDeploys(readerHb_, 0, [](const SStmStreamDeploy &) { ADD_FAILURE() << "unexpected replay"; }, now_++);
  *trigger_ = newer;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  trigger_->ackedSchemaReqId = request_->reqId;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  EXPECT_EQ(nullptr, other->pendingSchema);
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, AnUnclaimedInitialInstallReleasesItsBatchAtTheDeadline) {
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  const auto queued = now_ - 1;
  now_ = queued + MST_ISOLATION_DURATION - 1;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  ASSERT_NE(nullptr, owner_->pendingSchema);
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, TSDB_CODE_MND_STREAM_TASK_LOST));
  ASSERT_NO_FATAL_FAILURE(CheckReleased());
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, TSDB_CODE_MND_STREAM_TASK_LOST));
  readerHb_.readerDeployEpoch = 17;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  EXPECT_EQ(0, deployment_.taskId);
}

TEST_F(MndStreamRemoteReaderTest, LateFirstPollCannotClaimAnExpiredInstall) {
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  readerHb_.readerDeployEpoch = 17;
  now_ += MST_ISOLATION_DURATION;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  EXPECT_EQ(0, deployment_.taskId);
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, TSDB_CODE_MND_STREAM_TASK_LOST));
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, MixedReaderKeepsFullClaimWindowAfterPreparingLocalRoute) {
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, streamId_, owner_->triggerTask->id));
  trigger_->status = STREAM_STATUS_RUNNING;
  trigger_->ackedSchemaReqId = -1;
  request_ = trigger_->pMgmtReq;
  trigger_->pMgmtReq = nullptr;
  request_->reqId = 1;
  auto *ext = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(request_->cont.pExtOrigTblReaders, 0));
  ext->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_GUARDED_DEPLOY;
  request_->cont.pReqs = taosArrayInit_s(sizeof(SStreamDbTableName), 1);
  ASSERT_NE(nullptr, request_->cont.pReqs);
  auto *table = static_cast<SStreamDbTableName *>(taosArrayGet(request_->cont.pReqs, 0));
  tstrncpy(table->dbFName, "1.db", sizeof(table->dbFName));
  tstrncpy(table->tbName, "sensor", sizeof(table->tbName));
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  TriggerHeartbeat(true);
  msmCleanupSchemaWorker(&mnode_);

  stub_.set(
      mstBuildDBVgroupsMap, +[](SMnode *, SSHashObj **map) -> int32_t {
        *map = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
        return *map ? 0 : terrno;
      });
  stub_.set(
      mstGetTableVgId, +[](SSHashObj *, char *, char *, int32_t *id) -> int32_t {
        heartbeatNowNs += (MST_ISOLATION_DURATION / 2) * NANOSECOND_PER_MSEC;
        *id = 5;
        return 0;
      });
  // The local Reader was already requested. The retry still resolves its current routing.
  owner_->lastTrigMgmtReqId = request_->reqId;
  const int64_t published = now_ + MST_ISOLATION_DURATION / 2;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_EQ(published, heartbeatNowNs / NANOSECOND_PER_MSEC);
  ASSERT_FALSE(success_);

  now_ = published + MST_ISOLATION_DURATION - 2;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  readerHb_.readerDeployEpoch = 17;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const auto installed = deployment_;
  ASSERT_NE(0, installed.taskId);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  EXPECT_EQ(installed.taskId, replyTask_);
  trigger_->ackedSchemaReqId = request_->reqId;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, ColocatedReaderStartsConfirmationWhenItsBatchIsAppended) {
  Stream(streamId_)->mainSnodeId = triggerHb_.snodeId;
  triggerHb_.readerDeployEpoch = 17;
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, streamId_, owner_->triggerTask->id));
  trigger_->status = STREAM_STATUS_RUNNING;
  trigger_->ackedSchemaReqId = -1;
  request_ = trigger_->pMgmtReq;
  trigger_->pMgmtReq = nullptr;
  request_->reqId = 1;
  auto *tables = request_->cont.pExtOrigTblReaders;
  auto *first = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(tables, 0));
  first->capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_GUARDED_DEPLOY;
  auto table = *first;
  for (int i = 2; i <= 3; ++i) {
    table.registryEntryId = i;
    ASSERT_NE(nullptr, taosArrayPush(tables, &table));
  }
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  TriggerHeartbeat(true);
  msmCleanupSchemaWorker(&mnode_);

  std::vector<SStreamTask> installs;
  std::vector<int64_t>     retired;
  auto                     retry = [&](bool expectReply) {
    installs.clear();
    retired.clear();
    int32_t index = 0;
    ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamReq, &index));
    ASSERT_EQ(0, tCloneSStreamMgmtReq(request_, &trigger_->pMgmtReq));
    ReadHeartbeatDeploys(
        triggerHb_, 0,
        [&](const SStmStreamDeploy &stream) {
          for (size_t i = 0; i < taosArrayGetSize(stream.readerTasks); ++i)
            installs.push_back(static_cast<SStmTaskDeploy *>(taosArrayGet(stream.readerTasks, i))->task);
        },
        now_++,
        [&](const SMStreamHbRspMsg &rsp) {
          EXPECT_EQ(expectReply ? 1u : 0u, taosArrayGetSize(rsp.rsps.rspList));
          for (size_t i = 0; i < taosArrayGetSize(rsp.undeploy.taskList); ++i) {
            auto *cleanup = static_cast<SStreamTaskUndeploy *>(taosArrayGet(rsp.undeploy.taskList, i));
            retired.push_back(cleanup->task.seriousId);
          }
        });
    taosArrayClear(triggerHb_.pStreamReq);
    tFreeSStreamMgmtReq(trigger_->pMgmtReq);
    taosMemoryFreeClear(trigger_->pMgmtReq);
  };

  // Advance the heartbeat clock just as the complete group enters the reply. On the
  // failing attempt, the middle Reader already has its new serial when the last push fails.
  static bool failLast, pendingGroup;
  failLast = pendingGroup = false;
  Stub append;
  append.set(
      taosArrayAddBatch, +[](SArray *array, const void *data, int32_t count) -> void * {
        if (array->elemSize == sizeof(SStmTaskDeploy) && array->size == 2) {
          if (failLast) {
            failLast = false;
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            return nullptr;
          }
          pendingGroup = true;
        } else if (pendingGroup && array->elemSize == sizeof(SStmStreamDeploy)) {
          pendingGroup = false;
          heartbeatNowNs += (MST_ISOLATION_DURATION / 2) * NANOSECOND_PER_MSEC;
        }
        void *slot = taosArrayReserve(array, count);
        return slot ? memcpy(slot, data, count * array->elemSize) : nullptr;
      });
  const int64_t firstDispatch = now_ + MST_ISOLATION_DURATION / 2;
  ASSERT_NO_FATAL_FAILURE(retry(true));
  ASSERT_EQ(3u, installs.size());
  ASSERT_TRUE(retired.empty());
  ASSERT_EQ(firstDispatch, heartbeatNowNs / NANOSECOND_PER_MSEC);
  const auto      original = installs;
  SStmTaskStatus *live[3] = {};
  for (size_t i = 0; i < original.size(); ++i) {
    const int64_t key[] = {streamId_, original[i].taskId};
    auto        **entry = static_cast<SStmTaskStatus **>(taosHashGet(mStreamMgmt.taskMap, key, sizeof(key)));
    ASSERT_TRUE(entry && *entry);
    live[i] = *entry;
    EXPECT_EQ(firstDispatch + MST_ISOLATION_DURATION, live[i]->readerRepairRetryTs);
  }
  now_ = firstDispatch + 1;
  auto failed = original[1];
  failed.status = STREAM_STATUS_FAILED;
  failed.errorCode = TSDB_CODE_MND_STREAM_TASK_LOST;
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamStatus, &failed));
  trigger_ = static_cast<SStmTaskStatusMsg *>(taosArrayGet(triggerHb_.pStreamStatus, 0));
  ReadHeartbeatDeploys(triggerHb_, 0, [](const SStmStreamDeploy &) {}, now_++);
  taosArrayPop(triggerHb_.pStreamStatus);

  failLast = true;
  ASSERT_NO_FATAL_FAILURE(retry(false));
  ASSERT_FALSE(failLast);
  ASSERT_TRUE(installs.empty());
  ASSERT_TRUE(retired.empty());
  const auto replacement = live[1]->id.seriousId;
  ASSERT_NE(original[1].seriousId, replacement);

  now_ += MST_ISOLATION_DURATION / 4;
  const int64_t dispatched = now_ + MST_ISOLATION_DURATION / 2;
  ASSERT_NO_FATAL_FAILURE(retry(true));
  append.reset(taosArrayAddBatch);
  ASSERT_EQ(dispatched, heartbeatNowNs / NANOSECOND_PER_MSEC);
  ASSERT_EQ(3u, installs.size());
  EXPECT_EQ(std::vector<int64_t>{original[1].seriousId}, retired);
  const int64_t deadline = dispatched + MST_ISOLATION_DURATION;
  for (size_t i = 0; i < installs.size(); ++i) {
    EXPECT_EQ(original[i].taskId, installs[i].taskId);
    EXPECT_EQ(i == 1 ? replacement : original[i].seriousId, installs[i].seriousId);
    EXPECT_EQ(i == 1 ? deadline : firstDispatch + MST_ISOLATION_DURATION, live[i]->readerRepairRetryTs);
  }
  RecordProperty("local_replacement_confirmation_ms", static_cast<int>(live[1]->readerRepairRetryTs - dispatched));

  now_ = deadline - 2;
  ASSERT_NO_FATAL_FAILURE(retry(true));
  ASSERT_EQ(3u, installs.size());
  EXPECT_EQ(replacement, installs[1].seriousId);
  EXPECT_EQ(deadline, live[1]->readerRepairRetryTs);
  const int64_t saved = mStreamMgmt.hCtx.currentTs;
  mStreamMgmt.hCtx.currentTs = now_;
  msmCheckTaskListStatus(streamId_, &live[1], 1);
  mStreamMgmt.hCtx.currentTs = saved;
  ASSERT_EQ(0u, actionQueue_.qRemainNum);
  installs[1].status = STREAM_STATUS_RUNNING;
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamStatus, &installs[1]));
  trigger_ = static_cast<SStmTaskStatusMsg *>(taosArrayGet(triggerHb_.pStreamStatus, 0));
  trigger_->ackedSchemaReqId = request_->reqId;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  EXPECT_EQ(deadline - 1, live[1]->lastUpTs);
  EXPECT_GT(live[1]->remoteSchemaConfirmation, 0);
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, FirstReaderDeliveryStartsConfirmationWindowAfterLockWait) {
  readerHb_.readerDeployEpoch = 17;
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  const int64_t dispatched = now_ + MST_ISOLATION_DURATION / 2;
  {
    static bool delayed;
    delayed = false;
    Stub wait;
    wait.set(
        mstWaitLock, +[](SRWLatch *lock, bool read) -> bool {
          if (read) {
            taosRLockLatch(lock);
            if (!delayed && lock == &mStreamMgmt.actionQLock) {
              heartbeatNowNs += (MST_ISOLATION_DURATION / 2) * NANOSECOND_PER_MSEC;
              delayed = true;
            }
          } else {
            taosWLockLatch(lock);
          }
          return true;
        });
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  }
  const auto installed = deployment_;
  ASSERT_NE(0, installed.taskId);
  ASSERT_EQ(dispatched, heartbeatNowNs / NANOSECOND_PER_MSEC);
  const int64_t deadline = dispatched + MST_ISOLATION_DURATION;
  int64_t       key[] = {streamId_, installed.taskId};
  auto        **live = static_cast<SStmTaskStatus **>(taosHashGet(mStreamMgmt.taskMap, key, sizeof(key)));
  ASSERT_TRUE(live && *live);
  EXPECT_EQ(deadline, (*live)->readerRepairRetryTs);

  now_ = deadline - 3;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  ASSERT_EQ(installed.taskId, deployment_.taskId);
  ASSERT_EQ(installed.seriousId, deployment_.seriousId);
  EXPECT_EQ(deadline, (*live)->readerRepairRetryTs);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  EXPECT_EQ(installed.taskId, replyTask_);
  trigger_->ackedSchemaReqId = request_->reqId;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, LostNodeReleasesInstallAfterTriggerHasReplacedItsRequest) {
  stub_.set(sdbFetch, +[](SSdb *, ESdbType, void *, void **) -> void * { return nullptr; });
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  tFreeSStreamMgmtReq(request_);
  *request_ = {};
  request_->type = STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER;
  request_->reqId = 2;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  ASSERT_EQ(nullptr, owner_->pendingSchema);
  now_ += MST_ISOLATION_DURATION;
  CheckReaderHealth();
  CheckReleased();
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
}

TEST_F(MndStreamRemoteReaderTest, NodeLossReleasesUnclaimedReaderWhileRunnerMovesToSurvivingNode) {
  SStmStatus *other = owner_;
  streamId_ = kStreamB;
  ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(false, streamId_));
  Stream(streamId_)->mainSnodeId = readerHb_.snodeId;
  static int placements;
  placements = 0;
  stub_.set(msmAssignTaskSnodeId, +[](SMnode *, SStreamObj *, bool) -> int32_t { return ++placements == 2 ? 2 : 1; });
  stub_.set(sdbFetch, +[](SSdb *, ESdbType, void *, void **) -> void * { return nullptr; });
  ASSERT_EQ(0,
            mstPostStreamAction(&actionQueue_, streamId_, Stream(streamId_)->name, nullptr, false, STREAM_ACT_DEPLOY));
  HandleActions();
  owner_ = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &streamId_, sizeof(streamId_)));
  ASSERT_TRUE(owner_ && owner_->triggerTask);
  ASSERT_EQ(triggerHb_.snodeId, owner_->triggerTask->id.nodeId);
  auto *runner = static_cast<SStmTaskStatus *>(taosArrayGet(owner_->runners[0], 0));
  ASSERT_NE(nullptr, runner);
  ASSERT_EQ(readerHb_.snodeId, runner->id.nodeId);
  const auto originalRunner = runner->id;
  atomic_store_32(&owner_->runnerReplica, 1);
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  ASSERT_NE(nullptr, owner_->pendingSchema);

  now_ += MST_ISOLATION_DURATION;
  CheckReaderHealth();  // The target never picked up its Reader.
  HandleActions();
  ASSERT_EQ(0, owner_->stopped);
  EXPECT_EQ(originalRunner.taskId, runner->id.taskId);
  EXPECT_NE(originalRunner.seriousId, runner->id.seriousId);
  EXPECT_EQ(triggerHb_.snodeId, runner->id.nodeId);
  ASSERT_NO_FATAL_FAILURE(CheckReleased());
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, TSDB_CODE_MND_STREAM_TASK_LOST));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true, TSDB_CODE_MND_STREAM_TASK_LOST));

  // Another stream can start preparing its request while the lost node is still offline.
  streamId_ = kStreamA;
  owner_ = other;
  Stream(streamId_)->mainSnodeId = triggerHb_.snodeId;
  tFreeSStreamMgmtReq(request_);
  taosMemoryFreeClear(request_);
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(trigger_, streamId_, owner_->triggerTask->id));
  trigger_->status = STREAM_STATUS_RUNNING;
  trigger_->ackedSchemaReqId = -1;
  request_ = trigger_->pMgmtReq;
  trigger_->pMgmtReq = nullptr;
  request_->reqId = 1;
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  TriggerHeartbeat(true);
  msmCleanupSchemaWorker(&mnode_);
  ASSERT_NE(nullptr, owner_->pendingSchema);
  ASSERT_EQ(1, atomic_load_32(&mnode_.streamSchemaBatchCount));

  int32_t index = 0;
  ASSERT_NE(nullptr, taosArrayPush(triggerHb_.pStreamReq, &index));
  ASSERT_EQ(0, tCloneSStreamMgmtReq(request_, &trigger_->pMgmtReq));
  int64_t installed = 0, returned = 0;
  ReadHeartbeatDeploys(
      triggerHb_, 0,
      [&](const SStmStreamDeploy &stream) {
        if (stream.streamId != streamId_) return;
        ASSERT_EQ(1u, taosArrayGetSize(stream.readerTasks));
        installed = static_cast<SStmTaskDeploy *>(taosArrayGet(stream.readerTasks, 0))->task.taskId;
      },
      now_++,
      [&](const SMStreamHbRspMsg &rsp) {
        ASSERT_EQ(1u, taosArrayGetSize(rsp.rsps.rspList));
        auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(rsp.rsps.rspList, 0));
        ASSERT_EQ(0, reply->code);
        auto *ext = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
        ASSERT_NE(nullptr, ext);
        ASSERT_EQ(0, ext->code);
        EXPECT_EQ(triggerHb_.snodeId, ext->readerTaskAddr.nodeId);
        returned = ext->readerTaskAddr.taskId;
      });
  taosArrayClear(triggerHb_.pStreamReq);
  tFreeSStreamMgmtReq(trigger_->pMgmtReq);
  taosMemoryFreeClear(trigger_->pMgmtReq);
  EXPECT_NE(0, returned);
  EXPECT_EQ(returned, installed);
  trigger_->ackedSchemaReqId = 1;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, NormalPatchSurvivesHealthScanUntilFirstDeliveryIsConfirmed) {
  readerHb_.readerDeployEpoch = 17;
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  const auto previous = deployment_;
  ASSERT_NE(0, previous.taskId);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&previous));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  trigger_->ackedSchemaReqId = 1;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();

  now_ += MST_ISOLATION_DURATION;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());  // The node is online, but the old Reader report is stale.
  ASSERT_NO_FATAL_FAILURE(Submit(true, true));
  const auto queued = now_ - 1;
  CheckReaderHealth();
  HandleActions();
  ASSERT_EQ(0, owner_->stopped);
  now_ = queued + MST_ISOLATION_DURATION - 2;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&previous));
  const auto replacement = deployment_;
  ASSERT_NE(0, replacement.taskId);
  ASSERT_NE(previous.taskId, replacement.taskId);
  ASSERT_NE(previous.seriousId, replacement.seriousId);
  int64_t key[] = {streamId_, replacement.taskId};
  auto  **live = static_cast<SStmTaskStatus **>(taosHashGet(mStreamMgmt.taskMap, key, sizeof(key)));
  ASSERT_TRUE(live && *live);
  const auto deadline = queued + 2 * MST_ISOLATION_DURATION - 2;
  EXPECT_EQ(deadline, (*live)->readerRepairRetryTs);

  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  now_ = deadline - 3;
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&previous));
  ASSERT_EQ(replacement.seriousId, deployment_.seriousId);
  CheckReaderHealth();
  HandleActions();
  ASSERT_EQ(0, owner_->stopped);
  ASSERT_EQ(deadline, (*live)->readerRepairRetryTs);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&replacement));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  EXPECT_EQ(replacement.taskId, replyTask_);
  trigger_->ackedSchemaReqId = request_->reqId;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();
}

TEST_F(MndStreamRemoteReaderTest, RemotePatchSurvivesLostDeploymentAndTriggerRetry) {
  readerHb_.readerDeployEpoch = 17;
  ASSERT_NO_FATAL_FAILURE(Submit(true));
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  SStreamTask previous = deployment_;
  ASSERT_NE(0, previous.taskId);
  // Hold A's first CREATE while B retries. A must receive the same instance.
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_FALSE(success_);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  ASSERT_EQ(previous.taskId, deployment_.taskId);
  ASSERT_EQ(previous.seriousId, deployment_.seriousId);
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&previous));
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  trigger_->ackedSchemaReqId = 1;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  ASSERT_NO_FATAL_FAILURE(CheckReleased());

  ASSERT_NO_FATAL_FAILURE(Submit(true, true));
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&previous));
  SStreamTask dropped = deployment_;
  ASSERT_NE(0, dropped.taskId);
  EXPECT_NE(previous.seriousId, dropped.seriousId);
  // Drop A's entire PATCH response. B retries, and acknowledges if a result was returned.
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  if (!success_) {
    // Another retirement on A can change its epoch while the replacement is queued.
    // The replacement must use the epoch from the heartbeat that actually takes it.
    ++readerHb_.readerDeployEpoch;
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  }
  const bool delivered = success_;
  if (delivered) {
    trigger_->ackedSchemaReqId = 2;
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  }
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&previous));
  SStreamTask installed = deployment_;
  ASSERT_EQ(dropped.taskId, installed.taskId);
  ASSERT_EQ(dropped.seriousId, installed.seriousId);
  bool retiredPrevious = false;
  for (const auto &cleanup : cleanups_) {
    EXPECT_EQ(readerHb_.snodeId, cleanup.task.nodeId);
    EXPECT_EQ(previous.taskId, cleanup.task.taskId);
    EXPECT_EQ(previous.seriousId, cleanup.task.seriousId);
    retiredPrevious |= cleanup.undeployMsg.doCleanup && !cleanup.undeployMsg.doCheckpoint;
  }
  EXPECT_TRUE(retiredPrevious);
  if (!delivered) {
    // This instance fails before B receives success. Its earlier INIT report can no longer confirm installation.
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed, STREAM_STATUS_INIT));
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed, STREAM_STATUS_FAILED));
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
    ASSERT_FALSE(success_);
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
    ASSERT_EQ(installed.taskId, deployment_.taskId);
    ASSERT_NE(installed.seriousId, deployment_.seriousId);
    SStreamTask replacement = deployment_;
    bool        retiredFailed = false;
    for (const auto &cleanup : cleanups_) {
      if (cleanup.task.taskId == installed.taskId && cleanup.task.seriousId == installed.seriousId) {
        retiredFailed = cleanup.undeployMsg.doCleanup && (cleanup.task.flags & STREAM_FLAG_REMOTE_READER_DEPLOY);
      }
    }
    EXPECT_TRUE(retiredFailed);
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
    ASSERT_FALSE(success_);
    installed = replacement;
  }
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
  if (!delivered) {
    EXPECT_NE(nullptr, owner_->pendingSchema);
    ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
    ASSERT_TRUE(success_);
    trigger_->ackedSchemaReqId = 2;
  }
  EXPECT_EQ(installed.taskId, replyTask_);
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(false));
  CheckReleased();

  // Remove the final binding. B receives the result, A keeps receiving cleanup until the retired Reader disappears.
  auto *req = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(request_->cont.pExtOrigTblReaders, 0));
  req->capabilities = STREAM_EXT_ORIGTBL_READER_CAP_PATCH | STREAM_EXT_ORIGTBL_READER_CAP_GUARDED_DEPLOY;
  taosArrayClear(req->bindings);
  request_->reqId = 3;
  ASSERT_NO_FATAL_FAILURE(TriggerHeartbeat(true));
  ASSERT_TRUE(success_);
  EXPECT_EQ(installed.taskId, replyTask_);
  for (int i = 0; i < 2; ++i) {
    ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat(&installed));
    EXPECT_EQ(0, deployment_.taskId);
    ASSERT_EQ(1u, cleanups_.size());
    EXPECT_EQ(readerHb_.snodeId, cleanups_[0].task.nodeId);
    EXPECT_EQ(installed.taskId, cleanups_[0].task.taskId);
    EXPECT_EQ(installed.seriousId, cleanups_[0].task.seriousId);
    EXPECT_TRUE(cleanups_[0].undeployMsg.doCleanup);
    EXPECT_FALSE(cleanups_[0].undeployMsg.doCheckpoint);
  }
  ASSERT_NO_FATAL_FAILURE(ReaderHeartbeat());
  EXPECT_TRUE(cleanups_.empty());
}

TEST_F(MndStreamDeployOwnershipTest, SchemaBatchFailurePreservesHealthyTriggerReport) {
  QueueDeploy(kStreamA);
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
  HandleActions();
  auto *a = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  auto *b = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_TRUE(a && a->triggerTask && b && b->triggerTask);
  SnodeLookupStubGuard snode;
  SStreamHbMsg         hb = {};
  hb.dnodeId = 1;
  hb.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 2);
  hb.pStreamReq = taosArrayInit_s(sizeof(int32_t), 1);
  auto *task = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, 0));
  auto *healthy = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, 1));
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(task, kStreamA, a->triggerTask->id));
  task->pMgmtReq->reqId = 21;
  auto *request = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(task->pMgmtReq->cont.pExtOrigTblReaders, 0));
  request->registryEntryId = 11;
  request->protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
  request->capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START;
  task->status = healthy->status = STREAM_STATUS_RUNNING;
  healthy->type = STREAM_TRIGGER_TASK;
  healthy->streamId = kStreamB;
  healthy->taskId = b->triggerTask->id.taskId;
  healthy->nodeId = b->triggerTask->id.nodeId;
  healthy->seriousId = b->triggerTask->id.seriousId;
  SStreamMgmtReq *retry = nullptr;
  ASSERT_EQ(0, tCloneSStreamMgmtReq(task->pMgmtReq, &retry));
  static std::thread::id   mainThread;
  static std::atomic<bool> failWorker{false};
  mainThread = std::this_thread::get_id();
  auto skipDeploy = [](const SStmStreamDeploy &) {};
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  stub_.set(
      taosArrayInit_s, +[](size_t elem, size_t size) -> SArray * {
        if (std::this_thread::get_id() != mainThread && failWorker.exchange(false)) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return nullptr;
        }
        SArray *result = taosArrayInit(size, elem);
        if (result) result->size = size;
        return result;
      });
  failWorker.store(true);
  ReadHeartbeatDeploys(hb, 0, skipDeploy, 101);
  msmCleanupSchemaWorker(&mnode_);
  stub_.reset(taosArrayInit_s);
  EXPECT_FALSE(failWorker.load());
  task->pMgmtReq = retry;
  healthy->pMgmtReq = static_cast<SStreamMgmtReq *>(taosMemoryCalloc(1, sizeof(SStreamMgmtReq)));
  ASSERT_NE(nullptr, healthy->pMgmtReq);
  healthy->pMgmtReq->type = STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER;
  healthy->pMgmtReq->reqId = 22;
  int32_t healthyIndex = 1;
  ASSERT_NE(nullptr, taosArrayPush(hb.pStreamReq, &healthyIndex));
  ReadHeartbeatDeploys(hb, 0, skipDeploy, 105, [&](const SMStreamHbRspMsg &response) {
    ASSERT_EQ(2u, taosArrayGetSize(response.rsps.rspList));
    auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(response.rsps.rspList, 0));
    EXPECT_EQ(21, reply->reqId);
    ASSERT_EQ(1u, taosArrayGetSize(reply->cont.pExtOrigTblReaders));
    auto *ext = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, ext->code);
    auto *other = static_cast<SStreamMgmtRsp *>(taosArrayGet(response.rsps.rspList, 1));
    ASSERT_NE(nullptr, other);
    EXPECT_EQ(22, other->reqId);
    EXPECT_EQ(b->triggerTask->id.taskId, other->task.taskId);
    EXPECT_EQ(0, other->code);
  });
  EXPECT_EQ(105, b->triggerTask->lastUpTs);
  EXPECT_EQ(STREAM_STATUS_RUNNING, b->triggerTask->status);
  EXPECT_EQ(nullptr, a->pendingSchema);
  EXPECT_EQ(0, atomic_load_32(&mnode_.streamSchemaBatchCount));
  tCleanupStreamHbMsg(&hb, true);
}

TEST_F(MndStreamDeployOwnershipTest, SchemaAdmissionDefersRequestsAndStopReleasesQueuedInput) {
  QueueDeploy(kStreamA);
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
  HandleActions();
  auto *a = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  auto *b = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_TRUE(a && a->triggerTask && b && b->triggerTask);
  SnodeLookupStubGuard snode;
  PrivateBuildPause    pause(false);
  PauseExternalReaderSchema(stub_);
  stub_.set(sdbReadLock, +[](SSdb *, int32_t) {});
  stub_.set(sdbUnLock, +[](SSdb *, int32_t) {});
  SStreamHbMsg hb = {};
  hb.dnodeId = 1;
  hb.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 1);
  hb.pStreamReq = taosArrayInit_s(sizeof(int32_t), 1);
  auto *task = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, 0));
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(task, kStreamA, a->triggerTask->id));
  auto addTables = [](SStreamMgmtReq *request, const std::string &condition) {
    auto                      *tables = request->cont.pExtOrigTblReaders;
    auto                      *first = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(tables, 0));
    SStreamExtOrigTblReaderReq cleanup = *first;
    cleanup.registryEntryId = 2;
    cleanup.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
    ASSERT_NE(nullptr, taosArrayPush(tables, &cleanup));
    for (size_t i = 0; !condition.empty() && i < tables->size; ++i) {
      auto *table = static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(tables, i));
      table->tagCondLen = static_cast<int32_t>(condition.size());
      table->tagCondJson = taosStrdup(condition.c_str());
      ASSERT_NE(nullptr, table->tagCondJson);
    }
  };
  ASSERT_NO_FATAL_FAILURE(addTables(task->pMgmtReq, ""));
  task->status = STREAM_STATUS_RUNNING;
  const int32_t original = tsNumOfMnodeStreamMgmtThreads;
  tsNumOfMnodeStreamMgmtThreads = 1;
  const int32_t init = msmInitSchemaWorker(&mnode_);
  tsNumOfMnodeStreamMgmtThreads = original;
  ASSERT_EQ(0, init);
  auto skipDeploy = [](const SStmStreamDeploy &) {};
  ReadHeartbeatDeploys(hb, 0, skipDeploy);
  EXPECT_TRUE(pause.Entered());
  const auto retained = atomic_load_64(&mnode_.streamSchemaBatchBytes);
  EXPECT_GT(retained, 0);
  EXPECT_EQ(1, atomic_load_32(&mnode_.streamSchemaBatchCount));
  InitExternalReaderRequest(task, kStreamB, b->triggerTask->id);
  const std::string condition = "\"" + std::string(STREAM_EXT_SOURCE_IDENTITY_MAX_TAG_COND_LEN - 2, 'x') + "\"";
  ASSERT_NO_FATAL_FAILURE(addTables(task->pMgmtReq, condition));
  const auto *retry = task->pMgmtReq;
  // Simulate capacity occupied by other batches without creating a large fixture.
  (void)atomic_add_fetch_32(&mnode_.streamSchemaBatchCount, 31);
  ReadHeartbeatDeploys(hb, 0, skipDeploy, 103, [](const SMStreamHbRspMsg &response) {
    EXPECT_EQ(0u, taosArrayGetSize(response.rsps.rspList));
  });
  EXPECT_EQ(retry, task->pMgmtReq);
  EXPECT_EQ(nullptr, b->pendingSchema);
  EXPECT_EQ(103, b->triggerTask->lastUpTs);
  (void)atomic_sub_fetch_32(&mnode_.streamSchemaBatchCount, 31);
  // One prepared table owns request/spec/replay copies, the cleanup-only table owns its request.
  const int64_t tagBytes = 4 * (static_cast<int64_t>(condition.size()) + 1);
  const int64_t occupied = INT64_C(256) * 1024 * 1024 - 2 * retained - tagBytes + 1;
  (void)atomic_add_fetch_64(&mnode_.streamSchemaBatchBytes, occupied);
  ReadHeartbeatDeploys(hb, 0, skipDeploy, 104, [](const SMStreamHbRspMsg &response) {
    EXPECT_EQ(0u, taosArrayGetSize(response.rsps.rspList));
  });
  EXPECT_EQ(retry, task->pMgmtReq);
  EXPECT_EQ(nullptr, b->pendingSchema);
  EXPECT_EQ(104, b->triggerTask->lastUpTs);
  (void)atomic_sub_fetch_64(&mnode_.streamSchemaBatchBytes, 1);
  ReadHeartbeatDeploys(hb, 0, skipDeploy, 105);
  EXPECT_NE(nullptr, b->pendingSchema);
  EXPECT_EQ(INT64_C(256) * 1024 * 1024, atomic_load_64(&mnode_.streamSchemaBatchBytes));
  (void)atomic_sub_fetch_64(&mnode_.streamSchemaBatchBytes, occupied - 1);
  const auto queued = atomic_load_64(&mnode_.streamSchemaBatchBytes);
  EXPECT_EQ(2 * retained + tagBytes, queued);
  msmUndeployStream(&mnode_, kStreamB, Stream(kStreamB)->name);
  EXPECT_EQ(nullptr, b->pendingSchema);
  EXPECT_LT(atomic_load_64(&mnode_.streamSchemaBatchBytes), queued);
  // A newer request queues behind A's in-flight provider call.
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(task, kStreamA, a->triggerTask->id));
  task->pMgmtReq->reqId = 2;
  ReadHeartbeatDeploys(hb, 0, skipDeploy, 106);
  EXPECT_NE(nullptr, a->pendingSchema);
  const auto beforeExpiry = atomic_load_64(&mnode_.streamSchemaBatchBytes);
  EXPECT_EQ(TSDB_CODE_GRANT_STREAM_EXPIRED, msmHandleGrantExpired(&mnode_, TSDB_CODE_GRANT_STREAM_EXPIRED));
  EXPECT_EQ(nullptr, a->pendingSchema);
  EXPECT_LT(atomic_load_64(&mnode_.streamSchemaBatchBytes), beforeExpiry);
  const auto version = atomic_load_64(&a->buildVersion);
  EXPECT_EQ(TSDB_CODE_GRANT_STREAM_EXPIRED, msmHandleGrantExpired(&mnode_, TSDB_CODE_GRANT_STREAM_EXPIRED));
  EXPECT_EQ(version, atomic_load_64(&a->buildVersion));
  pause.Release();
  msmCleanupSchemaWorker(&mnode_);
  EXPECT_EQ(0, atomic_load_32(&mnode_.streamSchemaBatchCount));
  EXPECT_EQ(0, atomic_load_64(&mnode_.streamSchemaBatchBytes));
  tCleanupStreamHbMsg(&hb, true);
}

TEST_F(MndStreamDeployOwnershipTest, MnodeStopSkipsQueuedSchemaBatchesAndRemainingTables) {
  CheckSchemaWorkerStop(true);
}

TEST_F(MndStreamDeployOwnershipTest, ErrorStopSkipsQueuedSchemaBatchesAndRemainingTables) {
  CheckSchemaWorkerStop(false);
}

void MndStreamDeployOwnershipTest::CheckSchemaWorkerStop(bool stopMnode) {
  QueueDeploy(kStreamA);
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
  HandleActions();
  auto *a = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  auto *b = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_TRUE(a && a->triggerTask && b && b->triggerTask);
  SnodeLookupStubGuard snode;
  PrivateBuildPause    pause(false);
  PauseExternalReaderSchema(stub_);
  stub_.set(sdbReadLock, +[](SSdb *, int32_t) {});
  stub_.set(sdbUnLock, +[](SSdb *, int32_t) {});
  static std::atomic<int> opens, fetches;
  opens.store(0);
  fetches.store(0);
  stub_.set(
      extConnectorOpen, +[](const SExtSourceCfg *, SExtConnectorHandle **) -> int32_t {
        ++opens;
        PrivateBuildPause::Pause();
        return 0;
      });
  stub_.set(
      extConnectorGetTableSchema, +[](SExtConnectorHandle *, const SExtTableNode *, SExtTableMeta **) -> int32_t {
        ++fetches;
        return TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
      });
  SStreamHbMsg hb = {};
  hb.dnodeId = 1;
  hb.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 1);
  hb.pStreamReq = taosArrayInit_s(sizeof(int32_t), 1);
  auto *task = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, 0));
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(task, kStreamA, a->triggerTask->id));
  task->status = STREAM_STATUS_RUNNING;
  auto *tables = task->pMgmtReq->cont.pExtOrigTblReaders;
  auto  second = *static_cast<SStreamExtOrigTblReaderReq *>(taosArrayGet(tables, 0));
  second.registryEntryId = 2;
  tstrncpy(second.identity.tableName, "other", sizeof(second.identity.tableName));
  ASSERT_NE(nullptr, taosArrayPush(tables, &second));
  const auto savedThreads = tsNumOfMnodeStreamMgmtThreads;
  tsNumOfMnodeStreamMgmtThreads = 1;
  const auto init = msmInitSchemaWorker(&mnode_);
  tsNumOfMnodeStreamMgmtThreads = savedThreads;
  ASSERT_EQ(0, init);
  auto skip = [](const SStmStreamDeploy &) {};
  ReadHeartbeatDeploys(hb, 0, skip);
  EXPECT_TRUE(pause.Entered());
  InitExternalReaderRequest(task, kStreamB, b->triggerTask->id);
  ReadHeartbeatDeploys(hb, 0, skip);
  EXPECT_NE(nullptr, b->pendingSchema);

  if (!stopMnode) {
    stub_.set(
        mstIsStreamDropped, +[](SMnode *, int64_t, bool *dropped) -> int32_t {
          *dropped = false;
          return TSDB_CODE_SUCCESS;
        });
    StreamFailureStubGuard report;
    const auto             reserved = atomic_load_64(&mnode_.streamSchemaBatchBytes);
    taosArrayClear(hb.pStreamReq);
    task->status = STREAM_STATUS_FAILED;
    task->errorCode = TSDB_CODE_MND_STREAM_TASK_LOST;
    ReadHeartbeatDeploys(hb, 0, skip);  // No requests: actionQLock is held for reading.
    EXPECT_EQ(1, b->stopped);
    EXPECT_EQ(nullptr, b->pendingSchema);
    EXPECT_EQ(1, atomic_load_32(&mnode_.streamSchemaBatchCount));
    EXPECT_LT(atomic_load_64(&mnode_.streamSchemaBatchBytes), reserved);

    int32_t index = 0;
    EXPECT_NE(nullptr, taosArrayPush(hb.pStreamReq, &index));
    InitExternalReaderRequest(task, kStreamB, b->triggerTask->id);
    task->status = STREAM_STATUS_RUNNING;
    ReadHeartbeatDeploys(hb, 0, skip, 102, [](const SMStreamHbRspMsg &response) {
      auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(response.rsps.rspList, 0));
      ASSERT_NE(nullptr, reply);
      auto *ext = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
      ASSERT_NE(nullptr, ext);
      EXPECT_EQ(TSDB_CODE_MND_STREAM_NOT_RUNNING, ext->code);
    });
    EXPECT_EQ(nullptr, b->pendingSchema);

    taosWLockLatch(&mStreamMgmt.runtimeLock);  // Health checks stop a stream by ID.
    msmStopStreamByError(kStreamA, nullptr, TSDB_CODE_MND_STREAM_TASK_LOST, 103);
    taosWUnLockLatch(&mStreamMgmt.runtimeLock);
    EXPECT_EQ(nullptr, a->pendingSchema);
    pause.Release();
    msmCleanupSchemaWorker(&mnode_);
    EXPECT_EQ(1, opens.load());
    EXPECT_EQ(0, fetches.load());
    EXPECT_EQ(0, atomic_load_32(&mnode_.streamSchemaBatchCount));
    EXPECT_EQ(0, atomic_load_64(&mnode_.streamSchemaBatchBytes));
    tCleanupStreamHbMsg(&hb, true);
    return;
  }

  stub_.set(syncLeaderTransfer, +[](int64_t) -> int32_t { return 0; });
  stub_.set(syncPreStop, +[](int64_t) {});
  stub_.set(sdbWriteFile, +[](SSdb *, int32_t) -> int32_t { return 0; });
  mndPreClose(&mnode_);
  pause.Release();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!taosQueueEmpty(mnode_.streamSchemaWorker.queue) && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  EXPECT_TRUE(taosQueueEmpty(mnode_.streamSchemaWorker.queue));
  EXPECT_EQ(1, opens.load());
  EXPECT_EQ(0, fetches.load());
  // Supersede B's outstanding request while the queue is still open.
  InitExternalReaderRequest(task, kStreamB, b->triggerTask->id);
  task->pMgmtReq->reqId = 2;
  ReadHeartbeatDeploys(hb, 0, skip, 102, [](const SMStreamHbRspMsg &response) {
    ASSERT_EQ(1u, taosArrayGetSize(response.rsps.rspList));
    auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(response.rsps.rspList, 0));
    auto *ext = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
    ASSERT_NE(nullptr, ext);
    EXPECT_EQ(TSDB_CODE_APP_IS_STOPPING, ext->code);
  });
  EXPECT_EQ(nullptr, b->pendingSchema);
  msmCleanupSchemaWorker(&mnode_);
  msmUndeployStream(&mnode_, kStreamA, Stream(kStreamA)->name);
  EXPECT_EQ(1, opens.load());
  EXPECT_EQ(0, fetches.load());
  EXPECT_EQ(0, atomic_load_64(&mnode_.streamSchemaBatchBytes));
  tCleanupStreamHbMsg(&hb, true);
}

TEST_F(MndStreamDeployOwnershipTest, ErrorStopReleasesPreparedAndPublishedSchema) {
  ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(false));
  QueueDeploy(kStreamA);
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
  HandleActions();
  auto *a = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  auto *b = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_TRUE(a && a->triggerTask && a->runners[0] && b && b->triggerTask);
  auto *runner = static_cast<SStmTaskStatus *>(taosArrayGet(a->runners[0], 0));
  ASSERT_NE(nullptr, runner);
  StreamFailureStubGuard report;
  stub_.set(mndAcquireExtSource, +[](SMnode *, const char *) -> SExtSourceObj * { return nullptr; });

  SStreamHbMsg hb = {};
  hb.dnodeId = 1;
  hb.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 2);
  hb.pStreamReq = taosArrayInit_s(sizeof(int32_t), 2);
  auto *triggerA = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, 0));
  auto *triggerB = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, 1));
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(triggerA, kStreamA, a->triggerTask->id));
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(triggerB, kStreamB, b->triggerTask->id));
  triggerA->status = triggerB->status = STREAM_STATUS_RUNNING;
  triggerA->ackedSchemaReqId = -1;
  *static_cast<int32_t *>(taosArrayGet(hb.pStreamReq, 1)) = 1;
  ASSERT_EQ(0, msmInitSchemaWorker(&mnode_));
  auto skip = [](const SStmStreamDeploy &) {};
  ReadHeartbeatDeploys(hb, 0, skip);
  msmCleanupSchemaWorker(&mnode_);
  ASSERT_EQ(2, atomic_load_32(&mnode_.streamSchemaBatchCount));

  // A retries and borrows its published reply for encoding. B still owns its prepared result.
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(triggerA, kStreamA, a->triggerTask->id));
  taosArrayPop(hb.pStreamReq);
  triggerB->status = STREAM_STATUS_FAILED;
  triggerB->errorCode = TSDB_CODE_MND_STREAM_TASK_LOST;
  SStmTaskStatusMsg failed = {};
  failed.streamId = kStreamA;
  failed.type = STREAM_RUNNER_TASK;
  failed.taskId = runner->id.taskId;
  failed.seriousId = runner->id.seriousId;
  failed.nodeId = runner->id.nodeId;
  failed.status = STREAM_STATUS_FAILED;
  failed.errorCode = TSDB_CODE_MND_STREAM_TASK_LOST;
  ASSERT_NE(nullptr, taosArrayPush(hb.pStreamStatus, &failed));
  ReadHeartbeatDeploys(hb, 0, skip, 102, [](const SMStreamHbRspMsg &response) {
    ASSERT_EQ(1u, taosArrayGetSize(response.rsps.rspList));
    auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(response.rsps.rspList, 0));
    auto *ext = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
    ASSERT_NE(nullptr, ext);
    EXPECT_EQ(TSDB_CODE_EXT_SOURCE_NOT_FOUND, ext->code);
  });
  EXPECT_EQ(1, a->stopped);
  EXPECT_EQ(1, b->stopped);
  EXPECT_EQ(nullptr, a->pendingSchema);
  EXPECT_EQ(nullptr, b->pendingSchema);
  EXPECT_EQ(0, atomic_load_32(&mnode_.streamSchemaBatchCount));
  EXPECT_EQ(0, atomic_load_64(&mnode_.streamSchemaBatchBytes));
  tCleanupStreamHbMsg(&hb, true);
}

TEST_F(MndStreamDeployOwnershipTest, StopAndStatusReadFinishDuringExternalSchemaFetch) {
  QueueDeploy(kStreamA);
  HandleActions();
  auto *owner = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, owner);
  ASSERT_NE(nullptr, owner->triggerTask);
  SStreamObj other = BuildMinimalStreamObj();
  ASSERT_NE(nullptr, other.pCreate);
  other.pCreate->streamId = kStreamB + 1;
  auto *block = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(nullptr, block);
  SnodeLookupStubGuard snode;
  PrivateBuildPause    pause(false);
  PauseExternalReaderSchema(stub_);
  stub_.set(sdbReadLock, +[](SSdb *, int32_t) {});
  stub_.set(sdbUnLock, +[](SSdb *, int32_t) {});
  SStmTaskStatusMsg task = {};
  ASSERT_NO_FATAL_FAILURE(InitExternalReaderRequest(&task, kStreamA, owner->triggerTask->id));
  SMStreamHbRspMsg response = {};
  SStmGrpCtx       ctx = {};
  ctx.pMnode = &mnode_;
  ctx.pRsp = &response;
  std::thread fetch([&] { EXPECT_EQ(0, handleHeartbeatTaskMgmtReq(&ctx, &task)); });
  EXPECT_TRUE(pause.Entered());
  EXPECT_EQ(0, mstPostStreamCreate(kStreamB, Stream(kStreamB)->name));
  HandleActions();
  auto *published = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  EXPECT_TRUE(published && published->triggerTask);
  SStreamHbMsg heartbeat = {};
  heartbeat.dnodeId = 1;
  heartbeat.streamGId = 1;
  ReadHeartbeatDeploys(heartbeat, 0, [](const SStmStreamDeploy &) {});
  auto *seen = static_cast<int64_t *>(taosHashGet(mStreamMgmt.dnodeMap, &heartbeat.dnodeId, sizeof(heartbeat.dnodeId)));
  EXPECT_EQ(101, seen ? *seen : 0);
  msmUndeployStream(&mnode_, kStreamB, Stream(kStreamB)->name);
  EXPECT_TRUE(published && published->stopped == 2);
  EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, &other, block, 0, 0));
  msmUndeployStream(&mnode_, kStreamA, Stream(kStreamA)->name);
  pause.Release();
  fetch.join();
  auto *reply = static_cast<SStreamMgmtRsp *>(taosArrayGet(response.rsps.rspList, 0));
  ASSERT_NE(nullptr, reply);
  auto *extReply = static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(reply->cont.pExtOrigTblReaders, 0));
  ASSERT_NE(nullptr, extReply);
  EXPECT_EQ(TSDB_CODE_MND_STREAM_NOT_RUNNING, extReply->code);
  tFreeSMStreamHbRspMsg(&response);
  blockDataDestroy(block);
  tFreeStreamObj(&other);
}
#endif

TEST_F(MndStreamDeployOwnershipTest, SameNameReplacementDuringBuildDiscardsOldTasks) {
  PrivateBuildPause pause;
  QueueDeploy(kStreamA);
  std::thread builder([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  auto *old = Stream(kStreamA);
  auto *replacement = Stream(kStreamB);
  taosWLockLatch(&mStreamMgmt.runtimeLock);
  atomic_store_8(&old->userDropped, 1);
  tstrncpy(replacement->name, old->name, sizeof(replacement->name));
  gRecoveryStreams[0] = replacement;
  taosWUnLockLatch(&mStreamMgmt.runtimeLock);
  pause.Release();
  builder.join();
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
  QueueDeploy(kStreamB);
  HandleActions();
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(1, mStreamMgmt.toDeploySnodeTaskNum);
}

TEST_F(MndStreamDeployOwnershipTest, StopDuringPublicationPreventsNextHeartbeatClaim) {
  auto *status = AddStatus(kStreamA);
  ASSERT_NE(nullptr, status);
  PrivateBuildPause pause(false);
  Stub              reset;
  reset.set(
      mstResetSStmStatus, +[](SStmStatus *) {
        PrivateBuildPause::Pause();  // Both the old fixture status and the transferred draft own no tasks here.
      });
  QueueDeploy(kStreamA);
  std::thread builder([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  static std::atomic<bool> waiting{false};
  waiting.store(false);
  std::atomic<bool> finished{false};
  Stub              wait;
  wait.set(
      mstWaitLock, +[](SRWLatch *lock, bool read) -> bool {
        if (lock == &mStreamMgmt.runtimeLock) waiting.store(true);
        if (read)
          taosRLockLatch(lock);
        else
          taosWLockLatch(lock);
        return true;
      });
  std::thread stop([&] {
    atomic_store_8(&Stream(kStreamA)->userStopped, 1);
    msmUndeployStream(&mnode_, kStreamA, Stream(kStreamA)->name);
    finished.store(true);
  });
  while (!waiting.load() && !finished.load()) std::this_thread::yield();
  EXPECT_TRUE(waiting.load());
  EXPECT_FALSE(finished.load());
  pause.Release();
  builder.join();
  stop.join();
  EXPECT_TRUE(finished.load());
  EXPECT_EQ(2, status->stopped);
  SStreamHbMsg req = {};
  req.snodeId = 1;
  SStmGrpCtx ctx = {};
  ctx.pReq = &req;
  ctx.deployStm = deployStm_;
  EXPECT_EQ(0, msmGrpAddDeploySnodeTasks(&ctx, nullptr));
  EXPECT_EQ(0, taosHashGetSize(deployStm_));
}

TEST_F(MndStreamDeployOwnershipTest, ReaderRepairDropsSupersededSerialAfterPrivateBuild) {
  auto *status = AddStatus(kStreamA);
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, AddStatus(kStreamB));
  status->pCreate->calcScanPlanList = taosArrayInit_s(sizeof(SStreamCalcScan), 1);
  auto *scan = static_cast<SStreamCalcScan *>(taosArrayGet(status->pCreate->calcScanPlanList, 0));
  ASSERT_NE(nullptr, scan);
  scan->scanPlan = taosStrdup("reader plan");
  ASSERT_NE(nullptr, scan->scanPlan);
  SStmTaskStatus reader = {};
  reader.pStream = status;
  reader.id.nodeId = 3;
  reader.id.taskId = 742;
  reader.id.seriousId = 1;
  reader.type = STREAM_READER_TASK;
  int64_t         key[] = {kStreamA, reader.id.taskId};
  SStmTaskStatus *ptr = &reader;
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.taskMap, key, sizeof(key), &ptr, sizeof(ptr)));
  SStmTaskAction repair = {};
  repair.type = reader.type;
  repair.id = reader.id;
  repair.streamId = kStreamA;
  auto *block = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(nullptr, block);
  SnodeLookupStubGuard snode;
  PrivateBuildPause    pause(false);
  static Stub         *guard;
  guard = &stub_;
  stub_.set(
      msmBuildReaderDeployInfo, +[](SStmTaskDeploy *deploy, void *plan, SStmStatus *input, bool trigger) -> int32_t {
        PrivateBuildPause::Pause();
        guard->reset(msmBuildReaderDeployInfo);
        return msmBuildReaderDeployInfo(deploy, plan, input, trigger);
      });
  mstPostTaskAction(&actionQueue_, &repair, STREAM_ACT_DEPLOY);
  std::thread worker([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, Stream(kStreamB), block, 0, 0));
  EXPECT_TRUE(HeartbeatTriggerIds(0).empty());
  taosWLockLatch(&mStreamMgmt.runtimeLock);
  repair.id.seriousId = mstBumpTaskSeriousId(&reader);
  mstPostTaskAction(&actionQueue_, &repair, STREAM_ACT_DEPLOY);
  taosWUnLockLatch(&mStreamMgmt.runtimeLock);
  pause.Release();
  worker.join();
  blockDataDestroy(block);

  EXPECT_EQ(0, status->stopped);
  ASSERT_EQ(1, mStreamMgmt.toDeployVgTaskNum);
  auto *queue = static_cast<SStmVgTasksToDeploy *>(
      taosHashGet(mStreamMgmt.toDeployVgMap, &reader.id.nodeId, sizeof(reader.id.nodeId)));
  ASSERT_NE(nullptr, queue);
  auto *entry = static_cast<SStmTaskToDeployExt *>(taosArrayGet(queue->taskList, 0));
  ASSERT_NE(nullptr, entry);
  EXPECT_EQ(repair.id.seriousId, entry->deploy.task.seriousId);
  EXPECT_STREQ("reader plan", static_cast<const char *>(entry->deploy.msg.reader.msg.calc.calcScanPlan));
}

TEST_F(MndStreamDeployOwnershipTest, ReaderTimeoutKeepsOneRepairUntilHeartbeatDispatch) {
  auto *create = Stream(kStreamA)->pCreate;
  create->calcScanPlanList = taosArrayInit_s(sizeof(SStreamCalcScan), 1);
  auto *scan = static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, 0));
  ASSERT_NE(nullptr, scan);
  const int32_t vgId = 3;
  scan->vgList = taosArrayInit(1, sizeof(vgId));
  ASSERT_NE(nullptr, taosArrayPush(scan->vgList, &vgId));
  SNode *raw = nullptr;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &raw));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> plan(raw, nodesDestroyNode);
  ASSERT_EQ(0, nodesNodeToString(raw, false, reinterpret_cast<char **>(&scan->scanPlan), nullptr));
  ASSERT_NE(nullptr, scan->scanPlan);

  SStmGrpCtx ctx = {};
  ctx.pMnode = &mnode_;
  ctx.currTs = 1;
  SStmTaskStatusMsg report = {};
  report.type = STREAM_TRIGGER_TASK;
  report.streamId = kStreamA;
  report.taskId = 100;
  report.seriousId = 10;
  report.nodeId = 1;
  report.status = STREAM_STATUS_RUNNING;
  ASSERT_EQ(0, msmWatchRecordNewTask(&ctx, &report));
  report.type = STREAM_READER_TASK;
  report.taskId = 101;
  report.nodeId = vgId;
  ASSERT_EQ(0, msmWatchRecordNewTask(&ctx, &report));
  SStreamHbMsg hb = {};
  hb.dnodeId = 1;
  hb.pVgLeaders = taosArrayInit(1, sizeof(report.nodeId));
  ASSERT_NE(nullptr, taosArrayPush(hb.pVgLeaders, &report.nodeId));
  const auto health = mStreamMgmt.hCtx;
  mStreamMgmt.hCtx.slotIdx = report.nodeId % MND_STREAM_ISOLATION_PERIOD_NUM;
  auto *vg = static_cast<SStmVgroupStatus *>(taosHashGet(mStreamMgmt.vgroupMap, &report.nodeId, sizeof(report.nodeId)));
  ASSERT_NE(nullptr, vg);
  vg->lastUpTs = ctx.currTs;
  int64_t              now = taosGetTimestampMs() - 4 * MST_ISOLATION_DURATION;
  std::vector<int64_t> delivered;
  auto                 heartbeat = [&] {
    ReadHeartbeatDeploys(
        hb, 0,
        [&](const SStmStreamDeploy &stream) {
          for (size_t i = 0; i < taosArrayGetSize(stream.readerTasks); ++i) {
            auto *entry = static_cast<SStmTaskDeploy *>(taosArrayGet(stream.readerTasks, i));
            EXPECT_EQ(report.taskId, entry->task.taskId);
            delivered.push_back(entry->task.seriousId);
          }
        },
        now);
    EXPECT_EQ(now, vg->lastUpTs);
  };
  auto check = [&] {
    mStreamMgmt.hCtx.currentTs = now;
    msmCheckVgroupStatus(&mnode_);
  };
  heartbeat();
  stub_.set(
      mstPostTaskAction, +[](SStmActionQ *, SStmTaskAction *, int32_t) -> int32_t { return TSDB_CODE_OUT_OF_MEMORY; });
  check();
  EXPECT_EQ(0, actionQueue_.qRemainNum);
  stub_.reset(mstPostTaskAction);
  check();
  check();
  EXPECT_EQ(1, actionQueue_.qRemainNum);
  PrivateBuildPause pause(false);
  static Stub      *guard;
  guard = &stub_;
  stub_.set(
      msmBuildReaderDeployInfo, +[](SStmTaskDeploy *deploy, void *plan, SStmStatus *input, bool trigger) -> int32_t {
        PrivateBuildPause::Pause();
        guard->reset(msmBuildReaderDeployInfo);
        return msmBuildReaderDeployInfo(deploy, plan, input, trigger);
      });
  std::thread worker([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  for (int i = 0; i < 3; ++i) {
    now += MST_ISOLATION_DURATION;
    heartbeat();
    check();
  }
  EXPECT_EQ(0, actionQueue_.qRemainNum);
  EXPECT_TRUE(delivered.empty());
  pause.Release();
  worker.join();
  check();
  EXPECT_EQ(0, actionQueue_.qRemainNum);
  const int64_t beforeDispatch = taosGetTimestampMs();
  heartbeat();
  const int64_t afterDispatch = taosGetTimestampMs();
  EXPECT_EQ(std::vector<int64_t>{report.seriousId + 1}, delivered);
  now = beforeDispatch + MST_ISOLATION_DURATION - 1;
  heartbeat();
  check();
  EXPECT_EQ(0, actionQueue_.qRemainNum);
  now = afterDispatch + MST_ISOLATION_DURATION;
  heartbeat();
  check();
  EXPECT_EQ(1, actionQueue_.qRemainNum);
  HandleActions();
  heartbeat();
  EXPECT_EQ((std::vector<int64_t>{report.seriousId + 1, report.seriousId + 2}), delivered);
  taosArrayDestroy(hb.pVgLeaders);
  mStreamMgmt.hCtx = health;
}

TEST_F(MndStreamDeployOwnershipTest, StaleReaderRepairCannotStopNewDeployment) {
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  SStmTaskStatus reader = {};
  reader.pStream = status;
  reader.type = STREAM_READER_TASK;
  reader.id.taskId = 742;
  SStmTaskStatus   *oldReader = &reader;
  PrivateBuildPause pause;
  QueueDeploy(kStreamA);
  std::thread builder([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  auto saved = mStreamMgmt.hCtx;
  mStreamMgmt.hCtx.currentTs = taosGetTimestampMs();
  msmCheckTaskListStatus(kStreamA, &oldReader, 1);
  EXPECT_EQ(1u, actionQueue_.qRemainNum);
  auto oldGeneration = status->triggerTask->id.taskId;
  pause.Release();
  builder.join();
  EXPECT_NE(oldGeneration, status->triggerTask->id.taskId);
  EXPECT_EQ(0, status->stopped);
  SStmTaskStatus missing = {};
  missing.type = STREAM_READER_TASK;
  missing.pStream = status;
  missing.id.taskId = 743;
  SStmTaskStatus *missingReader = &missing;
  msmCheckTaskListStatus(kStreamA, &missingReader, 1);  // Same generation, missing ID still fails.
  HandleActions();
  EXPECT_EQ(1, status->stopped);
  EXPECT_EQ(TSDB_CODE_STREAM_INTERNAL_ERROR, status->fatalError);
  mStreamMgmt.hCtx = saved;
}

TEST_F(MndStreamDeployOwnershipTest, RetryIsClaimedOnceAcrossSuccessAndBackoff) {
  auto *status = AddStatus(kStreamA);
  ASSERT_NE(nullptr, status);
  stub_.set(sdbCheckExists, reportStreamExists);
  const auto saved = mStreamMgmt.hCtx;
  status->stopped = 1;
  status->fatalRetryTs = 1;
  mStreamMgmt.hCtx.currentTs = taosGetTimestampMs();
  msmCheckLoopStreamMap(&mnode_);
  ASSERT_EQ(1u, actionQueue_.qRemainNum);
  status->fatalRetryTs = taosGetTimestampMs() + MST_ISOLATION_DURATION;
  HandleActions();
  EXPECT_EQ(0, status->deployTimes);
  EXPECT_FALSE(status->retryQueued);
  status->fatalRetryTs = 1;
  msmCheckLoopStreamMap(&mnode_);
  ASSERT_EQ(1u, actionQueue_.qRemainNum);
  ++status->buildVersion;
  HandleActions();
  EXPECT_EQ(0, status->deployTimes);
  EXPECT_FALSE(status->retryQueued);
  for (bool fail : {false, true}) {
    Stub failRestore;
    if (fail)
      failRestore.set(
          mndStreamRecalcPrepareRestore, +[](const SStreamObj *, SStreamRecalcRestore *, int64_t, int64_t) -> int32_t {
            return TSDB_CODE_OUT_OF_MEMORY;
          });
    status->stopped = 1;
    status->fatalRetryTs = 1;
    mStreamMgmt.hCtx.currentTs = taosGetTimestampMs();
    msmCheckLoopStreamMap(&mnode_);
    msmCheckLoopStreamMap(&mnode_);
    EXPECT_EQ(1u, actionQueue_.qRemainNum);
    PrivateBuildPause pause;
    auto              attempts = status->deployTimes;
    std::thread       worker([&] { HandleActions(); });
    EXPECT_TRUE(pause.Entered());
    msmCheckLoopStreamMap(&mnode_);
    EXPECT_EQ(0u, actionQueue_.qRemainNum);
    pause.Release();
    worker.join();
    EXPECT_EQ(attempts + 1, status->deployTimes);
    EXPECT_EQ(fail ? 1 : 0, status->stopped);
    if (fail) EXPECT_GT(status->fatalRetryTs, mStreamMgmt.hCtx.currentTs);
    msmCheckLoopStreamMap(&mnode_);
    EXPECT_EQ(0u, actionQueue_.qRemainNum);
  }
  mStreamMgmt.hCtx = saved;
}

TEST_F(MndStreamDeployOwnershipTest, ClaimedStartSurvivesRuntimeMapCleanup) {
  auto *status = AddStatus(kStreamA);
  ASSERT_NE(nullptr, status);
  status->stopped = 2;
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamA, Stream(kStreamA)->name, nullptr, true, STREAM_ACT_DEPLOY));
  PrivateBuildPause pause;
  std::thread       worker([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  msmCheckLoopStreamMap(&mnode_);
  EXPECT_EQ(status, taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  pause.Release();
  worker.join();
  ASSERT_EQ(status, taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  EXPECT_EQ(0, status->stopped);
  EXPECT_EQ(1, status->deployTimes);
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.taskMap));
}

TEST_F(MndStreamDeployOwnershipTest, StatusQueryFinishesDuringFirstPlanCopy) {
  ASSERT_NE(nullptr, AddStatus(kStreamB));
  auto *block = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(nullptr, block);
  SnodeLookupStubGuard snode;
  PrivateBuildPause    pause(false);
  static Stub         *clone;
  clone = &stub_;
  stub_.set(
      tCloneStreamCreateDeployPointers, +[](SCMCreateStreamReq *from, SCMCreateStreamReq **to) -> int32_t {
        PrivateBuildPause::Pause();
        clone->reset(tCloneStreamCreateDeployPointers);
        return tCloneStreamCreateDeployPointers(from, to);
      });
  QueueDeploy(kStreamA);
  std::thread worker([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, Stream(kStreamB), block, 0, 0));
  pause.Release();
  worker.join();
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.taskMap));
  blockDataDestroy(block);
}

class MndStreamMetadataOrderTest : public MndStreamDeployOwnershipTest,
                                   public ::testing::WithParamInterface<int> {};

TEST_P(MndStreamMetadataOrderTest, RemovedMetadataPartitionsRecoverAndRepairRunner) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan(GetParam() == 0 ? 3 : 1, GetParam() != 0, GetParam() == 2));
  SetMetadataCatalog({20}, 1000);  // A TSMA-only vgroup is intentionally absent from the persisted scan.
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  ASSERT_EQ(1, listNEles(status->calcReaders));
  EXPECT_EQ(1, status->calcReaderNum);
  auto reader = reinterpret_cast<SStmTaskStatus *>(listHead(status->calcReaders)->data)->id;
  EXPECT_EQ(20, reader.nodeId);
  EXPECT_EQ(GetParam() == 0 ? 1 : 0, reader.taskIdx);
  auto verifyRunner = [&](bool repaired) {
    int runners = 0;
    SStreamHbMsg hb = {};
    hb.dnodeId = hb.snodeId = 1;
    ReadHeartbeatDeploys(hb, 0, [&](const SStmStreamDeploy &stream) {
      for (size_t i = 0; i < taosArrayGetSize(stream.runnerTasks); ++i) {
        auto *task = static_cast<SStmTaskDeploy *>(taosArrayGet(stream.runnerTasks, i));
        EXPECT_EQ(repaired, !!STREAM_IS_REDEPLOY_RUNNER(task->task.flags));
        SNode *raw = nullptr;
        ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(task->msg.runner.pPlan), &raw));
        std::unique_ptr<SNode, decltype(&nodesDestroyNode)> owner(raw, nodesDestroyNode);
        ASSERT_NE(nullptr, raw);
        ASSERT_EQ(QUERY_NODE_PHYSICAL_SUBPLAN, nodeType(raw));
        auto *plan = reinterpret_cast<SSubplan *>(raw);
        ASSERT_NE(nullptr, plan->pNode);
        ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, nodeType(plan->pNode));
        auto *exchange = reinterpret_cast<SExchangePhysiNode *>(plan->pNode);
        ASSERT_EQ(1, LIST_LENGTH(exchange->pSrcEndPoints));
        auto *source = reinterpret_cast<SDownstreamSourceNode *>(nodesListGetNode(exchange->pSrcEndPoints, 0));
        EXPECT_EQ(reader.taskId, source->taskId);
        EXPECT_EQ(20, source->addr.nodeId);
        ++runners;
      }
    });
    EXPECT_GT(runners, 0);
  };
  ASSERT_NO_FATAL_FAILURE(verifyRunner(false));
  SStmTaskAction repair = {};
  repair.streamId = kStreamA;
  repair.generation = status->triggerTask->id.taskId;
  repair.type = STREAM_RUNNER_TASK;
  repair.multiRunner = true;
  repair.deployNum = 1;
  ASSERT_EQ(0, mstPostTaskAction(&actionQueue_, &repair, STREAM_ACT_DEPLOY));
  HandleActions();
  ASSERT_NO_FATAL_FAILURE(verifyRunner(true));
  EXPECT_EQ(reader.taskId, reinterpret_cast<SStmTaskStatus *>(listHead(status->calcReaders)->data)->id.taskId);
}

INSTANTIATE_TEST_SUITE_P(PartitionOrder, MndStreamMetadataOrderTest, ::testing::Values(0, 1, 2));

TEST_F(MndStreamDeployOwnershipTest, HealthyMetadataDoesNotProbeCatalogAgain) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  static std::map<int32_t, int> lookups;
  lookups.clear();
  stub_.set(mndGetVgroupEpsetByIdWithStatus, +[](SMnode *, int32_t vg, int32_t *lookupCode) -> SEpSet {
    ++lookups[vg];
    *lookupCode = 0;
    SEpSet ep = {};
    ep.numOfEps = 1;
    ep.eps[0].port = 6000 + vg;
    tstrncpy(ep.eps[0].fqdn, "node1", sizeof(ep.eps[0].fqdn));
    return ep;
  });
  static int probes;
  probes = 0;
  stub_.set(sdbTraverse, +[](SSdb *, ESdbType, sdbTraverseFp, void *, void *, void *) { ++probes; });
  stub_.set(mndAcquireVgroup, +[](SMnode *, int32_t) -> SVgObj * {
    ++probes;
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  });
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  EXPECT_EQ(3, listNEles(status->calcReaders));
  EXPECT_EQ((std::map<int32_t, int>{{20, 1}, {21, 1}, {23, 1}}), lookups);
  EXPECT_EQ(0, probes);
}

TEST_F(MndStreamDeployOwnershipTest, WatchRedeploysLegacySubqueryMetadataAfterRemovedPartitions) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan(3, false, false, true));
  auto *create = Stream(kStreamA)->pCreate;
  create->triggerTblType = TSDB_NORMAL_TABLE;
  create->triggerTblVgId = 20;
  create->triggerTblName = taosStrdup("meters");
  ASSERT_NE(nullptr, create->triggerTblName);
  EXPECT_FALSE(STREAM_IS_VIRTUAL_TABLE(create->triggerTblType, create->flags));
  EXPECT_FALSE(create->vtableCalc);
  auto *firstScan = static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, 0));
  SNode *firstPlan = nullptr;
  ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(firstScan->scanPlan), &firstPlan));
  ASSERT_NE(nullptr, firstPlan);
  EXPECT_EQ(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, nodeType(reinterpret_cast<SSubplan *>(firstPlan)->pNode));
  nodesDestroyNode(firstPlan);
  SNode *calcPlan = nullptr;
  ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(create->calcPlan), &calcPlan));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> calcPlanOwner(calcPlan, nodesDestroyNode);
  ASSERT_NE(nullptr, calcPlan);
  auto *root = reinterpret_cast<SQueryPlan *>(calcPlan);
  ASSERT_NE(nullptr, root->pChildren);
  auto *subquery = reinterpret_cast<SQueryPlan *>(nodesListGetNode(root->pChildren, 0));
  auto *subqueryLevel = reinterpret_cast<SNodeListNode *>(nodesListGetNode(subquery->pSubplans, 0));
  auto *subqueryRunner = reinterpret_cast<SSubplan *>(nodesListGetNode(subqueryLevel->pNodeList, 0));
  EXPECT_EQ(3, LIST_LENGTH(subqueryRunner->pChildren));
  calcPlanOwner.reset();
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  ASSERT_EQ(2, status->calcReaderNum);
  ASSERT_EQ(4u, taosArrayGetSize(create->calcScanPlanList));
  const auto previous = status->triggerTask->id;

  // A new leader starts WATCH with no in-memory deployment state.
  void *node = nullptr;
  while ((node = taosHashIterate(mStreamMgmt.snodeMap, node))) {
    auto *snode = static_cast<SStmSnodeStatus *>(node);
    ASSERT_EQ(0, taosHashRemove(snode->streamTasks, &kStreamA, sizeof(kStreamA)));
  }
  while ((node = taosHashIterate(mStreamMgmt.vgroupMap, node))) {
    auto *vgroup = static_cast<SStmVgroupStatus *>(node);
    ASSERT_EQ(0, taosHashRemove(vgroup->streamTasks, &kStreamA, sizeof(kStreamA)));
  }
  taosHashClear(mStreamMgmt.taskMap);
  mstDestroySStmStatus(status);
  taosHashClear(mStreamMgmt.streamMap);
  taosHashClear(mStreamMgmt.toDeploySnodeMap);
  taosHashClear(mStreamMgmt.toDeployVgMap);
  mStreamMgmt.toDeploySnodeTaskNum = mStreamMgmt.toDeployVgTaskNum = 0;
  mStreamMgmt.state = MND_STM_STATE_WATCH;
  const auto savedHealth = mStreamMgmt.hCtx;
  SStmLastTs savedTimes[STM_EVENT_MAX_VALUE];
  memcpy(savedTimes, mStreamMgmt.lastTs, sizeof(savedTimes));
  SStreamHbMsg hb = {};
  hb.dnodeId = hb.snodeId = 1;
  hb.pStreamStatus = taosArrayInit(3, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(nullptr, hb.pStreamStatus);
  SStmTaskStatusMsg report = {};
  report.streamId = kStreamA;
  report.taskId = previous.taskId;
  report.seriousId = previous.seriousId;
  report.nodeId = 1;
  report.status = STREAM_STATUS_RUNNING;
  for (EStreamTaskType type : {STREAM_TRIGGER_TASK, STREAM_READER_TASK, STREAM_RUNNER_TASK}) {
    report.type = type;
    ASSERT_NE(nullptr, taosArrayPush(hb.pStreamStatus, &report));
  }
  static int acquisitions;
  acquisitions = 0;
  stub_.set(mndAcquireStreamById, +[](SMnode *mnode, int64_t id, SStreamObj **stream) -> int32_t {
    ++acquisitions;
    return returnRecoveryStreamById(mnode, id, stream);
  });
  const int64_t begin = MND_STREAM_GET_LAST_TS(STM_EVENT_ACTIVE_BEGIN);
  ReadHeartbeatDeploys(hb, 0, [](const SStmStreamDeploy &) {}, begin + 1);
  EXPECT_EQ(MND_STM_STATE_WATCH, mStreamMgmt.state);
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.streamMap));
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(1, acquisitions);
  // A later packet must reuse the deferral but still advance the task-ID watermark.
  auto *last = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, 2));
  last->taskId = previous.taskId + 1000;
  ReadHeartbeatDeploys(hb, 0, [](const SStmStreamDeploy &) {}, begin + 2);
  EXPECT_EQ(MND_STM_STATE_WATCH, mStreamMgmt.state);
  EXPECT_EQ(previous.taskId + 1001, mStreamMgmt.lastTaskId);
  EXPECT_EQ(1, acquisitions);
  ReadHeartbeatDeploys(hb, 0, [](const SStmStreamDeploy &) {}, begin + MST_SHORT_ISOLATION_DURATION + 1);
  taosArrayDestroy(hb.pStreamStatus);
  EXPECT_EQ(1, acquisitions);
  EXPECT_GT(mStreamMgmt.lastTaskId, previous.taskId + 1000);
  EXPECT_EQ(MND_STM_STATE_NORMAL, mStreamMgmt.state);
  EXPECT_EQ(nullptr, mStreamMgmt.watch.deferredStreams);

  // The production health scan, rather than a manually queued deploy, must recover the missing stream.
  stub_.set(sdbFetch, +[](SSdb *, ESdbType type, void *iter, void **row) -> void * {
    static SVgObj live = {};
    if (iter) return nullptr;
    if (type == SDB_STREAM) return *row = gRecoveryStreams[0];
    if (type == SDB_VGROUP) {
      live.vgId = 20;
      return *row = &live;
    }
    return nullptr;
  });
  stub_.set(sdbCheckExists, reportStreamExists);
  mStreamMgmt.hCtx.currentTs = taosGetTimestampMs() + MST_ISOLATION_DURATION;
  msmCheckStreamsStatus(&mnode_);
  EXPECT_EQ(1u, actionQueue_.qRemainNum);
  HandleActions();
  memcpy(mStreamMgmt.lastTs, savedTimes, sizeof(savedTimes));
  mStreamMgmt.hCtx = savedHealth;
  status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  EXPECT_NE(previous.taskId, status->triggerTask->id.taskId);
  EXPECT_EQ(0, status->fatalError);
  EXPECT_EQ(0, status->stopped);
  EXPECT_EQ(2, status->calcReaderNum);
  ASSERT_NE(nullptr, status->calcReaders);
  ASSERT_EQ(2, listNEles(status->calcReaders));
  auto *ordinaryReader = reinterpret_cast<SStmTaskStatus *>(listHead(status->calcReaders)->data);
  auto *metadataReader = reinterpret_cast<SStmTaskStatus *>(TD_DLIST_NODE_NEXT(listHead(status->calcReaders))->data);
  EXPECT_EQ(0, ordinaryReader->id.taskIdx);
  EXPECT_EQ(2, metadataReader->id.taskIdx);
  EXPECT_EQ(20, metadataReader->id.nodeId);
  EXPECT_EQ(4u, taosArrayGetSize(create->calcScanPlanList));
  const int32_t snodeId = 1, vgId = 20;
  auto *snode = static_cast<SStmSnodeStatus *>(taosHashGet(mStreamMgmt.snodeMap, &snodeId, sizeof(snodeId)));
  ASSERT_NE(nullptr, snode);
  auto *snodeStream = static_cast<SStmSnodeStreamStatus *>(taosHashGet(snode->streamTasks, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, snodeStream);
  EXPECT_EQ(status->triggerTask, snodeStream->trigger);
  std::set<SStmTaskStatus *> expectedRunners;
  for (auto *tasks : status->runners) {
    for (size_t i = 0; i < taosArrayGetSize(tasks); ++i) {
      auto *task = static_cast<SStmTaskStatus *>(taosArrayGet(tasks, i));
      if (task->id.nodeId == snodeId) expectedRunners.insert(task);
    }
  }
  size_t runners = 0;
  for (auto *tasks : snodeStream->runners) {
    for (size_t i = 0; i < taosArrayGetSize(tasks); ++i) {
      auto *task = static_cast<SStmTaskStatus *>(taosArrayGetP(tasks, i));
      EXPECT_EQ(1u, expectedRunners.erase(task));
      ++runners;
    }
  }
  EXPECT_TRUE(expectedRunners.empty());
  EXPECT_GT(runners, 0u);
  auto *vgroup = static_cast<SStmVgroupStatus *>(taosHashGet(mStreamMgmt.vgroupMap, &vgId, sizeof(vgId)));
  ASSERT_NE(nullptr, vgroup);
  auto *vgroupStream = static_cast<SStmVgStreamStatus *>(taosHashGet(vgroup->streamTasks, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, vgroupStream);
  ASSERT_EQ(2u, taosArrayGetSize(vgroupStream->calcReaders));
  EXPECT_EQ(ordinaryReader, taosArrayGetP(vgroupStream->calcReaders, 0));
  EXPECT_EQ(metadataReader, taosArrayGetP(vgroupStream->calcReaders, 1));
  SStreamHbMsg deployHb = {};
  deployHb.dnodeId = deployHb.snodeId = 1;
  int checkedMainRunners = 0;
  int checkedSubqueryRunners = 0;
  ReadHeartbeatDeploys(deployHb, 0, [&](const SStmStreamDeploy &deployment) {
    for (size_t i = 0; i < taosArrayGetSize(deployment.runnerTasks); ++i) {
      auto *task = static_cast<SStmTaskDeploy *>(taosArrayGet(deployment.runnerTasks, i));
      SNode *decoded = nullptr;
      ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(task->msg.runner.pPlan), &decoded));
      std::unique_ptr<SNode, decltype(&nodesDestroyNode)> planOwner(decoded, nodesDestroyNode);
      ASSERT_NE(nullptr, decoded);
      ASSERT_EQ(QUERY_NODE_PHYSICAL_SUBPLAN, nodeType(decoded));
      auto *plan = reinterpret_cast<SSubplan *>(decoded);
      ASSERT_NE(nullptr, plan->pNode);
      ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, nodeType(plan->pNode));
      auto *exchange = reinterpret_cast<SExchangePhysiNode *>(plan->pNode);
      ASSERT_EQ(1, LIST_LENGTH(exchange->pSrcEndPoints));
      auto *source = reinterpret_cast<SDownstreamSourceNode *>(nodesListGetNode(exchange->pSrcEndPoints, 0));
      if (plan->id.groupId == 1) {
        EXPECT_EQ(ordinaryReader->id.taskId, source->taskId);
        ++checkedMainRunners;
      } else {
        EXPECT_EQ(2, plan->id.groupId);
        EXPECT_EQ(metadataReader->id.taskId, source->taskId);
        ++checkedSubqueryRunners;
      }
      EXPECT_EQ(vgId, source->addr.nodeId);
    }
  });
  EXPECT_GT(checkedMainRunners, 0);
  EXPECT_GT(checkedSubqueryRunners, 0);
}

TEST_F(MndStreamDeployOwnershipTest, WatchDefersUndecodableStreamAndKeepsOtherStreamAcrossHeartbeats) {
  ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(true));
  static int decodes;
  static int acquisitions;
  decodes = acquisitions = 0;
  stub_.set(nodesStringToNode, +[](const char *, SNode **) -> int32_t {
    ++decodes;
    return TSDB_CODE_OUT_OF_MEMORY;
  });
  stub_.set(mndAcquireStreamById, +[](SMnode *mnode, int64_t id, SStreamObj **stream) -> int32_t {
    ++acquisitions;
    return returnRecoveryStreamById(mnode, id, stream);
  });
  SStreamHbMsg hb = {};
  hb.dnodeId = hb.snodeId = 1;
  hb.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 2);
  ASSERT_NE(nullptr, hb.pStreamStatus);
  for (int i = 0; i < 2; ++i) {
    auto *task = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, i));
    task->type = STREAM_TRIGGER_TASK;
    task->streamId = i == 0 ? kStreamA : kStreamB;
    task->taskId = 201 + i;
    task->seriousId = 1;
    task->nodeId = 1;
    task->status = STREAM_STATUS_RUNNING;
  }
  const auto normalBegin = mStreamMgmt.lastTs[STM_EVENT_NORMAL_BEGIN];
  mStreamMgmt.state = MND_STM_STATE_WATCH;
  const int64_t begin = MND_STREAM_GET_LAST_TS(STM_EVENT_ACTIVE_BEGIN);
  for (int i = 1; i <= 2; ++i) {
    ReadHeartbeatDeploys(hb, 0, [](const SStmStreamDeploy &) {}, begin + i);
    EXPECT_EQ(MND_STM_STATE_WATCH, mStreamMgmt.state);
    EXPECT_EQ(nullptr, taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
    auto *b = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
    EXPECT_TRUE(b && b->triggerTask && b->triggerTask->id.taskId == 202 && b->triggerTask->lastUpTs == begin + i);
    EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.taskMap));
    EXPECT_EQ(1, decodes);
    EXPECT_EQ(2, acquisitions);
  }
  ReadHeartbeatDeploys(hb, 0, [](const SStmStreamDeploy &) {}, begin + MST_SHORT_ISOLATION_DURATION + 1);
  mStreamMgmt.lastTs[STM_EVENT_NORMAL_BEGIN] = normalBegin;
  taosArrayDestroy(hb.pStreamStatus);
  EXPECT_EQ(MND_STM_STATE_NORMAL, mStreamMgmt.state);
  EXPECT_EQ(nullptr, mStreamMgmt.watch.deferredStreams);
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.taskMap));
  auto *healthy = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_NE(nullptr, healthy);
  EXPECT_EQ(0, healthy->stopped);
  EXPECT_EQ(0, healthy->fatalError);
  ASSERT_NE(nullptr, healthy->triggerTask);
  EXPECT_EQ(202, healthy->triggerTask->id.taskId);
  EXPECT_EQ(1, decodes);
  EXPECT_EQ(2, acquisitions);
}

// Replay actual deployed task identities through the full leader-adoption lifecycle.
void MndStreamDeployOwnershipTest::CheckWatchDeployedScans(bool cache, bool empty, bool missingReader) {
  ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(false));
  auto *create = Stream(kStreamA)->pCreate;
  create->calcScanPlanList = taosArrayInit(2, sizeof(SStreamCalcScan));
  ASSERT_NE(nullptr, create->calcScanPlanList);
  auto *scan = static_cast<SStreamCalcScan *>(taosArrayReserve(create->calcScanPlanList, 1));
  ASSERT_NE(nullptr, scan);
  SNode *raw = nullptr;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &raw));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> plan(raw, nodesDestroyNode);
  auto *subplan = reinterpret_cast<SSubplan *>(raw);
  subplan->subplanType = SUBPLAN_TYPE_SCAN;
  subplan->id.groupId = 3;
  subplan->id.subplanId = 13;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, reinterpret_cast<SNode **>(&subplan->pNode)));
  ASSERT_EQ(0, nodesNodeToString(raw, false, reinterpret_cast<char **>(&scan->scanPlan), nullptr));
  ASSERT_NE(nullptr, scan->scanPlan);
  scan->vgList = taosArrayInit(2, sizeof(int32_t));
  ASSERT_NE(nullptr, scan->vgList);
  int32_t first = 20;
  ASSERT_NE(nullptr, taosArrayPush(scan->vgList, &first));
  stub_.set(mndGetVgroupEpsetById, +[](SMnode *, int32_t vg) -> SEpSet {
    SEpSet ep = {};
    ep.numOfEps = 1;
    ep.eps[0].port = 6000 + vg;
    tstrncpy(ep.eps[0].fqdn, "node1", sizeof(ep.eps[0].fqdn));
    return ep;
  });
  stub_.set(mndGetVgroupEpsetByIdWithStatus, +[](SMnode *mnode, int32_t vg, int32_t *code) -> SEpSet {
    *code = 0;
    return mndGetVgroupEpsetById(mnode, vg);
  });
  scan->readFromCache = cache;
  if (!cache) {
    int32_t second = 21;
    ASSERT_NE(nullptr, taosArrayPush(scan->vgList, &second));
  }
  if (empty) {
    SStreamCalcScan zero = {};
    zero.scanPlan = taosStrdup(static_cast<const char *>(scan->scanPlan));
    ASSERT_NE(nullptr, zero.scanPlan);
    // Both persisted decoders represent a zero-partition scan with NULL.
    ASSERT_NE(nullptr, taosArrayPush(create->calcScanPlanList, &zero));
  }
  QueueDeploy(kStreamA);
  HandleActions();
  auto *deployed = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, deployed);
  ASSERT_NE(nullptr, deployed->triggerTask);
  ASSERT_EQ(cache ? 0 : 2, deployed->calcReaderNum);
  ASSERT_GT(deployed->runnerNum, 0);

  std::unique_ptr<SArray, decltype(&taosArrayDestroy)> reports(
      taosArrayInit(8, sizeof(SStmTaskStatusMsg)), taosArrayDestroy);
  ASSERT_NE(nullptr, reports);
  bool omitted = false;
  void *entry = nullptr;
  while ((entry = taosHashIterate(mStreamMgmt.taskMap, entry))) {
    auto *task = *static_cast<SStmTaskStatus **>(entry);
    if (missingReader && !omitted && task->type == STREAM_READER_TASK && !STREAM_IS_TRIGGER_READER(task->flags)) {
      omitted = true;
      continue;
    }
    SStmTaskStatusMsg report = {};
    memcpy(&report.streamId, taosHashGetKey(entry, nullptr), sizeof(report.streamId));
    report.taskId = task->id.taskId;
    report.seriousId = task->id.seriousId;
    report.deployId = task->id.deployId;
    report.nodeId = task->id.nodeId;
    report.taskIdx = task->id.taskIdx;
    report.type = task->type;
    report.flags = task->flags;
    report.status = STREAM_STATUS_RUNNING;
    ASSERT_NE(nullptr, taosArrayPush(reports.get(), &report));
  }
  SStmTaskStatusMsg peer = {};
  peer.streamId = kStreamB;
  peer.taskId = 900;
  peer.seriousId = 1;
  peer.nodeId = 1;
  peer.type = STREAM_TRIGGER_TASK;
  peer.status = STREAM_STATUS_RUNNING;
  ASSERT_NE(nullptr, taosArrayPush(reports.get(), &peer));
  EXPECT_EQ(missingReader, omitted);
  // Detach node indexes before freeing the previous leader's task ownership.
  for (int64_t id : {kStreamA, kStreamB}) {
    while ((entry = taosHashIterate(mStreamMgmt.snodeMap, entry))) {
      auto *node = static_cast<SStmSnodeStatus *>(entry);
      if (taosHashGet(node->streamTasks, &id, sizeof(id))) EXPECT_EQ(0, taosHashRemove(node->streamTasks, &id, sizeof(id)));
    }
    while ((entry = taosHashIterate(mStreamMgmt.vgroupMap, entry))) {
      auto *node = static_cast<SStmVgroupStatus *>(entry);
      if (taosHashGet(node->streamTasks, &id, sizeof(id))) EXPECT_EQ(0, taosHashRemove(node->streamTasks, &id, sizeof(id)));
    }
  }
  taosHashClear(mStreamMgmt.taskMap);
  while ((entry = taosHashIterate(mStreamMgmt.streamMap, entry))) mstDestroySStmStatus(static_cast<SStmStatus *>(entry));
  taosHashClear(mStreamMgmt.streamMap);
  taosHashClear(mStreamMgmt.toDeploySnodeMap);
  taosHashClear(mStreamMgmt.toDeployVgMap);
  mStreamMgmt.toDeploySnodeTaskNum = mStreamMgmt.toDeployVgTaskNum = 0;
  mStreamMgmt.state = MND_STM_STATE_WATCH;
  SStreamHbMsg hb = {};
  hb.dnodeId = hb.snodeId = 1;
  hb.pStreamStatus = reports.get();
  const auto normalBegin = mStreamMgmt.lastTs[STM_EVENT_NORMAL_BEGIN];
  const auto errorEvent = mStreamMgmt.lastTs[STM_EVENT_STM_TERR];
  const int64_t begin = MND_STREAM_GET_LAST_TS(STM_EVENT_ACTIVE_BEGIN);
  ReadHeartbeatDeploys(hb, 0, [](const SStmStreamDeploy &) {}, begin + 1);
  EXPECT_EQ(MND_STM_STATE_WATCH, mStreamMgmt.state);
  EXPECT_EQ(2, taosHashGetSize(mStreamMgmt.streamMap));
  ReadHeartbeatDeploys(hb, 0, [](const SStmStreamDeploy &) {}, begin + MST_SHORT_ISOLATION_DURATION + 1);
  mStreamMgmt.lastTs[STM_EVENT_NORMAL_BEGIN] = normalBegin;
  mStreamMgmt.lastTs[STM_EVENT_STM_TERR] = errorEvent;
  EXPECT_EQ(MND_STM_STATE_NORMAL, mStreamMgmt.state);
  for (int64_t id : {kStreamA, kStreamB}) {
    auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &id, sizeof(id)));
    ASSERT_NE(nullptr, status);
    ASSERT_NE(nullptr, status->triggerTask);
    EXPECT_EQ(id == kStreamA && missingReader ? TSDB_CODE_MND_STREAM_TASK_LOST : 0, status->fatalError);
    EXPECT_EQ(id == kStreamA && missingReader, status->stopped != 0);
    if (id == kStreamA) {
      EXPECT_EQ(cache ? 0 : 2, status->calcReaderNum);
      if (empty) EXPECT_EQ(nullptr, static_cast<SStreamCalcScan *>(taosArrayGet(status->pCreate->calcScanPlanList, 1))->vgList);
    }
  }
}

TEST_F(MndStreamDeployOwnershipTest, WatchAdoptsCacheScansThroughEnding) {
  CheckWatchDeployedScans(true, false, false);
}

TEST_F(MndStreamDeployOwnershipTest, WatchAdoptsNullPartitionsAndKeepsHealthyPeer) {
  CheckWatchDeployedScans(false, true, false);
}

TEST_F(MndStreamDeployOwnershipTest, WatchStillDetectsMissingPartitionReader) {
  CheckWatchDeployedScans(false, false, true);
}

TEST_F(MndStreamDeployOwnershipTest, WatchDoesNotDecodeCacheEmptyOrSentinelScans) {
  ASSERT_NO_FATAL_FAILURE(PrependWatchNonVgroupScans());
  static int decodes;
  decodes = 0;
  stub_.set(nodesStringToNode, +[](const char *, SNode **) -> int32_t {
    ++decodes;
    return TSDB_CODE_OUT_OF_MEMORY;
  });
  EXPECT_EQ(0, WatchTask());
  EXPECT_EQ(0, decodes);
  EXPECT_NE(nullptr, taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  EXPECT_EQ(nullptr, mStreamMgmt.watch.deferredStreams);
}

TEST_F(MndStreamDeployOwnershipTest, WatchFindsVirtualMetadataAfterNonVgroupScans) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  ASSERT_NO_FATAL_FAILURE(PrependWatchNonVgroupScans());
  // Count only the real scan; the skipped entries deliberately contain invalid plan text.
  static int decodes;
  decodes = 0;
  stub_.set(nodesStringToNode, +[](const char *text, SNode **out) -> int32_t {
    ++decodes;
    EXPECT_STRNE("must not decode non-vgroup scan", text);
    int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, out);
    if (code) return code;
    auto *plan = reinterpret_cast<SSubplan *>(*out);
    code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN, reinterpret_cast<SNode **>(&plan->pNode));
    if (code) return code;
    auto *scan = &reinterpret_cast<SSystemTableScanPhysiNode *>(plan->pNode)->scan;
    scan->virtualStableScan = true;
    tstrncpy(scan->tableName.dbname, TSDB_INFORMATION_SCHEMA_DB, sizeof(scan->tableName.dbname));
    tstrncpy(scan->tableName.tname, TSDB_INS_TABLE_VC_COLS, sizeof(scan->tableName.tname));
    return 0;
  });
  EXPECT_EQ(0, WatchTask());
  EXPECT_EQ(1, decodes);
  EXPECT_EQ(nullptr, taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.watch.deferredStreams));
}

TEST_F(MndStreamDeployOwnershipTest, MetadataRecoveryDoesNotRedecodeCacheOrEmptyScans) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  auto *create = Stream(kStreamA)->pCreate;
  auto *source = static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, 0));
  SNode *raw = nullptr;
  ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(source->scanPlan), &raw));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> owner(raw, nodesDestroyNode);
  reinterpret_cast<SSubplan *>(raw)->id.groupId = 99;
  reinterpret_cast<SSubplan *>(raw)->id.subplanId = 99;
  const size_t cacheIndex = taosArrayGetSize(create->calcScanPlanList);
  auto *cacheEntry = static_cast<SStreamCalcScan *>(taosArrayReserve(create->calcScanPlanList, 1));
  ASSERT_NE(nullptr, cacheEntry);
  cacheEntry->readFromCache = true;
  ASSERT_EQ(0, nodesNodeToString(raw, false, reinterpret_cast<char **>(&cacheEntry->scanPlan), nullptr));
  ASSERT_NE(nullptr, cacheEntry->scanPlan);
  cacheEntry->vgList = taosArrayInit(1, sizeof(int32_t));
  ASSERT_NE(nullptr, cacheEntry->vgList);
  int32_t cacheVg = 20;
  ASSERT_NE(nullptr, taosArrayPush(cacheEntry->vgList, &cacheVg));
  reinterpret_cast<SSubplan *>(raw)->id.subplanId = 100;
  auto *emptyEntry = static_cast<SStreamCalcScan *>(taosArrayReserve(create->calcScanPlanList, 1));
  ASSERT_NE(nullptr, emptyEntry);
  ASSERT_EQ(0, nodesNodeToString(raw, false, reinterpret_cast<char **>(&emptyEntry->scanPlan), nullptr));
  ASSERT_NE(nullptr, emptyEntry->scanPlan);
  static Stub *activeStub;
  static std::string cacheText;
#ifdef BUILD_TEST_SEAM
  static std::string emptyText;
#endif
  static int cacheDecodes;
#ifdef BUILD_TEST_SEAM
  static int cacheCoverageDecodes;
  static int emptyDecodes;
  static bool inCoverage;
#endif
  static int (*decode)(const char *, SNode **);
  activeStub = &stub_;
  cacheText = static_cast<const char *>(
      static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, cacheIndex))->scanPlan);
#ifdef BUILD_TEST_SEAM
  emptyText = static_cast<const char *>(emptyEntry->scanPlan);
#endif
  cacheDecodes = 0;
#ifdef BUILD_TEST_SEAM
  cacheCoverageDecodes = 0;
  emptyDecodes = 0;
  inCoverage = false;
  const auto previousPhase = gTdSeam_msmDeployPhase;
  gTdSeam_msmDeployPhase = [](const char *phase, const char *) {
    if (strcmp(phase, "metadata-coverage-begin") == 0) inCoverage = true;
    if (strcmp(phase, "metadata-coverage-end") == 0) inCoverage = false;
  };
#endif
  decode = +[](const char *text, SNode **out) -> int32_t {
    if (cacheText == text) ++cacheDecodes;
#ifdef BUILD_TEST_SEAM
    if (inCoverage && cacheText == text) ++cacheCoverageDecodes;
    if (inCoverage && emptyText == text) ++emptyDecodes;
#endif
    activeStub->reset(nodesStringToNode);
    int32_t code = nodesStringToNode(text, out);
    activeStub->set(nodesStringToNode, decode);
    return code;
  };
  stub_.set(nodesStringToNode, decode);
  QueueDeploy(kStreamA);
  HandleActions();
#ifdef BUILD_TEST_SEAM
  gTdSeam_msmDeployPhase = previousPhase;
#endif
  stub_.reset(nodesStringToNode);
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  EXPECT_EQ(0, status->fatalError);
  EXPECT_EQ(1, status->calcReaderNum);
  // The cache plan is decoded once to bind the Trigger, never by the coverage index.
  EXPECT_EQ(1, cacheDecodes);
#ifdef BUILD_TEST_SEAM
  EXPECT_EQ(0, cacheCoverageDecodes);
  EXPECT_EQ(0, emptyDecodes);
#endif
}

TEST_F(MndStreamDeployOwnershipTest, WatchDeferralCacheAllocationFailureDoesNotAbortAdoption) {
  ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(true));
  stub_.set(nodesStringToNode, +[](const char *, SNode **) -> int32_t { return TSDB_CODE_OUT_OF_MEMORY; });
  stub_.set(taosHashInit, +[](size_t, _hash_fn_t, bool, SHashLockTypeE) -> SHashObj * { return nullptr; });
  EXPECT_EQ(0, WatchTask());
  EXPECT_EQ(nullptr, mStreamMgmt.watch.deferredStreams);
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.streamMap));
  stub_.reset(taosHashInit);
  EXPECT_EQ(0, WatchTask());
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.watch.deferredStreams));
}

TEST_F(MndStreamDeployOwnershipTest, ExistingUnavailableMetadataPartitionIsRetained) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  stub_.set(mndAcquireVgroup, +[](SMnode *, int32_t) -> SVgObj * {
    static SVgObj existing = {};
    return &existing;
  });
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  EXPECT_EQ(3, listNEles(status->calcReaders));
  EXPECT_EQ(3, status->calcReaderNum);
}

TEST_F(MndStreamDeployOwnershipTest, ManyRemovedMetadataPartitionsKeepSparseReaderIdentity) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan(256));
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  ASSERT_EQ(1, listNEles(status->calcReaders));
  auto reader = reinterpret_cast<SStmTaskStatus *>(listHead(status->calcReaders)->data)->id;
  EXPECT_EQ(1, reader.taskIdx);
  std::vector<int64_t> initialRunners;
  HeartbeatTriggerIds(0, &initialRunners);
  ASSERT_FALSE(initialRunners.empty());
  SStmTaskAction repair = {};
  repair.streamId = kStreamA;
  repair.generation = status->triggerTask->id.taskId;
  repair.type = STREAM_RUNNER_TASK;
  repair.multiRunner = true;
  repair.deployNum = 1;
  ASSERT_EQ(0, mstPostTaskAction(&actionQueue_, &repair, STREAM_ACT_DEPLOY));
  HandleActions();
  SStreamHbMsg hb = {};
  hb.dnodeId = hb.snodeId = 1;
  int runners = 0;
  ReadHeartbeatDeploys(hb, 0, [&](const SStmStreamDeploy &stream) {
    for (size_t i = 0; i < taosArrayGetSize(stream.runnerTasks); ++i) {
      auto *task = static_cast<SStmTaskDeploy *>(taosArrayGet(stream.runnerTasks, i));
      EXPECT_TRUE(STREAM_IS_REDEPLOY_RUNNER(task->task.flags));
      SNode *raw = nullptr;
      ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(task->msg.runner.pPlan), &raw));
      std::unique_ptr<SNode, decltype(&nodesDestroyNode)> owner(raw, nodesDestroyNode);
      ASSERT_NE(nullptr, raw);
      ASSERT_EQ(QUERY_NODE_PHYSICAL_SUBPLAN, nodeType(raw));
      auto *plan = reinterpret_cast<SSubplan *>(raw);
      ASSERT_NE(nullptr, plan->pNode);
      ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, nodeType(plan->pNode));
      auto *exchange = reinterpret_cast<SExchangePhysiNode *>(plan->pNode);
      ASSERT_EQ(1, LIST_LENGTH(exchange->pSrcEndPoints));
      auto *source = reinterpret_cast<SDownstreamSourceNode *>(nodesListGetNode(exchange->pSrcEndPoints, 0));
      EXPECT_EQ(reader.taskId, source->taskId);
      EXPECT_EQ(20, source->addr.nodeId);
      ++runners;
    }
  });
  EXPECT_GT(runners, 0);
}

TEST_F(MndStreamDeployOwnershipTest, MissingDataScanIsNotPrunedAsMetadata) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  auto *scan = static_cast<SStreamCalcScan *>(taosArrayGet(Stream(kStreamA)->pCreate->calcScanPlanList, 0));
  SNode *raw = nullptr;
  ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(scan->scanPlan), &raw));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> owner(raw, nodesDestroyNode);
  auto *plan = reinterpret_cast<SSubplan *>(raw);
  SNode *newScan = nullptr;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, &newScan));
  nodesDestroyNode(reinterpret_cast<SNode *>(plan->pNode));
  plan->pNode = reinterpret_cast<SPhysiNode *>(newScan);
  void *encodedScan = nullptr;
  ASSERT_EQ(0, nodesNodeToString(raw, false, reinterpret_cast<char **>(&encodedScan), nullptr));
  ASSERT_NE(nullptr, encodedScan);
  taosMemoryFree(scan->scanPlan);
  scan->scanPlan = encodedScan;
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  ASSERT_EQ(2, listNEles(status->calcReaders));
  auto *reader = reinterpret_cast<SStmTaskStatus *>(listHead(status->calcReaders)->data);
  EXPECT_EQ(21, reader->id.nodeId);
  EXPECT_EQ(0, reader->id.taskIdx);
}

TEST_F(MndStreamDeployOwnershipTest, MissingEntireMetadataGroupDoesNotPublishEmptyExchange) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  stub_.set(mndGetVgroupEpsetById, +[](SMnode *, int32_t) -> SEpSet { return {}; });
  stub_.set(mndAcquireVgroup, +[](SMnode *, int32_t) -> SVgObj * {
    terrno = TSDB_CODE_MND_VGROUP_NOT_EXIST;
    return nullptr;
  });
  QueueDeploy(kStreamA);
  HandleActions();
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
}

TEST_F(MndStreamDeployOwnershipTest, MissingOrdinarySystemScanIsNotPrunedAsMetadata) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  auto *scan = static_cast<SStreamCalcScan *>(taosArrayGet(Stream(kStreamA)->pCreate->calcScanPlanList, 0));
  SNode *raw = nullptr;
  ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(scan->scanPlan), &raw));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> owner(raw, nodesDestroyNode);
  auto *plan = reinterpret_cast<SSubplan *>(raw);
  auto *meta = reinterpret_cast<SSystemTableScanPhysiNode *>(plan->pNode);
  meta->scan.virtualStableScan = false;
  tstrncpy(meta->scan.tableName.tname, TSDB_INS_TABLE_TAGS, sizeof(meta->scan.tableName.tname));
  void *encodedScan = nullptr;
  ASSERT_EQ(0, nodesNodeToString(raw, false, reinterpret_cast<char **>(&encodedScan), nullptr));
  ASSERT_NE(nullptr, encodedScan);
  taosMemoryFree(scan->scanPlan);
  scan->scanPlan = encodedScan;
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  ASSERT_EQ(2, listNEles(status->calcReaders));
  auto *reader = reinterpret_cast<SStmTaskStatus *>(listHead(status->calcReaders)->data);
  EXPECT_EQ(21, reader->id.nodeId);
  EXPECT_EQ(0, reader->id.taskIdx);
}

TEST_F(MndStreamDeployOwnershipTest, MetadataLookupErrorDoesNotPrunePartition) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  stub_.set(mndAcquireVgroup, +[](SMnode *, int32_t) -> SVgObj * {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  });
  QueueDeploy(kStreamA);
  HandleActions();
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, status->fatalError);
}

TEST_F(MndStreamDeployOwnershipTest, RemovedMetadataWithNewVgroupsRefusesIncompleteCoverage) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  SetMetadataCatalog({20, 30, 31});
  QueueDeploy(kStreamA);
  HandleActions();
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
}

TEST_F(MndStreamDeployOwnershipTest, MetadataRecoveryRejectsVisibleVgroupBeforeVersionIncrement) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  static int visited;
  visited = 0;
  // The build sees the old catalog; READY publication can precede tableVer's increment.
  stub_.set(sdbTraverse, +[](SSdb *, ESdbType type, sdbTraverseFp callback, void *p1, void *p2, void *p3) {
    EXPECT_EQ(SDB_VGROUP, type);
    for (int32_t id : {20, 30}) {
      SVgObj row = {};
      row.vgId = id;
      ++visited;
      if (!callback(nullptr, &row, p1, p2, p3)) break;
    }
  });
  QueueDeploy(kStreamA);
  HandleActions();
  EXPECT_EQ(2, visited);
  EXPECT_EQ(0, sdbGetTableVer(mnode_.pSdb, SDB_VGROUP));
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
  EXPECT_EQ(0, mStreamMgmt.toDeployVgTaskNum);
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(TSDB_CODE_MND_STREAM_NOT_AVAILABLE, status->fatalError);
}

TEST_F(MndStreamDeployOwnershipTest, AnotherGroupWithSameSubplanIdCannotMaskEmptyMetadata) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan(2));
  auto *create = Stream(kStreamA)->pCreate;
  auto *scan = static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, 1));
  SNode *scanNode = nullptr;
  ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(scan->scanPlan), &scanNode));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> scanOwner(scanNode, nodesDestroyNode);
  auto *sub = reinterpret_cast<SSubplan *>(scanNode);
  sub->id.groupId = 4;
  sub->id.subplanId = 13;
  void *encodedScan = nullptr;
  ASSERT_EQ(0, nodesNodeToString(scanNode, false, reinterpret_cast<char **>(&encodedScan), nullptr));
  ASSERT_NE(nullptr, encodedScan);
  taosMemoryFree(scan->scanPlan);
  scan->scanPlan = encodedScan;
  SNode *calcNode = nullptr;
  ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(create->calcPlan), &calcNode));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> owner(calcNode, nodesDestroyNode);
  auto *dag = reinterpret_cast<SQueryPlan *>(calcNode);
  auto *level = reinterpret_cast<SNodeListNode *>(nodesListGetNode(dag->pSubplans, 0));
  auto *runner = reinterpret_cast<SSubplan *>(nodesListGetNode(level->pNodeList, 0));
  auto *ref = reinterpret_cast<SValueNode *>(nodesListGetNode(runner->pChildren, 1));
  ref->datum.i = (INT64_C(4) << 32) | 13;
  char *encodedCalcPlan = nullptr;
  ASSERT_EQ(0, nodesNodeToString(calcNode, false, &encodedCalcPlan, nullptr));
  ASSERT_NE(nullptr, encodedCalcPlan);
  taosMemoryFree(create->calcPlan);
  create->calcPlan = encodedCalcPlan;
  // A short-key regression must not fail early on duplicate insertion of the empty marker.
  std::swap(*static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, 0)),
            *static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, 1)));
  SetMetadataCatalog({});
  QueueDeploy(kStreamA);
  HandleActions();
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(TSDB_CODE_MND_STREAM_INTERNAL_ERROR, status->fatalError);
}

TEST_F(MndStreamDeployOwnershipTest, RunnerRepairRebuildsReadersRemovedSinceDeployment) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  stub_.set(mndGetVgroupEpsetById, +[](SMnode *, int32_t vg) -> SEpSet {
    SEpSet ep = {};
    ep.numOfEps = 1;
    ep.eps[0].port = 6000 + vg;
    tstrncpy(ep.eps[0].fqdn, "node1", sizeof(ep.eps[0].fqdn));
    return ep;
  });
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  ASSERT_EQ(3, listNEles(status->calcReaders));
  int64_t oldTrigger = status->triggerTask->id.taskId;
  std::vector<int64_t> initialRunners;
  HeartbeatTriggerIds(0, &initialRunners);
  ASSERT_FALSE(initialRunners.empty());
  stub_.set(mndGetVgroupEpsetById, +[](SMnode *, int32_t vg) -> SEpSet {
    SEpSet ep = {};
    if (vg == 20) {
      ep.numOfEps = 1;
      ep.eps[0].port = 6020;
      tstrncpy(ep.eps[0].fqdn, "node1", sizeof(ep.eps[0].fqdn));
    }
    return ep;
  });
  SStmTaskAction repair = {};
  repair.streamId = kStreamA;
  repair.generation = oldTrigger;
  repair.type = STREAM_RUNNER_TASK;
  repair.multiRunner = true;
  repair.deployNum = 1;
  ASSERT_EQ(0, mstPostTaskAction(&actionQueue_, &repair, STREAM_ACT_DEPLOY));
  HandleActions();
  HandleActions();
  status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  EXPECT_NE(oldTrigger, status->triggerTask->id.taskId);
  ASSERT_EQ(1, listNEles(status->calcReaders));
  EXPECT_EQ(1, status->calcReaderNum);
  EXPECT_EQ(20, reinterpret_cast<SStmTaskStatus *>(listHead(status->calcReaders)->data)->id.nodeId);
}

class MndStreamMetadataGroupsTest : public MndStreamDeployOwnershipTest,
                                    public ::testing::WithParamInterface<bool> {};

void MndStreamDeployOwnershipTest::SetMetadataGroups(bool recovering) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan(2));
  auto *create = Stream(kStreamA)->pCreate;
  auto *scan = static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, 1));
  SNode *scanNode = nullptr;
  ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(scan->scanPlan), &scanNode));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> scanOwner(scanNode, nodesDestroyNode);
  auto *readerPlan = reinterpret_cast<SSubplan *>(scanNode);
  readerPlan->id.groupId = 4;
  readerPlan->id.subplanId = 13;
  void *encodedScan = nullptr;
  ASSERT_EQ(0, nodesNodeToString(scanNode, false, reinterpret_cast<char **>(&encodedScan), nullptr));
  ASSERT_NE(nullptr, encodedScan);
  taosMemoryFree(scan->scanPlan);
  scan->scanPlan = encodedScan;
  SNode *calcNode = nullptr;
  ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(create->calcPlan), &calcNode));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> owner(calcNode, nodesDestroyNode);
  auto *dag = reinterpret_cast<SQueryPlan *>(calcNode);
  auto *level = reinterpret_cast<SNodeListNode *>(nodesListGetNode(dag->pSubplans, 0));
  auto *runner = reinterpret_cast<SSubplan *>(nodesListGetNode(level->pNodeList, 0));
  auto *ref = reinterpret_cast<SValueNode *>(nodesListGetNode(runner->pChildren, 1));
  ref->datum.i = (INT64_C(4) << 32) | 13;
  SNode *project = nullptr, *exchange = nullptr;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PROJECT, &project));
  auto *oldRoot = runner->pNode;
  runner->pNode = reinterpret_cast<SPhysiNode *>(project);
  ASSERT_EQ(0, nodesListMakeStrictAppend(&reinterpret_cast<SPhysiNode *>(project)->pChildren,
                                       reinterpret_cast<SNode *>(oldRoot)));
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, &exchange));
  reinterpret_cast<SExchangePhysiNode *>(exchange)->srcStartGroupId = 4;
  reinterpret_cast<SExchangePhysiNode *>(exchange)->srcEndGroupId = 4;
  ASSERT_EQ(0, nodesListMakeStrictAppend(&runner->pNode->pChildren, exchange));
  char *encodedCalcPlan = nullptr;
  ASSERT_EQ(0, nodesNodeToString(calcNode, false, &encodedCalcPlan, nullptr));
  ASSERT_NE(nullptr, encodedCalcPlan);
  taosMemoryFree(create->calcPlan);
  create->calcPlan = encodedCalcPlan;
  if (recovering) {
    // Both groups need recovery, share the live partition, and reuse subplan 13.
    for (int i = 0; i < 2; ++i) {
      auto *item = static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, i));
      *static_cast<int32_t *>(taosArrayGet(item->vgList, 0)) = 21;
      int32_t live = 20;
      ASSERT_NE(nullptr, taosArrayPush(item->vgList, &live));
    }
  } else {
    stub_.set(mndGetVgroupEpsetById, +[](SMnode *, int32_t vg) -> SEpSet {
      SEpSet ep = {};
      ep.numOfEps = 1;
      ep.eps[0].port = 6000 + vg;
      tstrncpy(ep.eps[0].fqdn, "node1", sizeof(ep.eps[0].fqdn));
      return ep;
    });
  }
}

TEST_P(MndStreamMetadataGroupsTest, SameNumberReadersBindTheirOwnGroups) {
  ASSERT_NO_FATAL_FAILURE(SetMetadataGroups(GetParam()));
  static int traversals;
  traversals = 0;
  stub_.set(sdbTraverse, +[](SSdb *, ESdbType type, sdbTraverseFp callback, void *p1, void *p2, void *p3) {
    EXPECT_EQ(SDB_VGROUP, type);
    ++traversals;
    SVgObj row = {};
    row.vgId = 20;
    callback(nullptr, &row, p1, p2, p3);
  });
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  ASSERT_EQ(2, listNEles(status->calcReaders));
  EXPECT_EQ(GetParam() ? 1 : 0, traversals);
  SStreamHbMsg hb = {};
  hb.dnodeId = hb.snodeId = 1;
  int runners = 0;
  ReadHeartbeatDeploys(hb, 0, [&](const SStmStreamDeploy &stream) {
    for (size_t i = 0; i < taosArrayGetSize(stream.runnerTasks); ++i) {
      auto *task = static_cast<SStmTaskDeploy *>(taosArrayGet(stream.runnerTasks, i));
      SNode *node = nullptr;
      ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(task->msg.runner.pPlan), &node));
      std::unique_ptr<SNode, decltype(&nodesDestroyNode)> planOwner(node, nodesDestroyNode);
      ASSERT_NE(nullptr, node);
      ASSERT_EQ(QUERY_NODE_PHYSICAL_SUBPLAN, nodeType(node));
      auto *plan = reinterpret_cast<SSubplan *>(node);
      ASSERT_NE(nullptr, plan->pNode);
      ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_PROJECT, nodeType(plan->pNode));
      ASSERT_EQ(2, LIST_LENGTH(plan->pNode->pChildren));
      SListNode *reader = listHead(status->calcReaders);
      for (int child = 0; child < 2; ++child) {
        auto *childNode = nodesListGetNode(plan->pNode->pChildren, child);
        ASSERT_NE(nullptr, childNode);
        ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, nodeType(childNode));
        auto *input = reinterpret_cast<SExchangePhysiNode *>(childNode);
        ASSERT_EQ(1, LIST_LENGTH(input->pSrcEndPoints));
        auto *sourceNode = nodesListGetNode(input->pSrcEndPoints, 0);
        ASSERT_NE(nullptr, sourceNode);
        ASSERT_EQ(QUERY_NODE_DOWNSTREAM_SOURCE, nodeType(sourceNode));
        auto *source = reinterpret_cast<SDownstreamSourceNode *>(sourceNode);
        EXPECT_EQ(child == 0 && !GetParam() ? 21 : 20, source->addr.nodeId);
        EXPECT_EQ(reinterpret_cast<SStmTaskStatus *>(reader->data)->id.taskId, source->taskId);
        reader = TD_DLIST_NODE_NEXT(reader);
      }
      ++runners;
    }
  });
  EXPECT_GT(runners, 0);
}

INSTANTIATE_TEST_SUITE_P(Recovery, MndStreamMetadataGroupsTest, ::testing::Bool());

TEST_F(MndStreamDeployOwnershipTest, MetadataCoverageRequiresEveryRecoveredGroup) {
  ASSERT_NO_FATAL_FAILURE(SetMetadataGroups(true));
  auto *scan = static_cast<SStreamCalcScan *>(taosArrayGet(Stream(kStreamA)->pCreate->calcScanPlanList, 0));
  const int32_t extra = 30;
  ASSERT_NE(nullptr, taosArrayPush(scan->vgList, &extra));
  stub_.set(mndGetVgroupEpsetById, +[](SMnode *, int32_t vg) -> SEpSet {
    SEpSet ep = {};
    if (vg == 20 || vg == 30) {
      ep.numOfEps = 1;
      ep.eps[0].port = 6000 + vg;
      tstrncpy(ep.eps[0].fqdn, "node1", sizeof(ep.eps[0].fqdn));
    }
    return ep;
  });
  static int visited;
  visited = 0;
  stub_.set(sdbTraverse, +[](SSdb *, ESdbType type, sdbTraverseFp callback, void *p1, void *p2, void *p3) {
    EXPECT_EQ(SDB_VGROUP, type);
    for (int32_t id : {20, 30}) {
      SVgObj row = {};
      row.vgId = id;
      ++visited;
      if (!callback(nullptr, &row, p1, p2, p3)) break;
    }
  });
  QueueDeploy(kStreamA);
  HandleActions();
  EXPECT_EQ(2, visited);
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
  EXPECT_EQ(0, mStreamMgmt.toDeployVgTaskNum);
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(TSDB_CODE_MND_STREAM_NOT_AVAILABLE, status->fatalError);
}

TEST_F(MndStreamDeployOwnershipTest, VgroupEndpointLookupPreservesStatusAndOrder) {
  static SVgObj group;
  static SDnodeObj nodes[2];
  static int32_t lookupError;
  static int lookups;
  group = {};
  group.replica = 2;
  group.vnodeGid[0].dnodeId = 1;
  group.vnodeGid[1].dnodeId = 2;
  group.vnodeGid[1].syncState = TAOS_SYNC_STATE_LEADER;
  nodes[0] = {};
  nodes[1] = {};
  tstrncpy(nodes[0].fqdn, "z-node", sizeof(nodes[0].fqdn));
  tstrncpy(nodes[1].fqdn, "a-node", sizeof(nodes[1].fqdn));
  nodes[0].port = nodes[1].port = 6030;
  stub_.set(mndAcquireVgroup, +[](SMnode *, int32_t) -> SVgObj * {
    ++lookups;
    terrno = lookupError;
    return lookupError ? nullptr : &group;
  });
  stub_.set(mndReleaseVgroup, +[](SMnode *, SVgObj *) {});
  stub_.set(mndAcquireDnode, +[](SMnode *, int32_t id) -> SDnodeObj * { return &nodes[id - 1]; });
  stub_.set(mndReleaseDnode, +[](SMnode *, SDnodeObj *) {});
  for (int32_t error : {0, TSDB_CODE_MND_VGROUP_NOT_EXIST, TSDB_CODE_OUT_OF_MEMORY}) {
    lookupError = error;
    lookups = 0;
    int32_t code = -1;
    SEpSet ep = mndGetVgroupEpsetByIdWithStatus(&mnode_, 20, &code);
    EXPECT_EQ(1, lookups);
    EXPECT_EQ(error, code);
    EXPECT_EQ(error ? 0 : 2, ep.numOfEps);
    if (!error) {
      EXPECT_STREQ("z-node", ep.eps[0].fqdn);
      EXPECT_STREQ("a-node", ep.eps[1].fqdn);
      EXPECT_EQ(1, ep.inUse);
    }
  }
  lookupError = 0;
  lookups = 0;
  stub_.set(mndAcquireDnode, +[](SMnode *, int32_t) -> SDnodeObj * {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    return nullptr;
  });
  int32_t code = -1;
  EXPECT_EQ(0, mndGetVgroupEpsetByIdWithStatus(&mnode_, 20, &code).numOfEps);
  EXPECT_EQ(0, code);  // Existing without endpoints must not be classified as deleted.
  EXPECT_EQ(1, lookups);
}

TEST_F(MndStreamDeployOwnershipTest, MetadataRecoveryRevalidatesBindingsAfterRestoreRetry) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  static int traversals;
  static int commits;
  traversals = commits = 0;
  stub_.set(sdbTraverse, +[](SSdb *, ESdbType type, sdbTraverseFp callback, void *p1, void *p2, void *p3) {
    EXPECT_EQ(SDB_VGROUP, type);
    if (++traversals == 1) {
      SVgObj row = {};
      row.vgId = 20;
      callback(nullptr, &row, p1, p2, p3);
    }
  });
  stub_.set(mndStreamRecalcCommitRestore, +[](SStreamObj *, SStmStatus *, SStreamRecalcRestore *) -> bool {
    ++commits;
    return false;
  });
  QueueDeploy(kStreamA);
  HandleActions();
  EXPECT_EQ(2, traversals);
  EXPECT_EQ(1, commits);
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(TSDB_CODE_MND_STREAM_NOT_AVAILABLE, status->fatalError);
}

TEST_F(MndStreamDeployOwnershipTest, MetadataRecoveryRejectsBoundVgroupRemovedBeforeSnapshot) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  static int traversals;
  traversals = 0;
  // Vgroup 20 supplied an endpoint during construction, but is absent when the read lock is acquired.
  stub_.set(sdbTraverse, +[](SSdb *, ESdbType type, sdbTraverseFp, void *, void *, void *) {
    EXPECT_EQ(SDB_VGROUP, type);
    ++traversals;
  });
  QueueDeploy(kStreamA);
  HandleActions();
  EXPECT_EQ(1, traversals);
  EXPECT_EQ(0, sdbGetTableVer(mnode_.pSdb, SDB_VGROUP));
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
  EXPECT_EQ(0, mStreamMgmt.toDeployVgTaskNum);
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(TSDB_CODE_MND_STREAM_NOT_AVAILABLE, status->fatalError);
}

TEST_F(MndStreamDeployOwnershipTest, MetadataRecoveryAlsoChecksBindingsOfHealthyGroups) {
  ASSERT_NO_FATAL_FAILURE(SetMetadataGroups(true));
  auto *create = Stream(kStreamA)->pCreate;
  auto *recovering = static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, 0));
  auto *healthy = static_cast<SStreamCalcScan *>(taosArrayGet(create->calcScanPlanList, 1));
  // A recovers {22,20}; B binds {21,20}. Only B's 21 disappears at final validation.
  *static_cast<int32_t *>(taosArrayGet(recovering->vgList, 0)) = 22;
  stub_.set(mndGetVgroupEpsetById, +[](SMnode *, int32_t vg) -> SEpSet {
    SEpSet ep = {};
    if (vg == 20 || vg == 21) {
      ep.numOfEps = 1;
      ep.eps[0].port = 6000 + vg;
      tstrncpy(ep.eps[0].fqdn, "node1", sizeof(ep.eps[0].fqdn));
    }
    return ep;
  });
  ASSERT_EQ(21, *static_cast<int32_t *>(taosArrayGet(healthy->vgList, 0)));
  SetMetadataCatalog({20, 21});
  static int visited;
  visited = 0;
  stub_.set(sdbTraverse, +[](SSdb *, ESdbType type, sdbTraverseFp callback, void *p1, void *p2, void *p3) {
    EXPECT_EQ(SDB_VGROUP, type);
    SVgObj row = {};
    row.vgId = 20;
    ++visited;
    callback(nullptr, &row, p1, p2, p3);
  });
  QueueDeploy(kStreamA);
  HandleActions();
  EXPECT_EQ(1, visited);
  EXPECT_EQ(0, sdbGetTableVer(mnode_.pSdb, SDB_VGROUP));
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(0, mStreamMgmt.toDeploySnodeTaskNum);
  EXPECT_EQ(0, mStreamMgmt.toDeployVgTaskNum);
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(TSDB_CODE_MND_STREAM_NOT_AVAILABLE, status->fatalError);
}

TEST_F(MndStreamDeployOwnershipTest, MetadataCatalogTraversalErrorIsPreserved) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  stub_.set(sdbTraverse, +[](SSdb *, ESdbType, sdbTraverseFp, void *, void *, void *) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  });
  QueueDeploy(kStreamA);
  HandleActions();
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, status->fatalError);
}

TEST_F(MndStreamDeployOwnershipTest, RunnerRepairRejectsVisibleVgroupWithUnchangedVersion) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  std::vector<int64_t> initialRunners;
  HeartbeatTriggerIds(0, &initialRunners);
  ASSERT_FALSE(initialRunners.empty());
  static int visited;
  visited = 0;
  // Keep the build catalog at {20}; expose 30 only in the final locked snapshot.
  stub_.set(sdbTraverse, +[](SSdb *, ESdbType type, sdbTraverseFp callback, void *p1, void *p2, void *p3) {
    EXPECT_EQ(SDB_VGROUP, type);
    for (int32_t id : {20, 30}) {
      SVgObj row = {};
      row.vgId = id;
      ++visited;
      if (!callback(nullptr, &row, p1, p2, p3)) break;
    }
  });
  SStmTaskAction repair = {};
  repair.streamId = kStreamA;
  repair.generation = status->triggerTask->id.taskId;
  repair.type = STREAM_RUNNER_TASK;
  repair.multiRunner = true;
  repair.deployNum = 1;
  ASSERT_EQ(0, mstPostTaskAction(&actionQueue_, &repair, STREAM_ACT_DEPLOY));
  HandleActions();
  EXPECT_EQ(1, status->stopped);
  EXPECT_EQ(TSDB_CODE_MND_STREAM_NOT_AVAILABLE, status->fatalError);
  std::vector<int64_t> repairedRunners;
  HeartbeatTriggerIds(0, &repairedRunners);
  EXPECT_TRUE(repairedRunners.empty());
  EXPECT_EQ(2, visited);
  EXPECT_EQ(0, sdbGetTableVer(mnode_.pSdb, SDB_VGROUP));
}

TEST_F(MndStreamDeployOwnershipTest, RunnerRepairRejectsBoundVgroupRemovedBeforeSnapshot) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  std::vector<int64_t> initialRunners;
  HeartbeatTriggerIds(0, &initialRunners);
  ASSERT_FALSE(initialRunners.empty());
  static int traversals;
  traversals = 0;
  stub_.set(sdbTraverse, +[](SSdb *, ESdbType type, sdbTraverseFp, void *, void *, void *) {
    EXPECT_EQ(SDB_VGROUP, type);
    ++traversals;
  });
  SStmTaskAction repair = {};
  repair.streamId = kStreamA;
  repair.generation = status->triggerTask->id.taskId;
  repair.type = STREAM_RUNNER_TASK;
  repair.multiRunner = true;
  repair.deployNum = 1;
  ASSERT_EQ(0, mstPostTaskAction(&actionQueue_, &repair, STREAM_ACT_DEPLOY));
  HandleActions();
  EXPECT_EQ(1, status->stopped);
  EXPECT_EQ(TSDB_CODE_MND_STREAM_NOT_AVAILABLE, status->fatalError);
  std::vector<int64_t> repairedRunners;
  HeartbeatTriggerIds(0, &repairedRunners);
  EXPECT_TRUE(repairedRunners.empty());
  EXPECT_EQ(1, traversals);
  EXPECT_EQ(0, sdbGetTableVer(mnode_.pSdb, SDB_VGROUP));
}

TEST_F(MndStreamDeployOwnershipTest, StatusQueryFinishesDuringRunnerOnlyPlanSerialization) {
  ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(false));
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  ASSERT_NE(nullptr, status->runners[0]);
  auto *runner = static_cast<SStmTaskStatus *>(taosArrayGet(status->runners[0], 0));
  ASSERT_NE(nullptr, runner);
  const auto savedRunner = runner->id.taskId;
  ASSERT_NE(nullptr, AddStatus(kStreamB));
  auto *block = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(nullptr, block);
  SnodeLookupStubGuard snode;
  PrivateBuildPause    pause(false);
  stub_.set(
      tjsonToUnformattedString, +[](const SJson *json) -> char * {
        PrivateBuildPause::Pause();
        return cJSON_PrintUnformatted(reinterpret_cast<const cJSON *>(json));
      });
  SStmTaskAction repair = {};
  repair.streamId = kStreamA;
  repair.generation = status->triggerTask->id.taskId;
  repair.type = STREAM_RUNNER_TASK;
  repair.multiRunner = true;
  repair.deployNum = 1;
  mstPostTaskAction(&actionQueue_, &repair, STREAM_ACT_DEPLOY);
  auto        pending = mStreamMgmt.toDeploySnodeTaskNum;
  std::thread worker([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, Stream(kStreamB), block, 0, 0));
  pause.Release();
  worker.join();
  EXPECT_EQ(0, status->stopped);
  EXPECT_EQ(savedRunner, runner->id.taskId);
  EXPECT_TRUE(STREAM_IS_TOP_RUNNER(runner->flags));
  EXPECT_TRUE(STREAM_IS_REDEPLOY_RUNNER(runner->flags));
  EXPECT_EQ(STREAM_STATUS_UNDEPLOYED, runner->status);
  EXPECT_EQ(pending + 1, mStreamMgmt.toDeploySnodeTaskNum);
  blockDataDestroy(block);
}

TEST_F(MndStreamDeployOwnershipTest, RunnerLossRebindsNestedSubqueryEndpoints) {
  ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(true));
  const int32_t   lostNode = 2, vg = 20;
  const int64_t   lastUp = 0;
  SStmSnodeStatus node = {};
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.snodeMap, &lostNode, sizeof(lostNode), &node, sizeof(node)));
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.dnodeMap, &lostNode, sizeof(lostNode), &lastUp, sizeof(lastUp)));
  // Place only the first child DAG on node 2. Trigger and parent Runner stay on node 1.
  static int placement;
  placement = 0;
  stub_.set(msmAssignTaskSnodeId, +[](SMnode *, SStreamObj *, bool) -> int32_t { return ++placement == 2 ? 2 : 1; });
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  std::vector<int64_t> oldTasks;
  auto                 checkGeneration = [&](int32_t childNode) {
    ASSERT_NE(nullptr, status->triggerTask);
    ASSERT_NE(nullptr, status->calcReaders);
    ASSERT_EQ(1, listNEles(status->calcReaders));
    auto reader = reinterpret_cast<SStmTaskStatus *>(listHead(status->calcReaders)->data)->id;
    EXPECT_EQ(vg, reader.nodeId);
    auto runner = [&](int deploy, int index) {
      return static_cast<SStmTaskStatus *>(taosArrayGet(status->runners[deploy], index))->id;
    };
    std::vector<int64_t> current = {status->triggerTask->id.taskId, reader.taskId};
    for (int deploy = 0; deploy < status->runnerDeploys; ++deploy) {
      ASSERT_EQ(2u, taosArrayGetSize(status->runners[deploy]));
      EXPECT_EQ(deploy == 0 ? childNode : 1, runner(deploy, 0).nodeId);
      EXPECT_EQ(1, runner(deploy, 1).nodeId);
      current.push_back(runner(deploy, 0).taskId);
      current.push_back(runner(deploy, 1).taskId);
    }
    for (auto id : current) EXPECT_EQ(oldTasks.end(), std::find(oldTasks.begin(), oldTasks.end(), id));
    oldTasks = current;
    std::vector<int64_t> delivered;
    auto checkEndpoint = [&](SNode *raw, int64_t task, int32_t dest, int32_t msgType, int64_t clientId) {
      ASSERT_NE(nullptr, raw);
      ASSERT_EQ(QUERY_NODE_DOWNSTREAM_SOURCE, nodeType(raw));
      auto *source = reinterpret_cast<SDownstreamSourceNode *>(raw);
      EXPECT_EQ(clientId, source->clientId);
      EXPECT_EQ(task, source->taskId);
      EXPECT_EQ(msgType, source->fetchMsgType);
      EXPECT_EQ(dest, source->addr.nodeId);
      ASSERT_EQ(1, source->addr.epSet.numOfEps);
      EXPECT_EQ(6000 + dest, source->addr.epSet.eps[0].port);
      EXPECT_STREQ(dest == 2 ? "node2" : "node1", source->addr.epSet.eps[0].fqdn);
    };
    for (int nodeId = 1; nodeId <= childNode; ++nodeId) {
      SStreamHbMsg hb = {};
      hb.dnodeId = hb.snodeId = nodeId;
      if (nodeId == 1) {
        hb.pVgLeaders = taosArrayInit(1, sizeof(vg));
        ASSERT_NE(nullptr, taosArrayPush(hb.pVgLeaders, &vg));
      }
      ReadHeartbeatDeploys(hb, 0, [&](const SStmStreamDeploy &stream) {
        EXPECT_EQ(kStreamA, stream.streamId);
        if (stream.triggerTask) delivered.push_back(stream.triggerTask->task.taskId);
        for (SArray *tasks : {stream.readerTasks, stream.runnerTasks}) {
          for (size_t i = 0; i < taosArrayGetSize(tasks); ++i) {
            auto       *task = static_cast<SStmTaskDeploy *>(taosArrayGet(tasks, i));
            bool        isReader = tasks == stream.readerTasks;
            const void *serialized = isReader ? task->msg.reader.msg.calc.calcScanPlan : task->msg.runner.pPlan;
            SNode      *raw = nullptr;
            ASSERT_EQ(0, nodesStringToNode(static_cast<const char *>(serialized), &raw));
            std::unique_ptr<SNode, decltype(&nodesDestroyNode)> owner(raw, nodesDestroyNode);
            ASSERT_NE(nullptr, raw);
            ASSERT_EQ(QUERY_NODE_PHYSICAL_SUBPLAN, nodeType(raw));
            delivered.push_back(task->task.taskId);
            ASSERT_GE(task->task.deployId, 0);
            ASSERT_LT(task->task.deployId, status->runnerDeploys);
            auto *plan = reinterpret_cast<SSubplan *>(raw);
            ASSERT_NE(nullptr, plan->pNode);
            if (plan->id.subplanId == 12) {
              auto child = runner(task->task.deployId, 0);
              EXPECT_FALSE(isReader);
              EXPECT_EQ(child.nodeId, nodeId);
              EXPECT_EQ(child.taskId, task->task.taskId);
              continue;
            }
            EXPECT_EQ(1, nodeId);
            ASSERT_NE(nullptr, plan->pNode->pConditions);
            ASSERT_EQ(QUERY_NODE_REMOTE_VALUE, nodeType(plan->pNode->pConditions));
            EXPECT_EQ(0, reinterpret_cast<SRemoteValueNode *>(plan->pNode->pConditions)->subQIdx);
            ASSERT_EQ(status->runnerDeploys, LIST_LENGTH(plan->pSubQ));
            for (int deploy = 0; deploy < status->runnerDeploys; ++deploy) {
              auto child = runner(deploy, 0);
              ASSERT_NO_FATAL_FAILURE(checkEndpoint(nodesListGetNode(plan->pSubQ, deploy), child.taskId, child.nodeId,
                                                                    TDMT_STREAM_FETCH_FROM_RUNNER, kStreamA));
            }
            if (isReader) {
              EXPECT_EQ(13, plan->id.subplanId);
              EXPECT_EQ(reader.taskId, task->task.taskId);
            } else {
              auto parent = runner(task->task.deployId, 1);
              EXPECT_EQ(11, plan->id.subplanId);
              EXPECT_EQ(parent.taskId, task->task.taskId);
              ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, nodeType(plan->pNode));
              auto *exchange = reinterpret_cast<SExchangePhysiNode *>(plan->pNode);
              ASSERT_EQ(1, LIST_LENGTH(exchange->pSrcEndPoints));
              ASSERT_NO_FATAL_FAILURE(checkEndpoint(nodesListGetNode(exchange->pSrcEndPoints, 0), reader.taskId, vg,
                                                                    TDMT_STREAM_FETCH, parent.taskId));
            }
          }
        }
      });
      taosArrayDestroy(hb.pVgLeaders);
    }
    std::sort(delivered.begin(), delivered.end());
    std::sort(current.begin(), current.end());
    EXPECT_EQ(current, delivered);
  };
  ASSERT_NO_FATAL_FAILURE(checkGeneration(lostNode));
  static_cast<SStmSnodeStatus *>(taosHashGet(mStreamMgmt.snodeMap, &lostNode, sizeof(lostNode)))->runnerThreadNum = -1;
  SStmTaskAction repair = {};
  repair.streamId = kStreamA;
  repair.generation = status->triggerTask->id.taskId;
  repair.type = STREAM_RUNNER_TASK;
  repair.multiRunner = true;
  repair.deployNum = 1;
  mstPostTaskAction(&actionQueue_, &repair, STREAM_ACT_DEPLOY);
  HandleActions();
  EXPECT_EQ(0, status->stopped);
  ASSERT_NE(nullptr, status->triggerTask);
  EXPECT_NE(repair.generation, status->triggerTask->id.taskId);
  EXPECT_EQ(1, status->deployTimes);
  ASSERT_NO_FATAL_FAILURE(checkGeneration(1));
}

TEST_F(MndStreamDeployOwnershipTest, StaleActionCannotDeploySameNameReplacement) {
  QueueDeploy(kStreamB);
  HandleActions();

  EXPECT_EQ(taosHashGetSize(mStreamMgmt.streamMap), 0);
  EXPECT_EQ(taosHashGetSize(mStreamMgmt.taskMap), 0);
}

TEST_F(MndStreamDeployOwnershipTest, StaleActionPreservesReplacementAndContinuesQueue) {
  SStmStatus *replacement = AddStatus(kStreamA);
  ASSERT_NE(replacement, nullptr);
  replacement->deployTimes = 7;
  QueueDeploy(kStreamB);
  QueueDeploy(kStreamA);
  HandleActions();

  EXPECT_EQ(mStreamMgmt.actionQ->qRemainNum, 0);
  EXPECT_EQ(replacement->deployTimes, 8);
  EXPECT_EQ(replacement->stopped, 0);
  EXPECT_NE(replacement->triggerTask, nullptr);
  EXPECT_EQ(taosHashGetSize(mStreamMgmt.streamMap), 1);
}

TEST_F(MndStreamDeployOwnershipTest, SuccessfulInsertTransfersStatusOwnership) {
  QueueDeploy(kStreamA);
  HandleActions();

  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(status, nullptr);
  EXPECT_STREQ(status->streamName, Stream(kStreamA)->name);
  EXPECT_NE(status->pCreate, Stream(kStreamA)->pCreate);
  ASSERT_NE(status->pCreate, nullptr);
  EXPECT_EQ(status->pCreate->triggerType, WINDOW_TYPE_INTERVAL);
  EXPECT_NE(status->triggerTask, nullptr);
}

TEST_F(MndStreamDeployOwnershipTest, FailedHeartbeatPinDeliversOriginalTaskOnRetry) {
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(status, nullptr);
  ASSERT_NE(status->triggerTask, nullptr);
  const int64_t expectedTask = status->triggerTask->id.taskId;

  {
    Stub fail;
    fail.set(
        taosArrayInit, +[](size_t size, size_t elemSize) -> SArray * {
          if (elemSize == sizeof(SStmDeployInput *)) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            return nullptr;
          }
          auto *array = taosArrayInit_s(elemSize, (std::max)(size, static_cast<size_t>(TARRAY_MIN_SIZE)));
          if (array) array->size = 0;
          return array;
        });
    EXPECT_TRUE(HeartbeatTriggerIds(TSDB_CODE_OUT_OF_MEMORY).empty());
  }
  EXPECT_EQ(std::vector<int64_t>{expectedTask}, HeartbeatTriggerIds(0));
}

TEST_F(MndStreamDeployOwnershipTest, LaterPinFailurePreservesBothStreamsAcrossHeartbeats) {
  QueueDeploy(kStreamA);
  HandleActions();
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
  HandleActions();
  auto *first = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  auto *second = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  ASSERT_NE(nullptr, first->triggerTask);
  ASSERT_NE(nullptr, second->triggerTask);
  const int64_t firstTask = first->triggerTask->id.taskId;
  const int64_t secondTask = second->triggerTask->id.taskId;
  auto         *input = second->deployInput;
  second->deployInput = nullptr;
  EXPECT_EQ(std::vector<int64_t>{firstTask}, HeartbeatTriggerIds(0));
  second->deployInput = input;
  EXPECT_EQ(std::vector<int64_t>{secondTask}, HeartbeatTriggerIds(0));
}

// LSan checks that failed initialization/insertion releases the unregistered status.
TEST_F(MndStreamDeployOwnershipTest, DeployInsertFailureReleasesStatus) {
  QueueDeploy(kStreamA);
  stub_.set(taosHashPut, FailHashPut);
  HandleActions();
  EXPECT_EQ(taosHashGetSize(mStreamMgmt.streamMap), 0);
}

TEST_F(MndStreamDeployOwnershipTest, DeployCloneFailureReleasesStatus) {
  QueueDeploy(kStreamA);
  FailClone();
  HandleActions();
  EXPECT_EQ(taosHashGetSize(mStreamMgmt.streamMap), 0);
}

#ifdef BUILD_TEST_SEAM
class DeployPhasePause : public PrivateBuildPause {
 public:
  explicit DeployPhasePause(const char *phase) : PrivateBuildPause(false), previous_(gTdSeam_msmDeployPhase) {
    phase_ = phase;
    calls_.store(0);
    gTdSeam_msmDeployPhase = [](const char *phase, const char *) {
      if (strcmp(phase_, phase) == 0 && calls_.fetch_add(1) == 0) Pause();
    };
  }
  ~DeployPhasePause() { gTdSeam_msmDeployPhase = previous_; }
  int Calls() const { return calls_.load(); }

 private:
  decltype(gTdSeam_msmDeployPhase) previous_;
  static const char               *phase_;
  static std::atomic<int>          calls_;
};
const char      *DeployPhasePause::phase_;
std::atomic<int> DeployPhasePause::calls_{0};

TEST_F(MndStreamDeployOwnershipTest, ConcurrentWatchDeferralsReuseCachedDecision) {
  Stream(kStreamA)->pCreate->vtableCalc = true;
  DeployPhasePause pause("watch-defer");
  std::atomic<int32_t> firstCode{TSDB_CODE_OUT_OF_MEMORY};
  std::thread first([&] { firstCode = WatchTask(); });
  if (!pause.Entered()) {
    pause.Release();
    first.join();
    FAIL() << "WATCH deferral did not reach pause";
  }
  EXPECT_EQ(0, WatchTask());
  EXPECT_NE(nullptr, mStreamMgmt.watch.deferredStreams);
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.watch.deferredStreams));

  static std::atomic<int> duplicatePuts{0};
  duplicatePuts = 0;
  stub_.set(taosHashPut, +[](SHashObj *, const void *, size_t, const void *, size_t) -> int32_t {
    ++duplicatePuts;
    return TSDB_CODE_DUP_KEY;
  });
  pause.Release();
  first.join();
  stub_.reset(taosHashPut);
  EXPECT_EQ(0, firstCode.load());
  EXPECT_EQ(0, duplicatePuts.load());
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.watch.deferredStreams));
}


TEST_F(MndStreamDeployOwnershipTest, RunnerPartialPublicationFailsBeforeCleanupHeartbeat) {
  ASSERT_NO_FATAL_FAILURE(SetRunnerPlan(false));
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  ASSERT_GE(status->runnerDeploys, 2);
  std::vector<int64_t> initial;
  HeartbeatTriggerIds(0, &initial);
  ASSERT_FALSE(initial.empty());
  // Keep an empty published queue so both appends use the same injection point.
  const int32_t node = 1;
  if (!taosHashGet(mStreamMgmt.toDeploySnodeMap, &node, sizeof(node))) {
    SStmSnodeTasksDeploy empty = {};
    ASSERT_EQ(0, taosHashPut(mStreamMgmt.toDeploySnodeMap, &node, sizeof(node), &empty, sizeof(empty)));
  }
  SStmTaskAction repair = {};
  repair.streamId = kStreamA;
  repair.generation = status->triggerTask->id.taskId;
  repair.type = STREAM_RUNNER_TASK;
  repair.multiRunner = true;
  repair.deployNum = 2;
  repair.deployId[1] = 1;
  ASSERT_EQ(0, mstPostTaskAction(&actionQueue_, &repair, STREAM_ACT_DEPLOY));
  static int attempts;
  attempts = 0;
  stub_.set(taosArrayAddBatch, +[](SArray *array, const void *data, int32_t count) -> void * {
    const int32_t node = 1;
    auto *queue = static_cast<SStmSnodeTasksDeploy *>(taosHashGet(mStreamMgmt.toDeploySnodeMap, &node, sizeof(node)));
    if (queue && array == queue->runnerList && ++attempts == 2) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return nullptr;
    }
    void *slot = taosArrayReserve(array, count);
    return slot ? memcpy(slot, data, count * array->elemSize) : nullptr;
  });
  DeployPhasePause pause("runner-cleanup");
  std::thread worker([&] { HandleActions(); });
  if (!pause.Entered()) {
    pause.Release();
    worker.join();
    FAIL() << "Runner repair did not reach cleanup";
  }
  EXPECT_EQ(2, attempts);
  EXPECT_EQ(1, status->stopped);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, status->fatalError);
  std::vector<int64_t> partial;
  HeartbeatTriggerIds(0, &partial);
  EXPECT_TRUE(partial.empty());
  pause.Release();
  worker.join();
  EXPECT_EQ(1, status->stopped);
}

void MndStreamDeployOwnershipTest::CheckRunnerCleanupReplacement(bool rebuild) {
  ASSERT_NO_FATAL_FAILURE(SetLegacyMetadataPlan());
  if (rebuild) {
    stub_.set(mndGetVgroupEpsetById, +[](SMnode *, int32_t vg) -> SEpSet {
      SEpSet ep = {};
      ep.numOfEps = 1;
      ep.eps[0].port = 6000 + vg;
      tstrncpy(ep.eps[0].fqdn, "node1", sizeof(ep.eps[0].fqdn));
      return ep;
    });
  }
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  ASSERT_EQ(rebuild ? 3 : 1, listNEles(status->calcReaders));
  std::vector<int64_t> initial;
  HeartbeatTriggerIds(0, &initial);
  ASSERT_FALSE(initial.empty());
  stub_.set(mndGetVgroupEpsetById, +[](SMnode *, int32_t vg) -> SEpSet {
    SEpSet ep = {};
    if (vg == 20) {
      ep.numOfEps = 1;
      ep.eps[0].port = 6020;
      tstrncpy(ep.eps[0].fqdn, "node1", sizeof(ep.eps[0].fqdn));
    }
    return ep;
  });
  if (!rebuild) {
    stub_.set(mndAcquireVgroup, +[](SMnode *, int32_t) -> SVgObj * {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return nullptr;
    });
  }
  SStmTaskAction repair = {};
  repair.streamId = kStreamA;
  repair.generation = status->triggerTask->id.taskId;
  repair.type = STREAM_RUNNER_TASK;
  repair.multiRunner = true;
  repair.deployNum = 1;
  ASSERT_EQ(0, mstPostTaskAction(&actionQueue_, &repair, STREAM_ACT_DEPLOY));
  auto *replacementTrigger = static_cast<SStmTaskStatus *>(taosMemoryCalloc(1, sizeof(SStmTaskStatus)));
  ASSERT_NE(nullptr, replacementTrigger);
  const int64_t replacementId = status->triggerTask->id.taskId + 10000;
  replacementTrigger->id.taskId = replacementId;
  replacementTrigger->id.nodeId = 1;
  replacementTrigger->type = STREAM_TRIGGER_TASK;
  replacementTrigger->status = STREAM_STATUS_RUNNING;
  DeployPhasePause pause("runner-cleanup");
  std::thread worker([&] { HandleActions(); });
  if (!pause.Entered()) {
    pause.Release();
    worker.join();
    taosMemoryFree(replacementTrigger);
    FAIL() << "Runner repair did not reach cleanup";
  }
  EXPECT_EQ(0, status->stopped);
  // Model watch/runtime replacement after the old result was validated, during private cleanup.
  taosWLockLatch(&mStreamMgmt.runtimeLock);
  void *node = nullptr;
  while ((node = taosHashIterate(mStreamMgmt.snodeMap, node))) {
    auto *snode = static_cast<SStmSnodeStatus *>(node);
    if (snode->streamTasks && taosHashGet(snode->streamTasks, &kStreamA, sizeof(kStreamA))) {
      EXPECT_EQ(0, taosHashRemove(snode->streamTasks, &kStreamA, sizeof(kStreamA)));
    }
  }
  while ((node = taosHashIterate(mStreamMgmt.vgroupMap, node))) {
    auto *vgroup = static_cast<SStmVgroupStatus *>(node);
    if (vgroup->streamTasks && taosHashGet(vgroup->streamTasks, &kStreamA, sizeof(kStreamA))) {
      EXPECT_EQ(0, taosHashRemove(vgroup->streamTasks, &kStreamA, sizeof(kStreamA)));
    }
  }
  taosHashClear(mStreamMgmt.taskMap);
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  mstDestroySStmStatus(status);
  EXPECT_EQ(0, taosHashRemove(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  auto *replacement = AddStatus(kStreamA);
  EXPECT_NE(nullptr, replacement);
  if (replacement) {
    replacement->triggerTask = replacementTrigger;
    replacementTrigger->pStream = replacement;
    int64_t taskKey[2] = {kStreamA, replacementId};
    EXPECT_EQ(0, taosHashPut(mStreamMgmt.taskMap, taskKey, sizeof(taskKey), &replacementTrigger, POINTER_BYTES));
    replacement->registrations = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    EXPECT_NE(nullptr, replacement->registrations);
    const uint8_t present = 1;
    int64_t taskRegistration[2] = {0, replacementId};
    int64_t snodeRegistration[2] = {2, replacementTrigger->id.nodeId};
    if (replacement->registrations) {
      EXPECT_EQ(0, taosHashPut(replacement->registrations, taskRegistration, sizeof(taskRegistration), &present,
                               sizeof(present)));
      EXPECT_EQ(0, taosHashPut(replacement->registrations, snodeRegistration, sizeof(snodeRegistration), &present,
                               sizeof(present)));
    }
    const int32_t snodeId = 1;
    auto *snode = static_cast<SStmSnodeStatus *>(taosHashGet(mStreamMgmt.snodeMap, &snodeId, sizeof(snodeId)));
    if (snode && snode->streamTasks) {
      SStmSnodeStreamStatus nodeStream = {};
      nodeStream.trigger = replacementTrigger;
      EXPECT_EQ(0, taosHashPut(snode->streamTasks, &kStreamA, sizeof(kStreamA), &nodeStream, sizeof(nodeStream)));
    }
    if (replacement->registrations) {
      EXPECT_EQ(2, taosHashGetSize(replacement->registrations));
      EXPECT_NE(nullptr, taosHashGet(replacement->registrations, taskRegistration, sizeof(taskRegistration)));
      EXPECT_NE(nullptr, taosHashGet(replacement->registrations, snodeRegistration, sizeof(snodeRegistration)));
    }
  } else {
    taosMemoryFree(replacementTrigger);
  }
  taosWUnLockLatch(&mStreamMgmt.runtimeLock);
  pause.Release();
  worker.join();
  ASSERT_NE(nullptr, replacement);
  EXPECT_EQ(replacement, taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  EXPECT_EQ(0, replacement->stopped);
  EXPECT_EQ(0, replacement->fatalError);
  ASSERT_NE(nullptr, replacement->triggerTask);
  EXPECT_EQ(replacementId, replacement->triggerTask->id.taskId);
  EXPECT_EQ(STREAM_STATUS_RUNNING, replacement->triggerTask->status);
  int64_t replacementKey[2] = {kStreamA, replacementId};
  auto **registered = static_cast<SStmTaskStatus **>(taosHashGet(mStreamMgmt.taskMap, replacementKey, sizeof(replacementKey)));
  ASSERT_NE(nullptr, registered);
  EXPECT_EQ(replacementTrigger, *registered);
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.taskMap));
  ASSERT_NE(nullptr, replacement->registrations);
  int64_t taskRegistration[2] = {0, replacementId};
  int64_t snodeRegistration[2] = {2, replacementTrigger->id.nodeId};
  EXPECT_NE(nullptr, taosHashGet(replacement->registrations, taskRegistration, sizeof(taskRegistration)));
  EXPECT_NE(nullptr, taosHashGet(replacement->registrations, snodeRegistration, sizeof(snodeRegistration)));
  EXPECT_EQ(2, taosHashGetSize(replacement->registrations));
  const int32_t snodeId = 1;
  auto *snode = static_cast<SStmSnodeStatus *>(taosHashGet(mStreamMgmt.snodeMap, &snodeId, sizeof(snodeId)));
  ASSERT_NE(nullptr, snode);
  ASSERT_NE(nullptr, snode->streamTasks);
  auto *nodeStream = static_cast<SStmSnodeStreamStatus *>(taosHashGet(snode->streamTasks, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, nodeStream);
  EXPECT_EQ(replacementTrigger, nodeStream->trigger);
}

TEST_F(MndStreamDeployOwnershipTest, RunnerCleanupDiscardsOldErrorAfterRuntimeReplacement) {
  CheckRunnerCleanupReplacement(false);
}

TEST_F(MndStreamDeployOwnershipTest, RunnerCleanupDiscardsOldRedeployAfterRuntimeReplacement) {
  CheckRunnerCleanupReplacement(true);
}

#ifdef TD_ENTERPRISE
static std::atomic<int32_t> deployGrantCode{0};
static int32_t              currentDeployGrant(EGrantType) { return deployGrantCode.load(); }

TEST_F(MndStreamDeployOwnershipTest, GrantExpiryDuringFirstCopySkipsQueuedDeployUntilRenewal) {
  deployGrantCode.store(0);
  stub_.set(grantCheckExpire, currentDeployGrant);
  stub_.set(sdbCheckExists, reportStreamExists);
  auto *existing = AddStatus(kStreamB);
  ASSERT_NE(nullptr, existing);
  QueueDeploy(kStreamA);
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamB, Stream(kStreamB)->name, nullptr, false, STREAM_ACT_DEPLOY));
  DeployPhasePause pause("first-copy");
  std::thread      worker([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  deployGrantCode.store(TSDB_CODE_GRANT_STREAM_EXPIRED);
  EXPECT_EQ(TSDB_CODE_GRANT_STREAM_EXPIRED, msmHandleGrantExpired(&mnode_, TSDB_CODE_GRANT_STREAM_EXPIRED));
  pause.Release();
  worker.join();
  EXPECT_EQ(nullptr, taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  EXPECT_TRUE(MST_IS_GRANT_STOPPED(existing->stopped));
  EXPECT_EQ(nullptr, existing->triggerTask);
  EXPECT_TRUE(HeartbeatTriggerIds(0).empty());
  deployGrantCode.store(0);
  ASSERT_EQ(0, mstPostStreamCreate(kStreamA, Stream(kStreamA)->name));
  msmCheckLoopStreamMap(&mnode_);
  HandleActions();
  EXPECT_EQ(0, existing->stopped);
  EXPECT_EQ(2u, HeartbeatTriggerIds(0).size());
}

TEST_F(MndStreamDeployOwnershipTest, ExpiryDuringDraftPreventsPublicationBeforeExpiryHeartbeat) {
  deployGrantCode.store(0);
  stub_.set(grantCheckExpire, currentDeployGrant);
  stub_.set(sdbCheckExists, reportStreamExists);
  QueueDeploy(kStreamA);
  DeployPhasePause pause("recalc-restore");
  std::thread      worker([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  deployGrantCode.store(TSDB_CODE_GRANT_BASIC_EXPIRED);
  pause.Release();
  worker.join();
  auto *owner = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, owner);
  EXPECT_TRUE(MST_IS_GRANT_STOPPED(owner->stopped));
  EXPECT_EQ(nullptr, owner->triggerTask);
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_TRUE(HeartbeatTriggerIds(0).empty());
  deployGrantCode.store(0);
  msmCheckLoopStreamMap(&mnode_);
  HandleActions();
  EXPECT_EQ(0, owner->stopped);
  EXPECT_EQ(1u, HeartbeatTriggerIds(0).size());
}

TEST_F(MndStreamDeployOwnershipTest, WatchFirstCopyCannotAdoptAfterGrantExpires) {
  deployGrantCode.store(0);
  stub_.set(grantCheckExpire, currentDeployGrant);
  SStreamHbMsg hb = {};
  hb.dnodeId = hb.snodeId = 1;
  hb.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 1);
  ASSERT_NE(nullptr, hb.pStreamStatus);
  auto *task = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, 0));
  task->type = STREAM_TRIGGER_TASK;
  task->streamId = kStreamA;
  task->taskId = 105;
  task->seriousId = 65;
  task->nodeId = 1;
  task->status = STREAM_STATUS_RUNNING;

  const auto watch = mStreamMgmt.watch;
  mStreamMgmt.watch = {};
  mStreamMgmt.state = MND_STM_STATE_WATCH;
  const int64_t    now = MND_STREAM_GET_LAST_TS(STM_EVENT_ACTIVE_BEGIN);
  auto             report = [&] { ReadHeartbeatDeploys(hb, 0, [](const SStmStreamDeploy &) {}, now); };
  DeployPhasePause pause("watch-copy");
  std::thread      first(report);
  EXPECT_TRUE(pause.Entered());
  deployGrantCode.store(TSDB_CODE_GRANT_STREAM_EXPIRED);
  EXPECT_EQ(TSDB_CODE_GRANT_STREAM_EXPIRED, msmHandleGrantExpired(&mnode_, TSDB_CODE_GRANT_STREAM_EXPIRED));
  pause.Release();
  first.join();
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.streamMap));
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
  deployGrantCode.store(0);
  report();
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.taskMap));
  mStreamMgmt.watch = watch;
  taosArrayDestroy(hb.pStreamStatus);
}

TEST_F(MndStreamDeployOwnershipTest, ExternalReaderCopiesAllowHeartbeatAndStatusReads) {
  ASSERT_NO_FATAL_FAILURE(SetExternalReaders());
  ASSERT_NE(nullptr, AddStatus(kStreamB));
  auto *block = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(nullptr, block);
  SnodeLookupStubGuard snode;
  DeployPhasePause     pause("ext-reader-copy");
  QueueDeploy(kStreamA);
  std::thread builder([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, Stream(kStreamB), block, 0, 0));
  SStreamHbMsg hb = {};
  hb.dnodeId = hb.snodeId = 1;
  ReadHeartbeatDeploys(hb, 0, [](const SStmStreamDeploy &) { ADD_FAILURE() << "Draft reached the live queue"; });
  auto *lastUp = static_cast<int64_t *>(taosHashGet(mStreamMgmt.dnodeMap, &hb.dnodeId, sizeof(hb.dnodeId)));
  EXPECT_TRUE(lastUp && atomic_load_64(lastUp) == 101);
  auto *src = static_cast<SStreamExtTriggerSpec *>(taosArrayGetP(Stream(kStreamA)->pCreate->extSpecs, 0));
  // Change the inline pair under the same lock used by a credential refresh.
  // The first reader has already captured both fields when its mapping copy pauses.
  taosWLockLatch(&mStreamMgmt.actionQLock);
  src->encryptedPassword[0] = 2;
  tstrncpy(src->options, "second", sizeof(src->options));
  taosWUnLockLatch(&mStreamMgmt.actionQLock);
  pause.Release();
  builder.join();
  blockDataDestroy(block);

  ASSERT_EQ(2, pause.Calls());
  const int32_t node = 1;
  auto *queue = static_cast<SStmSnodeTasksDeploy *>(taosHashGet(mStreamMgmt.toDeploySnodeMap, &node, sizeof(node)));
  ASSERT_NE(nullptr, queue);
  ASSERT_EQ(2u, taosArrayGetSize(queue->calcReaderList));
  auto *previous = src;
  for (int i = 0; i < 2; ++i) {
    auto *entry = static_cast<SStmTaskToDeployExt *>(taosArrayGet(queue->calcReaderList, i));
    auto *copy = entry->deploy.msg.reader.pExtSpec;
    ASSERT_NE(nullptr, copy);
    EXPECT_EQ(i + 1, copy->encryptedPassword[0]);
    EXPECT_STREQ(i == 0 ? "first" : "second", copy->options);
    EXPECT_TRUE(entry->ownsExtSpec);
    EXPECT_TRUE(entry->deploy.msg.reader.ownsExtSpec);
    EXPECT_NE(src->pCalcMappings, copy->pCalcMappings);
    EXPECT_NE(previous, copy);
    EXPECT_NE(previous->pCalcMappings, copy->pCalcMappings);
    ASSERT_NE(nullptr, copy->pCalcMappings);
    ASSERT_EQ(128, copy->numCalcMappings);
    EXPECT_STREQ("value_127", copy->pCalcMappings[127].colName);
    previous = copy;
  }
}

TEST_F(MndStreamDeployOwnershipTest, StopDiscardsPrivateExternalReaderConfigs) {
  ASSERT_NO_FATAL_FAILURE(SetExternalReaders());
  DeployPhasePause pause("ext-reader-copy");
  QueueDeploy(kStreamA);
  std::thread builder([&] { HandleActions(); });
  EXPECT_TRUE(pause.Entered());
  atomic_store_8(&Stream(kStreamA)->userStopped, 1);
  msmUndeployStream(&mnode_, kStreamA, Stream(kStreamA)->name);
  pause.Release();
  builder.join();
  EXPECT_EQ(2, pause.Calls());
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.toDeploySnodeMap));
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
}

TEST_F(MndStreamDeployOwnershipTest, PartialPublicationReleasesQueuedExternalReaderConfigs) {
  ASSERT_NO_FATAL_FAILURE(SetExternalReaders());
  {
    FailPublishedTriggerPush fail;
    QueueDeploy(kStreamA);
    HandleActions();
  }
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, status->fatalError);
  const int32_t node = 1;
  auto *queue = static_cast<SStmSnodeTasksDeploy *>(taosHashGet(mStreamMgmt.toDeploySnodeMap, &node, sizeof(node)));
  ASSERT_NE(nullptr, queue);
  ASSERT_EQ(2, queue->calcReaderDeployed);
  for (int i = 0; i < queue->calcReaderDeployed; ++i) {
    auto *entry = static_cast<SStmTaskToDeployExt *>(taosArrayGet(queue->calcReaderList, i));
    EXPECT_TRUE(entry->deployed);
    EXPECT_FALSE(entry->ownsExtSpec);
    EXPECT_EQ(nullptr, entry->deploy.msg.reader.pExtSpec);
  }
  EXPECT_EQ(0, taosHashGetSize(mStreamMgmt.taskMap));
}
#endif

TEST_F(MndStreamDeployOwnershipTest, WatchFirstCopyAllowsStatusQueryAndConcurrentAdoption) {
  ASSERT_NE(nullptr, AddStatus(kStreamB));
  auto *block = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(nullptr, block);
  SnodeLookupStubGuard snode;
  SStmTaskStatusMsg    task = {};
  task.type = STREAM_TRIGGER_TASK;
  task.streamId = kStreamA;
  task.taskId = 105;
  task.seriousId = 65;
  task.nodeId = 1;
  task.status = STREAM_STATUS_RUNNING;
  SArray *reports = taosArrayInit(1, sizeof(task));
  ASSERT_NE(nullptr, reports);
  ASSERT_NE(nullptr, taosArrayPush(reports, &task));

  SStmThreadCtx threadCtx = {};
  auto         *oldCtx = mStreamMgmt.tCtx;
  auto          oldThreads = mStreamMgmt.threadNum;
  auto          oldWatch = mStreamMgmt.watch;
  mStreamMgmt.tCtx = &threadCtx;
  mStreamMgmt.threadNum = 1;
  mStreamMgmt.watch = {};
  mStreamMgmt.state = MND_STM_STATE_WATCH;
  auto heartbeat = [&](int group) {
    SStreamHbMsg hb = {};
    hb.streamGId = group;
    hb.dnodeId = 1;
    hb.snodeId = 1;
    hb.pStreamStatus = reports;
    SRpcMsg req = {}, rsp = {};
    int     code = msmHandleStreamHbMsg(&mnode_, MND_STREAM_GET_LAST_TS(STM_EVENT_ACTIVE_BEGIN), &hb, &req, &rsp);
    rpcFreeCont(rsp.pCont);
    return code;
  };

  DeployPhasePause pause("watch-copy");
  std::thread      first([&] { EXPECT_EQ(0, heartbeat(0)); });
  EXPECT_TRUE(pause.Entered());
  EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, Stream(kStreamB), block, 0, 0));
  EXPECT_EQ(0, heartbeat(1));
  pause.Release();
  first.join();
  EXPECT_EQ(2, pause.Calls());
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.taskMap));
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  EXPECT_TRUE(status && status->triggerTask && status->triggerTask->id.taskId == task.taskId);
  mStreamMgmt.tCtx = oldCtx;
  mStreamMgmt.threadNum = oldThreads;
  mStreamMgmt.watch = oldWatch;
  taosArrayDestroy(reports);
  blockDataDestroy(block);
}

void finishRecalcAfterCapture(const char *phase, const char *name) {
  auto *stream = gRecoveryStreams[0];
  if (strcmp(phase, "recalc-restore") || strcmp(name, stream->name)) return;
  // Completing a persisted request after each capture invalidates the real commit check.
  taosWLockLatch(&stream->lock);
  if (taosArrayGetSize(stream->pIncompleteRecalcs) > 1) {
    taosArrayPop(stream->pIncompleteRecalcs);
    ++stream->recalcRevision;
  }
  taosWUnLockLatch(&stream->lock);
}

TEST_F(MndStreamDeployOwnershipTest, WatchRecalcConflictRetriesOnNextHeartbeatAndAdoptsOtherStream) {
  for (int i = 0; i < 3; ++i) AddPersisted(kStreamA, kRecalcId + i);
  auto *a = AddStatus(kStreamA);
  ASSERT_NE(nullptr, a);
  AddRuntimeRecord(a, 11, 22);
  const auto *oldRecords = a->recalcRecords;

  SStreamHbMsg hb = {};
  hb.dnodeId = hb.snodeId = 1;
  hb.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 2);
  ASSERT_NE(nullptr, hb.pStreamStatus);
  for (int i = 0; i < 2; ++i) {
    auto *task = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, i));
    task->type = STREAM_TRIGGER_TASK;
    task->streamId = i == 0 ? kStreamA : kStreamB;
    task->taskId = 201 + i;
    task->seriousId = 1;
    task->nodeId = 1;
    task->status = STREAM_STATUS_RUNNING;
  }

  const auto watch = mStreamMgmt.watch;
  const auto normalBegin = mStreamMgmt.lastTs[STM_EVENT_NORMAL_BEGIN];
  const auto previous = gTdSeam_msmDeployPhase;
  mStreamMgmt.watch = {};
  mStreamMgmt.state = MND_STM_STATE_WATCH;
  gTdSeam_msmDeployPhase = finishRecalcAfterCapture;
  const int64_t begin = MND_STREAM_GET_LAST_TS(STM_EVENT_ACTIVE_BEGIN);
  for (int i = 1; i <= 2; ++i) {
    ReadHeartbeatDeploys(hb, 0, [](const SStmStreamDeploy &) {}, begin + i);
    EXPECT_EQ(i, Stream(kStreamA)->recalcRevision);
    EXPECT_EQ(nullptr, a->triggerTask);
    EXPECT_EQ(oldRecords, a->recalcRecords);
    EXPECT_EQ(0, a->stopped);
    auto *b = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
    EXPECT_TRUE(b && b->triggerTask && b->triggerTask->id.taskId == 202 && b->triggerTask->lastUpTs == begin + i);
    EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.taskMap));
  }

  gTdSeam_msmDeployPhase = previous;
  ReadHeartbeatDeploys(hb, 0, [](const SStmStreamDeploy &) {}, begin + MST_SHORT_ISOLATION_DURATION + 1);
  EXPECT_EQ(MND_STM_STATE_NORMAL, mStreamMgmt.state);
  mStreamMgmt.watch = watch;
  mStreamMgmt.lastTs[STM_EVENT_NORMAL_BEGIN] = normalBegin;
  taosArrayDestroy(hb.pStreamStatus);

  EXPECT_EQ(0, a->stopped);
  ASSERT_NE(nullptr, a->triggerTask);
  EXPECT_EQ(201, a->triggerTask->id.taskId);
  EXPECT_EQ(2, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(1u, taosArrayGetSize(a->recalcRecords));
  auto *restored = FindRecord(a);
  ASSERT_NE(nullptr, restored);
  EXPECT_EQ(201, restored->triggerTaskId);
  EXPECT_EQ(STREAM_RECALC_STATUS_PENDING, restored->snapshot.status);
}

TEST_F(MndStreamDeployOwnershipTest, RecalcConflictsYieldToQueuedCreate) {
  for (int i = 0; i < 6; ++i) AddPersisted(kStreamA, kRecalcId + i);
  QueueDeploy(kStreamA);
  HandleActions();
  auto *a = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, a);
  ASSERT_NE(nullptr, a->triggerTask);
  const auto oldTask = a->triggerTask->id.taskId;
  ASSERT_EQ((std::vector<int64_t>{oldTask}), HeartbeatTriggerIds(0));

  QueueDeploy(kStreamA);
  ASSERT_EQ(0, mstPostStreamCreate(kStreamB, Stream(kStreamB)->name));
  auto previous = gTdSeam_msmDeployPhase;
  gTdSeam_msmDeployPhase = finishRecalcAfterCapture;
  HandleActions();
  gTdSeam_msmDeployPhase = previous;

  auto *b = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  ASSERT_NE(nullptr, b);
  ASSERT_NE(nullptr, a->triggerTask);
  EXPECT_EQ(oldTask, a->triggerTask->id.taskId);
  EXPECT_EQ(0, a->deployTimes);
  EXPECT_EQ(0, a->stopped);
  ASSERT_NE(nullptr, b->triggerTask);
  ASSERT_EQ(0, tsem_init(&mnode_.streamDeploySem, 0, 0));
  EXPECT_EQ((std::vector<int64_t>{b->triggerTask->id.taskId}), HeartbeatTriggerIds(0));
  EXPECT_EQ(0, tsem_timewait(&mnode_.streamDeploySem, 0));
  EXPECT_EQ(0, tsem_destroy(&mnode_.streamDeploySem));
  EXPECT_EQ(2u, Stream(kStreamA)->recalcRevision);
  EXPECT_EQ(1u, actionQueue_.qRemainNum);

  msmDrainStreamActions(&mnode_);
  ASSERT_NE(nullptr, a->triggerTask);
  EXPECT_NE(oldTask, a->triggerTask->id.taskId);
  EXPECT_EQ(1, a->deployTimes);
  EXPECT_EQ(4u, taosArrayGetSize(a->recalcRecords));
  EXPECT_EQ((std::vector<int64_t>{a->triggerTask->id.taskId}), HeartbeatTriggerIds(0));
  EXPECT_EQ(0u, actionQueue_.qRemainNum);
}

TEST_F(MndStreamDeployOwnershipTest, DeferredFirstCreatePublishesOnce) {
  for (int i = 0; i < 3; ++i) AddPersisted(kStreamA, kRecalcId + i);
  ASSERT_EQ(0, mstPostStreamCreate(kStreamA, Stream(kStreamA)->name));
  auto previous = gTdSeam_msmDeployPhase;
  gTdSeam_msmDeployPhase = finishRecalcAfterCapture;
  HandleActions();
  gTdSeam_msmDeployPhase = previous;
  ASSERT_EQ(1u, actionQueue_.qRemainNum);
  ASSERT_EQ(0, mstPostStreamCreate(kStreamA, Stream(kStreamA)->name));
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, status->triggerTask);
  EXPECT_EQ(0, status->deployTimes);
  EXPECT_EQ(1, taosHashGetSize(mStreamMgmt.taskMap));
  EXPECT_EQ(0u, actionQueue_.qRemainNum);
}

TEST_F(MndStreamDeployOwnershipTest, DeferredStartSurvivesHealthScanAndYieldsToStop) {
  auto *status = AddStatus(kStreamA);
  ASSERT_NE(nullptr, status);
  status->stopped = 2;
  for (int i = 0; i < 3; ++i) AddPersisted(kStreamA, kRecalcId + i);
  auto *stream = Stream(kStreamA);
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamA, stream->name, nullptr, true, STREAM_ACT_DEPLOY));
  auto previous = gTdSeam_msmDeployPhase;
  gTdSeam_msmDeployPhase = finishRecalcAfterCapture;
  HandleActions();
  gTdSeam_msmDeployPhase = previous;
  ASSERT_EQ(1u, actionQueue_.qRemainNum);
  msmCheckLoopStreamMap(&mnode_);
  ASSERT_EQ(status, taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));

  msmUndeployStream(&mnode_, kStreamA, stream->name);
  HandleActions();
  EXPECT_EQ(0u, actionQueue_.qRemainNum);
  EXPECT_EQ(2, status->stopped);
  EXPECT_EQ(nullptr, status->triggerTask);
  ASSERT_EQ(0, mstPostStreamAction(&actionQueue_, kStreamA, stream->name, nullptr, true, STREAM_ACT_DEPLOY));
  HandleActions();
  EXPECT_EQ(0, status->stopped);
  EXPECT_NE(nullptr, status->triggerTask);
}

TEST_F(MndStreamDeployOwnershipTest, RecalcRestoreKeepsConcurrentPersistenceAndRuntimeUpdates) {
  constexpr int kRequests = 128;
  for (int i = 0; i < kRequests; ++i) AddPersisted(kStreamA, kRecalcId + i);
  QueueDeploy(kStreamA);
  HandleActions();
  auto *status = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  ASSERT_NE(nullptr, status);
  ASSERT_EQ(kRequests, taosArrayGetSize(status->recalcRecords));
  ASSERT_NE(nullptr, AddStatus(kStreamB));
  auto *block = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(nullptr, block);
  SnodeLookupStubGuard snode;

  enum Update { durableRequest, finishCandidate, oldProgress };
  for (Update update : {durableRequest, finishCandidate, oldProgress}) {
    QueueDeploy(kStreamA);
    DeployPhasePause pause("recalc-restore");
    std::thread      builder([&] { HandleActions(); });
    EXPECT_TRUE(pause.Entered());
    EXPECT_EQ(0, mstSetStreamAttrResBlock(&mnode_, Stream(kStreamB), block, 0, 0));
    if (update == durableRequest) {
      auto *stream = Stream(kStreamA);
      taosWLockLatch(&stream->lock);
      AddPersisted(kStreamA, kRecalcId + kRequests);
      ++stream->recalcRevision;
      taosWUnLockLatch(&stream->lock);
    } else {
      // The real heartbeat path queues FINISH and defers persistence until its outer locks are released.
      auto                 *active = FindRecord(status, update == finishCandidate ? kRecalcId : kRecalcId + 1);
      SStreamRecalcSnapshot report = active->snapshot;
      report.status = update == finishCandidate ? STREAM_RECALC_STATUS_FINISHED : STREAM_RECALC_STATUS_RUNNING;
      report.progressPct = update == finishCandidate ? 100 : 42;
      SArray *snapshots = taosArrayInit(1, sizeof(report));
      EXPECT_NE(nullptr, taosArrayPush(snapshots, &report));
      bool deferred = false;
      EXPECT_EQ(0, mndStreamRecalcApplySnapshotDeferred(&mnode_, kStreamA, status, status->triggerTask->id.taskId,
                                                        status->triggerTask->id.seriousId, false,
                                                        STREAM_RECALC_DETAIL_ABSENT, snapshots, nullptr, &deferred));
      EXPECT_EQ(update == finishCandidate, deferred);
      if (update == oldProgress) EXPECT_EQ(42, active->snapshot.progressPct);
      taosArrayDestroy(snapshots);
    }
    pause.Release();
    builder.join();
    EXPECT_EQ(update == oldProgress ? 1 : 2, pause.Calls());
    EXPECT_EQ(0, FindRecord(status, kRecalcId + 1)->snapshot.progressPct);
    EXPECT_FALSE(FindRecord(status, kRecalcId + 1)->dispatchConfirmed);
    EXPECT_EQ(0, status->stopped);
    EXPECT_EQ(kRequests + 1, taosArrayGetSize(status->recalcRecords));
    EXPECT_NE(nullptr, FindRecord(status, kRecalcId + kRequests));
  }
  auto *record = FindRecord(status);
  ASSERT_NE(nullptr, record);
  EXPECT_TRUE(record->terminalCandidateValid);
  EXPECT_TRUE(record->terminalPersisting);
  EXPECT_EQ(STREAM_RECALC_STATUS_FINISHED, record->terminalCandidate.status);
  blockDataDestroy(block);
}
#endif

TEST_F(MndStreamDeployOwnershipTest, WatchTriggerConflictKeepsOtherStreamRunning) {
  SStmSnodeStatus snode = {};
  int32_t         node = 2;
  int64_t         lastUp = 0;
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.snodeMap, &node, sizeof(node), &snode, sizeof(snode)));
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.dnodeMap, &node, sizeof(node), &lastUp, sizeof(lastUp)));
  SStreamHbMsg hb = {};
  hb.pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), 1);
  auto *task = static_cast<SStmTaskStatusMsg *>(taosArrayGet(hb.pStreamStatus, 0));
  ASSERT_NE(nullptr, task);
  auto watch = mStreamMgmt.watch;
  auto normalBegin = mStreamMgmt.lastTs[STM_EVENT_NORMAL_BEGIN];
  auto errorTs = mStreamMgmt.lastTs[STM_EVENT_STM_TERR];
  mStreamMgmt.watch = {};
  mStreamMgmt.state = MND_STM_STATE_WATCH;
  int64_t now = MND_STREAM_GET_LAST_TS(STM_EVENT_ACTIVE_BEGIN);
  auto    report = [&](int64_t streamId, int64_t taskId, int32_t nodeId) {
    task->streamId = streamId;
    task->type = STREAM_TRIGGER_TASK;
    task->taskId = taskId;
    task->seriousId = 1;
    task->nodeId = hb.dnodeId = hb.snodeId = nodeId;
    task->status = STREAM_STATUS_RUNNING;
    ReadHeartbeatDeploys(hb, 0, [](const SStmStreamDeploy &) {}, now);
  };
  report(kStreamB, 201, 1);
  report(kStreamA, 202, 1);
  report(kStreamA, 203, 2);
  EXPECT_EQ(MND_STM_STATE_WATCH, mStreamMgmt.state);
  report(kStreamA, 204, 2);
  now += MST_SHORT_ISOLATION_DURATION + 1;
  report(kStreamB, 201, 1);
  EXPECT_EQ(MND_STM_STATE_NORMAL, mStreamMgmt.state);
  auto *failed = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamA, sizeof(kStreamA)));
  auto *healthy = static_cast<SStmStatus *>(taosHashGet(mStreamMgmt.streamMap, &kStreamB, sizeof(kStreamB)));
  EXPECT_TRUE(failed && failed->stopped && failed->fatalError == TSDB_CODE_MND_STREAM_TASK_LOST);
  EXPECT_TRUE(healthy && !healthy->stopped && healthy->triggerTask);
  int64_t key[] = {kStreamB, 201};
  auto  **entry = static_cast<SStmTaskStatus **>(taosHashGet(mStreamMgmt.taskMap, key, sizeof(key)));
  EXPECT_TRUE(entry && healthy && *entry == healthy->triggerTask && (*entry)->id.taskId == 201 &&
              (*entry)->status == STREAM_STATUS_RUNNING && (*entry)->lastUpTs == now);
  EXPECT_EQ(2, taosHashGetSize(mStreamMgmt.taskMap));
  taosArrayDestroy(hb.pStreamStatus);
  mStreamMgmt.watch = watch;
  mStreamMgmt.lastTs[STM_EVENT_NORMAL_BEGIN] = normalBegin;
  mStreamMgmt.lastTs[STM_EVENT_STM_TERR] = errorTs;
}

TEST_F(MndStreamDeployOwnershipTest, WatchInsertFailureReleasesStatus) {
  Stream(kStreamA)->pCreate->triggerTblType = TSDB_NORMAL_TABLE;
  stub_.set(taosHashPut, FailHashPut);
  EXPECT_EQ(WatchTask(), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(taosHashGetSize(mStreamMgmt.streamMap), 0);
}

TEST_F(MndStreamDeployOwnershipTest, WatchCloneFailureReleasesStatus) {
  FailClone();
  EXPECT_EQ(WatchTask(), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(taosHashGetSize(mStreamMgmt.streamMap), 0);
}

TEST_F(MndStreamRecalcRecoveryTest, NewRuntimeRestoresOriginalIdRangeAndRequestTime) {
  AddPersisted(kStreamA);
  SStmStatus status = {};
  SStreamRecalcRestore restore = {};
  ASSERT_EQ(0, mndStreamRecalcCaptureRecords(&status, &restore));
  ASSERT_EQ(0, mndStreamRecalcPrepareRestore(Stream(kStreamA), &restore, 55, 66));
  ASSERT_TRUE(mndStreamRecalcCommitRestore(Stream(kStreamA), &status, &restore));
  taosArrayDestroy(restore.records);

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

  atomic_store_8(&actionQueue_.draining, 1);
  msmDrainStreamActions(&mnode_);

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

TEST_F(MndStreamRecalcRecoveryTest, RepeatWatchReportPreservesCurrentTriggerProgress) {
  AddPersisted(kStreamA);
  SStmGrpCtx ctx = {};
  ctx.pMnode = &mnode_;
  SStmTaskStatusMsg task = {};
  task.type = STREAM_TRIGGER_TASK;
  task.streamId = kStreamA;
  task.taskId = 55;
  task.seriousId = 66;
  task.nodeId = 1;
  task.status = STREAM_STATUS_RUNNING;
  SStmStatus *status = AddStatus(kStreamA);
  ASSERT_NE(nullptr, status);
  ASSERT_EQ(0, msmWatchRecordNewTask(&ctx, &task));
  const auto *record = FindRecord(status);
  ASSERT_NE(nullptr, record);
  SStreamRecalcSnapshot progress = record->snapshot;
  progress.status = STREAM_RECALC_STATUS_RUNNING;
  progress.progressPct = 64;
  SArray *snapshots = taosArrayInit(1, sizeof(progress));
  ASSERT_NE(nullptr, snapshots);
  ASSERT_NE(nullptr, taosArrayPush(snapshots, &progress));
  ASSERT_EQ(0, mndStreamRecalcApplySnapshot(&mnode_, kStreamA, status, task.taskId, task.seriousId, false,
                                            STREAM_RECALC_DETAIL_ABSENT, snapshots, nullptr));
  taosArrayDestroy(snapshots);
  ASSERT_EQ(0, msmWatchRecordNewTask(&ctx, &task));
  record = FindRecord(status);
  ASSERT_NE(nullptr, record);
  EXPECT_EQ(STREAM_RECALC_STATUS_RUNNING, record->snapshot.status);
  EXPECT_EQ(64, record->snapshot.progressPct);
  EXPECT_TRUE(record->dispatchConfirmed);
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
    SStreamRecalcRestore      restore = {};
    code = mndStreamRecalcCaptureRecords(&status, &restore);
    if (!code) code = mndStreamRecalcPrepareRestore(Stream(kStreamA), &restore, 55, 66);
    EXPECT_TRUE(!code && mndStreamRecalcCommitRestore(Stream(kStreamA), &status, &restore));
    taosArrayDestroy(restore.records);
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

TEST(StreamObjTest, OnlyAlignedDefinitionsRequireTheNewStorageFrame) {
  SStreamObj                                             input = BuildMinimalStreamObj();
  std::unique_ptr<SStreamObj, decltype(&tFreeStreamObj)> guard(&input, tFreeStreamObj);
  ASSERT_NE(input.pCreate, nullptr);
  EXPECT_EQ(TSDB_CODE_SUCCESS, DecodeLegacyStreamObjAndConsumeOuterObject(EncodeStreamObj(input)));

  input.pCreate->addOptions = STREAM_OPTION_TIME_ALIGN;
  auto *spec = static_cast<SStreamTimeAlignSpec *>(taosMemoryCalloc(1, sizeof(SStreamTimeAlignSpec)));
  ASSERT_NE(spec, nullptr);
  input.pCreate->pTimeAlign = spec;
  spec->timezone = taosStrdup("UTC");
  col_id_t               cid = 2;
  SStreamTimeAlignColumn rule = {};
  rule.srcColId = cid;
  rule.type.type = TSDB_DATA_TYPE_INT;
  rule.type.bytes = sizeof(int32_t);
  tstrncpy(rule.outName, "prev_a", sizeof(rule.outName));
  spec->pRowsetColIds = taosArrayInit(1, sizeof(cid));
  spec->pColumns = taosArrayInit(1, sizeof(rule));
  ASSERT_NE(nullptr, taosArrayPush(spec->pRowsetColIds, &cid));
  ASSERT_NE(nullptr, taosArrayPush(spec->pColumns, &rule));

  auto bytes = EncodeStreamObj(input);
  ASSERT_FALSE(bytes.empty());
  EXPECT_EQ(TSDB_CODE_OUT_OF_RANGE, DecodeLegacyStreamObjAndConsumeOuterObject(bytes));
  SStreamObj                                             output = DecodeStreamObj(bytes, MND_STREAM_VER_NUMBER);
  std::unique_ptr<SStreamObj, decltype(&tFreeStreamObj)> outputGuard(&output, tFreeStreamObj);
  ASSERT_NE(nullptr, output.pCreate);
  ASSERT_NE(nullptr, output.pCreate->pTimeAlign);
  ASSERT_EQ(1, taosArrayGetSize(output.pCreate->pTimeAlign->pColumns));
  auto *decoded = static_cast<SStreamTimeAlignColumn *>(taosArrayGet(output.pCreate->pTimeAlign->pColumns, 0));
  EXPECT_EQ(cid, decoded->srcColId);
  EXPECT_STREQ("prev_a", decoded->outName);
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

TEST(StreamTimeConfigTest, RejectsIncompleteUserRequest) {
  SCMCreateStreamReq req = {0};
  req.tsmaId = 0;
  EXPECT_EQ(TSDB_CODE_MND_STREAM_INVALID_TIME_CONFIG, mndValidateStreamTimeConfig(&req));

  tstrncpy(req.timeConfig.timezone, "UTC", sizeof(req.timeConfig.timezone));
  req.timeConfig.firstDayOfWeek = -1;
  EXPECT_EQ(TSDB_CODE_MND_STREAM_INVALID_TIME_CONFIG, mndValidateStreamTimeConfig(&req));

  req.timeConfig.firstDayOfWeek = 7;
  EXPECT_EQ(TSDB_CODE_MND_STREAM_INVALID_TIME_CONFIG, mndValidateStreamTimeConfig(&req));

  req.timeConfig.firstDayOfWeek = 0;
  EXPECT_EQ(TSDB_CODE_SUCCESS, mndValidateStreamTimeConfig(&req));
}

TEST(StreamTimeConfigTest, ExemptsTsmaBackingStream) {
  SCMCreateStreamReq req = {0};
  req.tsmaId = 42;
  req.timeConfig.firstDayOfWeek = 127;
  EXPECT_EQ(TSDB_CODE_SUCCESS, mndValidateStreamTimeConfig(&req));
}

TEST(StreamTimeConfigTest, ReportsDistinctValidationAndLoadErrors) {
  EXPECT_NE(TSDB_CODE_MND_STREAM_INVALID_TIME_CONFIG, TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED);
  EXPECT_STRNE(tstrerror(TSDB_CODE_MND_STREAM_INVALID_TIME_CONFIG), tstrerror(TSDB_CODE_STREAM_TIMEZONE_LOAD_FAILED));
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

TEST_F(MndStreamTest, StreamSchemaPreservesMetricIndexesAndAppendsTimeConfig) {
  const SSysTableMeta *pStreams = getSysTableMeta("information_schema", TSDB_INS_TABLE_STREAMS);
  ASSERT_NE(pStreams, nullptr);
  ASSERT_EQ(pStreams->colNum, 17);
  EXPECT_STREQ(pStreams->schema[kStreamRealtimeLagColumn].name, "realtime_lag_ms");
  EXPECT_EQ(pStreams->schema[kStreamRealtimeLagColumn].type, TSDB_DATA_TYPE_BIGINT);
  EXPECT_STREQ(pStreams->schema[kStreamInputRateColumn].name, "input_rows_per_sec_1m");
  EXPECT_EQ(pStreams->schema[kStreamInputRateColumn].type, TSDB_DATA_TYPE_DOUBLE);
  EXPECT_STREQ(pStreams->schema[kStreamOutputRateColumn].name, "output_rows_per_sec_1m");
  EXPECT_EQ(pStreams->schema[kStreamOutputRateColumn].type, TSDB_DATA_TYPE_DOUBLE);
  EXPECT_STREQ(pStreams->schema[kStreamResultLatencyColumn].name, "runner_result_latency_avg_1m_ms");
  EXPECT_EQ(pStreams->schema[kStreamResultLatencyColumn].type, TSDB_DATA_TYPE_DOUBLE);
  EXPECT_STREQ(pStreams->schema[kStreamHistoryProgressColumn].name, "history_progress_pct");
  EXPECT_EQ(pStreams->schema[kStreamHistoryProgressColumn].type, TSDB_DATA_TYPE_INT);
  EXPECT_STREQ(pStreams->schema[kStreamTimezoneColumn].name, "timezone");
  EXPECT_EQ(pStreams->schema[kStreamTimezoneColumn].bytes, TD_TIMEZONE_LEN + VARSTR_HEADER_SIZE);
  EXPECT_EQ(pStreams->schema[kStreamTimezoneColumn].type, TSDB_DATA_TYPE_VARCHAR);
  EXPECT_STREQ(pStreams->schema[kStreamFirstDayOfWeekColumn].name, "first_day_of_week");
  EXPECT_EQ(pStreams->schema[kStreamFirstDayOfWeekColumn].bytes, 4);
  EXPECT_EQ(pStreams->schema[kStreamFirstDayOfWeekColumn].type, TSDB_DATA_TYPE_INT);

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

TEST_F(MndStreamTest, StreamTimeConfigColumnsPreserveValuesAndLegacyNulls) {
  const SSysTableMeta *pMeta = getSysTableMeta("information_schema", TSDB_INS_TABLE_STREAMS);
  ASSERT_NE(pMeta, nullptr);
  ASSERT_GE(pMeta->colNum, kStreamFirstDayOfWeekColumn + 1);
  SSDataBlock *pBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 2);
  ASSERT_NE(pBlock, nullptr);

  char               streamName[] = "test.stream";
  char               streamDb[] = "test";
  char               sql[] = "select 1";
  SCMCreateStreamReq createReq = {};
  createReq.name = streamName;
  createReq.streamDB = streamDb;
  createReq.sql = sql;
  createReq.streamId = 42;
  tstrncpy(createReq.timeConfig.timezone, "Asia/Shanghai", sizeof(createReq.timeConfig.timezone));
  createReq.timeConfig.firstDayOfWeek = 6;
  SStreamObj stream = {};
  tstrncpy(stream.name, streamName, sizeof(stream.name));
  stream.pCreate = &createReq;

  char               legacyName[] = "test.legacy";
  SCMCreateStreamReq legacyReq = createReq;
  legacyReq.name = legacyName;
  legacyReq.streamId = 43;
  legacyReq.timeConfig.timezone[0] = '\0';
  legacyReq.timeConfig.firstDayOfWeek = 5;
  SStreamObj legacyStream = {};
  tstrncpy(legacyStream.name, legacyName, sizeof(legacyStream.name));
  legacyStream.pCreate = &legacyReq;

  StreamMapGuard streamMap;
  ASSERT_NE(streamMap.get(), nullptr);
  SnodeLookupStubGuard snodeLookup;
  SMnode               mnode = {};
  ASSERT_EQ(mstSetStreamAttrResBlock(&mnode, &stream, pBlock, 0, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(mstSetStreamAttrResBlock(&mnode, &legacyStream, pBlock, 1, 0), TSDB_CODE_SUCCESS);

  EXPECT_FALSE(isNullValue(pBlock, kStreamTimezoneColumn, 0, 2));
  EXPECT_EQ(getVarCharValue(pBlock, kStreamTimezoneColumn, 0), "Asia/Shanghai");
  EXPECT_FALSE(isNullValue(pBlock, kStreamFirstDayOfWeekColumn, 0, 2));
  EXPECT_EQ(getInt32Value(pBlock, kStreamFirstDayOfWeekColumn, 0), 6);
  EXPECT_TRUE(isNullValue(pBlock, kStreamTimezoneColumn, 1, 2));
  EXPECT_TRUE(isNullValue(pBlock, kStreamFirstDayOfWeekColumn, 1, 2));

  blockDataDestroy(pBlock);
}

TEST_F(MndStreamRecalcTest, RuntimeProgressSchemasAppendExpectedColumns) {
  const SSysTableMeta *pStreams = getSysTableMeta("information_schema", TSDB_INS_TABLE_STREAMS);
  ASSERT_NE(pStreams, nullptr);
  EXPECT_STREQ(pStreams->schema[kStreamHistoryProgressColumn].name, "history_progress_pct");
  EXPECT_EQ(pStreams->schema[kStreamHistoryProgressColumn].type, TSDB_DATA_TYPE_INT);

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
  ASSERT_EQ(mstSetStreamAttrResBlock(&mnode, &stream_, pStreamBlock, 0, 0), TSDB_CODE_SUCCESS);
  EXPECT_EQ(getInt32Value(pStreamBlock, kStreamHistoryProgressColumn, 0), 37);
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
  ASSERT_EQ(mstSetStreamAttrResBlock(&mnode, &stream_, pBlock, 0, 0), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(isNullValue(pBlock, kStreamHistoryProgressColumn, 0, 1));
  blockDataDestroy(pBlock);

  mstInvalidateTaskMetrics(&trigger_);
  pBlock = createSystemTableBlock(TSDB_INS_TABLE_STREAMS, 1);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(mstSetStreamAttrResBlock(&mnode, &stream_, pBlock, 0, 0), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(isNullValue(pBlock, kStreamHistoryProgressColumn, 0, 1));
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
  auto *reader = appendTask(&topology.trigReaders, STREAM_READER_TASK, STREAM_FLAG_TRIGGER_READER);
  ASSERT_NE(nullptr, reader);
  reader->metricsValid = true;
  reader->metrics.windowReady = true;
  reader->metrics.validMask = STREAM_METRIC_PHYSICAL_INPUT;
  reader->metrics.physicalInputRows1m = 600;

  storedRunner->metricsValid = true;
  storedRunner->metrics.windowReady = true;
  storedRunner->metrics.validMask = STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY;
  storedRunner->metrics.deliveredOutputRows1m = 120;
  storedRunner->metrics.resultLatencyUs1m = 3000;
  storedRunner->metrics.resultLatencySamples1m = 2;

  auto *nonTop = appendTask(&topology.runners[1], STREAM_RUNNER_TASK, 0);
  ASSERT_NE(nullptr, nonTop);
  nonTop->id.deployId = 1;
  nonTop->metricsValid = true;
  nonTop->metrics.windowReady = true;
  nonTop->metrics.validMask = STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY;

  StreamMapGuard streamMap;
  ASSERT_NE(streamMap.get(), nullptr);
  ASSERT_EQ(0, taosHashPut(mStreamMgmt.streamMap, &createReq.streamId, sizeof(createReq.streamId), &topology,
                           sizeof(topology)));
  int32_t rows = 0;
  ASSERT_EQ(0, mstSetStreamTasksResBlock(&stream, pBlock, &rows, 3));
  ASSERT_EQ(3, rows);
  EXPECT_DOUBLE_EQ(getDoubleValue(pBlock, inputColumn, 0), 10.0);
  EXPECT_TRUE(isNullValue(pBlock, outputColumn, 0, 3));
  EXPECT_TRUE(isNullValue(pBlock, latencyColumn, 0, 3));
  EXPECT_TRUE(isNullValue(pBlock, inputColumn, 1, 3));
  EXPECT_DOUBLE_EQ(getDoubleValue(pBlock, outputColumn, 1), 2.0);
  EXPECT_DOUBLE_EQ(getDoubleValue(pBlock, latencyColumn, 1), 1.5);
  EXPECT_TRUE(isNullValue(pBlock, inputColumn, 2, 3));
  EXPECT_TRUE(isNullValue(pBlock, outputColumn, 2, 3));
  EXPECT_TRUE(isNullValue(pBlock, latencyColumn, 2, 3));

  taosArrayDestroy(topology.trigReaders);
  taosArrayDestroy(topology.runners[0]);
  taosArrayDestroy(topology.runners[1]);
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
  ASSERT_EQ(mstSetStreamAttrResBlock(&mnode, &stream, pBlock, 0, 0), TSDB_CODE_SUCCESS);

  EXPECT_EQ(getInt64Value(pBlock, kStreamRealtimeLagColumn, 0), 1234);
  EXPECT_DOUBLE_EQ(getDoubleValue(pBlock, kStreamInputRateColumn, 0), 10.0);
  EXPECT_TRUE(isNullValue(pBlock, kStreamOutputRateColumn, 0, 1));
  EXPECT_TRUE(isNullValue(pBlock, kStreamResultLatencyColumn, 0, 1));
  EXPECT_TRUE(isNullValue(pBlock, kStreamHistoryProgressColumn, 0, 1));

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
  SnodeLookupStubGuard snodeLookup;
  SMnode               mnode = {};
  ASSERT_EQ(mstSetStreamAttrResBlock(&mnode, &stream, pStreamBlock, 0, 0), TSDB_CODE_SUCCESS);
  EXPECT_DOUBLE_EQ(getDoubleValue(pStreamBlock, kStreamOutputRateColumn, 0), 2.0);
  EXPECT_DOUBLE_EQ(getDoubleValue(pStreamBlock, kStreamResultLatencyColumn, 0), 1000.0);

  int32_t rows = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, mstSetStreamTasksResBlock(&stream, pTaskBlock, &rows, 4));
  ASSERT_EQ(3, rows);
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

TEST(MndStreamWatchTest, TimeAlignPeriodRequiresLocalMetadataWithoutRawReads) {
  SCMCreateStreamReq create{};
  SStmStatus         status{};
  status.pCreate = &create;
  SStmTaskDeploy deploy{};
  create.triggerType = WINDOW_TYPE_PERIOD;
  ASSERT_EQ(TSDB_CODE_SUCCESS, msmBuildReaderDeployInfo(&deploy, nullptr, &status, true));
  EXPECT_FALSE(deploy.msg.reader.msg.trigger.requireLocalVtableRefs);
  EXPECT_FALSE(deploy.msg.reader.msg.trigger.timeAlignConsumer);

  SStreamTimeAlignSpec spec{};
  create.pTimeAlign = &spec;
  ASSERT_EQ(TSDB_CODE_SUCCESS, msmBuildReaderDeployInfo(&deploy, nullptr, &status, true));
  EXPECT_TRUE(deploy.msg.reader.msg.trigger.requireLocalVtableRefs);
  EXPECT_FALSE(deploy.msg.reader.msg.trigger.timeAlignConsumer);

  create.triggerType = WINDOW_TYPE_INTERVAL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, msmBuildReaderDeployInfo(&deploy, nullptr, &status, true));
  EXPECT_TRUE(deploy.msg.reader.msg.trigger.requireLocalVtableRefs);
  EXPECT_TRUE(deploy.msg.reader.msg.trigger.timeAlignConsumer);
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
  std::memset(createReq.timeConfig.timezone, 0xA5, sizeof(createReq.timeConfig.timezone));
  std::memcpy(createReq.timeConfig.timezone, "Asia/Shanghai", sizeof("Asia/Shanghai"));
  createReq.timeConfig.firstDayOfWeek = 6;
  SCMCreateStreamReq statusCreateReq = createReq;
  tstrncpy(statusCreateReq.timeConfig.timezone, "UTC", sizeof(statusCreateReq.timeConfig.timezone));
  statusCreateReq.timeConfig.firstDayOfWeek = 1;

  SSdb   sdb = {0};
  SMnode mnode = {0};
  mnode.pSdb = &sdb;

  SStmStatus status = {0};
  status.pCreate = &statusCreateReq;
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
  EXPECT_EQ(std::memcmp(&deploy.msg.trigger.timeConfig, &createReq.timeConfig, sizeof(createReq.timeConfig)), 0);
  EXPECT_EQ(deploy.msg.trigger.readerList, nullptr);
  EXPECT_EQ(deploy.msg.trigger.runnerList, nullptr);

  taosArrayDestroy(deploy.msg.trigger.readerList);
  taosArrayDestroy(deploy.msg.trigger.runnerList);
  taosArrayDestroy(status.trigReaders);
}

#ifdef TD_ENTERPRISE
extern "C" int32_t msmGrpAddActionUndeploy(SStmGrpCtx *pCtx, int64_t streamId, SStreamTask *pTask);

namespace {
void *failMixedReaderResponsePush(SArray *, const void *, int32_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

int32_t acceptMixedReaderUndeploy(SStmGrpCtx *, int64_t, SStreamTask *) {
  return TSDB_CODE_SUCCESS;
}
}  // namespace

TEST_F(MndStreamOrigReaderReplyTest, MixedReaderResponsePushFailureReleasesAcquiredStream) {
  SStreamMgmtReq *request = static_cast<SStreamMgmtReq *>(taosMemoryCalloc(1, sizeof(SStreamMgmtReq)));
  ASSERT_NE(request, nullptr);
  request->type = STREAM_MGMT_REQ_TRIGGER_MIXED_ORIGTBL_READER;
  request->cont.pExtOrigTblReaders = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderReq), 1);
  ASSERT_NE(request->cont.pExtOrigTblReaders, nullptr);
  SStmTaskStatusMsg task = {};
  task.streamId = streamId_;
  task.taskId = trigger_.id.taskId;
  task.type = trigger_.type;
  task.pMgmtReq = request;
  SMnode mnode = {};
  SMStreamHbRspMsg response = {};
  SStmGrpCtx context = {};
  context.pMnode = &mnode;
  context.pRsp = &response;
  Stub stub;
  stub.set(mndAcquireExtSource, +[](SMnode *, const char *) -> SExtSourceObj * { return nullptr; });
  stub.set(taosArrayAddBatch, failMixedReaderResponsePush);
  stub.set(msmGrpAddActionUndeploy, acceptMixedReaderUndeploy);

  EXPECT_EQ(handleHeartbeatTaskMgmtReq(&context, &task), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_GT(acquireCalls_, 0);
  EXPECT_EQ(task.pMgmtReq, nullptr);

  stub.reset(msmGrpAddActionUndeploy);
  stub.reset(taosArrayAddBatch);
  tFreeSMStreamHbRspMsg(&response);
}
#endif

TEST_F(MndStreamHeartbeatTest, MixedInternalAndExternalReaderRequestPreservesBothOutcomes) {
  SStreamDbTableName internalReq = {};
  tstrncpy(internalReq.dbFName, "acct.metrics", sizeof(internalReq.dbFName));
  tstrncpy(internalReq.tbName, "cpu", sizeof(internalReq.tbName));

  SStreamExtOrigTblReaderReq extReq = {};
  extReq.registryEntryId = 101;
  extReq.deploymentGeneration = 7;
  extReq.protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
  extReq.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE;
  tstrncpy(extReq.identity.sourceName, "remote", sizeof(extReq.identity.sourceName));
  tstrncpy(extReq.identity.dbName, "metrics", sizeof(extReq.identity.dbName));
  tstrncpy(extReq.identity.tableName, "cpu", sizeof(extReq.identity.tableName));

  SStreamMgmtReq *mgmtReq = static_cast<SStreamMgmtReq *>(taosMemoryCalloc(1, sizeof(SStreamMgmtReq)));
  ASSERT_NE(mgmtReq, nullptr);
  mgmtReq->reqId = 9;
  mgmtReq->type = STREAM_MGMT_REQ_TRIGGER_MIXED_ORIGTBL_READER;
  mgmtReq->cont.pReqs = taosArrayInit(1, sizeof(SStreamDbTableName));
  ASSERT_NE(mgmtReq->cont.pReqs, nullptr);
  ASSERT_NE(taosArrayPush(mgmtReq->cont.pReqs, &internalReq), nullptr);
  mgmtReq->cont.pExtOrigTblReaders = taosArrayInit(1, sizeof(SStreamExtOrigTblReaderReq));
  ASSERT_NE(mgmtReq->cont.pExtOrigTblReaders, nullptr);
  ASSERT_NE(taosArrayPush(mgmtReq->cont.pExtOrigTblReaders, &extReq), nullptr);

  SStmTaskStatusMsg task = {};
  task.streamId = 1;
  task.pMgmtReq = mgmtReq;
  SMStreamHbRspMsg hbRsp = {};
  SStmGrpCtx ctx = {};
  ctx.pRsp = &hbRsp;

  EXPECT_EQ(handleHeartbeatTaskMgmtReq(&ctx, &task), TSDB_CODE_SUCCESS);
  ASSERT_NE(hbRsp.rsps.rspList, nullptr);
  ASSERT_EQ(taosArrayGetSize(hbRsp.rsps.rspList), 1);
  SStreamMgmtRsp *rsp = static_cast<SStreamMgmtRsp *>(taosArrayGet(hbRsp.rsps.rspList, 0));
  ASSERT_NE(rsp, nullptr);
  EXPECT_EQ(rsp->header.msgType, STREAM_MSG_MIXED_ORIGTBL_READER_INFO);
  EXPECT_EQ(rsp->code, TSDB_CODE_MND_STREAM_NOT_RUNNING);
  ASSERT_NE(rsp->cont.pExtOrigTblReaders, nullptr);
  ASSERT_EQ(taosArrayGetSize(rsp->cont.pExtOrigTblReaders), 1);
  SStreamExtOrigTblReaderRsp *extRsp =
      static_cast<SStreamExtOrigTblReaderRsp *>(taosArrayGet(rsp->cont.pExtOrigTblReaders, 0));
  ASSERT_NE(extRsp, nullptr);
  EXPECT_EQ(extRsp->registryEntryId, extReq.registryEntryId);
  EXPECT_EQ(extRsp->code, rsp->code);

  tFreeSMStreamHbRspMsg(&hbRsp);
}

TEST_F(MndStreamHeartbeatTest, SingleExtReaderRequestTypeIsRejectedAsUnsupported) {
  // The single-EXT management request type has no consumer on the trigger side (it always sends
  // the MIXED type), so the dedicated mnode handler was removed. The request must now be
  // rejected instead of producing a response nothing can dispatch.
  const int64_t registryEntryId = 202;
  const int64_t deploymentGeneration = 13;
  SStreamExtOrigTblReaderReq *extReq =
      static_cast<SStreamExtOrigTblReaderReq *>(taosMemoryCalloc(1, sizeof(SStreamExtOrigTblReaderReq)));
  ASSERT_NE(extReq, nullptr);
  extReq->registryEntryId = registryEntryId;
  extReq->deploymentGeneration = deploymentGeneration;
  extReq->protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
  extReq->capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE;
  extReq->sourceType = EXT_SOURCE_MYSQL;
  tstrncpy(extReq->identity.sourceName, "remote", sizeof(extReq->identity.sourceName));
  tstrncpy(extReq->identity.dbName, "metrics", sizeof(extReq->identity.dbName));
  tstrncpy(extReq->identity.tableName, "cpu", sizeof(extReq->identity.tableName));

  SStreamMgmtReq *mgmtReq = static_cast<SStreamMgmtReq *>(taosMemoryCalloc(1, sizeof(SStreamMgmtReq)));
  ASSERT_NE(mgmtReq, nullptr);
  mgmtReq->reqId = 10;
  mgmtReq->type = STREAM_MGMT_REQ_TRIGGER_EXT_ORIGTBL_READER;
  mgmtReq->cont.pExtOrigTblReader = extReq;

  SStmTaskStatusMsg task = {};
  task.streamId = 2;
  task.pMgmtReq = mgmtReq;
  SMStreamHbRspMsg hbRsp = {};
  SStmGrpCtx ctx = {};
  ctx.pRsp = &hbRsp;

  EXPECT_NE(handleHeartbeatTaskMgmtReq(&ctx, &task), TSDB_CODE_SUCCESS);

  // The rejected request is not consumed: free it here.
  tFreeSStreamMgmtReq(mgmtReq);
  taosMemoryFree(mgmtReq);
}

#ifdef TD_ENTERPRISE
class MndStreamExtReaderSchemaTest : public testing::Test {
 protected:
  void SetUp() override {
    source_.type = EXT_SOURCE_MYSQL;
    req_.sourceType = EXT_SOURCE_MYSQL;
    tstrncpy(columns_[0].colName, "device", sizeof(columns_[0].colName));
    tstrncpy(columns_[0].extTypeName, "varchar", sizeof(columns_[0].extTypeName));
    columns_[0].isRemotePrimaryKey = true;
    tstrncpy(columns_[1].colName, "ts", sizeof(columns_[1].colName));
    tstrncpy(columns_[1].extTypeName, "timestamp", sizeof(columns_[1].extTypeName));
    columns_[1].isRemotePrimaryKey = true;
    tstrncpy(columns_[2].colName, "value", sizeof(columns_[2].colName));
    tstrncpy(columns_[2].extTypeName, "int", sizeof(columns_[2].extTypeName));
    meta_.pCols = columns_;
    meta_.numOfCols = 3;
    req_.bindings = taosArrayInit(1, sizeof(SStreamExtOrigTblReaderBinding));
    ASSERT_NE(nullptr, req_.bindings);
    SStreamExtOrigTblReaderBinding binding = {};
    binding.kind = STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER;
    binding.expectedType = TSDB_DATA_TYPE_INT;
    tstrncpy(binding.externalColumn, "value", sizeof(binding.externalColumn));
    ASSERT_NE(nullptr, taosArrayPush(req_.bindings, &binding));
  }

  void TearDown() override {
    tFreeSStreamExtOrigTblReaderRsp(&rsp_);
    tFreeSStreamExtTriggerSpec(spec_);
    tFreeSStreamExtOrigTblReaderReq(&req_);
  }

  int32_t build() { return msmBuildExtOrigTblReaderSpec(&source_, &req_, &meta_, &spec_, &rsp_); }

  SExtSourceObj source_ = {};
  SExtColumnDef columns_[4] = {};
  SExtTableMeta meta_ = {};
  SStreamExtOrigTblReaderReq req_ = {};
  SStreamExtOrigTblReaderRsp rsp_ = {};
  SStreamExtTriggerSpec *spec_ = nullptr;
};

TEST_F(MndStreamExtReaderSchemaTest, SelectsTimestampAfterNonTimestampCompositePrimaryKeyMember) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_STREQ("ts", spec_->tsColumn);
  EXPECT_EQ(TSDB_DATA_TYPE_TIMESTAMP, spec_->pColMappings[0].tdType.type);
  EXPECT_EQ(TSDB_DATA_TYPE_TIMESTAMP, spec_->pCalcMappings[0].tdType.type);
}

TEST_F(MndStreamExtReaderSchemaTest, RejectsPrimaryKeyWithoutTimestamp) {
  columns_[1].isRemotePrimaryKey = false;
  EXPECT_EQ(TSDB_CODE_EXT_NO_TS_PRIMARY_KEY, build());
  EXPECT_EQ(nullptr, spec_);
}

TEST_F(MndStreamExtReaderSchemaTest, RejectsAmbiguousTimestampPrimaryKey) {
  tstrncpy(columns_[0].extTypeName, "timestamp", sizeof(columns_[0].extTypeName));
  EXPECT_EQ(TSDB_CODE_EXT_NO_TS_PRIMARY_KEY, build());
  EXPECT_EQ(nullptr, spec_);
}

TEST_F(MndStreamExtReaderSchemaTest, RejectsSubMicrosecondTimestampPrimaryKey) {
  // SQL Server datetime2(7) keeps a seventh fractional digit that the connector
  // truncates, so a row written into the already consumed microsecond can never
  // be observed: the reader must reject the binding instead of losing rows.
  source_.type = EXT_SOURCE_SQLSERVER;
  req_.sourceType = EXT_SOURCE_SQLSERVER;
  tstrncpy(columns_[1].extTypeName, "datetime2(7)", sizeof(columns_[1].extTypeName));
  EXPECT_EQ(TSDB_CODE_STREAM_EXT_TS_PRECISION_NOT_SUPPORTED, build());
  EXPECT_EQ(nullptr, spec_);
}

TEST_F(MndStreamExtReaderSchemaTest, AcceptsMicrosecondTimestampPrimaryKey) {
  // datetime2(6) is exactly representable in the microsecond transport, so the
  // same schema is accepted for SQL Server.
  source_.type = EXT_SOURCE_SQLSERVER;
  req_.sourceType = EXT_SOURCE_SQLSERVER;
  tstrncpy(columns_[1].extTypeName, "datetime2(6)", sizeof(columns_[1].extTypeName));
  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_STREQ("ts", spec_->tsColumn);
}

TEST_F(MndStreamExtReaderSchemaTest, RejectsDecimalScaleMismatchOnRedeploy) {
  // The virtual column keeps its declared DECIMAL(10,2) while the remote column changed to
  // DECIMAL(10,3): same type enum, but re-interpreting the integer payload under the virtual
  // column's scale would silently scale every value by 10. The redeploy must be rejected.
  tstrncpy(columns_[2].extTypeName, "decimal(10,3)", sizeof(columns_[2].extTypeName));
  SStreamExtOrigTblReaderBinding *pBinding =
      static_cast<SStreamExtOrigTblReaderBinding *>(taosArrayGet(req_.bindings, 0));
  pBinding->expectedType = TSDB_DATA_TYPE_DECIMAL64;
  pBinding->expectedPrecision = 10;
  pBinding->expectedScale = 2;
  EXPECT_EQ(TSDB_CODE_EXT_TYPE_NOT_MAPPABLE, build());
  EXPECT_EQ(nullptr, spec_);
}

TEST_F(MndStreamExtReaderSchemaTest, AcceptsMatchingDecimalPrecisionScale) {
  tstrncpy(columns_[2].extTypeName, "decimal(10,2)", sizeof(columns_[2].extTypeName));
  SStreamExtOrigTblReaderBinding *pBinding =
      static_cast<SStreamExtOrigTblReaderBinding *>(taosArrayGet(req_.bindings, 0));
  pBinding->expectedType = TSDB_DATA_TYPE_DECIMAL64;
  pBinding->expectedPrecision = 10;
  pBinding->expectedScale = 2;
  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  ASSERT_EQ(2, spec_->numColMappings);
  EXPECT_EQ(TSDB_DATA_TYPE_DECIMAL64, spec_->pColMappings[1].tdType.type);
  EXPECT_EQ(10, spec_->pColMappings[1].tdType.precision);
  EXPECT_EQ(2, spec_->pColMappings[1].tdType.scale);
  EXPECT_EQ(1, rsp_.acceptedTriggerBindingCount);
}

TEST_F(MndStreamExtReaderSchemaTest, SharedBindingsReusePhysicalColumnSlotsPerKind) {
  columns_[0].isRemotePrimaryKey = false;
  for (int32_t index = 1; index <= STREAM_EXT_ORIGTBL_READER_MAX_COLUMNS; ++index) {
    SStreamExtOrigTblReaderBinding binding =
        *static_cast<SStreamExtOrigTblReaderBinding *>(taosArrayGet(req_.bindings, 0));
    binding.bindingId = index;
    binding.virtualUid = index;
    binding.kind = index % 2 ? STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER : STREAM_EXT_ORIGTBL_READER_BINDING_CALC;
    ASSERT_NE(nullptr, taosArrayPush(req_.bindings, &binding));
  }
  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_EQ(2, spec_->numColMappings);
  EXPECT_EQ(2, spec_->numCalcMappings);
  EXPECT_EQ(2, taosArrayGetSize(spec_->triggerColumns));
  EXPECT_EQ(2, taosArrayGetSize(spec_->calcColumns));
  ASSERT_EQ(STREAM_EXT_ORIGTBL_READER_MAX_COLUMNS + 1, taosArrayGetSize(rsp_.results));
  EXPECT_EQ(STREAM_EXT_ORIGTBL_READER_MAX_COLUMNS + 1,
            rsp_.acceptedTriggerBindingCount + rsp_.acceptedCalcBindingCount);
  for (int32_t index = 0; index <= STREAM_EXT_ORIGTBL_READER_MAX_COLUMNS; ++index) {
    auto *result = static_cast<SStreamExtOrigTblReaderResult *>(taosArrayGet(rsp_.results, index));
    EXPECT_EQ(TSDB_CODE_SUCCESS, result->code);
    EXPECT_EQ(index, result->binding.virtualUid);
    EXPECT_EQ(1, result->binding.resultColumnSlot);
  }
}

TEST_F(MndStreamExtReaderSchemaTest, ResolvesLegacySqlServerFourPartNamespace) {
  // A 4-part SQL Server reference (source.schema.table.column) reaches the reader with the schema
  // in dbName and an empty schemaName. The reader must connect to the source's default database
  // and use the middle segment as the schema, while leaving the wire identity untouched so the
  // registry entries still match (mirrors the parser normalization done before persisting).
  source_.type = EXT_SOURCE_SQLSERVER;
  req_.sourceType = EXT_SOURCE_SQLSERVER;
  tstrncpy(source_.defaultDatabase, "sales_db", sizeof(source_.defaultDatabase));
  tstrncpy(source_.defaultSchema, "dbo", sizeof(source_.defaultSchema));
  tstrncpy(req_.identity.dbName, "archive", sizeof(req_.identity.dbName));
  req_.identity.schemaName[0] = '\0';
  tstrncpy(req_.identity.tableName, "orders", sizeof(req_.identity.tableName));
  tstrncpy(columns_[1].extTypeName, "datetime2(6)", sizeof(columns_[1].extTypeName));

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_STREQ("sales_db", spec_->extDb);
  EXPECT_STREQ("archive", spec_->extSchema);
  EXPECT_STREQ("orders", spec_->extTable);
  // Registry identity must stay exactly as it arrived on the wire.
  EXPECT_STREQ("archive", req_.identity.dbName);
  EXPECT_STREQ("", req_.identity.schemaName);
}

TEST_F(MndStreamExtReaderSchemaTest, KeepsExplicitSqlServerDatabaseAndSchema) {
  // A 5-part reference (source.database.schema.table.column) keeps both segments as given.
  source_.type = EXT_SOURCE_SQLSERVER;
  req_.sourceType = EXT_SOURCE_SQLSERVER;
  tstrncpy(source_.defaultDatabase, "sales_db", sizeof(source_.defaultDatabase));
  tstrncpy(source_.defaultSchema, "dbo", sizeof(source_.defaultSchema));
  tstrncpy(req_.identity.dbName, "archive", sizeof(req_.identity.dbName));
  tstrncpy(req_.identity.schemaName, "reporting", sizeof(req_.identity.schemaName));
  tstrncpy(req_.identity.tableName, "orders", sizeof(req_.identity.tableName));
  tstrncpy(columns_[1].extTypeName, "datetime2(6)", sizeof(columns_[1].extTypeName));

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_STREQ("archive", spec_->extDb);
  EXPECT_STREQ("reporting", spec_->extSchema);
}

TEST_F(MndStreamExtReaderSchemaTest, KeepsFivePartNamespaceWhenSchemaMatchesSourceDefault) {
  // A five-part reference may legitimately point at another database of the source while naming
  // the source's default schema. That identity is byte-identical to the shape the pre-
  // normalization CREATE path wrote for a four-part reference (path schema in dbName, default
  // schema in schemaName), so the reader must keep it verbatim instead of guessing the legacy
  // layout -- otherwise `src.otherdb.dbo.t.c` would read otherdb as a schema.
  source_.type = EXT_SOURCE_SQLSERVER;
  req_.sourceType = EXT_SOURCE_SQLSERVER;
  tstrncpy(source_.defaultDatabase, "sales_db", sizeof(source_.defaultDatabase));
  tstrncpy(source_.defaultSchema, "dbo", sizeof(source_.defaultSchema));
  tstrncpy(req_.identity.dbName, "archive", sizeof(req_.identity.dbName));
  tstrncpy(req_.identity.schemaName, "dbo", sizeof(req_.identity.schemaName));
  tstrncpy(req_.identity.tableName, "orders", sizeof(req_.identity.tableName));
  tstrncpy(columns_[1].extTypeName, "datetime2(6)", sizeof(columns_[1].extTypeName));

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_STREQ("archive", spec_->extDb);
  EXPECT_STREQ("dbo", spec_->extSchema);
}

TEST_F(MndStreamExtReaderSchemaTest, KeepsDatabaseScopedNamespaceWithoutSchema) {
  // Non-SQL Server sources keep the database segment and must never gain a schema.
  tstrncpy(source_.defaultDatabase, "metrics", sizeof(source_.defaultDatabase));
  tstrncpy(req_.identity.dbName, "warehouse", sizeof(req_.identity.dbName));
  req_.identity.schemaName[0] = '\0';
  tstrncpy(req_.identity.tableName, "cpu", sizeof(req_.identity.tableName));

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_STREQ("warehouse", spec_->extDb);
  EXPECT_STREQ("", spec_->extSchema);
}

TEST_F(MndStreamExtReaderSchemaTest, ResolvesOracleSchemaOnlyNamespace) {
  // Oracle has exactly one user namespace level - the schema - and its "database" is the Easy
  // Connect service name, which is connection state rather than a namespace. A virtual table
  // binding persisted as source.<schema>.<table> reaches the reader with the folded schema in
  // dbName; reading that as a database would dial the listener with service=<schema> and fail
  // with ORA-12514, so the reader re-maps it and keeps the service name from the source.
  source_.type = EXT_SOURCE_ORACLE;
  req_.sourceType = EXT_SOURCE_ORACLE;
  tstrncpy(source_.defaultDatabase, "ORCL", sizeof(source_.defaultDatabase));
  tstrncpy(source_.defaultSchema, "TAOSDATA", sizeof(source_.defaultSchema));
  tstrncpy(req_.identity.dbName, "TAOSDATA", sizeof(req_.identity.dbName));
  req_.identity.schemaName[0] = '\0';
  tstrncpy(req_.identity.tableName, "same_a", sizeof(req_.identity.tableName));
  tstrncpy(columns_[1].extTypeName, "TIMESTAMP(6)", sizeof(columns_[1].extTypeName));
  // "int" is not an Oracle type: every numeric column there is a NUMBER, and
  // NUMBER(10,0) maps to BIGINT, so the virtual column must declare BIGINT too.
  tstrncpy(columns_[2].colName, "value", sizeof(columns_[2].colName));
  tstrncpy(columns_[2].extTypeName, "NUMBER(10,0)", sizeof(columns_[2].extTypeName));
  static_cast<SStreamExtOrigTblReaderBinding *>(taosArrayGet(req_.bindings, 0))->expectedType =
      TSDB_DATA_TYPE_BIGINT;

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_STREQ("ORCL", spec_->extDb);
  EXPECT_STREQ("TAOSDATA", spec_->extSchema);
  // The wire identity must stay exactly as it arrived so registry entries still match.
  EXPECT_STREQ("TAOSDATA", req_.identity.dbName);
  EXPECT_STREQ("", req_.identity.schemaName);
}

TEST_F(MndStreamExtReaderSchemaTest, RejectsOracleSourceWithoutAnySchema) {
  // Oracle has no database level, so a table path can only be completed by a schema. Without one
  // the reader would render "<service>.<table>", which names nothing, and the user would see the
  // provider's ORA-00942 instead of the real cause. The catalog path (ctgAsync.c) already refuses
  // such a source with TSDB_CODE_EXT_DEFAULT_NS_MISSING and resolves no implicit login schema
  // (DS 5.3.3), so the reader must return the same code instead of deploying a broken spec.
  source_.type = EXT_SOURCE_ORACLE;
  req_.sourceType = EXT_SOURCE_ORACLE;
  tstrncpy(source_.defaultDatabase, "ORCL", sizeof(source_.defaultDatabase));
  source_.defaultSchema[0] = '\0';
  tstrncpy(req_.identity.tableName, "same_a", sizeof(req_.identity.tableName));
  req_.identity.dbName[0] = '\0';
  req_.identity.schemaName[0] = '\0';

  EXPECT_EQ(TSDB_CODE_EXT_DEFAULT_NS_MISSING, build());
  EXPECT_EQ(nullptr, spec_);
}

TEST_F(MndStreamExtReaderSchemaTest, AcceptsOraclePathSchemaWithoutSourceDefault) {
  // A source may leave SCHEMA unset while the column path names the schema explicitly; that path
  // supplies the only namespace level Oracle has, so it must still resolve.
  source_.type = EXT_SOURCE_ORACLE;
  req_.sourceType = EXT_SOURCE_ORACLE;
  tstrncpy(source_.defaultDatabase, "ORCL", sizeof(source_.defaultDatabase));
  source_.defaultSchema[0] = '\0';
  tstrncpy(req_.identity.dbName, "TAOSDATA", sizeof(req_.identity.dbName));
  req_.identity.schemaName[0] = '\0';
  tstrncpy(req_.identity.tableName, "same_a", sizeof(req_.identity.tableName));
  tstrncpy(columns_[1].extTypeName, "TIMESTAMP(6)", sizeof(columns_[1].extTypeName));
  tstrncpy(columns_[2].colName, "value", sizeof(columns_[2].colName));
  tstrncpy(columns_[2].extTypeName, "NUMBER(10,0)", sizeof(columns_[2].extTypeName));
  static_cast<SStreamExtOrigTblReaderBinding *>(taosArrayGet(req_.bindings, 0))->expectedType =
      TSDB_DATA_TYPE_BIGINT;

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_STREQ("ORCL", spec_->extDb);
  EXPECT_STREQ("TAOSDATA", spec_->extSchema);
}

TEST_F(MndStreamExtReaderSchemaTest, UsesOracleDictionaryTableSpelling) {
  // The reader renders the table name quoted, which makes the remote side case-sensitive, while
  // Oracle folds every unquoted identifier to upper case: the spelling used in the column path
  // ("same_a") would then name nothing (ORA-00942). The reader uses the name the schema fetch
  // resolved, mirroring what createExtTableRefNode() already does for the schema.
  source_.type = EXT_SOURCE_ORACLE;
  req_.sourceType = EXT_SOURCE_ORACLE;
  tstrncpy(source_.defaultDatabase, "ORCL", sizeof(source_.defaultDatabase));
  tstrncpy(source_.defaultSchema, "TAOSDATA", sizeof(source_.defaultSchema));
  tstrncpy(req_.identity.tableName, "same_a", sizeof(req_.identity.tableName));
  tstrncpy(meta_.remoteTableName, "SAME_A", sizeof(meta_.remoteTableName));
  tstrncpy(columns_[1].extTypeName, "TIMESTAMP(6)", sizeof(columns_[1].extTypeName));
  tstrncpy(columns_[2].colName, "value", sizeof(columns_[2].colName));
  tstrncpy(columns_[2].extTypeName, "NUMBER(10,0)", sizeof(columns_[2].extTypeName));
  static_cast<SStreamExtOrigTblReaderBinding *>(taosArrayGet(req_.bindings, 0))->expectedType =
      TSDB_DATA_TYPE_BIGINT;

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_STREQ("SAME_A", spec_->extTable);
}

TEST_F(MndStreamExtReaderSchemaTest, KeepsOracleDictionaryTableSpellingVerbatim) {
  // The connector reports the exact remote spelling.  Quote characters in it are
  // part of the name and are never stripped or reinterpreted; the reader emits its
  // own quoting when it renders the table reference, so the spec stores the bytes
  // unchanged.
  source_.type = EXT_SOURCE_ORACLE;
  req_.sourceType = EXT_SOURCE_ORACLE;
  tstrncpy(source_.defaultDatabase, "ORCL", sizeof(source_.defaultDatabase));
  tstrncpy(source_.defaultSchema, "TAOSDATA", sizeof(source_.defaultSchema));
  tstrncpy(req_.identity.tableName, "same_a", sizeof(req_.identity.tableName));
  tstrncpy(meta_.remoteTableName, "\"same_a\"", sizeof(meta_.remoteTableName));
  tstrncpy(columns_[1].extTypeName, "TIMESTAMP(6)", sizeof(columns_[1].extTypeName));
  tstrncpy(columns_[2].colName, "value", sizeof(columns_[2].colName));
  tstrncpy(columns_[2].extTypeName, "NUMBER(10,0)", sizeof(columns_[2].extTypeName));
  static_cast<SStreamExtOrigTblReaderBinding *>(taosArrayGet(req_.bindings, 0))->expectedType =
      TSDB_DATA_TYPE_BIGINT;

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_STREQ("\"same_a\"", spec_->extTable);
}

TEST_F(MndStreamExtReaderSchemaTest, KeepsOracleEmbeddedQuoteSpellingVerbatim) {
  // An embedded double quote is a literal character of the exact remote name and
  // must survive the copy unchanged; the renderer doubles it when it quotes the
  // identifier.
  source_.type = EXT_SOURCE_ORACLE;
  req_.sourceType = EXT_SOURCE_ORACLE;
  tstrncpy(source_.defaultDatabase, "ORCL", sizeof(source_.defaultDatabase));
  tstrncpy(source_.defaultSchema, "TAOSDATA", sizeof(source_.defaultSchema));
  tstrncpy(req_.identity.tableName, "same_a", sizeof(req_.identity.tableName));
  tstrncpy(meta_.remoteTableName, "sa\"me", sizeof(meta_.remoteTableName));
  tstrncpy(columns_[1].extTypeName, "TIMESTAMP(6)", sizeof(columns_[1].extTypeName));
  tstrncpy(columns_[2].colName, "value", sizeof(columns_[2].colName));
  tstrncpy(columns_[2].extTypeName, "NUMBER(10,0)", sizeof(columns_[2].extTypeName));
  static_cast<SStreamExtOrigTblReaderBinding *>(taosArrayGet(req_.bindings, 0))->expectedType =
      TSDB_DATA_TYPE_BIGINT;

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_STREQ("sa\"me", spec_->extTable);
}

TEST_F(MndStreamExtReaderSchemaTest, FallsBackToReferenceSpellingWithoutDictionaryName) {
  // A schema fetch that did not report a remote spelling (older connector, or a
  // table that vanished) must not blank the name: the reference spelling stays.
  source_.type = EXT_SOURCE_ORACLE;
  req_.sourceType = EXT_SOURCE_ORACLE;
  tstrncpy(source_.defaultDatabase, "ORCL", sizeof(source_.defaultDatabase));
  tstrncpy(source_.defaultSchema, "TAOSDATA", sizeof(source_.defaultSchema));
  tstrncpy(req_.identity.tableName, "same_a", sizeof(req_.identity.tableName));
  tstrncpy(columns_[1].extTypeName, "TIMESTAMP(6)", sizeof(columns_[1].extTypeName));
  tstrncpy(columns_[2].colName, "value", sizeof(columns_[2].colName));
  tstrncpy(columns_[2].extTypeName, "NUMBER(10,0)", sizeof(columns_[2].extTypeName));
  static_cast<SStreamExtOrigTblReaderBinding *>(taosArrayGet(req_.bindings, 0))->expectedType =
      TSDB_DATA_TYPE_BIGINT;

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_STREQ("same_a", spec_->extTable);
}

TEST_F(MndStreamExtReaderSchemaTest, KeepsReferenceSpellingForNonOracleSources) {
  // Only Oracle needs the dictionary spelling; every other provider resolves the
  // reference spelling itself, so a remote name reported for them is not applied.
  tstrncpy(source_.defaultDatabase, "metrics", sizeof(source_.defaultDatabase));
  tstrncpy(req_.identity.dbName, "warehouse", sizeof(req_.identity.dbName));
  req_.identity.schemaName[0] = '\0';
  tstrncpy(req_.identity.tableName, "cpu", sizeof(req_.identity.tableName));
  tstrncpy(meta_.remoteTableName, "CPU_DICTIONARY", sizeof(meta_.remoteTableName));

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  EXPECT_STREQ("cpu", spec_->extTable);
}

TEST_F(MndStreamExtReaderSchemaTest, FillsSourceTimestampPrecisionForSideTimestampColumns) {
  // Providers decode every timestamp column at the table's transport precision, not only at the
  // primary-key one (DS 5.3.7). Leaving tsPrecision at 0 on a side column made the Oracle
  // connector decode a TIMESTAMP(7..9) value at microseconds, so its last three digits were gone
  // before any later normalization could run; carrying the precision here is the only place that
  // knows both the binding and the source schema.
  source_.type = EXT_SOURCE_ORACLE;
  req_.sourceType = EXT_SOURCE_ORACLE;
  tstrncpy(source_.defaultDatabase, "ORCL", sizeof(source_.defaultDatabase));
  tstrncpy(source_.defaultSchema, "TAOSDATA", sizeof(source_.defaultSchema));
  tstrncpy(req_.identity.tableName, "same_a", sizeof(req_.identity.tableName));
  tstrncpy(columns_[1].extTypeName, "TIMESTAMP(6)", sizeof(columns_[1].extTypeName));
  tstrncpy(columns_[2].colName, "value", sizeof(columns_[2].colName));
  tstrncpy(columns_[2].extTypeName, "NUMBER(10,0)", sizeof(columns_[2].extTypeName));
  static_cast<SStreamExtOrigTblReaderBinding *>(taosArrayGet(req_.bindings, 0))->expectedType =
      TSDB_DATA_TYPE_BIGINT;
  tstrncpy(columns_[3].colName, "event_ts", sizeof(columns_[3].colName));
  tstrncpy(columns_[3].extTypeName, "TIMESTAMP(9)", sizeof(columns_[3].extTypeName));
  meta_.numOfCols = 4;
  meta_.tsPrecision = TSDB_TIME_PRECISION_NANO;

  SStreamExtOrigTblReaderBinding sideTs = {};
  sideTs.kind = STREAM_EXT_ORIGTBL_READER_BINDING_CALC;
  sideTs.expectedType = TSDB_DATA_TYPE_TIMESTAMP;
  // The plain spelling binds by exact match; representation quotes are not
  // interpreted on the binding path (same rule as the parser).
  tstrncpy(sideTs.externalColumn, "event_ts", sizeof(sideTs.externalColumn));
  ASSERT_NE(nullptr, taosArrayPush(req_.bindings, &sideTs));

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  ASSERT_EQ(spec_->numCalcMappings, 2);
  EXPECT_EQ(TSDB_DATA_TYPE_TIMESTAMP, spec_->pCalcMappings[0].tdType.type);
  EXPECT_EQ(static_cast<int>(TSDB_TIME_PRECISION_NANO),
            static_cast<int>(spec_->pCalcMappings[0].tsPrecision));
  EXPECT_EQ(TSDB_DATA_TYPE_TIMESTAMP, spec_->pCalcMappings[1].tdType.type);
  EXPECT_STREQ("event_ts", spec_->pCalcMappings[1].colName);
  EXPECT_EQ(static_cast<int>(TSDB_TIME_PRECISION_NANO),
            static_cast<int>(spec_->pCalcMappings[1].tsPrecision));
}

// The reader binding resolves its column with the same rule as the parser and as
// the mapping dedup: extColumnNameEqual() candidate by candidate, with no
// interpretation of Oracle representation quotes.  A binding that keeps those
// quotes therefore resolves nothing and is rejected, which is what the main
// branch does, instead of binding a lower-case column that the dedup step would
// then merge back into the upper-case one.
TEST_F(MndStreamExtReaderSchemaTest, RejectsQuotedBindingLikeMain) {
  source_.type = EXT_SOURCE_ORACLE;
  req_.sourceType = EXT_SOURCE_ORACLE;
  tstrncpy(source_.defaultDatabase, "ORCL", sizeof(source_.defaultDatabase));
  tstrncpy(source_.defaultSchema, "TAOSDATA", sizeof(source_.defaultSchema));
  tstrncpy(req_.identity.tableName, "metrics_case", sizeof(req_.identity.tableName));
  tstrncpy(columns_[2].colName, "VAL", sizeof(columns_[2].colName));
  tstrncpy(columns_[2].extTypeName, "TIMESTAMP(6)", sizeof(columns_[2].extTypeName));
  tstrncpy(columns_[3].colName, "val", sizeof(columns_[3].colName));
  tstrncpy(columns_[3].extTypeName, "TIMESTAMP(9)", sizeof(columns_[3].extTypeName));
  meta_.numOfCols = 4;
  meta_.tsPrecision = TSDB_TIME_PRECISION_NANO;
  taosArrayPopTailBatch(req_.bindings, 1);

  SStreamExtOrigTblReaderBinding quoted = {};
  quoted.kind = STREAM_EXT_ORIGTBL_READER_BINDING_CALC;
  quoted.expectedType = TSDB_DATA_TYPE_TIMESTAMP;
  tstrncpy(quoted.externalColumn, "\"val\"", sizeof(quoted.externalColumn));
  ASSERT_NE(nullptr, taosArrayPush(req_.bindings, &quoted));

  ASSERT_EQ(TSDB_CODE_EXT_COLUMN_NOT_EXIST, build());
  EXPECT_EQ(rsp_.code, TSDB_CODE_EXT_COLUMN_NOT_EXIST);
  EXPECT_EQ(nullptr, spec_);
}

// The same table with the plain spelling binds the exactly-named column: the
// comparison is byte-exact, so catalog order does not matter and "val" cannot be
// merged into VAL.  The mapping dedup uses the same resolution.
TEST_F(MndStreamExtReaderSchemaTest, BindsPlainSpellingExactly) {
  source_.type = EXT_SOURCE_ORACLE;
  req_.sourceType = EXT_SOURCE_ORACLE;
  tstrncpy(source_.defaultDatabase, "ORCL", sizeof(source_.defaultDatabase));
  tstrncpy(source_.defaultSchema, "TAOSDATA", sizeof(source_.defaultSchema));
  tstrncpy(req_.identity.tableName, "metrics_case", sizeof(req_.identity.tableName));
  tstrncpy(columns_[2].colName, "VAL", sizeof(columns_[2].colName));
  tstrncpy(columns_[2].extTypeName, "TIMESTAMP(6)", sizeof(columns_[2].extTypeName));
  tstrncpy(columns_[3].colName, "val", sizeof(columns_[3].colName));
  tstrncpy(columns_[3].extTypeName, "TIMESTAMP(9)", sizeof(columns_[3].extTypeName));
  meta_.numOfCols = 4;
  meta_.tsPrecision = TSDB_TIME_PRECISION_NANO;
  taosArrayPopTailBatch(req_.bindings, 1);

  SStreamExtOrigTblReaderBinding plain = {};
  plain.kind = STREAM_EXT_ORIGTBL_READER_BINDING_CALC;
  plain.expectedType = TSDB_DATA_TYPE_TIMESTAMP;
  tstrncpy(plain.externalColumn, "val", sizeof(plain.externalColumn));
  ASSERT_NE(nullptr, taosArrayPush(req_.bindings, &plain));

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  bool foundLower = false;
  for (int32_t i = 0; i < spec_->numCalcMappings; ++i) {
    if (strcmp(spec_->pCalcMappings[i].colName, "val") == 0) {
      foundLower = true;
    }
    EXPECT_STRNE(spec_->pCalcMappings[i].colName, "VAL");
  }
  EXPECT_TRUE(foundLower);
}

// Mapping slots are matched by the resolved spelling, not by the provider's fold rule: an Oracle
// table can hold both TS and a quoted "ts", and folding the second binding onto the primary
// timestamp's slot would have the reader fetch TS for a virtual column bound to "ts" - a wrong
// value the type check cannot notice because both columns are TIMESTAMP.
TEST_F(MndStreamExtReaderSchemaTest, KeepsCaseSiblingTimestampColumnsInSeparateSlots) {
  source_.type = EXT_SOURCE_ORACLE;
  req_.sourceType = EXT_SOURCE_ORACLE;
  tstrncpy(source_.defaultDatabase, "ORCL", sizeof(source_.defaultDatabase));
  tstrncpy(source_.defaultSchema, "TAOSDATA", sizeof(source_.defaultSchema));
  tstrncpy(req_.identity.tableName, "metrics_ts_case", sizeof(req_.identity.tableName));
  // The quoted lower-case column is listed first so the lookup resolves it exactly; the folded
  // primary timestamp column comes second.
  tstrncpy(columns_[0].colName, "ts", sizeof(columns_[0].colName));
  tstrncpy(columns_[0].extTypeName, "TIMESTAMP(6)", sizeof(columns_[0].extTypeName));
  columns_[0].isRemotePrimaryKey = false;
  tstrncpy(columns_[1].colName, "TS", sizeof(columns_[1].colName));
  tstrncpy(columns_[1].extTypeName, "TIMESTAMP(6)", sizeof(columns_[1].extTypeName));
  columns_[1].isRemotePrimaryKey = true;
  tstrncpy(columns_[2].colName, "V", sizeof(columns_[2].colName));
  tstrncpy(columns_[2].extTypeName, "BINARY_DOUBLE", sizeof(columns_[2].extTypeName));
  meta_.numOfCols = 3;
  meta_.tsPrecision = TSDB_TIME_PRECISION_MICRO;
  taosArrayPopTailBatch(req_.bindings, 1);

  SStreamExtOrigTblReaderBinding sideTs = {};
  sideTs.kind = STREAM_EXT_ORIGTBL_READER_BINDING_CALC;
  sideTs.expectedType = TSDB_DATA_TYPE_TIMESTAMP;
  tstrncpy(sideTs.externalColumn, "ts", sizeof(sideTs.externalColumn));
  ASSERT_NE(nullptr, taosArrayPush(req_.bindings, &sideTs));

  ASSERT_EQ(TSDB_CODE_SUCCESS, build());
  ASSERT_NE(nullptr, spec_);
  ASSERT_EQ(spec_->numCalcMappings, 2);
  EXPECT_STREQ("TS", spec_->pCalcMappings[0].colName);
  EXPECT_STREQ("ts", spec_->pCalcMappings[1].colName);
}

TEST(MndStreamExtReaderDeployTest, BuildsSpecAndConfirmsCompatibleBindingsFromSchema) {
  SExtSourceObj source = {};
  tstrncpy(source.sourceName, "remote", sizeof(source.sourceName));
  source.type = EXT_SOURCE_MYSQL;
  tstrncpy(source.host, "127.0.0.1", sizeof(source.host));
  source.port = 3306;
  tstrncpy(source.user, "reader", sizeof(source.user));
  tstrncpy(source.defaultDatabase, "metrics", sizeof(source.defaultDatabase));
  source.metaVersion = 12;

  SExtColumnDef columns[3] = {};
  tstrncpy(columns[0].colName, "ts", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "timestamp", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  tstrncpy(columns[1].colName, "cpu", sizeof(columns[1].colName));
  tstrncpy(columns[1].extTypeName, "double", sizeof(columns[1].extTypeName));
  tstrncpy(columns[2].colName, "voltage", sizeof(columns[2].colName));
  tstrncpy(columns[2].extTypeName, "float", sizeof(columns[2].extTypeName));
  SExtTableMeta meta = {};
  meta.pCols = columns;
  meta.numOfCols = 3;
  meta.tsPrecision = TSDB_TIME_PRECISION_MICRO;

  SStreamExtOrigTblReaderBinding binding = {};
  binding.bindingId = 11;
  binding.virtualUid = 101;
  binding.virtualSlot = 3;
  binding.kind = STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER;
  binding.expectedType = TSDB_DATA_TYPE_DOUBLE;
  binding.resultColumnSlot = 3;
  tstrncpy(binding.externalColumn, "cpu", sizeof(binding.externalColumn));

  SStreamExtOrigTblReaderBinding calcBinding = {};
  calcBinding.bindingId = 12;
  calcBinding.virtualUid = 101;
  calcBinding.virtualSlot = 4;
  calcBinding.kind = STREAM_EXT_ORIGTBL_READER_BINDING_CALC;
  calcBinding.expectedType = TSDB_DATA_TYPE_FLOAT;
  calcBinding.resultColumnSlot = 4;
  tstrncpy(calcBinding.externalColumn, "voltage", sizeof(calcBinding.externalColumn));

  SStreamExtOrigTblReaderReq req = {};
  req.registryEntryId = 99;
  req.sourceType = EXT_SOURCE_MYSQL;
  req.sourceConfigVersion = source.metaVersion;
  tstrncpy(req.identity.sourceName, "remote", sizeof(req.identity.sourceName));
  tstrncpy(req.identity.dbName, "metrics", sizeof(req.identity.dbName));
  tstrncpy(req.identity.tableName, "cpu", sizeof(req.identity.tableName));
  req.bindings = taosArrayInit(2, sizeof(SStreamExtOrigTblReaderBinding));
  ASSERT_NE(req.bindings, nullptr);
  ASSERT_NE(taosArrayPush(req.bindings, &binding), nullptr);
  ASSERT_NE(taosArrayPush(req.bindings, &calcBinding), nullptr);

  SStreamExtTriggerSpec *spec = nullptr;
  SStreamExtOrigTblReaderRsp rsp = {};
  ASSERT_EQ(msmBuildExtOrigTblReaderSpec(&source, &req, &meta, &spec, &rsp), TSDB_CODE_SUCCESS);
  ASSERT_NE(spec, nullptr);
  EXPECT_STREQ(spec->sourceName, "remote");
  EXPECT_STREQ(spec->extDb, "metrics");
  EXPECT_STREQ(spec->extTable, "cpu");
  EXPECT_STREQ(spec->tsColumn, "ts");
  ASSERT_NE(spec->triggerColumns, nullptr);
  ASSERT_EQ(taosArrayGetSize(spec->triggerColumns), 2);
  EXPECT_STREQ(static_cast<const char *>(taosArrayGet(spec->triggerColumns, 0)), "ts");
  EXPECT_STREQ(static_cast<const char *>(taosArrayGet(spec->triggerColumns, 1)), "cpu");
  ASSERT_NE(spec->pColMappings, nullptr);
  EXPECT_EQ(spec->numColMappings, 2);
  EXPECT_EQ(spec->pColMappings[0].tdType.type, TSDB_DATA_TYPE_TIMESTAMP);
  EXPECT_EQ(spec->pColMappings[1].tdType.type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_NE(spec->calcColumns, nullptr);
  ASSERT_EQ(taosArrayGetSize(spec->calcColumns), 2);
  EXPECT_STREQ(static_cast<const char *>(taosArrayGet(spec->calcColumns, 0)), "ts");
  EXPECT_STREQ(static_cast<const char *>(taosArrayGet(spec->calcColumns, 1)), "voltage");
  ASSERT_NE(spec->pCalcMappings, nullptr);
  EXPECT_EQ(spec->numCalcMappings, 2);
  EXPECT_EQ(spec->pCalcMappings[0].tdType.type, TSDB_DATA_TYPE_TIMESTAMP);
  EXPECT_EQ(spec->pCalcMappings[1].tdType.type, TSDB_DATA_TYPE_FLOAT);
  EXPECT_EQ(rsp.acceptedTriggerBindingCount, 1);
  EXPECT_EQ(rsp.acceptedCalcBindingCount, 1);
  ASSERT_NE(rsp.results, nullptr);
  ASSERT_EQ(taosArrayGetSize(rsp.results), 2);
  const SStreamExtOrigTblReaderResult *result =
      static_cast<const SStreamExtOrigTblReaderResult *>(taosArrayGet(rsp.results, 0));
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(result->binding.resultColumnSlot, 1);
  EXPECT_EQ(result->actualType, TSDB_DATA_TYPE_DOUBLE);
  const SStreamExtOrigTblReaderResult *calcResult =
      static_cast<const SStreamExtOrigTblReaderResult *>(taosArrayGet(rsp.results, 1));
  ASSERT_NE(calcResult, nullptr);
  EXPECT_EQ(calcResult->code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(calcResult->binding.kind, STREAM_EXT_ORIGTBL_READER_BINDING_CALC);
  EXPECT_EQ(calcResult->binding.resultColumnSlot, 1);
  EXPECT_EQ(calcResult->actualType, TSDB_DATA_TYPE_FLOAT);

  tFreeSStreamExtOrigTblReaderRsp(&rsp);
  tFreeSStreamExtTriggerSpec(spec);
  tFreeSStreamExtOrigTblReaderReq(&req);
}

TEST_F(MndStreamExtReaderRuntimeTest, DeploysAndReusesExternalOriginalReaderFromSchema) {
  char streamName[] = "test.ext_stream";
  char triggerTbl[] = "vmeters";
  SCMCreateStreamReq createReq = {};
  createReq.name = streamName;
  createReq.streamId = 7001;
  createReq.triggerTblName = triggerTbl;
  createReq.triggerTblType = TSDB_VIRTUAL_NORMAL_TABLE;

  SStreamObj stream = {};
  stream.pCreate = &createReq;
  stream.mainSnodeId = snodeId_;

  SStmStatus status = {};
  status.streamName = streamName;
  status.pCreate = &createReq;

  SStmTaskStatus triggerTask = {};
  triggerTask.id.taskId = 8001;
  triggerTask.id.nodeId = snodeId_;
  triggerTask.type = STREAM_TRIGGER_TASK;
  status.triggerTask = &triggerTask;

  SExtSourceObj source = {};
  tstrncpy(source.sourceName, "remote", sizeof(source.sourceName));
  source.type = EXT_SOURCE_MYSQL;
  tstrncpy(source.host, "127.0.0.1", sizeof(source.host));
  source.port = 3306;
  tstrncpy(source.user, "reader", sizeof(source.user));
  tstrncpy(source.defaultDatabase, "metrics", sizeof(source.defaultDatabase));
  source.metaVersion = 12;

  std::vector<SExtColumnDef> columns(256);
  tstrncpy(columns[0].colName, "ts", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "timestamp", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  tstrncpy(columns[1].colName, "cpu", sizeof(columns[1].colName));
  tstrncpy(columns[1].extTypeName, "double", sizeof(columns[1].extTypeName));
  SExtTableMeta meta = {};
  for (size_t i = 2; i < columns.size(); ++i) {
    snprintf(columns[i].colName, sizeof(columns[i].colName), "extra_%u", (unsigned)i);
    tstrncpy(columns[i].extTypeName, "double", sizeof(columns[i].extTypeName));
  }
  meta.pCols = columns.data();
  meta.numOfCols = static_cast<int32_t>(columns.size());

  SStreamExtOrigTblReaderBinding binding = {};
  binding.bindingId = 11;
  binding.kind = STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER;
  binding.virtualUid = 101;
  binding.virtualSlot = 3;
  binding.expectedType = TSDB_DATA_TYPE_DOUBLE;
  binding.resultColumnSlot = 3;
  tstrncpy(binding.externalColumn, "cpu", sizeof(binding.externalColumn));

  SStreamExtOrigTblReaderReq req = {};
  req.registryEntryId = 99;
  req.deploymentGeneration = 5;
  req.protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
  req.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START;
  req.sourceType = EXT_SOURCE_MYSQL;
  req.sourceConfigVersion = source.metaVersion;
  tstrncpy(req.identity.sourceName, "remote", sizeof(req.identity.sourceName));
  tstrncpy(req.identity.dbName, "metrics", sizeof(req.identity.dbName));
  tstrncpy(req.identity.tableName, "cpu", sizeof(req.identity.tableName));
  req.bindings = taosArrayInit(1, sizeof(SStreamExtOrigTblReaderBinding));
  ASSERT_NE(req.bindings, nullptr);
  ASSERT_NE(taosArrayPush(req.bindings, &binding), nullptr);

  SStmGrpCtx ctx = {};
  ctx.currTs = 1000;
  SStreamExtOrigTblReaderRsp firstRsp = {};
  // The background batch holds both tables' prepared payloads before either is queued.
  SStreamExtOrigTblReaderReq secondTable = req;
  ++secondTable.registryEntryId;
  tstrncpy(secondTable.identity.tableName, "other_metrics", sizeof(secondTable.identity.tableName));
  SStmTaskToDeployExt        prepared[2] = {};
  SStreamExtOrigTblReaderRsp otherRsp = {};
  ASSERT_EQ(0,
            msmPrepareExtOrigTblReader(createReq.streamId, &createReq, &source, &req, &meta, &prepared[0], &firstRsp));
  ASSERT_EQ(0, msmPrepareExtOrigTblReader(createReq.streamId, &createReq, &source, &secondTable, &meta, &prepared[1],
                                          &otherRsp));
  auto checkRetained = [](const SStreamExtTriggerSpec *spec) {
    ASSERT_NE(nullptr, spec);
    EXPECT_EQ(2, spec->numColMappings);
    EXPECT_EQ(1, spec->numCalcMappings);
    EXPECT_LE(spec->triggerColumns->capacity, TARRAY_MIN_SIZE);
    EXPECT_LE(spec->calcColumns->capacity, TARRAY_MIN_SIZE);
    EXPECT_LE(taosMemorySize(spec->pColMappings), 2 * sizeof(SExtColTypeMapping) + 64);
    EXPECT_LE(taosMemorySize(spec->pCalcMappings), sizeof(SExtColTypeMapping) + 64);
  };
  for (auto &entry : prepared) checkRetained(entry.deploy.msg.reader.pExtSpec);
  SStmTaskStatus *firstSlot = nullptr, *otherSlot = nullptr;
  ASSERT_EQ(0, msmPublishExtOrigTblReader(&ctx, &status, &stream, &req, &prepared[0], &firstSlot, &firstRsp));
  SArray *nextBlock = taosArrayInit(MST_ORIGINAL_READER_LIST_SIZE, sizeof(SStmTaskStatus));
  ASSERT_NE(nullptr, nextBlock);
  ASSERT_NE(nullptr, taosArrayPush(status.trigOReaders, &nextBlock));
  ASSERT_EQ(0, msmPublishExtOrigTblReader(&ctx, &status, &stream, &secondTable, &prepared[1], &otherSlot, &otherRsp));
  EXPECT_EQ(firstRsp.readerTaskAddr.taskId, prepared[0].deploy.task.taskId);
  EXPECT_EQ(otherRsp.readerTaskAddr.taskId, prepared[1].deploy.task.taskId);
  EXPECT_EQ(snodeId_, prepared[1].deploy.task.nodeId);
  EXPECT_NE(0, prepared[1].deploy.task.seriousId);
  for (auto &entry : prepared) mstDestroySStmTaskToDeployExt(&entry);
  ASSERT_EQ(firstRsp.code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(firstRsp.readerTaskAddr.nodeId, snodeId_);
  EXPECT_NE(firstRsp.readerTaskAddr.taskId, 0);
  EXPECT_EQ(firstRsp.acceptedTriggerBindingCount, 1);
  ASSERT_EQ(atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum), 2);

  SStmSnodeTasksDeploy *deploy = static_cast<SStmSnodeTasksDeploy *>(
      taosHashGet(mStreamMgmt.toDeploySnodeMap, &snodeId_, sizeof(snodeId_)));
  ASSERT_NE(deploy, nullptr);
  ASSERT_NE(deploy->calcReaderList, nullptr);
  ASSERT_EQ(taosArrayGetSize(deploy->calcReaderList), 2);
  SStmTaskToDeployExt *deployTask =
      static_cast<SStmTaskToDeployExt *>(taosArrayGet(deploy->calcReaderList, 0));
  ASSERT_NE(deployTask, nullptr);
  for (size_t i = 0; i < taosArrayGetSize(deploy->calcReaderList); ++i) {
    auto *queued = static_cast<SStmTaskToDeployExt *>(taosArrayGet(deploy->calcReaderList, i));
    checkRetained(queued->deploy.msg.reader.pExtSpec);
  }
  EXPECT_TRUE(deployTask->ownsExtSpec);
  ASSERT_NE(deployTask->deploy.msg.reader.pExtSpec, nullptr);
  EXPECT_TRUE(deployTask->deploy.msg.reader.ownsExtSpec);

  SStreamExtOrigTblReaderRsp secondRsp = {};
  ASSERT_EQ(DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &secondTable, &meta, &secondRsp),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(secondRsp.readerTaskAddr.taskId, otherRsp.readerTaskAddr.taskId);
  EXPECT_EQ(secondRsp.readerTaskAddr.nodeId, otherRsp.readerTaskAddr.nodeId);
  EXPECT_EQ(atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum), 2);

  tFreeSStreamExtOrigTblReaderRsp(&firstRsp);
  tFreeSStreamExtOrigTblReaderRsp(&secondRsp);
  tFreeSStreamExtOrigTblReaderRsp(&otherRsp);
  tFreeSStreamExtOrigTblReaderReq(&req);
  destroyTrigOReaders(&status);
}

TEST_F(MndStreamExtReaderRuntimeTest, RejectsExternalOriginalReaderWhenCreateStartCapabilityIsMissing) {
  char streamName[] = "test.ext_cap_stream";
  char triggerTbl[] = "vmeters";
  SCMCreateStreamReq createReq = {};
  createReq.name = streamName;
  createReq.streamId = 7006;
  createReq.triggerTblName = triggerTbl;
  createReq.triggerTblType = TSDB_VIRTUAL_NORMAL_TABLE;

  SStreamObj stream = {};
  stream.pCreate = &createReq;
  stream.mainSnodeId = snodeId_;

  SStmStatus status = {};
  status.streamName = streamName;
  status.pCreate = &createReq;

  SStmTaskStatus triggerTask = {};
  triggerTask.id.taskId = 8006;
  triggerTask.id.nodeId = snodeId_;
  triggerTask.type = STREAM_TRIGGER_TASK;
  status.triggerTask = &triggerTask;

  SExtSourceObj source = {};
  tstrncpy(source.sourceName, "remote", sizeof(source.sourceName));
  source.type = EXT_SOURCE_MYSQL;
  tstrncpy(source.defaultDatabase, "metrics", sizeof(source.defaultDatabase));
  source.metaVersion = 12;

  SExtColumnDef columns[2] = {};
  tstrncpy(columns[0].colName, "ts", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "timestamp", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  tstrncpy(columns[1].colName, "cpu", sizeof(columns[1].colName));
  tstrncpy(columns[1].extTypeName, "double", sizeof(columns[1].extTypeName));
  SExtTableMeta meta = {};
  meta.pCols = columns;
  meta.numOfCols = 2;

  SStreamExtOrigTblReaderBinding binding = {};
  binding.bindingId = 61;
  binding.kind = STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER;
  binding.virtualUid = 601;
  binding.virtualSlot = 1;
  binding.expectedType = TSDB_DATA_TYPE_DOUBLE;
  tstrncpy(binding.externalColumn, "cpu", sizeof(binding.externalColumn));

  SStreamExtOrigTblReaderReq req = {};
  req.registryEntryId = 399;
  req.protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
  req.capabilities = 0;
  req.sourceType = EXT_SOURCE_MYSQL;
  req.sourceConfigVersion = source.metaVersion;
  tstrncpy(req.identity.sourceName, "remote", sizeof(req.identity.sourceName));
  tstrncpy(req.identity.dbName, "metrics", sizeof(req.identity.dbName));
  tstrncpy(req.identity.tableName, "cpu", sizeof(req.identity.tableName));
  req.bindings = taosArrayInit(1, sizeof(SStreamExtOrigTblReaderBinding));
  ASSERT_NE(req.bindings, nullptr);
  ASSERT_NE(taosArrayPush(req.bindings, &binding), nullptr);

  SStmGrpCtx ctx = {};
  ctx.currTs = 1000;
  SStreamExtOrigTblReaderRsp rsp = {};
  ASSERT_EQ(DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &req, &meta, &rsp), TSDB_CODE_OPS_NOT_SUPPORT);
  EXPECT_EQ(rsp.code, TSDB_CODE_OPS_NOT_SUPPORT);
  EXPECT_EQ(msmGetTrigOReaderSize(status.trigOReaders), 0);
  EXPECT_EQ(atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum), 0);

  tFreeSStreamExtOrigTblReaderRsp(&rsp);
  tFreeSStreamExtOrigTblReaderReq(&req);
  destroyTrigOReaders(&status);
}

TEST_F(MndStreamExtReaderRuntimeTest, PatchRedeploysExistingExternalOriginalReader) {
  char streamName[] = "test.ext_patch_stream";
  char triggerTbl[] = "vmeters";
  SCMCreateStreamReq createReq = {};
  createReq.name = streamName;
  createReq.streamId = 7004;
  createReq.triggerTblName = triggerTbl;
  createReq.triggerTblType = TSDB_VIRTUAL_NORMAL_TABLE;

  SStreamObj stream = {};
  stream.pCreate = &createReq;
  stream.mainSnodeId = snodeId_;
  SStmStatus status = {};
  status.streamName = streamName;
  status.pCreate = &createReq;
  SStmTaskStatus triggerTask = {};
  triggerTask.id.taskId = 8004;
  triggerTask.id.nodeId = snodeId_;
  triggerTask.type = STREAM_TRIGGER_TASK;
  status.triggerTask = &triggerTask;

  SExtSourceObj source = {};
  tstrncpy(source.sourceName, "remote", sizeof(source.sourceName));
  source.type = EXT_SOURCE_MYSQL;
  tstrncpy(source.defaultDatabase, "metrics", sizeof(source.defaultDatabase));
  source.metaVersion = 12;
  SExtColumnDef columns[2] = {};
  tstrncpy(columns[0].colName, "ts", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "timestamp", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  tstrncpy(columns[1].colName, "cpu", sizeof(columns[1].colName));
  tstrncpy(columns[1].extTypeName, "double", sizeof(columns[1].extTypeName));
  SExtTableMeta meta = {};
  meta.pCols = columns;
  meta.numOfCols = 2;

  SStreamExtOrigTblReaderBinding binding = {};
  binding.bindingId = 41;
  binding.kind = STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER;
  binding.virtualUid = 401;
  binding.virtualSlot = 1;
  tstrncpy(binding.externalColumn, "cpu", sizeof(binding.externalColumn));
  SStreamExtOrigTblReaderReq req = {};
  req.registryEntryId = 199;
  req.protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
  req.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START;
  req.sourceType = EXT_SOURCE_MYSQL;
  req.sourceConfigVersion = source.metaVersion;
  tstrncpy(req.identity.sourceName, "remote", sizeof(req.identity.sourceName));
  tstrncpy(req.identity.dbName, "metrics", sizeof(req.identity.dbName));
  tstrncpy(req.identity.tableName, "cpu", sizeof(req.identity.tableName));
  req.bindings = taosArrayInit(2, sizeof(SStreamExtOrigTblReaderBinding));
  ASSERT_NE(req.bindings, nullptr);
  ASSERT_NE(taosArrayPush(req.bindings, &binding), nullptr);

  SStmGrpCtx ctx = {};
  ctx.currTs = 1000;
  SStreamExtOrigTblReaderRsp firstRsp = {};
  ASSERT_EQ(DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &req, &meta, &firstRsp), TSDB_CODE_SUCCESS);
  ASSERT_NE(firstRsp.readerTaskAddr.taskId, 0);

  taosHashCleanup(mStreamMgmt.toDeploySnodeMap);
  mStreamMgmt.toDeploySnodeMap =
      taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  ASSERT_NE(mStreamMgmt.toDeploySnodeMap, nullptr);
  taosHashSetFreeFp(mStreamMgmt.toDeploySnodeMap, mstDestroySStmSnodeTasksDeploy);
  mStreamMgmt.toDeploySnodeTaskNum = 0;

  SStreamExtOrigTblReaderBinding added = binding;
  added.bindingId = 42;
  added.virtualUid = 402;
  ASSERT_NE(taosArrayPush(req.bindings, &added), nullptr);
  req.capabilities |= STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
  SMStreamHbRspMsg hbRsp = {};
  ctx.pRsp = &hbRsp;
  SStreamExtOrigTblReaderRsp patchRsp = {};
  ASSERT_EQ(DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &req, &meta, &patchRsp), TSDB_CODE_SUCCESS);
  EXPECT_EQ(patchRsp.readerTaskAddr.taskId, firstRsp.readerTaskAddr.taskId);
  EXPECT_EQ(patchRsp.acceptedTriggerBindingCount, 2);
  EXPECT_EQ(atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum), 1);
  ASSERT_NE(hbRsp.undeploy.taskList, nullptr);
  ASSERT_EQ(taosArrayGetSize(hbRsp.undeploy.taskList), 1);
  auto* undeploy = static_cast<SStreamTaskUndeploy*>(taosArrayGet(hbRsp.undeploy.taskList, 0));
  ASSERT_NE(undeploy, nullptr);
  EXPECT_EQ(undeploy->task.taskId, firstRsp.readerTaskAddr.taskId);
  EXPECT_FALSE(undeploy->undeployMsg.doCheckpoint);
  EXPECT_TRUE(undeploy->undeployMsg.doCleanup);

  tFreeSMStreamHbRspMsg(&hbRsp);
  tFreeSStreamExtOrigTblReaderRsp(&firstRsp);
  tFreeSStreamExtOrigTblReaderRsp(&patchRsp);
  tFreeSStreamExtOrigTblReaderReq(&req);
  destroyTrigOReaders(&status);
}

TEST_F(MndStreamExtReaderRuntimeTest, CleanupAndRecreatePreservePeerRegistrations) {
  char streamName[] = "test.ext_cleanup_stream";
  char triggerTbl[] = "vmeters";
  SCMCreateStreamReq createReq = {};
  createReq.name = streamName;
  createReq.streamId = 7005;
  createReq.triggerTblName = triggerTbl;
  createReq.triggerTblType = TSDB_VIRTUAL_NORMAL_TABLE;

  SStreamObj stream = {};
  stream.pCreate = &createReq;
  stream.mainSnodeId = snodeId_;
  SStmStatus status = {};
  status.streamName = streamName;
  status.pCreate = &createReq;
  SStmTaskStatus triggerTask = {};
  triggerTask.id.taskId = 8005;
  triggerTask.id.nodeId = snodeId_;
  triggerTask.type = STREAM_TRIGGER_TASK;
  status.triggerTask = &triggerTask;

  SExtSourceObj source = {};
  tstrncpy(source.sourceName, "remote", sizeof(source.sourceName));
  source.type = EXT_SOURCE_MYSQL;
  tstrncpy(source.defaultDatabase, "metrics", sizeof(source.defaultDatabase));
  source.metaVersion = 13;
  SExtColumnDef columns[2] = {};
  tstrncpy(columns[0].colName, "ts", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "timestamp", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  tstrncpy(columns[1].colName, "cpu", sizeof(columns[1].colName));
  tstrncpy(columns[1].extTypeName, "double", sizeof(columns[1].extTypeName));
  SExtTableMeta meta = {};
  meta.pCols = columns;
  meta.numOfCols = 2;

  SStreamExtOrigTblReaderBinding binding = {};
  binding.bindingId = 51;
  binding.kind = STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER;
  binding.virtualUid = 501;
  binding.virtualSlot = 1;
  tstrncpy(binding.externalColumn, "cpu", sizeof(binding.externalColumn));
  SStreamExtOrigTblReaderReq req = {};
  req.registryEntryId = 299;
  req.protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
  req.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START;
  req.sourceType = EXT_SOURCE_MYSQL;
  req.sourceConfigVersion = source.metaVersion;
  tstrncpy(req.identity.sourceName, "remote", sizeof(req.identity.sourceName));
  tstrncpy(req.identity.dbName, "metrics", sizeof(req.identity.dbName));
  tstrncpy(req.identity.tableName, "cpu", sizeof(req.identity.tableName));
  req.bindings = taosArrayInit(1, sizeof(SStreamExtOrigTblReaderBinding));
  ASSERT_NE(req.bindings, nullptr);
  ASSERT_NE(taosArrayPush(req.bindings, &binding), nullptr);

  SStmGrpCtx ctx = {};
  ctx.currTs = 1000;
  SStreamExtOrigTblReaderRsp firstRsp = {};
  ASSERT_EQ(DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &req, &meta, &firstRsp), TSDB_CODE_SUCCESS);
  ASSERT_NE(firstRsp.readerTaskAddr.taskId, 0);

  auto peerReq = req;
  ++peerReq.registryEntryId;
  tstrncpy(peerReq.identity.tableName, "peer_cpu", sizeof(peerReq.identity.tableName));
  SStreamExtOrigTblReaderRsp peerRsp = {};
  ASSERT_EQ(0, DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &peerReq, &meta, &peerRsp));
  const int64_t peerKey[] = {createReq.streamId, peerRsp.readerTaskAddr.taskId};
  auto        **peer = static_cast<SStmTaskStatus **>(taosHashGet(mStreamMgmt.taskMap, peerKey, sizeof(peerKey)));
  ASSERT_NE(nullptr, peer);
  auto *peerSlot = *peer;

  taosHashCleanup(mStreamMgmt.toDeploySnodeMap);
  mStreamMgmt.toDeploySnodeMap =
      taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  ASSERT_NE(mStreamMgmt.toDeploySnodeMap, nullptr);
  taosHashSetFreeFp(mStreamMgmt.toDeploySnodeMap, mstDestroySStmSnodeTasksDeploy);
  mStreamMgmt.toDeploySnodeTaskNum = 0;

  taosArrayClear(req.bindings);
  req.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
  SMStreamHbRspMsg hbRsp = {};
  ctx.pRsp = &hbRsp;
  SStreamExtOrigTblReaderRsp cleanupRsp = {};
  {
    Stub fail;
    fail.set(
        taosHashPut, +[](SHashObj *, const void *, size_t, const void *, size_t) -> int32_t {
          return terrno = TSDB_CODE_OUT_OF_MEMORY;
        });
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY,
              DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &req, &meta, &cleanupRsp));
  }
  const int64_t firstKey[] = {createReq.streamId, firstRsp.readerTaskAddr.taskId};
  EXPECT_NE(nullptr, taosHashGet(mStreamMgmt.taskMap, firstKey, sizeof(firstKey)));
  EXPECT_EQ(0, taosHashGetSize(status.retiredExtReaders));
  EXPECT_EQ(0u, taosArrayGetSize(hbRsp.undeploy.taskList));
  cleanupRsp = {};
  ASSERT_EQ(DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &req, &meta, &cleanupRsp), TSDB_CODE_SUCCESS);
  EXPECT_EQ(cleanupRsp.readerTaskAddr.taskId, firstRsp.readerTaskAddr.taskId);
  EXPECT_EQ(cleanupRsp.acceptedTriggerBindingCount, 0);
  EXPECT_EQ(cleanupRsp.acceptedCalcBindingCount, 0);
  EXPECT_EQ(cleanupRsp.results, nullptr);
  EXPECT_EQ(atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum), 0);
  ASSERT_NE(hbRsp.undeploy.taskList, nullptr);
  ASSERT_EQ(taosArrayGetSize(hbRsp.undeploy.taskList), 1);
  auto* undeploy = static_cast<SStreamTaskUndeploy*>(taosArrayGet(hbRsp.undeploy.taskList, 0));
  ASSERT_NE(undeploy, nullptr);
  EXPECT_EQ(undeploy->task.taskId, firstRsp.readerTaskAddr.taskId);
  EXPECT_FALSE(undeploy->undeployMsg.doCheckpoint);
  EXPECT_TRUE(undeploy->undeployMsg.doCleanup);
  auto* snodeStatus = static_cast<SStmSnodeStatus*>(
      taosHashGet(mStreamMgmt.snodeMap, &snodeId_, sizeof(snodeId_)));
  ASSERT_NE(snodeStatus, nullptr);
  auto* streamStatus = static_cast<SStmSnodeStreamStatus*>(
      taosHashGet(snodeStatus->streamTasks, &createReq.streamId, sizeof(createReq.streamId)));
  ASSERT_NE(streamStatus, nullptr);
  EXPECT_EQ(msmGetTrigOReaderSize(status.trigOReaders), 2);

  ASSERT_NE(taosArrayPush(req.bindings, &binding), nullptr);
  req.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START;
  SStreamExtOrigTblReaderRsp recreateRsp = {};
  ASSERT_EQ(DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &req, &meta, &recreateRsp), TSDB_CODE_SUCCESS);
  EXPECT_EQ(0, taosHashGetSize(status.retiredExtReaders));
  EXPECT_NE(recreateRsp.readerTaskAddr.taskId, firstRsp.readerTaskAddr.taskId);
  EXPECT_EQ(atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum), 1);
  EXPECT_EQ(msmGetTrigOReaderSize(status.trigOReaders), 2);
  req.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
  taosArrayClear(req.bindings);
  for (int i = 0; i < 2; ++i) {
    SStreamExtOrigTblReaderRsp reply = {};
    ASSERT_EQ(0, DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &req, &meta, &reply));
    EXPECT_EQ(recreateRsp.readerTaskAddr.taskId, reply.readerTaskAddr.taskId);
    EXPECT_EQ(snodeId_, reply.readerTaskAddr.nodeId);
  }
  stream.mainSnodeId = snodeId_ + 1;
  req.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START;
  SStreamExtOrigTblReaderRsp failed = {};
  EXPECT_EQ(TSDB_CODE_MND_STREAM_INTERNAL_ERROR,
            DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &req, &meta, &failed));
  EXPECT_NE(nullptr, taosHashGet(status.retiredExtReaders, &recreateRsp.readerTaskAddr.taskId,
                                 sizeof(recreateRsp.readerTaskAddr.taskId)));
  tFreeSStreamExtOrigTblReaderRsp(&failed);
  stream.mainSnodeId = snodeId_;
  req.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_PATCH;
  SStreamExtOrigTblReaderRsp previous = {};
  ASSERT_EQ(0, DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &req, &meta, &previous));
  EXPECT_EQ(recreateRsp.readerTaskAddr.taskId, previous.readerTaskAddr.taskId);
  peer = static_cast<SStmTaskStatus **>(taosHashGet(mStreamMgmt.taskMap, peerKey, sizeof(peerKey)));
  ASSERT_NE(nullptr, peer);
  EXPECT_EQ(peerSlot, *peer);
  EXPECT_EQ(peerReq.registryEntryId, (*peer)->id.uid);
  EXPECT_EQ(peerRsp.readerTaskAddr.taskId, (*peer)->id.taskId);
  ASSERT_EQ(1u, taosArrayGetSize(streamStatus->runners[0]));
  EXPECT_EQ(peerSlot, taosArrayGetP(streamStatus->runners[0], 0));
  SStreamExtOrigTblReaderRsp repeatedPeer = {};
  ASSERT_EQ(0, DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &peerReq, &meta, &repeatedPeer));
  EXPECT_EQ(peerRsp.readerTaskAddr.taskId, repeatedPeer.readerTaskAddr.taskId);
  EXPECT_EQ(snodeId_, repeatedPeer.readerTaskAddr.nodeId);

  tFreeSMStreamHbRspMsg(&hbRsp);
  tFreeSStreamExtOrigTblReaderRsp(&peerRsp);
  tFreeSStreamExtOrigTblReaderRsp(&repeatedPeer);
  tFreeSStreamExtOrigTblReaderRsp(&firstRsp);
  tFreeSStreamExtOrigTblReaderRsp(&cleanupRsp);
  tFreeSStreamExtOrigTblReaderRsp(&recreateRsp);
  tFreeSStreamExtOrigTblReaderReq(&req);
  destroyTrigOReaders(&status);
}

TEST_F(MndStreamExtReaderRuntimeTest, FailedBindingDoesNotCreateAdditionalExternalOriginalReader) {
  char streamName[] = "test.ext_stream";
  char triggerTbl[] = "vmeters";
  SCMCreateStreamReq createReq = {};
  createReq.name = streamName;
  createReq.streamId = 7002;
  createReq.triggerTblName = triggerTbl;
  createReq.triggerTblType = TSDB_VIRTUAL_NORMAL_TABLE;

  SStreamObj stream = {};
  stream.pCreate = &createReq;
  stream.mainSnodeId = snodeId_;

  SStmStatus status = {};
  status.streamName = streamName;
  status.pCreate = &createReq;

  SStmTaskStatus triggerTask = {};
  triggerTask.id.taskId = 8002;
  triggerTask.id.nodeId = snodeId_;
  triggerTask.type = STREAM_TRIGGER_TASK;
  status.triggerTask = &triggerTask;

  SExtSourceObj source = {};
  tstrncpy(source.sourceName, "remote", sizeof(source.sourceName));
  source.type = EXT_SOURCE_MYSQL;
  tstrncpy(source.host, "127.0.0.1", sizeof(source.host));
  source.port = 3306;
  tstrncpy(source.user, "reader", sizeof(source.user));
  tstrncpy(source.defaultDatabase, "metrics", sizeof(source.defaultDatabase));
  source.metaVersion = 12;

  SExtColumnDef columns[2] = {};
  tstrncpy(columns[0].colName, "ts", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "timestamp", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  tstrncpy(columns[1].colName, "cpu", sizeof(columns[1].colName));
  tstrncpy(columns[1].extTypeName, "double", sizeof(columns[1].extTypeName));
  SExtTableMeta meta = {};
  meta.pCols = columns;
  meta.numOfCols = 2;

  SStreamExtOrigTblReaderBinding binding = {};
  binding.bindingId = 11;
  binding.kind = STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER;
  binding.virtualUid = 101;
  binding.virtualSlot = 3;
  binding.expectedType = TSDB_DATA_TYPE_DOUBLE;
  binding.resultColumnSlot = 3;
  tstrncpy(binding.externalColumn, "cpu", sizeof(binding.externalColumn));

  SStreamExtOrigTblReaderReq goodReq = {};
  goodReq.registryEntryId = 99;
  goodReq.deploymentGeneration = 5;
  goodReq.protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
  goodReq.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START;
  goodReq.sourceType = EXT_SOURCE_MYSQL;
  goodReq.sourceConfigVersion = source.metaVersion;
  tstrncpy(goodReq.identity.sourceName, "remote", sizeof(goodReq.identity.sourceName));
  tstrncpy(goodReq.identity.dbName, "metrics", sizeof(goodReq.identity.dbName));
  tstrncpy(goodReq.identity.tableName, "cpu", sizeof(goodReq.identity.tableName));
  goodReq.bindings = taosArrayInit(1, sizeof(SStreamExtOrigTblReaderBinding));
  ASSERT_NE(goodReq.bindings, nullptr);
  ASSERT_NE(taosArrayPush(goodReq.bindings, &binding), nullptr);

  SStmGrpCtx ctx = {};
  ctx.currTs = 1000;
  SStreamExtOrigTblReaderRsp goodRsp = {};
  ASSERT_EQ(DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &goodReq, &meta, &goodRsp), TSDB_CODE_SUCCESS);
  ASSERT_EQ(goodRsp.code, TSDB_CODE_SUCCESS);
  ASSERT_EQ(msmGetTrigOReaderSize(status.trigOReaders), 1);
  ASSERT_EQ(atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum), 1);

  SStreamExtOrigTblReaderBinding badBinding = binding;
  badBinding.bindingId = 12;
  tstrncpy(badBinding.externalColumn, "missing_col", sizeof(badBinding.externalColumn));

  SStreamExtOrigTblReaderReq badReq = goodReq;
  badReq.registryEntryId = 100;
  badReq.bindings = taosArrayInit(1, sizeof(SStreamExtOrigTblReaderBinding));
  ASSERT_NE(badReq.bindings, nullptr);
  ASSERT_NE(taosArrayPush(badReq.bindings, &badBinding), nullptr);

  SStreamExtOrigTblReaderRsp badRsp = {};
  ASSERT_EQ(DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &badReq, &meta, &badRsp),
            TSDB_CODE_EXT_COLUMN_NOT_EXIST);
  EXPECT_EQ(badRsp.code, TSDB_CODE_EXT_COLUMN_NOT_EXIST);
  EXPECT_EQ(msmGetTrigOReaderSize(status.trigOReaders), 1);
  EXPECT_EQ(atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum), 1);

  tFreeSStreamExtOrigTblReaderRsp(&goodRsp);
  tFreeSStreamExtOrigTblReaderRsp(&badRsp);
  tFreeSStreamExtOrigTblReaderReq(&goodReq);
  tFreeSStreamExtOrigTblReaderReq(&badReq);
  destroyTrigOReaders(&status);
}

TEST_F(MndStreamExtReaderRuntimeTest, RollsBackExternalOriginalReaderWhenSnodeRegistrationFails) {
  char streamName[] = "test.ext_stream";
  char triggerTbl[] = "vmeters";
  SCMCreateStreamReq createReq = {};
  createReq.name = streamName;
  createReq.streamId = 7003;
  createReq.triggerTblName = triggerTbl;
  createReq.triggerTblType = TSDB_VIRTUAL_NORMAL_TABLE;

  SStreamObj stream = {};
  stream.pCreate = &createReq;
  stream.mainSnodeId = snodeId_;

  SStmStatus status = {};
  status.streamName = streamName;
  status.pCreate = &createReq;

  SStmTaskStatus triggerTask = {};
  triggerTask.id.taskId = 8003;
  triggerTask.id.nodeId = snodeId_;
  triggerTask.type = STREAM_TRIGGER_TASK;
  status.triggerTask = &triggerTask;

  SExtSourceObj source = {};
  tstrncpy(source.sourceName, "remote", sizeof(source.sourceName));
  source.type = EXT_SOURCE_MYSQL;
  tstrncpy(source.host, "127.0.0.1", sizeof(source.host));
  source.port = 3306;
  tstrncpy(source.user, "reader", sizeof(source.user));
  tstrncpy(source.defaultDatabase, "metrics", sizeof(source.defaultDatabase));
  source.metaVersion = 12;

  SExtColumnDef columns[2] = {};
  tstrncpy(columns[0].colName, "ts", sizeof(columns[0].colName));
  tstrncpy(columns[0].extTypeName, "timestamp", sizeof(columns[0].extTypeName));
  columns[0].isPrimaryKey = true;
  tstrncpy(columns[1].colName, "cpu", sizeof(columns[1].colName));
  tstrncpy(columns[1].extTypeName, "double", sizeof(columns[1].extTypeName));
  SExtTableMeta meta = {};
  meta.pCols = columns;
  meta.numOfCols = 2;

  SStreamExtOrigTblReaderBinding binding = {};
  binding.bindingId = 11;
  binding.kind = STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER;
  binding.virtualUid = 101;
  binding.virtualSlot = 3;
  binding.expectedType = TSDB_DATA_TYPE_DOUBLE;
  binding.resultColumnSlot = 3;
  tstrncpy(binding.externalColumn, "cpu", sizeof(binding.externalColumn));

  SStreamExtOrigTblReaderReq req = {};
  req.registryEntryId = 99;
  req.deploymentGeneration = 5;
  req.protocolVersion = STREAM_EXT_ORIGTBL_READER_PROTOCOL_VERSION;
  req.capabilities = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START;
  req.sourceType = EXT_SOURCE_MYSQL;
  req.sourceConfigVersion = source.metaVersion;
  tstrncpy(req.identity.sourceName, "remote", sizeof(req.identity.sourceName));
  tstrncpy(req.identity.dbName, "metrics", sizeof(req.identity.dbName));
  tstrncpy(req.identity.tableName, "cpu", sizeof(req.identity.tableName));
  req.bindings = taosArrayInit(1, sizeof(SStreamExtOrigTblReaderBinding));
  ASSERT_NE(req.bindings, nullptr);
  ASSERT_NE(taosArrayPush(req.bindings, &binding), nullptr);

  ASSERT_EQ(taosHashRemove(mStreamMgmt.snodeMap, &snodeId_, sizeof(snodeId_)), TSDB_CODE_SUCCESS);

  SStmGrpCtx ctx = {};
  ctx.currTs = 1000;
  SStreamExtOrigTblReaderRsp rsp = {};
  ASSERT_EQ(DeployExtReaderFromMeta(&ctx, &status, &stream, &source, &req, &meta, &rsp),
            TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(msmGetTrigOReaderSize(status.trigOReaders), 0);
  EXPECT_EQ(atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum), 0);

  SStmSnodeTasksDeploy *deploy = static_cast<SStmSnodeTasksDeploy *>(
      taosHashGet(mStreamMgmt.toDeploySnodeMap, &snodeId_, sizeof(snodeId_)));
  if (deploy != nullptr && deploy->triggerList != nullptr) {
    EXPECT_EQ(taosArrayGetSize(deploy->triggerList), 0);
  }

  tFreeSStreamExtOrigTblReaderRsp(&rsp);
  tFreeSStreamExtOrigTblReaderReq(&req);
  destroyTrigOReaders(&status);
}
#endif

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
  SMnode mnode = {};
  int8_t savedActive = atomic_exchange_8(&mStreamMgmt.active, 1);

  taosWLockLatch(&queue_.lock);
  std::thread heartbeat([&]() {
    heartbeatCode.store(msmNormalHandleHbMsgImpl(&ctx, nullptr, nullptr));
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

  atomic_store_8(&queue_.draining, 1);
  std::atomic<bool> consumerFinished{false};
  std::thread       consumer([&]() {
    msmDrainStreamActions(&mnode);
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
  atomic_store_8(&mStreamMgmt.active, savedActive);

  EXPECT_TRUE(posted);
  EXPECT_TRUE(writerWaitedForReader);
  EXPECT_EQ(heartbeatCode.load(), TSDB_CODE_SUCCESS);
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

#ifdef TD_ENTERPRISE
  static SExtSourceObj* acquireExtSource(SMnode*, const char* sourceName) {
    gCreateMetadataTest->acquiredExtSources_.emplace_back(sourceName == nullptr ? "" : sourceName);
    return gCreateMetadataTest->returnExtSource_ ? &gCreateMetadataTest->extSource_ : nullptr;
  }

  static void releaseExtSource(SMnode*, SExtSourceObj*) {}
#endif

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
#ifdef TD_ENTERPRISE
    extSource_.type = EXT_SOURCE_MYSQL;
#endif

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
#ifdef TD_ENTERPRISE
    stub_.set(mndAcquireExtSource, acquireExtSource);
    stub_.set(mndReleaseExtSource, releaseExtSource);
#endif
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
    tstrncpy(req.timeConfig.timezone, "UTC", sizeof(req.timeConfig.timezone));
    req.timeConfig.firstDayOfWeek = 1;
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
#ifdef TD_ENTERPRISE
  SExtSourceObj           extSource_ = {};
#endif
  MndMsgFp                handler_ = nullptr;
  SMsgSendInfo*           pendingSend_ = nullptr;
  SRpcMsg                 queuedMsg_ = {};
  std::vector<uint8_t>    callbackBytes_;
  bool                    returnStb_ = false;
  bool                    returnDb_ = true;
#ifdef TD_ENTERPRISE
  bool                    returnExtSource_ = true;
#endif
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
#ifdef TD_ENTERPRISE
  std::vector<std::string> acquiredExtSources_;
#endif
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

#ifdef TD_ENTERPRISE
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
#else
TEST_F(MndStreamCreateMetadataTest, CommunityExtCandidateReturnsDisabled) {
  SCMCreateStreamReq req = makeRequest();
  ASSERT_NE(nullptr, req.pWindowPlan);
  addExtSpec(&req, "cluster.0.ext_source", "remote_meters", "ts");

  EXPECT_EQ(TSDB_CODE_STREAM_EXT_DISABLED, runCreate(&req));
  EXPECT_EQ(0, asyncSendCalls_);
  EXPECT_EQ(0, createTransCalls_);

  freeRequestFixture(&req);
}
#endif

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

extern "C" int32_t msmUpdateRunnerPlans(SStmGrpCtx *, SArray *, SStreamObj *);
extern "C" int32_t msmUpdateLowestPlanSourceAddr(SSubplan *, SStmTaskDeploy *, int64_t, SHashObj *);

static int32_t broadcastReuseSourceAddrStub(SSubplan *, SStmTaskDeploy *, int64_t, SHashObj *) { return 0; }

TEST(MndBroadcastReuseTest, CommonDeploymentQualifiesOldPlansBeforeErasingReferences) {
  // These are persisted payloads without BroadcastInputReusable. Endpoint routing
  // is stubbed; common deployment's qualification, cleanup and codec are real.
  SNode *raw = nullptr;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &raw));
  auto                                               *runner = reinterpret_cast<SSubplan *>(raw);
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> ownedRunner(raw, nodesDestroyNode);
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW, reinterpret_cast<SNode **>(&runner->pNode)));
  auto *window = reinterpret_cast<SExternalWindowPhysiNode *>(runner->pNode);
  window->extWinInputMode = EXT_WIN_INPUT_BROADCAST;
  SNode *child = nullptr;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, &child));
  auto *exchange = reinterpret_cast<SExchangePhysiNode *>(child);
  exchange->srcStartGroupId = exchange->srcEndGroupId = 4;
  ASSERT_EQ(0, nodesListMakeStrictAppend(&runner->pNode->pChildren, child));
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_VALUE, &child));
  auto *ref = reinterpret_cast<SValueNode *>(child);
  ref->translate = true;
  ref->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  ref->datum.i = (INT64_C(4) << 32) | 14;
  ASSERT_EQ(0, nodesListMakeStrictAppend(&runner->pChildren, child));
  char *serialized = nullptr;
  ASSERT_EQ(0, nodesNodeToString(raw, false, &serialized, nullptr));
  std::string legacy(serialized);
  taosMemoryFree(serialized);
  const std::string field = ",\"BroadcastInputReusable\":false";
  auto              pos = legacy.find(field);
  if (pos != std::string::npos) legacy.erase(pos, field.size());
  ASSERT_EQ(0, nodesStringToNode(legacy.c_str(), &raw));
  ownedRunner.reset(raw);
  runner = reinterpret_cast<SSubplan *>(raw);
  window = reinterpret_cast<SExternalWindowPhysiNode *>(runner->pNode);
  ASSERT_FALSE(window->broadcastInputReusable);

  SNode *readerNode = nullptr;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &readerNode));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> ownedReader(readerNode, nodesDestroyNode);
  auto                                               *reader = reinterpret_cast<SSubplan *>(readerNode);
  reader->id.groupId = 4;
  reader->id.subplanId = 14;
  reader->subplanType = SUBPLAN_TYPE_SCAN;
  reader->requiresAncestorContext = true;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, reinterpret_cast<SNode **>(&reader->pNode)));
  auto *scan = reinterpret_cast<STableScanPhysiNode *>(reader->pNode);
  scan->scan.uid = scan->scan.suid = 42;
  scan->scan.tableType = TSDB_SUPER_TABLE;
  scan->scanSeq[0] = 1;
  scan->ratio = 1;
  scan->scanRange = {INT64_MIN, INT64_MAX};
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_COLUMN, &child));
  auto *col = reinterpret_cast<SColumnNode *>(child);
  col->colType = COLUMN_TYPE_COLUMN;
  col->tableId = 42;
  col->colId = 2;
  ASSERT_EQ(0, nodesListMakeStrictAppend(&scan->scan.pScanCols, child));
  SStreamCalcScan calc = {};
  ASSERT_EQ(0, nodesNodeToString(readerNode, false, reinterpret_cast<char **>(&calc.scanPlan), nullptr));
  SStreamObj         stream = {};
  SCMCreateStreamReq create = {};
  stream.pCreate = &create;
  create.calcScanPlanList = taosArrayInit(1, sizeof(SStreamCalcScan));
  ASSERT_NE(nullptr, taosArrayPush(create.calcScanPlanList, &calc));
  auto          *runners = taosArrayInit(1, sizeof(SStmTaskDeploy));
  SStmTaskDeploy deploy = {};
  deploy.msg.runner.pPlan = runner;
  ASSERT_NE(nullptr, taosArrayPush(runners, &deploy));
  Stub stub;
  stub.set(msmUpdateLowestPlanSourceAddr, broadcastReuseSourceAddrStub);
  EXPECT_EQ(0, msmUpdateRunnerPlans(nullptr, runners, &stream));
  EXPECT_TRUE(window->broadcastInputReusable);
  EXPECT_EQ(nullptr, runner->pChildren);
  auto  *updated = reinterpret_cast<SStmTaskDeploy *>(taosArrayGet(runners, 0));
  SNode *decoded = nullptr;
  ASSERT_EQ(0, nodesStringToNode(reinterpret_cast<char *>(updated->msg.runner.pPlan), &decoded));
  EXPECT_TRUE(reinterpret_cast<SExternalWindowPhysiNode *>(reinterpret_cast<SSubplan *>(decoded)->pNode)
                  ->broadcastInputReusable);
  nodesDestroyNode(decoded);
  taosMemoryFree(updated->msg.runner.pPlan);
  taosArrayDestroy(runners);
  taosMemoryFree(calc.scanPlan);
  taosArrayDestroy(create.calcScanPlanList);
}

static int32_t broadcastReuseDecodeOom(const char *, SNode **) { return TSDB_CODE_OUT_OF_MEMORY; }

TEST(MndBroadcastReuseTest, ReaderDecodeFailureStopsBeforeRunnerMutation) {
  SCMCreateStreamReq create = {};
  SStreamObj         stream = {};
  stream.pCreate = &create;
  create.calcScanPlanList = taosArrayInit(1, sizeof(SStreamCalcScan));
  SStreamCalcScan calc = {};
  calc.scanPlan = const_cast<char *>("invalid json");
  SNode *raw = nullptr;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &raw));
  char *prefix = nullptr;
  ASSERT_EQ(0, nodesNodeToString(raw, false, &prefix, nullptr));
  SStreamCalcScan first = {};
  first.scanPlan = prefix;
  ASSERT_NE(nullptr, taosArrayPush(create.calcScanPlanList, &first));
  ASSERT_NE(nullptr, taosArrayPush(create.calcScanPlanList, &calc));
  SStmTaskDeploy deploy = {};
  deploy.msg.runner.pPlan = reinterpret_cast<SSubplan *>(raw);
  auto *runners = taosArrayInit(1, sizeof(SStmTaskDeploy));
  ASSERT_NE(nullptr, taosArrayPush(runners, &deploy));
  EXPECT_NE(0, msmUpdateRunnerPlans(nullptr, runners, &stream));
  EXPECT_EQ(raw,
            reinterpret_cast<SNode *>(reinterpret_cast<SStmTaskDeploy *>(taosArrayGet(runners, 0))->msg.runner.pPlan));
  Stub stub;
  stub.set(nodesStringToNode, broadcastReuseDecodeOom);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, msmUpdateRunnerPlans(nullptr, runners, &stream));
  EXPECT_EQ(raw,
            reinterpret_cast<SNode *>(reinterpret_cast<SStmTaskDeploy *>(taosArrayGet(runners, 0))->msg.runner.pPlan));
  taosMemoryFree(prefix);
  nodesDestroyNode(raw);
  taosArrayDestroy(runners);
  taosArrayDestroy(create.calcScanPlanList);
}

// Shadow ops of an ACTIVE batch-meta txn are runtime-only and rebuilt from SDB (STB / stream
// rows carrying txnId + txnStatus). A new Raft leader first reports "restore finished" from the
// committed prefix it had at startup, applies the remaining entries of earlier terms, and only
// then finishes the restore of its own term -- a second sdbAfterRestored() in the same process.
// These tests replay that sequence without Raft, to pin down that the second pass must see
// the SDB as it is by then instead of keeping the shadow ops built by the first pass.
class MndTxnRestoreShadowRebuildTest : public MndTxnPendingStreamCreateTest {
 protected:
  void SetUp() override {
    savedErrno_ = terrno;
    savedDisableStream_ = tsDisableStream;
    SSdbOpt options = {};
    options.path = TD_TMP_DIR_PATH "stream_restore_shadow";
    options.pMnode = &node_;
    taosRemoveDir(options.path);
    node_.pSdb = sdbInit(&options);
    ASSERT_NE(node_.pSdb, nullptr);
    ASSERT_EQ(mndInitTrans(&node_), TSDB_CODE_SUCCESS);
    ASSERT_EQ(mndInitTxn(&node_), TSDB_CODE_SUCCESS);
    ASSERT_EQ(mndInitStb(&node_), TSDB_CODE_SUCCESS);
    // Register the real stream table (decode/insert/update/delete) without the stream runtime.
    tsDisableStream = true;
    ASSERT_EQ(mndInitStream(&node_), TSDB_CODE_SUCCESS);
  }

  void TearDown() override {
    tsDisableStream = savedDisableStream_;
    if (node_.pSdb != nullptr) sdbCleanup(node_.pSdb);
    node_.pSdb = nullptr;
    mndCleanupTxn(&node_);
    taosRemoveDir(TD_TMP_DIR_PATH "stream_restore_shadow");
    terrno = savedErrno_;
  }

  // Writes (or overwrites) the stream row owned by txnId_, as a marker trans would.
  void WriteStreamMarker(EMetaTxnStatus status) {
    SStreamObj stream = BuildMinimalStreamObj();
    ASSERT_NE(stream.pCreate, nullptr);
    taosMemoryFreeClear(stream.pCreate->name);
    stream.pCreate->name = taosStrdup(name_);
    tstrncpy(stream.name, name_, sizeof(stream.name));
    stream.pCreate->streamId = 42;
    stream.txnId = txnId_;
    stream.txnStatus = status;
    ASSERT_TRUE(WriteRaw(mndStreamActionEncode(&stream), SDB_STATUS_READY));
    tFreeStreamObj(&stream);
  }

  std::vector<int8_t> ShadowOpTypes() {
    std::vector<int8_t> types;
    auto *txn = static_cast<STxnObj *>(sdbAcquire(node_.pSdb, SDB_TXN, &txnId_));
    if (txn == nullptr) return types;
    if (txn->pShadowOps != nullptr) {
      for (int32_t i = 0; i < taosArrayGetSize(txn->pShadowOps); ++i) {
        types.push_back(static_cast<SMndShadowOp *>(taosArrayGet(txn->pShadowOps, i))->opType);
      }
    }
    sdbRelease(node_.pSdb, txn);
    return types;
  }

  bool savedDisableStream_ = false;
};

TEST_F(MndTxnRestoreShadowRebuildTest, RepeatRestoreRebuildsShadowOpsFromCurrentSdb) {
  ASSERT_TRUE(WriteUserTxn());
  // First restore: only the PRE_CREATE marker is applied so far.
  WriteStreamMarker(META_TXN_PRE_CREATE);
  ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ShadowOpTypes(), std::vector<int8_t>({MND_SHADOW_OP_CREATE_STREAM}));

  // The remaining log entries of the previous term are applied: the DROP marker lands.
  WriteStreamMarker(META_TXN_PRE_CREATE_DROP);

  // The node wins the election, then finishes the restore of its own term. The cached ops
  // from the first pass are stale.
  mndTxnHandleBecomeLeader(&node_);
  ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ShadowOpTypes(), std::vector<int8_t>({MND_SHADOW_OP_CREATE_STREAM, MND_SHADOW_OP_DROP_STREAM}));
}

// sdbAfterRestored runs after sync already reports restored, so on a leader DDL / COMMIT
// workers can be reading pShadowOps at that point. It may fill a NULL cache but must never
// free a populated one; dropping the cache belongs to the become-leader callback.
TEST_F(MndTxnRestoreShadowRebuildTest, RestoreDoesNotFreeCachedShadowOps) {
  ASSERT_TRUE(WriteUserTxn());
  WriteStreamMarker(META_TXN_PRE_CREATE);
  ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);

  auto *txn = static_cast<STxnObj *>(sdbAcquire(node_.pSdb, SDB_TXN, &txnId_));
  ASSERT_NE(txn, nullptr);
  // Release on every exit so a failed assertion does not leak the reference.
  std::unique_ptr<void, std::function<void(void *)>> guard(
      txn, [this](void *p) { sdbRelease(node_.pSdb, p); });
  SArray *cached = txn->pShadowOps;
  ASSERT_NE(cached, nullptr);
  ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);
  EXPECT_EQ(txn->pShadowOps, cached);
}

// Becoming leader drops the cache and its conflict-map entries; the next restore rebuilds both.
TEST_F(MndTxnRestoreShadowRebuildTest, BecomeLeaderDropsCacheAndConflictEntries) {
  ASSERT_TRUE(WriteUserTxn());
  WriteStreamMarker(META_TXN_PRE_CREATE);
  ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);
  ASSERT_EQ(ShadowOpTypes(), std::vector<int8_t>({MND_SHADOW_OP_CREATE_STREAM}));

  mndTxnHandleBecomeLeader(&node_);
  EXPECT_TRUE(ShadowOpTypes().empty());
  EXPECT_EQ(mndTxnCheckStreamConflict(&node_, name_, txnId_ + 1), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mndTxnCheckStbConflict(&node_, name_, txnId_ + 1), TSDB_CODE_SUCCESS);

  ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ShadowOpTypes(), std::vector<int8_t>({MND_SHADOW_OP_CREATE_STREAM}));
  EXPECT_NE(mndTxnCheckStreamConflict(&node_, name_, txnId_ + 1), TSDB_CODE_SUCCESS);
  EXPECT_NE(mndTxnCheckStbConflict(&node_, name_, txnId_ + 1), TSDB_CODE_SUCCESS);
}

TEST_F(MndTxnRestoreShadowRebuildTest, RepeatRestoreKeepsOtherTxnsConflictOwnership) {
  ASSERT_TRUE(WriteUserTxn());
  WriteStreamMarker(META_TXN_PRE_CREATE);

  // An entry owned by another txn must survive this txn's discard and rebuild.
  const char    otherName[] = "1.db.owned_by_other";
  const txn_id_t otherTxn = txnId_ + 100;
  ASSERT_NE(node_.txnMgmt.pStbConflictMap, nullptr);
  ASSERT_EQ(taosHashPut(node_.txnMgmt.pStbConflictMap, otherName, strlen(otherName), &otherTxn, sizeof(otherTxn)), 0);

  ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);
  mndTxnHandleBecomeLeader(&node_);
  ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);
  // Rebuilding twice must not duplicate ops.
  EXPECT_EQ(ShadowOpTypes(), std::vector<int8_t>({MND_SHADOW_OP_CREATE_STREAM}));

  auto *pOwner = static_cast<txn_id_t *>(taosHashGet(node_.txnMgmt.pStbConflictMap, otherName, strlen(otherName)));
  ASSERT_NE(pOwner, nullptr);
  EXPECT_EQ(*pOwner, otherTxn);
}

TEST_F(MndTxnRestoreShadowRebuildTest, PendingDropMarkerDefersRebuildUntilItIsDeleted) {
  ASSERT_TRUE(WriteUserTxn());
  WriteStreamMarker(META_TXN_PRE_CREATE);
  // A DROP marker trans is still unfinished (its commit-log has not been applied yet).
  ASSERT_TRUE(WritePending(TDMT_MND_DROP_STREAM));
  ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);
  ExpectRecoveryState(true);
  EXPECT_EQ(mndTxnCheckPendingStreamCreate(&node_, txnId_), TSDB_CODE_MND_TRANS_CONFLICT);

  // The marker trans finishes: its commit-log flips the row, then the trans is deleted.
  WriteStreamMarker(META_TXN_PRE_CREATE_DROP);
  ASSERT_TRUE(WritePending(TDMT_MND_DROP_STREAM, SDB_STATUS_DROPPED));
  mndTxnHandleBecomeLeader(&node_);
  ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mndTxnCheckPendingStreamCreate(&node_, txnId_), TSDB_CODE_SUCCESS);
  EXPECT_EQ(ShadowOpTypes(), std::vector<int8_t>({MND_SHADOW_OP_CREATE_STREAM, MND_SHADOW_OP_DROP_STREAM}));
}

// A leader restores exactly once. If it restores while the DROP marker trans of the txn is still
// unfinished, the ops it builds are based on the PRE_CREATE row. Clearing the cache at restore
// cannot help here: no further restore follows. The only protection is to not build the ops until
// the marker trans is gone. Any later use of the txn (COMMIT, or another DDL as exercised here)
// then has to see both ops.
TEST_F(MndTxnRestoreShadowRebuildTest, SingleRestoreWithPendingDropMarkerNeverKeepsStaleOps) {
  ASSERT_TRUE(WriteUserTxn());
  WriteStreamMarker(META_TXN_PRE_CREATE);
  ASSERT_TRUE(WritePending(TDMT_MND_DROP_STREAM));
  ASSERT_EQ(sdbAfterRestored(node_.pSdb), TSDB_CODE_SUCCESS);  // the one and only restore

  // The pulled-up marker trans now finishes: row flips to PRE_CREATE_DROP, trans is deleted.
  WriteStreamMarker(META_TXN_PRE_CREATE_DROP);
  ASSERT_TRUE(WritePending(TDMT_MND_DROP_STREAM, SDB_STATUS_DROPPED));

  // Next use of the txn: recovery is resumed first and must see the final row.
  ASSERT_EQ(mndTxnAddShadowOp(&node_, txnId_, MND_SHADOW_OP_DROP_STREAM, "1.db.second", 7, "1.db", nullptr, 0),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(ShadowOpTypes(), std::vector<int8_t>({MND_SHADOW_OP_CREATE_STREAM, MND_SHADOW_OP_DROP_STREAM,
                                                  MND_SHADOW_OP_DROP_STREAM}));
}

TEST_F(MndTxnRestoreShadowRebuildTest, PendingDropMarkerDoesNotBlockFurtherDdlOrNameReservation) {
  ASSERT_TRUE(WriteUserTxn());
  ASSERT_TRUE(WritePending(TDMT_MND_DROP_STREAM));
  // COMMIT / ROLLBACK / restore wait for the DROP marker ...
  EXPECT_EQ(mndTxnCheckPendingStreamCreate(&node_, txnId_), TSDB_CODE_MND_TRANS_CONFLICT);
  // ... but a DROP marker never reserves the stream name for other DDL, and the client has
  // already been answered when it is still pending, so a following DDL in the txn must pass.
  EXPECT_EQ(mndTxnCheckStreamConflict(&node_, name_, txnId_ + 1), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mndTxnAddShadowOp(&node_, txnId_, MND_SHADOW_OP_DROP_STREAM, "1.db.second", 7, "1.db", nullptr, 0),
            TSDB_CODE_SUCCESS);
}

// ============================================================================
// Offline mini-mnode guard
//
// `taosd -fMnode` (and the other mndDump.c one-shot tools) drive a mini-mnode that never calls
// streamInit(), so the global stream mgmt state is left unallocated. The become-leader callbacks
// still fire on that path through the sync FSM, and they must bail out on streamMgmtInited()
// instead of dereferencing a NULL gStreamMgmt (mndDump.c records the gdb-verified segfault this
// guard prevents). Without the guard the calls below crash the whole test binary.
//
// NOTE: nothing in this binary calls streamInit(), so gStreamMgmt is guaranteed to be
// zero-initialized here. If that ever changes, these tests must start from a fresh state rather
// than silently lose their meaning -- hence the ASSERT_FALSE below.
// ============================================================================

TEST(MndStreamOfflineMiniMnode, BecomeLeaderToleratesUninitializedMgmt) {
  ASSERT_FALSE(streamMgmtInited()) << "gStreamMgmt was already initialized by another test";

  SMnode node = {};
  msmHandleBecomeLeader(&node);

  SUCCEED();
}

TEST(MndStreamOfflineMiniMnode, BecomeNotLeaderToleratesUninitializedMgmt) {
  ASSERT_FALSE(streamMgmtInited()) << "gStreamMgmt was already initialized by another test";

  SMnode node = {};
  msmHandleBecomeNotLeader(&node);

  SUCCEED();
}
