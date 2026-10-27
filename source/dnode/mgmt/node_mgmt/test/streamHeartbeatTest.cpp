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
#include <vector>

extern "C" {
#include "dmMgmt.h"
#include "stream.h"
#include "tversion.h"
int32_t dmDispatchStreamHbMsg(struct SDispatchWorkerPool*, void*, int32_t*);
void    __real_rpcFreeCont(void*);
}

namespace {
std::vector<SRpcInit> registrations;
void*                 trackedBody = nullptr;
void*                 queuedWrapper = nullptr;
int32_t               bodyFrees = 0;
int32_t               queued = 0;
int32_t               processed = 0;
int32_t               processedGid = -1;
int32_t               errorCalls = 0;
int32_t               lastError = 0;
int32_t               responseCode = 0;

int32_t captureNodeQueue(void*, SRpcMsg* msg) {
  ++queued;
  queuedWrapper = msg->info.wrapper;
  rpcFreeCont(msg->pCont);
  taosFreeQitem(msg);
  return 0;
}

class HeartbeatMessage {
 public:
  explicit HeartbeatMessage(int32_t headerGid = 1, int32_t bodyGid = 1) {
    SMStreamHbRspMsg response = {};
    response.streamGId = bodyGid;
    int32_t length = 0;
    int32_t code = 0;
    tEncodeSize(tEncodeStreamHbRsp, &response, length, code);
    EXPECT_EQ(code, 0);
    msg.msgType = TDMT_MND_STREAM_HEARTBEAT_RSP;
    msg.contLen = sizeof(SStreamMsgGrpHeader) + length;
    msg.pCont = rpcMallocCont(msg.contLen);
    EXPECT_NE(msg.pCont, nullptr);
    trackedBody = msg.pCont;
    if (msg.pCont == nullptr) return;
    SStreamMsgGrpHeader header = {headerGid};
    memcpy(msg.pCont, &header, sizeof(header));
    SEncoder encoder = {};
    tEncoderInit(&encoder, static_cast<uint8_t*>(msg.pCont) + sizeof(header), length);
    EXPECT_EQ(tEncodeStreamHbRsp(&encoder, &response), 0);
    tEncoderClear(&encoder);
    EXPECT_EQ(taosVersionStrToInt(td_version, &msg.info.cliVer), 0);
  }
  ~HeartbeatMessage() {
    if (bodyFrees == 0) rpcFreeCont(msg.pCont);
    trackedBody = nullptr;
  }
  SRpcMsg msg = {};
};

class StreamHeartbeatTest : public testing::Test {
 protected:
  SDnode  node = {};
  int32_t savedRpcThreads = 0;
  int32_t savedMgmtThreads = 0;
  int64_t savedQueueMemoryAllowed = 0;
  void    SetUp() override {
    registrations.clear();
    trackedBody = nullptr;
    queuedWrapper = nullptr;
    bodyFrees = queued = processed = errorCalls = lastError = 0;
    responseCode = 0;
    processedGid = -1;
    savedRpcThreads = tsNumOfRpcThreads;
    savedMgmtThreads = tsNumOfStreamMgmtThreads;
    savedQueueMemoryAllowed = tsQueueMemoryAllowed;
    tsNumOfRpcThreads = 8;
    tsNumOfStreamMgmtThreads = 3;
    tsQueueMemoryAllowed = 1024 * 1024;
    node.status = DND_STAT_RUNNING;
    auto& wrapper = node.wrappers[DNODE];
    wrapper.name = "heartbeat-test";
    wrapper.pDnode = &node;
    for (int32_t type : {TDMT_MND_STREAM_HEARTBEAT_RSP, TDMT_MND_STATUS_RSP, TDMT_STREAM_SYNC_CHECKPOINT_RSP,
                            TDMT_STREAM_TRIGGER_CALC}) {
      // Start from DNODE so a valid routing header must explicitly select SNODE.
      node.trans.msgHandles[TMSG_INDEX(type)].defaultNtype = DNODE;
      wrapper.msgFps[TMSG_INDEX(type)] = captureNodeQueue;
    }
    node.trans.msgHandles[TMSG_INDEX(TDMT_STREAM_TRIGGER_CALC)].needCheckVgId = true;
  }
  void TearDown() override {
    tsNumOfRpcThreads = savedRpcThreads;
    tsNumOfStreamMgmtThreads = savedMgmtThreads;
    tsQueueMemoryAllowed = savedQueueMemoryAllowed;
  }
  void registerCallbacks() {
    ASSERT_EQ(dmInitClient(&node), 0);
    ASSERT_EQ(dmInitStatusClient(&node), 0);
    ASSERT_EQ(dmInitSyncClient(&node), 0);
    ASSERT_EQ(dmInitServer(&node), 0);
    ASSERT_EQ(registrations.size(), 4);
  }
};

TEST_F(StreamHeartbeatTest, ClientAndServerRegisterDifferentCallbacks) {
  registerCallbacks();
  EXPECT_EQ(registrations[0].cfp, registrations[1].cfp);
  EXPECT_EQ(registrations[0].cfp, registrations[2].cfp);
  EXPECT_NE(registrations[0].cfp, registrations[3].cfp);
}
TEST_F(StreamHeartbeatTest, ServerHeartbeatIsFreedBeforeManagementQueue) {
  registerCallbacks();
  HeartbeatMessage response;
  registrations[3].cfp(&node, &response.msg, nullptr);
  EXPECT_EQ(queued, 0);
  EXPECT_EQ(processed, 0);
  EXPECT_EQ(errorCalls, 0);
  EXPECT_EQ(bodyFrees, 1);
}
TEST_F(StreamHeartbeatTest, ServerHeartbeatErrorCannotUpdateErrorState) {
  registerCallbacks();
  HeartbeatMessage response;
  response.msg.code = TSDB_CODE_RPC_BROKEN_LINK;
  registrations[3].cfp(&node, &response.msg, nullptr);
  EXPECT_EQ(queued, 0);
  EXPECT_EQ(errorCalls, 0);
  EXPECT_EQ(bodyFrees, 1);
}
TEST_F(StreamHeartbeatTest, ServerCheckpointResponseIsFreedBeforeManagementQueue) {
  registerCallbacks();
  HeartbeatMessage response;
  response.msg.msgType = TDMT_STREAM_SYNC_CHECKPOINT_RSP;
  registrations[3].cfp(&node, &response.msg, nullptr);
  EXPECT_EQ(queued, 0);
  EXPECT_EQ(bodyFrees, 1);
}
TEST_F(StreamHeartbeatTest, ClientCheckpointResponseStillUsesManagementQueue) {
  registerCallbacks();
  HeartbeatMessage response;
  response.msg.msgType = TDMT_STREAM_SYNC_CHECKPOINT_RSP;
  registrations[0].cfp(&node, &response.msg, nullptr);
  EXPECT_EQ(queued, 1);
  EXPECT_EQ(bodyFrees, 1);
}
TEST_F(StreamHeartbeatTest, RunnerRequestRejectsTruncatedRoutingHeader) {
  registerCallbacks();
  for (int32_t length = -1; length < 8; ++length) {
    HeartbeatMessage request;
    bodyFrees = queued = responseCode = 0;
    request.msg.msgType = TDMT_STREAM_TRIGGER_CALC;
    request.msg.contLen = length;
    registrations[3].cfp(&node, &request.msg, nullptr);
    EXPECT_EQ(queued, 0);
    EXPECT_EQ(bodyFrees, 1);
    EXPECT_EQ(responseCode, TSDB_CODE_INVALID_MSG_LEN);
  }
}
TEST_F(StreamHeartbeatTest, RunnerRequestWithValidRoutingHeaderUsesSnodeQueue) {
  registerCallbacks();
  auto& wrapper = node.wrappers[SNODE];
  wrapper.name = "snode-routing-test";
  wrapper.pDnode = &node;
  wrapper.msgFps[TMSG_INDEX(TDMT_STREAM_TRIGGER_CALC)] = captureNodeQueue;

  HeartbeatMessage request;
  ASSERT_NE(request.msg.pCont, nullptr);
  ASSERT_GE(request.msg.contLen, static_cast<int32_t>(sizeof(SMsgHead)));
  request.msg.msgType = TDMT_STREAM_TRIGGER_CALC;
  request.msg.contLen = sizeof(SMsgHead);
  SMsgHead header = {};
  header.contLen = htonl(request.msg.contLen);
  header.vgId = htonl(SNODE_HANDLE);
  memcpy(request.msg.pCont, &header, sizeof(header));

  registrations[3].cfp(&node, &request.msg, nullptr);
  EXPECT_EQ(queuedWrapper, &wrapper);
  EXPECT_EQ(queued, 1);
  EXPECT_EQ(bodyFrees, 1);
  EXPECT_EQ(responseCode, 0);
}
TEST_F(StreamHeartbeatTest, ClientHeartbeatStillUsesManagementQueue) {
  registerCallbacks();
  HeartbeatMessage response;
  registrations[0].cfp(&node, &response.msg, nullptr);
  EXPECT_EQ(queued, 1);
  EXPECT_EQ(bodyFrees, 1);
}
TEST_F(StreamHeartbeatTest, OtherServerResponseIsDroppedToo) {
  // The server listener used to drop only TDMT_MND_STREAM_HEARTBEAT_RSP, so any other response
  // type was routed like a request and a peer could forge mnode replies. dmProcessServerRpcMsg
  // now rejects every response type; responses belong to the client transport.
  registerCallbacks();
  HeartbeatMessage response;
  response.msg.msgType = TDMT_MND_STATUS_RSP;
  registrations[3].cfp(&node, &response.msg, nullptr);
  EXPECT_EQ(queued, 0);
  EXPECT_EQ(bodyFrees, 1);
}

TEST_F(StreamHeartbeatTest, ClientStatusResponseKeepsSharedRouting) {
  registerCallbacks();
  HeartbeatMessage response;
  response.msg.msgType = TDMT_MND_STATUS_RSP;
  registrations[0].cfp(&node, &response.msg, nullptr);
  EXPECT_EQ(queued, 1);
  EXPECT_EQ(bodyFrees, 1);
}
TEST_F(StreamHeartbeatTest, DispatcherRejectsMissingHeader) {
  SRpcMsg msg = {};
  msg.msgType = TDMT_MND_STREAM_HEARTBEAT_RSP;
  int32_t worker = -1;
  EXPECT_EQ(dmDispatchStreamHbMsg(nullptr, &msg, &worker), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(worker, -1);
}
TEST_F(StreamHeartbeatTest, DispatcherRejectsShortLengthsAndInvalidGroups) {
  HeartbeatMessage response;
  const int32_t    fullLength = response.msg.contLen;
  for (int32_t length : {-1, 0, 1, static_cast<int32_t>(sizeof(SStreamMsgGrpHeader)) - 1}) {
    response.msg.contLen = length;
    int32_t worker = -1;
    EXPECT_EQ(dmDispatchStreamHbMsg(nullptr, &response.msg, &worker), TSDB_CODE_INVALID_MSG);
    EXPECT_EQ(worker, -1);
  }
  response.msg.contLen = fullLength;
  for (int32_t gid : {-1, STREAM_MAX_GROUP_NUM, INT32_MAX}) {
    SStreamMsgGrpHeader header = {gid};
    memcpy(response.msg.pCont, &header, sizeof(header));
    int32_t worker = -1;
    EXPECT_EQ(dmDispatchStreamHbMsg(nullptr, &response.msg, &worker), TSDB_CODE_INVALID_MSG);
    EXPECT_EQ(worker, -1);
  }
  EXPECT_EQ(errorCalls, 0);
}
TEST_F(StreamHeartbeatTest, DispatcherPreservesValidGroupAndBodylessErrorRoutes) {
  HeartbeatMessage response(4, 4);
  int32_t          worker = -1;
  EXPECT_EQ(dmDispatchStreamHbMsg(nullptr, &response.msg, &worker), 0);
  EXPECT_EQ(worker, 1);
  SRpcMsg error = {};
  error.code = TSDB_CODE_RPC_BROKEN_LINK;
  EXPECT_EQ(dmDispatchStreamHbMsg(nullptr, &error, &worker), 0);
  EXPECT_EQ(worker, 0);
}
TEST_F(StreamHeartbeatTest, AssociatedBodylessErrorPreservesErrorHandling) {
  SRpcMsg msg = {};
  msg.code = TSDB_CODE_RPC_BROKEN_LINK;
  EXPECT_EQ(dmProcessStreamHbRsp(nullptr, &msg), 0);
  EXPECT_EQ(errorCalls, 1);
  EXPECT_EQ(lastError, TSDB_CODE_RPC_BROKEN_LINK);
  EXPECT_EQ(processed, 0);
}
TEST_F(StreamHeartbeatTest, HeaderBodyMismatchDoesNotRunManagementActions) {
  HeartbeatMessage response(1, 2);
  EXPECT_EQ(dmProcessStreamHbRsp(nullptr, &response.msg), 0);
  EXPECT_EQ(processed, 0);
  EXPECT_EQ(errorCalls, 1);
  EXPECT_EQ(lastError, TSDB_CODE_INVALID_MSG);
}
TEST_F(StreamHeartbeatTest, ValidBodyRunsManagementCallbackOnce) {
  HeartbeatMessage response(2, 2);
  EXPECT_EQ(dmProcessStreamHbRsp(nullptr, &response.msg), 0);
  EXPECT_EQ(processed, 1);
  EXPECT_EQ(processedGid, 2);
  EXPECT_EQ(errorCalls, 0);
}
TEST_F(StreamHeartbeatTest, TruncatedBodyPreservesDecodeErrorPolicy) {
  HeartbeatMessage response;
  response.msg.contLen = sizeof(SStreamMsgGrpHeader) + 1;
  EXPECT_EQ(dmProcessStreamHbRsp(nullptr, &response.msg), 0);
  EXPECT_EQ(processed, 0);
  EXPECT_EQ(errorCalls, 1);
  EXPECT_EQ(lastError, TSDB_CODE_INVALID_MSG);
}
}  // namespace

extern "C" {
void* __wrap_rpcOpen(const SRpcInit* init) {
  registrations.push_back(*init);
  return &registrations;
}
void __wrap_rpcFreeCont(void* body) {
  if (body != nullptr && body == trackedBody) ++bodyFrees;
  __real_rpcFreeCont(body);
}
int32_t __wrap_rpcSendResponse(const SRpcMsg* msg) {
  responseCode = msg->code;
  rpcFreeCont(msg->pCont);
  return 0;
}
int32_t __wrap_dmMarkWrapper(SMgmtWrapper*) { return 0; }
void    __wrap_dmReleaseWrapper(SMgmtWrapper*) {}
int32_t __wrap_streamHbProcessRspMsg(SMStreamHbRspMsg* response) {
  ++processed;
  processedGid = response->streamGId;
  tDeepFreeSMStreamHbRspMsg(response);
  return 0;
}
int32_t __wrap_streamHbHandleRspErr(int32_t code, int64_t) {
  ++errorCalls;
  lastError = code;
  return 0;
}
}
