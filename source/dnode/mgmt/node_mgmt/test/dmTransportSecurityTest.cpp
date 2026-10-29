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

// Covers the two dispatch-level guards in dmTransport.c:
//
//   1. dmProcessServerRpcMsg rejects response-typed frames. dmProcessRpcMsg is the rpc callback
//      of both the inbound server listener and of the dnode's own client transports, and SRpcMsg
//      carries no origin. Without the guard any peer able to reach the listener could forge mnode
//      replies - IP whitelist, datetime whitelist, anode algorithm table, stream heartbeat - and
//      have them applied as trusted configuration.
//   2. dmProcessRpcMsg rejects msgTypes that tmsgIsValid() refuses, before TMSG_INDEX() uses them
//      to index tMsgDict / msgHandles / msgFps.
//
// The payload deserializers are wrapped so that every forged frame "decodes" successfully: that
// way a missing guard shows up as the configuration actually being applied, rather than as a
// payload that happened to be malformed.

#include <gtest/gtest.h>
#include <vector>

extern "C" {
#include "dmMgmt.h"
#include "tanalytics.h"
#include "tversion.h"
void __real_rpcFreeCont(void*);
}

namespace {

std::vector<SRpcInit> registrations;
void*                 trackedBody = nullptr;
int32_t               bodyFrees = 0;
int32_t               queued = 0;
int32_t               ipWhiteApplies = 0;
int32_t               timeWhiteApplies = 0;
int32_t               analyticApplies = 0;
int32_t               wrapperMarks = 0;

constexpr int64_t kForgedVer = 0x7fffffff00000001LL;
constexpr int64_t kInitialIpWhiteVer = 11;
constexpr int64_t kInitialTimeWhiteVer = 22;

// Index of each transport inside `registrations`, in dmInitClient / dmInitStatusClient /
// dmInitSyncClient / dmInitServer order.
constexpr size_t kClientTrans = 0;
constexpr size_t kServerTrans = 3;

int32_t captureNodeQueue(void*, SRpcMsg* msg) {
  ++queued;
  rpcFreeCont(msg->pCont);
  taosFreeQitem(msg);
  return 0;
}

class Frame {
 public:
  explicit Frame(tmsg_t type, int32_t bodyLen = 64) {
    msg.msgType = type;
    msg.contLen = bodyLen;
    msg.pCont = rpcMallocCont(bodyLen);
    EXPECT_NE(msg.pCont, nullptr);
    trackedBody = msg.pCont;
    EXPECT_EQ(taosVersionStrToInt(td_version, &msg.info.cliVer), 0);
  }
  ~Frame() {
    if (bodyFrees == 0) rpcFreeCont(msg.pCont);
    trackedBody = nullptr;
  }
  SRpcMsg msg = {};
};

class DmTransportSecurityTest : public testing::Test {
 protected:
  SDnode  node = {};
  int32_t savedRpcThreads = 0;
  int64_t savedQueueMemoryAllowed = 0;

  void SetUp() override {
    registrations.clear();
    trackedBody = nullptr;
    resetCounters();
    savedRpcThreads = tsNumOfRpcThreads;
    savedQueueMemoryAllowed = tsQueueMemoryAllowed;
    tsNumOfRpcThreads = 8;
    // taosAllocateQitem() rejects RPC_QITEM allocations once tsQueueMemoryUsed exceeds this, and
    // it defaults to 0 outside of dnode startup, which would make every request-typed frame fail
    // to queue regardless of the dispatch guards under test.
    tsQueueMemoryAllowed = 1024 * 1024;
    node.status = DND_STAT_RUNNING;
    node.data.ipWhiteVer = kInitialIpWhiteVer;
    node.data.timeWhiteVer = kInitialTimeWhiteVer;

    auto& wrapper = node.wrappers[DNODE];
    wrapper.name = "transport-security-test";
    wrapper.pDnode = &node;
    for (int32_t type : {TDMT_MND_STATUS_RSP, TDMT_MND_STREAM_HEARTBEAT_RSP, TDMT_DND_SERVER_STATUS}) {
      node.trans.msgHandles[TMSG_INDEX(type)].defaultNtype = DNODE;
      wrapper.msgFps[TMSG_INDEX(type)] = captureNodeQueue;
    }
  }

  void TearDown() override {
    tsNumOfRpcThreads = savedRpcThreads;
    tsQueueMemoryAllowed = savedQueueMemoryAllowed;
  }

  static void resetCounters() {
    bodyFrees = queued = ipWhiteApplies = timeWhiteApplies = analyticApplies = wrapperMarks = 0;
  }

  void registerCallbacks() {
    ASSERT_EQ(dmInitClient(&node), 0);
    ASSERT_EQ(dmInitStatusClient(&node), 0);
    ASSERT_EQ(dmInitSyncClient(&node), 0);
    ASSERT_EQ(dmInitServer(&node), 0);
    ASSERT_EQ(registrations.size(), 4u);
  }
};

TEST_F(DmTransportSecurityTest, ServerListenerDropsForgedMnodeConfigResponses) {
  registerCallbacks();

  const std::vector<tmsg_t> forged = {
      TDMT_MND_RETRIEVE_IP_WHITELIST_DUAL_RSP,
      TDMT_MND_RETRIEVE_DATETIME_WHITELIST_RSP,
      TDMT_MND_RETRIEVE_ANAL_ALGO_RSP,
      TDMT_MND_STREAM_HEARTBEAT_RSP,
  };

  for (tmsg_t type : forged) {
    resetCounters();
    {
      Frame frame(type);
      registrations[kServerTrans].cfp(&node, &frame.msg, nullptr);
    }
    EXPECT_EQ(ipWhiteApplies, 0) << "msgType " << type;
    EXPECT_EQ(timeWhiteApplies, 0) << "msgType " << type;
    EXPECT_EQ(analyticApplies, 0) << "msgType " << type;
    EXPECT_EQ(queued, 0) << "msgType " << type;
    // Dropped, not leaked.
    EXPECT_EQ(bodyFrees, 1) << "msgType " << type;
    // The versions that gate dmMayShouldUpdateIpWhiteList / dmMayShouldUpdateTimeWhiteList stay
    // untouched, so a forged push cannot pin them to the mnode's current value and suppress the
    // periodic repair.
    EXPECT_EQ(node.data.ipWhiteVer, kInitialIpWhiteVer) << "msgType " << type;
    EXPECT_EQ(node.data.timeWhiteVer, kInitialTimeWhiteVer) << "msgType " << type;
  }
}

TEST_F(DmTransportSecurityTest, ServerListenerDropsOrdinaryResponsesToo) {
  // TDMT_MND_STATUS_RSP has a registered dnode handler, so an unsolicited copy on the listener
  // used to be queued like a request. Responses belong to the client transport.
  registerCallbacks();
  {
    Frame frame(TDMT_MND_STATUS_RSP);
    registrations[kServerTrans].cfp(&node, &frame.msg, nullptr);
  }
  EXPECT_EQ(queued, 0);
  EXPECT_EQ(wrapperMarks, 0);
  EXPECT_EQ(bodyFrees, 1);
}

TEST_F(DmTransportSecurityTest, ClientTransportStillAppliesMnodeConfigResponses) {
  // The guard is about which direction a frame arrived from, so the legitimate route - a reply to
  // a request this dnode sent - has to keep working.
  registerCallbacks();

  resetCounters();
  {
    Frame frame(TDMT_MND_RETRIEVE_IP_WHITELIST_DUAL_RSP);
    registrations[kClientTrans].cfp(&node, &frame.msg, nullptr);
  }
  EXPECT_EQ(ipWhiteApplies, 1);
  EXPECT_EQ(node.data.ipWhiteVer, kForgedVer);

  resetCounters();
  {
    Frame frame(TDMT_MND_RETRIEVE_DATETIME_WHITELIST_RSP);
    registrations[kClientTrans].cfp(&node, &frame.msg, nullptr);
  }
  EXPECT_EQ(timeWhiteApplies, 1);
  EXPECT_EQ(node.data.timeWhiteVer, kForgedVer);

  resetCounters();
  {
    Frame frame(TDMT_MND_RETRIEVE_ANAL_ALGO_RSP);
    registrations[kClientTrans].cfp(&node, &frame.msg, nullptr);
  }
  EXPECT_EQ(analyticApplies, 1);
}

TEST_F(DmTransportSecurityTest, ClientTransportStillRoutesOrdinaryResponses) {
  registerCallbacks();
  {
    Frame frame(TDMT_MND_STATUS_RSP);
    registrations[kClientTrans].cfp(&node, &frame.msg, nullptr);
  }
  EXPECT_EQ(queued, 1);
  EXPECT_EQ(bodyFrees, 1);
}

TEST_F(DmTransportSecurityTest, ServerListenerStillRoutesRequests) {
  registerCallbacks();
  {
    Frame frame(TDMT_DND_SERVER_STATUS);
    registrations[kServerTrans].cfp(&node, &frame.msg, nullptr);
  }
  // dmProcessServerStartupStatus only answers inline while the dnode is not running; with
  // DND_STAT_RUNNING the request reaches the registered handler.
  EXPECT_EQ(queued, 1);
  EXPECT_EQ(bodyFrees, 1);
}

TEST_F(DmTransportSecurityTest, InvalidMsgTypesAreRejectedBeforeHandlerLookup) {
  registerCallbacks();

  const int32_t numOfSegments = TMSG_SEG_CODE(TDMT_MAX_MSG_MIN);
  ASSERT_GT(numOfSegments, 0);
  ASSERT_LT(numOfSegments, 256);

  // An undefined segment code, a far-out segment code, and a sequence byte past the end of a
  // defined segment: each would make TMSG_INDEX() read outside tMsgDict or msgHandles. All are
  // odd, i.e. request-typed, so they would otherwise pass the direction guard.
  const std::vector<tmsg_t> invalidTypes = {
      static_cast<tmsg_t>((numOfSegments << 8) | 1),
      static_cast<tmsg_t>(0xFF01),
      static_cast<tmsg_t>(TDMT_DND_MSG_MAX | 1),
  };

  for (tmsg_t type : invalidTypes) {
    ASSERT_FALSE(tmsgIsValid(type)) << "msgType " << type << " is defined; pick another probe";

    // Both entry points must refuse it: the listener and the shared dispatcher.
    resetCounters();
    {
      Frame frame(type);
      registrations[kServerTrans].cfp(&node, &frame.msg, nullptr);
    }
    EXPECT_EQ(queued, 0) << "msgType " << type;
    EXPECT_EQ(wrapperMarks, 0) << "msgType " << type;
    EXPECT_EQ(bodyFrees, 1) << "msgType " << type;

    resetCounters();
    {
      Frame frame(type);
      registrations[kClientTrans].cfp(&node, &frame.msg, nullptr);
    }
    EXPECT_EQ(queued, 0) << "msgType " << type;
    EXPECT_EQ(wrapperMarks, 0) << "msgType " << type;
    EXPECT_EQ(bodyFrees, 1) << "msgType " << type;
  }
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
int32_t __wrap_dmMarkWrapper(SMgmtWrapper*) {
  ++wrapperMarks;
  return 0;
}
void __wrap_dmReleaseWrapper(SMgmtWrapper*) {}

// Pretend every body decodes into an empty but valid payload carrying an attacker-chosen version,
// so reaching the apply step is observable no matter what bytes the frame contained.
int32_t __wrap_tDeserializeSUpdateIpWhiteDual(void*, int32_t, SUpdateIpWhite* pReq) {
  pReq->ver = kForgedVer;
  pReq->numOfUser = 0;
  pReq->pUserIpWhite = nullptr;
  return 0;
}
int32_t __wrap_tDeserializeSRetrieveDateTimeWhiteListRsp(void*, int32_t, SRetrieveDateTimeWhiteListRsp* pRsp) {
  pRsp->ver = kForgedVer;
  pRsp->numOfUser = 0;
  pRsp->pUsers = nullptr;
  return 0;
}
int32_t __wrap_tDeserializeRetrieveAnalyticAlgoRsp(void*, int32_t, SRetrieveAnalyticAlgoRsp* pRsp) {
  pRsp->ver = kForgedVer;
  pRsp->hash = nullptr;
  return 0;
}

int32_t __wrap_rpcSetIpWhite(void*, void*) {
  ++ipWhiteApplies;
  return 0;
}
int32_t __wrap_rpcSetTimeIpWhite(void*, void*) {
  ++timeWhiteApplies;
  return 0;
}
void __wrap_taosAnalyUpdate(int64_t, SHashObj*) { ++analyticApplies; }
}
