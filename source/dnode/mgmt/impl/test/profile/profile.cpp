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

#include "deploy.h"

class DndTestProfile : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  static void SetUpTestSuite() {
    pServer = createServer("/tmp/dndTestProfile", "localhost", 9527);
    pClient = createClient("root", "taosdata");
  }

  static void TearDownTestSuite() {
    dropServer(pServer);
    dropClient(pClient);
  }

  static SServer* pServer;
  static SClient* pClient;
};

SServer* DndTestProfile::pServer;
SClient* DndTestProfile::pClient;

TEST_F(DndTestProfile, connectMsg_01) {
  ASSERT_NE(pServer, nullptr);
  ASSERT_NE(pClient, nullptr);

  SConnectMsg* pReq = (SConnectMsg*)rpcMallocCont(sizeof(SConnectMsg));
  pReq->pid = htonl(1234);
  strcpy(pReq->app, "test01");
  strcpy(pReq->db, "");

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SConnectMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_CONNECT;

  sendMsg(pClient, &rpcMsg);

  SConnectRsp* pRsp = (SConnectRsp*)pClient->pRsp->pCont;
  ASSERT_NE(pRsp, nullptr);
  pRsp->acctId = htonl(pRsp->acctId);
  pRsp->clusterId = htonl(pRsp->clusterId);
  pRsp->connId = htonl(pRsp->connId);
  pRsp->epSet.port[0] = htons(pRsp->epSet.port[0]);

  EXPECT_EQ(pRsp->acctId, 1);
  EXPECT_GT(pRsp->clusterId, 0);
  EXPECT_EQ(pRsp->connId, 1);
  EXPECT_EQ(pRsp->superAuth, 1);
  EXPECT_EQ(pRsp->readAuth, 1);
  EXPECT_EQ(pRsp->writeAuth, 1);

  EXPECT_EQ(pRsp->epSet.inUse, 0);
  EXPECT_EQ(pRsp->epSet.numOfEps, 1);
  EXPECT_EQ(pRsp->epSet.port[0], 9527);
  EXPECT_STREQ(pRsp->epSet.fqdn[0], "localhost");
}

TEST_F(DndTestProfile, heartbeatMsg_01) {
  ASSERT_NE(pServer, nullptr);
  ASSERT_NE(pClient, nullptr);

  SHeartBeatMsg* pReq = (SHeartBeatMsg*)rpcMallocCont(sizeof(SHeartBeatMsg));
  pReq->connId = htonl(1);
  pReq->pid = htonl(1234);
  pReq->numOfQueries = htonl(0);
  pReq->numOfStreams = htonl(0);
  strcpy(pReq->app, "test01");

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SHeartBeatMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_HEARTBEAT;

  sendMsg(pClient, &rpcMsg);

  SHeartBeatRsp* pRsp = (SHeartBeatRsp*)pClient->pRsp->pCont;
  ASSERT_NE(pRsp, nullptr);
  pRsp->connId = htonl(pRsp->connId);
  pRsp->queryId = htonl(pRsp->queryId);
  pRsp->streamId = htonl(pRsp->streamId);
  pRsp->totalDnodes = htonl(pRsp->totalDnodes);
  pRsp->onlineDnodes = htonl(pRsp->onlineDnodes);
  pRsp->epSet.port[0] = htons(pRsp->epSet.port[0]);

  EXPECT_EQ(pRsp->connId, 1);
  EXPECT_EQ(pRsp->queryId, 0);
  EXPECT_EQ(pRsp->streamId, 0);
  EXPECT_EQ(pRsp->totalDnodes, 1);
  EXPECT_EQ(pRsp->onlineDnodes, 1);
  EXPECT_EQ(pRsp->killConnection, 0);

  EXPECT_EQ(pRsp->epSet.inUse, 0);
  EXPECT_EQ(pRsp->epSet.numOfEps, 1);
  EXPECT_EQ(pRsp->epSet.port[0], 9527);
  EXPECT_STREQ(pRsp->epSet.fqdn[0], "localhost");
}
