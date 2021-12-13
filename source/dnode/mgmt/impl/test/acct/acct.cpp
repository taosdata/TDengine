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

class DndTestAcct : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  static void SetUpTestSuite() {
    const char* user = "root";
    const char* pass = "taosdata";
    const char* path = "/tmp/dndTestAcct";
    const char* fqdn = "localhost";
    uint16_t    port = 9520;

    pServer = createServer(path, fqdn, port);
    ASSERT(pServer);
    pClient = createClient(user, pass, fqdn, port);
  }

  static void TearDownTestSuite() {
    stopServer(pServer);
    dropClient(pClient);
  }

  static SServer* pServer;
  static SClient* pClient;
  static int32_t  connId;
};

SServer* DndTestAcct::pServer;
SClient* DndTestAcct::pClient;
int32_t  DndTestAcct::connId;

TEST_F(DndTestAcct, CreateAcct) {
  ASSERT_NE(pClient, nullptr);

  SCreateAcctMsg* pReq = (SCreateAcctMsg*)rpcMallocCont(sizeof(SCreateAcctMsg));

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SCreateAcctMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_CREATE_ACCT;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_MSG_NOT_PROCESSED);
}

TEST_F(DndTestAcct, AlterAcct) {
  ASSERT_NE(pClient, nullptr);

  SAlterAcctMsg* pReq = (SAlterAcctMsg*)rpcMallocCont(sizeof(SAlterAcctMsg));

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SAlterAcctMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_ALTER_ACCT;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_MSG_NOT_PROCESSED);
}

TEST_F(DndTestAcct, DropAcct) {
  ASSERT_NE(pClient, nullptr);

  SDropAcctMsg* pReq = (SDropAcctMsg*)rpcMallocCont(sizeof(SDropAcctMsg));

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SDropAcctMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_DROP_ACCT;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_MSG_NOT_PROCESSED);
}

TEST_F(DndTestAcct, ShowAcct) {
  ASSERT_NE(pClient, nullptr);

  SShowMsg* pReq = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
  pReq->type = TSDB_MGMT_TABLE_ACCT;

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SShowMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_SHOW;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_MSG_TYPE);
}