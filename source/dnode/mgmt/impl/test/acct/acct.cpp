/**
 * @file acct.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module acct-msg tests
 * @version 0.1
 * @date 2021-12-15
 *
 * @copyright Copyright (c) 2021
 *
 */

#include "deploy.h"

class DndTestAcct : public ::testing::Test {
 protected:
   static SServer* CreateServer(const char* path, const char* fqdn, uint16_t port, const char* firstEp) {
    SServer* pServer = createServer(path, fqdn, port, firstEp);
    ASSERT(pServer);
    return pServer;
  }

  static void SetUpTestSuite() {
    initLog("/tmp/tdlog");

    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9012";
    pServer = CreateServer("/tmp/dnode_test_user", fqdn, 9012, firstEp);
    pClient = createClient("root", "taosdata", fqdn, 9012);
    taosMsleep(300);
  }

  static void TearDownTestSuite() {
    stopServer(pServer);
    dropClient(pClient);
    pServer = NULL;
    pClient = NULL;
  }

  static SServer* pServer;
  static SClient* pClient;
  static int32_t  connId;
};

SServer* DndTestAcct::pServer;
SClient* DndTestAcct::pClient;
int32_t  DndTestAcct::connId;

TEST_F(DndTestAcct, 01_CreateAcct) {
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

TEST_F(DndTestAcct, 02_AlterAcct) {
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

TEST_F(DndTestAcct, 03_DropAcct) {
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

TEST_F(DndTestAcct, 04_ShowAcct) {
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