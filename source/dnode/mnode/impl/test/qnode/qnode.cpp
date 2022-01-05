/**
 * @file qnode.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module qnode tests
 * @version 1.0
 * @date 2022-01-05
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestQnode : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 public:
  static void SetUpTestSuite() {
    test.Init("/tmp/mnode_test_qnode1", 9014);
    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9014";

    server2.Start("/tmp/mnode_test_qnode2", fqdn, 9015, firstEp);
    taosMsleep(300);
  }

  static void TearDownTestSuite() {
    server2.Stop();
    test.Cleanup();
  }

  static Testbase   test;
  static TestServer server2;
};

Testbase   MndTestQnode::test;
TestServer MndTestQnode::server2;

TEST_F(MndTestQnode, 01_Show_Qnode) {
  test.SendShowMetaMsg(TSDB_MGMT_TABLE_QNODE, "");
  CHECK_META("show qnodes", 3);

  CHECK_SCHEMA(0, TSDB_DATA_TYPE_SMALLINT, 2, "id");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN + VARSTR_HEADER_SIZE, "endpoint");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");

  test.SendShowRetrieveMsg();
  EXPECT_EQ(test.GetShowRows(), 0);
}

TEST_F(MndTestQnode, 02_Create_Qnode) {
  {
    int32_t contLen = sizeof(SMCreateQnodeReq);

    SMCreateQnodeReq* pReq = (SMCreateQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_DNODE_NOT_EXIST);
  }

  {
    int32_t contLen = sizeof(SMCreateQnodeReq);

    SMCreateQnodeReq* pReq = (SMCreateQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    test.SendShowMetaMsg(TSDB_MGMT_TABLE_QNODE, "");
    CHECK_META("show qnodes", 3);
    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckInt16(1);
    CheckBinary("localhost:9014", TSDB_EP_LEN);
    CheckTimestamp();
  }

  {
    int32_t contLen = sizeof(SMCreateQnodeReq);

    SMCreateQnodeReq* pReq = (SMCreateQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_QNODE_ALREADY_EXIST);
  }
}

TEST_F(MndTestQnode, 03_Drop_Qnode) {
  {
    int32_t contLen = sizeof(SCreateDnodeMsg);

    SCreateDnodeMsg* pReq = (SCreateDnodeMsg*)rpcMallocCont(contLen);
    strcpy(pReq->fqdn, "localhost");
    pReq->port = htonl(9015);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    taosMsleep(1300);
    test.SendShowMetaMsg(TSDB_MGMT_TABLE_DNODE, "");
    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    int32_t contLen = sizeof(SMCreateQnodeReq);

    SMCreateQnodeReq* pReq = (SMCreateQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    test.SendShowMetaMsg(TSDB_MGMT_TABLE_QNODE, "");
    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 2);

    CheckInt16(1);
    CheckInt16(2);
    CheckBinary("localhost:9014", TSDB_EP_LEN);
    CheckBinary("localhost:9015", TSDB_EP_LEN);
    CheckTimestamp();
    CheckTimestamp();
  }

  {
    int32_t contLen = sizeof(SMDropQnodeReq);

    SMDropQnodeReq* pReq = (SMDropQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    test.SendShowMetaMsg(TSDB_MGMT_TABLE_QNODE, "");
    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckInt16(1);
    CheckBinary("localhost:9014", TSDB_EP_LEN);
    CheckTimestamp();
  }

  {
    int32_t contLen = sizeof(SMDropQnodeReq);

    SMDropQnodeReq* pReq = (SMDropQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_QNODE_NOT_EXIST);
  }
}

TEST_F(MndTestQnode, 03_Create_Qnode_Rollback) {
  {
    // send message first, then dnode2 crash, result is returned, and rollback is started
    int32_t contLen = sizeof(SMCreateQnodeReq);

    SMCreateQnodeReq* pReq = (SMCreateQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    server2.Stop();
    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_RPC_NETWORK_UNAVAIL);
  }

  {
    // continue send message, qnode is creating
    int32_t contLen = sizeof(SMCreateQnodeReq);

    SMCreateQnodeReq* pReq = (SMCreateQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    server2.Stop();
    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_SDB_OBJ_CREATING);
  }

  {
    // continue send message, qnode is creating
    int32_t contLen = sizeof(SMDropQnodeReq);

    SMDropQnodeReq* pReq = (SMDropQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    server2.Stop();
    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_SDB_OBJ_CREATING);
  }

  {
    // server start, wait until the rollback finished
    server2.DoStart();
    taosMsleep(1000);

    int32_t retry = 0;
    int32_t retryMax = 10;

    for (retry = 0; retry < retryMax; retry++) {
      int32_t contLen = sizeof(SMCreateQnodeReq);

      SMCreateQnodeReq* pReq = (SMCreateQnodeReq*)rpcMallocCont(contLen);
      pReq->dnodeId = htonl(2);

      SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_QNODE, pReq, contLen);
      ASSERT_NE(pMsg, nullptr);
      if (pMsg->code == 0) break;
      taosMsleep(1000);
    }

    ASSERT_NE(retry, retryMax);
  }
}

TEST_F(MndTestQnode, 04_Drop_Qnode_Rollback) {
  {
    // send message first, then dnode2 crash, result is returned, and rollback is started
    int32_t contLen = sizeof(SMDropQnodeReq);

    SMDropQnodeReq* pReq = (SMDropQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    server2.Stop();
    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_RPC_NETWORK_UNAVAIL);
  }

  {
    // continue send message, qnode is dropping
    int32_t contLen = sizeof(SMCreateQnodeReq);

    SMCreateQnodeReq* pReq = (SMCreateQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    server2.Stop();
    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_SDB_OBJ_DROPPING);
  }

  {
    // continue send message, qnode is dropping
    int32_t contLen = sizeof(SMDropQnodeReq);

    SMDropQnodeReq* pReq = (SMDropQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    server2.Stop();
    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_SDB_OBJ_DROPPING);
  }

  {
    // server start, wait until the rollback finished
    server2.DoStart();
    taosMsleep(1000);

    int32_t retry = 0;
    int32_t retryMax = 10;

    for (retry = 0; retry < retryMax; retry++) {
      int32_t contLen = sizeof(SMCreateQnodeReq);

      SMCreateQnodeReq* pReq = (SMCreateQnodeReq*)rpcMallocCont(contLen);
      pReq->dnodeId = htonl(2);

      SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_QNODE, pReq, contLen);
      ASSERT_NE(pMsg, nullptr);
      if (pMsg->code == 0) break;
      taosMsleep(1000);
    }

    ASSERT_NE(retry, retryMax);
  }
}