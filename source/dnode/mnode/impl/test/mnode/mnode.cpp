/**
 * @file mnode.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module mnode tests
 * @version 1.0
 * @date 2022-01-07
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestMnode : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 public:
  static void SetUpTestSuite() {
    test.Init(TD_TMP_DIR_PATH "mnode_test_mnode1", 9028);
    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9028";

    // server2.Start(TD_TMP_DIR_PATH "mnode_test_mnode2", fqdn, 9029, firstEp);
    taosMsleep(300);
  }

  static void TearDownTestSuite() {
    server2.Stop();
    test.Cleanup();
  }

  static Testbase   test;
  static TestServer server2;
};

Testbase   MndTestMnode::test;
TestServer MndTestMnode::server2;

TEST_F(MndTestMnode, 01_ShowDnode) {
  test.SendShowReq(TSDB_MGMT_TABLE_MNODE, "mnodes", "");
  EXPECT_EQ(test.GetShowRows(), 1);
}

TEST_F(MndTestMnode, 02_Create_Mnode_Invalid_Id) {
  {
    SMCreateMnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_MNODE_ALREADY_EXIST);
  }
}

TEST_F(MndTestMnode, 03_Create_Mnode_Invalid_Id) {
  {
    SMCreateMnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_DNODE_NOT_EXIST);
  }
}

TEST_F(MndTestMnode, 04_Create_Mnode) {
  {
    // create dnode
    SCreateDnodeReq createReq = {0};
    strcpy(createReq.fqdn, "localhost");
    createReq.port = 9029;

    int32_t contLen = tSerializeSCreateDnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    taosMsleep(1300);
    test.SendShowReq(TSDB_MGMT_TABLE_DNODE, "dnodes", "");
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    // create mnode
    SMCreateMnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowReq(TSDB_MGMT_TABLE_MNODE, "mnodes", "");
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    // drop mnode
    SMDropMnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowReq(TSDB_MGMT_TABLE_MNODE, "mnodes", "");
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  {
    // drop mnode
    SMDropMnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_MNODE_NOT_EXIST);
  }
}

TEST_F(MndTestMnode, 03_Create_Mnode_Rollback) {
  {
    // send message first, then dnode2 crash, result is returned, and rollback is started
    SMCreateMnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    server2.Stop();
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_RPC_NETWORK_UNAVAIL);
  }

  {
    // continue send message, mnode is creating
    SMCreateMnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_SDB_OBJ_CREATING);
  }

  {
    // continue send message, mnode is creating
    SMDropMnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_SDB_OBJ_CREATING);
  }

  {
    // server start, wait until the rollback finished
    // server2.Start();
    taosMsleep(1000);

    int32_t retry = 0;
    int32_t retryMax = 20;

    for (retry = 0; retry < retryMax; retry++) {
      SMCreateMnodeReq createReq = {0};
      createReq.dnodeId = 2;

      int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
      void*   pReq = rpcMallocCont(contLen);
      tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

      SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_MNODE, pReq, contLen);
      ASSERT_NE(pRsp, nullptr);
      if (pRsp->code == TSDB_CODE_MND_MNODE_ALREADY_EXIST) break;
      taosMsleep(1000);
    }

    ASSERT_NE(retry, retryMax);
  }
}

TEST_F(MndTestMnode, 04_Drop_Mnode_Rollback) {
  {
    // send message first, then dnode2 crash, result is returned, and rollback is started
    SMDropMnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    server2.Stop();
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_RPC_NETWORK_UNAVAIL);
  }

  {
    // continue send message, mnode is dropping
    SMCreateMnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_SDB_OBJ_DROPPING);
  }

  {
    // continue send message, mnode is dropping
    SMDropMnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_SDB_OBJ_DROPPING);
  }

  {
    // server start, wait until the rollback finished
    // server2.Start();
    taosMsleep(1000);

    int32_t retry = 0;
    int32_t retryMax = 20;

    for (retry = 0; retry < retryMax; retry++) {
      SMCreateMnodeReq createReq = {0};
      createReq.dnodeId = 2;

      int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
      void*   pReq = rpcMallocCont(contLen);
      tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

      SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_MNODE, pReq, contLen);
      ASSERT_NE(pRsp, nullptr);
      if (pRsp->code == 0) break;
      taosMsleep(1000);
    }

    ASSERT_NE(retry, retryMax);
  }
}