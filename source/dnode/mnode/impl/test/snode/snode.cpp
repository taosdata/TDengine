/**
 * @file snode.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module snode tests
 * @version 1.0
 * @date 2022-01-05
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestSnode : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 public:
  static void SetUpTestSuite() {
    test.Init("/tmp/mnode_test_snode1", 9016);
    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9016";

    server2.Start("/tmp/mnode_test_snode2", fqdn, 9017, firstEp);
    taosMsleep(300);
  }

  static void TearDownTestSuite() {
    server2.Stop();
    test.Cleanup();
  }

  static Testbase   test;
  static TestServer server2;
};

Testbase   MndTestSnode::test;
TestServer MndTestSnode::server2;

TEST_F(MndTestSnode, 01_Show_Snode) {
  test.SendShowMetaReq(TSDB_MGMT_TABLE_SNODE, "");
  CHECK_META("show snodes", 3);

  CHECK_SCHEMA(0, TSDB_DATA_TYPE_SMALLINT, 2, "id");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN + VARSTR_HEADER_SIZE, "endpoint");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 0);
}

TEST_F(MndTestSnode, 02_Create_Snode) {
  {
    SMCreateSnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_DNODE_NOT_EXIST);
  }

  {
    SMCreateSnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowMetaReq(TSDB_MGMT_TABLE_SNODE, "");
    CHECK_META("show snodes", 3);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckInt16(1);
    CheckBinary("localhost:9016", TSDB_EP_LEN);
    CheckTimestamp();
  }

  {
    SMCreateSnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_SNODE_ALREADY_EXIST);
  }
}

TEST_F(MndTestSnode, 03_Drop_Snode) {
  {
    SCreateDnodeReq createReq = {0};
    strcpy(createReq.fqdn, "localhost");
    createReq.port = 9017;

    int32_t contLen = tSerializeSCreateDnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    taosMsleep(1300);
    test.SendShowMetaReq(TSDB_MGMT_TABLE_DNODE, "");
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    SMCreateSnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowMetaReq(TSDB_MGMT_TABLE_SNODE, "");
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 2);

    CheckInt16(1);
    CheckInt16(2);
    CheckBinary("localhost:9016", TSDB_EP_LEN);
    CheckBinary("localhost:9017", TSDB_EP_LEN);
    CheckTimestamp();
    CheckTimestamp();
  }

  {
    SMDropSnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowMetaReq(TSDB_MGMT_TABLE_SNODE, "");
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckInt16(1);
    CheckBinary("localhost:9016", TSDB_EP_LEN);
    CheckTimestamp();
  }

  {
    SMDropSnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_SNODE_NOT_EXIST);
  }
}

TEST_F(MndTestSnode, 03_Create_Snode_Rollback) {
  {
    // send message first, then dnode2 crash, result is returned, and rollback is started
    SMCreateSnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    server2.Stop();
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_RPC_NETWORK_UNAVAIL);
  }

  {
    // continue send message, snode is creating
    SMCreateSnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_SDB_OBJ_CREATING);
  }

  {
    // continue send message, snode is creating
    SMDropSnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_SDB_OBJ_CREATING);
  }

  {
    // server start, wait until the rollback finished
    server2.DoStart();
    taosMsleep(1000);

    int32_t retry = 0;
    int32_t retryMax = 20;

    for (retry = 0; retry < retryMax; retry++) {
      SMCreateSnodeReq createReq = {0};
      createReq.dnodeId = 2;

      int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
      void*   pReq = rpcMallocCont(contLen);
      tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

      SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_SNODE, pReq, contLen);
      ASSERT_NE(pRsp, nullptr);
      if (pRsp->code == 0) break;
      taosMsleep(1000);
    }

    ASSERT_NE(retry, retryMax);
  }
}

TEST_F(MndTestSnode, 04_Drop_Snode_Rollback) {
  {
    // send message first, then dnode2 crash, result is returned, and rollback is started
    SMDropSnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &dropReq);

    server2.Stop();
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_RPC_NETWORK_UNAVAIL);
  }

  {
    // continue send message, snode is dropping
    SMCreateSnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_SDB_OBJ_DROPPING);
  }

  {
    // continue send message, snode is dropping
    SMDropSnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_SDB_OBJ_DROPPING);
  }

  {
    // server start, wait until the rollback finished
    server2.DoStart();
    taosMsleep(1000);

    int32_t retry = 0;
    int32_t retryMax = 20;

    for (retry = 0; retry < retryMax; retry++) {
      SMCreateSnodeReq createReq = {0};
      createReq.dnodeId = 2;

      int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
      void*   pReq = rpcMallocCont(contLen);
      tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

      SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_SNODE, pReq, contLen);
      ASSERT_NE(pRsp, nullptr);
      if (pRsp->code == 0) break;
      taosMsleep(1000);
    }

    ASSERT_NE(retry, retryMax);
  }
}