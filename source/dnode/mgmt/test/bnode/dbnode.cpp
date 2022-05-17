/**
 * @file dbnode.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module bnode tests
 * @version 1.0
 * @date 2022-01-05
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class DndTestBnode : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    test.Init("/tmp/dbnodeTest", 9112);
    taosMsleep(1100);
  }
  static void TearDownTestSuite() { test.Cleanup(); }
  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestBnode::test;

TEST_F(DndTestBnode, 01_Create_Bnode) {
  {
    SDCreateBnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_BNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_INVALID_OPTION);
  }

  {
    SDCreateBnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_BNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SDCreateBnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_BNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_NODE_ALREADY_DEPLOYED);
  }

  // test.Restart();

  {
    SDCreateBnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);
    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_BNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_NODE_ALREADY_DEPLOYED);
  }
}

TEST_F(DndTestBnode, 02_Drop_Bnode) {
  {
    SDDropBnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_BNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_INVALID_OPTION);
  }

  {
    SDDropBnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_BNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SDDropBnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_BNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_NODE_NOT_DEPLOYED);
  }

  // test.Restart();

  {
    SDDropBnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_BNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_NODE_NOT_DEPLOYED);
  }

  {
    SDCreateBnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_BNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }
}
