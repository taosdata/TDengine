/**
 * @file dqnode.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module qnode tests
 * @version 1.0
 * @date 2022-01-05
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class DndTestQnode : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init("/tmp/dnode_test_qnode", 9111); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestQnode::test;

TEST_F(DndTestQnode, 01_Create_Qnode) {
  {
    SDCreateQnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSMCreateDropQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSMCreateDropQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_NODE_INVALID_OPTION);
  }

  {
    SDCreateQnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSMCreateDropQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSMCreateDropQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SDCreateQnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSMCreateDropQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSMCreateDropQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_NODE_ALREADY_DEPLOYED);
  }

  test.Restart();

  {
    SDCreateQnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSMCreateDropQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSMCreateDropQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_NODE_ALREADY_DEPLOYED);
  }
}

TEST_F(DndTestQnode, 02_Drop_Qnode) {
  {
    SDDropQnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSMCreateDropQSBNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSMCreateDropQSBNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_NODE_INVALID_OPTION);
  }

  {
    SDDropQnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSMCreateDropQSBNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSMCreateDropQSBNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SDDropQnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSMCreateDropQSBNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSMCreateDropQSBNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_NODE_NOT_DEPLOYED);
  }

  test.Restart();

  {
    SDDropQnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSMCreateDropQSBNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSMCreateDropQSBNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_NODE_NOT_DEPLOYED);
  }

  {
    SDCreateQnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSMCreateDropQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSMCreateDropQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }
}