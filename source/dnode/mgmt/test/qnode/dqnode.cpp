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
  static void SetUpTestSuite() { test.Init(TD_TMP_DIR_PATH "dqnodeTest", 9111); }
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

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_INVALID_OPTION);
    rpcFreeCont(pRsp->pCont);
  }

  {
    SDCreateQnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    SDCreateQnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_QNODE_ALREADY_DEPLOYED);
    rpcFreeCont(pRsp->pCont);
  }

  test.Restart();

  {
    SDCreateQnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_QNODE_ALREADY_DEPLOYED);
    rpcFreeCont(pRsp->pCont);
  }
}

TEST_F(DndTestQnode, 02_Drop_Qnode) {
#if 0
  {
    SDDropQnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_INVALID_OPTION);
    rpcFreeCont(pRsp->pCont);
  }
#endif

  {
    SDDropQnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    SDDropQnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_QNODE_NOT_DEPLOYED);
    rpcFreeCont(pRsp->pCont);
  }

  test.Restart();

  {
    SDDropQnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_QNODE_NOT_DEPLOYED);
    rpcFreeCont(pRsp->pCont);
  }

  {
    SDCreateQnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }
}