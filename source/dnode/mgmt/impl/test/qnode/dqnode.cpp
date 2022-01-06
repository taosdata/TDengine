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
    int32_t contLen = sizeof(SDCreateQnodeReq);

    SDCreateQnodeReq* pReq = (SDCreateQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_QNODE_ID_INVALID);
  }

  {
    int32_t contLen = sizeof(SDCreateQnodeReq);

    SDCreateQnodeReq* pReq = (SDCreateQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t contLen = sizeof(SDCreateQnodeReq);

    SDCreateQnodeReq* pReq = (SDCreateQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_QNODE_ALREADY_DEPLOYED);
  }

  test.Restart();

  {
    int32_t contLen = sizeof(SDCreateQnodeReq);

    SDCreateQnodeReq* pReq = (SDCreateQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_QNODE_ALREADY_DEPLOYED);
  }
}

TEST_F(DndTestQnode, 02_Drop_Qnode) {
  {
    int32_t contLen = sizeof(SDDropQnodeReq);

    SDDropQnodeReq* pReq = (SDDropQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_QNODE_ID_INVALID);
  }

  {
    int32_t contLen = sizeof(SDDropQnodeReq);

    SDDropQnodeReq* pReq = (SDDropQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t contLen = sizeof(SDDropQnodeReq);

    SDDropQnodeReq* pReq = (SDDropQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_QNODE_NOT_DEPLOYED);
  }

  test.Restart();

  {
    int32_t contLen = sizeof(SDDropQnodeReq);

    SDDropQnodeReq* pReq = (SDDropQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_QNODE_NOT_DEPLOYED);
  }

  {
    int32_t contLen = sizeof(SDCreateQnodeReq);

    SDCreateQnodeReq* pReq = (SDCreateQnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }
}