/**
 * @file dmnode.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module mnode tests
 * @version 1.0
 * @date 2022-01-07
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class DndTestMnode : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init("/tmp/dnode_test_mnode", 9113); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestMnode::test;

#if 0
TEST_F(DndTestMnode, 01_Create_Mnode) {
  {
    int32_t contLen = sizeof(SDCreateMnodeReq);

    SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_ID_INVALID);
  }

  {
    int32_t contLen = sizeof(SDCreateMnodeReq);

    SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t contLen = sizeof(SDCreateMnodeReq);

    SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_ALREADY_DEPLOYED);
  }

  test.Restart();

  {
    int32_t contLen = sizeof(SDCreateMnodeReq);

    SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_ALREADY_DEPLOYED);
  }
}

TEST_F(DndTestMnode, 02_Alter_Mnode) {
  {
    int32_t contLen = sizeof(SDCreateMnodeReq);

    SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_ID_INVALID);
  }

  {
    int32_t contLen = sizeof(SDCreateMnodeReq);

    SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t contLen = sizeof(SDCreateMnodeReq);

    SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_ALREADY_DEPLOYED);
  }

  test.Restart();

  {
    int32_t contLen = sizeof(SDCreateMnodeReq);

    SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_ALREADY_DEPLOYED);
  }
}

TEST_F(DndTestMnode, 03_Drop_Mnode) {
  {
    int32_t contLen = sizeof(SDDropMnodeReq);

    SDDropMnodeReq* pReq = (SDDropMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_ID_INVALID);
  }

  {
    int32_t contLen = sizeof(SDDropMnodeReq);

    SDDropMnodeReq* pReq = (SDDropMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t contLen = sizeof(SDDropMnodeReq);

    SDDropMnodeReq* pReq = (SDDropMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_NOT_DEPLOYED);
  }

  test.Restart();

  {
    int32_t contLen = sizeof(SDDropMnodeReq);

    SDDropMnodeReq* pReq = (SDDropMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_NOT_DEPLOYED);
  }

  {
    int32_t contLen = sizeof(SDCreateMnodeReq);

    SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }
}
#endif