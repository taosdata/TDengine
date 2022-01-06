/**
 * @file dsnode.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module snode tests
 * @version 1.0
 * @date 2022-01-05
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class DndTestSnode : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init("/tmp/dnode_test_snode", 9112); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestSnode::test;

TEST_F(DndTestSnode, 01_Create_Snode) {
  {
    int32_t contLen = sizeof(SDCreateSnodeReq);

    SDCreateSnodeReq* pReq = (SDCreateSnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_SNODE_ID_INVALID);
  }

  {
    int32_t contLen = sizeof(SDCreateSnodeReq);

    SDCreateSnodeReq* pReq = (SDCreateSnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t contLen = sizeof(SDCreateSnodeReq);

    SDCreateSnodeReq* pReq = (SDCreateSnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_SNODE_ALREADY_DEPLOYED);
  }

  test.Restart();

  {
    int32_t contLen = sizeof(SDCreateSnodeReq);

    SDCreateSnodeReq* pReq = (SDCreateSnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_SNODE_ALREADY_DEPLOYED);
  }
}

TEST_F(DndTestSnode, 01_Drop_Snode) {
  {
    int32_t contLen = sizeof(SDDropSnodeReq);

    SDDropSnodeReq* pReq = (SDDropSnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_SNODE_ID_INVALID);
  }

  {
    int32_t contLen = sizeof(SDDropSnodeReq);

    SDDropSnodeReq* pReq = (SDDropSnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t contLen = sizeof(SDDropSnodeReq);

    SDDropSnodeReq* pReq = (SDDropSnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_SNODE_NOT_DEPLOYED);
  }

  test.Restart();

  {
    int32_t contLen = sizeof(SDDropSnodeReq);

    SDDropSnodeReq* pReq = (SDDropSnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_SNODE_NOT_DEPLOYED);
  }

  {
    int32_t contLen = sizeof(SDCreateSnodeReq);

    SDCreateSnodeReq* pReq = (SDCreateSnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }
}