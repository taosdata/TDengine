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
  static void SetUpTestSuite() { test.Init("/tmp/dnode_test_snode", 9112); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestBnode::test;

TEST_F(DndTestBnode, 01_Create_Bnode) {
  {
    int32_t contLen = sizeof(SDCreateBnodeReq);

    SDCreateBnodeReq* pReq = (SDCreateBnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_DND_CREATE_BNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_DND_BNODE_ID_INVALID);
  }

  {
    int32_t contLen = sizeof(SDCreateBnodeReq);

    SDCreateBnodeReq* pReq = (SDCreateBnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pMsg = test.SendMsg(TDMT_DND_CREATE_BNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }

  {
    int32_t contLen = sizeof(SDCreateBnodeReq);

    SDCreateBnodeReq* pReq = (SDCreateBnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pMsg = test.SendMsg(TDMT_DND_CREATE_BNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_DND_BNODE_ALREADY_DEPLOYED);
  }

  test.Restart();

  {
    int32_t contLen = sizeof(SDCreateBnodeReq);

    SDCreateBnodeReq* pReq = (SDCreateBnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pMsg = test.SendMsg(TDMT_DND_CREATE_BNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_DND_BNODE_ALREADY_DEPLOYED);
  }
}

TEST_F(DndTestBnode, 01_Drop_Bnode) {
  {
    int32_t contLen = sizeof(SDDropBnodeReq);

    SDDropBnodeReq* pReq = (SDDropBnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_DND_DROP_BNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_DND_BNODE_ID_INVALID);
  }

  {
    int32_t contLen = sizeof(SDDropBnodeReq);

    SDDropBnodeReq* pReq = (SDDropBnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pMsg = test.SendMsg(TDMT_DND_DROP_BNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }

  {
    int32_t contLen = sizeof(SDDropBnodeReq);

    SDDropBnodeReq* pReq = (SDDropBnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pMsg = test.SendMsg(TDMT_DND_DROP_BNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_DND_BNODE_NOT_DEPLOYED);
  }

  test.Restart();

  {
    int32_t contLen = sizeof(SDDropBnodeReq);

    SDDropBnodeReq* pReq = (SDDropBnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pMsg = test.SendMsg(TDMT_DND_DROP_BNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_DND_BNODE_NOT_DEPLOYED);
  }

  {
    int32_t contLen = sizeof(SDCreateBnodeReq);

    SDCreateBnodeReq* pReq = (SDCreateBnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pMsg = test.SendMsg(TDMT_DND_CREATE_BNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }
}