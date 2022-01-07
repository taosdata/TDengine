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
  static void SetUpTestSuite() {
    test.Init("/tmp/dnode_test_mnode", 9113);
    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9113";

    server2.Start("/tmp/dnode_test_mnode2", fqdn, 9114, firstEp);
  }
  static void TearDownTestSuite() {
    server2.Stop();
    test.Cleanup();
  }

  static Testbase   test;
  static TestServer server2;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase   DndTestMnode::test;
TestServer DndTestMnode::server2;

TEST_F(DndTestMnode, 01_Create_Dnode) {
  int32_t contLen = sizeof(SCreateDnodeReq);

  SCreateDnodeReq* pReq = (SCreateDnodeReq*)rpcMallocCont(contLen);
  strcpy(pReq->fqdn, "localhost");
  pReq->port = htonl(9114);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, 0);

  taosMsleep(1300);
  test.SendShowMetaReq(TSDB_MGMT_TABLE_DNODE, "");
  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 2);
}

TEST_F(DndTestMnode, 01_Create_Mnode) {
  {
    int32_t contLen = sizeof(SDCreateMnodeReq);

    SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);
    pReq->replica = 1;
    pReq->replicas[0].id = htonl(1);
    pReq->replicas[0].port = htonl(9113);
    strcpy(pReq->replicas[0].fqdn, "localhost");

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_INVALID_OPTION);
  }

  {
    int32_t contLen = sizeof(SDCreateMnodeReq);

    SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);
    pReq->replica = 1;
    pReq->replicas[0].id = htonl(2);
    pReq->replicas[0].port = htonl(9113);
    strcpy(pReq->replicas[0].fqdn, "localhost");

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_INVALID_OPTION);
  }

  {
    int32_t contLen = sizeof(SDCreateMnodeReq);

    SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);
    pReq->replica = 2;
    pReq->replicas[0].id = htonl(1);
    pReq->replicas[0].port = htonl(9113);
    strcpy(pReq->replicas[0].fqdn, "localhost");
    pReq->replicas[1].id = htonl(1);
    pReq->replicas[1].port = htonl(9114);
    strcpy(pReq->replicas[1].fqdn, "localhost");

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_ALREADY_DEPLOYED);
  }

  // {
  //   int32_t contLen = sizeof(SDCreateMnodeReq);

  //   SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
  //   pReq->dnodeId = htonl(1);
  //   pReq->replica = 2;
  //   pReq->replicas[0].id = htonl(1);
  //   pReq->replicas[0].port = htonl(9113);
  //    pReq->replicas[0].id = htonl(1);
  //   pReq->replicas[0].port = htonl(9113);
  //   strcpy(pReq->replicas[0].fqdn, "localhost");

  //   SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
  //   ASSERT_NE(pRsp, nullptr);
  //   ASSERT_EQ(pRsp->code, 0);
  // }

  // {
  //   int32_t contLen = sizeof(SDCreateMnodeReq);

  //   SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
  //   pReq->dnodeId = htonl(1);

  //   SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
  //   ASSERT_NE(pRsp, nullptr);
  //   ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_ALREADY_DEPLOYED);
  // }

  // test.Restart();

  // {
  //   int32_t contLen = sizeof(SDCreateMnodeReq);

  //   SDCreateMnodeReq* pReq = (SDCreateMnodeReq*)rpcMallocCont(contLen);
  //   pReq->dnodeId = htonl(1);

  //   SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
  //   ASSERT_NE(pRsp, nullptr);
  //   ASSERT_EQ(pRsp->code, TSDB_CODE_DND_MNODE_ALREADY_DEPLOYED);
  // }
}


#if 0
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