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
  test.SendShowMetaMsg(TSDB_MGMT_TABLE_SNODE, "");
  CHECK_META("show snodes", 3);

  CHECK_SCHEMA(0, TSDB_DATA_TYPE_SMALLINT, 2, "id");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN + VARSTR_HEADER_SIZE, "endpoint");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");

  test.SendShowRetrieveMsg();
  EXPECT_EQ(test.GetShowRows(), 0);
}

TEST_F(MndTestSnode, 02_Create_Snode_Invalid_Id) {
  {
    int32_t contLen = sizeof(SMCreateSnodeReq);

    SMCreateSnodeReq* pReq = (SMCreateSnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    test.SendShowMetaMsg(TSDB_MGMT_TABLE_SNODE, "");
    CHECK_META("show snodes", 3);

    CHECK_SCHEMA(0, TSDB_DATA_TYPE_SMALLINT, 2, "id");
    CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN + VARSTR_HEADER_SIZE, "endpoint");
    CHECK_SCHEMA(2, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");

    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckInt16(1);
    CheckBinary("localhost:9016", TSDB_EP_LEN);
    CheckTimestamp();
  }
}

TEST_F(MndTestSnode, 03_Create_Snode_Invalid_Id) {
  {
    int32_t contLen = sizeof(SMCreateSnodeReq);

    SMCreateSnodeReq* pReq = (SMCreateSnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_DNODE_NOT_EXIST);
  }
}

TEST_F(MndTestSnode, 04_Create_Snode) {
  {
    // create dnode
    int32_t contLen = sizeof(SCreateDnodeMsg);

    SCreateDnodeMsg* pReq = (SCreateDnodeMsg*)rpcMallocCont(contLen);
    strcpy(pReq->fqdn, "localhost");
    pReq->port = htonl(9017);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    taosMsleep(1300);
    test.SendShowMetaMsg(TSDB_MGMT_TABLE_DNODE, "");
    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    // create snode
    int32_t contLen = sizeof(SMCreateSnodeReq);

    SMCreateSnodeReq* pReq = (SMCreateSnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    test.SendShowMetaMsg(TSDB_MGMT_TABLE_SNODE, "");
    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 2);

    CheckInt16(1);
    CheckInt16(2);
    CheckBinary("localhost:9016", TSDB_EP_LEN);
    CheckBinary("localhost:9017", TSDB_EP_LEN);
    CheckTimestamp();
    CheckTimestamp();
  }

  {
    // drop snode
    int32_t contLen = sizeof(SMDropSnodeReq);

    SMDropSnodeReq* pReq = (SMDropSnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    test.SendShowMetaMsg(TSDB_MGMT_TABLE_SNODE, "");
    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckInt16(1);
    CheckBinary("localhost:9016", TSDB_EP_LEN);
    CheckTimestamp();
  }
}