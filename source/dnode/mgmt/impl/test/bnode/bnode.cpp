/**
 * @file dnode.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module dnode-msg tests
 * @version 0.1
 * @date 2021-12-15
 *
 * @copyright Copyright (c) 2021
 *
 */

#include "sut.h"

class DndTestBnode : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 public:
  static void SetUpTestSuite() {
    test.Init("/tmp/dnode_test_bnode1", 9068);
    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9068";

    server2.Start("/tmp/dnode_test_bnode2", fqdn, 9069, firstEp);
    taosMsleep(300);
  }

  static void TearDownTestSuite() {
    server2.Stop();
    test.Cleanup();
  }

  static Testbase   test;
  static TestServer server2;
};

Testbase   DndTestBnode::test;
TestServer DndTestBnode::server2;

TEST_F(DndTestBnode, 01_ShowBnode) {
  test.SendShowMetaMsg(TSDB_MGMT_TABLE_BNODE, "");
  CHECK_META("show bnodes", 3);

  CHECK_SCHEMA(0, TSDB_DATA_TYPE_SMALLINT, 2, "id");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN + VARSTR_HEADER_SIZE, "endpoint");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");

  test.SendShowRetrieveMsg();
  EXPECT_EQ(test.GetShowRows(), 0);
}

TEST_F(DndTestBnode, 02_Create_Bnode_Invalid_Id) {
  {
    int32_t contLen = sizeof(SMCreateBnodeReq);

    SMCreateBnodeReq* pReq = (SMCreateBnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_BNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    test.SendShowMetaMsg(TSDB_MGMT_TABLE_BNODE, "");
    CHECK_META("show bnodes", 3);

    CHECK_SCHEMA(0, TSDB_DATA_TYPE_SMALLINT, 2, "id");
    CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN + VARSTR_HEADER_SIZE, "endpoint");
    CHECK_SCHEMA(2, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");

    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckInt16(1);
    CheckBinary("localhost:9068", TSDB_EP_LEN);
    CheckTimestamp();
  }
}

TEST_F(DndTestBnode, 03_Create_Bnode_Invalid_Id) {
  {
    int32_t contLen = sizeof(SMCreateBnodeReq);

    SMCreateBnodeReq* pReq = (SMCreateBnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_BNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_DNODE_NOT_EXIST);
  }
}

TEST_F(DndTestBnode, 04_Create_Bnode) {
  {
    // create dnode
    int32_t contLen = sizeof(SCreateDnodeMsg);

    SCreateDnodeMsg* pReq = (SCreateDnodeMsg*)rpcMallocCont(contLen);
    strcpy(pReq->fqdn, "localhost");
    pReq->port = htonl(9069);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    taosMsleep(1300);
    test.SendShowMetaMsg(TSDB_MGMT_TABLE_DNODE, "");
    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    // create bnode
    int32_t contLen = sizeof(SMCreateBnodeReq);

    SMCreateBnodeReq* pReq = (SMCreateBnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_BNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    test.SendShowMetaMsg(TSDB_MGMT_TABLE_BNODE, "");
    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 2);

    CheckInt16(1);
    CheckInt16(2);
    CheckBinary("localhost:9068", TSDB_EP_LEN);
    CheckBinary("localhost:9069", TSDB_EP_LEN);
    CheckTimestamp();
    CheckTimestamp();
  }

  {
    // drop bnode
    int32_t contLen = sizeof(SMDropBnodeReq);

    SMDropBnodeReq* pReq = (SMDropBnodeReq*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_BNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    test.SendShowMetaMsg(TSDB_MGMT_TABLE_BNODE, "");
    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckInt16(1);
    CheckBinary("localhost:9068", TSDB_EP_LEN);
    CheckTimestamp();
  }
}