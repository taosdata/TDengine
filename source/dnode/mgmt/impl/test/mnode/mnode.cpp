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

#include "base.h"

class DndTestMnode : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 public:
  static void SetUpTestSuite() {
    test.Init("/tmp/dnode_test_mnode1", 9061);
    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9061";

    server2.Start("/tmp/dnode_test_mnode2", fqdn, 9062, firstEp);
    server3.Start("/tmp/dnode_test_mnode3", fqdn, 9063, firstEp);
    server4.Start("/tmp/dnode_test_mnode4", fqdn, 9064, firstEp);
    server5.Start("/tmp/dnode_test_mnode5", fqdn, 9065, firstEp);
    taosMsleep(300);
  }

  static void TearDownTestSuite() {
    server2.Stop();
    server3.Stop();
    server4.Stop();
    server5.Stop();
    test.Cleanup();
  }

  static Testbase   test;
  static TestServer server2;
  static TestServer server3;
  static TestServer server4;
  static TestServer server5;
};

Testbase   DndTestMnode::test;
TestServer DndTestMnode::server2;
TestServer DndTestMnode::server3;
TestServer DndTestMnode::server4;
TestServer DndTestMnode::server5;

TEST_F(DndTestMnode, 01_ShowDnode) {
  test.SendShowMetaMsg(TSDB_MGMT_TABLE_MNODE, "");
  CHECK_META("show mnodes", 5);

  CHECK_SCHEMA(0, TSDB_DATA_TYPE_SMALLINT, 2, "id");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN + VARSTR_HEADER_SIZE, "endpoint");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_BINARY, 12 + VARSTR_HEADER_SIZE, "role");
  CHECK_SCHEMA(3, TSDB_DATA_TYPE_TIMESTAMP, 8, "role_time");
  CHECK_SCHEMA(4, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");

  test.SendShowRetrieveMsg();
  EXPECT_EQ(test.GetShowRows(), 1);

  CheckInt16(1);
  CheckBinary("localhost:9061", TSDB_EP_LEN);
  CheckBinary("master", 12);
  CheckInt64(0);
  CheckTimestamp();
}

TEST_F(DndTestMnode, 02_Create_Mnode_Invalid_Id) {
  {
    int32_t contLen = sizeof(SCreateMnodeMsg);

    SCreateMnodeMsg* pReq = (SCreateMnodeMsg*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_MNODE_ALREADY_EXIST);
  }
}

TEST_F(DndTestMnode, 03_Create_Mnode_Invalid_Id) {
  {
    int32_t contLen = sizeof(SCreateMnodeMsg);

    SCreateMnodeMsg* pReq = (SCreateMnodeMsg*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_DNODE_NOT_EXIST);
  }
}

TEST_F(DndTestMnode, 04_Create_Mnode) {
  {
    // create dnode
    int32_t contLen = sizeof(SCreateDnodeMsg);

    SCreateDnodeMsg* pReq = (SCreateDnodeMsg*)rpcMallocCont(contLen);
    strcpy(pReq->fqdn, "localhost");
    pReq->port = htonl(9062);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    taosMsleep(1300);
    test.SendShowMetaMsg(TSDB_MGMT_TABLE_DNODE, "");
    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    // create mnode
    int32_t contLen = sizeof(SCreateMnodeMsg);

    SCreateMnodeMsg* pReq = (SCreateMnodeMsg*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    test.SendShowMetaMsg(TSDB_MGMT_TABLE_MNODE, "");
    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 2);

    CheckInt16(1);
    CheckInt16(2);
    CheckBinary("localhost:9061", TSDB_EP_LEN);
    CheckBinary("localhost:9062", TSDB_EP_LEN);
    CheckBinary("master", 12);
    CheckBinary("slave", 12);
    CheckInt64(0);
    CheckInt64(0);
    CheckTimestamp();
    CheckTimestamp();
  }

  {
    // drop mnode
    int32_t contLen = sizeof(SDropMnodeMsg);

    SDropMnodeMsg* pReq = (SDropMnodeMsg*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    test.SendShowMetaMsg(TSDB_MGMT_TABLE_MNODE, "");
    test.SendShowRetrieveMsg();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckInt16(1);
    CheckBinary("localhost:9061", TSDB_EP_LEN);
    CheckBinary("master", 12);
    CheckInt64(0);
    CheckTimestamp();
  }
}
// {
//   int32_t contLen = sizeof(SDropDnodeMsg);

//   SDropDnodeMsg* pReq = (SDropDnodeMsg*)rpcMallocCont(contLen);
//   pReq->dnodeId = htonl(2);

//   SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_DNODE, pReq, contLen);
//   ASSERT_NE(pMsg, nullptr);
//   ASSERT_EQ(pMsg->code, 0);
// }

// test.SendShowMetaMsg(TSDB_MGMT_TABLE_DNODE, "");
// CHECK_META("show dnodes", 7);
// test.SendShowRetrieveMsg();
// EXPECT_EQ(test.GetShowRows(), 1);

// CheckInt16(1);
// CheckBinary("localhost:9061", TSDB_EP_LEN);
// CheckInt16(0);
// CheckInt16(1);
// CheckBinary("ready", 10);
// CheckTimestamp();
// CheckBinary("", 24);

// {
//   int32_t contLen = sizeof(SCreateDnodeMsg);

//   SCreateDnodeMsg* pReq = (SCreateDnodeMsg*)rpcMallocCont(contLen);
//   strcpy(pReq->ep, "localhost:9063");

//   SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_DNODE, pReq, contLen);
//   ASSERT_NE(pMsg, nullptr);
//   ASSERT_EQ(pMsg->code, 0);
// }

// {
//   int32_t contLen = sizeof(SCreateDnodeMsg);

//   SCreateDnodeMsg* pReq = (SCreateDnodeMsg*)rpcMallocCont(contLen);
//   strcpy(pReq->ep, "localhost:9064");

//   SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_DNODE, pReq, contLen);
//   ASSERT_NE(pMsg, nullptr);
//   ASSERT_EQ(pMsg->code, 0);
// }

// {
//   int32_t contLen = sizeof(SCreateDnodeMsg);

//   SCreateDnodeMsg* pReq = (SCreateDnodeMsg*)rpcMallocCont(contLen);
//   strcpy(pReq->ep, "localhost:9065");

//   SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_DNODE, pReq, contLen);
//   ASSERT_NE(pMsg, nullptr);
//   ASSERT_EQ(pMsg->code, 0);
// }

// taosMsleep(1300);
// test.SendShowMetaMsg(TSDB_MGMT_TABLE_DNODE, "");
// CHECK_META("show dnodes", 7);
// test.SendShowRetrieveMsg();
// EXPECT_EQ(test.GetShowRows(), 4);

// CheckInt16(1);
// CheckInt16(3);
// CheckInt16(4);
// CheckInt16(5);
// CheckBinary("localhost:9061", TSDB_EP_LEN);
// CheckBinary("localhost:9063", TSDB_EP_LEN);
// CheckBinary("localhost:9064", TSDB_EP_LEN);
// CheckBinary("localhost:9065", TSDB_EP_LEN);
// CheckInt16(0);
// CheckInt16(0);
// CheckInt16(0);
// CheckInt16(0);
// CheckInt16(1);
// CheckInt16(1);
// CheckInt16(1);
// CheckInt16(1);
// CheckBinary("ready", 10);
// CheckBinary("ready", 10);
// CheckBinary("ready", 10);
// CheckBinary("ready", 10);
// CheckTimestamp();
// CheckTimestamp();
// CheckTimestamp();
// CheckTimestamp();
// CheckBinary("", 24);
// CheckBinary("", 24);
// CheckBinary("", 24);
// CheckBinary("", 24);

// // restart
// uInfo("stop all server");
// test.Restart();
// server2.Restart();
// server3.Restart();
// server4.Restart();
// server5.Restart();

// taosMsleep(1300);
// test.SendShowMetaMsg(TSDB_MGMT_TABLE_DNODE, "");
// CHECK_META("show dnodes", 7);
// test.SendShowRetrieveMsg();
// EXPECT_EQ(test.GetShowRows(), 4);

// CheckInt16(1);
// CheckInt16(3);
// CheckInt16(4);
// CheckInt16(5);
// CheckBinary("localhost:9061", TSDB_EP_LEN);
// CheckBinary("localhost:9063", TSDB_EP_LEN);
// CheckBinary("localhost:9064", TSDB_EP_LEN);
// CheckBinary("localhost:9065", TSDB_EP_LEN);
// CheckInt16(0);
// CheckInt16(0);
// CheckInt16(0);
// CheckInt16(0);
// CheckInt16(1);
// CheckInt16(1);
// CheckInt16(1);
// CheckInt16(1);
// CheckBinary("ready", 10);
// CheckBinary("ready", 10);
// CheckBinary("ready", 10);
// CheckBinary("ready", 10);
// CheckTimestamp();
// CheckTimestamp();
// CheckTimestamp();
// CheckTimestamp();
// CheckBinary("", 24);
// CheckBinary("", 24);
// CheckBinary("", 24);
// CheckBinary("", 24);
// }
