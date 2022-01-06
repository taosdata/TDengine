/**
 * @file mnode.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module mnode tests
 * @version 1.0
 * @date 2022-01-07
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestMnode : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 public:
  static void SetUpTestSuite() {
    test.Init("/tmp/mnode_test_mnode1", 9031);
    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9031";

    server2.Start("/tmp/mnode_test_mnode2", fqdn, 9032, firstEp);
    server3.Start("/tmp/mnode_test_mnode3", fqdn, 9033, firstEp);
    server4.Start("/tmp/mnode_test_mnode4", fqdn, 9034, firstEp);
    server5.Start("/tmp/mnode_test_mnode5", fqdn, 9035, firstEp);
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

Testbase   MndTestMnode::test;
TestServer MndTestMnode::server2;
TestServer MndTestMnode::server3;
TestServer MndTestMnode::server4;
TestServer MndTestMnode::server5;

TEST_F(MndTestMnode, 01_ShowDnode) {
  test.SendShowMetaReq(TSDB_MGMT_TABLE_MNODE, "");
  CHECK_META("show mnodes", 5);

  CHECK_SCHEMA(0, TSDB_DATA_TYPE_SMALLINT, 2, "id");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN + VARSTR_HEADER_SIZE, "endpoint");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_BINARY, 12 + VARSTR_HEADER_SIZE, "role");
  CHECK_SCHEMA(3, TSDB_DATA_TYPE_TIMESTAMP, 8, "role_time");
  CHECK_SCHEMA(4, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);

  CheckInt16(1);
  CheckBinary("localhost:9031", TSDB_EP_LEN);
  CheckBinary("master", 12);
  CheckInt64(0);
  CheckTimestamp();
}

TEST_F(MndTestMnode, 02_Create_Mnode_Invalid_Id) {
  {
    int32_t contLen = sizeof(SMCreateMnodeMsg);

    SMCreateMnodeMsg* pReq = (SMCreateMnodeMsg*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(1);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_MNODE_ALREADY_EXIST);
  }
}

TEST_F(MndTestMnode, 03_Create_Mnode_Invalid_Id) {
  {
    int32_t contLen = sizeof(SMCreateMnodeMsg);

    SMCreateMnodeMsg* pReq = (SMCreateMnodeMsg*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_DNODE_NOT_EXIST);
  }
}

TEST_F(MndTestMnode, 04_Create_Mnode) {
  {
    // create dnode
    int32_t contLen = sizeof(SCreateDnodeReq);

    SCreateDnodeReq* pReq = (SCreateDnodeReq*)rpcMallocCont(contLen);
    strcpy(pReq->fqdn, "localhost");
    pReq->port = htonl(9032);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    taosMsleep(1300);
    test.SendShowMetaReq(TSDB_MGMT_TABLE_DNODE, "");
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    // create mnode
    int32_t contLen = sizeof(SMCreateMnodeMsg);

    SMCreateMnodeMsg* pReq = (SMCreateMnodeMsg*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowMetaReq(TSDB_MGMT_TABLE_MNODE, "");
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 2);

    CheckInt16(1);
    CheckInt16(2);
    CheckBinary("localhost:9031", TSDB_EP_LEN);
    CheckBinary("localhost:9032", TSDB_EP_LEN);
    CheckBinary("master", 12);
    CheckBinary("slave", 12);
    CheckInt64(0);
    CheckInt64(0);
    CheckTimestamp();
    CheckTimestamp();
  }

  {
    // drop mnode
    int32_t contLen = sizeof(SMDropMnodeMsg);

    SMDropMnodeMsg* pReq = (SMDropMnodeMsg*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowMetaReq(TSDB_MGMT_TABLE_MNODE, "");
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckInt16(1);
    CheckBinary("localhost:9031", TSDB_EP_LEN);
    CheckBinary("master", 12);
    CheckInt64(0);
    CheckTimestamp();
  }
}
// {
//   int32_t contLen = sizeof(SDropDnodeReq);

//   SDropDnodeReq* pReq = (SDropDnodeReq*)rpcMallocCont(contLen);
//   pReq->dnodeId = htonl(2);

//   SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DNODE, pReq, contLen);
//   ASSERT_NE(pRsp, nullptr);
//   ASSERT_EQ(pRsp->code, 0);
// }

// test.SendShowMetaReq(TSDB_MGMT_TABLE_DNODE, "");
// CHECK_META("show dnodes", 7);
// test.SendShowRetrieveReq();
// EXPECT_EQ(test.GetShowRows(), 1);

// CheckInt16(1);
// CheckBinary("localhost:9031", TSDB_EP_LEN);
// CheckInt16(0);
// CheckInt16(1);
// CheckBinary("ready", 10);
// CheckTimestamp();
// CheckBinary("", 24);

// {
//   int32_t contLen = sizeof(SCreateDnodeReq);

//   SCreateDnodeReq* pReq = (SCreateDnodeReq*)rpcMallocCont(contLen);
//   strcpy(pReq->ep, "localhost:9033");

//   SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
//   ASSERT_NE(pRsp, nullptr);
//   ASSERT_EQ(pRsp->code, 0);
// }

// {
//   int32_t contLen = sizeof(SCreateDnodeReq);

//   SCreateDnodeReq* pReq = (SCreateDnodeReq*)rpcMallocCont(contLen);
//   strcpy(pReq->ep, "localhost:9034");

//   SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
//   ASSERT_NE(pRsp, nullptr);
//   ASSERT_EQ(pRsp->code, 0);
// }

// {
//   int32_t contLen = sizeof(SCreateDnodeReq);

//   SCreateDnodeReq* pReq = (SCreateDnodeReq*)rpcMallocCont(contLen);
//   strcpy(pReq->ep, "localhost:9035");

//   SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
//   ASSERT_NE(pRsp, nullptr);
//   ASSERT_EQ(pRsp->code, 0);
// }

// taosMsleep(1300);
// test.SendShowMetaReq(TSDB_MGMT_TABLE_DNODE, "");
// CHECK_META("show dnodes", 7);
// test.SendShowRetrieveReq();
// EXPECT_EQ(test.GetShowRows(), 4);

// CheckInt16(1);
// CheckInt16(3);
// CheckInt16(4);
// CheckInt16(5);
// CheckBinary("localhost:9031", TSDB_EP_LEN);
// CheckBinary("localhost:9033", TSDB_EP_LEN);
// CheckBinary("localhost:9034", TSDB_EP_LEN);
// CheckBinary("localhost:9035", TSDB_EP_LEN);
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
// test.SendShowMetaReq(TSDB_MGMT_TABLE_DNODE, "");
// CHECK_META("show dnodes", 7);
// test.SendShowRetrieveReq();
// EXPECT_EQ(test.GetShowRows(), 4);

// CheckInt16(1);
// CheckInt16(3);
// CheckInt16(4);
// CheckInt16(5);
// CheckBinary("localhost:9031", TSDB_EP_LEN);
// CheckBinary("localhost:9033", TSDB_EP_LEN);
// CheckBinary("localhost:9034", TSDB_EP_LEN);
// CheckBinary("localhost:9035", TSDB_EP_LEN);
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
