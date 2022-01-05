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

class DndTestDnode : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 public:
  static void SetUpTestSuite() {
    test.Init("/tmp/dnode_test_dnode1", 9041);
    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9041";

    server2.Start("/tmp/dnode_test_dnode2", fqdn, 9042, firstEp);
    server3.Start("/tmp/dnode_test_dnode3", fqdn, 9043, firstEp);
    server4.Start("/tmp/dnode_test_dnode4", fqdn, 9044, firstEp);
    server5.Start("/tmp/dnode_test_dnode5", fqdn, 9045, firstEp);
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

Testbase   DndTestDnode::test;
TestServer DndTestDnode::server2;
TestServer DndTestDnode::server3;
TestServer DndTestDnode::server4;
TestServer DndTestDnode::server5;

TEST_F(DndTestDnode, 01_ShowDnode) {
  test.SendShowMetaReq(TSDB_MGMT_TABLE_DNODE, "");
  CHECK_META("show dnodes", 7);

  CHECK_SCHEMA(0, TSDB_DATA_TYPE_SMALLINT, 2, "id");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN + VARSTR_HEADER_SIZE, "endpoint");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_SMALLINT, 2, "vnodes");
  CHECK_SCHEMA(3, TSDB_DATA_TYPE_SMALLINT, 2, "support_vnodes");
  CHECK_SCHEMA(4, TSDB_DATA_TYPE_BINARY, 10 + VARSTR_HEADER_SIZE, "status");
  CHECK_SCHEMA(5, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");
  CHECK_SCHEMA(6, TSDB_DATA_TYPE_BINARY, 24 + VARSTR_HEADER_SIZE, "offline_reason");

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);

  CheckInt16(1);
  CheckBinary("localhost:9041", TSDB_EP_LEN);
  CheckInt16(0);
  CheckInt16(16);
  CheckBinary("ready", 10);
  CheckTimestamp();
  CheckBinary("", 24);
}

TEST_F(DndTestDnode, 02_ConfigDnode) {
  int32_t contLen = sizeof(SCfgDnodeMsg);

  SCfgDnodeMsg* pReq = (SCfgDnodeMsg*)rpcMallocCont(contLen);
  pReq->dnodeId = htonl(1);
  strcpy(pReq->config, "ddebugflag 131");

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_CONFIG_DNODE, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, 0);
}

TEST_F(DndTestDnode, 03_Create_Drop_Restart_Dnode) {
  {
    int32_t contLen = sizeof(SCreateDnodeMsg);

    SCreateDnodeMsg* pReq = (SCreateDnodeMsg*)rpcMallocCont(contLen);
    strcpy(pReq->fqdn, "localhost");
    pReq->port = htonl(9042);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  taosMsleep(1300);

  test.SendShowMetaReq(TSDB_MGMT_TABLE_DNODE, "");
  CHECK_META("show dnodes", 7);
  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 2);

  CheckInt16(1);
  CheckInt16(2);
  CheckBinary("localhost:9041", TSDB_EP_LEN);
  CheckBinary("localhost:9042", TSDB_EP_LEN);
  CheckInt16(0);
  CheckInt16(0);
  CheckInt16(16);
  CheckInt16(16);
  CheckBinary("ready", 10);
  CheckBinary("ready", 10);
  CheckTimestamp();
  CheckTimestamp();
  CheckBinary("", 24);
  CheckBinary("", 24);

  {
    int32_t contLen = sizeof(SDropDnodeMsg);

    SDropDnodeMsg* pReq = (SDropDnodeMsg*)rpcMallocCont(contLen);
    pReq->dnodeId = htonl(2);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_DNODE, "");
  CHECK_META("show dnodes", 7);
  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);

  CheckInt16(1);
  CheckBinary("localhost:9041", TSDB_EP_LEN);
  CheckInt16(0);
  CheckInt16(16);
  CheckBinary("ready", 10);
  CheckTimestamp();
  CheckBinary("", 24);

  {
    int32_t contLen = sizeof(SCreateDnodeMsg);

    SCreateDnodeMsg* pReq = (SCreateDnodeMsg*)rpcMallocCont(contLen);
    strcpy(pReq->fqdn, "localhost");
    pReq->port = htonl(9043);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t contLen = sizeof(SCreateDnodeMsg);

    SCreateDnodeMsg* pReq = (SCreateDnodeMsg*)rpcMallocCont(contLen);
    strcpy(pReq->fqdn, "localhost");
    pReq->port = htonl(9044);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t contLen = sizeof(SCreateDnodeMsg);

    SCreateDnodeMsg* pReq = (SCreateDnodeMsg*)rpcMallocCont(contLen);
    strcpy(pReq->fqdn, "localhost");
    pReq->port = htonl(9045);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  taosMsleep(1300);
  test.SendShowMetaReq(TSDB_MGMT_TABLE_DNODE, "");
  CHECK_META("show dnodes", 7);
  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 4);

  CheckInt16(1);
  CheckInt16(3);
  CheckInt16(4);
  CheckInt16(5);
  CheckBinary("localhost:9041", TSDB_EP_LEN);
  CheckBinary("localhost:9043", TSDB_EP_LEN);
  CheckBinary("localhost:9044", TSDB_EP_LEN);
  CheckBinary("localhost:9045", TSDB_EP_LEN);
  CheckInt16(0);
  CheckInt16(0);
  CheckInt16(0);
  CheckInt16(0);
  CheckInt16(16);
  CheckInt16(16);
  CheckInt16(16);
  CheckInt16(16);
  CheckBinary("ready", 10);
  CheckBinary("ready", 10);
  CheckBinary("ready", 10);
  CheckBinary("ready", 10);
  CheckTimestamp();
  CheckTimestamp();
  CheckTimestamp();
  CheckTimestamp();
  CheckBinary("", 24);
  CheckBinary("", 24);
  CheckBinary("", 24);
  CheckBinary("", 24);

  // restart
  uInfo("stop all server");
  test.Restart();
  server2.Restart();
  server3.Restart();
  server4.Restart();
  server5.Restart();

  taosMsleep(1300);
  test.SendShowMetaReq(TSDB_MGMT_TABLE_DNODE, "");
  CHECK_META("show dnodes", 7);
  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 4);

  CheckInt16(1);
  CheckInt16(3);
  CheckInt16(4);
  CheckInt16(5);
  CheckBinary("localhost:9041", TSDB_EP_LEN);
  CheckBinary("localhost:9043", TSDB_EP_LEN);
  CheckBinary("localhost:9044", TSDB_EP_LEN);
  CheckBinary("localhost:9045", TSDB_EP_LEN);
  CheckInt16(0);
  CheckInt16(0);
  CheckInt16(0);
  CheckInt16(0);
  CheckInt16(16);
  CheckInt16(16);
  CheckInt16(16);
  CheckInt16(16);
  CheckBinary("ready", 10);
  CheckBinary("ready", 10);
  CheckBinary("ready", 10);
  CheckBinary("ready", 10);
  CheckTimestamp();
  CheckTimestamp();
  CheckTimestamp();
  CheckTimestamp();
  CheckBinary("", 24);
  CheckBinary("", 24);
  CheckBinary("", 24);
  CheckBinary("", 24);
}
