/**
 * @file dnode.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module dnode tests
 * @version 1.0
 * @date 2022-01-06
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestDnode : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 public:
  static void SetUpTestSuite() {
    test.Init(TD_TMP_DIR_PATH "dnode_test_dnode1", 9023);
    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9023";

    // server2.Start(TD_TMP_DIR_PATH "dnode_test_dnode2", fqdn, 9024, firstEp);
    // server3.Start(TD_TMP_DIR_PATH "dnode_test_dnode3", fqdn, 9025, firstEp);
    // server4.Start(TD_TMP_DIR_PATH "dnode_test_dnode4", fqdn, 9026, firstEp);
    // server5.Start(TD_TMP_DIR_PATH "dnode_test_dnode5", fqdn, 9027, firstEp);
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

Testbase   MndTestDnode::test;
TestServer MndTestDnode::server2;
TestServer MndTestDnode::server3;
TestServer MndTestDnode::server4;
TestServer MndTestDnode::server5;

TEST_F(MndTestDnode, 01_ShowDnode) {
  test.SendShowReq(TSDB_MGMT_TABLE_DNODE, "dnodes", "");
  EXPECT_EQ(test.GetShowRows(), 1);
}

TEST_F(MndTestDnode, 02_ConfigDnode) {
  SMCfgDnodeReq cfgReq = {0};
  cfgReq.dnodeId = 1;
  strcpy(cfgReq.config, "ddebugflag");
  strcpy(cfgReq.value, "131");

  int32_t contLen = tSerializeSMCfgDnodeReq(NULL, 0, &cfgReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSMCfgDnodeReq(pReq, contLen, &cfgReq);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_CONFIG_DNODE, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, 0);
}

TEST_F(MndTestDnode, 03_Create_Dnode) {
  {
    SCreateDnodeReq createReq = {0};
    strcpy(createReq.fqdn, "");
    createReq.port = 9024;

    int32_t contLen = tSerializeSCreateDnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_DNODE_EP);
  }

  {
    SCreateDnodeReq createReq = {0};
    strcpy(createReq.fqdn, "localhost");
    createReq.port = -1;

    int32_t contLen = tSerializeSCreateDnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_DNODE_EP);
  }

  {
    SCreateDnodeReq createReq = {0};
    strcpy(createReq.fqdn, "localhost");
    createReq.port = 123456;

    int32_t contLen = tSerializeSCreateDnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_DNODE_EP);
  }

  {
    SCreateDnodeReq createReq = {0};
    strcpy(createReq.fqdn, "localhost");
    createReq.port = 9024;

    int32_t contLen = tSerializeSCreateDnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SCreateDnodeReq createReq = {0};
    strcpy(createReq.fqdn, "localhost");
    createReq.port = 9024;

    int32_t contLen = tSerializeSCreateDnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_DNODE_ALREADY_EXIST);
  }

  taosMsleep(1300);

  test.SendShowReq(TSDB_MGMT_TABLE_DNODE, "dnodes", "");
  EXPECT_EQ(test.GetShowRows(), 2);
}

TEST_F(MndTestDnode, 04_Drop_Dnode) {
  {
    SDropDnodeReq dropReq = {0};
    dropReq.dnodeId = -3;

    int32_t contLen = tSerializeSDropDnodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropDnodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_DNODE_ID);
  }

  {
    SDropDnodeReq dropReq = {0};
    dropReq.dnodeId = 5;

    int32_t contLen = tSerializeSDropDnodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropDnodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_DNODE_NOT_EXIST);
  }

  {
    SDropDnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSDropDnodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropDnodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SDropDnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSDropDnodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropDnodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_DNODE_NOT_EXIST);
  }

  test.SendShowReq(TSDB_MGMT_TABLE_DNODE, "dnodes", "");
  EXPECT_EQ(test.GetShowRows(), 1);

  taosMsleep(2000);
  server2.Stop();
  server2.Start();
}

TEST_F(MndTestDnode, 05_Create_Drop_Restart_Dnode) {
  {
    SCreateDnodeReq createReq = {0};
    strcpy(createReq.fqdn, "localhost");
    createReq.port = 9025;

    int32_t contLen = tSerializeSCreateDnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SCreateDnodeReq createReq = {0};
    strcpy(createReq.fqdn, "localhost");
    createReq.port = 9026;

    int32_t contLen = tSerializeSCreateDnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SCreateDnodeReq createReq = {0};
    strcpy(createReq.fqdn, "localhost");
    createReq.port = 9027;

    int32_t contLen = tSerializeSCreateDnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  taosMsleep(1300);
  test.SendShowReq(TSDB_MGMT_TABLE_DNODE, "dnodes", "");
  EXPECT_EQ(test.GetShowRows(), 4);

  // restart
  uInfo("stop all server");
  test.Restart();
  server2.Restart();
  server3.Restart();
  server4.Restart();
  server5.Restart();

  taosMsleep(1300);
  test.SendShowReq(TSDB_MGMT_TABLE_DNODE, "dnodes", "");
  EXPECT_EQ(test.GetShowRows(), 4);

  // alter replica
#if 0
  {
    SCreateDbReq createReq = {0};
    strcpy(createReq.db, "1.d2");
    createReq.numOfVgroups = 2;
    createReq.buffer = -1;
    createReq.pageSize = -1;
    createReq.pages = -1;
    createReq.daysPerFile = 1000;
    createReq.daysToKeep0 = 3650;
    createReq.daysToKeep1 = 3650;
    createReq.daysToKeep2 = 3650;
    createReq.minRows = 100;
    createReq.maxRows = 4096;
    createReq.fsyncPeriod = 3000;
    createReq.walLevel = 1;
    createReq.precision = 0;
    createReq.compression = 2;
    createReq.replications = 1;
    createReq.strict = 1;
    createReq.cacheLast = 0;
    createReq.ignoreExist = 1;
    createReq.numOfStables = 0;
    createReq.numOfRetensions = 0;

    int32_t contLen = tSerializeSCreateDbReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDbReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowReq(TSDB_MGMT_TABLE_DB, "user_databases", "");
    EXPECT_EQ(test.GetShowRows(), 3);
  }

  {
    SAlterDbReq alterdbReq = {0};
    strcpy(alterdbReq.db, "1.d2");

    alterdbReq.buffer = 12;
    alterdbReq.pageSize = -1;
    alterdbReq.pages = -1;
    alterdbReq.daysPerFile = -1;
    alterdbReq.daysToKeep0 = -1;
    alterdbReq.daysToKeep1 = -1;
    alterdbReq.daysToKeep2 = -1;
    alterdbReq.fsyncPeriod = 4000;
    alterdbReq.walLevel = 2;
    alterdbReq.strict = 1;
    alterdbReq.cacheLast = 1;
    alterdbReq.replications = 3;

    int32_t contLen = tSerializeSAlterDbReq(NULL, 0, &alterdbReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterDbReq(pReq, contLen, &alterdbReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SAlterDbReq alterdbReq = {0};
    strcpy(alterdbReq.db, "1.d2");

    alterdbReq.buffer = 12;
    alterdbReq.pageSize = -1;
    alterdbReq.pages = -1;
    alterdbReq.daysPerFile = -1;
    alterdbReq.daysToKeep0 = -1;
    alterdbReq.daysToKeep1 = -1;
    alterdbReq.daysToKeep2 = -1;
    alterdbReq.fsyncPeriod = 4000;
    alterdbReq.walLevel = 2;
    alterdbReq.strict = 1;
    alterdbReq.cacheLast = 1;
    alterdbReq.replications = 1;

    int32_t contLen = tSerializeSAlterDbReq(NULL, 0, &alterdbReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterDbReq(pReq, contLen, &alterdbReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

#endif
}
