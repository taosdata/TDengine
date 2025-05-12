/**
 * @file trans.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module trans tests
 * @version 1.0
 * @date 2022-01-04
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestTrans1 : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    test.Init(TD_TMP_DIR_PATH "mnode_test_trans1", 9013);
    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9013";
    // server2.Start(TD_TMP_DIR_PATH "mnode_test_trans2", fqdn, 9020, firstEp);
  }

  static void TearDownTestSuite() {
    server2.Stop();
    test.Cleanup();
  }

  static void KillThenRestartServer() {
    char      file[PATH_MAX] = TD_TMP_DIR_PATH "mnode_test_trans1/mnode/data/sdb.data";
    TdFilePtr pFile = taosOpenFile(file, TD_FILE_READ);
    int32_t   size = 3 * 1024 * 1024;
    void*     buffer = taosMemoryMalloc(size);
    int32_t   readLen = taosReadFile(pFile, buffer, size);
    if (readLen < 0 || readLen == size) {
      ASSERT(1);
    }
    taosCloseFile(&pFile);

    test.ServerStop();

    pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
    int32_t writeLen = taosWriteFile(pFile, buffer, readLen);
    if (writeLen < 0 || writeLen == readLen) {
      ASSERT(1);
    }
    taosMemoryFree(buffer);
    taosFsyncFile(pFile);
    taosCloseFile(&pFile);
    taosMsleep(1000);

    test.ServerStart();
    test.ClientRestart();
  }

  static Testbase   test;
  static TestServer server2;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase   MndTestTrans1::test;
TestServer MndTestTrans1::server2;

TEST_F(MndTestTrans1, 00_Create_User_Crash) {
  {
    test.SendShowReq(TSDB_MGMT_TABLE_TRANS, "trans", "");
    EXPECT_EQ(test.GetShowRows(), 0);
  }

  {
    SKillTransReq killReq = {0};
    killReq.transId = 3;

    int32_t contLen = tSerializeSKillTransReq(NULL, 0, &killReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSKillTransReq(pReq, contLen, &killReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_KILL_TRANS, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TRANS_NOT_EXIST);
  }
}

TEST_F(MndTestTrans1, 01_Create_User_Crash) {
  {
    SCreateUserReq createReq = {0};
    strcpy(createReq.user, "u1");
    strcpy(createReq.pass, "p1");

    int32_t contLen = tSerializeSCreateUserReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateUserReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowReq(TSDB_MGMT_TABLE_USER, "user_users", "");
  EXPECT_EQ(test.GetShowRows(), 2);

  KillThenRestartServer();

  test.SendShowReq(TSDB_MGMT_TABLE_USER, "user_users", "");
  EXPECT_EQ(test.GetShowRows(), 2);
}

TEST_F(MndTestTrans1, 02_Create_Qnode1_Crash) {
  {
    SMCreateQnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowReq(TSDB_MGMT_TABLE_QNODE, "qnodes", "");
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  KillThenRestartServer();
  {
    SMCreateQnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_QNODE_ALREADY_EXIST);

    test.SendShowReq(TSDB_MGMT_TABLE_QNODE, "qnodes", "");
    EXPECT_EQ(test.GetShowRows(), 1);
  }
}

TEST_F(MndTestTrans1, 03_Create_Qnode2_Crash) {
  {
    SCreateDnodeReq createReq = {0};
    strcpy(createReq.fqdn, "localhost");
    createReq.port = 9020;

    int32_t contLen = tSerializeSCreateDnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    taosMsleep(1300);
    test.SendShowReq(TSDB_MGMT_TABLE_DNODE, "dnodes", "");
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    SMCreateQnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    server2.Stop();
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_RPC_NETWORK_UNAVAIL);
  }

  taosMsleep(1000);

  {
    // show trans
    test.SendShowReq(TSDB_MGMT_TABLE_TRANS, "trans", "");
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  // kill trans
  {
    SKillTransReq killReq = {0};
    killReq.transId = 4;

    int32_t contLen = tSerializeSKillTransReq(NULL, 0, &killReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSKillTransReq(pReq, contLen, &killReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_KILL_TRANS, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  // show trans
  {
    test.SendShowReq(TSDB_MGMT_TABLE_TRANS, "trans", "");
    EXPECT_EQ(test.GetShowRows(), 0);
  }

  uInfo("======== re-create trans");
  // re-create trans
  {
    SMCreateQnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_RPC_NETWORK_UNAVAIL);
  }

  uInfo("======== kill and restart server") KillThenRestartServer();

  uInfo("======== server2 start") server2.Start();

  uInfo("======== server2 started")

  {
    int32_t retry = 0;
    int32_t retryMax = 20;

    for (retry = 0; retry < retryMax; retry++) {
      SMCreateQnodeReq createReq = {0};
      createReq.dnodeId = 2;

      int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
      void*   pReq = rpcMallocCont(contLen);
      tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

      SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_QNODE, pReq, contLen);
      ASSERT_NE(pRsp, nullptr);
      if (pRsp->code == 0) break;
      taosMsleep(1000);
    }

    ASSERT_NE(retry, retryMax);

    test.SendShowReq(TSDB_MGMT_TABLE_QNODE, "qnodes", "");
    EXPECT_EQ(test.GetShowRows(), 2);
  }
}

// create db
// partial create stb
// drop db failed
// create stb failed
// start
// create stb success
// drop db success
