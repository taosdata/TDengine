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

class MndTestTrans : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    test.Init("/tmp/mnode_test_trans", 9013);
    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9013";
    server2.Start("/tmp/mnode_test_trans2", fqdn, 9020, firstEp);
  }

  static void TearDownTestSuite() {
    server2.Stop();
    test.Cleanup();
  }

  static void KillThenRestartServer() {
    char    file[PATH_MAX] = "/tmp/mnode_test_trans/mnode/data/sdb.data";
    TdFilePtr pFile = taosOpenFile(file, TD_FILE_READ);
    int32_t size = 3 * 1024 * 1024;
    void*   buffer = taosMemoryMalloc(size);
    int32_t readLen = taosReadFile(pFile, buffer, size);
    if (readLen < 0 || readLen == size) {
      ASSERT(1);
    }
    taosCloseFile(&pFile);

    test.ServerStop();

    pFile = taosOpenFile(file, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_TRUNC);
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

Testbase   MndTestTrans::test;
TestServer MndTestTrans::server2;

TEST_F(MndTestTrans, 00_Create_User_Crash) {
  {
    test.SendShowMetaReq(TSDB_MGMT_TABLE_TRANS, "");
    CHECK_META("show trans", 7);

    CHECK_SCHEMA(0, TSDB_DATA_TYPE_INT, 4, "id");
    CHECK_SCHEMA(1, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");
    CHECK_SCHEMA(2, TSDB_DATA_TYPE_BINARY, TSDB_TRANS_STAGE_LEN + VARSTR_HEADER_SIZE, "stage");
    CHECK_SCHEMA(3, TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN - 1 + VARSTR_HEADER_SIZE, "db");
    CHECK_SCHEMA(4, TSDB_DATA_TYPE_BINARY, TSDB_TRANS_TYPE_LEN + VARSTR_HEADER_SIZE, "type");
    CHECK_SCHEMA(5, TSDB_DATA_TYPE_TIMESTAMP, 8, "last_exec_time");
    CHECK_SCHEMA(6, TSDB_DATA_TYPE_BINARY, TSDB_TRANS_ERROR_LEN - 1 + VARSTR_HEADER_SIZE, "last_error");

    test.SendShowRetrieveReq();
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

TEST_F(MndTestTrans, 01_Create_User_Crash) {
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

  test.SendShowMetaReq(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);
  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 2);

  KillThenRestartServer();

  test.SendShowMetaReq(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);
  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 2);

  CheckBinary("u1", TSDB_USER_LEN);
  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("normal", 10);
  CheckBinary("super", 10);
  CheckTimestamp();
  CheckTimestamp();
  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("root", TSDB_USER_LEN);
}

TEST_F(MndTestTrans, 02_Create_Qnode1_Crash) {
  {
    SMCreateQnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowMetaReq(TSDB_MGMT_TABLE_QNODE, "");
    CHECK_META("show qnodes", 3);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  KillThenRestartServer();
  {
    SMCreateQnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_QNODE_ALREADY_EXIST);

    test.SendShowMetaReq(TSDB_MGMT_TABLE_QNODE, "");
    CHECK_META("show qnodes", 3);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);
  }
}

TEST_F(MndTestTrans, 03_Create_Qnode2_Crash) {
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
    test.SendShowMetaReq(TSDB_MGMT_TABLE_DNODE, "");
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    SMCreateQnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    server2.Stop();
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_RPC_NETWORK_UNAVAIL);
  }

  taosMsleep(1000);

  {
    // show trans
    test.SendShowMetaReq(TSDB_MGMT_TABLE_TRANS, "");
    CHECK_META("show trans", 7);
    test.SendShowRetrieveReq();

    EXPECT_EQ(test.GetShowRows(), 1);
    CheckInt32(4);
    CheckTimestamp();
    CheckBinary("undoAction", TSDB_TRANS_STAGE_LEN);
    CheckBinary("", TSDB_DB_NAME_LEN - 1);
    CheckBinary("create-qnode", TSDB_TRANS_TYPE_LEN);
    CheckTimestamp();
    CheckBinary("Unable to establish connection", TSDB_TRANS_ERROR_LEN - 1);
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
    test.SendShowMetaReq(TSDB_MGMT_TABLE_TRANS, "");
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 0);
  }

  uInfo("======== re-create trans");
  // re-create trans
  {
    SMCreateQnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_QNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_RPC_NETWORK_UNAVAIL);
  }

  uInfo("======== kill and restart server")
  KillThenRestartServer();

  uInfo("======== server2 start")
  server2.DoStart();

  uInfo("======== server2 started")

  {
    int32_t retry = 0;
    int32_t retryMax = 20;

    for (retry = 0; retry < retryMax; retry++) {
      SMCreateQnodeReq createReq = {0};
      createReq.dnodeId = 2;

      int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
      void*   pReq = rpcMallocCont(contLen);
      tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

      SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_QNODE, pReq, contLen);
      ASSERT_NE(pRsp, nullptr);
      if (pRsp->code == 0) break;
      taosMsleep(1000);
    }

    ASSERT_NE(retry, retryMax);

    test.SendShowMetaReq(TSDB_MGMT_TABLE_QNODE, "");
    CHECK_META("show qnodes", 3);
    test.SendShowRetrieveReq();
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
