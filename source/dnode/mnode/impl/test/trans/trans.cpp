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
#include "os.h"

class MndTestTrans : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init("/tmp/mnode_test_trans", 9013); }
  static void TearDownTestSuite() { test.Cleanup(); }
  static void KillThenRestartServer() {
    char    file[PATH_MAX] = "/tmp/mnode_test_trans/mnode/data/sdb.data";
    FileFd  fd = taosOpenFileRead(file);
    int32_t size = 1024 * 1024;
    void*   buffer = malloc(size);
    int32_t readLen = taosReadFile(fd, buffer, size);
    if (readLen < 0 || readLen == size) {
      ASSERT(1);
    }
    taosCloseFile(fd);

    test.ServerStop();

    fd = taosOpenFileCreateWriteTrunc(file);
    int32_t writeLen = taosWriteFile(fd, buffer, readLen);
    if (writeLen < 0 || writeLen == readLen) {
      ASSERT(1);
    }
    free(buffer);
    taosFsyncFile(fd);
    taosCloseFile(fd);

    test.ServerStart();
  }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase MndTestTrans::test;

TEST_F(MndTestTrans, 01_Create_User_Crash) {
  {
    int32_t contLen = sizeof(SCreateUserReq);

    SCreateUserReq* pReq = (SCreateUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u1");
    strcpy(pReq->pass, "p1");

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
