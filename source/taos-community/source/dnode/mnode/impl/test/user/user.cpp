/**
 * @file user.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module user tests
 * @version 1.0
 * @date 2022-01-04
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestUser : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init(TD_TMP_DIR_PATH "mnode_test_user", 9011); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase MndTestUser::test;

TEST_F(MndTestUser, 01_Show_User) {
  test.SendShowReq(TSDB_MGMT_TABLE_USER, "ins_users", "");
  EXPECT_EQ(test.GetShowRows(), 1);
}

TEST_F(MndTestUser, 02_Create_User) {
  {
    SCreateUserReq createReq = {0};
    createReq.enable = 1;
    createReq.sysInfo = 1;
    strcpy(createReq.user, "");
    strcpy(createReq.pass, "p1");

    int32_t contLen = tSerializeSCreateUserReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateUserReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_USER_FORMAT);
  }

  {
    SCreateUserReq createReq = {0};
    createReq.enable = 1;
    createReq.sysInfo = 1;
    strcpy(createReq.user, "u1");
    strcpy(createReq.pass, "");

    int32_t contLen = tSerializeSCreateUserReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateUserReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_PASS_FORMAT);
  }

  {
    SCreateUserReq createReq = {0};
    createReq.enable = 1;
    createReq.sysInfo = 1;
    strcpy(createReq.user, "root");
    strcpy(createReq.pass, "1");

    int32_t contLen = tSerializeSCreateUserReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateUserReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_USER_ALREADY_EXIST);
  }

  {
    SCreateUserReq createReq = {0};
    createReq.enable = 1;
    createReq.sysInfo = 1;
    strcpy(createReq.user, "u1");
    strcpy(createReq.pass, "p1");

    int32_t contLen = tSerializeSCreateUserReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateUserReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowReq(TSDB_MGMT_TABLE_USER, "ins_users", "");
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    SDropUserReq dropReq = {0};
    strcpy(dropReq.user, "u1");

    int32_t contLen = tSerializeSDropUserReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropUserReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowReq(TSDB_MGMT_TABLE_USER, "ins_users", "");
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  {
    SCreateUserReq createReq = {0};
    createReq.enable = 1;
    createReq.sysInfo = 1;
    strcpy(createReq.user, "u2");
    strcpy(createReq.pass, "p1");
    createReq.superUser = 0;

    int32_t contLen = tSerializeSCreateUserReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateUserReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowReq(TSDB_MGMT_TABLE_USER, "ins_users", "");
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    SDropUserReq dropReq = {0};
    strcpy(dropReq.user, "u2");

    int32_t contLen = tSerializeSDropUserReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropUserReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowReq(TSDB_MGMT_TABLE_USER, "ins_users", "");
    EXPECT_EQ(test.GetShowRows(), 1);
  }
}

TEST_F(MndTestUser, 03_Alter_User) {
  {
    SCreateUserReq createReq = {0};
    createReq.enable = 1;
    createReq.sysInfo = 1;
    strcpy(createReq.user, "u3");
    strcpy(createReq.pass, "p1");
    createReq.superUser = 0;

    int32_t contLen = tSerializeSCreateUserReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateUserReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowReq(TSDB_MGMT_TABLE_USER, "ins_users", "");
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_PASSWD;
    strcpy(alterReq.user, "");
    strcpy(alterReq.pass, "p1");

    int32_t contLen = tSerializeSAlterUserReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterUserReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_USER_FORMAT);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_PASSWD;
    strcpy(alterReq.user, "u3");
    strcpy(alterReq.pass, "");

    int32_t contLen = tSerializeSAlterUserReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterUserReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_PASS_FORMAT);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_PASSWD;
    strcpy(alterReq.user, "u4");
    strcpy(alterReq.pass, "1");

    int32_t contLen = tSerializeSAlterUserReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterUserReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_USER_NOT_EXIST);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_PASSWD;
    strcpy(alterReq.user, "u3");
    strcpy(alterReq.pass, "1");

    int32_t contLen = tSerializeSAlterUserReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterUserReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_SUPERUSER;
    strcpy(alterReq.user, "u3");
    strcpy(alterReq.pass, "1");
    alterReq.superUser = 0;

    int32_t contLen = tSerializeSAlterUserReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterUserReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_DEL_PRIVILEGES;
    alterReq.privileges = PRIVILEGE_TYPE_ALL；
    strcpy(alterReq.user, "u3");
    strcpy(alterReq.pass, "1");
    strcpy(alterReq.dbname, "1.*");

    int32_t contLen = tSerializeSAlterUserReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterUserReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_DEL_PRIVILEGES;
    alterReq.privileges = PRIVILEGE_TYPE_ALL；
    strcpy(alterReq.user, "u3");
    strcpy(alterReq.pass, "1");
    strcpy(alterReq.dbname, "1.*");

    int32_t contLen = tSerializeSAlterUserReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterUserReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_ADD_PRIVILEGES;
    alterReq.privileges = PRIVILEGE_TYPE_READ;
    strcpy(alterReq.user, "u3");
    strcpy(alterReq.pass, "1");
    strcpy(alterReq.dbname, "d1");

    int32_t contLen = tSerializeSAlterUserReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterUserReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_DB_NOT_EXIST);
  }

  {
    SCreateDbReq createReq = {0};
    strcpy(createReq.db, "1.d2");
    createReq.numOfVgroups = 2;
    createReq.buffer = -1;
    createReq.pageSize = -1;
    createReq.pages = -1;
    createReq.daysPerFile = 10 * 1440;
    createReq.daysToKeep0 = 3650 * 1440;
    createReq.daysToKeep1 = 3650 * 1440;
    createReq.daysToKeep2 = 3650 * 1440;
    createReq.minRows = 100;
    createReq.maxRows = 4096;
    createReq.walFsyncPeriod = 3000;
    createReq.walLevel = 1;
    createReq.precision = 0;
    createReq.compression = 2;
    createReq.replications = 1;
    createReq.strict = 1;
    createReq.cacheLast = 0;
    createReq.ignoreExist = 1;

    int32_t contLen = tSerializeSCreateDbReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDbReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_ADD_PRIVILEGES;
    alterReq.privileges = PRIVILEGE_TYPE_READ;
    strcpy(alterReq.user, "u3");
    strcpy(alterReq.pass, "1");
    strcpy(alterReq.dbname, "1.d2");

    int32_t contLen = tSerializeSAlterUserReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterUserReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_ADD_PRIVILEGES;
    alterReq.privileges = PRIVILEGE_TYPE_READ;
    strcpy(alterReq.user, "u3");
    strcpy(alterReq.pass, "1");
    strcpy(alterReq.dbname, "1.d2");

    int32_t contLen = tSerializeSAlterUserReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterUserReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SGetUserAuthReq authReq = {0};
    strcpy(authReq.user, "u3");
    int32_t contLen = tSerializeSGetUserAuthReq(NULL, 0, &authReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSGetUserAuthReq(pReq, contLen, &authReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_GET_USER_AUTH, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    SGetUserAuthRsp authRsp = {0};
    tDeserializeSGetUserAuthRsp(pRsp->pCont, pRsp->contLen, &authRsp);
    EXPECT_STREQ(authRsp.user, "u3");
    EXPECT_EQ(authRsp.superAuth, 0);
    int32_t numOfReadDbs = taosHashGetSize(authRsp.readDbs);
    int32_t numOfWriteDbs = taosHashGetSize(authRsp.writeDbs);
    EXPECT_EQ(numOfReadDbs, 1);
    EXPECT_EQ(numOfWriteDbs, 0);

    char* dbname = (char*)taosHashGet(authRsp.readDbs, "1.d2", 4);
    EXPECT_TRUE(dbname != NULL);

    taosHashCleanup(authRsp.readDbs);
    taosHashCleanup(authRsp.writeDbs);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_DEL_PRIVILEGES;
    alterReq.alterType = PRIVILEGE_TYPE_READ;
    strcpy(alterReq.user, "u3");
    strcpy(alterReq.pass, "1");
    strcpy(alterReq.dbname, "1.d2");

    int32_t contLen = tSerializeSAlterUserReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterUserReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SDropUserReq dropReq = {0};
    strcpy(dropReq.user, "u3");

    int32_t contLen = tSerializeSDropUserReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropUserReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowReq(TSDB_MGMT_TABLE_USER, "ins_users", "");
    EXPECT_EQ(test.GetShowRows(), 1);
  }
}

TEST_F(MndTestUser, 05_Drop_User) {
  {
    SDropUserReq dropReq = {0};
    strcpy(dropReq.user, "");

    int32_t contLen = tSerializeSDropUserReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropUserReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_USER_FORMAT);
  }

  {
    SDropUserReq dropReq = {0};
    strcpy(dropReq.user, "u4");

    int32_t contLen = tSerializeSDropUserReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropUserReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_USER_NOT_EXIST);
  }

  {
    SCreateUserReq createReq = {0};
    createReq.enable = 1;
    createReq.sysInfo = 1;
    strcpy(createReq.user, "u1");
    strcpy(createReq.pass, "p1");

    int32_t contLen = tSerializeSCreateUserReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateUserReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SDropUserReq dropReq = {0};
    strcpy(dropReq.user, "u1");

    int32_t contLen = tSerializeSDropUserReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropUserReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowReq(TSDB_MGMT_TABLE_USER, "ins_users", "");
  EXPECT_EQ(test.GetShowRows(), 1);
}

TEST_F(MndTestUser, 06_Create_Drop_Alter_User) {
  {
    SCreateUserReq createReq = {0};
    createReq.enable = 1;
    createReq.sysInfo = 1;
    strcpy(createReq.user, "u1");
    strcpy(createReq.pass, "p1");

    int32_t contLen = tSerializeSCreateUserReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateUserReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SCreateUserReq createReq = {0};
    createReq.enable = 1;
    createReq.sysInfo = 1;
    strcpy(createReq.user, "u2");
    strcpy(createReq.pass, "p2");

    int32_t contLen = tSerializeSCreateUserReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateUserReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowReq(TSDB_MGMT_TABLE_USER, "ins_users", "");
  EXPECT_EQ(test.GetShowRows(), 3);

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_PASSWD;
    strcpy(alterReq.user, "u1");
    strcpy(alterReq.pass, "p2");

    int32_t contLen = tSerializeSAlterUserReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterUserReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowReq(TSDB_MGMT_TABLE_USER, "ins_users", "");
  EXPECT_EQ(test.GetShowRows(), 3);
  {
    SDropUserReq dropReq = {0};
    strcpy(dropReq.user, "u1");

    int32_t contLen = tSerializeSDropUserReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropUserReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowReq(TSDB_MGMT_TABLE_USER, "ins_users", "");
  EXPECT_EQ(test.GetShowRows(), 2);

  // restart
  test.Restart();

  taosMsleep(1000);
  test.SendShowReq(TSDB_MGMT_TABLE_USER, "ins_users", "");
  EXPECT_EQ(test.GetShowRows(), 2);
}
