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
  static void SetUpTestSuite() { test.Init("/tmp/mnode_test_user", 9011); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase MndTestUser::test;

TEST_F(MndTestUser, 01_Show_User) {
  test.SendShowMetaReq(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  CHECK_SCHEMA(0, TSDB_DATA_TYPE_BINARY, TSDB_USER_LEN + VARSTR_HEADER_SIZE, "name");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, 10 + VARSTR_HEADER_SIZE, "privilege");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");
  CHECK_SCHEMA(3, TSDB_DATA_TYPE_BINARY, TSDB_USER_LEN + VARSTR_HEADER_SIZE, "account");

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);

  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("super", 10);
  CheckTimestamp();
  CheckBinary("root", TSDB_USER_LEN);
}

TEST_F(MndTestUser, 02_Create_User) {
  {
    SCreateUserReq createReq = {0};
    strcpy(createReq.user, "");
    strcpy(createReq.pass, "p1");

    int32_t contLen = tSerializeSCreateUserReq(NULL, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSCreateUserReq(&pBuf, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_USER_FORMAT);
  }

  {
    SCreateUserReq createReq = {0};
    strcpy(createReq.user, "u1");
    strcpy(createReq.pass, "");

    int32_t contLen = tSerializeSCreateUserReq(NULL, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSCreateUserReq(&pBuf, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_PASS_FORMAT);
  }

  {
    SCreateUserReq createReq = {0};
    strcpy(createReq.user, "root");
    strcpy(createReq.pass, "1");

    int32_t contLen = tSerializeSCreateUserReq(NULL, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSCreateUserReq(&pBuf, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_USER_ALREADY_EXIST);
  }

  {
    SCreateUserReq createReq = {0};
    strcpy(createReq.user, "u1");
    strcpy(createReq.pass, "p1");

    int32_t contLen = tSerializeSCreateUserReq(NULL, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSCreateUserReq(&pBuf, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 2);
}

TEST_F(MndTestUser, 03_Alter_User) {
  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_PASSWD;
    strcpy(alterReq.user, "");
    strcpy(alterReq.pass, "p1");

    int32_t contLen = tSerializeSAlterUserReq(NULL, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSAlterUserReq(&pBuf, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_USER_FORMAT);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_PASSWD;
    strcpy(alterReq.user, "u1");
    strcpy(alterReq.pass, "");

    int32_t contLen = tSerializeSAlterUserReq(NULL, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSAlterUserReq(&pBuf, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_PASS_FORMAT);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_PASSWD;
    strcpy(alterReq.user, "u4");
    strcpy(alterReq.pass, "1");

    int32_t contLen = tSerializeSAlterUserReq(NULL, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSAlterUserReq(&pBuf, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_USER_NOT_EXIST);
  }

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_PASSWD;
    strcpy(alterReq.user, "u1");
    strcpy(alterReq.pass, "1");

    int32_t contLen = tSerializeSAlterUserReq(NULL, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSAlterUserReq(&pBuf, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }
}

TEST_F(MndTestUser, 04_Drop_User) {
  {
    SDropUserReq dropReq = {0};
    strcpy(dropReq.user, "");

    int32_t contLen = tSerializeSDropUserReq(NULL, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSDropUserReq(&pBuf, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_USER_FORMAT);
  }

  {
    SDropUserReq dropReq = {0};
    strcpy(dropReq.user, "u4");

    int32_t contLen = tSerializeSDropUserReq(NULL, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSDropUserReq(&pBuf, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_USER_NOT_EXIST);
  }

  {
    SDropUserReq dropReq = {0};
    strcpy(dropReq.user, "u1");

    int32_t contLen = tSerializeSDropUserReq(NULL, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSDropUserReq(&pBuf, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);
}

TEST_F(MndTestUser, 05_Create_Drop_Alter_User) {
  {
    SCreateUserReq createReq = {0};
    strcpy(createReq.user, "u1");
    strcpy(createReq.pass, "p1");

    int32_t contLen = tSerializeSCreateUserReq(NULL, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSCreateUserReq(&pBuf, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SCreateUserReq createReq = {0};
    strcpy(createReq.user, "u2");
    strcpy(createReq.pass, "p2");

    int32_t contLen = tSerializeSCreateUserReq(NULL, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSCreateUserReq(&pBuf, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 3);

  CheckBinary("u1", TSDB_USER_LEN);
  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("u2", TSDB_USER_LEN);
  CheckBinary("normal", 10);
  CheckBinary("super", 10);
  CheckBinary("normal", 10);
  CheckTimestamp();
  CheckTimestamp();
  CheckTimestamp();
  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("root", TSDB_USER_LEN);

  {
    SAlterUserReq alterReq = {0};
    alterReq.alterType = TSDB_ALTER_USER_PASSWD;
    strcpy(alterReq.user, "u1");
    strcpy(alterReq.pass, "p2");

    int32_t contLen = tSerializeSAlterUserReq(NULL, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSAlterUserReq(&pBuf, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 3);

  CheckBinary("u1", TSDB_USER_LEN);
  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("u2", TSDB_USER_LEN);
  CheckBinary("normal", 10);
  CheckBinary("super", 10);
  CheckBinary("normal", 10);
  CheckTimestamp();
  CheckTimestamp();
  CheckTimestamp();
  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("root", TSDB_USER_LEN);

  {
    SDropUserReq dropReq = {0};
    strcpy(dropReq.user, "u1");

    int32_t contLen = tSerializeSDropUserReq(NULL, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    void*   pBuf = pReq;
    tSerializeSDropUserReq(&pBuf, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 2);

  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("u2", TSDB_USER_LEN);
  CheckBinary("super", 10);
  CheckBinary("normal", 10);
  CheckTimestamp();
  CheckTimestamp();
  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("root", TSDB_USER_LEN);

  // restart
  test.Restart();

  test.SendShowMetaReq(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 2);

  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("u2", TSDB_USER_LEN);
  CheckBinary("super", 10);
  CheckBinary("normal", 10);
  CheckTimestamp();
  CheckTimestamp();
  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("root", TSDB_USER_LEN);
}