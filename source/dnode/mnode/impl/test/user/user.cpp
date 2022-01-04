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
  test.SendShowMetaMsg(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  CHECK_SCHEMA(0, TSDB_DATA_TYPE_BINARY, TSDB_USER_LEN + VARSTR_HEADER_SIZE, "name");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, 10 + VARSTR_HEADER_SIZE, "privilege");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");
  CHECK_SCHEMA(3, TSDB_DATA_TYPE_BINARY, TSDB_USER_LEN + VARSTR_HEADER_SIZE, "account");

  test.SendShowRetrieveMsg();
  EXPECT_EQ(test.GetShowRows(), 1);

  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("super", 10);
  CheckTimestamp();
  CheckBinary("root", TSDB_USER_LEN);
}

TEST_F(MndTestUser, 02_Create_User) {
  {
    int32_t contLen = sizeof(SCreateUserReq);

    SCreateUserReq* pReq = (SCreateUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "");
    strcpy(pReq->pass, "p1");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_USER_FORMAT);
  }

  {
    int32_t contLen = sizeof(SCreateUserReq);

    SCreateUserReq* pReq = (SCreateUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u1");
    strcpy(pReq->pass, "");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_PASS_FORMAT);
  }

  {
    int32_t contLen = sizeof(SCreateUserReq);

    SCreateUserReq* pReq = (SCreateUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "root");
    strcpy(pReq->pass, "1");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_USER_ALREADY_EXIST);
  }

  {
    int32_t contLen = sizeof(SCreateUserReq);

    SCreateUserReq* pReq = (SCreateUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u1");
    strcpy(pReq->pass, "p1");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }

  test.SendShowMetaMsg(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  test.SendShowRetrieveMsg();
  EXPECT_EQ(test.GetShowRows(), 2);
}

TEST_F(MndTestUser, 03_Alter_User) {
  {
    int32_t contLen = sizeof(SAlterUserReq);

    SAlterUserReq* pReq = (SAlterUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "");
    strcpy(pReq->pass, "p1");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_USER_FORMAT);
  }

  {
    int32_t contLen = sizeof(SAlterUserReq);

    SAlterUserReq* pReq = (SAlterUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u1");
    strcpy(pReq->pass, "");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_PASS_FORMAT);
  }

  {
    int32_t contLen = sizeof(SAlterUserReq);

    SAlterUserReq* pReq = (SAlterUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u4");
    strcpy(pReq->pass, "1");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_USER_NOT_EXIST);
  }

  {
    int32_t contLen = sizeof(SAlterUserReq);

    SAlterUserReq* pReq = (SAlterUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u1");
    strcpy(pReq->pass, "1");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }
}

TEST_F(MndTestUser, 04_Drop_User) {
  {
    int32_t contLen = sizeof(SDropUserReq);

    SDropUserReq* pReq = (SDropUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_USER_FORMAT);
  }

  {
    int32_t contLen = sizeof(SDropUserReq);

    SDropUserReq* pReq = (SDropUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u4");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_USER_NOT_EXIST);
  }

  {
    int32_t contLen = sizeof(SDropUserReq);

    SDropUserReq* pReq = (SDropUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u1");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }

  test.SendShowMetaMsg(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  test.SendShowRetrieveMsg();
  EXPECT_EQ(test.GetShowRows(), 1);
}

TEST_F(MndTestUser, 05_Create_Drop_Alter_User) {
  {
    int32_t contLen = sizeof(SCreateUserReq);

    SCreateUserReq* pReq = (SCreateUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u1");
    strcpy(pReq->pass, "p1");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }

  {
    int32_t contLen = sizeof(SCreateUserReq);

    SCreateUserReq* pReq = (SCreateUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u2");
    strcpy(pReq->pass, "p2");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CREATE_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }

  test.SendShowMetaMsg(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  test.SendShowRetrieveMsg();
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
    int32_t contLen = sizeof(SAlterUserReq);

    SAlterUserReq* pReq = (SAlterUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u1");
    strcpy(pReq->pass, "p2");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_ALTER_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }

  test.SendShowMetaMsg(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  test.SendShowRetrieveMsg();
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
    int32_t contLen = sizeof(SDropUserReq);

    SDropUserReq* pReq = (SDropUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u1");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }

  test.SendShowMetaMsg(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  test.SendShowRetrieveMsg();
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

  test.SendShowMetaMsg(TSDB_MGMT_TABLE_USER, "");
  CHECK_META("show users", 4);

  test.SendShowRetrieveMsg();
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