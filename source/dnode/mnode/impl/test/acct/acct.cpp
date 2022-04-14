/**
 * @file acct.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module acct tests
 * @version 1.0
 * @date 2022-01-04
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestAcct : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init("/tmp/mnode_test_acct", 9012); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase MndTestAcct::test;

TEST_F(MndTestAcct, 01_Create_Acct) {
  int32_t contLen = sizeof(SCreateAcctReq);

  SCreateAcctReq* pReq = (SCreateAcctReq*)rpcMallocCont(contLen);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_ACCT, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, TSDB_CODE_MND_MSG_NOT_PROCESSED);
}

TEST_F(MndTestAcct, 02_Alter_Acct) {
  int32_t contLen = sizeof(SCreateAcctReq);

  SAlterAcctReq* pReq = (SAlterAcctReq*)rpcMallocCont(contLen);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_ACCT, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, TSDB_CODE_MND_MSG_NOT_PROCESSED);
}

TEST_F(MndTestAcct, 03_Drop_Acct) {
  int32_t contLen = sizeof(SDropAcctReq);

  SDropAcctReq* pReq = (SDropAcctReq*)rpcMallocCont(contLen);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_ACCT, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, TSDB_CODE_MND_MSG_NOT_PROCESSED);
}

TEST_F(MndTestAcct, 04_Show_Acct) {
  SShowReq showReq = {0};
  showReq.type = TSDB_MGMT_TABLE_ACCT;

  int32_t contLen = tSerializeSShowReq(NULL, 0, &showReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSShowReq(pReq, contLen, &showReq);
  tFreeSShowReq(&showReq);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_SYSTABLE_RETRIEVE, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, TSDB_CODE_INVALID_MSG);
}