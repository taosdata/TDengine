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
  static void SetUpTestSuite() { test.Init(TD_TMP_DIR_PATH "acctTest", 9012); }
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
  ASSERT_EQ(pRsp->code, TSDB_CODE_OPS_NOT_SUPPORT);
  rpcFreeCont(pRsp->pCont);
}

TEST_F(MndTestAcct, 02_Alter_Acct) {
  int32_t contLen = sizeof(SCreateAcctReq);

  SAlterAcctReq* pReq = (SAlterAcctReq*)rpcMallocCont(contLen);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_ACCT, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, TSDB_CODE_OPS_NOT_SUPPORT);
  rpcFreeCont(pRsp->pCont);
}

TEST_F(MndTestAcct, 03_Drop_Acct) {
  int32_t contLen = sizeof(SDropAcctReq);

  SDropAcctReq* pReq = (SDropAcctReq*)rpcMallocCont(contLen);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_ACCT, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, TSDB_CODE_OPS_NOT_SUPPORT);
  rpcFreeCont(pRsp->pCont);
}
