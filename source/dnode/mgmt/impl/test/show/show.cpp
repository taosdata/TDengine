/**
 * @file show.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module show-msg tests
 * @version 0.1
 * @date 2021-12-15
 *
 * @copyright Copyright (c) 2021
 *
 */

#include "base.h"

class DndTestShow : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init("/tmp/dnode_test_show", 9091); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestShow::test;

TEST_F(DndTestShow, 01_ShowMsg_InvalidMsgMax) {
  int32_t contLen = sizeof(SShowMsg);

  SShowMsg* pReq = (SShowMsg*)rpcMallocCont(contLen);
  pReq->type = TSDB_MGMT_TABLE_MAX;
  strcpy(pReq->db, "");

  SRpcMsg* pMsg = test.SendMsg(TSDB_MSG_TYPE_SHOW, pReq, contLen);
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_MSG_TYPE);
}

TEST_F(DndTestShow, 02_ShowMsg_InvalidMsgStart) {
  int32_t contLen = sizeof(SShowMsg);

  SShowMsg* pReq = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
  pReq->type = TSDB_MGMT_TABLE_START;
  strcpy(pReq->db, "");

  SRpcMsg* pMsg = test.SendMsg(TSDB_MSG_TYPE_SHOW, pReq, contLen);
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_MSG_TYPE);
}
