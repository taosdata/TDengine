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

#include "sut.h"

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

  SRpcMsg* pMsg = test.SendMsg(TDMT_MND_SHOW, pReq, contLen);
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_MSG_TYPE);
}

TEST_F(DndTestShow, 02_ShowMsg_InvalidMsgStart) {
  int32_t contLen = sizeof(SShowMsg);

  SShowMsg* pReq = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
  pReq->type = TSDB_MGMT_TABLE_START;
  strcpy(pReq->db, "");

  SRpcMsg* pMsg = test.SendMsg(TDMT_MND_SHOW, pReq, contLen);
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_MSG_TYPE);
}

TEST_F(DndTestShow, 02_ShowMsg_Conn) {
  int32_t contLen = sizeof(SConnectMsg);

  SConnectMsg* pReq = (SConnectMsg*)rpcMallocCont(contLen);
  pReq->pid = htonl(1234);
  strcpy(pReq->app, "dnode_test_show");
  strcpy(pReq->db, "");

  SRpcMsg* pMsg = test.SendMsg(TDMT_MND_CONNECT, pReq, contLen);
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, 0);

  test.SendShowMetaMsg(TSDB_MGMT_TABLE_CONNS, "");

  STableMetaMsg* pMeta = test.GetShowMeta();
  EXPECT_STREQ(pMeta->tbFname, "show connections");
  EXPECT_EQ(pMeta->numOfTags, 0);
  EXPECT_EQ(pMeta->numOfColumns, 7);
  EXPECT_EQ(pMeta->precision, 0);
  EXPECT_EQ(pMeta->tableType, 0);
  EXPECT_EQ(pMeta->update, 0);
  EXPECT_EQ(pMeta->sversion, 0);
  EXPECT_EQ(pMeta->tversion, 0);
  EXPECT_EQ(pMeta->tuid, 0);
  EXPECT_EQ(pMeta->suid, 0);

  test.SendShowRetrieveMsg();

  SRetrieveTableRsp* pRetrieveRsp = test.GetRetrieveRsp();
  EXPECT_EQ(pRetrieveRsp->numOfRows, 1);
  EXPECT_EQ(pRetrieveRsp->useconds, 0);
  EXPECT_EQ(pRetrieveRsp->completed, 1);
  EXPECT_EQ(pRetrieveRsp->precision, TSDB_TIME_PRECISION_MILLI);
  EXPECT_EQ(pRetrieveRsp->compressed, 0);
  EXPECT_EQ(pRetrieveRsp->compLen, 0);
}
