/**
 * @file show.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module show tests
 * @version 1.0
 * @date 2022-01-06
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestShow : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init("/tmp/mnode_test_show", 9021); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase MndTestShow::test;

TEST_F(MndTestShow, 01_ShowMsg_InvalidMsgMax) {
  SShowReq showReq = {0};
  showReq.type = TSDB_MGMT_TABLE_MAX;

  int32_t contLen = tSerializeSShowReq(NULL, 0, &showReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSShowReq(pReq, contLen, &showReq);
  tFreeSShowReq(&showReq);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_SYSTABLE_RETRIEVE, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, TSDB_CODE_INVALID_MSG);
}

TEST_F(MndTestShow, 02_ShowMsg_InvalidMsgStart) {
  SShowReq showReq = {0};
  showReq.type = TSDB_MGMT_TABLE_START;

  int32_t contLen = tSerializeSShowReq(NULL, 0, &showReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSShowReq(pReq, contLen, &showReq);
  tFreeSShowReq(&showReq);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_SYSTABLE_RETRIEVE, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, TSDB_CODE_INVALID_MSG);
}

TEST_F(MndTestShow, 03_ShowMsg_Conn) {
  SConnectReq connectReq = {0};
  connectReq.pid = 1234;
  strcpy(connectReq.app, "mnode_test_show");
  strcpy(connectReq.db, "");

  int32_t contLen = tSerializeSConnectReq(NULL, 0, &connectReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSConnectReq(pReq, contLen, &connectReq);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_CONNECT, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, 0);

  test.SendShowReq(TSDB_MGMT_TABLE_CONNS, "connections", "");
  // EXPECT_EQ(test.GetShowRows(), 1);
}

TEST_F(MndTestShow, 04_ShowMsg_Cluster) {
  test.SendShowReq(TSDB_MGMT_TABLE_CLUSTER, "cluster", "");
  EXPECT_EQ(test.GetShowRows(), 1);
}