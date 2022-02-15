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

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_SHOW, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_MSG_TYPE);
}

TEST_F(MndTestShow, 02_ShowMsg_InvalidMsgStart) {
  SShowReq showReq = {0};
  showReq.type = TSDB_MGMT_TABLE_START;

  int32_t contLen = tSerializeSShowReq(NULL, 0, &showReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSShowReq(pReq, contLen, &showReq);
  tFreeSShowReq(&showReq);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_SHOW, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_MSG_TYPE);
}

TEST_F(MndTestShow, 03_ShowMsg_Conn) {
  int32_t contLen = sizeof(SConnectReq);

  SConnectReq* pReq = (SConnectReq*)rpcMallocCont(contLen);
  pReq->pid = htonl(1234);
  strcpy(pReq->app, "mnode_test_show");
  strcpy(pReq->db, "");

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_CONNECT, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, 0);

  test.SendShowMetaReq(TSDB_MGMT_TABLE_CONNS, "");

  STableMetaRsp* pMeta = test.GetShowMeta();
  EXPECT_STREQ(pMeta->tbName, "show connections");
  EXPECT_EQ(pMeta->numOfTags, 0);
  EXPECT_EQ(pMeta->numOfColumns, 7);
  EXPECT_EQ(pMeta->precision, 0);
  EXPECT_EQ(pMeta->tableType, 0);
  EXPECT_EQ(pMeta->update, 0);
  EXPECT_EQ(pMeta->sversion, 0);
  EXPECT_EQ(pMeta->tversion, 0);
  EXPECT_EQ(pMeta->tuid, 0);
  EXPECT_EQ(pMeta->suid, 0);

  test.SendShowRetrieveReq();

  SRetrieveTableRsp* pRetrieveRsp = test.GetRetrieveRsp();
  EXPECT_EQ(pRetrieveRsp->numOfRows, 1);
  EXPECT_EQ(pRetrieveRsp->useconds, 0);
  EXPECT_EQ(pRetrieveRsp->completed, 1);
  EXPECT_EQ(pRetrieveRsp->precision, TSDB_TIME_PRECISION_MILLI);
  EXPECT_EQ(pRetrieveRsp->compressed, 0);
  EXPECT_EQ(pRetrieveRsp->compLen, 0);
}

TEST_F(MndTestShow, 04_ShowMsg_Cluster) {
  test.SendShowMetaReq(TSDB_MGMT_TABLE_CLUSTER, "");
  CHECK_META( "show cluster", 3);
  CHECK_SCHEMA(0, TSDB_DATA_TYPE_BIGINT, 8, "id");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, TSDB_CLUSTER_ID_LEN + VARSTR_HEADER_SIZE, "name");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);

  IgnoreInt64();
  IgnoreBinary(TSDB_CLUSTER_ID_LEN);
  CheckTimestamp();
}