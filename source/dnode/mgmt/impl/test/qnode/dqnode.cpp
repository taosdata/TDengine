/**
 * @file dqnode.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module qnode tests
 * @version 1.0
 * @date 2022-01-05
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class DndTestQnode : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init("/tmp/dnode_test_qnode", 9111); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestQnode::test;

TEST_F(DndTestQnode, 04_Drop_User) {
  {
    int32_t contLen = sizeof(SDropUserReq);

    SDropUserReq* pReq = (SDropUserReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "");

    SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_USER, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_USER_FORMAT);
  }

  // {
  //   int32_t contLen = sizeof(SDropUserReq);

  //   SDropUserReq* pReq = (SDropUserReq*)rpcMallocCont(contLen);
  //   strcpy(pReq->user, "u4");

  //   SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_USER, pReq, contLen);
  //   ASSERT_NE(pMsg, nullptr);
  //   ASSERT_EQ(pMsg->code, TSDB_CODE_MND_USER_NOT_EXIST);
  // }

  // {
  //   int32_t contLen = sizeof(SDropUserReq);

  //   SDropUserReq* pReq = (SDropUserReq*)rpcMallocCont(contLen);
  //   strcpy(pReq->user, "u1");

  //   SRpcMsg* pMsg = test.SendMsg(TDMT_MND_DROP_USER, pReq, contLen);
  //   ASSERT_NE(pMsg, nullptr);
  //   ASSERT_EQ(pMsg->code, 0);
  // }

  // test.SendShowMetaMsg(TSDB_MGMT_TABLE_USER, "");
  // CHECK_META("show users", 4);

  // test.SendShowRetrieveMsg();
  // EXPECT_EQ(test.GetShowRows(), 1);
}