/**
 * @file dsnode.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module snode tests
 * @version 1.0
 * @date 2022-01-05
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class DndTestSnode : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init(TD_TMP_DIR_PATH "dsnodeTest", 9113); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestSnode::test;

TEST_F(DndTestSnode, 01_Create_Snode) {
  {
    SDCreateSnodeReq createReq = {0};
    createReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_INVALID_OPTION);
    rpcFreeCont(pRsp->pCont);
  }

  {
    SDCreateSnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    SDCreateSnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_SNODE_ALREADY_DEPLOYED);
    rpcFreeCont(pRsp->pCont);
  }

  test.Restart();

  {
    SDCreateSnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_SNODE_ALREADY_DEPLOYED);
    rpcFreeCont(pRsp->pCont);
  }
}

TEST_F(DndTestSnode, 01_Drop_Snode) {
#if 0
  {
    SDDropSnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_INVALID_OPTION);
    rpcFreeCont(pRsp->pCont);
  }
#endif

  {
    SDDropSnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    SDDropSnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_SNODE_NOT_DEPLOYED);
    rpcFreeCont(pRsp->pCont);
  }

  test.Restart();

  {
    SDDropSnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_SNODE_NOT_DEPLOYED);
    rpcFreeCont(pRsp->pCont);
  }

  {
    SDCreateSnodeReq createReq = {0};
    createReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_SNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }
}