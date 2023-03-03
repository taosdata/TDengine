/**
 * @file dmnode.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module mnode tests
 * @version 1.0
 * @date 2022-01-07
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class DndTestMnode : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init(TD_TMP_DIR_PATH "dmnodeTest", 9114); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestMnode::test;

TEST_F(DndTestMnode, 01_Create_Mnode) {
  {
    SDCreateMnodeReq createReq = {0};
    createReq.dnodeId = 2;
    createReq.replica = 1;
    createReq.replicas[0].id = 1;
    createReq.replicas[0].port = 9113;
    strcpy(createReq.replicas[0].fqdn, "localhost");

    int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDCreateMnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MNODE_ALREADY_DEPLOYED);
  }

  {
    SDCreateMnodeReq createReq = {0};
    createReq.dnodeId = 1;
    createReq.replica = 1;
    createReq.replicas[0].id = 2;
    createReq.replicas[0].port = 9113;
    strcpy(createReq.replicas[0].fqdn, "localhost");

    int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDCreateMnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MNODE_ALREADY_DEPLOYED);
  }

  {
    SDCreateMnodeReq createReq = {0};
    createReq.dnodeId = 1;
    createReq.replica = 2;
    createReq.replicas[0].id = 1;
    createReq.replicas[0].port = 9113;
    strcpy(createReq.replicas[0].fqdn, "localhost");
    createReq.replicas[1].id = 1;
    createReq.replicas[1].port = 9114;
    strcpy(createReq.replicas[1].fqdn, "localhost");

    int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDCreateMnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MNODE_ALREADY_DEPLOYED);
  }
}

TEST_F(DndTestMnode, 02_Alter_Mnode) {
  {
    SDAlterMnodeReq alterReq = {0};
    alterReq.dnodeId = 2;
    alterReq.replica = 1;
    alterReq.replicas[0].id = 1;
    alterReq.replicas[0].port = 9113;
    strcpy(alterReq.replicas[0].fqdn, "localhost");

    int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDCreateMnodeReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_INVALID_OPTION);
  }

  {
    SDAlterMnodeReq alterReq = {0};
    alterReq.dnodeId = 1;
    alterReq.replica = 1;
    alterReq.replicas[0].id = 2;
    alterReq.replicas[0].port = 9113;
    strcpy(alterReq.replicas[0].fqdn, "localhost");

    int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDCreateMnodeReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_INVALID_OPTION);
  }

  {
    SDAlterMnodeReq alterReq = {0};
    alterReq.dnodeId = 1;
    alterReq.replica = 1;
    alterReq.replicas[0].id = 1;
    alterReq.replicas[0].port = 9113;
    strcpy(alterReq.replicas[0].fqdn, "localhost");

    int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDCreateMnodeReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }
}

TEST_F(DndTestMnode, 03_Drop_Mnode) {
  {
    SDDropMnodeReq dropReq = {0};
    dropReq.dnodeId = 2;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_INVALID_OPTION);
  }

  {
    SDDropMnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SDDropMnodeReq dropReq = {0};
    dropReq.dnodeId = 1;

    int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MNODE_NOT_DEPLOYED);
  }

  {
    SDAlterMnodeReq alterReq = {0};
    alterReq.dnodeId = 1;
    alterReq.replica = 1;
    alterReq.replicas[0].id = 1;
    alterReq.replicas[0].port = 9113;
    strcpy(alterReq.replicas[0].fqdn, "localhost");

    int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDCreateMnodeReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MNODE_NOT_DEPLOYED);
  }

  {
    SDCreateMnodeReq createReq = {0};
    createReq.dnodeId = 1;
    createReq.replica = 2;
    createReq.replicas[0].id = 1;
    createReq.replicas[0].port = 9113;
    strcpy(createReq.replicas[0].fqdn, "localhost");

    int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDCreateMnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_MNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }
}