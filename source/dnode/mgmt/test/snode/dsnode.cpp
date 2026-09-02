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

extern "C" {
#include "smInt.h"
#include "tglobal.h"
}

namespace {

int32_t dispatchToFirstTriggerWorker(SDispatchWorkerPool*, void*, int32_t* pWorkerIdx) {
  *pWorkerIdx = 0;
  return TSDB_CODE_SUCCESS;
}

}  // namespace

TEST(SnodeQueueOwnershipTest, TriggerDispatchFailureReleasesTransferredRpcItem) {
  SSnodeMgmt      mgmt = {};
  SDispatchWorker worker = {};
  mgmt.pSnode = reinterpret_cast<SSnode*>(&mgmt);
  mgmt.triggerWorkerPool.name = "snode-queue-ownership-test";
  mgmt.triggerWorkerPool.num = 1;
  mgmt.triggerWorkerPool.pWorkers = &worker;
  mgmt.triggerWorkerPool.dispatchFp = dispatchToFirstTriggerWorker;
  ASSERT_EQ(0, taosThreadMutexInit(&mgmt.triggerWorkerPool.poolLock, nullptr));
  ASSERT_EQ(0, taosOpenQueue(&worker.queue));
  taosSetQueueMemoryCapacity(worker.queue, 1);

  const int64_t queueMemoryBefore = atomic_load_64(&tsQueueMemoryUsed);
  const int64_t queueMemoryAllowedBefore = tsQueueMemoryAllowed;
  tsQueueMemoryAllowed = INT64_MAX;

  SRpcMsg msg = {};
  msg.msgType = TDMT_STREAM_TRIGGER_CTRL;
  msg.contLen = 94;
  msg.pCont = rpcMallocCont(msg.contLen);
  ASSERT_NE(nullptr, msg.pCont);

  EXPECT_EQ(TSDB_CODE_UTIL_QUEUE_OUT_OF_MEMORY, smPutMsgToQueue(&mgmt, STREAM_TRIGGER_QUEUE, &msg));
  EXPECT_EQ(nullptr, msg.pCont);
  EXPECT_EQ(queueMemoryBefore, atomic_load_64(&tsQueueMemoryUsed));

  tsQueueMemoryAllowed = queueMemoryAllowedBefore;
  taosCloseQueue(worker.queue);
  taosThreadMutexDestroy(&mgmt.triggerWorkerPool.poolLock);
}

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
#if 0
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
#endif
