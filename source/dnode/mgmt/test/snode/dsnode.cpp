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
#include "query.h"
#include "smInt.h"
#include "tglobal.h"
}

namespace {

int32_t dispatchToFirstTriggerWorker(SDispatchWorkerPool*, void*, int32_t* pWorkerIdx) {
  *pWorkerIdx = 0;
  return TSDB_CODE_SUCCESS;
}

int32_t g_triggerAhandleFreeCount = 0;

void testTriggerAhandleParamFree(void* param) {
  ++g_triggerAhandleFreeCount;
  taosMemoryFree(param);
}

SMsgSendInfo* testAllocTriggerAhandle() {
  auto* pInfo = static_cast<SMsgSendInfo*>(taosMemoryCalloc(1, sizeof(SMsgSendInfo)));
  if (pInfo == nullptr) {
    return nullptr;
  }
  pInfo->streamAHandle = 1;
  pInfo->param = taosMemoryCalloc(1, sizeof(int32_t));
  pInfo->target.dbFName = static_cast<char*>(taosMemoryMalloc(16));
  pInfo->msgInfo.pData = taosMemoryMalloc(16);
  pInfo->msgInfo.len = 16;
  if (pInfo->param == nullptr || pInfo->target.dbFName == nullptr || pInfo->msgInfo.pData == nullptr) {
    taosMemoryFree(pInfo->param);
    taosMemoryFree(pInfo->target.dbFName);
    taosMemoryFree(pInfo->msgInfo.pData);
    taosMemoryFree(pInfo);
    return nullptr;
  }
  pInfo->paramFreeFp = testTriggerAhandleParamFree;
  // The three buffers above are exactly what destroySendMsgInfo() releases, so an ASAN/LSAN
  // run of these tests fails if any of those release paths is ever dropped.
  return pInfo;
}

// Queues the message the same way the snode worker queues do it and then tears the queue
// down: register smFreeRpcQitem as the free hook (as smStartWorker() does for the runner
// queue and for every trigger worker queue), write the item, close the queue.  Going through
// taosQueueSetFreeFp/taosCloseQueue keeps a missing hook registration detectable, which a
// direct smFreeRpcQitem() call would not.
int32_t testQueueAndCloseWithFreeHook(SRpcMsg* msg) {
  // dataSize is derived from contLen, so a message without a cont must not claim one.
  if (msg->pCont == nullptr && msg->contLen != 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  STaosQueue* queue = nullptr;
  int32_t     code = taosOpenQueue(&queue);
  if (code != 0) {
    return code;
  }
  taosQueueSetFreeFp(queue, smFreeRpcQitem);

  void* qitem = nullptr;
  // Response items are allocated as DEF_QITEM (see dmProcessRpcMsg), so this also keeps the
  // queue memory accounting globals out of the test.
  code = taosAllocateQitem(sizeof(SRpcMsg), DEF_QITEM, msg->contLen, &qitem);
  if (code == 0) {
    *static_cast<SRpcMsg*>(qitem) = *msg;
    code = taosWriteQitem(queue, qitem);
    if (code != 0) {
      taosFreeQitem(qitem);
    }
  }
  if (code != 0) {
    // Nothing was enqueued, so the free hook will not run for this message: release the cont
    // here to keep the failure path leak-free as well.
    rpcFreeCont(msg->pCont);
    msg->pCont = nullptr;
    msg->contLen = 0;
  }

  taosCloseQueue(queue);
  return code;
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

// A stream trigger response that is still queued when the snode worker queue is torn down
// must release the send handle of the trigger task exactly once.  A response type missing
// from tmsgIsStreamTriggerRsp() is exactly what leaked the EXT PULL response handle, so
// every registered type is exercised through the real queue free-hook chain.
TEST(SnodeQueueOwnershipTest, TriggerRspAhandleReleasedOnceWhenQueueIsClosed) {
  const tmsg_t triggerRspTypes[] = {TDMT_STREAM_TRIGGER_CALC_RSP, TDMT_STREAM_TRIGGER_PULL_RSP,
                                    TDMT_STREAM_TRIGGER_DROP_RSP, TDMT_STREAM_TRIGGER_PULL_EXT_RSP};
  for (tmsg_t type : triggerRspTypes) {
    g_triggerAhandleFreeCount = 0;

    SRpcMsg msg = {};
    msg.msgType = type;
    // A real cont: releasing it is the free hook's first duty, so an ASAN/LSAN run of this
    // case fails if that release path is ever dropped.
    msg.contLen = 8;
    msg.pCont = rpcMallocCont(msg.contLen);
    ASSERT_NE(nullptr, msg.pCont) << "type:" << type;
    msg.info.ahandle = testAllocTriggerAhandle();
    ASSERT_NE(nullptr, msg.info.ahandle) << "type:" << type;

    ASSERT_EQ(0, testQueueAndCloseWithFreeHook(&msg)) << "type:" << type;
    EXPECT_EQ(1, g_triggerAhandleFreeCount) << "type:" << type;
  }
}

// An item whose handle was already released must not release it again when another free
// path sees the same message.
TEST(SnodeQueueOwnershipTest, ReleasedAhandleIsNotReleasedTwice) {
  g_triggerAhandleFreeCount = 0;

  SRpcMsg msg = {};
  msg.msgType = TDMT_STREAM_TRIGGER_PULL_EXT_RSP;
  msg.info.ahandle = testAllocTriggerAhandle();
  ASSERT_NE(nullptr, msg.info.ahandle);

  smFreeRpcQitem(&msg);
  EXPECT_EQ(1, g_triggerAhandleFreeCount);
  EXPECT_EQ(nullptr, msg.info.ahandle);

  smFreeRpcQitem(&msg);
  EXPECT_EQ(1, g_triggerAhandleFreeCount);
}

// The queue free hook only owns the trigger task send handles: a message that reaches the
// snode queues but carries no trigger send handle must be left untouched.
TEST(SnodeQueueOwnershipTest, ForeignAhandleSurvivesQueueTeardown) {
  g_triggerAhandleFreeCount = 0;

  auto* pInfo = testAllocTriggerAhandle();
  ASSERT_NE(nullptr, pInfo);

  SRpcMsg msg = {};
  // Routed to the snode runner queue (same free hook) but not a stream trigger response.
  msg.msgType = TDMT_STREAM_SYNC_CHECKPOINT_RSP;
  msg.contLen = 8;
  msg.pCont = rpcMallocCont(msg.contLen);
  ASSERT_NE(nullptr, msg.pCont);
  msg.info.ahandle = pInfo;

  ASSERT_EQ(0, testQueueAndCloseWithFreeHook(&msg));
  EXPECT_EQ(0, g_triggerAhandleFreeCount);

  // Still owned by the test: proving the hook did not release it.
  destroyAhandle(pInfo);
  EXPECT_EQ(1, g_triggerAhandleFreeCount);
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
