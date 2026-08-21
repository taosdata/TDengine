/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

extern "C" {
#include "mmInt.h"
#include "mndInt.h"
#include "streamReader.h"
#include "tglobal.h"
#include "tmsgcb.h"
#include "vnodeInt.h"
}

namespace {

constexpr auto kWaitTimeout = std::chrono::seconds(5);

struct ObservedItem {
  void*   handle;
  void*   payload;
  int32_t responseCode;
  int32_t responses;
  int32_t frees;
};

struct ReaderSideEffectCounts {
  int32_t initStorage;
  int32_t destroyTask;
  int32_t createTask;
  int32_t resetScan;
  int32_t setTaskId;
  int32_t updateOperator;
  int32_t setStreamGen;
  int32_t setScalarExtra;
  int32_t executeTask;
};

std::mutex                gMutex;
std::condition_variable   gCv;
std::vector<ObservedItem> gItems;
SMnodeMgmt*               gMgmt = nullptr;
bool                      gBlockFirstResponse = false;
bool                      gFirstResponseEntered = false;
bool                      gReleaseFirstResponse = false;
bool                      gResponseBarrierTimedOut = false;
int32_t                   gCleanupSignals = 0;
int32_t                   gHandlerCalls = 0;
SArray*                   gReaderCalcInfos = nullptr;
ReaderSideEffectCounts    gReaderSideEffects = {};

ObservedItem* findByHandle(void* handle) {
  for (auto& item : gItems) {
    if (item.handle == handle) return &item;
  }
  return nullptr;
}

ObservedItem* findByPayload(void* payload) {
  for (auto& item : gItems) {
    if (item.payload == payload) return &item;
  }
  return nullptr;
}

int32_t businessHandler(SRpcMsg*) {
  std::lock_guard<std::mutex> lock(gMutex);
  ++gHandlerCalls;
  gCv.notify_all();
  return TSDB_CODE_SUCCESS;
}

bool waitUntil(const std::function<bool()>& predicate) {
  std::unique_lock<std::mutex> lock(gMutex);
  return gCv.wait_for(lock, kWaitTimeout, predicate);
}

class MgmtMnodeWorkerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    {
      std::lock_guard<std::mutex> lock(gMutex);
      gItems.clear();
      gMgmt = &mgmt_;
      gBlockFirstResponse = false;
      gFirstResponseEntered = false;
      gReleaseFirstResponse = false;
      gResponseBarrierTimedOut = false;
      gCleanupSignals = 0;
      gHandlerCalls = 0;
      gReaderSideEffects = {};
    }
    gReaderCalcInfos = nullptr;

    savedQueryThreads_ = tsNumOfMnodeQueryThreads;
    savedFetchThreads_ = tsNumOfMnodeFetchThreads;
    savedReadThreads_ = tsNumOfMnodeReadThreads;
    savedQueryThreadsTotal_ = tsNumOfQueryThreads;
    savedQueueMemoryAllowed_ = tsQueueMemoryAllowed;
    tsNumOfMnodeQueryThreads = 1;
    tsNumOfMnodeFetchThreads = 1;
    tsNumOfMnodeReadThreads = 1;
    tsQueueMemoryAllowed = 1024 * 1024;

    ASSERT_EQ(0, taosThreadRwlockInit(&mnode_.lock, nullptr));
    ASSERT_EQ(0, taosThreadRwlockInit(&mgmt_.lock, nullptr));
    mnode_.stopped = true;
    mnode_.msgFp[TMSG_INDEX(TDMT_MND_CREATE_STREAM)] = businessHandler;
    mgmt_.pMnode = &mnode_;
    ASSERT_EQ(TSDB_CODE_SUCCESS, mmStartWorker(&mgmt_));

    msgCb_.mgmt = reinterpret_cast<SMgmtWrapper*>(&mgmt_);
    msgCb_.putToQueueFp = reinterpret_cast<PutToQueueFp>(mmPutMsgToQueue);
  }

  void TearDown() override {
    if (!workersStopped_) mmStopWorker(&mgmt_);
    taosThreadRwlockDestroy(&mgmt_.lock);
    taosThreadRwlockDestroy(&mnode_.lock);
    tsNumOfMnodeQueryThreads = savedQueryThreads_;
    tsNumOfMnodeFetchThreads = savedFetchThreads_;
    tsNumOfMnodeReadThreads = savedReadThreads_;
    tsNumOfQueryThreads = savedQueryThreadsTotal_;
    tsQueueMemoryAllowed = savedQueueMemoryAllowed_;
    std::lock_guard<std::mutex> lock(gMutex);
    gMgmt = nullptr;
    gItems.clear();
    taosArrayDestroy(gReaderCalcInfos);
    gReaderCalcInfos = nullptr;
  }

  void enqueue(void* handle) {
    void* payload = rpcMallocCont(16);
    ASSERT_NE(nullptr, payload);
    {
      std::lock_guard<std::mutex> lock(gMutex);
      gItems.push_back({handle, payload, TSDB_CODE_SUCCESS, 0, 0});
    }
    SRpcMsg msg = {};
    msg.msgType = TDMT_MND_CREATE_STREAM;
    msg.pCont = payload;
    msg.contLen = 16;
    msg.info.handle = handle;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tmsgPutToQueue(&msgCb_, WRITE_QUEUE, &msg));
    EXPECT_EQ(nullptr, msg.pCont);
  }

  void stopWorkers() {
    mmStopWorker(&mgmt_);
    workersStopped_ = true;
  }

  SMnode     mnode_ = {};
  SMnodeMgmt mgmt_ = {};
  SMsgCb     msgCb_ = {};
  bool       workersStopped_ = false;
  int32_t    savedQueryThreads_ = 0;
  int32_t    savedFetchThreads_ = 0;
  int32_t    savedReadThreads_ = 0;
  int32_t    savedQueryThreadsTotal_ = 0;
  int64_t    savedQueueMemoryAllowed_ = 0;
};

TEST_F(MgmtMnodeWorkerTest, StoppingPrecheckRespondsAndFreesAcceptedPayload) {
  void* handle = reinterpret_cast<void*>(static_cast<uintptr_t>(0x1001));
  enqueue(handle);

  ASSERT_TRUE(waitUntil([&] {
    ObservedItem* item = findByHandle(handle);
    return item != nullptr && item->responses == 1 && item->frees == 1;
  }));
  stopWorkers();

  std::lock_guard<std::mutex> lock(gMutex);
  ASSERT_EQ(1U, gItems.size());
  EXPECT_EQ(1, gItems[0].responses);
  EXPECT_EQ(TSDB_CODE_APP_IS_STOPPING, gItems[0].responseCode);
  EXPECT_EQ(1, gItems[0].frees);
  EXPECT_EQ(0, gHandlerCalls);
}

TEST_F(MgmtMnodeWorkerTest, StopDrainsEveryAcceptedWriteItemBeforeCleanupReturns) {
  constexpr int32_t kItemCount = 4;
  {
    std::lock_guard<std::mutex> lock(gMutex);
    gBlockFirstResponse = true;
  }

  enqueue(reinterpret_cast<void*>(static_cast<uintptr_t>(0x2001)));
  ASSERT_TRUE(waitUntil([] { return gFirstResponseEntered; }));

  for (int32_t i = 1; i < kItemCount; ++i) {
    enqueue(reinterpret_cast<void*>(static_cast<uintptr_t>(0x2001 + i)));
  }

  std::thread stopThread([this] { stopWorkers(); });
  const bool  cleanupSignaled = waitUntil([] { return gCleanupSignals == 1; });
  {
    std::lock_guard<std::mutex> lock(gMutex);
    EXPECT_FALSE(gReleaseFirstResponse);
    gReleaseFirstResponse = true;
  }
  gCv.notify_all();
  stopThread.join();

  ASSERT_TRUE(cleanupSignaled);

  std::lock_guard<std::mutex> lock(gMutex);
  ASSERT_EQ(static_cast<size_t>(kItemCount), gItems.size());
  EXPECT_EQ(1, gCleanupSignals);
  EXPECT_FALSE(gResponseBarrierTimedOut);
  EXPECT_EQ(0, gHandlerCalls);
  for (const auto& item : gItems) {
    EXPECT_EQ(1, item.responses);
    EXPECT_EQ(TSDB_CODE_APP_IS_STOPPING, item.responseCode);
    EXPECT_EQ(1, item.frees);
  }
}

TEST_F(MgmtMnodeWorkerTest, StreamReaderQueueRejectsMissingNestedPolicy) {
  SStreamTriggerReaderCalcInfo calcInfo = {};
  calcInfo.requiresContextPolicy = true;
  gReaderCalcInfos = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(gReaderCalcInfos, nullptr);
  SStreamTriggerReaderCalcInfo* pCalcInfo = &calcInfo;
  ASSERT_NE(taosArrayPush(gReaderCalcInfos, &pCalcInfo), nullptr);

  SStreamRuntimeFuncInfo runtime = {};
  runtime.groupId = 101;
  runtime.sessionId = 33;
  runtime.curIdx = 0;
  SResFetchReq request = {};
  request.queryId = 11;
  request.taskId = 22;
  request.execId = 0;
  request.reset = true;
  request.pStRtFuncInfo = &runtime;
  const int32_t size = tSerializeSResFetchReq(nullptr, 0, &request, false, false);
  ASSERT_GT(size, 0);

  SRpcMsg* msg = nullptr;
  ASSERT_EQ(taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, size, reinterpret_cast<void**>(&msg)), TSDB_CODE_SUCCESS);
  ASSERT_NE(msg, nullptr);
  msg->msgType = TDMT_STREAM_FETCH;
  msg->contLen = size;
  msg->pCont = rpcMallocCont(size);
  ASSERT_NE(msg->pCont, nullptr);
  ASSERT_EQ(tSerializeSResFetchReq(msg->pCont, size, &request, false, false), size);
  msg->info.handle = reinterpret_cast<void*>(static_cast<uintptr_t>(0x3001));
  {
    std::lock_guard<std::mutex> lock(gMutex);
    gItems.push_back({msg->info.handle, msg->pCont, TSDB_CODE_SUCCESS, 0, 0});
  }

  ASSERT_EQ(mmPutMsgToStreamReaderQueue(&mgmt_, msg), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(waitUntil([] {
    ObservedItem* item = findByHandle(reinterpret_cast<void*>(static_cast<uintptr_t>(0x3001)));
    return item != nullptr && item->responses == 1 && item->frees == 1;
  }));

  std::lock_guard<std::mutex> lock(gMutex);
  const ObservedItem*         item = findByHandle(reinterpret_cast<void*>(static_cast<uintptr_t>(0x3001)));
  ASSERT_NE(item, nullptr);
  EXPECT_EQ(item->responseCode, TSDB_CODE_INVALID_PARA);
}

TEST_F(MgmtMnodeWorkerTest, VnodeFetchMatchesMnodeAdmissionBeforeReaderSideEffects) {
  STableScanPhysiNode scan = {};
  scan.scan.node.type = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
  SSubplan calcAst = {};
  calcAst.pNode = &scan.scan.node;

  SStreamTriggerReaderCalcInfo calcInfo = {};
  calcInfo.requiresContextPolicy = true;
  calcInfo.calcAst = &calcAst;
  calcInfo.rtInfo.execId = 73;
  calcInfo.rtInfo.funcInfo.groupId = -701;
  calcInfo.rtInfo.funcInfo.sessionId = -702;
  calcInfo.rtInfo.funcInfo.triggerType = -703;
  calcInfo.rtInfo.funcInfo.streamGen = 704;
  const SStreamRuntimeFuncInfo initialFuncInfo = calcInfo.rtInfo.funcInfo;

  gReaderCalcInfos = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(gReaderCalcInfos, nullptr);
  SStreamTriggerReaderCalcInfo* pCalcInfo = &calcInfo;
  ASSERT_NE(taosArrayPush(gReaderCalcInfos, &pCalcInfo), nullptr);

  SStreamRuntimeFuncInfo runtime = {};
  runtime.groupId = 101;
  runtime.sessionId = 33;
  runtime.curIdx = 0;
  SResFetchReq request = {};
  request.queryId = 11;
  request.taskId = 22;
  request.execId = 0;
  request.reset = true;
  request.pStRtFuncInfo = &runtime;

  const int32_t size = tSerializeSResFetchReq(nullptr, 0, &request, false, false);
  ASSERT_GT(size, 0);
  std::vector<uint8_t> wire(size);
  ASSERT_EQ(tSerializeSResFetchReq(wire.data(), size, &request, false, false), size);

  void* const mnodeHandle = reinterpret_cast<void*>(static_cast<uintptr_t>(0x4001));
  SRpcMsg*    mnodeMsg = nullptr;
  ASSERT_EQ(taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, size, reinterpret_cast<void**>(&mnodeMsg)),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(mnodeMsg, nullptr);
  mnodeMsg->msgType = TDMT_STREAM_FETCH;
  mnodeMsg->contLen = size;
  mnodeMsg->pCont = rpcMallocCont(size);
  ASSERT_NE(mnodeMsg->pCont, nullptr);
  std::memcpy(mnodeMsg->pCont, wire.data(), wire.size());
  mnodeMsg->info.handle = mnodeHandle;
  {
    std::lock_guard<std::mutex> lock(gMutex);
    gItems.push_back({mnodeHandle, mnodeMsg->pCont, TSDB_CODE_SUCCESS, 0, 0});
  }

  ASSERT_EQ(mmPutMsgToStreamReaderQueue(&mgmt_, mnodeMsg), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(waitUntil([mnodeHandle] {
    ObservedItem* item = findByHandle(mnodeHandle);
    return item != nullptr && item->responses == 1 && item->frees == 1;
  }));

  void* const vnodeHandle = reinterpret_cast<void*>(static_cast<uintptr_t>(0x4002));
  SRpcMsg     vnodeMsg = {};
  vnodeMsg.msgType = TDMT_STREAM_FETCH;
  vnodeMsg.contLen = size;
  vnodeMsg.pCont = rpcMallocCont(size);
  ASSERT_NE(vnodeMsg.pCont, nullptr);
  std::memcpy(vnodeMsg.pCont, wire.data(), wire.size());
  vnodeMsg.info.handle = vnodeHandle;
  {
    std::lock_guard<std::mutex> lock(gMutex);
    gItems.push_back({vnodeHandle, vnodeMsg.pCont, TSDB_CODE_SUCCESS, 0, 0});
  }

  SVnode        vnode = {};
  SQueueInfo    queueInfo = {};
  const int32_t vnodeCode = vnodeProcessStreamReaderMsg(&vnode, &vnodeMsg, &queueInfo);
  rpcFreeCont(vnodeMsg.pCont);
  vnodeMsg.pCont = nullptr;

  std::lock_guard<std::mutex> lock(gMutex);
  const ObservedItem*         mnodeItem = findByHandle(mnodeHandle);
  const ObservedItem*         vnodeItem = findByHandle(vnodeHandle);
  ASSERT_NE(mnodeItem, nullptr);
  ASSERT_NE(vnodeItem, nullptr);
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, mnodeItem->responseCode);
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, vnodeCode);
  EXPECT_EQ(mnodeItem->responseCode, vnodeItem->responseCode);
  EXPECT_EQ(73, calcInfo.rtInfo.execId);
  EXPECT_EQ(0, std::memcmp(&initialFuncInfo, &calcInfo.rtInfo.funcInfo, sizeof(initialFuncInfo)));
  EXPECT_EQ(0, gReaderSideEffects.initStorage);
  EXPECT_EQ(0, gReaderSideEffects.destroyTask);
  EXPECT_EQ(0, gReaderSideEffects.createTask);
  EXPECT_EQ(0, gReaderSideEffects.resetScan);
  EXPECT_EQ(0, gReaderSideEffects.setTaskId);
  EXPECT_EQ(0, gReaderSideEffects.updateOperator);
  EXPECT_EQ(0, gReaderSideEffects.setStreamGen);
  EXPECT_EQ(0, gReaderSideEffects.setScalarExtra);
  EXPECT_EQ(0, gReaderSideEffects.executeTask);
}

}  // namespace

extern "C" {

SDmNotifyHandle dmNotifyHdl = {};

void __real_rpcFreeCont(void* pCont);

int32_t __wrap_rpcSendResponse(const SRpcMsg* pMsg) {
  std::unique_lock<std::mutex> lock(gMutex);
  ObservedItem*                item = findByHandle(pMsg->info.handle);
  if (item == nullptr) return TSDB_CODE_SUCCESS;
  ++item->responses;
  item->responseCode = pMsg->code;
  if (gBlockFirstResponse && !gFirstResponseEntered) {
    gFirstResponseEntered = true;
    gCv.notify_all();
    if (!gCv.wait_for(lock, kWaitTimeout, [] { return gReleaseFirstResponse; })) {
      gResponseBarrierTimedOut = true;
    }
  }
  gCv.notify_all();
  void* response = pMsg->pCont;
  lock.unlock();
  __real_rpcFreeCont(response);
  return TSDB_CODE_SUCCESS;
}

void __wrap_rpcFreeCont(void* pCont) {
  {
    std::lock_guard<std::mutex> lock(gMutex);
    ObservedItem*               item = findByPayload(pCont);
    if (item != nullptr) {
      ++item->frees;
      gCv.notify_all();
    }
  }
  __real_rpcFreeCont(pCont);
}

void __real_tSingleWorkerCleanup(SSingleWorker* pWorker);

void __wrap_tSingleWorkerCleanup(SSingleWorker* pWorker) {
  {
    std::lock_guard<std::mutex> lock(gMutex);
    if (gMgmt != nullptr && pWorker == &gMgmt->writeWorker) {
      ++gCleanupSignals;
      gCv.notify_all();
    }
  }
  __real_tSingleWorkerCleanup(pWorker);
}

void* __wrap_qStreamGetReaderInfo(int64_t, int64_t, void** taskAddr) {
  *taskAddr = nullptr;
  return gReaderCalcInfos;
}

bool __wrap_syncIsReadyForRead(int64_t) { return true; }

void __wrap_initStorageAPI(SStorageAPI*) {
  std::lock_guard<std::mutex> lock(gMutex);
  ++gReaderSideEffects.initStorage;
}

void __wrap_qDestroyTask(qTaskInfo_t) {
  std::lock_guard<std::mutex> lock(gMutex);
  ++gReaderSideEffects.destroyTask;
}

int32_t __wrap_qCreateStreamExecTaskInfo(qTaskInfo_t* pTaskInfo, void*, SReadHandle*, SStreamInserterParam*, int32_t,
                                         int32_t) {
  std::lock_guard<std::mutex> lock(gMutex);
  ++gReaderSideEffects.createTask;
  *pTaskInfo = reinterpret_cast<qTaskInfo_t>(static_cast<uintptr_t>(0x5001));
  return TSDB_CODE_SUCCESS;
}

int32_t __wrap_qResetTableScan(qTaskInfo_t, SReadHandle*) {
  std::lock_guard<std::mutex> lock(gMutex);
  ++gReaderSideEffects.resetScan;
  return TSDB_CODE_SUCCESS;
}

int32_t __wrap_qSetTaskId(qTaskInfo_t, uint64_t, uint64_t) {
  std::lock_guard<std::mutex> lock(gMutex);
  ++gReaderSideEffects.setTaskId;
  return TSDB_CODE_SUCCESS;
}

void __wrap_qUpdateOperatorParam(qTaskInfo_t, void*) {
  std::lock_guard<std::mutex> lock(gMutex);
  ++gReaderSideEffects.updateOperator;
}

void __wrap_qSetStreamGen(qTaskInfo_t, uint64_t) {
  std::lock_guard<std::mutex> lock(gMutex);
  ++gReaderSideEffects.setStreamGen;
}

void __wrap_setTaskScalarExtraInfo(qTaskInfo_t) {
  std::lock_guard<std::mutex> lock(gMutex);
  ++gReaderSideEffects.setScalarExtra;
}

int32_t __wrap_qExecTaskOpt(qTaskInfo_t, SArray*, uint64_t*, bool* hasMore, SLocalFetch*, bool) {
  std::lock_guard<std::mutex> lock(gMutex);
  ++gReaderSideEffects.executeTask;
  *hasMore = false;
  return TSDB_CODE_SUCCESS;
}
}
