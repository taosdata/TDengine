#include <gtest/gtest.h>

#include "dataSinkMgt.h"
#include "stream.h"
#include "streamInt.h"
#include "streamRunner.h"
#include "stub.h"

namespace {

struct RunnerCallState {
  int32_t resetCalls = 0;
  int32_t executeCalls = 0;
  int32_t forceOutputCalls = 0;
  int32_t sinkCalls = 0;
  int32_t notifyCalls = 0;
  bool    sinkBlockIsNull = false;
  bool    autoCreateTable = false;
  int64_t groupId = 0;
  char    tableName[TSDB_TABLE_NAME_LEN] = {0};
};

RunnerCallState gCalls;

int32_t mockStreamClearStatesForOperators(qTaskInfo_t) {
  ++gCalls.resetCalls;
  return TSDB_CODE_SUCCESS;
}

int32_t mockStreamExecuteTask(qTaskInfo_t, SSDataBlock **, bool *) {
  ++gCalls.executeCalls;
  return TSDB_CODE_INVALID_PARA;
}

int32_t mockStreamForceOutput(qTaskInfo_t, SSDataBlock **, int32_t) {
  ++gCalls.forceOutputCalls;
  return TSDB_CODE_INVALID_PARA;
}

int32_t mockDsPutDataBlock(DataSinkHandle, const SInputData *pInput, bool *pContinue) {
  ++gCalls.sinkCalls;
  gCalls.sinkBlockIsNull = pInput->pData == nullptr;
  gCalls.autoCreateTable = pInput->pStreamDataInserterInfo->isAutoCreateTable;
  gCalls.groupId = pInput->pStreamDataInserterInfo->groupId;
  tstrncpy(gCalls.tableName, pInput->pStreamDataInserterInfo->tbName, sizeof(gCalls.tableName));
  *pContinue = false;
  return TSDB_CODE_SUCCESS;
}

int32_t mockStreamSendNotifyContent(SStreamTask *, const char *, const char *, int32_t, int64_t, const SArray *,
                                    int32_t, const SSTriggerCalcParam *, int32_t) {
  ++gCalls.notifyCalls;
  return TSDB_CODE_SUCCESS;
}

void destroyRuntimeInfoInList(SList *pList) {
  SListNode *pNode = tdListGetHead(pList);
  while (pNode != nullptr) {
    auto *pExec = reinterpret_cast<SStreamRunnerTaskExecution *>(pNode->data);
    tDestroyStRtFuncInfo(&pExec->runtimeInfo.funcInfo);
    pNode = pNode->dl_next_;
  }
}

class StreamRunnerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gCalls = {};
    stub_.set(streamClearStatesForOperators, mockStreamClearStatesForOperators);
    stub_.set(streamExecuteTask, mockStreamExecuteTask);
    stub_.set(streamForceOutput, mockStreamForceOutput);
    stub_.set(dsPutDataBlock, mockDsPutDataBlock);
    stub_.set(streamSendNotifyContent, mockStreamSendNotifyContent);

    ASSERT_EQ(taosThreadMutexInit(&task_.execMgr.lock, nullptr), TSDB_CODE_SUCCESS);
    task_.execMgr.lockInited = true;
    task_.execMgr.pFreeExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
    task_.execMgr.pRunningExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
    ASSERT_NE(task_.execMgr.pFreeExecs, nullptr);
    ASSERT_NE(task_.execMgr.pRunningExecs, nullptr);

    auto *pExec = static_cast<SStreamRunnerTaskExecution *>(tdListReserve(task_.execMgr.pFreeExecs));
    ASSERT_NE(pExec, nullptr);
    pExec->pExecutor = &executorSentinel_;
    pExec->pSinkHandle = &sinkSentinel_;
    pExec->runtimeInfo.execId = 7;
    tstrncpy(pExec->tbname, "out_zero_window", sizeof(pExec->tbname));
    pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
    ASSERT_NE(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, nullptr);
    SSTriggerCalcParam staleWindow = {.wstart = 1000, .wend = 2000};
    ASSERT_NE(taosArrayPush(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, &staleWindow), nullptr);

    task_.topTask = true;
    task_.output.outTblType = TSDB_NORMAL_TABLE;
    task_.task.streamId = 0x1234;
    task_.notification.pNotifyAddrUrls = taosArrayInit(1, sizeof(char *));
    ASSERT_NE(task_.notification.pNotifyAddrUrls, nullptr);
    char *notifyUrl = notifyUrl_;
    ASSERT_NE(taosArrayPush(task_.notification.pNotifyAddrUrls, &notifyUrl), nullptr);

    request_.brandNew = true;
    request_.execId = -1;
    request_.createTable = 1;
    request_.gid = 42;
    request_.sessionId = 1;
  }

  void TearDown() override {
    tDestroySTriggerCalcRequest(&request_);
    if (task_.execMgr.pRunningExecs != nullptr) {
      destroyRuntimeInfoInList(task_.execMgr.pRunningExecs);
      task_.execMgr.pRunningExecs = static_cast<SList *>(tdListFree(task_.execMgr.pRunningExecs));
    }
    if (task_.execMgr.pFreeExecs != nullptr) {
      destroyRuntimeInfoInList(task_.execMgr.pFreeExecs);
      task_.execMgr.pFreeExecs = static_cast<SList *>(tdListFree(task_.execMgr.pFreeExecs));
    }
    if (task_.execMgr.lockInited) {
      EXPECT_EQ(taosThreadMutexDestroy(&task_.execMgr.lock), TSDB_CODE_SUCCESS);
    }
    taosArrayDestroy(task_.notification.pNotifyAddrUrls);
  }

  Stub                 stub_;
  char                 executorSentinel_ = 0;
  char                 sinkSentinel_ = 0;
  char                 notifyUrl_[32] = "ws://unused/zero-window";
  SStreamRunnerTask    task_ = {};
  SSTriggerCalcRequest request_ = {};
};

TEST_F(StreamRunnerTest, BrandNewZeroWindowRequestOnReusedExecCreatesTableWithoutExecutingOrNotifying) {
  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_), TSDB_CODE_SUCCESS);

  EXPECT_EQ(gCalls.resetCalls, 1);
  EXPECT_EQ(gCalls.executeCalls, 0);
  EXPECT_EQ(gCalls.forceOutputCalls, 0);
  EXPECT_EQ(gCalls.sinkCalls, 1);
  EXPECT_TRUE(gCalls.sinkBlockIsNull);
  EXPECT_TRUE(gCalls.autoCreateTable);
  EXPECT_EQ(gCalls.groupId, 42);
  EXPECT_STREQ(gCalls.tableName, "out_zero_window");
  EXPECT_EQ(gCalls.notifyCalls, 0);

  ASSERT_EQ(listNEles(task_.execMgr.pFreeExecs), 1);
  ASSERT_EQ(listNEles(task_.execMgr.pRunningExecs), 0);
  auto *pExec = reinterpret_cast<SStreamRunnerTaskExecution *>(tdListGetHead(task_.execMgr.pFreeExecs)->data);
  EXPECT_EQ(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, nullptr);
  ASSERT_NE(request_.params, nullptr);
  EXPECT_EQ(taosArrayGetSize(request_.params), 1);
}

}  // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
