#include <gtest/gtest.h>
#include "tstream.h"
#include "streamInt.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"

void initTaskLock(SStreamTask* pTask) {
  TdThreadMutexAttr attr = {0};
  int32_t code = taosThreadMutexAttrInit(&attr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = taosThreadMutexAttrSetType(&attr, PTHREAD_MUTEX_RECURSIVE);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = taosThreadMutexInit(&pTask->lock, &attr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = taosThreadMutexAttrDestroy(&attr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
}

TEST(streamCheckpointTest, StreamTaskProcessCheckpointTriggerRsp) {
    SStreamTask* pTask = NULL;
    int64_t uid = 1111111111111111;
    SArray* array = taosArrayInit(4, POINTER_BYTES);
    int32_t code = tNewStreamTask(uid, TASK_LEVEL__SINK, NULL, false, 0, 0, array,
                                       false, 1, &pTask);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    initTaskLock(pTask);

    code = streamTaskCreateActiveChkptInfo(&pTask->chkInfo.pActiveInfo);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    pTask->chkInfo.pActiveInfo->activeId = 123111;
    pTask->chkInfo.pActiveInfo->transId = 4561111;

    streamTaskSetStatusReady(pTask);
    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_GEN_CHECKPOINT);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    SCheckpointTriggerRsp pRsp;
    memset(&pRsp, 0, sizeof(SCheckpointTriggerRsp));
    pRsp.rspCode = TSDB_CODE_SUCCESS;
    pRsp.checkpointId = 123;
    pRsp.transId = 456;
    pRsp.upstreamTaskId = 789;

    code = streamTaskProcessCheckpointTriggerRsp(pTask, &pRsp);
    ASSERT_NE(code, TSDB_CODE_SUCCESS);

    pRsp.rspCode = TSDB_CODE_FAILED;
    code = streamTaskProcessCheckpointTriggerRsp(pTask, &pRsp);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    tFreeStreamTask(pTask);
    taosArrayDestroy(array);
}

TEST(streamCheckpointTest, StreamTaskSetFailedCheckpointId) {
    SStreamTask* pTask = NULL;
    int64_t uid = 1111111111111111;
    SArray* array = taosArrayInit(4, POINTER_BYTES);
    int32_t code = tNewStreamTask(uid, TASK_LEVEL__SINK, NULL, false, 0, 0, array,
                                       false, 1, &pTask);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    initTaskLock(pTask);

    code = streamTaskCreateActiveChkptInfo(&pTask->chkInfo.pActiveInfo);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;
    pInfo->failedId = 0;

    int64_t failedCheckpointId = 123;

    streamTaskSetFailedCheckpointId(pTask, failedCheckpointId);
    ASSERT_EQ(pInfo->failedId, failedCheckpointId);

    streamTaskSetFailedCheckpointId(pTask, 0);
    ASSERT_EQ(pInfo->failedId, failedCheckpointId);

    streamTaskSetFailedCheckpointId(pTask, pInfo->failedId - 1);
    ASSERT_EQ(pInfo->failedId, failedCheckpointId);
    tFreeStreamTask(pTask);
    taosArrayDestroy(array);
}

TEST(UploadCheckpointDataTest, UploadSuccess) {
    streamMetaInit();
    SStreamTask* pTask = NULL;
    int64_t uid = 1111111111111111;
    SArray* array = taosArrayInit(4, POINTER_BYTES);
    int32_t code = tNewStreamTask(uid, TASK_LEVEL__SINK, NULL, false, 0, 0, array,
                                       false, 1, &pTask);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    initTaskLock(pTask);

    code = streamTaskCreateActiveChkptInfo(&pTask->chkInfo.pActiveInfo);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    int64_t checkpointId = 123;
    int64_t dbRefId = 1;
    ECHECKPOINT_BACKUP_TYPE type = DATA_UPLOAD_S3;

    STaskDbWrapper* pBackend = NULL;
    int64_t processVer = -1;
    const char *path = "/tmp/backend3/stream";
    code = streamMetaOpen((path), NULL, NULL, NULL, 0, 0, NULL, &pTask->pMeta);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    SStreamState *pState = streamStateOpen((char *)path, pTask, 0, 0);
    ASSERT(pState != NULL);

    pTask->pBackend = pState->pTdbState->pOwner->pBackend;

    code = taskDbDoCheckpoint(pTask->pBackend, checkpointId, 0);
    ASSERT(code == 0);

    int32_t result = uploadCheckpointData(pTask, checkpointId, dbRefId, type);

    EXPECT_EQ(result, TSDB_CODE_SUCCESS) << "uploadCheckpointData should return 0 on success";
    tFreeStreamTask(pTask);
    taosRemoveDir(path);
    streamStateClose(pState, true);
    taosArrayDestroy(array);
}

TEST(UploadCheckpointDataTest, UploadDisabled) {
    SStreamTask* pTask = NULL;
    int64_t uid = 2222222222222;
    SArray* array = taosArrayInit(4, POINTER_BYTES);
    int32_t code = tNewStreamTask(uid, TASK_LEVEL__SINK, NULL, false, 0, 0, array,
                                       false, 1, &pTask);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    initTaskLock(pTask);

    code = streamTaskCreateActiveChkptInfo(&pTask->chkInfo.pActiveInfo);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    int64_t checkpointId = 123;
    int64_t dbRefId = 1;

    STaskDbWrapper* pBackend = NULL;
    int64_t processVer = -1;
    const char *path = "/tmp/backend4/stream";
    code = streamMetaOpen((path), NULL, NULL, NULL, 0, 0, NULL, &pTask->pMeta);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    SStreamState *pState = streamStateOpen((char *)path, pTask, 0, 0);
    ASSERT(pState != NULL);

    pTask->pBackend = pState->pTdbState->pOwner->pBackend;

    code = taskDbDoCheckpoint(pTask->pBackend, checkpointId, 0);
    ASSERT(code == 0);

    ECHECKPOINT_BACKUP_TYPE type = DATA_UPLOAD_DISABLE;

    int32_t result = uploadCheckpointData(pTask, checkpointId, dbRefId, type);

    EXPECT_NE(result, TSDB_CODE_SUCCESS) << "uploadCheckpointData should return 0 when backup type is disabled";

    streamStateClose(pState, true);
    tFreeStreamTask(pTask);
    taosArrayDestroy(array);
}

TEST(StreamTaskAlreadySendTriggerTest, AlreadySendTrigger) {
    SStreamTask* pTask = NULL;
    int64_t uid = 2222222222222;
    SArray* array = taosArrayInit(4, POINTER_BYTES);
    int32_t code = tNewStreamTask(uid, TASK_LEVEL__SINK, NULL, false, 0, 0, array,
                                       false, 1, &pTask);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    initTaskLock(pTask);

    code = streamTaskCreateActiveChkptInfo(&pTask->chkInfo.pActiveInfo);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    pTask->chkInfo.pActiveInfo->activeId = 123111;
    pTask->chkInfo.pActiveInfo->transId = 4561111;

    streamTaskSetStatusReady(pTask);
    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_GEN_CHECKPOINT);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    int32_t downstreamNodeId = 1;
    int64_t sendingCheckpointId = 123;
    TSKEY ts = taosGetTimestampMs();

    STaskTriggerSendInfo triggerInfo;
    triggerInfo.sendTs = ts;
    triggerInfo.recved = false;
    triggerInfo.nodeId = downstreamNodeId;

    taosArrayPush(pTask->chkInfo.pActiveInfo->pDispatchTriggerList, &triggerInfo);

    pTask->chkInfo.pActiveInfo->dispatchTrigger = true;
    bool result = streamTaskAlreadySendTrigger(pTask, downstreamNodeId);

    EXPECT_TRUE(result) << "The trigger message should have been sent to the downstream node";

    tFreeStreamTask(pTask);
    taosArrayDestroy(array);
}

TEST(ChkptTriggerRecvMonitorHelperTest, chkptTriggerRecvMonitorHelper) {
    SStreamTask* pTask = NULL;
    int64_t uid = 2222222222222;
    SArray* array = taosArrayInit(4, POINTER_BYTES);
    int32_t code = tNewStreamTask(uid, TASK_LEVEL__SINK, NULL, false, 0, 0, array,
                                       false, 1, &pTask);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    initTaskLock(pTask);

    const char *path = "/tmp/backend5/stream";
    code = streamMetaOpen((path), NULL, NULL, NULL, 0, 0, NULL, &pTask->pMeta);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    code = streamTaskCreateActiveChkptInfo(&pTask->chkInfo.pActiveInfo);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    pTask->chkInfo.pActiveInfo->activeId = 123111;
    pTask->chkInfo.pActiveInfo->chkptTriggerMsgTmr.launchChkptId = pTask->chkInfo.pActiveInfo->activeId;
    pTask->chkInfo.pActiveInfo->transId = 4561111;
    pTask->chkInfo.startTs = 11111;

    streamTaskSetStatusReady(pTask);
    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_GEN_CHECKPOINT);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    int32_t downstreamNodeId = 1;
    int64_t sendingCheckpointId = 123;
    TSKEY ts = taosGetTimestampMs();

    STaskTriggerSendInfo triggerInfo;
    triggerInfo.sendTs = ts;
    triggerInfo.recved = false;
    triggerInfo.nodeId = downstreamNodeId;

    taosArrayPush(pTask->chkInfo.pActiveInfo->pDispatchTriggerList, &triggerInfo);

    pTask->chkInfo.pActiveInfo->dispatchTrigger = true;
    SArray* array1 = NULL;
    code = chkptTriggerRecvMonitorHelper(pTask, NULL, &array1);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS);

    pTask->pMeta->fatalInfo.code = TSDB_CODE_SUCCESS;
    streamSetFatalError(pTask->pMeta, code, __func__, __LINE__);

    pTask->pMeta->fatalInfo.code = TSDB_CODE_FAILED;
    streamSetFatalError(pTask->pMeta, code, __func__, __LINE__);
    tFreeStreamTask(pTask);
    taosArrayDestroy(array);
    taosArrayDestroy(array1);
}
