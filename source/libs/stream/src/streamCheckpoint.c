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
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "cos.h"
#include "rsync.h"
#include "streamBackendRocksdb.h"
#include "streamInt.h"

static int32_t downloadCheckpointDataByName(const char* id, const char* fname, const char* dstName);
static int32_t deleteCheckpointFile(const char* id, const char* name);
static int32_t streamTaskUploadCheckpoint(const char* id, const char* path);
static int32_t deleteCheckpoint(const char* id);
static int32_t downloadCheckpointByNameS3(const char* id, const char* fname, const char* dstName);
static int32_t continueDispatchCheckpointTriggerBlock(SStreamDataBlock* pBlock, SStreamTask* pTask);
static int32_t appendCheckpointIntoInputQ(SStreamTask* pTask, int32_t checkpointType, int64_t checkpointId,
                                          int32_t transId, int32_t srcTaskId);
static int32_t doSendRetrieveTriggerMsg(SStreamTask* pTask, SArray* pNotSendList);
static void    checkpointTriggerMonitorFn(void* param, void* tmrId);

SStreamDataBlock* createChkptTriggerBlock(SStreamTask* pTask, int32_t checkpointType, int64_t checkpointId,
                                          int32_t transId, int32_t srcTaskId) {
  SStreamDataBlock* pChkpoint = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, sizeof(SSDataBlock));
  if (pChkpoint == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pChkpoint->type = checkpointType;
  if (checkpointType == STREAM_INPUT__CHECKPOINT_TRIGGER && (pTask->info.taskLevel != TASK_LEVEL__SOURCE)) {
    pChkpoint->srcTaskId = srcTaskId;
    ASSERT(srcTaskId != 0);
  }

  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    taosFreeQitem(pChkpoint);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pBlock->info.type = STREAM_CHECKPOINT;
  pBlock->info.version = checkpointId;
  pBlock->info.window.ekey = pBlock->info.window.skey = transId;  // NOTE: set the transId
  pBlock->info.rows = 1;
  pBlock->info.childId = pTask->info.selfChildId;

  pChkpoint->blocks = taosArrayInit(4, sizeof(SSDataBlock));  // pBlock;
  taosArrayPush(pChkpoint->blocks, pBlock);

  taosMemoryFree(pBlock);
  terrno = 0;

  return pChkpoint;
}

int32_t appendCheckpointIntoInputQ(SStreamTask* pTask, int32_t checkpointType, int64_t checkpointId, int32_t transId,
                                   int32_t srcTaskId) {
  SStreamDataBlock* pCheckpoint = createChkptTriggerBlock(pTask, checkpointType, checkpointId, transId, srcTaskId);

  if (streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pCheckpoint) < 0) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  streamTrySchedExec(pTask);
  return TSDB_CODE_SUCCESS;
}

int32_t streamProcessCheckpointSourceReq(SStreamTask* pTask, SStreamCheckpointSourceReq* pReq) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE);

  // todo this status may not be set here.
  // 1. set task status to be prepared for check point, no data are allowed to put into inputQ.
  int32_t code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_GEN_CHECKPOINT);
  ASSERT(code == TSDB_CODE_SUCCESS);

  pTask->chkInfo.pActiveInfo->transId = pReq->transId;
  pTask->chkInfo.pActiveInfo->activeId = pReq->checkpointId;
  pTask->chkInfo.startTs = taosGetTimestampMs();
  pTask->execInfo.checkpoint += 1;

  // 2. Put the checkpoint block into inputQ, to make sure all blocks with less version have been handled by this task
  // and this is the last item in the inputQ.
  return appendCheckpointIntoInputQ(pTask, STREAM_INPUT__CHECKPOINT_TRIGGER, pReq->checkpointId, pReq->transId, -1);
}

int32_t streamTaskProcessCheckpointTriggerRsp(SStreamTask* pTask, SCheckpointTriggerRsp* pRsp) {
  ASSERT(pTask->info.taskLevel != TASK_LEVEL__SOURCE);

  if (pRsp->rspCode != TSDB_CODE_SUCCESS) {
    stDebug("s-task:%s retrieve checkpoint-trgger rsp from upstream:0x%x invalid, code:%s", pTask->id.idStr,
            pRsp->upstreamTaskId, tstrerror(pRsp->rspCode));
    return TSDB_CODE_SUCCESS;
  }

  appendCheckpointIntoInputQ(pTask, STREAM_INPUT__CHECKPOINT_TRIGGER, pRsp->checkpointId, pRsp->transId,
                             pRsp->upstreamTaskId);
  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskSendCheckpointTriggerMsg(SStreamTask* pTask, int32_t dstTaskId, int32_t downstreamNodeId,
                                           SRpcHandleInfo* pRpcInfo, int32_t code) {
  int32_t size = sizeof(SMsgHead) + sizeof(SCheckpointTriggerRsp);

  void*                  pBuf = rpcMallocCont(size);
  SCheckpointTriggerRsp* pRsp = POINTER_SHIFT(pBuf, sizeof(SMsgHead));

  ((SMsgHead*)pBuf)->vgId = htonl(downstreamNodeId);

  pRsp->streamId = pTask->id.streamId;
  pRsp->upstreamTaskId = pTask->id.taskId;
  pRsp->taskId = dstTaskId;
  pRsp->rspCode = code;

  if (code == TSDB_CODE_SUCCESS) {
    pRsp->checkpointId = pTask->chkInfo.pActiveInfo->activeId;
    pRsp->transId = pTask->chkInfo.pActiveInfo->transId;
  } else {
    pRsp->checkpointId = -1;
    pRsp->transId = -1;
  }

  SRpcMsg rspMsg = {.code = 0, .pCont = pBuf, .contLen = size, .info = *pRpcInfo};
  tmsgSendRsp(&rspMsg);
  return 0;
}

int32_t continueDispatchCheckpointTriggerBlock(SStreamDataBlock* pBlock, SStreamTask* pTask) {
  pBlock->srcTaskId = pTask->id.taskId;
  pBlock->srcVgId = pTask->pMeta->vgId;

  int32_t code = taosWriteQitem(pTask->outputq.queue->pQueue, pBlock);
  if (code == 0) {
    ASSERT(pTask->chkInfo.pActiveInfo->dispatchTrigger == false);
    streamDispatchStreamBlock(pTask);
  } else {
    stError("s-task:%s failed to put checkpoint into outputQ, code:%s", pTask->id.idStr, tstrerror(code));
    streamFreeQitem((SStreamQueueItem*)pBlock);
  }

  return code;
}

int32_t streamProcessCheckpointTriggerBlock(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  SSDataBlock* pDataBlock = taosArrayGet(pBlock->blocks, 0);
  int64_t      checkpointId = pDataBlock->info.version;
  int32_t      transId = pDataBlock->info.window.skey;
  const char*  id = pTask->id.idStr;
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      vgId = pTask->pMeta->vgId;
  int32_t      taskLevel = pTask->info.taskLevel;

  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;

  taosThreadMutexLock(&pTask->lock);
  if (pTask->chkInfo.checkpointId > checkpointId) {
    stError("s-task:%s vgId:%d current checkpointId:%" PRId64
            " recv expired checkpoint-trigger block, checkpointId:%" PRId64 " transId:%d, discard",
            id, vgId, pTask->chkInfo.checkpointId, checkpointId, transId);
    taosThreadMutexUnlock(&pTask->lock);

    streamFreeQitem((SStreamQueueItem*)pBlock);
    return TSDB_CODE_SUCCESS;
  }

  if (pTask->chkInfo.checkpointId == checkpointId) {
    {  // send checkpoint-ready msg to upstream
      SRpcMsg msg = {0};

      SStreamUpstreamEpInfo* pInfo = streamTaskGetUpstreamTaskEpInfo(pTask, pBlock->srcTaskId);
      initCheckpointReadyMsg(pTask, pInfo->nodeId, pBlock->srcTaskId, pInfo->childId, checkpointId, &msg);
      tmsgSendReq(&pInfo->epSet, &msg);
    }

    stWarn(
        "s-task:%s vgId:%d recv already finished checkpoint msg, send checkpoint-ready to upstream:0x%x to resume the "
        "interrupted checkpoint",
        id, vgId, pBlock->srcTaskId);

    streamTaskOpenUpstreamInput(pTask, pBlock->srcTaskId);
    taosThreadMutexUnlock(&pTask->lock);

    streamFreeQitem((SStreamQueueItem*)pBlock);
    return TSDB_CODE_SUCCESS;
  }

  if (streamTaskGetStatus(pTask)->state == TASK_STATUS__CK) {
    if (pActiveInfo->activeId != checkpointId) {
      stError("s-task:%s vgId:%d active checkpointId:%" PRId64 ", recv invalid checkpoint-trigger checkpointId:%" PRId64
              " discard",
              id, vgId, pActiveInfo->activeId, checkpointId);
      taosThreadMutexUnlock(&pTask->lock);

      streamFreeQitem((SStreamQueueItem*)pBlock);
      return TSDB_CODE_SUCCESS;
    } else {  // checkpointId == pActiveInfo->activeId
      if (pActiveInfo->allUpstreamTriggerRecv == 1) {
        stDebug(
            "s-task:%s vgId:%d all upstream checkpoint-trigger recv, discard this checkpoint-trigger, "
            "checkpointId:%" PRId64 " transId:%d",
            id, vgId, checkpointId, transId);
        taosThreadMutexUnlock(&pTask->lock);
        streamFreeQitem((SStreamQueueItem*)pBlock);
        return TSDB_CODE_SUCCESS;
      }

      if (taskLevel == TASK_LEVEL__SINK || taskLevel == TASK_LEVEL__AGG) {
        //  check if already recv or not, and duplicated checkpoint-trigger msg recv, discard it
        for (int32_t i = 0; i < taosArrayGetSize(pActiveInfo->pReadyMsgList); ++i) {
          STaskCheckpointReadyInfo* p = taosArrayGet(pActiveInfo->pReadyMsgList, i);
          if (p->upstreamTaskId == pBlock->srcTaskId) {
            ASSERT(p->checkpointId == checkpointId);
            stWarn("s-task:%s repeatly recv checkpoint-source msg from task:0x%x vgId:%d, checkpointId:%" PRId64
                   ", prev recvTs:%" PRId64 " discard",
                   pTask->id.idStr, p->upstreamTaskId, p->upstreamNodeId, p->checkpointId, p->recvTs);

            taosThreadMutexUnlock(&pTask->lock);
            streamFreeQitem((SStreamQueueItem*)pBlock);
            return TSDB_CODE_SUCCESS;
          }
        }
      }
    }
  }

  taosThreadMutexUnlock(&pTask->lock);

  stDebug("s-task:%s vgId:%d start to handle the checkpoint-trigger block, checkpointId:%" PRId64 " ver:%" PRId64
          ", transId:%d current active checkpointId:%" PRId64,
          id, vgId, pTask->chkInfo.checkpointId, pTask->chkInfo.checkpointVer, transId, checkpointId);

  // set task status
  if (streamTaskGetStatus(pTask)->state != TASK_STATUS__CK) {
    pActiveInfo->activeId = checkpointId;
    pActiveInfo->transId = transId;

    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_GEN_CHECKPOINT);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s handle checkpoint-trigger block failed, code:%s", id, tstrerror(code));
      streamFreeQitem((SStreamQueueItem*)pBlock);
      return code;
    }

    // if previous launched timer not started yet, not start a new timer
    // todo: fix this bug: previous set checkpoint-trigger check tmr is running, while we happen to try to launch
    //  a new checkpoint-trigger timer right now.
    //  And if we don't start a new timer, and the lost of checkpoint-trigger message may cause the whole checkpoint
    //  procedure to be stucked.
    SStreamTmrInfo* pTmrInfo = &pActiveInfo->chkptTriggerMsgTmr;
    int8_t old = atomic_val_compare_exchange_8(&pTmrInfo->isActive, 0, 1);
    if (old == 0) {
      int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
      stDebug("s-task:%s start checkpoint-trigger monitor in 10s, ref:%d ", pTask->id.idStr, ref);
      streamMetaAcquireOneTask(pTask);

      if (pTmrInfo->tmrHandle == NULL) {
        pTmrInfo->tmrHandle = taosTmrStart(checkpointTriggerMonitorFn, 200, pTask, streamTimer);
      } else {
        taosTmrReset(checkpointTriggerMonitorFn, 200, pTask, streamTimer, &pTmrInfo->tmrHandle);
      }
      pTmrInfo->launchChkptId = pActiveInfo->activeId;
    } else { // already launched, do nothing
      stError("s-task:%s previous checkpoint-trigger monitor tmr is set, not start new one", pTask->id.idStr);
    }
  }

  if (taskLevel == TASK_LEVEL__SOURCE) {
    int8_t type = pTask->outputInfo.type;
    pActiveInfo->allUpstreamTriggerRecv = 1;

    // We need to transfer state here, before dispatching checkpoint-trigger to downstream tasks.
    // The transfer of state may generate new data that need to dispatch to downstream tasks,
    // Otherwise, those new generated data by executors that is kept in outputQ, may be lost if this program crashed
    // before the next checkpoint.
    flushStateDataInExecutor(pTask, (SStreamQueueItem*)pBlock);

    if (type == TASK_OUTPUT__FIXED_DISPATCH || type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      stDebug("s-task:%s set childIdx:%d, and add checkpoint-trigger block into outputQ", id, pTask->info.selfChildId);
      continueDispatchCheckpointTriggerBlock(pBlock, pTask);
    } else {  // only one task exists, no need to dispatch downstream info
      appendCheckpointIntoInputQ(pTask, STREAM_INPUT__CHECKPOINT, pActiveInfo->activeId, pActiveInfo->transId, -1);
      streamFreeQitem((SStreamQueueItem*)pBlock);
    }
  } else if (taskLevel == TASK_LEVEL__SINK || taskLevel == TASK_LEVEL__AGG) {
    ASSERT(taosArrayGetSize(pTask->upstreamInfo.pList) > 0);
    if (pTask->chkInfo.startTs == 0) {
      pTask->chkInfo.startTs = taosGetTimestampMs();
      pTask->execInfo.checkpoint += 1;
    }

    // update the child Id for downstream tasks
    streamAddCheckpointReadyMsg(pTask, pBlock->srcTaskId, pTask->info.selfChildId, checkpointId);

    // there are still some upstream tasks not send checkpoint request, do nothing and wait for then
    if (pActiveInfo->allUpstreamTriggerRecv != 1) {
      streamFreeQitem((SStreamQueueItem*)pBlock);
      return code;
    }

    int32_t num = streamTaskGetNumOfUpstream(pTask);
    if (taskLevel == TASK_LEVEL__SINK) {
      stDebug("s-task:%s process checkpoint-trigger block, all %d upstreams sent, send ready msg to upstream", id, num);
      streamFreeQitem((SStreamQueueItem*)pBlock);
      streamTaskBuildCheckpoint(pTask);
    } else {  // source & agg tasks need to forward the checkpoint msg downwards
      stDebug("s-task:%s process checkpoint-trigger block, all %d upstreams sent, forwards to downstream", id, num);

      flushStateDataInExecutor(pTask, (SStreamQueueItem*)pBlock);

      // Put the checkpoint-trigger block into outputQ, to make sure all blocks with less version have been handled by
      // this task already. And then, dispatch check point msg to all downstream tasks
      code = continueDispatchCheckpointTriggerBlock(pBlock, pTask);
    }
  }

  return code;
}

/**
 * All down stream tasks have successfully completed the check point task.
 * Current stream task is allowed to start to do checkpoint things in ASYNC model.
 */
int32_t streamProcessCheckpointReadyMsg(SStreamTask* pTask, int64_t checkpointId, int32_t downstreamNodeId,
                                        int32_t downstreamTaskId) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE || pTask->info.taskLevel == TASK_LEVEL__AGG);
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;

  const char* id = pTask->id.idStr;
  bool        received = false;
  int32_t     total = streamTaskGetNumOfDownstream(pTask);
  ASSERT(total > 0);

  // 1. not in checkpoint status now
  SStreamTaskState* pStat = streamTaskGetStatus(pTask);
  if (pStat->state != TASK_STATUS__CK) {
    stError("s-task:%s status:%s discard checkpoint-ready msg from task:0x%x", id, pStat->name, downstreamTaskId);
    return TSDB_CODE_STREAM_TASK_IVLD_STATUS;
  }

  // 2. expired checkpoint-ready msg, invalid checkpoint-ready msg
  if (pTask->chkInfo.checkpointId > checkpointId || pInfo->activeId != checkpointId) {
    stError("s-task:%s status:%s checkpointId:%" PRId64 " new arrival checkpoint-ready msg (checkpointId:%" PRId64
            ") from task:0x%x, expired and discard ",
            id, pStat->name, pTask->chkInfo.checkpointId, checkpointId, downstreamTaskId);
    return -1;
  }

  taosThreadMutexLock(&pInfo->lock);

  // only when all downstream tasks are send checkpoint rsp, we can start the checkpoint procedure for the agg task
  int32_t size = taosArrayGetSize(pInfo->pCheckpointReadyRecvList);
  for (int32_t i = 0; i < size; ++i) {
    STaskDownstreamReadyInfo* p = taosArrayGet(pInfo->pCheckpointReadyRecvList, i);
    if (p->downstreamTaskId == downstreamTaskId) {
      received = true;
      break;
    }
  }

  if (received) {
    stDebug("s-task:%s already recv checkpoint-ready msg from downstream:0x%x, ignore. %d/%d downstream not ready", id,
            downstreamTaskId, (int32_t)(total - taosArrayGetSize(pInfo->pCheckpointReadyRecvList)), total);
  } else {
    STaskDownstreamReadyInfo info = {.recvTs = taosGetTimestampMs(),
                                     .downstreamTaskId = downstreamTaskId,
                                     .checkpointId = pInfo->activeId,
                                     .transId = pInfo->transId,
                                     .streamId = pTask->id.streamId,
                                     .downstreamNodeId = downstreamNodeId};
    taosArrayPush(pInfo->pCheckpointReadyRecvList, &info);
  }

  int32_t notReady = total - taosArrayGetSize(pInfo->pCheckpointReadyRecvList);
  int32_t transId = pInfo->transId;
  taosThreadMutexUnlock(&pInfo->lock);

  if (notReady == 0) {
    stDebug("s-task:%s all downstream tasks have completed build checkpoint, do checkpoint for current task", id);
    appendCheckpointIntoInputQ(pTask, STREAM_INPUT__CHECKPOINT, checkpointId, transId, -1);
  }

  return 0;
}

int32_t streamTaskProcessCheckpointReadyRsp(SStreamTask* pTask, int32_t upstreamTaskId, int64_t checkpointId) {
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;
  int64_t                now = taosGetTimestampMs();
  int32_t                numOfConfirmed = 0;

  taosThreadMutexLock(&pInfo->lock);
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pReadyMsgList); ++i) {
    STaskCheckpointReadyInfo* pReadyInfo = taosArrayGet(pInfo->pReadyMsgList, i);
    if (pReadyInfo->upstreamTaskId == upstreamTaskId && pReadyInfo->checkpointId == checkpointId) {
      pReadyInfo->sendCompleted = 1;
      stDebug("s-task:%s send checkpoint-ready msg to upstream:0x%x confirmed, checkpointId:%" PRId64 " ts:%" PRId64,
              pTask->id.idStr, upstreamTaskId, checkpointId, now);
      break;
    }
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pReadyMsgList); ++i) {
    STaskCheckpointReadyInfo* pReadyInfo = taosArrayGet(pInfo->pReadyMsgList, i);
    if (pReadyInfo->sendCompleted == 1) {
      numOfConfirmed += 1;
    }
  }

  stDebug("s-task:%s send checkpoint-ready msg to %d upstream confirmed, checkpointId:%" PRId64, pTask->id.idStr,
          numOfConfirmed, checkpointId);

  taosThreadMutexUnlock(&pInfo->lock);
  return TSDB_CODE_SUCCESS;
}

void streamTaskClearCheckInfo(SStreamTask* pTask, bool clearChkpReadyMsg) {
  pTask->chkInfo.startTs = 0;  // clear the recorded start time

  streamTaskClearActiveInfo(pTask->chkInfo.pActiveInfo);
  streamTaskOpenAllUpstreamInput(pTask);  // open inputQ for all upstream tasks
  if (clearChkpReadyMsg) {
    streamClearChkptReadyMsg(pTask->chkInfo.pActiveInfo);
  }
}

int32_t streamTaskUpdateTaskCheckpointInfo(SStreamTask* pTask, bool restored, SVUpdateCheckpointInfoReq* pReq) {
  SStreamMeta*     pMeta = pTask->pMeta;
  int32_t          vgId = pMeta->vgId;
  int32_t          code = 0;
  const char*      id = pTask->id.idStr;
  SCheckpointInfo* pInfo = &pTask->chkInfo;

  taosThreadMutexLock(&pTask->lock);

  if (pReq->checkpointId <= pInfo->checkpointId) {
    stDebug("s-task:%s vgId:%d latest checkpointId:%" PRId64 " checkpointVer:%" PRId64
            " no need to update the checkpoint info, updated checkpointId:%" PRId64 " checkpointVer:%" PRId64
            " transId:%d ignored",
            id, vgId, pInfo->checkpointId, pInfo->checkpointVer, pReq->checkpointId, pReq->checkpointVer,
            pReq->transId);
    taosThreadMutexUnlock(&pTask->lock);

    {  // destroy the related fill-history tasks
      // drop task should not in the meta-lock, and drop the related fill-history task now
      streamMetaWUnLock(pMeta);
      if (pReq->dropRelHTask) {
        streamMetaUnregisterTask(pMeta, pReq->hStreamId, pReq->hTaskId);
        int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
        stDebug("s-task:%s vgId:%d related fill-history task:0x%x dropped in update checkpointInfo, remain tasks:%d",
                id, vgId, pReq->taskId, numOfTasks);
      }
      streamMetaWLock(pMeta);
      if (streamMetaCommit(pMeta) < 0) {
        // persist to disk
      }
    }

    return TSDB_CODE_SUCCESS;
  }

  SStreamTaskState* pStatus = streamTaskGetStatus(pTask);

//  if (restored && (pStatus->state != TASK_STATUS__CK)) {
//    stDebug("s-task:0x%x vgId:%d restored:%d status:%s not update checkpoint-info, checkpointId:%" PRId64 "->%" PRId64
//            " failed",
//            pReq->taskId, vgId, restored, pStatus->name, pInfo->checkpointId, pReq->checkpointId);
//    taosThreadMutexUnlock(&pTask->lock);
//    return TSDB_CODE_STREAM_TASK_IVLD_STATUS;
//  }

  if (!restored) {  // during restore procedure, do update checkpoint-info
    stDebug("s-task:%s vgId:%d status:%s update the checkpoint-info during restore, checkpointId:%" PRId64 "->%" PRId64
            " checkpointVer:%" PRId64 "->%" PRId64 " checkpointTs:%" PRId64 "->%" PRId64,
            id, vgId, pStatus->name, pInfo->checkpointId, pReq->checkpointId, pInfo->checkpointVer, pReq->checkpointVer,
            pInfo->checkpointTime, pReq->checkpointTs);
  } else {  // not in restore status, must be in checkpoint status
    stDebug("s-task:%s vgId:%d status:%s start to update the checkpoint-info, checkpointId:%" PRId64 "->%" PRId64
            " checkpointVer:%" PRId64 "->%" PRId64 " checkpointTs:%" PRId64 "->%" PRId64,
            id, vgId, pStatus->name, pInfo->checkpointId, pReq->checkpointId, pInfo->checkpointVer, pReq->checkpointVer,
            pInfo->checkpointTime, pReq->checkpointTs);
  }

  ASSERT(pInfo->checkpointId <= pReq->checkpointId && pInfo->checkpointVer <= pReq->checkpointVer &&
         pInfo->processedVer <= pReq->checkpointVer);

  pInfo->checkpointId = pReq->checkpointId;
  pInfo->checkpointVer = pReq->checkpointVer;
  pInfo->checkpointTime = pReq->checkpointTs;

  streamTaskClearCheckInfo(pTask, true);

  if (pStatus->state == TASK_STATUS__CK) {
    // todo handle error
    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_CHECKPOINT_DONE);
  } else {
    stDebug("s-task:0x%x vgId:%d not handle checkpoint-done event, status:%s", pReq->taskId, vgId, pStatus->name);
  }

  if (pReq->dropRelHTask) {
    stDebug("s-task:0x%x vgId:%d drop the related fill-history task:0x%" PRIx64 " after update checkpoint",
            pReq->taskId, vgId, pReq->hTaskId);
    CLEAR_RELATED_FILLHISTORY_TASK(pTask);
  }

  stDebug("s-task:0x%x set the persistent status attr to be ready, prev:%s, status in sm:%s", pReq->taskId,
          streamTaskGetStatusStr(pTask->status.taskStatus), streamTaskGetStatus(pTask)->name);

  pTask->status.taskStatus = TASK_STATUS__READY;

  code = streamMetaSaveTask(pMeta, pTask);
  if (code != TSDB_CODE_SUCCESS) {
    stError("s-task:%s vgId:%d failed to save task info after do checkpoint, checkpointId:%" PRId64 ", since %s", id,
            vgId, pReq->checkpointId, terrstr());
    return code;
  }

  taosThreadMutexUnlock(&pTask->lock);
  streamMetaWUnLock(pMeta);

  // drop task should not in the meta-lock, and drop the related fill-history task now
  if (pReq->dropRelHTask) {
    streamMetaUnregisterTask(pMeta, pReq->hStreamId, pReq->hTaskId);
    int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
    stDebug("s-task:%s vgId:%d related fill-history task:0x%x dropped, remain tasks:%d", id, vgId,
            (int32_t)pReq->hTaskId, numOfTasks);
  }

  streamMetaWLock(pMeta);

  if (streamMetaCommit(pMeta) < 0) {
    // persist to disk
  }

  return TSDB_CODE_SUCCESS;
}

void streamTaskSetFailedCheckpointId(SStreamTask* pTask) {
  pTask->chkInfo.pActiveInfo->failedId = pTask->chkInfo.pActiveInfo->activeId;
  stDebug("s-task:%s mark the checkpointId:%" PRId64 " (transId:%d) failed", pTask->id.idStr,
          pTask->chkInfo.pActiveInfo->activeId, pTask->chkInfo.pActiveInfo->transId);
}

void streamTaskSetCheckpointFailed(SStreamTask* pTask) {
  taosThreadMutexLock(&pTask->lock);
  ETaskStatus status = streamTaskGetStatus(pTask)->state;
  if (status == TASK_STATUS__CK) {
    streamTaskSetFailedCheckpointId(pTask);
  }
  taosThreadMutexUnlock(&pTask->lock);
}

static int32_t getCheckpointDataMeta(const char* id, const char* path, SArray* list) {
  char buf[128] = {0};

  char* file = taosMemoryCalloc(1, strlen(path) + 32);
  sprintf(file, "%s%s%s", path, TD_DIRSEP, "META_TMP");

  int32_t code = downloadCheckpointDataByName(id, "META", file);
  if (code != 0) {
    stDebug("%s chkp failed to download meta file:%s", id, file);
    taosMemoryFree(file);
    return code;
  }

  TdFilePtr pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    stError("%s failed to open meta file:%s for checkpoint", id, file);
    code = -1;
    return code;
  }

  if (taosReadFile(pFile, buf, sizeof(buf)) <= 0) {
    stError("%s failed to read meta file:%s for checkpoint", id, file);
    code = -1;
  } else {
    int32_t len = strnlen(buf, tListLen(buf));
    for (int i = 0; i < len; i++) {
      if (buf[i] == '\n') {
        char* item = taosMemoryCalloc(1, i + 1);
        memcpy(item, buf, i);
        taosArrayPush(list, &item);

        item = taosMemoryCalloc(1, len - i);
        memcpy(item, buf + i + 1, len - i - 1);
        taosArrayPush(list, &item);
      }
    }
  }

  taosCloseFile(&pFile);
  taosRemoveFile(file);
  taosMemoryFree(file);
  return code;
}

int32_t uploadCheckpointData(SStreamTask* pTask, int64_t checkpointId, int64_t dbRefId, ECHECKPOINT_BACKUP_TYPE type) {
  char*        path = NULL;
  int32_t      code = 0;
  SArray*      toDelFiles = taosArrayInit(4, POINTER_BYTES);
  int64_t      now = taosGetTimestampMs();
  SStreamMeta* pMeta = pTask->pMeta;
  const char*  idStr = pTask->id.idStr;

  if ((code = taskDbGenChkpUploadData(pTask->pBackend, pMeta->bkdChkptMgt, checkpointId, type, &path, toDelFiles,
                                      pTask->id.idStr)) != 0) {
    stError("s-task:%s failed to gen upload checkpoint:%" PRId64, idStr, checkpointId);
  }

  if (type == DATA_UPLOAD_S3) {
    if (code == TSDB_CODE_SUCCESS && (code = getCheckpointDataMeta(idStr, path, toDelFiles)) != 0) {
      stError("s-task:%s failed to get checkpointData for checkpointId:%" PRId64 " meta", idStr, checkpointId);
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = streamTaskUploadCheckpoint(idStr, path);
    if (code == TSDB_CODE_SUCCESS) {
      stDebug("s-task:%s upload checkpointId:%" PRId64 " to remote succ", idStr, checkpointId);
    } else {
      stError("s-task:%s failed to upload checkpointId:%" PRId64 " data:%s", idStr, checkpointId, path);
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    int32_t size = taosArrayGetSize(toDelFiles);
    stDebug("s-task:%s remove redundant %d files", idStr, size);

    for (int i = 0; i < size; i++) {
      char* pName = taosArrayGetP(toDelFiles, i);
      code = deleteCheckpointFile(idStr, pName);
      if (code != 0) {
        stDebug("s-task:%s failed to remove file: %s", idStr, pName);
        break;
      }
    }

    stDebug("s-task:%s remove redundant files in uploading checkpointId:%" PRId64 " data", idStr, checkpointId);
  }

  taosArrayDestroyP(toDelFiles, taosMemoryFree);
  double el = (taosGetTimestampMs() - now) / 1000.0;

  if (code == TSDB_CODE_SUCCESS) {
    stDebug("s-task:%s complete update checkpointId:%" PRId64 ", elapsed time:%.2fs remove local checkpoint data %s",
            idStr, checkpointId, el, path);
    taosRemoveDir(path);
  } else {
    stDebug("s-task:%s failed to upload checkpointId:%" PRId64 " keep local checkpoint data, elapsed time:%.2fs", idStr,
            checkpointId, el);
  }

  taosMemoryFree(path);
  return code;
}

int32_t streamTaskRemoteBackupCheckpoint(SStreamTask* pTask, int64_t checkpointId) {
  ECHECKPOINT_BACKUP_TYPE type = streamGetCheckpointBackupType();
  if (type == DATA_UPLOAD_DISABLE) {
    stDebug("s-task:%s not allowed to upload checkpoint data", pTask->id.idStr);
    return 0;
  }

  if (pTask == NULL || pTask->pBackend == NULL) {
    return 0;
  }

  int64_t dbRefId = taskGetDBRef(pTask->pBackend);
  void*   pBackend = taskAcquireDb(dbRefId);
  if (pBackend == NULL) {
    stError("s-task:%s failed to acquire db during update checkpoint data, failed to upload checkpointData",
            pTask->id.idStr);
    return -1;
  }

  int32_t code = uploadCheckpointData(pTask, checkpointId, taskGetDBRef(pTask->pBackend), type);
  taskReleaseDb(dbRefId);

  return code;
}

int32_t streamTaskBuildCheckpoint(SStreamTask* pTask) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int64_t      startTs = pTask->chkInfo.startTs;
  int64_t      ckId = pTask->chkInfo.pActiveInfo->activeId;
  const char*  id = pTask->id.idStr;
  bool         dropRelHTask = (streamTaskGetPrevStatus(pTask) == TASK_STATUS__HALT);
  SStreamMeta* pMeta = pTask->pMeta;

  // sink task does not need to save the status, and generated the checkpoint
  if (pTask->info.taskLevel != TASK_LEVEL__SINK) {
    stDebug("s-task:%s level:%d start gen checkpoint, checkpointId:%" PRId64, id, pTask->info.taskLevel, ckId);

    code = streamBackendDoCheckpoint(pTask->pBackend, ckId);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s gen checkpoint:%" PRId64 " failed, code:%s", id, ckId, tstrerror(terrno));
    }
  }

  // TODO: monitoring the checkpoint-source msg
  // send check point response to upstream task
  if (code == TSDB_CODE_SUCCESS) {
    if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
      code = streamTaskSendCheckpointSourceRsp(pTask);
    } else {
      code = streamTaskSendCheckpointReadyMsg(pTask);
    }

    if (code != TSDB_CODE_SUCCESS) {
      // todo: let's retry send rsp to mnode, checkpoint-ready has monitor now
      stError("s-task:%s failed to send checkpoint rsp to upstream, checkpointId:%" PRId64 ", code:%s", id, ckId,
              tstrerror(code));
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = streamTaskRemoteBackupCheckpoint(pTask, ckId);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s upload checkpointId:%" PRId64 " data failed, code:%s", id, ckId, tstrerror(code));
    }
  } else {
    stError("s-task:%s taskInfo failed, checkpoint:%" PRId64 " failed, code:%s", id, ckId, tstrerror(code));
  }

  // TODO: monitoring the checkpoint-report msg
  // update the latest checkpoint info if all works are done successfully, for rsma, the pMsgCb is null.
  if (code == TSDB_CODE_SUCCESS) {
    if (pTask->pMsgCb != NULL) {
      code = streamSendChkptReportMsg(pTask, &pTask->chkInfo, dropRelHTask);
    }
  } else {  // clear the checkpoint info if failed
    taosThreadMutexLock(&pTask->lock);
    streamTaskSetFailedCheckpointId(pTask);  // set failed checkpoint id before clear the checkpoint info
    taosThreadMutexUnlock(&pTask->lock);

    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_CHECKPOINT_DONE);
    stDebug("s-task:%s clear checkpoint flag since gen checkpoint failed, checkpointId:%" PRId64, id, ckId);
  }

  double el = (taosGetTimestampMs() - startTs) / 1000.0;
  stInfo("s-task:%s vgId:%d level:%d, checkpointId:%" PRId64 " ver:%" PRId64 " elapsed time:%.2fs, %s ", id,
         pMeta->vgId, pTask->info.taskLevel, ckId, pTask->chkInfo.checkpointVer, el,
         (code == TSDB_CODE_SUCCESS) ? "succ" : "failed");

  return code;
}

void checkpointTriggerMonitorFn(void* param, void* tmrId) {
  SStreamTask* pTask = param;
  int32_t      vgId = pTask->pMeta->vgId;
  int64_t      now = taosGetTimestampMs();
  const char*  id = pTask->id.idStr;

  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;
  SStreamTmrInfo* pTmrInfo = &pActiveInfo->chkptTriggerMsgTmr;

  // check the status every 100ms
  if (streamTaskShouldStop(pTask)) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stDebug("s-task:%s vgId:%d quit from monitor checkpoint-trigger, ref:%d", id, vgId, ref);
    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  if (++pTmrInfo->activeCounter < 50) {
    taosTmrReset(checkpointTriggerMonitorFn, 200, pTask, streamTimer, &pTmrInfo->tmrHandle);
    return;
  }

  pTmrInfo->activeCounter = 0;
  stDebug("s-task:%s vgId:%d checkpoint-trigger monitor in tmr, ts:%" PRId64, id, vgId, now);

  taosThreadMutexLock(&pTask->lock);
  SStreamTaskState* pState = streamTaskGetStatus(pTask);
  if (pState->state != TASK_STATUS__CK) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stDebug("s-task:%s vgId:%d not in checkpoint status, quit from monitor checkpoint-trigger, ref:%d", id, vgId, ref);

    taosThreadMutexUnlock(&pTask->lock);
    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  // checkpoint-trigger recv flag is set, quit
  if (pActiveInfo->allUpstreamTriggerRecv) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stDebug("s-task:%s vgId:%d all checkpoint-trigger recv, quit from monitor checkpoint-trigger, ref:%d", id, vgId,
            ref);

    taosThreadMutexUnlock(&pTask->lock);
    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }
  taosThreadMutexUnlock(&pTask->lock);

  taosThreadMutexLock(&pActiveInfo->lock);

  // send msg to retrieve checkpoint trigger msg
  SArray* pList = pTask->upstreamInfo.pList;
  ASSERT(pTask->info.taskLevel > TASK_LEVEL__SOURCE);

  SArray* pNotSendList = taosArrayInit(4, sizeof(SStreamUpstreamEpInfo));
  if (pNotSendList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    stError("s-task:%s quit tmr function due to out of memory", id);
    taosThreadMutexUnlock(&pActiveInfo->lock);

    stDebug("s-task:%s start to monitor checkpoint-trigger in 10s", id);
    taosTmrReset(checkpointTriggerMonitorFn, 200, pTask, streamTimer, &pTmrInfo->tmrHandle);
    return;
  }

  if ((pTmrInfo->launchChkptId != pActiveInfo->activeId) || (pActiveInfo->activeId == 0)) {
    taosThreadMutexUnlock(&pActiveInfo->lock);
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stWarn("s-task:%s vgId:%d checkpoint-trigger retrieve by previous checkpoint procedure, checkpointId:%" PRId64
               ", quit, ref:%d",
           id, vgId, pTmrInfo->launchChkptId, ref);

    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  // active checkpoint info is cleared for now
  if ((pActiveInfo->activeId == 0) || (pActiveInfo->transId == 0) || (pTask->chkInfo.startTs == 0)) {
    taosThreadMutexUnlock(&pActiveInfo->lock);
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stWarn("s-task:%s vgId:%d active checkpoint may be cleared, quit from retrieve checkpoint-trigger send tmr, ref:%d",
           id, vgId, ref);

    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pList); ++i) {
    SStreamUpstreamEpInfo* pInfo = taosArrayGetP(pList, i);

    bool recved = false;
    for (int32_t j = 0; j < taosArrayGetSize(pActiveInfo->pReadyMsgList); ++j) {
      STaskCheckpointReadyInfo* pReady = taosArrayGet(pActiveInfo->pReadyMsgList, j);
      if (pInfo->nodeId == pReady->upstreamNodeId) {
        recved = true;
        break;
      }
    }

    if (!recved) {  // make sure the inputQ is opened for not recv upstream checkpoint-trigger message
      streamTaskOpenUpstreamInput(pTask, pInfo->taskId);
      taosArrayPush(pNotSendList, pInfo);
    }
  }

  // do send retrieve checkpoint trigger msg to upstream
  int32_t size = taosArrayGetSize(pNotSendList);
  doSendRetrieveTriggerMsg(pTask, pNotSendList);
  taosThreadMutexUnlock(&pActiveInfo->lock);

  // check every 100ms
  if (size > 0) {
    stDebug("s-task:%s start to monitor checkpoint-trigger in 10s", id);
    taosTmrReset(checkpointTriggerMonitorFn, 200, pTask, streamTimer, &pTmrInfo->tmrHandle);
  } else {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stDebug("s-task:%s all checkpoint-trigger recved, quit from monitor checkpoint-trigger tmr, ref:%d", id, ref);
    streamMetaReleaseTask(pTask->pMeta, pTask);
  }

  taosArrayDestroy(pNotSendList);
}

int32_t doSendRetrieveTriggerMsg(SStreamTask* pTask, SArray* pNotSendList) {
  int32_t     code = 0;
  int32_t     vgId = pTask->pMeta->vgId;
  const char* pId = pTask->id.idStr;
  int32_t     size = taosArrayGetSize(pNotSendList);
  int32_t     numOfUpstream = streamTaskGetNumOfUpstream(pTask);
  int64_t     checkpointId = pTask->chkInfo.pActiveInfo->activeId;

  if (size <= 0) {
    stDebug("s-task:%s all upstream checkpoint trigger recved, no need to send retrieve", pId);
    return code;
  }

  stDebug("s-task:%s %d/%d not recv checkpoint-trigger from upstream(s), start to send trigger-retrieve", pId, size,
          numOfUpstream);

  for (int32_t i = 0; i < size; i++) {
    SStreamUpstreamEpInfo* pUpstreamTask = taosArrayGet(pNotSendList, i);

    SRetrieveChkptTriggerReq* pReq = rpcMallocCont(sizeof(SRetrieveChkptTriggerReq));
    if (pReq == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      stError("vgId:%d failed to create msg to retrieve trigger msg for task:%s exec, code:out of memory", vgId, pId);
      continue;
    }

    pReq->head.vgId = htonl(pUpstreamTask->nodeId);
    pReq->streamId = pTask->id.streamId;
    pReq->downstreamTaskId = pTask->id.taskId;
    pReq->downstreamNodeId = vgId;
    pReq->upstreamTaskId = pUpstreamTask->taskId;
    pReq->upstreamNodeId = pUpstreamTask->nodeId;
    pReq->checkpointId = checkpointId;

    SRpcMsg rpcMsg = {0};
    initRpcMsg(&rpcMsg, TDMT_STREAM_RETRIEVE_TRIGGER, pReq, sizeof(SRetrieveChkptTriggerReq));

    code = tmsgSendReq(&pUpstreamTask->epSet, &rpcMsg);
    stDebug("s-task:%s vgId:%d send checkpoint-trigger retrieve msg to 0x%x(vgId:%d) checkpointId:%" PRId64, pId, vgId,
            pUpstreamTask->taskId, pUpstreamTask->nodeId, checkpointId);
  }

  return TSDB_CODE_SUCCESS;
}

bool streamTaskAlreadySendTrigger(SStreamTask* pTask, int32_t downstreamNodeId) {
  int64_t                now = taosGetTimestampMs();
  const char*            id = pTask->id.idStr;
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;
  SStreamTaskState*      pStatus = streamTaskGetStatus(pTask);

  if (pStatus->state != TASK_STATUS__CK) {
    return false;
  }

  taosThreadMutexLock(&pInfo->lock);
  if (!pInfo->dispatchTrigger) {
    taosThreadMutexUnlock(&pInfo->lock);
    return false;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pDispatchTriggerList); ++i) {
    STaskTriggerSendInfo* pSendInfo = taosArrayGet(pInfo->pDispatchTriggerList, i);
    if (pSendInfo->nodeId != downstreamNodeId) {
      continue;
    }

    // has send trigger msg to downstream node,
    double before = (now - pSendInfo->sendTs) / 1000.0;
    if (pSendInfo->recved) {
      stWarn("s-task:%s checkpoint-trigger msg already send at:%" PRId64
             "(%.2fs before) and recv confirmed by downstream:0x%x, checkpointId:%" PRId64 ", transId:%d",
             id, pSendInfo->sendTs, before, pSendInfo->taskId, pInfo->activeId, pInfo->transId);
    } else {
      stWarn("s-task:%s checkpoint-trigger already send at:%" PRId64 "(%.2fs before), checkpointId:%" PRId64
             ", transId:%d",
             id, pSendInfo->sendTs, before, pInfo->activeId, pInfo->transId);
    }

    taosThreadMutexUnlock(&pInfo->lock);
    return true;
  }

  ASSERT(0);
  return false;
}

void streamTaskGetTriggerRecvStatus(SStreamTask* pTask, int32_t* pRecved, int32_t* pTotal) {
  *pRecved = taosArrayGetSize(pTask->chkInfo.pActiveInfo->pReadyMsgList);

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    *pTotal = 1;
  } else {
    *pTotal = streamTaskGetNumOfUpstream(pTask);
  }
}

// record the dispatch checkpoint trigger info in the list
void streamTaskInitTriggerDispatchInfo(SStreamTask* pTask) {
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;

  int64_t now = taosGetTimestampMs();
  taosThreadMutexLock(&pInfo->lock);

  pInfo->dispatchTrigger = true;
  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    STaskDispatcherFixed* pDispatch = &pTask->outputInfo.fixedDispatcher;

    STaskTriggerSendInfo p = {.sendTs = now, .recved = false, .nodeId = pDispatch->nodeId, .taskId = pDispatch->taskId};
    taosArrayPush(pInfo->pDispatchTriggerList, &p);
  } else {
    for (int32_t i = 0; i < streamTaskGetNumOfDownstream(pTask); ++i) {
      SVgroupInfo* pVgInfo = taosArrayGet(pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos, i);

      STaskTriggerSendInfo p = {.sendTs = now, .recved = false, .nodeId = pVgInfo->vgId, .taskId = pVgInfo->taskId};
      taosArrayPush(pInfo->pDispatchTriggerList, &p);
    }
  }

  taosThreadMutexUnlock(&pInfo->lock);
}

int32_t streamTaskGetNumOfConfirmed(SActiveCheckpointInfo* pInfo) {
  int32_t num = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pDispatchTriggerList); ++i) {
    STaskTriggerSendInfo* p = taosArrayGet(pInfo->pDispatchTriggerList, i);
    if (p->recved) {
      num++;
    }
  }
  return num;
}

void streamTaskSetTriggerDispatchConfirmed(SStreamTask* pTask, int32_t vgId) {
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;

  int32_t taskId = 0;
  taosThreadMutexLock(&pInfo->lock);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pDispatchTriggerList); ++i) {
    STaskTriggerSendInfo* p = taosArrayGet(pInfo->pDispatchTriggerList, i);
    if (p->nodeId == vgId) {
      ASSERT(p->recved == false);

      p->recved = true;
      p->recvTs = taosGetTimestampMs();
      taskId = p->taskId;
      break;
    }
  }

  int32_t numOfConfirmed = streamTaskGetNumOfConfirmed(pInfo);
  taosThreadMutexUnlock(&pInfo->lock);

  int32_t total = streamTaskGetNumOfDownstream(pTask);
  stDebug("s-task:%s set downstream:0x%x(vgId:%d) checkpoint-trigger dispatch confirmed, total confirmed:%d/%d",
          pTask->id.idStr, taskId, vgId, numOfConfirmed, total);

  ASSERT(taskId != 0);
}

static int32_t uploadCheckpointToS3(const char* id, const char* path) {
  TdDirPtr pDir = taosOpenDir(path);
  if (pDir == NULL) return -1;

  TdDirEntryPtr de = NULL;
  s3Init();
  while ((de = taosReadDir(pDir)) != NULL) {
    char* name = taosGetDirEntryName(de);
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0 || taosDirEntryIsDir(de)) continue;

    char filename[PATH_MAX] = {0};
    if (path[strlen(path) - 1] == TD_DIRSEP_CHAR) {
      snprintf(filename, sizeof(filename), "%s%s", path, name);
    } else {
      snprintf(filename, sizeof(filename), "%s%s%s", path, TD_DIRSEP, name);
    }

    char object[PATH_MAX] = {0};
    snprintf(object, sizeof(object), "%s%s%s", id, TD_DIRSEP, name);

    if (s3PutObjectFromFile2(filename, object, 0) != 0) {
      taosCloseDir(&pDir);
      return -1;
    }
    stDebug("[s3] upload checkpoint:%s", filename);
    // break;
  }

  taosCloseDir(&pDir);
  return 0;
}

int32_t downloadCheckpointByNameS3(const char* id, const char* fname, const char* dstName) {
  int32_t code = 0;
  char*   buf = taosMemoryCalloc(1, strlen(id) + strlen(dstName) + 4);
  if (buf == NULL) {
    code = terrno = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  sprintf(buf, "%s/%s", id, fname);
  if (s3GetObjectToFile(buf, dstName) != 0) {
    code = errno;
  }

  taosMemoryFree(buf);
  return code;
}

ECHECKPOINT_BACKUP_TYPE streamGetCheckpointBackupType() {
  if (strlen(tsSnodeAddress) != 0) {
    return DATA_UPLOAD_RSYNC;
  } else if (tsS3StreamEnabled) {
    return DATA_UPLOAD_S3;
  } else {
    return DATA_UPLOAD_DISABLE;
  }
}

int32_t streamTaskUploadCheckpoint(const char* id, const char* path) {
  if (id == NULL || path == NULL || strlen(id) == 0 || strlen(path) == 0 || strlen(path) >= PATH_MAX) {
    stError("invalid parameters in upload checkpoint, %s", id);
    return -1;
  }

  if (strlen(tsSnodeAddress) != 0) {
    return uploadByRsync(id, path);
  } else if (tsS3StreamEnabled) {
    return uploadCheckpointToS3(id, path);
  }

  return 0;
}

// fileName:  CURRENT
int32_t downloadCheckpointDataByName(const char* id, const char* fname, const char* dstName) {
  if (id == NULL || fname == NULL || strlen(id) == 0 || strlen(fname) == 0 || strlen(fname) >= PATH_MAX) {
    stError("down load checkpoint data parameters invalid");
    return -1;
  }

  if (strlen(tsSnodeAddress) != 0) {
    return 0;
  } else if (tsS3StreamEnabled) {
    return downloadCheckpointByNameS3(id, fname, dstName);
  }

  return 0;
}

int32_t streamTaskDownloadCheckpointData(const char* id, char* path) {
  if (id == NULL || path == NULL || strlen(id) == 0 || strlen(path) == 0 || strlen(path) >= PATH_MAX) {
    stError("down checkpoint data parameters invalid");
    return -1;
  }

  if (strlen(tsSnodeAddress) != 0) {
    return downloadRsync(id, path);
  } else if (tsS3StreamEnabled) {
    return s3GetObjectsByPrefix(id, path);
  }

  return 0;
}

int32_t deleteCheckpoint(const char* id) {
  if (id == NULL || strlen(id) == 0) {
    stError("deleteCheckpoint parameters invalid");
    return -1;
  }
  if (strlen(tsSnodeAddress) != 0) {
    return deleteRsync(id);
  } else if (tsS3StreamEnabled) {
    s3DeleteObjectsByPrefix(id);
  }
  return 0;
}

int32_t deleteCheckpointFile(const char* id, const char* name) {
  char object[128] = {0};
  snprintf(object, sizeof(object), "%s/%s", id, name);

  char* tmp = object;
  s3DeleteObjects((const char**)&tmp, 1);
  return 0;
}
