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

#include "rsync.h"
#include "streamBackendRocksdb.h"
#include "streamInt.h"
#include "tcs.h"

static int32_t downloadCheckpointDataByName(const char* id, const char* fname, const char* dstName);
static int32_t deleteCheckpointFile(const char* id, const char* name);
static int32_t streamTaskUploadCheckpoint(const char* id, const char* path, int64_t checkpointId);
static int32_t deleteCheckpoint(const char* id);
static int32_t downloadCheckpointByNameS3(const char* id, const char* fname, const char* dstName);
static int32_t continueDispatchCheckpointTriggerBlock(SStreamDataBlock* pBlock, SStreamTask* pTask);
static int32_t appendCheckpointIntoInputQ(SStreamTask* pTask, int32_t checkpointType, int64_t checkpointId,
                                          int32_t transId, int32_t srcTaskId);
static int32_t doSendRetrieveTriggerMsg(SStreamTask* pTask, SArray* pNotSendList);
static void    checkpointTriggerMonitorFn(void* param, void* tmrId);

int32_t createChkptTriggerBlock(SStreamTask* pTask, int32_t checkpointType, int64_t checkpointId, int32_t transId,
                                int32_t srcTaskId, SStreamDataBlock** pRes) {
  SStreamDataBlock* pChkpoint = NULL;
  int32_t code = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, sizeof(SSDataBlock), (void**)&pChkpoint);
  if (code) {
    return code;
  }

  pChkpoint->type = checkpointType;
  if (checkpointType == STREAM_INPUT__CHECKPOINT_TRIGGER && (pTask->info.taskLevel != TASK_LEVEL__SOURCE)) {
    pChkpoint->srcTaskId = srcTaskId;
    if (srcTaskId <= 0) {
      stDebug("s-task:%s invalid src task id:%d for creating checkpoint trigger block", pTask->id.idStr, srcTaskId);
      return TSDB_CODE_INVALID_PARA;
    }
  }

  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    taosFreeQitem(pChkpoint);
    return terrno;
  }

  pBlock->info.type = STREAM_CHECKPOINT;
  pBlock->info.version = checkpointId;
  pBlock->info.window.ekey = pBlock->info.window.skey = transId;  // NOTE: set the transId
  pBlock->info.rows = 1;
  pBlock->info.childId = pTask->info.selfChildId;

  pChkpoint->blocks = taosArrayInit(4, sizeof(SSDataBlock));  // pBlock;
  if (pChkpoint->blocks == NULL) {
    taosMemoryFree(pBlock);
    taosFreeQitem(pChkpoint);
    return terrno;
  }

  void* p = taosArrayPush(pChkpoint->blocks, pBlock);
  if (p == NULL) {
    taosArrayDestroy(pChkpoint->blocks);
    taosMemoryFree(pBlock);
    taosFreeQitem(pChkpoint);
    return terrno;
  }

  *pRes = pChkpoint;

  taosMemoryFree(pBlock);
  return TSDB_CODE_SUCCESS;
}

// this message must be put into inputq successfully, continue retrying until it succeeds
// todo must be success
int32_t appendCheckpointIntoInputQ(SStreamTask* pTask, int32_t checkpointType, int64_t checkpointId, int32_t transId,
                                   int32_t srcTaskId) {
  SStreamDataBlock* pCheckpoint = NULL;
  int32_t code = createChkptTriggerBlock(pTask, checkpointType, checkpointId, transId, srcTaskId, &pCheckpoint);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pCheckpoint) < 0) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return streamTrySchedExec(pTask);
}

int32_t streamProcessCheckpointSourceReq(SStreamTask* pTask, SStreamCheckpointSourceReq* pReq) {
  if (pTask->info.taskLevel != TASK_LEVEL__SOURCE) {
    return TSDB_CODE_INVALID_MSG;
  }

  // todo this status may not be set here.
  // 1. set task status to be prepared for check point, no data are allowed to put into inputQ.
  int32_t code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_GEN_CHECKPOINT);
  if (code != TSDB_CODE_SUCCESS) {
    stError("s-task:%s failed to handle gen-checkpoint event, failed to start checkpoint procedure", pTask->id.idStr);
    return code;
  }

  pTask->chkInfo.pActiveInfo->transId = pReq->transId;
  pTask->chkInfo.pActiveInfo->activeId = pReq->checkpointId;
  pTask->chkInfo.startTs = taosGetTimestampMs();
  pTask->execInfo.checkpoint += 1;

  // 2. Put the checkpoint block into inputQ, to make sure all blocks with less version have been handled by this task
  // and this is the last item in the inputQ.
  return appendCheckpointIntoInputQ(pTask, STREAM_INPUT__CHECKPOINT_TRIGGER, pReq->checkpointId, pReq->transId, -1);
}

int32_t streamTaskProcessCheckpointTriggerRsp(SStreamTask* pTask, SCheckpointTriggerRsp* pRsp) {
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;
  bool                   unQualified = false;
  const char*            id = pTask->id.idStr;

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    stError("s-task:%s invalid msg recv, checkpoint-trigger rsp not handled", id);
    return TSDB_CODE_INVALID_MSG;
  }

  if (pRsp->rspCode != TSDB_CODE_SUCCESS) {
    stDebug("s-task:%s retrieve checkpoint-trgger rsp from upstream:0x%x invalid, code:%s", id, pRsp->upstreamTaskId,
            tstrerror(pRsp->rspCode));
    return TSDB_CODE_SUCCESS;
  }

  streamMutexLock(&pTask->lock);
  SStreamTaskState status = streamTaskGetStatus(pTask);
  streamMutexUnlock(&pTask->lock);

  if (status.state != TASK_STATUS__CK) {
    stError("s-task:%s status:%s not in checkpoint status, discard the checkpoint-trigger msg", id, status.name);
    return TSDB_CODE_STREAM_TASK_IVLD_STATUS;
  }

  streamMutexLock(&pInfo->lock);
  unQualified = (pInfo->activeId != pRsp->checkpointId || pInfo->transId != pRsp->transId);
  streamMutexUnlock(&pInfo->lock);

  if (unQualified) {
    stError("s-task:%s status:%s not in checkpoint status, discard the checkpoint-trigger msg", id, status.name);
    return TSDB_CODE_STREAM_TASK_IVLD_STATUS;
  }

  // NOTE: here we do not do the duplicated checkpoint-trigger msg check, since it will be done by following functions.
  int32_t code = appendCheckpointIntoInputQ(pTask, STREAM_INPUT__CHECKPOINT_TRIGGER, pRsp->checkpointId, pRsp->transId,
                                            pRsp->upstreamTaskId);
  return code;
}

int32_t streamTaskSendCheckpointTriggerMsg(SStreamTask* pTask, int32_t dstTaskId, int32_t downstreamNodeId,
                                           SRpcHandleInfo* pRpcInfo, int32_t code) {
  int32_t size = sizeof(SMsgHead) + sizeof(SCheckpointTriggerRsp);
  void*   pBuf = rpcMallocCont(size);
  if (pBuf == NULL) {
    return terrno;
  }

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

  if (pTask->chkInfo.pActiveInfo->dispatchTrigger == true) {
    stError("s-task:%s already dispatch checkpoint-trigger, not dispatch again", pTask->id.idStr);
    return 0;
  }

  int32_t code = taosWriteQitem(pTask->outputq.queue->pQueue, pBlock);
  if (code == 0) {
    code = streamDispatchStreamBlock(pTask);
  } else {
    stError("s-task:%s failed to put checkpoint into outputQ, code:%s", pTask->id.idStr, tstrerror(code));
    streamFreeQitem((SStreamQueueItem*)pBlock);
  }

  return code;
}

static int32_t doCheckBeforeHandleChkptTrigger(SStreamTask* pTask, int64_t checkpointId, SStreamDataBlock* pBlock,
                                               int32_t transId) {
  int32_t     code = 0;
  int32_t     vgId = pTask->pMeta->vgId;
  int32_t     taskLevel = pTask->info.taskLevel;
  const char* id = pTask->id.idStr;

  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;
  if (pTask->chkInfo.checkpointId > checkpointId) {
    stError("s-task:%s vgId:%d current checkpointId:%" PRId64
            " recv expired checkpoint-trigger block, checkpointId:%" PRId64 " transId:%d, discard",
            id, vgId, pTask->chkInfo.checkpointId, checkpointId, transId);
    return code;
  }

  if (pActiveInfo->failedId >= checkpointId) {
    stError("s-task:%s vgId:%d checkpointId:%" PRId64 " transId:%d, has been marked failed, failedId:%" PRId64
            " discard the checkpoint-trigger block",
            id, vgId, checkpointId, transId, pActiveInfo->failedId);
    return code;
  }

  if (pTask->chkInfo.checkpointId == checkpointId) {
    {  // send checkpoint-ready msg to upstream
      SRpcMsg                msg = {0};
      SStreamUpstreamEpInfo* pInfo = NULL;
      streamTaskGetUpstreamTaskEpInfo(pTask, pBlock->srcTaskId, &pInfo);
      if (pInfo == NULL) {
        return TSDB_CODE_STREAM_TASK_NOT_EXIST;
      }

      code = initCheckpointReadyMsg(pTask, pInfo->nodeId, pBlock->srcTaskId, pInfo->childId, checkpointId, &msg);
      if (code == TSDB_CODE_SUCCESS) {
        code = tmsgSendReq(&pInfo->epSet, &msg);
        if (code) {
          stError("s-task:%s vgId:%d failed send chkpt-ready msg to upstream, code:%s", id, vgId, tstrerror(code));
        }
      }
    }

    stWarn(
        "s-task:%s vgId:%d recv already finished checkpoint-trigger, send checkpoint-ready to upstream:0x%x to resume "
        "the interrupted checkpoint",
        id, vgId, pBlock->srcTaskId);

    streamTaskOpenUpstreamInput(pTask, pBlock->srcTaskId);
    return code;
  }

  if (streamTaskGetStatus(pTask).state == TASK_STATUS__CK) {
    if (pActiveInfo->activeId != checkpointId) {
      stError("s-task:%s vgId:%d active checkpointId:%" PRId64 ", recv invalid checkpoint-trigger checkpointId:%" PRId64
              " discard",
              id, vgId, pActiveInfo->activeId, checkpointId);
      return code;
    } else {  // checkpointId == pActiveInfo->activeId
      if (pActiveInfo->allUpstreamTriggerRecv == 1) {
        stDebug(
            "s-task:%s vgId:%d all upstream checkpoint-trigger recv, discard this checkpoint-trigger, "
            "checkpointId:%" PRId64 " transId:%d",
            id, vgId, checkpointId, transId);
        return code;
      }

      if (taskLevel == TASK_LEVEL__SINK || taskLevel == TASK_LEVEL__AGG) {
        //  check if already recv or not, and duplicated checkpoint-trigger msg recv, discard it
        for (int32_t i = 0; i < taosArrayGetSize(pActiveInfo->pReadyMsgList); ++i) {
          STaskCheckpointReadyInfo* p = taosArrayGet(pActiveInfo->pReadyMsgList, i);
          if (p == NULL) {
            return TSDB_CODE_INVALID_PARA;
          }

          if (p->upstreamTaskId == pBlock->srcTaskId) {
            stWarn("s-task:%s repeatly recv checkpoint-source msg from task:0x%x vgId:%d, checkpointId:%" PRId64
                   ", prev recvTs:%" PRId64 " discard",
                   pTask->id.idStr, p->upstreamTaskId, p->upstreamNodeId, p->checkpointId, p->recvTs);
            return code;
          }
        }
      }
    }
  }

  return 0;
}

int32_t streamProcessCheckpointTriggerBlock(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  int64_t                checkpointId = 0;
  int32_t                transId = 0;
  const char*            id = pTask->id.idStr;
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                vgId = pTask->pMeta->vgId;
  int32_t                taskLevel = pTask->info.taskLevel;
  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;

  SSDataBlock* pDataBlock = taosArrayGet(pBlock->blocks, 0);
  if (pDataBlock == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  checkpointId = pDataBlock->info.version;
  transId = pDataBlock->info.window.skey;

  streamMutexLock(&pTask->lock);
  code = doCheckBeforeHandleChkptTrigger(pTask, checkpointId, pBlock, transId);
  streamMutexUnlock(&pTask->lock);
  if (code) {
    streamFreeQitem((SStreamQueueItem*)pBlock);
    return code;
  }

  stDebug("s-task:%s vgId:%d start to handle the checkpoint-trigger block, checkpointId:%" PRId64 " ver:%" PRId64
          ", transId:%d current active checkpointId:%" PRId64,
          id, vgId, pTask->chkInfo.checkpointId, pTask->chkInfo.checkpointVer, transId, checkpointId);

  // set task status
  if (streamTaskGetStatus(pTask).state != TASK_STATUS__CK) {
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
    int8_t          old = atomic_val_compare_exchange_8(&pTmrInfo->isActive, 0, 1);
    if (old == 0) {
      int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
      stDebug("s-task:%s start checkpoint-trigger monitor in 10s, ref:%d ", pTask->id.idStr, ref);
      streamMetaAcquireOneTask(pTask);
      streamTmrStart(checkpointTriggerMonitorFn, 200, pTask, streamTimer, &pTmrInfo->tmrHandle, vgId,
                     "trigger-recv-monitor");
      pTmrInfo->launchChkptId = pActiveInfo->activeId;
    } else {  // already launched, do nothing
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
    code = flushStateDataInExecutor(pTask, (SStreamQueueItem*)pBlock);
    if (code) {
      streamFreeQitem((SStreamQueueItem*)pBlock);
      return code;
    }

    if (type == TASK_OUTPUT__FIXED_DISPATCH || type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      stDebug("s-task:%s set childIdx:%d, and add checkpoint-trigger block into outputQ", id, pTask->info.selfChildId);
      code = continueDispatchCheckpointTriggerBlock(pBlock, pTask);  // todo handle this failure
    } else {  // only one task exists, no need to dispatch downstream info
      code =
          appendCheckpointIntoInputQ(pTask, STREAM_INPUT__CHECKPOINT, pActiveInfo->activeId, pActiveInfo->transId, -1);
      streamFreeQitem((SStreamQueueItem*)pBlock);
    }
  } else if (taskLevel == TASK_LEVEL__SINK || taskLevel == TASK_LEVEL__AGG) {
    if (pTask->chkInfo.startTs == 0) {
      pTask->chkInfo.startTs = taosGetTimestampMs();
      pTask->execInfo.checkpoint += 1;
    }

    // todo: handle this
    // update the child Id for downstream tasks
    code = streamAddCheckpointReadyMsg(pTask, pBlock->srcTaskId, pTask->info.selfChildId, checkpointId);

    // there are still some upstream tasks not send checkpoint request, do nothing and wait for then
    if (pActiveInfo->allUpstreamTriggerRecv != 1) {
      streamFreeQitem((SStreamQueueItem*)pBlock);
      return code;
    }

    int32_t num = streamTaskGetNumOfUpstream(pTask);
    if (taskLevel == TASK_LEVEL__SINK) {
      stDebug("s-task:%s process checkpoint-trigger block, all %d upstreams sent, send ready msg to upstream", id, num);
      streamFreeQitem((SStreamQueueItem*)pBlock);
      code = streamTaskBuildCheckpoint(pTask);  // todo: not handle error yet
    } else {                                    // source & agg tasks need to forward the checkpoint msg downwards
      stDebug("s-task:%s process checkpoint-trigger block, all %d upstreams sent, forwards to downstream", id, num);
      code = flushStateDataInExecutor(pTask, (SStreamQueueItem*)pBlock);
      if (code) {
        return code;
      }

      // Put the checkpoint-trigger block into outputQ, to make sure all blocks with less version have been handled by
      // this task already. And then, dispatch check point msg to all downstream tasks
      code = continueDispatchCheckpointTriggerBlock(pBlock, pTask);
    }
  }

  return code;
}

// only when all downstream tasks are send checkpoint rsp, we can start the checkpoint procedure for the agg task
static int32_t processCheckpointReadyHelp(SActiveCheckpointInfo* pInfo, int32_t numOfDownstream,
                                          int32_t downstreamNodeId, int64_t streamId, int32_t downstreamTaskId,
                                          const char* id, int32_t* pNotReady, int32_t* pTransId, bool* alreadyRecv) {
  *alreadyRecv = false;
  int32_t size = taosArrayGetSize(pInfo->pCheckpointReadyRecvList);
  for (int32_t i = 0; i < size; ++i) {
    STaskDownstreamReadyInfo* p = taosArrayGet(pInfo->pCheckpointReadyRecvList, i);
    if (p == NULL) {
      return TSDB_CODE_INVALID_PARA;
    }

    if (p->downstreamTaskId == downstreamTaskId) {
      (*alreadyRecv) = true;
      break;
    }
  }

  if (*alreadyRecv) {
    stDebug("s-task:%s already recv checkpoint-ready msg from downstream:0x%x, ignore. %d/%d downstream not ready", id,
            downstreamTaskId, (int32_t)(numOfDownstream - taosArrayGetSize(pInfo->pCheckpointReadyRecvList)),
            numOfDownstream);
  } else {
    STaskDownstreamReadyInfo info = {.recvTs = taosGetTimestampMs(),
                                     .downstreamTaskId = downstreamTaskId,
                                     .checkpointId = pInfo->activeId,
                                     .transId = pInfo->transId,
                                     .streamId = streamId,
                                     .downstreamNodeId = downstreamNodeId};
    void*                    p = taosArrayPush(pInfo->pCheckpointReadyRecvList, &info);
    if (p == NULL) {
      stError("s-task:%s failed to set checkpoint ready recv msg, code:%s", id, tstrerror(terrno));
      return terrno;
    }
  }

  *pNotReady = numOfDownstream - taosArrayGetSize(pInfo->pCheckpointReadyRecvList);
  *pTransId = pInfo->transId;
  return 0;
}

/**
 * All down stream tasks have successfully completed the check point task.
 * Current stream task is allowed to start to do checkpoint things in ASYNC model.
 */
int32_t streamProcessCheckpointReadyMsg(SStreamTask* pTask, int64_t checkpointId, int32_t downstreamNodeId,
                                        int32_t downstreamTaskId) {
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;

  const char* id = pTask->id.idStr;
  int32_t     total = streamTaskGetNumOfDownstream(pTask);
  int32_t     code = 0;
  int32_t     notReady = 0;
  int32_t     transId = 0;
  bool        alreadyHandled = false;

  // 1. not in checkpoint status now
  SStreamTaskState pStat = streamTaskGetStatus(pTask);
  if (pStat.state != TASK_STATUS__CK) {
    stError("s-task:%s status:%s discard checkpoint-ready msg from task:0x%x", id, pStat.name, downstreamTaskId);
    return TSDB_CODE_STREAM_TASK_IVLD_STATUS;
  }

  // 2. expired checkpoint-ready msg, invalid checkpoint-ready msg
  if (pTask->chkInfo.checkpointId > checkpointId || pInfo->activeId != checkpointId) {
    stError("s-task:%s status:%s checkpointId:%" PRId64 " new arrival checkpoint-ready msg (checkpointId:%" PRId64
            ") from task:0x%x, expired and discard",
            id, pStat.name, pTask->chkInfo.checkpointId, checkpointId, downstreamTaskId);
    return TSDB_CODE_INVALID_MSG;
  }

  streamMutexLock(&pInfo->lock);
  code = processCheckpointReadyHelp(pInfo, total, downstreamNodeId, pTask->id.streamId, downstreamTaskId, id, &notReady,
                                    &transId, &alreadyHandled);
  streamMutexUnlock(&pInfo->lock);

  if (alreadyHandled) {
    stDebug("s-task:%s checkpoint-ready msg checkpointId:%" PRId64 " from task:0x%x already handled, not handle again",
            id, checkpointId, downstreamTaskId);
  } else {
    if ((notReady == 0) && (code == 0) && (!alreadyHandled)) {
      stDebug("s-task:%s all downstream tasks have completed build checkpoint, do checkpoint for current task", id);
      code = appendCheckpointIntoInputQ(pTask, STREAM_INPUT__CHECKPOINT, checkpointId, transId, -1);
    }
  }

  return code;
}

int32_t streamTaskProcessCheckpointReadyRsp(SStreamTask* pTask, int32_t upstreamTaskId, int64_t checkpointId) {
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;
  int64_t                now = taosGetTimestampMs();
  int32_t                numOfConfirmed = 0;

  streamMutexLock(&pInfo->lock);
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pReadyMsgList); ++i) {
    STaskCheckpointReadyInfo* pReadyInfo = taosArrayGet(pInfo->pReadyMsgList, i);
    if (pReadyInfo == NULL) {
      stError("s-task:%s invalid index during iterate the checkpoint-ready msg list, index:%d, ignore and continue",
              pTask->id.idStr, i);
      continue;
    }

    if (pReadyInfo->upstreamTaskId == upstreamTaskId && pReadyInfo->checkpointId == checkpointId) {
      pReadyInfo->sendCompleted = 1;
      stDebug("s-task:%s send checkpoint-ready msg to upstream:0x%x confirmed, checkpointId:%" PRId64 " ts:%" PRId64,
              pTask->id.idStr, upstreamTaskId, checkpointId, now);
      break;
    }
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pReadyMsgList); ++i) {
    STaskCheckpointReadyInfo* pReadyInfo = taosArrayGet(pInfo->pReadyMsgList, i);
    if (pReadyInfo == NULL) {
      stError("s-task:%s invalid index during iterate the checkpoint-ready msg list, index:%d, ignore and continue",
              pTask->id.idStr, i);
      continue;
    }

    if (pReadyInfo->sendCompleted == 1) {
      numOfConfirmed += 1;
    }
  }

  stDebug("s-task:%s send checkpoint-ready msg to %d upstream confirmed, checkpointId:%" PRId64, pTask->id.idStr,
          numOfConfirmed, checkpointId);

  streamMutexUnlock(&pInfo->lock);
  return TSDB_CODE_SUCCESS;
}

void streamTaskClearCheckInfo(SStreamTask* pTask, bool clearChkpReadyMsg) {
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;

  pTask->chkInfo.startTs = 0;             // clear the recorded start time
  streamTaskOpenAllUpstreamInput(pTask);  // open inputQ for all upstream tasks

  streamMutexLock(&pInfo->lock);
  streamTaskClearActiveInfo(pInfo);
  if (clearChkpReadyMsg) {
    streamClearChkptReadyMsg(pInfo);
  }
  streamMutexUnlock(&pInfo->lock);

  stDebug("s-task:%s clear active checkpointInfo, failed checkpointId:%" PRId64 ", current checkpointId:%" PRId64,
          pTask->id.idStr, pInfo->failedId, pTask->chkInfo.checkpointId);
}

int32_t streamTaskUpdateTaskCheckpointInfo(SStreamTask* pTask, bool restored, SVUpdateCheckpointInfoReq* pReq) {
  SStreamMeta*     pMeta = pTask->pMeta;
  int32_t          vgId = pMeta->vgId;
  int32_t          code = 0;
  const char*      id = pTask->id.idStr;
  SCheckpointInfo* pInfo = &pTask->chkInfo;

  streamMutexLock(&pTask->lock);

  if (pReq->checkpointId <= pInfo->checkpointId) {
    stDebug("s-task:%s vgId:%d latest checkpointId:%" PRId64 " Ver:%" PRId64
            " no need to update checkpoint info, updated checkpointId:%" PRId64 " Ver:%" PRId64 " transId:%d ignored",
            id, vgId, pInfo->checkpointId, pInfo->checkpointVer, pReq->checkpointId, pReq->checkpointVer,
            pReq->transId);
    streamMutexUnlock(&pTask->lock);

    {  // destroy the related fill-history tasks
      // drop task should not in the meta-lock, and drop the related fill-history task now
      streamMetaWUnLock(pMeta);
      if (pReq->dropRelHTask) {
        code = streamMetaUnregisterTask(pMeta, pReq->hStreamId, pReq->hTaskId);
        int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
        stDebug("s-task:%s vgId:%d related fill-history task:0x%x dropped in update checkpointInfo, remain tasks:%d",
                id, vgId, pReq->taskId, numOfTasks);
      }

      streamMetaWLock(pMeta);
      if (pReq->dropRelHTask) {
        code = streamMetaCommit(pMeta);
      }
    }

    // always return true
    return TSDB_CODE_SUCCESS;
  }

  SStreamTaskState pStatus = streamTaskGetStatus(pTask);

  if (!restored) {  // during restore procedure, do update checkpoint-info
    stDebug("s-task:%s vgId:%d status:%s update the checkpoint-info during restore, checkpointId:%" PRId64 "->%" PRId64
            " checkpointVer:%" PRId64 "->%" PRId64 " checkpointTs:%" PRId64 "->%" PRId64,
            id, vgId, pStatus.name, pInfo->checkpointId, pReq->checkpointId, pInfo->checkpointVer, pReq->checkpointVer,
            pInfo->checkpointTime, pReq->checkpointTs);
  } else {  // not in restore status, must be in checkpoint status
    if ((pStatus.state == TASK_STATUS__CK) || (pMeta->role == NODE_ROLE_FOLLOWER)) {
      stDebug("s-task:%s vgId:%d status:%s start to update the checkpoint-info, checkpointId:%" PRId64 "->%" PRId64
              " checkpointVer:%" PRId64 "->%" PRId64 " checkpointTs:%" PRId64 "->%" PRId64,
              id, vgId, pStatus.name, pInfo->checkpointId, pReq->checkpointId, pInfo->checkpointVer,
              pReq->checkpointVer, pInfo->checkpointTime, pReq->checkpointTs);
    } else {
      stDebug("s-task:%s vgId:%d status:%s NOT update the checkpoint-info, checkpointId:%" PRId64 "->%" PRId64
              " checkpointVer:%" PRId64 "->%" PRId64,
              id, vgId, pStatus.name, pInfo->checkpointId, pReq->checkpointId, pInfo->checkpointVer,
              pReq->checkpointVer);
    }
  }

  bool valid = (pInfo->checkpointId <= pReq->checkpointId && pInfo->checkpointVer <= pReq->checkpointVer &&
                pInfo->processedVer <= pReq->checkpointVer);

  if (!valid) {
    stFatal("invalid checkpoint id check, current checkpointId:%" PRId64 " checkpointVer:%" PRId64
            " processedVer:%" PRId64 " req checkpointId:%" PRId64 " checkpointVer:%" PRId64,
            pInfo->checkpointId, pInfo->checkpointVer, pInfo->processedVer, pReq->checkpointId, pReq->checkpointVer);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  // update only it is in checkpoint status, or during restore procedure.
  if ((pStatus.state == TASK_STATUS__CK) || (!restored) || (pMeta->role == NODE_ROLE_FOLLOWER)) {
    pInfo->checkpointId = pReq->checkpointId;
    pInfo->checkpointVer = pReq->checkpointVer;
    pInfo->checkpointTime = pReq->checkpointTs;

    if (restored && (pMeta->role == NODE_ROLE_LEADER)) {
      code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_CHECKPOINT_DONE);
    }
  }

  streamTaskClearCheckInfo(pTask, true);

  if (pReq->dropRelHTask) {
    stDebug("s-task:0x%x vgId:%d drop the related fill-history task:0x%" PRIx64 " after update checkpoint",
            pReq->taskId, vgId, pReq->hTaskId);
    CLEAR_RELATED_FILLHISTORY_TASK(pTask);
  }

  stDebug("s-task:0x%x set the persistent status attr to be ready, prev:%s, status in sm:%s", pReq->taskId,
          streamTaskGetStatusStr(pTask->status.taskStatus), streamTaskGetStatus(pTask).name);

  pTask->status.taskStatus = TASK_STATUS__READY;

  code = streamMetaSaveTask(pMeta, pTask);
  streamMutexUnlock(&pTask->lock);

  if (code != TSDB_CODE_SUCCESS) {
    stError("s-task:%s vgId:%d failed to save task info after do checkpoint, checkpointId:%" PRId64 ", since %s", id,
            vgId, pReq->checkpointId, terrstr());
    return TSDB_CODE_SUCCESS;
  }

  streamMetaWUnLock(pMeta);

  // drop task should not in the meta-lock, and drop the related fill-history task now
  if (pReq->dropRelHTask) {
    code = streamMetaUnregisterTask(pMeta, pReq->hStreamId, pReq->hTaskId);
    int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
    stDebug("s-task:%s vgId:%d related fill-history task:0x%x dropped, remain tasks:%d", id, vgId,
            (int32_t)pReq->hTaskId, numOfTasks);
  }

  streamMetaWLock(pMeta);
  code = streamMetaCommit(pMeta);

  return TSDB_CODE_SUCCESS;
}

void streamTaskSetFailedCheckpointId(SStreamTask* pTask) {
  struct SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;

  if (pInfo->activeId <= 0) {
    stWarn("s-task:%s checkpoint-info is cleared now, not set the failed checkpoint info", pTask->id.idStr);
  } else {
    pInfo->failedId = pInfo->activeId;
    stDebug("s-task:%s mark and set the failed checkpointId:%" PRId64 " (transId:%d)", pTask->id.idStr, pInfo->activeId,
            pInfo->transId);
  }
}

void streamTaskSetCheckpointFailed(SStreamTask* pTask) {
  streamMutexLock(&pTask->lock);
  ETaskStatus status = streamTaskGetStatus(pTask).state;
  if (status == TASK_STATUS__CK) {
    streamTaskSetFailedCheckpointId(pTask);
  }
  streamMutexUnlock(&pTask->lock);
}

static int32_t getCheckpointDataMeta(const char* id, const char* path, SArray* list) {
  int32_t code = 0;
  int32_t cap = strlen(path) + 64;

  char* filePath = taosMemoryCalloc(1, cap);
  if (filePath == NULL) {
    return terrno;
  }

  int32_t nBytes = snprintf(filePath, cap, "%s%s%s", path, TD_DIRSEP, "META_TMP");
  if (nBytes <= 0 || nBytes >= cap) {
    taosMemoryFree(filePath);
    return TSDB_CODE_OUT_OF_RANGE;
  }

  code = downloadCheckpointDataByName(id, "META", filePath);
  if (code != 0) {
    stError("%s chkp failed to download meta file:%s", id, filePath);
    taosMemoryFree(filePath);
    return code;
  }

  code = remoteChkpGetDelFile(filePath, list);
  if (code != 0) {
    stError("%s chkp failed to get to del:%s", id, filePath);
    taosMemoryFree(filePath);
  }
  return 0;
}

int32_t uploadCheckpointData(SStreamTask* pTask, int64_t checkpointId, int64_t dbRefId, ECHECKPOINT_BACKUP_TYPE type) {
  int32_t code = 0;
  char*   path = NULL;

  SStreamMeta* pMeta = pTask->pMeta;
  const char*  idStr = pTask->id.idStr;
  int64_t      now = taosGetTimestampMs();

  SArray* toDelFiles = taosArrayInit(4, POINTER_BYTES);
  if (toDelFiles == NULL) {
    return terrno;
  }

  if ((code = taskDbGenChkpUploadData(pTask->pBackend, pMeta->bkdChkptMgt, checkpointId, type, &path, toDelFiles,
                                      pTask->id.idStr)) != 0) {
    stError("s-task:%s failed to gen upload checkpoint:%" PRId64 ", reason:%s", idStr, checkpointId, tstrerror(code));
  }

  if (type == DATA_UPLOAD_S3) {
    if (code == TSDB_CODE_SUCCESS && (code = getCheckpointDataMeta(idStr, path, toDelFiles)) != 0) {
      stError("s-task:%s failed to get checkpointData for checkpointId:%" PRId64 ", reason:%s", idStr, checkpointId,
              tstrerror(code));
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = streamTaskUploadCheckpoint(idStr, path, checkpointId);
    if (code == TSDB_CODE_SUCCESS) {
      stDebug("s-task:%s upload checkpointId:%" PRId64 " to remote succ", idStr, checkpointId);
    } else {
      stError("s-task:%s failed to upload checkpointId:%" PRId64 " path:%s,reason:%s", idStr, checkpointId, path,
              tstrerror(code));
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

    int64_t ver = pTask->chkInfo.processedVer;
    code = streamBackendDoCheckpoint(pTask->pBackend, ckId, ver);
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
    streamMutexLock(&pTask->lock);
    streamTaskSetFailedCheckpointId(pTask);  // set failed checkpoint id before clear the checkpoint info
    streamMutexUnlock(&pTask->lock);

    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_CHECKPOINT_DONE);
    stDebug("s-task:%s clear checkpoint flag since gen checkpoint failed, checkpointId:%" PRId64, id, ckId);
  }

  double el = (taosGetTimestampMs() - startTs) / 1000.0;
  stInfo("s-task:%s vgId:%d level:%d, checkpointId:%" PRId64 " ver:%" PRId64 " elapsed time:%.2fs, %s ", id,
         pMeta->vgId, pTask->info.taskLevel, ckId, pTask->chkInfo.checkpointVer, el,
         (code == TSDB_CODE_SUCCESS) ? "succ" : "failed");

  return code;
}

static int32_t doChkptStatusCheck(SStreamTask* pTask) {
  const char*            id = pTask->id.idStr;
  int32_t                vgId = pTask->pMeta->vgId;
  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;
  SStreamTmrInfo*        pTmrInfo = &pActiveInfo->chkptTriggerMsgTmr;

  // checkpoint-trigger recv flag is set, quit
  if (pActiveInfo->allUpstreamTriggerRecv) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stDebug("s-task:%s vgId:%d all checkpoint-trigger recv, quit from monitor checkpoint-trigger, ref:%d", id, vgId,
            ref);
    return -1;
  }

  if ((pTmrInfo->launchChkptId != pActiveInfo->activeId) || (pActiveInfo->activeId == 0)) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stWarn("s-task:%s vgId:%d checkpoint-trigger retrieve by previous checkpoint procedure, checkpointId:%" PRId64
           ", quit, ref:%d",
           id, vgId, pTmrInfo->launchChkptId, ref);
    return -1;
  }

  // active checkpoint info is cleared for now
  if ((pActiveInfo->activeId == 0) || (pActiveInfo->transId == 0) || (pTask->chkInfo.startTs == 0)) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stWarn("s-task:%s vgId:%d active checkpoint may be cleared, quit from retrieve checkpoint-trigger send tmr, ref:%d",
           id, vgId, ref);
    return -1;
  }

  return 0;
}

static int32_t doFindNotSendUpstream(SStreamTask* pTask, SArray* pList, SArray** ppNotSendList) {
  const char*            id = pTask->id.idStr;
  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;

  SArray* pNotSendList = taosArrayInit(4, sizeof(SStreamUpstreamEpInfo));
  if (pNotSendList == NULL) {
    stDebug("s-task:%s start to triggerMonitor, reason:%s", id, tstrerror(terrno));
    return terrno;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pList); ++i) {
    SStreamUpstreamEpInfo* pInfo = taosArrayGetP(pList, i);

    bool recved = false;
    for (int32_t j = 0; j < taosArrayGetSize(pActiveInfo->pReadyMsgList); ++j) {
      STaskCheckpointReadyInfo* pReady = taosArrayGet(pActiveInfo->pReadyMsgList, j);
      if (pReady == NULL) {
        continue;
      }

      if (pInfo->nodeId == pReady->upstreamNodeId) {
        recved = true;
        break;
      }
    }

    if (!recved) {  // make sure the inputQ is opened for not recv upstream checkpoint-trigger message
      streamTaskOpenUpstreamInput(pTask, pInfo->taskId);
      void* px = taosArrayPush(pNotSendList, pInfo);
      if (px == NULL) {
        stError("s-task:%s failed to record not send info, code: out of memory", id);
        taosArrayDestroy(pNotSendList);
        return terrno;
      }
    }
  }

  *ppNotSendList = pNotSendList;
  return 0;
}

static int32_t chkptTriggerRecvMonitorHelper(SStreamTask* pTask, SArray* pNotSendList) {
  const char*            id = pTask->id.idStr;
  SArray*                pList = pTask->upstreamInfo.pList;  // send msg to retrieve checkpoint trigger msg
  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;
  SStreamTmrInfo*        pTmrInfo = &pActiveInfo->chkptTriggerMsgTmr;
  int32_t                vgId = pTask->pMeta->vgId;

  int32_t code = doChkptStatusCheck(pTask);
  if (code) {
    return code;
  }

  code = doFindNotSendUpstream(pTask, pList, &pNotSendList);
  if (code) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stDebug("s-task:%s failed to find not send upstream, code:%s, out of tmr, ref:%d", id, tstrerror(code), ref);
    return code;
  }

  // do send retrieve checkpoint trigger msg to upstream
  code = doSendRetrieveTriggerMsg(pTask, pNotSendList);
  if (code) {
    stError("s-task:%s vgId:%d failed to retrieve trigger msg, code:%s", pTask->id.idStr, vgId, tstrerror(code));
    code = 0;
  }

  return code;
}

void checkpointTriggerMonitorFn(void* param, void* tmrId) {
  SStreamTask* pTask = param;
  int32_t      vgId = pTask->pMeta->vgId;
  int64_t      now = taosGetTimestampMs();
  const char*  id = pTask->id.idStr;
  SArray*      pNotSendList = NULL;
  SArray*      pList = pTask->upstreamInfo.pList;  // send msg to retrieve checkpoint trigger msg
  int32_t      code = 0;
  int32_t      numOfNotSend = 0;

  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;
  SStreamTmrInfo*        pTmrInfo = &pActiveInfo->chkptTriggerMsgTmr;

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stError("s-task:%s source task should not start the checkpoint-trigger monitor fn, ref:%d quit", id, ref);
    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  // check the status every 100ms
  if (streamTaskShouldStop(pTask)) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stDebug("s-task:%s vgId:%d quit from monitor checkpoint-trigger, ref:%d", id, vgId, ref);
    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  if (++pTmrInfo->activeCounter < 50) {
    streamTmrStart(checkpointTriggerMonitorFn, 200, pTask, streamTimer, &pTmrInfo->tmrHandle, vgId,
                   "trigger-recv-monitor");
    return;
  }

  pTmrInfo->activeCounter = 0;
  stDebug("s-task:%s vgId:%d checkpoint-trigger monitor in tmr, ts:%" PRId64, id, vgId, now);

  streamMutexLock(&pTask->lock);
  SStreamTaskState state = streamTaskGetStatus(pTask);
  streamMutexUnlock(&pTask->lock);

  if (state.state != TASK_STATUS__CK) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stDebug("s-task:%s vgId:%d status:%s not in checkpoint status, quit from monitor checkpoint-trigger, ref:%d", id,
            vgId, state.name, ref);
    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  streamMutexLock(&pActiveInfo->lock);
  code = chkptTriggerRecvMonitorHelper(pTask, pNotSendList);
  streamMutexUnlock(&pActiveInfo->lock);

  if (code != TSDB_CODE_SUCCESS) {
    streamMetaReleaseTask(pTask->pMeta, pTask);
    taosArrayDestroy(pNotSendList);
    return;
  }

  // check every 100ms
  numOfNotSend = taosArrayGetSize(pNotSendList);
  if (numOfNotSend > 0) {
    stDebug("s-task:%s start to monitor checkpoint-trigger in 10s", id);
    streamTmrStart(checkpointTriggerMonitorFn, 200, pTask, streamTimer, &pTmrInfo->tmrHandle, vgId,
                   "trigger-recv-monitor");
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
    if (pUpstreamTask == NULL) {
      return TSDB_CODE_INVALID_PARA;
    }

    SRetrieveChkptTriggerReq* pReq = rpcMallocCont(sizeof(SRetrieveChkptTriggerReq));
    if (pReq == NULL) {
      code = terrno;
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
    if (code == TSDB_CODE_SUCCESS) {
      stDebug("s-task:%s vgId:%d send checkpoint-trigger retrieve msg to 0x%x(vgId:%d) checkpointId:%" PRId64, pId,
              vgId, pUpstreamTask->taskId, pUpstreamTask->nodeId, checkpointId);
    } else {
      stError("s-task:%s vgId:%d failed to send checkpoint-trigger retrieve msg to 0x%x(vgId:%d) checkpointId:%" PRId64,
              pId, vgId, pUpstreamTask->taskId, pUpstreamTask->nodeId, checkpointId);
    }
  }

  return code;
}

static int32_t isAlreadySendTriggerNoLock(SStreamTask* pTask, int32_t downstreamNodeId) {
  int64_t                now = taosGetTimestampMs();
  const char*            id = pTask->id.idStr;
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;
  SStreamTaskState       pStatus = streamTaskGetStatus(pTask);

  if (!pInfo->dispatchTrigger) {
    return false;
  }

  int32_t num = taosArrayGetSize(pInfo->pDispatchTriggerList);
  for (int32_t i = 0; i < num; ++i) {
    STaskTriggerSendInfo* pSendInfo = taosArrayGet(pInfo->pDispatchTriggerList, i);
    if (pSendInfo == NULL) {
      stError("s-task:%s invalid index in dispatch-trigger list, index:%d, size:%d, ignore and continue", id, i, num);
      continue;
    }

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

    return true;
  }

  return false;
}

bool streamTaskAlreadySendTrigger(SStreamTask* pTask, int32_t downstreamNodeId) {
  int64_t                now = taosGetTimestampMs();
  const char*            id = pTask->id.idStr;
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;
  SStreamTaskState       pStatus = streamTaskGetStatus(pTask);

  if (pStatus.state != TASK_STATUS__CK) {
    return false;
  }

  streamMutexLock(&pInfo->lock);
  bool send = isAlreadySendTriggerNoLock(pTask, downstreamNodeId);
  streamMutexUnlock(&pInfo->lock);

  return send;
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
// memory insufficient may cause the stream computing stopped
int32_t streamTaskInitTriggerDispatchInfo(SStreamTask* pTask) {
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;
  int64_t                now = taosGetTimestampMs();
  int32_t                code = 0;

  streamMutexLock(&pInfo->lock);

  pInfo->dispatchTrigger = true;
  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    STaskDispatcherFixed* pDispatch = &pTask->outputInfo.fixedDispatcher;

    STaskTriggerSendInfo p = {.sendTs = now, .recved = false, .nodeId = pDispatch->nodeId, .taskId = pDispatch->taskId};
    void*                px = taosArrayPush(pInfo->pDispatchTriggerList, &p);
    if (px == NULL) {  // pause the stream task, if memory not enough
      code = terrno;
    }
  } else {
    for (int32_t i = 0; i < streamTaskGetNumOfDownstream(pTask); ++i) {
      SVgroupInfo* pVgInfo = taosArrayGet(pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos, i);
      if (pVgInfo == NULL) {
        continue;
      }

      STaskTriggerSendInfo p = {.sendTs = now, .recved = false, .nodeId = pVgInfo->vgId, .taskId = pVgInfo->taskId};
      void*                px = taosArrayPush(pInfo->pDispatchTriggerList, &p);
      if (px == NULL) {  // pause the stream task, if memory not enough
        code = terrno;
        break;
      }
    }
  }

  streamMutexUnlock(&pInfo->lock);

  return code;
}

int32_t streamTaskGetNumOfConfirmed(SActiveCheckpointInfo* pInfo) {
  int32_t num = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pDispatchTriggerList); ++i) {
    STaskTriggerSendInfo* p = taosArrayGet(pInfo->pDispatchTriggerList, i);
    if (p == NULL) {
      continue;
    }

    if (p->recved) {
      num++;
    }
  }
  return num;
}

void streamTaskSetTriggerDispatchConfirmed(SStreamTask* pTask, int32_t vgId) {
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;

  int64_t now = taosGetTimestampMs();
  int32_t taskId = 0;
  int32_t total = streamTaskGetNumOfDownstream(pTask);
  bool    alreadyRecv = false;

  streamMutexLock(&pInfo->lock);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pDispatchTriggerList); ++i) {
    STaskTriggerSendInfo* p = taosArrayGet(pInfo->pDispatchTriggerList, i);
    if (p == NULL) {
      continue;
    }

    if (p->nodeId == vgId) {
      if (p->recved) {
        stWarn("s-task:%s already recv checkpoint-trigger msg rsp from vgId:%d down:0x%x %.2fs ago, req send:%" PRId64
               " discard",
               pTask->id.idStr, vgId, p->taskId, (now - p->recvTs) / 1000.0, p->sendTs);
        alreadyRecv = true;
      } else {
        p->recved = true;
        p->recvTs = taosGetTimestampMs();
        taskId = p->taskId;
      }
      break;
    }
  }

  int32_t numOfConfirmed = streamTaskGetNumOfConfirmed(pInfo);
  streamMutexUnlock(&pInfo->lock);

  if (taskId == 0) {
    stError("s-task:%s recv invalid trigger-dispatch confirm, vgId:%d", pTask->id.idStr, vgId);
  } else {
    if (!alreadyRecv) {
      stDebug("s-task:%s set downstream:0x%x(vgId:%d) checkpoint-trigger dispatch confirmed, total confirmed:%d/%d",
              pTask->id.idStr, taskId, vgId, numOfConfirmed, total);
    }
  }
}

static int32_t uploadCheckpointToS3(const char* id, const char* path) {
  int32_t code = 0;
  int32_t nBytes = 0;
  /*
  if (tcsInit() != 0) {
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
  */
  TdDirPtr pDir = taosOpenDir(path);
  if (pDir == NULL) {
    return terrno;
  }

  TdDirEntryPtr de = NULL;
  while ((de = taosReadDir(pDir)) != NULL) {
    char* name = taosGetDirEntryName(de);
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0 || taosDirEntryIsDir(de)) continue;

    char filename[PATH_MAX] = {0};
    if (path[strlen(path) - 1] == TD_DIRSEP_CHAR) {
      nBytes = snprintf(filename, sizeof(filename), "%s%s", path, name);
      if (nBytes <= 0 || nBytes >= sizeof(filename)) {
        code = TSDB_CODE_OUT_OF_RANGE;
        break;
      }
    } else {
      nBytes = snprintf(filename, sizeof(filename), "%s%s%s", path, TD_DIRSEP, name);
      if (nBytes <= 0 || nBytes >= sizeof(filename)) {
        code = TSDB_CODE_OUT_OF_RANGE;
        break;
      }
    }

    char object[PATH_MAX] = {0};
    nBytes = snprintf(object, sizeof(object), "%s%s%s", id, TD_DIRSEP, name);
    if (nBytes <= 0 || nBytes >= sizeof(object)) {
      code = TSDB_CODE_OUT_OF_RANGE;
      break;
    }

    code = tcsPutObjectFromFile2(filename, object, 0);
    if (code != 0) {
      stError("[s3] failed to upload checkpoint:%s, reason:%s", filename, tstrerror(code));
    } else {
      stDebug("[s3] upload checkpoint:%s", filename);
    }
  }

  int32_t ret = taosCloseDir(&pDir);
  if (code == 0 && ret != 0) {
    code = ret;
  }

  return code;
}

int32_t downloadCheckpointByNameS3(const char* id, const char* fname, const char* dstName) {
  int32_t nBytes;
  int32_t cap = strlen(id) + strlen(dstName) + 16;

  char* buf = taosMemoryCalloc(1, cap);
  if (buf == NULL) {
    return terrno;
  }

  nBytes = snprintf(buf, cap, "%s/%s", id, fname);
  if (nBytes <= 0 || nBytes >= cap) {
    taosMemoryFree(buf);
    return TSDB_CODE_OUT_OF_RANGE;
  }
  int32_t code = tcsGetObjectToFile(buf, dstName);
  if (code != 0) {
    taosMemoryFree(buf);
    return TAOS_SYSTEM_ERROR(errno);
  }
  taosMemoryFree(buf);
  return 0;
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

int32_t streamTaskUploadCheckpoint(const char* id, const char* path, int64_t checkpointId) {
  int32_t code = 0;
  if (id == NULL || path == NULL || strlen(id) == 0 || strlen(path) == 0 || strlen(path) >= PATH_MAX) {
    stError("invalid parameters in upload checkpoint, %s", id);
    return TSDB_CODE_INVALID_CFG;
  }

  if (strlen(tsSnodeAddress) != 0) {
    code = uploadByRsync(id, path, checkpointId);
    if (code != 0) {
      return TAOS_SYSTEM_ERROR(errno);
    }
  } else if (tsS3StreamEnabled) {
    return uploadCheckpointToS3(id, path);
  }

  return 0;
}

// fileName:  CURRENT
int32_t downloadCheckpointDataByName(const char* id, const char* fname, const char* dstName) {
  if (id == NULL || fname == NULL || strlen(id) == 0 || strlen(fname) == 0 || strlen(fname) >= PATH_MAX) {
    stError("down load checkpoint data parameters invalid");
    return TSDB_CODE_INVALID_PARA;
  }

  if (strlen(tsSnodeAddress) != 0) {
    return 0;
  } else if (tsS3StreamEnabled) {
    return downloadCheckpointByNameS3(id, fname, dstName);
  }

  return 0;
}

int32_t streamTaskDownloadCheckpointData(const char* id, char* path, int64_t checkpointId) {
  if (id == NULL || path == NULL || strlen(id) == 0 || strlen(path) == 0 || strlen(path) >= PATH_MAX) {
    stError("down checkpoint data parameters invalid");
    return -1;
  }

  if (strlen(tsSnodeAddress) != 0) {
    return downloadByRsync(id, path, checkpointId);
  } else if (tsS3StreamEnabled) {
    return tcsGetObjectsByPrefix(id, path);
  }

  return 0;
}

int32_t deleteCheckpoint(const char* id) {
  if (id == NULL || strlen(id) == 0) {
    stError("deleteCheckpoint parameters invalid");
    return TSDB_CODE_INVALID_PARA;
  }
  if (strlen(tsSnodeAddress) != 0) {
    return deleteRsync(id);
  } else if (tsS3StreamEnabled) {
    tcsDeleteObjectsByPrefix(id);
  }
  return 0;
}

int32_t deleteCheckpointFile(const char* id, const char* name) {
  char object[128] = {0};

  int32_t nBytes = snprintf(object, sizeof(object), "%s/%s", id, name);
  if (nBytes <= 0 || nBytes >= sizeof(object)) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  char*   tmp = object;
  int32_t code = tcsDeleteObjects((const char**)&tmp, 1);
  if (code != 0) {
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
  return code;
}

int32_t streamTaskSendNegotiateChkptIdMsg(SStreamTask* pTask) {
  streamMutexLock(&pTask->lock);
  ETaskStatus p = streamTaskGetStatus(pTask).state;
  //  if (pInfo->alreadySendChkptId == true) {
  //    stDebug("s-task:%s already start to consensus-checkpointId, not start again before it completed", id);
  //    streamMutexUnlock(&pTask->lock);
  //    return TSDB_CODE_SUCCESS;
  //  } else {
  //    pInfo->alreadySendChkptId = true;
  //  }
  //
  streamTaskSetReqConsenChkptId(pTask, taosGetTimestampMs());
  streamMutexUnlock(&pTask->lock);

  if (pTask->pBackend != NULL) {
    streamFreeTaskState(pTask, p);
    pTask->pBackend = NULL;
  }
  return 0;
}

int32_t streamTaskSendCheckpointsourceRsp(SStreamTask* pTask) {
  int32_t code = 0;
  if (pTask->info.taskLevel != TASK_LEVEL__SOURCE) {
    return code;
  }

  streamMutexLock(&pTask->lock);
  SStreamTaskState p = streamTaskGetStatus(pTask);
  if (p.state == TASK_STATUS__CK) {
    code = streamTaskSendCheckpointSourceRsp(pTask);
  }
  streamMutexUnlock(&pTask->lock);

  return code;
}
