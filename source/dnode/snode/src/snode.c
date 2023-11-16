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
#include "executor.h"
#include "sndInt.h"
#include "tstream.h"
#include "tuuid.h"
#include "stream.h"

#define sndError(...)                                                     \
  do {                                                                  \
    if (sndDebugFlag & DEBUG_ERROR) {                                     \
      taosPrintLog("SND ERROR ", DEBUG_ERROR, sndDebugFlag, __VA_ARGS__); \
    }                                                                   \
  } while (0)

#define sndInfo(...)                                                     \
  do {                                                                  \
    if (sndDebugFlag & DEBUG_INFO) {                                     \
      taosPrintLog("SND INFO ", DEBUG_INFO, sndDebugFlag, __VA_ARGS__); \
    }                                                                   \
  } while (0)

#define sndDebug(...)                                               \
  do {                                                            \
    if (sndDebugFlag & DEBUG_DEBUG) {                               \
      taosPrintLog("SND ", DEBUG_DEBUG, sndDebugFlag, __VA_ARGS__); \
    }                                                             \
  } while (0)

void sndEnqueueStreamDispatch(SSnode *pSnode, SRpcMsg *pMsg) {
  char   *msgStr = pMsg->pCont;
  char   *msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);
  int32_t code = 0;

  SStreamDispatchReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, msgBody, msgLen);
  if (tDecodeStreamDispatchReq(&decoder, &req) < 0) {
    code = TSDB_CODE_MSG_DECODE_ERROR;
    tDecoderClear(&decoder);
    goto FAIL;
  }

  tDecoderClear(&decoder);

  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, req.streamId, req.taskId);
  if (pTask) {
    SRpcMsg rsp = { .info = pMsg->info, .code = 0 };
    streamProcessDispatchMsg(pTask, &req, &rsp);
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
    return;
  }

FAIL:
  if (pMsg->info.handle == NULL) return;
  SRpcMsg rsp = { .code = code, .info = pMsg->info};
  tmsgSendRsp(&rsp);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

int32_t sndExpandTask(SSnode *pSnode, SStreamTask *pTask, int64_t nextProcessVer) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__AGG && taosArrayGetSize(pTask->upstreamInfo.pList) != 0);
  int32_t code = streamTaskInit(pTask, pSnode->pMeta, &pSnode->msgCb, nextProcessVer);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  streamTaskOpenAllUpstreamInput(pTask);

  SStreamTask* pSateTask = pTask;
  SStreamTask  task = {0};
  if (pTask->info.fillHistory) {
    task.id.streamId = pTask->streamTaskId.streamId;
    task.id.taskId = pTask->streamTaskId.taskId;
    task.pMeta = pTask->pMeta;
    pSateTask = &task;
  }

  pTask->pState = streamStateOpen(pSnode->path, pSateTask, false, -1, -1);
  if (pTask->pState == NULL) {
    sndError("s-task:%s failed to open state for task", pTask->id.idStr);
    return -1;
  } else {
    sndDebug("s-task:%s state:%p", pTask->id.idStr, pTask->pState);
  }

  int32_t     numOfVgroups = (int32_t)taosArrayGetSize(pTask->upstreamInfo.pList);
  SReadHandle handle = {
      .checkpointId = pTask->chkInfo.checkpointId,
      .vnode = NULL,
      .numOfVgroups = numOfVgroups,
      .pStateBackend = pTask->pState,
      .fillHistory = pTask->info.fillHistory,
      .winRange = pTask->dataRange.window,
  };
  initStreamStateAPI(&handle.api);

  pTask->exec.pExecutor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle, SNODE_HANDLE, pTask->id.taskId);
  ASSERT(pTask->exec.pExecutor);
  qSetTaskId(pTask->exec.pExecutor, pTask->id.taskId, pTask->id.streamId);

  streamTaskResetUpstreamStageInfo(pTask);
  streamSetupScheduleTrigger(pTask);

  SCheckpointInfo* pChkInfo = &pTask->chkInfo;
  // checkpoint ver is the kept version, handled data should be the next version.
  if (pTask->chkInfo.checkpointId != 0) {
    pTask->chkInfo.nextProcessVer = pTask->chkInfo.checkpointVer + 1;
    sndInfo("s-task:%s restore from the checkpointId:%" PRId64 " ver:%" PRId64 " nextProcessVer:%" PRId64, pTask->id.idStr,
           pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer);
  } else {
    if (pTask->chkInfo.nextProcessVer == -1) {
      pTask->chkInfo.nextProcessVer = 0;
    }
  }

  char* p = NULL;
  streamTaskGetStatus(pTask, &p);

  sndInfo("snode:%d expand stream task, s-task:%s, checkpointId:%" PRId64 " checkpointVer:%" PRId64
        " nextProcessVer:%" PRId64 " child id:%d, level:%d, status:%s fill-history:%d, trigger:%" PRId64 " ms",
        SNODE_HANDLE, pTask->id.idStr, pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer,
        pTask->info.selfChildId, pTask->info.taskLevel, p, pTask->info.fillHistory, pTask->info.triggerParam);

  return 0;
}

int32_t sndStartStreamTasks(SSnode* pSnode) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      vgId = SNODE_HANDLE;
  SStreamMeta* pMeta = pSnode->pMeta;

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  sndDebug("vgId:%d start to check all %d stream task(s) downstream status", vgId, numOfTasks);
  if (numOfTasks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SArray* pTaskList = NULL;
  streamMetaWLock(pMeta);
  pTaskList = taosArrayDup(pMeta->pTaskList, NULL);
  taosHashClear(pMeta->startInfo.pReadyTaskSet);
  taosHashClear(pMeta->startInfo.pFailedTaskSet);
  pMeta->startInfo.startTs = taosGetTimestampMs();
  streamMetaWUnLock(pMeta);

  // broadcast the check downstream tasks msg
  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pTaskList, i);
    SStreamTask*   pTask = streamMetaAcquireTask(pMeta, pTaskId->streamId, pTaskId->taskId);
    if (pTask == NULL) {
      continue;
    }

    // fill-history task can only be launched by related stream tasks.
    if (pTask->info.fillHistory == 1) {
      streamMetaReleaseTask(pMeta, pTask);
      continue;
    }

    if (pTask->status.downstreamReady == 1) {
      if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
        sndDebug("s-task:%s downstream ready, no need to check downstream, check only related fill-history task",
                 pTask->id.idStr);
        streamLaunchFillHistoryTask(pTask);
      }

      streamMetaUpdateTaskDownstreamStatus(pTask, pTask->execInfo.init, pTask->execInfo.start, true);
      streamMetaReleaseTask(pMeta, pTask);
      continue;
    }

    EStreamTaskEvent event = (HAS_RELATED_FILLHISTORY_TASK(pTask)) ? TASK_EVENT_INIT_STREAM_SCANHIST : TASK_EVENT_INIT;
    int32_t ret = streamTaskHandleEvent(pTask->status.pSM, event);
    if (ret != TSDB_CODE_SUCCESS) {
      code = ret;
    }

    streamMetaReleaseTask(pMeta, pTask);
  }

  taosArrayDestroy(pTaskList);
  return code;
}

int32_t sndResetStreamTaskStatus(SSnode* pSnode) {
  SStreamMeta* pMeta = pSnode->pMeta;
  int32_t      vgId = pMeta->vgId;
  int32_t      numOfTasks = taosArrayGetSize(pMeta->pTaskList);

  sndDebug("vgId:%d reset all %d stream task(s) status to be uninit", vgId, numOfTasks);
  if (numOfTasks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pMeta->pTaskList, i);

    STaskId id = {.streamId = pTaskId->streamId, .taskId = pTaskId->taskId};
    SStreamTask** pTask = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
    streamTaskResetStatus(*pTask);
  }

  return 0;
}

int32_t sndRestartStreamTasks(SSnode* pSnode) {
  SStreamMeta* pMeta = pSnode->pMeta;
  int32_t      vgId = pMeta->vgId;
  int32_t      code = 0;
  int64_t      st = taosGetTimestampMs();

  while(1) {
    int32_t startVal = atomic_val_compare_exchange_32(&pMeta->startInfo.taskStarting, 0, 1);
    if (startVal == 0) {
      break;
    }

    sndDebug("vgId:%d in start stream tasks procedure, wait for 500ms and recheck", vgId);
    taosMsleep(500);
  }

  terrno = 0;
  sndInfo("vgId:%d tasks are all updated and stopped, restart all tasks, triggered by transId:%d", vgId,
          pMeta->updateInfo.transId);

  while (streamMetaTaskInTimer(pMeta)) {
    sndDebug("vgId:%d some tasks in timer, wait for 100ms and recheck", pMeta->vgId);
    taosMsleep(100);
  }

  streamMetaWLock(pMeta);

  code = streamMetaReopen(pMeta);
  if (code != TSDB_CODE_SUCCESS) {
    sndError("vgId:%d failed to reopen stream meta", vgId);
    streamMetaWUnLock(pMeta);
    code = terrno;
    return code;
  }

  streamMetaInitBackend(pMeta);
  int64_t el = taosGetTimestampMs() - st;

  sndInfo("vgId:%d close&reload state elapsed time:%.3fs", vgId, el/1000.);

  code = streamMetaLoadAllTasks(pMeta);
  if (code != TSDB_CODE_SUCCESS) {
    sndError("vgId:%d failed to load stream tasks, code:%s", vgId, tstrerror(terrno));
    streamMetaWUnLock(pMeta);
    code = terrno;
    return code;
  }
  sndInfo("vgId:%d restart all stream tasks after all tasks being updated", vgId);
  sndResetStreamTaskStatus(pSnode);

  streamMetaWUnLock(pMeta);
  sndStartStreamTasks(pSnode);

  code = terrno;
  return code;
}

SSnode *sndOpen(const char *path, const SSnodeOpt *pOption) {
  SSnode *pSnode = taosMemoryCalloc(1, sizeof(SSnode));
  if (pSnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  pSnode->path = taosStrdup(path);
  if (pSnode->path == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  pSnode->msgCb = pOption->msgCb;
  pSnode->pMeta = streamMetaOpen(path, pSnode, (FTaskExpand *)sndExpandTask, SNODE_HANDLE, taosGetTimestampMs());
  if (pSnode->pMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  if (streamMetaLoadAllTasks(pSnode->pMeta) < 0) {
    goto FAIL;
  }

  stopRsync();
  startRsync();

  return pSnode;

FAIL:
  taosMemoryFree(pSnode->path);
  taosMemoryFree(pSnode);
  return NULL;
}

int32_t sndInit(SSnode * pSnode) {
  sndResetStreamTaskStatus(pSnode);
  sndStartStreamTasks(pSnode);
  return 0;
}

void sndClose(SSnode *pSnode) {
  streamMetaNotifyClose(pSnode->pMeta);
  streamMetaCommit(pSnode->pMeta);
  streamMetaClose(pSnode->pMeta);
  taosMemoryFree(pSnode->path);
  taosMemoryFree(pSnode);
}

int32_t sndGetLoad(SSnode *pSnode, SSnodeLoad *pLoad) { return 0; }

int32_t sndStartStreamTaskAsync(SSnode* pSnode, bool restart) {
  SStreamMeta* pMeta = pSnode->pMeta;
  int32_t      vgId = pMeta->vgId;

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  if (numOfTasks == 0) {
    sndDebug("vgId:%d no stream tasks existed to run", vgId);
    return 0;
  }

  SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
  if (pRunReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    sndError("vgId:%d failed to create msg to start wal scanning to launch stream tasks, code:%s", vgId, terrstr());
    return -1;
  }

  sndDebug("vgId:%d start all %d stream task(s) async", vgId, numOfTasks);
  pRunReq->head.vgId = vgId;
  pRunReq->streamId = 0;
  pRunReq->taskId = restart? STREAM_EXEC_RESTART_ALL_TASKS_ID:STREAM_EXEC_START_ALL_TASKS_ID;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  tmsgPutToQueue(&pSnode->msgCb, STREAM_QUEUE, &msg);
  return 0;
}

int32_t sndProcessTaskDeployReq(SSnode *pSnode, char *msg, int32_t msgLen) {
  int32_t code;

  // 1.deserialize msg and build task
  SStreamTask *pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    return -1;
  }

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t *)msg, msgLen);
  code = tDecodeStreamTask(&decoder, pTask);
  if (code < 0) {
    tDecoderClear(&decoder);
    taosMemoryFree(pTask);
    return -1;
  }

  tDecoderClear(&decoder);

  ASSERT(pTask->info.taskLevel == TASK_LEVEL__AGG);

  // 2.save task
  streamMetaWLock(pSnode->pMeta);

  bool added = false;
  code = streamMetaRegisterTask(pSnode->pMeta, -1, pTask, &added);
  if (code < 0) {
    streamMetaWUnLock(pSnode->pMeta);
    return -1;
  }

  int32_t numOfTasks = streamMetaGetNumOfTasks(pSnode->pMeta);
  streamMetaWUnLock(pSnode->pMeta);

  char* p = NULL;
  streamTaskGetStatus(pTask, &p);

  sndDebug("snode:%d s-task:%s is deployed on snode and add into meta, status:%s, numOfTasks:%d", SNODE_HANDLE,
         pTask->id.idStr, p, numOfTasks);

  EStreamTaskEvent event = (HAS_RELATED_FILLHISTORY_TASK(pTask)) ? TASK_EVENT_INIT_STREAM_SCANHIST : TASK_EVENT_INIT;
  streamTaskHandleEvent(pTask->status.pSM, event);
  return 0;
}

int32_t sndProcessTaskDropReq(SSnode *pSnode, char *msg, int32_t msgLen) {
  SVDropStreamTaskReq *pReq = (SVDropStreamTaskReq *)msg;
  sndDebug("snode:%d receive msg to drop stream task:0x%x", pSnode->pMeta->vgId, pReq->taskId);
  streamMetaUnregisterTask(pSnode->pMeta, pReq->streamId, pReq->taskId);

  // commit the update
  streamMetaWLock(pSnode->pMeta);
  int32_t numOfTasks = streamMetaGetNumOfTasks(pSnode->pMeta);
  sndDebug("vgId:%d task:0x%x dropped, remain tasks:%d", pSnode->pMeta->vgId, pReq->taskId, numOfTasks);

  if (streamMetaCommit(pSnode->pMeta) < 0) {
    // persist to disk
  }
  streamMetaWUnLock(pSnode->pMeta);
  return 0;
}

int32_t sndProcessTaskRunReq(SSnode *pSnode, SRpcMsg *pMsg) {
  SStreamTaskRunReq *pReq = pMsg->pCont;

  int32_t taskId = pReq->taskId;

  if (taskId == STREAM_EXEC_START_ALL_TASKS_ID) {
    sndStartStreamTasks(pSnode);
    return 0;
  } else if (taskId == STREAM_EXEC_RESTART_ALL_TASKS_ID) {
    sndRestartStreamTasks(pSnode);
    return 0;
  }

  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, pReq->streamId, pReq->taskId);
  if (pTask) {
    streamExecTask(pTask);
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    return 0;
  } else {
    return -1;
  }
}

int32_t sndProcessTaskDispatchReq(SSnode *pSnode, SRpcMsg *pMsg, bool exec) {
  char              *msgStr = pMsg->pCont;
  char              *msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t            msgLen = pMsg->contLen - sizeof(SMsgHead);
  SStreamDispatchReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, (uint8_t *)msgBody, msgLen);
  tDecodeStreamDispatchReq(&decoder, &req);

  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, req.streamId, req.taskId);
  if (pTask) {
    SRpcMsg rsp = {.info = pMsg->info, .code = 0};
    streamProcessDispatchMsg(pTask, &req, &rsp);
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    return 0;
  } else {
    return -1;
  }
}

int32_t sndProcessTaskRetrieveReq(SSnode *pSnode, SRpcMsg *pMsg) {
  char              *msgStr = pMsg->pCont;
  char              *msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t            msgLen = pMsg->contLen - sizeof(SMsgHead);
  SStreamRetrieveReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, msgBody, msgLen);
  tDecodeStreamRetrieveReq(&decoder, &req);
  tDecoderClear(&decoder);
  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, req.streamId, req.dstTaskId);

  if (pTask) {
    SRpcMsg rsp = {.info = pMsg->info, .code = 0};
    streamProcessRetrieveReq(pTask, &req, &rsp);
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    tDeleteStreamRetrieveReq(&req);
    return 0;
  } else {
    return -1;
  }
}

int32_t sndProcessTaskDispatchRsp(SSnode *pSnode, SRpcMsg *pMsg) {
  SStreamDispatchRsp *pRsp = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));

  pRsp->upstreamTaskId = htonl(pRsp->upstreamTaskId);
  pRsp->streamId = htobe64(pRsp->streamId);
  pRsp->downstreamTaskId = htonl(pRsp->downstreamTaskId);
  pRsp->downstreamNodeId = htonl(pRsp->downstreamNodeId);
  pRsp->stage = htobe64(pRsp->stage);
  pRsp->msgId = htonl(pRsp->msgId);

  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, pRsp->streamId, pRsp->upstreamTaskId);
  if (pTask) {
    streamProcessDispatchRsp(pTask, pRsp, pMsg->code);
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    return 0;
  } else {
    return -1;
  }
}

int32_t sndProcessTaskRetrieveRsp(SSnode *pSnode, SRpcMsg *pMsg) {
  //
  return 0;
}

int32_t sndProcessTaskScanHistoryFinishReq(SSnode *pSnode, SRpcMsg *pMsg) {
  char   *msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  // deserialize
  SStreamScanHistoryFinishReq req;

  SDecoder decoder;
  tDecoderInit(&decoder, msg, msgLen);
  tDecodeStreamScanHistoryFinishReq(&decoder, &req);
  tDecoderClear(&decoder);

  // find task
  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, req.streamId, req.downstreamTaskId);
  if (pTask == NULL) {
    return -1;
  }
  // do process request
  if (streamProcessScanHistoryFinishReq(pTask, &req, &pMsg->info) < 0) {
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    return -1;
  }

  streamMetaReleaseTask(pSnode->pMeta, pTask);
  return 0;
}

int32_t sndProcessTaskScanHistoryFinishRsp(SSnode *pSnode, SRpcMsg *pMsg) {
  char*   msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  // deserialize
  SStreamCompleteHistoryMsg req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  tDecodeCompleteHistoryDataMsg(&decoder, &req);
  tDecoderClear(&decoder);

  SStreamTask* pTask = streamMetaAcquireTask(pSnode->pMeta, req.streamId, req.upstreamTaskId);
  if (pTask == NULL) {
    sndError("vgId:%d process scan history finish rsp, failed to find task:0x%x, it may be destroyed",
           pSnode->pMeta->vgId, req.upstreamTaskId);
    return -1;
  }

  int32_t remain = atomic_sub_fetch_32(&pTask->notReadyTasks, 1);
  if (remain > 0) {
    sndDebug("s-task:%s scan-history finish rsp received from downstream task:0x%x, unfinished remain:%d",
            pTask->id.idStr, req.downstreamId, remain);
  } else {
    sndDebug(
        "s-task:%s scan-history finish rsp received from downstream task:0x%x, all downstream tasks rsp scan-history "
        "completed msg",
        pTask->id.idStr, req.downstreamId);
    streamProcessScanHistoryFinishRsp(pTask);
  }

  streamMetaReleaseTask(pSnode->pMeta, pTask);
  return 0;
}

// downstream task has complete the stream task checkpoint procedure, let's start the handle the rsp by execute task
int32_t sndProcessTaskCheckpointReadyMsg(SSnode *pSnode, SRpcMsg* pMsg) {
  SStreamMeta* pMeta = pSnode->pMeta;
  char*        msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t      len = pMsg->contLen - sizeof(SMsgHead);
  int32_t      code = 0;

  SStreamCheckpointReadyMsg req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, len);
  if (tDecodeStreamCheckpointReadyMsg(&decoder, &req) < 0) {
    code = TSDB_CODE_MSG_DECODE_ERROR;
    tDecoderClear(&decoder);
    return code;
  }
  tDecoderClear(&decoder);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, req.streamId, req.upstreamTaskId);
  if (pTask == NULL) {
    sndError("vgId:%d failed to find s-task:0x%x, it may have been destroyed already", pMeta->vgId, req.downstreamTaskId);
    return code;
  }

  sndDebug("snode vgId:%d s-task:%s received the checkpoint ready msg from task:0x%x (vgId:%d), handle it", pMeta->vgId,
          pTask->id.idStr, req.downstreamTaskId, req.downstreamNodeId);

  streamProcessCheckpointReadyMsg(pTask);
  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t sndProcessStreamTaskCheckReq(SSnode *pSnode, SRpcMsg *pMsg) {
  char   *msgStr = pMsg->pCont;
  char   *msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  SStreamTaskCheckReq req;
  SDecoder            decoder;

  tDecoderInit(&decoder, (uint8_t *)msgBody, msgLen);
  tDecodeStreamTaskCheckReq(&decoder, &req);
  tDecoderClear(&decoder);

  int32_t taskId = req.downstreamTaskId;

  SStreamTaskCheckRsp rsp = {
      .reqId = req.reqId,
      .streamId = req.streamId,
      .childId = req.childId,
      .downstreamNodeId = req.downstreamNodeId,
      .downstreamTaskId = req.downstreamTaskId,
      .upstreamNodeId = req.upstreamNodeId,
      .upstreamTaskId = req.upstreamTaskId,
  };

  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, req.streamId, taskId);

  if (pTask != NULL) {
    rsp.status = streamTaskCheckStatus(pTask, req.upstreamTaskId, req.upstreamNodeId, req.stage, &rsp.oldStage);
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    char* p = NULL;
    streamTaskGetStatus(pTask, &p);
    sndDebug("s-task:%s status:%s, recv task check req(reqId:0x%" PRIx64 ") task:0x%x (vgId:%d), ready:%d",
            pTask->id.idStr, p, rsp.reqId, rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);
  } else {
    rsp.status = TASK_DOWNSTREAM_NOT_READY;
    sndDebug("recv task check(taskId:0x%x not built yet) req(reqId:0x%" PRIx64 ") from task:0x%x (vgId:%d), rsp status %d",
           taskId, rsp.reqId, rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);
  }

  SEncoder encoder;
  int32_t  code;
  int32_t  len;

  tEncodeSize(tEncodeStreamTaskCheckRsp, &rsp, len, code);
  if (code < 0) {
    sndError("vgId:%d failed to encode task check rsp, task:0x%x", pSnode->pMeta->vgId, taskId);
    return -1;
  }

  void *buf = rpcMallocCont(sizeof(SMsgHead) + len);
  ((SMsgHead *)buf)->vgId = htonl(req.upstreamNodeId);

  void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncoderInit(&encoder, (uint8_t *)abuf, len);
  tEncodeStreamTaskCheckRsp(&encoder, &rsp);
  tEncoderClear(&encoder);

  SRpcMsg rspMsg = {.code = 0, .pCont = buf, .contLen = sizeof(SMsgHead) + len, .info = pMsg->info};

  tmsgSendRsp(&rspMsg);
  return 0;
}

int32_t sndProcessStreamTaskCheckRsp(SSnode* pSnode, SRpcMsg* pMsg) {
  char* pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t len = pMsg->contLen - sizeof(SMsgHead);

  int32_t             code;
  SStreamTaskCheckRsp rsp;

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)pReq, len);
  code = tDecodeStreamTaskCheckRsp(&decoder, &rsp);

  if (code < 0) {
    tDecoderClear(&decoder);
    return -1;
  }

  tDecoderClear(&decoder);
  sndDebug("tq task:0x%x (vgId:%d) recv check rsp(reqId:0x%" PRIx64 ") from 0x%x (vgId:%d) status %d",
          rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.reqId, rsp.downstreamTaskId, rsp.downstreamNodeId, rsp.status);

  SStreamTask* pTask = streamMetaAcquireTask(pSnode->pMeta, rsp.streamId, rsp.upstreamTaskId);
  if (pTask == NULL) {
    sndError("tq failed to locate the stream task:0x%x (vgId:%d), it may have been destroyed", rsp.upstreamTaskId,
            pSnode->pMeta->vgId);
    return -1;
  }

  code = streamProcessCheckRsp(pTask, &rsp);
  streamMetaReleaseTask(pSnode->pMeta, pTask);
  return code;
}

int32_t sndProcessTaskUpdateReq(SSnode* pSnode, SRpcMsg* pMsg) {
  SStreamMeta* pMeta = pSnode->pMeta;
  int32_t      vgId = SNODE_HANDLE;
  char*        msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t      len = pMsg->contLen - sizeof(SMsgHead);
  SRpcMsg      rsp = {.info = pMsg->info, .code = TSDB_CODE_SUCCESS};

  SStreamTaskNodeUpdateMsg req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, len);
  if (tDecodeStreamTaskUpdateMsg(&decoder, &req) < 0) {
    rsp.code = TSDB_CODE_MSG_DECODE_ERROR;
    sndError("vgId:%d failed to decode task update msg, code:%s", vgId, tstrerror(rsp.code));
    tDecoderClear(&decoder);
    return rsp.code;
  }

  tDecoderClear(&decoder);

  // update the nodeEpset when it exists
  streamMetaWLock(pMeta);

  // the task epset may be updated again and again, when replaying the WAL, the task may be in stop status.
  STaskId id = {.streamId = req.streamId, .taskId = req.taskId};
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (ppTask == NULL || *ppTask == NULL) {
    sndError("vgId:%d failed to acquire task:0x%x when handling update, it may have been dropped already", pMeta->vgId,
            req.taskId);
    rsp.code = TSDB_CODE_SUCCESS;
    streamMetaWUnLock(pMeta);

    taosArrayDestroy(req.pNodeList);
    return rsp.code;
  }

  SStreamTask* pTask = *ppTask;

  if (pMeta->updateInfo.transId != req.transId) {
    pMeta->updateInfo.transId = req.transId;
    sndInfo("s-task:%s receive new trans to update nodeEp msg from mnode, transId:%d", pTask->id.idStr, req.transId);
    // info needs to be kept till the new trans to update the nodeEp arrived.
    taosHashClear(pMeta->updateInfo.pTasks);
  } else {
    sndDebug("s-task:%s recv trans to update nodeEp from mnode, transId:%d", pTask->id.idStr, req.transId);
  }

  STaskUpdateEntry entry = {.streamId = req.streamId, .taskId = req.taskId, .transId = req.transId};
  void* exist = taosHashGet(pMeta->updateInfo.pTasks, &entry, sizeof(STaskUpdateEntry));
  if (exist != NULL) {
    sndDebug("s-task:%s (vgId:%d) already update in trans:%d, discard the nodeEp update msg", pTask->id.idStr, vgId,
            req.transId);
    rsp.code = TSDB_CODE_SUCCESS;
    streamMetaWUnLock(pMeta);
    taosArrayDestroy(req.pNodeList);
    return rsp.code;
  }

  streamMetaWUnLock(pMeta);

  // the following two functions should not be executed within the scope of meta lock to avoid deadlock
  streamTaskUpdateEpsetInfo(pTask, req.pNodeList);
  streamTaskResetStatus(pTask);

  // continue after lock the meta again
  streamMetaWLock(pMeta);

  SStreamTask** ppHTask = NULL;
  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    ppHTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &pTask->hTaskInfo.id, sizeof(pTask->hTaskInfo.id));
    if (ppHTask == NULL || *ppHTask == NULL) {
      sndError("vgId:%d failed to acquire fill-history task:0x%x when handling update, it may have been dropped already",
              pMeta->vgId, req.taskId);
      CLEAR_RELATED_FILLHISTORY_TASK(pTask);
    } else {
      sndDebug("s-task:%s fill-history task update nodeEp along with stream task", (*ppHTask)->id.idStr);
      streamTaskUpdateEpsetInfo(*ppHTask, req.pNodeList);
    }
  }

  {
    streamMetaSaveTask(pMeta, pTask);
    if (ppHTask != NULL) {
      streamMetaSaveTask(pMeta, *ppHTask);
    }

    if (streamMetaCommit(pMeta) < 0) {
      //     persist to disk
    }
  }

  streamTaskStop(pTask);

  // keep the already handled info
  taosHashPut(pMeta->updateInfo.pTasks, &entry, sizeof(entry), NULL, 0);

  if (ppHTask != NULL) {
    streamTaskStop(*ppHTask);
    sndDebug("s-task:%s task nodeEp update completed, streamTask and related fill-history task closed", pTask->id.idStr);
    taosHashPut(pMeta->updateInfo.pTasks, &(*ppHTask)->id, sizeof(pTask->id), NULL, 0);
  } else {
    sndDebug("s-task:%s task nodeEp update completed, streamTask closed", pTask->id.idStr);
  }

  rsp.code = 0;

  // possibly only handle the stream task.
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
  int32_t updateTasks = taosHashGetSize(pMeta->updateInfo.pTasks);

  pMeta->startInfo.tasksWillRestart = 1;

  if (updateTasks < numOfTasks) {
    sndDebug("vgId:%d closed tasks:%d, unclosed:%d, all tasks will be started when nodeEp update completed", vgId,
            updateTasks, (numOfTasks - updateTasks));
    streamMetaWUnLock(pMeta);
  } else {
      sndDebug("vgId:%d all %d task(s) nodeEp updated and closed", vgId, numOfTasks);
#if 1
      sndStartStreamTaskAsync(pSnode, true);
      streamMetaWUnLock(pMeta);
#else
      streamMetaWUnLock(pMeta);

      // For debug purpose.
      // the following procedure consume many CPU resource, result in the re-election of leader
      // with high probability. So we employ it as a test case for the stream processing framework, with
      // checkpoint/restart/nodeUpdate etc.
      while(1) {
        int32_t startVal = atomic_val_compare_exchange_32(&pMeta->startInfo.taskStarting, 0, 1);
        if (startVal == 0) {
          break;
        }

        tqDebug("vgId:%d in start stream tasks procedure, wait for 500ms and recheck", vgId);
        taosMsleep(500);
      }

      while (streamMetaTaskInTimer(pMeta)) {
        tqDebug("vgId:%d some tasks in timer, wait for 100ms and recheck", pMeta->vgId);
        taosMsleep(100);
      }

      streamMetaWLock(pMeta);

      int32_t code = streamMetaReopen(pMeta);
      if (code != 0) {
        tqError("vgId:%d failed to reopen stream meta", vgId);
        streamMetaWUnLock(pMeta);
        taosArrayDestroy(req.pNodeList);
        return -1;
      }

      if (streamMetaLoadAllTasks(pTq->pStreamMeta) < 0) {
        tqError("vgId:%d failed to load stream tasks", vgId);
        streamMetaWUnLock(pMeta);
        taosArrayDestroy(req.pNodeList);
        return -1;
      }

      if (vnodeIsRoleLeader(pTq->pVnode) && !tsDisableStream) {
        tqInfo("vgId:%d start all stream tasks after all being updated", vgId);
        tqResetStreamTaskStatus(pTq);
        tqStartStreamTaskAsync(pTq, false);
      } else {
        tqInfo("vgId:%d, follower node not start stream tasks", vgId);
      }
      streamMetaWUnLock(pMeta);
#endif
  }

  taosArrayDestroy(req.pNodeList);
  return rsp.code;
}

int32_t sndProcessStreamMsg(SSnode *pSnode, SRpcMsg *pMsg) {
  switch (pMsg->msgType) {
    case TDMT_STREAM_TASK_RUN:
      return sndProcessTaskRunReq(pSnode, pMsg);
    case TDMT_STREAM_TASK_DISPATCH:
      return sndProcessTaskDispatchReq(pSnode, pMsg, true);
    case TDMT_STREAM_TASK_DISPATCH_RSP:
      return sndProcessTaskDispatchRsp(pSnode, pMsg);
    case TDMT_STREAM_RETRIEVE:
      return sndProcessTaskRetrieveReq(pSnode, pMsg);
    case TDMT_STREAM_RETRIEVE_RSP:
      return sndProcessTaskRetrieveRsp(pSnode, pMsg);
    case TDMT_VND_STREAM_SCAN_HISTORY_FINISH:
      return sndProcessTaskScanHistoryFinishReq(pSnode, pMsg);
    case TDMT_VND_STREAM_SCAN_HISTORY_FINISH_RSP:
      return sndProcessTaskScanHistoryFinishRsp(pSnode, pMsg);
    case TDMT_VND_STREAM_TASK_CHECK:
      return sndProcessStreamTaskCheckReq(pSnode, pMsg);
    case TDMT_VND_STREAM_TASK_CHECK_RSP:
      return sndProcessStreamTaskCheckRsp(pSnode, pMsg);
    case TDMT_STREAM_TASK_CHECKPOINT_READY:
      return sndProcessTaskCheckpointReadyMsg(pSnode, pMsg);
    default:
      ASSERT(0);
  }
  return 0;
}

int32_t sndProcessWriteMsg(SSnode *pSnode, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  switch (pMsg->msgType) {
    case TDMT_STREAM_TASK_DEPLOY: {
      void   *pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
      int32_t len = pMsg->contLen - sizeof(SMsgHead);
      return sndProcessTaskDeployReq(pSnode, pReq, len);
    }

    case TDMT_STREAM_TASK_DROP:
      return sndProcessTaskDropReq(pSnode, pMsg->pCont, pMsg->contLen);
    case TDMT_VND_STREAM_TASK_UPDATE:
      sndProcessTaskUpdateReq(pSnode, pMsg);
      break;
    default:
      ASSERT(0);
  }
  return 0;
}