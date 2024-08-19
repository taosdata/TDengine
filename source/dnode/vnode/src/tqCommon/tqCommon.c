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

#include "tmsgcb.h"
#include "tq.h"
#include "tstream.h"

typedef struct SMStreamCheckpointReadyRspMsg {
  SMsgHead head;
  int64_t  streamId;
  int32_t  upstreamTaskId;
  int32_t  upstreamNodeId;
  int32_t  downstreamTaskId;
  int32_t  downstreamNodeId;
  int64_t  checkpointId;
  int32_t  transId;
} SMStreamCheckpointReadyRspMsg;

static int32_t doProcessDummyRspMsg(SStreamMeta* pMeta, SRpcMsg* pMsg);

int32_t tqExpandStreamTask(SStreamTask* pTask) {
  SStreamMeta* pMeta = pTask->pMeta;
  int32_t      vgId = pMeta->vgId;
  int64_t      st = taosGetTimestampMs();
  int64_t      streamId = 0;
  int32_t      taskId = 0;

  tqDebug("s-task:%s vgId:%d start to expand stream task", pTask->id.idStr, vgId);

  if (pTask->info.fillHistory) {
    streamId = pTask->streamTaskId.streamId;
    taskId = pTask->streamTaskId.taskId;
  } else {
    streamId = pTask->id.streamId;
    taskId = pTask->id.taskId;
  }

  // sink task does not need the pState
  if (pTask->info.taskLevel != TASK_LEVEL__SINK) {
    pTask->pState = streamStateOpen(pMeta->path, pTask, streamId, taskId);
    if (pTask->pState == NULL) {
      tqError("s-task:%s (vgId:%d) failed to open state for task, expand task failed", pTask->id.idStr, vgId);
      return -1;
    } else {
      tqDebug("s-task:%s state:%p", pTask->id.idStr, pTask->pState);
    }
  }

  SReadHandle handle = {
      .checkpointId = pTask->chkInfo.checkpointId,
      .pStateBackend = pTask->pState,
      .fillHistory = pTask->info.fillHistory,
      .winRange = pTask->dataRange.window,
  };

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    handle.vnode = ((STQ*)pMeta->ahandle)->pVnode;
    handle.initTqReader = 1;
  } else if (pTask->info.taskLevel == TASK_LEVEL__AGG) {
    handle.numOfVgroups = (int32_t)taosArrayGetSize(pTask->upstreamInfo.pList);
  }

  initStorageAPI(&handle.api);

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE || pTask->info.taskLevel == TASK_LEVEL__AGG) {
    pTask->exec.pExecutor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle, vgId, pTask->id.taskId);
    if (pTask->exec.pExecutor == NULL) {
      tqError("s-task:%s failed to create exec taskInfo, failed to expand task", pTask->id.idStr);
      return -1;
    }
    qSetTaskId(pTask->exec.pExecutor, pTask->id.taskId, pTask->id.streamId);
  }

  double el = (taosGetTimestampMs() - st) / 1000.0;
  tqDebug("s-task:%s vgId:%d expand stream task completed, elapsed time:%.2fsec", pTask->id.idStr, vgId, el);

  return TSDB_CODE_SUCCESS;
}

void tqSetRestoreVersionInfo(SStreamTask* pTask) {
  SCheckpointInfo* pChkInfo = &pTask->chkInfo;

  // checkpoint ver is the kept version, handled data should be the next version.
  if (pChkInfo->checkpointId != 0) {
    pChkInfo->nextProcessVer = pChkInfo->checkpointVer + 1;
    pChkInfo->processedVer = pChkInfo->checkpointVer;
    pTask->execInfo.startCheckpointId = pChkInfo->checkpointId;

    tqInfo("s-task:%s restore from the checkpointId:%" PRId64 " ver:%" PRId64 " currentVer:%" PRId64, pTask->id.idStr,
           pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer);
  }

  pTask->execInfo.startCheckpointVer = pChkInfo->nextProcessVer;
}

int32_t tqStreamTaskStartAsync(SStreamMeta* pMeta, SMsgCb* cb, bool restart) {
  int32_t vgId = pMeta->vgId;
  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  if (numOfTasks == 0) {
    tqDebug("vgId:%d no stream tasks existed to run", vgId);
    return 0;
  }

  tqDebug("vgId:%d start all %d stream task(s) async", vgId, numOfTasks);

  int32_t type = restart ? STREAM_EXEC_T_RESTART_ALL_TASKS : STREAM_EXEC_T_START_ALL_TASKS;
  return streamTaskSchedTask(cb, vgId, 0, 0, type);
}

int32_t tqStreamStartOneTaskAsync(SStreamMeta* pMeta, SMsgCb* cb, int64_t streamId, int32_t taskId) {
  int32_t vgId = pMeta->vgId;

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  if (numOfTasks == 0) {
    tqDebug("vgId:%d no stream tasks existed to run", vgId);
    return 0;
  }

  tqDebug("vgId:%d start task:0x%x async", vgId, taskId);
  return streamTaskSchedTask(cb, vgId, streamId, taskId, STREAM_EXEC_T_START_ONE_TASK);
}

int32_t tqStreamTaskProcessUpdateReq(SStreamMeta* pMeta, SMsgCb* cb, SRpcMsg* pMsg, bool restored) {
  int32_t vgId = pMeta->vgId;
  char*   msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t len = pMsg->contLen - sizeof(SMsgHead);
  SRpcMsg rsp = {.info = pMsg->info, .code = TSDB_CODE_SUCCESS};
  int64_t st = taosGetTimestampMs();
  bool    updated = false;

  SStreamTaskNodeUpdateMsg req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, len);
  if (tDecodeStreamTaskUpdateMsg(&decoder, &req) < 0) {
    rsp.code = TSDB_CODE_MSG_DECODE_ERROR;
    tqError("vgId:%d failed to decode task update msg, code:%s", vgId, tstrerror(rsp.code));
    tDecoderClear(&decoder);
    return rsp.code;
  }

  tDecoderClear(&decoder);

  // update the nodeEpset when it exists
  streamMetaWLock(pMeta);

  // the task epset may be updated again and again, when replaying the WAL, the task may be in stop status.
  STaskId       id = {.streamId = req.streamId, .taskId = req.taskId};
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (ppTask == NULL || *ppTask == NULL) {
    tqError("vgId:%d failed to acquire task:0x%x when handling update task epset, it may have been dropped", vgId,
            req.taskId);
    rsp.code = TSDB_CODE_SUCCESS;
    streamMetaWUnLock(pMeta);
    taosArrayDestroy(req.pNodeList);
    return rsp.code;
  }

  SStreamTask* pTask = *ppTask;
  const char*  idstr = pTask->id.idStr;

  if (pMeta->updateInfo.transId == -1) { // info needs to be kept till the new trans to update the nodeEp arrived.
    streamMetaInitUpdateTaskList(pMeta, req.transId);
  }

  if (pMeta->updateInfo.transId != req.transId) {
    if (req.transId < pMeta->updateInfo.transId) {
      tqError("s-task:%s vgId:%d disorder update nodeEp msg recv, discarded, newest transId:%d, recv:%d", idstr, vgId,
              pMeta->updateInfo.transId, req.transId);
      rsp.code = TSDB_CODE_SUCCESS;
      streamMetaWUnLock(pMeta);

      taosArrayDestroy(req.pNodeList);
      return rsp.code;
    } else {
      tqInfo("s-task:%s vgId:%d receive new trans to update nodeEp msg from mnode, transId:%d, prev transId:%d", idstr,
             vgId, req.transId, pMeta->updateInfo.transId);
      // info needs to be kept till the new trans to update the nodeEp arrived.
      streamMetaInitUpdateTaskList(pMeta, req.transId);
    }
  } else {
    tqDebug("s-task:%s vgId:%d recv trans to update nodeEp from mnode, transId:%d", idstr, vgId, req.transId);
  }

  // duplicate update epset msg received, discard this redundant message
  STaskUpdateEntry entry = {.streamId = req.streamId, .taskId = req.taskId, .transId = req.transId};

  void* pReqTask = taosHashGet(pMeta->updateInfo.pTasks, &entry, sizeof(STaskUpdateEntry));
  if (pReqTask != NULL) {
    tqDebug("s-task:%s (vgId:%d) already update in transId:%d, discard the nodeEp update msg", idstr, vgId,
            req.transId);
    rsp.code = TSDB_CODE_SUCCESS;
    streamMetaWUnLock(pMeta);
    taosArrayDestroy(req.pNodeList);
    return rsp.code;
  }

  updated = streamTaskUpdateEpsetInfo(pTask, req.pNodeList);
  streamTaskResetStatus(pTask);

  streamTaskStopMonitorCheckRsp(&pTask->taskCheckInfo, pTask->id.idStr);

  SStreamTask** ppHTask = NULL;
  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    ppHTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &pTask->hTaskInfo.id, sizeof(pTask->hTaskInfo.id));
    if (ppHTask == NULL || *ppHTask == NULL) {
      tqError(
          "vgId:%d failed to acquire fill-history task:0x%x when handling update, may have been dropped already, rel "
          "stream task:0x%x",
          vgId, (uint32_t)pTask->hTaskInfo.id.taskId, req.taskId);
      CLEAR_RELATED_FILLHISTORY_TASK(pTask);
    } else {
      tqDebug("s-task:%s fill-history task update nodeEp along with stream task", (*ppHTask)->id.idStr);
      bool updateEpSet = streamTaskUpdateEpsetInfo(*ppHTask, req.pNodeList);
      if (updateEpSet) {
        updated = updateEpSet;
      }

      streamTaskResetStatus(*ppHTask);
      streamTaskStopMonitorCheckRsp(&(*ppHTask)->taskCheckInfo, (*ppHTask)->id.idStr);
    }
  }

  // save
  if (updated) {
    tqDebug("s-task:%s vgId:%d save task after update epset, and stop task", idstr, vgId);
    streamMetaSaveTask(pMeta, pTask);
    if (ppHTask != NULL) {
      streamMetaSaveTask(pMeta, *ppHTask);
    }
  } else {
    tqDebug("s-task:%s vgId:%d not save task since not update epset actually, stop task", idstr, vgId);
  }

  // stop
  streamTaskStop(pTask);
  if (ppHTask != NULL) {
    streamTaskStop(*ppHTask);
  }

  // keep info
  streamMetaAddIntoUpdateTaskList(pMeta, pTask, (ppHTask != NULL) ? (*ppHTask) : NULL, req.transId, st);

  rsp.code = 0;

  // possibly only handle the stream task.
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
  int32_t updateTasks = taosHashGetSize(pMeta->updateInfo.pTasks);

  pMeta->startInfo.tasksWillRestart = 1;

  if (updateTasks < numOfTasks) {
    tqDebug("vgId:%d closed tasks:%d, unclosed:%d, all tasks will be started when nodeEp update completed", vgId,
            updateTasks, (numOfTasks - updateTasks));
  } else {
    if (streamMetaCommit(pMeta) < 0) {
      //     persist to disk
    }

    streamMetaClearUpdateTaskList(pMeta);

    if (!restored) {
      tqDebug("vgId:%d vnode restore not completed, not start the tasks, clear the start after nodeUpdate flag", vgId);
      pMeta->startInfo.tasksWillRestart = 0;
    } else {
      tqDebug("vgId:%d all %d task(s) nodeEp updated and closed, transId:%d", vgId, numOfTasks, req.transId);
#if 0
      taosMSleep(5000);// for test purpose, to trigger the leader election
#endif
      tqStreamTaskStartAsync(pMeta, cb, true);
    }
  }

  streamMetaWUnLock(pMeta);
  taosArrayDestroy(req.pNodeList);
  return rsp.code;
}

int32_t tqStreamTaskProcessDispatchReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  char*   msgStr = pMsg->pCont;
  char*   msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  SStreamDispatchReq req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  if (tDecodeStreamDispatchReq(&decoder, &req) < 0) {
    tDecoderClear(&decoder);
    return TSDB_CODE_MSG_DECODE_ERROR;
  }
  tDecoderClear(&decoder);

  tqDebug("s-task:0x%x recv dispatch msg from 0x%x(vgId:%d)", req.taskId, req.upstreamTaskId, req.upstreamNodeId);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, req.streamId, req.taskId);
  if (pTask) {
    SRpcMsg rsp = {.info = pMsg->info, .code = 0};
    if (streamProcessDispatchMsg(pTask, &req, &rsp) != 0) {
      return -1;
    }
    tCleanupStreamDispatchReq(&req);
    streamMetaReleaseTask(pMeta, pTask);
    return 0;
  } else {
    tqError("vgId:%d failed to find task:0x%x to handle the dispatch req, it may have been destroyed already",
            pMeta->vgId, req.taskId);

    SMsgHead* pRspHead = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamDispatchRsp));
    if (pRspHead == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      tqError("s-task:0x%x send dispatch error rsp, out of memory", req.taskId);
      return -1;
    }

    pRspHead->vgId = htonl(req.upstreamNodeId);
    ASSERT(pRspHead->vgId != 0);

    SStreamDispatchRsp* pRsp = POINTER_SHIFT(pRspHead, sizeof(SMsgHead));
    pRsp->streamId = htobe64(req.streamId);
    pRsp->upstreamTaskId = htonl(req.upstreamTaskId);
    pRsp->upstreamNodeId = htonl(req.upstreamNodeId);
    pRsp->downstreamNodeId = htonl(pMeta->vgId);
    pRsp->downstreamTaskId = htonl(req.taskId);
    pRsp->msgId = htonl(req.msgId);
    pRsp->stage = htobe64(req.stage);
    pRsp->inputStatus = TASK_OUTPUT_STATUS__NORMAL;

    int32_t len = sizeof(SMsgHead) + sizeof(SStreamDispatchRsp);
    SRpcMsg rsp = {.code = TSDB_CODE_STREAM_TASK_NOT_EXIST, .info = pMsg->info, .contLen = len, .pCont = pRspHead};
    tqError("s-task:0x%x send dispatch error rsp, no task", req.taskId);

    tmsgSendRsp(&rsp);
    tCleanupStreamDispatchReq(&req);

    return 0;
  }
}

int32_t tqStreamTaskProcessDispatchRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  SStreamDispatchRsp* pRsp = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));

  int32_t vgId = pMeta->vgId;
  pRsp->upstreamNodeId = htonl(pRsp->upstreamNodeId);
  pRsp->upstreamTaskId = htonl(pRsp->upstreamTaskId);
  pRsp->streamId = htobe64(pRsp->streamId);
  pRsp->downstreamTaskId = htonl(pRsp->downstreamTaskId);
  pRsp->downstreamNodeId = htonl(pRsp->downstreamNodeId);
  pRsp->stage = htobe64(pRsp->stage);
  pRsp->msgId = htonl(pRsp->msgId);

  tqDebug("s-task:0x%x vgId:%d recv dispatch-rsp from 0x%x vgId:%d", pRsp->upstreamTaskId, pRsp->upstreamNodeId,
          pRsp->downstreamTaskId, pRsp->downstreamNodeId);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pRsp->streamId, pRsp->upstreamTaskId);
  if (pTask) {
    streamProcessDispatchRsp(pTask, pRsp, pMsg->code);
    streamMetaReleaseTask(pMeta, pTask);
    return TSDB_CODE_SUCCESS;
  } else {
    tqDebug("vgId:%d failed to handle the dispatch rsp, since find task:0x%x failed", vgId, pRsp->upstreamTaskId);
    terrno = TSDB_CODE_STREAM_TASK_NOT_EXIST;
    return terrno;
  }
}

int32_t tqStreamTaskProcessRetrieveReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  char*    msgStr = pMsg->pCont;
  char*    msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t  msgLen = pMsg->contLen - sizeof(SMsgHead);
  SDecoder decoder;

  SStreamRetrieveReq req;
  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  tDecodeStreamRetrieveReq(&decoder, &req);
  tDecoderClear(&decoder);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, req.streamId, req.dstTaskId);
  if (pTask == NULL) {
    tqError("vgId:%d process retrieve req, failed to acquire task:0x%x, it may have been dropped already", pMeta->vgId,
            req.dstTaskId);
    tCleanupStreamRetrieveReq(&req);
    return -1;
  }

  int32_t code = 0;
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    code = streamProcessRetrieveReq(pTask, &req);
  } else {
    req.srcNodeId = pTask->info.nodeId;
    req.srcTaskId = pTask->id.taskId;
    code = streamTaskBroadcastRetrieveReq(pTask, &req);
  }

  SRpcMsg rsp = {.info = pMsg->info, .code = 0};
  streamTaskSendRetrieveRsp(&req, &rsp);

  streamMetaReleaseTask(pMeta, pTask);
  tCleanupStreamRetrieveReq(&req);

  // always return success, to disable the auto rsp
  return TSDB_CODE_SUCCESS;
}

int32_t tqStreamTaskProcessCheckReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  char*   msgStr = pMsg->pCont;
  char*   msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  SStreamTaskCheckReq req;
  SStreamTaskCheckRsp rsp = {0};

  SDecoder decoder;

  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  tDecodeStreamTaskCheckReq(&decoder, &req);
  tDecoderClear(&decoder);

  streamTaskProcessCheckMsg(pMeta, &req, &rsp);
  return streamTaskSendCheckRsp(pMeta, req.upstreamNodeId, &rsp, &pMsg->info, req.upstreamTaskId);
}

int32_t tqStreamTaskProcessCheckRsp(SStreamMeta* pMeta, SRpcMsg* pMsg, bool isLeader) {
  char*   pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t len = pMsg->contLen - sizeof(SMsgHead);
  int32_t vgId = pMeta->vgId;
  int32_t code = TSDB_CODE_SUCCESS;

  SStreamTaskCheckRsp rsp;

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)pReq, len);
  code = tDecodeStreamTaskCheckRsp(&decoder, &rsp);
  if (code < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    tDecoderClear(&decoder);
    tqError("vgId:%d failed to parse check rsp msg, code:%s", vgId, tstrerror(terrno));
    return -1;
  }

  tDecoderClear(&decoder);
  tqDebug("tq task:0x%x (vgId:%d) recv check rsp(reqId:0x%" PRIx64 ") from 0x%x (vgId:%d) status %d",
          rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.reqId, rsp.downstreamTaskId, rsp.downstreamNodeId, rsp.status);

  if (!isLeader) {
    tqError("vgId:%d not leader, task:0x%x not handle the check rsp, downstream:0x%x (vgId:%d)", vgId,
            rsp.upstreamTaskId, rsp.downstreamTaskId, rsp.downstreamNodeId);
    return streamMetaAddFailedTask(pMeta, rsp.streamId, rsp.upstreamTaskId);
  }

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, rsp.streamId, rsp.upstreamTaskId);
  if (pTask == NULL) {
    return streamMetaAddFailedTask(pMeta, rsp.streamId, rsp.upstreamTaskId);
  }

  code = streamTaskProcessCheckRsp(pTask, &rsp);
  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t tqStreamTaskProcessCheckpointReadyMsg(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  int32_t vgId = pMeta->vgId;
  char*   msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t len = pMsg->contLen - sizeof(SMsgHead);
  int32_t code = 0;

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
    tqError("vgId:%d failed to find s-task:0x%x, it may have been destroyed already", vgId, req.downstreamTaskId);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  tqDebug("vgId:%d s-task:%s received the checkpoint-ready msg from task:0x%x (vgId:%d), handle it", vgId,
          pTask->id.idStr, req.downstreamTaskId, req.downstreamNodeId);

  streamProcessCheckpointReadyMsg(pTask, req.checkpointId, req.downstreamTaskId, req.downstreamNodeId);
  streamMetaReleaseTask(pMeta, pTask);

  {  // send checkpoint ready rsp
    SMStreamCheckpointReadyRspMsg* pReadyRsp = rpcMallocCont(sizeof(SMStreamCheckpointReadyRspMsg));

    pReadyRsp->upstreamTaskId = req.upstreamTaskId;
    pReadyRsp->upstreamNodeId = req.upstreamNodeId;
    pReadyRsp->downstreamTaskId = req.downstreamTaskId;
    pReadyRsp->downstreamNodeId = req.downstreamNodeId;
    pReadyRsp->checkpointId = req.checkpointId;
    pReadyRsp->streamId = req.streamId;
    pReadyRsp->head.vgId = htonl(req.downstreamNodeId);

    SRpcMsg rsp = {.code = 0, .info = pMsg->info, .pCont = pReadyRsp, .contLen = sizeof(SMStreamCheckpointReadyRspMsg)};
    tmsgSendRsp(&rsp);

    pMsg->info.handle = NULL;  // disable auto rsp
  }

  return code;
}

int32_t tqStreamTaskProcessDeployReq(SStreamMeta* pMeta, SMsgCb* cb, int64_t sversion, char* msg, int32_t msgLen,
                                     bool isLeader, bool restored) {
  int32_t code = 0;
  int32_t vgId = pMeta->vgId;

  if (tsDisableStream) {
    tqInfo("vgId:%d stream disabled, not deploy stream tasks", vgId);
    return code;
  }

  tqDebug("vgId:%d receive new stream task deploy msg, start to build stream task", vgId);

  // 1.deserialize msg and build task
  int32_t      size = sizeof(SStreamTask);
  SStreamTask* pTask = taosMemoryCalloc(1, size);
  if (pTask == NULL) {
    tqError("vgId:%d failed to create stream task due to out of memory, alloc size:%d", vgId, size);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  code = tDecodeStreamTask(&decoder, pTask);
  tDecoderClear(&decoder);

  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pTask);
    return TSDB_CODE_INVALID_MSG;
  }

  // 2.save task, use the latest commit version as the initial start version of stream task.
  int32_t taskId = pTask->id.taskId;
  int64_t streamId = pTask->id.streamId;
  bool    added = false;

  streamMetaWLock(pMeta);
  code = streamMetaRegisterTask(pMeta, sversion, pTask, &added);
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
  streamMetaWUnLock(pMeta);

  if (code < 0) {
    tqError("failed to add s-task:0x%x into vgId:%d meta, existed:%d, code:%s", vgId, taskId, numOfTasks,
            tstrerror(code));
    tFreeStreamTask(pTask);
    return code;
  }

  // added into meta store, pTask cannot be reference since it may have been destroyed by other threads already now if
  // it is added into the meta store
  if (added) {
    // only handled in the leader node
    if (isLeader) {
      tqDebug("vgId:%d s-task:0x%x is deployed and add into meta, numOfTasks:%d", vgId, taskId, numOfTasks);

      if (restored) {
        SStreamTask* p = streamMetaAcquireTask(pMeta, streamId, taskId);
        if ((p != NULL) && (p->info.fillHistory == 0)) {
          tqStreamStartOneTaskAsync(pMeta, cb, streamId, taskId);
        }

        if (p != NULL) {
          streamMetaReleaseTask(pMeta, p);
        }
      } else {
        tqWarn("s-task:0x%x not launched since vnode(vgId:%d) not ready", taskId, vgId);
      }

    } else {
      tqDebug("vgId:%d not leader, not launch stream task s-task:0x%x", vgId, taskId);
    }
  } else {
    tqWarn("vgId:%d failed to add s-task:0x%x, since already exists in meta store", vgId, taskId);
    tFreeStreamTask(pTask);
  }

  return code;
}

int32_t tqStreamTaskProcessDropReq(SStreamMeta* pMeta, char* msg, int32_t msgLen) {
  SVDropStreamTaskReq* pReq = (SVDropStreamTaskReq*)msg;

  int32_t vgId = pMeta->vgId;
  STaskId hTaskId = {0};
  tqDebug("vgId:%d receive msg to drop s-task:0x%x", vgId, pReq->taskId);

  streamMetaWLock(pMeta);

  STaskId       id = {.streamId = pReq->streamId, .taskId = pReq->taskId};
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if ((ppTask != NULL) && ((*ppTask) != NULL)) {
    streamMetaAcquireOneTask(*ppTask);
    SStreamTask* pTask = *ppTask;

    if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
      hTaskId.streamId = pTask->hTaskInfo.id.streamId;
      hTaskId.taskId = pTask->hTaskInfo.id.taskId;
    }

    streamTaskSetRemoveBackendFiles(pTask);
    streamTaskClearHTaskAttr(pTask, pReq->resetRelHalt);
    streamMetaReleaseTask(pMeta, pTask);
  }

  streamMetaWUnLock(pMeta);

  // drop the related fill-history task firstly
  if (hTaskId.taskId != 0 && hTaskId.streamId != 0) {
    tqDebug("s-task:0x%x vgId:%d drop rel fill-history task:0x%x firstly", pReq->taskId, vgId, (int32_t)hTaskId.taskId);
    streamMetaUnregisterTask(pMeta, hTaskId.streamId, hTaskId.taskId);
  }

  // drop the stream task now
  streamMetaUnregisterTask(pMeta, pReq->streamId, pReq->taskId);

  // commit the update
  streamMetaWLock(pMeta);
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
  tqDebug("vgId:%d task:0x%x dropped, remain tasks:%d", vgId, pReq->taskId, numOfTasks);

  if (streamMetaCommit(pMeta) < 0) {
    // persist to disk
  }

  streamMetaWUnLock(pMeta);
  return 0;
}

int32_t tqStreamTaskProcessUpdateCheckpointReq(SStreamMeta* pMeta, bool restored, char* msg, int32_t msgLen) {
  SVUpdateCheckpointInfoReq* pReq = (SVUpdateCheckpointInfoReq*)msg;

  int32_t vgId = pMeta->vgId;
  tqDebug("vgId:%d receive msg to update-checkpoint-info for s-task:0x%x", vgId, pReq->taskId);

  streamMetaWLock(pMeta);

  STaskId       id = {.streamId = pReq->streamId, .taskId = pReq->taskId};
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));

  if (ppTask != NULL && (*ppTask) != NULL) {
    streamTaskUpdateTaskCheckpointInfo(*ppTask, restored, pReq);
  } else {  // failed to get the task.
    tqError("vgId:%d failed to locate the s-task:0x%x to update the checkpoint info, it may have been dropped already",
            vgId, pReq->taskId);
  }

  streamMetaWUnLock(pMeta);
  // always return success when handling the requirement issued by mnode during transaction.
  return TSDB_CODE_SUCCESS;
}

static int32_t restartStreamTasks(SStreamMeta* pMeta, bool isLeader) {
  int32_t vgId = pMeta->vgId;
  int32_t code = 0;
  int64_t st = taosGetTimestampMs();

  streamMetaWLock(pMeta);
  if (pMeta->startInfo.startAllTasks == 1) {
    pMeta->startInfo.restartCount += 1;
    tqDebug("vgId:%d in start tasks procedure, inc restartCounter by 1, remaining restart:%d", vgId,
            pMeta->startInfo.restartCount);
    streamMetaWUnLock(pMeta);
    return TSDB_CODE_SUCCESS;
  }

  pMeta->startInfo.startAllTasks = 1;
  streamMetaWUnLock(pMeta);

  terrno = 0;
  tqInfo("vgId:%d tasks are all updated and stopped, restart all tasks, triggered by transId:%d", vgId,
         pMeta->updateInfo.transId);

  while (streamMetaTaskInTimer(pMeta)) {
    tqDebug("vgId:%d some tasks in timer, wait for 100ms and recheck", pMeta->vgId);
    taosMsleep(100);
  }

  streamMetaWLock(pMeta);
  streamMetaClear(pMeta);

  int64_t el = taosGetTimestampMs() - st;
  tqInfo("vgId:%d close&reload state elapsed time:%.3fs", vgId, el / 1000.);

  streamMetaLoadAllTasks(pMeta);

  {
    STaskStartInfo* pStartInfo = &pMeta->startInfo;
    taosHashClear(pStartInfo->pReadyTaskSet);
    taosHashClear(pStartInfo->pFailedTaskSet);
    pStartInfo->readyTs = 0;
  }

  if (isLeader && !tsDisableStream) {
    streamMetaWUnLock(pMeta);
    streamMetaStartAllTasks(pMeta, tqExpandStreamTask);
  } else {
    streamMetaResetStartInfo(&pMeta->startInfo);
    streamMetaWUnLock(pMeta);
    tqInfo("vgId:%d, follower node not start stream tasks or stream is disabled", vgId);
  }

  code = terrno;
  return code;
}

int32_t tqStreamTaskProcessRunReq(SStreamMeta* pMeta, SRpcMsg* pMsg, bool isLeader) {
  SStreamTaskRunReq* pReq = pMsg->pCont;

  int32_t type = pReq->reqType;
  int32_t vgId = pMeta->vgId;

  if (type == STREAM_EXEC_T_START_ONE_TASK) {
    streamMetaStartOneTask(pMeta, pReq->streamId, pReq->taskId, tqExpandStreamTask);
    return 0;
  } else if (type == STREAM_EXEC_T_START_ALL_TASKS) {
    streamMetaStartAllTasks(pMeta, tqExpandStreamTask);
    return 0;
  } else if (type == STREAM_EXEC_T_RESTART_ALL_TASKS) {
    restartStreamTasks(pMeta, isLeader);
    return 0;
  } else if (type == STREAM_EXEC_T_STOP_ALL_TASKS) {
    streamMetaStopAllTasks(pMeta);
    return 0;
  } else if (type == STREAM_EXEC_T_ADD_FAILED_TASK) {
    int32_t code = streamMetaAddFailedTask(pMeta, pReq->streamId, pReq->taskId);
    return code;
  } else if (type == STREAM_EXEC_T_RESUME_TASK) {  // task resume to run after idle for a while
    SStreamTask* pTask = streamMetaAcquireTask(pMeta, pReq->streamId, pReq->taskId);

    if (pTask != NULL) {
      char* pStatus = NULL;
      if (streamTaskReadyToRun(pTask, &pStatus)) {
        int64_t execTs = pTask->status.lastExecTs;
        int32_t idle = taosGetTimestampMs() - execTs;
        tqDebug("s-task:%s task resume to run after idle for:%dms from:%" PRId64, pTask->id.idStr, idle, execTs);

        streamResumeTask(pTask);
      } else {
        int8_t status = streamTaskSetSchedStatusInactive(pTask);
        tqDebug("vgId:%d s-task:%s ignore run req since not in ready state, status:%s, sched-status:%d", vgId,
                pTask->id.idStr, pStatus, status);
      }
      streamMetaReleaseTask(pMeta, pTask);
    }

    return 0;
  }

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pReq->streamId, pReq->taskId);
  if (pTask != NULL) {  // even in halt status, the data in inputQ must be processed
    char* p = NULL;
    if (streamTaskReadyToRun(pTask, &p)) {
      tqDebug("vgId:%d s-task:%s status:%s start to process block from inputQ, next checked ver:%" PRId64, vgId,
              pTask->id.idStr, p, pTask->chkInfo.nextProcessVer);
      streamExecTask(pTask);
    } else {
      int8_t status = streamTaskSetSchedStatusInactive(pTask);
      tqDebug("vgId:%d s-task:%s ignore run req since not in ready state, status:%s, sched-status:%d", vgId,
              pTask->id.idStr, p, status);
    }

    streamMetaReleaseTask(pMeta, pTask);
    return 0;
  } else {  // NOTE: pTask->status.schedStatus is not updated since it is not be handled by the run exec.
    // todo add one function to handle this
    tqError("vgId:%d failed to found s-task, taskId:0x%x may have been dropped", vgId, pReq->taskId);
    return -1;
  }
}

int32_t tqStartTaskCompleteCallback(SStreamMeta* pMeta) {
  STaskStartInfo* pStartInfo = &pMeta->startInfo;
  int32_t         vgId = pMeta->vgId;
  bool            scanWal = false;

  streamMetaWLock(pMeta);
  if (pStartInfo->startAllTasks == 1) {
    tqDebug("vgId:%d already in start tasks procedure in other thread, restartCounter:%d, do nothing", vgId,
            pMeta->startInfo.restartCount);
  } else {  // not in starting procedure
    bool allReady = streamMetaAllTasksReady(pMeta);

    if ((pStartInfo->restartCount > 0) && (!allReady)) {
      // if all tasks are ready now, do NOT restart again, and reset the value of pStartInfo->restartCount
      pStartInfo->restartCount -= 1;
      tqDebug("vgId:%d role:%d need to restart all tasks again, restartCounter:%d", vgId, pMeta->role,
              pStartInfo->restartCount);
      streamMetaWUnLock(pMeta);

      restartStreamTasks(pMeta, (pMeta->role == NODE_ROLE_LEADER));
      return TSDB_CODE_SUCCESS;
    } else {
      if (pStartInfo->restartCount == 0) {
        tqDebug("vgId:%d start all tasks completed in callbackFn, restartCount is 0", pMeta->vgId);
      } else if (allReady) {
        pStartInfo->restartCount = 0;
        tqDebug("vgId:%d all tasks are ready, reset restartCounter 0, not restart tasks", vgId);
      }

      scanWal = true;
    }
  }

  streamMetaWUnLock(pMeta);

  if (scanWal && (vgId != SNODE_HANDLE)) {
    tqDebug("vgId:%d start scan wal for executing tasks", vgId);
    tqScanWalAsync(pMeta->ahandle, true);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tqStreamTaskProcessTaskResetReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  SVPauseStreamTaskReq* pReq = (SVPauseStreamTaskReq*)pMsg->pCont;

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pReq->streamId, pReq->taskId);
  if (pTask == NULL) {
    tqError("vgId:%d process task-reset req, failed to acquire task:0x%x, it may have been dropped already",
            pMeta->vgId, pReq->taskId);
    return TSDB_CODE_SUCCESS;
  }

  tqDebug("s-task:%s receive task-reset msg from mnode, reset status and ready for data processing", pTask->id.idStr);

  taosThreadMutexLock(&pTask->lock);

  streamTaskClearCheckInfo(pTask, true);

  // clear flag set during do checkpoint, and open inputQ for all upstream tasks
  SStreamTaskState* pState = streamTaskGetStatus(pTask);
  if (pState->state == TASK_STATUS__CK) {
    int32_t tranId = 0;
    int64_t activeChkId = 0;
    streamTaskGetActiveCheckpointInfo(pTask, &tranId, &activeChkId);

    tqDebug("s-task:%s reset task status from checkpoint, current checkpointingId:%" PRId64 ", transId:%d",
            pTask->id.idStr, activeChkId, tranId);

    streamTaskSetStatusReady(pTask);
  } else if (pState->state == TASK_STATUS__UNINIT) {
    tqDebug("s-task:%s start task by checking downstream tasks", pTask->id.idStr);
    ASSERT(pTask->status.downstreamReady == 0);
    tqStreamStartOneTaskAsync(pMeta, pTask->pMsgCb, pTask->id.streamId, pTask->id.taskId);
  } else {
    tqDebug("s-task:%s status:%s do nothing after receiving reset-task from mnode", pTask->id.idStr, pState->name);
  }

  taosThreadMutexUnlock(&pTask->lock);

  streamMetaReleaseTask(pMeta, pTask);
  return TSDB_CODE_SUCCESS;
}

int32_t tqStreamTaskProcessRetrieveTriggerReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  SRetrieveChkptTriggerReq* pReq = (SRetrieveChkptTriggerReq*)pMsg->pCont;

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pReq->streamId, pReq->upstreamTaskId);
  if (pTask == NULL) {
    tqError("vgId:%d process retrieve checkpoint trigger, checkpointId:%" PRId64
            " from s-task:0x%x, failed to acquire task:0x%x, it may have been dropped already",
            pMeta->vgId, pReq->checkpointId, (int32_t)pReq->downstreamTaskId, pReq->upstreamTaskId);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  tqDebug("s-task:0x%x recv retrieve checkpoint-trigger msg from downstream s-task:0x%x, checkpointId:%" PRId64,
          pReq->upstreamTaskId, (int32_t)pReq->downstreamTaskId, pReq->checkpointId);

  if (pTask->status.downstreamReady != 1) {
    tqError("s-task:%s not ready for checkpoint-trigger retrieve from 0x%x, since downstream not ready",
            pTask->id.idStr, (int32_t)pReq->downstreamTaskId);

    streamTaskSendCheckpointTriggerMsg(pTask, pReq->downstreamTaskId, pReq->downstreamNodeId, &pMsg->info,
                                       TSDB_CODE_STREAM_TASK_IVLD_STATUS);
    streamMetaReleaseTask(pMeta, pTask);

    return TSDB_CODE_SUCCESS;
  }

  SStreamTaskState* pState = streamTaskGetStatus(pTask);
  if (pState->state == TASK_STATUS__CK) {  // recv the checkpoint-source/trigger already
    int32_t transId = 0;
    int64_t checkpointId = 0;

    streamTaskGetActiveCheckpointInfo(pTask, &transId, &checkpointId);
    if (checkpointId != pReq->checkpointId) {
      tqError("s-task:%s invalid checkpoint-trigger retrieve msg from %x, current checkpointId:%"PRId64" req:%"PRId64,
          pTask->id.idStr, (int32_t) pReq->downstreamTaskId, checkpointId, pReq->checkpointId);
      streamMetaReleaseTask(pMeta, pTask);
      return TSDB_CODE_INVALID_MSG;
    }

    if (streamTaskAlreadySendTrigger(pTask, pReq->downstreamNodeId)) {
      // re-send the lost checkpoint-trigger msg to downstream task
      tqDebug("s-task:%s re-send checkpoint-trigger to:0x%x, checkpointId:%" PRId64 ", transId:%d", pTask->id.idStr,
              (int32_t)pReq->downstreamTaskId, checkpointId, transId);
      streamTaskSendCheckpointTriggerMsg(pTask, pReq->downstreamTaskId, pReq->downstreamNodeId, &pMsg->info,
                                         TSDB_CODE_SUCCESS);
    } else {  // not send checkpoint-trigger yet, wait
      int32_t recv = 0, total = 0;
      streamTaskGetTriggerRecvStatus(pTask, &recv, &total);

      if (recv == total) {  // add the ts info
        tqWarn("s-task:%s all upstream send checkpoint-source/trigger, but not processed yet, wait", pTask->id.idStr);
      } else {
        tqWarn(
            "s-task:%s not all upstream send checkpoint-source/trigger, total recv:%d/%d, wait for all upstream "
            "sending checkpoint-source/trigger",
            pTask->id.idStr, recv, total);
      }
      streamTaskSendCheckpointTriggerMsg(pTask, pReq->downstreamTaskId, pReq->downstreamNodeId, &pMsg->info,
                                         TSDB_CODE_ACTION_IN_PROGRESS);
    }
  } else {  // upstream not recv the checkpoint-source/trigger till now
    ASSERT(pState->state == TASK_STATUS__READY || pState->state == TASK_STATUS__HALT);
    tqWarn(
        "s-task:%s not recv checkpoint-source from mnode or checkpoint-trigger from upstream yet, wait for all "
        "upstream sending checkpoint-source/trigger",
        pTask->id.idStr);
    streamTaskSendCheckpointTriggerMsg(pTask, pReq->downstreamTaskId, pReq->downstreamNodeId, &pMsg->info,
                                       TSDB_CODE_ACTION_IN_PROGRESS);
  }

  streamMetaReleaseTask(pMeta, pTask);
  return TSDB_CODE_SUCCESS;
}

int32_t tqStreamTaskProcessRetrieveTriggerRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  SCheckpointTriggerRsp* pRsp = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pRsp->streamId, pRsp->taskId);
  if (pTask == NULL) {
    tqError(
        "vgId:%d process retrieve checkpoint-trigger, failed to acquire task:0x%x, it may have been dropped already",
        pMeta->vgId, pRsp->taskId);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  tqDebug("s-task:%s recv re-send checkpoint-trigger msg from upstream:0x%x, checkpointId:%" PRId64 ", transId:%d",
          pTask->id.idStr, pRsp->upstreamTaskId, pRsp->checkpointId, pRsp->transId);

  streamTaskProcessCheckpointTriggerRsp(pTask, pRsp);
  streamMetaReleaseTask(pMeta, pTask);
  return TSDB_CODE_SUCCESS;
}

int32_t tqStreamTaskProcessTaskPauseReq(SStreamMeta* pMeta, char* pMsg) {
  SVPauseStreamTaskReq* pReq = (SVPauseStreamTaskReq*)pMsg;

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pReq->streamId, pReq->taskId);
  if (pTask == NULL) {
    tqError("vgId:%d process pause req, failed to acquire task:0x%x, it may have been dropped already", pMeta->vgId,
            pReq->taskId);
    // since task is in [STOP|DROPPING] state, it is safe to assume the pause is active
    return TSDB_CODE_SUCCESS;
  }

  tqDebug("s-task:%s receive pause msg from mnode", pTask->id.idStr);
  streamTaskPause(pTask);

  SStreamTask* pHistoryTask = NULL;
  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    pHistoryTask = streamMetaAcquireTask(pMeta, pTask->hTaskInfo.id.streamId, pTask->hTaskInfo.id.taskId);
    if (pHistoryTask == NULL) {
      tqError("vgId:%d process pause req, failed to acquire fill-history task:0x%" PRIx64
              ", it may have been dropped already",
              pMeta->vgId, pTask->hTaskInfo.id.taskId);
      streamMetaReleaseTask(pMeta, pTask);

      // since task is in [STOP|DROPPING] state, it is safe to assume the pause is active
      return TSDB_CODE_SUCCESS;
    }

    tqDebug("s-task:%s fill-history task handle paused along with related stream task", pHistoryTask->id.idStr);

    streamTaskPause(pHistoryTask);
    streamMetaReleaseTask(pMeta, pHistoryTask);
  }

  streamMetaReleaseTask(pMeta, pTask);
  return TSDB_CODE_SUCCESS;
}

static int32_t tqProcessTaskResumeImpl(void* handle, SStreamTask* pTask, int64_t sversion, int8_t igUntreated,
                                       bool fromVnode) {
  SStreamMeta* pMeta = fromVnode ? ((STQ*)handle)->pStreamMeta : handle;
  int32_t      vgId = pMeta->vgId;
  if (pTask == NULL) {
    return -1;
  }

  streamTaskResume(pTask);
  ETaskStatus status = streamTaskGetStatus(pTask)->state;

  int32_t level = pTask->info.taskLevel;
  if (status == TASK_STATUS__READY || status == TASK_STATUS__SCAN_HISTORY || status == TASK_STATUS__CK) {
    // no lock needs to secure the access of the version
    if (igUntreated && level == TASK_LEVEL__SOURCE && !pTask->info.fillHistory) {
      // discard all the data  when the stream task is suspended.
      walReaderSetSkipToVersion(pTask->exec.pWalReader, sversion);
      tqDebug("vgId:%d s-task:%s resume to exec, prev paused version:%" PRId64 ", start from vnode ver:%" PRId64
              ", schedStatus:%d",
              vgId, pTask->id.idStr, pTask->chkInfo.nextProcessVer, sversion, pTask->status.schedStatus);
    } else {  // from the previous paused version and go on
      tqDebug("vgId:%d s-task:%s resume to exec, from paused ver:%" PRId64 ", vnode ver:%" PRId64 ", schedStatus:%d",
              vgId, pTask->id.idStr, pTask->chkInfo.nextProcessVer, sversion, pTask->status.schedStatus);
    }

    if (level == TASK_LEVEL__SOURCE && pTask->info.fillHistory && status == TASK_STATUS__SCAN_HISTORY) {
      pTask->hTaskInfo.operatorOpen = false;
      streamStartScanHistoryAsync(pTask, igUntreated);
    } else if (level == TASK_LEVEL__SOURCE && (streamQueueGetNumOfItems(pTask->inputq.queue) == 0)) {
      tqScanWalAsync((STQ*)handle, false);
    } else {
      streamTrySchedExec(pTask);
    }
  } /*else {
    ASSERT(status != TASK_STATUS__UNINIT);
  }*/

  streamMetaReleaseTask(pMeta, pTask);
  return 0;
}

int32_t tqStreamTaskProcessTaskResumeReq(void* handle, int64_t sversion, char* msg, bool fromVnode) {
  SVResumeStreamTaskReq* pReq = (SVResumeStreamTaskReq*)msg;

  SStreamMeta* pMeta = fromVnode ? ((STQ*)handle)->pStreamMeta : handle;

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pReq->streamId, pReq->taskId);
  if (pTask == NULL) {
    tqError("s-task:0x%x failed to acquire task to resume, it may have been dropped or stopped", pReq->taskId);
    return TSDB_CODE_STREAM_TASK_IVLD_STATUS;
  }

  taosThreadMutexLock(&pTask->lock);
  SStreamTaskState* pState = streamTaskGetStatus(pTask);
  tqDebug("s-task:%s start to resume from paused, current status:%s", pTask->id.idStr, pState->name);
  taosThreadMutexUnlock(&pTask->lock);

  int32_t code = tqProcessTaskResumeImpl(handle, pTask, sversion, pReq->igUntreated, fromVnode);
  if (code != 0) {
    return code;
  }

  STaskId*     pHTaskId = &pTask->hTaskInfo.id;
  SStreamTask* pHTask = streamMetaAcquireTask(pMeta, pHTaskId->streamId, pHTaskId->taskId);
  if (pHTask) {
    taosThreadMutexLock(&pHTask->lock);
    SStreamTaskState* p = streamTaskGetStatus(pHTask);
    tqDebug("s-task:%s related history task start to resume from paused, current status:%s", pHTask->id.idStr, p->name);
    taosThreadMutexUnlock(&pHTask->lock);

    code = tqProcessTaskResumeImpl(handle, pHTask, sversion, pReq->igUntreated, fromVnode);
  }

  return code;
}

int32_t tqStreamTasksGetTotalNum(SStreamMeta* pMeta) { return taosArrayGetSize(pMeta->pTaskList); }

int32_t doProcessDummyRspMsg(SStreamMeta* UNUSED_PARAM(pMeta), SRpcMsg* pMsg) {
  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = NULL;
  return TSDB_CODE_SUCCESS;
}

int32_t tqStreamProcessStreamHbRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) { return doProcessDummyRspMsg(pMeta, pMsg); }

int32_t tqStreamProcessReqCheckpointRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) { return doProcessDummyRspMsg(pMeta, pMsg); }

int32_t tqStreamProcessChkptReportRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) { return doProcessDummyRspMsg(pMeta, pMsg); }

int32_t tqStreamProcessCheckpointReadyRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  SMStreamCheckpointReadyRspMsg* pRsp = pMsg->pCont;

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pRsp->streamId, pRsp->downstreamTaskId);
  if (pTask == NULL) {
    tqError("vgId:%d failed to acquire task:0x%x when handling checkpoint-ready msg, it may have been dropped",
            pRsp->downstreamNodeId, pRsp->downstreamTaskId);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  streamTaskProcessCheckpointReadyRsp(pTask, pRsp->upstreamTaskId, pRsp->checkpointId);
  streamMetaReleaseTask(pMeta, pTask);
  return TSDB_CODE_SUCCESS;
}