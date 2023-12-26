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

typedef struct STaskUpdateEntry {
  int64_t streamId;
  int32_t taskId;
  int32_t transId;
} STaskUpdateEntry;

int32_t tqStreamTaskStartAsync(SStreamMeta* pMeta, SMsgCb* cb, bool restart) {
  int32_t vgId = pMeta->vgId;

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  if (numOfTasks == 0) {
    tqDebug("vgId:%d no stream tasks existed to run", vgId);
    return 0;
  }

  SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
  if (pRunReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tqError("vgId:%d failed to create msg to start wal scanning to launch stream tasks, code:%s", vgId, terrstr());
    return -1;
  }

  tqDebug("vgId:%d start all %d stream task(s) async", vgId, numOfTasks);
  pRunReq->head.vgId = vgId;
  pRunReq->streamId = 0;
  pRunReq->taskId = restart ? STREAM_EXEC_RESTART_ALL_TASKS_ID : STREAM_EXEC_START_ALL_TASKS_ID;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  tmsgPutToQueue(cb, STREAM_QUEUE, &msg);
  return 0;
}

int32_t tqStreamTaskProcessUpdateReq(SStreamMeta* pMeta, SMsgCb* cb, SRpcMsg* pMsg, bool restored) {
  int32_t vgId = pMeta->vgId;
  char*   msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t len = pMsg->contLen - sizeof(SMsgHead);
  SRpcMsg rsp = {.info = pMsg->info, .code = TSDB_CODE_SUCCESS};

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
    tqError("vgId:%d failed to acquire task:0x%x when handling update, it may have been dropped already", pMeta->vgId,
            req.taskId);
    rsp.code = TSDB_CODE_SUCCESS;
    streamMetaWUnLock(pMeta);

    taosArrayDestroy(req.pNodeList);
    return rsp.code;
  }

  SStreamTask* pTask = *ppTask;

  if (pMeta->updateInfo.transId != req.transId) {
    pMeta->updateInfo.transId = req.transId;
    tqInfo("s-task:%s receive new trans to update nodeEp msg from mnode, transId:%d", pTask->id.idStr, req.transId);
    // info needs to be kept till the new trans to update the nodeEp arrived.
    taosHashClear(pMeta->updateInfo.pTasks);
  } else {
    tqDebug("s-task:%s recv trans to update nodeEp from mnode, transId:%d", pTask->id.idStr, req.transId);
  }

  STaskUpdateEntry entry = {.streamId = req.streamId, .taskId = req.taskId, .transId = req.transId};
  void*            exist = taosHashGet(pMeta->updateInfo.pTasks, &entry, sizeof(STaskUpdateEntry));
  if (exist != NULL) {
    tqDebug("s-task:%s (vgId:%d) already update in trans:%d, discard the nodeEp update msg", pTask->id.idStr, vgId,
            req.transId);
    rsp.code = TSDB_CODE_SUCCESS;
    streamMetaWUnLock(pMeta);
    taosArrayDestroy(req.pNodeList);
    return rsp.code;
  }

//  streamMetaWUnLock(pMeta);

  // todo for test purpose
  // the following two functions should not be executed within the scope of meta lock to avoid deadlock
  streamTaskUpdateEpsetInfo(pTask, req.pNodeList);
  streamTaskResetStatus(pTask);

  // continue after lock the meta again
//  streamMetaWLock(pMeta);

  SStreamTask** ppHTask = NULL;
  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    ppHTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &pTask->hTaskInfo.id, sizeof(pTask->hTaskInfo.id));
    if (ppHTask == NULL || *ppHTask == NULL) {
      tqError("vgId:%d failed to acquire fill-history task:0x%x when handling update, it may have been dropped already",
              pMeta->vgId, req.taskId);
      CLEAR_RELATED_FILLHISTORY_TASK(pTask);
    } else {
      tqDebug("s-task:%s fill-history task update nodeEp along with stream task", (*ppHTask)->id.idStr);
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
    tqDebug("s-task:%s task nodeEp update completed, streamTask and related fill-history task closed", pTask->id.idStr);
    taosHashPut(pMeta->updateInfo.pTasks, &(*ppHTask)->id, sizeof(pTask->id), NULL, 0);
  } else {
    tqDebug("s-task:%s task nodeEp update completed, streamTask closed", pTask->id.idStr);
  }

  rsp.code = 0;

  // possibly only handle the stream task.
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
  int32_t updateTasks = taosHashGetSize(pMeta->updateInfo.pTasks);

  pMeta->startInfo.tasksWillRestart = 1;

  if (updateTasks < numOfTasks) {
    tqDebug("vgId:%d closed tasks:%d, unclosed:%d, all tasks will be started when nodeEp update completed", vgId,
            updateTasks, (numOfTasks - updateTasks));
    streamMetaWUnLock(pMeta);
  } else {
    if (!restored) {
      tqDebug("vgId:%d vnode restore not completed, not start the tasks, clear the start after nodeUpdate flag", vgId);
      pMeta->startInfo.tasksWillRestart = 0;
      streamMetaWUnLock(pMeta);
    } else {
      tqDebug("vgId:%d all %d task(s) nodeEp updated and closed", vgId, numOfTasks);
#if 0
      // for test purpose, to trigger the leader election
      taosMSleep(5000);
#endif
      tqStreamTaskStartAsync(pMeta, cb, true);
      streamMetaWUnLock(pMeta);
    }
  }

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
    tDeleteStreamDispatchReq(&req);
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
    tDeleteStreamDispatchReq(&req);

    return 0;
  }
}

int32_t tqStreamTaskProcessDispatchRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  SStreamDispatchRsp* pRsp = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));

  int32_t vgId = pMeta->vgId;
  pRsp->upstreamTaskId = htonl(pRsp->upstreamTaskId);
  pRsp->streamId = htobe64(pRsp->streamId);
  pRsp->downstreamTaskId = htonl(pRsp->downstreamTaskId);
  pRsp->downstreamNodeId = htonl(pRsp->downstreamNodeId);
  pRsp->stage = htobe64(pRsp->stage);
  pRsp->msgId = htonl(pRsp->msgId);

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
    return -1;
  }

  SRpcMsg rsp = {.info = pMsg->info, .code = 0};
  streamProcessRetrieveReq(pTask, &req, &rsp);

  streamMetaReleaseTask(pMeta, pTask);
  tDeleteStreamRetrieveReq(&req);
  return 0;
}

int32_t tqStreamTaskProcessScanHistoryFinishReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  char*   msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  // deserialize
  SStreamScanHistoryFinishReq req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  tDecodeStreamScanHistoryFinishReq(&decoder, &req);
  tDecoderClear(&decoder);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, req.streamId, req.downstreamTaskId);
  if (pTask == NULL) {
    tqError("vgId:%d process scan history finish msg, failed to find task:0x%x, it may be destroyed", pMeta->vgId,
            req.downstreamTaskId);
    return -1;
  }

  tqDebug("s-task:%s receive scan-history finish msg from task:0x%x", pTask->id.idStr, req.upstreamTaskId);

  int32_t code = streamProcessScanHistoryFinishReq(pTask, &req, &pMsg->info);
  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t tqStreamTaskProcessScanHistoryFinishRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  char*   msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  // deserialize
  SStreamCompleteHistoryMsg req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  tDecodeCompleteHistoryDataMsg(&decoder, &req);
  tDecoderClear(&decoder);

  if (pMeta->role == NODE_ROLE_FOLLOWER) {
    tqError("s-task:0x%x (vgId:%d) not handle the scan-history finish rsp, since it becomes follower",
            req.upstreamTaskId, pMeta->vgId);
    return TASK_DOWNSTREAM_NOT_LEADER;
  }

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, req.streamId, req.upstreamTaskId);
  if (pTask == NULL) {
    tqError("vgId:%d process scan history finish rsp, failed to find task:0x%x, it may be destroyed", pMeta->vgId,
            req.upstreamTaskId);
    return -1;
  }

  int32_t remain = atomic_sub_fetch_32(&pTask->notReadyTasks, 1);
  if (remain > 0) {
    tqDebug("s-task:%s scan-history finish rsp received from downstream task:0x%x, unfinished remain:%d",
            pTask->id.idStr, req.downstreamId, remain);
  } else {
    tqDebug(
        "s-task:%s scan-history finish rsp received from downstream task:0x%x, all downstream tasks rsp scan-history "
        "completed msg",
        pTask->id.idStr, req.downstreamId);
    code = streamProcessScanHistoryFinishRsp(pTask);
  }

  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t tqStreamTaskProcessCheckReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  char*   msgStr = pMsg->pCont;
  char*   msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  SStreamTaskCheckReq req;
  SDecoder            decoder;

  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
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

  // only the leader node handle the check request
  if (pMeta->role == NODE_ROLE_FOLLOWER) {
    tqError(
        "s-task:0x%x invalid check msg from upstream:0x%x(vgId:%d), vgId:%d is follower, not handle check status msg",
        taskId, req.upstreamTaskId, req.upstreamNodeId, pMeta->vgId);
    rsp.status = TASK_DOWNSTREAM_NOT_LEADER;
  } else {
    SStreamTask* pTask = streamMetaAcquireTask(pMeta, req.streamId, taskId);
    if (pTask != NULL) {
      rsp.status = streamTaskCheckStatus(pTask, req.upstreamTaskId, req.upstreamNodeId, req.stage, &rsp.oldStage);
      streamMetaReleaseTask(pMeta, pTask);

      char* p = NULL;
      streamTaskGetStatus(pTask, &p);
      tqDebug("s-task:%s status:%s, stage:%" PRId64 " recv task check req(reqId:0x%" PRIx64
              ") task:0x%x (vgId:%d), check_status:%d",
              pTask->id.idStr, p, rsp.oldStage, rsp.reqId, rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);
    } else {
      rsp.status = TASK_DOWNSTREAM_NOT_READY;
      tqDebug("tq recv task check(taskId:0x%" PRIx64 "-0x%x not built yet) req(reqId:0x%" PRIx64
              ") from task:0x%x (vgId:%d), rsp check_status %d",
              req.streamId, taskId, rsp.reqId, rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);
    }
  }

  return streamSendCheckRsp(pMeta, &req, &rsp, &pMsg->info, taskId);
}

int32_t tqStreamTaskProcessCheckRsp(SStreamMeta* pMeta, SRpcMsg* pMsg, bool isLeader) {
  char*   pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t len = pMsg->contLen - sizeof(SMsgHead);
  int32_t vgId = pMeta->vgId;

  int32_t             code;
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
    streamMetaUpdateTaskDownstreamStatus(pMeta, rsp.streamId, rsp.upstreamTaskId, 0, taosGetTimestampMs(), false);
    tqError("vgId:%d not leader, task:0x%x not handle the check rsp, downstream:0x%x (vgId:%d)", vgId,
            rsp.upstreamTaskId, rsp.downstreamTaskId, rsp.downstreamNodeId);
    return code;
  }

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, rsp.streamId, rsp.upstreamTaskId);
  if (pTask == NULL) {
    streamMetaUpdateTaskDownstreamStatus(pMeta, rsp.streamId, rsp.upstreamTaskId, 0, taosGetTimestampMs(), false);
    tqError("tq failed to locate the stream task:0x%" PRIx64 "-0x%x (vgId:%d), it may have been destroyed or stopped",
            rsp.streamId, rsp.upstreamTaskId, vgId);
    terrno = TSDB_CODE_STREAM_TASK_NOT_EXIST;
    return -1;
  }

  code = streamProcessCheckRsp(pTask, &rsp);
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
    return code;
  }

  tqDebug("vgId:%d s-task:%s received the checkpoint ready msg from task:0x%x (vgId:%d), handle it", vgId,
          pTask->id.idStr, req.downstreamTaskId, req.downstreamNodeId);

  streamProcessCheckpointReadyMsg(pTask);
  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t tqStreamTaskProcessDeployReq(SStreamMeta* pMeta, int64_t sversion, char* msg, int32_t msgLen, bool isLeader,
                                     bool restored) {
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
    tqError("failed to add s-task:0x%x into vgId:%d meta, total:%d, code:%s", vgId, taskId, numOfTasks,
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
      SStreamTask* p = streamMetaAcquireTask(pMeta, streamId, taskId);

      if (p != NULL && restored && p->info.fillHistory == 0) {
        EStreamTaskEvent event = (HAS_RELATED_FILLHISTORY_TASK(p)) ? TASK_EVENT_INIT_STREAM_SCANHIST : TASK_EVENT_INIT;
        streamTaskHandleEvent(p->status.pSM, event);
      } else if (!restored) {
        tqWarn("s-task:%s not launched since vnode(vgId:%d) not ready", p->id.idStr, vgId);
      }

      if (p != NULL) {
        streamMetaReleaseTask(pMeta, p);
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
  tqDebug("vgId:%d receive msg to drop s-task:0x%x", vgId, pReq->taskId);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pReq->streamId, pReq->taskId);
  if (pTask != NULL) {
    // drop the related fill-history task firstly
    if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
      STaskId* pHTaskId = &pTask->hTaskInfo.id;
      streamMetaUnregisterTask(pMeta, pHTaskId->streamId, pHTaskId->taskId);
      tqDebug("vgId:%d drop fill-history task:0x%x dropped firstly", vgId, (int32_t)pHTaskId->taskId);
    }
    streamMetaReleaseTask(pMeta, pTask);
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

int32_t tqStreamTaskResetStatus(SStreamMeta* pMeta) {
  int32_t vgId = pMeta->vgId;
  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);

  tqDebug("vgId:%d reset all %d stream task(s) status to be uninit", vgId, numOfTasks);
  if (numOfTasks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pMeta->pTaskList, i);

    STaskId       id = {.streamId = pTaskId->streamId, .taskId = pTaskId->taskId};
    SStreamTask** pTask = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
    streamTaskResetStatus(*pTask);
  }

  return 0;
}

static int32_t restartStreamTasks(SStreamMeta* pMeta, bool isLeader) {
  int32_t vgId = pMeta->vgId;
  int32_t code = 0;
  int64_t st = taosGetTimestampMs();

  streamMetaWLock(pMeta);
  if (pMeta->startInfo.taskStarting == 1) {
    pMeta->startInfo.restartCount += 1;
    tqDebug("vgId:%d in start tasks procedure, inc restartCounter by 1, remaining restart:%d", vgId,
            pMeta->startInfo.restartCount);
    streamMetaWUnLock(pMeta);
    return TSDB_CODE_SUCCESS;
  }

  pMeta->startInfo.taskStarting = 1;
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

  code = streamMetaLoadAllTasks(pMeta);
  if (code != TSDB_CODE_SUCCESS) {
    tqError("vgId:%d failed to load stream tasks, code:%s", vgId, tstrerror(terrno));
    streamMetaWUnLock(pMeta);
    code = terrno;
    return code;
  }

  if (isLeader && !tsDisableStream) {
    tqStreamTaskResetStatus(pMeta);
    streamMetaWUnLock(pMeta);
    streamMetaStartAllTasks(pMeta);
  } else {
    streamMetaResetStartInfo(&pMeta->startInfo);
    streamMetaWUnLock(pMeta);
    tqInfo("vgId:%d, follower node not start stream tasks", vgId);
  }

  code = terrno;
  return code;
}

int32_t tqStreamTaskProcessRunReq(SStreamMeta* pMeta, SRpcMsg* pMsg, bool isLeader) {
  SStreamTaskRunReq* pReq = pMsg->pCont;

  int32_t taskId = pReq->taskId;
  int32_t vgId = pMeta->vgId;

  if (taskId == STREAM_EXEC_START_ALL_TASKS_ID) {
    streamMetaStartAllTasks(pMeta);
    return 0;
  } else if (taskId == STREAM_EXEC_RESTART_ALL_TASKS_ID) {
    restartStreamTasks(pMeta, isLeader);
    return 0;
  } else if (taskId == STREAM_EXEC_STOP_ALL_TASKS_ID) {
    streamMetaStopAllTasks(pMeta);
    return 0;
  }

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pReq->streamId, taskId);
  if (pTask != NULL) {  // even in halt status, the data in inputQ must be processed
    char* p = NULL;
    if (streamTaskReadyToRun(pTask, &p)) {
      tqDebug("vgId:%d s-task:%s start to process block from inputQ, next checked ver:%" PRId64, vgId, pTask->id.idStr,
              pTask->chkInfo.nextProcessVer);
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
    tqError("vgId:%d failed to found s-task, taskId:0x%x may have been dropped", vgId, taskId);
    return -1;
  }
}

int32_t tqStartTaskCompleteCallback(SStreamMeta* pMeta) {
  STaskStartInfo* pStartInfo = &pMeta->startInfo;
  streamMetaWLock(pMeta);

  if (pStartInfo->restartCount > 0) {
    pStartInfo->restartCount -= 1;

    ASSERT(pStartInfo->taskStarting == 0);
    tqDebug("vgId:%d role:%d need to restart all tasks again, restartCounter:%d", pMeta->vgId, pMeta->role,
            pStartInfo->restartCount);

    streamMetaWUnLock(pMeta);
    restartStreamTasks(pMeta, (pMeta->role == NODE_ROLE_LEADER));
  } else {
    streamMetaWUnLock(pMeta);
    tqDebug("vgId:%d start all tasks completed", pMeta->vgId);
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

  // clear flag set during do checkpoint, and open inputQ for all upstream tasks
  if (streamTaskGetStatus(pTask, NULL) == TASK_STATUS__CK) {
    streamTaskClearCheckInfo(pTask, true);
    streamTaskSetStatusReady(pTask);
  }

  streamMetaReleaseTask(pMeta, pTask);
  return TSDB_CODE_SUCCESS;
}