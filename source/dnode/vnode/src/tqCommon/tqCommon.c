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
  int32_t      code = 0;

  tqDebug("s-task:%s vgId:%d start to expand stream task", pTask->id.idStr, vgId);

  if (pTask->info.fillHistory != STREAM_NORMAL_TASK) {
    streamId = pTask->streamTaskId.streamId;
    taskId = pTask->streamTaskId.taskId;
  } else {
    streamId = pTask->id.streamId;
    taskId = pTask->id.taskId;
  }

  // sink task does not need the pState
  if (pTask->info.taskLevel != TASK_LEVEL__SINK) {
    if (pTask->info.fillHistory == STREAM_RECALCUL_TASK) {
      pTask->pRecalState = streamStateRecalatedOpen(pMeta->path, pTask, pTask->id.streamId, pTask->id.taskId);
      if (pTask->pRecalState == NULL) {
        tqError("s-task:%s (vgId:%d) failed to open state for task, expand task failed", pTask->id.idStr, vgId);
        return terrno;
      } else {
        tqDebug("s-task:%s recal state:%p", pTask->id.idStr, pTask->pRecalState);
      }
    }

    pTask->pState = streamStateOpen(pMeta->path, pTask, streamId, taskId);
    if (pTask->pState == NULL) {
      tqError("s-task:%s (vgId:%d) failed to open state for task, expand task failed", pTask->id.idStr, vgId);
      return terrno;
    } else {
      tqDebug("s-task:%s stream state:%p", pTask->id.idStr, pTask->pState);
    }
  }

  SReadHandle handle = {
      .checkpointId = pTask->chkInfo.checkpointId,
      .pStateBackend = NULL,
      .fillHistory = pTask->info.fillHistory,
      .winRange = pTask->dataRange.window,
      .pOtherBackend = NULL,
  };

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE || pTask->info.taskLevel == TASK_LEVEL__MERGE) {
    handle.vnode = ((STQ*)pMeta->ahandle)->pVnode;
    handle.initTqReader = 1;
  } else if (pTask->info.taskLevel == TASK_LEVEL__AGG) {
    handle.numOfVgroups = (int32_t)taosArrayGetSize(pTask->upstreamInfo.pList);
  }

  initStorageAPI(&handle.api);

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE || pTask->info.taskLevel == TASK_LEVEL__AGG ||
      pTask->info.taskLevel == TASK_LEVEL__MERGE) {
    if (pTask->info.fillHistory == STREAM_RECALCUL_TASK) {
      handle.pStateBackend = pTask->pRecalState;
      handle.pOtherBackend = pTask->pState;
    } else {
      handle.pStateBackend = pTask->pState;
      handle.pOtherBackend = NULL;
    }

    code = qCreateStreamExecTaskInfo(&pTask->exec.pExecutor, pTask->exec.qmsg, &handle, vgId, pTask->id.taskId);
    if (code) {
      tqError("s-task:%s failed to expand task, code:%s", pTask->id.idStr, tstrerror(code));
      return code;
    }

    code = qSetTaskId(pTask->exec.pExecutor, pTask->id.taskId, pTask->id.streamId);
    if (code) {
      return code;
    }

    code = qSetStreamNotifyInfo(pTask->exec.pExecutor, pTask->notifyInfo.notifyEventTypes,
                                pTask->notifyInfo.pSchemaWrapper, pTask->notifyInfo.stbFullName,
                                IS_NEW_SUBTB_RULE(pTask), &pTask->notifyEventStat);
    if (code) {
      tqError("s-task:%s failed to set stream notify info, code:%s", pTask->id.idStr, tstrerror(code));
      return code;
    }

    qSetStreamMergeInfo(pTask->exec.pExecutor, pTask->pVTables);
  }

  streamSetupScheduleTrigger(pTask);

  double el = (taosGetTimestampMs() - st) / 1000.0;
  tqDebug("s-task:%s vgId:%d expand stream task completed, elapsed time:%.2fsec", pTask->id.idStr, vgId, el);

  return code;
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

  tqInfo("vgId:%d start all %d stream task(s) async", vgId, numOfTasks);

  int32_t type = restart ? STREAM_EXEC_T_RESTART_ALL_TASKS : STREAM_EXEC_T_START_ALL_TASKS;
  return streamTaskSchedTask(cb, vgId, 0, 0, type, false);
}

int32_t tqStreamStartOneTaskAsync(SStreamMeta* pMeta, SMsgCb* cb, int64_t streamId, int32_t taskId) {
  int32_t vgId = pMeta->vgId;
  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  if (numOfTasks == 0) {
    tqDebug("vgId:%d no stream tasks existed to run", vgId);
    return 0;
  }

  tqDebug("vgId:%d start task:0x%x async", vgId, taskId);
  return streamTaskSchedTask(cb, vgId, streamId, taskId, STREAM_EXEC_T_START_ONE_TASK, false);
}

// this is to process request from transaction, always return true.
int32_t tqStreamTaskProcessUpdateReq(SStreamMeta* pMeta, SMsgCb* cb, SRpcMsg* pMsg, bool restored, bool isLeader) {
  int32_t                  vgId = pMeta->vgId;
  char*                    msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t                  len = pMsg->contLen - sizeof(SMsgHead);
  SRpcMsg                  rsp = {.info = pMsg->info, .code = TSDB_CODE_SUCCESS};
  int64_t                  st = taosGetTimestampMs();
  bool                     updated = false;
  int32_t                  code = 0;
  SStreamTask*             pTask = NULL;
  SStreamTask*             pHTask = NULL;
  SStreamTaskNodeUpdateMsg req = {0};
  SDecoder                 decoder;

  tDecoderInit(&decoder, (uint8_t*)msg, len);
  code = tDecodeStreamTaskUpdateMsg(&decoder, &req);
  tDecoderClear(&decoder);

  if (code < 0) {
    rsp.code = TSDB_CODE_MSG_DECODE_ERROR;
    tqError("vgId:%d failed to decode task update msg, code:%s", vgId, tstrerror(code));
    tDestroyNodeUpdateMsg(&req);
    return rsp.code;
  }

  int32_t gError = streamGetFatalError(pMeta);
  if (gError != 0) {
    tqError("vgId:%d global fatal occurs, code:%s, ts:%" PRId64 " func:%s", pMeta->vgId, tstrerror(gError),
            pMeta->fatalInfo.ts, pMeta->fatalInfo.func);
    return 0;
  }

  // update the nodeEpset when it exists
  streamMetaWLock(pMeta);

  // the task epset may be updated again and again, when replaying the WAL, the task may be in stop status.
  STaskId id = {.streamId = req.streamId, .taskId = req.taskId};
  code = streamMetaAcquireTaskUnsafe(pMeta, &id, &pTask);
  if (code != 0) {
    tqError("vgId:%d failed to acquire task:0x%x when handling update task epset, it may have been dropped", vgId,
            req.taskId);
    rsp.code = TSDB_CODE_SUCCESS;
    streamMetaWUnLock(pMeta);
    tDestroyNodeUpdateMsg(&req);
    return rsp.code;
  }

  const char* idstr = pTask->id.idStr;

  if (req.transId <= 0) {
    tqError("vgId:%d invalid update nodeEp task, transId:%d, discard", vgId, req.taskId);
    rsp.code = TSDB_CODE_SUCCESS;

    streamMetaReleaseTask(pMeta, pTask);
    streamMetaWUnLock(pMeta);

    tDestroyNodeUpdateMsg(&req);
    return rsp.code;
  }

  // info needs to be kept till the new trans to update the nodeEp arrived.
  bool update = streamMetaInitUpdateTaskList(pMeta, req.transId, req.pTaskList);
  if (!update) {
    rsp.code = TSDB_CODE_SUCCESS;

    streamMetaReleaseTask(pMeta, pTask);
    streamMetaWUnLock(pMeta);

    tDestroyNodeUpdateMsg(&req);
    return rsp.code;
  }

  // duplicate update epset msg received, discard this redundant message
  STaskUpdateEntry entry = {.streamId = req.streamId, .taskId = req.taskId, .transId = req.transId};

  void* pReqTask = taosHashGet(pMeta->updateInfo.pTasks, &entry, sizeof(STaskUpdateEntry));
  if (pReqTask != NULL) {
    tqDebug("s-task:%s (vgId:%d) already update in transId:%d, discard the nodeEp update msg", idstr, vgId,
            req.transId);
    rsp.code = TSDB_CODE_SUCCESS;

    streamMetaReleaseTask(pMeta, pTask);
    streamMetaWUnLock(pMeta);

    tDestroyNodeUpdateMsg(&req);
    return rsp.code;
  }

  updated = streamTaskUpdateEpsetInfo(pTask, req.pNodeList);

  // send the checkpoint-source-rsp for source task to end the checkpoint trans in mnode
  code = streamTaskSendCheckpointsourceRsp(pTask);
  if (code) {
    tqError("%s failed to send checkpoint-source rsp, code:%s", pTask->id.idStr, tstrerror(code));
  }
  streamTaskResetStatus(pTask);

  streamTaskStopMonitorCheckRsp(&pTask->taskCheckInfo, pTask->id.idStr);

  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    code = streamMetaAcquireTaskUnsafe(pMeta, &pTask->hTaskInfo.id, &pHTask);
    if (code != 0) {
      tqError(
          "vgId:%d failed to acquire fill-history task:0x%x when handling update, may have been dropped already, rel "
          "stream task:0x%x",
          vgId, (uint32_t)pTask->hTaskInfo.id.taskId, req.taskId);
      CLEAR_RELATED_FILLHISTORY_TASK(pTask);
    } else {
      tqDebug("s-task:%s fill-history task update nodeEp along with stream task", pHTask->id.idStr);
      bool updateEpSet = streamTaskUpdateEpsetInfo(pHTask, req.pNodeList);
      if (updateEpSet) {
        updated = updateEpSet;
      }

      streamTaskResetStatus(pHTask);
      streamTaskStopMonitorCheckRsp(&pHTask->taskCheckInfo, pHTask->id.idStr);
    }
  }

  // stream do update the nodeEp info, write it into stream meta.
  if (updated) {
    tqInfo("s-task:%s vgId:%d save task after update epset, and stop task", idstr, vgId);
    code = streamMetaSaveTaskInMeta(pMeta, pTask);
    if (code) {
      tqError("s-task:%s vgId:%d failed to save task, code:%s", idstr, vgId, tstrerror(code));
    }

    if (pHTask != NULL) {
      code = streamMetaSaveTaskInMeta(pMeta, pHTask);
      if (code) {
        tqError("s-task:%s vgId:%d failed to save related history task, code:%s", idstr, vgId, tstrerror(code));
      }
    }
  } else {
    tqInfo("s-task:%s vgId:%d not save task since not update epset actually, stop task", idstr, vgId);
  }

  code = streamTaskStop(pTask);
  if (code) {
    tqError("s-task:%s vgId:%d failed to stop task, code:%s", idstr, vgId, tstrerror(code));
  }

  if (pHTask != NULL) {
    code = streamTaskStop(pHTask);
    if (code) {
      tqError("s-task:%s vgId:%d failed to stop related history task, code:%s", idstr, vgId, tstrerror(code));
    }
  }

  // keep info
  streamMetaAddIntoUpdateTaskList(pMeta, pTask, req.transId, st);
  streamMetaReleaseTask(pMeta, pTask);
  streamMetaReleaseTask(pMeta, pHTask);

  rsp.code = TSDB_CODE_SUCCESS;

  // possibly only handle the stream task.
  int32_t reqUpdateTasks = taosArrayGetSize(req.pTaskList);
  int32_t updateTasks = taosHashGetSize(pMeta->updateInfo.pTasks);
  bool    hasUnupdated = false;

  for(int32_t i = 0; i < taosArrayGetSize(req.pTaskList); ++i) {

    int32_t* pTaskId = (int32_t*) taosArrayGet(req.pTaskList, i);
    if (pTaskId != NULL) {
      int32_t index = -1;

      for(int32_t j = 0; j < taosArrayGetSize(pMeta->pTaskList); ++j) {
        SStreamTaskId* pId = taosArrayGet(pMeta->pTaskList, j);
        if (*pTaskId == pId->taskId) {  // task id exist in vnode
          index = j;
          break;
        }
      }

      if (index != -1) {
        SStreamTaskId*   pId = taosArrayGet(pMeta->pTaskList, index);
        STaskUpdateEntry uEntry = {.streamId = pId->streamId, .taskId = pId->taskId, .transId = req.transId};
        void*            p = taosHashGet(pMeta->updateInfo.pTasks, &uEntry, sizeof(uEntry));
        if (p == NULL) {
          tqInfo("vgId:%d s-task:0x%x not updated yet, wait for it to be updated", vgId, uEntry.taskId);
          hasUnupdated = true;
        }
      } else {
        tqError("vgId:%d s-task:0x%x not exists, ignore update", vgId, *pTaskId);
      }
    }
  }

  int32_t numOfActualTasks = streamMetaGetNumOfTasks(pMeta);
  if (numOfActualTasks < reqUpdateTasks) {
    tqInfo("vgId:%d req updated tasks from mnode-side:%d to vnode-side:%d", vgId, updateTasks, numOfActualTasks);
    reqUpdateTasks = numOfActualTasks;
  }

  if (restored && isLeader) {
    tqDebug("vgId:%d s-task:0x%x update epset transId:%d, set the restart flag", vgId, req.taskId, req.transId);
    pMeta->startInfo.tasksWillRestart = 1;
  }

  if (hasUnupdated) {
    if (isLeader) {
      tqInfo("vgId:%d closed tasks:%d, unclosed:%d, all tasks will be started when nodeEp update completed", vgId,
              updateTasks, (reqUpdateTasks - updateTasks));
    } else {
      tqInfo("vgId:%d closed tasks:%d, unclosed:%d, follower not restart tasks", vgId, updateTasks,
              (reqUpdateTasks - updateTasks));
    }
  } else {
    if ((code = streamMetaCommit(pMeta)) < 0) {
      // always return true
      streamMetaWUnLock(pMeta);
      tDestroyNodeUpdateMsg(&req);
      tqError("vgId:%d commit meta failed, code:%s not restart the stream tasks", vgId, tstrerror(code));
      return TSDB_CODE_SUCCESS;
    }

    streamMetaClearSetUpdateTaskListComplete(pMeta);

    if (isLeader) {
      if (!restored) {
        tqInfo("vgId:%d vnode restore not completed, not start all tasks", vgId);
      } else {
        tqInfo("vgId:%d all %d task(s) nodeEp updated and closed, transId:%d", vgId, reqUpdateTasks, req.transId);
#if 0
      taosMSleep(5000);// for test purpose, to trigger the leader election
#endif
        code = tqStreamTaskStartAsync(pMeta, cb, true);
        if (code) {
          tqError("vgId:%d async start all tasks, failed, code:%s", vgId, tstrerror(code));
        }
      }
    } else {
      tqInfo("vgId:%d follower nodes not restart tasks", vgId);
    }
  }

  streamMetaWUnLock(pMeta);
  tDestroyNodeUpdateMsg(&req);
  return rsp.code;  // always return true
}

int32_t tqStreamTaskProcessDispatchReq(SStreamMeta* pMeta, SRpcMsg* pMsg, const SMsgCb* msgcb) {
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

  SStreamTask* pTask = NULL;
  int32_t      code = streamMetaAcquireTask(pMeta, req.streamId, req.taskId, &pTask);
  if (pTask && (code == 0)) {
    SRpcMsg rsp = {.info = pMsg->info, .code = 0};
    if (streamProcessDispatchMsg(pTask, &req, &rsp, msgcb) != 0) {
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
      tqError("s-task:0x%x send dispatch error rsp, out of memory", req.taskId);
      return terrno;
    }

    pRspHead->vgId = htonl(req.upstreamNodeId);
    if (pRspHead->vgId == 0) {
      tqError("vgId:%d invalid dispatch msg from upstream to task:0x%x", pMeta->vgId, req.taskId);
      return TSDB_CODE_INVALID_MSG;
    }

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

  SStreamTask* pTask = NULL;
  int32_t      code = streamMetaAcquireTask(pMeta, pRsp->streamId, pRsp->upstreamTaskId, &pTask);
  if (pTask && (code == 0)) {
    code = streamProcessDispatchRsp(pTask, pRsp, pMsg->code);
    streamMetaReleaseTask(pMeta, pTask);
    return code;
  } else {
    tqDebug("vgId:%d failed to handle the dispatch rsp, since find task:0x%x failed", vgId, pRsp->upstreamTaskId);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }
}

int32_t tqStreamTaskProcessRetrieveReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  char*    msgStr = pMsg->pCont;
  char*    msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t  msgLen = pMsg->contLen - sizeof(SMsgHead);
  int32_t  code = 0;
  SDecoder decoder;

  SStreamRetrieveReq req;
  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  code = tDecodeStreamRetrieveReq(&decoder, &req);
  tDecoderClear(&decoder);

  if (code) {
    tqError("vgId:%d failed to decode retrieve msg, discard it", pMeta->vgId);
    return code;
  }

  SStreamTask* pTask = NULL;
  code = streamMetaAcquireTask(pMeta, req.streamId, req.dstTaskId, &pTask);
  if (pTask == NULL || code != 0) {
    tqError("vgId:%d process retrieve req, failed to acquire task:0x%x, it may have been dropped already", pMeta->vgId,
            req.dstTaskId);
    tCleanupStreamRetrieveReq(&req);
    return code;
  }

  // enqueue
  tqDebug("s-task:%s (vgId:%d level:%d) recv retrieve req from task:0x%x(vgId:%d), QID:0x%" PRIx64, pTask->id.idStr,
          pTask->pMeta->vgId, pTask->info.taskLevel, req.srcTaskId, req.srcNodeId, req.reqId);

  // if task is in ck status, set current ck failed
  streamTaskSetCheckpointFailed(pTask);

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    code = streamProcessRetrieveReq(pTask, &req);
  } else {
    req.srcNodeId = pTask->info.nodeId;
    req.srcTaskId = pTask->id.taskId;
    code = streamTaskBroadcastRetrieveReq(pTask, &req);
  }

  if (code != TSDB_CODE_SUCCESS) {  // return error not send rsp manually
    tqError("s-task:0x%x vgId:%d failed to process retrieve request from 0x%x, code:%s", req.dstTaskId, req.dstNodeId,
            req.srcTaskId, tstrerror(code));
  } else {  // send rsp manually only on success.
    SRpcMsg rsp = {.info = pMsg->info, .code = 0};
    streamTaskSendRetrieveRsp(&req, &rsp);
  }

  streamMetaReleaseTask(pMeta, pTask);
  tCleanupStreamRetrieveReq(&req);

  // always return success, to disable the auto rsp
  return code;
}

int32_t tqStreamTaskProcessCheckReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  char*   msgStr = pMsg->pCont;
  char*   msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);
  int32_t code = 0;

  SStreamTaskCheckReq req;
  SStreamTaskCheckRsp rsp = {0};

  SDecoder decoder;

  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  code = tDecodeStreamTaskCheckReq(&decoder, &req);
  tDecoderClear(&decoder);

  if (code) {
    tqError("vgId:%d decode check msg failed, not handle this msg", pMeta->vgId);
    return code;
  }

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
  tqDebug("tq task:0x%x (vgId:%d) recv check rsp(QID:0x%" PRIx64 ") from 0x%x (vgId:%d) status %d", rsp.upstreamTaskId,
          rsp.upstreamNodeId, rsp.reqId, rsp.downstreamTaskId, rsp.downstreamNodeId, rsp.status);

  if (!isLeader) {
    tqError("vgId:%d not leader, task:0x%x not handle the check rsp, downstream:0x%x (vgId:%d)", vgId,
            rsp.upstreamTaskId, rsp.downstreamTaskId, rsp.downstreamNodeId);
    return streamMetaAddFailedTask(pMeta, rsp.streamId, rsp.upstreamTaskId, true);
  }

  SStreamTask* pTask = NULL;
  code = streamMetaAcquireTask(pMeta, rsp.streamId, rsp.upstreamTaskId, &pTask);
  if ((pTask == NULL) || (code != 0)) {
    return streamMetaAddFailedTask(pMeta, rsp.streamId, rsp.upstreamTaskId, true);
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

  SStreamTask* pTask = NULL;
  code = streamMetaAcquireTask(pMeta, req.streamId, req.upstreamTaskId, &pTask);
  if (code != 0) {
    tqError("vgId:%d failed to find s-task:0x%x, it may have been destroyed already", vgId, req.downstreamTaskId);
    return code;
  }

  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    tqDebug("vgId:%d s-task:%s recv invalid the checkpoint-ready msg from task:0x%x (vgId:%d), discard", vgId,
            pTask->id.idStr, req.downstreamTaskId, req.downstreamNodeId);
    streamMetaReleaseTask(pMeta, pTask);
    return TSDB_CODE_INVALID_MSG;
  } else {
    tqDebug("vgId:%d s-task:%s received the checkpoint-ready msg from task:0x%x (vgId:%d), handle it", vgId,
            pTask->id.idStr, req.downstreamTaskId, req.downstreamNodeId);
  }

  code = streamProcessCheckpointReadyMsg(pTask, req.checkpointId, req.downstreamNodeId, req.downstreamTaskId);
  streamMetaReleaseTask(pMeta, pTask);
  if (code) {
    return code;
  }

  {  // send checkpoint ready rsp
    SMStreamCheckpointReadyRspMsg* pReadyRsp = rpcMallocCont(sizeof(SMStreamCheckpointReadyRspMsg));
    if (pReadyRsp == NULL) {
      return terrno;
    }

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
  int32_t numOfTasks = 0;
  int32_t taskId = -1;
  int64_t streamId = -1;
  bool    added = false;
  int32_t size = sizeof(SStreamTask);

  if (tsDisableStream) {
    tqInfo("vgId:%d stream disabled, not deploy stream tasks", vgId);
    return code;
  }

  tqDebug("vgId:%d receive new stream task deploy msg, start to build stream task", vgId);

  // 1.deserialize msg and build task
  SStreamTask* pTask = taosMemoryCalloc(1, size);
  if (pTask == NULL) {
    tqError("vgId:%d failed to create stream task due to out of memory, alloc size:%d", vgId, size);
    return terrno;
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
  taskId = pTask->id.taskId;
  streamId = pTask->id.streamId;

  streamMetaWLock(pMeta);
  code = streamMetaRegisterTask(pMeta, sversion, pTask, &added);
  numOfTasks = streamMetaGetNumOfTasks(pMeta);
  streamMetaWUnLock(pMeta);

  if (code < 0) {
    tqError("vgId:%d failed to register s-task:0x%x into meta, existed tasks:%d, code:%s", vgId, taskId, numOfTasks,
            tstrerror(code));
    return code;
  }

  // added into meta store, pTask cannot be reference since it may have been destroyed by other threads already now if
  // it is added into the meta store
  if (added) {
    // only handled in the leader node
    if (isLeader) {
      tqDebug("vgId:%d s-task:0x%x is deployed and add into meta, numOfTasks:%d", vgId, taskId, numOfTasks);

      if (restored) {
        SStreamTask* p = NULL;
        code = streamMetaAcquireTask(pMeta, streamId, taskId, &p);
        if ((p != NULL) && (code == 0) && (p->info.fillHistory == 0)) {
          code = tqStreamStartOneTaskAsync(pMeta, cb, streamId, taskId);
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
    tqWarn("vgId:%d failed to add s-task:0x%x, since already exists in meta store, total:%d", vgId, taskId, numOfTasks);
  }

  return code;
}

int32_t tqStreamTaskProcessDropReq(SStreamMeta* pMeta, char* msg, int32_t msgLen) {
  SVDropStreamTaskReq* pReq = (SVDropStreamTaskReq*)msg;
  int32_t              code = 0;
  int32_t              vgId = pMeta->vgId;
  STaskId              hTaskId = {0};
  SStreamTask*         pTask = NULL;

  tqDebug("vgId:%d receive msg to drop s-task:0x%x", vgId, pReq->taskId);

  streamMetaWLock(pMeta);

  STaskId id = {.streamId = pReq->streamId, .taskId = pReq->taskId};
  code = streamMetaAcquireTaskUnsafe(pMeta, &id, &pTask);
  if (code == 0) {
    if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
      hTaskId.streamId = pTask->hTaskInfo.id.streamId;
      hTaskId.taskId = pTask->hTaskInfo.id.taskId;
    }

    // clear the relationship, and then release the stream tasks, to avoid invalid accessing of already freed
    // related stream(history) task
    streamTaskSetRemoveBackendFiles(pTask);
    code = streamTaskClearHTaskAttr(pTask, pReq->resetRelHalt);
    streamMetaReleaseTask(pMeta, pTask);

    if (code) {
      tqError("s-task:0x%x failed to clear related fill-history info, still exists", pReq->taskId);
    }
  }

  // drop the related fill-history task firstly
  if (hTaskId.taskId != 0 && hTaskId.streamId != 0) {
    tqDebug("s-task:0x%x vgId:%d drop rel fill-history task:0x%x firstly", pReq->taskId, vgId, (int32_t)hTaskId.taskId);
    code = streamMetaUnregisterTask(pMeta, hTaskId.streamId, hTaskId.taskId);
    if (code) {
      tqDebug("s-task:0x%x vgId:%d drop rel fill-history task:0x%x failed", pReq->taskId, vgId,
              (int32_t)hTaskId.taskId);
    }
  }

  // drop the stream task now
  code = streamMetaUnregisterTask(pMeta, pReq->streamId, pReq->taskId);
  if (code) {
    tqDebug("s-task:0x%x vgId:%d drop task failed", pReq->taskId, vgId);
  }

  // commit the update
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
  tqDebug("vgId:%d task:0x%x dropped, remain tasks:%d", vgId, pReq->taskId, numOfTasks);
  if (numOfTasks == 0) {
    streamMetaResetStartInfo(&pMeta->startInfo, vgId);
  }

  if (streamMetaCommit(pMeta) < 0) {
    // persist to disk
  }

  streamMetaWUnLock(pMeta);
  tqDebug("vgId:%d process drop task:0x%x completed", vgId, pReq->taskId);

  return 0;  // always return success
}

int32_t tqStreamTaskProcessUpdateCheckpointReq(SStreamMeta* pMeta, bool restored, char* msg) {
  SVUpdateCheckpointInfoReq* pReq = (SVUpdateCheckpointInfoReq*)msg;
  int32_t                    code = 0;
  int32_t                    vgId = pMeta->vgId;
  SStreamTask*               pTask = NULL;

  tqDebug("vgId:%d receive msg to update-checkpoint-info for s-task:0x%x", vgId, pReq->taskId);

  streamMetaWLock(pMeta);

  STaskId id = {.streamId = pReq->streamId, .taskId = pReq->taskId};
  code = streamMetaAcquireTaskUnsafe(pMeta, &id, &pTask);
  if (code == 0) {
    code = streamTaskUpdateTaskCheckpointInfo(pTask, restored, pReq);
    streamMetaReleaseTask(pMeta, pTask);
  } else {  // failed to get the task.
    int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
    tqError(
        "vgId:%d failed to locate the s-task:0x%x to update the checkpoint info, numOfTasks:%d, it may have been "
        "dropped already",
        vgId, pReq->taskId, numOfTasks);
  }

  streamMetaWUnLock(pMeta);
  // always return success when handling the requirement issued by mnode during transaction.
  return TSDB_CODE_SUCCESS;
}

static int32_t restartStreamTasks(SStreamMeta* pMeta, bool isLeader) {
  int32_t         vgId = pMeta->vgId;
  int32_t         code = 0;
  int64_t         st = taosGetTimestampMs();
  STaskStartInfo* pStartInfo = &pMeta->startInfo;

  if (pStartInfo->startAllTasks == 1) {
    // wait for the checkpoint id rsp, this rsp will be expired
    if (pStartInfo->curStage == START_MARK_REQ_CHKPID) {
      SStartTaskStageInfo* pCurStageInfo = taosArrayGetLast(pStartInfo->pStagesList);
      tqInfo("vgId:%d only mark the req consensus checkpointId flag, reqTs:%"PRId64 " ignore and continue", vgId, pCurStageInfo->ts);

      taosArrayClear(pStartInfo->pStagesList);
      pStartInfo->curStage = 0;
      goto _start;

    } else if (pStartInfo->curStage == START_WAIT_FOR_CHKPTID) {
      SStartTaskStageInfo* pCurStageInfo = taosArrayGetLast(pStartInfo->pStagesList);
      tqInfo("vgId:%d already sent consensus-checkpoint msg(waiting for chkptid) expired, reqTs:%" PRId64
             " rsp will be discarded",
             vgId, pCurStageInfo->ts);

      taosArrayClear(pStartInfo->pStagesList);
      pStartInfo->curStage = 0;
      goto _start;

    } else if (pStartInfo->curStage == START_CHECK_DOWNSTREAM) {
      int32_t numOfRecv = taosHashGetSize(pStartInfo->pReadyTaskSet);
      taosHashGetSize(pStartInfo->pFailedTaskSet);

      int32_t newTotal = taosArrayGetSize(pStartInfo->pRecvChkptIdTasks);
      tqDebug(
          "vgId:%d start all tasks procedure is interrupted by transId:%d, wait for partial tasks rsp. recv check "
          "downstream results, received:%d results, total req tasks:%d",
          vgId, pMeta->updateInfo.activeTransId, numOfRecv, newTotal);

      bool allRsp = allCheckDownstreamRspPartial(pStartInfo, newTotal, pMeta->vgId);
      if (allRsp) {
        tqDebug("vgId:%d all partial results received, continue the restart procedure", pMeta->vgId);
        streamMetaResetStartInfo(pStartInfo, vgId);
        goto _start;
      } else {
        pStartInfo->restartCount += 1;
        SStartTaskStageInfo* pCurStageInfo = taosArrayGetLast(pStartInfo->pStagesList);

        tqDebug("vgId:%d in start tasks procedure (check downstream), reqTs:%" PRId64
                ", inc restartCounter by 1 and wait for it completes, "
                "remaining restart:%d",
                vgId, pCurStageInfo->ts, pStartInfo->restartCount);
      }
    } else {
      tqInfo("vgId:%d in start procedure, but not start to do anything yet, do nothing", vgId);
    }

    return TSDB_CODE_SUCCESS;
  }

_start:

  pStartInfo->startAllTasks = 1;
  terrno = 0;
  tqInfo("vgId:%d tasks are all updated and stopped, restart all tasks, triggered by transId:%d, ts:%" PRId64, vgId,
         pMeta->updateInfo.completeTransId, pMeta->updateInfo.completeTs);

  streamMetaClear(pMeta);

  int64_t el = taosGetTimestampMs() - st;
  tqInfo("vgId:%d clear&close stream meta completed, elapsed time:%.3fs", vgId, el / 1000.);

  streamMetaLoadAllTasks(pMeta);

  if (isLeader && !tsDisableStream) {
    code = streamMetaStartAllTasks(pMeta);
  } else {
    streamMetaResetStartInfo(&pMeta->startInfo, pMeta->vgId);
    pStartInfo->restartCount = 0;
    tqInfo("vgId:%d, follower node not start stream tasks or stream is disabled", vgId);
  }

  code = terrno;
  return code;
}

int32_t tqStreamTaskProcessRunReq(SStreamMeta* pMeta, SRpcMsg* pMsg, bool isLeader) {
  int32_t  code = 0;
  int32_t  vgId = pMeta->vgId;
  char*    msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t  len = pMsg->contLen - sizeof(SMsgHead);
  SDecoder decoder;

  SStreamTaskRunReq req = {0};
  tDecoderInit(&decoder, (uint8_t*)msg, len);
  if ((code = tDecodeStreamTaskRunReq(&decoder, &req)) < 0) {
    tqError("vgId:%d failed to decode task run req, code:%s", pMeta->vgId, tstrerror(code));
    tDecoderClear(&decoder);
    return TSDB_CODE_SUCCESS;
  }

  tDecoderClear(&decoder);

  int32_t type = req.reqType;
  if (type == STREAM_EXEC_T_START_ONE_TASK) {
    code = streamMetaStartOneTask(pMeta, req.streamId, req.taskId);
    return 0;
  } else if (type == STREAM_EXEC_T_START_ALL_TASKS) {
    streamMetaWLock(pMeta);
    code = streamMetaStartAllTasks(pMeta);
    streamMetaWUnLock(pMeta);
    return 0;
  } else if (type == STREAM_EXEC_T_RESTART_ALL_TASKS) {
    streamMetaWLock(pMeta);
    code = restartStreamTasks(pMeta, isLeader);
    streamMetaWUnLock(pMeta);
    return 0;
  } else if (type == STREAM_EXEC_T_STOP_ALL_TASKS) {
    code = streamMetaStopAllTasks(pMeta);
    return 0;
  } else if (type == STREAM_EXEC_T_ADD_FAILED_TASK) {
    code = streamMetaAddFailedTask(pMeta, req.streamId, req.taskId, true);
    return code;
  } else if (type == STREAM_EXEC_T_STOP_ONE_TASK) {
    code = streamMetaStopOneTask(pMeta, req.streamId, req.taskId);
    return code;
  } else if (type == STREAM_EXEC_T_RESUME_TASK) {  // task resume to run after idle for a while
    SStreamTask* pTask = NULL;
    code = streamMetaAcquireTask(pMeta, req.streamId, req.taskId, &pTask);

    if (pTask != NULL && (code == 0)) {
      char* pStatus = NULL;
      if (streamTaskReadyToRun(pTask, &pStatus)) {
        int64_t execTs = pTask->status.lastExecTs;
        int32_t idle = taosGetTimestampMs() - execTs;
        tqDebug("s-task:%s task resume to run after idle for:%dms from:%" PRId64, pTask->id.idStr, idle, execTs);

        code = streamResumeTask(pTask);
      } else {
        int8_t status = streamTaskSetSchedStatusInactive(pTask);
        tqDebug("vgId:%d s-task:%s ignore run req since not in ready state, status:%s, sched-status:%d", vgId,
                pTask->id.idStr, pStatus, status);
      }
      streamMetaReleaseTask(pMeta, pTask);
    }

    return code;
  }

  SStreamTask* pTask = NULL;
  code = streamMetaAcquireTask(pMeta, req.streamId, req.taskId, &pTask);
  if ((pTask != NULL) && (code == 0)) {  // even in halt status, the data in inputQ must be processed
    char* p = NULL;
    if (streamTaskReadyToRun(pTask, &p)) {
      tqDebug("vgId:%d s-task:%s status:%s start to process block from inputQ, next checked ver:%" PRId64, vgId,
              pTask->id.idStr, p, pTask->chkInfo.nextProcessVer);
      (void)streamExecTask(pTask);
    } else {
      int8_t status = streamTaskSetSchedStatusInactive(pTask);
      tqDebug("vgId:%d s-task:%s ignore run req since not in ready state, status:%s, sched-status:%d", vgId,
              pTask->id.idStr, p, status);
    }

    streamMetaReleaseTask(pMeta, pTask);
    return 0;
  } else {  // NOTE: pTask->status.schedStatus is not updated since it is not be handled by the run exec.
    // todo add one function to handle this
    tqError("vgId:%d failed to found s-task, taskId:0x%x may have been dropped", vgId, req.taskId);
    return code;
  }
}

int32_t tqStartTaskCompleteCallback(SStreamMeta* pMeta) {
  STaskStartInfo* pStartInfo = &pMeta->startInfo;
  int32_t         vgId = pMeta->vgId;
  bool            scanWal = false;
  int32_t         code = 0;

//  streamMetaWLock(pMeta);
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

      return restartStreamTasks(pMeta, (pMeta->role == NODE_ROLE_LEADER));
    } else {
      if (pStartInfo->restartCount == 0) {
        tqDebug("vgId:%d start all tasks completed in callbackFn, restartCounter is 0", pMeta->vgId);
      } else if (allReady) {
        pStartInfo->restartCount = 0;
        tqDebug("vgId:%d all tasks are ready, reset restartCounter 0, not restart tasks", vgId);
      }

      scanWal = true;
    }
  }

//  streamMetaWUnLock(pMeta);

  return code;
}

int32_t tqStreamTaskProcessTaskResetReq(SStreamMeta* pMeta, char* pMsg) {
  SVResetStreamTaskReq* pReq = (SVResetStreamTaskReq*)pMsg;

  SStreamTask* pTask = NULL;
  int32_t      code = streamMetaAcquireTask(pMeta, pReq->streamId, pReq->taskId, &pTask);
  if (pTask == NULL || (code != 0)) {
    tqError("vgId:%d process task-reset req, failed to acquire task:0x%x, it may have been dropped already",
            pMeta->vgId, pReq->taskId);
    return TSDB_CODE_SUCCESS;
  }

  tqDebug("s-task:%s receive task-reset msg from mnode, reset status and ready for data processing", pTask->id.idStr);

  streamMutexLock(&pTask->lock);

  streamTaskSetFailedCheckpointId(pTask, pReq->chkptId);
  streamTaskClearCheckInfo(pTask, true);

  // clear flag set during do checkpoint, and open inputQ for all upstream tasks
  SStreamTaskState pState = streamTaskGetStatus(pTask);
  if (pState.state == TASK_STATUS__CK) {
    streamTaskSetStatusReady(pTask);
    tqDebug("s-task:%s reset checkpoint status to ready", pTask->id.idStr);
  } else if (pState.state == TASK_STATUS__UNINIT) {
    //    tqDebug("s-task:%s start task by checking downstream tasks", pTask->id.idStr);
    //    tqStreamTaskRestoreCheckpoint(pMeta, pTask->id.streamId, pTask->id.taskId);
    tqDebug("s-task:%s status:%s do nothing after receiving reset-task from mnode", pTask->id.idStr, pState.name);
  } else {
    tqDebug("s-task:%s status:%s do nothing after receiving reset-task from mnode", pTask->id.idStr, pState.name);
  }

  streamMutexUnlock(&pTask->lock);

  streamMetaReleaseTask(pMeta, pTask);
  return TSDB_CODE_SUCCESS;
}

int32_t tqStreamTaskProcessAllTaskStopReq(SStreamMeta* pMeta, SMsgCb* pMsgCb, SRpcMsg* pMsg) {
  int32_t  code = 0;
  int32_t  vgId = pMeta->vgId;
  char*    msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t  len = pMsg->contLen - sizeof(SMsgHead);
  SDecoder decoder;

  SStreamTaskStopReq req = {0};
  tDecoderInit(&decoder, (uint8_t*)msg, len);
  if ((code = tDecodeStreamTaskStopReq(&decoder, &req)) < 0) {
    tqError("vgId:%d failed to decode stop all streams, code:%s", pMeta->vgId, tstrerror(code));
    tDecoderClear(&decoder);
    return TSDB_CODE_SUCCESS;
  }

  tDecoderClear(&decoder);

  // stop all stream tasks, only invoked when trying to drop db
  if (req.streamId <= 0) {
    tqDebug("vgId:%d recv msg to stop all tasks in sync before dropping vnode", vgId);
    code = streamMetaStopAllTasks(pMeta);
    if (code) {
      tqError("vgId:%d failed to stop all tasks, code:%s", vgId, tstrerror(code));
    }

  } else {  // stop only one stream tasks

  }

  // always return success
  return TSDB_CODE_SUCCESS;
}

int32_t tqStreamTaskProcessRetrieveTriggerReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  SRetrieveChkptTriggerReq req = {0};
  SStreamTask*             pTask = NULL;
  char*                    msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t                  len = pMsg->contLen - sizeof(SMsgHead);
  SDecoder                 decoder = {0};

  tDecoderInit(&decoder, (uint8_t*)msg, len);
  if (tDecodeRetrieveChkptTriggerReq(&decoder, &req) < 0) {
    tDecoderClear(&decoder);
    tqError("vgId:%d invalid retrieve checkpoint-trigger req received", pMeta->vgId);
    return TSDB_CODE_INVALID_MSG;
  }
  tDecoderClear(&decoder);

  int32_t code = streamMetaAcquireTask(pMeta, req.streamId, req.upstreamTaskId, &pTask);
  if (pTask == NULL || (code != 0)) {
    tqError("vgId:%d process retrieve checkpoint-trigger, checkpointId:%" PRId64
            " from s-task:0x%x, failed to acquire task:0x%x, it may have been dropped already",
            pMeta->vgId, req.checkpointId, (int32_t)req.downstreamTaskId, req.upstreamTaskId);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  tqDebug("s-task:0x%x recv retrieve checkpoint-trigger msg from downstream s-task:0x%x, checkpointId:%" PRId64,
          req.upstreamTaskId, (int32_t)req.downstreamTaskId, req.checkpointId);

  if (pTask->status.downstreamReady != 1) {
    tqError("s-task:%s not ready for checkpoint-trigger retrieve from 0x%x, since downstream not ready",
            pTask->id.idStr, (int32_t)req.downstreamTaskId);

    code = streamTaskSendCheckpointTriggerMsg(pTask, req.downstreamTaskId, req.downstreamNodeId, &pMsg->info,
                                              TSDB_CODE_STREAM_TASK_IVLD_STATUS);
    streamMetaReleaseTask(pMeta, pTask);
    return code;
  }

  SStreamTaskState pState = streamTaskGetStatus(pTask);
  if (pState.state == TASK_STATUS__CK) {  // recv the checkpoint-source/trigger already
    int32_t transId = 0;
    int64_t checkpointId = 0;

    streamTaskGetActiveCheckpointInfo(pTask, &transId, &checkpointId);
    if (checkpointId != req.checkpointId) {
      tqError("s-task:%s invalid checkpoint-trigger retrieve msg from 0x%" PRIx64 ", current checkpointId:%" PRId64
              " req:%" PRId64,
              pTask->id.idStr, req.downstreamTaskId, checkpointId, req.checkpointId);
      streamMetaReleaseTask(pMeta, pTask);
      return TSDB_CODE_INVALID_MSG;
    }

    if (streamTaskAlreadySendTrigger(pTask, req.downstreamNodeId)) {
      // re-send the lost checkpoint-trigger msg to downstream task
      tqDebug("s-task:%s re-send checkpoint-trigger to:0x%x, checkpointId:%" PRId64 ", transId:%d", pTask->id.idStr,
              (int32_t)req.downstreamTaskId, checkpointId, transId);
      code = streamTaskSendCheckpointTriggerMsg(pTask, req.downstreamTaskId, req.downstreamNodeId, &pMsg->info,
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
      code = streamTaskSendCheckpointTriggerMsg(pTask, req.downstreamTaskId, req.downstreamNodeId, &pMsg->info,
                                                TSDB_CODE_ACTION_IN_PROGRESS);
    }
  } else {  // upstream not recv the checkpoint-source/trigger till now
    if (!(pState.state == TASK_STATUS__READY || pState.state == TASK_STATUS__HALT)) {
      tqFatal("s-task:%s invalid task status:%s", pTask->id.idStr, pState.name);
    }

    tqWarn(
        "s-task:%s not recv checkpoint-source from mnode or checkpoint-trigger from upstream yet, wait for all "
        "upstream sending checkpoint-source/trigger",
        pTask->id.idStr);
    code = streamTaskSendCheckpointTriggerMsg(pTask, req.downstreamTaskId, req.downstreamNodeId, &pMsg->info,
                                              TSDB_CODE_ACTION_IN_PROGRESS);
  }

  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t tqStreamTaskProcessRetrieveTriggerRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  SCheckpointTriggerRsp rsp = {0};
  SStreamTask*          pTask = NULL;
  char*                 msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t               len = pMsg->contLen - sizeof(SMsgHead);
  SDecoder              decoder = {0};

  tDecoderInit(&decoder, (uint8_t*)msg, len);
  if (tDecodeCheckpointTriggerRsp(&decoder, &rsp) < 0) {
    tDecoderClear(&decoder);
    tqError("vgId:%d invalid retrieve checkpoint-trigger rsp received", pMeta->vgId);
    return TSDB_CODE_INVALID_MSG;
  }
  tDecoderClear(&decoder);

  int32_t code = streamMetaAcquireTask(pMeta, rsp.streamId, rsp.taskId, &pTask);
  if (pTask == NULL || (code != 0)) {
    tqError(
        "vgId:%d process retrieve checkpoint-trigger, failed to acquire task:0x%x, it may have been dropped already",
        pMeta->vgId, rsp.taskId);
    return code;
  }

  tqDebug(
      "s-task:%s recv re-send checkpoint-trigger msg through retrieve/rsp channel, upstream:0x%x, checkpointId:%" PRId64
      ", transId:%d",
      pTask->id.idStr, rsp.upstreamTaskId, rsp.checkpointId, rsp.transId);

  code = streamTaskProcessCheckpointTriggerRsp(pTask, &rsp);
  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t tqStreamTaskProcessTaskPauseReq(SStreamMeta* pMeta, char* pMsg) {
  SVPauseStreamTaskReq* pReq = (SVPauseStreamTaskReq*)pMsg;

  SStreamTask* pTask = NULL;
  int32_t      code = streamMetaAcquireTask(pMeta, pReq->streamId, pReq->taskId, &pTask);
  if (pTask == NULL || (code != 0)) {
    tqError("vgId:%d process pause req, failed to acquire task:0x%x, it may have been dropped already", pMeta->vgId,
            pReq->taskId);
    // since task is in [STOP|DROPPING] state, it is safe to assume the pause is active
    return TSDB_CODE_SUCCESS;
  }

  tqDebug("s-task:%s receive pause msg from mnode", pTask->id.idStr);
  streamTaskPause(pTask);

  SStreamTask* pHistoryTask = NULL;
  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    pHistoryTask = NULL;
    code = streamMetaAcquireTask(pMeta, pTask->hTaskInfo.id.streamId, pTask->hTaskInfo.id.taskId, &pHistoryTask);
    if (pHistoryTask == NULL || (code != 0)) {
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
  int32_t      code = 0;

  streamTaskResume(pTask);
  ETaskStatus status = streamTaskGetStatus(pTask).state;

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
      code = streamStartScanHistoryAsync(pTask, igUntreated);
    } else if (level == TASK_LEVEL__SOURCE && (streamQueueGetNumOfItems(pTask->inputq.queue) == 0)) {
      //      code = tqScanWalAsync((STQ*)handle, false);
    } else {
      code = streamTrySchedExec(pTask, false);
    }
  }

  return code;
}

int32_t tqStreamTaskProcessTaskResumeReq(void* handle, int64_t sversion, char* msg, bool fromVnode) {
  SVResumeStreamTaskReq* pReq = (SVResumeStreamTaskReq*)msg;

  SStreamMeta* pMeta = fromVnode ? ((STQ*)handle)->pStreamMeta : handle;

  SStreamTask* pTask = NULL;
  int32_t      code = streamMetaAcquireTask(pMeta, pReq->streamId, pReq->taskId, &pTask);
  if (pTask == NULL || (code != 0)) {
    tqError("s-task:0x%x failed to acquire task to resume, it may have been dropped or stopped", pReq->taskId);
    return TSDB_CODE_SUCCESS;
  }

  streamMutexLock(&pTask->lock);
  SStreamTaskState pState = streamTaskGetStatus(pTask);
  tqDebug("s-task:%s start to resume from paused, current status:%s", pTask->id.idStr, pState.name);
  streamMutexUnlock(&pTask->lock);

  code = tqProcessTaskResumeImpl(handle, pTask, sversion, pReq->igUntreated, fromVnode);
  if (code != 0) {
    streamMetaReleaseTask(pMeta, pTask);
    tqError("s-task:%s failed to resume tasks, code:%s", pTask->id.idStr, tstrerror(code));
    return TSDB_CODE_SUCCESS;
  }

  STaskId*     pHTaskId = &pTask->hTaskInfo.id;
  SStreamTask* pHTask = NULL;
  code = streamMetaAcquireTask(pMeta, pHTaskId->streamId, pHTaskId->taskId, &pHTask);
  if (pHTask && (code == 0)) {
    streamMutexLock(&pHTask->lock);
    SStreamTaskState p = streamTaskGetStatus(pHTask);
    tqDebug("s-task:%s related history task start to resume from paused, current status:%s", pHTask->id.idStr, p.name);
    streamMutexUnlock(&pHTask->lock);

    code = tqProcessTaskResumeImpl(handle, pHTask, sversion, pReq->igUntreated, fromVnode);
    tqDebug("s-task:%s resume complete, code:%s", pHTask->id.idStr, tstrerror(code));

    streamMetaReleaseTask(pMeta, pHTask);
  }

  streamMetaReleaseTask(pMeta, pTask);
  return TSDB_CODE_SUCCESS;
}

int32_t tqStreamTasksGetTotalNum(SStreamMeta* pMeta) { return taosArrayGetSize(pMeta->pTaskList); }

int32_t doProcessDummyRspMsg(SStreamMeta* UNUSED_PARAM(pMeta), SRpcMsg* pMsg) {
  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = NULL;
  return TSDB_CODE_SUCCESS;
}

int32_t tqStreamProcessStreamHbRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  SMStreamHbRspMsg rsp = {0};
  int32_t          code = 0;
  SDecoder         decoder;
  char*            msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t          len = pMsg->contLen - sizeof(SMsgHead);

  tDecoderInit(&decoder, (uint8_t*)msg, len);
  code = tDecodeStreamHbRsp(&decoder, &rsp);
  if (code < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    tDecoderClear(&decoder);
    tqError("vgId:%d failed to parse hb rsp msg, code:%s", pMeta->vgId, tstrerror(terrno));
    return terrno;
  }

  tDecoderClear(&decoder);
  return streamProcessHeartbeatRsp(pMeta, &rsp);
}

int32_t tqStreamProcessReqCheckpointRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) { return doProcessDummyRspMsg(pMeta, pMsg); }

int32_t tqStreamProcessChkptReportRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) { return doProcessDummyRspMsg(pMeta, pMsg); }

int32_t tqStreamProcessCheckpointReadyRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  SMStreamCheckpointReadyRspMsg* pRsp = pMsg->pCont;

  SStreamTask* pTask = NULL;
  int32_t      code = streamMetaAcquireTask(pMeta, pRsp->streamId, pRsp->downstreamTaskId, &pTask);
  if (pTask == NULL || (code != 0)) {
    tqError("vgId:%d failed to acquire task:0x%x when handling checkpoint-ready msg, it may have been dropped",
            pRsp->downstreamNodeId, pRsp->downstreamTaskId);
    return code;
  }

  code = streamTaskProcessCheckpointReadyRsp(pTask, pRsp->upstreamTaskId, pRsp->checkpointId);
  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t tqStreamTaskProcessConsenChkptIdReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  int32_t                vgId = pMeta->vgId;
  int32_t                code = 0;
  SStreamTask*           pTask = NULL;
  char*                  msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t                len = pMsg->contLen - sizeof(SMsgHead);
  int64_t                now = taosGetTimestampMs();
  SDecoder               decoder;
  SRestoreCheckpointInfo req = {0};

  tDecoderInit(&decoder, (uint8_t*)msg, len);
  if ((code = tDecodeRestoreCheckpointInfo(&decoder, &req)) < 0) {
    tqError("vgId:%d failed to decode set consensus checkpointId req, code:%s", vgId, tstrerror(code));
    tDecoderClear(&decoder);
    return TSDB_CODE_SUCCESS;
  }

  tDecoderClear(&decoder);

  code = streamMetaAcquireTask(pMeta, req.streamId, req.taskId, &pTask);
  if (pTask == NULL || (code != 0)) {
    // ignore this code to avoid error code over writing
    if (pMeta->role == NODE_ROLE_LEADER) {
      tqError("vgId:%d process consensus checkpointId req:%" PRId64
              " transId:%d, failed to acquire task:0x%x, it may have been dropped/stopped already",
              pMeta->vgId, req.checkpointId, req.transId, req.taskId);

      int32_t ret = streamMetaAddFailedTask(pMeta, req.streamId, req.taskId, true);
      if (ret) {
        tqError("s-task:0x%x failed add check downstream failed, core:%s", req.taskId, tstrerror(ret));
      }
    } else {
      tqDebug("vgId:%d task:0x%x stopped in follower node, not set the consensus checkpointId:%" PRId64 " transId:%d",
              pMeta->vgId, req.taskId, req.checkpointId, req.transId);
    }

    return 0;
  }

  // discard the rsp, since it is expired.
  if (req.startTs < pTask->execInfo.created) {
    tqWarn("s-task:%s vgId:%d createTs:%" PRId64 " recv expired consensus checkpointId:%" PRId64
           " from task createTs:%" PRId64 " < task createTs:%" PRId64 ", discard",
           pTask->id.idStr, pMeta->vgId, pTask->execInfo.created, req.checkpointId, req.startTs,
           pTask->execInfo.created);
    if (pMeta->role == NODE_ROLE_LEADER) {
      streamMetaAddFailedTaskSelf(pTask, now, true);
    }

    streamMetaReleaseTask(pMeta, pTask);
    return TSDB_CODE_SUCCESS;
  }

  tqInfo("s-task:%s vgId:%d checkpointId:%" PRId64 " restore to consensus-checkpointId:%" PRId64
          " transId:%d from mnode, reqTs:%" PRId64 " task createTs:%" PRId64,
          pTask->id.idStr, vgId, pTask->chkInfo.checkpointId, req.checkpointId, req.transId, req.startTs,
          pTask->execInfo.created);

  streamMutexLock(&pTask->lock);
  SConsenChkptInfo* pConsenInfo = &pTask->status.consenChkptInfo;

  if (pTask->chkInfo.checkpointId < req.checkpointId) {
    tqFatal("s-task:%s vgId:%d invalid consensus-checkpointId:%" PRId64 ", greater than existed checkpointId:%" PRId64,
            pTask->id.idStr, vgId, req.checkpointId, pTask->chkInfo.checkpointId);

    streamMutexUnlock(&pTask->lock);
    streamMetaReleaseTask(pMeta, pTask);
    return 0;
  }

  if (pConsenInfo->consenChkptTransId >= req.transId) {
    tqWarn("s-task:%s vgId:%d latest consensus transId:%d, expired consensus trans:%d, discard", pTask->id.idStr, vgId,
            pConsenInfo->consenChkptTransId, req.transId);
    streamMutexUnlock(&pTask->lock);
    streamMetaReleaseTask(pMeta, pTask);
    return TSDB_CODE_SUCCESS;
  }

  if (pTask->chkInfo.checkpointId != req.checkpointId) {
    tqDebug("s-task:%s vgId:%d update the checkpoint from %" PRId64 " to %" PRId64 " transId:%d", pTask->id.idStr, vgId,
            pTask->chkInfo.checkpointId, req.checkpointId, req.transId);
    pTask->chkInfo.checkpointId = req.checkpointId;
    tqSetRestoreVersionInfo(pTask);
  } else {
    tqDebug("s-task:%s vgId:%d consensus-checkpointId:%" PRId64 " equals to current id, transId:%d not update",
            pTask->id.idStr, vgId, req.checkpointId, req.transId);
  }

  streamTaskSetConsenChkptIdRecv(pTask, req.transId, now);
  streamMutexUnlock(&pTask->lock);

  streamMetaWLock(pTask->pMeta);
  if (pMeta->startInfo.curStage == START_WAIT_FOR_CHKPTID) {
    pMeta->startInfo.curStage = START_CHECK_DOWNSTREAM;

    SStartTaskStageInfo info = {.stage = pMeta->startInfo.curStage, .ts = now};
    taosArrayPush(pMeta->startInfo.pStagesList, &info);

    tqDebug("vgId:%d wait_for_chkptId stage -> check_down_stream stage, reqTs:%" PRId64 " , numOfStageHist:%d",
            pMeta->vgId, info.ts, (int32_t)taosArrayGetSize(pMeta->startInfo.pStagesList));
  }

  if (pMeta->role == NODE_ROLE_LEADER) {
    STaskId id = {.streamId = req.streamId, .taskId = req.taskId};

    bool exist = false;
    for (int32_t i = 0; i < taosArrayGetSize(pMeta->startInfo.pRecvChkptIdTasks); ++i) {
      STaskId* pId = taosArrayGet(pMeta->startInfo.pRecvChkptIdTasks, i);
      if (id.streamId == pId->streamId && id.taskId == pId->taskId) {
        exist = true;
        break;
      }
    }

    if (!exist) {
      void* p = taosArrayPush(pMeta->startInfo.pRecvChkptIdTasks, &id);
      if (p == NULL) {  // todo handle error, not record the newly attached, start may dead-lock
        tqError("s-task:0x%x failed to add into recv checkpointId task list, code:%s", req.taskId, tstrerror(code));
      } else {
        int32_t num = taosArrayGetSize(pMeta->startInfo.pRecvChkptIdTasks);
        tqDebug("s-task:0x%x vgId:%d added into recv checkpointId task list, already recv %d", req.taskId, req.nodeId,
                num);
      }
    } else {
      int32_t num = taosArrayGetSize(pMeta->startInfo.pRecvChkptIdTasks);
      tqDebug("s-task:0x%x vgId:%d already exist in recv consensus checkpontId, total existed:%d", req.taskId,
              req.nodeId, num);
    }

    code = tqStreamStartOneTaskAsync(pMeta, pTask->pMsgCb, req.streamId, req.taskId);
    if (code != 0) {
      // todo remove the newly added, otherwise, deadlock exist
      tqError("s-task:0x%x vgId:%d failed start task async, code:%s", req.taskId, vgId, tstrerror(code));
    }
  } else {
    tqDebug("vgId:%d follower not start task:%s", vgId, pTask->id.idStr);
  }

  streamMetaWUnLock(pTask->pMeta);

  streamMetaReleaseTask(pMeta, pTask);
  return 0;
}
