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

#define CHECK_NOT_RSP_DURATION 10 * 1000  // 10 sec

static void    processDownstreamReadyRsp(SStreamTask* pTask);
static int32_t addIntoNodeUpdateList(SStreamTask* pTask, int32_t nodeId);
static void    rspMonitorFn(void* param, void* tmrId);
static void    streamTaskInitTaskCheckInfo(STaskCheckInfo* pInfo, STaskOutputInfo* pOutputInfo, int64_t startTs);
static int32_t streamTaskStartCheckDownstream(STaskCheckInfo* pInfo, const char* id);
static void    streamTaskCompleteCheckRsp(STaskCheckInfo* pInfo, bool lock, const char* id);
static void    streamTaskAddReqInfo(STaskCheckInfo* pInfo, int64_t reqId, int32_t taskId, int32_t vgId, const char* id);
static int32_t doSendCheckMsg(SStreamTask* pTask, SDownstreamStatusInfo* p);
static void    handleTimeoutDownstreamTasks(SStreamTask* pTask, SArray* pTimeoutList);
static void    handleNotReadyDownstreamTask(SStreamTask* pTask, SArray* pNotReadyList);
static int32_t streamTaskUpdateCheckInfo(STaskCheckInfo* pInfo, int32_t taskId, int32_t status, int64_t rspTs,
                                         int64_t reqId, int32_t* pNotReady, const char* id);
static void setCheckDownstreamReqInfo(SStreamTaskCheckReq* pReq, int64_t reqId, int32_t dstTaskId, int32_t dstNodeId);
static void getCheckRspStatus(STaskCheckInfo* pInfo, int64_t el, int32_t* numOfReady, int32_t* numOfFault,
                              int32_t* numOfNotRsp, SArray* pTimeoutList, SArray* pNotReadyList, const char* id);
static int32_t addDownstreamFailedStatusResultAsync(SMsgCb* pMsgCb, int32_t vgId, int64_t streamId, int32_t taskId);
static void    findCheckRspStatus(STaskCheckInfo* pInfo, int32_t taskId, SDownstreamStatusInfo** pStatusInfo);

int32_t streamTaskCheckStatus(SStreamTask* pTask, int32_t upstreamTaskId, int32_t vgId, int64_t stage,
                              int64_t* oldStage) {
  SStreamUpstreamEpInfo* pInfo = NULL;
  streamTaskGetUpstreamTaskEpInfo(pTask, upstreamTaskId, &pInfo);
  if (pInfo == NULL) {
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  *oldStage = pInfo->stage;
  const char* id = pTask->id.idStr;
  if (stage == -1) {
    stDebug("s-task:%s receive check msg from upstream task:0x%x(vgId:%d), invalid stageId:%" PRId64 ", not ready", id,
            upstreamTaskId, vgId, stage);
    return 0;
  }

  if (pInfo->stage == -1) {
    pInfo->stage = stage;
    stDebug("s-task:%s receive check msg from upstream task:0x%x(vgId:%d) first time, init stage value:%" PRId64, id,
            upstreamTaskId, vgId, stage);
  }

  if (pInfo->stage < stage) {
    stError("s-task:%s receive check msg from upstream task:0x%x(vgId:%d), new stage received:%" PRId64
            ", prev:%" PRId64,
            id, upstreamTaskId, vgId, stage, pInfo->stage);
    // record the checkpoint failure id and sent to mnode
    streamTaskSetCheckpointFailed(pTask);
  }

  if (pInfo->stage != stage) {
    return TASK_UPSTREAM_NEW_STAGE;
  } else if (pTask->status.downstreamReady != 1) {
    stDebug("s-task:%s vgId:%d leader:%d, downstream not ready", id, vgId, (pTask->pMeta->role == NODE_ROLE_LEADER));
    return TASK_DOWNSTREAM_NOT_READY;
  } else {
    return TASK_DOWNSTREAM_READY;
  }
}

// check status
void streamTaskSendCheckMsg(SStreamTask* pTask) {
  SDataRange*  pRange = &pTask->dataRange;
  STimeWindow* pWindow = &pRange->window;
  const char*  idstr = pTask->id.idStr;
  int32_t      code = 0;

  SStreamTaskCheckReq req = {
      .streamId = pTask->id.streamId,
      .upstreamTaskId = pTask->id.taskId,
      .upstreamNodeId = pTask->info.nodeId,
      .childId = pTask->info.selfChildId,
      .stage = pTask->pMeta->stage,
  };

  // serialize streamProcessScanHistoryFinishRsp
  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    streamTaskStartMonitorCheckRsp(pTask);

    STaskDispatcherFixed* pDispatch = &pTask->outputInfo.fixedDispatcher;

    setCheckDownstreamReqInfo(&req, tGenIdPI64(), pDispatch->taskId, pDispatch->nodeId);
    streamTaskAddReqInfo(&pTask->taskCheckInfo, req.reqId, pDispatch->taskId, pDispatch->nodeId, idstr);

    stDebug("s-task:%s (vgId:%d) stage:%" PRId64 " check single downstream task:0x%x(vgId:%d) ver:%" PRId64 "-%" PRId64
            " window:%" PRId64 "-%" PRId64 " QID:0x%" PRIx64,
            idstr, pTask->info.nodeId, req.stage, req.downstreamTaskId, req.downstreamNodeId, pRange->range.minVer,
            pRange->range.maxVer, pWindow->skey, pWindow->ekey, req.reqId);

    code = streamSendCheckMsg(pTask, &req, pTask->outputInfo.fixedDispatcher.nodeId,
                              &pTask->outputInfo.fixedDispatcher.epSet);

  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    streamTaskStartMonitorCheckRsp(pTask);

    SArray* vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgs = taosArrayGetSize(vgInfo);
    stDebug("s-task:%s check %d downstream tasks, ver:%" PRId64 "-%" PRId64 " window:%" PRId64 "-%" PRId64, idstr,
            numOfVgs, pRange->range.minVer, pRange->range.maxVer, pWindow->skey, pWindow->ekey);

    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      if (pVgInfo == NULL) {
        continue;
      }

      setCheckDownstreamReqInfo(&req, tGenIdPI64(), pVgInfo->taskId, pVgInfo->vgId);
      streamTaskAddReqInfo(&pTask->taskCheckInfo, req.reqId, pVgInfo->taskId, pVgInfo->vgId, idstr);

      stDebug("s-task:%s (vgId:%d) stage:%" PRId64
              " check downstream task:0x%x (vgId:%d) (shuffle), idx:%d, QID:0x%" PRIx64,
              idstr, pTask->info.nodeId, req.stage, req.downstreamTaskId, req.downstreamNodeId, i, req.reqId);
      code = streamSendCheckMsg(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
    }
  } else {  // for sink task, set it ready directly.
    stDebug("s-task:%s (vgId:%d) set downstream ready, since no downstream", idstr, pTask->info.nodeId);
    streamTaskStopMonitorCheckRsp(&pTask->taskCheckInfo, idstr);
    processDownstreamReadyRsp(pTask);
  }

  if (code) {
    stError("s-task:%s failed to send check msg to downstream, code:%s", idstr, tstrerror(code));
  }
}

void streamTaskProcessCheckMsg(SStreamMeta* pMeta, SStreamTaskCheckReq* pReq, SStreamTaskCheckRsp* pRsp) {
  int32_t taskId = pReq->downstreamTaskId;

  *pRsp = (SStreamTaskCheckRsp){
      .reqId = pReq->reqId,
      .streamId = pReq->streamId,
      .childId = pReq->childId,
      .downstreamNodeId = pReq->downstreamNodeId,
      .downstreamTaskId = pReq->downstreamTaskId,
      .upstreamNodeId = pReq->upstreamNodeId,
      .upstreamTaskId = pReq->upstreamTaskId,
  };

  // only the leader node handle the check request
  if (pMeta->role == NODE_ROLE_FOLLOWER) {
    stError(
        "s-task:0x%x invalid check msg from upstream:0x%x(vgId:%d), vgId:%d is follower, not handle check status msg",
        taskId, pReq->upstreamTaskId, pReq->upstreamNodeId, pMeta->vgId);
    pRsp->status = TASK_DOWNSTREAM_NOT_LEADER;
  } else {
    SStreamTask* pTask = NULL;
    int32_t      code = streamMetaAcquireTask(pMeta, pReq->streamId, taskId, &pTask);
    if (pTask != NULL) {
      pRsp->status =
          streamTaskCheckStatus(pTask, pReq->upstreamTaskId, pReq->upstreamNodeId, pReq->stage, &pRsp->oldStage);

      SStreamTaskState pState = streamTaskGetStatus(pTask);
      stDebug("s-task:%s status:%s, stage:%" PRId64 " recv task check req(QID:0x%" PRIx64
              ") task:0x%x (vgId:%d), check_status:%d",
              pTask->id.idStr, pState.name, pRsp->oldStage, pRsp->reqId, pRsp->upstreamTaskId, pRsp->upstreamNodeId,
              pRsp->status);
      streamMetaReleaseTask(pMeta, pTask);
    } else {
      pRsp->status = TASK_DOWNSTREAM_NOT_READY;
      stDebug("tq recv task check(taskId:0x%" PRIx64 "-0x%x not built yet) req(QID:0x%" PRIx64
              ") from task:0x%x (vgId:%d), rsp check_status %d",
              pReq->streamId, taskId, pRsp->reqId, pRsp->upstreamTaskId, pRsp->upstreamNodeId, pRsp->status);
    }
  }
}

int32_t streamTaskProcessCheckRsp(SStreamTask* pTask, const SStreamTaskCheckRsp* pRsp) {
  int64_t         now = taosGetTimestampMs();
  const char*     id = pTask->id.idStr;
  STaskCheckInfo* pInfo = &pTask->taskCheckInfo;
  int32_t         total = streamTaskGetNumOfDownstream(pTask);
  int32_t         left = -1;

  if (streamTaskShouldStop(pTask)) {
    stDebug("s-task:%s should stop, do not do check downstream again", id);
    return TSDB_CODE_SUCCESS;
  }

  if (pTask->id.taskId != pRsp->upstreamTaskId) {
    stError("s-task:%s invalid check downstream rsp, upstream task:0x%x discard", id, pRsp->upstreamTaskId);
    return TSDB_CODE_INVALID_MSG;
  }

  if (pRsp->status == TASK_DOWNSTREAM_READY) {
    int32_t code = streamTaskUpdateCheckInfo(pInfo, pRsp->downstreamTaskId, pRsp->status, now, pRsp->reqId, &left, id);
    if (code != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_SUCCESS;
    }

    if (left == 0) {
      processDownstreamReadyRsp(pTask);  // all downstream tasks are ready, set the complete check downstream flag
      streamTaskStopMonitorCheckRsp(pInfo, id);
    } else {
      stDebug("s-task:%s (vgId:%d) recv check rsp from task:0x%x (vgId:%d) status:%d, total:%d not ready:%d", id,
              pRsp->upstreamNodeId, pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->status, total, left);
    }
  } else {  // not ready, wait for 100ms and retry
    int32_t code = streamTaskUpdateCheckInfo(pInfo, pRsp->downstreamTaskId, pRsp->status, now, pRsp->reqId, &left, id);
    if (code != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_SUCCESS;  // return success in any cases.
    }

    if (pRsp->status == TASK_UPSTREAM_NEW_STAGE || pRsp->status == TASK_DOWNSTREAM_NOT_LEADER) {
      if (pRsp->status == TASK_UPSTREAM_NEW_STAGE) {
        stError("s-task:%s vgId:%d self vnode-transfer/leader-change/restart detected, old stage:%" PRId64
                ", current stage:%" PRId64 ", not check wait for downstream task nodeUpdate, and all tasks restart",
                id, pRsp->upstreamNodeId, pRsp->oldStage, pTask->pMeta->stage);
        code = addIntoNodeUpdateList(pTask, pRsp->upstreamNodeId);
      } else {
        stError(
            "s-task:%s downstream taskId:0x%x (vgId:%d) not leader, self dispatch epset needs to be updated, not check "
            "downstream again, nodeUpdate needed",
            id, pRsp->downstreamTaskId, pRsp->downstreamNodeId);
        code = addIntoNodeUpdateList(pTask, pRsp->downstreamNodeId);
      }

      streamMetaAddFailedTaskSelf(pTask, now);
    } else {  // TASK_DOWNSTREAM_NOT_READY, rsp-check monitor will retry in 300 ms
      stDebug("s-task:%s (vgId:%d) recv check rsp from task:0x%x (vgId:%d) status:%d, total:%d not ready:%d", id,
              pRsp->upstreamNodeId, pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->status, total, left);
    }
  }

  return 0;
}

int32_t streamTaskSendCheckRsp(const SStreamMeta* pMeta, int32_t vgId, SStreamTaskCheckRsp* pRsp,
                               SRpcHandleInfo* pRpcInfo, int32_t taskId) {
  SEncoder encoder;
  int32_t  code = 0;
  int32_t  len;

  tEncodeSize(tEncodeStreamTaskCheckRsp, pRsp, len, code);
  if (code < 0) {
    stError("vgId:%d failed to encode task check rsp, s-task:0x%x", pMeta->vgId, taskId);
    return TSDB_CODE_INVALID_MSG;
  }

  void* buf = rpcMallocCont(sizeof(SMsgHead) + len);
  if (buf == NULL) {
    stError("s-task:0x%x vgId:%d failed prepare msg, %s at line:%d code:%s", taskId, pMeta->vgId, __func__, __LINE__,
            tstrerror(code));
    return terrno;
  }

  ((SMsgHead*)buf)->vgId = htonl(vgId);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncoderInit(&encoder, (uint8_t*)abuf, len);
  code = tEncodeStreamTaskCheckRsp(&encoder, pRsp);
  tEncoderClear(&encoder);

  SRpcMsg rspMsg = {.code = 0, .pCont = buf, .contLen = sizeof(SMsgHead) + len, .info = *pRpcInfo};
  tmsgSendRsp(&rspMsg);

  code = TMIN(code, 0);
  return code;
}

void streamTaskStartMonitorCheckRsp(SStreamTask* pTask) {
  int32_t         vgId = pTask->pMeta->vgId;
  STaskCheckInfo* pInfo = &pTask->taskCheckInfo;

  streamMutexLock(&pInfo->checkInfoLock);

  // drop procedure already started, not start check downstream now
  ETaskStatus s = streamTaskGetStatus(pTask).state;
  if (s == TASK_STATUS__DROPPING) {
    stDebug("s-task:%s task not in uninit status, status:%s not start monitor check-rsp", pTask->id.idStr,
            streamTaskGetStatusStr(s));
    streamMutexUnlock(&pInfo->checkInfoLock);
    return;
  }

  int32_t code = streamTaskStartCheckDownstream(pInfo, pTask->id.idStr);
  if (code != TSDB_CODE_SUCCESS) {
    streamMutexUnlock(&pInfo->checkInfoLock);
    return;
  }

  streamTaskInitTaskCheckInfo(pInfo, &pTask->outputInfo, taosGetTimestampMs());

  int64_t* pTaskRefId = NULL;
  code = streamTaskAllocRefId(pTask, &pTaskRefId);
  if (code == 0) {
    streamTmrStart(rspMonitorFn, CHECK_RSP_CHECK_INTERVAL, pTaskRefId, streamTimer, &pInfo->checkRspTmr, vgId,
                   "check-status-monitor");
  }

  streamMutexUnlock(&pInfo->checkInfoLock);
}

void streamTaskStopMonitorCheckRsp(STaskCheckInfo* pInfo, const char* id) {
  streamMutexLock(&pInfo->checkInfoLock);
  pInfo->stopCheckProcess = 1;
  streamMutexUnlock(&pInfo->checkInfoLock);

  stDebug("s-task:%s set stop check-rsp monitor flag", id);
}

void streamTaskCleanupCheckInfo(STaskCheckInfo* pInfo) {
  taosArrayDestroy(pInfo->pList);
  pInfo->pList = NULL;

  if (pInfo->checkRspTmr != NULL) {
    streamTmrStop(pInfo->checkRspTmr);
    pInfo->checkRspTmr = NULL;
  }

  streamMutexDestroy(&pInfo->checkInfoLock);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void processDownstreamReadyRsp(SStreamTask* pTask) {
  EStreamTaskEvent event = (pTask->info.fillHistory == 0) ? TASK_EVENT_INIT : TASK_EVENT_INIT_SCANHIST;
  int32_t          code = streamTaskOnHandleEventSuccess(pTask->status.pSM, event, NULL, NULL);
  if (code) {
    stError("s-task:%s failed to set event succ, code:%s", pTask->id.idStr, tstrerror(code));
  }

  int64_t checkTs = pTask->execInfo.checkTs;
  int64_t readyTs = pTask->execInfo.readyTs;
  code = streamMetaAddTaskLaunchResult(pTask->pMeta, pTask->id.streamId, pTask->id.taskId, checkTs, readyTs, true);
  if (code) {
    stError("s-task:%s failed to record the downstream task status, code:%s", pTask->id.idStr, tstrerror(code));
  }

  if (pTask->status.taskStatus == TASK_STATUS__HALT) {
    if (!HAS_RELATED_FILLHISTORY_TASK(pTask) || (pTask->info.fillHistory != 0)) {
      stError("s-task:%s status:halt fillhistory:%d not handle the ready rsp", pTask->id.idStr,
              pTask->info.fillHistory);
    }

    // halt it self for count window stream task until the related fill history task completed.
    stDebug("s-task:%s level:%d initial status is %s from mnode, set it to be halt", pTask->id.idStr,
            pTask->info.taskLevel, streamTaskGetStatusStr(pTask->status.taskStatus));
    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_HALT);
    if (code != 0) {  // todo: handle error
      stError("s-task:%s failed to handle halt event, code:%s", pTask->id.idStr, tstrerror(code));
    }
  }

  // start the related fill-history task, when current task is ready
  // not invoke in success callback due to the deadlock.
  // todo: let's retry
  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    stDebug("s-task:%s try to launch related fill-history task", pTask->id.idStr);
    code = streamLaunchFillHistoryTask(pTask);
    if (code) {
      stError("s-task:%s failed to launch history task, code:%s", pTask->id.idStr, tstrerror(code));
    }
  }
}

int32_t addIntoNodeUpdateList(SStreamTask* pTask, int32_t nodeId) {
  int32_t vgId = pTask->pMeta->vgId;
  int32_t code = 0;
  ;
  bool existed = false;

  streamMutexLock(&pTask->lock);

  int32_t num = taosArrayGetSize(pTask->outputInfo.pNodeEpsetUpdateList);
  for (int i = 0; i < num; ++i) {
    SDownstreamTaskEpset* p = taosArrayGet(pTask->outputInfo.pNodeEpsetUpdateList, i);
    if (p == NULL) {
      continue;
    }

    if (p->nodeId == nodeId) {
      existed = true;
      break;
    }
  }

  if (!existed) {
    SDownstreamTaskEpset t = {.nodeId = nodeId};

    void* p = taosArrayPush(pTask->outputInfo.pNodeEpsetUpdateList, &t);
    if (p == NULL) {
      code = terrno;
      stError("s-task:%s vgId:%d failed to update epset, code:%s", pTask->id.idStr, vgId, tstrerror(code));
    } else {
      stInfo("s-task:%s vgId:%d downstream nodeId:%d needs to be updated, total needs updated:%d", pTask->id.idStr,
             vgId, t.nodeId, (num + 1));
    }
  }

  streamMutexUnlock(&pTask->lock);
  return code;
}

void streamTaskInitTaskCheckInfo(STaskCheckInfo* pInfo, STaskOutputInfo* pOutputInfo, int64_t startTs) {
  taosArrayClear(pInfo->pList);

  if (pOutputInfo->type == TASK_OUTPUT__FIXED_DISPATCH) {
    pInfo->notReadyTasks = 1;
  } else if (pOutputInfo->type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    pInfo->notReadyTasks = taosArrayGetSize(pOutputInfo->shuffleDispatcher.dbInfo.pVgroupInfos);
  }

  pInfo->startTs = startTs;
  pInfo->timeoutStartTs = startTs;
  pInfo->stopCheckProcess = 0;
}

void findCheckRspStatus(STaskCheckInfo* pInfo, int32_t taskId, SDownstreamStatusInfo** pStatusInfo) {
  if (pStatusInfo == NULL) {
    return;
  }

  *pStatusInfo = NULL;
  for (int32_t j = 0; j < taosArrayGetSize(pInfo->pList); ++j) {
    SDownstreamStatusInfo* p = taosArrayGet(pInfo->pList, j);
    if (p == NULL) {
      continue;
    }

    if (p->taskId == taskId) {
      *pStatusInfo = p;
    }
  }
}

int32_t streamTaskUpdateCheckInfo(STaskCheckInfo* pInfo, int32_t taskId, int32_t status, int64_t rspTs, int64_t reqId,
                                  int32_t* pNotReady, const char* id) {
  SDownstreamStatusInfo* p = NULL;

  streamMutexLock(&pInfo->checkInfoLock);
  findCheckRspStatus(pInfo, taskId, &p);
  if (p != NULL) {
    if (reqId != p->reqId) {
      stError("s-task:%sQID:0x%" PRIx64 " expected:0x%" PRIx64
              " expired check-rsp recv from downstream task:0x%x, discarded",
              id, reqId, p->reqId, taskId);
      streamMutexUnlock(&pInfo->checkInfoLock);
      return TSDB_CODE_FAILED;
    }

    // subtract one not-ready-task, since it is ready now
    if ((p->status != TASK_DOWNSTREAM_READY) && (status == TASK_DOWNSTREAM_READY)) {
      *pNotReady = atomic_sub_fetch_32(&pInfo->notReadyTasks, 1);
    } else {
      *pNotReady = pInfo->notReadyTasks;
    }

    p->status = status;
    p->rspTs = rspTs;

    streamMutexUnlock(&pInfo->checkInfoLock);
    return TSDB_CODE_SUCCESS;
  }

  streamMutexUnlock(&pInfo->checkInfoLock);
  stError("s-task:%s unexpected check rsp msg, invalid downstream task:0x%x,QID:%" PRIx64 " discarded", id, taskId,
          reqId);
  return TSDB_CODE_FAILED;
}

int32_t streamTaskStartCheckDownstream(STaskCheckInfo* pInfo, const char* id) {
  if (pInfo->inCheckProcess == 0) {
    pInfo->inCheckProcess = 1;
  } else {
    stError("s-task:%s already in check procedure, checkTs:%" PRId64 ", start monitor check rsp failed", id,
            pInfo->startTs);
    pInfo->stopCheckProcess = 0;  // disable auto stop of check process
    return TSDB_CODE_FAILED;
  }

  stDebug("s-task:%s set the in check-rsp flag", id);
  return TSDB_CODE_SUCCESS;
}

void streamTaskCompleteCheckRsp(STaskCheckInfo* pInfo, bool lock, const char* id) {
  if (lock) {
    streamMutexLock(&pInfo->checkInfoLock);
  }

  if (pInfo->inCheckProcess) {
    int64_t el = (pInfo->startTs != 0) ? (taosGetTimestampMs() - pInfo->startTs) : 0;
    stDebug("s-task:%s clear the in check-rsp flag, set the check-rsp done, elapsed time:%" PRId64 " ms", id, el);

    pInfo->startTs = 0;
    pInfo->timeoutStartTs = 0;
    pInfo->notReadyTasks = 0;
    pInfo->inCheckProcess = 0;
    pInfo->stopCheckProcess = 0;

    pInfo->notReadyRetryCount = 0;
    pInfo->timeoutRetryCount = 0;

    taosArrayClear(pInfo->pList);
  } else {
    stDebug("s-task:%s already not in check-rsp procedure", id);
  }

  if (lock) {
    streamMutexUnlock(&pInfo->checkInfoLock);
  }
}

// todo: retry until success
void streamTaskAddReqInfo(STaskCheckInfo* pInfo, int64_t reqId, int32_t taskId, int32_t vgId, const char* id) {
  SDownstreamStatusInfo info = {.taskId = taskId, .status = -1, .vgId = vgId, .reqId = reqId, .rspTs = 0};
  streamMutexLock(&pInfo->checkInfoLock);

  SDownstreamStatusInfo* p = NULL;
  findCheckRspStatus(pInfo, taskId, &p);
  if (p != NULL) {
    stDebug("s-task:%s check info to task:0x%x already sent", id, taskId);
    streamMutexUnlock(&pInfo->checkInfoLock);
    return;
  }

  void* px = taosArrayPush(pInfo->pList, &info);
  if (px == NULL) {
    // todo: retry
  }

  streamMutexUnlock(&pInfo->checkInfoLock);
}

int32_t doSendCheckMsg(SStreamTask* pTask, SDownstreamStatusInfo* p) {
  const char* id = pTask->id.idStr;
  int32_t     code = 0;

  SStreamTaskCheckReq req = {
      .streamId = pTask->id.streamId,
      .upstreamTaskId = pTask->id.taskId,
      .upstreamNodeId = pTask->info.nodeId,
      .childId = pTask->info.selfChildId,
      .stage = pTask->pMeta->stage,
  };

  // update the reqId for the new check msg
  p->reqId = tGenIdPI64();

  STaskOutputInfo* pOutputInfo = &pTask->outputInfo;
  if (pOutputInfo->type == TASK_OUTPUT__FIXED_DISPATCH) {
    STaskDispatcherFixed* pDispatch = &pOutputInfo->fixedDispatcher;
    setCheckDownstreamReqInfo(&req, p->reqId, pDispatch->taskId, pDispatch->nodeId);

    stDebug("s-task:%s (vgId:%d) stage:%" PRId64 " re-send check downstream task:0x%x(vgId:%d) QID:0x%" PRIx64, id,
            pTask->info.nodeId, req.stage, req.downstreamTaskId, req.downstreamNodeId, req.reqId);

    code = streamSendCheckMsg(pTask, &req, pOutputInfo->fixedDispatcher.nodeId, &pOutputInfo->fixedDispatcher.epSet);
  } else if (pOutputInfo->type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pOutputInfo->shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t numOfVgs = taosArrayGetSize(vgInfo);

    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      if (pVgInfo == NULL) {
        continue;
      }

      if (p->taskId == pVgInfo->taskId) {
        setCheckDownstreamReqInfo(&req, p->reqId, pVgInfo->taskId, pVgInfo->vgId);

        stDebug("s-task:%s (vgId:%d) stage:%" PRId64
                " re-send check downstream task:0x%x(vgId:%d) (shuffle), idx:%d QID:0x%" PRIx64,
                id, pTask->info.nodeId, req.stage, req.downstreamTaskId, req.downstreamNodeId, i, p->reqId);
        code = streamSendCheckMsg(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
        break;
      }
    }
  }

  if (code) {
    stError("s-task:%s failed to send check msg to downstream, code:%s", pTask->id.idStr, tstrerror(code));
  }
  return code;
}

void getCheckRspStatus(STaskCheckInfo* pInfo, int64_t el, int32_t* numOfReady, int32_t* numOfFault,
                       int32_t* numOfNotRsp, SArray* pTimeoutList, SArray* pNotReadyList, const char* id) {
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pList); ++i) {
    SDownstreamStatusInfo* p = taosArrayGet(pInfo->pList, i);
    if (p == NULL) {
      continue;
    }

    if (p->status == TASK_DOWNSTREAM_READY) {
      (*numOfReady) += 1;
    } else if (p->status == TASK_UPSTREAM_NEW_STAGE || p->status == TASK_DOWNSTREAM_NOT_LEADER) {
      stDebug("s-task:%s recv status:NEW_STAGE/NOT_LEADER from downstream, task:0x%x, quit from check downstream", id,
              p->taskId);
      (*numOfFault) += 1;
    } else {                                 // TASK_DOWNSTREAM_NOT_READY
      if (p->rspTs == 0) {                   // not response yet
        if (el >= CHECK_NOT_RSP_DURATION) {  // not receive info for 10 sec.
          void* px = taosArrayPush(pTimeoutList, &p->taskId);
          if (px == NULL) {
            stError("s-task:%s failed to record time out task:0x%x", id, p->taskId);
          }
        } else {                // el < CHECK_NOT_RSP_DURATION
          (*numOfNotRsp) += 1;  // do nothing and continue waiting for their rsp
        }
      } else {
        void* px = taosArrayPush(pNotReadyList, &p->taskId);
        if (px == NULL) {
          stError("s-task:%s failed to record not ready task:0x%x", id, p->taskId);
        }
      }
    }
  }
}

void setCheckDownstreamReqInfo(SStreamTaskCheckReq* pReq, int64_t reqId, int32_t dstTaskId, int32_t dstNodeId) {
  pReq->reqId = reqId;
  pReq->downstreamTaskId = dstTaskId;
  pReq->downstreamNodeId = dstNodeId;
}

void handleTimeoutDownstreamTasks(SStreamTask* pTask, SArray* pTimeoutList) {
  STaskCheckInfo* pInfo = &pTask->taskCheckInfo;
  const char*     id = pTask->id.idStr;
  int32_t         vgId = pTask->pMeta->vgId;
  int32_t         numOfTimeout = taosArrayGetSize(pTimeoutList);
  int32_t         code = 0;

  pInfo->timeoutStartTs = taosGetTimestampMs();
  for (int32_t i = 0; i < numOfTimeout; ++i) {
    int32_t* px = taosArrayGet(pTimeoutList, i);
    if (px == NULL) {
      continue;
    }

    int32_t                taskId = *px;
    SDownstreamStatusInfo* p = NULL;
    findCheckRspStatus(pInfo, taskId, &p);

    if (p != NULL) {
      if (p->status != -1 || p->rspTs != 0) {
        stError("s-task:%s invalid rsp record entry, index:%d, status:%d, rspTs:%" PRId64, id, i, p->status, p->rspTs);
        continue;
      }
      code = doSendCheckMsg(pTask, p);
    }
  }

  pInfo->timeoutRetryCount += 1;

  // timeout more than 100 sec, add into node update list
  if (pInfo->timeoutRetryCount > 10) {
    pInfo->timeoutRetryCount = 0;

    for (int32_t i = 0; i < numOfTimeout; ++i) {
      int32_t* pTaskId = taosArrayGet(pTimeoutList, i);
      if (pTaskId == NULL) {
        continue;
      }

      SDownstreamStatusInfo* p = NULL;
      findCheckRspStatus(pInfo, *pTaskId, &p);
      if (p != NULL) {
        code = addIntoNodeUpdateList(pTask, p->vgId);
        stDebug("s-task:%s vgId:%d downstream task:0x%x (vgId:%d) timeout more than 100sec, add into nodeUpate list",
                id, vgId, p->taskId, p->vgId);
      }
    }

    stDebug("s-task:%s vgId:%d %d downstream task(s) all add into nodeUpate list", id, vgId, numOfTimeout);
  } else {
    stDebug("s-task:%s vgId:%d %d downstream task(s) timeout, send check msg again, retry:%d start time:%" PRId64, id,
            vgId, numOfTimeout, pInfo->timeoutRetryCount, pInfo->timeoutStartTs);
  }
}

void handleNotReadyDownstreamTask(SStreamTask* pTask, SArray* pNotReadyList) {
  STaskCheckInfo* pInfo = &pTask->taskCheckInfo;
  const char*     id = pTask->id.idStr;
  int32_t         vgId = pTask->pMeta->vgId;
  int32_t         numOfNotReady = taosArrayGetSize(pNotReadyList);

  // reset the info, and send the check msg to failure downstream again
  for (int32_t i = 0; i < numOfNotReady; ++i) {
    int32_t* pTaskId = taosArrayGet(pNotReadyList, i);
    if (pTaskId == NULL) {
      continue;
    }

    SDownstreamStatusInfo* p = NULL;
    findCheckRspStatus(pInfo, *pTaskId, &p);
    if (p != NULL) {
      p->rspTs = 0;
      p->status = -1;
      int32_t code = doSendCheckMsg(pTask, p);
    }
  }

  pInfo->notReadyRetryCount += 1;
  stDebug("s-task:%s vgId:%d %d downstream task(s) not ready, send check msg again, retry:%d start time:%" PRId64, id,
          vgId, numOfNotReady, pInfo->notReadyRetryCount, pInfo->startTs);
}

// the action of add status may incur the restart procedure, which should NEVER be executed in the timer thread.
// The restart of all tasks requires that all tasks should not have active timer for now. Therefore, the execution
// of restart in timer thread will result in a dead lock.
int32_t addDownstreamFailedStatusResultAsync(SMsgCb* pMsgCb, int32_t vgId, int64_t streamId, int32_t taskId) {
  return streamTaskSchedTask(pMsgCb, vgId, streamId, taskId, STREAM_EXEC_T_ADD_FAILED_TASK);
}

static void doCleanup(SStreamTask* pTask, SArray* pNotReadyList, SArray* pTimeoutList, void* param) {
  streamMetaReleaseTask(pTask->pMeta, pTask);

  taosArrayDestroy(pNotReadyList);
  taosArrayDestroy(pTimeoutList);
  streamTaskFreeRefId(param);
}

// this function is executed in timer thread
void rspMonitorFn(void* param, void* tmrId) {
  int32_t         numOfReady = 0;
  int32_t         numOfFault = 0;
  int32_t         numOfNotRsp = 0;
  int32_t         numOfNotReady = 0;
  int32_t         numOfTimeout = 0;
  int64_t         taskRefId = *(int64_t*)param;
  int64_t         now = taosGetTimestampMs();
  SArray*         pNotReadyList = NULL;
  SArray*         pTimeoutList = NULL;
  SStreamMeta*    pMeta = NULL;
  STaskCheckInfo* pInfo = NULL;
  int32_t         vgId = -1;
  int64_t         timeoutDuration = 0;
  const char*     id = NULL;
  int32_t         total = 0;

  SStreamTask* pTask = taosAcquireRef(streamTaskRefPool, taskRefId);
  if (pTask == NULL) {
    stError("invalid task rid:%" PRId64 " failed to acquired stream-task", taskRefId);
    streamTaskFreeRefId(param);
    return;
  }

  pMeta = pTask->pMeta;
  pInfo = &pTask->taskCheckInfo;
  vgId = pTask->pMeta->vgId;
  timeoutDuration = now - pInfo->timeoutStartTs;
  id = pTask->id.idStr;
  total = (int32_t) taosArrayGetSize(pInfo->pList);

  stDebug("s-task:%s start to do check-downstream-rsp check in tmr", id);

  streamMutexLock(&pTask->lock);
  SStreamTaskState state = streamTaskGetStatus(pTask);
  streamMutexUnlock(&pTask->lock);

  if (state.state == TASK_STATUS__STOP) {
    stDebug("s-task:%s status:%s vgId:%d quit from monitor check-rsp tmr", id, state.name, vgId);
    streamTaskCompleteCheckRsp(pInfo, true, id);

    // not record the failure of the current task if try to close current vnode
    // otherwise, the put of message operation may incur invalid read of message queue.
    if (!pMeta->closeFlag) {
      int32_t code = addDownstreamFailedStatusResultAsync(pTask->pMsgCb, vgId, pTask->id.streamId, pTask->id.taskId);
      if (code) {
        stError("s-task:%s failed to create async record start failed task, code:%s", id, tstrerror(code));
      }
    }

    doCleanup(pTask, pNotReadyList, pTimeoutList, param);
    return;
  }

  if (state.state == TASK_STATUS__DROPPING || state.state == TASK_STATUS__READY) {
    stDebug("s-task:%s status:%s vgId:%d quit from monitor check-rsp tmr", id, state.name, vgId);

    streamTaskCompleteCheckRsp(pInfo, true, id);
    doCleanup(pTask, pNotReadyList, pTimeoutList, param);
    return;
  }

  streamMutexLock(&pInfo->checkInfoLock);
  if (pInfo->notReadyTasks == 0) {
    stDebug("s-task:%s status:%s vgId:%d all downstream ready, quit from monitor rsp tmr", id, state.name, vgId);

    streamTaskCompleteCheckRsp(pInfo, false, id);
    streamMutexUnlock(&pInfo->checkInfoLock);
    doCleanup(pTask, pNotReadyList, pTimeoutList, param);
    return;
  }

  pNotReadyList = taosArrayInit(4, sizeof(int64_t));
  pTimeoutList = taosArrayInit(4, sizeof(int64_t));

  if (state.state == TASK_STATUS__UNINIT) {
    getCheckRspStatus(pInfo, timeoutDuration, &numOfReady, &numOfFault, &numOfNotRsp, pTimeoutList, pNotReadyList, id);

    numOfNotReady = (int32_t)taosArrayGetSize(pNotReadyList);
    numOfTimeout = (int32_t)taosArrayGetSize(pTimeoutList);

    // fault tasks detected, not try anymore
    bool jumpOut = false;
    if ((numOfReady + numOfFault + numOfNotReady + numOfTimeout + numOfNotRsp) != total) {
      stError(
          "s-task:%s vgId:%d internal error in handling the check downstream procedure, rsp number is inconsistent, "
          "stop rspMonitor tmr, total:%d, notRsp:%d, notReady:%d, fault:%d, timeout:%d, ready:%d",
          id, vgId, total, numOfNotRsp, numOfNotReady, numOfFault, numOfTimeout, numOfReady);
      jumpOut = true;
    }

    if (numOfFault > 0) {
      stDebug(
          "s-task:%s status:%s vgId:%d all rsp. quit from monitor rsp tmr, since vnode-transfer/leader-change/restart "
          "detected, total:%d, notRsp:%d, notReady:%d, fault:%d, timeout:%d, ready:%d",
          id, state.name, vgId, total, numOfNotRsp, numOfNotReady, numOfFault, numOfTimeout, numOfReady);
      jumpOut = true;
    }

    if (jumpOut) {
      streamTaskCompleteCheckRsp(pInfo, false, id);
      streamMutexUnlock(&pInfo->checkInfoLock);
      doCleanup(pTask, pNotReadyList, pTimeoutList, param);
      return;
    }
  } else {  // unexpected status
    stError("s-task:%s unexpected task status:%s during waiting for check rsp", id, state.name);
  }

  // checking of downstream tasks has been stopped by other threads
  if (pInfo->stopCheckProcess == 1) {
    stDebug(
        "s-task:%s status:%s vgId:%d stopped by other threads to check downstream process, total:%d, notRsp:%d, "
        "notReady:%d, fault:%d, timeout:%d, ready:%d",
        id, state.name, vgId, total, numOfNotRsp, numOfNotReady, numOfFault, numOfTimeout, numOfReady);

    streamTaskCompleteCheckRsp(pInfo, false, id);
    streamMutexUnlock(&pInfo->checkInfoLock);

    int32_t code = addDownstreamFailedStatusResultAsync(pTask->pMsgCb, vgId, pTask->id.streamId, pTask->id.taskId);
    if (code) {
      stError("s-task:%s failed to create async record start failed task, code:%s", id, tstrerror(code));
    }

    doCleanup(pTask, pNotReadyList, pTimeoutList, param);
    return;
  }

  if (numOfNotReady > 0) {  // check to make sure not in recheck timer
    handleNotReadyDownstreamTask(pTask, pNotReadyList);
  }

  if (numOfTimeout > 0) {
    handleTimeoutDownstreamTasks(pTask, pTimeoutList);
  }

  streamTmrStart(rspMonitorFn, CHECK_RSP_CHECK_INTERVAL, param, streamTimer, &pInfo->checkRspTmr, vgId,
                 "check-status-monitor");
  streamMutexUnlock(&pInfo->checkInfoLock);

  stDebug(
      "s-task:%s vgId:%d continue checking rsp in 300ms, total:%d, notRsp:%d, notReady:%d, fault:%d, timeout:%d, "
      "ready:%d",
      id, vgId, total, numOfNotRsp, numOfNotReady, numOfFault, numOfTimeout, numOfReady);
  doCleanup(pTask, pNotReadyList, pTimeoutList, NULL);
}
