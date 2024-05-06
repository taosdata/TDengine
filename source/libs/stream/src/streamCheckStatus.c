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
static void    addIntoNodeUpdateList(SStreamTask* pTask, int32_t nodeId);
static void    rspMonitorFn(void* param, void* tmrId);
static int32_t streamTaskInitTaskCheckInfo(STaskCheckInfo* pInfo, STaskOutputInfo* pOutputInfo, int64_t startTs);
static int32_t streamTaskStartCheckDownstream(STaskCheckInfo* pInfo, const char* id);
static int32_t streamTaskCompleteCheckRsp(STaskCheckInfo* pInfo, bool lock, const char* id);
static int32_t streamTaskAddReqInfo(STaskCheckInfo* pInfo, int64_t reqId, int32_t taskId, int32_t vgId, const char* id);
static void    doSendCheckMsg(SStreamTask* pTask, SDownstreamStatusInfo* p);
static void    handleTimeoutDownstreamTasks(SStreamTask* pTask, SArray* pTimeoutList);
static void    handleNotReadyDownstreamTask(SStreamTask* pTask, SArray* pNotReadyList);
static int32_t streamTaskUpdateCheckInfo(STaskCheckInfo* pInfo, int32_t taskId, int32_t status, int64_t rspTs,
                                         int64_t reqId, int32_t* pNotReady, const char* id);
static void setCheckDownstreamReqInfo(SStreamTaskCheckReq* pReq, int64_t reqId, int32_t dstTaskId, int32_t dstNodeId);
static void getCheckRspStatus(STaskCheckInfo* pInfo, int64_t el, int32_t* numOfReady, int32_t* numOfFault,
                              int32_t* numOfNotRsp, SArray* pTimeoutList, SArray* pNotReadyList, const char* id);
static int32_t addDownstreamFailedStatusResultAsync(SMsgCb* pMsgCb, int32_t vgId, int64_t streamId, int32_t taskId);
static SDownstreamStatusInfo* findCheckRspStatus(STaskCheckInfo* pInfo, int32_t taskId);

// check status
void streamTaskCheckDownstream(SStreamTask* pTask) {
  SDataRange*  pRange = &pTask->dataRange;
  STimeWindow* pWindow = &pRange->window;
  const char*  idstr = pTask->id.idStr;

  SStreamTaskCheckReq req = {
      .streamId = pTask->id.streamId,
      .upstreamTaskId = pTask->id.taskId,
      .upstreamNodeId = pTask->info.nodeId,
      .childId = pTask->info.selfChildId,
      .stage = pTask->pMeta->stage,
  };

  ASSERT(pTask->status.downstreamReady == 0);

  // serialize streamProcessScanHistoryFinishRsp
  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    streamTaskStartMonitorCheckRsp(pTask);

    STaskDispatcherFixed* pDispatch = &pTask->outputInfo.fixedDispatcher;

    setCheckDownstreamReqInfo(&req, tGenIdPI64(), pDispatch->taskId, pDispatch->nodeId);
    streamTaskAddReqInfo(&pTask->taskCheckInfo, req.reqId, pDispatch->taskId, pDispatch->nodeId, idstr);

    stDebug("s-task:%s (vgId:%d) stage:%" PRId64 " check single downstream task:0x%x(vgId:%d) ver:%" PRId64 "-%" PRId64
            " window:%" PRId64 "-%" PRId64 " reqId:0x%" PRIx64,
            idstr, pTask->info.nodeId, req.stage, req.downstreamTaskId, req.downstreamNodeId, pRange->range.minVer,
            pRange->range.maxVer, pWindow->skey, pWindow->ekey, req.reqId);

    streamSendCheckMsg(pTask, &req, pTask->outputInfo.fixedDispatcher.nodeId, &pTask->outputInfo.fixedDispatcher.epSet);

  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    streamTaskStartMonitorCheckRsp(pTask);

    SArray* vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgs = taosArrayGetSize(vgInfo);
    stDebug("s-task:%s check %d downstream tasks, ver:%" PRId64 "-%" PRId64 " window:%" PRId64 "-%" PRId64, idstr,
            numOfVgs, pRange->range.minVer, pRange->range.maxVer, pWindow->skey, pWindow->ekey);

    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);

      setCheckDownstreamReqInfo(&req, tGenIdPI64(), pVgInfo->taskId, pVgInfo->vgId);
      streamTaskAddReqInfo(&pTask->taskCheckInfo, req.reqId, pVgInfo->taskId, pVgInfo->vgId, idstr);

      stDebug("s-task:%s (vgId:%d) stage:%" PRId64
              " check downstream task:0x%x (vgId:%d) (shuffle), idx:%d, reqId:0x%" PRIx64,
              idstr, pTask->info.nodeId, req.stage, req.downstreamTaskId, req.downstreamNodeId, i, req.reqId);
      streamSendCheckMsg(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
    }
  } else {  // for sink task, set it ready directly.
    stDebug("s-task:%s (vgId:%d) set downstream ready, since no downstream", idstr, pTask->info.nodeId);
    streamTaskStopMonitorCheckRsp(&pTask->taskCheckInfo, idstr);
    processDownstreamReadyRsp(pTask);
  }
}

int32_t streamProcessCheckRsp(SStreamTask* pTask, const SStreamTaskCheckRsp* pRsp) {
  ASSERT(pTask->id.taskId == pRsp->upstreamTaskId);

  int64_t         now = taosGetTimestampMs();
  const char*     id = pTask->id.idStr;
  STaskCheckInfo* pInfo = &pTask->taskCheckInfo;
  int32_t         total = streamTaskGetNumOfDownstream(pTask);
  int32_t         left = -1;

  if (streamTaskShouldStop(pTask)) {
    stDebug("s-task:%s should stop, do not do check downstream again", id);
    return TSDB_CODE_SUCCESS;
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
        addIntoNodeUpdateList(pTask, pRsp->upstreamNodeId);
      } else {
        stError(
            "s-task:%s downstream taskId:0x%x (vgId:%d) not leader, self dispatch epset needs to be updated, not check "
            "downstream again, nodeUpdate needed",
            id, pRsp->downstreamTaskId, pRsp->downstreamNodeId);
        addIntoNodeUpdateList(pTask, pRsp->downstreamNodeId);
      }

      int32_t startTs = pTask->execInfo.checkTs;
      streamMetaAddTaskLaunchResult(pTask->pMeta, pTask->id.streamId, pTask->id.taskId, startTs, now, false);

      // automatically set the related fill-history task to be failed.
      if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
        STaskId* pId = &pTask->hTaskInfo.id;
        streamMetaAddTaskLaunchResult(pTask->pMeta, pId->streamId, pId->taskId, startTs, now, false);
      }
    } else {  // TASK_DOWNSTREAM_NOT_READY, let's retry in 100ms
      ASSERT(left > 0);
      stDebug("s-task:%s (vgId:%d) recv check rsp from task:0x%x (vgId:%d) status:%d, total:%d not ready:%d", id,
              pRsp->upstreamNodeId, pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->status, total, left);
    }
  }

  return 0;
}

int32_t streamTaskStartMonitorCheckRsp(SStreamTask* pTask) {
  STaskCheckInfo* pInfo = &pTask->taskCheckInfo;
  taosThreadMutexLock(&pInfo->checkInfoLock);

  int32_t code = streamTaskStartCheckDownstream(pInfo, pTask->id.idStr);
  if (code != TSDB_CODE_SUCCESS) {
    taosThreadMutexUnlock(&pInfo->checkInfoLock);
    return TSDB_CODE_FAILED;
  }

  /*SStreamTask* p = */ streamMetaAcquireOneTask(pTask);  // add task ref here
  streamTaskInitTaskCheckInfo(pInfo, &pTask->outputInfo, taosGetTimestampMs());

  int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
  stDebug("s-task:%s start check-rsp monit, ref:%d ", pTask->id.idStr, ref);

  if (pInfo->checkRspTmr == NULL) {
    pInfo->checkRspTmr = taosTmrStart(rspMonitorFn, CHECK_RSP_CHECK_INTERVAL, pTask, streamTimer);
  } else {
    taosTmrReset(rspMonitorFn, CHECK_RSP_CHECK_INTERVAL, pTask, streamTimer, &pInfo->checkRspTmr);
  }

  taosThreadMutexUnlock(&pInfo->checkInfoLock);
  return 0;
}

int32_t streamTaskStopMonitorCheckRsp(STaskCheckInfo* pInfo, const char* id) {
  taosThreadMutexLock(&pInfo->checkInfoLock);
  streamTaskCompleteCheckRsp(pInfo, false, id);

  pInfo->stopCheckProcess = 1;
  taosThreadMutexUnlock(&pInfo->checkInfoLock);

  stDebug("s-task:%s set stop check-rsp monit", id);
  return TSDB_CODE_SUCCESS;
}

void streamTaskCleanupCheckInfo(STaskCheckInfo* pInfo) {
  ASSERT(pInfo->inCheckProcess == 0);

  pInfo->pList = taosArrayDestroy(pInfo->pList);
  if (pInfo->checkRspTmr != NULL) {
    /*bool ret = */ taosTmrStop(pInfo->checkRspTmr);
    pInfo->checkRspTmr = NULL;
  }

  taosThreadMutexDestroy(&pInfo->checkInfoLock);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void processDownstreamReadyRsp(SStreamTask* pTask) {
  EStreamTaskEvent event = (pTask->info.fillHistory == 0) ? TASK_EVENT_INIT : TASK_EVENT_INIT_SCANHIST;
  streamTaskOnHandleEventSuccess(pTask->status.pSM, event, NULL, NULL);

  int64_t checkTs = pTask->execInfo.checkTs;
  int64_t readyTs = pTask->execInfo.readyTs;
  streamMetaAddTaskLaunchResult(pTask->pMeta, pTask->id.streamId, pTask->id.taskId, checkTs, readyTs, true);

  if (pTask->status.taskStatus == TASK_STATUS__HALT) {
    ASSERT(HAS_RELATED_FILLHISTORY_TASK(pTask) && (pTask->info.fillHistory == 0));

    // halt it self for count window stream task until the related fill history task completed.
    stDebug("s-task:%s level:%d initial status is %s from mnode, set it to be halt", pTask->id.idStr,
            pTask->info.taskLevel, streamTaskGetStatusStr(pTask->status.taskStatus));
    streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_HALT);
  }

  // start the related fill-history task, when current task is ready
  // not invoke in success callback due to the deadlock.
  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    stDebug("s-task:%s try to launch related fill-history task", pTask->id.idStr);
    streamLaunchFillHistoryTask(pTask);
  }
}

void addIntoNodeUpdateList(SStreamTask* pTask, int32_t nodeId) {
  int32_t vgId = pTask->pMeta->vgId;

  taosThreadMutexLock(&pTask->lock);
  int32_t num = taosArrayGetSize(pTask->outputInfo.pNodeEpsetUpdateList);
  bool    existed = false;
  for (int i = 0; i < num; ++i) {
    SDownstreamTaskEpset* p = taosArrayGet(pTask->outputInfo.pNodeEpsetUpdateList, i);
    if (p->nodeId == nodeId) {
      existed = true;
      break;
    }
  }

  if (!existed) {
    SDownstreamTaskEpset t = {.nodeId = nodeId};
    taosArrayPush(pTask->outputInfo.pNodeEpsetUpdateList, &t);

    stInfo("s-task:%s vgId:%d downstream nodeId:%d needs to be updated, total needs updated:%d", pTask->id.idStr, vgId,
           t.nodeId, (num + 1));
  }

  taosThreadMutexUnlock(&pTask->lock);
}

int32_t streamTaskInitTaskCheckInfo(STaskCheckInfo* pInfo, STaskOutputInfo* pOutputInfo, int64_t startTs) {
  taosArrayClear(pInfo->pList);

  if (pOutputInfo->type == TASK_OUTPUT__FIXED_DISPATCH) {
    pInfo->notReadyTasks = 1;
  } else if (pOutputInfo->type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    pInfo->notReadyTasks = taosArrayGetSize(pOutputInfo->shuffleDispatcher.dbInfo.pVgroupInfos);
    ASSERT(pInfo->notReadyTasks == pOutputInfo->shuffleDispatcher.dbInfo.vgNum);
  }

  pInfo->startTs = startTs;
  pInfo->timeoutStartTs = startTs;
  pInfo->stopCheckProcess = 0;
  return TSDB_CODE_SUCCESS;
}

SDownstreamStatusInfo* findCheckRspStatus(STaskCheckInfo* pInfo, int32_t taskId) {
  for (int32_t j = 0; j < taosArrayGetSize(pInfo->pList); ++j) {
    SDownstreamStatusInfo* p = taosArrayGet(pInfo->pList, j);
    if (p->taskId == taskId) {
      return p;
    }
  }

  return NULL;
}

int32_t streamTaskUpdateCheckInfo(STaskCheckInfo* pInfo, int32_t taskId, int32_t status, int64_t rspTs, int64_t reqId,
                                  int32_t* pNotReady, const char* id) {
  taosThreadMutexLock(&pInfo->checkInfoLock);

  SDownstreamStatusInfo* p = findCheckRspStatus(pInfo, taskId);
  if (p != NULL) {
    if (reqId != p->reqId) {
      stError("s-task:%s reqId:0x%" PRIx64 " expected:0x%" PRIx64 " expired check-rsp recv from downstream task:0x%x, discarded",
              id, reqId, p->reqId, taskId);
      taosThreadMutexUnlock(&pInfo->checkInfoLock);
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

    taosThreadMutexUnlock(&pInfo->checkInfoLock);
    return TSDB_CODE_SUCCESS;
  }

  taosThreadMutexUnlock(&pInfo->checkInfoLock);
  stError("s-task:%s unexpected check rsp msg, invalid downstream task:0x%x, reqId:%" PRIx64 " discarded", id, taskId,
          reqId);
  return TSDB_CODE_FAILED;
}

int32_t streamTaskStartCheckDownstream(STaskCheckInfo* pInfo, const char* id) {
  if (pInfo->inCheckProcess == 0) {
    pInfo->inCheckProcess = 1;
  } else {
    ASSERT(pInfo->startTs > 0);
    stError("s-task:%s already in check procedure, checkTs:%" PRId64 ", start monitor check rsp failed", id,
            pInfo->startTs);
    return TSDB_CODE_FAILED;
  }

  stDebug("s-task:%s set the in check-rsp flag", id);
  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskCompleteCheckRsp(STaskCheckInfo* pInfo, bool lock, const char* id) {
  if (lock) {
    taosThreadMutexLock(&pInfo->checkInfoLock);
  }

  if (!pInfo->inCheckProcess) {
//    stWarn("s-task:%s already not in-check-procedure", id);
  }

  int64_t el = (pInfo->startTs != 0) ? (taosGetTimestampMs() - pInfo->startTs) : 0;
  stDebug("s-task:%s clear the in check-rsp flag, not in check-rsp anymore, elapsed time:%" PRId64 " ms", id, el);

  pInfo->startTs = 0;
  pInfo->timeoutStartTs = 0;
  pInfo->notReadyTasks = 0;
  pInfo->inCheckProcess = 0;
  pInfo->stopCheckProcess = 0;

  pInfo->notReadyRetryCount = 0;
  pInfo->timeoutRetryCount = 0;

  taosArrayClear(pInfo->pList);

  if (lock) {
    taosThreadMutexUnlock(&pInfo->checkInfoLock);
  }

  return 0;
}

int32_t streamTaskAddReqInfo(STaskCheckInfo* pInfo, int64_t reqId, int32_t taskId, int32_t vgId, const char* id) {
  SDownstreamStatusInfo info = {.taskId = taskId, .status = -1, .vgId = vgId, .reqId = reqId, .rspTs = 0};
  taosThreadMutexLock(&pInfo->checkInfoLock);

  SDownstreamStatusInfo* p = findCheckRspStatus(pInfo, taskId);
  if (p != NULL) {
    stDebug("s-task:%s check info to task:0x%x already sent", id, taskId);
    taosThreadMutexUnlock(&pInfo->checkInfoLock);
    return TSDB_CODE_SUCCESS;
  }

  taosArrayPush(pInfo->pList, &info);

  taosThreadMutexUnlock(&pInfo->checkInfoLock);
  return TSDB_CODE_SUCCESS;
}

void doSendCheckMsg(SStreamTask* pTask, SDownstreamStatusInfo* p) {
  const char* id = pTask->id.idStr;

  SStreamTaskCheckReq req = {
      .streamId = pTask->id.streamId,
      .upstreamTaskId = pTask->id.taskId,
      .upstreamNodeId = pTask->info.nodeId,
      .childId = pTask->info.selfChildId,
      .stage = pTask->pMeta->stage,
  };

  STaskOutputInfo* pOutputInfo = &pTask->outputInfo;
  if (pOutputInfo->type == TASK_OUTPUT__FIXED_DISPATCH) {
    STaskDispatcherFixed* pDispatch = &pOutputInfo->fixedDispatcher;
    setCheckDownstreamReqInfo(&req, p->reqId, pDispatch->taskId, pDispatch->taskId);

    stDebug("s-task:%s (vgId:%d) stage:%" PRId64 " re-send check downstream task:0x%x(vgId:%d) reqId:0x%" PRIx64,
            id, pTask->info.nodeId, req.stage, req.downstreamTaskId, req.downstreamNodeId, req.reqId);

    streamSendCheckMsg(pTask, &req, pOutputInfo->fixedDispatcher.nodeId, &pOutputInfo->fixedDispatcher.epSet);
  } else if (pOutputInfo->type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pOutputInfo->shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t numOfVgs = taosArrayGetSize(vgInfo);

    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);

      if (p->taskId == pVgInfo->taskId) {
        setCheckDownstreamReqInfo(&req, p->reqId, pVgInfo->taskId, pVgInfo->vgId);

        stDebug("s-task:%s (vgId:%d) stage:%" PRId64
                " re-send check downstream task:0x%x(vgId:%d) (shuffle), idx:%d reqId:0x%" PRIx64,
                id, pTask->info.nodeId, req.stage, req.downstreamTaskId, req.downstreamNodeId, i, p->reqId);
        streamSendCheckMsg(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
        break;
      }
    }
  } else {
    ASSERT(0);
  }
}

void getCheckRspStatus(STaskCheckInfo* pInfo, int64_t el, int32_t* numOfReady, int32_t* numOfFault,
                       int32_t* numOfNotRsp, SArray* pTimeoutList, SArray* pNotReadyList, const char* id) {
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pList); ++i) {
    SDownstreamStatusInfo* p = taosArrayGet(pInfo->pList, i);
    if (p->status == TASK_DOWNSTREAM_READY) {
      (*numOfReady) += 1;
    } else if (p->status == TASK_UPSTREAM_NEW_STAGE || p->status == TASK_DOWNSTREAM_NOT_LEADER) {
      stDebug("s-task:%s recv status:NEW_STAGE/NOT_LEADER from downstream, task:0x%x, quit from check downstream", id,
              p->taskId);
      (*numOfFault) += 1;
    } else {                // TASK_DOWNSTREAM_NOT_READY
      if (p->rspTs == 0) {  // not response yet
        ASSERT(p->status == -1);
        if (el >= CHECK_NOT_RSP_DURATION) {  // not receive info for 10 sec.
          taosArrayPush(pTimeoutList, &p->taskId);
        } else {                // el < CHECK_NOT_RSP_DURATION
          (*numOfNotRsp) += 1;  // do nothing and continue waiting for their rsp
        }
      } else {
        taosArrayPush(pNotReadyList, &p->taskId);
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

  ASSERT(pTask->status.downstreamReady == 0);
  pInfo->timeoutStartTs = taosGetTimestampMs();

  for (int32_t i = 0; i < numOfTimeout; ++i) {
    int32_t taskId = *(int32_t*)taosArrayGet(pTimeoutList, i);

    SDownstreamStatusInfo* p = findCheckRspStatus(pInfo, taskId);
    if (p != NULL) {
      ASSERT(p->status == -1 && p->rspTs == 0);
      doSendCheckMsg(pTask, p);
    }
  }

  pInfo->timeoutRetryCount += 1;

  // timeout more than 100 sec, add into node update list
  if (pInfo->timeoutRetryCount > 10) {
    pInfo->timeoutRetryCount = 0;

    for (int32_t i = 0; i < numOfTimeout; ++i) {
      int32_t                taskId = *(int32_t*)taosArrayGet(pTimeoutList, i);
      SDownstreamStatusInfo* p = findCheckRspStatus(pInfo, taskId);
      if (p != NULL) {
        addIntoNodeUpdateList(pTask, p->vgId);
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

  ASSERT(pTask->status.downstreamReady == 0);

  // reset the info, and send the check msg to failure downstream again
  for (int32_t i = 0; i < numOfNotReady; ++i) {
    int32_t taskId = *(int32_t*)taosArrayGet(pNotReadyList, i);

    SDownstreamStatusInfo* p = findCheckRspStatus(pInfo, taskId);
    if (p != NULL) {
      p->rspTs = 0;
      p->status = -1;
      doSendCheckMsg(pTask, p);
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
  SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
  if (pRunReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    stError("vgId:%d failed to create msg to stop tasks async, code:%s", vgId, terrstr());
    return -1;
  }

  stDebug("vgId:%d create msg add failed s-task:0x%x", vgId, taskId);

  pRunReq->head.vgId = vgId;
  pRunReq->streamId = streamId;
  pRunReq->taskId = taskId;
  pRunReq->reqType = STREAM_EXEC_T_ADD_FAILED_TASK;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  tmsgPutToQueue(pMsgCb, STREAM_QUEUE, &msg);
  return 0;
}

// this function is executed in timer thread
void rspMonitorFn(void* param, void* tmrId) {
  SStreamTask*      pTask = param;
  SStreamMeta*      pMeta = pTask->pMeta;
  SStreamTaskState* pStat = streamTaskGetStatus(pTask);
  STaskCheckInfo*   pInfo = &pTask->taskCheckInfo;
  int32_t           vgId = pTask->pMeta->vgId;
  int64_t           now = taosGetTimestampMs();
  int64_t           timeoutDuration = now - pInfo->timeoutStartTs;
  ETaskStatus       state = pStat->state;
  const char*       id = pTask->id.idStr;
  int32_t           numOfReady = 0;
  int32_t           numOfFault = 0;
  int32_t           numOfNotRsp = 0;
  int32_t           numOfNotReady = 0;
  int32_t           numOfTimeout = 0;
  int32_t           total = taosArrayGetSize(pInfo->pList);

  stDebug("s-task:%s start to do check-downstream-rsp check in tmr", id);

  if (state == TASK_STATUS__STOP) {
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s status:%s vgId:%d quit from monitor check-rsp tmr, ref:%d", id, pStat->name, vgId, ref);

    streamTaskCompleteCheckRsp(pInfo, true, id);
    addDownstreamFailedStatusResultAsync(pTask->pMsgCb, vgId, pTask->id.streamId, pTask->id.taskId);

    streamMetaReleaseTask(pMeta, pTask);
    return;
  }

  if (state == TASK_STATUS__DROPPING || state == TASK_STATUS__READY || state == TASK_STATUS__PAUSE) {
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s status:%s vgId:%d quit from monitor check-rsp tmr, ref:%d", id, pStat->name, vgId, ref);

    streamTaskCompleteCheckRsp(pInfo, true, id);
    streamMetaReleaseTask(pMeta, pTask);
    return;
  }

  taosThreadMutexLock(&pInfo->checkInfoLock);
  if (pInfo->notReadyTasks == 0) {
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s status:%s vgId:%d all downstream ready, quit from monitor rsp tmr, ref:%d", id, pStat->name,
            vgId, ref);

    streamTaskCompleteCheckRsp(pInfo, false, id);
    taosThreadMutexUnlock(&pInfo->checkInfoLock);
    streamMetaReleaseTask(pMeta, pTask);
    return;
  }

  SArray* pNotReadyList = taosArrayInit(4, sizeof(int64_t));
  SArray* pTimeoutList = taosArrayInit(4, sizeof(int64_t));

  if (pStat->state == TASK_STATUS__UNINIT) {
    getCheckRspStatus(pInfo, timeoutDuration, &numOfReady, &numOfFault, &numOfNotRsp, pTimeoutList, pNotReadyList, id);
  } else {  // unexpected status
    stError("s-task:%s unexpected task status:%s during waiting for check rsp", id, pStat->name);
  }

  numOfNotReady = (int32_t)taosArrayGetSize(pNotReadyList);
  numOfTimeout = (int32_t)taosArrayGetSize(pTimeoutList);

  // fault tasks detected, not try anymore
  ASSERT((numOfReady + numOfFault + numOfNotReady + numOfTimeout + numOfNotRsp) == total);
  if (numOfFault > 0) {
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug(
        "s-task:%s status:%s vgId:%d all rsp. quit from monitor rsp tmr, since vnode-transfer/leader-change/restart "
        "detected, total:%d, notRsp:%d, notReady:%d, fault:%d, timeout:%d, ready:%d ref:%d",
        id, pStat->name, vgId, total, numOfNotRsp, numOfNotReady, numOfFault, numOfTimeout, numOfReady, ref);

    streamTaskCompleteCheckRsp(pInfo, false, id);
    taosThreadMutexUnlock(&pInfo->checkInfoLock);
    streamMetaReleaseTask(pMeta, pTask);

    taosArrayDestroy(pNotReadyList);
    taosArrayDestroy(pTimeoutList);
    return;
  }

  // checking of downstream tasks has been stopped by other threads
  if (pInfo->stopCheckProcess == 1) {
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug(
        "s-task:%s status:%s vgId:%d stopped by other threads to check downstream process, total:%d, notRsp:%d, "
        "notReady:%d, fault:%d, timeout:%d, ready:%d ref:%d",
        id, pStat->name, vgId, total, numOfNotRsp, numOfNotReady, numOfFault, numOfTimeout, numOfReady, ref);

    streamTaskCompleteCheckRsp(pInfo, false, id);
    taosThreadMutexUnlock(&pInfo->checkInfoLock);

    addDownstreamFailedStatusResultAsync(pTask->pMsgCb, vgId, pTask->id.streamId, pTask->id.taskId);
    streamMetaReleaseTask(pMeta, pTask);

    taosArrayDestroy(pNotReadyList);
    taosArrayDestroy(pTimeoutList);
    return;
  }

  if (numOfNotReady > 0) {  // check to make sure not in recheck timer
    handleNotReadyDownstreamTask(pTask, pNotReadyList);
  }

  if (numOfTimeout > 0) {
    handleTimeoutDownstreamTasks(pTask, pTimeoutList);
  }

  taosTmrReset(rspMonitorFn, CHECK_RSP_CHECK_INTERVAL, pTask, streamTimer, &pInfo->checkRspTmr);
  taosThreadMutexUnlock(&pInfo->checkInfoLock);

  stDebug(
      "s-task:%s vgId:%d continue checking rsp in 300ms, total:%d, notRsp:%d, notReady:%d, fault:%d, timeout:%d, "
      "ready:%d",
      id, vgId, total, numOfNotRsp, numOfNotReady, numOfFault, numOfTimeout, numOfReady);

  taosArrayDestroy(pNotReadyList);
  taosArrayDestroy(pTimeoutList);
}

int32_t tEncodeStreamTaskCheckReq(SEncoder* pEncoder, const SStreamTaskCheckReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->reqId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->childId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->stage) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTaskCheckReq(SDecoder* pDecoder, SStreamTaskCheckReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->reqId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->downstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->downstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->childId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->stage) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamTaskCheckRsp(SEncoder* pEncoder, const SStreamTaskCheckRsp* pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->reqId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->downstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->downstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->childId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->oldStage) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->status) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTaskCheckRsp(SDecoder* pDecoder, SStreamTaskCheckRsp* pRsp) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->reqId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->childId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->oldStage) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->status) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}
