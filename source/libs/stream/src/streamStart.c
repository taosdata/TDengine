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

#include "streamInt.h"
#include "streamsm.h"
#include "trpc.h"
#include "ttimer.h"
#include "wal.h"

#define SCANHISTORY_IDLE_TIME_SLICE 100  // 100ms
#define SCANHISTORY_MAX_IDLE_TIME   10   // 10 sec
#define SCANHISTORY_IDLE_TICK       ((SCANHISTORY_MAX_IDLE_TIME * 1000) / SCANHISTORY_IDLE_TIME_SLICE)

typedef struct SLaunchHTaskInfo {
  SStreamMeta* pMeta;
  STaskId      id;
  STaskId      hTaskId;
} SLaunchHTaskInfo;

typedef struct STaskRecheckInfo {
  SStreamTask*        pTask;
  SStreamTaskCheckReq req;
  void*               checkTimer;
} STaskRecheckInfo;

static int32_t           streamSetParamForScanHistory(SStreamTask* pTask);
static void              streamTaskSetRangeStreamCalc(SStreamTask* pTask);
static int32_t           initScanHistoryReq(SStreamTask* pTask, SStreamScanHistoryReq* pReq, int8_t igUntreated);
static SLaunchHTaskInfo* createHTaskLaunchInfo(SStreamMeta* pMeta, int64_t streamId, int32_t taskId, int64_t hStreamId,
                                               int32_t hTaskId);
static void              tryLaunchHistoryTask(void* param, void* tmrId);
static void              doProcessDownstreamReadyRsp(SStreamTask* pTask);

int32_t streamTaskSetReady(SStreamTask* pTask) {
  int32_t     numOfDowns = streamTaskGetNumOfDownstream(pTask);
  SStreamTaskState* p = streamTaskGetStatus(pTask);

  if ((p->state == TASK_STATUS__SCAN_HISTORY) && pTask->info.taskLevel != TASK_LEVEL__SOURCE) {
    pTask->numOfWaitingUpstream = taosArrayGetSize(pTask->upstreamInfo.pList);
    stDebug("s-task:%s level:%d task wait for %d upstream tasks complete scan-history procedure, status:%s",
            pTask->id.idStr, pTask->info.taskLevel, pTask->numOfWaitingUpstream, p->name);
  }

  ASSERT(pTask->status.downstreamReady == 0);
  pTask->status.downstreamReady = 1;

  pTask->execInfo.start = taosGetTimestampMs();
  int64_t el = (pTask->execInfo.start - pTask->execInfo.init);
  stDebug("s-task:%s all %d downstream ready, init completed, elapsed time:%" PRId64 "ms, task status:%s",
          pTask->id.idStr, numOfDowns, el, p->name);
  return TSDB_CODE_SUCCESS;
}

int32_t streamStartScanHistoryAsync(SStreamTask* pTask, int8_t igUntreated) {
  SStreamScanHistoryReq req;
  initScanHistoryReq(pTask, &req, igUntreated);

  int32_t len = sizeof(SStreamScanHistoryReq);
  void*   serializedReq = rpcMallocCont(len);
  if (serializedReq == NULL) {
    return -1;
  }

  memcpy(serializedReq, &req, len);

  SRpcMsg rpcMsg = {.contLen = len, .pCont = serializedReq, .msgType = TDMT_VND_STREAM_SCAN_HISTORY};
  if (tmsgPutToQueue(pTask->pMsgCb, STREAM_QUEUE, &rpcMsg) < 0) {
    /*ASSERT(0);*/
  }

  return 0;
}

static void doReExecScanhistory(void* param, void* tmrId) {
  SStreamTask* pTask = param;
  pTask->schedHistoryInfo.numOfTicks -= 1;

  SStreamTaskState* p = streamTaskGetStatus(pTask);
  if (p->state == TASK_STATUS__DROPPING || p->state == TASK_STATUS__STOP) {
    streamMetaReleaseTask(pTask->pMeta, pTask);
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s status:%s not start scan-history again, ref:%d", pTask->id.idStr, p->name, ref);
    return;
  }

  if (pTask->schedHistoryInfo.numOfTicks <= 0) {
    streamStartScanHistoryAsync(pTask, 0);

    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s fill-history:%d start scan-history data, out of tmr, ref:%d", pTask->id.idStr,
            pTask->info.fillHistory, ref);

    // release the task.
    streamMetaReleaseTask(pTask->pMeta, pTask);
  } else {
    taosTmrReset(doReExecScanhistory, SCANHISTORY_IDLE_TIME_SLICE, pTask, streamTimer, &pTask->schedHistoryInfo.pTimer);
  }
}

int32_t streamReExecScanHistoryFuture(SStreamTask* pTask, int32_t idleDuration) {
  int32_t numOfTicks = idleDuration / SCANHISTORY_IDLE_TIME_SLICE;
  if (numOfTicks <= 0) {
    numOfTicks = 1;
  } else if (numOfTicks > SCANHISTORY_IDLE_TICK) {
    numOfTicks = SCANHISTORY_IDLE_TICK;
  }

  // add ref for task
  SStreamTask* p = streamMetaAcquireTask(pTask->pMeta, pTask->id.streamId, pTask->id.taskId);
  ASSERT(p != NULL);

  pTask->schedHistoryInfo.numOfTicks = numOfTicks;

  int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
  stDebug("s-task:%s scan-history resumed in %.2fs, ref:%d", pTask->id.idStr, numOfTicks * 0.1, ref);

  if (pTask->schedHistoryInfo.pTimer == NULL) {
    pTask->schedHistoryInfo.pTimer = taosTmrStart(doReExecScanhistory, SCANHISTORY_IDLE_TIME_SLICE, pTask, streamTimer);
  } else {
    taosTmrReset(doReExecScanhistory, SCANHISTORY_IDLE_TIME_SLICE, pTask, streamTimer, &pTask->schedHistoryInfo.pTimer);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doStartScanHistoryTask(SStreamTask* pTask) {
  SVersionRange* pRange = &pTask->dataRange.range;
  if (pTask->info.fillHistory) {
    streamSetParamForScanHistory(pTask);
  }

  streamSetParamForStreamScannerStep1(pTask, pRange, &pTask->dataRange.window);
  int32_t code = streamStartScanHistoryAsync(pTask, 0);
  return code;
}

int32_t streamTaskStartScanHistory(SStreamTask* pTask) {
  int32_t     level = pTask->info.taskLevel;
  ETaskStatus status = streamTaskGetStatus(pTask)->state;

  ASSERT((pTask->status.downstreamReady == 1) && (status == TASK_STATUS__SCAN_HISTORY));

  if (level == TASK_LEVEL__SOURCE) {
    return doStartScanHistoryTask(pTask);
  } else if (level == TASK_LEVEL__AGG) {
    if (pTask->info.fillHistory) {
      streamSetParamForScanHistory(pTask);
    }
  } else if (level == TASK_LEVEL__SINK) {
    stDebug("s-task:%s sink task do nothing to handle scan-history", pTask->id.idStr);
  }

  return 0;
}

// check status
void streamTaskCheckDownstream(SStreamTask* pTask) {
  SDataRange*  pRange = &pTask->dataRange;
  STimeWindow* pWindow = &pRange->window;

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
    req.reqId = tGenIdPI64();
    req.downstreamNodeId = pTask->outputInfo.fixedDispatcher.nodeId;
    req.downstreamTaskId = pTask->outputInfo.fixedDispatcher.taskId;
    pTask->checkReqId = req.reqId;

    stDebug("s-task:%s (vgId:%d) stage:%" PRId64 " check single downstream task:0x%x(vgId:%d) ver:%" PRId64 "-%" PRId64
            " window:%" PRId64 "-%" PRId64 " req:0x%" PRIx64,
            pTask->id.idStr, pTask->info.nodeId, req.stage, req.downstreamTaskId, req.downstreamNodeId,
            pRange->range.minVer, pRange->range.maxVer, pWindow->skey, pWindow->ekey, req.reqId);

    streamSendCheckMsg(pTask, &req, pTask->outputInfo.fixedDispatcher.nodeId, &pTask->outputInfo.fixedDispatcher.epSet);
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgs = taosArrayGetSize(vgInfo);
    pTask->notReadyTasks = numOfVgs;
    if (pTask->checkReqIds == NULL) {
      pTask->checkReqIds = taosArrayInit(numOfVgs, sizeof(int64_t));
    } else {
      taosArrayClear(pTask->checkReqIds);
    }

    stDebug("s-task:%s check %d downstream tasks, ver:%" PRId64 "-%" PRId64 " window:%" PRId64 "-%" PRId64,
            pTask->id.idStr, numOfVgs, pRange->range.minVer, pRange->range.maxVer, pWindow->skey, pWindow->ekey);

    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      req.reqId = tGenIdPI64();
      taosArrayPush(pTask->checkReqIds, &req.reqId);
      req.downstreamNodeId = pVgInfo->vgId;
      req.downstreamTaskId = pVgInfo->taskId;
      stDebug("s-task:%s (vgId:%d) stage:%" PRId64 " check downstream task:0x%x (vgId:%d) (shuffle), idx:%d",
              pTask->id.idStr, pTask->info.nodeId, req.stage, req.downstreamTaskId, req.downstreamNodeId, i);
      streamSendCheckMsg(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
    }
  } else {
    stDebug("s-task:%s (vgId:%d) set downstream ready, since no downstream", pTask->id.idStr, pTask->info.nodeId);
    doProcessDownstreamReadyRsp(pTask);
  }
}

static STaskRecheckInfo* createRecheckInfo(SStreamTask* pTask, const SStreamTaskCheckRsp* pRsp) {
  STaskRecheckInfo* pInfo = taosMemoryCalloc(1, sizeof(STaskRecheckInfo));
  if (pInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pInfo->pTask = pTask;
  pInfo->req = (SStreamTaskCheckReq){
      .reqId = pRsp->reqId,
      .streamId = pRsp->streamId,
      .upstreamTaskId = pRsp->upstreamTaskId,
      .upstreamNodeId = pRsp->upstreamNodeId,
      .downstreamTaskId = pRsp->downstreamTaskId,
      .downstreamNodeId = pRsp->downstreamNodeId,
      .childId = pRsp->childId,
      .stage = pTask->pMeta->stage,
  };

  return pInfo;
}

static void destroyRecheckInfo(STaskRecheckInfo* pInfo) {
  if (pInfo != NULL) {
    taosTmrStop(pInfo->checkTimer);
    pInfo->checkTimer = NULL;
    taosMemoryFree(pInfo);
  }
}

static void recheckDownstreamTasks(void* param, void* tmrId) {
  STaskRecheckInfo* pInfo = param;
  SStreamTask*      pTask = pInfo->pTask;

  SStreamTaskCheckReq* pReq = &pInfo->req;

  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    stDebug("s-task:%s (vgId:%d) check downstream task:0x%x (vgId:%d) stage:%" PRId64 " (recheck)", pTask->id.idStr,
            pTask->info.nodeId, pReq->downstreamTaskId, pReq->downstreamNodeId, pReq->stage);
    streamSendCheckMsg(pTask, pReq, pReq->downstreamNodeId, &pTask->outputInfo.fixedDispatcher.epSet);
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgs = taosArrayGetSize(vgInfo);
    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      if (pVgInfo->taskId == pReq->downstreamTaskId) {
        stDebug("s-task:%s (vgId:%d) check downstream task:0x%x (vgId:%d) stage:%" PRId64 " (recheck)", pTask->id.idStr,
                pTask->info.nodeId, pReq->downstreamTaskId, pReq->downstreamNodeId, pReq->stage);
        streamSendCheckMsg(pTask, pReq, pReq->downstreamNodeId, &pVgInfo->epSet);
      }
    }
  }

  destroyRecheckInfo(pInfo);
  int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
  stDebug("s-task:%s complete send check in timer, ref:%d", pTask->id.idStr, ref);
}

int32_t streamTaskCheckStatus(SStreamTask* pTask, int32_t upstreamTaskId, int32_t vgId, int64_t stage,
                              int64_t* oldStage) {
  SStreamChildEpInfo* pInfo = streamTaskGetUpstreamTaskEpInfo(pTask, upstreamTaskId);
  ASSERT(pInfo != NULL);

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
    taosThreadMutexLock(&pTask->lock);
    ETaskStatus status = streamTaskGetStatus(pTask)->state;
    if (status == TASK_STATUS__CK) {
      streamTaskSetCheckpointFailedId(pTask);
    }
    taosThreadMutexUnlock(&pTask->lock);
  }

  if (pInfo->stage != stage) {

    taosThreadMutexLock(&pTask->lock);
    ETaskStatus status = streamTaskGetStatus(pTask)->state;
    if (status == TASK_STATUS__CK) {
      streamTaskSetCheckpointFailedId(pTask);
    }
    taosThreadMutexUnlock(&pTask->lock);

    return TASK_UPSTREAM_NEW_STAGE;
  } else if (pTask->status.downstreamReady != 1) {
    stDebug("s-task:%s vgId:%d leader:%d, downstream not ready", id, vgId, (pTask->pMeta->role == NODE_ROLE_LEADER));
    return TASK_DOWNSTREAM_NOT_READY;
  } else {
    return TASK_DOWNSTREAM_READY;
  }
}

int32_t streamTaskOnNormalTaskReady(SStreamTask* pTask) {
  const char* id = pTask->id.idStr;

  streamTaskSetReady(pTask);
  streamTaskSetRangeStreamCalc(pTask);

  SStreamTaskState* p = streamTaskGetStatus(pTask);
  ASSERT(p->state == TASK_STATUS__READY);

  int8_t schedStatus = pTask->status.schedStatus;
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    int64_t startVer = walReaderGetCurrentVer(pTask->exec.pWalReader);
    if (startVer == -1) {
      startVer = pTask->chkInfo.nextProcessVer;
    }

    stDebug("s-task:%s status:%s, sched-status:%d, ready for data from wal ver:%" PRId64, id, p->name, schedStatus,
            startVer);
  } else {
    stDebug("s-task:%s level:%d status:%s sched-status:%d", id, pTask->info.taskLevel, p->name, schedStatus);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskOnScanhistoryTaskReady(SStreamTask* pTask) {
  const char* id = pTask->id.idStr;

  // set the state to be ready
  streamTaskSetReady(pTask);
  streamTaskSetRangeStreamCalc(pTask);

  SStreamTaskState* p = streamTaskGetStatus(pTask);
  ASSERT(p->state == TASK_STATUS__SCAN_HISTORY);

  if (pTask->info.fillHistory == 1) {
    stDebug("s-task:%s fill-history task enters into scan-history data stage, status:%s", id, p->name);
    streamTaskStartScanHistory(pTask);
  } else {
    stDebug("s-task:%s scan wal data, status:%s", id, p->name);
  }

  // NOTE: there will be an deadlock if launch fill history here.
//  // start the related fill-history task, when current task is ready
//  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
//    streamLaunchFillHistoryTask(pTask);
//  }

  return TSDB_CODE_SUCCESS;
}

void doProcessDownstreamReadyRsp(SStreamTask* pTask) {
  EStreamTaskEvent event = (pTask->info.fillHistory == 0) ? TASK_EVENT_INIT : TASK_EVENT_INIT_SCANHIST;
  streamTaskOnHandleEventSuccess(pTask->status.pSM, event);

  int64_t initTs = pTask->execInfo.init;
  int64_t startTs = pTask->execInfo.start;
  streamMetaAddTaskLaunchResult(pTask->pMeta, pTask->id.streamId, pTask->id.taskId, initTs, startTs, true);

  if (pTask->status.taskStatus == TASK_STATUS__HALT) {
    ASSERT(HAS_RELATED_FILLHISTORY_TASK(pTask) && (pTask->info.fillHistory == 0));

    // halt it self for count window stream task until the related
    // fill history task completd.
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

static void addIntoNodeUpdateList(SStreamTask* pTask, int32_t nodeId) {
  int32_t vgId = pTask->pMeta->vgId;

  taosThreadMutexLock(&pTask->lock);
  int32_t num = taosArrayGetSize(pTask->outputInfo.pDownstreamUpdateList);
  bool    existed = false;
  for (int i = 0; i < num; ++i) {
    SDownstreamTaskEpset* p = taosArrayGet(pTask->outputInfo.pDownstreamUpdateList, i);
    if (p->nodeId == nodeId) {
      existed = true;
      break;
    }
  }

  if (!existed) {
    SDownstreamTaskEpset t = {.nodeId = nodeId};
    taosArrayPush(pTask->outputInfo.pDownstreamUpdateList, &t);

    stInfo("s-task:%s vgId:%d downstream nodeId:%d needs to be updated, total needs updated:%d", pTask->id.idStr, vgId,
           t.nodeId, (int32_t)taosArrayGetSize(pTask->outputInfo.pDownstreamUpdateList));
  }

  taosThreadMutexUnlock(&pTask->lock);
}

int32_t streamProcessCheckRsp(SStreamTask* pTask, const SStreamTaskCheckRsp* pRsp) {
  ASSERT(pTask->id.taskId == pRsp->upstreamTaskId);
  const char* id = pTask->id.idStr;

  if (streamTaskShouldStop(pTask)) {
    stDebug("s-task:%s should stop, do not do check downstream again", id);
    return TSDB_CODE_SUCCESS;
  }

  if (pRsp->status == TASK_DOWNSTREAM_READY) {
    if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      bool    found = false;
      int32_t numOfReqs = taosArrayGetSize(pTask->checkReqIds);
      for (int32_t i = 0; i < numOfReqs; i++) {
        int64_t reqId = *(int64_t*)taosArrayGet(pTask->checkReqIds, i);
        if (reqId == pRsp->reqId) {
          found = true;
          break;
        }
      }

      if (!found) {
        return -1;
      }

      int32_t left = atomic_sub_fetch_32(&pTask->notReadyTasks, 1);
      ASSERT(left >= 0);

      if (left == 0) {
        pTask->checkReqIds = taosArrayDestroy(pTask->checkReqIds);;

        doProcessDownstreamReadyRsp(pTask);
      } else {
        int32_t total = taosArrayGetSize(pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos);
        stDebug("s-task:%s (vgId:%d) recv check rsp from task:0x%x (vgId:%d) status:%d, total:%d not ready:%d", id,
                pRsp->upstreamNodeId, pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->status, total, left);
      }
    } else {
      ASSERT(pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH);
      if (pRsp->reqId != pTask->checkReqId) {
        return -1;
      }

      doProcessDownstreamReadyRsp(pTask);
    }
  } else {  // not ready, wait for 100ms and retry
    if (pRsp->status == TASK_UPSTREAM_NEW_STAGE || pRsp->status == TASK_DOWNSTREAM_NOT_LEADER) {
      if (pRsp->status == TASK_UPSTREAM_NEW_STAGE) {
        stError("s-task:%s vgId:%d self vnode-transfer/leader-change/restart detected, old stage:%" PRId64
                ", current stage:%" PRId64
                ", not check wait for downstream task nodeUpdate, and all tasks restart",
                id, pRsp->upstreamNodeId, pRsp->oldStage, pTask->pMeta->stage);
        addIntoNodeUpdateList(pTask, pRsp->upstreamNodeId);
      } else {
        stError(
            "s-task:%s downstream taskId:0x%x (vgId:%d) not leader, self dispatch epset needs to be updated, not check "
            "downstream again, nodeUpdate needed",
            id, pRsp->downstreamTaskId, pRsp->downstreamNodeId);
        addIntoNodeUpdateList(pTask, pRsp->downstreamNodeId);
      }

      int32_t startTs = pTask->execInfo.init;
      int64_t now = taosGetTimestampMs();
      streamMetaAddTaskLaunchResult(pTask->pMeta, pTask->id.streamId, pTask->id.taskId, startTs, now, false);

      // automatically set the related fill-history task to be failed.
      if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
        STaskId* pId = &pTask->hTaskInfo.id;
        streamMetaAddTaskLaunchResult(pTask->pMeta, pId->streamId, pId->taskId, startTs, now, false);
      }
    } else {  // TASK_DOWNSTREAM_NOT_READY, let's retry in 100ms
      STaskRecheckInfo* pInfo = createRecheckInfo(pTask, pRsp);

      int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
      stDebug("s-task:%s downstream taskId:0x%x (vgId:%d) not ready, stage:%" PRId64 ", retry in 100ms, ref:%d ", id,
              pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->oldStage, ref);
      pInfo->checkTimer = taosTmrStart(recheckDownstreamTasks, CHECK_DOWNSTREAM_INTERVAL, pInfo, streamTimer);
    }
  }

  return 0;
}

int32_t streamSendCheckRsp(const SStreamMeta* pMeta, const SStreamTaskCheckReq* pReq, SStreamTaskCheckRsp* pRsp,
                           SRpcHandleInfo* pRpcInfo, int32_t taskId) {
  SEncoder encoder;
  int32_t  code;
  int32_t  len;

  tEncodeSize(tEncodeStreamTaskCheckRsp, pRsp, len, code);
  if (code < 0) {
    stError("vgId:%d failed to encode task check rsp, s-task:0x%x", pMeta->vgId, taskId);
    return -1;
  }

  void* buf = rpcMallocCont(sizeof(SMsgHead) + len);
  ((SMsgHead*)buf)->vgId = htonl(pReq->upstreamNodeId);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncoderInit(&encoder, (uint8_t*)abuf, len);
  tEncodeStreamTaskCheckRsp(&encoder, pRsp);
  tEncoderClear(&encoder);

  SRpcMsg rspMsg = {.code = 0, .pCont = buf, .contLen = sizeof(SMsgHead) + len, .info = *pRpcInfo};

  tmsgSendRsp(&rspMsg);
  return 0;
}

// common
int32_t streamSetParamForScanHistory(SStreamTask* pTask) {
  stDebug("s-task:%s set operator option for scan-history data", pTask->id.idStr);
  return qSetStreamOperatorOptionForScanHistory(pTask->exec.pExecutor);
}

// source
int32_t streamSetParamForStreamScannerStep1(SStreamTask* pTask, SVersionRange* pVerRange, STimeWindow* pWindow) {
  return qStreamSourceScanParamForHistoryScanStep1(pTask->exec.pExecutor, pVerRange, pWindow);
}

int32_t streamSetParamForStreamScannerStep2(SStreamTask* pTask, SVersionRange* pVerRange, STimeWindow* pWindow) {
  return qStreamSourceScanParamForHistoryScanStep2(pTask->exec.pExecutor, pVerRange, pWindow);
}

int32_t initScanHistoryReq(SStreamTask* pTask, SStreamScanHistoryReq* pReq, int8_t igUntreated) {
  pReq->msgHead.vgId = pTask->info.nodeId;
  pReq->streamId = pTask->id.streamId;
  pReq->taskId = pTask->id.taskId;
  pReq->igUntreated = igUntreated;
  return 0;
}

int32_t streamTaskPutTranstateIntoInputQ(SStreamTask* pTask) {
  SStreamDataBlock* pTranstate = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, sizeof(SSDataBlock));
  if (pTranstate == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    taosFreeQitem(pTranstate);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pTranstate->type = STREAM_INPUT__TRANS_STATE;

  pBlock->info.type = STREAM_TRANS_STATE;
  pBlock->info.rows = 1;
  pBlock->info.childId = pTask->info.selfChildId;

  pTranstate->blocks = taosArrayInit(4, sizeof(SSDataBlock));  // pBlock;
  taosArrayPush(pTranstate->blocks, pBlock);

  taosMemoryFree(pBlock);
  if (streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pTranstate) < 0) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pTask->status.appendTranstateBlock = true;
  return TSDB_CODE_SUCCESS;
}

static void checkFillhistoryTaskStatus(SStreamTask* pTask, SStreamTask* pHTask) {
  SDataRange* pRange = &pHTask->dataRange;

  // the query version range should be limited to the already processed data
  pHTask->execInfo.init = taosGetTimestampMs();

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    stDebug("s-task:%s set the launch condition for fill-history s-task:%s, window:%" PRId64 " - %" PRId64
            " verRange:%" PRId64 " - %" PRId64 ", init:%" PRId64,
            pTask->id.idStr, pHTask->id.idStr, pRange->window.skey, pRange->window.ekey, pRange->range.minVer,
            pRange->range.maxVer, pHTask->execInfo.init);
  } else {
    stDebug("s-task:%s no fill-history condition for non-source task:%s", pTask->id.idStr, pHTask->id.idStr);
  }

  // check if downstream tasks have been ready
  streamTaskHandleEvent(pHTask->status.pSM, TASK_EVENT_INIT_SCANHIST);
}

static void noRetryLaunchFillHistoryTask(SStreamTask* pTask, SLaunchHTaskInfo* pInfo, int64_t now) {
  SStreamMeta*      pMeta = pTask->pMeta;
  SHistoryTaskInfo* pHTaskInfo = &pTask->hTaskInfo;

  int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
  streamMetaAddTaskLaunchResult(pMeta, pInfo->hTaskId.streamId, pInfo->hTaskId.taskId, 0, now, false);

  stError("s-task:%s max retry:%d reached, quit from retrying launch related fill-history task:0x%x, ref:%d",
          pTask->id.idStr, MAX_RETRY_LAUNCH_HISTORY_TASK, (int32_t)pHTaskInfo->id.taskId, ref);

  pHTaskInfo->id.taskId = 0;
  pHTaskInfo->id.streamId = 0;
}

static void doRetryLaunchFillHistoryTask(SStreamTask* pTask, SLaunchHTaskInfo* pInfo, int64_t now) {
  SStreamMeta*      pMeta = pTask->pMeta;
  SHistoryTaskInfo* pHTaskInfo = &pTask->hTaskInfo;

  if (streamTaskShouldStop(pTask)) {  // record the failure
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:0x%" PRIx64 " stopped, not launch rel history task:0x%" PRIx64 ", ref:%d", pInfo->id.taskId,
            pInfo->hTaskId.taskId, ref);

    streamMetaAddTaskLaunchResult(pMeta, pInfo->hTaskId.streamId, pInfo->hTaskId.taskId, 0, now, false);
    taosMemoryFree(pInfo);
  } else {
    char*   p = streamTaskGetStatus(pTask)->name;
    int32_t hTaskId = pHTaskInfo->id.taskId;

    stDebug("s-task:%s status:%s failed to launch fill-history task:0x%x, retry launch:%dms, retryCount:%d",
            pTask->id.idStr, p, hTaskId, pHTaskInfo->waitInterval, pHTaskInfo->retryTimes);

    taosTmrReset(tryLaunchHistoryTask, LAUNCH_HTASK_INTERVAL, pInfo, streamTimer, &pHTaskInfo->pTimer);
  }
}

static void tryLaunchHistoryTask(void* param, void* tmrId) {
  SLaunchHTaskInfo* pInfo = param;
  SStreamMeta*      pMeta = pInfo->pMeta;
  int64_t           now = taosGetTimestampMs();

  streamMetaWLock(pMeta);

  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &pInfo->id, sizeof(pInfo->id));
  if (ppTask == NULL || *ppTask == NULL) {
    stError("s-task:0x%x and rel fill-history task:0x%" PRIx64 " all have been destroyed, not launch",
            (int32_t)pInfo->id.taskId, pInfo->hTaskId.taskId);
    streamMetaWUnLock(pMeta);

    // already dropped, no need to set the failure info into the stream task meta.
    taosMemoryFree(pInfo);
    return;
  }

  if (streamTaskShouldStop(*ppTask)) {
    ASSERT((*ppTask)->status.timerActive >= 1);

    char*   p = streamTaskGetStatus(*ppTask)->name;
    int32_t ref = atomic_sub_fetch_32(&(*ppTask)->status.timerActive, 1);
    stDebug("s-task:%s status:%s should stop, quit launch fill-history task timer, retry:%d, ref:%d",
            (*ppTask)->id.idStr, p, (*ppTask)->hTaskInfo.retryTimes, ref);

    streamMetaWUnLock(pMeta);

    // record the related fill-history task failed
    streamMetaAddTaskLaunchResult(pMeta, pInfo->hTaskId.streamId, pInfo->hTaskId.taskId, 0, now, false);
    taosMemoryFree(pInfo);
    return;
  }

  SStreamTask* pTask = streamMetaAcquireTaskNoLock(pMeta, pInfo->id.streamId, pInfo->id.taskId);
  streamMetaWUnLock(pMeta);

  if (pTask != NULL) {
    SHistoryTaskInfo* pHTaskInfo = &pTask->hTaskInfo;

    pHTaskInfo->tickCount -= 1;
    if (pHTaskInfo->tickCount > 0) {
      taosTmrReset(tryLaunchHistoryTask, LAUNCH_HTASK_INTERVAL, pInfo, streamTimer, &pHTaskInfo->pTimer);
      streamMetaReleaseTask(pMeta, pTask);
      return;
    }

    if (pHTaskInfo->retryTimes > MAX_RETRY_LAUNCH_HISTORY_TASK) {
      noRetryLaunchFillHistoryTask(pTask, pInfo, now);
    } else {  // not reach the limitation yet, let's continue retrying launch related fill-history task.
      streamTaskSetRetryInfoForLaunch(pHTaskInfo);
      ASSERT(pTask->status.timerActive >= 1);

      // abort the timer if intend to stop task
      SStreamTask* pHTask = streamMetaAcquireTask(pMeta, pHTaskInfo->id.streamId, pHTaskInfo->id.taskId);
      if (pHTask == NULL) {
        doRetryLaunchFillHistoryTask(pTask, pInfo, now);
        streamMetaReleaseTask(pMeta, pTask);
        return;
      } else {
        checkFillhistoryTaskStatus(pTask, pHTask);
        streamMetaReleaseTask(pMeta, pHTask);

        // not in timer anymore
        int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
        stDebug("s-task:0x%x fill-history task launch completed, retry times:%d, ref:%d", (int32_t)pInfo->id.taskId,
                pHTaskInfo->retryTimes, ref);
      }
    }

    streamMetaReleaseTask(pMeta, pTask);
  } else {
    streamMetaAddTaskLaunchResult(pMeta, pInfo->hTaskId.streamId, pInfo->hTaskId.taskId, 0, now, false);

    int32_t ref = atomic_sub_fetch_32(&(*ppTask)->status.timerActive, 1);
    stError("s-task:0x%x rel fill-history task:0x%" PRIx64 " may have been destroyed, not launch, ref:%d",
            (int32_t)pInfo->id.taskId, pInfo->hTaskId.taskId, ref);
  }

  taosMemoryFree(pInfo);
}

SLaunchHTaskInfo* createHTaskLaunchInfo(SStreamMeta* pMeta, int64_t streamId, int32_t taskId, int64_t hStreamId,
                                        int32_t hTaskId) {
  SLaunchHTaskInfo* pInfo = taosMemoryCalloc(1, sizeof(SLaunchHTaskInfo));
  if (pInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pInfo->id.streamId = streamId;
  pInfo->id.taskId = taskId;

  pInfo->hTaskId.streamId = hStreamId;
  pInfo->hTaskId.taskId = hTaskId;

  pInfo->pMeta = pMeta;
  return pInfo;
}

static int32_t launchNotBuiltFillHistoryTask(SStreamTask* pTask) {
  SStreamMeta*         pMeta = pTask->pMeta;
  STaskExecStatisInfo* pExecInfo = &pTask->execInfo;
  const char*          idStr = pTask->id.idStr;
  int64_t              hStreamId = pTask->hTaskInfo.id.streamId;
  int32_t              hTaskId = pTask->hTaskInfo.id.taskId;
  ASSERT(hTaskId != 0);

  stWarn("s-task:%s vgId:%d failed to launch history task:0x%x, since not built yet", idStr, pMeta->vgId, hTaskId);

  SLaunchHTaskInfo* pInfo = createHTaskLaunchInfo(pMeta, pTask->id.streamId, pTask->id.taskId, hStreamId, hTaskId);
  if (pInfo == NULL) {
    stError("s-task:%s failed to launch related fill-history task, since Out Of Memory", idStr);

    streamMetaAddTaskLaunchResult(pMeta, hStreamId, hTaskId, pExecInfo->init, pExecInfo->start, false);
    return terrno;
  }

  // set the launch time info
  streamTaskInitForLaunchHTask(&pTask->hTaskInfo);

  // check for the timer
  if (pTask->hTaskInfo.pTimer == NULL) {
    int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
    pTask->hTaskInfo.pTimer = taosTmrStart(tryLaunchHistoryTask, WAIT_FOR_MINIMAL_INTERVAL, pInfo, streamTimer);

    if (pTask->hTaskInfo.pTimer == NULL) {
      ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
      stError("s-task:%s failed to start timer, related fill-history task not launched, ref:%d", idStr, ref);

      taosMemoryFree(pInfo);
      streamMetaAddTaskLaunchResult(pMeta, hStreamId, hTaskId, pExecInfo->init, pExecInfo->start, false);
      return terrno;
    }

    ASSERT(ref >= 1);

    stDebug("s-task:%s set timer active flag, ref:%d", idStr, ref);
  } else {  // timer exists
    ASSERT(pTask->status.timerActive >= 1);
    stDebug("s-task:%s set timer active flag, task timer not null", idStr);
    taosTmrReset(tryLaunchHistoryTask, WAIT_FOR_MINIMAL_INTERVAL, pInfo, streamTimer, &pTask->hTaskInfo.pTimer);
  }

  return TSDB_CODE_SUCCESS;
}

// an fill history task needs to be started.
int32_t streamLaunchFillHistoryTask(SStreamTask* pTask) {
  SStreamMeta*         pMeta = pTask->pMeta;
  STaskExecStatisInfo* pExecInfo = &pTask->execInfo;
  const char*          idStr = pTask->id.idStr;
  int64_t              hStreamId = pTask->hTaskInfo.id.streamId;
  int32_t              hTaskId = pTask->hTaskInfo.id.taskId;
  ASSERT(hTaskId != 0);

  // check stream task status in the first place.
  SStreamTaskState* pStatus = streamTaskGetStatus(pTask);
  if (pStatus->state != TASK_STATUS__READY && pStatus->state != TASK_STATUS__HALT) {
    stDebug("s-task:%s not launch related fill-history task:0x%" PRIx64 "-0x%x, status:%s", idStr, hStreamId, hTaskId,
            pStatus->name);

    streamMetaAddTaskLaunchResult(pMeta, hStreamId, hTaskId, pExecInfo->init, pExecInfo->start, false);
    return -1;  // todo set the correct error code
  }

  stDebug("s-task:%s start to launch related fill-history task:0x%" PRIx64 "-0x%x", idStr, hStreamId, hTaskId);

  // Set the execute conditions, including the query time window and the version range
  streamMetaRLock(pMeta);
  SStreamTask** pHTask = taosHashGet(pMeta->pTasksMap, &pTask->hTaskInfo.id, sizeof(pTask->hTaskInfo.id));
  streamMetaRUnLock(pMeta);

  if (pHTask != NULL) {  // it is already added into stream meta store.
    SStreamTask* pHisTask = streamMetaAcquireTask(pMeta, hStreamId, hTaskId);
    if (pHisTask == NULL) {
      stDebug("s-task:%s failed acquire and start fill-history task, it may have been dropped/stopped", idStr);
      streamMetaAddTaskLaunchResult(pMeta, hStreamId, hTaskId, pExecInfo->init, pExecInfo->start, false);
    } else {
      if (pHisTask->status.downstreamReady == 1) {  // it's ready now, do nothing
        stDebug("s-task:%s fill-history task is ready, no need to check downstream", pHisTask->id.idStr);
        streamMetaAddTaskLaunchResult(pMeta, hStreamId, hTaskId, pExecInfo->init, pExecInfo->start, true);
      } else {  // exist, but not ready, continue check downstream task status
        checkFillhistoryTaskStatus(pTask, pHisTask);
      }

      streamMetaReleaseTask(pMeta, pHisTask);
    }

    return TSDB_CODE_SUCCESS;
  } else {
    return launchNotBuiltFillHistoryTask(pTask);
  }
}

int32_t streamTaskResetTimewindowFilter(SStreamTask* pTask) {
  void* exec = pTask->exec.pExecutor;
  return qStreamInfoResetTimewindowFilter(exec);
}

bool streamHistoryTaskSetVerRangeStep2(SStreamTask* pTask, int64_t nextProcessVer) {
  SVersionRange* pRange = &pTask->dataRange.range;
  ASSERT(nextProcessVer >= pRange->maxVer);

  int64_t walScanStartVer = pRange->maxVer + 1;
  if (walScanStartVer > nextProcessVer - 1) {
    stDebug(
        "s-task:%s no need to perform secondary scan-history data(step 2), since no data ingest during step1 scan, "
        "related stream task currentVer:%" PRId64,
        pTask->id.idStr, nextProcessVer);
    return true;
  } else {
    // 2. do secondary scan of the history data, the time window remain, and the version range is updated to
    // [pTask->dataRange.range.maxVer, ver1]
    pRange->minVer = walScanStartVer;
    pRange->maxVer = nextProcessVer - 1;
    return false;
  }
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

int32_t tEncodeStreamTaskCheckpointReq(SEncoder* pEncoder, const SStreamTaskCheckpointReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->nodeId) < 0) return -1;
  tEndEncode(pEncoder);
  return 0;
}

int32_t tDecodeStreamTaskCheckpointReq(SDecoder* pDecoder, SStreamTaskCheckpointReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->nodeId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

void streamTaskSetRangeStreamCalc(SStreamTask* pTask) {
  SDataRange* pRange = &pTask->dataRange;

  if (!HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    if (pTask->info.fillHistory == 1) {
      stDebug("s-task:%s fill-history task, time window:%" PRId64 "-%" PRId64 ", verRange:%" PRId64 "-%" PRId64,
              pTask->id.idStr, pRange->window.skey, pRange->window.ekey, pRange->range.minVer, pRange->range.maxVer);
    } else {
      stDebug(
          "s-task:%s no related fill-history task, stream time window and verRange are not set. default stream time "
          "window:%" PRId64 "-%" PRId64 ", verRange:%" PRId64 "-%" PRId64,
          pTask->id.idStr, pRange->window.skey, pRange->window.ekey, pRange->range.minVer, pRange->range.maxVer);
    }
  } else {
    ASSERT(pTask->info.fillHistory == 0);
    if (pTask->info.taskLevel >= TASK_LEVEL__AGG) {
      return;
    }

    stDebug("s-task:%s level:%d related fill-history task exists, stream task timeWindow:%" PRId64 " - %" PRId64
            ", verRang:%" PRId64 " - %" PRId64,
            pTask->id.idStr, pTask->info.taskLevel, pRange->window.skey, pRange->window.ekey, pRange->range.minVer,
            pRange->range.maxVer);

    SVersionRange verRange = pRange->range;
    STimeWindow win = pRange->window;
    streamSetParamForStreamScannerStep2(pTask, &verRange, &win);
  }
}
