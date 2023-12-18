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
} SLaunchHTaskInfo;

typedef struct STaskRecheckInfo {
  SStreamTask*        pTask;
  SStreamTaskCheckReq req;
  void*               checkTimer;
} STaskRecheckInfo;

typedef struct STaskInitTs {
  int64_t start;
  int64_t end;
  bool    success;
} STaskInitTs;

static int32_t           streamSetParamForScanHistory(SStreamTask* pTask);
static void              streamTaskSetRangeStreamCalc(SStreamTask* pTask);
static int32_t           initScanHistoryReq(SStreamTask* pTask, SStreamScanHistoryReq* pReq, int8_t igUntreated);
static SLaunchHTaskInfo* createHTaskLaunchInfo(SStreamMeta* pMeta, int64_t streamId, int32_t taskId);
static void              tryLaunchHistoryTask(void* param, void* tmrId);
static void              doProcessDownstreamReadyRsp(SStreamTask* pTask);

int32_t streamTaskSetReady(SStreamTask* pTask) {
  char*       p = NULL;
  int32_t     numOfDowns = streamTaskGetNumOfDownstream(pTask);
  ETaskStatus status = streamTaskGetStatus(pTask, &p);

  if ((status == TASK_STATUS__SCAN_HISTORY || status == TASK_STATUS__STREAM_SCAN_HISTORY) &&
      pTask->info.taskLevel != TASK_LEVEL__SOURCE) {
    pTask->numOfWaitingUpstream = taosArrayGetSize(pTask->upstreamInfo.pList);
    stDebug("s-task:%s level:%d task wait for %d upstream tasks complete scan-history procedure, status:%s",
            pTask->id.idStr, pTask->info.taskLevel, pTask->numOfWaitingUpstream, p);
  }

  ASSERT(pTask->status.downstreamReady == 0);
  pTask->status.downstreamReady = 1;

  pTask->execInfo.start = taosGetTimestampMs();
  int64_t el = (pTask->execInfo.start - pTask->execInfo.init);
  stDebug("s-task:%s all %d downstream ready, init completed, elapsed time:%" PRId64 "ms, task status:%s",
          pTask->id.idStr, numOfDowns, el, p);

  streamMetaUpdateTaskDownstreamStatus(pTask->pMeta, pTask->id.streamId, pTask->id.taskId, pTask->execInfo.init,
                                       pTask->execInfo.start, true);
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

  char*       p = NULL;
  ETaskStatus status = streamTaskGetStatus(pTask, &p);
  if (status == TASK_STATUS__DROPPING || status == TASK_STATUS__STOP) {
    streamMetaReleaseTask(pTask->pMeta, pTask);
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s status:%s not start scan-history again, ref:%d", pTask->id.idStr, p, ref);
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
  ETaskStatus status = streamTaskGetStatus(pTask, NULL);

  ASSERT(pTask->status.downstreamReady == 1 &&
         ((status == TASK_STATUS__SCAN_HISTORY) || (status == TASK_STATUS__STREAM_SCAN_HISTORY)));

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
    pTask->checkReqIds = taosArrayInit(numOfVgs, sizeof(int64_t));

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

int32_t streamTaskOnNormalTaskReady(SStreamTask* pTask) {
  const char* id = pTask->id.idStr;

  streamTaskSetReady(pTask);
  streamTaskSetRangeStreamCalc(pTask);

  char*       p = NULL;
  ETaskStatus status = streamTaskGetStatus(pTask, &p);
  ASSERT(status == TASK_STATUS__READY);

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    int64_t startVer = walReaderGetCurrentVer(pTask->exec.pWalReader);
    if (startVer == -1) {
      startVer = pTask->chkInfo.nextProcessVer;
    }

    stDebug("s-task:%s no need to scan-history data, status:%s, sched-status:%d, ready for data from wal ver:%" PRId64,
            id, p, pTask->status.schedStatus, startVer);
  } else {
    stDebug("s-task:%s level:%d status:%s sched-status:%d", id, pTask->info.taskLevel, p, pTask->status.schedStatus);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskOnScanhistoryTaskReady(SStreamTask* pTask) {
  const char* id = pTask->id.idStr;

  // set the state to be ready
  streamTaskSetReady(pTask);
  streamTaskSetRangeStreamCalc(pTask);

  char*       p = NULL;
  ETaskStatus status = streamTaskGetStatus(pTask, &p);
  ASSERT(status == TASK_STATUS__SCAN_HISTORY || status == TASK_STATUS__STREAM_SCAN_HISTORY);

  stDebug("s-task:%s enter into scan-history data stage, status:%s", id, p);
  streamTaskStartScanHistory(pTask);

  // start the related fill-history task, when current task is ready
  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    streamLaunchFillHistoryTask(pTask);
  }

  return TSDB_CODE_SUCCESS;
}

void doProcessDownstreamReadyRsp(SStreamTask* pTask) {
  EStreamTaskEvent event;
  if (pTask->info.fillHistory == 0) {
    event = HAS_RELATED_FILLHISTORY_TASK(pTask) ? TASK_EVENT_INIT_STREAM_SCANHIST : TASK_EVENT_INIT;
  } else {
    event = TASK_EVENT_INIT_SCANHIST;
  }

  streamTaskOnHandleEventSuccess(pTask->status.pSM, event);
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
        taosArrayDestroy(pTask->checkReqIds);
        pTask->checkReqIds = NULL;

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
                ", "
                "not check wait for downstream task nodeUpdate, and all tasks restart",
                id, pRsp->upstreamNodeId, pRsp->oldStage, pTask->pMeta->stage);
        addIntoNodeUpdateList(pTask, pRsp->upstreamNodeId);
      } else {
        stError(
            "s-task:%s downstream taskId:0x%x (vgId:%d) not leader, self dispatch epset needs to be updated, not check "
            "downstream again, nodeUpdate needed",
            id, pRsp->downstreamTaskId, pRsp->downstreamNodeId);
        addIntoNodeUpdateList(pTask, pRsp->downstreamNodeId);
      }

      streamMetaUpdateTaskDownstreamStatus(pTask->pMeta, pTask->id.streamId, pTask->id.taskId, pTask->execInfo.init,
                                           taosGetTimestampMs(), false);

      // automatically set the related fill-history task to be failed.
      if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
        STaskId* pId = &pTask->hTaskInfo.id;
        int64_t  current = taosGetTimestampMs();

        SStreamTask* pHTask = streamMetaAcquireTask(pTask->pMeta, pId->streamId, pId->taskId);
        if (pHTask != NULL) {
          streamMetaUpdateTaskDownstreamStatus(pTask->pMeta, pId->streamId, pId->taskId, pHTask->execInfo.init, current,
                                               false);
          streamMetaReleaseTask(pTask->pMeta, pHTask);
        } else {
          streamMetaUpdateTaskDownstreamStatus(pTask->pMeta, pId->streamId, pId->taskId, 0, current, false);
        }
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

int32_t streamRestoreParam(SStreamTask* pTask) {
  stDebug("s-task:%s restore operator param after scan-history", pTask->id.idStr);
  return qRestoreStreamOperatorOption(pTask->exec.pExecutor);
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

int32_t streamAggUpstreamScanHistoryFinish(SStreamTask* pTask) {
  void* exec = pTask->exec.pExecutor;
  if (pTask->info.fillHistory && qRestoreStreamOperatorOption(exec) < 0) {
    return -1;
  }

  if (qStreamRecoverFinish(exec) < 0) {
    return -1;
  }
  return 0;
}

int32_t streamProcessScanHistoryFinishReq(SStreamTask* pTask, SStreamScanHistoryFinishReq* pReq,
                                          SRpcHandleInfo* pRpcInfo) {
  int32_t taskLevel = pTask->info.taskLevel;
  ASSERT(taskLevel == TASK_LEVEL__AGG || taskLevel == TASK_LEVEL__SINK);

  const char* id = pTask->id.idStr;
  char*       p = NULL;
  ETaskStatus status = streamTaskGetStatus(pTask, &p);

  if (status != TASK_STATUS__SCAN_HISTORY && status != TASK_STATUS__STREAM_SCAN_HISTORY) {
    stError("s-task:%s not in scan-history status, status:%s return upstream:0x%x scan-history finish directly", id, p,
            pReq->upstreamTaskId);

    void*   pBuf = NULL;
    int32_t len = 0;
    streamTaskBuildScanhistoryRspMsg(pTask, pReq, &pBuf, &len);

    SRpcMsg msg = {.info = *pRpcInfo};
    initRpcMsg(&msg, 0, pBuf, sizeof(SMsgHead) + len);

    tmsgSendRsp(&msg);
    stDebug("s-task:%s level:%d notify upstream:0x%x(vgId:%d) to continue process data in WAL", id, taskLevel,
            pReq->upstreamTaskId, pReq->upstreamNodeId);
    return 0;
  }

  // sink tasks do not send end of scan history msg to its upstream, which is agg task.
  streamAddEndScanHistoryMsg(pTask, pRpcInfo, pReq);

  int32_t left = atomic_sub_fetch_32(&pTask->numOfWaitingUpstream, 1);
  ASSERT(left >= 0);

  if (left == 0) {
    int32_t numOfTasks = taosArrayGetSize(pTask->upstreamInfo.pList);
    if (taskLevel == TASK_LEVEL__AGG) {
      stDebug(
          "s-task:%s all %d upstream tasks finish scan-history data, set param for agg task for stream data processing "
          "and send rsp to all upstream tasks",
          id, numOfTasks);
      streamAggUpstreamScanHistoryFinish(pTask);
    } else {
      stDebug("s-task:%s all %d upstream task(s) finish scan-history data, and rsp to all upstream tasks", id,
              numOfTasks);
    }

    // all upstream tasks have completed the scan-history task in the stream time window, let's start to extract data
    // from the WAL files, which contains the real time stream data.
    streamNotifyUpstreamContinue(pTask);

    // mnode will not send the pause/resume message to the sink task, so no need to enable the pause for sink tasks.
    if (taskLevel == TASK_LEVEL__AGG) {
      /*int32_t code = */ streamTaskScanHistoryDataComplete(pTask);
    } else {  // for sink task, set normal
      streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_SCANHIST_DONE);
    }
  } else {
    stDebug("s-task:%s receive scan-history data finish msg from upstream:0x%x(index:%d), unfinished:%d", id,
            pReq->upstreamTaskId, pReq->childId, left);
  }

  return 0;
}

int32_t streamProcessScanHistoryFinishRsp(SStreamTask* pTask) {
  ETaskStatus status = streamTaskGetStatus(pTask, NULL);

  // task restart now, not handle the scan-history finish rsp
  if (status == TASK_STATUS__UNINIT) {
    return TSDB_CODE_INVALID_MSG;
  }

  ASSERT(status == TASK_STATUS__SCAN_HISTORY || status == TASK_STATUS__STREAM_SCAN_HISTORY);
  SStreamMeta* pMeta = pTask->pMeta;

  // execute in the scan history complete call back msg, ready to process data from inputQ
  int32_t code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_SCANHIST_DONE);
  streamTaskSetSchedStatusInactive(pTask);

  streamMetaWLock(pMeta);
  streamMetaSaveTask(pMeta, pTask);
  streamMetaCommit(pMeta);
  streamMetaWUnLock(pMeta);

  // for source tasks, let's continue execute.
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    streamSchedExec(pTask);
  }

  return TSDB_CODE_SUCCESS;
}

static void checkFillhistoryTaskStatus(SStreamTask* pTask, SStreamTask* pHTask) {
  SDataRange* pRange = &pHTask->dataRange;

  // the query version range should be limited to the already processed data
  pRange->range.minVer = 0;
  pRange->range.maxVer = pTask->chkInfo.nextProcessVer - 1;
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

static void tryLaunchHistoryTask(void* param, void* tmrId) {
  SLaunchHTaskInfo* pInfo = param;
  SStreamMeta*      pMeta = pInfo->pMeta;

  streamMetaWLock(pMeta);

  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &pInfo->id, sizeof(pInfo->id));
  if (ppTask) {
    ASSERT((*ppTask)->status.timerActive >= 1);

    if (streamTaskShouldStop(*ppTask)) {
      char* p = NULL;
      streamTaskGetStatus((*ppTask), &p);
      int32_t ref = atomic_sub_fetch_32(&(*ppTask)->status.timerActive, 1);
      stDebug("s-task:%s status:%s should stop, quit launch fill-history task timer, retry:%d, ref:%d",
              (*ppTask)->id.idStr, p, (*ppTask)->hTaskInfo.retryTimes, ref);

      taosMemoryFree(pInfo);
      streamMetaWUnLock(pMeta);
      return;
    }
  }
  streamMetaWUnLock(pMeta);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pInfo->id.streamId, pInfo->id.taskId);
  if (pTask != NULL) {
    SHistoryTaskInfo* pHTaskInfo = &pTask->hTaskInfo;

    pHTaskInfo->tickCount -= 1;
    if (pHTaskInfo->tickCount > 0) {
      taosTmrReset(tryLaunchHistoryTask, LAUNCH_HTASK_INTERVAL, pInfo, streamTimer, &pHTaskInfo->pTimer);
      streamMetaReleaseTask(pMeta, pTask);
      return;
    }

    if (pHTaskInfo->retryTimes > MAX_RETRY_LAUNCH_HISTORY_TASK) {
      int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
      streamMetaReleaseTask(pMeta, pTask);

      stError("s-task:%s max retry:%d reached, quit from retrying launch related fill-history task:0x%x, ref:%d",
              pTask->id.idStr, MAX_RETRY_LAUNCH_HISTORY_TASK, (int32_t)pHTaskInfo->id.taskId, ref);

      pHTaskInfo->id.taskId = 0;
      pHTaskInfo->id.streamId = 0;
    } else {  // not reach the limitation yet, let's continue retrying launch related fill-history task.
      streamTaskSetRetryInfoForLaunch(pHTaskInfo);
      ASSERT(pTask->status.timerActive >= 1);

      // abort the timer if intend to stop task
      SStreamTask* pHTask = streamMetaAcquireTask(pMeta, pHTaskInfo->id.streamId, pHTaskInfo->id.taskId);
      if (pHTask == NULL && (!streamTaskShouldStop(pTask))) {
        char*   p = NULL;
        int32_t hTaskId = pHTaskInfo->id.taskId;

        streamTaskGetStatus(pTask, &p);
        stDebug("s-task:%s status:%s failed to launch fill-history task:0x%x, retry launch:%dms, retryCount:%d",
                pTask->id.idStr, p, hTaskId, pHTaskInfo->waitInterval, pHTaskInfo->retryTimes);

        taosTmrReset(tryLaunchHistoryTask, LAUNCH_HTASK_INTERVAL, pInfo, streamTimer, &pHTaskInfo->pTimer);
        streamMetaReleaseTask(pMeta, pTask);
        return;
      }

      if (pHTask != NULL) {
        checkFillhistoryTaskStatus(pTask, pHTask);
        streamMetaReleaseTask(pMeta, pHTask);
      }

      // not in timer anymore
      int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
      stDebug("s-task:0x%x fill-history task launch completed, retry times:%d, ref:%d", (int32_t)pInfo->id.taskId,
              pHTaskInfo->retryTimes, ref);
      streamMetaReleaseTask(pMeta, pTask);
    }
  } else {
    stError("s-task:0x%x failed to load task, it may have been destroyed, not launch related fill-history task",
            (int32_t)pInfo->id.taskId);
  }

  taosMemoryFree(pInfo);
}

SLaunchHTaskInfo* createHTaskLaunchInfo(SStreamMeta* pMeta, int64_t streamId, int32_t taskId) {
  SLaunchHTaskInfo* pInfo = taosMemoryCalloc(1, sizeof(SLaunchHTaskInfo));
  if (pInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pInfo->id.taskId = taskId;
  pInfo->id.streamId = streamId;
  pInfo->pMeta = pMeta;
  return pInfo;
}

// an fill history task needs to be started.
int32_t streamLaunchFillHistoryTask(SStreamTask* pTask) {
  SStreamMeta* pMeta = pTask->pMeta;
  int32_t      hTaskId = pTask->hTaskInfo.id.taskId;

  ASSERT((hTaskId != 0) && (pTask->status.downstreamReady == 1));
  stDebug("s-task:%s start to launch related fill-history task:0x%" PRIx64 "-0x%x", pTask->id.idStr,
          pTask->hTaskInfo.id.streamId, hTaskId);

  // Set the execute conditions, including the query time window and the version range
  SStreamTask** pHTask = taosHashGet(pMeta->pTasksMap, &pTask->hTaskInfo.id, sizeof(pTask->hTaskInfo.id));
  if (pHTask == NULL) {
    stWarn("s-task:%s vgId:%d failed to launch history task:0x%x, since not built yet", pTask->id.idStr, pMeta->vgId,
           hTaskId);

    SLaunchHTaskInfo* pInfo = createHTaskLaunchInfo(pTask->pMeta, pTask->id.streamId, pTask->id.taskId);
    if (pInfo == NULL) {
      stError("s-task:%s failed to launch related fill-history task, since Out Of Memory", pTask->id.idStr);
      return terrno;
    }

    streamTaskInitForLaunchHTask(&pTask->hTaskInfo);
    if (pTask->hTaskInfo.pTimer == NULL) {
      int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
      pTask->hTaskInfo.pTimer = taosTmrStart(tryLaunchHistoryTask, WAIT_FOR_MINIMAL_INTERVAL, pInfo, streamTimer);
      if (pTask->hTaskInfo.pTimer == NULL) {
        atomic_sub_fetch_32(&pTask->status.timerActive, 1);
        stError("s-task:%s failed to start timer, related fill-history task not launched, ref:%d", pTask->id.idStr,
                pTask->status.timerActive);
        taosMemoryFree(pInfo);
      } else {
        ASSERT(ref >= 1);
        stDebug("s-task:%s set timer active flag, ref:%d", pTask->id.idStr, ref);
      }
    } else {  // timer exists
      ASSERT(pTask->status.timerActive >= 1);
      stDebug("s-task:%s set timer active flag, task timer not null", pTask->id.idStr);
      taosTmrReset(tryLaunchHistoryTask, WAIT_FOR_MINIMAL_INTERVAL, pInfo, streamTimer, &pTask->hTaskInfo.pTimer);
    }

    return TSDB_CODE_SUCCESS;
  }

  if ((*pHTask)->status.downstreamReady == 1) {
    stDebug("s-task:%s fill-history task is ready, no need to check downstream", (*pHTask)->id.idStr);
  } else {
    checkFillhistoryTaskStatus(pTask, *pHTask);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskScanHistoryDataComplete(SStreamTask* pTask) {
  if (streamTaskGetStatus(pTask, NULL) == TASK_STATUS__DROPPING) {
    return 0;
  }

  // restore param
  int32_t code = 0;
  if (pTask->info.fillHistory) {
    code = streamRestoreParam(pTask);
    if (code < 0) {
      return -1;
    }
  }

  // dispatch scan-history finish req to all related downstream task
  code = streamDispatchScanHistoryFinishMsg(pTask);
  if (code < 0) {
    return -1;
  }

  return 0;
}

int32_t streamTaskFillHistoryFinished(SStreamTask* pTask) {
  void* exec = pTask->exec.pExecutor;
  return qStreamInfoResetTimewindowFilter(exec);
}

bool streamHistoryTaskSetVerRangeStep2(SStreamTask* pTask, int64_t nextProcessVer) {
  SVersionRange* pRange = &pTask->dataRange.range;
  ASSERT(nextProcessVer >= pRange->maxVer);

  int64_t walScanStartVer = pRange->maxVer + 1;
  if (walScanStartVer > nextProcessVer - 1) {
    // no input data yet. no need to execute the secondary scan while stream task halt
    streamTaskFillHistoryFinished(pTask);
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

int32_t tEncodeStreamScanHistoryFinishReq(SEncoder* pEncoder, const SStreamScanHistoryFinishReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->childId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamScanHistoryFinishReq(SDecoder* pDecoder, SStreamScanHistoryFinishReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->downstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->childId) < 0) return -1;
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
    int64_t ekey = 0;
    if (pRange->window.ekey < INT64_MAX) {
      ekey = pRange->window.ekey + 1;
    } else {
      ekey = pRange->window.ekey;
    }

    int64_t ver = pRange->range.minVer;

    pRange->window.skey = ekey;
    pRange->window.ekey = INT64_MAX;
    pRange->range.minVer = 0;
    pRange->range.maxVer = ver;

    stDebug("s-task:%s level:%d related fill-history task exists, update stream calc time window:%" PRId64 " - %" PRId64
            ", verRang:%" PRId64 " - %" PRId64,
            pTask->id.idStr, pTask->info.taskLevel, pRange->window.skey, pRange->window.ekey, pRange->range.minVer,
            pRange->range.maxVer);
  }
}

void streamTaskPause(SStreamTask* pTask, SStreamMeta* pMeta) {
  streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_PAUSE);

  int32_t num = atomic_add_fetch_32(&pMeta->numOfPausedTasks, 1);
  stInfo("vgId:%d s-task:%s pause stream task. pause task num:%d", pMeta->vgId, pTask->id.idStr, num);

  // in case of fill-history task, stop the tsdb file scan operation.
  if (pTask->info.fillHistory == 1) {
    void* pExecutor = pTask->exec.pExecutor;
    qKillTask(pExecutor, TSDB_CODE_SUCCESS);
  }

  stDebug("vgId:%d s-task:%s set pause flag and pause task", pMeta->vgId, pTask->id.idStr);
}

void streamTaskResume(SStreamTask* pTask) {
  char*        p = NULL;
  ETaskStatus  status = streamTaskGetStatus(pTask, &p);
  SStreamMeta* pMeta = pTask->pMeta;

  if (status == TASK_STATUS__PAUSE || status == TASK_STATUS__HALT) {
    streamTaskRestoreStatus(pTask);

    char* pNew = NULL;
    streamTaskGetStatus(pTask, &pNew);
    if (status == TASK_STATUS__PAUSE) {
      int32_t num = atomic_sub_fetch_32(&pMeta->numOfPausedTasks, 1);
      stInfo("s-task:%s status:%s resume from %s, paused task(s):%d", pTask->id.idStr, pNew, p, num);
    } else {
      stInfo("s-task:%s status:%s resume from %s", pTask->id.idStr, pNew, p);
    }
  } else {
    stDebug("s-task:%s status:%s not in pause/halt status, no need to resume", pTask->id.idStr, p);
  }
}

static void displayStatusInfo(SStreamMeta* pMeta, SHashObj* pTaskSet, bool succ) {
  int32_t vgId = pMeta->vgId;
  void*   pIter = NULL;
  size_t  keyLen = 0;

  stInfo("vgId:%d %d tasks check-downstream completed %s", vgId, taosHashGetSize(pTaskSet),
         succ ? "success" : "failed");

  while ((pIter = taosHashIterate(pTaskSet, pIter)) != NULL) {
    STaskInitTs* pInfo = pIter;
    void*        key = taosHashGetKey(pIter, &keyLen);

    SStreamTask** pTask1 = taosHashGet(pMeta->pTasksMap, key, sizeof(STaskId));
    if (pTask1 == NULL) {
      stInfo("s-task:0x%x is dropped already, %s", (int32_t)((STaskId*)key)->taskId, succ ? "success" : "failed");
    } else {
      stInfo("s-task:%s level:%d vgId:%d, init:%" PRId64 ", initEnd:%" PRId64 ", %s", (*pTask1)->id.idStr,
             (*pTask1)->info.taskLevel, vgId, pInfo->start, pInfo->end, pInfo->success ? "success" : "failed");
    }
  }
}

int32_t streamMetaUpdateTaskDownstreamStatus(SStreamMeta* pMeta, int64_t streamId, int32_t taskId, int64_t startTs,
                                             int64_t endTs, bool ready) {
  STaskStartInfo* pStartInfo = &pMeta->startInfo;
  STaskId         id = {.streamId = streamId, .taskId = taskId};

  streamMetaWLock(pMeta);
  SHashObj* pDst = ready ? pStartInfo->pReadyTaskSet : pStartInfo->pFailedTaskSet;

  STaskInitTs initTs = {.start = startTs, .end = endTs, .success = ready};
  taosHashPut(pDst, &id, sizeof(id), &initTs, sizeof(STaskInitTs));

  int32_t numOfTotal = streamMetaGetNumOfTasks(pMeta);
  int32_t numOfRecv = taosHashGetSize(pStartInfo->pReadyTaskSet) + taosHashGetSize(pStartInfo->pFailedTaskSet);

  if (numOfRecv == numOfTotal) {
    pStartInfo->readyTs = taosGetTimestampMs();
    pStartInfo->elapsedTime = (pStartInfo->startTs != 0) ? pStartInfo->readyTs - pStartInfo->startTs : 0;

    stDebug("vgId:%d all %d task(s) check downstream completed, last completed task:0x%x startTs:%" PRId64
            ", readyTs:%" PRId64 " total elapsed time:%.2fs",
            pMeta->vgId, numOfTotal, taskId, pStartInfo->startTs, pStartInfo->readyTs,
            pStartInfo->elapsedTime / 1000.0);

    // print the initialization elapsed time and info
    displayStatusInfo(pMeta, pStartInfo->pReadyTaskSet, true);
    displayStatusInfo(pMeta, pStartInfo->pFailedTaskSet, false);
    streamMetaResetStartInfo(pStartInfo);
  } else {
    stDebug("vgId:%d recv check down results:%d, total:%d", pMeta->vgId, numOfRecv, numOfTotal);
  }

  streamMetaWUnLock(pMeta);
  return TSDB_CODE_SUCCESS;
}
