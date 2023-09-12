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

#include <tstream.h>
#include "streamInt.h"
#include "trpc.h"
#include "ttimer.h"
#include "wal.h"

typedef struct SStreamTaskRetryInfo {
  SStreamMeta* pMeta;
  int32_t      taskId;
  int64_t      streamId;
} SStreamTaskRetryInfo;

static int32_t streamSetParamForScanHistory(SStreamTask* pTask);
static void    streamTaskSetRangeStreamCalc(SStreamTask* pTask);
static int32_t initScanHistoryReq(SStreamTask* pTask, SStreamScanHistoryReq* pReq, int8_t igUntreated);

static void streamTaskSetReady(SStreamTask* pTask, int32_t numOfReqs) {
  if (pTask->status.taskStatus == TASK_STATUS__SCAN_HISTORY && pTask->info.taskLevel != TASK_LEVEL__SOURCE) {
    pTask->numOfWaitingUpstream = taosArrayGetSize(pTask->pUpstreamInfoList);
    qDebug("s-task:%s level:%d task wait for %d upstream tasks complete scan-history procedure, status:%s",
           pTask->id.idStr, pTask->info.taskLevel, pTask->numOfWaitingUpstream,
           streamGetTaskStatusStr(pTask->status.taskStatus));
  }

  ASSERT(pTask->status.downstreamReady == 0);
  pTask->status.downstreamReady = 1;

  int64_t el = (taosGetTimestampMs() - pTask->tsInfo.init);
  qDebug("s-task:%s all %d downstream ready, init completed, elapsed time:%"PRId64"ms, task status:%s",
         pTask->id.idStr, numOfReqs, el, streamGetTaskStatusStr(pTask->status.taskStatus));
}

int32_t streamStartScanHistoryAsync(SStreamTask* pTask, int8_t igUntreated) {
  SStreamScanHistoryReq req;
  initScanHistoryReq(pTask, &req, igUntreated);

  int32_t len = sizeof(SStreamScanHistoryReq);
  void* serializedReq = rpcMallocCont(len);
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

const char* streamGetTaskStatusStr(int32_t status) {
  switch(status) {
    case TASK_STATUS__NORMAL: return "normal";
    case TASK_STATUS__SCAN_HISTORY: return "scan-history";
    case TASK_STATUS__HALT: return "halt";
    case TASK_STATUS__PAUSE: return "paused";
    case TASK_STATUS__CK: return "check-point";
    case TASK_STATUS__DROPPING: return "dropping";
    case TASK_STATUS__STOP: return "stop";
    default:return "";
  }
}

static int32_t doLaunchScanHistoryTask(SStreamTask* pTask) {
  SVersionRange* pRange = &pTask->dataRange.range;
  if (pTask->info.fillHistory) {
    streamSetParamForScanHistory(pTask);
  }

  streamSetParamForStreamScannerStep1(pTask, pRange, &pTask->dataRange.window);
  int32_t code = streamStartScanHistoryAsync(pTask, 0);
  return code;
}

int32_t streamTaskLaunchScanHistory(SStreamTask* pTask) {
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    if (pTask->status.taskStatus == TASK_STATUS__SCAN_HISTORY) {
      return doLaunchScanHistoryTask(pTask);
    } else {
      ASSERT(pTask->status.taskStatus == TASK_STATUS__NORMAL);
      qDebug("s-task:%s no need to scan-history-data, status:%s, sched-status:%d, ver:%" PRId64, pTask->id.idStr,
             streamGetTaskStatusStr(pTask->status.taskStatus), pTask->status.schedStatus,
             walReaderGetCurrentVer(pTask->exec.pWalReader));
    }
  } else if (pTask->info.taskLevel == TASK_LEVEL__AGG) {
    if (pTask->info.fillHistory) {
      streamSetParamForScanHistory(pTask);
      streamTaskEnablePause(pTask);
    }
  } else if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    qDebug("s-task:%s sink task do nothing to handle scan-history", pTask->id.idStr);
  }
  return 0;
}

// check status
static int32_t doCheckDownstreamStatus(SStreamTask* pTask) {
  SDataRange* pRange = &pTask->dataRange;
  STimeWindow*    pWindow = &pRange->window;

  SStreamTaskCheckReq req = {
      .streamId = pTask->id.streamId,
      .upstreamTaskId = pTask->id.taskId,
      .upstreamNodeId = pTask->info.nodeId,
      .childId = pTask->info.selfChildId,
      .stage = pTask->pMeta->stage,
  };

  // serialize streamProcessScanHistoryFinishRsp
  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    req.reqId = tGenIdPI64();
    req.downstreamNodeId = pTask->fixedEpDispatcher.nodeId;
    req.downstreamTaskId = pTask->fixedEpDispatcher.taskId;
    pTask->checkReqId = req.reqId;

    qDebug("s-task:%s check single downstream task:0x%x(vgId:%d) ver:%" PRId64 "-%" PRId64 " window:%" PRId64
           "-%" PRId64 ", stage:%"PRId64" req:0x%" PRIx64,
           pTask->id.idStr, req.downstreamTaskId, req.downstreamNodeId, pRange->range.minVer, pRange->range.maxVer,
           pWindow->skey, pWindow->ekey, req.stage, req.reqId);

    streamDispatchCheckMsg(pTask, &req, pTask->fixedEpDispatcher.nodeId, &pTask->fixedEpDispatcher.epSet);
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgs = taosArrayGetSize(vgInfo);
    pTask->notReadyTasks = numOfVgs;
    pTask->checkReqIds = taosArrayInit(numOfVgs, sizeof(int64_t));

    qDebug("s-task:%s check %d downstream tasks, ver:%" PRId64 "-%" PRId64 " window:%" PRId64 "-%" PRId64,
           pTask->id.idStr, numOfVgs, pRange->range.minVer, pRange->range.maxVer, pWindow->skey, pWindow->ekey);

    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      req.reqId = tGenIdPI64();
      taosArrayPush(pTask->checkReqIds, &req.reqId);
      req.downstreamNodeId = pVgInfo->vgId;
      req.downstreamTaskId = pVgInfo->taskId;
      qDebug("s-task:%s (vgId:%d) check downstream task:0x%x (vgId:%d) (shuffle), idx:%d, stage:%" PRId64,
             pTask->id.idStr, pTask->info.nodeId, req.downstreamTaskId, req.downstreamNodeId, i, req.stage);
      streamDispatchCheckMsg(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
    }
  } else {
    qDebug("s-task:%s (vgId:%d) set downstream ready, since no downstream", pTask->id.idStr, pTask->info.nodeId);

    streamTaskSetReady(pTask, 0);
    streamTaskSetRangeStreamCalc(pTask);
    streamTaskLaunchScanHistory(pTask);
    streamLaunchFillHistoryTask(pTask);
  }

  return 0;
}

int32_t streamRecheckDownstream(SStreamTask* pTask, const SStreamTaskCheckRsp* pRsp) {
  SStreamTaskCheckReq req = {
      .reqId = pRsp->reqId,
      .streamId = pRsp->streamId,
      .upstreamTaskId = pRsp->upstreamTaskId,
      .upstreamNodeId = pRsp->upstreamNodeId,
      .downstreamTaskId = pRsp->downstreamTaskId,
      .downstreamNodeId = pRsp->downstreamNodeId,
      .childId = pRsp->childId,
      .stage = pTask->pMeta->stage,
  };

  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    qDebug("s-task:%s (vgId:%d) check downstream task:0x%x (vgId:%d) stage:%" PRId64 " (recheck)", pTask->id.idStr,
           pTask->info.nodeId, req.downstreamTaskId, req.downstreamNodeId, req.stage);
    streamDispatchCheckMsg(pTask, &req, pRsp->downstreamNodeId, &pTask->fixedEpDispatcher.epSet);
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgs = taosArrayGetSize(vgInfo);
    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      if (pVgInfo->taskId == req.downstreamTaskId) {
        qDebug("s-task:%s (vgId:%d) check downstream task:0x%x (vgId:%d) stage:%" PRId64 " (recheck)", pTask->id.idStr,
               pTask->info.nodeId, req.downstreamTaskId, req.downstreamNodeId, req.stage);
        streamDispatchCheckMsg(pTask, &req, pRsp->downstreamNodeId, &pVgInfo->epSet);
      }
    }
  }

  return 0;
}

int32_t streamTaskCheckStatus(SStreamTask* pTask, int32_t upstreamTaskId, int32_t vgId, int64_t stage) {
  SStreamChildEpInfo* pInfo = streamTaskGetUpstreamTaskEpInfo(pTask, upstreamTaskId);
  ASSERT(pInfo != NULL);

  if (stage == -1) {
    qDebug("s-task:%s receive check msg from upstream task:0x%x, invalid stageId:%" PRId64 ", not ready", pTask->id.idStr,
           upstreamTaskId, stage);
    return 0;
  }

  if (pInfo->stage == -1) {
    pInfo->stage = stage;
    qDebug("s-task:%s receive check msg from upstream task:0x%x, init stage value:%" PRId64, pTask->id.idStr,
           upstreamTaskId, stage);
  }

  if (pInfo->stage < stage) {
    qError("s-task:%s receive msg from upstream task:0x%x(vgId:%d), new stage received:%" PRId64 ", prev:%" PRId64,
           pTask->id.idStr, upstreamTaskId, vgId, stage, pInfo->stage);
  }

  return ((pTask->status.downstreamReady == 1) && (pInfo->stage == stage))? 1:0;
}

static void doProcessDownstreamReadyRsp(SStreamTask* pTask, int32_t numOfReqs) {
  streamTaskSetReady(pTask, numOfReqs);
  const char* id = pTask->id.idStr;

  int8_t      status = pTask->status.taskStatus;
  const char* str = streamGetTaskStatusStr(status);

  ASSERT(status == TASK_STATUS__SCAN_HISTORY || status == TASK_STATUS__NORMAL);
  streamTaskSetRangeStreamCalc(pTask);

  if (status == TASK_STATUS__SCAN_HISTORY) {
    qDebug("s-task:%s enter into scan-history data stage, status:%s", id, str);
    streamTaskLaunchScanHistory(pTask);
  } else {
    qDebug("s-task:%s downstream tasks are ready, now ready for data from wal, status:%s", id, str);
  }

  // when current stream task is ready, check the related fill history task.
  streamLaunchFillHistoryTask(pTask);
}

// todo handle error
int32_t streamProcessCheckRsp(SStreamTask* pTask, const SStreamTaskCheckRsp* pRsp) {
  ASSERT(pTask->id.taskId == pRsp->upstreamTaskId);
  const char* id = pTask->id.idStr;

  if (pRsp->status == 1) {
    if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      bool found = false;

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

        doProcessDownstreamReadyRsp(pTask, numOfReqs);
      } else {
        int32_t total = taosArrayGetSize(pTask->shuffleDispatcher.dbInfo.pVgroupInfos);
        qDebug("s-task:%s (vgId:%d) recv check rsp from task:0x%x (vgId:%d) status:%d, total:%d not ready:%d", id,
               pRsp->upstreamNodeId, pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->status, total, left);
      }
    } else {
      ASSERT(pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH);
      if (pRsp->reqId != pTask->checkReqId) {
        return -1;
      }

      doProcessDownstreamReadyRsp(pTask, 1);
    }
  } else {  // not ready, wait for 100ms and retry
    qDebug("s-task:%s downstream taskId:0x%x (vgId:%d) not ready, stage:%d, wait for 100ms and retry", id,
           pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->oldStage);
    taosMsleep(100);
    streamRecheckDownstream(pTask, pRsp);
  }

  return 0;
}

int32_t streamSendCheckRsp(const SStreamMeta* pMeta, const SStreamTaskCheckReq* pReq, SStreamTaskCheckRsp* pRsp,
                           SRpcHandleInfo *pRpcInfo, int32_t taskId) {
  SEncoder encoder;
  int32_t  code;
  int32_t  len;

  tEncodeSize(tEncodeStreamTaskCheckRsp, pRsp, len, code);
  if (code < 0) {
    qError("vgId:%d failed to encode task check rsp, s-task:0x%x", pMeta->vgId, taskId);
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
  qDebug("s-task:%s set operator option for scan-history data", pTask->id.idStr);
  return qSetStreamOperatorOptionForScanHistory(pTask->exec.pExecutor);
}

int32_t streamRestoreParam(SStreamTask* pTask) {
  qDebug("s-task:%s restore operator param after scan-history", pTask->id.idStr);
  return qRestoreStreamOperatorOption(pTask->exec.pExecutor);
}

int32_t streamSetStatusNormal(SStreamTask* pTask) {
  int32_t status = atomic_load_8(&pTask->status.taskStatus);
  if (status == TASK_STATUS__DROPPING) {
    qError("s-task:%s cannot be set normal, since in dropping state", pTask->id.idStr);
    return -1;
  } else {
    qDebug("s-task:%s set task status to be normal, prev:%s", pTask->id.idStr, streamGetTaskStatusStr(status));
    atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__NORMAL);
    return 0;
  }
}

// source
int32_t streamSetParamForStreamScannerStep1(SStreamTask* pTask, SVersionRange *pVerRange, STimeWindow* pWindow) {
  return qStreamSourceScanParamForHistoryScanStep1(pTask->exec.pExecutor, pVerRange, pWindow);
}

int32_t streamSetParamForStreamScannerStep2(SStreamTask* pTask, SVersionRange *pVerRange, STimeWindow* pWindow) {
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

  pTranstate->blocks = taosArrayInit(4, sizeof(SSDataBlock));//pBlock;
  taosArrayPush(pTranstate->blocks, pBlock);

  taosMemoryFree(pBlock);
  if (streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pTranstate) < 0) {
    taosFreeQitem(pTranstate);
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

  if (pTask->status.taskStatus != TASK_STATUS__SCAN_HISTORY) {
    qError("s-task:%s not in scan-history status, status:%s return upstream:0x%x scan-history finish directly",
           pTask->id.idStr, streamGetTaskStatusStr(pTask->status.taskStatus), pReq->upstreamTaskId);

    void*   pBuf = NULL;
    int32_t len = 0;
    streamTaskBuildScanhistoryRspMsg(pTask, pReq, &pBuf, &len);

    SRpcMsg msg = {.info = *pRpcInfo};
    initRpcMsg(&msg, 0, pBuf, sizeof(SMsgHead) + len);

    tmsgSendRsp(&msg);
    qDebug("s-task:%s level:%d notify upstream:0x%x(vgId:%d) to continue process data in WAL", pTask->id.idStr,
           pTask->info.taskLevel, pReq->upstreamTaskId, pReq->upstreamNodeId);
    return 0;
  }

  // sink tasks do not send end of scan history msg to its upstream, which is agg task.
  streamAddEndScanHistoryMsg(pTask, pRpcInfo, pReq);

  int32_t left = atomic_sub_fetch_32(&pTask->numOfWaitingUpstream, 1);
  ASSERT(left >= 0);

  if (left == 0) {
    int32_t numOfTasks = taosArrayGetSize(pTask->pUpstreamInfoList);
    qDebug(
        "s-task:%s all %d upstream tasks finish scan-history data, set param for agg task for stream data and send "
        "rsp to all upstream tasks",
        pTask->id.idStr, numOfTasks);

    if (pTask->info.taskLevel == TASK_LEVEL__AGG) {
      streamAggUpstreamScanHistoryFinish(pTask);
    }

    // all upstream tasks have completed the scan-history task in the stream time window, let's start to extract data
    // from the WAL files, which contains the real time stream data.
    streamNotifyUpstreamContinue(pTask);

    // mnode will not send the pause/resume message to the sink task, so no need to enable the pause for sink tasks.
    if (taskLevel == TASK_LEVEL__AGG) {
      /*int32_t code = */streamTaskScanHistoryDataComplete(pTask);
    } else {  // for sink task, set normal
      if (pTask->status.taskStatus != TASK_STATUS__PAUSE && pTask->status.taskStatus != TASK_STATUS__STOP &&
          pTask->status.taskStatus != TASK_STATUS__DROPPING) {
        streamSetStatusNormal(pTask);
      }
    }
  } else {
    qDebug("s-task:%s receive scan-history data finish msg from upstream:0x%x(index:%d), unfinished:%d",
           pTask->id.idStr, pReq->upstreamTaskId, pReq->childId, left);
  }

  return 0;
}

int32_t streamProcessScanHistoryFinishRsp(SStreamTask* pTask) {
  ASSERT(pTask->status.taskStatus == TASK_STATUS__SCAN_HISTORY);
  SStreamMeta* pMeta = pTask->pMeta;

  // execute in the scan history complete call back msg, ready to process data from inputQ
  streamSetStatusNormal(pTask);
  atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__INACTIVE);

  taosWLockLatch(&pMeta->lock);
  streamMetaSaveTask(pMeta, pTask);
  streamMetaCommit(pMeta);
  taosWUnLockLatch(&pMeta->lock);

  // history data scan in the stream time window finished, now let's enable the pause
  streamTaskEnablePause(pTask);

  // for source tasks, let's continue execute.
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    streamSchedExec(pTask);
  }

  return TSDB_CODE_SUCCESS;
}

static void checkFillhistoryTaskStatus(SStreamTask* pTask, SStreamTask* pHTask) {
  pHTask->dataRange.range.minVer = 0;
  // the query version range should be limited to the already processed data
  pHTask->dataRange.range.maxVer = pTask->chkInfo.nextProcessVer - 1;

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    qDebug("s-task:%s set the launch condition for fill-history s-task:%s, window:%" PRId64 " - %" PRId64
           " ver range:%" PRId64 " - %" PRId64,
           pTask->id.idStr, pHTask->id.idStr, pHTask->dataRange.window.skey, pHTask->dataRange.window.ekey,
           pHTask->dataRange.range.minVer, pHTask->dataRange.range.maxVer);
  } else {
    qDebug("s-task:%s no fill history condition for non-source task:%s", pTask->id.idStr, pHTask->id.idStr);
  }

  // check if downstream tasks have been ready
  doCheckDownstreamStatus(pHTask);
}

static void tryLaunchHistoryTask(void* param, void* tmrId) {
  SStreamTaskRetryInfo* pInfo = param;
  SStreamMeta*          pMeta = pInfo->pMeta;

  qDebug("s-task:0x%x in timer to launch related history task", pInfo->taskId);

  taosWLockLatch(&pMeta->lock);
  int64_t keys[2] = {pInfo->streamId, pInfo->taskId};

  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasks, keys, sizeof(keys));
  if (ppTask) {
    ASSERT((*ppTask)->status.timerActive >= 1);

    if (streamTaskShouldStop(&(*ppTask)->status)) {
      const char* pStatus = streamGetTaskStatusStr((*ppTask)->status.taskStatus);
      qDebug("s-task:%s status:%s quit timer task", (*ppTask)->id.idStr, pStatus);

      taosMemoryFree(pInfo);
      atomic_sub_fetch_8(&(*ppTask)->status.timerActive, 1);
      taosWUnLockLatch(&pMeta->lock);
      return;
    }
  }
  taosWUnLockLatch(&pMeta->lock);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pInfo->streamId, pInfo->taskId);
  if (pTask != NULL) {
    ASSERT(pTask->status.timerActive >= 1);

    // abort the timer if intend to stop task
    SStreamTask* pHTask = streamMetaAcquireTask(pMeta, pTask->historyTaskId.streamId, pTask->historyTaskId.taskId);
    if (pHTask == NULL && (!streamTaskShouldStop(&pTask->status))) {
      const char* pStatus = streamGetTaskStatusStr(pTask->status.taskStatus);
      qWarn(
          "s-task:%s vgId:%d status:%s failed to launch history task:0x%x, since it may not be built, or may have been "
          "destroyed, or should stop",
          pTask->id.idStr, pMeta->vgId, pStatus, pTask->historyTaskId.taskId);

      taosTmrReset(tryLaunchHistoryTask, 100, pInfo, streamEnv.timer, &pTask->launchTaskTimer);
      streamMetaReleaseTask(pMeta, pTask);
      return;
    }

    if (pHTask != NULL) {
      checkFillhistoryTaskStatus(pTask, pHTask);
      streamMetaReleaseTask(pMeta, pHTask);
    }

    // not in timer anymore
    atomic_sub_fetch_8(&pTask->status.timerActive, 1);
    streamMetaReleaseTask(pMeta, pTask);
  } else {
    qError("s-task:0x%x failed to load task, it may have been destroyed", pInfo->taskId);
  }

  taosMemoryFree(pInfo);
}

// todo fix the bug: 2. race condition
// an fill history task needs to be started.
int32_t streamLaunchFillHistoryTask(SStreamTask* pTask) {
  int32_t tId = pTask->historyTaskId.taskId;
  if (tId == 0) {
    return TSDB_CODE_SUCCESS;
  }

  ASSERT(pTask->status.downstreamReady == 1);
  qDebug("s-task:%s start to launch related fill-history task:0x%" PRIx64 "-0x%x", pTask->id.idStr,
         pTask->historyTaskId.streamId, tId);

  SStreamMeta* pMeta = pTask->pMeta;
  int32_t      hTaskId = pTask->historyTaskId.taskId;

  int64_t keys[2] = {pTask->historyTaskId.streamId, pTask->historyTaskId.taskId};

  // Set the execute conditions, including the query time window and the version range
  SStreamTask** pHTask = taosHashGet(pMeta->pTasks, keys, sizeof(keys));
  if (pHTask == NULL) {
    qWarn("s-task:%s vgId:%d failed to launch history task:0x%x, since it is not built yet", pTask->id.idStr,
          pMeta->vgId, hTaskId);

    SStreamTaskRetryInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamTaskRetryInfo));
    pInfo->taskId = pTask->id.taskId;
    pInfo->streamId = pTask->id.streamId;
    pInfo->pMeta = pTask->pMeta;

    if (pTask->launchTaskTimer == NULL) {
      pTask->launchTaskTimer = taosTmrStart(tryLaunchHistoryTask,  100, pInfo, streamEnv.timer);
      if (pTask->launchTaskTimer == NULL) {
        // todo failed to create timer
        taosMemoryFree(pInfo);
      } else {
        atomic_add_fetch_8(&pTask->status.timerActive, 1);// timer is active
        qDebug("s-task:%s set timer active flag", pTask->id.idStr);
      }
    } else {  // timer exists
      ASSERT(pTask->status.timerActive > 0);
      qDebug("s-task:%s set timer active flag, task timer not null", pTask->id.idStr);
      taosTmrReset(tryLaunchHistoryTask, 100, pInfo, streamEnv.timer, &pTask->launchTaskTimer);
    }

    // try again in 100ms
    return TSDB_CODE_SUCCESS;
  }

  if ((*pHTask)->status.downstreamReady == 1) {
    qDebug("s-task:%s fill-history task is ready, no need to check downstream", (*pHTask)->id.idStr);
  } else {
    checkFillhistoryTaskStatus(pTask, *pHTask);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskScanHistoryDataComplete(SStreamTask* pTask) {
  if (atomic_load_8(&pTask->status.taskStatus) == TASK_STATUS__DROPPING) {
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

bool streamHistoryTaskSetVerRangeStep2(SStreamTask* pTask, int64_t latestVer) {
  SVersionRange* pRange = &pTask->dataRange.range;
  ASSERT(latestVer >= pRange->maxVer);

  int64_t nextStartVer = pRange->maxVer + 1;
  if (nextStartVer > latestVer - 1) {
    // no input data yet. no need to execute the secondardy scan while stream task halt
    streamTaskFillHistoryFinished(pTask);
    qDebug(
        "s-task:%s no need to perform secondary scan-history data(step 2), since no data ingest during step1 scan, "
        "related stream task currentVer:%" PRId64,
        pTask->id.idStr, latestVer);
    return true;
  } else {
    // 2. do secondary scan of the history data, the time window remain, and the version range is updated to
    // [pTask->dataRange.range.maxVer, ver1]
    pRange->minVer = nextStartVer;
    pRange->maxVer = latestVer - 1;
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
  if (tEncodeI32(pEncoder, pRsp->oldStage) < 0) return -1;
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
  if (tDecodeI32(pDecoder, &pRsp->oldStage) < 0) return -1;
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
  if (pTask->historyTaskId.taskId == 0) {
    SDataRange* pRange = &pTask->dataRange;
    if (pTask->info.fillHistory == 1) {
      qDebug("s-task:%s fill-history task, time window:%" PRId64 "-%" PRId64 ", verRange:%" PRId64
                 "-%" PRId64,
             pTask->id.idStr, pRange->window.skey, pRange->window.ekey, pRange->range.minVer, pRange->range.maxVer);
    } else {
      qDebug("s-task:%s no related fill-history task, stream time window:%" PRId64 "-%" PRId64 ", verRange:%" PRId64
             "-%" PRId64,
             pTask->id.idStr, pRange->window.skey, pRange->window.ekey, pRange->range.minVer, pRange->range.maxVer);
    }
  } else {
    SDataRange* pRange = &pTask->dataRange;

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

    qDebug("s-task:%s level:%d related fill-history task exists, update stream calc time window:%" PRId64 " - %" PRId64
           ", verRang:%" PRId64 " - %" PRId64,
           pTask->id.idStr, pTask->info.taskLevel, pRange->window.skey, pRange->window.ekey, pRange->range.minVer,
           pRange->range.maxVer);
  }
}

// only the downstream tasks are ready, set the task to be ready to work.
void streamTaskCheckDownstream(SStreamTask* pTask) {
  if (pTask->info.fillHistory) {
    qDebug("s-task:%s fill history task, wait for being launched", pTask->id.idStr);
    return;
  }

  ASSERT(pTask->status.downstreamReady == 0);
  doCheckDownstreamStatus(pTask);
}

// normal -> pause, pause/stop/dropping -> pause, halt -> pause, scan-history -> pause
void streamTaskPause(SStreamTask* pTask, SStreamMeta* pMeta) {
  int64_t st = taosGetTimestampMs();

  int8_t status = pTask->status.taskStatus;
  if (status == TASK_STATUS__DROPPING) {
    qDebug("vgId:%d s-task:%s task already dropped, do nothing", pMeta->vgId, pTask->id.idStr);
    return;
  }

  const char* str = streamGetTaskStatusStr(status);
  if (status == TASK_STATUS__STOP || status == TASK_STATUS__PAUSE) {
    qDebug("vgId:%d s-task:%s task already stopped/paused, status:%s, do nothing", pMeta->vgId, pTask->id.idStr, str);
    return;
  }

  if(pTask->info.taskLevel == TASK_LEVEL__SINK) {
    int32_t num = atomic_add_fetch_32(&pMeta->pauseTaskNum, 1);
    qInfo("vgId:%d s-task:%s pause stream sink task. pause task num:%d", pMeta->vgId, pTask->id.idStr, num);
    return;
  }

  while (!pTask->status.pauseAllowed || (pTask->status.taskStatus == TASK_STATUS__HALT)) {
    status = pTask->status.taskStatus;
    if (status == TASK_STATUS__DROPPING) {
      qDebug("vgId:%d s-task:%s task already dropped, do nothing", pMeta->vgId, pTask->id.idStr);
      return;
    }

    if (status == TASK_STATUS__STOP || status == TASK_STATUS__PAUSE) {
      qDebug("vgId:%d s-task:%s task already stopped/paused, status:%s, do nothing", pMeta->vgId, pTask->id.idStr, str);
      return;
    }

    const char* pStatus = streamGetTaskStatusStr(status);
    qDebug("s-task:%s wait for the task can be paused, status:%s, vgId:%d", pTask->id.idStr, pStatus, pMeta->vgId);
    taosMsleep(100);
  }

  // todo: use the task lock, stead of meta lock
  taosWLockLatch(&pMeta->lock);

  status = pTask->status.taskStatus;
  if (status == TASK_STATUS__DROPPING || status == TASK_STATUS__STOP) {
    taosWUnLockLatch(&pMeta->lock);
    qDebug("vgId:%d s-task:%s task already dropped/stopped/paused, do nothing", pMeta->vgId, pTask->id.idStr);
    return;
  }

  atomic_store_8(&pTask->status.keepTaskStatus, pTask->status.taskStatus);
  atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__PAUSE);
  int32_t num = atomic_add_fetch_32(&pMeta->pauseTaskNum, 1);
  qInfo("vgId:%d s-task:%s pause stream task. pause task num:%d", pMeta->vgId, pTask->id.idStr, num);
  taosWUnLockLatch(&pMeta->lock);

  // in case of fill-history task, stop the tsdb file scan operation.
  if (pTask->info.fillHistory == 1) {
    void* pExecutor = pTask->exec.pExecutor;
    qKillTask(pExecutor, TSDB_CODE_SUCCESS);
  }

  int64_t el = taosGetTimestampMs() - st;
  qDebug("vgId:%d s-task:%s set pause flag, prev:%s, pause elapsed time:%dms", pMeta->vgId, pTask->id.idStr,
         streamGetTaskStatusStr(pTask->status.keepTaskStatus), (int32_t)el);
}

void streamTaskResume(SStreamTask* pTask, SStreamMeta* pMeta) {
  int8_t status = pTask->status.taskStatus;
  if (status == TASK_STATUS__PAUSE) {
    pTask->status.taskStatus = pTask->status.keepTaskStatus;
    pTask->status.keepTaskStatus = TASK_STATUS__NORMAL;
    int32_t num = atomic_sub_fetch_32(&pMeta->pauseTaskNum, 1);
    qInfo("vgId:%d s-task:%s resume from pause, status:%s. pause task num:%d", pMeta->vgId, pTask->id.idStr, streamGetTaskStatusStr(status), num);
  } else if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    int32_t num = atomic_sub_fetch_32(&pMeta->pauseTaskNum, 1);
    qInfo("vgId:%d s-task:%s sink task.resume from pause, status:%s. pause task num:%d", pMeta->vgId, pTask->id.idStr, streamGetTaskStatusStr(status), num);
  } else {
    qError("s-task:%s not in pause, failed to resume, status:%s", pTask->id.idStr, streamGetTaskStatusStr(status));
  }
}

// todo fix race condition
void streamTaskDisablePause(SStreamTask* pTask) {
  // pre-condition check
  const char* id = pTask->id.idStr;
  while (pTask->status.taskStatus == TASK_STATUS__PAUSE) {
    qDebug("s-task:%s already in pause, wait for pause being cancelled, and set pause disabled, recheck in 100ms", id);
    taosMsleep(100);
  }

  qDebug("s-task:%s disable task pause", id);
  pTask->status.pauseAllowed = 0;
}

void streamTaskEnablePause(SStreamTask* pTask) {
  qDebug("s-task:%s enable task pause", pTask->id.idStr);
  pTask->status.pauseAllowed = 1;
}

void streamTaskHalt(SStreamTask* pTask) {
  int8_t status = pTask->status.taskStatus;
  if (status == TASK_STATUS__DROPPING || status == TASK_STATUS__STOP) {
    return;
  }

  if (status == TASK_STATUS__HALT) {
    return;
  }

  // upgrade to halt status
  if (status == TASK_STATUS__PAUSE) {
    qDebug("s-task:%s upgrade status to %s from %s", pTask->id.idStr, streamGetTaskStatusStr(TASK_STATUS__HALT),
           streamGetTaskStatusStr(TASK_STATUS__PAUSE));
  } else {
    qDebug("s-task:%s halt task", pTask->id.idStr);
  }

  pTask->status.keepTaskStatus = status;
  pTask->status.taskStatus = TASK_STATUS__HALT;
}

void streamTaskResumeFromHalt(SStreamTask* pTask) {
  const char* id = pTask->id.idStr;
  int8_t status = pTask->status.taskStatus;
  if (status != TASK_STATUS__HALT) {
    qError("s-task:%s not in halt status, status:%s", id, streamGetTaskStatusStr(status));
    return;
  }

  pTask->status.taskStatus = pTask->status.keepTaskStatus;
  pTask->status.keepTaskStatus = TASK_STATUS__NORMAL;
  qDebug("s-task:%s resume from halt, current status:%s", id, streamGetTaskStatusStr(pTask->status.taskStatus));
}
