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
#include "ttimer.h"
#include "wal.h"

static void launchFillHistoryTask(SStreamTask* pTask);
static void streamTaskSetRangeStreamCalc(SStreamTask* pTask);

static void streamTaskSetForReady(SStreamTask* pTask, int32_t numOfReqs) {
  ASSERT(pTask->status.downstreamReady == 0);
  pTask->status.downstreamReady = 1;
  int64_t el = (taosGetTimestampMs() - pTask->initTs);

  qDebug("s-task:%s all %d downstream ready, init completed, elapsed time:%dms, task status:%s",
         pTask->id.idStr, numOfReqs, (int32_t) el, streamGetTaskStatusStr(pTask->status.taskStatus));
}

int32_t streamStartRecoverTask(SStreamTask* pTask, int8_t igUntreated) {
  SStreamScanHistoryReq req;
  streamBuildSourceRecover1Req(pTask, &req, igUntreated);
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
    case TASK_STATUS__DROPPING: return "dropping";
    default:return "";
  }
}

static int32_t doLaunchScanHistoryTask(SStreamTask* pTask) {
  SVersionRange* pRange = &pTask->dataRange.range;
  streamSetParamForScanHistory(pTask);
  streamSetParamForStreamScannerStep1(pTask, pRange, &pTask->dataRange.window);

  int32_t code = streamStartRecoverTask(pTask, 0);
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
    streamSetParamForScanHistory(pTask);
    streamTaskScanHistoryPrepare(pTask);
  } else if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    qDebug("s-task:%s sink task do nothing to handle scan-history", pTask->id.idStr);
    streamTaskScanHistoryPrepare(pTask);
  }
  return 0;
}

// check status
int32_t streamTaskDoCheckDownstreamTasks(SStreamTask* pTask) {
  SHistDataRange* pRange = &pTask->dataRange;
  STimeWindow*    pWindow = &pRange->window;

  SStreamTaskCheckReq req = {
      .streamId = pTask->id.streamId,
      .upstreamTaskId = pTask->id.taskId,
      .upstreamNodeId = pTask->info.nodeId,
      .childId = pTask->info.selfChildId,
  };

  // serialize
  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    req.reqId = tGenIdPI64();
    req.downstreamNodeId = pTask->fixedEpDispatcher.nodeId;
    req.downstreamTaskId = pTask->fixedEpDispatcher.taskId;
    pTask->checkReqId = req.reqId;

    qDebug("s-task:%s check single downstream task:0x%x(vgId:%d) ver:%" PRId64 "-%" PRId64 " window:%" PRId64
           "-%" PRId64 ", req:0x%" PRIx64,
           pTask->id.idStr, req.downstreamTaskId, req.downstreamNodeId, pRange->range.minVer, pRange->range.maxVer,
           pWindow->skey, pWindow->ekey, req.reqId);

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
      qDebug("s-task:%s (vgId:%d) check downstream task:0x%x (vgId:%d) (shuffle), idx:%d", pTask->id.idStr, pTask->info.nodeId,
             req.downstreamTaskId, req.downstreamNodeId, i);
      streamDispatchCheckMsg(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
    }
  } else {
    qDebug("s-task:%s (vgId:%d) set downstream ready, since no downstream", pTask->id.idStr, pTask->info.nodeId);

    streamTaskSetForReady(pTask, 0);
    streamTaskSetRangeStreamCalc(pTask);
    streamTaskLaunchScanHistory(pTask);

    // enable pause when init completed.
    if (pTask->historyTaskId.taskId == 0) {
      streamTaskEnablePause(pTask);
    }

    launchFillHistoryTask(pTask);
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
  };

  qDebug("s-task:%s (vgId:%d) check downstream task:0x%x (vgId:%d) (recheck)", pTask->id.idStr, pTask->info.nodeId,
         req.downstreamTaskId, req.downstreamNodeId);

  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    streamDispatchCheckMsg(pTask, &req, pRsp->downstreamNodeId, &pTask->fixedEpDispatcher.epSet);
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgs = taosArrayGetSize(vgInfo);
    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      if (pVgInfo->taskId == req.downstreamTaskId) {
        streamDispatchCheckMsg(pTask, &req, pRsp->downstreamNodeId, &pVgInfo->epSet);
      }
    }
  }

  return 0;
}

int32_t streamTaskCheckStatus(SStreamTask* pTask) {
  return (pTask->status.downstreamReady == 1)? 1:0;
}

static void doProcessDownstreamReadyRsp(SStreamTask* pTask, int32_t numOfReqs) {
  streamTaskSetForReady(pTask, numOfReqs);
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
  launchFillHistoryTask(pTask);
}

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
    qDebug("s-task:%s downstream taskId:0x%x (vgId:%d) not ready, wait for 100ms and retry", id, pRsp->downstreamTaskId,
           pRsp->downstreamNodeId);
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
  qDebug("s-task:%s set operator option for scan-history-data", pTask->id.idStr);
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

int32_t streamBuildSourceRecover1Req(SStreamTask* pTask, SStreamScanHistoryReq* pReq, int8_t igUntreated) {
  pReq->msgHead.vgId = pTask->info.nodeId;
  pReq->streamId = pTask->id.streamId;
  pReq->taskId = pTask->id.taskId;
  pReq->igUntreated = igUntreated;
  return 0;
}

int32_t streamSourceScanHistoryData(SStreamTask* pTask) {
  return streamScanExec(pTask, 100);
}

int32_t streamDispatchScanHistoryFinishMsg(SStreamTask* pTask) {
  SStreamScanHistoryFinishReq req = {
      .streamId = pTask->id.streamId,
      .childId = pTask->info.selfChildId,
      .upstreamTaskId = pTask->id.taskId,
      .upstreamNodeId = pTask->pMeta->vgId,
  };

  // serialize
  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    req.downstreamTaskId = pTask->fixedEpDispatcher.taskId;
    pTask->notReadyTasks = 1;
    streamDoDispatchScanHistoryFinishMsg(pTask, &req, pTask->fixedEpDispatcher.nodeId, &pTask->fixedEpDispatcher.epSet);
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t numOfVgs = taosArrayGetSize(vgInfo);
    pTask->notReadyTasks = numOfVgs;

    qDebug("s-task:%s send scan-history-data complete msg to downstream (shuffle-dispatch) %d tasks, status:%s", pTask->id.idStr,
           numOfVgs, streamGetTaskStatusStr(pTask->status.taskStatus));
    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      req.downstreamTaskId = pVgInfo->taskId;
      streamDoDispatchScanHistoryFinishMsg(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
    }
  } else {
    qDebug("s-task:%s no downstream tasks, invoke history finish rsp directly", pTask->id.idStr);
    streamProcessScanHistoryFinishRsp(pTask);
  }

  return 0;
}

static int32_t doDispatchTransferMsg(SStreamTask* pTask, const SStreamTransferReq* pReq, int32_t vgId, SEpSet* pEpSet) {
  void*   buf = NULL;
  int32_t code = -1;
  SRpcMsg msg = {0};

  int32_t tlen;
  tEncodeSize(tEncodeStreamScanHistoryFinishReq, pReq, tlen, code);
  if (code < 0) {
    return -1;
  }

  buf = rpcMallocCont(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  ((SMsgHead*)buf)->vgId = htonl(vgId);
  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeStreamScanHistoryFinishReq(&encoder, pReq)) < 0) {
    if (buf) {
      rpcFreeCont(buf);
    }
    return code;
  }

  tEncoderClear(&encoder);

  msg.contLen = tlen + sizeof(SMsgHead);
  msg.pCont = buf;
  msg.msgType = TDMT_STREAM_TRANSFER_STATE;
  msg.info.noResp = 1;

  tmsgSendReq(pEpSet, &msg);
  qDebug("s-task:%s level:%d, status:%s dispatch transfer state msg to taskId:0x%x (vgId:%d)", pTask->id.idStr,
         pTask->info.taskLevel, streamGetTaskStatusStr(pTask->status.taskStatus), pReq->downstreamTaskId, vgId);

  return 0;
}

int32_t streamDispatchTransferStateMsg(SStreamTask* pTask) {
  SStreamTransferReq req = { .streamId = pTask->id.streamId, .childId = pTask->info.selfChildId };

  // serialize
  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    req.downstreamTaskId = pTask->fixedEpDispatcher.taskId;
    doDispatchTransferMsg(pTask, &req, pTask->fixedEpDispatcher.nodeId, &pTask->fixedEpDispatcher.epSet);
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgs = taosArrayGetSize(vgInfo);
    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      req.downstreamTaskId = pVgInfo->taskId;
      doDispatchTransferMsg(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
    }
  }

  return 0;
}

// agg
int32_t streamTaskScanHistoryPrepare(SStreamTask* pTask) {
  pTask->numOfWaitingUpstream = taosArrayGetSize(pTask->pUpstreamEpInfoList);
  qDebug("s-task:%s level:%d task wait for %d upstream tasks complete scan-history procedure, status:%s",
         pTask->id.idStr, pTask->info.taskLevel, pTask->numOfWaitingUpstream,
         streamGetTaskStatusStr(pTask->status.taskStatus));
  return 0;
}

int32_t streamAggUpstreamScanHistoryFinish(SStreamTask* pTask) {
  void* exec = pTask->exec.pExecutor;
  if (qRestoreStreamOperatorOption(exec) < 0) {
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

  // sink node do not send end of scan history msg to its upstream, which is agg task.
  streamAddEndScanHistoryMsg(pTask, pRpcInfo, pReq);

  int32_t left = atomic_sub_fetch_32(&pTask->numOfWaitingUpstream, 1);
  ASSERT(left >= 0);

  if (left == 0) {
    int32_t numOfTasks = taosArrayGetSize(pTask->pUpstreamEpInfoList);
    qDebug(
        "s-task:%s all %d upstream tasks finish scan-history data, set param for agg task for stream data and send "
        "rsp to all upstream tasks",
        pTask->id.idStr, numOfTasks);

    if (pTask->info.taskLevel == TASK_LEVEL__AGG) {
      streamAggUpstreamScanHistoryFinish(pTask);
    }

    streamNotifyUpstreamContinue(pTask);

    // sink node does not receive the pause msg from mnode
    if (pTask->info.taskLevel == TASK_LEVEL__AGG) {
      streamTaskEnablePause(pTask);
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
  taosWUnLockLatch(&pMeta->lock);

  streamTaskEnablePause(pTask);

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    streamSchedExec(pTask);
  }

  return TSDB_CODE_SUCCESS;
}

static void doCheckDownstreamStatus(SStreamTask* pTask, SStreamTask* pHTask) {
  pHTask->dataRange.range.minVer = 0;
  pHTask->dataRange.range.maxVer = pTask->chkInfo.currentVer;

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    qDebug("s-task:%s set the launch condition for fill history s-task:%s, window:%" PRId64 " - %" PRId64
           " ver range:%" PRId64 " - %" PRId64,
           pTask->id.idStr, pHTask->id.idStr, pHTask->dataRange.window.skey, pHTask->dataRange.window.ekey,
           pHTask->dataRange.range.minVer, pHTask->dataRange.range.maxVer);
  } else {
    qDebug("s-task:%s no fill history condition for non-source task:%s", pTask->id.idStr, pHTask->id.idStr);
  }

  // check if downstream tasks have been ready
  streamTaskDoCheckDownstreamTasks(pHTask);
}

typedef struct SStreamTaskRetryInfo {
  SStreamMeta* pMeta;
  int32_t taskId;
} SStreamTaskRetryInfo;

static void tryLaunchHistoryTask(void* param, void* tmrId) {
  SStreamTaskRetryInfo* pInfo = param;
  SStreamMeta*          pMeta = pInfo->pMeta;

  qDebug("s-task:0x%x in timer to launch related history task", pInfo->taskId);

  taosWLockLatch(&pMeta->lock);
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasks, &pInfo->taskId, sizeof(int32_t));
  if (ppTask) {
    ASSERT((*ppTask)->status.timerActive == 1);

    if (streamTaskShouldStop(&(*ppTask)->status)) {
      const char* pStatus = streamGetTaskStatusStr((*ppTask)->status.taskStatus);
      qDebug("s-task:%s status:%s quit timer task", (*ppTask)->id.idStr, pStatus);

      taosMemoryFree(pInfo);
      (*ppTask)->status.timerActive = 0;
      taosWUnLockLatch(&pMeta->lock);
      return;
    }
  }
  taosWUnLockLatch(&pMeta->lock);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pInfo->taskId);
  if (pTask != NULL) {
    ASSERT(pTask->status.timerActive == 1);

    // abort the timer if intend to stop task
    SStreamTask* pHTask = streamMetaAcquireTask(pMeta, pTask->historyTaskId.taskId);
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
      doCheckDownstreamStatus(pTask, pHTask);
      streamMetaReleaseTask(pMeta, pHTask);
    }

    // not in timer anymore
    pTask->status.timerActive = 0;
    streamMetaReleaseTask(pMeta, pTask);
  } else {
    qError("s-task:0x%x failed to load task, it may have been destroyed", pInfo->taskId);
  }

  taosMemoryFree(pInfo);
}

// todo fix the bug: 2. race condition
// an fill history task needs to be started.
int32_t streamLaunchFillHistoryTask(SStreamTask* pTask) {
  SStreamMeta* pMeta = pTask->pMeta;
  int32_t      hTaskId = pTask->historyTaskId.taskId;

  // Set the execute conditions, including the query time window and the version range
  SStreamTask** pHTask = taosHashGet(pMeta->pTasks, &hTaskId, sizeof(hTaskId));
  if (pHTask == NULL) {
    qWarn("s-task:%s vgId:%d failed to launch history task:0x%x, since it is not built yet", pTask->id.idStr,
          pMeta->vgId, hTaskId);

    SStreamTaskRetryInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamTaskRetryInfo));
    pInfo->taskId = pTask->id.taskId;
    pInfo->pMeta = pTask->pMeta;

    if (pTask->launchTaskTimer == NULL) {
      pTask->launchTaskTimer = taosTmrStart(tryLaunchHistoryTask,  100, pInfo, streamEnv.timer);
      if (pTask->launchTaskTimer == NULL) {
        // todo failed to create timer
        taosMemoryFree(pInfo);
      } else {
        pTask->status.timerActive = 1;  // timer is active
        qDebug("s-task:%s set timer active flag", pTask->id.idStr);
      }
    } else {  // timer exists
      pTask->status.timerActive = 1;
      qDebug("s-task:%s set timer active flag, task timer not null", pTask->id.idStr);
      taosTmrReset(tryLaunchHistoryTask, 100, pInfo, streamEnv.timer, &pTask->launchTaskTimer);
    }

    // try again in 500ms
    return TSDB_CODE_SUCCESS;
  }

  doCheckDownstreamStatus(pTask, *pHTask);
  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskScanHistoryDataComplete(SStreamTask* pTask) {
  if (atomic_load_8(&pTask->status.taskStatus) == TASK_STATUS__DROPPING) {
    return 0;
  }

  // restore param
  int32_t code = streamRestoreParam(pTask);
  if (code < 0) {
    return -1;
  }

  // dispatch recover finish req to all related downstream task
  code = streamDispatchScanHistoryFinishMsg(pTask);
  if (code < 0) {
    return -1;
  }

  return 0;
}

bool streamTaskRecoverScanStep1Finished(SStreamTask* pTask) {
  void* exec = pTask->exec.pExecutor;
  return qStreamRecoverScanStep1Finished(exec);
}

bool streamTaskRecoverScanStep2Finished(SStreamTask* pTask) {
  void* exec = pTask->exec.pExecutor;
  return qStreamRecoverScanStep2Finished(exec);
}

int32_t streamTaskRecoverSetAllStepFinished(SStreamTask* pTask) {
  void* exec = pTask->exec.pExecutor;
  return qStreamRecoverSetAllStepFinished(exec);
}

void streamHistoryTaskSetVerRangeStep2(SStreamTask* pTask) {
  SVersionRange* pRange = &pTask->dataRange.range;
  int64_t latestVer = walReaderGetCurrentVer(pTask->exec.pWalReader);
  ASSERT(latestVer >= pRange->maxVer);

  int64_t nextStartVer = pRange->maxVer + 1;
  if (nextStartVer > latestVer - 1) {
    // no input data yet. no need to execute the secondardy scan while stream task halt
    streamTaskRecoverSetAllStepFinished(pTask);
    qDebug(
        "s-task:%s no need to perform secondary scan-history-data(step 2), since no data ingest during secondary scan",
        pTask->id.idStr);
  } else {
    // 2. do secondary scan of the history data, the time window remain, and the version range is updated to
    // [pTask->dataRange.range.maxVer, ver1]
    pRange->minVer = nextStartVer;
    pRange->maxVer = latestVer - 1;
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
    SHistDataRange* pRange = &pTask->dataRange;
    qDebug("s-task:%s no related fill-history task, stream time window:%" PRId64 " - %" PRId64 ", ver range:%" PRId64
           " - %" PRId64,
           pTask->id.idStr, pRange->window.skey, pRange->window.ekey, pRange->range.minVer, pRange->range.maxVer);
  } else {
    SHistDataRange* pRange = &pTask->dataRange;

    int64_t ekey = pRange->window.ekey + 1;
    int64_t ver = pRange->range.minVer;

    pRange->window.skey = ekey;
    pRange->window.ekey = INT64_MAX;
    pRange->range.minVer = 0;
    pRange->range.maxVer = ver;

    qDebug("s-task:%s level:%d related-fill-history task exists, update stream calc time window:%" PRId64 " - %" PRId64
           ", verRang:%" PRId64 " - %" PRId64,
           pTask->id.idStr, pTask->info.taskLevel, pRange->window.skey, pRange->window.ekey, pRange->range.minVer,
           pRange->range.maxVer);
  }
}

void launchFillHistoryTask(SStreamTask* pTask) {
  int32_t tId = pTask->historyTaskId.taskId;
  if (tId == 0) {
    return;
  }

  ASSERT(pTask->status.downstreamReady == 1);
  qDebug("s-task:%s start to launch related fill-history task:0x%x", pTask->id.idStr, tId);

  // launch associated fill history task
  streamLaunchFillHistoryTask(pTask);
}

void streamTaskCheckDownstreamTasks(SStreamTask* pTask) {
  if (pTask->info.fillHistory) {
    qDebug("s-task:%s fill history task, wait for being launched", pTask->id.idStr);
    return;
  }

  ASSERT(pTask->status.downstreamReady == 0);

  // check downstream tasks for itself
  streamTaskDoCheckDownstreamTasks(pTask);
}

void streamTaskPause(SStreamTask* pTask) {
  SStreamMeta* pMeta = pTask->pMeta;

  int64_t st = taosGetTimestampMs();
  while(!pTask->status.pauseAllowed) {
    qDebug("s-task:%s wait for the task can be paused, vgId:%d", pTask->id.idStr, pMeta->vgId);
    taosMsleep(100);
  }

  atomic_store_8(&pTask->status.keepTaskStatus, pTask->status.taskStatus);
  atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__PAUSE);

  int64_t el = taosGetTimestampMs() - st;
  qDebug("vgId:%d s-task:%s set pause flag, prev:%s, elapsed time:%dms", pMeta->vgId, pTask->id.idStr,
         streamGetTaskStatusStr(pTask->status.keepTaskStatus), (int32_t)el);
}

// todo fix race condition
void streamTaskDisablePause(SStreamTask* pTask) {
  // pre-condition check
  const char* id = pTask->id.idStr;
  while (pTask->status.taskStatus == TASK_STATUS__PAUSE) {
    taosMsleep(100);
    qDebug("s-task:%s already in pause, wait for pause being cancelled, and set pause disabled, check in 100ms", id);
  }

  qDebug("s-task:%s disable task pause", id);
  pTask->status.pauseAllowed = 0;
}

void streamTaskEnablePause(SStreamTask* pTask) {
  qDebug("s-task:%s enable task pause", pTask->id.idStr);
  pTask->status.pauseAllowed = 1;
}