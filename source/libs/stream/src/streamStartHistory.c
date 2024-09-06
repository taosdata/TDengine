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

static int32_t           streamSetParamForScanHistory(SStreamTask* pTask);
static void              streamTaskSetRangeStreamCalc(SStreamTask* pTask);
static int32_t           initScanHistoryReq(SStreamTask* pTask, SStreamScanHistoryReq* pReq, int8_t igUntreated);
static SLaunchHTaskInfo* createHTaskLaunchInfo(SStreamMeta* pMeta, STaskId* pTaskId, int64_t hStreamId,
                                               int32_t hTaskId);
static void              tryLaunchHistoryTask(void* param, void* tmrId);
static void              doExecScanhistoryInFuture(void* param, void* tmrId);
static int32_t           doStartScanHistoryTask(SStreamTask* pTask);
static int32_t           streamTaskStartScanHistory(SStreamTask* pTask);
static void              checkFillhistoryTaskStatus(SStreamTask* pTask, SStreamTask* pHTask);
static int32_t           launchNotBuiltFillHistoryTask(SStreamTask* pTask);
static void              doRetryLaunchFillHistoryTask(SStreamTask* pTask, SLaunchHTaskInfo* pInfo, int64_t now);
static void              notRetryLaunchFillHistoryTask(SStreamTask* pTask, SLaunchHTaskInfo* pInfo, int64_t now);

static int32_t streamTaskSetReady(SStreamTask* pTask) {
  int32_t           numOfDowns = streamTaskGetNumOfDownstream(pTask);
  SStreamTaskState* p = streamTaskGetStatus(pTask);

  if ((p->state == TASK_STATUS__SCAN_HISTORY) && pTask->info.taskLevel != TASK_LEVEL__SOURCE) {
    int32_t numOfUps = taosArrayGetSize(pTask->upstreamInfo.pList);
    stDebug("s-task:%s level:%d task wait for %d upstream tasks complete scan-history procedure, status:%s",
            pTask->id.idStr, pTask->info.taskLevel, numOfUps, p->name);
  }

  ASSERT(pTask->status.downstreamReady == 0);
  pTask->status.downstreamReady = 1;

  pTask->execInfo.readyTs = taosGetTimestampMs();
  int64_t el = (pTask->execInfo.readyTs - pTask->execInfo.checkTs);
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

int32_t streamExecScanHistoryInFuture(SStreamTask* pTask, int32_t idleDuration) {
  int32_t numOfTicks = idleDuration / SCANHISTORY_IDLE_TIME_SLICE;
  if (numOfTicks <= 0) {
    numOfTicks = 1;
  } else if (numOfTicks > SCANHISTORY_IDLE_TICK) {
    numOfTicks = SCANHISTORY_IDLE_TICK;
  }

  // add ref for task
  SStreamTask* p = streamMetaAcquireTask(pTask->pMeta, pTask->id.streamId, pTask->id.taskId);
  if (p == NULL) {
    stError("s-task:0x%x failed to acquire task, status:%s, not exec scan-history data", pTask->id.taskId,
            streamTaskGetStatus(pTask)->name);
    return TSDB_CODE_SUCCESS;
  }

  pTask->schedHistoryInfo.numOfTicks = numOfTicks;

  int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
  stDebug("s-task:%s scan-history resumed in %.2fs, ref:%d", pTask->id.idStr, numOfTicks * 0.1, ref);

  if (pTask->schedHistoryInfo.pTimer == NULL) {
    pTask->schedHistoryInfo.pTimer =
        taosTmrStart(doExecScanhistoryInFuture, SCANHISTORY_IDLE_TIME_SLICE, pTask, streamTimer);
  } else {
    taosTmrReset(doExecScanhistoryInFuture, SCANHISTORY_IDLE_TIME_SLICE, pTask, streamTimer,
                 &pTask->schedHistoryInfo.pTimer);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskStartScanHistory(SStreamTask* pTask) {
  int32_t     level = pTask->info.taskLevel;
  ETaskStatus status = streamTaskGetStatus(pTask)->state;

  ASSERT((pTask->status.downstreamReady == 1) && (status == TASK_STATUS__SCAN_HISTORY) &&
         (pTask->info.fillHistory == 1));

  if (level == TASK_LEVEL__SOURCE) {
    return doStartScanHistoryTask(pTask);
  } else if (level == TASK_LEVEL__AGG) {
    return streamSetParamForScanHistory(pTask);
  } else if (level == TASK_LEVEL__SINK) {
    stDebug("s-task:%s sink task do nothing to handle scan-history", pTask->id.idStr);
  }
  return 0;
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

int32_t streamTaskOnScanHistoryTaskReady(SStreamTask* pTask) {
  // set the state to be ready
  streamTaskSetReady(pTask);
  streamTaskSetRangeStreamCalc(pTask);

  SStreamTaskState* p = streamTaskGetStatus(pTask);
  ASSERT((p->state == TASK_STATUS__SCAN_HISTORY) && (pTask->info.fillHistory == 1));

  stDebug("s-task:%s fill-history task enters into scan-history data stage, status:%s", pTask->id.idStr, p->name);
  streamTaskStartScanHistory(pTask);

  // NOTE: there will be an deadlock if launch fill history here.
  // start the related fill-history task, when current task is ready
  //  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
  //    streamLaunchFillHistoryTask(pTask);
  //  }

  return TSDB_CODE_SUCCESS;
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

// an fill history task needs to be started.
int32_t streamLaunchFillHistoryTask(SStreamTask* pTask) {
  SStreamMeta*         pMeta = pTask->pMeta;
  STaskExecStatisInfo* pExecInfo = &pTask->execInfo;
  const char*          idStr = pTask->id.idStr;
  int64_t              hStreamId = pTask->hTaskInfo.id.streamId;
  int32_t              hTaskId = pTask->hTaskInfo.id.taskId;
  int64_t              now = taosGetTimestampMs();
  int32_t              code = 0;

  ASSERT(hTaskId != 0);

  // check stream task status in the first place.
  SStreamTaskState* pStatus = streamTaskGetStatus(pTask);
  if (pStatus->state != TASK_STATUS__READY && pStatus->state != TASK_STATUS__HALT &&
      pStatus->state != TASK_STATUS__PAUSE) {
    stDebug("s-task:%s not launch related fill-history task:0x%" PRIx64 "-0x%x, status:%s", idStr, hStreamId, hTaskId,
            pStatus->name);

    streamMetaAddTaskLaunchResult(pMeta, hStreamId, hTaskId, pExecInfo->checkTs, pExecInfo->readyTs, false);
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
      streamMetaAddTaskLaunchResult(pMeta, hStreamId, hTaskId, pExecInfo->checkTs, pExecInfo->readyTs, false);
    } else {
      if (pHisTask->status.downstreamReady == 1) {  // it's ready now, do nothing
        stDebug("s-task:%s fill-history task is ready, no need to check downstream", pHisTask->id.idStr);
        streamMetaAddTaskLaunchResult(pMeta, hStreamId, hTaskId, pExecInfo->checkTs, pExecInfo->readyTs, true);
      } else {  // exist, but not ready, continue check downstream task status
        if (pHisTask->pBackend == NULL) {
          code = pMeta->expandTaskFn(pHisTask);
          if (code != TSDB_CODE_SUCCESS) {
            streamMetaAddFailedTaskSelf(pHisTask, now);
            stError("s-task:%s failed to expand fill-history task, code:%s", pHisTask->id.idStr, tstrerror(code));
          }
        }

        if (code == TSDB_CODE_SUCCESS) {
          checkFillhistoryTaskStatus(pTask, pHisTask);
        }
      }

      streamMetaReleaseTask(pMeta, pHisTask);
    }

    return TSDB_CODE_SUCCESS;
  } else {
    return launchNotBuiltFillHistoryTask(pTask);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
int32_t initScanHistoryReq(SStreamTask* pTask, SStreamScanHistoryReq* pReq, int8_t igUntreated) {
  pReq->msgHead.vgId = pTask->info.nodeId;
  pReq->streamId = pTask->id.streamId;
  pReq->taskId = pTask->id.taskId;
  pReq->igUntreated = igUntreated;
  return 0;
}

void checkFillhistoryTaskStatus(SStreamTask* pTask, SStreamTask* pHTask) {
  SDataRange* pRange = &pHTask->dataRange;

  // the query version range should be limited to the already processed data
  pHTask->execInfo.checkTs = taosGetTimestampMs();

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    stDebug("s-task:%s set the launch condition for fill-history s-task:%s, window:%" PRId64 " - %" PRId64
            " verRange:%" PRId64 " - %" PRId64 ", init:%" PRId64,
            pTask->id.idStr, pHTask->id.idStr, pRange->window.skey, pRange->window.ekey, pRange->range.minVer,
            pRange->range.maxVer, pHTask->execInfo.checkTs);
  } else {
    stDebug("s-task:%s no fill-history condition for non-source task:%s", pTask->id.idStr, pHTask->id.idStr);
  }

  // check if downstream tasks have been ready
  streamTaskHandleEvent(pHTask->status.pSM, TASK_EVENT_INIT_SCANHIST);
}

void notRetryLaunchFillHistoryTask(SStreamTask* pTask, SLaunchHTaskInfo* pInfo, int64_t now) {
  SStreamMeta*      pMeta = pTask->pMeta;
  SHistoryTaskInfo* pHTaskInfo = &pTask->hTaskInfo;

  int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
  streamMetaAddTaskLaunchResult(pMeta, pInfo->hTaskId.streamId, pInfo->hTaskId.taskId, 0, now, false);

  stError("s-task:%s max retry:%d reached, quit from retrying launch related fill-history task:0x%x, ref:%d",
          pTask->id.idStr, MAX_RETRY_LAUNCH_HISTORY_TASK, (int32_t)pHTaskInfo->id.taskId, ref);

  pHTaskInfo->id.taskId = 0;
  pHTaskInfo->id.streamId = 0;
}

void doRetryLaunchFillHistoryTask(SStreamTask* pTask, SLaunchHTaskInfo* pInfo, int64_t now) {
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

void tryLaunchHistoryTask(void* param, void* tmrId) {
  SLaunchHTaskInfo* pInfo = param;
  SStreamMeta*      pMeta = pInfo->pMeta;
  int64_t           now = taosGetTimestampMs();
  int32_t           code = 0;

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
      notRetryLaunchFillHistoryTask(pTask, pInfo, now);
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
        if (pHTask->pBackend == NULL) {
          code = pMeta->expandTaskFn(pHTask);
          if (code != TSDB_CODE_SUCCESS) {
            streamMetaAddFailedTaskSelf(pHTask, now);
            stError("failed to expand fill-history task:%s, code:%s", pHTask->id.idStr, tstrerror(code));
          }
        }

        if (code == TSDB_CODE_SUCCESS) {
          checkFillhistoryTaskStatus(pTask, pHTask);
          // not in timer anymore
          int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
          stDebug("s-task:0x%x fill-history task launch completed, retry times:%d, ref:%d", (int32_t)pInfo->id.taskId,
                  pHTaskInfo->retryTimes, ref);
        }
        streamMetaReleaseTask(pMeta, pHTask);
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

SLaunchHTaskInfo* createHTaskLaunchInfo(SStreamMeta* pMeta, STaskId* pTaskId, int64_t hStreamId, int32_t hTaskId) {
  SLaunchHTaskInfo* pInfo = taosMemoryCalloc(1, sizeof(SLaunchHTaskInfo));
  if (pInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pInfo->id.streamId = pTaskId->streamId;
  pInfo->id.taskId = pTaskId->taskId;

  pInfo->hTaskId.streamId = hStreamId;
  pInfo->hTaskId.taskId = hTaskId;

  pInfo->pMeta = pMeta;
  return pInfo;
}

int32_t launchNotBuiltFillHistoryTask(SStreamTask* pTask) {
  SStreamMeta*         pMeta = pTask->pMeta;
  STaskExecStatisInfo* pExecInfo = &pTask->execInfo;
  const char*          idStr = pTask->id.idStr;
  int64_t              hStreamId = pTask->hTaskInfo.id.streamId;
  int32_t              hTaskId = pTask->hTaskInfo.id.taskId;
  ASSERT(hTaskId != 0);

  stWarn("s-task:%s vgId:%d failed to launch history task:0x%x, since not built yet", idStr, pMeta->vgId, hTaskId);

  STaskId           id = streamTaskGetTaskId(pTask);
  SLaunchHTaskInfo* pInfo = createHTaskLaunchInfo(pMeta, &id, hStreamId, hTaskId);
  if (pInfo == NULL) {
    stError("s-task:%s failed to launch related fill-history task, since Out Of Memory", idStr);
    streamMetaAddTaskLaunchResult(pMeta, hStreamId, hTaskId, pExecInfo->checkTs, pExecInfo->readyTs, false);
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
      streamMetaAddTaskLaunchResult(pMeta, hStreamId, hTaskId, pExecInfo->checkTs, pExecInfo->readyTs, false);
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

int32_t streamTaskResetTimewindowFilter(SStreamTask* pTask) {
  void* exec = pTask->exec.pExecutor;
  return qStreamInfoResetTimewindowFilter(exec);
}

bool streamHistoryTaskSetVerRangeStep2(SStreamTask* pTask, int64_t nextProcessVer) {
  SVersionRange* pRange = &pTask->dataRange.range;
  ASSERT(nextProcessVer >= pRange->maxVer);

  // maxVer for fill-history task is the version, where the last timestamp is acquired.
  // it's also the maximum version to scan data in tsdb.
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
    pTask->step2Range.minVer = walScanStartVer;
    pTask->step2Range.maxVer = nextProcessVer - 1;
    stDebug("s-task:%s set step2 verRange:%" PRId64 "-%" PRId64 ", step1 verRange:%" PRId64 "-%" PRId64,
            pTask->id.idStr, pTask->step2Range.minVer, pTask->step2Range.maxVer, pRange->minVer, pRange->maxVer);
    return false;
  }
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
    STimeWindow   win = pRange->window;
    streamSetParamForStreamScannerStep2(pTask, &verRange, &win);
  }
}

void doExecScanhistoryInFuture(void* param, void* tmrId) {
  SStreamTask* pTask = param;
  pTask->schedHistoryInfo.numOfTicks -= 1;

  SStreamTaskState* p = streamTaskGetStatus(pTask);
  if (p->state == TASK_STATUS__DROPPING || p->state == TASK_STATUS__STOP) {
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s status:%s not start scan-history again, ref:%d", pTask->id.idStr, p->name, ref);

    streamMetaReleaseTask(pTask->pMeta, pTask);
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
    taosTmrReset(doExecScanhistoryInFuture, SCANHISTORY_IDLE_TIME_SLICE, pTask, streamTimer,
                 &pTask->schedHistoryInfo.pTimer);
  }
}

int32_t doStartScanHistoryTask(SStreamTask* pTask) {
  SVersionRange* pRange = &pTask->dataRange.range;
  if (pTask->info.fillHistory) {
    streamSetParamForScanHistory(pTask);
  }

  streamSetParamForStreamScannerStep1(pTask, pRange, &pTask->dataRange.window);
  int32_t code = streamStartScanHistoryAsync(pTask, 0);
  return code;
}
