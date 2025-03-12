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

#include "ttime.h"
#include "streamInt.h"
#include "ttimer.h"

#define TRIGGER_RECHECK_INTERVAL (5 * 1000)
#define INITIAL_TRIGGER_INTERVAL (120 * 1000)

static void streamTaskResumeHelper(void* param, void* tmrId);
static void streamTaskSchedHelper(void* param, void* tmrId);

void streamSetupScheduleTrigger(SStreamTask* pTask) {
  int64_t     delay = 0;
  int32_t     code = 0;
  const char* id = pTask->id.idStr;
  int64_t*    pTaskRefId = NULL;

  if (pTask->info.fillHistory == 1) {
    return;
  }

  // dynamic set the trigger & triggerParam for STREAM_TRIGGER_FORCE_WINDOW_CLOSE
  if ((pTask->info.trigger == STREAM_TRIGGER_FORCE_WINDOW_CLOSE) && (pTask->info.taskLevel == TASK_LEVEL__SOURCE)) {
    int64_t     waterMark = 0;
    SInterval   interval = {0};
    STimeWindow lastTimeWindow = {0};
    code = qGetStreamIntervalExecInfo(pTask->exec.pExecutor, &waterMark, &interval, &lastTimeWindow);
    if (code) {
      stError("s-task:%s failed to init scheduler info, code:%s", id, tstrerror(code));
      return;
    }

    pTask->info.delaySchedParam = interval.sliding;
    pTask->info.watermark = waterMark;
    pTask->info.interval = interval;

    // calculate the first start timestamp
    int64_t     now = taosGetTimestamp(interval.precision);
    STimeWindow curWin = getAlignQueryTimeWindow(&pTask->info.interval, now);

    if (lastTimeWindow.skey == INT64_MIN) {  // start from now, not the exec task timestamp after delay
      pTask->status.latestForceWindow.skey = curWin.skey - pTask->info.interval.interval;
      pTask->status.latestForceWindow.ekey = now;

      delay = (curWin.ekey + 1) - now + waterMark;
      delay = convertTimePrecision(delay, interval.precision, TSDB_TIME_PRECISION_MILLI);

    } else {
      pTask->status.latestForceWindow = lastTimeWindow;
      // It's the current calculated time window
      int64_t calEkey = lastTimeWindow.skey + pTask->info.interval.interval * 2;
      if (calEkey + waterMark < now) {     // unfinished time window existed
        delay = INITIAL_TRIGGER_INTERVAL;  // wait for 2min to start to calculate
      } else {
        delay = (curWin.ekey + 1) - now + waterMark;
        delay = convertTimePrecision(delay, interval.precision, TSDB_TIME_PRECISION_MILLI);
      }
    }

    stInfo("s-task:%s extract interval info from executor, wm:%" PRId64 " interval:%" PRId64 " unit:%c sliding:%" PRId64
           " unit:%c, initial start after:%" PRId64 "ms last_win:%" PRId64 "-%" PRId64,
           id, waterMark, interval.interval, interval.intervalUnit, interval.sliding, interval.slidingUnit, delay,
           pTask->status.latestForceWindow.skey, pTask->status.latestForceWindow.ekey);
  } else {
    delay = pTask->info.delaySchedParam;
    if (delay == 0) {
      return;
    }
  }

  code = streamTaskAllocRefId(pTask, &pTaskRefId);
  if (code == 0) {
    stDebug("s-task:%s refId:%" PRId64 " enable the scheduler trigger, delay:%" PRId64, pTask->id.idStr,
            pTask->id.refId, delay);

    streamTmrStart(streamTaskSchedHelper, (int32_t)delay, pTaskRefId, streamTimer, &pTask->schedInfo.pDelayTimer,
                   pTask->pMeta->vgId, "sched-tmr");
    pTask->schedInfo.status = TASK_TRIGGER_STATUS__INACTIVE;
  }
}

int32_t streamTrySchedExec(SStreamTask* pTask, bool chkptQueue) {
  if (streamTaskSetSchedStatusWait(pTask)) {
    return streamTaskSchedTask(pTask->pMsgCb, pTask->info.nodeId, pTask->id.streamId, pTask->id.taskId, 0, chkptQueue);
  } else {
    if (chkptQueue) {
      stWarn("s-task:%s not launch task in chkpt queue, may delay checkpoint procedure", pTask->id.idStr);
    } else {
      stTrace("s-task:%s not launch task since sched status:%d", pTask->id.idStr, pTask->status.schedStatus);
    }
  }

  return 0;
}

int32_t streamTaskSchedTask(SMsgCb* pMsgCb, int32_t vgId, int64_t streamId, int32_t taskId, int32_t execType, bool chkptExec) {
  int32_t code = 0;
  int32_t tlen = 0;

  SStreamTaskRunReq req = {.streamId = streamId, .taskId = taskId, .reqType = execType};

  tEncodeSize(tEncodeStreamTaskRunReq, &req, tlen, code);
  if (code < 0) {
    stError("s-task:0x%" PRIx64 " vgId:%d encode stream task run req failed, code:%s", streamId, vgId, tstrerror(code));
    return code;
  }

  void* buf = rpcMallocCont(tlen + sizeof(SMsgHead));
  if (buf == NULL) {
    stError("vgId:%d failed to create msg to start stream task:0x%x exec, type:%d, code:%s", vgId, taskId, execType,
            tstrerror(terrno));
    return terrno;
  }

  ((SMsgHead*)buf)->vgId = vgId;
  char* bufx = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, (uint8_t*)bufx, tlen);
  if ((code = tEncodeStreamTaskRunReq(&encoder, &req)) < 0) {
    rpcFreeCont(buf);
    tEncoderClear(&encoder);
    stError("s-task:0x%x vgId:%d encode run task msg failed, code:%s", taskId, vgId, tstrerror(code));
    return code;
  }
  tEncoderClear(&encoder);

  if (streamId != 0) {
    stDebug("vgId:%d create msg to for task:0x%x, exec type:%d, %s", vgId, taskId, execType,
            streamTaskGetExecType(execType));
  } else {
    stDebug("vgId:%d create msg to exec, type:%d, %s", vgId, execType, streamTaskGetExecType(execType));
  }

  if (chkptExec) {
    SRpcMsg msg = {.msgType = TDMT_STREAM_CHKPT_EXEC, .pCont = buf, .contLen = tlen + sizeof(SMsgHead)};
    code = tmsgPutToQueue(pMsgCb, STREAM_CHKPT_QUEUE, &msg);
    if (code) {
      stError("vgId:%d failed to put msg into stream chkpt queue, code:%s, %x", vgId, tstrerror(code), taskId);
    }
  } else {
    SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = buf, .contLen = tlen + sizeof(SMsgHead)};
    code = tmsgPutToQueue(pMsgCb, STREAM_QUEUE, &msg);
    if (code) {
      stError("vgId:%d failed to put msg into stream queue, code:%s, %x", vgId, tstrerror(code), taskId);
    }
  }
  return code;
}

void streamTaskClearSchedIdleInfo(SStreamTask* pTask) { pTask->status.schedIdleTime = 0; }

void streamTaskSetIdleInfo(SStreamTask* pTask, int32_t idleTime) { pTask->status.schedIdleTime = idleTime; }

void streamTaskResumeInFuture(SStreamTask* pTask) {
  stDebug("s-task:%s task should idle, add into timer to retry in %dms", pTask->id.idStr,
          pTask->status.schedIdleTime);

  // add one ref count for task
  int64_t* pTaskRefId = NULL;
  int32_t  code = streamTaskAllocRefId(pTask, &pTaskRefId);
  if (code == 0) {
    streamTmrStart(streamTaskResumeHelper, pTask->status.schedIdleTime, pTaskRefId, streamTimer,
                   &pTask->schedInfo.pIdleTimer, pTask->pMeta->vgId, "resume-task-tmr");
  }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void streamTaskResumeHelper(void* param, void* tmrId) {
  int32_t      code = 0;
  int64_t      taskRefId = *(int64_t*)param;
  SStreamTask* pTask = taosAcquireRef(streamTaskRefPool, taskRefId);
  if (pTask == NULL) {
    stError("invalid task rid:%" PRId64 " failed to acquired stream-task at %s", taskRefId, __func__);
    streamTaskFreeRefId(param);
    return;
  }

  SStreamTaskId*   pId = &pTask->id;
  SStreamTaskState p = streamTaskGetStatus(pTask);

  if (p.state == TASK_STATUS__DROPPING || p.state == TASK_STATUS__STOP) {
    int8_t status = streamTaskSetSchedStatusInactive(pTask);
    TAOS_UNUSED(status);

    stDebug("s-task:%s status:%s not resume task", pId->idStr, p.name);
    streamMetaReleaseTask(pTask->pMeta, pTask);
    streamTaskFreeRefId(param);
    return;
  }

  code = streamTaskSchedTask(pTask->pMsgCb, pTask->info.nodeId, pId->streamId, pId->taskId, STREAM_EXEC_T_RESUME_TASK,
                             (p.state == TASK_STATUS__CK));
  if (code) {
    stError("s-task:%s sched task failed, code:%s", pId->idStr, tstrerror(code));
  } else {
    if (p.state == TASK_STATUS__CK) {
      stDebug("trigger to resume s-task:%s in stream chkpt queue after idled for %dms", pId->idStr,
              pTask->status.schedIdleTime);
    } else {
      stDebug("trigger to resume s-task:%s after idled for %dms", pId->idStr, pTask->status.schedIdleTime);
    }
    // release the task ref count
    streamTaskClearSchedIdleInfo(pTask);
  }

  streamMetaReleaseTask(pTask->pMeta, pTask);
  streamTaskFreeRefId(param);
}

static int32_t doCreateForceWindowTrigger(SStreamTask* pTask, int32_t* pNextTrigger) {
  int32_t         num = 0;
  int32_t         code = 0;
  const char*     id = pTask->id.idStr;
  int8_t          precision = pTask->info.interval.precision;
  SStreamTrigger* pTrigger = NULL;
  bool            isFull = false;

  while (1) {
    code = streamCreateForcewindowTrigger(&pTrigger, pTask->info.delaySchedParam, &pTask->info.interval,
                                          &pTask->status.latestForceWindow, id);
    if (code != 0) {
      *pNextTrigger = convertTimePrecision(*pNextTrigger, precision, TSDB_TIME_PRECISION_MILLI);
      stError("s-task:%s failed to prepare force window close trigger, code:%s, try again in %dms", id,
              tstrerror(code), *pNextTrigger);
      return code;
    }

    // in the force window close model, status trigger does not matter. So we do not set the trigger model
    code = streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pTrigger);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s failed to put retrieve aggRes block into q, code:%s", pTask->id.idStr, tstrerror(code));
      return code;
    }

    num += 1;

    // check whether the time window gaps exist or not
    int64_t now = taosGetTimestamp(precision);

    // there are gaps, needs to be filled
    STimeWindow w = pTrigger->pBlock->info.window;
    w.ekey = w.skey + pTask->info.interval.interval;
    if (w.skey <= pTask->status.latestForceWindow.skey) {
      stFatal("s-task:%s invalid new time window in force_window_close trigger model, skey:%" PRId64
              " should be greater than latestForceWindow skey:%" PRId64,
              pTask->id.idStr, w.skey, pTask->status.latestForceWindow.skey);
    }

    pTask->status.latestForceWindow = w;
    isFull = streamQueueIsFull(pTask->inputq.queue);

    if ((w.ekey + pTask->info.watermark + pTask->info.interval.interval > now) || isFull) {
      int64_t prev = convertTimePrecision(*pNextTrigger, precision, TSDB_TIME_PRECISION_MILLI);
      if (!isFull) {
        *pNextTrigger = w.ekey + pTask->info.watermark + pTask->info.interval.interval - now;
      }

      *pNextTrigger = convertTimePrecision(*pNextTrigger, precision, TSDB_TIME_PRECISION_MILLI);
      pTask->chkInfo.nextProcessVer = w.ekey + pTask->info.interval.interval;
      stDebug("s-task:%s generate %d time window(s), trigger delay adjust from %" PRId64 " to %d, set ver:%" PRId64, id,
              num, prev, *pNextTrigger, pTask->chkInfo.nextProcessVer);
      return code;
    } else {
      stDebug("s-task:%s gap exist for force_window_close, current force_window_skey:%" PRId64, id, w.skey);
    }
  }
}

void streamTaskSchedHelper(void* param, void* tmrId) {
  int32_t      code = 0;
  int64_t      taskRefId = *(int64_t*)param;
  SStreamTask* pTask = taosAcquireRef(streamTaskRefPool, taskRefId);
  if (pTask == NULL) {
    stError("invalid task rid:%" PRId64 " failed to acquired stream-task at %s", taskRefId, __func__);
    streamTaskFreeRefId(param);
    return;
  }

  stDebug("s-task:%s acquire task, refId:%" PRId64, pTask->id.idStr, pTask->id.refId);

  const char* id = pTask->id.idStr;
  int32_t     nextTrigger = (int32_t)pTask->info.delaySchedParam;
  int32_t     vgId = pTask->pMeta->vgId;

  int8_t status = atomic_load_8(&pTask->schedInfo.status);

  if (pTask->info.trigger == STREAM_TRIGGER_FORCE_WINDOW_CLOSE && pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    int32_t next = convertTimePrecision(nextTrigger, pTask->info.interval.precision, TSDB_TIME_PRECISION_MILLI);
    stTrace("s-task:%s in scheduler, trigger status:%d, next:%dms", id, status, next);
  } else {
    stTrace("s-task:%s in scheduler, trigger status:%d, next:%dms", id, status, nextTrigger);
  }

  if (streamTaskShouldStop(pTask)) {
    stDebug("s-task:%s should stop, jump out of schedTimer", id);
    streamMetaReleaseTask(pTask->pMeta, pTask);
    streamTaskFreeRefId(param);
    return;
  }

  if (streamTaskShouldPause(pTask)) {
    stDebug("s-task:%s is paused, recheck in %.2fs", id, TRIGGER_RECHECK_INTERVAL / 1000.0);
    streamTmrStart(streamTaskSchedHelper, TRIGGER_RECHECK_INTERVAL, param, streamTimer, &pTask->schedInfo.pDelayTimer,
                   vgId, "sched-run-tmr");
    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  if (pTask->status.downstreamReady == 0) {
    stDebug("s-task:%s downstream not ready, recheck in %.2fs", id, TRIGGER_RECHECK_INTERVAL / 1000.0);
    streamTmrStart(streamTaskSchedHelper, TRIGGER_RECHECK_INTERVAL, param, streamTimer, &pTask->schedInfo.pDelayTimer,
                   vgId, "sched-run-tmr");
    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  if (streamTaskGetStatus(pTask).state == TASK_STATUS__CK) {
    nextTrigger = TRIGGER_RECHECK_INTERVAL;  // retry in 10 sec
    stDebug("s-task:%s in checkpoint procedure, not retrieve result, next:%dms", id, TRIGGER_RECHECK_INTERVAL);
  } else {
    if (pTask->info.trigger == STREAM_TRIGGER_FORCE_WINDOW_CLOSE && pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
      code = doCreateForceWindowTrigger(pTask, &nextTrigger);
      if (code != TSDB_CODE_SUCCESS) {
        goto _end;
      }
    } else if (status == TASK_TRIGGER_STATUS__MAY_ACTIVE) {
      SStreamTrigger* pTrigger = NULL;
      code = streamCreateSinkResTrigger(&pTrigger);
      if (code) {
        stError("s-task:%s failed to prepare retrieve data trigger, code:%s, try again in %dms", id, tstrerror(code),
                nextTrigger);
        goto _end;
      }

      atomic_store_8(&pTask->schedInfo.status, TASK_TRIGGER_STATUS__INACTIVE);

      code = streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pTrigger);
      if (code != TSDB_CODE_SUCCESS) {
        stError("s-task:%s failed to put retrieve aggRes block into q, code:%s", pTask->id.idStr, tstrerror(code));
        goto _end;
      }
    }

    code = streamTrySchedExec(pTask, false);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s failed to sched to run, wait for next time", pTask->id.idStr);
    }
  }

_end:
  streamTmrStart(streamTaskSchedHelper, nextTrigger, param, streamTimer, &pTask->schedInfo.pDelayTimer, vgId,
                 "sched-run-tmr");
  streamMetaReleaseTask(pTask->pMeta, pTask);
}
