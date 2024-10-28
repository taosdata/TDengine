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

static void streamTaskResumeHelper(void* param, void* tmrId);
static void streamTaskSchedHelper(void* param, void* tmrId);

void streamSetupScheduleTrigger(SStreamTask* pTask) {
  int64_t     delay = 0;
  int32_t     code = 0;
  const char* id = pTask->id.idStr;

  if (pTask->info.fillHistory == 1) {
    return;
  }

  // dynamic set the trigger & triggerParam for STREAM_TRIGGER_FORCE_WINDOW_CLOSE
  if ((pTask->info.trigger == STREAM_TRIGGER_FORCE_WINDOW_CLOSE) && (pTask->info.taskLevel == TASK_LEVEL__SOURCE)) {
    int64_t   waterMark = 0;
    SInterval interval = {0};
    STimeWindow lastTimeWindow = {0};
    code = qGetStreamIntervalExecInfo(pTask->exec.pExecutor, &waterMark, &interval, &lastTimeWindow);
    if (code) {
      stError("s-task:%s failed to init scheduler info, code:%s", id, tstrerror(code));
      return;
    }

    pTask->status.latestForceWindow = lastTimeWindow;
    pTask->info.delaySchedParam = interval.sliding;
    pTask->info.watermark = waterMark;
    pTask->info.interval = interval;

    // calculate the first start timestamp
    int64_t now = taosGetTimestamp(interval.precision);
    STimeWindow curWin = getAlignQueryTimeWindow(&pTask->info.interval, now);
    delay = (curWin.ekey + 1) - now + waterMark;

    stInfo("s-task:%s extract interval info from executor, wm:%" PRId64 " interval:%" PRId64 " unit:%c sliding:%" PRId64
           " unit:%c, initial start after:%" PRId64,
           id, waterMark, interval.interval, interval.intervalUnit, interval.sliding, interval.slidingUnit, delay);
  } else {
    delay = pTask->info.delaySchedParam;
    if (delay == 0) {
      return;
    }
  }

  int32_t ref = streamMetaAcquireOneTask(pTask);
  stDebug("s-task:%s setup scheduler trigger, ref:%d delay:%" PRId64 " ms", id, ref, pTask->info.delaySchedParam);

  streamTmrStart(streamTaskSchedHelper, delay, pTask, streamTimer, &pTask->schedInfo.pDelayTimer,
                 pTask->pMeta->vgId, "sched-tmr");
  pTask->schedInfo.status = TASK_TRIGGER_STATUS__INACTIVE;
}

int32_t streamTrySchedExec(SStreamTask* pTask) {
  if (streamTaskSetSchedStatusWait(pTask)) {
    return streamTaskSchedTask(pTask->pMsgCb, pTask->info.nodeId, pTask->id.streamId, pTask->id.taskId, 0);
  } else {
    stTrace("s-task:%s not launch task since sched status:%d", pTask->id.idStr, pTask->status.schedStatus);
  }

  return 0;
}

int32_t streamTaskSchedTask(SMsgCb* pMsgCb, int32_t vgId, int64_t streamId, int32_t taskId, int32_t execType) {
  SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
  if (pRunReq == NULL) {
    stError("vgId:%d failed to create msg to start stream task:0x%x exec, type:%d, code:%s", vgId, taskId, execType,
            tstrerror(terrno));
    return terrno;
  }

  if (streamId != 0) {
    stDebug("vgId:%d create msg to for task:0x%x, exec type:%d, %s", vgId, taskId, execType,
            streamTaskGetExecType(execType));
  } else {
    stDebug("vgId:%d create msg to exec, type:%d, %s", vgId, execType, streamTaskGetExecType(execType));
  }

  pRunReq->head.vgId = vgId;
  pRunReq->streamId = streamId;
  pRunReq->taskId = taskId;
  pRunReq->reqType = execType;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  int32_t code = tmsgPutToQueue(pMsgCb, STREAM_QUEUE, &msg);
  if (code) {
    stError("vgId:%d failed to put msg into stream queue, code:%s, %x", vgId, tstrerror(code), taskId);
  }
  return code;
}

void streamTaskClearSchedIdleInfo(SStreamTask* pTask) { pTask->status.schedIdleTime = 0; }

void streamTaskSetIdleInfo(SStreamTask* pTask, int32_t idleTime) { pTask->status.schedIdleTime = idleTime; }

void streamTaskResumeInFuture(SStreamTask* pTask) {
  int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
  stDebug("s-task:%s task should idle, add into timer to retry in %dms, ref:%d", pTask->id.idStr,
          pTask->status.schedIdleTime, ref);

  // add one ref count for task
  int32_t unusedRetRef = streamMetaAcquireOneTask(pTask);
  streamTmrStart(streamTaskResumeHelper, pTask->status.schedIdleTime, pTask, streamTimer, &pTask->schedInfo.pIdleTimer,
                 pTask->pMeta->vgId, "resume-task-tmr");
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void streamTaskResumeHelper(void* param, void* tmrId) {
  SStreamTask*     pTask = (SStreamTask*)param;
  SStreamTaskId*   pId = &pTask->id;
  SStreamTaskState p = streamTaskGetStatus(pTask);
  int32_t          code = 0;

  if (p.state == TASK_STATUS__DROPPING || p.state == TASK_STATUS__STOP) {
    int8_t status = streamTaskSetSchedStatusInactive(pTask);
    TAOS_UNUSED(status);

    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s status:%s not resume task, ref:%d", pId->idStr, p.name, ref);

    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  code = streamTaskSchedTask(pTask->pMsgCb, pTask->info.nodeId, pId->streamId, pId->taskId, STREAM_EXEC_T_RESUME_TASK);
  int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
  if (code) {
    stError("s-task:%s sched task failed, code:%s, ref:%d", pId->idStr, tstrerror(code), ref);
  } else {
    stDebug("trigger to resume s-task:%s after idled for %dms, ref:%d", pId->idStr, pTask->status.schedIdleTime, ref);

    // release the task ref count
    streamTaskClearSchedIdleInfo(pTask);
    streamMetaReleaseTask(pTask->pMeta, pTask);
  }
}

void streamTaskSchedHelper(void* param, void* tmrId) {
  SStreamTask* pTask = (void*)param;
  const char*  id = pTask->id.idStr;
  int32_t      nextTrigger = (int32_t)pTask->info.delaySchedParam;
  int32_t      vgId = pTask->pMeta->vgId;
  int32_t      code = 0;

  int8_t status = atomic_load_8(&pTask->schedInfo.status);
  stTrace("s-task:%s in scheduler, trigger status:%d, next:%dms", id, status, nextTrigger);

  if (streamTaskShouldStop(pTask)) {
    stDebug("s-task:%s should stop, jump out of schedTimer", id);
    return;
  }

  if (streamTaskShouldPause(pTask)) {
    stDebug("s-task:%s is paused, check in nextTrigger:%ds", id, nextTrigger/1000);
    streamTmrStart(streamTaskSchedHelper, nextTrigger, pTask, streamTimer, &pTask->schedInfo.pDelayTimer, vgId,
                   "sched-run-tmr");
  }

  if (streamTaskGetStatus(pTask).state == TASK_STATUS__CK) {
    stDebug("s-task:%s in checkpoint procedure, not retrieve result, next:%dms", id, nextTrigger);
  } else {
    if (pTask->info.trigger == STREAM_TRIGGER_FORCE_WINDOW_CLOSE && pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
      SStreamTrigger* pTrigger = NULL;

      while (1) {
        code = streamCreateForcewindowTrigger(&pTrigger, pTask->info.delaySchedParam, &pTask->info.interval,
                                              &pTask->status.latestForceWindow);
        if (code != 0) {
          stError("s-task:%s failed to prepare force window close trigger, code:%s, try again in %dms", id,
                  tstrerror(code), nextTrigger);
          goto _end;
        }

        // in the force window close model, status trigger does not matter. So we do not set the trigger model
        code = streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pTrigger);
        if (code != TSDB_CODE_SUCCESS) {
          stError("s-task:%s failed to put retrieve aggRes block into q, code:%s", pTask->id.idStr, tstrerror(code));
          goto _end;
        }

        // check whether the time window gaps exist or not
        int64_t now = taosGetTimestamp(pTask->info.interval.precision);
        int64_t intervalEndTs = pTrigger->pBlock->info.window.skey + pTask->info.interval.interval;

        // there are gaps, needs to be filled
        STimeWindow w = pTrigger->pBlock->info.window;
        w.ekey = w.skey + pTask->info.interval.interval;
        if (w.skey <= pTask->status.latestForceWindow.skey) {
          stFatal("s-task:%s invalid new time window in force_window_close model, skey:%" PRId64
                  " should be greater than latestForceWindow skey:%" PRId64,
                  pTask->id.idStr, w.skey, pTask->status.latestForceWindow.skey);
        }

        pTask->status.latestForceWindow = w;
        if (intervalEndTs + pTask->info.watermark + pTask->info.interval.interval > now) {
          break;
        } else {
          stDebug("s-task:%s gap exist for force_window_close, current force_window_skey:%" PRId64, id, w.skey);
        }
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

    code = streamTrySchedExec(pTask);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s failed to sched to run, wait for next time", pTask->id.idStr);
    }
  }

_end:
  streamTmrStart(streamTaskSchedHelper, nextTrigger, pTask, streamTimer, &pTask->schedInfo.pDelayTimer, vgId,
                 "sched-run-tmr");
}
