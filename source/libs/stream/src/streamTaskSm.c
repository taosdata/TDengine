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
#include "tmisce.h"
#include "tstream.h"
#include "ttimer.h"
#include "wal.h"

static int32_t initRes = 0;

#define GET_EVT_NAME(_ev)  (StreamTaskEventList[(_ev)].name)
#define CHECK_RET_VAL(_exec)             \
  do {                                   \
    void* p = (_exec);                   \
    if (p == NULL) {                     \
      initRes = TSDB_CODE_OUT_OF_MEMORY; \
      return;                            \
    }                                    \
  } while (0);

SStreamTaskState StreamTaskStatusList[9] = {
    {.state = TASK_STATUS__READY, .name = "ready"},
    {.state = TASK_STATUS__DROPPING, .name = "dropped"},
    {.state = TASK_STATUS__UNINIT, .name = "uninit"},
    {.state = TASK_STATUS__STOP, .name = "stop"},
    {.state = TASK_STATUS__SCAN_HISTORY, .name = "scan-history"},
    {.state = TASK_STATUS__HALT, .name = "halt"},
    {.state = TASK_STATUS__PAUSE, .name = "paused"},
    {.state = TASK_STATUS__CK, .name = "checkpoint"},
};

typedef struct SStreamEventInfo {
  EStreamTaskEvent event;
  const char*      name;
} SStreamEventInfo;

SStreamEventInfo StreamTaskEventList[12] = {
    {.event = 0, .name = ""},  // dummy event, place holder
    {.event = TASK_EVENT_INIT, .name = "initialize"},
    {.event = TASK_EVENT_INIT_SCANHIST, .name = "scan-history-init"},
    {.event = TASK_EVENT_SCANHIST_DONE, .name = "scan-history-completed"},
    {.event = TASK_EVENT_STOP, .name = "stopping"},
    {.event = TASK_EVENT_GEN_CHECKPOINT, .name = "checkpoint"},
    {.event = TASK_EVENT_CHECKPOINT_DONE, .name = "checkpoint-done"},
    {.event = TASK_EVENT_PAUSE, .name = "pausing"},
    {.event = TASK_EVENT_RESUME, .name = "resuming"},
    {.event = TASK_EVENT_HALT, .name = "halting"},
    {.event = TASK_EVENT_DROPPING, .name = "dropping"},
};

static TdThreadOnce streamTaskStateMachineInit = PTHREAD_ONCE_INIT;
static SArray*      streamTaskSMTrans = NULL;

static int32_t streamTaskInitStatus(SStreamTask* pTask);
static int32_t streamTaskKeepCurrentVerInWal(SStreamTask* pTask);
static int32_t initStateTransferTable();
static void    doInitStateTransferTable(void);

static STaskStateTrans createStateTransform(ETaskStatus current, ETaskStatus next, EStreamTaskEvent event,
                                            __state_trans_fn fn, __state_trans_succ_fn succFn,
                                            SFutureHandleEventInfo* pEventInfo);

static int32_t dummyFn(SStreamTask* UNUSED_PARAM(p)) { return TSDB_CODE_SUCCESS; }

static int32_t attachWaitedEvent(SStreamTask* pTask, SFutureHandleEventInfo* pEvtInfo) {
  char* p = streamTaskGetStatus(pTask).name;

  stDebug("s-task:%s status:%s attach event:%s required status:%s, since not allowed to handle it", pTask->id.idStr, p,
          GET_EVT_NAME(pEvtInfo->event), StreamTaskStatusList[pEvtInfo->status].name);

  SArray* pList = pTask->status.pSM->pWaitingEventList;

  void* px = taosArrayPush(pList, pEvtInfo);
  if (px == NULL) {
    stError("s-task:%s failed to add into waiting list, total waiting events:%d, code: out of memory", pTask->id.idStr,
            (int32_t)taosArrayGetSize(pList));
    return terrno;
  } else {
    stDebug("s-task:%s add into waiting list, total waiting events:%d", pTask->id.idStr,
            (int32_t)taosArrayGetSize(pList));
    return TSDB_CODE_SUCCESS;
  }
}

int32_t streamTaskInitStatus(SStreamTask* pTask) {
  pTask->execInfo.checkTs = taosGetTimestampMs();
  stDebug("s-task:%s start init, and check downstream tasks, set the init ts:%" PRId64, pTask->id.idStr,
          pTask->execInfo.checkTs);

  streamTaskSendCheckMsg(pTask);
  return 0;
}

int32_t streamTaskKeepCurrentVerInWal(SStreamTask* pTask) {
  if (!HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    stError("s-task:%s no related fill-history task, since it may have been dropped already", pTask->id.idStr);
  }

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    pTask->hTaskInfo.haltVer = walReaderGetCurrentVer(pTask->exec.pWalReader);
    if (pTask->hTaskInfo.haltVer == -1) {
      pTask->hTaskInfo.haltVer = pTask->dataRange.range.minVer;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// todo check rsp code for handle Event:TASK_EVENT_SCANHIST_DONE
static bool isInvalidStateTransfer(ETaskStatus state, const EStreamTaskEvent event) {
  if (event == TASK_EVENT_INIT || event == TASK_EVENT_INIT_SCANHIST) {
    return (state != TASK_STATUS__UNINIT);
  }

  if (event == TASK_EVENT_SCANHIST_DONE) {
    return (state != TASK_STATUS__SCAN_HISTORY);
  }

  if (event == TASK_EVENT_GEN_CHECKPOINT) {
    return (state != TASK_STATUS__READY);
  }

  if (event == TASK_EVENT_CHECKPOINT_DONE) {
    return (state != TASK_STATUS__CK);
  }

  // todo refactor later
  if (event == TASK_EVENT_RESUME) {
    return true;
  }

  if (event == TASK_EVENT_HALT) {
    if (state == TASK_STATUS__DROPPING || state == TASK_STATUS__UNINIT || state == TASK_STATUS__STOP ||
        state == TASK_STATUS__SCAN_HISTORY) {
      return true;
    }
  }

  return false;
}

// todo optimize the perf of find the trans objs by using hash table
static STaskStateTrans* streamTaskFindTransform(ETaskStatus state, const EStreamTaskEvent event) {
  int32_t numOfTrans = taosArrayGetSize(streamTaskSMTrans);
  for (int32_t i = 0; i < numOfTrans; ++i) {
    STaskStateTrans* pTrans = taosArrayGet(streamTaskSMTrans, i);
    if (pTrans == NULL) {
      continue;
    }

    if (pTrans->state.state == state && pTrans->event == event) {
      return pTrans;
    }
  }

  if (isInvalidStateTransfer(state, event)) {
    stError("invalid state transfer %d, handle event:%s", state, GET_EVT_NAME(event));
  }

  return NULL;
}

static int32_t doHandleWaitingEvent(SStreamTaskSM* pSM, const char* pEventName, SStreamTask* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t el = (taosGetTimestampMs() - pSM->startTs);
  stDebug("s-task:%s handle event:%s completed, elapsed time:%" PRId64 "ms state:%s -> %s", pTask->id.idStr,
          pEventName, el, pSM->prev.state.name, pSM->current.name);

  SFutureHandleEventInfo* pEvtInfo = taosArrayGet(pSM->pWaitingEventList, 0);
  if (pEvtInfo == NULL) {
    return terrno;
  }

  // OK, let's handle the waiting event, since the task has reached the required status now
  if (pSM->current.state == pEvtInfo->status) {
    stDebug("s-task:%s handle the event:%s in waiting list, state:%s", pTask->id.idStr,
            GET_EVT_NAME(pEvtInfo->event), pSM->current.name);

    // remove it
    void* px = taosArrayPop(pSM->pWaitingEventList);

    STaskStateTrans* pNextTrans = streamTaskFindTransform(pSM->current.state, pEvtInfo->event);

    pSM->pActiveTrans = pNextTrans;
    pSM->startTs = taosGetTimestampMs();
    streamMutexUnlock(&pTask->lock);

    code = pNextTrans->pAction(pSM->pTask);
    if (pNextTrans->autoInvokeEndFn) {
      return streamTaskOnHandleEventSuccess(pSM, pNextTrans->event, pEvtInfo->callBackFn, pEvtInfo->pParam);
    } else {
      return code;
    }
  } else {
    streamMutexUnlock(&pTask->lock);
    stDebug("s-task:%s state:%s event:%s in waiting list, req state:%s not fulfilled, put it back", pTask->id.idStr,
            pSM->current.name, GET_EVT_NAME(pEvtInfo->event),
            StreamTaskStatusList[pEvtInfo->status].name);
  }

  return code;
}

static int32_t removeEventInWaitingList(SStreamTask* pTask, EStreamTaskEvent event) {
  SStreamTaskSM* pSM = pTask->status.pSM;
  bool           removed = false;
  int32_t        num = taosArrayGetSize(pSM->pWaitingEventList);

  for (int32_t i = 0; i < num; ++i) {
    SFutureHandleEventInfo* pInfo = taosArrayGet(pSM->pWaitingEventList, i);
    if (pInfo == NULL) {
      continue;
    }

    if (pInfo->event == event) {
      taosArrayRemove(pSM->pWaitingEventList, i);
      stDebug("s-task:%s %s event in waiting list not be handled yet, remove it from waiting list, remaining events:%d",
              pTask->id.idStr, GET_EVT_NAME(pInfo->event), num - 1);
      removed = true;
      break;
    }
  }

  if (!removed) {
    stDebug("s-task:%s failed to remove event:%s in waiting list", pTask->id.idStr, GET_EVT_NAME(event));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskRestoreStatus(SStreamTask* pTask) {
  SStreamTaskSM* pSM = pTask->status.pSM;
  int32_t        code = 0;

  streamMutexLock(&pTask->lock);

  if (pSM->current.state == TASK_STATUS__PAUSE && pSM->pActiveTrans == NULL) {
    SStreamTaskState state = pSM->current;
    pSM->current = pSM->prev.state;

    pSM->prev.state = state;
    pSM->prev.evt = 0;

    pSM->startTs = taosGetTimestampMs();

    if (taosArrayGetSize(pSM->pWaitingEventList) > 0) {
      stDebug("s-task:%s restore status, %s -> %s, and then handle waiting event", pTask->id.idStr,
              pSM->prev.state.name, pSM->current.name);
      code = doHandleWaitingEvent(pSM, "restore-pause/halt", pTask);
    } else {
      stDebug("s-task:%s restore status, %s -> %s", pTask->id.idStr, pSM->prev.state.name, pSM->current.name);
    }
  } else {
    code = removeEventInWaitingList(pTask, TASK_EVENT_PAUSE);  // ignore the return value,
    if (code) {
      stError("s-task:%s failed to remove event in waiting list, code:%s", pTask->id.idStr, tstrerror(code));
    }

    code = TSDB_CODE_FAILED;  // failed to restore the status, since it is not in pause status
  }

  streamMutexUnlock(&pTask->lock);
  return code;
}

int32_t streamCreateStateMachine(SStreamTask* pTask) {
  int32_t code = initStateTransferTable();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  const char* id = pTask->id.idStr;

  SStreamTaskSM* pSM = taosMemoryCalloc(1, sizeof(SStreamTaskSM));
  if (pSM == NULL) {
    stError("s-task:%s failed to create task stateMachine, size:%d, code:%s", id, (int32_t)sizeof(SStreamTaskSM),
            tstrerror(terrno));
    return terrno;
  }

  pSM->pTask = pTask;
  pSM->pWaitingEventList = taosArrayInit(4, sizeof(SFutureHandleEventInfo));
  if (pSM->pWaitingEventList == NULL) {
    taosMemoryFree(pSM);
    stError("s-task:%s failed to create task stateMachine, size:%d, code:%s", id, (int32_t)sizeof(SStreamTaskSM),
            tstrerror(terrno));
    return terrno;
  }

  // set the initial state for the state-machine of stream task
  pSM->current = StreamTaskStatusList[TASK_STATUS__UNINIT];
  pSM->startTs = taosGetTimestampMs();

  pTask->status.pSM = pSM;
  return TSDB_CODE_SUCCESS;
}

void streamDestroyStateMachine(SStreamTaskSM* pSM) {
  if (pSM == NULL) {
    return;
  }

  taosArrayDestroy(pSM->pWaitingEventList);
  taosMemoryFree(pSM);
}

static int32_t doHandleEvent(SStreamTaskSM* pSM, EStreamTaskEvent event, STaskStateTrans* pTrans) {
  SStreamTask* pTask = pSM->pTask;
  const char*  id = pTask->id.idStr;
  int32_t      code = 0;

  if (pTrans->attachEvent.event != 0) {
    code = attachWaitedEvent(pTask, &pTrans->attachEvent);
    streamMutexUnlock(&pTask->lock);
    if (code) {
      return code;
    }

    while (1) {
      // wait for the task to be here
      streamMutexLock(&pTask->lock);
      ETaskStatus s = streamTaskGetStatus(pTask).state;
      streamMutexUnlock(&pTask->lock);

      if ((s == pTrans->next.state) && (pSM->prev.evt == pTrans->event)) {// this event has been handled already
        stDebug("s-task:%s attached event:%s handled", id, GET_EVT_NAME(pTrans->event));
        return TSDB_CODE_SUCCESS;
      } else if (s != TASK_STATUS__DROPPING && s != TASK_STATUS__STOP && s != TASK_STATUS__UNINIT) {
        stDebug("s-task:%s not handle event:%s yet, wait for 100ms and recheck", id, GET_EVT_NAME(event));
        taosMsleep(100);
      } else {
        stDebug("s-task:%s is dropped or stopped already, not wait.", id);
        return TSDB_CODE_STREAM_INVALID_STATETRANS;
      }
    }

  } else {  // override current active trans
    pSM->pActiveTrans = pTrans;
    pSM->startTs = taosGetTimestampMs();
    streamMutexUnlock(&pTask->lock);

    code = pTrans->pAction(pTask);

    if (pTrans->autoInvokeEndFn) {
      int32_t c = streamTaskOnHandleEventSuccess(pSM, event, NULL, NULL);
      if (code == TSDB_CODE_SUCCESS) {
        code = c;
      }
    }
  }

  return code;
}

static int32_t doHandleEventAsync(SStreamTaskSM* pSM, EStreamTaskEvent event, STaskStateTrans* pTrans, __state_trans_user_fn callbackFn, void* param) {
  SStreamTask* pTask = pSM->pTask;
  int32_t code = 0;

  if (pTrans->attachEvent.event != 0) {
    SFutureHandleEventInfo info = pTrans->attachEvent;
    info.pParam = param;
    info.callBackFn = callbackFn;

    code = attachWaitedEvent(pTask, &info);
    streamMutexUnlock(&pTask->lock);
  } else {  // override current active trans
    pSM->pActiveTrans = pTrans;
    pSM->startTs = taosGetTimestampMs();
    streamMutexUnlock(&pTask->lock);

    code = pTrans->pAction(pTask);
    // todo handle error code;

    if (pTrans->autoInvokeEndFn) {
      int32_t c = streamTaskOnHandleEventSuccess(pSM, event, callbackFn, param);
      if (code == TSDB_CODE_SUCCESS) {
        code = c;
      }
    }
  }

  return code;
}

int32_t streamTaskHandleEvent(SStreamTaskSM* pSM, EStreamTaskEvent event) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SStreamTask*     pTask = pSM->pTask;
  STaskStateTrans* pTrans = NULL;

  while (1) {
    streamMutexLock(&pTask->lock);

    if (pSM->pActiveTrans != NULL && pSM->pActiveTrans->autoInvokeEndFn) {
      EStreamTaskEvent evt = pSM->pActiveTrans->event;
      streamMutexUnlock(&pTask->lock);

      stDebug("s-task:%s status:%s handling event:%s by another thread, wait for 100ms and check if completed",
              pTask->id.idStr, pSM->current.name, GET_EVT_NAME(evt));
      taosMsleep(100);
    } else {
      // no active event trans exists, handle this event directly
      pTrans = streamTaskFindTransform(pSM->current.state, event);
      if (pTrans == NULL) {
        stDebug("s-task:%s failed to handle event:%s, status:%s", pTask->id.idStr, GET_EVT_NAME(event),
                pSM->current.name);
        streamMutexUnlock(&pTask->lock);
        return TSDB_CODE_STREAM_INVALID_STATETRANS;
      }

      if (pSM->pActiveTrans != NULL) {
        // not allowed concurrently initialization
        if (event == TASK_EVENT_INIT && pSM->pActiveTrans->event == TASK_EVENT_INIT) {
          streamMutexUnlock(&pTask->lock);
          stError("s-task:%s already in handling init procedure, handle this init event failed", pTask->id.idStr);
          return TSDB_CODE_STREAM_CONFLICT_EVENT;
        }

        // currently in some state transfer procedure, not auto invoke transfer, abort it
        stDebug("s-task:%s event:%s handle procedure quit, status %s -> %s failed, handle event %s now",
                pTask->id.idStr, GET_EVT_NAME(pSM->pActiveTrans->event), pSM->current.name,
                pSM->pActiveTrans->next.name, GET_EVT_NAME(event));
      }

      code = doHandleEvent(pSM, event, pTrans);
      break;
    }
  }

  return code;
}

int32_t streamTaskHandleEventAsync(SStreamTaskSM* pSM, EStreamTaskEvent event, __state_trans_user_fn callbackFn, void* param) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SStreamTask*     pTask = pSM->pTask;
  STaskStateTrans* pTrans = NULL;

  while (1) {
    streamMutexLock(&pTask->lock);

    if (pSM->pActiveTrans != NULL && pSM->pActiveTrans->autoInvokeEndFn) {
      EStreamTaskEvent evt = pSM->pActiveTrans->event;
      streamMutexUnlock(&pTask->lock);

      stDebug("s-task:%s status:%s handling event:%s by some other thread, wait for 100ms and check if completed",
              pTask->id.idStr, pSM->current.name, GET_EVT_NAME(evt));
      taosMsleep(100);
    } else {
      // no active event trans exists, handle this event directly
      pTrans = streamTaskFindTransform(pSM->current.state, event);
      if (pTrans == NULL) {
        stDebug("s-task:%s failed to handle event:%s, status:%s", pTask->id.idStr, GET_EVT_NAME(event), pSM->current.name);
        streamMutexUnlock(&pTask->lock);
        return TSDB_CODE_STREAM_INVALID_STATETRANS;
      }

      if (pSM->pActiveTrans != NULL) {
        // currently in some state transfer procedure, not auto invoke transfer, quit from this procedure
        stDebug("s-task:%s event:%s handle procedure quit, status %s -> %s failed, handle event %s now",
                pTask->id.idStr, GET_EVT_NAME(pSM->pActiveTrans->event), pSM->current.name,
                pSM->pActiveTrans->next.name, GET_EVT_NAME(event));
      }

      code = doHandleEventAsync(pSM, event, pTrans, callbackFn, param);
      break;
    }
  }

  return code;
}

static void keepPrevInfo(SStreamTaskSM* pSM) {
  STaskStateTrans* pTrans = pSM->pActiveTrans;

  // we only keep the latest pause state
  if (pSM->prev.state.state == TASK_STATUS__PAUSE && pSM->current.state == TASK_STATUS__PAUSE) {
    return;
  }

  pSM->prev.state = pSM->current;
  pSM->prev.evt = pTrans->event;
}

int32_t streamTaskOnHandleEventSuccess(SStreamTaskSM* pSM, EStreamTaskEvent event, __state_trans_user_fn callbackFn, void* param) {
  SStreamTask* pTask = pSM->pTask;
  const char*  id = pTask->id.idStr;
  int32_t      code = 0;

  // do update the task status
  streamMutexLock(&pTask->lock);

  STaskStateTrans* pTrans = pSM->pActiveTrans;
  if (pTrans == NULL) {
    ETaskStatus s = pSM->current.state;

    if (s != TASK_STATUS__DROPPING && s != TASK_STATUS__PAUSE && s != TASK_STATUS__STOP && s != TASK_STATUS__UNINIT &&
        s != TASK_STATUS__READY) {
      stError("s-task:%s invalid task status:%s on handling event:%s success", id, pSM->current.name,
              GET_EVT_NAME(pSM->prev.evt));
    }

    // the pSM->prev.evt may be 0, so print string is not appropriate.
    stDebug("s-task:%s event:%s handled failed, current status:%s, trigger event:%s", id, GET_EVT_NAME(event),
            pSM->current.name, GET_EVT_NAME(pSM->prev.evt));

    streamMutexUnlock(&pTask->lock);
    return TSDB_CODE_STREAM_INVALID_STATETRANS;
  }

  if (pTrans->event != event) {
    stWarn("s-task:%s handle event:%s failed, current status:%s, active trans evt:%s", id, GET_EVT_NAME(event),
           pSM->current.name, GET_EVT_NAME(pTrans->event));
    streamMutexUnlock(&pTask->lock);
    return TSDB_CODE_STREAM_INVALID_STATETRANS;
  }

  keepPrevInfo(pSM);

  pSM->current = pTrans->next;
  pSM->pActiveTrans = NULL;

  // todo remove it
  // todo: handle the error code
  // on success callback, add into lock if necessary, or maybe we should add an option for this?
  code = pTrans->pSuccAction(pTask);

  streamMutexUnlock(&pTask->lock);

  // todo: add parameter to control lock
  // after handling the callback function assigned by invoker, go on handling the waiting tasks
  if (callbackFn != NULL) {
    stDebug("s-task:%s start to handle user-specified callback fn for event:%s", id, GET_EVT_NAME(pTrans->event));
    int32_t ret = callbackFn(pSM->pTask, param);
    if (ret != TSDB_CODE_SUCCESS) {
      // todo  handle error
    }

    stDebug("s-task:%s handle user-specified callback fn for event:%s completed", id, GET_EVT_NAME(pTrans->event));
  }

  streamMutexLock(&pTask->lock);

  // tasks in waiting list
  if (taosArrayGetSize(pSM->pWaitingEventList) > 0) {
    code = doHandleWaitingEvent(pSM, GET_EVT_NAME(pTrans->event), pTask);
  } else {
    streamMutexUnlock(&pTask->lock);

    int64_t el = (taosGetTimestampMs() - pSM->startTs);
    stDebug("s-task:%s handle event:%s completed, elapsed time:%" PRId64 "ms state:%s -> %s", id,
            GET_EVT_NAME(pTrans->event), el, pSM->prev.state.name, pSM->current.name);
  }

  return code;
}

SStreamTaskState streamTaskGetStatus(const SStreamTask* pTask) {
  return pTask->status.pSM->current;  // copy one obj in case of multi-thread environment
}

ETaskStatus streamTaskGetPrevStatus(const SStreamTask* pTask) {
  return pTask->status.pSM->prev.state.state;
}

const char* streamTaskGetStatusStr(ETaskStatus status) {
  int32_t index = status;
  if (index < 0 || index > tListLen(StreamTaskStatusList)) {
    return "";
  }

  return StreamTaskStatusList[status].name;
}

void streamTaskResetStatus(SStreamTask* pTask) {
  SStreamTaskSM* pSM = pTask->status.pSM;

  streamMutexLock(&pTask->lock);
  stDebug("s-task:%s level:%d fill-history:%d vgId:%d set uninit, prev status:%s", pTask->id.idStr,
          pTask->info.taskLevel, pTask->info.fillHistory, pTask->pMeta->vgId, pSM->current.name);

  pSM->current = StreamTaskStatusList[TASK_STATUS__UNINIT];
  pSM->pActiveTrans = NULL;
  taosArrayClear(pSM->pWaitingEventList);
  streamMutexUnlock(&pTask->lock);

  // clear the downstream ready status
  pTask->status.downstreamReady = 0;
}

void streamTaskSetStatusReady(SStreamTask* pTask) {
  SStreamTaskSM* pSM = pTask->status.pSM;
  if (pSM->current.state == TASK_STATUS__DROPPING) {
    stError("s-task:%s task in dropping state, cannot be set ready", pTask->id.idStr);
    return;
  }

  pSM->prev.state = pSM->current;
  pSM->prev.evt = 0;

  pSM->current = StreamTaskStatusList[TASK_STATUS__READY];
  pSM->startTs = taosGetTimestampMs();
  pSM->pActiveTrans = NULL;
  taosArrayClear(pSM->pWaitingEventList);
}

STaskStateTrans createStateTransform(ETaskStatus current, ETaskStatus next, EStreamTaskEvent event, __state_trans_fn fn,
                                     __state_trans_succ_fn succFn, SFutureHandleEventInfo* pEventInfo) {
  STaskStateTrans trans = {0};
  trans.state = StreamTaskStatusList[current];
  trans.next = StreamTaskStatusList[next];
  trans.event = event;

  if (pEventInfo != NULL) {
    trans.attachEvent = *pEventInfo;
  } else {
    trans.attachEvent.event = 0;
    trans.attachEvent.status = 0;
  }

  trans.pAction = (fn != NULL) ? fn : dummyFn;
  trans.pSuccAction = (succFn != NULL) ? succFn : dummyFn;
  trans.autoInvokeEndFn = (fn == NULL);
  return trans;
}

int32_t initStateTransferTable() {
  return taosThreadOnce(&streamTaskStateMachineInit, doInitStateTransferTable);
}

//clang-format off
void doInitStateTransferTable(void) {
  streamTaskSMTrans = taosArrayInit(8, sizeof(STaskStateTrans));

  // initialization event handle
  STaskStateTrans trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__READY, TASK_EVENT_INIT, streamTaskInitStatus, streamTaskOnNormalTaskReady, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));

  trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__SCAN_HISTORY, TASK_EVENT_INIT_SCANHIST, streamTaskInitStatus, streamTaskOnScanHistoryTaskReady, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));

  // scan-history related event
  trans = createStateTransform(TASK_STATUS__SCAN_HISTORY, TASK_STATUS__READY, TASK_EVENT_SCANHIST_DONE, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));

  // halt stream task, from other task status
  trans = createStateTransform(TASK_STATUS__READY, TASK_STATUS__HALT, TASK_EVENT_HALT, NULL, streamTaskKeepCurrentVerInWal, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__HALT, TASK_STATUS__HALT, TASK_EVENT_HALT, NULL, streamTaskKeepCurrentVerInWal, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));

  SFutureHandleEventInfo info = {.status = TASK_STATUS__READY, .event = TASK_EVENT_HALT};

  trans = createStateTransform(TASK_STATUS__CK, TASK_STATUS__HALT, TASK_EVENT_HALT, NULL, streamTaskKeepCurrentVerInWal, &info);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__PAUSE, TASK_STATUS__HALT, TASK_EVENT_HALT, NULL, streamTaskKeepCurrentVerInWal, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));

  // checkpoint related event
  trans = createStateTransform(TASK_STATUS__READY, TASK_STATUS__CK, TASK_EVENT_GEN_CHECKPOINT, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__HALT, TASK_STATUS__CK, TASK_EVENT_GEN_CHECKPOINT, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__CK, TASK_STATUS__READY, TASK_EVENT_CHECKPOINT_DONE, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));

  // pause & resume related event handle
  trans = createStateTransform(TASK_STATUS__READY, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__SCAN_HISTORY, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));

  info = (SFutureHandleEventInfo){.status = TASK_STATUS__READY, .event = TASK_EVENT_PAUSE};
  trans = createStateTransform(TASK_STATUS__CK, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, NULL, &info);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__HALT, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, NULL, &info);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, NULL, &info);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));

  trans = createStateTransform(TASK_STATUS__PAUSE, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__STOP, TASK_STATUS__STOP, TASK_EVENT_PAUSE, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__DROPPING, TASK_STATUS__DROPPING, TASK_EVENT_PAUSE, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));

  // resume is completed by restore status of state-machine

  // stop related event
  trans = createStateTransform(TASK_STATUS__READY, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__DROPPING, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__STOP, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__SCAN_HISTORY, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__HALT, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__PAUSE, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__CK, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));

  // dropping related event
  trans = createStateTransform(TASK_STATUS__READY, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__DROPPING, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__STOP, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__SCAN_HISTORY, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__HALT, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__PAUSE, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
  trans = createStateTransform(TASK_STATUS__CK, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL);
  CHECK_RET_VAL(taosArrayPush(streamTaskSMTrans, &trans));
}
//clang-format on
