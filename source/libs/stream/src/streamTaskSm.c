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

#include <streamsm.h>
#include "streamInt.h"
#include "streamsm.h"
#include "tmisce.h"
#include "tstream.h"
#include "ttimer.h"
#include "wal.h"

SStreamTaskState StreamTaskStatusList[9] = {
    {.state = TASK_STATUS__READY, .name = "ready"},
    {.state = TASK_STATUS__DROPPING, .name = "dropped"},
    {.state = TASK_STATUS__UNINIT, .name = "uninit"},
    {.state = TASK_STATUS__STOP, .name = "stop"},
    {.state = TASK_STATUS__SCAN_HISTORY, .name = "scan-history"},
    {.state = TASK_STATUS__HALT, .name = "halt"},
    {.state = TASK_STATUS__PAUSE, .name = "paused"},
    {.state = TASK_STATUS__CK, .name = "checkpoint"},
    {.state = TASK_STATUS__STREAM_SCAN_HISTORY, .name = "stream-scan-history"},
};

SStreamEventInfo StreamTaskEventList[12] = {
    {.event = 0, .name = ""},  // dummy event, place holder
    {.event = TASK_EVENT_INIT, .name = "initialize"},
    {.event = TASK_EVENT_INIT_SCANHIST, .name = "scan-history-init"},
    {.event = TASK_EVENT_INIT_STREAM_SCANHIST, .name = "stream-scan-history-init"},
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
                                            SAttachedEventInfo* pEventInfo, bool autoInvoke);

static int32_t dummyFn(SStreamTask* UNUSED_PARAM(p)) { return TSDB_CODE_SUCCESS; }

static int32_t attachEvent(SStreamTask* pTask, SAttachedEventInfo* pEvtInfo) {
  char* p = NULL;
  streamTaskGetStatus(pTask, &p);

  stDebug("s-task:%s status:%s attach event:%s required status:%s, since not allowed to handle it", pTask->id.idStr, p,
          StreamTaskEventList[pEvtInfo->event].name, StreamTaskStatusList[pEvtInfo->status].name);
  taosArrayPush(pTask->status.pSM->pWaitingEventList, pEvtInfo);
  return 0;
}

int32_t streamTaskInitStatus(SStreamTask* pTask) {
  pTask->execInfo.init = taosGetTimestampMs();

  stDebug("s-task:%s start init, and check downstream tasks, set the init ts:%" PRId64, pTask->id.idStr,
          pTask->execInfo.init);
  streamTaskCheckDownstream(pTask);
  return 0;
}

int32_t streamTaskSetReadyForWal(SStreamTask* pTask) {
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    stDebug("s-task:%s ready for extract data from wal", pTask->id.idStr);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t streamTaskDoCheckpoint(SStreamTask* pTask) {
  stDebug("s-task:%s start to do checkpoint", pTask->id.idStr);
  return 0;
}

int32_t streamTaskKeepCurrentVerInWal(SStreamTask* pTask) {
  ASSERT(HAS_RELATED_FILLHISTORY_TASK(pTask));

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    pTask->hTaskInfo.haltVer = walReaderGetCurrentVer(pTask->exec.pWalReader);
    if (pTask->hTaskInfo.haltVer == -1) {
      pTask->hTaskInfo.haltVer = pTask->dataRange.range.maxVer + 1;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// todo optimize the perf of find the trans objs by using hash table
static STaskStateTrans* streamTaskFindTransform(const SStreamTaskSM* pState, const EStreamTaskEvent event) {
  int32_t numOfTrans = taosArrayGetSize(streamTaskSMTrans);
  for (int32_t i = 0; i < numOfTrans; ++i) {
    STaskStateTrans* pTrans = taosArrayGet(streamTaskSMTrans, i);
    if (pTrans->state.state == pState->current.state && pTrans->event == event) {
      return pTrans;
    }
  }

  if (event == TASK_EVENT_CHECKPOINT_DONE && pState->current.state == TASK_STATUS__STOP) {

  } else {
    ASSERT(0);
  }
  return NULL;
}

void streamTaskRestoreStatus(SStreamTask* pTask) {
  SStreamTaskSM* pSM = pTask->status.pSM;
  taosThreadMutexLock(&pTask->lock);
  ASSERT(pSM->pActiveTrans == NULL);

  ASSERT(pSM->current.state == TASK_STATUS__PAUSE || pSM->current.state == TASK_STATUS__HALT);

  SStreamTaskState state = pSM->current;
  pSM->current = pSM->prev.state;

  pSM->prev.state = state;
  pSM->prev.evt = 0;

  pSM->startTs = taosGetTimestampMs();

  taosThreadMutexUnlock(&pTask->lock);
  stDebug("s-task:%s restore status, %s -> %s", pTask->id.idStr, pSM->prev.state.name, pSM->current.name);
}

SStreamTaskSM* streamCreateStateMachine(SStreamTask* pTask) {
  initStateTransferTable();
  const char* id = pTask->id.idStr;

  SStreamTaskSM* pSM = taosMemoryCalloc(1, sizeof(SStreamTaskSM));
  if (pSM == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    stError("s-task:%s failed to create task stateMachine, size:%d, code:%s", id, (int32_t)sizeof(SStreamTaskSM),
            tstrerror(terrno));
    return NULL;
  }

  pSM->pTask = pTask;
  pSM->pWaitingEventList = taosArrayInit(4, sizeof(SAttachedEventInfo));
  if (pSM->pWaitingEventList == NULL) {
    taosMemoryFree(pSM);

    terrno = TSDB_CODE_OUT_OF_MEMORY;
    stError("s-task:%s failed to create task stateMachine, size:%d, code:%s", id, (int32_t)sizeof(SStreamTaskSM),
            tstrerror(terrno));
    return NULL;
  }

  // set the initial state for the state-machine of stream task
  pSM->current = StreamTaskStatusList[TASK_STATUS__UNINIT];
  pSM->startTs = taosGetTimestampMs();
  return pSM;
}

void* streamDestroyStateMachine(SStreamTaskSM* pSM) {
  if (pSM == NULL) {
    return NULL;
  }

  taosArrayDestroy(pSM->pWaitingEventList);
  taosMemoryFree(pSM);
  return NULL;
}

int32_t streamTaskHandleEvent(SStreamTaskSM* pSM, EStreamTaskEvent event) {
  SStreamTask* pTask = pSM->pTask;

  taosThreadMutexLock(&pTask->lock);

  STaskStateTrans* pTrans = streamTaskFindTransform(pSM, event);
  if (pTrans == NULL) {
    stWarn("s-task:%s status:%s not allowed handle event:%s", pTask->id.idStr, pSM->current.name, StreamTaskEventList[event].name);
    return -1;
  } else {
    stDebug("s-task:%s start to handle event:%s, state:%s", pTask->id.idStr, StreamTaskEventList[event].name,
            pSM->current.name);
  }

  if (pTrans->attachEvent.event != 0) {
    attachEvent(pTask, &pTrans->attachEvent);
    taosThreadMutexUnlock(&pTask->lock);

    while (1) {
      // wait for the task to be here
      taosThreadMutexLock(&pTask->lock);
      ETaskStatus s = streamTaskGetStatus(pTask, NULL);
      taosThreadMutexUnlock(&pTask->lock);

      if ((s == pTrans->next.state) && (pSM->prev.evt == pTrans->event)) {
        stDebug("s-task:%s attached event:%s handled", pTask->id.idStr, StreamTaskEventList[pTrans->event].name);
        return TSDB_CODE_SUCCESS;
      } else {// this event has been handled already
        stDebug("s-task:%s not handle event:%s yet, wait for 100ms and recheck", pTask->id.idStr,
                StreamTaskEventList[event].name);
        taosMsleep(100);
      }
    }

  } else {
    if (pSM->pActiveTrans != NULL) {
      ASSERT(!pSM->pActiveTrans->autoInvokeEndFn);
      stWarn("s-task:%s status:%s handle event:%s is interrupted, handle the new event:%s", pTask->id.idStr,
             pSM->current.name, StreamTaskEventList[pSM->pActiveTrans->event].name, StreamTaskEventList[event].name);
    }

    pSM->pActiveTrans = pTrans;
    pSM->startTs = taosGetTimestampMs();
    taosThreadMutexUnlock(&pTask->lock);

    int32_t code = pTrans->pAction(pTask);
    // todo handle error code;

    if (pTrans->autoInvokeEndFn) {
      streamTaskOnHandleEventSuccess(pSM);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void keepPrevInfo(SStreamTaskSM* pSM) {
  STaskStateTrans* pTrans = pSM->pActiveTrans;

  pSM->prev.state = pSM->current;
  pSM->prev.evt = pTrans->event;
}

int32_t streamTaskOnHandleEventSuccess(SStreamTaskSM* pSM) {
  SStreamTask* pTask = pSM->pTask;

  // do update the task status
  taosThreadMutexLock(&pTask->lock);
  STaskStateTrans* pTrans = pSM->pActiveTrans;

  if (pTrans == NULL) {
    ETaskStatus s = pSM->current.state;
    ASSERT(s == TASK_STATUS__DROPPING || s == TASK_STATUS__PAUSE || s == TASK_STATUS__STOP);
    // the pSM->prev.evt may be 0, so print string is not appropriate.
    stDebug("status not handled success, current status:%s, trigger event:%d, %s", pSM->current.name, pSM->prev.evt,
        pTask->id.idStr);

    taosThreadMutexUnlock(&pTask->lock);
    return TSDB_CODE_INVALID_PARA;
  }

  keepPrevInfo(pSM);

  pSM->current = pTrans->next;
  pSM->pActiveTrans = NULL;

  // on success callback, add into lock if necessary, or maybe we should add an option for this?
  pTrans->pSuccAction(pTask);

  if (taosArrayGetSize(pSM->pWaitingEventList) > 0) {
    int64_t el = (taosGetTimestampMs() - pSM->startTs);
    stDebug("s-task:%s handle event:%s completed, elapsed time:%" PRId64 "ms state:%s -> %s", pTask->id.idStr,
            StreamTaskEventList[pTrans->event].name, el, pSM->prev.state.name, pSM->current.name);

    SAttachedEventInfo* pEvtInfo = taosArrayPop(pSM->pWaitingEventList);

    // OK, let's handle the attached event, since the task has reached the required status now
    if (pSM->current.state == pEvtInfo->status) {
      stDebug("s-task:%s handle the attached event:%s, state:%s", pTask->id.idStr,
              StreamTaskEventList[pEvtInfo->event].name, pSM->current.name);

      STaskStateTrans* pNextTrans = streamTaskFindTransform(pSM, pEvtInfo->event);
      ASSERT(pSM->pActiveTrans == NULL && pNextTrans != NULL);

      pSM->pActiveTrans = pNextTrans;
      pSM->startTs = taosGetTimestampMs();
      taosThreadMutexUnlock(&pTask->lock);

      int32_t code = pNextTrans->pAction(pSM->pTask);
      if (pNextTrans->autoInvokeEndFn) {
        return streamTaskOnHandleEventSuccess(pSM);
      } else {
        return code;
      }
    }
  } else {
    taosThreadMutexUnlock(&pTask->lock);

    int64_t el = (taosGetTimestampMs() - pSM->startTs);
    stDebug("s-task:%s handle event:%s completed, elapsed time:%" PRId64 "ms state:%s -> %s", pTask->id.idStr,
            StreamTaskEventList[pTrans->event].name, el, pSM->prev.state.name, pSM->current.name);
  }

  return TSDB_CODE_SUCCESS;
}

ETaskStatus streamTaskGetStatus(const SStreamTask* pTask, char** pStr) {
  SStreamTaskState s = pTask->status.pSM->current;  // copy one obj in case of multi-thread environment
  if (pStr != NULL) {
    *pStr = s.name;
  }
  return s.state;
}

const char* streamTaskGetStatusStr(ETaskStatus status) {
  return StreamTaskStatusList[status].name;
}

void streamTaskResetStatus(SStreamTask* pTask) {
  SStreamTaskSM* pSM = pTask->status.pSM;

  taosThreadMutexLock(&pTask->lock);
  pSM->current = StreamTaskStatusList[TASK_STATUS__UNINIT];
  pSM->pActiveTrans = NULL;
  taosArrayClear(pSM->pWaitingEventList);
  taosThreadMutexUnlock(&pTask->lock);

  // clear the downstream ready status
  pTask->status.downstreamReady = 0;
}

void streamTaskSetStatusReady(SStreamTask* pTask) {
  SStreamTaskSM* pSM = pTask->status.pSM;
  if (pSM->current.state == TASK_STATUS__DROPPING) {
    stError("s-task:%s task in dropping state, cannot be set ready", pTask->id.idStr);
    return;
  }

  taosThreadMutexLock(&pTask->lock);

  pSM->prev.state = pSM->current;
  pSM->prev.evt = 0;

  pSM->current = StreamTaskStatusList[TASK_STATUS__READY];
  pSM->startTs = taosGetTimestampMs();
  pSM->pActiveTrans = NULL;
  taosArrayClear(pSM->pWaitingEventList);

  taosThreadMutexUnlock(&pTask->lock);
}

STaskStateTrans createStateTransform(ETaskStatus current, ETaskStatus next, EStreamTaskEvent event, __state_trans_fn fn,
                                     __state_trans_succ_fn succFn, SAttachedEventInfo* pEventInfo, bool autoInvoke) {
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
  trans.autoInvokeEndFn = autoInvoke;
  return trans;
}

int32_t initStateTransferTable() {
  taosThreadOnce(&streamTaskStateMachineInit, doInitStateTransferTable);
  return TSDB_CODE_SUCCESS;
}

void doInitStateTransferTable(void) {
  streamTaskSMTrans = taosArrayInit(8, sizeof(STaskStateTrans));

  // initialization event handle
  STaskStateTrans trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__READY, TASK_EVENT_INIT,
                                               streamTaskInitStatus, onNormalTaskReady, false, false);
  taosArrayPush(streamTaskSMTrans, &trans);

  trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__SCAN_HISTORY, TASK_EVENT_INIT_SCANHIST,
                               streamTaskInitStatus, onScanhistoryTaskReady, false, false);
  taosArrayPush(streamTaskSMTrans, &trans);

  trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__STREAM_SCAN_HISTORY, TASK_EVENT_INIT_STREAM_SCANHIST,
                               streamTaskInitStatus, onScanhistoryTaskReady, false, false);
  taosArrayPush(streamTaskSMTrans, &trans);

  // scan-history related event
  trans = createStateTransform(TASK_STATUS__SCAN_HISTORY, TASK_STATUS__READY, TASK_EVENT_SCANHIST_DONE,
                               streamTaskSetReadyForWal, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);

  trans = createStateTransform(TASK_STATUS__STREAM_SCAN_HISTORY, TASK_STATUS__READY, TASK_EVENT_SCANHIST_DONE,
                               streamTaskSetReadyForWal, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);

  // halt stream task, from other task status
  trans = createStateTransform(TASK_STATUS__READY, TASK_STATUS__HALT, TASK_EVENT_HALT, NULL,
                               streamTaskKeepCurrentVerInWal, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);

  SAttachedEventInfo info = {.status = TASK_STATUS__READY, .event = TASK_EVENT_HALT};
  trans = createStateTransform(TASK_STATUS__STREAM_SCAN_HISTORY, TASK_STATUS__HALT, TASK_EVENT_HALT, NULL,
                               streamTaskKeepCurrentVerInWal, &info, true);
  taosArrayPush(streamTaskSMTrans, &trans);

  trans = createStateTransform(TASK_STATUS__CK, TASK_STATUS__HALT, TASK_EVENT_HALT, NULL, streamTaskKeepCurrentVerInWal,
                               &info, true);
  taosArrayPush(streamTaskSMTrans, &trans);

  trans = createStateTransform(TASK_STATUS__PAUSE, TASK_STATUS__HALT, TASK_EVENT_HALT, NULL,
                               streamTaskKeepCurrentVerInWal, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);

  // checkpoint related event
  trans = createStateTransform(TASK_STATUS__READY, TASK_STATUS__CK, TASK_EVENT_GEN_CHECKPOINT, NULL,
                               streamTaskDoCheckpoint, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);

  trans =
      createStateTransform(TASK_STATUS__CK, TASK_STATUS__READY, TASK_EVENT_CHECKPOINT_DONE, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);

  // pause & resume related event handle
  trans = createStateTransform(TASK_STATUS__READY, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__SCAN_HISTORY, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);

  info = (SAttachedEventInfo){.status = TASK_STATUS__READY, .event = TASK_EVENT_PAUSE};
  trans = createStateTransform(TASK_STATUS__STREAM_SCAN_HISTORY, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, NULL, &info, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__CK, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, NULL, &info, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__HALT, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, NULL, &info, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, NULL, &info, true);
  taosArrayPush(streamTaskSMTrans, &trans);

  trans = createStateTransform(TASK_STATUS__PAUSE, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__STOP, TASK_STATUS__STOP, TASK_EVENT_PAUSE, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__DROPPING, TASK_STATUS__DROPPING, TASK_EVENT_PAUSE, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);

  // resume is completed by restore status of state-machine

  // stop related event
  trans = createStateTransform(TASK_STATUS__READY, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__DROPPING, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__STOP, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__SCAN_HISTORY, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__HALT, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__PAUSE, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__CK, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__STREAM_SCAN_HISTORY, TASK_STATUS__STOP, TASK_EVENT_STOP, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);

  // dropping related event
  trans = createStateTransform(TASK_STATUS__READY, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__DROPPING, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__STOP, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__SCAN_HISTORY, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__HALT, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__PAUSE, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__CK, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
  trans = createStateTransform(TASK_STATUS__STREAM_SCAN_HISTORY, TASK_STATUS__DROPPING, TASK_EVENT_DROPPING, NULL, NULL, NULL, true);
  taosArrayPush(streamTaskSMTrans, &trans);
}