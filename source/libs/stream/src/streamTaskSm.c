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
#include "tmisce.h"
#include "tstream.h"
#include "ttimer.h"
#include "wal.h"
#include "streamsm.h"

SStreamTaskState StreamTaskStatusList[8] = {
    {.state = TASK_STATUS__NORMAL, .name = "normal"},
    {.state = TASK_STATUS__DROPPING, .name = "dropping"},
    {.state = TASK_STATUS__UNINIT, .name = "uninit"},
    {.state = TASK_STATUS__STOP, .name = "stop"},
    {.state = TASK_STATUS__SCAN_HISTORY, .name = "scan-history"},
    {.state = TASK_STATUS__HALT, .name = "halt"},
    {.state = TASK_STATUS__PAUSE, .name = "paused"},
    {.state = TASK_STATUS__CK, .name = "checkpoint"},
};

SStreamEventInfo StreamTaskEventList[8] = {
    {}, // dummy event, place holder
    {.event = TASK_EVENT_INIT, .name = "initialize"},
    {.event = TASK_EVENT_INIT_SCAN_HISTORY, .name = "scan-history-initialize"},
    {.event = TASK_EVENT_SCANHIST_COMPLETED, .name = "scan-history-completed"},
};

static int32_t         initStateTransferTable(SStreamTaskSM* pSM);
static STaskStateTrans createStateTransform(ETaskStatus current, ETaskStatus next, EStreamTaskEvent event,
                                            __state_trans_fn preFn, __state_trans_fn fn, __state_trans_succ_fn succFn,
                                            bool autoInvoke);
static int32_t streamTaskInitStatus(SStreamTask* pTask);
static int32_t streamTaskKeepCurrentVerInWal(SStreamTask* pTask);

static int32_t dummyFn(SStreamTask* UNUSED_PARAM(p)) { return TSDB_CODE_SUCCESS; }

int32_t streamTaskInitStatus(SStreamTask* pTask) {
  pTask->execInfo.init = taosGetTimestampMs();

  stDebug("s-task:%s start init, and check downstream tasks, set the init ts:%" PRId64, pTask->id.idStr,
          pTask->execInfo.init);
  streamTaskCheckDownstream(pTask);
  return 0;
}

int32_t streamTaskSetReadyForWal(SStreamTask* pTask) {
  stDebug("s-task:%s ready for extract data from wal", pTask->id.idStr);
  streamSetStatusNormal(pTask); // todo remove it
  return TSDB_CODE_SUCCESS;
}

static int32_t streamTaskDoPause(SStreamTask* pTask) {
  stDebug("s-task:%s start to pause tasks", pTask->id.idStr);
  return 0;
}

static int32_t streamTaskDoResume(SStreamTask* pTask) {
  stDebug("s-task:%s start to resume tasks", pTask->id.idStr);
  return 0;
}
static int32_t streamTaskDoCheckpoint(SStreamTask* pTask) {
  stDebug("s-task:%s start to do checkpoint", pTask->id.idStr);
  return 0;
}

int32_t streamTaskWaitBeforeHalt(SStreamTask* pTask) {
  char* p = NULL;
  while (streamTaskGetStatus(pTask, &p) != TASK_STATUS__NORMAL) {
    stDebug("related stream task:%s(status:%s) not ready for halt, wait for 100ms and retry", pTask->id.idStr, p);
    taosMsleep(100);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskKeepCurrentVerInWal(SStreamTask* pTask) {
  ASSERT(HAS_RELATED_FILLHISTORY_TASK(pTask));

  pTask->hTaskInfo.haltVer = walReaderGetCurrentVer(pTask->exec.pWalReader);
  if (pTask->hTaskInfo.haltVer == -1) {
    pTask->hTaskInfo.haltVer = pTask->dataRange.range.maxVer + 1;
  }
  return TSDB_CODE_SUCCESS;
}

// todo optimize the perf of find the trans objs by using hash table
static STaskStateTrans* streamTaskFindTransform(const SStreamTaskSM* pState, const EStreamTaskEvent event) {
  int32_t numOfTrans = taosArrayGetSize(pState->pTransList);
  for(int32_t i = 0; i < numOfTrans; ++i) {
    STaskStateTrans* pTrans = taosArrayGet(pState->pTransList, i);
    if (pTrans->state.state == pState->current.state && pTrans->event == event) {
      return pTrans;
    }
  }

  ASSERT(0);
  return NULL;
}

void streamTaskRestoreStatus(SStreamTask* pTask) {
  SStreamTaskSM* pSM = pTask->status.pSM;
  taosThreadMutexLock(&pTask->lock);
  ASSERT(pSM->pActiveTrans == NULL);

  SStreamTaskState state = pSM->current;
  pSM->current = pSM->prev;
  pSM->prev = state;
  pSM->startTs = taosGetTimestampMs();

  taosThreadMutexUnlock(&pTask->lock);
  stDebug("s-task:%s restore status, %s -> %s", pTask->id.idStr, pSM->prev.name, pSM->current.name);
}

SStreamTaskSM* streamCreateStateMachine(SStreamTask* pTask) {
  SStreamTaskSM* pSM = taosMemoryCalloc(1, sizeof(SStreamTaskSM));
  if (pSM == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pSM->pTask = pTask;

  // set the initial state for the state-machine of stream task
  pSM->current = StreamTaskStatusList[TASK_STATUS__UNINIT];
  pSM->startTs = taosGetTimestampMs();
  int32_t code = initStateTransferTable(pSM);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pSM);
    return NULL;
  }
  return pSM;
}

int32_t streamTaskHandleEvent(SStreamTaskSM* pSM, EStreamTaskEvent event) {
  STaskStateTrans* pTrans = streamTaskFindTransform(pSM, event);
  stDebug("s-task:%s start to handle event:%s, state:%s", pSM->pTask->id.idStr, StreamTaskEventList[event].name,
          pSM->current.name);

  int32_t code = pTrans->preAction(pSM->pTask);

  taosThreadMutexLock(&pSM->pTask->lock);
  ASSERT(pSM->pActiveTrans == NULL);
  pSM->pActiveTrans = pTrans;
  pSM->startTs = taosGetTimestampMs();
  taosThreadMutexUnlock(&pSM->pTask->lock);

  code = pTrans->pAction(pSM->pTask);
  // todo handle error code;

  if (pTrans->autoInvokeEndFn) {
    streamTaskOnHandleEventSuccess(pSM);
  }

  return code;
}

int32_t streamTaskOnHandleEventSuccess(SStreamTaskSM* pSM) {
  STaskStateTrans* pTrans = pSM->pActiveTrans;
  SStreamTask*     pTask = pSM->pTask;

  // do update the task status
  taosThreadMutexLock(&pTask->lock);
  SStreamTaskState current = pSM->current;

  pSM->prev = pSM->current;
  pSM->current = pTrans->next;
  pSM->pActiveTrans = NULL;

  // on success callback, add into lock if necessary, or maybe we should add an option for this?
  pTrans->pSuccAction(pTask);
  taosThreadMutexUnlock(&pTask->lock);

  int64_t el = (taosGetTimestampMs() - pSM->startTs);
  stDebug("s-task:%s handle event:%s completed, elapsed time:%" PRId64 "ms state:%s -> %s", pTask->id.idStr,
          StreamTaskEventList[pTrans->event].name, el, current.name, pSM->current.name);
  return TSDB_CODE_SUCCESS;
}

ETaskStatus streamTaskGetStatus(SStreamTask* pTask, char** pStr) {
  SStreamTaskState s = pTask->status.pSM->current;  // copy one obj in case of multi-thread environment
  if (pStr != NULL) {
    *pStr = s.name;
  }
  return s.state;
}

STaskStateTrans createStateTransform(ETaskStatus current, ETaskStatus next, EStreamTaskEvent event,
                                     __state_trans_fn preFn, __state_trans_fn fn, __state_trans_succ_fn succFn,
                                     bool autoInvoke) {
  STaskStateTrans trans = {0};
  trans.state = StreamTaskStatusList[current];
  trans.next = StreamTaskStatusList[next];
  trans.event = event;

  trans.preAction = (preFn != NULL)? preFn:dummyFn;
  trans.pAction = (fn != NULL)? fn : dummyFn;
  trans.pSuccAction = (succFn != NULL)? succFn:dummyFn;
  trans.autoInvokeEndFn = autoInvoke;
  return trans;
}

int32_t initStateTransferTable(SStreamTaskSM* pSM) {
  if (pSM->pTransList == NULL) {
    pSM->pTransList = taosArrayInit(8, sizeof(STaskStateTrans));
    if (pSM->pTransList == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  // initialization event handle
  STaskStateTrans trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__NORMAL, TASK_EVENT_INIT, NULL,
                                               streamTaskInitStatus, onNormalTaskReady, false);
  taosArrayPush(pSM->pTransList, &trans);

  trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__SCAN_HISTORY, TASK_EVENT_INIT_SCAN_HISTORY, NULL,
                               streamTaskInitStatus, onScanhistoryTaskReady, false);
  taosArrayPush(pSM->pTransList, &trans);

  trans = createStateTransform(TASK_STATUS__SCAN_HISTORY, TASK_STATUS__NORMAL, TASK_EVENT_SCANHIST_COMPLETED, NULL,
                               streamTaskSetReadyForWal, NULL, true);
  taosArrayPush(pSM->pTransList, &trans);

  // pause & resume related event handle
  trans = createStateTransform(TASK_STATUS__NORMAL, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, NULL, streamTaskDoPause, NULL,
                               true);
  taosArrayPush(pSM->pTransList, &trans);

  trans = createStateTransform(TASK_STATUS__PAUSE, TASK_STATUS__NORMAL, TASK_EVENT_RESUME, NULL, streamTaskDoResume,
                               NULL, true);
  taosArrayPush(pSM->pTransList, &trans);

  trans = createStateTransform(TASK_STATUS__NORMAL, TASK_STATUS__CK, TASK_EVENT_GEN_CHECKPOINT, NULL,
                               streamTaskDoCheckpoint, NULL, true);
  taosArrayPush(pSM->pTransList, &trans);

  trans = createStateTransform(TASK_STATUS__CK, TASK_STATUS__NORMAL, TASK_EVENT_PAUSE, NULL, NULL, NULL, true);
  taosArrayPush(pSM->pTransList, &trans);

  // halt stream task, from other task status
  trans = createStateTransform(TASK_STATUS__NORMAL, TASK_STATUS__HALT, TASK_EVENT_HALT, NULL, NULL,
                               streamTaskKeepCurrentVerInWal, true);
  taosArrayPush(pSM->pTransList, &trans);

  trans = createStateTransform(TASK_STATUS__SCAN_HISTORY, TASK_STATUS__HALT, TASK_EVENT_HALT, streamTaskWaitBeforeHalt,
                               NULL, streamTaskKeepCurrentVerInWal, true);
  taosArrayPush(pSM->pTransList, &trans);

  trans = createStateTransform(TASK_STATUS__CK, TASK_STATUS__HALT, TASK_EVENT_HALT, streamTaskWaitBeforeHalt, NULL,
                               streamTaskKeepCurrentVerInWal, true);
  taosArrayPush(pSM->pTransList, &trans);

  trans = createStateTransform(TASK_STATUS__PAUSE, TASK_STATUS__HALT, TASK_EVENT_HALT, NULL, NULL,
                               streamTaskKeepCurrentVerInWal, true);
  taosArrayPush(pSM->pTransList, &trans);
  return 0;
}