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

#include "executor.h"
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

static STaskStateTrans createStateTransform(ETaskStatus current, ETaskStatus next, EStreamTaskEvent event, __state_trans_fn fn);
static int32_t         initStateTransferTable(SStreamTaskSM* pSM);

static int32_t dummyFn(SStreamTask* UNUSED_PARAM(p)) { return 0; }
static int32_t streamTaskStartCheckDownstream(SStreamTask* pTask) {
  stDebug("s-task:%s start to check downstream tasks", pTask->id.idStr);
  return 0;
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

// todo optimize the perf of find the trans objs by using hash table
static STaskStateTrans* streamTaskFindTransform(const SStreamTaskSM* pState, const EStreamTaskEvent* pEvent) {
  int32_t numOfTrans = taosArrayGetSize(pState->pTransList);
  for(int32_t i = 0; i < numOfTrans; ++i) {
    STaskStateTrans* pTrans = taosArrayGet(pState->pTransList, i);
    if (pTrans->state.state == pState->current.state && pTrans->event == *pEvent) {
      return pTrans;
    }
  }

  ASSERT(0);
  return NULL;
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
  pSM->stateTs = taosGetTimestampMs();
  int32_t code = initStateTransferTable(pSM);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pSM);
    return NULL;
  }

  return pSM;
}

int32_t taskSMHandleEvent(SStreamTaskSM* pSM, const EStreamTaskEvent* pEvent) {
  STaskStateTrans* pTrans = streamTaskFindTransform(pSM, pEvent);
  qDebug("start to handle event:%d", *pEvent);

  pSM->current = pTrans->next;
  pSM->stateTs = taosGetTimestampMs();
  qDebug("new state:%s from %s", pTrans->next.name, pSM->current.name);

  return pTrans->pAction(pSM->pTask);
}

STaskStateTrans createStateTransform(ETaskStatus current, ETaskStatus next, EStreamTaskEvent event, __state_trans_fn fn) {
  STaskStateTrans trans = {0};
  trans.state = StreamTaskStatusList[current];
  trans.next = StreamTaskStatusList[next];
  trans.event = event;
  trans.pAction = (fn != NULL)? fn : dummyFn;
  return trans;
}

int32_t initStateTransferTable(SStreamTaskSM* pSM) {
  if (pSM->pTransList == NULL) {
    pSM->pTransList = taosArrayInit(8, sizeof(STaskStateTrans));
    if (pSM->pTransList == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  STaskStateTrans trans = createStateTransform(TASK_STATUS__UNINIT, TASK_STATUS__NORMAL, TASK_EVENT_INIT, streamTaskStartCheckDownstream);
  taosArrayPush(pSM->pTransList, &trans);

  trans = createStateTransform(TASK_STATUS__NORMAL, TASK_STATUS__PAUSE, TASK_EVENT_PAUSE, streamTaskDoPause);
  taosArrayPush(pSM->pTransList, &trans);

  trans = createStateTransform(TASK_STATUS__PAUSE, TASK_STATUS__NORMAL, TASK_EVENT_RESUME, streamTaskDoResume);
  taosArrayPush(pSM->pTransList, &trans);

  trans = createStateTransform(TASK_STATUS__NORMAL, TASK_STATUS__CK, TASK_EVENT_GEN_CHECKPOINT, streamTaskDoCheckpoint);
  taosArrayPush(pSM->pTransList, &trans);

  trans = createStateTransform(TASK_STATUS__CK, TASK_STATUS__NORMAL, TASK_EVENT_PAUSE, NULL);
  taosArrayPush(pSM->pTransList, &trans);

  return 0;
}