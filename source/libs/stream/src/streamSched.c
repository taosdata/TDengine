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

static void streamTaskResumeHelper(void* param, void* tmrId);
static void streamTaskSchedHelper(void* param, void* tmrId);

int32_t streamSetupScheduleTrigger(SStreamTask* pTask) {
  if (pTask->info.delaySchedParam != 0 && pTask->info.fillHistory == 0) {
    int32_t ref = atomic_add_fetch_32(&pTask->refCnt, 1);
    ASSERT(ref == 2 && pTask->schedInfo.pDelayTimer == NULL);

    stDebug("s-task:%s setup scheduler trigger, delay:%" PRId64 " ms", pTask->id.idStr, pTask->info.delaySchedParam);

    pTask->schedInfo.pDelayTimer = taosTmrStart(streamTaskSchedHelper, (int32_t)pTask->info.delaySchedParam, pTask, streamTimer);
    pTask->schedInfo.status = TASK_TRIGGER_STATUS__INACTIVE;
  }

  return 0;
}

int32_t streamTrySchedExec(SStreamTask* pTask) {
  if (streamTaskSetSchedStatusWait(pTask)) {
    streamTaskSchedTask(pTask->pMsgCb, pTask->info.nodeId, pTask->id.streamId, pTask->id.taskId, 0);
  } else {
    stTrace("s-task:%s not launch task since sched status:%d", pTask->id.idStr, pTask->status.schedStatus);
  }

  return 0;
}

int32_t streamTaskSchedTask(SMsgCb* pMsgCb, int32_t vgId, int64_t streamId, int32_t taskId, int32_t execType) {
  SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
  if (pRunReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    stError("vgId:%d failed to create msg to start stream task:0x%x exec, type:%d, code:%s", vgId, taskId, execType,
            terrstr());
    return -1;
  }

  if (streamId != 0) {
    stDebug("vgId:%d create msg to start stream task:0x%x, exec type:%d", vgId, taskId, execType);
  } else {
    stDebug("vgId:%d create msg to exec, type:%d", vgId, execType);
  }

  pRunReq->head.vgId = vgId;
  pRunReq->streamId = streamId;
  pRunReq->taskId = taskId;
  pRunReq->reqType = execType;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  tmsgPutToQueue(pMsgCb, STREAM_QUEUE, &msg);
  return TSDB_CODE_SUCCESS;
}

void streamTaskClearSchedIdleInfo(SStreamTask* pTask) { pTask->status.schedIdleTime = 0; }

void streamTaskSetIdleInfo(SStreamTask* pTask, int32_t idleTime) { pTask->status.schedIdleTime = idleTime; }

int32_t streamTaskResumeInFuture(SStreamTask* pTask) {
  int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
  stDebug("s-task:%s task should idle, add into timer to retry in %dms, ref:%d", pTask->id.idStr,
          pTask->status.schedIdleTime, ref);

  // add one ref count for task
  /*SStreamTask* pAddRefTask = */streamMetaAcquireOneTask(pTask);

  if (pTask->schedInfo.pIdleTimer == NULL) {
    pTask->schedInfo.pIdleTimer = taosTmrStart(streamTaskResumeHelper, pTask->status.schedIdleTime, pTask, streamTimer);
  } else {
    taosTmrReset(streamTaskResumeHelper, pTask->status.schedIdleTime, pTask, streamTimer, &pTask->schedInfo.pIdleTimer);
  }

  return TSDB_CODE_SUCCESS;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void streamTaskResumeHelper(void* param, void* tmrId) {
  SStreamTask*      pTask = (SStreamTask*)param;
  SStreamTaskId*    pId = &pTask->id;
  SStreamTaskState* p = streamTaskGetStatus(pTask);

  if (p->state == TASK_STATUS__DROPPING || p->state == TASK_STATUS__STOP) {
    streamTaskSetSchedStatusInactive(pTask);

    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s status:%s not resume task, ref:%d", pId->idStr, p->name, ref);

    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  streamTaskSchedTask(pTask->pMsgCb, pTask->info.nodeId, pId->streamId, pId->taskId, STREAM_EXEC_T_RESUME_TASK);

  int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
  stDebug("trigger to resume s-task:%s after being idled for %dms, ref:%d", pId->idStr, pTask->status.schedIdleTime,
          ref);

  // release the task ref count
  streamTaskClearSchedIdleInfo(pTask);
  streamMetaReleaseTask(pTask->pMeta, pTask);
}

void streamTaskSchedHelper(void* param, void* tmrId) {
  SStreamTask* pTask = (void*)param;
  const char*  id = pTask->id.idStr;
  int32_t      nextTrigger = (int32_t)pTask->info.delaySchedParam;

  int8_t status = atomic_load_8(&pTask->schedInfo.status);
  stTrace("s-task:%s in scheduler, trigger status:%d, next:%dms", id, status, nextTrigger);

  if (streamTaskShouldStop(pTask) || streamTaskShouldPause(pTask)) {
    stDebug("s-task:%s jump out of schedTimer", id);
    return;
  }

  if (streamTaskGetStatus(pTask)->state == TASK_STATUS__CK) {
    stDebug("s-task:%s in checkpoint procedure, not retrieve result, next:%dms", id, nextTrigger);
  } else {
    if (status == TASK_TRIGGER_STATUS__ACTIVE) {
      SStreamTrigger* pTrigger = taosAllocateQitem(sizeof(SStreamTrigger), DEF_QITEM, 0);
      if (pTrigger == NULL) {
        stError("s-task:%s failed to prepare retrieve data trigger, code:%s, try again in %dms", id, "out of memory",
                nextTrigger);
        taosTmrReset(streamTaskSchedHelper, nextTrigger, pTask, streamTimer, &pTask->schedInfo.pDelayTimer);
        return;
      }

      pTrigger->type = STREAM_INPUT__GET_RES;
      pTrigger->pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
      if (pTrigger->pBlock == NULL) {
        taosFreeQitem(pTrigger);

        stError("s-task:%s failed to prepare retrieve data trigger, code:%s, try again in %dms", id, "out of memory",
                nextTrigger);
        taosTmrReset(streamTaskSchedHelper, nextTrigger, pTask, streamTimer, &pTask->schedInfo.pDelayTimer);
        return;
      }

      atomic_store_8(&pTask->schedInfo.status, TASK_TRIGGER_STATUS__INACTIVE);
      pTrigger->pBlock->info.type = STREAM_GET_ALL;

      int32_t code = streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pTrigger);
      if (code != TSDB_CODE_SUCCESS) {
        taosTmrReset(streamTaskSchedHelper, nextTrigger, pTask, streamTimer, &pTask->schedInfo.pDelayTimer);
        return;
      }

      streamTrySchedExec(pTask);
    }
  }

  taosTmrReset(streamTaskSchedHelper, nextTrigger, pTask, streamTimer, &pTask->schedInfo.pDelayTimer);
}
