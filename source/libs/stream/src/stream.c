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

SStreamGlobalEnv streamEnv;

int32_t streamInit() {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(&streamEnv.inited, 0, 2);
    if (old != 2) break;
  }

  if (old == 0) {
    streamEnv.timer = taosTmrInit(1000, 100, 10000, "STREAM");
    if (streamEnv.timer == NULL) {
      atomic_store_8(&streamEnv.inited, 0);
      return -1;
    }
    atomic_store_8(&streamEnv.inited, 1);
  }

  return 0;
}

void streamCleanUp() {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(&streamEnv.inited, 1, 2);
    if (old != 2) break;
  }

  if (old == 1) {
    taosTmrCleanUp(streamEnv.timer);
    atomic_store_8(&streamEnv.inited, 0);
  }
}

char* createStreamTaskIdStr(int64_t streamId, int32_t taskId) {
  char buf[128] = {0};
  sprintf(buf, "0x%" PRIx64 "-0x%x", streamId, taskId);
  return taosStrdup(buf);
}

static void streamSchedByTimer(void* param, void* tmrId) {
  SStreamTask* pTask = (void*)param;
  const char*  id = pTask->id.idStr;
  int32_t      nextTrigger = (int32_t)pTask->info.triggerParam;

  int8_t status = atomic_load_8(&pTask->schedInfo.status);
  stTrace("s-task:%s in scheduler, trigger status:%d, next:%dms", id, status, nextTrigger);

  if (streamTaskShouldStop(pTask) || streamTaskShouldPause(pTask)) {
    stDebug("s-task:%s jump out of schedTimer", id);
    return;
  }

  if (streamTaskGetStatus(pTask, NULL) == TASK_STATUS__CK) {
    stDebug("s-task:%s in checkpoint procedure, not retrieve result, next:%dms", id, nextTrigger);
  } else {
    if (status == TASK_TRIGGER_STATUS__ACTIVE) {
      SStreamTrigger* pTrigger = taosAllocateQitem(sizeof(SStreamTrigger), DEF_QITEM, 0);
      if (pTrigger == NULL) {
        stError("s-task:%s failed to prepare retrieve data trigger, code:%s, try again in %dms", id, "out of memory",
                nextTrigger);
        taosTmrReset(streamSchedByTimer, nextTrigger, pTask, streamEnv.timer, &pTask->schedInfo.pTimer);
        return;
      }

      pTrigger->type = STREAM_INPUT__GET_RES;
      pTrigger->pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
      if (pTrigger->pBlock == NULL) {
        taosFreeQitem(pTrigger);

        stError("s-task:%s failed to prepare retrieve data trigger, code:%s, try again in %dms", id, "out of memory",
                nextTrigger);
        taosTmrReset(streamSchedByTimer, nextTrigger, pTask, streamEnv.timer, &pTask->schedInfo.pTimer);
        return;
      }

      atomic_store_8(&pTask->schedInfo.status, TASK_TRIGGER_STATUS__INACTIVE);
      pTrigger->pBlock->info.type = STREAM_GET_ALL;

      int32_t code = streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pTrigger);
      if (code != TSDB_CODE_SUCCESS) {
        taosTmrReset(streamSchedByTimer, nextTrigger, pTask, streamEnv.timer, &pTask->schedInfo.pTimer);
        return;
      }

      streamSchedExec(pTask);
    }
  }

  taosTmrReset(streamSchedByTimer, nextTrigger, pTask, streamEnv.timer, &pTask->schedInfo.pTimer);
}

int32_t streamSetupScheduleTrigger(SStreamTask* pTask) {
  if (pTask->info.triggerParam != 0 && pTask->info.fillHistory == 0) {
    int32_t ref = atomic_add_fetch_32(&pTask->refCnt, 1);
    ASSERT(ref == 2 && pTask->schedInfo.pTimer == NULL);

    stDebug("s-task:%s setup scheduler trigger, delay:%" PRId64 " ms", pTask->id.idStr, pTask->info.triggerParam);

    pTask->schedInfo.pTimer = taosTmrStart(streamSchedByTimer, (int32_t)pTask->info.triggerParam, pTask, streamEnv.timer);
    pTask->schedInfo.status = TASK_TRIGGER_STATUS__INACTIVE;
  }

  return 0;
}

int32_t streamSchedExec(SStreamTask* pTask) {
  if (streamTaskSetSchedStatusWait(pTask)) {
    SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
    if (pRunReq == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      /*int8_t status = */streamTaskSetSchedStatusInactive(pTask);
      stError("failed to create msg to aunch s-task:%s, reason out of memory", pTask->id.idStr);
      return -1;
    }

    pRunReq->head.vgId = pTask->info.nodeId;
    pRunReq->streamId = pTask->id.streamId;
    pRunReq->taskId = pTask->id.taskId;

    stDebug("trigger to run s-task:%s", pTask->id.idStr);

    SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
    tmsgPutToQueue(pTask->pMsgCb, STREAM_QUEUE, &msg);
  } else {
    stTrace("s-task:%s not launch task since sched status:%d", pTask->id.idStr, pTask->status.schedStatus);
  }

  return 0;
}

static int32_t buildDispatchRsp(const SStreamTask* pTask, const SStreamDispatchReq* pReq, int32_t status, void** pBuf) {
  *pBuf = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamDispatchRsp));
  if (*pBuf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  ((SMsgHead*)(*pBuf))->vgId = htonl(pReq->upstreamNodeId);
  ASSERT(((SMsgHead*)(*pBuf))->vgId != 0);

  SStreamDispatchRsp* pDispatchRsp = POINTER_SHIFT((*pBuf), sizeof(SMsgHead));

  pDispatchRsp->stage = htobe64(pReq->stage);
  pDispatchRsp->msgId = htonl(pReq->msgId);
  pDispatchRsp->inputStatus = status;
  pDispatchRsp->streamId = htobe64(pReq->streamId);
  pDispatchRsp->upstreamNodeId = htonl(pReq->upstreamNodeId);
  pDispatchRsp->upstreamTaskId = htonl(pReq->upstreamTaskId);
  pDispatchRsp->downstreamNodeId = htonl(pTask->info.nodeId);
  pDispatchRsp->downstreamTaskId = htonl(pTask->id.taskId);

  return TSDB_CODE_SUCCESS;
}

static int32_t streamTaskAppendInputBlocks(SStreamTask* pTask, const SStreamDispatchReq* pReq) {
  int8_t status = 0;

  SStreamDataBlock* pBlock = createStreamBlockFromDispatchMsg(pReq, pReq->type, pReq->srcVgId);
  if (pBlock == NULL) {
    streamTaskInputFail(pTask);
    status = TASK_INPUT_STATUS__FAILED;
    stError("vgId:%d, s-task:%s failed to receive dispatch msg, reason: out of memory", pTask->pMeta->vgId,
           pTask->id.idStr);
  } else {
    if (pBlock->type == STREAM_INPUT__TRANS_STATE) {
      pTask->status.appendTranstateBlock = true;
    }

    int32_t code = streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pBlock);
    // input queue is full, upstream is blocked now
    status = (code == TSDB_CODE_SUCCESS) ? TASK_INPUT_STATUS__NORMAL : TASK_INPUT_STATUS__BLOCKED;
  }

  return status;
}

int32_t streamTaskEnqueueRetrieve(SStreamTask* pTask, SStreamRetrieveReq* pReq, SRpcMsg* pRsp) {
  SStreamDataBlock* pData = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, sizeof(SStreamDataBlock));
  int8_t            status = TASK_INPUT_STATUS__NORMAL;

  // enqueue
  if (pData != NULL) {
    stDebug("s-task:%s (child %d) recv retrieve req from task:0x%x(vgId:%d), reqId:0x%" PRIx64, pTask->id.idStr,
           pTask->info.selfChildId, pReq->srcTaskId, pReq->srcNodeId, pReq->reqId);

    pData->type = STREAM_INPUT__DATA_RETRIEVE;
    pData->srcVgId = 0;
    streamRetrieveReqToData(pReq, pData);
    if (streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pData) == 0) {
      status = TASK_INPUT_STATUS__NORMAL;
    } else {
      status = TASK_INPUT_STATUS__FAILED;
    }
  } else {  // todo handle oom
    /*streamTaskInputFail(pTask);*/
    /*status = TASK_INPUT_STATUS__FAILED;*/
  }

  // rsp by input status
  void* buf = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamRetrieveRsp));
  ((SMsgHead*)buf)->vgId = htonl(pReq->srcNodeId);
  SStreamRetrieveRsp* pCont = POINTER_SHIFT(buf, sizeof(SMsgHead));
  pCont->streamId = pReq->streamId;
  pCont->rspToTaskId = pReq->srcTaskId;
  pCont->rspFromTaskId = pReq->dstTaskId;
  pRsp->pCont = buf;
  pRsp->contLen = sizeof(SMsgHead) + sizeof(SStreamRetrieveRsp);
  tmsgSendRsp(pRsp);

  return status == TASK_INPUT_STATUS__NORMAL ? 0 : -1;
}

int32_t streamProcessDispatchMsg(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pRsp) {
  int32_t     status = 0;
  const char* id = pTask->id.idStr;

  stDebug("s-task:%s receive dispatch msg from taskId:0x%x(vgId:%d), msgLen:%" PRId64 ", msgId:%d", id,
          pReq->upstreamTaskId, pReq->upstreamNodeId, pReq->totalLen, pReq->msgId);

  SStreamChildEpInfo* pInfo = streamTaskGetUpstreamTaskEpInfo(pTask, pReq->upstreamTaskId);
  ASSERT(pInfo != NULL);

  if (pTask->pMeta->role == NODE_ROLE_FOLLOWER) {
    stError("s-task:%s task on follower received dispatch msgs, dispatch msg rejected", id);
    status = TASK_INPUT_STATUS__REFUSED;
  } else {
    if (pReq->stage > pInfo->stage) {
      // upstream task has restarted/leader-follower switch/transferred to other dnodes
      stError("s-task:%s upstream task:0x%x (vgId:%d) has restart/leader-switch/vnode-transfer, prev stage:%" PRId64
              ", current:%" PRId64 " dispatch msg rejected",
              id, pReq->upstreamTaskId, pReq->upstreamNodeId, pInfo->stage, pReq->stage);
      status = TASK_INPUT_STATUS__REFUSED;
    } else {
      if (!pInfo->dataAllowed) {
        stWarn("s-task:%s data from task:0x%x is denied, since inputQ is closed for it", id, pReq->upstreamTaskId);
        status = TASK_INPUT_STATUS__BLOCKED;
      } else {
        // This task has received the checkpoint req from the upstream task, from which all the messages should be
        // blocked. Note that there is no race condition here.
        if (pReq->type == STREAM_INPUT__CHECKPOINT_TRIGGER) {
          atomic_add_fetch_32(&pTask->upstreamInfo.numOfClosed, 1);
          streamTaskCloseUpstreamInput(pTask, pReq->upstreamTaskId);
          stDebug("s-task:%s close inputQ for upstream:0x%x, msgId:%d", id, pReq->upstreamTaskId, pReq->msgId);
        }

        status = streamTaskAppendInputBlocks(pTask, pReq);
      }
    }
  }

  // disable the data from upstream tasks
  if (streamTaskGetStatus(pTask, NULL) == TASK_STATUS__HALT) {
    status = TASK_INPUT_STATUS__BLOCKED;
  }

  {
    // do send response with the input status
    int32_t code = buildDispatchRsp(pTask, pReq, status, &pRsp->pCont);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s failed to build dispatch rsp, msgId:%d, code:%s", id, pReq->msgId, tstrerror(code));
      return code;
    }

    pRsp->contLen = sizeof(SMsgHead) + sizeof(SStreamDispatchRsp);
    tmsgSendRsp(pRsp);
  }

  tDeleteStreamDispatchReq(pReq);
  streamSchedExec(pTask);

  return 0;
}

int32_t streamProcessRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* pReq, SRpcMsg* pRsp) {
  streamTaskEnqueueRetrieve(pTask, pReq, pRsp);
  ASSERT(pTask->info.taskLevel != TASK_LEVEL__SINK);
  streamSchedExec(pTask);
  return 0;
}

void streamTaskInputFail(SStreamTask* pTask) { atomic_store_8(&pTask->inputInfo.status, TASK_INPUT_STATUS__FAILED); }

void streamTaskOpenAllUpstreamInput(SStreamTask* pTask) {
  int32_t num = taosArrayGetSize(pTask->upstreamInfo.pList);
  if (num == 0) {
    return;
  }

  for (int32_t i = 0; i < num; ++i) {
    SStreamChildEpInfo* pInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
    pInfo->dataAllowed = true;
  }

  pTask->upstreamInfo.numOfClosed = 0;
}

void streamTaskCloseUpstreamInput(SStreamTask* pTask, int32_t taskId) {
  SStreamChildEpInfo* pInfo = streamTaskGetUpstreamTaskEpInfo(pTask, taskId);
  if (pInfo != NULL) {
    pInfo->dataAllowed = false;
  }
}

SStreamChildEpInfo* streamTaskGetUpstreamTaskEpInfo(SStreamTask* pTask, int32_t taskId) {
  int32_t num = taosArrayGetSize(pTask->upstreamInfo.pList);
  for (int32_t i = 0; i < num; ++i) {
    SStreamChildEpInfo* pInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
    if (pInfo->taskId == taskId) {
      return pInfo;
    }
  }

  stError("s-task:%s failed to find upstream task:0x%x", pTask->id.idStr, taskId);
  return NULL;
}