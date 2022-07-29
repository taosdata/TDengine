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

#include "streamInc.h"
#include "ttimer.h"

int32_t streamInit() {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(&streamEnv.inited, 0, 2);
    if (old != 2) break;
  }

  if (old == 0) {
    streamEnv.timer = taosTmrInit(10000, 100, 10000, "STREAM");
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

void streamTriggerByTimer(void* param, void* tmrId) {
  SStreamTask* pTask = (void*)param;

  if (atomic_load_8(&pTask->taskStatus) == TASK_STATUS__DROPPING) {
    return;
  }

  if (atomic_load_8(&pTask->triggerStatus) == TASK_TRIGGER_STATUS__ACTIVE) {
    SStreamTrigger* trigger = taosAllocateQitem(sizeof(SStreamTrigger), DEF_QITEM);
    if (trigger == NULL) return;
    trigger->type = STREAM_INPUT__GET_RES;
    trigger->pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
    if (trigger->pBlock == NULL) {
      taosFreeQitem(trigger);
      return;
    }
    trigger->pBlock->info.type = STREAM_GET_ALL;

    atomic_store_8(&pTask->triggerStatus, TASK_TRIGGER_STATUS__IN_ACTIVE);

    streamTaskInput(pTask, (SStreamQueueItem*)trigger);
    streamLaunchByWrite(pTask, pTask->nodeId);
  }

  taosTmrReset(streamTriggerByTimer, (int32_t)pTask->triggerParam, pTask, streamEnv.timer, &pTask->timer);
}

int32_t streamSetupTrigger(SStreamTask* pTask) {
  if (pTask->triggerParam != 0) {
    pTask->timer = taosTmrStart(streamTriggerByTimer, (int32_t)pTask->triggerParam, pTask, streamEnv.timer);
    pTask->triggerStatus = TASK_TRIGGER_STATUS__IN_ACTIVE;
  }
  return 0;
}

int32_t streamLaunchByWrite(SStreamTask* pTask, int32_t vgId) {
  int8_t execStatus = atomic_load_8(&pTask->execStatus);
  if (execStatus == TASK_EXEC_STATUS__IDLE || execStatus == TASK_EXEC_STATUS__CLOSING) {
    SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
    if (pRunReq == NULL) return -1;

    // TODO: do we need htonl?
    pRunReq->head.vgId = vgId;
    pRunReq->streamId = pTask->streamId;
    pRunReq->taskId = pTask->taskId;
    SRpcMsg msg = {
        .msgType = TDMT_STREAM_TASK_RUN,
        .pCont = pRunReq,
        .contLen = sizeof(SStreamTaskRunReq),
    };
    tmsgPutToQueue(pTask->pMsgCb, STREAM_QUEUE, &msg);
  }
  return 0;
}

int32_t streamTaskEnqueue(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pRsp) {
  SStreamDataBlock* pData = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM);
  int8_t            status;

  // enqueue
  if (pData != NULL) {
    pData->type = STREAM_INPUT__DATA_BLOCK;
    pData->srcVgId = pReq->dataSrcVgId;
    // decode
    /*pData->blocks = pReq->data;*/
    /*pBlock->sourceVer = pReq->sourceVer;*/
    streamDispatchReqToData(pReq, pData);
    if (streamTaskInput(pTask, (SStreamQueueItem*)pData) == 0) {
      status = TASK_INPUT_STATUS__NORMAL;
    } else {
      status = TASK_INPUT_STATUS__FAILED;
    }
  } else {
    streamTaskInputFail(pTask);
    status = TASK_INPUT_STATUS__FAILED;
  }

  // rsp by input status
  void* buf = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamDispatchRsp));
  ((SMsgHead*)buf)->vgId = htonl(pReq->upstreamNodeId);
  SStreamDispatchRsp* pCont = POINTER_SHIFT(buf, sizeof(SMsgHead));
  pCont->inputStatus = status;
  pCont->streamId = pReq->streamId;
  pCont->taskId = pReq->upstreamTaskId;
  pRsp->pCont = buf;
  pRsp->contLen = sizeof(SMsgHead) + sizeof(SStreamDispatchRsp);
  tmsgSendRsp(pRsp);
  return status == TASK_INPUT_STATUS__NORMAL ? 0 : -1;
}

int32_t streamTaskEnqueueRetrieve(SStreamTask* pTask, SStreamRetrieveReq* pReq, SRpcMsg* pRsp) {
  SStreamDataBlock* pData = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM);
  int8_t            status = TASK_INPUT_STATUS__NORMAL;

  // enqueue
  if (pData != NULL) {
    qDebug("task %d(child %d) recv retrieve req from task %d, reqId %ld", pTask->taskId, pTask->selfChildId,
           pReq->srcTaskId, pReq->reqId);

    pData->type = STREAM_INPUT__DATA_RETRIEVE;
    pData->srcVgId = 0;
    // decode
    /*pData->blocks = pReq->data;*/
    /*pBlock->sourceVer = pReq->sourceVer;*/
    streamRetrieveReqToData(pReq, pData);
    if (streamTaskInput(pTask, (SStreamQueueItem*)pData) == 0) {
      status = TASK_INPUT_STATUS__NORMAL;
    } else {
      status = TASK_INPUT_STATUS__FAILED;
    }
  } else {
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

int32_t streamProcessDispatchReq(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pRsp) {
  qDebug("task %d receive dispatch req from node %d task %d", pTask->taskId, pReq->upstreamNodeId,
         pReq->upstreamTaskId);

  // 1. handle input
  streamTaskEnqueue(pTask, pReq, pRsp);

  // 2. try exec
  // 2.1. idle: exec
  // 2.2. executing: return
  // 2.3. closing: keep trying
#if 0
  if (pTask->execType != TASK_EXEC__NONE) {
#endif
  streamExec(pTask);
#if 0
  } else {
    ASSERT(pTask->sinkType != TASK_SINK__NONE);
    while (1) {
      void* data = streamQueueNextItem(pTask->inputQueue);
      if (data == NULL) return 0;
      if (streamTaskOutput(pTask, data) < 0) {
        ASSERT(0);
      }
    }
  }
#endif

  // 3. handle output
  // 3.1 check and set status
  // 3.2 dispatch / sink
  if (pTask->dispatchType != TASK_DISPATCH__NONE) {
    ASSERT(pTask->sinkType == TASK_SINK__NONE);
    streamDispatch(pTask);
  }

  return 0;
}

int32_t streamProcessDispatchRsp(SStreamTask* pTask, SStreamDispatchRsp* pRsp) {
  ASSERT(pRsp->inputStatus == TASK_OUTPUT_STATUS__NORMAL || pRsp->inputStatus == TASK_OUTPUT_STATUS__BLOCKED);

  qDebug("task %d receive dispatch rsp", pTask->taskId);

  if (pTask->dispatchType == TASK_DISPATCH__SHUFFLE) {
    int32_t leftRsp = atomic_sub_fetch_32(&pTask->shuffleDispatcher.waitingRspCnt, 1);
    qDebug("task %d is shuffle, left waiting rsp %d", pTask->taskId, leftRsp);
    if (leftRsp > 0) return 0;
  }

  int8_t old = atomic_exchange_8(&pTask->outputStatus, pRsp->inputStatus);
  ASSERT(old == TASK_OUTPUT_STATUS__WAIT);
  if (pRsp->inputStatus == TASK_INPUT_STATUS__BLOCKED) {
    // TODO: init recover timer
    ASSERT(0);
    return 0;
  }
  // continue dispatch
  streamDispatch(pTask);
  return 0;
}

int32_t streamProcessRunReq(SStreamTask* pTask) {
  streamExec(pTask);

  if (pTask->dispatchType != TASK_DISPATCH__NONE) {
    streamDispatch(pTask);
  }
  return 0;
}

int32_t streamProcessRecoverReq(SStreamTask* pTask, SStreamTaskRecoverReq* pReq, SRpcMsg* pRsp) {
  void* buf = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamTaskRecoverRsp));
  ((SMsgHead*)buf)->vgId = htonl(pReq->upstreamNodeId);

  SStreamTaskRecoverRsp* pCont = POINTER_SHIFT(buf, sizeof(SMsgHead));
  pCont->inputStatus = pTask->inputStatus;
  pCont->streamId = pTask->streamId;
  pCont->reqTaskId = pTask->taskId;
  pCont->rspTaskId = pReq->upstreamTaskId;

  pRsp->pCont = buf;
  pRsp->contLen = sizeof(SMsgHead) + sizeof(SStreamTaskRecoverRsp);
  tmsgSendRsp(pRsp);
  return 0;
}

int32_t streamProcessRecoverRsp(SStreamTask* pTask, SStreamTaskRecoverRsp* pRsp) {
  if (pRsp->inputStatus == TASK_INPUT_STATUS__NORMAL) {
    pTask->outputStatus = TASK_OUTPUT_STATUS__NORMAL;

    streamProcessRunReq(pTask);

    if (pTask->isDataScan) {
      // scan data to recover
      pTask->inputStatus = TASK_INPUT_STATUS__RECOVER;
      pTask->taskStatus = TASK_STATUS__RECOVERING;
      qStreamPrepareRecover(pTask->exec.executor, pTask->startVer, pTask->recoverSnapVer);
      if (streamPipelineExec(pTask, 100) < 0) {
        return -1;
      }
    } else {
      pTask->inputStatus = TASK_INPUT_STATUS__NORMAL;
      pTask->taskStatus = TASK_STATUS__NORMAL;
    }
  }

  return 0;
}

int32_t streamProcessRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* pReq, SRpcMsg* pRsp) {
  qDebug("task %d receive retrieve req from node %d task %d", pTask->taskId, pReq->srcNodeId, pReq->srcTaskId);

  streamTaskEnqueueRetrieve(pTask, pReq, pRsp);

  ASSERT(pTask->execType != TASK_EXEC__NONE);
  streamExec(pTask);

  ASSERT(pTask->dispatchType != TASK_DISPATCH__NONE);
  streamDispatch(pTask);

  return 0;
}

int32_t streamProcessRetrieveRsp(SStreamTask* pTask, SStreamRetrieveRsp* pRsp) {
  //
  return 0;
}
