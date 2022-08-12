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

void streamSchedByTimer(void* param, void* tmrId) {
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

    atomic_store_8(&pTask->triggerStatus, TASK_TRIGGER_STATUS__INACTIVE);

    streamTaskInput(pTask, (SStreamQueueItem*)trigger);
    streamSchedExec(pTask);
  }

  taosTmrReset(streamSchedByTimer, (int32_t)pTask->triggerParam, pTask, streamEnv.timer, &pTask->timer);
}

int32_t streamSetupTrigger(SStreamTask* pTask) {
  if (pTask->triggerParam != 0) {
    pTask->timer = taosTmrStart(streamSchedByTimer, (int32_t)pTask->triggerParam, pTask, streamEnv.timer);
    pTask->triggerStatus = TASK_TRIGGER_STATUS__INACTIVE;
  }
  return 0;
}

int32_t streamSchedExec(SStreamTask* pTask) {
  int8_t schedStatus =
      atomic_val_compare_exchange_8(&pTask->schedStatus, TASK_SCHED_STATUS__INACTIVE, TASK_SCHED_STATUS__WAITING);
  if (schedStatus == TASK_SCHED_STATUS__INACTIVE) {
    SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
    if (pRunReq == NULL) {
      atomic_store_8(&pTask->schedStatus, TASK_SCHED_STATUS__INACTIVE);
      return -1;
    }
    pRunReq->head.vgId = pTask->nodeId;
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

int32_t streamTaskEnqueue(SStreamTask* pTask, const SStreamDispatchReq* pReq, SRpcMsg* pRsp) {
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
    qDebug("task %d(child %d) recv retrieve req from task %d, reqId %" PRId64, pTask->taskId, pTask->selfChildId,
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

int32_t streamProcessDispatchReq(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pRsp, bool exec) {
  qDebug("task %d receive dispatch req from node %d task %d", pTask->taskId, pReq->upstreamNodeId,
         pReq->upstreamTaskId);

  streamTaskEnqueue(pTask, pReq, pRsp);
  tFreeStreamDispatchReq(pReq);

  if (exec) {
    streamTryExec(pTask);

    if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      streamDispatch(pTask);
    }
  } else {
    streamSchedExec(pTask);
  }

  return 0;
}

int32_t streamProcessDispatchRsp(SStreamTask* pTask, SStreamDispatchRsp* pRsp) {
  ASSERT(pRsp->inputStatus == TASK_OUTPUT_STATUS__NORMAL || pRsp->inputStatus == TASK_OUTPUT_STATUS__BLOCKED);

  qDebug("task %d receive dispatch rsp", pTask->taskId);

  if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
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
  streamTryExec(pTask);

  if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    streamDispatch(pTask);
  }
  return 0;
}

#if 0
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

int32_t streamProcessRecoverRsp(SStreamMeta* pMeta, SStreamTask* pTask, SStreamRecoverDownstreamRsp* pRsp) {
  streamProcessRunReq(pTask);

  if (pTask->taskLevel == TASK_LEVEL__SOURCE) {
    // scan data to recover
    pTask->inputStatus = TASK_INPUT_STATUS__RECOVER;
    pTask->taskStatus = TASK_STATUS__RECOVER_SELF;
    qStreamPrepareRecover(pTask->exec.executor, pTask->startVer, pTask->recoverSnapVer);
    if (streamPipelineExec(pTask, 100, true) < 0) {
      return -1;
    }
  } else {
    pTask->inputStatus = TASK_INPUT_STATUS__NORMAL;
    pTask->taskStatus = TASK_STATUS__NORMAL;
  }

  return 0;
}
#endif

int32_t streamProcessRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* pReq, SRpcMsg* pRsp) {
  qDebug("task %d receive retrieve req from node %d task %d", pTask->taskId, pReq->srcNodeId, pReq->srcTaskId);

  streamTaskEnqueueRetrieve(pTask, pReq, pRsp);

  ASSERT(pTask->taskLevel != TASK_LEVEL__SINK);
  streamSchedExec(pTask);

  /*streamTryExec(pTask);*/

  /*streamDispatch(pTask);*/

  return 0;
}

int32_t streamProcessRetrieveRsp(SStreamTask* pTask, SStreamRetrieveRsp* pRsp) {
  //
  return 0;
}
