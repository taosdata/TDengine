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
    streamMetaReleaseTask(NULL, pTask);
    return;
  }

  if (atomic_load_8(&pTask->triggerStatus) == TASK_TRIGGER_STATUS__ACTIVE) {
    SStreamTrigger* trigger = taosAllocateQitem(sizeof(SStreamTrigger), DEF_QITEM, 0);
    if (trigger == NULL) return;
    trigger->type = STREAM_INPUT__GET_RES;
    trigger->pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
    if (trigger->pBlock == NULL) {
      taosFreeQitem(trigger);
      return;
    }
    trigger->pBlock->info.type = STREAM_GET_ALL;

    atomic_store_8(&pTask->triggerStatus, TASK_TRIGGER_STATUS__INACTIVE);

    if (tAppendDataForStream(pTask, (SStreamQueueItem*)trigger) < 0) {
      taosFreeQitem(trigger);
      taosTmrReset(streamSchedByTimer, (int32_t)pTask->triggerParam, pTask, streamEnv.timer, &pTask->timer);
      return;
    }
    streamSchedExec(pTask);
  }

  taosTmrReset(streamSchedByTimer, (int32_t)pTask->triggerParam, pTask, streamEnv.timer, &pTask->timer);
}

int32_t streamSetupTrigger(SStreamTask* pTask) {
  if (pTask->triggerParam != 0) {
    int32_t ref = atomic_add_fetch_32(&pTask->refCnt, 1);
    ASSERT(ref == 2);
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

    SRpcMsg msg = { .msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq) };
    tmsgPutToQueue(pTask->pMsgCb, STREAM_QUEUE, &msg);
  }

  return 0;
}

int32_t streamTaskEnqueue(SStreamTask* pTask, const SStreamDispatchReq* pReq, SRpcMsg* pRsp) {
  SStreamDataBlock* pData = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, 0);
  int8_t            status;

  // enqueue
  if (pData != NULL) {
    pData->type = STREAM_INPUT__DATA_BLOCK;
    pData->srcVgId = pReq->dataSrcVgId;
    // decode
    /*pData->blocks = pReq->data;*/
    /*pBlock->sourceVer = pReq->sourceVer;*/
    streamDispatchReqToData(pReq, pData);
    if (tAppendDataForStream(pTask, (SStreamQueueItem*)pData) == 0) {
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
  pCont->streamId = htobe64(pReq->streamId);
  pCont->upstreamNodeId = htonl(pReq->upstreamNodeId);
  pCont->upstreamTaskId = htonl(pReq->upstreamTaskId);
  pCont->downstreamNodeId = htonl(pTask->nodeId);
  pCont->downstreamTaskId = htonl(pTask->taskId);
  pRsp->pCont = buf;
  pRsp->contLen = sizeof(SMsgHead) + sizeof(SStreamDispatchRsp);
  tmsgSendRsp(pRsp);
  return status == TASK_INPUT_STATUS__NORMAL ? 0 : -1;
}

int32_t streamTaskEnqueueRetrieve(SStreamTask* pTask, SStreamRetrieveReq* pReq, SRpcMsg* pRsp) {
  SStreamDataBlock* pData = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, 0);
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
    if (tAppendDataForStream(pTask, (SStreamQueueItem*)pData) == 0) {
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

int32_t streamTaskOutput(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  if (pTask->outputType == TASK_OUTPUT__TABLE) {
    pTask->tbSink.tbSinkFunc(pTask, pTask->tbSink.vnode, 0, pBlock->blocks);
    taosArrayDestroyEx(pBlock->blocks, (FDelete)blockDataFreeRes);
    taosFreeQitem(pBlock);
  } else if (pTask->outputType == TASK_OUTPUT__SMA) {
    pTask->smaSink.smaSink(pTask->smaSink.vnode, pTask->smaSink.smaId, pBlock->blocks);
    taosArrayDestroyEx(pBlock->blocks, (FDelete)blockDataFreeRes);
    taosFreeQitem(pBlock);
  } else {
    ASSERT(pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH);
    taosWriteQitem(pTask->outputQueue->queue, pBlock);
    streamDispatch(pTask);
  }
  return 0;
}

int32_t streamProcessDispatchReq(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pRsp, bool exec) {
  qDebug("task %d receive dispatch req from node %d task %d", pTask->taskId, pReq->upstreamNodeId,
         pReq->upstreamTaskId);

  streamTaskEnqueue(pTask, pReq, pRsp);
  tDeleteStreamDispatchReq(pReq);

  if (exec) {
    if (streamTryExec(pTask) < 0) {
      return -1;
    }

    /*if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {*/
    /*streamDispatch(pTask);*/
    /*}*/
  } else {
    streamSchedExec(pTask);
  }

  return 0;
}

int32_t streamProcessDispatchRsp(SStreamTask* pTask, SStreamDispatchRsp* pRsp, int32_t code) {
  ASSERT(pRsp->inputStatus == TASK_OUTPUT_STATUS__NORMAL || pRsp->inputStatus == TASK_OUTPUT_STATUS__BLOCKED);

  qDebug("task %d receive dispatch rsp, code: %x", pTask->taskId, code);

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
  if (streamTryExec(pTask) < 0) {
    return -1;
  }

  /*if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {*/
  /*streamDispatch(pTask);*/
  /*}*/
  return 0;
}

int32_t streamProcessRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* pReq, SRpcMsg* pRsp) {
  qDebug("task %d receive retrieve req from node %d task %d", pTask->taskId, pReq->srcNodeId, pReq->srcTaskId);

  streamTaskEnqueueRetrieve(pTask, pReq, pRsp);

  ASSERT(pTask->taskLevel != TASK_LEVEL__SINK);
  streamSchedExec(pTask);

  /*streamTryExec(pTask);*/

  /*streamDispatch(pTask);*/

  return 0;
}

int32_t tAppendDataForStream(SStreamTask* pTask, SStreamQueueItem* pItem) {
  int8_t type = pItem->type;

  if (type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamDataSubmit2* pSubmitBlock = streamSubmitBlockClone((SStreamDataSubmit2*)pItem);
    if (pSubmitBlock == NULL) {
      qDebug("task %d %p submit enqueue failed since out of memory", pTask->taskId, pTask);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      atomic_store_8(&pTask->inputStatus, TASK_INPUT_STATUS__FAILED);
      return -1;
    }

    int32_t total = taosQueueItemSize(pTask->inputQueue->queue) + 1;
    qDebug("stream task:%d %p submit enqueue %p %p %p msgLen:%d ver:%" PRId64 ", total in queue:%d", pTask->taskId,
           pTask, pItem, pSubmitBlock, pSubmitBlock->submit.msgStr, pSubmitBlock->submit.msgLen,
           pSubmitBlock->submit.ver, total);

    taosWriteQitem(pTask->inputQueue->queue, pSubmitBlock);
  } else if (type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__DATA_RETRIEVE ||
             type == STREAM_INPUT__REF_DATA_BLOCK) {
    taosWriteQitem(pTask->inputQueue->queue, pItem);
  } else if (type == STREAM_INPUT__CHECKPOINT) {
    taosWriteQitem(pTask->inputQueue->queue, pItem);
  } else if (type == STREAM_INPUT__GET_RES) {
    taosWriteQitem(pTask->inputQueue->queue, pItem);
  }

  if (type != STREAM_INPUT__GET_RES && type != STREAM_INPUT__CHECKPOINT && pTask->triggerParam != 0) {
    atomic_val_compare_exchange_8(&pTask->triggerStatus, TASK_TRIGGER_STATUS__INACTIVE, TASK_TRIGGER_STATUS__ACTIVE);
  }

#if 0
  // TODO: back pressure
  atomic_store_8(&pTask->inputStatus, TASK_INPUT_STATUS__NORMAL);
#endif

  return 0;
}

void* streamQueueNextItem(SStreamQueue* queue) {
  int8_t dequeueFlag = atomic_exchange_8(&queue->status, STREAM_QUEUE__PROCESSING);
  if (dequeueFlag == STREAM_QUEUE__FAILED) {
    ASSERT(queue->qItem != NULL);
    return streamQueueCurItem(queue);
  } else {
    queue->qItem = NULL;
    taosGetQitem(queue->qall, &queue->qItem);
    if (queue->qItem == NULL) {
      taosReadAllQitems(queue->queue, queue->qall);
      taosGetQitem(queue->qall, &queue->qItem);
    }
    return streamQueueCurItem(queue);
  }
}