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

#define STREAM_TASK_INPUT_QUEUEU_CAPACITY         20480
#define STREAM_TASK_INPUT_QUEUEU_CAPACITY_IN_SIZE (50)
#define ONE_MB_F                                  (1048576.0)
#define QUEUE_MEM_SIZE_IN_MB(_q)                  (taosQueueMemorySize(_q) / ONE_MB_F)

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

  if (streamTaskShouldStop(&pTask->status) || streamTaskShouldPause(&pTask->status)) {
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

    if (tAppendDataToInputQueue(pTask, (SStreamQueueItem*)trigger) < 0) {
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
  int8_t schedStatus = atomic_val_compare_exchange_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__INACTIVE,
                                                     TASK_SCHED_STATUS__WAITING);

  if (schedStatus == TASK_SCHED_STATUS__INACTIVE) {
    SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
    if (pRunReq == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__INACTIVE);
      return -1;
    }

    pRunReq->head.vgId = pTask->nodeId;
    pRunReq->streamId = pTask->id.streamId;
    pRunReq->taskId = pTask->id.taskId;

    SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
    tmsgPutToQueue(pTask->pMsgCb, STREAM_QUEUE, &msg);
    qDebug("trigger to run s-task:%s", pTask->id.idStr);
  }

  return 0;
}

int32_t streamTaskEnqueueBlocks(SStreamTask* pTask, const SStreamDispatchReq* pReq, SRpcMsg* pRsp) {
  int8_t status = 0;

  SStreamDataBlock* pBlock = createStreamDataFromDispatchMsg(pReq, STREAM_INPUT__DATA_BLOCK, pReq->dataSrcVgId);
  if (pBlock == NULL) {
    streamTaskInputFail(pTask);
    status = TASK_INPUT_STATUS__FAILED;
    qError("vgId:%d, s-task:%s failed to receive dispatch msg, reason: out of memory", pTask->pMeta->vgId,
           pTask->id.idStr);
  } else {
    int32_t code = tAppendDataToInputQueue(pTask, (SStreamQueueItem*)pBlock);
    // input queue is full, upstream is blocked now
    status = (code == TSDB_CODE_SUCCESS)? TASK_INPUT_STATUS__NORMAL:TASK_INPUT_STATUS__BLOCKED;
  }

  // rsp by input status
  void* buf = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamDispatchRsp));
  ((SMsgHead*)buf)->vgId = htonl(pReq->upstreamNodeId);
  SStreamDispatchRsp* pDispatchRsp = POINTER_SHIFT(buf, sizeof(SMsgHead));

  pDispatchRsp->inputStatus = status;
  pDispatchRsp->streamId = htobe64(pReq->streamId);
  pDispatchRsp->upstreamNodeId = htonl(pReq->upstreamNodeId);
  pDispatchRsp->upstreamTaskId = htonl(pReq->upstreamTaskId);
  pDispatchRsp->downstreamNodeId = htonl(pTask->nodeId);
  pDispatchRsp->downstreamTaskId = htonl(pTask->id.taskId);

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
    qDebug("s-task:%s (child %d) recv retrieve req from task:0x%x, reqId %" PRId64, pTask->id.idStr, pTask->selfChildId,
           pReq->srcTaskId, pReq->reqId);

    pData->type = STREAM_INPUT__DATA_RETRIEVE;
    pData->srcVgId = 0;
    // decode
    /*pData->blocks = pReq->data;*/
    /*pBlock->sourceVer = pReq->sourceVer;*/
    streamRetrieveReqToData(pReq, pData);
    if (tAppendDataToInputQueue(pTask, (SStreamQueueItem*)pData) == 0) {
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

// todo add log
int32_t streamTaskOutputResultBlock(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  int32_t code = 0;
  if (pTask->outputType == TASK_OUTPUT__TABLE) {
    pTask->tbSink.tbSinkFunc(pTask, pTask->tbSink.vnode, 0, pBlock->blocks);
    destroyStreamDataBlock(pBlock);
  } else if (pTask->outputType == TASK_OUTPUT__SMA) {
    pTask->smaSink.smaSink(pTask->smaSink.vnode, pTask->smaSink.smaId, pBlock->blocks);
    destroyStreamDataBlock(pBlock);
  } else {
    ASSERT(pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH);
    code = taosWriteQitem(pTask->outputQueue->queue, pBlock);
    if (code != 0) {  // todo failed to add it into the output queue, free it.
      return code;
    }

    streamDispatchStreamBlock(pTask);
  }

  return 0;
}

int32_t streamProcessDispatchMsg(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pRsp, bool exec) {
  qDebug("s-task:%s receive dispatch msg from taskId:0x%x(vgId:%d), msgLen:%" PRId64, pTask->id.idStr,
         pReq->upstreamTaskId, pReq->upstreamNodeId, pReq->totalLen);

  // todo add the input queue buffer limitation
  streamTaskEnqueueBlocks(pTask, pReq, pRsp);
  tDeleteStreamDispatchReq(pReq);

  if (exec) {
    if (streamTryExec(pTask) < 0) {
      return -1;
    }
  } else {
    streamSchedExec(pTask);
  }

  return 0;
}

int32_t streamProcessDispatchRsp(SStreamTask* pTask, SStreamDispatchRsp* pRsp, int32_t code) {
  qDebug("s-task:%s receive dispatch rsp, output status:%d code:%d", pTask->id.idStr, pRsp->inputStatus, code);

  if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    int32_t leftRsp = atomic_sub_fetch_32(&pTask->shuffleDispatcher.waitingRspCnt, 1);
    qDebug("s-task:%s is shuffle, left waiting rsp %d", pTask->id.idStr, leftRsp);
    if (leftRsp > 0) {
      return 0;
    }
  }

  int8_t old = atomic_exchange_8(&pTask->outputStatus, pRsp->inputStatus);
  ASSERT(old == TASK_OUTPUT_STATUS__WAIT);

  // the input queue of the (down stream) task that receive the output data is full, so the TASK_INPUT_STATUS_BLOCKED is rsp
  // todo we need to send EMPTY PACKAGE to detect if the input queue is available for output of upstream task, every 50 ms.
  if (pRsp->inputStatus == TASK_INPUT_STATUS__BLOCKED) {
    // TODO: init recover timer
    qError("s-task:%s inputQ of downstream task:0x%x is full, need to block output", pTask->id.idStr, pRsp->downstreamTaskId);

    atomic_store_8(&pTask->outputStatus, TASK_OUTPUT_STATUS__NORMAL);
    qError("s-task:%s ignore error, and reset task output status:%d", pTask->id.idStr, pTask->outputStatus);

    return 0;
  }

  // otherwise, continue dispatch the first block to down stream task in pipeline
  streamDispatchStreamBlock(pTask);
  return 0;
}

int32_t streamProcessRunReq(SStreamTask* pTask) {
  if (streamTryExec(pTask) < 0) {
    return -1;
  }

  /*if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {*/
  /*streamDispatchStreamBlock(pTask);*/
  /*}*/
  return 0;
}

int32_t streamProcessRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* pReq, SRpcMsg* pRsp) {
  qDebug("s-task:%s receive retrieve req from node %d taskId:0x%x", pTask->id.idStr, pReq->srcNodeId, pReq->srcTaskId);
  streamTaskEnqueueRetrieve(pTask, pReq, pRsp);

  ASSERT(pTask->taskLevel != TASK_LEVEL__SINK);
  streamSchedExec(pTask);
  return 0;
}

bool tInputQueueIsFull(const SStreamTask* pTask) {
  bool   isFull = taosQueueItemSize((pTask->inputQueue->queue)) >= STREAM_TASK_INPUT_QUEUEU_CAPACITY;
  double size = QUEUE_MEM_SIZE_IN_MB(pTask->inputQueue->queue);
  return (isFull || size >= STREAM_TASK_INPUT_QUEUEU_CAPACITY_IN_SIZE);
}

int32_t tAppendDataToInputQueue(SStreamTask* pTask, SStreamQueueItem* pItem) {
  int8_t  type = pItem->type;
  int32_t total = taosQueueItemSize(pTask->inputQueue->queue) + 1;
  double  size = QUEUE_MEM_SIZE_IN_MB(pTask->inputQueue->queue);

  if (type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamDataSubmit* px = (SStreamDataSubmit*)pItem;
    if ((pTask->taskLevel == TASK_LEVEL__SOURCE) && tInputQueueIsFull(pTask)) {
      qError("s-task:%s input queue is full, capacity(size:%d num:%dMiB), current(blocks:%d, size:%.2fMiB) stop to push data",
             pTask->id.idStr, STREAM_TASK_INPUT_QUEUEU_CAPACITY, STREAM_TASK_INPUT_QUEUEU_CAPACITY_IN_SIZE, total,
             size);
      streamDataSubmitDestroy(px);
      taosFreeQitem(pItem);
      return -1;
    }

    int32_t code = taosWriteQitem(pTask->inputQueue->queue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      streamDataSubmitDestroy(px);
      taosFreeQitem(pItem);
      return code;
    }

    qDebug("s-task:%s submit enqueue msgLen:%d ver:%" PRId64 ", total in queue:%d, size:%.2fMiB", pTask->id.idStr,
           px->submit.msgLen, px->submit.ver, total, size + px->submit.msgLen/1048576.0);
  } else if (type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__DATA_RETRIEVE ||
             type == STREAM_INPUT__REF_DATA_BLOCK) {
    if ((pTask->taskLevel == TASK_LEVEL__SOURCE) && (tInputQueueIsFull(pTask))) {
      qError("s-task:%s input queue is full, capacity:%d size:%d MiB, current(blocks:%d, size:%.2fMiB) abort",
             pTask->id.idStr, STREAM_TASK_INPUT_QUEUEU_CAPACITY, STREAM_TASK_INPUT_QUEUEU_CAPACITY_IN_SIZE, total,
             size);
      destroyStreamDataBlock((SStreamDataBlock*) pItem);
      return -1;
    }

    qDebug("s-task:%s data block enqueue, current(blocks:%d, size:%.2fMiB)", pTask->id.idStr, total, size);
    int32_t code = taosWriteQitem(pTask->inputQueue->queue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      destroyStreamDataBlock((SStreamDataBlock*) pItem);
      return code;
    }
  } else if (type == STREAM_INPUT__CHECKPOINT) {
    taosWriteQitem(pTask->inputQueue->queue, pItem);
  } else if (type == STREAM_INPUT__GET_RES) {
    // use the default memory limit, refactor later.
    taosWriteQitem(pTask->inputQueue->queue, pItem);
    qDebug("s-task:%s data res enqueue, current(blocks:%d, size:%.2fMiB)", pTask->id.idStr, total, size);
  }

  if (type != STREAM_INPUT__GET_RES && type != STREAM_INPUT__CHECKPOINT && pTask->triggerParam != 0) {
    atomic_val_compare_exchange_8(&pTask->triggerStatus, TASK_TRIGGER_STATUS__INACTIVE, TASK_TRIGGER_STATUS__ACTIVE);
  }

  return 0;
}

static void* streamQueueCurItem(SStreamQueue* queue) { return queue->qItem; }

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

void streamTaskInputFail(SStreamTask* pTask) { atomic_store_8(&pTask->inputStatus, TASK_INPUT_STATUS__FAILED); }