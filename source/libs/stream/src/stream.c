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

#define STREAM_TASK_INPUT_QUEUE_CAPACITY          20480
#define STREAM_TASK_INPUT_QUEUE_CAPACITY_IN_SIZE  (30)
#define ONE_MB_F                                  (1048576.0)
#define QUEUE_MEM_SIZE_IN_MB(_q)                  (taosQueueMemorySize(_q) / ONE_MB_F)

SStreamGlobalEnv streamEnv;

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

char* createStreamTaskIdStr(int64_t streamId, int32_t taskId) {
  char buf[128] = {0};
  sprintf(buf, "0x%" PRIx64 "-0x%x", streamId, taskId);
  return taosStrdup(buf);
}

static void streamSchedByTimer(void* param, void* tmrId) {
  SStreamTask* pTask = (void*)param;

  int8_t status = atomic_load_8(&pTask->triggerStatus);
  qDebug("s-task:%s in scheduler, trigger status:%d, next:%dms", pTask->id.idStr, status, (int32_t)pTask->triggerParam);

  if (streamTaskShouldStop(&pTask->status) || streamTaskShouldPause(&pTask->status)) {
    streamMetaReleaseTask(NULL, pTask);
    qDebug("s-task:%s jump out of schedTimer", pTask->id.idStr);
    return;
  }

  if (status == TASK_TRIGGER_STATUS__ACTIVE) {
    SStreamTrigger* pTrigger = taosAllocateQitem(sizeof(SStreamTrigger), DEF_QITEM, 0);
    if (pTrigger == NULL) {
      return;
    }

    pTrigger->type = STREAM_INPUT__GET_RES;
    pTrigger->pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
    if (pTrigger->pBlock == NULL) {
      taosFreeQitem(pTrigger);
      return;
    }

    atomic_store_8(&pTask->triggerStatus, TASK_TRIGGER_STATUS__INACTIVE);
    pTrigger->pBlock->info.type = STREAM_GET_ALL;
    if (tAppendDataToInputQueue(pTask, (SStreamQueueItem*)pTrigger) < 0) {
      taosFreeQitem(pTrigger);
      taosTmrReset(streamSchedByTimer, (int32_t)pTask->triggerParam, pTask, streamEnv.timer, &pTask->schedTimer);
      return;
    }

    streamSchedExec(pTask);
  }

  taosTmrReset(streamSchedByTimer, (int32_t)pTask->triggerParam, pTask, streamEnv.timer, &pTask->schedTimer);
}

int32_t streamSetupScheduleTrigger(SStreamTask* pTask) {
  if (pTask->triggerParam != 0 && pTask->info.fillHistory == 0) {
    int32_t ref = atomic_add_fetch_32(&pTask->refCnt, 1);
    ASSERT(ref == 2 && pTask->schedTimer == NULL);

    qDebug("s-task:%s setup scheduler trigger, delay:%"PRId64" ms", pTask->id.idStr, pTask->triggerParam);

    pTask->schedTimer = taosTmrStart(streamSchedByTimer, (int32_t)pTask->triggerParam, pTask, streamEnv.timer);
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
      qError("failed to create msg to aunch s-task:%s, reason out of memory", pTask->id.idStr);
      return -1;
    }

    pRunReq->head.vgId = pTask->info.nodeId;
    pRunReq->streamId = pTask->id.streamId;
    pRunReq->taskId = pTask->id.taskId;

    qDebug("trigger to run s-task:%s", pTask->id.idStr);

    SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
    tmsgPutToQueue(pTask->pMsgCb, STREAM_QUEUE, &msg);
  } else {
    qDebug("s-task:%s not launch task since sched status:%d", pTask->id.idStr, pTask->status.schedStatus);
  }

  return 0;
}

int32_t streamTaskEnqueueRetrieve(SStreamTask* pTask, SStreamRetrieveReq* pReq, SRpcMsg* pRsp) {
  SStreamDataBlock* pData = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, 0);
  int8_t            status = TASK_INPUT_STATUS__NORMAL;

  // enqueue
  if (pData != NULL) {
    qDebug("s-task:%s (child %d) recv retrieve req from task:0x%x(vgId:%d), reqId:0x%" PRIx64, pTask->id.idStr, pTask->info.selfChildId,
           pReq->srcTaskId, pReq->srcNodeId, pReq->reqId);

    pData->type = STREAM_INPUT__DATA_RETRIEVE;
    pData->srcVgId = 0;
    streamRetrieveReqToData(pReq, pData);
    if (tAppendDataToInputQueue(pTask, (SStreamQueueItem*)pData) == 0) {
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

// todo add log
int32_t streamTaskOutputResultBlock(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  int32_t code = 0;
  int32_t type = pTask->outputInfo.type;
  if (type == TASK_OUTPUT__TABLE) {
    pTask->tbSink.tbSinkFunc(pTask, pTask->tbSink.vnode, 0, pBlock->blocks);
    destroyStreamDataBlock(pBlock);
  } else if (type == TASK_OUTPUT__SMA) {
    pTask->smaSink.smaSink(pTask->smaSink.vnode, pTask->smaSink.smaId, pBlock->blocks);
    destroyStreamDataBlock(pBlock);
  } else {
    ASSERT(type == TASK_OUTPUT__FIXED_DISPATCH || type == TASK_OUTPUT__SHUFFLE_DISPATCH);
    code = taosWriteQitem(pTask->outputInfo.queue->queue, pBlock);
    if (code != 0) {  // todo failed to add it into the output queue, free it.
      return code;
    }

    streamDispatchStreamBlock(pTask);
  }

  return 0;
}



static int32_t streamTaskAppendInputBlocks(SStreamTask* pTask, const SStreamDispatchReq* pReq) {
  int8_t status = 0;

  SStreamDataBlock* pBlock = createStreamDataFromDispatchMsg(pReq, pReq->type, pReq->srcVgId);
  if (pBlock == NULL) {
    streamTaskInputFail(pTask);
    status = TASK_INPUT_STATUS__FAILED;
    qError("vgId:%d, s-task:%s failed to receive dispatch msg, reason: out of memory", pTask->pMeta->vgId,
           pTask->id.idStr);
  } else {
    int32_t code = tAppendDataToInputQueue(pTask, (SStreamQueueItem*)pBlock);
    // input queue is full, upstream is blocked now
    status = (code == TSDB_CODE_SUCCESS) ? TASK_INPUT_STATUS__NORMAL : TASK_INPUT_STATUS__BLOCKED;
  }

  return status;
}

static int32_t buildDispatchRsp(const SStreamTask* pTask, const SStreamDispatchReq* pReq, int32_t status, void** pBuf) {
  *pBuf = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamDispatchRsp));
  if (*pBuf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  ((SMsgHead*)(*pBuf))->vgId = htonl(pReq->upstreamNodeId);
  SStreamDispatchRsp* pDispatchRsp = POINTER_SHIFT((*pBuf), sizeof(SMsgHead));

  pDispatchRsp->inputStatus = status;
  pDispatchRsp->streamId = htobe64(pReq->streamId);
  pDispatchRsp->upstreamNodeId = htonl(pReq->upstreamNodeId);
  pDispatchRsp->upstreamTaskId = htonl(pReq->upstreamTaskId);
  pDispatchRsp->downstreamNodeId = htonl(pTask->info.nodeId);
  pDispatchRsp->downstreamTaskId = htonl(pTask->id.taskId);

  return TSDB_CODE_SUCCESS;
}

void streamTaskCloseUpstreamInput(SStreamTask* pTask, int32_t taskId) {
  SStreamChildEpInfo* pInfo = streamTaskGetUpstreamTaskEpInfo(pTask, taskId);
  if (pInfo != NULL) {
    pInfo->dataAllowed = false;
  }
}

int32_t streamProcessDispatchMsg(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pRsp, bool exec) {
  qDebug("s-task:%s receive dispatch msg from taskId:0x%x(vgId:%d), msgLen:%" PRId64, pTask->id.idStr,
         pReq->upstreamTaskId, pReq->upstreamNodeId, pReq->totalLen);

  int32_t status = 0;

  SStreamChildEpInfo* pInfo = streamTaskGetUpstreamTaskEpInfo(pTask, pReq->upstreamTaskId);
  ASSERT(pInfo != NULL);

  if (!pInfo->dataAllowed) {
    qWarn("s-task:%s data from task:0x%x is denied, since inputQ is closed for it", pTask->id.idStr, pReq->upstreamTaskId);
    status = TASK_INPUT_STATUS__BLOCKED;
  } else {
    // Current task has received the checkpoint req from the upstream task, from which the message should all be blocked
    if (pReq->type == STREAM_INPUT__CHECKPOINT_TRIGGER) {
      streamTaskCloseUpstreamInput(pTask, pReq->upstreamTaskId);
      qDebug("s-task:%s close inputQ for upstream:0x%x", pTask->id.idStr, pReq->upstreamTaskId);
    }

    status = streamTaskAppendInputBlocks(pTask, pReq);
  }

  {
    // do send response with the input status
    int32_t code = buildDispatchRsp(pTask, pReq, status, &pRsp->pCont);
    if (code != TSDB_CODE_SUCCESS) {
      // todo handle failure
      return code;
    }

    pRsp->contLen = sizeof(SMsgHead) + sizeof(SStreamDispatchRsp);
    tmsgSendRsp(pRsp);
  }

  tDeleteStreamDispatchReq(pReq);
  streamSchedExec(pTask);

  return 0;
}

//int32_t streamProcessDispatchMsg(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pRsp, bool exec) {
//  qDebug("s-task:%s receive dispatch msg from taskId:0x%x(vgId:%d), msgLen:%" PRId64, pTask->id.idStr,
//         pReq->upstreamTaskId, pReq->upstreamNodeId, pReq->totalLen);
//
//  // todo add the input queue buffer limitation
//  streamTaskEnqueueBlocks(pTask, pReq, pRsp);
//  tDeleteStreamDispatchReq(pReq);
//
//  if (exec) {
//    if (streamTryExec(pTask) < 0) {
//      return -1;
//    }
//  } else {
//    streamSchedExec(pTask);
//  }
//
//  return 0;
//}

int32_t streamProcessRunReq(SStreamTask* pTask) {
  if (streamTryExec(pTask) < 0) {
    return -1;
  }

  return 0;
}

int32_t streamProcessRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* pReq, SRpcMsg* pRsp) {
  streamTaskEnqueueRetrieve(pTask, pReq, pRsp);
  ASSERT(pTask->info.taskLevel != TASK_LEVEL__SINK);
  streamSchedExec(pTask);
  return 0;
}

bool tInputQueueIsFull(const SStreamTask* pTask) {
  bool   isFull = taosQueueItemSize((pTask->inputQueue->queue)) >= STREAM_TASK_INPUT_QUEUE_CAPACITY;
  double size = QUEUE_MEM_SIZE_IN_MB(pTask->inputQueue->queue);
  return (isFull || size >= STREAM_TASK_INPUT_QUEUE_CAPACITY_IN_SIZE);
}

int32_t tAppendDataToInputQueue(SStreamTask* pTask, SStreamQueueItem* pItem) {
  int8_t  type = pItem->type;
  int32_t total = taosQueueItemSize(pTask->inputQueue->queue) + 1;
  double  size = QUEUE_MEM_SIZE_IN_MB(pTask->inputQueue->queue);

  if (type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamDataSubmit* px = (SStreamDataSubmit*)pItem;
    if ((pTask->info.taskLevel == TASK_LEVEL__SOURCE) && tInputQueueIsFull(pTask)) {
      qError("s-task:%s input queue is full, capacity(size:%d num:%dMiB), current(blocks:%d, size:%.2fMiB) stop to push data",
             pTask->id.idStr, STREAM_TASK_INPUT_QUEUE_CAPACITY, STREAM_TASK_INPUT_QUEUE_CAPACITY_IN_SIZE, total,
             size);
      streamDataSubmitDestroy(px);
      taosFreeQitem(pItem);
      return -1;
    }

    int32_t msgLen = px->submit.msgLen;
    int64_t ver = px->submit.ver;

    int32_t code = taosWriteQitem(pTask->inputQueue->queue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      streamDataSubmitDestroy(px);
      taosFreeQitem(pItem);
      return code;
    }

    // use the local variable to avoid the pItem be freed by other threads, since it has been put into queue already.
    qDebug("s-task:%s submit enqueue msgLen:%d ver:%" PRId64 ", total in queue:%d, size:%.2fMiB", pTask->id.idStr,
           msgLen, ver, total, size + msgLen/1048576.0);
  } else if (type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__DATA_RETRIEVE ||
             type == STREAM_INPUT__REF_DATA_BLOCK) {
    if ((pTask->info.taskLevel == TASK_LEVEL__SOURCE) && (tInputQueueIsFull(pTask))) {
      qError("s-task:%s input queue is full, capacity:%d size:%d MiB, current(blocks:%d, size:%.2fMiB) abort",
             pTask->id.idStr, STREAM_TASK_INPUT_QUEUE_CAPACITY, STREAM_TASK_INPUT_QUEUE_CAPACITY_IN_SIZE, total,
             size);
      destroyStreamDataBlock((SStreamDataBlock*) pItem);
      return -1;
    }

    qDebug("s-task:%s blockdata enqueue, total in queue:%d, size:%.2fMiB", pTask->id.idStr, total, size);
    int32_t code = taosWriteQitem(pTask->inputQueue->queue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      destroyStreamDataBlock((SStreamDataBlock*) pItem);
      return code;
    }
  } else if (type == STREAM_INPUT__CHECKPOINT || type == STREAM_INPUT__TRANS_STATE) {
    taosWriteQitem(pTask->inputQueue->queue, pItem);
    qDebug("s-task:%s trans-state blockdata enqueue, total in queue:%d, size:%.2fMiB", pTask->id.idStr, total, size);
  } else if (type == STREAM_INPUT__GET_RES) {
    // use the default memory limit, refactor later.
    taosWriteQitem(pTask->inputQueue->queue, pItem);
    qDebug("s-task:%s data res enqueue, current(blocks:%d, size:%.2fMiB)", pTask->id.idStr, total, size);
  }

  if (type != STREAM_INPUT__GET_RES && type != STREAM_INPUT__CHECKPOINT && pTask->triggerParam != 0) {
    atomic_val_compare_exchange_8(&pTask->triggerStatus, TASK_TRIGGER_STATUS__INACTIVE, TASK_TRIGGER_STATUS__ACTIVE);
    qDebug("s-task:%s new data arrived, active the trigger, trigerStatus:%d", pTask->id.idStr, pTask->triggerStatus);
  }

  return 0;
}

static void* streamQueueCurItem(SStreamQueue* queue) { return queue->qItem; }

void* streamQueueNextItem(SStreamQueue* pQueue) {
  int8_t flag = atomic_exchange_8(&pQueue->status, STREAM_QUEUE__PROCESSING);

  if (flag == STREAM_QUEUE__FAILED) {
    ASSERT(pQueue->qItem != NULL);
    return streamQueueCurItem(pQueue);
  } else {
    pQueue->qItem = NULL;
    taosGetQitem(pQueue->qall, &pQueue->qItem);
    if (pQueue->qItem == NULL) {
      taosReadAllQitems(pQueue->queue, pQueue->qall);
      taosGetQitem(pQueue->qall, &pQueue->qItem);
    }

    return streamQueueCurItem(pQueue);
  }
}

void streamTaskInputFail(SStreamTask* pTask) { atomic_store_8(&pTask->inputStatus, TASK_INPUT_STATUS__FAILED); }

SStreamChildEpInfo * streamTaskGetUpstreamTaskEpInfo(SStreamTask* pTask, int32_t taskId) {
  int32_t num = taosArrayGetSize(pTask->pUpstreamEpInfoList);
  for(int32_t i = 0; i < num; ++i) {
    SStreamChildEpInfo* pInfo = taosArrayGetP(pTask->pUpstreamEpInfoList, i);
    if (pInfo->taskId == taskId) {
      return pInfo;
    }
  }

  return NULL;
}