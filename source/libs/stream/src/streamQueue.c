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

#define MAX_STREAM_EXEC_BATCH_NUM                32
#define MIN_STREAM_EXEC_BATCH_NUM                4
#define STREAM_TASK_INPUT_QUEUE_CAPACITY         20480
#define STREAM_TASK_INPUT_QUEUE_CAPACITY_IN_SIZE (30)
#define QUEUE_MEM_SIZE_IN_MB(_q)                 (taosQueueMemorySize(_q) / ONE_MB_F)

// todo refactor:
// read data from input queue
typedef struct SQueueReader {
  SStreamQueue* pQueue;
  int32_t       taskLevel;
  int32_t       maxBlocks;     // maximum block in one batch
  int32_t       waitDuration;  // maximum wait time to format several block into a batch to process, unit: ms
} SQueueReader;

static void streamQueueCleanup(SStreamQueue* pQueue) {
  void* qItem = NULL;
  while ((qItem = streamQueueNextItem(pQueue)) != NULL) {
    streamFreeQitem(qItem);
  }
  pQueue->status = STREAM_QUEUE__SUCESS;
}

static void* streamQueueCurItem(SStreamQueue* queue) { return queue->qItem; }

SStreamQueue* streamQueueOpen(int64_t cap) {
  SStreamQueue* pQueue = taosMemoryCalloc(1, sizeof(SStreamQueue));
  if (pQueue == NULL) {
    return NULL;
  }

  pQueue->queue = taosOpenQueue();
  pQueue->qall = taosAllocateQall();

  if (pQueue->queue == NULL || pQueue->qall == NULL) {
    if (pQueue->queue) taosCloseQueue(pQueue->queue);
    if (pQueue->qall) taosFreeQall(pQueue->qall);
    taosMemoryFree(pQueue);
    return NULL;
  }

  pQueue->status = STREAM_QUEUE__SUCESS;
  taosSetQueueCapacity(pQueue->queue, cap);
  taosSetQueueMemoryCapacity(pQueue->queue, cap * 1024);
  return pQueue;
}

void streamQueueClose(SStreamQueue* pQueue, int32_t taskId) {
  qDebug("s-task:0x%x free the queue:%p, items in queue:%d", taskId, pQueue->queue, taosQueueItemSize(pQueue->queue));
  streamQueueCleanup(pQueue);

  taosFreeQall(pQueue->qall);
  taosCloseQueue(pQueue->queue);
  taosMemoryFree(pQueue);
}

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

#if 0
bool streamQueueResEmpty(const SStreamQueueRes* pRes) {
  //
  return true;
}
int64_t           streamQueueResSize(const SStreamQueueRes* pRes) { return pRes->size; }
SStreamQueueNode* streamQueueResFront(SStreamQueueRes* pRes) { return pRes->head; }
SStreamQueueNode* streamQueueResPop(SStreamQueueRes* pRes) {
  SStreamQueueNode* pRet = pRes->head;
  pRes->head = pRes->head->next;
  return pRet;
}

void streamQueueResClear(SStreamQueueRes* pRes) {
  while (pRes->head) {
    SStreamQueueNode* pNode = pRes->head;
    streamFreeQitem(pRes->head->item);
    pRes->head = pNode;
  }
}

SStreamQueueRes streamQueueBuildRes(SStreamQueueNode* pTail) {
  int64_t           size = 0;
  SStreamQueueNode* head = NULL;

  while (pTail) {
    SStreamQueueNode* pTmp = pTail->next;
    pTail->next = head;
    head = pTail;
    pTail = pTmp;
    size++;
  }

  return (SStreamQueueRes){.head = head, .size = size};
}

bool    streamQueueHasTask(const SStreamQueue1* pQueue) { return atomic_load_ptr(pQueue->pHead); }
int32_t streamQueuePush(SStreamQueue1* pQueue, SStreamQueueItem* pItem) {
  SStreamQueueNode* pNode = taosMemoryMalloc(sizeof(SStreamQueueNode));
  pNode->item = pItem;
  SStreamQueueNode* pHead = atomic_load_ptr(pQueue->pHead);
  while (1) {
    pNode->next = pHead;
    SStreamQueueNode* pOld = atomic_val_compare_exchange_ptr(pQueue->pHead, pHead, pNode);
    if (pOld == pHead) {
      break;
    }
  }
  return 0;
}

SStreamQueueRes streamQueueGetRes(SStreamQueue1* pQueue) {
  SStreamQueueNode* pNode = atomic_exchange_ptr(pQueue->pHead, NULL);
  if (pNode) return streamQueueBuildRes(pNode);
  return (SStreamQueueRes){0};
}
#endif

bool streamQueueIsFull(const STaosQueue* pQueue) {
  bool   isFull = taosQueueItemSize((STaosQueue*) pQueue) >= STREAM_TASK_INPUT_QUEUE_CAPACITY;
  double size = QUEUE_MEM_SIZE_IN_MB((STaosQueue*) pQueue);
  return (isFull || size >= STREAM_TASK_INPUT_QUEUE_CAPACITY_IN_SIZE);
}

int32_t streamTaskGetDataFromInputQ(SStreamTask* pTask, SStreamQueueItem** pInput, int32_t* numOfBlocks) {
  int32_t     retryTimes = 0;
  int32_t     MAX_RETRY_TIMES = 5;
  const char* id = pTask->id.idStr;

  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {  // extract block from inputQ, one-by-one
    while (1) {
      if (streamTaskShouldPause(&pTask->status) || streamTaskShouldStop(&pTask->status)) {
        qDebug("s-task:%s task should pause, extract input blocks:%d", pTask->id.idStr, *numOfBlocks);
        return TSDB_CODE_SUCCESS;
      }

      SStreamQueueItem* qItem = streamQueueNextItem(pTask->inputQueue);
      if (qItem == NULL) {
        qDebug("===stream===break batchSize:%d, %s", *numOfBlocks, id);
        return TSDB_CODE_SUCCESS;
      }

      qDebug("s-task:%s sink task handle block one-by-one, type:%d", id, qItem->type);

      *numOfBlocks = 1;
      *pInput = qItem;
      return TSDB_CODE_SUCCESS;
    }
  }

  while (1) {
    if (streamTaskShouldPause(&pTask->status) || streamTaskShouldStop(&pTask->status)) {
      qDebug("s-task:%s task should pause, extract input blocks:%d", pTask->id.idStr, *numOfBlocks);
      return TSDB_CODE_SUCCESS;
    }

    SStreamQueueItem* qItem = streamQueueNextItem(pTask->inputQueue);
    if (qItem == NULL) {
      if (pTask->info.taskLevel == TASK_LEVEL__SOURCE && (++retryTimes) < MAX_RETRY_TIMES) {
        taosMsleep(10);
        qDebug("===stream===try again batchSize:%d, retry:%d, %s", *numOfBlocks, retryTimes, id);
        continue;
      }

      qDebug("===stream===break batchSize:%d, %s", *numOfBlocks, id);
      return TSDB_CODE_SUCCESS;
    }

    // do not merge blocks for sink node and check point data block
    if (qItem->type == STREAM_INPUT__CHECKPOINT || qItem->type == STREAM_INPUT__CHECKPOINT_TRIGGER ||
        qItem->type == STREAM_INPUT__TRANS_STATE) {
      const char* p = streamGetBlockTypeStr(qItem->type);

      if (*pInput == NULL) {
        qDebug("s-task:%s %s msg extracted, start to process immediately", id, p);

        *numOfBlocks = 1;
        *pInput = qItem;
        return TSDB_CODE_SUCCESS;
      } else {
        // previous existed blocks needs to be handle, before handle the checkpoint msg block
        qDebug("s-task:%s %s msg extracted, handle previous blocks, numOfBlocks:%d", id, p, *numOfBlocks);
        streamQueueProcessFail(pTask->inputQueue);
        return TSDB_CODE_SUCCESS;
      }
    } else {
      if (*pInput == NULL) {
        ASSERT((*numOfBlocks) == 0);
        *pInput = qItem;
      } else {
        // todo we need to sort the data block, instead of just appending into the array list.
        void* newRet = streamMergeQueueItem(*pInput, qItem);
        if (newRet == NULL) {
          if (terrno != 0) {
            qError("s-task:%s failed to merge blocks from inputQ, numOfBlocks:%d, code:%s", id, *numOfBlocks,
                   tstrerror(terrno));
          }

          streamQueueProcessFail(pTask->inputQueue);
          return TSDB_CODE_SUCCESS;
        }

        *pInput = newRet;
      }

      *numOfBlocks += 1;
      streamQueueProcessSuccess(pTask->inputQueue);

      if (*numOfBlocks >= MAX_STREAM_EXEC_BATCH_NUM) {
        qDebug("s-task:%s batch size limit:%d reached, start to process blocks", id, MAX_STREAM_EXEC_BATCH_NUM);
        return TSDB_CODE_SUCCESS;
      }
    }
  }
}

int32_t streamTaskPutDataIntoInputQ(SStreamTask* pTask, SStreamQueueItem* pItem) {
  int8_t  type = pItem->type;
  int32_t total = taosQueueItemSize(pTask->inputQueue->queue) + 1;
  double  size = QUEUE_MEM_SIZE_IN_MB(pTask->inputQueue->queue);

  if (type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamDataSubmit* px = (SStreamDataSubmit*)pItem;
    if ((pTask->info.taskLevel == TASK_LEVEL__SOURCE) && streamQueueIsFull(pTask->inputQueue->queue)) {
      qError(
          "s-task:%s inputQ is full, capacity(size:%d num:%dMiB), current(blocks:%d, size:%.2fMiB) stop to push data",
          pTask->id.idStr, STREAM_TASK_INPUT_QUEUE_CAPACITY, STREAM_TASK_INPUT_QUEUE_CAPACITY_IN_SIZE, total, size);
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
           msgLen, ver, total, size + SIZE_IN_MB(msgLen));
  } else if (type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__DATA_RETRIEVE ||
             type == STREAM_INPUT__REF_DATA_BLOCK) {
    if (streamQueueIsFull(pTask->inputQueue->queue)) {
      qError("s-task:%s input queue is full, capacity:%d size:%d MiB, current(blocks:%d, size:%.2fMiB) abort",
             pTask->id.idStr, STREAM_TASK_INPUT_QUEUE_CAPACITY, STREAM_TASK_INPUT_QUEUE_CAPACITY_IN_SIZE, total, size);
      destroyStreamDataBlock((SStreamDataBlock*)pItem);
      return -1;
    }

    qDebug("s-task:%s blockdata enqueue, total in queue:%d, size:%.2fMiB", pTask->id.idStr, total, size);
    int32_t code = taosWriteQitem(pTask->inputQueue->queue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      destroyStreamDataBlock((SStreamDataBlock*)pItem);
      return code;
    }
  } else if (type == STREAM_INPUT__CHECKPOINT || type == STREAM_INPUT__CHECKPOINT_TRIGGER ||
             type == STREAM_INPUT__TRANS_STATE) {
    taosWriteQitem(pTask->inputQueue->queue, pItem);
    qDebug("s-task:%s level:%d %s blockdata enqueue, total in queue:%d, size:%.2fMiB", pTask->id.idStr,
           pTask->info.taskLevel, streamGetBlockTypeStr(type), total, size);
  } else if (type == STREAM_INPUT__GET_RES) {
    // use the default memory limit, refactor later.
    taosWriteQitem(pTask->inputQueue->queue, pItem);
    qDebug("s-task:%s data res enqueue, current(blocks:%d, size:%.2fMiB)", pTask->id.idStr, total, size);
  } else {
    ASSERT(0);
  }

  if (type != STREAM_INPUT__GET_RES && type != STREAM_INPUT__CHECKPOINT && pTask->triggerParam != 0) {
    atomic_val_compare_exchange_8(&pTask->triggerStatus, TASK_TRIGGER_STATUS__INACTIVE, TASK_TRIGGER_STATUS__ACTIVE);
    qDebug("s-task:%s new data arrived, active the trigger, triggerStatus:%d", pTask->id.idStr, pTask->triggerStatus);
  }

  return 0;
}
