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

#define MAX_STREAM_EXEC_BATCH_NUM 32
#define MIN_STREAM_EXEC_BATCH_NUM 4

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

void streamQueueClose(SStreamQueue* pQueue) {
  streamQueueCleanup(pQueue);

  taosFreeQall(pQueue->qall);
  taosCloseQueue(pQueue->queue);
  taosMemoryFree(pQueue);
}

void streamQueueCleanup(SStreamQueue* pQueue) {
  void* qItem = NULL;
  while ((qItem = streamQueueNextItem(pQueue)) != NULL) {
    streamFreeQitem(qItem);
  }
  pQueue->status = STREAM_QUEUE__SUCESS;
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


// todo refactor:
// read data from input queue
typedef struct SQueueReader {
  SStreamQueue* pQueue;
  int32_t  taskLevel;
  int32_t  maxBlocks;        // maximum block in one batch
  int32_t  waitDuration;     // maximum wait time to format several block into a batch to process, unit: ms
} SQueueReader;

#if 0
SStreamQueueItem* doReadMultiBlocksFromQueue(SQueueReader* pReader, const char* idstr) {
  int32_t numOfBlocks = 0;
  int32_t tryCount = 0;
  SStreamQueueItem* pRet = NULL;

  while (1) {
    SStreamQueueItem* qItem = streamQueueNextItem(pReader->pQueue);
    if (qItem == NULL) {
      if (pReader->taskLevel == TASK_LEVEL__SOURCE && numOfBlocks < MIN_STREAM_EXEC_BATCH_NUM && tryCount < pReader->waitDuration) {
        tryCount++;
        taosMsleep(1);
        qDebug("===stream===try again batchSize:%d", numOfBlocks);
        continue;
      }

      qDebug("===stream===break batchSize:%d", numOfBlocks);
      break;
    }

    if (pRet == NULL) {
      pRet = qItem;
      streamQueueProcessSuccess(pReader->pQueue);
      if (pReader->taskLevel == TASK_LEVEL__SINK) {
        break;
      }
    } else {
      // todo we need to sort the data block, instead of just appending into the array list.
      void* newRet = NULL;
      if ((newRet = streamMergeQueueItem(pRet, qItem)) == NULL) {
        streamQueueProcessFail(pReader->pQueue);
        break;
      } else {
        numOfBlocks++;
        pRet = newRet;
        streamQueueProcessSuccess(pReader->pQueue);
        if (numOfBlocks > pReader->maxBlocks) {
          qDebug("maximum blocks limit:%d reached, processing, %s", pReader->maxBlocks, idstr);
          break;
        }
      }
    }
  }

  return pRet;
}
#endif

int32_t extractBlocksFromInputQ(SStreamTask* pTask, SStreamQueueItem** pInput, int32_t* numOfBlocks) {
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
        if (pTask->info.taskLevel == TASK_LEVEL__SOURCE && (++retryTimes) < MAX_RETRY_TIMES) {
          taosMsleep(10);
          qDebug("===stream===try again batchSize:%d, retry:%d, %s", *numOfBlocks, retryTimes, id);
          continue;
        }

        qDebug("===stream===break batchSize:%d, %s", *numOfBlocks, id);
        return TSDB_CODE_SUCCESS;
      }

      qDebug("s-task:%s sink task handle result block one-by-one", id);
      *numOfBlocks = 1;
      *pInput = qItem;
      return TSDB_CODE_SUCCESS;
    }
  }

  // non sink task
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
    if ((qItem->type == STREAM_INPUT__CHECKPOINT || qItem->type == STREAM_INPUT__CHECKPOINT_TRIGGER)) {
      if (*pInput == NULL) {
        qDebug("s-task:%s checkpoint msg extracted, start to process immediately", id);
        *numOfBlocks = 1;
        *pInput = qItem;
        return TSDB_CODE_SUCCESS;
      } else {
        // previous existed blocks needs to be handle, before handle the checkpoint msg block
        qDebug("s-task:%s checkpoint msg extracted, handle previous block first, numOfBlocks:%d", id, *numOfBlocks);
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
          qError("s-task:%s failed to merge blocks from inputQ, numOfBlocks:%d", id, *numOfBlocks);
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
