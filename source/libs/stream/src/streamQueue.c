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

SStreamQueue* streamQueueOpen(int64_t cap) {
  SStreamQueue* pQueue = taosMemoryCalloc(1, sizeof(SStreamQueue));
  if (pQueue == NULL) return NULL;
  pQueue->queue = taosOpenQueue();
  pQueue->qall = taosAllocateQall();
  if (pQueue->queue == NULL || pQueue->qall == NULL) {
    goto FAIL;
  }
  pQueue->status = STREAM_QUEUE__SUCESS;
  taosSetQueueCapacity(pQueue->queue, cap);
  taosSetQueueMemoryCapacity(pQueue->queue, cap * 1024);
  return pQueue;

FAIL:
  if (pQueue->queue) taosCloseQueue(pQueue->queue);
  if (pQueue->qall) taosFreeQall(pQueue->qall);
  taosMemoryFree(pQueue);
  return NULL;
}

void streamQueueClose(SStreamQueue* queue) {
  while (1) {
    void* qItem = streamQueueNextItem(queue);
    if (qItem) {
      streamFreeQitem(qItem);
    } else {
      break;
    }
  }
  taosFreeQall(queue->qall);
  taosCloseQueue(queue->queue);
  taosMemoryFree(queue);
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

#define MAX_STREAM_EXEC_BATCH_NUM 128
#define MIN_STREAM_EXEC_BATCH_NUM 16

// todo refactor:
// read data from input queue
typedef struct SQueueReader {
  SStreamQueue* pQueue;
  int32_t  taskLevel;
  int32_t  maxBlocks;        // maximum block in one batch
  int32_t  waitDuration;     // maximum wait time to format several block into a batch to process, unit: ms
} SQueueReader;

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
