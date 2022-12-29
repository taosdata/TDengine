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

SStreamQueue* streamQueueOpen() {
  SStreamQueue* pQueue = taosMemoryCalloc(1, sizeof(SStreamQueue));
  if (pQueue == NULL) return NULL;
  pQueue->queue = taosOpenQueue();
  pQueue->qall = taosAllocateQall();
  if (pQueue->queue == NULL || pQueue->qall == NULL) {
    goto FAIL;
  }
  pQueue->status = STREAM_QUEUE__SUCESS;
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
