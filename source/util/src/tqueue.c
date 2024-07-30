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

#define _DEFAULT_SOURCE
#include "tqueue.h"
#include "taoserror.h"
#include "tlog.h"
#include "tutil.h"

int64_t tsRpcQueueMemoryAllowed = 0;
int64_t tsRpcQueueMemoryUsed = 0;

struct STaosQueue {
  STaosQnode   *head;
  STaosQnode   *tail;
  STaosQueue   *next;     // for queue set
  STaosQset    *qset;     // for queue set
  void         *ahandle;  // for queue set
  FItem         itemFp;
  FItems        itemsFp;
  TdThreadMutex mutex;
  int64_t       memOfItems;
  int32_t       numOfItems;
  int64_t       threadId;
  int64_t       memLimit;
  int64_t       itemLimit;
};

struct STaosQset {
  STaosQueue   *head;
  STaosQueue   *current;
  TdThreadMutex mutex;
  tsem_t        sem;
  int32_t       numOfQueues;
  int32_t       numOfItems;
};

struct STaosQall {
  STaosQnode *current;
  STaosQnode *start;
  int32_t     numOfItems;
  int64_t     memOfItems;
  int32_t     unAccessedNumOfItems;
  int64_t     unAccessMemOfItems;
};

void taosSetQueueMemoryCapacity(STaosQueue *queue, int64_t cap) { queue->memLimit = cap; }
void taosSetQueueCapacity(STaosQueue *queue, int64_t size) { queue->itemLimit = size; }

int32_t taosOpenQueue(STaosQueue **queue) {
  *queue = taosMemoryCalloc(1, sizeof(STaosQueue));
  if (*queue == NULL) {
    return (terrno = TSDB_CODE_OUT_OF_MEMORY);
  }

  int32_t code = taosThreadMutexInit(&(*queue)->mutex, NULL);
  if (code) {
    taosMemoryFreeClear(*queue);
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }

  uDebug("queue:%p is opened", queue);
  return 0;
}

void taosSetQueueFp(STaosQueue *queue, FItem itemFp, FItems itemsFp) {
  if (queue == NULL) return;
  queue->itemFp = itemFp;
  queue->itemsFp = itemsFp;
}

void taosCloseQueue(STaosQueue *queue) {
  if (queue == NULL) return;
  STaosQnode *pTemp;
  STaosQset  *qset;

  (void)taosThreadMutexLock(&queue->mutex);
  STaosQnode *pNode = queue->head;
  queue->head = NULL;
  qset = queue->qset;
  (void)taosThreadMutexUnlock(&queue->mutex);

  if (queue->qset) {
    taosRemoveFromQset(qset, queue);
  }

  while (pNode) {
    pTemp = pNode;
    pNode = pNode->next;
    taosMemoryFree(pTemp);
  }

  (void)taosThreadMutexDestroy(&queue->mutex);
  taosMemoryFree(queue);

  uDebug("queue:%p is closed", queue);
}

bool taosQueueEmpty(STaosQueue *queue) {
  if (queue == NULL) return true;

  bool empty = false;
  (void)taosThreadMutexLock(&queue->mutex);
  if (queue->head == NULL && queue->tail == NULL && queue->numOfItems == 0 /*&& queue->memOfItems == 0*/) {
    empty = true;
  }
  (void)taosThreadMutexUnlock(&queue->mutex);

  return empty;
}

void taosUpdateItemSize(STaosQueue *queue, int32_t items) {
  if (queue == NULL) return;

  (void)taosThreadMutexLock(&queue->mutex);
  queue->numOfItems -= items;
  (void)taosThreadMutexUnlock(&queue->mutex);
}

int32_t taosQueueItemSize(STaosQueue *queue) {
  if (queue == NULL) return 0;

  (void)taosThreadMutexLock(&queue->mutex);
  int32_t numOfItems = queue->numOfItems;
  (void)taosThreadMutexUnlock(&queue->mutex);

  uTrace("queue:%p, numOfItems:%d memOfItems:%" PRId64, queue, queue->numOfItems, queue->memOfItems);
  return numOfItems;
}

int64_t taosQueueMemorySize(STaosQueue *queue) {
  (void)taosThreadMutexLock(&queue->mutex);
  int64_t memOfItems = queue->memOfItems;
  (void)taosThreadMutexUnlock(&queue->mutex);
  return memOfItems;
}

int32_t taosAllocateQitem(int32_t size, EQItype itype, int64_t dataSize, void **item) {
  *item = NULL;

  STaosQnode *pNode = taosMemoryCalloc(1, sizeof(STaosQnode) + size);
  if (pNode == NULL) {
    return terrno = TSDB_CODE_OUT_OF_MEMORY;
  }

  pNode->dataSize = dataSize;
  pNode->size = size;
  pNode->itype = itype;
  pNode->timestamp = taosGetTimestampUs();

  if (itype == RPC_QITEM) {
    int64_t alloced = atomic_add_fetch_64(&tsRpcQueueMemoryUsed, size + dataSize);
    if (alloced > tsRpcQueueMemoryAllowed) {
      uError("failed to alloc qitem, size:%" PRId64 " alloc:%" PRId64 " allowed:%" PRId64, size + dataSize, alloced,
             tsRpcQueueMemoryAllowed);
      (void)atomic_sub_fetch_64(&tsRpcQueueMemoryUsed, size + dataSize);
      taosMemoryFree(pNode);
      return (terrno = TSDB_CODE_OUT_OF_RPC_MEMORY_QUEUE);
    }
    uTrace("item:%p, node:%p is allocated, alloc:%" PRId64, pNode->item, pNode, alloced);
  } else {
    uTrace("item:%p, node:%p is allocated", pNode->item, pNode);
  }

  *item = pNode->item;
  return 0;
}

void taosFreeQitem(void *pItem) {
  if (pItem == NULL) return;

  STaosQnode *pNode = (STaosQnode *)((char *)pItem - sizeof(STaosQnode));
  if (pNode->itype == RPC_QITEM) {
    int64_t alloced = atomic_sub_fetch_64(&tsRpcQueueMemoryUsed, pNode->size + pNode->dataSize);
    uTrace("item:%p, node:%p is freed, alloc:%" PRId64, pItem, pNode, alloced);
  } else {
    uTrace("item:%p, node:%p is freed", pItem, pNode);
  }

  taosMemoryFree(pNode);
}

int32_t taosWriteQitem(STaosQueue *queue, void *pItem) {
  int32_t     code = 0;
  STaosQnode *pNode = (STaosQnode *)(((char *)pItem) - sizeof(STaosQnode));
  pNode->timestamp = taosGetTimestampUs();
  pNode->next = NULL;

  (void)taosThreadMutexLock(&queue->mutex);
  if (queue->memLimit > 0 && (queue->memOfItems + pNode->size + pNode->dataSize) > queue->memLimit) {
    code = TSDB_CODE_UTIL_QUEUE_OUT_OF_MEMORY;
    uError("item:%p failed to put into queue:%p, queue mem limit: %" PRId64 ", reason: %s" PRId64, pItem, queue,
           queue->memLimit, tstrerror(code));

    (void)taosThreadMutexUnlock(&queue->mutex);
    return code;
  } else if (queue->itemLimit > 0 && queue->numOfItems + 1 > queue->itemLimit) {
    code = TSDB_CODE_UTIL_QUEUE_OUT_OF_MEMORY;
    uError("item:%p failed to put into queue:%p, queue size limit: %" PRId64 ", reason: %s" PRId64, pItem, queue,
           queue->itemLimit, tstrerror(code));
    (void)taosThreadMutexUnlock(&queue->mutex);
    return code;
  }

  if (queue->tail) {
    queue->tail->next = pNode;
    queue->tail = pNode;
  } else {
    queue->head = pNode;
    queue->tail = pNode;
  }
  queue->numOfItems++;
  queue->memOfItems += (pNode->size + pNode->dataSize);
  if (queue->qset) {
    (void)atomic_add_fetch_32(&queue->qset->numOfItems, 1);
  }

  uTrace("item:%p is put into queue:%p, items:%d mem:%" PRId64, pItem, queue, queue->numOfItems, queue->memOfItems);

  (void)taosThreadMutexUnlock(&queue->mutex);

  if (queue->qset) {
    (void)tsem_post(&queue->qset->sem);
  }
  return code;
}

int32_t taosReadQitem(STaosQueue *queue, void **ppItem) {
  STaosQnode *pNode = NULL;
  int32_t     code = 0;

  (void)taosThreadMutexLock(&queue->mutex);

  if (queue->head) {
    pNode = queue->head;
    *ppItem = pNode->item;
    queue->head = pNode->next;
    if (queue->head == NULL) {
      queue->tail = NULL;
    }
    queue->numOfItems--;
    queue->memOfItems -= (pNode->size + pNode->dataSize);
    if (queue->qset) {
      (void)atomic_sub_fetch_32(&queue->qset->numOfItems, 1);
    }
    code = 1;
    uTrace("item:%p is read out from queue:%p, items:%d mem:%" PRId64, *ppItem, queue, queue->numOfItems,
           queue->memOfItems);
  }

  (void)taosThreadMutexUnlock(&queue->mutex);

  return code;
}

int32_t taosAllocateQall(STaosQall **qall) {
  *qall = taosMemoryCalloc(1, sizeof(STaosQall));
  if (*qall == NULL) {
    return terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return 0;
}

void taosFreeQall(STaosQall *qall) { taosMemoryFree(qall); }

int32_t taosReadAllQitems(STaosQueue *queue, STaosQall *qall) {
  int32_t numOfItems = 0;
  bool    empty;

  (void)taosThreadMutexLock(&queue->mutex);

  empty = queue->head == NULL;
  if (!empty) {
    memset(qall, 0, sizeof(STaosQall));
    qall->current = queue->head;
    qall->start = queue->head;
    qall->numOfItems = queue->numOfItems;
    qall->memOfItems = queue->memOfItems;

    qall->unAccessedNumOfItems = queue->numOfItems;
    qall->unAccessMemOfItems = queue->memOfItems;

    numOfItems = qall->numOfItems;

    queue->head = NULL;
    queue->tail = NULL;
    queue->numOfItems = 0;
    queue->memOfItems = 0;
    uTrace("read %d items from queue:%p, items:%d mem:%" PRId64, numOfItems, queue, queue->numOfItems,
           queue->memOfItems);
    if (queue->qset) {
      (void)atomic_sub_fetch_32(&queue->qset->numOfItems, qall->numOfItems);
    }
  }

  (void)taosThreadMutexUnlock(&queue->mutex);

  // if source queue is empty, we set destination qall to empty too.
  if (empty) {
    qall->current = NULL;
    qall->start = NULL;
    qall->numOfItems = 0;
  }
  return numOfItems;
}

int32_t taosGetQitem(STaosQall *qall, void **ppItem) {
  STaosQnode *pNode;
  int32_t     num = 0;

  pNode = qall->current;
  if (pNode) qall->current = pNode->next;

  if (pNode) {
    *ppItem = pNode->item;
    num = 1;

    qall->unAccessedNumOfItems -= 1;
    qall->unAccessMemOfItems -= pNode->dataSize;

    uTrace("item:%p is fetched", *ppItem);
  } else {
    *ppItem = NULL;
  }

  return num;
}

int32_t taosOpenQset(STaosQset **qset) {
  *qset = taosMemoryCalloc(sizeof(STaosQset), 1);
  if (*qset == NULL) {
    return terrno = TSDB_CODE_OUT_OF_MEMORY;
  }

  (void)taosThreadMutexInit(&(*qset)->mutex, NULL);
  (void)tsem_init(&(*qset)->sem, 0, 0);

  uDebug("qset:%p is opened", qset);
  return 0;
}

void taosCloseQset(STaosQset *qset) {
  if (qset == NULL) return;

  // remove all the queues from qset
  (void)taosThreadMutexLock(&qset->mutex);
  while (qset->head) {
    STaosQueue *queue = qset->head;
    qset->head = qset->head->next;

    queue->qset = NULL;
    queue->next = NULL;
  }
  (void)taosThreadMutexUnlock(&qset->mutex);

  (void)taosThreadMutexDestroy(&qset->mutex);
  (void)tsem_destroy(&qset->sem);
  taosMemoryFree(qset);
  uDebug("qset:%p is closed", qset);
}

// tsem_post 'qset->sem', so that reader threads waiting for it
// resumes execution and return, should only be used to signal the
// thread to exit.
void taosQsetThreadResume(STaosQset *qset) {
  uDebug("qset:%p, it will exit", qset);
  (void)tsem_post(&qset->sem);
}

int32_t taosAddIntoQset(STaosQset *qset, STaosQueue *queue, void *ahandle) {
  if (queue->qset) return -1;

  (void)taosThreadMutexLock(&qset->mutex);

  queue->next = qset->head;
  queue->ahandle = ahandle;
  qset->head = queue;
  qset->numOfQueues++;

  (void)taosThreadMutexLock(&queue->mutex);
  (void)atomic_add_fetch_32(&qset->numOfItems, queue->numOfItems);
  queue->qset = qset;
  (void)taosThreadMutexUnlock(&queue->mutex);

  (void)taosThreadMutexUnlock(&qset->mutex);

  uTrace("queue:%p is added into qset:%p", queue, qset);
  return 0;
}

void taosRemoveFromQset(STaosQset *qset, STaosQueue *queue) {
  STaosQueue *tqueue = NULL;

  (void)taosThreadMutexLock(&qset->mutex);

  if (qset->head) {
    if (qset->head == queue) {
      qset->head = qset->head->next;
      tqueue = queue;
    } else {
      STaosQueue *prev = qset->head;
      tqueue = qset->head->next;
      while (tqueue) {
        if (tqueue == queue) {
          prev->next = tqueue->next;
          break;
        } else {
          prev = tqueue;
          tqueue = tqueue->next;
        }
      }
    }

    if (tqueue) {
      if (qset->current == queue) qset->current = tqueue->next;
      qset->numOfQueues--;

      (void)taosThreadMutexLock(&queue->mutex);
      (void)atomic_sub_fetch_32(&qset->numOfItems, queue->numOfItems);
      queue->qset = NULL;
      queue->next = NULL;
      (void)taosThreadMutexUnlock(&queue->mutex);
    }
  }

  (void)taosThreadMutexUnlock(&qset->mutex);

  uDebug("queue:%p is removed from qset:%p", queue, qset);
}

int32_t taosReadQitemFromQset(STaosQset *qset, void **ppItem, SQueueInfo *qinfo) {
  STaosQnode *pNode = NULL;
  int32_t     code = 0;

  (void)tsem_wait(&qset->sem);

  (void)taosThreadMutexLock(&qset->mutex);

  for (int32_t i = 0; i < qset->numOfQueues; ++i) {
    if (qset->current == NULL) qset->current = qset->head;
    STaosQueue *queue = qset->current;
    if (queue) qset->current = queue->next;
    if (queue == NULL) break;
    if (queue->head == NULL) continue;

    (void)taosThreadMutexLock(&queue->mutex);

    if (queue->head) {
      pNode = queue->head;
      *ppItem = pNode->item;
      qinfo->ahandle = queue->ahandle;
      qinfo->fp = queue->itemFp;
      qinfo->queue = queue;
      qinfo->timestamp = pNode->timestamp;

      queue->head = pNode->next;
      if (queue->head == NULL) queue->tail = NULL;
      // queue->numOfItems--;
      queue->memOfItems -= (pNode->size + pNode->dataSize);
      (void)atomic_sub_fetch_32(&qset->numOfItems, 1);
      code = 1;
      uTrace("item:%p is read out from queue:%p, items:%d mem:%" PRId64, *ppItem, queue, queue->numOfItems - 1,
             queue->memOfItems);
    }

    (void)taosThreadMutexUnlock(&queue->mutex);
    if (pNode) break;
  }

  (void)taosThreadMutexUnlock(&qset->mutex);

  return code;
}

int32_t taosReadAllQitemsFromQset(STaosQset *qset, STaosQall *qall, SQueueInfo *qinfo) {
  STaosQueue *queue;
  int32_t     code = 0;

  (void)tsem_wait(&qset->sem);
  (void)taosThreadMutexLock(&qset->mutex);

  for (int32_t i = 0; i < qset->numOfQueues; ++i) {
    if (qset->current == NULL) qset->current = qset->head;
    queue = qset->current;
    if (queue) qset->current = queue->next;
    if (queue == NULL) break;
    if (queue->head == NULL) continue;

    (void)taosThreadMutexLock(&queue->mutex);

    if (queue->head) {
      qall->current = queue->head;
      qall->start = queue->head;
      qall->numOfItems = queue->numOfItems;
      qall->memOfItems = queue->memOfItems;
      qall->unAccessedNumOfItems = queue->numOfItems;
      qall->unAccessMemOfItems = queue->memOfItems;

      code = qall->numOfItems;
      qinfo->ahandle = queue->ahandle;
      qinfo->fp = queue->itemsFp;
      qinfo->queue = queue;
      qinfo->timestamp = queue->head->timestamp;

      queue->head = NULL;
      queue->tail = NULL;
      // queue->numOfItems = 0;
      queue->memOfItems = 0;
      uTrace("read %d items from queue:%p, items:0 mem:%" PRId64, code, queue, queue->memOfItems);

      (void)atomic_sub_fetch_32(&qset->numOfItems, qall->numOfItems);
      for (int32_t j = 1; j < qall->numOfItems; ++j) {
        (void)tsem_wait(&qset->sem);
      }
    }

    (void)taosThreadMutexUnlock(&queue->mutex);

    if (code != 0) break;
  }

  (void)taosThreadMutexUnlock(&qset->mutex);
  return code;
}

int32_t taosQallItemSize(STaosQall *qall) { return qall->numOfItems; }
int64_t taosQallMemSize(STaosQall *qall) { return qall->memOfItems; }

int64_t taosQallUnAccessedItemSize(STaosQall *qall) { return qall->unAccessedNumOfItems; }
int64_t taosQallUnAccessedMemSize(STaosQall *qall) { return qall->unAccessMemOfItems; }

void    taosResetQitems(STaosQall *qall) { qall->current = qall->start; }
int32_t taosGetQueueNumber(STaosQset *qset) { return qset->numOfQueues; }

void taosQueueSetThreadId(STaosQueue *pQueue, int64_t threadId) { pQueue->threadId = threadId; }

int64_t taosQueueGetThreadId(STaosQueue *pQueue) { return pQueue->threadId; }

#if 0

void taosResetQsetThread(STaosQset *qset, void *pItem) {
  if (pItem == NULL) return;
  STaosQnode *pNode = (STaosQnode *)((char *)pItem - sizeof(STaosQnode));

  (void)taosThreadMutexLock(&qset->mutex);
  for (int32_t i = 0; i < pNode->queue->numOfItems; ++i) {
    tsem_post(&qset->sem);
  }
  (void)taosThreadMutexUnlock(&qset->mutex);
}

#endif
