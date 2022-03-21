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

typedef struct STaosQnode STaosQnode;

typedef struct STaosQnode {
  STaosQnode *next;
  STaosQueue *queue;
  char        item[];
} STaosQnode;

typedef struct STaosQueue {
  int32_t         itemSize;
  int32_t         numOfItems;
  int32_t         threadId;
  STaosQnode     *head;
  STaosQnode     *tail;
  STaosQueue     *next;     // for queue set
  STaosQset      *qset;     // for queue set
  void           *ahandle;  // for queue set
  FItem           itemFp;
  FItems          itemsFp;
  TdThreadMutex mutex;
} STaosQueue;

typedef struct STaosQset {
  STaosQueue     *head;
  STaosQueue     *current;
  TdThreadMutex mutex;
  int32_t         numOfQueues;
  int32_t         numOfItems;
  tsem_t          sem;
} STaosQset;

typedef struct STaosQall {
  STaosQnode *current;
  STaosQnode *start;
  int32_t     itemSize;
  int32_t     numOfItems;
} STaosQall;

STaosQueue *taosOpenQueue() {
  STaosQueue *queue = calloc(1, sizeof(STaosQueue));
  if (queue == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (taosThreadMutexInit(&queue->mutex, NULL) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  queue->threadId = -1;
  uDebug("queue:%p is opened", queue);
  return queue;
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

  taosThreadMutexLock(&queue->mutex);
  STaosQnode *pNode = queue->head;
  queue->head = NULL;
  qset = queue->qset;
  taosThreadMutexUnlock(&queue->mutex);

  if (queue->qset) {
    taosRemoveFromQset(qset, queue);
  }

  while (pNode) {
    pTemp = pNode;
    pNode = pNode->next;
    free(pTemp);
  }

  taosThreadMutexDestroy(&queue->mutex);
  free(queue);

  uDebug("queue:%p is closed", queue);
}

bool taosQueueEmpty(STaosQueue *queue) {
  if (queue == NULL) return true;

  bool empty = false;
  taosThreadMutexLock(&queue->mutex);
  if (queue->head == NULL && queue->tail == NULL) {
    empty = true;
  }
  taosThreadMutexUnlock(&queue->mutex);

  return empty;
}

int32_t taosQueueSize(STaosQueue *queue) {
  taosThreadMutexLock(&queue->mutex);
  int32_t numOfItems = queue->numOfItems;
  taosThreadMutexUnlock(&queue->mutex);
  return numOfItems;
}

void *taosAllocateQitem(int32_t size) {
  STaosQnode *pNode = calloc(1, sizeof(STaosQnode) + size);

  if (pNode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  uTrace("item:%p, node:%p is allocated", pNode->item, pNode);
  return (void *)pNode->item;
}

void taosFreeQitem(void *pItem) {
  if (pItem == NULL) return;

  char *temp = pItem;
  temp -= sizeof(STaosQnode);
  uTrace("item:%p, node:%p is freed", pItem, temp);
  free(temp);
}

int32_t taosWriteQitem(STaosQueue *queue, void *pItem) {
  STaosQnode *pNode = (STaosQnode *)(((char *)pItem) - sizeof(STaosQnode));
  pNode->next = NULL;

  taosThreadMutexLock(&queue->mutex);

  if (queue->tail) {
    queue->tail->next = pNode;
    queue->tail = pNode;
  } else {
    queue->head = pNode;
    queue->tail = pNode;
  }

  queue->numOfItems++;
  if (queue->qset) atomic_add_fetch_32(&queue->qset->numOfItems, 1);
  uTrace("item:%p is put into queue:%p, items:%d", pItem, queue, queue->numOfItems);

  taosThreadMutexUnlock(&queue->mutex);

  if (queue->qset) tsem_post(&queue->qset->sem);

  return 0;
}

int32_t taosReadQitem(STaosQueue *queue, void **ppItem) {
  STaosQnode *pNode = NULL;
  int32_t     code = 0;

  taosThreadMutexLock(&queue->mutex);

  if (queue->head) {
    pNode = queue->head;
    *ppItem = pNode->item;
    queue->head = pNode->next;
    if (queue->head == NULL) queue->tail = NULL;
    queue->numOfItems--;
    if (queue->qset) atomic_sub_fetch_32(&queue->qset->numOfItems, 1);
    code = 1;
    uTrace("item:%p is read out from queue:%p, items:%d", *ppItem, queue, queue->numOfItems);
  }

  taosThreadMutexUnlock(&queue->mutex);

  return code;
}

STaosQall *taosAllocateQall() { return calloc(1, sizeof(STaosQall)); }

void taosFreeQall(STaosQall *qall) { free(qall); }

int32_t taosReadAllQitems(STaosQueue *queue, STaosQall *qall) {
  int32_t code = 0;
  bool    empty;

  taosThreadMutexLock(&queue->mutex);

  empty = queue->head == NULL;
  if (!empty) {
    memset(qall, 0, sizeof(STaosQall));
    qall->current = queue->head;
    qall->start = queue->head;
    qall->numOfItems = queue->numOfItems;
    qall->itemSize = queue->itemSize;
    code = qall->numOfItems;

    queue->head = NULL;
    queue->tail = NULL;
    queue->numOfItems = 0;
    if (queue->qset) atomic_sub_fetch_32(&queue->qset->numOfItems, qall->numOfItems);
  }

  taosThreadMutexUnlock(&queue->mutex);

  // if source queue is empty, we set destination qall to empty too.
  if (empty) {
    qall->current = NULL;
    qall->start = NULL;
    qall->numOfItems = 0;
  }
  return code;
}

int32_t taosGetQitem(STaosQall *qall, void **ppItem) {
  STaosQnode *pNode;
  int32_t     num = 0;

  pNode = qall->current;
  if (pNode) qall->current = pNode->next;

  if (pNode) {
    *ppItem = pNode->item;
    num = 1;
    uTrace("item:%p is fetched", *ppItem);
  }

  return num;
}

void taosResetQitems(STaosQall *qall) { qall->current = qall->start; }

STaosQset *taosOpenQset() {
  STaosQset *qset = calloc(sizeof(STaosQset), 1);
  if (qset == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  taosThreadMutexInit(&qset->mutex, NULL);
  tsem_init(&qset->sem, 0, 0);

  uDebug("qset:%p is opened", qset);
  return qset;
}

void taosCloseQset(STaosQset *qset) {
  if (qset == NULL) return;

  // remove all the queues from qset
  taosThreadMutexLock(&qset->mutex);
  while (qset->head) {
    STaosQueue *queue = qset->head;
    qset->head = qset->head->next;

    queue->qset = NULL;
    queue->next = NULL;
  }
  taosThreadMutexUnlock(&qset->mutex);

  taosThreadMutexDestroy(&qset->mutex);
  tsem_destroy(&qset->sem);
  free(qset);
  uDebug("qset:%p is closed", qset);
}

// tsem_post 'qset->sem', so that reader threads waiting for it
// resumes execution and return, should only be used to signal the
// thread to exit.
void taosQsetThreadResume(STaosQset *qset) {
  uDebug("qset:%p, it will exit", qset);
  tsem_post(&qset->sem);
}

int32_t taosAddIntoQset(STaosQset *qset, STaosQueue *queue, void *ahandle) {
  if (queue->qset) return -1;

  taosThreadMutexLock(&qset->mutex);

  queue->next = qset->head;
  queue->ahandle = ahandle;
  qset->head = queue;
  qset->numOfQueues++;

  taosThreadMutexLock(&queue->mutex);
  atomic_add_fetch_32(&qset->numOfItems, queue->numOfItems);
  queue->qset = qset;
  taosThreadMutexUnlock(&queue->mutex);

  taosThreadMutexUnlock(&qset->mutex);

  uTrace("queue:%p is added into qset:%p", queue, qset);
  return 0;
}

void taosRemoveFromQset(STaosQset *qset, STaosQueue *queue) {
  STaosQueue *tqueue = NULL;

  taosThreadMutexLock(&qset->mutex);

  if (qset->head) {
    if (qset->head == queue) {
      qset->head = qset->head->next;
      tqueue = queue;
    } else {
      STaosQueue *prev = qset->head;
      tqueue = qset->head->next;
      while (tqueue) {
        assert(tqueue->qset);
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

      taosThreadMutexLock(&queue->mutex);
      atomic_sub_fetch_32(&qset->numOfItems, queue->numOfItems);
      queue->qset = NULL;
      queue->next = NULL;
      taosThreadMutexUnlock(&queue->mutex);
    }
  }

  taosThreadMutexUnlock(&qset->mutex);

  uDebug("queue:%p is removed from qset:%p", queue, qset);
}

int32_t taosGetQueueNumber(STaosQset *qset) { return qset->numOfQueues; }

int32_t taosReadQitemFromQset(STaosQset *qset, void **ppItem, void **ahandle, FItem *itemFp) {
  STaosQnode *pNode = NULL;
  int32_t     code = 0;

  tsem_wait(&qset->sem);

  taosThreadMutexLock(&qset->mutex);

  for (int32_t i = 0; i < qset->numOfQueues; ++i) {
    if (qset->current == NULL) qset->current = qset->head;
    STaosQueue *queue = qset->current;
    if (queue) qset->current = queue->next;
    if (queue == NULL) break;
    if (queue->head == NULL) continue;

    taosThreadMutexLock(&queue->mutex);

    if (queue->head) {
      pNode = queue->head;
      *ppItem = pNode->item;
      if (ahandle) *ahandle = queue->ahandle;
      if (itemFp) *itemFp = queue->itemFp;

      queue->head = pNode->next;
      if (queue->head == NULL) queue->tail = NULL;
      queue->numOfItems--;
      atomic_sub_fetch_32(&qset->numOfItems, 1);
      code = 1;
      uTrace("item:%p is read out from queue:%p, items:%d", *ppItem, queue, queue->numOfItems);
    }

    taosThreadMutexUnlock(&queue->mutex);
    if (pNode) break;
  }

  taosThreadMutexUnlock(&qset->mutex);

  return code;
}

int32_t taosReadAllQitemsFromQset(STaosQset *qset, STaosQall *qall, void **ahandle, FItems *itemsFp) {
  STaosQueue *queue;
  int32_t     code = 0;

  tsem_wait(&qset->sem);
  taosThreadMutexLock(&qset->mutex);

  for (int32_t i = 0; i < qset->numOfQueues; ++i) {
    if (qset->current == NULL) qset->current = qset->head;
    queue = qset->current;
    if (queue) qset->current = queue->next;
    if (queue == NULL) break;
    if (queue->head == NULL) continue;

    taosThreadMutexLock(&queue->mutex);

    if (queue->head) {
      qall->current = queue->head;
      qall->start = queue->head;
      qall->numOfItems = queue->numOfItems;
      qall->itemSize = queue->itemSize;
      code = qall->numOfItems;
      if (ahandle) *ahandle = queue->ahandle;
      if (itemsFp) *itemsFp = queue->itemsFp;

      queue->head = NULL;
      queue->tail = NULL;
      queue->numOfItems = 0;
      atomic_sub_fetch_32(&qset->numOfItems, qall->numOfItems);
      for (int32_t j = 1; j < qall->numOfItems; ++j) {
        tsem_wait(&qset->sem);
      }
    }

    taosThreadMutexUnlock(&queue->mutex);

    if (code != 0) break;
  }

  taosThreadMutexUnlock(&qset->mutex);
  return code;
}

int32_t taosReadQitemFromQsetByThread(STaosQset *qset, void **ppItem, void **ahandle, FItem *itemFp, int32_t threadId) {
  STaosQnode *pNode = NULL;
  int32_t     code = -1;

  tsem_wait(&qset->sem);

  taosThreadMutexLock(&qset->mutex);

  for (int32_t i = 0; i < qset->numOfQueues; ++i) {
    if (qset->current == NULL) qset->current = qset->head;
    STaosQueue *queue = qset->current;
    if (queue) qset->current = queue->next;
    if (queue == NULL) break;
    if (queue->head == NULL) continue;
    if (queue->threadId != -1 && queue->threadId != threadId) {
      code = 0;
      continue;
    }

    taosThreadMutexLock(&queue->mutex);

    if (queue->head) {
      pNode = queue->head;
      pNode->queue = queue;
      queue->threadId = threadId;
      *ppItem = pNode->item;

      if (ahandle) *ahandle = queue->ahandle;
      if (itemFp) *itemFp = queue->itemFp;

      queue->head = pNode->next;
      if (queue->head == NULL) queue->tail = NULL;
      queue->numOfItems--;
      atomic_sub_fetch_32(&qset->numOfItems, 1);
      code = 1;
      uTrace("item:%p is read out from queue:%p, items:%d", *ppItem, queue, queue->numOfItems);
    }

    taosThreadMutexUnlock(&queue->mutex);
    if (pNode) break;
  }

  taosThreadMutexUnlock(&qset->mutex);

  return code;
}

void taosResetQsetThread(STaosQset *qset, void *pItem) {
  if (pItem == NULL) return;
  STaosQnode *pNode = (STaosQnode *)((char *)pItem - sizeof(STaosQnode));

  taosThreadMutexLock(&qset->mutex);
  pNode->queue->threadId = -1;
  for (int32_t i = 0; i < pNode->queue->numOfItems; ++i) {
    tsem_post(&qset->sem);
  }
  taosThreadMutexUnlock(&qset->mutex);
}

int32_t taosGetQueueItemsNumber(STaosQueue *queue) {
  if (!queue) return 0;

  int32_t num;
  taosThreadMutexLock(&queue->mutex);
  num = queue->numOfItems;
  taosThreadMutexUnlock(&queue->mutex);
  return num;
}

int32_t taosGetQsetItemsNumber(STaosQset *qset) {
  if (!qset) return 0;

  int32_t num = 0;
  taosThreadMutexLock(&qset->mutex);
  num = qset->numOfItems;
  taosThreadMutexUnlock(&qset->mutex);
  return num;
}
