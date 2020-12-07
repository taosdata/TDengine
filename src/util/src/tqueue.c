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

#include "os.h"
#include "tulog.h"
#include "taoserror.h"
#include "tqueue.h"

typedef struct STaosQnode {
  int                 type;
  struct STaosQnode  *next;
  char                item[];
} STaosQnode;

typedef struct STaosQueue {
  int32_t             itemSize;
  int32_t             numOfItems;
  struct STaosQnode  *head;
  struct STaosQnode  *tail;
  struct STaosQueue  *next;    // for queue set
  struct STaosQset   *qset;    // for queue set
  void               *ahandle; // for queue set
  pthread_mutex_t     mutex;  
} STaosQueue;

typedef struct STaosQset {
  STaosQueue        *head;
  STaosQueue        *current;
  pthread_mutex_t    mutex;
  int32_t            numOfQueues;
  int32_t            numOfItems;
  tsem_t             sem;
} STaosQset;

typedef struct STaosQall {
  STaosQnode   *current;
  STaosQnode   *start;
  int32_t       itemSize;
  int32_t       numOfItems;
} STaosQall; 
  
taos_queue taosOpenQueue() {
  
  STaosQueue *queue = (STaosQueue *) calloc(sizeof(STaosQueue), 1);
  if (queue == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    return NULL;
  }

  pthread_mutex_init(&queue->mutex, NULL);

  uTrace("queue:%p is openned", queue);
  return queue;
}

void taosCloseQueue(taos_queue param) {
  if (param == NULL) return;
  STaosQueue *queue = (STaosQueue *)param;
  STaosQnode *pTemp;
  STaosQset  *qset;

  pthread_mutex_lock(&queue->mutex);
  STaosQnode *pNode = queue->head;  
  queue->head = NULL;
  qset = queue->qset;
  pthread_mutex_unlock(&queue->mutex);

  if (queue->qset) taosRemoveFromQset(qset, queue); 

  while (pNode) {
    pTemp = pNode;
    pNode = pNode->next;
    free (pTemp);
  }

  pthread_mutex_destroy(&queue->mutex);
  free(queue);

  uTrace("queue:%p is closed", queue);
}

void *taosAllocateQitem(int size) {
  STaosQnode *pNode = (STaosQnode *)calloc(sizeof(STaosQnode) + size, 1);
  
  if (pNode == NULL) return NULL;
  uTrace("item:%p, node:%p is allocated", pNode->item, pNode);
  return (void *)pNode->item;
}

void taosFreeQitem(void *param) {
  if (param == NULL) return;

  char *temp = (char *)param;
  temp -= sizeof(STaosQnode);
  uTrace("item:%p, node:%p is freed", param, temp);
  free(temp);
}

int taosWriteQitem(taos_queue param, int type, void *item) {
  STaosQueue *queue = (STaosQueue *)param;
  STaosQnode *pNode = (STaosQnode *)(((char *)item) - sizeof(STaosQnode));
  pNode->type = type;
  pNode->next = NULL;

  pthread_mutex_lock(&queue->mutex);

  if (queue->tail) {
    queue->tail->next = pNode;
    queue->tail = pNode;
  } else {
    queue->head = pNode;
    queue->tail = pNode; 
  }

  queue->numOfItems++;
  if (queue->qset) atomic_add_fetch_32(&queue->qset->numOfItems, 1);
  uTrace("item:%p is put into queue:%p, type:%d items:%d", item, queue, type, queue->numOfItems);

  pthread_mutex_unlock(&queue->mutex);

  if (queue->qset) tsem_post(&queue->qset->sem);

  return 0;
}

int taosReadQitem(taos_queue param, int *type, void **pitem) {
  STaosQueue *queue = (STaosQueue *)param;
  STaosQnode *pNode = NULL;
  int         code = 0;

  pthread_mutex_lock(&queue->mutex);

  if (queue->head) {
      pNode = queue->head;
      *pitem = pNode->item;
      *type = pNode->type;
      queue->head = pNode->next;
      if (queue->head == NULL) 
        queue->tail = NULL;
      queue->numOfItems--;
      if (queue->qset) atomic_sub_fetch_32(&queue->qset->numOfItems, 1);
      code = 1;
      uDebug("item:%p is read out from queue:%p, type:%d items:%d", *pitem, queue, *type, queue->numOfItems);
  } 

  pthread_mutex_unlock(&queue->mutex);

  return code;
}

void *taosAllocateQall() {
  void *p = calloc(sizeof(STaosQall), 1);
  return p;
}

void taosFreeQall(void *param) {
  free(param);
}

int taosReadAllQitems(taos_queue param, taos_qall p2) {
  STaosQueue *queue = (STaosQueue *)param;
  STaosQall  *qall = (STaosQall *)p2;
  int         code = 0;

  pthread_mutex_lock(&queue->mutex);

  if (queue->head) {
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

  pthread_mutex_unlock(&queue->mutex);
  
  return code; 
}

int taosGetQitem(taos_qall param, int *type, void **pitem) {
  STaosQall  *qall = (STaosQall *)param;
  STaosQnode *pNode;
  int         num = 0;

  pNode = qall->current;
  if (pNode)
    qall->current = pNode->next;
 
  if (pNode) {
    *pitem = pNode->item;
    *type = pNode->type;
    num = 1;
    uTrace("item:%p is fetched, type:%d", *pitem, *type);
  }

  return num;
}

void taosResetQitems(taos_qall param) {
  STaosQall  *qall = (STaosQall *)param;
  qall->current = qall->start;
}

taos_qset taosOpenQset() {

  STaosQset *qset = (STaosQset *) calloc(sizeof(STaosQset), 1);
  if (qset == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    return NULL;
  }

  pthread_mutex_init(&qset->mutex, NULL);
  tsem_init(&qset->sem, 0, 0);

  uTrace("qset:%p is openned", qset);
  return qset;
}

void taosCloseQset(taos_qset param) {
  if (param == NULL) return;
  STaosQset *qset = (STaosQset *)param;

  // remove all the queues from qset
  pthread_mutex_lock(&qset->mutex);
  while (qset->head) {
    STaosQueue *queue = qset->head;
    qset->head = qset->head->next;

    queue->qset = NULL;
    queue->next = NULL;
  }
  pthread_mutex_unlock(&qset->mutex);

  pthread_mutex_destroy(&qset->mutex);
  tsem_destroy(&qset->sem);
  free(qset);
  uTrace("qset:%p is closed", qset);
}

// tsem_post 'qset->sem', so that reader threads waiting for it
// resumes execution and return, should only be used to signal the
// thread to exit.
void taosQsetThreadResume(taos_qset param) {
  STaosQset *qset = (STaosQset *)param;
  uDebug("qset:%p, it will exit", qset);
  tsem_post(&qset->sem);
}

int taosAddIntoQset(taos_qset p1, taos_queue p2, void *ahandle) {
  STaosQueue *queue = (STaosQueue *)p2;
  STaosQset  *qset = (STaosQset *)p1;

  if (queue->qset) return -1; 

  pthread_mutex_lock(&qset->mutex);

  queue->next = qset->head;
  queue->ahandle = ahandle;
  qset->head = queue;
  qset->numOfQueues++;

  pthread_mutex_lock(&queue->mutex);
  atomic_add_fetch_32(&qset->numOfItems, queue->numOfItems);
  queue->qset = qset;
  pthread_mutex_unlock(&queue->mutex);

  pthread_mutex_unlock(&qset->mutex);

  uTrace("queue:%p is added into qset:%p", queue, qset);
  return 0;
}

void taosRemoveFromQset(taos_qset p1, taos_queue p2) {
  STaosQueue *queue = (STaosQueue *)p2;
  STaosQset  *qset = (STaosQset *)p1;
 
  STaosQueue *tqueue = NULL;

  pthread_mutex_lock(&qset->mutex);

  if (qset->head) {
    if (qset->head == queue) {
      qset->head = qset->head->next;
      tqueue = queue;
    } else {
      STaosQueue *prev = qset->head;
      tqueue = qset->head->next;
      while (tqueue) {
        assert(tqueue->qset);
        if (tqueue== queue) {
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

      pthread_mutex_lock(&queue->mutex);
      atomic_sub_fetch_32(&qset->numOfItems, queue->numOfItems);
      queue->qset = NULL;
      queue->next = NULL;
      pthread_mutex_unlock(&queue->mutex);
    }
  } 
  
  pthread_mutex_unlock(&qset->mutex);

  uTrace("queue:%p is removed from qset:%p", queue, qset);
}

int taosGetQueueNumber(taos_qset param) {
  return ((STaosQset *)param)->numOfQueues;
}

int taosReadQitemFromQset(taos_qset param, int *type, void **pitem, void **phandle) {
  STaosQset  *qset = (STaosQset *)param;
  STaosQnode *pNode = NULL;
  int         code = 0;
   
  tsem_wait(&qset->sem);

  pthread_mutex_lock(&qset->mutex);

  for(int i=0; i<qset->numOfQueues; ++i) {
    if (qset->current == NULL) 
      qset->current = qset->head;   
    STaosQueue *queue = qset->current;
    if (queue) qset->current = queue->next;
    if (queue == NULL) break;
    if (queue->head == NULL) continue;

    pthread_mutex_lock(&queue->mutex);

    if (queue->head) {
        pNode = queue->head;
        *pitem = pNode->item;
        if (type) *type = pNode->type;
        if (phandle) *phandle = queue->ahandle;
        queue->head = pNode->next;
        if (queue->head == NULL) 
          queue->tail = NULL;
        queue->numOfItems--;
        atomic_sub_fetch_32(&qset->numOfItems, 1);
        code = 1;
        uTrace("item:%p is read out from queue:%p, type:%d items:%d", *pitem, queue, *type, queue->numOfItems);
    } 

    pthread_mutex_unlock(&queue->mutex);
    if (pNode) break;
  }

  pthread_mutex_unlock(&qset->mutex);

  return code; 
}

int taosReadAllQitemsFromQset(taos_qset param, taos_qall p2, void **phandle) {
  STaosQset  *qset = (STaosQset *)param;
  STaosQueue *queue;
  STaosQall  *qall = (STaosQall *)p2;
  int         code = 0;

  tsem_wait(&qset->sem);
  pthread_mutex_lock(&qset->mutex);

  for(int i=0; i<qset->numOfQueues; ++i) {
    if (qset->current == NULL) 
      qset->current = qset->head;   
    queue = qset->current;
    if (queue) qset->current = queue->next;
    if (queue == NULL) break;
    if (queue->head == NULL) continue;

    pthread_mutex_lock(&queue->mutex);

    if (queue->head) {
      qall->current = queue->head;
      qall->start = queue->head;
      qall->numOfItems = queue->numOfItems;
      qall->itemSize = queue->itemSize;
      code = qall->numOfItems;
      *phandle = queue->ahandle;
          
      queue->head = NULL;
      queue->tail = NULL;
      queue->numOfItems = 0;
      atomic_sub_fetch_32(&qset->numOfItems, qall->numOfItems);
      for (int j=1; j<qall->numOfItems; ++j) tsem_wait(&qset->sem);
    } 

    pthread_mutex_unlock(&queue->mutex);

    if (code != 0) break;  
  }

  pthread_mutex_unlock(&qset->mutex);
  return code;
}

int taosGetQueueItemsNumber(taos_queue param) {
  STaosQueue *queue = (STaosQueue *)param;
  return queue->numOfItems;
}

int taosGetQsetItemsNumber(taos_qset param) {
  STaosQset *qset = (STaosQset *)param;
  return qset->numOfItems;
}
