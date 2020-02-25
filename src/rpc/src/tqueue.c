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
#include "tlog.h"
#include "tqueue.h"

#define DUMP_SCHEDULER_TIME_WINDOW 30000 //every 30sec, take a snap shot of task queue.

typedef struct {
  char            label[16];
  int             num;
  tsem_t          emptySem;
  tsem_t          fullSem;
  pthread_mutex_t queueMutex;
  int             fullSlot;
  int             emptySlot;
  int             queueSize;
  SRpcMsg        *queue;
  SRpcMsg        *oqueue;
  pthread_t       qthread;
  void          (*fp)(int num, SRpcMsg *);
} SRpcQueue;

static void *taosProcessMsgQueue(void *param);

void *taosInitMsgQueue(int queueSize, void (*fp)(int num, SRpcMsg *), const char *label) {
  pthread_attr_t attr;
  SRpcQueue *  pQueue = (SRpcQueue *)malloc(sizeof(SRpcQueue));
  if (pQueue == NULL) {
    pError("%s: no enough memory for pQueue, reason: %s", label, strerror(errno));
    goto _error;
  }

  memset(pQueue, 0, sizeof(SRpcQueue));
  pQueue->queueSize = queueSize;
  strncpy(pQueue->label, label, sizeof(pQueue->label)); // fix buffer overflow
  pQueue->label[sizeof(pQueue->label)-1] = '\0';
  pQueue->fp = fp;

  if (pthread_mutex_init(&pQueue->queueMutex, NULL) < 0) {
    pError("init %s:queueMutex failed, reason:%s", pQueue->label, strerror(errno));
    goto _error;
  }

  if (tsem_init(&pQueue->emptySem, 0, (unsigned int)pQueue->queueSize) != 0) {
    pError("init %s:empty semaphore failed, reason:%s", pQueue->label, strerror(errno));
    goto _error;
  }

  if (tsem_init(&pQueue->fullSem, 0, 0) != 0) {
    pError("init %s:full semaphore failed, reason:%s", pQueue->label, strerror(errno));
    goto _error;
  }

  if ((pQueue->queue = (SRpcMsg *)malloc((size_t)pQueue->queueSize * sizeof(SRpcMsg))) == NULL) {
    pError("%s: no enough memory for queue, reason:%s", pQueue->label, strerror(errno));
    goto _error;
  }

  memset(pQueue->queue, 0, (size_t)pQueue->queueSize * sizeof(SRpcMsg));

  if ((pQueue->oqueue = (SRpcMsg *)malloc((size_t)pQueue->queueSize * sizeof(SRpcMsg))) == NULL) {
    pError("%s: no enough memory for queue, reason:%s", pQueue->label, strerror(errno));
    goto _error;
  }

  memset(pQueue->oqueue, 0, (size_t)pQueue->queueSize * sizeof(SRpcMsg));

  pQueue->fullSlot = 0;
  pQueue->fullSlot = 0;
  pQueue->emptySlot = 0;

  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  if (pthread_create(&pQueue->qthread, &attr, taosProcessMsgQueue, (void *)pQueue) != 0) {
    pError("%s: failed to create taos thread, reason:%s", pQueue->label, strerror(errno));
    goto _error;
  }

  pTrace("%s RPC msg queue is initialized", pQueue->label);

  return (void *)pQueue;

_error:
  taosCleanUpMsgQueue(pQueue);
  return NULL;
}

void *taosProcessMsgQueue(void *param) {
  SRpcQueue *pQueue = (SRpcQueue *)param;
  int        num = 0;

  while (1) {
    if (tsem_wait(&pQueue->fullSem) != 0) {
      if (errno == EINTR) {
        /* sem_wait is interrupted by interrupt, ignore and continue */
        pTrace("wait %s fullSem was interrupted", pQueue->label);
        continue;
      }
      pError("wait %s fullSem failed, errno:%d, reason:%s", pQueue->label, errno, strerror(errno));
    }

    if (pthread_mutex_lock(&pQueue->queueMutex) != 0)
      pError("lock %s queueMutex failed, reason:%s", pQueue->label, strerror(errno));

    num = 0;
    do {
      pQueue->oqueue[num] = pQueue->queue[pQueue->fullSlot];
      pQueue->fullSlot = (pQueue->fullSlot + 1) % pQueue->queueSize;
      ++num;
      pQueue->num--;
    } while (pQueue->fullSlot != pQueue->emptySlot); 

    if (pthread_mutex_unlock(&pQueue->queueMutex) != 0)
      pError("unlock %s queueMutex failed, reason:%s\n", pQueue->label, strerror(errno));

    for (int i= 0; i<num; ++i) {
      if (tsem_post(&pQueue->emptySem) != 0)
        pError("post %s emptySem failed, reason:%s\n", pQueue->label, strerror(errno));
    }

    for (int i=0; i<num-1; ++i) {
      if (tsem_wait(&pQueue->fullSem) != 0)
        pError("wait %s fullSem failed, reason:%s\n", pQueue->label, strerror(errno));
    }

    (*pQueue->fp)(num, pQueue->oqueue);

  }

  return NULL;
}

int taosPutIntoMsgQueue(void *qhandle, SRpcMsg *pMsg) {
  SRpcQueue *pQueue = (SRpcQueue *)qhandle;
  if (pQueue == NULL) {
    pError("sched is not ready, msg:%p is dropped", pMsg);
    return 0;
  }

  while (tsem_wait(&pQueue->emptySem) != 0) {
    if (errno != EINTR) {
      pError("wait %s emptySem failed, reason:%s", pQueue->label, strerror(errno));
      break;
    }
  }

  if (pthread_mutex_lock(&pQueue->queueMutex) != 0)
    pError("lock %s queueMutex failed, reason:%s", pQueue->label, strerror(errno));

  pQueue->queue[pQueue->emptySlot] = *pMsg;
  pQueue->emptySlot = (pQueue->emptySlot + 1) % pQueue->queueSize;
  pQueue->num++;
 
  if (pthread_mutex_unlock(&pQueue->queueMutex) != 0)
    pError("unlock %s queueMutex failed, reason:%s", pQueue->label, strerror(errno));

  if (tsem_post(&pQueue->fullSem) != 0) pError("post %s fullSem failed, reason:%s", pQueue->label, strerror(errno));

  return 0;
}

void taosCleanUpMsgQueue(void *param) {
  SRpcQueue *pQueue = (SRpcQueue *)param;
  if (pQueue == NULL) return;

  pthread_cancel(pQueue->qthread);

  tsem_destroy(&pQueue->emptySem);
  tsem_destroy(&pQueue->fullSem);
  pthread_mutex_destroy(&pQueue->queueMutex);

  free(pQueue->queue);
  free(pQueue); 
}

