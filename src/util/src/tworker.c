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
#include "os.h"
#include "tulog.h"
#include "tqueue.h"
#include "tworker.h"

int32_t tWorkerInit(SWorkerPool *pPool) {
  pPool->qset = taosOpenQset();
  pPool->worker = calloc(sizeof(SWorker), pPool->max);
  pthread_mutex_init(&pPool->mutex, NULL);
  for (int i = 0; i < pPool->max; ++i) {
    SWorker *pWorker = pPool->worker + i;
    pWorker->id = i;
    pWorker->pPool = pPool;
  }

  uInfo("worker:%s is initialized, min:%d max:%d", pPool->name, pPool->min, pPool->max);
  return 0;
}

void tWorkerCleanup(SWorkerPool *pPool) {
  for (int i = 0; i < pPool->max; ++i) {
    SWorker *pWorker = pPool->worker + i;
    if (pWorker->thread != 0) {
      taosQsetThreadResume(pPool->qset);
    }
  }

  for (int i = 0; i < pPool->max; ++i) {
    SWorker *pWorker = pPool->worker + i;
    if (pWorker->thread != 0) {
      pthread_join(pWorker->thread, NULL);
    }
  }

  free(pPool->worker);
  taosCloseQset(pPool->qset);
  pthread_mutex_destroy(&pPool->mutex);

  uInfo("worker:%s is closed", pPool->name);
}

void *tWorkerAllocQueue(SWorkerPool *pPool, void *ahandle) {
  pthread_mutex_lock(&pPool->mutex);
  taos_queue pQueue = taosOpenQueue();
  if (pQueue == NULL) {
    pthread_mutex_unlock(&pPool->mutex);
    return NULL;
  }

  taosAddIntoQset(pPool->qset, pQueue, ahandle);

  // spawn a thread to process queue
  if (pPool->num < pPool->max) {
    do {
      SWorker *pWorker = pPool->worker + pPool->num;

      pthread_attr_t thAttr;
      pthread_attr_init(&thAttr);
      pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

      if (pthread_create(&pWorker->thread, &thAttr, pPool->workerFp, pWorker) != 0) {
        uError("worker:%s:%d failed to create thread to process since %s", pPool->name, pWorker->id, strerror(errno));
      }

      pthread_attr_destroy(&thAttr);
      pPool->num++;
      uDebug("worker:%s:%d is launched, total:%d", pPool->name, pWorker->id, pPool->num);
    } while (pPool->num < pPool->min);
  }

  pthread_mutex_unlock(&pPool->mutex);
  uDebug("worker:%s, queue:%p is allocated, ahandle:%p", pPool->name, pQueue, ahandle);

  return pQueue;
}

void tWorkerFreeQueue(SWorkerPool *pPool, void *pQueue) {
  taosCloseQueue(pQueue);
  uDebug("worker:%s, queue:%p is freed", pPool->name, pQueue);
}
