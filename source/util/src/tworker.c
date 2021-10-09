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
#include "ulog.h"
#include "tqueue.h"
#include "tworker.h"

static void *taosWorkerThreadFp(void *wparam) {
  SWorker *    worker = wparam;
  SWorkerPool *pool = worker->pool;
  void *       msg = NULL;
  int32_t      qtype = 0;
  void *       ahandle = NULL;
  int32_t      code = 0;

  setThreadName(pool->name);

  while (1) {
    if (taosReadQitemFromQset(pool->qset, &qtype, (void **)&msg, &ahandle) == 0) {
      uDebug("pool:%s, worker:%d qset:%p, got no message and exiting", pool->name, worker->id, pool->qset);
      break;
    }

    code = (*pool->reqFp)(ahandle, msg);
    (*pool->rspFp)(ahandle, msg, qtype, code);
  }

  return NULL;
}

int32_t tWorkerInit(SWorkerPool *pool) {
  pool->qset = taosOpenQset();
  pool->workers = calloc(sizeof(SWorker), pool->max);
  pthread_mutex_init(&pool->mutex, NULL);
  for (int i = 0; i < pool->max; ++i) {
    SWorker *pWorker = pool->workers + i;
    pWorker->id = i;
    pWorker->pool = pool;
  }

  uInfo("worker:%s is initialized, min:%d max:%d", pool->name, pool->min, pool->max);
  return 0;
}

void tWorkerCleanup(SWorkerPool *pool) {
  for (int i = 0; i < pool->max; ++i) {
    SWorker *pWorker = pool->workers + i;
    if(taosCheckPthreadValid(pWorker->thread)) {
      taosQsetThreadResume(pool->qset);
    }
  }

  for (int i = 0; i < pool->max; ++i) {
    SWorker *pWorker = pool->workers + i;
    if (taosCheckPthreadValid(pWorker->thread)) {
      pthread_join(pWorker->thread, NULL);
    }
  }

  free(pool->workers);
  taosCloseQset(pool->qset);
  pthread_mutex_destroy(&pool->mutex);

  uInfo("worker:%s is closed", pool->name);
}

void *tWorkerAllocQueue(SWorkerPool *pool, void *ahandle) {
  pthread_mutex_lock(&pool->mutex);
  taos_queue pQueue = taosOpenQueue();
  if (pQueue == NULL) {
    pthread_mutex_unlock(&pool->mutex);
    return NULL;
  }

  taosAddIntoQset(pool->qset, pQueue, ahandle);

  // spawn a thread to process queue
  if (pool->num < pool->max) {
    do {
      SWorker *pWorker = pool->workers + pool->num;

      pthread_attr_t thAttr;
      pthread_attr_init(&thAttr);
      pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

      if (pthread_create(&pWorker->thread, &thAttr, taosWorkerThreadFp, pWorker) != 0) {
        uError("workers:%s:%d failed to create thread to process since %s", pool->name, pWorker->id, strerror(errno));
      }

      pthread_attr_destroy(&thAttr);
      pool->num++;
      uDebug("workers:%s:%d is launched, total:%d", pool->name, pWorker->id, pool->num);
    } while (pool->num < pool->min);
  }

  pthread_mutex_unlock(&pool->mutex);
  uDebug("workers:%s, queue:%p is allocated, ahandle:%p", pool->name, pQueue, ahandle);

  return pQueue;
}

void tWorkerFreeQueue(SWorkerPool *pool, void *pQueue) {
  taosCloseQueue(pQueue);
  uDebug("workers:%s, queue:%p is freed", pool->name, pQueue);
}
