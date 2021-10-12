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

typedef void* (*ThreadFp)(void *param);

int32_t tWorkerInit(SWorkerPool *pool) {
  pool->qset = taosOpenQset();
  pool->workers = calloc(sizeof(SWorker), pool->max);
  pthread_mutex_init(&pool->mutex, NULL);
  for (int i = 0; i < pool->max; ++i) {
    SWorker *worker = pool->workers + i;
    worker->id = i;
    worker->pool = pool;
  }

  uInfo("worker:%s is initialized, min:%d max:%d", pool->name, pool->min, pool->max);
  return 0;
}

void tWorkerCleanup(SWorkerPool *pool) {
  for (int i = 0; i < pool->max; ++i) {
    SWorker *worker = pool->workers + i;
    if (taosCheckPthreadValid(worker->thread)) {
      taosQsetThreadResume(pool->qset);
    }
  }

  for (int i = 0; i < pool->max; ++i) {
    SWorker *worker = pool->workers + i;
    if (taosCheckPthreadValid(worker->thread)) {
      pthread_join(worker->thread, NULL);
    }
  }

  free(pool->workers);
  taosCloseQset(pool->qset);
  pthread_mutex_destroy(&pool->mutex);

  uInfo("worker:%s is closed", pool->name);
}

static void *tWorkerThreadFp(SWorker *worker) {
  SWorkerPool *pool = worker->pool;

  void *  msg = NULL;
  void *  ahandle = NULL;
  int32_t qtype = 0;
  int32_t code = 0;

  taosBlockSIGPIPE();
  setThreadName(pool->name);
  uDebug("worker:%s:%d is running", pool->name, worker->id);

  while (1) {
    if (taosReadQitemFromQset(pool->qset, &qtype, (void **)&msg, &ahandle) == 0) {
      uDebug("worker:%s:%d qset:%p, got no message and exiting", pool->name, worker->id, pool->qset);
      break;
    }

    code = (*pool->startFp)(ahandle, msg, qtype);
    (*pool->endFp)(ahandle, msg, qtype, code);
  }

  return NULL;
}

taos_queue tWorkerAllocQueue(SWorkerPool *pool, void *ahandle) {
  pthread_mutex_lock(&pool->mutex);
  taos_queue queue = taosOpenQueue();
  if (queue == NULL) {
    pthread_mutex_unlock(&pool->mutex);
    return NULL;
  }

  taosAddIntoQset(pool->qset, queue, ahandle);

  // spawn a thread to process queue
  if (pool->num < pool->max) {
    do {
      SWorker *worker = pool->workers + pool->num;

      pthread_attr_t thAttr;
      pthread_attr_init(&thAttr);
      pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

      if (pthread_create(&worker->thread, &thAttr, (ThreadFp)tWorkerThreadFp, worker) != 0) {
        uError("worker:%s:%d failed to create thread to process since %s", pool->name, worker->id, strerror(errno));
      }

      pthread_attr_destroy(&thAttr);
      pool->num++;
      uDebug("worker:%s:%d is launched, total:%d", pool->name, worker->id, pool->num);
    } while (pool->num < pool->min);
  }

  pthread_mutex_unlock(&pool->mutex);
  uDebug("worker:%s, queue:%p is allocated, ahandle:%p", pool->name, queue, ahandle);

  return queue;
}

void tWorkerFreeQueue(SWorkerPool *pool, void *queue) {
  taosCloseQueue(queue);
  uDebug("worker:%s, queue:%p is freed", pool->name, queue);
}

int32_t tWriteWorkerInit(SWriteWorkerPool *pool) {
  pool->nextId = 0;
  pool->workers = calloc(sizeof(SWriteWorker), pool->max);
  if (pool->workers == NULL) return -1;

  pthread_mutex_init(&pool->mutex, NULL);
  for (int32_t i = 0; i < pool->max; ++i) {
    SWriteWorker *worker = pool->workers + i;
    worker->id = i;
    worker->qall = NULL;
    worker->qset = NULL;
    worker->pool = pool;
  }

  uInfo("worker:%s is initialized, max:%d", pool->name, pool->max);
  return 0;
}

void tWriteWorkerCleanup(SWriteWorkerPool *pool) {
  for (int32_t i = 0; i < pool->max; ++i) {
    SWriteWorker *worker = pool->workers + i;
    if (taosCheckPthreadValid(worker->thread)) {
      if (worker->qset) taosQsetThreadResume(worker->qset);
    }
  }

  for (int32_t i = 0; i < pool->max; ++i) {
    SWriteWorker *worker = pool->workers + i;
    if (taosCheckPthreadValid(worker->thread)) {
      pthread_join(worker->thread, NULL);
      taosFreeQall(worker->qall);
      taosCloseQset(worker->qset);
    }
  }

  free(pool->workers);
  pthread_mutex_destroy(&pool->mutex);

  uInfo("worker:%s is closed", pool->name);
}

static void *tWriteWorkerThreadFp(SWriteWorker *worker) {
  SWriteWorkerPool *pool = worker->pool;

  void *  msg = NULL;
  void *  ahandle = NULL;
  int32_t numOfMsgs = 0;
  int32_t qtype = 0;

  taosBlockSIGPIPE();
  setThreadName(pool->name);
  uDebug("worker:%s:%d is running", pool->name, worker->id);

  while (1) {
    numOfMsgs = taosReadAllQitemsFromQset(worker->qset, worker->qall, &ahandle);
    if (numOfMsgs == 0) {
      uDebug("worker:%s:%d qset:%p, got no message and exiting", pool->name, worker->id, worker->qset);
      break;
    }

    bool fsync = false;
    for (int32_t i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(worker->qall, &qtype, (void **)&msg);
      fsync = fsync | (*pool->startFp)(ahandle, msg, qtype);
    }

    (*pool->syncFp)(ahandle, fsync);

    // browse all items, and process them one by one
    taosResetQitems(worker->qall);
    for (int32_t i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(worker->qall, &qtype, (void **)&msg);
      (*pool->endFp)(ahandle, msg, qtype);
    }
  }

  return NULL;
}

taos_queue tWriteWorkerAllocQueue(SWriteWorkerPool *pool, void *ahandle) {
  pthread_mutex_lock(&pool->mutex);
  SWriteWorker *worker = pool->workers + pool->nextId;

  taos_queue *queue = taosOpenQueue();
  if (queue == NULL) {
    pthread_mutex_unlock(&pool->mutex);
    return NULL;
  }

  if (worker->qset == NULL) {
    worker->qset = taosOpenQset();
    if (worker->qset == NULL) {
      taosCloseQueue(queue);
      pthread_mutex_unlock(&pool->mutex);
      return NULL;
    }

    taosAddIntoQset(worker->qset, queue, ahandle);
    worker->qall = taosAllocateQall();
    if (worker->qall == NULL) {
      taosCloseQset(worker->qset);
      taosCloseQueue(queue);
      pthread_mutex_unlock(&pool->mutex);
      return NULL;
    }
    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&worker->thread, &thAttr, (ThreadFp)tWriteWorkerThreadFp, worker) != 0) {
      uError("worker:%s:%d failed to create thread to process since %s", pool->name, worker->id, strerror(errno));
      taosFreeQall(worker->qall);
      taosCloseQset(worker->qset);
      taosCloseQueue(queue);
      queue = NULL;
    } else {
      uDebug("worker:%s:%d is launched, max:%d", pool->name, worker->id, pool->max);
      pool->nextId = (pool->nextId + 1) % pool->max;
    }

    pthread_attr_destroy(&thAttr);
  } else {
    taosAddIntoQset(worker->qset, queue, ahandle);
    pool->nextId = (pool->nextId + 1) % pool->max;
  }

  pthread_mutex_unlock(&pool->mutex);
  uDebug("worker:%s, queue:%p is allocated, ahandle:%p", pool->name, queue, ahandle);

  return queue;
}

void tWriteWorkerFreeQueue(SWriteWorkerPool *pool, taos_queue queue) {
  taosCloseQueue(queue);
  uDebug("worker:%s, queue:%p is freed", pool->name, queue);
}
