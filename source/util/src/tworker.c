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
#include "tworker.h"
#include "ulog.h"

typedef void *(*ThreadFp)(void *param);

int32_t tQWorkerInit(SQWorkerPool *pool) {
  pool->qset = taosOpenQset();
  pool->workers = calloc(sizeof(SQWorker), pool->max);
  if (pthread_mutex_init(&pool->mutex, NULL)) {
    return -1;
  }

  for (int32_t i = 0; i < pool->max; ++i) {
    SQWorker *worker = pool->workers + i;
    worker->id = i;
    worker->pool = pool;
  }

  uDebug("qworker:%s is initialized, min:%d max:%d", pool->name, pool->min, pool->max);
  return 0;
}

void tQWorkerCleanup(SQWorkerPool *pool) {
  for (int32_t i = 0; i < pool->max; ++i) {
    SQWorker *worker = pool->workers + i;
    if (worker == NULL) continue;
    if (taosCheckPthreadValid(worker->thread)) {
      taosQsetThreadResume(pool->qset);
    }
  }

  for (int32_t i = 0; i < pool->max; ++i) {
    SQWorker *worker = pool->workers + i;
    if (worker == NULL) continue;
    if (taosCheckPthreadValid(worker->thread)) {
      pthread_join(worker->thread, NULL);
    }
  }

  tfree(pool->workers);
  taosCloseQset(pool->qset);
  pthread_mutex_destroy(&pool->mutex);

  uDebug("qworker:%s is closed", pool->name);
}

static void *tQWorkerThreadFp(SQWorker *worker) {
  SQWorkerPool *pool = worker->pool;
  FProcessItem  fp = NULL;

  void *  msg = NULL;
  void *  ahandle = NULL;
  int32_t code = 0;

  taosBlockSIGPIPE();
  setThreadName(pool->name);
  uDebug("qworker:%s:%d is running", pool->name, worker->id);

  while (1) {
    if (taosReadQitemFromQset(pool->qset, (void **)&msg, &ahandle, &fp) == 0) {
      uDebug("qworker:%s:%d qset:%p, got no message and exiting", pool->name, worker->id, pool->qset);
      break;
    }

    if (fp != NULL) {
      (*fp)(ahandle, msg);
    }
  }

  return NULL;
}

STaosQueue *tQWorkerAllocQueue(SQWorkerPool *pool, void *ahandle, FProcessItem fp) {
  pthread_mutex_lock(&pool->mutex);
  STaosQueue *queue = taosOpenQueue();
  if (queue == NULL) {
    pthread_mutex_unlock(&pool->mutex);
    return NULL;
  }

  taosSetQueueFp(queue, fp, NULL);
  taosAddIntoQset(pool->qset, queue, ahandle);

  // spawn a thread to process queue
  if (pool->num < pool->max) {
    do {
      SQWorker *worker = pool->workers + pool->num;

      pthread_attr_t thAttr;
      pthread_attr_init(&thAttr);
      pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

      if (pthread_create(&worker->thread, &thAttr, (ThreadFp)tQWorkerThreadFp, worker) != 0) {
        uError("qworker:%s:%d failed to create thread to process since %s", pool->name, worker->id, strerror(errno));
        taosCloseQueue(queue);
        queue = NULL;
        break;
      }

      pthread_attr_destroy(&thAttr);
      pool->num++;
      uDebug("qworker:%s:%d is launched, total:%d", pool->name, worker->id, pool->num);
    } while (pool->num < pool->min);
  }

  pthread_mutex_unlock(&pool->mutex);
  uDebug("qworker:%s, queue:%p is allocated, ahandle:%p", pool->name, queue, ahandle);

  return queue;
}

void tQWorkerFreeQueue(SQWorkerPool *pool, STaosQueue *queue) {
  taosCloseQueue(queue);
  uDebug("qworker:%s, queue:%p is freed", pool->name, queue);
}

int32_t tWWorkerInit(SWWorkerPool *pool) {
  pool->nextId = 0;
  pool->workers = calloc(sizeof(SWWorker), pool->max);
  if (pool->workers == NULL) return -1;

  pthread_mutex_init(&pool->mutex, NULL);
  for (int32_t i = 0; i < pool->max; ++i) {
    SWWorker *worker = pool->workers + i;
    worker->id = i;
    worker->qall = NULL;
    worker->qset = NULL;
    worker->pool = pool;
  }

  uInfo("wworker:%s is initialized, max:%d", pool->name, pool->max);
  return 0;
}

void tWWorkerCleanup(SWWorkerPool *pool) {
  for (int32_t i = 0; i < pool->max; ++i) {
    SWWorker *worker = pool->workers + i;
    if (taosCheckPthreadValid(worker->thread)) {
      if (worker->qset) {
        taosQsetThreadResume(worker->qset);
      }
    }
  }

  for (int32_t i = 0; i < pool->max; ++i) {
    SWWorker *worker = pool->workers + i;
    if (taosCheckPthreadValid(worker->thread)) {
      pthread_join(worker->thread, NULL);
      taosFreeQall(worker->qall);
      taosCloseQset(worker->qset);
    }
  }

  tfree(pool->workers);
  pthread_mutex_destroy(&pool->mutex);

  uInfo("wworker:%s is closed", pool->name);
}

static void *tWriteWorkerThreadFp(SWWorker *worker) {
  SWWorkerPool *pool = worker->pool;
  FProcessItems fp = NULL;

  void *  msg = NULL;
  void *  ahandle = NULL;
  int32_t numOfMsgs = 0;
  int32_t qtype = 0;

  taosBlockSIGPIPE();
  setThreadName(pool->name);
  uDebug("wworker:%s:%d is running", pool->name, worker->id);

  while (1) {
    numOfMsgs = taosReadAllQitemsFromQset(worker->qset, worker->qall, &ahandle, &fp);
    if (numOfMsgs == 0) {
      uDebug("wworker:%s:%d qset:%p, got no message and exiting", pool->name, worker->id, worker->qset);
      break;
    }

    if (fp != NULL) {
      (*fp)(ahandle, worker->qall, numOfMsgs);
    }
  }

  return NULL;
}

STaosQueue *tWWorkerAllocQueue(SWWorkerPool *pool, void *ahandle, FProcessItems fp) {
  pthread_mutex_lock(&pool->mutex);
  SWWorker *worker = pool->workers + pool->nextId;

  STaosQueue *queue = taosOpenQueue();
  if (queue == NULL) {
    pthread_mutex_unlock(&pool->mutex);
    return NULL;
  }

  taosSetQueueFp(queue, NULL, fp);

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
      uError("wworker:%s:%d failed to create thread to process since %s", pool->name, worker->id, strerror(errno));
      taosFreeQall(worker->qall);
      taosCloseQset(worker->qset);
      taosCloseQueue(queue);
      queue = NULL;
    } else {
      uDebug("wworker:%s:%d is launched, max:%d", pool->name, worker->id, pool->max);
      pool->nextId = (pool->nextId + 1) % pool->max;
    }

    pthread_attr_destroy(&thAttr);
  } else {
    taosAddIntoQset(worker->qset, queue, ahandle);
    pool->nextId = (pool->nextId + 1) % pool->max;
  }

  pthread_mutex_unlock(&pool->mutex);
  uDebug("wworker:%s, queue:%p is allocated, ahandle:%p", pool->name, queue, ahandle);

  return queue;
}

void tWWorkerFreeQueue(SWWorkerPool *pool, STaosQueue *queue) {
  taosCloseQueue(queue);
  uDebug("wworker:%s, queue:%p is freed", pool->name, queue);
}
