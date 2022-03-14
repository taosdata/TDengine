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
#include "taoserror.h"
#include "tlog.h"

typedef void *(*ThreadFp)(void *param);

int32_t tQWorkerInit(SQWorkerPool *pool) {
  pool->qset = taosOpenQset();
  pool->workers = calloc(sizeof(SQWorker), pool->max);
  if (pool->workers == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (pthread_mutex_init(&pool->mutex, NULL)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < pool->max; ++i) {
    SQWorker *worker = pool->workers + i;
    worker->id = i;
    worker->pool = pool;
  }

  uDebug("worker:%s is initialized, min:%d max:%d", pool->name, pool->min, pool->max);
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

  uDebug("worker:%s is closed", pool->name);
}

static void *tQWorkerThreadFp(SQWorker *worker) {
  SQWorkerPool *pool = worker->pool;
  FItem         fp = NULL;

  void   *msg = NULL;
  void   *ahandle = NULL;
  int32_t code = 0;

  taosBlockSIGPIPE();
  setThreadName(pool->name);
  uDebug("worker:%s:%d is running", pool->name, worker->id);

  while (1) {
    if (taosReadQitemFromQset(pool->qset, (void **)&msg, &ahandle, &fp) == 0) {
      uDebug("worker:%s:%d qset:%p, got no message and exiting", pool->name, worker->id, pool->qset);
      break;
    }

    if (fp != NULL) {
      (*fp)(ahandle, msg);
    }
  }

  return NULL;
}

STaosQueue *tWorkerAllocQueue(SQWorkerPool *pool, void *ahandle, FItem fp, ThreadFp threadFp) {
  pthread_mutex_lock(&pool->mutex);
  STaosQueue *queue = taosOpenQueue();
  if (queue == NULL) {
    pthread_mutex_unlock(&pool->mutex);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
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

      if (pthread_create(&worker->thread, &thAttr, threadFp, worker) != 0) {
        uError("worker:%s:%d failed to create thread to process since %s", pool->name, worker->id, strerror(errno));
        taosCloseQueue(queue);
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        queue = NULL;
        break;
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

STaosQueue *tQWorkerAllocQueue(SQWorkerPool *pool, void *ahandle, FItem fp) {
  return tWorkerAllocQueue(pool, ahandle, fp, (ThreadFp)tQWorkerThreadFp);
}

void tQWorkerFreeQueue(SQWorkerPool *pool, STaosQueue *queue) {
  taosCloseQueue(queue);
  uDebug("worker:%s, queue:%p is freed", pool->name, queue);
}

int32_t tFWorkerInit(SFWorkerPool *pool) { return tQWorkerInit((SQWorkerPool *)pool); }

void tFWorkerCleanup(SFWorkerPool *pool) { tQWorkerCleanup(pool); }

static void *tFWorkerThreadFp(SQWorker *worker) {
  SQWorkerPool *pool = worker->pool;

  FItem   fp = NULL;
  void   *msg = NULL;
  void   *ahandle = NULL;
  int32_t code = 0;

  taosBlockSIGPIPE();
  setThreadName(pool->name);
  uDebug("worker:%s:%d is running", pool->name, worker->id);

  while (1) {
    code = taosReadQitemFromQsetByThread(pool->qset, (void **)&msg, &ahandle, &fp, worker->id);

    if (code < 0) {
      uDebug("worker:%s:%d qset:%p, got no message and exiting", pool->name, worker->id, pool->qset);
      break;
    } else if (code == 0) {
      // uTrace("worker:%s:%d qset:%p, got no message and continue", pool->name, worker->id, pool->qset);
      continue;
    }

    if (fp != NULL) {
      (*fp)(ahandle, msg);
    }

    taosResetQsetThread(pool->qset, msg);
  }

  return NULL;
}

STaosQueue *tFWorkerAllocQueue(SQWorkerPool *pool, void *ahandle, FItem fp) {
  return tWorkerAllocQueue(pool, ahandle, fp, (ThreadFp)tQWorkerThreadFp);
}

void tFWorkerFreeQueue(SFWorkerPool *pool, STaosQueue *queue) { tQWorkerFreeQueue(pool, queue); }

int32_t tWWorkerInit(SWWorkerPool *pool) {
  pool->nextId = 0;
  pool->workers = calloc(pool->max, sizeof(SWWorker));
  if (pool->workers == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (pthread_mutex_init(&pool->mutex, NULL) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < pool->max; ++i) {
    SWWorker *worker = pool->workers + i;
    worker->id = i;
    worker->qall = NULL;
    worker->qset = NULL;
    worker->pool = pool;
  }

  uInfo("worker:%s is initialized, max:%d", pool->name, pool->max);
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

  uInfo("worker:%s is closed", pool->name);
}

static void *tWWorkerThreadFp(SWWorker *worker) {
  SWWorkerPool *pool = worker->pool;
  FItems        fp = NULL;

  void   *msg = NULL;
  void   *ahandle = NULL;
  int32_t numOfMsgs = 0;
  int32_t qtype = 0;

  taosBlockSIGPIPE();
  setThreadName(pool->name);
  uDebug("worker:%s:%d is running", pool->name, worker->id);

  while (1) {
    numOfMsgs = taosReadAllQitemsFromQset(worker->qset, worker->qall, &ahandle, &fp);
    if (numOfMsgs == 0) {
      uDebug("worker:%s:%d qset:%p, got no message and exiting", pool->name, worker->id, worker->qset);
      break;
    }

    if (fp != NULL) {
      (*fp)(ahandle, worker->qall, numOfMsgs);
    }
  }

  return NULL;
}

STaosQueue *tWWorkerAllocQueue(SWWorkerPool *pool, void *ahandle, FItems fp) {
  pthread_mutex_lock(&pool->mutex);
  SWWorker *worker = pool->workers + pool->nextId;

  STaosQueue *queue = taosOpenQueue();
  if (queue == NULL) {
    pthread_mutex_unlock(&pool->mutex);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
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
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&worker->thread, &thAttr, (ThreadFp)tWWorkerThreadFp, worker) != 0) {
      uError("worker:%s:%d failed to create thread to process since %s", pool->name, worker->id, strerror(errno));
      taosFreeQall(worker->qall);
      taosCloseQset(worker->qset);
      taosCloseQueue(queue);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
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

void tWWorkerFreeQueue(SWWorkerPool *pool, STaosQueue *queue) {
  taosCloseQueue(queue);
  uDebug("worker:%s, queue:%p is freed", pool->name, queue);
}
