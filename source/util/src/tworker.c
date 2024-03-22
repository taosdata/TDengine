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
#include "tgeosctx.h"
#include "tlog.h"

#define QUEUE_THRESHOLD 1000 * 1000

typedef void *(*ThreadFp)(void *param);

int32_t tQWorkerInit(SQWorkerPool *pool) {
  pool->qset = taosOpenQset();
  pool->workers = taosMemoryCalloc(pool->max, sizeof(SQueueWorker));
  if (pool->workers == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  (void)taosThreadMutexInit(&pool->mutex, NULL);

  for (int32_t i = 0; i < pool->max; ++i) {
    SQueueWorker *worker = pool->workers + i;
    worker->id = i;
    worker->pool = pool;
  }

  uInfo("worker:%s is initialized, min:%d max:%d", pool->name, pool->min, pool->max);
  return 0;
}

void tQWorkerCleanup(SQWorkerPool *pool) {
  for (int32_t i = 0; i < pool->max; ++i) {
    SQueueWorker *worker = pool->workers + i;
    if (taosCheckPthreadValid(worker->thread)) {
      taosQsetThreadResume(pool->qset);
    }
  }

  for (int32_t i = 0; i < pool->max; ++i) {
    SQueueWorker *worker = pool->workers + i;
    if (taosCheckPthreadValid(worker->thread)) {
      uInfo("worker:%s:%d is stopping", pool->name, worker->id);
      taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
      uInfo("worker:%s:%d is stopped", pool->name, worker->id);
    }
  }

  taosMemoryFreeClear(pool->workers);
  taosCloseQset(pool->qset);
  taosThreadMutexDestroy(&pool->mutex);

  uInfo("worker:%s is closed", pool->name);
}

static void *tQWorkerThreadFp(SQueueWorker *worker) {
  SQWorkerPool *pool = worker->pool;
  SQueueInfo    qinfo = {0};
  void         *msg = NULL;
  int32_t       code = 0;

  taosBlockSIGPIPE();
  setThreadName(pool->name);
  worker->pid = taosGetSelfPthreadId();
  uInfo("worker:%s:%d is running, thread:%08" PRId64, pool->name, worker->id, worker->pid);

  while (1) {
    if (taosReadQitemFromQset(pool->qset, (void **)&msg, &qinfo) == 0) {
      uInfo("worker:%s:%d qset:%p, got no message and exiting, thread:%08" PRId64, pool->name, worker->id, pool->qset,
            worker->pid);
      break;
    }

    if (qinfo.timestamp != 0) {
      int64_t cost = taosGetTimestampUs() - qinfo.timestamp;
      if (cost > QUEUE_THRESHOLD) {
        uWarn("worker:%s,message has been queued for too long, cost: %" PRId64 "s", pool->name, cost / QUEUE_THRESHOLD);
      }
    }

    if (qinfo.fp != NULL) {
      qinfo.workerId = worker->id;
      qinfo.threadNum = pool->num;
      (*((FItem)qinfo.fp))(&qinfo, msg);
    }

    taosUpdateItemSize(qinfo.queue, 1);
  }

  destroyThreadLocalGeosCtx();

  return NULL;
}

STaosQueue *tQWorkerAllocQueue(SQWorkerPool *pool, void *ahandle, FItem fp) {
  STaosQueue *queue = taosOpenQueue();
  if (queue == NULL) return NULL;

  taosThreadMutexLock(&pool->mutex);
  taosSetQueueFp(queue, fp, NULL);
  taosAddIntoQset(pool->qset, queue, ahandle);

  // spawn a thread to process queue
  if (pool->num < pool->max) {
    do {
      SQueueWorker *worker = pool->workers + pool->num;

      TdThreadAttr thAttr;
      taosThreadAttrInit(&thAttr);
      taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

      if (taosThreadCreate(&worker->thread, &thAttr, (ThreadFp)tQWorkerThreadFp, worker) != 0) {
        taosCloseQueue(queue);
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        queue = NULL;
        break;
      }

      taosThreadAttrDestroy(&thAttr);
      pool->num++;
      uInfo("worker:%s:%d is launched, total:%d", pool->name, worker->id, pool->num);
    } while (pool->num < pool->min);
  }

  taosThreadMutexUnlock(&pool->mutex);
  uInfo("worker:%s, queue:%p is allocated, ahandle:%p", pool->name, queue, ahandle);

  return queue;
}

void tQWorkerFreeQueue(SQWorkerPool *pool, STaosQueue *queue) {
  uInfo("worker:%s, queue:%p is freed", pool->name, queue);
  taosCloseQueue(queue);
}

int32_t tAutoQWorkerInit(SAutoQWorkerPool *pool) {
  pool->qset = taosOpenQset();
  pool->workers = taosArrayInit(2, sizeof(SQueueWorker *));
  if (pool->workers == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  (void)taosThreadMutexInit(&pool->mutex, NULL);

  uInfo("worker:%s is initialized as auto", pool->name);
  return 0;
}

void tAutoQWorkerCleanup(SAutoQWorkerPool *pool) {
  int32_t size = taosArrayGetSize(pool->workers);
  for (int32_t i = 0; i < size; ++i) {
    SQueueWorker *worker = taosArrayGetP(pool->workers, i);
    if (taosCheckPthreadValid(worker->thread)) {
      taosQsetThreadResume(pool->qset);
    }
  }

  for (int32_t i = 0; i < size; ++i) {
    SQueueWorker *worker = taosArrayGetP(pool->workers, i);
    if (taosCheckPthreadValid(worker->thread)) {
      uInfo("worker:%s:%d is stopping", pool->name, worker->id);
      taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
      uInfo("worker:%s:%d is stopped", pool->name, worker->id);
    }
    taosMemoryFree(worker);
  }

  taosArrayDestroy(pool->workers);
  taosCloseQset(pool->qset);
  taosThreadMutexDestroy(&pool->mutex);

  uInfo("worker:%s is closed", pool->name);
}

static void *tAutoQWorkerThreadFp(SQueueWorker *worker) {
  SAutoQWorkerPool *pool = worker->pool;
  SQueueInfo        qinfo = {0};
  void             *msg = NULL;
  int32_t           code = 0;

  taosBlockSIGPIPE();
  setThreadName(pool->name);
  worker->pid = taosGetSelfPthreadId();
  uInfo("worker:%s:%d is running, thread:%08" PRId64, pool->name, worker->id, worker->pid);

  while (1) {
    if (taosReadQitemFromQset(pool->qset, (void **)&msg, &qinfo) == 0) {
      uInfo("worker:%s:%d qset:%p, got no message and exiting, thread:%08" PRId64, pool->name, worker->id, pool->qset,
            worker->pid);
      break;
    }

    if (qinfo.timestamp != 0) {
      int64_t cost = taosGetTimestampUs() - qinfo.timestamp;
      if (cost > QUEUE_THRESHOLD) {
        uWarn("worker:%s,message has been queued for too long, cost: %" PRId64 "s", pool->name, cost / QUEUE_THRESHOLD);
      }
    }

    if (qinfo.fp != NULL) {
      qinfo.workerId = worker->id;
      qinfo.threadNum = taosArrayGetSize(pool->workers);
      (*((FItem)qinfo.fp))(&qinfo, msg);
    }

    taosUpdateItemSize(qinfo.queue, 1);
  }

  return NULL;
}

STaosQueue *tAutoQWorkerAllocQueue(SAutoQWorkerPool *pool, void *ahandle, FItem fp) {
  STaosQueue *queue = taosOpenQueue();
  if (queue == NULL) return NULL;

  taosThreadMutexLock(&pool->mutex);
  taosSetQueueFp(queue, fp, NULL);
  taosAddIntoQset(pool->qset, queue, ahandle);

  int32_t queueNum = taosGetQueueNumber(pool->qset);
  int32_t curWorkerNum = taosArrayGetSize(pool->workers);
  int32_t dstWorkerNum = ceilf(queueNum * pool->ratio);
  if (dstWorkerNum < 2) dstWorkerNum = 2;

  // spawn a thread to process queue
  while (curWorkerNum < dstWorkerNum) {
    SQueueWorker *worker = taosMemoryCalloc(1, sizeof(SQueueWorker));
    if (worker == NULL || taosArrayPush(pool->workers, &worker) == NULL) {
      uError("worker:%s:%d failed to create", pool->name, curWorkerNum);
      taosMemoryFree(worker);
      taosCloseQueue(queue);
      taosThreadMutexUnlock(&pool->mutex);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    worker->id = curWorkerNum;
    worker->pool = pool;

    TdThreadAttr thAttr;
    taosThreadAttrInit(&thAttr);
    taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (taosThreadCreate(&worker->thread, &thAttr, (ThreadFp)tAutoQWorkerThreadFp, worker) != 0) {
      uError("worker:%s:%d failed to create thread, total:%d", pool->name, worker->id, curWorkerNum);
      (void)taosArrayPop(pool->workers);
      taosMemoryFree(worker);
      taosCloseQueue(queue);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }

    taosThreadAttrDestroy(&thAttr);
    int32_t numOfThreads = taosArrayGetSize(pool->workers);
    uInfo("worker:%s:%d is launched, total:%d, expect:%d", pool->name, worker->id, numOfThreads, dstWorkerNum);

    curWorkerNum++;
  }

  taosThreadMutexUnlock(&pool->mutex);
  uInfo("worker:%s, queue:%p is allocated, ahandle:%p", pool->name, queue, ahandle);

  return queue;
}

void tAutoQWorkerFreeQueue(SAutoQWorkerPool *pool, STaosQueue *queue) {
  uInfo("worker:%s, queue:%p is freed", pool->name, queue);
  taosCloseQueue(queue);
}

int32_t tWWorkerInit(SWWorkerPool *pool) {
  pool->nextId = 0;
  pool->workers = taosMemoryCalloc(pool->max, sizeof(SWWorker));
  if (pool->workers == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  (void)taosThreadMutexInit(&pool->mutex, NULL);

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
      uInfo("worker:%s:%d is stopping", pool->name, worker->id);
      taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
      taosFreeQall(worker->qall);
      taosCloseQset(worker->qset);
      uInfo("worker:%s:%d is stopped", pool->name, worker->id);
    }
  }

  taosMemoryFreeClear(pool->workers);
  taosThreadMutexDestroy(&pool->mutex);

  uInfo("worker:%s is closed", pool->name);
}

static void *tWWorkerThreadFp(SWWorker *worker) {
  SWWorkerPool *pool = worker->pool;
  SQueueInfo    qinfo = {0};
  void         *msg = NULL;
  int32_t       code = 0;
  int32_t       numOfMsgs = 0;

  taosBlockSIGPIPE();
  setThreadName(pool->name);
  worker->pid = taosGetSelfPthreadId();
  uInfo("worker:%s:%d is running, thread:%08" PRId64, pool->name, worker->id, worker->pid);

  while (1) {
    numOfMsgs = taosReadAllQitemsFromQset(worker->qset, worker->qall, &qinfo);
    if (numOfMsgs == 0) {
      uInfo("worker:%s:%d qset:%p, got no message and exiting, thread:%08" PRId64, pool->name, worker->id, worker->qset,
            worker->pid);
      break;
    }

    if (qinfo.timestamp != 0) {
      int64_t cost = taosGetTimestampUs() - qinfo.timestamp;
      if (cost > QUEUE_THRESHOLD) {
        uWarn("worker:%s,message has been queued for too long, cost: %" PRId64 "s", pool->name, cost / QUEUE_THRESHOLD);
      }
    }

    if (qinfo.fp != NULL) {
      qinfo.workerId = worker->id;
      qinfo.threadNum = pool->num;
      (*((FItems)qinfo.fp))(&qinfo, worker->qall, numOfMsgs);
    }
    taosUpdateItemSize(qinfo.queue, numOfMsgs);
  }

  return NULL;
}

STaosQueue *tWWorkerAllocQueue(SWWorkerPool *pool, void *ahandle, FItems fp) {
  taosThreadMutexLock(&pool->mutex);
  SWWorker *worker = pool->workers + pool->nextId;
  int32_t   code = -1;

  STaosQueue *queue = taosOpenQueue();
  if (queue == NULL) goto _OVER;

  taosSetQueueFp(queue, NULL, fp);
  if (worker->qset == NULL) {
    worker->qset = taosOpenQset();
    if (worker->qset == NULL) goto _OVER;

    taosAddIntoQset(worker->qset, queue, ahandle);
    worker->qall = taosAllocateQall();
    if (worker->qall == NULL) goto _OVER;

    TdThreadAttr thAttr;
    taosThreadAttrInit(&thAttr);
    taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (taosThreadCreate(&worker->thread, &thAttr, (ThreadFp)tWWorkerThreadFp, worker) != 0) goto _OVER;

    uInfo("worker:%s:%d is launched, max:%d", pool->name, worker->id, pool->max);
    pool->nextId = (pool->nextId + 1) % pool->max;

    taosThreadAttrDestroy(&thAttr);
    pool->num++;
    if (pool->num > pool->max) pool->num = pool->max;
  } else {
    taosAddIntoQset(worker->qset, queue, ahandle);
    pool->nextId = (pool->nextId + 1) % pool->max;
  }

  code = 0;

_OVER:
  taosThreadMutexUnlock(&pool->mutex);

  if (code == -1) {
    if (queue != NULL) taosCloseQueue(queue);
    if (worker->qset != NULL) taosCloseQset(worker->qset);
    if (worker->qall != NULL) taosFreeQall(worker->qall);
    return NULL;
  } else {
    while (worker->pid <= 0) taosMsleep(10);

    taosQueueSetThreadId(queue, worker->pid);
    uInfo("worker:%s, queue:%p is allocated, ahandle:%p thread:%08" PRId64, pool->name, queue, ahandle, worker->pid);
    return queue;
  }
}

void tWWorkerFreeQueue(SWWorkerPool *pool, STaosQueue *queue) {
  uInfo("worker:%s, queue:%p is freed", pool->name, queue);
  taosCloseQueue(queue);
}

int32_t tSingleWorkerInit(SSingleWorker *pWorker, const SSingleWorkerCfg *pCfg) {
  SQWorkerPool *pPool = &pWorker->pool;
  pPool->name = pCfg->name;
  pPool->min = pCfg->min;
  pPool->max = pCfg->max;
  if (tQWorkerInit(pPool) != 0) return -1;

  pWorker->queue = tQWorkerAllocQueue(pPool, pCfg->param, pCfg->fp);
  if (pWorker->queue == NULL) return -1;

  pWorker->name = pCfg->name;
  return 0;
}

void tSingleWorkerCleanup(SSingleWorker *pWorker) {
  if (pWorker->queue == NULL) return;

  while (!taosQueueEmpty(pWorker->queue)) {
    taosMsleep(10);
  }

  tQWorkerCleanup(&pWorker->pool);
  tQWorkerFreeQueue(&pWorker->pool, pWorker->queue);
}

int32_t tMultiWorkerInit(SMultiWorker *pWorker, const SMultiWorkerCfg *pCfg) {
  SWWorkerPool *pPool = &pWorker->pool;
  pPool->name = pCfg->name;
  pPool->max = pCfg->max;
  if (tWWorkerInit(pPool) != 0) return -1;

  pWorker->queue = tWWorkerAllocQueue(pPool, pCfg->param, pCfg->fp);
  if (pWorker->queue == NULL) return -1;

  pWorker->name = pCfg->name;
  return 0;
}

void tMultiWorkerCleanup(SMultiWorker *pWorker) {
  if (pWorker->queue == NULL) return;

  while (!taosQueueEmpty(pWorker->queue)) {
    taosMsleep(10);
  }

  tWWorkerCleanup(&pWorker->pool);
  tWWorkerFreeQueue(&pWorker->pool, pWorker->queue);
}
