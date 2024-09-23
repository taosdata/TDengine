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
#include "tcompare.h"
#include "tgeosctx.h"
#include "tlog.h"

#define QUEUE_THRESHOLD (1000 * 1000)

typedef void *(*ThreadFp)(void *param);

int32_t tQWorkerInit(SQWorkerPool *pool) {
  int32_t code = taosOpenQset(&pool->qset);
  if (code) return code;

  pool->workers = taosMemoryCalloc(pool->max, sizeof(SQueueWorker));
  if (pool->workers == NULL) {
    taosCloseQset(pool->qset);
    return terrno;
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
      (void)taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
      uInfo("worker:%s:%d is stopped", pool->name, worker->id);
    }
  }

  taosMemoryFreeClear(pool->workers);
  taosCloseQset(pool->qset);
  (void)taosThreadMutexDestroy(&pool->mutex);

  uInfo("worker:%s is closed", pool->name);
}

static void *tQWorkerThreadFp(SQueueWorker *worker) {
  SQWorkerPool *pool = worker->pool;
  SQueueInfo    qinfo = {0};
  void         *msg = NULL;
  int32_t       code = 0;

  int32_t ret = taosBlockSIGPIPE();
  if (ret < 0) {
    uError("worker:%s:%d failed to block SIGPIPE", pool->name, worker->id);
  }

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

  DestoryThreadLocalRegComp();

  return NULL;
}

STaosQueue *tQWorkerAllocQueue(SQWorkerPool *pool, void *ahandle, FItem fp) {
  int32_t     code;
  STaosQueue *queue;

  code = taosOpenQueue(&queue);
  if (code) {
    terrno = code;
    return NULL;
  }

  (void)taosThreadMutexLock(&pool->mutex);
  taosSetQueueFp(queue, fp, NULL);
  code = taosAddIntoQset(pool->qset, queue, ahandle);
  if (code) {
    taosCloseQueue(queue);
    (void)taosThreadMutexUnlock(&pool->mutex);
    terrno = code;
    return NULL;
  }

  // spawn a thread to process queue
  if (pool->num < pool->max) {
    do {
      SQueueWorker *worker = pool->workers + pool->num;

      TdThreadAttr thAttr;
      (void)taosThreadAttrInit(&thAttr);
      (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

      if (taosThreadCreate(&worker->thread, &thAttr, (ThreadFp)tQWorkerThreadFp, worker) != 0) {
        taosCloseQueue(queue);
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        queue = NULL;
        break;
      }

      (void)taosThreadAttrDestroy(&thAttr);
      pool->num++;
      uInfo("worker:%s:%d is launched, total:%d", pool->name, worker->id, pool->num);
    } while (pool->num < pool->min);
  }

  (void)taosThreadMutexUnlock(&pool->mutex);
  uInfo("worker:%s, queue:%p is allocated, ahandle:%p", pool->name, queue, ahandle);

  return queue;
}

void tQWorkerFreeQueue(SQWorkerPool *pool, STaosQueue *queue) {
  uInfo("worker:%s, queue:%p is freed", pool->name, queue);
  taosCloseQueue(queue);
}

int32_t tAutoQWorkerInit(SAutoQWorkerPool *pool) {
  int32_t code;

  code = taosOpenQset(&pool->qset);
  if (code) {
    return terrno = code;
  }

  pool->workers = taosArrayInit(2, sizeof(SQueueWorker *));
  if (pool->workers == NULL) {
    taosCloseQset(pool->qset);
    return terrno = TSDB_CODE_OUT_OF_MEMORY;
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
      (void)taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
      uInfo("worker:%s:%d is stopped", pool->name, worker->id);
    }
    taosMemoryFree(worker);
  }

  taosArrayDestroy(pool->workers);
  taosCloseQset(pool->qset);
  (void)taosThreadMutexDestroy(&pool->mutex);

  uInfo("worker:%s is closed", pool->name);
}

static void *tAutoQWorkerThreadFp(SQueueWorker *worker) {
  SAutoQWorkerPool *pool = worker->pool;
  SQueueInfo        qinfo = {0};
  void             *msg = NULL;
  int32_t           code = 0;

  int32_t ret = taosBlockSIGPIPE();
  if (ret < 0) {
    uError("worker:%s:%d failed to block SIGPIPE", pool->name, worker->id);
  }

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
  DestoryThreadLocalRegComp();

  return NULL;
}

STaosQueue *tAutoQWorkerAllocQueue(SAutoQWorkerPool *pool, void *ahandle, FItem fp) {
  int32_t     code;
  STaosQueue *queue;

  code = taosOpenQueue(&queue);
  if (code) {
    terrno = code;
    return NULL;
  }

  (void)taosThreadMutexLock(&pool->mutex);
  taosSetQueueFp(queue, fp, NULL);

  code = taosAddIntoQset(pool->qset, queue, ahandle);
  if (code) {
    taosCloseQueue(queue);
    (void)taosThreadMutexUnlock(&pool->mutex);
    terrno = code;
    return NULL;
  }

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
      (void)taosThreadMutexUnlock(&pool->mutex);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    worker->id = curWorkerNum;
    worker->pool = pool;

    TdThreadAttr thAttr;
    (void)taosThreadAttrInit(&thAttr);
    (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (taosThreadCreate(&worker->thread, &thAttr, (ThreadFp)tAutoQWorkerThreadFp, worker) != 0) {
      uError("worker:%s:%d failed to create thread, total:%d", pool->name, worker->id, curWorkerNum);
      void *tmp = taosArrayPop(pool->workers);
      taosMemoryFree(worker);
      taosCloseQueue(queue);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }

    (void)taosThreadAttrDestroy(&thAttr);
    int32_t numOfThreads = taosArrayGetSize(pool->workers);
    uInfo("worker:%s:%d is launched, total:%d, expect:%d", pool->name, worker->id, numOfThreads, dstWorkerNum);

    curWorkerNum++;
  }

  (void)taosThreadMutexUnlock(&pool->mutex);
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
    return terrno;
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
      (void)taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
      taosFreeQall(worker->qall);
      taosCloseQset(worker->qset);
      uInfo("worker:%s:%d is stopped", pool->name, worker->id);
    }
  }

  taosMemoryFreeClear(pool->workers);
  (void)taosThreadMutexDestroy(&pool->mutex);

  uInfo("worker:%s is closed", pool->name);
}

static void *tWWorkerThreadFp(SWWorker *worker) {
  SWWorkerPool *pool = worker->pool;
  SQueueInfo    qinfo = {0};
  void         *msg = NULL;
  int32_t       code = 0;
  int32_t       numOfMsgs = 0;

  int32_t ret = taosBlockSIGPIPE();
  if (ret < 0) {
    uError("worker:%s:%d failed to block SIGPIPE", pool->name, worker->id);
  }

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
  (void)taosThreadMutexLock(&pool->mutex);
  SWWorker   *worker = pool->workers + pool->nextId;
  int32_t     code = -1;
  STaosQueue *queue;

  code = taosOpenQueue(&queue);
  if (code) goto _OVER;

  taosSetQueueFp(queue, NULL, fp);
  if (worker->qset == NULL) {
    code = taosOpenQset(&worker->qset);
    if (code) goto _OVER;

    code = taosAddIntoQset(worker->qset, queue, ahandle);
    if (code) goto _OVER;
    code = taosAllocateQall(&worker->qall);
    if (code) goto _OVER;

    TdThreadAttr thAttr;
    (void)taosThreadAttrInit(&thAttr);
    (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (taosThreadCreate(&worker->thread, &thAttr, (ThreadFp)tWWorkerThreadFp, worker) != 0) goto _OVER;

    uInfo("worker:%s:%d is launched, max:%d", pool->name, worker->id, pool->max);
    pool->nextId = (pool->nextId + 1) % pool->max;

    (void)taosThreadAttrDestroy(&thAttr);
    pool->num++;
    if (pool->num > pool->max) pool->num = pool->max;
  } else {
    code = taosAddIntoQset(worker->qset, queue, ahandle);
    if (code) goto _OVER;
    pool->nextId = (pool->nextId + 1) % pool->max;
  }

_OVER:
  (void)taosThreadMutexUnlock(&pool->mutex);

  if (code) {
    if (queue != NULL) taosCloseQueue(queue);
    if (worker->qset != NULL) taosCloseQset(worker->qset);
    if (worker->qall != NULL) taosFreeQall(worker->qall);
    terrno = code;
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
  int32_t code;
  pWorker->poolType = pCfg->poolType;
  pWorker->name = pCfg->name;

  switch (pCfg->poolType) {
    case QWORKER_POOL: {
      SQWorkerPool *pPool = taosMemoryCalloc(1, sizeof(SQWorkerPool));
      if (!pPool) {
        return terrno;
      }
      pPool->name = pCfg->name;
      pPool->min = pCfg->min;
      pPool->max = pCfg->max;
      pWorker->pool = pPool;
      if ((code = tQWorkerInit(pPool))) {
        return (terrno = code);
      }

      pWorker->queue = tQWorkerAllocQueue(pPool, pCfg->param, pCfg->fp);
      if (pWorker->queue == NULL) {
        return terrno;
      }
    } break;
    case QUERY_AUTO_QWORKER_POOL: {
      SQueryAutoQWorkerPool *pPool = taosMemoryCalloc(1, sizeof(SQueryAutoQWorkerPool));
      if (!pPool) {
        return terrno;
      }
      pPool->name = pCfg->name;
      pPool->min = pCfg->min;
      pPool->max = pCfg->max;
      pWorker->pool = pPool;

      code = tQueryAutoQWorkerInit(pPool);
      if (code) return code;

      pWorker->queue = tQueryAutoQWorkerAllocQueue(pPool, pCfg->param, pCfg->fp);
      if (!pWorker->queue) {
        return terrno;
      }
    } break;
    default:
      return TSDB_CODE_INVALID_PARA;
  }
  return 0;
}

void tSingleWorkerCleanup(SSingleWorker *pWorker) {
  if (pWorker->queue == NULL) return;
  while (!taosQueueEmpty(pWorker->queue)) {
    taosMsleep(10);
  }

  switch (pWorker->poolType) {
    case QWORKER_POOL:
      tQWorkerCleanup(pWorker->pool);
      tQWorkerFreeQueue(pWorker->pool, pWorker->queue);
      taosMemoryFree(pWorker->pool);
      break;
    case QUERY_AUTO_QWORKER_POOL:
      tQueryAutoQWorkerCleanup(pWorker->pool);
      tQueryAutoQWorkerFreeQueue(pWorker->pool, pWorker->queue);
      taosMemoryFree(pWorker->pool);
      break;
    default:
      break;
  }
}

int32_t tMultiWorkerInit(SMultiWorker *pWorker, const SMultiWorkerCfg *pCfg) {
  SWWorkerPool *pPool = &pWorker->pool;
  pPool->name = pCfg->name;
  pPool->max = pCfg->max;

  int32_t code = tWWorkerInit(pPool);
  if (code) return code;

  pWorker->queue = tWWorkerAllocQueue(pPool, pCfg->param, pCfg->fp);
  if (pWorker->queue == NULL) {
    return terrno;
  }

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

static int32_t tQueryAutoQWorkerAddWorker(SQueryAutoQWorkerPool *pool);
static int32_t tQueryAutoQWorkerBeforeBlocking(void *p);
static int32_t tQueryAutoQWorkerRecoverFromBlocking(void *p);
static void    tQueryAutoQWorkerWaitingCheck(SQueryAutoQWorkerPool *pPool);
static bool    tQueryAutoQWorkerTryRecycleWorker(SQueryAutoQWorkerPool *pPool, SQueryAutoQWorker *pWorker);

#define GET_ACTIVE_N(int64_val)  (int32_t)((int64_val) >> 32)
#define GET_RUNNING_N(int64_val) (int32_t)(int64_val & 0xFFFFFFFF)

static int32_t atomicFetchSubActive(int64_t *ptr, int32_t val) {
  int64_t acutalSubVal = val;
  acutalSubVal <<= 32;
  int64_t newVal64 = atomic_fetch_sub_64(ptr, acutalSubVal);
  return GET_ACTIVE_N(newVal64);
}

static int32_t atomicFetchSubRunning(int64_t *ptr, int32_t val) { return GET_RUNNING_N(atomic_fetch_sub_64(ptr, val)); }

static int32_t atomicFetchAddActive(int64_t *ptr, int32_t val) {
  int64_t actualAddVal = val;
  actualAddVal <<= 32;
  int64_t newVal64 = atomic_fetch_add_64(ptr, actualAddVal);
  return GET_ACTIVE_N(newVal64);
}

static int32_t atomicFetchAddRunning(int64_t *ptr, int32_t val) { return GET_RUNNING_N(atomic_fetch_add_64(ptr, val)); }

static bool atomicCompareExchangeActive(int64_t *ptr, int32_t *expectedVal, int32_t newVal) {
  int64_t oldVal64 = *expectedVal, newVal64 = newVal;
  int32_t running = GET_RUNNING_N(*ptr);
  oldVal64 <<= 32;
  newVal64 <<= 32;
  oldVal64 |= running;
  newVal64 |= running;
  int64_t actualNewVal64 = atomic_val_compare_exchange_64(ptr, oldVal64, newVal64);
  if (actualNewVal64 == oldVal64) {
    return true;
  } else {
    *expectedVal = GET_ACTIVE_N(actualNewVal64);
    return false;
  }
}

static int64_t atomicCompareExchangeRunning(int64_t *ptr, int32_t *expectedVal, int32_t newVal) {
  int64_t oldVal64 = *expectedVal, newVal64 = newVal;
  int64_t activeShifted = GET_ACTIVE_N(*ptr);
  activeShifted <<= 32;
  oldVal64 |= activeShifted;
  newVal64 |= activeShifted;
  int64_t actualNewVal64 = atomic_val_compare_exchange_64(ptr, oldVal64, newVal64);
  if (actualNewVal64 == oldVal64) {
    return true;
  } else {
    *expectedVal = GET_RUNNING_N(actualNewVal64);
    return false;
  }
}

static int64_t atomicCompareExchangeActiveAndRunning(int64_t *ptr, int32_t *expectedActive, int32_t newActive,
                                                     int32_t *expectedRunning, int32_t newRunning) {
  int64_t oldVal64 = *expectedActive, newVal64 = newActive;
  oldVal64 <<= 32;
  oldVal64 |= *expectedRunning;
  newVal64 <<= 32;
  newVal64 |= newRunning;
  int64_t actualNewVal64 = atomic_val_compare_exchange_64(ptr, oldVal64, newVal64);
  if (actualNewVal64 == oldVal64) {
    return true;
  } else {
    *expectedActive = GET_ACTIVE_N(actualNewVal64);
    *expectedRunning = GET_RUNNING_N(actualNewVal64);
    return false;
  }
}

static void *tQueryAutoQWorkerThreadFp(SQueryAutoQWorker *worker) {
  SQueryAutoQWorkerPool *pool = worker->pool;
  SQueueInfo             qinfo = {0};
  void                  *msg = NULL;
  int32_t                code = 0;

  int32_t ret = taosBlockSIGPIPE();
  if (ret < 0) {
    uError("worker:%s:%d failed to block SIGPIPE", pool->name, worker->id);
  }

  setThreadName(pool->name);
  worker->pid = taosGetSelfPthreadId();
  uDebug("worker:%s:%d is running, thread:%08" PRId64, pool->name, worker->id, worker->pid);

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

    tQueryAutoQWorkerWaitingCheck(pool);

    if (qinfo.fp != NULL) {
      qinfo.workerId = worker->id;
      qinfo.threadNum = pool->num;
      qinfo.workerCb = pool->pCb;
      (*((FItem)qinfo.fp))(&qinfo, msg);
    }

    taosUpdateItemSize(qinfo.queue, 1);
    if (!tQueryAutoQWorkerTryRecycleWorker(pool, worker)) {
      uDebug("worker:%s:%d exited", pool->name, worker->id);
      break;
    }
  }

  DestoryThreadLocalRegComp();

  return NULL;
}

static bool tQueryAutoQWorkerTrySignalWaitingAfterBlock(void *p) {
  SQueryAutoQWorkerPool *pPool = p;
  bool                   ret = false;
  int32_t                waiting = pPool->waitingAfterBlockN;
  while (waiting > 0) {
    int32_t waitingNew = atomic_val_compare_exchange_32(&pPool->waitingAfterBlockN, waiting, waiting - 1);
    if (waitingNew == waiting) {
      (void)taosThreadMutexLock(&pPool->waitingAfterBlockLock);
      (void)taosThreadCondSignal(&pPool->waitingAfterBlockCond);
      (void)taosThreadMutexUnlock(&pPool->waitingAfterBlockLock);
      ret = true;
      break;
    }
    waiting = waitingNew;
  }
  return ret;
}

static bool tQueryAutoQWorkerTrySignalWaitingBeforeProcess(void *p) {
  SQueryAutoQWorkerPool *pPool = p;
  bool                   ret = false;
  int32_t                waiting = pPool->waitingBeforeProcessMsgN;
  while (waiting > 0) {
    int32_t waitingNew = atomic_val_compare_exchange_32(&pPool->waitingBeforeProcessMsgN, waiting, waiting - 1);
    if (waitingNew == waiting) {
      (void)taosThreadMutexLock(&pPool->waitingBeforeProcessMsgLock);
      (void)taosThreadCondSignal(&pPool->waitingBeforeProcessMsgCond);
      (void)taosThreadMutexUnlock(&pPool->waitingBeforeProcessMsgLock);
      ret = true;
      break;
    }
    waiting = waitingNew;
  }
  return ret;
}

static bool tQueryAutoQWorkerTryDecActive(void *p, int32_t minActive) {
  SQueryAutoQWorkerPool *pPool = p;
  bool                   ret = false;
  int64_t                val64 = pPool->activeRunningN;
  int32_t                active = GET_ACTIVE_N(val64), running = GET_RUNNING_N(val64);
  while (active > minActive) {
    if (atomicCompareExchangeActiveAndRunning(&pPool->activeRunningN, &active, active - 1, &running, running - 1))
      return true;
  }
  return false;
}

static void tQueryAutoQWorkerWaitingCheck(SQueryAutoQWorkerPool *pPool) {
  while (1) {
    int64_t val64 = pPool->activeRunningN;
    int32_t running = GET_RUNNING_N(val64), active = GET_ACTIVE_N(val64);
    while (running < pPool->num) {
      if (atomicCompareExchangeActiveAndRunning(&pPool->activeRunningN, &active, active, &running, running + 1)) {
        return;
      }
    }
    if (atomicCompareExchangeActive(&pPool->activeRunningN, &active, active - 1)) {
      break;
    }
  }
  // to wait for process
  (void)taosThreadMutexLock(&pPool->waitingBeforeProcessMsgLock);
  (void)atomic_fetch_add_32(&pPool->waitingBeforeProcessMsgN, 1);
  if (!pPool->exit) (void)taosThreadCondWait(&pPool->waitingBeforeProcessMsgCond, &pPool->waitingBeforeProcessMsgLock);
  // recovered from waiting
  (void)taosThreadMutexUnlock(&pPool->waitingBeforeProcessMsgLock);
  return;
}

bool tQueryAutoQWorkerTryRecycleWorker(SQueryAutoQWorkerPool *pPool, SQueryAutoQWorker *pWorker) {
  if (tQueryAutoQWorkerTrySignalWaitingAfterBlock(pPool) || tQueryAutoQWorkerTrySignalWaitingBeforeProcess(pPool) ||
      tQueryAutoQWorkerTryDecActive(pPool, pPool->num)) {
    (void)taosThreadMutexLock(&pPool->poolLock);
    SListNode *pNode = listNode(pWorker);
    SListNode *tNode = tdListPopNode(pPool->workers, pNode);
    // reclaim some workers
    if (pWorker->id >= pPool->maxInUse) {
      while (listNEles(pPool->exitedWorkers) > pPool->maxInUse - pPool->num) {
        SListNode         *head = tdListPopHead(pPool->exitedWorkers);
        SQueryAutoQWorker *pWorker = (SQueryAutoQWorker *)head->data;
        if (pWorker && taosCheckPthreadValid(pWorker->thread)) {
          (void)taosThreadJoin(pWorker->thread, NULL);
          taosThreadClear(&pWorker->thread);
        }
        taosMemoryFree(head);
      }
      tdListAppendNode(pPool->exitedWorkers, pNode);
      (void)taosThreadMutexUnlock(&pPool->poolLock);
      return false;
    }

    // put back to backup pool
    tdListAppendNode(pPool->backupWorkers, pNode);
    (void)taosThreadMutexUnlock(&pPool->poolLock);

    // start to wait at backup cond
    (void)taosThreadMutexLock(&pPool->backupLock);
    (void)atomic_fetch_add_32(&pPool->backupNum, 1);
    if (!pPool->exit) (void)taosThreadCondWait(&pPool->backupCond, &pPool->backupLock);
    (void)taosThreadMutexUnlock(&pPool->backupLock);

    // recovered from backup
    (void)taosThreadMutexLock(&pPool->poolLock);
    if (pPool->exit) {
      (void)taosThreadMutexUnlock(&pPool->poolLock);
      return false;
    }
    SListNode *tNode1 = tdListPopNode(pPool->backupWorkers, pNode);
    tdListAppendNode(pPool->workers, pNode);
    (void)taosThreadMutexUnlock(&pPool->poolLock);

    return true;
  } else {
    (void)atomicFetchSubRunning(&pPool->activeRunningN, 1);
    return true;
  }
}

int32_t tQueryAutoQWorkerInit(SQueryAutoQWorkerPool *pool) {
  int32_t code;

  (void)taosThreadMutexInit(&pool->poolLock, NULL);
  (void)taosThreadMutexInit(&pool->backupLock, NULL);
  (void)taosThreadMutexInit(&pool->waitingAfterBlockLock, NULL);
  (void)taosThreadMutexInit(&pool->waitingBeforeProcessMsgLock, NULL);

  (void)taosThreadCondInit(&pool->waitingBeforeProcessMsgCond, NULL);
  (void)taosThreadCondInit(&pool->waitingAfterBlockCond, NULL);
  (void)taosThreadCondInit(&pool->backupCond, NULL);

  code = taosOpenQset(&pool->qset);
  if (code) return terrno = code;
  pool->workers = tdListNew(sizeof(SQueryAutoQWorker));
  if (!pool->workers) return TSDB_CODE_OUT_OF_MEMORY;
  pool->backupWorkers = tdListNew(sizeof(SQueryAutoQWorker));
  if (!pool->backupWorkers) return TSDB_CODE_OUT_OF_MEMORY;
  pool->exitedWorkers = tdListNew(sizeof(SQueryAutoQWorker));
  if (!pool->exitedWorkers) return TSDB_CODE_OUT_OF_MEMORY;
  pool->maxInUse = pool->max * 2 + 2;

  if (!pool->pCb) {
    pool->pCb = taosMemoryCalloc(1, sizeof(SQueryAutoQWorkerPoolCB));
    if (!pool->pCb) return terrno;
    pool->pCb->pPool = pool;
    pool->pCb->beforeBlocking = tQueryAutoQWorkerBeforeBlocking;
    pool->pCb->afterRecoverFromBlocking = tQueryAutoQWorkerRecoverFromBlocking;
  }
  return TSDB_CODE_SUCCESS;
}

void tQueryAutoQWorkerCleanup(SQueryAutoQWorkerPool *pPool) {
  (void)taosThreadMutexLock(&pPool->poolLock);
  pPool->exit = true;
  int32_t size = 0;
  if (pPool->workers) {
    size = listNEles(pPool->workers);
  }
  if (pPool->backupWorkers) {
    size += listNEles(pPool->backupWorkers);
  }
  if (pPool->qset) {
    for (int32_t i = 0; i < size; ++i) {
      taosQsetThreadResume(pPool->qset);
    }
  }
  (void)taosThreadMutexUnlock(&pPool->poolLock);

  (void)taosThreadMutexLock(&pPool->backupLock);
  (void)taosThreadCondBroadcast(&pPool->backupCond);
  (void)taosThreadMutexUnlock(&pPool->backupLock);

  (void)taosThreadMutexLock(&pPool->waitingAfterBlockLock);
  (void)taosThreadCondBroadcast(&pPool->waitingAfterBlockCond);
  (void)taosThreadMutexUnlock(&pPool->waitingAfterBlockLock);

  (void)taosThreadMutexLock(&pPool->waitingBeforeProcessMsgLock);
  (void)taosThreadCondBroadcast(&pPool->waitingBeforeProcessMsgCond);
  (void)taosThreadMutexUnlock(&pPool->waitingBeforeProcessMsgLock);

  int32_t            idx = 0;
  SQueryAutoQWorker *worker = NULL;
  while (pPool->workers) {
    (void)taosThreadMutexLock(&pPool->poolLock);
    if (listNEles(pPool->workers) == 0) {
      (void)taosThreadMutexUnlock(&pPool->poolLock);
      break;
    }
    SListNode *pNode = tdListPopHead(pPool->workers);
    worker = (SQueryAutoQWorker *)pNode->data;
    (void)taosThreadMutexUnlock(&pPool->poolLock);
    if (worker && taosCheckPthreadValid(worker->thread)) {
      (void)taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
    }
    taosMemoryFree(pNode);
  }

  while (pPool->backupWorkers && listNEles(pPool->backupWorkers) > 0) {
    SListNode *pNode = tdListPopHead(pPool->backupWorkers);
    worker = (SQueryAutoQWorker *)pNode->data;
    if (worker && taosCheckPthreadValid(worker->thread)) {
      (void)taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
    }
    taosMemoryFree(pNode);
  }

  while (pPool->exitedWorkers && listNEles(pPool->exitedWorkers) > 0) {
    SListNode *pNode = tdListPopHead(pPool->exitedWorkers);
    worker = (SQueryAutoQWorker *)pNode->data;
    if (worker && taosCheckPthreadValid(worker->thread)) {
      (void)taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
    }
    taosMemoryFree(pNode);
  }

  pPool->workers = tdListFree(pPool->workers);
  pPool->backupWorkers = tdListFree(pPool->backupWorkers);
  pPool->exitedWorkers = tdListFree(pPool->exitedWorkers);
  taosMemoryFree(pPool->pCb);

  (void)taosThreadMutexDestroy(&pPool->poolLock);
  (void)taosThreadMutexDestroy(&pPool->backupLock);
  (void)taosThreadMutexDestroy(&pPool->waitingAfterBlockLock);
  (void)taosThreadMutexDestroy(&pPool->waitingBeforeProcessMsgLock);

  (void)taosThreadCondDestroy(&pPool->backupCond);
  (void)taosThreadCondDestroy(&pPool->waitingAfterBlockCond);
  (void)taosThreadCondDestroy(&pPool->waitingBeforeProcessMsgCond);
  taosCloseQset(pPool->qset);
}

STaosQueue *tQueryAutoQWorkerAllocQueue(SQueryAutoQWorkerPool *pool, void *ahandle, FItem fp) {
  STaosQueue *queue;
  int32_t     code = taosOpenQueue(&queue);
  if (code) {
    terrno = code;
    return NULL;
  }

  (void)taosThreadMutexLock(&pool->poolLock);
  taosSetQueueFp(queue, fp, NULL);
  code = taosAddIntoQset(pool->qset, queue, ahandle);
  if (code) {
    taosCloseQueue(queue);
    queue = NULL;
    (void)taosThreadMutexUnlock(&pool->poolLock);
    return NULL;
  }
  SQueryAutoQWorker  worker = {0};
  SQueryAutoQWorker *pWorker = NULL;

  // spawn a thread to process queue
  if (pool->num < pool->max) {
    do {
      worker.id = listNEles(pool->workers);
      worker.backupIdx = -1;
      worker.pool = pool;
      SListNode *pNode = tdListAdd(pool->workers, &worker);
      if (!pNode) {
        taosCloseQueue(queue);
        queue = NULL;
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      pWorker = (SQueryAutoQWorker *)pNode->data;

      TdThreadAttr thAttr;
      (void)taosThreadAttrInit(&thAttr);
      (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

      if (taosThreadCreate(&pWorker->thread, &thAttr, (ThreadFp)tQueryAutoQWorkerThreadFp, pWorker) != 0) {
        taosCloseQueue(queue);
        queue = NULL;
        break;
      }

      (void)taosThreadAttrDestroy(&thAttr);
      pool->num++;
      (void)atomicFetchAddActive(&pool->activeRunningN, 1);
      uInfo("worker:%s:%d is launched, total:%d", pool->name, pWorker->id, pool->num);
    } while (pool->num < pool->min);
  }

  (void)taosThreadMutexUnlock(&pool->poolLock);
  uInfo("worker:%s, queue:%p is allocated, ahandle:%p", pool->name, queue, ahandle);

  return queue;
}

void tQueryAutoQWorkerFreeQueue(SQueryAutoQWorkerPool *pPool, STaosQueue *pQ) { taosCloseQueue(pQ); }

static int32_t tQueryAutoQWorkerAddWorker(SQueryAutoQWorkerPool *pool) {
  // try backup pool
  int32_t backup = pool->backupNum;
  while (backup > 0) {
    int32_t backupNew = atomic_val_compare_exchange_32(&pool->backupNum, backup, backup - 1);
    if (backupNew == backup) {
      (void)taosThreadCondSignal(&pool->backupCond);
      return TSDB_CODE_SUCCESS;
    }
    backup = backupNew;
  }
  // backup pool is empty, create new
  SQueryAutoQWorker *pWorker = NULL;
  SQueryAutoQWorker  worker = {0};
  worker.pool = pool;
  worker.backupIdx = -1;
  (void)taosThreadMutexLock(&pool->poolLock);
  worker.id = listNEles(pool->workers);
  SListNode *pNode = tdListAdd(pool->workers, &worker);
  if (!pNode) {
    (void)taosThreadMutexUnlock(&pool->poolLock);
    return terrno;
  }
  (void)taosThreadMutexUnlock(&pool->poolLock);
  pWorker = (SQueryAutoQWorker *)pNode->data;

  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (taosThreadCreate(&pWorker->thread, &thAttr, (ThreadFp)tQueryAutoQWorkerThreadFp, pWorker) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }
  (void)taosThreadAttrDestroy(&thAttr);

  return TSDB_CODE_SUCCESS;
}

static int32_t tQueryAutoQWorkerBeforeBlocking(void *p) {
  SQueryAutoQWorkerPool *pPool = p;
  if (tQueryAutoQWorkerTrySignalWaitingAfterBlock(p) || tQueryAutoQWorkerTrySignalWaitingBeforeProcess(p) ||
      tQueryAutoQWorkerTryDecActive(p, pPool->num)) {
  } else {
    int32_t code = tQueryAutoQWorkerAddWorker(pPool);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    (void)atomicFetchSubRunning(&pPool->activeRunningN, 1);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tQueryAutoQWorkerRecoverFromBlocking(void *p) {
  SQueryAutoQWorkerPool *pPool = p;
  int64_t                val64 = pPool->activeRunningN;
  int32_t                running = GET_RUNNING_N(val64), active = GET_ACTIVE_N(val64);
  while (running < pPool->num) {
    if (atomicCompareExchangeActiveAndRunning(&pPool->activeRunningN, &active, active + 1, &running, running + 1)) {
      return TSDB_CODE_SUCCESS;
    }
  }
  (void)taosThreadMutexLock(&pPool->waitingAfterBlockLock);
  (void)atomic_fetch_add_32(&pPool->waitingAfterBlockN, 1);
  if (!pPool->exit) (void)taosThreadCondWait(&pPool->waitingAfterBlockCond, &pPool->waitingAfterBlockLock);
  (void)taosThreadMutexUnlock(&pPool->waitingAfterBlockLock);
  if (pPool->exit) return TSDB_CODE_QRY_QWORKER_QUIT;
  return TSDB_CODE_SUCCESS;
}
