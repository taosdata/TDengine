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
#include "tcompare.h"

#define QUEUE_THRESHOLD (1000 * 1000)

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
  DestoryThreadLocalRegComp();

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
  DestoryThreadLocalRegComp();

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
  pWorker->poolType = pCfg->poolType;
  pWorker->name = pCfg->name;

  switch (pCfg->poolType) {
    case QWORKER_POOL: {
      SQWorkerPool *pPool = taosMemoryCalloc(1, sizeof(SQWorkerPool));
      if (!pPool) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }
      pPool->name = pCfg->name;
      pPool->min = pCfg->min;
      pPool->max = pCfg->max;
      pWorker->pool = pPool;
      if (tQWorkerInit(pPool) != 0) return -1;

      pWorker->queue = tQWorkerAllocQueue(pPool, pCfg->param, pCfg->fp);
      if (pWorker->queue == NULL) return -1;
    } break;
    case QUERY_AUTO_QWORKER_POOL: {
      SQueryAutoQWorkerPool *pPool = taosMemoryCalloc(1, sizeof(SQueryAutoQWorkerPool));
      if (!pPool) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }
      pPool->name = pCfg->name;
      pPool->min = pCfg->min;
      pPool->max = pCfg->max;
      pWorker->pool = pPool;
      if (tQueryAutoQWorkerInit(pPool) != 0) return -1;

      pWorker->queue = tQueryAutoQWorkerAllocQueue(pPool, pCfg->param, pCfg->fp);
      if (!pWorker->queue) return -1;
    } break;
    default:
      assert(0);
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
      assert(0);
  }
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

static int32_t tQueryAutoQWorkerAddWorker(SQueryAutoQWorkerPool* pool);
static int32_t tQueryAutoQWorkerBeforeBlocking(void *p);
static int32_t tQueryAutoQWorkerRecoverFromBlocking(void *p);
static int32_t tQueryAutoQWorkerWaitingCheck(SQueryAutoQWorkerPool* pPool);
static bool    tQueryAutoQWorkerTryRecycleWorker(SQueryAutoQWorkerPool* pPool, SQueryAutoQWorker* pWorker);

static void *tQueryAutoQWorkerThreadFp(SQueryAutoQWorker *worker) {
  SQueryAutoQWorkerPool *pool = worker->pool;
  SQueueInfo             qinfo = {0};
  void                  *msg = NULL;
  int32_t                code = 0;

  taosBlockSIGPIPE();
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

  destroyThreadLocalGeosCtx();
  DestoryThreadLocalRegComp();

  return NULL;
}

static bool tQueryAutoQWorkerTrySignalWaitingAfterBlock(void *p) {
  SQueryAutoQWorkerPool *pPool = p;
  bool                   ret = false;
  taosThreadMutexLock(&pPool->waitingAfterBlockLock);
  int32_t waiting = pPool->waitingAfterBlockN;
  while (waiting > 0) {
    int32_t waitingNew = atomic_val_compare_exchange_32(&pPool->waitingAfterBlockN, waiting, waiting - 1);
    if (waitingNew == waiting) {
      taosThreadCondSignal(&pPool->waitingAfterBlockCond);
      ret = true;
      break;
    }
    waiting = waitingNew;
  }
  taosThreadMutexUnlock(&pPool->waitingAfterBlockLock);
  return ret;
}

static bool tQueryAutoQWorkerTrySignalWaitingBeforeProcess(void* p) {
  SQueryAutoQWorkerPool *pPool = p;
  bool                   ret = false;
  taosThreadMutexLock(&pPool->waitingBeforeProcessMsgLock);
  int32_t waiting = pPool->waitingBeforeProcessMsgN;
  while (waiting > 0) {
    int32_t waitingNew = atomic_val_compare_exchange_32(&pPool->waitingBeforeProcessMsgN, waiting, waiting - 1);
    if (waitingNew == waiting) {
      taosThreadCondSignal(&pPool->waitingBeforeProcessMsgCond);
      ret = true;
      break;
    }
    waiting = waitingNew;
  }
  taosThreadMutexUnlock(&pPool->waitingBeforeProcessMsgLock);
  return ret;
}

static bool tQueryAutoQWorkerTryDecActive(void* p, int32_t minActive) {
  SQueryAutoQWorkerPool *pPool = p;
  bool                   ret = false;
  taosThreadMutexLock(&pPool->activeLock);
  int32_t active = pPool->activeN;
  while (active > minActive) {
    int32_t activeNew = atomic_val_compare_exchange_32(&pPool->activeN, active, active - 1);
    if (activeNew == active) {
      ret = true;
      break;
    }
    active = activeNew;
  }
  atomic_fetch_sub_32(&pPool->runningN, 1);
  taosThreadMutexUnlock(&pPool->activeLock);
  return ret;
}

static int32_t tQueryAutoQWorkerWaitingCheck(SQueryAutoQWorkerPool* pPool) {
  taosThreadMutexLock(&pPool->activeLock);
  int32_t running = pPool->runningN;
  while (running < pPool->num) {
    int32_t runningNew = atomic_val_compare_exchange_32(&pPool->runningN, running, running + 1);
    if (runningNew == running) {
      // to running
      taosThreadMutexUnlock(&pPool->activeLock);
      return TSDB_CODE_SUCCESS;
    }
    running = runningNew;
  }
  atomic_fetch_sub_32(&pPool->activeN, 1);
  taosThreadMutexUnlock(&pPool->activeLock);
  // to wait for process
  taosThreadMutexLock(&pPool->waitingBeforeProcessMsgLock);
  atomic_fetch_add_32(&pPool->waitingBeforeProcessMsgN, 1);
  if (!pPool->exit) taosThreadCondWait(&pPool->waitingBeforeProcessMsgCond, &pPool->waitingBeforeProcessMsgLock);
  // recovered from waiting
  taosThreadMutexUnlock(&pPool->waitingBeforeProcessMsgLock);
  return TSDB_CODE_SUCCESS;
}

bool tQueryAutoQWorkerTryRecycleWorker(SQueryAutoQWorkerPool* pPool, SQueryAutoQWorker* pWorker) {
  if (tQueryAutoQWorkerTrySignalWaitingAfterBlock(pPool) || tQueryAutoQWorkerTrySignalWaitingBeforeProcess(pPool) ||
      tQueryAutoQWorkerTryDecActive(pPool, pPool->num)) {
    taosThreadMutexLock(&pPool->poolLock);
    SListNode* pNode = listNode(pWorker);
    tdListPopNode(pPool->workers, pNode);
    // reclaim some workers
    if (pWorker->id >= pPool->maxInUse) {
      while (listNEles(pPool->exitedWorkers) > pPool->maxInUse - pPool->num) {
        SListNode* head = tdListPopHead(pPool->exitedWorkers);
        SQueryAutoQWorker* pWorker = (SQueryAutoQWorker*)head->data;
        if (pWorker && taosCheckPthreadValid(pWorker->thread)) {
          taosThreadJoin(pWorker->thread, NULL);
          taosThreadClear(&pWorker->thread);
        }
        taosMemoryFree(head);
      }
      tdListAppendNode(pPool->exitedWorkers, pNode);
      taosThreadMutexUnlock(&pPool->poolLock);
      return false;
    }

    // put back to backup pool
    tdListAppendNode(pPool->backupWorkers, pNode);
    taosThreadMutexUnlock(&pPool->poolLock);

    // start to wait at backup cond
    taosThreadMutexLock(&pPool->backupLock);
    atomic_fetch_add_32(&pPool->backupNum, 1);
    if (!pPool->exit) taosThreadCondWait(&pPool->backupCond, &pPool->backupLock);
    taosThreadMutexUnlock(&pPool->backupLock);

    // recovered from backup
    taosThreadMutexLock(&pPool->poolLock);
    if (pPool->exit) {
      taosThreadMutexUnlock(&pPool->poolLock);
      return false;
    }
    tdListPopNode(pPool->backupWorkers, pNode);
    tdListAppendNode(pPool->workers, pNode);
    taosThreadMutexUnlock(&pPool->poolLock);

    return true;
  } else {
    return true;
  }
}

int32_t tQueryAutoQWorkerInit(SQueryAutoQWorkerPool *pool) {
  pool->qset = taosOpenQset();
  if (!pool->qset) return terrno;
  pool->workers = tdListNew(sizeof(SQueryAutoQWorker));
  if (!pool->workers) return TSDB_CODE_OUT_OF_MEMORY;
  pool->backupWorkers = tdListNew(sizeof(SQueryAutoQWorker));
  if (!pool->backupWorkers) return TSDB_CODE_OUT_OF_MEMORY;
  pool->exitedWorkers = tdListNew(sizeof(SQueryAutoQWorker));
  if (!pool->exitedWorkers) return TSDB_CODE_OUT_OF_MEMORY;
  pool->maxInUse = pool->max * 2 + 2;

  (void)taosThreadMutexInit(&pool->poolLock, NULL);
  (void)taosThreadMutexInit(&pool->backupLock, NULL);
  (void)taosThreadMutexInit(&pool->waitingAfterBlockLock, NULL);
  (void)taosThreadMutexInit(&pool->waitingBeforeProcessMsgLock, NULL);
  (void)taosThreadMutexInit(&pool->activeLock, NULL);

  (void)taosThreadCondInit(&pool->waitingBeforeProcessMsgCond, NULL);
  (void)taosThreadCondInit(&pool->waitingAfterBlockCond, NULL);
  (void)taosThreadCondInit(&pool->backupCond, NULL);

  if (!pool->pCb) {
    pool->pCb = taosMemoryCalloc(1, sizeof(SQueryAutoQWorkerPoolCB));
    if (!pool->pCb) return TSDB_CODE_OUT_OF_MEMORY;
    pool->pCb->pPool = pool;
    pool->pCb->beforeBlocking = tQueryAutoQWorkerBeforeBlocking;
    pool->pCb->afterRecoverFromBlocking = tQueryAutoQWorkerRecoverFromBlocking;
  }
  return TSDB_CODE_SUCCESS;
}

void tQueryAutoQWorkerCleanup(SQueryAutoQWorkerPool *pPool) {
  taosThreadMutexLock(&pPool->poolLock);
  pPool->exit = true;
  int32_t size = listNEles(pPool->workers);
  for (int32_t i = 0; i < size; ++i) {
    taosQsetThreadResume(pPool->qset);
  }
  size = listNEles(pPool->backupWorkers);
  for (int32_t i = 0; i < size; ++i) {
    taosQsetThreadResume(pPool->qset);
  }
  taosThreadMutexUnlock(&pPool->poolLock);

  taosThreadMutexLock(&pPool->backupLock);
  taosThreadCondBroadcast(&pPool->backupCond);
  taosThreadMutexUnlock(&pPool->backupLock);

  taosThreadMutexLock(&pPool->waitingAfterBlockLock);
  taosThreadCondBroadcast(&pPool->waitingAfterBlockCond);
  taosThreadMutexUnlock(&pPool->waitingAfterBlockLock);

  taosThreadMutexLock(&pPool->waitingBeforeProcessMsgLock);
  taosThreadCondBroadcast(&pPool->waitingBeforeProcessMsgCond);
  taosThreadMutexUnlock(&pPool->waitingBeforeProcessMsgLock);

  int32_t idx = 0;
  SQueryAutoQWorker* worker = NULL;
  while (true) {
    taosThreadMutexLock(&pPool->poolLock);
    if (listNEles(pPool->workers) == 0) {
      taosThreadMutexUnlock(&pPool->poolLock);
      break;
    }
    SListNode* pNode = tdListPopHead(pPool->workers);
    worker = (SQueryAutoQWorker*)pNode->data;
    taosThreadMutexUnlock(&pPool->poolLock);
    if (worker && taosCheckPthreadValid(worker->thread)) {
      taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
    }
    taosMemoryFree(pNode);
  }

  while (listNEles(pPool->backupWorkers) > 0) {
    SListNode* pNode = tdListPopHead(pPool->backupWorkers);
    worker = (SQueryAutoQWorker*)pNode->data;
    if (worker && taosCheckPthreadValid(worker->thread)) {
      taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
    }
    taosMemoryFree(pNode);
  }

  while (listNEles(pPool->exitedWorkers) > 0) {
    SListNode* pNode = tdListPopHead(pPool->exitedWorkers);
    worker = (SQueryAutoQWorker*)pNode->data;
    if (worker && taosCheckPthreadValid(worker->thread)) {
      taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
    }
    taosMemoryFree(pNode);
  }

  tdListFree(pPool->workers);
  tdListFree(pPool->backupWorkers);
  tdListFree(pPool->exitedWorkers);
  taosMemoryFree(pPool->pCb);

  taosThreadMutexDestroy(&pPool->poolLock);
  taosThreadMutexDestroy(&pPool->backupLock);
  taosThreadMutexDestroy(&pPool->waitingAfterBlockLock);
  taosThreadMutexDestroy(&pPool->waitingBeforeProcessMsgLock);
  taosThreadMutexDestroy(&pPool->activeLock);

  taosThreadCondDestroy(&pPool->backupCond);
  taosThreadCondDestroy(&pPool->waitingAfterBlockCond);
  taosThreadCondDestroy(&pPool->waitingBeforeProcessMsgCond);
  taosCloseQset(pPool->qset);
}

STaosQueue *tQueryAutoQWorkerAllocQueue(SQueryAutoQWorkerPool *pool, void *ahandle, FItem fp) {
  STaosQueue *queue = taosOpenQueue();
  if (queue == NULL) return NULL;

  taosThreadMutexLock(&pool->poolLock);
  taosSetQueueFp(queue, fp, NULL);
  taosAddIntoQset(pool->qset, queue, ahandle);
  SQueryAutoQWorker worker = {0};
  SQueryAutoQWorker* pWorker = NULL;

  // spawn a thread to process queue
  if (pool->num < pool->max) {
    do {
      worker.id = listNEles(pool->workers);
      worker.backupIdx = -1;
      worker.pool = pool;
      SListNode* pNode = tdListAdd(pool->workers, &worker);
      if (!pNode) {
        taosCloseQueue(queue);
        queue = NULL;
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      pWorker = (SQueryAutoQWorker*)pNode->data;

      TdThreadAttr thAttr;
      taosThreadAttrInit(&thAttr);
      taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

      if (taosThreadCreate(&pWorker->thread, &thAttr, (ThreadFp)tQueryAutoQWorkerThreadFp, pWorker) != 0) {
        taosCloseQueue(queue);
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        queue = NULL;
        break;
      }

      taosThreadAttrDestroy(&thAttr);
      pool->num++;
      pool->activeN++;
      uInfo("worker:%s:%d is launched, total:%d", pool->name, pWorker->id, pool->num);
    } while (pool->num < pool->min);
  }

  taosThreadMutexUnlock(&pool->poolLock);
  uInfo("worker:%s, queue:%p is allocated, ahandle:%p", pool->name, queue, ahandle);

  return queue;
}

void tQueryAutoQWorkerFreeQueue(SQueryAutoQWorkerPool *pPool, STaosQueue *pQ) {
  taosCloseQueue(pQ);
}

static int32_t tQueryAutoQWorkerAddWorker(SQueryAutoQWorkerPool* pool) {
  // try backup pool
  int32_t backup = pool->backupNum;
  while (backup > 0) {
    int32_t backupNew = atomic_val_compare_exchange_32(&pool->backupNum, backup, backup - 1);
    if (backupNew == backup) {
      taosThreadCondSignal(&pool->backupCond);
      return TSDB_CODE_SUCCESS;
    }
    backup = backupNew;
  }
  // backup pool is empty, create new
  SQueryAutoQWorker* pWorker = NULL;
  SQueryAutoQWorker worker = {0};
  worker.pool = pool;
  worker.backupIdx = -1;
  taosThreadMutexLock(&pool->poolLock);
  worker.id = listNEles(pool->workers);
  SListNode* pNode = tdListAdd(pool->workers, &worker);
  if (!pNode) {
    taosThreadMutexUnlock(&pool->poolLock);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }
  taosThreadMutexUnlock(&pool->poolLock);
  pWorker = (SQueryAutoQWorker*)pNode->data;

  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (taosThreadCreate(&pWorker->thread, &thAttr, (ThreadFp)tQueryAutoQWorkerThreadFp, pWorker) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }
  taosThreadAttrDestroy(&thAttr);

  return TSDB_CODE_SUCCESS;
}

static int32_t tQueryAutoQWorkerBeforeBlocking(void *p) {
  SQueryAutoQWorkerPool* pPool = p;
  if (tQueryAutoQWorkerTrySignalWaitingAfterBlock(p) || tQueryAutoQWorkerTrySignalWaitingBeforeProcess(p) ||
      tQueryAutoQWorkerTryDecActive(p, 1)) {
  } else {
    int32_t code = tQueryAutoQWorkerAddWorker(pPool);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tQueryAutoQWorkerRecoverFromBlocking(void *p) {
  SQueryAutoQWorkerPool* pPool = p;
  int32_t running = pPool->runningN;
  while (running < pPool->num) {
    int32_t runningNew = atomic_val_compare_exchange_32(&pPool->runningN, running, running + 1);
    if (runningNew == running) {
      atomic_fetch_add_32(&pPool->activeN, 1);
      return TSDB_CODE_SUCCESS;
    }
    running = runningNew;
  }
  taosThreadMutexLock(&pPool->waitingAfterBlockLock);
  atomic_fetch_add_32(&pPool->waitingAfterBlockN, 1);
  if (!pPool->exit) taosThreadCondWait(&pPool->waitingAfterBlockCond, &pPool->waitingAfterBlockLock);
  taosThreadMutexUnlock(&pPool->waitingAfterBlockLock);
  if (pPool->exit) return TSDB_CODE_QRY_QWORKER_QUIT;
  return TSDB_CODE_SUCCESS;
}
