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
#include "tsched.h"
#include "tdef.h"
#include "tgeosctx.h"
#include "tlog.h"
#include "ttimer.h"
#include "tutil.h"

#define DUMP_SCHEDULER_TIME_WINDOW 30000  // every 30sec, take a snap shot of task queue.

static void *taosProcessSchedQueue(void *param);
static void  taosDumpSchedulerStatus(void *qhandle, void *tmrId);

void *taosInitScheduler(int32_t queueSize, int32_t numOfThreads, const char *label, SSchedQueue *pSched) {
  bool schedMalloced = false;
  
  if (NULL == pSched) {
    pSched = (SSchedQueue *)taosMemoryCalloc(sizeof(SSchedQueue), 1);
    if (pSched == NULL) {
      uError("%s: no enough memory for pSched", label);
      return NULL;
    }

    schedMalloced = true;
  }

  pSched->queue = (SSchedMsg *)taosMemoryCalloc(sizeof(SSchedMsg), queueSize);
  if (pSched->queue == NULL) {
    uError("%s: no enough memory for queue", label);
    taosCleanUpScheduler(pSched);
    if (schedMalloced) {
      taosMemoryFree(pSched);
    }
    return NULL;
  }

  pSched->qthread = taosMemoryCalloc(sizeof(TdThread), numOfThreads);
  if (pSched->qthread == NULL) {
    uError("%s: no enough memory for qthread", label);
    taosCleanUpScheduler(pSched);
    if (schedMalloced) {
      taosMemoryFree(pSched);
    }
    return NULL;
  }

  pSched->queueSize = queueSize;
  tstrncpy(pSched->label, label, sizeof(pSched->label));  // fix buffer overflow

  pSched->fullSlot = 0;
  pSched->emptySlot = 0;

  if (taosThreadMutexInit(&pSched->queueMutex, NULL) < 0) {
    uError("init %s:queueMutex failed(%s)", label, strerror(errno));
    taosCleanUpScheduler(pSched);
    if (schedMalloced) {
      taosMemoryFree(pSched);
    }
    return NULL;
  }

  if (tsem_init(&pSched->emptySem, 0, (uint32_t)pSched->queueSize) != 0) {
    uError("init %s:empty semaphore failed(%s)", label, strerror(errno));
    taosCleanUpScheduler(pSched);
    if (schedMalloced) {
      taosMemoryFree(pSched);
    }
    return NULL;
  }

  if (tsem_init(&pSched->fullSem, 0, 0) != 0) {
    uError("init %s:full semaphore failed(%s)", label, strerror(errno));
    taosCleanUpScheduler(pSched);
    if (schedMalloced) {
      taosMemoryFree(pSched);
    }
    return NULL;
  }

  atomic_store_8(&pSched->stop, 0);
  for (int32_t i = 0; i < numOfThreads; ++i) {
    TdThreadAttr attr;
    taosThreadAttrInit(&attr);
    taosThreadAttrSetDetachState(&attr, PTHREAD_CREATE_JOINABLE);
    int32_t code = taosThreadCreate(pSched->qthread + i, &attr, taosProcessSchedQueue, (void *)pSched);
    taosThreadAttrDestroy(&attr);
    if (code != 0) {
      uError("%s: failed to create rpc thread(%s)", label, strerror(errno));
      taosCleanUpScheduler(pSched);
      if (schedMalloced) {
        taosMemoryFree(pSched);
      }
      return NULL;
    }
    ++pSched->numOfThreads;
  }

  uDebug("%s scheduler is initialized, numOfThreads:%d", label, pSched->numOfThreads);

  return (void *)pSched;
}

#ifdef BUILD_NO_CALL
void *taosInitSchedulerWithInfo(int32_t queueSize, int32_t numOfThreads, const char *label, void *tmrCtrl) {
  SSchedQueue *pSched = taosInitScheduler(queueSize, numOfThreads, label, NULL);

  if (tmrCtrl != NULL && pSched != NULL) {
    pSched->pTmrCtrl = tmrCtrl;
    taosTmrReset(taosDumpSchedulerStatus, DUMP_SCHEDULER_TIME_WINDOW, pSched, pSched->pTmrCtrl, &pSched->pTimer);
  }

  return pSched;
}
#endif

void *taosProcessSchedQueue(void *scheduler) {
  SSchedMsg    msg;
  SSchedQueue *pSched = (SSchedQueue *)scheduler;
  int32_t      ret = 0;

  char name[16] = {0};
  snprintf(name, tListLen(name), "%s-taskQ", pSched->label);
  setThreadName(name);

  while (1) {
    if ((ret = tsem_wait(&pSched->fullSem)) != 0) {
      uFatal("wait %s fullSem failed(%s)", pSched->label, strerror(errno));
    }
    if (atomic_load_8(&pSched->stop)) {
      break;
    }

    if ((ret = taosThreadMutexLock(&pSched->queueMutex)) != 0) {
      uFatal("lock %s queueMutex failed(%s)", pSched->label, strerror(errno));
    }

    msg = pSched->queue[pSched->fullSlot];
    memset(pSched->queue + pSched->fullSlot, 0, sizeof(SSchedMsg));
    pSched->fullSlot = (pSched->fullSlot + 1) % pSched->queueSize;

    if ((ret = taosThreadMutexUnlock(&pSched->queueMutex)) != 0) {
      uFatal("unlock %s queueMutex failed(%s)", pSched->label, strerror(errno));
    }

    if ((ret = tsem_post(&pSched->emptySem)) != 0) {
      uFatal("post %s emptySem failed(%s)", pSched->label, strerror(errno));
    }

    if (msg.fp)
      (*(msg.fp))(&msg);
    else if (msg.tfp)
      (*(msg.tfp))(msg.ahandle, msg.thandle);
  }

  destroyThreadLocalGeosCtx();

  return NULL;
}

int taosScheduleTask(void *queueScheduler, SSchedMsg *pMsg) {
  SSchedQueue *pSched = (SSchedQueue *)queueScheduler;
  int32_t      ret = 0;

  if (pSched == NULL) {
    uError("sched is not ready, msg:%p is dropped", pMsg);
    return -1;
  }

  if (atomic_load_8(&pSched->stop)) {
    uError("sched is already stopped, msg:%p is dropped", pMsg);
    return -1;
  }

  if ((ret = tsem_wait(&pSched->emptySem)) != 0) {
    uFatal("wait %s emptySem failed(%s)", pSched->label, strerror(errno));
  }

  if ((ret = taosThreadMutexLock(&pSched->queueMutex)) != 0) {
    uFatal("lock %s queueMutex failed(%s)", pSched->label, strerror(errno));
  }

  pSched->queue[pSched->emptySlot] = *pMsg;
  pSched->emptySlot = (pSched->emptySlot + 1) % pSched->queueSize;

  if ((ret = taosThreadMutexUnlock(&pSched->queueMutex)) != 0) {
    uFatal("unlock %s queueMutex failed(%s)", pSched->label, strerror(errno));
  }

  if ((ret = tsem_post(&pSched->fullSem)) != 0) {
    uFatal("post %s fullSem failed(%s)", pSched->label, strerror(errno));
  }
  return ret;
}

void taosCleanUpScheduler(void *param) {
  SSchedQueue *pSched = (SSchedQueue *)param;
  if (pSched == NULL) return;

  uDebug("start to cleanup %s schedQsueue", pSched->label);

  atomic_store_8(&pSched->stop, 1);

  taosMsleep(200);

  for (int32_t i = 0; i < pSched->numOfThreads; ++i) {
    if (taosCheckPthreadValid(pSched->qthread[i])) {
      tsem_post(&pSched->fullSem);
    }
  }
  for (int32_t i = 0; i < pSched->numOfThreads; ++i) {
    if (taosCheckPthreadValid(pSched->qthread[i])) {
      taosThreadJoin(pSched->qthread[i], NULL);
      taosThreadClear(&pSched->qthread[i]);
    }
  }

  tsem_destroy(&pSched->emptySem);
  tsem_destroy(&pSched->fullSem);
  taosThreadMutexDestroy(&pSched->queueMutex);

  if (pSched->pTimer) {
    taosTmrStop(pSched->pTimer);
    pSched->pTimer = NULL;
  }

  if (pSched->queue) taosMemoryFree(pSched->queue);
  if (pSched->qthread) taosMemoryFree(pSched->qthread);
  // taosMemoryFree(pSched);
}

#ifdef BUILD_NO_CALL
// for debug purpose, dump the scheduler status every 1min.
void taosDumpSchedulerStatus(void *qhandle, void *tmrId) {
  SSchedQueue *pSched = (SSchedQueue *)qhandle;
  if (pSched == NULL || pSched->pTimer == NULL || pSched->pTimer != tmrId) {
    return;
  }

  int32_t size = ((pSched->emptySlot - pSched->fullSlot) + pSched->queueSize) % pSched->queueSize;
  if (size > 0) {
    uDebug("scheduler:%s, current tasks in queue:%d, task thread:%d", pSched->label, size, pSched->numOfThreads);
  }

  taosTmrReset(taosDumpSchedulerStatus, DUMP_SCHEDULER_TIME_WINDOW, pSched, pSched->pTmrCtrl, &pSched->pTimer);
}
#endif
