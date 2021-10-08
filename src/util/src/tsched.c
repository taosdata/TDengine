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
#include "taosdef.h"
#include "tutil.h"
#include "tulog.h"
#include "tsched.h"
#include "ttimer.h"

#define DUMP_SCHEDULER_TIME_WINDOW 30000 //every 30sec, take a snap shot of task queue.

typedef struct {
  char            label[TSDB_LABEL_LEN];
  tsem_t          emptySem;
  tsem_t          fullSem;
  pthread_mutex_t queueMutex;
  int             fullSlot;
  int             emptySlot;
  int             queueSize;
  int             numOfThreads;
  pthread_t *     qthread;
  SSchedMsg *     queue;
  bool            stop;
  void*           pTmrCtrl;
  void*           pTimer;
} SSchedQueue;

static void *taosProcessSchedQueue(void *param);
static void taosDumpSchedulerStatus(void *qhandle, void *tmrId);

void *taosInitScheduler(int queueSize, int numOfThreads, const char *label) {
  SSchedQueue *pSched = (SSchedQueue *)calloc(sizeof(SSchedQueue), 1);
  if (pSched == NULL) {
    uError("%s: no enough memory for pSched", label);
    return NULL;
  }

  pSched->queue = (SSchedMsg *)calloc(sizeof(SSchedMsg), queueSize);
  if (pSched->queue == NULL) {
    uError("%s: no enough memory for queue", label);
    taosCleanUpScheduler(pSched);
    return NULL;
  }

  pSched->qthread = calloc(sizeof(pthread_t), numOfThreads);
  if (pSched->qthread == NULL) {
    uError("%s: no enough memory for qthread", label);
    taosCleanUpScheduler(pSched);
    return NULL;
  }

  pSched->queueSize = queueSize;
  tstrncpy(pSched->label, label, sizeof(pSched->label)); // fix buffer overflow

  pSched->fullSlot = 0;
  pSched->emptySlot = 0;

  if (pthread_mutex_init(&pSched->queueMutex, NULL) < 0) {
    uError("init %s:queueMutex failed(%s)", label, strerror(errno));
    taosCleanUpScheduler(pSched);
    return NULL;
  }

  if (tsem_init(&pSched->emptySem, 0, (unsigned int)pSched->queueSize) != 0) {
    uError("init %s:empty semaphore failed(%s)", label, strerror(errno));
    taosCleanUpScheduler(pSched);
    return NULL;
  }

  if (tsem_init(&pSched->fullSem, 0, 0) != 0) {
    uError("init %s:full semaphore failed(%s)", label, strerror(errno));
    taosCleanUpScheduler(pSched);
    return NULL;
  }

  pSched->stop = false;
  for (int i = 0; i < numOfThreads; ++i) {
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    int code = pthread_create(pSched->qthread + i, &attr, taosProcessSchedQueue, (void *)pSched);
    pthread_attr_destroy(&attr);
    if (code != 0) {
      uError("%s: failed to create rpc thread(%s)", label, strerror(errno));
      taosCleanUpScheduler(pSched);
      return NULL;
    }
    ++pSched->numOfThreads;
  }

  uDebug("%s scheduler is initialized, numOfThreads:%d", label, pSched->numOfThreads);

  return (void *)pSched;
}

void *taosInitSchedulerWithInfo(int queueSize, int numOfThreads, const char *label, void *tmrCtrl) {
  SSchedQueue* pSched = taosInitScheduler(queueSize, numOfThreads, label);

  if (tmrCtrl != NULL && pSched != NULL) {
    pSched->pTmrCtrl = tmrCtrl;
    taosTmrReset(taosDumpSchedulerStatus, DUMP_SCHEDULER_TIME_WINDOW, pSched, pSched->pTmrCtrl, &pSched->pTimer);
  }

  return pSched;
}

void *taosProcessSchedQueue(void *scheduler) {
  SSchedMsg    msg;
  SSchedQueue *pSched = (SSchedQueue *)scheduler;
  int ret = 0;

  char name[16] = {0};
  snprintf(name, tListLen(name), "%s-taskQ", pSched->label);
  setThreadName(name);

  while (1) {
    if ((ret = tsem_wait(&pSched->fullSem)) != 0) {
      uFatal("wait %s fullSem failed(%s)", pSched->label, strerror(errno));
      exit(ret);
    }
    if (pSched->stop) {
      break;
    }

    if ((ret = pthread_mutex_lock(&pSched->queueMutex)) != 0) {
      uFatal("lock %s queueMutex failed(%s)", pSched->label, strerror(errno));
      exit(ret);
    }

    msg = pSched->queue[pSched->fullSlot];
    memset(pSched->queue + pSched->fullSlot, 0, sizeof(SSchedMsg));
    pSched->fullSlot = (pSched->fullSlot + 1) % pSched->queueSize;

    if ((ret = pthread_mutex_unlock(&pSched->queueMutex)) != 0) {
      uFatal("unlock %s queueMutex failed(%s)", pSched->label, strerror(errno));
      exit(ret);
    }

    if ((ret = tsem_post(&pSched->emptySem)) != 0) {
      uFatal("post %s emptySem failed(%s)", pSched->label, strerror(errno));
      exit(ret);
    }

    if (msg.fp)
      (*(msg.fp))(&msg);
    else if (msg.tfp)
      (*(msg.tfp))(msg.ahandle, msg.thandle);
  }

  return NULL;
}

void taosScheduleTask(void *queueScheduler, SSchedMsg *pMsg) {
  SSchedQueue *pSched = (SSchedQueue *)queueScheduler;
  int ret = 0;

  if (pSched == NULL) {
    uError("sched is not ready, msg:%p is dropped", pMsg);
    return;
  }

  if ((ret = tsem_wait(&pSched->emptySem)) != 0) {
    uFatal("wait %s emptySem failed(%s)", pSched->label, strerror(errno));
    exit(ret);
  }

  if ((ret = pthread_mutex_lock(&pSched->queueMutex)) != 0) {
    uFatal("lock %s queueMutex failed(%s)", pSched->label, strerror(errno));
    exit(ret);
  }

  pSched->queue[pSched->emptySlot] = *pMsg;
  pSched->emptySlot = (pSched->emptySlot + 1) % pSched->queueSize;

  if ((ret = pthread_mutex_unlock(&pSched->queueMutex)) != 0) {
    uFatal("unlock %s queueMutex failed(%s)", pSched->label, strerror(errno));
    exit(ret);
  }

  if ((ret = tsem_post(&pSched->fullSem)) != 0) {
    uFatal("post %s fullSem failed(%s)", pSched->label, strerror(errno));
    exit(ret);
  }
}

void taosCleanUpScheduler(void *param) {
  SSchedQueue *pSched = (SSchedQueue *)param;
  if (pSched == NULL) return;

  pSched->stop = true;
  for (int i = 0; i < pSched->numOfThreads; ++i) {
  if (taosCheckPthreadValid(pSched->qthread[i])) {
      tsem_post(&pSched->fullSem);
    }
  }
  for (int i = 0; i < pSched->numOfThreads; ++i) {
    if (taosCheckPthreadValid(pSched->qthread[i])) {
      pthread_join(pSched->qthread[i], NULL);
    }
  }

  tsem_destroy(&pSched->emptySem);
  tsem_destroy(&pSched->fullSem);
  pthread_mutex_destroy(&pSched->queueMutex);
  
  if (pSched->pTimer) {
    taosTmrStopA(&pSched->pTimer);
  }

  if (pSched->queue) free(pSched->queue);
  if (pSched->qthread) free(pSched->qthread);
  free(pSched); // fix memory leak
}

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
