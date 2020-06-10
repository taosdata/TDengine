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
#include "tlog.h"
#include "tsched.h"
#include "ttimer.h"

#define DUMP_SCHEDULER_TIME_WINDOW 30000 //every 30sec, take a snap shot of task queue.

typedef struct {
  char            label[16];
  tsem_t          emptySem;
  tsem_t          fullSem;
  pthread_mutex_t queueMutex;
  int             fullSlot;
  int             emptySlot;
  int             queueSize;
  int             numOfThreads;
  pthread_t *     qthread;
  SSchedMsg *     queue;
  
  void*           pTmrCtrl;
  void*           pTimer;
} SSchedQueue;

static void *taosProcessSchedQueue(void *param);
static void taosDumpSchedulerStatus(void *qhandle, void *tmrId);

void *taosInitScheduler(int queueSize, int numOfThreads, const char *label) {
  pthread_attr_t attr;
  SSchedQueue *  pSched = (SSchedQueue *)malloc(sizeof(SSchedQueue));
  if (pSched == NULL) {
    pError("%s: no enough memory for pSched, reason: %s", label, strerror(errno));
    goto _error;
  }

  memset(pSched, 0, sizeof(SSchedQueue));
  pSched->queueSize = queueSize;
  strncpy(pSched->label, label, sizeof(pSched->label)); // fix buffer overflow
  pSched->label[sizeof(pSched->label)-1] = '\0';

  if (pthread_mutex_init(&pSched->queueMutex, NULL) < 0) {
    pError("init %s:queueMutex failed, reason:%s", pSched->label, strerror(errno));
    goto _error;
  }

  if (tsem_init(&pSched->emptySem, 0, (unsigned int)pSched->queueSize) != 0) {
    pError("init %s:empty semaphore failed, reason:%s", pSched->label, strerror(errno));
    goto _error;
  }

  if (tsem_init(&pSched->fullSem, 0, 0) != 0) {
    pError("init %s:full semaphore failed, reason:%s", pSched->label, strerror(errno));
    goto _error;
  }

  if ((pSched->queue = (SSchedMsg *)malloc((size_t)pSched->queueSize * sizeof(SSchedMsg))) == NULL) {
    pError("%s: no enough memory for queue, reason:%s", pSched->label, strerror(errno));
    goto _error;
  }

  memset(pSched->queue, 0, (size_t)pSched->queueSize * sizeof(SSchedMsg));
  pSched->fullSlot = 0;
  pSched->emptySlot = 0;

  pSched->qthread = malloc(sizeof(pthread_t) * (size_t)numOfThreads);
  if (pSched->qthread == NULL) {
    pError("%s: no enough memory for qthread, reason: %s", pSched->label, strerror(errno));
    goto _error;
  }

  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  for (int i = 0; i < numOfThreads; ++i) {
    if (pthread_create(pSched->qthread + i, &attr, taosProcessSchedQueue, (void *)pSched) != 0) {
      pError("%s: failed to create rpc thread, reason:%s", pSched->label, strerror(errno));
      goto _error;
    }
    ++pSched->numOfThreads;
  }

  pTrace("%s scheduler is initialized, numOfThreads:%d", pSched->label, pSched->numOfThreads);

  return (void *)pSched;

_error:
  taosCleanUpScheduler(pSched);
  return NULL;
}

void *taosInitSchedulerWithInfo(int queueSize, int numOfThreads, const char *label, void *tmrCtrl) {
  SSchedQueue* pSched = taosInitScheduler(queueSize, numOfThreads, label);
  
  if (tmrCtrl != NULL && pSched != NULL) {
    pSched->pTmrCtrl = tmrCtrl;
    taosTmrReset(taosDumpSchedulerStatus, DUMP_SCHEDULER_TIME_WINDOW, pSched, pSched->pTmrCtrl, &pSched->pTimer);
  }
  
  return pSched;
}

void *taosProcessSchedQueue(void *param) {
  SSchedMsg    msg;
  SSchedQueue *pSched = (SSchedQueue *)param;

  while (1) {
    if (tsem_wait(&pSched->fullSem) != 0) {
      if (errno == EINTR) {
        /* sem_wait is interrupted by interrupt, ignore and continue */
        pTrace("wait %s fullSem was interrupted", pSched->label);
        continue;
      }
      pError("wait %s fullSem failed, errno:%d, reason:%s", pSched->label, errno, strerror(errno));
    }

    if (pthread_mutex_lock(&pSched->queueMutex) != 0)
      pError("lock %s queueMutex failed, reason:%s", pSched->label, strerror(errno));

    msg = pSched->queue[pSched->fullSlot];
    memset(pSched->queue + pSched->fullSlot, 0, sizeof(SSchedMsg));
    pSched->fullSlot = (pSched->fullSlot + 1) % pSched->queueSize;

    if (pthread_mutex_unlock(&pSched->queueMutex) != 0)
      pError("unlock %s queueMutex failed, reason:%s\n", pSched->label, strerror(errno));

    if (tsem_post(&pSched->emptySem) != 0)
      pError("post %s emptySem failed, reason:%s\n", pSched->label, strerror(errno));

    if (msg.fp)
      (*(msg.fp))(&msg);
    else if (msg.tfp)
      (*(msg.tfp))(msg.ahandle, msg.thandle);
  }

  return NULL;
}

int taosScheduleTask(void *qhandle, SSchedMsg *pMsg) {
  SSchedQueue *pSched = (SSchedQueue *)qhandle;
  if (pSched == NULL) {
    pError("sched is not ready, msg:%p is dropped", pMsg);
    return 0;
  }

  while (tsem_wait(&pSched->emptySem) != 0) {
    if (errno != EINTR) {
      pError("wait %s emptySem failed, reason:%s", pSched->label, strerror(errno));
      break;
    }
    pTrace("wait %s emptySem was interrupted", pSched->label);
  }

  if (pthread_mutex_lock(&pSched->queueMutex) != 0)
    pError("lock %s queueMutex failed, reason:%s", pSched->label, strerror(errno));

  pSched->queue[pSched->emptySlot] = *pMsg;
  pSched->emptySlot = (pSched->emptySlot + 1) % pSched->queueSize;

  if (pthread_mutex_unlock(&pSched->queueMutex) != 0)
    pError("unlock %s queueMutex failed, reason:%s", pSched->label, strerror(errno));

  if (tsem_post(&pSched->fullSem) != 0) pError("post %s fullSem failed, reason:%s", pSched->label, strerror(errno));

  return 0;
}

void taosCleanUpScheduler(void *param) {
  SSchedQueue *pSched = (SSchedQueue *)param;
  if (pSched == NULL) return;

  for (int i = 0; i < pSched->numOfThreads; ++i) {
    pthread_cancel(pSched->qthread[i]);
  }
  for (int i = 0; i < pSched->numOfThreads; ++i) {
    pthread_join(pSched->qthread[i], NULL);
  }

  tsem_destroy(&pSched->emptySem);
  tsem_destroy(&pSched->fullSem);
  pthread_mutex_destroy(&pSched->queueMutex);
  
  if (pSched->pTimer) {
    taosTmrStopA(&pSched->pTimer);
  }

  free(pSched->queue);
  free(pSched->qthread);
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
    pTrace("scheduler:%s, current tasks in queue:%d, task thread:%d", pSched->label, size, pSched->numOfThreads);
  }
  
  taosTmrReset(taosDumpSchedulerStatus, DUMP_SCHEDULER_TIME_WINDOW, pSched, pSched->pTmrCtrl, &pSched->pTimer);
}
