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

#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "tlog.h"
#include "tsched.h"

typedef struct {
  char            label[16];
  sem_t           emptySem;
  sem_t           fullSem;
  pthread_mutex_t queueMutex;
  int             fullSlot;
  int             emptySlot;
  int             queueSize;
  int             numOfThreads;
  pthread_t *     qthread;
  SSchedMsg *     queue;
} SSchedQueue;

void (*taosSchedFp[128])(SSchedMsg *msg) = {0};
void *taosProcessSchedQueue(void *param);
void taosCleanUpScheduler(void *param);

void *taosInitScheduler(int queueSize, int numOfThreads, char *label) {
  pthread_attr_t attr;
  SSchedQueue *  pSched = (SSchedQueue *)malloc(sizeof(SSchedQueue));

  memset(pSched, 0, sizeof(SSchedQueue));
  pSched->queueSize = queueSize;
  pSched->numOfThreads = numOfThreads;
  strcpy(pSched->label, label);

  if (pthread_mutex_init(&pSched->queueMutex, NULL) < 0) {
    pError("init %s:queueMutex failed, reason:%s", pSched->label, strerror(errno));
    goto _error;
  }

  if (sem_init(&pSched->emptySem, 0, (unsigned int)pSched->queueSize) != 0) {
    pError("init %s:empty semaphore failed, reason:%s", pSched->label, strerror(errno));
    goto _error;
  }

  if (sem_init(&pSched->fullSem, 0, 0) != 0) {
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

  pSched->qthread = malloc(sizeof(pthread_t) * (size_t)pSched->numOfThreads);

  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  for (int i = 0; i < pSched->numOfThreads; ++i) {
    if (pthread_create(pSched->qthread + i, &attr, taosProcessSchedQueue, (void *)pSched) != 0) {
      pError("%s: failed to create rpc thread, reason:%s", pSched->label, strerror(errno));
      goto _error;
    }
  }

  pTrace("%s scheduler is initialized, numOfThreads:%d", pSched->label, pSched->numOfThreads);

  return (void *)pSched;

_error:
  taosCleanUpScheduler(pSched);
  return NULL;
}

void *taosProcessSchedQueue(void *param) {
  SSchedMsg    msg;
  SSchedQueue *pSched = (SSchedQueue *)param;

  while (1) {
    if (sem_wait(&pSched->fullSem) != 0) {
      pError("wait %s fullSem failed, errno:%d, reason:%s", pSched->label, errno, strerror(errno));
      if (errno == EINTR) {
        /* sem_wait is interrupted by interrupt, ignore and continue */
        continue;
      }
    }

    if (pthread_mutex_lock(&pSched->queueMutex) != 0)
      pError("lock %s queueMutex failed, reason:%s", pSched->label, strerror(errno));

    msg = pSched->queue[pSched->fullSlot];
    memset(pSched->queue + pSched->fullSlot, 0, sizeof(SSchedMsg));
    pSched->fullSlot = (pSched->fullSlot + 1) % pSched->queueSize;

    if (pthread_mutex_unlock(&pSched->queueMutex) != 0)
      pError("unlock %s queueMutex failed, reason:%s\n", pSched->label, strerror(errno));

    if (sem_post(&pSched->emptySem) != 0)
      pError("post %s emptySem failed, reason:%s\n", pSched->label, strerror(errno));

    if (msg.fp)
      (*(msg.fp))(&msg);
    else if (msg.tfp)
      (*(msg.tfp))(msg.ahandle, msg.thandle);
  }
}

int taosScheduleTask(void *qhandle, SSchedMsg *pMsg) {
  SSchedQueue *pSched = (SSchedQueue *)qhandle;
  if (pSched == NULL) {
    pError("sched is not ready, msg:%p is dropped", pMsg);
    return 0;
  }

  if (sem_wait(&pSched->emptySem) != 0) pError("wait %s emptySem failed, reason:%s", pSched->label, strerror(errno));

  if (pthread_mutex_lock(&pSched->queueMutex) != 0)
    pError("lock %s queueMutex failed, reason:%s", pSched->label, strerror(errno));

  pSched->queue[pSched->emptySlot] = *pMsg;
  pSched->emptySlot = (pSched->emptySlot + 1) % pSched->queueSize;

  if (pthread_mutex_unlock(&pSched->queueMutex) != 0)
    pError("unlock %s queueMutex failed, reason:%s", pSched->label, strerror(errno));

  if (sem_post(&pSched->fullSem) != 0) pError("post %s fullSem failed, reason:%s", pSched->label, strerror(errno));

  return 0;
}

void taosCleanUpScheduler(void *param) {
  SSchedQueue *pSched = (SSchedQueue *)param;
  if (pSched == NULL) return;

  for (int i = 0; i < pSched->numOfThreads; ++i) {
    pthread_cancel(pSched->qthread[i]);
    pthread_join(pSched->qthread[i], NULL);
  }

  sem_destroy(&pSched->emptySem);
  sem_destroy(&pSched->fullSem);
  pthread_mutex_destroy(&pSched->queueMutex);

  free(pSched->queue);
  free(pSched->qthread);
}
