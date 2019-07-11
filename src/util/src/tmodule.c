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
#include <unistd.h>

#include "tmodule.h"
#include "tutil.h"

void *taosProcessQueue(void *param);

char *taosDisplayModuleStatus(int moduleNum) {
  static char status[256];
  int         i;

  status[0] = 0;

  for (i = 1; i < moduleNum; ++i)
    if (taosCheckPthreadValid(moduleObj[i].thread)) sprintf(status + strlen(status), "%s ", moduleObj[i].name);

  if (status[0] == 0)
    sprintf(status, "all module is down");
  else
    sprintf(status, " is(are) up");

  return status;
}

int taosInitModule(module_t *pMod) {
  pthread_attr_t attr;

  if (pthread_mutex_init(&pMod->queueMutex, NULL) < 0) {
    printf("ERROR: init %s queueMutex failed, reason:%s\n", pMod->name, strerror(errno));
    taosCleanUpModule(pMod);
    return -1;
  }

  if (pthread_mutex_init(&pMod->stmMutex, NULL) < 0) {
    printf("ERROR: init %s stmMutex failed, reason:%s\n", pMod->name, strerror(errno));
    taosCleanUpModule(pMod);
    return -1;
  }

  if (sem_init(&pMod->emptySem, 0, (unsigned int)pMod->queueSize) != 0) {
    printf("ERROR: init %s empty semaphore failed, reason:%s\n", pMod->name, strerror(errno));
    taosCleanUpModule(pMod);
    return -1;
  }

  if (sem_init(&pMod->fullSem, 0, 0) != 0) {
    printf("ERROR: init %s full semaphore failed, reason:%s\n", pMod->name, strerror(errno));
    taosCleanUpModule(pMod);
    return -1;
  }

  if ((pMod->queue = (msg_t *)malloc((size_t)pMod->queueSize * sizeof(msg_t))) == NULL) {
    printf("ERROR: %s no enough memory, reason:%s\n", pMod->name, strerror(errno));
    taosCleanUpModule(pMod);
    return -1;
  }

  memset(pMod->queue, 0, (size_t)pMod->queueSize * sizeof(msg_t));
  pMod->fullSlot = 0;
  pMod->emptySlot = 0;

  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  if (pthread_create(&pMod->thread, &attr, taosProcessQueue, (void *)pMod) != 0) {
    printf("ERROR: %s failed to create thread, reason:%s\n", pMod->name, strerror(errno));
    taosCleanUpModule(pMod);
    return -1;
  }

  if (pMod->init) return (*(pMod->init))();

  return 0;
}

void *taosProcessQueue(void *param) {
  msg_t     msg;
  module_t *pMod = (module_t *)param;
  int       oldType;

  pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, &oldType);

  signal(SIGINT, SIG_IGN);

  while (1) {
    if (sem_wait(&pMod->fullSem) != 0)
      printf("ERROR: wait %s fullSem failed, reason:%s\n", pMod->name, strerror(errno));

    if (pthread_mutex_lock(&pMod->queueMutex) != 0)
      printf("ERROR: lock %s queueMutex failed, reason:%s\n", pMod->name, strerror(errno));

    msg = pMod->queue[pMod->fullSlot];
    memset(&(pMod->queue[pMod->fullSlot]), 0, sizeof(msg_t));
    pMod->fullSlot = (pMod->fullSlot + 1) % pMod->queueSize;

    if (pthread_mutex_unlock(&pMod->queueMutex) != 0)
      printf("ERROR: unlock %s queueMutex failed, reason:%s\n", pMod->name, strerror(errno));

    if (sem_post(&pMod->emptySem) != 0)
      printf("ERROR: post %s emptySem failed, reason:%s\n", pMod->name, strerror(errno));

    /* process the message */
    if (msg.cid < 0 || msg.cid >= maxCid) {
      /*printf("ERROR: cid:%d is out of range, msg is discarded\n", msg.cid);*/
      continue;
    }

    /*
        if ( pthread_mutex_lock ( &(pMod->stmMutex)) != 0 )
          printf("ERROR: lock %s stmMutex failed, reason:%s\n", pMod->name,
       strerror(errno));
    */
    (*(pMod->processMsg))(&msg);

    tfree(msg.msg);
    /*
        if ( pthread_mutex_unlock ( &(pMod->stmMutex)) != 0 )
          printf("ERROR: unlock %s stmMutex failed, reason:%s\n", pMod->name,
       strerror(errno));
    */
  }
}

int taosSendMsgToModule(module_t *pMod, int cid, int mid, int tid, char *msg) {
  if (sem_wait(&pMod->emptySem) != 0)
    printf("ERROR: wait %s emptySem failed, reason:%s\n", pMod->name, strerror(errno));

  if (pthread_mutex_lock(&pMod->queueMutex) != 0)
    printf("ERROR: lock %s queueMutex failed, reason:%s\n", pMod->name, strerror(errno));

  pMod->queue[pMod->emptySlot].cid = cid;
  pMod->queue[pMod->emptySlot].mid = mid;
  pMod->queue[pMod->emptySlot].tid = tid;
  pMod->queue[pMod->emptySlot].msg = msg;
  pMod->emptySlot = (pMod->emptySlot + 1) % pMod->queueSize;

  if (pthread_mutex_unlock(&pMod->queueMutex) != 0)
    printf("ERROR: unlock %s queueMutex failed, reason:%s\n", pMod->name, strerror(errno));

  if (sem_post(&pMod->fullSem) != 0) printf("ERROR: post %s fullSem failed, reason:%s\n", pMod->name, strerror(errno));

  return 0;
}

void taosCleanUpModule(module_t *pMod) {
  int i;

  if (pMod->cleanUp) pMod->cleanUp();

  if (taosCheckPthreadValid(pMod->thread)) {
    pthread_cancel(pMod->thread);
    pthread_join(pMod->thread, NULL);
  }

  taosResetPthread(&pMod->thread);
  sem_destroy(&pMod->emptySem);
  sem_destroy(&pMod->fullSem);
  pthread_mutex_destroy(&pMod->queueMutex);
  pthread_mutex_destroy(&pMod->stmMutex);

  for (i = 0; i < pMod->queueSize; ++i) {
    tfree(pMod->queue[i].msg);
  }

  tfree(pMod->queue);

  memset(pMod, 0, sizeof(module_t));
}
