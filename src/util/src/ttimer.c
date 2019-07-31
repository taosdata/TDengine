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

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "os.h"
#include "tidpool.h"
#include "tlog.h"
#include "tsched.h"
#include "ttimer.h"
#include "tutil.h"

// special mempool without mutex
#define mpool_h void *

typedef struct {
  int   numOfFree;  /* number of free slots */
  int   first;      /* the first free slot  */
  int   numOfBlock; /* the number of blocks */
  int   blockSize;  /* block size in bytes  */
  int * freeList;   /* the index list       */
  char *pool;       /* the actual mem block */
} pool_t;

mpool_h tmrMemPoolInit(int maxNum, int blockSize);
char *tmrMemPoolMalloc(mpool_h handle);
void tmrMemPoolFree(mpool_h handle, char *p);
void tmrMemPoolCleanUp(mpool_h handle);

#define tmrError(...)                                 \
  if (tmrDebugFlag & DEBUG_ERROR) {                   \
    tprintf("ERROR TMR ", tmrDebugFlag, __VA_ARGS__); \
  }
#define tmrWarn(...)                                  \
  if (tmrDebugFlag & DEBUG_WARN) {                    \
    tprintf("WARN  TMR ", tmrDebugFlag, __VA_ARGS__); \
  }
#define tmrTrace(...)                           \
  if (tmrDebugFlag & DEBUG_TRACE) {             \
    tprintf("TMR ", tmrDebugFlag, __VA_ARGS__); \
  }

#define maxNumOfTmrCtrl 512
#define MSECONDS_PER_TICK 5

typedef struct _tmr_obj {
  void *param1;
  void (*fp)(void *, void *);
  tmr_h               timerId;
  short               cycle;
  struct _tmr_obj *   prev;
  struct _tmr_obj *   next;
  int                 index;
  struct _tmr_ctrl_t *pCtrl;
} tmr_obj_t;

typedef struct {
  tmr_obj_t *head;
  int        count;
} tmr_list_t;

typedef struct _tmr_ctrl_t {
  void *          signature;
  pthread_mutex_t mutex;            /* mutex to protect critical resource */
  int             resolution;       /* resolution in mseconds */
  int             numOfPeriods;     /* total number of periods */
  int64_t         periodsFromStart; /* count number of periods since start */
  pthread_t       thread;           /* timer thread ID */
  tmr_list_t *    tmrList;
  mpool_h         poolHandle;
  char            label[12];
  int             maxNumOfTmrs;
  int             numOfTmrs;
  int             ticks;
  int             maxTicks;
  int             tmrCtrlId;
} tmr_ctrl_t;

int  tmrDebugFlag = DEBUG_ERROR | DEBUG_WARN | DEBUG_FILE;
void taosTmrProcessList(tmr_ctrl_t *);

tmr_ctrl_t tmrCtrl[maxNumOfTmrCtrl];
int        numOfTmrCtrl = 0;
void *     tmrIdPool = NULL;
void *     tmrQhandle;
int        taosTmrThreads = 1;

void *taosTimerLoopFunc(int signo) {
  tmr_ctrl_t *pCtrl;
  int         count = 0;

  for (int i = 1; i < maxNumOfTmrCtrl; ++i) {
    pCtrl = tmrCtrl + i;
    if (pCtrl->signature) {
      count++;
      pCtrl->ticks++;
      if (pCtrl->ticks >= pCtrl->maxTicks) {
        taosTmrProcessList(pCtrl);
        pCtrl->ticks = 0;
      }
      if (count >= numOfTmrCtrl) break;
    }
  }

  return NULL;
}

#ifndef WINDOWS
void *taosProcessAlarmSignal(void *tharg) {
  // Block the signal
  sigset_t sigset;
  sigemptyset(&sigset);
  sigaddset(&sigset, SIGALRM);
  sigprocmask(SIG_BLOCK, &sigset, NULL);

  timer_t         timerId;
  struct sigevent sevent;
  sevent.sigev_notify = SIGEV_THREAD_ID;
  sevent._sigev_un._tid = syscall(__NR_gettid);
  sevent.sigev_signo = SIGALRM;

  if (timer_create(CLOCK_REALTIME, &sevent, &timerId) == -1) {
    tmrError("Failed to create timer");
  }

  struct itimerspec ts;
  ts.it_value.tv_sec = 0;
  ts.it_value.tv_nsec = 1000000 * MSECONDS_PER_TICK;
  ts.it_interval.tv_sec = 0;
  ts.it_interval.tv_nsec = 1000000 * MSECONDS_PER_TICK;

  if (timer_settime(timerId, 0, &ts, NULL)) {
    tmrError("Failed to init timer");
    return NULL;
  }

  int signo;
  while (1) {
    if (sigwait(&sigset, &signo)) {
      tmrError("Failed to wait signal: number %d", signo);
      continue;
    }
    /* printf("Signal handling: number %d ......\n", signo); */

    taosTimerLoopFunc(0);
  }

  assert(0);
  return NULL;
}
#endif

void taosTmrModuleInit(void) {
  tmrIdPool = taosInitIdPool(maxNumOfTmrCtrl);
  memset(tmrCtrl, 0, sizeof(tmrCtrl));

#ifdef LINUX
  pthread_t      thread;
  pthread_attr_t tattr;
  pthread_attr_init(&tattr);
  pthread_attr_setdetachstate(&tattr, PTHREAD_CREATE_DETACHED);
  if (pthread_create(&thread, &tattr, taosProcessAlarmSignal, NULL) != 0) {
    tmrError("failed to create timer thread");
    return;
  }

  pthread_attr_destroy(&tattr);
#else
  taosInitTimer(taosTimerLoopFunc, MSECONDS_PER_TICK);
#endif

  tmrQhandle = taosInitScheduler(10000, taosTmrThreads, "tmr");
  tmrTrace("timer module is initialized, thread:%d", taosTmrThreads);
}

void *taosTmrInit(int maxNumOfTmrs, int resolution, int longest, char *label) {
  static pthread_once_t tmrInit = PTHREAD_ONCE_INIT;
  tmr_ctrl_t *          pCtrl;

  pthread_once(&tmrInit, taosTmrModuleInit);

  int tmrCtrlId = taosAllocateId(tmrIdPool);

  if (tmrCtrlId < 0) {
    tmrError("%s bug!!! too many timers!!!", label);
    return NULL;
  }

  pCtrl = tmrCtrl + tmrCtrlId;
  tfree(pCtrl->tmrList);
  tmrMemPoolCleanUp(pCtrl->poolHandle);

  memset(pCtrl, 0, sizeof(tmr_ctrl_t));

  pCtrl->tmrCtrlId = tmrCtrlId;
  strcpy(pCtrl->label, label);
  pCtrl->maxNumOfTmrs = maxNumOfTmrs;

  if ((pCtrl->poolHandle = tmrMemPoolInit(maxNumOfTmrs + 10, sizeof(tmr_obj_t))) == NULL) {
    tmrError("%s failed to allocate mem pool", label);
    tmrMemPoolCleanUp(pCtrl->poolHandle);
    return NULL;
  }

  if (resolution < MSECONDS_PER_TICK) resolution = MSECONDS_PER_TICK;
  pCtrl->resolution = resolution;
  pCtrl->maxTicks = resolution / MSECONDS_PER_TICK;
  pCtrl->ticks = rand() / pCtrl->maxTicks;
  pCtrl->numOfPeriods = longest / resolution;
  if (pCtrl->numOfPeriods < 10) pCtrl->numOfPeriods = 10;

  pCtrl->tmrList = (tmr_list_t *)malloc(sizeof(tmr_list_t) * pCtrl->numOfPeriods);
  for (int i = 0; i < pCtrl->numOfPeriods; i++) {
    pCtrl->tmrList[i].head = NULL;
    pCtrl->tmrList[i].count = 0;
  }

  if (pthread_mutex_init(&pCtrl->mutex, NULL) < 0) {
    tmrError("%s failed to create the mutex, reason:%s", label, strerror(errno));
    taosTmrCleanUp(pCtrl);
    return NULL;
  }

  pCtrl->signature = pCtrl;
  numOfTmrCtrl++;
  tmrTrace("%s timer ctrl is initialized, index:%d", label, tmrCtrlId);
  return pCtrl;
}

void taosTmrProcessList(tmr_ctrl_t *pCtrl) {
  unsigned int index;
  tmr_list_t * pList;
  tmr_obj_t *  pObj, *header;

  pthread_mutex_lock(&pCtrl->mutex);
  index = pCtrl->periodsFromStart % pCtrl->numOfPeriods;
  pList = &pCtrl->tmrList[index];

  while (1) {
    header = pList->head;
    if (header == NULL) break;

    if (header->cycle > 0) {
      pObj = header;
      while (pObj) {
        pObj->cycle--;
        pObj = pObj->next;
      }
      break;
    }

    pCtrl->numOfTmrs--;
    tmrTrace("%s %p, timer expired, fp:%p, tmr_h:%p, index:%d, total:%d", pCtrl->label, header->param1, header->fp,
             header, index, pCtrl->numOfTmrs);

    pList->head = header->next;
    if (header->next) header->next->prev = NULL;
    pList->count--;
    header->timerId = NULL;

    SSchedMsg schedMsg;
    schedMsg.fp = NULL;
    schedMsg.tfp = header->fp;
    schedMsg.ahandle = header->param1;
    schedMsg.thandle = header;
    taosScheduleTask(tmrQhandle, &schedMsg);

    tmrMemPoolFree(pCtrl->poolHandle, (char *)header);
  }

  pCtrl->periodsFromStart++;
  pthread_mutex_unlock(&pCtrl->mutex);
}

void taosTmrCleanUp(void *handle) {
  tmr_ctrl_t *pCtrl = (tmr_ctrl_t *)handle;
  if (pCtrl == NULL || pCtrl->signature != pCtrl) return;

  pCtrl->signature = NULL;
  taosFreeId(tmrIdPool, pCtrl->tmrCtrlId);
  numOfTmrCtrl--;
  tmrTrace("%s is cleaned up, numOfTmrs:%d", pCtrl->label, numOfTmrCtrl);
}

tmr_h taosTmrStart(void (*fp)(void *, void *), int mseconds, void *param1, void *handle) {
  tmr_obj_t * pObj, *cNode, *pNode;
  tmr_list_t *pList;
  int         index, period;
  tmr_ctrl_t *pCtrl = (tmr_ctrl_t *)handle;

  if (handle == NULL) return NULL;

  period = mseconds / pCtrl->resolution;
  if (pthread_mutex_lock(&pCtrl->mutex) != 0)
    tmrError("%s mutex lock failed, reason:%s", pCtrl->label, strerror(errno));

  pObj = (tmr_obj_t *)tmrMemPoolMalloc(pCtrl->poolHandle);
  if (pObj == NULL) {
    tmrError("%s reach max number of timers:%d", pCtrl->label, pCtrl->maxNumOfTmrs);
    pthread_mutex_unlock(&pCtrl->mutex);
    return NULL;
  }

  pObj->cycle = period / pCtrl->numOfPeriods;
  pObj->param1 = param1;
  pObj->fp = fp;
  pObj->timerId = pObj;
  pObj->pCtrl = pCtrl;

  index = (period + pCtrl->periodsFromStart) % pCtrl->numOfPeriods;
  int cindex = (pCtrl->periodsFromStart) % pCtrl->numOfPeriods;
  pList = &(pCtrl->tmrList[index]);

  pObj->index = index;
  cNode = pList->head;
  pNode = NULL;

  while (cNode != NULL) {
    if (cNode->cycle < pObj->cycle) {
      pNode = cNode;
      cNode = cNode->next;
    } else {
      break;
    }
  }

  pObj->next = cNode;
  pObj->prev = pNode;

  if (cNode != NULL) {
    cNode->prev = pObj;
  }

  if (pNode != NULL) {
    pNode->next = pObj;
  } else {
    pList->head = pObj;
  }

  pList->count++;
  pCtrl->numOfTmrs++;

  if (pthread_mutex_unlock(&pCtrl->mutex) != 0)
    tmrError("%s mutex unlock failed, reason:%s", pCtrl->label, strerror(errno));

  tmrTrace("%s %p, timer started, fp:%p, tmr_h:%p, index:%d, total:%d cindex:%d", pCtrl->label, param1, fp, pObj, index,
           pCtrl->numOfTmrs, cindex);

  return (tmr_h)pObj;
}

void taosTmrStopA(tmr_h *timerId) {
  tmr_obj_t * pObj;
  tmr_list_t *pList;
  tmr_ctrl_t *pCtrl;

  pObj = *(tmr_obj_t **)timerId;
  if (pObj == NULL) return;

  pCtrl = pObj->pCtrl;
  if (pCtrl == NULL) return;

  if (pthread_mutex_lock(&pCtrl->mutex) != 0)
    tmrError("%s mutex lock failed, reason:%s", pCtrl->label, strerror(errno));

  if (pObj->timerId == pObj) {
    pList = &(pCtrl->tmrList[pObj->index]);
    if (pObj->prev) {
      pObj->prev->next = pObj->next;
    } else {
      pList->head = pObj->next;
    }

    if (pObj->next) {
      pObj->next->prev = pObj->prev;
    }

    pList->count--;
    pObj->timerId = NULL;
    pCtrl->numOfTmrs--;

    tmrTrace("%s %p, timer stopped atomiclly, fp:%p, tmr_h:%p, total:%d", pCtrl->label, pObj->param1, pObj->fp, pObj,
             pCtrl->numOfTmrs);
    tmrMemPoolFree(pCtrl->poolHandle, (char *)(pObj));

    *(tmr_obj_t **)timerId = NULL;
  } else {
    tmrTrace("%s %p, timer stopped atomiclly, fp:%p, tmr_h:%p, total:%d", pCtrl->label, pObj->param1, pObj->fp, pObj,
             pCtrl->numOfTmrs);
  }

  pthread_mutex_unlock(&pCtrl->mutex);
}

void taosTmrReset(void (*fp)(void *, void *), int mseconds, void *param1, void *handle, tmr_h *pTmrId) {
  tmr_obj_t * pObj, *cNode, *pNode;
  tmr_list_t *pList;
  int         index, period;
  tmr_ctrl_t *pCtrl = (tmr_ctrl_t *)handle;

  if (handle == NULL) return;
  if (pTmrId == NULL) return;

  period = mseconds / pCtrl->resolution;
  if (pthread_mutex_lock(&pCtrl->mutex) != 0)
    tmrError("%s mutex lock failed, reason:%s", pCtrl->label, strerror(errno));

  pObj = (tmr_obj_t *)(*pTmrId);

  if (pObj && pObj->timerId == *pTmrId) {
    // exist, stop it first
    pList = &(pCtrl->tmrList[pObj->index]);
    if (pObj->prev) {
      pObj->prev->next = pObj->next;
    } else {
      pList->head = pObj->next;
    }

    if (pObj->next) {
      pObj->next->prev = pObj->prev;
    }

    pList->count--;
    pObj->timerId = NULL;
    pCtrl->numOfTmrs--;
  } else {
    // timer not there, or already expired
    pObj = (tmr_obj_t *)tmrMemPoolMalloc(pCtrl->poolHandle);
    *pTmrId = pObj;

    if (pObj == NULL) {
      tmrError("%s failed to allocate timer, max:%d allocated:%d", pCtrl->label, pCtrl->maxNumOfTmrs, pCtrl->numOfTmrs);
      pthread_mutex_unlock(&pCtrl->mutex);
      return;
    }
  }

  pObj->cycle = period / pCtrl->numOfPeriods;
  pObj->param1 = param1;
  pObj->fp = fp;
  pObj->timerId = pObj;
  pObj->pCtrl = pCtrl;

  index = (period + pCtrl->periodsFromStart) % pCtrl->numOfPeriods;
  pList = &(pCtrl->tmrList[index]);

  pObj->index = index;
  cNode = pList->head;
  pNode = NULL;

  while (cNode != NULL) {
    if (cNode->cycle < pObj->cycle) {
      pNode = cNode;
      cNode = cNode->next;
    } else {
      break;
    }
  }

  pObj->next = cNode;
  pObj->prev = pNode;

  if (cNode != NULL) {
    cNode->prev = pObj;
  }

  if (pNode != NULL) {
    pNode->next = pObj;
  } else {
    pList->head = pObj;
  }

  pList->count++;
  pCtrl->numOfTmrs++;

  if (pthread_mutex_unlock(&pCtrl->mutex) != 0)
    tmrError("%s mutex unlock failed, reason:%s", pCtrl->label, strerror(errno));

  tmrTrace("%s %p, timer is reset, fp:%p, tmr_h:%p, index:%d, total:%d numOfFree:%d", pCtrl->label, param1, fp, pObj,
           index, pCtrl->numOfTmrs, ((pool_t *)pCtrl->poolHandle)->numOfFree);

  return;
}

void taosTmrList(void *handle) {
  int         i;
  tmr_list_t *pList;
  tmr_obj_t * pObj;
  tmr_ctrl_t *pCtrl = (tmr_ctrl_t *)handle;

  for (i = 0; i < pCtrl->numOfPeriods; ++i) {
    pList = &(pCtrl->tmrList[i]);
    pObj = pList->head;
    if (!pObj) continue;
    printf("\nindex=%d count:%d\n", i, pList->count);
    while (pObj) {
      pObj = pObj->next;
    }
  }
}

mpool_h tmrMemPoolInit(int numOfBlock, int blockSize) {
  int     i;
  pool_t *pool_p;

  if (numOfBlock <= 1 || blockSize <= 1) {
    tmrError("invalid parameter in memPoolInit\n");
    return NULL;
  }

  pool_p = (pool_t *)malloc(sizeof(pool_t));
  if (pool_p == NULL) {
    tmrError("mempool malloc failed\n");
    return NULL;
  } else {
    memset(pool_p, 0, sizeof(pool_t));
  }

  pool_p->blockSize = blockSize;
  pool_p->numOfBlock = numOfBlock;
  pool_p->pool = (char *)malloc(blockSize * numOfBlock);
  memset(pool_p->pool, 0, blockSize * numOfBlock);
  pool_p->freeList = (int *)malloc(sizeof(int) * numOfBlock);

  if (pool_p->pool == NULL || pool_p->freeList == NULL) {
    tmrError("failed to allocate memory\n");
    free(pool_p->freeList);
    free(pool_p->pool);
    free(pool_p);
    return NULL;
  }

  for (i = 0; i < pool_p->numOfBlock; ++i) pool_p->freeList[i] = i;

  pool_p->first = 0;
  pool_p->numOfFree = pool_p->numOfBlock;

  return (mpool_h)pool_p;
}

char *tmrMemPoolMalloc(mpool_h handle) {
  char *  pos = NULL;
  pool_t *pool_p = (pool_t *)handle;

  if (pool_p->numOfFree <= 0 || pool_p->numOfFree > pool_p->numOfBlock) {
    tmrError("mempool: out of memory, numOfFree:%d, numOfBlock:%d", pool_p->numOfFree, pool_p->numOfBlock);
  } else {
    pos = pool_p->pool + pool_p->blockSize * (pool_p->freeList[pool_p->first]);
    pool_p->first++;
    pool_p->first = pool_p->first % pool_p->numOfBlock;
    pool_p->numOfFree--;
  }

  return pos;
}

void tmrMemPoolFree(mpool_h handle, char *pMem) {
  int     index;
  pool_t *pool_p = (pool_t *)handle;

  if (pMem == NULL) return;

  index = (int)(pMem - pool_p->pool) / pool_p->blockSize;

  if (index < 0 || index >= pool_p->numOfBlock) {
    tmrError("tmr mempool: error, invalid address:%p\n", pMem);
  } else {
    memset(pMem, 0, pool_p->blockSize);
    pool_p->freeList[(pool_p->first + pool_p->numOfFree) % pool_p->numOfBlock] = index;
    pool_p->numOfFree++;
  }
}

void tmrMemPoolCleanUp(mpool_h handle) {
  pool_t *pool_p = (pool_t *)handle;
  if (pool_p == NULL) return;

  if (pool_p->pool) free(pool_p->pool);
  if (pool_p->freeList) free(pool_p->freeList);
  memset(&pool_p, 0, sizeof(pool_p));
  free(pool_p);
}
