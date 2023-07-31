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

#include "vnd.h"

typedef struct SVnodeTask SVnodeTask;
struct SVnodeTask {
  SVnodeTask* next;
  SVnodeTask* prev;
  int (*execute)(void*);
  void* arg;
};

typedef struct {
  int           nthreads;
  TdThread*     threads;
  TdThreadMutex mutex;
  TdThreadCond  hasTask;
  SVnodeTask    queue;
} SVnodeThreadPool;

struct SVnodeGlobal {
  int8_t           init;
  int8_t           stop;
  SVnodeThreadPool tp[2];
};

struct SVnodeGlobal vnodeGlobal;

static void* loop(void* arg);

int vnodeInit(int nthreads) {
  int8_t init;
  int    ret;

  init = atomic_val_compare_exchange_8(&(vnodeGlobal.init), 0, 1);
  if (init) {
    return 0;
  }
  vnodeGlobal.stop = 0;

  for (int32_t i = 0; i < ARRAY_SIZE(vnodeGlobal.tp); i++) {
    taosThreadMutexInit(&vnodeGlobal.tp[i].mutex, NULL);
    taosThreadCondInit(&vnodeGlobal.tp[i].hasTask, NULL);

    taosThreadMutexLock(&vnodeGlobal.tp[i].mutex);

    vnodeGlobal.tp[i].queue.next = &vnodeGlobal.tp[i].queue;
    vnodeGlobal.tp[i].queue.prev = &vnodeGlobal.tp[i].queue;

    taosThreadMutexUnlock(&(vnodeGlobal.tp[i].mutex));

    vnodeGlobal.tp[i].nthreads = nthreads;
    vnodeGlobal.tp[i].threads = taosMemoryCalloc(nthreads, sizeof(TdThread));
    if (vnodeGlobal.tp[i].threads == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      vError("failed to init vnode module since:%s", tstrerror(terrno));
      return -1;
    }

    for (int j = 0; j < nthreads; j++) {
      taosThreadCreate(&(vnodeGlobal.tp[i].threads[j]), NULL, loop, &vnodeGlobal.tp[i]);
    }
  }

  if (walInit() < 0) {
    return -1;
  }
  if (tqInit() < 0) {
    return -1;
  }

  return 0;
}

void vnodeCleanup() {
  int8_t init;

  init = atomic_val_compare_exchange_8(&(vnodeGlobal.init), 1, 0);
  if (init == 0) return;

  // set stop
  vnodeGlobal.stop = 1;
  for (int32_t i = 0; i < ARRAY_SIZE(vnodeGlobal.tp); i++) {
    taosThreadMutexLock(&(vnodeGlobal.tp[i].mutex));
    taosThreadCondBroadcast(&(vnodeGlobal.tp[i].hasTask));
    taosThreadMutexUnlock(&(vnodeGlobal.tp[i].mutex));

    // wait for threads
    for (int j = 0; j < vnodeGlobal.tp[i].nthreads; j++) {
      taosThreadJoin(vnodeGlobal.tp[i].threads[j], NULL);
    }

    // clear source
    taosMemoryFreeClear(vnodeGlobal.tp[i].threads);
    taosThreadCondDestroy(&(vnodeGlobal.tp[i].hasTask));
    taosThreadMutexDestroy(&(vnodeGlobal.tp[i].mutex));
  }

  walCleanUp();
  tqCleanUp();
  smaCleanUp();
}

int vnodeScheduleTaskEx(int tpid, int (*execute)(void*), void* arg) {
  SVnodeTask* pTask;

  ASSERT(!vnodeGlobal.stop);

  pTask = taosMemoryMalloc(sizeof(*pTask));
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pTask->execute = execute;
  pTask->arg = arg;

  taosThreadMutexLock(&(vnodeGlobal.tp[tpid].mutex));
  pTask->next = &vnodeGlobal.tp[tpid].queue;
  pTask->prev = vnodeGlobal.tp[tpid].queue.prev;
  vnodeGlobal.tp[tpid].queue.prev->next = pTask;
  vnodeGlobal.tp[tpid].queue.prev = pTask;
  taosThreadCondSignal(&(vnodeGlobal.tp[tpid].hasTask));
  taosThreadMutexUnlock(&(vnodeGlobal.tp[tpid].mutex));

  return 0;
}

int vnodeScheduleTask(int (*execute)(void*), void* arg) { return vnodeScheduleTaskEx(0, execute, arg); }

/* ------------------------ STATIC METHODS ------------------------ */
static void* loop(void* arg) {
  SVnodeThreadPool* tp = (SVnodeThreadPool*)arg;
  SVnodeTask*       pTask;
  int               ret;

  if (tp == &vnodeGlobal.tp[0]) {
    setThreadName("vnode-commit");
  } else if (tp == &vnodeGlobal.tp[1]) {
    setThreadName("vnode-merge");
  }

  for (;;) {
    taosThreadMutexLock(&(tp->mutex));
    for (;;) {
      pTask = tp->queue.next;
      if (pTask == &tp->queue) {
        // no task
        if (vnodeGlobal.stop) {
          taosThreadMutexUnlock(&(tp->mutex));
          return NULL;
        } else {
          taosThreadCondWait(&(tp->hasTask), &(tp->mutex));
        }
      } else {
        // has task
        pTask->prev->next = pTask->next;
        pTask->next->prev = pTask->prev;
        break;
      }
    }

    taosThreadMutexUnlock(&(tp->mutex));

    pTask->execute(pTask->arg);
    taosMemoryFree(pTask);
  }

  return NULL;
}
