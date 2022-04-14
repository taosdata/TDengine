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

#include "vnodeInt.h"

SVnodeMgr vnodeMgr = {.vnodeInitFlag = TD_MOD_UNINITIALIZED};

static void* loop(void* arg);

int vnodeInit() {
  if (TD_CHECK_AND_SET_MODE_INIT(&(vnodeMgr.vnodeInitFlag)) == TD_MOD_INITIALIZED) {
    return 0;
  }

  vnodeMgr.stop = false;

  // Start commit handers
  vnodeMgr.nthreads = tsNumOfCommitThreads;
  vnodeMgr.threads = taosMemoryCalloc(vnodeMgr.nthreads, sizeof(TdThread));
  if (vnodeMgr.threads == NULL) {
    return -1;
  }

  taosThreadMutexInit(&(vnodeMgr.mutex), NULL);
  taosThreadCondInit(&(vnodeMgr.hasTask), NULL);
  TD_DLIST_INIT(&(vnodeMgr.queue));

  for (uint16_t i = 0; i < vnodeMgr.nthreads; i++) {
    taosThreadCreate(&(vnodeMgr.threads[i]), NULL, loop, NULL);
    // pthread_setname_np(vnodeMgr.threads[i], "VND Commit Thread");
  }

  if (walInit() < 0) {
    return -1;
  }

  return 0;
}

void vnodeCleanup() {
  if (TD_CHECK_AND_SET_MOD_CLEAR(&(vnodeMgr.vnodeInitFlag)) == TD_MOD_UNINITIALIZED) {
    return;
  }

  // Stop commit handler
  taosThreadMutexLock(&(vnodeMgr.mutex));
  vnodeMgr.stop = true;
  taosThreadCondBroadcast(&(vnodeMgr.hasTask));
  taosThreadMutexUnlock(&(vnodeMgr.mutex));

  for (uint16_t i = 0; i < vnodeMgr.nthreads; i++) {
    taosThreadJoin(vnodeMgr.threads[i], NULL);
  }

  taosMemoryFreeClear(vnodeMgr.threads);
  taosThreadCondDestroy(&(vnodeMgr.hasTask));
  taosThreadMutexDestroy(&(vnodeMgr.mutex));
}

int vnodeScheduleTask(SVnodeTask* pTask) {
  taosThreadMutexLock(&(vnodeMgr.mutex));

  TD_DLIST_APPEND(&(vnodeMgr.queue), pTask);

  taosThreadCondSignal(&(vnodeMgr.hasTask));

  taosThreadMutexUnlock(&(vnodeMgr.mutex));

  return 0;
}

/* ------------------------ STATIC METHODS ------------------------ */
static void* loop(void* arg) {
  setThreadName("vnode-commit");

  SVnodeTask* pTask;
  for (;;) {
    taosThreadMutexLock(&(vnodeMgr.mutex));
    for (;;) {
      pTask = TD_DLIST_HEAD(&(vnodeMgr.queue));
      if (pTask == NULL) {
        if (vnodeMgr.stop) {
          taosThreadMutexUnlock(&(vnodeMgr.mutex));
          return NULL;
        } else {
          taosThreadCondWait(&(vnodeMgr.hasTask), &(vnodeMgr.mutex));
        }
      } else {
        TD_DLIST_POP(&(vnodeMgr.queue), pTask);
        break;
      }
    }

    taosThreadMutexUnlock(&(vnodeMgr.mutex));

    (*(pTask->execute))(pTask->arg);
    taosMemoryFree(pTask);
  }

  return NULL;
}