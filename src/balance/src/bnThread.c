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
#include "os.h"
#include "tutil.h"
#include "tbalance.h"
#include "tsync.h"
#include "tsync.h"
#include "ttimer.h"
#include "tglobal.h"
#include "tdataformat.h"
#include "dnode.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeDnode.h"
#include "mnodeDb.h"
#include "mnodeMnode.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"

typedef struct {
  bool            stop;
  pthread_mutex_t mutex;
  pthread_cond_t  cond;
  pthread_t       thread;
} SBalanceThread;

static SBalanceThread tsBnThread;

static void *bnThreadFunc(void *arg) {
  while (1) {
    pthread_mutex_lock(&tsBnThread.mutex);
    if (tsBnThread.stop) {
      pthread_mutex_unlock(&tsBnThread.mutex);
      break;
    }

    pthread_cond_wait(&tsBnThread.cond, &tsBnThread.mutex);

    pthread_mutex_unlock(&(tsBnThread.mutex));
  }

  return NULL;
}

int32_t bnThreadInit() {
  tsBnThread.stop = false;
  pthread_mutex_init(&tsBnThread.mutex, NULL);
  pthread_cond_init(&tsBnThread.cond, NULL);

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  int32_t ret = pthread_create(&tsBnThread.thread, &thattr, bnThreadFunc, NULL);
  pthread_attr_destroy(&thattr);

  if (ret != 0) {
    mError("failed to create balance thread since %s", strerror(errno));
    return -1;
  }

  mDebug("balance thread is created");
  return 0;
}

void bnThreadCleanup() {
  mDebug("balance thread will be cleanup");

  pthread_mutex_lock(&tsBnThread.mutex);
  tsBnThread.stop = true;
  pthread_cond_signal(&tsBnThread.cond);
  pthread_mutex_unlock(&(tsBnThread.mutex));
  pthread_join(tsBnThread.thread, NULL);

  pthread_cond_destroy(&tsBnThread.cond);
  pthread_mutex_destroy(&tsBnThread.mutex);
}

void bnThreadSyncNotify() {
  mDebug("balance thread sync notify");
  pthread_mutex_lock(&tsBnThread.mutex);
  pthread_cond_signal(&tsBnThread.cond);
  pthread_mutex_unlock(&(tsBnThread.mutex));
}

void bnThreadAsyncNotify() {
  mDebug("balance thread async notify");
  pthread_mutex_lock(&tsBnThread.mutex);
  pthread_cond_signal(&tsBnThread.cond);
  pthread_mutex_unlock(&(tsBnThread.mutex));
}
