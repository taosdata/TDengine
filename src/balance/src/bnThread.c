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
#include "ttimer.h"
#include "tglobal.h"
#include "mnodeSdb.h"
#include "bnThread.h"

static SBnThread tsBnThread;

static void *bnThreadFunc(void *arg) {
  while (1) {
    pthread_mutex_lock(&tsBnThread.mutex);
    if (tsBnThread.stop) {
      pthread_mutex_unlock(&tsBnThread.mutex);
      break;
    }

    pthread_cond_wait(&tsBnThread.cond, &tsBnThread.mutex);
    mDebug("balance thread wakes up to work");
    bool updateSoon = bnStart();
    mDebug("balance thread finished this poll, updateSoon:%d", updateSoon);
    
    bnStartTimer(updateSoon ? 1000 : -1);
    pthread_mutex_unlock(&(tsBnThread.mutex));
  }

  mDebug("balance thread is stopped");
  return NULL;
}

int32_t bnInitThread() {
  memset(&tsBnThread, 0, sizeof(SBnThread));
  tsBnThread.stop = false;
  pthread_mutex_init(&tsBnThread.mutex, NULL);
  pthread_cond_init(&tsBnThread.cond, NULL);

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  int32_t ret = pthread_create(&tsBnThread.thread, &thattr, bnThreadFunc, NULL);
  pthread_attr_destroy(&thattr);

  if (ret != 0) {
    mError("failed to create balance thread since %s", strerror(errno));
    return -1;
  }

  bnStartTimer(2000);
  mDebug("balance thread is created");
  return 0;
}

void bnCleanupThread() {
  mDebug("balance thread will be cleanup");

  if (tsBnThread.timer != NULL) {
    taosTmrStopA(&tsBnThread.timer);
    tsBnThread.timer = NULL;
    mDebug("stop balance timer");
  }

  pthread_mutex_lock(&tsBnThread.mutex);
  tsBnThread.stop = true;
  pthread_cond_signal(&tsBnThread.cond);
  pthread_mutex_unlock(&(tsBnThread.mutex));
  pthread_join(tsBnThread.thread, NULL);

  pthread_cond_destroy(&tsBnThread.cond);
  pthread_mutex_destroy(&tsBnThread.mutex);
}

static void bnPostSignal() {
  if (tsBnThread.stop) return;

  pthread_mutex_lock(&tsBnThread.mutex);
  pthread_cond_signal(&tsBnThread.cond);
  pthread_mutex_unlock(&(tsBnThread.mutex));
}

/*
 * once sdb work as mater, then tsAccessSquence reset to zero
 * increase tsAccessSquence every balance interval
 */

static void bnProcessTimer(void *handle, void *tmrId) {
  if (!sdbIsMaster()) return;
  if (tsBnThread.stop) return;

  tsBnThread.timer = NULL;
  tsAccessSquence++;

  bnStartTimer(-1);
  bnCheckStatus();

  if (handle == NULL) {
    if (tsAccessSquence % tsBalanceInterval == 0) {
      mDebug("balance function is scheduled by timer");
      bnPostSignal();
    }
  } else {
    int64_t mseconds = (int64_t)handle;
    mDebug("balance function is scheduled by event for %" PRId64 " mseconds arrived", mseconds);
    bnPostSignal();
  }
}

void bnStartTimer(int32_t mseconds) {
  if (tsBnThread.stop) return;

  bool updateSoon = (mseconds != -1);
  if (updateSoon) {
    mTrace("balance function will be called after %d ms", mseconds);
    taosTmrReset(bnProcessTimer, mseconds, (void *)(int64_t)mseconds, tsMnodeTmr, &tsBnThread.timer);
  } else {
    taosTmrReset(bnProcessTimer, tsStatusInterval * 1000, NULL, tsMnodeTmr, &tsBnThread.timer);
  }
}

void bnNotify() {
  bnStartTimer(500); 
}
