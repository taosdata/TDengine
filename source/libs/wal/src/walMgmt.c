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
#include "taoserror.h"
#include "tref.h"
#include "tfile.h"
#include "walInt.h"

typedef struct {
  int32_t   refSetId;
  int32_t   seq;
  int8_t    stop;
  pthread_t thread;
  pthread_mutex_t mutex;
} SWalMgmt;

static SWalMgmt tsWal = {0};
static int32_t  walCreateThread();
static void     walStopThread();
static int32_t  walInitObj(SWal *pWal);
static void     walFreeObj(void *pWal);

int32_t walInit() {
  int32_t code = 0;
  tsWal.refSetId = taosOpenRef(TSDB_MIN_VNODES, walFreeObj);

  code = pthread_mutex_init(&tsWal.mutex, NULL);
  if (code) {
    wError("failed to init wal mutex since %s", tstrerror(code));
    return code;
  }

  code = walCreateThread();
  if (code != 0) {
    wError("failed to init wal module since %s", tstrerror(code));
    return code;
  }

  wInfo("wal module is initialized, rsetId:%d", tsWal.refSetId);
  return code;
}

void walCleanUp() {
  walStopThread();
  taosCloseRef(tsWal.refSetId);
  pthread_mutex_destroy(&tsWal.mutex);
  wInfo("wal module is cleaned up");
}

SWal *walOpen(const char *path, SWalCfg *pCfg) {
  SWal *pWal = malloc(sizeof(SWal));
  if (pWal == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  pWal->vgId = pCfg->vgId;
  pWal->curLogTfd = -1;
  /*pWal->curFileId = -1;*/
  pWal->level = pCfg->walLevel;
  pWal->fsyncPeriod = pCfg->fsyncPeriod;
  tstrncpy(pWal->path, path, sizeof(pWal->path));
  pthread_mutex_init(&pWal->mutex, NULL);

  pWal->fsyncSeq = pCfg->fsyncPeriod / 1000;
  if (pWal->fsyncSeq <= 0) pWal->fsyncSeq = 1;

  if (walInitObj(pWal) != 0) {
    walFreeObj(pWal);
    return NULL;
  }

   pWal->refId = taosAddRef(tsWal.refSetId, pWal);
   if (pWal->refId < 0) {
    walFreeObj(pWal);
    return NULL;
  }

  wDebug("vgId:%d, wal:%p is opened, level:%d fsyncPeriod:%d", pWal->vgId, pWal, pWal->level, pWal->fsyncPeriod);

  return pWal;
}

int32_t walAlter(SWal *pWal, SWalCfg *pCfg) {
  if (pWal == NULL) return TSDB_CODE_WAL_APP_ERROR;

  if (pWal->level == pCfg->walLevel && pWal->fsyncPeriod == pCfg->fsyncPeriod) {
    wDebug("vgId:%d, old walLevel:%d fsync:%d, new walLevel:%d fsync:%d not change", pWal->vgId, pWal->level,
           pWal->fsyncPeriod, pCfg->walLevel, pCfg->fsyncPeriod);
    return 0;
  }

  wInfo("vgId:%d, change old walLevel:%d fsync:%d, new walLevel:%d fsync:%d", pWal->vgId, pWal->level,
        pWal->fsyncPeriod, pCfg->walLevel, pCfg->fsyncPeriod);

  pWal->level = pCfg->walLevel;
  pWal->fsyncPeriod = pCfg->fsyncPeriod;
  pWal->fsyncSeq = pCfg->fsyncPeriod / 1000;
  if (pWal->fsyncSeq <= 0) pWal->fsyncSeq = 1;

  return 0;
}

void walClose(SWal *pWal) {
  if (pWal == NULL) return;

  pthread_mutex_lock(&pWal->mutex);
  tfClose(pWal->curLogTfd);
  pthread_mutex_unlock(&pWal->mutex);
  taosRemoveRef(tsWal.refSetId, pWal->refId);
}

static int32_t walInitObj(SWal *pWal) {
  if (taosMkDir(pWal->path) != 0) {
    wError("vgId:%d, path:%s, failed to create directory since %s", pWal->vgId, pWal->path, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  wDebug("vgId:%d, object is initialized", pWal->vgId);
  return 0;
}

static void walFreeObj(void *wal) {
  SWal *pWal = wal;
  wDebug("vgId:%d, wal:%p is freed", pWal->vgId, pWal);

  tfClose(pWal->curLogTfd);
  pthread_mutex_destroy(&pWal->mutex);
  tfree(pWal);
}

static bool walNeedFsync(SWal *pWal) {
  if (pWal->fsyncPeriod <= 0 || pWal->level != TAOS_WAL_FSYNC) {
    return false;
  }

  if (tsWal.seq % pWal->fsyncSeq == 0) {
    return true;
  }

  return false;
}

static void walUpdateSeq() {
  taosMsleep(WAL_REFRESH_MS);
  if (++tsWal.seq <= 0) {
    tsWal.seq = 1;
  }
}

static void walFsyncAll() {
  SWal *pWal = taosIterateRef(tsWal.refSetId, 0);
  while (pWal) {
    if (walNeedFsync(pWal)) {
      wTrace("vgId:%d, do fsync, level:%d seq:%d rseq:%d", pWal->vgId, pWal->level, pWal->fsyncSeq, tsWal.seq);
      int32_t code = tfFsync(pWal->curLogTfd);
      if (code != 0) {
        wError("vgId:%d, file:%"PRId64".log, failed to fsync since %s", pWal->vgId, pWal->curFileFirstVersion, strerror(code));
      }
    }
    pWal = taosIterateRef(tsWal.refSetId, pWal->refId);
  }
}

static void *walThreadFunc(void *param) {
  int stop = 0;
  setThreadName("wal");
  while (1) {
    walUpdateSeq();
    walFsyncAll();

    pthread_mutex_lock(&tsWal.mutex);
    stop = tsWal.stop;
    pthread_mutex_unlock(&tsWal.mutex);
    if (stop) break;
  }

  return NULL;
}

static int32_t walCreateThread() {
  pthread_attr_t thAttr;
  pthread_attr_init(&thAttr);
  pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (pthread_create(&tsWal.thread, &thAttr, walThreadFunc, NULL) != 0) {
    wError("failed to create wal thread since %s", strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  pthread_attr_destroy(&thAttr);
  wDebug("wal thread is launched, thread:0x%08" PRIx64, taosGetPthreadId(tsWal.thread));

  return 0;
}

static void walStopThread() {
  pthread_mutex_lock(&tsWal.mutex);
  tsWal.stop = 1;
  pthread_mutex_unlock(&tsWal.mutex);

  if (taosCheckPthreadValid(tsWal.thread)) {
    pthread_join(tsWal.thread, NULL);
  }

  wDebug("wal thread is stopped");
}
