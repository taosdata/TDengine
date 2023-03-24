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
#include "tcompare.h"
#include "tref.h"
#include "walInt.h"

typedef struct {
  int8_t   stop;
  int8_t   inited;
  uint32_t seq;
  int32_t  refSetId;
  TdThread thread;
} SWalMgmt;

static SWalMgmt tsWal = {0, .seq = 1};
static int32_t  walCreateThread();
static void     walStopThread();
static void     walFreeObj(void *pWal);

int64_t walGetSeq() { return (int64_t)atomic_load_32(&tsWal.seq); }

int32_t walInit() {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(&tsWal.inited, 0, 2);
    if (old != 2) break;
  }

  if (old == 0) {
    tsWal.refSetId = taosOpenRef(TSDB_MIN_VNODES, walFreeObj);

    int32_t code = walCreateThread();
    if (code != 0) {
      wError("failed to init wal module since %s", tstrerror(code));
      atomic_store_8(&tsWal.inited, 0);
      return code;
    }

    wInfo("wal module is initialized, rsetId:%d", tsWal.refSetId);
    atomic_store_8(&tsWal.inited, 1);
  }

  return 0;
}

void walCleanUp() {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(&tsWal.inited, 1, 2);
    if (old != 2) break;
  }

  if (old == 1) {
    walStopThread();
    taosCloseRef(tsWal.refSetId);
    wInfo("wal module is cleaned up");
    atomic_store_8(&tsWal.inited, 0);
  }
}

SWal *walOpen(const char *path, SWalCfg *pCfg) {
  SWal *pWal = taosMemoryCalloc(1, sizeof(SWal));
  if (pWal == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  if (taosThreadMutexInit(&pWal->mutex, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosMemoryFree(pWal);
    return NULL;
  }

  // set config
  memcpy(&pWal->cfg, pCfg, sizeof(SWalCfg));

  pWal->fsyncSeq = pCfg->fsyncPeriod / 1000;
  if (pWal->cfg.retentionSize > 0) {
    pWal->cfg.retentionSize *= 1024;
  }

  if (pWal->cfg.segSize > 0) {
    pWal->cfg.segSize *= 1024;
  }

  if (pWal->fsyncSeq <= 0) pWal->fsyncSeq = 1;

  tstrncpy(pWal->path, path, sizeof(pWal->path));
  if (taosMkDir(pWal->path) != 0) {
    wError("vgId:%d, path:%s, failed to create directory since %s", pWal->cfg.vgId, pWal->path, strerror(errno));
    goto _err;
  }

  // init ref
  pWal->pRefHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  if (pWal->pRefHash == NULL) {
    wError("failed to init hash since %s", tstrerror(terrno));
    goto _err;
  }

  // open meta
  walResetVer(&pWal->vers);
  pWal->pLogFile = NULL;
  pWal->pIdxFile = NULL;
  pWal->writeCur = -1;
  pWal->fileInfoSet = taosArrayInit(8, sizeof(SWalFileInfo));
  if (pWal->fileInfoSet == NULL) {
    wError("vgId:%d, failed to init taosArray of fileInfoSet due to %s. path:%s", pWal->cfg.vgId, strerror(errno),
           pWal->path);
    goto _err;
  }

  // init gc
  pWal->toDeleteFiles = taosArrayInit(8, sizeof(SWalFileInfo));
  if (pWal->toDeleteFiles == NULL) {
    wError("vgId:%d, failed to init taosArray of toDeleteFiles due to %s. path:%s", pWal->cfg.vgId, strerror(errno),
           pWal->path);
    goto _err;
  }

  // init status
  pWal->totSize = 0;
  pWal->lastRollSeq = -1;

  // init write buffer
  memset(&pWal->writeHead, 0, sizeof(SWalCkHead));
  pWal->writeHead.head.protoVer = WAL_PROTO_VER;
  pWal->writeHead.magic = WAL_MAGIC;

  // load meta
  (void)walLoadMeta(pWal);

  if (walCheckAndRepairMeta(pWal) < 0) {
    wError("vgId:%d, cannot open wal since repair meta file failed", pWal->cfg.vgId);
    goto _err;
  }

  if (walCheckAndRepairIdx(pWal) < 0) {
    wError("vgId:%d, cannot open wal since repair idx file failed", pWal->cfg.vgId);
    goto _err;
  }

  // add ref
  pWal->refId = taosAddRef(tsWal.refSetId, pWal);
  if (pWal->refId < 0) {
    wError("failed to add ref for Wal since %s", tstrerror(terrno));
    goto _err;
  }

  wDebug("vgId:%d, wal:%p is opened, level:%d fsyncPeriod:%d", pWal->cfg.vgId, pWal, pWal->cfg.level,
         pWal->cfg.fsyncPeriod);
  return pWal;

_err:
  taosArrayDestroy(pWal->fileInfoSet);
  taosHashCleanup(pWal->pRefHash);
  taosThreadMutexDestroy(&pWal->mutex);
  taosMemoryFree(pWal);
  pWal = NULL;
  return NULL;
}

int32_t walAlter(SWal *pWal, SWalCfg *pCfg) {
  if (pWal == NULL) return TSDB_CODE_APP_ERROR;

  if (pWal->cfg.level == pCfg->level && pWal->cfg.fsyncPeriod == pCfg->fsyncPeriod &&
      pWal->cfg.retentionPeriod == pCfg->retentionPeriod && pWal->cfg.retentionSize == pCfg->retentionSize) {
    wDebug("vgId:%d, walLevel:%d fsync:%d walRetentionPeriod:%d walRetentionSize:%" PRId64 " not change",
           pWal->cfg.vgId, pWal->cfg.level, pWal->cfg.fsyncPeriod, pWal->cfg.retentionPeriod, pWal->cfg.retentionSize);
    return 0;
  }

  wInfo("vgId:%d, change old walLevel:%d fsync:%d walRetentionPeriod:%d walRetentionSize:%" PRId64
        ", new walLevel:%d fsync:%d walRetentionPeriod:%d walRetentionSize:%" PRId64,
        pWal->cfg.vgId, pWal->cfg.level, pWal->cfg.fsyncPeriod, pWal->cfg.retentionPeriod, pWal->cfg.retentionSize,
        pCfg->level, pCfg->fsyncPeriod, pCfg->retentionPeriod, pCfg->retentionSize);

  pWal->cfg.level = pCfg->level;
  pWal->cfg.fsyncPeriod = pCfg->fsyncPeriod;
  pWal->cfg.retentionPeriod = pCfg->retentionPeriod;
  pWal->cfg.retentionSize = pCfg->retentionSize;

  pWal->fsyncSeq = pCfg->fsyncPeriod / 1000;
  if (pWal->fsyncSeq <= 0) pWal->fsyncSeq = 1;

  return 0;
}

int32_t walPersist(SWal *pWal) {
  taosThreadMutexLock(&pWal->mutex);
  int32_t ret = walSaveMeta(pWal);
  taosThreadMutexUnlock(&pWal->mutex);
  return ret;
}

void walClose(SWal *pWal) {
  taosThreadMutexLock(&pWal->mutex);
  (void)walSaveMeta(pWal);
  taosCloseFile(&pWal->pLogFile);
  pWal->pLogFile = NULL;
  taosCloseFile(&pWal->pIdxFile);
  pWal->pIdxFile = NULL;
  taosArrayDestroy(pWal->fileInfoSet);
  pWal->fileInfoSet = NULL;
  taosArrayDestroy(pWal->toDeleteFiles);
  pWal->toDeleteFiles = NULL;

  void *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pWal->pRefHash, pIter);
    if (pIter == NULL) break;
    SWalRef *pRef = *(SWalRef **)pIter;
    taosMemoryFree(pRef);
  }
  taosHashCleanup(pWal->pRefHash);
  pWal->pRefHash = NULL;
  taosThreadMutexUnlock(&pWal->mutex);

  taosRemoveRef(tsWal.refSetId, pWal->refId);
}

static void walFreeObj(void *wal) {
  SWal *pWal = wal;
  wDebug("vgId:%d, wal:%p is freed", pWal->cfg.vgId, pWal);

  taosThreadMutexDestroy(&pWal->mutex);
  taosMemoryFreeClear(pWal);
}

static bool walNeedFsync(SWal *pWal) {
  if (pWal->cfg.fsyncPeriod <= 0 || pWal->cfg.level != TAOS_WAL_FSYNC) {
    return false;
  }

  if (atomic_load_32(&tsWal.seq) % pWal->fsyncSeq == 0) {
    return true;
  }

  return false;
}

static void walUpdateSeq() {
  taosMsleep(WAL_REFRESH_MS);
  atomic_add_fetch_32(&tsWal.seq, 1);
}

static void walFsyncAll() {
  SWal *pWal = taosIterateRef(tsWal.refSetId, 0);
  while (pWal) {
    if (walNeedFsync(pWal)) {
      wTrace("vgId:%d, do fsync, level:%d seq:%d rseq:%d", pWal->cfg.vgId, pWal->cfg.level, pWal->fsyncSeq,
             atomic_load_32(&tsWal.seq));
      int32_t code = taosFsyncFile(pWal->pLogFile);
      if (code != 0) {
        wError("vgId:%d, file:%" PRId64 ".log, failed to fsync since %s", pWal->cfg.vgId, walGetLastFileFirstVer(pWal),
               strerror(errno));
      }
    }
    pWal = taosIterateRef(tsWal.refSetId, pWal->refId);
  }
}

static void *walThreadFunc(void *param) {
  setThreadName("wal");
  while (1) {
    walUpdateSeq();
    walFsyncAll();

    if (atomic_load_8(&tsWal.stop)) break;
  }

  return NULL;
}

static int32_t walCreateThread() {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (taosThreadCreate(&tsWal.thread, &thAttr, walThreadFunc, NULL) != 0) {
    wError("failed to create wal thread since %s", strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  taosThreadAttrDestroy(&thAttr);
  wDebug("wal thread is launched, thread:0x%08" PRIx64, taosGetPthreadId(tsWal.thread));

  return 0;
}

static void walStopThread() {
  atomic_store_8(&tsWal.stop, 1);

  if (taosCheckPthreadValid(tsWal.thread)) {
    taosThreadJoin(tsWal.thread, NULL);
    taosThreadClear(&tsWal.thread);
  }

  wDebug("wal thread is stopped");
}
