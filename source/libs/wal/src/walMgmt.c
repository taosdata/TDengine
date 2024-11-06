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
  int8_t      stop;
  int8_t      inited;
  uint32_t    seq;
  int32_t     refSetId;
  TdThread    thread;
  stopDnodeFn stopDnode;
} SWalMgmt;

static SWalMgmt tsWal = {0, .seq = 1};
static int32_t  walCreateThread();
static void     walStopThread();
static void     walFreeObj(void *pWal);

int64_t walGetSeq() { return (int64_t)atomic_load_32((volatile int32_t *)&tsWal.seq); }

int32_t walInit(stopDnodeFn stopDnode) {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(&tsWal.inited, 0, 2);
    if (old != 2) break;
  }

  if (old == 0) {
    tsWal.refSetId = taosOpenRef(TSDB_MIN_VNODES, walFreeObj);

    int32_t code = walCreateThread();
    if (TSDB_CODE_SUCCESS != code) {
      wError("failed to init wal module since %s", tstrerror(code));
      atomic_store_8(&tsWal.inited, 0);

      TAOS_RETURN(code);
    }

    wInfo("wal module is initialized, rsetId:%d", tsWal.refSetId);
    atomic_store_8(&tsWal.inited, 1);
  }

  if (stopDnode == NULL) {
    wWarn("failed to set stop dnode call back");
  }
  tsWal.stopDnode = stopDnode;

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

static int32_t walInitLock(SWal *pWal) {
  TdThreadRwlockAttr attr;
  (void)taosThreadRwlockAttrInit(&attr);
  (void)taosThreadRwlockAttrSetKindNP(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  (void)taosThreadRwlockInit(&pWal->mutex, &attr);
  (void)taosThreadRwlockAttrDestroy(&attr);
  return 0;
}

int32_t walInitWriteFileForSkip(SWal *pWal) {
  TdFilePtr pIdxTFile, pLogTFile;
  int64_t   fileFirstVer = 0;

  char fnameStr[WAL_FILE_LEN];
  walBuildIdxName(pWal, fileFirstVer, fnameStr);
  pIdxTFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pIdxTFile == NULL) {
    TAOS_RETURN(terrno);
  }
  walBuildLogName(pWal, fileFirstVer, fnameStr);
  pLogTFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pLogTFile == NULL) {
    TAOS_RETURN(terrno);
  }
  // switch file
  pWal->pIdxFile = pIdxTFile;
  pWal->pLogFile = pLogTFile;
  pWal->writeCur = taosArrayGetSize(pWal->fileInfoSet) - 1;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

SWal *walOpen(const char *path, SWalCfg *pCfg) {
  int32_t code = 0;
  SWal   *pWal = taosMemoryCalloc(1, sizeof(SWal));
  if (pWal == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  if (walInitLock(pWal) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosMemoryFree(pWal);
    return NULL;
  }

  // set config
  (void)memcpy(&pWal->cfg, pCfg, sizeof(SWalCfg));

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
    wError("vgId:%d, path:%s, failed to create directory since %s", pWal->cfg.vgId, pWal->path, tstrerror(terrno));
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
  (void)memset(&pWal->writeHead, 0, sizeof(SWalCkHead));
  pWal->writeHead.head.protoVer = WAL_PROTO_VER;
  pWal->writeHead.magic = WAL_MAGIC;

  // load meta
  code = walLoadMeta(pWal);
  if (code < 0) {
    wWarn("vgId:%d, failed to load meta since %s", pWal->cfg.vgId, tstrerror(code));
  }
  if (pWal->cfg.level != TAOS_WAL_SKIP) {
    code = walCheckAndRepairMeta(pWal);
    if (code < 0) {
      wError("vgId:%d, cannot open wal since repair meta file failed since %s", pWal->cfg.vgId, tstrerror(code));
      goto _err;
    }

    code = walCheckAndRepairIdx(pWal);
    if (code < 0) {
      wError("vgId:%d, cannot open wal since repair idx file failed since %s", pWal->cfg.vgId, tstrerror(code));
      goto _err;
    }
  } else {
    code = walInitWriteFileForSkip(pWal);
    if (code < 0) {
      wError("vgId:%d, cannot open wal since init write file for wal_level = 0 failed since %s", pWal->cfg.vgId, tstrerror(code));
      goto _err;
    }
  }

  // add ref
  pWal->refId = taosAddRef(tsWal.refSetId, pWal);
  if (pWal->refId < 0) {
    wError("failed to add ref for Wal since %s", tstrerror(terrno));
    goto _err;
  }

  pWal->stopDnode = tsWal.stopDnode;

  wDebug("vgId:%d, wal:%p is opened, level:%d fsyncPeriod:%d", pWal->cfg.vgId, pWal, pWal->cfg.level,
         pWal->cfg.fsyncPeriod);
  return pWal;

_err:
  taosArrayDestroy(pWal->fileInfoSet);
  taosArrayDestroy(pWal->toDeleteFiles);
  taosHashCleanup(pWal->pRefHash);
  TAOS_UNUSED(taosThreadRwlockDestroy(&pWal->mutex));
  taosMemoryFreeClear(pWal);

  return NULL;
}

int32_t walAlter(SWal *pWal, SWalCfg *pCfg) {
  if (pWal == NULL) TAOS_RETURN(TSDB_CODE_APP_ERROR);

  if (pWal->cfg.level == pCfg->level && pWal->cfg.fsyncPeriod == pCfg->fsyncPeriod &&
      pWal->cfg.retentionPeriod == pCfg->retentionPeriod && pWal->cfg.retentionSize == pCfg->retentionSize) {
    wDebug("vgId:%d, walLevel:%d fsync:%d walRetentionPeriod:%d walRetentionSize:%" PRId64 " not change",
           pWal->cfg.vgId, pWal->cfg.level, pWal->cfg.fsyncPeriod, pWal->cfg.retentionPeriod, pWal->cfg.retentionSize);

    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  wInfo("vgId:%d, change old walLevel:%d fsync:%d walRetentionPeriod:%d walRetentionSize:%" PRId64
        ", new walLevel:%d fsync:%d walRetentionPeriod:%d walRetentionSize:%" PRId64,
        pWal->cfg.vgId, pWal->cfg.level, pWal->cfg.fsyncPeriod, pWal->cfg.retentionPeriod, pWal->cfg.retentionSize,
        pCfg->level, pCfg->fsyncPeriod, pCfg->retentionPeriod, pCfg->retentionSize);

  if (pWal->cfg.level == TAOS_WAL_SKIP && pCfg->level != TAOS_WAL_SKIP) {
    wInfo("vgId:%d, remove all wals, path:%s", pWal->cfg.vgId, pWal->path);
    taosRemoveDir(pWal->path);
    if (taosMkDir(pWal->path) != 0) {
      wError("vgId:%d, path:%s, failed to create directory since %s", pWal->cfg.vgId, pWal->path, tstrerror(terrno));
    }
  }

  pWal->cfg.level = pCfg->level;
  pWal->cfg.fsyncPeriod = pCfg->fsyncPeriod;
  pWal->cfg.retentionPeriod = pCfg->retentionPeriod;
  pWal->cfg.retentionSize = pCfg->retentionSize;

  pWal->fsyncSeq = pCfg->fsyncPeriod / 1000;
  if (pWal->fsyncSeq <= 0) pWal->fsyncSeq = 1;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t walPersist(SWal *pWal) {
  int32_t code = 0;

  TAOS_UNUSED(taosThreadRwlockWrlock(&pWal->mutex));
  code = walSaveMeta(pWal);
  TAOS_UNUSED(taosThreadRwlockUnlock(&pWal->mutex));

  TAOS_RETURN(code);
}

void walClose(SWal *pWal) {
  TAOS_UNUSED(taosThreadRwlockWrlock(&pWal->mutex));
  if (walSaveMeta(pWal) < 0) {
    wError("vgId:%d, failed to save meta since %s", pWal->cfg.vgId, tstrerror(terrno));
  }
  TAOS_UNUSED(taosCloseFile(&pWal->pLogFile));
  pWal->pLogFile = NULL;
  (void)taosCloseFile(&pWal->pIdxFile);
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
  (void)taosThreadRwlockUnlock(&pWal->mutex);

  if (pWal->cfg.level == TAOS_WAL_SKIP) {
    wInfo("vgId:%d, remove all wals, path:%s", pWal->cfg.vgId, pWal->path);
    taosRemoveDir(pWal->path);
    if (taosMkDir(pWal->path) != 0) {
      wError("vgId:%d, path:%s, failed to create directory since %s", pWal->cfg.vgId, pWal->path, tstrerror(terrno));
    }
  }

  if (taosRemoveRef(tsWal.refSetId, pWal->refId) < 0) {
    wError("vgId:%d, failed to remove ref for Wal since %s", pWal->cfg.vgId, tstrerror(terrno));
  }
}

static void walFreeObj(void *wal) {
  SWal *pWal = wal;
  wDebug("vgId:%d, wal:%p is freed", pWal->cfg.vgId, pWal);

  (void)taosThreadRwlockDestroy(&pWal->mutex);
  taosMemoryFreeClear(pWal);
}

static bool walNeedFsync(SWal *pWal) {
  if (pWal->cfg.fsyncPeriod <= 0 || pWal->cfg.level != TAOS_WAL_FSYNC) {
    return false;
  }

  if (atomic_load_32((volatile int32_t *)&tsWal.seq) % pWal->fsyncSeq == 0) {
    return true;
  }

  return false;
}

static void walUpdateSeq() {
  taosMsleep(WAL_REFRESH_MS);
  if (atomic_add_fetch_32((volatile int32_t *)&tsWal.seq, 1) < 0) {
    wError("failed to update wal seq since %s", strerror(errno));
  }
}

static void walFsyncAll() {
  SWal *pWal = taosIterateRef(tsWal.refSetId, 0);
  while (pWal) {
    if (walNeedFsync(pWal)) {
      wTrace("vgId:%d, do fsync, level:%d seq:%d rseq:%d", pWal->cfg.vgId, pWal->cfg.level, pWal->fsyncSeq,
             atomic_load_32((volatile int32_t *)&tsWal.seq));
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
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (taosThreadCreate(&tsWal.thread, &thAttr, walThreadFunc, NULL) != 0) {
    wError("failed to create wal thread since %s", strerror(errno));

    TAOS_RETURN(TAOS_SYSTEM_ERROR(errno));
  }

  (void)taosThreadAttrDestroy(&thAttr);
  wDebug("wal thread is launched, thread:0x%08" PRIx64, taosGetPthreadId(tsWal.thread));

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static void walStopThread() {
  atomic_store_8(&tsWal.stop, 1);

  if (taosCheckPthreadValid(tsWal.thread)) {
    (void)taosThreadJoin(tsWal.thread, NULL);
    taosThreadClear(&tsWal.thread);
  }

  wDebug("wal thread is stopped");
}
