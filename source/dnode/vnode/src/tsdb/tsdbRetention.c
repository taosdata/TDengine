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

#include "tss.h"
#include "tsdb.h"
#include "tsdbFS2.h"
#include "tsdbFSet2.h"
#include "vnd.h"
#include "tsdbInt.h"

extern int32_t tsdbAsyncCompact(STsdb *tsdb, const STimeWindow *tw, bool s3Migrate);

static int32_t tsdbDoRemoveFileObject(SRTNer *rtner, const STFileObj *fobj) {
  STFileOp op = {
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  return TARRAY2_APPEND(&rtner->fopArr, op);
}

static int32_t tsdbCopyFileWithLimitedSpeed(TdFilePtr from, TdFilePtr to, int64_t size, uint32_t limitMB) {
  int64_t interval = 1000;  // 1s
  int64_t limit = limitMB ? limitMB * 1024 * 1024 : INT64_MAX;
  int64_t offset = 0;
  int64_t remain = size;

  while (remain > 0) {
    int64_t n;
    int64_t last = taosGetTimestampMs();
    if ((n = taosFSendFile(to, from, &offset, TMIN(limit, remain))) < 0) {
      TAOS_CHECK_RETURN(terrno);
    }

    remain -= n;

    if (remain > 0) {
      int64_t elapsed = taosGetTimestampMs() - last;
      if (elapsed < interval) {
        taosMsleep(interval - elapsed);
      }
    }
  }

  return 0;
}

static int32_t tsdbDoCopyFileLC(SRTNer *rtner, const STFileObj *from, const STFile *to) {
  int32_t   code = 0;
  int32_t   lino = 0;
  TdFilePtr fdFrom = NULL, fdTo = NULL;
  char      fname_from[TSDB_FILENAME_LEN];
  char      fname_to[TSDB_FILENAME_LEN];

  tsdbTFileLastChunkName(rtner->tsdb, from->f, fname_from);
  tsdbTFileLastChunkName(rtner->tsdb, to, fname_to);

  fdFrom = taosOpenFile(fname_from, TD_FILE_READ);
  if (fdFrom == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  tsdbInfo("vgId: %d, open tofile: %s size: %" PRId64, TD_VID(rtner->tsdb->pVnode), fname_to, from->f->size);

  fdTo = taosOpenFile(fname_to, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fdTo == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  SVnodeCfg *pCfg = &rtner->tsdb->pVnode->config;
  int64_t    chunksize = (int64_t)pCfg->tsdbPageSize * pCfg->s3ChunkSize;
  int64_t    lc_size = tsdbLogicToFileSize(to->size, rtner->szPage) - chunksize * (to->lcn - 1);

  if (taosFSendFile(fdTo, fdFrom, 0, lc_size) < 0) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  if (taosCloseFile(&fdFrom) != 0) {
    tsdbError("vgId:%d, failed to close file %s", TD_VID(rtner->tsdb->pVnode), fname_from);
  }
  if (taosCloseFile(&fdTo) != 0) {
    tsdbError("vgId:%d, failed to close file %s", TD_VID(rtner->tsdb->pVnode), fname_to);
  }
  return code;
}

static int32_t tsdbDoCopyFile(SRTNer *rtner, const STFileObj *from, const STFile *to) {
  int32_t code = 0;
  int32_t lino = 0;

  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr fdFrom = NULL;
  TdFilePtr fdTo = NULL;

  tsdbTFileName(rtner->tsdb, to, fname);

  fdFrom = taosOpenFile(from->fname, TD_FILE_READ);
  if (fdFrom == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  tsdbInfo("vgId: %d, open tofile: %s size: %" PRId64, TD_VID(rtner->tsdb->pVnode), fname, from->f->size);

  fdTo = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fdTo == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }
  TSDB_CHECK_CODE(code, lino, _exit);

  TAOS_CHECK_GOTO(tsdbCopyFileWithLimitedSpeed(fdFrom, fdTo, tsdbLogicToFileSize(from->f->size, rtner->szPage),
                                               tsRetentionSpeedLimitMB),
                  &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  if (taosCloseFile(&fdFrom) != 0) {
    tsdbTrace("vgId:%d, failed to close file", TD_VID(rtner->tsdb->pVnode));
  }
  if (taosCloseFile(&fdTo) != 0) {
    tsdbTrace("vgId:%d, failed to close file", TD_VID(rtner->tsdb->pVnode));
  }
  return code;
}

static int32_t tsdbDoMigrateFileObj(SRTNer *rtner, const STFileObj *fobj, const SDiskID *did) {
  int32_t  code = 0;
  int32_t  lino = 0;
  STFileOp op = {0};
  int32_t  lcn = fobj->f->lcn;

  // remove old
  op = (STFileOp){
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  TAOS_CHECK_GOTO(TARRAY2_APPEND(&rtner->fopArr, op), &lino, _exit);

  // create new
  op = (STFileOp){
      .optype = TSDB_FOP_CREATE,
      .fid = fobj->f->fid,
      .nf =
          {
              .type = fobj->f->type,
              .did = did[0],
              .fid = fobj->f->fid,
              .minVer = fobj->f->minVer,
              .maxVer = fobj->f->maxVer,
              .mid = fobj->f->mid,
              .cid = fobj->f->cid,
              .size = fobj->f->size,
              .lcn = lcn,
              .stt[0] =
                  {
                      .level = fobj->f->stt[0].level,
                  },
          },
  };

  TAOS_CHECK_GOTO(TARRAY2_APPEND(&rtner->fopArr, op), &lino, _exit);

  // do copy the file

  if (lcn < 1) {
    TAOS_CHECK_GOTO(tsdbDoCopyFile(rtner, fobj, &op.nf), &lino, _exit);
  } else {
    TAOS_CHECK_GOTO(tsdbDoCopyFileLC(rtner, fobj, &op.nf), &lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

typedef struct {
  STsdb  *tsdb;
  int64_t now;
  int32_t nodeId; // node id of leader vnode in s3 migration
  int32_t fid;
  bool    s3Migrate;
} SRtnArg;

static int32_t tsdbDoRetentionEnd(SRTNer *rtner, bool s3Migrate) {
  int32_t code = 0;
  int32_t lino = 0;

  if (TARRAY2_SIZE(&rtner->fopArr) > 0) {
    EFEditT etype = s3Migrate ? TSDB_FEDIT_S3MIGRATE : TSDB_FEDIT_RETENTION;
    TAOS_CHECK_GOTO(tsdbFSEditBegin(rtner->tsdb->pFS, &rtner->fopArr, etype), &lino, _exit);

    (void)taosThreadMutexLock(&rtner->tsdb->mutex);

    code = tsdbFSEditCommit(rtner->tsdb->pFS);
    if (code) {
      (void)taosThreadMutexUnlock(&rtner->tsdb->mutex);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    (void)taosThreadMutexUnlock(&rtner->tsdb->mutex);

    TARRAY2_DESTROY(&rtner->fopArr, NULL);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  } else {
    tsdbDebug("vid:%d, cid:%" PRId64 ", %s done", TD_VID(rtner->tsdb->pVnode), rtner->cid, __func__);
  }
  return code;
}

static int32_t tsdbRemoveOrMoveFileObject(SRTNer *rtner, int32_t expLevel, STFileObj *fobj) {
  int32_t code = 0;
  int32_t lino = 0;

  if (fobj == NULL) {
    return code;
  }

  if (expLevel < 0) {
    // remove the file
    code = tsdbDoRemoveFileObject(rtner, fobj);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (expLevel > fobj->f->did.level) {
    // Try to move the file to a new level
    for (; expLevel > fobj->f->did.level; expLevel--) {
      SDiskID diskId = {0};

      code = tsdbAllocateDiskAtLevel(rtner->tsdb, expLevel, tsdbFTypeLabel(fobj->f->type), &diskId);
      if (code) {
        tsdbTrace("vgId:%d, cannot allocate disk for file %s, level:%d, reason:%s, skip!", TD_VID(rtner->tsdb->pVnode),
                  fobj->fname, expLevel, tstrerror(code));
        code = 0;
        continue;
      } else {
        tsdbInfo("vgId:%d start to migrate file %s from level %d to %d, size:%" PRId64, TD_VID(rtner->tsdb->pVnode),
                 fobj->fname, fobj->f->did.level, diskId.level, fobj->f->size);

        code = tsdbDoMigrateFileObj(rtner, fobj, &diskId);
        TSDB_CHECK_CODE(code, lino, _exit);

        tsdbInfo("vgId:%d end to migrate file %s from level %d to %d, size:%" PRId64, TD_VID(rtner->tsdb->pVnode),
                 fobj->fname, fobj->f->did.level, diskId.level, fobj->f->size);
        break;
      }
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbDoRetention(SRTNer *rtner) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STFileObj *fobj = NULL;
  STFileSet *fset = rtner->fset;

  // handle data file sets
  int32_t expLevel = tsdbFidLevel(fset->fid, &rtner->tsdb->keepCfg, rtner->now);
  for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ++ftype) {
    code = tsdbRemoveOrMoveFileObject(rtner, expLevel, fset->farr[ftype]);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // handle stt file
  SSttLvl *lvl;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      code = tsdbRemoveOrMoveFileObject(rtner, expLevel, fobj);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

static void tsdbRetentionCancel(void *arg) { taosMemoryFree(arg); }

static int32_t tsdbDoS3Migrate(SRTNer *rtner);

static int32_t tsdbRetention(void *arg) {
  int32_t code = 0;
  int32_t lino = 0;

  SRtnArg   *rtnArg = (SRtnArg *)arg;
  STsdb     *pTsdb = rtnArg->tsdb;
  SVnode    *pVnode = pTsdb->pVnode;
  STFileSet *fset = NULL;
  SRTNer     rtner = {
          .tsdb = pTsdb,
          .szPage = pVnode->config.tsdbPageSize,
          .now = rtnArg->now,
          .cid = tsdbFSAllocEid(pTsdb->pFS),
          .nodeId = rtnArg->nodeId,
  };

  // begin task
  (void)taosThreadMutexLock(&pTsdb->mutex);

  // check if background task is disabled
  if (pTsdb->bgTaskDisabled) {
    tsdbInfo("vgId:%d, background task is disabled, skip retention", TD_VID(pTsdb->pVnode));
    (void)taosThreadMutexUnlock(&pTsdb->mutex);
    return 0;
  }

  // set flag and copy
  tsdbBeginTaskOnFileSet(pTsdb, rtnArg->fid, EVA_TASK_RETENTION, &fset);
  if (fset && (code = tsdbTFileSetInitCopy(pTsdb, fset, &rtner.fset))) {
    (void)taosThreadMutexUnlock(&pTsdb->mutex);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  (void)taosThreadMutexUnlock(&pTsdb->mutex);

  // do retention
  if (rtner.fset) {
    if (rtnArg->s3Migrate) {
#ifdef USE_S3
      TAOS_CHECK_GOTO(tsdbDoSsMigrate(&rtner), &lino, _exit);
#endif
    } else {
      TAOS_CHECK_GOTO(tsdbDoRetention(&rtner), &lino, _exit);
    }

    TAOS_CHECK_GOTO(tsdbDoRetentionEnd(&rtner, rtnArg->s3Migrate), &lino, _exit);
  }

_exit:
  if (rtner.fset) {
    (void)taosThreadMutexLock(&pTsdb->mutex);
    tsdbFinishTaskOnFileSet(pTsdb, rtnArg->fid, EVA_TASK_RETENTION);
    (void)taosThreadMutexUnlock(&pTsdb->mutex);
  }

  // clear resources
  tsdbTFileSetClear(&rtner.fset);
  TARRAY2_DESTROY(&rtner.fopArr, NULL);
  taosMemoryFree(arg);
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(pTsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbAsyncRetentionImpl(STsdb *tsdb, int64_t now, bool s3Migrate, int32_t nodeId) {
  void tsdbS3MigrateMonitorAddFileSet(STsdb *tsdb, int32_t fid);

  int32_t code = 0;
  int32_t lino = 0;

  // check if background task is disabled
  if (tsdb->bgTaskDisabled) {
    if (s3Migrate) {
      tsdbInfo("vgId:%d, background task is disabled, skip s3 migration", TD_VID(tsdb->pVnode));
    } else {
      tsdbInfo("vgId:%d, background task is disabled, skip retention", TD_VID(tsdb->pVnode));
    }
    return 0;
  }

  STFileSet *fset;
  TARRAY2_FOREACH(tsdb->pFS->fSetArr, fset) {
    // TODO: when migrating to S3, skip fset that should not be migrated
    
    if (s3Migrate && fset->lastMigrate/1000 >= now) {
      tsdbDebug("vgId:%d, fid:%d, skip migration as start time < last migration time", TD_VID(tsdb->pVnode), fset->fid);
      continue;
    }

    SRtnArg *arg = taosMemoryMalloc(sizeof(*arg));
    if (arg == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }

    arg->tsdb = tsdb;
    arg->now = now;
    arg->fid = fset->fid;
    arg->nodeId = nodeId;
    arg->s3Migrate = s3Migrate;

    tsdbS3MigrateMonitorAddFileSet(tsdb, fset->fid);
    code = vnodeAsync(RETENTION_TASK_ASYNC, EVA_PRIORITY_LOW, tsdbRetention, tsdbRetentionCancel, arg,
                      &fset->retentionTask);
    if (code) {
      taosMemoryFree(arg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbAsyncRetention(STsdb *tsdb, int64_t now) {
  int32_t code = 0;
  (void)taosThreadMutexLock(&tsdb->mutex);
  code = tsdbAsyncRetentionImpl(tsdb, now, false, 0);
  (void)taosThreadMutexUnlock(&tsdb->mutex);
  return code;
}

#ifdef USE_S3

int32_t tsdbAsyncS3Migrate(STsdb *tsdb, SS3MigrateVgroupReq *pReq) {
  int32_t code = 0;

  #if 0
  int32_t expired = grantCheck(TSDB_GRANT_OBJECT_STORAGE);
  if (expired && tsSsEnabled) {
    tsdbWarn("s3 grant expired: %d", expired);
    tsSsEnabled = false;
  } else if (!expired) {
    tsSsEnabled = true;
  }

  if (!tsSsEnabled) {
    return 0;
  }
    #endif

  void tsdbStartS3MigrateMonitor(STsdb *tsdb, int32_t s3MigrateId);

  (void)taosThreadMutexLock(&tsdb->mutex);
  tsdbStartS3MigrateMonitor(tsdb, pReq->s3MigrateId);
  code = tsdbAsyncRetentionImpl(tsdb, pReq->timestamp, true, pReq->nodeId);
  (void)taosThreadMutexUnlock(&tsdb->mutex);

  if (code) {
    tsdbError("vgId:%d, %s failed, reason:%s", TD_VID(tsdb->pVnode), __func__, tstrerror(code));
  }
  return code;
}

#endif

