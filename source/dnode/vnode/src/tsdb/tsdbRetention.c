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

extern int32_t tsdbAsyncCompact(STsdb *tsdb, const STimeWindow *tw, ETsdbOpType type);

// tsdbRetentionMonitor.c
extern int32_t tsdbAddRetentionMonitorTask(STsdb *tsdb, int32_t fid, SVATaskID *taskId, int64_t fileSize);
extern void    tsdbRemoveRetentionMonitorTask(STsdb *tsdb, SVATaskID *taskId);

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
  int64_t    chunksize = (int64_t)pCfg->tsdbPageSize * pCfg->ssChunkSize;
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


static int32_t tsdbDoRetentionEnd(SRTNer *rtner, EFEditT etype) {
  int32_t code = 0;
  int32_t lino = 0;

  if (TARRAY2_SIZE(&rtner->fopArr) > 0) {
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
  int32_t expLevel = tsdbFidLevel(fset->fid, &rtner->tsdb->keepCfg, rtner->tw.ekey);
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

void tsdbRetentionCancel(void *arg) { taosMemoryFree(arg); }

static bool tsdbFSetNeedRetention(STFileSet *fset, int32_t expLevel) {
  if (expLevel < 0) {
    return false;
  }

  STFileObj *fobj = NULL;
  for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ++ftype) {
    fobj = fset->farr[ftype];
    if (fobj && (expLevel > fset->farr[ftype]->f->did.level)) {
      return true;
    }
  }

  // handle stt file
  SSttLvl *lvl = NULL;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      if (fobj && (expLevel > fobj->f->did.level)) {
        return true;
      }
    }
  }

  return false;
}
#ifdef TD_ENTERPRISE
static bool tsdbShouldRollup(STsdb *tsdb, SRTNer *rtner, SRtnArg *rtnArg) {
  SVnode    *pVnode = tsdb->pVnode;
  STFileSet *fset = rtner->fset;

  if (!VND_IS_RSMA(pVnode)) {
    return false;
  }

  int32_t expLevel = tsdbFidLevel(fset->fid, &tsdb->keepCfg, rtner->tw.ekey);
  if (expLevel <= 0) {
    return false;
  }

  if (rtnArg->optrType == TSDB_OPTR_ROLLUP) {
    return true;
  } else if (rtnArg->optrType == TSDB_OPTR_NORMAL) {
    return tsdbFSetNeedRetention(fset, expLevel);
  }
  return false;
}
#endif
int32_t tsdbRetention(void *arg) {
  int32_t code = 0;
  int32_t lino = 0;

  SRtnArg   *rtnArg = (SRtnArg *)arg;
  STsdb     *pTsdb = rtnArg->tsdb;
  SVnode    *pVnode = pTsdb->pVnode;
  STFileSet *fset = NULL;
  SRTNer     rtner = {
          .tsdb = pTsdb,
          .szPage = pVnode->config.tsdbPageSize,
          .tw = rtnArg->tw,
          .lastCommit = rtnArg->lastCommit,
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
    EFEditT etype = TSDB_FEDIT_RETENTION;
    if (rtnArg->optrType == TSDB_OPTR_SSMIGRATE) {
      etype = TSDB_FEDIT_SSMIGRATE;
      TAOS_CHECK_GOTO(tsdbDoSsMigrate(&rtner), &lino, _exit);
#ifdef TD_ENTERPRISE
    } else if (tsdbShouldRollup(pTsdb, &rtner, rtnArg)) {
      etype = TSDB_FEDIT_ROLLUP;
      TAOS_CHECK_GOTO(tsdbDoRollup(&rtner), &lino, _exit);
#endif
    } else if (rtnArg->optrType == TSDB_OPTR_NORMAL) {
      TAOS_CHECK_GOTO(tsdbDoRetention(&rtner), &lino, _exit);
    } else {
      goto _exit;
    }

    TAOS_CHECK_GOTO(tsdbDoRetentionEnd(&rtner, etype), &lino, _exit);
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
  (void)tsdbRemoveRetentionMonitorTask(pTsdb, &rtnArg->taskid);
  taosMemoryFree(arg);
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(pTsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

static bool tsdbInRetentionTimeRange(STsdb *tsdb, int32_t fid, STimeWindow tw, int8_t optrType) {
  if (optrType == TSDB_OPTR_ROLLUP) {
    TSKEY  minKey, maxKey;
    int8_t precision = tsdb->keepCfg.precision;
    if (precision < TSDB_TIME_PRECISION_MILLI || precision > TSDB_TIME_PRECISION_NANO) {
      tsdbError("vgId:%d, failed to check retention time range since invalid precision %" PRIi8, TD_VID(tsdb->pVnode), precision);
      return false;
    }
    tsdbFidKeyRange(fid, tsdb->keepCfg.days, precision, &minKey, &maxKey);

    if ((tw.ekey != INT64_MAX) && ((double)tw.ekey * (double)tsSecTimes[precision] < (double)minKey)) {
      return false;
    }
    if ((tw.skey != INT64_MIN) && ((double)tw.skey * (double)tsSecTimes[precision] > (double)maxKey)) {
      return false;
    }
    return true;
  }
  return true;
}

static int32_t tsdbAsyncRetentionImpl(STsdb *tsdb, STimeWindow tw, int8_t optrType, int8_t triggerType) {
  int32_t code = 0;
  int32_t lino = 0;

  // check if background task is disabled
  if (tsdb->bgTaskDisabled) {
    tsdbInfo("vgId:%d, background task is disabled, skip retention", TD_VID(tsdb->pVnode));
    return 0;
  }

  STFileSet *fset;
  TARRAY2_FOREACH(tsdb->pFS->fSetArr, fset) {
    if (!tsdbInRetentionTimeRange(tsdb, fset->fid, tw, optrType)) {
      continue;
    }
    SRtnArg *arg = taosMemoryMalloc(sizeof(*arg));
    if (arg == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }

    arg->tsdb = tsdb;
    arg->tw = tw;
    arg->fid = fset->fid;
    arg->nodeId = 0;
    arg->optrType = optrType;
    arg->triggerType = triggerType;
    arg->lastCommit = fset->lastCommit;

    code = vnodeAsync(RETENTION_TASK_ASYNC, EVA_PRIORITY_LOW, tsdbRetention, tsdbRetentionCancel, arg,
                      &fset->retentionTask);
    if (code) {
      taosMemoryFree(arg);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      arg->taskid = fset->retentionTask;
      int64_t fileSize = tsdbTFileSetGetDataSize(fset);
      TAOS_UNUSED(tsdbAddRetentionMonitorTask(tsdb, fset->fid, &arg->taskid, fileSize));
    }
  }
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbAsyncRetention(STsdb *tsdb, STimeWindow tw, int8_t optrType, int8_t triggerType) {
  int32_t code = 0;
  (void)taosThreadMutexLock(&tsdb->mutex);
  code = tsdbAsyncRetentionImpl(tsdb, tw, optrType, triggerType);
  (void)taosThreadMutexUnlock(&tsdb->mutex);
  return code;
}
