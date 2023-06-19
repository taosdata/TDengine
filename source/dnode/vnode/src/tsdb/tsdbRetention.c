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

#include "tsdb.h"
#include "tsdbFS2.h"

static bool tsdbShouldDoRetentionImpl(STsdb *pTsdb, int64_t now) {
  for (int32_t iSet = 0; iSet < taosArrayGetSize(pTsdb->fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pTsdb->fs.aDFileSet, iSet);
    int32_t    expLevel = tsdbFidLevel(pSet->fid, &pTsdb->keepCfg, now);
    SDiskID    did;

    if (expLevel == pSet->diskId.level) continue;

    if (expLevel < 0) {
      return true;
    } else {
      if (tfsAllocDisk(pTsdb->pVnode->pTfs, expLevel, &did) < 0) {
        return false;
      }

      if (did.level == pSet->diskId.level) continue;

      return true;
    }
  }

  return false;
}
bool tsdbShouldDoRetention(STsdb *pTsdb, int64_t now) {
  bool should;
  taosThreadRwlockRdlock(&pTsdb->rwLock);
  should = tsdbShouldDoRetentionImpl(pTsdb, now);
  taosThreadRwlockUnlock(&pTsdb->rwLock);
  return should;
}

int32_t tsdbDoRetention(STsdb *pTsdb, int64_t now) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdbFS fs = {0};

  code = tsdbFSCopy(pTsdb, &fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  for (int32_t iSet = 0; iSet < taosArrayGetSize(fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(fs.aDFileSet, iSet);
    int32_t    expLevel = tsdbFidLevel(pSet->fid, &pTsdb->keepCfg, now);
    SDiskID    did;

    if (expLevel < 0) {
      taosMemoryFree(pSet->pHeadF);
      taosMemoryFree(pSet->pDataF);
      taosMemoryFree(pSet->pSmaF);
      for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
        taosMemoryFree(pSet->aSttF[iStt]);
      }
      taosArrayRemove(fs.aDFileSet, iSet);
      iSet--;
    } else {
      if (expLevel == 0) continue;
      if (tfsAllocDisk(pTsdb->pVnode->pTfs, expLevel, &did) < 0) {
        code = terrno;
        goto _exit;
      }

      if (did.level == pSet->diskId.level) continue;

      // copy file to new disk (todo)
      SDFileSet fSet = *pSet;
      fSet.diskId = did;

      code = tsdbDFileSetCopy(pTsdb, pSet, &fSet);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbFSUpsertFSet(&fs, &fSet);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // do change fs
  code = tsdbFSPrepareCommit(pTsdb, &fs);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  tsdbFSDestroy(&fs);
  return code;
}

static int32_t tsdbCommitRetentionImpl(STsdb *pTsdb) { return tsdbFSCommit(pTsdb); }

int32_t tsdbCommitRetention(STsdb *pTsdb) {
  taosThreadRwlockWrlock(&pTsdb->rwLock);
  tsdbCommitRetentionImpl(pTsdb);
  taosThreadRwlockUnlock(&pTsdb->rwLock);
  tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  return 0;
}

// new ==============
typedef struct {
  STsdb  *tsdb;
  int32_t szPage;
  int64_t now;
  int64_t cid;

  TFileSetArray *fsetArr;
  TFileOpArray  *fopArr;

  struct {
    int32_t    fsetArrIdx;
    STFileSet *fset;
  } ctx[1];
} SRTXer;

static int32_t tsdbDoRemoveFileObject(SRTXer *rtxer, const STFileObj *fobj) {
  STFileOp op = {
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  return TARRAY2_APPEND(rtxer->fopArr, op);
}

static int32_t tsdbDoCopyFile(SRTXer *rtxer, const STFileObj *from, const STFile *to) {
  int32_t code = 0;
  int32_t lino = 0;

  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr fdFrom = NULL;
  TdFilePtr fdTo = NULL;

  tsdbTFileName(rtxer->tsdb, to, fname);

  fdFrom = taosOpenFile(from->fname, TD_FILE_READ);
  if (fdFrom == NULL) code = terrno;
  TSDB_CHECK_CODE(code, lino, _exit);

  fdTo = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fdTo == NULL) code = terrno;
  TSDB_CHECK_CODE(code, lino, _exit);

  int64_t n = taosFSendFile(fdTo, fdFrom, 0, tsdbLogicToFileSize(from->f->size, rtxer->szPage));
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  taosCloseFile(&fdFrom);
  taosCloseFile(&fdTo);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtxer->tsdb->pVnode), lino, code);
    taosCloseFile(&fdFrom);
    taosCloseFile(&fdTo);
  }
  return code;
}

static int32_t tsdbDoMigrateFileObj(SRTXer *rtxer, const STFileObj *fobj, const SDiskID *did) {
  int32_t  code = 0;
  int32_t  lino = 0;
  STFileOp op = {0};

  // remove old
  op = (STFileOp){
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  code = TARRAY2_APPEND(rtxer->fopArr, op);
  TSDB_CHECK_CODE(code, lino, _exit);

  // create new
  op = (STFileOp){
      .optype = TSDB_FOP_CREATE,
      .fid = fobj->f->fid,
      .nf =
          {
              .type = fobj->f->type,
              .did = did[0],
              .fid = fobj->f->fid,
              .cid = rtxer->cid,
              .size = fobj->f->size,
              .stt[0] =
                  {
                      .level = fobj->f->stt[0].level,
                  },
          },
  };

  code = TARRAY2_APPEND(rtxer->fopArr, op);
  TSDB_CHECK_CODE(code, lino, _exit);

  // do copy the file
  code = tsdbDoCopyFile(rtxer, fobj, &op.nf);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtxer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDoRetentionBegin(STsdb *tsdb, SRTXer *rtxer) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO: wait for merge and compact task done

  rtxer->tsdb = tsdb;
  rtxer->szPage = tsdb->pVnode->config.tsdbPageSize;
  rtxer->now = taosGetTimestampMs();
  rtxer->cid = tsdbFSAllocEid(tsdb->pFS);

  code = tsdbFSCreateCopySnapshot(tsdb->pFS, &rtxer->fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtxer->tsdb->pVnode), lino, code);
  } else {
    tsdbInfo("vid:%d, cid:%" PRId64 ", %s done", TD_VID(rtxer->tsdb->pVnode), rtxer->cid, __func__);
  }
  return code;
}

static int32_t tsdbDoRetentionEnd(SRTXer *rtxer) {
  int32_t code = 0;
  int32_t lino = 0;

  if (TARRAY2_SIZE(rtxer->fopArr) == 0) goto _exit;

  code = tsdbFSEditBegin(rtxer->tsdb->pFS, rtxer->fopArr, TSDB_FEDIT_MERGE);
  TSDB_CHECK_CODE(code, lino, _exit);

  taosThreadRwlockWrlock(&rtxer->tsdb->rwLock);

  code = tsdbFSEditCommit(rtxer->tsdb->pFS);
  if (code) {
    taosThreadRwlockUnlock(&rtxer->tsdb->rwLock);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  taosThreadRwlockUnlock(&rtxer->tsdb->rwLock);

  TARRAY2_DESTROY(rtxer->fopArr, NULL);
  tsdbFSDestroyCopySnapshot(&rtxer->fsetArr);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtxer->tsdb->pVnode), lino, code);
  } else {
    tsdbInfo("vid:%d, cid:%" PRId64 ", %s done", TD_VID(rtxer->tsdb->pVnode), rtxer->cid, __func__);
  }
  return code;
}

static int32_t tsdbDoRetention2(STsdb *tsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  SRTXer rtxer[1] = {0};

  code = tsdbDoRetentionBegin(tsdb, rtxer);
  TSDB_CHECK_CODE(code, lino, _exit);

  while (rtxer->ctx->fsetArrIdx < TARRAY2_SIZE(rtxer->fsetArr)) {
    rtxer->ctx->fset = TARRAY2_GET(rtxer->fsetArr, rtxer->ctx->fsetArrIdx);

    STFileObj *fobj;
    int32_t    expLevel = tsdbFidLevel(rtxer->ctx->fset->fid, &rtxer->tsdb->keepCfg, rtxer->now);

    if (expLevel < 0) {  // remove the file set
      for (int32_t ftype = 0; (ftype < TSDB_FTYPE_MAX) && (fobj = rtxer->ctx->fset->farr[ftype], 1); ++ftype) {
        if (fobj == NULL) continue;

        code = tsdbDoRemoveFileObject(rtxer, fobj);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      SSttLvl *lvl;
      TARRAY2_FOREACH(rtxer->ctx->fset->lvlArr, lvl) {
        TARRAY2_FOREACH(lvl->fobjArr, fobj) {
          code = tsdbDoRemoveFileObject(rtxer, fobj);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }
    } else if (expLevel == 0) {
      continue;
    } else {
      SDiskID did;

      if (tfsAllocDisk(rtxer->tsdb->pVnode->pTfs, expLevel, &did) < 0) {
        code = terrno;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      // data
      for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX && (fobj = rtxer->ctx->fset->farr[ftype], 1); ++ftype) {
        if (fobj == NULL) continue;

        if (fobj->f->did.level == did.level) continue;
        code = tsdbDoMigrateFileObj(rtxer, fobj, &did);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      // stt
      SSttLvl *lvl;
      TARRAY2_FOREACH(rtxer->ctx->fset->lvlArr, lvl) {
        TARRAY2_FOREACH(lvl->fobjArr, fobj) {
          if (fobj->f->did.level == did.level) continue;

          code = tsdbDoMigrateFileObj(rtxer, fobj, &did);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }
    }
  }

  code = tsdbDoRetentionEnd(rtxer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtxer->tsdb->pVnode), lino, code);
  }
  return code;
}
