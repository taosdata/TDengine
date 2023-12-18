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

#include "cos.h"
#include "tsdb.h"
#include "tsdbFS2.h"
#include "vnd.h"

typedef struct {
  STsdb  *tsdb;
  int32_t szPage;
  int64_t now;
  int64_t cid;

  TFileSetArray *fsetArr;
  TFileOpArray   fopArr[1];
} SRTNer;

static int32_t tsdbDoRemoveFileObject(SRTNer *rtner, const STFileObj *fobj) {
  STFileOp op = {
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  return TARRAY2_APPEND(rtner->fopArr, op);
}

static int32_t tsdbRemoveFileObjectS3(SRTNer *rtner, const STFileObj *fobj) {
  int32_t code = 0, lino = 0;

  STFileOp op = {
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  code = TARRAY2_APPEND(rtner->fopArr, op);
  TSDB_CHECK_CODE(code, lino, _exit);

  const char *object_name = taosDirEntryBaseName((char *)fobj->fname);
  s3DeleteObjects(&object_name, 1);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtner->tsdb->pVnode), lino, code);
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
  if (fdFrom == NULL) code = terrno;
  TSDB_CHECK_CODE(code, lino, _exit);

  tsdbInfo("vgId: %d, open tofile: %s size: %" PRId64, TD_VID(rtner->tsdb->pVnode), fname, from->f->size);

  fdTo = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fdTo == NULL) code = terrno;
  TSDB_CHECK_CODE(code, lino, _exit);

  int64_t n = taosFSendFile(fdTo, fdFrom, 0, tsdbLogicToFileSize(from->f->size, rtner->szPage));
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  taosCloseFile(&fdFrom);
  taosCloseFile(&fdTo);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtner->tsdb->pVnode), lino, code);
    taosCloseFile(&fdFrom);
    taosCloseFile(&fdTo);
  }
  return code;
}

static int32_t tsdbCopyFileS3(SRTNer *rtner, const STFileObj *from, const STFile *to) {
  int32_t code = 0;
  int32_t lino = 0;

  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr fdFrom = NULL;
  // TdFilePtr fdTo = NULL;

  tsdbTFileName(rtner->tsdb, to, fname);

  fdFrom = taosOpenFile(from->fname, TD_FILE_READ);
  if (fdFrom == NULL) code = terrno;
  TSDB_CHECK_CODE(code, lino, _exit);

  char *object_name = taosDirEntryBaseName(fname);
  code = s3PutObjectFromFile2(from->fname, object_name, 1);
  TSDB_CHECK_CODE(code, lino, _exit);

  taosCloseFile(&fdFrom);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtner->tsdb->pVnode), lino, code);
    taosCloseFile(&fdFrom);
  }
  return code;
}

static int32_t tsdbDoMigrateFileObj(SRTNer *rtner, const STFileObj *fobj, const SDiskID *did) {
  int32_t  code = 0;
  int32_t  lino = 0;
  STFileOp op = {0};

  // remove old
  op = (STFileOp){
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  code = TARRAY2_APPEND(rtner->fopArr, op);
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
              .minVer = fobj->f->minVer,
              .maxVer = fobj->f->maxVer,
              .cid = fobj->f->cid,
              .size = fobj->f->size,
              .stt[0] =
                  {
                      .level = fobj->f->stt[0].level,
                  },
          },
  };

  code = TARRAY2_APPEND(rtner->fopArr, op);
  TSDB_CHECK_CODE(code, lino, _exit);

  // do copy the file
  code = tsdbDoCopyFile(rtner, fobj, &op.nf);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtner->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbMigrateDataFileS3(SRTNer *rtner, const STFileObj *fobj, const SDiskID *did) {
  int32_t  code = 0;
  int32_t  lino = 0;
  STFileOp op = {0};

  // remove old
  op = (STFileOp){
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  code = TARRAY2_APPEND(rtner->fopArr, op);
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
              .minVer = fobj->f->minVer,
              .maxVer = fobj->f->maxVer,
              .cid = fobj->f->cid,
              .size = fobj->f->size,
              .stt[0] =
                  {
                      .level = fobj->f->stt[0].level,
                  },
          },
  };

  op.nf.s3flag = true;

  code = TARRAY2_APPEND(rtner->fopArr, op);
  TSDB_CHECK_CODE(code, lino, _exit);

  // do copy the file
  code = tsdbCopyFileS3(rtner, fobj, &op.nf);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtner->tsdb->pVnode), lino, code);
  }
  return code;
}

typedef struct {
  STsdb  *tsdb;
  int64_t now;
  int32_t fid;
} SRtnArg;

static int32_t tsdbDoRetentionBegin(SRtnArg *arg, SRTNer *rtner) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *tsdb = arg->tsdb;

  rtner->tsdb = tsdb;
  rtner->szPage = tsdb->pVnode->config.tsdbPageSize;
  rtner->now = arg->now;
  rtner->cid = tsdbFSAllocEid(tsdb->pFS);

  code = tsdbFSCreateCopySnapshot(tsdb->pFS, &rtner->fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtner->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vid:%d, cid:%" PRId64 ", %s done", TD_VID(rtner->tsdb->pVnode), rtner->cid, __func__);
  }
  return code;
}

static int32_t tsdbDoRetentionEnd(SRTNer *rtner) {
  int32_t code = 0;
  int32_t lino = 0;

  if (TARRAY2_SIZE(rtner->fopArr) == 0) goto _exit;

  code = tsdbFSEditBegin(rtner->tsdb->pFS, rtner->fopArr, TSDB_FEDIT_MERGE);
  TSDB_CHECK_CODE(code, lino, _exit);

  taosThreadMutexLock(&rtner->tsdb->mutex);

  code = tsdbFSEditCommit(rtner->tsdb->pFS);
  if (code) {
    taosThreadMutexUnlock(&rtner->tsdb->mutex);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  taosThreadMutexUnlock(&rtner->tsdb->mutex);

  TARRAY2_DESTROY(rtner->fopArr, NULL);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtner->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vid:%d, cid:%" PRId64 ", %s done", TD_VID(rtner->tsdb->pVnode), rtner->cid, __func__);
  }
  tsdbFSDestroyCopySnapshot(&rtner->fsetArr);
  return code;
}

static int32_t tsdbDoRetentionOnFileSet(SRTNer *rtner, STFileSet *fset) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STFileObj *fobj = NULL;
  int32_t    expLevel = tsdbFidLevel(fset->fid, &rtner->tsdb->keepCfg, rtner->now);

  if (expLevel < 0) {  // remove the fileset
    for (int32_t ftype = 0; (ftype < TSDB_FTYPE_MAX) && (fobj = fset->farr[ftype], 1); ++ftype) {
      if (fobj == NULL) continue;
      /*
      int32_t nlevel = tfsGetLevel(rtner->tsdb->pVnode->pTfs);
      if (tsS3Enabled && nlevel > 1 && TSDB_FTYPE_DATA == ftype && fobj->f->did.level == nlevel - 1) {
        code = tsdbRemoveFileObjectS3(rtner, fobj);
        TSDB_CHECK_CODE(code, lino, _exit);
        } else {*/
      code = tsdbDoRemoveFileObject(rtner, fobj);
      TSDB_CHECK_CODE(code, lino, _exit);
      //}
    }

    SSttLvl *lvl;
    TARRAY2_FOREACH(fset->lvlArr, lvl) {
      TARRAY2_FOREACH(lvl->fobjArr, fobj) {
        code = tsdbDoRemoveFileObject(rtner, fobj);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  } else if (expLevel == 0) {  // only migrate to upper level
    return 0;
  } else {  // migrate
    SDiskID did;

    if (tfsAllocDisk(rtner->tsdb->pVnode->pTfs, expLevel, &did) < 0) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tfsMkdirRecurAt(rtner->tsdb->pVnode->pTfs, rtner->tsdb->path, did);

    // data
    for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX && (fobj = fset->farr[ftype], 1); ++ftype) {
      if (fobj == NULL) continue;

      int32_t nlevel = tfsGetLevel(rtner->tsdb->pVnode->pTfs);

      if (fobj->f->did.level == did.level) {
        if (tsS3Enabled && nlevel > 1 && TSDB_FTYPE_DATA == ftype && did.level == nlevel - 1 &&
            taosCheckExistFile(fobj->fname)) {
          int32_t mtime = 0;
          taosStatFile(fobj->fname, NULL, &mtime, NULL);
          if (mtime < rtner->now - tsS3UploadDelaySec) {
            tsdbInfo("file:%s size: %" PRId64 " do migrate s3", fobj->fname, fobj->f->size);
            code = tsdbMigrateDataFileS3(rtner, fobj, &did);
            TSDB_CHECK_CODE(code, lino, _exit);
          }
        }

        continue;
      }
      /*
    if (tsS3Enabled && nlevel > 1 && TSDB_FTYPE_DATA == ftype && did.level == nlevel - 1) {
      code = tsdbMigrateDataFileS3(rtner, fobj, &did);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {

      if (tsS3Enabled) {
        int64_t fsize = 0;
        if (taosStatFile(fobj->fname, &fsize, NULL, NULL) < 0) {
          code = TAOS_SYSTEM_ERROR(terrno);
          tsdbError("vgId:%d %s failed since file:%s stat failed, reason:%s", TD_VID(rtner->tsdb->pVnode),
      __func__, fobj->fname, tstrerror(code)); TSDB_CHECK_CODE(code, lino, _exit);
        }
        s3EvictCache(fobj->fname, fsize * 2);
      }
      */
      if (fobj->f->did.level > did.level) {
        continue;
      }
      tsdbInfo("file:%s size: %" PRId64 " do migrate from %d to %d", fobj->fname, fobj->f->size, fobj->f->did.level,
               did.level);

      code = tsdbDoMigrateFileObj(rtner, fobj, &did);
      TSDB_CHECK_CODE(code, lino, _exit);
      //}
    }

    // stt
    SSttLvl *lvl;
    TARRAY2_FOREACH(fset->lvlArr, lvl) {
      TARRAY2_FOREACH(lvl->fobjArr, fobj) {
        if (fobj->f->did.level == did.level) continue;

        code = tsdbDoMigrateFileObj(rtner, fobj, &did);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtner->tsdb->pVnode), lino, code);
  }
  return code;
}

static void tsdbFreeRtnArg(void *arg) { taosMemoryFree(arg); }

static int32_t tsdbDoRetentionAsync(void *arg) {
  int32_t code = 0;
  int32_t lino = 0;
  SRTNer  rtner[1] = {0};

  code = tsdbDoRetentionBegin(arg, rtner);
  TSDB_CHECK_CODE(code, lino, _exit);

  STFileSet *fset;
  TARRAY2_FOREACH(rtner->fsetArr, fset) {
    if (fset->fid != ((SRtnArg *)arg)->fid) continue;

    code = tsdbDoRetentionOnFileSet(rtner, fset);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbDoRetentionEnd(rtner);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    if (TARRAY2_DATA(rtner->fopArr)) {
      TARRAY2_DESTROY(rtner->fopArr, NULL);
    }
    TFileSetArray **fsetArr = &rtner->fsetArr;
    if (fsetArr[0]) {
      tsdbFSDestroyCopySnapshot(&rtner->fsetArr);
    }

    TSDB_ERROR_LOG(TD_VID(rtner->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbRetention(STsdb *tsdb, int64_t now, int32_t sync) {
  int32_t code = 0;

  taosThreadMutexLock(&tsdb->mutex);

  if (tsdb->bgTaskDisabled) {
    taosThreadMutexUnlock(&tsdb->mutex);
    return 0;
  }

  STFileSet *fset;
  TARRAY2_FOREACH(tsdb->pFS->fSetArr, fset) {
    code = tsdbTFileSetOpenChannel(fset);
    if (code) {
      taosThreadMutexUnlock(&tsdb->mutex);
      return code;
    }

    SRtnArg *arg = taosMemoryMalloc(sizeof(*arg));
    if (arg == NULL) {
      taosThreadMutexUnlock(&tsdb->mutex);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    arg->tsdb = tsdb;
    arg->now = now;
    arg->fid = fset->fid;

    if (sync) {
      code = vnodeAsyncC(vnodeAsyncHandle[0], tsdb->pVnode->commitChannel, EVA_PRIORITY_LOW, tsdbDoRetentionAsync,
                         tsdbFreeRtnArg, arg, NULL);
    } else {
      code = vnodeAsyncC(vnodeAsyncHandle[1], fset->bgTaskChannel, EVA_PRIORITY_LOW, tsdbDoRetentionAsync,
                         tsdbFreeRtnArg, arg, NULL);
    }
    if (code) {
      tsdbFreeRtnArg(arg);
      taosThreadMutexUnlock(&tsdb->mutex);
      return code;
    }
  }

  taosThreadMutexUnlock(&tsdb->mutex);

  return code;
}
