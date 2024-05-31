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

  STFileSet   *fset;
  TFileOpArray fopArr;
} SRTNer;

static int32_t tsdbDoRemoveFileObject(SRTNer *rtner, const STFileObj *fobj) {
  STFileOp op = {
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  return TARRAY2_APPEND(&rtner->fopArr, op);
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
  if (fdFrom == NULL) code = terrno;
  TSDB_CHECK_CODE(code, lino, _exit);

  tsdbInfo("vgId: %d, open tofile: %s size: %" PRId64, TD_VID(rtner->tsdb->pVnode), fname_to, from->f->size);

  fdTo = taosOpenFile(fname_to, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fdTo == NULL) code = terrno;
  TSDB_CHECK_CODE(code, lino, _exit);

  SVnodeCfg *pCfg = &rtner->tsdb->pVnode->config;
  int64_t    chunksize = (int64_t)pCfg->tsdbPageSize * pCfg->s3ChunkSize;
  int64_t    lc_size = tsdbLogicToFileSize(to->size, rtner->szPage) - chunksize * (to->lcn - 1);
  int64_t    n = taosFSendFile(fdTo, fdFrom, 0, lc_size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  taosCloseFile(&fdFrom);
  taosCloseFile(&fdTo);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed, code:%d, line:%d", TD_VID(rtner->tsdb->pVnode), __func__, code, lino);
    if (fdFrom) taosCloseFile(&fdFrom);
    if (fdTo) taosCloseFile(&fdTo);
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
    tsdbError("vgId:%d, %s failed, code:%d, line:%d", TD_VID(rtner->tsdb->pVnode), __func__, code, lino);
    taosCloseFile(&fdFrom);
    taosCloseFile(&fdTo);
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

  code = TARRAY2_APPEND(&rtner->fopArr, op);
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
              .lcn = lcn,
              .stt[0] =
                  {
                      .level = fobj->f->stt[0].level,
                  },
          },
  };

  code = TARRAY2_APPEND(&rtner->fopArr, op);
  TSDB_CHECK_CODE(code, lino, _exit);

  // do copy the file

  if (lcn < 1) {
    code = tsdbDoCopyFile(rtner, fobj, &op.nf);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = tsdbDoCopyFileLC(rtner, fobj, &op.nf);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed, code:%d, line:%d", TD_VID(rtner->tsdb->pVnode), __func__, code, lino);
  }
  return code;
}

typedef struct {
  STsdb  *tsdb;
  int64_t now;
  int32_t fid;
  bool    s3Migrate;
} SRtnArg;

static int32_t tsdbDoRetentionEnd(SRTNer *rtner) {
  int32_t code = 0;
  int32_t lino = 0;

  if (TARRAY2_SIZE(&rtner->fopArr) > 0) {
    code = tsdbFSEditBegin(rtner->tsdb->pFS, &rtner->fopArr, TSDB_FEDIT_RETENTION);
    TSDB_CHECK_CODE(code, lino, _exit);

    taosThreadMutexLock(&rtner->tsdb->mutex);

    code = tsdbFSEditCommit(rtner->tsdb->pFS);
    if (code) {
      taosThreadMutexUnlock(&rtner->tsdb->mutex);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    taosThreadMutexUnlock(&rtner->tsdb->mutex);

    TARRAY2_DESTROY(&rtner->fopArr, NULL);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed, code:%d, line:%d", TD_VID(rtner->tsdb->pVnode), __func__, code, lino);
  } else {
    tsdbDebug("vid:%d, cid:%" PRId64 ", %s done", TD_VID(rtner->tsdb->pVnode), rtner->cid, __func__);
  }
  return code;
}

static int32_t tsdbDoRetention(SRTNer *rtner) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STFileObj *fobj = NULL;
  STFileSet *fset = rtner->fset;
  int32_t    expLevel = tsdbFidLevel(fset->fid, &rtner->tsdb->keepCfg, rtner->now);

  if (expLevel < 0) {  // remove the fileset
    for (int32_t ftype = 0; (ftype < TSDB_FTYPE_MAX) && (fobj = fset->farr[ftype], 1); ++ftype) {
      if (fobj == NULL) continue;
      code = tsdbDoRemoveFileObject(rtner, fobj);
      TSDB_CHECK_CODE(code, lino, _exit);
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

      if (fobj->f->did.level == did.level) {
        continue;
      }

      if (fobj->f->did.level > did.level) {
        continue;
      }
      tsdbInfo("file:%s size: %" PRId64 " do migrate from %d to %d", fobj->fname, fobj->f->size, fobj->f->did.level,
               did.level);

      code = tsdbDoMigrateFileObj(rtner, fobj, &did);
      TSDB_CHECK_CODE(code, lino, _exit);
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
    tsdbError("vgId:%d, %s failed, code:%d, line:%d", TD_VID(rtner->tsdb->pVnode), __func__, code, lino);
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
  };

  // begin task
  taosThreadMutexLock(&pTsdb->mutex);
  tsdbBeginTaskOnFileSet(pTsdb, rtnArg->fid, &fset);
  if (fset && (code = tsdbTFileSetInitCopy(pTsdb, fset, &rtner.fset))) {
    taosThreadMutexUnlock(&pTsdb->mutex);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  taosThreadMutexUnlock(&pTsdb->mutex);

  // do retention
  if (rtner.fset) {
    if (rtnArg->s3Migrate) {
      code = tsdbDoS3Migrate(&rtner);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbDoRetention(&rtner);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbDoRetentionEnd(&rtner);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (rtner.fset) {
    taosThreadMutexLock(&pTsdb->mutex);
    tsdbFinishTaskOnFileSet(pTsdb, rtnArg->fid);
    taosThreadMutexUnlock(&pTsdb->mutex);
  }

  // clear resources
  tsdbTFileSetClear(&rtner.fset);
  TARRAY2_DESTROY(&rtner.fopArr, NULL);
  taosMemoryFree(arg);
  if (code) {
    tsdbError("vgId:%d, %s failed, code:%d, line:%d", TD_VID(((SRtnArg *)arg)->tsdb->pVnode), __func__, code, lino);
  }
  return code;
}

static int32_t tsdbAsyncRetentionImpl(STsdb *tsdb, int64_t now, bool s3Migrate) {
  int32_t code = 0;
  int32_t lino = 0;

  STFileSet *fset;

  if (!tsdb->bgTaskDisabled) {
    TARRAY2_FOREACH(tsdb->pFS->fSetArr, fset) {
      code = tsdbTFileSetOpenChannel(fset);
      TSDB_CHECK_CODE(code, lino, _exit);

      SRtnArg *arg = taosMemoryMalloc(sizeof(*arg));
      if (arg == NULL) {
        TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
      }

      arg->tsdb = tsdb;
      arg->now = now;
      arg->fid = fset->fid;
      arg->s3Migrate = s3Migrate;

      if ((code = vnodeAsync(&fset->channel, EVA_PRIORITY_LOW, tsdbRetention, tsdbRetentionCancel, arg, NULL))) {
        taosMemoryFree(arg);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed, code:%d, line:%d", TD_VID(tsdb->pVnode), __func__, code, lino);
  }
  return code;
}

int32_t tsdbAsyncRetention(STsdb *tsdb, int64_t now) {
  int32_t code = 0;
  taosThreadMutexLock(&tsdb->mutex);
  code = tsdbAsyncRetentionImpl(tsdb, now, false);
  taosThreadMutexUnlock(&tsdb->mutex);
  return code;
}

static int32_t tsdbS3FidLevel(int32_t fid, STsdbKeepCfg *pKeepCfg, int32_t s3KeepLocal, int64_t nowSec) {
  int32_t localFid;
  TSKEY   key;

  if (pKeepCfg->precision == TSDB_TIME_PRECISION_MILLI) {
    nowSec = nowSec * 1000;
  } else if (pKeepCfg->precision == TSDB_TIME_PRECISION_MICRO) {
    nowSec = nowSec * 1000000l;
  } else if (pKeepCfg->precision == TSDB_TIME_PRECISION_NANO) {
    nowSec = nowSec * 1000000000l;
  } else {
    ASSERT(0);
  }

  nowSec = nowSec - pKeepCfg->keepTimeOffset * tsTickPerHour[pKeepCfg->precision];

  key = nowSec - s3KeepLocal * tsTickPerMin[pKeepCfg->precision];
  localFid = tsdbKeyFid(key, pKeepCfg->days, pKeepCfg->precision);

  if (fid >= localFid) {
    return 0;
  } else {
    return 1;
  }
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

static int32_t tsdbMigrateDataFileLCS3(SRTNer *rtner, const STFileObj *fobj, int64_t size, int64_t chunksize) {
  int32_t   code = 0;
  int32_t   lino = 0;
  STFileOp  op = {0};
  TdFilePtr fdFrom = NULL, fdTo = NULL;
  int32_t   lcn = fobj->f->lcn + (size - 1) / chunksize;

  // remove old
  op = (STFileOp){
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  code = TARRAY2_APPEND(&rtner->fopArr, op);
  TSDB_CHECK_CODE(code, lino, _exit);

  // create new
  op = (STFileOp){
      .optype = TSDB_FOP_CREATE,
      .fid = fobj->f->fid,
      .nf =
          {
              .type = fobj->f->type,
              .did = fobj->f->did,
              .fid = fobj->f->fid,
              .minVer = fobj->f->minVer,
              .maxVer = fobj->f->maxVer,
              .cid = fobj->f->cid,
              .size = fobj->f->size,
              .lcn = lcn,
              .stt[0] =
                  {
                      .level = fobj->f->stt[0].level,
                  },
          },
  };

  code = TARRAY2_APPEND(&rtner->fopArr, op);
  TSDB_CHECK_CODE(code, lino, _exit);

  char fname[TSDB_FILENAME_LEN];
  tsdbTFileName(rtner->tsdb, &op.nf, fname);
  char   *object_name = taosDirEntryBaseName(fname);
  char    object_name_prefix[TSDB_FILENAME_LEN];
  int32_t node_id = vnodeNodeId(rtner->tsdb->pVnode);
  snprintf(object_name_prefix, TSDB_FQDN_LEN, "%d/%s", node_id, object_name);

  char *dot = strrchr(object_name_prefix, '.');
  if (!dot) {
    tsdbError("vgId:%d, incorrect lcn: %d, %s at line %d", TD_VID(rtner->tsdb->pVnode), lcn, __func__, lino);
    return -1;
  }

  char *dot2 = strchr(object_name, '.');
  if (!dot) {
    tsdbError("vgId:%d, incorrect lcn: %d, %s at line %d", TD_VID(rtner->tsdb->pVnode), lcn, __func__, lino);
    return -1;
  }
  snprintf(dot2 + 1, TSDB_FQDN_LEN - (dot2 + 1 - object_name), "%d.data", fobj->f->lcn);

  // do copy the file
  for (int32_t cn = fobj->f->lcn; cn < lcn; ++cn) {
    snprintf(dot + 1, TSDB_FQDN_LEN - (dot + 1 - object_name_prefix), "%d.data", cn);
    int64_t c_offset = chunksize * (cn - fobj->f->lcn);

    code = s3PutObjectFromFileOffset(fname, object_name_prefix, c_offset, chunksize);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // copy last chunk
  int64_t lc_offset = chunksize * (lcn - fobj->f->lcn);
  int64_t lc_size = size - lc_offset;

  snprintf(dot2 + 1, TSDB_FQDN_LEN - (dot2 + 1 - object_name), "%d.data", fobj->f->lcn);

  fdFrom = taosOpenFile(fname, TD_FILE_READ);
  if (fdFrom == NULL) code = terrno;
  TSDB_CHECK_CODE(code, lino, _exit);

  tsdbInfo("vgId:%d, open lcfile: %s size: %" PRId64, TD_VID(rtner->tsdb->pVnode), fname, lc_size);

  snprintf(dot2 + 1, TSDB_FQDN_LEN - (dot2 + 1 - object_name), "%d.data", lcn);
  fdTo = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fdTo == NULL) code = terrno;
  TSDB_CHECK_CODE(code, lino, _exit);

  int64_t n = taosFSendFile(fdTo, fdFrom, &lc_offset, lc_size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  taosCloseFile(&fdFrom);
  taosCloseFile(&fdTo);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtner->tsdb->pVnode), lino, code);
    if (fdFrom) taosCloseFile(&fdFrom);
    if (fdTo) taosCloseFile(&fdTo);
  }
  return code;
}

static int32_t tsdbMigrateDataFileS3(SRTNer *rtner, const STFileObj *fobj, int64_t size, int64_t chunksize) {
  int32_t   code = 0;
  int32_t   lino = 0;
  STFileOp  op = {0};
  int32_t   lcn = (size - 1) / chunksize + 1;
  TdFilePtr fdFrom = NULL, fdTo = NULL;

  // remove old
  op = (STFileOp){
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  code = TARRAY2_APPEND(&rtner->fopArr, op);
  TSDB_CHECK_CODE(code, lino, _exit);

  // create new
  op = (STFileOp){
      .optype = TSDB_FOP_CREATE,
      .fid = fobj->f->fid,
      .nf =
          {
              .type = fobj->f->type,
              .did = fobj->f->did,
              .fid = fobj->f->fid,
              .minVer = fobj->f->minVer,
              .maxVer = fobj->f->maxVer,
              .cid = fobj->f->cid,
              .size = fobj->f->size,
              .lcn = lcn,
              .stt[0] =
                  {
                      .level = fobj->f->stt[0].level,
                  },
          },
  };

  code = TARRAY2_APPEND(&rtner->fopArr, op);
  TSDB_CHECK_CODE(code, lino, _exit);

  char fname[TSDB_FILENAME_LEN];
  tsdbTFileName(rtner->tsdb, &op.nf, fname);
  char   *object_name = taosDirEntryBaseName(fname);
  char    object_name_prefix[TSDB_FILENAME_LEN];
  int32_t node_id = vnodeNodeId(rtner->tsdb->pVnode);
  snprintf(object_name_prefix, TSDB_FQDN_LEN, "%d/%s", node_id, object_name);

  char *dot = strrchr(object_name_prefix, '.');
  if (!dot) {
    tsdbError("vgId:%d, incorrect lcn: %d, %s at line %d", TD_VID(rtner->tsdb->pVnode), lcn, __func__, lino);
    return -1;
  }

  // do copy the file
  for (int32_t cn = 1; cn < lcn; ++cn) {
    snprintf(dot + 1, TSDB_FQDN_LEN - (dot + 1 - object_name_prefix), "%d.data", cn);
    int64_t c_offset = chunksize * (cn - 1);

    code = s3PutObjectFromFileOffset(fobj->fname, object_name_prefix, c_offset, chunksize);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // copy last chunk
  int64_t lc_offset = (int64_t)(lcn - 1) * chunksize;
  int64_t lc_size = size - lc_offset;

  dot = strchr(object_name, '.');
  if (!dot) {
    tsdbError("vgId:%d, incorrect lcn: %d, %s at line %d", TD_VID(rtner->tsdb->pVnode), lcn, __func__, lino);
    return -1;
  }
  snprintf(dot + 1, TSDB_FQDN_LEN - (dot + 1 - object_name), "%d.data", lcn);

  fdFrom = taosOpenFile(fobj->fname, TD_FILE_READ);
  if (fdFrom == NULL) code = terrno;
  TSDB_CHECK_CODE(code, lino, _exit);

  tsdbInfo("vgId: %d, open lcfile: %s size: %" PRId64, TD_VID(rtner->tsdb->pVnode), fname, fobj->f->size);

  fdTo = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fdTo == NULL) code = terrno;
  TSDB_CHECK_CODE(code, lino, _exit);

  int64_t n = taosFSendFile(fdTo, fdFrom, &lc_offset, lc_size);
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

static int32_t tsdbDoS3Migrate(SRTNer *rtner) {
  int32_t code = 0;
  int32_t lino = 0;

  STFileSet *fset = rtner->fset;
  STFileObj *fobj = fset->farr[TSDB_FTYPE_DATA];
  if (!fobj) return code;

  int32_t expLevel = tsdbFidLevel(fset->fid, &rtner->tsdb->keepCfg, rtner->now);
  if (expLevel < 0) return code;  // expired

  SVnodeCfg *pCfg = &rtner->tsdb->pVnode->config;
  int32_t    s3KeepLocal = pCfg->s3KeepLocal;
  int32_t    s3ExpLevel = tsdbS3FidLevel(fset->fid, &rtner->tsdb->keepCfg, s3KeepLocal, rtner->now);
  if (s3ExpLevel < 1) return code;  // keep on local storage

  int64_t chunksize = (int64_t)pCfg->tsdbPageSize * pCfg->s3ChunkSize;
  int32_t lcn = fobj->f->lcn;

  if (/*lcn < 1 && */ taosCheckExistFile(fobj->fname)) {
    int32_t mtime = 0;
    int64_t size = 0;
    taosStatFile(fobj->fname, &size, &mtime, NULL);
    if (size > chunksize && mtime < rtner->now - tsS3UploadDelaySec) {
      if (pCfg->s3Compact && lcn < 0) {
        extern int32_t tsdbAsyncCompact(STsdb * tsdb, const STimeWindow *tw, bool sync);

        STimeWindow win = {0};
        tsdbFidKeyRange(fset->fid, rtner->tsdb->keepCfg.days, rtner->tsdb->keepCfg.precision, &win.skey, &win.ekey);

        tsdbInfo("vgId:%d, compact begin lcn: %d.", TD_VID(rtner->tsdb->pVnode), lcn);
        tsdbAsyncCompact(rtner->tsdb, &win, pCfg->sttTrigger == 1);
        tsdbInfo("vgId:%d, compact end lcn: %d.", TD_VID(rtner->tsdb->pVnode), lcn);
        return code;
      }

      code = tsdbMigrateDataFileS3(rtner, fobj, size, chunksize);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  } else {
    if (lcn <= 1) {
      tsdbError("vgId:%d, incorrect lcn: %d, %s at line %d", TD_VID(rtner->tsdb->pVnode), lcn, __func__, lino);
      return code;
    }
    char fname1[TSDB_FILENAME_LEN];
    tsdbTFileLastChunkName(rtner->tsdb, fobj->f, fname1);

    if (taosCheckExistFile(fname1)) {
      int32_t mtime = 0;
      int64_t size = 0;
      taosStatFile(fname1, &size, &mtime, NULL);
      if (size > chunksize && mtime < rtner->now - tsS3UploadDelaySec) {
        code = tsdbMigrateDataFileLCS3(rtner, fobj, size, chunksize);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    } else {
      tsdbError("vgId:%d, file: %s not found, %s at line %d", TD_VID(rtner->tsdb->pVnode), fname1, __func__, lino);
      return code;
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtner->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbAsyncS3Migrate(STsdb *tsdb, int64_t now) {
  int32_t code = 0;

  extern int8_t tsS3EnabledCfg;

  int32_t expired = grantCheck(TSDB_GRANT_OBJECT_STORAGE);
  if (expired && tsS3Enabled) {
    tsdbWarn("s3 grant expired: %d", expired);
    tsS3Enabled = false;
  } else if (!expired && tsS3EnabledCfg) {
    tsS3Enabled = true;
  }

  if (!tsS3Enabled) {
    return code;
  }

  taosThreadMutexLock(&tsdb->mutex);
  code = tsdbAsyncRetentionImpl(tsdb, now, true);
  taosThreadMutexUnlock(&tsdb->mutex);
  return code;
}
