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

typedef struct {
  STsdb  *tsdb;
  int32_t szPage;
  int64_t now;
  int64_t cid;

  TFileSetArray *fsetArr;
  TFileOpArray   fopArr[1];

  struct {
    int32_t    fsetArrIdx;
    STFileSet *fset;
  } ctx[1];
} SRTNer;

static int32_t tsdbDoRemoveFileObject(SRTNer *rtner, const STFileObj *fobj) {
  STFileOp op = {
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  return TARRAY2_APPEND(rtner->fopArr, op);
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
              .cid = rtner->cid,
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

typedef struct {
  STsdb  *tsdb;
  int64_t now;
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
    tsdbInfo("vid:%d, cid:%" PRId64 ", %s done", TD_VID(rtner->tsdb->pVnode), rtner->cid, __func__);
  }
  return code;
}

static int32_t tsdbDoRetentionEnd(SRTNer *rtner) {
  int32_t code = 0;
  int32_t lino = 0;

  if (TARRAY2_SIZE(rtner->fopArr) == 0) goto _exit;

  code = tsdbFSEditBegin(rtner->tsdb->pFS, rtner->fopArr, TSDB_FEDIT_MERGE);
  TSDB_CHECK_CODE(code, lino, _exit);

  taosThreadRwlockWrlock(&rtner->tsdb->rwLock);

  code = tsdbFSEditCommit(rtner->tsdb->pFS);
  if (code) {
    taosThreadRwlockUnlock(&rtner->tsdb->rwLock);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  taosThreadRwlockUnlock(&rtner->tsdb->rwLock);

  TARRAY2_DESTROY(rtner->fopArr, NULL);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtner->tsdb->pVnode), lino, code);
  } else {
    tsdbInfo("vid:%d, cid:%" PRId64 ", %s done", TD_VID(rtner->tsdb->pVnode), rtner->cid, __func__);
  }
  tsdbFSDestroyCopySnapshot(&rtner->fsetArr);
  return code;
}

static int32_t tsdbDoRetention2(void *arg) {
  int32_t code = 0;
  int32_t lino = 0;
  SRTNer  rtner[1] = {0};

  code = tsdbDoRetentionBegin(arg, rtner);
  TSDB_CHECK_CODE(code, lino, _exit);

  for (rtner->ctx->fsetArrIdx = 0; rtner->ctx->fsetArrIdx < TARRAY2_SIZE(rtner->fsetArr); rtner->ctx->fsetArrIdx++) {
    rtner->ctx->fset = TARRAY2_GET(rtner->fsetArr, rtner->ctx->fsetArrIdx);

    STFileObj *fobj;
    int32_t    expLevel = tsdbFidLevel(rtner->ctx->fset->fid, &rtner->tsdb->keepCfg, rtner->now);

    if (expLevel < 0) {  // remove the file set
      for (int32_t ftype = 0; (ftype < TSDB_FTYPE_MAX) && (fobj = rtner->ctx->fset->farr[ftype], 1); ++ftype) {
        if (fobj == NULL) continue;

        code = tsdbDoRemoveFileObject(rtner, fobj);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      SSttLvl *lvl;
      TARRAY2_FOREACH(rtner->ctx->fset->lvlArr, lvl) {
        TARRAY2_FOREACH(lvl->fobjArr, fobj) {
          code = tsdbDoRemoveFileObject(rtner, fobj);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }
    } else if (expLevel == 0) {
      continue;
    } else {
      SDiskID did;

      if (tfsAllocDisk(rtner->tsdb->pVnode->pTfs, expLevel, &did) < 0) {
        code = terrno;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      tfsMkdirRecurAt(rtner->tsdb->pVnode->pTfs, rtner->tsdb->path, did);

      // data
      for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX && (fobj = rtner->ctx->fset->farr[ftype], 1); ++ftype) {
        if (fobj == NULL) continue;

        if (fobj->f->did.level == did.level) continue;
        code = tsdbDoMigrateFileObj(rtner, fobj, &did);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      // stt
      SSttLvl *lvl;
      TARRAY2_FOREACH(rtner->ctx->fset->lvlArr, lvl) {
        TARRAY2_FOREACH(lvl->fobjArr, fobj) {
          if (fobj->f->did.level == did.level) continue;

          code = tsdbDoMigrateFileObj(rtner, fobj, &did);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }
    }
  }

  code = tsdbDoRetentionEnd(rtner);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(rtner->tsdb->pVnode), lino, code);
  }
  taosMemoryFree(arg);
  return code;
}

int32_t tsdbAsyncRetention(STsdb *tsdb, int64_t now, int64_t *taskid) {
  SRtnArg *arg = taosMemoryMalloc(sizeof(*arg));
  if (arg == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  arg->tsdb = tsdb;
  arg->now = now;

  int32_t code = tsdbFSScheduleBgTask(tsdb->pFS, TSDB_BG_TASK_RETENTION, tsdbDoRetention2, arg, taskid);
  if (code) taosMemoryFree(arg);

  return code;
}

int32_t tsdbSyncRetention(STsdb *tsdb, int64_t now) {
  int64_t taskid;

  int32_t code = tsdbAsyncRetention(tsdb, now, &taskid);
  if (code) return code;

  return tsdbFSWaitBgTask(tsdb->pFS, taskid);
}