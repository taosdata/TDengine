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

#include "tsdbCommit2.h"

// extern dependencies
typedef struct {
  STsdb         *tsdb;
  TFileSetArray *fsetArr;
  TFileOpArray   fopArray[1];

  // SSkmInfo skmTb[1];
  // SSkmInfo skmRow[1];

  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int32_t sttTrigger;
  int32_t szPage;
  int64_t compactVersion;

  struct {
    int64_t    cid;
    int64_t    now;
    TSKEY      nextKey;
    TSKEY      maxDelKey;
    int32_t    fid;
    int32_t    expLevel;
    SDiskID    did;
    TSKEY      minKey;
    TSKEY      maxKey;
    STFileSet *fset;
    TABLEID    tbid[1];
    bool       hasTSData;
    bool       skipTsRow;
  } ctx[1];

  // reader
  TSttFileReaderArray sttReaderArray[1];

  // iter
  TTsdbIterArray dataIterArray[1];
  SIterMerger   *dataIterMerger;
  TTsdbIterArray tombIterArray[1];
  SIterMerger   *tombIterMerger;

  // writer
  SFSetWriter *writer;
} SCommitter2;

static int32_t tsdbCommitOpenWriter(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  SFSetWriterConfig config = {
      .tsdb = committer->tsdb,
      .toSttOnly = true,
      .compactVersion = committer->compactVersion,
      .minRow = committer->minRow,
      .maxRow = committer->maxRow,
      .szPage = committer->szPage,
      .cmprAlg = committer->cmprAlg,
      .fid = committer->ctx->fid,
      .cid = committer->ctx->cid,
      .did = committer->ctx->did,
      .level = 0,
  };

  if (committer->sttTrigger == 1) {
    config.toSttOnly = false;

    if (committer->ctx->fset) {
      for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ftype++) {
        if (committer->ctx->fset->farr[ftype] != NULL) {
          config.files[ftype].exist = true;
          config.files[ftype].file = committer->ctx->fset->farr[ftype]->f[0];
        }
      }
    }
  }

  code = tsdbFSetWriterOpen(&config, &committer->writer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitCloseWriter(SCommitter2 *committer) {
  return tsdbFSetWriterClose(&committer->writer, 0, committer->fopArray);
}

static int32_t tsdbCommitTSData(SCommitter2 *committer) {
  int32_t   code = 0;
  int32_t   lino = 0;
  int64_t   numOfRow = 0;
  SMetaInfo info;

  committer->ctx->hasTSData = false;

  committer->ctx->tbid->suid = 0;
  committer->ctx->tbid->uid = 0;
  for (SRowInfo *row; (row = tsdbIterMergerGetData(committer->dataIterMerger)) != NULL;) {
    if (row->uid != committer->ctx->tbid->uid) {
      committer->ctx->tbid->suid = row->suid;
      committer->ctx->tbid->uid = row->uid;

      if (metaGetInfo(committer->tsdb->pVnode->pMeta, row->uid, &info, NULL) != 0) {
        code = tsdbIterMergerSkipTableData(committer->dataIterMerger, committer->ctx->tbid);
        TSDB_CHECK_CODE(code, lino, _exit);
        continue;
      }
    }
    /*
    extern int8_t tsS3Enabled;

    int32_t nlevel = tfsGetLevel(committer->tsdb->pVnode->pTfs);
    committer->ctx->skipTsRow = false;
    if (tsS3Enabled && nlevel > 1 && committer->ctx->did.level == nlevel - 1) {
      committer->ctx->skipTsRow = true;
    }
    */
    int64_t ts = TSDBROW_TS(&row->row);

    if (committer->ctx->skipTsRow && ts <= committer->ctx->maxKey) {
      ts = committer->ctx->maxKey + 1;
    }

    if (ts > committer->ctx->maxKey) {
      committer->ctx->nextKey = TMIN(committer->ctx->nextKey, ts);
      code = tsdbIterMergerSkipTableData(committer->dataIterMerger, committer->ctx->tbid);
      TSDB_CHECK_CODE(code, lino, _exit);
      continue;
    }

    committer->ctx->hasTSData = true;
    numOfRow++;

    code = tsdbFSetWriteRow(committer->writer, row);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbIterMergerNext(committer->dataIterMerger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d fid:%d commit %" PRId64 " rows", TD_VID(committer->tsdb->pVnode), committer->ctx->fid, numOfRow);
  }
  return code;
}

static int32_t tsdbCommitTombData(SCommitter2 *committer) {
  int32_t   code = 0;
  int32_t   lino = 0;
  int64_t   numRecord = 0;
  SMetaInfo info;

  if (committer->ctx->fset == NULL && !committer->ctx->hasTSData) {
    if (committer->ctx->maxKey < committer->ctx->maxDelKey) {
      committer->ctx->nextKey = committer->ctx->maxKey + 1;
    } else {
      committer->ctx->nextKey = TSKEY_MAX;
    }
    return 0;
  }

  committer->ctx->tbid->suid = 0;
  committer->ctx->tbid->uid = 0;
  for (STombRecord *record; (record = tsdbIterMergerGetTombRecord(committer->tombIterMerger));) {
    if (record->uid != committer->ctx->tbid->uid) {
      committer->ctx->tbid->suid = record->suid;
      committer->ctx->tbid->uid = record->uid;

      if (metaGetInfo(committer->tsdb->pVnode->pMeta, record->uid, &info, NULL) != 0) {
        code = tsdbIterMergerSkipTableData(committer->tombIterMerger, committer->ctx->tbid);
        TSDB_CHECK_CODE(code, lino, _exit);
        continue;
      }
    }

    if (record->ekey < committer->ctx->minKey) {
      // do nothing
    } else if (record->skey > committer->ctx->maxKey) {
      committer->ctx->nextKey = TMIN(record->skey, committer->ctx->nextKey);
    } else {
      if (record->ekey > committer->ctx->maxKey) {
        committer->ctx->nextKey = TMIN(committer->ctx->nextKey, committer->ctx->maxKey + 1);
      }

      record->skey = TMAX(record->skey, committer->ctx->minKey);
      record->ekey = TMIN(record->ekey, committer->ctx->maxKey);

      numRecord++;
      code = tsdbFSetWriteTombRecord(committer->writer, record);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbIterMergerNext(committer->tombIterMerger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d fid:%d commit %" PRId64 " tomb records", TD_VID(committer->tsdb->pVnode), committer->ctx->fid,
              numRecord);
  }
  return code;
}

static int32_t tsdbCommitOpenReader(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(TARRAY2_SIZE(committer->sttReaderArray) == 0);

  if (committer->ctx->fset == NULL                        //
      || committer->sttTrigger > 1                        //
      || TARRAY2_SIZE(committer->ctx->fset->lvlArr) == 0  //
  ) {
    return 0;
  }

  SSttLvl *lvl;
  TARRAY2_FOREACH(committer->ctx->fset->lvlArr, lvl) {
    STFileObj *fobj = NULL;
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      SSttFileReader *sttReader;

      SSttFileReaderConfig config = {
          .tsdb = committer->tsdb,
          .szPage = committer->szPage,
          .file = fobj->f[0],
      };

      code = tsdbSttFileReaderOpen(fobj->fname, &config, &sttReader);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = TARRAY2_APPEND(committer->sttReaderArray, sttReader);
      TSDB_CHECK_CODE(code, lino, _exit);

      STFileOp op = {
          .optype = TSDB_FOP_REMOVE,
          .fid = fobj->f->fid,
          .of = fobj->f[0],
      };

      code = TARRAY2_APPEND(committer->fopArray, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitCloseReader(SCommitter2 *committer) {
  TARRAY2_CLEAR(committer->sttReaderArray, tsdbSttFileReaderClose);
  return 0;
}

static int32_t tsdbCommitOpenIter(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(TARRAY2_SIZE(committer->dataIterArray) == 0);
  ASSERT(committer->dataIterMerger == NULL);
  ASSERT(TARRAY2_SIZE(committer->tombIterArray) == 0);
  ASSERT(committer->tombIterMerger == NULL);

  STsdbIter      *iter;
  STsdbIterConfig config = {0};

  // mem data iter
  config.type = TSDB_ITER_TYPE_MEMT;
  config.memt = committer->tsdb->imem;
  config.from->ts = committer->ctx->minKey;
  config.from->version = VERSION_MIN;

  code = tsdbIterOpen(&config, &iter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = TARRAY2_APPEND(committer->dataIterArray, iter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // mem tomb iter
  config.type = TSDB_ITER_TYPE_MEMT_TOMB;
  config.memt = committer->tsdb->imem;

  code = tsdbIterOpen(&config, &iter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = TARRAY2_APPEND(committer->tombIterArray, iter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // STT
  SSttFileReader *sttReader;
  TARRAY2_FOREACH(committer->sttReaderArray, sttReader) {
    // data iter
    config.type = TSDB_ITER_TYPE_STT;
    config.sttReader = sttReader;

    code = tsdbIterOpen(&config, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(committer->dataIterArray, iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    // tomb iter
    config.type = TSDB_ITER_TYPE_STT_TOMB;
    config.sttReader = sttReader;

    code = tsdbIterOpen(&config, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(committer->tombIterArray, iter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // open merger
  code = tsdbIterMergerOpen(committer->dataIterArray, &committer->dataIterMerger, false);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbIterMergerOpen(committer->tombIterArray, &committer->tombIterMerger, true);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitCloseIter(SCommitter2 *committer) {
  tsdbIterMergerClose(&committer->tombIterMerger);
  tsdbIterMergerClose(&committer->dataIterMerger);
  TARRAY2_CLEAR(committer->tombIterArray, tsdbIterClose);
  TARRAY2_CLEAR(committer->dataIterArray, tsdbIterClose);
  return 0;
}

static int32_t tsdbCommitFileSetBegin(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *tsdb = committer->tsdb;

  int32_t fid = tsdbKeyFid(committer->ctx->nextKey, committer->minutes, committer->precision);

  // check if can commit
  tsdbFSCheckCommit(tsdb, fid);

  committer->ctx->fid = fid;
  committer->ctx->expLevel = tsdbFidLevel(committer->ctx->fid, &tsdb->keepCfg, committer->ctx->now);
  tsdbFidKeyRange(committer->ctx->fid, committer->minutes, committer->precision, &committer->ctx->minKey,
                  &committer->ctx->maxKey);
  code = tfsAllocDisk(committer->tsdb->pVnode->pTfs, committer->ctx->expLevel, &committer->ctx->did);
  TSDB_CHECK_CODE(code, lino, _exit);
  tfsMkdirRecurAt(committer->tsdb->pVnode->pTfs, committer->tsdb->path, committer->ctx->did);
  STFileSet fset = {.fid = committer->ctx->fid};
  committer->ctx->fset = &fset;
  STFileSet **fsetPtr = TARRAY2_SEARCH(committer->fsetArr, &committer->ctx->fset, tsdbTFileSetCmprFn, TD_EQ);
  committer->ctx->fset = (fsetPtr == NULL) ? NULL : *fsetPtr;
  committer->ctx->tbid->suid = 0;
  committer->ctx->tbid->uid = 0;

  ASSERT(TARRAY2_SIZE(committer->dataIterArray) == 0);
  ASSERT(committer->dataIterMerger == NULL);
  ASSERT(committer->writer == NULL);

  code = tsdbCommitOpenReader(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCommitOpenIter(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCommitOpenWriter(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  // reset nextKey
  committer->ctx->nextKey = TSKEY_MAX;

  committer->ctx->skipTsRow = false;

  extern int8_t  tsS3Enabled;
  extern int32_t tsS3UploadDelaySec;
  long           s3Size(const char *object_name);
  int32_t        nlevel = tfsGetLevel(committer->tsdb->pVnode->pTfs);
  if (tsS3Enabled && nlevel > 1 && committer->ctx->fset) {
    STFileObj *fobj = committer->ctx->fset->farr[TSDB_FTYPE_DATA];
    if (fobj && fobj->f->did.level == nlevel - 1) {
      // if exists on s3 or local mtime < committer->ctx->now - tsS3UploadDelay
      const char *object_name = taosDirEntryBaseName((char *)fobj->fname);

      if (taosCheckExistFile(fobj->fname)) {
        int32_t mtime = 0;
        taosStatFile(fobj->fname, NULL, &mtime, NULL);
        if (mtime < committer->ctx->now - tsS3UploadDelaySec) {
          committer->ctx->skipTsRow = true;
        }
      } else /*if (s3Size(object_name) > 0) */ {
        committer->ctx->skipTsRow = true;
      }
    }
    // new fset can be written with ts data
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d minKey:%" PRId64 " maxKey:%" PRId64 " expLevel:%d", TD_VID(tsdb->pVnode),
              __func__, committer->ctx->fid, committer->ctx->minKey, committer->ctx->maxKey, committer->ctx->expLevel);
  }
  return code;
}

static int32_t tsdbCommitFileSetEnd(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbCommitCloseWriter(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCommitCloseIter(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCommitCloseReader(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(committer->tsdb->pVnode), __func__, committer->ctx->fid);
  }
  return code;
}

static int32_t tsdbCommitFileSet(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  // fset commit start
  code = tsdbCommitFileSetBegin(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  // commit fset
  code = tsdbCommitTSData(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCommitTombData(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  // fset commit end
  code = tsdbCommitFileSetEnd(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(committer->tsdb->pVnode), __func__, committer->ctx->fid);
  }
  return code;
}

static int32_t tsdbOpenCommitter(STsdb *tsdb, SCommitInfo *info, SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  memset(committer, 0, sizeof(committer[0]));

  committer->tsdb = tsdb;
  code = tsdbFSCreateCopySnapshot(tsdb->pFS, &committer->fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);
  committer->minutes = tsdb->keepCfg.days;
  committer->precision = tsdb->keepCfg.precision;
  committer->minRow = info->info.config.tsdbCfg.minRows;
  committer->maxRow = info->info.config.tsdbCfg.maxRows;
  committer->cmprAlg = info->info.config.tsdbCfg.compression;
  committer->sttTrigger = info->info.config.sttTrigger;
  committer->szPage = info->info.config.tsdbPageSize;
  committer->compactVersion = INT64_MAX;
  committer->ctx->cid = tsdbFSAllocEid(tsdb->pFS);
  committer->ctx->now = taosGetTimestampSec();

  committer->ctx->nextKey = tsdb->imem->minKey;
  if (tsdb->imem->nDel > 0) {
    SRBTreeIter iter[1] = {tRBTreeIterCreate(tsdb->imem->tbDataTree, 1)};

    for (SRBTreeNode *node = tRBTreeIterNext(iter); node; node = tRBTreeIterNext(iter)) {
      STbData *tbData = TCONTAINER_OF(node, STbData, rbtn);

      for (SDelData *delData = tbData->pHead; delData; delData = delData->pNext) {
        if (delData->sKey < committer->ctx->nextKey) {
          committer->ctx->nextKey = delData->sKey;
        }
      }
    }
  }

  committer->ctx->maxDelKey = TSKEY_MIN;
  TSKEY minKey = TSKEY_MAX;
  TSKEY maxKey = TSKEY_MIN;
  if (TARRAY2_SIZE(committer->fsetArr) > 0) {
    STFileSet *fset = TARRAY2_LAST(committer->fsetArr);
    tsdbFidKeyRange(fset->fid, committer->minutes, committer->precision, &minKey, &committer->ctx->maxDelKey);

    fset = TARRAY2_FIRST(committer->fsetArr);
    tsdbFidKeyRange(fset->fid, committer->minutes, committer->precision, &minKey, &maxKey);
  }

  if (committer->ctx->nextKey < TMIN(tsdb->imem->minKey, minKey)) {
    committer->ctx->nextKey = TMIN(tsdb->imem->minKey, minKey);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(tsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbCloseCommitter(SCommitter2 *committer, int32_t eno) {
  int32_t code = 0;
  int32_t lino = 0;

  if (eno == 0) {
    code = tsdbFSEditBegin(committer->tsdb->pFS, committer->fopArray, TSDB_FEDIT_COMMIT);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    // TODO
    ASSERT(0);
  }

  ASSERT(committer->writer == NULL);
  ASSERT(committer->dataIterMerger == NULL);
  ASSERT(committer->tombIterMerger == NULL);
  TARRAY2_DESTROY(committer->dataIterArray, NULL);
  TARRAY2_DESTROY(committer->tombIterArray, NULL);
  TARRAY2_DESTROY(committer->sttReaderArray, NULL);
  TARRAY2_DESTROY(committer->fopArray, NULL);
  TARRAY2_DESTROY(committer->sttReaderArray, NULL);
  tsdbFSDestroyCopySnapshot(&committer->fsetArr);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, eid:%" PRId64, TD_VID(committer->tsdb->pVnode), __func__, lino,
              tstrerror(code), committer->ctx->cid);
  } else {
    tsdbDebug("vgId:%d %s done, eid:%" PRId64, TD_VID(committer->tsdb->pVnode), __func__, committer->ctx->cid);
  }
  return code;
}

int32_t tsdbPreCommit(STsdb *tsdb) {
  taosThreadMutexLock(&tsdb->mutex);
  ASSERT(tsdb->imem == NULL);
  tsdb->imem = tsdb->mem;
  tsdb->mem = NULL;
  taosThreadMutexUnlock(&tsdb->mutex);
  return 0;
}

int32_t tsdbCommitBegin(STsdb *tsdb, SCommitInfo *info) {
  if (!tsdb) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  SMemTable *imem = tsdb->imem;
  int64_t    nRow = imem->nRow;
  int64_t    nDel = imem->nDel;

  if (nRow == 0 && nDel == 0) {
    taosThreadMutexLock(&tsdb->mutex);
    tsdb->imem = NULL;
    taosThreadMutexUnlock(&tsdb->mutex);
    tsdbUnrefMemTable(imem, NULL, true);
  } else {
    SCommitter2 committer[1];

    code = tsdbOpenCommitter(tsdb, info, committer);
    TSDB_CHECK_CODE(code, lino, _exit);

    while (committer->ctx->nextKey != TSKEY_MAX) {
      code = tsdbCommitFileSet(committer);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbCloseCommitter(committer, code);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else {
    tsdbInfo("vgId:%d %s done, nRow:%" PRId64 " nDel:%" PRId64, TD_VID(tsdb->pVnode), __func__, nRow, nDel);
  }
  return code;
}

int32_t tsdbCommitCommit(STsdb *tsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tsdb->imem == NULL) goto _exit;

  SMemTable *pMemTable = tsdb->imem;
  taosThreadMutexLock(&tsdb->mutex);
  code = tsdbFSEditCommit(tsdb->pFS);
  if (code) {
    taosThreadMutexUnlock(&tsdb->mutex);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  tsdb->imem = NULL;
  taosThreadMutexUnlock(&tsdb->mutex);
  tsdbUnrefMemTable(pMemTable, NULL, true);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(tsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbCommitAbort(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pTsdb->imem == NULL) goto _exit;

  code = tsdbFSEditAbort(pTsdb->pFS);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}
