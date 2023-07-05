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

#include "tsdbMerge.h"

typedef struct {
  STsdb         *tsdb;
  TFileSetArray *fsetArr;

  int32_t sttTrigger;
  int32_t maxRow;
  int32_t minRow;
  int32_t szPage;
  int8_t  cmprAlg;
  int64_t compactVersion;
  int64_t cid;

  // context
  struct {
    bool       opened;
    int64_t    now;
    STFileSet *fset;
    bool       toData;
    int32_t    level;
    SSttLvl   *lvl;
    TABLEID    tbid[1];
  } ctx[1];

  TFileOpArray fopArr[1];

  // reader
  TSttFileReaderArray sttReaderArr[1];
  // iter
  TTsdbIterArray dataIterArr[1];
  SIterMerger   *dataIterMerger;
  TTsdbIterArray tombIterArr[1];
  SIterMerger   *tombIterMerger;
  // writer
  SFSetWriter *writer;
} SMerger;

static int32_t tsdbMergerOpen(SMerger *merger) {
  merger->ctx->now = taosGetTimestampSec();
  merger->maxRow = merger->tsdb->pVnode->config.tsdbCfg.maxRows;
  merger->minRow = merger->tsdb->pVnode->config.tsdbCfg.minRows;
  merger->szPage = merger->tsdb->pVnode->config.tsdbPageSize;
  merger->cmprAlg = merger->tsdb->pVnode->config.tsdbCfg.compression;
  merger->compactVersion = INT64_MAX;
  merger->cid = tsdbFSAllocEid(merger->tsdb->pFS);
  merger->ctx->opened = true;
  return 0;
}

static int32_t tsdbMergerClose(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  SVnode *pVnode = merger->tsdb->pVnode;

  // edit file system
  code = tsdbFSEditBegin(merger->tsdb->pFS, merger->fopArr, TSDB_FEDIT_MERGE);
  TSDB_CHECK_CODE(code, lino, _exit);

  taosThreadRwlockWrlock(&merger->tsdb->rwLock);
  code = tsdbFSEditCommit(merger->tsdb->pFS);
  if (code) {
    taosThreadRwlockUnlock(&merger->tsdb->rwLock);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  taosThreadRwlockUnlock(&merger->tsdb->rwLock);

  ASSERT(merger->writer == NULL);
  ASSERT(merger->dataIterMerger == NULL);
  ASSERT(merger->tombIterMerger == NULL);
  ASSERT(TARRAY2_SIZE(merger->dataIterArr) == 0);
  ASSERT(TARRAY2_SIZE(merger->tombIterArr) == 0);
  ASSERT(TARRAY2_SIZE(merger->sttReaderArr) == 0);

  // clear the merge
  TARRAY2_DESTROY(merger->tombIterArr, NULL);
  TARRAY2_DESTROY(merger->dataIterArr, NULL);
  TARRAY2_DESTROY(merger->sttReaderArr, NULL);
  TARRAY2_DESTROY(merger->fopArr, NULL);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbMergeFileSetBeginOpenReader(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;

  merger->ctx->toData = true;
  merger->ctx->level = 0;

  // TODO: optimize merge strategy
  for (int32_t i = 0;; ++i) {
    if (i >= TARRAY2_SIZE(merger->ctx->fset->lvlArr)) {
      merger->ctx->lvl = NULL;
      break;
    }

    merger->ctx->lvl = TARRAY2_GET(merger->ctx->fset->lvlArr, i);
    if (merger->ctx->lvl->level != merger->ctx->level ||
        TARRAY2_SIZE(merger->ctx->lvl->fobjArr) + 1 < merger->sttTrigger) {
      merger->ctx->toData = false;
      merger->ctx->lvl = NULL;
      break;
    }

    merger->ctx->level++;

    STFileObj *fobj;
    int32_t    numFile = 0;
    TARRAY2_FOREACH(merger->ctx->lvl->fobjArr, fobj) {
      if (numFile == merger->sttTrigger) {
        break;
      }

      STFileOp op = {
          .optype = TSDB_FOP_REMOVE,
          .fid = merger->ctx->fset->fid,
          .of = fobj->f[0],
      };
      code = TARRAY2_APPEND(merger->fopArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);

      SSttFileReader      *reader;
      SSttFileReaderConfig config = {
          .tsdb = merger->tsdb,
          .szPage = merger->szPage,
          .file[0] = fobj->f[0],
      };

      code = tsdbSttFileReaderOpen(fobj->fname, &config, &reader);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = TARRAY2_APPEND(merger->sttReaderArr, reader);
      TSDB_CHECK_CODE(code, lino, _exit);

      numFile++;
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(merger->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbMergeFileSetBeginOpenIter(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  SSttFileReader *sttReader;
  TARRAY2_FOREACH(merger->sttReaderArr, sttReader) {
    STsdbIter      *iter;
    STsdbIterConfig config = {0};

    // data iter
    config.type = TSDB_ITER_TYPE_STT;
    config.sttReader = sttReader;

    code = tsdbIterOpen(&config, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(merger->dataIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    // tomb iter
    config.type = TSDB_ITER_TYPE_STT_TOMB;
    config.sttReader = sttReader;

    code = tsdbIterOpen(&config, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(merger->tombIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbIterMergerOpen(merger->dataIterArr, &merger->dataIterMerger, false);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbIterMergerOpen(merger->tombIterArr, &merger->tombIterMerger, true);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

static int32_t tsdbMergeFileSetBeginOpenWriter(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  SDiskID did;
  int32_t level = tsdbFidLevel(merger->ctx->fset->fid, &merger->tsdb->keepCfg, merger->ctx->now);
  if (tfsAllocDisk(merger->tsdb->pVnode->pTfs, level, &did) < 0) {
    code = TSDB_CODE_FS_NO_VALID_DISK;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  tfsMkdirRecurAt(merger->tsdb->pVnode->pTfs, merger->tsdb->path, did);
  SFSetWriterConfig config = {
      .tsdb = merger->tsdb,
      .toSttOnly = true,
      .compactVersion = merger->compactVersion,
      .minRow = merger->minRow,
      .maxRow = merger->maxRow,
      .szPage = merger->szPage,
      .cmprAlg = merger->cmprAlg,
      .fid = merger->ctx->fset->fid,
      .cid = merger->cid,
      .did = did,
      .level = merger->ctx->level,
  };

  if (merger->ctx->toData) {
    config.toSttOnly = false;

    for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ++ftype) {
      if (merger->ctx->fset->farr[ftype]) {
        config.files[ftype].exist = true;
        config.files[ftype].file = merger->ctx->fset->farr[ftype]->f[0];
      } else {
        config.files[ftype].exist = false;
      }
    }
  }

  code = tsdbFSetWriterOpen(&config, &merger->writer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

static int32_t tsdbMergeFileSetBegin(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(TARRAY2_SIZE(merger->sttReaderArr) == 0);
  ASSERT(TARRAY2_SIZE(merger->dataIterArr) == 0);
  ASSERT(merger->dataIterMerger == NULL);
  ASSERT(merger->writer == NULL);

  merger->ctx->tbid->suid = 0;
  merger->ctx->tbid->uid = 0;

  // open reader
  code = tsdbMergeFileSetBeginOpenReader(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open iterator
  code = tsdbMergeFileSetBeginOpenIter(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open writer
  code = tsdbMergeFileSetBeginOpenWriter(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(merger->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbMergeFileSetEndCloseWriter(SMerger *merger) {
  return tsdbFSetWriterClose(&merger->writer, 0, merger->fopArr);
}

static int32_t tsdbMergeFileSetEndCloseIter(SMerger *merger) {
  tsdbIterMergerClose(&merger->tombIterMerger);
  TARRAY2_CLEAR(merger->tombIterArr, tsdbIterClose);
  tsdbIterMergerClose(&merger->dataIterMerger);
  TARRAY2_CLEAR(merger->dataIterArr, tsdbIterClose);
  return 0;
}

static int32_t tsdbMergeFileSetEndCloseReader(SMerger *merger) {
  TARRAY2_CLEAR(merger->sttReaderArr, tsdbSttFileReaderClose);
  return 0;
}

static int32_t tsdbMergeFileSetEnd(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbMergeFileSetEndCloseWriter(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbMergeFileSetEndCloseIter(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbMergeFileSetEndCloseReader(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(merger->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbMergeFileSet(SMerger *merger, STFileSet *fset) {
  int32_t code = 0;
  int32_t lino = 0;

  merger->ctx->fset = fset;
  code = tsdbMergeFileSetBegin(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

  // data
  SMetaInfo info;
  SRowInfo *row;
  merger->ctx->tbid->suid = 0;
  merger->ctx->tbid->uid = 0;
  while ((row = tsdbIterMergerGetData(merger->dataIterMerger)) != NULL) {
    if (row->uid != merger->ctx->tbid->uid) {
      merger->ctx->tbid->uid = row->uid;
      merger->ctx->tbid->suid = row->suid;

      if (metaGetInfo(merger->tsdb->pVnode->pMeta, row->uid, &info, NULL) != 0) {
        code = tsdbIterMergerSkipTableData(merger->dataIterMerger, merger->ctx->tbid);
        TSDB_CHECK_CODE(code, lino, _exit);
        continue;
      }
    }

    code = tsdbFSetWriteRow(merger->writer, row);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbIterMergerNext(merger->dataIterMerger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // tomb
  merger->ctx->tbid->suid = 0;
  merger->ctx->tbid->uid = 0;
  for (STombRecord *record; (record = tsdbIterMergerGetTombRecord(merger->tombIterMerger)) != NULL;) {
    if (record->uid != merger->ctx->tbid->uid) {
      merger->ctx->tbid->uid = record->uid;
      merger->ctx->tbid->suid = record->suid;

      if (metaGetInfo(merger->tsdb->pVnode->pMeta, record->uid, &info, NULL) != 0) {
        code = tsdbIterMergerSkipTableData(merger->tombIterMerger, merger->ctx->tbid);
        TSDB_CHECK_CODE(code, lino, _exit);
        continue;
      }
    }
    code = tsdbFSetWriteTombRecord(merger->writer, record);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbIterMergerNext(merger->tombIterMerger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbMergeFileSetEnd(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(merger->tsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(merger->tsdb->pVnode), __func__, fset->fid);
  }
  return code;
}

static int32_t tsdbDoMerge(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;

  STFileSet *fset;
  TARRAY2_FOREACH(merger->fsetArr, fset) {
    if (TARRAY2_SIZE(fset->lvlArr) == 0) continue;

    SSttLvl *lvl = TARRAY2_FIRST(fset->lvlArr);

    if (lvl->level != 0 || TARRAY2_SIZE(lvl->fobjArr) < merger->sttTrigger) continue;

    if (!merger->ctx->opened) {
      code = tsdbMergerOpen(merger);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbMergeFileSet(merger, fset);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (merger->ctx->opened) {
    code = tsdbMergerClose(merger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(merger->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(merger->tsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbMerge(void *arg) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *tsdb = (STsdb *)arg;

  SMerger merger[1] = {{
      .tsdb = tsdb,
      .sttTrigger = tsdb->pVnode->config.sttTrigger,
  }};

  ASSERT(merger->sttTrigger > 1);

  code = tsdbFSCreateCopySnapshot(tsdb->pFS, &merger->fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDoMerge(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

  tsdbFSDestroyCopySnapshot(&merger->fsetArr);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else if (merger->ctx->opened) {
    tsdbDebug("vgId:%d %s done", TD_VID(tsdb->pVnode), __func__);
  }
  return code;
}
