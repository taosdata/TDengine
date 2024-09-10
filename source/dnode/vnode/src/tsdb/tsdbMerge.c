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

#define TSDB_MAX_LEVEL 2  // means max level is 3

typedef struct {
  STsdb     *tsdb;
  int32_t    fid;
  STFileSet *fset;

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
  int32_t lino = 0;
  SVnode *pVnode = merger->tsdb->pVnode;

  // clear the merge
  TARRAY2_DESTROY(merger->tombIterArr, NULL);
  TARRAY2_DESTROY(merger->dataIterArr, NULL);
  TARRAY2_DESTROY(merger->sttReaderArr, NULL);
  TARRAY2_DESTROY(merger->fopArr, NULL);
  return 0;
}

static int32_t tsdbMergeFileSetEndCloseReader(SMerger *merger) {
  TARRAY2_CLEAR(merger->sttReaderArr, tsdbSttFileReaderClose);
  return 0;
}

static int32_t tsdbMergeFileSetBeginOpenReader(SMerger *merger) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SSttLvl *lvl;

  bool hasLevelLargerThanMax = false;
  TARRAY2_FOREACH_REVERSE(merger->ctx->fset->lvlArr, lvl) {
    if (lvl->level <= TSDB_MAX_LEVEL) {
      break;
    } else if (TARRAY2_SIZE(lvl->fobjArr) > 0) {
      hasLevelLargerThanMax = true;
      break;
    }
  }

  if (hasLevelLargerThanMax) {
    // merge all stt files
    merger->ctx->toData = true;
    merger->ctx->level = TSDB_MAX_LEVEL;

    TARRAY2_FOREACH(merger->ctx->fset->lvlArr, lvl) {
      int32_t numMergeFile = TARRAY2_SIZE(lvl->fobjArr);

      for (int32_t i = 0; i < numMergeFile; ++i) {
        STFileObj *fobj = TARRAY2_GET(lvl->fobjArr, i);

        STFileOp op = {
            .optype = TSDB_FOP_REMOVE,
            .fid = merger->ctx->fset->fid,
            .of = fobj->f[0],
        };
        TAOS_CHECK_GOTO(TARRAY2_APPEND(merger->fopArr, op), &lino, _exit);

        SSttFileReader      *reader;
        SSttFileReaderConfig config = {
            .tsdb = merger->tsdb,
            .szPage = merger->szPage,
            .file[0] = fobj->f[0],
        };

        TAOS_CHECK_GOTO(tsdbSttFileReaderOpen(fobj->fname, &config, &reader), &lino, _exit);

        TAOS_CHECK_GOTO(TARRAY2_APPEND(merger->sttReaderArr, reader), &lino, _exit);
      }
    }
  } else {
    // do regular merge
    merger->ctx->toData = true;
    merger->ctx->level = 0;

    // find the highest level that can be merged to
    for (int32_t i = 0, numCarry = 0;;) {
      int32_t numFile = numCarry;
      if (i < TARRAY2_SIZE(merger->ctx->fset->lvlArr) &&
          merger->ctx->level == TARRAY2_GET(merger->ctx->fset->lvlArr, i)->level) {
        numFile += TARRAY2_SIZE(TARRAY2_GET(merger->ctx->fset->lvlArr, i)->fobjArr);
        i++;
      }

      numCarry = numFile / merger->sttTrigger;
      if (numCarry == 0) {
        break;
      } else {
        merger->ctx->level++;
      }
    }

    if (merger->ctx->level <= TSDB_MAX_LEVEL) {
      TARRAY2_FOREACH_REVERSE(merger->ctx->fset->lvlArr, lvl) {
        if (TARRAY2_SIZE(lvl->fobjArr) == 0) {
          continue;
        }

        if (lvl->level >= merger->ctx->level) {
          merger->ctx->toData = false;
        }
        break;
      }
    }

    // get number of level-0 files to merge
    int32_t numFile = pow(merger->sttTrigger, merger->ctx->level);
    TARRAY2_FOREACH(merger->ctx->fset->lvlArr, lvl) {
      if (lvl->level == 0) continue;
      if (lvl->level >= merger->ctx->level) break;

      numFile = numFile - TARRAY2_SIZE(lvl->fobjArr) * pow(merger->sttTrigger, lvl->level);
    }

    // get file system operations
    TARRAY2_FOREACH(merger->ctx->fset->lvlArr, lvl) {
      if (lvl->level >= merger->ctx->level) {
        break;
      }

      int32_t numMergeFile;
      if (lvl->level == 0) {
        numMergeFile = numFile;
      } else {
        numMergeFile = TARRAY2_SIZE(lvl->fobjArr);
      }

      for (int32_t i = 0; i < numMergeFile; ++i) {
        STFileObj *fobj = TARRAY2_GET(lvl->fobjArr, i);

        STFileOp op = {
            .optype = TSDB_FOP_REMOVE,
            .fid = merger->ctx->fset->fid,
            .of = fobj->f[0],
        };
        TAOS_CHECK_GOTO(TARRAY2_APPEND(merger->fopArr, op), &lino, _exit);

        SSttFileReader      *reader;
        SSttFileReaderConfig config = {
            .tsdb = merger->tsdb,
            .szPage = merger->szPage,
            .file[0] = fobj->f[0],
        };

        TAOS_CHECK_GOTO(tsdbSttFileReaderOpen(fobj->fname, &config, &reader), &lino, _exit);

        if ((code = TARRAY2_APPEND(merger->sttReaderArr, reader))) {
          tsdbSttFileReaderClose(&reader);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }
    }

    if (merger->ctx->level > TSDB_MAX_LEVEL) {
      merger->ctx->level = TSDB_MAX_LEVEL;
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(merger->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
    (void)tsdbMergeFileSetEndCloseReader(merger);
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

    TAOS_CHECK_GOTO(tsdbIterOpen(&config, &iter), &lino, _exit);
    TAOS_CHECK_GOTO(TARRAY2_APPEND(merger->dataIterArr, iter), &lino, _exit);

    // tomb iter
    config.type = TSDB_ITER_TYPE_STT_TOMB;
    config.sttReader = sttReader;

    TAOS_CHECK_GOTO(tsdbIterOpen(&config, &iter), &lino, _exit);

    TAOS_CHECK_GOTO(TARRAY2_APPEND(merger->tombIterArr, iter), &lino, _exit);
  }

  TAOS_CHECK_GOTO(tsdbIterMergerOpen(merger->dataIterArr, &merger->dataIterMerger, false), &lino, _exit);

  TAOS_CHECK_GOTO(tsdbIterMergerOpen(merger->tombIterArr, &merger->tombIterMerger, true), &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", vid, __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbMergeFileSetBeginOpenWriter(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  SDiskID did;
  int32_t level = tsdbFidLevel(merger->ctx->fset->fid, &merger->tsdb->keepCfg, merger->ctx->now);

  TAOS_CHECK_GOTO(tfsAllocDisk(merger->tsdb->pVnode->pTfs, level, &did), &lino, _exit);

  (void)tfsMkdirRecurAt(merger->tsdb->pVnode->pTfs, merger->tsdb->path, did);
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

  TAOS_CHECK_GOTO(tsdbFSetWriterOpen(&config, &merger->writer), &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", vid, __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbMergeFileSetBegin(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;

  TARRAY2_CLEAR(merger->fopArr, NULL);

  merger->ctx->tbid->suid = 0;
  merger->ctx->tbid->uid = 0;

  // open reader
  TAOS_CHECK_GOTO(tsdbMergeFileSetBeginOpenReader(merger), &lino, _exit);

  // open iterator
  TAOS_CHECK_GOTO(tsdbMergeFileSetBeginOpenIter(merger), &lino, _exit);

  // open writer
  TAOS_CHECK_GOTO(tsdbMergeFileSetBeginOpenWriter(merger), &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(merger->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
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

static int32_t tsdbMergeFileSetEnd(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;

  TAOS_CHECK_GOTO(tsdbMergeFileSetEndCloseWriter(merger), &lino, _exit);

  TAOS_CHECK_GOTO(tsdbMergeFileSetEndCloseIter(merger), &lino, _exit);

  TAOS_CHECK_GOTO(tsdbMergeFileSetEndCloseReader(merger), &lino, _exit);

  // edit file system
  TAOS_CHECK_GOTO(tsdbFSEditBegin(merger->tsdb->pFS, merger->fopArr, TSDB_FEDIT_MERGE), &lino, _exit);

  (void)taosThreadMutexLock(&merger->tsdb->mutex);
  code = tsdbFSEditCommit(merger->tsdb->pFS);
  if (code) {
    (void)taosThreadMutexUnlock(&merger->tsdb->mutex);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  (void)taosThreadMutexUnlock(&merger->tsdb->mutex);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(merger->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
    TSDB_ERROR_LOG(TD_VID(merger->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbMergeFileSet(SMerger *merger, STFileSet *fset) {
  int32_t code = 0;
  int32_t lino = 0;

  merger->ctx->fset = fset;
  TAOS_CHECK_GOTO(tsdbMergeFileSetBegin(merger), &lino, _exit);

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
        TAOS_CHECK_GOTO(tsdbIterMergerSkipTableData(merger->dataIterMerger, merger->ctx->tbid), &lino, _exit);
        continue;
      }
    }

    TAOS_CHECK_GOTO(tsdbFSetWriteRow(merger->writer, row), &lino, _exit);

    TAOS_CHECK_GOTO(tsdbIterMergerNext(merger->dataIterMerger), &lino, _exit);
  }

  // tomb
  merger->ctx->tbid->suid = 0;
  merger->ctx->tbid->uid = 0;
  for (STombRecord *record; (record = tsdbIterMergerGetTombRecord(merger->tombIterMerger)) != NULL;) {
    if (record->uid != merger->ctx->tbid->uid) {
      merger->ctx->tbid->uid = record->uid;
      merger->ctx->tbid->suid = record->suid;

      if (metaGetInfo(merger->tsdb->pVnode->pMeta, record->uid, &info, NULL) != 0) {
        TAOS_CHECK_GOTO(tsdbIterMergerSkipTableData(merger->tombIterMerger, merger->ctx->tbid), &lino, _exit);
        continue;
      }
    }
    TAOS_CHECK_GOTO(tsdbFSetWriteTombRecord(merger->writer, record), &lino, _exit);

    TAOS_CHECK_GOTO(tsdbIterMergerNext(merger->tombIterMerger), &lino, _exit);
  }

  TAOS_CHECK_GOTO(tsdbMergeFileSetEnd(merger), &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(merger->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(merger->tsdb->pVnode), __func__, fset->fid);
  }
  return code;
}

static int32_t tsdbDoMerge(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;

  if (TARRAY2_SIZE(merger->fset->lvlArr) == 0) return 0;

  SSttLvl *lvl = TARRAY2_FIRST(merger->fset->lvlArr);
  if (lvl->level != 0 || TARRAY2_SIZE(lvl->fobjArr) < merger->sttTrigger) {
    return 0;
  }

  TAOS_CHECK_GOTO(tsdbMergerOpen(merger), &lino, _exit);
  TAOS_CHECK_GOTO(tsdbMergeFileSet(merger, merger->fset), &lino, _exit);
  TAOS_CHECK_GOTO(tsdbMergerClose(merger), &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(merger->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(merger->tsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbMergeGetFSet(SMerger *merger) {
  STFileSet *fset;

  (void)taosThreadMutexLock(&merger->tsdb->mutex);
  tsdbFSGetFSet(merger->tsdb->pFS, merger->fid, &fset);
  if (fset == NULL) {
    (void)taosThreadMutexUnlock(&merger->tsdb->mutex);
    return 0;
  }

  fset->mergeScheduled = false;

  int32_t code = tsdbTFileSetInitCopy(merger->tsdb, fset, &merger->fset);
  if (code) {
    (void)taosThreadMutexUnlock(&merger->tsdb->mutex);
    return code;
  }
  (void)taosThreadMutexUnlock(&merger->tsdb->mutex);
  return 0;
}

int32_t tsdbMerge(void *arg) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SMergeArg *mergeArg = (SMergeArg *)arg;
  STsdb     *tsdb = mergeArg->tsdb;

  SMerger merger[1] = {{
      .tsdb = tsdb,
      .fid = mergeArg->fid,
      .sttTrigger = tsdb->pVnode->config.sttTrigger,
  }};

  if (merger->sttTrigger <= 1) return 0;

  // copy snapshot
  TAOS_CHECK_GOTO(tsdbMergeGetFSet(merger), &lino, _exit);

  if (merger->fset == NULL) {
    return 0;
  }

  // do merge
  tsdbInfo("vgId:%d merge begin, fid:%d", TD_VID(tsdb->pVnode), merger->fid);
  code = tsdbDoMerge(merger);
  tsdbInfo("vgId:%d merge done, fid:%d", TD_VID(tsdb->pVnode), mergeArg->fid);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
    tsdbFatal("vgId:%d, failed to merge stt files since %s. code:%d", TD_VID(tsdb->pVnode), terrstr(), code);
    taosMsleep(100);
    exit(EXIT_FAILURE);
  }
  tsdbTFileSetClear(&merger->fset);
  taosMemoryFree(arg);
  return code;
}
