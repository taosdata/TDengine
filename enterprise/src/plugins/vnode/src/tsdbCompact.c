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

#include "../../../../../source/dnode/vnode/src/tsdb/tsdbDataFileRW.h"
#include "../../../../../source/dnode/vnode/src/tsdb/tsdbFS2.h"
#include "../../../../../source/dnode/vnode/src/tsdb/tsdbFSetRW.h"
#include "../../../../../source/dnode/vnode/src/tsdb/tsdbIter.h"
#include "../../../../../source/dnode/vnode/src/tsdb/tsdbSttFileRW.h"
#include "tsdb.h"
#include "vnd.h"

extern int     vnodeScheduleTask(int (*execute)(void *), void *arg);
extern int32_t tsdbUpdateTableSchema(SMeta *pMeta, int64_t suid, int64_t uid, SSkmInfo *pSkmInfo);
extern int32_t tsdbWriteDataBlock(SDataFWriter *pWriter, SBlockData *pBlockData, SMapData *mDataBlk, int8_t cmprAlg);
extern int32_t tsdbWriteSttBlock(SDataFWriter *pWriter, SBlockData *pBlockData, SArray *aSttBlk, int8_t cmprAlg);

// tsdbCompactMonitor.c
extern bool    tsdbCompMonHasTask(STsdb *tsdb);
extern int32_t tsdbAddCompMonitorTask(STsdb *tsdb, int32_t fid, int64_t taskId);
extern int32_t tsdbRemoveCompMonitorTask(STsdb *tsdb, int64_t taskId);

// new code ====================================================================================
typedef struct {
  STsdb  *tsdb;
  int32_t szPage;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int64_t cid;
  int64_t compactVersion;

  int32_t        minFid;
  int32_t        maxFid;
  TFileSetArray *fsetArr;
  TFileOpArray   fopArr[1];

  struct {
    STFileSet *fset;
    SDiskID    did;

    // reader
    SDataFileReader    *dataReader;
    TSttFileReaderArray sttReaderArr[1];

    // iter & merger
    TTsdbIterArray dataIterArr[1];
    SIterMerger   *dataIterMerger;
    TTsdbIterArray tombIterArr[1];
    SIterMerger   *tombIterMerger;

    // writer
    SFSetWriter *writer;

    TABLEID tbid[1];

    // skyline
    SArray  *aSkyLine;
    int32_t  iSkyLine;
    TSDBKEY *pDKey;
    TSDBKEY  dKey;
  } ctx[1];
} SCompactor2;

typedef struct {
  STsdb      *tsdb;
  STimeWindow tw;
  int32_t     fid;
  int64_t     taskid;
} SCompactArg;

static int32_t tsdbCompactBegin(SCompactArg *arg, SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *tsdb = arg->tsdb;

  compactor->tsdb = tsdb;
  compactor->szPage = tsdb->pVnode->config.tsdbPageSize;
  compactor->minRow = tsdb->pVnode->config.tsdbCfg.minRows;
  compactor->maxRow = tsdb->pVnode->config.tsdbCfg.maxRows;
  compactor->cmprAlg = tsdb->pVnode->config.tsdbCfg.compression;
  compactor->cid = tsdbFSAllocEid(tsdb->pFS);
  compactor->compactVersion = INT64_MAX;
  compactor->minFid = tsdbKeyFid(arg->tw.skey, tsdb->keepCfg.days, tsdb->keepCfg.precision);
  compactor->maxFid = tsdbKeyFid(arg->tw.ekey, tsdb->keepCfg.days, tsdb->keepCfg.precision);

  code = tsdbFSCreateCopySnapshot(tsdb->pFS, &compactor->fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactEnd(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  taosArrayDestroy(compactor->ctx->aSkyLine);

  TARRAY2_DESTROY(compactor->ctx->tombIterArr, NULL);
  TARRAY2_DESTROY(compactor->ctx->dataIterArr, NULL);
  TARRAY2_DESTROY(compactor->ctx->sttReaderArr, NULL);
  TARRAY2_DESTROY(compactor->fopArr, NULL);

  tsdbFSDestroyCopySnapshot(&compactor->fsetArr);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactFSetOpenReader(SCompactor2 *compactor) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STFileObj *fobj;

  ASSERT(compactor->ctx->dataReader == NULL);
  ASSERT(TARRAY2_SIZE(compactor->ctx->sttReaderArr) == 0);

  // data
  SDataFileReaderConfig dataFileReaderConfig = {
      .tsdb = compactor->tsdb,
      .szPage = compactor->szPage,
  };
  for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX && (fobj = compactor->ctx->fset->farr[ftype], 1); ftype++) {
    if (fobj == NULL) continue;
    dataFileReaderConfig.files[ftype].exist = true;
    dataFileReaderConfig.files[ftype].file = fobj->f[0];

    STFileOp op = {
        .optype = TSDB_FOP_REMOVE,
        .fid = compactor->ctx->fset->fid,
        .of = fobj->f[0],
    };

    code = TARRAY2_APPEND(compactor->fopArr, op);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  code = tsdbDataFileReaderOpen(NULL, &dataFileReaderConfig, &compactor->ctx->dataReader);
  TSDB_CHECK_CODE(code, lino, _exit);

  // stt
  SSttLvl *lvl;
  TARRAY2_FOREACH(compactor->ctx->fset->lvlArr, lvl) {
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      SSttFileReader      *sttReader;
      SSttFileReaderConfig sttFileReaderConfig = {
          .tsdb = compactor->tsdb,
          .szPage = compactor->szPage,
          .file = fobj->f[0],
      };

      code = tsdbSttFileReaderOpen(fobj->fname, &sttFileReaderConfig, &sttReader);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = TARRAY2_APPEND(compactor->ctx->sttReaderArr, sttReader);
      TSDB_CHECK_CODE(code, lino, _exit);

      STFileOp op = {
          .optype = TSDB_FOP_REMOVE,
          .fid = compactor->ctx->fset->fid,
          .of = fobj->f[0],
      };

      code = TARRAY2_APPEND(compactor->fopArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactFSetCloseReader(SCompactor2 *compactor) {
  TARRAY2_CLEAR(compactor->ctx->sttReaderArr, tsdbSttFileReaderClose);
  tsdbDataFileReaderClose(&compactor->ctx->dataReader);
  return 0;
}

static int32_t tsdbCompactFSetOpenIter(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbIter      *iter;
  STsdbIterConfig iterConfig = {0};

  ASSERT(compactor->ctx->dataIterMerger == NULL);
  ASSERT(compactor->ctx->tombIterMerger == NULL);
  ASSERT(TARRAY2_SIZE(compactor->ctx->dataIterArr) == 0);
  ASSERT(TARRAY2_SIZE(compactor->ctx->tombIterArr) == 0);

  // data
  if (compactor->ctx->dataReader != NULL) {
    // data
    iterConfig.type = TSDB_ITER_TYPE_DATA;
    iterConfig.dataReader = compactor->ctx->dataReader;

    code = tsdbIterOpen(&iterConfig, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(compactor->ctx->dataIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    // tomb
    iterConfig.type = TSDB_ITER_TYPE_DATA_TOMB;
    iterConfig.dataReader = compactor->ctx->dataReader;

    code = tsdbIterOpen(&iterConfig, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(compactor->ctx->tombIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // stt
  SSttFileReader *sttReader;
  TARRAY2_FOREACH(compactor->ctx->sttReaderArr, sttReader) {
    // data
    iterConfig.type = TSDB_ITER_TYPE_STT;
    iterConfig.sttReader = sttReader;

    code = tsdbIterOpen(&iterConfig, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(compactor->ctx->dataIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    // tomb
    iterConfig.type = TSDB_ITER_TYPE_STT_TOMB;
    iterConfig.sttReader = sttReader;

    code = tsdbIterOpen(&iterConfig, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(compactor->ctx->tombIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // merger
  code = tsdbIterMergerOpen(compactor->ctx->dataIterArr, &compactor->ctx->dataIterMerger, false);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbIterMergerOpen(compactor->ctx->tombIterArr, &compactor->ctx->tombIterMerger, true);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactFSetCloseIter(SCompactor2 *compactor) {
  tsdbIterMergerClose(&compactor->ctx->dataIterMerger);
  tsdbIterMergerClose(&compactor->ctx->tombIterMerger);
  TARRAY2_CLEAR(compactor->ctx->tombIterArr, tsdbIterClose);
  TARRAY2_CLEAR(compactor->ctx->dataIterArr, tsdbIterClose);
  return 0;
}

static int32_t tsdbCompactFSetOpenWriter(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t lcn = 0;

  ASSERT(compactor->ctx->writer == NULL);

  STFileObj *fobj = compactor->ctx->fset->farr[TSDB_FTYPE_DATA];
  if (fobj) {
    lcn = fobj->f->lcn;
  }

  SFSetWriterConfig config = {
      .tsdb = compactor->tsdb,
      .toSttOnly = false,
      .compactVersion = compactor->compactVersion,
      .minRow = compactor->minRow,
      .maxRow = compactor->maxRow,
      .szPage = compactor->szPage,
      .cmprAlg = compactor->cmprAlg,
      .fid = compactor->ctx->fset->fid,
      .cid = compactor->cid,
      .did = compactor->ctx->did,
      .level = 0,
      .lcn = lcn,
  };

  code = tsdbFSetWriterOpen(&config, &compactor->ctx->writer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactFSetCloseWriter(SCompactor2 *compactor) {
  return tsdbFSetWriterClose(&compactor->ctx->writer, 0, compactor->fopArr);
}

static int32_t tsdbCompactFSetBegin(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  TARRAY2_CLEAR(compactor->fopArr, NULL);

  code = tsdbCompactFSetOpenReader(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCompactFSetOpenIter(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCompactFSetOpenWriter(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  compactor->ctx->tbid->suid = 0;
  compactor->ctx->tbid->uid = 0;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactFSetEnd(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbCompactFSetCloseWriter(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCompactFSetCloseIter(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCompactFSetCloseReader(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFSEditBegin(compactor->tsdb->pFS, compactor->fopArr, TSDB_FEDIT_MERGE);
  TSDB_CHECK_CODE(code, lino, _exit);

  taosThreadMutexLock(&compactor->tsdb->mutex);
  code = tsdbFSEditCommit(compactor->tsdb->pFS);
  if (code) {
    taosThreadMutexUnlock(&compactor->tsdb->mutex);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  taosThreadMutexUnlock(&compactor->tsdb->mutex);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactFSetTableDataEnd(SCompactor2 *compactor) {
  // do nothing
  return 0;
}

static int32_t tsdbCompactFSetTableDataBegin(SCompactor2 *compactor, const TABLEID *tbid) {
  int32_t code = 0;
  int32_t lino = 0;

  compactor->ctx->tbid->suid = tbid->suid;
  compactor->ctx->tbid->uid = tbid->uid;

  SArray *delDataArr = NULL;

  for (STombRecord *record; (record = tsdbIterMergerGetTombRecord(compactor->ctx->tombIterMerger)) != NULL;) {
    if (record->suid > tbid->suid || (record->suid == tbid->suid && record->uid > tbid->uid)) {
      break;
    } else {
      if (record->uid == tbid->uid) {
        SDelData delData = {
            .version = record->version,
            .sKey = record->skey,
            .eKey = record->ekey,
        };

        if (delDataArr == NULL && (delDataArr = taosArrayInit(0, sizeof(SDelData))) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        if (taosArrayPush(delDataArr, &delData) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }

      code = tsdbIterMergerNext(compactor->ctx->tombIterMerger);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (delDataArr) {
    if (compactor->ctx->aSkyLine == NULL && (compactor->ctx->aSkyLine = taosArrayInit(0, sizeof(TSDBKEY))) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbBuildDeleteSkyline(delDataArr, 0, taosArrayGetSize(delDataArr) - 1, compactor->ctx->aSkyLine);
    TSDB_CHECK_CODE(code, lino, _exit);

    compactor->ctx->iSkyLine = 0;
    TSDBKEY *pKey = (TSDBKEY *)taosArrayGet(compactor->ctx->aSkyLine, compactor->ctx->iSkyLine);
    compactor->ctx->dKey.version = 0;
    compactor->ctx->dKey.ts = pKey->ts;
    compactor->ctx->pDKey = &compactor->ctx->dKey;

    taosArrayDestroy(delDataArr);
  } else {
    compactor->ctx->pDKey = NULL;
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static bool tsdbRowIsDeleted(SCompactor2 *compactor, TSDBROW *row) {
  TSDBKEY  tKey = TSDBROW_KEY(row);
  TSDBKEY *aKey = (TSDBKEY *)TARRAY_DATA(compactor->ctx->aSkyLine);
  int32_t  nKey = TARRAY_SIZE(compactor->ctx->aSkyLine);

  if (tKey.ts > compactor->ctx->pDKey->ts) {
    do {
      compactor->ctx->pDKey->version = aKey[compactor->ctx->iSkyLine].version;
      compactor->ctx->iSkyLine++;
      if (compactor->ctx->iSkyLine < nKey) {
        compactor->ctx->dKey.ts = aKey[compactor->ctx->iSkyLine].ts;
      } else {
        if (compactor->ctx->pDKey->version == 0) {
          compactor->ctx->pDKey = NULL;
          return false;
        } else {
          compactor->ctx->pDKey->ts = INT64_MAX;
        }
      }
    } while (tKey.ts > compactor->ctx->pDKey->ts);
  }

  if (tKey.ts < compactor->ctx->pDKey->ts) {
    if (tKey.version > compactor->ctx->pDKey->version) {
      return false;
    } else {
      return true;
    }
  } else if (tKey.ts == compactor->ctx->pDKey->ts) {
    ASSERT(compactor->ctx->iSkyLine < nKey);
    if (tKey.version > TMAX(compactor->ctx->pDKey->version, aKey[compactor->ctx->iSkyLine].version)) {
      return false;
    } else {
      return true;
    }
  }

  return false;
}

static int32_t tsdbCompactFSet(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaInfo info;
  int64_t   numOfRow = 0;
  for (SRowInfo *row; (row = tsdbIterMergerGetData(compactor->ctx->dataIterMerger)) != NULL;) {
    if (row->uid != compactor->ctx->tbid->uid) {
      code = tsdbCompactFSetTableDataEnd(compactor);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (metaGetInfo(compactor->tsdb->pVnode->pMeta, row->uid, &info, NULL) != 0) {
        TABLEID tbid = {.suid = row->suid, .uid = row->uid};
        code = tsdbIterMergerSkipTableData(compactor->ctx->dataIterMerger, &tbid);
        TSDB_CHECK_CODE(code, lino, _exit);
        continue;
      }

      code = tsdbCompactFSetTableDataBegin(compactor, (TABLEID *)row);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (compactor->ctx->pDKey == NULL || !tsdbRowIsDeleted(compactor, &row->row)) {
      code = tsdbFSetWriteRow(compactor->ctx->writer, row);
      TSDB_CHECK_CODE(code, lino, _exit);
      numOfRow++;
    }
    code = tsdbIterMergerNext(compactor->ctx->dataIterMerger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  } else {
    tsdbInfo("vgId:%d fid:%d compact %" PRId64 " rows", TD_VID(compactor->tsdb->pVnode), compactor->ctx->fset->fid,
             numOfRow);
  }
  return code;
}

static bool tsdbCheckCompactNecessary(SCompactor2 *compactor) {
  // TODO
  return true;
}

static int32_t tsdbDoCompactAsync(void *arg) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SCompactArg *compactArg = (SCompactArg *)arg;

  SCompactor2 compactor[1] = {0};

  code = tsdbCompactBegin(arg, compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  TARRAY2_FOREACH(compactor->fsetArr, compactor->ctx->fset) {
    if (compactor->ctx->fset->fid != compactArg->fid) {
      continue;
    }

    // check if the file set should be compacted
    if (!tsdbCheckCompactNecessary(compactor)) {
      continue;
    }

    // allocate disk
    int32_t expLevel = tsdbFidLevel(compactor->ctx->fset->fid, &compactor->tsdb->keepCfg, taosGetTimestampSec());
    if (expLevel < 0) {
      continue;
    }
    code = tfsAllocDisk(compactor->tsdb->pVnode->pTfs, expLevel, &compactor->ctx->did);
    if (code) {
      code = TAOS_SYSTEM_ERROR(code);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbCompactFSetBegin(compactor);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbCompactFSet(compactor);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbCompactFSetEnd(compactor);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbCompactEnd(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactArg->tsdb->pVnode), lino, code);

    (void)tsdbCompactEnd(compactor);
  }
  tsdbRemoveCompMonitorTask(compactArg->tsdb, compactArg->taskid);
  return code;
}

static void tsdbFreeCompactArg(void *arg) { taosMemoryFree(arg); }

int32_t tsdbAsyncCompact(STsdb *tsdb, const STimeWindow *tw, bool sync) {
  int32_t code = 0;

  int32_t minFid = tsdbKeyFid(tw->skey, tsdb->keepCfg.days, tsdb->keepCfg.precision);
  int32_t maxFid = tsdbKeyFid(tw->ekey, tsdb->keepCfg.days, tsdb->keepCfg.precision);

  taosThreadMutexLock(&tsdb->mutex);

  if (tsdb->bgTaskDisabled) {
    taosThreadMutexUnlock(&tsdb->mutex);
    return 0;
  }

  STFileSet *fset;
  TARRAY2_FOREACH(tsdb->pFS->fSetArr, fset) {
    if (fset->fid < minFid || fset->fid > maxFid) continue;

    code = tsdbTFileSetOpenChannel(fset);
    if (code) {
      taosThreadMutexUnlock(&tsdb->mutex);
      return code;
    }

    SCompactArg *arg = taosMemoryMalloc(sizeof(*arg));
    if (arg == NULL) {
      taosThreadMutexUnlock(&tsdb->mutex);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    arg->tsdb = tsdb;
    arg->tw = *tw;
    arg->fid = fset->fid;

    if (sync) {
      code = vnodeAsyncC(vnodeAsyncHandle[0], tsdb->pVnode->commitChannel, EVA_PRIORITY_NORMAL, tsdbDoCompactAsync,
                         tsdbFreeCompactArg, arg, &arg->taskid);
    } else {
      code = vnodeAsyncC(vnodeAsyncHandle[1], fset->bgTaskChannel, EVA_PRIORITY_NORMAL, tsdbDoCompactAsync,
                         tsdbFreeCompactArg, arg, &arg->taskid);
    }
    if (code) {
      tsdbFreeCompactArg(arg);
      taosThreadMutexUnlock(&tsdb->mutex);
      return code;
    } else {
      tsdbAddCompMonitorTask(tsdb, fset->fid, arg->taskid);
    }
  }

  taosThreadMutexUnlock(&tsdb->mutex);

  return code;
}
