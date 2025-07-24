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
#include "meta.h"
#include "tsdb.h"
#include "vnd.h"

extern int     vnodeScheduleTask(int (*execute)(void *), void *arg);
extern int32_t tsdbUpdateTableSchema(SMeta *pMeta, int64_t suid, int64_t uid, SSkmInfo *pSkmInfo);
extern int32_t tsdbWriteDataBlock(SDataFWriter *pWriter, SBlockData *pBlockData, SMapData *mDataBlk, int8_t cmprAlg);
extern int32_t tsdbWriteSttBlock(SDataFWriter *pWriter, SBlockData *pBlockData, SArray *aSttBlk, int8_t cmprAlg);

// tsdbCompactMonitor.c
extern bool    tsdbCompMonHasTask(STsdb *tsdb);
extern int32_t tsdbAddCompMonitorTask(STsdb *tsdb, int32_t fid, SVATaskID *taskId, int64_t compactSize);
extern int32_t tsdbUpdateCompMonitorTask(STsdb *tsdb, int32_t fid, SVATaskID *taskId, int64_t compactSize);
extern int32_t tsdbCompMonitorGetKilled(STsdb *tsdb);
extern void    tsdbRemoveCompMonitorTask(STsdb *tsdb, SVATaskID *taskId);
static void    tsdbCompactCancel(void *arg);

// new code ====================================================================================
typedef struct {
  STsdb  *tsdb;
  int32_t szPage;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int64_t cid;
  int64_t compactVersion;

  STFileSet   *fset;
  TFileOpArray fopArr[1];

  struct {
    int32_t expLevel;
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
    SHashObj *pKeepHashObj;  // SHashObj<suid, keep>

    // skyline
    SArray  *aSkyLine;
    int32_t  iSkyLine;
    TSDBKEY *pDKey;
    TSDBKEY  dKey;
  } ctx[1];
} SCompactor2;

typedef struct {
  STsdb    *tsdb;
  int32_t   fid;
  SVATaskID taskid;
  bool      ssMigrate;
} SCompactArg;

static int32_t tsdbCompactFSetOpenReader(SCompactor2 *compactor) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STFileObj *fobj;

  // data
  SDataFileReaderConfig dataFileReaderConfig = {
      .tsdb = compactor->tsdb,
      .szPage = compactor->szPage,
  };
  for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX && (fobj = compactor->fset->farr[ftype], 1); ftype++) {
    if (fobj == NULL) continue;
    dataFileReaderConfig.files[ftype].exist = true;
    dataFileReaderConfig.files[ftype].file = fobj->f[0];

    STFileOp op = {
        .optype = TSDB_FOP_REMOVE,
        .fid = compactor->fset->fid,
        .of = fobj->f[0],
    };

    code = TARRAY2_APPEND(compactor->fopArr, op);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  code = tsdbDataFileReaderOpen(NULL, &dataFileReaderConfig, &compactor->ctx->dataReader);
  TSDB_CHECK_CODE(code, lino, _exit);

  // stt
  SSttLvl *lvl;
  TARRAY2_FOREACH(compactor->fset->lvlArr, lvl) {
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      if (tsdbCompMonitorGetKilled(compactor->tsdb)) {
        tsdbInfo("vgId:%d fid:%d (file:%s) compact killed during open reader", TD_VID(compactor->tsdb->pVnode),
                 compactor->fset->fid, fobj->fname);
        code = TSDB_CODE_TSC_QUERY_KILLED;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

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
          .fid = compactor->fset->fid,
          .of = fobj->f[0],
      };

      code = TARRAY2_APPEND(compactor->fopArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(compactor->tsdb->pVnode), __func__, lino,
              tstrerror(code));
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
    if (tsdbCompMonitorGetKilled(compactor->tsdb)) {
      tsdbInfo("vgId:%d fid:%d compact killed during open iter for stt", TD_VID(compactor->tsdb->pVnode),
               compactor->fset->fid);
      code = TSDB_CODE_TSC_QUERY_KILLED;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(compactor->tsdb->pVnode), __func__, lino,
              tstrerror(code));
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

  STFileObj *fobj = compactor->fset->farr[TSDB_FTYPE_DATA];
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
      .fid = compactor->fset->fid,
      .cid = compactor->cid,
      .expLevel = compactor->ctx->expLevel,
      .level = 0,
      .lcn = lcn,
  };

  code = tsdbFSetWriterOpen(&config, &compactor->ctx->writer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(compactor->tsdb->pVnode), __func__, lino,
              tstrerror(code));
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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(compactor->tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCompactFSetEnd(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbCompactFSetCloseWriter(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (TARRAY2_SIZE(compactor->fopArr) > 0) {
    code = tsdbFSEditBegin(compactor->tsdb->pFS, compactor->fopArr, TSDB_FEDIT_COMPACT);
    TSDB_CHECK_CODE(code, lino, _exit);

    TAOS_UNUSED(taosThreadMutexLock(&compactor->tsdb->mutex));
    if ((code = tsdbFSEditCommit(compactor->tsdb->pFS))) {
      TAOS_UNUSED(taosThreadMutexUnlock(&compactor->tsdb->mutex));
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    TAOS_UNUSED(taosThreadMutexUnlock(&compactor->tsdb->mutex));
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(compactor->tsdb->pVnode), __func__, lino,
              tstrerror(code));
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
    if (tsdbCompMonitorGetKilled(compactor->tsdb)) {
      tsdbInfo("vgId:%d fid:%d compact killed during tombstone processing", TD_VID(compactor->tsdb->pVnode),
               compactor->fset->fid);
      code = TSDB_CODE_TSC_QUERY_KILLED;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(compactor->tsdb->pVnode), __func__, lino,
              tstrerror(code));
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
    if (tKey.version > TMAX(compactor->ctx->pDKey->version, aKey[compactor->ctx->iSkyLine].version)) {
      return false;
    } else {
      return true;
    }
  }

  return false;
}

static bool tsdbRowIsExpired(SCompactor2 *compactor, TSDBROW *row, int64_t keep, int64_t expireTs) {
  if (keep <= 0) {
    return false;
  }
  TSDBKEY tKey = TSDBROW_KEY(row);

  tsdbDebug("vgId:%d, tKey.ts:%" PRId64 ", expireTs:%" PRId64, TD_VID(compactor->tsdb->pVnode), tKey.ts, expireTs);
  return (tKey.ts < expireTs);
}

static int32_t tsdbCompactFSetGetKeep(SCompactor2 *compactor, int64_t suid, int64_t *keep) {
  int32_t code = 0;
  int32_t lino = 0;

  if (compactor->ctx->pKeepHashObj == NULL) {
    compactor->ctx->pKeepHashObj =
        taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  }

  int64_t *pKeep = (int64_t *)taosHashGet(compactor->ctx->pKeepHashObj, &suid, sizeof(suid));
  if (pKeep == NULL) {
    *keep = metaGetStbKeep(compactor->tsdb->pVnode->pMeta, suid);
    taosHashPut(compactor->ctx->pKeepHashObj, &suid, sizeof(suid), &keep, sizeof(keep));
  } else {
    *keep = *pKeep;
  }
  return code;
}

static int32_t tsdbCompactFSet(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaInfo info;
  int64_t   numOfRow = 0;
  int64_t   keep = 0;
  int64_t   now = taosGetTimestamp(compactor->tsdb->keepCfg.precision);
  int64_t   expireTs = 0;

  for (SRowInfo *row; (row = tsdbIterMergerGetData(compactor->ctx->dataIterMerger)) != NULL;) {
    if (tsdbCompMonitorGetKilled(compactor->tsdb)) {
      tsdbInfo("vgId:%d fid:%d compact killed during data processing", TD_VID(compactor->tsdb->pVnode),
               compactor->fset->fid);
      code = TSDB_CODE_TSC_QUERY_KILLED;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (row->suid != compactor->ctx->tbid->suid) {
      code = tsdbCompactFSetGetKeep(compactor, row->suid, &keep);
      expireTs = now - (tsTickPerMin[compactor->tsdb->keepCfg.precision] * keep);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

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

    if ((compactor->ctx->pDKey == NULL || !tsdbRowIsDeleted(compactor, &row->row)) &&
        !tsdbRowIsExpired(compactor, &row->row, keep, expireTs)) {
      code = tsdbFSetWriteRow(compactor->ctx->writer, row);
      TSDB_CHECK_CODE(code, lino, _exit);
      numOfRow++;
    }
    code = tsdbIterMergerNext(compactor->ctx->dataIterMerger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(compactor->tsdb->pVnode), __func__, lino,
              tstrerror(code));
  } else {
    tsdbInfo("vgId:%d fid:%d compact %" PRId64 " rows", TD_VID(compactor->tsdb->pVnode), compactor->fset->fid,
             numOfRow);
  }
  return code;
}

bool tsdbShouldCompact(STFileSet *fset, int32_t vgId) {
  STFileObj *fobj = fset->farr[TSDB_FTYPE_DATA];
  if (fobj) {
    tsdbInfo("vgId:%d, fid:%d,lastCompact:%" PRId64 ", lastCommit:%" PRId64 ", lastMigrate:%" PRId64 ", lcn:%d", vgId,
             fset->fid, fset->lastCompact, fset->lastCommit, fset->lastMigrate, fobj->f->lcn);
  }

  // because we only do compact on the leader vnode of a migration, "fset->lastCompact > fset->lastCommit"
  // is true on this vnode, but false on the follower vnodes. that's, when trigger a compact without new
  // data, only follower vnodes will do compact, and leader vnode will not. so, if we then trigger a
  // migration and the previous leader is a follower this time, and the migration failed, then the previous
  // leader may read corrupted data from remote storage. to avoid this, if there's not a commit after the
  // last migration, we don't do compact.
  if (fset->lastMigrate > fset->lastCommit) {
    return false;
  }

  if (fset->lastCompact > fset->lastCommit) {
    return false;
  }

  return true;
}

static void tsdbCompactEnd(SCompactor2 *compactor) {
  TAOS_UNUSED(tsdbCompactFSetCloseWriter(compactor));
  TAOS_UNUSED(tsdbCompactFSetCloseIter(compactor));
  TAOS_UNUSED(tsdbCompactFSetCloseReader(compactor));
  taosArrayDestroy(compactor->ctx->aSkyLine);
  TARRAY2_DESTROY(compactor->ctx->tombIterArr, NULL);
  TARRAY2_DESTROY(compactor->ctx->dataIterArr, NULL);
  TARRAY2_DESTROY(compactor->ctx->sttReaderArr, NULL);
  TARRAY2_DESTROY(compactor->fopArr, NULL);
  taosHashCleanup(compactor->ctx->pKeepHashObj);
}

static int32_t tsdbDoCompact(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *tsdb = compactor->tsdb;
  compactor->ctx->expLevel = tsdbFidLevel(compactor->fset->fid, &compactor->tsdb->keepCfg, taosGetTimestampSec());

  tsdbInfo("vgId:%d compact fileset:%d start", TD_VID(tsdb->pVnode), compactor->fset->fid);

  code = tsdbCompactFSetBegin(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCompactFSet(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCompactFSetEnd(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  tsdbCompactEnd(compactor);
  if (code) {
    tsdbError("vgId:%d compact fileset %d failed at line %d since %s", TD_VID(tsdb->pVnode), compactor->fset->fid, lino,
              tstrerror(code));
  } else {
    tsdbInfo("vgId:%d compact fileset %d done", TD_VID(tsdb->pVnode), compactor->fset->fid);
  }
  return code;
}

static int32_t tsdbCompact(void *arg) {
  int32_t code = 0;
  int32_t lino = 0;

  SCompactArg *compactArg = (SCompactArg *)arg;
  STsdb       *tsdb = compactArg->tsdb;
  STFileSet   *fset = NULL;
  SCompactor2  compactor = {
       .tsdb = tsdb,
       .szPage = tsdb->pVnode->config.tsdbPageSize,
       .minRow = tsdb->pVnode->config.tsdbCfg.minRows,
       .maxRow = tsdb->pVnode->config.tsdbCfg.maxRows,
       .cmprAlg = tsdb->pVnode->config.tsdbCfg.compression,
       .cid = tsdbFSAllocEid(tsdb->pFS),
       .compactVersion = INT64_MAX,
  };

  // begin task
  (void)taosThreadMutexLock(&tsdb->mutex);

  if (tsdb->bgTaskDisabled) {
    tsdbInfo("vgId:%d, background task is disabled, skip compact", TD_VID(tsdb->pVnode));
    (void)taosThreadMutexUnlock(&tsdb->mutex);
    return 0;
  }

  tsdbBeginTaskOnFileSet(tsdb, compactArg->fid, EVA_TASK_COMPACT, &fset);
  if (NULL == fset) {
    (void)taosThreadMutexUnlock(&tsdb->mutex);
    return 0;
  }

  code = tsdbTFileSetInitCopy(tsdb, fset, &compactor.fset);
  if (code) {
    (void)taosThreadMutexUnlock(&tsdb->mutex);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  (void)taosThreadMutexUnlock(&tsdb->mutex);

  // do compact
  if (compactor.fset && tsdbShouldCompact(compactor.fset, TD_VID(tsdb->pVnode))) {
    code = tsdbDoCompact(&compactor);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  // finish task
  if (compactor.fset) {
    (void)taosThreadMutexLock(&tsdb->mutex);
    tsdbFinishTaskOnFileSet(tsdb, compactArg->fid, EVA_TASK_COMPACT);
    (void)taosThreadMutexUnlock(&tsdb->mutex);
  }

  // clear resources
  tsdbTFileSetClear(&compactor.fset);
  TARRAY2_DESTROY(compactor.fopArr, NULL);
  if (!compactArg->ssMigrate) { 
    tsdbRemoveCompMonitorTask(tsdb, &compactArg->taskid);
  }
  taosMemoryFree(arg);

  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(tsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static void tsdbCompactCancel(void *arg) { taosMemoryFree(arg); }

static int32_t tsdbAsyncCompactImpl(STsdb *tsdb, const STimeWindow *tw,bool ssMigrate) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tsdb->bgTaskDisabled) {
    tsdbInfo("vgId:%d, background task is disabled, skip compact", TD_VID(tsdb->pVnode));
    return 0;
  }

  int32_t minFid = tsdbKeyFid(tw->skey, tsdb->keepCfg.days, tsdb->keepCfg.precision);
  int32_t maxFid = tsdbKeyFid(tw->ekey, tsdb->keepCfg.days, tsdb->keepCfg.precision);

  STFileSet *fset;
  TARRAY2_FOREACH(tsdb->pFS->fSetArr, fset) {
    if (fset->fid < minFid || fset->fid > maxFid) {
      continue;
    }

    SCompactArg *arg = taosMemoryMalloc(sizeof(*arg));
    if (arg == NULL) {
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }

    arg->tsdb = tsdb;
    arg->fid = fset->fid;
    arg->ssMigrate = ssMigrate;

    // if the compact task is already running, skip it
    // because the compact task may be running by ss migrate
    if (vnodeATaskValid(&fset->compactTask)) {
      taosMemoryFree(arg);
      continue;
    }

    code = vnodeAsync(COMPACT_TASK_ASYNC, EVA_PRIORITY_NORMAL, tsdbCompact, tsdbCompactCancel, arg, &fset->compactTask);
    if (code) {
      taosMemoryFree(arg);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      arg->taskid = fset->compactTask;
      if (!ssMigrate) {
        int64_t compactSize = 0;
        if (fset->farr[TSDB_FTYPE_DATA]) {
          compactSize += fset->farr[TSDB_FTYPE_DATA]->f->size;
        }

        SSttLvl *lvl;
        TARRAY2_FOREACH(fset->lvlArr, lvl) {
          STFileObj *fobj;
          TARRAY2_FOREACH(lvl->fobjArr, fobj) { compactSize += fobj->f->size; }
        }
        TAOS_UNUSED(tsdbAddCompMonitorTask(tsdb, fset->fid, &arg->taskid, compactSize));
      }
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(tsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbAsyncCompact(STsdb *tsdb, const STimeWindow *tw, bool ssMigrate) {
  int32_t code = 0;
  TAOS_UNUSED(taosThreadMutexLock(&tsdb->mutex));
  code = tsdbAsyncCompactImpl(tsdb, tw, ssMigrate);
  TAOS_UNUSED(taosThreadMutexUnlock(&tsdb->mutex));
  return code;
}

void tsdbAlterNumCompactThreads() { (void)vnodeAsyncSetWorkers(COMPACT_TASK_ASYNC, tsNumOfCompactThreads); }
