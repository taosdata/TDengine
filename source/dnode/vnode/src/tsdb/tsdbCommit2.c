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
  int32_t    fid;
  STFileSet *fset;
} SFileSetCommitInfo;

typedef struct {
  STsdb  *tsdb;
  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int32_t sttTrigger;
  int32_t szPage;
  int64_t compactVersion;
  int64_t cid;
  int64_t now;

  struct {
    SFileSetCommitInfo *info;

    int32_t expLevel;
    SDiskID did;
    TSKEY   minKey;
    TSKEY   maxKey;
    TABLEID tbid[1];
    bool    hasTSData;

    bool      skipTsRow;
    SHashObj *pColCmprObj;
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

  TFileOpArray fopArray[1];
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
      .fid = committer->ctx->info->fid,
      .cid = committer->cid,
      .did = committer->ctx->did,
      .level = 0,
  };

  if (committer->sttTrigger == 1) {
    config.toSttOnly = false;

    if (committer->ctx->info->fset) {
      for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ftype++) {
        if (committer->ctx->info->fset->farr[ftype] != NULL) {
          config.files[ftype].exist = true;
          config.files[ftype].file = committer->ctx->info->fset->farr[ftype]->f[0];
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

    int64_t ts = TSDBROW_TS(&row->row);
    if (ts > committer->ctx->maxKey) {
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
    tsdbDebug("vgId:%d fid:%d commit %" PRId64 " rows", TD_VID(committer->tsdb->pVnode), committer->ctx->info->fid,
              numOfRow);
  }
  return code;
}

static int32_t tsdbCommitTombData(SCommitter2 *committer) {
  int32_t   code = 0;
  int32_t   lino = 0;
  int64_t   numRecord = 0;
  SMetaInfo info;

  // if no history data and no new timestamp data, skip tomb data
  if (committer->ctx->info->fset || committer->ctx->hasTSData) {
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
        // committer->ctx->nextKey = TMIN(record->skey, committer->ctx->nextKey);
      } else {
        record->skey = TMAX(record->skey, committer->ctx->minKey);
        record->ekey = TMIN(record->ekey, committer->ctx->maxKey);

        numRecord++;
        code = tsdbFSetWriteTombRecord(committer->writer, record);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      code = tsdbIterMergerNext(committer->tombIterMerger);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d fid:%d commit %" PRId64 " tomb records", TD_VID(committer->tsdb->pVnode),
              committer->ctx->info->fid, numRecord);
  }
  return code;
}

static int32_t tsdbCommitCloseReader(SCommitter2 *committer) {
  TARRAY2_CLEAR(committer->sttReaderArray, tsdbSttFileReaderClose);
  return 0;
}

static int32_t tsdbCommitOpenReader(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(TARRAY2_SIZE(committer->sttReaderArray) == 0);

  if (committer->ctx->info->fset == NULL                        //
      || committer->sttTrigger > 1                              //
      || TARRAY2_SIZE(committer->ctx->info->fset->lvlArr) == 0  //
  ) {
    return 0;
  }

  SSttLvl *lvl;
  TARRAY2_FOREACH(committer->ctx->info->fset->lvlArr, lvl) {
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
    tsdbCommitCloseReader(committer);
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
  config.from->version = VERSION_MIN;
  config.from->key = (SRowKey){
      .ts = committer->ctx->minKey,
      .numOfPKs = 0,
  };

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
    tsdbCommitCloseIter(committer);
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitFileSetBegin(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *tsdb = committer->tsdb;

  // check if can commit
  tsdbFSCheckCommit(tsdb, committer->ctx->info->fid);

  committer->ctx->expLevel = tsdbFidLevel(committer->ctx->info->fid, &tsdb->keepCfg, committer->now);
  tsdbFidKeyRange(committer->ctx->info->fid, committer->minutes, committer->precision, &committer->ctx->minKey,
                  &committer->ctx->maxKey);
  code = tfsAllocDisk(committer->tsdb->pVnode->pTfs, committer->ctx->expLevel, &committer->ctx->did);
  TSDB_CHECK_CODE(code, lino, _exit);
  tfsMkdirRecurAt(committer->tsdb->pVnode->pTfs, committer->tsdb->path, committer->ctx->did);
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

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d minKey:%" PRId64 " maxKey:%" PRId64 " expLevel:%d", TD_VID(tsdb->pVnode),
              __func__, committer->ctx->info->fid, committer->ctx->minKey, committer->ctx->maxKey,
              committer->ctx->expLevel);
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
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(committer->tsdb->pVnode), __func__, committer->ctx->info->fid);
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
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(committer->tsdb->pVnode), __func__, committer->ctx->info->fid);
  }
  return code;
}

static int32_t tFileSetCommitInfoCompare(const void *arg1, const void *arg2) {
  SFileSetCommitInfo *info1 = (SFileSetCommitInfo *)arg1;
  SFileSetCommitInfo *info2 = (SFileSetCommitInfo *)arg2;

  if (info1->fid < info2->fid) {
    return -1;
  } else if (info1->fid > info2->fid) {
    return 1;
  } else {
    return 0;
  }
}

static int32_t tFileSetCommitInfoPCompare(const void *arg1, const void *arg2) {
  return tFileSetCommitInfoCompare(*(SFileSetCommitInfo **)arg1, *(SFileSetCommitInfo **)arg2);
}

static uint32_t tFileSetCommitInfoHash(const void *arg) {
  SFileSetCommitInfo *info = (SFileSetCommitInfo *)arg;
  return MurmurHash3_32((const char *)&info->fid, sizeof(info->fid));
}

static int32_t tsdbCommitInfoDestroy(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pTsdb->commitInfo) {
    for (int32_t i = 0; i < taosArrayGetSize(pTsdb->commitInfo->arr); i++) {
      SFileSetCommitInfo *info = *(SFileSetCommitInfo **)taosArrayGet(pTsdb->commitInfo->arr, i);
      vHashDrop(pTsdb->commitInfo->ht, info);
      tsdbTFileSetClear(&info->fset);
      taosMemoryFree(info);
    }

    vHashDestroy(&pTsdb->commitInfo->ht);
    taosArrayDestroy(pTsdb->commitInfo->arr);
    pTsdb->commitInfo->arr = NULL;
    taosMemoryFreeClear(pTsdb->commitInfo);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitInfoInit(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  pTsdb->commitInfo = taosMemoryCalloc(1, sizeof(*pTsdb->commitInfo));
  if (pTsdb->commitInfo == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }

  code = vHashInit(&pTsdb->commitInfo->ht, tFileSetCommitInfoHash, tFileSetCommitInfoCompare);
  TSDB_CHECK_CODE(code, lino, _exit);

  pTsdb->commitInfo->arr = taosArrayInit(0, sizeof(SFileSetCommitInfo *));
  if (pTsdb->commitInfo->arr == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }

_exit:
  if (code) {
    tsdbCommitInfoDestroy(pTsdb);
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitInfoAdd(STsdb *tsdb, int32_t fid) {
  int32_t code = 0;
  int32_t lino = 0;

  SFileSetCommitInfo *tinfo;

  if ((tinfo = taosMemoryMalloc(sizeof(*tinfo))) == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }
  tinfo->fid = fid;
  tinfo->fset = NULL;

  code = vHashPut(tsdb->commitInfo->ht, tinfo);
  TSDB_CHECK_CODE(code, lino, _exit);

  if ((taosArrayPush(tsdb->commitInfo->arr, &tinfo)) == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }
  taosArraySort(tsdb->commitInfo->arr, tFileSetCommitInfoPCompare);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(tsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitInfoBuild(STsdb *tsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  STFileSet  *fset = NULL;
  SRBTreeIter iter;

  code = tsdbCommitInfoInit(tsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

  // scan time-series data
  iter = tRBTreeIterCreate(tsdb->imem->tbDataTree, 1);
  for (SRBTreeNode *node = tRBTreeIterNext(&iter); node; node = tRBTreeIterNext(&iter)) {
    STbData *pTbData = TCONTAINER_OF(node, STbData, rbtn);

    // scan time-series data
    STsdbRowKey from = {
        .key.ts = INT64_MIN,
        .key.numOfPKs = 0,
        .version = INT64_MIN,
    };
    for (;;) {
      int64_t     minKey, maxKey;
      STbDataIter tbDataIter = {0};
      TSDBROW    *row;
      int32_t     fid;

      tsdbTbDataIterOpen(pTbData, &from, 0, &tbDataIter);
      if ((row = tsdbTbDataIterGet(&tbDataIter)) == NULL) {
        break;
      }

      fid = tsdbKeyFid(TSDBROW_TS(row), tsdb->keepCfg.days, tsdb->keepCfg.precision);
      tsdbFidKeyRange(fid, tsdb->keepCfg.days, tsdb->keepCfg.precision, &minKey, &maxKey);

      SFileSetCommitInfo *info;
      SFileSetCommitInfo  tinfo = {
           .fid = fid,
      };
      vHashGet(tsdb->commitInfo->ht, &tinfo, (void **)&info);
      if (info == NULL) {
        code = tsdbCommitInfoAdd(tsdb, fid);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      from.key.ts = maxKey + 1;
    }
  }

  taosThreadMutexLock(&tsdb->mutex);

  // scan tomb data
  if (tsdb->imem->nDel > 0) {
    TARRAY2_FOREACH(tsdb->pFS->fSetArr, fset) {
      if (tsdbTFileSetIsEmpty(fset)) {
        continue;
      }

      SFileSetCommitInfo *info;
      SFileSetCommitInfo  tinfo = {
           .fid = fset->fid,
      };

      // check if the file set already on the commit list
      vHashGet(tsdb->commitInfo->ht, &tinfo, (void **)&info);
      if (info != NULL) {
        continue;
      }

      int64_t minKey, maxKey;
      bool    hasDataToCommit = false;
      tsdbFidKeyRange(fset->fid, tsdb->keepCfg.days, tsdb->keepCfg.precision, &minKey, &maxKey);
      iter = tRBTreeIterCreate(tsdb->imem->tbDataTree, 1);
      for (SRBTreeNode *node = tRBTreeIterNext(&iter); node; node = tRBTreeIterNext(&iter)) {
        STbData *pTbData = TCONTAINER_OF(node, STbData, rbtn);
        for (SDelData *pDelData = pTbData->pHead; pDelData; pDelData = pDelData->pNext) {
          if (pDelData->sKey > maxKey || pDelData->eKey < minKey) {
            continue;
          } else {
            hasDataToCommit = true;
            if ((code = tsdbCommitInfoAdd(tsdb, fset->fid))) {
              taosThreadMutexUnlock(&tsdb->mutex);
              TSDB_CHECK_CODE(code, lino, _exit);
            }
            break;
          }
        }

        if (hasDataToCommit) {
          break;
        }
      }
    }
  }

  // begin tasks on file set
  for (int i = 0; i < taosArrayGetSize(tsdb->commitInfo->arr); i++) {
    SFileSetCommitInfo *info = *(SFileSetCommitInfo **)taosArrayGet(tsdb->commitInfo->arr, i);
    tsdbBeginTaskOnFileSet(tsdb, info->fid, &fset);
    if (fset) {
      tsdbTFileSetInitCopy(tsdb, fset, &info->fset);
    }
  }

  taosThreadMutexUnlock(&tsdb->mutex);

_exit:
  if (code) {
    tsdbCommitInfoDestroy(tsdb);
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(tsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbOpenCommitter(STsdb *tsdb, SCommitInfo *info, SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  committer->tsdb = tsdb;
  committer->minutes = tsdb->keepCfg.days;
  committer->precision = tsdb->keepCfg.precision;
  committer->minRow = info->info.config.tsdbCfg.minRows;
  committer->maxRow = info->info.config.tsdbCfg.maxRows;
  committer->cmprAlg = info->info.config.tsdbCfg.compression;
  committer->sttTrigger = info->info.config.sttTrigger;
  committer->szPage = info->info.config.tsdbPageSize;
  committer->compactVersion = INT64_MAX;
  committer->cid = tsdbFSAllocEid(tsdb->pFS);
  committer->now = taosGetTimestampSec();

  code = tsdbCommitInfoBuild(tsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

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

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, eid:%" PRId64, TD_VID(committer->tsdb->pVnode), __func__, lino,
              tstrerror(code), committer->cid);
  } else {
    tsdbDebug("vgId:%d %s done, eid:%" PRId64, TD_VID(committer->tsdb->pVnode), __func__, committer->cid);
  }
  return code;
}

int32_t tsdbPreCommit(STsdb *tsdb) {
  taosThreadMutexLock(&tsdb->mutex);
  ASSERT_CORE(tsdb->imem == NULL, "imem should be null to commit mem");
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
    SCommitter2 committer = {0};

    code = tsdbOpenCommitter(tsdb, info, &committer);
    TSDB_CHECK_CODE(code, lino, _exit);

    for (int32_t i = 0; i < taosArrayGetSize(tsdb->commitInfo->arr); i++) {
      committer.ctx->info = *(SFileSetCommitInfo **)taosArrayGet(tsdb->commitInfo->arr, i);
      code = tsdbCommitFileSet(&committer);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbCloseCommitter(&committer, code);
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

  if (tsdb->imem) {
    SMemTable *pMemTable = tsdb->imem;

    taosThreadMutexLock(&tsdb->mutex);

    if ((code = tsdbFSEditCommit(tsdb->pFS))) {
      taosThreadMutexUnlock(&tsdb->mutex);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tsdb->imem = NULL;

    for (int32_t i = 0; i < taosArrayGetSize(tsdb->commitInfo->arr); i++) {
      SFileSetCommitInfo *info = *(SFileSetCommitInfo **)taosArrayGet(tsdb->commitInfo->arr, i);
      if (info->fset) {
        tsdbFinishTaskOnFileSet(tsdb, info->fid);
      }
    }

    taosThreadMutexUnlock(&tsdb->mutex);

    tsdbCommitInfoDestroy(tsdb);
    tsdbUnrefMemTable(pMemTable, NULL, true);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(tsdb->pVnode), __func__, lino, tstrerror(code));
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

  taosThreadMutexLock(&pTsdb->mutex);
  for (int32_t i = 0; i < taosArrayGetSize(pTsdb->commitInfo->arr); i++) {
    SFileSetCommitInfo *info = *(SFileSetCommitInfo **)taosArrayGet(pTsdb->commitInfo->arr, i);
    if (info->fset) {
      tsdbFinishTaskOnFileSet(pTsdb, info->fid);
    }
  }
  taosThreadMutexUnlock(&pTsdb->mutex);
  tsdbCommitInfoDestroy(pTsdb);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}
