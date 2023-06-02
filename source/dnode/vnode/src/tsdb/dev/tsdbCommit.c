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

#include "inc/tsdbCommit.h"

// extern dependencies
typedef struct {
  STsdb         *tsdb;
  TFileSetArray *fsetArr;

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
    int32_t    fid;
    int32_t    expLevel;
    TSKEY      minKey;
    TSKEY      maxKey;
    STFileSet *fset;
    TABLEID    tbid[1];
  } ctx[1];

  TFileOpArray   fopArray[1];
  TTsdbIterArray iterArray[1];
  SIterMerger   *iterMerger;

  // writer
  SSttFileWriter  *sttWriter;
  SDataFileWriter *dataWriter;
} SCommitter2;

static int32_t tsdbCommitOpenNewSttWriter(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  SDiskID did[1];
  if (tfsAllocDisk(committer->tsdb->pVnode->pTfs, committer->ctx->expLevel, did) < 0) {
    code = TSDB_CODE_FS_NO_VALID_DISK;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SSttFileWriterConfig config[1] = {{
      .tsdb = committer->tsdb,
      .maxRow = committer->maxRow,
      .szPage = committer->tsdb->pVnode->config.tsdbPageSize,
      .cmprAlg = committer->cmprAlg,
      .compactVersion = committer->compactVersion,
      .file =
          {
              .type = TSDB_FTYPE_STT,
              .did = did[0],
              .fid = committer->ctx->fid,
              .cid = committer->ctx->cid,
          },
  }};

  code = tsdbSttFileWriterOpen(config, &committer->sttWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s success", TD_VID(committer->tsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbCommitOpenExistSttWriter(SCommitter2 *committer, const STFile *f) {
  int32_t code = 0;
  int32_t lino = 0;

  SSttFileWriterConfig config[1] = {{
      .tsdb = committer->tsdb,
      .maxRow = committer->maxRow,
      .szPage = committer->szPage,
      .cmprAlg = committer->cmprAlg,
      .compactVersion = committer->compactVersion,
      .file = f[0],
  }};

  code = tsdbSttFileWriterOpen(config, &committer->sttWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s success", TD_VID(committer->tsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbCommitOpenWriter(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  // stt writer
  if (!committer->ctx->fset) {
    return tsdbCommitOpenNewSttWriter(committer);
  }

  const SSttLvl *lvl0 = tsdbTFileSetGetSttLvl(committer->ctx->fset, 0);
  if (lvl0 == NULL || TARRAY2_SIZE(lvl0->fobjArr) == 0) {
    return tsdbCommitOpenNewSttWriter(committer);
  }

  STFileObj *fobj = TARRAY2_LAST(lvl0->fobjArr);
  if (fobj->f->stt->nseg >= committer->sttTrigger) {
    return tsdbCommitOpenNewSttWriter(committer);
  } else {
    return tsdbCommitOpenExistSttWriter(committer, fobj->f);
  }

  // data writer
  if (0) {
    // TODO
  }

  return code;
}

static int32_t tsdbCommitWriteDelData(SCommitter2 *committer, int64_t suid, int64_t uid, int64_t version, int64_t sKey,
                                      int64_t eKey) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t tsdbCommitTSData(SCommitter2 *committer) {
  int32_t   code = 0;
  int32_t   lino = 0;
  int64_t   nRow = 0;
  int32_t   vid = TD_VID(committer->tsdb->pVnode);
  SRowInfo *row;

  if (committer->tsdb->imem->nRow == 0) goto _exit;

  // open iter and iter merger
  STsdbIter      *iter;
  STsdbIterConfig config[1] = {{
      .type = TSDB_ITER_TYPE_MEMT,
      .memt = committer->tsdb->imem,
      .from = {{
          .ts = committer->ctx->minKey,
          .version = VERSION_MIN,
      }},
  }};

  code = tsdbIterOpen(config, &iter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = TARRAY2_APPEND(committer->iterArray, iter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbIterMergerOpen(committer->iterArray, &committer->iterMerger);
  TSDB_CHECK_CODE(code, lino, _exit);

  // loop iter
  while ((row = tsdbIterMergerGet(committer->iterMerger)) != NULL) {
    if (row->uid != committer->ctx->tbid->uid) {
      committer->ctx->tbid->suid = row->suid;
      committer->ctx->tbid->uid = row->uid;

      // Ignore table of obsolescence
      SMetaInfo info[1];
      if (metaGetInfo(committer->tsdb->pVnode->pMeta, row->uid, info, NULL) != 0) {
        code = tsdbIterMergerSkipTableData(committer->iterMerger, committer->ctx->tbid);
        TSDB_CHECK_CODE(code, lino, _exit);
        continue;
      }
    }

    TSKEY ts = TSDBROW_TS(&row->row);
    if (ts > committer->ctx->maxKey) {
      committer->ctx->nextKey = TMIN(committer->ctx->nextKey, ts);
      code = tsdbIterMergerSkipTableData(committer->iterMerger, committer->ctx->tbid);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbSttFileWriteTSData(committer->sttWriter, row);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbIterMergerNext(committer->iterMerger);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d nRow:%" PRId64, vid, __func__, committer->ctx->fid, nRow);
  }
  return code;
}

static int32_t tsdbCommitDelData(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino;

  return 0;

#if 0
  ASSERTS(0, "TODO: Not implemented yet");

  int64_t    nDel = 0;
  SMemTable *pMem = committer->tsdb->imem;

  if (pMem->nDel == 0) {  // no del data
    goto _exit;
  }

  for (int32_t iTbData = 0; iTbData < taosArrayGetSize(committer->aTbDataP); iTbData++) {
    STbData *pTbData = (STbData *)taosArrayGetP(committer->aTbDataP, iTbData);

    for (SDelData *pDelData = pTbData->pHead; pDelData; pDelData = pDelData->pNext) {
      if (pDelData->eKey < committer->ctx->minKey) continue;
      if (pDelData->sKey > committer->ctx->maxKey) {
        committer->ctx->nextKey = TMIN(committer->ctx->nextKey, pDelData->sKey);
        continue;
      }

      code = tsdbCommitWriteDelData(committer, pTbData->suid, pTbData->uid, pDelData->version,
                                    pDelData->sKey /* TODO */, pDelData->eKey /* TODO */);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d failed at line %d since %s", TD_VID(committer->tsdb->pVnode), lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d nDel:%" PRId64, TD_VID(committer->tsdb->pVnode), __func__, committer->ctx->fid,
              pMem->nDel);
  }
  return code;
#endif
}

static int32_t tsdbCommitFileSetBegin(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *tsdb = committer->tsdb;
  int32_t vid = TD_VID(tsdb->pVnode);

  committer->ctx->fid = tsdbKeyFid(committer->ctx->nextKey, committer->minutes, committer->precision);
  committer->ctx->expLevel = tsdbFidLevel(committer->ctx->fid, &tsdb->keepCfg, committer->ctx->now);
  tsdbFidKeyRange(committer->ctx->fid, committer->minutes, committer->precision, &committer->ctx->minKey,
                  &committer->ctx->maxKey);
  STFileSet fset = {.fid = committer->ctx->fid};
  committer->ctx->fset = &fset;
  committer->ctx->fset = TARRAY2_SEARCH_EX(committer->fsetArr, &committer->ctx->fset, tsdbTFileSetCmprFn, TD_EQ);
  committer->ctx->tbid->suid = 0;
  committer->ctx->tbid->uid = 0;

  ASSERT(TARRAY2_SIZE(committer->iterArray) == 0);
  ASSERT(committer->iterMerger == NULL);
  ASSERT(committer->sttWriter == NULL);
  ASSERT(committer->dataWriter == NULL);

  code = tsdbCommitOpenWriter(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  // reset nextKey
  committer->ctx->nextKey = TSKEY_MAX;

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d minKey:%" PRId64 " maxKey:%" PRId64 " expLevel:%d", vid, __func__,
              committer->ctx->fid, committer->ctx->minKey, committer->ctx->maxKey, committer->ctx->expLevel);
  }
  return 0;
}

static int32_t tsdbCommitFileSetEnd(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbSttFileWriterClose(&committer->sttWriter, 0, committer->fopArray);
  TSDB_CHECK_CODE(code, lino, _exit);

  tsdbIterMergerClose(&committer->iterMerger);
  TARRAY2_CLEAR(committer->iterArray, tsdbIterClose);

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
  int32_t vid = TD_VID(committer->tsdb->pVnode);

  // fset commit start
  code = tsdbCommitFileSetBegin(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  // commit fset
  code = tsdbCommitTSData(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCommitDelData(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  // fset commit end
  code = tsdbCommitFileSetEnd(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", vid, __func__, committer->ctx->fid);
  }
  return code;
}

static int32_t tsdbOpenCommitter(STsdb *tsdb, SCommitInfo *info, SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(tsdb->pVnode);

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
  committer->ctx->nextKey = tsdb->imem->minKey;  // TODO

  TARRAY2_INIT(committer->fopArray);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(tsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbCloseCommitter(SCommitter2 *committer, int32_t eno) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(committer->tsdb->pVnode);

  if (eno == 0) {
    code = tsdbFSEditBegin(committer->tsdb->pFS, committer->fopArray, TSDB_FEDIT_COMMIT);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    // TODO
    ASSERT(0);
  }

  ASSERT(committer->dataWriter == NULL);
  ASSERT(committer->sttWriter == NULL);
  ASSERT(committer->iterMerger == NULL);
  TARRAY2_FREE(committer->iterArray);
  TARRAY2_FREE(committer->fopArray);
  tsdbFSDestroyCopySnapshot(&committer->fsetArr);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, eid:%" PRId64, vid, __func__, lino, tstrerror(code),
              committer->ctx->cid);
  } else {
    tsdbDebug("vgId:%d %s done, eid:%" PRId64, vid, __func__, committer->ctx->cid);
  }
  return code;
}

int32_t tsdbPreCommit(STsdb *tsdb) {
  taosThreadRwlockWrlock(&tsdb->rwLock);
  ASSERT(tsdb->imem == NULL);
  tsdb->imem = tsdb->mem;
  tsdb->mem = NULL;
  taosThreadRwlockUnlock(&tsdb->rwLock);
  return 0;
}

int32_t tsdbCommitBegin(STsdb *tsdb, SCommitInfo *info) {
  if (!tsdb) return 0;

  int32_t    code = 0;
  int32_t    lino = 0;
  int32_t    vid = TD_VID(tsdb->pVnode);
  SMemTable *imem = tsdb->imem;
  int64_t    nRow = imem->nRow;
  int64_t    nDel = imem->nDel;

  if (!nRow && !nDel) {
    taosThreadRwlockWrlock(&tsdb->rwLock);
    tsdb->imem = NULL;
    taosThreadRwlockUnlock(&tsdb->rwLock);
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
    TSDB_ERROR_LOG(vid, lino, code);
  } else {
    tsdbInfo("vgId:%d %s done, nRow:%" PRId64 " nDel:%" PRId64, vid, __func__, nRow, nDel);
  }
  return code;
}

int32_t tsdbCommitCommit(STsdb *tsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tsdb->imem == NULL) goto _exit;

  SMemTable *pMemTable = tsdb->imem;
  taosThreadRwlockWrlock(&tsdb->rwLock);
  code = tsdbFSEditCommit(tsdb->pFS);
  if (code) {
    taosThreadRwlockUnlock(&tsdb->rwLock);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  tsdb->imem = NULL;
  taosThreadRwlockUnlock(&tsdb->rwLock);
  tsdbUnrefMemTable(pMemTable, NULL, true);

  // TODO: make this call async
  code = tsdbMerge(tsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

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
  int32_t vid = TD_VID(pTsdb->pVnode);

  if (pTsdb->imem == NULL) goto _exit;

  code = tsdbFSEditAbort(pTsdb->pFS);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done", vid, __func__);
  }
  return code;
}