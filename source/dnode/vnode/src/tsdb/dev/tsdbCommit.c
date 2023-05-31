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
  STsdb  *tsdb;
  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int8_t  sttTrigger;
  int64_t compactVersion;

  struct {
    int64_t    now;
    TSKEY      nextKey;
    int32_t    fid;
    int32_t    expLevel;
    TSKEY      minKey;
    TSKEY      maxKey;
    STFileSet *fset;
    TABLEID    tbid[1];
  } ctx[1];

  int64_t        eid;  // edit id
  TFileOpArray   fopArray[1];
  TTsdbIterArray iterArray[1];
  SIterMerger   *iterMerger;

  // writer
  SDataFileWriter *dataWriter;
  SSttFileWriter  *sttWriter;
} SCommitter2;

static int32_t tsdbCommitOpenNewSttWriter(SCommitter2 *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pCommitter->tsdb;
  SVnode *pVnode = pTsdb->pVnode;
  int32_t vid = TD_VID(pVnode);

  SSttFileWriterConfig config[1];
  SDiskID              did[1];

  if (tfsAllocDisk(pVnode->pTfs, pCommitter->ctx->expLevel, did) < 0) {
    code = TSDB_CODE_FS_NO_VALID_DISK;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  config->tsdb = pTsdb;
  config->maxRow = pCommitter->maxRow;
  config->szPage = pVnode->config.tsdbPageSize;
  config->cmprAlg = pCommitter->cmprAlg;
  config->skmTb = NULL;
  config->skmRow = NULL;
  config->aBuf = NULL;
  config->file.type = TSDB_FTYPE_STT;
  config->file.did = did[0];
  config->file.fid = pCommitter->ctx->fid;
  config->file.cid = pCommitter->eid;
  config->file.size = 0;
  config->file.stt->level = 0;
  config->file.stt->nseg = 0;

  code = tsdbSttFileWriterOpen(config, &pCommitter->sttWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s success", vid, __func__);
  }
  return code;
}
static int32_t tsdbCommitOpenExistSttWriter(SCommitter2 *pCommitter, const STFile *pFile) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pCommitter->tsdb;
  SVnode *pVnode = pTsdb->pVnode;
  int32_t vid = TD_VID(pVnode);

  SSttFileWriterConfig config = {
      //
      .tsdb = pTsdb,
      .maxRow = pCommitter->maxRow,
      .szPage = pVnode->config.tsdbPageSize,
      .cmprAlg = pCommitter->cmprAlg,
      .skmTb = NULL,
      .skmRow = NULL,
      .aBuf = NULL,
      .file = *pFile  //
  };

  code = tsdbSttFileWriterOpen(&config, &pCommitter->sttWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s success", vid, __func__);
  }
  return code;
}
static int32_t tsdbCommitOpenWriter(SCommitter2 *committer) {
  if (!committer->ctx->fset) {
    return tsdbCommitOpenNewSttWriter(committer);
  }

  const SSttLvl *lvl0 = tsdbTFileSetGetLvl(committer->ctx->fset, 0);
  if (lvl0 == NULL) {
    return tsdbCommitOpenNewSttWriter(committer);
  }

  ASSERT(TARRAY2_SIZE(lvl0->fobjArr) > 0);
  STFileObj *fobj = TARRAY2_LAST(lvl0->fobjArr);
  if (fobj->f->stt->nseg >= committer->sttTrigger) {
    return tsdbCommitOpenNewSttWriter(committer);
  } else {
    return tsdbCommitOpenExistSttWriter(committer, fobj->f);
  }
}

static int32_t tsdbCommitTSRow(SCommitter2 *committer, SRowInfo *row) {
  return tsdbSttFileWriteTSData(committer->sttWriter, row);
}

static int32_t tsdbCommitWriteDelData(SCommitter2 *pCommitter, int64_t suid, int64_t uid, int64_t version, int64_t sKey,
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

  code = tsdbIterMergerInit(committer->iterArray, &committer->iterMerger);
  TSDB_CHECK_CODE(code, lino, _exit);

  // loop iter
  while ((row = tsdbIterMergerGet(committer->iterMerger)) != NULL) {
    if (row->uid != committer->ctx->tbid->uid) {
      committer->ctx->tbid->suid = row->suid;
      committer->ctx->tbid->uid = row->uid;

      // Ignore deleted table
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
      code = tsdbCommitTSRow(committer, row);
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

static int32_t tsdbCommitDelData(SCommitter2 *pCommitter) {
  int32_t code = 0;
  int32_t lino;

  return 0;

#if 0
  ASSERTS(0, "TODO: Not implemented yet");

  int64_t    nDel = 0;
  SMemTable *pMem = pCommitter->tsdb->imem;

  if (pMem->nDel == 0) {  // no del data
    goto _exit;
  }

  for (int32_t iTbData = 0; iTbData < taosArrayGetSize(pCommitter->aTbDataP); iTbData++) {
    STbData *pTbData = (STbData *)taosArrayGetP(pCommitter->aTbDataP, iTbData);

    for (SDelData *pDelData = pTbData->pHead; pDelData; pDelData = pDelData->pNext) {
      if (pDelData->eKey < pCommitter->ctx->minKey) continue;
      if (pDelData->sKey > pCommitter->ctx->maxKey) {
        pCommitter->ctx->nextKey = TMIN(pCommitter->ctx->nextKey, pDelData->sKey);
        continue;
      }

      code = tsdbCommitWriteDelData(pCommitter, pTbData->suid, pTbData->uid, pDelData->version,
                                    pDelData->sKey /* TODO */, pDelData->eKey /* TODO */);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d failed at line %d since %s", TD_VID(pCommitter->tsdb->pVnode), lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d nDel:%" PRId64, TD_VID(pCommitter->tsdb->pVnode), __func__, pCommitter->ctx->fid,
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
  tsdbFidKeyRange(committer->ctx->fid, committer->minutes, committer->precision, &committer->ctx->minKey,
                  &committer->ctx->maxKey);
  committer->ctx->expLevel = tsdbFidLevel(committer->ctx->fid, &tsdb->keepCfg, committer->ctx->now);
  committer->ctx->nextKey = TSKEY_MAX;

  // TODO: use a thread safe function to get fset
  tsdbFSGetFSet(tsdb->pFS, committer->ctx->fid, &committer->ctx->fset);

  code = tsdbCommitOpenWriter(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d minKey:%" PRId64 " maxKey:%" PRId64 " expLevel:%d", vid, __func__,
              committer->ctx->fid, committer->ctx->minKey, committer->ctx->maxKey, committer->ctx->expLevel);
  }
  return 0;
}

static int32_t tsdbCommitFileSetEnd(SCommitter2 *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(pCommitter->tsdb->pVnode);

  if (pCommitter->sttWriter == NULL) return 0;

  STFileOp op;
  code = tsdbSttFileWriterClose(&pCommitter->sttWriter, 0, &op);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (op.optype != TSDB_FOP_NONE) {
    code = TARRAY2_APPEND(pCommitter->fopArray, op);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d failed at line %d since %s", vid, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", vid, __func__, pCommitter->ctx->fid);
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
  committer->minutes = tsdb->keepCfg.days;
  committer->precision = tsdb->keepCfg.precision;
  committer->minRow = info->info.config.tsdbCfg.minRows;
  committer->maxRow = info->info.config.tsdbCfg.maxRows;
  committer->cmprAlg = info->info.config.tsdbCfg.compression;
  committer->sttTrigger = info->info.config.sttTrigger;
  committer->compactVersion = INT64_MAX;  // TODO: use a function

  TARRAY2_INIT(committer->fopArray);
  tsdbFSAllocEid(tsdb->pFS, &committer->eid);

  committer->ctx->now = taosGetTimestampSec();
  committer->ctx->nextKey = tsdb->imem->minKey;  // TODO

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(tsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbCloseCommitter(SCommitter2 *pCommiter, int32_t eno) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(pCommiter->tsdb->pVnode);

  if (eno == 0) {
    code = tsdbFSEditBegin(pCommiter->tsdb->pFS, pCommiter->fopArray, TSDB_FEDIT_COMMIT);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    // TODO
    ASSERT(0);
  }

  ASSERT(pCommiter->sttWriter == NULL);
  TARRAY2_FREE(pCommiter->fopArray);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, eid:%" PRId64, vid, __func__, lino, tstrerror(code),
              pCommiter->eid);
  } else {
    tsdbDebug("vgId:%d %s done, eid:%" PRId64, vid, __func__, pCommiter->eid);
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
  SMemTable *memt = tsdb->imem;
  int64_t    nRow = memt->nRow;
  int64_t    nDel = memt->nDel;

  if (!nRow && !nDel) {
    taosThreadRwlockWrlock(&tsdb->rwLock);
    tsdb->imem = NULL;
    taosThreadRwlockUnlock(&tsdb->rwLock);
    tsdbUnrefMemTable(memt, NULL, true);
  } else {
    SCommitter2 committer[1];

    code = tsdbOpenCommitter(tsdb, info, committer);
    TSDB_CHECK_CODE(code, lino, _exit);

    while (committer->ctx->nextKey != TSKEY_MAX) {
      code = tsdbCommitFileSet(committer);
      if (code) {
        lino = __LINE__;
        break;
      }
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

int32_t tsdbCommitCommit(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(pTsdb->pVnode);

  if (pTsdb->imem == NULL) goto _exit;

  SMemTable *pMemTable = pTsdb->imem;
  taosThreadRwlockWrlock(&pTsdb->rwLock);
  code = tsdbFSEditCommit(pTsdb->pFS);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pTsdb->imem = NULL;
  taosThreadRwlockUnlock(&pTsdb->rwLock);
  tsdbUnrefMemTable(pMemTable, NULL, true);

  // TODO: make this call async
  code = tsdbMerge(pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done", vid, __func__);
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