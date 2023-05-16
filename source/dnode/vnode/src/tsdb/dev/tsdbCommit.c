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
  STsdb *pTsdb;

  // config
  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int8_t  sttTrigger;

  SArray *aTbDataP;  // SArray<STbData *>
  SArray *aFileOp;   // SArray<STFileOp>
  int64_t eid;       // edit id

  // context
  TSKEY            nextKey;
  int32_t          fid;
  int32_t          expLevel;
  TSKEY            minKey;
  TSKEY            maxKey;
  const STFileSet *pFileSet;

  // writer
  SSttFileWriter *pWriter;
} SCommitter;

static int32_t open_committer_writer(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pCommitter->pTsdb;
  int32_t vid = TD_VID(pTsdb->pVnode);

  SSttFileWriterConfig config = {
      .pTsdb = pCommitter->pTsdb,
      .maxRow = pCommitter->maxRow,
      .szPage = pTsdb->pVnode->config.tsdbPageSize,
      .cmprAlg = pCommitter->cmprAlg,
      .pSkmTb = NULL,
      .pSkmRow = NULL,
      .aBuf = NULL,
  };

  if (pCommitter->pFileSet) {
    // TODO
    ASSERT(0);
  } else {
    config.file.type = TSDB_FTYPE_STT;

    if (tfsAllocDisk(pTsdb->pVnode->pTfs, pCommitter->expLevel, &config.file.did) < 0) {
      code = TSDB_CODE_FS_NO_VALID_DISK;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    config.file.fid = pCommitter->fid;
    config.file.cid = pCommitter->eid;
    config.file.size = 0;
    config.file.stt.level = 0;
    config.file.stt.nseg = 0;

    tsdbTFileInit(pTsdb, &config.file);
  }

  code = tsdbSttFWriterOpen(&config, &pCommitter->pWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d", vid, __func__, lino, tstrerror(code), pCommitter->fid);
  }
  return code;
}

static int32_t tsdbCommitWriteTSData(SCommitter *pCommitter, TABLEID *tbid, TSDBROW *pRow) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(pCommitter->pTsdb->pVnode);

  if (pCommitter->pWriter == NULL) {
    code = open_committer_writer(pCommitter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbSttFWriteTSData(pCommitter->pWriter, tbid, pRow);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d failed at line %d since %s", vid, lino, tstrerror(code));
  } else {
    tsdbTrace("vgId:%d %s done, fid:%d suid:%" PRId64 " uid:%" PRId64 " ts:%" PRId64 " version:%" PRId64, vid, __func__,
              pCommitter->fid, tbid->suid, tbid->uid, TSDBROW_KEY(pRow).ts, TSDBROW_KEY(pRow).version);
  }
  return 0;
}

static int32_t tsdbCommitWriteDelData(SCommitter *pCommitter, int64_t suid, int64_t uid, int64_t version, int64_t sKey,
                                      int64_t eKey) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t commit_timeseries_data(SCommitter *pCommitter) {
  int32_t    code = 0;
  int32_t    lino = 0;
  int64_t    nRow = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  int32_t    vid = TD_VID(pTsdb->pVnode);
  SMemTable *pMem = pTsdb->imem;

  if (pMem->nRow == 0) goto _exit;

  TSDBKEY from = {.ts = pCommitter->minKey, .version = VERSION_MIN};
  for (int32_t iTbData = 0; iTbData < taosArrayGetSize(pCommitter->aTbDataP); iTbData++) {
    STbDataIter iter;
    STbData    *pTbData = (STbData *)taosArrayGetP(pCommitter->aTbDataP, iTbData);

    tsdbTbDataIterOpen(pTbData, &from, 0, &iter);

    for (TSDBROW *pRow; (pRow = tsdbTbDataIterGet(&iter)) != NULL; tsdbTbDataIterNext(&iter)) {
      TSDBKEY rowKey = TSDBROW_KEY(pRow);

      if (rowKey.ts > pCommitter->maxKey) {
        pCommitter->nextKey = TMIN(pCommitter->nextKey, rowKey.ts);
        break;
      }

      code = tsdbCommitWriteTSData(pCommitter, (TABLEID *)pTbData, pRow);
      TSDB_CHECK_CODE(code, lino, _exit);

      nRow++;
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d nRow:%" PRId64, vid, __func__, pCommitter->fid, nRow);
  }
  return code;
}

static int32_t commit_delete_data(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino;

  return 0;

  ASSERTS(0, "TODO: Not implemented yet");

  int64_t    nDel = 0;
  SMemTable *pMem = pCommitter->pTsdb->imem;

  if (pMem->nDel == 0) {  // no del data
    goto _exit;
  }

  for (int32_t iTbData = 0; iTbData < taosArrayGetSize(pCommitter->aTbDataP); iTbData++) {
    STbData *pTbData = (STbData *)taosArrayGetP(pCommitter->aTbDataP, iTbData);

    for (SDelData *pDelData = pTbData->pHead; pDelData; pDelData = pDelData->pNext) {
      if (pDelData->eKey < pCommitter->minKey) continue;
      if (pDelData->sKey > pCommitter->maxKey) {
        pCommitter->nextKey = TMIN(pCommitter->nextKey, pDelData->sKey);
        continue;
      }

      code = tsdbCommitWriteDelData(pCommitter, pTbData->suid, pTbData->uid, pDelData->version,
                                    pDelData->sKey /* TODO */, pDelData->eKey /* TODO */);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d nDel:%" PRId64, TD_VID(pCommitter->pTsdb->pVnode), __func__, pCommitter->fid,
              pMem->nDel);
  }
  return code;
}

static int32_t commit_fset_start(SCommitter *pCommitter) {
  STsdb  *pTsdb = pCommitter->pTsdb;
  int32_t vid = TD_VID(pTsdb->pVnode);

  pCommitter->fid = tsdbKeyFid(pCommitter->nextKey, pCommitter->minutes, pCommitter->precision);
  tsdbFidKeyRange(pCommitter->fid, pCommitter->minutes, pCommitter->precision, &pCommitter->minKey,
                  &pCommitter->maxKey);
  pCommitter->expLevel = tsdbFidLevel(pCommitter->fid, &pTsdb->keepCfg, taosGetTimestampSec());
  pCommitter->nextKey = TSKEY_MAX;

  tsdbFSGetFSet(pTsdb->pFS, pCommitter->fid, &pCommitter->pFileSet);

  tsdbDebug("vgId:%d %s done, fid:%d minKey:%" PRId64 " maxKey:%" PRId64 " expLevel:%d", vid, __func__, pCommitter->fid,
            pCommitter->minKey, pCommitter->maxKey, pCommitter->expLevel);
  return 0;
}

static int32_t commit_fset_end(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(pCommitter->pTsdb->pVnode);

  if (pCommitter->pWriter == NULL) return 0;

  struct STFileOp *pFileOp = taosArrayReserve(pCommitter->aFileOp, 1);
  if (pFileOp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbSttFWriterClose(&pCommitter->pWriter, 0, pFileOp);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d failed at line %d since %s", vid, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", vid, __func__, pCommitter->fid);
  }
  return code;
}

static int32_t commit_fset(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(pCommitter->pTsdb->pVnode);

  // fset commit start
  code = commit_fset_start(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // commit fset
  code = commit_timeseries_data(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = commit_delete_data(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // fset commit end
  code = commit_fset_end(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", vid, __func__);
  }
  return code;
}

static int32_t open_committer(STsdb *pTsdb, SCommitInfo *pInfo, SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino;

  // set config
  memset(pCommitter, 0, sizeof(SCommitter));
  pCommitter->pTsdb = pTsdb;
  pCommitter->minutes = pTsdb->keepCfg.days;
  pCommitter->precision = pTsdb->keepCfg.precision;
  pCommitter->minRow = pInfo->info.config.tsdbCfg.minRows;
  pCommitter->maxRow = pInfo->info.config.tsdbCfg.maxRows;
  pCommitter->cmprAlg = pInfo->info.config.tsdbCfg.compression;
  pCommitter->sttTrigger = 2;  // TODO

  pCommitter->aTbDataP = tsdbMemTableGetTbDataArray(pTsdb->imem);
  pCommitter->aFileOp = taosArrayInit(16, sizeof(STFileOp));
  if (pCommitter->aTbDataP == NULL || pCommitter->aFileOp == NULL) {
    taosArrayDestroy(pCommitter->aTbDataP);
    taosArrayDestroy(pCommitter->aFileOp);
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }
  tsdbFSAllocEid(pTsdb->pFS, &pCommitter->eid);

  // start loop
  pCommitter->nextKey = pTsdb->imem->minKey;  // TODO

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

static int32_t close_committer(SCommitter *pCommiter, int32_t eno) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(pCommiter->pTsdb->pVnode);

  if (eno == 0) {
    code = tsdbFSEditBegin(pCommiter->pTsdb->pFS, pCommiter->eid, pCommiter->aFileOp, TSDB_FEDIT_COMMIT);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    // TODO
    ASSERT(0);
  }

  ASSERT(pCommiter->pWriter == NULL);
  taosArrayDestroy(pCommiter->aTbDataP);
  taosArrayDestroy(pCommiter->aFileOp);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, eid:%" PRId64, vid, __func__, lino, tstrerror(code),
              pCommiter->eid);
  } else {
    tsdbDebug("vgId:%d %s done, eid:%" PRId64, vid, __func__, pCommiter->eid);
  }
  return code;
}

int32_t tsdbPreCommit(STsdb *pTsdb) {
  taosThreadRwlockWrlock(&pTsdb->rwLock);
  ASSERT(pTsdb->imem == NULL);
  pTsdb->imem = pTsdb->mem;
  pTsdb->mem = NULL;
  taosThreadRwlockUnlock(&pTsdb->rwLock);
  return 0;
}

int32_t tsdbCommitBegin(STsdb *pTsdb, SCommitInfo *pInfo) {
  if (!pTsdb) return 0;

  int32_t    code = 0;
  int32_t    lino = 0;
  SMemTable *pMem = pTsdb->imem;

  if (pMem->nRow == 0 && pMem->nDel == 0) {
    taosThreadRwlockWrlock(&pTsdb->rwLock);
    pTsdb->imem = NULL;
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    tsdbUnrefMemTable(pMem, NULL, true);
  } else {
    SCommitter committer;

    code = open_committer(pTsdb, pInfo, &committer);
    TSDB_CHECK_CODE(code, lino, _exit);

    while (committer.nextKey != TSKEY_MAX) {
      code = commit_fset(&committer);
      if (code) {
        lino = __LINE__;
        break;
      }
    }

    code = close_committer(&committer, code);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done, nRow:%" PRId64 " nDel:%" PRId64, TD_VID(pTsdb->pVnode), __func__, pMem->nRow,
             pMem->nDel);
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