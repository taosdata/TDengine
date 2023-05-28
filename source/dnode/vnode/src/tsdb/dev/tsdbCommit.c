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

  SArray      *aTbDataP;  // SArray<STbData *>
  TFileOpArray opArray;
  int64_t      eid;  // edit id

  // context
  TSKEY      nextKey;
  int32_t    fid;
  int32_t    expLevel;
  TSKEY      minKey;
  TSKEY      maxKey;
  STFileSet *fset;

  // writer
  SSttFileWriter *pWriter;
} SCommitter;

static int32_t open_writer_with_new_stt(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pCommitter->pTsdb;
  SVnode *pVnode = pTsdb->pVnode;
  int32_t vid = TD_VID(pVnode);

  SSttFileWriterConfig config;
  SDiskID              did;

  if (tfsAllocDisk(pVnode->pTfs, pCommitter->expLevel, &did) < 0) {
    code = TSDB_CODE_FS_NO_VALID_DISK;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  config.tsdb = pTsdb;
  config.maxRow = pCommitter->maxRow;
  config.szPage = pVnode->config.tsdbPageSize;
  config.cmprAlg = pCommitter->cmprAlg;
  config.skmTb = NULL;
  config.skmRow = NULL;
  config.aBuf = NULL;
  config.file.type = TSDB_FTYPE_STT;
  config.file.did = did;
  config.file.fid = pCommitter->fid;
  config.file.cid = pCommitter->eid;
  config.file.size = 0;
  config.file.stt->level = 0;
  config.file.stt->nseg = 0;

  code = tsdbSttFWriterOpen(&config, &pCommitter->pWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s success", vid, __func__);
  }
  return code;
}
static int32_t open_writer_with_exist_stt(SCommitter *pCommitter, const STFile *pFile) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pCommitter->pTsdb;
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

  code = tsdbSttFWriterOpen(&config, &pCommitter->pWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s success", vid, __func__);
  }
  return code;
}
static int32_t open_committer_writer(SCommitter *pCommitter) {
  if (!pCommitter->fset) {
    return open_writer_with_new_stt(pCommitter);
  }

  const SSttLvl *lvl0 = tsdbTFileSetGetLvl(pCommitter->fset, 0);
  if (lvl0 == NULL) {
    return open_writer_with_new_stt(pCommitter);
  }

  ASSERT(TARRAY2_SIZE(&lvl0->farr) > 0);
  STFileObj *fobj = TARRAY2_LAST(&lvl0->farr);
  if (fobj->f.stt->nseg >= pCommitter->sttTrigger) {
    return open_writer_with_new_stt(pCommitter);
  } else {
    return open_writer_with_exist_stt(pCommitter, &fobj->f);
  }
}

static int32_t tsdbCommitWriteTSData(SCommitter *pCommitter, SRowInfo *pRowInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(pCommitter->pTsdb->pVnode);

  if (pCommitter->pWriter == NULL) {
    code = open_committer_writer(pCommitter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbSttFWriteTSData(pCommitter->pWriter, pRowInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d failed at line %d since %s", vid, lino, tstrerror(code));
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
    SRowInfo    rowInfo = {.suid = pTbData->suid, .uid = pTbData->uid};

    tsdbTbDataIterOpen(pTbData, &from, 0, &iter);

    for (TSDBROW *pRow; (pRow = tsdbTbDataIterGet(&iter)) != NULL; tsdbTbDataIterNext(&iter)) {
      TSDBKEY rowKey = TSDBROW_KEY(pRow);

      if (rowKey.ts > pCommitter->maxKey) {
        pCommitter->nextKey = TMIN(pCommitter->nextKey, rowKey.ts);
        break;
      }

      rowInfo.row = *pRow;
      code = tsdbCommitWriteTSData(pCommitter, &rowInfo);
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

  tsdbFSGetFSet(pTsdb->pFS, pCommitter->fid, &pCommitter->fset);

  tsdbDebug("vgId:%d %s done, fid:%d minKey:%" PRId64 " maxKey:%" PRId64 " expLevel:%d", vid, __func__, pCommitter->fid,
            pCommitter->minKey, pCommitter->maxKey, pCommitter->expLevel);
  return 0;
}

static int32_t commit_fset_end(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(pCommitter->pTsdb->pVnode);

  if (pCommitter->pWriter == NULL) return 0;

  STFileOp op;
  code = tsdbSttFWriterClose(&pCommitter->pWriter, 0, &op);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (op.optype != TSDB_FOP_NONE) {
    code = TARRAY2_APPEND(&pCommitter->opArray, op);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

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
  pCommitter->sttTrigger = pInfo->info.config.sttTrigger;

  pCommitter->aTbDataP = tsdbMemTableGetTbDataArray(pTsdb->imem);
  if (pCommitter->aTbDataP == NULL) {
    taosArrayDestroy(pCommitter->aTbDataP);
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }
  TARRAY2_INIT(&pCommitter->opArray);
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
    code = tsdbFSEditBegin(pCommiter->pTsdb->pFS, &pCommiter->opArray, TSDB_FEDIT_COMMIT);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    // TODO
    ASSERT(0);
  }

  ASSERT(pCommiter->pWriter == NULL);
  taosArrayDestroy(pCommiter->aTbDataP);
  TARRAY2_CLEAR_FREE(&pCommiter->opArray, NULL);

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