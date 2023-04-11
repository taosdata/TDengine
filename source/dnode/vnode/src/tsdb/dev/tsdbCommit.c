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

#include "dev.h"

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
  SArray *aTbDataP;
  // context
  TSKEY   nextKey;
  int32_t fid;
  int32_t expLevel;
  TSKEY   minKey;
  TSKEY   maxKey;
  // writer
  SArray             *aFileOp;
  struct SSttFWriter *pWriter;
} SCommitter;

static int32_t open_committer_writer(SCommitter *pCommitter) {
  int32_t code;
  int32_t lino;

  struct SSttFWriterConf conf = {
      .pTsdb = pCommitter->pTsdb,
      .maxRow = pCommitter->maxRow,
      .szPage = pCommitter->pTsdb->pVnode->config.tsdbPageSize,
      .cmprAlg = pCommitter->cmprAlg,
      .pSkmTb = NULL,
      .pSkmRow = NULL,
      .aBuf = NULL,
  };

  // pCommitter->pTsdb->pFS = NULL;
  // taosbsearch(pCommitter->pTsdb->pFS->aFileSet, &pCommitter->fid, tsdbCompareFid, &lino);
  struct SFileSet *pSet = NULL;
  if (pSet == NULL) {
    conf.file = (struct STFile){
        .cid = 1,
        .fid = pCommitter->fid,
        .diskId = (SDiskID){0},
        .type = TSDB_FTYPE_STT,
    };
    tsdbTFileInit(pCommitter->pTsdb, &conf.file);
  } else {
    // TODO
    ASSERT(0);
  }

  code = tsdbSttFWriterOpen(&conf, &pCommitter->pWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code), pCommitter->fid);
  }
  return code;
}

static int32_t tsdbCommitWriteTSData(SCommitter *pCommitter, TABLEID *tbid, TSDBROW *pRow) {
  int32_t code = 0;
  int32_t lino;

  if (pCommitter->pWriter == NULL) {
    code = open_committer_writer(pCommitter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbSttFWriteTSData(pCommitter->pWriter, tbid, pRow);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), lino, tstrerror(code));
  } else {
    tsdbTrace("vgId:%d %s done, fid:%d suid:%" PRId64 " uid:%" PRId64 " ts:%" PRId64 " version:%" PRId64,
              TD_VID(pCommitter->pTsdb->pVnode), __func__, pCommitter->fid, tbid->suid, tbid->uid, TSDBROW_KEY(pRow).ts,
              TSDBROW_KEY(pRow).version);
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
  int32_t code = 0;
  int32_t lino;

  int64_t    nRow = 0;
  SMemTable *pMem = pCommitter->pTsdb->imem;

  if (pMem->nRow == 0) {  // no time-series data to commit
    goto _exit;
  }

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
    tsdbError(                                    //
        "vgId:%d %s failed at line %d since %s",  //
        TD_VID(pCommitter->pTsdb->pVnode),        //
        __func__,                                 //
        lino,                                     //
        tstrerror(code));
  } else {
    tsdbDebug(                                    //
        "vgId:%d %s done, fid:%d nRow:%" PRId64,  //
        TD_VID(pCommitter->pTsdb->pVnode),        //
        __func__,                                 //
        pCommitter->fid,                          //
        nRow);
  }
  return code;
}

static int32_t commit_delete_data(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino;

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

static int32_t start_commit_file_set(SCommitter *pCommitter) {
  pCommitter->fid = tsdbKeyFid(pCommitter->nextKey, pCommitter->minutes, pCommitter->precision);
  tsdbFidKeyRange(pCommitter->fid, pCommitter->minutes, pCommitter->precision, &pCommitter->minKey,
                  &pCommitter->maxKey);
  pCommitter->expLevel = tsdbFidLevel(pCommitter->fid, &pCommitter->pTsdb->keepCfg, taosGetTimestampSec());
  pCommitter->nextKey = TSKEY_MAX;

  tsdbDebug(                                                                        //
      "vgId:%d %s done, fid:%d minKey:%" PRId64 " maxKey:%" PRId64 " expLevel:%d",  //
      TD_VID(pCommitter->pTsdb->pVnode),                                            //
      __func__,                                                                     //
      pCommitter->fid,                                                              //
      pCommitter->minKey,                                                           //
      pCommitter->maxKey,                                                           //
      pCommitter->expLevel);
  return 0;
}

static int32_t end_commit_file_set(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO

_exit:
  if (code) {
    tsdbError("vgId:%d failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), lino, tstrerror(code));
  }
  return code;
}

static int32_t commit_next_file_set(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  // fset commit start
  code = start_commit_file_set(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // commit fset
  code = commit_timeseries_data(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* TODO
  code = commit_delete_data(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);
  */

  // fset commit end
  code = end_commit_file_set(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
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
  pCommitter->sttTrigger = 7;  // TODO

  pCommitter->aTbDataP = tsdbMemTableGetTbDataArray(pTsdb->imem);
  if (pCommitter->aTbDataP == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pCommitter->aFileOp = taosArrayInit(16, sizeof(struct SFileOp));
  if (pCommitter->aFileOp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // start loop
  pCommitter->nextKey = pTsdb->imem->minKey;  // TODO

_exit:
  if (code) {
    tsdbError(                                    //
        "vgId:%d %s failed at line %d since %s",  //
        TD_VID(pTsdb->pVnode),                    //
        __func__,                                 //
        lino,                                     //
        tstrerror(code));
  } else {
    tsdbDebug(                  //
        "vgId:%d %s done",      //
        TD_VID(pTsdb->pVnode),  //
        __func__);
  }
  return code;
}

static int32_t close_committer(SCommitter *pCommiter, int32_t eno) {
  int32_t code = 0;
  int32_t lino;

  code = tsdbFileSystemEditBegin(pCommiter->pTsdb->pFS,  //
                                 pCommiter->aFileOp,     //
                                 TSDB_FS_EDIT_COMMIT);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
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
      code = commit_next_file_set(&committer);
      if (code) break;
    }

    code = close_committer(&committer, code);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s",  //
              TD_VID(pTsdb->pVnode),                    //
              __func__,                                 //
              lino,                                     //
              tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done, nRow:%" PRId64 " nDel:%" PRId64,  //
             TD_VID(pTsdb->pVnode),                              //
             __func__,                                           //
             pMem->nRow,                                         //
             pMem->nDel);
  }
  return code;
}

int32_t tsdbCommitCommit(STsdb *pTsdb) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SMemTable *pMemTable = pTsdb->imem;

  // lock
  taosThreadRwlockWrlock(&pTsdb->rwLock);

  code = tsdbFileSystemEditCommit(pTsdb->pFS,  //
                                  TSDB_FS_EDIT_COMMIT);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pTsdb->imem = NULL;

  // unlock
  taosThreadRwlockUnlock(&pTsdb->rwLock);
  if (pMemTable) {
    tsdbUnrefMemTable(pMemTable, NULL, true);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s",  //
              TD_VID(pTsdb->pVnode),                     //
              __func__,                                  //
              lino,                                      //
              tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done",  //
             TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbCommitAbort(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFileSystemEditAbort(pTsdb->pFS,  //
                                 TSDB_FS_EDIT_COMMIT);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s",  //
              TD_VID(pTsdb->pVnode),                     //
              __func__,                                  //
              lino,                                      //
              tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done",      //
             TD_VID(pTsdb->pVnode),  //
             __func__);
  }
  return code;
}