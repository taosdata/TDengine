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

#include "tsdb.h"

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
  TSKEY      nextKey;
  int32_t    fid;
  int32_t    expLevel;
  TSKEY      minKey;
  TSKEY      maxKey;
  int64_t    cid;  // commit id
  SSkmInfo   skmTable;
  SSkmInfo   skmRow;
  SBlockData bData;
  SColData   aColData[4];  // <suid, uid, ts, version>
  SArray    *aSttBlk;      // SArray<SSttBlk>
  SArray    *aDelBlk;      // SArray<SDelBlk>
} SCommitter;

static int32_t tsdbRowIsDeleted(SCommitter *pCommitter, TSDBROW *pRow) {
  // TODO
  ASSERT(0);
  return 0;
}

static int32_t tsdbCommitTimeSeriesData(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino;

  SMemTable *pMem = pCommitter->pTsdb->imem;

  if (pMem->nRow == 0) goto _exit;

  for (int32_t iTbData = 0; iTbData < taosArrayGetSize(pCommitter->aTbDataP); iTbData++) {
    STbData *pTbData = (STbData *)taosArrayGetP(pCommitter->aTbDataP, iTbData);

    // TODO: prepare commit next table

    STbDataIter iter;
    TSDBKEY     from = {.ts = pCommitter->minKey, .version = VERSION_MIN};
    tsdbTbDataIterOpen(pTbData, &from, 0, &iter);

    for (TSDBROW *pRow; (pRow = tsdbTbDataIterGet(&iter)) != NULL; tsdbTbDataIterNext(&iter)) {
      TSDBKEY rowKey = TSDBROW_KEY(pRow);

      if (rowKey.ts > pCommitter->maxKey) {
        pCommitter->nextKey = TMIN(rowKey.ts, pCommitter->nextKey);
        break;
      }

      if (pRow->type == TSDBROW_ROW_FMT) {
        // code = tsdbUpdateSkmInfo(&pCommitter->skmRow, pTbData->suid, pTbData->uid, TSDBROW_SVERSION(pRow));
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      code = tBlockDataAppendRow(&pCommitter->bData, pRow, pCommitter->skmRow.pTSchema, pTbData->uid);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (pCommitter->bData.nRow >= pCommitter->maxRow) {
        // code = tsdbWriteSttBlock(pCommitter);
        TSDB_CHECK_CODE(code, lino, _exit);

        tBlockDataClear(&pCommitter->bData);
      }
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d nRow:%" PRId64, TD_VID(pCommitter->pTsdb->pVnode), __func__, pCommitter->fid,
              pMem->nRow);
  }
  return code;
}

static int32_t tsdbCommitTombstoneData(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino;

  SMemTable *pMem = pCommitter->pTsdb->imem;

  if (pMem->nDel == 0) goto _exit;

  for (int32_t iTbData = 0; iTbData < taosArrayGetSize(pCommitter->aTbDataP); iTbData++) {
    STbData *pTbData = (STbData *)taosArrayGetP(pCommitter->aTbDataP, iTbData);

    if (pTbData->pHead == NULL) continue;

    for (SDelData *pDelData = pTbData->pHead; pDelData; pDelData = pDelData->pNext) {
      if (pDelData->sKey > pCommitter->maxKey || pDelData->eKey < pCommitter->minKey) continue;

      // code = tsdbAppendDelData(pCommitter, pTbData->suid, pTbData->uid, TMAX(pDelData->sKey, pCommitter->minKey),
      //                          TMIN(pDelData->eKey, pCommitter->maxKey), pDelData->version);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (/* TODO */ 0 > pCommitter->maxRow) {
        // code = tsdbWriteDelBlock(pCommitter);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
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

static int32_t tsdbCommitDelData(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO

_exit:
  if (code) {
    tsdbError("vgId:%d failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitFSetStart(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  pCommitter->fid = tsdbKeyFid(pCommitter->nextKey, pCommitter->minutes, pCommitter->precision);
  tsdbFidKeyRange(pCommitter->fid, pCommitter->minutes, pCommitter->precision, &pCommitter->minKey,
                  &pCommitter->maxKey);
  pCommitter->expLevel = tsdbFidLevel(pCommitter->fid, &pCommitter->pTsdb->keepCfg, taosGetTimestampSec());
#if 0
  // pCommitter->cid = tsdbFileSetNextCid(STsdb * pTsdb, pCommitter->fid);
#else
  pCommitter->cid = 0;
#endif

  // TODO

_exit:
  if (code) {
    tsdbError("vgId:%d failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitFSetEnd(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO

_exit:
  if (code) {
    tsdbError("vgId:%d failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitNextFSet(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCommitter->pTsdb;

  // fset commit start
  code = tsdbCommitFSetStart(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // commit fset
  code = tsdbCommitTimeSeriesData(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCommitTombstoneData(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // fset commit end
  code = tsdbCommitFSetEnd(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitterOpen(STsdb *pTsdb, SCommitInfo *pInfo, SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  memset(pCommitter, 0, sizeof(SCommitter));
  pCommitter->pTsdb = pTsdb;

  // TODO

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbCommitterClose(SCommitter *pCommiter, int32_t eno) {
  int32_t code = 0;
  // TODO
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

    code = tsdbCommitterOpen(pTsdb, pInfo, &committer);
    TSDB_CHECK_CODE(code, lino, _exit);

    while (committer.nextKey != TSKEY_MAX && (code = tsdbCommitNextFSet(&committer))) {
    }

    code = tsdbCommitterClose(&committer, code);
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

#if 0
int32_t tsdbCommitCommit(STsdb *pTsdb) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SMemTable *pMemTable = pTsdb->imem;

  // lock
  taosThreadRwlockWrlock(&pTsdb->rwLock);

  code = tsdbFSCommit(pTsdb);
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
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d, tsdb finish commit", TD_VID(pTsdb->pVnode));
  }
  return code;
}

int32_t tsdbCommitRollback(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFSRollback(pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d, tsdb rollback commit", TD_VID(pTsdb->pVnode));
  }
  return code;
}
#endif