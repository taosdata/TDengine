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
#include "tsdbMain.h"

typedef struct {
  int   minFid;
  int   midFid;
  int   maxFid;
  TSKEY minKey;
} SRtn;

typedef struct {
  SRtn         rtn;
  SCommitIter *iters;
  SRWHelper    whelper;
  SDataCols *  pDataCols;
} SCommitH;

static int          tsdbCommitTSData(STsdbRepo *pRepo);
static int          tsdbCommitMeta(STsdbRepo *pRepo);
static int          tsdbStartCommit(STsdbRepo *pRepo);
static void         tsdbEndCommit(STsdbRepo *pRepo, int eno);
static bool         tsdbHasDataToCommit(SCommitIter *iters, int nIters, TSKEY minKey, TSKEY maxKey);
static int          tsdbCommitToFile(STsdbRepo *pRepo, int fid, SCommitH *pch);
static SCommitIter *tsdbCreateCommitIters(STsdbRepo *pRepo);
static void         tsdbDestroyCommitIters(SCommitIter *iters, int maxTables);
static void         tsdbSeekCommitIter(SCommitIter *pIters, int nIters, TSKEY key);
static int          tsdbInitCommitH(STsdbRepo *pRepo, SCommitH *pch);
static void         tsdbDestroyCommitH(SCommitH *pch, int niter);
static void         tsdbGetRtnSnap(STsdbRepo *pRepo, SRtn *pRtn);
static int          tsdbGetFidLevel(int fid, SRtn *pRtn);

void *tsdbCommitData(STsdbRepo *pRepo) {
  if (tsdbStartCommit(pRepo) < 0) {
    tsdbError("vgId:%d failed to commit data while startting to commit since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  // Commit to update meta file
  if (tsdbCommitMeta(pRepo) < 0) {
    tsdbError("vgId:%d error occurs while committing META data since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  // Create the iterator to read from cache
  if (tsdbCommitTSData(pRepo) < 0) {
    tsdbError("vgId:%d error occurs while committing TS data since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  tsdbEndCommit(pRepo, TSDB_CODE_SUCCESS);
  return NULL;

_err:
  ASSERT(terrno != TSDB_CODE_SUCCESS);
  pRepo->code = terrno;

  tsdbEndCommit(pRepo, terrno);
  return NULL;
}

static int tsdbCommitTSData(STsdbRepo *pRepo) {
  SMemTable *pMem = pRepo->imem;
  STsdbCfg * pCfg = &(pRepo->config);
  SCommitH   ch = {0};

  if (pMem->numOfRows <= 0) return 0;

  if (tsdbInitCommitH(pRepo, &ch) < 0) {
    return -1;
  }

  // TODO
  int sfid = MIN(TSDB_KEY_FILEID(pMem->keyFirst, pCfg->daysPerFile, pCfg->precision), 1 /*TODO*/);
  int efid = MAX(TSDB_KEY_FILEID(pMem->keyLast, pCfg->daysPerFile, pCfg->precision), 1 /*TODO*/);

  for (int fid = sfid; fid <= efid; fid++) {
    if (tsdbCommitToFile(pRepo, fid, &ch) < 0) {
      tsdbDestroyCommitH(&ch, pMem->maxTables);
      return -1;
    }
  }

  tsdbDestroyCommitH(&ch, pMem->maxTables);
  return 0;
}

static int tsdbCommitMeta(STsdbRepo *pRepo) {
  SMemTable *pMem = pRepo->imem;
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  SActObj *  pAct = NULL;
  SActCont * pCont = NULL;

  if (listNEles(pMem->actList) <= 0) return 0;

  if (tdKVStoreStartCommit(pMeta->pStore) < 0) {
    tsdbError("vgId:%d failed to commit data while start commit meta since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  SListNode *pNode = NULL;

  while ((pNode = tdListPopHead(pMem->actList)) != NULL) {
    pAct = (SActObj *)pNode->data;
    if (pAct->act == TSDB_UPDATE_META) {
      pCont = (SActCont *)POINTER_SHIFT(pAct, sizeof(SActObj));
      if (tdUpdateKVStoreRecord(pMeta->pStore, pAct->uid, (void *)(pCont->cont), pCont->len) < 0) {
        tsdbError("vgId:%d failed to update meta with uid %" PRIu64 " since %s", REPO_ID(pRepo), pAct->uid,
                  tstrerror(terrno));
        tdKVStoreEndCommit(pMeta->pStore);
        goto _err;
      }
    } else if (pAct->act == TSDB_DROP_META) {
      if (tdDropKVStoreRecord(pMeta->pStore, pAct->uid) < 0) {
        tsdbError("vgId:%d failed to drop meta with uid %" PRIu64 " since %s", REPO_ID(pRepo), pAct->uid,
                  tstrerror(terrno));
        tdKVStoreEndCommit(pMeta->pStore);
        goto _err;
      }
    } else {
      ASSERT(false);
    }
  }

  if (tdKVStoreEndCommit(pMeta->pStore) < 0) {
    tsdbError("vgId:%d failed to commit data while end commit meta since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  // TODO: update meta file
  tsdbUpdateMFile(pRepo, &(pMeta->pStore.f));

  return 0;

_err:
  return -1;
}

static int tsdbStartCommit(STsdbRepo *pRepo) {
  SMemTable *pMem = pRepo->imem;

  tsdbInfo("vgId:%d start to commit! keyFirst %" PRId64 " keyLast %" PRId64 " numOfRows %" PRId64 " meta rows: %d",
           REPO_ID(pRepo), pMem->keyFirst, pMem->keyLast, pMem->numOfRows, listNEles(pMem->actList));

  if (tsdbFSNewTxn(pRepo) < 0) return -1;

  pRepo->code = TSDB_CODE_SUCCESS;
  return 0;
}

static void tsdbEndCommit(STsdbRepo *pRepo, int eno) {
  if (tsdbFSEndTxn(pRepo, eno != TSDB_CODE_SUCCESS) < 0) {
    eno = terrno;
  }

  tsdbInfo("vgId:%d commit over, %s", REPO_ID(pRepo), (eno == TSDB_CODE_SUCCESS) ? "succeed" : "failed");

  if (pRepo->appH.notifyStatus) pRepo->appH.notifyStatus(pRepo->appH.appH, TSDB_STATUS_COMMIT_OVER, eno);

  SMemTable *pIMem = pRepo->imem;
  tsdbLockRepo(pRepo);
  pRepo->imem = NULL;
  tsdbUnlockRepo(pRepo);
  tsdbUnRefMemTable(pRepo, pIMem);

  sem_post(&(pRepo->readyToCommit));
}

static bool tsdbHasDataToCommit(SCommitIter *iters, int nIters, TSKEY minKey, TSKEY maxKey) {
  for (int i = 0; i < nIters; i++) {
    TSKEY nextKey = tsdbNextIterKey((iters + i)->pIter);
    if (nextKey != TSDB_DATA_TIMESTAMP_NULL && (nextKey >= minKey && nextKey <= maxKey)) return true;
  }
  return false;
}

static int tsdbCommitToFile(STsdbRepo *pRepo, int fid, SCommitH *pch) {
  STsdbCfg * pCfg = &(pRepo->config);
  SMemTable *pMem = pRepo->imem;
  TSKEY      minKey, maxKey;
  SDFileSet *pOldSet = NULL;
  SDFileSet  newSet = {0};

  tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, fid, &minKey, &maxKey);

  if (pOldSet) {  // file exists
    int level = tsdbGetFidLevel(fid, &(pch->rtn));
    if (level < 0) {  // if out of data, remove it and ignore expired memory data
      tsdbRemoveExpiredDFileSet(pRepo, fid);
      tsdbSeekCommitIter(pch->iters, pMem->maxTables, maxKey + 1);
      return 0;
    }

    // Move the data file set to correct level
    tsdbMoveDFileSet(pOldSet, level);
  }

  if (tsdbHasDataToCommit(pch->iters, pMem->maxTables, minKey, maxKey)) {
    if (tsdbSetAndOpenHelperFile(&(pch->whelper), pOldSet, &newSet) < 0) return -1;

    if (tsdbLoadCompIdx(&pch->whelper, NULL) < 0) return -1;

    for (int tid = 0; tid < pMem->maxTables; tid++) {
      SCommitIter *pIter = pch->iters + tid;
      if (pIter->pTable == NULL) continue;

      if (tsdbSetHelperTable(&(pch->whelper), pIter->pTable, pRepo) < 0) return -1;

      TSDB_RLOCK_TABLE(pIter->pTable);

      if (pIter->pIter != NULL) {  // has data in memory to commit
      }

      TSDB_RUNLOCK_TABLE(pIter->pTable);
      if (tsdbMoveLastBlockIfNeccessary() < 0) return -1;

      if (tsdbWriteCompInfo() < 0) return -1;
    }

    if (tsdbWriteCompIdx() < 0) return -1;
  }

  if (/*file exists OR has data to commit*/) {
    tsdbUpdateDFileSet(pRepo, &newSet);
  }

  return 0;
}

static SCommitIter *tsdbCreateCommitIters(STsdbRepo *pRepo) {
  SMemTable *pMem = pRepo->imem;
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  SCommitIter *iters = (SCommitIter *)calloc(pMem->maxTables, sizeof(SCommitIter));
  if (iters == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  if (tsdbRLockRepoMeta(pRepo) < 0) goto _err;

  // reference all tables
  for (int i = 0; i < pMem->maxTables; i++) {
    if (pMeta->tables[i] != NULL) {
      tsdbRefTable(pMeta->tables[i]);
      iters[i].pTable = pMeta->tables[i];
    }
  }

  if (tsdbUnlockRepoMeta(pRepo) < 0) goto _err;

  for (int i = 0; i < pMem->maxTables; i++) {
    if ((iters[i].pTable != NULL) && (pMem->tData[i] != NULL) && (TABLE_UID(iters[i].pTable) == pMem->tData[i]->uid)) {
      if ((iters[i].pIter = tSkipListCreateIter(pMem->tData[i]->pData)) == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto _err;
      }

      tSkipListIterNext(iters[i].pIter);
    }
  }

  return iters;

_err:
  tsdbDestroyCommitIters(iters, pMem->maxTables);
  return NULL;
}

static void tsdbDestroyCommitIters(SCommitIter *iters, int maxTables) {
  if (iters == NULL) return;

  for (int i = 1; i < maxTables; i++) {
    if (iters[i].pTable != NULL) {
      tsdbUnRefTable(iters[i].pTable);
      tSkipListDestroyIter(iters[i].pIter);
    }
  }

  free(iters);
}

static void tsdbSeekCommitIter(SCommitIter *pIters, int nIters, TSKEY key) {
  for (int i = 0; i < nIters; i++) {
    SCommitIter *pIter = pIters + i;
    if (pIter->pTable == NULL) continue;
    if (pIter->pIter == NULL) continue;

    tsdbLoadDataFromCache(pIter->pTable, pIter->pIter, key-1, INT32_MAX, NULL, NULL, 0, true, NULL);
  }
}

static int tsdbInitCommitH(STsdbRepo *pRepo, SCommitH *pch) {
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  STsdbCfg * pCfg = &(pRepo->config);

  pch->iters = tsdbCreateCommitIters(pRepo);
  if (pch->iters == NULL) {
    tsdbError("vgId:%d failed to create commit iterator since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  if (tsdbInitWriteHelper(&(pch->whelper), pRepo) < 0) {
    tsdbError("vgId:%d failed to init write helper since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  if ((pch->pDataCols = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pCfg->maxRowsPerFileBlock)) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbError("vgId:%d failed to init data cols with maxRowBytes %d maxCols %d maxRowsPerFileBlock %d since %s",
              REPO_ID(pRepo), pMeta->maxCols, pMeta->maxRowBytes, pCfg->maxRowsPerFileBlock, tstrerror(terrno));
    return -1;
  }

  return 0;
}

static void tsdbDestroyCommitH(SCommitH *pch, int niter) {
  tdFreeDataCols(pch->pDataCols);
  tsdbDestroyCommitIters(pch->iters, niter);
  tsdbDestroyHelper(&(pch->whelper));
}

static void tsdbGetRtnSnap(STsdbRepo *pRepo, SRtn *pRtn) {
  STsdbCfg *pCfg = &(pRepo->config);
  TSKEY     minKey, midKey, maxKey, now;

  now = taosGetTimestamp(pCfg->precision);
  minKey = now - pCfg->keep * tsMsPerDay[pCfg->precision];
  midKey = now - pCfg->keep2 * tsMsPerDay[pCfg->precision];
  maxKey = now - pCfg->keep1 * tsMsPerDay[pCfg->precision];

  pRtn->minKey = minKey;
  pRtn->minFid = TSDB_KEY_FILEID(minKey, pCfg->daysPerFile, pCfg->precision);
  pRtn->midFid = TSDB_KEY_FILEID(midKey, pCfg->daysPerFile, pCfg->precision);
  pRtn->maxFid = TSDB_KEY_FILEID(maxKey, pCfg->daysPerFile, pCfg->precision);
}

static int tsdbGetFidLevel(int fid, SRtn *pRtn) {
  if (fid >= pRtn->maxFid) {
    return 0;
  } else if (fid >= pRtn->midFid) {
    return 1;
  } else if (fid >= pRtn->minFid) {
    return 2;
  } else {
    return -1;
  }
}