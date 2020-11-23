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

static int  tsdbCommitTSData(STsdbRepo *pRepo);
static int  tsdbCommitMeta(STsdbRepo *pRepo);
static void tsdbEndCommit(STsdbRepo *pRepo, int eno);
static bool tsdbHasDataToCommit(SCommitIter *iters, int nIters, TSKEY minKey, TSKEY maxKey);
static int  tsdbCommitToFile(STsdbRepo *pRepo, int fid, SCommitIter *iters, SRWHelper *pHelper, SDataCols *pDataCols);
static SCommitIter *tsdbCreateCommitIters(STsdbRepo *pRepo);
static void         tsdbDestroyCommitIters(SCommitIter *iters, int maxTables);
static void         tsdbSeekCommitIter(SCommitIter *pIters, int nIters, TSKEY key);

void *tsdbCommitData(STsdbRepo *pRepo) {
  SMemTable *  pMem = pRepo->imem;

  tsdbInfo("vgId:%d start to commit! keyFirst %" PRId64 " keyLast %" PRId64 " numOfRows %" PRId64 " meta rows: %d",
           REPO_ID(pRepo), pMem->keyFirst, pMem->keyLast, pMem->numOfRows, listNEles(pMem->actList));

  pRepo->code = TSDB_CODE_SUCCESS;

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

  tsdbInfo("vgId:%d commit over, succeed", REPO_ID(pRepo));
  tsdbEndCommit(pRepo, TSDB_CODE_SUCCESS);

  return NULL;

_err:
  ASSERT(terrno != TSDB_CODE_SUCCESS);
  pRepo->code = terrno;
  tsdbInfo("vgId:%d commit over, failed", REPO_ID(pRepo));
  tsdbEndCommit(pRepo, terrno);

  return NULL;
}

static int tsdbCommitTSData(STsdbRepo *pRepo) {
  SMemTable *  pMem = pRepo->imem;
  SDataCols *  pDataCols = NULL;
  STsdbMeta *  pMeta = pRepo->tsdbMeta;
  SCommitIter *iters = NULL;
  SRWHelper    whelper = {0};
  STsdbCfg *   pCfg = &(pRepo->config);
  SFidGroup    fidGroup = {0};
  TSKEY        minKey = 0;
  TSKEY        maxKey = 0;

  if (pMem->numOfRows <= 0) return 0;

  tsdbGetFidGroup(pCfg, &fidGroup);
  tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, fidGroup.minFid, &minKey, &maxKey);
  tsdbRemoveFilesBeyondRetention(pRepo, &fidGroup);

  iters = tsdbCreateCommitIters(pRepo);
  if (iters == NULL) {
    tsdbError("vgId:%d failed to create commit iterator since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  if (tsdbInitWriteHelper(&whelper, pRepo) < 0) {
    tsdbError("vgId:%d failed to init write helper since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  if ((pDataCols = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pCfg->maxRowsPerFileBlock)) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbError("vgId:%d failed to init data cols with maxRowBytes %d maxCols %d maxRowsPerFileBlock %d since %s",
              REPO_ID(pRepo), pMeta->maxCols, pMeta->maxRowBytes, pCfg->maxRowsPerFileBlock, tstrerror(terrno));
    goto _err;
  }

  int sfid = (int)(TSDB_KEY_FILEID(pMem->keyFirst, pCfg->daysPerFile, pCfg->precision));
  int efid = (int)(TSDB_KEY_FILEID(pMem->keyLast, pCfg->daysPerFile, pCfg->precision));

  tsdbSeekCommitIter(iters, pMem->maxTables, minKey);

  // Loop to commit to each file
  for (int fid = sfid; fid <= efid; fid++) {
    if (fid < fidGroup.minFid) continue;

    if (tsdbCommitToFile(pRepo, fid, iters, &whelper, pDataCols) < 0) {
      tsdbError("vgId:%d failed to commit to file %d since %s", REPO_ID(pRepo), fid, tstrerror(terrno));
      goto _err;
    }
  }

  tsdbApplyRetention(pRepo, &fidGroup);

  tdFreeDataCols(pDataCols);
  tsdbDestroyCommitIters(iters, pMem->maxTables);
  tsdbDestroyHelper(&whelper);

  return 0;

_err:
  tdFreeDataCols(pDataCols);
  tsdbDestroyCommitIters(iters, pMem->maxTables);
  tsdbDestroyHelper(&whelper);

  return -1;
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

  return 0;

_err:
  return -1;
}

static void tsdbEndCommit(STsdbRepo *pRepo, int eno) {
  if (pRepo->appH.notifyStatus) pRepo->appH.notifyStatus(pRepo->appH.appH, TSDB_STATUS_COMMIT_OVER, eno);
  sem_post(&(pRepo->readyToCommit));
}

static bool tsdbHasDataToCommit(SCommitIter *iters, int nIters, TSKEY minKey, TSKEY maxKey) {
  for (int i = 0; i < nIters; i++) {
    TSKEY nextKey = tsdbNextIterKey((iters + i)->pIter);
    if (nextKey != TSDB_DATA_TIMESTAMP_NULL && (nextKey >= minKey && nextKey <= maxKey)) return true;
  }
  return false;
}

static int tsdbCommitToFile(STsdbRepo *pRepo, int fid, SCommitIter *iters, SRWHelper *pHelper, SDataCols *pDataCols) {
  STsdbCfg *  pCfg = &pRepo->config;
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup *pGroup = NULL;
  SMemTable * pMem = pRepo->imem;
  bool        newLast = false;
  TSKEY       minKey = 0;
  TSKEY       maxKey = 0;

  tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, fid, &minKey, &maxKey);

  // Check if there are data to commit to this file
  if (!tsdbHasDataToCommit(iters, pMem->maxTables, minKey, maxKey)) {
    tsdbDebug("vgId:%d no data to commit to file %d", REPO_ID(pRepo), fid);
    return 0;
  }

  if ((pGroup = tsdbSearchFGroup(pFileH, fid, TD_EQ)) == NULL) {
    pGroup = tsdbCreateFGroup(pRepo, fid);
    if (pGroup == NULL) {
      tsdbError("vgId:%d failed to create file group %d since %s", REPO_ID(pRepo), fid, tstrerror(terrno));
      return -1;
    }
  }

  // Open files for write/read
  if (tsdbSetAndOpenHelperFile(pHelper, pGroup) < 0) {
    tsdbError("vgId:%d failed to set helper file since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  newLast = TSDB_NLAST_FILE_OPENED(pHelper);

  if (tsdbLoadCompIdx(pHelper, NULL) < 0) {
    tsdbError("vgId:%d failed to load SCompIdx part since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  // Loop to commit data in each table
  for (int tid = 1; tid < pMem->maxTables; tid++) {
    SCommitIter *pIter = iters + tid;
    if (pIter->pTable == NULL) continue;

    taosRLockLatch(&(pIter->pTable->latch));

    if (tsdbSetHelperTable(pHelper, pIter->pTable, pRepo) < 0) goto _err;

    if (pIter->pIter != NULL) {
      if (tdInitDataCols(pDataCols, tsdbGetTableSchemaImpl(pIter->pTable, false, false, -1)) < 0) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto _err;
      }

      if (tsdbCommitTableData(pHelper, pIter, pDataCols, maxKey) < 0) {
        taosRUnLockLatch(&(pIter->pTable->latch));
        tsdbError("vgId:%d failed to write data of table %s tid %d uid %" PRIu64 " since %s", REPO_ID(pRepo),
                  TABLE_CHAR_NAME(pIter->pTable), TABLE_TID(pIter->pTable), TABLE_UID(pIter->pTable),
                  tstrerror(terrno));
        goto _err;
      }
    }

    taosRUnLockLatch(&(pIter->pTable->latch));

    // Move the last block to the new .l file if neccessary
    if (tsdbMoveLastBlockIfNeccessary(pHelper) < 0) {
      tsdbError("vgId:%d, failed to move last block, since %s", REPO_ID(pRepo), tstrerror(terrno));
      goto _err;
    }

    // Write the SCompBlock part
    if (tsdbWriteCompInfo(pHelper) < 0) {
      tsdbError("vgId:%d, failed to write compInfo part since %s", REPO_ID(pRepo), tstrerror(terrno));
      goto _err;
    }
  }

  if (tsdbWriteCompIdx(pHelper) < 0) {
    tsdbError("vgId:%d failed to write compIdx part to file %d since %s", REPO_ID(pRepo), fid, tstrerror(terrno));
    goto _err;
  }

  tsdbCloseHelperFile(pHelper, 0, pGroup);

  pthread_rwlock_wrlock(&(pFileH->fhlock));

  (void)rename(TSDB_FILE_NAME(helperNewHeadF(pHelper)), TSDB_FILE_NAME(helperHeadF(pHelper)));
  pGroup->files[TSDB_FILE_TYPE_HEAD].info = helperNewHeadF(pHelper)->info;

  if (newLast) {
    (void)rename(TSDB_FILE_NAME(helperNewLastF(pHelper)), TSDB_FILE_NAME(helperLastF(pHelper)));
    pGroup->files[TSDB_FILE_TYPE_LAST].info = helperNewLastF(pHelper)->info;
  } else {
    pGroup->files[TSDB_FILE_TYPE_LAST].info = helperLastF(pHelper)->info;
  }

  pGroup->files[TSDB_FILE_TYPE_DATA].info = helperDataF(pHelper)->info;

  pthread_rwlock_unlock(&(pFileH->fhlock));

  return 0;

_err:
  tsdbCloseHelperFile(pHelper, 1, pGroup);
  return -1;
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