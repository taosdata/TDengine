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
#include "tsdbint.h"

#define TSDB_IVLD_FID INT_MIN
#define TSDB_MAX_SUBBLOCKS 8
#define TSDB_KEY_FID(key, days, precision) ((key) / tsMsPerDay[(precision)] / (days))

typedef struct {
  int   minFid;
  int   midFid;
  int   maxFid;
  TSKEY minKey;
} SRtn;

typedef struct {
  int          version;
  SRtn         rtn;  // retention snapshot
  bool         isRFileSet;
  SReadH       readh;
  SFSIter      fsIter;  // tsdb file iterator
  int          niters;  // memory iterators
  SCommitIter *iters;
  SDFileSet    wSet;  // commit file
  TSKEY        minKey;
  TSKEY        maxKey;
  SArray *     aBlkIdx;  // SBlockIdx array
  SArray *     aSupBlk;  // Table super-block array
  SArray *     aSubBlk;  // table sub-block array
  SDataCols *  pDataCols;
} SCommitH;

#define TSDB_COMMIT_REPO(ch) TSDB_READ_REPO(&(ch->readh))
#define TSDB_COMMIT_REPO_ID(ch) REPO_ID(TSDB_READ_REPO(&(ch->readh)))
#define TSDB_COMMIT_WRITE_FSET(ch) (&((ch)->wSet))
#define TSDB_COMMIT_TABLE(ch) TSDB_READ_TABLE(&(ch->readh))
#define TSDB_COMMIT_HEAD_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_HEAD)
#define TSDB_COMMIT_DATA_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_DATA)
#define TSDB_COMMIT_LAST_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_LAST)
#define TSDB_COMMIT_BUF(ch) TSDB_READ_BUF(&((ch)->readh))
#define TSDB_COMMIT_COMP_BUF(ch) TSDB_READ_COMP_BUF(&((ch)->readh))
#define TSDB_COMMIT_DEFAULT_ROWS(ch) (TSDB_COMMIT_REPO(ch)->config.maxRowsPerFileBlock * 4 / 5)

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

// =================== Commit Meta Data
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

// =================== Commit Time-Series Data
static int tsdbCommitTSData(STsdbRepo *pRepo) {
  SMemTable *pMem = pRepo->imem;
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  SCommitH   ch = {0};
  SDFileSet *pSet = NULL;
  SDFileSet  nSet;
  int        fid;

  if (pMem->numOfRows <= 0) return 0;

  // Resource initialization
  if (tsdbInitCommitH(pRepo, &ch) < 0) {
    return -1;
  }

  // Skip expired memory data and expired FSET
  tsdbSeekCommitIter(&ch, ch.rtn.minKey);
  while (true) {
    pSet = tsdbFSIterNext(&(ch.fsIter));
    if (pSet == NULL || pSet->fid >= ch.rtn.minFid) break;
  }

  // Loop to commit to each file
  fid = tsdbNextCommitFid(&(ch));
  while (true) {
    // Loop over both on disk and memory
    if (pSet == NULL && fid == TSDB_IVLD_FID) break;

    if (pSet && (fid == TSDB_IVLD_FID || pSet->fid < fid)) {
      // Only has existing FSET but no memory data to commit in this
      // existing FSET, only check if file in correct retention
      int level, id;

      tfsAllocDisk(tsdbGetFidLevel(pSet->fid, &(ch.rtn)), &level, &id);
      if (level == TFS_UNDECIDED_LEVEL) {
        terrno = TSDB_TDB_NO_AVAIL_DISK;
        tsdbDestroyCommitH(&ch);
        return -1;
      }

      if (level > TSDB_FSET_LEVEL(pSet)) {
        if (tsdbCopyDFileSet(*pSet, level, id, &nSet) < 0) {
          tsdbDestroyCommitH(&ch);
          return -1;
        }

        if (tsdbUpdateDFileSet(pRepo, &nSet) < 0) {
          tsdbDestroyCommitH(&ch);
          return -1;
        }
      } else {
        if (tsdbUpdateDFileSet(pRepo, pSet) < 0) {
          tsdbDestroyCommitH(&ch);
          return -1;
        }
      }

      pSet = tsdbFSIterNext(&(ch.fsIter));
    } else {
      // Has memory data to commit
      SDFileSet *pCSet;
      int        cfid;

      if (pSet == NULL || pSet->fid > fid) {
        // Commit to a new FSET with fid: fid
        pCSet = NULL;
        cfid = fid;
      } else {
        // Commit to an existing FSET
        pCSet = pSet;
        cfid = pSet->fid;
        pSet = tsdbFSIterNext(&(ch.fsIter));
      }
      fid = tsdbNextCommitFid(&ch);

      if (tsdbCommitToFile(pCSet, &ch, cfid) < 0) {
        tsdbDestroyCommitH(&ch);
        return -1;
      }
    }
  }

  tsdbDestroyCommitH(&ch);
  return 0;
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

static int tsdbCommitToFile(SCommitH *pCommith, SDFileSet *pSet, int fid) {
  int        level, id;
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg * pCfg = REPO_CFG(pRepo);

  ASSERT(pSet == NULL || pSet->fid == fid);

  tsdbResetCommitFile(pCommith);
  tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, fid, &(pCommith->minKey), &(pCommith->maxKey));

  tfsAllocDisk(tsdbGetFidLevel(fid, &(pCommith->rtn)), &level, &id);
  if (level == TFS_UNDECIDED_LEVEL) {
    terrno = TSDB_TDB_NO_AVAIL_DISK;
    return -1;
  }

  // Set commit file
  if (pSet == NULL || level > TSDB_FSET_LEVEL(pSet)) {
    tsdbInitDFileSet(TSDB_COMMIT_WRITE_FSET(pCommith), REPO_ID(pRepo), fid, pCommith->version, level, id);
  } else {
    level = TSDB_FSET_LEVEL(pSet);
    id = TSDB_FSET_ID(pSet);

    // TSDB_FILE_HEAD
    SDFile *pWHeadf = TSDB_COMMIT_HEAD_FILE(pCommith);
    tsdbInitDFile(pWHeadf, REPO_ID(pRepo), fid, pCommith->version, level, id, NULL, TSDB_FILE_HEAD);

    // TSDB_FILE_DATA
    SDFile *pRDataf = TSDB_READ_DATA_FILE(&(pCommith->readh));
    SDFile *pWDataf = TSDB_COMMIT_DATA_FILE(pCommith);
    tsdbInitDFileWithOld(pWDataf, pRDataf);

    // TSDB_FILE_LAST
    SDFile *pRLastf = TSDB_READ_LAST_FILE(&(pCommith->readh));
    SDFile *pWLastf = TSDB_COMMIT_LAST_FILE(pCommith);
    if (pRLastf->info.size < 32 * 1024) {
      tsdbInitDFileWithOld(pWLastf, pRLastf);
    } else {
      tsdbInitDFile(pWLastf, REPO_ID(pRepo), fid, pCommith->version, level, id, NULL, TSDB_FILE_LAST);
    }
  }

  // Open commit file
  if (tsdbOpenCommitFile(pCommith, pSet) < 0) {
    return -1;
  }

  // Loop to commit each table data
  for (int tid = 0; tid < pCommith->niters; tid++) {
    SCommitIter *pIter = pCommith->iters + tid;

    if (pIter->pTable == NULL) continue;

    if (tsdbCommitToTable(pCommith, tid) < 0) {
      // TODO: revert the file change
      tsdbCloseCommitFile(pCommith, true);
      return -1;
    }
  }

  tsdbCloseCommitFile(pCommith, false);

  if (tsdbUpdateDFileSet(pRepo, &(pCommith->wSet)) < 0) {
    // TODO
    return -1;
  }

  return 0;
}

static SCommitIter *tsdbCreateCommitIters(SCommitH *pCommith) {
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  SMemTable *pMem = pRepo->imem;
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  pCommith->niters = pMem->maxTables;
  pCommith->iters = (SCommitIter *)calloc(pMem->maxTables, sizeof(SCommitIter));
  if (pCommith->iters == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (tsdbRLockRepoMeta(pRepo) < 0) return -1

  // reference all tables
  for (int i = 0; i < pMem->maxTables; i++) {
    if (pMeta->tables[i] != NULL) {
      tsdbRefTable(pMeta->tables[i]);
      pCommith->iters[i].pTable = pMeta->tables[i];
    }
  }

  if (tsdbUnlockRepoMeta(pRepo) < 0) return -1;

  for (int i = 0; i < pMem->maxTables; i++) {
    if ((pCommith->iters[i].pTable != NULL) && (pMem->tData[i] != NULL) && (TABLE_UID(pCommith->iters[i].pTable) == pMem->tData[i]->uid)) {
      if ((pCommith->iters[i].pIter = tSkipListCreateIter(pMem->tData[i]->pData)) == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }

      tSkipListIterNext(pCommith->iters[i].pIter);
    }
  }

  return 0;
}

static void tsdbDestroyCommitIters(SCommitH *pCommith) {
  if (pCommith->iters == NULL) return;

  for (int i = 1; i < pCommith->niters; i++) {
    if (pCommith->iters[i].pTable != NULL) {
      tsdbUnRefTable(pCommith->iters[i].pTable);
      tSkipListDestroyIter(iters[i].pIter);
    }
  }

  free(pCommith->iters);
  pCommith->iters = NULL;
}

// Skip all keys until key (not included)
static void tsdbSeekCommitIter(SCommitH *pCommith, TSKEY key) {
  for (int i = 0; i < pCommith->niters; i++) {
    SCommitIter *pIter = pCommith->iters + i;
    if (pIter->pTable == NULL) continue;
    if (pIter->pIter == NULL) continue;

    tsdbLoadDataFromCache(pIter->pTable, pIter->pIter, key - 1, INT32_MAX, NULL, NULL, 0, true, NULL);
  }
}

static int tsdbInitCommitH(STsdbRepo *pRepo, SCommitH *pCommith) {
  STsdbCfg *pCfg = REPO_CFG(pRepo);

  memset(pCommith, 0, sizeof(*pCommith));
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    TSDB_FILE_SET_CLOSED(TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(pCommith), ftype));
  }

  pCommith->version = REPO_FS_VERSION(pRepo) + 1;

  tsdbGetRtnSnap(pRepo, &(pCommith->rtn));

  // Init read handle
  if (tsdbInitReadH(&(pCommith->readh), pRepo) < 0) {
    return -1;
  }

  // Init file iterator
  if (tsdbInitFSIter(pRepo, &(pCommith->fsIter)) < 0) {
    tsdbDestroyCommitH(pCommith);
    return -1;
  }

  if (tsdbCreateCommitIters(pCommith) < 0) {
    tsdbDestroyCommitH(pCommith);
    return -1;
  }

  pCommith->aBlkIdx = taosArrayInit(1024, sizeof(SBlockIdx));
  if (pCommith->aBlkIdx == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyCommitH(pCommith);
    return -1;
  }

  pCommith->aSupBlk = taosArrayInit(1024, sizeof(SBlock));
  if (pCommith->aSupBlk == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyCommitH(pCommith);
    return -1;
  }

  pCommith->aSubBlk = taosArrayInit(1024, sizeof(SBlock));
  if (pCommith->aSubBlk == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyCommitH(pCommith);
    return -1;
  }

  pCommith->pDataCols = tdNewDataCols(0, 0, pCfg->maxRowsPerFileBlock);
  if (pCommith->pDataCols == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyCommitH(pCommith);
    return -1;
  }

  return 0;
}

static void tsdbDestroyCommitH(SCommitH *pCommith) {
  pCommith->pDataCols = tdFreeDataCols(pCommith->pDataCols);
  pCommith->aSubBlk = taosArrayDestroy(pCommith->aSubBlk);
  pCommith->aSupBlk = taosArrayDestroy(pCommith->aSupBlk);
  pCommith->aBlkIdx = taosArrayDestroy(pCommith->aBlkIdx);
  tsdbDestroyCommitIters(pCommith);
  tsdbDestroyReadH(&(pCommith->readh));
  tsdbCloseDFileSet(TSDB_COMMIT_WRITE_FSET(pCommith));
}

static void tsdbGetRtnSnap(STsdbRepo *pRepo, SRtn *pRtn) {
  STsdbCfg *pCfg = REPO_CFG(pRepo);
  TSKEY     minKey, midKey, maxKey, now;

  now = taosGetTimestamp(pCfg->precision);
  minKey = now - pCfg->keep * tsMsPerDay[pCfg->precision];
  midKey = now - pCfg->keep2 * tsMsPerDay[pCfg->precision];
  maxKey = now - pCfg->keep1 * tsMsPerDay[pCfg->precision];

  pRtn->minKey = minKey;
  pRtn->minFid = TSDB_KEY_FID(minKey, pCfg->daysPerFile, pCfg->precision);
  pRtn->midFid = TSDB_KEY_FID(midKey, pCfg->daysPerFile, pCfg->precision);
  pRtn->maxFid = TSDB_KEY_FID(maxKey, pCfg->daysPerFile, pCfg->precision);
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

static int tsdbNextCommitFid(SCommitH *pCommith) {
  SCommitIter *pIter;
  STsdbRepo *  pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg *   pCfg = REPO_CFG(pRepo);
  int          fid = TSDB_IVLD_FID;

  for (int i = 0; i < pCommith->niters; i++) {
    pIter = pCommith->iters + i;
    if (pIter->pTable == NULL || pIter->pIter == NULL) continue;

    TSKEY nextKey = tsdbNextIterKey(pIter->pIter);
    if (nextKey == TSDB_DATA_TIMESTAMP_NULL) {
      continue;
    } else {
      int tfid = TSDB_KEY_FID(nextKey, pCfg->daysPerFile, pCfg->precision);
      if (fid == TSDB_IVLD_FID || fid > tfid) {
        fid = tfid;
      }
    }
  }

  return fid;
}

static int tsdbCommitToTable(SCommitH *pCommith, int tid) {
  SCommitIter *pIter = pCommith->iters + tid;
  if (pIter->pTable == NULL) return 0;

  TSDB_RLOCK_TABLE(pIter->pTable);

  // Set commit table
  tsdbResetCommitTable(pCommith);
  if (tsdbSetCommitTable(pCommith, pIter->pTable) < 0) {
    TSDB_RUNLOCK_TABLE(pIter->pTable);
    return -1;
  }

  if (!pCommith->isRFileSet) {
    if (pIter->pIter == NULL) {
      // No memory data
      TSDB_RUNLOCK_TABLE(pIter->pTable);
      return 0;
    } else {
      // TODO: think about no data committed at all
      if (tsdbCommitMemData(pCommith, pIter, pCommith->maxKey, true) < 0) {
        TSDB_RUNLOCK_TABLE(pIter->pTable);
        return -1;
      }

      TSDB_RUNLOCK_TABLE(pIter->pTable);
      if (tsdbWriteBlockInfo(pCommith) < 0) {
        return -1;
      }

      return 0;
    }
  }

  // No memory data and no disk data, just return
  if (pIter->pIter == NULL && pCommith->readh.pBlkIdx == NULL) {
    TSDB_RUNLOCK_TABLE(pIter->pTable);
    return 0;
  }

  if (tsdbLoadBlockInfo(&(pCommith->readh), NULL) < 0) {
    TSDB_RUNLOCK_TABLE(pIter->pTable);
    return -1;
  }

  // Process merge commit
  int     nBlocks = (pCommith->readh.pBlkIdx == NULL) ? 0 : pCommith->readh.pBlkIdx->numOfBlocks;
  TSKEY   nextKey = tsdbNextIterKey(pIter->pIter);
  int     cidx = 0;
  void *  ptr = NULL;
  SBlock *pBlock;

  if (cidx < nBlocks) {
    pBlock = pCommith->readh.pBlkInfo->blocks + cidx;
  } else {
    pBlock = NULL;
  }

  while (true) {
    if ((nextKey == TSDB_DATA_TIMESTAMP_NULL || nextKey > pCommith->maxKey) && (pBlock == NULL)) break;

    if ((nextKey == TSDB_DATA_TIMESTAMP_NULL || nextKey > pCommith->maxKey) ||
        (pBlock && (!pBlock->last) && tsdbComparKeyBlock((void *)(&nextKey), pBlock) > 0)) {
      if (tsdbMoveBlock(pCommith, cidx) < 0) {
        TSDB_RUNLOCK_TABLE(pIter->pTable);
        return -1;
      }

      cidx++;
      if (cidx < nBlocks) {
        pBlock = pCommith->readh.pBlkInfo->blocks + cidx;
      } else {
        pBlock = NULL;
      }
    } else if ((cidx < nBlocks) && (pBlock->last || tsdbComparKeyBlock((void *)(&nextKey), pBlock) == 0)) {
      if (tsdbMergeMemData(pCommith, pIter, cidx) < 0) {
        TSDB_RUNLOCK_TABLE(pIter->pTable);
        return -1;
      }

      cidx++;
      if (cidx < nBlocks) {
        pBlock = pCommith->readh.pBlkInfo->blocks + cidx;
      } else {
        pBlock = NULL;
      }
      nextKey = tsdbNextIterKey(pIter->pIter);
    } else {
      if (pBlock == NULL) {
        if (tsdbCommitMemData(pCommith, pIter, pCommith->maxKey, false) < 0) {
          TSDB_RUNLOCK_TABLE(pIter->pTable);
          return -1;
        }
        nextKey = tsdbNextIterKey(pIter->pIter);
      } else {
        if (tsdbCommitMemData(pCommith, pIter, pBlock->keyFirst-1, true) < 0) {
          TSDB_RUNLOCK_TABLE(pIter->pTable);
          return -1;
        }
        nextKey = tsdbNextIterKey(pIter->pIter);
      }
    }
  }

  TSDB_RUNLOCK_TABLE(pIter->pTable);

  if (tsdbWriteBlockInfo(pCommith) < 0) return -1;

  return 0;
}

static int tsdbSetCommitTable(SCommitH *pCommith, STable *pTable) {
  STSchema *pSchema = tsdbGetTableSchemaImpl(pTable, false, false, -1);

  if (tdInitDataCols(pCommith->pDataCols, pSchema) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (pCommith->isRFileSet) {
    if (tsdbSetReadTable(&(pCommith->readh), pTable) < 0) {
      return -1;
    }
  }
  return 0;
}

static int tsdbComparKeyBlock(const void *arg1, const void *arg2) {
  TSKEY   key = *(TSKEY *)arg1;
  SBlock *pBlock = (SBlock *)arg2;

  if (key < pBlock->keyFirst) {
    return -1;
  } else if (key > pBlock->keyLast) {
    return 1;
  } else {
    return 0;
  }
}

static int tsdbWriteBlock(SCommitH *pCommih, SDFile *pDFile, SDataCols *pDataCols, SBlock *pBlock, bool isLast,
                          bool isSuper) {
  // TODO
  STsdbCfg *  pCfg = &(pHelper->pRepo->config);
  SBlockData *pCompData = (SBlockData *)(pHelper->pBuffer);
  int64_t     offset = 0;
  int         rowsToWrite = pDataCols->numOfRows;

  ASSERT(rowsToWrite > 0 && rowsToWrite <= pCfg->maxRowsPerFileBlock);
  ASSERT(isLast ? rowsToWrite < pCfg->minRowsPerFileBlock : true);

  offset = lseek(pFile->fd, 0, SEEK_END);
  if (offset < 0) {
    tsdbError("vgId:%d failed to write block to file %s since %s", REPO_ID(pHelper->pRepo), TSDB_FILE_NAME(pFile),
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  int nColsNotAllNull = 0;
  for (int ncol = 1; ncol < pDataCols->numOfCols; ncol++) {  // ncol from 1, we skip the timestamp column
    SDataCol * pDataCol = pDataCols->cols + ncol;
    SBlockCol *pCompCol = pCompData->cols + nColsNotAllNull;

    if (isNEleNull(pDataCol, rowsToWrite)) {  // all data to commit are NULL, just ignore it
      continue;
    }

    memset(pCompCol, 0, sizeof(*pCompCol));

    pCompCol->colId = pDataCol->colId;
    pCompCol->type = pDataCol->type;
    if (tDataTypeDesc[pDataCol->type].getStatisFunc) {
      (*tDataTypeDesc[pDataCol->type].getStatisFunc)(
          (TSKEY *)(pDataCols->cols[0].pData), pDataCol->pData, rowsToWrite, &(pCompCol->min), &(pCompCol->max),
          &(pCompCol->sum), &(pCompCol->minIndex), &(pCompCol->maxIndex), &(pCompCol->numOfNull));
    }
    nColsNotAllNull++;
  }

  ASSERT(nColsNotAllNull >= 0 && nColsNotAllNull <= pDataCols->numOfCols);

  // Compress the data if neccessary
  int     tcol = 0;
  int32_t toffset = 0;
  int32_t tsize = TSDB_GET_COMPCOL_LEN(nColsNotAllNull);
  int32_t lsize = tsize;
  int32_t keyLen = 0;
  for (int ncol = 0; ncol < pDataCols->numOfCols; ncol++) {
    if (ncol != 0 && tcol >= nColsNotAllNull) break;

    SDataCol * pDataCol = pDataCols->cols + ncol;
    SBlockCol *pCompCol = pCompData->cols + tcol;

    if (ncol != 0 && (pDataCol->colId != pCompCol->colId)) continue;
    void *tptr = POINTER_SHIFT(pCompData, lsize);

    int32_t flen = 0;  // final length
    int32_t tlen = dataColGetNEleLen(pDataCol, rowsToWrite);

    if (pCfg->compression) {
      if (pCfg->compression == TWO_STAGE_COMP) {
        pHelper->compBuffer = taosTRealloc(pHelper->compBuffer, tlen + COMP_OVERFLOW_BYTES);
        if (pHelper->compBuffer == NULL) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          goto _err;
        }
      }

      flen = (*(tDataTypeDesc[pDataCol->type].compFunc))(
          (char *)pDataCol->pData, tlen, rowsToWrite, tptr, (int32_t)taosTSizeof(pHelper->pBuffer) - lsize,
          pCfg->compression, pHelper->compBuffer, (int32_t)taosTSizeof(pHelper->compBuffer));
    } else {
      flen = tlen;
      memcpy(tptr, pDataCol->pData, flen);
    }

    // Add checksum
    ASSERT(flen > 0);
    flen += sizeof(TSCKSUM);
    taosCalcChecksumAppend(0, (uint8_t *)tptr, flen);
    pFile->info.magic =
        taosCalcChecksum(pFile->info.magic, (uint8_t *)POINTER_SHIFT(tptr, flen - sizeof(TSCKSUM)), sizeof(TSCKSUM));

    if (ncol != 0) {
      pCompCol->offset = toffset;
      pCompCol->len = flen;
      tcol++;
    } else {
      keyLen = flen;
    }

    toffset += flen;
    lsize += flen;
  }

  pCompData->delimiter = TSDB_FILE_DELIMITER;
  pCompData->uid = pHelper->tableInfo.uid;
  pCompData->numOfCols = nColsNotAllNull;

  taosCalcChecksumAppend(0, (uint8_t *)pCompData, tsize);
  pFile->info.magic = taosCalcChecksum(pFile->info.magic, (uint8_t *)POINTER_SHIFT(pCompData, tsize - sizeof(TSCKSUM)),
                                       sizeof(TSCKSUM));

  // Write the whole block to file
  if (taosWrite(pFile->fd, (void *)pCompData, lsize) < lsize) {
    tsdbError("vgId:%d failed to write %d bytes to file %s since %s", REPO_ID(helperRepo(pHelper)), lsize,
              TSDB_FILE_NAME(pFile), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // Update pBlock membership vairables
  pBlock->last = isLast;
  pBlock->offset = offset;
  pBlock->algorithm = pCfg->compression;
  pBlock->numOfRows = rowsToWrite;
  pBlock->len = lsize;
  pBlock->keyLen = keyLen;
  pBlock->numOfSubBlocks = isSuper ? 1 : 0;
  pBlock->numOfCols = nColsNotAllNull;
  pBlock->keyFirst = dataColsKeyFirst(pDataCols);
  pBlock->keyLast = dataColsKeyAt(pDataCols, rowsToWrite - 1);

  tsdbDebug("vgId:%d tid:%d a block of data is written to file %s, offset %" PRId64
            " numOfRows %d len %d numOfCols %" PRId16 " keyFirst %" PRId64 " keyLast %" PRId64,
            REPO_ID(helperRepo(pHelper)), pHelper->tableInfo.tid, TSDB_FILE_NAME(pFile), (int64_t)(pBlock->offset),
            (int)(pBlock->numOfRows), pBlock->len, pBlock->numOfCols, pBlock->keyFirst, pBlock->keyLast);

  pFile->info.size += pBlock->len;
  // ASSERT(pFile->info.size == lseek(pFile->fd, 0, SEEK_CUR));

  return 0;

_err:
  return -1;
}

static int tsdbWriteBlockInfo(SCommitH *pCommih) {
  SDFile *pHeadf = TSDB_COMMIT_HEAD_FILE(pCommih);

  SBlockIdx   blkIdx;
  STable *    pTable = TSDB_COMMIT_TABLE(pCommih);
  SBlock *    pBlock;
  size_t      nSupBlocks;
  size_t      nSubBlocks;
  uint32_t    tlen;
  SBlockInfo *pBlkInfo;
  int64_t     offset;

  nSupBlocks = taosArrayGetSize(pCommih->aSupBlk);
  nSubBlocks = taosArrayGetSize(pCommih->aSubBlk);
  tlen = sizeof(SBlockInfo) + sizeof(SBlock) * (nSupBlocks + nSubBlocks) + sizeof(TSCKSUM);

  ASSERT(nSupBlocks > 0);

  // Write SBlockInfo part
  if (tsdbMakeRoom((void **)(&(TSDB_COMMIT_BUF(pCommih))), tlen) < 0) return -1;
  pBlkInfo = TSDB_COMMIT_BUF(pCommih);

  pBlkInfo->delimiter = TSDB_FILE_DELIMITER;
  pBlkInfo->tid = TABLE_TID(pTable);
  pBlkInfo->uid = TABLE_UID(pTable);

  memcpy((void *)(pBlkInfo->blocks), taosArrayGet(pCommih->aSupBlk, 0), nSupBlocks * sizeof(SBlock));
  if (nSubBlocks > 0) {
    memcpy((void *)(pBlkInfo->blocks + nSupBlocks), taosArrayGet(pCommih->aSubBlk, 0), nSubBlocks * sizeof(SBlock));

    for (int i = 0; i < nSupBlocks; i++) {
      pBlock = pBlkInfo->blocks + i;

      if (pBlock->numOfSubBlocks > 1) {
        pBlock->offset += (sizeof(SBlockInfo) + sizeof(SBlock) * nSupBlocks);
      }
    }
  }

  taosCalcChecksumAppend(0, (uint8_t *)pBlkInfo, tlen);

  offset = tsdbSeekDFile(pHeadf, 0, SEEK_END);
  if (offset < 0) {
    tsdbError("vgId:%d failed to write block info part to file %s while seek since %s", TSDB_COMMIT_REPO_ID(pCommih),
              TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno));
    return -1;
  }

  if (tsdbWriteDFile(pHeadf, TSDB_COMMIT_BUF(pCommih), tlen) < tlen) {
    tsdbError("vgId:%d failed to write block info part to file %s since %s", TSDB_COMMIT_REPO_ID(pCommih),
              TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno));
    return -1;
  }

  tsdbUpdateDFileMagic(pHeadf, POINTER_SHIFT(pBlkInfo, tlen - sizeof(TSCKSUM)));

  // Set blkIdx
  pBlock = taosArrayGet(pCommih->aSupBlk, nSupBlocks - 1);

  blkIdx.tid = TABLE_TID(pTable);
  blkIdx.uid = TABLE_UID(pTable);
  blkIdx.hasLast = pBlock->last ? 1 : 0;
  blkIdx.maxKey = pBlock->keyLast;
  blkIdx.numOfBlocks = nSupBlocks;
  blkIdx.len = tlen;
  blkIdx.offset = (uint32_t)offset;

  ASSERT(blkIdx.numOfBlocks > 0);

  if (taosArrayPush(pCommih->aBlkIdx, (void *)(&blkIdx)) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int tsdbWriteBlockIdx(SCommitH *pCommih) {
  SBlockIdx *pBlkIdx;
  SDFile *   pHeadf = TSDB_COMMIT_HEAD_FILE(pCommih);
  size_t     nidx = taosArrayGetSize(pCommih->aBlkIdx);
  int        tlen = 0, size;
  int64_t    offset;

  ASSERT(nidx > 0);

  for (size_t i = 0; i < nidx; i++) {
    pBlkIdx = (SBlockIdx *)taosArrayGet(pCommih->aBlkIdx, i);

    size = tsdbEncodeSBlockIdx(NULL, pBlkIdx);
    if (tsdbMakeRoom((void **)(&TSDB_COMMIT_BUF(pCommih)), tlen + size) < 0) return -1;

    void *ptr = POINTER_SHIFT(TSDB_COMMIT_BUF(pCommih), tlen);
    tsdbEncodeSBlockIdx(&ptr, pBlkIdx);

    tlen += size;
  }

  tlen += sizeof(TSCKSUM);
  if (tsdbMakeRoom((void **)(&TSDB_COMMIT_BUF(pCommih)), tlen) < 0) return -1;
  taosCalcChecksumAppend(0, (uint8_t *)TSDB_COMMIT_BUF(pCommih), tlen);

  if (tsdbAppendDFile(pHeadf, TSDB_COMMIT_BUF(pCommih), tlen, &offset) < tlen) {
    tsdbError("vgId:%d failed to write block index part to file %s since %s", TSDB_COMMIT_REPO_ID(pCommih),
              TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno));
    return -1;
  }

  tsdbUpdateDFileMagic(pHeadf, POINTER_SHIFT(TSDB_COMMIT_BUF(pCommih), tlen - sizeof(TSCKSUM)));
  pHeadf->info.offset = offset;
  pHeadf->info.len = tlen;

  return 0;
}

static int tsdbCommitMemData(SCommitH *pCommith, SCommitIter *pIter, TSKEY keyLimit, bool toData) {
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  SMergeInfo mInfo;
  int32_t    defaultRows = TSDB_COMMIT_DEFAULT_ROWS(pCommith);
  SDFile *   pDFile;
  bool       isLast;
  SBlock     block;

  while (true) {
    tsdbLoadDataFromCache(pIter->pTable, pIter->pIter, keyLimit, defaultRows, pCommith->pDataCols, NULL, 0,
                          pCfg->update, &mInfo);

    if (pCommith->pDataCols->numOfRows <= 0) break;

    if (toData || pCommith->pDataCols->numOfRows >= pCfg->minRowsPerFileBlock) {
      pDFile = TSDB_COMMIT_DATA_FILE(pCommith);
      isLast = false;
    } else {
      pDFile = TSDB_COMMIT_LAST_FILE(pCommith);
      isLast = true;
    }

    if (tsdbWriteBlock(pCommith, pDFile, pCommith->pDataCols, &block, isLast, true) < 0) return -1;

    if (taosArrayPush(pCommith->aSupBlk, (void *)(&block)) == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  }

  return 0;
}

static int tsdbMergeMemData(SCommitH *pCommith, SCommitIter *pIter, int bidx) {
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  int        nBlocks = pCommith->readh.pBlkIdx->numOfBlocks;
  SBlock *   pBlock = pCommith->readh.pBlkInfo->blocks + bidx;
  TSKEY      keyLimit;
  int16_t    colId = 0;
  SMergeInfo mInfo;
  SBlock     subBlocks[TSDB_MAX_SUBBLOCKS];
  SBlock     block, supBlock;
  SDFile *   pDFile;

  if (bidx == nBlocks - 1) {
    keyLimit = pCommith->maxKey;
  } else {
    keyLimit = pBlock[1].keyFirst - 1;
  }

  SSkipListIterator titer = *(pIter->pIter);
  if (tsdbLoadBlockDataCols(&(pCommith->readh), pBlock, NULL, &colId, 1) < 0) return -1;

  tsdbLoadDataFromCache(pIter->pTable, &titer, keyLimit, INT32_MAX, NULL, pCommith->readh.pDCols[0]->cols[0].pData,
                        pCommith->readh.pDCols[0]->numOfRows, pCfg->update, &mInfo);

  if (mInfo.nOperations == 0) {
    // no new data to insert (all updates denied)
    if (tsdbMoveBlock(pCommith, bidx) < 0) {
      return -1;
    }
    *(pIter->pIter) = titer;
  } else if (pBlock->numOfRows + mInfo.rowsInserted - mInfo.rowsDeleteSucceed == 0) {
    // Ignore the block
    ASSERT(0);
    *(pIter->pIter) = titer;
  } else if (tsdbCanAddSubBlock()) {
    // Add a sub-block
    tsdbLoadDataFromCache(pIter->pTable, pIter->pIter, keyLimit, INT32_MAX, pCommith->pDataCols,
                          pCommith->readh.pDCols[0]->cols[0].pData, pCommith->readh.pDCols[0]->numOfRows, pCfg->update,
                          &mInfo);
    if (pBlock->last) {
      pDFile = TSDB_COMMIT_LAST_FILE(pCommith);
    } else {
      pDFile = TSDB_COMMIT_DATA_FILE(pCommith);
    }

    if (tsdbWriteBlock(pCommith, pDFile, pCommith->pDataCols, &block, pBlock->last, false) < 0) return -1;

    if (pBlock->numOfSubBlocks == 1) {
      subBlocks[0] = *pBlock;
      subBlocks[0].numOfSubBlocks = 0;
    } else {
      memcpy(subBlocks, POINTER_SHIFT(pCommith->readh.pBlkInfo, pBlock->offset),
             sizeof(SBlock) * pBlock->numOfSubBlocks);
    }
    subBlocks[pBlock->numOfSubBlocks] = block;
    supBlock = *pBlock;
    supBlock.keyFirst = mInfo.keyFirst;
    supBlock.keyLast = mInfo.keyLast;
    supBlock.numOfSubBlocks++;
    supBlock.numOfRows = pBlock->numOfRows + mInfo.rowsInserted - mInfo.rowsDeleteSucceed;
    supBlock.offset = taosArrayGetSize(pCommith->aSubBlk) * sizeof(SBlock);

    if (tsdbCommitAddBlock(pCommith, &supBlock, subBlocks, pBlock->numOfSubBlocks + 1) < 0) return -1;
  } else {
    if (tsdbLoadBlockData(&(pCommith->readh), pBlock, NULL) < 0) return -1;
    if (tsdbMergeBlockData(pCommith, pIter, pCommith->readh.pDCols[0], keyLimit, bidx == (nBlocks - 1)) < 0) return -1;
  }

  return 0;
}

static int tsdbMoveBlock(SCommitH *pCommith, int bidx) {
  SBlock *pBlock = pCommith->readh.pBlkInfo->blocks+bidx;
  SDFile *pCommitF = (pBlock->last) ? TSDB_COMMIT_LAST_FILE(pCommith) : TSDB_COMMIT_DATA_FILE(pCommith);
  SDFile *pReadF = (pBlock->last) ? TSDB_READ_LAST_FILE(&(pCommith->readh)) : TSDB_READ_DATA_FILE(&(pCommith->readh));
  SBlock  block;

  if (tfsIsSameFile(&(pCommitF->f), &(pReadF->f))) {
    if (pBlock->numOfSubBlocks == 1) {
      if (tsdbCommitAddBlock(pCommith, pBlock, NULL, 0) < 0) return -1;
    } else {
      block = *pBlock;
      block.offset = sizeof(SBlock) * taosArrayGetSize(pCommith->aSupBlock);

      if (tsdbCommitAddBlock(pCommith, &block, POINTER_SHIFT(pCommith->readh.pBlkInfo, pBlock->offset),
                             pBlock->numOfSubBlocks) < 0) {
        return -1;
      }
    }
  } else {
    if (tsdbLoadBlockData(&(pCommith->readh), pBlock, NULL) < 0) return -1;
    if (tsdbWriteBlock(pCommith, pCommitF, pCommith->readh.pDCols[0], &block, pBlock->last, true) < 0) return -1;
    if (tsdbCommitAddBlock(pCommith, &block, NULL, 0) < 0) return -1;
  }

  return 0;
}

static int tsdbCommitAddBlock(SCommitH *pCommith, const SBlock *pSupBlock, const SBlock *pSubBlocks, int nSubBlocks) {
  ASSERT(pSupBlock != NULL);

  if (taosArrayPush(pCommith->aSupBlk, pSupBlock) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (pSubBlocks && taosArrayPushBatch(pCommith->aSupBlk, pSubBlocks, nSubBlocks) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int tsdbMergeBlockData(SCommitH *pCommith, SCommitIter *pIter, SDataCols *pDataCols, TSKEY keyLimit, bool isLastOneBlock) {
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  SBlock     block;
  SDFile *   pDFile;
  bool       isLast;
  int32_t    defaultRows = TSDB_COMMIT_DEFAULT_ROWS(pCommith);

  int biter = 0;
  while (true) {
    tsdbLoadAndMergeFromCache(pCommith->readh.pDCols[0], &biter, pIter, pCommith->pDataCols, keyLimit, defaultRows,
                              pCfg->update);

    if (pCommith->pDataCols->numOfRows == 0) break;

    if (isLastOneBlock) {
      if (pCommith->pDataCols->numOfRows < pCfg->minRowsPerFileBlock) {
        pDFile = TSDB_COMMIT_LAST_FILE(pCommith);
        isLast = true;
      } else {
        pDFile = TSDB_COMMIT_DATA_FILE(pCommith);
        isLast = false;
      }
    } else {
      pDFile = TSDB_COMMIT_DATA_FILE(pCommith);
      isLast = false;
    }

    if (tsdbWriteBlock(pCommith, pDFile, pCommith->pDataCols, &block, isLast, true) < 0) return -1;
    if (tsdbCommitAddBlock(pCommith, &block, NULL, 0) < 0) return -1;
  }
  

}

static void tsdbLoadAndMergeFromCache(SDataCols *pDataCols, int *iter, SCommitIter *pCommitIter, SDataCols *pTarget,
                                     TSKEY maxKey, int maxRows, int8_t update) {
  TSKEY     key1 = INT64_MAX;
  TSKEY     key2 = INT64_MAX;
  STSchema *pSchema = NULL;

  ASSERT(maxRows > 0 && dataColsKeyLast(pDataCols) <= maxKey);
  tdResetDataCols(pTarget);

  while (true) {
    key1 = (*iter >= pDataCols->numOfRows) ? INT64_MAX : dataColsKeyAt(pDataCols, *iter);
    bool isRowDel = false;
    SDataRow row = tsdbNextIterRow(pCommitIter->pIter);
    if (row == NULL || dataRowKey(row) > maxKey) {
      key2 = INT64_MAX;
    } else {
      key2 = dataRowKey(row);
      isRowDel = dataRowDeleted(row);
    }

    if (key1 == INT64_MAX && key2 == INT64_MAX) break;

    if (key1 < key2) {
      for (int i = 0; i < pDataCols->numOfCols; i++) {
        dataColAppendVal(pTarget->cols + i, tdGetColDataOfRow(pDataCols->cols + i, *iter), pTarget->numOfRows,
                         pTarget->maxPoints);
      }

      pTarget->numOfRows++;
      (*iter)++;
    } else if (key1 > key2) {
      if (!isRowDel) {
        if (pSchema == NULL || schemaVersion(pSchema) != dataRowVersion(row)) {
          pSchema = tsdbGetTableSchemaImpl(pCommitIter->pTable, false, false, dataRowVersion(row));
          ASSERT(pSchema != NULL);
        }

        tdAppendDataRowToDataCol(row, pSchema, pTarget);
      }

      tSkipListIterNext(pCommitIter->pIter);
    } else {
      if (update) {
        if (!isRowDel) {
          if (pSchema == NULL || schemaVersion(pSchema) != dataRowVersion(row)) {
            pSchema = tsdbGetTableSchemaImpl(pCommitIter->pTable, false, false, dataRowVersion(row));
            ASSERT(pSchema != NULL);
          }

          tdAppendDataRowToDataCol(row, pSchema, pTarget);
        }
      } else {
        ASSERT(!isRowDel);

        for (int i = 0; i < pDataCols->numOfCols; i++) {
          dataColAppendVal(pTarget->cols + i, tdGetColDataOfRow(pDataCols->cols + i, *iter), pTarget->numOfRows,
                           pTarget->maxPoints);
        }

        pTarget->numOfRows++;
      }
      (*iter)++;
      tSkipListIterNext(pCommitIter->pIter);
    }

    if (pTarget->numOfRows >= maxRows) break;
  }
}

static void tsdbResetCommitFile(SCommitH *pCommith) {
  tsdbResetCommitTable(pCommith);
  taosArrayClear(pCommith->aBlkIdx);
}

static void tsdbResetCommitTable(SCommitH *pCommith) {
  tdResetDataCols(pCommith->pDataCols);
  taosArrayClear(pCommith->aSubBlk);
  taosArrayClear(pCommith->aSupBlk);
}

static int tsdbOpenCommitFile(SCommitH *pCommith, SDFileSet *pRSet) {
  if (pRSet == NULL) {
    pCommith->isRFileSet = false;
  } else {
    pCommith->isRFileSet = true;
    if (tsdbSetAndOpenReadFSet(&(pCommith->readh), pRSet) < 0) {
      return -1;
    }
  }

  if (tsdbOpenDFileSet(TSDB_COMMIT_WRITE_FSET(pCommith), O_WRONLY | O_CREAT) < 0) {
    return -1;
  }

  return 0;
}

static void tsdbCloseCommitFile(SCommitH *pCommith, bool hasError) {
  if (pCommith->isRFileSet) {
    tsdbCloseAndUnsetFSet(&(pCommith->readh));
  }

  if (!hasError) {
    for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
      SDFile *pDFile = TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(pCommith), ftype);
      fsync(TSDB_FILE_FD(pDFile));
    }
  }
  tsdbCloseDFileSet(TSDB_COMMIT_WRITE_FSET(pCommith));
}