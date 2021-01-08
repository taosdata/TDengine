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

#define TSDB_IVLD_FID INT_MIN

typedef struct {
  int   minFid;
  int   midFid;
  int   maxFid;
  TSKEY minKey;
} SRtn;

typedef struct {
  SRtn         rtn;  // retention snapshot
  int          niters;
  SCommitIter *iters;  // memory iterators
  SReadH       readh;
  SDFileSet *  pWSet;
  TSKEY        minKey;
  TSKEY        maxKey;
  SArray *     aBlkIdx;
  SArray *     aSupBlk;
  SArray *     aSubBlk;
  SDataCols *  pDataCols;
} SCommitH;

#define TSDB_COMMIT_REPO(ch) TSDB_READ_REPO(&(ch->readh))
#define TSDB_COMMIT_REPO_ID(ch) REPO_ID(TSDB_READ_REPO(&(ch->readh)))
#define TSDB_COMMIT_WRITE_FSET(ch) ((ch)->pWSet)
#define TSDB_COMMIT_TABLE(ch) TSDB_READ_TABLE(&(ch->readh))
#define TSDB_COMMIT_HEAD_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_HEAD)
#define TSDB_COMMIT_DATA_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_DATA)
#define TSDB_COMMIT_LAST_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_LAST)
#define TSDB_COMMIT_BUF(ch) TSDB_READ_BUF(&(ch->readh))
#define TSDB_COMMIT_COMP_BUF(ch) TSDB_READ_COMP_BUF(&(ch->readh))
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

static int tsdbCommitTSData(STsdbRepo *pRepo) {
  SMemTable *pMem = pRepo->imem;
  STsdbCfg * pCfg = &(pRepo->config);
  SCommitH   ch = {0};
  SFSIter    fsIter = {0};
  SDFileSet *pOldSet = NULL;
  SDFileSet  nSet;
  int        level, id;
  int        fid;

  if (pMem->numOfRows <= 0) return 0;

  // Resource initialization
  if (tsdbInitCommitH(pRepo, &ch) < 0) {
    // TODO
    return -1;
  }
  tsdbInitFSIter(pRepo, &fsIter);

  // Skip expired memory data and expired FSET
  tsdbSeekCommitIter(ch.iters, pMem->maxTables, ch.rtn.minKey);
  fid = tsdbNextCommitFid(ch.iters, pMem->maxTables);
  while (true) {
    pOldSet = tsdbFSIterNext(&fsIter);
    if (pOldSet == NULL || pOldSet->fid >= ch.rtn.minFid) break;
  }

  // Loop to commit to each file
  while (true) {
    // Loop over both on disk and memory
    if (pOldSet == NULL && fid == TSDB_IVLD_FID) break;

    // Only has existing FSET but no memory data to commit in this 
    // existing FSET, only check if file in correct retention
    if (pOldSet && (fid == TSDB_IVLD_FID || pOldSet->fid < fid)) {
      if (tsdbApplyRtn(*pOldSet, &(ch.rtn), &nSet) < 0) {
        return -1;
      }

      tsdbUpdateDFileSet(pRepo, &nSet);

      pOldSet = tsdbFSIterNext(&fsIter);
      continue;
    }

    SDFileSet *pCSet;
    int        cfid;

    if (pOldSet == NULL || pOldSet->fid > fid) {
      // Commit to a new FSET with fid: fid
      pCSet = NULL;
      cfid = fid;
    } else {
      // Commit to an existing FSET
      pCSet = pOldSet;
      cfid = pOldSet->fid;
      pOldSet = tsdbFSIterNext(&fsIter);
    }
    fid = tsdbNextCommitFid(ch.iters, pMem->maxTables);

    tsdbCommitToFile(pCSet, &ch, cfid);
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

static int tsdbCommitToFile(SCommitH *pch, SDFileSet *pOldSet, int fid) {
  int        level, id;
  int        nSet, ver;
  STsdbRepo *pRepo;

  ASSERT(pOldSet == NULL || pOldSet->fid == fid);

  tfsAllocDisk(tsdbGetFidLevel(fid, &(pch->rtn)), &level, &id);
  if (level == TFS_UNDECIDED_LEVEL) {
    // TODO
    return -1;
  }

  if (pOldSet == NULL || level > TSDB_FSET_LEVEL(pOldSet)) {
    // Create new fset to commit
    tsdbInitDFileSet(&nSet, pRepo, fid, ver, level, id);
    if (tsdbOpenDFileSet(&nSet, O_WRONLY | O_CREAT) < 0) {
      // TODO:
      return -1;
    }

    if (tsdbUpdateDFileSetHeader(&nSet) < 0) {
      // TODO
      return -1;
    }
  } else {
    level = TSDB_FSET_LEVEL(pOldSet);

    tsdbInitDFile(TSDB_DFILE_IN_SET(&nSet, TSDB_FILE_HEAD), ...);

    tsdbInitDFileWithOld(TSDB_DFILE_IN_SET(&nSet, TSDB_FILE_DATA), TSDB_DFILE_IN_SET(pOldSet, TSDB_FILE_DATA))

    SDFile *pDFile = TSDB_DFILE_IN_SET(&nSet, TSDB_FILE_LAST);
    if (pDFile->info.size < 32 * 1024 * 1024) {
      tsdbInitDFileWithOld(TSDB_DFILE_IN_SET(&nSet, TSDB_FILE_LAST), TSDB_DFILE_IN_SET(pOldSet, TSDB_FILE_LAST))
    } else {
      tsdbInitDFile(TSDB_DFILE_IN_SET(&nSet, TSDB_FILE_LAST), ...);
    }

    tsdbOpenDFileSet(&nSet, O_WRONLY | O_CREAT);

    // TODO: update file header
  }

  tsdbSetCommitFile(pch, pOldSet, &nSet);

  for (size_t tid = 0; tid < pMem->maxTables; tid++) {
    SCommitIter *pIter = pch->iters + tid;

    // No table exists, continue
    if (pIter->pTable == NULL) continue;

    if (tsdbCommitToTable(pch, tid) < 0) {
      // TODO
      return -1;
    }
  }

  tsdbUpdateDFileSet(pRepo, &wSet);
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

static int tsdbNextCommitFid(SCommitIter *iters, int niters) {
  int fid = TSDB_IVLD_FID;

  // TODO

  return fid;
}

static int tsdbApplyRtn(const SDFileSet oSet, const SRtn *pRtn, SDFileSet *pRSet) {
  int level, id;
  int vid, ver;

  tfsAllocDisk(tsdbGetFidLevel(oSet.fid, pRtn), &level, &id);

  if (level == TFS_UNDECIDED_LEVEL) {
    // terrno = TSDB_CODE_TDB_NO_AVAILABLE_DISK;
    return -1;
  }

  if (level > TSDB_FSET_LEVEL(pSet)) {
    tsdbInitDFileSet(pRSet, vid, TSDB_FSET_FID(&oSet), ver, level, id);
    if (tsdbCopyDFileSet(&oSet, pRSet) < 0) {
      return -1;
    }
  } else {
    tsdbInitDFileSetWithOld(pRSet, &oSet);
  }

  return 0;
}

static int tsdbCommitToTable(SCommitH *pch, int tid) {
  SCommitIter *pIter = pch->iters + tid;
  if (pIter->pTable == NULL) return 0;

  TSDB_RLOCK_TABLE(pIter->pTable);

  tsdbSetCommitTable(pch, pIter->pTable);

  // No memory data and no disk data, just return
  if (pIter->pIter == NULL && pch->readh.pBlkIdx == NULL) {
    TSDB_RUNLOCK_TABLE(pIter->pTable);
    return 0;
  }

  if (tsdbLoadBlockInfo(&(pch->readh), NULL) < 0) {
    TSDB_RUNLOCK_TABLE(pIter->pTable);
    return -1;
  }

  // Process merge commit
  int     nBlocks = (pch->readh.pBlkIdx == NULL) ? 0 : pch->readh.pBlkIdx->numOfBlocks;
  TSKEY   nextKey = tsdbNextIterKey(pIter->pIter);
  int     cidx = 0;
  void *  ptr = NULL;
  SBlock *pBlock;

  if (cidx < nBlocks) {
    pBlock = pch->readh.pBlkInfo->blocks + cidx;
  } else {
    pBlock = NULL;
  }

  while (true) {
    if ((nextKey == TSDB_DATA_TIMESTAMP_NULL || nextKey > pch->maxKey) && (pBlock == NULL)) break;

    if ((nextKey == TSDB_DATA_TIMESTAMP_NULL || nextKey > pch->maxKey) ||
        (pBlock && (!pBlock->last) && tsdbComparKeyBlock((void *)(&nextKey), pBlock) > 0)) {
      // TODO: move the block
      ASSERT(pBlock->numOfSubBlocks > 0);
      if (pBlock->numOfSubBlocks == 1) { // move super block
        if (taosArrayPush(pch->aSupBlk, (void *)pBlock) == NULL) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          TSDB_RUNLOCK_TABLE(pIter->pTable);
          return -1;
        }
      } else {

      }

      cidx++;
      if (cidx < nBlocks) {
        pBlock = pch->readh.pBlkInfo->blocks + cidx;
      } else {
        pBlock = NULL;
      }
    } else if ((cidx < nBlocks) && (pBlock->last || tsdbComparKeyBlock((void *)(&nextKey), pBlock) == 0)) {
      TSKEY keyLimit;
      if (cidx == nBlocks - 1) {
        keyLimit = pch->maxKey;
      } else {
        keyLimit = pBlock[1].keyFirst - 1;
      }

      if (tsdbMergeMemData(pch, pIter, pBlock, keyLimit) < 0) {
        TSDB_RUNLOCK_TABLE(pIter->pTable);
        return -1;
      }

      cidx++;
      if (cidx < nBlocks) {
        pBlock = pch->readh.pBlkInfo->blocks + cidx;
      } else {
        pBlock = NULL;
      }
      nextKey = tsdbNextIterKey(pIter->pIter);
    } else {
      if (pBlock == NULL) {
        if (tsdbCommitMemData(pch, pIter, pch->maxKey, false) < 0) {
          TSDB_RUNLOCK_TABLE(pIter->pTable);
          return -1;
        }
        nextKey = tsdbNextIterKey(pIter->pIter);
      } else {
        if (tsdbCommitMemData(pch, pIter, pBlock->keyFirst-1, true) < 0) {
          TSDB_RUNLOCK_TABLE(pIter->pTable);
          return -1;
        }
        nextKey = tsdbNextIterKey(pIter->pIter);
      }
    }

#if 0
    if (/* Key end */) {
      tsdbMoveBlock(); =============
    } else {
      if (/*block end*/) {
        // process append commit until pch->maxKey >>>>>>>
      } else {
        if (pBlock->last) {
          // TODO: merge the block ||||||||||||||||||||||
        } else {
          if (pBlock > nextKey) {
            // process append commit until pBlock->keyFirst-1 >>>>>>
          } else if (pBlock < nextKey) {
            // tsdbMoveBlock() ============
          } else {
            // merge the block ||||||||||||
          }
        }
      }
    }
#endif
  }

  TSDB_RUNLOCK_TABLE(pIter->pTable);

  if (tsdbWriteBlockInfo(pch) < 0) return -1;

  return 0;
}

static int tsdbSetCommitTable(SCommitH *pch, STable *pTable) {
  // TODO
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

static int tsdbAppendCommit(SCommitIter *pIter, TSKEY keyEnd) {
  // TODO
  return 0;
}

static int tsdbMergeCommit(SCommitIter *pIter, SBlock *pBlock, TSKEY keyEnd) {
  // TODO
  return 0;
}

static int tsdbWriteBlock(SCommitH *pCommih, SDFile *pDFile, SDataCols *pDataCols, SBlock *pBlock, bool isLast,
                          bool isSuper) {
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

static int tsdbMergeMemData(SCommitH *pCommith, SCommitIter *pIter, SBlock *pBlock, TSKEY keyLimit) {
  // TODO
  return 0;
}

static int tsdbMoveBlock(SCommitH *pCommith, int bidx) {
  SBlock *pBlock = pCommith->readh.pBlkInfo->blocks+bidx;
  SDFile *pCommitF = (pBlock->last) ? TSDB_COMMIT_LAST_FILE(pCommith) : TSDB_COMMIT_DATA_FILE(pCommith);
  SDFile *pReadF = (pBlock->last) ? TSDB_READ_LAST_FILE(&(pCommith->readh)) : TSDB_READ_DATA_FILE(&(pCommith->readh));
  SBlock  block;

  if (tfsIsSameFile(&(pCommitF->f), &(pReadF->f))) {
    if (pBlock->numOfSubBlocks == 1) {
      if (taosArrayPush(pCommith->aSupBlk, (void *)pBlock) == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }
    } else {
      block = *pBlock;
      block.offset = sizeof(SBlock) * taosArrayGetSize(pCommith->aSupBlock);

      if (taosArrayPush(pCommith->aSupBlk, (void *)(&block)) == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }

      if (taosArrayPushBatch(pCommith->aSubBlk, POINTER_SHIFT(pCommith->readh.pBlkInfo, pBlock->offset), pBlock->numOfSubBlocks) == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }
    }
  } else {
    if (tsdbLoadBlockData(&(pCommith->readh), pBlock, NULL) < 0) return -1;
    if (tsdbWriteBlock(pCommith, pCommitF, pCommith->readh.pDCols[0], &block, pBlock->last, true) < 0) return -1;
    if (taosArrayPush(pCommith->aSupBlk, (void *)(&block)) == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  }

  return 0;
}