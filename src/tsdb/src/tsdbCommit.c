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

#define TSDB_MAX_SUBBLOCKS 8
#define TSDB_KEY_FID(key, days, precision) ((key) / tsMsPerDay[(precision)] / (days))

typedef struct {
  int   minFid;
  int   midFid;
  int   maxFid;
  TSKEY minKey;
} SRtn;

typedef struct {
  SRtn         rtn;     // retention snapshot
  SFSIter      fsIter;  // tsdb file iterator
  int          niters;  // memory iterators
  SCommitIter *iters;
  bool         isRFileSet; // read and commit FSET
  SReadH       readh;
  SDFileSet    wSet;
  bool         isDFileSame;
  bool         isLFileSame;
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
#define TSDB_COMMIT_TXN_VERSION(ch) FS_TXN_VERSION(REPO_FS(TSDB_COMMIT_REPO(ch)))

static int  tsdbCommitMeta(STsdbRepo *pRepo);
static int  tsdbUpdateMetaRecord(STsdbFS *pfs, SMFile *pMFile, uint64_t uid, void *cont, int contLen);
static int  tsdbDropMetaRecord(STsdbFS *pfs, SMFile *pMFile, uint64_t uid);
static int  tsdbCommitTSData(STsdbRepo *pRepo);
static int  tsdbStartCommit(STsdbRepo *pRepo);
static void tsdbEndCommit(STsdbRepo *pRepo, int eno);
static int  tsdbCommitToFile(SCommitH *pCommith, SDFileSet *pSet, int fid);
static int  tsdbCreateCommitIters(SCommitH *pCommith);
static void tsdbDestroyCommitIters(SCommitH *pCommith);
static void tsdbSeekCommitIter(SCommitH *pCommith, TSKEY key);
static int  tsdbInitCommitH(SCommitH *pCommith, STsdbRepo *pRepo);
static void tsdbDestroyCommitH(SCommitH *pCommith);
static void tsdbGetRtnSnap(STsdbRepo *pRepo, SRtn *pRtn);
static int  tsdbGetFidLevel(int fid, SRtn *pRtn);
static int  tsdbNextCommitFid(SCommitH *pCommith);
static int  tsdbCommitToTable(SCommitH *pCommith, int tid);
static int  tsdbSetCommitTable(SCommitH *pCommith, STable *pTable);
static int  tsdbComparKeyBlock(const void *arg1, const void *arg2);
static int  tsdbWriteBlockInfo(SCommitH *pCommih);
static int  tsdbWriteBlockIdx(SCommitH *pCommih);
static int  tsdbCommitMemData(SCommitH *pCommith, SCommitIter *pIter, TSKEY keyLimit, bool toData);
static int  tsdbMergeMemData(SCommitH *pCommith, SCommitIter *pIter, int bidx);
static int  tsdbMoveBlock(SCommitH *pCommith, int bidx);
static int  tsdbCommitAddBlock(SCommitH *pCommith, const SBlock *pSupBlock, const SBlock *pSubBlocks, int nSubBlocks);
static int  tsdbMergeBlockData(SCommitH *pCommith, SCommitIter *pIter, SDataCols *pDataCols, TSKEY keyLimit,
                               bool isLastOneBlock);
static void tsdbResetCommitFile(SCommitH *pCommith);
static void tsdbResetCommitTable(SCommitH *pCommith);
static int  tsdbSetAndOpenCommitFile(SCommitH *pCommith, SDFileSet *pSet, int fid);
static void tsdbCloseCommitFile(SCommitH *pCommith, bool hasError);
static bool tsdbCanAddSubBlock(SCommitH *pCommith, SBlock *pBlock, SMergeInfo *pInfo);
static void tsdbLoadAndMergeFromCache(SDataCols *pDataCols, int *iter, SCommitIter *pCommitIter, SDataCols *pTarget,
                                      TSKEY maxKey, int maxRows, int8_t update);
static int  tsdbApplyRtn(STsdbRepo *pRepo);
static int  tsdbApplyRtnOnFSet(STsdbRepo *pRepo, SDFileSet *pSet, SRtn *pRtn);

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
  STsdbFS *  pfs = REPO_FS(pRepo);
  SMemTable *pMem = pRepo->imem;
  SMFile *   pOMFile = pfs->cstatus->pmf;
  SMFile     mf;
  SActObj *  pAct = NULL;
  SActCont * pCont = NULL;
  SListNode *pNode = NULL;
  SDiskID    did;

  ASSERT(pOMFile != NULL || listNEles(pMem->actList) > 0);

  if (listNEles(pMem->actList) <= 0) {
    // no meta data to commit, just keep the old meta file
    tsdbUpdateMFile(pfs, pOMFile);
    return 0;
  } else {
    // Create/Open a meta file or open the existing file
    if (pOMFile == NULL) {
      // Create a new meta file
      did.level = TFS_PRIMARY_LEVEL;
      did.id = TFS_PRIMARY_ID;
      tsdbInitMFile(&mf, did, REPO_ID(pRepo), FS_TXN_VERSION(REPO_FS(pRepo)));

      if (tsdbCreateMFile(&mf) < 0) {
        return -1;
      }
    } else {
      tsdbInitMFileEx(&mf, pOMFile);
      if (tsdbOpenMFile(&mf, O_WRONLY) < 0) {
        return -1;
      }
    }
  }

  // Loop to write
  while ((pNode = tdListPopHead(pMem->actList)) != NULL) {
    pAct = (SActObj *)pNode->data;
    if (pAct->act == TSDB_UPDATE_META) {
      pCont = (SActCont *)POINTER_SHIFT(pAct, sizeof(SActObj));
      if (tsdbUpdateMetaRecord(pfs, &mf, pAct->uid, (void *)(pCont->cont), pCont->len) < 0) {
        tsdbCloseMFile(&mf);
        return -1;
      }
    } else if (pAct->act == TSDB_DROP_META) {
      if (tsdbDropMetaRecord(pfs, &mf, pAct->uid) < 0) {
        tsdbCloseMFile(&mf);
        return -1;
      }
    } else {
      ASSERT(false);
    }
  }

  if (tsdbUpdateMFileHeader(&mf) < 0) {
    return -1;
  }

  TSDB_FILE_FSYNC(&mf);
  tsdbCloseMFile(&mf);
  tsdbUpdateMFile(pfs, &mf);

  return 0;
}

int tsdbEncodeKVRecord(void **buf, SKVRecord *pRecord) {
  int tlen = 0;
  tlen += taosEncodeFixedU64(buf, pRecord->uid);
  tlen += taosEncodeFixedI64(buf, pRecord->offset);
  tlen += taosEncodeFixedI64(buf, pRecord->size);

  return tlen;
}

void *tsdbDecodeKVRecord(void *buf, SKVRecord *pRecord) {
  buf = taosDecodeFixedU64(buf, &(pRecord->uid));
  buf = taosDecodeFixedI64(buf, &(pRecord->offset));
  buf = taosDecodeFixedI64(buf, &(pRecord->size));

  return buf;
}

static int tsdbUpdateMetaRecord(STsdbFS *pfs, SMFile *pMFile, uint64_t uid, void *cont, int contLen) {
  char      buf[64] = "\0";
  void *    pBuf = buf;
  SKVRecord rInfo;
  int64_t   offset;

  // Seek to end of meta file
  offset = tsdbSeekMFile(pMFile, 0, SEEK_END);
  if (offset < 0) {
    return -1;
  }

  rInfo.offset = offset;
  rInfo.uid = uid;
  rInfo.size = contLen;

  int tlen = tsdbEncodeKVRecord((void **)(&pBuf), &rInfo);
  if (tsdbAppendMFile(pMFile, buf, tlen, NULL) < tlen) {
    return -1;
  }

  if (tsdbAppendMFile(pMFile, cont, contLen, NULL) < contLen) {
    return -1;
  }

  tsdbUpdateMFileMagic(pMFile, POINTER_SHIFT(cont, contLen - sizeof(TSCKSUM)));
  SKVRecord *pRecord = taosHashGet(pfs->metaCache, (void *)&uid, sizeof(uid));
  if (pRecord != NULL) {
    pMFile->info.tombSize += pRecord->size;
  } else {
    pMFile->info.nRecords++;
  }
  taosHashPut(pfs->metaCache, (void *)(&uid), sizeof(uid), (void *)(&rInfo), sizeof(rInfo));

  return 0;
}

static int tsdbDropMetaRecord(STsdbFS *pfs, SMFile *pMFile, uint64_t uid) {
  SKVRecord rInfo = {0};
  char      buf[128] = "\0";

  SKVRecord *pRecord = taosHashGet(pfs->metaCache, (void *)(&uid), sizeof(uid));
  if (pRecord == NULL) {
    tsdbError("failed to drop KV store record with key %" PRIu64 " since not find", uid);
    return -1;
  }

  rInfo.offset = -pRecord->offset;
  rInfo.uid = pRecord->uid;
  rInfo.size = pRecord->size;

  void *pBuf = buf;
  tsdbEncodeKVRecord(&pBuf, &rInfo);

  if (tsdbAppendMFile(pMFile, buf, POINTER_DISTANCE(pBuf, buf), NULL) < 0) {
    return -1;
  }

  pMFile->info.magic = taosCalcChecksum(pMFile->info.magic, (uint8_t *)buf, (uint32_t)POINTER_DISTANCE(pBuf, buf));
  pMFile->info.nDels++;
  pMFile->info.nRecords--;
  pMFile->info.tombSize += (rInfo.size + sizeof(SKVRecord) * 2);

  taosHashRemove(pfs->metaCache, (void *)(&uid), sizeof(uid));
  return 0;
}

// =================== Commit Time-Series Data
static int tsdbCommitTSData(STsdbRepo *pRepo) {
  SMemTable *pMem = pRepo->imem;
  SCommitH   commith = {0};
  SDFileSet *pSet = NULL;
  int        fid;

  if (pMem->numOfRows <= 0) {
    // No memory data, just apply retention on each file on disk
    if (tsdbApplyRtn(pRepo) < 0) {
      return -1;
    }
    return 0;
  }

  // Resource initialization
  if (tsdbInitCommitH(&commith, pRepo) < 0) {
    return -1;
  }

  // Skip expired memory data and expired FSET
  tsdbSeekCommitIter(&commith, commith.rtn.minKey);
  while ((pSet = tsdbFSIterNext(&(commith.fsIter)))) {
    if (pSet->fid >= commith.rtn.minFid) break;
  }

  // Loop to commit to each file
  fid = tsdbNextCommitFid(&(commith));
  while (true) {
    // Loop over both on disk and memory
    if (pSet == NULL && fid == TSDB_IVLD_FID) break;

    if (pSet && (fid == TSDB_IVLD_FID || pSet->fid < fid)) {
      // Only has existing FSET but no memory data to commit in this
      // existing FSET, only check if file in correct retention
      if (tsdbApplyRtnOnFSet(pRepo, pSet, &(commith.rtn)) < 0) {
        tsdbDestroyCommitH(&commith);
        return -1;
      }

      pSet = tsdbFSIterNext(&(commith.fsIter));
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
        pSet = tsdbFSIterNext(&(commith.fsIter));
      }
      fid = tsdbNextCommitFid(&commith);

      if (tsdbCommitToFile(&commith, pCSet, cfid) < 0) {
        tsdbDestroyCommitH(&commith);
        return -1;
      }
    }
  }

  tsdbDestroyCommitH(&commith);
  return 0;
}

static int tsdbStartCommit(STsdbRepo *pRepo) {
  SMemTable *pMem = pRepo->imem;

  ASSERT(pMem->numOfRows > 0 || listNEles(pMem->actList) > 0);

  tsdbInfo("vgId:%d start to commit! keyFirst %" PRId64 " keyLast %" PRId64 " numOfRows %" PRId64 " meta rows: %d",
           REPO_ID(pRepo), pMem->keyFirst, pMem->keyLast, pMem->numOfRows, listNEles(pMem->actList));

  tsdbStartFSTxn(REPO_FS(pRepo), pMem->pointsAdd, pMem->storageAdd);

  pRepo->code = TSDB_CODE_SUCCESS;
  return 0;
}

static void tsdbEndCommit(STsdbRepo *pRepo, int eno) {
  if (eno != TSDB_CODE_SUCCESS) {
    tsdbEndFSTxnWithError(REPO_FS(pRepo));
  } else {
    tsdbEndFSTxn(REPO_FS(pRepo));
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

#if 0
static bool tsdbHasDataToCommit(SCommitIter *iters, int nIters, TSKEY minKey, TSKEY maxKey) {
  for (int i = 0; i < nIters; i++) {
    TSKEY nextKey = tsdbNextIterKey((iters + i)->pIter);
    if (nextKey != TSDB_DATA_TIMESTAMP_NULL && (nextKey >= minKey && nextKey <= maxKey)) return true;
  }
  return false;
}
#endif

static int tsdbCommitToFile(SCommitH *pCommith, SDFileSet *pSet, int fid) {
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg * pCfg = REPO_CFG(pRepo);

  ASSERT(pSet == NULL || pSet->fid == fid);

  tsdbResetCommitFile(pCommith);
  tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, fid, &(pCommith->minKey), &(pCommith->maxKey));

  // Set and open files
  if (tsdbSetAndOpenCommitFile(pCommith, pSet, fid) < 0) {
    return -1;
  }

  // Loop to commit each table data
  for (int tid = 1; tid < pCommith->niters; tid++) {
    SCommitIter *pIter = pCommith->iters + tid;

    if (pIter->pTable == NULL) continue;

    if (tsdbCommitToTable(pCommith, tid) < 0) {
      // TODO: revert the file change
      tsdbCloseCommitFile(pCommith, true);
      return -1;
    }
  }

  if (tsdbWriteBlockIdx(pCommith) < 0) {
    tsdbCloseCommitFile(pCommith, true);
    return -1;
  }

  // Close commit file
  tsdbCloseCommitFile(pCommith, false);

  if (tsdbUpdateDFileSet(REPO_FS(pRepo), &(pCommith->wSet)) < 0) {
    return -1;
  }

  return 0;
}

static int tsdbCreateCommitIters(SCommitH *pCommith) {
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  SMemTable *pMem = pRepo->imem;
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  pCommith->niters = pMem->maxTables;
  pCommith->iters = (SCommitIter *)calloc(pMem->maxTables, sizeof(SCommitIter));
  if (pCommith->iters == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (tsdbRLockRepoMeta(pRepo) < 0) return -1;

  // reference all tables
  for (int i = 0; i < pMem->maxTables; i++) {
    if (pMeta->tables[i] != NULL) {
      tsdbRefTable(pMeta->tables[i]);
      pCommith->iters[i].pTable = pMeta->tables[i];
    }
  }

  if (tsdbUnlockRepoMeta(pRepo) < 0) return -1;

  for (int i = 0; i < pMem->maxTables; i++) {
    if ((pCommith->iters[i].pTable != NULL) && (pMem->tData[i] != NULL) &&
        (TABLE_UID(pCommith->iters[i].pTable) == pMem->tData[i]->uid)) {
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
      tSkipListDestroyIter(pCommith->iters[i].pIter);
    }
  }

  free(pCommith->iters);
  pCommith->iters = NULL;
  pCommith->niters = 0;
}

// Skip all keys until key (not included)
static void tsdbSeekCommitIter(SCommitH *pCommith, TSKEY key) {
  for (int i = 0; i < pCommith->niters; i++) {
    SCommitIter *pIter = pCommith->iters + i;
    if (pIter->pTable == NULL || pIter->pIter == NULL) continue;

    tsdbLoadDataFromCache(pIter->pTable, pIter->pIter, key - 1, INT32_MAX, NULL, NULL, 0, true, NULL);
  }
}

static int tsdbInitCommitH(SCommitH *pCommith, STsdbRepo *pRepo) {
  STsdbCfg *pCfg = REPO_CFG(pRepo);

  memset(pCommith, 0, sizeof(*pCommith));
  tsdbGetRtnSnap(pRepo, &(pCommith->rtn));

  TSDB_FSET_SET_CLOSED(TSDB_COMMIT_WRITE_FSET(pCommith));

  // Init read handle
  if (tsdbInitReadH(&(pCommith->readh), pRepo) < 0) {
    return -1;
  }

  // Init file iterator
  tsdbFSIterInit(&(pCommith->fsIter), REPO_FS(pRepo), TSDB_FS_ITER_FORWARD);

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
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  int        fid = TSDB_IVLD_FID;

  for (int i = 0; i < pCommith->niters; i++) {
    SCommitIter *pIter = pCommith->iters + i;
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
  TSKEY        nextKey = tsdbNextIterKey(pIter->pIter);

  tsdbResetCommitTable(pCommith);

  TSDB_RLOCK_TABLE(pIter->pTable);

  // Set commit table
  if (tsdbSetCommitTable(pCommith, pIter->pTable) < 0) {
    TSDB_RUNLOCK_TABLE(pIter->pTable);
    return -1;
  }

  // No disk data and no memory data, just return
  if (pCommith->readh.pBlkIdx == NULL && (nextKey == TSDB_DATA_TIMESTAMP_NULL || nextKey > pCommith->maxKey)) {
    TSDB_RUNLOCK_TABLE(pIter->pTable);
    return 0;
  }

  // Must has disk data or has memory data
  int     nBlocks;
  int     bidx = 0;
  SBlock *pBlock;

  if (pCommith->readh.pBlkIdx) {
    if (tsdbLoadBlockInfo(&(pCommith->readh), NULL) < 0) {
      TSDB_RUNLOCK_TABLE(pIter->pTable);
      return -1;
    }

    nBlocks = pCommith->readh.pBlkIdx->numOfBlocks;
  } else {
    nBlocks = 0;
  }

  if (bidx < nBlocks) {
    pBlock = pCommith->readh.pBlkInfo->blocks + bidx;
  } else {
    pBlock = NULL;
  }

  while (true) {
    if (pBlock == NULL && (nextKey == TSDB_DATA_TIMESTAMP_NULL || nextKey > pCommith->maxKey)) break;

    if ((nextKey == TSDB_DATA_TIMESTAMP_NULL || nextKey > pCommith->maxKey) ||
        (pBlock && (!pBlock->last) && tsdbComparKeyBlock((void *)(&nextKey), pBlock) > 0)) {
      if (tsdbMoveBlock(pCommith, bidx) < 0) {
        TSDB_RUNLOCK_TABLE(pIter->pTable);
        return -1;
      }

      bidx++;
      if (bidx < nBlocks) {
        pBlock = pCommith->readh.pBlkInfo->blocks + bidx;
      } else {
        pBlock = NULL;
      }
    } else if (pBlock && (pBlock->last || tsdbComparKeyBlock((void *)(&nextKey), pBlock) == 0)) {
      // merge pBlock data and memory data
      if (tsdbMergeMemData(pCommith, pIter, bidx) < 0) {
        TSDB_RUNLOCK_TABLE(pIter->pTable);
        return -1;
      }

      bidx++;
      if (bidx < nBlocks) {
        pBlock = pCommith->readh.pBlkInfo->blocks + bidx;
      } else {
        pBlock = NULL;
      }
      nextKey = tsdbNextIterKey(pIter->pIter);
    } else {
      // Only commit memory data
      if (pBlock == NULL) {
        if (tsdbCommitMemData(pCommith, pIter, pCommith->maxKey, false) < 0) {
          TSDB_RUNLOCK_TABLE(pIter->pTable);
          return -1;
        }
      } else {
        if (tsdbCommitMemData(pCommith, pIter, pBlock->keyFirst - 1, true) < 0) {
          TSDB_RUNLOCK_TABLE(pIter->pTable);
          return -1;
        }
      }
      nextKey = tsdbNextIterKey(pIter->pIter);
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
  } else {
    pCommith->readh.pBlkIdx = NULL;
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

static int tsdbWriteBlock(SCommitH *pCommith, SDFile *pDFile, SDataCols *pDataCols, SBlock *pBlock, bool isLast,
                          bool isSuper) {
  STsdbRepo * pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg *  pCfg = REPO_CFG(pRepo);
  SBlockData *pBlockData;
  int64_t     offset = 0;
  STable *    pTable = TSDB_COMMIT_TABLE(pCommith);
  int         rowsToWrite = pDataCols->numOfRows;

  ASSERT(rowsToWrite > 0 && rowsToWrite <= pCfg->maxRowsPerFileBlock);
  ASSERT((!isLast) || rowsToWrite < pCfg->minRowsPerFileBlock);

  // Seek file
  offset = tsdbSeekDFile(pDFile, 0, SEEK_END);
  if (offset < 0) {
    return -1;
  }

  // Make buffer space
  if (tsdbMakeRoom((void **)(&TSDB_COMMIT_BUF(pCommith)), TSDB_BLOCK_STATIS_SIZE(pDataCols->numOfCols)) < 0) {
    return -1;
  }
  pBlockData = (SBlockData *)TSDB_COMMIT_BUF(pCommith);

  // Get # of cols not all NULL(not including key column)
  int nColsNotAllNull = 0;
  for (int ncol = 1; ncol < pDataCols->numOfCols; ncol++) {  // ncol from 1, we skip the timestamp column
    SDataCol * pDataCol = pDataCols->cols + ncol;
    SBlockCol *pBlockCol = pBlockData->cols + nColsNotAllNull;

    if (isNEleNull(pDataCol, rowsToWrite)) {  // all data to commit are NULL, just ignore it
      continue;
    }

    memset(pBlockCol, 0, sizeof(*pBlockCol));

    pBlockCol->colId = pDataCol->colId;
    pBlockCol->type = pDataCol->type;
    if (tDataTypeDesc[pDataCol->type].getStatisFunc) {
      (*tDataTypeDesc[pDataCol->type].getStatisFunc)(pDataCol->pData, rowsToWrite, &(pBlockCol->min), &(pBlockCol->max),
                                                     &(pBlockCol->sum), &(pBlockCol->minIndex), &(pBlockCol->maxIndex),
                                                     &(pBlockCol->numOfNull));
    }
    nColsNotAllNull++;
  }

  ASSERT(nColsNotAllNull >= 0 && nColsNotAllNull <= pDataCols->numOfCols);

  // Compress the data if neccessary
  int     tcol = 0;  // counter of not all NULL and written columns
  int32_t toffset = 0;
  int32_t tsize = TSDB_BLOCK_STATIS_SIZE(nColsNotAllNull);
  int32_t lsize = tsize;
  int32_t keyLen = 0;
  for (int ncol = 0; ncol < pDataCols->numOfCols; ncol++) {
    // All not NULL columns finish
    if (ncol != 0 && tcol >= nColsNotAllNull) break;

    SDataCol * pDataCol = pDataCols->cols + ncol;
    SBlockCol *pBlockCol = pBlockData->cols + tcol;

    if (ncol != 0 && (pDataCol->colId != pBlockCol->colId)) continue;

    int32_t flen;  // final length
    int32_t tlen = dataColGetNEleLen(pDataCol, rowsToWrite);
    void *  tptr;

    // Make room
    if (tsdbMakeRoom((void **)TSDB_COMMIT_BUF(pCommith), lsize + tlen + COMP_OVERFLOW_BYTES + sizeof(TSCKSUM)) < 0) {
      return -1;
    }
    pBlockData = (SBlockData *)TSDB_COMMIT_BUF(pCommith);
    tptr = POINTER_SHIFT(pBlockData, lsize);

    if (pCfg->compression == TWO_STAGE_COMP &&
        tsdbMakeRoom((void **)TSDB_COMMIT_COMP_BUF(pCommith), tlen + COMP_OVERFLOW_BYTES) < 0) {
      return -1;
    }

    // Compress or just copy
    if (pCfg->compression) {
      flen = (*(tDataTypeDesc[pDataCol->type].compFunc))((char *)pDataCol->pData, tlen, rowsToWrite, tptr,
                                                         tlen + COMP_OVERFLOW_BYTES, pCfg->compression,
                                                         TSDB_COMMIT_COMP_BUF(pCommith), tlen + COMP_OVERFLOW_BYTES);
    } else {
      flen = tlen;
      memcpy(tptr, pDataCol->pData, flen);
    }

    // Add checksum
    ASSERT(flen > 0);
    flen += sizeof(TSCKSUM);
    taosCalcChecksumAppend(0, (uint8_t *)tptr, flen);
    tsdbUpdateDFileMagic(pDFile, POINTER_SHIFT(tptr, flen - sizeof(TSCKSUM)));

    if (ncol != 0) {
      pBlockCol->offset = toffset;
      pBlockCol->len = flen;
      tcol++;
    } else {
      keyLen = flen;
    }

    toffset += flen;
    lsize += flen;
  }

  pBlockData->delimiter = TSDB_FILE_DELIMITER;
  pBlockData->uid = TABLE_UID(pTable);
  pBlockData->numOfCols = nColsNotAllNull;

  taosCalcChecksumAppend(0, (uint8_t *)pBlockData, tsize);
  tsdbUpdateDFileMagic(pDFile, POINTER_SHIFT(pBlockData, tsize - sizeof(TSCKSUM)));

  // Write the whole block to file
  if (tsdbWriteDFile(pDFile, (void *)pBlockData, lsize) < lsize) {
    return -1;
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
  pBlock->keyLast = dataColsKeyLast(pDataCols);

  pDFile->info.size += pBlock->len;

  tsdbDebug("vgId:%d tid:%d a block of data is written to file %s, offset %" PRId64
            " numOfRows %d len %d numOfCols %" PRId16 " keyFirst %" PRId64 " keyLast %" PRId64,
            REPO_ID(pRepo), TABLE_TID(pTable), TSDB_FILE_FULL_NAME(pDFile), offset, rowsToWrite, pBlock->len,
            pBlock->numOfCols, pBlock->keyFirst, pBlock->keyLast);

  return 0;
}

static int tsdbWriteBlockInfo(SCommitH *pCommih) {
  SDFile *    pHeadf = TSDB_COMMIT_HEAD_FILE(pCommih);
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

  if (nSupBlocks <= 0) {
    // No data (data all deleted)
    return 0;
  }

  tlen = sizeof(SBlockInfo) + sizeof(SBlock) * (nSupBlocks + nSubBlocks) + sizeof(TSCKSUM);

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

  if (tsdbAppendDFile(pHeadf, TSDB_COMMIT_BUF(pCommih), tlen, &offset) < 0) {
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

  if (nidx <= 0) {
    // All data are deleted
    return 0;
  }

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
  } else if (tsdbCanAddSubBlock(pCommith, pBlock, &mInfo)) {
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
  SBlock *pBlock = pCommith->readh.pBlkInfo->blocks + bidx;
  SDFile *pCommitF = (pBlock->last) ? TSDB_COMMIT_LAST_FILE(pCommith) : TSDB_COMMIT_DATA_FILE(pCommith);
  // SDFile *pReadF = (pBlock->last) ? TSDB_READ_LAST_FILE(&(pCommith->readh)) : TSDB_READ_DATA_FILE(&(pCommith->readh));
  SBlock  block;

  if ((pBlock->last && pCommith->isLFileSame) || ((!pBlock->last) && pCommith->isDFileSame)) {
    if (pBlock->numOfSubBlocks == 1) {
      if (tsdbCommitAddBlock(pCommith, pBlock, NULL, 0) < 0) return -1;
    } else {
      block = *pBlock;
      block.offset = sizeof(SBlock) * taosArrayGetSize(pCommith->aSupBlk);

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

  return 0;
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
  pCommith->isRFileSet = false;
  pCommith->isDFileSame = false;
  pCommith->isLFileSame = false;
  taosArrayClear(pCommith->aBlkIdx);
}

static void tsdbResetCommitTable(SCommitH *pCommith) {
  tdResetDataCols(pCommith->pDataCols);
  taosArrayClear(pCommith->aSubBlk);
  taosArrayClear(pCommith->aSupBlk);
}

static int tsdbSetAndOpenCommitFile(SCommitH *pCommith, SDFileSet *pSet, int fid) {
  SDiskID    did;
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  SDFileSet *pWSet = TSDB_COMMIT_WRITE_FSET(pCommith);

  tfsAllocDisk(tsdbGetFidLevel(fid, &(pCommith->rtn)), &(did.level), &(did.id));
  if (did.level == TFS_UNDECIDED_LEVEL) {
    terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
    return -1;
  }

  // Open read FSET
  if (pSet) {
    if (tsdbSetAndOpenReadFSet(&(pCommith->readh), pSet) < 0) {
      return -1;
    }

    pCommith->isRFileSet = true;

    if (tsdbLoadBlockIdx(&(pCommith->readh)) < 0) {
      tsdbCloseAndUnsetFSet(&(pCommith->readh));
      return -1;
    }
  } else {
    pCommith->isRFileSet = false;
  }

  // Set and open commit FSET
  if (pSet == NULL || did.level > TSDB_FSET_LEVEL(pSet)) {
    // Create a new FSET to write data
    tsdbInitDFileSet(pWSet, did, REPO_ID(pRepo), fid, FS_TXN_VERSION(REPO_FS(pRepo)));

    if (tsdbCreateDFileSet(pWSet) < 0) {
      if (pCommith->isRFileSet) {
        tsdbCloseAndUnsetFSet(&(pCommith->readh));
      }
      return -1;
    }

    pCommith->isDFileSame = false;
    pCommith->isLFileSame = false;
  } else {
    did.level = TSDB_FSET_LEVEL(pSet);
    did.id = TSDB_FSET_ID(pSet);

    // TSDB_FILE_HEAD
    SDFile *pWHeadf = TSDB_COMMIT_HEAD_FILE(pCommith);
    tsdbInitDFile(pWHeadf, did, REPO_ID(pRepo), fid, FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_FILE_HEAD);
    if (tsdbCreateDFile(pWHeadf) < 0) {
      if (pCommith->isRFileSet) {
        tsdbCloseAndUnsetFSet(&(pCommith->readh));
        return -1;
      }
    }

    // TSDB_FILE_DATA
    SDFile *pRDataf = TSDB_READ_DATA_FILE(&(pCommith->readh));
    SDFile *pWDataf = TSDB_COMMIT_DATA_FILE(pCommith);
    tsdbInitDFileEx(pWHeadf, pRDataf);
    if (tsdbOpenDFile(pWDataf, O_WRONLY) < 0) {
      tsdbCloseDFile(pWHeadf);
      tsdbRemoveDFile(pWHeadf);
      if (pCommith->isRFileSet) {
        tsdbCloseAndUnsetFSet(&(pCommith->readh));
        return -1;
      }
    }
    pCommith->isDFileSame = true;

    // TSDB_FILE_LAST
    SDFile *pRLastf = TSDB_READ_LAST_FILE(&(pCommith->readh));
    SDFile *pWLastf = TSDB_COMMIT_LAST_FILE(pCommith);
    if (pRLastf->info.size < 32 * 1024) {
      tsdbInitDFileEx(pWLastf, pRLastf);
      pCommith->isLFileSame = true;

      if (tsdbOpenDFile(pWLastf, O_WRONLY) < 0) {
        tsdbCloseDFileSet(pWSet);
        tsdbRemoveDFile(pWHeadf);
        if (pCommith->isRFileSet) {
          tsdbCloseAndUnsetFSet(&(pCommith->readh));
          return -1;
        }
      }
    } else {
      tsdbInitDFile(pWLastf, did, REPO_ID(pRepo), fid, FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_FILE_LAST);
      pCommith->isLFileSame = false;

      if (tsdbCreateDFile(pWLastf) < 0) {
        tsdbCloseDFileSet(pWSet);
        tsdbRemoveDFile(pWHeadf);
        if (pCommith->isRFileSet) {
          tsdbCloseAndUnsetFSet(&(pCommith->readh));
          return -1;
        }
      }
    }
  }

  return 0;
}

static void tsdbCloseCommitFile(SCommitH *pCommith, bool hasError) {
  if (pCommith->isRFileSet) {
    tsdbCloseAndUnsetFSet(&(pCommith->readh));
  }

  if (!hasError) {
    TSDB_FSET_FSYNC(TSDB_COMMIT_WRITE_FSET(pCommith));
  }
  tsdbCloseDFileSet(TSDB_COMMIT_WRITE_FSET(pCommith));
}

static bool tsdbCanAddSubBlock(SCommitH *pCommith, SBlock *pBlock, SMergeInfo *pInfo) {
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  int        mergeRows = pBlock->numOfRows + pInfo->rowsInserted - pInfo->rowsDeleteSucceed;

  ASSERT(mergeRows > 0);

  if (pBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS && pInfo->nOperations <= pCfg->maxRowsPerFileBlock) {
    if (pBlock->last) {
      if (pCommith->isLFileSame && mergeRows < pCfg->minRowsPerFileBlock) return true;
    } else {
      if (mergeRows < pCfg->maxRowsPerFileBlock) return true;
    }
  }

  return false;
}

static int tsdbApplyRtn(STsdbRepo *pRepo) {
  SRtn       rtn;
  SFSIter    fsiter;
  STsdbFS *  pfs = REPO_FS(pRepo);
  SDFileSet *pSet;

  // Get retentioni snapshot
  tsdbGetRtnSnap(pRepo, &rtn);

  tsdbFSIterInit(&fsiter, pfs, TSDB_FS_ITER_FORWARD);
  while ((pSet = tsdbFSIterNext(&fsiter))) {
    if (pSet->fid < rtn.minFid) continue;

    if (tsdbApplyRtnOnFSet(pRepo, pSet, &rtn) < 0) {
      return -1;
    }
  }

  return 0;
}

static int tsdbApplyRtnOnFSet(STsdbRepo *pRepo, SDFileSet *pSet, SRtn *pRtn) {
  SDiskID   did;
  SDFileSet nSet;
  STsdbFS * pfs = REPO_FS(pRepo);

  ASSERT(pSet->fid >= pRtn->minFid);

  tfsAllocDisk(tsdbGetFidLevel(pSet->fid, pRtn), &(did.level), &(did.id));
  if (did.level == TFS_UNDECIDED_LEVEL) {
    terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
    return -1;
  }

  if (did.level > TSDB_FSET_LEVEL(pSet)) {
    // Need to move the FSET to higher level
    tsdbInitDFileSet(&nSet, did, REPO_ID(pRepo), pSet->fid, FS_TXN_VERSION(pfs));

    if (tsdbCopyDFileSet(pSet, &nSet) < 0) {
      return -1;
    }

    if (tsdbUpdateDFileSet(pfs, &nSet) < 0) {
      return -1;
    }
  } else {
    // On a correct level
    if (tsdbUpdateDFileSet(pfs, pSet) < 0) {
      return -1;
    }
  }

  return 0;
}