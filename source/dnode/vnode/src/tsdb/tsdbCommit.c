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

#include "tsdbDef.h"

#define TSDB_MAX_SUBBLOCKS 8

typedef struct {
  STable            *pTable;
  SSkipListIterator *pIter;
} SCommitIter;

typedef struct {
  SRtn         rtn;     // retention snapshot
  SFSIter      fsIter;  // tsdb file iterator
  int          niters;  // memory iterators
  SCommitIter *iters;
  bool         isRFileSet;  // read and commit FSET
  SReadH       readh;
  SDFileSet    wSet;
  bool         isDFileSame;
  bool         isLFileSame;
  TSKEY        minKey;
  TSKEY        maxKey;
  SArray      *aBlkIdx;  // SBlockIdx array
  STable      *pTable;
  SArray      *aSupBlk;  // Table super-block array
  SArray      *aSubBlk;  // table sub-block array
  SDataCols   *pDataCols;
} SCommitH;

#define TSDB_DEFAULT_BLOCK_ROWS(maxRows) ((maxRows)*4 / 5)

#define TSDB_COMMIT_REPO(ch) TSDB_READ_REPO(&(ch->readh))
#define TSDB_COMMIT_REPO_ID(ch) REPO_ID(TSDB_READ_REPO(&(ch->readh)))
#define TSDB_COMMIT_WRITE_FSET(ch) (&((ch)->wSet))
#define TSDB_COMMIT_TABLE(ch) ((ch)->pTable)
#define TSDB_COMMIT_HEAD_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_HEAD)
#define TSDB_COMMIT_DATA_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_DATA)
#define TSDB_COMMIT_LAST_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_LAST)
#define TSDB_COMMIT_SMAD_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_SMAD)
#define TSDB_COMMIT_SMAL_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_SMAL)
#define TSDB_COMMIT_BUF(ch) TSDB_READ_BUF(&((ch)->readh))
#define TSDB_COMMIT_COMP_BUF(ch) TSDB_READ_COMP_BUF(&((ch)->readh))
#define TSDB_COMMIT_EXBUF(ch) TSDB_READ_EXBUF(&((ch)->readh))
#define TSDB_COMMIT_DEFAULT_ROWS(ch) TSDB_DEFAULT_BLOCK_ROWS(TSDB_COMMIT_REPO(ch)->config.maxRowsPerFileBlock)
#define TSDB_COMMIT_TXN_VERSION(ch) FS_TXN_VERSION(REPO_FS(TSDB_COMMIT_REPO(ch)))

static void tsdbStartCommit(STsdb *pRepo);
static void tsdbEndCommit(STsdb *pTsdb, int eno);
static int  tsdbInitCommitH(SCommitH *pCommith, STsdb *pRepo);
static void tsdbSeekCommitIter(SCommitH *pCommith, TSKEY key);
static int  tsdbNextCommitFid(SCommitH *pCommith);
static void tsdbDestroyCommitH(SCommitH *pCommith);
static int  tsdbCreateCommitIters(SCommitH *pCommith);
static void tsdbDestroyCommitIters(SCommitH *pCommith);
static int  tsdbCommitToFile(SCommitH *pCommith, SDFileSet *pSet, int fid);
static void tsdbResetCommitFile(SCommitH *pCommith);
static int  tsdbSetAndOpenCommitFile(SCommitH *pCommith, SDFileSet *pSet, int fid);
// static int  tsdbCommitMeta(STsdbRepo *pRepo);
// static int  tsdbUpdateMetaRecord(STsdbFS *pfs, SMFile *pMFile, uint64_t uid, void *cont, int contLen, bool compact);
// static int  tsdbDropMetaRecord(STsdbFS *pfs, SMFile *pMFile, uint64_t uid);
// static int  tsdbCompactMetaFile(STsdbRepo *pRepo, STsdbFS *pfs, SMFile *pMFile);
static int  tsdbCommitToTable(SCommitH *pCommith, int tid);
static int  tsdbSetCommitTable(SCommitH *pCommith, STable *pTable);
static int  tsdbComparKeyBlock(const void *arg1, const void *arg2);
static int  tsdbWriteBlockInfo(SCommitH *pCommih);
static int  tsdbCommitMemData(SCommitH *pCommith, SCommitIter *pIter, TSKEY keyLimit, bool toData);
static int  tsdbMergeMemData(SCommitH *pCommith, SCommitIter *pIter, int bidx);
static int  tsdbMoveBlock(SCommitH *pCommith, int bidx);
static int  tsdbCommitAddBlock(SCommitH *pCommith, const SBlock *pSupBlock, const SBlock *pSubBlocks, int nSubBlocks);
static int  tsdbMergeBlockData(SCommitH *pCommith, SCommitIter *pIter, SDataCols *pDataCols, TSKEY keyLimit,
                               bool isLastOneBlock);
static void tsdbResetCommitTable(SCommitH *pCommith);
static void tsdbCloseCommitFile(SCommitH *pCommith, bool hasError);
static bool tsdbCanAddSubBlock(SCommitH *pCommith, SBlock *pBlock, SMergeInfo *pInfo);
static void tsdbLoadAndMergeFromCache(SDataCols *pDataCols, int *iter, SCommitIter *pCommitIter, SDataCols *pTarget,
                                      TSKEY maxKey, int maxRows, int8_t update);
int         tsdbWriteBlockIdx(SDFile *pHeadf, SArray *pIdxA, void **ppBuf);

int tsdbApplyRtnOnFSet(STsdb *pRepo, SDFileSet *pSet, SRtn *pRtn) {
  SDiskID   did;
  SDFileSet nSet = {0};
  STsdbFS  *pfs = REPO_FS(pRepo);
  int       level;

  ASSERT(pSet->fid >= pRtn->minFid);

  level = tsdbGetFidLevel(pSet->fid, pRtn);

  if (tfsAllocDisk(pRepo->pTfs, level, &did) < 0) {
    terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
    return -1;
  }

  if (did.level > TSDB_FSET_LEVEL(pSet)) {
    // Need to move the FSET to higher level
    tsdbInitDFileSet(pRepo, &nSet, did, pSet->fid, FS_TXN_VERSION(pfs));

    if (tsdbCopyDFileSet(pSet, &nSet) < 0) {
      tsdbError("vgId:%d failed to copy FSET %d from level %d to level %d since %s", REPO_ID(pRepo), pSet->fid,
                TSDB_FSET_LEVEL(pSet), did.level, tstrerror(terrno));
      return -1;
    }

    if (tsdbUpdateDFileSet(pfs, &nSet) < 0) {
      return -1;
    }

    tsdbInfo("vgId:%d FSET %d is copied from level %d disk id %d to level %d disk id %d", REPO_ID(pRepo), pSet->fid,
             TSDB_FSET_LEVEL(pSet), TSDB_FSET_ID(pSet), did.level, did.id);
  } else {
    // On a correct level
    if (tsdbUpdateDFileSet(pfs, pSet) < 0) {
      return -1;
    }
  }

  return 0;
}

int tsdbPrepareCommit(STsdb *pTsdb) {
  if (pTsdb->mem == NULL) return 0;

  ASSERT(pTsdb->imem == NULL);

  pTsdb->imem = pTsdb->mem;
  pTsdb->mem = NULL;
  return 0;
}

int tsdbCommit(STsdb *pRepo) {
  STsdbMemTable *pMem = pRepo->imem;
  SCommitH       commith = {0};
  SDFileSet     *pSet = NULL;
  int            fid;

  if (pRepo->imem == NULL) return 0;

  tsdbStartCommit(pRepo);
  // Resource initialization
  if (tsdbInitCommitH(&commith, pRepo) < 0) {
    return -1;
  }

  // Skip expired memory data and expired FSET
  tsdbSeekCommitIter(&commith, commith.rtn.minKey);
  while ((pSet = tsdbFSIterNext(&(commith.fsIter)))) {
    if (pSet->fid < commith.rtn.minFid) {
      tsdbInfo("vgId:%d FSET %d on level %d disk id %d expires, remove it", REPO_ID(pRepo), pSet->fid,
               TSDB_FSET_LEVEL(pSet), TSDB_FSET_ID(pSet));
    } else {
      break;
    }
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

      if (tsdbCommitToFile(&commith, pCSet, cfid) < 0) {
        tsdbDestroyCommitH(&commith);
        return -1;
      }

      fid = tsdbNextCommitFid(&commith);
    }
  }

  tsdbDestroyCommitH(&commith);
  tsdbEndCommit(pRepo, TSDB_CODE_SUCCESS);

  return 0;
}

void tsdbGetRtnSnap(STsdb *pRepo, SRtn *pRtn) {
  STsdbCfg *pCfg = REPO_CFG(pRepo);
  TSKEY     minKey, midKey, maxKey, now;

  now = taosGetTimestamp(pCfg->precision);
  minKey = now - pCfg->keep * tsTickPerDay[pCfg->precision];
  midKey = now - pCfg->keep2 * tsTickPerDay[pCfg->precision];
  maxKey = now - pCfg->keep1 * tsTickPerDay[pCfg->precision];

  pRtn->minKey = minKey;
  pRtn->minFid = (int)(TSDB_KEY_FID(minKey, pCfg->daysPerFile, pCfg->precision));
  pRtn->midFid = (int)(TSDB_KEY_FID(midKey, pCfg->daysPerFile, pCfg->precision));
  pRtn->maxFid = (int)(TSDB_KEY_FID(maxKey, pCfg->daysPerFile, pCfg->precision));
  tsdbDebug("vgId:%d now:%" PRId64 " minKey:%" PRId64 " minFid:%d, midFid:%d, maxFid:%d", REPO_ID(pRepo), now, minKey,
            pRtn->minFid, pRtn->midFid, pRtn->maxFid);
}

static void tsdbStartCommit(STsdb *pRepo) {
  STsdbMemTable *pMem = pRepo->imem;

  tsdbInfo("vgId:%d start to commit", REPO_ID(pRepo));

  tsdbStartFSTxn(pRepo, 0, 0);
}

static void tsdbEndCommit(STsdb *pTsdb, int eno) {
  tsdbEndFSTxn(pTsdb);
  tsdbFreeMemTable(pTsdb, pTsdb->imem);
  pTsdb->imem = NULL;
  tsdbInfo("vgId:%d commit over, %s", REPO_ID(pTsdb), (eno == TSDB_CODE_SUCCESS) ? "succeed" : "failed");
}

static int tsdbInitCommitH(SCommitH *pCommith, STsdb *pRepo) {
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

  pCommith->pDataCols = tdNewDataCols(0, pCfg->maxRowsPerFileBlock);
  if (pCommith->pDataCols == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyCommitH(pCommith);
    return -1;
  }

  return 0;
}

// Skip all keys until key (not included)
static void tsdbSeekCommitIter(SCommitH *pCommith, TSKEY key) {
  for (int i = 0; i < pCommith->niters; i++) {
    SCommitIter *pIter = pCommith->iters + i;
    if (pIter->pTable == NULL || pIter->pIter == NULL) continue;

    tsdbLoadDataFromCache(pIter->pTable, pIter->pIter, key - 1, INT32_MAX, NULL, NULL, 0, true, NULL);
  }
}

static int tsdbNextCommitFid(SCommitH *pCommith) {
  STsdb    *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg *pCfg = REPO_CFG(pRepo);
  int       fid = TSDB_IVLD_FID;

  for (int i = 0; i < pCommith->niters; i++) {
    SCommitIter *pIter = pCommith->iters + i;
    // if (pIter->pTable == NULL || pIter->pIter == NULL) continue;

    TSKEY nextKey = tsdbNextIterKey(pIter->pIter);
    if (nextKey == TSDB_DATA_TIMESTAMP_NULL) {
      continue;
    } else {
      int tfid = (int)(TSDB_KEY_FID(nextKey, pCfg->daysPerFile, pCfg->precision));
      if (fid == TSDB_IVLD_FID || fid > tfid) {
        fid = tfid;
      }
    }
  }

  return fid;
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

static int tsdbCommitToFile(SCommitH *pCommith, SDFileSet *pSet, int fid) {
  STsdb    *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg *pCfg = REPO_CFG(pRepo);

  ASSERT(pSet == NULL || pSet->fid == fid);

  tsdbResetCommitFile(pCommith);
  tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, fid, &(pCommith->minKey), &(pCommith->maxKey));

  // Set and open files
  if (tsdbSetAndOpenCommitFile(pCommith, pSet, fid) < 0) {
    return -1;
  }

  // Loop to commit each table data
  for (int tid = 0; tid < pCommith->niters; tid++) {
    SCommitIter *pIter = pCommith->iters + tid;

    if (pIter->pTable == NULL) continue;

    if (tsdbCommitToTable(pCommith, tid) < 0) {
      tsdbCloseCommitFile(pCommith, true);
      // revert the file change
      tsdbApplyDFileSetChange(TSDB_COMMIT_WRITE_FSET(pCommith), pSet);
      return -1;
    }
  }

  if (tsdbWriteBlockIdx(TSDB_COMMIT_HEAD_FILE(pCommith), pCommith->aBlkIdx, (void **)(&(TSDB_COMMIT_BUF(pCommith)))) <
      0) {
    tsdbError("vgId:%d failed to write SBlockIdx part to FSET %d since %s", REPO_ID(pRepo), fid, tstrerror(terrno));
    tsdbCloseCommitFile(pCommith, true);
    // revert the file change
    tsdbApplyDFileSetChange(TSDB_COMMIT_WRITE_FSET(pCommith), pSet);
    return -1;
  }

  if (tsdbUpdateDFileSetHeader(&(pCommith->wSet)) < 0) {
    tsdbError("vgId:%d failed to update FSET %d header since %s", REPO_ID(pRepo), fid, tstrerror(terrno));
    tsdbCloseCommitFile(pCommith, true);
    // revert the file change
    tsdbApplyDFileSetChange(TSDB_COMMIT_WRITE_FSET(pCommith), pSet);
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
  STsdb             *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbMemTable     *pMem = pRepo->imem;
  SSkipListIterator *pSlIter;
  SCommitIter       *pCommitIter;
  SSkipListNode     *pNode;
  STbData           *pTbData;

  pCommith->niters = SL_SIZE(pMem->pSlIdx);
  pCommith->iters = (SCommitIter *)taosMemoryCalloc(pCommith->niters, sizeof(SCommitIter));
  if (pCommith->iters == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  // Loop to create iters for each skiplist
  pSlIter = tSkipListCreateIter(pMem->pSlIdx);
  if (pSlIter == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }
  for (int i = 0; i < pCommith->niters; i++) {
    tSkipListIterNext(pSlIter);
    pNode = tSkipListIterGet(pSlIter);
    pTbData = (STbData *)pNode->pData;

    pCommitIter = pCommith->iters + i;
    pCommitIter->pIter = tSkipListCreateIter(pTbData->pData);
    tSkipListIterNext(pCommitIter->pIter);

    pCommitIter->pTable = (STable *)taosMemoryMalloc(sizeof(STable));
    pCommitIter->pTable->uid = pTbData->uid;
    pCommitIter->pTable->tid = pTbData->uid;
    pCommitIter->pTable->pSchema = metaGetTbTSchema(pRepo->pMeta, pTbData->uid, 0);
  }

  return 0;
}

static void tsdbDestroyCommitIters(SCommitH *pCommith) {
  if (pCommith->iters == NULL) return;

  for (int i = 1; i < pCommith->niters; i++) {
    tSkipListDestroyIter(pCommith->iters[i].pIter);
    tdFreeSchema(pCommith->iters[i].pTable->pSchema);
    taosMemoryFree(pCommith->iters[i].pTable);
  }

  taosMemoryFree(pCommith->iters);
  pCommith->iters = NULL;
  pCommith->niters = 0;
}

static void tsdbResetCommitFile(SCommitH *pCommith) {
  pCommith->isRFileSet = false;
  pCommith->isDFileSame = false;
  pCommith->isLFileSame = false;
  taosArrayClear(pCommith->aBlkIdx);
}

static int tsdbSetAndOpenCommitFile(SCommitH *pCommith, SDFileSet *pSet, int fid) {
  SDiskID    did;
  STsdb     *pRepo = TSDB_COMMIT_REPO(pCommith);
  SDFileSet *pWSet = TSDB_COMMIT_WRITE_FSET(pCommith);

  if (tfsAllocDisk(pRepo->pTfs, tsdbGetFidLevel(fid, &(pCommith->rtn)), &did) < 0) {
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

    tsdbDebug("vgId:%d FSET %d at level %d disk id %d is opened to read to commit", REPO_ID(pRepo), TSDB_FSET_FID(pSet),
              TSDB_FSET_LEVEL(pSet), TSDB_FSET_ID(pSet));
  } else {
    pCommith->isRFileSet = false;
  }

  // Set and open commit FSET
  if (pSet == NULL || did.level > TSDB_FSET_LEVEL(pSet)) {
    // Create a new FSET to write data
    tsdbInitDFileSet(pRepo, pWSet, did, fid, FS_TXN_VERSION(REPO_FS(pRepo)));

    if (tsdbCreateDFileSet(pRepo, pWSet, true) < 0) {
      tsdbError("vgId:%d failed to create FSET %d at level %d disk id %d since %s", REPO_ID(pRepo),
                TSDB_FSET_FID(pWSet), TSDB_FSET_LEVEL(pWSet), TSDB_FSET_ID(pWSet), tstrerror(terrno));
      if (pCommith->isRFileSet) {
        tsdbCloseAndUnsetFSet(&(pCommith->readh));
      }
      return -1;
    }

    pCommith->isDFileSame = false;
    pCommith->isLFileSame = false;

    tsdbDebug("vgId:%d FSET %d at level %d disk id %d is created to commit", REPO_ID(pRepo), TSDB_FSET_FID(pWSet),
              TSDB_FSET_LEVEL(pWSet), TSDB_FSET_ID(pWSet));
  } else {
    did.level = TSDB_FSET_LEVEL(pSet);
    did.id = TSDB_FSET_ID(pSet);

    pCommith->wSet.fid = fid;
    pCommith->wSet.state = 0;

    // TSDB_FILE_HEAD
    SDFile *pWHeadf = TSDB_COMMIT_HEAD_FILE(pCommith);
    tsdbInitDFile(pRepo, pWHeadf, did, fid, FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_FILE_HEAD);
    if (tsdbCreateDFile(pRepo, pWHeadf, true, TSDB_FILE_HEAD) < 0) {
      tsdbError("vgId:%d failed to create file %s to commit since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pWHeadf),
                tstrerror(terrno));

      if (pCommith->isRFileSet) {
        tsdbCloseAndUnsetFSet(&(pCommith->readh));
        return -1;
      }
    }

    // TSDB_FILE_DATA
    SDFile *pRDataf = TSDB_READ_DATA_FILE(&(pCommith->readh));
    SDFile *pWDataf = TSDB_COMMIT_DATA_FILE(pCommith);
    tsdbInitDFileEx(pWDataf, pRDataf);
    // if (tsdbOpenDFile(pWDataf, O_WRONLY) < 0) {
    if (tsdbOpenDFile(pWDataf, TD_FILE_WRITE) < 0) {
      tsdbError("vgId:%d failed to open file %s to commit since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pWDataf),
                tstrerror(terrno));

      tsdbCloseDFileSet(pWSet);
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

      // if (tsdbOpenDFile(pWLastf, O_WRONLY) < 0) {
      if (tsdbOpenDFile(pWLastf, TD_FILE_WRITE) < 0) {
        tsdbError("vgId:%d failed to open file %s to commit since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pWLastf),
                  tstrerror(terrno));

        tsdbCloseDFileSet(pWSet);
        tsdbRemoveDFile(pWHeadf);
        if (pCommith->isRFileSet) {
          tsdbCloseAndUnsetFSet(&(pCommith->readh));
          return -1;
        }
      }
    } else {
      tsdbInitDFile(pRepo, pWLastf, did, fid, FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_FILE_LAST);
      pCommith->isLFileSame = false;

      if (tsdbCreateDFile(pRepo, pWLastf, true, TSDB_FILE_LAST) < 0) {
        tsdbError("vgId:%d failed to create file %s to commit since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pWLastf),
                  tstrerror(terrno));

        tsdbCloseDFileSet(pWSet);
        (void)tsdbRemoveDFile(pWHeadf);
        if (pCommith->isRFileSet) {
          tsdbCloseAndUnsetFSet(&(pCommith->readh));
          return -1;
        }
      }
    }

    // TSDB_FILE_SMAD
    SDFile *pRSmadF = TSDB_READ_SMAD_FILE(&(pCommith->readh));
    SDFile *pWSmadF = TSDB_COMMIT_SMAD_FILE(pCommith);

    if (!taosCheckExistFile(TSDB_FILE_FULL_NAME(pRSmadF))) {
      tsdbDebug("vgId:%d create data file %s as not exist", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pRSmadF));
      tsdbInitDFile(pRepo, pWSmadF, did, fid, FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_FILE_SMAD);

      if (tsdbCreateDFile(pRepo, pWSmadF, true, TSDB_FILE_SMAD) < 0) {
        tsdbError("vgId:%d failed to create file %s to commit since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pWSmadF),
                  tstrerror(terrno));

        tsdbCloseDFileSet(pWSet);
        (void)tsdbRemoveDFile(pWHeadf);
        if (pCommith->isRFileSet) {
          tsdbCloseAndUnsetFSet(&(pCommith->readh));
          return -1;
        }
      }
    } else {
      tsdbInitDFileEx(pWSmadF, pRSmadF);
      if (tsdbOpenDFile(pWSmadF, O_RDWR) < 0) {
        tsdbError("vgId:%d failed to open file %s to commit since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pWSmadF),
                  tstrerror(terrno));

        tsdbCloseDFileSet(pWSet);
        tsdbRemoveDFile(pWHeadf);
        if (pCommith->isRFileSet) {
          tsdbCloseAndUnsetFSet(&(pCommith->readh));
          return -1;
        }
      }
    }

    // TSDB_FILE_SMAL
    SDFile *pRSmalF = TSDB_READ_SMAL_FILE(&(pCommith->readh));
    SDFile *pWSmalF = TSDB_COMMIT_SMAL_FILE(pCommith);

    if ((pCommith->isLFileSame) && taosCheckExistFile(TSDB_FILE_FULL_NAME(pRSmalF))) {
      tsdbInitDFileEx(pWSmalF, pRSmalF);
      if (tsdbOpenDFile(pWSmalF, O_RDWR) < 0) {
        tsdbError("vgId:%d failed to open file %s to commit since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pWSmalF),
                  tstrerror(terrno));

        tsdbCloseDFileSet(pWSet);
        tsdbRemoveDFile(pWHeadf);
        if (pCommith->isRFileSet) {
          tsdbCloseAndUnsetFSet(&(pCommith->readh));
          return -1;
        }
      }
    } else {
      tsdbDebug("vgId:%d create data file %s as not exist", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pRSmalF));
      tsdbInitDFile(pRepo, pWSmalF, did, fid, FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_FILE_SMAL);

      if (tsdbCreateDFile(pRepo, pWSmalF, true, TSDB_FILE_SMAL) < 0) {
        tsdbError("vgId:%d failed to create file %s to commit since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pWSmalF),
                  tstrerror(terrno));

        tsdbCloseDFileSet(pWSet);
        (void)tsdbRemoveDFile(pWHeadf);
        if (pCommith->isRFileSet) {
          tsdbCloseAndUnsetFSet(&(pCommith->readh));
          return -1;
        }
      }
    }
  }

  return 0;
}

// extern int32_t tsTsdbMetaCompactRatio;

int tsdbWriteBlockInfoImpl(SDFile *pHeadf, STable *pTable, SArray *pSupA, SArray *pSubA, void **ppBuf,
                           SBlockIdx *pIdx) {
  size_t      nSupBlocks;
  size_t      nSubBlocks;
  uint32_t    tlen;
  SBlockInfo *pBlkInfo;
  int64_t     offset;
  SBlock     *pBlock;

  memset(pIdx, 0, sizeof(*pIdx));

  nSupBlocks = taosArrayGetSize(pSupA);
  nSubBlocks = (pSubA == NULL) ? 0 : taosArrayGetSize(pSubA);

  if (nSupBlocks <= 0) {
    // No data (data all deleted)
    return 0;
  }

  tlen = (uint32_t)(sizeof(SBlockInfo) + sizeof(SBlock) * (nSupBlocks + nSubBlocks) + sizeof(TSCKSUM));
  if (tsdbMakeRoom(ppBuf, tlen) < 0) return -1;
  pBlkInfo = *ppBuf;

  pBlkInfo->delimiter = TSDB_FILE_DELIMITER;
  pBlkInfo->tid = TABLE_TID(pTable);
  pBlkInfo->uid = TABLE_UID(pTable);

  memcpy((void *)(pBlkInfo->blocks), taosArrayGet(pSupA, 0), nSupBlocks * sizeof(SBlock));
  if (nSubBlocks > 0) {
    memcpy((void *)(pBlkInfo->blocks + nSupBlocks), taosArrayGet(pSubA, 0), nSubBlocks * sizeof(SBlock));

    for (int i = 0; i < nSupBlocks; i++) {
      pBlock = pBlkInfo->blocks + i;

      if (pBlock->numOfSubBlocks > 1) {
        pBlock->offset += (sizeof(SBlockInfo) + sizeof(SBlock) * nSupBlocks);
      }
    }
  }

  taosCalcChecksumAppend(0, (uint8_t *)pBlkInfo, tlen);

  if (tsdbAppendDFile(pHeadf, (void *)pBlkInfo, tlen, &offset) < 0) {
    return -1;
  }

  tsdbUpdateDFileMagic(pHeadf, POINTER_SHIFT(pBlkInfo, tlen - sizeof(TSCKSUM)));

  // Set pIdx
  pBlock = taosArrayGetLast(pSupA);

  pIdx->tid = TABLE_TID(pTable);
  pIdx->uid = TABLE_UID(pTable);
  pIdx->hasLast = pBlock->last ? 1 : 0;
  pIdx->maxKey = pBlock->keyLast;
  pIdx->numOfBlocks = (uint32_t)nSupBlocks;
  pIdx->len = tlen;
  pIdx->offset = (uint32_t)offset;

  return 0;
}

int tsdbWriteBlockIdx(SDFile *pHeadf, SArray *pIdxA, void **ppBuf) {
  SBlockIdx *pBlkIdx;
  size_t     nidx = taosArrayGetSize(pIdxA);
  int        tlen = 0, size;
  int64_t    offset;

  if (nidx <= 0) {
    // All data are deleted
    pHeadf->info.offset = 0;
    pHeadf->info.len = 0;
    return 0;
  }

  for (size_t i = 0; i < nidx; i++) {
    pBlkIdx = (SBlockIdx *)taosArrayGet(pIdxA, i);

    size = tsdbEncodeSBlockIdx(NULL, pBlkIdx);
    if (tsdbMakeRoom(ppBuf, tlen + size) < 0) return -1;

    void *ptr = POINTER_SHIFT(*ppBuf, tlen);
    tsdbEncodeSBlockIdx(&ptr, pBlkIdx);

    tlen += size;
  }

  tlen += sizeof(TSCKSUM);
  if (tsdbMakeRoom(ppBuf, tlen) < 0) return -1;
  taosCalcChecksumAppend(0, (uint8_t *)(*ppBuf), tlen);

  if (tsdbAppendDFile(pHeadf, *ppBuf, tlen, &offset) < tlen) {
    return -1;
  }

  tsdbUpdateDFileMagic(pHeadf, POINTER_SHIFT(*ppBuf, tlen - sizeof(TSCKSUM)));
  pHeadf->info.offset = (uint32_t)offset;
  pHeadf->info.len = tlen;

  return 0;
}

// // =================== Commit Meta Data
// static int tsdbInitCommitMetaFile(STsdbRepo *pRepo, SMFile* pMf, bool open) {
//   STsdbFS *  pfs = REPO_FS(pRepo);
//   SMFile *   pOMFile = pfs->cstatus->pmf;
//   SDiskID    did;

//   // Create/Open a meta file or open the existing file
//   if (pOMFile == NULL) {
//     // Create a new meta file
//     did.level = TFS_PRIMARY_LEVEL;
//     did.id = TFS_PRIMARY_ID;
//     tsdbInitMFile(pMf, did, REPO_ID(pRepo), FS_TXN_VERSION(REPO_FS(pRepo)));

//     if (open && tsdbCreateMFile(pMf, true) < 0) {
//       tsdbError("vgId:%d failed to create META file since %s", REPO_ID(pRepo), tstrerror(terrno));
//       return -1;
//     }

//     tsdbInfo("vgId:%d meta file %s is created to commit", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pMf));
//   } else {
//     tsdbInitMFileEx(pMf, pOMFile);
//     if (open && tsdbOpenMFile(pMf, O_WRONLY) < 0) {
//       tsdbError("vgId:%d failed to open META file since %s", REPO_ID(pRepo), tstrerror(terrno));
//       return -1;
//     }
//   }

//   return 0;
// }

// static int tsdbCommitMeta(STsdbRepo *pRepo) {
//   STsdbFS *  pfs = REPO_FS(pRepo);
//   SMemTable *pMem = pRepo->imem;
//   SMFile *   pOMFile = pfs->cstatus->pmf;
//   SMFile     mf;
//   SActObj *  pAct = NULL;
//   SActCont * pCont = NULL;
//   SListNode *pNode = NULL;

//   ASSERT(pOMFile != NULL || listNEles(pMem->actList) > 0);

//   if (listNEles(pMem->actList) <= 0) {
//     // no meta data to commit, just keep the old meta file
//     tsdbUpdateMFile(pfs, pOMFile);
//     if (tsTsdbMetaCompactRatio > 0) {
//       if (tsdbInitCommitMetaFile(pRepo, &mf, false) < 0) {
//         return -1;
//       }
//       int ret = tsdbCompactMetaFile(pRepo, pfs, &mf);
//       if (ret < 0) tsdbError("compact meta file error");

//       return ret;
//     }
//     return 0;
//   } else {
//     if (tsdbInitCommitMetaFile(pRepo, &mf, true) < 0) {
//       return -1;
//     }
//   }

//   // Loop to write
//   while ((pNode = tdListPopHead(pMem->actList)) != NULL) {
//     pAct = (SActObj *)pNode->data;
//     if (pAct->act == TSDB_UPDATE_META) {
//       pCont = (SActCont *)POINTER_SHIFT(pAct, sizeof(SActObj));
//       if (tsdbUpdateMetaRecord(pfs, &mf, pAct->uid, (void *)(pCont->cont), pCont->len, false) < 0) {
//         tsdbError("vgId:%d failed to update META record, uid %" PRIu64 " since %s", REPO_ID(pRepo), pAct->uid,
//                   tstrerror(terrno));
//         tsdbCloseMFile(&mf);
//         (void)tsdbApplyMFileChange(&mf, pOMFile);
//         // TODO: need to reload metaCache
//         return -1;
//       }
//     } else if (pAct->act == TSDB_DROP_META) {
//       if (tsdbDropMetaRecord(pfs, &mf, pAct->uid) < 0) {
//         tsdbError("vgId:%d failed to drop META record, uid %" PRIu64 " since %s", REPO_ID(pRepo), pAct->uid,
//                   tstrerror(terrno));
//         tsdbCloseMFile(&mf);
//         tsdbApplyMFileChange(&mf, pOMFile);
//         // TODO: need to reload metaCache
//         return -1;
//       }
//     } else {
//       ASSERT(false);
//     }
//   }

//   if (tsdbUpdateMFileHeader(&mf) < 0) {
//     tsdbError("vgId:%d failed to update META file header since %s, revert it", REPO_ID(pRepo), tstrerror(terrno));
//     tsdbApplyMFileChange(&mf, pOMFile);
//     // TODO: need to reload metaCache
//     return -1;
//   }

//   TSDB_FILE_FSYNC(&mf);
//   tsdbCloseMFile(&mf);
//   tsdbUpdateMFile(pfs, &mf);

//   if (tsTsdbMetaCompactRatio > 0 && tsdbCompactMetaFile(pRepo, pfs, &mf) < 0) {
//     tsdbError("compact meta file error");
//   }

//   return 0;
// }

// int tsdbEncodeKVRecord(void **buf, SKVRecord *pRecord) {
//   int tlen = 0;
//   tlen += taosEncodeFixedU64(buf, pRecord->uid);
//   tlen += taosEncodeFixedI64(buf, pRecord->offset);
//   tlen += taosEncodeFixedI64(buf, pRecord->size);

//   return tlen;
// }

// void *tsdbDecodeKVRecord(void *buf, SKVRecord *pRecord) {
//   buf = taosDecodeFixedU64(buf, &(pRecord->uid));
//   buf = taosDecodeFixedI64(buf, &(pRecord->offset));
//   buf = taosDecodeFixedI64(buf, &(pRecord->size));

//   return buf;
// }

// static int tsdbUpdateMetaRecord(STsdbFS *pfs, SMFile *pMFile, uint64_t uid, void *cont, int contLen, bool compact) {
//   char      buf[64] = "\0";
//   void *    pBuf = buf;
//   SKVRecord rInfo;
//   int64_t   offset;

//   // Seek to end of meta file
//   offset = tsdbSeekMFile(pMFile, 0, SEEK_END);
//   if (offset < 0) {
//     return -1;
//   }

//   rInfo.offset = offset;
//   rInfo.uid = uid;
//   rInfo.size = contLen;

//   int tlen = tsdbEncodeKVRecord((void **)(&pBuf), &rInfo);
//   if (tsdbAppendMFile(pMFile, buf, tlen, NULL) < tlen) {
//     return -1;
//   }

//   if (tsdbAppendMFile(pMFile, cont, contLen, NULL) < contLen) {
//     return -1;
//   }

//   tsdbUpdateMFileMagic(pMFile, POINTER_SHIFT(cont, contLen - sizeof(TSCKSUM)));

//   SHashObj* cache = compact ? pfs->metaCacheComp : pfs->metaCache;

//   pMFile->info.nRecords++;

//   SKVRecord *pRecord = taosHashGet(cache, (void *)&uid, sizeof(uid));
//   if (pRecord != NULL) {
//     pMFile->info.tombSize += (pRecord->size + sizeof(SKVRecord));
//   } else {
//     pMFile->info.nRecords++;
//   }
//   taosHashPut(cache, (void *)(&uid), sizeof(uid), (void *)(&rInfo), sizeof(rInfo));

//   return 0;
// }

// static int tsdbDropMetaRecord(STsdbFS *pfs, SMFile *pMFile, uint64_t uid) {
//   SKVRecord rInfo = {0};
//   char      buf[128] = "\0";

//   SKVRecord *pRecord = taosHashGet(pfs->metaCache, (void *)(&uid), sizeof(uid));
//   if (pRecord == NULL) {
//     tsdbError("failed to drop META record with key %" PRIu64 " since not find", uid);
//     return -1;
//   }

//   rInfo.offset = -pRecord->offset;
//   rInfo.uid = pRecord->uid;
//   rInfo.size = pRecord->size;

//   void *pBuf = buf;
//   tsdbEncodeKVRecord(&pBuf, &rInfo);

//   if (tsdbAppendMFile(pMFile, buf, sizeof(SKVRecord), NULL) < 0) {
//     return -1;
//   }

//   pMFile->info.magic = taosCalcChecksum(pMFile->info.magic, (uint8_t *)buf, sizeof(SKVRecord));
//   pMFile->info.nDels++;
//   pMFile->info.nRecords--;
//   pMFile->info.tombSize += (rInfo.size + sizeof(SKVRecord) * 2);

//   taosHashRemove(pfs->metaCache, (void *)(&uid), sizeof(uid));
//   return 0;
// }

// static int tsdbCompactMetaFile(STsdbRepo *pRepo, STsdbFS *pfs, SMFile *pMFile) {
//   float delPercent = (float)(pMFile->info.nDels) / (float)(pMFile->info.nRecords);
//   float tombPercent = (float)(pMFile->info.tombSize) / (float)(pMFile->info.size);
//   float compactRatio = (float)(tsTsdbMetaCompactRatio)/100;

//   if (delPercent < compactRatio && tombPercent < compactRatio) {
//     return 0;
//   }

//   if (tsdbOpenMFile(pMFile, O_RDONLY) < 0) {
//     tsdbError("open meta file %s compact fail", pMFile->f.rname);
//     return -1;
//   }

//   tsdbInfo("begin compact tsdb meta file, ratio:%d, nDels:%" PRId64 ",nRecords:%" PRId64 ",tombSize:%" PRId64
//   ",size:%" PRId64,
//     tsTsdbMetaCompactRatio, pMFile->info.nDels,pMFile->info.nRecords,pMFile->info.tombSize,pMFile->info.size);

//   SMFile mf;
//   SDiskID did;

//   // first create tmp meta file
//   did.level = TFS_PRIMARY_LEVEL;
//   did.id = TFS_PRIMARY_ID;
//   tsdbInitMFile(&mf, did, REPO_ID(pRepo), FS_TXN_VERSION(REPO_FS(pRepo)) + 1);

//   if (tsdbCreateMFile(&mf, true) < 0) {
//     tsdbError("vgId:%d failed to create META file since %s", REPO_ID(pRepo), tstrerror(terrno));
//     return -1;
//   }

//   tsdbInfo("vgId:%d meta file %s is created to compact meta data", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(&mf));

//   // second iterator metaCache
//   int code = -1;
//   int64_t maxBufSize = 1024;
//   SKVRecord *pRecord;
//   void *pBuf = NULL;

//   pBuf = taosMemoryMalloc((size_t)maxBufSize);
//   if (pBuf == NULL) {
//     goto _err;
//   }

//   // init Comp
//   assert(pfs->metaCacheComp == NULL);
//   pfs->metaCacheComp = taosHashInit(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
//   if (pfs->metaCacheComp == NULL) {
//     goto _err;
//   }

//   pRecord = taosHashIterate(pfs->metaCache, NULL);
//   while (pRecord) {
//     if (tsdbSeekMFile(pMFile, pRecord->offset + sizeof(SKVRecord), SEEK_SET) < 0) {
//       tsdbError("vgId:%d failed to seek file %s since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pMFile),
//                 tstrerror(terrno));
//       goto _err;
//     }
//     if (pRecord->size > maxBufSize) {
//       maxBufSize = pRecord->size;
//       void* tmp = taosMemoryRealloc(pBuf, (size_t)maxBufSize);
//       if (tmp == NULL) {
//         goto _err;
//       }
//       pBuf = tmp;
//     }
//     int nread = (int)tsdbReadMFile(pMFile, pBuf, pRecord->size);
//     if (nread < 0) {
//       tsdbError("vgId:%d failed to read file %s since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pMFile),
//         tstrerror(terrno));
//       goto _err;
//     }

//     if (nread < pRecord->size) {
//       tsdbError("vgId:%d failed to read file %s since file corrupted, expected read:%" PRId64 " actual read:%d",
//                 REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pMFile), pRecord->size, nread);
//       goto _err;
//     }

//     if (tsdbUpdateMetaRecord(pfs, &mf, pRecord->uid, pBuf, (int)pRecord->size, true) < 0) {
//       tsdbError("vgId:%d failed to update META record, uid %" PRIu64 " since %s", REPO_ID(pRepo), pRecord->uid,
//                 tstrerror(terrno));
//       goto _err;
//     }

//     pRecord = taosHashIterate(pfs->metaCache, pRecord);
//   }
//   code = 0;

// _err:
//   if (code == 0) TSDB_FILE_FSYNC(&mf);
//   tsdbCloseMFile(&mf);
//   tsdbCloseMFile(pMFile);

//   if (code == 0) {
//     // rename meta.tmp -> meta
//     tsdbInfo("vgId:%d meta file rename %s -> %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(&mf),
//     TSDB_FILE_FULL_NAME(pMFile)); taosRename(mf.f.aname,pMFile->f.aname); tstrncpy(mf.f.aname, pMFile->f.aname,
//     TSDB_FILENAME_LEN); tstrncpy(mf.f.rname, pMFile->f.rname, TSDB_FILENAME_LEN);
//     // update current meta file info
//     pfs->nstatus->pmf = NULL;
//     tsdbUpdateMFile(pfs, &mf);

//     taosHashCleanup(pfs->metaCache);
//     pfs->metaCache = pfs->metaCacheComp;
//     pfs->metaCacheComp = NULL;
//   } else {
//     // remove meta.tmp file
//     taosRemoveFile(mf.f.aname);
//     taosHashCleanup(pfs->metaCacheComp);
//     pfs->metaCacheComp = NULL;
//   }

//   taosMemoryFreeClear(pBuf);

//   ASSERT(mf.info.nDels == 0);
//   ASSERT(mf.info.tombSize == 0);

//   tsdbInfo("end compact tsdb meta file,code:%d,nRecords:%" PRId64 ",size:%" PRId64,
//     code,mf.info.nRecords,mf.info.size);
//   return code;
// }

// // =================== Commit Time-Series Data
// #if 0
// static bool tsdbHasDataToCommit(SCommitIter *iters, int nIters, TSKEY minKey, TSKEY maxKey) {
//   for (int i = 0; i < nIters; i++) {
//     TSKEY nextKey = tsdbNextIterKey((iters + i)->pIter);
//     if (nextKey != TSDB_DATA_TIMESTAMP_NULL && (nextKey >= minKey && nextKey <= maxKey)) return true;
//   }
//   return false;
// }
// #endif

static int tsdbCommitToTable(SCommitH *pCommith, int tid) {
  SCommitIter *pIter = pCommith->iters + tid;
  TSKEY        nextKey = tsdbNextIterKey(pIter->pIter);

  tsdbResetCommitTable(pCommith);

  // Set commit table
  if (tsdbSetCommitTable(pCommith, pIter->pTable) < 0) {
    return -1;
  }

  // No disk data and no memory data, just return
  if (pCommith->readh.pBlkIdx == NULL && (nextKey == TSDB_DATA_TIMESTAMP_NULL || nextKey > pCommith->maxKey)) {
    return 0;
  }

  // Must has disk data or has memory data
  int     nBlocks;
  int     bidx = 0;
  SBlock *pBlock;

  if (pCommith->readh.pBlkIdx) {
    if (tsdbLoadBlockInfo(&(pCommith->readh), NULL) < 0) {
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
          return -1;
        }
      } else {
        if (tsdbCommitMemData(pCommith, pIter, pBlock->keyFirst - 1, true) < 0) {
          return -1;
        }
      }
      nextKey = tsdbNextIterKey(pIter->pIter);
    }
  }

  if (tsdbWriteBlockInfo(pCommith) < 0) {
    tsdbError("vgId:%d failed to write SBlockInfo part into file %s since %s", TSDB_COMMIT_REPO_ID(pCommith),
              TSDB_FILE_FULL_NAME(TSDB_COMMIT_HEAD_FILE(pCommith)), tstrerror(terrno));
    return -1;
  }

  return 0;
}

static int tsdbSetCommitTable(SCommitH *pCommith, STable *pTable) {
  STSchema *pSchema = tsdbGetTableSchemaImpl(pTable, false, false, -1);

  pCommith->pTable = pTable;

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

int tsdbWriteBlockImpl(STsdb *pRepo, STable *pTable, SDFile *pDFile, SDFile *pDFileAggr, SDataCols *pDataCols,
                       SBlock *pBlock, bool isLast, bool isSuper, void **ppBuf, void **ppCBuf, void **ppExBuf) {
  STsdbCfg *    pCfg = REPO_CFG(pRepo);
  SBlockData *  pBlockData = NULL;
  SAggrBlkData *pAggrBlkData = NULL;
  int64_t       offset = 0, offsetAggr = 0;
  int           rowsToWrite = pDataCols->numOfRows;

  ASSERT(rowsToWrite > 0 && rowsToWrite <= pCfg->maxRowsPerFileBlock);
  ASSERT((!isLast) || rowsToWrite < pCfg->minRowsPerFileBlock);

  // Make buffer space
  if (tsdbMakeRoom(ppBuf, tsdbBlockStatisSize(pDataCols->numOfCols, SBlockVerLatest)) < 0) {
    return -1;
  }
  pBlockData = (SBlockData *)(*ppBuf);

  if (tsdbMakeRoom(ppExBuf, tsdbBlockAggrSize(pDataCols->numOfCols, SBlockVerLatest)) < 0) {
    return -1;
  }
  pAggrBlkData = (SAggrBlkData *)(*ppExBuf);

  // Get # of cols not all NULL(not including key column)
  int nColsNotAllNull = 0;
  for (int ncol = 1; ncol < pDataCols->numOfCols; ++ncol) {  // ncol from 1, we skip the timestamp column
    SDataCol *   pDataCol = pDataCols->cols + ncol;
    SBlockCol *  pBlockCol = pBlockData->cols + nColsNotAllNull;
    SAggrBlkCol *pAggrBlkCol = (SAggrBlkCol *)pAggrBlkData + nColsNotAllNull;

    if (isAllRowsNull(pDataCol)) {  // all data to commit are NULL, just ignore it
      continue;
    }

    memset(pBlockCol, 0, sizeof(*pBlockCol));
    memset(pAggrBlkCol, 0, sizeof(*pAggrBlkCol));

    pBlockCol->colId = pDataCol->colId;
    pBlockCol->type = pDataCol->type;
    pAggrBlkCol->colId = pDataCol->colId;

    if (tDataTypes[pDataCol->type].statisFunc) {
#if 0
      (*tDataTypes[pDataCol->type].statisFunc)(pDataCol->pData, rowsToWrite, &(pBlockCol->min), &(pBlockCol->max),
                                               &(pBlockCol->sum), &(pBlockCol->minIndex), &(pBlockCol->maxIndex),
                                               &(pBlockCol->numOfNull));
#endif
      (*tDataTypes[pDataCol->type].statisFunc)(pDataCol->pData, rowsToWrite, &(pAggrBlkCol->min), &(pAggrBlkCol->max),
                                               &(pAggrBlkCol->sum), &(pAggrBlkCol->minIndex), &(pAggrBlkCol->maxIndex),
                                               &(pAggrBlkCol->numOfNull));

      if (pAggrBlkCol->numOfNull == 0) {
        TD_SET_COL_ROWS_NORM(pBlockCol);
      } else {
        TD_SET_COL_ROWS_MISC(pBlockCol);
      }
    } else {
      TD_SET_COL_ROWS_MISC(pBlockCol);
    }
    nColsNotAllNull++;
  }

  ASSERT(nColsNotAllNull >= 0 && nColsNotAllNull <= pDataCols->numOfCols);

  // Compress the data if neccessary
  int      tcol = 0;  // counter of not all NULL and written columns
  uint32_t toffset = 0;
  int32_t  tsize = (int32_t)tsdbBlockStatisSize(nColsNotAllNull, SBlockVerLatest);
  int32_t  lsize = tsize;
  uint32_t tsizeAggr = (uint32_t)tsdbBlockAggrSize(nColsNotAllNull, SBlockVerLatest);
  int32_t  keyLen = 0;
  int32_t  nBitmaps = (int32_t)TD_BITMAP_BYTES(rowsToWrite);
  int32_t  tBitmaps = 0;

  for (int ncol = 0; ncol < pDataCols->numOfCols; ++ncol) {
    // All not NULL columns finish
    if (ncol != 0 && tcol >= nColsNotAllNull) break;

    SDataCol  *pDataCol = pDataCols->cols + ncol;
    SBlockCol *pBlockCol = pBlockData->cols + tcol;

    if (ncol != 0 && (pDataCol->colId != pBlockCol->colId)) continue;

    int32_t flen;  // final length
    int32_t tlen = dataColGetNEleLen(pDataCol, rowsToWrite);

#ifdef TD_SUPPORT_BITMAP
    int32_t tBitmaps = 0;
    if ((ncol != 0) && !TD_COL_ROWS_NORM(pBlockCol)) {
      if (IS_VAR_DATA_TYPE(pDataCol->type)) {
        tBitmaps = nBitmaps;
        tlen += tBitmaps;
      } else {
        tBitmaps = (int32_t)ceil((double)nBitmaps / TYPE_BYTES[pDataCol->type]);
        tlen += tBitmaps * TYPE_BYTES[pDataCol->type];
      }
      // move bitmap parts ahead
      // TODO: put bitmap part to the 1st location(pBitmap points to pData) to avoid the memmove
      memcpy(POINTER_SHIFT(pDataCol->pData, pDataCol->len), pDataCol->pBitmap, nBitmaps);
    }
#endif

    void *tptr;

    // Make room
    if (tsdbMakeRoom(ppBuf, lsize + tlen + COMP_OVERFLOW_BYTES + sizeof(TSCKSUM)) < 0) {
      return -1;
    }
    pBlockData = (SBlockData *)(*ppBuf);
    pBlockCol = pBlockData->cols + tcol;
    tptr = POINTER_SHIFT(pBlockData, lsize);

    if (pCfg->compression == TWO_STAGE_COMP && tsdbMakeRoom(ppCBuf, tlen + COMP_OVERFLOW_BYTES) < 0) {
      return -1;
    }

    // Compress or just copy
    if (pCfg->compression) {
      flen = (*(tDataTypes[pDataCol->type].compFunc))((char *)pDataCol->pData, tlen, rowsToWrite + tBitmaps, tptr,
                                                      tlen + COMP_OVERFLOW_BYTES, pCfg->compression, *ppCBuf,
                                                      tlen + COMP_OVERFLOW_BYTES);
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
      tsdbSetBlockColOffset(pBlockCol, toffset);
      pBlockCol->len = flen;
      ++tcol;
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
  if (tsdbAppendDFile(pDFile, (void *)pBlockData, lsize, &offset) < lsize) {
    return -1;
  }

  uint32_t aggrStatus = nColsNotAllNull > 0 ? 1 : 0;
  if (aggrStatus > 0) {

    taosCalcChecksumAppend(0, (uint8_t *)pAggrBlkData, tsizeAggr);
    tsdbUpdateDFileMagic(pDFileAggr, POINTER_SHIFT(pAggrBlkData, tsizeAggr - sizeof(TSCKSUM)));

    // Write the whole block to file
    if (tsdbAppendDFile(pDFileAggr, (void *)pAggrBlkData, tsizeAggr, &offsetAggr) < tsizeAggr) {
      return -1;
    }
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
  pBlock->aggrStat = aggrStatus;
  pBlock->blkVer = SBlockVerLatest;
  pBlock->aggrOffset = (uint64_t)offsetAggr;

  tsdbDebug("vgId:%d uid:%" PRId64 " a block of data is written to file %s, offset %" PRId64
            " numOfRows %d len %d numOfCols %" PRId16 " keyFirst %" PRId64 " keyLast %" PRId64,
            REPO_ID(pRepo), TABLE_TID(pTable), TSDB_FILE_FULL_NAME(pDFile), offset, rowsToWrite, pBlock->len,
            pBlock->numOfCols, pBlock->keyFirst, pBlock->keyLast);

  return 0;
}

static int tsdbWriteBlock(SCommitH *pCommith, SDFile *pDFile, SDataCols *pDataCols, SBlock *pBlock, bool isLast,
                          bool isSuper) {
  return tsdbWriteBlockImpl(TSDB_COMMIT_REPO(pCommith), TSDB_COMMIT_TABLE(pCommith), pDFile,
                            isLast ? TSDB_COMMIT_SMAL_FILE(pCommith) : TSDB_COMMIT_SMAD_FILE(pCommith), pDataCols,
                            pBlock, isLast, isSuper, (void **)(&(TSDB_COMMIT_BUF(pCommith))),
                            (void **)(&(TSDB_COMMIT_COMP_BUF(pCommith))), (void **)(&(TSDB_COMMIT_EXBUF(pCommith))));
}

static int tsdbWriteBlockInfo(SCommitH *pCommih) {
  SDFile   *pHeadf = TSDB_COMMIT_HEAD_FILE(pCommih);
  SBlockIdx blkIdx;
  STable   *pTable = TSDB_COMMIT_TABLE(pCommih);

  if (tsdbWriteBlockInfoImpl(pHeadf, pTable, pCommih->aSupBlk, pCommih->aSubBlk, (void **)(&(TSDB_COMMIT_BUF(pCommih))),
                             &blkIdx) < 0) {
    return -1;
  }

  if (blkIdx.numOfBlocks == 0) {
    return 0;
  }

  if (taosArrayPush(pCommih->aBlkIdx, (void *)(&blkIdx)) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int tsdbCommitMemData(SCommitH *pCommith, SCommitIter *pIter, TSKEY keyLimit, bool toData) {
  STsdb     *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg  *pCfg = REPO_CFG(pRepo);
  SMergeInfo mInfo;
  int32_t    defaultRows = TSDB_COMMIT_DEFAULT_ROWS(pCommith);
  SDFile    *pDFile;
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

    if (tsdbCommitAddBlock(pCommith, &block, NULL, 0) < 0) {
      return -1;
    }
  }

  return 0;
}

static int tsdbMergeMemData(SCommitH *pCommith, SCommitIter *pIter, int bidx) {
  STsdb     *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg  *pCfg = REPO_CFG(pRepo);
  int        nBlocks = pCommith->readh.pBlkIdx->numOfBlocks;
  SBlock    *pBlock = pCommith->readh.pBlkInfo->blocks + bidx;
  TSKEY      keyLimit;
  int16_t    colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  SMergeInfo mInfo;
  SBlock     subBlocks[TSDB_MAX_SUBBLOCKS];
  SBlock     block, supBlock;
  SDFile    *pDFile;

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

    if (tsdbCommitAddBlock(pCommith, &supBlock, subBlocks, supBlock.numOfSubBlocks) < 0) return -1;
  } else {
    if (tsdbLoadBlockData(&(pCommith->readh), pBlock, NULL) < 0) return -1;
    if (tsdbMergeBlockData(pCommith, pIter, pCommith->readh.pDCols[0], keyLimit, bidx == (nBlocks - 1)) < 0) return -1;
  }

  return 0;
}

static int tsdbMoveBlock(SCommitH *pCommith, int bidx) {
  SBlock *pBlock = pCommith->readh.pBlkInfo->blocks + bidx;
  SDFile *pDFile;
  SBlock  block;
  bool    isSameFile;

  ASSERT(pBlock->numOfSubBlocks > 0);

  if (pBlock->last) {
    pDFile = TSDB_COMMIT_LAST_FILE(pCommith);
    isSameFile = pCommith->isLFileSame;
  } else {
    pDFile = TSDB_COMMIT_DATA_FILE(pCommith);
    isSameFile = pCommith->isDFileSame;
  }

  if (isSameFile) {
    if (pBlock->numOfSubBlocks == 1) {
      if (tsdbCommitAddBlock(pCommith, pBlock, NULL, 0) < 0) {
        return -1;
      }
    } else {
      block = *pBlock;
      block.offset = sizeof(SBlock) * taosArrayGetSize(pCommith->aSubBlk);

      if (tsdbCommitAddBlock(pCommith, &block, POINTER_SHIFT(pCommith->readh.pBlkInfo, pBlock->offset),
                             pBlock->numOfSubBlocks) < 0) {
        return -1;
      }
    }
  } else {
    if (tsdbLoadBlockData(&(pCommith->readh), pBlock, NULL) < 0) return -1;
    if (tsdbWriteBlock(pCommith, pDFile, pCommith->readh.pDCols[0], &block, pBlock->last, true) < 0) return -1;
    if (tsdbCommitAddBlock(pCommith, &block, NULL, 0) < 0) return -1;
  }

  return 0;
}

static int tsdbCommitAddBlock(SCommitH *pCommith, const SBlock *pSupBlock, const SBlock *pSubBlocks, int nSubBlocks) {
  if (taosArrayPush(pCommith->aSupBlk, pSupBlock) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (pSubBlocks && taosArrayAddBatch(pCommith->aSubBlk, pSubBlocks, nSubBlocks) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int tsdbMergeBlockData(SCommitH *pCommith, SCommitIter *pIter, SDataCols *pDataCols, TSKEY keyLimit,
                              bool isLastOneBlock) {
  STsdb    *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg *pCfg = REPO_CFG(pRepo);
  SBlock    block;
  SDFile   *pDFile;
  bool      isLast;
  int32_t   defaultRows = TSDB_COMMIT_DEFAULT_ROWS(pCommith);

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
    STSRow *row = tsdbNextIterRow(pCommitIter->pIter);
    if (row == NULL || TD_ROW_KEY(row) > maxKey) {
      key2 = INT64_MAX;
    } else {
      key2 = TD_ROW_KEY(row);
    }

    if (key1 == INT64_MAX && key2 == INT64_MAX) break;

    if (key1 < key2) {
      for (int i = 0; i < pDataCols->numOfCols; i++) {
        // TODO: dataColAppendVal may fail
        SCellVal sVal = {0};
        if (tdGetColDataOfRow(&sVal, pDataCols->cols + i, *iter) < 0) {
          TASSERT(0);
        }
        tdAppendValToDataCol(pTarget->cols + i, sVal.valType, sVal.val, pTarget->numOfRows, pTarget->maxPoints);
      }

      pTarget->numOfRows++;
      (*iter)++;
    } else if (key1 > key2) {
      if (pSchema == NULL || schemaVersion(pSchema) != TD_ROW_SVER(row)) {
        pSchema = tsdbGetTableSchemaImpl(pCommitIter->pTable, false, false, TD_ROW_SVER(row));
        ASSERT(pSchema != NULL);
      }

      tdAppendSTSRowToDataCol(row, pSchema, pTarget, true);

      tSkipListIterNext(pCommitIter->pIter);
    } else {
      if (update != TD_ROW_OVERWRITE_UPDATE) {
        // copy disk data
        for (int i = 0; i < pDataCols->numOfCols; i++) {
          // TODO: dataColAppendVal may fail
          SCellVal sVal = {0};
          if (tdGetColDataOfRow(&sVal, pDataCols->cols + i, *iter) < 0) {
            TASSERT(0);
          }
          tdAppendValToDataCol(pTarget->cols + i, sVal.valType, sVal.val, pTarget->numOfRows, pTarget->maxPoints);
        }

        if (update == TD_ROW_DISCARD_UPDATE) pTarget->numOfRows++;
      }
      if (update != TD_ROW_DISCARD_UPDATE) {
        // copy mem data
        if (pSchema == NULL || schemaVersion(pSchema) != TD_ROW_SVER(row)) {
          pSchema = tsdbGetTableSchemaImpl(pCommitIter->pTable, false, false, TD_ROW_SVER(row));
          ASSERT(pSchema != NULL);
        }

        tdAppendSTSRowToDataCol(row, pSchema, pTarget, update == TD_ROW_OVERWRITE_UPDATE);
      }
      (*iter)++;
      tSkipListIterNext(pCommitIter->pIter);
    }

    if (pTarget->numOfRows >= maxRows) break;
  }
}

static void tsdbResetCommitTable(SCommitH *pCommith) {
  taosArrayClear(pCommith->aSubBlk);
  taosArrayClear(pCommith->aSupBlk);
  pCommith->pTable = NULL;
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
  STsdb    *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg *pCfg = REPO_CFG(pRepo);
  int       mergeRows = pBlock->numOfRows + pInfo->rowsInserted - pInfo->rowsDeleteSucceed;

  ASSERT(mergeRows > 0);

  if (pBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS && pInfo->nOperations <= pCfg->maxRowsPerFileBlock) {
    if (pBlock->last) {
      if (pCommith->isLFileSame && mergeRows < pCfg->minRowsPerFileBlock) return true;
    } else {
      if (pCommith->isDFileSame && mergeRows <= pCfg->maxRowsPerFileBlock) return true;
    }
  }

  return false;
}

// int tsdbApplyRtn(STsdbRepo *pRepo) {
//   SRtn       rtn;
//   SFSIter    fsiter;
//   STsdbFS *  pfs = REPO_FS(pRepo);
//   SDFileSet *pSet;

//   // Get retention snapshot
//   tsdbGetRtnSnap(pRepo, &rtn);

//   tsdbFSIterInit(&fsiter, pfs, TSDB_FS_ITER_FORWARD);
//   while ((pSet = tsdbFSIterNext(&fsiter))) {
//     if (pSet->fid < rtn.minFid) {
//       tsdbInfo("vgId:%d FSET %d at level %d disk id %d expires, remove it", REPO_ID(pRepo), pSet->fid,
//                TSDB_FSET_LEVEL(pSet), TSDB_FSET_ID(pSet));
//       continue;
//     }

//     if (tsdbApplyRtnOnFSet(pRepo, pSet, &rtn) < 0) {
//       return -1;
//     }
//   }

//   return 0;
// }
