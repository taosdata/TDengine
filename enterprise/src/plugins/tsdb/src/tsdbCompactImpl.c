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

typedef struct {
  STable *    pTable;
  SBlockIdx * pBlkIdx;
  SBlockIdx   bindex;
  SBlockInfo *pInfo;
} STableCompactH;

typedef struct {
  SRtn       rtn;
  SFSIter    fsIter;
  SArray *   tbArray;  // table array to cache table obj and block indexes
  SReadH     readh;
  SDFileSet  wSet;
  SArray *   aBlkIdx;
  STable *   pTable;
  SArray *   aSupBlk;
  SDataCols *pDataCols;
} SCompactH;

#define TSDB_COMPACT_WSET(pComph) &((pComph)->wSet)
#define TSDB_COMPACT_REPO(pComph) TSDB_READ_REPO(&((pComph)->readh))
#define TSDB_COMPACT_HEAD_FILE(pComph) TSDB_DFILE_IN_SET(TSDB_COMPACT_WSET(pComph), TSDB_FILE_HEAD)
#define TSDB_COMPACT_DATA_FILE(pComph) TSDB_DFILE_IN_SET(TSDB_COMPACT_WSET(pComph), TSDB_FILE_DATA)
#define TSDB_COMPACT_LAST_FILE(pComph) TSDB_DFILE_IN_SET(TSDB_COMPACT_WSET(pComph), TSDB_FILE_LAST)
#define TSDB_COMPACT_BUF(pComph) TSDB_READ_BUF(&((pComph)->readh))
#define TSDB_COMPACT_COMP_BUF(pComph) TSDB_READ_COMP_BUF(&((pComph)->readh))

int tsdbCompact(STsdbRepo *pRepo) { return tsdbAsyncCompact(pRepo); }

void *tsdbCompactImpl(STsdbRepo *pRepo) {
  // Check if there are files in TSDB FS to compact
  if (REPO_FS(pRepo)->cstatus->pmf == NULL) {
    tsdbInfo("vgId:%d no file to compact in FS", REPO_ID(pRepo));
    return NULL;
  }

  tsdbStartCompact(pRepo);

  if (tsdbCompactMeta(pRepo) < 0) {
    tsdbError("vgId:%d failed to compact META data since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  if (tsdbCompactTSData(pRepo) < 0) {
    tsdbError("vgId:%d failed to compact TS data since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  tsdbEndCompact(pRepo, TSDB_CODE_SUCCESS);
  return NULL;

_err:
  pRepo->code = terrno;
  tsdbEndCompact(pRepo, terrno);
  return NULL;
}

static int tsdbAsyncCompact(STsdbRepo *pRepo) {
  tsem_wait(&(pRepo->readyToCommit));
  return tsdbScheduleCommit(pRepo, COMPACT_REQ)
}

static void tsdbStartCompact(STsdbRepo *pRepo) {
  tsdbInfo("vgId:%d start to compact!", REPO_ID(pRepo));
  tsdbStartFSTxn(pRepo, 0, 0);
  pRepo->code = TSDB_CODE_SUCCESS;
}

static void tsdbEndCompact(STsdbRepo *pRepo, int eno) {
  if (eno != TSDB_CODE_SUCCESS) {
    tsdbEndFSTxnWithError(REPO_FS(pRepo));
  } else {
    tsdbEndFSTxn(pRepo);
  }

  tsdbInfo("vgId:%d compact over, %s", REPO_ID(pRepo), (eno == TSDB_CODE_SUCCESS) ? "succeed" : "failed");
  tsem_post(&(pRepo->readyToCommit));
}

static int tsdbCompactMeta(STsdbRepo *pRepo) {
  // TODO
  return 0;
}

static int tsdbCompactTSData(STsdbRepo *pRepo) {
  SCompactH  compactH;
  SDFileSet *pSet = NULL;

  tsdbDebug("vgId:%d start to compact TS data", REPO_ID(pRepo));

  // If no file, just return 0;
  if (taosArrayGetSize(REPO_FS(pRepo)->cstatus->df) <= 0) {
    tsdbDebug("vgId:%d no TS data file to compact, compact over", REPO_ID(pRepo));
    return 0;
  }

  if (tsdbInitCompactH(&compactH, pRepo) < 0) {
    return -1;
  }

  while ((pSet = tsdbFSIterNext(&(compactH.fsIter)))) {
    // Remove those expired files
    if (pSet->fid < compactH.rtn.minFid) {
      tsdbInfo("vgId:%d FSET %d on level %d disk id %d expires, remove it", REPO_ID(pRepo), pSet->fid,
               TSDB_FSET_LEVEL(pSet), TSDB_FSET_ID(pSet));
      continue;
    }

    if (TSDB_FSET_LEVEL(pSet) == TFS_MAX_LEVEL) {
      tsdbDebug("vgId:%d FSET %d on level %d, should not compact", REPO_ID(pRepo), pSet->fid, TFS_MAX_LEVEL);
      tsdbUpdateDFileSet(REPO_FS(pRepo), pSet);
      continue;
    }

    if (tsdbCompactFSet(&compactH, pSet) < 0) {
      tsdbDestroyCompactH(&compactH);
      tsdbError("vgId:%d failed to compact FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
      return -1;
    }
  }

  tsdbDestroyCompactH(&compactH);
  tsdbDebug("vgId:%d compact TS data over", REPO_ID(pRepo));
  return 0;
}

static int tsdbCompactFSet(SCompactH *pCompH, SDFileSet *pSet) {
  STsdbRepo *pRepo = TSDB_COMPACT_REPO(pCompH);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  SReadH *   pReadH = &(pCompH->readh);
  SDiskID    did;
  SBlock     block;
  SBlockIdx  blkIdx;
  void **    ppBuf = &TSDB_COMPACT_BUF(pCompH);
  void **    ppCBuf = &TSDB_COMPACT_COMP_BUF(pCompH);
  int        defaultRows = TSDB_DEFAULT_BLOCK_ROWS(pCfg->maxRowsPerFileBlock);

  tsdbDebug("vgId:%d start to compact FSET %d", REPO_ID(pRepo), pSet->fid);

  // TODO: init fset state
  if (tsdbCacheFSetIndex(pCompH, pSet) < 0) {
    // TODO
    return -1;
  }

  if (!tsdbShouldCompact(pCommit)) {
    tsdbDebug("vgId:%d no need to compact FSET %d", REPO_ID(pRepo), pSet->fid);
    if (tsdbApplyRtnOnFSet(TSDB_COMPACT_REPO(pCompH), pSet, &(pCompH->rtn)) < 0) {
      return -1;
    }
  } else {
    // Create new fset as compacted fset
    tfsAllocDisk(tsdbGetFidLevel(pSet->fid, &(pCompH->rtn)), &(did.level), &(did.id));
    if (did.level == TFS_UNDECIDED_LEVEL) {
      terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
      tsdbError("vgId:%d failed to compact FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
      return -1;
    }

    tsdbInitDFileSet(TSDB_COMPACT_WSET(pCompH), did, REPO_ID(pRepo), TSDB_FSET_FID(pSet), FS_TXN_VERSION(REPO_FS(pRepo)));
    if (tsdbCreateDFileSet(&wfset, true) < 0) {
      // TODO
      return -1;
    }

    for (int tid = 1; tid < taosArrayGetSize(pCompH->tbArray); tid++) {
      STableCompactH *pTh = (STableCompactH *)taosArrayGet(pCompH->tbArray, tid);

      if (pTh->pTable == NULL || pTh->pBlkIdx == NULL) continue;

      tdInitDataCols(pCompH->pDataCols, tsdbGetTableSchemaImpl(pTh->pTable, false, false, -1)); // TODO
      for (int i = 0; i < pTh->pBlkIdx->numOfBlocks; i++) {
        SBlock *pBlock = pTh->pInfo->blocks + i;

        if (pBlock->numOfSubBlocks == 1 && pCompH->pDataCols->numOfRows == 0 && pBlock->numOfRows >= defaultRows) {
          if (tsdbLoadBlockData(pReadH, pBlock, pTh->pInfo) < 0) {
            // TODO
            return -1;
          }

          if (tsdbWriteBlockImpl(TSDB_COMPACT_REPO(pCompH), pTh->pTable, TSDB_COMPACT_DATA_FILE(pCompH),
                                 pReadH->pDCols[0], &block, false, true, ppBuf, ppCBuf) < 0) {
            // if (tsdbWriteBlock(NULL, NULL, pReadH->pDCols[0], &block, true, true) < 0) {
            // TODO
            return -1;
          }

          if (taosArrayPush(pCompH->aSupBlk, (void *)pBlock) < 0) {
            // TODO
            terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
            return -1;
          }
        } else {
          if (tsdbLoadBlockData(pReadH, pBlock, pTh->pInfo) < 0) {
            // TODO
            return -1;
          }

          int rowsToMerge = (pBlock->numOfRows, defaultRows - pCompH->pDataCols->numOfRows);
        }

        if (pCompH->pDataCols->numOfRows > 0) {
          if (tsdbWriteBlock(NULL/*TODO*/, NULL, pCompH->pDataCols, &block, true/*TODO*/, true) < 0) {
            // TODO
            return -1;
          }
        }
      }

      if (tsdbWriteBlockInfoImpl(TSDB_COMPACT_HEAD_FILE(pCompH), pTh->pTable, pCompH->aSupBlk, NULL, ppBuf, &blkIdx) <
          0) {
        // TODO
        return -1;
      }

      if (blkIdx.numOfBlocks > 0) {
        if (taosArrayPush(pCompH->aBlkIdx, (void *)(&blkIdx)) == NULL) {
          // TODO
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          return -1;
        }
      }
    }

    if (tsdbWriteBlockIdx(TSDB_COMPACT_HEAD_FILE(pCompH), pCompH->aBlkIdx, ppBuf) < 0) {
      // TODO
      return -1;
    }

    tsdbCloseDFileSet(TSDB_COMPACT_WSET(pCompH));
    tsdbUpdateDFileSet(REPO_FS(pRepo), TSDB_COMPACT_WSET(pCompH));
    tsdbDebug("vgId:%d FSET %d compact over", REPO_ID(pRepo), fset.fid);
  }

  return 0;
}

static bool tsdbShouldCompact(SCompactH *pCompH) {
  // TODO
  return false;
}

static int tsdbInitCompactH(SCompactH *pCompH, STsdbRepo *pRepo) {
  STsdbCfg *pCfg = REPO_CFG(pRepo);

  memset(pCompH, 0, sizeof(*pCompH));

  TSDB_FSET_SET_CLOSED(TSDB_COMPACT_WSET(pCompH));

  tsdbGetRtnSnap(pRepo, &(pCompH->rtn));
  tsdbFSIterInit(&(pCompH->fsIter), REPO_FS(pRepo), TSDB_FS_ITER_FORWARD);

  if (tsdbInitReadH(&(pCompH->readh), pRepo) < 0) {
    return -1;
  }

  if (tsdbInitCompTableH(pCompH) < 0) {
    tsdbDestroyCompactH(pCompH);
    return -1;
  }

  pCompH->aBlkIdx = taosArrayInit(1024, sizeof(SBlockIdx));
  if (pCompH->aBlkIdx == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyCompactH(pCompH);
    return -1;
  }

  pCompH->aSupBlk = taosArrayInit(1024, sizeof(SBlockIdx));
  if (pCompH->aSupBlk == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyCompactH(pCompH);
    return -1;
  }

  pCompH->pDataCols = tdNewDataCols(0, 0, pCfg->maxRowsPerFileBlock);
  if (pCompH->pDataCols == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyCompactH(pCompH);
    return -1;
  }

  return 0;
}

static void tsdbDestroyCompactH(SCompactH *pCompH) {
  pCompH->pDataCols = tdFreeDataCols(pCompH->pDataCols);
  pCompH->aSupBlk = taosArrayDestroy(pCompH->aSupBlk);
  pCompH->aBlkIdx = taosArrayDestroy(pCompH->aBlkIdx);
  pCompH->tbArray = taosArrayDestroy(pCompH->tbArray);
  tsdbDestroyReadH(&(pCompH->readh));
  tsdbCloseDFileSet(TSDB_COMPACT_WSET(pCompH));
}

static int tsdbInitCompTableH(SCompactH *pCompH) {  // Init pComp->tbArray
  STsdbRepo *pRepo = TSDB_COMPACT_REPO(pCompH);
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  pCompH->tbArray = taosArrayInit(pMeta->maxTables, sizeof(STableCompactH));
  if (pCompH->tbArray == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (tsdbRLockRepoMeta(pRepo) < 0) return -1;

  for (int i = 0; i < pMeta->maxTables; i++) {
    STableCompactH ch = {0};
    if (pMeta->tables[i] != NULL) {
      ch.pTable = pMeta->tables[i];
    }

    if (taosArrayPush(pCompH->tbArray, &ch) < 0) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      tsdbUnlockRepoMeta(pRepo);
      return -1;
    }
  }

  if (tsdbUnlockRepoMeta(pRepo) < 0) return -1;
}

static int tsdbCacheFSetIndex(SCompactH *pCompH, SDFileSet *pSet) {
  SReadH *pReadH = &(pCompH->readh);
  if (tsdbSetAndOpenReadFSet(pReadH, pSet) < 0) {
    // TODO
    return -1;
  }

  if (tsdbLoadBlockIdx(pReadH) < 0) {
    // TODO
    return -1;
  }

  for (int tid = 1; tid < taosArrayGetSize(pCompH->tbArray); tid++) {
    STableCompactH *pTh = taosArrayGet(pCompH->tbArray, tid);
    pTh->pBlkIdx = NULL;

    if (pTh->pTable == NULL) continue;
    if (tsdbSetReadTable(pReadH, pTh->pTable) < 0) {
      // TODO
      return -1;
    }

    if (pReadH->pBlkIdx == NULL) continue;
    pTh->bindex = *(pReadH->pBlkIdx);
    pTh->pBlkIdx = &(pTh->bindex);

    if (tsdbMakeRoom((void **)(&pTh->pInfo), pTh->pBlkIdx->len) < 0) {
      // TODO
      return -1;
    }

    if (tsdbLoadBlockInfo(pReadH, (void *)(pTh->pInfo)) < 0) {
      // TODO
      return -1;
    }
  }

  return 0;
}