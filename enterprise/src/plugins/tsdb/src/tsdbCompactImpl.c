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
  SArray *   aSupBlk;
  SDataCols *pDataCols;
} SCompactH;

#define TSDB_COMPACT_WSET(pComph) (&((pComph)->wSet))
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

static int tsdbCompactFSet(SCompactH *pComph, SDFileSet *pSet) {
  STsdbRepo *pRepo = TSDB_COMPACT_REPO(pComph);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  SReadH *   pReadH = &(pComph->readh);
  SDiskID    did;
  SBlock     block;
  SBlockIdx  blkIdx;
  void **    ppBuf = &TSDB_COMPACT_BUF(pComph);
  void **    ppCBuf = &TSDB_COMPACT_COMP_BUF(pComph);
  int        defaultRows = TSDB_DEFAULT_BLOCK_ROWS(pCfg->maxRowsPerFileBlock);

  tsdbDebug("vgId:%d start to compact FSET %d", REPO_ID(pRepo), pSet->fid);

  // TODO: init fset state
  if (tsdbCacheFSetIndex(pComph, pSet) < 0) {
    // TODO
    return -1;
  }

  if (!tsdbShouldCompact(pCommit)) {
    tsdbDebug("vgId:%d no need to compact FSET %d", REPO_ID(pRepo), pSet->fid);
    if (tsdbApplyRtnOnFSet(TSDB_COMPACT_REPO(pComph), pSet, &(pComph->rtn)) < 0) {
      return -1;
    }
  } else {
    // Create new fset as compacted fset
    tfsAllocDisk(tsdbGetFidLevel(pSet->fid, &(pComph->rtn)), &(did.level), &(did.id));
    if (did.level == TFS_UNDECIDED_LEVEL) {
      terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
      tsdbError("vgId:%d failed to compact FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
      return -1;
    }

    tsdbInitDFileSet(TSDB_COMPACT_WSET(pComph), did, REPO_ID(pRepo), TSDB_FSET_FID(pSet), FS_TXN_VERSION(REPO_FS(pRepo)));
    if (tsdbCreateDFileSet(&wfset, true) < 0) {
      // TODO
      return -1;
    }

    for (int tid = 1; tid < taosArrayGetSize(pComph->tbArray); tid++) {
      STableCompactH *pTh = (STableCompactH *)taosArrayGet(pComph->tbArray, tid);

      if (pTh->pTable == NULL || pTh->pBlkIdx == NULL) continue;

      tdInitDataCols(pComph->pDataCols, tsdbGetTableSchemaImpl(pTh->pTable, false, false, -1)); // TODO
      for (int i = 0; i < pTh->pBlkIdx->numOfBlocks; i++) {
        SBlock *pBlock = pTh->pInfo->blocks + i;

        if (pBlock->numOfSubBlocks == 1 && pComph->pDataCols->numOfRows == 0 && pBlock->numOfRows >= defaultRows) {
          if (tsdbLoadBlockData(pReadH, pBlock, pTh->pInfo) < 0) {
            // TODO
            return -1;
          }

          if (tsdbWriteBlockImpl(TSDB_COMPACT_REPO(pComph), pTh->pTable, TSDB_COMPACT_DATA_FILE(pComph),
                                 pReadH->pDCols[0], &block, false, true, ppBuf, ppCBuf) < 0) {
            // if (tsdbWriteBlock(NULL, NULL, pReadH->pDCols[0], &block, true, true) < 0) {
            // TODO
            return -1;
          }

          if (taosArrayPush(pComph->aSupBlk, (void *)pBlock) < 0) {
            // TODO
            terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
            return -1;
          }
        } else {
          if (tsdbLoadBlockData(pReadH, pBlock, pTh->pInfo) < 0) {
            // TODO
            return -1;
          }

          int rowsToMerge = (pBlock->numOfRows, defaultRows - pComph->pDataCols->numOfRows);
        }

        if (pComph->pDataCols->numOfRows > 0) {
          if (tsdbWriteBlock(NULL/*TODO*/, NULL, pComph->pDataCols, &block, true/*TODO*/, true) < 0) {
            // TODO
            return -1;
          }
        }
      }

      if (tsdbWriteBlockInfoImpl(TSDB_COMPACT_HEAD_FILE(pComph), pTh->pTable, pComph->aSupBlk, NULL, ppBuf, &blkIdx) <
          0) {
        // TODO
        return -1;
      }

      if (blkIdx.numOfBlocks > 0) {
        if (taosArrayPush(pComph->aBlkIdx, (void *)(&blkIdx)) == NULL) {
          // TODO
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          return -1;
        }
      }
    }

    if (tsdbWriteBlockIdx(TSDB_COMPACT_HEAD_FILE(pComph), pComph->aBlkIdx, ppBuf) < 0) {
      // TODO
      return -1;
    }

    tsdbCloseDFileSet(TSDB_COMPACT_WSET(pComph));
    tsdbUpdateDFileSet(REPO_FS(pRepo), TSDB_COMPACT_WSET(pComph));
    tsdbDebug("vgId:%d FSET %d compact over", REPO_ID(pRepo), fset.fid);
  }

  return 0;
}

static bool tsdbShouldCompact(SCompactH *pComph) {
  // TODO
  return false;
}

static int tsdbInitCompactH(SCompactH *pComph, STsdbRepo *pRepo) {
  STsdbCfg *pCfg = REPO_CFG(pRepo);

  memset(pComph, 0, sizeof(*pComph));

  TSDB_FSET_SET_CLOSED(TSDB_COMPACT_WSET(pComph));

  tsdbGetRtnSnap(pRepo, &(pComph->rtn));
  tsdbFSIterInit(&(pComph->fsIter), REPO_FS(pRepo), TSDB_FS_ITER_FORWARD);

  if (tsdbInitReadH(&(pComph->readh), pRepo) < 0) {
    return -1;
  }

  if (tsdbInitCompTbArray(pComph) < 0) {
    tsdbDestroyCompactH(pComph);
    return -1;
  }

  pComph->aBlkIdx = taosArrayInit(1024, sizeof(SBlockIdx));
  if (pComph->aBlkIdx == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyCompactH(pComph);
    return -1;
  }

  pComph->aSupBlk = taosArrayInit(1024, sizeof(SBlock));
  if (pComph->aSupBlk == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyCompactH(pComph);
    return -1;
  }

  pComph->pDataCols = tdNewDataCols(0, 0, pCfg->maxRowsPerFileBlock);
  if (pComph->pDataCols == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyCompactH(pComph);
    return -1;
  }

  return 0;
}

static void tsdbDestroyCompactH(SCompactH *pComph) {
  pComph->pDataCols = tdFreeDataCols(pComph->pDataCols);
  pComph->aSupBlk = taosArrayDestroy(pComph->aSupBlk);
  pComph->aBlkIdx = taosArrayDestroy(pComph->aBlkIdx);
  tsdbDestroyCompTbArray(pComph);
  tsdbDestroyReadH(&(pComph->readh));
  tsdbCloseDFileSet(TSDB_COMPACT_WSET(pComph));
}

static int tsdbInitCompTbArray(SCompactH *pComph) {  // Init pComp->tbArray
  STsdbRepo *pRepo = TSDB_COMPACT_REPO(pComph);
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  if (tsdbRLockRepoMeta(pRepo) < 0) return -1;

  pComph->tbArray = taosArrayInit(pMeta->maxTables, sizeof(STableCompactH));
  if (pComph->tbArray == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbUnlockRepoMeta(pRepo);
    return -1;
  }

  // Note here must start from 0
  for (int i = 0; i < pMeta->maxTables; i++) {
    STableCompactH ch = {0};
    if (pMeta->tables[i] != NULL) {
      tsdbRefTable(pMeta->tables[i]);
      ch.pTable = pMeta->tables[i];
    }

    if (taosArrayPush(pComph->tbArray, &ch) < 0) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      tsdbUnlockRepoMeta(pRepo);
      return -1;
    }
  }

  if (tsdbUnlockRepoMeta(pRepo) < 0) return -1;
  return 0;
}

static void tsdbDestroyCompTbArray(SCompactH *pComph) {
  STableCompactH *pTh;

  if (pComph->tbArray == NULL) return;

  for (size_t i = 0; i < taosArrayGetSize(pComph->tbArray); i++) {
    pTh = (STableCompactH *)taosArrayGet(pComph->tbArray, i);
    if (pTh->pTable) {
      tsdbUnRefTable(pTh->pTable);
    }
  }

  pComph->tbArray = taosArrayDestroy(pComph->tbArray);
}

static int tsdbCacheFSetIndex(SCompactH *pComph) {
  SReadH *pReadH = &(pComph->readh);

  if (tsdbLoadBlockIdx(pReadH) < 0) {
    // TODO
    return -1;
  }

  for (int tid = 1; tid < taosArrayGetSize(pComph->tbArray); tid++) {
    STableCompactH *pTh = taosArrayGet(pComph->tbArray, tid);
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

static int tsdbCompactFSetInit(SCompactH *pComph, SDFileSet *pSet) {
  taosArrayClear(pComph->aBlkIdx);
  taosArrayClear(pComph->aSupBlk);

  if (tsdbSetAndOpenReadFSet(&(pComph->readh), pSet) < 0) {
    return -1;
  }

  if (tsdbCacheFSetIndex(pComph) < 0) {
    tsdbCloseAndUnsetFSet(&(pComph->readh));
    return -1;
  }

  return 0;
}

static void tsdbCompactFSetEnd(SCompactH *pComph) {
  // TODO
}