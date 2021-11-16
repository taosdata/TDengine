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

static int  tsdbAsyncCompact(STsdbRepo *pRepo);
static void tsdbStartCompact(STsdbRepo *pRepo);
static void tsdbEndCompact(STsdbRepo *pRepo, int eno);
static int  tsdbCompactMeta(STsdbRepo *pRepo);
static int  tsdbCompactTSData(STsdbRepo *pRepo);
static int  tsdbCompactFSet(SCompactH *pComph, SDFileSet *pSet);
static bool tsdbShouldCompact(SCompactH *pComph);
static int  tsdbInitCompactH(SCompactH *pComph, STsdbRepo *pRepo);
static void tsdbDestroyCompactH(SCompactH *pComph);
static int  tsdbInitCompTbArray(SCompactH *pComph);
static void tsdbDestroyCompTbArray(SCompactH *pComph);
static int  tsdbCacheFSetIndex(SCompactH *pComph);
static int  tsdbCompactFSetInit(SCompactH *pComph, SDFileSet *pSet);
static void tsdbCompactFSetEnd(SCompactH *pComph);
static int  tsdbCompactFSetImpl(SCompactH *pComph);
static int  tsdbWriteBlockToRightFile(SCompactH *pComph, STable *pTable, SDataCols *pDataCols, void **ppBuf,
                                      void **ppCBuf);

enum { TSDB_NO_COMPACT, TSDB_IN_COMPACT, TSDB_WAITING_COMPACT};
int tsdbCompact(STsdbRepo *pRepo) { return tsdbAsyncCompact(pRepo); }

void *tsdbCompactImpl(STsdbRepo *pRepo) {
  // Check if there are files in TSDB FS to compact
  if (REPO_FS(pRepo)->cstatus->pmf == NULL) {
    pRepo->compactState = TSDB_NO_COMPACT;
    tsem_post(&(pRepo->readyToCommit));
    tsdbInfo("vgId:%d compact over, no file to compact in FS", REPO_ID(pRepo));
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
  if (pRepo->compactState != TSDB_NO_COMPACT) {
    tsdbInfo("vgId:%d not compact tsdb again ", REPO_ID(pRepo));
    return 0; 
  } 
  pRepo->compactState = TSDB_WAITING_COMPACT;   
  tsem_wait(&(pRepo->readyToCommit));
  return tsdbScheduleCommit(pRepo, COMPACT_REQ);
}

static void tsdbStartCompact(STsdbRepo *pRepo) {
  assert(pRepo->compactState != TSDB_IN_COMPACT);
  tsdbInfo("vgId:%d start to compact!", REPO_ID(pRepo));
  tsdbStartFSTxn(pRepo, 0, 0);
  pRepo->code = TSDB_CODE_SUCCESS;
  pRepo->compactState = TSDB_IN_COMPACT;
}

static void tsdbEndCompact(STsdbRepo *pRepo, int eno) {
  if (eno != TSDB_CODE_SUCCESS) {
    tsdbEndFSTxnWithError(REPO_FS(pRepo));
  } else {
    tsdbEndFSTxn(pRepo);
  }
  pRepo->compactState = TSDB_NO_COMPACT;
  tsdbInfo("vgId:%d compact over, %s", REPO_ID(pRepo), (eno == TSDB_CODE_SUCCESS) ? "succeed" : "failed");
  tsem_post(&(pRepo->readyToCommit));
}

static int tsdbCompactMeta(STsdbRepo *pRepo) {
  STsdbFS *pfs = REPO_FS(pRepo);
  tsdbUpdateMFile(pfs, pfs->cstatus->pmf);
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
    SDiskID    did;

    tsdbDebug("vgId:%d start to compact FSET %d on level %d id %d", REPO_ID(pRepo), pSet->fid, TSDB_FSET_LEVEL(pSet),
              TSDB_FSET_ID(pSet));

    if (tsdbCompactFSetInit(pComph, pSet) < 0) {
      return -1;
    }

    if (!tsdbShouldCompact(pComph)) {
      tsdbDebug("vgId:%d no need to compact FSET %d", REPO_ID(pRepo), pSet->fid);
      if (tsdbApplyRtnOnFSet(TSDB_COMPACT_REPO(pComph), pSet, &(pComph->rtn)) < 0) {
        tsdbCompactFSetEnd(pComph);
        return -1;
      }
    } else {
      // Create new fset as compacted fset
      tfsAllocDisk(tsdbGetFidLevel(pSet->fid, &(pComph->rtn)), &(did.level), &(did.id));
      if (did.level == TFS_UNDECIDED_LEVEL) {
        terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
        tsdbError("vgId:%d failed to compact FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
        tsdbCompactFSetEnd(pComph);
        return -1;
      }

      tsdbInitDFileSet(TSDB_COMPACT_WSET(pComph), did, REPO_ID(pRepo), TSDB_FSET_FID(pSet),
                      FS_TXN_VERSION(REPO_FS(pRepo)));
      if (tsdbCreateDFileSet(TSDB_COMPACT_WSET(pComph), true) < 0) {
        tsdbError("vgId:%d failed to compact FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
        tsdbCompactFSetEnd(pComph);
        return -1;
      }

      if (tsdbCompactFSetImpl(pComph) < 0) {
        tsdbCloseDFileSet(TSDB_COMPACT_WSET(pComph));
        tsdbRemoveDFileSet(TSDB_COMPACT_WSET(pComph));
        tsdbCompactFSetEnd(pComph);
        return -1;
      }

      tsdbCloseDFileSet(TSDB_COMPACT_WSET(pComph));
      tsdbUpdateDFileSet(REPO_FS(pRepo), TSDB_COMPACT_WSET(pComph));
      tsdbDebug("vgId:%d FSET %d compact over", REPO_ID(pRepo), pSet->fid);
    }

    tsdbCompactFSetEnd(pComph);
    return 0;
  }

  static bool tsdbShouldCompact(SCompactH *pComph) {
    STsdbRepo *     pRepo = TSDB_COMPACT_REPO(pComph);
    STsdbCfg *      pCfg = REPO_CFG(pRepo);
    SReadH *        pReadh = &(pComph->readh);
    STableCompactH *pTh;
    SBlock *        pBlock;
    int             defaultRows = TSDB_DEFAULT_BLOCK_ROWS(pCfg->maxRowsPerFileBlock);
    SDFile *        pDataF = TSDB_READ_DATA_FILE(pReadh);
    SDFile *        pLastF = TSDB_READ_LAST_FILE(pReadh);

    int     tblocks = 0;       // total blocks
    int     nSubBlocks = 0;    // # of blocks with sub-blocks
    int     nSmallBlocks = 0;  // # of blocks with rows < defaultRows
    int64_t tsize = 0;

    for (size_t i = 0; i < taosArrayGetSize(pComph->tbArray); i++) {
      pTh = (STableCompactH *)taosArrayGet(pComph->tbArray, i);

      if (pTh->pTable == NULL || pTh->pBlkIdx == NULL) continue;

      for (size_t bidx = 0; bidx < pTh->pBlkIdx->numOfBlocks; bidx++) {
        tblocks++;
        pBlock = pTh->pInfo->blocks + bidx;

        if (pBlock->numOfRows < defaultRows) {
          nSmallBlocks++;
        }

        if (pBlock->numOfSubBlocks > 1) {
          nSubBlocks++;
          for (int k = 0; k < pBlock->numOfSubBlocks; k++) {
            SBlock *iBlock = ((SBlock *)POINTER_SHIFT(pTh->pInfo, pBlock->offset)) + k;
            tsize = tsize + iBlock->len;
          }
        } else if (pBlock->numOfSubBlocks == 1) {
          tsize += pBlock->len;
        } else {
          ASSERT(0);
        }
      }
    }

    return (((nSubBlocks * 1.0 / tblocks) > 0.33) || ((nSmallBlocks * 1.0 / tblocks) > 0.33) ||
            (tsize * 1.0 / (pDataF->info.size + pLastF->info.size - 2 * TSDB_FILE_HEAD_SIZE) < 0.85));
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

    pComph->pDataCols = tdNewDataCols(0, pCfg->maxRowsPerFileBlock);
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

      if (taosArrayPush(pComph->tbArray, &ch) == NULL) {
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

      pTh->pInfo = taosTZfree(pTh->pInfo);
    }

    pComph->tbArray = taosArrayDestroy(pComph->tbArray);
  }

  static int tsdbCacheFSetIndex(SCompactH *pComph) {
    SReadH *pReadH = &(pComph->readh);

    if (tsdbLoadBlockIdx(pReadH) < 0) {
      return -1;
    }

    for (int tid = 1; tid < taosArrayGetSize(pComph->tbArray); tid++) {
      STableCompactH *pTh = (STableCompactH *)taosArrayGet(pComph->tbArray, tid);
      pTh->pBlkIdx = NULL;

      if (pTh->pTable == NULL) continue;
      if (tsdbSetReadTable(pReadH, pTh->pTable) < 0) {
        return -1;
      }

      if (pReadH->pBlkIdx == NULL) continue;
      pTh->bindex = *(pReadH->pBlkIdx);
      pTh->pBlkIdx = &(pTh->bindex);

      if (tsdbMakeRoom((void **)(&(pTh->pInfo)), pTh->pBlkIdx->len) < 0) {
        return -1;
      }

      if (tsdbLoadBlockInfo(pReadH, (void *)(pTh->pInfo)) < 0) {
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

  static void tsdbCompactFSetEnd(SCompactH *pComph) { tsdbCloseAndUnsetFSet(&(pComph->readh)); }

  static int tsdbCompactFSetImpl(SCompactH *pComph) {
    STsdbRepo *pRepo = TSDB_COMPACT_REPO(pComph);
    STsdbCfg * pCfg = REPO_CFG(pRepo);
    SReadH *   pReadh = &(pComph->readh);
    SBlockIdx  blkIdx;
    void **    ppBuf = &(TSDB_COMPACT_BUF(pComph));
    void **    ppCBuf = &(TSDB_COMPACT_COMP_BUF(pComph));
    int        defaultRows = TSDB_DEFAULT_BLOCK_ROWS(pCfg->maxRowsPerFileBlock);

    taosArrayClear(pComph->aBlkIdx);

    for (int tid = 1; tid < taosArrayGetSize(pComph->tbArray); tid++) {
      STableCompactH *pTh = (STableCompactH *)taosArrayGet(pComph->tbArray, tid);
      STSchema *      pSchema;

      if (pTh->pTable == NULL || pTh->pBlkIdx == NULL) continue;

      pSchema = tsdbGetTableSchemaImpl(pTh->pTable, true, true, -1, -1);
      taosArrayClear(pComph->aSupBlk);
      if ((tdInitDataCols(pComph->pDataCols, pSchema) < 0) || (tdInitDataCols(pReadh->pDCols[0], pSchema) < 0) ||
          (tdInitDataCols(pReadh->pDCols[1], pSchema) < 0)) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }
      tdFreeSchema(pSchema);

      // Loop to compact each block data
      for (int i = 0; i < pTh->pBlkIdx->numOfBlocks; i++) {
        SBlock *pBlock = pTh->pInfo->blocks + i;

        // Load the block data
        if (tsdbLoadBlockData(pReadh, pBlock, pTh->pInfo) < 0) {
          return -1;
        }

        // Merge pComph->pDataCols and pReadh->pDCols[0] and write data to file
        if (pComph->pDataCols->numOfRows == 0 && pBlock->numOfRows >= defaultRows) {
          if (tsdbWriteBlockToRightFile(pComph, pTh->pTable, pReadh->pDCols[0], ppBuf, ppCBuf) < 0) {
            return -1;
          }
        } else {
          int ridx = 0;

          while (true) {
            if (pReadh->pDCols[0]->numOfRows - ridx == 0) break;
            int rowsToMerge = MIN(pReadh->pDCols[0]->numOfRows - ridx, defaultRows - pComph->pDataCols->numOfRows);

            tdMergeDataCols(pComph->pDataCols, pReadh->pDCols[0], rowsToMerge, &ridx, pCfg->update != TD_ROW_PARTIAL_UPDATE);

            if (pComph->pDataCols->numOfRows < defaultRows) {
              break;
            }

            if (tsdbWriteBlockToRightFile(pComph, pTh->pTable, pComph->pDataCols, ppBuf, ppCBuf) < 0) {
              return -1;
            }
            tdResetDataCols(pComph->pDataCols);
          }
        }
      }

      if (pComph->pDataCols->numOfRows > 0 &&
          tsdbWriteBlockToRightFile(pComph, pTh->pTable, pComph->pDataCols, ppBuf, ppCBuf) < 0) {
        return -1;
      }

      if (tsdbWriteBlockInfoImpl(TSDB_COMPACT_HEAD_FILE(pComph), pTh->pTable, pComph->aSupBlk, NULL, ppBuf, &blkIdx) <
          0) {
        return -1;
      }

      if ((blkIdx.numOfBlocks > 0) && (taosArrayPush(pComph->aBlkIdx, (void *)(&blkIdx)) == NULL)) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }
    }

    if (tsdbWriteBlockIdx(TSDB_COMPACT_HEAD_FILE(pComph), pComph->aBlkIdx, ppBuf) < 0) {
      return -1;
    }

    return 0;
  }

  static int tsdbWriteBlockToRightFile(SCompactH *pComph, STable *pTable, SDataCols *pDataCols, void **ppBuf,
                                      void **ppCBuf) {
    STsdbRepo *pRepo = TSDB_COMPACT_REPO(pComph);
    STsdbCfg * pCfg = REPO_CFG(pRepo);
    SDFile *   pDFile;
    bool       isLast;
    SBlock     block;

    ASSERT(pDataCols->numOfRows > 0);

    if (pDataCols->numOfRows < pCfg->minRowsPerFileBlock) {
      pDFile = TSDB_COMPACT_LAST_FILE(pComph);
      isLast = true;
    } else {
      pDFile = TSDB_COMPACT_DATA_FILE(pComph);
      isLast = false;
    }

    if (tsdbWriteBlockImpl(pRepo, pTable, pDFile, pDataCols, &block, isLast, true, ppBuf, ppCBuf) < 0) {
      return -1;
    }

    if (taosArrayPush(pComph->aSupBlk, (void *)(&block)) == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }

    return 0;
}

