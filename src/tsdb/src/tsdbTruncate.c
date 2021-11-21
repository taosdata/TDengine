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
  SBlockIdx   bIndex;
  SBlockInfo *pInfo;
} STableTruncateH;

typedef struct {
  SRtn       rtn;
  SFSIter    fsIter;
  SArray *   tblArray;  // STableTruncateH, table array to cache table obj and block indexes
  SReadH     readh;
  SDFileSet  wSet;
  SArray *   aBlkIdx;
  SArray *   aSupBlk;
  SDataCols *pDataCols;
  void *     param;  // STruncateTblMsg *pMsg
} STruncateH;

#define TSDB_TRUNCATE_WSET(pTruncateH) (&((pTruncateH)->wSet))
#define TSDB_TRUNCATE_REPO(pTruncateH) TSDB_READ_REPO(&((pTruncateH)->readh))
#define TSDB_TRUNCATE_HEAD_FILE(pTruncateH) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(pTruncateH), TSDB_FILE_HEAD)
#define TSDB_TRUNCATE_DATA_FILE(pTruncateH) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(pTruncateH), TSDB_FILE_DATA)
#define TSDB_TRUNCATE_LAST_FILE(pTruncateH) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(pTruncateH), TSDB_FILE_LAST)
#define TSDB_TRUNCATE_SMAD_FILE(pTruncateH) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(pTruncateH), TSDB_FILE_SMAD)
#define TSDB_TRUNCATE_SMAL_FILE(pTruncateH) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(pTruncateH), TSDB_FILE_SMAL)
#define TSDB_TRUNCATE_BUF(pTruncateH) TSDB_READ_BUF(&((pTruncateH)->readh))
#define TSDB_TRUNCATE_COMP_BUF(pTruncateH) TSDB_READ_COMP_BUF(&((pTruncateH)->readh))
#define TSDB_TRUNCATE_EXBUF(pTruncateH) TSDB_READ_EXBUF(&((pTruncateH)->readh))

static int  tsdbAsyncTruncate(STsdbRepo *pRepo, void *param);
static void tsdbStartTruncate(STsdbRepo *pRepo);
static void tsdbEndTruncate(STsdbRepo *pRepo, int eno);
static int  tsdbTruncateMeta(STsdbRepo *pRepo);
static int  tsdbTruncateTSData(STsdbRepo *pRepo, void *param);
static int  tsdbTruncateFSet(STruncateH *pTruncateH, SDFileSet *pSet);
static int  tsdbInitTruncateH(STruncateH *pTruncateH, STsdbRepo *pRepo);
static void tsdbDestroyTruncateH(STruncateH *pTruncateH);
static int  tsdbInitTruncateTblArray(STruncateH *pTruncateH);
static void tsdbDestroyTruncateTblArray(STruncateH *pTruncateH);
static int  tsdbCacheFSetIndex(STruncateH *pTruncateH);
static int  tsdbTruncateCache(STsdbRepo *pRepo, void *param);
static int  tsdbTruncateFSetInit(STruncateH *pTruncateH, SDFileSet *pSet);
static void tsdbTruncateFSetEnd(STruncateH *pTruncateH);
static int  tsdbTruncateFSetImpl(STruncateH *pTruncateH);
static int  tsdbWriteBlockToRightFile(STruncateH *pTruncateH, STable *pTable, SDataCols *pDataCols, void **ppBuf,
                                      void **ppCBuf, void **ppExBuf);

enum {
  TSDB_NO_TRUNCATE,
  TSDB_IN_TRUNCATE,
  TSDB_WAITING_TRUNCATE,
};

int tsdbTruncate(STsdbRepo *pRepo, void *param) { return tsdbAsyncTruncate(pRepo, param); }

void *tsdbTruncateImpl(STsdbRepo *pRepo, void *param) {
  int32_t code = 0;
  // Step 1: check and clear cache
  if ((code = tsdbTruncateCache(pRepo, param)) != 0) {
    pRepo->code = terrno;
    tsem_post(&(pRepo->readyToCommit));
    tsdbInfo("vgId:%d failed to truncate since %s", REPO_ID(pRepo), tstrerror(terrno));
    return NULL;
  }

  // Step 2: truncate and rebuild DFileSets
  // Check if there are files in TSDB FS to truncate
  if ((REPO_FS(pRepo)->cstatus->pmf == NULL) || (taosArrayGetSize(REPO_FS(pRepo)->cstatus->df) <= 0)) {
    pRepo->truncateState = TSDB_NO_TRUNCATE;
    tsem_post(&(pRepo->readyToCommit));
    tsdbInfo("vgId:%d truncate over, no meta or data file", REPO_ID(pRepo));
    return NULL;
  }

  tsdbStartTruncate(pRepo);

  if (tsdbTruncateMeta(pRepo) < 0) {
    tsdbError("vgId:%d failed to truncate META data since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  if (tsdbTruncateTSData(pRepo, param) < 0) {
    tsdbError("vgId:%d failed to truncate TS data since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  tsdbEndTruncate(pRepo, TSDB_CODE_SUCCESS);
  return NULL;

_err:
  pRepo->code = terrno;
  tsdbEndTruncate(pRepo, terrno);
  return NULL;
}

static int tsdbTruncateCache(STsdbRepo *pRepo, void *param) {
  // step 1: reset query cache(reset all or the specific cache)
  // TODO ... check with Doctor Liao
  // if(... <0){
  //   terrno = ...;
  //   return -1;
  // }

  // step 2: check and clear cache of last_row/last
  // TODO: ... scan/check/clear stable/child table/common table
  // if(... <0){
  //   terrno = ...;
  //   return -1;
  // }

  return 0;
}

static int tsdbAsyncTruncate(STsdbRepo *pRepo, void *param) {
  // avoid repeated input of commands by end users in a short period of time
  if (pRepo->truncateState != TSDB_NO_TRUNCATE) {
    tsdbInfo("vgId:%d retry later as tsdb in truncating state", REPO_ID(pRepo));
    return -1;
  }
  pRepo->truncateState = TSDB_WAITING_TRUNCATE;

  // flush the mem data to disk synchronously(have impact on the compression rate)
  if (tsdbSyncCommit(pRepo) < 0) {
    return -1;
  }

  // truncate
  tsem_wait(&(pRepo->readyToCommit));
  int code = tsdbScheduleCommit(pRepo, param, TRUNCATE_REQ);
  if (code < 0) {
    tsem_post(&(pRepo->readyToCommit));
  }
  return code;
}

static void tsdbStartTruncate(STsdbRepo *pRepo) {
  assert(pRepo->truncateState != TSDB_IN_TRUNCATE);
  tsdbInfo("vgId:%d start to truncate!", REPO_ID(pRepo));
  tsdbStartFSTxn(pRepo, 0, 0);
  pRepo->code = TSDB_CODE_SUCCESS;
  pRepo->truncateState = TSDB_IN_TRUNCATE;
}

static void tsdbEndTruncate(STsdbRepo *pRepo, int eno) {
  if (eno != TSDB_CODE_SUCCESS) {
    tsdbEndFSTxnWithError(REPO_FS(pRepo));
  } else {
    tsdbEndFSTxn(pRepo);
  }
  pRepo->truncateState = TSDB_NO_TRUNCATE;
  tsdbInfo("vgId:%d truncate over, %s", REPO_ID(pRepo), (eno == TSDB_CODE_SUCCESS) ? "succeed" : "failed");
  tsem_post(&(pRepo->readyToCommit));
}

static int tsdbTruncateMeta(STsdbRepo *pRepo) {
  STsdbFS *pfs = REPO_FS(pRepo);
  tsdbUpdateMFile(pfs, pfs->cstatus->pmf);
  return 0;
}

static int tsdbTruncateTSData(STsdbRepo *pRepo, void *param) {
  STsdbCfg *       pCfg = REPO_CFG(pRepo);
  STruncateH       truncateH = {0};
  SDFileSet *      pSet = NULL;
  STruncateTblMsg *pMsg = (STruncateTblMsg *)param;
  ASSERT(pMsg != NULL);

  truncateH.param = pMsg;

  tsdbDebug("vgId:%d start to truncate TS data for %" PRIu64, REPO_ID(pRepo), pMsg->uid);

  if (tsdbInitTruncateH(&truncateH, pRepo) < 0) {
    return -1;
  }

  int sFid = TSDB_KEY_FID(pMsg->span[0].skey, pCfg->daysPerFile, pCfg->precision);
  int eFid = TSDB_KEY_FID(pMsg->span[0].ekey, pCfg->daysPerFile, pCfg->precision);
  ASSERT(sFid <= eFid);

  while ((pSet = tsdbFSIterNext(&(truncateH.fsIter)))) {
    // remove expired files
    if (pSet->fid < truncateH.rtn.minFid) {
      tsdbInfo("vgId:%d FSET %d on level %d disk id %d expires, remove it", REPO_ID(pRepo), pSet->fid,
               TSDB_FSET_LEVEL(pSet), TSDB_FSET_ID(pSet));
      continue;
    }

    if ((pSet->fid < sFid) || (pSet->fid > eFid)) {
      tsdbDebug("vgId:%d no need to truncate FSET %d, sFid %d, eFid %d", REPO_ID(pRepo), pSet->fid, sFid, eFid);
      if (tsdbApplyRtnOnFSet(pRepo, pSet, &(truncateH.rtn)) < 0) {
        return -1;
      }
      continue;
    }

#if 0  // TODO: How to make the decision? The test case should cover this scenario.
    if (TSDB_FSET_LEVEL(pSet) == TFS_MAX_LEVEL) {
      tsdbDebug("vgId:%d FSET %d on level %d, should not truncate", REPO_ID(pRepo), pSet->fid, TFS_MAX_LEVEL);
      tsdbUpdateDFileSet(REPO_FS(pRepo), pSet);
      continue;
    }
#endif

    if (tsdbTruncateFSet(&truncateH, pSet) < 0) {
      tsdbDestroyTruncateH(&truncateH);
      tsdbError("vgId:%d failed to truncate FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
      return -1;
    }
  }

  tsdbDestroyTruncateH(&truncateH);
  tsdbDebug("vgId:%d truncate TS data over", REPO_ID(pRepo));
  return 0;
}

static int tsdbTruncateFSet(STruncateH *pTruncateH, SDFileSet *pSet) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(pTruncateH);
  SDiskID    did = {0};

  tsdbDebug("vgId:%d start to truncate FSET %d on level %d id %d", REPO_ID(pRepo), pSet->fid, TSDB_FSET_LEVEL(pSet),
            TSDB_FSET_ID(pSet));

  if (tsdbTruncateFSetInit(pTruncateH, pSet) < 0) {
    return -1;
  }

  // Create new fset as truncated fset
  tfsAllocDisk(tsdbGetFidLevel(pSet->fid, &(pTruncateH->rtn)), &(did.level), &(did.id));
  if (did.level == TFS_UNDECIDED_LEVEL) {
    terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
    tsdbError("vgId:%d failed to truncate FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
    tsdbTruncateFSetEnd(pTruncateH);
    return -1;
  }

  tsdbInitDFileSet(TSDB_TRUNCATE_WSET(pTruncateH), did, REPO_ID(pRepo), TSDB_FSET_FID(pSet),
                   FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_LATEST_FSET_VER);

  if (tsdbCreateDFileSet(TSDB_TRUNCATE_WSET(pTruncateH), true) < 0) {
    tsdbError("vgId:%d failed to truncate FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
    tsdbTruncateFSetEnd(pTruncateH);
    return -1;
  }

  if (tsdbTruncateFSetImpl(pTruncateH) < 0) {
    tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(pTruncateH));
    tsdbRemoveDFileSet(TSDB_TRUNCATE_WSET(pTruncateH));
    tsdbTruncateFSetEnd(pTruncateH);
    return -1;
  }

  tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(pTruncateH));
  tsdbUpdateDFileSet(REPO_FS(pRepo), TSDB_TRUNCATE_WSET(pTruncateH));
  tsdbDebug("vgId:%d FSET %d truncate over", REPO_ID(pRepo), pSet->fid);

  tsdbTruncateFSetEnd(pTruncateH);
  return 0;
}

static int tsdbInitTruncateH(STruncateH *pTruncateH, STsdbRepo *pRepo) {
  STsdbCfg *pCfg = REPO_CFG(pRepo);

  memset(pTruncateH, 0, sizeof(*pTruncateH));

  TSDB_FSET_SET_CLOSED(TSDB_TRUNCATE_WSET(pTruncateH));

  tsdbGetRtnSnap(pRepo, &(pTruncateH->rtn));
  tsdbFSIterInit(&(pTruncateH->fsIter), REPO_FS(pRepo), TSDB_FS_ITER_FORWARD);

  if (tsdbInitReadH(&(pTruncateH->readh), pRepo) < 0) {
    return -1;
  }

  if (tsdbInitTruncateTblArray(pTruncateH) < 0) {
    tsdbDestroyTruncateH(pTruncateH);
    return -1;
  }

  pTruncateH->aBlkIdx = taosArrayInit(1024, sizeof(SBlockIdx));
  if (pTruncateH->aBlkIdx == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyTruncateH(pTruncateH);
    return -1;
  }

  pTruncateH->aSupBlk = taosArrayInit(1024, sizeof(SBlock));
  if (pTruncateH->aSupBlk == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyTruncateH(pTruncateH);
    return -1;
  }

  pTruncateH->pDataCols = tdNewDataCols(0, pCfg->maxRowsPerFileBlock);
  if (pTruncateH->pDataCols == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyTruncateH(pTruncateH);
    return -1;
  }

  return 0;
}

static void tsdbDestroyTruncateH(STruncateH *pTruncateH) {
  pTruncateH->pDataCols = tdFreeDataCols(pTruncateH->pDataCols);
  pTruncateH->aSupBlk = taosArrayDestroy(pTruncateH->aSupBlk);
  pTruncateH->aBlkIdx = taosArrayDestroy(pTruncateH->aBlkIdx);
  tsdbDestroyTruncateTblArray(pTruncateH);
  tsdbDestroyReadH(&(pTruncateH->readh));
  tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(pTruncateH));
}

static int tsdbInitTruncateTblArray(STruncateH *pTruncateH) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(pTruncateH);
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  if (tsdbRLockRepoMeta(pRepo) < 0) return -1;

  pTruncateH->tblArray = taosArrayInit(pMeta->maxTables, sizeof(STableTruncateH));
  if (pTruncateH->tblArray == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbUnlockRepoMeta(pRepo);
    return -1;
  }

  // Note here must start from 0
  for (int i = 0; i < pMeta->maxTables; ++i) {
    STableTruncateH ch = {0};
    if (pMeta->tables[i] != NULL) {
      tsdbRefTable(pMeta->tables[i]);
      ch.pTable = pMeta->tables[i];
    }

    if (taosArrayPush(pTruncateH->tblArray, &ch) == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      tsdbUnlockRepoMeta(pRepo);
      return -1;
    }
  }

  if (tsdbUnlockRepoMeta(pRepo) < 0) return -1;
  return 0;
}

static void tsdbDestroyTruncateTblArray(STruncateH *pTruncateH) {
  STableTruncateH *pTblHandle = NULL;

  if (pTruncateH->tblArray == NULL) return;

  for (size_t i = 0; i < taosArrayGetSize(pTruncateH->tblArray); ++i) {
    pTblHandle = (STableTruncateH *)taosArrayGet(pTruncateH->tblArray, i);
    if (pTblHandle->pTable) {
      tsdbUnRefTable(pTblHandle->pTable);
    }

    tfree(pTblHandle->pInfo);
  }

  pTruncateH->tblArray = taosArrayDestroy(pTruncateH->tblArray);
}

static int tsdbCacheFSetIndex(STruncateH *pTruncateH) {
  SReadH *pReadH = &(pTruncateH->readh);

  if (tsdbLoadBlockIdx(pReadH) < 0) {
    return -1;
  }

  size_t tblArraySize = taosArrayGetSize(pTruncateH->tblArray);
  for (size_t tid = 1; tid < tblArraySize; ++tid) {
    STableTruncateH *pTblHandle = (STableTruncateH *)taosArrayGet(pTruncateH->tblArray, tid);
    pTblHandle->pBlkIdx = NULL;

    if (pTblHandle->pTable == NULL) continue;
    if (tsdbSetReadTable(pReadH, pTblHandle->pTable) < 0) {
      return -1;
    }

    if (pReadH->pBlkIdx == NULL) continue;
    pTblHandle->bIndex = *(pReadH->pBlkIdx);
    pTblHandle->pBlkIdx = &(pTblHandle->bIndex);

    uint32_t originLen = 0;
    if (tsdbLoadBlockInfo(pReadH, (void **)(&(pTblHandle->pInfo)), &originLen) < 0) {
      return -1;
    }
  }

  return 0;
}

static int tsdbTruncateFSetInit(STruncateH *pTruncateH, SDFileSet *pSet) {
  taosArrayClear(pTruncateH->aBlkIdx);
  taosArrayClear(pTruncateH->aSupBlk);

  if (tsdbSetAndOpenReadFSet(&(pTruncateH->readh), pSet) < 0) {
    return -1;
  }

  if (tsdbCacheFSetIndex(pTruncateH) < 0) {
    tsdbCloseAndUnsetFSet(&(pTruncateH->readh));
    return -1;
  }

  return 0;
}

static void tsdbTruncateFSetEnd(STruncateH *pTruncateH) { tsdbCloseAndUnsetFSet(&(pTruncateH->readh)); }

static int tsdbTruncateFSetImpl(STruncateH *pTruncateH) {
  STsdbRepo *      pRepo = TSDB_TRUNCATE_REPO(pTruncateH);
  STruncateTblMsg *pMsg = (STruncateTblMsg *)pTruncateH->param;
  STsdbCfg *       pCfg = REPO_CFG(pRepo);
  SReadH *         pReadh = &(pTruncateH->readh);
  SBlockIdx        blkIdx = {0};
  void **          ppBuf = &(TSDB_TRUNCATE_BUF(pTruncateH));
  void **          ppCBuf = &(TSDB_TRUNCATE_COMP_BUF(pTruncateH));
  void **          ppExBuf = &(TSDB_TRUNCATE_EXBUF(pTruncateH));
  int              defaultRows = TSDB_DEFAULT_BLOCK_ROWS(pCfg->maxRowsPerFileBlock);

  taosArrayClear(pTruncateH->aBlkIdx);

  for (size_t tid = 1; tid < taosArrayGetSize(pTruncateH->tblArray); ++tid) {
    STableTruncateH *pTblHandle = (STableTruncateH *)taosArrayGet(pTruncateH->tblArray, tid);
    STSchema *       pSchema = NULL;

    if (pTblHandle->pTable == NULL || pTblHandle->pBlkIdx == NULL) continue;

    if ((pSchema = tsdbGetTableSchemaImpl(pTblHandle->pTable, true, true, -1, -1)) == NULL) {
      return -1;
    }

    taosArrayClear(pTruncateH->aSupBlk);

    if ((tdInitDataCols(pTruncateH->pDataCols, pSchema) < 0) || (tdInitDataCols(pReadh->pDCols[0], pSchema) < 0) ||
        (tdInitDataCols(pReadh->pDCols[1], pSchema) < 0)) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      tdFreeSchema(pSchema);
      return -1;
    }

    tdFreeSchema(pSchema);

    // Loop to truncate each block data
    for (int i = 0; i < pTblHandle->pBlkIdx->numOfBlocks; ++i) {
      SBlock *pBlock = pTblHandle->pInfo->blocks + i;

      // Load the block data
      if (tsdbLoadBlockData(pReadh, pBlock, pTblHandle->pInfo) < 0) {
        return -1;
      }

      // Merge pTruncateH->pDataCols and pReadh->pDCols[0] and write data to file
      if ((pTruncateH->pDataCols->numOfRows == 0) && (pBlock->numOfRows >= defaultRows)) {
        // if pTruncateH->pDataCols has no data and pBlock->numOfRows >= defaultRows, write data directly
        if (tsdbWriteBlockToRightFile(pTruncateH, pTblHandle->pTable, pReadh->pDCols[0], ppBuf, ppCBuf, ppExBuf) < 0) {
          return -1;
        }
      } else {
        int ridx = 0;
        while (true) {
          // no data left to merge anymore
          if ((pReadh->pDCols[0]->numOfRows - ridx) == 0) break;

          // min(data left, target space left)
          int rowsToMerge = MIN(pReadh->pDCols[0]->numOfRows - ridx, defaultRows - pTruncateH->pDataCols->numOfRows);

          tdMergeDataCols(pTruncateH->pDataCols, pReadh->pDCols[0], rowsToMerge, &ridx,
                          pCfg->update != TD_ROW_PARTIAL_UPDATE);

          if (pTruncateH->pDataCols->numOfRows < defaultRows) {
            // continue to read more blocks
            break;
          }

          if (tsdbWriteBlockToRightFile(pTruncateH, pTblHandle->pTable, pTruncateH->pDataCols, ppBuf, ppCBuf, ppExBuf) < 0) {
            return -1;
          }
          tdResetDataCols(pTruncateH->pDataCols);
        }
      }
    }

    if (pTruncateH->pDataCols->numOfRows > 0 &&
        tsdbWriteBlockToRightFile(pTruncateH, pTblHandle->pTable, pTruncateH->pDataCols, ppBuf, ppCBuf, ppExBuf) < 0) {
      return -1;
    }

    if (tsdbWriteBlockInfoImpl(TSDB_TRUNCATE_HEAD_FILE(pTruncateH), pTblHandle->pTable, pTruncateH->aSupBlk, NULL, ppBuf,
                               &blkIdx) < 0) {
      return -1;
    }

    if ((blkIdx.numOfBlocks > 0) && (taosArrayPush(pTruncateH->aBlkIdx, (const void *)(&blkIdx)) == NULL)) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  }

  if (tsdbWriteBlockIdx(TSDB_TRUNCATE_HEAD_FILE(pTruncateH), pTruncateH->aBlkIdx, ppBuf) < 0) {
    return -1;
  }

  return 0;
}

static int tsdbWriteBlockToRightFile(STruncateH *pTruncateH, STable *pTable, SDataCols *pDataCols, void **ppBuf,
                                     void **ppCBuf, void **ppExBuf) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(pTruncateH);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  SDFile *   pDFile = NULL;
  bool       isLast = false;
  SBlock     block = {0};

  ASSERT(pDataCols->numOfRows > 0);

  if (pDataCols->numOfRows < pCfg->minRowsPerFileBlock) {
    pDFile = TSDB_TRUNCATE_LAST_FILE(pTruncateH);
    isLast = true;
  } else {
    pDFile = TSDB_TRUNCATE_DATA_FILE(pTruncateH);
    isLast = false;
  }

  if (tsdbWriteBlockImpl(pRepo, pTable, pDFile,
                         isLast ? TSDB_TRUNCATE_SMAL_FILE(pTruncateH) : TSDB_TRUNCATE_SMAD_FILE(pTruncateH), pDataCols,
                         &block, isLast, true, ppBuf, ppCBuf, ppExBuf) < 0) {
    return -1;
  }

  if (taosArrayPush(pTruncateH->aSupBlk, (void *)(&block)) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}