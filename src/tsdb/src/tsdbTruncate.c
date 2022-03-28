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
#include "tsdbTruncate.h"

enum {
  TSDB_NO_TRUNCATE,
  TSDB_IN_TRUNCATE,
  TSDB_WAITING_TRUNCATE,
};

enum BlockSolve {
  BLOCK_READ = 0,
  BLOCK_MODIFY,
  BLOCK_DELETE
};

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
  SArray *   aSubBlk;
  SDataCols *pDCols;
  SControlDataInfo* pCtlInfo;
} STruncateH;


#define TSDB_TRUNCATE_WSET(ptru) (&((ptru)->wSet))
#define TSDB_TRUNCATE_REPO(ptru) TSDB_READ_REPO(&((ptru)->readh))
#define TSDB_TRUNCATE_HEAD_FILE(ptru) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(ptru), TSDB_FILE_HEAD)
#define TSDB_TRUNCATE_DATA_FILE(ptru) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(ptru), TSDB_FILE_DATA)
#define TSDB_TRUNCATE_LAST_FILE(ptru) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(ptru), TSDB_FILE_LAST)
#define TSDB_TRUNCATE_SMAD_FILE(ptru) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(ptru), TSDB_FILE_SMAD)
#define TSDB_TRUNCATE_SMAL_FILE(ptru) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(ptru), TSDB_FILE_SMAL)
#define TSDB_TRUNCATE_BUF(ptru) TSDB_READ_BUF(&((ptru)->readh))
#define TSDB_TRUNCATE_COMP_BUF(ptru) TSDB_READ_COMP_BUF(&((ptru)->readh))
#define TSDB_TRUNCATE_EXBUF(ptru) TSDB_READ_EXBUF(&((ptru)->readh))


static void  tsdbStartTruncate(STsdbRepo *pRepo);
static void  tsdbEndTruncate(STsdbRepo *pRepo, int eno);
static int   tsdbTruncateMeta(STsdbRepo *pRepo);
static int   tsdbTruncateTSData(STsdbRepo *pRepo, SControlDataInfo* pCtlInfo);
static int   tsdbFSetTruncate(STruncateH *ptru, SDFileSet *pSet);
static int   tsdbFSetDelete(STruncateH *ptru, SDFileSet *pSet);
static int   tsdbInitTruncateH(STruncateH *ptru, STsdbRepo *pRepo);
static void  tsdbDestroyTruncateH(STruncateH *ptru);
static int   tsdbInitTruncateTblArray(STruncateH *ptru);
static void  tsdbDestroyTruncateTblArray(STruncateH *ptru);
static int   tsdbCacheFSetIndex(STruncateH *ptru);
static int   tsdbTruncateCache(STsdbRepo *pRepo, void *param);
static int   tsdbFSetInit(STruncateH *ptru, SDFileSet *pSet);
static void  tsdbTruncateFSetEnd(STruncateH *ptru);
static int   tsdbTruncateFSetImpl(STruncateH *ptru);
static int   tsdbFSetDeleteImpl(STruncateH *ptru);
static int   tsdbBlockSolve(STruncateH *ptru, SBlock *pBlock);
static int   tsdbWriteBlockToFile(STruncateH *ptru, STable *pTable, SDataCols *pDCols, void **ppBuf,
                                       void **ppCBuf, void **ppExBuf, SBlock * pBlock);
static int   tsdbTruncateImplCommon(STsdbRepo *pRepo, SControlDataInfo* pCtlInfo);


// delete
int tsdbControlDelete(STsdbRepo* pRepo, SControlDataInfo* pCtlInfo) {
  int ret = TSDB_CODE_SUCCESS;

  if(pCtlInfo->pRsp) {
    pCtlInfo->pRsp->affectedRows = htonl(23);
    pCtlInfo->pRsp->code = ret;
  }

  return tsdbTruncateImplCommon(pRepo, pCtlInfo);
}

static int tsdbTruncateImplCommon(STsdbRepo *pRepo, SControlDataInfo* pCtlInfo) {
  int32_t code = 0;
  // Step 1: check and clear cache
  if ((code = tsdbTruncateCache(pRepo, pCtlInfo)) != 0) {
    pRepo->code = terrno;
    tsem_post(&(pRepo->readyToCommit));
    tsdbInfo("vgId:%d failed to truncate since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  // Step 2: truncate and rebuild DFileSets
  // Check if there are files in TSDB FS to truncate
  if ((REPO_FS(pRepo)->cstatus->pmf == NULL) || (taosArrayGetSize(REPO_FS(pRepo)->cstatus->df) <= 0)) {
    pRepo->truncateState = TSDB_NO_TRUNCATE;
    tsem_post(&(pRepo->readyToCommit));
    tsdbInfo("vgId:%d truncate over, no meta or data file", REPO_ID(pRepo));
    return -1;
  }

  tsdbStartTruncate(pRepo);

  if (tsdbTruncateMeta(pRepo) < 0) {
    tsdbError("vgId:%d failed to truncate META data since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  if (tsdbTruncateTSData(pRepo, pCtlInfo) < 0) {
    tsdbError("vgId:%d failed to truncate TS data since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  tsdbEndTruncate(pRepo, TSDB_CODE_SUCCESS);
  return TSDB_CODE_SUCCESS;

_err:
  pRepo->code = terrno;
  tsdbEndTruncate(pRepo, terrno);
  return -1;
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

static int tsdbTruncateTSData(STsdbRepo *pRepo, SControlDataInfo* pCtlInfo) {
  STsdbCfg *       pCfg = REPO_CFG(pRepo);
  STruncateH       truncateH = {0};
  SDFileSet *      pSet = NULL;

  tsdbDebug("vgId:%d start to truncate TS data for %d", REPO_ID(pRepo), pCtlInfo->ctlData.tids[0]);

  if (tsdbInitTruncateH(&truncateH, pRepo) < 0) {
    return -1;
  }

  truncateH.pCtlInfo = pCtlInfo;
  STimeWindow win = pCtlInfo->ctlData.win;

  int sFid = TSDB_KEY_FID(win.skey, pCfg->daysPerFile, pCfg->precision);
  int eFid = TSDB_KEY_FID(win.ekey, pCfg->daysPerFile, pCfg->precision);
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
    
    if (pCtlInfo->ctlData.command == CMD_TRUNCATE) {
      if (tsdbFSetTruncate(&truncateH, pSet) < 0) {
        tsdbDestroyTruncateH(&truncateH);
        tsdbError("vgId:%d failed to truncate table in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
        return -1;
      }
    } else if (pCtlInfo->ctlData.command == CMD_DELETE_DATA) {
      if (tsdbFSetDelete(&truncateH, pSet) < 0) {
        tsdbDestroyTruncateH(&truncateH);
        tsdbError("vgId:%d failed to truncate data in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
        return -1;
      }
    } else {
      ASSERT(false);
    }
    
  }

  tsdbDestroyTruncateH(&truncateH);
  tsdbDebug("vgId:%d truncate TS data over", REPO_ID(pRepo));
  return 0;
}

static int tsdbFSetDelete(STruncateH *ptru, SDFileSet *pSet) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(ptru);
  SDiskID    did = {0};

  tsdbDebug("vgId:%d start to truncate data in FSET %d on level %d id %d", REPO_ID(pRepo), pSet->fid,
            TSDB_FSET_LEVEL(pSet), TSDB_FSET_ID(pSet));

  if (tsdbFSetInit(ptru, pSet) < 0) {
    return -1;
  }

  // Create new fset as deleted fset
  tfsAllocDisk(tsdbGetFidLevel(pSet->fid, &(ptru->rtn)), &(did.level), &(did.id));
  if (did.level == TFS_UNDECIDED_LEVEL) {
    terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
    tsdbError("vgId:%d failed to truncate data in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
    tsdbTruncateFSetEnd(ptru);
    return -1;
  }

  tsdbInitDFileSet(TSDB_TRUNCATE_WSET(ptru), did, REPO_ID(pRepo), TSDB_FSET_FID(pSet),
                   FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_LATEST_FSET_VER);

  if (tsdbCreateDFileSet(TSDB_TRUNCATE_WSET(ptru), true) < 0) {
    tsdbError("vgId:%d failed to truncate data in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
    tsdbTruncateFSetEnd(ptru);
    return -1;
  }

  if (tsdbFSetDeleteImpl(ptru) < 0) {
    tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(ptru));
    tsdbRemoveDFileSet(TSDB_TRUNCATE_WSET(ptru));
    tsdbTruncateFSetEnd(ptru);
    return -1;
  }

  tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(ptru));
  tsdbUpdateDFileSet(REPO_FS(pRepo), TSDB_TRUNCATE_WSET(ptru));
  tsdbDebug("vgId:%d FSET %d truncate data over", REPO_ID(pRepo), pSet->fid);

  tsdbTruncateFSetEnd(ptru);
  return 0;
}

static int tsdbFSetTruncate(STruncateH *ptru, SDFileSet *pSet) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(ptru);
  SDiskID    did = {0};
  SDFileSet *pWSet = TSDB_TRUNCATE_WSET(ptru);

  tsdbDebug("vgId:%d start to truncate table in FSET %d on level %d id %d", REPO_ID(pRepo), pSet->fid,
            TSDB_FSET_LEVEL(pSet), TSDB_FSET_ID(pSet));

  if (tsdbFSetInit(ptru, pSet) < 0) {
    return -1;
  }

  // Create new fset as truncated fset
  tfsAllocDisk(tsdbGetFidLevel(pSet->fid, &(ptru->rtn)), &(did.level), &(did.id));
  if (did.level == TFS_UNDECIDED_LEVEL) {
    terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
    tsdbError("vgId:%d failed to truncate table in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
    tsdbTruncateFSetEnd(ptru);
    return -1;
  }

  // Only .head is created, use original .data/.last/.smad/.smal
  tsdbInitDFileSetEx(pWSet, pSet);
  pWSet->state = 0;
  SDFile *pHeadFile = TSDB_DFILE_IN_SET(pWSet, TSDB_FILE_HEAD);
  tsdbInitDFile(pHeadFile, did, REPO_ID(pRepo), TSDB_FSET_FID(pSet), FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_FILE_HEAD);

  if (tsdbCreateDFile(pHeadFile, true, TSDB_FILE_HEAD) < 0) {
    tsdbError("vgId:%d failed to truncate table in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
    tsdbCloseDFile(pHeadFile);
    tsdbRemoveDFile(pHeadFile);
    return -1;
  }

  tsdbCloseDFile(pHeadFile);

  if (tsdbOpenDFileSet(pWSet, O_RDWR) < 0) {
    tsdbError("vgId:%d failed to open file set %d since %s", REPO_ID(pRepo), TSDB_FSET_FID(pWSet), tstrerror(terrno));
    return -1;
  }

  if (tsdbTruncateFSetImpl(ptru) < 0) {
    tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(ptru));
    tsdbRemoveDFileSet(TSDB_TRUNCATE_WSET(ptru));
    tsdbTruncateFSetEnd(ptru);
    return -1;
  }

  tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(ptru));
  tsdbUpdateDFileSet(REPO_FS(pRepo), TSDB_TRUNCATE_WSET(ptru));
  tsdbDebug("vgId:%d FSET %d truncate table over", REPO_ID(pRepo), pSet->fid);

  tsdbTruncateFSetEnd(ptru);
  return 0;
}

static int tsdbInitTruncateH(STruncateH *ptru, STsdbRepo *pRepo) {
  STsdbCfg *pCfg = REPO_CFG(pRepo);

  memset(ptru, 0, sizeof(*ptru));

  TSDB_FSET_SET_CLOSED(TSDB_TRUNCATE_WSET(ptru));

  tsdbGetRtnSnap(pRepo, &(ptru->rtn));
  tsdbFSIterInit(&(ptru->fsIter), REPO_FS(pRepo), TSDB_FS_ITER_FORWARD);

  if (tsdbInitReadH(&(ptru->readh), pRepo) < 0) {
    return -1;
  }

  if (tsdbInitTruncateTblArray(ptru) < 0) {
    tsdbDestroyTruncateH(ptru);
    return -1;
  }

  ptru->aBlkIdx = taosArrayInit(1024, sizeof(SBlockIdx));
  if (ptru->aBlkIdx == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyTruncateH(ptru);
    return -1;
  }

  ptru->aSupBlk = taosArrayInit(1024, sizeof(SBlock));
  if (ptru->aSupBlk == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyTruncateH(ptru);
    return -1;
  }

  ptru->aSubBlk = taosArrayInit(20, sizeof(SBlock));
  if (ptru->aSubBlk == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyTruncateH(ptru);
    return -1;
  }

  ptru->pDCols = tdNewDataCols(0, pCfg->maxRowsPerFileBlock);
  if (ptru->pDCols == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyTruncateH(ptru);
    return -1;
  }

  return 0;
}

static void tsdbDestroyTruncateH(STruncateH *ptru) {
  ptru->pDCols = tdFreeDataCols(ptru->pDCols);
  ptru->aSupBlk = taosArrayDestroy(&ptru->aSupBlk);
  ptru->aSubBlk = taosArrayDestroy(&ptru->aSubBlk);
  ptru->aBlkIdx = taosArrayDestroy(&ptru->aBlkIdx);
  tsdbDestroyTruncateTblArray(ptru);
  tsdbDestroyReadH(&(ptru->readh));
  tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(ptru));
}

// init tbl array with pRepo->meta
static int tsdbInitTruncateTblArray(STruncateH *ptru) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(ptru);
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  if (tsdbRLockRepoMeta(pRepo) < 0) return -1;

  ptru->tblArray = taosArrayInit(pMeta->maxTables, sizeof(STableTruncateH));
  if (ptru->tblArray == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbUnlockRepoMeta(pRepo);
    return -1;
  }

  // Note here must start from 0
  for (int i = 0; i < pMeta->maxTables; ++i) {
    STableTruncateH tbl = {0};
    if (pMeta->tables[i] != NULL) {
      tsdbRefTable(pMeta->tables[i]);
      tbl.pTable = pMeta->tables[i];
    }

    if (taosArrayPush(ptru->tblArray, &tbl) == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      tsdbUnlockRepoMeta(pRepo);
      return -1;
    }
  }

  if (tsdbUnlockRepoMeta(pRepo) < 0) return -1;
  return 0;
}

static void tsdbDestroyTruncateTblArray(STruncateH *ptru) {
  STableTruncateH *pItem = NULL;

  if (ptru->tblArray == NULL) return;

  for (size_t i = 0; i < taosArrayGetSize(ptru->tblArray); ++i) {
    pItem = (STableTruncateH *)taosArrayGet(ptru->tblArray, i);
    if (pItem->pTable) {
      tsdbUnRefTable(pItem->pTable);
    }

    tfree(pItem->pInfo);
  }

  ptru->tblArray = taosArrayDestroy(&ptru->tblArray);
}

static int tsdbCacheFSetIndex(STruncateH *ptru) {
  SReadH *pReadH = &(ptru->readh);

  if (tsdbLoadBlockIdx(pReadH) < 0) {
    return -1;
  }

  size_t cnt = taosArrayGetSize(ptru->tblArray);
  for (size_t tid = 1; tid < cnt; ++tid) {
    STableTruncateH *pItem = (STableTruncateH *)taosArrayGet(ptru->tblArray, tid);
    pItem->pBlkIdx = NULL;

    if (pItem->pTable == NULL) 
      continue;
    if (tsdbSetReadTable(pReadH, pItem->pTable) < 0)
      return -1;
    if (pReadH->pBlkIdx == NULL) 
      continue;
    pItem->bIndex = *(pReadH->pBlkIdx);
    pItem->pBlkIdx = &(pItem->bIndex);

    uint32_t originLen = 0;
    if (tsdbLoadBlockInfo(pReadH, (void **)(&(pItem->pInfo)), &originLen) < 0) {
      return -1;
    }
  }

  return 0;
}

static int tsdbFSetInit(STruncateH *ptru, SDFileSet *pSet) {
  taosArrayClear(ptru->aBlkIdx);
  taosArrayClear(ptru->aSupBlk);

  // open
  if (tsdbSetAndOpenReadFSet(&(ptru->readh), pSet) < 0) {
    return -1;
  }

  // load index to cache
  if (tsdbCacheFSetIndex(ptru) < 0) {
    tsdbCloseAndUnsetFSet(&(ptru->readh));
    return -1;
  }

  return 0;
}

static void tsdbTruncateFSetEnd(STruncateH *ptru) { tsdbCloseAndUnsetFSet(&(ptru->readh)); }


static int32_t tsdbFilterDataCols(STruncateH *ptru, SDataCols *pSrcDCols) {
  SDataCols * pDstDCols = ptru->pDCols;
  SControlData* pCtlData = &ptru->pCtlInfo->ctlData;

  tdResetDataCols(pDstDCols);
  pDstDCols->maxCols = pSrcDCols->maxCols;
  pDstDCols->maxPoints = pSrcDCols->maxPoints;
  pDstDCols->numOfCols = pSrcDCols->numOfCols;
  pDstDCols->sversion = pSrcDCols->sversion;

  for (int i = 0; i < pSrcDCols->numOfRows; ++i) {
    int64_t tsKey = *(int64_t *)tdGetColDataOfRow(pSrcDCols->cols, i);
    if ((tsKey >= pCtlData->win.skey) && (tsKey <= pCtlData->win.ekey)) {
      printf("tsKey %" PRId64 " is filtered\n", tsKey);
      continue;
    }
    for (int j = 0; j < pSrcDCols->numOfCols; ++j) {
      if (pSrcDCols->cols[j].len > 0 || pDstDCols->cols[j].len > 0) {
        dataColAppendVal(pDstDCols->cols + j, tdGetColDataOfRow(pSrcDCols->cols + j, i), pDstDCols->numOfRows,
                         pDstDCols->maxPoints, 0);
      }
    }
    ++ pDstDCols->numOfRows;
  }

  return 0;
}

// table in delete list
bool tableInDel(STruncateH* ptru, int32_t tid) {
  for (int32_t i = 0; i < ptru->pCtlInfo->ctlData.tnum; i++) {
    if (tid == ptru->pCtlInfo->ctlData.tids[i])
      return true;
  }

  return false;
}

static int tsdbTruncateFSetImpl(STruncateH *ptru) {
  STsdbRepo *      pRepo = TSDB_TRUNCATE_REPO(ptru);
  // SReadH *         pReadh = &(ptru->readh);
  SBlockIdx *      pBlkIdx = NULL;
  void **          ppBuf = &(TSDB_TRUNCATE_BUF(ptru));
  // void **          ppCBuf = &(TSDB_TRUNCATE_COMP_BUF(ptru));
  // void **          ppExBuf = &(TSDB_TRUNCATE_EXBUF(ptru));

  taosArrayClear(ptru->aBlkIdx);

  for (size_t tid = 1; tid < taosArrayGetSize(ptru->tblArray); ++tid) {
    STableTruncateH *pItem = (STableTruncateH *)taosArrayGet(ptru->tblArray, tid);
    pBlkIdx = pItem->pBlkIdx;

    if (pItem->pTable == NULL || pItem->pBlkIdx == NULL) continue;

    taosArrayClear(ptru->aSupBlk);

    if (!tableInDel(ptru, tid)) {
      if ((pBlkIdx->numOfBlocks > 0) && (taosArrayPush(ptru->aBlkIdx, (const void *)(pBlkIdx)) == NULL)) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }
    } else {
      // Loop to mark delete flag for each block data
      tsdbDebug("vgId:%d tid %ld matched to truncate", REPO_ID(pRepo), tid);
      // for (int i = 0; i < pItem->pBlkIdx->numOfBlocks; ++i) {
      //   SBlock *pBlock = pItem->pInfo->blocks + i;

      //   if (tsdbWriteBlockToFile(ptru, pItem->pTable, ptru->pDCols, ppBuf, ppCBuf, ppExBuf) <
      //   0) {
      //     return -1;
      //   }
      // }
    }
  }

  if (tsdbWriteBlockIdx(TSDB_TRUNCATE_HEAD_FILE(ptru), ptru->aBlkIdx, ppBuf) < 0) {
    return -1;
  }

  return 0;
}

// if pBlock is border block return true else return false
static int tsdbBlockSolve(STruncateH *ptru, SBlock *pBlock) {
  // delete window
  STimeWindow* pdel = &ptru->pCtlInfo->ctlData.win;

  // do nothing for no delete
  if(pBlock->keyFirst > pdel->ekey || pBlock->keyLast < pdel->skey)
    return BLOCK_READ;

  // border block
  if(pBlock->keyFirst <= pdel->skey || pBlock->keyLast >= pdel->ekey)
    return BLOCK_MODIFY;

  // need del
  return BLOCK_DELETE;
}

// remove del block from pBlockInfo
int tsdbRemoveDelBlocks(STruncateH *ptru, STableTruncateH * pItem) {
  // loop
  int numOfBlocks = pItem->pBlkIdx->numOfBlocks;
  int from = -1;
  int delAll = 0;

  for (int i = numOfBlocks - 1; i >= 0; --i) {
    SBlock *pBlock = pItem->pInfo->blocks + i;
    int32_t solve = tsdbBlockSolve(ptru, pBlock);
    if (solve == BLOCK_DELETE) {
      if (from == -1)
         from = i;
    } else {
      if(from != -1) {
        // do del
        int delCnt = from - i;
        memmove(pItem->pInfo->blocks + i + 1, pItem->pInfo->blocks + i + 1 + delCnt, (numOfBlocks - (i+1) - delCnt) * sizeof(SBlock));
        delAll += delCnt;
        numOfBlocks  -= delCnt;
        from = -1;
      }
    }
  }

  if(from != -1) {
    int delCnt = from;
    memmove(pItem->pInfo->blocks, pItem->pInfo->blocks + delCnt, (numOfBlocks - delCnt) * sizeof(SBlock));
    delAll += delCnt;
    numOfBlocks  -= delCnt;
  }

  // set value
  pItem->pBlkIdx->numOfBlocks = numOfBlocks;

  return delAll;
}

static void tsdbAddBlock(STruncateH *ptru, STableTruncateH *pItem, SBlock *pBlock) {
  taosArrayPush(ptru->aSupBlk, (const void *)pBlock);
  // have sub block
  if (pBlock->numOfSubBlocks > 1) {
    SBlock *jBlock = POINTER_SHIFT(pItem->pInfo, pBlock->offset);;
    for (int j = 0; j < pBlock->numOfSubBlocks; j++) {
      taosArrayPush(ptru->aSubBlk, (const void *)jBlock++);
    }
  }
}

// need modify blocks
static int tsdbModifyBlocks(STruncateH *ptru, STableTruncateH *pItem) {
  SReadH *   pReadh  = &(ptru->readh);
  void **    ppBuf   = &(TSDB_TRUNCATE_BUF(ptru));
  void **    ppCBuf  = &(TSDB_TRUNCATE_COMP_BUF(ptru));
  void **    ppExBuf = &(TSDB_TRUNCATE_EXBUF(ptru));
  STSchema  *pSchema = NULL;
  SBlockIdx  blkIdx  = {0};

  // get pSchema for del table
  if ((pSchema = tsdbGetTableSchemaImpl(pItem->pTable, true, true, -1, -1)) == NULL) {
    return -1;
  }
  
  if ((tdInitDataCols(ptru->pDCols, pSchema) < 0) || (tdInitDataCols(pReadh->pDCols[0], pSchema) < 0) ||
      (tdInitDataCols(pReadh->pDCols[1], pSchema) < 0)) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tdFreeSchema(pSchema);
    return -1;
  }
  tdFreeSchema(pSchema);

  // delete block
  tsdbRemoveDelBlocks(ptru, pItem);
  if(pItem->pBlkIdx->numOfBlocks == 0) {
    // all blocks were deleted
    return TSDB_CODE_SUCCESS;
  }

  taosArrayClear(ptru->aSupBlk);
  taosArrayClear(ptru->aSubBlk);

  // Loop to truncate each block data
  for (int i = 0; i < pItem->pBlkIdx->numOfBlocks; ++i) {
    SBlock *pBlock = pItem->pInfo->blocks + i;
    int32_t solve = tsdbBlockSolve(ptru, pBlock);
    if (solve == BLOCK_READ) {
      tsdbAddBlock(ptru, pItem, pBlock);
      continue;
    } 

    // border block need load to delete no-use data
    if (tsdbLoadBlockData(pReadh, pBlock, pItem->pInfo) < 0) {
      return -1;
    }

    tsdbFilterDataCols(ptru, pReadh->pDCols[0]);    
    if (ptru->pDCols->numOfRows <= 0) {
      continue;
    }

    SBlock newBlock = {0};
    if (tsdbWriteBlockToFile(ptru, pItem->pTable, ptru->pDCols, ppBuf, ppCBuf, ppExBuf, &newBlock) < 0) {
      return -1;
    }

    // add new block to info
    tsdbAddBlock(ptru, pItem, &newBlock);
  }

  // write block info for each table
  if (tsdbWriteBlockInfoImpl(TSDB_TRUNCATE_HEAD_FILE(ptru), pItem->pTable, ptru->aSupBlk, ptru->aSubBlk,
                              ppBuf, &blkIdx) < 0) {
    return -1;
  }

  // each table's blkIdx 
  if (blkIdx.numOfBlocks > 0 && taosArrayPush(ptru->aBlkIdx, (const void *)(&blkIdx)) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

// keep intact blocks info and write to head file then save offset to blkIdx
static int tsdbKeepIntactBlocks(STruncateH *ptru, STableTruncateH * pItem) {
  // init
  SBlockIdx  blkIdx  = {0};
  taosArrayClear(ptru->aSupBlk);
  taosArrayClear(ptru->aSubBlk);

  for (int32_t i = 0; i < pItem->pBlkIdx->numOfBlocks; i++) {
    SBlock *pBlock = pItem->pInfo->blocks + i;
    tsdbAddBlock(ptru, pItem, pBlock);
  }

  // write block info for one table
  void **ppBuf = &(TSDB_TRUNCATE_BUF(ptru));
  int32_t ret  = tsdbWriteBlockInfoImpl(TSDB_TRUNCATE_HEAD_FILE(ptru), pItem->pTable, ptru->aSupBlk, 
                                       ptru->aSubBlk, ppBuf, &blkIdx);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  // each table's blkIdx 
  if (blkIdx.numOfBlocks > 0 && taosArrayPush(ptru->aBlkIdx, (const void *)&blkIdx) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return ret;
}

static int tsdbFSetDeleteImpl(STruncateH *ptru) {
  void **   ppBuf  = &(TSDB_TRUNCATE_BUF(ptru));
  int32_t   ret    = TSDB_CODE_SUCCESS;

  // 1.INIT
  taosArrayClear(ptru->aBlkIdx);

  for (size_t tid = 1; tid < taosArrayGetSize(ptru->tblArray); ++tid) {
    STableTruncateH *pItem = (STableTruncateH *)taosArrayGet(ptru->tblArray, tid);

    // no table in this tid position
    if (pItem->pTable == NULL || pItem->pBlkIdx == NULL)
      continue;

    // 2.WRITE INFO OF EACH TABLE BLOCK INFO TO HEAD FILE
    if (tableInDel(ptru, tid)) {
      // modify blocks info and write to head file then save offset to blkIdx
      ret = tsdbModifyBlocks(ptru, pItem);
    } else {
      // keep intact blocks info and write to head file then save offset to blkIdx
      ret = tsdbKeepIntactBlocks(ptru, pItem);
    }
    if (ret != TSDB_CODE_SUCCESS)
      return ret;
  } // tid for

  // 3.WRITE INDEX OF ALL TABLE'S BLOCK TO HEAD FILE
  if (tsdbWriteBlockIdx(TSDB_TRUNCATE_HEAD_FILE(ptru), ptru->aBlkIdx, ppBuf) < 0) {
    return -1;
  }

  return ret;
}

static int tsdbWriteBlockToFile(STruncateH *ptru, STable *pTable, SDataCols *pDCols, void **ppBuf,
                                     void **ppCBuf, void **ppExBuf, SBlock *pBlock) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(ptru);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  SDFile *   pDFile = NULL;
  bool       isLast = false;

  ASSERT(pDCols->numOfRows > 0);

  if (pDCols->numOfRows < pCfg->minRowsPerFileBlock) {
    pDFile = TSDB_TRUNCATE_LAST_FILE(ptru);
    isLast = true;
  } else {
    pDFile = TSDB_TRUNCATE_DATA_FILE(ptru);
    isLast = false;
  }

  if (tsdbWriteBlockImpl(pRepo, pTable, pDFile,
                         isLast ? TSDB_TRUNCATE_SMAL_FILE(ptru) : TSDB_TRUNCATE_SMAD_FILE(ptru), pDCols,
                         pBlock, isLast, true, ppBuf, ppCBuf, ppExBuf) < 0) {
    return -1;
  }

  return 0;
}
