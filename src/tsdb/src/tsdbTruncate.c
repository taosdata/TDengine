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
  SDataCols *pDCols;
  SControlDataInfo* pCtlInfo;
} STruncateH;

#define TSDB_TRUNCATE_WSET(prh) (&((prh)->wSet))
#define TSDB_TRUNCATE_REPO(prh) TSDB_READ_REPO(&((prh)->readh))
#define TSDB_TRUNCATE_HEAD_FILE(prh) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(prh), TSDB_FILE_HEAD)
#define TSDB_TRUNCATE_DATA_FILE(prh) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(prh), TSDB_FILE_DATA)
#define TSDB_TRUNCATE_LAST_FILE(prh) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(prh), TSDB_FILE_LAST)
#define TSDB_TRUNCATE_SMAD_FILE(prh) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(prh), TSDB_FILE_SMAD)
#define TSDB_TRUNCATE_SMAL_FILE(prh) TSDB_DFILE_IN_SET(TSDB_TRUNCATE_WSET(prh), TSDB_FILE_SMAL)
#define TSDB_TRUNCATE_BUF(prh) TSDB_READ_BUF(&((prh)->readh))
#define TSDB_TRUNCATE_COMP_BUF(prh) TSDB_READ_COMP_BUF(&((prh)->readh))
#define TSDB_TRUNCATE_EXBUF(prh) TSDB_READ_EXBUF(&((prh)->readh))


static void  tsdbStartTruncate(STsdbRepo *pRepo);
static void  tsdbEndTruncate(STsdbRepo *pRepo, int eno);
static int   tsdbTruncateMeta(STsdbRepo *pRepo);
static int   tsdbTruncateTSData(STsdbRepo *pRepo, SControlDataInfo* pCtlInfo);
static int   tsdbTruncateFSet(STruncateH *prh, SDFileSet *pSet);
static int   tsdbDeleteFSet(STruncateH *prh, SDFileSet *pSet);
static int   tsdbInitTruncateH(STruncateH *prh, STsdbRepo *pRepo);
static void  tsdbDestroyTruncateH(STruncateH *prh);
static int   tsdbInitTruncateTblArray(STruncateH *prh);
static void  tsdbDestroyTruncateTblArray(STruncateH *prh);
static int   tsdbCacheFSetIndex(STruncateH *prh);
static int   tsdbTruncateCache(STsdbRepo *pRepo, void *param);
static int   tsdbTruncateFSetInit(STruncateH *prh, SDFileSet *pSet);
static void  tsdbTruncateFSetEnd(STruncateH *prh);
static int   tsdbTruncateFSetImpl(STruncateH *prh);
static int   tsdbDeleteFSetImpl(STruncateH *prh);
static bool  tsdbBlockInterleaved(STruncateH *prh, SBlock *pBlock);
static int   tsdbWriteBlockToRightFile(STruncateH *prh, STable *pTable, SDataCols *pDCols, void **ppBuf,
                                       void **ppCBuf, void **ppExBuf);
static int  tsdbTruncateImplCommon(STsdbRepo *pRepo, SControlDataInfo* pCtlInfo);



enum {
  TSDB_NO_TRUNCATE,
  TSDB_IN_TRUNCATE,
  TSDB_WAITING_TRUNCATE,
};

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

  tsdbDebug("vgId:%d start to truncate TS data for %" PRIu64, REPO_ID(pRepo), pCtlInfo->uid);

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
      if (tsdbTruncateFSet(&truncateH, pSet) < 0) {
        tsdbDestroyTruncateH(&truncateH);
        tsdbError("vgId:%d failed to truncate table in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
        return -1;
      }
    } else if (pCtlInfo->ctlData.command == CMD_DELETE_DATA) {
      if (tsdbDeleteFSet(&truncateH, pSet) < 0) {
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

static int tsdbDeleteFSet(STruncateH *prh, SDFileSet *pSet) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(prh);
  SDiskID    did = {0};

  tsdbDebug("vgId:%d start to truncate data in FSET %d on level %d id %d", REPO_ID(pRepo), pSet->fid,
            TSDB_FSET_LEVEL(pSet), TSDB_FSET_ID(pSet));

  if (tsdbTruncateFSetInit(prh, pSet) < 0) {
    return -1;
  }

  // Create new fset as deleted fset
  tfsAllocDisk(tsdbGetFidLevel(pSet->fid, &(prh->rtn)), &(did.level), &(did.id));
  if (did.level == TFS_UNDECIDED_LEVEL) {
    terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
    tsdbError("vgId:%d failed to truncate data in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
    tsdbTruncateFSetEnd(prh);
    return -1;
  }

  tsdbInitDFileSet(TSDB_TRUNCATE_WSET(prh), did, REPO_ID(pRepo), TSDB_FSET_FID(pSet),
                   FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_LATEST_FSET_VER);

  if (tsdbCreateDFileSet(TSDB_TRUNCATE_WSET(prh), true) < 0) {
    tsdbError("vgId:%d failed to truncate data in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
    tsdbTruncateFSetEnd(prh);
    return -1;
  }

  if (tsdbDeleteFSetImpl(prh) < 0) {
    tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(prh));
    tsdbRemoveDFileSet(TSDB_TRUNCATE_WSET(prh));
    tsdbTruncateFSetEnd(prh);
    return -1;
  }

  tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(prh));
  tsdbUpdateDFileSet(REPO_FS(pRepo), TSDB_TRUNCATE_WSET(prh));
  tsdbDebug("vgId:%d FSET %d truncate data over", REPO_ID(pRepo), pSet->fid);

  tsdbTruncateFSetEnd(prh);
  return 0;
}

static int tsdbTruncateFSet(STruncateH *prh, SDFileSet *pSet) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(prh);
  SDiskID    did = {0};
  SDFileSet *pWSet = TSDB_TRUNCATE_WSET(prh);

  tsdbDebug("vgId:%d start to truncate table in FSET %d on level %d id %d", REPO_ID(pRepo), pSet->fid,
            TSDB_FSET_LEVEL(pSet), TSDB_FSET_ID(pSet));

  if (tsdbTruncateFSetInit(prh, pSet) < 0) {
    return -1;
  }

  // Create new fset as truncated fset
  tfsAllocDisk(tsdbGetFidLevel(pSet->fid, &(prh->rtn)), &(did.level), &(did.id));
  if (did.level == TFS_UNDECIDED_LEVEL) {
    terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
    tsdbError("vgId:%d failed to truncate table in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
    tsdbTruncateFSetEnd(prh);
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

  if (tsdbTruncateFSetImpl(prh) < 0) {
    tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(prh));
    tsdbRemoveDFileSet(TSDB_TRUNCATE_WSET(prh));
    tsdbTruncateFSetEnd(prh);
    return -1;
  }

  tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(prh));
  tsdbUpdateDFileSet(REPO_FS(pRepo), TSDB_TRUNCATE_WSET(prh));
  tsdbDebug("vgId:%d FSET %d truncate table over", REPO_ID(pRepo), pSet->fid);

  tsdbTruncateFSetEnd(prh);
  return 0;
}

static int tsdbInitTruncateH(STruncateH *prh, STsdbRepo *pRepo) {
  STsdbCfg *pCfg = REPO_CFG(pRepo);

  memset(prh, 0, sizeof(*prh));

  TSDB_FSET_SET_CLOSED(TSDB_TRUNCATE_WSET(prh));

  tsdbGetRtnSnap(pRepo, &(prh->rtn));
  tsdbFSIterInit(&(prh->fsIter), REPO_FS(pRepo), TSDB_FS_ITER_FORWARD);

  if (tsdbInitReadH(&(prh->readh), pRepo) < 0) {
    return -1;
  }

  if (tsdbInitTruncateTblArray(prh) < 0) {
    tsdbDestroyTruncateH(prh);
    return -1;
  }

  prh->aBlkIdx = taosArrayInit(1024, sizeof(SBlockIdx));
  if (prh->aBlkIdx == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyTruncateH(prh);
    return -1;
  }

  prh->aSupBlk = taosArrayInit(1024, sizeof(SBlock));
  if (prh->aSupBlk == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyTruncateH(prh);
    return -1;
  }

  prh->pDCols = tdNewDataCols(0, pCfg->maxRowsPerFileBlock);
  if (prh->pDCols == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyTruncateH(prh);
    return -1;
  }

  return 0;
}

static void tsdbDestroyTruncateH(STruncateH *prh) {
  prh->pDCols = tdFreeDataCols(prh->pDCols);
  prh->aSupBlk = taosArrayDestroy(&prh->aSupBlk);
  prh->aBlkIdx = taosArrayDestroy(&prh->aBlkIdx);
  tsdbDestroyTruncateTblArray(prh);
  tsdbDestroyReadH(&(prh->readh));
  tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(prh));
}

static int tsdbInitTruncateTblArray(STruncateH *prh) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(prh);
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  if (tsdbRLockRepoMeta(pRepo) < 0) return -1;

  prh->tblArray = taosArrayInit(pMeta->maxTables, sizeof(STableTruncateH));
  if (prh->tblArray == NULL) {
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

    if (taosArrayPush(prh->tblArray, &ch) == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      tsdbUnlockRepoMeta(pRepo);
      return -1;
    }
  }

  if (tsdbUnlockRepoMeta(pRepo) < 0) return -1;
  return 0;
}

static void tsdbDestroyTruncateTblArray(STruncateH *prh) {
  STableTruncateH *pHandle = NULL;

  if (prh->tblArray == NULL) return;

  for (size_t i = 0; i < taosArrayGetSize(prh->tblArray); ++i) {
    pHandle = (STableTruncateH *)taosArrayGet(prh->tblArray, i);
    if (pHandle->pTable) {
      tsdbUnRefTable(pHandle->pTable);
    }

    tfree(pHandle->pInfo);
  }

  prh->tblArray = taosArrayDestroy(&prh->tblArray);
}

static int tsdbCacheFSetIndex(STruncateH *prh) {
  SReadH *pReadH = &(prh->readh);

  if (tsdbLoadBlockIdx(pReadH) < 0) {
    return -1;
  }

  size_t tblArraySize = taosArrayGetSize(prh->tblArray);
  for (size_t tid = 1; tid < tblArraySize; ++tid) {
    STableTruncateH *pHandle = (STableTruncateH *)taosArrayGet(prh->tblArray, tid);
    pHandle->pBlkIdx = NULL;

    if (pHandle->pTable == NULL) continue;
    if (tsdbSetReadTable(pReadH, pHandle->pTable) < 0) {
      return -1;
    }

    if (pReadH->pBlkIdx == NULL) continue;
    pHandle->bIndex = *(pReadH->pBlkIdx);
    pHandle->pBlkIdx = &(pHandle->bIndex);

    uint32_t originLen = 0;
    if (tsdbLoadBlockInfo(pReadH, (void **)(&(pHandle->pInfo)), &originLen) < 0) {
      return -1;
    }
  }

  return 0;
}

static int tsdbTruncateFSetInit(STruncateH *prh, SDFileSet *pSet) {
  taosArrayClear(prh->aBlkIdx);
  taosArrayClear(prh->aSupBlk);

  if (tsdbSetAndOpenReadFSet(&(prh->readh), pSet) < 0) {
    return -1;
  }

  if (tsdbCacheFSetIndex(prh) < 0) {
    tsdbCloseAndUnsetFSet(&(prh->readh));
    return -1;
  }

  return 0;
}

static void tsdbTruncateFSetEnd(STruncateH *prh) { tsdbCloseAndUnsetFSet(&(prh->readh)); }

static bool tsdbBlockInterleaved(STruncateH *prh, SBlock *pBlock) {
  // STruncateTblMsg *pMsg = (STruncateTblMsg *)prh->param;
  // for (uint16_t i = 0; i < pMsg->nSpan; ++i) {
  //   STimeWindow tw = pMsg->span[i];
  //   if (!(pBlock->keyFirst > tw.ekey || pBlock->keyLast < tw.skey)) {
  //     return true;
  //   }
  // }
  // return false;
  return true;
}

static int32_t tsdbFilterDataCols(STruncateH *prh, SDataCols *pSrcDCols) {
  SDataCols * pDstDCols = prh->pDCols;
  SControlData* pCtlData = &prh->pCtlInfo->ctlData;

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
    ++pDstDCols->numOfRows;
  }

  return 0;
}

static int tsdbTruncateFSetImpl(STruncateH *prh) {
  STsdbRepo *      pRepo = TSDB_TRUNCATE_REPO(prh);
  // SReadH *         pReadh = &(prh->readh);
  SBlockIdx *      pBlkIdx = NULL;
  void **          ppBuf = &(TSDB_TRUNCATE_BUF(prh));
  // void **          ppCBuf = &(TSDB_TRUNCATE_COMP_BUF(prh));
  // void **          ppExBuf = &(TSDB_TRUNCATE_EXBUF(prh));

  taosArrayClear(prh->aBlkIdx);

  for (size_t tid = 1; tid < taosArrayGetSize(prh->tblArray); ++tid) {
    STableTruncateH *pHandle = (STableTruncateH *)taosArrayGet(prh->tblArray, tid);
    pBlkIdx = pHandle->pBlkIdx;

    if (pHandle->pTable == NULL || pHandle->pBlkIdx == NULL) continue;

    taosArrayClear(prh->aSupBlk);

    uint64_t uid = pHandle->pTable->tableId.uid;

    if (uid != prh->pCtlInfo->uid) {
      if ((pBlkIdx->numOfBlocks > 0) && (taosArrayPush(prh->aBlkIdx, (const void *)(pBlkIdx)) == NULL)) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }
    } else {
      // Loop to mark delete flag for each block data
      tsdbDebug("vgId:%d uid %" PRIu64 " matched to truncate", REPO_ID(pRepo), uid);
      // for (int i = 0; i < pHandle->pBlkIdx->numOfBlocks; ++i) {
      //   SBlock *pBlock = pHandle->pInfo->blocks + i;

      //   if (tsdbWriteBlockToRightFile(prh, pHandle->pTable, prh->pDCols, ppBuf, ppCBuf, ppExBuf) <
      //   0) {
      //     return -1;
      //   }
      // }
    }
  }

  if (tsdbWriteBlockIdx(TSDB_TRUNCATE_HEAD_FILE(prh), prh->aBlkIdx, ppBuf) < 0) {
    return -1;
  }

  return 0;
}

static int tsdbDeleteFSetImpl(STruncateH *prh) {
  STsdbRepo *      pRepo = TSDB_TRUNCATE_REPO(prh);
  // STsdbCfg *       pCfg = REPO_CFG(pRepo);
  SReadH *  pReadh = &(prh->readh);
  SBlockIdx blkIdx = {0};
  void **   ppBuf = &(TSDB_TRUNCATE_BUF(prh));
  void **   ppCBuf = &(TSDB_TRUNCATE_COMP_BUF(prh));
  void **   ppExBuf = &(TSDB_TRUNCATE_EXBUF(prh));
  // int              defaultRows = TSDB_DEFAULT_BLOCK_ROWS(pCfg->maxRowsPerFileBlock);

  taosArrayClear(prh->aBlkIdx);

  for (size_t tid = 1; tid < taosArrayGetSize(prh->tblArray); ++tid) {
    STableTruncateH *pHandle = (STableTruncateH *)taosArrayGet(prh->tblArray, tid);
    STSchema *       pSchema = NULL;

    if (pHandle->pTable == NULL || pHandle->pBlkIdx == NULL) continue;

    if ((pSchema = tsdbGetTableSchemaImpl(pHandle->pTable, true, true, -1, -1)) == NULL) {
      return -1;
    }

    taosArrayClear(prh->aSupBlk);

    uint64_t uid = pHandle->pTable->tableId.uid;
    // if(uid != pMsg->uid) {
    // TODO: copy the block data directly
    // }

    if ((tdInitDataCols(prh->pDCols, pSchema) < 0) || (tdInitDataCols(pReadh->pDCols[0], pSchema) < 0) ||
        (tdInitDataCols(pReadh->pDCols[1], pSchema) < 0)) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      tdFreeSchema(pSchema);
      return -1;
    }

    tdFreeSchema(pSchema);

    // Loop to truncate each block data
    for (int i = 0; i < pHandle->pBlkIdx->numOfBlocks; ++i) {
      SBlock *pBlock = pHandle->pInfo->blocks + i;

      // Copy the Blocks directly if TS is not interleaved.
      if (!tsdbBlockInterleaved(prh, pBlock)) {
        // tsdbWriteBlockAndDataToFile();
        continue;
      }

      // Otherwise load the block data and copy the specific rows.
      if (tsdbLoadBlockData(pReadh, pBlock, pHandle->pInfo) < 0) {
        return -1;
      }
      if (uid == prh->pCtlInfo->uid) {
        tsdbFilterDataCols(prh, pReadh->pDCols[0]);
        tsdbDebug("vgId:%d uid %" PRIu64 " matched, filter block data from rows %d to %d rows", REPO_ID(pRepo), uid,
                  pReadh->pDCols[0]->numOfRows, prh->pDCols->numOfRows);
        if (prh->pDCols->numOfRows <= 0) continue;

        if (tsdbWriteBlockToRightFile(prh, pHandle->pTable, prh->pDCols, ppBuf, ppCBuf, ppExBuf) < 0) {
          return -1;
        }
      } else {
        tsdbDebug("vgId:%d uid %" PRIu64 " not matched, copy block data directly\n", REPO_ID(pRepo), uid);
        if (tsdbWriteBlockToRightFile(prh, pHandle->pTable, pReadh->pDCols[0], ppBuf, ppCBuf, ppExBuf) < 0) {
          return -1;
        }
      }
    }

    if (tsdbWriteBlockInfoImpl(TSDB_TRUNCATE_HEAD_FILE(prh), pHandle->pTable, prh->aSupBlk, NULL,
                               ppBuf, &blkIdx) < 0) {
      return -1;
    }

    if ((blkIdx.numOfBlocks > 0) && (taosArrayPush(prh->aBlkIdx, (const void *)(&blkIdx)) == NULL)) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  }

  if (tsdbWriteBlockIdx(TSDB_TRUNCATE_HEAD_FILE(prh), prh->aBlkIdx, ppBuf) < 0) {
    return -1;
  }

  return 0;
}

static int tsdbWriteBlockToRightFile(STruncateH *prh, STable *pTable, SDataCols *pDCols, void **ppBuf,
                                     void **ppCBuf, void **ppExBuf) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(prh);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  SDFile *   pDFile = NULL;
  bool       isLast = false;
  SBlock     block = {0};

  ASSERT(pDCols->numOfRows > 0);

  if (pDCols->numOfRows < pCfg->minRowsPerFileBlock) {
    pDFile = TSDB_TRUNCATE_LAST_FILE(prh);
    isLast = true;
  } else {
    pDFile = TSDB_TRUNCATE_DATA_FILE(prh);
    isLast = false;
  }

  if (tsdbWriteBlockImpl(pRepo, pTable, pDFile,
                         isLast ? TSDB_TRUNCATE_SMAL_FILE(prh) : TSDB_TRUNCATE_SMAD_FILE(prh), pDCols,
                         &block, isLast, true, ppBuf, ppCBuf, ppExBuf) < 0) {
    return -1;
  }

  if (taosArrayPush(prh->aSupBlk, (void *)(&block)) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}
