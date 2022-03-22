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
  SDataCols *pDCols;
  void *     param;  // STruncateTblMsg or SDeleteTblMsg
  TSDB_REQ_T type;   // truncate or delete
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

static int   tsdbAsyncTruncate(STsdbRepo *pRepo, void *param, TSDB_REQ_T type);
static void  tsdbStartTruncate(STsdbRepo *pRepo);
static void  tsdbEndTruncate(STsdbRepo *pRepo, int eno);
static int   tsdbTruncateMeta(STsdbRepo *pRepo);
static int   tsdbTruncateTSData(STsdbRepo *pRepo, void *param, TSDB_REQ_T type);
static int   tsdbTruncateFSet(STruncateH *pTruncateH, SDFileSet *pSet);
static int   tsdbDeleteFSet(STruncateH *pTruncateH, SDFileSet *pSet);
static int   tsdbInitTruncateH(STruncateH *pTruncateH, STsdbRepo *pRepo);
static void  tsdbDestroyTruncateH(STruncateH *pTruncateH);
static int   tsdbInitTruncateTblArray(STruncateH *pTruncateH);
static void  tsdbDestroyTruncateTblArray(STruncateH *pTruncateH);
static int   tsdbCacheFSetIndex(STruncateH *pTruncateH);
static int   tsdbTruncateCache(STsdbRepo *pRepo, void *param);
static int   tsdbTruncateFSetInit(STruncateH *pTruncateH, SDFileSet *pSet);
static void  tsdbTruncateFSetEnd(STruncateH *pTruncateH);
static int   tsdbTruncateFSetImpl(STruncateH *pTruncateH);
static int   tsdbDeleteFSetImpl(STruncateH *pTruncateH);
static bool  tsdbBlockInterleaved(STruncateH *pTruncateH, SBlock *pBlock);
static int   tsdbWriteBlockToRightFile(STruncateH *pTruncateH, STable *pTable, SDataCols *pDCols, void **ppBuf,
                                       void **ppCBuf, void **ppExBuf);
static void *tsdbTruncateImplCommon(STsdbRepo *pRepo, void *param, TSDB_REQ_T type);

enum {
  TSDB_NO_TRUNCATE,
  TSDB_IN_TRUNCATE,
  TSDB_WAITING_TRUNCATE,
};

int tsdbTruncateTbl(STsdbRepo *pRepo, void *param) { return tsdbAsyncTruncate(pRepo, param, TRUNCATE_TBL_REQ); }
int tsdbDeleteData(STsdbRepo *pRepo, void *param) { return tsdbAsyncTruncate(pRepo, param, DELETE_TBL_REQ); }

void *tsdbTruncateImpl(STsdbRepo *pRepo, void *param) {
  tsdbTruncateImplCommon(pRepo, param, TRUNCATE_TBL_REQ);
  return NULL;
}
void *tsdbDeleteImpl(STsdbRepo *pRepo, void *param) {
  tsdbTruncateImplCommon(pRepo, param, DELETE_TBL_REQ);
  return NULL;
}

static void *tsdbTruncateImplCommon(STsdbRepo *pRepo, void *param, TSDB_REQ_T type) {
  ASSERT(param != NULL);
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

  if (tsdbTruncateTSData(pRepo, param, type) < 0) {
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

static int tsdbAsyncTruncate(STsdbRepo *pRepo, void *param, TSDB_REQ_T type) {
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
  int code = tsdbScheduleCommit(pRepo, param, type);
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

static int tsdbTruncateTSData(STsdbRepo *pRepo, void *param, TSDB_REQ_T type) {
  STsdbCfg *       pCfg = REPO_CFG(pRepo);
  STruncateH       truncateH = {0};
  SDFileSet *      pSet = NULL;
  STruncateTblMsg *pMsg = (STruncateTblMsg *)param;

  tsdbDebug("vgId:%d start to truncate TS data for %" PRIu64, REPO_ID(pRepo), pMsg->uid);

  if (tsdbInitTruncateH(&truncateH, pRepo) < 0) {
    return -1;
  }

  truncateH.param = pMsg;
  truncateH.type = type;

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
    if (truncateH.type == TRUNCATE_TBL_REQ) {
      if (tsdbTruncateFSet(&truncateH, pSet) < 0) {
        tsdbDestroyTruncateH(&truncateH);
        tsdbError("vgId:%d failed to truncate table in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
        return -1;
      }
    } else if (truncateH.type == DELETE_TBL_REQ) {
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

static int tsdbDeleteFSet(STruncateH *pTruncateH, SDFileSet *pSet) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(pTruncateH);
  SDiskID    did = {0};

  tsdbDebug("vgId:%d start to truncate data in FSET %d on level %d id %d", REPO_ID(pRepo), pSet->fid,
            TSDB_FSET_LEVEL(pSet), TSDB_FSET_ID(pSet));

  if (tsdbTruncateFSetInit(pTruncateH, pSet) < 0) {
    return -1;
  }

  // Create new fset as deleted fset
  tfsAllocDisk(tsdbGetFidLevel(pSet->fid, &(pTruncateH->rtn)), &(did.level), &(did.id));
  if (did.level == TFS_UNDECIDED_LEVEL) {
    terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
    tsdbError("vgId:%d failed to truncate data in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
    tsdbTruncateFSetEnd(pTruncateH);
    return -1;
  }

  tsdbInitDFileSet(TSDB_TRUNCATE_WSET(pTruncateH), did, REPO_ID(pRepo), TSDB_FSET_FID(pSet),
                   FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_LATEST_FSET_VER);

  if (tsdbCreateDFileSet(TSDB_TRUNCATE_WSET(pTruncateH), true) < 0) {
    tsdbError("vgId:%d failed to truncate data in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
    tsdbTruncateFSetEnd(pTruncateH);
    return -1;
  }

  if (tsdbDeleteFSetImpl(pTruncateH) < 0) {
    tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(pTruncateH));
    tsdbRemoveDFileSet(TSDB_TRUNCATE_WSET(pTruncateH));
    tsdbTruncateFSetEnd(pTruncateH);
    return -1;
  }

  tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(pTruncateH));
  tsdbUpdateDFileSet(REPO_FS(pRepo), TSDB_TRUNCATE_WSET(pTruncateH));
  tsdbDebug("vgId:%d FSET %d truncate data over", REPO_ID(pRepo), pSet->fid);

  tsdbTruncateFSetEnd(pTruncateH);
  return 0;
}

static int tsdbTruncateFSet(STruncateH *pTruncateH, SDFileSet *pSet) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(pTruncateH);
  SDiskID    did = {0};
  SDFileSet *pWSet = TSDB_TRUNCATE_WSET(pTruncateH);

  tsdbDebug("vgId:%d start to truncate table in FSET %d on level %d id %d", REPO_ID(pRepo), pSet->fid,
            TSDB_FSET_LEVEL(pSet), TSDB_FSET_ID(pSet));

  if (tsdbTruncateFSetInit(pTruncateH, pSet) < 0) {
    return -1;
  }

  // Create new fset as truncated fset
  tfsAllocDisk(tsdbGetFidLevel(pSet->fid, &(pTruncateH->rtn)), &(did.level), &(did.id));
  if (did.level == TFS_UNDECIDED_LEVEL) {
    terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
    tsdbError("vgId:%d failed to truncate table in FSET %d since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
    tsdbTruncateFSetEnd(pTruncateH);
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

  if (tsdbTruncateFSetImpl(pTruncateH) < 0) {
    tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(pTruncateH));
    tsdbRemoveDFileSet(TSDB_TRUNCATE_WSET(pTruncateH));
    tsdbTruncateFSetEnd(pTruncateH);
    return -1;
  }

  tsdbCloseDFileSet(TSDB_TRUNCATE_WSET(pTruncateH));
  tsdbUpdateDFileSet(REPO_FS(pRepo), TSDB_TRUNCATE_WSET(pTruncateH));
  tsdbDebug("vgId:%d FSET %d truncate table over", REPO_ID(pRepo), pSet->fid);

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

  pTruncateH->pDCols = tdNewDataCols(0, pCfg->maxRowsPerFileBlock);
  if (pTruncateH->pDCols == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyTruncateH(pTruncateH);
    return -1;
  }

  return 0;
}

static void tsdbDestroyTruncateH(STruncateH *pTruncateH) {
  pTruncateH->pDCols = tdFreeDataCols(pTruncateH->pDCols);
  pTruncateH->aSupBlk = taosArrayDestroy(&pTruncateH->aSupBlk);
  pTruncateH->aBlkIdx = taosArrayDestroy(&pTruncateH->aBlkIdx);
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

  pTruncateH->tblArray = taosArrayDestroy(&pTruncateH->tblArray);
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

static bool tsdbBlockInterleaved(STruncateH *pTruncateH, SBlock *pBlock) {
  // STruncateTblMsg *pMsg = (STruncateTblMsg *)pTruncateH->param;
  // for (uint16_t i = 0; i < pMsg->nSpan; ++i) {
  //   STimeWindow tw = pMsg->span[i];
  //   if (!(pBlock->keyFirst > tw.ekey || pBlock->keyLast < tw.skey)) {
  //     return true;
  //   }
  // }
  // return false;
  return true;
}

static int32_t tsdbFilterDataCols(STruncateH *pTruncateH, SDataCols *pSrcDCols) {
  STruncateTblMsg *pMsg = (STruncateTblMsg *)pTruncateH->param;
  SDataCols *      pDstDCols = pTruncateH->pDCols;

  tdResetDataCols(pDstDCols);
  pDstDCols->maxCols = pSrcDCols->maxCols;
  pDstDCols->maxPoints = pSrcDCols->maxPoints;
  pDstDCols->numOfCols = pSrcDCols->numOfCols;
  pDstDCols->sversion = pSrcDCols->sversion;

  for (int i = 0; i < pSrcDCols->numOfRows; ++i) {
    int64_t tsKey = *(int64_t *)tdGetColDataOfRow(pSrcDCols->cols, i);
    if ((tsKey >= pMsg->span[0].skey) && (tsKey <= pMsg->span[0].ekey)) {
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

static int tsdbTruncateFSetImpl(STruncateH *pTruncateH) {
  STsdbRepo *      pRepo = TSDB_TRUNCATE_REPO(pTruncateH);
  STruncateTblMsg *pMsg = (STruncateTblMsg *)pTruncateH->param;
  // SReadH *         pReadh = &(pTruncateH->readh);
  SBlockIdx *      pBlkIdx = NULL;
  void **          ppBuf = &(TSDB_TRUNCATE_BUF(pTruncateH));
  // void **          ppCBuf = &(TSDB_TRUNCATE_COMP_BUF(pTruncateH));
  // void **          ppExBuf = &(TSDB_TRUNCATE_EXBUF(pTruncateH));

  taosArrayClear(pTruncateH->aBlkIdx);

  for (size_t tid = 1; tid < taosArrayGetSize(pTruncateH->tblArray); ++tid) {
    STableTruncateH *pTblHandle = (STableTruncateH *)taosArrayGet(pTruncateH->tblArray, tid);
    pBlkIdx = pTblHandle->pBlkIdx;

    if (pTblHandle->pTable == NULL || pTblHandle->pBlkIdx == NULL) continue;

    taosArrayClear(pTruncateH->aSupBlk);

    uint64_t uid = pTblHandle->pTable->tableId.uid;

    if (uid != pMsg->uid) {
      if ((pBlkIdx->numOfBlocks > 0) && (taosArrayPush(pTruncateH->aBlkIdx, (const void *)(pBlkIdx)) == NULL)) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }
    } else {
      // Loop to mark delete flag for each block data
      tsdbDebug("vgId:%d uid %" PRIu64 " matched to truncate", REPO_ID(pRepo), uid);
      // for (int i = 0; i < pTblHandle->pBlkIdx->numOfBlocks; ++i) {
      //   SBlock *pBlock = pTblHandle->pInfo->blocks + i;

      //   if (tsdbWriteBlockToRightFile(pTruncateH, pTblHandle->pTable, pTruncateH->pDCols, ppBuf, ppCBuf, ppExBuf) <
      //   0) {
      //     return -1;
      //   }
      // }
    }
  }

  if (tsdbWriteBlockIdx(TSDB_TRUNCATE_HEAD_FILE(pTruncateH), pTruncateH->aBlkIdx, ppBuf) < 0) {
    return -1;
  }

  return 0;
}

static int tsdbDeleteFSetImpl(STruncateH *pTruncateH) {
  STsdbRepo *      pRepo = TSDB_TRUNCATE_REPO(pTruncateH);
  STruncateTblMsg *pMsg = (STruncateTblMsg *)pTruncateH->param;
  // STsdbCfg *       pCfg = REPO_CFG(pRepo);
  SReadH *  pReadh = &(pTruncateH->readh);
  SBlockIdx blkIdx = {0};
  void **   ppBuf = &(TSDB_TRUNCATE_BUF(pTruncateH));
  void **   ppCBuf = &(TSDB_TRUNCATE_COMP_BUF(pTruncateH));
  void **   ppExBuf = &(TSDB_TRUNCATE_EXBUF(pTruncateH));
  // int              defaultRows = TSDB_DEFAULT_BLOCK_ROWS(pCfg->maxRowsPerFileBlock);

  taosArrayClear(pTruncateH->aBlkIdx);

  for (size_t tid = 1; tid < taosArrayGetSize(pTruncateH->tblArray); ++tid) {
    STableTruncateH *pTblHandle = (STableTruncateH *)taosArrayGet(pTruncateH->tblArray, tid);
    STSchema *       pSchema = NULL;

    if (pTblHandle->pTable == NULL || pTblHandle->pBlkIdx == NULL) continue;

    if ((pSchema = tsdbGetTableSchemaImpl(pTblHandle->pTable, true, true, -1, -1)) == NULL) {
      return -1;
    }

    taosArrayClear(pTruncateH->aSupBlk);

    uint64_t uid = pTblHandle->pTable->tableId.uid;
    // if(uid != pMsg->uid) {
    // TODO: copy the block data directly
    // }

    if ((tdInitDataCols(pTruncateH->pDCols, pSchema) < 0) || (tdInitDataCols(pReadh->pDCols[0], pSchema) < 0) ||
        (tdInitDataCols(pReadh->pDCols[1], pSchema) < 0)) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      tdFreeSchema(pSchema);
      return -1;
    }

    tdFreeSchema(pSchema);

    // Loop to truncate each block data
    for (int i = 0; i < pTblHandle->pBlkIdx->numOfBlocks; ++i) {
      SBlock *pBlock = pTblHandle->pInfo->blocks + i;

      // Copy the Blocks directly if TS is not interleaved.
      if (!tsdbBlockInterleaved(pTruncateH, pBlock)) {
        // tsdbWriteBlockAndDataToFile();
        continue;
      }

      // Otherwise load the block data and copy the specific rows.
      if (tsdbLoadBlockData(pReadh, pBlock, pTblHandle->pInfo) < 0) {
        return -1;
      }
      if (uid == pMsg->uid) {
        tsdbFilterDataCols(pTruncateH, pReadh->pDCols[0]);
        tsdbDebug("vgId:%d uid %" PRIu64 " matched, filter block data from rows %d to %d rows", REPO_ID(pRepo), uid,
                  pReadh->pDCols[0]->numOfRows, pTruncateH->pDCols->numOfRows);
        if (pTruncateH->pDCols->numOfRows <= 0) continue;

        if (tsdbWriteBlockToRightFile(pTruncateH, pTblHandle->pTable, pTruncateH->pDCols, ppBuf, ppCBuf, ppExBuf) < 0) {
          return -1;
        }
      } else {
        tsdbDebug("vgId:%d uid %" PRIu64 " not matched, copy block data directly\n", REPO_ID(pRepo), uid);
        if (tsdbWriteBlockToRightFile(pTruncateH, pTblHandle->pTable, pReadh->pDCols[0], ppBuf, ppCBuf, ppExBuf) < 0) {
          return -1;
        }
      }
    }

    if (tsdbWriteBlockInfoImpl(TSDB_TRUNCATE_HEAD_FILE(pTruncateH), pTblHandle->pTable, pTruncateH->aSupBlk, NULL,
                               ppBuf, &blkIdx) < 0) {
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

static int tsdbWriteBlockToRightFile(STruncateH *pTruncateH, STable *pTable, SDataCols *pDCols, void **ppBuf,
                                     void **ppCBuf, void **ppExBuf) {
  STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(pTruncateH);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  SDFile *   pDFile = NULL;
  bool       isLast = false;
  SBlock     block = {0};

  ASSERT(pDCols->numOfRows > 0);

  if (pDCols->numOfRows < pCfg->minRowsPerFileBlock) {
    pDFile = TSDB_TRUNCATE_LAST_FILE(pTruncateH);
    isLast = true;
  } else {
    pDFile = TSDB_TRUNCATE_DATA_FILE(pTruncateH);
    isLast = false;
  }

  if (tsdbWriteBlockImpl(pRepo, pTable, pDFile,
                         isLast ? TSDB_TRUNCATE_SMAL_FILE(pTruncateH) : TSDB_TRUNCATE_SMAD_FILE(pTruncateH), pDCols,
                         &block, isLast, true, ppBuf, ppCBuf, ppExBuf) < 0) {
    return -1;
  }

  if (taosArrayPush(pTruncateH->aSupBlk, (void *)(&block)) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

// static int tsdbWriteBlockAndDataToFile(STruncateH *pTruncateH, STable *pTable, SBlock *pSupBlock, void **ppBuf,
//                                        void **ppCBuf, void **ppExBuf) {
//   STsdbRepo *pRepo = TSDB_TRUNCATE_REPO(pTruncateH);
//   SDFile *   pDFile = NULL;
//   bool       isLast = false;

//   ASSERT(pSupBlock->numOfRows > 0);

//   if (pSupBlock->last) {
//     pDFile = TSDB_TRUNCATE_LAST_FILE(pTruncateH);
//     isLast = true;
//   } else {
//     pDFile = TSDB_TRUNCATE_DATA_FILE(pTruncateH);
//     isLast = false;
//   }

//   if (tsdbWriteBlockImpl(pRepo, pTable, pDFile,
//                          isLast ? TSDB_TRUNCATE_SMAL_FILE(pTruncateH) : TSDB_TRUNCATE_SMAD_FILE(pTruncateH),
//                          pDCols, &block, isLast, true, ppBuf, ppCBuf, ppExBuf) < 0) {
//     return -1;
//   }

//   if (taosArrayPush(pTruncateH->aSupBlk, (void *)(&block)) == NULL) {
//     terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
//     return -1;
//   }

//   return 0;
// }