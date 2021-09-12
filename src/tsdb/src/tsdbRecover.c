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
  SReadH    readh;
  SDFileSet wSet;
  SArray *  aBlkIdx;  // SBlockIdx array
  SArray *  aSupBlk;  // Table super-block array
  SArray *  aSubBlk;  // table sub-block array
} SRecoverH;

#define TSDB_RECOVER_WFSET(rh) (&((rh)->wSet))
#define TSDB_RECOVER_WHEAD_FILE(rh) TSDB_DFILE_IN_SET(TSDB_RECOVER_WFSET(rh), TSDB_FILE_HEAD)
#define TSDB_RECOVER_WDATA_FILE(rh) TSDB_DFILE_IN_SET(TSDB_RECOVER_WFSET(rh), TSDB_FILE_DATA)
#define TSDB_RECOVER_WLAST_FILE(rh) TSDB_DFILE_IN_SET(TSDB_RECOVER_WFSET(rh), TSDB_FILE_LAST)

static int tsdbInitRecoverH(SRecoverH *pRecoverH, STsdbRepo *pRepo);
static int tsdbDestoryRecoverH(SRecoverH *pRecoverH);
static int tsdbInitHFile(STsdbRepo *pRepo, SDFile *pDestDFile, const SDFile *pSrcDFile, int fid);
static int tsdbDestroyHFile(SDFile *pDFile);
static int tsdbHeadWriteBlockInfo(SRecoverH *pRecoverH);
static int tsdbHeadWriteBlockIdx(SRecoverH *pRecoverH);
static int tsdbHeadAddBlock(SRecoverH *pRecoverH, const SBlock *pSupBlock, const SBlock *pSubBlocks, int nSubBlocks);

static int tsdbInitRecoverH(SRecoverH *pRecoverH, STsdbRepo *pRepo) {
  memset(pRecoverH, 0, sizeof(SRecoverH));

  // Init read handle
  if (tsdbInitReadH(&(pRecoverH->readh), pRepo) < 0) {
    return -1;
  }

  pRecoverH->aBlkIdx = taosArrayInit(1024, sizeof(SBlockIdx));
  if (pRecoverH->aBlkIdx == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestoryRecoverH(pRecoverH);
    return -1;
  }

  pRecoverH->aSupBlk = taosArrayInit(1024, sizeof(SBlock));
  if (pRecoverH->aSupBlk == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestoryRecoverH(pRecoverH);
    return -1;
  }

  pRecoverH->aSubBlk = taosArrayInit(1024, sizeof(SBlock));
  if (pRecoverH->aSubBlk == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestoryRecoverH(pRecoverH);
    return -1;
  }
  return 0;
}

static int tsdbDestoryRecoverH(SRecoverH *pRecoverH) {
  pRecoverH->aSubBlk = taosArrayDestroy(pRecoverH->aSubBlk);
  pRecoverH->aSupBlk = taosArrayDestroy(pRecoverH->aSupBlk);
  pRecoverH->aBlkIdx = taosArrayDestroy(pRecoverH->aBlkIdx);
  tsdbDestroyReadH(&(pRecoverH->readh));
  return 0;
}

static int tsdbHeadWriteBlockInfo(SRecoverH *pRecoverH) {
  SReadH *    pReadH = &pRecoverH->readh;
  SDFile *    pWHeadf = TSDB_RECOVER_WHEAD_FILE(pRecoverH);
  SBlockIdx * pBlkIdx = pReadH->pBlkIdx;
  SBlockIdx   blkIdx;
  SBlock *    pBlock = NULL;
  uint32_t    nSupBlocks = (uint32_t)taosArrayGetSize(pRecoverH->aSupBlk);
  uint32_t    nSubBlocks = (uint32_t)taosArrayGetSize(pRecoverH->aSubBlk);
  uint32_t    tlen = 0;
  SBlockInfo *pBlkInfo = NULL;
  int64_t     offset = 0;

  if (nSupBlocks <= 0) {
    // No data (data all deleted)
    return 0;
  }

  tlen = (uint32_t)(sizeof(SBlockInfo) + sizeof(SBlock) * (nSupBlocks + nSubBlocks) + sizeof(TSCKSUM));

  // Write SBlockInfo part
  if (tsdbMakeRoom((void **)(&(TSDB_READ_BUF(pReadH))), tlen) < 0) {
    return -1;
  }
  pBlkInfo = TSDB_READ_BUF(pReadH);

  pBlkInfo->delimiter = TSDB_FILE_DELIMITER;
  pBlkInfo->tid = pBlkIdx->tid;
  pBlkInfo->uid = pBlkIdx->uid;

  memcpy((void *)(pBlkInfo->blocks), taosArrayGet(pRecoverH->aSupBlk, 0), nSupBlocks * sizeof(SBlock));
  if (nSubBlocks > 0) {
    memcpy((void *)(pBlkInfo->blocks + nSupBlocks), taosArrayGet(pRecoverH->aSubBlk, 0), nSubBlocks * sizeof(SBlock));

    for (uint32_t i = 0; i < nSupBlocks; ++i) {
      pBlock = pBlkInfo->blocks + i;

      if (pBlock->numOfSubBlocks > 1) {
        pBlock->offset += (sizeof(SBlockInfo) + sizeof(SBlock) * nSupBlocks);
      }
    }
  }

  taosCalcChecksumAppend(0, (uint8_t *)pBlkInfo, tlen);

  if (tsdbAppendDFile(pWHeadf, TSDB_READ_BUF(pReadH), tlen, &offset) < 0) {
    return -1;
  }

  tsdbUpdateDFileMagic(pWHeadf, POINTER_SHIFT(pBlkInfo, tlen - sizeof(TSCKSUM)));

  // Set blkIdx
  pBlock = taosArrayGet(pRecoverH->aSupBlk, nSupBlocks - 1);

  blkIdx.tid = pBlkIdx->tid;
  blkIdx.uid = pBlkIdx->uid;
  blkIdx.hasLast = pBlock->last ? 1 : 0;
  blkIdx.maxKey = pBlock->keyLast;
  blkIdx.numOfBlocks = nSupBlocks;
  blkIdx.len = tlen;
  blkIdx.offset = (uint32_t)offset;

  ASSERT(blkIdx.numOfBlocks > 0);

  if (taosArrayPush(pRecoverH->aBlkIdx, (void *)(&blkIdx)) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int tsdbHeadWriteBlockIdx(SRecoverH *pRecoverH) {
  SReadH *   pReadH = &pRecoverH->readh;
  SDFile *   pWHeadf = TSDB_RECOVER_WHEAD_FILE(pRecoverH);
  SBlockIdx *pBlkIdx = NULL;
  uint32_t   nidx = (uint32_t)taosArrayGetSize(pRecoverH->aBlkIdx);
  int        tlen = 0, size = 0;
  int64_t    offset = 0;

  if (nidx <= 0) {
    // All data are deleted
    pWHeadf->info.offset = 0;
    pWHeadf->info.len = 0;
    return 0;
  }

  for (uint32_t i = 0; i < nidx; ++i) {
    pBlkIdx = (SBlockIdx *)taosArrayGet(pRecoverH->aBlkIdx, i);

    size = tsdbEncodeSBlockIdx(NULL, pBlkIdx);
    if (tsdbMakeRoom((void **)(&TSDB_READ_BUF(pReadH)), tlen + size) < 0) return -1;

    void *ptr = POINTER_SHIFT(TSDB_READ_BUF(pReadH), tlen);
    tsdbEncodeSBlockIdx(&ptr, pBlkIdx);

    tlen += size;
  }

  tlen += sizeof(TSCKSUM);
  if (tsdbMakeRoom((void **)(&TSDB_READ_BUF(pReadH)), tlen) < 0) return -1;
  taosCalcChecksumAppend(0, (uint8_t *)TSDB_READ_BUF(pReadH), tlen);

  if (tsdbAppendDFile(pWHeadf, TSDB_READ_BUF(pReadH), tlen, &offset) < tlen) {
    tsdbError("vgId:%d failed to write block index part to file %s since %s", REPO_ID(pReadH->pRepo),
              TSDB_FILE_FULL_NAME(pWHeadf), tstrerror(terrno));
    return -1;
  }

  tsdbUpdateDFileMagic(pWHeadf, POINTER_SHIFT(TSDB_READ_BUF(pReadH), tlen - sizeof(TSCKSUM)));
  pWHeadf->info.offset = (uint32_t)offset;
  pWHeadf->info.len = tlen;

  return 0;
}

static int tsdbHeadAddBlock(SRecoverH *pRecoverH, const SBlock *pSupBlock, const SBlock *pSubBlocks, int nSubBlocks) {
  if (taosArrayPush(pRecoverH->aSupBlk, pSupBlock) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (pSubBlocks && taosArrayAddBatch(pRecoverH->aSubBlk, pSubBlocks, nSubBlocks) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int tsdbInitHFile(STsdbRepo *pRepo, SDFile *pDestDFile, const SDFile *pSrcDFile, int fid) {
  SDiskID did;
  did.level = pSrcDFile->f.level;
  did.id = pSrcDFile->f.id;

  tsdbInitDFile(pDestDFile, did, REPO_ID(pRepo), fid, FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_FILE_HEAD);
  if (tsdbCreateDFile(pDestDFile, true, TSDB_FILE_HEAD) < 0) {
    tsdbError("vgId:%d failed to create file %s since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDestDFile),
              tstrerror(terrno));
    return -1;
  }
  return TSDB_CODE_SUCCESS;
}

static int tsdbDestroyHFile(SDFile *pDFile) {
  tsdbCloseDFile(pDFile);
  return tsdbRemoveDFile(pDFile);
}

static int tsdbRefactorHeadF(STsdbRepo *pRepo, SRecoverH *pRecoverH, SDFileSet *pSet, int32_t *nRemain) {
  SDFile *pHeadF = TSDB_DFILE_IN_SET(pSet, TSDB_FILE_HEAD);
  if (pHeadF->info.fver == tsdbGetDFSVersion(TSDB_FILE_HEAD)) {
    if (taosArrayPush(REPO_FS(pRepo)->nstatus->df, pSet) == NULL) {
      terrno = TSDB_CODE_FS_OUT_OF_MEMORY;
      return -1;
    }
    ++*nRemain;
    return TSDB_CODE_SUCCESS;
  }

  SReadH *pReadH = &(pRecoverH->readh);

  if (tsdbSetAndOpenReadFSet(pReadH, pSet) < 0) {
    return -1;
  }

  if (tsdbLoadBlockIdx(pReadH) < 0) {
    tsdbCloseDFileSet(TSDB_READ_FSET(pReadH));
    return -1;
  }

  SDFile *pTmpHeadF = TSDB_RECOVER_WHEAD_FILE(pRecoverH);
  if (tsdbInitHFile(pRepo, pTmpHeadF, pHeadF, pSet->fid) < 0) {
    tsdbError("vgId:%d failed to init file %s to refactor since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pTmpHeadF),
              tstrerror(terrno));
    tsdbCloseDFileSet(TSDB_READ_FSET(pReadH));
    return -1;
  }

  int    arraySize = taosArrayGetSize(pReadH->aBlkIdx);
  SBlock supBlk;
  for (int iBlkIdx = 0; iBlkIdx < arraySize; ++iBlkIdx) {
    pReadH->pBlkIdx = taosArrayGet(pReadH->aBlkIdx, iBlkIdx);
    pReadH->cidx = iBlkIdx;

    if (tsdbLoadBlockInfo(pReadH, NULL, NULL) < 0) {
      tsdbCloseDFileSet(TSDB_READ_FSET(pReadH));
      tsdbDestroyHFile(pTmpHeadF);
      return -1;
    }

    // clear the reused resource
    taosArrayClear(pRecoverH->aSupBlk);
    taosArrayClear(pRecoverH->aSubBlk);

    for (uint32_t iSupBlk = 0; iSupBlk < pReadH->pBlkIdx->numOfBlocks; ++iSupBlk) {
      SBlock *pSupBlk = pReadH->pBlkInfo->blocks + iSupBlk;
      if (pSupBlk->numOfSubBlocks == 1) {
        if (tsdbHeadAddBlock(pRecoverH, pSupBlk, NULL, 0) < 0) {
          tsdbCloseDFileSet(TSDB_READ_FSET(pReadH));
          tsdbDestroyHFile(pTmpHeadF);
          return -1;
        }
      } else {
        supBlk = *pSupBlk;
        supBlk.offset = sizeof(SBlock) * taosArrayGetSize(pRecoverH->aSubBlk);
        if (tsdbHeadAddBlock(pRecoverH, &supBlk, POINTER_SHIFT(pReadH->pBlkInfo, pSupBlk->offset),
                             pSupBlk->numOfSubBlocks) < 0) {
          tsdbCloseDFileSet(TSDB_READ_FSET(pReadH));
          tsdbDestroyHFile(pTmpHeadF);
          return -1;
        }
      }
    }
    if (tsdbHeadWriteBlockInfo(pRecoverH) < 0) {
      tsdbError("vgId:%d failed to write SBlockInfo part into file %s since %s", REPO_ID(pReadH->pRepo),
                TSDB_FILE_FULL_NAME(pTmpHeadF), tstrerror(terrno));
      tsdbCloseDFileSet(TSDB_READ_FSET(pReadH));
      tsdbDestroyHFile(pTmpHeadF);
      return -1;
    }
  }

  if (tsdbHeadWriteBlockIdx(pRecoverH) < 0) {
    tsdbError("vgId:%d failed to write SBlockIdx part into file %s since %s", REPO_ID(pReadH->pRepo),
              TSDB_FILE_FULL_NAME(pTmpHeadF), tstrerror(terrno));
    tsdbCloseDFileSet(TSDB_READ_FSET(pReadH));
    tsdbDestroyHFile(pTmpHeadF);
    return -1;
  }

  if (tsdbUpdateDFileHeader(pTmpHeadF) < 0) {
    tsdbError("vgId:%d failed to update header of file %s since %s", REPO_ID(pReadH->pRepo),
              TSDB_FILE_FULL_NAME(pTmpHeadF), tstrerror(terrno));
    tsdbCloseDFileSet(TSDB_READ_FSET(pReadH));
    tsdbDestroyHFile(pTmpHeadF);
    return -1;
  }

  // resource release
  tsdbCloseDFileSet(TSDB_READ_FSET(pReadH));

  TSDB_FILE_FSYNC(pTmpHeadF);
  tsdbCloseDFile(pTmpHeadF);
  SDFileSet *pDestFSet = TSDB_READ_FSET(pReadH);
  tsdbInitDFileEx(TSDB_DFILE_IN_SET(pDestFSet, TSDB_FILE_HEAD), pTmpHeadF);

  if (taosArrayPush(REPO_FS(pRepo)->nstatus->df, pDestFSet) == NULL) {
    terrno = TSDB_CODE_FS_OUT_OF_MEMORY;
    return -1;
  }

  return TSDB_CODE_SUCCESS;
}

int tsdbRefactorFS(STsdbRepo *pRepo) {
  STsdbFS *  pfs = REPO_FS(pRepo);
  SFSStatus *pStatus = pfs->cstatus;
  size_t     size = taosArrayGetSize(pStatus->df);
  if (size <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SRecoverH recoverH;
  if (tsdbInitRecoverH(&recoverH, pRepo) < 0) {
    return -1;
  }

  tsem_wait(&pRepo->readyToCommit);

  tsdbStartFSTxn(pRepo, 0, 0);

  int32_t nRemain = 0;
  for (size_t i = 0; i < size; ++i) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pStatus->df, i);
    if (tsdbRefactorHeadF(pRepo, &recoverH, pSet, &nRemain) < 0) {
      tsdbDestoryRecoverH(&recoverH);
      tsdbEndFSTxnWithError(REPO_FS(pRepo));
      tsem_post(&pRepo->readyToCommit);
      tsdbError("vgId:%d failed to refactor DFileSet since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }
  }

  tsdbDestoryRecoverH(&recoverH);
  if (nRemain == size) {
    tsdbEndFSTxnWithError(REPO_FS(pRepo));
  } else {
    if (pStatus != NULL) {
      pfs->nstatus->mf = pStatus->mf;
      pfs->nstatus->pmf = &pfs->nstatus->mf;
    }
    if (tsdbEndFSTxn(pRepo) < 0) {
      tsem_post(&pRepo->readyToCommit);
      return -1;
    }
  }
  tsem_post(&pRepo->readyToCommit);
  return TSDB_CODE_SUCCESS;
}