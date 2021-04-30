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

extern int tsTsdbCheckRestoreMode;

#define TSDB_FNAME_PREFIX_TMP "t."

typedef struct {
  // SRtn         rtn;     // retention snapshot
  // SFSIter      fsIter;  // tsdb file iterator
  // int          niters;  // memory iterators
  // SCommitIter *iters;
  // bool         isRFileSet;  // read and commit FSET
  SReadH readh;
  // SDFileSet wSet;
  // bool      isDFileSame;
  // bool      isLFileSame;
  // TSKEY     minKey;
  // TSKEY     maxKey;
  SArray *aBlkIdx;  // SBlockIdx array
  // STable *pTable;
  SArray *aSupBlk;  // Table super-block array
  SArray *aSubBlk;  // table sub-block array
  // SDataCols *  pDataCols;
  // size_t nSupScanned;
  // size_t nSupCorrupted;
} SRecoverH;

static int tsdbInitRecoverH(SRecoverH *pRecoverH, STsdbRepo *pRepo);
static int tsdbDestoryRecoverH(SRecoverH *pRecoverH);
static int tsdbHeadWriteBlockInfo(SRecoverH *pRecoverH);
static int tsdbHeadWriteBlockIdx(SRecoverH *pRecoverH);
static int tsdbHeadAddBlock(SRecoverH *pRecoverH, const SBlock *pSupBlock, const SBlock *pSubBlocks,
                            const int nSubBlocks);
/**
 *  internal func
 */
static int tsdbRecoverManager(SRecoverH *pRecoverH);
/**
 *  get blkidx and blkinfo of head, and check the data block chksum of dfile
 */
static int tsdbCheckDFileChksum(SRecoverH *pRecoverH);
/**
 *  function should return in case of fatal error, e.g. out of memory
 */
static bool tsdbRecoverIsFatalError();
static int  tsdbBackUpDFileSet(SDFileSet *pFileSet);
/**
 * load SBlockInfo from .head
 */
static int tsdbRecoverLoadBlockInfo(SRecoverH *pRecoverH, void *pTarget);
/**
 * load and check block data in .data/.last
 */
static int tsdbRecoverCheckBlockData(SRecoverH *pRecoverH, SBlock *pBlock, SBlockInfo *pBlkInfo);
static int tsdbCheckBlockDataColsChkSum(SReadH *pReadh, SBlock *pBlock, SDataCols *pDataCols);

static int tsdbInitHFile(SDFile *pDestDFile, const SDFile *pSrcDFile);
static int tsdbDestroyHFile(SDFile *pDFile);

int tsdbRecoverDataMain(STsdbRepo *pRepo) {
  SRecoverH recoverH;
  SReadH *  pReadH = &recoverH.readh;
  SArray *  fSetArray = NULL;  // SDFileSet array
  STsdbFS * pfs = REPO_FS(pRepo);

  if (tsdbFetchDFileSet(pRepo, &fSetArray) < 0) {
    tsdbError("vgId:%d failed to fetch DFileSet to recover since %s", REPO_ID(pRepo), strerror(terrno));
    return -1;
  }

  if (taosArrayGetSize(fSetArray) <= 0) {
    taosArrayDestroy(fSetArray);
    tsdbInfo("vgId:%d no need to recover since empty DFileSet", REPO_ID(pRepo));
    return 0;
  }

  // init pRecoverH
  tsdbInitRecoverH(&recoverH, pRepo);
  // check for each SDFileSet
  for (size_t iDFileSet = 0; iDFileSet < taosArrayGetSize(fSetArray); ++iDFileSet) {
    pReadH->rSet = *(SDFileSet *)taosArrayGet(fSetArray, iDFileSet);

    if (tsdbRecoverManager(&recoverH) < 0) {
      // backup the SDFileSet
      if (tsdbBackUpDFileSet(&pReadH->rSet) < 0) {
        tsdbError("vgId:%d failed to backup DFileSet %d since %s", REPO_ID(pRepo), pReadH->rSet.fid, strerror(terrno));
        return -1;
      }
      // check next SDFileSet although return not zero
      continue;
    }

    tsdbInfo("vgId:%d FSET %d is restored in mode %d", REPO_ID(pRepo), pReadH->rSet.fid, tsTsdbCheckRestoreMode);
    taosArrayPush(pfs->cstatus->df, &pReadH->rSet);
  }

  // release resources
  taosArrayDestroy(fSetArray);
  // release pRecoverH
  tsdbDestoryRecoverH(&recoverH);

  return 0;
}

static int tsdbBackUpDFileSet(SDFileSet *pFileSet) { return 0; }

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

static int tsdbRecoverManager(SRecoverH *pRecoverH) {
  SReadH *pReadH = &pRecoverH->readh;
  int     result = 0;

  // init
  if (tsdbSetAndOpenReadFSet(pReadH, &pReadH->rSet) < 0) {
    tsdbCloseDFileSet(TSDB_READ_FSET(pReadH));
    return -1;
  }
  // process
  switch (tsTsdbCheckRestoreMode) {
    case TSDB_CHECK_MODE_CHKSUM_IF_NO_CURRENT: {
      result = tsdbCheckDFileChksum(pRecoverH);
      break;
    }
    default:
      break;
  }

  // resource release
  tsdbCloseDFileSet(TSDB_READ_FSET(pReadH));

  return result;
}

static bool tsdbRecoverIsFatalError() {
  if (TSDB_CODE_TDB_OUT_OF_MEMORY == terrno) {
    return true;
  }
  return false;
}

static int tsdbCheckDFileChksum(SRecoverH *pRecoverH) {
  SReadH *pReadH = &pRecoverH->readh;
  SDFile *pHeadF = TSDB_READ_HEAD_FILE(pReadH);
  SDFile *pTmpHeadF = NULL;
  SBlock *pSupBlk = NULL;
  SBlock  supBlk;
  size_t  nSupBlkScanned = 0;
  size_t  nSupBlkCorrupted = 0;
  size_t  nBlkIdxChkPassed = 0;

  // make sure DFileSet is opened before
  if (tsdbLoadBlockIdx(pReadH) < 0) {
    return -1;
  }

  if (taosArrayGetSize(pReadH->aBlkIdx) <= 0) {
    tsdbInfo("vgId:%d empty SBlockIdx in %s", TSDB_READ_REPO_ID(pReadH), TSDB_FILE_FULL_NAME(pHeadF));
    return -1;  // invalid head file, not return 0
  }

  if (tsdbInitHFile(pTmpHeadF, pHeadF) < 0) {
    tsdbInfo("vgId:%d failed to init %s since %s", TSDB_READ_REPO_ID(pReadH), TSDB_FILE_FULL_NAME(pTmpHeadF),tstrerror(terrno));
    return -1;
  }

  for (size_t iBlkIdx = 0; iBlkIdx < taosArrayGetSize(pReadH->aBlkIdx); ++iBlkIdx) {
    pReadH->pBlkIdx = taosArrayGet(pReadH->aBlkIdx, iBlkIdx);
    pReadH->cidx = iBlkIdx;

    if (tsdbRecoverLoadBlockInfo(pRecoverH, NULL) < 0) {
      if (tsdbRecoverIsFatalError()) {
        tsdbDestroyHFile(pTmpHeadF);
        return -1;
      }
      continue;
    }

    // clear the reused resource
    taosArrayClear(pRecoverH->aSupBlk);
    taosArrayClear(pRecoverH->aSubBlk);

    for (size_t iSupBlk = 0; iSupBlk < pReadH->pBlkIdx->numOfBlocks; ++iSupBlk) {
      pSupBlk = pReadH->pBlkInfo->blocks + iSupBlk;
      ++nSupBlkScanned;
      if (tsdbRecoverCheckBlockData(pRecoverH, pSupBlk, NULL) < 0) {
        ++nSupBlkCorrupted;
        if (tsdbRecoverIsFatalError()) {
          tsdbDestroyHFile(pTmpHeadF);
          return -1;
        }
        continue;
      }
      // pass the check, add the supblk to SHeadFileInfo
      supBlk = *pSupBlk;
      supBlk.offset = sizeof(SBlock) * taosArrayGetSize(pRecoverH->aSubBlk);

      if (tsdbHeadAddBlock(pRecoverH, &supBlk,
                           supBlk.numOfSubBlocks > 1 ? POINTER_SHIFT(pRecoverH->readh.pBlkInfo, pSupBlk->offset) : NULL,
                           pSupBlk->numOfSubBlocks) < 0) {
        tsdbDestroyHFile(pTmpHeadF);
        return -1;
      }
      if (tsdbHeadWriteBlockInfo(pRecoverH) < 0) {
        tsdbError("vgId:%d failed to write SBlockInfo part into file %s since %s", REPO_ID(pReadH->pRepo),
                  TSDB_FILE_FULL_NAME(pTmpHeadF), tstrerror(terrno));
        tsdbDestroyHFile(pTmpHeadF);
        return -1;
      }
    }
    // pass the check
    ++nBlkIdxChkPassed;
  }

  if (tsdbHeadWriteBlockIdx(pRecoverH) < 0) {
    tsdbError("vgId:%d failed to write SBlockIdx part into file %s since %s", REPO_ID(pReadH->pRepo),
              TSDB_FILE_FULL_NAME(pTmpHeadF), tstrerror(terrno));
    tsdbDestroyHFile(pTmpHeadF);
    return -1;
  }

  // use the rebuild .head file if
  if ((nSupBlkCorrupted > 0) || (nBlkIdxChkPassed != taosArrayGetSize(pReadH->aBlkIdx))) {
    if (taosRename("oldName", "newName") < 0) {
      // remove t.vdfdddd.head{-ver2}. Use the prefix but not suffix to avoid error
      tsdbDestroyHFile(pTmpHeadF);
      return -1;
    }
  } else {
    tsdbDestroyHFile(pTmpHeadF);
  }

  // release resources
  // Nothing TODO
  return 0;
}

static int tsdbHeadAddBlock(SRecoverH *pRecoverH, const SBlock *pSupBlock, const SBlock *pSubBlocks, int nSubBlocks) {
  if (taosArrayPush(pRecoverH->aSupBlk, pSupBlock) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (pSubBlocks && taosArrayPushBatch(pRecoverH->aSubBlk, pSubBlocks, nSubBlocks) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int tsdbHeadWriteBlockInfo(SRecoverH *pRecoverH) {
  SReadH *    pReadH = &pRecoverH->readh;
  SDFile *    pHeadf = TSDB_READ_HEAD_FILE(pReadH);
  SBlockIdx * pBlkIdx = pReadH->pBlkIdx;
  SBlockIdx   blkIdx;
  SBlock *    pBlock = NULL;
  size_t      nSupBlocks;
  size_t      nSubBlocks;
  uint32_t    tlen;
  SBlockInfo *pBlkInfo = NULL;
  int64_t     offset;

  nSupBlocks = taosArrayGetSize(pRecoverH->aSupBlk);
  nSubBlocks = taosArrayGetSize(pRecoverH->aSubBlk);

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

    for (int i = 0; i < nSupBlocks; i++) {
      pBlock = pBlkInfo->blocks + i;

      if (pBlock->numOfSubBlocks > 1) {
        pBlock->offset += (sizeof(SBlockInfo) + sizeof(SBlock) * nSupBlocks);
      }
    }
  }

  taosCalcChecksumAppend(0, (uint8_t *)pBlkInfo, tlen);

  if (tsdbAppendDFile(pHeadf, TSDB_READ_BUF(pReadH), tlen, &offset) < 0) {
    return -1;
  }

  tsdbUpdateDFileMagic(pHeadf, POINTER_SHIFT(pBlkInfo, tlen - sizeof(TSCKSUM)));

  // Set blkIdx
  pBlock = taosArrayGet(pRecoverH->aSupBlk, nSupBlocks - 1);

  blkIdx.tid = pBlkIdx->tid;
  blkIdx.uid = pBlkIdx->uid;
  blkIdx.hasLast = pBlock->last ? 1 : 0;
  blkIdx.maxKey = pBlock->keyLast;
  blkIdx.numOfBlocks = (uint32_t)nSupBlocks;
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
  SDFile *   pHeadf = TSDB_READ_HEAD_FILE(pReadH);
  SBlockIdx *pBlkIdx = NULL;
  size_t     nidx = taosArrayGetSize(pRecoverH->aBlkIdx);
  int        tlen = 0, size;
  int64_t    offset;

  if (nidx <= 0) {
    // All data are deleted
    pHeadf->info.offset = 0;
    pHeadf->info.len = 0;
    return 0;
  }

  for (size_t i = 0; i < nidx; i++) {
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

  if (tsdbAppendDFile(pHeadf, TSDB_READ_BUF(pReadH), tlen, &offset) < tlen) {
    tsdbError("vgId:%d failed to write block index part to file %s since %s", REPO_ID(pReadH->pRepo),
              TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno));
    return -1;
  }

  tsdbUpdateDFileMagic(pHeadf, POINTER_SHIFT(TSDB_READ_BUF(pReadH), tlen - sizeof(TSCKSUM)));
  pHeadf->info.offset = (uint32_t)offset;
  pHeadf->info.len = tlen;

  return 0;
}

static int tsdbRecoverLoadBlockInfo(SRecoverH *pRecoverH, void *pTarget) {
  SReadH *pReadh = &pRecoverH->readh;
  ASSERT(pReadh->pBlkIdx != NULL);

  SDFile *   pHeadf = TSDB_READ_HEAD_FILE(pReadh);
  SBlockIdx *pBlkIdx = pReadh->pBlkIdx;

  if (tsdbSeekDFile(pHeadf, pBlkIdx->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to load SBlockInfo part while seek file %s since %s, offset:%u len:%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno), pBlkIdx->offset, pBlkIdx->len);
    return -1;
  }

  if (tsdbMakeRoom((void **)(&(pReadh->pBlkInfo)), pBlkIdx->len) < 0) {
    return -1;
  }

  int64_t nread = tsdbReadDFile(pHeadf, (void *)(pReadh->pBlkInfo), pBlkIdx->len);
  if (nread < 0) {
    tsdbError("vgId:%d failed to load SBlockInfo part while read file %s since %s, offset:%u len :%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno), pBlkIdx->offset, pBlkIdx->len);
    return -1;
  }

  if (nread < pBlkIdx->len) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d SBlockInfo part in file %s is corrupted, offset:%u expected bytes:%u read bytes:%" PRId64,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), pBlkIdx->offset, pBlkIdx->len, nread);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)(pReadh->pBlkInfo), pBlkIdx->len)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d SBlockInfo part in file %s is corrupted since wrong checksum, offset:%u len :%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), pBlkIdx->offset, pBlkIdx->len);
    return -1;
  }

  if (pBlkIdx->tid != pReadh->pBlkInfo->tid || pBlkIdx->uid != pReadh->pBlkInfo->uid) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d SBlockInfo part in file %s is corrupted since mismatch tid[%d,%d] or uid[%" PRIu64 ",%" PRIu64
              "], offset:%u len :%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), pBlkIdx->tid, pReadh->pBlkInfo->tid, pBlkIdx->uid,
              pReadh->pBlkInfo->uid, pBlkIdx->offset, pBlkIdx->len);
    return -1;
  }

  if (pTarget) {
    memcpy(pTarget, (void *)(pReadh->pBlkInfo), pBlkIdx->len);
  }

  return 0;
}

static int tsdbRecoverCheckBlockData(SRecoverH *pRecoverH, SBlock *pSupBlock, SBlockInfo *pBlkInfo) {
  ASSERT(pSupBlock->numOfSubBlocks > 0);
  SReadH *pReadH = &pRecoverH->readh;

  SBlock *iBlock = pSupBlock;
  if (pSupBlock->numOfSubBlocks > 1) {
    if (pBlkInfo) {
      iBlock = (SBlock *)POINTER_SHIFT(pBlkInfo, pSupBlock->offset);
    } else {
      iBlock = (SBlock *)POINTER_SHIFT(pReadH->pBlkInfo, pSupBlock->offset);
    }
  }

  if (tsdbCheckBlockDataColsChkSum(pReadH, iBlock, pReadH->pDCols[0]) < 0) {
    return -1;
  }
  for (int i = 1; i < pSupBlock->numOfSubBlocks; ++i) {
    ++iBlock;
    if (tsdbCheckBlockDataColsChkSum(pReadH, iBlock, pReadH->pDCols[1]) < 0) {
      return -1;
    }
  }

  // TODO: ASSERT update to if-else judgement.
  ASSERT(pReadH->pDCols[0]->numOfRows == pSupBlock->numOfRows);
  ASSERT(dataColsKeyFirst(pReadH->pDCols[0]) == pSupBlock->keyFirst);
  ASSERT(dataColsKeyLast(pReadH->pDCols[0]) == pSupBlock->keyLast);

  return 0;
}

static int tsdbInitHFile(SDFile *pDestDFile, const SDFile *pSrcDFile) {
  const TFILE *pf = &pSrcDFile->f;
  int          tvid, tfid;
  TSDB_FILE_T  ttype;
  uint32_t     tversion;
  char         aname[TSDB_FILENAME_LEN];
  char         rname[TSDB_FILENAME_LEN];
  char         dname[TSDB_FILENAME_LEN];

  memset(pDestDFile, 0, sizeof(SDFile));
  pDestDFile->info.magic = TSDB_FILE_INIT_MAGIC;


  tfsbasename(pf, aname);
  tfsdirname(pf, dname);
  tsdbParseDFilename(aname, &tvid, &tfid, &ttype, &tversion);
  tsdbGetAbsoluteNameByPrefix(rname, tvid, tfid, tversion, ttype, TSDB_FNAME_PREFIX_TMP, dname);
  tstrncpy(pDestDFile->f.aname, aname, sizeof(aname));
  tstrncpy(pDestDFile->f.rname, rname, sizeof(rname));  

  if (tsdbCreateDFile(pDestDFile, true) < 0) {
    return -1;
  }

  return 0;
}

static int tsdbDestroyHFile(SDFile *pDFile) {
  if (tsdbRemoveDFile(pDFile) < 0) {
    // print error(while has no other impact)
  }
  return 0;
}

static int tsdbCheckBlockDataColsChkSum(SReadH *pReadh, SBlock *pBlock, SDataCols *pDataCols) {
  ASSERT(pBlock->numOfSubBlocks == 0 || pBlock->numOfSubBlocks == 1);

  SDFile *pDFile = (pBlock->last) ? TSDB_READ_LAST_FILE(pReadh) : TSDB_READ_DATA_FILE(pReadh);

  tdResetDataCols(pDataCols);

  if (tsdbMakeRoom((void **)(&TSDB_READ_BUF(pReadh)), pBlock->len) < 0) return -1;

  SBlockData *pBlockData = (SBlockData *)TSDB_READ_BUF(pReadh);

  if (tsdbSeekDFile(pDFile, pBlock->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to load block data part while seek file %s to offset %" PRId64 " since %s",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, tstrerror(terrno));
    return -1;
  }

  int64_t nread = tsdbReadDFile(pDFile, TSDB_READ_BUF(pReadh), pBlock->len);
  if (nread < 0) {
    tsdbError("vgId:%d failed to load block data part while read file %s since %s, offset:%" PRId64 " len :%d",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), tstrerror(terrno), (int64_t)pBlock->offset,
              pBlock->len);
    return -1;
  }

  if (nread < pBlock->len) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d block data part in file %s is corrupted, offset:%" PRId64
              " expected bytes:%d read bytes: %" PRId64,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, pBlock->len, nread);
    return -1;
  }

  int32_t tsize = TSDB_BLOCK_STATIS_SIZE(pBlock->numOfCols);
  if (!taosCheckChecksumWhole((uint8_t *)TSDB_READ_BUF(pReadh), tsize)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d block statis part in file %s is corrupted since wrong checksum, offset:%" PRId64 " len :%d",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, tsize);
    return -1;
  }

  ASSERT(tsize < pBlock->len);
  ASSERT(pBlockData->numOfCols == pBlock->numOfCols);

  pDataCols->numOfRows = pBlock->numOfRows;

  // Recover the data
  int ccol = 0;  // loop iter for SBlockCol object
  int dcol = 0;  // loop iter for SDataCols object
  while (dcol < pDataCols->numOfCols) {
    SDataCol *pDataCol = &(pDataCols->cols[dcol]);
    if (dcol != 0 && ccol >= pBlockData->numOfCols) {
      // Set current column as NULL and forward
      dataColSetNEleNull(pDataCol, pBlock->numOfRows, pDataCols->maxPoints);
      dcol++;
      continue;
    }

    int16_t  tcolId = 0;
    uint32_t toffset = TSDB_KEY_COL_OFFSET;
    int32_t  tlen = pBlock->keyLen;

    if (dcol != 0) {
      SBlockCol *pBlockCol = &(pBlockData->cols[ccol]);
      tcolId = pBlockCol->colId;
      toffset = tsdbGetBlockColOffset(pBlockCol);
      tlen = pBlockCol->len;
    } else {
      ASSERT(pDataCol->colId == tcolId);
    }

    if (tcolId == pDataCol->colId) {
      if (pBlock->algorithm == TWO_STAGE_COMP) {
        int zsize = pDataCol->bytes * pBlock->numOfRows + COMP_OVERFLOW_BYTES;
        if (tsdbMakeRoom((void **)(&TSDB_READ_COMP_BUF(pReadh)), zsize) < 0) return -1;
      }

      if (!taosCheckChecksumWhole((uint8_t *)POINTER_SHIFT(pBlockData, tsize + toffset), tlen)) {
        terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
        tsdbError("vgId:%d file %s is broken at column %d block offset %" PRId64 " column offset %u",
                  TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), tcolId, (int64_t)pBlock->offset, toffset);
        return -1;
      }
      if (dcol != 0) {
        ccol++;
      }
      dcol++;
    } else if (tcolId < pDataCol->colId) {
      ccol++;
    } else {
      // Set current column as NULL and forward
      dataColSetNEleNull(pDataCol, pBlock->numOfRows, pDataCols->maxPoints);
      dcol++;
    }
  }

  return 0;
}