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
  STable *pTable;
  SArray *aSupBlk;  // Table super-block array
  SArray *aSubBlk;  // table sub-block array
  // SDataCols *  pDataCols;
  // size_t nSupScanned;
  // size_t nSupCorrupted;
} SRecoverH;

static int tsdbInitRecoverH(SRecoverH *pRecoverH, const STsdbRepo *pRepo);
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
static int  tsdbRecoverLoadBlockInfo(SRecoverH *pRecoverH, void *pTarget);
static int  tsdbRecoverLoadBlockData(SRecoverH *pRecoverH, SBlock *pBlock, SBlockInfo *pBlkInfo);

static int tsdbInitHFile(SDFile *pDFile);
static int tsdbDestroyHFile(SDFile *pDFile);

// check and recover data
int tsdbRecoverData(STsdbRepo *pRepo) {
  SRecoverH recoverH;
  SReadH *  pReadH = &recoverH.readh;
  pReadH->pRepo = pRepo;
  SArray * fSetArray = NULL;  // SDFileSet array
  STsdbFS *pfs = REPO_FS(pRepo);

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
  // TODO
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
  // TODO
  return 0;
}

static int tsdbBackUpDFileSet(SDFileSet *pFileSet) { return 0; }
static int tsdbInitRecoverH(SRecoverH *pRecoverH, const STsdbRepo *pRepo) { return 0; }
static int tsdbDestoryRecoverH(SRecoverH *pRecoverH) { return 0; }

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
    tsdbInfo("vgId:%d empty SBlockIdx in %s", TSDB_READ_REPO_ID(pReadH), TSDB_FILE_FULL_NAME(pHeadF));  // TODO
    return -1;  // invalid head file, not return 0
  }

  if (tsdbInitHFile(pTmpHeadF) < 0) {
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

    // clear for reused resource
    taosArrayClear(pRecoverH->aSupBlk);
    taosArrayClear(pRecoverH->aSubBlk);

    for (size_t iSupBlk = 0; iSupBlk < pReadH->pBlkIdx->numOfBlocks; ++iSupBlk) {
      pSupBlk = pReadH->pBlkInfo->blocks + iSupBlk;
      ++nSupBlkScanned;
      if (tsdbRecoverLoadBlockData(pRecoverH, pSupBlk, NULL) < 0) {
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

static int tsdbRecoverLoadBlockData(SRecoverH *pRecoverH, SBlock *pBlock, SBlockInfo *pBlkInfo) {
  ASSERT(pBlock->numOfSubBlocks > 0);
  SReadH *pReadh = &pRecoverH->readh;

  SBlock *iBlock = pBlock;
  if (pBlock->numOfSubBlocks > 1) {
    if (pBlkInfo) {
      iBlock = (SBlock *)POINTER_SHIFT(pBlkInfo, pBlock->offset);
    } else {
      iBlock = (SBlock *)POINTER_SHIFT(pReadh->pBlkInfo, pBlock->offset);
    }
  }

  // if (tsdbLoadBlockDataImpl(pReadh, iBlock, pReadh->pDCols[0]) < 0) {
  //   return -1;
  // }
  for (int i = 1; i < pBlock->numOfSubBlocks; ++i) {
    ++iBlock;
    // if (tsdbLoadBlockDataImpl(pReadh, iBlock, pReadh->pDCols[1]) < 0) {
    //   return -1;
    // }
  }

  // ASSERT(pReadh->pDCols[0]->numOfRows == pBlock->numOfRows);
  // ASSERT(dataColsKeyFirst(pReadh->pDCols[0]) == pBlock->keyFirst);
  // ASSERT(dataColsKeyLast(pReadh->pDCols[0]) == pBlock->keyLast);

  return 0;
}

static int tsdbInitHFile(SDFile *pDFile) {
  // memset(&tHeadFile, 0, sizeof(tHeadFile));
  // tHeadFile.info.magic = TSDB_FILE_INIT_MAGIC;
  // tstrncpy(tHeadFile.f.aname, anameHeadT, sizeof(anameHeadT));
  // tstrncpy(tHeadFile.f.rname, anameHeadT, sizeof(rnameHeadT));
  return 0;
}

static int tsdbDestroyHFile(SDFile *pDFile) {
  //
  if (tsdbRemoveDFile(pDFile) < 0) {
    // print error(while has no other impact)
  }
  return 0;
}

#if 0

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
  SArray *     aBlkIdx;  // SBlockIdx array
  STable *     pTable;
  SArray *     aSupBlk;  // Table super-block array
  SArray *     aSubBlk;  // table sub-block array
  SDataCols *  pDataCols;
} SCommitH;

#define TSDB_COMMIT_REPO(ch) TSDB_READ_REPO(&(ch->readh))
#define TSDB_COMMIT_REPO_ID(ch) REPO_ID(TSDB_READ_REPO(&(ch->readh)))
#define TSDB_COMMIT_WRITE_FSET(ch) (&((ch)->wSet))
#define TSDB_COMMIT_TABLE(ch) ((ch)->pTable)
#define TSDB_COMMIT_HEAD_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_HEAD)
#define TSDB_COMMIT_DATA_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_DATA)
#define TSDB_COMMIT_LAST_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_LAST)
#define TSDB_COMMIT_BUF(ch) TSDB_READ_BUF(&((ch)->readh))
#define TSDB_COMMIT_COMP_BUF(ch) TSDB_READ_COMP_BUF(&((ch)->readh))
#define TSDB_COMMIT_DEFAULT_ROWS(ch) (TSDB_COMMIT_REPO(ch)->config.maxRowsPerFileBlock * 4 / 5)
#define TSDB_COMMIT_TXN_VERSION(ch) FS_TXN_VERSION(REPO_FS(TSDB_COMMIT_REPO(ch)))

static int  tsdbCommitMeta(STsdbRepo *pRepo);
static int  tsdbUpdateMetaRecord(STsdbFS *pfs, SMFile *pMFile, uint64_t uid, void *cont, int contLen);
static int  tsdbDropMetaRecord(STsdbFS *pfs, SMFile *pMFile, uint64_t uid);
static int  tsdbCommitTSData(STsdbRepo *pRepo);
static void tsdbStartCommit(STsdbRepo *pRepo);
static void tsdbEndCommit(STsdbRepo *pRepo, int eno);
static int  tsdbCommitToFile(SCommitH *pCommith, SDFileSet *pSet, int fid);
static int  tsdbCreateCommitIters(SCommitH *pCommith);
static void tsdbDestroyCommitIters(SCommitH *pCommith);
static void tsdbSeekCommitIter(SCommitH *pCommith, TSKEY key);
static int  tsdbInitCommitH(SCommitH *pCommith, STsdbRepo *pRepo);
static void tsdbDestroyCommitH(SCommitH *pCommith);
static int  tsdbNextCommitFid(SCommitH *pCommith);
static int  tsdbCommitToTable(SCommitH *pCommith, int tid);
static int  tsdbSetCommitTable(SCommitH *pCommith, STable *pTable);
static int  tsdbComparKeyBlock(const void *arg1, const void *arg2);
static int  tsdbWriteBlockInfo(SCommitH *pCommih);
static int  tsdbWriteBlockIdx(SCommitH *pCommih);
static int  tsdbCommitMemData(SCommitH *pCommith, SCommitIter *pIter, TSKEY keyLimit, bool toData);
static int  tsdbMergeMemData(SCommitH *pCommith, SCommitIter *pIter, int bidx);
static int  tsdbMoveBlock(SCommitH *pCommith, int bidx);
static int  tsdbCommitAddBlock(SCommitH *pCommith, const SBlock *pSupBlock, const SBlock *pSubBlocks, int nSubBlocks);
static int  tsdbMergeBlockData(SCommitH *pCommith, SCommitIter *pIter, SDataCols *pDataCols, TSKEY keyLimit,
                               bool isLastOneBlock);
static void tsdbResetCommitFile(SCommitH *pCommith);
static void tsdbResetCommitTable(SCommitH *pCommith);
static int  tsdbSetAndOpenCommitFile(SCommitH *pCommith, SDFileSet *pSet, int fid);
static void tsdbCloseCommitFile(SCommitH *pCommith, bool hasError);
static bool tsdbCanAddSubBlock(SCommitH *pCommith, SBlock *pBlock, SMergeInfo *pInfo);
static void tsdbLoadAndMergeFromCache(SDataCols *pDataCols, int *iter, SCommitIter *pCommitIter, SDataCols *pTarget,
                                      TSKEY maxKey, int maxRows, int8_t update);
static int  tsdbApplyRtn(STsdbRepo *pRepo);
static int  tsdbApplyRtnOnFSet(STsdbRepo *pRepo, SDFileSet *pSet, SRtn *pRtn);

#define TSDB_KEY_COL_OFFSET 0

static void tsdbResetReadTable(SReadH *pReadh);
static void tsdbResetReadFile(SReadH *pReadh);
static int  tsdbLoadBlockDataImpl(SReadH *pReadh, SBlock *pBlock, SDataCols *pDataCols);
static int  tsdbCheckAndDecodeColumnData(SDataCol *pDataCol, void *content, int32_t len, int8_t comp, int numOfRows,
                                         int maxPoints, char *buffer, int bufferSize);
static int  tsdbLoadBlockDataColsImpl(SReadH *pReadh, SBlock *pBlock, SDataCols *pDataCols, int16_t *colIds,
                                      int numOfColIds);
static int  tsdbLoadColData(SReadH *pReadh, SDFile *pDFile, SBlock *pBlock, SBlockCol *pBlockCol, SDataCol *pDataCol);

int tsdbInitReadH(SReadH *pReadh, STsdbRepo *pRepo) {
  ASSERT(pReadh != NULL && pRepo != NULL);

  STsdbCfg *pCfg = REPO_CFG(pRepo);

  memset((void *)pReadh, 0, sizeof(*pReadh));
  pReadh->pRepo = pRepo;

  TSDB_FSET_SET_CLOSED(TSDB_READ_FSET(pReadh));

  pReadh->aBlkIdx = taosArrayInit(1024, sizeof(SBlockIdx));
  if (pReadh->aBlkIdx == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  pReadh->pDCols[0] = tdNewDataCols(0, 0, pCfg->maxRowsPerFileBlock);
  if (pReadh->pDCols[0] == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyReadH(pReadh);
    return -1;
  }

  pReadh->pDCols[1] = tdNewDataCols(0, 0, pCfg->maxRowsPerFileBlock);
  if (pReadh->pDCols[1] == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyReadH(pReadh);
    return -1;
  }

  return 0;
}

void tsdbDestroyReadH(SReadH *pReadh) {
  if (pReadh == NULL) return;

  pReadh->pCBuf = taosTZfree(pReadh->pCBuf);
  pReadh->pBuf = taosTZfree(pReadh->pBuf);
  pReadh->pDCols[0] = tdFreeDataCols(pReadh->pDCols[0]);
  pReadh->pDCols[1] = tdFreeDataCols(pReadh->pDCols[1]);
  pReadh->pBlkData = taosTZfree(pReadh->pBlkData);
  pReadh->pBlkInfo = taosTZfree(pReadh->pBlkInfo);
  pReadh->cidx = 0;
  pReadh->pBlkIdx = NULL;
  pReadh->pTable = NULL;
  pReadh->aBlkIdx = taosArrayDestroy(pReadh->aBlkIdx);
  tsdbCloseDFileSet(TSDB_READ_FSET(pReadh));
  pReadh->pRepo = NULL;
}

int tsdbSetAndOpenReadFSet(SReadH *pReadh, SDFileSet *pSet) {
  ASSERT(pSet != NULL);
  tsdbResetReadFile(pReadh);

  pReadh->rSet = *pSet;
  TSDB_FSET_SET_CLOSED(TSDB_READ_FSET(pReadh));
  if (tsdbOpenDFileSet(TSDB_READ_FSET(pReadh), O_RDONLY) < 0) {
    tsdbError("vgId:%d failed to open file set %d since %s", TSDB_READ_REPO_ID(pReadh), TSDB_FSET_FID(pSet),
              tstrerror(terrno));
    return -1;
  }

  return 0;
}

void tsdbCloseAndUnsetFSet(SReadH *pReadh) { tsdbResetReadFile(pReadh); }

int tsdbLoadBlockIdx(SReadH *pReadh) {
  SDFile *  pHeadf = TSDB_READ_HEAD_FILE(pReadh);
  SBlockIdx blkIdx;

  ASSERT(taosArrayGetSize(pReadh->aBlkIdx) == 0);

  // No data at all, just return
  if (pHeadf->info.offset <= 0) return 0;

  if (tsdbSeekDFile(pHeadf, pHeadf->info.offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to load SBlockIdx part while seek file %s since %s, offset:%u len :%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno), pHeadf->info.offset,
              pHeadf->info.len);
    return -1;
  }

  if (tsdbMakeRoom((void **)(&TSDB_READ_BUF(pReadh)), pHeadf->info.len) < 0) return -1;

  int64_t nread = tsdbReadDFile(pHeadf, TSDB_READ_BUF(pReadh), pHeadf->info.len);
  if (nread < 0) {
    tsdbError("vgId:%d failed to load SBlockIdx part while read file %s since %s, offset:%u len :%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno), pHeadf->info.offset,
              pHeadf->info.len);
    return -1;
  }

  if (nread < pHeadf->info.len) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d SBlockIdx part in file %s is corrupted, offset:%u expected bytes:%u read bytes: %" PRId64,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), pHeadf->info.offset, pHeadf->info.len, nread);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)TSDB_READ_BUF(pReadh), pHeadf->info.len)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d SBlockIdx part in file %s is corrupted since wrong checksum, offset:%u len :%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), pHeadf->info.offset, pHeadf->info.len);
    return -1;
  }

  void *ptr = TSDB_READ_BUF(pReadh);
  int   tsize = 0;
  while (POINTER_DISTANCE(ptr, TSDB_READ_BUF(pReadh)) < (pHeadf->info.len - sizeof(TSCKSUM))) {
    ptr = tsdbDecodeSBlockIdx(ptr, &blkIdx);
    ASSERT(ptr != NULL);

    if (taosArrayPush(pReadh->aBlkIdx, (void *)(&blkIdx)) == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }

    tsize++;
    ASSERT(tsize == 1 || ((SBlockIdx *)taosArrayGet(pReadh->aBlkIdx, tsize - 2))->tid <
                             ((SBlockIdx *)taosArrayGet(pReadh->aBlkIdx, tsize - 1))->tid);
  }

  return 0;
}

int tsdbSetReadTable(SReadH *pReadh, STable *pTable) {
  STSchema *pSchema = tsdbGetTableSchemaImpl(pTable, false, false, -1);

  pReadh->pTable = pTable;

  if (tdInitDataCols(pReadh->pDCols[0], pSchema) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (tdInitDataCols(pReadh->pDCols[1], pSchema) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  size_t size = taosArrayGetSize(pReadh->aBlkIdx);
  if (size > 0) {
    while (true) {
      if (pReadh->cidx >= size) {
        pReadh->pBlkIdx = NULL;
        break;
      }

      SBlockIdx *pBlkIdx = taosArrayGet(pReadh->aBlkIdx, pReadh->cidx);
      if (pBlkIdx->tid == TABLE_TID(pTable)) {
        if (pBlkIdx->uid == TABLE_UID(pTable)) {
          pReadh->pBlkIdx = pBlkIdx;
        } else {
          pReadh->pBlkIdx = NULL;
        }
        pReadh->cidx++;
        break;
      } else if (pBlkIdx->tid > TABLE_TID(pTable)) {
        pReadh->pBlkIdx = NULL;
        break;
      } else {
        pReadh->cidx++;
      }
    }
  } else {
    pReadh->pBlkIdx = NULL;
  }

  return 0;
}

int tsdbLoadBlockInfo(SReadH *pReadh, void *pTarget) {
  ASSERT(pReadh->pBlkIdx != NULL);

  SDFile *   pHeadf = TSDB_READ_HEAD_FILE(pReadh);
  SBlockIdx *pBlkIdx = pReadh->pBlkIdx;

  if (tsdbSeekDFile(pHeadf, pBlkIdx->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to load SBlockInfo part while seek file %s since %s, offset:%u len:%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno), pBlkIdx->offset, pBlkIdx->len);
    return -1;
  }

  if (tsdbMakeRoom((void **)(&(pReadh->pBlkInfo)), pBlkIdx->len) < 0) return -1;

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

  ASSERT(pBlkIdx->tid == pReadh->pBlkInfo->tid && pBlkIdx->uid == pReadh->pBlkInfo->uid);

  if (pTarget) {
    memcpy(pTarget, (void *)(pReadh->pBlkInfo), pBlkIdx->len);
  }

  return 0;
}

int tsdbLoadBlockData(SReadH *pReadh, SBlock *pBlock, SBlockInfo *pBlkInfo) {
  ASSERT(pBlock->numOfSubBlocks > 0);

  SBlock *iBlock = pBlock;
  if (pBlock->numOfSubBlocks > 1) {
    if (pBlkInfo) {
      iBlock = (SBlock *)POINTER_SHIFT(pBlkInfo, pBlock->offset);
    } else {
      iBlock = (SBlock *)POINTER_SHIFT(pReadh->pBlkInfo, pBlock->offset);
    }
  }

  if (tsdbLoadBlockDataImpl(pReadh, iBlock, pReadh->pDCols[0]) < 0) return -1;
  for (int i = 1; i < pBlock->numOfSubBlocks; i++) {
    iBlock++;
    if (tsdbLoadBlockDataImpl(pReadh, iBlock, pReadh->pDCols[1]) < 0) return -1;
    if (tdMergeDataCols(pReadh->pDCols[0], pReadh->pDCols[1], pReadh->pDCols[1]->numOfRows) < 0) return -1;
  }

  ASSERT(pReadh->pDCols[0]->numOfRows == pBlock->numOfRows);
  ASSERT(dataColsKeyFirst(pReadh->pDCols[0]) == pBlock->keyFirst);
  ASSERT(dataColsKeyLast(pReadh->pDCols[0]) == pBlock->keyLast);

  return 0;
}

int tsdbLoadBlockDataCols(SReadH *pReadh, SBlock *pBlock, SBlockInfo *pBlkInfo, int16_t *colIds, int numOfColsIds) {
  ASSERT(pBlock->numOfSubBlocks > 0);

  SBlock *iBlock = pBlock;
  if (pBlock->numOfSubBlocks > 1) {
    if (pBlkInfo) {
      iBlock = POINTER_SHIFT(pBlkInfo, pBlock->offset);
    } else {
      iBlock = POINTER_SHIFT(pReadh->pBlkInfo, pBlock->offset);
    }
  }

  if (tsdbLoadBlockDataColsImpl(pReadh, iBlock, pReadh->pDCols[0], colIds, numOfColsIds) < 0) return -1;
  for (int i = 1; i < pBlock->numOfSubBlocks; i++) {
    iBlock++;
    if (tsdbLoadBlockDataColsImpl(pReadh, iBlock, pReadh->pDCols[1], colIds, numOfColsIds) < 0) return -1;
    if (tdMergeDataCols(pReadh->pDCols[0], pReadh->pDCols[1], pReadh->pDCols[1]->numOfRows) < 0) return -1;
  }

  ASSERT(pReadh->pDCols[0]->numOfRows == pBlock->numOfRows);
  ASSERT(dataColsKeyFirst(pReadh->pDCols[0]) == pBlock->keyFirst);
  ASSERT(dataColsKeyLast(pReadh->pDCols[0]) == pBlock->keyLast);

  return 0;
}

int tsdbLoadBlockStatis(SReadH *pReadh, SBlock *pBlock) {
  ASSERT(pBlock->numOfSubBlocks <= 1);

  SDFile *pDFile = (pBlock->last) ? TSDB_READ_LAST_FILE(pReadh) : TSDB_READ_DATA_FILE(pReadh);

  if (tsdbSeekDFile(pDFile, pBlock->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to load block statis part while seek file %s to offset %" PRId64 " since %s",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, tstrerror(terrno));
    return -1;
  }

  size_t size = TSDB_BLOCK_STATIS_SIZE(pBlock->numOfCols);
  if (tsdbMakeRoom((void **)(&(pReadh->pBlkData)), size) < 0) return -1;

  int64_t nread = tsdbReadDFile(pDFile, (void *)(pReadh->pBlkData), size);
  if (nread < 0) {
    tsdbError("vgId:%d failed to load block statis part while read file %s since %s, offset:%" PRId64 " len :%" PRIzu,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), tstrerror(terrno), (int64_t)pBlock->offset, size);
    return -1;
  }

  if (nread < size) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d block statis part in file %s is corrupted, offset:%" PRId64 " expected bytes:%" PRIzu
              " read bytes: %" PRId64,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, size, nread);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)(pReadh->pBlkData), (uint32_t)size)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d block statis part in file %s is corrupted since wrong checksum, offset:%" PRId64 " len :%" PRIzu,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, size);
    return -1;
  }

  return 0;
}

int tsdbEncodeSBlockIdx(void **buf, SBlockIdx *pIdx) {
  int tlen = 0;

  tlen += taosEncodeVariantI32(buf, pIdx->tid);
  tlen += taosEncodeVariantU32(buf, pIdx->len);
  tlen += taosEncodeVariantU32(buf, pIdx->offset);
  tlen += taosEncodeFixedU8(buf, pIdx->hasLast);
  tlen += taosEncodeVariantU32(buf, pIdx->numOfBlocks);
  tlen += taosEncodeFixedU64(buf, pIdx->uid);
  tlen += taosEncodeFixedU64(buf, pIdx->maxKey);

  return tlen;
}

void *tsdbDecodeSBlockIdx(void *buf, SBlockIdx *pIdx) {
  uint8_t  hasLast = 0;
  uint32_t numOfBlocks = 0;
  uint64_t value = 0;

  if ((buf = taosDecodeVariantI32(buf, &(pIdx->tid))) == NULL) return NULL;
  if ((buf = taosDecodeVariantU32(buf, &(pIdx->len))) == NULL) return NULL;
  if ((buf = taosDecodeVariantU32(buf, &(pIdx->offset))) == NULL) return NULL;
  if ((buf = taosDecodeFixedU8(buf, &(hasLast))) == NULL) return NULL;
  pIdx->hasLast = hasLast;
  if ((buf = taosDecodeVariantU32(buf, &(numOfBlocks))) == NULL) return NULL;
  pIdx->numOfBlocks = numOfBlocks;
  if ((buf = taosDecodeFixedU64(buf, &value)) == NULL) return NULL;
  pIdx->uid = (int64_t)value;
  if ((buf = taosDecodeFixedU64(buf, &value)) == NULL) return NULL;
  pIdx->maxKey = (TSKEY)value;

  return buf;
}

void tsdbGetBlockStatis(SReadH *pReadh, SDataStatis *pStatis, int numOfCols) {
  SBlockData *pBlockData = pReadh->pBlkData;

  for (int i = 0, j = 0; i < numOfCols;) {
    if (j >= pBlockData->numOfCols) {
      pStatis[i].numOfNull = -1;
      i++;
      continue;
    }

    if (pStatis[i].colId == pBlockData->cols[j].colId) {
      pStatis[i].sum = pBlockData->cols[j].sum;
      pStatis[i].max = pBlockData->cols[j].max;
      pStatis[i].min = pBlockData->cols[j].min;
      pStatis[i].maxIndex = pBlockData->cols[j].maxIndex;
      pStatis[i].minIndex = pBlockData->cols[j].minIndex;
      pStatis[i].numOfNull = pBlockData->cols[j].numOfNull;
      i++;
      j++;
    } else if (pStatis[i].colId < pBlockData->cols[j].colId) {
      pStatis[i].numOfNull = -1;
      i++;
    } else {
      j++;
    }
  }
}

static void tsdbResetReadTable(SReadH *pReadh) {
  tdResetDataCols(pReadh->pDCols[0]);
  tdResetDataCols(pReadh->pDCols[1]);
  pReadh->cidx = 0;
  pReadh->pBlkIdx = NULL;
  pReadh->pTable = NULL;
}

static void tsdbResetReadFile(SReadH *pReadh) {
  tsdbResetReadTable(pReadh);
  taosArrayClear(pReadh->aBlkIdx);
  tsdbCloseDFileSet(TSDB_READ_FSET(pReadh));
}

static int tsdbLoadBlockDataImpl(SReadH *pReadh, SBlock *pBlock, SDataCols *pDataCols) {
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

      if (tsdbCheckAndDecodeColumnData(pDataCol, POINTER_SHIFT(pBlockData, tsize + toffset), tlen, pBlock->algorithm,
                                       pBlock->numOfRows, pDataCols->maxPoints, TSDB_READ_COMP_BUF(pReadh),
                                       (int)taosTSizeof(TSDB_READ_COMP_BUF(pReadh))) < 0) {
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

static int tsdbCheckAndDecodeColumnData(SDataCol *pDataCol, void *content, int32_t len, int8_t comp, int numOfRows,
                                        int maxPoints, char *buffer, int bufferSize) {
  if (!taosCheckChecksumWhole((uint8_t *)content, len)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  // Decode the data
  if (comp) {
    // Need to decompress
    int tlen = (*(tDataTypes[pDataCol->type].decompFunc))(content, len - sizeof(TSCKSUM), numOfRows, pDataCol->pData,
                                                          pDataCol->spaceSize, comp, buffer, bufferSize);
    if (tlen <= 0) {
      tsdbError("Failed to decompress column, file corrupted, len:%d comp:%d numOfRows:%d maxPoints:%d bufferSize:%d",
                len, comp, numOfRows, maxPoints, bufferSize);
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
      return -1;
    }
    pDataCol->len = tlen;
  } else {
    // No need to decompress, just memcpy it
    pDataCol->len = len - sizeof(TSCKSUM);
    memcpy(pDataCol->pData, content, pDataCol->len);
  }

  if (IS_VAR_DATA_TYPE(pDataCol->type)) {
    dataColSetOffset(pDataCol, numOfRows);
  }
  return 0;
}

static int tsdbLoadBlockDataColsImpl(SReadH *pReadh, SBlock *pBlock, SDataCols *pDataCols, int16_t *colIds,
                                     int numOfColIds) {
  ASSERT(pBlock->numOfSubBlocks == 0 || pBlock->numOfSubBlocks == 1);
  ASSERT(colIds[0] == 0);

  SDFile *  pDFile = (pBlock->last) ? TSDB_READ_LAST_FILE(pReadh) : TSDB_READ_DATA_FILE(pReadh);
  SBlockCol blockCol = {0};

  tdResetDataCols(pDataCols);

  // If only load timestamp column, no need to load SBlockData part
  if (numOfColIds > 1 && tsdbLoadBlockStatis(pReadh, pBlock) < 0) return -1;

  pDataCols->numOfRows = pBlock->numOfRows;

  int dcol = 0;
  int ccol = 0;
  for (int i = 0; i < numOfColIds; i++) {
    int16_t    colId = colIds[i];
    SDataCol * pDataCol = NULL;
    SBlockCol *pBlockCol = NULL;

    while (true) {
      if (dcol >= pDataCols->numOfCols) {
        pDataCol = NULL;
        break;
      }
      pDataCol = &pDataCols->cols[dcol];
      if (pDataCol->colId > colId) {
        pDataCol = NULL;
        break;
      } else {
        dcol++;
        if (pDataCol->colId == colId) break;
      }
    }

    if (pDataCol == NULL) continue;
    ASSERT(pDataCol->colId == colId);

    if (colId == 0) {  // load the key row
      blockCol.colId = colId;
      blockCol.len = pBlock->keyLen;
      blockCol.type = pDataCol->type;
      blockCol.offset = TSDB_KEY_COL_OFFSET;
      pBlockCol = &blockCol;
    } else {  // load non-key rows
      while (true) {
        if (ccol >= pBlock->numOfCols) {
          pBlockCol = NULL;
          break;
        }

        pBlockCol = &(pReadh->pBlkData->cols[ccol]);
        if (pBlockCol->colId > colId) {
          pBlockCol = NULL;
          break;
        } else {
          ccol++;
          if (pBlockCol->colId == colId) break;
        }
      }

      if (pBlockCol == NULL) {
        dataColSetNEleNull(pDataCol, pBlock->numOfRows, pDataCols->maxPoints);
        continue;
      }

      ASSERT(pBlockCol->colId == pDataCol->colId);
    }

    if (tsdbLoadColData(pReadh, pDFile, pBlock, pBlockCol, pDataCol) < 0) return -1;
  }

  return 0;
}

static int tsdbLoadColData(SReadH *pReadh, SDFile *pDFile, SBlock *pBlock, SBlockCol *pBlockCol, SDataCol *pDataCol) {
  ASSERT(pDataCol->colId == pBlockCol->colId);

  STsdbRepo *pRepo = TSDB_READ_REPO(pReadh);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  int        tsize = pDataCol->bytes * pBlock->numOfRows + COMP_OVERFLOW_BYTES;

  if (tsdbMakeRoom((void **)(&TSDB_READ_BUF(pReadh)), pBlockCol->len) < 0) return -1;
  if (tsdbMakeRoom((void **)(&TSDB_READ_COMP_BUF(pReadh)), tsize) < 0) return -1;

  int64_t offset = pBlock->offset + TSDB_BLOCK_STATIS_SIZE(pBlock->numOfCols) + tsdbGetBlockColOffset(pBlockCol);
  if (tsdbSeekDFile(pDFile, offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to load block column data while seek file %s to offset %" PRId64 " since %s",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), offset, tstrerror(terrno));
    return -1;
  }

  int64_t nread = tsdbReadDFile(pDFile, TSDB_READ_BUF(pReadh), pBlockCol->len);
  if (nread < 0) {
    tsdbError("vgId:%d failed to load block column data while read file %s since %s, offset:%" PRId64 " len :%d",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), tstrerror(terrno), offset, pBlockCol->len);
    return -1;
  }

  if (nread < pBlockCol->len) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d block column data in file %s is corrupted, offset:%" PRId64 " expected bytes:%d" PRIzu
              " read bytes: %" PRId64,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), offset, pBlockCol->len, nread);
    return -1;
  }

  if (tsdbCheckAndDecodeColumnData(pDataCol, pReadh->pBuf, pBlockCol->len, pBlock->algorithm, pBlock->numOfRows,
                                   pCfg->maxRowsPerFileBlock, pReadh->pCBuf, (int32_t)taosTSizeof(pReadh->pCBuf)) < 0) {
    tsdbError("vgId:%d file %s is broken at column %d offset %" PRId64, REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile),
              pBlockCol->colId, offset);
    return -1;
  }

  return 0;
}

void *tsdbCommitData(STsdbRepo *pRepo) {
  tsdbStartCommit(pRepo);

  // Commit to update meta file
  if (tsdbCommitMeta(pRepo) < 0) {
    tsdbError("vgId:%d error occurs while committing META data since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  // Create the iterator to read from cache
  if (tsdbCommitTSData(pRepo) < 0) {
    tsdbError("vgId:%d error occurs while committing TS data since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  tsdbEndCommit(pRepo, TSDB_CODE_SUCCESS);
  return NULL;

_err:
  ASSERT(terrno != TSDB_CODE_SUCCESS);
  pRepo->code = terrno;

  tsdbEndCommit(pRepo, terrno);
  return NULL;
}

// =================== Commit Meta Data
static int tsdbCommitMeta(STsdbRepo *pRepo) {
  STsdbFS *  pfs = REPO_FS(pRepo);
  SMemTable *pMem = pRepo->imem;
  SMFile *   pOMFile = pfs->cstatus->pmf;
  SMFile     mf;
  SActObj *  pAct = NULL;
  SActCont * pCont = NULL;
  SListNode *pNode = NULL;
  SDiskID    did;

  ASSERT(pOMFile != NULL || listNEles(pMem->actList) > 0);

  if (listNEles(pMem->actList) <= 0) {
    // no meta data to commit, just keep the old meta file
    tsdbUpdateMFile(pfs, pOMFile);
    return 0;
  } else {
    // Create/Open a meta file or open the existing file
    if (pOMFile == NULL) {
      // Create a new meta file
      did.level = TFS_PRIMARY_LEVEL;
      did.id = TFS_PRIMARY_ID;
      tsdbInitMFile(&mf, did, REPO_ID(pRepo), FS_TXN_VERSION(REPO_FS(pRepo)));

      if (tsdbCreateMFile(&mf, true) < 0) {
        tsdbError("vgId:%d failed to create META file since %s", REPO_ID(pRepo), tstrerror(terrno));
        return -1;
      }

      tsdbInfo("vgId:%d meta file %s is created to commit", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(&mf));
    } else {
      tsdbInitMFileEx(&mf, pOMFile);
      if (tsdbOpenMFile(&mf, O_WRONLY) < 0) {
        tsdbError("vgId:%d failed to open META file since %s", REPO_ID(pRepo), tstrerror(terrno));
        return -1;
      }
    }
  }

  // Loop to write
  while ((pNode = tdListPopHead(pMem->actList)) != NULL) {
    pAct = (SActObj *)pNode->data;
    if (pAct->act == TSDB_UPDATE_META) {
      pCont = (SActCont *)POINTER_SHIFT(pAct, sizeof(SActObj));
      if (tsdbUpdateMetaRecord(pfs, &mf, pAct->uid, (void *)(pCont->cont), pCont->len) < 0) {
        tsdbError("vgId:%d failed to update META record, uid %" PRIu64 " since %s", REPO_ID(pRepo), pAct->uid,
                  tstrerror(terrno));
        tsdbCloseMFile(&mf);
        (void)tsdbApplyMFileChange(&mf, pOMFile);
        // TODO: need to reload metaCache
        return -1;
      }
    } else if (pAct->act == TSDB_DROP_META) {
      if (tsdbDropMetaRecord(pfs, &mf, pAct->uid) < 0) {
        tsdbError("vgId:%d failed to drop META record, uid %" PRIu64 " since %s", REPO_ID(pRepo), pAct->uid,
                  tstrerror(terrno));
        tsdbCloseMFile(&mf);
        tsdbApplyMFileChange(&mf, pOMFile);
        // TODO: need to reload metaCache
        return -1;
      }
    } else {
      ASSERT(false);
    }
  }

  if (tsdbUpdateMFileHeader(&mf) < 0) {
    tsdbError("vgId:%d failed to update META file header since %s, revert it", REPO_ID(pRepo), tstrerror(terrno));
    tsdbApplyMFileChange(&mf, pOMFile);
    // TODO: need to reload metaCache
    return -1;
  }

  TSDB_FILE_FSYNC(&mf);
  tsdbCloseMFile(&mf);
  tsdbUpdateMFile(pfs, &mf);

  return 0;
}

int tsdbEncodeKVRecord(void **buf, SKVRecord *pRecord) {
  int tlen = 0;
  tlen += taosEncodeFixedU64(buf, pRecord->uid);
  tlen += taosEncodeFixedI64(buf, pRecord->offset);
  tlen += taosEncodeFixedI64(buf, pRecord->size);

  return tlen;
}

void *tsdbDecodeKVRecord(void *buf, SKVRecord *pRecord) {
  buf = taosDecodeFixedU64(buf, &(pRecord->uid));
  buf = taosDecodeFixedI64(buf, &(pRecord->offset));
  buf = taosDecodeFixedI64(buf, &(pRecord->size));

  return buf;
}

void tsdbGetRtnSnap(STsdbRepo *pRepo, SRtn *pRtn) {
  STsdbCfg *pCfg = REPO_CFG(pRepo);
  TSKEY     minKey, midKey, maxKey, now;

  now = taosGetTimestamp(pCfg->precision);
  minKey = now - pCfg->keep * tsMsPerDay[pCfg->precision];
  midKey = now - pCfg->keep2 * tsMsPerDay[pCfg->precision];
  maxKey = now - pCfg->keep1 * tsMsPerDay[pCfg->precision];

  pRtn->minKey = minKey;
  pRtn->minFid = (int)(TSDB_KEY_FID(minKey, pCfg->daysPerFile, pCfg->precision));
  pRtn->midFid = (int)(TSDB_KEY_FID(midKey, pCfg->daysPerFile, pCfg->precision));
  pRtn->maxFid = (int)(TSDB_KEY_FID(maxKey, pCfg->daysPerFile, pCfg->precision));
  tsdbDebug("vgId:%d now:%" PRId64 " minKey:%" PRId64 " minFid:%d, midFid:%d, maxFid:%d", REPO_ID(pRepo), now, minKey,
            pRtn->minFid, pRtn->midFid, pRtn->maxFid);
}

static int tsdbUpdateMetaRecord(STsdbFS *pfs, SMFile *pMFile, uint64_t uid, void *cont, int contLen) {
  char      buf[64] = "\0";
  void *    pBuf = buf;
  SKVRecord rInfo;
  int64_t   offset;

  // Seek to end of meta file
  offset = tsdbSeekMFile(pMFile, 0, SEEK_END);
  if (offset < 0) {
    return -1;
  }

  rInfo.offset = offset;
  rInfo.uid = uid;
  rInfo.size = contLen;

  int tlen = tsdbEncodeKVRecord((void **)(&pBuf), &rInfo);
  if (tsdbAppendMFile(pMFile, buf, tlen, NULL) < tlen) {
    return -1;
  }

  if (tsdbAppendMFile(pMFile, cont, contLen, NULL) < contLen) {
    return -1;
  }

  tsdbUpdateMFileMagic(pMFile, POINTER_SHIFT(cont, contLen - sizeof(TSCKSUM)));
  SKVRecord *pRecord = taosHashGet(pfs->metaCache, (void *)&uid, sizeof(uid));
  if (pRecord != NULL) {
    pMFile->info.tombSize += (pRecord->size + sizeof(SKVRecord));
  } else {
    pMFile->info.nRecords++;
  }
  taosHashPut(pfs->metaCache, (void *)(&uid), sizeof(uid), (void *)(&rInfo), sizeof(rInfo));

  return 0;
}

static int tsdbDropMetaRecord(STsdbFS *pfs, SMFile *pMFile, uint64_t uid) {
  SKVRecord rInfo = {0};
  char      buf[128] = "\0";

  SKVRecord *pRecord = taosHashGet(pfs->metaCache, (void *)(&uid), sizeof(uid));
  if (pRecord == NULL) {
    tsdbError("failed to drop META record with key %" PRIu64 " since not find", uid);
    return -1;
  }

  rInfo.offset = -pRecord->offset;
  rInfo.uid = pRecord->uid;
  rInfo.size = pRecord->size;

  void *pBuf = buf;
  tsdbEncodeKVRecord(&pBuf, &rInfo);

  if (tsdbAppendMFile(pMFile, buf, sizeof(SKVRecord), NULL) < 0) {
    return -1;
  }

  pMFile->info.magic = taosCalcChecksum(pMFile->info.magic, (uint8_t *)buf, sizeof(SKVRecord));
  pMFile->info.nDels++;
  pMFile->info.nRecords--;
  pMFile->info.tombSize += (rInfo.size + sizeof(SKVRecord) * 2);

  taosHashRemove(pfs->metaCache, (void *)(&uid), sizeof(uid));
  return 0;
}

// =================== Commit Time-Series Data
static int tsdbCommitTSData(STsdbRepo *pRepo) {
  SMemTable *pMem = pRepo->imem;
  SCommitH   commith;
  SDFileSet *pSet = NULL;
  int        fid;

  memset(&commith, 0, sizeof(commith));

  if (pMem->numOfRows <= 0) {
    // No memory data, just apply retention on each file on disk
    if (tsdbApplyRtn(pRepo) < 0) {
      return -1;
    }
    return 0;
  }

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
  return 0;
}

static void tsdbStartCommit(STsdbRepo *pRepo) {
  SMemTable *pMem = pRepo->imem;

  ASSERT(pMem->numOfRows > 0 || listNEles(pMem->actList) > 0);

  tsdbInfo("vgId:%d start to commit! keyFirst %" PRId64 " keyLast %" PRId64 " numOfRows %" PRId64 " meta rows: %d",
           REPO_ID(pRepo), pMem->keyFirst, pMem->keyLast, pMem->numOfRows, listNEles(pMem->actList));

  tsdbStartFSTxn(pRepo, pMem->pointsAdd, pMem->storageAdd);

  pRepo->code = TSDB_CODE_SUCCESS;
}

static void tsdbEndCommit(STsdbRepo *pRepo, int eno) {
  if (eno != TSDB_CODE_SUCCESS) {
    tsdbEndFSTxnWithError(REPO_FS(pRepo));
  } else {
    tsdbEndFSTxn(pRepo);
  }

  tsdbInfo("vgId:%d commit over, %s", REPO_ID(pRepo), (eno == TSDB_CODE_SUCCESS) ? "succeed" : "failed");

  if (pRepo->appH.notifyStatus) pRepo->appH.notifyStatus(pRepo->appH.appH, TSDB_STATUS_COMMIT_OVER, eno);

  SMemTable *pIMem = pRepo->imem;
  (void)tsdbLockRepo(pRepo);
  pRepo->imem = NULL;
  (void)tsdbUnlockRepo(pRepo);
  tsdbUnRefMemTable(pRepo, pIMem);
  tsem_post(&(pRepo->readyToCommit));
}

#if 0
static bool tsdbHasDataToCommit(SCommitIter *iters, int nIters, TSKEY minKey, TSKEY maxKey) {
  for (int i = 0; i < nIters; i++) {
    TSKEY nextKey = tsdbNextIterKey((iters + i)->pIter);
    if (nextKey != TSDB_DATA_TIMESTAMP_NULL && (nextKey >= minKey && nextKey <= maxKey)) return true;
  }
  return false;
}
#endif

static int tsdbCommitToFile(SCommitH *pCommith, SDFileSet *pSet, int fid) {
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg * pCfg = REPO_CFG(pRepo);

  ASSERT(pSet == NULL || pSet->fid == fid);

  tsdbResetCommitFile(pCommith);
  tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, fid, &(pCommith->minKey), &(pCommith->maxKey));

  // Set and open files
  if (tsdbSetAndOpenCommitFile(pCommith, pSet, fid) < 0) {
    return -1;
  }

  // Loop to commit each table data
  for (int tid = 1; tid < pCommith->niters; tid++) {
    SCommitIter *pIter = pCommith->iters + tid;

    if (pIter->pTable == NULL) continue;

    if (tsdbCommitToTable(pCommith, tid) < 0) {
      tsdbCloseCommitFile(pCommith, true);
      // revert the file change
      tsdbApplyDFileSetChange(TSDB_COMMIT_WRITE_FSET(pCommith), pSet);
      return -1;
    }
  }

  if (tsdbWriteBlockIdx(pCommith) < 0) {
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
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  SMemTable *pMem = pRepo->imem;
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  pCommith->niters = pMem->maxTables;
  pCommith->iters = (SCommitIter *)calloc(pMem->maxTables, sizeof(SCommitIter));
  if (pCommith->iters == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (tsdbRLockRepoMeta(pRepo) < 0) return -1;

  // reference all tables
  for (int i = 0; i < pMem->maxTables; i++) {
    if (pMeta->tables[i] != NULL) {
      tsdbRefTable(pMeta->tables[i]);
      pCommith->iters[i].pTable = pMeta->tables[i];
    }
  }

  if (tsdbUnlockRepoMeta(pRepo) < 0) return -1;

  for (int i = 0; i < pMem->maxTables; i++) {
    if ((pCommith->iters[i].pTable != NULL) && (pMem->tData[i] != NULL) &&
        (TABLE_UID(pCommith->iters[i].pTable) == pMem->tData[i]->uid)) {
      if ((pCommith->iters[i].pIter = tSkipListCreateIter(pMem->tData[i]->pData)) == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }

      tSkipListIterNext(pCommith->iters[i].pIter);
    }
  }

  return 0;
}

static void tsdbDestroyCommitIters(SCommitH *pCommith) {
  if (pCommith->iters == NULL) return;

  for (int i = 1; i < pCommith->niters; i++) {
    if (pCommith->iters[i].pTable != NULL) {
      tsdbUnRefTable(pCommith->iters[i].pTable);
      tSkipListDestroyIter(pCommith->iters[i].pIter);
    }
  }

  free(pCommith->iters);
  pCommith->iters = NULL;
  pCommith->niters = 0;
}

// Skip all keys until key (not included)
static void tsdbSeekCommitIter(SCommitH *pCommith, TSKEY key) {
  for (int i = 0; i < pCommith->niters; i++) {
    SCommitIter *pIter = pCommith->iters + i;
    if (pIter->pTable == NULL || pIter->pIter == NULL) continue;

    tsdbLoadDataFromCache(pIter->pTable, pIter->pIter, key - 1, INT32_MAX, NULL, NULL, 0, true, NULL);
  }
}

static int tsdbInitCommitH(SCommitH *pCommith, STsdbRepo *pRepo) {
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

  pCommith->pDataCols = tdNewDataCols(0, 0, pCfg->maxRowsPerFileBlock);
  if (pCommith->pDataCols == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyCommitH(pCommith);
    return -1;
  }

  return 0;
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

static int tsdbNextCommitFid(SCommitH *pCommith) {
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  int        fid = TSDB_IVLD_FID;

  for (int i = 0; i < pCommith->niters; i++) {
    SCommitIter *pIter = pCommith->iters + i;
    if (pIter->pTable == NULL || pIter->pIter == NULL) continue;

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

static int tsdbCommitToTable(SCommitH *pCommith, int tid) {
  SCommitIter *pIter = pCommith->iters + tid;
  TSKEY        nextKey = tsdbNextIterKey(pIter->pIter);

  tsdbResetCommitTable(pCommith);

  TSDB_RLOCK_TABLE(pIter->pTable);

  // Set commit table
  if (tsdbSetCommitTable(pCommith, pIter->pTable) < 0) {
    TSDB_RUNLOCK_TABLE(pIter->pTable);
    return -1;
  }

  // No disk data and no memory data, just return
  if (pCommith->readh.pBlkIdx == NULL && (nextKey == TSDB_DATA_TIMESTAMP_NULL || nextKey > pCommith->maxKey)) {
    TSDB_RUNLOCK_TABLE(pIter->pTable);
    return 0;
  }

  // Must has disk data or has memory data
  int     nBlocks;
  int     bidx = 0;
  SBlock *pBlock;

  if (pCommith->readh.pBlkIdx) {
    if (tsdbLoadBlockInfo(&(pCommith->readh), NULL) < 0) {
      TSDB_RUNLOCK_TABLE(pIter->pTable);
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
        TSDB_RUNLOCK_TABLE(pIter->pTable);
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
        TSDB_RUNLOCK_TABLE(pIter->pTable);
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
          TSDB_RUNLOCK_TABLE(pIter->pTable);
          return -1;
        }
      } else {
        if (tsdbCommitMemData(pCommith, pIter, pBlock->keyFirst - 1, true) < 0) {
          TSDB_RUNLOCK_TABLE(pIter->pTable);
          return -1;
        }
      }
      nextKey = tsdbNextIterKey(pIter->pIter);
    }
  }

  TSDB_RUNLOCK_TABLE(pIter->pTable);

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

static int tsdbWriteBlock(SCommitH *pCommith, SDFile *pDFile, SDataCols *pDataCols, SBlock *pBlock, bool isLast,
                          bool isSuper) {
  STsdbRepo * pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg *  pCfg = REPO_CFG(pRepo);
  SBlockData *pBlockData;
  int64_t     offset = 0;
  STable *    pTable = TSDB_COMMIT_TABLE(pCommith);
  int         rowsToWrite = pDataCols->numOfRows;

  ASSERT(rowsToWrite > 0 && rowsToWrite <= pCfg->maxRowsPerFileBlock);
  ASSERT((!isLast) || rowsToWrite < pCfg->minRowsPerFileBlock);

  // Make buffer space
  if (tsdbMakeRoom((void **)(&TSDB_COMMIT_BUF(pCommith)), TSDB_BLOCK_STATIS_SIZE(pDataCols->numOfCols)) < 0) {
    return -1;
  }
  pBlockData = (SBlockData *)TSDB_COMMIT_BUF(pCommith);

  // Get # of cols not all NULL(not including key column)
  int nColsNotAllNull = 0;
  for (int ncol = 1; ncol < pDataCols->numOfCols; ncol++) {  // ncol from 1, we skip the timestamp column
    SDataCol * pDataCol = pDataCols->cols + ncol;
    SBlockCol *pBlockCol = pBlockData->cols + nColsNotAllNull;

    if (isNEleNull(pDataCol, rowsToWrite)) {  // all data to commit are NULL, just ignore it
      continue;
    }

    memset(pBlockCol, 0, sizeof(*pBlockCol));

    pBlockCol->colId = pDataCol->colId;
    pBlockCol->type = pDataCol->type;
    if (tDataTypes[pDataCol->type].statisFunc) {
      (*tDataTypes[pDataCol->type].statisFunc)(pDataCol->pData, rowsToWrite, &(pBlockCol->min), &(pBlockCol->max),
                                               &(pBlockCol->sum), &(pBlockCol->minIndex), &(pBlockCol->maxIndex),
                                               &(pBlockCol->numOfNull));
    }
    nColsNotAllNull++;
  }

  ASSERT(nColsNotAllNull >= 0 && nColsNotAllNull <= pDataCols->numOfCols);

  // Compress the data if neccessary
  int      tcol = 0;  // counter of not all NULL and written columns
  uint32_t toffset = 0;
  int32_t  tsize = TSDB_BLOCK_STATIS_SIZE(nColsNotAllNull);
  int32_t  lsize = tsize;
  int32_t  keyLen = 0;
  for (int ncol = 0; ncol < pDataCols->numOfCols; ncol++) {
    // All not NULL columns finish
    if (ncol != 0 && tcol >= nColsNotAllNull) break;

    SDataCol * pDataCol = pDataCols->cols + ncol;
    SBlockCol *pBlockCol = pBlockData->cols + tcol;

    if (ncol != 0 && (pDataCol->colId != pBlockCol->colId)) continue;

    int32_t flen;  // final length
    int32_t tlen = dataColGetNEleLen(pDataCol, rowsToWrite);
    void *  tptr;

    // Make room
    if (tsdbMakeRoom((void **)(&TSDB_COMMIT_BUF(pCommith)), lsize + tlen + COMP_OVERFLOW_BYTES + sizeof(TSCKSUM)) < 0) {
      return -1;
    }
    pBlockData = (SBlockData *)TSDB_COMMIT_BUF(pCommith);
    pBlockCol = pBlockData->cols + tcol;
    tptr = POINTER_SHIFT(pBlockData, lsize);

    if (pCfg->compression == TWO_STAGE_COMP &&
        tsdbMakeRoom((void **)(&TSDB_COMMIT_COMP_BUF(pCommith)), tlen + COMP_OVERFLOW_BYTES) < 0) {
      return -1;
    }

    // Compress or just copy
    if (pCfg->compression) {
      flen = (*(tDataTypes[pDataCol->type].compFunc))((char *)pDataCol->pData, tlen, rowsToWrite, tptr,
                                                      tlen + COMP_OVERFLOW_BYTES, pCfg->compression,
                                                      TSDB_COMMIT_COMP_BUF(pCommith), tlen + COMP_OVERFLOW_BYTES);
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
      tcol++;
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

  tsdbDebug("vgId:%d tid:%d a block of data is written to file %s, offset %" PRId64
            " numOfRows %d len %d numOfCols %" PRId16 " keyFirst %" PRId64 " keyLast %" PRId64,
            REPO_ID(pRepo), TABLE_TID(pTable), TSDB_FILE_FULL_NAME(pDFile), offset, rowsToWrite, pBlock->len,
            pBlock->numOfCols, pBlock->keyFirst, pBlock->keyLast);

  return 0;
}

static int tsdbWriteBlockInfo(SCommitH *pCommih) {
  SDFile *    pHeadf = TSDB_COMMIT_HEAD_FILE(pCommih);
  SBlockIdx   blkIdx;
  STable *    pTable = TSDB_COMMIT_TABLE(pCommih);
  SBlock *    pBlock;
  size_t      nSupBlocks;
  size_t      nSubBlocks;
  uint32_t    tlen;
  SBlockInfo *pBlkInfo;
  int64_t     offset;

  nSupBlocks = taosArrayGetSize(pCommih->aSupBlk);
  nSubBlocks = taosArrayGetSize(pCommih->aSubBlk);

  if (nSupBlocks <= 0) {
    // No data (data all deleted)
    return 0;
  }

  tlen = (uint32_t)(sizeof(SBlockInfo) + sizeof(SBlock) * (nSupBlocks + nSubBlocks) + sizeof(TSCKSUM));

  // Write SBlockInfo part
  if (tsdbMakeRoom((void **)(&(TSDB_COMMIT_BUF(pCommih))), tlen) < 0) return -1;
  pBlkInfo = TSDB_COMMIT_BUF(pCommih);

  pBlkInfo->delimiter = TSDB_FILE_DELIMITER;
  pBlkInfo->tid = TABLE_TID(pTable);
  pBlkInfo->uid = TABLE_UID(pTable);

  memcpy((void *)(pBlkInfo->blocks), taosArrayGet(pCommih->aSupBlk, 0), nSupBlocks * sizeof(SBlock));
  if (nSubBlocks > 0) {
    memcpy((void *)(pBlkInfo->blocks + nSupBlocks), taosArrayGet(pCommih->aSubBlk, 0), nSubBlocks * sizeof(SBlock));

    for (int i = 0; i < nSupBlocks; i++) {
      pBlock = pBlkInfo->blocks + i;

      if (pBlock->numOfSubBlocks > 1) {
        pBlock->offset += (sizeof(SBlockInfo) + sizeof(SBlock) * nSupBlocks);
      }
    }
  }

  taosCalcChecksumAppend(0, (uint8_t *)pBlkInfo, tlen);

  if (tsdbAppendDFile(pHeadf, TSDB_COMMIT_BUF(pCommih), tlen, &offset) < 0) {
    return -1;
  }

  tsdbUpdateDFileMagic(pHeadf, POINTER_SHIFT(pBlkInfo, tlen - sizeof(TSCKSUM)));

  // Set blkIdx
  pBlock = taosArrayGet(pCommih->aSupBlk, nSupBlocks - 1);

  blkIdx.tid = TABLE_TID(pTable);
  blkIdx.uid = TABLE_UID(pTable);
  blkIdx.hasLast = pBlock->last ? 1 : 0;
  blkIdx.maxKey = pBlock->keyLast;
  blkIdx.numOfBlocks = (uint32_t)nSupBlocks;
  blkIdx.len = tlen;
  blkIdx.offset = (uint32_t)offset;

  ASSERT(blkIdx.numOfBlocks > 0);

  if (taosArrayPush(pCommih->aBlkIdx, (void *)(&blkIdx)) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int tsdbWriteBlockIdx(SCommitH *pCommih) {
  SBlockIdx *pBlkIdx;
  SDFile *   pHeadf = TSDB_COMMIT_HEAD_FILE(pCommih);
  size_t     nidx = taosArrayGetSize(pCommih->aBlkIdx);
  int        tlen = 0, size;
  int64_t    offset;

  if (nidx <= 0) {
    // All data are deleted
    pHeadf->info.offset = 0;
    pHeadf->info.len = 0;
    return 0;
  }

  for (size_t i = 0; i < nidx; i++) {
    pBlkIdx = (SBlockIdx *)taosArrayGet(pCommih->aBlkIdx, i);

    size = tsdbEncodeSBlockIdx(NULL, pBlkIdx);
    if (tsdbMakeRoom((void **)(&TSDB_COMMIT_BUF(pCommih)), tlen + size) < 0) return -1;

    void *ptr = POINTER_SHIFT(TSDB_COMMIT_BUF(pCommih), tlen);
    tsdbEncodeSBlockIdx(&ptr, pBlkIdx);

    tlen += size;
  }

  tlen += sizeof(TSCKSUM);
  if (tsdbMakeRoom((void **)(&TSDB_COMMIT_BUF(pCommih)), tlen) < 0) return -1;
  taosCalcChecksumAppend(0, (uint8_t *)TSDB_COMMIT_BUF(pCommih), tlen);

  if (tsdbAppendDFile(pHeadf, TSDB_COMMIT_BUF(pCommih), tlen, &offset) < tlen) {
    tsdbError("vgId:%d failed to write block index part to file %s since %s", TSDB_COMMIT_REPO_ID(pCommih),
              TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno));
    return -1;
  }

  tsdbUpdateDFileMagic(pHeadf, POINTER_SHIFT(TSDB_COMMIT_BUF(pCommih), tlen - sizeof(TSCKSUM)));
  pHeadf->info.offset = (uint32_t)offset;
  pHeadf->info.len = tlen;

  return 0;
}

static int tsdbCommitMemData(SCommitH *pCommith, SCommitIter *pIter, TSKEY keyLimit, bool toData) {
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  SMergeInfo mInfo;
  int32_t    defaultRows = TSDB_COMMIT_DEFAULT_ROWS(pCommith);
  SDFile *   pDFile;
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
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  int        nBlocks = pCommith->readh.pBlkIdx->numOfBlocks;
  SBlock *   pBlock = pCommith->readh.pBlkInfo->blocks + bidx;
  TSKEY      keyLimit;
  int16_t    colId = 0;
  SMergeInfo mInfo;
  SBlock     subBlocks[TSDB_MAX_SUBBLOCKS];
  SBlock     block, supBlock;
  SDFile *   pDFile;

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

  if (pSubBlocks && taosArrayPushBatch(pCommith->aSubBlk, pSubBlocks, nSubBlocks) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int tsdbMergeBlockData(SCommitH *pCommith, SCommitIter *pIter, SDataCols *pDataCols, TSKEY keyLimit,
                              bool isLastOneBlock) {
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  SBlock     block;
  SDFile *   pDFile;
  bool       isLast;
  int32_t    defaultRows = TSDB_COMMIT_DEFAULT_ROWS(pCommith);

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
    bool     isRowDel = false;
    SDataRow row = tsdbNextIterRow(pCommitIter->pIter);
    if (row == NULL || dataRowKey(row) > maxKey) {
      key2 = INT64_MAX;
    } else {
      key2 = dataRowKey(row);
      isRowDel = dataRowDeleted(row);
    }

    if (key1 == INT64_MAX && key2 == INT64_MAX) break;

    if (key1 < key2) {
      for (int i = 0; i < pDataCols->numOfCols; i++) {
        dataColAppendVal(pTarget->cols + i, tdGetColDataOfRow(pDataCols->cols + i, *iter), pTarget->numOfRows,
                         pTarget->maxPoints);
      }

      pTarget->numOfRows++;
      (*iter)++;
    } else if (key1 > key2) {
      if (!isRowDel) {
        if (pSchema == NULL || schemaVersion(pSchema) != dataRowVersion(row)) {
          pSchema = tsdbGetTableSchemaImpl(pCommitIter->pTable, false, false, dataRowVersion(row));
          ASSERT(pSchema != NULL);
        }

        tdAppendDataRowToDataCol(row, pSchema, pTarget);
      }

      tSkipListIterNext(pCommitIter->pIter);
    } else {
      if (update) {
        if (!isRowDel) {
          if (pSchema == NULL || schemaVersion(pSchema) != dataRowVersion(row)) {
            pSchema = tsdbGetTableSchemaImpl(pCommitIter->pTable, false, false, dataRowVersion(row));
            ASSERT(pSchema != NULL);
          }

          tdAppendDataRowToDataCol(row, pSchema, pTarget);
        }
      } else {
        ASSERT(!isRowDel);

        for (int i = 0; i < pDataCols->numOfCols; i++) {
          dataColAppendVal(pTarget->cols + i, tdGetColDataOfRow(pDataCols->cols + i, *iter), pTarget->numOfRows,
                           pTarget->maxPoints);
        }

        pTarget->numOfRows++;
      }
      (*iter)++;
      tSkipListIterNext(pCommitIter->pIter);
    }

    if (pTarget->numOfRows >= maxRows) break;
  }
}

static void tsdbResetCommitFile(SCommitH *pCommith) {
  pCommith->isRFileSet = false;
  pCommith->isDFileSame = false;
  pCommith->isLFileSame = false;
  taosArrayClear(pCommith->aBlkIdx);
}

static void tsdbResetCommitTable(SCommitH *pCommith) {
  taosArrayClear(pCommith->aSubBlk);
  taosArrayClear(pCommith->aSupBlk);
  pCommith->pTable = NULL;
}

static int tsdbSetAndOpenCommitFile(SCommitH *pCommith, SDFileSet *pSet, int fid) {
  SDiskID    did;
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  SDFileSet *pWSet = TSDB_COMMIT_WRITE_FSET(pCommith);

  tfsAllocDisk(tsdbGetFidLevel(fid, &(pCommith->rtn)), &(did.level), &(did.id));
  if (did.level == TFS_UNDECIDED_LEVEL) {
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
    tsdbInitDFileSet(pWSet, did, REPO_ID(pRepo), fid, FS_TXN_VERSION(REPO_FS(pRepo)));

    if (tsdbCreateDFileSet(pWSet, true) < 0) {
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
    tsdbInitDFile(pWHeadf, did, REPO_ID(pRepo), fid, FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_FILE_HEAD);
    if (tsdbCreateDFile(pWHeadf, true) < 0) {
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
    if (tsdbOpenDFile(pWDataf, O_WRONLY) < 0) {
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

      if (tsdbOpenDFile(pWLastf, O_WRONLY) < 0) {
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
      tsdbInitDFile(pWLastf, did, REPO_ID(pRepo), fid, FS_TXN_VERSION(REPO_FS(pRepo)), TSDB_FILE_LAST);
      pCommith->isLFileSame = false;

      if (tsdbCreateDFile(pWLastf, true) < 0) {
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
  }

  return 0;
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
  STsdbRepo *pRepo = TSDB_COMMIT_REPO(pCommith);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  int        mergeRows = pBlock->numOfRows + pInfo->rowsInserted - pInfo->rowsDeleteSucceed;

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

static int tsdbApplyRtn(STsdbRepo *pRepo) {
  SRtn       rtn;
  SFSIter    fsiter;
  STsdbFS *  pfs = REPO_FS(pRepo);
  SDFileSet *pSet;

  // Get retention snapshot
  tsdbGetRtnSnap(pRepo, &rtn);

  tsdbFSIterInit(&fsiter, pfs, TSDB_FS_ITER_FORWARD);
  while ((pSet = tsdbFSIterNext(&fsiter))) {
    if (pSet->fid < rtn.minFid) {
      tsdbInfo("vgId:%d FSET %d at level %d disk id %d expires, remove it", REPO_ID(pRepo), pSet->fid,
               TSDB_FSET_LEVEL(pSet), TSDB_FSET_ID(pSet));
      continue;
    }

    if (tsdbApplyRtnOnFSet(pRepo, pSet, &rtn) < 0) {
      return -1;
    }
  }

  return 0;
}

static int tsdbApplyRtnOnFSet(STsdbRepo *pRepo, SDFileSet *pSet, SRtn *pRtn) {
  SDiskID   did;
  SDFileSet nSet;
  STsdbFS * pfs = REPO_FS(pRepo);
  int       level;

  ASSERT(pSet->fid >= pRtn->minFid);

  level = tsdbGetFidLevel(pSet->fid, pRtn);

  tfsAllocDisk(level, &(did.level), &(did.id));
  if (did.level == TFS_UNDECIDED_LEVEL) {
    terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
    return -1;
  }

  if (did.level > TSDB_FSET_LEVEL(pSet)) {
    // Need to move the FSET to higher level
    tsdbInitDFileSet(&nSet, did, REPO_ID(pRepo), pSet->fid, FS_TXN_VERSION(pfs));

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

#endif