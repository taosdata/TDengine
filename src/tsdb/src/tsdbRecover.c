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

extern uint16_t tsTsdbCheckMode;

#define TSDB_FNAME_PREFIX_TMP "t."

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
static int  tsdbBackUpDFileSet(STsdbRepo *pRepo, SDFileSet *pFileSet);
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
static int tsdbCloseHFile(SDFile *pDFile);

static void tsdbGetDataBakPath(int repoid, SDFile *pDFile, char dirName[]);

int tsdbRecoverDataMain(STsdbRepo *pRepo) {
  SRecoverH recoverH;
  SReadH *  pReadH = &recoverH.readh;
  SArray *  fSetArray = NULL;  // SDFileSet array
  STsdbFS * pfs = REPO_FS(pRepo);

  if (tsdbFetchDFileSet(pRepo, &fSetArray) < 0) {
    if (TSDB_CODE_TDB_NO_AVAIL_DFILE != terrno) {
      tsdbError("vgId:%d failed to fetch DFileSet to restore since %s", REPO_ID(pRepo), tstrerror(terrno));
    }
    return -1;
  }

  if (taosArrayGetSize(fSetArray) <= 0) {
    taosArrayDestroy(fSetArray);
    tsdbInfo("vgId:%d no need to restore since empty DFileSet", REPO_ID(pRepo));
    return 0;
  }

  if (tsdbInitRecoverH(&recoverH, pRepo) < 0) {
    tsdbError("vgId:%d failed to init restore handle since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  tsdbInfo("vgId:%d restore with DFileSet size %" PRIu64, REPO_ID(pRepo), taosArrayGetSize(fSetArray));

  // check for each SDFileSet
  for (size_t iDFileSet = 0; iDFileSet < taosArrayGetSize(fSetArray); ++iDFileSet) {
    pReadH->rSet = *(SDFileSet *)taosArrayGet(fSetArray, iDFileSet);

    if (tsdbRecoverManager(&recoverH) < 0) {
      tsdbError("vgId:%d failed to restore DFileSet %d since %s", REPO_ID(pRepo), pReadH->rSet.fid, tstrerror(terrno));
      // backup the SDFileSet
      if (tsdbBackUpDFileSet(pRepo, &pReadH->rSet) < 0) {
        tsdbError("vgId:%d failed to backup DFileSet %d since %s", REPO_ID(pRepo), pReadH->rSet.fid, tstrerror(terrno));
        return -1;
      }
      // check next SDFileSet although return not zero
      continue;
    }

    tsdbInfo("vgId:%d FSET %d is checked in mode %" PRIu16, REPO_ID(pRepo), pReadH->rSet.fid, tsTsdbCheckMode);
    taosArrayPush(pfs->cstatus->df, &pReadH->rSet);
  }

  // release resources
  taosArrayDestroy(fSetArray);
  // release pRecoverH
  tsdbDestoryRecoverH(&recoverH);

  return 0;
}

static void tsdbGetDataBakPath(int repoid, SDFile *pDFile, char dirName[]) {
  char root_dname[TSDB_FILENAME_LEN - 32] = "\0";
  snprintf(root_dname, strlen(pDFile->f.aname) - strlen(pDFile->f.rname), "%s", pDFile->f.aname);
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/vnode_bak/.tsdb/vnode%d", root_dname, repoid);
}

// path:    vnode_bak/.tsdb/${unix_ts_seconds}.fileName
// expire:  default(half year, or 500G)
static int tsdbBackUpDFileSet(STsdbRepo *pRepo, SDFileSet *pFileSet) {
  int32_t ts = taosGetTimestampSec();
  for (TSDB_FILE_T ftype = TSDB_FILE_HEAD; ftype < TSDB_FILE_MAX; ++ftype) {
    SDFile *pDFile = &(pFileSet->files[ftype]);
    char    bname[TSDB_FILENAME_LEN] = "\0";
    char    dest_aname[TSDB_FILENAME_LEN] = "\0";

    tfsbasename(&(pDFile->f), bname);

    tsdbGetDataBakPath(REPO_ID(pRepo), pDFile, dest_aname);

    if (taosMkDir(dest_aname, 0755) < 0) {  // make sure the parent path already exists
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }

    snprintf(dest_aname + strlen(dest_aname), TSDB_FILENAME_LEN - strlen(dest_aname), "/%" PRId32 ".%s", ts, bname);

    if (taosRename(pDFile->f.aname, dest_aname) < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
    tsdbInfo("vgId:%d success to back up from %s to %s", REPO_ID(pRepo), pDFile->f.aname, dest_aname);
  }
  return 0;
}

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
  SReadH *   pReadH = &pRecoverH->readh;
  STsdbRepo *pRepo = pReadH->pRepo;
  int        result = 0;

  // init
  if (tsdbSetAndOpenReadFSet(pReadH, &pReadH->rSet) < 0) {
    return -1;
  }

  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ++ftype) {
    SDFile *pDFile = TSDB_DFILE_IN_SET(&pReadH->rSet, ftype);
    // TODO:QA: If header of .head/.data/.last corrupted, the check of one fset would fail.
    if (tsdbLoadDFileHeader(pDFile, &(pDFile->info)) < 0) {
      tsdbError("vgId:%d failed to load DFile %s header since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile),
                tstrerror(terrno));
      tsdbCloseDFileSet(TSDB_READ_FSET(pReadH));
      return -1;
    }
  }

  // process
  switch (tsTsdbCheckMode) {
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
  SDFile *pTmpHeadF = TSDB_RECOVER_WHEAD_FILE(pRecoverH);
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
    tsdbInfo("vgId:%d failed to init %s since %s", TSDB_READ_REPO_ID(pReadH), TSDB_FILE_FULL_NAME(pTmpHeadF),
             tstrerror(terrno));
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

      if (supBlk.numOfSubBlocks > 1) {
        supBlk.offset = sizeof(SBlock) * taosArrayGetSize(pRecoverH->aSubBlk);
      }

      if (tsdbHeadAddBlock(pRecoverH, &supBlk,
                           supBlk.numOfSubBlocks > 1 ? POINTER_SHIFT(pReadH->pBlkInfo, pSupBlk->offset) : NULL,
                           pSupBlk->numOfSubBlocks) < 0) {
        if (tsdbRecoverIsFatalError()) {
          tsdbDestroyHFile(pTmpHeadF);
          return -1;
        }
        continue;
      }
    }
    if (tsdbHeadWriteBlockInfo(pRecoverH) < 0) {
      tsdbError("vgId:%d failed to write SBlockInfo part into file %s since %s", REPO_ID(pReadH->pRepo),
                TSDB_FILE_FULL_NAME(pTmpHeadF), tstrerror(terrno));
      if (tsdbRecoverIsFatalError()) {
        tsdbDestroyHFile(pTmpHeadF);
        return -1;
      }
      continue;
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

  if (tsdbUpdateDFileHeader(pTmpHeadF) < 0) {
    tsdbError("vgId:%d failed to update header of file %s since %s", REPO_ID(pReadH->pRepo),
              TSDB_FILE_FULL_NAME(pTmpHeadF), tstrerror(terrno));
    tsdbDestroyHFile(pTmpHeadF);
    return -1;
  }

  // use the rebuilt .head file if partial pass
  if ((nSupBlkCorrupted > 0) || (nBlkIdxChkPassed != taosArrayGetSize(pReadH->aBlkIdx))) {
    tsdbInfo("vgId:%d partial pass the chksum scan. nBlkIdxScan %" PRIu64 ", nBlkIdxAll %" PRIu64
             ", nSupBlkCorrupt %" PRIu64 ", nSupBlkScan %" PRIu64 ", file %s ",
             REPO_ID(pReadH->pRepo), nBlkIdxChkPassed, taosArrayGetSize(pReadH->aBlkIdx), nSupBlkCorrupted,
             nSupBlkScanned, TSDB_FILE_FULL_NAME(pTmpHeadF));
    // rename t.vdfdddd.head{-ver2}. Use the prefix but not suffix to avoid error
    if (tsdbRenameDFile(pTmpHeadF, pHeadF) < 0) {  // fsync/close/rename
      tsdbDestroyHFile(pTmpHeadF);
      return -1;
    }
    // update the head file info in rset, which would be stored in cstatus->df as to generate the current file.
    tsdbInfo("vgId:%d partial pass the chksum scan and head info updated. size:%" PRIu64 "->%" PRIu64
             ", offset:%" PRIu32 "->%" PRIu32 ", len:%" PRIu32 "->%" PRIu32,
             REPO_ID(pReadH->pRepo), pHeadF->info.size, pTmpHeadF->info.size, pHeadF->info.offset,
             pTmpHeadF->info.offset, pHeadF->info.len, pTmpHeadF->info.len);
    pHeadF->info = pTmpHeadF->info;
  } else {
    tsdbInfo("vgId:%d all pass the chksum scan. nBlkIdxScan %" PRIu64 ", nBlkIdxAll %" PRIu64
             ", nSupBlkCorrupt %" PRIu64 ", nSupBlkScan %" PRIu64 ", file %s ",
             REPO_ID(pReadH->pRepo), nBlkIdxChkPassed, taosArrayGetSize(pReadH->aBlkIdx), nSupBlkCorrupted,
             nSupBlkScanned, TSDB_FILE_FULL_NAME(pTmpHeadF));
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
  SDFile *    pWHeadf = TSDB_RECOVER_WHEAD_FILE(pRecoverH);
  SBlockIdx * pBlkIdx = pReadH->pBlkIdx;
  SBlockIdx   blkIdx;
  SBlock *    pBlock = NULL;
  size_t      nSupBlocks = 0;
  size_t      nSubBlocks = 0;
  uint32_t    tlen = 0;
  SBlockInfo *pBlkInfo = NULL;
  int64_t     offset = 0;

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

    for (int i = 0; i < nSupBlocks; ++i) {
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
  SDFile *   pWHeadf = TSDB_RECOVER_WHEAD_FILE(pRecoverH);
  SBlockIdx *pBlkIdx = NULL;
  size_t     nidx = taosArrayGetSize(pRecoverH->aBlkIdx);
  int        tlen = 0, size = 0;
  int64_t    offset = 0;

  if (nidx <= 0) {
    // All data are deleted
    pWHeadf->info.offset = 0;
    pWHeadf->info.len = 0;
    return 0;
  }

  for (size_t i = 0; i < nidx; ++i) {
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

// Just verify checksum above, no real data to assert.
#if 0
  ASSERT(pReadH->pDCols[0]->numOfRows == pSupBlock->numOfRows);
  ASSERT(dataColsKeyFirst(pReadH->pDCols[0]) == pSupBlock->keyFirst);
  ASSERT(dataColsKeyLast(pReadH->pDCols[0]) == pSupBlock->keyLast);
#endif

  return 0;
}

static int tsdbInitHFile(SDFile *pDestDFile, const SDFile *pSrcDFile) {
  const TFILE *pf = &pSrcDFile->f;
  int          tvid = -1;
  int          tfid = -1;
  TSDB_FILE_T  ttype = TSDB_FILE_MAX;
  uint32_t     tversion = -1;
  char         bname[TSDB_FILENAME_LEN] = "\0";   // basename
  char         dname[TSDB_FILENAME_LEN] = "\0";   // absolute dir
  char         rdname[TSDB_FILENAME_LEN] = "\0";  // relative dir
  // destHFile name
  char dest_aname[TSDB_FILENAME_LEN] = "\0";
  char dest_rname[TSDB_FILENAME_LEN] = "\0";

  memset(pDestDFile, 0, sizeof(SDFile));
  pDestDFile->info.magic = TSDB_FILE_INIT_MAGIC;

  tfsdirname(pf, dname);
  tfsrdirname(pf, rdname);
  tfsbasename(pf, bname);

  tsdbParseDFilename(bname, &tvid, &tfid, &ttype, &tversion);

  ASSERT(tvid != -1 && tfid != -1 && ttype < TSDB_FILE_MAX && tversion != -1);

  tsdbGetFilePathNameByPrefix(dest_aname, tvid, tfid, tversion, ttype, TSDB_FNAME_PREFIX_TMP, dname);
  tsdbGetFilePathNameByPrefix(dest_rname, tvid, tfid, tversion, ttype, TSDB_FNAME_PREFIX_TMP, rdname);

  tstrncpy(pDestDFile->f.aname, dest_aname, sizeof(dest_aname));
  tstrncpy(pDestDFile->f.rname, dest_rname, sizeof(dest_rname));

  if (tsdbCreateDFile(pDestDFile, true) < 0) {
    return -1;
  }

  return 0;
}

static int tsdbDestroyHFile(SDFile *pDFile) {
  tsdbCloseHFile(pDFile);
  return tsdbRemoveDFile(pDFile);
}

static int tsdbCloseHFile(SDFile *pDFile) {
  if (TSDB_FILE_OPENED(pDFile)) {
    TSDB_FILE_FSYNC(pDFile);
    tsdbCloseDFile(pDFile);
  }
  return 0;
}

static int tsdbCheckBlockDataColsChkSum(SReadH *pReadH, SBlock *pBlock, SDataCols *pDataCols) {
  ASSERT(pBlock->numOfSubBlocks == 0 || pBlock->numOfSubBlocks == 1);

  SDFile *pDFile = (pBlock->last) ? TSDB_READ_LAST_FILE(pReadH) : TSDB_READ_DATA_FILE(pReadH);

  tdResetDataCols(pDataCols);

  if (tsdbMakeRoom((void **)(&TSDB_READ_BUF(pReadH)), pBlock->len) < 0) return -1;

  SBlockData *pBlockData = (SBlockData *)TSDB_READ_BUF(pReadH);

  if (tsdbSeekDFile(pDFile, pBlock->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to load block data part while seek file %s to offset %" PRId64 " since %s",
              TSDB_READ_REPO_ID(pReadH), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, tstrerror(terrno));
    return -1;
  }

  int64_t nread = tsdbReadDFile(pDFile, TSDB_READ_BUF(pReadH), pBlock->len);
  if (nread < 0) {
    tsdbError("vgId:%d failed to load block data part while read file %s since %s, offset:%" PRId64 " len :%d",
              TSDB_READ_REPO_ID(pReadH), TSDB_FILE_FULL_NAME(pDFile), tstrerror(terrno), (int64_t)pBlock->offset,
              pBlock->len);
    return -1;
  }

  if (nread < pBlock->len) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d block data part in file %s is corrupted, offset:%" PRId64
              " expected bytes:%d read bytes: %" PRId64,
              TSDB_READ_REPO_ID(pReadH), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, pBlock->len, nread);
    return -1;
  }

  uint32_t tsize = TSDB_BLOCK_STATIS_SIZE(pBlock->numOfCols);
  if (!taosCheckChecksumWhole((uint8_t *)TSDB_READ_BUF(pReadH), tsize)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d block statis part in file %s is corrupted since wrong checksum, offset:%" PRId64 " len :%u",
              TSDB_READ_REPO_ID(pReadH), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, tsize);
    return -1;
  }

  ASSERT(tsize < pBlock->len);
  ASSERT(pBlockData->numOfCols == pBlock->numOfCols);

  for (int iCol = 0; iCol < pBlockData->numOfCols; ++iCol) {
    SBlockCol *pBlockCol = pBlockData->cols + iCol;
    uint32_t   toffset = tsdbGetBlockColOffset(pBlockCol);
    int32_t    tlen = pBlockCol->len;
    int16_t    tcolId = pBlockCol->colId;

    if (!taosCheckChecksumWhole((uint8_t *)POINTER_SHIFT(pBlockData, tsize + toffset), tlen)) {
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
      tsdbError("vgId:%d file %s is broken at column %d block offset %" PRId64 " column offset %u",
                TSDB_READ_REPO_ID(pReadH), TSDB_FILE_FULL_NAME(pDFile), tcolId, (int64_t)pBlock->offset, toffset);
      return -1;
    }
  }
  return 0;
}