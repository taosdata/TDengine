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

#include "tsdb.h"

extern const char *TSDB_LEVEL_DNAME[];

typedef enum { TSDB_TXN_TEMP_FILE = 0, TSDB_TXN_CURR_FILE } TSDB_TXN_FILE_T;
static const char *tsdbTxnFname[] = {"current.t", "current"};
#define TSDB_MAX_FSETS(keep, days) ((keep) / (days) + 3)
#define TSDB_MAX_INIT_FSETS        (365000)

static int  tsdbComparFidFSet(const void *arg1, const void *arg2);
static void tsdbResetFSStatus(SFSStatus *pStatus);
static int  tsdbSaveFSStatus(STsdb *pRepo, SFSStatus *pStatus);
static void tsdbApplyFSTxnOnDisk(SFSStatus *pFrom, SFSStatus *pTo);
static void tsdbGetTxnFname(STsdb *pRepo, TSDB_TXN_FILE_T ftype, char fname[]);
static int  tsdbOpenFSFromCurrent(STsdb *pRepo);
static int  tsdbScanAndTryFixFS(STsdb *pRepo);
static int  tsdbScanRootDir(STsdb *pRepo);
static int  tsdbScanDataDir(STsdb *pRepo);
static bool tsdbIsTFileInFS(STsdbFS *pfs, const STfsFile *pf);
static int  tsdbRestoreCurrent(STsdb *pRepo);
static int  tsdbComparTFILE(const void *arg1, const void *arg2);
static void tsdbScanAndTryFixDFilesHeader(STsdb *pRepo, int32_t *nExpired);
// static int  tsdbProcessExpiredFS(STsdb *pRepo);
// static int  tsdbCreateMeta(STsdb *pRepo);

static void tsdbGetRootDir(int repoid, const char *dir, char dirName[]) {
  snprintf(dirName, TSDB_FILENAME_LEN, "vnode/vnode%d/%s", repoid, dir);
}

static void tsdbGetDataDir(int repoid, const char *dir, char dirName[]) {
  snprintf(dirName, TSDB_FILENAME_LEN, "vnode/vnode%d/%s/data", repoid, dir);
}

// For backward compatibility
// ================== CURRENT file header info
static int tsdbEncodeFSHeader(void **buf, SFSHeader *pHeader) {
  int tlen = 0;

  tlen += taosEncodeFixedU32(buf, pHeader->version);
  tlen += taosEncodeFixedU32(buf, pHeader->len);

  return tlen;
}

static void *tsdbDecodeFSHeader(void *buf, SFSHeader *pHeader) {
  buf = taosDecodeFixedU32(buf, &(pHeader->version));
  buf = taosDecodeFixedU32(buf, &(pHeader->len));

  return buf;
}

// ================== STsdbFSMeta
static int tsdbEncodeFSMeta(void **buf, STsdbFSMeta *pMeta) {
  int tlen = 0;

  tlen += taosEncodeFixedU32(buf, pMeta->version);
  tlen += taosEncodeFixedI64(buf, pMeta->totalPoints);
  tlen += taosEncodeFixedI64(buf, pMeta->totalStorage);

  return tlen;
}

static void *tsdbDecodeFSMeta(void *buf, STsdbFSMeta *pMeta) {
  buf = taosDecodeFixedU32(buf, &(pMeta->version));
  buf = taosDecodeFixedI64(buf, &(pMeta->totalPoints));
  buf = taosDecodeFixedI64(buf, &(pMeta->totalStorage));

  return buf;
}

// ================== SFSStatus
static int tsdbEncodeDFileSetArray(void **buf, SArray *pArray) {
  int      tlen = 0;
  uint64_t nset = taosArrayGetSize(pArray);

  tlen += taosEncodeFixedU64(buf, nset);
  for (size_t i = 0; i < nset; i++) {
    SDFileSet *pSet = taosArrayGet(pArray, i);

    tlen += tsdbEncodeDFileSet(buf, pSet);
  }

  return tlen;
}

static void *tsdbDecodeDFileSetArray(STsdb *pRepo, void *buf, SArray *pArray) {
  uint64_t nset = 0;

  taosArrayClear(pArray);

  buf = taosDecodeFixedU64(buf, &nset);
  for (size_t i = 0; i < nset; i++) {
    SDFileSet dset = {0};
    buf = tsdbDecodeDFileSet(pRepo, buf, &dset);
    taosArrayPush(pArray, (void *)(&dset));
  }
  return buf;
}

static int tsdbEncodeFSStatus(void **buf, SFSStatus *pStatus) {
  // ASSERT(pStatus->pmf);

  int tlen = 0;

  // tlen += tsdbEncodeSMFile(buf, pStatus->pmf);
  tlen += tsdbEncodeDFileSetArray(buf, pStatus->df);

  return tlen;
}

static void *tsdbDecodeFSStatus(STsdb *pRepo, void *buf, SFSStatus *pStatus) {
  tsdbResetFSStatus(pStatus);

  // pStatus->pmf = &(pStatus->mf);

  // buf = tsdbDecodeSMFile(buf, pStatus->pmf);
  buf = tsdbDecodeDFileSetArray(pRepo, buf, pStatus->df);

  return buf;
}

static SFSStatus *tsdbNewFSStatus(int maxFSet) {
  SFSStatus *pStatus = (SFSStatus *)taosMemoryCalloc(1, sizeof(*pStatus));
  if (pStatus == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  // TSDB_FILE_SET_CLOSED(&(pStatus->mf));

  pStatus->df = taosArrayInit(maxFSet, sizeof(SDFileSet));
  if (pStatus->df == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    taosMemoryFree(pStatus);
    return NULL;
  }

  return pStatus;
}

static SFSStatus *tsdbFreeFSStatus(SFSStatus *pStatus) {
  if (pStatus) {
    pStatus->df = taosArrayDestroy(pStatus->df);
    taosMemoryFree(pStatus);
  }

  return NULL;
}

static void tsdbResetFSStatus(SFSStatus *pStatus) {
  if (pStatus == NULL) {
    return;
  }

  // TSDB_FILE_SET_CLOSED(&(pStatus->mf));

  // pStatus->pmf = NULL;
  taosArrayClear(pStatus->df);
}

// static void tsdbSetStatusMFile(SFSStatus *pStatus, const SMFile *pMFile) {
//   ASSERT(pStatus->pmf == NULL);

//   pStatus->pmf = &(pStatus->mf);
//   tsdbInitMFileEx(pStatus->pmf, (SMFile *)pMFile);
// }

static int tsdbAddDFileSetToStatus(SFSStatus *pStatus, const SDFileSet *pSet) {
  if (taosArrayPush(pStatus->df, (void *)pSet) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  TSDB_FSET_SET_CLOSED(((SDFileSet *)taosArrayGetLast(pStatus->df)));

  return 0;
}

// ================== STsdbFS
STsdbFS *tsdbNewFS(const STsdbKeepCfg *pCfg) {
  int      keep = pCfg->keep2;
  int      days = pCfg->days;
  int      maxFSet = TSDB_MAX_FSETS(keep, days);
  STsdbFS *pfs;

  pfs = (STsdbFS *)taosMemoryCalloc(1, sizeof(*pfs));
  if (pfs == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  int code = taosThreadRwlockInit(&(pfs->lock), NULL);
  if (code) {
    terrno = TAOS_SYSTEM_ERROR(code);
    taosMemoryFree(pfs);
    return NULL;
  }

  if (maxFSet > TSDB_MAX_INIT_FSETS) {
    maxFSet = TSDB_MAX_INIT_FSETS;
  }

  pfs->cstatus = tsdbNewFSStatus(maxFSet);
  if (pfs->cstatus == NULL) {
    tsdbFreeFS(pfs);
    return NULL;
  }

  pfs->intxn = false;
  pfs->nstatus = tsdbNewFSStatus(maxFSet);
  if (pfs->nstatus == NULL) {
    tsdbFreeFS(pfs);
    return NULL;
  }

  return pfs;
}

void *tsdbFreeFS(STsdbFS *pfs) {
  if (pfs) {
    pfs->nstatus = tsdbFreeFSStatus(pfs->nstatus);
    pfs->cstatus = tsdbFreeFSStatus(pfs->cstatus);
    taosThreadRwlockDestroy(&(pfs->lock));
    taosMemoryFree(pfs);
  }

  return NULL;
}

int tsdbOpenFS(STsdb *pRepo) {
  STsdbFS *pfs = REPO_FS(pRepo);
  char     current[TSDB_FILENAME_LEN] = "\0";
  int      nExpired = 0;

  ASSERT(pfs != NULL);

  tsdbGetTxnFname(pRepo, TSDB_TXN_CURR_FILE, current);

  tsdbGetRtnSnap(pRepo, &pRepo->rtn);
  if (taosCheckExistFile(current)) {
    if (tsdbOpenFSFromCurrent(pRepo) < 0) {
      tsdbError("vgId:%d, failed to open FS since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }

    tsdbScanAndTryFixDFilesHeader(pRepo, &nExpired);
    // if (nExpired > 0) {
    //   tsdbProcessExpiredFS(pRepo);
    // }
  } else {
    // should skip expired fileset inside of the function
    if (tsdbRestoreCurrent(pRepo) < 0) {
      tsdbError("vgId:%d, failed to restore current file since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }
  }

  if (tsdbScanAndTryFixFS(pRepo) < 0) {
    tsdbError("vgId:%d, failed to scan and fix FS since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  // // Load meta cache if has meta file
  // if ((!(pRepo->state & TSDB_STATE_BAD_META)) && tsdbLoadMetaCache(pRepo, true) < 0) {
  //   tsdbError("vgId:%d, failed to open FS while loading meta cache since %s", REPO_ID(pRepo), tstrerror(terrno));
  //   return -1;
  // }

  return 0;
}

void tsdbCloseFS(STsdb *pRepo) {
  // Do nothing
}

// Start a new transaction to modify the file system
void tsdbStartFSTxn(STsdb *pRepo, int64_t pointsAdd, int64_t storageAdd) {
  STsdbFS *pfs = REPO_FS(pRepo);
  ASSERT(pfs->intxn == false);

  pfs->intxn = true;
  tsdbResetFSStatus(pfs->nstatus);
  pfs->nstatus->meta = pfs->cstatus->meta;
  // if (pfs->cstatus->pmf == NULL) {
  pfs->nstatus->meta.version += 1;
  // } else {
  //   pfs->nstatus->meta.version = pfs->cstatus->meta.version + 1;
  // }
  pfs->nstatus->meta.totalPoints = pfs->cstatus->meta.totalPoints + pointsAdd;
  pfs->nstatus->meta.totalStorage = pfs->cstatus->meta.totalStorage += storageAdd;
}

void tsdbUpdateFSTxnMeta(STsdbFS *pfs, STsdbFSMeta *pMeta) { pfs->nstatus->meta = *pMeta; }

int tsdbEndFSTxn(STsdb *pRepo) {
  STsdbFS *pfs = REPO_FS(pRepo);
  ASSERT(FS_IN_TXN(pfs));
  SFSStatus *pStatus;

  // Write current file system snapshot
  if (tsdbSaveFSStatus(pRepo, pfs->nstatus) < 0) {
    tsdbEndFSTxnWithError(pfs);
    return -1;
  }

  // Make new
  tsdbWLockFS(pfs);
  pStatus = pfs->cstatus;
  pfs->cstatus = pfs->nstatus;
  pfs->nstatus = pStatus;
  tsdbUnLockFS(pfs);

  // Apply actual change to each file and SDFileSet
  tsdbApplyFSTxnOnDisk(pfs->nstatus, pfs->cstatus);

  pfs->intxn = false;
  return 0;
}

int tsdbEndFSTxnWithError(STsdbFS *pfs) {
  tsdbApplyFSTxnOnDisk(pfs->nstatus, pfs->cstatus);
  // TODO: if mf change, reload pfs->metaCache
  pfs->intxn = false;
  return 0;
}

// void tsdbUpdateMFile(STsdbFS *pfs, const SMFile *pMFile) { tsdbSetStatusMFile(pfs->nstatus, pMFile); }

int tsdbUpdateDFileSet(STsdbFS *pfs, const SDFileSet *pSet) { return tsdbAddDFileSetToStatus(pfs->nstatus, pSet); }

static int tsdbSaveFSStatus(STsdb *pRepo, SFSStatus *pStatus) {
  SFSHeader fsheader;
  void     *pBuf = NULL;
  void     *ptr;
  char      hbuf[TSDB_FILE_HEAD_SIZE] = "\0";
  char      tfname[TSDB_FILENAME_LEN] = "\0";
  char      cfname[TSDB_FILENAME_LEN] = "\0";

  tsdbGetTxnFname(pRepo, TSDB_TXN_TEMP_FILE, tfname);
  tsdbGetTxnFname(pRepo, TSDB_TXN_CURR_FILE, cfname);

  TdFilePtr pFile = taosOpenFile(tfname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  fsheader.version = TSDB_LATEST_SFS_VER;
  if (taosArrayGetSize(pStatus->df) == 0) {
    fsheader.len = 0;
  } else {
    fsheader.len = tsdbEncodeFSStatus(NULL, pStatus) + sizeof(TSCKSUM);
  }

  // Encode header part and write
  ptr = hbuf;
  tsdbEncodeFSHeader(&ptr, &fsheader);
  tsdbEncodeFSMeta(&ptr, &(pStatus->meta));

  taosCalcChecksumAppend(0, (uint8_t *)hbuf, TSDB_FILE_HEAD_SIZE);

  if (taosWriteFile(pFile, hbuf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosCloseFile(&pFile);
    taosRemoveFile(tfname);
    return -1;
  }

  // Encode file status and write to file
  if (fsheader.len > 0) {
    if (tsdbMakeRoom(&(pBuf), fsheader.len) < 0) {
      taosCloseFile(&pFile);
      taosRemoveFile(tfname);
      return -1;
    }

    ptr = pBuf;
    tsdbEncodeFSStatus(&ptr, pStatus);
    taosCalcChecksumAppend(0, (uint8_t *)pBuf, fsheader.len);

    if (taosWriteFile(pFile, pBuf, fsheader.len) < fsheader.len) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      taosCloseFile(&pFile);
      (void)taosRemoveFile(tfname);
      taosTZfree(pBuf);
      return -1;
    }
  }

  // fsync, close and rename
  if (taosFsyncFile(pFile) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosCloseFile(&pFile);
    taosRemoveFile(tfname);
    taosTZfree(pBuf);
    return -1;
  }

  (void)taosCloseFile(&pFile);
  (void)taosRenameFile(tfname, cfname);
  taosTZfree(pBuf);

  return 0;
}

static void tsdbApplyFSTxnOnDisk(SFSStatus *pFrom, SFSStatus *pTo) {
  int        ifrom = 0;
  int        ito = 0;
  size_t     sizeFrom, sizeTo;
  SDFileSet *pSetFrom;
  SDFileSet *pSetTo;

  sizeFrom = taosArrayGetSize(pFrom->df);
  sizeTo = taosArrayGetSize(pTo->df);

  // Apply meta file change
  // (void)tsdbApplyMFileChange(pFrom->pmf, pTo->pmf);

  // Apply SDFileSet change
  if (ifrom >= sizeFrom) {
    pSetFrom = NULL;
  } else {
    pSetFrom = taosArrayGet(pFrom->df, ifrom);
  }

  if (ito >= sizeTo) {
    pSetTo = NULL;
  } else {
    pSetTo = taosArrayGet(pTo->df, ito);
  }

  while (true) {
    if ((pSetTo == NULL) && (pSetFrom == NULL)) break;

    if (pSetTo == NULL || (pSetFrom && pSetFrom->fid < pSetTo->fid)) {
      tsdbApplyDFileSetChange(pSetFrom, NULL);

      ifrom++;
      if (ifrom >= sizeFrom) {
        pSetFrom = NULL;
      } else {
        pSetFrom = taosArrayGet(pFrom->df, ifrom);
      }
    } else if (pSetFrom == NULL || pSetFrom->fid > pSetTo->fid) {
      // Do nothing
      ito++;
      if (ito >= sizeTo) {
        pSetTo = NULL;
      } else {
        pSetTo = taosArrayGet(pTo->df, ito);
      }
    } else {
      tsdbApplyDFileSetChange(pSetFrom, pSetTo);

      ifrom++;
      if (ifrom >= sizeFrom) {
        pSetFrom = NULL;
      } else {
        pSetFrom = taosArrayGet(pFrom->df, ifrom);
      }

      ito++;
      if (ito >= sizeTo) {
        pSetTo = NULL;
      } else {
        pSetTo = taosArrayGet(pTo->df, ito);
      }
    }
  }
}

// ================== SFSIter
// ASSUMPTIONS: the FS Should be read locked when calling these functions
void tsdbFSIterInit(SFSIter *pIter, STsdbFS *pfs, int direction) {
  pIter->pfs = pfs;
  pIter->direction = direction;

  size_t size = taosArrayGetSize(pfs->cstatus->df);

  pIter->version = pfs->cstatus->meta.version;

  if (size == 0) {
    pIter->index = -1;
    pIter->fid = TSDB_IVLD_FID;
  } else {
    if (direction == TSDB_FS_ITER_FORWARD) {
      pIter->index = 0;
    } else {
      pIter->index = (int)(size - 1);
    }

    pIter->fid = ((SDFileSet *)taosArrayGet(pfs->cstatus->df, pIter->index))->fid;
  }
}

void tsdbFSIterSeek(SFSIter *pIter, int fid) {
  STsdbFS *pfs = pIter->pfs;
  size_t   size = taosArrayGetSize(pfs->cstatus->df);

  int flags;
  if (pIter->direction == TSDB_FS_ITER_FORWARD) {
    flags = TD_GE;
  } else {
    flags = TD_LE;
  }

  void *ptr = taosbsearch(&fid, pfs->cstatus->df->pData, size, sizeof(SDFileSet), tsdbComparFidFSet, flags);
  if (ptr == NULL) {
    pIter->index = -1;
    pIter->fid = TSDB_IVLD_FID;
  } else {
    pIter->index = (int)(TARRAY_ELEM_IDX(pfs->cstatus->df, ptr));
    pIter->fid = ((SDFileSet *)ptr)->fid;
  }
}

SDFileSet *tsdbFSIterNext(SFSIter *pIter) {
  STsdbFS   *pfs = pIter->pfs;
  SDFileSet *pSet;

  if (pIter->index < 0) {
    ASSERT(pIter->fid == TSDB_IVLD_FID);
    return NULL;
  }

  ASSERT(pIter->fid != TSDB_IVLD_FID);

  if (pIter->version != pfs->cstatus->meta.version) {
    pIter->version = pfs->cstatus->meta.version;
    tsdbFSIterSeek(pIter, pIter->fid);
  }

  if (pIter->index < 0) {
    return NULL;
  }

  pSet = (SDFileSet *)taosArrayGet(pfs->cstatus->df, pIter->index);
  ASSERT(pSet->fid == pIter->fid);

  if (pIter->direction == TSDB_FS_ITER_FORWARD) {
    pIter->index++;
    if (pIter->index >= taosArrayGetSize(pfs->cstatus->df)) {
      pIter->index = -1;
    }
  } else {
    pIter->index--;
  }

  if (pIter->index >= 0) {
    pIter->fid = ((SDFileSet *)taosArrayGet(pfs->cstatus->df, pIter->index))->fid;
  } else {
    pIter->fid = TSDB_IVLD_FID;
  }

  return pSet;
}

static int tsdbComparFidFSet(const void *arg1, const void *arg2) {
  int        fid = *(int *)arg1;
  SDFileSet *pSet = (SDFileSet *)arg2;

  if (fid < pSet->fid) {
    return -1;
  } else if (fid == pSet->fid) {
    return 0;
  } else {
    return 1;
  }
}

static void tsdbGetTxnFname(STsdb *pRepo, TSDB_TXN_FILE_T ftype, char fname[]) {
  snprintf(fname, TSDB_FILENAME_LEN, "%s/vnode/vnode%d/%s/%s", tfsGetPrimaryPath(REPO_TFS(pRepo)), REPO_ID(pRepo),
           pRepo->dir, tsdbTxnFname[ftype]);
}

static int tsdbOpenFSFromCurrent(STsdb *pRepo) {
  STsdbFS  *pfs = REPO_FS(pRepo);
  TdFilePtr pFile = NULL;
  void     *buffer = NULL;
  SFSHeader fsheader;
  char      current[TSDB_FILENAME_LEN] = "\0";
  void     *ptr;

  tsdbGetTxnFname(pRepo, TSDB_TXN_CURR_FILE, current);

  // current file exists, try to recover
  pFile = taosOpenFile(current, TD_FILE_READ);
  if (pFile == NULL) {
    tsdbError("vgId:%d, failed to open file %s since %s", REPO_ID(pRepo), current, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (tsdbMakeRoom(&buffer, TSDB_FILE_HEAD_SIZE) < 0) {
    goto _err;
  }

  int nread = (int)taosReadFile(pFile, buffer, TSDB_FILE_HEAD_SIZE);
  if (nread < 0) {
    tsdbError("vgId:%d, failed to read %d bytes from file %s since %s", REPO_ID(pRepo), TSDB_FILENAME_LEN, current,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (nread < TSDB_FILE_HEAD_SIZE) {
    tsdbError("vgId:%d, failed to read header of file %s, read bytes:%d", REPO_ID(pRepo), current, nread);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    goto _err;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buffer, TSDB_FILE_HEAD_SIZE)) {
    tsdbError("vgId:%d, header of file %s failed checksum check", REPO_ID(pRepo), current);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    goto _err;
  }

  SFSStatus *pStatus = pfs->cstatus;
  ptr = buffer;
  ptr = tsdbDecodeFSHeader(ptr, &fsheader);
  ptr = tsdbDecodeFSMeta(ptr, &(pStatus->meta));

  if (fsheader.version != TSDB_LATEST_SFS_VER) {
    // TODO: handle file version change
  }

  if (fsheader.len > 0) {
    if (tsdbMakeRoom(&buffer, fsheader.len) < 0) {
      goto _err;
    }

    nread = (int)taosReadFile(pFile, buffer, fsheader.len);
    if (nread < 0) {
      tsdbError("vgId:%d, failed to read file %s since %s", REPO_ID(pRepo), current, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (nread < fsheader.len) {
      tsdbError("vgId:%d, failed to read %d bytes from file %s", REPO_ID(pRepo), fsheader.len, current);
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
      goto _err;
    }

    if (!taosCheckChecksumWhole((uint8_t *)buffer, fsheader.len)) {
      tsdbError("vgId:%d, file %s is corrupted since wrong checksum", REPO_ID(pRepo), current);
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
      goto _err;
    }

    ptr = buffer;
    ptr = tsdbDecodeFSStatus(pRepo, ptr, pStatus);
  } else {
    tsdbResetFSStatus(pStatus);
  }

  taosTZfree(buffer);
  taosCloseFile(&pFile);

  return 0;

_err:
  if (pFile != NULL) {
    taosCloseFile(&pFile);
  }
  taosTZfree(buffer);
  return -1;
}

// Scan and try to fix incorrect files
static int tsdbScanAndTryFixFS(STsdb *pRepo) {
  STsdbFS   *pfs = REPO_FS(pRepo);
  SFSStatus *pStatus = pfs->cstatus;

  // if (tsdbScanAndTryFixMFile(pRepo) < 0) {
  //   tsdbError("vgId:%d, failed to fix MFile since %s", REPO_ID(pRepo), tstrerror(terrno));
  //   return -1;
  // }

  size_t size = taosArrayGetSize(pStatus->df);

  for (size_t i = 0; i < size; i++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pStatus->df, i);

    if (tsdbScanAndTryFixDFileSet(pRepo, pSet) < 0) {
      tsdbError("vgId:%d, failed to fix MFile since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }
  }

  // remove those unused files
  tsdbScanRootDir(pRepo);
  tsdbScanDataDir(pRepo);
  return 0;
}

static int tsdbScanRootDir(STsdb *pRepo) {
  char            rootDir[TSDB_FILENAME_LEN];
  char            bname[TSDB_FILENAME_LEN];
  STsdbFS        *pfs = REPO_FS(pRepo);
  const STfsFile *pf;

  tsdbGetRootDir(REPO_ID(pRepo), pRepo->dir, rootDir);
  STfsDir *tdir = tfsOpendir(REPO_TFS(pRepo), rootDir);
  if (tdir == NULL) {
    tsdbError("vgId:%d, failed to open directory %s since %s", REPO_ID(pRepo), rootDir, tstrerror(terrno));
    return -1;
  }

  while ((pf = tfsReaddir(tdir))) {
    tfsBasename(pf, bname);

    if (strcmp(bname, tsdbTxnFname[TSDB_TXN_CURR_FILE]) == 0 || strcmp(bname, "data") == 0) {
      // Skip current file and data directory
      continue;
    }

    // if (/*pfs->cstatus->pmf && */ tfsIsSameFile(pf, &(pfs->cstatus->pmf->f))) {
    //   continue;
    // }

    (void)tfsRemoveFile(pf);
    tsdbDebug("vgId:%d, invalid file %s is removed", REPO_ID(pRepo), pf->aname);
  }

  tfsClosedir(tdir);

  return 0;
}

static int tsdbScanDataDir(STsdb *pRepo) {
  char            dataDir[TSDB_FILENAME_LEN];
  char            bname[TSDB_FILENAME_LEN];
  STsdbFS        *pfs = REPO_FS(pRepo);
  const STfsFile *pf;

  tsdbGetDataDir(REPO_ID(pRepo), pRepo->dir, dataDir);
  STfsDir *tdir = tfsOpendir(REPO_TFS(pRepo), dataDir);
  if (tdir == NULL) {
    tsdbError("vgId:%d, failed to open directory %s since %s", REPO_ID(pRepo), dataDir, tstrerror(terrno));
    return -1;
  }

  while ((pf = tfsReaddir(tdir))) {
    tfsBasename(pf, bname);

    if (!tsdbIsTFileInFS(pfs, pf)) {
      (void)tfsRemoveFile(pf);
      tsdbDebug("vgId:%d, invalid file %s is removed", REPO_ID(pRepo), pf->aname);
    }
  }

  tfsClosedir(tdir);

  return 0;
}

static bool tsdbIsTFileInFS(STsdbFS *pfs, const STfsFile *pf) {
  SFSIter fsiter;
  tsdbFSIterInit(&fsiter, pfs, TSDB_FS_ITER_FORWARD);
  SDFileSet *pSet;

  while ((pSet = tsdbFSIterNext(&fsiter))) {
    for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
      SDFile *pDFile = TSDB_DFILE_IN_SET(pSet, ftype);
      if (tfsIsSameFile(pf, TSDB_FILE_F(pDFile))) {
        return true;
      }
    }
  }

  return false;
}

static int tsdbRestoreDFileSet(STsdb *pRepo) {
  char            dataDir[TSDB_FILENAME_LEN];
  char            bname[TSDB_FILENAME_LEN];
  STfsDir        *tdir = NULL;
  const STfsFile *pf = NULL;
  const char     *pattern = "^v[0-9]+f[0-9]+\\.(head|data|last|smad|smal)(-ver[0-9]+)?$";
  SArray         *fArray = NULL;
  regex_t         regex;
  STsdbFS        *pfs = REPO_FS(pRepo);

  tsdbGetDataDir(REPO_ID(pRepo), pRepo->dir, dataDir);

  // Resource allocation and init
  regcomp(&regex, pattern, REG_EXTENDED);

  fArray = taosArrayInit(1024, sizeof(STfsFile));
  if (fArray == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbError("vgId:%d, failed to restore DFileSet while open directory %s since %s", REPO_ID(pRepo), dataDir,
              tstrerror(terrno));
    regfree(&regex);
    return -1;
  }

  tdir = tfsOpendir(REPO_TFS(pRepo), dataDir);
  if (tdir == NULL) {
    tsdbError("vgId:%d, failed to restore DFileSet while open directory %s since %s", REPO_ID(pRepo), dataDir,
              tstrerror(terrno));
    taosArrayDestroy(fArray);
    regfree(&regex);
    return -1;
  }

  while ((pf = tfsReaddir(tdir))) {
    tfsBasename(pf, bname);

    int code = regexec(&regex, bname, 0, NULL, 0);
    if (code == 0) {
      if (taosArrayPush(fArray, (void *)pf) == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        tfsClosedir(tdir);
        taosArrayDestroy(fArray);
        regfree(&regex);
        return -1;
      }
    } else if (code == REG_NOMATCH) {
      // Not match
      tsdbInfo("vgId:%d, invalid file %s exists, remove it", REPO_ID(pRepo), pf->aname);
      (void)tfsRemoveFile(pf);
      continue;
    } else {
      // Has other error
      tsdbError("vgId:%d, failed to restore DFileSet Array while run regexec since %s", REPO_ID(pRepo), strerror(code));
      terrno = TAOS_SYSTEM_ERROR(code);
      tfsClosedir(tdir);
      taosArrayDestroy(fArray);
      regfree(&regex);
      return -1;
    }
  }

  tfsClosedir(tdir);
  regfree(&regex);

  // Sort the array according to file name
  taosArraySort(fArray, tsdbComparTFILE);

  size_t index = 0;
  // Loop to recover each file set
  for (;;) {
    if (index >= taosArrayGetSize(fArray)) {
      break;
    }

    SDFileSet fset = {0};

    TSDB_FSET_SET_CLOSED(&fset);

    // Loop to recover ONE fset
    for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
      SDFile *pDFile = TSDB_DFILE_IN_SET(&fset, ftype);

      if (index >= taosArrayGetSize(fArray)) {
        tsdbError("vgId:%d, incomplete DFileSet, fid:%d", REPO_ID(pRepo), fset.fid);
        taosArrayDestroy(fArray);
        return -1;
      }

      pf = taosArrayGet(fArray, index);

      int         tvid, tfid;
      TSDB_FILE_T ttype;
      uint32_t    tversion;
      char        _bname[TSDB_FILENAME_LEN];

      tfsBasename(pf, _bname);
      tsdbParseDFilename(_bname, &tvid, &tfid, &ttype, &tversion);

      ASSERT(tvid == REPO_ID(pRepo));

      if (tfid < pRepo->rtn.minFid) {  // skip file expired
        ++index;
        continue;
      }

      if (ftype == 0) {
        fset.fid = tfid;
      } else {
        if (tfid != fset.fid) {
          tsdbError("vgId:%d, incomplete dFileSet, fid:%d", REPO_ID(pRepo), fset.fid);
          taosArrayDestroy(fArray);
          return -1;
        }
      }

      if (ttype != ftype) {
        tsdbError("vgId:%d, incomplete dFileSet, fid:%d", REPO_ID(pRepo), fset.fid);
        taosArrayDestroy(fArray);
        return -1;
      }

      pDFile->f = *pf;

      // if (tsdbOpenDFile(pDFile, O_RDONLY) < 0) {
      if (tsdbOpenDFile(pDFile, TD_FILE_READ) < 0) {
        tsdbError("vgId:%d, failed to open DFile %s since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile),
                  tstrerror(terrno));
        taosArrayDestroy(fArray);
        return -1;
      }

      if (tsdbLoadDFileHeader(pDFile, &(pDFile->info)) < 0) {
        tsdbError("vgId:%d, failed to load DFile %s header since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile),
                  tstrerror(terrno));
        taosArrayDestroy(fArray);
        return -1;
      }

      if (tsdbForceKeepFile) {
        int64_t file_size;
        // Get real file size
        if (taosFStatFile(pDFile->pFile, &file_size, NULL) < 0) {
          terrno = TAOS_SYSTEM_ERROR(errno);
          taosArrayDestroy(fArray);
          return -1;
        }

        if (pDFile->info.size != file_size) {
          int64_t tfsize = pDFile->info.size;
          pDFile->info.size = file_size;
          tsdbInfo("vgId:%d, file %s header size is changed from %" PRId64 " to %" PRId64, REPO_ID(pRepo),
                   TSDB_FILE_FULL_NAME(pDFile), tfsize, pDFile->info.size);
        }
      }

      tsdbCloseDFile(pDFile);
      index++;
    }

    tsdbInfo("vgId:%d, FSET %d is restored", REPO_ID(pRepo), fset.fid);
    taosArrayPush(pfs->cstatus->df, &fset);
  }

  // Resource release
  taosArrayDestroy(fArray);

  return 0;
}

static int tsdbRestoreCurrent(STsdb *pRepo) {
  if (tsdbRestoreDFileSet(pRepo) < 0) {
    tsdbError("vgId:%d, failed to restore DFileSet since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  if (tsdbSaveFSStatus(pRepo, pRepo->fs->cstatus) < 0) {
    tsdbError("vgId:%d, failed to restore current since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  return 0;
}

static int tsdbComparTFILE(const void *arg1, const void *arg2) {
  STfsFile *pf1 = (STfsFile *)arg1;
  STfsFile *pf2 = (STfsFile *)arg2;

  int         vid1, fid1, vid2, fid2;
  TSDB_FILE_T ftype1, ftype2;
  uint32_t    version1, version2;
  char        bname1[TSDB_FILENAME_LEN];
  char        bname2[TSDB_FILENAME_LEN];

  tfsBasename(pf1, bname1);
  tfsBasename(pf2, bname2);
  tsdbParseDFilename(bname1, &vid1, &fid1, &ftype1, &version1);
  tsdbParseDFilename(bname2, &vid2, &fid2, &ftype2, &version2);

  if (fid1 < fid2) {
    return -1;
  } else if (fid1 > fid2) {
    return 1;
  } else {
    if (ftype1 < ftype2) {
      return -1;
    } else if (ftype1 > ftype2) {
      return 1;
    } else {
      return 0;
    }
  }
}

static void tsdbScanAndTryFixDFilesHeader(STsdb *pRepo, int32_t *nExpired) {
  STsdbFS   *pfs = REPO_FS(pRepo);
  SFSStatus *pStatus = pfs->cstatus;
  SDFInfo    info;

  for (size_t i = 0; i < taosArrayGetSize(pStatus->df); i++) {
    SDFileSet fset;
    tsdbInitDFileSetEx(&fset, (SDFileSet *)taosArrayGet(pStatus->df, i));
    if (fset.fid < pRepo->rtn.minFid) {
      ++*nExpired;
    }
    tsdbDebug("vgId:%d, scan DFileSet %d header", REPO_ID(pRepo), fset.fid);

    // if (tsdbOpenDFileSet(&fset, O_RDWR) < 0) {
    if (tsdbOpenDFileSet(&fset, TD_FILE_WRITE | TD_FILE_READ) < 0) {
      tsdbError("vgId:%d, failed to open DFileSet %d since %s, continue", REPO_ID(pRepo), fset.fid, tstrerror(terrno));
      continue;
    }

    for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
      SDFile *pDFile = TSDB_DFILE_IN_SET(&fset, ftype);

      if ((tsdbLoadDFileHeader(pDFile, &info) < 0) || pDFile->info.size != info.size ||
          pDFile->info.magic != info.magic) {
        if (tsdbUpdateDFileHeader(pDFile) < 0) {
          tsdbError("vgId:%d, failed to update DFile header of %s since %s, continue", REPO_ID(pRepo),
                    TSDB_FILE_FULL_NAME(pDFile), tstrerror(terrno));
        } else {
          tsdbInfo("vgId:%d, DFile header of %s is updated", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile));
          TSDB_FILE_FSYNC(pDFile);
        }
      } else {
        tsdbDebug("vgId:%d, DFile header of %s is correct", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile));
      }
    }

    tsdbCloseDFileSet(&fset);
  }
}

int tsdbRLockFS(STsdbFS *pFs) {
  int code = taosThreadRwlockRdlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

int tsdbWLockFS(STsdbFS *pFs) {
  int code = taosThreadRwlockWrlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

int tsdbUnLockFS(STsdbFS *pFs) {
  int code = taosThreadRwlockUnlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}