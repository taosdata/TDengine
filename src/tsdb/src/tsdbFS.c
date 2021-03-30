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

#include "os.h"
#include "tsdbint.h"
#include <regex.h>

typedef enum { TSDB_TXN_TEMP_FILE = 0, TSDB_TXN_CURR_FILE } TSDB_TXN_FILE_T;
static const char *tsdbTxnFname[] = {"current.t", "current"};
#define TSDB_MAX_FSETS(keep, days) ((keep) / (days) + 3)

static int  tsdbComparFidFSet(const void *arg1, const void *arg2);
static void tsdbResetFSStatus(SFSStatus *pStatus);
static int  tsdbSaveFSStatus(SFSStatus *pStatus, int vid);
static void tsdbApplyFSTxnOnDisk(SFSStatus *pFrom, SFSStatus *pTo);
static void tsdbGetTxnFname(int repoid, TSDB_TXN_FILE_T ftype, char fname[]);
static int  tsdbOpenFSFromCurrent(STsdbRepo *pRepo);
static int  tsdbScanAndTryFixFS(STsdbRepo *pRepo);
static int  tsdbScanRootDir(STsdbRepo *pRepo);
static int  tsdbScanDataDir(STsdbRepo *pRepo);
static bool tsdbIsTFileInFS(STsdbFS *pfs, const TFILE *pf);
static int  tsdbRestoreCurrent(STsdbRepo *pRepo);
static int  tsdbComparTFILE(const void *arg1, const void *arg2);
static void tsdbScanAndTryFixDFilesHeader(STsdbRepo *pRepo);

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

static void *tsdbDecodeDFileSetArray(void *buf, SArray *pArray) {
  uint64_t  nset;
  SDFileSet dset;

  taosArrayClear(pArray);

  buf = taosDecodeFixedU64(buf, &nset);
  for (size_t i = 0; i < nset; i++) {
    buf = tsdbDecodeDFileSet(buf, &dset);
    taosArrayPush(pArray, (void *)(&dset));
  }
  return buf;
}

static int tsdbEncodeFSStatus(void **buf, SFSStatus *pStatus) {
  ASSERT(pStatus->pmf);

  int tlen = 0;

  tlen += tsdbEncodeSMFile(buf, pStatus->pmf);
  tlen += tsdbEncodeDFileSetArray(buf, pStatus->df);

  return tlen;
}

static void *tsdbDecodeFSStatus(void *buf, SFSStatus *pStatus) {
  tsdbResetFSStatus(pStatus);

  pStatus->pmf = &(pStatus->mf);

  buf = tsdbDecodeSMFile(buf, pStatus->pmf);
  buf = tsdbDecodeDFileSetArray(buf, pStatus->df);

  return buf;
}

static SFSStatus *tsdbNewFSStatus(int maxFSet) {
  SFSStatus *pStatus = (SFSStatus *)calloc(1, sizeof(*pStatus));
  if (pStatus == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  TSDB_FILE_SET_CLOSED(&(pStatus->mf));

  pStatus->df = taosArrayInit(maxFSet, sizeof(SDFileSet));
  if (pStatus->df == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    free(pStatus);
    return NULL;
  }

  return pStatus;
}

static SFSStatus *tsdbFreeFSStatus(SFSStatus *pStatus) {
  if (pStatus) {
    pStatus->df = taosArrayDestroy(pStatus->df);
    free(pStatus);
  }

  return NULL;
}

static void tsdbResetFSStatus(SFSStatus *pStatus) {
  if (pStatus == NULL) {
    return;
  }

  TSDB_FILE_SET_CLOSED(&(pStatus->mf));

  pStatus->pmf = NULL;
  taosArrayClear(pStatus->df);
}

static void tsdbSetStatusMFile(SFSStatus *pStatus, const SMFile *pMFile) {
  ASSERT(pStatus->pmf == NULL);

  pStatus->pmf = &(pStatus->mf);
  tsdbInitMFileEx(pStatus->pmf, (SMFile *)pMFile);
}

static int tsdbAddDFileSetToStatus(SFSStatus *pStatus, const SDFileSet *pSet) {
  if (taosArrayPush(pStatus->df, (void *)pSet) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  TSDB_FSET_SET_CLOSED(((SDFileSet *)taosArrayGetLast(pStatus->df)));

  return 0;
}

// ================== STsdbFS
STsdbFS *tsdbNewFS(STsdbCfg *pCfg) {
  int      keep = pCfg->keep;
  int      days = pCfg->daysPerFile;
  int      maxFSet = TSDB_MAX_FSETS(keep, days);
  STsdbFS *pfs;

  pfs = (STsdbFS *)calloc(1, sizeof(*pfs));
  if (pfs == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  int code = pthread_rwlock_init(&(pfs->lock), NULL);
  if (code) {
    terrno = TAOS_SYSTEM_ERROR(code);
    free(pfs);
    return NULL;
  }

  pfs->cstatus = tsdbNewFSStatus(maxFSet);
  if (pfs->cstatus == NULL) {
    tsdbFreeFS(pfs);
    return NULL;
  }

  pfs->metaCache = taosHashInit(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (pfs->metaCache == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbFreeFS(pfs);
    return NULL;
  }

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
    taosHashCleanup(pfs->metaCache);
    pfs->metaCache = NULL;
    pfs->cstatus = tsdbFreeFSStatus(pfs->cstatus);
    pthread_rwlock_destroy(&(pfs->lock));
    free(pfs);
  }

  return NULL;
}

int tsdbOpenFS(STsdbRepo *pRepo) {
  STsdbFS *pfs = REPO_FS(pRepo);
  char     current[TSDB_FILENAME_LEN] = "\0";

  ASSERT(pfs != NULL);

  tsdbGetTxnFname(REPO_ID(pRepo), TSDB_TXN_CURR_FILE, current);

  if (access(current, F_OK) == 0) {
    if (tsdbOpenFSFromCurrent(pRepo) < 0) {
      tsdbError("vgId:%d failed to open FS since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }

    tsdbScanAndTryFixDFilesHeader(pRepo);
  } else {
    if (tsdbRestoreCurrent(pRepo) < 0) {
      tsdbError("vgId:%d failed to restore current file since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }
  }

  if (tsdbScanAndTryFixFS(pRepo) < 0) {
    tsdbError("vgId:%d failed to scan and fix FS since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  // Load meta cache if has meta file
  if ((!(pRepo->state & TSDB_STATE_BAD_META)) && tsdbLoadMetaCache(pRepo, true) < 0) {
    tsdbError("vgId:%d failed to open FS while loading meta cache since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  return 0;
}

void tsdbCloseFS(STsdbRepo *pRepo) {
  // Do nothing
}

// Start a new transaction to modify the file system
void tsdbStartFSTxn(STsdbRepo *pRepo, int64_t pointsAdd, int64_t storageAdd) {
  STsdbFS *pfs = REPO_FS(pRepo);
  ASSERT(pfs->intxn == false);

  pfs->intxn = true;
  tsdbResetFSStatus(pfs->nstatus);
  pfs->nstatus->meta = pfs->cstatus->meta;
  if (pfs->cstatus->pmf == NULL) {
    pfs->nstatus->meta.version = 0;
  } else {
    pfs->nstatus->meta.version = pfs->cstatus->meta.version + 1;
  }
  pfs->nstatus->meta.totalPoints = pfs->cstatus->meta.totalPoints + pointsAdd;
  pfs->nstatus->meta.totalStorage = pfs->cstatus->meta.totalStorage += storageAdd;
}

void tsdbUpdateFSTxnMeta(STsdbFS *pfs, STsdbFSMeta *pMeta) { pfs->nstatus->meta = *pMeta; }

int tsdbEndFSTxn(STsdbRepo *pRepo) {
  STsdbFS *pfs = REPO_FS(pRepo);
  ASSERT(FS_IN_TXN(pfs));
  SFSStatus *pStatus;

  // Write current file system snapshot
  if (tsdbSaveFSStatus(pfs->nstatus, REPO_ID(pRepo)) < 0) {
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

void tsdbUpdateMFile(STsdbFS *pfs, const SMFile *pMFile) { tsdbSetStatusMFile(pfs->nstatus, pMFile); }

int tsdbUpdateDFileSet(STsdbFS *pfs, const SDFileSet *pSet) { return tsdbAddDFileSetToStatus(pfs->nstatus, pSet); }

static int tsdbSaveFSStatus(SFSStatus *pStatus, int vid) {
  SFSHeader fsheader;
  void *    pBuf = NULL;
  void *    ptr;
  char      hbuf[TSDB_FILE_HEAD_SIZE] = "\0";
  char      tfname[TSDB_FILENAME_LEN] = "\0";
  char      cfname[TSDB_FILENAME_LEN] = "\0";

  tsdbGetTxnFname(vid, TSDB_TXN_TEMP_FILE, tfname);
  tsdbGetTxnFname(vid, TSDB_TXN_CURR_FILE, cfname);

  int fd = open(tfname, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, 0755);
  if (fd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  fsheader.version = TSDB_FS_VERSION;
  if (pStatus->pmf == NULL) {
    ASSERT(taosArrayGetSize(pStatus->df) == 0);
    fsheader.len = 0;
  } else {
    fsheader.len = tsdbEncodeFSStatus(NULL, pStatus) + sizeof(TSCKSUM);
  }

  // Encode header part and write
  ptr = hbuf;
  tsdbEncodeFSHeader(&ptr, &fsheader);
  tsdbEncodeFSMeta(&ptr, &(pStatus->meta));

  taosCalcChecksumAppend(0, (uint8_t *)hbuf, TSDB_FILE_HEAD_SIZE);

  if (taosWrite(fd, hbuf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    close(fd);
    remove(tfname);
    return -1;
  }

  // Encode file status and write to file
  if (fsheader.len > 0) {
    if (tsdbMakeRoom(&(pBuf), fsheader.len) < 0) {
      close(fd);
      remove(tfname);
      return -1;
    }

    ptr = pBuf;
    tsdbEncodeFSStatus(&ptr, pStatus);
    taosCalcChecksumAppend(0, (uint8_t *)pBuf, fsheader.len);

    if (taosWrite(fd, pBuf, fsheader.len) < fsheader.len) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      close(fd);
      remove(tfname);
      taosTZfree(pBuf);
      return -1;
    }
  }

  // fsync, close and rename
  if (fsync(fd) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    close(fd);
    remove(tfname);
    taosTZfree(pBuf);
    return -1;
  }

  (void)close(fd);
  (void)taosRename(tfname, cfname);
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
  tsdbApplyMFileChange(pFrom->pmf, pTo->pmf);

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
  STsdbFS *  pfs = pIter->pfs;
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

static void tsdbGetTxnFname(int repoid, TSDB_TXN_FILE_T ftype, char fname[]) {
  snprintf(fname, TSDB_FILENAME_LEN, "%s/vnode/vnode%d/tsdb/%s", TFS_PRIMARY_PATH(), repoid, tsdbTxnFname[ftype]);
}

static int tsdbOpenFSFromCurrent(STsdbRepo *pRepo) {
  STsdbFS * pfs = REPO_FS(pRepo);
  int       fd = -1;
  void *    buffer = NULL;
  SFSHeader fsheader;
  char      current[TSDB_FILENAME_LEN] = "\0";
  void *    ptr;

  tsdbGetTxnFname(REPO_ID(pRepo), TSDB_TXN_CURR_FILE, current);

  // current file exists, try to recover
  fd = open(current, O_RDONLY | O_BINARY);
  if (fd < 0) {
    tsdbError("vgId:%d failed to open file %s since %s", REPO_ID(pRepo), current, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (tsdbMakeRoom(&buffer, TSDB_FILE_HEAD_SIZE) < 0) {
    goto _err;
  }

  int nread = (int)taosRead(fd, buffer, TSDB_FILE_HEAD_SIZE);
  if (nread < 0) {
    tsdbError("vgId:%d failed to read %d bytes from file %s since %s", REPO_ID(pRepo), TSDB_FILENAME_LEN, current,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (nread < TSDB_FILE_HEAD_SIZE) {
    tsdbError("vgId:%d failed to read header of file %s, read bytes:%d", REPO_ID(pRepo), current, nread);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    goto _err;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buffer, TSDB_FILE_HEAD_SIZE)) {
    tsdbError("vgId:%d header of file %s failed checksum check", REPO_ID(pRepo), current);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    goto _err;
  }

  SFSStatus *pStatus = pfs->cstatus;
  ptr = buffer;
  ptr = tsdbDecodeFSHeader(ptr, &fsheader);
  ptr = tsdbDecodeFSMeta(ptr, &(pStatus->meta));

  if (fsheader.version != TSDB_FS_VERSION) {
    // TODO: handle file version change
  }

  if (fsheader.len > 0) {
    if (tsdbMakeRoom(&buffer, fsheader.len) < 0) {
      goto _err;
    }

    nread = (int)taosRead(fd, buffer, fsheader.len);
    if (nread < 0) {
      tsdbError("vgId:%d failed to read file %s since %s", REPO_ID(pRepo), current, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (nread < fsheader.len) {
      tsdbError("vgId:%d failed to read %d bytes from file %s", REPO_ID(pRepo), fsheader.len, current);
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
      goto _err;
    }

    if (!taosCheckChecksumWhole((uint8_t *)buffer, fsheader.len)) {
      tsdbError("vgId:%d file %s is corrupted since wrong checksum", REPO_ID(pRepo), current);
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
      goto _err;
    }

    ptr = buffer;
    ptr = tsdbDecodeFSStatus(ptr, pStatus);
  } else {
    tsdbResetFSStatus(pStatus);
  }

  taosTZfree(buffer);
  close(fd);

  return 0;

_err:
  if (fd >= 0) {
    close(fd);
  }
  taosTZfree(buffer);
  return -1;
}

// Scan and try to fix incorrect files
static int tsdbScanAndTryFixFS(STsdbRepo *pRepo) {
  STsdbFS *  pfs = REPO_FS(pRepo);
  SFSStatus *pStatus = pfs->cstatus;

  if (tsdbScanAndTryFixMFile(pRepo) < 0) {
    tsdbError("vgId:%d failed to fix MFile since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  size_t size = taosArrayGetSize(pStatus->df);

  for (size_t i = 0; i < size; i++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pStatus->df, i);

    if (tsdbScanAndTryFixDFileSet(pRepo, pSet) < 0) {
      tsdbError("vgId:%d failed to fix MFile since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }
  }

  // remove those unused files
  tsdbScanRootDir(pRepo);
  tsdbScanDataDir(pRepo);
  return 0;
}

int tsdbLoadMetaCache(STsdbRepo *pRepo, bool recoverMeta) {
  char      tbuf[128];
  STsdbFS * pfs = REPO_FS(pRepo);
  SMFile    mf;
  SMFile *  pMFile = &mf;
  void *    pBuf = NULL;
  SKVRecord rInfo;
  int64_t   maxBufSize = 0;
  SMFInfo   minfo;

  taosHashEmpty(pfs->metaCache);

  // No meta file, just return
  if (pfs->cstatus->pmf == NULL) return 0;

  mf = pfs->cstatus->mf;
  // Load cache first
  if (tsdbOpenMFile(pMFile, O_RDONLY) < 0) {
    return -1;
  }

  if (tsdbLoadMFileHeader(pMFile, &minfo) < 0) {
    tsdbCloseMFile(pMFile);
    return -1;
  }

  while (true) {
    int64_t tsize = tsdbReadMFile(pMFile, tbuf, sizeof(SKVRecord));
    if (tsize == 0) break;

    if (tsize < 0) {
      tsdbError("vgId:%d failed to read META file since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }

    if (tsize < sizeof(SKVRecord)) {
      tsdbError("vgId:%d failed to read %" PRIzu " bytes from file %s", REPO_ID(pRepo), sizeof(SKVRecord),
                TSDB_FILE_FULL_NAME(pMFile));
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
      tsdbCloseMFile(pMFile);
      return -1;
    }

    void *ptr = tsdbDecodeKVRecord(tbuf, &rInfo);
    ASSERT(POINTER_DISTANCE(ptr, tbuf) == sizeof(SKVRecord));
    // ASSERT((rInfo.offset > 0) ? (pStore->info.size == rInfo.offset) : true);

    if (rInfo.offset < 0) {
      taosHashRemove(pfs->metaCache, (void *)(&rInfo.uid), sizeof(rInfo.uid));
#if 0
      pStore->info.size += sizeof(SKVRecord);
      pStore->info.nRecords--;
      pStore->info.nDels++;
      pStore->info.tombSize += (rInfo.size + sizeof(SKVRecord) * 2);
#endif
    } else {
      ASSERT(rInfo.offset > 0 && rInfo.size > 0);
      if (taosHashPut(pfs->metaCache, (void *)(&rInfo.uid), sizeof(rInfo.uid), &rInfo, sizeof(rInfo)) < 0) {
        tsdbError("vgId:%d failed to load meta cache from file %s since OOM", REPO_ID(pRepo),
                  TSDB_FILE_FULL_NAME(pMFile));
        terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
        tsdbCloseMFile(pMFile);
        return -1;
      }

      maxBufSize = MAX(maxBufSize, rInfo.size);

      if (tsdbSeekMFile(pMFile, rInfo.size, SEEK_CUR) < 0) {
        tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pMFile),
                  tstrerror(terrno));
        tsdbCloseMFile(pMFile);
        return -1;
      }

#if 0
      pStore->info.size += (sizeof(SKVRecord) + rInfo.size);
      pStore->info.nRecords++;
#endif
    }
  }

  if (recoverMeta) {
    pBuf = malloc((size_t)maxBufSize);
    if (pBuf == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      tsdbCloseMFile(pMFile);
      return -1;
    }

    SKVRecord *pRecord = taosHashIterate(pfs->metaCache, NULL);
    while (pRecord) {
      if (tsdbSeekMFile(pMFile, pRecord->offset + sizeof(SKVRecord), SEEK_SET) < 0) {
        tsdbError("vgId:%d failed to seek file %s since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pMFile),
                  tstrerror(terrno));
        tfree(pBuf);
        tsdbCloseMFile(pMFile);
        return -1;
      }

      int nread = (int)tsdbReadMFile(pMFile, pBuf, pRecord->size);
      if (nread < 0) {
        tsdbError("vgId:%d failed to read file %s since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pMFile),
                  tstrerror(terrno));
        tfree(pBuf);
        tsdbCloseMFile(pMFile);
        return -1;
      }

      if (nread < pRecord->size) {
        tsdbError("vgId:%d failed to read file %s since file corrupted, expected read:%" PRId64 " actual read:%d",
                  REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pMFile), pRecord->size, nread);
        terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
        tfree(pBuf);
        tsdbCloseMFile(pMFile);
        return -1;
      }

      if (tsdbRestoreTable(pRepo, pBuf, (int)pRecord->size) < 0) {
        tsdbError("vgId:%d failed to restore table, uid %" PRId64 ", since %s" PRIu64, REPO_ID(pRepo), pRecord->uid,
                  tstrerror(terrno));
        tfree(pBuf);
        tsdbCloseMFile(pMFile);
        return -1;
      }

      pRecord = taosHashIterate(pfs->metaCache, pRecord);
    }

    tsdbOrgMeta(pRepo);
  }

  tsdbCloseMFile(pMFile);
  tfree(pBuf);
  return 0;
}

static int tsdbScanRootDir(STsdbRepo *pRepo) {
  char         rootDir[TSDB_FILENAME_LEN];
  char         bname[TSDB_FILENAME_LEN];
  STsdbFS *    pfs = REPO_FS(pRepo);
  const TFILE *pf;

  tsdbGetRootDir(REPO_ID(pRepo), rootDir);
  TDIR *tdir = tfsOpendir(rootDir);
  if (tdir == NULL) {
    tsdbError("vgId:%d failed to open directory %s since %s", REPO_ID(pRepo), rootDir, tstrerror(terrno));
    return -1;
  }

  while ((pf = tfsReaddir(tdir))) {
    tfsbasename(pf, bname);

    if (strcmp(bname, tsdbTxnFname[TSDB_TXN_CURR_FILE]) == 0 || strcmp(bname, "data") == 0) {
      // Skip current file and data directory
      continue;
    }

    if (pfs->cstatus->pmf && tfsIsSameFile(pf, &(pfs->cstatus->pmf->f))) {
      continue;
    }

    tfsremove(pf);
    tsdbDebug("vgId:%d invalid file %s is removed", REPO_ID(pRepo), TFILE_NAME(pf));
  }

  tfsClosedir(tdir);

  return 0;
}

static int tsdbScanDataDir(STsdbRepo *pRepo) {
  char         dataDir[TSDB_FILENAME_LEN];
  char         bname[TSDB_FILENAME_LEN];
  STsdbFS *    pfs = REPO_FS(pRepo);
  const TFILE *pf;

  tsdbGetDataDir(REPO_ID(pRepo), dataDir);
  TDIR *tdir = tfsOpendir(dataDir);
  if (tdir == NULL) {
    tsdbError("vgId:%d failed to open directory %s since %s", REPO_ID(pRepo), dataDir, tstrerror(terrno));
    return -1;
  }

  while ((pf = tfsReaddir(tdir))) {
    tfsbasename(pf, bname);

    if (!tsdbIsTFileInFS(pfs, pf)) {
      tfsremove(pf);
      tsdbDebug("vgId:%d invalid file %s is removed", REPO_ID(pRepo), TFILE_NAME(pf));
    }
  }

  tfsClosedir(tdir);

  return 0;
}

static bool tsdbIsTFileInFS(STsdbFS *pfs, const TFILE *pf) {
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

static int tsdbRestoreMeta(STsdbRepo *pRepo) {
  char         rootDir[TSDB_FILENAME_LEN];
  char         bname[TSDB_FILENAME_LEN];
  TDIR *       tdir = NULL;
  const TFILE *pf = NULL;
  const char * pattern = "^meta(-ver[0-9]+)?$";
  regex_t      regex;
  STsdbFS *    pfs = REPO_FS(pRepo);

  regcomp(&regex, pattern, REG_EXTENDED);

  tsdbInfo("vgId:%d try to restore meta", REPO_ID(pRepo));

  tsdbGetRootDir(REPO_ID(pRepo), rootDir);

  tdir = tfsOpendir(rootDir);
  if (tdir == NULL) {
    tsdbError("vgId:%d failed to open dir %s since %s", REPO_ID(pRepo), rootDir, tstrerror(terrno));
    regfree(&regex);
    return -1;
  }

  while ((pf = tfsReaddir(tdir))) {
    tfsbasename(pf, bname);

    if (strcmp(bname, "data") == 0) {
      // Skip the data/ directory
      continue;
    }

    if (strcmp(bname, tsdbTxnFname[TSDB_TXN_TEMP_FILE]) == 0) {
      // Skip current.t file
      tsdbInfo("vgId:%d file %s exists, remove it", REPO_ID(pRepo), TFILE_NAME(pf));
      tfsremove(pf);
      continue;
    }

    int code = regexec(&regex, bname, 0, NULL, 0);
    if (code == 0) {
      // Match
      if (pfs->cstatus->pmf != NULL) {
        tsdbError("vgId:%d failed to restore meta since two file exists, file1 %s and file2 %s", REPO_ID(pRepo),
                  TSDB_FILE_FULL_NAME(pfs->cstatus->pmf), TFILE_NAME(pf));
        terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
        tfsClosedir(tdir);
        regfree(&regex);
        return -1;
      } else {
        uint32_t version = 0;
        if (strcmp(bname, "meta") != 0) {
          sscanf(bname, "meta-ver%" PRIu32, &version);
          pfs->cstatus->meta.version = version;
        }

        pfs->cstatus->pmf = &(pfs->cstatus->mf);
        pfs->cstatus->pmf->f = *pf;
        TSDB_FILE_SET_CLOSED(pfs->cstatus->pmf);

        if (tsdbOpenMFile(pfs->cstatus->pmf, O_RDONLY) < 0) {
          tsdbError("vgId:%d failed to restore meta since %s", REPO_ID(pRepo), tstrerror(terrno));
          tfsClosedir(tdir);
          regfree(&regex);
          return -1;
        }

        if (tsdbLoadMFileHeader(pfs->cstatus->pmf, &(pfs->cstatus->pmf->info)) < 0) {
          tsdbError("vgId:%d failed to restore meta since %s", REPO_ID(pRepo), tstrerror(terrno));
          tsdbCloseMFile(pfs->cstatus->pmf);
          tfsClosedir(tdir);
          regfree(&regex);
          return -1;
        }

        tsdbCloseMFile(pfs->cstatus->pmf);
      }
    } else if (code == REG_NOMATCH) {
      // Not match
      tsdbInfo("vgId:%d invalid file %s exists, remove it", REPO_ID(pRepo), TFILE_NAME(pf));
      tfsremove(pf);
      continue;
    } else {
      // Has other error
      tsdbError("vgId:%d failed to restore meta file while run regexec since %s", REPO_ID(pRepo), strerror(code));
      terrno = TAOS_SYSTEM_ERROR(code);
      tfsClosedir(tdir);
      regfree(&regex);
      return -1;
    }
  }

  if (pfs->cstatus->pmf) {
    tsdbInfo("vgId:%d meta file %s is restored", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pfs->cstatus->pmf));
  } else {
    tsdbInfo("vgId:%d no meta file is restored", REPO_ID(pRepo));
  }

  tfsClosedir(tdir);
  regfree(&regex);
  return 0;
}

static int tsdbRestoreDFileSet(STsdbRepo *pRepo) {
  char         dataDir[TSDB_FILENAME_LEN];
  char         bname[TSDB_FILENAME_LEN];
  TDIR *       tdir = NULL;
  const TFILE *pf = NULL;
  const char * pattern = "^v[0-9]+f[0-9]+\\.(head|data|last)(-ver[0-9]+)?$";
  SArray *     fArray = NULL;
  regex_t      regex;
  STsdbFS *    pfs = REPO_FS(pRepo);

  tsdbGetDataDir(REPO_ID(pRepo), dataDir);

  // Resource allocation and init
  regcomp(&regex, pattern, REG_EXTENDED);

  fArray = taosArrayInit(1024, sizeof(TFILE));
  if (fArray == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbError("vgId:%d failed to restore DFileSet while open directory %s since %s", REPO_ID(pRepo), dataDir,
              tstrerror(terrno));
    regfree(&regex);
    return -1;
  }

  tdir = tfsOpendir(dataDir);
  if (tdir == NULL) {
    tsdbError("vgId:%d failed to restore DFileSet while open directory %s since %s", REPO_ID(pRepo), dataDir,
              tstrerror(terrno));
    taosArrayDestroy(fArray);
    regfree(&regex);
    return -1;
  }

  while ((pf = tfsReaddir(tdir))) {
    tfsbasename(pf, bname);

    int code = regexec(&regex, bname, 0, NULL, 0);
    if (code == 0) {
      if (taosArrayPush(fArray, (void *)pf) < 0) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        tfsClosedir(tdir);
        taosArrayDestroy(fArray);
        regfree(&regex);
        return -1;
      }
    } else if (code == REG_NOMATCH) {
      // Not match
      tsdbInfo("vgId:%d invalid file %s exists, remove it", REPO_ID(pRepo), TFILE_NAME(pf));
      tfsremove(pf);
      continue;
    } else {
      // Has other error
      tsdbError("vgId:%d failed to restore DFileSet Array while run regexec since %s", REPO_ID(pRepo), strerror(code));
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
        tsdbError("vgId:%d incomplete DFileSet, fid:%d", REPO_ID(pRepo), fset.fid);
        taosArrayDestroy(fArray);
        return -1;
      }

      pf = taosArrayGet(fArray, index);

      int         tvid, tfid;
      TSDB_FILE_T ttype;
      uint32_t    tversion;
      char        bname[TSDB_FILENAME_LEN];

      tfsbasename(pf, bname);
      tsdbParseDFilename(bname, &tvid, &tfid, &ttype, &tversion);

      ASSERT(tvid == REPO_ID(pRepo));

      if (ftype == 0) {
        fset.fid = tfid;
      } else {
        if (tfid != fset.fid) {
          tsdbError("vgId:%d incomplete dFileSet, fid:%d", REPO_ID(pRepo), fset.fid);
          taosArrayDestroy(fArray);
          return -1;
        }
      }

      if (ttype != ftype) {
        tsdbError("vgId:%d incomplete dFileSet, fid:%d", REPO_ID(pRepo), fset.fid);
        taosArrayDestroy(fArray);
        return -1;
      }

      pDFile->f = *pf;
      
      if (tsdbOpenDFile(pDFile, O_RDONLY) < 0) {
        tsdbError("vgId:%d failed to open DFile %s since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile), tstrerror(terrno));
        taosArrayDestroy(fArray);
        return -1;
      }

      if (tsdbLoadDFileHeader(pDFile, &(pDFile->info)) < 0) {
        tsdbError("vgId:%d failed to load DFile %s header since %s", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile),
                  tstrerror(terrno));
        taosArrayDestroy(fArray);
        return -1;
      }

      tsdbCloseDFile(pDFile);
      index++;
    }

    tsdbInfo("vgId:%d FSET %d is restored", REPO_ID(pRepo), fset.fid);
    taosArrayPush(pfs->cstatus->df, &fset);
  }

  // Resource release
  taosArrayDestroy(fArray);

  return 0;
}

static int tsdbRestoreCurrent(STsdbRepo *pRepo) {
  // Loop to recover mfile
  if (tsdbRestoreMeta(pRepo) < 0) {
    tsdbError("vgId:%d failed to restore current since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  // Loop to recover dfile set
  if (tsdbRestoreDFileSet(pRepo) < 0) {
    tsdbError("vgId:%d failed to restore DFileSet since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  if (tsdbSaveFSStatus(pRepo->fs->cstatus, REPO_ID(pRepo)) < 0) {
    tsdbError("vgId:%d failed to restore corrent since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  return 0;
}

static int tsdbComparTFILE(const void *arg1, const void *arg2) {
  TFILE *pf1 = (TFILE *)arg1;
  TFILE *pf2 = (TFILE *)arg2;

  int         vid1, fid1, vid2, fid2;
  TSDB_FILE_T ftype1, ftype2;
  uint32_t    version1, version2;
  char        bname1[TSDB_FILENAME_LEN];
  char        bname2[TSDB_FILENAME_LEN];

  tfsbasename(pf1, bname1);
  tfsbasename(pf2, bname2);
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

static void tsdbScanAndTryFixDFilesHeader(STsdbRepo *pRepo) {
  STsdbFS *  pfs = REPO_FS(pRepo);
  SFSStatus *pStatus = pfs->cstatus;
  SDFInfo    info;

  for (size_t i = 0; i < taosArrayGetSize(pStatus->df); i++) {
    SDFileSet fset;
    tsdbInitDFileSetEx(&fset, (SDFileSet *)taosArrayGet(pStatus->df, i));

    tsdbDebug("vgId:%d scan DFileSet %d header", REPO_ID(pRepo), fset.fid);

    if (tsdbOpenDFileSet(&fset, O_RDWR) < 0) {
      tsdbError("vgId:%d failed to open DFileSet %d since %s, continue", REPO_ID(pRepo), fset.fid, tstrerror(terrno));
      continue;
    }

    for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
      SDFile *pDFile = TSDB_DFILE_IN_SET(&fset, ftype);

      if ((tsdbLoadDFileHeader(pDFile, &info) < 0) || pDFile->info.size != info.size ||
          pDFile->info.magic != info.magic) {
        if (tsdbUpdateDFileHeader(pDFile) < 0) {
          tsdbError("vgId:%d failed to update DFile header of %s since %s, continue", REPO_ID(pRepo),
                    TSDB_FILE_FULL_NAME(pDFile), tstrerror(terrno));
        } else {
          tsdbInfo("vgId:%d DFile header of %s is updated", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile));
          TSDB_FILE_FSYNC(pDFile);
        }
      } else {
        tsdbDebug("vgId:%d DFile header of %s is correct", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile));
      }
    }

    tsdbCloseDFileSet(&fset);
  }
}