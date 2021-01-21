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

typedef enum { TSDB_TXN_TEMP_FILE = 0, TSDB_TXN_CURR_FILE } TSDB_TXN_FILE_T;
static const char *tsdbTxnFname[] = {"current.t", "current"};
#define TSDB_MAX_FSETS(keep, days) ((keep) / (days) + 3)

static int  tsdbComparFidFSet(const void *arg1, const void *arg2);
static void tsdbResetFSStatus(SFSStatus *pStatus);
static int  tsdbApplyFSTxn(STsdbFS *pfs, int vid);
static void tsdbApplyFSTxnOnDisk(SFSStatus *pFrom, SFSStatus *pTo);
static void tsdbGetTxnFname(int repoid, TSDB_TXN_FILE_T ftype, char fname[]);
static int  tsdbOpenFSFromCurrent(STsdbRepo *pRepo);
static int  tsdbScanAndTryFixFS(STsdbRepo *pRepo);

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
  ASSERT(pStatus->pmf == NULL && TSDB_FILE_CLOSED(pMFile));

  pStatus->pmf = &(pStatus->mf);
  *(pStatus->pmf) = *pMFile;
}

static int tsdbAddDFileSetToStatus(SFSStatus *pStatus, const SDFileSet *pSet) {
  ASSERT(TSDB_FILE_CLOSED(&(pSet->files[0])));
  ASSERT(TSDB_FILE_CLOSED(&(pSet->files[1])));
  ASSERT(TSDB_FILE_CLOSED(&(pSet->files[2])));

  if (taosArrayPush(pStatus->df, (void *)pSet) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

// ================== STsdbFS
// TODO
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

// TODO
void *tsdbFreeFS(STsdbFS *pfs) {
  if (pfs) {
    pfs->nstatus = tsdbFreeFSStatus(pfs->nstatus);
    taosHashCleanup(pfs->metaCache);
    pfs->metaCache = NULL;
    pfs->cstatus = tsdbFreeFSStatus(pfs->cstatus);
    pthread_rwlock_destroy(&(pfs->lock));
  }

  return NULL;
}

// TODO
int tsdbOpenFS(STsdbRepo *pRepo) {
  STsdbFS * pfs = REPO_FS(pRepo);
  char      current[TSDB_FILENAME_LEN] = "\0";

  ASSERT(pfs != NULL);

  tsdbGetTxnFname(REPO_ID(pRepo), TSDB_TXN_CURR_FILE, current);

  if (access(current, F_OK) == 0) {
    if (tsdbOpenFSFromCurrent(pRepo) < 0) {
      tsdbError("vgId:%d failed to open FS since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }
 } else {
    // TODO: current file not exists, try to recover it
  }

  // Load meta cache if has meta file
  if (tsdbLoadMetaCache(pRepo, true) < 0) {
    tsdbError("vgId:%d failed to open FS while loading meta cache since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  return 0;
}

// TODO
void tsdbCloseFS(STsdbRepo *pRepo) {
  // TODO
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
  if (tsdbApplyFSTxn(pfs, REPO_ID(pRepo)) < 0) {
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

static int tsdbApplyFSTxn(STsdbFS *pfs, int vid) {
  ASSERT(FS_IN_TXN(pfs));
  SFSHeader fsheader;
  void *    pBuf = NULL;
  void *    ptr;
  char      hbuf[TSDB_FILE_HEAD_SIZE] = "\0";
  char      tfname[TSDB_FILENAME_LEN] = "\0";
  char      cfname[TSDB_FILENAME_LEN] = "\0";

  tsdbGetTxnFname(vid, TSDB_TXN_TEMP_FILE, tfname);
  tsdbGetTxnFname(vid, TSDB_TXN_CURR_FILE, cfname);

  int fd = open(tfname, O_WRONLY | O_CREAT | O_TRUNC, 0755);
  if (fd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  fsheader.version = TSDB_FS_VERSION;
  if (pfs->nstatus->pmf == NULL) {
    ASSERT(taosArrayGetSize(pfs->nstatus->df) == 0);
    fsheader.len = 0;
  } else {
    fsheader.len = tsdbEncodeFSStatus(NULL, pfs->nstatus) + sizeof(TSCKSUM);
  }

  // Encode header part and write
  ptr = hbuf;
  tsdbEncodeFSHeader(&ptr, &fsheader);
  tsdbEncodeFSMeta(&ptr, &(pfs->nstatus->meta));

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
    tsdbEncodeFSStatus(&ptr, pfs->nstatus);
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
  (void)rename(tfname, cfname);
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
      pIter->index = size - 1;
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
    pIter->index = TARRAY_ELEM_IDX(pfs->cstatus->df, ptr);
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

  if (pIter->index > 0) {
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
  fd = open(current, O_RDONLY);
  if (fd < 0) {
    tsdbError("vgId:%d failed to open file %s since %s", REPO_ID(pRepo), current, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (tsdbMakeRoom(&buffer, TSDB_FILE_HEAD_SIZE) < 0) {
    goto _err;
  }

  int nread = taosRead(fd, buffer, TSDB_FILE_HEAD_SIZE);
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

    nread = taosRead(fd, buffer, fsheader.len);
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

  if (tsdbScanAndTryFixFS(pRepo) < 0) {
    return -1;
  }

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

  if (pStatus->pmf) {
    if (tsdbScanAndTryFixMFile(pStatus->pmf) < 0) {
      tsdbError("vgId:%d failed to fix MFile since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }
  }

  size_t size = taosArrayGetSize(pStatus->df);

  for (size_t i = 0; i < size; i++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pStatus->df, i);

    if (tsdbScanAndTryFixDFileSet(pSet) < 0) {
      tsdbError("vgId:%d failed to fix MFile since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }
  }

  // TODO: remove those unused files
  {}

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

  // TODO: clear meta at first

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
    pBuf = malloc(maxBufSize);
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

      int nread = tsdbReadMFile(pMFile, pBuf, pRecord->size);
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

      if (tsdbRestoreTable(pRepo, pBuf, pRecord->size) < 0) {
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