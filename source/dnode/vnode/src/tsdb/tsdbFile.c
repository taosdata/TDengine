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

static const char *TSDB_FNAME_SUFFIX[] = {
    "head",  // TSDB_FILE_HEAD
    "data",  // TSDB_FILE_DATA
    "last",  // TSDB_FILE_LAST
    "smad",  // TSDB_FILE_SMAD
    "smal",  // TSDB_FILE_SMAL
    "",      // TSDB_FILE_MAX
    "meta",  // TSDB_FILE_META
};

static void tsdbGetFilename(int vid, int fid, uint32_t ver, TSDB_FILE_T ftype, const char* dname, char *fname);
// static int   tsdbRollBackMFile(SMFile *pMFile);
static int   tsdbEncodeDFInfo(void **buf, SDFInfo *pInfo);
static void *tsdbDecodeDFInfo(void *buf, SDFInfo *pInfo);
static int   tsdbRollBackDFile(SDFile *pDFile);

// ============== Operations on SDFile
void tsdbInitDFile(STsdb *pRepo, SDFile *pDFile, SDiskID did, int fid, uint32_t ver, TSDB_FILE_T ftype) {
  char fname[TSDB_FILENAME_LEN];

  TSDB_FILE_SET_STATE(pDFile, TSDB_FILE_STATE_OK);

  TSDB_FILE_SET_CLOSED(pDFile);

  memset(&(pDFile->info), 0, sizeof(pDFile->info));
  pDFile->info.magic = TSDB_FILE_INIT_MAGIC;
  pDFile->info.fver = tsdbGetDFSVersion(ftype);

  tsdbGetFilename(REPO_ID(pRepo), fid, ver, ftype, pRepo->dir, fname);
  tfsInitFile(REPO_TFS(pRepo), &(pDFile->f), did, fname);
}

void tsdbInitDFileEx(SDFile *pDFile, SDFile *pODFile) {
  *pDFile = *pODFile;
  TSDB_FILE_SET_CLOSED(pDFile);
}

int tsdbEncodeSDFile(void **buf, SDFile *pDFile) {
  int tlen = 0;

  tlen += tsdbEncodeDFInfo(buf, &(pDFile->info));
  tlen += tfsEncodeFile(buf, &(pDFile->f));

  return tlen;
}

void *tsdbDecodeSDFile(STsdb *pRepo, void *buf, SDFile *pDFile) {
  buf = tsdbDecodeDFInfo(buf, &(pDFile->info));
  buf = tfsDecodeFile(REPO_TFS(pRepo), buf, &(pDFile->f));
  TSDB_FILE_SET_CLOSED(pDFile);

  return buf;
}

static int tsdbEncodeSDFileEx(void **buf, SDFile *pDFile) {
  int tlen = 0;

  tlen += tsdbEncodeDFInfo(buf, &(pDFile->info));
  tlen += taosEncodeString(buf, TSDB_FILE_FULL_NAME(pDFile));

  return tlen;
}

static void *tsdbDecodeSDFileEx(void *buf, SDFile *pDFile) {
  char *aname = NULL;

  buf = tsdbDecodeDFInfo(buf, &(pDFile->info));
  buf = taosDecodeString(buf, &aname);
  strncpy(TSDB_FILE_FULL_NAME(pDFile), aname, TSDB_FILENAME_LEN);
  TSDB_FILE_SET_CLOSED(pDFile);
  taosMemoryFreeClear(aname);

  return buf;
}

int tsdbCreateDFile(STsdb *pRepo, SDFile *pDFile, bool updateHeader, TSDB_FILE_T fType) {
  ASSERT(pDFile->info.size == 0 && pDFile->info.magic == TSDB_FILE_INIT_MAGIC);

  pDFile->pFile = taosOpenFile(TSDB_FILE_FULL_NAME(pDFile), TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pDFile->pFile == NULL) {
    if (errno == ENOENT) {
      // Try to create directory recursively
      char *s = strdup(TSDB_FILE_REL_NAME(pDFile));
      if (tfsMkdirRecurAt(REPO_TFS(pRepo), taosDirName(s), TSDB_FILE_DID(pDFile)) < 0) {
        taosMemoryFreeClear(s);
        return -1;
      }
      taosMemoryFreeClear(s);

      pDFile->pFile = taosOpenFile(TSDB_FILE_FULL_NAME(pDFile), TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
      if (pDFile->pFile == NULL) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
    } else {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }

  if (!updateHeader) {
    return 0;
  }

  pDFile->info.size += TSDB_FILE_HEAD_SIZE;
  pDFile->info.fver = tsdbGetDFSVersion(fType);

  if (tsdbUpdateDFileHeader(pDFile) < 0) {
    tsdbCloseDFile(pDFile);
    tsdbRemoveDFile(pDFile);
    return -1;
  }

  return 0;
}

int tsdbUpdateDFileHeader(SDFile *pDFile) {
  char buf[TSDB_FILE_HEAD_SIZE] = "\0";

  if (tsdbSeekDFile(pDFile, 0, SEEK_SET) < 0) {
    return -1;
  }

  void *ptr = buf;
  // taosEncodeFixedU32(&ptr, 0); // fver moved to SDFInfo and saved to current
  tsdbEncodeDFInfo(&ptr, &(pDFile->info));

  taosCalcChecksumAppend(0, (uint8_t *)buf, TSDB_FILE_HEAD_SIZE);
  if (tsdbWriteDFile(pDFile, buf, TSDB_FILE_HEAD_SIZE) < 0) {
    return -1;
  }

  return 0;
}

int tsdbLoadDFileHeader(SDFile *pDFile, SDFInfo *pInfo) {
  char     buf[TSDB_FILE_HEAD_SIZE] = "\0";
  uint32_t _version;

  ASSERT(TSDB_FILE_OPENED(pDFile));

  if (tsdbSeekDFile(pDFile, 0, SEEK_SET) < 0) {
    return -1;
  }

  if (tsdbReadDFile(pDFile, buf, TSDB_FILE_HEAD_SIZE) < 0) {
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buf, TSDB_FILE_HEAD_SIZE)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  void *pBuf = buf;
  // pBuf = taosDecodeFixedU32(pBuf, &_version);
  pBuf = tsdbDecodeDFInfo(pBuf, pInfo);
  return 0;
}

static int tsdbScanAndTryFixDFile(STsdb *pRepo, SDFile *pDFile) {
  SDFile df;

  tsdbInitDFileEx(&df, pDFile);

  if (!taosCheckExistFile(TSDB_FILE_FULL_NAME(pDFile))) {
    tsdbError("vgId:%d data file %s not exit, report to upper layer to fix it", REPO_ID(pRepo),
              TSDB_FILE_FULL_NAME(pDFile));
    // pRepo->state |= TSDB_STATE_BAD_DATA;
    TSDB_FILE_SET_STATE(pDFile, TSDB_FILE_STATE_BAD);
    return 0;
  }
  int64_t file_size = 0;
  if (taosStatFile(TSDB_FILE_FULL_NAME(&df), &file_size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (pDFile->info.size < file_size) {
    // if (tsdbOpenDFile(&df, O_WRONLY) < 0) {
    if (tsdbOpenDFile(&df, TD_FILE_WRITE) < 0) {
      return -1;
    }

    if (taosFtruncateFile(df.pFile, df.info.size) < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      tsdbCloseDFile(&df);
      return -1;
    }

    if (tsdbUpdateDFileHeader(&df) < 0) {
      tsdbCloseDFile(&df);
      return -1;
    }

    tsdbCloseDFile(&df);
    tsdbInfo("vgId:%d file %s is truncated from %" PRId64 " to %" PRId64, REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile),
             file_size, pDFile->info.size);
  } else if (pDFile->info.size > file_size) {
    tsdbError("vgId:%d data file %s has wrong size %" PRId64 " expected %" PRId64 ", report to upper layer to fix it",
              REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile), file_size, pDFile->info.size);
    // pRepo->state |= TSDB_STATE_BAD_DATA;
    TSDB_FILE_SET_STATE(pDFile, TSDB_FILE_STATE_BAD);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return 0;
  } else {
    tsdbDebug("vgId:%d file %s passes the scan", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile));
  }

  return 0;
}

static int tsdbEncodeDFInfo(void **buf, SDFInfo *pInfo) {
  int tlen = 0;

  tlen += taosEncodeFixedU32(buf, pInfo->magic);
  tlen += taosEncodeFixedU32(buf, pInfo->fver);
  tlen += taosEncodeFixedU32(buf, pInfo->len);
  tlen += taosEncodeFixedU32(buf, pInfo->totalBlocks);
  tlen += taosEncodeFixedU32(buf, pInfo->totalSubBlocks);
  tlen += taosEncodeFixedU32(buf, pInfo->offset);
  tlen += taosEncodeFixedU64(buf, pInfo->size);
  tlen += taosEncodeFixedU64(buf, pInfo->tombSize);

  return tlen;
}

static void *tsdbDecodeDFInfo(void *buf, SDFInfo *pInfo) {
  buf = taosDecodeFixedU32(buf, &(pInfo->magic));
  buf = taosDecodeFixedU32(buf, &(pInfo->fver));
  buf = taosDecodeFixedU32(buf, &(pInfo->len));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalBlocks));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalSubBlocks));
  buf = taosDecodeFixedU32(buf, &(pInfo->offset));
  buf = taosDecodeFixedU64(buf, &(pInfo->size));
  buf = taosDecodeFixedU64(buf, &(pInfo->tombSize));

  return buf;
}

static int tsdbApplyDFileChange(SDFile *from, SDFile *to) {
  ASSERT(from != NULL || to != NULL);

  if (from != NULL) {
    if (to == NULL) {
      tsdbRemoveDFile(from);
    } else {
      if (tfsIsSameFile(TSDB_FILE_F(from), TSDB_FILE_F(to))) {
        if (from->info.size > to->info.size) {
          tsdbRollBackDFile(to);
        }
      } else {
        (void)tsdbRemoveDFile(from);
      }
    }
  }

  return 0;
}

static int tsdbRollBackDFile(SDFile *pDFile) {
  SDFile df = *pDFile;

  // if (tsdbOpenDFile(&df, O_WRONLY) < 0) {
  if (tsdbOpenDFile(&df, TD_FILE_WRITE) < 0) {
    return -1;
  }

  if (taosFtruncateFile(TSDB_FILE_PFILE(&df), pDFile->info.size) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    tsdbCloseDFile(&df);
    return -1;
  }

  if (tsdbUpdateDFileHeader(&df) < 0) {
    tsdbCloseDFile(&df);
    return -1;
  }

  TSDB_FILE_FSYNC(&df);

  tsdbCloseDFile(&df);
  return 0;
}

// ============== Operations on SDFileSet
void tsdbInitDFileSet(STsdb *pRepo, SDFileSet *pSet, SDiskID did, int fid, uint32_t ver) {
  TSDB_FSET_FID(pSet) = fid;
  TSDB_FSET_VER(pSet) = TSDB_LATEST_FSET_VER;
  TSDB_FSET_STATE(pSet) = 0;
  pSet->reserve = 0;

  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    SDFile *pDFile = TSDB_DFILE_IN_SET(pSet, ftype);
    tsdbInitDFile(pRepo, pDFile, did, fid, ver, ftype);
  }
}

void tsdbInitDFileSetEx(SDFileSet *pSet, SDFileSet *pOSet) {
  TSDB_FSET_FID(pSet) = TSDB_FSET_FID(pOSet);
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    tsdbInitDFileEx(TSDB_DFILE_IN_SET(pSet, ftype), TSDB_DFILE_IN_SET(pOSet, ftype));
  }
}

int tsdbEncodeDFileSet(void **buf, SDFileSet *pSet) {
  int tlen = 0;

  tlen += taosEncodeFixedI32(buf, TSDB_FSET_FID(pSet));
  // state not included
  tlen += taosEncodeFixedU8(buf, TSDB_FSET_VER(pSet));
  tlen += taosEncodeFixedU16(buf, pSet->reserve);
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    tlen += tsdbEncodeSDFile(buf, TSDB_DFILE_IN_SET(pSet, ftype));
  }

  return tlen;
}

void *tsdbDecodeDFileSet(STsdb *pRepo, void *buf, SDFileSet *pSet) {
  buf = taosDecodeFixedI32(buf, &(TSDB_FSET_FID(pSet)));
  TSDB_FSET_STATE(pSet) = 0;
  buf = taosDecodeFixedU8(buf, &(TSDB_FSET_VER(pSet)));
  buf = taosDecodeFixedU16(buf, &(pSet->reserve));

  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    buf = tsdbDecodeSDFile(pRepo, buf, TSDB_DFILE_IN_SET(pSet, ftype));
  }
  return buf;
}

int tsdbEncodeDFileSetEx(void **buf, SDFileSet *pSet) {
  int tlen = 0;

  tlen += taosEncodeFixedI32(buf, TSDB_FSET_FID(pSet));
  tlen += taosEncodeFixedU8(buf, TSDB_FSET_VER(pSet));
  tlen += taosEncodeFixedU16(buf, pSet->reserve);
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    tlen += tsdbEncodeSDFileEx(buf, TSDB_DFILE_IN_SET(pSet, ftype));
  }

  return tlen;
}

void *tsdbDecodeDFileSetEx(void *buf, SDFileSet *pSet) {
  buf = taosDecodeFixedI32(buf, &(TSDB_FSET_FID(pSet)));
  buf = taosDecodeFixedU8(buf, &(TSDB_FSET_VER(pSet)));
  buf = taosDecodeFixedU16(buf, &(pSet->reserve));

  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    buf = tsdbDecodeSDFileEx(buf, TSDB_DFILE_IN_SET(pSet, ftype));
  }
  return buf;
}

int tsdbApplyDFileSetChange(SDFileSet *from, SDFileSet *to) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    SDFile *pDFileFrom = (from) ? TSDB_DFILE_IN_SET(from, ftype) : NULL;
    SDFile *pDFileTo = (to) ? TSDB_DFILE_IN_SET(to, ftype) : NULL;
    if (tsdbApplyDFileChange(pDFileFrom, pDFileTo) < 0) {
      return -1;
    }
  }

  return 0;
}

int tsdbCreateDFileSet(STsdb *pRepo, SDFileSet *pSet, bool updateHeader) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    if (tsdbCreateDFile(pRepo, TSDB_DFILE_IN_SET(pSet, ftype), updateHeader, ftype) < 0) {
      tsdbCloseDFileSet(pSet);
      tsdbRemoveDFileSet(pSet);
      return -1;
    }
  }

  return 0;
}

int tsdbUpdateDFileSetHeader(SDFileSet *pSet) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    if (tsdbUpdateDFileHeader(TSDB_DFILE_IN_SET(pSet, ftype)) < 0) {
      return -1;
    }
  }
  return 0;
}

int tsdbScanAndTryFixDFileSet(STsdb *pRepo, SDFileSet *pSet) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    if (tsdbScanAndTryFixDFile(pRepo, TSDB_DFILE_IN_SET(pSet, ftype)) < 0) {
      return -1;
    }
  }
  return 0;
}

int tsdbParseDFilename(const char *fname, int *vid, int *fid, TSDB_FILE_T *ftype, uint32_t *_version) {
  char *p = NULL;
  *_version = 0;
  *ftype = TSDB_FILE_MAX;

  sscanf(fname, "v%df%d.%m[a-z]-ver%" PRIu32, vid, fid, &p, _version);
  for (TSDB_FILE_T i = 0; i < TSDB_FILE_MAX; i++) {
    if (strcmp(p, TSDB_FNAME_SUFFIX[i]) == 0) {
      *ftype = i;
      break;
    }
  }

  taosMemoryFreeClear(p);
  return 0;
}

static void tsdbGetFilename(int vid, int fid, uint32_t ver, TSDB_FILE_T ftype, const char *dname, char *fname) {
  ASSERT(ftype != TSDB_FILE_MAX);

  if (ftype < TSDB_FILE_MAX) {
    if (ver == 0) {
      snprintf(fname, TSDB_FILENAME_LEN, "vnode/vnode%d/%s/data/v%df%d.%s", vid, dname, vid, fid,
               TSDB_FNAME_SUFFIX[ftype]);
    } else {
      snprintf(fname, TSDB_FILENAME_LEN, "vnode/vnode%d/%s/data/v%df%d.%s-ver%" PRIu32, vid, dname, vid, fid,
               TSDB_FNAME_SUFFIX[ftype], ver);
    }
  } else {
    if (ver == 0) {
      snprintf(fname, TSDB_FILENAME_LEN, "vnode/vnode%d/tsdb/%s", vid, TSDB_FNAME_SUFFIX[ftype]);
    } else {
      snprintf(fname, TSDB_FILENAME_LEN, "vnode/vnode%d/tsdb/%s-ver%" PRIu32, vid, TSDB_FNAME_SUFFIX[ftype], ver);
    }
  }
}