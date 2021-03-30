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

static const char *TSDB_FNAME_SUFFIX[] = {
    "head",  // TSDB_FILE_HEAD
    "data",  // TSDB_FILE_DATA
    "last",  // TSDB_FILE_LAST
    "",      // TSDB_FILE_MAX
    "meta"   // TSDB_FILE_META
};

static void  tsdbGetFilename(int vid, int fid, uint32_t ver, TSDB_FILE_T ftype, char *fname);
static int   tsdbRollBackMFile(SMFile *pMFile);
static int   tsdbEncodeDFInfo(void **buf, SDFInfo *pInfo);
static void *tsdbDecodeDFInfo(void *buf, SDFInfo *pInfo);
static int   tsdbRollBackDFile(SDFile *pDFile);

// ============== SMFile
void tsdbInitMFile(SMFile *pMFile, SDiskID did, int vid, uint32_t ver) {
  char fname[TSDB_FILENAME_LEN];

  TSDB_FILE_SET_STATE(pMFile, TSDB_FILE_STATE_OK);

  memset(&(pMFile->info), 0, sizeof(pMFile->info));
  pMFile->info.magic = TSDB_FILE_INIT_MAGIC;

  tsdbGetFilename(vid, 0, ver, TSDB_FILE_META, fname);
  tfsInitFile(TSDB_FILE_F(pMFile), did.level, did.id, fname);
}

void tsdbInitMFileEx(SMFile *pMFile, const SMFile *pOMFile) {
  *pMFile = *pOMFile;
  TSDB_FILE_SET_CLOSED(pMFile);
}

int tsdbEncodeSMFile(void **buf, SMFile *pMFile) {
  int tlen = 0;

  tlen += tsdbEncodeMFInfo(buf, &(pMFile->info));
  tlen += tfsEncodeFile(buf, &(pMFile->f));

  return tlen;
}

void *tsdbDecodeSMFile(void *buf, SMFile *pMFile) {
  buf = tsdbDecodeMFInfo(buf, &(pMFile->info));
  buf = tfsDecodeFile(buf, &(pMFile->f));
  TSDB_FILE_SET_CLOSED(pMFile);

  return buf;
}

int tsdbEncodeSMFileEx(void **buf, SMFile *pMFile) {
  int tlen = 0;

  tlen += tsdbEncodeMFInfo(buf, &(pMFile->info));
  tlen += taosEncodeString(buf, TSDB_FILE_FULL_NAME(pMFile));

  return tlen;
}

void *tsdbDecodeSMFileEx(void *buf, SMFile *pMFile) {
  char *aname;
  buf = tsdbDecodeMFInfo(buf, &(pMFile->info));
  buf = taosDecodeString(buf, &aname);
  strncpy(TSDB_FILE_FULL_NAME(pMFile), aname, TSDB_FILENAME_LEN);
  TSDB_FILE_SET_CLOSED(pMFile);

  tfree(aname);

  return buf;
}

int tsdbApplyMFileChange(SMFile *from, SMFile *to) {
  if (from == NULL && to == NULL) return 0;

  if (from != NULL) {
    if (to == NULL) {
      return tsdbRemoveMFile(from);
    } else {
      if (tfsIsSameFile(TSDB_FILE_F(from), TSDB_FILE_F(to))) {
        if (from->info.size > to->info.size) {
          tsdbRollBackMFile(to);
        }
      } else {
        return tsdbRemoveMFile(from);
      }
    }
  }

  return 0;
}

int tsdbCreateMFile(SMFile *pMFile, bool updateHeader) {
  ASSERT(pMFile->info.size == 0 && pMFile->info.magic == TSDB_FILE_INIT_MAGIC);

  pMFile->fd = open(TSDB_FILE_FULL_NAME(pMFile), O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, 0755);
  if (pMFile->fd < 0) {
    if (errno == ENOENT) {
      // Try to create directory recursively
      char *s = strdup(TFILE_REL_NAME(&(pMFile->f)));
      if (tfsMkdirRecurAt(dirname(s), TSDB_FILE_LEVEL(pMFile), TSDB_FILE_ID(pMFile)) < 0) {
        tfree(s);
        return -1;
      }
      tfree(s);

      pMFile->fd = open(TSDB_FILE_FULL_NAME(pMFile), O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, 0755);
      if (pMFile->fd < 0) {
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

  pMFile->info.size += TSDB_FILE_HEAD_SIZE;
  if (tsdbUpdateMFileHeader(pMFile) < 0) {
    tsdbCloseMFile(pMFile);
    tsdbRemoveMFile(pMFile);
    return -1;
  }

  return 0;
}

int tsdbUpdateMFileHeader(SMFile *pMFile) {
  char buf[TSDB_FILE_HEAD_SIZE] = "\0";

  if (tsdbSeekMFile(pMFile, 0, SEEK_SET) < 0) {
    return -1;
  }

  void *ptr = buf;
  tsdbEncodeMFInfo(&ptr, TSDB_FILE_INFO(pMFile));

  taosCalcChecksumAppend(0, (uint8_t *)buf, TSDB_FILE_HEAD_SIZE);
  if (tsdbWriteMFile(pMFile, buf, TSDB_FILE_HEAD_SIZE) < 0) {
    return -1;
  }

  return 0;
}

int tsdbLoadMFileHeader(SMFile *pMFile, SMFInfo *pInfo) {
  char buf[TSDB_FILE_HEAD_SIZE] = "\0";

  ASSERT(TSDB_FILE_OPENED(pMFile));

  if (tsdbSeekMFile(pMFile, 0, SEEK_SET) < 0) {
    return -1;
  }

  if (tsdbReadMFile(pMFile, buf, TSDB_FILE_HEAD_SIZE) < 0) {
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buf, TSDB_FILE_HEAD_SIZE)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  tsdbDecodeMFInfo(buf, pInfo);
  return 0;
}

int tsdbScanAndTryFixMFile(STsdbRepo *pRepo) {
  SMFile *    pMFile = pRepo->fs->cstatus->pmf;
  struct stat mfstat;
  SMFile      mf;

  if (pMFile == NULL) {
    // No meta file, no need to scan
    return 0;
  }

  tsdbInitMFileEx(&mf, pMFile);

  if (access(TSDB_FILE_FULL_NAME(pMFile), F_OK) != 0) {
    tsdbError("vgId:%d meta file %s not exit, report to upper layer to fix it", REPO_ID(pRepo),
              TSDB_FILE_FULL_NAME(pMFile));
    pRepo->state |= TSDB_STATE_BAD_META;
    TSDB_FILE_SET_STATE(pMFile, TSDB_FILE_STATE_BAD);
    return 0;
  }

  if (stat(TSDB_FILE_FULL_NAME(&mf), &mfstat) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (pMFile->info.size < mfstat.st_size) {
    if (tsdbOpenMFile(&mf, O_WRONLY) < 0) {
      return -1;
    }

    if (taosFtruncate(mf.fd, mf.info.size) < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      tsdbCloseMFile(&mf);
      return -1;
    }

    if (tsdbUpdateMFileHeader(&mf) < 0) {
      tsdbCloseMFile(&mf);
      return -1;
    }

    tsdbCloseMFile(&mf);
    tsdbInfo("vgId:%d file %s is truncated from %" PRId64 " to %" PRId64, REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pMFile),
             mfstat.st_size, pMFile->info.size);
  } else if (pMFile->info.size > mfstat.st_size) {
    tsdbError("vgId:%d meta file %s has wrong size %" PRId64 " expected %" PRId64 ", report to upper layer to fix it",
              REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pMFile), mfstat.st_size, pMFile->info.size);
    pRepo->state |= TSDB_STATE_BAD_META;
    TSDB_FILE_SET_STATE(pMFile, TSDB_FILE_STATE_BAD);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return 0;
  } else {
    tsdbDebug("vgId:%d meta file %s passes the scan", REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pMFile));
  }

  return 0;
}

int tsdbEncodeMFInfo(void **buf, SMFInfo *pInfo) {
  int tlen = 0;

  tlen += taosEncodeVariantI64(buf, pInfo->size);
  tlen += taosEncodeVariantI64(buf, pInfo->tombSize);
  tlen += taosEncodeVariantI64(buf, pInfo->nRecords);
  tlen += taosEncodeVariantI64(buf, pInfo->nDels);
  tlen += taosEncodeFixedU32(buf, pInfo->magic);

  return tlen;
}

void *tsdbDecodeMFInfo(void *buf, SMFInfo *pInfo) {
  buf = taosDecodeVariantI64(buf, &(pInfo->size));
  buf = taosDecodeVariantI64(buf, &(pInfo->tombSize));
  buf = taosDecodeVariantI64(buf, &(pInfo->nRecords));
  buf = taosDecodeVariantI64(buf, &(pInfo->nDels));
  buf = taosDecodeFixedU32(buf, &(pInfo->magic));

  return buf;
}

static int tsdbRollBackMFile(SMFile *pMFile) {
  SMFile mf;

  tsdbInitMFileEx(&mf, pMFile);

  if (tsdbOpenMFile(&mf, O_WRONLY) < 0) {
    return -1;
  }

  if (taosFtruncate(TSDB_FILE_FD(&mf), pMFile->info.size) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    tsdbCloseMFile(&mf);
    return -1;
  }

  if (tsdbUpdateMFileHeader(&mf) < 0) {
    tsdbCloseMFile(&mf);
    return -1;
  }

  TSDB_FILE_FSYNC(&mf);

  tsdbCloseMFile(&mf);
  return 0;
}

// ============== Operations on SDFile
void tsdbInitDFile(SDFile *pDFile, SDiskID did, int vid, int fid, uint32_t ver, TSDB_FILE_T ftype) {
  char fname[TSDB_FILENAME_LEN];

  TSDB_FILE_SET_STATE(pDFile, TSDB_FILE_STATE_OK);

  TSDB_FILE_SET_CLOSED(pDFile);

  memset(&(pDFile->info), 0, sizeof(pDFile->info));
  pDFile->info.magic = TSDB_FILE_INIT_MAGIC;

  tsdbGetFilename(vid, fid, ver, ftype, fname);
  tfsInitFile(&(pDFile->f), did.level, did.id, fname);
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

void *tsdbDecodeSDFile(void *buf, SDFile *pDFile) {
  buf = tsdbDecodeDFInfo(buf, &(pDFile->info));
  buf = tfsDecodeFile(buf, &(pDFile->f));
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
  char *aname;

  buf = tsdbDecodeDFInfo(buf, &(pDFile->info));
  buf = taosDecodeString(buf, &aname);
  strncpy(TSDB_FILE_FULL_NAME(pDFile), aname, TSDB_FILENAME_LEN);
  TSDB_FILE_SET_CLOSED(pDFile);
  tfree(aname);

  return buf;
}

int tsdbCreateDFile(SDFile *pDFile, bool updateHeader) {
  ASSERT(pDFile->info.size == 0 && pDFile->info.magic == TSDB_FILE_INIT_MAGIC);

  pDFile->fd = open(TSDB_FILE_FULL_NAME(pDFile), O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, 0755);
  if (pDFile->fd < 0) {
    if (errno == ENOENT) {
      // Try to create directory recursively
      char *s = strdup(TFILE_REL_NAME(&(pDFile->f)));
      if (tfsMkdirRecurAt(dirname(s), TSDB_FILE_LEVEL(pDFile), TSDB_FILE_ID(pDFile)) < 0) {
        tfree(s);
        return -1;
      }
      tfree(s);

      pDFile->fd = open(TSDB_FILE_FULL_NAME(pDFile), O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, 0755);
      if (pDFile->fd < 0) {
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
  taosEncodeFixedU32(&ptr, TSDB_FS_VERSION);
  tsdbEncodeDFInfo(&ptr, &(pDFile->info));

  taosCalcChecksumAppend(0, (uint8_t *)buf, TSDB_FILE_HEAD_SIZE);
  if (tsdbWriteDFile(pDFile, buf, TSDB_FILE_HEAD_SIZE) < 0) {
    return -1;
  }

  return 0;
}

int tsdbLoadDFileHeader(SDFile *pDFile, SDFInfo *pInfo) {
  char     buf[TSDB_FILE_HEAD_SIZE] = "\0";
  uint32_t version;

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
  pBuf = taosDecodeFixedU32(pBuf, &version);
  pBuf = tsdbDecodeDFInfo(pBuf, pInfo);
  return 0;
}

static int tsdbScanAndTryFixDFile(STsdbRepo *pRepo, SDFile *pDFile) {
  struct stat dfstat;
  SDFile      df;

  tsdbInitDFileEx(&df, pDFile);

  if (access(TSDB_FILE_FULL_NAME(pDFile), F_OK) != 0) {
    tsdbError("vgId:%d data file %s not exit, report to upper layer to fix it", REPO_ID(pRepo),
              TSDB_FILE_FULL_NAME(pDFile));
    pRepo->state |= TSDB_STATE_BAD_DATA;
    TSDB_FILE_SET_STATE(pDFile, TSDB_FILE_STATE_BAD);
    return 0;
  }

  if (stat(TSDB_FILE_FULL_NAME(&df), &dfstat) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (pDFile->info.size < dfstat.st_size) {
    if (tsdbOpenDFile(&df, O_WRONLY) < 0) {
      return -1;
    }

    if (taosFtruncate(df.fd, df.info.size) < 0) {
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
             dfstat.st_size, pDFile->info.size);
  } else if (pDFile->info.size > dfstat.st_size) {
    tsdbError("vgId:%d data file %s has wrong size %" PRId64 " expected %" PRId64 ", report to upper layer to fix it",
              REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile), dfstat.st_size, pDFile->info.size);
    pRepo->state |= TSDB_STATE_BAD_DATA;
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
        tsdbRemoveDFile(from);
      }
    }
  }

  return 0;
}

static int tsdbRollBackDFile(SDFile *pDFile) {
  SDFile df = *pDFile;

  if (tsdbOpenDFile(&df, O_WRONLY) < 0) {
    return -1;
  }

  if (taosFtruncate(TSDB_FILE_FD(&df), pDFile->info.size) < 0) {
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
void tsdbInitDFileSet(SDFileSet *pSet, SDiskID did, int vid, int fid, uint32_t ver) {
  pSet->fid = fid;
  pSet->state = 0;

  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    SDFile *pDFile = TSDB_DFILE_IN_SET(pSet, ftype);
    tsdbInitDFile(pDFile, did, vid, fid, ver, ftype);
  }
}

void tsdbInitDFileSetEx(SDFileSet *pSet, SDFileSet *pOSet) {
  pSet->fid = pOSet->fid;
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    tsdbInitDFileEx(TSDB_DFILE_IN_SET(pSet, ftype), TSDB_DFILE_IN_SET(pOSet, ftype));
  }
}

int tsdbEncodeDFileSet(void **buf, SDFileSet *pSet) {
  int tlen = 0;

  tlen += taosEncodeFixedI32(buf, pSet->fid);
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    tlen += tsdbEncodeSDFile(buf, TSDB_DFILE_IN_SET(pSet, ftype));
  }

  return tlen;
}

void *tsdbDecodeDFileSet(void *buf, SDFileSet *pSet) {
  int32_t fid;

  buf = taosDecodeFixedI32(buf, &(fid));
  pSet->state = 0;
  pSet->fid = fid;
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    buf = tsdbDecodeSDFile(buf, TSDB_DFILE_IN_SET(pSet, ftype));
  }
  return buf;
}

int tsdbEncodeDFileSetEx(void **buf, SDFileSet *pSet) {
  int tlen = 0;

  tlen += taosEncodeFixedI32(buf, pSet->fid);
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    tlen += tsdbEncodeSDFileEx(buf, TSDB_DFILE_IN_SET(pSet, ftype));
  }

  return tlen;
}

void *tsdbDecodeDFileSetEx(void *buf, SDFileSet *pSet) {
  int32_t fid;

  buf = taosDecodeFixedI32(buf, &(fid));
  pSet->fid = fid;
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

int tsdbCreateDFileSet(SDFileSet *pSet, bool updateHeader) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    if (tsdbCreateDFile(TSDB_DFILE_IN_SET(pSet, ftype), updateHeader) < 0) {
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

int tsdbScanAndTryFixDFileSet(STsdbRepo *pRepo, SDFileSet *pSet) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    if (tsdbScanAndTryFixDFile(pRepo, TSDB_DFILE_IN_SET(pSet, ftype)) < 0) {
      return -1;
    }
  }
  return 0;
}

int tsdbParseDFilename(const char *fname, int *vid, int *fid, TSDB_FILE_T *ftype, uint32_t *version) {
  char *p = NULL;
  *version = 0;
  *ftype = TSDB_FILE_MAX;

  sscanf(fname, "v%df%d.%m[a-z]-ver%" PRIu32, vid, fid, &p, version);
  for (TSDB_FILE_T i = 0; i < TSDB_FILE_MAX; i++) {
    if (strcmp(p, TSDB_FNAME_SUFFIX[i]) == 0) {
      *ftype = i;
      break;
    }
  }

  tfree(p);
  return 0;
}

static void tsdbGetFilename(int vid, int fid, uint32_t ver, TSDB_FILE_T ftype, char *fname) {
  ASSERT(ftype != TSDB_FILE_MAX);

  if (ftype < TSDB_FILE_MAX) {
    if (ver == 0) {
      snprintf(fname, TSDB_FILENAME_LEN, "vnode/vnode%d/tsdb/data/v%df%d.%s", vid, vid, fid, TSDB_FNAME_SUFFIX[ftype]);
    } else {
      snprintf(fname, TSDB_FILENAME_LEN, "vnode/vnode%d/tsdb/data/v%df%d.%s-ver%" PRIu32, vid, vid, fid,
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