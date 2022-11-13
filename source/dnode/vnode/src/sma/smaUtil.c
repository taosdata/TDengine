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

#include "sma.h"

// smaFileUtil ================
#if 0
#define TD_FILE_STATE_OK  0
#define TD_FILE_STATE_BAD 1

#define TD_FILE_INIT_MAGIC 0xFFFFFFFF

static int32_t tdEncodeTFInfo(void **buf, STFInfo *pInfo);
static void   *tdDecodeTFInfo(void *buf, STFInfo *pInfo);

static int32_t tdEncodeTFInfo(void **buf, STFInfo *pInfo) {
  int32_t tlen = 0;

  tlen += taosEncodeFixedU32(buf, pInfo->magic);
  tlen += taosEncodeFixedU32(buf, pInfo->ftype);
  tlen += taosEncodeFixedU32(buf, pInfo->fver);
  tlen += taosEncodeFixedI64(buf, pInfo->fsize);

  return tlen;
}

static void *tdDecodeTFInfo(void *buf, STFInfo *pInfo) {
  buf = taosDecodeFixedU32(buf, &(pInfo->magic));
  buf = taosDecodeFixedU32(buf, &(pInfo->ftype));
  buf = taosDecodeFixedU32(buf, &(pInfo->fver));
  buf = taosDecodeFixedI64(buf, &(pInfo->fsize));

  return buf;
}

int64_t tdWriteTFile(STFile *pTFile, void *buf, int64_t nbyte) {
  ASSERT(TD_TFILE_OPENED(pTFile));

  int64_t nwrite = taosWriteFile(pTFile->pFile, buf, nbyte);
  if (nwrite < nbyte) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return nwrite;
}

int64_t tdSeekTFile(STFile *pTFile, int64_t offset, int whence) {
  ASSERT(TD_TFILE_OPENED(pTFile));

  int64_t loffset = taosLSeekFile(TD_TFILE_PFILE(pTFile), offset, whence);
  if (loffset < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return loffset;
}

int64_t tdGetTFileSize(STFile *pTFile, int64_t *size) {
  ASSERT(TD_TFILE_OPENED(pTFile));
  return taosFStatFile(pTFile->pFile, size, NULL);
}

int64_t tdReadTFile(STFile *pTFile, void *buf, int64_t nbyte) {
  ASSERT(TD_TFILE_OPENED(pTFile));

  int64_t nread = taosReadFile(pTFile->pFile, buf, nbyte);
  if (nread < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return nread;
}

int32_t tdUpdateTFileHeader(STFile *pTFile) {
  char buf[TD_FILE_HEAD_SIZE] = "\0";

  if (tdSeekTFile(pTFile, 0, SEEK_SET) < 0) {
    return -1;
  }

  void *ptr = buf;
  tdEncodeTFInfo(&ptr, &(pTFile->info));

  taosCalcChecksumAppend(0, (uint8_t *)buf, TD_FILE_HEAD_SIZE);
  if (tdWriteTFile(pTFile, buf, TD_FILE_HEAD_SIZE) < 0) {
    return -1;
  }

  return 0;
}

int32_t tdLoadTFileHeader(STFile *pTFile, STFInfo *pInfo) {
  char     buf[TD_FILE_HEAD_SIZE] = "\0";
  uint32_t _version;

  ASSERT(TD_TFILE_OPENED(pTFile));

  if (tdSeekTFile(pTFile, 0, SEEK_SET) < 0) {
    return -1;
  }

  if (tdReadTFile(pTFile, buf, TD_FILE_HEAD_SIZE) < 0) {
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buf, TD_FILE_HEAD_SIZE)) {
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return -1;
  }

  void *pBuf = buf;
  pBuf = tdDecodeTFInfo(pBuf, pInfo);
  return 0;
}

void tdUpdateTFileMagic(STFile *pTFile, void *pCksm) {
  pTFile->info.magic = taosCalcChecksum(pTFile->info.magic, (uint8_t *)(pCksm), sizeof(TSCKSUM));
}

int64_t tdAppendTFile(STFile *pTFile, void *buf, int64_t nbyte, int64_t *offset) {
  ASSERT(TD_TFILE_OPENED(pTFile));

  int64_t toffset;

  if ((toffset = tdSeekTFile(pTFile, 0, SEEK_END)) < 0) {
    return -1;
  }

#if 0
  smaDebug("append to file %s, offset:%" PRIi64 " nbyte:%" PRIi64 " fsize:%" PRIi64, TD_TFILE_FULL_NAME(pTFile),
           toffset, nbyte, toffset + nbyte);
#endif

  ASSERT(pTFile->info.fsize == toffset);

  if (offset) {
    *offset = toffset;
  }

  if (tdWriteTFile(pTFile, buf, nbyte) < 0) {
    return -1;
  }

  pTFile->info.fsize += nbyte;

  return nbyte;
}

int32_t tdOpenTFile(STFile *pTFile, int flags) {
  ASSERT(!TD_TFILE_OPENED(pTFile));

  pTFile->pFile = taosOpenFile(TD_TFILE_FULL_NAME(pTFile), flags);
  if (pTFile->pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

void tdCloseTFile(STFile *pTFile) {
  if (TD_TFILE_OPENED(pTFile)) {
    taosCloseFile(&pTFile->pFile);
    TD_TFILE_SET_CLOSED(pTFile);
  }
}

void tdDestroyTFile(STFile *pTFile) { taosMemoryFreeClear(TD_TFILE_FULL_NAME(pTFile)); }

#endif

void tdGetVndFileName(int32_t vgId, const char *pdname, const char *dname, const char *fname, int64_t version,
                      char *outputName) {
  if (version < 0) {
    if (pdname) {
      snprintf(outputName, TSDB_FILENAME_LEN, "%s%svnode%svnode%d%s%s%sv%d%s", pdname, TD_DIRSEP, TD_DIRSEP, vgId,
               TD_DIRSEP, dname, TD_DIRSEP, vgId, fname);
    } else {
      snprintf(outputName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s%sv%d%s", TD_DIRSEP, vgId, TD_DIRSEP, dname, TD_DIRSEP,
               vgId, fname);
    }
  } else {
    if (pdname) {
      snprintf(outputName, TSDB_FILENAME_LEN, "%s%svnode%svnode%d%s%s%sv%d%s%" PRIi64, pdname, TD_DIRSEP, TD_DIRSEP,
               vgId, TD_DIRSEP, dname, TD_DIRSEP, vgId, fname, version);
    } else {
      snprintf(outputName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s%sv%d%s%" PRIi64, TD_DIRSEP, vgId, TD_DIRSEP, dname,
               TD_DIRSEP, vgId, fname, version);
    }
  }
}

void tdGetVndDirName(int32_t vgId, const char *pdname, const char *dname, bool endWithSep, char *outputName) {
  if (pdname) {
    if (endWithSep) {
      snprintf(outputName, TSDB_FILENAME_LEN, "%s%svnode%svnode%d%s%s%s", pdname, TD_DIRSEP, TD_DIRSEP, vgId, TD_DIRSEP,
               dname, TD_DIRSEP);
    } else {
      snprintf(outputName, TSDB_FILENAME_LEN, "%s%svnode%svnode%d%s%s", pdname, TD_DIRSEP, TD_DIRSEP, vgId, TD_DIRSEP,
               dname);
    }
  } else {
    if (endWithSep) {
      snprintf(outputName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s%s", TD_DIRSEP, vgId, TD_DIRSEP, dname, TD_DIRSEP);
    } else {
      snprintf(outputName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s", TD_DIRSEP, vgId, TD_DIRSEP, dname);
    }
  }
}

#if 0
int32_t tdInitTFile(STFile *pTFile, const char *dname, const char *fname) {
  TD_TFILE_SET_STATE(pTFile, TD_FILE_STATE_OK);
  TD_TFILE_SET_CLOSED(pTFile);

  memset(&(pTFile->info), 0, sizeof(pTFile->info));
  pTFile->info.magic = TD_FILE_INIT_MAGIC;

  char tmpName[TSDB_FILENAME_LEN * 2 + 32] = {0};
  snprintf(tmpName, TSDB_FILENAME_LEN * 2 + 32, "%s%s%s", dname, TD_DIRSEP, fname);
  int32_t tmpNameLen = strlen(tmpName) + 1;
  pTFile->fname = taosMemoryMalloc(tmpNameLen);
  if (!pTFile->fname) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tstrncpy(pTFile->fname, tmpName, tmpNameLen);

  return 0;
}

int32_t tdCreateTFile(STFile *pTFile, bool updateHeader, int8_t fType) {
  ASSERT(pTFile->info.fsize == 0 && pTFile->info.magic == TD_FILE_INIT_MAGIC);
  pTFile->pFile = taosOpenFile(TD_TFILE_FULL_NAME(pTFile), TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pTFile->pFile == NULL) {
    if (errno == ENOENT) {
      // Try to create directory recursively
      char *s = strdup(TD_TFILE_FULL_NAME(pTFile));
      if (taosMulMkDir(taosDirName(s)) != 0) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        taosMemoryFree(s);
        return -1;
      }
      taosMemoryFree(s);
      pTFile->pFile = taosOpenFile(TD_TFILE_FULL_NAME(pTFile), TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
      if (pTFile->pFile == NULL) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
    }
  }

  if (!updateHeader) {
    return 0;
  }

  pTFile->info.fsize += TD_FILE_HEAD_SIZE;
  pTFile->info.fver = 0;

  if (tdUpdateTFileHeader(pTFile) < 0) {
    tdCloseTFile(pTFile);
    tdRemoveTFile(pTFile);
    return -1;
  }

  return 0;
}

int32_t tdRemoveTFile(STFile *pTFile) {
  if (taosRemoveFile(TD_TFILE_FULL_NAME(pTFile)) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  };
  return 0;
}

#endif

// smaXXXUtil ================
void *tdAcquireSmaRef(int32_t rsetId, int64_t refId) {
  void *pResult = taosAcquireRef(rsetId, refId);
  if (!pResult) {
    smaWarn("rsma acquire ref for rsetId:%d refId:%" PRIi64 " failed since %s", rsetId, refId, terrstr());
  } else {
    smaDebug("rsma acquire ref for rsetId:%d refId:%" PRIi64 " success", rsetId, refId);
  }
  return pResult;
}

int32_t tdReleaseSmaRef(int32_t rsetId, int64_t refId) {
  if (taosReleaseRef(rsetId, refId) < 0) {
    smaWarn("rsma release ref for rsetId:%d refId:%" PRIi64 " failed since %s", rsetId, refId, terrstr());
    return TSDB_CODE_FAILED;
  }
  smaDebug("rsma release ref for rsetId:%d refId:%" PRIi64 " success", rsetId, refId);

  return TSDB_CODE_SUCCESS;
}