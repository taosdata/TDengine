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

#define TD_QTASKINFO_FNAME_PREFIX "main.tdb"

void tdRSmaQTaskInfoGetFileName(int32_t vgId, int64_t suid, int8_t level, int64_t version, char *outputName) {
  tdRSmaGetFileName(vgId, NULL, VNODE_RSMA_DIR, TD_QTASKINFO_FNAME_PREFIX, suid, level, version, outputName);
}

void tdRSmaQTaskInfoGetFullName(int32_t vgId, int64_t suid, int8_t level, int64_t version, const char *path,
                                char *outputName) {
  tdRSmaGetFileName(vgId, path, VNODE_RSMA_DIR, TD_QTASKINFO_FNAME_PREFIX, suid, level, version, outputName);
}

void tdRSmaQTaskInfoGetFullPath(int32_t vgId, int8_t level, const char *path, char *outputName) {
  tdRSmaGetDirName(vgId, path, VNODE_RSMA_DIR, true, outputName);
  int32_t rsmaLen = strlen(outputName);
  snprintf(outputName + rsmaLen, TSDB_FILENAME_LEN - rsmaLen, "%" PRIi8, level);
}

void tdRSmaQTaskInfoGetFullPathEx(int32_t vgId, tb_uid_t suid, int8_t level, const char *path, char *outputName) {
  tdRSmaGetDirName(vgId, path, VNODE_RSMA_DIR, true, outputName);
  int32_t rsmaLen = strlen(outputName);
  snprintf(outputName + rsmaLen, TSDB_FILENAME_LEN - rsmaLen, "%" PRIi64 "%s%" PRIi8, suid, TD_DIRSEP, level);
}

void tdRSmaGetFileName(int32_t vgId, const char *pdname, const char *dname, const char *fname, int64_t suid,
                       int8_t level, int64_t version, char *outputName) {
  if (level >= 0 && suid > 0) {
    if (version >= 0) {
      if (pdname) {
        snprintf(outputName, TSDB_FILENAME_LEN, "%s%svnode%svnode%d%s%s%s%" PRIi64 "%s%" PRIi8 "%s%s.%" PRIi64, pdname,
                 TD_DIRSEP, TD_DIRSEP, vgId, TD_DIRSEP, dname, TD_DIRSEP, suid, TD_DIRSEP, level, TD_DIRSEP, fname,
                 version);
      } else {
        snprintf(outputName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s%s%" PRIi64 "%s%" PRIi8 "%s%s.%" PRIi64, TD_DIRSEP,
                 vgId, TD_DIRSEP, dname, TD_DIRSEP, suid, TD_DIRSEP, level, TD_DIRSEP, fname, version);
      }
    } else {
      if (pdname) {
        snprintf(outputName, TSDB_FILENAME_LEN, "%s%svnode%svnode%d%s%s%s%" PRIi64 "%s%" PRIi8 "%s%s", pdname,
                 TD_DIRSEP, TD_DIRSEP, vgId, TD_DIRSEP, dname, TD_DIRSEP, suid, TD_DIRSEP, level, TD_DIRSEP, fname);
      } else {
        snprintf(outputName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s%s%" PRIi64 "%s%" PRIi8 "%s%s", TD_DIRSEP, vgId,
                 TD_DIRSEP, dname, TD_DIRSEP, suid, TD_DIRSEP, level, TD_DIRSEP, fname);
      }
    }
  } else {
    if (version >= 0) {
      if (pdname) {
        snprintf(outputName, TSDB_FILENAME_LEN, "%s%svnode%svnode%d%s%s%sv%d%s%" PRIi64, pdname, TD_DIRSEP, TD_DIRSEP,
                 vgId, TD_DIRSEP, dname, TD_DIRSEP, vgId, fname, version);
      } else {
        snprintf(outputName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s%sv%d%s%" PRIi64, TD_DIRSEP, vgId, TD_DIRSEP, dname,
                 TD_DIRSEP, vgId, fname, version);
      }
    } else {
      if (pdname) {
        snprintf(outputName, TSDB_FILENAME_LEN, "%s%svnode%svnode%d%s%s%sv%d%s", pdname, TD_DIRSEP, TD_DIRSEP, vgId,
                 TD_DIRSEP, dname, TD_DIRSEP, vgId, fname);
      } else {
        snprintf(outputName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s%sv%d%s", TD_DIRSEP, vgId, TD_DIRSEP, dname,
                 TD_DIRSEP, vgId, fname);
      }
    }
  }
}

void tdRSmaGetDirName(int32_t vgId, const char *pdname, const char *dname, bool endWithSep, char *outputName) {
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
    smaTrace("rsma acquire ref for rsetId:%d refId:%" PRIi64 " success", rsetId, refId);
  }
  return pResult;
}

int32_t tdReleaseSmaRef(int32_t rsetId, int64_t refId) {
  if (taosReleaseRef(rsetId, refId) < 0) {
    smaWarn("rsma release ref for rsetId:%d refId:%" PRIi64 " failed since %s", rsetId, refId, terrstr());
    return TSDB_CODE_FAILED;
  }
  smaTrace("rsma release ref for rsetId:%d refId:%" PRIi64 " success", rsetId, refId);

  return TSDB_CODE_SUCCESS;
}