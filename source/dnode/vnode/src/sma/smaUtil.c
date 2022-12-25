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
  snprintf(outputName + rsmaLen, TSDB_FILENAME_LEN - rsmaLen, "%" PRIi8 "%s%" PRIi64, level, TD_DIRSEP, suid);
}

void tdRSmaGetFileName(int32_t vgId, const char *pdname, const char *dname, const char *fname, int64_t suid,
                       int8_t level, int64_t version, char *outputName) {
  if (level >= 0 && suid > 0) {
    if (version >= 0) {
      if (pdname) {
        snprintf(outputName, TSDB_FILENAME_LEN, "%s%svnode%svnode%d%s%s%s%" PRIi8 "%s%" PRIi64 "%s%s.%" PRIi64, pdname,
                 TD_DIRSEP, TD_DIRSEP, vgId, TD_DIRSEP, dname, TD_DIRSEP, level, TD_DIRSEP, suid, TD_DIRSEP, fname,
                 version);
      } else {
        snprintf(outputName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s%s%" PRIi8 "%s%" PRIi64 "%s%s.%" PRIi64, TD_DIRSEP,
                 vgId, TD_DIRSEP, dname, TD_DIRSEP, level, TD_DIRSEP, suid, TD_DIRSEP, fname, version);
      }
    } else {
      if (pdname) {
        snprintf(outputName, TSDB_FILENAME_LEN, "%s%svnode%svnode%d%s%s%s%" PRIi8 "%s%" PRIi64 "%s%s", pdname,
                 TD_DIRSEP, TD_DIRSEP, vgId, TD_DIRSEP, dname, TD_DIRSEP, level, TD_DIRSEP, suid, TD_DIRSEP, fname);
      } else {
        snprintf(outputName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s%s%" PRIi8 "%s%" PRIi64 "%s%s", TD_DIRSEP, vgId,
                 TD_DIRSEP, dname, TD_DIRSEP, level, TD_DIRSEP, suid, TD_DIRSEP, fname);
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