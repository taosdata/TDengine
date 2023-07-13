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
#include "vnd.h"

#define TD_QTASKINFO_FNAME_PREFIX "main.tdb"

void tdRSmaQTaskInfoGetFileName(SVnode *pVnode, int64_t suid, int8_t level, int64_t version, char *outputName) {
  tdRSmaGetFileName(pVnode, NULL, TD_QTASKINFO_FNAME_PREFIX, suid, level, version, outputName);
}

void tdRSmaQTaskInfoGetFullName(SVnode *pVnode, int64_t suid, int8_t level, int64_t version, STfs *pTfs,
                                char *outputName) {
  tdRSmaGetFileName(pVnode, pTfs, TD_QTASKINFO_FNAME_PREFIX, suid, level, version, outputName);
}

void tdRSmaQTaskInfoGetFullPath(SVnode *pVnode, int8_t level, STfs *pTfs, char *outputName) {
  tdRSmaGetDirName(pVnode, pTfs, true, outputName);
  int32_t rsmaLen = strlen(outputName);
  snprintf(outputName + rsmaLen, TSDB_FILENAME_LEN - rsmaLen, "%" PRIi8, level);
}

void tdRSmaQTaskInfoGetFullPathEx(SVnode *pVnode, tb_uid_t suid, int8_t level, STfs *pTfs, char *outputName) {
  tdRSmaGetDirName(pVnode, pTfs, true, outputName);
  int32_t rsmaLen = strlen(outputName);
  snprintf(outputName + rsmaLen, TSDB_FILENAME_LEN - rsmaLen, "%" PRIi8 "%s%" PRIi64, level, TD_DIRSEP, suid);
}

void tdRSmaGetFileName(SVnode *pVnode, STfs *pTfs, const char *fname, int64_t suid, int8_t level, int64_t version,
                       char *outputName) {
  int32_t offset = 0;

  // vnode
  vnodeGetPrimaryDir(pVnode->path, pTfs, outputName, TSDB_FILENAME_LEN);
  offset = strlen(outputName);

  // rsma
  snprintf(outputName + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s", TD_DIRSEP, VNODE_RSMA_DIR);
  offset = strlen(outputName);

  // level & suid || vgid
  if (level >= 0 && suid > 0) {
    snprintf(outputName + offset, TSDB_FILENAME_LEN - offset - 1, "%s%" PRIi8 "%s%" PRIi64 "%s", TD_DIRSEP, level,
             TD_DIRSEP, suid, TD_DIRSEP);
  } else {
    snprintf(outputName + offset, TSDB_FILENAME_LEN - offset - 1, "%sv%d", TD_DIRSEP, TD_VID(pVnode));
  }
  offset = strlen(outputName);

  // fname
  snprintf(outputName + offset, TSDB_FILENAME_LEN - offset - 1, "%s", fname);
  offset = strlen(outputName);

  // version
  if (version >= 0) {
    snprintf(outputName + offset, TSDB_FILENAME_LEN - offset - 1, ".%" PRIi64, version);
  }
}

void tdRSmaGetDirName(SVnode *pVnode, STfs *pTfs, bool endWithSep, char *outputName) {
  int32_t offset = 0;

  // vnode
  vnodeGetPrimaryDir(pVnode->path, pTfs, outputName, TSDB_FILENAME_LEN);
  offset = strlen(outputName);

  // rsma
  snprintf(outputName + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s%s", TD_DIRSEP, VNODE_RSMA_DIR,
           (endWithSep ? TD_DIRSEP : ""));
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
