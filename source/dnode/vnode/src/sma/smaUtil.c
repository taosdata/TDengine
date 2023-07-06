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

void tdRSmaQTaskInfoGetFullPath(int32_t vgId, tb_uid_t suid, int8_t level, const char *path, char *outputName) {
  tdRSmaGetDirName(vgId, path, VNODE_RSMA_DIR, true, outputName);
  int32_t rsmaLen = strlen(outputName);
  snprintf(outputName + rsmaLen, TSDB_FILENAME_LEN - rsmaLen, "%" PRIi8 "%s%" PRIi64, level, TD_DIRSEP, suid);
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
#if 0
    if (endWithSep) {
      snprintf(outputName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s%s", TD_DIRSEP, vgId, TD_DIRSEP, dname, TD_DIRSEP);
    } else {
      snprintf(outputName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s", TD_DIRSEP, vgId, TD_DIRSEP, dname);
    }
#endif
  }
}

// smaXXXUtil ================
void *tdAcquireSmaRef(int32_t rsetId, int64_t refId) { return taosAcquireRef(rsetId, refId); }

int32_t tdReleaseSmaRef(int32_t rsetId, int64_t refId) {
  if (taosReleaseRef(rsetId, refId) < 0) {
    smaWarn("rsma release ref for rsetId:%d refId:%" PRIi64 " failed since %s", rsetId, refId, terrstr());
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}