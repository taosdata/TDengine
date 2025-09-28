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
#if 0
#include "sma.h"
#include "vnd.h"

void tdRSmaQTaskInfoGetFullPath(SVnode *pVnode, tb_uid_t suid, int8_t level, STfs *pTfs, char *outputName) {
  tdRSmaGetDirName(pVnode, pTfs, true, outputName);
  int32_t rsmaLen = strlen(outputName);
  (void)snprintf(outputName + rsmaLen, TSDB_FILENAME_LEN - rsmaLen, "%" PRIi8 "%s%" PRIi64, level, TD_DIRSEP, suid);
}

void tdRSmaGetDirName(SVnode *pVnode, STfs *pTfs, bool endWithSep, char *outputName) {
  int32_t offset = 0;

  // vnode
  vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pTfs, outputName, TSDB_FILENAME_LEN);
  offset = strlen(outputName);

  // rsma
  (void)snprintf(outputName + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s%s", TD_DIRSEP, VNODE_RSMA_DIR,
                 (endWithSep ? TD_DIRSEP : ""));
}

// smaXXXUtil ================
void *tdAcquireSmaRef(int32_t rsetId, int64_t refId) { return taosAcquireRef(rsetId, refId); }

int32_t tdReleaseSmaRef(int32_t rsetId, int64_t refId) {
  if (taosReleaseRef(rsetId, refId) < 0) {
    int32_t code = terrno;
    smaWarn("rsma release ref for rsetId:%d refId:%" PRIi64 " failed since %s", rsetId, refId, tstrerror(code));
    TAOS_RETURN(code);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}
#endif