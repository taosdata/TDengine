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

#include "vnd.h"

#if 0
static int32_t vnodeMergeTask(void *param) {
  int32_t code = 0;
  int32_t lino = 0;

  SMergeInfo *pInfo = (SMergeInfo *)param;
  SVnode       *pVnode = pInfo->taskInfo.pVnode;

  code = tsdbMerge(pVnode->pTsdb, pInfo, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  char dir[TSDB_FILENAME_LEN] = {0};
  if (pVnode->pTfs) {
    snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path);
  } else {
    snprintf(dir, TSDB_FILENAME_LEN, "%s", pVnode->path);
  }
  vnodeCommitInfo(dir);

_exit:
  taosMemoryFree(pInfo);
  return code;
}
#endif

#if 0
static int32_t vnodePrepareMerge(SVnode *pVnode, SMergeInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  pInfo->taskInfo.type = VND_TASK_MERGE;
  pInfo->taskInfo.pVnode = pVnode;
  pInfo->flag = 0;
  pInfo->commitID = atomic_add_fetch_64(&pVnode->state.commitID, 1);

#if 0
  char       dir[TSDB_FILENAME_LEN] = {0};
  SVnodeInfo info = {0};

  if (pVnode->pTfs) {
    snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path);
  } else {
    snprintf(dir, TSDB_FILENAME_LEN, "%s", pVnode->path);
  }

  vnodeLoadInfo(dir, &info);
  info.state.commitID = pInfo->commitID;
  vnodeSaveInfo(dir, &info);
#endif
_exit:
  if (code) {
    vError("vgId:%d %s failed at line %d since %s, commit ID:%" PRId64, TD_VID(pVnode), __func__, lino, tstrerror(code),
           pVnode->state.commitID);
  } else {
    vDebug("vgId:%d %s done, commit ID:%" PRId64, TD_VID(pVnode), __func__, pVnode->state.commitID);
  }
  return code;
}
#endif