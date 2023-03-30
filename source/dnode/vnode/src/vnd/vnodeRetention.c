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

typedef struct {
  SVnode    *pVnode;
  int64_t    now;
  int64_t    commitID;
  SVnodeInfo info;
} SRetentionInfo;

extern bool    tsdbShouldDoRetention(STsdb *pTsdb, int64_t now);
extern int32_t tsdbDoRetention(STsdb *pTsdb, int64_t now);
extern int32_t tsdbCommitRetention(STsdb *pTsdb);

static int32_t vnodePrepareRentention(SVnode *pVnode, SRetentionInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  tsem_wait(&pVnode->canCommit);

  pInfo->commitID = ++pVnode->state.commitID;

  char dir[TSDB_FILENAME_LEN] = {0};
  if (pVnode->pTfs) {
    snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path);
  } else {
    snprintf(dir, TSDB_FILENAME_LEN, "%s", pVnode->path);
  }

  if (vnodeLoadInfo(dir, &pInfo->info) < 0) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
    tsem_post(&pVnode->canCommit);
  } else {
    vInfo("vgId:%d %s done", TD_VID(pVnode), __func__);
  }
  return code;
}

static int32_t vnodeRetentionTask(void *param) {
  int32_t code = 0;
  int32_t lino = 0;

  SRetentionInfo *pInfo = (SRetentionInfo *)param;
  SVnode         *pVnode = pInfo->pVnode;
  char            dir[TSDB_FILENAME_LEN] = {0};

  if (pVnode->pTfs) {
    snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path);
  } else {
    snprintf(dir, TSDB_FILENAME_LEN, "%s", pVnode->path);
  }

  // save info
  pInfo->info.state.commitID = pInfo->commitID;

  if (vnodeSaveInfo(dir, &pInfo->info) < 0) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // do job
  code = tsdbDoRetention(pInfo->pVnode->pTsdb, pInfo->now);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = smaDoRetention(pInfo->pVnode->pSma, pInfo->now);
  TSDB_CHECK_CODE(code, lino, _exit);

  // commit info
  vnodeCommitInfo(dir);

  // commit sub-job
  tsdbCommitRetention(pVnode->pTsdb);

_exit:
  if (code) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pInfo->pVnode), __func__, lino, tstrerror(code));
  } else {
    vInfo("vgId:%d %s done", TD_VID(pInfo->pVnode), __func__);
  }
  tsem_post(&pInfo->pVnode->canCommit);
  taosMemoryFree(pInfo);
  return code;
}

int32_t vnodeAsyncRentention(SVnode *pVnode, int64_t now) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!tsdbShouldDoRetention(pVnode->pTsdb, now)) return code;

  SRetentionInfo *pInfo = (SRetentionInfo *)taosMemoryCalloc(1, sizeof(*pInfo));
  if (pInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pInfo->pVnode = pVnode;
  pInfo->now = now;

  code = vnodePrepareRentention(pVnode, pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

  vnodeScheduleTask(vnodeRetentionTask, pInfo);

_exit:
  if (code) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pInfo->pVnode), __func__, lino, tstrerror(code));
    if (pInfo) taosMemoryFree(pInfo);
  } else {
    vInfo("vgId:%d %s done", TD_VID(pInfo->pVnode), __func__);
  }
  return 0;
}