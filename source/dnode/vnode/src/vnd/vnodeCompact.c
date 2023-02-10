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

extern int32_t tsdbCompact(STsdb *pTsdb, int32_t flag);

int32_t vnodePrepareCommit(SVnode *pVnode, SCommitInfo *pInfo);

static int32_t vnodeCompactImpl(SCommitInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO
  SVnode *pVnode = pInfo->pVnode;

  code = tsdbCompact(pVnode->pTsdb, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    vError("vgId:%d %s failed since %s", TD_VID(pInfo->pVnode), __func__, tstrerror(code));
  } else {
    vDebug("vgId:%d %s done", TD_VID(pInfo->pVnode), __func__);
  }
  return code;
}

static int32_t vnodeCompactTask(void *param) {
  int32_t code = 0;

  SCommitInfo *pInfo = (SCommitInfo *)param;

  // compact
  vnodeCompactImpl(pInfo);

  // end compact
  tsem_post(&pInfo->pVnode->canCommit);

_exit:
  taosMemoryFree(pInfo);
  return code;
}
int32_t vnodeAsyncCompact(SVnode *pVnode) {
  int32_t code = 0;

  // schedule compact task
  SCommitInfo *pInfo = taosMemoryCalloc(1, sizeof(*pInfo));
  if (NULL == pInfo) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  vnodePrepareCommit(pVnode, pInfo);
  vnodeScheduleTask(vnodeCompactTask, pInfo);

_exit:
  if (code) {
    vError("vgId:%d %s failed since %s", TD_VID(pInfo->pVnode), __func__, tstrerror(code));
  }
  return code;
}

int32_t vnodeSyncCompact(SVnode *pVnode) {
  vnodeAsyncCompact(pVnode);
  tsem_wait(&pVnode->canCommit);
  tsem_post(&pVnode->canCommit);
  return 0;
}