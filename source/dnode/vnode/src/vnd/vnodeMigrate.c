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

static int32_t vnodeMigrateTask(void *param) {
  int32_t code = 0;
  int32_t lino = 0;

  SMigrateInfo *pInfo = (SMigrateInfo *)param;
  SVnode       *pVnode = pInfo->pVnode;

  // do Migrate
  code = tsdbDoRetention(pInfo->pVnode->pTsdb, pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = smaDoRetention(pInfo->pVnode->pSma, pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  tsem_post(&pInfo->pVnode->canCommit);
  if (code) {
    vError("vgId:%d, %s failed at line %d since %s, timestamp:%" PRIi64, TD_VID(pVnode), __func__, lino,
           tstrerror(code), pInfo->timestamp);
  } else {
    vDebug("vgId:%d, %s done, timestamp:%" PRIi64, TD_VID(pVnode), __func__, pInfo->timestamp);
  }
  taosMemoryFree(pInfo);
  return code;
}

static int32_t vnodePrepareMigrate(SVnode *pVnode, SMigrateInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  tsem_wait(&pVnode->canCommit);

  pInfo->pVnode = pVnode;

_exit:
  if (code) {
    vError("vgId:%d, %s failed at line %d since %s, timestamp:%" PRIi64, TD_VID(pVnode), __func__, lino,
           tstrerror(code), pInfo->timestamp);
  } else {
    vDebug("vgId:%d, %s done, timestamp:%" PRIi64, TD_VID(pVnode), __func__, pInfo->timestamp);
  }
  return code;
}

int32_t vnodeAsyncMigrate(SVnode *pVnode, void *arg) {
  int32_t code = 0;
  int32_t lino = 0;

  SMigrateInfo *pInfo = taosMemoryCalloc(1, sizeof(*pInfo));
  if (!pInfo) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pInfo->timestamp = ((SVTrimDbReq *)arg)->timestamp;
  pInfo->maxSpeed = ((SVTrimDbReq *)arg)->maxSpeed;

  vnodeAsyncCommit(pVnode);

  code = vnodePrepareMigrate(pVnode, pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

  vnodeScheduleTask(vnodeMigrateTask, pInfo);

_exit:
  if (code) {
    vError("vgId:%d, %s failed at line %d since %s, timestamp:%" PRIi64, TD_VID(pVnode), __func__, lino,
           tstrerror(code), pInfo->timestamp);
    if (pInfo) taosMemoryFree(pInfo);
  } else {
    vInfo("vgId:%d, %s done, timestamp:%" PRIi64, TD_VID(pVnode), __func__, pInfo->timestamp);
  }
  return code;
}

int32_t vnodeSyncMigrate(SVnode *pVnode, void *arg) {
  vnodeAsyncMigrate(pVnode, arg);
  tsem_wait(&pVnode->canCommit);
  tsem_post(&pVnode->canCommit);
  return 0;
}