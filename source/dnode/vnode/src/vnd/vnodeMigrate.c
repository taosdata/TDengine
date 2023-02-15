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

#define MIGRATE_DEFAULT_SPEED (5 << 20)  // for single vnode
#define MIGRATE_CHECK_PERIOD  (3000)     // ms

typedef struct {
  int8_t    inited;
  int8_t    lock;
  SHashObj *pHash;  // key: dbName, val: item
} SMigrateHandle;

static SMigrateHandle vndMigrateHdl = {0};

typedef struct {
  int64_t maxSpeed;   // bytes/s
  int64_t lastSpeed;  // bytes/s
  int64_t lastCheck;  // ms
  int32_t count;
  int8_t  lock;
} SMigrateItem;

int64_t vnodeGetMigrateSpeed(SVnode *pVnode, int32_t cost) {
  const char   *dbFName = pVnode->config.dbname;
  SMigrateItem *pItem = NULL;

  pItem = taosHashGet(vndMigrateHdl.pHash, dbFName, strlen(dbFName));
  if (!pItem || !(pItem = *(SMigrateItem **)pItem)) {
    return 0;
  }

  if (pItem->maxSpeed <= 0) {
    return 0;
  }

  int64_t now = taosGetTimestampMs();
  if ((now - atomic_load_64(&pItem->lastCheck)) > MIGRATE_CHECK_PERIOD) {
    int32_t count = atomic_load_32(&pItem->count);
    if (0 == atomic_val_compare_exchange_8(&pItem->lock, 0, 1)) {
      atomic_store_32(&pItem->count, 0);
      ++count;
      if (count <= 0) {
        count = 1;
      } else if (count > 1024) {
        count = 1024;
      }
      if (pItem->lastCheck != 0) {
        pItem->lastSpeed = pItem->maxSpeed / ((int64_t)ceil((double)(count) / MIGRATE_CHECK_PERIOD));
      } else {
        pItem->lastSpeed = MIGRATE_DEFAULT_SPEED;
      }
      if (pItem->lastSpeed <= 0) {
        pItem->lastSpeed = MIGRATE_DEFAULT_SPEED;
      } else if (pItem->lastSpeed > pItem->maxSpeed) {
        pItem->lastSpeed = pItem->maxSpeed;
      }

      atomic_store_64(&pItem->lastCheck, now);
      atomic_store_8(&pItem->lock, 0);
    } else {
      atomic_fetch_and_32(&pItem->count, 1);
    }
  } else {
    atomic_fetch_and_32(&pItem->count, 1);
  }

  return pItem->lastSpeed;
}

static int32_t vnodeMigrateUpsert(SVnode *pVnode, int64_t maxSpeed, bool *isDup) {
  int32_t       code = 0;
  const char   *dbFName = pVnode->config.dbname;
  SMigrateItem *pItem = NULL;
  pItem = taosHashGet(vndMigrateHdl.pHash, dbFName, strlen(dbFName));
  if (!pItem) {
    if (0 == atomic_val_compare_exchange_8(&vndMigrateHdl.lock, 0, 1)) {
      pItem = taosMemoryCalloc(1, sizeof(SMigrateItem));
      if (pItem) {
        pItem->lock = 0;
        pItem->count = 0;
        pItem->lastCheck = 0;
        pItem->lastSpeed = MIGRATE_DEFAULT_SPEED;
        pItem->maxSpeed = maxSpeed;
        taosHashPut(vndMigrateHdl.pHash, dbFName, strlen(dbFName), &pItem, POINTER_BYTES);
      }
      atomic_store_8(&vndMigrateHdl.lock, 0);
    }
    if (isDup) *isDup = false;
  } else if ((pItem = *(SMigrateItem **)pItem)) {
    vInfo("vgId:%d, %s update maxSpeed from %" PRIi64 " to %" PRIi64, TD_VID(pVnode), __func__, pItem->maxSpeed,
          maxSpeed);
    pItem->maxSpeed = maxSpeed;
    if (isDup) *isDup = true;
  }
  return code;
}

static void vnodeMigrateHashFreeNode(void *data) {
  SMigrateItem *pItem = NULL;

  if (data && (pItem = *(SMigrateItem **)data)) {
    taosMemoryFree(pItem);
  }
}

int32_t vnodeMigrateInit() {
  int32_t code = 0;
  int32_t nLoops = 0;

  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(&vndMigrateHdl.inited, 0, 2);
    if (old != 2) break;
    if (++nLoops > 1000) {
      sched_yield();
      nLoops = 0;
    }
  }

  if (old == 0) {
    vndMigrateHdl.pHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (!vndMigrateHdl.pHash) {
      atomic_store_8(&vndMigrateHdl.inited, 0);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    taosHashSetFreeFp(vndMigrateHdl.pHash, vnodeMigrateHashFreeNode);
    atomic_store_8(&vndMigrateHdl.inited, 1);
  }

_exit:
  return code;
}

void vnodeMigrateCleanUp() {
  int8_t  old;
  int32_t nLoops = 0;
  while (1) {
    old = atomic_val_compare_exchange_8(&vndMigrateHdl.inited, 1, 2);
    if (old != 2) break;
    if (++nLoops > 1000) {
      sched_yield();
      nLoops = 0;
    }
  }

  if (old == 1) {
    taosHashCleanup(vndMigrateHdl.pHash);
    vndMigrateHdl.pHash = NULL;
    atomic_store_8(&vndMigrateHdl.inited, 0);
  }
}

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

  // end Migrate
  //   char dir[TSDB_FILENAME_LEN] = {0};
  //   if (pVnode->pTfs) {
  //     snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path);
  //   } else {
  //     snprintf(dir, TSDB_FILENAME_LEN, "%s", pVnode->path);
  //   }
  //   vnodeCommitInfo(dir);

_exit:
  taosHashRemove(vndMigrateHdl.pHash, pVnode->config.dbname, strlen(pVnode->config.dbname));
  tsem_post(&pInfo->pVnode->canCommit);
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
    vError("vgId:%d, %s failed at line %d since %s, commit ID:%" PRId64, TD_VID(pVnode), __func__, lino,
           tstrerror(code), pVnode->state.commitID);
  } else {
    vDebug("vgId:%d, %s done, commit ID:%" PRId64, TD_VID(pVnode), __func__, pVnode->state.commitID);
  }
  return code;
}

int32_t vnodeAsyncMigrate(SVnode *pVnode, void *arg) {
  int32_t code = 0;
  int32_t lino = 0;

  SMigrateInfo *pInfo = taosMemoryCalloc(1, sizeof(*pInfo));
  if (pInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pInfo->timestamp = ((SVTrimDbReq *)arg)->timestamp;
  pInfo->maxSpeed = ((SVTrimDbReq *)arg)->maxSpeed;

  // vnodeAsyncCommit(pVnode);

  bool isDup = false;
  vnodeMigrateUpsert(pVnode, pInfo->maxSpeed, &isDup);

  if (isDup) {
    code = TSDB_CODE_VND_TASK_ALREADY_EXIST;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = vnodePrepareMigrate(pVnode, pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

  vnodeScheduleTask(vnodeMigrateTask, pInfo);

_exit:
  if (code) {
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
    if (pInfo) taosMemoryFree(pInfo);
  } else {
    vInfo("vgId:%d, %s done", TD_VID(pVnode), __func__);
  }
  return code;
}

int32_t vnodeSyncMigrate(SVnode *pVnode, void *arg) {
  vnodeAsyncMigrate(pVnode, arg);
  tsem_wait(&pVnode->canCommit);
  tsem_post(&pVnode->canCommit);
  return 0;
}