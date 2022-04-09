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

#define _DEFAULT_SOURCE
#include "mndSync.h"
#include "mndTrans.h"

static int32_t mndInitWal(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  char path[PATH_MAX] = {0};
  snprintf(path, sizeof(path), "%s%swal", pMnode->path, TD_DIRSEP);
  SWalCfg cfg = {.vgId = 1,
                 .fsyncPeriod = 0,
                 .rollPeriod = -1,
                 .segSize = -1,
                 .retentionPeriod = -1,
                 .retentionSize = -1,
                 .level = TAOS_WAL_FSYNC};
  pMgmt->pWal = walOpen(path, &cfg);
  if (pMgmt->pWal == NULL) return -1;

  return 0;
}

static void mndCloseWal(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  if (pMgmt->pWal != NULL) {
    walClose(pMgmt->pWal);
    pMgmt->pWal = NULL;
  }
}

static int32_t mndRestoreWal(SMnode *pMnode) {
  SWal   *pWal = pMnode->syncMgmt.pWal;
  SSdb   *pSdb = pMnode->pSdb;
  int64_t lastSdbVer = sdbUpdateVer(pSdb, 0);
  int32_t code = -1;

  SWalReadHandle *pHandle = walOpenReadHandle(pWal);
  if (pHandle == NULL) return -1;

  int64_t first = walGetFirstVer(pWal);
  int64_t last = walGetLastVer(pWal);
  mDebug("start to restore sdb wal, sdb ver:%" PRId64 ", wal first:%" PRId64 " last:%" PRId64, lastSdbVer, first, last);

  first = TMAX(lastSdbVer + 1, first);
  for (int64_t ver = first; ver >= 0 && ver <= last; ++ver) {
    if (walReadWithHandle(pHandle, ver) < 0) {
      mError("failed to read by wal handle since %s, ver:%" PRId64, terrstr(), ver);
      goto WAL_RESTORE_OVER;
    }

    SWalHead *pHead = pHandle->pHead;
    int64_t   sdbVer = sdbUpdateVer(pSdb, 0);
    if (sdbVer + 1 != ver) {
      terrno = TSDB_CODE_SDB_INVALID_WAl_VER;
      mError("failed to read wal from sdb, sdbVer:%" PRId64 " inconsistent with ver:%" PRId64, sdbVer, ver);
      goto WAL_RESTORE_OVER;
    }

    mTrace("wal:%" PRId64 ", will be restored, content:%p", ver, pHead->head.body);
    if (sdbWriteNotFree(pSdb, (void *)pHead->head.body) < 0) {
      mError("failed to read wal from sdb since %s, ver:%" PRId64, terrstr(), ver);
      goto WAL_RESTORE_OVER;
    }

    sdbUpdateVer(pSdb, 1);
    mDebug("wal:%" PRId64 ", is restored", ver);
  }

  int64_t sdbVer = sdbUpdateVer(pSdb, 0);
  mDebug("restore sdb wal finished, sdb ver:%" PRId64, sdbVer);

  mndTransPullup(pMnode);
  sdbVer = sdbUpdateVer(pSdb, 0);
  mDebug("pullup trans finished, sdb ver:%" PRId64, sdbVer);

  if (sdbVer != lastSdbVer) {
    mInfo("sdb restored from %" PRId64 " to %" PRId64 ", write file", lastSdbVer, sdbVer);
    if (sdbWriteFile(pSdb) != 0) {
      goto WAL_RESTORE_OVER;
    }

    if (walCommit(pWal, sdbVer) != 0) {
      goto WAL_RESTORE_OVER;
    }

    if (walBeginSnapshot(pWal, sdbVer) < 0) {
      goto WAL_RESTORE_OVER;
    }

    if (walEndSnapshot(pWal) < 0) {
      goto WAL_RESTORE_OVER;
    }
  }

  code = 0;

WAL_RESTORE_OVER:
  walCloseReadHandle(pHandle);
  return code;
}

int32_t mndInitSync(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  tsem_init(&pMgmt->syncSem, 0, 0);

  if (mndInitWal(pMnode) < 0) {
    mError("failed to open wal since %s", terrstr());
    return -1;
  }

  if (mndRestoreWal(pMnode) < 0) {
    mError("failed to restore wal since %s", terrstr());
    return -1;
  }

  if (pMnode->selfId == 1) {
    pMgmt->state = TAOS_SYNC_STATE_LEADER;
  }
  pMgmt->pSyncNode = NULL;
  return 0;
}

void mndCleanupSync(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  tsem_destroy(&pMgmt->syncSem);
  mndCloseWal(pMnode);
}

static int32_t mndSyncApplyCb(struct SSyncFSM *fsm, SyncIndex index, const SSyncBuffer *buf, void *pData) {
  SMnode    *pMnode = pData;
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  pMgmt->errCode = 0;
  tsem_post(&pMgmt->syncSem);

  return 0;
}

int32_t mndSyncPropose(SMnode *pMnode, SSdbRaw *pRaw) {
  SWal *pWal = pMnode->syncMgmt.pWal;
  SSdb *pSdb = pMnode->pSdb;

  int64_t ver = sdbUpdateVer(pSdb, 1);
  if (walWrite(pWal, ver, 1, pRaw, sdbGetRawTotalSize(pRaw)) < 0) {
    sdbUpdateVer(pSdb, -1);
    mError("failed to write raw:%p since %s, ver:%" PRId64, pRaw, terrstr(), ver);
    return -1;
  }

  mTrace("raw:%p, write to wal, ver:%" PRId64, pRaw, ver);
  walCommit(pWal, ver);
  walFsync(pWal, true);

#if 1
  return 0;
#else
  if (pMnode->replica == 1) return 0;

  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  pMgmt->errCode = 0;

  SSyncBuffer buf = {.data = pRaw, .len = sdbGetRawTotalSize(pRaw)};

  bool    isWeak = false;
  int32_t code = syncPropose(pMgmt->pSyncNode, &buf, pMnode, isWeak);

  if (code != 0) return code;

  tsem_wait(&pMgmt->syncSem);
  return pMgmt->errCode;
#endif
}

bool mndIsMaster(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  return pMgmt->state == TAOS_SYNC_STATE_LEADER;
}
