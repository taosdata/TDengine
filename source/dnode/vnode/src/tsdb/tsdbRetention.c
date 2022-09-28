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

#include "tsdb.h"

enum { RETENTION_NO = 0, RETENTION_EXPIRED = 1, RETENTION_MIGRATE = 2 };

#define MIGRATE_MAX_SPEED (1048576 << 4)  // 16 MB, vnode level
#define MIGRATE_MIN_COST  (5)             // second

static bool    tsdbShouldDoMigrate(STsdb *pTsdb);
static int32_t tsdbShouldDoRetention(STsdb *pTsdb, int64_t now);
static int32_t tsdbProcessRetention(STsdb *pTsdb, int64_t now, int64_t maxSpeed, int32_t retention, int8_t type);

static bool tsdbShouldDoMigrate(STsdb *pTsdb) {
  if (tfsGetLevel(pTsdb->pVnode->pTfs) < 2) {
    return false;
  }

  STsdbKeepCfg *keepCfg = &pTsdb->keepCfg;
  if (keepCfg->keep0 == keepCfg->keep1 && keepCfg->keep1 == keepCfg->keep2) {
    return false;
  }
  return true;
}

static int32_t tsdbShouldDoRetention(STsdb *pTsdb, int64_t now) {
  int32_t retention = RETENTION_NO;
  if (taosArrayGetSize(pTsdb->fs.aDFileSet) == 0) {
    return retention;
  }

  SDFileSet *pSet = (SDFileSet *)taosArrayGet(pTsdb->fs.aDFileSet, 0);
  if (tsdbFidLevel(pSet->fid, &pTsdb->keepCfg, now) < 0) {
    retention |= RETENTION_EXPIRED;
  }

  if (!tsdbShouldDoMigrate(pTsdb)) {
    return retention;
  }

  for (int32_t iSet = 0; iSet < taosArrayGetSize(pTsdb->fs.aDFileSet); ++iSet) {
    pSet = (SDFileSet *)taosArrayGet(pTsdb->fs.aDFileSet, iSet);
    int32_t expLevel = tsdbFidLevel(pSet->fid, &pTsdb->keepCfg, now);

    if (expLevel == pSet->diskId.level) continue;

    if (expLevel > 0) {
      retention |= RETENTION_MIGRATE;
      break;
    }
  }

  return retention;
}

/**
 * @brief process retention
 *
 * @param pTsdb
 * @param now
 * @param maxSpeed
 * @param retention
 * @param type 0 RETENTION_EXPIRED, 1 RETENTION_MIGRATE
 * @return int32_t
 */
static int32_t tsdbProcessRetention(STsdb *pTsdb, int64_t now, int64_t maxSpeed, int32_t retention, int8_t type) {
  int32_t code = 0;
  int32_t nBatch = 0;
  int32_t nLoops = 0;
  int32_t maxFid = 0;
  int64_t fSize = 0;
  int64_t speed = maxSpeed > 0 ? maxSpeed : MIGRATE_MAX_SPEED;
  STsdbFS fs = {0};
  STsdbFS fsLatest = {0};

  if (!(retention & type)) {
    goto _exit;
  }

_retention_loop:
  // reset
  maxFid = INT32_MIN;
  fSize = 0;
  tsdbFSDestroy(&fs);
  tsdbFSDestroy(&fsLatest);

  if (atomic_load_8(&pTsdb->trimHdl.commitInWait) == 1) {
    atomic_store_32(&pTsdb->trimHdl.maxRetentFid, INT32_MIN);
    taosMsleep(50);
  }

  code = tsdbFSCopy(pTsdb, &fs);
  if (code) goto _exit;

  if (type == RETENTION_MIGRATE) {
    int32_t fsSize = taosArrayGetSize(fs.aDFileSet);
    for (int32_t iSet = 0; iSet < fsSize; ++iSet) {
      SDFileSet *pSet = (SDFileSet *)taosArrayGet(fs.aDFileSet, iSet);
      int32_t    expLevel = tsdbFidLevel(pSet->fid, &pTsdb->keepCfg, now);
      SDiskID    did;

      if (pSet->diskId.level == expLevel) continue;

      if (expLevel > 0) {
        ASSERT(pSet->fid > maxFid);
        maxFid = pSet->fid;
        fSize += (pSet->pDataF->size + pSet->pHeadF->size + pSet->pSmaF->size);
        if (fSize / speed > MIGRATE_MIN_COST) {
          tsdbDebug("vgId:%d migrate loop %d with maxFid:%d", TD_VID(pTsdb->pVnode), nBatch, maxFid);
          break;
        }
        for (int32_t iStt = 0; iStt < pSet->nSttF; ++iStt) {
          fSize += pSet->aSttF[iStt]->size;
        }
        if (fSize / speed > MIGRATE_MIN_COST) {
          tsdbDebug("vgId:%d migrate loop %d with maxFid:%d", TD_VID(pTsdb->pVnode), nBatch, maxFid);
          break;
        }
      }
    }
  } else if (type == RETENTION_EXPIRED) {
    for (int32_t iSet = 0; iSet < taosArrayGetSize(fs.aDFileSet); ++iSet) {
      SDFileSet *pSet = (SDFileSet *)taosArrayGet(fs.aDFileSet, iSet);
      int32_t    expLevel = tsdbFidLevel(pSet->fid, &pTsdb->keepCfg, now);
      SDiskID    did;

      if (expLevel < 0) {
        ASSERT(pSet->fid > maxFid);
        if (pSet->fid > maxFid) maxFid = pSet->fid;
        taosMemoryFree(pSet->pHeadF);
        taosMemoryFree(pSet->pDataF);
        taosMemoryFree(pSet->pSmaF);
        for (int32_t iStt = 0; iStt < pSet->nSttF; ++iStt) {
          taosMemoryFree(pSet->aSttF[iStt]);
        }
        taosArrayRemove(fs.aDFileSet, iSet);
        --iSet;
      } else {
        break;
      }
    }
  }

  if (maxFid == INT32_MIN) goto _exit;

_commit_conflict_check:
  while (atomic_load_32(&pTsdb->trimHdl.minCommitFid) <= maxFid) {
    if (++nLoops > 1000) {
      nLoops = 0;
      sched_yield();
    }
  }
  if (atomic_val_compare_exchange_8(&pTsdb->trimHdl.state, 0, 1) == 0) {
    if (atomic_load_32(&pTsdb->trimHdl.minCommitFid) <= maxFid) {
      atomic_store_8(&pTsdb->trimHdl.state, 0);
      goto _commit_conflict_check;
    }
    atomic_store_32(&pTsdb->trimHdl.maxRetentFid, maxFid);
    atomic_store_8(&pTsdb->trimHdl.state, 0);
  } else {
    goto _commit_conflict_check;
  }

  // migrate
  if (type == RETENTION_MIGRATE) {
    for (int32_t iSet = 0; iSet < taosArrayGetSize(fs.aDFileSet); ++iSet) {
      SDFileSet *pSet = (SDFileSet *)taosArrayGet(fs.aDFileSet, iSet);
      int32_t    expLevel = tsdbFidLevel(pSet->fid, &pTsdb->keepCfg, now);
      SDiskID    did;

      if (pSet->fid > maxFid) break;

      tsdbDebug("vgId:%d migrate loop %d with maxFid:%d, fid:%d, did:%d, level:%d, expLevel:%d", TD_VID(pTsdb->pVnode),
                nBatch, maxFid, pSet->fid, pSet->diskId.id, pSet->diskId.level, expLevel);

      if (expLevel < 0) {
        taosMemoryFree(pSet->pHeadF);
        taosMemoryFree(pSet->pDataF);
        taosMemoryFree(pSet->pSmaF);
        for (int32_t iStt = 0; iStt < pSet->nSttF; ++iStt) {
          taosMemoryFree(pSet->aSttF[iStt]);
        }
        taosArrayRemove(fs.aDFileSet, iSet);
        --iSet;
      } else {
        if (expLevel == pSet->diskId.level) continue;

        if (tfsAllocDisk(pTsdb->pVnode->pTfs, expLevel, &did) < 0) {
          code = terrno;
          goto _exit;
        }

        if (did.level == pSet->diskId.level) continue;

        // copy file to new disk
        SDFileSet fSet = *pSet;
        fSet.diskId = did;

        code = tsdbDFileSetCopy(pTsdb, pSet, &fSet, maxSpeed);
        if (code) goto _exit;

        code = tsdbFSUpsertFSet(&fs, &fSet);
        if (code) goto _exit;
      }
    }
  }

_merge_fs:
  taosThreadRwlockWrlock(&pTsdb->rwLock);

  // 1) prepare fs, merge tsdbFSNew and pTsdb->fs if needed
  STsdbFS *pTsdbFS = &fs;
  ASSERT(fs.version <= pTsdb->fs.version);

  if (fs.version < pTsdb->fs.version) {
    if ((code = tsdbFSCopy(pTsdb, &fsLatest))) {
      taosThreadRwlockUnlock(&pTsdb->rwLock);
      goto _exit;
    }

    if ((code = tsdbFSUpdDel(pTsdb, &fsLatest, &fs, maxFid))) {
      taosThreadRwlockUnlock(&pTsdb->rwLock);
      goto _exit;
    }
    pTsdbFS = &fsLatest;
  }

  // 2) save CURRENT
  if ((code = tsdbFSCommit1(pTsdb, pTsdbFS))) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    goto _exit;
  }

  // 3) apply the tsdbFS to pTsdb->fs
  if ((code = tsdbFSCommit2(pTsdb, pTsdbFS))) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    goto _exit;
  }
  taosThreadRwlockUnlock(&pTsdb->rwLock);

  if (type == RETENTION_MIGRATE) {
    ++nBatch;
    goto _retention_loop;
  }

_exit:
  tsdbFSDestroy(&fs);
  tsdbFSDestroy(&fsLatest);
  if (code != 0) {
    tsdbError("vgId:%d, tsdb do retention %" PRIi8 " failed since %s", TD_VID(pTsdb->pVnode), type, tstrerror(code));
    ASSERT(0);
  }
  return code;
}

/**
 * @brief Data migration between multi-tier storage, including remove expired data.
 *  1) firstly, remove expired DFileSet;
 *  2) partition the tsdbFS by the expLevel and the estimated cost(e.g. 5s) to copy, and migrate
 * DFileSet groups between multi-tier storage;
 *  3) update the tsdbFS and CURRENT in the same transaction;
 *  4) finish the migration
 * @param pTsdb
 * @param now
 * @param maxSpeed
 * @return int32_t
 */
int32_t tsdbDoRetention(STsdb *pTsdb, int64_t now, int64_t maxSpeed) {
  int32_t code = 0;
  int32_t retention = RETENTION_NO;

  retention = tsdbShouldDoRetention(pTsdb, now);
  if (retention == RETENTION_NO) {
    goto _exit;
  }

  // step 1: process expire
  code = tsdbProcessRetention(pTsdb, now, maxSpeed, retention, RETENTION_EXPIRED);
  if (code < 0) goto _exit;

  // step 2: process multi-tier migration
  code = tsdbProcessRetention(pTsdb, now, maxSpeed, retention, RETENTION_MIGRATE);
  if (code < 0) goto _exit;

_exit:
  pTsdb->trimHdl.maxRetentFid = INT32_MIN;
  if (code != 0) {
    tsdbError("vgId:%d, tsdb do retention %d failed since %s, time:%" PRIi64 ", max speed:%" PRIi64,
              TD_VID(pTsdb->pVnode), retention, tstrerror(code), now, maxSpeed);
    ASSERT(0);
    // tsdbFSRollback(pTsdb->pFS);
  } else {
    tsdbInfo("vgId:%d, tsdb do retention %d succeed, time:%" PRIi64 ", max speed:%" PRIi64, TD_VID(pTsdb->pVnode),
             retention, now, maxSpeed);
  }
  return code;
}