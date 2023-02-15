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

static bool tsdbShouldDoRetention(STsdb *pTsdb, int64_t now) {
  for (int32_t iSet = 0; iSet < taosArrayGetSize(pTsdb->fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pTsdb->fs.aDFileSet, iSet);
    int32_t    expLevel = tsdbFidLevel(pSet->fid, &pTsdb->keepCfg, now);
    SDiskID    did;

    if (expLevel == pSet->diskId.level) continue;

    if (expLevel < 0) {
      return true;
    } else {
      if (tfsAllocDisk(pTsdb->pVnode->pTfs, expLevel, &did) < 0) {
        return false;
      }

      if (did.level == pSet->diskId.level) continue;

      return true;
    }
  }

  return false;
}

static int32_t tsdbAbortRetention(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFSRollback(pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbDoRetention(STsdb *pTsdb, SMigrateInfo *pInfo) {
  int32_t code = 0;

  if (!tsdbShouldDoRetention(pTsdb, pInfo->timestamp)) {
    tsdbInfo("vgId:%d, tsdb do retention not need", TD_VID(pTsdb->pVnode));
    return code;
  }

  // do retention
  STsdbFS fs = {0};

  code = tsdbFSCopy(pTsdb, &fs);
  if (code) goto _exit;

  for (int32_t iSet = 0; iSet < taosArrayGetSize(fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(fs.aDFileSet, iSet);
    int32_t    expLevel = tsdbFidLevel(pSet->fid, &pTsdb->keepCfg, pInfo->timestamp);
    SDiskID    did;

    if (expLevel < 0) {
      taosMemoryFree(pSet->pHeadF);
      taosMemoryFree(pSet->pDataF);
      taosMemoryFree(pSet->aSttF[0]);
      taosMemoryFree(pSet->pSmaF);
      taosArrayRemove(fs.aDFileSet, iSet);
      iSet--;
    } else {
      if (expLevel == 0) continue;
      if (tfsAllocDisk(pTsdb->pVnode->pTfs, expLevel, &did) < 0) {
        code = terrno;
        goto _exit;
      }

      if (did.level == pSet->diskId.level) continue;

      // copy file to new disk (todo)
      SDFileSet fSet = *pSet;
      fSet.diskId = did;

      code = tsdbDFileSetCopy(pTsdb, pSet, &fSet, vnodeGetMigrateSpeed);
      if (code) goto _exit;

      code = tsdbFSUpsertFSet(&fs, &fSet);
      if (code) goto _exit;
    }
  }

  // do change fs
  code = tsdbFSPrepareCommit(pTsdb, &fs);
  if (code) goto _exit;

  taosThreadRwlockWrlock(&pTsdb->rwLock);

  code = tsdbFSCommit(pTsdb);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    goto _exit;
  }

  taosThreadRwlockUnlock(&pTsdb->rwLock);

  tsdbFSDestroy(&fs);

_exit:
  if (code) {
    tsdbError("vgId:%d, tsdb do retention failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
    tsdbAbortRetention(pTsdb);
  } else {
    tsdbInfo("vgId:%d, tsdb do retention done", TD_VID(pTsdb->pVnode));
  }
  return code;
}