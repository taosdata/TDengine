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

int32_t tsdbDoRetention(STsdb *pTsdb, int64_t now) {
  int32_t code = 0;

  if (!tsdbShouldDoRetention(pTsdb, now)) {
    return code;
  }

  // do retention
  STsdbFS fs = {0};

  code = tsdbFSCopy(pTsdb, &fs);
  if (code) goto _err;

  for (int32_t iSet = 0; iSet < taosArrayGetSize(fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(fs.aDFileSet, iSet);
    int32_t    expLevel = tsdbFidLevel(pSet->fid, &pTsdb->keepCfg, now);
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

      code = tsdbDFileSetCopy(pTsdb, pSet, &fSet);
      if (code) goto _err;

      code = tsdbFSUpsertFSet(&fs, &fSet);
      if (code) goto _err;
    }
  }

  // do change fs
  code = tsdbFSPrepareCommit(pTsdb, &fs);
  if (code) goto _err;

  taosThreadRwlockWrlock(&pTsdb->rwLock);

  code = tsdbFSCommit(pTsdb);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    goto _err;
  }

  taosThreadRwlockUnlock(&pTsdb->rwLock);

  tsdbFSDestroy(&fs);

_exit:
  return code;

_err:
  tsdbError("vgId:%d, tsdb do retention failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  ASSERT(0);
  // tsdbFSRollback(pTsdb->pFS);
  return code;
}