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

static bool tsdbShouldDoRetentionImpl(STsdb *pTsdb, int64_t now) {
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
bool tsdbShouldDoRetention(STsdb *pTsdb, int64_t now) {
  bool should;
  taosThreadRwlockRdlock(&pTsdb->rwLock);
  should = tsdbShouldDoRetentionImpl(pTsdb, now);
  taosThreadRwlockUnlock(&pTsdb->rwLock);
  return should;
}

int32_t tsdbDoRetention(STsdb *pTsdb, int64_t now) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdbFS fs = {0};

  code = tsdbFSCopy(pTsdb, &fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  for (int32_t iSet = 0; iSet < taosArrayGetSize(fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(fs.aDFileSet, iSet);
    int32_t    expLevel = tsdbFidLevel(pSet->fid, &pTsdb->keepCfg, now);
    SDiskID    did;

    if (expLevel < 0) {
      taosMemoryFree(pSet->pHeadF);
      taosMemoryFree(pSet->pDataF);
      taosMemoryFree(pSet->pSmaF);
      for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
        taosMemoryFree(pSet->aSttF[iStt]);
      }
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
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbFSUpsertFSet(&fs, &fSet);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // do change fs
  code = tsdbFSPrepareCommit(pTsdb, &fs);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  tsdbFSDestroy(&fs);
  return code;
}

static int32_t tsdbCommitRetentionImpl(STsdb *pTsdb) { return tsdbFSCommit(pTsdb); }

int32_t tsdbCommitRetention(STsdb *pTsdb) {
  taosThreadRwlockWrlock(&pTsdb->rwLock);
  tsdbCommitRetentionImpl(pTsdb);
  taosThreadRwlockUnlock(&pTsdb->rwLock);
  tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  return 0;
}