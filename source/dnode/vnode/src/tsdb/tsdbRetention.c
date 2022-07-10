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

int32_t tsdbDoRetention(STsdb *pTsdb, int64_t now) {
  int32_t code = 0;

  // begin
  code = tsdbFSBegin(pTsdb->fs);
  if (code) goto _err;

  // do retention
  for (int32_t iSet = 0; iSet < taosArrayGetSize(pTsdb->fs->nState->aDFileSet); iSet++) {
    SDFileSet *pDFileSet = (SDFileSet *)taosArrayGet(pTsdb->fs->nState->aDFileSet, iSet);
    int32_t    expLevel = tsdbFidLevel(pDFileSet->fid, &pTsdb->keepCfg, now);
    SDiskID    did;

    // check
    if (expLevel == pDFileSet->fid) continue;

    if (expLevel < 0) {
      tsdbFSStateDeleteDFileSet(pTsdb->fs->nState, pDFileSet->fid);
      iSet--;
      // tsdbInfo("vgId:%d file is out of data, remove it", td);
    } else {
      // alloc
      if (tfsAllocDisk(pTsdb->pVnode->pTfs, expLevel, &did) < 0) {
        code = terrno;
        goto _err;
      }

      if (did.level == pDFileSet->diskId.level) continue;

      ASSERT(did.level > pDFileSet->diskId.level);

      // copy the file to new disk
      SDFileSet nDFileSet = *pDFileSet;
      nDFileSet.diskId = did;

      code = tsdbDFileSetCopy(pTsdb, pDFileSet, &nDFileSet);
      if (code) goto _err;

      code = tsdbFSStateUpsertDFileSet(pTsdb->fs->nState, &nDFileSet);
      if (code) goto _err;
    }
  }

  // commit
  code = tsdbFSCommit(pTsdb->fs);
  if (code) goto _err;

_exit:
  return code;

_err:
  tsdbError("vgId:%d tsdb do retention failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  tsdbFSRollback(pTsdb->fs);
  return code;
}