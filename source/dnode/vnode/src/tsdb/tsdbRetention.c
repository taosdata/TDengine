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

static int32_t tsdbDoRetentionImpl(STsdb *pTsdb, int64_t now, int8_t try, int8_t *canDo) {
  int32_t       code = 0;
  STsdbFSState *pState;

  if (try) {
    pState = pTsdb->fs->cState;
    *canDo = 0;
  } else {
    pState = pTsdb->fs->nState;
  }

  for (int32_t iSet = 0; iSet < taosArrayGetSize(pState->aDFileSet); iSet++) {
    SDFileSet *pDFileSet = (SDFileSet *)taosArrayGet(pState->aDFileSet, iSet);
    int32_t    expLevel = tsdbFidLevel(pDFileSet->fid, &pTsdb->keepCfg, now);
    SDiskID    did;

    // check
    if (expLevel == pDFileSet->fid) continue;

    // delete or move
    if (expLevel < 0) {
      if (try) {
        *canDo = 1;
      } else {
        tsdbFSStateDeleteDFileSet(pState, pDFileSet->fid);
        iSet--;
      }
    } else {
      // alloc
      if (tfsAllocDisk(pTsdb->pVnode->pTfs, expLevel, &did) < 0) {
        code = terrno;
        goto _exit;
      }

      if (did.level == pDFileSet->diskId.level) continue;

      if (try) {
        *canDo = 1;
      } else {
        // copy the file to new disk

        SDFileSet nDFileSet = *pDFileSet;
        nDFileSet.diskId = did;

        tfsMkdirRecurAt(pTsdb->pVnode->pTfs, pTsdb->path, did);

        code = tsdbDFileSetCopy(pTsdb, pDFileSet, &nDFileSet);
        if (code) goto _exit;

        code = tsdbFSStateUpsertDFileSet(pState, &nDFileSet);
        if (code) goto _exit;
      }
    }
  }

_exit:
  return code;
}

int32_t tsdbDoRetention(STsdb *pTsdb, int64_t now) {
  int32_t code = 0;
  int8_t  canDo;

  // try
  tsdbDoRetentionImpl(pTsdb, now, 1, &canDo);
  if (!canDo) goto _exit;

  // begin
  code = tsdbFSBegin(pTsdb->fs);
  if (code) goto _err;

  // do retention
  code = tsdbDoRetentionImpl(pTsdb, now, 0, NULL);
  if (code) goto _err;

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