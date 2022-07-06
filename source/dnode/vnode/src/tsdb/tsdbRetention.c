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

    // TODO
  }

  // commit
  code = tsdbFSCommit(pTsdb->fs);
  if (code) goto _err;

_exit:
  return code;

_err:
  tsdbError("vgId:%d tsdb do retention failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}