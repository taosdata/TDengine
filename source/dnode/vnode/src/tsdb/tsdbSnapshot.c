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

struct STsdbSnapshotReader {
  STsdb*     pTsdb;
  int64_t    sver;
  int64_t    ever;
  SFSIter    iter;
  SDFileSet* pSet;
  SReadH     rh;
};

int32_t tsdbSnapshotReaderOpen(STsdb* pTsdb, STsdbSnapshotReader** ppReader, int64_t sver, int64_t ever) {
  int32_t              code = 0;
  STsdbSnapshotReader* pTsdbReader = NULL;

  pTsdbReader = (STsdbSnapshotReader*)taosMemoryCalloc(1, sizeof(*pTsdbReader));
  if (pTsdbReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pTsdbReader->pTsdb = pTsdb;
  pTsdbReader->sver = sver;
  pTsdbReader->ever = ever;
  tsdbFSIterInit(&pTsdbReader->iter, pTsdb->fs, TSDB_FS_ITER_FORWARD);
  tsdbInitReadH(&pTsdbReader->rh, pTsdb);

  *ppReader = pTsdbReader;
  return code;

_err:
  *ppReader = NULL;
  return code;
}

int32_t tsdbSnapshotReaderClose(STsdbSnapshotReader* pReader) {
  if (pReader) {
    tsdbDestroyReadH(&pReader->rh);
    taosMemoryFree(pReader);
  }
  return 0;
}

int32_t tsdbSnapshotRead(STsdbSnapshotReader* pReader, void** ppData, uint32_t* nData) {
  // TODO
  return 0;
}

int32_t tsdbRollback(STsdb* pTsdb, int64_t ver) {
  // TODO
  return 0;
}
