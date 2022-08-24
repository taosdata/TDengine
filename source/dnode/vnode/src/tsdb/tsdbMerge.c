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

typedef struct {
  STsdb  *pTsdb;
  int8_t  maxLast;
  STsdbFS fs;
  struct {
    SDataFReader *pReader;
  } dReader;
  struct {
    SDataFWriter *pWriter;
  } dWriter;
} STsdbMerger;

int32_t tsdbMerge(STsdb *pTsdb) {
  int32_t      code = 0;
  STsdbMerger  merger = {0};
  STsdbMerger *pMerger = &merger;

  pMerger->pTsdb = pTsdb;
  pMerger->maxLast = TSDB_DEFAULT_LAST_FILE;
  code = tsdbFSCopy(pTsdb, &pMerger->fs);
  if (code) goto _err;

  for (int32_t iSet = 0; iSet < taosArrayGetSize(pMerger->fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pMerger->fs.aDFileSet, iSet);
    if (pSet->nLastF < pMerger->maxLast) continue;

    // do merge the file
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb merge failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}