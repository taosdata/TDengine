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

#include "dev.h"

typedef struct {
  STsdb     *pTsdb;
  STFileSet *pSet;

  SBlockData bData;

  // reader
  SSttFileReader *pSttFReader;
  int32_t         nSttFSegReader;
  SSttFSegReader *aSttFSegReader;

  // writer
  SSttFileWriter  *pSttFWriter;
  SDataFileWriter *pDataFWriter;

  SArray *aFileOp;  // SArray<struct SFileOp>
} SMerger;

static int32_t tsdbFileSystemShouldMerge(STsdb *pTsdb) {
  ASSERTS(0, "TODO: not implemented yet");
  // TODO
  return 0;
}

static int32_t tsdbFileSetShouldMerge(struct STFileSet *pSet) {
  ASSERTS(0, "TODO: not implemented yet");
  // TODO
  return 0;
}

static int32_t tsdbFileSetMerge(struct STFileSet *pFileSet) {
  ASSERTS(0, "TODO: not implemented yet");
  // TODO
  return 0;
}

static int32_t tsdbOpenMerger(STsdb *pTsdb, SMerger *pMerger) {
  pMerger->pTsdb = pTsdb;
  // TODO
  return 0;
}

static int32_t tsdbDestroyMerger(SMerger *pMerger) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t tsdbCloseMerger(SMerger *pMerger) {
  int32_t code = 0;
  int32_t lino;

  STsdb *pTsdb = pMerger->pTsdb;

  code = tsdbFileSystemEditBegin(pTsdb->pFS, pMerger->aFileOp, TSDB_FS_EDIT_MERGE);
  TSDB_CHECK_CODE(code, lino, _exit)

_exit:
  if (code) {
    tsdbFileSystemEditAbort(pTsdb->pFS, TSDB_FS_EDIT_MERGE);
  } else {
    tsdbFileSystemEditCommit(pTsdb->pFS, TSDB_FS_EDIT_MERGE);
  }
  tsdbDestroyMerger(pMerger);
  return code;
}

int32_t tsdbMerge(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino;

  if (!tsdbFileSystemShouldMerge(pTsdb)) {
    goto _exit;
  }

  SMerger pMerger = {0};
  code = tsdbOpenMerger(pTsdb, &pMerger);
  TSDB_CHECK_CODE(code, lino, _exit)

  for (int32_t i = 0; i < taosArrayGetSize(pTsdb->pFS->cstate); i++) {
    struct STFileSet *pFileSet = taosArrayGet(pTsdb->pFS->cstate, i);
    if (!tsdbFileSetShouldMerge(pFileSet)) {
      continue;
    }

    code = tsdbFileSetMerge(pFileSet);
    TSDB_CHECK_CODE(code, lino, _exit)
  }

  code = tsdbCloseMerger(&pMerger);
  TSDB_CHECK_CODE(code, lino, _exit)

_exit:
  if (code) {
  } else {
  }
  return 0;
}
