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
  STsdb *pTsdb;

  SSttFileReader *pSttFReader;
  SSttFileWriter *pSttFWriter;

  SArray *aFileOp;  // SArray<struct SFileOp>
} SMerger;

static int32_t tsdbFileSystemShouldMerge(STsdb *pTsdb) {
  ASSERTS(0, "TODO: not implemented yet");
  // TODO
  return 0;
}

static int32_t tsdbFileSetShouldMerge(struct SFileSet *pSet) {
  ASSERTS(0, "TODO: not implemented yet");
  // TODO
  return 0;
}

static int32_t tsdbFileSetMerge(struct SFileSet *pFileSet) {
  ASSERTS(0, "TODO: not implemented yet");
  // TODO
  return 0;
}
static int32_t tsdbOpenMerger(STsdb *pTsdb, SMerger *merger) {
  ASSERTS(0, "TODO: not implemented yet");
  // TODO
  return 0;
}

int32_t tsdbMerge(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino;

  if (!tsdbFileSystemShouldMerge(pTsdb)) {
    goto _exit;
  }

  // do merge
  SMerger merger = {0};

  TSDB_CHECK_CODE(                            //
      code = tsdbOpenMerger(pTsdb, &merger),  //
      lino,                                   //
      _exit);

  for (int32_t i = 0; i < taosArrayGetSize(pTsdb->pFS->aFileSet); i++) {
    struct SFileSet *pFileSet = taosArrayGet(pTsdb->pFS->aFileSet, i);
    if (!tsdbFileSetShouldMerge(pFileSet)) {
      continue;
    }

    TSDB_CHECK_CODE(                        //
        code = tsdbFileSetMerge(pFileSet),  //
        lino,                               //
        _exit);
  }

  TSDB_CHECK_CODE(                                                                     //
      code = tsdbFileSystemEditBegin(pTsdb->pFS, merger.aFileOp, TSDB_FS_EDIT_MERGE),  //
      lino,                                                                            //
      _exit);

  TSDB_CHECK_CODE(                                                      //
      code = tsdbFileSystemEditCommit(pTsdb->pFS, TSDB_FS_EDIT_MERGE),  //
      lino,                                                             //
      _exit);

_exit:
  if (code) {
  } else {
  }
  return 0;
}
