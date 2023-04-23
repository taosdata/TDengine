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

#ifndef _TSDB_FILE_SYSTEM_H
#define _TSDB_FILE_SYSTEM_H

#include "tsdbFSet.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Exposed Handle */
typedef struct STFileSystem STFileSystem;

typedef enum {
  TSDB_FS_EDIT_NONE = 0,
  TSDB_FS_EDIT_COMMIT,
  TSDB_FS_EDIT_MERGE,
  TSDB_FS_EDIT_MAX,
} tsdb_fs_edit_t;

/* Exposed APIs */
// open/close
int32_t tsdbOpenFileSystem(STsdb *pTsdb, STFileSystem **ppFS, int8_t rollback);
int32_t tsdbCloseFileSystem(STFileSystem **ppFS);
// txn
int32_t tsdbFileSystemEditBegin(STFileSystem *pFS, const SArray *aFileOp, tsdb_fs_edit_t etype);
int32_t tsdbFileSystemEditCommit(STFileSystem *pFS, tsdb_fs_edit_t etype);
int32_t tsdbFileSystemEditAbort(STFileSystem *pFS, tsdb_fs_edit_t etype);

/* Exposed Structs */
struct STFileSystem {
  STsdb  *pTsdb;
  int32_t state;
  tsem_t  canEdit;
  int64_t nextEditId;
  SArray *aFileSet;  // SArray<struct SFileSet>
  SArray *nState;    // SArray<struct SFileSet>
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FILE_SYSTEM_H*/