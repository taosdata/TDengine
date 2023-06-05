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

#include "tsdbFSet.h"

#ifndef _TSDB_FILE_SYSTEM_H
#define _TSDB_FILE_SYSTEM_H

#ifdef __cplusplus
extern "C" {
#endif

/* Exposed Handle */
typedef struct STFileSystem STFileSystem;
typedef TARRAY2(STFileSet *) TFileSetArray;

typedef enum {
  TSDB_FEDIT_COMMIT = 1,  //
  TSDB_FEDIT_MERGE
} EFEditT;

/* Exposed APIs */
// open/close
int32_t tsdbOpenFS(STsdb *pTsdb, STFileSystem **fs, int8_t rollback);
int32_t tsdbCloseFS(STFileSystem **fs);
// snapshot
int32_t tsdbFSCreateCopySnapshot(STFileSystem *fs, TFileSetArray **fsetArr);
int32_t tsdbFSDestroyCopySnapshot(TFileSetArray **fsetArr);
// txn
int64_t tsdbFSAllocEid(STFileSystem *fs);
int32_t tsdbFSEditBegin(STFileSystem *fs, const TFileOpArray *opArray, EFEditT etype);
int32_t tsdbFSEditCommit(STFileSystem *fs);
int32_t tsdbFSEditAbort(STFileSystem *fs);
// other
int32_t tsdbFSGetFSet(STFileSystem *fs, int32_t fid, STFileSet **fset);

/* Exposed Structs */
struct STFileSystem {
  STsdb        *tsdb;
  tsem_t        canEdit;
  int32_t       state;
  int64_t       neid;
  EFEditT       etype;
  bool          mergeTaskOn;
  TFileSetArray fSetArr[1];
  TFileSetArray fSetArrTmp[1];
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FILE_SYSTEM_H*/