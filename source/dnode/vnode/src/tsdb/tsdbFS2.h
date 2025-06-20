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

#include "tsdbFSet2.h"
#include "vnd.h"

#ifndef _TSDB_FILE_SYSTEM_H
#define _TSDB_FILE_SYSTEM_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  TSDB_FEDIT_COMMIT = 1,  //
  TSDB_FEDIT_MERGE,
  TSDB_FEDIT_COMPACT,
  TSDB_FEDIT_RETENTION,
  TSDB_FEDIT_S3MIGRATE,
} EFEditT;

typedef enum {
  TSDB_FCURRENT = 1,
  TSDB_FCURRENT_C,  // for commit
  TSDB_FCURRENT_M,  // for merge
} EFCurrentT;

/* Exposed APIs */
// open/close
int32_t tsdbOpenFS(STsdb *pTsdb, STFileSystem **fs, int8_t rollback);
void    tsdbCloseFS(STFileSystem **fs);
// snapshot
int32_t tsdbFSCreateCopySnapshot(STFileSystem *fs, TFileSetArray **fsetArr);
void    tsdbFSDestroyCopySnapshot(TFileSetArray **fsetArr);
int32_t tsdbFSCreateRefSnapshot(STFileSystem *fs, TFileSetArray **fsetArr);
int32_t tsdbFSCreateRefSnapshotWithoutLock(STFileSystem *fs, TFileSetArray **fsetArr);
void    tsdbFSDestroyRefSnapshot(TFileSetArray **fsetArr);

int32_t tsdbFSCreateCopyRangedSnapshot(STFileSystem *fs, TFileSetRangeArray *pExclude, TFileSetArray **fsetArr,
                                       TFileOpArray *fopArr);
void    tsdbFSDestroyCopyRangedSnapshot(TFileSetArray **fsetArr);
int32_t tsdbFSCreateRefRangedSnapshot(STFileSystem *fs, int64_t sver, int64_t ever, TFileSetRangeArray *pRanges,
                                      TFileSetRangeArray **fsrArr);
void    tsdbFSDestroyRefRangedSnapshot(TFileSetRangeArray **fsrArr);
//  txn
int64_t tsdbFSAllocEid(STFileSystem *fs);
void    tsdbFSUpdateEid(STFileSystem *fs, int64_t cid);
int32_t tsdbFSEditBegin(STFileSystem *fs, const TFileOpArray *opArray, EFEditT etype);
int32_t tsdbFSEditCommit(STFileSystem *fs);
int32_t tsdbFSEditAbort(STFileSystem *fs);
// other
void tsdbFSGetFSet(STFileSystem *fs, int32_t fid, STFileSet **fset);
void tsdbFSCheckCommit(STsdb *tsdb, int32_t fid);
void tsdbBeginTaskOnFileSet(STsdb *tsdb, int32_t fid, EVATaskT task, STFileSet **fset);
void tsdbFinishTaskOnFileSet(STsdb *tsdb, int32_t fid, EVATaskT task);
// utils
int32_t save_fs(const TFileSetArray *arr, const char *fname);
void    current_fname(STsdb *pTsdb, char *fname, EFCurrentT ftype);

/* Exposed Structs */
struct STFileSystem {
  STsdb        *tsdb;
  tsem_t        canEdit;
  int32_t       fsstate;
  int64_t       neid;
  EFEditT       etype;
  TFileSetArray fSetArr[1];
  TFileSetArray fSetArrTmp[1];
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FILE_SYSTEM_H*/
