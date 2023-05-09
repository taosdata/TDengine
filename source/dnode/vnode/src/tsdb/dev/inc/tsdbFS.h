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

typedef enum {
  TSDB_FEDIT_COMMIT = 1,  //
  TSDB_FEDIT_MERGE
} EFEditT;

/* Exposed APIs */
// open/close
int32_t tsdbOpenFS(STsdb *pTsdb, STFileSystem **ppFS, int8_t rollback);
int32_t tsdbCloseFS(STFileSystem **ppFS);
// txn
int32_t tsdbFSEditBegin(STFileSystem *pFS, const SArray *aFileOp, EFEditT etype);
int32_t tsdbFSEditCommit(STFileSystem *pFS, EFEditT etype);
int32_t tsdbFSEditAbort(STFileSystem *pFS, EFEditT etype);

/* Exposed Structs */
struct STFileSystem {
  STsdb  *pTsdb;
  int32_t state;
  tsem_t  canEdit;
  int64_t neid;
  SArray *cstate;  // current state, SArray<STFileSet>
  EFEditT etype;
  int64_t eid;
  SArray *nstate;  // next state, SArray<STFileSet>
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FILE_SYSTEM_H*/