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

#ifndef _TD_TDB_MPFILE_H_
#define _TD_TDB_MPFILE_H_

#include "tdbDef.h"
#include "tdb_mpool.h"

#ifdef __cplusplus
extern "C" {
#endif

// Exposed handle
typedef struct TDB_MPFILE TDB_MPFILE;

// Exposed apis
int tdbMPFOpen(TDB_MPFILE **mpfp, const char *fname, TDB_MPOOL *mp);
int tdbMPFClose(TDB_MPFILE *mpf);
int tdbMPFGet(TDB_MPFILE *mpf, pgid_t pgid, void *addr);
int tdbMPFPut(TDB_MPOOL *mpf, pgid_t pgid, void *addr);

// Hidden structures
struct TDB_MPFILE {
  uint8_t    fuid[20];  // file unique ID
  TDB_MPOOL *mp;        // underlying memory pool
  char *     fname;     // file name
  int        fd;        // fd
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDB_MPFILE_H_*/