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

#ifndef _TD_TDB_MPOOL_H_
#define _TD_TDB_MPOOL_H_

#include "tdb_inc.h"

#ifdef __cplusplus
extern "C" {
#endif

// Exposed handle
typedef struct TDB_MPOOL  TDB_MPOOL;
typedef struct TDB_MPFILE TDB_MPFILE;

typedef struct MP_PAGE {
  // SRWLatch  rwLatch;
  pgid_t  mpgid;
  uint8_t dirty;
  uint8_t fileid[TDB_FILE_UID_LEN];
  int32_t pinRef;
  TD_DLIST_NODE(MP_PAGE);
  char *page[];
} MP_PAGE;

#define MP_PAGE_SIZE(pgsize) (sizeof(MP_PAGE) + (pgsize))

typedef TD_DLIST(MP_PAGE) MP_PAGE_LIST;
struct TDB_MPOOL {
  int64_t      cachesize;
  pgsize_t     pgsize;
  int32_t      npages;
  MP_PAGE *    pages;
  MP_PAGE_LIST freeList;
  // Hash<pgid_t, frame_id_t>
  int32_t       nbucket;
  MP_PAGE_LIST *hashtab;
  // TODO: TD_DLIST(TD_MPFILE) mpfList; // MPFILE registered on this memory pool
};

struct TDB_MPFILE {
  uint8_t    fuid[20];  // file unique ID
  TDB_MPOOL *mp;        // underlying memory pool
  char *     fname;     // file name
  int        fd;        // fd
};

#define MP_PAGE_AT(mp, idx) ((char *)((mp)->pages) + MP_PAGE_SIZE((mp)->pgsize) * (idx))

/*=================================================== Exposed apis ==================================================*/
// TDB_MPOOL
int tdbMPoolOpen(TDB_MPOOL **mpp, uint64_t cachesize, pgsize_t pgsize);
int tdbMPoolClose(TDB_MPOOL *mp);

// TDB_MPFILE
int tdbMPFOpen(TDB_MPFILE **mpfp, const char *fname, TDB_MPOOL *mp);
int tdbMPFClose(TDB_MPFILE *mpf);
int tdbMPFGet(TDB_MPFILE *mpf, pgno_t pgid, void *addr);
int tdbMPFPut(TDB_MPOOL *mpf, pgno_t pgid, void *addr);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDB_MPOOL_H_*/