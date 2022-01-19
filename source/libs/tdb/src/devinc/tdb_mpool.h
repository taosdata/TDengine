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

#include "tdbDef.h"

#ifdef __cplusplus
extern "C" {
#endif

// Exposed handle
typedef struct TDB_MPOOL TDB_MPOOL;

// Exposed apis
int tdbOpenMP(TDB_MPOOL **mpp, uint64_t cachesize, pgsize_t pgsize);
int tdbCloseMP(TDB_MPOOL *mp);
int tdbMPFetchPage(TDB_MPOOL *mp, mp_pgid_t mpgid, void *p);
int tdbMpUnfetchPage(TDB_MPOOL *mp, mp_pgid_t mpgid, void *p);

// Hidden impls
#define TDB_FILE_UID_LEN 20

typedef struct {
  uint8_t fuid[TDB_FILE_UID_LEN];
  pgid_t  pgid;
} mp_pgid_t;

typedef struct MP_PAGE {
  SRWLatch  rwLatch;
  mp_pgid_t mpgid;
  uint8_t   dirty;
  int32_t   pinRef;
  // TD_DLIST_NODE(MP_PAGE); // The free list handle
  char *page[];
} MP_PAGE;

struct TDB_MPOOL {
  pthread_mutex_t mutex;
  int64_t         cachesize;
  pgsize_t        pgsize;
  MP_PAGE *       pages;
  // TD_DBLIST(MP_PAGE) freeList;
  // TD_DLIST(TD_MPFILE) mpfList; // MPFILE registered on this memory pool
  // Hash<mp_pgid_t, frameid> hash;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDB_MPOOL_H_*/