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

#include "tdbInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// Exposed handle
typedef struct TDB_MPOOL  TDB_MPOOL;
typedef struct TDB_MPFILE TDB_MPFILE;

typedef TD_DLIST_NODE(pg_t) pg_free_dlist_node_t, pg_hash_dlist_node_t;
typedef struct pg_t {
  SRWLatch             rwLatch;
  frame_id_t           frameid;
  pgid_t               pgid;
  uint8_t              dirty;
  uint8_t              rbit;
  int32_t              pinRef;
  pg_free_dlist_node_t free;
  pg_hash_dlist_node_t hash;
  void *               p;
} pg_t;

typedef TD_DLIST(pg_t) pg_list_t;
typedef struct {
  SRWLatch latch;
  TD_DLIST(TDB_MPFILE);
} mpf_bucket_t;
struct TDB_MPOOL {
  int64_t    cachesize;
  pgsize_t   pgsize;
  int32_t    npages;
  pg_t *     pages;
  pg_list_t  freeList;
  frame_id_t clockHand;
  struct {
    int32_t    nbucket;
    pg_list_t *hashtab;
  } pgtab;  // page table, hash<pgid_t, pg_t>
  struct {
#define MPF_HASH_BUCKETS 16
    mpf_bucket_t buckets[MPF_HASH_BUCKETS];
  } mpfht;  // MPF hash table. MPFs using this MP will be put in this hash table
};

#define MP_PAGE_AT(mp, idx) (mp)->pages[idx]

typedef TD_DLIST_NODE(TDB_MPFILE) td_mpf_dlist_node_t;
struct TDB_MPFILE {
  char *              fname;                    // file name
  int                 fd;                       // fd
  uint8_t             fileid[TDB_FILE_ID_LEN];  // file ID
  TDB_MPOOL *         mp;                       // underlying memory pool
  td_mpf_dlist_node_t node;
};

/*=================================================== Exposed apis ==================================================*/
// TDB_MPOOL
int tdbMPoolOpen(TDB_MPOOL **mpp, uint64_t cachesize, pgsize_t pgsize);
int tdbMPoolClose(TDB_MPOOL *mp);
int tdbMPoolSync(TDB_MPOOL *mp);

// TDB_MPFILE
int tdbMPoolFileOpen(TDB_MPFILE **mpfp, const char *fname, TDB_MPOOL *mp);
int tdbMPoolFileClose(TDB_MPFILE *mpf);
int tdbMPoolFileNewPage(TDB_MPFILE *mpf, pgno_t *pgno, void *addr);
int tdbMPoolFileFreePage(TDB_MPOOL *mpf, pgno_t *pgno, void *addr);
int tdbMPoolFileGetPage(TDB_MPFILE *mpf, pgno_t pgno, void *addr);
int tdbMPoolFilePutPage(TDB_MPFILE *mpf, pgno_t pgno, void *addr);
int tdbMPoolFileSync(TDB_MPFILE *mpf);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDB_MPOOL_H_*/