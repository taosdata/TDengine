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

#ifndef _TDB_PAGE_H_
#define _TDB_PAGE_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct __attribute__((__packed__)) {
  u16 flags;
  u16 nCells;
  u16 cellCont;
  u16 freeCell;
  u16 nFree;
} SPageHdr;

typedef struct SPage SPage;
struct SPage {
  pthread_spinlock_t lock;
  // Fields below used by page cache
  void *   pData;
  SPgid    pgid;
  u8       isAnchor;
  u8       isLocalPage;
  u8       isDirty;
  i32      nRef;
  SPCache *pCache;
  SPage *  pFreeNext;
  SPage *  pHashNext;
  SPage *  pLruNext;
  SPage *  pLruPrev;
  SPage *  pDirtyNext;
  SPager * pPager;
  // Fields below used by pager and am
  SPageHdr *pPageHdr;
  u16 *     aCellIdx;
  int       kLen;
  int       vLen;
  int       maxLocal;
  int       minLocal;
};

// For page lock
#define P_LOCK_SUCC 0
#define P_LOCK_BUSY 1
#define P_LOCK_FAIL -1

#define TDB_INIT_PAGE_LOCK(pPage)    pthread_spin_init(&((pPage)->lock), 0)
#define TDB_DESTROY_PAGE_LOCK(pPage) pthread_spin_destroy(&((pPage)->lock))
#define TDB_LOCK_PAGE(pPage)         pthread_spin_lock(&((pPage)->lock))
#define TDB_UNLOCK_PAGE(pPage)       pthread_spin_unlock(&((pPage)->lock))
#define TDB_TRY_LOCK_PAGE(pPage)                       \
  ({                                                   \
    int ret;                                           \
    if (pthread_spin_trylock(&((pPage)->lock)) == 0) { \
      ret = P_LOCK_SUCC;                               \
    } else if (errno == EBUSY) {                       \
      ret = P_LOCK_BUSY;                               \
    } else {                                           \
      ret = P_LOCK_FAIL;                               \
    }                                                  \
    ret;                                               \
  })

// For page ref (TODO: Need atomic operation)
#define TDB_INIT_PAGE_REF(pPage) ((pPage)->nRef = 0)
#if 0
#define TDB_REF_PAGE(pPage)   (++(pPage)->nRef)
#define TDB_UNREF_PAGE(pPage) (--(pPage)->nRef)
#define TDB_PAGE_REF(pPage)   ((pPage)->nRef)
#else
#define TDB_REF_PAGE(pPage)   atomic_add_fetch_32(&((pPage)->nRef), 1)
#define TDB_UNREF_PAGE(pPage) atomic_sub_fetch_32(&((pPage)->nRef), 1)
#define TDB_PAGE_REF(pPage)   atomic_load_32(&((pPage)->nRef))
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TDB_PAGE_H_*/