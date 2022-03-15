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

typedef u8 SCell;

// Page header (pageSize < 65536 (64K))
typedef struct __attribute__((__packed__)) {
  u16 flags;
  u16 nCells;  // number of cells
  u16 cCells;  // cell content offset
  u16 fCell;   // first free cell offset
  u16 nFree;   // total fragment bytes in this page
} SPageHdr;

// Large page header (pageSize >= 65536 (64K))
typedef struct __attribute__((__packed__)) {
  u16 flags;
  u8  nCells[3];
  u8  cCells[3];
  u8  fCell[3];
  u8  nFree[3];
} SPageHdrL;

// Page footer
typedef struct __attribute__((__packed__)) {
  u8 cksm[4];
} SPageFtr;

struct SPage {
  pthread_spinlock_t lock;
  u8                *pData;
  int                pageSize;
  u8                 szOffset;
  u8                 szPageHdr;
  u8                 szFreeCell;
  // Fields below used by pager and am
  u8        szAmHdr;
  u8       *pPageHdr;
  u8       *pAmHdr;
  u8       *pCellIdx;
  u8       *pFreeStart;
  u8       *pFreeEnd;
  SPageFtr *pPageFtr;
  int       kLen;  // key length of the page, -1 for unknown
  int       vLen;  // value length of the page, -1 for unknown
  int       nFree;
  int       maxLocal;
  int       minLocal;
  int       nOverflow;
  SCell    *apOvfl[4];
  int       aiOvfl[4];
  // Fields used by SPCache
  TDB_PCACHE_PAGE
};

// Macros
#define TDB_IS_LARGE_PAGE(pPage) ((pPage)->szOffset == 3)

/* For small page */
#define TDB_SPAGE_FLAGS(pPage)               (((SPageHdr *)(pPage)->pPageHdr)->flags)
#define TDB_SPAGE_NCELLS(pPage)              (((SPageHdr *)(pPage)->pPageHdr)->nCells)
#define TDB_SPAGE_CCELLS(pPage)              (((SPageHdr *)(pPage)->pPageHdr)->cCells)
#define TDB_SPAGE_FCELL(pPage)               (((SPageHdr *)(pPage)->pPageHdr)->fCell)
#define TDB_SPAGE_NFREE(pPage)               (((SPageHdr *)(pPage)->pPageHdr)->nFree)
#define TDB_SPAGE_CELL_OFFSET_AT(pPage, idx) ((u16 *)((pPage)->pCellIdx))[idx]

#define TDB_SPAGE_FLAGS_SET(pPage, FLAGS)                TDB_SPAGE_FLAGS(pPage) = (FLAGS)
#define TDB_SPAGE_NCELLS_SET(pPage, NCELLS)              TDB_SPAGE_NCELLS(pPage) = (NCELLS)
#define TDB_SPAGE_CCELLS_SET(pPage, CCELLS)              TDB_SPAGE_CCELLS(pPage) = (CCELLS)
#define TDB_SPAGE_FCELL_SET(pPage, FCELL)                TDB_SPAGE_FCELL(pPage) = (FCELL)
#define TDB_SPAGE_NFREE_SET(pPage, NFREE)                TDB_SPAGE_NFREE(pPage) = (NFREE)
#define TDB_SPAGE_CELL_OFFSET_AT_SET(pPage, idx, OFFSET) TDB_SPAGE_CELL_OFFSET_AT(pPage, idx) = (OFFSET)

/* For large page */
#define TDB_LPAGE_FLAGS(pPage)               (((SPageHdrL *)(pPage)->pPageHdr)->flags)
#define TDB_LPAGE_NCELLS(pPage)              TDB_GET_U24(((SPageHdrL *)(pPage)->pPageHdr)->nCells)
#define TDB_LPAGE_CCELLS(pPage)              TDB_GET_U24(((SPageHdrL *)(pPage)->pPageHdr)->cCells)
#define TDB_LPAGE_FCELL(pPage)               TDB_GET_U24(((SPageHdrL *)(pPage)->pPageHdr)->fCell)
#define TDB_LPAGE_NFREE(pPage)               TDB_GET_U24(((SPageHdrL *)(pPage)->pPageHdr)->nFree)
#define TDB_LPAGE_CELL_OFFSET_AT(pPage, idx) TDB_GET_U24((pPage)->pCellIdx + idx * 3)

#define TDB_LPAGE_FLAGS_SET(pPage, FLAGS)                TDB_LPAGE_FLAGS(pPage) = (flags)
#define TDB_LPAGE_NCELLS_SET(pPage, NCELLS)              TDB_PUT_U24(((SPageHdrL *)(pPage)->pPageHdr)->nCells, NCELLS)
#define TDB_LPAGE_CCELLS_SET(pPage, CCELLS)              TDB_PUT_U24(((SPageHdrL *)(pPage)->pPageHdr)->cCells, CCELLS)
#define TDB_LPAGE_FCELL_SET(pPage, FCELL)                TDB_PUT_U24(((SPageHdrL *)(pPage)->pPageHdr)->fCell, FCELL)
#define TDB_LPAGE_NFREE_SET(pPage, NFREE)                TDB_PUT_U24(((SPageHdrL *)(pPage)->pPageHdr)->nFree, NFREE)
#define TDB_LPAGE_CELL_OFFSET_AT_SET(pPage, idx, OFFSET) TDB_PUT_U24((pPage)->pCellIdx + idx * 3, OFFSET)

/* For page */
#define TDB_PAGE_FLAGS(pPage)  (TDB_IS_LARGE_PAGE(pPage) ? TDB_LPAGE_FLAGS(pPage) : TDB_SPAGE_FLAGS(pPage))
#define TDB_PAGE_NCELLS(pPage) (TDB_IS_LARGE_PAGE(pPage) ? TDB_LPAGE_NCELLS(pPage) : TDB_SPAGE_NCELLS(pPage))
#define TDB_PAGE_CCELLS(pPage) (TDB_IS_LARGE_PAGE(pPage) ? TDB_LPAGE_CCELLS(pPage) : TDB_SPAGE_CCELLS(pPage))
#define TDB_PAGE_FCELL(pPage)  (TDB_IS_LARGE_PAGE(pPage) ? TDB_LPAGE_FCELL(pPage) : TDB_SPAGE_FCELL(pPage))
#define TDB_PAGE_NFREE(pPage)  (TDB_IS_LARGE_PAGE(pPage) ? TDB_LPAGE_NFREE(pPage) : TDB_SPAGE_NFREE(pPage))
#define TDB_PAGE_CELL_OFFSET_AT(pPage, idx) \
  (TDB_IS_LARGE_PAGE(pPage) ? TDB_LPAGE_CELL_OFFSET_AT(pPage, idx) : TDB_SPAGE_CELL_OFFSET_AT(pPage, idx))

#define TDB_PAGE_FLAGS_SET(pPage, FLAGS) \
  do {                                   \
    if (TDB_IS_LARGE_PAGE(pPage)) {      \
      TDB_LPAGE_FLAGS_SET(pPage, FLAGS); \
    } else {                             \
      TDB_SPAGE_FLAGS_SET(pPage, FLAGS); \
    }                                    \
  } while (0)

#define TDB_PAGE_NCELLS_SET(pPage, NCELLS) \
  do {                                     \
    if (TDB_IS_LARGE_PAGE(pPage)) {        \
      TDB_LPAGE_NCELLS_SET(pPage, NCELLS); \
    } else {                               \
      TDB_SPAGE_NCELLS_SET(pPage, NCELLS); \
    }                                      \
  } while (0)

#define TDB_PAGE_CCELLS_SET(pPage, CCELLS) \
  do {                                     \
    if (TDB_IS_LARGE_PAGE(pPage)) {        \
      TDB_LPAGE_CCELLS_SET(pPage, CCELLS); \
    } else {                               \
      TDB_SPAGE_CCELLS_SET(pPage, CCELLS); \
    }                                      \
  } while (0)

#define TDB_PAGE_FCELL_SET(pPage, FCELL) \
  do {                                   \
    if (TDB_IS_LARGE_PAGE(pPage)) {      \
      TDB_LPAGE_FCELL_SET(pPage, FCELL); \
    } else {                             \
      TDB_SPAGE_FCELL_SET(pPage, FCELL); \
    }                                    \
  } while (0)

#define TDB_PAGE_NFREE_SET(pPage, NFREE) \
  do {                                   \
    if (TDB_IS_LARGE_PAGE(pPage)) {      \
      TDB_LPAGE_NFREE_SET(pPage, NFREE); \
    } else {                             \
      TDB_SPAGE_NFREE_SET(pPage, NFREE); \
    }                                    \
  } while (0)

#define TDB_PAGE_CELL_OFFSET_AT_SET(pPage, idx, OFFSET) \
  do {                                                  \
    if (TDB_IS_LARGE_PAGE(pPage)) {                     \
      TDB_LPAGE_CELL_OFFSET_AT_SET(pPage, idx, OFFSET); \
    } else {                                            \
      TDB_SPAGE_CELL_OFFSET_AT_SET(pPage, idx, OFFSET); \
    }                                                   \
  } while (0)

#define TDB_PAGE_CELL_AT(pPage, idx) ((pPage)->pData + TDB_PAGE_CELL_OFFSET_AT(pPage, idx))

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

// For page ref
#define TDB_INIT_PAGE_REF(pPage) ((pPage)->nRef = 0)
#if 0
#define TDB_REF_PAGE(pPage)     (++(pPage)->nRef)
#define TDB_UNREF_PAGE(pPage)   (--(pPage)->nRef)
#define TDB_GET_PAGE_REF(pPage) ((pPage)->nRef)
#else
#define TDB_REF_PAGE(pPage)     atomic_add_fetch_32(&((pPage)->nRef), 1)
#define TDB_UNREF_PAGE(pPage)   atomic_sub_fetch_32(&((pPage)->nRef), 1)
#define TDB_GET_PAGE_REF(pPage) atomic_load_32(&((pPage)->nRef))
#endif

// APIs
int tdbPageCreate(int pageSize, SPage **ppPage, void *(*xMalloc)(void *, size_t), void *arg);
int tdbPageDestroy(SPage *pPage, void (*xFree)(void *arg, void *ptr), void *arg);
int tdbPageInsertCell(SPage *pPage, int idx, SCell *pCell, int szCell);
int tdbPageDropCell(SPage *pPage, int idx);

#ifdef __cplusplus
}
#endif

#endif /*_TDB_PAGE_H_*/