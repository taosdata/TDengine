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

// PAGE APIS implemented
typedef struct {
  int szOffset;
  int szPageHdr;
  int szFreeCell;
  // flags
  u16 (*getFlags)(SPage *);
  void (*setFlags)(SPage *, u16);
  // cell number
  int (*getCellNum)(SPage *);
  void (*setCellNum)(SPage *, int);
  // cell content offset
  int (*getCellBody)(SPage *);
  void (*setCellBody)(SPage *, int);
  // first free cell offset (0 means no free cells)
  int (*getCellFree)(SPage *);
  void (*setCellFree)(SPage *, int);
  // total free bytes
  int (*getFreeBytes)(SPage *);
  void (*setFreeBytes)(SPage *, int);
  // cell offset at idx
  int (*getCellOffset)(SPage *, int);
  void (*setCellOffset)(SPage *, int, int);
} SPageMethods;

// Page footer
typedef struct __attribute__((__packed__)) {
  u8 cksm[4];
} SPageFtr;

struct SPage {
  TdThreadSpinlock lock;
  u8                *pData;
  int                pageSize;
  SPageMethods      *pPageMethods;
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

/* For page */
#define TDB_PAGE_FLAGS(pPage)               (*(pPage)->pPageMethods->getFlags)(pPage)
#define TDB_PAGE_NCELLS(pPage)              (*(pPage)->pPageMethods->getCellNum)(pPage)
#define TDB_PAGE_CCELLS(pPage)              (*(pPage)->pPageMethods->getCellBody)(pPage)
#define TDB_PAGE_FCELL(pPage)               (*(pPage)->pPageMethods->getCellFree)(pPage)
#define TDB_PAGE_NFREE(pPage)               (*(pPage)->pPageMethods->getFreeBytes)(pPage)
#define TDB_PAGE_CELL_OFFSET_AT(pPage, idx) (*(pPage)->pPageMethods->getCellOffset)(pPage, idx)

#define TDB_PAGE_FLAGS_SET(pPage, FLAGS)                (*(pPage)->pPageMethods->setFlags)(pPage, FLAGS)
#define TDB_PAGE_NCELLS_SET(pPage, NCELLS)              (*(pPage)->pPageMethods->setCellNum)(pPage, NCELLS)
#define TDB_PAGE_CCELLS_SET(pPage, CCELLS)              (*(pPage)->pPageMethods->setCellBody)(pPage, CCELLS)
#define TDB_PAGE_FCELL_SET(pPage, FCELL)                (*(pPage)->pPageMethods->setCellFree)(pPage, FCELL)
#define TDB_PAGE_NFREE_SET(pPage, NFREE)                (*(pPage)->pPageMethods->setFreeBytes)(pPage, NFREE)
#define TDB_PAGE_CELL_OFFSET_AT_SET(pPage, idx, OFFSET) (*(pPage)->pPageMethods->setCellOffset)(pPage, idx, OFFSET)

#define TDB_PAGE_OFFSET_SIZE(pPage) ((pPage)->pPageMethods->szOffset)

#define TDB_PAGE_CELL_AT(pPage, idx) ((pPage)->pData + TDB_PAGE_CELL_OFFSET_AT(pPage, idx))

// For page lock
#define P_LOCK_SUCC 0
#define P_LOCK_BUSY 1
#define P_LOCK_FAIL -1

#define TDB_INIT_PAGE_LOCK(pPage)    taosThreadSpinInit(&((pPage)->lock), 0)
#define TDB_DESTROY_PAGE_LOCK(pPage) taosThreadSpinDestroy(&((pPage)->lock))
#define TDB_LOCK_PAGE(pPage)         taosThreadSpinLock(&((pPage)->lock))
#define TDB_UNLOCK_PAGE(pPage)       taosThreadSpinUnlock(&((pPage)->lock))
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

// APIs
int tdbPageCreate(int pageSize, SPage **ppPage, void *(*xMalloc)(void *, size_t), void *arg);
int tdbPageDestroy(SPage *pPage, void (*xFree)(void *arg, void *ptr), void *arg);
int tdbPageInsertCell(SPage *pPage, int idx, SCell *pCell, int szCell);
int tdbPageDropCell(SPage *pPage, int idx);

#ifdef __cplusplus
}
#endif

#endif /*_TDB_PAGE_H_*/