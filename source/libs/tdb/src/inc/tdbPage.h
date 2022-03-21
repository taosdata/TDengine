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
  // free cell info
  void (*getFreeCellInfo)(SCell *pCell, int *szCell, int *nxOffset);
  void (*setFreeCellInfo)(SCell *pCell, int szCell, int nxOffset);
} SPageMethods;

// Page footer
typedef struct __attribute__((__packed__)) {
  u8 cksm[4];
} SPageFtr;

struct SPage {
  pthread_spinlock_t lock;
  int                pageSize;
  u8                *pData;
  SPageMethods      *pPageMethods;
  // Fields below used by pager and am
  u8       *pAmHdr;
  u8       *pPageHdr;
  u8       *pCellIdx;
  u8       *pFreeStart;
  u8       *pFreeEnd;
  SPageFtr *pPageFtr;
  int       nOverflow;
  SCell    *apOvfl[4];
  int       aiOvfl[4];
  int       kLen;  // key length of the page, -1 for unknown
  int       vLen;  // value length of the page, -1 for unknown
  int       maxLocal;
  int       minLocal;
  int (*xCellSize)(SCell *pCell);
  // Fields used by SPCache
  TDB_PCACHE_PAGE
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

// APIs
#define TDB_PAGE_TOTAL_CELLS(pPage) ((pPage)->nOverflow + (pPage)->pPageMethods->getCellNum(pPage))

int  tdbPageCreate(int pageSize, SPage **ppPage, void *(*xMalloc)(void *, size_t), void *arg);
int  tdbPageDestroy(SPage *pPage, void (*xFree)(void *arg, void *ptr), void *arg);
void tdbPageZero(SPage *pPage, u8 szAmHdr);
void tdbPageInit(SPage *pPage, u8 szAmHdr);
int  tdbPageInsertCell(SPage *pPage, int idx, SCell *pCell, int szCell);
int  tdbPageDropCell(SPage *pPage, int idx);

static inline SCell *tdbPageGetCell(SPage *pPage, int idx) {
  SCell *pCell;
  int    iOvfl;
  int    lidx;

  ASSERT(idx >= 0 && idx < pPage->nOverflow + pPage->pPageMethods->getCellNum(pPage));

  iOvfl = 0;
  for (; iOvfl < pPage->nOverflow; iOvfl++) {
    if (pPage->aiOvfl[iOvfl] == idx) {
      pCell = pPage->apOvfl[iOvfl];
      return pCell;
    } else if (pPage->aiOvfl[iOvfl] > idx) {
      break;
    }
  }

  lidx = idx - iOvfl;
  ASSERT(lidx >= 0 && lidx < pPage->pPageMethods->getCellNum(pPage));
  pCell = pPage->pData + pPage->pPageMethods->getCellOffset(pPage, lidx);

  return pCell;
}

#ifdef __cplusplus
}
#endif

#endif /*_TDB_PAGE_H_*/