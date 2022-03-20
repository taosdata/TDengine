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

#include "tdbInt.h"

extern SPageMethods pageMethods;
extern SPageMethods pageLargeMethods;

#define TDB_PAGE_OFFSET_SIZE(pPage)    ((pPage)->pPageMethods->szOffset)
#define TDB_PAGE_HDR_SIZE(pPage)       ((pPage)->pPageMethods->szPageHdr)
#define TDB_PAGE_FREE_CELL_SIZE(pPage) ((pPage)->pPageMethods->szFreeCell)
#define TDB_PAGE_MAX_FREE_BLOCK(pPage) \
  ((pPage)->pageSize - (pPage)->szAmHdr - TDB_PAGE_HDR_SIZE(pPage) - sizeof(SPageFtr))

static int tdbPageAllocate(SPage *pPage, int size, SCell **ppCell);
static int tdbPageDefragment(SPage *pPage);

int tdbPageCreate(int pageSize, SPage **ppPage, void *(*xMalloc)(void *, size_t), void *arg) {
  SPage *pPage;
  u8    *ptr;
  int    size;

  ASSERT(TDB_IS_PGSIZE_VLD(pageSize));

  *ppPage = NULL;
  size = pageSize + sizeof(*pPage);

  ptr = (u8 *)((*xMalloc)(arg, size));
  if (pPage == NULL) {
    return -1;
  }

  memset(ptr, 0, size);
  pPage = (SPage *)(ptr + pageSize);

  TDB_INIT_PAGE_LOCK(pPage);
  pPage->pData = ptr;
  pPage->pageSize = pageSize;
  if (pageSize < 65536) {
    pPage->pPageMethods = &pageMethods;
  } else {
    pPage->pPageMethods = &pageLargeMethods;
  }

  *ppPage = pPage;
  return 0;
}

int tdbPageDestroy(SPage *pPage, void (*xFree)(void *arg, void *ptr), void *arg) {
  u8 *ptr;

  ptr = pPage->pData;
  (*xFree)(arg, ptr);

  return 0;
}

void tdbPageZero(SPage *pPage) {
  TDB_PAGE_NCELLS_SET(pPage, 0);
  TDB_PAGE_CCELLS_SET(pPage, pPage->pageSize - sizeof(SPageFtr));
  TDB_PAGE_FCELL_SET(pPage, 0);
  TDB_PAGE_NFREE_SET(pPage, TDB_PAGE_MAX_FREE_BLOCK(pPage));
  tdbPageInit(pPage);
}

void tdbPageInit(SPage *pPage) {
  pPage->pAmHdr = pPage->pData;
  pPage->pPageHdr = pPage->pAmHdr + pPage->szAmHdr;
  pPage->pCellIdx = pPage->pPageHdr + TDB_PAGE_HDR_SIZE(pPage);
  pPage->pFreeStart = pPage->pCellIdx + TDB_PAGE_OFFSET_SIZE(pPage) * TDB_PAGE_NCELLS(pPage);
  pPage->pFreeEnd = pPage->pData + TDB_PAGE_CCELLS(pPage);
  pPage->pPageFtr = (SPageFtr *)(pPage->pData + pPage->pageSize - sizeof(SPageFtr));
  pPage->nOverflow = 0;

  ASSERT(pPage->pFreeEnd >= pPage->pFreeStart);
  ASSERT(pPage->pFreeEnd - pPage->pFreeStart <= TDB_PAGE_NFREE(pPage));
}

int tdbPageInsertCell(SPage *pPage, int idx, SCell *pCell, int szCell) {
  int    nFree;
  int    ret;
  int    nCells;
  int    lidx;  // local idx
  SCell *pNewCell;

  ASSERT(szCell <= TDB_PAGE_MAX_FREE_BLOCK(pPage));

  nFree = TDB_PAGE_NFREE(pPage);
  nCells = TDB_PAGE_NCELLS(pPage);

  if (nFree >= szCell + TDB_PAGE_OFFSET_SIZE(pPage)) {
    // page must has enough space to hold the cell locally
    ret = tdbPageAllocate(pPage, szCell, &pNewCell);
    ASSERT(ret == 0);

    memcpy(pNewCell, pCell, szCell);

    if (pPage->nOverflow == 0) {
      lidx = idx;
    } else {
      // TODO
      // lidx = ;
    }

    // no overflow cell exists in this page
    u8 *src = pPage->pCellIdx + TDB_PAGE_OFFSET_SIZE(pPage) * lidx;
    u8 *dest = src + TDB_PAGE_OFFSET_SIZE(pPage);
    memmove(dest, src, pPage->pFreeStart - dest);
    TDB_PAGE_CELL_OFFSET_AT_SET(pPage, lidx, TDB_PAGE_OFFSET_SIZE(pPage) * lidx);
    TDB_PAGE_NCELLS_SET(pPage, nCells + 1);
  } else {
    // TODO: page not has enough space
    pPage->apOvfl[pPage->nOverflow] = pCell;
    pPage->aiOvfl[pPage->nOverflow] = idx;
    pPage->nOverflow++;
  }

  return 0;
}

int tdbPageDropCell(SPage *pPage, int idx) {
  // TODO
  return 0;
}

static int tdbPageAllocate(SPage *pPage, int szCell, SCell **ppCell) {
  SCell *pFreeCell;
  u8    *pOffset;
  int    nFree;
  int    ret;
  int    cellFree;

  *ppCell = NULL;
  nFree = TDB_PAGE_NFREE(pPage);

  ASSERT(nFree >= szCell + TDB_PAGE_OFFSET_SIZE(pPage));
  ASSERT(TDB_PAGE_CCELLS(pPage) == pPage->pFreeEnd - pPage->pData);

  // 1. Try to allocate from the free space block area
  if (pPage->pFreeEnd - pPage->pFreeStart >= szCell + TDB_PAGE_OFFSET_SIZE(pPage)) {
    pPage->pFreeStart += TDB_PAGE_OFFSET_SIZE(pPage);
    pPage->pFreeEnd -= szCell;

    TDB_PAGE_CCELLS_SET(pPage, pPage->pFreeEnd - pPage->pData);
    TDB_PAGE_NFREE_SET(pPage, nFree - szCell - TDB_PAGE_OFFSET_SIZE(pPage));

    *ppCell = pPage->pFreeEnd;
    return 0;
  }

  // 2. Try to allocate from the page free list
  cellFree = TDB_PAGE_FCELL(pPage);
  ASSERT(cellFree == 0 || cellFree > pPage->pFreeEnd - pPage->pData);
  if (cellFree && pPage->pFreeEnd - pPage->pFreeStart >= TDB_PAGE_OFFSET_SIZE(pPage)) {
    SCell *pPrevFreeCell = NULL;
    int    szPrevFreeCell;
    int    szFreeCell;
    int    nxFreeCell;
    int    newSize;

    for (;;) {
      if (cellFree == 0) break;

      pFreeCell = pPage->pData + cellFree;
      pPage->pPageMethods->getFreeCellInfo(pFreeCell, &szFreeCell, &nxFreeCell);

      if (szFreeCell >= szCell) {
        pPage->pFreeStart += TDB_PAGE_OFFSET_SIZE(pPage);
        TDB_PAGE_NFREE_SET(pPage, nFree - TDB_PAGE_OFFSET_SIZE(pPage) - szCell);
        *ppCell = pFreeCell;

        newSize = szFreeCell - szCell;
        pFreeCell += szCell;
        if (newSize >= TDB_PAGE_FREE_CELL_SIZE(pPage)) {
          pPage->pPageMethods->setFreeCellInfo(pFreeCell, newSize, nxFreeCell);
          if (pPrevFreeCell) {
            pPage->pPageMethods->setFreeCellInfo(pPrevFreeCell, szPrevFreeCell, pFreeCell - pPage->pData);
          } else {
            TDB_PAGE_FCELL_SET(pPage, pFreeCell - pPage->pData);
          }

          return 0;
        } else {
          if (pPrevFreeCell) {
            pPage->pPageMethods->setFreeCellInfo(pPrevFreeCell, szPrevFreeCell, nxFreeCell);
          } else {
            TDB_PAGE_FCELL_SET(pPage, nxFreeCell);
          }
        }
      } else {
        pPrevFreeCell = pFreeCell;
        szPrevFreeCell = szFreeCell;
        cellFree = nxFreeCell;
      }
    }
  }

// 3. Try to dfragment and allocate again
#if 0
  if (pCell == NULL) {
    ret = tdbPageDefragment(pPage);
    if (ret < 0) {
      return -1;
    }

    ASSERT(pPage->pFreeEnd - pPage->pFreeStart > size + TDB_PAGE_OFFSET_SIZE(pPage));
    // ASSERT(pPage->nFree == pPage->pFreeEnd - pPage->pFreeStart);

    // Allocate from the free space area again
    pPage->pFreeEnd -= size;
    pPage->pFreeStart += TDB_PAGE_OFFSET_SIZE(pPage);
    pCell = pPage->pFreeEnd;
  }

  ASSERT(pCell != NULL);

  // pPage->nFree = pPage->nFree - size - TDB_PAGE_OFFSET_SIZE(pPage);
  *ppCell = pCell;
#endif
  return 0;
}

static int tdbPageFree(SPage *pPage, int idx, SCell *pCell, int size) {
  // TODO
  return 0;
}

static int tdbPageDefragment(SPage *pPage) {
  // TODO
  ASSERT(0);
  return 0;
}

/* ---------------------------------------------------------------------------------------------------------- */
typedef struct __attribute__((__packed__)) {
  u16 flags;
  u16 cellNum;
  u16 cellBody;
  u16 cellFree;
  u16 nFree;
} SPageHdr;

typedef struct __attribute__((__packed__)) {
  u16 szCell;
  u16 nxOffset;
} SFreeCell;

// flags
static inline u16  getPageFlags(SPage *pPage) { return ((SPageHdr *)(pPage->pPageHdr))[0].flags; }
static inline void setPageFlags(SPage *pPage, u16 flags) { ((SPageHdr *)(pPage->pPageHdr))[0].flags = flags; }

// cellNum
static inline int  getPageCellNum(SPage *pPage) { return ((SPageHdr *)(pPage->pPageHdr))[0].cellNum; }
static inline void setPageCellNum(SPage *pPage, int cellNum) {
  ASSERT(cellNum < 65536);
  ((SPageHdr *)(pPage->pPageHdr))[0].cellNum = (u16)cellNum;
}

// cellBody
static inline int  getPageCellBody(SPage *pPage) { return ((SPageHdr *)(pPage->pPageHdr))[0].cellBody; }
static inline void setPageCellBody(SPage *pPage, int cellBody) {
  ASSERT(cellBody < 65536);
  ((SPageHdr *)(pPage->pPageHdr))[0].cellBody = (u16)cellBody;
}

// cellFree
static inline int  getPageCellFree(SPage *pPage) { return ((SPageHdr *)(pPage->pPageHdr))[0].cellFree; }
static inline void setPageCellFree(SPage *pPage, int cellFree) {
  ASSERT(cellFree < 65536);
  ((SPageHdr *)(pPage->pPageHdr))[0].cellFree = (u16)cellFree;
}

// nFree
static inline int  getPageNFree(SPage *pPage) { return ((SPageHdr *)(pPage->pPageHdr))[0].nFree; }
static inline void setPageNFree(SPage *pPage, int nFree) {
  ASSERT(nFree < 65536);
  ((SPageHdr *)(pPage->pPageHdr))[0].nFree = (u16)nFree;
}

// cell offset
static inline int getPageCellOffset(SPage *pPage, int idx) {
  ASSERT(idx >= 0 && idx < getPageCellNum(pPage));
  return ((u16 *)pPage->pCellIdx)[idx];
}

static inline void setPageCellOffset(SPage *pPage, int idx, int offset) {
  ASSERT(offset < 65536);
  ((u16 *)pPage->pCellIdx)[idx] = (u16)offset;
}

// free cell info
static inline void getPageFreeCellInfo(SCell *pCell, int *szCell, int *nxOffset) {
  SFreeCell *pFreeCell = (SFreeCell *)pCell;
  *szCell = pFreeCell->szCell;
  *nxOffset = pFreeCell->nxOffset;
}

static inline void setPageFreeCellInfo(SCell *pCell, int szCell, int nxOffset) {
  SFreeCell *pFreeCell = (SFreeCell *)pCell;
  pFreeCell->szCell = szCell;
  pFreeCell->nxOffset = nxOffset;
}

SPageMethods pageMethods = {
    2,                    // szOffset
    sizeof(SPageHdr),     // szPageHdr
    sizeof(SFreeCell),    // szFreeCell
    getPageFlags,         // getPageFlags
    setPageFlags,         // setFlagsp
    getPageCellNum,       // getCellNum
    setPageCellNum,       // setCellNum
    getPageCellBody,      // getCellBody
    setPageCellBody,      // setCellBody
    getPageCellFree,      // getCellFree
    setPageCellFree,      // setCellFree
    getPageNFree,         // getFreeBytes
    setPageNFree,         // setFreeBytes
    getPageCellOffset,    // getCellOffset
    setPageCellOffset,    // setCellOffset
    getPageFreeCellInfo,  // getFreeCellInfo
    setPageFreeCellInfo   // setFreeCellInfo
};