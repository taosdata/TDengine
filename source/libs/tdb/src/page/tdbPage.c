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

typedef struct __attribute__((__packed__)) {
  u16 szCell;
  u16 nxOffset;
} SFreeCell;

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

  pPage->pData = ptr;
  pPage->pageSize = pageSize;
  if (pageSize < 65536) {
    pPage->pPageMethods = &pageMethods;
  } else {
    pPage->pPageMethods = &pageLargeMethods;
  }
  TDB_INIT_PAGE_LOCK(pPage);

  /* TODO */

  *ppPage = pPage;
  return 0;
}

int tdbPageDestroy(SPage *pPage, void (*xFree)(void *arg, void *ptr), void *arg) {
  u8 *ptr;

  ptr = pPage->pData;
  (*xFree)(arg, ptr);

  return 0;
}

int tdbPageInsertCell(SPage *pPage, int idx, SCell *pCell, int szCell) {
  int    ret;
  SCell *pTarget;
  u8    *pTmp;
  int    j;

  if (pPage->nOverflow || szCell + TDB_PAGE_OFFSET_SIZE(pPage) > pPage->nFree) {
    // TODO: need to figure out if pCell may be used by outside of this function
    j = pPage->nOverflow++;

    pPage->apOvfl[j] = pCell;
    pPage->aiOvfl[j] = idx;
  } else {
    ret = tdbPageAllocate(pPage, szCell, &pTarget);
    if (ret < 0) {
      return -1;
    }

    memcpy(pTarget, pCell, szCell);
    pTmp = pPage->pCellIdx + idx * TDB_PAGE_OFFSET_SIZE(pPage);
    memmove(pTmp + TDB_PAGE_OFFSET_SIZE(pPage), pTmp, pPage->pFreeStart - pTmp - TDB_PAGE_OFFSET_SIZE(pPage));
    TDB_PAGE_CELL_OFFSET_AT_SET(pPage, idx, pTarget - pPage->pData);
    TDB_PAGE_NCELLS_SET(pPage, TDB_PAGE_NCELLS(pPage) + 1);
  }

  return 0;
}

int tdbPageDropCell(SPage *pPage, int idx) {
  // TODO
  return 0;
}

static int tdbPageAllocate(SPage *pPage, int size, SCell **ppCell) {
  SCell     *pCell;
  SFreeCell *pFreeCell;
  u8        *pOffset;
  int        ret;

  ASSERT(pPage->nFree > size + TDB_PAGE_OFFSET_SIZE(pPage));

  pCell = NULL;
  *ppCell = NULL;

  // 1. Try to allocate from the free space area
  if (pPage->pFreeEnd - pPage->pFreeStart > size + TDB_PAGE_OFFSET_SIZE(pPage)) {
    pPage->pFreeEnd -= size;
    pPage->pFreeStart += TDB_PAGE_OFFSET_SIZE(pPage);
    pCell = pPage->pFreeEnd;
  }

  // 2. Try to allocate from the page free list
  if ((pCell == NULL) && (pPage->pFreeEnd - pPage->pFreeStart >= TDB_PAGE_OFFSET_SIZE(pPage)) &&
      TDB_PAGE_FCELL(pPage)) {
#if 0
    int szCell;
    int nxOffset;

    pCell = pPage->pData + TDB_PAGE_FCELL(pPage);
    pOffset = TDB_IS_LARGE_PAGE(pPage) ? ((SPageHdrL *)(pPage->pPageHdr))[0].fCell
                                       : (u8 *)&(((SPageHdr *)(pPage->pPageHdr))[0].fCell);
    szCell = TDB_PAGE_FREE_CELL_SIZE(pPage, pCell);
    nxOffset = TDB_PAGE_FREE_CELL_NXOFFSET(pPage, pCell);

    for (;;) {
      // Find a cell
      if (szCell >= size) {
        if (szCell - size >= pPage->szFreeCell) {
          SCell *pTmpCell = pCell + size;

          TDB_PAGE_FREE_CELL_SIZE_SET(pPage, pTmpCell, szCell - size);
          TDB_PAGE_FREE_CELL_NXOFFSET_SET(pPage, pTmpCell, nxOffset);
          // TODO: *pOffset = pTmpCell - pPage->pData;
        } else {
          TDB_PAGE_NFREE_SET(pPage, TDB_PAGE_NFREE(pPage) + szCell - size);
          // TODO: *pOffset = nxOffset;
        }
        break;
      }

      // Not find a cell yet
      if (nxOffset > 0) {
        pCell = pPage->pData + nxOffset;
        pOffset = TDB_PAGE_FREE_CELL_NXOFFSET_PTR(pPage, pCell);
        szCell = TDB_PAGE_FREE_CELL_SIZE(pPage, pCell);
        nxOffset = TDB_PAGE_FREE_CELL_NXOFFSET(pPage, pCell);
        continue;
      } else {
        pCell = NULL;
        break;
      }
    }

    if (pCell) {
      pPage->pFreeStart = pPage->pFreeStart + pPage->szOffset;
    }
#endif
  }

  // 3. Try to dfragment and allocate again
  if (pCell == NULL) {
    ret = tdbPageDefragment(pPage);
    if (ret < 0) {
      return -1;
    }

    ASSERT(pPage->pFreeEnd - pPage->pFreeStart > size + TDB_PAGE_OFFSET_SIZE(pPage));
    ASSERT(pPage->nFree == pPage->pFreeEnd - pPage->pFreeStart);

    // Allocate from the free space area again
    pPage->pFreeEnd -= size;
    pPage->pFreeStart += TDB_PAGE_OFFSET_SIZE(pPage);
    pCell = pPage->pFreeEnd;
  }

  ASSERT(pCell != NULL);

  pPage->nFree = pPage->nFree - size - TDB_PAGE_OFFSET_SIZE(pPage);
  *ppCell = pCell;
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

SPageMethods pageMethods = {
    2,                  // szOffset
    sizeof(SPageHdr),   // szPageHdr
    sizeof(SFreeCell),  // szFreeCell
    getPageFlags,       // getPageFlags
    setPageFlags,       // setFlagsp
    getPageCellNum,     // getCellNum
    setPageCellNum,     // setCellNum
    getPageCellBody,    // getCellBody
    setPageCellBody,    // setCellBody
    getPageCellFree,    // getCellFree
    setPageCellFree,    // setCellFree
    getPageNFree,       // getFreeBytes
    setPageNFree,       // setFreeBytes
    getPageCellOffset,  // getCellOffset
    setPageCellOffset   // setCellOffset
};