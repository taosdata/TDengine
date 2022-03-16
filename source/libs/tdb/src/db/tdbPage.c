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

typedef struct __attribute__((__packed__)) {
  u8 szCell[2];
  u8 nxOffset[2];
} SFreeCell;

typedef struct __attribute__((__packed__)) {
  u8 szCell[3];
  u8 nxOffset[3];
} SFreeCellL;

/* For small page */
#define TDB_SPAGE_FREE_CELL_SIZE_PTR(PCELL)     (((SFreeCell *)(PCELL))->szCell)
#define TDB_SPAGE_FREE_CELL_NXOFFSET_PTR(PCELL) (((SFreeCell *)(PCELL))->nxOffset)

#define TDB_SPAGE_FREE_CELL_SIZE(PCELL)     ((u16 *)TDB_SPAGE_FREE_CELL_SIZE_PTR(PCELL))[0]
#define TDB_SPAGE_FREE_CELL_NXOFFSET(PCELL) ((u16 *)TDB_SPAGE_FREE_CELL_NXOFFSET_PTR(PCELL))[0]

#define TDB_SPAGE_FREE_CELL_SIZE_SET(PCELL, SIZE)       (TDB_SPAGE_FREE_CELL_SIZE(PCELL) = (SIZE))
#define TDB_SPAGE_FREE_CELL_NXOFFSET_SET(PCELL, OFFSET) (TDB_SPAGE_FREE_CELL_NXOFFSET(PCELL) = (OFFSET))

/* For large page */
#define TDB_LPAGE_FREE_CELL_SIZE_PTR(PCELL)     (((SFreeCellL *)(PCELL))->szCell)
#define TDB_LPAGE_FREE_CELL_NXOFFSET_PTR(PCELL) (((SFreeCellL *)(PCELL))->nxOffset)

#define TDB_LPAGE_FREE_CELL_SIZE(PCELL)     TDB_GET_U24(TDB_LPAGE_FREE_CELL_SIZE_PTR(PCELL))
#define TDB_LPAGE_FREE_CELL_NXOFFSET(PCELL) TDB_GET_U24(TDB_LPAGE_FREE_CELL_NXOFFSET_PTR(PCELL))

#define TDB_LPAGE_FREE_CELL_SIZE_SET(PCELL, SIZE)       TDB_PUT_U24(TDB_LPAGE_FREE_CELL_SIZE_PTR(PCELL), SIZE)
#define TDB_LPAGE_FREE_CELL_NXOFFSET_SET(PCELL, OFFSET) TDB_PUT_U24(TDB_LPAGE_FREE_CELL_NXOFFSET_PTR(PCELL), OFFSET)

/* For page */
#define TDB_PAGE_FREE_CELL_SIZE_PTR(PPAGE, PCELL) \
  (TDB_IS_LARGE_PAGE(pPage) ? TDB_LPAGE_FREE_CELL_SIZE_PTR(PCELL) : TDB_SPAGE_FREE_CELL_SIZE_PTR(PCELL))
#define TDB_PAGE_FREE_CELL_NXOFFSET_PTR(PPAGE, PCELL) \
  (TDB_IS_LARGE_PAGE(pPage) ? TDB_LPAGE_FREE_CELL_NXOFFSET_PTR(PCELL) : TDB_SPAGE_FREE_CELL_NXOFFSET_PTR(PCELL))

#define TDB_PAGE_FREE_CELL_SIZE(PPAGE, PCELL) \
  (TDB_IS_LARGE_PAGE(pPage) ? TDB_LPAGE_FREE_CELL_SIZE(PCELL) : TDB_SPAGE_FREE_CELL_SIZE(PCELL))
#define TDB_PAGE_FREE_CELL_NXOFFSET(PPAGE, PCELL) \
  (TDB_IS_LARGE_PAGE(pPage) ? TDB_LPAGE_FREE_CELL_NXOFFSET(PCELL) : TDB_SPAGE_FREE_CELL_NXOFFSET(PCELL))

#define TDB_PAGE_FREE_CELL_SIZE_SET(PPAGE, PCELL, SIZE) \
  do {                                                  \
    if (TDB_IS_LARGE_PAGE(PPAGE)) {                     \
      TDB_LPAGE_FREE_CELL_SIZE_SET(PCELL, SIZE);        \
    } else {                                            \
      TDB_SPAGE_FREE_CELL_SIZE_SET(PCELL, SIZE);        \
    }                                                   \
  } while (0)
#define TDB_PAGE_FREE_CELL_NXOFFSET_SET(PPAGE, PCELL, OFFSET) \
  do {                                                        \
    if (TDB_IS_LARGE_PAGE(PPAGE)) {                           \
      TDB_LPAGE_FREE_CELL_NXOFFSET_SET(PCELL, OFFSET);        \
    } else {                                                  \
      TDB_SPAGE_FREE_CELL_NXOFFSET_SET(PCELL, OFFSET);        \
    }                                                         \
  } while (0)

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
    pPage->szOffset = 2;
    pPage->szPageHdr = sizeof(SPageHdr);
    pPage->szFreeCell = sizeof(SFreeCell);
  } else {
    pPage->szOffset = 3;
    pPage->szPageHdr = sizeof(SPageHdrL);
    pPage->szFreeCell = sizeof(SFreeCellL);
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

  if (pPage->nOverflow || szCell + pPage->szOffset > pPage->nFree) {
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
    pTmp = pPage->pCellIdx + idx * pPage->szOffset;
    memmove(pTmp + pPage->szOffset, pTmp, pPage->pFreeStart - pTmp - pPage->szOffset);
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

  ASSERT(pPage->nFree > size + pPage->szOffset);

  pCell = NULL;
  *ppCell = NULL;

  // 1. Try to allocate from the free space area
  if (pPage->pFreeEnd - pPage->pFreeStart > size + pPage->szOffset) {
    pPage->pFreeEnd -= size;
    pPage->pFreeStart += pPage->szOffset;
    pCell = pPage->pFreeEnd;
  }

  // 2. Try to allocate from the page free list
  if ((pCell == NULL) && (pPage->pFreeEnd - pPage->pFreeStart >= pPage->szOffset) && TDB_PAGE_FCELL(pPage)) {
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
  }

  // 3. Try to dfragment and allocate again
  if (pCell == NULL) {
    ret = tdbPageDefragment(pPage);
    if (ret < 0) {
      return -1;
    }

    ASSERT(pPage->pFreeEnd - pPage->pFreeStart > size + pPage->szOffset);
    ASSERT(pPage->nFree == pPage->pFreeEnd - pPage->pFreeStart);

    // Allocate from the free space area again
    pPage->pFreeEnd -= size;
    pPage->pFreeStart += pPage->szOffset;
    pCell = pPage->pFreeEnd;
  }

  ASSERT(pCell != NULL);

  pPage->nFree = pPage->nFree - size - pPage->szOffset;
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