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
  u16 size;
  u16 nOffset;
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
    pPage->szOffset = 2;
    pPage->szPageHdr = sizeof(SPageHdr);
  } else {
    pPage->szOffset = 3;
    pPage->szPageHdr = sizeof(SLPageHdr);
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

  if (pPage->nOverflow || szCell + pPage->szOffset > pPage->nFree) {
    // TODO: Page is full
  } else {
    ret = tdbPageAllocate(pPage, szCell, &pTarget);
    if (ret < 0) {
      return -1;
    }

    memcpy(pTarget, pCell, szCell);
    // TODO: memmove();
    // pPage->pPaggHdr->nCells++;
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
  if (pCell == NULL && TDB_PAGE_FCELL(pPage)) {
    pCell = pPage->pData + TDB_PAGE_FCELL(pPage);
    for (;;) {
      pFreeCell = (SFreeCell *)pCell;

      if (pFreeCell->size >= size) {
        break;
      }

      if (pFreeCell->nOffset) {
        pCell = pPage->pData + pFreeCell->nOffset;
      } else {
        pCell = NULL;
        break;
      }

      continue;
    }
  }

  // 3. Try to dfragment
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
  return 0;
}