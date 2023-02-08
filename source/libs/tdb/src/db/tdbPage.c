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

#define TDB_PAGE_HDR_SIZE(pPage)                        ((pPage)->pPageMethods->szPageHdr)
#define TDB_PAGE_FREE_CELL_SIZE(pPage)                  ((pPage)->pPageMethods->szFreeCell)
#define TDB_PAGE_NCELLS(pPage)                          (*(pPage)->pPageMethods->getCellNum)(pPage)
#define TDB_PAGE_CCELLS(pPage)                          (*(pPage)->pPageMethods->getCellBody)(pPage)
#define TDB_PAGE_FCELL(pPage)                           (*(pPage)->pPageMethods->getCellFree)(pPage)
#define TDB_PAGE_NFREE(pPage)                           (*(pPage)->pPageMethods->getFreeBytes)(pPage)
#define TDB_PAGE_CELL_OFFSET_AT(pPage, idx)             (*(pPage)->pPageMethods->getCellOffset)(pPage, idx)
#define TDB_PAGE_NCELLS_SET(pPage, NCELLS)              (*(pPage)->pPageMethods->setCellNum)(pPage, NCELLS)
#define TDB_PAGE_CCELLS_SET(pPage, CCELLS)              (*(pPage)->pPageMethods->setCellBody)(pPage, CCELLS)
#define TDB_PAGE_FCELL_SET(pPage, FCELL)                (*(pPage)->pPageMethods->setCellFree)(pPage, FCELL)
#define TDB_PAGE_NFREE_SET(pPage, NFREE)                (*(pPage)->pPageMethods->setFreeBytes)(pPage, NFREE)
#define TDB_PAGE_CELL_OFFSET_AT_SET(pPage, idx, OFFSET) (*(pPage)->pPageMethods->setCellOffset)(pPage, idx, OFFSET)
#define TDB_PAGE_CELL_AT(pPage, idx)                    ((pPage)->pData + TDB_PAGE_CELL_OFFSET_AT(pPage, idx))
#define TDB_PAGE_MAX_FREE_BLOCK(pPage, szAmHdr) \
  ((pPage)->pageSize - (szAmHdr)-TDB_PAGE_HDR_SIZE(pPage) - sizeof(SPageFtr))

static int tdbPageAllocate(SPage *pPage, int size, SCell **ppCell);
static int tdbPageDefragment(SPage *pPage);
static int tdbPageFree(SPage *pPage, int idx, SCell *pCell, int szCell);

int tdbPageCreate(int pageSize, SPage **ppPage, void *(*xMalloc)(void *, size_t), void *arg) {
  SPage *pPage;
  u8    *ptr;
  int    size;

  ASSERT(xMalloc);

  ASSERT(TDB_IS_PGSIZE_VLD(pageSize));

  *ppPage = NULL;
  size = pageSize + sizeof(*pPage);

  ptr = (u8 *)(xMalloc(arg, size));
  if (ptr == NULL) {
    return -1;
  }

  memset(ptr, 0, size);
  pPage = (SPage *)(ptr + pageSize);

  TDB_INIT_PAGE_LOCK(pPage);
  pPage->pageSize = pageSize;
  pPage->pData = ptr;
  if (pageSize < 65536) {
    pPage->pPageMethods = &pageMethods;
  } else {
    pPage->pPageMethods = &pageLargeMethods;
  }

  *ppPage = pPage;

  tdbTrace("page/create: %p/%d %p", pPage, pPage->id, xMalloc);
  return 0;
}

int tdbPageDestroy(SPage *pPage, void (*xFree)(void *arg, void *ptr), void *arg) {
  u8 *ptr;

  tdbTrace("page/destroy: %p/%d %p", pPage, pPage->id, xFree);
  ASSERT(!pPage->isDirty);
  ASSERT(xFree);

  for (int iOvfl = 0; iOvfl < pPage->nOverflow; iOvfl++) {
    tdbTrace("tdbPage/destroy/free ovfl cell: %p/%p", pPage->apOvfl[iOvfl], pPage);
    tdbOsFree(pPage->apOvfl[iOvfl]);
  }

  ptr = pPage->pData;
  xFree(arg, ptr);

  return 0;
}

void tdbPageZero(SPage *pPage, u8 szAmHdr, int (*xCellSize)(const SPage *, SCell *, int, TXN *, SBTree *pBt)) {
  tdbTrace("page/zero: %p %" PRIu8 " %p", pPage, szAmHdr, xCellSize);
  pPage->pPageHdr = pPage->pData + szAmHdr;
  TDB_PAGE_NCELLS_SET(pPage, 0);
  TDB_PAGE_CCELLS_SET(pPage, pPage->pageSize - sizeof(SPageFtr));
  TDB_PAGE_FCELL_SET(pPage, 0);
  TDB_PAGE_NFREE_SET(pPage, TDB_PAGE_MAX_FREE_BLOCK(pPage, szAmHdr));
  pPage->pCellIdx = pPage->pPageHdr + TDB_PAGE_HDR_SIZE(pPage);
  pPage->pFreeStart = pPage->pCellIdx;
  pPage->pFreeEnd = pPage->pData + TDB_PAGE_CCELLS(pPage);
  pPage->pPageFtr = (SPageFtr *)(pPage->pData + pPage->pageSize - sizeof(SPageFtr));
  pPage->nOverflow = 0;
  pPage->xCellSize = xCellSize;

  ASSERT((u8 *)pPage->pPageFtr == pPage->pFreeEnd);
}

void tdbPageInit(SPage *pPage, u8 szAmHdr, int (*xCellSize)(const SPage *, SCell *, int, TXN *, SBTree *pBt)) {
  tdbTrace("page/init: %p %" PRIu8 " %p", pPage, szAmHdr, xCellSize);
  pPage->pPageHdr = pPage->pData + szAmHdr;
  if (TDB_PAGE_NCELLS(pPage) == 0) {
    return tdbPageZero(pPage, szAmHdr, xCellSize);
  }
  pPage->pCellIdx = pPage->pPageHdr + TDB_PAGE_HDR_SIZE(pPage);
  pPage->pFreeStart = pPage->pCellIdx + TDB_PAGE_OFFSET_SIZE(pPage) * TDB_PAGE_NCELLS(pPage);
  pPage->pFreeEnd = pPage->pData + TDB_PAGE_CCELLS(pPage);
  pPage->pPageFtr = (SPageFtr *)(pPage->pData + pPage->pageSize - sizeof(SPageFtr));
  pPage->nOverflow = 0;
  pPage->xCellSize = xCellSize;

  ASSERT(pPage->pFreeEnd >= pPage->pFreeStart);
  ASSERT(pPage->pFreeEnd - pPage->pFreeStart <= TDB_PAGE_NFREE(pPage));
}

int tdbPageInsertCell(SPage *pPage, int idx, SCell *pCell, int szCell, u8 asOvfl) {
  int    nFree;
  int    nCells;
  int    iOvfl;
  int    lidx;  // local idx
  SCell *pNewCell;

  ASSERT(szCell <= TDB_PAGE_MAX_FREE_BLOCK(pPage, pPage->pPageHdr - pPage->pData));

  nFree = TDB_PAGE_NFREE(pPage);
  nCells = TDB_PAGE_NCELLS(pPage);

  for (iOvfl = 0; iOvfl < pPage->nOverflow; ++iOvfl) {
    if (pPage->aiOvfl[iOvfl] >= idx) {
      break;
    }
  }

  lidx = idx - iOvfl;

  if (asOvfl || nFree < szCell + TDB_PAGE_OFFSET_SIZE(pPage)) {
    // TODO: make it extensible
    // add the cell as an overflow cell
    for (int i = pPage->nOverflow; i > iOvfl; i--) {
      pPage->apOvfl[i] = pPage->apOvfl[i - 1];
      pPage->aiOvfl[i] = pPage->aiOvfl[i - 1];
    }

    // TODO: here has memory leak
    pNewCell = (SCell *)tdbOsMalloc(szCell);
    memcpy(pNewCell, pCell, szCell);

    tdbTrace("tdbPage/insert/new ovfl cell: %p/%p", pNewCell, pPage);

    pPage->apOvfl[iOvfl] = pNewCell;
    pPage->aiOvfl[iOvfl] = idx;
    pPage->nOverflow++;
    iOvfl++;
  } else {
    // page must has enough space to hold the cell locally
    tdbPageAllocate(pPage, szCell, &pNewCell);

    memcpy(pNewCell, pCell, szCell);

    // no overflow cell exists in this page
    u8 *src = pPage->pCellIdx + TDB_PAGE_OFFSET_SIZE(pPage) * lidx;
    u8 *dest = src + TDB_PAGE_OFFSET_SIZE(pPage);
    memmove(dest, src, pPage->pFreeStart - dest);
    TDB_PAGE_CELL_OFFSET_AT_SET(pPage, lidx, pNewCell - pPage->pData);
    TDB_PAGE_NCELLS_SET(pPage, nCells + 1);

    ASSERT(pPage->pFreeStart == pPage->pCellIdx + TDB_PAGE_OFFSET_SIZE(pPage) * (nCells + 1));
  }

  for (; iOvfl < pPage->nOverflow; iOvfl++) {
    pPage->aiOvfl[iOvfl]++;
  }

  return 0;
}

int tdbPageUpdateCell(SPage *pPage, int idx, SCell *pCell, int szCell, TXN *pTxn, SBTree *pBt) {
  tdbPageDropCell(pPage, idx, pTxn, pBt);
  return tdbPageInsertCell(pPage, idx, pCell, szCell, 0);
}

int tdbPageDropCell(SPage *pPage, int idx, TXN *pTxn, SBTree *pBt) {
  int    lidx;
  SCell *pCell;
  int    szCell;
  int    nCells;
  int    iOvfl;

  nCells = TDB_PAGE_NCELLS(pPage);

  ASSERT(idx >= 0 && idx < nCells + pPage->nOverflow);

  iOvfl = 0;
  for (; iOvfl < pPage->nOverflow; iOvfl++) {
    if (pPage->aiOvfl[iOvfl] == idx) {
      // remove the over flow cell
      tdbOsFree(pPage->apOvfl[iOvfl]);
      tdbTrace("tdbPage/drop/free ovfl cell: %p", pPage->apOvfl[iOvfl]);
      for (; (++iOvfl) < pPage->nOverflow;) {
        pPage->aiOvfl[iOvfl - 1] = pPage->aiOvfl[iOvfl] - 1;
        pPage->apOvfl[iOvfl - 1] = pPage->apOvfl[iOvfl];
      }

      pPage->nOverflow--;
      return 0;
    } else if (pPage->aiOvfl[iOvfl] > idx) {
      break;
    }
  }

  lidx = idx - iOvfl;
  pCell = TDB_PAGE_CELL_AT(pPage, lidx);
  szCell = (*pPage->xCellSize)(pPage, pCell, 1, pTxn, pBt);
  tdbPageFree(pPage, lidx, pCell, szCell);
  TDB_PAGE_NCELLS_SET(pPage, nCells - 1);

  for (; iOvfl < pPage->nOverflow; iOvfl++) {
    pPage->aiOvfl[iOvfl]--;
    ASSERT(pPage->aiOvfl[iOvfl] > 0);
  }

  return 0;
}

void tdbPageCopy(SPage *pFromPage, SPage *pToPage, int deepCopyOvfl) {
  int delta, nFree;

  pToPage->pFreeStart = pToPage->pPageHdr + (pFromPage->pFreeStart - pFromPage->pPageHdr);
  pToPage->pFreeEnd = (u8 *)(pToPage->pPageFtr) - ((u8 *)pFromPage->pPageFtr - pFromPage->pFreeEnd);

  ASSERT(pToPage->pFreeEnd >= pToPage->pFreeStart);

  memcpy(pToPage->pPageHdr, pFromPage->pPageHdr, pFromPage->pFreeStart - pFromPage->pPageHdr);
  memcpy(pToPage->pFreeEnd, pFromPage->pFreeEnd, (u8 *)pFromPage->pPageFtr - pFromPage->pFreeEnd);

  ASSERT(TDB_PAGE_CCELLS(pToPage) == pToPage->pFreeEnd - pToPage->pData);

  delta = (pToPage->pPageHdr - pToPage->pData) - (pFromPage->pPageHdr - pFromPage->pData);
  if (delta != 0) {
    nFree = TDB_PAGE_NFREE(pFromPage);
    TDB_PAGE_NFREE_SET(pToPage, nFree - delta);
  }

  // Copy the overflow cells
  for (int iOvfl = 0; iOvfl < pFromPage->nOverflow; iOvfl++) {
    SCell *pNewCell = pFromPage->apOvfl[iOvfl];
    if (deepCopyOvfl) {
      int szCell = (*pFromPage->xCellSize)(pFromPage, pFromPage->apOvfl[iOvfl], 0, NULL, NULL);
      pNewCell = (SCell *)tdbOsMalloc(szCell);
      memcpy(pNewCell, pFromPage->apOvfl[iOvfl], szCell);
      tdbTrace("tdbPage/copy/new ovfl cell: %p/%p/%p", pNewCell, pToPage, pFromPage);
    }

    pToPage->apOvfl[iOvfl] = pNewCell;
    pToPage->aiOvfl[iOvfl] = pFromPage->aiOvfl[iOvfl];
  }
  pToPage->nOverflow = pFromPage->nOverflow;
}

int tdbPageCapacity(int pageSize, int amHdrSize) {
  int szPageHdr;
  int minCellIndexSize;  // at least one cell in cell index

  if (pageSize < 65536) {
    szPageHdr = pageMethods.szPageHdr;
    minCellIndexSize = pageMethods.szOffset;
  } else {
    szPageHdr = pageLargeMethods.szPageHdr;
    minCellIndexSize = pageLargeMethods.szOffset;
  }

  return pageSize - szPageHdr - amHdrSize - sizeof(SPageFtr) - minCellIndexSize;
}

static int tdbPageAllocate(SPage *pPage, int szCell, SCell **ppCell) {
  SCell *pFreeCell;
  u8    *pOffset;
  int    nFree;
  int    ret;
  int    cellFree;
  SCell *pCell = NULL;

  *ppCell = NULL;
  nFree = TDB_PAGE_NFREE(pPage);

  ASSERT(nFree >= szCell + TDB_PAGE_OFFSET_SIZE(pPage));
  ASSERT(TDB_PAGE_CCELLS(pPage) == pPage->pFreeEnd - pPage->pData);

  // 1. Try to allocate from the free space block area
  if (pPage->pFreeEnd - pPage->pFreeStart >= szCell + TDB_PAGE_OFFSET_SIZE(pPage)) {
    pPage->pFreeEnd -= szCell;
    pCell = pPage->pFreeEnd;
    TDB_PAGE_CCELLS_SET(pPage, pPage->pFreeEnd - pPage->pData);
    goto _alloc_finish;
  }

  // 2. Try to allocate from the page free list
  cellFree = TDB_PAGE_FCELL(pPage);
  ASSERT(cellFree == 0 || cellFree >= pPage->pFreeEnd - pPage->pData);
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
        pCell = pFreeCell;

        newSize = szFreeCell - szCell;
        pFreeCell += szCell;
        if (newSize >= TDB_PAGE_FREE_CELL_SIZE(pPage)) {
          pPage->pPageMethods->setFreeCellInfo(pFreeCell, newSize, nxFreeCell);
          if (pPrevFreeCell) {
            pPage->pPageMethods->setFreeCellInfo(pPrevFreeCell, szPrevFreeCell, pFreeCell - pPage->pData);
          } else {
            TDB_PAGE_FCELL_SET(pPage, pFreeCell - pPage->pData);
          }
        } else {
          if (pPrevFreeCell) {
            pPage->pPageMethods->setFreeCellInfo(pPrevFreeCell, szPrevFreeCell, nxFreeCell);
          } else {
            TDB_PAGE_FCELL_SET(pPage, nxFreeCell);
          }
        }

        goto _alloc_finish;
      } else {
        pPrevFreeCell = pFreeCell;
        szPrevFreeCell = szFreeCell;
        cellFree = nxFreeCell;
      }
    }
  }

  // 3. Try to dfragment and allocate again
  tdbPageDefragment(pPage);
  ASSERT(pPage->pFreeEnd - pPage->pFreeStart == nFree);
  ASSERT(nFree == TDB_PAGE_NFREE(pPage));
  ASSERT(pPage->pFreeEnd - pPage->pData == TDB_PAGE_CCELLS(pPage));

  pPage->pFreeEnd -= szCell;
  pCell = pPage->pFreeEnd;
  TDB_PAGE_CCELLS_SET(pPage, pPage->pFreeEnd - pPage->pData);

_alloc_finish:
  ASSERT(pCell);
  pPage->pFreeStart += TDB_PAGE_OFFSET_SIZE(pPage);
  TDB_PAGE_NFREE_SET(pPage, nFree - szCell - TDB_PAGE_OFFSET_SIZE(pPage));
  *ppCell = pCell;
  return 0;
}

static int tdbPageFree(SPage *pPage, int idx, SCell *pCell, int szCell) {
  int nFree;
  int cellFree;
  u8 *dest;
  u8 *src;

  ASSERT(pCell >= pPage->pFreeEnd);
  ASSERT(pCell + szCell <= (u8 *)(pPage->pPageFtr));
  ASSERT(pCell == TDB_PAGE_CELL_AT(pPage, idx));

  nFree = TDB_PAGE_NFREE(pPage);

  if (pCell == pPage->pFreeEnd) {
    pPage->pFreeEnd += szCell;
    TDB_PAGE_CCELLS_SET(pPage, pPage->pFreeEnd - pPage->pData);
  } else {
    if (szCell >= TDB_PAGE_FREE_CELL_SIZE(pPage)) {
      cellFree = TDB_PAGE_FCELL(pPage);
      pPage->pPageMethods->setFreeCellInfo(pCell, szCell, cellFree);
      TDB_PAGE_FCELL_SET(pPage, pCell - pPage->pData);
    } else {
      ASSERT(0);
    }
  }

  dest = pPage->pCellIdx + TDB_PAGE_OFFSET_SIZE(pPage) * idx;
  src = dest + TDB_PAGE_OFFSET_SIZE(pPage);
  memmove(dest, src, pPage->pFreeStart - src);

  pPage->pFreeStart -= TDB_PAGE_OFFSET_SIZE(pPage);
  nFree = nFree + szCell + TDB_PAGE_OFFSET_SIZE(pPage);
  TDB_PAGE_NFREE_SET(pPage, nFree);
  return 0;
}

typedef struct {
  int32_t iCell;
  int32_t offset;
} SCellIdx;
static int32_t tCellIdxCmprFn(const void *p1, const void *p2) {
  if (((SCellIdx *)p1)->offset < ((SCellIdx *)p2)->offset) {
    return -1;
  } else if (((SCellIdx *)p1)->offset > ((SCellIdx *)p2)->offset) {
    return 1;
  } else {
    return 0;
  }
}
static int tdbPageDefragment(SPage *pPage) {
  int32_t nFree = TDB_PAGE_NFREE(pPage);
  int32_t nCell = TDB_PAGE_NCELLS(pPage);

  SCellIdx *aCellIdx = (SCellIdx *)tdbOsMalloc(sizeof(SCellIdx) * nCell);
  if (aCellIdx == NULL) return -1;
  for (int32_t iCell = 0; iCell < nCell; iCell++) {
    aCellIdx[iCell].iCell = iCell;
    aCellIdx[iCell].offset = TDB_PAGE_CELL_OFFSET_AT(pPage, iCell);
  }
  taosSort(aCellIdx, nCell, sizeof(SCellIdx), tCellIdxCmprFn);

  SCell *pNextCell = (u8 *)pPage->pPageFtr;
  for (int32_t iCell = nCell - 1; iCell >= 0; iCell--) {
    SCell  *pCell = TDB_PAGE_CELL_AT(pPage, aCellIdx[iCell].iCell);
    int32_t szCell = pPage->xCellSize(pPage, pCell, 0, NULL, NULL);

    ASSERT(pNextCell - szCell >= pCell);

    pNextCell -= szCell;
    if (pNextCell > pCell) {
      memmove(pNextCell, pCell, szCell);
      TDB_PAGE_CELL_OFFSET_AT_SET(pPage, aCellIdx[iCell].iCell, pNextCell - pPage->pData);
    }
  }
  pPage->pFreeEnd = pNextCell;
  TDB_PAGE_CCELLS_SET(pPage, pPage->pFreeEnd - pPage->pData);
  TDB_PAGE_FCELL_SET(pPage, 0);
  tdbOsFree(aCellIdx);

  ASSERT(pPage->pFreeEnd - pPage->pFreeStart == nFree);

  return 0;
}

/* ---------------------------------------------------------------------------------------------------------- */

#pragma pack(push, 1)
typedef struct {
  u16 cellNum;
  u16 cellBody;
  u16 cellFree;
  u16 nFree;
} SPageHdr;

typedef struct {
  u16 szCell;
  u16 nxOffset;
} SFreeCell;
#pragma pack(pop)

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

#pragma pack(push, 1)
typedef struct {
  u8 cellNum[3];
  u8 cellBody[3];
  u8 cellFree[3];
  u8 nFree[3];
} SPageHdrL;

typedef struct {
  u8 szCell[3];
  u8 nxOffset[3];
} SFreeCellL;
#pragma pack(pop)

// cellNum
static inline int  getLPageCellNum(SPage *pPage) { return TDB_GET_U24(((SPageHdrL *)(pPage->pPageHdr))[0].cellNum); }
static inline void setLPageCellNum(SPage *pPage, int cellNum) {
  TDB_PUT_U24(((SPageHdrL *)(pPage->pPageHdr))[0].cellNum, cellNum);
}

// cellBody
static inline int  getLPageCellBody(SPage *pPage) { return TDB_GET_U24(((SPageHdrL *)(pPage->pPageHdr))[0].cellBody); }
static inline void setLPageCellBody(SPage *pPage, int cellBody) {
  TDB_PUT_U24(((SPageHdrL *)(pPage->pPageHdr))[0].cellBody, cellBody);
}

// cellFree
static inline int  getLPageCellFree(SPage *pPage) { return TDB_GET_U24(((SPageHdrL *)(pPage->pPageHdr))[0].cellFree); }
static inline void setLPageCellFree(SPage *pPage, int cellFree) {
  TDB_PUT_U24(((SPageHdrL *)(pPage->pPageHdr))[0].cellFree, cellFree);
}

// nFree
static inline int  getLPageNFree(SPage *pPage) { return TDB_GET_U24(((SPageHdrL *)(pPage->pPageHdr))[0].nFree); }
static inline void setLPageNFree(SPage *pPage, int nFree) {
  TDB_PUT_U24(((SPageHdrL *)(pPage->pPageHdr))[0].nFree, nFree);
}

// cell offset
static inline int getLPageCellOffset(SPage *pPage, int idx) {
  ASSERT(idx >= 0 && idx < getLPageCellNum(pPage));
  return TDB_GET_U24(pPage->pCellIdx + 3 * idx);
}

static inline void setLPageCellOffset(SPage *pPage, int idx, int offset) {
  TDB_PUT_U24(pPage->pCellIdx + 3 * idx, offset);
}

// free cell info
static inline void getLPageFreeCellInfo(SCell *pCell, int *szCell, int *nxOffset) {
  SFreeCellL *pFreeCell = (SFreeCellL *)pCell;
  *szCell = TDB_GET_U24(pFreeCell->szCell);
  *nxOffset = TDB_GET_U24(pFreeCell->nxOffset);
}

static inline void setLPageFreeCellInfo(SCell *pCell, int szCell, int nxOffset) {
  SFreeCellL *pFreeCell = (SFreeCellL *)pCell;
  TDB_PUT_U24(pFreeCell->szCell, szCell);
  TDB_PUT_U24(pFreeCell->nxOffset, nxOffset);
}

SPageMethods pageLargeMethods = {
    3,                     // szOffset
    sizeof(SPageHdrL),     // szPageHdr
    sizeof(SFreeCellL),    // szFreeCell
    getLPageCellNum,       // getCellNum
    setLPageCellNum,       // setCellNum
    getLPageCellBody,      // getCellBody
    setLPageCellBody,      // setCellBody
    getLPageCellFree,      // getCellFree
    setLPageCellFree,      // setCellFree
    getLPageNFree,         // getFreeBytes
    setLPageNFree,         // setFreeBytes
    getLPageCellOffset,    // getCellOffset
    setLPageCellOffset,    // setCellOffset
    getLPageFreeCellInfo,  // getFreeCellInfo
    setLPageFreeCellInfo   // setFreeCellInfo
};
