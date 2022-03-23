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

#define TDB_BTREE_ROOT 0x1
#define TDB_BTREE_LEAF 0x2

#define TDB_BTREE_PAGE_IS_ROOT(flags) TDB_FLAG_HAS(flags, TDB_BTREE_ROOT)
#define TDB_BTREE_PAGE_IS_LEAF(flags) TDB_FLAG_HAS(flags, TDB_BTREE_LEAF)
#define TDB_BTREE_ASSERT_FLAG(flags)                                                 \
  ASSERT(TDB_FLAG_IS(flags, TDB_BTREE_ROOT) || TDB_FLAG_IS(flags, TDB_BTREE_LEAF) || \
         TDB_FLAG_IS(flags, TDB_BTREE_ROOT | TDB_BTREE_LEAF) || TDB_FLAG_IS(flags, 0))

struct SBTree {
  SPgno          root;
  int            keyLen;
  int            valLen;
  SPager        *pPager;
  FKeyComparator kcmpr;
  u8             fanout;
  int            pageSize;
  int            maxLocal;
  int            minLocal;
  int            maxLeaf;
  int            minLeaf;
  u8            *pTmp;
};

#define TDB_BTREE_PAGE_COMMON_HDR u8 flags;

#define TDB_BTREE_PAGE_GET_FLAGS(PAGE)        (PAGE)->pData[0]
#define TDB_BTREE_PAGE_SET_FLAGS(PAGE, flags) ((PAGE)->pData[0] = (flags))

typedef struct __attribute__((__packed__)) {
  TDB_BTREE_PAGE_COMMON_HDR
} SLeafHdr;

typedef struct __attribute__((__packed__)) {
  TDB_BTREE_PAGE_COMMON_HDR;
  SPgno pgno;  // right-most child
} SIntHdr;

typedef struct {
  u8      flags;
  SBTree *pBt;
} SBtreeInitPageArg;

typedef struct {
  int   kLen;
  u8   *pKey;
  int   vLen;
  u8   *pVal;
  SPgno pgno;
  u8   *pTmpSpace;
} SCellDecoder;

static int tdbBtCursorMoveTo(SBtCursor *pCur, const void *pKey, int kLen, int *pCRst);
static int tdbDefaultKeyCmprFn(const void *pKey1, int keyLen1, const void *pKey2, int keyLen2);
static int tdbBtreeOpenImpl(SBTree *pBt);
static int tdbBtreeZeroPage(SPage *pPage, void *arg);
static int tdbBtreeInitPage(SPage *pPage, void *arg);
static int tdbBtreeEncodeCell(SPage *pPage, const void *pKey, int kLen, const void *pVal, int vLen, SCell *pCell,
                              int *szCell);
static int tdbBtreeDecodeCell(SPage *pPage, const SCell *pCell, SCellDecoder *pDecoder);
static int tdbBtreeBalance(SBtCursor *pCur);
static int tdbBtreeCellSize(const SPage *pPage, SCell *pCell);

int tdbBtreeOpen(int keyLen, int valLen, SPager *pPager, FKeyComparator kcmpr, SBTree **ppBt) {
  SBTree *pBt;
  int     ret;

  *ppBt = NULL;

  pBt = (SBTree *)calloc(1, sizeof(*pBt));
  if (pBt == NULL) {
    return -1;
  }

  // pBt->keyLen
  pBt->keyLen = keyLen;
  // pBt->valLen
  pBt->valLen = valLen;
  // pBt->pPager
  pBt->pPager = pPager;
  // pBt->kcmpr
  pBt->kcmpr = kcmpr ? kcmpr : tdbDefaultKeyCmprFn;
  // pBt->fanout
  if (keyLen == TDB_VARIANT_LEN) {
    pBt->fanout = TDB_DEFAULT_FANOUT;
  } else {
    ASSERT(0);
    // TODO: pBt->fanout = 0;
  }
  // pBt->pageSize
  pBt->pageSize = tdbPagerGetPageSize(pPager);
  // pBt->maxLocal
  pBt->maxLocal = (pBt->pageSize - 14) / pBt->fanout;
  // pBt->minLocal: Should not be allowed smaller than 15, which is [nPayload][nKey][nData]
  pBt->minLocal = (pBt->pageSize - 14) / pBt->fanout / 2;
  // pBt->maxLeaf
  pBt->maxLeaf = pBt->pageSize - 14;
  // pBt->minLeaf
  pBt->minLeaf = pBt->minLocal;

  // TODO: pBt->root
  ret = tdbBtreeOpenImpl(pBt);
  if (ret < 0) {
    free(pBt);
    return -1;
  }

  *ppBt = pBt;
  return 0;
}

int tdbBtreeClose(SBTree *pBt) {
  // TODO
  return 0;
}

int tdbBtreeCursor(SBtCursor *pCur, SBTree *pBt) {
  pCur->pBt = pBt;
  pCur->iPage = -1;
  pCur->pPage = NULL;
  pCur->idx = -1;

  return 0;
}

int tdbBtCursorInsert(SBtCursor *pCur, const void *pKey, int kLen, const void *pVal, int vLen) {
  int     ret;
  int     idx;
  SPager *pPager;
  SCell  *pCell;
  int     szCell;
  int     cret;
  SBTree *pBt;

  ret = tdbBtCursorMoveTo(pCur, pKey, kLen, &cret);
  if (ret < 0) {
    // TODO: handle error
    return -1;
  }

  if (pCur->idx == -1) {
    ASSERT(TDB_PAGE_TOTAL_CELLS(pCur->pPage) == 0);
    idx = 0;
  } else {
    if (cret > 0) {
      idx = pCur->idx + 1;
    } else if (cret < 0) {
      idx = pCur->idx;
    } else {
      /* TODO */
      ASSERT(0);
    }
  }

  // TODO: refact code here
  pBt = pCur->pBt;
  if (!pBt->pTmp) {
    pBt->pTmp = (u8 *)malloc(pBt->pageSize);
    if (pBt->pTmp == NULL) {
      return -1;
    }
  }

  pCell = pBt->pTmp;

  // Encode the cell
  ret = tdbBtreeEncodeCell(pCur->pPage, pKey, kLen, pVal, vLen, pCell, &szCell);
  if (ret < 0) {
    return -1;
  }

  // Insert the cell to the index
  ret = tdbPageInsertCell(pCur->pPage, idx, pCell, szCell, 0);
  if (ret < 0) {
    return -1;
  }

  // If page is overflow, balance the tree
  if (pCur->pPage->nOverflow > 0) {
    ret = tdbBtreeBalance(pCur);
    if (ret < 0) {
      return -1;
    }
  }

  return 0;
}

static int tdbBtCursorMoveToChild(SBtCursor *pCur, SPgno pgno) {
  int ret;

  pCur->pgStack[pCur->iPage] = pCur->pPage;
  pCur->idxStack[pCur->iPage] = pCur->idx;
  pCur->iPage++;
  pCur->pPage = NULL;
  pCur->idx = -1;

  ret = tdbPagerFetchPage(pCur->pBt->pPager, pgno, &pCur->pPage, tdbBtreeInitPage, pCur->pBt);
  if (ret < 0) {
    ASSERT(0);
  }

  return 0;
}

static int tdbBtCursorMoveTo(SBtCursor *pCur, const void *pKey, int kLen, int *pCRst) {
  int     ret;
  SBTree *pBt;
  SPager *pPager;

  pBt = pCur->pBt;
  pPager = pBt->pPager;

  if (pCur->iPage < 0) {
    ASSERT(pCur->iPage == -1);
    ASSERT(pCur->idx == -1);

    // Move from the root
    ret = tdbPagerFetchPage(pPager, pBt->root, &(pCur->pPage), tdbBtreeInitPage, pBt);
    if (ret < 0) {
      ASSERT(0);
      return -1;
    }

    pCur->iPage = 0;

    if (TDB_PAGE_TOTAL_CELLS(pCur->pPage) == 0) {
      // Current page is empty
      // ASSERT(TDB_FLAG_IS(TDB_PAGE_FLAGS(pCur->pPage), TDB_BTREE_ROOT | TDB_BTREE_LEAF));
      return 0;
    }

    for (;;) {
      int          lidx, ridx, midx, c, nCells;
      SCell       *pCell;
      SPage       *pPage;
      SCellDecoder cd = {0};

      pPage = pCur->pPage;
      nCells = TDB_PAGE_TOTAL_CELLS(pPage);
      lidx = 0;
      ridx = nCells - 1;

      ASSERT(nCells > 0);

      for (;;) {
        if (lidx > ridx) break;

        midx = (lidx + ridx) >> 1;

        pCell = tdbPageGetCell(pPage, midx);
        ret = tdbBtreeDecodeCell(pPage, pCell, &cd);
        if (ret < 0) {
          // TODO: handle error
          ASSERT(0);
          return -1;
        }

        // Compare the key values
        c = pBt->kcmpr(pKey, kLen, cd.pKey, cd.kLen);
        if (c < 0) {
          /* input-key < cell-key */
          ridx = midx - 1;
        } else if (c > 0) {
          /* input-key > cell-key */
          lidx = midx + 1;
        } else {
          /* input-key == cell-key */
          break;
        }
      }

      // Move downward or break
      u8 flags = TDB_BTREE_PAGE_GET_FLAGS(pPage);
      u8 leaf = TDB_BTREE_PAGE_IS_LEAF(flags);
      if (leaf) {
        pCur->idx = midx;
        *pCRst = c;
        break;
      } else {
        if (c <= 0) {
          pCur->idx = midx;
          tdbBtCursorMoveToChild(pCur, cd.pgno);
        } else {
          pCur->idx = midx + 1;
          if (midx == nCells - 1) {
            /* Move to right-most child */
            tdbBtCursorMoveToChild(pCur, ((SIntHdr *)pCur->pPage->pData)->pgno);
          } else {
            pCell = tdbPageGetCell(pPage, pCur->idx);
            tdbBtreeDecodeCell(pPage, pCell, &cd);
            tdbBtCursorMoveToChild(pCur, cd.pgno);
          }
        }
      }
    }

  } else {
    // TODO: Move the cursor from a some position instead of a clear state
    ASSERT(0);
  }

  return 0;
}

static int tdbDefaultKeyCmprFn(const void *pKey1, int keyLen1, const void *pKey2, int keyLen2) {
  int mlen;
  int cret;

  ASSERT(keyLen1 > 0 && keyLen2 > 0 && pKey1 != NULL && pKey2 != NULL);

  mlen = keyLen1 < keyLen2 ? keyLen1 : keyLen2;
  cret = memcmp(pKey1, pKey2, mlen);
  if (cret == 0) {
    if (keyLen1 < keyLen2) {
      cret = -1;
    } else if (keyLen1 > keyLen2) {
      cret = 1;
    } else {
      cret = 0;
    }
  }
  return cret;
}

static int tdbBtreeOpenImpl(SBTree *pBt) {
  // Try to get the root page of the an existing btree

  SPgno  pgno;
  SPage *pPage;
  int    ret;

  {
    // 1. TODO: Search the main DB to check if the DB exists
    pgno = 0;
  }

  if (pgno != 0) {
    pBt->root = pgno;
    return 0;
  }

  // Try to create a new database
  SBtreeInitPageArg zArg = {.flags = TDB_BTREE_ROOT | TDB_BTREE_LEAF, .pBt = pBt};
  ret = tdbPagerNewPage(pBt->pPager, &pgno, &pPage, tdbBtreeZeroPage, &zArg);
  if (ret < 0) {
    return -1;
  }

  // TODO: Unref the page

  ASSERT(pgno != 0);
  pBt->root = pgno;

  return 0;
}

static int tdbBtreeInitPage(SPage *pPage, void *arg) {
  SBTree *pBt;
  u8      flags;
  u8      isLeaf;

  pBt = (SBTree *)arg;
  flags = TDB_BTREE_PAGE_GET_FLAGS(pPage);
  isLeaf = TDB_BTREE_PAGE_IS_LEAF(flags);

  ASSERT(flags == TDB_BTREE_PAGE_GET_FLAGS(pPage));

  tdbPageInit(pPage, isLeaf ? sizeof(SLeafHdr) : sizeof(SIntHdr), tdbBtreeCellSize);

  TDB_BTREE_ASSERT_FLAG(flags);

  if (isLeaf) {
    pPage->kLen = pBt->keyLen;
    pPage->vLen = pBt->valLen;
    pPage->maxLocal = pBt->maxLeaf;
    pPage->minLocal = pBt->minLeaf;
  } else {
    pPage->kLen = pBt->keyLen;
    pPage->vLen = sizeof(SPgno);
    pPage->maxLocal = pBt->maxLocal;
    pPage->minLocal = pBt->minLocal;
  }

  return 0;
}

static int tdbBtreeZeroPage(SPage *pPage, void *arg) {
  u8      flags;
  SBTree *pBt;
  u8      isLeaf;

  flags = ((SBtreeInitPageArg *)arg)->flags;
  pBt = ((SBtreeInitPageArg *)arg)->pBt;
  isLeaf = TDB_BTREE_PAGE_IS_LEAF(flags);

  tdbPageZero(pPage, isLeaf ? sizeof(SLeafHdr) : sizeof(SIntHdr), tdbBtreeCellSize);

  if (isLeaf) {
    SLeafHdr *pLeafHdr = (SLeafHdr *)(pPage->pData);
    pLeafHdr->flags = flags;

    pPage->kLen = pBt->keyLen;
    pPage->vLen = pBt->valLen;
    pPage->maxLocal = pBt->maxLeaf;
    pPage->minLocal = pBt->minLeaf;
  } else {
    SIntHdr *pIntHdr = (SIntHdr *)(pPage->pData);
    pIntHdr->flags = flags;
    pIntHdr->pgno = 0;

    pPage->kLen = pBt->keyLen;
    pPage->vLen = sizeof(SPgno);
    pPage->maxLocal = pBt->maxLocal;
    pPage->minLocal = pBt->minLocal;
  }

  return 0;
}

#ifndef TDB_BTREE_BALANCE
typedef struct {
  SBTree *pBt;
  SPage  *pParent;
  int     idx;
  i8      nOld;
  SPage  *pOldPages[3];
  i8      nNewPages;
  SPage  *pNewPages[5];
} SBtreeBalanceHelper;

static int tdbBtreeBalanceDeeper(SBTree *pBt, SPage *pRoot, SPage **ppChild) {
  SPager           *pPager;
  SPage            *pChild;
  SPgno             pgnoChild;
  int               ret;
  u8                flags;
  SIntHdr          *pIntHdr;
  SBtreeInitPageArg zArg;

  pPager = pRoot->pPager;
  flags = TDB_BTREE_PAGE_GET_FLAGS(pRoot);

  // Allocate a new child page
  zArg.flags = TDB_FLAG_REMOVE(flags, TDB_BTREE_ROOT);
  zArg.pBt = pBt;
  ret = tdbPagerNewPage(pPager, &pgnoChild, &pChild, tdbBtreeZeroPage, &zArg);
  if (ret < 0) {
    return -1;
  }

  // Copy the root page content to the child page
  tdbPageCopy(pRoot, pChild);

  // Reinitialize the root page
  zArg.flags = TDB_BTREE_ROOT;
  zArg.pBt = pBt;
  ret = tdbBtreeZeroPage(pRoot, &zArg);
  if (ret < 0) {
    return -1;
  }

  pIntHdr = (SIntHdr *)(pRoot->pData);
  pIntHdr->pgno = pgnoChild;

  *ppChild = pChild;
  return 0;
}

static int tdbBtreeBalanceNonRoot(SBTree *pBt, SPage *pParent, int idx) {
  int ret;

  int    nOlds;
  SPage *pOlds[3];
  SCell *pDivCell[2] = {0};
  int    szDivCell[2];
  int    sIdx;
  u8     childNotLeaf;

  {  // Find 3 child pages at most to do balance
    int    nCells = TDB_PAGE_TOTAL_CELLS(pParent);
    SCell *pCell;

    if (nCells <= 2) {
      sIdx = 0;
      nOlds = nCells + 1;
    } else {
      // has more than three child pages
      if (idx == 0) {
        sIdx = 0;
      } else if (idx == nCells) {
        sIdx = idx - 2;
      } else {
        sIdx = idx - 1;
      }
      nOlds = 3;
    }
    for (int i = 0; i < nOlds; i++) {
      ASSERT(sIdx + i <= nCells);

      SPgno pgno;
      if (sIdx + i == nCells) {
        ASSERT(!TDB_BTREE_PAGE_IS_LEAF(TDB_BTREE_PAGE_GET_FLAGS(pParent)));
        pgno = ((SIntHdr *)(pParent->pData))->pgno;
      } else {
        pCell = tdbPageGetCell(pParent, sIdx + i);
        pgno = *(SPgno *)pCell;
      }

      ret = tdbPagerFetchPage(pBt->pPager, pgno, pOlds + i, tdbBtreeInitPage, pBt);
      if (ret < 0) {
        ASSERT(0);
        return -1;
      }
    }
    // copy the parent key out if child pages are not leaf page
    childNotLeaf = !TDB_BTREE_PAGE_IS_LEAF(TDB_BTREE_PAGE_GET_FLAGS(pOlds[0]));
    if (childNotLeaf) {
      for (int i = 0; i < nOlds - 1; i++) {
        pCell = tdbPageGetCell(pParent, sIdx + i);

        szDivCell[i] = tdbBtreeCellSize(pParent, pCell);
        pDivCell[i] = malloc(szDivCell[i]);
        memcpy(pDivCell, pCell, szDivCell[i]);

        ((SPgno *)pDivCell)[0] = ((SIntHdr *)pOlds[i]->pData)->pgno;
        ((SIntHdr *)pOlds[i]->pData)->pgno = 0;

        // here we insert the cell as an overflow cell to avoid
        // the slow defragment process
        tdbPageInsertCell(pOlds[i], TDB_PAGE_TOTAL_CELLS(pOlds[i]), pDivCell[i], szDivCell[i], 1);
      }
    }
    // drop the cells on parent page
    for (int i = 0; i < nOlds; i++) {
      nCells = TDB_PAGE_TOTAL_CELLS(pParent);
      if (sIdx < nCells) {
        tdbPageDropCell(pParent, sIdx);
      } else {
        ((SIntHdr *)pParent->pData)->pgno = 0;
      }
    }
  }

  int nNews = 0;
  struct {
    int    cnt;
    int    size;
    SPage *pPage;
    int    oIdx;
  } infoNews[5] = {0};

  {  // Get how many new pages are needed and the new distribution

    // first loop to find minimum number of pages needed
    for (int oPage = 0; oPage < nOlds; oPage++) {
      SPage *pPage = pOlds[oPage];
      SCell *pCell;
      int    cellBytes;
      int    oIdx;

      for (oIdx = 0; oIdx < TDB_PAGE_TOTAL_CELLS(pPage); oIdx++) {
        pCell = tdbPageGetCell(pPage, oIdx);
        cellBytes = TDB_BYTES_CELL_TAKEN(pPage, pCell);

        if (infoNews[nNews].size + cellBytes > TDB_PAGE_USABLE_SIZE(pPage)) {
          // page is full, use a new page
          nNews++;

          ASSERT(infoNews[nNews].size + cellBytes <= TDB_PAGE_USABLE_SIZE(pPage));

          if (childNotLeaf) {
            // for non-child page, this cell is used as the right-most child,
            // the divider cell to parent as well
            continue;
          }
        }
        infoNews[nNews].cnt++;
        infoNews[nNews].size += cellBytes;
        infoNews[nNews].pPage = pPage;
        infoNews[nNews].oIdx = oIdx;
      }
    }

    nNews++;

    // back loop to make the distribution even
    for (int iNew = nNews - 1; iNew > 0; iNew--) {
      SCell *pCell;
      int    szLCell, szRCell;

      for (;;) {
        pCell = tdbPageGetCell(infoNews[iNew - 1].pPage, infoNews[iNew - 1].oIdx);

        if (childNotLeaf) {
          szLCell = szRCell = tdbBtreeCellSize(infoNews[iNew - 1].pPage, pCell);
        } else {
          szLCell = tdbBtreeCellSize(infoNews[iNew - 1].pPage, pCell);

          SPage *pPage = infoNews[iNew - 1].pPage;
          int    oIdx = infoNews[iNew - 1].oIdx + 1;
          for (;;) {
            if (oIdx < TDB_PAGE_TOTAL_CELLS(pPage)) {
              break;
            }

            pPage++;
            oIdx = 0;
          }

          pCell = tdbPageGetCell(pPage, oIdx);
          szRCell = tdbBtreeCellSize(pPage, pCell);
        }

        ASSERT(infoNews[iNew - 1].cnt > 0);

        if (infoNews[iNew].size + szRCell >= infoNews[iNew - 1].size - szRCell) {
          break;
        }

        // Move a cell right forward
        infoNews[iNew - 1].cnt--;
        infoNews[iNew - 1].size -= szLCell;
        infoNews[iNew - 1].oIdx--;
        for (;;) {
          if (infoNews[iNew - 1].oIdx >= 0) {
            break;
          }

          infoNews[iNew - 1].pPage--;
          infoNews[iNew - 1].oIdx = TDB_PAGE_TOTAL_CELLS(infoNews[iNew - 1].pPage) - 1;
        }

        infoNews[iNew].cnt++;
        infoNews[iNew].size += szRCell;
      }
    }
  }

  SPage *pNews[5] = {0};
  {  // Allocate new pages, reuse the old page when possible

    SPgno             pgno;
    SBtreeInitPageArg iarg;
    u8                flags;

    flags = TDB_BTREE_PAGE_GET_FLAGS(pOlds[0]);

    for (int iNew = 0; iNew < nNews; iNew++) {
      if (iNew < nOlds) {
        pNews[iNew] = pOlds[iNew];
      } else {
        iarg.pBt = pBt;
        iarg.flags = flags;
        ret = tdbPagerNewPage(pBt->pPager, &pgno, pNews + iNew, tdbBtreeZeroPage, &iarg);
        if (ret < 0) {
          ASSERT(0);
        }
      }
    }

    // TODO: sort the page according to the page number
  }

  {  // Do the real cell distribution
    SPage            *pOldsCopy[3];
    SCell            *pCell;
    int               szCell;
    SBtreeInitPageArg iarg;
    int               iNew, nNewCells;

    iarg.pBt = pBt;
    iarg.flags = TDB_BTREE_PAGE_GET_FLAGS(pOlds[0]);
    for (int i = 0; i < nOlds; i++) {
      tdbPageCreate(pOlds[0]->pageSize, pOldsCopy + i, NULL, NULL);
      tdbBtreeZeroPage(pOldsCopy[i], &iarg);
      tdbPageCopy(pOlds[i], pOldsCopy[i]);
    }
    iNew = 0;
    nNewCells = 0;

    for (int iOld = 0; iOld < nOlds; iOld++) {
      SPage *pPage;

      pPage = pOldsCopy[iOld];

      for (int oIdx = 0; oIdx < TDB_PAGE_TOTAL_CELLS(pPage); oIdx++) {
        pCell = tdbPageGetCell(pPage, oIdx);
        szCell = tdbBtreeCellSize(pPage, pCell);

        ASSERT(nNewCells <= infoNews[iNew].cnt);

        if (nNewCells < infoNews[iNew].cnt) {
          tdbPageInsertCell(pNews[iNew], nNewCells, pCell, szCell, 0);
          nNewCells++;
        } else {
          if (childNotLeaf) {
            // Insert to parent

            // set current new page right-most child
            ((SIntHdr *)pNews[iNew]->pData)->pgno = ((SPgno *)pCell)[0];

            // move to next new page
            iNew++;
            nNewCells = 0;
          } else {
            // TODO: Insert to parent max key and current page number or the right most child

            // move to next new page
            iNew++;
            nNewCells = 0;

            // insert the cell to the new page
            ASSERT(nNewCells < infoNews[iNew].cnt);
            tdbPageInsertCell(pNews[iNew], nNewCells, pCell, szCell, 0);
            nNewCells++;
          }
        }
      }
    }

    for (int i = 0; i < nOlds; i++) {
      tdbPageDestroy(pOldsCopy[i], NULL, NULL);
    }
  }

  return 0;
}

static int tdbBtreeBalance(SBtCursor *pCur) {
  int    iPage;
  SPage *pParent;
  SPage *pPage;
  int    ret;
  u8     flags;
  u8     leaf;
  u8     root;

  // Main loop to balance the BTree
  for (;;) {
    iPage = pCur->iPage;
    pPage = pCur->pPage;
    flags = TDB_BTREE_PAGE_GET_FLAGS(pPage);
    leaf = TDB_BTREE_PAGE_IS_LEAF(flags);
    root = TDB_BTREE_PAGE_IS_ROOT(flags);

    // when the page is not overflow and not too empty, the balance work
    // is finished. Just break out the balance loop.
    if (pPage->nOverflow == 0 /* TODO: && pPage->nFree <= */) {
      break;
    }

    if (iPage == 0) {
      // For the root page, only balance when the page is overfull,
      // ignore the case of empty
      if (pPage->nOverflow == 0) break;

      ret = tdbBtreeBalanceDeeper(pCur->pBt, pPage, &(pCur->pgStack[1]));
      if (ret < 0) {
        return -1;
      }

      pCur->idx = 0;
      pCur->idxStack[0] = 0;
      pCur->pgStack[0] = pCur->pPage;
      pCur->iPage = 1;
      pCur->pPage = pCur->pgStack[1];
    } else {
      // Generalized balance step
      pParent = pCur->pgStack[iPage - 1];

      ret = tdbBtreeBalanceNonRoot(pCur->pBt, pParent, pCur->idxStack[pCur->iPage - 1]);
      if (ret < 0) {
        return -1;
      }

      pCur->iPage--;
      pCur->pPage = pCur->pgStack[pCur->iPage];
    }
  }

  return 0;
}
#endif

#ifndef TDB_BTREE_CELL  // =========================================================
static int tdbBtreeEncodePayload(SPage *pPage, u8 *pPayload, const void *pKey, int kLen, const void *pVal, int vLen,
                                 int *szPayload) {
  int nPayload;

  ASSERT(pKey != NULL);

  if (pVal == NULL) {
    vLen = 0;
  }

  nPayload = kLen + vLen;
  if (nPayload <= pPage->maxLocal) {
    // General case without overflow
    memcpy(pPayload, pKey, kLen);
    if (pVal) {
      memcpy(pPayload + kLen, pVal, vLen);
    }

    *szPayload = nPayload;
    return 0;
  }

  {
    // TODO: handle overflow case
    ASSERT(0);
  }

  return 0;
}

static int tdbBtreeEncodeCell(SPage *pPage, const void *pKey, int kLen, const void *pVal, int vLen, SCell *pCell,
                              int *szCell) {
  u8  flags;
  u8  leaf;
  int nHeader;
  int nPayload;
  int ret;

  ASSERT(pPage->kLen == TDB_VARIANT_LEN || pPage->kLen == kLen);
  ASSERT(pPage->vLen == TDB_VARIANT_LEN || pPage->vLen == vLen);

  nPayload = 0;
  nHeader = 0;
  flags = TDB_BTREE_PAGE_GET_FLAGS(pPage);
  leaf = TDB_BTREE_PAGE_IS_LEAF(flags);

  // 1. Encode Header part
  /* Encode SPgno if interior page */
  if (!leaf) {
    ASSERT(pPage->vLen == sizeof(SPgno));

    ((SPgno *)(pCell + nHeader))[0] = ((SPgno *)pVal)[0];
    nHeader = nHeader + sizeof(SPgno);
  }

  /* Encode kLen if need */
  if (pPage->kLen == TDB_VARIANT_LEN) {
    nHeader += tdbPutVarInt(pCell + nHeader, kLen);
  }

  /* Encode vLen if need */
  if (pPage->vLen == TDB_VARIANT_LEN) {
    nHeader += tdbPutVarInt(pCell + nHeader, vLen);
  }

  // 2. Encode payload part
  if (leaf) {
    ret = tdbBtreeEncodePayload(pPage, pCell + nHeader, pKey, kLen, pVal, vLen, &nPayload);
  } else {
    ret = tdbBtreeEncodePayload(pPage, pCell + nHeader, pKey, kLen, NULL, 0, &nPayload);
  }
  if (ret < 0) {
    // TODO: handle error
    return -1;
  }

  *szCell = nHeader + nPayload;
  return 0;
}

static int tdbBtreeDecodePayload(SPage *pPage, const u8 *pPayload, SCellDecoder *pDecoder) {
  int nPayload;

  ASSERT(pDecoder->pKey == NULL);

  if (pDecoder->pVal) {
    nPayload = pDecoder->kLen + pDecoder->vLen;
  } else {
    nPayload = pDecoder->kLen;
  }

  if (nPayload <= pPage->maxLocal) {
    // General case without overflow
    pDecoder->pKey = (void *)pPayload;
    if (!pDecoder->pVal) {
      pDecoder->pVal = (void *)(pPayload + pDecoder->kLen);
    }
  } else {
    // TODO: handle overflow case
    ASSERT(0);
  }

  return 0;
}

static int tdbBtreeDecodeCell(SPage *pPage, const SCell *pCell, SCellDecoder *pDecoder) {
  u8  flags;
  u8  leaf;
  int nHeader;
  int ret;

  nHeader = 0;
  flags = TDB_BTREE_PAGE_GET_FLAGS(pPage);
  leaf = TDB_BTREE_PAGE_IS_LEAF(flags);

  // Clear the state of decoder
  pDecoder->kLen = -1;
  pDecoder->pKey = NULL;
  pDecoder->vLen = -1;
  pDecoder->pVal = NULL;
  pDecoder->pgno = 0;

  // 1. Decode header part
  if (!leaf) {
    ASSERT(pPage->vLen == sizeof(SPgno));

    pDecoder->pgno = ((SPgno *)(pCell + nHeader))[0];
    pDecoder->pVal = (u8 *)(&(pDecoder->pgno));
    nHeader = nHeader + sizeof(SPgno);
  }

  if (pPage->kLen == TDB_VARIANT_LEN) {
    nHeader += tdbGetVarInt(pCell + nHeader, &(pDecoder->kLen));
  } else {
    pDecoder->kLen = pPage->kLen;
  }

  if (pPage->vLen == TDB_VARIANT_LEN) {
    nHeader += tdbGetVarInt(pCell + nHeader, &(pDecoder->vLen));
  } else {
    pDecoder->vLen = pPage->vLen;
  }

  // 2. Decode payload part
  ret = tdbBtreeDecodePayload(pPage, pCell + nHeader, pDecoder);
  if (ret < 0) {
    return -1;
  }

  return 0;
}

static int tdbBtreeCellSize(const SPage *pPage, SCell *pCell) {
  u8  flags;
  u8  isLeaf;
  int szCell;
  int kLen = 0, vLen = 0;

  flags = TDB_BTREE_PAGE_GET_FLAGS(pPage);
  isLeaf = TDB_BTREE_PAGE_IS_LEAF(flags);
  szCell = 0;

  if (!isLeaf) {
    szCell += sizeof(SPgno);
  }

  if (pPage->kLen == TDB_VARIANT_LEN) {
    szCell += tdbGetVarInt(pCell + szCell, &kLen);
  } else {
    kLen = pPage->kLen;
  }

  if (isLeaf) {
    if (pPage->vLen == TDB_VARIANT_LEN) {
      szCell += tdbGetVarInt(pCell + szCell, &vLen);
    } else {
      vLen = pPage->vLen;
    }
  }

  szCell = szCell + kLen + vLen;

  return szCell;
}

#endif
