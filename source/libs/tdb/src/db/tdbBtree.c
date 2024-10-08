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
#define TDB_BTREE_OVFL 0x4

struct SBTree {
  SPgno         root;
  int           keyLen;
  int           valLen;
  SPager       *pPager;
  tdb_cmpr_fn_t kcmpr;
  int           pageSize;
  int           maxLocal;
  int           minLocal;
  int           maxLeaf;
  int           minLeaf;
  SBtInfo       info;
  char         *tbname;
  void         *pBuf;
};

#define TDB_BTREE_PAGE_COMMON_HDR u8 flags;

#define TDB_BTREE_PAGE_GET_FLAGS(PAGE)        (PAGE)->pData[0]
#define TDB_BTREE_PAGE_SET_FLAGS(PAGE, flags) ((PAGE)->pData[0] = (flags))
#define TDB_BTREE_PAGE_IS_ROOT(PAGE)          (TDB_BTREE_PAGE_GET_FLAGS(PAGE) & TDB_BTREE_ROOT)
#define TDB_BTREE_PAGE_IS_LEAF(PAGE)          (TDB_BTREE_PAGE_GET_FLAGS(PAGE) & TDB_BTREE_LEAF)
#define TDB_BTREE_PAGE_IS_OVFL(PAGE)          (TDB_BTREE_PAGE_GET_FLAGS(PAGE) & TDB_BTREE_OVFL)
#define TDB_BTREE_ASSERT_FLAG(flags)                                                     \
  ASSERT(TDB_FLAG_IS(flags, TDB_BTREE_ROOT) || TDB_FLAG_IS(flags, TDB_BTREE_LEAF) ||     \
         TDB_FLAG_IS(flags, TDB_BTREE_ROOT | TDB_BTREE_LEAF) || TDB_FLAG_IS(flags, 0) || \
         TDB_FLAG_IS(flags, TDB_BTREE_OVFL))

#pragma pack(push, 1)
typedef struct {
  TDB_BTREE_PAGE_COMMON_HDR
} SLeafHdr;

typedef struct {
  TDB_BTREE_PAGE_COMMON_HDR
  SPgno pgno;  // right-most child
} SIntHdr;
#pragma pack(pop)

static int tdbDefaultKeyCmprFn(const void *pKey1, int keyLen1, const void *pKey2, int keyLen2);
static int tdbBtreeOpenImpl(SBTree *pBt);
// static int tdbBtreeInitPage(SPage *pPage, void *arg, int init);
static int tdbBtreeEncodeCell(SPage *pPage, const void *pKey, int kLen, const void *pVal, int vLen, SCell *pCell,
                              int *szCell, TXN *pTxn, SBTree *pBt);
static int tdbBtreeDecodeCell(SPage *pPage, const SCell *pCell, SCellDecoder *pDecoder, TXN *pTxn, SBTree *pBt);
static int tdbBtreeBalance(SBTC *pBtc);
static int tdbBtreeCellSize(const SPage *pPage, SCell *pCell, int dropOfp, TXN *pTxn, SBTree *pBt);
static int tdbBtcMoveDownward(SBTC *pBtc);
static int tdbBtcMoveUpward(SBTC *pBtc);

int tdbBtreeOpen(int keyLen, int valLen, SPager *pPager, char const *tbname, SPgno pgno, tdb_cmpr_fn_t kcmpr, TDB *pEnv,
                 SBTree **ppBt) {
  SBTree *pBt;
  int     ret;

  if (keyLen == 0) {
    tdbError("tdb/btree-open: key len cannot be zero.");
    return -1;
  }

  *ppBt = NULL;

  pBt = (SBTree *)tdbOsCalloc(1, sizeof(*pBt));
  if (pBt == NULL) {
    return -1;
  }

  // pBt->keyLen
  pBt->keyLen = keyLen < 0 ? TDB_VARIANT_LEN : keyLen;
  // pBt->valLen
  pBt->valLen = valLen < 0 ? TDB_VARIANT_LEN : valLen;
  // pBt->pPager
  pBt->pPager = pPager;
  // pBt->kcmpr
  pBt->kcmpr = kcmpr ? kcmpr : tdbDefaultKeyCmprFn;
  // pBt->pageSize
  pBt->pageSize = pPager->pageSize;
  // pBt->maxLocal
  pBt->maxLocal = tdbPageCapacity(pBt->pageSize, sizeof(SIntHdr)) / 4;
  // pBt->minLocal: Should not be allowed smaller than 15, which is [nPayload][nKey][nData]
  pBt->minLocal = pBt->maxLocal / 2;
  // pBt->maxLeaf
  pBt->maxLeaf = tdbPageCapacity(pBt->pageSize, sizeof(SLeafHdr));
  // pBt->minLeaf
  pBt->minLeaf = pBt->minLocal;

  // if pgno == 0 fetch new btree root leaf page
  if (pgno == 0) {
    // fetch page & insert into main db
    SPage *pPage;
    TXN   *txn;

    ret = tdbBegin(pEnv, &txn, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
    if (ret < 0) {
      tdbOsFree(pBt);
      return -1;
    }

    SBtreeInitPageArg zArg;
    zArg.flags = 0x1 | 0x2;  // root leaf node;
    zArg.pBt = pBt;
    ret = tdbPagerFetchPage(pPager, &pgno, &pPage, tdbBtreeInitPage, &zArg, txn);
    if (ret < 0) {
      tdbAbort(pEnv, txn);
      tdbOsFree(pBt);
      return -1;
    }

    ret = tdbPagerWrite(pPager, pPage);
    if (ret < 0) {
      tdbError("failed to write page since %s", terrstr());
      tdbAbort(pEnv, txn);
      tdbOsFree(pBt);
      return -1;
    }

    if (strcmp(TDB_MAINDB_NAME, tbname)) {
      pBt->info.root = pgno;
      pBt->info.nLevel = 1;
      pBt->info.nData = 0;
      pBt->tbname = (char *)tbname;

      ret = tdbTbInsert(pPager->pEnv->pMainDb, tbname, strlen(tbname) + 1, &pBt->info, sizeof(pBt->info), txn);
      if (ret < 0) {
        tdbAbort(pEnv, txn);
        tdbOsFree(pBt);
        return -1;
      }
    }

    tdbPCacheRelease(pPager->pCache, pPage, txn);

    tdbCommit(pPager->pEnv, txn);
    tdbPostCommit(pPager->pEnv, txn);
  }

  if (pgno == 0) {
    tdbError("tdb/btree-open: pgno cannot be zero.");
    tdbOsFree(pBt);
    return -1;
  }
  pBt->root = pgno;
  /*
  // TODO: pBt->root
  ret = tdbBtreeOpenImpl(pBt);
  if (ret < 0) {
    tdbOsFree(pBt);
    return -1;
  }
  */
  *ppBt = pBt;
  return 0;
}

int tdbBtreeClose(SBTree *pBt) {
  if (pBt) {
    tdbFree(pBt->pBuf);
    tdbOsFree(pBt);
  }
  return 0;
}

int tdbBtreeInsert(SBTree *pBt, const void *pKey, int kLen, const void *pVal, int vLen, TXN *pTxn) {
  SBTC   btc;
  SCell *pCell;
  void  *pBuf;
  int    szCell;
  int    szBuf;
  int    ret;
  int    idx;
  int    c;

  tdbBtcOpen(&btc, pBt, pTxn);

  tdbTrace("tdb insert, btc: %p, pTxn: %p", &btc, pTxn);

  // move to the position to insert
  ret = tdbBtcMoveTo(&btc, pKey, kLen, &c);
  if (ret < 0) {
    tdbBtcClose(&btc);
    tdbError("tdb/btree-insert: btc move to failed with ret: %d.", ret);
    return -1;
  }

  if (btc.idx == -1) {
    btc.idx = 0;
  } else {
    if (c > 0) {
      btc.idx++;
    } else if (c == 0) {
      // dup key not allowed with insert
      tdbBtcClose(&btc);
      tdbError("tdb/btree-insert: dup key. pKey: %p, kLen: %d, btc: %p, pTxn: %p", pKey, kLen, &btc, pTxn);
      return -1;
    }
  }

  ret = tdbBtcUpsert(&btc, pKey, kLen, pVal, vLen, 1);
  if (ret < 0) {
    tdbBtcClose(&btc);
    tdbError("tdb/btree-insert: btc upsert failed with ret: %d.", ret);
    return -1;
  }

  tdbBtcClose(&btc);
  return 0;
}

int tdbBtreeDelete(SBTree *pBt, const void *pKey, int kLen, TXN *pTxn) {
  SBTC btc;
  int  c;
  int  ret;

  tdbBtcOpen(&btc, pBt, pTxn);
  /*
  btc.coder.ofps = taosArrayInit(8, sizeof(SPage *));
  // btc.coder.ofps = taosArrayInit(8, sizeof(SPgno));
  //pBtc->coder.ofps = taosArrayInit(8, sizeof(SPage *));
  */
  tdbTrace("tdb delete, btc: %p, pTxn: %p", &btc, pTxn);

  // move the cursor
  ret = tdbBtcMoveTo(&btc, pKey, kLen, &c);
  if (ret < 0) {
    tdbBtcClose(&btc);
    tdbError("tdb/btree-delete: btc move to failed with ret: %d.", ret);
    return -1;
  }

  if (btc.idx < 0 || c != 0) {
    tdbBtcClose(&btc);
    return -1;
  }

  // delete the key
  if (tdbBtcDelete(&btc) < 0) {
    tdbBtcClose(&btc);
    return -1;
  }
  /*
  SArray *ofps = btc.coder.ofps;
  if (ofps) {
    for (int i = 0; i < TARRAY_SIZE(ofps); ++i) {
      SPage *ofp = *(SPage **)taosArrayGet(ofps, i);
      tdbPagerInsertFreePage(btc.pBt->pPager, ofp, btc.pTxn);
    }

    taosArrayDestroy(ofps);
    btc.coder.ofps = NULL;
  }
  */
  tdbBtcClose(&btc);
  return 0;
}

int tdbBtreeUpsert(SBTree *pBt, const void *pKey, int nKey, const void *pData, int nData, TXN *pTxn) {
  SBTC btc = {0};
  int  c;
  int  ret;

  tdbBtcOpen(&btc, pBt, pTxn);

  tdbTrace("tdb upsert, btc: %p, pTxn: %p", &btc, pTxn);

  // move the cursor
  ret = tdbBtcMoveTo(&btc, pKey, nKey, &c);
  if (ret < 0) {
    tdbError("tdb/btree-upsert: btc move to failed with ret: %d.", ret);
    if (TDB_CELLDECODER_FREE_KEY(&btc.coder)) {
      tdbFree(btc.coder.pKey);
    }
    tdbBtcClose(&btc);
    return -1;
  }

  if (TDB_CELLDECODER_FREE_KEY(&btc.coder)) {
    tdbFree(btc.coder.pKey);
  }

  if (btc.idx == -1) {
    btc.idx = 0;
    c = 1;
  } else {
    if (c > 0) {
      btc.idx = btc.idx + 1;
    }
  }

  ret = tdbBtcUpsert(&btc, pKey, nKey, pData, nData, c);
  if (ret < 0) {
    tdbBtcClose(&btc);
    tdbError("tdb/btree-upsert: btc upsert failed with ret: %d.", ret);
    return -1;
  }

  tdbBtcClose(&btc);
  return 0;
}

int tdbBtreeGet(SBTree *pBt, const void *pKey, int kLen, void **ppVal, int *vLen) {
  return tdbBtreePGet(pBt, pKey, kLen, NULL, NULL, ppVal, vLen);
}

int tdbBtreePGet(SBTree *pBt, const void *pKey, int kLen, void **ppKey, int *pkLen, void **ppVal, int *vLen) {
  SBTC         btc;
  SCell       *pCell;
  int          cret;
  int          ret;
  void        *pTKey = NULL;
  void        *pTVal = NULL;
  SCellDecoder cd = {0};

  tdbBtcOpen(&btc, pBt, NULL);

  tdbTrace("tdb pget, btc: %p", &btc);

  ret = tdbBtcMoveTo(&btc, pKey, kLen, &cret);
  if (ret < 0) {
    tdbBtcClose(&btc);
    tdbError("tdb/btree-pget: btc move to failed with ret: %d.", ret);
    return -1;
  }

  if (btc.idx < 0 || cret) {
    tdbBtcClose(&btc);

    return -1;
  }

  pCell = tdbPageGetCell(btc.pPage, btc.idx);
  tdbBtreeDecodeCell(btc.pPage, pCell, &cd, btc.pTxn, pBt);

  if (ppKey) {
    pTKey = tdbRealloc(*ppKey, cd.kLen);
    if (pTKey == NULL) {
      tdbBtcClose(&btc);
      tdbError("tdb/btree-pget: realloc pTKey failed.");
      return -1;
    }
    *ppKey = pTKey;
    *pkLen = cd.kLen;
    memcpy(*ppKey, cd.pKey, (size_t)cd.kLen);
  }

  if (ppVal) {
    pTVal = tdbRealloc(*ppVal, cd.vLen);
    if (pTVal == NULL) {
      tdbBtcClose(&btc);
      tdbError("tdb/btree-pget: realloc pTVal failed.");
      return -1;
    }
    *ppVal = pTVal;
    *vLen = cd.vLen;
    memcpy(*ppVal, cd.pVal, (size_t)cd.vLen);
  }

  if (TDB_CELLDECODER_FREE_KEY(&cd)) {
    tdbFree(cd.pKey);
  }

  if (TDB_CELLDECODER_FREE_VAL(&cd)) {
    tdbTrace("tdb btc/pget/2 decoder: %p pVal free: %p", &cd, cd.pVal);

    tdbFree(cd.pVal);
  }

  tdbTrace("tdb pget end, btc decoder: %p/0x%x, local decoder:%p", &btc.coder, btc.coder.freeKV, &cd);

  tdbBtcClose(&btc);

  return 0;
}

static int tdbDefaultKeyCmprFn(const void *pKey1, int keyLen1, const void *pKey2, int keyLen2) {
  int mlen;
  int cret;

  if (ASSERT(keyLen1 > 0 && keyLen2 > 0 && pKey1 != NULL && pKey2 != NULL)) {
    // -1 is less than
  }

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

int tdbBtreeInitPage(SPage *pPage, void *arg, int init) {
  SBTree *pBt;
  u8      flags;
  u8      leaf;

  pBt = ((SBtreeInitPageArg *)arg)->pBt;

  if (init) {
    // init page
    flags = TDB_BTREE_PAGE_GET_FLAGS(pPage);
    leaf = TDB_BTREE_PAGE_IS_LEAF(pPage);
    TDB_BTREE_ASSERT_FLAG(flags);

    tdbPageInit(pPage, leaf ? sizeof(SLeafHdr) : sizeof(SIntHdr), tdbBtreeCellSize);
  } else {
    // zero page
    flags = ((SBtreeInitPageArg *)arg)->flags;
    leaf = flags & TDB_BTREE_LEAF;
    TDB_BTREE_ASSERT_FLAG(flags);

    tdbPageZero(pPage, leaf ? sizeof(SLeafHdr) : sizeof(SIntHdr), tdbBtreeCellSize);

    if (leaf) {
      SLeafHdr *pLeafHdr = (SLeafHdr *)(pPage->pData);
      pLeafHdr->flags = flags;

    } else {
      SIntHdr *pIntHdr = (SIntHdr *)(pPage->pData);
      pIntHdr->flags = flags;
      pIntHdr->pgno = 0;
    }
  }

  if (leaf) {
    pPage->kLen = pBt->keyLen;
    pPage->vLen = pBt->valLen;
    pPage->maxLocal = pBt->maxLeaf;
    pPage->minLocal = pBt->minLeaf;
  } else if (TDB_BTREE_PAGE_IS_OVFL(pPage)) {
    pPage->kLen = pBt->keyLen;
    pPage->vLen = pBt->valLen;
    pPage->maxLocal = tdbPageCapacity(pBt->pageSize, sizeof(SIntHdr));
    pPage->minLocal = pBt->minLocal;
  } else {
    pPage->kLen = pBt->keyLen;
    pPage->vLen = sizeof(SPgno);
    pPage->maxLocal = pBt->maxLocal;
    pPage->minLocal = pBt->minLocal;
  }

  return 0;
}

// TDB_BTREE_BALANCE =====================
static int tdbBtreeBalanceDeeper(SBTree *pBt, SPage *pRoot, SPage **ppChild, TXN *pTxn) {
  SPager           *pPager;
  SPage            *pChild;
  SPgno             pgnoChild;
  int               ret;
  u8                flags;
  SIntHdr          *pIntHdr;
  SBtreeInitPageArg zArg;
  u8                leaf;

  pPager = pRoot->pPager;
  flags = TDB_BTREE_PAGE_GET_FLAGS(pRoot);
  leaf = TDB_BTREE_PAGE_IS_LEAF(pRoot);

  // allocate a new child page
  pgnoChild = 0;
  zArg.flags = TDB_FLAG_REMOVE(flags, TDB_BTREE_ROOT);
  zArg.pBt = pBt;
  ret = tdbPagerFetchPage(pPager, &pgnoChild, &pChild, tdbBtreeInitPage, &zArg, pTxn);
  if (ret < 0) {
    return -1;
  }

  if (!leaf) {
    ((SIntHdr *)pChild->pData)->pgno = ((SIntHdr *)(pRoot->pData))->pgno;
  }

  ret = tdbPagerWrite(pPager, pChild);
  if (ret < 0) {
    tdbError("failed to write page since %s", terrstr());
    return -1;
  }

  // Copy the root page content to the child page
  tdbPageCopy(pRoot, pChild, 0);

  // Reinitialize the root page
  zArg.flags = TDB_BTREE_ROOT;
  zArg.pBt = pBt;
  ret = tdbBtreeInitPage(pRoot, &zArg, 0);
  if (ret < 0) {
    return -1;
  }

  pIntHdr = (SIntHdr *)(pRoot->pData);
  pIntHdr->pgno = pgnoChild;

  *ppChild = pChild;
  return 0;
}

static int tdbBtreeBalanceNonRoot(SBTree *pBt, SPage *pParent, int idx, TXN *pTxn) {
  int ret;

  int    nOlds, pageIdx;
  SPage *pOlds[3] = {0};
  SCell *pDivCell[3] = {0};
  int    szDivCell[3];
  int    sIdx;
  u8     childNotLeaf;
  SPgno  rPgno;

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
      if (ASSERT(sIdx + i <= nCells)) {
        return -1;
      }

      SPgno pgno;
      if (sIdx + i == nCells) {
        if (ASSERT(!TDB_BTREE_PAGE_IS_LEAF(pParent))) {
          return -1;
        }
        pgno = ((SIntHdr *)(pParent->pData))->pgno;
      } else {
        pCell = tdbPageGetCell(pParent, sIdx + i);
        pgno = *(SPgno *)pCell;
      }

      ret = tdbPagerFetchPage(pBt->pPager, &pgno, pOlds + i, tdbBtreeInitPage,
                              &((SBtreeInitPageArg){.pBt = pBt, .flags = 0}), pTxn);
      if (ret < 0) {
        tdbError("tdb/btree-balance: fetch page failed with ret: %d.", ret);
        return -1;
      }

      ret = tdbPagerWrite(pBt->pPager, pOlds[i]);
      if (ret < 0) {
        tdbError("failed to write page since %s", terrstr());
        return -1;
      }
    }
    // copy the parent key out if child pages are not leaf page
    // childNotLeaf = !(TDB_BTREE_PAGE_IS_LEAF(pOlds[0]) || TDB_BTREE_PAGE_IS_OVFL(pOlds[0]));
    childNotLeaf = !TDB_BTREE_PAGE_IS_LEAF(pOlds[0]);
    if (childNotLeaf) {
      for (int i = 0; i < nOlds; i++) {
        if (sIdx + i < TDB_PAGE_TOTAL_CELLS(pParent)) {
          pCell = tdbPageGetCell(pParent, sIdx + i);
          szDivCell[i] = tdbBtreeCellSize(pParent, pCell, 0, NULL, NULL);
          pDivCell[i] = tdbOsMalloc(szDivCell[i]);
          memcpy(pDivCell[i], pCell, szDivCell[i]);
        }

        if (i < nOlds - 1) {
          ((SPgno *)pDivCell[i])[0] = ((SIntHdr *)pOlds[i]->pData)->pgno;
          ((SIntHdr *)pOlds[i]->pData)->pgno = 0;
          tdbPageInsertCell(pOlds[i], TDB_PAGE_TOTAL_CELLS(pOlds[i]), pDivCell[i], szDivCell[i], 1);
        }
      }
      rPgno = ((SIntHdr *)pOlds[nOlds - 1]->pData)->pgno;
    }

    ret = tdbPagerWrite(pBt->pPager, pParent);
    if (ret < 0) {
      tdbError("failed to write page since %s", terrstr());
      return -1;
    }

    // drop the cells on parent page
    for (int i = 0; i < nOlds; i++) {
      nCells = TDB_PAGE_TOTAL_CELLS(pParent);
      if (sIdx < nCells) {
        bool destroyOfps = false;
        if (!childNotLeaf) {
          if (!pParent->pPager->ofps) {
            pParent->pPager->ofps = taosArrayInit(8, sizeof(SPage *));
            destroyOfps = true;
          }
        }

        tdbPageDropCell(pParent, sIdx, pTxn, pBt);

        if (!childNotLeaf) {
          SArray *ofps = pParent->pPager->ofps;
          if (ofps) {
            for (int i = 0; i < TARRAY_SIZE(ofps); ++i) {
              SPage *ofp = *(SPage **)taosArrayGet(ofps, i);
              tdbPagerInsertFreePage(pParent->pPager, ofp, pTxn);
            }

            if (destroyOfps) {
              taosArrayDestroy(ofps);
              pParent->pPager->ofps = NULL;
            }
          }
        }
      } else {
        ((SIntHdr *)pParent->pData)->pgno = 0;
      }
    }
  }

  int nNews = 0;
  struct {
    int cnt;
    int size;
    int iPage;
    int oIdx;
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
        infoNews[nNews].iPage = oPage;
        infoNews[nNews].oIdx = oIdx;
      }
    }

    nNews++;

    // back loop to make the distribution even
    for (int iNew = nNews - 1; iNew > 0; iNew--) {
      SCell *pCell;
      int    szLCell, szRCell;

      // balance page (iNew) and (iNew-1)
      for (;;) {
        pCell = tdbPageGetCell(pOlds[infoNews[iNew - 1].iPage], infoNews[iNew - 1].oIdx);

        szLCell = tdbBtreeCellSize(pOlds[infoNews[iNew - 1].iPage], pCell, 0, NULL, NULL);
        if (!childNotLeaf) {
          szRCell = szLCell;
        } else {
          int    iPage = infoNews[iNew - 1].iPage;
          int    oIdx = infoNews[iNew - 1].oIdx + 1;
          SPage *pPage;
          for (;;) {
            pPage = pOlds[iPage];
            if (oIdx < TDB_PAGE_TOTAL_CELLS(pPage)) {
              break;
            }

            iPage++;
            oIdx = 0;
          }

          pCell = tdbPageGetCell(pPage, oIdx);
          szRCell = tdbBtreeCellSize(pPage, pCell, 0, NULL, NULL);
        }

        if (ASSERT(infoNews[iNew - 1].cnt > 0)) {
          return -1;
        }

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

          infoNews[iNew - 1].iPage--;
          infoNews[iNew - 1].oIdx = TDB_PAGE_TOTAL_CELLS(pOlds[infoNews[iNew - 1].iPage]) - 1;
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
        pgno = 0;
        iarg.pBt = pBt;
        iarg.flags = flags;
        ret = tdbPagerFetchPage(pBt->pPager, &pgno, pNews + iNew, tdbBtreeInitPage, &iarg, pTxn);
        if (ret < 0) {
          tdbError("tdb/btree-balance: fetch page failed with ret: %d.", ret);
          return -1;
        }

        ret = tdbPagerWrite(pBt->pPager, pNews[iNew]);
        if (ret < 0) {
          tdbError("failed to write page since %s", terrstr());
          return -1;
        }
      }
    }

    // TODO: sort the page according to the page number
  }

  {  // Do the real cell distribution
    SPage            *pOldsCopy[3] = {0};
    SCell            *pCell;
    int               szCell;
    SBtreeInitPageArg iarg;
    int               iNew, nNewCells;
    SCellDecoder      cd = {0};

    iarg.pBt = pBt;
    iarg.flags = TDB_BTREE_PAGE_GET_FLAGS(pOlds[0]);
    for (int i = 0; i < nOlds; i++) {
      tdbPageCreate(pOlds[0]->pageSize, &pOldsCopy[i], tdbDefaultMalloc, NULL);
      tdbBtreeInitPage(pOldsCopy[i], &iarg, 0);
      tdbPageCopy(pOlds[i], pOldsCopy[i], 0);
      pOlds[i]->nOverflow = 0;
    }

    iNew = 0;
    nNewCells = 0;
    tdbBtreeInitPage(pNews[iNew], &iarg, 0);

    for (int iOld = 0; iOld < nOlds; iOld++) {
      SPage *pPage;

      pPage = pOldsCopy[iOld];

      for (int oIdx = 0; oIdx < TDB_PAGE_TOTAL_CELLS(pPage); oIdx++) {
        pCell = tdbPageGetCell(pPage, oIdx);
        szCell = tdbBtreeCellSize(pPage, pCell, 0, NULL, NULL);

        if (ASSERT(nNewCells <= infoNews[iNew].cnt)) {
          return -1;
        }
        if (ASSERT(iNew < nNews)) {
          return -1;
        }

        if (nNewCells < infoNews[iNew].cnt) {
          tdbPageInsertCell(pNews[iNew], nNewCells, pCell, szCell, 0);
          nNewCells++;

          // insert parent page
          if (!childNotLeaf && nNewCells == infoNews[iNew].cnt) {
            SIntHdr *pIntHdr = (SIntHdr *)pParent->pData;

            if (iNew == nNews - 1 && pIntHdr->pgno == 0) {
              pIntHdr->pgno = TDB_PAGE_PGNO(pNews[iNew]);
            } else {
              tdbBtreeDecodeCell(pPage, pCell, &cd, pTxn, pBt);

              // TODO: pCell here may be inserted as an overflow cell, handle it
              SCell *pNewCell = tdbOsMalloc(cd.kLen + 9);
              int    szNewCell;
              SPgno  pgno;
              pgno = TDB_PAGE_PGNO(pNews[iNew]);
              tdbBtreeEncodeCell(pParent, cd.pKey, cd.kLen, (void *)&pgno, sizeof(SPgno), pNewCell, &szNewCell, pTxn,
                                 pBt);
              tdbPageInsertCell(pParent, sIdx++, pNewCell, szNewCell, 0);
              tdbOsFree(pNewCell);

              if (TDB_CELLDECODER_FREE_VAL(&cd)) {
                tdbFree(cd.pVal);
                cd.pVal = NULL;
              }
            }

            // move to next new page
            iNew++;
            nNewCells = 0;
            if (iNew < nNews) {
              tdbBtreeInitPage(pNews[iNew], &iarg, 0);
            }
          }
        } else {
          if (ASSERT(childNotLeaf)) {
            return -1;
          }
          if (ASSERT(iNew < nNews - 1)) {
            return -1;
          }

          // set current new page right-most child
          ((SIntHdr *)pNews[iNew]->pData)->pgno = ((SPgno *)pCell)[0];

          // insert to parent as divider cell
          if (ASSERT(iNew < nNews - 1)) {
            return -1;
          }
          ((SPgno *)pCell)[0] = TDB_PAGE_PGNO(pNews[iNew]);
          tdbPageInsertCell(pParent, sIdx++, pCell, szCell, 0);

          // move to next new page
          iNew++;
          nNewCells = 0;
          if (iNew < nNews) {
            tdbBtreeInitPage(pNews[iNew], &iarg, 0);
          }
        }
      }
    }

    if (childNotLeaf) {
      if (ASSERT(TDB_PAGE_TOTAL_CELLS(pNews[nNews - 1]) == infoNews[nNews - 1].cnt)) {
        return -1;
      }
      ((SIntHdr *)(pNews[nNews - 1]->pData))->pgno = rPgno;

      SIntHdr *pIntHdr = (SIntHdr *)pParent->pData;
      if (pIntHdr->pgno == 0) {
        pIntHdr->pgno = TDB_PAGE_PGNO(pNews[nNews - 1]);
      } else {
        ((SPgno *)pDivCell[nOlds - 1])[0] = TDB_PAGE_PGNO(pNews[nNews - 1]);
        tdbPageInsertCell(pParent, sIdx, pDivCell[nOlds - 1], szDivCell[nOlds - 1], 0);
      }
    }

    for (int i = 0; i < nOlds; i++) {
      tdbPageDestroy(pOldsCopy[i], tdbDefaultFree, NULL);
    }
  }

  if (TDB_BTREE_PAGE_IS_ROOT(pParent) && TDB_PAGE_TOTAL_CELLS(pParent) == 0) {
    i8 flags = TDB_BTREE_ROOT | TDB_BTREE_PAGE_IS_LEAF(pNews[0]);
    // copy content to the parent page
    tdbBtreeInitPage(pParent, &(SBtreeInitPageArg){.flags = flags, .pBt = pBt}, 0);
    tdbPageCopy(pNews[0], pParent, 1);

    if (!TDB_BTREE_PAGE_IS_LEAF(pNews[0])) {
      ((SIntHdr *)(pParent->pData))->pgno = ((SIntHdr *)(pNews[0]->pData))->pgno;
    }

    tdbPagerInsertFreePage(pBt->pPager, pNews[0], pTxn);
  }

  for (int i = 0; i < 3; i++) {
    if (pDivCell[i]) {
      tdbOsFree(pDivCell[i]);
    }
  }

  for (pageIdx = 0; pageIdx < nOlds; ++pageIdx) {
    if (pageIdx >= nNews) {
      tdbPagerInsertFreePage(pBt->pPager, pOlds[pageIdx], pTxn);
    }
    tdbPagerReturnPage(pBt->pPager, pOlds[pageIdx], pTxn);
  }
  for (; pageIdx < nNews; ++pageIdx) {
    tdbPagerReturnPage(pBt->pPager, pNews[pageIdx], pTxn);
  }

  return 0;
}

static int tdbBtreeBalance(SBTC *pBtc) {
  int    iPage;
  int    ret;
  int    nFree;
  SPage *pParent;
  SPage *pPage;
  u8     flags;
  u8     leaf;
  u8     root;

  // Main loop to balance the BTree
  for (;;) {
    iPage = pBtc->iPage;
    pPage = pBtc->pPage;
    leaf = TDB_BTREE_PAGE_IS_LEAF(pPage);
    root = TDB_BTREE_PAGE_IS_ROOT(pPage);
    nFree = TDB_PAGE_FREE_SIZE(pPage);

    // when the page is not overflow and not too empty, the balance work
    // is finished. Just break out the balance loop.
    if (pPage->nOverflow == 0 && nFree < TDB_PAGE_USABLE_SIZE(pPage) * 2 / 3) {
      break;
    }

    if (iPage == 0) {
      // For the root page, only balance when the page is overfull,
      // ignore the case of empty
      if (pPage->nOverflow == 0) break;

      ret = tdbBtreeBalanceDeeper(pBtc->pBt, pPage, &(pBtc->pgStack[1]), pBtc->pTxn);
      if (ret < 0) {
        return -1;
      }

      pBtc->idx = 0;
      pBtc->idxStack[0] = 0;
      pBtc->pgStack[0] = pBtc->pPage;
      pBtc->iPage = 1;
      pBtc->pPage = pBtc->pgStack[1];
    } else {
      // Generalized balance step
      pParent = pBtc->pgStack[iPage - 1];

      ret = tdbBtreeBalanceNonRoot(pBtc->pBt, pParent, pBtc->idxStack[pBtc->iPage - 1], pBtc->pTxn);
      if (ret < 0) {
        return -1;
      }

      tdbPagerReturnPage(pBtc->pBt->pPager, pBtc->pPage, pBtc->pTxn);

      pBtc->iPage--;
      pBtc->pPage = pBtc->pgStack[pBtc->iPage];
    }
  }

  return 0;
}
// TDB_BTREE_BALANCE

static int tdbFetchOvflPage(SPgno *pPgno, SPage **ppOfp, TXN *pTxn, SBTree *pBt) {
  int ret = 0;

  *pPgno = 0;
  SBtreeInitPageArg iArg;
  iArg.pBt = pBt;
  iArg.flags = TDB_FLAG_ADD(0, TDB_BTREE_OVFL);
  ret = tdbPagerFetchPage(pBt->pPager, pPgno, ppOfp, tdbBtreeInitPage, &iArg, pTxn);
  if (ret < 0) {
    return -1;
  }

  // mark dirty
  ret = tdbPagerWrite(pBt->pPager, *ppOfp);
  if (ret < 0) {
    tdbError("failed to write page since %s", terrstr());
    return -1;
  }

  tdbPCacheRelease(pBt->pPager->pCache, *ppOfp, pTxn);

  return ret;
}

static int tdbLoadOvflPage(SPgno *pPgno, SPage **ppOfp, TXN *pTxn, SBTree *pBt) {
  int ret = 0;

  SBtreeInitPageArg iArg;
  iArg.pBt = pBt;
  iArg.flags = TDB_FLAG_ADD(0, TDB_BTREE_OVFL);
  ret = tdbPagerFetchPage(pBt->pPager, pPgno, ppOfp, tdbBtreeInitPage, &iArg, pTxn);
  if (ret < 0) {
    return -1;
  }

  return ret;
}

// TDB_BTREE_CELL =====================
static int tdbBtreeEncodePayload(SPage *pPage, SCell *pCell, int nHeader, const void *pKey, int kLen, const void *pVal,
                                 int vLen, int *szPayload, TXN *pTxn, SBTree *pBt) {
  int ret = 0;
  int nPayload = kLen + vLen;
  int maxLocal = pPage->maxLocal;

  if (nPayload + nHeader <= maxLocal) {
    // no overflow page is needed
    memcpy(pCell + nHeader, pKey, kLen);
    if (pVal) {
      memcpy(pCell + nHeader + kLen, pVal, vLen);
    }

    *szPayload = nPayload;
    return 0;
  } else {
    // handle overflow case
    // calc local storage size
    int minLocal = pPage->minLocal;
    int surplus = minLocal + (nPayload + nHeader - minLocal) % (maxLocal - sizeof(SPgno));
    int nLocal = surplus <= maxLocal ? surplus : minLocal;

    // int ofpCap = tdbPageCapacity(pBt->pageSize, sizeof(SIntHdr));

    // fetch a new ofp and make it dirty
    SPgno  pgno = 0;
    SPage *ofp = NULL, *nextOfp = NULL;

    ret = tdbFetchOvflPage(&pgno, &ofp, pTxn, pBt);
    if (ret < 0) {
      return -1;
    }

    // local buffer for cell
    SCell *pBuf = tdbRealloc(NULL, pBt->pageSize);
    if (pBuf == NULL) {
      return -1;
    }

    int nLeft = nPayload;
    int bytes;
    int lastPage = 0;
    if (nLocal >= nHeader + kLen + sizeof(SPgno)) {
      // pack key to local
      memcpy(pCell + nHeader, pKey, kLen);
      nLeft -= kLen;
      // pack partial val to local if any space left
      if (nLocal > nHeader + kLen + sizeof(SPgno)) {
        if (ASSERT(pVal != NULL && vLen != 0)) {
          return -1;
        }
        memcpy(pCell + nHeader + kLen, pVal, nLocal - nHeader - kLen - sizeof(SPgno));
        nLeft -= nLocal - nHeader - kLen - sizeof(SPgno);
      }

      // pack nextPgno
      memcpy(pCell + nHeader + nPayload - nLeft, &pgno, sizeof(pgno));

      // pack left val data to ovpages
      do {
        lastPage = 0;
        if (nLeft <= ofp->maxLocal - sizeof(SPgno)) {
          bytes = nLeft;
          lastPage = 1;
        } else {
          bytes = ofp->maxLocal - sizeof(SPgno);
        }

        // fetch next ofp if not last page
        if (!lastPage) {
          // fetch a new ofp and make it dirty
          ret = tdbFetchOvflPage(&pgno, &nextOfp, pTxn, pBt);
          if (ret < 0) {
            tdbFree(pBuf);
            return -1;
          }
        } else {
          pgno = 0;
        }

        memcpy(pBuf, ((SCell *)pVal) + vLen - nLeft, bytes);
        memcpy(pBuf + bytes, &pgno, sizeof(pgno));

        ret = tdbPageInsertCell(ofp, 0, pBuf, bytes + sizeof(pgno), 0);
        if (ret < 0) {
          tdbFree(pBuf);
          return -1;
        }

        ofp = nextOfp;
        nLeft -= bytes;
      } while (nLeft > 0);
    } else {
      int nLeftKey = kLen;
      // pack partial key and nextPgno
      memcpy(pCell + nHeader, pKey, nLocal - nHeader - sizeof(pgno));
      nLeft -= nLocal - nHeader - sizeof(pgno);
      nLeftKey -= nLocal - nHeader - sizeof(pgno);

      memcpy(pCell + nLocal - sizeof(pgno), &pgno, sizeof(pgno));

      int lastKeyPageSpace = 0;
      // pack left key & val to ovpages
      do {
        // cal key to cpy
        int lastKeyPage = 0;
        if (nLeftKey <= ofp->maxLocal - sizeof(SPgno)) {
          bytes = nLeftKey;
          lastKeyPage = 1;
          lastKeyPageSpace = ofp->maxLocal - sizeof(SPgno) - nLeftKey;
        } else {
          bytes = ofp->maxLocal - sizeof(SPgno);
        }

        // cpy key
        memcpy(pBuf, ((SCell *)pKey) + kLen - nLeftKey, bytes);

        if (lastKeyPage) {
          if (lastKeyPageSpace >= vLen) {
            if (vLen > 0) {
              memcpy(pBuf + kLen - nLeftKey, pVal, vLen);

              nLeft -= vLen;
            }

            pgno = 0;
          } else {
            memcpy(pBuf + kLen - nLeftKey, pVal, lastKeyPageSpace);
            nLeft -= lastKeyPageSpace;

            // fetch next ofp, a new ofp and make it dirty
            ret = tdbFetchOvflPage(&pgno, &nextOfp, pTxn, pBt);
            if (ret < 0) {
              tdbFree(pBuf);
              return -1;
            }
          }
        } else {
          // fetch next ofp, a new ofp and make it dirty
          ret = tdbFetchOvflPage(&pgno, &nextOfp, pTxn, pBt);
          if (ret < 0) {
            tdbFree(pBuf);
            return -1;
          }
        }

        memcpy(pBuf + bytes, &pgno, sizeof(pgno));

        ret = tdbPageInsertCell(ofp, 0, pBuf, bytes + sizeof(pgno), 0);
        if (ret < 0) {
          return -1;
        }

        ofp = nextOfp;
        nLeftKey -= bytes;
        nLeft -= bytes;
      } while (nLeftKey > 0);

      while (nLeft > 0) {
        // pack left val data to ovpages
        lastPage = 0;
        if (nLeft <= maxLocal - sizeof(SPgno)) {
          bytes = nLeft;
          lastPage = 1;
        } else {
          bytes = maxLocal - sizeof(SPgno);
        }

        // fetch next ofp if not last page
        if (!lastPage) {
          // fetch a new ofp and make it dirty
          ret = tdbFetchOvflPage(&pgno, &nextOfp, pTxn, pBt);
          if (ret < 0) {
            tdbFree(pBuf);
            return -1;
          }
        } else {
          pgno = 0;
        }

        memcpy(pBuf, ((SCell *)pVal) + vLen - nLeft, bytes);
        memcpy(pBuf + bytes, &pgno, sizeof(pgno));

        if (ofp == NULL) {
          tdbFree(pBuf);
          return -1;
        }

        ret = tdbPageInsertCell(ofp, 0, pBuf, bytes + sizeof(pgno), 0);
        if (ret < 0) {
          tdbFree(pBuf);
          return -1;
        }

        ofp = nextOfp;
        nLeft -= bytes;
      }
    }

    // free local buffer
    tdbFree(pBuf);

    *szPayload = nLocal - nHeader;
  }

  return 0;
}

static int tdbBtreeEncodeCell(SPage *pPage, const void *pKey, int kLen, const void *pVal, int vLen, SCell *pCell,
                              int *szCell, TXN *pTxn, SBTree *pBt) {
  u8  leaf;
  int nHeader;
  int nPayload;
  int ret;

  if (ASSERT(pPage->kLen == TDB_VARIANT_LEN || pPage->kLen == kLen)) {
    return -1;
  }
  if (ASSERT(pPage->vLen == TDB_VARIANT_LEN || pPage->vLen == vLen)) {
    return -1;
  }
  if (ASSERT(pKey != NULL && kLen > 0)) {
    return -1;
  }

  nPayload = 0;
  nHeader = 0;
  leaf = TDB_BTREE_PAGE_IS_LEAF(pPage);

  // 1. Encode Header part
  /* Encode SPgno if interior page */
  if (!leaf) {
    if (pPage->vLen != sizeof(SPgno)) {
      tdbError("tdb/btree-encode-cell: invalid cell.");
      return -1;
    }

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
  if ((!leaf) || pPage->vLen == 0) {
    pVal = NULL;
    vLen = 0;
  }

  ret = tdbBtreeEncodePayload(pPage, pCell, nHeader, pKey, kLen, pVal, vLen, &nPayload, pTxn, pBt);
  if (ret < 0) {
    // TODO
    tdbError("tdb/btree-encode-cell: encode payload failed with ret: %d.", ret);
    return -1;
  }

  *szCell = nHeader + nPayload;
  return 0;
}

static int tdbBtreeDecodePayload(SPage *pPage, const SCell *pCell, int nHeader, SCellDecoder *pDecoder, TXN *pTxn,
                                 SBTree *pBt) {
  int ret = 0;
  int nPayload;
  int maxLocal = pPage->maxLocal;

  int kLen = pDecoder->kLen;
  int vLen = pDecoder->vLen;

  if (pDecoder->pVal) {
    if (TDB_BTREE_PAGE_IS_LEAF(pPage)) {
      tdbError("tdb/btree-decode-payload: leaf page with non-null pVal.");
      return -1;
    }
    nPayload = pDecoder->kLen;
  } else {
    nPayload = pDecoder->kLen + pDecoder->vLen;
  }

  if (nHeader + nPayload <= maxLocal) {
    // no over flow case
    pDecoder->pKey = (SCell *)pCell + nHeader;
    if (pDecoder->pVal == NULL && pDecoder->vLen > 0) {
      pDecoder->pVal = (SCell *)pCell + nHeader + pDecoder->kLen;
    }
    return 0;
  } else {
    // handle overflow case
    // calc local storage size
    int minLocal = pPage->minLocal;
    int surplus = minLocal + (nPayload + nHeader - minLocal) % (maxLocal - sizeof(SPgno));
    int nLocal = surplus <= maxLocal ? surplus : minLocal;

    int    nLeft = nPayload;
    SPgno  pgno = 0;
    SPage *ofp;
    SCell *ofpCell;
    int    bytes;
    int    lastPage = 0;

    if (nLocal >= pDecoder->kLen + nHeader + sizeof(SPgno)) {
      pDecoder->pKey = (SCell *)pCell + nHeader;
      nLeft -= kLen;
      if (nLocal > kLen + nHeader + sizeof(SPgno)) {
        // read partial val to local
        pDecoder->pVal = tdbRealloc(pDecoder->pVal, vLen);
        if (pDecoder->pVal == NULL) {
          return -1;
        }
        TDB_CELLDECODER_SET_FREE_VAL(pDecoder);

        tdbDebug("tdb btc decoder: %p/0x%x pVal: %p ", pDecoder, pDecoder->freeKV, pDecoder->pVal);

        memcpy(pDecoder->pVal, pCell + nHeader + kLen, nLocal - nHeader - kLen - sizeof(SPgno));

        nLeft -= nLocal - nHeader - kLen - sizeof(SPgno);
      }

      memcpy(&pgno, pCell + nHeader + nPayload - nLeft, sizeof(pgno));

      // unpack left val data from ovpages
      while (pgno != 0) {
        ret = tdbLoadOvflPage(&pgno, &ofp, pTxn, pBt);
        if (ret < 0) {
          return -1;
        }
        /*
        if (pDecoder->ofps) {
          taosArrayPush(pDecoder->ofps, &ofp);
        }
        */
        ofpCell = tdbPageGetCell(ofp, 0);

        if (nLeft <= ofp->maxLocal - sizeof(SPgno)) {
          bytes = nLeft;
          lastPage = 1;
        } else {
          bytes = ofp->maxLocal - sizeof(SPgno);
        }

        memcpy(pDecoder->pVal + vLen - nLeft, ofpCell, bytes);
        nLeft -= bytes;

        memcpy(&pgno, ofpCell + bytes, sizeof(pgno));

        tdbPCacheRelease(pBt->pPager->pCache, ofp, pTxn);
      }
    } else {
      int nLeftKey = kLen;
      // load partial key and nextPgno
      pDecoder->pKey = tdbRealloc(pDecoder->pKey, kLen);
      if (pDecoder->pKey == NULL) {
        return -1;
      }
      TDB_CELLDECODER_SET_FREE_KEY(pDecoder);

      memcpy(pDecoder->pKey, pCell + nHeader, nLocal - nHeader - sizeof(pgno));
      nLeft -= nLocal - nHeader - sizeof(pgno);
      nLeftKey -= nLocal - nHeader - sizeof(pgno);

      memcpy(&pgno, pCell + nLocal - sizeof(pgno), sizeof(pgno));

      int lastKeyPageSpace = 0;
      // load left key & val to ovpages
      while (pgno != 0) {
        tdbTrace("tdb decode-ofp, pTxn: %p, pgno:%u by cell:%p", pTxn, pgno, pCell);
        // printf("tdb decode-ofp, pTxn: %p, pgno:%u by cell:%p\n", pTxn, pgno, pCell);
        ret = tdbLoadOvflPage(&pgno, &ofp, pTxn, pBt);
        if (ret < 0) {
          return -1;
        }
        /*
        if (pDecoder->ofps) {
          taosArrayPush(pDecoder->ofps, &ofp);
        }
        */
        ofpCell = tdbPageGetCell(ofp, 0);

        int lastKeyPage = 0;
        if (nLeftKey <= maxLocal - sizeof(SPgno)) {
          bytes = nLeftKey;
          lastKeyPage = 1;
          lastKeyPageSpace = ofp->maxLocal - sizeof(SPgno) - nLeftKey;
        } else {
          bytes = ofp->maxLocal - sizeof(SPgno);
        }

        // cpy key
        memcpy(pDecoder->pKey + kLen - nLeftKey, ofpCell, bytes);

        if (lastKeyPage) {
          if (lastKeyPageSpace >= vLen) {
            if (vLen > 0) {
              pDecoder->pVal = ofpCell + kLen - nLeftKey;

              nLeft -= vLen;
            }
            pgno = 0;
          } else {
            // read partial val to local
            pDecoder->pVal = tdbRealloc(pDecoder->pVal, vLen);
            if (pDecoder->pVal == NULL) {
              return -1;
            }
            TDB_CELLDECODER_SET_FREE_VAL(pDecoder);

            memcpy(pDecoder->pVal, ofpCell + kLen - nLeftKey, lastKeyPageSpace);
            nLeft -= lastKeyPageSpace;
          }
        }

        memcpy(&pgno, ofpCell + bytes, sizeof(pgno));

        tdbPCacheRelease(pBt->pPager->pCache, ofp, pTxn);

        nLeftKey -= bytes;
        nLeft -= bytes;
      }

      while (nLeft > 0) {
        ret = tdbLoadOvflPage(&pgno, &ofp, pTxn, pBt);
        if (ret < 0) {
          return -1;
        }

        ofpCell = tdbPageGetCell(ofp, 0);

        // load left val data to ovpages
        lastPage = 0;
        if (nLeft <= ofp->maxLocal - sizeof(SPgno)) {
          bytes = nLeft;
          lastPage = 1;
        } else {
          bytes = ofp->maxLocal - sizeof(SPgno);
        }

        if (lastPage) {
          pgno = 0;
        }

        if (!pDecoder->pVal) {
          pDecoder->pVal = tdbRealloc(pDecoder->pVal, vLen);
          if (pDecoder->pVal == NULL) {
            return -1;
          }
          TDB_CELLDECODER_SET_FREE_VAL(pDecoder);
        }

        memcpy(pDecoder->pVal, ofpCell + vLen - nLeft, bytes);
        nLeft -= bytes;

        memcpy(&pgno, ofpCell + vLen - nLeft + bytes, sizeof(pgno));

        tdbPCacheRelease(pBt->pPager->pCache, ofp, pTxn);

        nLeft -= bytes;
      }
    }
  }

  return 0;
}

static int tdbBtreeDecodeCell(SPage *pPage, const SCell *pCell, SCellDecoder *pDecoder, TXN *pTxn, SBTree *pBt) {
  u8  leaf;
  int nHeader;
  int ret;

  nHeader = 0;
  leaf = TDB_BTREE_PAGE_IS_LEAF(pPage);

  // Clear the state of decoder
  if (TDB_CELLDECODER_FREE_VAL(pDecoder)) {
    tdbFree(pDecoder->pVal);
    TDB_CELLDECODER_CLZ_FREE_VAL(pDecoder);
    // tdbTrace("tdb btc decoder val set nil: %p/0x%x ", pDecoder, pDecoder->freeKV);
  }
  if (TDB_CELLDECODER_FREE_KEY(pDecoder)) {
    tdbFree(pDecoder->pKey);
    TDB_CELLDECODER_CLZ_FREE_KEY(pDecoder);
    // tdbTrace("tdb btc decoder key set nil: %p/0x%x ", pDecoder, pDecoder->freeKV);
  }
  pDecoder->kLen = -1;
  pDecoder->pKey = NULL;
  pDecoder->vLen = -1;
  pDecoder->pVal = NULL;
  pDecoder->pgno = 0;

  // 1. Decode header part
  if (!leaf) {
    if (pPage->vLen != sizeof(SPgno)) {
      tdbError("tdb/btree-decode-cell: invalid cell.");
      return -1;
    }

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
    if (!leaf) {
      tdbError("tdb/btree-decode-cell: not a leaf page.");
      return -1;
    }
    nHeader += tdbGetVarInt(pCell + nHeader, &(pDecoder->vLen));
  } else {
    pDecoder->vLen = pPage->vLen;
  }

  // 2. Decode payload part
  ret = tdbBtreeDecodePayload(pPage, pCell, nHeader, pDecoder, pTxn, pBt);
  if (ret < 0) {
    return -1;
  }

  return 0;
}

static int tdbBtreeCellSize(const SPage *pPage, SCell *pCell, int dropOfp, TXN *pTxn, SBTree *pBt) {
  u8  leaf;
  int kLen = 0, vLen = 0, nHeader = 0;

  leaf = TDB_BTREE_PAGE_IS_LEAF(pPage);

  if (!leaf) {
    nHeader += sizeof(SPgno);
  }

  if (pPage->kLen == TDB_VARIANT_LEN) {
    nHeader += tdbGetVarInt(pCell + nHeader, &kLen);
  } else {
    kLen = pPage->kLen;
  }

  if (pPage->vLen == TDB_VARIANT_LEN) {
    if (!leaf) {
      tdbError("tdb/btree-cell-size: not a leaf page:%p, pgno:%" PRIu32 ".", pPage, TDB_PAGE_PGNO(pPage));
      // return -1;
    }
    nHeader += tdbGetVarInt(pCell + nHeader, &vLen);
  } else if (leaf) {
    vLen = pPage->vLen;
  }

  int nPayload = kLen + vLen;
  if (nHeader + nPayload <= pPage->maxLocal) {
    return nHeader + nPayload;
  } else {
    int maxLocal = pPage->maxLocal;

    // calc local storage size
    int minLocal = pPage->minLocal;
    int surplus = minLocal + (nPayload + nHeader - minLocal) % (maxLocal - sizeof(SPgno));
    int nLocal = surplus <= maxLocal ? surplus : minLocal;

    // free ofp pages' cells
    if (dropOfp) {
      int    ret = 0;
      SPgno  pgno = *(SPgno *)(pCell + nLocal - sizeof(SPgno));
      int    nLeft = nPayload - nLocal + sizeof(SPgno) + nHeader;
      SPage *ofp;
      int    bytes;

      while (pgno != 0) {
        ret = tdbLoadOvflPage(&pgno, &ofp, pTxn, pBt);
        if (ret < 0) {
          return -1;
        }

        SCell *ofpCell = tdbPageGetCell(ofp, 0);

        if (nLeft <= ofp->maxLocal - sizeof(SPgno)) {
          bytes = nLeft;
        } else {
          bytes = ofp->maxLocal - sizeof(SPgno);
        }

        // SPgno origPgno = pgno;
        memcpy(&pgno, ofpCell + bytes, sizeof(pgno));

        ret = tdbPagerWrite(pBt->pPager, ofp);
        if (ret < 0) {
          tdbError("failed to write page since %s", terrstr());
          return -1;
        }
        /*
        tdbPageDropCell(ofp, 0, pTxn, pBt);
        */
        // SIntHdr *pIntHdr = (SIntHdr *)(ofp->pData);
        //  pIntHdr->flags = TDB_FLAG_ADD(0, TDB_BTREE_OVFL);
        // pIntHdr->pgno = 0;
        // ofp->pPager = NULL;

        SArray *ofps = pPage->pPager->ofps;
        if (ofps) {
          taosArrayPush(ofps, &ofp);
        }

        tdbPagerReturnPage(pPage->pPager, ofp, pTxn);

        nLeft -= bytes;
      }
    }

    return nLocal;
  }
}
// TDB_BTREE_CELL

// TDB_BTREE_CURSOR =====================
int tdbBtcOpen(SBTC *pBtc, SBTree *pBt, TXN *pTxn) {
  pBtc->pBt = pBt;
  pBtc->iPage = -1;
  pBtc->pPage = NULL;
  pBtc->idx = -1;
  memset(&pBtc->coder, 0, sizeof(SCellDecoder));

  if (pTxn == NULL) {
    TXN *pTxn = tdbOsCalloc(1, sizeof(*pTxn));
    if (!pTxn) {
      return -1;
    }

    if (tdbTxnOpen(pTxn, 0, tdbDefaultMalloc, tdbDefaultFree, NULL, 0) < 0) {
      tdbOsFree(pTxn);
      return -1;
    }

    pBtc->pTxn = pTxn;
    pBtc->freeTxn = 1;
  } else {
    pBtc->pTxn = pTxn;
    pBtc->freeTxn = 0;
  }

  return 0;
}

int tdbBtcMoveToFirst(SBTC *pBtc) {
  int     ret;
  SBTree *pBt;
  SPager *pPager;
  SCell  *pCell;
  SPgno   pgno;

  pBt = pBtc->pBt;
  pPager = pBt->pPager;

  if (pBtc->iPage < 0) {
    // move a clean cursor
    ret = tdbPagerFetchPage(pPager, &pBt->root, &(pBtc->pPage), tdbBtreeInitPage,
                            &((SBtreeInitPageArg){.pBt = pBt, .flags = TDB_BTREE_ROOT | TDB_BTREE_LEAF}), pBtc->pTxn);
    if (ret < 0) {
      tdbError("tdb/btc-move-tofirst: fetch page failed with ret: %d.", ret);
      return -1;
    }

    if (!TDB_BTREE_PAGE_IS_ROOT(pBtc->pPage)) {
      tdbError("tdb/btc-move-tofirst: not a root page");
      return -1;
    }

    pBtc->iPage = 0;
    if (TDB_PAGE_TOTAL_CELLS(pBtc->pPage) > 0) {
      pBtc->idx = 0;
    } else {
      // no any data, point to an invalid position
      if (!TDB_BTREE_PAGE_IS_LEAF(pBtc->pPage)) {
        tdbError("tdb/btc-move-to-first: not a leaf page.");
        return -1;
      }

      pBtc->idx = -1;
      return 0;
    }
  } else {
    // TODO
    tdbError("tdb/btc-move-to-first: move from a dirty cursor.");
    return -1;
#if 0
    // move from a position
    int iPage = 0;

    for (; iPage < pBtc->iPage; iPage++) {
      if (pBtc->idxStack[iPage] < 0) {
        tdbError("tdb/btc-move-to-first: invalid idx: %d.", pBtc->idxStack[iPage]);
        return -1;
      }

      if (pBtc->idxStack[iPage]) break;
    }

    // move upward
    for (;;) {
      if (pBtc->iPage == iPage) {
        pBtc->idx = 0;
        break;
      }

      tdbBtcMoveUpward(pBtc);
    }
#endif
  }

  // move downward
  for (;;) {
    if (TDB_BTREE_PAGE_IS_LEAF(pBtc->pPage)) break;

    ret = tdbBtcMoveDownward(pBtc);
    if (ret < 0) {
      tdbError("tdb/btc-move-tofirst: btc move downward failed with ret: %d.", ret);
      return -1;
    }

    pBtc->idx = 0;
  }

  return 0;
}

int tdbBtcMoveToLast(SBTC *pBtc) {
  int     ret;
  int     nCells;
  SBTree *pBt;
  SPager *pPager;
  SPgno   pgno;

  pBt = pBtc->pBt;
  pPager = pBt->pPager;

  if (pBtc->iPage < 0) {
    // move a clean cursor
    ret = tdbPagerFetchPage(pPager, &pBt->root, &(pBtc->pPage), tdbBtreeInitPage,
                            &((SBtreeInitPageArg){.pBt = pBt, .flags = TDB_BTREE_ROOT | TDB_BTREE_LEAF}), pBtc->pTxn);
    if (ret < 0) {
      tdbError("tdb/btc-move-tolast: fetch page failed with ret: %d.", ret);
      return -1;
    }

    nCells = TDB_PAGE_TOTAL_CELLS(pBtc->pPage);
    pBtc->iPage = 0;
    if (nCells > 0) {
      pBtc->idx = TDB_BTREE_PAGE_IS_LEAF(pBtc->pPage) ? nCells - 1 : nCells;
    } else {
      // no data at all, point to an invalid position
      if (!TDB_BTREE_PAGE_IS_LEAF(pBtc->pPage)) {
        tdbError("tdb/btc-move-to-last: not a leaf page.");
        return -1;
      }

      pBtc->idx = -1;
      return 0;
    }
  } else {
    // TODO
    tdbError("tdb/btc-move-to-last: move from a dirty cursor.");
    return -1;
#if 0
    int iPage = 0;

    // downward search
    for (; iPage < pBtc->iPage; iPage++) {
      if (TDB_BTREE_PAGE_IS_LEAF(pBtc->pgStack[iPage])) {
        tdbError("tdb/btc-move-to-last: leaf page in cursor stack.");
        return -1;
      }

      nCells = TDB_PAGE_TOTAL_CELLS(pBtc->pgStack[iPage]);
      if (pBtc->idxStack[iPage] != nCells) break;
    }

    // move upward
    for (;;) {
      if (pBtc->iPage == iPage) {
        if (TDB_BTREE_PAGE_IS_LEAF(pBtc->pPage)) {
          pBtc->idx = TDB_PAGE_TOTAL_CELLS(pBtc->pPage) - 1;
        } else {
          pBtc->idx = TDB_PAGE_TOTAL_CELLS(pBtc->pPage);
        }
        break;
      }

      tdbBtcMoveUpward(pBtc);
    }
#endif
  }

  // move downward
  for (;;) {
    if (TDB_BTREE_PAGE_IS_LEAF(pBtc->pPage)) break;

    ret = tdbBtcMoveDownward(pBtc);
    if (ret < 0) {
      tdbError("tdb/btc-move-tolast: btc move downward failed with ret: %d.", ret);
      return -1;
    }

    nCells = TDB_PAGE_TOTAL_CELLS(pBtc->pPage);
    if (TDB_BTREE_PAGE_IS_LEAF(pBtc->pPage)) {
      pBtc->idx = nCells - 1;
    } else {
      pBtc->idx = nCells;
    }
  }

  return 0;
}

int tdbBtreeNext(SBTC *pBtc, void **ppKey, int *kLen, void **ppVal, int *vLen) {
  SCell       *pCell;
  SCellDecoder cd = {0};
  void        *pKey, *pVal;
  int          ret;

  // current cursor points to an invalid position
  if (pBtc->idx < 0) {
    return -1;
  }

  pCell = tdbPageGetCell(pBtc->pPage, pBtc->idx);

  tdbBtreeDecodeCell(pBtc->pPage, pCell, &cd, pBtc->pTxn, pBtc->pBt);

  pKey = tdbRealloc(*ppKey, cd.kLen);
  if (pKey == NULL) {
    return -1;
  }

  *ppKey = pKey;
  *kLen = cd.kLen;
  memcpy(pKey, cd.pKey, (size_t)cd.kLen);

  if (ppVal) {
    if (cd.vLen > 0) {
      pVal = tdbRealloc(*ppVal, cd.vLen);
      if (pVal == NULL) {
        tdbFree(pKey);
        return -1;
      }

      memcpy(pVal, cd.pVal, cd.vLen);
      if (TDB_CELLDECODER_FREE_VAL(&cd)) {
        tdbTrace("tdb/btree-next decoder: %p pVal free: %p", &cd, cd.pVal);
        tdbFree(cd.pVal);
      }
    } else {
      pVal = NULL;
    }

    *ppVal = pVal;
    *vLen = cd.vLen;
  } else {
    if (TDB_CELLDECODER_FREE_VAL(&cd)) {
      tdbTrace("tdb/btree-next2 decoder: %p pVal free: %p", &cd, cd.pVal);
      tdbFree(cd.pVal);
    }
  }

  ret = tdbBtcMoveToNext(pBtc);
  if (ret < 0) {
    tdbError("tdb/btree-next: btc move to next failed with ret: %d.", ret);
    return -1;
  }

  return 0;
}

int tdbBtreePrev(SBTC *pBtc, void **ppKey, int *kLen, void **ppVal, int *vLen) {
  SCell       *pCell;
  SCellDecoder cd = {0};
  void        *pKey, *pVal;
  int          ret;

  // current cursor points to an invalid position
  if (pBtc->idx < 0) {
    return -1;
  }

  pCell = tdbPageGetCell(pBtc->pPage, pBtc->idx);

  tdbBtreeDecodeCell(pBtc->pPage, pCell, &cd, pBtc->pTxn, pBtc->pBt);

  pKey = tdbRealloc(*ppKey, cd.kLen);
  if (pKey == NULL) {
    return -1;
  }

  *ppKey = pKey;
  *kLen = cd.kLen;
  memcpy(pKey, cd.pKey, (size_t)cd.kLen);

  if (ppVal) {
    // TODO: vLen may be zero
    pVal = tdbRealloc(*ppVal, cd.vLen);
    if (pVal == NULL) {
      tdbFree(pKey);
      return -1;
    }

    *ppVal = pVal;
    *vLen = cd.vLen;
    memcpy(pVal, cd.pVal, (size_t)cd.vLen);
  }

  ret = tdbBtcMoveToPrev(pBtc);
  if (ret < 0) {
    tdbError("tdb/btree-prev: btc move to prev failed with ret: %d.", ret);
    return -1;
  }

  return 0;
}

int tdbBtcMoveToNext(SBTC *pBtc) {
  int    nCells;
  int    ret;
  SCell *pCell;

  if (!TDB_BTREE_PAGE_IS_LEAF(pBtc->pPage)) {
    tdbError("tdb/btc-move-to-next: not a leaf page.");
    return -1;
  }

  if (pBtc->idx < 0) return -1;

  pBtc->idx++;
  if (pBtc->idx < TDB_PAGE_TOTAL_CELLS(pBtc->pPage)) {
    return 0;
  }

  // move upward
  for (;;) {
    if (pBtc->iPage == 0) {
      pBtc->idx = -1;
      return 0;
    }

    tdbBtcMoveUpward(pBtc);
    pBtc->idx++;

    if (TDB_BTREE_PAGE_IS_LEAF(pBtc->pPage)) {
      tdbError("tdb/btree-decode-cell: should not be a leaf page here.");
      return -1;
    }
    if (pBtc->idx <= TDB_PAGE_TOTAL_CELLS(pBtc->pPage)) {
      break;
    }
  }

  // move downward
  for (;;) {
    if (TDB_BTREE_PAGE_IS_LEAF(pBtc->pPage)) break;

    ret = tdbBtcMoveDownward(pBtc);
    if (ret < 0) {
      tdbError("tdb/btc-move-tonext: btc move downward failed with ret: %d.", ret);
      return -1;
    }

    pBtc->idx = 0;
  }

  return 0;
}

int tdbBtcMoveToPrev(SBTC *pBtc) {
  if (pBtc->idx < 0) return -1;

  pBtc->idx--;
  if (pBtc->idx >= 0) {
    return 0;
  }

  // move upward
  for (;;) {
    if (pBtc->iPage == 0) {
      pBtc->idx = -1;
      return 0;
    }

    tdbBtcMoveUpward(pBtc);
    pBtc->idx--;
    if (pBtc->idx >= 0) {
      break;
    }
  }

  // move downward
  for (;;) {
    if (TDB_BTREE_PAGE_IS_LEAF(pBtc->pPage)) break;

    tdbBtcMoveDownward(pBtc);
    if (TDB_BTREE_PAGE_IS_LEAF(pBtc->pPage)) {
      pBtc->idx = TDB_PAGE_TOTAL_CELLS(pBtc->pPage) - 1;
    } else {
      pBtc->idx = TDB_PAGE_TOTAL_CELLS(pBtc->pPage);
    }
  }

  return 0;
}

static int tdbBtcMoveDownward(SBTC *pBtc) {
  int    ret;
  SPgno  pgno;
  SCell *pCell;

  if (pBtc->idx < 0) {
    tdbError("tdb/btc-move-downward: invalid idx: %d.", pBtc->idx);
    return -1;
  }

  if (TDB_BTREE_PAGE_IS_LEAF(pBtc->pPage)) {
    tdbError("tdb/btc-move-downward: should not be a leaf page here.");
    return -1;
  }

  if (TDB_BTREE_PAGE_IS_OVFL(pBtc->pPage)) {
    tdbError("tdb/btc-move-downward: should not be a ovfl page here.");
    return -1;
  }

  if (pBtc->idx < TDB_PAGE_TOTAL_CELLS(pBtc->pPage)) {
    pCell = tdbPageGetCell(pBtc->pPage, pBtc->idx);
    pgno = ((SPgno *)pCell)[0];
  } else {
    pgno = ((SIntHdr *)pBtc->pPage->pData)->pgno;
  }

  if (!pgno) {
    tdbError("tdb/btc-move-downward: invalid pgno.");
    return -1;
  }

  pBtc->pgStack[pBtc->iPage] = pBtc->pPage;
  pBtc->idxStack[pBtc->iPage] = pBtc->idx;
  pBtc->iPage++;
  pBtc->pPage = NULL;
  pBtc->idx = -1;

  ret = tdbPagerFetchPage(pBtc->pBt->pPager, &pgno, &pBtc->pPage, tdbBtreeInitPage,
                          &((SBtreeInitPageArg){.pBt = pBtc->pBt, .flags = 0}), pBtc->pTxn);
  if (ret < 0) {
    tdbError("tdb/btc-move-downward: fetch page failed with ret: %d.", ret);
    return -1;
  }

  return 0;
}

static int tdbBtcMoveUpward(SBTC *pBtc) {
  if (pBtc->iPage == 0) return -1;

  tdbPagerReturnPage(pBtc->pBt->pPager, pBtc->pPage, pBtc->pTxn);

  pBtc->iPage--;
  pBtc->pPage = pBtc->pgStack[pBtc->iPage];
  pBtc->idx = pBtc->idxStack[pBtc->iPage];

  return 0;
}

int tdbBtcGet(SBTC *pBtc, const void **ppKey, int *kLen, const void **ppVal, int *vLen) {
  SCell *pCell;

  if (pBtc->idx < 0 || pBtc->idx >= TDB_PAGE_TOTAL_CELLS(pBtc->pPage)) {
    return -1;
  }

  pCell = tdbPageGetCell(pBtc->pPage, pBtc->idx);
  tdbBtreeDecodeCell(pBtc->pPage, pCell, &pBtc->coder, pBtc->pTxn, pBtc->pBt);

  if (ppKey) {
    *ppKey = (void *)pBtc->coder.pKey;
    *kLen = pBtc->coder.kLen;
  }

  if (ppVal) {
    *ppVal = (void *)pBtc->coder.pVal;
    *vLen = pBtc->coder.vLen;
  }

  return 0;
}

int tdbBtcDelete(SBTC *pBtc) {
  int         idx = pBtc->idx;
  int         nCells = TDB_PAGE_TOTAL_CELLS(pBtc->pPage);
  SPager     *pPager = pBtc->pBt->pPager;
  const void *pKey;
  i8          iPage;
  SPage      *pPage;
  SPgno       pgno;
  SCell      *pCell;
  int         szCell;
  int         nKey;
  int         ret;

  if (idx < 0 || idx >= nCells) {
    tdbError("tdb/btc-delete: idx: %d out of range[%d, %d).", idx, 0, nCells);
    return -1;
  }

  // drop the cell on the leaf
  ret = tdbPagerWrite(pPager, pBtc->pPage);
  if (ret < 0) {
    tdbError("failed to write page since %s", terrstr());
    return -1;
  }

  bool destroyOfps = false;
  if (!pBtc->pPage->pPager->ofps) {
    pBtc->pPage->pPager->ofps = taosArrayInit(8, sizeof(SPage *));
    destroyOfps = true;
  }

  tdbPageDropCell(pBtc->pPage, idx, pBtc->pTxn, pBtc->pBt);

  SArray *ofps = pBtc->pPage->pPager->ofps;
  if (ofps) {
    for (int i = 0; i < TARRAY_SIZE(ofps); ++i) {
      SPage *ofp = *(SPage **)taosArrayGet(ofps, i);
      tdbPagerInsertFreePage(pBtc->pPage->pPager, ofp, pBtc->pTxn);
    }

    if (destroyOfps) {
      taosArrayDestroy(ofps);
      pBtc->pPage->pPager->ofps = NULL;
    }
  }

  // update interior page or do balance
  if (idx == nCells - 1) {
    if (idx) {
      pBtc->idx--;
      tdbBtcGet(pBtc, &pKey, &nKey, NULL, NULL);

      // loop to update the interial page
      pgno = TDB_PAGE_PGNO(pBtc->pPage);
      for (iPage = pBtc->iPage - 1; iPage >= 0; iPage--) {
        pPage = pBtc->pgStack[iPage];
        idx = pBtc->idxStack[iPage];
        nCells = TDB_PAGE_TOTAL_CELLS(pPage);

        if (idx < nCells) {
          ret = tdbPagerWrite(pPager, pPage);
          if (ret < 0) {
            tdbError("failed to write page since %s", terrstr());
            return -1;
          }

          // update the cell with new key
          pCell = tdbOsMalloc(nKey + 9);
          tdbBtreeEncodeCell(pPage, pKey, nKey, &pgno, sizeof(pgno), pCell, &szCell, pBtc->pTxn, pBtc->pBt);

          ret = tdbPageUpdateCell(pPage, idx, pCell, szCell, pBtc->pTxn, pBtc->pBt);
          if (ret < 0) {
            tdbOsFree(pCell);
            tdbError("tdb/btc-delete: page update cell failed with ret: %d.", ret);
            return -1;
          }
          tdbOsFree(pCell);

          if (pPage->nOverflow > 0) {
            tdbDebug("tdb/btc-delete: btree balance after update cell, pPage/nOverflow/pgno: %p/%d/%" PRIu32 ".", pPage,
                     pPage->nOverflow, TDB_PAGE_PGNO(pPage));

            tdbPagerReturnPage(pBtc->pBt->pPager, pBtc->pPage, pBtc->pTxn);
            while (--pBtc->iPage != iPage) {
              tdbPagerReturnPage(pBtc->pBt->pPager, pBtc->pgStack[pBtc->iPage], pBtc->pTxn);
            }

            // pBtc->iPage = iPage;
            pBtc->pPage = pPage;
            ret = tdbBtreeBalance(pBtc);
            if (ret < 0) {
              tdbError("tdb/btc-delete: btree balance failed with ret: %d.", ret);
              return -1;
            }
          }

          break;
        } else {
          pgno = TDB_PAGE_PGNO(pPage);
        }
      }
    } else {
      // delete the leaf page and do balance
      if (TDB_PAGE_TOTAL_CELLS(pBtc->pPage) != 0) {
        tdbError("tdb/btc-delete: page to be deleted should be empty.");
        return -1;
      }

      // printf("tdb/btc-delete: btree balance delete pgno: %d.\n", TDB_PAGE_PGNO(pBtc->pPage));

      ret = tdbBtreeBalance(pBtc);
      if (ret < 0) {
        tdbError("tdb/btc-delete: btree balance failed with ret: %d.", ret);
        return -1;
      }
    }
  }

  return 0;
}

int tdbBtcUpsert(SBTC *pBtc, const void *pKey, int kLen, const void *pData, int nData, int insert) {
  SCell *pCell;
  int    szCell;
  int    nCells = TDB_PAGE_TOTAL_CELLS(pBtc->pPage);
  int    szBuf;
  void  *pBuf;
  int    ret;

  if (pBtc->idx < 0) {
    tdbError("tdb/btc-upsert: invalid idx: %d.", pBtc->idx);
    return -1;
  }

  // alloc space
  szBuf = kLen + nData + 14;
  pBuf = tdbRealloc(pBtc->pBt->pBuf, pBtc->pBt->pageSize > szBuf ? szBuf : pBtc->pBt->pageSize);
  if (pBuf == NULL) {
    tdbError("tdb/btc-upsert: realloc pBuf failed.");
    return -1;
  }
  pBtc->pBt->pBuf = pBuf;
  pCell = (SCell *)pBtc->pBt->pBuf;

  // encode cell
  ret = tdbBtreeEncodeCell(pBtc->pPage, pKey, kLen, pData, nData, pCell, &szCell, pBtc->pTxn, pBtc->pBt);
  if (ret < 0) {
    tdbError("tdb/btc-upsert: btree encode cell failed with ret: %d.", ret);
    return -1;
  }

  // mark dirty
  ret = tdbPagerWrite(pBtc->pBt->pPager, pBtc->pPage);
  if (ret < 0) {
    tdbError("failed to write page since %s", terrstr());
    return -1;
  }

  // insert or update
  if (insert) {
    if (pBtc->idx > nCells) {
      tdbError("tdb/btc-upsert: invalid idx: %d, nCells: %d.", pBtc->idx, nCells);
      return -1;
    }

    ret = tdbPageInsertCell(pBtc->pPage, pBtc->idx, pCell, szCell, 0);
  } else {
    if (pBtc->idx >= nCells) {
      tdbError("tdb/btc-upsert: invalid idx: %d, nCells: %d.", pBtc->idx, nCells);
      return -1;
    }

    ret = tdbPageUpdateCell(pBtc->pPage, pBtc->idx, pCell, szCell, pBtc->pTxn, pBtc->pBt);
  }
  if (ret < 0) {
    tdbError("tdb/btc-upsert: page insert/update cell failed with ret: %d.", ret);
    return -1;
  }
  /*
  bool destroyOfps = false;
  if (!pBtc->pPage->pPager->ofps) {
    pBtc->pPage->pPager->ofps = taosArrayInit(8, sizeof(SPage *));
    destroyOfps = true;
  }
  */
  // check balance
  if (pBtc->pPage->nOverflow > 0) {
    ret = tdbBtreeBalance(pBtc);
    if (ret < 0) {
      tdbError("tdb/btc-upsert: btree balance failed with ret: %d.", ret);
      return -1;
    }
  }
  /*
  SArray *ofps = pBtc->pPage->pPager->ofps;
  if (ofps) {
    for (int i = 0; i < TARRAY_SIZE(ofps); ++i) {
      SPage *ofp = *(SPage **)taosArrayGet(ofps, i);
      tdbPagerInsertFreePage(pBtc->pPage->pPager, ofp, pBtc->pTxn);
    }

    if (destroyOfps) {
      taosArrayDestroy(ofps);
      pBtc->pPage->pPager->ofps = NULL;
    }
  }
  */
  return 0;
}

int tdbBtcMoveTo(SBTC *pBtc, const void *pKey, int kLen, int *pCRst) {
  int         ret;
  int         nCells;
  int         c;
  SCell      *pCell;
  SBTree     *pBt = pBtc->pBt;
  SPager     *pPager = pBt->pPager;
  const void *pTKey;
  int         tkLen;

  tdbTrace("tdb moveto, pager:%p, ipage:%d", pPager, pBtc->iPage);
  if (pBtc->iPage < 0) {
    // move from a clear cursor
    ret = tdbPagerFetchPage(pPager, &pBt->root, &(pBtc->pPage), tdbBtreeInitPage,
                            &((SBtreeInitPageArg){.pBt = pBt, .flags = TDB_BTREE_ROOT | TDB_BTREE_LEAF}), pBtc->pTxn);
    if (ret < 0) {
      // TODO
      tdbError("tdb/btc-move-to: fetch page failed with ret: %d.", ret);
      return -1;
    }

    pBtc->iPage = 0;
    pBtc->idx = -1;
    // for empty tree, just return with an invalid position
    if (TDB_PAGE_TOTAL_CELLS(pBtc->pPage) == 0) return 0;
  } else {
    // TODO
    tdbError("tdb/btc-move-to: move from a dirty cursor.");
    return -1;
#if 0
    SPage *pPage;
    int    idx;
    int    iPage = 0;

    // downward search
    for (; iPage < pBtc->iPage; iPage++) {
      pPage = pBtc->pgStack[iPage];
      idx = pBtc->idxStack[iPage];
      nCells = TDB_PAGE_TOTAL_CELLS(pPage);

      if (TDB_BTREE_PAGE_IS_LEAF(pPage)) {
        tdbError("tdb/btc-move-to: leaf page in cursor stack.");
        return -1;
      }

      // check if key <= current position
      if (idx < nCells) {
        pCell = tdbPageGetCell(pPage, idx);
        tdbBtreeDecodeCell(pPage, pCell, &cd);
        c = pBt->kcmpr(pKey, kLen, cd.pKey, cd.kLen);
        if (c > 0) break;
      }

      // check if key > current - 1 position
      if (idx > 0) {
        pCell = tdbPageGetCell(pPage, idx - 1);
        tdbBtreeDecodeCell(pPage, pCell, &cd);
        c = pBt->kcmpr(pKey, kLen, cd.pKey, cd.kLen, pBtc->pTxn, pBtc->pBt);
        if (c <= 0) break;
      }
    }

    // move upward
    for (;;) {
      if (pBtc->iPage == iPage) break;
      tdbBtcMoveUpward(pBtc);
    }
#endif
  }

  // search downward to the leaf
  tdbTrace("tdb search downward, pager:%p, ipage:%d", pPager, pBtc->iPage);
  for (;;) {
    int    lidx, ridx;
    SPage *pPage;

    pPage = pBtc->pPage;
    nCells = TDB_PAGE_TOTAL_CELLS(pPage);
    lidx = 0;
    ridx = nCells - 1;

    ASSERT(nCells > 0);

    // compare first cell
    pBtc->idx = lidx;
    tdbBtcGet(pBtc, &pTKey, &tkLen, NULL, NULL);
    c = pBt->kcmpr(pKey, kLen, pTKey, tkLen);
    if (c <= 0) {
      ridx = lidx - 1;
    } else {
      lidx = lidx + 1;
    }
    // compare last cell
    if (lidx <= ridx) {
      pBtc->idx = ridx;
      tdbBtcGet(pBtc, &pTKey, &tkLen, NULL, NULL);
      c = pBt->kcmpr(pKey, kLen, pTKey, tkLen);
      if (c >= 0) {
        lidx = ridx + 1;
      } else {
        ridx = ridx - 1;
      }
    }

    // binary search
    tdbTrace("tdb binary search, pager:%p, ipage:%d", pPager, pBtc->iPage);
    for (;;) {
      if (lidx > ridx) break;

      pBtc->idx = (lidx + ridx) >> 1;
      tdbBtcGet(pBtc, &pTKey, &tkLen, NULL, NULL);
      c = pBt->kcmpr(pKey, kLen, pTKey, tkLen);
      if (c < 0) {
        // pKey < cd.pKey
        ridx = pBtc->idx - 1;
      } else if (c > 0) {
        // pKey > cd.pKey
        lidx = pBtc->idx + 1;
      } else {
        // pKey == cd.pKey
        break;
      }
    }

    // keep search downward or break
    if (TDB_BTREE_PAGE_IS_LEAF(pPage)) {
      *pCRst = c;
      break;
    } else {
      if (c > 0) {
        pBtc->idx += 1;
      }
      tdbBtcMoveDownward(pBtc);
    }
  }

  tdbTrace("tdb moveto end, pager:%p, ipage:%d", pPager, pBtc->iPage);

  return 0;
}

int tdbBtcClose(SBTC *pBtc) {
  if (pBtc->iPage < 0) return 0;

  for (;;) {
    if (NULL == pBtc->pPage) {
      tdbError("tdb/btc-close: null ptr pPage.");
      return -1;
    }

    tdbPagerReturnPage(pBtc->pBt->pPager, pBtc->pPage, pBtc->pTxn);

    pBtc->iPage--;
    if (pBtc->iPage < 0) break;

    pBtc->pPage = pBtc->pgStack[pBtc->iPage];
    pBtc->idx = pBtc->idxStack[pBtc->iPage];
  }

  if (TDB_CELLDECODER_FREE_VAL(&pBtc->coder)) {
    tdbDebug("tdb btc/close decoder: %p pVal free: %p", &pBtc->coder, pBtc->coder.pVal);

    tdbFree(pBtc->coder.pVal);
  }

  if (pBtc->freeTxn) {
    tdbTxnClose(pBtc->pTxn);
  }

  return 0;
}

int tdbBtcIsValid(SBTC *pBtc) {
  if (pBtc->idx < 0) {
    return 0;
  } else {
    return 1;
  }
}
// TDB_BTREE_CURSOR

// TDB_BTREE_DEBUG =====================
#ifndef NODEBUG
typedef struct {
  SPgno pgno;
  u8    root;
  u8    leaf;
  SPgno rChild;
  int   nCells;
  int   nOvfl;
} SBtPageInfo;

SBtPageInfo btPageInfos[20];

void tdbBtPageInfo(SPage *pPage, int idx) {
  SBtPageInfo *pBtPageInfo;

  pBtPageInfo = btPageInfos + idx;

  pBtPageInfo->pgno = TDB_PAGE_PGNO(pPage);

  pBtPageInfo->root = TDB_BTREE_PAGE_IS_ROOT(pPage);
  pBtPageInfo->leaf = TDB_BTREE_PAGE_IS_LEAF(pPage);

  pBtPageInfo->rChild = 0;
  if (!pBtPageInfo->leaf) {
    pBtPageInfo->rChild = *(SPgno *)(pPage->pData + 1);
  }

  pBtPageInfo->nCells = TDB_PAGE_TOTAL_CELLS(pPage) - pPage->nOverflow;
  pBtPageInfo->nOvfl = pPage->nOverflow;
}
#endif
// TDB_BTREE_DEBUG
