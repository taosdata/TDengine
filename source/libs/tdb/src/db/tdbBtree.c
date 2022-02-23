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

struct SBTree {
  SPgno   root;
  int     keyLen;
  int     valLen;
  SPFile *pFile;
  int (*FKeyComparator)(const void *pKey1, int keyLen1, const void *pKey2, int keyLen2);
};

struct SBtCursor {
  SBTree *pBt;
  int8_t  iPage;
};

typedef struct SBPage {
  u8    isInit;
  u8    isLeaf;
  SPgno pgno;
} SBPage;

int tdbBtreeOpen(SPgno root, SBTree **ppBt) {
  *ppBt = NULL;
  /* TODO */
  return 0;
}

int tdbBtreeClose(SBTree *pBt) {
  // TODO
  return 0;
}

int tdbBtreeCursor(SBTree *pBt, SBtCursor *pCur) {
  pCur->pBt = pBt;
  pCur->iPage = -1;
  return 0;
}

int tdbBtreeCursorMoveTo(SBtCursor *pCur) {
  /* TODO */
  return 0;
}

static int tdbBtreeCursorMoveToRoot(SBtCursor *pCur) {
  SPFile *pFile;
  SPgHdr *pPage;

  pFile = pCur->pBt->pFile;

  pPage = tdbPFileGet(pFile, pCur->pBt->root);
  if (pPage == NULL) {
    return -1;
  }

  return 0;
}

#if 0
struct SBtCursor {
  SBTree *pBtree;
  SPgno  pgno;
  SPage * pPage;  // current page traversing
};

typedef struct {
  SPgno pgno;
  pgsz_t offset;
} SBtIdx;

// Btree page header definition
typedef struct __attribute__((__packed__)) {
  uint8_t  flag;        // page flag
  int32_t  vlen;        // value length of current page, TDB_VARIANT_LEN for variant length
  uint16_t nPayloads;   // number of total payloads
  pgoff_t  freeOff;     // free payload offset
  pgsz_t   fragSize;    // total fragment size
  pgoff_t  offPayload;  // payload offset
  SPgno   rChildPgno;  // right most child page number
} SBtPgHdr;

typedef int (*BtreeCmprFn)(const void *, const void *);

#define BTREE_PAGE_HDR(pPage)             NULL /* TODO */
#define BTREE_PAGE_PAYLOAD_AT(pPage, idx) NULL /*TODO*/
#define BTREE_PAGE_IS_LEAF(pPage)         0    /* TODO */

static int btreeCreate(SBTree **ppBt);
static int btreeDestroy(SBTree *pBt);
static int btreeCursorMoveToChild(SBtCursor *pBtCur, SPgno pgno);

int btreeOpen(SBTree **ppBt, SPgFile *pPgFile) {
  SBTree *pBt;
  int     ret;

  ret = btreeCreate(&pBt);
  if (ret != 0) {
    return -1;
  }

  *ppBt = pBt;
  return 0;
}

int btreeClose(SBTree *pBt) {
  // TODO
  return 0;
}

static int btreeCreate(SBTree **ppBt) {
  SBTree *pBt;

  pBt = (SBTree *)calloc(1, sizeof(*pBt));
  if (pBt == NULL) {
    return -1;
  }

  // TODO
  return 0;
}

static int btreeDestroy(SBTree *pBt) {
  if (pBt) {
    free(pBt);
  }
  return 0;
}

int btreeCursorOpen(SBtCursor *pBtCur, SBTree *pBt) {
  // TODO
  return 0;
}

int btreeCursorClose(SBtCursor *pBtCur) {
  // TODO
  return 0;
}

int btreeCursorMoveTo(SBtCursor *pBtCur, int kLen, const void *pKey) {
  SPage *     pPage;
  SBtPgHdr *  pBtPgHdr;
  SPgFile *   pPgFile;
  SPgno      childPgno;
  SPgno      rootPgno;
  int         nPayloads;
  void *      pPayload;
  BtreeCmprFn cmpFn;

  // 1. Move the cursor to the root page
  if (rootPgno == TDB_IVLD_PGNO) {
    // No any data in this btree, just return not found (TODO)
    return 0;
  } else {
    // Load the page from the file by the SPgFile handle
    pPage = pgFileFetch(pPgFile, rootPgno);

    pBtCur->pPage = pPage;
  }

  // 2. Loop to search over the whole tree
  for (;;) {
    int lidx, ridx, midx, cret;

    pPage = pBtCur->pPage;
    pBtPgHdr = BTREE_PAGE_HDR(pPage);
    nPayloads = pBtPgHdr->nPayloads;

    // Binary search the page
    lidx = 0;
    ridx = nPayloads - 1;
    midx = (lidx + ridx) >> 1;
    for (;;) {
      // get the payload ptr at midx
      pPayload = BTREE_PAGE_PAYLOAD_AT(pPage, midx);

      // the payload and the key
      cret = cmpFn(pKey, pPayload);

      if (cret < 0) {
        /* TODO */
      } else if (cret > 0) {
        /* TODO */
      } else {
        /* TODO */
      }

      if (lidx > ridx) break;
      midx = (lidx + ridx) >> 1;
    }
    if (BTREE_PAGE_IS_LEAF(pPage)) {
      /* TODO */
      break;
    } else {
      /* TODO */
      btreeCursorMoveToChild(pBtCur, childPgno);
    }
  }

  return 0;
}

static int btreeCursorMoveToChild(SBtCursor *pBtCur, SPgno pgno) {
  SPgFile *pPgFile;
  // TODO
  return 0;
}
#endif