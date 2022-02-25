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

#define BTREE_MAX_DEPTH 20

struct SBTree {
  SPgno   root;
  int     keyLen;
  int     valLen;
  SPFile *pFile;
  int (*FKeyComparator)(const void *pKey1, int keyLen1, const void *pKey2, int keyLen2);
};

struct SBtCursor {
  SBTree *  pBt;
  i8        iPage;
  // SMemPage *pPage;
  // SMemPage *apPage[BTREE_MAX_DEPTH + 1];
};

// typedef struct SMemPage {
//   u8    isInit;
//   u8    isLeaf;
//   SPgno pgno;
// } SMemPage;

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
  /* TODO */
  return 0;
}

int tdbBtreeCursorMoveTo(SBtCursor *pCur) {
  /* TODO */
  return 0;
}

static int tdbBtreeCursorMoveToRoot(SBtCursor *pCur) {
  SPFile *pFile;
  SPage * pPage;

  pFile = pCur->pBt->pFile;

  pPage = tdbPFileGet(pFile, pCur->pBt->root);
  if (pPage == NULL) {
    return -1;
  }

  return 0;
}