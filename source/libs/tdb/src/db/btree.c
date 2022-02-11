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

struct SBtCursor {
  SBTree *pBtree;
  pgno_t  pgno;
  SPage * pPage;  // current page traversing
};

typedef struct {
  pgno_t   pgno;
  pgsize_t offset;
} SBtIdx;

static int btreeCreate(SBTree **pBt);
static int btreeDestroy(SBTree *pBt);
static int btreeCursorMoveToChild(SBtCursor *pBtCur, pgno_t pgno);

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

static int btreeCreate(SBTree **pBt) {
  // TODO
  return 0;
}

static int btreeDestroy(SBTree *pBt) {
  // TODO
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
  SPage *pPage;
  pgno_t childPgno;
  int    idx;

  // 1. Move the cursor to the root page

  // 2. Loop to search over the whole tree
  for (;;) {
    pPage = pBtCur->pPage;

    // Loop to search in current page
    for (;;) {
      /* code */
    }

    btreeCursorMoveToChild(pBtCur, childPgno);
  }

  return 0;
}

static int btreeCursorMoveToChild(SBtCursor *pBtCur, pgno_t pgno) {
  SPgFile *pPgFile;
  // TODO
  return 0;
}