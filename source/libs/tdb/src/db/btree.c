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
  // TODO
};

static int btreeCreate(SBTree **pBt);
static int btreeDestroy(SBTree *pBt);

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