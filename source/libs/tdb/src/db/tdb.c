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

struct STDb {
  // TODO
  SBTree *pBt;
};

int tdbCreate(TDB **ppDb) {
  TDB *pDb;

  pDb = (TDB *)calloc(1, sizeof(*pDb));
  if (pDb == NULL) {
    return -1;
  }

  /* TODO */

  return 0;
}

int tdbDestroy(TDB *pDb) {
  if (pDb) {
    free(pDb);
  }
  /* TODO */
  return 0;
}

int tdbOpen(TDB **pDb) {
  // TODO
  return 0;
}

int tdbClose(TDB *pDb) {
  // TODO
  return 0;
}