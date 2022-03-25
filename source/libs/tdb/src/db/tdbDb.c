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
  STEnv  *pEnv;
  SBTree *pBt;
};

int tdbDbOpen(const char *fname, int keyLen, int valLen, FKeyComparator keyCmprFn, STEnv *pEnv, STDb **ppDb) {
  STDb   *pDb;
  SPager *pPager;
  int     ret;
  char    fFullName[TDB_FILENAME_LEN];
  SPage  *pPage;
  SPgno   pgno;

  *ppDb = NULL;

  pDb = (STDb *)taosMemoryCalloc(1, sizeof(*pDb));
  if (pDb == NULL) {
    return -1;
  }

  // pDb->pEnv
  pDb->pEnv = pEnv;

  pPager = tdbEnvGetPager(pEnv, fname);
  if (pPager == NULL) {
    snprintf(fFullName, TDB_FILENAME_LEN, "%s/%s", pEnv->rootDir, fname);
    ret = tdbPagerOpen(pEnv->pCache, fFullName, &pPager);
    if (ret < 0) {
      return -1;
    }
  }

  ASSERT(pPager != NULL);

  // pDb->pBt
  ret = tdbBtreeOpen(keyLen, valLen, pPager, keyCmprFn, &(pDb->pBt));
  if (ret < 0) {
    return -1;
  }

  *ppDb = pDb;
  return 0;
}

int tdbDbClose(STDb *pDb) {
  // TODO
  return 0;
}

int tdbDbDrop(STDb *pDb) {
  // TODO
  return 0;
}

int tdbDbInsert(STDb *pDb, const void *pKey, int keyLen, const void *pVal, int valLen) {
  SBtCursor  btc;
  SBtCursor *pCur;
  int        ret;

  pCur = &btc;
  ret = tdbBtreeCursor(pCur, pDb->pBt);
  if (ret < 0) {
    return -1;
  }

  ret = tdbBtCursorInsert(pCur, pKey, keyLen, pVal, valLen);
  if (ret < 0) {
    return -1;
  }

  return 0;
}