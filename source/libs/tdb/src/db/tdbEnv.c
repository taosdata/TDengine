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

int tdbEnvOpen(const char *rootDir, int szPage, int pages, TENV **ppEnv) {
  TENV *pEnv;
  int   dsize;
  int   zsize;
  int   tsize;
  u8   *pPtr;
  int   ret;

  *ppEnv = NULL;

  dsize = strlen(rootDir);
  zsize = sizeof(*pEnv) + dsize * 2 + strlen(TDB_JOURNAL_NAME) + 3;

  pPtr = (uint8_t *)tdbOsCalloc(1, zsize);
  if (pPtr == NULL) {
    return -1;
  }

  pEnv = (TENV *)pPtr;
  pPtr += sizeof(*pEnv);
  // pEnv->rootDir
  pEnv->rootDir = pPtr;
  memcpy(pEnv->rootDir, rootDir, dsize);
  pEnv->rootDir[dsize] = '\0';
  pPtr = pPtr + dsize + 1;
  // pEnv->jfname
  pEnv->jfname = pPtr;
  memcpy(pEnv->jfname, rootDir, dsize);
  pEnv->jfname[dsize] = '/';
  memcpy(pEnv->jfname + dsize + 1, TDB_JOURNAL_NAME, strlen(TDB_JOURNAL_NAME));
  pEnv->jfname[dsize + 1 + strlen(TDB_JOURNAL_NAME)] = '\0';

  pEnv->jfd = -1;

  ret = tdbPCacheOpen(szPage, pages, &(pEnv->pCache));
  if (ret < 0) {
    return -1;
  }

  pEnv->nPgrHash = 8;
  tsize = sizeof(SPager *) * pEnv->nPgrHash;
  pEnv->pgrHash = tdbOsMalloc(tsize);
  if (pEnv->pgrHash == NULL) {
    return -1;
  }
  memset(pEnv->pgrHash, 0, tsize);

  mkdir(rootDir, 0755);

  *ppEnv = pEnv;
  return 0;
}

int tdbEnvClose(TENV *pEnv) {
  SPager *pPager;

  if (pEnv) {
    for (pPager = pEnv->pgrList; pPager; pPager = pEnv->pgrList) {
      pEnv->pgrList = pPager->pNext;
      tdbPagerClose(pPager);
    }

    tdbPCacheClose(pEnv->pCache);
    tdbOsFree(pEnv->pgrHash);
    tdbOsFree(pEnv);
  }

  return 0;
}

int tdbBegin(TENV *pEnv, TXN *pTxn) {
  SPager *pPager;
  int     ret;

  for (pPager = pEnv->pgrList; pPager; pPager = pPager->pNext) {
    ret = tdbPagerBegin(pPager, pTxn);
    if (ret < 0) {
      ASSERT(0);
      return -1;
    }
  }

  return 0;
}

int tdbCommit(TENV *pEnv, TXN *pTxn) {
  SPager *pPager;
  int     ret;

  for (pPager = pEnv->pgrList; pPager; pPager = pPager->pNext) {
    ret = tdbPagerCommit(pPager, pTxn);
    if (ret < 0) {
      ASSERT(0);
      return -1;
    }
  }

  return 0;
}

SPager *tdbEnvGetPager(TENV *pEnv, const char *fname) {
  u32      hash;
  SPager **ppPager;

  hash = tdbCstringHash(fname);
  ppPager = &pEnv->pgrHash[hash % pEnv->nPgrHash];
  for (; *ppPager && (strcmp(fname, (*ppPager)->dbFileName) != 0); ppPager = &((*ppPager)->pHashNext)) {
  }

  return *ppPager;
}

void tdbEnvAddPager(TENV *pEnv, SPager *pPager) {
  u32      hash;
  SPager **ppPager;

  // rehash if neccessary
  if (pEnv->nPager + 1 > pEnv->nPgrHash) {
    // TODO
  }

  // add to list
  pPager->pNext = pEnv->pgrList;
  pEnv->pgrList = pPager;

  // add to hash
  hash = tdbCstringHash(pPager->dbFileName);
  ppPager = &pEnv->pgrHash[hash % pEnv->nPgrHash];
  pPager->pHashNext = *ppPager;
  *ppPager = pPager;

  // increase the counter
  pEnv->nPager++;
}

void tdbEnvRemovePager(TENV *pEnv, SPager *pPager) {
  u32      hash;
  SPager **ppPager;

  // remove from the list
  for (ppPager = &pEnv->pgrList; *ppPager && (*ppPager != pPager); ppPager = &((*ppPager)->pNext)) {
  }
  ASSERT(*ppPager == pPager);
  *ppPager = pPager->pNext;

  // remove from hash
  hash = tdbCstringHash(pPager->dbFileName);
  ppPager = &pEnv->pgrHash[hash % pEnv->nPgrHash];
  for (; *ppPager && *ppPager != pPager; ppPager = &((*ppPager)->pHashNext)) {
  }
  ASSERT(*ppPager == pPager);
  *ppPager = pPager->pNext;

  // decrease the counter
  pEnv->nPager--;

  // rehash if necessary
  if (pEnv->nPgrHash > 8 && pEnv->nPager < pEnv->nPgrHash / 2) {
    // TODO
  }
}