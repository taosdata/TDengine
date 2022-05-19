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

int32_t tdbOpen(const char *dbname, int32_t szPage, int32_t pages, TDB **ppDb) {
  TDB *pDb;
  int  dsize;
  int  zsize;
  int  tsize;
  u8  *pPtr;
  int  ret;

  *ppDb = NULL;

  dsize = strlen(dbname);
  zsize = sizeof(*pDb) + dsize * 2 + strlen(TDB_JOURNAL_NAME) + 3;

  pPtr = (uint8_t *)tdbOsCalloc(1, zsize);
  if (pPtr == NULL) {
    return -1;
  }

  pDb = (TDB *)pPtr;
  pPtr += sizeof(*pDb);
  // pDb->rootDir
  pDb->dbName = pPtr;
  memcpy(pDb->dbName, dbname, dsize);
  pDb->dbName[dsize] = '\0';
  pPtr = pPtr + dsize + 1;
  // pDb->jfname
  pDb->jnName = pPtr;
  memcpy(pDb->jnName, dbname, dsize);
  pDb->jnName[dsize] = '/';
  memcpy(pDb->jnName + dsize + 1, TDB_JOURNAL_NAME, strlen(TDB_JOURNAL_NAME));
  pDb->jnName[dsize + 1 + strlen(TDB_JOURNAL_NAME)] = '\0';

  pDb->jfd = -1;

  ret = tdbPCacheOpen(szPage, pages, &(pDb->pCache));
  if (ret < 0) {
    return -1;
  }

  pDb->nPgrHash = 8;
  tsize = sizeof(SPager *) * pDb->nPgrHash;
  pDb->pgrHash = tdbOsMalloc(tsize);
  if (pDb->pgrHash == NULL) {
    return -1;
  }
  memset(pDb->pgrHash, 0, tsize);

  mkdir(dbname, 0755);

  *ppDb = pDb;
  return 0;
}

int tdbClose(TDB *pDb) {
  SPager *pPager;

  if (pDb) {
    for (pPager = pDb->pgrList; pPager; pPager = pDb->pgrList) {
      pDb->pgrList = pPager->pNext;
      tdbPagerClose(pPager);
    }

    tdbPCacheClose(pDb->pCache);
    tdbOsFree(pDb->pgrHash);
    tdbOsFree(pDb);
  }

  return 0;
}

int tdbBegin(TDB *pDb, TXN *pTxn) {
  SPager *pPager;
  int     ret;

  for (pPager = pDb->pgrList; pPager; pPager = pPager->pNext) {
    ret = tdbPagerBegin(pPager, pTxn);
    if (ret < 0) {
      ASSERT(0);
      return -1;
    }
  }

  return 0;
}

int tdbCommit(TDB *pDb, TXN *pTxn) {
  SPager *pPager;
  int     ret;

  for (pPager = pDb->pgrList; pPager; pPager = pPager->pNext) {
    ret = tdbPagerCommit(pPager, pTxn);
    if (ret < 0) {
      ASSERT(0);
      return -1;
    }
  }

  return 0;
}

SPager *tdbEnvGetPager(TDB *pDb, const char *fname) {
  u32      hash;
  SPager **ppPager;

  hash = tdbCstringHash(fname);
  ppPager = &pDb->pgrHash[hash % pDb->nPgrHash];
  for (; *ppPager && (strcmp(fname, (*ppPager)->dbFileName) != 0); ppPager = &((*ppPager)->pHashNext)) {
  }

  return *ppPager;
}

void tdbEnvAddPager(TDB *pDb, SPager *pPager) {
  u32      hash;
  SPager **ppPager;

  // rehash if neccessary
  if (pDb->nPager + 1 > pDb->nPgrHash) {
    // TODO
  }

  // add to list
  pPager->pNext = pDb->pgrList;
  pDb->pgrList = pPager;

  // add to hash
  hash = tdbCstringHash(pPager->dbFileName);
  ppPager = &pDb->pgrHash[hash % pDb->nPgrHash];
  pPager->pHashNext = *ppPager;
  *ppPager = pPager;

  // increase the counter
  pDb->nPager++;
}

void tdbEnvRemovePager(TDB *pDb, SPager *pPager) {
  u32      hash;
  SPager **ppPager;

  // remove from the list
  for (ppPager = &pDb->pgrList; *ppPager && (*ppPager != pPager); ppPager = &((*ppPager)->pNext)) {
  }
  ASSERT(*ppPager == pPager);
  *ppPager = pPager->pNext;

  // remove from hash
  hash = tdbCstringHash(pPager->dbFileName);
  ppPager = &pDb->pgrHash[hash % pDb->nPgrHash];
  for (; *ppPager && *ppPager != pPager; ppPager = &((*ppPager)->pHashNext)) {
  }
  ASSERT(*ppPager == pPager);
  *ppPager = pPager->pNext;

  // decrease the counter
  pDb->nPager--;

  // rehash if necessary
  if (pDb->nPgrHash > 8 && pDb->nPager < pDb->nPgrHash / 2) {
    // TODO
  }
}