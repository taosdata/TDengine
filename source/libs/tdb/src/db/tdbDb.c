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

struct STDB {
  TENV   *pEnv;
  SBTree *pBt;
};

struct STDBC {
  SBTC btc;
};

int tdbOpen(const char *fname, int keyLen, int valLen, tdb_cmpr_fn_t keyCmprFn, TENV *pEnv, TDB **ppDb) {
  TDB    *pDb;
  SPager *pPager;
  int     ret;
  char    fFullName[TDB_FILENAME_LEN];
  SPage  *pPage;
  SPgno   pgno;

  *ppDb = NULL;

  pDb = (TDB *)tdbOsCalloc(1, sizeof(*pDb));
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

    tdbEnvAddPager(pEnv, pPager);
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

int tdbClose(TDB *pDb) {
  if (pDb) {
    tdbBtreeClose(pDb->pBt);
    tdbOsFree(pDb);
  }
  return 0;
}

int tdbDrop(TDB *pDb) {
  // TODO
  return 0;
}

int tdbInsert(TDB *pDb, const void *pKey, int keyLen, const void *pVal, int valLen, TXN *pTxn) {
  return tdbBtreeInsert(pDb->pBt, pKey, keyLen, pVal, valLen, pTxn);
}

int tdbDelete(TDB *pDb, const void *pKey, int kLen, TXN *pTxn) { return tdbBtreeDelete(pDb->pBt, pKey, kLen, pTxn); }

int tdbUpsert(TDB *pDb, const void *pKey, int kLen, const void *pVal, int vLen, TXN *pTxn) {
  return tdbBtreeUpsert(pDb->pBt, pKey, kLen, pVal, vLen, pTxn);
}

int tdbGet(TDB *pDb, const void *pKey, int kLen, void **ppVal, int *vLen) {
  return tdbBtreeGet(pDb->pBt, pKey, kLen, ppVal, vLen);
}

int tdbPGet(TDB *pDb, const void *pKey, int kLen, void **ppKey, int *pkLen, void **ppVal, int *vLen) {
  return tdbBtreePGet(pDb->pBt, pKey, kLen, ppKey, pkLen, ppVal, vLen);
}

int tdbDbcOpen(TDB *pDb, TDBC **ppDbc, TXN *pTxn) {
  int   ret;
  TDBC *pDbc = NULL;

  *ppDbc = NULL;
  pDbc = (TDBC *)tdbOsMalloc(sizeof(*pDbc));
  if (pDbc == NULL) {
    return -1;
  }

  tdbBtcOpen(&pDbc->btc, pDb->pBt, pTxn);

  *ppDbc = pDbc;
  return 0;
}

int tdbDbcMoveTo(TDBC *pDbc, const void *pKey, int kLen, int *c) { return tdbBtcMoveTo(&pDbc->btc, pKey, kLen, c); }

int tdbDbcMoveToFirst(TDBC *pDbc) { return tdbBtcMoveToFirst(&pDbc->btc); }

int tdbDbcMoveToLast(TDBC *pDbc) { return tdbBtcMoveToLast(&pDbc->btc); }

int tdbDbcMoveToNext(TDBC *pDbc) { return tdbBtcMoveToNext(&pDbc->btc); }

int tdbDbcMoveToPrev(TDBC *pDbc) { return tdbBtcMoveToPrev(&pDbc->btc); }

int tdbDbcGet(TDBC *pDbc, const void **ppKey, int *pkLen, const void **ppVal, int *pvLen) {
  return tdbBtcGet(&pDbc->btc, ppKey, pkLen, ppVal, pvLen);
}

int tdbDbcDelete(TDBC *pDbc) { return tdbBtcDelete(&pDbc->btc); }

int tdbDbcNext(TDBC *pDbc, void **ppKey, int *kLen, void **ppVal, int *vLen) {
  return tdbBtreeNext(&pDbc->btc, ppKey, kLen, ppVal, vLen);
}

int tdbDbcUpsert(TDBC *pDbc, const void *pKey, int nKey, const void *pData, int nData, int insert) {
  return tdbBtcUpsert(&pDbc->btc, pKey, nKey, pData, nData, insert);
}

int tdbDbcClose(TDBC *pDbc) {
  if (pDbc) {
    tdbBtcClose(&pDbc->btc);
    tdbOsFree(pDbc);
  }

  return 0;
}

int tdbDbcIsValid(TDBC *pDbc) { return tdbBtcIsValid(&pDbc->btc); }