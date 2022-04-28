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

int tdbDbOpen(const char *fname, int keyLen, int valLen, tdb_cmpr_fn_t keyCmprFn, TENV *pEnv, TDB **ppDb) {
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

int tdbDbClose(TDB *pDb) {
  // TODO
  return 0;
}

int tdbDbDrop(TDB *pDb) {
  // TODO
  return 0;
}

int tdbDbInsert(TDB *pDb, const void *pKey, int keyLen, const void *pVal, int valLen, TXN *pTxn) {
  return tdbBtreeInsert(pDb->pBt, pKey, keyLen, pVal, valLen, pTxn);
}

int tdbDbGet(TDB *pDb, const void *pKey, int kLen, void **ppVal, int *vLen) {
  return tdbBtreeGet(pDb->pBt, pKey, kLen, ppVal, vLen);
}

int tdbDbPGet(TDB *pDb, const void *pKey, int kLen, void **ppKey, int *pkLen, void **ppVal, int *vLen) {
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

  // TODO: move to first now, we can move to any key-value
  // and in any direction, design new APIs.
  ret = tdbBtcMoveToFirst(&pDbc->btc);
  if (ret < 0) {
    ASSERT(0);
    return -1;
  }

  *ppDbc = pDbc;
  return 0;
}

int tdbDbcMoveTo(TDBC *pDbc, const void *pKey, int kLen, tdb_cmpr_fn_t cmprFn, int flags) {
  int tflags;

  // set/check flags
  if (flags == 0) {
    flags |= TDB_FLG_CMP_EQ;
  } else {
    tflags = flags & (TDB_FLG_CMP_LT | TDB_FLG_CMP_EQ | TDB_FLG_CMP_GT);
    if (tflags != TDB_FLG_CMP_LT && tflags != TDB_FLG_CMP_EQ && tflags != TDB_FLG_CMP_GT) return -1;
  }

  return tdbBtcMoveTo2(&pDbc->btc, pKey, kLen, cmprFn, flags);
}

int tdbDbcPut(TDBC *pDbc, const void *pKey, int keyLen, const void *pVal, int valLen) {
  // TODO
  ASSERT(0);
  return 0;
}

int tdbDbcUpdate(TDBC *pDbc, const void *pKey, int kLen, const void *pVal, int vLen) {
  // TODO
  ASSERT(0);
  return 0;
}

int tdbDbcDrop(TDBC *pDbc) {
  // TODO
  ASSERT(0);
  return 0;
}

int tdbDbcNext(TDBC *pDbc, void **ppKey, int *kLen, void **ppVal, int *vLen) {
  return tdbBtreeNext(&pDbc->btc, ppKey, kLen, ppVal, vLen);
}

int tdbDbcClose(TDBC *pDbc) {
  if (pDbc) {
    tdbOsFree(pDbc);
  }

  return 0;
}
