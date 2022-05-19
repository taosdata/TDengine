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

struct STTB {
  TDB    *pEnv;
  SBTree *pBt;
};

struct STBC {
  SBTC btc;
};

int tdbTbOpen(const char *tbname, int keyLen, int valLen, tdb_cmpr_fn_t keyCmprFn, TDB *pEnv, TTB **ppTb) {
  TTB    *pTb;
  SPager *pPager;
  int     ret;
  char    fFullName[TDB_FILENAME_LEN];
  SPage  *pPage;
  SPgno   pgno;

  *ppTb = NULL;

  pTb = (TTB *)tdbOsCalloc(1, sizeof(*pTb));
  if (pTb == NULL) {
    return -1;
  }

  // pTb->pEnv
  pTb->pEnv = pEnv;

  pPager = tdbEnvGetPager(pEnv, tbname);
  if (pPager == NULL) {
    snprintf(fFullName, TDB_FILENAME_LEN, "%s/%s", pEnv->dbName, tbname);
    ret = tdbPagerOpen(pEnv->pCache, fFullName, &pPager);
    if (ret < 0) {
      return -1;
    }

    tdbEnvAddPager(pEnv, pPager);
  }

  ASSERT(pPager != NULL);

  // pTb->pBt
  ret = tdbBtreeOpen(keyLen, valLen, pPager, keyCmprFn, &(pTb->pBt));
  if (ret < 0) {
    return -1;
  }

  *ppTb = pTb;
  return 0;
}

int tdbTbClose(TTB *pTb) {
  if (pTb) {
    tdbBtreeClose(pTb->pBt);
    tdbOsFree(pTb);
  }
  return 0;
}

int tdbTbDrop(TTB *pTb) {
  // TODO
  return 0;
}

int tdbTbInsert(TTB *pTb, const void *pKey, int keyLen, const void *pVal, int valLen, TXN *pTxn) {
  return tdbBtreeInsert(pTb->pBt, pKey, keyLen, pVal, valLen, pTxn);
}

int tdbTbDelete(TTB *pTb, const void *pKey, int kLen, TXN *pTxn) { return tdbBtreeDelete(pTb->pBt, pKey, kLen, pTxn); }

int tdbTbUpsert(TTB *pTb, const void *pKey, int kLen, const void *pVal, int vLen, TXN *pTxn) {
  return tdbBtreeUpsert(pTb->pBt, pKey, kLen, pVal, vLen, pTxn);
}

int tdbTbGet(TTB *pTb, const void *pKey, int kLen, void **ppVal, int *vLen) {
  return tdbBtreeGet(pTb->pBt, pKey, kLen, ppVal, vLen);
}

int tdbTbPGet(TTB *pTb, const void *pKey, int kLen, void **ppKey, int *pkLen, void **ppVal, int *vLen) {
  return tdbBtreePGet(pTb->pBt, pKey, kLen, ppKey, pkLen, ppVal, vLen);
}

int tdbTbcOpen(TTB *pTb, TBC **ppTbc, TXN *pTxn) {
  int  ret;
  TBC *pTbc = NULL;

  *ppTbc = NULL;
  pTbc = (TBC *)tdbOsMalloc(sizeof(*pTbc));
  if (pTbc == NULL) {
    return -1;
  }

  tdbBtcOpen(&pTbc->btc, pTb->pBt, pTxn);

  *ppTbc = pTbc;
  return 0;
}

int tdbTbcMoveTo(TBC *pTbc, const void *pKey, int kLen, int *c) { return tdbBtcMoveTo(&pTbc->btc, pKey, kLen, c); }

int tdbTbcMoveToFirst(TBC *pTbc) { return tdbBtcMoveToFirst(&pTbc->btc); }

int tdbTbcMoveToLast(TBC *pTbc) { return tdbBtcMoveToLast(&pTbc->btc); }

int tdbTbcMoveToNext(TBC *pTbc) { return tdbBtcMoveToNext(&pTbc->btc); }

int tdbTbcMoveToPrev(TBC *pTbc) { return tdbBtcMoveToPrev(&pTbc->btc); }

int tdbTbcGet(TBC *pTbc, const void **ppKey, int *pkLen, const void **ppVal, int *pvLen) {
  return tdbBtcGet(&pTbc->btc, ppKey, pkLen, ppVal, pvLen);
}

int tdbTbcDelete(TBC *pTbc) { return tdbBtcDelete(&pTbc->btc); }

int tdbTbcNext(TBC *pTbc, void **ppKey, int *kLen, void **ppVal, int *vLen) {
  return tdbBtreeNext(&pTbc->btc, ppKey, kLen, ppVal, vLen);
}

int tdbTbcUpsert(TBC *pTbc, const void *pKey, int nKey, const void *pData, int nData, int insert) {
  return tdbBtcUpsert(&pTbc->btc, pKey, nKey, pData, nData, insert);
}

int tdbTbcClose(TBC *pTbc) {
  if (pTbc) {
    tdbBtcClose(&pTbc->btc);
    tdbOsFree(pTbc);
  }

  return 0;
}

int tdbTbcIsValid(TBC *pTbc) { return tdbBtcIsValid(&pTbc->btc); }