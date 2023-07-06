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

int tdbTbOpen(const char *tbname, int keyLen, int valLen, tdb_cmpr_fn_t keyCmprFn, TDB *pEnv, TTB **ppTb,
              int8_t rollback) {
  TTB    *pTb;
  SPager *pPager;
  int     ret;
  char    fFullName[TDB_FILENAME_LEN];
  SPage  *pPage;
  SPgno   pgno;
  void   *pKey = NULL;
  int     nKey = 0;
  void   *pData = NULL;
  int     nData = 0;

  *ppTb = NULL;

  pTb = (TTB *)tdbOsCalloc(1, sizeof(*pTb));
  if (pTb == NULL) {
    return -1;
  }

  // pTb->pEnv
  pTb->pEnv = pEnv;

#ifdef USE_MAINDB
  snprintf(fFullName, TDB_FILENAME_LEN, "%s/%s", pEnv->dbName, TDB_MAINDB_NAME);

  if (strcmp(TDB_MAINDB_NAME, tbname)) {
    pPager = tdbEnvGetPager(pEnv, fFullName);
    if (!pPager) {
      tdbOsFree(pTb);
      return -1;
    }

    ret = tdbTbGet(pPager->pEnv->pMainDb, tbname, strlen(tbname) + 1, &pData, &nData);
    if (ret < 0) {
      // new pgno & insert into main db
      pgno = 0;
    } else {
      pgno = *(SPgno *)pData;

      tdbFree(pKey);
      tdbFree(pData);
    }

  } else {
    pPager = tdbEnvGetPager(pEnv, fFullName);
    if (pPager == NULL) {
      ret = tdbPagerOpen(pEnv->pCache, fFullName, &pPager);
      if (ret < 0) {
        tdbOsFree(pTb);
        return -1;
      }

      tdbEnvAddPager(pEnv, pPager);

      pPager->pEnv = pEnv;
    }

    if (pPager->dbOrigSize > 0) {
      pgno = 1;
    } else {
      pgno = 0;
    }
  }

#else

  pPager = tdbEnvGetPager(pEnv, tbname);
  if (pPager == NULL) {
    snprintf(fFullName, TDB_FILENAME_LEN, "%s/%s", pEnv->dbName, tbname);
    ret = tdbPagerOpen(pEnv->pCache, fFullName, &pPager);
    if (ret < 0) {
      tdbOsFree(pTb);
      return -1;
    }

    tdbEnvAddPager(pEnv, pPager);
  }

#endif

  if (rollback) {
    ret = tdbPagerRestoreJournals(pPager);
    if (ret < 0) {
      tdbOsFree(pTb);
      return -1;
    }
  } else {
    tdbPagerRollback(pPager);
  }

  // pTb->pBt
  ret = tdbBtreeOpen(keyLen, valLen, pPager, tbname, pgno, keyCmprFn, pEnv, &(pTb->pBt));
  if (ret < 0) {
    tdbOsFree(pTb);
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

bool tdbTbExist(const char *tbname, TDB *pEnv) {
  bool    exist = false;
  SPager *pPager;
  char    fFullName[TDB_FILENAME_LEN];

#ifdef USE_MAINDB

  snprintf(fFullName, TDB_FILENAME_LEN, "%s/%s", pEnv->dbName, TDB_MAINDB_NAME);

  if (strcmp(TDB_MAINDB_NAME, tbname)) {
    pPager = tdbEnvGetPager(pEnv, fFullName);

    exist = tdbTbGet(pPager->pEnv->pMainDb, tbname, strlen(tbname) + 1, NULL, NULL) == 0;
  } else {
    exist = taosCheckExistFile(fFullName);
  }

#else

  snprintf(fFullName, TDB_FILENAME_LEN, "%s/%s", pEnv->dbName, tbname);

  exist = taosCheckExistFile(fFullName);

#endif

  return exist;
}

int tdbTbDrop(TTB *pTb) {
  // TODO
  return 0;
}

int tdbTbDropByName(const char *tbname, TDB *pEnv, TXN *pTxn) {
  int     ret;
  SPager *pPager;
  char    fFullName[TDB_FILENAME_LEN];

#ifdef USE_MAINDB

  snprintf(fFullName, TDB_FILENAME_LEN, "%s/%s", pEnv->dbName, TDB_MAINDB_NAME);

  if (strcmp(TDB_MAINDB_NAME, tbname)) {
    pPager = tdbEnvGetPager(pEnv, fFullName);

    ret = tdbTbDelete(pPager->pEnv->pMainDb, tbname, strlen(tbname) + 1, pTxn);
  } else {
    ret = taosRemoveFile(fFullName);
  }

#else

  snprintf(fFullName, TDB_FILENAME_LEN, "%s/%s", pEnv->dbName, tbname);

  ret = taosRemoveFile(fFullName);

#endif

  return ret;
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

int32_t tdbTbTraversal(TTB *pTb, void *data,
                       int32_t (*func)(const void *pKey, int keyLen, const void *pVal, int valLen, void *data)) {
  TBC *pCur;
  int  ret = tdbTbcOpen(pTb, &pCur, NULL);
  if (ret < 0) {
    return ret;
  }

  tdbTbcMoveToFirst(pCur);

  void *pKey = NULL;
  int   kLen = 0;
  void *pValue = NULL;
  int   vLen = 0;

  while (1) {
    ret = tdbTbcNext(pCur, &pKey, &kLen, &pValue, &vLen);
    if (ret < 0) {
      ret = 0;
      break;
    }

    ret = func(pKey, kLen, pValue, vLen, data);
    if (ret < 0) break;
  }
  tdbFree(pKey);
  tdbFree(pValue);
  tdbTbcClose(pCur);

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

int tdbTbcPrev(TBC *pTbc, void **ppKey, int *kLen, void **ppVal, int *vLen) {
  return tdbBtreePrev(&pTbc->btc, ppKey, kLen, ppVal, vLen);
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
