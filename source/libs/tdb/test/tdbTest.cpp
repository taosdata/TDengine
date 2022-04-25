#include <gtest/gtest.h>

#include "tdbInt.h"

#include <string>

typedef struct SPoolMem {
  int64_t          size;
  struct SPoolMem *prev;
  struct SPoolMem *next;
} SPoolMem;

static SPoolMem *openPool() {
  SPoolMem *pPool = (SPoolMem *)tdbOsMalloc(sizeof(*pPool));

  pPool->prev = pPool->next = pPool;
  pPool->size = 0;

  return pPool;
}

static void clearPool(SPoolMem *pPool) {
  SPoolMem *pMem;

  do {
    pMem = pPool->next;

    if (pMem == pPool) break;

    pMem->next->prev = pMem->prev;
    pMem->prev->next = pMem->next;
    pPool->size -= pMem->size;

    tdbOsFree(pMem);
  } while (1);

  assert(pPool->size == 0);
}

static void closePool(SPoolMem *pPool) {
  clearPool(pPool);
  tdbOsFree(pPool);
}

static void *poolMalloc(void *arg, size_t size) {
  void     *ptr = NULL;
  SPoolMem *pPool = (SPoolMem *)arg;
  SPoolMem *pMem;

  pMem = (SPoolMem *)tdbOsMalloc(sizeof(*pMem) + size);
  if (pMem == NULL) {
    assert(0);
  }

  pMem->size = sizeof(*pMem) + size;
  pMem->next = pPool->next;
  pMem->prev = pPool;

  pPool->next->prev = pMem;
  pPool->next = pMem;
  pPool->size += pMem->size;

  ptr = (void *)(&pMem[1]);
  return ptr;
}

static void poolFree(void *arg, void *ptr) {
  SPoolMem *pPool = (SPoolMem *)arg;
  SPoolMem *pMem;

  pMem = &(((SPoolMem *)ptr)[-1]);

  pMem->next->prev = pMem->prev;
  pMem->prev->next = pMem->next;
  pPool->size -= pMem->size;

  tdbOsFree(pMem);
}

static int tKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  int k1, k2;

  std::string s1((char *)pKey1 + 3, kLen1 - 3);
  std::string s2((char *)pKey2 + 3, kLen2 - 3);
  k1 = stoi(s1);
  k2 = stoi(s2);

  if (k1 < k2) {
    return -1;
  } else if (k1 > k2) {
    return 1;
  } else {
    return 0;
  }
}

static int tDefaultKeyCmpr(const void *pKey1, int keyLen1, const void *pKey2, int keyLen2) {
  int mlen;
  int cret;

  ASSERT(keyLen1 > 0 && keyLen2 > 0 && pKey1 != NULL && pKey2 != NULL);

  mlen = keyLen1 < keyLen2 ? keyLen1 : keyLen2;
  cret = memcmp(pKey1, pKey2, mlen);
  if (cret == 0) {
    if (keyLen1 < keyLen2) {
      cret = -1;
    } else if (keyLen1 > keyLen2) {
      cret = 1;
    } else {
      cret = 0;
    }
  }
  return cret;
}

TEST(tdb_test, simple_test) {
  int            ret;
  TENV          *pEnv;
  TDB           *pDb;
  FKeyComparator compFunc;
  int            nData = 10000000;
  TXN            txn;

  // Open Env
  ret = tdbEnvOpen("tdb", 4096, 64, &pEnv);
  GTEST_ASSERT_EQ(ret, 0);

  // Create a database
  compFunc = tKeyCmpr;
  ret = tdbDbOpen("db.db", TDB_VARIANT_LEN, TDB_VARIANT_LEN, compFunc, pEnv, &pDb);
  GTEST_ASSERT_EQ(ret, 0);

  {
    char      key[64];
    char      val[64];
    int64_t   poolLimit = 4096;  // 1M pool limit
    int64_t   txnid = 0;
    SPoolMem *pPool;

    // open the pool
    pPool = openPool();

    // start a transaction
    txnid++;
    tdbTxnOpen(&txn, txnid, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
    tdbBegin(pEnv, &txn);

    for (int iData = 1; iData <= nData; iData++) {
      sprintf(key, "key%d", iData);
      sprintf(val, "value%d", iData);
      ret = tdbDbInsert(pDb, key, strlen(key), val, strlen(val), &txn);
      GTEST_ASSERT_EQ(ret, 0);

      // if pool is full, commit the transaction and start a new one
      if (pPool->size >= poolLimit) {
        // commit current transaction
        tdbCommit(pEnv, &txn);
        tdbTxnClose(&txn);

        // start a new transaction
        clearPool(pPool);
        txnid++;
        tdbTxnOpen(&txn, txnid, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
        tdbBegin(pEnv, &txn);
      }
    }

    // commit the transaction
    tdbCommit(pEnv, &txn);
    tdbTxnClose(&txn);

    {  // Query the data
      void *pVal = NULL;
      int   vLen;

      for (int i = 1; i <= nData; i++) {
        sprintf(key, "key%d", i);
        sprintf(val, "value%d", i);

        ret = tdbDbGet(pDb, key, strlen(key), &pVal, &vLen);
        ASSERT(ret == 0);
        GTEST_ASSERT_EQ(ret, 0);

        GTEST_ASSERT_EQ(vLen, strlen(val));
        GTEST_ASSERT_EQ(memcmp(val, pVal, vLen), 0);
      }

      TDB_FREE(pVal);
    }

    {  // Iterate to query the DB data
      TDBC *pDBC;
      void *pKey = NULL;
      void *pVal = NULL;
      int   vLen, kLen;
      int   count = 0;

      ret = tdbDbcOpen(pDb, &pDBC);
      GTEST_ASSERT_EQ(ret, 0);

      for (;;) {
        ret = tdbDbNext(pDBC, &pKey, &kLen, &pVal, &vLen);
        if (ret < 0) break;

        // std::cout.write((char *)pKey, kLen) /* << " " << kLen */ << " ";
        // std::cout.write((char *)pVal, vLen) /* << " " << vLen */;
        // std::cout << std::endl;

        count++;
      }

      GTEST_ASSERT_EQ(count, nData);

      tdbDbcClose(pDBC);

      TDB_FREE(pKey);
      TDB_FREE(pVal);
    }
  }

  ret = tdbDbDrop(pDb);
  GTEST_ASSERT_EQ(ret, 0);

  // Close a database
  tdbDbClose(pDb);

  // Close Env
  ret = tdbEnvClose(pEnv);
  GTEST_ASSERT_EQ(ret, 0);
}

TEST(tdb_test, simple_test2) {
  int            ret;
  TENV          *pEnv;
  TDB           *pDb;
  FKeyComparator compFunc;
  int            nData = 10000;
  TXN            txn;

  // Open Env
  ret = tdbEnvOpen("tdb", 1024, 0, &pEnv);
  GTEST_ASSERT_EQ(ret, 0);

  // Create a database
  compFunc = tKeyCmpr;
  ret = tdbDbOpen("db.db", TDB_VARIANT_LEN, TDB_VARIANT_LEN, compFunc, pEnv, &pDb);
  GTEST_ASSERT_EQ(ret, 0);

  {
    char      key[64];
    char      val[64];
    int64_t   txnid = 0;
    SPoolMem *pPool;

    // open the pool
    pPool = openPool();

    // start a transaction
    txnid++;
    tdbTxnOpen(&txn, txnid, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
    tdbBegin(pEnv, &txn);

    for (int iData = 1; iData <= nData; iData++) {
      sprintf(key, "key%d", iData);
      sprintf(val, "value%d", iData);
      ret = tdbDbInsert(pDb, key, strlen(key), val, strlen(val), &txn);
      GTEST_ASSERT_EQ(ret, 0);
    }

    {  // Iterate to query the DB data
      TDBC *pDBC;
      void *pKey = NULL;
      void *pVal = NULL;
      int   vLen, kLen;
      int   count = 0;

      ret = tdbDbcOpen(pDb, &pDBC);
      GTEST_ASSERT_EQ(ret, 0);

      for (;;) {
        ret = tdbDbNext(pDBC, &pKey, &kLen, &pVal, &vLen);
        if (ret < 0) break;

        std::cout.write((char *)pKey, kLen) /* << " " << kLen */ << " ";
        std::cout.write((char *)pVal, vLen) /* << " " << vLen */;
        std::cout << std::endl;

        count++;
      }

      GTEST_ASSERT_EQ(count, nData);

      tdbDbcClose(pDBC);

      TDB_FREE(pKey);
      TDB_FREE(pVal);
    }
  }

  // commit the transaction
  tdbCommit(pEnv, &txn);
  tdbTxnClose(&txn);

  ret = tdbDbDrop(pDb);
  GTEST_ASSERT_EQ(ret, 0);

  // Close a database
  tdbDbClose(pDb);

  // Close Env
  ret = tdbEnvClose(pEnv);
  GTEST_ASSERT_EQ(ret, 0);
}