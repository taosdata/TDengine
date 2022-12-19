#include <gtest/gtest.h>

#define ALLOW_FORBID_FUNC
#include "os.h"
#include "tdb.h"

#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>
#include "tlog.h"

typedef struct SPoolMem {
  int64_t          size;
  struct SPoolMem *prev;
  struct SPoolMem *next;
} SPoolMem;

static SPoolMem *openPool() {
  SPoolMem *pPool = (SPoolMem *)taosMemoryMalloc(sizeof(*pPool));

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

    taosMemoryFree(pMem);
  } while (1);

  assert(pPool->size == 0);
}

static void closePool(SPoolMem *pPool) {
  clearPool(pPool);
  taosMemoryFree(pPool);
}

static void *poolMalloc(void *arg, size_t size) {
  void     *ptr = NULL;
  SPoolMem *pPool = (SPoolMem *)arg;
  SPoolMem *pMem;

  pMem = (SPoolMem *)taosMemoryMalloc(sizeof(*pMem) + size);
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

  taosMemoryFree(pMem);
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

TEST(tdb_test, DISABLED_simple_insert1) {
  int           ret;
  TDB          *pEnv;
  TTB          *pDb;
  tdb_cmpr_fn_t compFunc;
  int           nData = 1000000;
  TXN          *txn;

  taosRemoveDir("tdb");

  // Open Env
  ret = tdbOpen("tdb", 4096, 64, &pEnv, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // Create a database
  compFunc = tKeyCmpr;
  ret = tdbTbOpen("db.db", -1, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  {
    char      key[64];
    char      val[64];
    int64_t   poolLimit = 4096;  // 1M pool limit
    SPoolMem *pPool;

    // open the pool
    pPool = openPool();

    // start a transaction
    tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

    for (int iData = 1; iData <= nData; iData++) {
      sprintf(key, "key%d", iData);
      sprintf(val, "value%d", iData);
      ret = tdbTbInsert(pDb, key, strlen(key), val, strlen(val), txn);
      GTEST_ASSERT_EQ(ret, 0);

      // if pool is full, commit the transaction and start a new one
      if (pPool->size >= poolLimit) {
        // commit current transaction
        tdbCommit(pEnv, txn);
        tdbPostCommit(pEnv, txn);

        // start a new transaction
        clearPool(pPool);
        tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
      }
    }

    // commit the transaction
    tdbCommit(pEnv, txn);
    tdbPostCommit(pEnv, txn);

    {  // Query the data
      void *pVal = NULL;
      int   vLen;

      for (int i = 1; i <= nData; i++) {
        sprintf(key, "key%d", i);
        sprintf(val, "value%d", i);

        ret = tdbTbGet(pDb, key, strlen(key), &pVal, &vLen);
        ASSERT(ret == 0);
        GTEST_ASSERT_EQ(ret, 0);

        GTEST_ASSERT_EQ(vLen, strlen(val));
        GTEST_ASSERT_EQ(memcmp(val, pVal, vLen), 0);
      }

      tdbFree(pVal);
    }

    {  // Iterate to query the DB data
      TBC  *pDBC;
      void *pKey = NULL;
      void *pVal = NULL;
      int   vLen, kLen;
      int   count = 0;

      ret = tdbTbcOpen(pDb, &pDBC, NULL);
      GTEST_ASSERT_EQ(ret, 0);

      tdbTbcMoveToFirst(pDBC);

      for (;;) {
        ret = tdbTbcNext(pDBC, &pKey, &kLen, &pVal, &vLen);
        if (ret < 0) break;

        // std::cout.write((char *)pKey, kLen) /* << " " << kLen */ << " ";
        // std::cout.write((char *)pVal, vLen) /* << " " << vLen */;
        // std::cout << std::endl;

        count++;
      }

      GTEST_ASSERT_EQ(count, nData);

      tdbTbcClose(pDBC);

      tdbFree(pKey);
      tdbFree(pVal);
    }
  }

  ret = tdbTbDrop(pDb);
  GTEST_ASSERT_EQ(ret, 0);

  // Close a database
  tdbTbClose(pDb);

  // Close Env
  ret = tdbClose(pEnv);
  GTEST_ASSERT_EQ(ret, 0);
}

TEST(tdb_test, DISABLED_simple_insert2) {
  int           ret;
  TDB          *pEnv;
  TTB          *pDb;
  tdb_cmpr_fn_t compFunc;
  int           nData = 1000000;
  TXN          *txn;

  taosRemoveDir("tdb");

  // Open Env
  ret = tdbOpen("tdb", 1024, 10, &pEnv, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // Create a database
  compFunc = tDefaultKeyCmpr;
  ret = tdbTbOpen("db.db", -1, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  {
    char      key[64];
    char      val[64];
    SPoolMem *pPool;

    // open the pool
    pPool = openPool();

    // start a transaction
    tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

    for (int iData = 1; iData <= nData; iData++) {
      sprintf(key, "key%d", iData);
      sprintf(val, "value%d", iData);
      ret = tdbTbInsert(pDb, key, strlen(key), val, strlen(val), txn);
      GTEST_ASSERT_EQ(ret, 0);
    }

    {  // Iterate to query the DB data
      TBC  *pDBC;
      void *pKey = NULL;
      void *pVal = NULL;
      int   vLen, kLen;
      int   count = 0;

      ret = tdbTbcOpen(pDb, &pDBC, NULL);
      GTEST_ASSERT_EQ(ret, 0);

      tdbTbcMoveToFirst(pDBC);

      for (;;) {
        ret = tdbTbcNext(pDBC, &pKey, &kLen, &pVal, &vLen);
        if (ret < 0) break;

        // std::cout.write((char *)pKey, kLen) /* << " " << kLen */ << " ";
        // std::cout.write((char *)pVal, vLen) /* << " " << vLen */;
        // std::cout << std::endl;

        count++;
      }

      GTEST_ASSERT_EQ(count, nData);

      tdbTbcClose(pDBC);

      tdbFree(pKey);
      tdbFree(pVal);
    }
  }

  // commit the transaction
  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  ret = tdbTbDrop(pDb);
  GTEST_ASSERT_EQ(ret, 0);

  // Close a database
  tdbTbClose(pDb);

  // Close Env
  ret = tdbClose(pEnv);
  GTEST_ASSERT_EQ(ret, 0);
}

TEST(tdb_test, DISABLED_simple_delete1) {
  int       ret;
  TTB      *pDb;
  char      key[128];
  char      data[128];
  TXN      *txn;
  TDB      *pEnv;
  SPoolMem *pPool;
  void     *pKey = NULL;
  void     *pData = NULL;
  int       nKey;
  TBC      *pDbc;
  int       nData;
  int       nKV = 69;

  taosRemoveDir("tdb");

  pPool = openPool();

  // open env
  ret = tdbOpen("tdb", 1024, 256, &pEnv, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // open database
  ret = tdbTbOpen("db.db", -1, -1, tKeyCmpr, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  // loop to insert batch data
  for (int iData = 0; iData < nKV; iData++) {
    sprintf(key, "key%d", iData);
    sprintf(data, "data%d", iData);
    ret = tdbTbInsert(pDb, key, strlen(key), data, strlen(data), txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  // query the data
  for (int iData = 0; iData < nKV; iData++) {
    sprintf(key, "key%d", iData);
    sprintf(data, "data%d", iData);

    ret = tdbTbGet(pDb, key, strlen(key), &pData, &nData);
    GTEST_ASSERT_EQ(ret, 0);
    GTEST_ASSERT_EQ(memcmp(data, pData, nData), 0);
  }

  // loop to delete some data
  for (int iData = nKV - 1; iData > 30; iData--) {
    sprintf(key, "key%d", iData);

    ret = tdbTbDelete(pDb, key, strlen(key), txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  // query the data
  for (int iData = 0; iData < nKV; iData++) {
    sprintf(key, "key%d", iData);

    ret = tdbTbGet(pDb, key, strlen(key), &pData, &nData);
    if (iData <= 30) {
      GTEST_ASSERT_EQ(ret, 0);
    } else {
      GTEST_ASSERT_EQ(ret, -1);
    }
  }

  // loop to iterate the data
  tdbTbcOpen(pDb, &pDbc, NULL);

  ret = tdbTbcMoveToFirst(pDbc);
  GTEST_ASSERT_EQ(ret, 0);

  pKey = NULL;
  pData = NULL;
  for (;;) {
    ret = tdbTbcNext(pDbc, &pKey, &nKey, &pData, &nData);
    if (ret < 0) break;

    std::cout.write((char *)pKey, nKey) /* << " " << kLen */ << " ";
    std::cout.write((char *)pData, nData) /* << " " << vLen */;
    std::cout << std::endl;
  }

  tdbTbcClose(pDbc);

  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  closePool(pPool);

  tdbTbClose(pDb);
  tdbClose(pEnv);
}

TEST(tdb_test, DISABLED_simple_upsert1) {
  int       ret;
  TDB      *pEnv;
  TTB      *pDb;
  int       nData = 100000;
  char      key[64];
  char      data[64];
  void     *pData = NULL;
  SPoolMem *pPool;
  TXN      *txn;

  taosRemoveDir("tdb");

  // open env
  ret = tdbOpen("tdb", 4096, 64, &pEnv, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // open database
  ret = tdbTbOpen("db.db", -1, -1, NULL, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  pPool = openPool();
  // insert some data
  tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  for (int iData = 0; iData < nData; iData++) {
    sprintf(key, "key%d", iData);
    sprintf(data, "data%d", iData);
    ret = tdbTbInsert(pDb, key, strlen(key), data, strlen(data), txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  // query the data
  for (int iData = 0; iData < nData; iData++) {
    sprintf(key, "key%d", iData);
    sprintf(data, "data%d", iData);
    ret = tdbTbGet(pDb, key, strlen(key), &pData, &nData);
    GTEST_ASSERT_EQ(ret, 0);
    GTEST_ASSERT_EQ(memcmp(pData, data, nData), 0);
  }

  // upsert some data
  for (int iData = 0; iData < nData; iData++) {
    sprintf(key, "key%d", iData);
    sprintf(data, "data%d-u", iData);
    ret = tdbTbUpsert(pDb, key, strlen(key), data, strlen(data), txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  // query the data
  for (int iData = 0; iData < nData; iData++) {
    sprintf(key, "key%d", iData);
    sprintf(data, "data%d-u", iData);
    ret = tdbTbGet(pDb, key, strlen(key), &pData, &nData);
    GTEST_ASSERT_EQ(ret, 0);
    GTEST_ASSERT_EQ(memcmp(pData, data, nData), 0);
  }

  tdbTbClose(pDb);
  tdbClose(pEnv);
}

TEST(tdb_test, multi_thread_query) {
  int           ret;
  TDB          *pEnv;
  TTB          *pDb;
  tdb_cmpr_fn_t compFunc;
  int           nData = 1000000;
  TXN          *txn;

  taosRemoveDir("tdb");

  // Open Env
  ret = tdbOpen("tdb", 4096, 10, &pEnv, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // Create a database
  compFunc = tKeyCmpr;
  ret = tdbTbOpen("db.db", -1, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  char      key[64];
  char      val[64];
  int64_t   poolLimit = 4096 * 20;  // 1M pool limit
  SPoolMem *pPool;

  // open the pool
  pPool = openPool();

  // start a transaction
  tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  for (int iData = 1; iData <= nData; iData++) {
    sprintf(key, "key%d", iData);
    sprintf(val, "value%d", iData);
    ret = tdbTbInsert(pDb, key, strlen(key), val, strlen(val), txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  auto f = [](TTB *pDb, int nData) {
    TBC  *pDBC;
    void *pKey = NULL;
    void *pVal = NULL;
    int   vLen, kLen;
    int   count = 0;
    int   ret;
    TXN   txn;

    SPoolMem *pPool = openPool();
    txn.flags = 0;
    txn.txnId = 0;
    txn.xMalloc = poolMalloc;
    txn.xFree = poolFree;
    txn.xArg = pPool;

    ret = tdbTbcOpen(pDb, &pDBC, &txn);
    GTEST_ASSERT_EQ(ret, 0);

    tdbTbcMoveToFirst(pDBC);

    for (;;) {
      ret = tdbTbcNext(pDBC, &pKey, &kLen, &pVal, &vLen);
      if (ret < 0) break;

      // std::cout.write((char *)pKey, kLen) /* << " " << kLen */ << " ";
      // std::cout.write((char *)pVal, vLen) /* << " " << vLen */;
      // std::cout << std::endl;

      count++;
    }

    GTEST_ASSERT_EQ(count, nData);

    tdbTbcClose(pDBC);

    tdbFree(pKey);
    tdbFree(pVal);
  };

  // tdbCommit(pEnv, &txn);

  // multi-thread query
  int                      nThreads = 20;
  std::vector<std::thread> threads;
  for (int i = 0; i < nThreads; i++) {
    if (i == 0) {
      threads.push_back(std::thread(tdbCommit, pEnv, txn));
    } else {
      threads.push_back(std::thread(f, pDb, nData));
    }
  }

  for (auto &th : threads) {
    th.join();
  }

  // commit the transaction
  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  // Close a database
  tdbTbClose(pDb);

  // Close Env
  ret = tdbClose(pEnv);
  GTEST_ASSERT_EQ(ret, 0);
}

TEST(tdb_test, DISABLED_multi_thread1) {
#if 0
  int           ret;
  TDB          *pDb;
  TTB          *pTb;
  tdb_cmpr_fn_t compFunc;
  int           nData = 10000000;
  TXN           txn;

  std::shared_timed_mutex mutex;

  taosRemoveDir("tdb");

  // Open Env
  ret = tdbOpen("tdb", 512, 1, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  ret = tdbTbOpen("db.db", -1, -1, NULL, pDb, &pTb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  auto insert = [](TDB *pDb, TTB *pTb, int nData, int *stop, std::shared_timed_mutex *mu) {
    TXN       *txn = NULL;
    char      key[128];
    char      val[128];
    SPoolMem *pPool = openPool();

    tdbBegin(pDb, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
    for (int iData = 1; iData <= nData; iData++) {
      sprintf(key, "key%d", iData);
      sprintf(val, "value%d", iData);
      {
        std::lock_guard<std::shared_timed_mutex> wmutex(*mu);

        int ret = tdbTbInsert(pTb, key, strlen(key), val, strlen(val), &txn);

        GTEST_ASSERT_EQ(ret, 0);
      }

      if (pPool->size > 1024 * 1024) {
        tdbCommit(pDb, txn);
        tdbPostCommit(pDb, txn);

        clearPool(pPool);
        tdbBegin(pDb, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
      }
    }

    tdbCommit(pDb, txn);
    tdbPostCommit(pDb, txn);

    closePool(pPool);

    *stop = 1;
  };

  auto query = [](TTB *pTb, int *stop, std::shared_timed_mutex *mu) {
    TBC  *pDBC;
    void *pKey = NULL;
    void *pVal = NULL;
    int   vLen, kLen;
    int   ret;
    TXN   txn;

    SPoolMem *pPool = openPool();
    txn.flags = 0;
    txn.txnId = 0;
    txn.xMalloc = poolMalloc;
    txn.xFree = poolFree;
    txn.xArg = pPool;

    for (;;) {
      if (*stop) break;

      clearPool(pPool);
      int count = 0;
      {
        std::shared_lock<std::shared_timed_mutex> rMutex(*mu);

        ret = tdbTbcOpen(pTb, &pDBC, &txn);
        GTEST_ASSERT_EQ(ret, 0);

        tdbTbcMoveToFirst(pDBC);

        for (;;) {
          ret = tdbTbcNext(pDBC, &pKey, &kLen, &pVal, &vLen);
          if (ret < 0) break;
          count++;
        }

        std::cout << count << std::endl;

        tdbTbcClose(pDBC);
      }

      usleep(500000);
    }

    closePool(pPool);
    tdbFree(pKey);
    tdbFree(pVal);
  };

  std::vector<std::thread> threads;
  int                      nThreads = 10;
  int                      stop = 0;
  for (int i = 0; i < nThreads; i++) {
    if (i == 0) {
      threads.push_back(std::thread(insert, pDb, pTb, nData, &stop, &mutex));
    } else {
      threads.push_back(std::thread(query, pTb, &stop, &mutex));
    }
  }

  for (auto &th : threads) {
    th.join();
  }

  // Close a database
  tdbTbClose(pTb);

  // Close Env
  ret = tdbClose(pDb);
  GTEST_ASSERT_EQ(ret, 0);
#endif
}
