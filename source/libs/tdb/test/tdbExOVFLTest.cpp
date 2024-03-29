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

// TEST(TdbOVFLPagesTest, DISABLED_TbUpsertTest) {
//  TEST(TdbOVFLPagesTest, TbUpsertTest) {
//}

// TEST(TdbOVFLPagesTest, DISABLED_TbPGetTest) {
//  TEST(TdbOVFLPagesTest, TbPGetTest) {
//}

static void generateBigVal(char *val, int valLen) {
  for (int i = 0; i < valLen; ++i) {
    char c = char(i & 0xff);
    if (c == 0) {
      c = 1;
    }
    val[i] = c;
  }
}

static TDB *openEnv(char const *envName, int const pageSize, int const pageNum) {
  TDB *pEnv = NULL;

  int ret = tdbOpen(envName, pageSize, pageNum, &pEnv, 0);
  if (ret) {
    pEnv = NULL;
  }

  return pEnv;
}

static void insertOfp(void) {
  int ret = 0;

  taosRemoveDir("tdb");

  // open Env
  int const pageSize = 4096;
  int const pageNum = 64;
  TDB      *pEnv = openEnv("tdb", pageSize, pageNum);
  GTEST_ASSERT_NE(pEnv, nullptr);

  // open db
  TTB          *pDb = NULL;
  tdb_cmpr_fn_t compFunc = tKeyCmpr;
  // ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  ret = tdbTbOpen("ofp_insert.db", 12, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // open the pool
  SPoolMem *pPool = openPool();

  // start a transaction
  TXN *txn = NULL;

  tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  // generate value payload
  // char val[((4083 - 4 - 3 - 2) + 1) * 100];  // pSize(4096) - amSize(1) - pageHdr(8) - footerSize(4)
  char val[32605];
  int  valLen = sizeof(val) / sizeof(val[0]);
  generateBigVal(val, valLen);

  // insert the generated big data
  // char const *key = "key1";
  char const *key = "key123456789";
  ret = tdbTbInsert(pDb, key, strlen(key), val, valLen, txn);
  GTEST_ASSERT_EQ(ret, 0);

  // commit current transaction
  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  closePool(pPool);

  // Close a database
  tdbTbClose(pDb);

  // Close Env
  ret = tdbClose(pEnv);
  GTEST_ASSERT_EQ(ret, 0);
}

// TEST(TdbOVFLPagesTest, DISABLED_TbInsertTest) {
TEST(TdbOVFLPagesTest, TbInsertTest) { insertOfp(); }

// TEST(TdbOVFLPagesTest, DISABLED_TbGetTest) {
TEST(TdbOVFLPagesTest, TbGetTest) {
  insertOfp();

  // open Env
  int const pageSize = 4096;
  int const pageNum = 64;
  TDB      *pEnv = openEnv("tdb", pageSize, pageNum);
  GTEST_ASSERT_NE(pEnv, nullptr);

  // open db
  TTB          *pDb = NULL;
  tdb_cmpr_fn_t compFunc = tKeyCmpr;
  // int           ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  int ret = tdbTbOpen("ofp_insert.db", 12, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // generate value payload
  // char val[((4083 - 4 - 3 - 2) + 1) * 100];  // pSize(4096) - amSize(1) - pageHdr(8) - footerSize(4)
  char val[32605];
  int  valLen = sizeof(val) / sizeof(val[0]);
  generateBigVal(val, valLen);

  {  // Query the data
    void *pVal = NULL;
    int   vLen;

    // char const *key = "key1";
    char const *key = "key123456789";
    ret = tdbTbGet(pDb, key, strlen(key), &pVal, &vLen);
    ASSERT(ret == 0);
    GTEST_ASSERT_EQ(ret, 0);

    GTEST_ASSERT_EQ(vLen, valLen);
    GTEST_ASSERT_EQ(memcmp(val, pVal, vLen), 0);

    tdbFree(pVal);
  }

  // Close a database
  tdbTbClose(pDb);

  // Close Env
  ret = tdbClose(pEnv);
  GTEST_ASSERT_EQ(ret, 0);
}

// TEST(TdbOVFLPagesTest, DISABLED_TbDeleteTest) {
TEST(TdbOVFLPagesTest, TbDeleteTest) {
  int ret = 0;

  taosRemoveDir("tdb");

  // open Env
  int const pageSize = 4096;
  int const pageNum = 64;
  TDB      *pEnv = openEnv("tdb", pageSize, pageNum);
  GTEST_ASSERT_NE(pEnv, nullptr);

  // open db
  TTB          *pDb = NULL;
  tdb_cmpr_fn_t compFunc = tKeyCmpr;
  ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // open the pool
  SPoolMem *pPool = openPool();

  // start a transaction
  TXN *txn;

  tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  // generate value payload
  // char val[((4083 - 4 - 3 - 2) + 1) * 100];  // pSize(4096) - amSize(1) - pageHdr(8) - footerSize(4)
  char val[((4083 - 4 - 3 - 2) + 1) * 2];  // pSize(4096) - amSize(1) - pageHdr(8) - footerSize(4)
  int  valLen = sizeof(val) / sizeof(val[0]);
  generateBigVal(val, valLen);

  {  // insert the generated big data
    ret = tdbTbInsert(pDb, "key1", strlen("key1"), val, valLen, txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  {  // query the data
    void *pVal = NULL;
    int   vLen;

    ret = tdbTbGet(pDb, "key1", strlen("key1"), &pVal, &vLen);
    ASSERT(ret == 0);
    GTEST_ASSERT_EQ(ret, 0);

    GTEST_ASSERT_EQ(vLen, valLen);
    GTEST_ASSERT_EQ(memcmp(val, pVal, vLen), 0);

    tdbFree(pVal);
  }
  /* open to debug committed file
tdbCommit(pEnv, &txn);
tdbTxnClose(&txn);

++txnid;
tdbTxnOpen(&txn, txnid, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
tdbBegin(pEnv, &txn);
  */
  {  // upsert the data
    ret = tdbTbUpsert(pDb, "key1", strlen("key1"), "value1", strlen("value1"), txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  {  // query the upserted data
    void *pVal = NULL;
    int   vLen;

    ret = tdbTbGet(pDb, "key1", strlen("key1"), &pVal, &vLen);
    ASSERT(ret == 0);
    GTEST_ASSERT_EQ(ret, 0);

    GTEST_ASSERT_EQ(vLen, strlen("value1"));
    GTEST_ASSERT_EQ(memcmp("value1", pVal, vLen), 0);

    tdbFree(pVal);
  }

  {  // delete the data
    ret = tdbTbDelete(pDb, "key1", strlen("key1"), txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  {  // query the deleted data
    void *pVal = NULL;
    int   vLen = -1;

    ret = tdbTbGet(pDb, "key1", strlen("key1"), &pVal, &vLen);
    ASSERT(ret == -1);
    GTEST_ASSERT_EQ(ret, -1);

    GTEST_ASSERT_EQ(vLen, -1);
    GTEST_ASSERT_EQ(pVal, nullptr);

    tdbFree(pVal);
  }

  // commit current transaction
  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  closePool(pPool);

  // Close a database
  tdbTbClose(pDb);

  // Close Env
  ret = tdbClose(pEnv);
  GTEST_ASSERT_EQ(ret, 0);
}

// TEST(tdb_test, DISABLED_simple_insert1) {
TEST(tdb_test, simple_insert1) {
  int           ret;
  TDB          *pEnv;
  TTB          *pDb;
  tdb_cmpr_fn_t compFunc;
  int           nData = 1;
  TXN          *txn;
  int const     pageSize = 4096;

  taosRemoveDir("tdb");

  // Open Env
  ret = tdbOpen("tdb", pageSize, 64, &pEnv, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // Create a database
  compFunc = tKeyCmpr;
  ret = tdbTbOpen("db.db", -1, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  {
    char key[64];
    // char      val[(4083 - 4 - 3 - 2)]; // pSize(4096) - amSize(1) - pageHdr(8) - footerSize(4)
    char      val[(4083 - 4 - 3 - 2) + 1];  // pSize(4096) - amSize(1) - pageHdr(8) - footerSize(4)
    int64_t   poolLimit = 4096;             // 1M pool limit
    SPoolMem *pPool;

    // open the pool
    pPool = openPool();

    // start a transaction
    tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

    for (int iData = 1; iData <= nData; iData++) {
      sprintf(key, "key0");
      sprintf(val, "value%d", iData);

      // ret = tdbTbInsert(pDb, key, strlen(key), val, strlen(val), &txn);
      // GTEST_ASSERT_EQ(ret, 0);

      // generate value payload
      int valLen = sizeof(val) / sizeof(val[0]);
      for (int i = 6; i < valLen; ++i) {
        char c = char(i & 0xff);
        if (c == 0) {
          c = 1;
        }
        val[i] = c;
      }

      ret = tdbTbInsert(pDb, "key1", strlen("key1"), val, valLen, txn);
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

    closePool(pPool);

    {  // Query the data
      void *pVal = NULL;
      int   vLen;

      for (int i = 1; i <= nData; i++) {
        sprintf(key, "key%d", i);
        // sprintf(val, "value%d", i);

        ret = tdbTbGet(pDb, key, strlen(key), &pVal, &vLen);
        ASSERT(ret == 0);
        GTEST_ASSERT_EQ(ret, 0);

        GTEST_ASSERT_EQ(vLen, sizeof(val) / sizeof(val[0]));
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
