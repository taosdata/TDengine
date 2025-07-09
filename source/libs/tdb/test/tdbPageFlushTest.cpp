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

  TD_ALWAYS_ASSERT(pPool->size == 0);
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
    TD_ALWAYS_ASSERT(0);
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

static void *poolMallocRestricted(void *arg, size_t size) {
  void     *ptr = NULL;
  SPoolMem *pPool = (SPoolMem *)arg;
  SPoolMem *pMem;

  if (pPool->size > 1024 * 1024 * 10) {
    return NULL;
  }

  pMem = (SPoolMem *)taosMemoryMalloc(sizeof(*pMem) + size);
  if (pMem == NULL) {
    TD_ALWAYS_ASSERT(0);
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

static TDB *openEnv(char const *envName, int const pageSize, int const pageNum) {
  TDB *pEnv = NULL;

  int ret = tdbOpen(envName, pageSize, pageNum, &pEnv, 0, 0, NULL);
  if (ret) {
    pEnv = NULL;
  }

  return pEnv;
}

static void clearDb(char const *db) { taosRemoveDir(db); }

static void generateBigVal(char *val, int valLen) {
  for (int i = 0; i < valLen; ++i) {
    char c = char(i & 0xff);
    if (c == 0) {
      c = 1;
    }
    val[i] = c;
  }
}

static void insertOfp(void) {
  int ret = 0;

  // open Env
  int const pageSize = 4096;
  int const pageNum = 64;
  TDB      *pEnv = openEnv("tdb", pageSize, pageNum);
  GTEST_ASSERT_NE(pEnv, nullptr);

  // open db
  TTB          *pDb = NULL;
  tdb_cmpr_fn_t compFunc = tKeyCmpr;
  // ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
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
  char const *key = "key123456789";
  ret = tdbTbInsert(pDb, key, strlen(key) + 1, val, valLen, txn);
  GTEST_ASSERT_EQ(ret, 0);

  // commit current transaction
  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  closePool(pPool);

  // Close a database
  tdbTbClose(pDb);

  // Close Env
  tdbClose(pEnv);
}

static void insertMultipleOfp(void) {
  int ret = 0;

  // open Env
  int const pageSize = 4096;
  int const pageNum = 64;
  TDB      *pEnv = openEnv("tdb", pageSize, pageNum);
  GTEST_ASSERT_NE(pEnv, nullptr);

  // open db
  TTB          *pDb = NULL;
  tdb_cmpr_fn_t compFunc = tKeyCmpr;
  // ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // open the pool
  SPoolMem *pPool = openPool();

  // start a transaction
  TXN *txn = NULL;

  tdbBegin(pEnv, &txn, poolMallocRestricted, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  // generate value payload
  // char val[((4083 - 4 - 3 - 2) + 1) * 100];  // pSize(4096) - amSize(1) - pageHdr(8) - footerSize(4)
  char val[32605];
  int  valLen = sizeof(val) / sizeof(val[0]);
  generateBigVal(val, valLen);

  // insert the generated big data
  // char const *key = "key1";
  for (int i = 0; i < 1024 * 4; ++i) {
    // char const *key = "key123456789";
    char key[32] = {0};
    sprintf(key, "key-%d", i);
    ret = tdbTbInsert(pDb, key, strlen(key) + 1, val, valLen, txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  // commit current transaction
  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  closePool(pPool);

  // Close a database
  tdbTbClose(pDb);

  // Close Env
  tdbClose(pEnv);
}

// TEST(TdbPageFlushTest, DISABLED_TbRestoreTest) {
TEST(TdbPageFlushTest, TbRestoreTest) {
  clearDb("tdb");

  insertMultipleOfp();
}

// TEST(TdbPageFlushTest, DISABLED_TbRestoreTest2) {
TEST(TdbPageFlushTest, TbRestoreTest2) {
  clearDb("tdb");

  int ret = 0;

  // open Env
  int const pageSize = 4096;
  int const pageNum = 64;
  TDB      *pEnv = openEnv("tdb", pageSize, pageNum);
  GTEST_ASSERT_NE(pEnv, nullptr);

  // open db
  TTB          *pDb = NULL;
  tdb_cmpr_fn_t compFunc = tKeyCmpr;
  // ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // open the pool
  SPoolMem *pPool = openPool();

  // start a transaction
  TXN *txn = NULL;

  tdbBegin(pEnv, &txn, poolMallocRestricted, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  // generate value payload
  // char val[((4083 - 4 - 3 - 2) + 1) * 100];  // pSize(4096) - amSize(1) - pageHdr(8) - footerSize(4)
  char val[32605];
  int  valLen = sizeof(val) / sizeof(val[0]);
  generateBigVal(val, valLen);

  // insert the generated big data
  // char const *key = "key1";
  for (int i = 0; i < 1024 * 4; ++i) {
    // char const *key = "key123456789";
    char key[32] = {0};
    sprintf(key, "key-%d", i);
    ret = tdbTbInsert(pDb, key, strlen(key) + 1, val, valLen, txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  // commit current transaction
  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);
  tdbBegin(pEnv, &txn, poolMallocRestricted, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  for (int i = 1024 * 4; i < 1024 * 8; ++i) {
    char key[32] = {0};
    sprintf(key, "key-%d", i);
    ret = tdbTbInsert(pDb, key, strlen(key) + 1, val, valLen, txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  closePool(pPool);

  // Close a database
  tdbTbClose(pDb);

  // Close Env
  tdbClose(pEnv);
}

// TEST(TdbPageFlushTest, DISABLED_TbRestoreTest3) {
TEST(TdbPageFlushTest, TbRestoreTest3) {
  clearDb("tdb");

  int ret = 0;

  // open Env
  int const pageSize = 4096;
  int const pageNum = 64;
  TDB      *pEnv = openEnv("tdb", pageSize, pageNum);
  GTEST_ASSERT_NE(pEnv, nullptr);

  // open db
  TTB          *pDb = NULL;
  tdb_cmpr_fn_t compFunc = tKeyCmpr;
  // ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // open the pool
  SPoolMem *pPool = openPool();

  // start a transaction
  TXN *txn = NULL;

  tdbBegin(pEnv, &txn, poolMallocRestricted, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  // generate value payload
  // char val[((4083 - 4 - 3 - 2) + 1) * 100];  // pSize(4096) - amSize(1) - pageHdr(8) - footerSize(4)
  char val[32605];
  int  valLen = sizeof(val) / sizeof(val[0]);
  generateBigVal(val, valLen);

  // insert the generated big data
  // char const *key = "key1";
  for (int i = 0; i < 1024 * 4; ++i) {
    // char const *key = "key123456789";
    char key[32] = {0};
    sprintf(key, "key-%d", i);
    ret = tdbTbInsert(pDb, key, strlen(key) + 1, val, valLen, txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  // commit current transaction
  tdbAbort(pEnv, txn);

  tdbBegin(pEnv, &txn, poolMallocRestricted, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  for (int i = 1024 * 4; i < 1024 * 8; ++i) {
    char key[32] = {0};
    sprintf(key, "key-%d", i);
    ret = tdbTbInsert(pDb, key, strlen(key) + 1, val, valLen, txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  closePool(pPool);

  // Close a database
  tdbTbClose(pDb);

  // Close Env
  tdbClose(pEnv);
}

// TEST(TdbPageFlushTest, DISABLED_TbRestoreTest4) {
TEST(TdbPageFlushTest, TbRestoreTest4) {
  clearDb("tdb");

  int ret = 0;

  // open Env
  int const pageSize = 4096;
  int const pageNum = 64;
  TDB      *pEnv = openEnv("tdb", pageSize, pageNum);
  GTEST_ASSERT_NE(pEnv, nullptr);

  // open db
  TTB          *pDb = NULL;
  tdb_cmpr_fn_t compFunc = tKeyCmpr;
  // ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // open the pool
  SPoolMem *pPool = openPool();

  // start a transaction
  TXN *txn = NULL;

  tdbBegin(pEnv, &txn, poolMallocRestricted, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  // generate value payload
  // char val[((4083 - 4 - 3 - 2) + 1) * 100];  // pSize(4096) - amSize(1) - pageHdr(8) - footerSize(4)
  char val[32605];
  int  valLen = sizeof(val) / sizeof(val[0]);
  generateBigVal(val, valLen);

  // insert the generated big data
  // char const *key = "key1";
  for (int i = 0; i < 1024 * 4; ++i) {
    // char const *key = "key123456789";
    char key[32] = {0};
    sprintf(key, "key-%d", i);
    ret = tdbTbInsert(pDb, key, strlen(key) + 1, val, valLen, txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  // commit current transaction
  tdbAbort(pEnv, txn);
  closePool(pPool);
  tdbTbClose(pDb);
  tdbClose(pEnv);

  pEnv = openEnv("tdb", pageSize, pageNum);
  GTEST_ASSERT_NE(pEnv, nullptr);

  ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  pPool = openPool();
  tdbBegin(pEnv, &txn, poolMallocRestricted, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  for (int i = 1024 * 4; i < 1024 * 8; ++i) {
    char key[32] = {0};
    sprintf(key, "key-%d", i);
    ret = tdbTbInsert(pDb, key, strlen(key) + 1, val, valLen, txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  closePool(pPool);

  // Close a database
  tdbTbClose(pDb);

  // Close Env
  tdbClose(pEnv);
}

// TEST(TdbPageFlushTest, DISABLED_TbRestoreTest5) {
TEST(TdbPageFlushTest, TbRestoreTest5) {
  clearDb("tdb");

  int ret = 0;

  // open Env
  int const pageSize = 4096;
  int const pageNum = 64;
  TDB      *pEnv = openEnv("tdb", pageSize, pageNum);
  GTEST_ASSERT_NE(pEnv, nullptr);

  // open db
  TTB          *pDb = NULL;
  tdb_cmpr_fn_t compFunc = tKeyCmpr;
  // ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  // open the pool
  SPoolMem *pPool = openPool();

  // start a transaction
  TXN *txn = NULL;

  tdbBegin(pEnv, &txn, poolMallocRestricted, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  // generate value payload
  // char val[((4083 - 4 - 3 - 2) + 1) * 100];  // pSize(4096) - amSize(1) - pageHdr(8) - footerSize(4)
  char val[32605];
  int  valLen = sizeof(val) / sizeof(val[0]);
  generateBigVal(val, valLen);

  // insert the generated big data
  // char const *key = "key1";
  for (int i = 0; i < 1024 * 4; ++i) {
    // char const *key = "key123456789";
    char key[32] = {0};
    sprintf(key, "key-%d", i);
    ret = tdbTbInsert(pDb, key, strlen(key) + 1, val, valLen, txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  // commit current transaction
  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);
  closePool(pPool);
  tdbTbClose(pDb);
  tdbClose(pEnv);

  pEnv = openEnv("tdb", pageSize, pageNum);
  GTEST_ASSERT_NE(pEnv, nullptr);

  ret = tdbTbOpen("ofp_insert.db", -1, -1, compFunc, pEnv, &pDb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  pPool = openPool();
  tdbBegin(pEnv, &txn, poolMallocRestricted, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  for (int i = 1024 * 4; i < 1024 * 8; ++i) {
    char key[32] = {0};
    sprintf(key, "key-%d", i);
    ret = tdbTbInsert(pDb, key, strlen(key) + 1, val, valLen, txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  closePool(pPool);

  // Close a database
  tdbTbClose(pDb);

  // Close Env
  tdbClose(pEnv);
}
