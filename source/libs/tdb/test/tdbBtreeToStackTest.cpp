#include <gtest/gtest.h>

#define ALLOW_FORBID_FUNC
#include "os.h"
#include "tdb.h"
#include "tdbInt.h"

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

static TDB *openEnv(char const *envName, int const pageSize, int const pageNum) {
  TDB *pEnv = NULL;

  int ret = tdbOpen(envName, pageSize, pageNum, &pEnv, 0, 0, NULL);
  if (ret) {
    pEnv = NULL;
  }

  return pEnv;
}

struct STTB {
  TDB    *pEnv;
  SBTree *pBt;
};

struct SBTree {
  SPgno         root;
  int           keyLen;
  int           valLen;
  SPager       *pPager;
  tdb_cmpr_fn_t kcmpr;
  int           pageSize;
  int           maxLocal;
  int           minLocal;
  int           maxLeaf;
  int           minLeaf;
  SBtInfo       info;
  char         *tbname;
  void         *pBuf;
};

TEST(upgrade_test, upgrade_full) {
  taosRemoveDir("tdb");

  // open Env
  TDB      *pEnv = openEnv("tdb", 4096, 64);
  GTEST_ASSERT_NE(pEnv, nullptr);

  // open db
  TTB          *pTb = NULL;
  tdb_cmpr_fn_t compFunc = tDefaultKeyCmpr;
  int ret = tdbTbOpen("free.db", 4, 0, compFunc, pEnv, &pTb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  SBTree* pBt = pTb->pBt;
  SPager *pPager = pBt->pPager;
  // open the pool
  SPoolMem *pPool = openPool();

  // start a transaction
  TXN *txn = NULL;
  tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  for (int i = pBt->root + 1; i <= 700; i++) {
    SPgno pgno = i;
    SPage *pPage = NULL;
    SBtreeInitPageArg arg;
    arg.flags = 0x1 | 0x2;  // root leaf node;
    arg.pBt = pBt;
    tdbPagerFetchPage(pPager, &pgno, &pPage, tdbBtreeInitPage, &arg, txn);
    tdbPagerWrite(pPager, pPage);
    tdbPagerReturnPage(pPager, pPage, txn);
  }

  for (int i = pBt->root + 1; i <= pBt->root + 680; i++) {
    ret = tdbTbInsert(pTb, &i, sizeof(i), NULL, 0, txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  // commit current transaction
  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  tdbBtreeToStack(pBt);

  tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  for (int i = 1; i <= 680; i++) {
    SPgno pgno;
    ret = tdbBtreePopFreePage(pBt, &pgno, txn);
    GTEST_ASSERT_EQ(ret, 0);
    GTEST_ASSERT_NE(pgno, 0);
  }

  SPgno pgno;
  ret = tdbBtreePopFreePage(pBt, &pgno, txn);
  GTEST_ASSERT_EQ(ret, 0);
  GTEST_ASSERT_EQ(pgno, 0);

  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  closePool(pPool);

  // Close a database
  tdbTbClose(pTb);

  // Close Env
  tdbClose(pEnv);
}


TEST(upgrade_test, upgrade_rebalanced) {
  taosRemoveDir("tdb");

  // open Env
  TDB      *pEnv = openEnv("tdb", 4096, 64);
  GTEST_ASSERT_NE(pEnv, nullptr);

  // open db
  TTB          *pTb = NULL;
  tdb_cmpr_fn_t compFunc = tDefaultKeyCmpr;
  int ret = tdbTbOpen("free.db", 4, 0, compFunc, pEnv, &pTb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  SBTree* pBt = pTb->pBt;
  SPager *pPager = pBt->pPager;
  // open the pool
  SPoolMem *pPool = openPool();

  // start a transaction
  TXN *txn = NULL;
  tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  for (int i = pBt->root + 1; i <= 700; i++) {
    SPgno pgno = i;
    SPage *pPage = NULL;
    SBtreeInitPageArg arg;
    arg.flags = 0x1 | 0x2;  // root leaf node;
    arg.pBt = pBt;
    tdbPagerFetchPage(pPager, &pgno, &pPage, tdbBtreeInitPage, &arg, txn);
    tdbPagerWrite(pPager, pPage);
    tdbPagerReturnPage(pPager, pPage, txn);
  }

  for (int i = pBt->root + 1; i <= pBt->root + 700; i++) {
    ret = tdbTbInsert(pTb, &i, sizeof(i), NULL, 0, txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  // commit current transaction
  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  tdbBtreeToStack(pBt);

  tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  SPgno pgno;
  ret = tdbBtreePopFreePage(pBt, &pgno, txn);
  GTEST_ASSERT_EQ(ret, 0);
  GTEST_ASSERT_EQ(pgno, 0);

  for (int i = pBt->root + 1; i <= pBt->root + 700; i++) {
    SPgno pgno = i;
    SPage *pPage = NULL;
    SBtreeInitPageArg arg;
    arg.flags = 0x1 | 0x2;  // root leaf node;
    arg.pBt = pBt;
    tdbPagerFetchPage(pPager, &pgno, &pPage, tdbBtreeInitPage, &arg, txn);
    tdbBtreePushFreePage(pBt, pPage, txn);
    tdbPagerReturnPage(pPager, pPage, txn);
  }

  for (int i = pBt->root + 700; i >= pBt->root + 1; i--) {
    SPgno pgno;
    ret = tdbBtreePopFreePage(pBt, &pgno, txn);
    GTEST_ASSERT_EQ(ret, 0);
    GTEST_ASSERT_EQ(pgno, i);
  }

  ret = tdbBtreePopFreePage(pBt, &pgno, txn);
  GTEST_ASSERT_EQ(ret, 0);
  GTEST_ASSERT_EQ(pgno, 0);

  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  closePool(pPool);

  // Close a database
  tdbTbClose(pTb);

  // Close Env
  tdbClose(pEnv);
}


TEST(upgrade_test, push_pop_after_upgrade) {
  taosRemoveDir("tdb");

  // open Env
  TDB      *pEnv = openEnv("tdb", 4096, 64);
  GTEST_ASSERT_NE(pEnv, nullptr);

  // open db
  TTB          *pTb = NULL;
  tdb_cmpr_fn_t compFunc = tDefaultKeyCmpr;
  int ret = tdbTbOpen("free.db", 4, 0, compFunc, pEnv, &pTb, 0);
  GTEST_ASSERT_EQ(ret, 0);

  SBTree* pBt = pTb->pBt;
  SPager *pPager = pBt->pPager;
  // open the pool
  SPoolMem *pPool = openPool();

  // start a transaction
  TXN *txn = NULL;
  tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  for (int i = pBt->root + 1; i <= pBt->root + 700; i++) {
    SPgno pgno = i;
    SPage *pPage = NULL;
    SBtreeInitPageArg arg;
    arg.flags = 0x1 | 0x2;  // root leaf node;
    arg.pBt = pBt;
    tdbPagerFetchPage(pPager, &pgno, &pPage, tdbBtreeInitPage, &arg, txn);
    tdbPagerWrite(pPager, pPage);
    tdbPagerReturnPage(pPager, pPage, txn);
  }

  for (int i = pBt->root + 1; i <= pBt->root + 680; i++) {
    ret = tdbTbInsert(pTb, &i, sizeof(i), NULL, 0, txn);
    GTEST_ASSERT_EQ(ret, 0);
  }

  // commit current transaction
  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  tdbBtreeToStack(pBt);

  tdbBegin(pEnv, &txn, poolMalloc, poolFree, pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);

  for (int i = pBt->root + 681; i <= pBt->root + 700; i++) {
    SPgno pgno = i;
    SPage *pPage = NULL;
    SBtreeInitPageArg arg;
    arg.flags = 0x1 | 0x2;  // root leaf node;
    arg.pBt = pBt;
    tdbPagerFetchPage(pPager, &pgno, &pPage, tdbBtreeInitPage, &arg, txn);
    tdbBtreePushFreePage(pBt, pPage, txn);
    tdbPagerReturnPage(pPager, pPage, txn);
  }

  for (int i = pBt->root + 700; i >= pBt->root + 681; i--) {
    SPgno pgno;
    ret = tdbBtreePopFreePage(pBt, &pgno, txn);
    GTEST_ASSERT_EQ(ret, 0);
    GTEST_ASSERT_EQ(pgno, i);
  }

  for (int i = 1; i <= 680; i++) {
    SPgno pgno;
    ret = tdbBtreePopFreePage(pBt, &pgno, txn);
    GTEST_ASSERT_EQ(ret, 0);
    GTEST_ASSERT_NE(pgno, 0);
  }

  SPgno pgno;
  ret = tdbBtreePopFreePage(pBt, &pgno, txn);
  GTEST_ASSERT_EQ(ret, 0);
  GTEST_ASSERT_EQ(pgno, 0);

  tdbCommit(pEnv, txn);
  tdbPostCommit(pEnv, txn);

  closePool(pPool);

  // Close a database
  tdbTbClose(pTb);

  // Close Env
  tdbClose(pEnv);
}
