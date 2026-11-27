/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "tdbInt.h"

#include <shared_mutex>
#include <string>
#include <thread>
#include <tuple>
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

  int ret = tdbOpen(envName, pageSize, pageNum, &pEnv, 0, NULL);
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

  for (int i = pBt->root + 1; i <= pBt->root + 700; i++) {
    SPgno pgno = 0;
    SPage *pPage = NULL;
    SBtreeInitPageArg arg;
    arg.flags = 0x1 | 0x2;  // root leaf node;
    arg.pBt = pBt;
    ret = tdbPagerFetchPage(pPager, &pgno, &pPage, tdbBtreeInitPage, &arg, txn);
    GTEST_ASSERT_EQ(ret, 0);
    GTEST_ASSERT_EQ(pgno, i);
    ret = tdbPagerWrite(pPager, pPage);
    GTEST_ASSERT_EQ(ret, 0);
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

  for (int i = pBt->root + 1; i <= pBt->root + 700; i++) {
    SPgno pgno = 0;
    SPage *pPage = NULL;
    SBtreeInitPageArg arg;
    arg.flags = 0x1 | 0x2;  // root leaf node;
    arg.pBt = pBt;
    ret = tdbPagerFetchPage(pPager, &pgno, &pPage, tdbBtreeInitPage, &arg, txn);
    GTEST_ASSERT_EQ(ret, 0);
    GTEST_ASSERT_EQ(pgno, i);
    ret = tdbPagerWrite(pPager, pPage);
    GTEST_ASSERT_EQ(ret, 0);
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
    SPgno pgno = 0;
    SPage *pPage = NULL;
    SBtreeInitPageArg arg;
    arg.flags = 0x1 | 0x2;  // root leaf node;
    arg.pBt = pBt;
    ret = tdbPagerFetchPage(pPager, &pgno, &pPage, tdbBtreeInitPage, &arg, txn);
    GTEST_ASSERT_EQ(ret, 0);
    GTEST_ASSERT_EQ(pgno, i);
    ret = tdbPagerWrite(pPager, pPage);
    GTEST_ASSERT_EQ(ret, 0);
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

class TdbFreePageJournalTest
    : public ::testing::TestWithParam<std::tuple<int, bool, bool>> {
 protected:
  enum {
    kPageSize = 4096,
    kCachePages = 1024,
    kLeafFlag = 0x2,
    kOverflowFlag = 0x4,
  };

  void SetUp() override {
    dbName = "tdb-free-page-journal-" + std::to_string(taosGetPId()) + "-" +
             std::to_string(taosGetTimestampUs());
    ASSERT_EQ(tdbOpen(dbName.c_str(), kPageSize, kCachePages, &env, 0, nullptr),
              0);
  }

  void TearDown() override {
    if (txn != nullptr) {
      EXPECT_EQ(tdbPagerAbort(pager(), txn), 0);
      tdbTxnClose(txn);
    }
    tdbClose(env);
    taosRemoveDir(dbName.c_str());
  }

  SPager *pager() { return env->pFreeDb->pBt->pPager; }

  void Begin() {
    ASSERT_EQ(tdbBegin(env, &txn, tdbDefaultMalloc, tdbDefaultFree, nullptr,
                       TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED),
              0);
  }

  void Commit() {
    ASSERT_EQ(tdbCommit(env, txn), 0);
    ASSERT_EQ(tdbPostCommit(env, txn), 0);
    txn = nullptr;
  }

  void Reopen() {
    ASSERT_EQ(txn, nullptr);
    tdbClose(env);
    env = nullptr;
    ASSERT_EQ(tdbOpen(dbName.c_str(), kPageSize, kCachePages, &env, 0, nullptr),
              0);
  }

  void Seed(int count) {
    ASSERT_NO_FATAL_FAILURE(Begin());
    SBtreeInitPageArg arg = {kOverflowFlag, env->pFreeDb->pBt};
    for (int i = 0; i < count; ++i) {
      SPgno pgno = 0;
      SPage *page = nullptr;
      ASSERT_EQ(tdbPagerFetchPage(pager(), &pgno, &page, tdbBtreeInitPage,
                                  &arg, txn),
                0);
      ASSERT_GT(pgno, pager()->dbOrigSize);
      ASSERT_EQ(tdbPagerWrite(pager(), page), 0);
      tdbPagerReturnPage(pager(), page, txn);
      freePages.push_back(pgno);
    }

    for (SPgno pgno : freePages) {
      SPage *page = nullptr;
      ASSERT_EQ(tdbPagerFetchPage(pager(), &pgno, &page, tdbBtreeInitPage,
                                  &arg, txn),
                0);
      ASSERT_EQ(tdbPagerInsertFreePage(pager(), page, txn), 0);
      tdbPagerReturnPage(pager(), page, txn);
    }
    ASSERT_NO_FATAL_FAILURE(Commit());
  }

  void Allocate(SPgno expected) {
    SPgno pgno = 0;
    SPage *page = nullptr;
    SBtreeInitPageArg arg = {kLeafFlag, env->pFreeDb->pBt};
    ASSERT_EQ(tdbPagerFetchPage(pager(), &pgno, &page, tdbBtreeInitPage,
                                &arg, txn),
              0);
    EXPECT_EQ(pgno, expected);
    EXPECT_EQ(page->pData[0], kLeafFlag);
    EXPECT_EQ(TDB_PAGE_TOTAL_CELLS(page), 0);
    EXPECT_EQ(tdbPagerWrite(pager(), page), 0);
    tdbPagerReturnPage(pager(), page, txn);
  }

  void CheckUndoImage(SPgno expected, const std::vector<u8> &image) {
    SPgno records = 0;
    ASSERT_EQ(tdbGetFileSize(txn->jfd, kPageSize + sizeof(SPgno), &records),
              0);
    int matches = 0;
    std::vector<u8> journalImage(kPageSize);
    for (SPgno i = 0; i < records; ++i) {
      SPgno pgno = 0;
      i64 offset = static_cast<i64>(i) * (kPageSize + sizeof(SPgno));
      ASSERT_EQ(tdbOsPRead(txn->jfd, &pgno, sizeof(pgno), offset),
                sizeof(pgno));
      if (pgno != expected) {
        continue;
      }
      ++matches;
      ASSERT_EQ(tdbOsPRead(txn->jfd, journalImage.data(), kPageSize,
                           offset + sizeof(pgno)),
                kPageSize);
      ASSERT_EQ(memcmp(journalImage.data(), image.data(), kPageSize), 0);
    }
    ASSERT_EQ(matches, 1);
  }

  TDB *env = nullptr;
  TXN *txn = nullptr;
  std::string dbName;
  std::vector<SPgno> freePages;
};

TEST_P(TdbFreePageJournalTest, RecycledPagePreservesUndoImage) {
  ASSERT_NO_FATAL_FAILURE(Seed(std::get<0>(GetParam())));
  if (std::get<1>(GetParam())) {
    ASSERT_NO_FATAL_FAILURE(Reopen());
  }

  SPgno head = freePages.back();
  i64 offset = static_cast<i64>(head - 1) * kPageSize;
  std::vector<u8> image(kPageSize);
  ASSERT_EQ(tdbOsPRead(pager()->fd, image.data(), kPageSize, offset), kPageSize);

  ASSERT_NO_FATAL_FAILURE(Begin());
  ASSERT_NO_FATAL_FAILURE(Allocate(head));
  ASSERT_NO_FATAL_FAILURE(CheckUndoImage(head, image));

  SPage *page = nullptr;
  ASSERT_EQ(tdbPagerFetchFreePage(pager(), head, &page, txn), 0);
  EXPECT_EQ(tdbPagerInsertFreePage(pager(), page, txn), 0);
  tdbPagerReturnPage(pager(), page, txn);
  ASSERT_FALSE(HasFailure());
  ASSERT_NO_FATAL_FAILURE(Allocate(head));
  ASSERT_NO_FATAL_FAILURE(CheckUndoImage(head, image));

  if (std::get<2>(GetParam())) {
    // Leave the undo journal behind to exercise startup recovery.
    ASSERT_EQ(tdbCommit(env, txn), 0);
    tdbTxnClose(txn);
    ASSERT_NO_FATAL_FAILURE(Reopen());
  } else {
    ASSERT_EQ(tdbPagerAbort(pager(), txn), 0);
    tdbTxnClose(txn);
  }

  std::vector<u8> restored(kPageSize);
  ASSERT_EQ(tdbOsPRead(pager()->fd, restored.data(), kPageSize, offset),
            kPageSize);
  ASSERT_EQ(memcmp(restored.data(), image.data(), kPageSize), 0);

  ASSERT_NO_FATAL_FAILURE(Begin());
  for (auto it = freePages.rbegin(); it != freePages.rend(); ++it) {
    SPgno pgno = 0;
    ASSERT_EQ(tdbBtreePopFreePage(env->pFreeDb->pBt, &pgno, txn), 0);
    ASSERT_EQ(pgno, *it);
  }
  SPgno pgno = 0;
  ASSERT_EQ(tdbBtreePopFreePage(env->pFreeDb->pBt, &pgno, txn), 0);
  ASSERT_EQ(pgno, 0);
  ASSERT_NO_FATAL_FAILURE(Commit());

  ASSERT_NO_FATAL_FAILURE(Reopen());
  ASSERT_NO_FATAL_FAILURE(Begin());
  SPgno extendedSize = pager()->dbFileSize + 1;
  SBtreeInitPageArg arg = {kLeafFlag, env->pFreeDb->pBt};
  pgno = 0;
  ASSERT_EQ(tdbPagerFetchPage(pager(), &pgno, &page, tdbBtreeInitPage,
                              &arg, txn),
            0);
  EXPECT_EQ(pgno, extendedSize);
  EXPECT_GT(pgno, pager()->dbOrigSize);
  EXPECT_FALSE(tdbPageGetFlag(page, PAGE_FLAG_DIRTY));

  // Recycle an unused extension page before it has ever been written.
  EXPECT_EQ(tdbPagerInsertFreePage(pager(), page, txn), 0);
  tdbPagerReturnPage(pager(), page, txn);
  ASSERT_FALSE(HasFailure());
  pgno = 0;
  ASSERT_EQ(tdbPagerFetchPage(pager(), &pgno, &page, tdbBtreeInitPage,
                              &arg, txn),
            0);
  EXPECT_EQ(pgno, extendedSize);
  EXPECT_EQ(pager()->dbFileSize, extendedSize);
  EXPECT_GT(pgno, pager()->dbOrigSize);
  EXPECT_TRUE(tdbPageGetFlag(page, PAGE_FLAG_DIRTY));
  EXPECT_EQ(page->pData[0], kLeafFlag);
  EXPECT_EQ(TDB_PAGE_TOTAL_CELLS(page), 0);
  EXPECT_EQ(tdbPagerWrite(pager(), page), 0);
  tdbPagerReturnPage(pager(), page, txn);
  ASSERT_FALSE(HasFailure());

  SPgno records = 0;
  ASSERT_EQ(tdbGetFileSize(txn->jfd, kPageSize + sizeof(SPgno), &records), 0);
  ASSERT_EQ(records, 1);
  ASSERT_EQ(tdbOsPRead(txn->jfd, &pgno, sizeof(pgno), 0), sizeof(pgno));
  ASSERT_EQ(pgno, env->pFreeDb->pBt->root);
  ASSERT_NO_FATAL_FAILURE(Commit());
}

INSTANTIATE_TEST_SUITE_P(
    StackAndCacheStates, TdbFreePageJournalTest,
    ::testing::Combine(::testing::Values(4, 700), ::testing::Bool(),
                       ::testing::Bool()));
