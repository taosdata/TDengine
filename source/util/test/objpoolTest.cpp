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
#include <stdlib.h>
#include <time.h>

#include "tarray.h"
#include "tobjpool.h"

TEST(objpoolTest, shrink_empty_pool_rebuilds_shared_free_list) {
  SObjPool pool = {};
  SObjList first = {};
  SObjList second = {};
  ASSERT_EQ(taosObjPoolInit(&pool, 32, sizeof(int32_t)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosObjListInit(&first, &pool), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosObjListInit(&second, &pool), TSDB_CODE_SUCCESS);
  for (int32_t value = 0; value < 32; ++value) {
    ASSERT_EQ(taosObjListAppend(value % 2 == 0 ? &first : &second, &value), TSDB_CODE_SUCCESS);
  }
  taosObjListClear(&first);
  taosObjListClear(&second);
  ASSERT_EQ(taosObjPoolShrinkEmpty(&pool, 8), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pool.capacity, 8);
  EXPECT_EQ(pool.size, 0);
  EXPECT_EQ(first.headIdx, TOBJPOOL_INVALID_IDX);
  EXPECT_EQ(second.tailIdx, TOBJPOOL_INVALID_IDX);
  for (int32_t idx = 0; idx < 8; ++idx) {
    const SObjPoolNode *node = TOBJPOOL_GET_NODE(&pool, idx);
    EXPECT_EQ(node->prevIdx, idx - 1);
    EXPECT_EQ(node->nextIdx, idx == 7 ? TOBJPOOL_INVALID_IDX : idx + 1);
  }
  for (int32_t value = 0; value < 20; ++value) {
    ASSERT_EQ(taosObjListAppend(value % 2 == 0 ? &first : &second, &value), TSDB_CODE_SUCCESS);
  }
  EXPECT_GE(pool.capacity, 20);
  for (int32_t value = 0; value < 20; ++value) {
    SObjList *list = value % 2 == 0 ? &first : &second;
    ASSERT_NE(taosObjListGetHead(list), nullptr);
    EXPECT_EQ(*static_cast<int32_t *>(taosObjListGetHead(list)), value);
    taosObjListPopHead(list);
  }
  EXPECT_EQ(pool.size, 0);
  taosObjPoolDestroy(&pool);
}

TEST(objpoolTest, shrink_rejects_nonempty_pool_with_high_index_node) {
  SObjPool pool = {};
  SObjList list = {};
  ASSERT_EQ(taosObjPoolInit(&pool, 32, sizeof(int32_t)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosObjListInit(&list, &pool), TSDB_CODE_SUCCESS);
  for (int32_t value = 0; value < 32; ++value) {
    ASSERT_EQ(taosObjListAppend(&list, &value), TSDB_CODE_SUCCESS);
  }
  for (int32_t count = 0; count < 31; ++count) {
    taosObjListPopHead(&list);
  }
  void *data = pool.pData;
  int64_t freeHead = pool.freeHeadIdx;
  int64_t freeTail = pool.freeTailIdx;
  ASSERT_EQ(taosObjPoolShrinkEmpty(&pool, 4), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(pool.pData, data);
  EXPECT_EQ(pool.capacity, 32);
  EXPECT_EQ(pool.size, 1);
  EXPECT_EQ(pool.freeHeadIdx, freeHead);
  EXPECT_EQ(pool.freeTailIdx, freeTail);
  EXPECT_EQ(list.headIdx, 31);
  EXPECT_EQ(*static_cast<int32_t *>(taosObjListGetHead(&list)), 31);
  taosObjListClear(&list);
  ASSERT_EQ(taosObjPoolShrinkEmpty(&pool, 4), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pool.capacity, 4);
  taosObjPoolDestroy(&pool);
}

TEST(objpoolTest, shrink_empty_pool_validates_capacity) {
  SObjPool pool = {};
  EXPECT_EQ(taosObjPoolShrinkEmpty(nullptr, 4), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(taosObjPoolShrinkEmpty(&pool, 4), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(taosObjPoolInit(&pool, 16, sizeof(int32_t)), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosObjPoolShrinkEmpty(&pool, -1), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(taosObjPoolShrinkEmpty(&pool, 16), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosObjPoolShrinkEmpty(&pool, 32), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pool.capacity, 16);
  EXPECT_EQ(taosObjPoolShrinkEmpty(&pool, 0), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pool.capacity, 4);
  EXPECT_EQ(pool.freeHeadIdx, 0);
  EXPECT_EQ(pool.freeTailIdx, 3);
  taosObjPoolDestroy(&pool);
}

TEST(objpoolTest, objpool_basic_test) {
  int32_t code = TSDB_CODE_SUCCESS;

  SObjPool pool;
  code = taosObjPoolInit(&pool, 4, sizeof(int32_t));
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  SObjList list;
  code = taosObjListInit(&list, &pool);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  SArray *ref = taosArrayInit(4, sizeof(int32_t));

  taosSeedRand(238757983);
  int32_t round = 10000;
  for (int32_t r = 0; r < round; r++) {
    int32_t nappend = taosRand() % 100 + 50;
    int32_t npop = taosRand() % 100;
    npop = TMIN(npop, list.neles + nappend);
    if (r % 100 == 0) {
      npop = list.neles + nappend;
    }

    printf("=== round %d: append %d, pop %d ===\n", r, nappend, npop);

    for (int32_t i = 0; i < nappend; i++) {
      int32_t e = taosRand();
      void   *px = taosArrayPush(ref, &e);
      ASSERT_NE(px, nullptr);
      code = taosObjListAppend(&list, &e);
      ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    }
    ASSERT_EQ(taosArrayGetSize(ref), list.neles);
    SObjListIter iter1;
    taosObjListInitIter(&list, &iter1, TOBJLIST_ITER_FORWARD);
    for (int32_t i = 0; i < list.neles; i++) {
      int32_t *pv = (int32_t *)taosObjListIterNext(&iter1);
      int32_t *pr = (int32_t *)taosArrayGet(ref, i);
      ASSERT_EQ(*pv, *pr);
    }

    if (npop == list.neles) {
      taosArrayClear(ref);
      taosObjListClear(&list);
    } else {
      taosArrayPopFrontBatch(ref, npop);
      for (int32_t i = 0; i < npop; i++) {
        taosObjListPopHead(&list);
      }
    }
    SObjListIter iter2;
    taosObjListInitIter(&list, &iter2, TOBJLIST_ITER_FORWARD);
    for (int32_t i = 0; i < list.neles; i++) {
      int32_t *pv = (int32_t *)taosObjListIterNext(&iter2);
      int32_t *pr = (int32_t *)taosArrayGet(ref, i);
      ASSERT_EQ(*pv, *pr);
    }
  }
}
