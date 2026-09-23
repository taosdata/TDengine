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
