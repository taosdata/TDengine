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
#include <iostream>
#include "executorimpl.h"
#include "tlinearhash.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

TEST(testCase, linear_hash_Tests) {
  srand(time(NULL));

  _hash_fn_t fn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT);
#if 1
  SLHashObj* pHashObj = tHashInit(10, 128 + 8, fn, 8);
  for(int32_t i = 0; i < 100; ++i) {
    int32_t code = tHashPut(pHashObj, &i, sizeof(i), &i, sizeof(i));
    assert(code == 0);
  }

//  tHashPrint(pHashObj, LINEAR_HASH_STATIS);

//  for(int32_t i = 0; i < 10000; ++i) {
//    char* v = tHashGet(pHashObj, &i, sizeof(i));
//    if (v != NULL) {
////      printf("find value: %d, key:%d\n", *(int32_t*) v, i);
//    } else {
//      printf("failed to found key:%d in hash\n", i);
//    }
//  }

  tHashPrint(pHashObj, LINEAR_HASH_DATA);
  tHashCleanup(pHashObj);
#endif

#if 0
  SHashObj* pHashObj = taosHashInit(1000, fn, false, HASH_NO_LOCK);
  for(int32_t i = 0; i < 500000; ++i) {
    taosHashPut(pHashObj, &i, sizeof(i), &i, sizeof(i));
  }

  for(int32_t i = 0; i < 10000; ++i) {
    void* v = taosHashGet(pHashObj, &i, sizeof(i));
  }
  taosHashCleanup(pHashObj);
#endif

}