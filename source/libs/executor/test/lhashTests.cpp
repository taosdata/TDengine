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
#include "executorInt.h"
#include "tlinearhash.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

TEST(testCase, linear_hash_Tests) {
  taosSeedRand(taosGetTimestampSec());
  strcpy(tsTempDir, "/tmp/");

  _hash_fn_t fn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT);

  int64_t st = taosGetTimestampUs();

  SLHashObj* pHashObj = tHashInit(4098 * 4 * 2, 512, fn, 40);
  for (int32_t i = 0; i < 1000000; ++i) {
    int32_t code = tHashPut(pHashObj, &i, sizeof(i), &i, sizeof(i));
    assert(code == 0);
  }

  //  tHashPrint(pHashObj, LINEAR_HASH_STATIS);
  int64_t et = taosGetTimestampUs();

  for (int32_t i = 0; i < 1000000; ++i) {
    if (i == 950000) {
      printf("kf\n");
    }
    char* v = tHashGet(pHashObj, &i, sizeof(i));
    if (v != NULL) {
      //      printf("find value: %d, key:%d\n", *(int32_t*) v, i);
    } else {
      //      printf("failed to found key:%d in hash\n", i);
    }
  }

  //  tHashPrint(pHashObj, LINEAR_HASH_STATIS);
  tHashCleanup(pHashObj);
  int64_t et1 = taosGetTimestampUs();

  SHashObj* pHashObj1 = taosHashInit(1000, fn, false, HASH_NO_LOCK);
  for (int32_t i = 0; i < 1000000; ++i) {
    taosHashPut(pHashObj1, &i, sizeof(i), &i, sizeof(i));
  }

  for (int32_t i = 0; i < 1000000; ++i) {
    void* v = taosHashGet(pHashObj1, &i, sizeof(i));
  }
  taosHashCleanup(pHashObj1);

  int64_t et2 = taosGetTimestampUs();
  printf("linear hash time:%.2f ms, buildHash:%.2f ms, hash:%.2f\n", (et1 - st) / 1000.0, (et - st) / 1000.0,
         (et2 - et1) / 1000.0);
}