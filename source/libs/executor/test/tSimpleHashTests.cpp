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
#include "taos.h"
#include "thash.h"
#include "tsimplehash.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

// int main(int argc, char **argv) {
//   testing::InitGoogleTest(&argc, argv);
//   return RUN_ALL_TESTS();
// }

TEST(testCase, tSimpleHashTest) {
  SSHashObj *pHashObj =
      tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));

  assert(pHashObj != nullptr);

  ASSERT_EQ(0, tSimpleHashGetSize(pHashObj));

  size_t keyLen = sizeof(int64_t);
  size_t dataLen = sizeof(int64_t);

  int64_t originKeySum = 0;
  for (int64_t i = 1; i <= 100; ++i) {
    originKeySum += i;
    tSimpleHashPut(pHashObj, (const void *)&i, keyLen, (const void *)&i, dataLen);
    ASSERT_EQ(i, tSimpleHashGetSize(pHashObj));
  }

  for (int64_t i = 1; i <= 100; ++i) {
    void *data = tSimpleHashGet(pHashObj, (const void *)&i, keyLen);
    ASSERT_EQ(i, *(int64_t *)data);
  }

  void   *data = NULL;
  int32_t iter = 0;
  int64_t keySum = 0;
  int64_t dataSum = 0;
  while ((data = tSimpleHashIterate(pHashObj, data, &iter))) {
    void *key = tSimpleHashGetKey(data, NULL);
    keySum += *(int64_t *)key;
    dataSum += *(int64_t *)data;
  }
  
  ASSERT_EQ(keySum, dataSum);
  ASSERT_EQ(keySum, originKeySum);

  for (int64_t i = 1; i <= 100; ++i) {
    tSimpleHashRemove(pHashObj, (const void *)&i, keyLen);
    ASSERT_EQ(100 - i, tSimpleHashGetSize(pHashObj));
  }

  tSimpleHashCleanup(pHashObj);
}

#pragma GCC diagnostic pop