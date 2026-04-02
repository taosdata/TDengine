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

#include "query.h"
#include "tmsg.h"
#include "trpc.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

namespace {
typedef struct SParam {
  int32_t v;
} SParam;
int32_t testPrint(void* p) {
  SParam* param = (SParam*)p;
  printf("hello world, %d\n", param->v);
  taosMemoryFreeClear(p);
  return 0;
}

int32_t testPrintError(void* p) {
  SParam* param = (SParam*)p;
  taosMemoryFreeClear(p);

  return -1;
}
}  // namespace

class QueryTestEnv : public testing::Environment {
 public:
  virtual void SetUp() { initTaskQueue(); }

  virtual void TearDown() { cleanupTaskQueue(); }

  QueryTestEnv() {}
  virtual ~QueryTestEnv() {}
};

int main(int argc, char** argv) {
  testing::AddGlobalTestEnvironment(new QueryTestEnv());
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, async_task_test) {
  SParam* p = (SParam*)taosMemoryCalloc(1, sizeof(SParam));
  taosAsyncExec(testPrint, p, NULL);
  taosMsleep(5);
}

TEST(testCase, many_async_task_test) {
  for (int32_t i = 0; i < 50; ++i) {
    SParam* p = (SParam*)taosMemoryCalloc(1, sizeof(SParam));
    p->v = i;
    taosAsyncExec(testPrint, p, NULL);
  }

  taosMsleep(10);
}

TEST(testCase, error_in_async_test) {
  int32_t code = 0;
  SParam* p = (SParam*)taosMemoryCalloc(1, sizeof(SParam));
  taosAsyncExec(testPrintError, p, &code);
  taosMsleep(1);
  printf("Error code:%d after asynchronously exec function\n", code);
}

#pragma GCC diagnostic pop