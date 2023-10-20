/*
 * Copyright (c) 2019 TAOS Data, Inc. <xsren@taosdata.com>
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
#include <thread>
#include "os.h"
#include "tlog.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"

TEST(osSemaphoreTests, InitAndDestroy) {
  tsem_t sem;
  int    result = tsem_init(&sem, 0, 1);
  EXPECT_EQ(result, 0);

  result = tsem_destroy(&sem);
  EXPECT_EQ(result, 0);
}

TEST(osSemaphoreTests, Destroy) {
  tsem_t sem;
  int    result = tsem_init(&sem, 0, 1);
  EXPECT_EQ(result, 0);

  result = tsem_destroy(&sem);
  EXPECT_EQ(result, 0);
  // result = tsem_destroy(&sem);
  // EXPECT_NE(result, 0);  // result == 0 if on mac
}

// skip, tsem_wait can not stopped, will block test.
// TEST(osSemaphoreTests, Wait) {
//   tsem_t sem;
//   tsem_init(&sem, 0, 0);
//   ASSERT_EQ(tsem_wait(&sem), -1);
//   tsem_destroy(&sem);
// }

TEST(osSemaphoreTests, WaitTime0) {
  tsem_t sem;
  tsem_init(&sem, 0, 0);
  EXPECT_NE(tsem_timewait(&sem, 1000), 0);
  tsem_destroy(&sem);
}

TEST(osSemaphoreTests, WaitTime1) {
  tsem_t sem;
  tsem_init(&sem, 0, 1);
  EXPECT_EQ(tsem_timewait(&sem, 1000), 0);
  EXPECT_NE(tsem_timewait(&sem, 1000), 0);
  tsem_destroy(&sem);
}


TEST(osSemaphoreTests, WaitAndPost) {
  tsem_t sem;
  int    result = tsem_init(&sem, 0, 0);
  EXPECT_EQ(result, 0);

  std::thread([&sem]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    tsem_post(&sem);
  }).detach();

  result = tsem_wait(&sem);
  EXPECT_EQ(result, 0);

  result = tsem_destroy(&sem);
  EXPECT_EQ(result, 0);
}


TEST(osSemaphoreTests, TimedWait) {
  tsem_t sem;
  int    result = tsem_init(&sem, 0, 0);
  EXPECT_EQ(result, 0);

  std::thread([&sem]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    tsem_post(&sem);
  }).detach();

  result = tsem_timewait(&sem, 1000);
  EXPECT_EQ(result, 0);

  result = tsem_destroy(&sem);
  EXPECT_EQ(result, 0);
}
