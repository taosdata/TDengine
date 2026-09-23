/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include <array>
#include <atomic>
#include <string>
#include <thread>

#include "osDef.h"
#include "taoserror.h"
#include "tconv.h"

TEST(taosConvInit, rejectsOverlongCharsetName) {
  // TD_CHARSET_LEN includes the NUL, so a name of exactly this length has
  // no room for a terminator and must be rejected outright rather than
  // silently truncated into a possibly-different, unintended charset.
  std::string longName(TD_CHARSET_LEN, 'A');
  terrno = TSDB_CODE_SUCCESS;
  void* cxt = taosConvInit(longName.c_str());
  EXPECT_EQ(nullptr, cxt);
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, terrno);
}

TEST(taosConvInit, acceptsCharsetNameUnderLimit) {
  // A normal supported name under the limit must resolve successfully.
  terrno = TSDB_CODE_SUCCESS;
  void* cxt = taosConvInit("UTF-8");
  EXPECT_NE(nullptr, cxt);
}

TEST(taosConvInit, boundaryLengthPassesGuardThenRejectedByIconv) {
  // A 63-byte name still fits with its terminator and must reach iconv rather
  // than being rejected by the length guard.
  std::string boundaryName(TD_CHARSET_LEN - 1, 'A');
  terrno = TSDB_CODE_SUCCESS;
  void* cxt = taosConvInit(boundaryName.c_str());
  EXPECT_EQ(nullptr, cxt);
  EXPECT_NE(TSDB_CODE_SUCCESS, terrno);
  EXPECT_NE(TSDB_CODE_INVALID_PARA, terrno);
}

TEST(taosConvInit, rejectsUnknownShortCharsetNameWithoutCrashing) {
  // Regression probe: any name iconv doesn't recognize must fail cleanly.
  // (A prior bug in the cleanup path segfaulted here -- see
  // taosConvDestroyInner.)
  terrno = TSDB_CODE_SUCCESS;
  void* cxt = taosConvInit("NOT-A-REAL-CHARSET");
  EXPECT_EQ(nullptr, cxt);
  EXPECT_NE(TSDB_CODE_SUCCESS, terrno);
}

class TaosConvCacheTest : public testing::Test {
 protected:
  void SetUp() override { taosConvDestroy(); }
  void TearDown() override { taosConvDestroy(); }
};

TEST_F(TaosConvCacheTest, caseVariantsShareOneEntry) {
  void* lower = taosConvInit("utf-8");
  ASSERT_NE(nullptr, lower);
  EXPECT_EQ(lower, taosConvInit("UTF-8"));
}

TEST_F(TaosConvCacheTest, destroyAllowsCacheReinitialization) {
  ASSERT_NE(nullptr, taosConvInit("UTF-8"));

  taosConvDestroy();

  EXPECT_NE(nullptr, taosConvInit("UTF-8"));
}

TEST_F(TaosConvCacheTest, concurrentFirstInitSharesOneEntry) {
  constexpr int32_t kThreads = 8;
  std::array<void*, kThreads> contexts = {nullptr};
  std::array<std::thread, kThreads> threads;
  std::atomic<int32_t> ready{0};
  std::atomic<bool> start{false};

  for (int32_t i = 0; i < kThreads; ++i) {
    threads[i] = std::thread([&, i] {
      ready.fetch_add(1, std::memory_order_release);
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      contexts[i] = taosConvInit("UTF-16LE");
    });
  }
  while (ready.load(std::memory_order_acquire) != kThreads) {
    std::this_thread::yield();
  }
  start.store(true, std::memory_order_release);
  for (std::thread& thread : threads) {
    thread.join();
  }

  ASSERT_NE(nullptr, contexts[0]);
  for (int32_t i = 1; i < kThreads; ++i) {
    EXPECT_EQ(contexts[0], contexts[i]);
  }
}

TEST_F(TaosConvCacheTest, rejectsNewCharsetAfterTenEntries) {
  const char* candidates[] = {
      "UTF-8",      "UTF8",       "GBK",       "GB18030",
      "BIG5",       "ISO-8859-1", "ISO-8859-2", "ISO-8859-15",
      "UTF-16LE",   "UTF-16BE",   "UTF-32LE",  "UTF-32BE",
      "CP1252",     "SHIFT_JIS",  "EUC-JP",
  };
  int32_t cached = 0;
  bool    full = false;

  for (const char* charset : candidates) {
    terrno = TSDB_CODE_SUCCESS;
    void* cxt = taosConvInit(charset);
    if (cxt != nullptr) {
      ++cached;
    } else if (terrno == TSDB_CODE_CHARSET_CONVERT_CACHE_FULL) {
      full = true;
      break;
    }
  }

  ASSERT_TRUE(full);
  EXPECT_EQ(10, cached);
  EXPECT_NE(nullptr, taosConvInit("UTF-8"));
  EXPECT_STREQ("Charset conversion cache is full",
               tstrerror(TSDB_CODE_CHARSET_CONVERT_CACHE_FULL));
}
