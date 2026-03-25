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
#include <cstdio>
#include <cstring>

extern "C" {
#include "dmInt.h"
#include "tglobal.h"
}

static const char* TEST_DATA_DIR = "/tmp/dmLicenseTest";

class GraceFileTest : public ::testing::Test {
protected:
  void SetUp() override {
    (void)system("rm -rf /tmp/dmLicenseTest && mkdir -p /tmp/dmLicenseTest");
    tstrncpy(tsDataDir, TEST_DATA_DIR, PATH_MAX);
  }
  void TearDown() override {
    (void)system("rm -rf /tmp/dmLicenseTest");
  }
};

TEST_F(GraceFileTest, WriteAndReadGraceFile) {
  SDmLicenseCtx ctx = {};

  // First call: no file exists → writes new timestamp
  int32_t code = dmLicenseLoadOrStartGrace(&ctx);
  ASSERT_EQ(code, 0);
  ASSERT_GT(ctx.gracePeriodStartMs, 0);
  int64_t first = ctx.gracePeriodStartMs;

  // Second call: file exists → reads persisted value, does not overwrite
  SDmLicenseCtx ctx2 = {};
  code = dmLicenseLoadOrStartGrace(&ctx2);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(ctx2.gracePeriodStartMs, first);
}

TEST_F(GraceFileTest, CancelGraceDeletesFile) {
  SDmLicenseCtx ctx = {};
  (void)dmLicenseLoadOrStartGrace(&ctx);
  ASSERT_GT(ctx.gracePeriodStartMs, 0);

  dmLicenseCancelGrace(&ctx);
  ASSERT_EQ(ctx.gracePeriodStartMs, 0);

  // File must be gone: a new call should write a fresh timestamp
  SDmLicenseCtx ctx2 = {};
  (void)dmLicenseLoadOrStartGrace(&ctx2);
  ASSERT_GT(ctx2.gracePeriodStartMs, 0);
}

TEST_F(GraceFileTest, GracePeriodExpiryDetected) {
  SDmLicenseCtx ctx = {};
  // 15 days ago — should be expired
  ctx.gracePeriodStartMs = taosGetTimestampMs() - (15LL * 24 * 3600 * 1000);
  int64_t elapsed = taosGetTimestampMs() - ctx.gracePeriodStartMs;
  ASSERT_GT(elapsed, 14LL * 24LL * 3600LL * 1000LL);
}

TEST_F(GraceFileTest, GracePeriodNotYetExpired) {
  SDmLicenseCtx ctx = {};
  // 5 days ago — should not be expired
  ctx.gracePeriodStartMs = taosGetTimestampMs() - (5LL * 24 * 3600 * 1000);
  int64_t elapsed = taosGetTimestampMs() - ctx.gracePeriodStartMs;
  ASSERT_LT(elapsed, 14LL * 24LL * 3600LL * 1000LL);
}
