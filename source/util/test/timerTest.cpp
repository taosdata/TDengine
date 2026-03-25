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

 #include <iostream>
#define ALLOW_FORBID_FUNC
#include <gtest/gtest.h>
#include <sys/time.h>
#include <atomic>

#include "os.h"
#include "tlog.h"
#include "ttimer.h"

// 调整系统时间的辅助函数，返回是否有权限
static bool adjustSystemTime(int64_t offsetMs) {
  struct timeval tv;
  if (gettimeofday(&tv, NULL) != 0) return false;

  int64_t us = (int64_t)tv.tv_sec * 1000000LL + tv.tv_usec + offsetMs * 1000;
  tv.tv_sec  = us / 1000000;
  tv.tv_usec = us % 1000000;

  return settimeofday(&tv, NULL) == 0;
}

// ============================================================
// 测试 1：系统时间向前跳（+1 小时），timer 应在预期时间内触发
// ============================================================
TEST(timerTest, TimerCorrectWhenTimeJumpForward) {
  void* ctrl = taosTmrInit(100, 10, 10000, "timerTest");
  ASSERT_NE(ctrl, nullptr);

  std::atomic<bool> fired{false};
  int64_t           fireMonoMs = 0;
  int64_t           startMonoMs = taosGetMonotonicMs();
  const int32_t     delayMs = 500;

  // 启动一个 500ms 后触发的 timer
  auto param = std::make_pair(&fired, &fireMonoMs);
  taosTmrStart(
      [](void* p, tmr_h id) {
        auto* pair    = static_cast<std::pair<std::atomic<bool>*, int64_t*>*>(p);
        *pair->second = taosGetMonotonicMs();
        pair->first->store(true);
        GTEST_LOG_(INFO) << "timer 触发";
      },
      delayMs, &param, ctrl);

  // timer 启动后立即将系统时间向前拨 1 小时
  bool hasPrivilege = adjustSystemTime(3600LL * 1000);
  if (!hasPrivilege) {
    taosTmrCleanUp(ctrl);
    GTEST_SKIP() << "需要 root 权限修改系统时间，跳过此测试";
  }

  // 等待最多 3 秒
  int waited = 0;
  while (!fired.load() && waited < 3000) {
    taosMsleep(10);
    waited += 10;
  }

  EXPECT_TRUE(fired.load()) << "timer 未触发";

  if (fired.load()) {
    int64_t elapsed = fireMonoMs - startMonoMs;
    GTEST_LOG_(INFO) << "timer 触发，elapsed=" << elapsed << "ms";
  }

  // 时间还原
  adjustSystemTime(-3600LL * 1000);

  taosTmrCleanUp(ctrl);
}

// ============================================================
// 测试 2：系统时间向后跳（-1 小时），timer 应在预期时间内触发
// ============================================================
TEST(timerTest, TimerCorrectWhenTimeJumpBackward) {
  void* ctrl = taosTmrInit(100, 10, 10000, "timerTest");
  ASSERT_NE(ctrl, nullptr);

  std::atomic<bool> fired{false};
  int64_t           fireMonoMs  = 0;
  int64_t           startMonoMs = taosGetMonotonicMs();
  const int32_t     delayMs     = 500;

  auto param = std::make_pair(&fired, &fireMonoMs);
  taosTmrStart(
      [](void* p, tmr_h id) {
        auto* pair    = static_cast<std::pair<std::atomic<bool>*, int64_t*>*>(p);
        *pair->second = taosGetMonotonicMs();
        pair->first->store(true);
        GTEST_LOG_(INFO) << "timer 触发";
      },
      delayMs, &param, ctrl);

  // timer 启动后立即将系统时间向后拨 1 小时
  bool hasPrivilege = adjustSystemTime(-3600LL * 1000);
  if (!hasPrivilege) {
    taosTmrCleanUp(ctrl);
    GTEST_SKIP() << "需要 root 权限修改系统时间，跳过此测试";
  }

  // 等待最多 3 秒
  int waited = 0;
  while (!fired.load() && waited < 3000) {
    taosMsleep(10);
    waited += 10;
  }

  EXPECT_TRUE(fired.load()) << "timer 未触发";

  if (fired.load()) {
    int64_t elapsed = fireMonoMs - startMonoMs;
    GTEST_LOG_(INFO) << "timer 触发，elapsed=" << elapsed << "ms";
  }

  // 时间还原
  adjustSystemTime(3600LL * 1000);
  taosTmrCleanUp(ctrl);
}
