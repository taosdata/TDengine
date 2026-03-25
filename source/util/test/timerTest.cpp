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
#ifdef WINDOWS
#include <windows.h>
int gettimeofday(struct timeval* tp, void* tzp) {
  FILETIME ft;
  unsigned __int64 tmpres = 0;
  static int tzflag = 0;

  if (NULL != tp) {
      GetSystemTimeAsFileTime(&ft);

      tmpres |= ft.dwHighDateTime;
      tmpres <<= 32;
      tmpres |= ft.dwLowDateTime;

      // Convert into microseconds
      tmpres /= 10;

      // Convert into seconds and microseconds
      tmpres -= 11644473600000000ULL;
      tp->tv_sec = (long)(tmpres / 1000000UL);
      tp->tv_usec = (long)(tmpres % 1000000UL);
  }
  return 0;
}
#else
#include <sys/time.h>
#endif
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <thread>

#include "os.h"
#include "tlog.h"
#include "ttimer.h"

// 调整系统时间的辅助函数，返回是否有权限
// adjust system time helper. On Windows we don't attempt to change system time here
// (would require elevated privileges and different API). Return false so tests
// that require changing system time will be skipped on Windows.
#if defined(_WIN32) || defined(_WIN64)
static bool adjustSystemTime(int64_t offsetMs) {
  // Try to enable SeSystemtimePrivilege to set system time
  HANDLE hToken = NULL;
  if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, &hToken)) {
    return false;
  }

  TOKEN_PRIVILEGES tp;
  LUID luid;
  if (!LookupPrivilegeValue(NULL, SE_SYSTEMTIME_NAME, &luid)) {
    CloseHandle(hToken);
    return false;
  }

  tp.PrivilegeCount = 1;
  tp.Privileges[0].Luid = luid;
  tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;

  if (!AdjustTokenPrivileges(hToken, FALSE, &tp, sizeof(tp), NULL, NULL)) {
    CloseHandle(hToken);
    return false;
  }

  if (GetLastError() != ERROR_SUCCESS) {
    // privilege not enabled
    CloseHandle(hToken);
    return false;
  }

  // Get current system time (UTC)
  SYSTEMTIME stUtc;
  GetSystemTime(&stUtc);

  // Convert SYSTEMTIME -> FILETIME -> 64-bit value
  FILETIME ft;
  if (!SystemTimeToFileTime(&stUtc, &ft)) {
    // revoke privilege
    tp.Privileges[0].Attributes = 0;
    AdjustTokenPrivileges(hToken, FALSE, &tp, sizeof(tp), NULL, NULL);
    CloseHandle(hToken);
    return false;
  }

  unsigned __int64 time100ns = ((unsigned __int64)ft.dwHighDateTime << 32) | ft.dwLowDateTime;
  // offsetMs to 100-ns units
  unsigned __int64 delta100ns = (unsigned __int64)offsetMs * 10000ULL;
  time100ns += delta100ns;

  FILETIME ftNew;
  ftNew.dwLowDateTime = (DWORD)(time100ns & 0xFFFFFFFF);
  ftNew.dwHighDateTime = (DWORD)(time100ns >> 32);

  SYSTEMTIME stNew;
  if (!FileTimeToSystemTime(&ftNew, &stNew)) {
    tp.Privileges[0].Attributes = 0;
    AdjustTokenPrivileges(hToken, FALSE, &tp, sizeof(tp), NULL, NULL);
    CloseHandle(hToken);
    return false;
  }

  // Set system time (UTC)
  BOOL ok = SetSystemTime(&stNew);

  // revoke privilege
  tp.Privileges[0].Attributes = 0;
  AdjustTokenPrivileges(hToken, FALSE, &tp, sizeof(tp), NULL, NULL);
  CloseHandle(hToken);

  return ok != 0;
}
#else
static bool adjustSystemTime(int64_t offsetMs) {
  struct timeval tv;
  if (gettimeofday(&tv, NULL) != 0) return false;

  int64_t us = (int64_t)tv.tv_sec * 1000000LL + tv.tv_usec + offsetMs * 1000;
  tv.tv_sec  = us / 1000000;
  tv.tv_usec = us % 1000000;

  return settimeofday(&tv, NULL) == 0;
}
#endif

// ============================================================
// 测试 1：系统时间向前跳（+1 小时），timer 应在预期时间内触发
// ============================================================
TEST(timerTest, TimerCorrectWhenTimeJumpForward) {
  void* ctrl = taosTmrInit(100, 10, 10000, "timerTest");
  ASSERT_NE(ctrl, nullptr);

  struct WaitFlag {
    std::mutex m;
    std::condition_variable cv;
    bool fired{false};
    int64_t fireMonoMs{0};
  } waitFlag;
  int64_t startMonoMs = taosGetMonotonicMs();
  const int32_t     delayMs = 500;

  // 启动一个 500ms 后触发的 timer
  taosTmrStart(
      [](void* p, tmr_h id) {
        auto* wf = static_cast<WaitFlag*>(p);
        {
          std::lock_guard<std::mutex> lg(wf->m);
          wf->fireMonoMs = taosGetMonotonicMs();
          wf->fired = true;
        }
        wf->cv.notify_one();
        GTEST_LOG_(INFO) << "timer 触发";
      },
      delayMs, &waitFlag, ctrl);

  // timer 启动后立即将系统时间向前拨 1 小时
  bool hasPrivilege = adjustSystemTime(3600LL * 1000);
  if (!hasPrivilege) {
    GTEST_LOG_(WARNING) << "adjustSystemTime failed (no privilege?) - continuing test anyway";
  }

  // 等待最多 3 秒（轮询以兼容老旧编译器）
  int waited = 0;
  bool signaled = false;
  while (waited < 3000) {
    {
      std::lock_guard<std::mutex> lg(waitFlag.m);
      if (waitFlag.fired) {
        signaled = true;
        break;
      }
    }
    taosMsleep(10);
    waited += 10;
  }
  if (!signaled) {
    GTEST_FAIL() << "timer 未触发";
  }
  if (signaled) {
    int64_t elapsed = waitFlag.fireMonoMs - startMonoMs;
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

  struct WaitFlag2 {
    std::mutex m;
    std::condition_variable cv;
    bool fired{false};
    int64_t fireMonoMs{0};
  } waitFlag2;
  int64_t startMonoMs = taosGetMonotonicMs();
  const int32_t     delayMs     = 500;

  taosTmrStart(
      [](void* p, tmr_h id) {
        auto* wf = static_cast<WaitFlag2*>(p);
        {
          std::lock_guard<std::mutex> lg(wf->m);
          wf->fireMonoMs = taosGetMonotonicMs();
          wf->fired = true;
        }
        wf->cv.notify_one();
        GTEST_LOG_(INFO) << "timer 触发";
      },
      delayMs, &waitFlag2, ctrl);

  // timer 启动后立即将系统时间向后拨 1 小时
  bool hasPrivilege = adjustSystemTime(-3600LL * 1000);
  if (!hasPrivilege) {
    GTEST_LOG_(WARNING) << "adjustSystemTime failed (no privilege?) - continuing test anyway";
  }

  // 等待最多 3 秒（轮询以兼容老旧编译器）
  int waited2 = 0;
  bool signaled2 = false;
  while (waited2 < 3000) {
    {
      std::lock_guard<std::mutex> lg(waitFlag2.m);
      if (waitFlag2.fired) {
        signaled2 = true;
        break;
      }
    }
    taosMsleep(10);
    waited2 += 10;
  }
  if (!signaled2) {
    GTEST_FAIL() << "timer 未触发";
  }
  if (signaled2) {
    int64_t elapsed = waitFlag2.fireMonoMs - startMonoMs;
    GTEST_LOG_(INFO) << "timer 触发，elapsed=" << elapsed << "ms";
  }

  // 时间还原
  adjustSystemTime(3600LL * 1000);
  taosTmrCleanUp(ctrl);
}
