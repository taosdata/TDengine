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

#ifdef WINDOWS
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0602  // GetSystemTimePreciseAsFileTime requires Windows 8.
#endif
#endif

#include <gtest/gtest.h>
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"

#include "os.h"
#include "tlog.h"
#include "ttime.h"

#ifdef __linux__
TEST(osTimeTests, monotonicClockIdUsesCurrentNamespace) {
  char namespaceId[32] = {0};
  ssize_t length = readlink("/proc/self/ns/time", namespaceId, sizeof(namespaceId) - 1);
  if (length < 0 && errno == ENOENT) GTEST_SKIP();
  ASSERT_GT(length, 0);
  namespaceId[length] = 0;

  char clockId[80] = {0};
  ASSERT_EQ(taosGetMonotonicClockId(clockId, sizeof(clockId)), 0);
  EXPECT_NE(strstr(clockId, namespaceId), nullptr);
}
#endif

// Compare every realtime entry point with an independent system clock.
TEST(osTimeTests, realtimeReturnsUnixTime) {
  auto systemTimeNs = []() -> int64_t {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  };
  int64_t         before = systemTimeNs();
  struct timespec ts = {0};
  ASSERT_EQ(taosClockGetTime(CLOCK_REALTIME, &ts), 0);
  struct timeval tv = {0};
  ASSERT_EQ(taosGetTimeOfDay(&tv), 0);
  time_t t = 0;
  ASSERT_EQ(taosTime(&t), 0);
  const struct {
    const char *name;
    int64_t     value;
    int64_t     nsPerUnit;
  } actual[] = {
      {"taosGetTimestampSec", taosGetTimestampSec(), NANOSECOND_PER_SEC},
      {"taosTime", (int64_t)t, NANOSECOND_PER_SEC},
      {"taosGetTimestampMs", taosGetTimestampMs(), NANOSECOND_PER_MSEC},
      {"taosGetTimestampUs", taosGetTimestampUs(), NANOSECOND_PER_USEC},
      {"taosGetTimeOfDay",
       (int64_t)tv.tv_sec * MICROSECOND_PER_SECOND + tv.tv_usec,
       NANOSECOND_PER_USEC},
      {"taosGetTimestampNs", taosGetTimestampNs(), 1},
      {"taosClockGetTime(CLOCK_REALTIME)",
       (int64_t)ts.tv_sec * NANOSECOND_PER_SEC + ts.tv_nsec, 1},
  };
  int64_t after = systemTimeNs();

  EXPECT_GE(ts.tv_nsec, 0);
  EXPECT_LT(ts.tv_nsec, NANOSECOND_PER_SEC);
  EXPECT_GE(tv.tv_usec, 0);
  EXPECT_LT(tv.tv_usec, MICROSECOND_PER_SECOND);
  const int64_t realtimeMargin = NANOSECOND_PER_MSEC;
  for (size_t i = 0; i < sizeof(actual) / sizeof(actual[0]); ++i) {
    SCOPED_TRACE(actual[i].name);
    // time()-based second entries may lag by a coarse tick across a second
    // boundary.
    const int64_t margin = actual[i].nsPerUnit == NANOSECOND_PER_SEC
                               ? NANOSECOND_PER_SEC
                               : realtimeMargin;
    EXPECT_GE(actual[i].value, (before - margin) / actual[i].nsPerUnit);
    EXPECT_LE(actual[i].value, (after + margin) / actual[i].nsPerUnit);
  }
}

#ifdef WINDOWS
TEST(osTimeTests, realtimeUsesPreciseSystemClock) {
  auto filetimeNs = [](const FILETIME &fileTime) -> int64_t {
    ULARGE_INTEGER ticks = {0};
    ticks.LowPart = fileTime.dwLowDateTime;
    ticks.HighPart = fileTime.dwHighDateTime;
    return static_cast<int64_t>((ticks.QuadPart - 116444736000000000ULL) * 100);
  };

  // Wait for a point inside one coarse clock tick. At that point the old
  // GetSystemTimeAsFileTime implementation must fall before the precise clock.
  const uint64_t deadline = GetTickCount64() + 5000;
  int64_t maximumOffsetNs = 0;
  int32_t checked = 0;
  do {
    FILETIME coarseBefore = {0}, coarseAfter = {0};
    FILETIME preciseBefore = {0}, preciseAfter = {0};
    struct timespec actual = {0};
    GetSystemTimeAsFileTime(&coarseBefore);
    GetSystemTimePreciseAsFileTime(&preciseBefore);
    ASSERT_EQ(taosClockGetTime(CLOCK_REALTIME, &actual), 0);
    GetSystemTimePreciseAsFileTime(&preciseAfter);
    GetSystemTimeAsFileTime(&coarseAfter);

    int64_t offsetNs = filetimeNs(preciseBefore) - filetimeNs(coarseBefore);
    if (offsetNs > maximumOffsetNs) maximumOffsetNs = offsetNs;
    if (filetimeNs(coarseBefore) != filetimeNs(coarseAfter) ||
        filetimeNs(preciseBefore) > filetimeNs(coarseAfter) ||
        offsetNs < 100 * NANOSECOND_PER_USEC) {
      continue;
    }
    int64_t actualNs = static_cast<int64_t>(actual.tv_sec) * NANOSECOND_PER_SEC + actual.tv_nsec;
    EXPECT_GE(actualNs, filetimeNs(preciseBefore));
    EXPECT_LE(actualNs, filetimeNs(preciseAfter));
    ++checked;
  } while (checked < 3 && GetTickCount64() < deadline);
  if (checked == 0) {
    GTEST_SKIP() << "No stable coarse clock tick with 100us offset; maximum observed offset: "
                 << maximumOffsetNs << " ns";
  }
}

TEST(osTimeTests, monotonicUsesPerformanceCounter) {
  LARGE_INTEGER frequency, before, after;
  ASSERT_TRUE(QueryPerformanceFrequency(&frequency));
  ASSERT_GT(frequency.QuadPart, 0);
  ASSERT_TRUE(QueryPerformanceCounter(&before));
  struct timespec actual = {0};
  ASSERT_EQ(taosClockGetTime(CLOCK_MONOTONIC, &actual), 0);
  ASSERT_TRUE(QueryPerformanceCounter(&after));

  auto toMicroseconds = [&frequency](LARGE_INTEGER ticks) -> int64_t {
    return ticks.QuadPart / frequency.QuadPart * MICROSECOND_PER_SECOND +
           ticks.QuadPart % frequency.QuadPart * MICROSECOND_PER_SECOND /
               frequency.QuadPart;
  };
  EXPECT_GE(actual.tv_nsec, 0);
  EXPECT_LT(actual.tv_nsec, NANOSECOND_PER_SEC);
  int64_t actualUs = (int64_t)actual.tv_sec * MICROSECOND_PER_SECOND +
                     actual.tv_nsec / NANOSECOND_PER_USEC;
  EXPECT_GE(actualUs, toMicroseconds(before));
  EXPECT_LE(actualUs, toMicroseconds(after));
}
#endif

TEST(osTimeTests, taosLocalTime) {
  // Test 1: Test when both timep and result are not NULL
  time_t     timep = 1617531000;  // 2021-04-04 18:10:00
  struct tm  result;
  struct tm *local_time = taosLocalTime(&timep, &result, NULL, 0, NULL);
  ASSERT_NE(local_time, nullptr);
  ASSERT_EQ(local_time->tm_year, 121);
  ASSERT_EQ(local_time->tm_mon, 3);
  ASSERT_EQ(local_time->tm_mday, 4);
  ASSERT_EQ(local_time->tm_hour, 18);
  ASSERT_EQ(local_time->tm_min, 10);
  ASSERT_EQ(local_time->tm_sec, 00);

  // Test 2: Test when timep is NULL
  local_time = taosLocalTime(NULL, &result, NULL, 0, NULL);
  ASSERT_EQ(local_time, nullptr);

  // Test 4: Test when timep is negative on Windows
#ifdef WINDOWS
  time_t pos_timep = 1609459200;  // 2021-01-01 08:00:00
  local_time = taosLocalTime(&pos_timep, &result, NULL, 0, NULL);
  ASSERT_NE(local_time, nullptr);
  ASSERT_EQ(local_time->tm_year, 121);
  ASSERT_EQ(local_time->tm_mon, 0);
  ASSERT_EQ(local_time->tm_mday, 1);
  ASSERT_EQ(local_time->tm_hour, 8);
  ASSERT_EQ(local_time->tm_min, 0);
  ASSERT_EQ(local_time->tm_sec, 0);

  time_t neg_timep = -1617531000;  // 1918-09-29 21:50:00
  local_time = taosLocalTime(&neg_timep, &result, NULL, 0, NULL);
  ASSERT_NE(local_time, nullptr);
  ASSERT_EQ(local_time->tm_year, 18);
  ASSERT_EQ(local_time->tm_mon, 8);
  ASSERT_EQ(local_time->tm_mday, 29);
  ASSERT_EQ(local_time->tm_hour, 21);
  ASSERT_EQ(local_time->tm_min, 50);
  ASSERT_EQ(local_time->tm_sec, 0);

  time_t neg_timep2 = -315619200;  // 1960-01-01 08:00:00
  local_time = taosLocalTime(&neg_timep2, &result, NULL, 0, NULL);
  ASSERT_NE(local_time, nullptr);
  ASSERT_EQ(local_time->tm_year, 60);
  ASSERT_EQ(local_time->tm_mon, 0);
  ASSERT_EQ(local_time->tm_mday, 1);
  ASSERT_EQ(local_time->tm_hour, 8);
  ASSERT_EQ(local_time->tm_min, 0);
  ASSERT_EQ(local_time->tm_sec, 0);

  time_t zero_timep = 0;  // 1970-01-01 08:00:00
  local_time = taosLocalTime(&zero_timep, &result, NULL, 0, NULL);
  ASSERT_NE(local_time, nullptr);
  ASSERT_EQ(local_time->tm_year, 70);
  ASSERT_EQ(local_time->tm_mon, 0);
  ASSERT_EQ(local_time->tm_mday, 1);
  ASSERT_EQ(local_time->tm_hour, 8);
  ASSERT_EQ(local_time->tm_min, 0);
  ASSERT_EQ(local_time->tm_sec, 0);

  time_t neg_timep3 = -78115158887;
  local_time = taosLocalTime(&neg_timep3, &result, NULL, 0, NULL);
  ASSERT_NE(local_time, nullptr);
  EXPECT_EQ(taosMktime(&result, NULL), neg_timep3);
#endif
}

TEST(osTimeTests, taosGmTimeR) {
  // Test 1: Test when both timep and result are not NULL
  time_t     timep = 1617531000;  // 2021-04-04 18:10:00
  struct tm  tmInfo;
  ASSERT_NE(taosGmTimeR(&timep, &tmInfo), nullptr);

  char buf[128];
  taosStrfTime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tmInfo);
  ASSERT_STREQ(buf, "2021-04-04T10:10:00");
}

TEST(osTimeTests, taosTimeGm) {
  char *timestr= "2021-04-04T18:10:00";
  struct tm tm = {0};

  taosStrpTime(timestr, "%Y-%m-%dT%H:%M:%S", &tm);
  int64_t seconds = taosTimeGm(&tm);
  ASSERT_EQ(seconds, 1617559800);
}

TEST(osTimeTests, taosMktime) {
  char *timestr= "2021-04-04T18:10:00";
  struct tm tm = {0};

  taosStrpTime(timestr, "%Y-%m-%dT%H:%M:%S", &tm);
  time_t seconds = taosMktime(&tm, NULL);
  ASSERT_EQ(seconds, 1617531000);
}

#ifdef WINDOWS
extern "C" long timezone;

class WindowsTimezoneGuard {
 public:
  WindowsTimezoneGuard()
      : host_(saveEnvironment("TZ")),
        internal_(saveEnvironment(TAOS_INTERNAL_TZ_ENV)),
        offset_(timezone) {
    memcpy(display_, tsTimezoneStr, sizeof(display_));
  }

  ~WindowsTimezoneGuard() {
    const char *host = host_.empty() ? nullptr : host_.data();
    const char *internal = internal_.empty() ? nullptr : internal_.data();
    EXPECT_TRUE(SetEnvironmentVariableA("TZ", host));
    EXPECT_TRUE(SetEnvironmentVariableA(TAOS_INTERNAL_TZ_ENV, internal));
    cleanupTimezoneInfo();
    memcpy(tsTimezoneStr, display_, sizeof(display_));
    timezone = offset_;
  }

 private:
  static std::vector<char> saveEnvironment(const char *name) {
    DWORD size = GetEnvironmentVariableA(name, nullptr, 0);
    std::vector<char> value(size);
    if (size != 0) {
      EXPECT_EQ(GetEnvironmentVariableA(name, value.data(), size), size - 1);
    }
    return value;
  }

  std::vector<char> host_;
  std::vector<char> internal_;
  long offset_;
  char display_[TD_TIMEZONE_LEN] = {0};
};

static void checkWindowsTimezoneOffset(int64_t expected) {
  int32_t code = TSDB_CODE_TIME_ERROR;
  int32_t offset = taosGetTZOffsetSeconds(NULL, &code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(offset, expected);
}

static void checkWindowsTimezoneDisplay() {
  char name[TD_TIMEZONE_LEN] = {0};
  tstrncpy(name, tsTimezoneStr, sizeof(name));
  truncateTimezoneString(name);
  char normalized[TD_TIMEZONE_LEN] = {0};
  EXPECT_EQ(taosValidateAndNormalizeTimezone(name, normalized,
                                           sizeof(normalized), NULL),
            TSDB_CODE_SUCCESS);

  // The display string carries the zone's own abbreviation
  // ("China Standard Time (CST, +0800)"); only the offset suffix is fixed.
  const char *suffix = strrchr(tsTimezoneStr, ',');
  ASSERT_NE(suffix, nullptr);
  char sign = '\0';
  int hours = 0;
  int minutes = 0;
  ASSERT_EQ(sscanf(suffix, ", %c%2d%2d)", &sign, &hours, &minutes), 3);
  ASSERT_TRUE(sign == '+' || sign == '-');
  ASSERT_LT(hours, 24);
  ASSERT_LT(minutes, 60);
  int64_t displayOffset = (hours * 60 + minutes) * 60;
  if (sign == '-') displayOffset = -displayOffset;
  checkWindowsTimezoneOffset(displayOffset);
}

TEST(osTimeTests, windowsGlobalTimezoneOffset) {
  WindowsTimezoneGuard guard;
  ASSERT_EQ(taosSetGlobalTimezone("UTC-8"), 0);
  checkWindowsTimezoneOffset(TdEastZone8);
  int32_t code = 0;
  ASSERT_EQ(taosGetLocalTimezoneOffset(&code), TdEastZone8);

  ASSERT_EQ(taosSetGlobalTimezone("UTC"), 0);
  checkWindowsTimezoneOffset(0);
  ASSERT_EQ(taosGetLocalTimezoneOffset(&code), 0);
}

TEST(osTimeTests, windowsInitTimezoneKeepsConfiguredTZ) {
  WindowsTimezoneGuard guard;
  // Simulate user config timezone = UTC before initTimezoneInfo() runs.
  ASSERT_EQ(taosSetGlobalTimezone("UTC"), 0);
  checkWindowsTimezoneOffset(0);

  ASSERT_EQ(initTimezoneInfo(), TSDB_CODE_SUCCESS);
  checkWindowsTimezoneOffset(0);
  int32_t code = 0;
  ASSERT_EQ(taosGetLocalTimezoneOffset(&code), 0);

  // 1602172800 is 2020-10-08 16:00:00 UTC.
  time_t ts = 1602172800;
  struct tm tmVal;
  ASSERT_NE(taosLocalTime(&ts, &tmVal, NULL, 0, NULL), nullptr);
  ASSERT_EQ(tmVal.tm_year + 1900, 2020);
  ASSERT_EQ(tmVal.tm_mon + 1, 10);
  ASSERT_EQ(tmVal.tm_mday, 8);
  ASSERT_EQ(tmVal.tm_hour, 16);
  ASSERT_EQ(tmVal.tm_min, 0);
  ASSERT_EQ(tmVal.tm_sec, 0);
}

TEST(osTimeTests, windowsInitTimezoneFromSystemZone) {
  WindowsTimezoneGuard guard;
  ASSERT_TRUE(SetEnvironmentVariableA(TAOS_INTERNAL_TZ_ENV, NULL));
  ASSERT_TRUE(SetEnvironmentVariableA("TZ", NULL));

  // Match startup: osDefaultInit resolves the name before config takes a copy.
  ASSERT_EQ(taosGetSystemTimezone(tsTimezoneStr), TSDB_CODE_SUCCESS);
  ASSERT_NE(tsTimezoneStr[0], '\0');
  char systemTimezone[TD_TIMEZONE_LEN] = {0};
  tstrncpy(systemTimezone, tsTimezoneStr, sizeof(systemTimezone));
  ASSERT_EQ(initTimezoneInfo(), TSDB_CODE_SUCCESS);
  truncateTimezoneString(systemTimezone);
  char resolvedName[TD_TIMEZONE_LEN] = {0};
  tstrncpy(resolvedName, tsTimezoneStr, sizeof(resolvedName));
  truncateTimezoneString(resolvedName);
  EXPECT_STREQ(resolvedName, systemTimezone);
  checkWindowsTimezoneDisplay();

  char env[TD_TIMEZONE_LEN * 2] = {0};
  EXPECT_EQ(GetEnvironmentVariableA("TZ", env, sizeof(env)), 0u);
  ASSERT_GT(GetEnvironmentVariableA(TAOS_INTERNAL_TZ_ENV, env, sizeof(env)),
             0u);
  timezone_t snapshot = tzalloc(env);
  ASSERT_NE(snapshot, nullptr);
  time_t stamp = 678369600;
  struct tm cached = {}, explicitZone = {};
  ASSERT_NE(taosLocalTime(&stamp, &cached, NULL, 0, NULL), nullptr);
  ASSERT_NE(taosLocalTime(&stamp, &explicitZone, NULL, 0, snapshot), nullptr);
  EXPECT_EQ(cached.tm_hour, explicitZone.tm_hour);
  tzfree(snapshot);
  int32_t code = 0;
  int32_t offset = taosGetLocalTimezoneOffset(&code);
  EXPECT_EQ(code, 0);
  checkWindowsTimezoneOffset(offset);
}

TEST(osTimeTests, windowsInitTimezoneWithoutResolvedName) {
  WindowsTimezoneGuard guard;
  ASSERT_TRUE(SetEnvironmentVariableA(TAOS_INTERNAL_TZ_ENV, NULL));
  ASSERT_TRUE(SetEnvironmentVariableA("TZ", NULL));
  tsTimezoneStr[0] = '\0';

  ASSERT_EQ(initTimezoneInfo(), TSDB_CODE_SUCCESS);

  char env[TD_TIMEZONE_LEN * 2] = {0};
  EXPECT_EQ(GetEnvironmentVariableA("TZ", env, sizeof(env)), 0u);
  ASSERT_GT(GetEnvironmentVariableA(TAOS_INTERNAL_TZ_ENV, env, sizeof(env)),
            0u);
  char resolvedName[TD_TIMEZONE_LEN] = {0};
  tstrncpy(resolvedName, tsTimezoneStr, sizeof(resolvedName));
  truncateTimezoneString(resolvedName);
  EXPECT_STREQ(resolvedName, env);
  checkWindowsTimezoneDisplay();
}

TEST(osTimeTests, windowsInitTimezoneRefreshesDisplayOffset) {
  WindowsTimezoneGuard guard;
  ASSERT_TRUE(SetEnvironmentVariableA("TZ", NULL));
  ASSERT_EQ(taosSetGlobalTimezone("America/New_York"), 0);
  // Model the stale registry Display suffix. Deliberately use an offset
  // outside current Windows system zones to exercise refresh on every host.
  tstrncpy(tsTimezoneStr, "America/New_York (UTC, -2359)", TD_TIMEZONE_LEN);
  ASSERT_EQ(initTimezoneInfo(), TSDB_CODE_SUCCESS);
  char resolvedName[TD_TIMEZONE_LEN] = {0};
  tstrncpy(resolvedName, tsTimezoneStr, sizeof(resolvedName));
  truncateTimezoneString(resolvedName);
  EXPECT_STREQ(resolvedName, "America/New_York");
  checkWindowsTimezoneDisplay();
}

TEST(osTimeTests, windowsOffsetFallbackWhenTZUnset) {
  WindowsTimezoneGuard guard;
  // Simulate conversion before explicit initialization.
  ASSERT_TRUE(SetEnvironmentVariableA(TAOS_INTERNAL_TZ_ENV, NULL));
  ASSERT_TRUE(SetEnvironmentVariableA("TZ", NULL));

  TIME_ZONE_INFORMATION tzi = {0};
  DWORD tzType = GetTimeZoneInformation(&tzi);
  ASSERT_NE(tzType, TIME_ZONE_ID_INVALID);

  LONG minute_offset = tzi.Bias;
  if (tzType == TIME_ZONE_ID_DAYLIGHT) {
    minute_offset += tzi.DaylightBias;
  } else if (tzType == TIME_ZONE_ID_STANDARD) {
    minute_offset += tzi.StandardBias;
  }

  int64_t expected = -(int64_t)minute_offset * 60;
  checkWindowsTimezoneOffset(expected);
}

// Keep rule names intact so DST and history remain available in every DLL.
TEST(osTimeTests, windowsTZEnvKeepsRuleName) {
  WindowsTimezoneGuard guard;
  const char *names[] = {"UTC-8", "UTC", "UTC+5", "UTC-5:30",
                         "America/New_York", "Asia/Shanghai"};
  for (const char *name : names) {
    ASSERT_EQ(taosSetGlobalTimezone(name), 0);
    char env[TD_TIMEZONE_LEN] = {0};
    ASSERT_GT(GetEnvironmentVariableA(TAOS_INTERNAL_TZ_ENV, env, sizeof(env)),
               0u);
    EXPECT_STREQ(env, name);
  }
}

// POSIX TZ signs are reversed relative to UTC offsets.
TEST(osTimeTests, windowsGetOffsetParsePOSIX) {
  WindowsTimezoneGuard guard;
  ASSERT_TRUE(SetEnvironmentVariableA(TAOS_INTERNAL_TZ_ENV, NULL));
  ASSERT_TRUE(SetEnvironmentVariableA("TZ", "UTC-8:00"));
  checkWindowsTimezoneOffset(TdEastZone8);

  ASSERT_TRUE(SetEnvironmentVariableA("TZ", "UTC+0:00"));
  checkWindowsTimezoneOffset(0);

  ASSERT_TRUE(SetEnvironmentVariableA("TZ", "UTC+5:00"));
  checkWindowsTimezoneOffset(-5 * 3600);

  ASSERT_TRUE(SetEnvironmentVariableA("TZ", "UTC-5:30"));
  checkWindowsTimezoneOffset(5 * 3600 + 30 * 60);
}

// Older clients may leave an ISO 8601 offset in the TZ variable.
TEST(osTimeTests, windowsGetOffsetISO8601Compat) {
  WindowsTimezoneGuard guard;
  ASSERT_TRUE(SetEnvironmentVariableA(TAOS_INTERNAL_TZ_ENV, NULL));
  ASSERT_TRUE(SetEnvironmentVariableA("TZ", "+08:00"));
  checkWindowsTimezoneOffset(TdEastZone8);

  ASSERT_TRUE(SetEnvironmentVariableA("TZ", "+00:00"));
  checkWindowsTimezoneOffset(0);

  ASSERT_TRUE(SetEnvironmentVariableA("TZ", "-05:00"));
  checkWindowsTimezoneOffset(-5 * 3600);
}

TEST(osTimeTests, windowsInitTimezoneKeepsSystemRules) {
  WindowsTimezoneGuard guard;
  ASSERT_TRUE(SetEnvironmentVariableA(TAOS_INTERNAL_TZ_ENV, NULL));
  ASSERT_TRUE(SetEnvironmentVariableA("TZ", NULL));
  ASSERT_EQ(initTimezoneInfo(), TSDB_CODE_SUCCESS);
  time_t stamp = 1721044800;
  struct tm local = {};
  EXPECT_NE(taosLocalTime(&stamp, &local, NULL, 0, NULL), nullptr);
}
#endif

TEST(osTimeTests, invalidParameter) {
  void          *retp = NULL;
  int32_t        reti = 0;
  char           buf[1024] = {0};
  char           fmt[1024] = {0};
  struct tm      tm = {0};
  struct timeval tv = {0};

  retp = taosStrpTime(buf, fmt, NULL);
  EXPECT_EQ(retp, nullptr);
  retp = taosStrpTime(NULL, fmt, &tm);
  EXPECT_EQ(retp, nullptr);
  retp = taosStrpTime(buf, NULL, &tm);
  EXPECT_EQ(retp, nullptr);

  reti = taosGetTimeOfDay(NULL);
  EXPECT_NE(reti, 0);

  reti = taosTime(NULL);
  EXPECT_NE(reti, 0);

  tm.tm_year = 2024;
  tm.tm_mon = 10;
  tm.tm_mday = 23;
  tm.tm_hour = 12;
  tm.tm_min = 1;
  tm.tm_sec = 0;
  tm.tm_isdst = -1;
  time_t rett = taosMktime(&tm, NULL);
  EXPECT_NE(rett, 0);

  retp = taosLocalTime(NULL, &tm, NULL, 0, NULL);
  EXPECT_EQ(retp, nullptr);

  retp = taosLocalTime(&rett, NULL, NULL, 0, NULL);
  EXPECT_EQ(retp, nullptr);

  reti = taosSetGlobalTimezone(NULL);
  EXPECT_NE(reti, 0);
}

#ifndef WINDOWS
TEST(osTimeTests, truncateTimezoneStringRemovesLeadingSlash) {
  char tz[TD_TIMEZONE_LEN] = "/UTC";

  truncateTimezoneString(tz);

  EXPECT_STREQ(tz, "UTC");
}
#endif

TEST(osTimeTests, user_mktime64) {
  int64_t reti = 0;

  reti = user_mktime64(2024, 10, 23, 12, 3, 2, 1);
  EXPECT_NE(reti, 0);

  reti = user_mktime64(2024, 1, 23, 12, 3, 2, 1);
  EXPECT_NE(reti, 0);
}

TEST(osTimeTests, taosLocalTimeBenchmark) {
  const char* runBenchmark = getenv("TAOS_RUN_OS_TIME_BENCHMARK");
  if (runBenchmark == nullptr || strcmp(runBenchmark, "1") != 0) {
    GTEST_SKIP() << "set TAOS_RUN_OS_TIME_BENCHMARK=1 to run this stress benchmark";
  }

  const int threads = 400;
  const int iters = 1000000;

  std::atomic<uint64_t> ok{0}, err{0};
  std::vector<std::thread> ths;

  char tsTimezoneStr[TD_TIMEZONE_LEN] = {0};
  (void)initTimezoneInfo();

  // Use a fixed timestamp to avoid extra syscalls in the hot loop
  time_t tp = taosGetTimestampSec();

  auto start = std::chrono::steady_clock::now();
  ths.reserve(threads);
  for (int i = 0; i < threads; ++i) {
    ths.emplace_back([&]() {
      struct tm tm1;
      for (int j = 0; j < iters; ++j) {
        if (taosLocalTime(&tp, &tm1, NULL, 0, NULL) != nullptr) {
          ok.fetch_add(1, std::memory_order_relaxed);
        } else {
          err.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (auto &t : ths) t.join();
  auto end = std::chrono::steady_clock::now();

  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  const uint64_t total = static_cast<uint64_t>(threads) * static_cast<uint64_t>(iters);
  double qps = ms > 0 ? (double)total / ((double)ms / 1000.0) : 0.0;

  std::cout << "[taosLocalTime bench] threads=" << threads
            << " iters=" << iters
            << " total_calls=" << total
            << " ok=" << ok.load()
            << " err=" << err.load()
            << " elapsed_ms=" << ms
            << " throughput_calls_per_sec=" << qps
            << std::endl;

  // Ensure correctness: all calls should succeed
  EXPECT_EQ(ok.load(), total);
  EXPECT_EQ(err.load(), 0u);
}

#ifdef WINDOWS
#else
TEST(osTimeTests, tzConcurrencyBreakTest) {
  constexpr int kReaderThreads = 16;
  constexpr int kDurationSec = 3;

  ASSERT_EQ(initTimezoneInfo(), TSDB_CODE_SUCCESS);

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> errors{0};
  std::vector<std::thread> threads;

  time_t tp = taosGetTimestampSec();

  threads.emplace_back([&]() {
    const char* tzs[] = {
      "UTC",
      "Asia/Shanghai",
      "America/New_York",
      "Europe/Berlin"
    };

    int i = 0;
    while (!stop.load(std::memory_order_relaxed)) {
      setenv("TZ", tzs[i++ % 4], 1);
      tzset();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  });

  for (int i = 0; i < kReaderThreads; ++i) {
    threads.emplace_back([&]() {
      struct tm tm1;
      while (!stop.load(std::memory_order_relaxed)) {
        if (!taosLocalTime(&tp, &tm1, nullptr, 0, nullptr)) {
          errors.fetch_add(1, std::memory_order_relaxed);
          continue;
        }

        if (tm1.tm_sec < 0 || tm1.tm_sec > 60 ||
            tm1.tm_min < 0 || tm1.tm_min > 59 ||
            tm1.tm_hour < 0 || tm1.tm_hour > 23 ||
            tm1.tm_mday < 1 || tm1.tm_mday > 31 ||
            tm1.tm_mon < 0 || tm1.tm_mon > 11) {
          errors.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::seconds(kDurationSec));

  stop.store(true, std::memory_order_relaxed);

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  std::cout << "Test completed with " << errors.load() << " errors" << std::endl;
  
  unsetenv("TZ");
  tzset();
}
#endif
