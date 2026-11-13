// Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#include <gtest/gtest.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <limits>
#include <atomic>
#include <string>
#include <thread>
#include <vector>
#include "ttime.h"
#include "tenv.h"
#ifdef WINDOWS
#include "osInt.h"
#endif

class TimezoneTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(taosSetGlobalTimezone("UTC"), 0);
  }

  void TearDown() override {
    EXPECT_EQ(taosSetGlobalTimezone("Asia/Shanghai"), 0);
  }
};

TEST_F(TimezoneTest, LinuxTimezoneStrings) {
  struct Case {
    const char *name;
    int offset;
  } cases[] = {
      {"utc8", -28800}, {"UTC8", -28800}, {"UTC+8", -28800},
      {"UTC-8", 28800}, {"utc-08:00", 28800}, {"+05:30", -19800},
      {"-05:45", 20700}, {"UTC+0530", -19800}, {"UTC0", 0},
      {"UTC", 0}, {"Z", 0}, {"z", 0}, {"UTC-14", 50400},
      {"UTC+12", -43200}, {"UTC+13", -46800}, {"UTC-11", 39600},
      {"UTC-02", 7200}, {"UTC-08", 28800}, {"UTC-09", 32400},
      {"Asia/Shanghai", 28800}, {"Asia/Kathmandu", 20700},
      {"China Standard Time", 28800}, {"Etc/GMT+8", -28800}};
  const time_t stamp = 1705320000;  // 2024-01-15 12:00:00 UTC.
  for (const auto &test : cases) {
    SCOPED_TRACE(test.name);
    char normalized[TD_TIMEZONE_LEN] = {0};
    timezone_t tz = nullptr;
    ASSERT_EQ(taosValidateAndNormalizeTimezone(test.name, normalized,
              sizeof(normalized), &tz), 0);
    ASSERT_NE(tz, nullptr);
    int64_t offset = 0;
    EXPECT_EQ(taosGetTimezoneOffsetAtSeconds(stamp, tz, &offset), 0);
    EXPECT_EQ(offset, test.offset);
    tzfree(tz);
  }
}

TEST_F(TimezoneTest, CentralAsiaWindowsNameUsesBishkek) {
  EXPECT_STREQ(taosWindowsNameToIana("Central Asia Standard Time"),
               "Asia/Bishkek");
  EXPECT_STREQ(taosIanaNameToWindows("Asia/Bishkek"),
               "Central Asia Standard Time");
  char normalized[TD_TIMEZONE_LEN] = {0};
  timezone_t tz = nullptr;
  int32_t code = taosValidateAndNormalizeTimezone("Central Asia Standard Time",
                                                  normalized,
                                                  sizeof(normalized), &tz);
  ASSERT_EQ(code, 0);
  ASSERT_NE(tz, nullptr);
  EXPECT_STREQ(normalized, "Asia/Bishkek");
  int64_t offset = 0;
  EXPECT_EQ(taosGetTimezoneOffsetAtSeconds(1721044800, tz, &offset), 0);
  EXPECT_EQ(offset, 21600);
  tzfree(tz);
}

TEST_F(TimezoneTest, UtcFormattingKeepsFractionsAndTargetOffset) {
  struct Case {
    const char *zone;
    int64_t seconds;
    const char *date;
    const char *offset;
  } cases[] = {
      {"UTC", 0, "1970-01-01T00:00:00", "+0000"},
      {"Asia/Shanghai", 0, "1970-01-01T08:00:00", "+0800"},
      {"America/New_York", 1705320000, "2024-01-15T07:00:00", "-0500"},
      {"America/New_York", 1721044800, "2024-07-15T08:00:00", "-0400"}};
  const int64_t factors[] = {1000, 1000000, 1000000000};
  const char *fractions[] = {".123", ".000123", ".000000123"};
  for (const auto &test : cases) {
    SCOPED_TRACE(test.zone);
    ASSERT_EQ(taosSetGlobalTimezone(test.zone), 0);
    for (int precision = 0; precision < 3; ++precision) {
      SCOPED_TRACE(precision);
      char text[40] = {0};
      int64_t stamp = test.seconds * factors[precision] + 123;
      ASSERT_EQ(taosFormatUtcTime(text, sizeof(text), stamp, precision), 0);
      std::string expected = std::string(test.date) + fractions[precision] +
                             test.offset;
      EXPECT_EQ(std::string(text), expected);
    }
  }
}

TEST_F(TimezoneTest, UtcFormattingStaysConsistentDuringDefaultChange) {
  const int iterations = 2000;
  std::atomic<int> step{0};
  std::atomic<int> updateErrors{0};
  std::thread updater([&] {
    for (int i = 0; i < iterations; ++i) {
      while (step.load() != 2 * i + 1) std::this_thread::yield();
      if (taosSetGlobalTimezone("Asia/Shanghai") != 0) ++updateErrors;
      step.store(2 * i + 2);
    }
  });
  int formatErrors = 0;
  std::string firstError;
  for (int i = 0; i < iterations; ++i) {
    EXPECT_EQ(taosSetGlobalTimezone("UTC"), 0);
    step.store(2 * i + 1);
    char text[40] = {0};
    int32_t code = taosFormatUtcTime(text, sizeof(text), 0,
                                     TSDB_TIME_PRECISION_MILLI);
    if (code != 0 ||
        (std::strcmp(text, "1970-01-01T00:00:00.000+0000") != 0 &&
         std::strcmp(text, "1970-01-01T08:00:00.000+0800") != 0)) {
      if (formatErrors++ == 0) firstError = text;
    }
    // Allow only one update per call so Unix's borrowed rules stay alive.
    while (step.load() != 2 * i + 2) std::this_thread::yield();
  }
  updater.join();
  EXPECT_EQ(updateErrors.load(), 0);
  EXPECT_EQ(formatErrors, 0) << firstError;
}

TEST_F(TimezoneTest, InvalidStringsKeepDefault) {
  const char *names[] = {"", "No/Such_Zone", "America/New_York/junk",
      "GMT", "GMT+8", "CST", "EST", "UTC15", "UTC+14:01",
      "UTC-15:00", "UTC+05:60", "UTC999", "+5:30"};
  for (const char *name : names) {
    SCOPED_TRACE(name);
    char normalized[TD_TIMEZONE_LEN] = {0};
    EXPECT_NE(taosValidateAndNormalizeTimezone(name, normalized,
              sizeof(normalized), nullptr), 0);
  }
  EXPECT_NE(taosSetGlobalTimezone("No/Such_Zone"), 0);
  time_t stamp = 1705320000;
  struct tm result = {};
  ASSERT_NE(taosLocalTime(&stamp, &result, nullptr, 0, nullptr), nullptr);
  EXPECT_EQ(result.tm_hour, 12);
}

TEST_F(TimezoneTest, NegativeFractionFormatsUseSameSecond) {
  timezone_t tz = tzalloc("America/New_York");
  ASSERT_NE(tz, nullptr);
  ASSERT_EQ(taosSetGlobalTimezone("America/New_York"), 0);
  const int precisions[] = {TSDB_TIME_PRECISION_MILLI,
      TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO};
  const int64_t factors[] = {1000, 1000000, 1000000000};
  for (int i = 0; i < 3; ++i) {
    SCOPED_TRACE(precisions[i]);
    struct STm local = {};
    ASSERT_EQ(taosTs2Tm(-1, precisions[i], &local, tz), 0);
    EXPECT_EQ(local.tm.tm_sec, 59);
    EXPECT_EQ(local.fsec, 1000000000LL - 1000000000LL / factors[i]);
    int64_t back = 0;
    ASSERT_EQ(taosTm2Ts(&local, &back, precisions[i], tz), 0);
    EXPECT_EQ(back, -1);
    SArray *formats = nullptr;
    char text[128] = {0};
    ASSERT_EQ(taosTs2Char("TZH TZ", &formats, -1, precisions[i],
              text, sizeof(text), tz), 0);
    EXPECT_STREQ(text, "-05 -05:00");
    taosArrayDestroy(formats);
    ASSERT_EQ(TEST_ts2char("TZH TZ", -1, precisions[i],
              text, sizeof(text)), 0);
    EXPECT_STREQ(text, "-05 -05:00");
  }
  tzfree(tz);
}

TEST_F(TimezoneTest, TimestampParsingChecksPrecisionBounds) {
  if (sizeof(time_t) <= sizeof(int32_t)) GTEST_SKIP();
  timezone_t tz = tzalloc("UTC");
  ASSERT_NE(tz, nullptr);
  struct Case {
    const char *text;
    int64_t fraction;
    int64_t expected[3];
    bool nanoValid;
  } cases[] = {
      {"2100-01-01 00:00:00", 0,
       {4102444800000LL, 4102444800000000LL, 4102444800000000000LL}, true},
      {"9999-12-31 23:59:59", 0,
       {253402300799000LL, 253402300799000000LL, 0}, false},
      {"1969-12-31 23:59:59", 0, {-1000, -1000000, -1000000000}, true},
      {"1969-12-31 23:59:59.999999999", 999999999, {-1, -1, -1}, true},
      {"1677-09-21 00:12:43.145224192", 145224192,
       {-9223372036855LL, -9223372036854776LL,
        (std::numeric_limits<int64_t>::min)()}, true},
      {"1677-09-21 00:12:43.145224191", 145224191,
       {-9223372036855LL, -9223372036854776LL, 0}, false},
      {"2262-04-11 23:47:16.854775807", 854775807,
       {9223372036854LL, 9223372036854775LL,
        (std::numeric_limits<int64_t>::max)()}, true},
      {"2262-04-11 23:47:16.854775808", 854775808,
       {9223372036854LL, 9223372036854775LL, 0}, false},
  };
  const int precisions[] = {TSDB_TIME_PRECISION_MILLI,
      TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO};
  for (const Case &test : cases) {
    SCOPED_TRACE(test.text);
    struct STm fields = {};
    ASSERT_EQ(std::sscanf(test.text, "%d-%d-%d %d:%d:%d", &fields.tm.tm_year,
                         &fields.tm.tm_mon, &fields.tm.tm_mday,
                         &fields.tm.tm_hour, &fields.tm.tm_min,
                         &fields.tm.tm_sec), 6);
    fields.tm.tm_year -= 1900;
    fields.tm.tm_mon -= 1;
    fields.tm.tm_isdst = -1;
    fields.fsec = test.fraction;
    const char *fraction = std::strchr(test.text, '.') ? ".NS" : "";
    for (int i = 0; i < 3; ++i) {
      SCOPED_TRACE(precisions[i]);
      const bool valid = i != 2 || test.nanoValid;
      int64_t actual = 0;
      for (const char *suffix : {"", "Z", "+00:00"}) {
        SCOPED_TRACE(suffix);
        std::string input = std::string(test.text) + suffix;
        errno = ERANGE;
        int32_t code = taosParseTime(input.c_str(), &actual, input.size(),
                                    precisions[i], tz);
        EXPECT_EQ(code == 0, valid);
        if (code == 0 && valid) EXPECT_EQ(actual, test.expected[i]);
      }
      for (bool explicitOffset : {false, true}) {
        SCOPED_TRACE(explicitOffset);
        std::string format = std::string("YYYY-MM-DD HH24:MI:SS") +
                             fraction + (explicitOffset ? " TZ" : "");
        std::string input = std::string(test.text) +
                            (explicitOffset ? " +00:00" : "");
        SArray *formats = nullptr;
        char error[128] = {0};
        errno = ERANGE;
        int32_t code = taosChar2Ts(format.c_str(), &formats, input.c_str(),
                                  &actual, precisions[i], error, sizeof(error),
                                  tz);
        EXPECT_EQ(code == 0, valid) << error;
        if (code == 0 && valid) EXPECT_EQ(actual, test.expected[i]);
        taosArrayDestroy(formats);
      }
      struct STm local = fields;
      errno = ERANGE;
      int32_t code = taosTm2Ts(&local, &actual, precisions[i], tz);
      EXPECT_EQ(code == 0, valid);
      if (code == 0 && valid) EXPECT_EQ(actual, test.expected[i]);
    }
  }
  struct OffsetCase {
    const char *text;
    const char *offset;
    int64_t expected;
  } offsetCases[] = {
      {"2262-04-12 00:47:16.854775807", "+01:00",
       (std::numeric_limits<int64_t>::max)()},
      {"1677-09-20 23:12:43.145224192", "-01:00",
       (std::numeric_limits<int64_t>::min)()},
  };
  for (const OffsetCase &test : offsetCases) {
    SCOPED_TRACE(test.text);
    std::string input = std::string(test.text) + test.offset;
    int64_t actual = 0;
    errno = ERANGE;
    int32_t code = taosParseTime(input.c_str(), &actual, input.size(),
                                TSDB_TIME_PRECISION_NANO, tz);
    EXPECT_EQ(code, 0);
    if (code == 0) EXPECT_EQ(actual, test.expected);
    input = std::string(test.text) + " " + test.offset;
    SArray *formats = nullptr;
    char error[128] = {0};
    errno = ERANGE;
    code = taosChar2Ts("YYYY-MM-DD HH24:MI:SS.NS TZ", &formats, input.c_str(),
                      &actual, TSDB_TIME_PRECISION_NANO, error, sizeof(error),
                      tz);
    EXPECT_EQ(code, 0) << error;
    if (code == 0) EXPECT_EQ(actual, test.expected);
    taosArrayDestroy(formats);
  }
  tzfree(tz);
}

TEST_F(TimezoneTest, BrokenDownTimestampRejectsMktimeOverflow) {
  timezone_t tz = tzalloc("UTC");
  ASSERT_NE(tz, nullptr);
  for (int precision : {TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO,
                       TSDB_TIME_PRECISION_NANO}) {
    struct STm local = {};
    local.tm.tm_year = (std::numeric_limits<int>::max)();
    local.tm.tm_mon = 12;
    local.tm.tm_mday = 1;
    local.tm.tm_isdst = -1;
    int64_t actual = 0;
    EXPECT_NE(taosTm2Ts(&local, &actual, precision, tz), 0);
  }
  tzfree(tz);
}

TEST_F(TimezoneTest, RefreshedDisplayFollowsCurrentOffset) {
  timezone_t tz = tzalloc("America/New_York");
  ASSERT_NE(tz, nullptr);
  char winter[TD_TIMEZONE_LEN] = {0};
  ASSERT_EQ(taosFormatTimezoneStr(1705320000, "America/New_York", tz, winter),
            0);
  EXPECT_STREQ(winter, "America/New_York (EST, -0500)");
  char summer[TD_TIMEZONE_LEN] = {0};
  ASSERT_EQ(taosFormatTimezoneStr(1721044800, "America/New_York", tz, summer),
            0);
  EXPECT_STREQ(summer, "America/New_York (EDT, -0400)");

  // Whichever season a display was formatted in, refreshing keeps the name
  // and renders the abbreviation and offset in effect now.
  char expected[TD_TIMEZONE_LEN] = {0};
  ASSERT_EQ(taosFormatTimezoneStr(taosGetTimestampSec(), "America/New_York",
                                  tz, expected), 0);
  for (const char *display : {winter, summer}) {
    SCOPED_TRACE(display);
    char refreshed[TD_TIMEZONE_LEN] = {0};
    ASSERT_EQ(taosRefreshTimezoneStr(display, tz, refreshed), 0);
    EXPECT_STREQ(refreshed, expected);
  }

  // tz == NULL renders with the global default rules.
  ASSERT_EQ(taosSetGlobalTimezone("America/New_York"), 0);
  char refreshed[TD_TIMEZONE_LEN] = {0};
  ASSERT_EQ(taosRefreshTimezoneStr(winter, NULL, refreshed), 0);
  EXPECT_STREQ(refreshed, expected);

  EXPECT_NE(taosRefreshTimezoneStr("", tz, refreshed), 0);
  EXPECT_NE(taosRefreshTimezoneStr(" (EST, -0500)", tz, refreshed), 0);
  tzfree(tz);
}

TEST_F(TimezoneTest, DisplayUsesAbsoluteInstantInRepeatedHour) {
  timezone_t tz = tzalloc("America/New_York");
  ASSERT_NE(tz, nullptr);
  struct Case {
    time_t stamp;
    const char *expected;
  } cases[] = {
      {1730611800, "America/New_York (EDT, -0400)"},
      {1730615400, "America/New_York (EST, -0500)"},
      {-2717650978LL, "America/New_York (LMT, -0456)"},
      {-2717650740LL, "America/New_York (EST, -0500)"}};
  for (const auto &test : cases) {
    SCOPED_TRACE(test.stamp);
    char display[TD_TIMEZONE_LEN] = {0};
    EXPECT_EQ(taosFormatTimezoneStr(test.stamp, "America/New_York", tz,
                                    display), 0);
    EXPECT_STREQ(display, test.expected);
  }
  tzfree(tz);
}

#ifdef DARWIN
TEST_F(TimezoneTest, StrftimeUsesSuppliedOffset) {
  timezone_t tz = tzalloc("America/New_York");
  ASSERT_NE(tz, nullptr);
  const time_t winter = 1705320000;
  const time_t summer = 1721044800;
  struct tm first = {}, second = {};
  ASSERT_NE(taosLocalTime(&winter, &first, nullptr, 0, tz), nullptr);
  ASSERT_NE(taosLocalTime(&summer, &second, nullptr, 0, tz), nullptr);
  ASSERT_EQ(taosSetGlobalTimezone("Asia/Shanghai"), 0);

  char text[128] = {0};
  ASSERT_GT(taosStrfTime(text, sizeof(text), "%H:%M %z %Z", &first), 0u);
  EXPECT_STREQ(text, "07:00 -0500 EST");
  ASSERT_GT(taosStrfTime(text, sizeof(text), "%H:%M %z %Z", &second), 0u);
  EXPECT_STREQ(text, "08:00 -0400 EDT");
  ASSERT_GT(taosStrfTime(text, sizeof(text), "%%z %z %Ez %Oz", &second), 0u);
  EXPECT_STREQ(text, "%z -0400 -0400 -0400");
  ASSERT_GT(taosStrfTime(text, sizeof(text), "%-z %_z %0z", &second), 0u);
  EXPECT_STREQ(text, "-400 - 400 -0400");
  EXPECT_EQ(taosStrfTime(text, 5, "%z", &second), 0u);
  EXPECT_EQ(taosStrfTime(text, 6, "%z", &second), 5u);
  EXPECT_STREQ(text, "-0400");
  second.tm_gmtoff = 37800;
  ASSERT_GT(taosStrfTime(text, sizeof(text), "%z", &second), 0u);
  EXPECT_STREQ(text, "+1030");
  second.tm_gmtoff = -30;
  ASSERT_GT(taosStrfTime(text, sizeof(text), "%z", &second), 0u);
  EXPECT_STREQ(text, "-0000");
  second.tm_isdst = -1;
  ASSERT_GT(taosStrfTime(text, sizeof(text), "[%z]", &second), 0u);
  EXPECT_STREQ(text, "[]");
  tzfree(tz);
}
#endif

#ifdef WINDOWS
TEST_F(TimezoneTest, LocaltimeOverflowSetsError) {
  if (sizeof(time_t) <= sizeof(int32_t)) GTEST_SKIP();
  timezone_t tz = tzalloc("America/New_York");
  ASSERT_NE(tz, nullptr);
  const time_t stamps[] = {(std::numeric_limits<time_t>::min)(),
                           (std::numeric_limits<time_t>::max)()};
  for (time_t stamp : stamps) {
    struct tm local = {};
    for (int previous : {0, EINVAL}) {
      errno = previous;
      EXPECT_EQ(taosLocalTime(&stamp, &local, nullptr, 0, tz), nullptr);
      EXPECT_EQ(errno, EOVERFLOW);
    }
  }
  tzfree(tz);
}

TEST_F(TimezoneTest, StrftimePreservesSuppliedFields) {
  ASSERT_EQ(taosSetGlobalTimezone("America/New_York"), 0);
  struct tm value = {};
  value.tm_year = 124;
  value.tm_mon = 6;
  value.tm_mday = 15;
  value.tm_hour = 8;
  value.tm_isdst = 0;
  char text[128] = {0};
  ASSERT_GT(taosStrfTime(text, sizeof(text), "%H %z", &value), 0u);
  EXPECT_EQ(std::string(text, 2), "08");
}

TEST_F(TimezoneTest, FailedOffsetDoesNotReturnErrorAsSeconds) {
  ASSERT_TRUE(SetEnvironmentVariableA(TAOS_INTERNAL_TZ_ENV, "No/Such_Zone"));
  int32_t code = 0;
  EXPECT_EQ(taosGetTZOffsetSeconds(nullptr, &code), 0);
  EXPECT_NE(code, 0);
  EXPECT_EQ(taosGetTZOffsetSeconds(nullptr, nullptr), 0);
  EXPECT_NE(terrno, 0);
  ASSERT_EQ(taosSetGlobalTimezone("UTC"), 0);
  terrno = TSDB_CODE_INVALID_PARA;
  EXPECT_EQ(taosGetTZOffsetSeconds(nullptr, nullptr), 0);
  EXPECT_EQ(terrno, TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(taosGetLocalTimezoneOffset(&code), 0);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(terrno, TSDB_CODE_INVALID_PARA);
}

TEST_F(TimezoneTest, EmbeddedRulesIncludeRecentTimezoneChanges) {
  struct Case {
    const char *name;
    time_t stamp;
    int64_t offset;
  } cases[] = {
      {"America/Coyhaique", 1751371200, -10800},
      {"America/Havana", 1794312000, -14400}};
  for (const auto &test : cases) {
    SCOPED_TRACE(test.name);
    timezone_t tz = tzalloc(test.name);
    ASSERT_NE(tz, nullptr);
    int64_t offset = 0;
    EXPECT_EQ(taosGetTimezoneOffsetAtSeconds(test.stamp, tz, &offset), 0);
    EXPECT_EQ(offset, test.offset);
    tzfree(tz);
  }
}

TEST_F(TimezoneTest, SystemRulesHonorDisabledDstAndFallback) {
  DYNAMIC_TIME_ZONE_INFORMATION info = {};
  info.Bias = 300;
  info.DaylightBias = -60;
  wcscpy(info.TimeZoneKeyName, L"Eastern Standard Time");
  info.DaylightDate.wMonth = 3;
  info.DaylightDate.wDay = 2;
  info.DaylightDate.wHour = 2;
  info.StandardDate.wMonth = 11;
  info.StandardDate.wDay = 1;
  info.StandardDate.wHour = 2;
  SWindowsTimezoneConfig config;
  taosWindowsTimezoneConfig(&info, TIME_ZONE_ID_DAYLIGHT, &config);
  EXPECT_STREQ(config.name, "America/New_York");

  info.DynamicDaylightTimeDisabled = TRUE;
  info.DaylightDate.wMonth = 0;
  info.StandardDate.wMonth = 0;
  taosWindowsTimezoneConfig(&info, TIME_ZONE_ID_UNKNOWN, &config);
  timezone_t tz = tzalloc(config.name);
  ASSERT_NE(tz, nullptr);
  time_t summer = 1721044800;
  struct tm local = {};
  ASSERT_NE(taosLocalTime(&summer, &local, nullptr, 0, tz), nullptr);
  EXPECT_EQ(local.tm_hour, 7);
  EXPECT_EQ(local.tm_isdst, 0);
  tzfree(tz);

  info.DaylightDate.wMonth = 4;
  info.DaylightDate.wDay = 1;
  info.StandardDate.wMonth = 10;
  info.StandardDate.wDay = 5;
  taosWindowsTimezoneConfig(&info, TIME_ZONE_ID_DAYLIGHT, &config);
  tz = tzalloc(config.name);
  ASSERT_NE(tz, nullptr);
  time_t march = 1710936000;
  ASSERT_NE(taosLocalTime(&march, &local, nullptr, 0, tz), nullptr);
  EXPECT_EQ(local.tm_hour, 7);
  ASSERT_NE(taosLocalTime(&summer, &local, nullptr, 0, tz), nullptr);
  EXPECT_EQ(local.tm_hour, 8);
  tzfree(tz);

  info.DynamicDaylightTimeDisabled = FALSE;
  wcscpy(info.TimeZoneKeyName, L"Unlisted Standard Time");
  taosWindowsTimezoneConfig(&info, TIME_ZONE_ID_DAYLIGHT, &config);
  EXPECT_TRUE(config.recovered);
  tz = tzalloc(config.name);
  ASSERT_NE(tz, nullptr);
  ASSERT_NE(taosLocalTime(&march, &local, nullptr, 0, tz), nullptr);
  EXPECT_EQ(local.tm_hour, 7);
  ASSERT_NE(taosLocalTime(&summer, &local, nullptr, 0, tz), nullptr);
  EXPECT_EQ(local.tm_hour, 8);
  tzfree(tz);
  info.DaylightDate.wMonth = 0;
  info.StandardDate.wMonth = 0;
  taosWindowsTimezoneConfig(&info, TIME_ZONE_ID_DAYLIGHT, &config);
  EXPECT_STREQ(config.name, "UTC+4:00");
  taosWindowsTimezoneConfig(&info, TIME_ZONE_ID_INVALID, &config);
  EXPECT_TRUE(config.recovered);
  EXPECT_STREQ(config.name, "UTC");
}

TEST_F(TimezoneTest, DefaultZonesWithoutCurrentDstKeepHistory) {
  struct Case {
    const wchar_t *windows;
    const char *iana;
    LONG bias;
    time_t stamp;
    int hour;
  } cases[] = {
      {L"China Standard Time", "Asia/Shanghai", -480, 678369600, 21},
      {L"Russian Standard Time", "Europe/Moscow", -180,
       1277985600, 16},
      {L"Central Asia Standard Time", "Asia/Bishkek", -360,
       1721044800, 18}};
  for (const auto &test : cases) {
    SCOPED_TRACE(test.iana);
    DYNAMIC_TIME_ZONE_INFORMATION info = {};
    info.Bias = test.bias;
    wcscpy(info.TimeZoneKeyName, test.windows);
    SWindowsTimezoneConfig config;
    taosWindowsTimezoneConfig(&info, TIME_ZONE_ID_UNKNOWN, &config);
    EXPECT_STREQ(config.name, test.iana);
    timezone_t tz = taosLoadWindowsTimezone(&config);
    ASSERT_NE(tz, nullptr);
    char text[128] = {0};
    ASSERT_EQ(taosFormatTimezoneStr(test.stamp, taosTimezoneName(tz), tz,
                                   text), 0);
    EXPECT_EQ(std::string(text).find(test.iana), 0u);
    struct tm local = {};
    ASSERT_NE(taosLocalTime(&test.stamp, &local, nullptr, 0, tz), nullptr);
    EXPECT_EQ(local.tm_hour, test.hour);
    tzfree(tz);
  }
}

TEST_F(TimezoneTest, FallbackDisplaysTheLoadedRuleName) {
  SWindowsTimezoneConfig config = {};
  strcpy(config.name, "No/Such_Zone");
  strcpy(config.fallback, "UTC-5:30");
  const char *expected[] = {"UTC-5:30", "UTC"};
  for (const char *name : expected) {
    timezone_t tz = taosLoadWindowsTimezone(&config);
    ASSERT_NE(tz, nullptr);
    EXPECT_STREQ(taosTimezoneName(tz), name);
    char text[128] = {0};
    ASSERT_EQ(taosFormatTimezoneStr(1705320000, taosTimezoneName(tz), tz,
                                   text), 0);
    EXPECT_EQ(std::string(text).find(name), 0u);
    tzfree(tz);
    strcpy(config.fallback, "No/Such_Zone");
  }
}

TEST_F(TimezoneTest, InternalTimezoneIsNotAConfigurationVariable) {
  char saved[TD_TIMEZONE_LEN * 2] = {0};
  DWORD length = GetEnvironmentVariableA("TAOS_TIMEZONE", saved,
                                         sizeof(saved));
  ASSERT_LT(length, sizeof(saved));
  ASSERT_TRUE(SetEnvironmentVariableA("TAOS_TIMEZONE", "UTC+3"));
  EXPECT_EQ(taosSetGlobalTimezone("Asia/Shanghai"), 0);
  char current[TD_TIMEZONE_LEN] = {0};
  EXPECT_GT(GetEnvironmentVariableA("TAOS_TIMEZONE", current,
                                    sizeof(current)), 0u);
  EXPECT_STREQ(current, "UTC+3");
  char cfg[128] = {0};
  EXPECT_LT(taosEnvNameToCfgName(TAOS_INTERNAL_TZ_ENV, cfg, sizeof(cfg)), 0);
  EXPECT_EQ(cfg[0], '\0');
  EXPECT_TRUE(SetEnvironmentVariableA("TAOS_TIMEZONE",
                                      length > 0 ? saved : NULL));
}

TEST_F(TimezoneTest, InheritedRuntimeTimezoneIsKept) {
  char saved[TD_TIMEZONE_LEN * 2] = {0};
  DWORD length = GetEnvironmentVariableA("TZ", saved, sizeof(saved));
  ASSERT_LT(length, sizeof(saved));
  ASSERT_TRUE(SetEnvironmentVariableA("TZ", "UTC+3"));
  // Simulate a child process that inherited the parent's TDengine rules.
  ASSERT_TRUE(SetEnvironmentVariableA(TAOS_INTERNAL_TZ_ENV, "Asia/Shanghai"));
  ASSERT_EQ(initTimezoneInfo(), TSDB_CODE_SUCCESS);
  time_t stamp = 1705320000;
  struct tm local = {};
  EXPECT_NE(taosLocalTime(&stamp, &local, nullptr, 0, nullptr), nullptr);
  EXPECT_EQ(local.tm_hour, 20);
  char current[TD_TIMEZONE_LEN] = {0};
  EXPECT_GT(GetEnvironmentVariableA(TAOS_INTERNAL_TZ_ENV, current,
                                    sizeof(current)), 0u);
  EXPECT_STREQ(current, "Asia/Shanghai");
  EXPECT_TRUE(SetEnvironmentVariableA("TZ", length > 0 ? saved : NULL));
}

TEST_F(TimezoneTest, SettingTimezonePreservesHostCrt) {
  char saved[TD_TIMEZONE_LEN * 2] = {0};
  DWORD length = GetEnvironmentVariableA("TZ", saved, sizeof(saved));
  ASSERT_LT(length, sizeof(saved));
  ASSERT_TRUE(SetEnvironmentVariableA("TZ", "UTC+2"));
  _tzset();
  time_t stamp = 1705320000;
  struct tm before = {}, after = {};
  EXPECT_EQ(_localtime64_s(&before, &stamp), 0);
  EXPECT_EQ(taosSetGlobalTimezone("Asia/Shanghai"), 0);
  char current[64] = {0};
  EXPECT_GT(GetEnvironmentVariableA("TZ", current, sizeof(current)), 0u);
  EXPECT_STREQ(current, "UTC+2");
  _tzset();
  EXPECT_EQ(_localtime64_s(&after, &stamp), 0);
  EXPECT_EQ(before.tm_hour, after.tm_hour);
  EXPECT_TRUE(SetEnvironmentVariableA("TZ", length > 0 ? saved : NULL));
  _tzset();
}
#endif

TEST_F(TimezoneTest, RulesAndRoundTrips) {
  struct Case {
    const char *name;
    time_t stamp;
    int offset;
    const char *formatted;
  } cases[] = {
      {"America/New_York", 1705320000, -18000, "2024-01-15 07:00 -0500 EST"},
      {"America/New_York", 1721044800, -14400, "2024-07-15 08:00 -0400 EDT"},
      {"America/New_York", 1710053999, -18000, "2024-03-10 01:59 -0500 EST"},
      {"America/New_York", 1710054000, -14400, "2024-03-10 03:00 -0400 EDT"},
      {"America/New_York", 1730613599, -14400, "2024-11-03 01:59 -0400 EDT"},
      {"America/New_York", 1730613600, -18000, "2024-11-03 01:00 -0500 EST"},
      {"Australia/Lord_Howe", 1705320000, 39600,
       "2024-01-15 23:00 +1100 +11"},
      {"Australia/Lord_Howe", 1721044800, 37800,
       "2024-07-15 22:30 +1030 +1030"},
      {"Asia/Kathmandu", 1705320000, 20700, "2024-01-15 17:45 +0545 +0545"},
      {"Asia/Shanghai", 678369600, 32400, "1991-07-01 21:00 +0900 CDT"},
      {"America/New_York", -2717650801LL, -17762,
       "1883-11-18 12:03 -0456 LMT"},
      {"UTC", -1, 0, "1969-12-31 23:59 +0000 UTC"},
      {"UTC", 2147483648LL, 0, "2038-01-19 03:14 +0000 UTC"},
      {"America/New_York", 4102488000LL, -18000,
       "2100-01-01 07:00 -0500 EST"}};
  for (const auto &test : cases) {
    SCOPED_TRACE(test.name);
    SCOPED_TRACE(test.stamp);
    timezone_t tz = tzalloc(test.name);
    ASSERT_NE(tz, nullptr);
    struct tm local = {};
    ASSERT_NE(taosLocalTime(&test.stamp, &local, nullptr, 0, tz), nullptr);
    int64_t offset = 0;
    EXPECT_EQ(taosGetTimezoneOffsetAtSeconds(test.stamp, tz, &offset), 0);
    EXPECT_EQ(offset, test.offset);
    char text[128] = {0};
    EXPECT_GT(taosStrfTimeAt(text, sizeof(text), "%Y-%m-%d %H:%M %z %Z",
                            &test.stamp, tz), 0u);
    EXPECT_STREQ(text, test.formatted);
    char date[64] = {0};
    EXPECT_GT(taosStrfTime(date, sizeof(date), "%Y-%m-%d %H:%M", &local),
              0u);
    EXPECT_EQ(std::string(date), std::string(test.formatted, 16));
    errno = EINVAL;
    // CRT struct tm cannot distinguish historical folds with equal isdst.
    if (test.stamp != -2717650801LL) {
      EXPECT_EQ(taosMktime(&local, tz), test.stamp);
      if (test.stamp == -1) EXPECT_EQ(errno, 0);
    }
    tzfree(tz);
  }
}

TEST_F(TimezoneTest, FoldGapAndNormalization) {
  timezone_t tz = tzalloc("America/New_York");
  ASSERT_NE(tz, nullptr);
  struct tm local = {};
  local.tm_year = 124;
  local.tm_mon = 10;
  local.tm_mday = 3;
  local.tm_hour = 1;
  local.tm_min = 30;
  local.tm_isdst = 1;
  EXPECT_EQ(taosMktime(&local, tz), 1730611800);
  local.tm_isdst = 0;
  EXPECT_EQ(taosMktime(&local, tz), 1730615400);

  local.tm_mon = 2;
  local.tm_mday = 10;
  local.tm_hour = 2;
  local.tm_isdst = -1;
  time_t gap = taosMktime(&local, tz);
  // tzcode rejects an unspecified DST type in a missing local hour.
  EXPECT_EQ(gap, (time_t)-1);
  local.tm_isdst = 0;
  EXPECT_EQ(taosMktime(&local, tz), 1710055800);
  EXPECT_EQ(local.tm_hour, 3);

  local = {};
  local.tm_year = 124;
  local.tm_mon = 0;
  local.tm_mday = 32;
  local.tm_hour = -1;
  local.tm_isdst = -1;
  EXPECT_EQ(taosMktime(&local, tz), 1706760000);
  EXPECT_EQ(local.tm_mon, 0);
  EXPECT_EQ(local.tm_mday, 31);
  EXPECT_EQ(local.tm_hour, 23);
  tzfree(tz);
}

TEST_F(TimezoneTest, DefaultKeepsHistoricalRules) {
  ASSERT_EQ(taosSetGlobalTimezone("America/New_York"), 0);
  const time_t winter = 1705320000;
  const time_t summer = 1721044800;
  struct tm first = {}, second = {};
  ASSERT_NE(taosLocalTime(&winter, &first, nullptr, 0, nullptr), nullptr);
  ASSERT_NE(taosLocalTime(&summer, &second, nullptr, 0, nullptr), nullptr);
  EXPECT_EQ(first.tm_hour, 7);
  EXPECT_EQ(second.tm_hour, 8);
  char text[64] = {0};
  ASSERT_GT(taosStrfTime(text, sizeof(text), "%z %Z", &first), 0u);
  EXPECT_STREQ(text, "-0500 EST");
  ASSERT_GT(taosStrfTime(text, sizeof(text), "%z %Z", &second), 0u);
  EXPECT_STREQ(text, "-0400 EDT");
}

TEST_F(TimezoneTest, SqlFormatUsesTargetDateAndZone) {
  timezone_t tz = tzalloc("America/New_York");
  ASSERT_NE(tz, nullptr);
  char text[128] = {0};
  SArray *formats = nullptr;
  ASSERT_EQ(taosTs2Char("YYYY-MM-DD HH24:MI:SS TZ", &formats,
            1721044800000LL, TSDB_TIME_PRECISION_MILLI,
            text, sizeof(text), tz), 0);
  EXPECT_STREQ(text, "2024-07-15 08:00:00 -04:00");
  taosArrayDestroy(formats);
  formats = nullptr;
  int64_t stamp = 0;
  char error[128] = {0};
  ASSERT_EQ(taosChar2Ts("YYYY-MM-DD HH24:MI:SS TZ", &formats,
            "2024-07-15 08:00:00 -04:00", &stamp,
            TSDB_TIME_PRECISION_MILLI, error, sizeof(error), tz), 0);
  EXPECT_EQ(stamp, 1721044800000LL);
  taosArrayDestroy(formats);
  formats = nullptr;
  ASSERT_EQ(taosChar2Ts("YYYY-MM-DD HH24:MI:SS TZ", &formats,
            "2024-03-10 02:30:00 -05:00", &stamp,
            TSDB_TIME_PRECISION_MILLI, error, sizeof(error), tz), 0);
  EXPECT_EQ(stamp, 1710055800000LL);
  taosArrayDestroy(formats);
  formats = nullptr;
  ASSERT_EQ(taosChar2Ts("YYYY-MM-DD HH24:MI:SS TZ", &formats, "2024-01-15 17:45:00 +05:45", &stamp,
                        TSDB_TIME_PRECISION_MILLI, error, sizeof(error), tz),
            0);
  EXPECT_EQ(stamp, 1705320000000LL);
  taosArrayDestroy(formats);
  formats = nullptr;
  ASSERT_EQ(taosChar2Ts("YYYY-MM-DD HH24:MI:SS", &formats, "2024-07-15 08:00:00", &stamp, TSDB_TIME_PRECISION_MILLI,
                        error, sizeof(error), tz),
            0);
  EXPECT_EQ(stamp, 1721044800000LL);
  taosArrayDestroy(formats);
  formats = nullptr;
  ASSERT_EQ(taosChar2Ts("YYYY-MM-DD HH24:MI:SS TZ", &formats, "1969-12-31 23:59:59 +00:00", &stamp,
                        TSDB_TIME_PRECISION_MILLI, error, sizeof(error), tz),
            0);
  EXPECT_EQ(stamp, -1000);
  taosArrayDestroy(formats);
  tzfree(tz);
}

#ifdef WINDOWS
TEST_F(TimezoneTest, DllCachesFollowProcessTimezone) {
  char path[MAX_PATH] = {0};
  ASSERT_GT(GetModuleFileNameA(NULL, path, sizeof(path)), 0u);
  char *file = strrchr(path, '\\');
  ASSERT_NE(file, nullptr);
  strcpy(file + 1, "timezoneTestA.dll");
  HMODULE first = LoadLibraryA(path);
  ASSERT_NE(first, nullptr);
  strcpy(file + 1, "timezoneTestB.dll");
  HMODULE second = LoadLibraryA(path);
  ASSERT_NE(second, nullptr);
  using Set = int (*)(const char *);
  using Format = int (*)(time_t, char *, int);
  using FormatExplicit = int (*)(time_t, char *, int, timezone_t);
  using Cleanup = void (*)(void);
  auto set = (Set)GetProcAddress(first, "timezoneTestSet");
  auto format = (Format)GetProcAddress(second, "timezoneTestFormat");
  auto formatExplicit = (FormatExplicit)GetProcAddress(second, "timezoneTestFormatExplicit");
  auto cleanup = (Cleanup)GetProcAddress(second, "timezoneTestCleanup");
  ASSERT_NE(set, nullptr);
  ASSERT_NE(format, nullptr);
  ASSERT_NE(formatExplicit, nullptr);
  ASSERT_NE(cleanup, nullptr);
  char text[128] = {0};
  ASSERT_EQ(set("America/New_York"), 0);
  ASSERT_EQ(format(1705320000, text, sizeof(text)), 1);
  EXPECT_STREQ(text, "2024-01-15 07:00 -0500 EST");
  ASSERT_EQ(format(1721044800, text, sizeof(text)), 1);
  EXPECT_STREQ(text, "2024-07-15 08:00 -0400 EDT");
  ASSERT_EQ(set("Asia/Shanghai"), 0);
  ASSERT_EQ(format(1721044800, text, sizeof(text)), 1);
  EXPECT_STREQ(text, "2024-07-15 20:00 +0800 CST");
  cleanup();
  ASSERT_EQ(set("UTC"), 0);
  ASSERT_EQ(format(1721044800, text, sizeof(text)), 1);
  EXPECT_STREQ(text, "2024-07-15 12:00 +0000 UTC");
  // A rule owned by another module remains explicit even when this DLL's
  // default cache is cleared or changed.
  timezone_t explicitZone = tzalloc("America/New_York");
  ASSERT_NE(explicitZone, nullptr);
  ASSERT_EQ(formatExplicit(1705320000, text, sizeof(text), explicitZone), 1);
  EXPECT_STREQ(text, "2024-01-15 07:00 -0500 EST");
  cleanup();
  ASSERT_EQ(formatExplicit(1721044800, text, sizeof(text), explicitZone), 1);
  EXPECT_STREQ(text, "2024-07-15 08:00 -0400 EDT");
  tzfree(explicitZone);
  cleanup();
  EXPECT_TRUE(FreeLibrary(second));
  EXPECT_TRUE(FreeLibrary(first));
}
#endif

TEST_F(TimezoneTest, ConcurrentExplicitZonesAndCleanup) {
  timezone_t zones[] = {tzalloc("UTC"), tzalloc("America/New_York"), tzalloc("Asia/Shanghai")};
  for (auto tz : zones) ASSERT_NE(tz, nullptr);
  const time_t             stamp = 1721044800;
  const int                hours[] = {12, 8, 20};
  std::atomic<int> errors{0};
  std::vector<std::thread> readers;
  for (int i = 0; i < 6; ++i) {
    readers.emplace_back([&, i]() {
      for (int j = 0; j < 2000; ++j) {
        struct tm local = {};
        if (!taosLocalTime(&stamp, &local, nullptr, 0, zones[i % 3]) ||
            local.tm_hour != hours[i % 3]) ++errors;
        if (!taosLocalTime(&stamp, &local, nullptr, 0, nullptr) ||
            local.tm_hour != 12) ++errors;
      }
    });
  }
#ifdef WINDOWS
  for (int i = 0; i < 100; ++i) cleanupTimezoneInfo();
#endif
  for (auto &reader : readers) reader.join();
  EXPECT_EQ(errors.load(), 0);
  for (auto tz : zones) tzfree(tz);
}
