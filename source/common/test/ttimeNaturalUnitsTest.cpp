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
#include <time.h>
#include <chrono>
#include "ttime.h"

/**
 * Test suite for natural time unit boundary alignment
 *
 * Tests the alignToNaturalBoundary() function for:
 * - Week unit alignment (Monday 00:00:00)
 * - Month unit alignment (1st of month 00:00:00)
 * - Year unit alignment (Jan 1st 00:00:00)
 * - Multi-period alignment (2w, 3n, 2y)
 * - Offset application
 */

class TimeNaturalUnitsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Use Asia/Shanghai timezone for consistent testing
    tz = NULL;
    taosSetGlobalTimezone("Asia/Shanghai");
  }

  /**
   * Helper function to convert human-readable datetime to timestamp
   * @param year Year (e.g., 2026)
   * @param month Month (1-12)
   * @param day Day (1-31)
   * @param hour Hour (0-23)
   * @param minute Minute (0-59)
   * @param second Second (0-59)
   * @param precision Time precision (TSDB_TIME_PRECISION_MILLI/MICRO/NANO)
   * @return Timestamp in specified precision
   */
  int64_t makeTimestamp(int year, int month, int day, int hour, int minute, int second,
                        int8_t precision = TSDB_TIME_PRECISION_MILLI) {
    struct tm tm = {0};
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    tm.tm_hour = hour;
    tm.tm_min = minute;
    tm.tm_sec = second;
    tm.tm_isdst = -1;

    time_t  t = taosMktime(&tm, tz);
    int64_t ts = (int64_t)t;

    switch (precision) {
      case TSDB_TIME_PRECISION_MILLI:
        return ts * 1000LL;
      case TSDB_TIME_PRECISION_MICRO:
        return ts * 1000000LL;
      case TSDB_TIME_PRECISION_NANO:
        return ts * 1000000000LL;
      default:
        return ts * 1000LL;
    }
  }

  timezone_t tz;
};

/**
 * Test week unit alignment to Monday 00:00:00
 */
TEST_F(TimeNaturalUnitsTest, WeekAlignmentBasic) {
  // Test timestamp: 2026-03-10 15:30:00 (Tuesday)
  // Expected: align to 2026-03-09 00:00:00 (Monday)

  int64_t ts = makeTimestamp(2026, 3, 10, 15, 30, 0);
  int64_t result = alignToNaturalBoundary(ts, 'w', 1, 0, TSDB_TIME_PRECISION_MILLI, tz);

  // Convert result to struct tm to verify
  time_t    t = result / 1000;
  struct tm tm;
  taosLocalTime(&t, &tm, NULL, 0, NULL);
  std::cout << "Original timestamp: " << ts << ", Aligned timestamp: " << result << std::endl;

  // Should be Monday (tm_wday = 1)
  EXPECT_EQ(tm.tm_wday, 1);
  // Should be 00:00:00
  EXPECT_EQ(tm.tm_hour, 0);
  EXPECT_EQ(tm.tm_min, 0);
  EXPECT_EQ(tm.tm_sec, 0);
}

/**
 * Test month unit alignment to 1st of month 00:00:00
 */
TEST_F(TimeNaturalUnitsTest, MonthAlignmentBasic) {
  // Test timestamp: 2026-03-15 12:00:00
  // Expected: align to 2026-03-01 00:00:00

  int64_t ts = makeTimestamp(2026, 3, 15, 12, 0, 0);
  int64_t result = alignToNaturalBoundary(ts, 'n', 1, 0, TSDB_TIME_PRECISION_MILLI, tz);

  time_t    t = result / 1000;
  struct tm tm;
  taosLocalTime(&t, &tm, NULL, 0, NULL);
  std::cout << "Original timestamp: " << ts << ", Aligned timestamp: " << result << std::endl;

  // Should be 1st of month
  EXPECT_EQ(tm.tm_mday, 1);
  // Should be 00:00:00
  EXPECT_EQ(tm.tm_hour, 0);
  EXPECT_EQ(tm.tm_min, 0);
  EXPECT_EQ(tm.tm_sec, 0);
}

/**
 * Test year unit alignment to Jan 1st 00:00:00
 */
TEST_F(TimeNaturalUnitsTest, YearAlignmentBasic) {
  // Test timestamp: 2026-06-15 12:00:00
  // Expected: align to 2026-01-01 00:00:00

  int64_t ts = makeTimestamp(2026, 6, 15, 12, 0, 0);
  int64_t result = alignToNaturalBoundary(ts, 'y', 1, 0, TSDB_TIME_PRECISION_MILLI, tz);

  time_t    t = result / 1000;
  struct tm tm;
  taosLocalTime(&t, &tm, NULL, 0, NULL);
  std::cout << "Original timestamp: " << ts << ", Aligned timestamp: " << result << std::endl;

  // Should be Jan 1st
  EXPECT_EQ(tm.tm_mon, 0);  // January = 0
  EXPECT_EQ(tm.tm_mday, 1);
  // Should be 00:00:00
  EXPECT_EQ(tm.tm_hour, 0);
  EXPECT_EQ(tm.tm_min, 0);
  EXPECT_EQ(tm.tm_sec, 0);
}

/**
 * Test week unit with offset (1 day = Tuesday)
 */
TEST_F(TimeNaturalUnitsTest, WeekAlignmentWithOffset) {
  // Test timestamp: 2026-03-10 15:30:00 (Tuesday)
  // With 1 day offset, should align to Tuesday 00:00:00

  int64_t ts = makeTimestamp(2026, 3, 10, 15, 30, 0);
  int64_t offset = 24LL * 60LL * 60LL * 1000LL;  // 1 day in milliseconds
  int64_t result = alignToNaturalBoundary(ts, 'w', 1, offset, TSDB_TIME_PRECISION_MILLI, tz);

  time_t    t = result / 1000;
  struct tm tm;
  taosLocalTime(&t, &tm, NULL, 0, NULL);
  std::cout << "Original timestamp: " << ts << ", Aligned timestamp: " << result << std::endl;

  // Should be Tuesday (tm_wday = 2)
  EXPECT_EQ(tm.tm_wday, 2);
  // Should be 00:00:00
  EXPECT_EQ(tm.tm_hour, 0);
  EXPECT_EQ(tm.tm_min, 0);
  EXPECT_EQ(tm.tm_sec, 0);
}

/**
 * Test multi-period week alignment (2 weeks)
 */
TEST_F(TimeNaturalUnitsTest, MultiPeriodWeekAlignment) {
  // Test that 2-week periods align consistently
  // Two timestamps within the same 2-week period should align to the same boundary

  int64_t ts1 = makeTimestamp(2026, 3, 10, 0, 0, 0);  // 2026-03-10 (Tuesday, week 1)
  int64_t ts2 = makeTimestamp(2026, 3, 13, 0, 0, 0);  // 2026-03-13 (Friday, week 1)

  int64_t result1 = alignToNaturalBoundary(ts1, 'w', 2, 0, TSDB_TIME_PRECISION_MILLI, tz);
  int64_t result2 = alignToNaturalBoundary(ts2, 'w', 2, 0, TSDB_TIME_PRECISION_MILLI, tz);
  std::cout << "Timestamp 1: " << ts1 << ", Aligned 1: " << result1 << std::endl;
  std::cout << "Timestamp 2: " << ts2 << ", Aligned 2: " << result2 << std::endl;

  // Both should align to the same 2-week boundary
  EXPECT_EQ(result1, result2);

  // Test that adjacent 2-week periods align to different boundaries
  int64_t ts3 = makeTimestamp(2026, 3, 17, 0, 0, 0);  // 2026-03-17 (next week)
  int64_t result3 = alignToNaturalBoundary(ts3, 'w', 2, 0, TSDB_TIME_PRECISION_MILLI, tz);
  std::cout << "Timestamp 3: " << ts3 << ", Aligned 3: " << result3 << std::endl;
  EXPECT_NE(result1, result3);  // Should be different 2-week boundary

  // Window duration should be 14 days
  int64_t duration = result3 - result1;
  EXPECT_EQ(duration, 14LL * 24LL * 60LL * 60LL * 1000LL);
}

/**
 * Test multi-period month alignment (3 months / quarterly)
 */
TEST_F(TimeNaturalUnitsTest, MultiPeriodMonthAlignment) {
  // Test that 3-month periods align consistently
  // Timestamps within the same quarter should align to the same boundary

  int64_t ts1 = makeTimestamp(2026, 2, 15, 0, 0, 0);  // 2026-02-15 (Q1)
  int64_t ts2 = makeTimestamp(2026, 3, 20, 0, 0, 0);  // 2026-03-20 (Q1)

  int64_t result1 = alignToNaturalBoundary(ts1, 'n', 3, 0, TSDB_TIME_PRECISION_MILLI, tz);
  int64_t result2 = alignToNaturalBoundary(ts2, 'n', 3, 0, TSDB_TIME_PRECISION_MILLI, tz);
  std::cout << "Timestamp 1: " << ts1 << ", Aligned 1: " << result1 << std::endl;
  std::cout << "Timestamp 2: " << ts2 << ", Aligned 2: " << result2 << std::endl;

  // Both should align to Q1 start (2026-01-01)
  EXPECT_EQ(result1, result2);

  // Verify it's January 1st
  time_t    t = result1 / 1000;
  struct tm tm;
  taosLocalTime(&t, &tm, NULL, 0, NULL);
  EXPECT_EQ(tm.tm_mon, 0);  // January
  EXPECT_EQ(tm.tm_mday, 1);
  EXPECT_EQ(tm.tm_hour, 0);

  // Test that next quarter aligns to different boundary
  int64_t ts3 = makeTimestamp(2026, 4, 15, 0, 0, 0);  // 2026-04-15 (Q2)
  int64_t result3 = alignToNaturalBoundary(ts3, 'n', 3, 0, TSDB_TIME_PRECISION_MILLI, tz);
  std::cout << "Timestamp 3: " << ts3 << ", Aligned 3: " << result3 << std::endl;
  EXPECT_NE(result1, result3);

  // Verify Q2 starts at April 1st
  time_t    t3 = result3 / 1000;
  struct tm tm3;
  taosLocalTime(&t3, &tm3, NULL, 0, NULL);
  EXPECT_EQ(tm3.tm_mon, 3);  // April
  EXPECT_EQ(tm3.tm_mday, 1);
}

/**
 * Test multi-period year alignment (2 years)
 */
TEST_F(TimeNaturalUnitsTest, MultiPeriodYearAlignment) {
  // Test that 2-year periods align consistently
  // Timestamps within the same 2-year period should align to the same boundary

  int64_t ts1 = makeTimestamp(2026, 3, 15, 0, 0, 0);  // 2026-03-15
  int64_t ts2 = makeTimestamp(2027, 8, 20, 0, 0, 0);  // 2027-08-20

  int64_t result1 = alignToNaturalBoundary(ts1, 'y', 2, 0, TSDB_TIME_PRECISION_MILLI, tz);
  int64_t result2 = alignToNaturalBoundary(ts2, 'y', 2, 0, TSDB_TIME_PRECISION_MILLI, tz);
  std::cout << "Timestamp 1: " << ts1 << ", Aligned 1: " << result1 << std::endl;
  std::cout << "Timestamp 2: " << ts2 << ", Aligned 2: " << result2 << std::endl;

  // Both should align to the same 2-year boundary (2026-01-01)
  EXPECT_EQ(result1, result2);

  // Verify it's 2026-01-01
  time_t    t = result1 / 1000;
  struct tm tm;
  taosLocalTime(&t, &tm, NULL, 0, NULL);
  EXPECT_EQ(tm.tm_year, 126);  // 2026
  EXPECT_EQ(tm.tm_mon, 0);     // January
  EXPECT_EQ(tm.tm_mday, 1);
  EXPECT_EQ(tm.tm_hour, 0);

  // Test that next 2-year period aligns to different boundary
  int64_t ts3 = makeTimestamp(2028, 6, 15, 0, 0, 0);  // 2028-06-15
  int64_t result3 = alignToNaturalBoundary(ts3, 'y', 2, 0, TSDB_TIME_PRECISION_MILLI, tz);
  std::cout << "Timestamp 3: " << ts3 << ", Aligned 3: " << result3 << std::endl;
  EXPECT_NE(result1, result3);

  // Verify next period starts at 2028-01-01
  time_t    t3 = result3 / 1000;
  struct tm tm3;
  taosLocalTime(&t3, &tm3, NULL, 0, NULL);
  EXPECT_EQ(tm3.tm_year, 128);  // 2028
  EXPECT_EQ(tm3.tm_mon, 0);     // January
  EXPECT_EQ(tm3.tm_mday, 1);
}

/**
 * Test microsecond precision
 */
TEST_F(TimeNaturalUnitsTest, MicrosecondPrecision) {
  // Test with microsecond precision
  int64_t ts = makeTimestamp(2026, 3, 10, 15, 30, 0, TSDB_TIME_PRECISION_MICRO);
  int64_t result = alignToNaturalBoundary(ts, 'w', 1, 0, TSDB_TIME_PRECISION_MICRO, tz);

  // Result should be in microseconds
  EXPECT_GT(result, 1000000000000LL);  // Should be > 1 trillion (microseconds)

  // Convert to seconds for verification
  time_t    t = result / 1000000;
  struct tm tm;
  taosLocalTime(&t, &tm, NULL, 0, NULL);

  EXPECT_EQ(tm.tm_wday, 1);  // Monday
  EXPECT_EQ(tm.tm_hour, 0);
  EXPECT_EQ(tm.tm_min, 0);
  EXPECT_EQ(tm.tm_sec, 0);
}

/**
 * Test nanosecond precision
 */
TEST_F(TimeNaturalUnitsTest, NanosecondPrecision) {
  // Test with nanosecond precision
  int64_t ts = makeTimestamp(2026, 3, 10, 15, 30, 0, TSDB_TIME_PRECISION_NANO);
  int64_t result = alignToNaturalBoundary(ts, 'n', 1, 0, TSDB_TIME_PRECISION_NANO, tz);

  // Result should be in nanoseconds
  EXPECT_GT(result, 1000000000000000LL);  // Should be > 1 quadrillion (nanoseconds)

  // Convert to seconds for verification
  time_t    t = result / 1000000000;
  struct tm tm;
  taosLocalTime(&t, &tm, NULL, 0, NULL);

  EXPECT_EQ(tm.tm_mday, 1);  // 1st of month
  EXPECT_EQ(tm.tm_hour, 0);
  EXPECT_EQ(tm.tm_min, 0);
  EXPECT_EQ(tm.tm_sec, 0);
}

/**
 * Test that non-natural units return timestamp as-is
 */
TEST_F(TimeNaturalUnitsTest, NonNaturalUnitsPassthrough) {
  int64_t ts = makeTimestamp(2026, 3, 10, 15, 30, 0);

  // Test with 'd' (day) unit - should return as-is
  int64_t result = alignToNaturalBoundary(ts, 'd', 1, 0, TSDB_TIME_PRECISION_MILLI, tz);
  EXPECT_EQ(result, ts);

  // Test with 'h' (hour) unit - should return as-is
  result = alignToNaturalBoundary(ts, 'h', 1, 0, TSDB_TIME_PRECISION_MILLI, tz);
  EXPECT_EQ(result, ts);
}

/**
 * Test getDuration() function for week unit conversion
 * Verifies that 'w' unit is correctly converted to 7 days
 */
TEST_F(TimeNaturalUnitsTest, GetDurationWeekUnit) {
  int64_t result = 0;
  int32_t code;

  // Test 1 week = 7 days in milliseconds
  code = getDuration(1, 'w', &result, TSDB_TIME_PRECISION_MILLI);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(result, 7LL * 24LL * 60LL * 60LL * 1000LL);  // 7 days in ms

  // Test 2 weeks = 14 days in milliseconds
  code = getDuration(2, 'w', &result, TSDB_TIME_PRECISION_MILLI);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(result, 14LL * 24LL * 60LL * 60LL * 1000LL);  // 14 days in ms

  // Test with microsecond precision
  code = getDuration(1, 'w', &result, TSDB_TIME_PRECISION_MICRO);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(result, 7LL * 24LL * 60LL * 60LL * 1000000LL);  // 7 days in us

  // Test with nanosecond precision
  code = getDuration(1, 'w', &result, TSDB_TIME_PRECISION_NANO);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(result, 7LL * 24LL * 60LL * 60LL * 1000000000LL);  // 7 days in ns

  // Test overflow protection (very large value)
  code = getDuration(INT64_MAX / (7LL * 24LL * 60LL * 60LL * 1000LL) + 1, 'w', &result, TSDB_TIME_PRECISION_MILLI);
  EXPECT_EQ(code, TSDB_CODE_OUT_OF_RANGE);
}

/**
 * Performance test for alignToNaturalBoundary() function
 * Verify that the function executes in < 1ms on average (SC-006)
 */
TEST_F(TimeNaturalUnitsTest, PerformanceAlignToNaturalBoundary) {
  const int iterations = 10000;
  int64_t   ts = makeTimestamp(2026, 3, 10, 15, 30, 0);

  // Test week unit performance
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; i++) {
    alignToNaturalBoundary(ts + i * 1000, 'w', 1, 0, TSDB_TIME_PRECISION_MILLI, tz);
  }
  auto   end = std::chrono::high_resolution_clock::now();
  auto   duration_week = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  double avg_week = duration_week / (double)iterations;

  std::cout << "Week alignment: " << iterations << " iterations in " << duration_week << " us" << std::endl;
  std::cout << "Average time per call: " << avg_week << " us" << std::endl;
  EXPECT_LT(avg_week, 1000.0);  // Should be < 1ms (1000 us)

  // Test month unit performance
  start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; i++) {
    alignToNaturalBoundary(ts + i * 1000, 'n', 1, 0, TSDB_TIME_PRECISION_MILLI, tz);
  }
  end = std::chrono::high_resolution_clock::now();
  auto   duration_month = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  double avg_month = duration_month / (double)iterations;

  std::cout << "Month alignment: " << iterations << " iterations in " << duration_month << " us" << std::endl;
  std::cout << "Average time per call: " << avg_month << " us" << std::endl;
  EXPECT_LT(avg_month, 1000.0);  // Should be < 1ms (1000 us)

  // Test year unit performance
  start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; i++) {
    alignToNaturalBoundary(ts + i * 1000, 'y', 1, 0, TSDB_TIME_PRECISION_MILLI, tz);
  }
  end = std::chrono::high_resolution_clock::now();
  auto   duration_year = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  double avg_year = duration_year / (double)iterations;

  std::cout << "Year alignment: " << iterations << " iterations in " << duration_year << " us" << std::endl;
  std::cout << "Average time per call: " << avg_year << " us" << std::endl;
  EXPECT_LT(avg_year, 1000.0);  // Should be < 1ms (1000 us)
}

/**
 * Performance test for getDuration() function
 * Verify that the function executes in < 1ms on average (SC-006)
 */
TEST_F(TimeNaturalUnitsTest, PerformanceGetDuration) {
  const int iterations = 10000;
  int64_t   result = 0;

  // Test week unit performance
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; i++) {
    getDuration(1 + (i % 100), 'w', &result, TSDB_TIME_PRECISION_MILLI);
  }
  auto   end = std::chrono::high_resolution_clock::now();
  auto   duration_week = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  double avg_week = duration_week / (double)iterations;

  std::cout << "getDuration (week): " << iterations << " iterations in " << duration_week << " us" << std::endl;
  std::cout << "Average time per call: " << avg_week << " us" << std::endl;
  EXPECT_LT(avg_week, 1000.0);  // Should be < 1ms (1000 us)

  // Test day unit performance
  start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; i++) {
    getDuration(1 + (i % 100), 'd', &result, TSDB_TIME_PRECISION_MILLI);
  }
  end = std::chrono::high_resolution_clock::now();
  auto   duration_day = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  double avg_day = duration_day / (double)iterations;

  std::cout << "getDuration (day): " << iterations << " iterations in " << duration_day << " us" << std::endl;
  std::cout << "Average time per call: " << avg_day << " us" << std::endl;
  EXPECT_LT(avg_day, 1000.0);  // Should be < 1ms (1000 us)

  // Test hour unit performance
  start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; i++) {
    getDuration(1 + (i % 100), 'h', &result, TSDB_TIME_PRECISION_MILLI);
  }
  end = std::chrono::high_resolution_clock::now();
  auto   duration_hour = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  double avg_hour = duration_hour / (double)iterations;

  std::cout << "getDuration (hour): " << iterations << " iterations in " << duration_hour << " us" << std::endl;
  std::cout << "Average time per call: " << avg_hour << " us" << std::endl;
  EXPECT_LT(avg_hour, 1000.0);  // Should be < 1ms (1000 us)
}
