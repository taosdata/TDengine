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

extern "C" {
#include "streamTriggerTask.h"
#include "taosdef.h"
#include "taoserror.h"
#include "ttime.h"
}

/**
 * Test suite for stream trigger task window calculation with natural time units
 *
 * Tests the window calculation functions for:
 * - Week unit window calculation and advancement
 * - Month unit window calculation and advancement
 * - Year unit window calculation and advancement
 * - Offset application
 * - Multi-period alignment
 */

// Helper function to create a mock trigger task
static SStreamTriggerTask* createMockTriggerTask(char unit, int64_t interval, int64_t offset, int32_t precision) {
  SStreamTriggerTask* pTask = (SStreamTriggerTask*)taosMemoryCalloc(1, sizeof(SStreamTriggerTask));
  if (!pTask) return NULL;

  pTask->interval.intervalUnit = unit;
  pTask->interval.slidingUnit = unit;  // For period trigger, slidingUnit == intervalUnit
  pTask->interval.interval = 0;        // For period trigger, interval is 0

  // Convert period count to sliding value based on getDuration() logic
  int64_t slidingValue = interval;
  if (unit == 'w') {
    // For week: getDuration converts to time value
    int64_t week_ms = 7LL * 24LL * 60LL * 60LL * 1000LL;
    slidingValue = interval * convertTimePrecision(week_ms, TSDB_TIME_PRECISION_MILLI, precision);
  } else if (unit == 'n' || unit == 'y') {
    // For month/year: getDuration does NOT handle these, sliding stores the count directly
    slidingValue = interval;
  }

  pTask->interval.sliding = slidingValue;
  pTask->interval.offset = offset;
  pTask->interval.offsetUnit = 'a';  // milliseconds
  pTask->interval.precision = precision;
  pTask->interval.timezone = NULL;

  return pTask;
}

// Helper function to free mock trigger task
static void freeMockTriggerTask(SStreamTriggerTask* pTask) {
  if (pTask) {
    taosMemoryFree(pTask);
  }
}

class StreamTriggerTaskTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Test setup
    // Use Asia/Shanghai timezone for consistent testing
    taosSetGlobalTimezone("Asia/Shanghai");
  }

  void TearDown() override {
    // Test cleanup
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

    time_t  t = taosMktime(&tm, NULL);
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

  /**
   * Helper function to verify window boundaries
   * @param win Window to verify
   * @param precision Time precision
   * @param skeyYear Expected skey year (e.g., 2026), -1 to skip
   * @param skeyMonth Expected skey month (1-12), -1 to skip
   * @param skeyDay Expected skey day (1-31), -1 to skip
   * @param skeyHour Expected skey hour (0-23), -1 to skip
   * @param skeyMin Expected skey minute (0-59), -1 to skip
   * @param skeySec Expected skey second (0-59), -1 to skip
   * @param skeyWday Expected skey day of week (0-6, 0=Sunday), -1 to skip
   * @param ekeyYear Expected ekey year (e.g., 2026), -1 to skip
   * @param ekeyMonth Expected ekey month (1-12), -1 to skip
   * @param ekeyDay Expected ekey day (1-31), -1 to skip
   * @param ekeyHour Expected ekey hour (0-23), -1 to skip
   * @param ekeyMin Expected ekey minute (0-59), -1 to skip
   * @param ekeySec Expected ekey second (0-59), -1 to skip
   * @param ekeyWday Expected ekey day of week (0-6, 0=Sunday), -1 to skip
   * @param expectedDurationMs Expected duration in milliseconds (closed interval), -1 to skip
   */
  void verifyWindow(const STimeWindow& win, int8_t precision, int skeyYear = -1, int skeyMonth = -1, int skeyDay = -1,
                    int skeyHour = -1, int skeyMin = -1, int skeySec = -1, int skeyWday = -1, int ekeyYear = -1,
                    int ekeyMonth = -1, int ekeyDay = -1, int ekeyHour = -1, int ekeyMin = -1, int ekeySec = -1,
                    int ekeyWday = -1, int64_t expectedDurationMs = -1) {
    // Convert precision factor
    int64_t precisionFactor = 1;
    switch (precision) {
      case TSDB_TIME_PRECISION_MILLI:
        precisionFactor = 1000LL;
        break;
      case TSDB_TIME_PRECISION_MICRO:
        precisionFactor = 1000000LL;
        break;
      case TSDB_TIME_PRECISION_NANO:
        precisionFactor = 1000000000LL;
        break;
    }

    // Verify skey
    time_t    t_skey = win.skey / precisionFactor;
    struct tm tm_skey;
    taosLocalTime(&t_skey, &tm_skey, NULL, 0, NULL);

    if (skeyYear >= 0) EXPECT_EQ(tm_skey.tm_year, skeyYear - 1900);
    if (skeyMonth >= 0) EXPECT_EQ(tm_skey.tm_mon, skeyMonth - 1);
    if (skeyDay >= 0) EXPECT_EQ(tm_skey.tm_mday, skeyDay);
    if (skeyHour >= 0) EXPECT_EQ(tm_skey.tm_hour, skeyHour);
    if (skeyMin >= 0) EXPECT_EQ(tm_skey.tm_min, skeyMin);
    if (skeySec >= 0) EXPECT_EQ(tm_skey.tm_sec, skeySec);
    if (skeyWday >= 0) EXPECT_EQ(tm_skey.tm_wday, skeyWday);

    // Verify ekey
    time_t    t_ekey = win.ekey / precisionFactor;
    struct tm tm_ekey;
    taosLocalTime(&t_ekey, &tm_ekey, NULL, 0, NULL);

    if (ekeyYear >= 0) EXPECT_EQ(tm_ekey.tm_year, ekeyYear - 1900);
    if (ekeyMonth >= 0) EXPECT_EQ(tm_ekey.tm_mon, ekeyMonth - 1);
    if (ekeyDay >= 0) EXPECT_EQ(tm_ekey.tm_mday, ekeyDay);
    if (ekeyHour >= 0) EXPECT_EQ(tm_ekey.tm_hour, ekeyHour);
    if (ekeyMin >= 0) EXPECT_EQ(tm_ekey.tm_min, ekeyMin);
    if (ekeySec >= 0) EXPECT_EQ(tm_ekey.tm_sec, ekeySec);
    if (ekeyWday >= 0) EXPECT_EQ(tm_ekey.tm_wday, ekeyWday);

    // Verify duration (closed interval: ekey - skey + 1)
    if (expectedDurationMs >= 0) {
      int64_t duration = win.ekey - win.skey + 1;
      int64_t expectedDuration = expectedDurationMs * (precisionFactor / 1000LL);
      EXPECT_EQ(duration, expectedDuration);
    }
  }
};

/**
 * Test week unit window calculation
 * Verify that window aligns to Monday 00:00:00
 */
TEST_F(StreamTriggerTaskTest, WeekWindowCalculation) {
  // Create task with PERIOD(1w)
  SStreamTriggerTask* pTask = createMockTriggerTask('w', 1, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Test timestamp: 2026-03-10 15:30:00 (Tuesday)
  int64_t ts = makeTimestamp(2026, 3, 10, 15, 30, 0);

  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Window should be [2026-03-09 00:00:00 Monday, 2026-03-16 00:00:00 Monday]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2026, 3, 9, 0, 0, 0, 1,  // skey: 2026-03-09 00:00:00 Monday
               2026, 3, 16, 0, 0, 0, 1,                                 // ekey: 2026-03-16 00:00:00 Monday
               7LL * 24LL * 60LL * 60LL * 1000LL);                      // 7 days

  freeMockTriggerTask(pTask);
}

/**
 * Test month unit window calculation
 * Verify that window aligns to 1st of month 00:00:00
 */
TEST_F(StreamTriggerTaskTest, MonthWindowCalculation) {
  // Create task with PERIOD(1n)
  SStreamTriggerTask* pTask = createMockTriggerTask('n', 1, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Test timestamp: 2026-03-15 12:00:00
  int64_t ts = makeTimestamp(2026, 3, 15, 12, 0, 0);

  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Window should be [2026-03-01 00:00:00, 2026-04-01 00:00:00]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2026, 3, 1, 0, 0, 0, -1,  // skey: 2026-03-01 00:00:00
               2026, 4, 1, 0, 0, 0, -1);                                 // ekey: 2026-04-01 00:00:00

  freeMockTriggerTask(pTask);
}

/**
 * Test year unit window calculation
 * Verify that window aligns to Jan 1st 00:00:00
 */
TEST_F(StreamTriggerTaskTest, YearWindowCalculation) {
  // Create task with PERIOD(1y)
  SStreamTriggerTask* pTask = createMockTriggerTask('y', 1, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Test timestamp: 2026-06-15 12:00:00
  int64_t ts = makeTimestamp(2026, 6, 15, 12, 0, 0);

  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Window should be [2026-01-01 00:00:00, 2027-01-01 00:00:00]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2026, 1, 1, 0, 0, 0, -1,  // skey: 2026-01-01 00:00:00
               2027, 1, 1, 0, 0, 0, -1);                                 // ekey: 2027-01-01 00:00:00

  freeMockTriggerTask(pTask);
}

/**
 * Test window advancement for week unit
 * Verify that next window's skey = current window's ekey + 1
 */
TEST_F(StreamTriggerTaskTest, WeekWindowAdvancement) {
  // Create task with PERIOD(1w)
  SStreamTriggerTask* pTask = createMockTriggerTask('w', 1, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Get initial window
  int64_t     ts = makeTimestamp(2026, 3, 10, 15, 30, 0);  // 2026-03-10 15:30:00 (Tuesday)
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Verify first window: [2026-03-09 00:00:00 Monday, 2026-03-16 00:00:00 Monday]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2026, 3, 9, 0, 0, 0, 1,  // skey: 2026-03-09 00:00:00 Monday
               2026, 3, 16, 0, -1, -1, 1,                               // ekey: 2026-03-16 00:00:00 Monday
               7LL * 24LL * 60LL * 60LL * 1000LL);                      // 7 days

  int64_t firstEkey = win.ekey;

  // Advance to next window
  stTriggerTaskNextTimeWindow(pTask, &win);

  // Next window's skey should equal previous window's ekey + 1 (closed interval)
  EXPECT_EQ(win.skey, firstEkey + 1);

  // Verify second window: [2026-03-16 00:00:00 Monday, 2026-03-23 00:00:00 Monday]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2026, 3, 16, 0, 0, 0, 1,  // skey: 2026-03-16 00:00:00 Monday
               2026, 3, 23, 0, 0, 0, 1,                                  // ekey: 2026-03-23 00:00:00 Monday
               7LL * 24LL * 60LL * 60LL * 1000LL);                       // 7 days

  freeMockTriggerTask(pTask);
}

/**
 * Test window advancement for month unit
 * Verify correct handling of variable month lengths
 */
TEST_F(StreamTriggerTaskTest, MonthWindowAdvancement) {
  // Create task with PERIOD(1n)
  SStreamTriggerTask* pTask = createMockTriggerTask('n', 1, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Start with January (31 days)
  int64_t     ts = makeTimestamp(2025, 1, 2, 0, 0, 0);  // 2025-01-02 00:00:00
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Verify first window: [2025-01-01 00:00:00, 2025-02-01 00:00:00]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2025, 1, 1, 0, 0, 0, -1,  // skey: 2025-01-01 00:00:00
               2025, 2, 1, 0, 0, 0, -1,                                  // ekey: 2025-02-01 00:00:00
               31LL * 24LL * 60LL * 60LL * 1000LL);                      // January has 31 days

  int64_t janEkey = win.ekey;

  // Advance to February
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_EQ(win.skey, janEkey + 1);

  // Verify second window: [2025-02-01 00:00:00, 2025-03-01 00:00:00]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2025, 2, 1, 0, 0, 0, -1,  // skey: 2025-02-01 00:00:00
               2025, 3, 1, 0, 0, 0, -1,                                  // ekey: 2025-03-01 00:00:00
               28LL * 24LL * 60LL * 60LL * 1000LL);                      // February has 28 days

  freeMockTriggerTask(pTask);
}

/**
 * Test offset application for week unit
 * Verify that window time = natural boundary + offset
 */
TEST_F(StreamTriggerTaskTest, WeekWindowWithOffset) {
  // Create task with PERIOD(1w, 1d) - trigger on Tuesday
  int64_t             oneDayMs = 24LL * 60LL * 60LL * 1000LL;
  SStreamTriggerTask* pTask = createMockTriggerTask('w', 1, oneDayMs, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Test timestamp: 2026-03-10 15:30:00 (Tuesday)
  int64_t ts = makeTimestamp(2026, 3, 10, 15, 30, 0);

  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Window should be [2026-03-10 00:00:00 Tuesday, 2026-03-17 00:00:00 Tuesday]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2026, 3, 10, 0, 0, 0, 2,  // skey: 2026-03-10 00:00:00 Tuesday
               2026, 3, 17, 0, 0, 0, 2);                                 // ekey: 2026-03-17 00:00:00 Tuesday

  freeMockTriggerTask(pTask);
}

/**
 * Test multi-period week alignment (2 weeks)
 * Verify epoch-based alignment
 */
TEST_F(StreamTriggerTaskTest, MultiPeriodWeekAlignment) {
  // Create task with PERIOD(2w)
  SStreamTriggerTask* pTask = createMockTriggerTask('w', 2, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Two timestamps in different weeks but same 2-week period should align to the same boundary
  // Use timestamps that are 7 days apart (1 week) within the same 2-week period
  int64_t ts1 = makeTimestamp(2026, 3, 03, 0, 0, 0);  // 2026-03-03 (Tuesday, week 1)
  int64_t ts2 = makeTimestamp(2026, 3, 10, 0, 0, 0);  // 2026-03-10 (Tuesday, week 2)

  STimeWindow win1 = stTriggerTaskGetTimeWindow(pTask, ts1);
  STimeWindow win2 = stTriggerTaskGetTimeWindow(pTask, ts2);

  // They are in the same 2-week period
  EXPECT_EQ(win1.ekey, win2.ekey);

  // Window should be [2026-03-02 00:00:00 Monday, 2026-03-16 00:00:00 Monday]
  verifyWindow(win1, TSDB_TIME_PRECISION_MILLI, 2026, 3, 2, 0, 0, 0, 1,  // skey: 2026-03-02 00:00:00 Monday
               2026, 3, 16, 0, 0, 0, 1,                                  // ekey: 2026-03-16 00:00:00 Monday
               14LL * 24LL * 60LL * 60LL * 1000LL);                      // 14 days

  freeMockTriggerTask(pTask);
}

/**
 * Test multi-period month alignment (3 months)
 * Verify epoch-based alignment for quarterly periods
 */
TEST_F(StreamTriggerTaskTest, MultiPeriodMonthAlignment) {
  // Create task with PERIOD(3n) - quarterly
  SStreamTriggerTask* pTask = createMockTriggerTask('n', 3, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Test timestamp in Q1 2026
  int64_t ts = makeTimestamp(2026, 2, 1, 0, 0, 0);  // 2026-02-01 00:00:00

  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Window should be [2026-01-01 00:00:00, 2026-04-01 00:00:00]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2026, 1, 1, 0, 0, 0, -1,  // skey: 2026-01-01 00:00:00
               2026, 4, 1, 0, 0, 0, -1);                                 // ekey: 2026-04-01 00:00:00

  freeMockTriggerTask(pTask);
}

/**
 * Test microsecond precision
 */
TEST_F(StreamTriggerTaskTest, MicrosecondPrecision) {
  // Create task with PERIOD(1w) in microsecond precision
  SStreamTriggerTask* pTask = createMockTriggerTask('w', 1, 0, TSDB_TIME_PRECISION_MICRO);
  ASSERT_NE(pTask, nullptr);

  // Test timestamp in microseconds
  int64_t ts = makeTimestamp(2026, 3, 10, 15, 30, 0, TSDB_TIME_PRECISION_MICRO);  // 2026-03-10 15:30:00 in microseconds

  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Result should be in microseconds
  EXPECT_GT(win.skey, 1000000000000LL);  // Should be > 1 trillion (microseconds)
  EXPECT_GT(win.ekey, 1000000000000LL);

  // Window should be [2026-03-09 00:00:00, 2026-03-16 00:00:00] in microseconds
  verifyWindow(win, TSDB_TIME_PRECISION_MICRO, 2026, 3, 9, 0, 0, 0, -1,  // skey: 2026-03-09 00:00:00
               2026, 3, 16, 0, 0, 0, -1,                                 // ekey: 2026-03-16 00:00:00
               7LL * 24LL * 60LL * 60LL * 1000LL);                       // 7 days (in ms, will be converted)

  freeMockTriggerTask(pTask);
}

/**
 * Test year window advancement
 * Verify correct handling of leap years
 */
TEST_F(StreamTriggerTaskTest, YearWindowAdvancement) {
  // Create task with PERIOD(1y)
  SStreamTriggerTask* pTask = createMockTriggerTask('y', 1, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Start with 2024 (leap year)
  int64_t     ts = makeTimestamp(2024, 1, 2, 0, 0, 0);  // 2024-01-01 00:00:00
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Verify first window: [2024-01-01 00:00:00, 2025-01-01 00:00:00]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2024, 1, 1, 0, 0, 0, -1,  // skey: 2024-01-01 00:00:00
               2025, 1, 1, 0, -1, -1, -1,                                // ekey: 2025-01-01 00:00:00
               366LL * 24LL * 60LL * 60LL * 1000LL);                     // 2024 is leap year (366 days)

  stTriggerTaskNextTimeWindow(pTask, &win);

  // Verify second window: [2025-01-01 00:00:00, 2026-01-01 00:00:00]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2025, 1, 1, 0, 0, 0, -1,  // skey: 2025-01-01 00:00:00
               2026, 1, 1, 0, 0, 0, -1,                                  // ekey: 2026-01-01 00:00:00
               365LL * 24LL * 60LL * 60LL * 1000LL);                     // 2025 is normal year (365 days)

  freeMockTriggerTask(pTask);
}

/**
 * Test leap year February 29th handling
 * Verify correct window calculation for leap year boundary
 */
TEST_F(StreamTriggerTaskTest, LeapYearFebruary29) {
  // Create task with PERIOD(1n)
  SStreamTriggerTask* pTask = createMockTriggerTask('n', 1, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Test timestamp: 2024-02-29 12:00:00 (leap year)
  int64_t ts = makeTimestamp(2024, 2, 29, 12, 0, 0);

  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Window should be [2024-02-01 00:00:00, 2024-03-01 00:00:00]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2024, 2, 1, 0, 0, 0, -1,  // skey: 2024-02-01 00:00:00
               2024, 3, 1, 0, 0, 0, -1,                                  // ekey: 2024-03-01 00:00:00
               29LL * 24LL * 60LL * 60LL * 1000LL);                      // February 2024 has 29 days

  freeMockTriggerTask(pTask);
}

/**
 * Test month boundary transitions (small to large month)
 * Verify correct handling of February to March transition
 */
TEST_F(StreamTriggerTaskTest, MonthBoundarySmallToLarge) {
  // Create task with PERIOD(1n)
  SStreamTriggerTask* pTask = createMockTriggerTask('n', 1, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Start with February 2025 (28 days, non-leap year)
  int64_t     ts = makeTimestamp(2025, 2, 2, 0, 0, 0);  // 2025-02-02 00:00:00
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Verify February window: [2025-02-01 00:00:00, 2025-03-01 00:00:00]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2025, 2, 1, 0, 0, 0, -1,  // skey: 2025-02-01 00:00:00
               2025, 3, 1, 0, 0, 0, -1,                                  // ekey: 2025-03-01 00:00:00
               28LL * 24LL * 60LL * 60LL * 1000LL);                      // February has 28 days

  int64_t febEkey = win.ekey;

  // Advance to March (31 days)
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_EQ(win.skey, febEkey + 1);

  // Verify March window: [2025-03-01 00:00:00, 2025-04-01 00:00:00]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2025, 3, 1, 0, 0, 0, -1,  // skey: 2025-03-01 00:00:00
               2025, 4, 1, 0, 0, 0, -1,                                  // ekey: 2025-04-01 00:00:00
               31LL * 24LL * 60LL * 60LL * 1000LL);                      // March has 31 days

  freeMockTriggerTask(pTask);
}

/**
 * Test month boundary transitions (large to small month)
 * Verify correct handling of January to February transition
 */
TEST_F(StreamTriggerTaskTest, MonthBoundaryLargeToSmall) {
  // Create task with PERIOD(1n)
  SStreamTriggerTask* pTask = createMockTriggerTask('n', 1, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Start with January 2025 (31 days)
  int64_t     ts = makeTimestamp(2025, 1, 2, 0, 0, 0);  // 2025-01-01 00:00:00
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Verify January window: [2025-01-01 00:00:00, 2025-02-01 00:00:00]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2025, 1, 1, 0, 0, 0, -1,  // skey: 2025-01-01 00:00:00
               2025, 2, 1, 0, 0, 0, -1,                                  // ekey: 2025-02-01 00:00:00
               31LL * 24LL * 60LL * 60LL * 1000LL);                      // January has 31 days

  int64_t janEkey = win.ekey;

  // Advance to February (28 days, non-leap year)
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_EQ(win.skey, janEkey + 1);

  // Verify February window: [2025-02-01 00:00:00, 2025-03-01 00:00:00]
  verifyWindow(win, TSDB_TIME_PRECISION_MILLI, 2025, 2, 1, 0, 0, 0, -1,  // skey: 2025-02-01 00:00:00
               2025, 3, 1, 0, 0, 0, -1,                                  // ekey: 2025-03-01 00:00:00
               28LL * 24LL * 60LL * 60LL * 1000LL);                      // February has 28 days

  freeMockTriggerTask(pTask);
}

/**
 * Test multi-period epoch alignment verification
 * Verify that 2-week periods align consistently across different timestamps
 */
TEST_F(StreamTriggerTaskTest, EpochAlignmentVerification) {
  // Create task with PERIOD(2w)
  SStreamTriggerTask* pTask = createMockTriggerTask('w', 2, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Test multiple timestamps across different 2-week periods
  // All should align to epoch-based 2-week boundaries

  // Timestamp 1: 2026-01-06 (week 1 of 2026)
  int64_t     ts1 = makeTimestamp(2026, 1, 6, 0, 0, 0);
  STimeWindow win1 = stTriggerTaskGetTimeWindow(pTask, ts1);

  // Timestamp 2: 2026-01-20 (week 3 of 2026, should be in next 2-week period)
  int64_t     ts2 = makeTimestamp(2026, 1, 20, 0, 0, 0);
  STimeWindow win2 = stTriggerTaskGetTimeWindow(pTask, ts2);

  // Windows should be different (different 2-week periods)
  EXPECT_NE(win1.skey, win2.skey);

  // But win2.skey should equal win1.ekey + 1 (continuous, closed interval)
  EXPECT_EQ(win2.skey, win1.ekey + 1);

  // Window 1 should be [2026-01-05 00:00:00 Monday, 2026-01-19 00:00:00 Monday]
  // Window 2 should be [2026-01-19 00:00:00 Monday, 2026-02-02 00:00:00 Monday]
  verifyWindow(win1, TSDB_TIME_PRECISION_MILLI, 2026, 1, 5, 0, 0, 0, 1,  // skey: 2026-01-05 00:00:00 Monday
               2026, 1, 19, 0, 0, 0, 1,                                  // ekey: 2026-01-19 00:00:00 Monday
               14LL * 24LL * 60LL * 60LL * 1000LL);                      // 14 days

  verifyWindow(win2, TSDB_TIME_PRECISION_MILLI, 2026, 1, 19, 0, 0, 0, 1,  // skey: 2026-01-19 00:00:00 Monday
               2026, 2, 2, 0, 0, 0, 1,                                    // ekey: 2026-02-02 00:00:00 Monday
               14LL * 24LL * 60LL * 60LL * 1000LL);                       // 14 days

  freeMockTriggerTask(pTask);
}

/**
 * Test daylight saving time transition (if applicable)
 * Note: This test assumes server timezone observes DST
 * The trigger time should remain at 00:00:00 local time regardless of DST
 */
TEST_F(StreamTriggerTaskTest, DaylightSavingTimeTransition) {
  // Create task with PERIOD(1w)
  SStreamTriggerTask* pTask = createMockTriggerTask('w', 1, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Test timestamp during DST transition period (example: March 2026 in US)
  // Note: Actual DST dates vary by timezone
  // This test verifies that mktime() handles DST correctly

  // Before DST: 2026-03-02 (Monday before DST starts)
  int64_t     ts1 = makeTimestamp(2026, 3, 2, 0, 0, 0);
  STimeWindow win1 = stTriggerTaskGetTimeWindow(pTask, ts1);

  // After DST: 2026-03-16 (Monday after DST starts)
  int64_t     ts2 = makeTimestamp(2026, 3, 16, 0, 0, 0);
  STimeWindow win2 = stTriggerTaskGetTimeWindow(pTask, ts2);

  // Both windows should align to Monday 00:00:00 local time
  verifyWindow(win1, TSDB_TIME_PRECISION_MILLI, 2026, -1, -1, 0, -1, -1, 1,  // skey: 2026 Monday 00:00:00
               -1, -1, -1, -1, -1, -1, -1);

  verifyWindow(win2, TSDB_TIME_PRECISION_MILLI, 2026, -1, -1, 0, -1, -1, 1,  // skey: 2026 Monday 00:00:00
               -1, -1, -1, -1, -1, -1, -1);

  freeMockTriggerTask(pTask);
}

/**
 * Test multi-period year alignment (2 years)
 * Verify epoch-based alignment for biennial periods
 */
TEST_F(StreamTriggerTaskTest, MultiPeriodYearAlignment) {
  // Create task with PERIOD(2y)
  SStreamTriggerTask* pTask = createMockTriggerTask('y', 2, 0, TSDB_TIME_PRECISION_MILLI);
  ASSERT_NE(pTask, nullptr);

  // Test timestamp in 2024 (even year from epoch 1970)
  int64_t     ts1 = makeTimestamp(2024, 1, 1, 0, 0, 0);  // 2024-01-01 00:00:00
  STimeWindow win1 = stTriggerTaskGetTimeWindow(pTask, ts1);

  // Test timestamp in 2025 (odd year from epoch 1970)
  int64_t     ts2 = makeTimestamp(2025, 1, 1, 0, 0, 0);  // 2025-01-01 00:00:00
  STimeWindow win2 = stTriggerTaskGetTimeWindow(pTask, ts2);

  // 2024 and 2025 are in different 2-year periods
  EXPECT_NE(win1.skey, win2.skey);
  EXPECT_NE(win1.ekey, win2.ekey);

  // win2.skey should equal win1.ekey + 1 (consecutive windows)
  EXPECT_EQ(win2.skey, win1.ekey + 1);

  // Window 1 should be [2022-01-01 00:00:00, 2024-01-01 00:00:00]
  // Window 2 should be [2024-01-01 00:00:00, 2026-01-01 00:00:00]
  verifyWindow(win1, TSDB_TIME_PRECISION_MILLI, 2022, 1, 1, 0, 0, 0, -1,  // skey: 2022-01-01 00:00:00
               2024, 1, 1, 0, 0, 0, -1);                                  // ekey: 2024-01-01 00:00:00

  verifyWindow(win2, TSDB_TIME_PRECISION_MILLI, 2024, 1, 1, 0, 0, 0, -1,  // skey: 2024-01-01 00:00:00
               2026, 1, 1, 0, 0, 0, -1);                                  // ekey: 2026-01-01 00:00:00

  freeMockTriggerTask(pTask);
}
