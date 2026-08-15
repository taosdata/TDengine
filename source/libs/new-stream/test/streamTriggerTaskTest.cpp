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

#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <string>
#include <vector>

#include "stub.h"
#include "trpc.h"

extern "C" {
#include "dataSink.h"
#include "streamInt.h"
#include "streamRecalcTracker.h"
#include "streamTriggerTask.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tdatablock.h"
#include "ttime.h"

int32_t stmAddPeriodReport(int64_t streamId, SArray** ppReport, SStreamTriggerTask* triggerTask);
void    tFreeSSTriggerRuntimeStatus(void* param);
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

namespace {

constexpr int64_t kRealtimeSessionId = 1;
constexpr int64_t kHistorySessionId = 2;
constexpr int64_t kCreateTableGroupId = 42;
constexpr int64_t kRunnerTaskId = 0x200;

int32_t compareRealtimeMaxDelayGroups(const HeapNode* lhs, const HeapNode* rhs);

class StreamTriggerObservabilityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    task_.task.type = STREAM_TRIGGER_TASK;
    taosInitRWLatch(&task_.readerProgressLock);
    task_.pReaderProgressSnapshots = taosArrayInit(0, sizeof(SStreamReaderProgressSnapshot));
    ASSERT_NE(task_.pReaderProgressSnapshots, nullptr);
    ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK,
                                STREAM_METRIC_LOGICAL_INPUT | STREAM_METRIC_REALTIME_LAG |
                                    STREAM_METRIC_HISTORY_PROGRESS | STREAM_METRIC_RECALCULATES,
                                0, 1000, &task_.pStats),
              TSDB_CODE_SUCCESS);
    ASSERT_EQ(stRecalcTrackerCreate(&task_.pRecalcTracker), TSDB_CODE_SUCCESS);
  }

  void TearDown() override {
    taosArrayDestroy(task_.pReaderProgressSnapshots);
    task_.pReaderProgressSnapshots = nullptr;
    stRecalcTrackerDestroy(&task_.pRecalcTracker);
    stTaskStatsDestroy(&task_.pStats);
  }

  void AddReaderProgress(int64_t taskId, int32_t nodeId, int64_t verTime) {
    const SStreamReaderProgressSnapshot snapshot = {
        .taskId = taskId,
        .nodeId = nodeId,
        .verTime = verTime,
    };
    ASSERT_NE(taosArrayPush(task_.pReaderProgressSnapshots, &snapshot), nullptr);
  }

  SStreamTriggerTask task_ = {};
};

SArray* gFailedTriggerStatusArray = nullptr;

void* failTriggerStatusArrayAddBatch(SArray* pArray, const void* pData, int32_t nEles) {
  if (pArray == gFailedTriggerStatusArray) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  if (pArray == nullptr || pData == nullptr || nEles < 0 ||
      taosArrayEnsureCap(pArray, pArray->size + nEles) != TSDB_CODE_SUCCESS) {
    return nullptr;
  }
  void* pTarget = TARRAY_GET_ELEM(pArray, pArray->size);
  std::memcpy(pTarget, pData, pArray->elemSize * nEles);
  pArray->size += nEles;
  return pTarget;
}

class TriggerStatusPushFailureGuard {
 public:
  explicit TriggerStatusPushFailureGuard(SArray* pFailedArray) {
    gFailedTriggerStatusArray = pFailedArray;
    addBatchStub_.set(taosArrayAddBatch, failTriggerStatusArrayAddBatch);
  }

  ~TriggerStatusPushFailureGuard() {
    addBatchStub_.reset(taosArrayAddBatch);
    gFailedTriggerStatusArray = nullptr;
  }

 private:
  Stub addBatchStub_;
};

int32_t gNonNullDestroyAfterInitFailure = 0;

SArray* failTriggerStatusArrayInit(size_t, size_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

void trackDestroyAfterInitFailure(SArray* pArray) {
  if (pArray != nullptr) ++gNonNullDestroyAfterInitFailure;
}

class TriggerStatusInitFailureGuard {
 public:
  TriggerStatusInitFailureGuard() {
    gNonNullDestroyAfterInitFailure = 0;
    initStub_.set(taosArrayInit, failTriggerStatusArrayInit);
    destroyStub_.set(taosArrayDestroy, trackDestroyAfterInitFailure);
  }

  ~TriggerStatusInitFailureGuard() {
    destroyStub_.reset(taosArrayDestroy);
    initStub_.reset(taosArrayInit);
  }

 private:
  Stub initStub_;
  Stub destroyStub_;
};

TEST_F(StreamTriggerObservabilityTest, RealtimeLagUsesOldestValidReaderTimeAndDeduplicatesFuture) {
  AddReaderProgress(11, 1, 9000000);
  AddReaderProgress(12, 2, 7000000);
  AddReaderProgress(13, 3, 11000000);

  ASSERT_EQ(stTriggerTaskRefreshRealtimeLag(&task_, 10000), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stTriggerTaskRefreshRealtimeLag(&task_, 10000), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsSnapshot metrics = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(task_.pStats, streamTaskGetMonotonicUs(), &metrics), TSDB_CODE_SUCCESS);
  EXPECT_NE(metrics.validMask & STREAM_METRIC_REALTIME_LAG, 0U);
  EXPECT_EQ(metrics.realtimeLagMs, 3000);

  SStreamTaskPeriodSnapshot period = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(task_.pStats, STREAM_STATS_PERIOD_US, &period, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(period.period.trigger.invalidWalTimeCount, 1U);
}

TEST_F(StreamTriggerObservabilityTest, RealtimeLagIgnoresMissingTimeAndExternalSourceInvalidatesLag) {
  AddReaderProgress(11, 1, 0);
  AddReaderProgress(12, 2, -1);
  AddReaderProgress(13, 3, 8000000);

  ASSERT_EQ(stTriggerTaskRefreshRealtimeLag(&task_, 10000), TSDB_CODE_SUCCESS);
  SStreamTaskMetricsSnapshot metrics = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(task_.pStats, streamTaskGetMonotonicUs(), &metrics), TSDB_CODE_SUCCESS);
  EXPECT_NE(metrics.validMask & STREAM_METRIC_REALTIME_LAG, 0U);
  EXPECT_EQ(metrics.realtimeLagMs, 2000);

  task_.task.flags |= STREAM_FLAG_REF_EXT_SOURCE;
  ASSERT_EQ(stTriggerTaskRefreshRealtimeLag(&task_, 10000), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stTaskStatsSnapshot1m(task_.pStats, streamTaskGetMonotonicUs(), &metrics), TSDB_CODE_SUCCESS);
  EXPECT_EQ(metrics.validMask & STREAM_METRIC_REALTIME_LAG, 0U);
}

TEST_F(StreamTriggerObservabilityTest, MetricsGetterRefreshesRealtimeLagFromCurrentWallTime) {
  const int64_t nowMs = taosGetTimestampMs();
  AddReaderProgress(11, 1, (nowMs - 3000) * 1000);
  stTaskStatsSetRealtimeLag(task_.pStats, true, 1);

  SStreamTaskMetricsSnapshot metrics = {};
  ASSERT_EQ(stTriggerTaskGetMetrics(&task_, &metrics), TSDB_CODE_SUCCESS);
  EXPECT_GE(metrics.realtimeLagMs, 3000);
  EXPECT_LT(metrics.realtimeLagMs, 10000);
  taosArrayDestroy(metrics.pRecalculates);
}

TEST_F(StreamTriggerObservabilityTest, LegacyAndTypedSnapshotsShareProgressButOwnTheirArrays) {
  ASSERT_EQ(stRecalcTrackerInitHistory(task_.pRecalcTracker, true, {100, 200}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerConfirmHistoryPrefix(task_.pRecalcTracker, 150), TSDB_CODE_SUCCESS);
  SArray* groups = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(groups, nullptr);
  const int64_t groupId = 1;
  ASSERT_NE(taosArrayPush(groups, &groupId), nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(task_.pRecalcTracker, 42, {100, 200}, groups), TSDB_CODE_SUCCESS);
  taosArrayDestroy(groups);
  ASSERT_EQ(stRecalcTrackerMarkJobRunning(task_.pRecalcTracker, 42), TSDB_CODE_SUCCESS);

  SSTriggerRuntimeStatus     legacy = {};
  SStreamTaskMetricsSnapshot typed = {};
  ASSERT_EQ(stTriggerTaskGetStatus(&task_.task, &legacy), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stTriggerTaskGetMetrics(&task_, &typed), TSDB_CODE_SUCCESS);

  EXPECT_EQ(legacy.histroyProgress, 50);
  EXPECT_TRUE(typed.historyProgressValid);
  EXPECT_EQ(legacy.histroyProgress, typed.historyProgressPct);
  EXPECT_NE(typed.validMask & STREAM_METRIC_HISTORY_PROGRESS, 0U);
  EXPECT_NE(typed.validMask & STREAM_METRIC_RECALCULATES, 0U);
  ASSERT_NE(legacy.userRecalcs, nullptr);
  ASSERT_NE(typed.pRecalculates, nullptr);
  ASSERT_EQ(taosArrayGetSize(legacy.userRecalcs), 1);
  ASSERT_EQ(taosArrayGetSize(typed.pRecalculates), 1);
  auto* legacyRecalc = static_cast<SSTriggerRecalcProgress*>(taosArrayGet(legacy.userRecalcs, 0));
  auto* typedRecalc = static_cast<SStreamRecalcSnapshot*>(taosArrayGet(typed.pRecalculates, 0));
  ASSERT_NE(legacyRecalc, nullptr);
  ASSERT_NE(typedRecalc, nullptr);
  EXPECT_EQ(legacyRecalc->recalcId, 42);
  EXPECT_EQ(legacyRecalc->progress, 0);
  EXPECT_EQ(typedRecalc->status, STREAM_RECALC_STATUS_RUNNING);
  legacyRecalc->progress = 99;
  EXPECT_EQ(typedRecalc->progressPct, 0);

  taosArrayDestroy(legacy.userRecalcs);
  taosArrayDestroy(typed.pRecalculates);
}

TEST_F(StreamTriggerObservabilityTest, PeriodReportPushFailureDestroysOwnedLegacySnapshot) {
  SArray* groups = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(groups, nullptr);
  const int64_t groupId = 1;
  ASSERT_NE(taosArrayPush(groups, &groupId), nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(task_.pRecalcTracker, 42, {100, 200}, groups), TSDB_CODE_SUCCESS);
  taosArrayDestroy(groups);

  SArray* reports = taosArrayInit(1, sizeof(SSTriggerRuntimeStatus));
  ASSERT_NE(reports, nullptr);
  {
    TriggerStatusPushFailureGuard guard(reports);
    EXPECT_EQ(stmAddPeriodReport(1, &reports, &task_), TSDB_CODE_OUT_OF_MEMORY);
  }
  EXPECT_EQ(taosArrayGetSize(reports), 0);
  taosArrayDestroy(reports);
}

TEST_F(StreamTriggerObservabilityTest, PeriodReportInitFailureDoesNotDestroyUninitializedSnapshot) {
  SArray* reports = nullptr;
  ASSERT_EQ(stmAddPeriodReport(1, &reports, &task_), TSDB_CODE_SUCCESS);
  taosArrayDestroyEx(reports, tFreeSSTriggerRuntimeStatus);
  reports = nullptr;
  {
    TriggerStatusInitFailureGuard guard;
    EXPECT_EQ(stmAddPeriodReport(1, &reports, &task_), TSDB_CODE_OUT_OF_MEMORY);
    EXPECT_EQ(gNonNullDestroyAfterInitFailure, 0);
  }
  EXPECT_EQ(reports, nullptr);
}

TEST_F(StreamTriggerObservabilityTest, ReaderProgressCopyHasIndependentOwnership) {
  AddReaderProgress(11, 1, 9000000);

  SArray* copy = nullptr;
  ASSERT_EQ(stTriggerTaskCopyReaderProgress(&task_, &copy), TSDB_CODE_SUCCESS);
  ASSERT_NE(copy, nullptr);
  ASSERT_EQ(taosArrayGetSize(copy), 1);
  auto* copied = static_cast<SStreamReaderProgressSnapshot*>(taosArrayGet(copy, 0));
  ASSERT_NE(copied, nullptr);
  EXPECT_EQ(copied->taskId, 11);
  copied->verTime = 1;
  const auto* original =
      static_cast<const SStreamReaderProgressSnapshot*>(taosArrayGet(task_.pReaderProgressSnapshots, 0));
  ASSERT_NE(original, nullptr);
  EXPECT_EQ(original->verTime, 9000000);
  taosArrayDestroy(copy);
}

TEST_F(StreamTriggerObservabilityTest, ReaderProgressCopyAfterCleanupReturnsDeterministicError) {
  taosArrayDestroy(task_.pReaderProgressSnapshots);
  task_.pReaderProgressSnapshots = nullptr;
  terrno = TSDB_CODE_SUCCESS;

  SArray* copy = reinterpret_cast<SArray*>(0x1);
  EXPECT_EQ(stTriggerTaskCopyReaderProgress(&task_, &copy), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(copy, nullptr);
}

struct TriggerRetryState {
  int32_t sendCalls = 0;
  bool    failSend = false;
};

TriggerRetryState gTriggerRetryState;

int32_t captureSuccessfulTriggerRetry(const SEpSet*, SRpcMsg* pMsg) {
  ++gTriggerRetryState.sendCalls;
  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = nullptr;
  if (gTriggerRetryState.failSend) {
    return TSDB_CODE_FAILED;
  }
  destroyAhandle(pMsg->info.ahandle);
  pMsg->info.ahandle = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t releaseTriggerRetryRequestForCheck(SStreamTriggerTask*, SSTriggerCalcRequest** ppRequest, bool) {
  *ppRequest = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t releaseTriggerRetryRequestAndClearPool(SStreamTriggerTask* pTask, SSTriggerCalcRequest** ppRequest, bool) {
  pTask->pRealtimeContext->calcParamPool.size = 0;
  *ppRequest = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t failTriggerCheckpointRead(int64_t, void** ppData, int64_t* pLen) {
  *ppData = nullptr;
  *pLen = 0;
  return TSDB_CODE_FAILED;
}

class StreamTriggerRetryObservabilityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gTriggerRetryState = {};
    stub_.set(tmsgSendReq, captureSuccessfulTriggerRetry);

    task_.task.type = STREAM_TRIGGER_TASK;
    task_.task.streamId = 0x300;
    task_.task.taskId = 0x301;
    task_.runnerList = taosArrayInit_s(sizeof(SStreamRunnerTarget), 1);
    ASSERT_NE(task_.runnerList, nullptr);
    auto* runner = static_cast<SStreamRunnerTarget*>(taosArrayGet(task_.runnerList, 0));
    ASSERT_NE(runner, nullptr);
    runner->addr.taskId = kRunnerTaskId;
    ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_LOGICAL_INPUT, 0, 1000, &task_.pStats),
              TSDB_CODE_SUCCESS);

    context_.pTask = &task_;
    context_.sessionId = kRealtimeSessionId;
    tdListInit(&context_.retryCalcReqs, POINTER_BYTES);
    task_.pRealtimeContext = &context_;

    request_.streamId = task_.task.streamId;
    request_.triggerTaskId = task_.task.taskId;
    request_.runnerTaskId = kRunnerTaskId;
    request_.sessionId = kRealtimeSessionId;
    request_.params = taosArrayInit(0, sizeof(SSTriggerCalcParam));
    request_.groupColVals = taosArrayInit(0, sizeof(SStreamGroupValue));
    ASSERT_NE(request_.params, nullptr);
    ASSERT_NE(request_.groupColVals, nullptr);
  }

  void TearDown() override {
    tdListEmpty(&context_.retryCalcReqs);
    tDestroySTriggerCalcRequest(&request_);
    taosArrayDestroy(task_.runnerList);
    stTaskStatsDestroy(&task_.pStats);
  }

  Stub                     stub_;
  SStreamTriggerTask       task_ = {};
  SSTriggerRealtimeContext context_ = {};
  SSTriggerCalcRequest     request_ = {};
};

TEST_F(StreamTriggerRetryObservabilityTest, SuccessfulRunnerRetryCountsAfterSendWithoutFailure) {
  SSTriggerAHandle responseAhandle = {};
  responseAhandle.param = &request_;
  SMsgSendInfo responseSendInfo = {};
  responseSendInfo.param = &responseAhandle;
  SRpcMsg response = {};
  response.msgType = TDMT_STREAM_TRIGGER_CALC_RSP;
  response.code = TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER;
  response.info.ahandle = &responseSendInfo;

  int64_t errorTaskId = 0;
  ASSERT_EQ(stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId), TSDB_CODE_SUCCESS);
  EXPECT_EQ(gTriggerRetryState.sendCalls, 1);

  SStreamTaskPeriodSnapshot period = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(task_.pStats, STREAM_STATS_PERIOD_US, &period, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(period.period.trigger.runnerCalcRetryCount, 1U);
  EXPECT_EQ(period.period.trigger.failureCount, 0U);
}

TEST_F(StreamTriggerRetryObservabilityTest, FailedImmediateRunnerRetryCountsOneLocalFailureAndKeepsOwnership) {
  gTriggerRetryState.failSend = true;
  SSTriggerAHandle responseAhandle = {};
  responseAhandle.param = &request_;
  SMsgSendInfo responseSendInfo = {};
  responseSendInfo.param = &responseAhandle;
  SRpcMsg response = {};
  response.msgType = TDMT_STREAM_TRIGGER_CALC_RSP;
  response.code = TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER;
  response.info.ahandle = &responseSendInfo;

  int64_t errorTaskId = 0;
  ASSERT_EQ(stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId), TSDB_CODE_FAILED);
  EXPECT_EQ(gTriggerRetryState.sendCalls, 1);
  EXPECT_EQ(listNEles(&context_.retryCalcReqs), 1);

  SStreamTaskPeriodSnapshot period = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(task_.pStats, STREAM_STATS_PERIOD_US, &period, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(period.period.trigger.runnerCalcRetryCount, 0U);
  EXPECT_EQ(period.period.trigger.failureCount, 1U);
}

TEST_F(StreamTriggerRetryObservabilityTest, NestedRealtimeCheckFailureIsNotCountedTwice) {
  stub_.set(stTriggerTaskReleaseRequest, releaseTriggerRetryRequestForCheck);
  stub_.set(streamReadCheckPoint, failTriggerCheckpointRead);
  context_.status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
  context_.haveReadCheckpoint = false;
  atomic_store_8(&task_.isCheckpointReady, 1);

  SSTriggerAHandle responseAhandle = {};
  responseAhandle.param = &request_;
  SMsgSendInfo responseSendInfo = {};
  responseSendInfo.param = &responseAhandle;
  SRpcMsg response = {};
  response.msgType = TDMT_STREAM_TRIGGER_CALC_RSP;
  response.code = TSDB_CODE_SUCCESS;
  response.info.ahandle = &responseSendInfo;

  int64_t errorTaskId = 0;
  ASSERT_EQ(stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId), TSDB_CODE_FAILED);

  SStreamTaskPeriodSnapshot period = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(task_.pStats, STREAM_STATS_PERIOD_US, &period, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(period.period.trigger.failureCount, 1U);
}

TEST_F(StreamTriggerRetryObservabilityTest, FailedResponseRefreshesMutatedOwnerGauges) {
  stub_.set(stTriggerTaskReleaseRequest, releaseTriggerRetryRequestAndClearPool);
  stub_.set(streamReadCheckPoint, failTriggerCheckpointRead);
  context_.status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
  context_.haveReadCheckpoint = false;
  context_.calcParamPool.nodeSize = 16;
  context_.calcParamPool.size = 1;
  context_.calcParamPool.capacity = 1;
  atomic_store_8(&task_.isCheckpointReady, 1);

  stTriggerTaskPublishRealtimeDebugGauges(&task_, &context_);
  ASSERT_TRUE(task_.realtimeDebugGauges.validMask & STREAM_TRIGGER_GAUGE_CALC_PARAM_POOL);
  ASSERT_EQ(task_.realtimeDebugGauges.calcParamPoolUsed, 1);

  SSTriggerAHandle responseAhandle = {};
  responseAhandle.param = &request_;
  SMsgSendInfo responseSendInfo = {};
  responseSendInfo.param = &responseAhandle;
  SRpcMsg response = {};
  response.msgType = TDMT_STREAM_TRIGGER_CALC_RSP;
  response.code = TSDB_CODE_SUCCESS;
  response.info.ahandle = &responseSendInfo;

  int64_t errorTaskId = 0;
  ASSERT_EQ(stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId), TSDB_CODE_FAILED);
  EXPECT_TRUE(task_.realtimeDebugGauges.validMask & STREAM_TRIGGER_GAUGE_CALC_PARAM_POOL);
  EXPECT_EQ(task_.realtimeDebugGauges.calcParamPoolUsed, 0);
}

struct TriggerRowState {
  SSDataBlock* dataBlock = nullptr;
  bool         returnSlice = false;
  int32_t      sendCalls = 0;
};

TriggerRowState      gTriggerRowState;
SSTriggerCalcRequest gTriggerRowRequest = {};

int32_t prepareTriggerRowSorter(SSTriggerNewTimestampSorter* pSorter, int64_t, int32_t, int32_t, STimeWindow*,
                                SObjList*, SSTriggerDataSlice*) {
  pSorter->inUse = true;
  gTriggerRowState.returnSlice = true;
  return TSDB_CODE_SUCCESS;
}

int32_t returnTriggerRowSlice(SSTriggerNewTimestampSorter*, SSDataBlock** ppBlock, int32_t* pStartIdx,
                              int32_t* pEndIdx) {
  if (gTriggerRowState.returnSlice) {
    *ppBlock = gTriggerRowState.dataBlock;
    *pStartIdx = 1;
    *pEndIdx = 5;
    gTriggerRowState.returnSlice = false;
  } else {
    *ppBlock = nullptr;
    *pStartIdx = 0;
    *pEndIdx = 0;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t acquireTriggerRowRequest(SStreamTriggerTask*, int64_t, int64_t gid, SSTriggerCalcRequest** ppRequest) {
  gTriggerRowRequest.gid = gid;
  *ppRequest = &gTriggerRowRequest;
  return TSDB_CODE_SUCCESS;
}

int32_t captureTriggerRowSend(const SEpSet*, SRpcMsg* pMsg) {
  ++gTriggerRowState.sendCalls;
  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = nullptr;
  destroyAhandle(pMsg->info.ahandle);
  pMsg->info.ahandle = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t initTriggerRowCache(int64_t, int64_t, int64_t, int32_t, int32_t, void** ppCache) {
  *ppCache = reinterpret_cast<void*>(0x1);
  return TSDB_CODE_SUCCESS;
}

int32_t putTriggerRowCache(void*, int64_t, TSKEY, TSKEY, SSDataBlock*, int32_t, int32_t) { return TSDB_CODE_SUCCESS; }

int32_t reportOneRunningTriggerRequest(SStreamTriggerTask*, int64_t, int64_t* pNumRunningReq) {
  *pNumRunningReq = 1;
  return TSDB_CODE_SUCCESS;
}

class StreamTriggerRowObservabilityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gTriggerRowState = {};
    gTriggerRowRequest = {};
    stub_.set(stNewTimestampSorterSetData, prepareTriggerRowSorter);
    stub_.set(stNewTimestampSorterNextDataBlock, returnTriggerRowSlice);
    stub_.set(stTriggerTaskAcquireRequest, acquireTriggerRowRequest);
    stub_.set(stTriggerTaskGetRunningReq, reportOneRunningTriggerRequest);
    stub_.set(tmsgSendReq, captureTriggerRowSend);
    stub_.set(initStreamDataCache, initTriggerRowCache);
    stub_.set(putStreamDataCache, putTriggerRowCache);

    task_.task.type = STREAM_TRIGGER_TASK;
    task_.task.streamId = 0x400;
    task_.task.taskId = 0x401;
    task_.triggerType = STREAM_TRIGGER_COUNT;
    task_.windowCount = 3;
    task_.windowSliding = 1;
    task_.trigTsIndex = 0;
    task_.calcTsIndex = 0;
    task_.calcEventType = STRIGGER_EVENT_WINDOW_CLOSE;
    task_.placeHolderBitmap = PLACE_HOLDER_PARTITION_ROWS;
    task_.lowLatencyCalc = true;
    task_.historyCalcStarted = true;
    task_.pRealtimeContext = &context_;
    task_.readerList = taosArrayInit_s(sizeof(SStreamTaskAddr), 1);
    task_.runnerList = taosArrayInit_s(sizeof(SStreamRunnerTarget), 1);
    task_.pUserRecalcRequests = taosArrayInit(0, sizeof(SStreamRecalcReq));
    task_.pGroupPendingRecalcs = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    ASSERT_NE(task_.readerList, nullptr);
    ASSERT_NE(task_.runnerList, nullptr);
    ASSERT_NE(task_.pUserRecalcRequests, nullptr);
    ASSERT_NE(task_.pGroupPendingRecalcs, nullptr);
    reader_ = static_cast<SStreamTaskAddr*>(taosArrayGet(task_.readerList, 0));
    reader_->nodeId = 1;
    reader_->taskId = 0x402;
    auto* runner = static_cast<SStreamRunnerTarget*>(taosArrayGet(task_.runnerList, 0));
    runner->addr.taskId = kRunnerTaskId;
    statsStartMonoUs_ = streamTaskGetMonotonicUs() - STREAM_STATS_BUCKET_COUNT * STREAM_STATS_BUCKET_US;
    ASSERT_EQ(
        stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_LOGICAL_INPUT, statsStartMonoUs_, 1000, &task_.pStats),
        TSDB_CODE_SUCCESS);
    taosInitRWLatch(&task_.readerProgressLock);
    task_.pReaderProgressSnapshots = taosArrayInit(0, sizeof(SStreamReaderProgressSnapshot));
    ASSERT_NE(task_.pReaderProgressSnapshots, nullptr);

    gTriggerRowRequest.streamId = task_.task.streamId;
    gTriggerRowRequest.runnerTaskId = kRunnerTaskId;
    gTriggerRowRequest.sessionId = kRealtimeSessionId;
    gTriggerRowRequest.params = taosArrayInit(0, sizeof(SSTriggerCalcParam));
    gTriggerRowRequest.groupColVals = taosArrayInit(0, sizeof(SStreamGroupValue));
    ASSERT_NE(gTriggerRowRequest.params, nullptr);
    ASSERT_NE(gTriggerRowRequest.groupColVals, nullptr);

    ASSERT_EQ(createDataBlock(&block_), TSDB_CODE_SUCCESS);
    SColumnInfoData tsCol = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 1);
    SColumnInfoData verCol = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 2);
    ASSERT_EQ(blockDataAppendColInfo(block_, &tsCol), TSDB_CODE_SUCCESS);
    ASSERT_EQ(blockDataAppendColInfo(block_, &verCol), TSDB_CODE_SUCCESS);
    ASSERT_EQ(blockDataEnsureCapacity(block_, 6), TSDB_CODE_SUCCESS);
    for (int32_t i = 0; i < 6; ++i) {
      TSKEY   ts = i + 1;
      int64_t ver = 1;
      ASSERT_EQ(colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(block_->pDataBlock, 0)), i,
                              reinterpret_cast<const char*>(&ts), false),
                TSDB_CODE_SUCCESS);
      ASSERT_EQ(colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(block_->pDataBlock, 1)), i,
                              reinterpret_cast<const char*>(&ver), false),
                TSDB_CODE_SUCCESS);
    }
    block_->info.rows = 6;
    gTriggerRowState.dataBlock = block_;

    context_.pTask = &task_;
    context_.sessionId = kRealtimeSessionId;
    context_.status = STRIGGER_CONTEXT_CHECK_CONDITION;
    context_.calcRange = {.skey = INT64_MIN, .ekey = INT64_MIN};
    context_.haveReadCheckpoint = true;
    context_.boundDetermined = true;
    context_.lastCheckpointTime = taosGetTimestampNs();
    context_.pReaderWalProgress = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    context_.pGroups = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    context_.pSlices = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    context_.pRanges = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    context_.pGroupColVals = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    context_.pWindows = taosArrayInit(0, sizeof(SSTriggerNotifyWindow));
    context_.pParentWindows = taosArrayInit(0, sizeof(SSTriggerNotifyWindow));
    context_.pNotifyParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
    context_.pTempSlices = taosArrayInit(0, sizeof(int64_t) * 3);
    context_.groupsToDelete = taosArrayInit(0, sizeof(int64_t));
    ASSERT_NE(context_.pReaderWalProgress, nullptr);
    ASSERT_NE(context_.pGroups, nullptr);
    ASSERT_NE(context_.pSlices, nullptr);
    ASSERT_NE(context_.pRanges, nullptr);
    ASSERT_NE(context_.pGroupColVals, nullptr);
    ASSERT_NE(context_.pWindows, nullptr);
    ASSERT_NE(context_.pParentWindows, nullptr);
    ASSERT_NE(context_.pNotifyParams, nullptr);
    ASSERT_NE(context_.pTempSlices, nullptr);
    ASSERT_NE(context_.groupsToDelete, nullptr);
    TD_DLIST_INIT(&context_.groupsToCheck);
    TD_DLIST_INIT(&context_.groupsToCheckIdle);
    tdListInit(&context_.retryPullReqs, POINTER_BYTES);
    tdListInit(&context_.retryCalcReqs, POINTER_BYTES);
    tdListInit(&context_.dropTableReqs, POINTER_BYTES);
    ASSERT_EQ(taosObjPoolInit(&context_.metaPool, 1, sizeof(SSTriggerMetaData)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosObjPoolInit(&context_.tableUidPool, 1, sizeof(int64_t) * 2), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosObjPoolInit(&context_.windowPool, 1, sizeof(SSTriggerWindow)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosObjPoolInit(&context_.calcParamPool, 1, sizeof(SSTriggerCalcParam)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosObjListInit(&context_.dumpTableUids, &context_.tableUidPool), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosObjListInit(&context_.pAllCalcTableUids, &context_.tableUidPool), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosObjListInit(&context_.pCalcTableUids, &context_.tableUidPool), TSDB_CODE_SUCCESS);
    context_.pSorter =
        static_cast<SSTriggerNewTimestampSorter*>(taosMemoryCalloc(1, sizeof(SSTriggerNewTimestampSorter)));
    context_.pCalcSorter =
        static_cast<SSTriggerNewTimestampSorter*>(taosMemoryCalloc(1, sizeof(SSTriggerNewTimestampSorter)));
    ASSERT_NE(context_.pSorter, nullptr);
    ASSERT_NE(context_.pCalcSorter, nullptr);
    ASSERT_EQ(stNewTimestampSorterInit(context_.pSorter, &task_, 0), TSDB_CODE_SUCCESS);
    ASSERT_EQ(stNewTimestampSorterInit(context_.pCalcSorter, &task_, 0), TSDB_CODE_SUCCESS);
    context_.pMaxDelayHeap = heapCreate(compareRealtimeMaxDelayGroups);
    ASSERT_NE(context_.pMaxDelayHeap, nullptr);

    SSTriggerWalProgress progress = {};
    progress.pTaskAddr = reader_;
    progress.pCalcBlock = block_;
    progress.pVersions = taosArrayInit(0, sizeof(int64_t));
    ASSERT_NE(progress.pVersions, nullptr);
    progress.pullReq.base.streamId = task_.task.streamId;
    progress.pullReq.base.readerTaskId = reader_->taskId;
    progress.pullReq.base.sessionId = kRealtimeSessionId;
    ASSERT_EQ(tSimpleHashPut(context_.pReaderWalProgress, &reader_->nodeId, sizeof(reader_->nodeId), &progress,
                             sizeof(progress)),
              TSDB_CODE_SUCCESS);
    progress_ = static_cast<SSTriggerWalProgress*>(
        tSimpleHashGet(context_.pReaderWalProgress, &reader_->nodeId, sizeof(reader_->nodeId)));

    group_.pContext = &context_;
    group_.gid = 42;
    group_.vgId = reader_->nodeId;
    group_.oldThreshold = 0;
    group_.newThreshold = 6;
    group_.prevWindow = {.skey = INT64_MIN, .ekey = INT64_MIN};
    group_.pWalMetas = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    ASSERT_NE(group_.pWalMetas, nullptr);
    ASSERT_EQ(taosObjListInit(&group_.tableUids, &context_.tableUidPool), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosObjListInit(&group_.windows, &context_.windowPool), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosObjListInit(&group_.pPendingCalcParams, &context_.calcParamPool), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosObjListInit(&group_.pPendingParWinCalcParams, &context_.calcParamPool), TSDB_CODE_SUCCESS);
    int64_t ids[2] = {100, reader_->nodeId};
    ASSERT_EQ(taosObjListAppend(&group_.tableUids, ids), TSDB_CODE_SUCCESS);
    SObjList metas = {};
    ASSERT_EQ(tSimpleHashPut(group_.pWalMetas, &reader_->nodeId, sizeof(reader_->nodeId), &metas, sizeof(metas)),
              TSDB_CODE_SUCCESS);
    metas_ = static_cast<SObjList*>(tSimpleHashGet(group_.pWalMetas, &reader_->nodeId, sizeof(reader_->nodeId)));
    ASSERT_NE(metas_, nullptr);
    ASSERT_EQ(taosObjListInit(metas_, &context_.metaPool), TSDB_CODE_SUCCESS);
    SSTriggerMetaData meta = {.skey = 1, .ekey = 6, .ver = 1};
    ASSERT_EQ(taosObjListAppend(metas_, &meta), TSDB_CODE_SUCCESS);
    SSTriggerDataSlice slice = {.pDataBlock = block_, .startIdx = 0, .endIdx = 6};
    int64_t            uid = 100;
    ASSERT_EQ(tSimpleHashPut(context_.pSlices, &uid, sizeof(uid), &slice, sizeof(slice)), TSDB_CODE_SUCCESS);
    TD_DLIST_APPEND(&context_.groupsToCheck, &group_);
    atomic_store_8(&task_.realtimeStarted, 1);
  }

  int32_t StartCheck() {
    SSTriggerCtrlRequest request = {.type = STRIGGER_CTRL_START,
                                    .streamId = task_.task.streamId,
                                    .taskId = task_.task.taskId,
                                    .sessionId = kRealtimeSessionId};
    SRpcMsg              response = {.msgType = TDMT_STREAM_TRIGGER_CTRL};
    response.contLen = tSerializeSTriggerCtrlRequest(nullptr, 0, &request);
    response.pCont = rpcMallocCont(response.contLen);
    if (response.pCont == nullptr) return terrno;
    tSerializeSTriggerCtrlRequest(response.pCont, response.contLen, &request);
    int64_t errorTaskId = 0;
    int32_t code = stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId);
    rpcFreeCont(response.pCont);
    return code;
  }

  void TearDown() override {
    taosObjListClear(metas_);
    taosObjListClear(&group_.pPendingParWinCalcParams);
    taosObjListClear(&group_.pPendingCalcParams);
    taosObjListClear(&group_.windows);
    taosObjListClear(&group_.tableUids);
    tSimpleHashCleanup(group_.pWalMetas);
    heapDestroy(context_.pMaxDelayHeap);
    stNewTimestampSorterDestroy(&context_.pCalcSorter);
    stNewTimestampSorterDestroy(&context_.pSorter);
    taosObjListClear(&context_.pCalcTableUids);
    taosObjListClear(&context_.pAllCalcTableUids);
    taosObjListClear(&context_.dumpTableUids);
    taosObjPoolDestroy(&context_.calcParamPool);
    taosObjPoolDestroy(&context_.windowPool);
    taosObjPoolDestroy(&context_.tableUidPool);
    taosObjPoolDestroy(&context_.metaPool);
    tdListEmpty(&context_.dropTableReqs);
    tdListEmpty(&context_.retryCalcReqs);
    tdListEmpty(&context_.retryPullReqs);
    taosArrayDestroy(context_.groupsToDelete);
    taosArrayDestroy(context_.pTempSlices);
    taosArrayDestroy(context_.pNotifyParams);
    taosArrayDestroy(context_.pParentWindows);
    taosArrayDestroy(context_.pWindows);
    tSimpleHashCleanup(context_.pGroupColVals);
    tSimpleHashCleanup(context_.pRanges);
    tSimpleHashCleanup(context_.pSlices);
    tSimpleHashCleanup(context_.pGroups);
    taosArrayDestroy(progress_->pVersions);
    tSimpleHashCleanup(context_.pReaderWalProgress);
    tDestroySTriggerCalcRequest(&gTriggerRowRequest);
    taosArrayDestroy(task_.pReaderProgressSnapshots);
    tSimpleHashCleanup(task_.pGroupPendingRecalcs);
    taosArrayDestroy(task_.pUserRecalcRequests);
    taosArrayDestroy(task_.runnerList);
    taosArrayDestroy(task_.readerList);
    blockDataDestroy(block_);
    stTaskStatsDestroy(&task_.pStats);
  }

  Stub                     stub_;
  SStreamTriggerTask       task_ = {};
  SSTriggerRealtimeContext context_ = {};
  SSTriggerRealtimeGroup   group_ = {};
  SStreamTaskAddr*         reader_ = nullptr;
  SSTriggerWalProgress*    progress_ = nullptr;
  SObjList*                metas_ = nullptr;
  SSDataBlock*             block_ = nullptr;
  int64_t                  statsStartMonoUs_ = 0;
};

TEST_F(StreamTriggerRowObservabilityTest, FilteredRowsCountOnceWithoutWindowFanoutOrCalcReentry) {
  ASSERT_EQ(StartCheck(), TSDB_CODE_SUCCESS);
  ASSERT_EQ(group_.pPendingCalcParams.neles, 2);
  ASSERT_EQ(StartCheck(), TSDB_CODE_SUCCESS);
  EXPECT_GT(gTriggerRowState.sendCalls, 0);
  ASSERT_EQ(context_.status, STRIGGER_CONTEXT_SEND_CALC_REQ);
  ASSERT_EQ(context_.pCalcReq, &gTriggerRowRequest);
  ASSERT_EQ(taosArrayGetSize(gTriggerRowRequest.params), 2);
  int64_t ids[2] = {100, reader_->nodeId};
  ASSERT_EQ(taosObjListAppend(&context_.pAllCalcTableUids, ids), TSDB_CODE_SUCCESS);
  ASSERT_EQ(StartCheck(), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsSnapshot metrics = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(task_.pStats, statsStartMonoUs_ + 61 * STREAM_STATS_BUCKET_US, &metrics),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(metrics.logicalInputRows1m, 4U);

  SStreamTaskPeriodSnapshot period = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(task_.pStats, statsStartMonoUs_ + STREAM_STATS_PERIOD_US, &period, &rotated),
            TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(period.period.trigger.logicalWindowCount, 2U);
}

struct CreateTableRequestState {
  int32_t              acquireCalls = 0;
  int32_t              sendCalls = 0;
  int32_t              msgType = 0;
  int32_t              decodeCode = TSDB_CODE_SUCCESS;
  SSTriggerCalcRequest request = {};
  SSTriggerCalcRequest decodedRequest = {};
};

CreateTableRequestState gCreateTableRequestState;

int32_t acquireCreateTableRequest(SStreamTriggerTask* pTask, int64_t sessionId, int64_t gid,
                                  SSTriggerCalcRequest** ppRequest) {
  ++gCreateTableRequestState.acquireCalls;
  gCreateTableRequestState.request.streamId = pTask->task.streamId;
  gCreateTableRequestState.request.runnerTaskId = kRunnerTaskId;
  gCreateTableRequestState.request.sessionId = sessionId;
  gCreateTableRequestState.request.gid = gid;
  *ppRequest = &gCreateTableRequestState.request;
  return TSDB_CODE_SUCCESS;
}

int32_t captureCreateTableRequest(const SEpSet*, SRpcMsg* pMsg) {
  ++gCreateTableRequestState.sendCalls;
  gCreateTableRequestState.msgType = pMsg->msgType;

  if (pMsg->msgType == TDMT_STREAM_TRIGGER_CALC) {
    if (pMsg->pCont != nullptr && pMsg->contLen > static_cast<int32_t>(sizeof(SMsgHead))) {
      gCreateTableRequestState.decodeCode =
          tDeserializeSTriggerCalcRequest(static_cast<char*>(pMsg->pCont) + sizeof(SMsgHead),
                                          pMsg->contLen - sizeof(SMsgHead), &gCreateTableRequestState.decodedRequest);
    } else {
      gCreateTableRequestState.decodeCode = TSDB_CODE_INVALID_MSG;
    }
  }

  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = nullptr;
  destroyAhandle(pMsg->info.ahandle);
  pMsg->info.ahandle = nullptr;
  return TSDB_CODE_SUCCESS;
}

class StreamTriggerCreateTableRequestTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gCreateTableRequestState = {};
    gCreateTableRequestState.request.params = taosArrayInit(1, sizeof(SSTriggerCalcParam));
    gCreateTableRequestState.request.groupColVals = taosArrayInit(1, sizeof(SStreamGroupValue));
    ASSERT_NE(gCreateTableRequestState.request.params, nullptr);
    ASSERT_NE(gCreateTableRequestState.request.groupColVals, nullptr);

    stub_.set(stTriggerTaskAcquireRequest, acquireCreateTableRequest);
    stub_.set(tmsgSendReq, captureCreateTableRequest);

    task_.task.streamId = 0x100;
    task_.task.taskId = 0x101;
    task_.triggerType = STREAM_TRIGGER_COUNT;
    task_.nodelayCreateSubtable = 1;
    task_.hasPartitionBy = true;

    task_.runnerList = taosArrayInit_s(sizeof(SStreamRunnerTarget), 1);
    ASSERT_NE(task_.runnerList, nullptr);
    auto* pRunner = static_cast<SStreamRunnerTarget*>(taosArrayGet(task_.runnerList, 0));
    ASSERT_NE(pRunner, nullptr);
    pRunner->addr.taskId = kRunnerTaskId;

    context_.pTask = &task_;
    context_.sessionId = kRealtimeSessionId;
    context_.status = STRIGGER_CONTEXT_DETERMINE_BOUND;
    context_.curReaderIdx = 2;
    context_.pGroupColVals = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    ASSERT_NE(context_.pGroupColVals, nullptr);
    context_.pPendingCreateTableGids = taosArrayInit(1, sizeof(SSTriggerPendingCreateTableEntry));
    ASSERT_NE(context_.pPendingCreateTableGids, nullptr);
    SSTriggerPendingCreateTableEntry pending = {};
    pending.gid = kCreateTableGroupId;
    ASSERT_NE(taosArrayPush(context_.pPendingCreateTableGids, &pending), nullptr);
    task_.pRealtimeContext = &context_;
  }

  void TearDown() override {
    tDestroySTriggerCalcRequest(&gCreateTableRequestState.request);
    tDestroySTriggerCalcRequest(&gCreateTableRequestState.decodedRequest);

    taosArrayDestroy(context_.pPendingCreateTableGids);
    tSimpleHashCleanup(context_.pGroupColVals);
    taosArrayDestroy(task_.runnerList);
  }

  Stub                     stub_;
  SStreamTriggerTask       task_ = {};
  SSTriggerRealtimeContext context_ = {};
};

TEST_F(StreamTriggerCreateTableRequestTest, NodelayCreateRequestContainsNoCalculationWindow) {
  SSTriggerGroupColValueRequest pullRequest = {};
  pullRequest.base.type = STRIGGER_PULL_GROUP_COL_VALUE;
  pullRequest.base.sessionId = kRealtimeSessionId;
  pullRequest.gid = kCreateTableGroupId;

  SSTriggerAHandle responseAhandle = {};
  responseAhandle.param = &pullRequest;
  SMsgSendInfo responseSendInfo = {};
  responseSendInfo.param = &responseAhandle;
  SRpcMsg response = {};
  response.msgType = TDMT_STREAM_TRIGGER_PULL_RSP;
  response.code = TSDB_CODE_SUCCESS;
  response.info.ahandle = &responseSendInfo;

  int64_t errorTaskId = 0;
  ASSERT_EQ(stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId), TSDB_CODE_SUCCESS);

  ASSERT_EQ(gCreateTableRequestState.acquireCalls, 1);
  ASSERT_EQ(gCreateTableRequestState.sendCalls, 1);
  EXPECT_EQ(gCreateTableRequestState.msgType, TDMT_STREAM_TRIGGER_CALC);
  EXPECT_EQ(taosArrayGetSize(gCreateTableRequestState.request.params), 0);
  ASSERT_EQ(gCreateTableRequestState.decodeCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(gCreateTableRequestState.decodedRequest.params, nullptr);
  EXPECT_EQ(gCreateTableRequestState.decodedRequest.createTable, 1);
  EXPECT_EQ(gCreateTableRequestState.decodedRequest.gid, kCreateTableGroupId);
}

int32_t compareRealtimeMaxDelayGroups(const HeapNode* lhs, const HeapNode* rhs) {
  auto* lhsGroup = TCONTAINER_OF(lhs, SSTriggerRealtimeGroup, heapNode);
  auto* rhsGroup = TCONTAINER_OF(rhs, SSTriggerRealtimeGroup, heapNode);
  return lhsGroup->nextExecTime < rhsGroup->nextExecTime;
}

int32_t ignoreWaitSessionAppend(SList*, const void*) { return TSDB_CODE_SUCCESS; }

int32_t gUnavailableCalcRequestAcquireCalls = 0;

int32_t acquireUnavailableCalcRequest(SStreamTriggerTask*, int64_t, int64_t, SSTriggerCalcRequest** ppRequest) {
  ++gUnavailableCalcRequestAcquireCalls;
  *ppRequest = nullptr;
  return TSDB_CODE_SUCCESS;
}

class StreamTriggerNotifyOnlyMaxDelayTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gUnavailableCalcRequestAcquireCalls = 0;
    stub_.set(tdListAppend, ignoreWaitSessionAppend);
    stub_.set(stTriggerTaskAcquireRequest, acquireUnavailableCalcRequest);

    task_.task.streamId = 0x200;
    task_.task.taskId = 0x201;
    task_.task.type = STREAM_TRIGGER_TASK;
    task_.triggerType = STREAM_TRIGGER_SLIDING;
    task_.calcEventType = STRIGGER_EVENT_WINDOW_NONE;
    task_.notifyEventType = static_cast<ESTriggerEventType>(STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
    task_.maxDelayNs = 5 * NANOSECOND_PER_SEC;
    task_.pRealtimeContext = &context_;
    task_.readerList = taosArrayInit_s(sizeof(SStreamTaskAddr), 1);
    taosInitRWLatch(&task_.readerProgressLock);
    task_.pReaderProgressSnapshots = taosArrayInit(0, sizeof(SStreamReaderProgressSnapshot));
    ASSERT_NE(task_.readerList, nullptr);
    ASSERT_NE(task_.pReaderProgressSnapshots, nullptr);
    ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_REALTIME_LAG, 0, 1000, &task_.pStats),
              TSDB_CODE_SUCCESS);
    reader_ = static_cast<SStreamTaskAddr*>(taosArrayGet(task_.readerList, 0));
    ASSERT_NE(reader_, nullptr);
    reader_->nodeId = 1;
    reader_->taskId = 0x202;

    context_.pTask = &task_;
    context_.sessionId = kRealtimeSessionId;
    context_.walMode = STRIGGER_WAL_META_THEN_DATA;
    context_.status = STRIGGER_CONTEXT_FETCH_META;
    context_.haveReadCheckpoint = true;
    context_.lastCheckpointTime = taosGetTimestampNs();
    context_.pReaderWalProgress = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    ASSERT_NE(context_.pReaderWalProgress, nullptr);
    SSTriggerWalProgress progress = {};
    ASSERT_EQ(tSimpleHashPut(context_.pReaderWalProgress, &reader_->nodeId, sizeof(reader_->nodeId), &progress,
                             sizeof(progress)),
              TSDB_CODE_SUCCESS);
    progress_ = static_cast<SSTriggerWalProgress*>(
        tSimpleHashGet(context_.pReaderWalProgress, &reader_->nodeId, sizeof(reader_->nodeId)));
    ASSERT_NE(progress_, nullptr);
    progress_->pTaskAddr = reader_;
    progress_->pTrigBlock = static_cast<SSDataBlock*>(taosMemoryCalloc(1, sizeof(SSDataBlock)));
    ASSERT_NE(progress_->pTrigBlock, nullptr);
    progress_->pullReq.base.type = STRIGGER_PULL_WAL_DATA_NEW;
    progress_->pullReq.base.streamId = task_.task.streamId;
    progress_->pullReq.base.readerTaskId = reader_->taskId;
    progress_->pullReq.base.sessionId = context_.sessionId;
    progress_->pullReq.base.triggerTaskId = task_.task.taskId;

    context_.pTempSlices = taosArrayInit(0, sizeof(int64_t) * 3);
    ASSERT_NE(context_.pTempSlices, nullptr);
    context_.groupsToDelete = taosArrayInit(0, sizeof(int64_t));
    ASSERT_NE(context_.groupsToDelete, nullptr);
    TD_DLIST_INIT(&context_.groupsToCheck);
    TD_DLIST_INIT(&context_.groupsToCheckIdle);
    tdListInit(&context_.retryPullReqs, POINTER_BYTES);
    tdListInit(&context_.retryCalcReqs, POINTER_BYTES);
    tdListInit(&context_.dropTableReqs, POINTER_BYTES);

    ASSERT_EQ(taosObjPoolInit(&context_.windowPool, 1, sizeof(SSTriggerWindow)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosObjPoolInit(&context_.calcParamPool, 1, sizeof(SSTriggerCalcParam)), TSDB_CODE_SUCCESS);
    group_.pContext = &context_;
    group_.gid = 0;
    ASSERT_EQ(taosObjListInit(&group_.windows, &context_.windowPool), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosObjListInit(&group_.pPendingCalcParams, &context_.calcParamPool), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosObjListInit(&group_.pPendingParWinCalcParams, &context_.calcParamPool), TSDB_CODE_SUCCESS);
    SSTriggerWindow window = {.range = {.skey = 1000, .ekey = 2000},
                              .wrownum = 1,
                              .prevProcTime = context_.lastCheckpointTime - task_.maxDelayNs};
    ASSERT_EQ(taosObjListAppend(&group_.windows, &window), TSDB_CODE_SUCCESS);

    context_.pMaxDelayHeap = heapCreate(compareRealtimeMaxDelayGroups);
    ASSERT_NE(context_.pMaxDelayHeap, nullptr);
    group_.nextExecTime = 1;
    heapInsert(context_.pMaxDelayHeap, &group_.heapNode);
    context_.pMinGroup = &group_;
  }

  int32_t ProcessWalNoDataResponse() {
    int64_t          walVersion = 0;
    SSTriggerAHandle responseAhandle = {};
    responseAhandle.param = &progress_->pullReq.base;
    SMsgSendInfo responseSendInfo = {};
    responseSendInfo.param = &responseAhandle;
    SRpcMsg response = {};
    response.msgType = TDMT_STREAM_TRIGGER_PULL_RSP;
    response.pCont = &walVersion;
    response.contLen = sizeof(walVersion);
    response.code = TSDB_CODE_STREAM_NO_DATA;
    response.info.ahandle = &responseSendInfo;

    int64_t errorTaskId = 0;
    return stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId);
  }

  int32_t ProcessMalformedWalDataResponse() {
    uint8_t          invalidPayload = 0;
    SSTriggerAHandle responseAhandle = {};
    responseAhandle.param = &progress_->pullReq.base;
    SMsgSendInfo responseSendInfo = {};
    responseSendInfo.param = &responseAhandle;
    SRpcMsg response = {};
    response.msgType = TDMT_STREAM_TRIGGER_PULL_RSP;
    response.pCont = &invalidPayload;
    response.contLen = sizeof(invalidPayload);
    response.code = TSDB_CODE_SUCCESS;
    response.info.ahandle = &responseSendInfo;

    int64_t errorTaskId = 0;
    return stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId);
  }

  void TearDown() override {
    heapDestroy(context_.pMaxDelayHeap);
    taosObjListClear(&group_.pPendingParWinCalcParams);
    taosObjListClear(&group_.pPendingCalcParams);
    taosObjListClear(&group_.windows);
    taosObjPoolDestroy(&context_.calcParamPool);
    taosObjPoolDestroy(&context_.windowPool);
    tdListEmpty(&context_.dropTableReqs);
    tdListEmpty(&context_.retryCalcReqs);
    tdListEmpty(&context_.retryPullReqs);
    taosArrayDestroy(context_.groupsToDelete);
    taosArrayDestroy(context_.pTempSlices);
    blockDataDestroy(progress_->pTrigBlock);
    tSimpleHashCleanup(context_.pReaderWalProgress);
    taosArrayDestroy(task_.pReaderProgressSnapshots);
    taosArrayDestroy(task_.runnerList);
    taosArrayDestroy(task_.readerList);
    stTaskStatsDestroy(&task_.pStats);
  }

  Stub                     stub_;
  SStreamTriggerTask       task_ = {};
  SSTriggerRealtimeContext context_ = {};
  SSTriggerRealtimeGroup   group_ = {};
  SStreamTaskAddr*         reader_ = nullptr;
  SSTriggerWalProgress*    progress_ = nullptr;
};

TEST_F(StreamTriggerNotifyOnlyMaxDelayTest, WalNoDataResponseClearsStaleCalculationSchedule) {
  ASSERT_EQ(ProcessWalNoDataResponse(), TSDB_CODE_SUCCESS);

  EXPECT_EQ(gUnavailableCalcRequestAcquireCalls, 0);
  EXPECT_EQ(context_.pCalcReq, nullptr);
  EXPECT_EQ(group_.nextExecTime, 0);
  EXPECT_EQ(context_.pMaxDelayHeap->nelts, 0U);
  EXPECT_EQ(context_.pMaxDelayHeap->min, nullptr);
}

TEST_F(StreamTriggerNotifyOnlyMaxDelayTest, WalNoDataResponsePreservesCalculationScheduleWhenRunnerExists) {
  task_.runnerList = taosArrayInit_s(sizeof(SStreamRunnerTarget), 1);
  ASSERT_NE(task_.runnerList, nullptr);

  ASSERT_EQ(ProcessWalNoDataResponse(), TSDB_CODE_SUCCESS);

  EXPECT_EQ(gUnavailableCalcRequestAcquireCalls, 1);
  EXPECT_EQ(context_.pCalcReq, nullptr);
  EXPECT_EQ(group_.nextExecTime, 1);
  EXPECT_EQ(context_.pMaxDelayHeap->nelts, 1U);
  EXPECT_EQ(context_.pMaxDelayHeap->min, &group_.heapNode);
}

TEST_F(StreamTriggerNotifyOnlyMaxDelayTest, ReaderProgressUpdateDoesNotAggregateRealtimeLag) {
  stTaskStatsSetRealtimeLag(task_.pStats, true, 123);

  ASSERT_EQ(ProcessWalNoDataResponse(), TSDB_CODE_SUCCESS);

  SStreamTaskMetricsSnapshot metrics = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(task_.pStats, streamTaskGetMonotonicUs(), &metrics), TSDB_CODE_SUCCESS);
  EXPECT_NE(metrics.validMask & STREAM_METRIC_REALTIME_LAG, 0U);
  EXPECT_EQ(metrics.realtimeLagMs, 123);
}

TEST_F(StreamTriggerNotifyOnlyMaxDelayTest, MalformedWalPullResponseCountsOneDirectLocalFailure) {
  ASSERT_NE(ProcessMalformedWalDataResponse(), TSDB_CODE_SUCCESS);

  SStreamTaskPeriodSnapshot period = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(task_.pStats, STREAM_STATS_PERIOD_US, &period, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(period.period.trigger.failureCount, 1U);
}

int32_t failExtFollowUpSend(const SEpSet*, SRpcMsg* pMsg) {
  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = nullptr;
  return TSDB_CODE_FAILED;
}

TEST(StreamTriggerExtObservabilityTest, FailedFollowUpSendCountsOneDirectLocalFailure) {
  Stub stub;
  stub.set(tmsgSendReq, failExtFollowUpSend);

  SStreamTriggerTask task = {};
  task.task.type = STREAM_TRIGGER_TASK;
  task.task.streamId = 0x400;
  task.task.taskId = 0x401;
  ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_REALTIME_LAG, 0, 1000, &task.pStats),
            TSDB_CODE_SUCCESS);

  SSTriggerRealtimeContext context = {};
  context.pTask = &task;
  context.sessionId = kRealtimeSessionId;
  context.walMode = STRIGGER_WAL_META_ONLY;
  task.pRealtimeContext = &context;

  SStreamTaskAddr reader = {};
  reader.taskId = 0x402;
  reader.nodeId = 4;
  SSTriggerExtProgress progress = {};
  progress.pTaskAddr = &reader;
  progress.pOwnerTask = &task;
  progress.sessionId = kRealtimeSessionId;
  progress.pullReq = reinterpret_cast<void*>(1);
  progress.pullType = STRIGGER_PULL_LAST_TS_EXT;
  progress.triggerSideUidMaxTs = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(progress.triggerSideUidMaxTs, nullptr);

  SSTriggerExtPullRsp extResponse = {
      .pullType = STRIGGER_PULL_LAST_TS_EXT,
      .code = TSDB_CODE_SUCCESS,
  };
  const int32_t encodedLen = tSerializeSSTriggerExtPullRsp(nullptr, 0, &extResponse);
  ASSERT_GT(encodedLen, 0);
  void* encoded = rpcMallocCont(encodedLen);
  ASSERT_NE(encoded, nullptr);
  ASSERT_EQ(tSerializeSSTriggerExtPullRsp(encoded, encodedLen, &extResponse), encodedLen);

  SSTriggerAHandle responseAhandle = {};
  responseAhandle.param = &progress;
  SMsgSendInfo responseSendInfo = {};
  responseSendInfo.param = &responseAhandle;
  SRpcMsg response = {};
  response.msgType = TDMT_STREAM_TRIGGER_PULL_EXT_RSP;
  response.code = TSDB_CODE_SUCCESS;
  response.pCont = encoded;
  response.contLen = encodedLen;
  response.info.ahandle = &responseSendInfo;

  int64_t errorTaskId = 0;
  EXPECT_EQ(stTriggerTaskProcessRsp(&task.task, &response, &errorTaskId), TSDB_CODE_FAILED);

  SStreamTaskPeriodSnapshot period = {};
  bool                      rotated = false;
  ASSERT_EQ(stTaskStatsRotatePeriod(task.pStats, STREAM_STATS_PERIOD_US, &period, &rotated), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(rotated);
  EXPECT_EQ(period.period.trigger.failureCount, 1U);

  rpcFreeCont(encoded);
  tSimpleHashCleanup(progress.triggerSideUidMaxTs);
  stTaskStatsDestroy(&task.pStats);
}

SStreamRecalcSnapshot CopyRecalcSnapshot(SStreamRecalcTracker* tracker, int64_t recalcId) {
  bool    historyValid = false;
  int32_t historyProgress = 0;
  SArray* snapshots = nullptr;
  EXPECT_EQ(stRecalcTrackerCopySnapshot(tracker, &historyValid, &historyProgress, &snapshots), TSDB_CODE_SUCCESS);
  EXPECT_NE(snapshots, nullptr);
  SStreamRecalcSnapshot result = {};
  if (snapshots != nullptr) {
    for (int32_t i = 0; i < taosArrayGetSize(snapshots); ++i) {
      auto* snapshot = static_cast<SStreamRecalcSnapshot*>(taosArrayGet(snapshots, i));
      if (snapshot->recalcId == recalcId) {
        result = *snapshot;
        break;
      }
    }
  }
  taosArrayDestroy(snapshots);
  return result;
}

void DestroyRecalcRequest(SSTriggerRecalcRequest** ppRequest) {
  if (ppRequest == nullptr || *ppRequest == nullptr) return;
  tSimpleHashCleanup((*ppRequest)->pTsdbVersions);
  taosArrayDestroy((*ppRequest)->pContributors);
  taosMemoryFreeClear(*ppRequest);
}

class StreamTriggerRecalcOwnershipTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(stRecalcTrackerCreate(&task_.pRecalcTracker), TSDB_CODE_SUCCESS);
    task_.triggerType = STREAM_TRIGGER_COUNT;
    task_.historyCalcStarted = true;
    atomic_store_8(&task_.realtimeStarted, 1);
    task_.pRecalcRequests = tdListNew(POINTER_BYTES);
    task_.pRecalcRequestMap = tSimpleHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    task_.pGroupPendingRecalcs = tSimpleHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    task_.pUserRecalcRequests = taosArrayInit(0, sizeof(SStreamRecalcReq));
    task_.pHistoryCutoffTime = tSimpleHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    task_.readerList = taosArrayInit(0, sizeof(SStreamTaskAddr));
    task_.runnerList = taosArrayInit(0, sizeof(SStreamRunnerTarget));
    task_.pGroupRunning = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    task_.pSessionRunning = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    ASSERT_NE(task_.pRecalcRequests, nullptr);
    ASSERT_NE(task_.pRecalcRequestMap, nullptr);
    ASSERT_NE(task_.pGroupPendingRecalcs, nullptr);
    ASSERT_NE(task_.pUserRecalcRequests, nullptr);
    ASSERT_NE(task_.pHistoryCutoffTime, nullptr);
    ASSERT_NE(task_.readerList, nullptr);
    ASSERT_NE(task_.runnerList, nullptr);
    ASSERT_NE(task_.pGroupRunning, nullptr);
    ASSERT_NE(task_.pSessionRunning, nullptr);

    context_.pTask = &task_;
    context_.sessionId = kRealtimeSessionId;
    context_.status = STRIGGER_CONTEXT_FETCH_META;
    context_.haveReadCheckpoint = true;
    context_.boundDetermined = true;
    context_.lastCheckpointTime = taosGetTimestampNs();
    context_.pGroups = tSimpleHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    context_.pReaderWalProgress = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    context_.groupsToDelete = taosArrayInit(0, sizeof(int64_t));
    context_.pMaxDelayHeap = heapCreate(compareRealtimeMaxDelayGroups);
    ASSERT_NE(context_.pGroups, nullptr);
    ASSERT_NE(context_.pReaderWalProgress, nullptr);
    ASSERT_NE(context_.groupsToDelete, nullptr);
    ASSERT_NE(context_.pMaxDelayHeap, nullptr);
    TD_DLIST_INIT(&context_.groupsToCheck);
    TD_DLIST_INIT(&context_.groupsToCheckIdle);
    tdListInit(&context_.retryPullReqs, POINTER_BYTES);
    tdListInit(&context_.retryCalcReqs, POINTER_BYTES);
    tdListInit(&context_.dropTableReqs, POINTER_BYTES);
    task_.pRealtimeContext = &context_;

    AddGroup(11);
    AddGroup(22);
  }

  void TearDown() override {
    SSTriggerRecalcRequest* request = nullptr;
    while (stTriggerTaskFetchRecalcRequest(&task_, &request) == TSDB_CODE_SUCCESS && request != nullptr) {
      DestroyRecalcRequest(&request);
    }
    int32_t iter = 0;
    auto*   pending =
        static_cast<SSTriggerGroupPendingRecalc*>(tSimpleHashIterate(task_.pGroupPendingRecalcs, nullptr, &iter));
    while (pending != nullptr) {
      while ((request = TD_DLIST_HEAD(&pending->pendingRequests)) != nullptr) {
        TD_DLIST_POP(&pending->pendingRequests, request);
        DestroyRecalcRequest(&request);
      }
      pending =
          static_cast<SSTriggerGroupPendingRecalc*>(tSimpleHashIterate(task_.pGroupPendingRecalcs, pending, &iter));
    }
    tdListEmpty(&context_.dropTableReqs);
    tdListEmpty(&context_.retryCalcReqs);
    tdListEmpty(&context_.retryPullReqs);
    heapDestroy(context_.pMaxDelayHeap);
    taosArrayDestroy(context_.groupsToDelete);
    tSimpleHashCleanup(context_.pReaderWalProgress);
    tSimpleHashCleanup(context_.pGroups);
    for (auto* group : groups_) taosMemoryFree(group);
    tSimpleHashCleanup(task_.pSessionRunning);
    tSimpleHashCleanup(task_.pGroupRunning);
    taosArrayDestroy(task_.runnerList);
    taosArrayDestroy(task_.readerList);
    tSimpleHashCleanup(task_.pHistoryCutoffTime);
    taosArrayDestroy(task_.pUserRecalcRequests);
    tSimpleHashCleanup(task_.pGroupPendingRecalcs);
    tSimpleHashCleanup(task_.pRecalcRequestMap);
    task_.pRecalcRequests = static_cast<SList*>(tdListFree(task_.pRecalcRequests));
    stRecalcTrackerDestroy(&task_.pRecalcTracker);
  }

  void AddGroup(int64_t gid) {
    auto* group = static_cast<SSTriggerRealtimeGroup*>(taosMemoryCalloc(1, sizeof(SSTriggerRealtimeGroup)));
    ASSERT_NE(group, nullptr);
    group->pContext = &context_;
    group->gid = gid;
    group->oldThreshold = 0;
    group->newThreshold = TSKEY_MAX;
    group->prevWindow = {.skey = TSKEY_MIN, .ekey = TSKEY_MIN};
    ASSERT_EQ(tSimpleHashPut(context_.pGroups, &gid, sizeof(gid), &group, POINTER_BYTES), TSDB_CODE_SUCCESS);
    groups_.push_back(group);
  }

  int32_t DriveUserRecalc(int64_t recalcId, TSKEY start, TSKEY end) {
    SStreamRecalcReq recalc = {.recalcId = recalcId, .start = start, .end = end};
    if (taosArrayPush(task_.pUserRecalcRequests, &recalc) == nullptr) return terrno;
    SSTriggerCtrlRequest request = {.type = STRIGGER_CTRL_START,
                                    .streamId = task_.task.streamId,
                                    .taskId = task_.task.taskId,
                                    .sessionId = kRealtimeSessionId};
    SRpcMsg              response = {.msgType = TDMT_STREAM_TRIGGER_CTRL};
    response.contLen = tSerializeSTriggerCtrlRequest(nullptr, 0, &request);
    response.pCont = rpcMallocCont(response.contLen);
    if (response.pCont == nullptr) return terrno;
    tSerializeSTriggerCtrlRequest(response.pCont, response.contLen, &request);
    int64_t errorTaskId = 0;
    int32_t code = stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId);
    rpcFreeCont(response.pCont);
    return code;
  }

  SStreamTriggerTask                   task_ = {};
  SSTriggerRealtimeContext             context_ = {};
  std::vector<SSTriggerRealtimeGroup*> groups_;
};

TEST_F(StreamTriggerRecalcOwnershipTest, UserRecalcSnapshotsGroupsAndKeepsOriginalRange) {
  ASSERT_EQ(DriveUserRecalc(41, 100, 200), TSDB_CODE_SUCCESS);

  auto snapshot = CopyRecalcSnapshot(task_.pRecalcTracker, 41);
  EXPECT_EQ(snapshot.start, 100);
  EXPECT_EQ(snapshot.end, 200);
  EXPECT_EQ(snapshot.progressPct, 0);
  EXPECT_EQ(snapshot.status, STREAM_RECALC_STATUS_PENDING);

  SSTriggerRecalcRequest* first = nullptr;
  SSTriggerRecalcRequest* second = nullptr;
  ASSERT_EQ(stTriggerTaskFetchRecalcRequest(&task_, &first), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stTriggerTaskFetchRecalcRequest(&task_, &second), TSDB_CODE_SUCCESS);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_NE(first->gid, second->gid);
  ASSERT_EQ(taosArrayGetSize(first->pContributors), 1);
  ASSERT_EQ(taosArrayGetSize(second->pContributors), 1);
  const auto* firstContributor = static_cast<const SStreamRecalcContributor*>(taosArrayGet(first->pContributors, 0));
  EXPECT_EQ(firstContributor->recalcId, 41);
  EXPECT_EQ(firstContributor->requestedRange.start, 100);
  EXPECT_EQ(firstContributor->requestedRange.end, 200);
  DestroyRecalcRequest(&first);
  DestroyRecalcRequest(&second);
}

TEST_F(StreamTriggerRecalcOwnershipTest, PendingMergeKeepsDistinctContributors) {
  SArray* groups = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(groups, nullptr);
  int64_t gid = groups_[0]->gid;
  ASSERT_NE(taosArrayPush(groups, &gid), nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(task_.pRecalcTracker, 51, {250, 300}, groups), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerRegisterJob(task_.pRecalcTracker, 52, {100, 200}, groups), TSDB_CODE_SUCCESS);
  taosArrayDestroy(groups);

  STimeWindow later = {.skey = 250, .ekey = 299};
  STimeWindow earlier = {.skey = 100, .ekey = 199};
  ASSERT_EQ(stTriggerTaskAddRecalcRequest(&task_, groups_[0], &later, false, false, false, 51), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stTriggerTaskAddRecalcRequest(&task_, groups_[0], &earlier, false, false, false, 52), TSDB_CODE_SUCCESS);

  auto* pending =
      static_cast<SSTriggerGroupPendingRecalc*>(tSimpleHashGet(task_.pGroupPendingRecalcs, &gid, sizeof(gid)));
  ASSERT_NE(pending, nullptr);
  ASSERT_EQ(TD_DLIST_NELES(&pending->pendingRequests), 1);
  auto* merged = TD_DLIST_HEAD(&pending->pendingRequests);
  EXPECT_EQ(merged->calcRange.skey, 100);
  EXPECT_EQ(merged->calcRange.ekey, 299);
  EXPECT_EQ(taosArrayGetSize(merged->pContributors), 2);
}

TEST_F(StreamTriggerRecalcOwnershipTest, SameContributorReplayIsDeduplicated) {
  ASSERT_EQ(DriveUserRecalc(61, 100, 200), TSDB_CODE_SUCCESS);
  ASSERT_EQ(DriveUserRecalc(61, 100, 200), TSDB_CODE_SUCCESS);

  for (int32_t i = 0; i < 2; ++i) {
    SSTriggerRecalcRequest* request = nullptr;
    ASSERT_EQ(stTriggerTaskFetchRecalcRequest(&task_, &request), TSDB_CODE_SUCCESS);
    ASSERT_NE(request, nullptr);
    EXPECT_EQ(taosArrayGetSize(request->pContributors), 1);
    DestroyRecalcRequest(&request);
  }
}

TEST_F(StreamTriggerRecalcOwnershipTest, FetchedRequestNoLongerAcceptsMerge) {
  ASSERT_EQ(DriveUserRecalc(71, 100, 200), TSDB_CODE_SUCCESS);
  SSTriggerRecalcRequest* fetched = nullptr;
  ASSERT_EQ(stTriggerTaskFetchRecalcRequest(&task_, &fetched), TSDB_CODE_SUCCESS);
  ASSERT_NE(fetched, nullptr);
  const int64_t fetchedGid = fetched->gid;

  SArray* groups = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(groups, nullptr);
  ASSERT_NE(taosArrayPush(groups, &fetchedGid), nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(task_.pRecalcTracker, 72, {150, 250}, groups), TSDB_CODE_SUCCESS);
  taosArrayDestroy(groups);
  STimeWindow range = {.skey = 150, .ekey = 249};
  auto*       group =
      static_cast<SSTriggerRealtimeGroup**>(tSimpleHashGet(context_.pGroups, &fetchedGid, sizeof(fetchedGid)));
  ASSERT_NE(group, nullptr);
  ASSERT_EQ(stTriggerTaskAddRecalcRequest(&task_, *group, &range, false, true, false, 72), TSDB_CODE_SUCCESS);

  SSTriggerRecalcRequest* next = nullptr;
  ASSERT_EQ(stTriggerTaskFetchRecalcRequest(&task_, &next), TSDB_CODE_SUCCESS);
  ASSERT_NE(next, nullptr);
  EXPECT_EQ(taosArrayGetSize(fetched->pContributors), 1);
  EXPECT_EQ(taosArrayGetSize(next->pContributors), 1);
  DestroyRecalcRequest(&fetched);
  DestroyRecalcRequest(&next);
}

TEST_F(StreamTriggerRecalcOwnershipTest, EmptyMaximumRangeFinishesWithoutEnqueueOrUnderflow) {
  ASSERT_EQ(DriveUserRecalc(73, TSKEY_MAX, TSKEY_MAX), TSDB_CODE_SUCCESS);
  const auto snapshot = CopyRecalcSnapshot(task_.pRecalcTracker, 73);
  EXPECT_EQ(snapshot.start, TSKEY_MAX);
  EXPECT_EQ(snapshot.end, TSKEY_MAX);
  EXPECT_EQ(snapshot.status, STREAM_RECALC_STATUS_FINISHED);
  EXPECT_EQ(snapshot.progressPct, 100);

  SSTriggerRecalcRequest* request = nullptr;
  ASSERT_EQ(stTriggerTaskFetchRecalcRequest(&task_, &request), TSDB_CODE_SUCCESS);
  EXPECT_EQ(request, nullptr);
}

TEST_F(StreamTriggerRecalcOwnershipTest, MaximumEndRequestMergesWithoutSignedOverflow) {
  const int64_t gid = groups_[0]->gid;
  STimeWindow   existingRange = {.skey = TSKEY_MAX - 100, .ekey = TSKEY_MAX - 50};
  ASSERT_EQ(stTriggerTaskAddRecalcRequest(&task_, groups_[0], &existingRange, false, true, false, 0),
            TSDB_CODE_SUCCESS);

  ASSERT_EQ(DriveUserRecalc(76, TSKEY_MAX - 75, TSKEY_MAX), TSDB_CODE_SUCCESS);
  int32_t                 requestCount = 0;
  int32_t                 matchingGroupCount = 0;
  SSTriggerRecalcRequest* request = nullptr;
  while (stTriggerTaskFetchRecalcRequest(&task_, &request) == TSDB_CODE_SUCCESS && request != nullptr) {
    ++requestCount;
    if (request->gid == gid) {
      ++matchingGroupCount;
      EXPECT_EQ(request->calcRange.skey, TSKEY_MAX - 100);
      EXPECT_EQ(request->calcRange.ekey, TSKEY_MAX - 1);
      ASSERT_EQ(taosArrayGetSize(request->pContributors), 1);
      const auto* contributor = static_cast<const SStreamRecalcContributor*>(taosArrayGet(request->pContributors, 0));
      EXPECT_EQ(contributor->requestedRange.end, TSKEY_MAX);
    }
    DestroyRecalcRequest(&request);
  }
  EXPECT_EQ(requestCount, 2);
  EXPECT_EQ(matchingGroupCount, 1);
}

TEST_F(StreamTriggerRecalcOwnershipTest, LaterGroupFailureRollsBackEarlierMergeExactly) {
  std::vector<SSTriggerRealtimeGroup*> orderedGroups;
  int32_t                              iter = 0;
  void*                                value = tSimpleHashIterate(context_.pGroups, nullptr, &iter);
  while (value != nullptr) {
    orderedGroups.push_back(*static_cast<SSTriggerRealtimeGroup**>(value));
    value = tSimpleHashIterate(context_.pGroups, value, &iter);
  }
  ASSERT_EQ(orderedGroups.size(), 2U);

  STimeWindow firstExistingRange = {.skey = 120, .ekey = 129};
  ASSERT_EQ(stTriggerTaskAddRecalcRequest(&task_, orderedGroups[0], &firstExistingRange, false, true, false, 0),
            TSDB_CODE_SUCCESS);

  SArray* oldGroups = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(oldGroups, nullptr);
  ASSERT_NE(taosArrayPush(oldGroups, &orderedGroups[1]->gid), nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(task_.pRecalcTracker, 74, {100, 200}, oldGroups), TSDB_CODE_SUCCESS);
  taosArrayDestroy(oldGroups);
  STimeWindow staleRange = {.skey = 100, .ekey = 199};
  ASSERT_EQ(stTriggerTaskAddRecalcRequest(&task_, orderedGroups[1], &staleRange, false, true, false, 74),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerFailJob(task_.pRecalcTracker, 74, TSDB_CODE_INTERNAL_ERROR), TSDB_CODE_SUCCESS);
  for (int64_t recalcId = 10000; recalcId < 10100; ++recalcId) {
    ASSERT_EQ(stRecalcTrackerRegisterJob(task_.pRecalcTracker, recalcId, {0, 0}, nullptr), TSDB_CODE_SUCCESS);
  }

  auto* firstList = static_cast<SSTriggerRecalcReqList*>(
      tSimpleHashGet(task_.pRecalcRequestMap, &orderedGroups[0]->gid, sizeof(orderedGroups[0]->gid)));
  auto* secondList = static_cast<SSTriggerRecalcReqList*>(
      tSimpleHashGet(task_.pRecalcRequestMap, &orderedGroups[1]->gid, sizeof(orderedGroups[1]->gid)));
  ASSERT_NE(firstList, nullptr);
  ASSERT_NE(secondList, nullptr);
  auto* firstExisting = TD_DLIST_HEAD(firstList);
  auto* secondExisting = TD_DLIST_HEAD(secondList);
  ASSERT_NE(firstExisting, nullptr);
  ASSERT_NE(secondExisting, nullptr);
  SSHashObj*  firstVersions = firstExisting->pTsdbVersions;
  STimeWindow firstScanRange = firstExisting->scanRange;
  EXPECT_EQ(firstExisting->pContributors, nullptr);
  ASSERT_EQ(taosArrayGetSize(secondExisting->pContributors), 1);

  EXPECT_EQ(DriveUserRecalc(74, 100, 200), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(firstExisting->scanRange.skey, firstScanRange.skey);
  EXPECT_EQ(firstExisting->scanRange.ekey, firstScanRange.ekey);
  EXPECT_EQ(firstExisting->calcRange.skey, 120);
  EXPECT_EQ(firstExisting->calcRange.ekey, 129);
  EXPECT_EQ(firstExisting->pTsdbVersions, firstVersions);
  EXPECT_EQ(firstExisting->pContributors, nullptr);
  EXPECT_EQ(secondExisting->calcRange.skey, 100);
  EXPECT_EQ(secondExisting->calcRange.ekey, 199);
  ASSERT_EQ(taosArrayGetSize(secondExisting->pContributors), 1);
  EXPECT_EQ(CopyRecalcSnapshot(task_.pRecalcTracker, 74).status, STREAM_RECALC_STATUS_FAILED);
}

TEST_F(StreamTriggerRecalcOwnershipTest, FatalBeforeFirstStepFailsAcceptedJob) {
  SArray* groups = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(groups, nullptr);
  int64_t gid = groups_[0]->gid;
  ASSERT_NE(taosArrayPush(groups, &gid), nullptr);
  ASSERT_EQ(stRecalcTrackerRegisterJob(task_.pRecalcTracker, 75, {100, 200}, groups), TSDB_CODE_SUCCESS);
  taosArrayDestroy(groups);

  STimeWindow range = {.skey = 100, .ekey = 199};
  ASSERT_EQ(stTriggerTaskAddRecalcRequest(&task_, groups_[0], &range, false, true, false, 75), TSDB_CODE_SUCCESS);

  SSTriggerHistoryContext history = {};
  history.pTask = &task_;
  history.sessionId = kHistorySessionId;
  history.status = STRIGGER_CONTEXT_WAIT_RECALC_REQ;
  history.pReaderTsdbProgress = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  ASSERT_NE(history.pReaderTsdbProgress, nullptr);
  SStreamTaskAddr       reader = {.taskId = 0x702, .nodeId = 7};
  SSTriggerTsdbProgress progress = {.pTaskAddr = &reader};
  ASSERT_EQ(
      tSimpleHashPut(history.pReaderTsdbProgress, &reader.nodeId, sizeof(reader.nodeId), &progress, sizeof(progress)),
      TSDB_CODE_SUCCESS);
  task_.pHistoryContext = &history;

  SSTriggerCtrlRequest request = {.type = STRIGGER_CTRL_START,
                                  .streamId = task_.task.streamId,
                                  .taskId = task_.task.taskId,
                                  .sessionId = kHistorySessionId};
  SRpcMsg              response = {.msgType = TDMT_STREAM_TRIGGER_CTRL};
  response.contLen = tSerializeSTriggerCtrlRequest(nullptr, 0, &request);
  response.pCont = rpcMallocCont(response.contLen);
  ASSERT_NE(response.pCont, nullptr);
  ASSERT_EQ(tSerializeSTriggerCtrlRequest(response.pCont, response.contLen, &request), response.contLen);
  int64_t errorTaskId = 0;
  EXPECT_EQ(stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId), TSDB_CODE_INTERNAL_ERROR);
  EXPECT_EQ(CopyRecalcSnapshot(task_.pRecalcTracker, 75).status, STREAM_RECALC_STATUS_FAILED);

  rpcFreeCont(response.pCont);
  taosArrayDestroy(history.pContributors);
  tSimpleHashCleanup(history.pReaderTsdbProgress);
  task_.pHistoryContext = nullptr;
}

struct RecalcBarrierSendState {
  SMsgSendInfo* retryAhandle = nullptr;
  SMsgSendInfo* pullAhandle = nullptr;
  int32_t       sendCalls = 0;
  int32_t       releaseCalls = 0;
  int32_t       completeReaderCalls = 0;
  int32_t       completeReaderCallsAtLastSend = 0;
};

RecalcBarrierSendState gRecalcBarrierSendState;
uint8_t                gRecalcBarrierCheckpoint[128];
int64_t                gRecalcBarrierCheckpointLen = 0;

int32_t captureRecalcBarrierRetry(const SEpSet*, SRpcMsg* pMsg) {
  ++gRecalcBarrierSendState.sendCalls;
  gRecalcBarrierSendState.completeReaderCallsAtLastSend = gRecalcBarrierSendState.completeReaderCalls;
  if (pMsg->msgType == TDMT_STREAM_TRIGGER_CALC) {
    gRecalcBarrierSendState.retryAhandle = static_cast<SMsgSendInfo*>(pMsg->info.ahandle);
  } else {
    gRecalcBarrierSendState.pullAhandle = static_cast<SMsgSendInfo*>(pMsg->info.ahandle);
  }
  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = nullptr;
  pMsg->info.ahandle = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t captureRecalcBarrierCompleteReader(SStreamRecalcTracker*, uint64_t, uint64_t) {
  ++gRecalcBarrierSendState.completeReaderCalls;
  return TSDB_CODE_SUCCESS;
}

int32_t readRecalcBarrierCheckpoint(int64_t, void** ppData, int64_t* pLen) {
  *ppData = taosMemoryMalloc(gRecalcBarrierCheckpointLen);
  if (*ppData == nullptr) return terrno;
  std::memcpy(*ppData, gRecalcBarrierCheckpoint, gRecalcBarrierCheckpointLen);
  *pLen = gRecalcBarrierCheckpointLen;
  return TSDB_CODE_SUCCESS;
}

int32_t releaseRecalcBarrierRequest(SStreamTriggerTask* pTask, SSTriggerCalcRequest** ppRequest, bool) {
  ++gRecalcBarrierSendState.releaseCalls;
  int64_t* pRunning = static_cast<int64_t*>(
      tSimpleHashGet(pTask->pSessionRunning, &(*ppRequest)->sessionId, sizeof((*ppRequest)->sessionId)));
  if (pRunning != nullptr && *pRunning > 0) --*pRunning;
  (*ppRequest)->progressStepId = 0;
  (*ppRequest)->progressRequestToken = 0;
  *ppRequest = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t compareRecalcBarrierHistoryGroups(const HeapNode* a, const HeapNode* b) {
  auto* groupA = reinterpret_cast<const SSTriggerHistoryGroup*>(reinterpret_cast<const char*>(a) -
                                                                offsetof(SSTriggerHistoryGroup, heapNode));
  auto* groupB = reinterpret_cast<const SSTriggerHistoryGroup*>(reinterpret_cast<const char*>(b) -
                                                                offsetof(SSTriggerHistoryGroup, heapNode));
  return groupA->pPendingCalcParams.neles > groupB->pPendingCalcParams.neles;
}

class StreamTriggerRecalcBarrierTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gRecalcBarrierSendState = {};
    stub_.set(tmsgSendReq, captureRecalcBarrierRetry);
    stub_.set(stTriggerTaskReleaseRequest, releaseRecalcBarrierRequest);
    ASSERT_EQ(stRecalcTrackerCreate(&task_.pRecalcTracker), TSDB_CODE_SUCCESS);
    task_.task.streamId = 0x900;
    task_.task.taskId = 0x901;
    task_.triggerType = STREAM_TRIGGER_COUNT;
    task_.historyCalcStarted = true;
    atomic_store_8(&task_.realtimeStarted, 1);
    task_.pRecalcRequests = tdListNew(POINTER_BYTES);
    task_.pRecalcRequestMap = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    task_.pGroupPendingRecalcs = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    task_.pUserRecalcRequests = taosArrayInit(0, sizeof(SStreamRecalcReq));
    task_.pHistoryCutoffTime = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    task_.runnerList = taosArrayInit_s(sizeof(SStreamRunnerTarget), 1);
    task_.readerList = taosArrayInit_s(sizeof(SStreamTaskAddr), 1);
    task_.pGroupRunning = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    task_.pSessionRunning = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    taosInitRWLatch(&task_.readerProgressLock);
    task_.pReaderProgressSnapshots = taosArrayInit(0, sizeof(SStreamReaderProgressSnapshot));
    ASSERT_NE(task_.pRecalcRequests, nullptr);
    ASSERT_NE(task_.pRecalcRequestMap, nullptr);
    ASSERT_NE(task_.pGroupPendingRecalcs, nullptr);
    ASSERT_NE(task_.pUserRecalcRequests, nullptr);
    ASSERT_NE(task_.pHistoryCutoffTime, nullptr);
    ASSERT_NE(task_.runnerList, nullptr);
    ASSERT_NE(task_.readerList, nullptr);
    ASSERT_NE(task_.pGroupRunning, nullptr);
    ASSERT_NE(task_.pSessionRunning, nullptr);
    ASSERT_NE(task_.pReaderProgressSnapshots, nullptr);
    auto* runner = static_cast<SStreamRunnerTarget*>(taosArrayGet(task_.runnerList, 0));
    runner->addr.taskId = kRunnerTaskId;
    auto* reader = static_cast<SStreamTaskAddr*>(taosArrayGet(task_.readerList, 0));
    reader->nodeId = 7;
    reader->taskId = 0x902;

    realtime_.pTask = &task_;
    realtime_.sessionId = kRealtimeSessionId;
    realtime_.status = STRIGGER_CONTEXT_FETCH_META;
    realtime_.haveReadCheckpoint = true;
    realtime_.boundDetermined = true;
    realtime_.lastCheckpointTime = taosGetTimestampNs();
    realtime_.pGroups = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    realtime_.pReaderWalProgress = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    realtime_.groupsToDelete = taosArrayInit(0, sizeof(int64_t));
    realtime_.pMaxDelayHeap = heapCreate(compareRealtimeMaxDelayGroups);
    ASSERT_NE(realtime_.pGroups, nullptr);
    ASSERT_NE(realtime_.pReaderWalProgress, nullptr);
    ASSERT_NE(realtime_.groupsToDelete, nullptr);
    ASSERT_NE(realtime_.pMaxDelayHeap, nullptr);
    TD_DLIST_INIT(&realtime_.groupsToCheck);
    TD_DLIST_INIT(&realtime_.groupsToCheckIdle);
    tdListInit(&realtime_.retryPullReqs, POINTER_BYTES);
    tdListInit(&realtime_.retryCalcReqs, POINTER_BYTES);
    tdListInit(&realtime_.dropTableReqs, POINTER_BYTES);
    SSTriggerWalProgress walProgress = {.pTaskAddr = reader, .lastScanVer = 1};
    walProgress.pullReq.base.sessionId = kRealtimeSessionId;
    ASSERT_EQ(tSimpleHashPut(realtime_.pReaderWalProgress, &reader->nodeId, sizeof(reader->nodeId), &walProgress,
                             sizeof(walProgress)),
              TSDB_CODE_SUCCESS);
    group_.pContext = &realtime_;
    group_.gid = 11;
    group_.oldThreshold = 0;
    group_.newThreshold = TSKEY_MAX;
    group_.prevWindow = {.skey = TSKEY_MIN, .ekey = TSKEY_MIN};
    SSTriggerRealtimeGroup* group = &group_;
    ASSERT_EQ(tSimpleHashPut(realtime_.pGroups, &group_.gid, sizeof(group_.gid), &group, POINTER_BYTES),
              TSDB_CODE_SUCCESS);
    task_.pRealtimeContext = &realtime_;

    context_.pTask = &task_;
    context_.sessionId = kHistorySessionId;
    context_.status = STRIGGER_CONTEXT_WAIT_RECALC_REQ;
    tdListInit(&context_.retryPullReqs, POINTER_BYTES);
    tdListInit(&context_.retryCalcReqs, POINTER_BYTES);
    context_.pReaderTsdbProgress = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    context_.pGroups = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    context_.pTrigDataBlocks = taosArrayInit(0, POINTER_BYTES);
    ASSERT_NE(context_.pReaderTsdbProgress, nullptr);
    ASSERT_NE(context_.pGroups, nullptr);
    ASSERT_NE(context_.pTrigDataBlocks, nullptr);
    SSTriggerTsdbProgress progress = {};
    progress.pTaskAddr = reader;
    progress.pMetadatas = taosArrayInit(0, POINTER_BYTES);
    ASSERT_NE(progress.pMetadatas, nullptr);
    progress.pullReq.base.streamId = task_.task.streamId;
    progress.pullReq.base.readerTaskId = reader->taskId;
    progress.pullReq.base.sessionId = kHistorySessionId;
    ASSERT_EQ(tSimpleHashPut(context_.pReaderTsdbProgress, &reader->nodeId, sizeof(reader->nodeId), &progress,
                             sizeof(progress)),
              TSDB_CODE_SUCCESS);
    progress_ = static_cast<SSTriggerTsdbProgress*>(
        tSimpleHashGet(context_.pReaderTsdbProgress, &reader->nodeId, sizeof(reader->nodeId)));
    task_.pHistoryContext = &context_;

    calcRequest_.streamId = task_.task.streamId;
    calcRequest_.runnerTaskId = kRunnerTaskId;
    calcRequest_.sessionId = kHistorySessionId;
    calcRequest_.gid = 11;
    calcRequest_.params = taosArrayInit(0, sizeof(SSTriggerCalcParam));
    calcRequest_.groupColVals = taosArrayInit(0, sizeof(SStreamGroupValue));
    ASSERT_NE(calcRequest_.params, nullptr);
    ASSERT_NE(calcRequest_.groupColVals, nullptr);
  }

  void TearDown() override {
    if (gRecalcBarrierSendState.retryAhandle != nullptr) {
      destroyAhandle(gRecalcBarrierSendState.retryAhandle);
      gRecalcBarrierSendState.retryAhandle = nullptr;
    }
    if (gRecalcBarrierSendState.pullAhandle != nullptr) {
      destroyAhandle(gRecalcBarrierSendState.pullAhandle);
      gRecalcBarrierSendState.pullAhandle = nullptr;
    }
    if (historyGroupInitialized_) {
      if (historyGroup_.inMaxDelayHeap) heapRemove(context_.pMaxDelayHeap, &historyGroup_.heapNode);
      taosObjListClear(&historyGroup_.pPendingParWinCalcParams);
      taosObjListClear(&historyGroup_.pPendingCalcParams);
      taosObjPoolDestroy(&context_.calcParamPool);
    }
    if (historyHeapInitialized_) heapDestroy(context_.pMaxDelayHeap);
    context_.pMaxDelayHeap = nullptr;
    taosArrayDestroy(context_.pContributors);
    context_.pContributors = nullptr;
    tDestroySTriggerCalcRequest(&calcRequest_);
    SSTriggerRecalcRequest* request = nullptr;
    while (stTriggerTaskFetchRecalcRequest(&task_, &request) == TSDB_CODE_SUCCESS && request != nullptr) {
      DestroyRecalcRequest(&request);
    }
    int32_t iter = 0;
    auto*   pending =
        static_cast<SSTriggerGroupPendingRecalc*>(tSimpleHashIterate(task_.pGroupPendingRecalcs, nullptr, &iter));
    while (pending != nullptr) {
      while ((request = TD_DLIST_HEAD(&pending->pendingRequests)) != nullptr) {
        TD_DLIST_POP(&pending->pendingRequests, request);
        DestroyRecalcRequest(&request);
      }
      pending =
          static_cast<SSTriggerGroupPendingRecalc*>(tSimpleHashIterate(task_.pGroupPendingRecalcs, pending, &iter));
    }
    auto* reader = static_cast<SStreamTaskAddr*>(taosArrayGet(task_.readerList, 0));
    auto* progress = static_cast<SSTriggerTsdbProgress*>(
        tSimpleHashGet(context_.pReaderTsdbProgress, &reader->nodeId, sizeof(reader->nodeId)));
    taosArrayDestroyP(progress->pMetadatas, reinterpret_cast<FDelete>(blockDataDestroy));
    progress->pMetadatas = nullptr;
    tSimpleHashCleanup(context_.pReaderTsdbProgress);
    taosArrayDestroyP(context_.pTrigDataBlocks, reinterpret_cast<FDelete>(blockDataDestroy));
    tSimpleHashCleanup(context_.pGroups);
    tSimpleHashCleanup(context_.pFirstTsMap);
    tdListEmpty(&context_.retryCalcReqs);
    tdListEmpty(&context_.retryPullReqs);
    tdListEmpty(&realtime_.dropTableReqs);
    tdListEmpty(&realtime_.retryCalcReqs);
    tdListEmpty(&realtime_.retryPullReqs);
    heapDestroy(realtime_.pMaxDelayHeap);
    taosArrayDestroy(realtime_.groupsToDelete);
    tSimpleHashCleanup(realtime_.pReaderWalProgress);
    tSimpleHashCleanup(realtime_.pGroups);
    tSimpleHashCleanup(task_.pSessionRunning);
    tSimpleHashCleanup(task_.pGroupRunning);
    taosArrayDestroy(task_.pReaderProgressSnapshots);
    taosArrayDestroy(task_.readerList);
    taosArrayDestroy(task_.runnerList);
    tSimpleHashCleanup(task_.pHistoryCutoffTime);
    taosArrayDestroy(task_.pUserRecalcRequests);
    tSimpleHashCleanup(task_.pGroupPendingRecalcs);
    tSimpleHashCleanup(task_.pRecalcRequestMap);
    task_.pRecalcRequests = static_cast<SList*>(tdListFree(task_.pRecalcRequests));
    stRecalcTrackerDestroy(&task_.pRecalcTracker);
  }

  SArray* RegisterContributors(std::initializer_list<int64_t> recalcIds) {
    SArray* contributors = nullptr;
    for (int64_t recalcId : recalcIds) {
      SStreamRecalcReq recalc = {.recalcId = recalcId, .start = 100, .end = 200};
      if (taosArrayPush(task_.pUserRecalcRequests, &recalc) == nullptr) {
        taosArrayDestroy(contributors);
        return nullptr;
      }
      SSTriggerCtrlRequest request = {.type = STRIGGER_CTRL_START,
                                      .streamId = task_.task.streamId,
                                      .taskId = task_.task.taskId,
                                      .sessionId = kRealtimeSessionId};
      SRpcMsg              response = {.msgType = TDMT_STREAM_TRIGGER_CTRL};
      response.contLen = tSerializeSTriggerCtrlRequest(nullptr, 0, &request);
      response.pCont = rpcMallocCont(response.contLen);
      if (response.pCont == nullptr) {
        taosArrayDestroy(contributors);
        return nullptr;
      }
      tSerializeSTriggerCtrlRequest(response.pCont, response.contLen, &request);
      const int32_t readerCount = TARRAY_SIZE(task_.readerList);
      TARRAY_SIZE(task_.readerList) = 0;
      int64_t errorTaskId = 0;
      int32_t code = stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId);
      TARRAY_SIZE(task_.readerList) = readerCount;
      rpcFreeCont(response.pCont);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(contributors);
        return nullptr;
      }
      SSTriggerRecalcRequest* queued = nullptr;
      if (stTriggerTaskFetchRecalcRequest(&task_, &queued) != TSDB_CODE_SUCCESS || queued == nullptr ||
          stRecalcContributorsMerge(&contributors, queued->pContributors) != TSDB_CODE_SUCCESS) {
        DestroyRecalcRequest(&queued);
        taosArrayDestroy(contributors);
        return nullptr;
      }
      DestroyRecalcRequest(&queued);
    }
    return contributors;
  }

  uint64_t BeginReaderStep(int64_t recalcId) {
    SArray* contributors = RegisterContributors({recalcId});
    EXPECT_NE(contributors, nullptr);
    uint64_t stepId = 0;
    EXPECT_EQ(stRecalcTrackerBeginStep(task_.pRecalcTracker, 11, {100, 200}, contributors, &stepId), TSDB_CODE_SUCCESS);
    EXPECT_EQ(stRecalcTrackerAddReader(task_.pRecalcTracker, stepId, 1), TSDB_CODE_SUCCESS);
    EXPECT_EQ(stRecalcTrackerSetTriggerDone(task_.pRecalcTracker, stepId, 0), TSDB_CODE_SUCCESS);
    taosArrayDestroy(contributors);
    return stepId;
  }

  uint64_t BeginRunnerStep(std::initializer_list<int64_t> recalcIds, uint64_t requestToken) {
    SArray* contributors = RegisterContributors(recalcIds);
    EXPECT_NE(contributors, nullptr);
    uint64_t stepId = 0;
    EXPECT_EQ(stRecalcTrackerBeginStep(task_.pRecalcTracker, 11, {100, 200}, contributors, &stepId), TSDB_CODE_SUCCESS);
    EXPECT_EQ(stRecalcTrackerAddRunner(task_.pRecalcTracker, stepId, requestToken), TSDB_CODE_SUCCESS);
    EXPECT_EQ(stRecalcTrackerSetTriggerDone(task_.pRecalcTracker, stepId, 0), TSDB_CODE_SUCCESS);
    taosArrayDestroy(contributors);
    calcRequest_.progressStepId = stepId;
    calcRequest_.progressRequestToken = requestToken;
    return stepId;
  }

  int32_t ProcessCalcResponse(int32_t responseCode, SSTriggerAHandle* responseAhandle = nullptr) {
    SSTriggerAHandle localAhandle = {.streamId = task_.task.streamId,
                                     .taskId = task_.task.taskId,
                                     .sessionId = kHistorySessionId,
                                     .param = &calcRequest_,
                                     .progressStepId = calcRequest_.progressStepId,
                                     .progressRequestToken = calcRequest_.progressRequestToken};
    if (responseAhandle == nullptr) responseAhandle = &localAhandle;
    SMsgSendInfo responseSendInfo = {};
    responseSendInfo.param = responseAhandle;
    SRpcMsg response = {};
    response.msgType = TDMT_STREAM_TRIGGER_CALC_RSP;
    response.code = responseCode;
    response.info.ahandle = &responseSendInfo;
    int64_t errorTaskId = 0;
    return stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId);
  }

  int32_t ProcessHistoryStart() {
    SSTriggerCtrlRequest request = {.type = STRIGGER_CTRL_START,
                                    .streamId = task_.task.streamId,
                                    .taskId = task_.task.taskId,
                                    .sessionId = kHistorySessionId};
    SRpcMsg              response = {.msgType = TDMT_STREAM_TRIGGER_CTRL};
    response.contLen = tSerializeSTriggerCtrlRequest(nullptr, 0, &request);
    response.pCont = rpcMallocCont(response.contLen);
    if (response.pCont == nullptr) return terrno;
    tSerializeSTriggerCtrlRequest(response.pCont, response.contLen, &request);
    int64_t errorTaskId = 0;
    int32_t code = stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId);
    rpcFreeCont(response.pCont);
    return code;
  }

  int32_t ProcessRealtimeStart() {
    SSTriggerCtrlRequest request = {.type = STRIGGER_CTRL_START,
                                    .streamId = task_.task.streamId,
                                    .taskId = task_.task.taskId,
                                    .sessionId = kRealtimeSessionId};
    SRpcMsg              response = {.msgType = TDMT_STREAM_TRIGGER_CTRL};
    response.contLen = tSerializeSTriggerCtrlRequest(nullptr, 0, &request);
    response.pCont = rpcMallocCont(response.contLen);
    if (response.pCont == nullptr) return terrno;
    tSerializeSTriggerCtrlRequest(response.pCont, response.contLen, &request);
    int64_t errorTaskId = 0;
    int32_t code = stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId);
    rpcFreeCont(response.pCont);
    return code;
  }

  void EncodeHistoryCheckpoint(bool finished, int32_t checkpointVgId = 0) {
    SEncoder encoder = {};
    tEncoderInit(&encoder, gRecalcBarrierCheckpoint, sizeof(gRecalcBarrierCheckpoint));
    ASSERT_EQ(tStartEncode(&encoder), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tEncodeI32(&encoder, 1), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tEncodeI64(&encoder, task_.task.streamId), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tEncodeI32(&encoder, 1), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tEncodeI32(&encoder, checkpointVgId == 0 ? 0 : 1), TSDB_CODE_SUCCESS);
    if (checkpointVgId != 0) {
      ASSERT_EQ(tEncodeI32(&encoder, checkpointVgId), TSDB_CODE_SUCCESS);
      ASSERT_EQ(tEncodeI64(&encoder, 1), TSDB_CODE_SUCCESS);
    }
    ASSERT_EQ(tEncodeI32(&encoder, 1), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tEncodeI64(&encoder, 11), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tEncodeI64(&encoder, 199), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tEncodeI8(&encoder, finished ? 1 : 0), TSDB_CODE_SUCCESS);
    tEndEncode(&encoder);
    gRecalcBarrierCheckpointLen = encoder.pos;
    tEncoderClear(&encoder);
  }

  void SetHistoryCutoff(TSKEY cutoff) {
    const int64_t gid = 11;
    ASSERT_EQ(tSimpleHashPut(task_.pHistoryCutoffTime, &gid, sizeof(gid), &cutoff, sizeof(cutoff)), TSDB_CODE_SUCCESS);
  }

  int32_t QueueFillHistory(TSKEY start, TSKEY cutoff) {
    task_.fillHistory = true;
    task_.fillHistoryStartTime = start;
    SetHistoryCutoff(cutoff);
    return stTriggerTaskAddRecalcRequest(&task_, nullptr, nullptr, true, false, false, 0);
  }

  int32_t CopyHistoryProgress(bool* pValid) {
    int32_t pct = -1;
    SArray* snapshots = nullptr;
    EXPECT_EQ(stRecalcTrackerCopySnapshot(task_.pRecalcTracker, pValid, &pct, &snapshots), TSDB_CODE_SUCCESS);
    taosArrayDestroy(snapshots);
    return pct;
  }

  uint64_t PreparePendingHistoryStep(int64_t recalcId, bool parentWindow) {
    SArray* contributors = RegisterContributors({recalcId});
    EXPECT_NE(contributors, nullptr);
    context_.pContributors = contributors;
    context_.gid = 11;
    context_.status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
    context_.needTsdbMeta = true;
    context_.finishCheck = false;
    context_.scanRange = {100, 200};
    context_.calcRange = context_.scanRange;
    context_.stepRange = {100, 149};
    task_.historyStep = 50;
    task_.calcEventType = STRIGGER_EVENT_WINDOW_CLOSE;

    EXPECT_EQ(taosObjPoolInit(&context_.calcParamPool, 2, sizeof(SSTriggerCalcParam)), TSDB_CODE_SUCCESS);
    historyGroup_.pContext = &context_;
    historyGroup_.gid = 11;
    EXPECT_EQ(taosObjListInit(&historyGroup_.pPendingParWinCalcParams, &context_.calcParamPool), TSDB_CODE_SUCCESS);
    EXPECT_EQ(taosObjListInit(&historyGroup_.pPendingCalcParams, &context_.calcParamPool), TSDB_CODE_SUCCESS);
    SSTriggerCalcParam calcParam = {.wstart = 100, .wend = 110};
    SObjList* pending = parentWindow ? &historyGroup_.pPendingParWinCalcParams : &historyGroup_.pPendingCalcParams;
    EXPECT_EQ(taosObjListAppend(pending, &calcParam), TSDB_CODE_SUCCESS);
    context_.pMaxDelayHeap = heapCreate(compareRecalcBarrierHistoryGroups);
    EXPECT_NE(context_.pMaxDelayHeap, nullptr);
    historyHeapInitialized_ = true;
    heapInsert(context_.pMaxDelayHeap, &historyGroup_.heapNode);
    historyGroup_.inMaxDelayHeap = true;
    context_.pMinGroup = nullptr;
    historyGroupInitialized_ = true;

    uint64_t stepId = 0;
    EXPECT_EQ(stRecalcTrackerBeginStep(task_.pRecalcTracker, 11, {100, 150}, contributors, &stepId), TSDB_CODE_SUCCESS);
    context_.progressStepId = stepId;
    return stepId;
  }

  uint64_t PrepareFillHistoryStep(SStreamProgressRange original, STimeWindow step, bool finishCheck,
                                  uint64_t runnerToken) {
    task_.fillHistory = true;
    task_.fillHistoryStartTime = original.start;
    task_.historyOriginalRange = original;
    task_.historyOriginalRangeValid = true;
    context_.isHistory = true;
    context_.status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
    context_.scanRange = {.skey = original.start, .ekey = original.end - 1};
    context_.calcRange = context_.scanRange;
    context_.stepRange = step;
    context_.finishCheck = finishCheck;
    context_.needTsdbMeta = true;
    context_.pMaxDelayHeap = heapCreate(compareRecalcBarrierHistoryGroups);
    EXPECT_NE(context_.pMaxDelayHeap, nullptr);
    historyHeapInitialized_ = true;
    task_.historyStep = 50;
    EXPECT_EQ(stRecalcTrackerInitHistory(task_.pRecalcTracker, true, original, false), TSDB_CODE_SUCCESS);

    uint64_t                   stepId = 0;
    const SStreamProgressRange progressStep = stTriggerTaskProgressRangeFromClosed(step);
    EXPECT_EQ(stRecalcTrackerBeginStep(task_.pRecalcTracker, 0, progressStep, nullptr, &stepId), TSDB_CODE_SUCCESS);
    EXPECT_EQ(stRecalcTrackerAddRunner(task_.pRecalcTracker, stepId, runnerToken), TSDB_CODE_SUCCESS);
    EXPECT_EQ(stRecalcTrackerSetTriggerDone(task_.pRecalcTracker, stepId, 0), TSDB_CODE_SUCCESS);
    context_.progressStepId = stepId;
    context_.historyProgressStepRange = progressStep;
    context_.historyProgressTriggerDone = true;
    calcRequest_.progressStepId = stepId;
    calcRequest_.progressRequestToken = runnerToken;
    int64_t running = 1;
    EXPECT_EQ(tSimpleHashPut(task_.pSessionRunning, &context_.sessionId, sizeof(context_.sessionId), &running,
                             sizeof(running)),
              TSDB_CODE_SUCCESS);
    return stepId;
  }

  uint64_t PrepareFillHistoryReaderStep(SStreamProgressRange original, STimeWindow step, uint64_t readerToken) {
    task_.fillHistory = true;
    task_.fillHistoryStartTime = original.start;
    task_.historyOriginalRange = original;
    task_.historyOriginalRangeValid = true;
    task_.historyStep = step.ekey - step.skey + 1;
    context_.isHistory = true;
    context_.status = STRIGGER_CONTEXT_FETCH_META;
    context_.scanRange = {.skey = original.start, .ekey = original.end - 1};
    context_.calcRange = context_.scanRange;
    context_.stepRange = step;
    context_.needTsdbMeta = true;
    context_.finishCheck = false;
    context_.pMaxDelayHeap = heapCreate(compareRecalcBarrierHistoryGroups);
    EXPECT_NE(context_.pMaxDelayHeap, nullptr);
    historyHeapInitialized_ = true;
    EXPECT_EQ(stRecalcTrackerInitHistory(task_.pRecalcTracker, true, original, false), TSDB_CODE_SUCCESS);

    uint64_t                   stepId = 0;
    const SStreamProgressRange progressStep = stTriggerTaskProgressRangeFromClosed(step);
    EXPECT_EQ(stRecalcTrackerBeginStep(task_.pRecalcTracker, 0, progressStep, nullptr, &stepId), TSDB_CODE_SUCCESS);
    EXPECT_EQ(stRecalcTrackerAddReader(task_.pRecalcTracker, stepId, readerToken), TSDB_CODE_SUCCESS);
    context_.progressStepId = stepId;
    context_.historyProgressStepRange = progressStep;
    progress_->pullReq.base.type = STRIGGER_PULL_TSDB_META;
    progress_->pullReq.base.progressStepId = stepId;
    progress_->pullReq.base.progressRequestToken = readerToken;
    context_.curReaderIdx = 1;
    return stepId;
  }

  Stub                     stub_;
  SStreamTriggerTask       task_ = {};
  SSTriggerRealtimeContext realtime_ = {};
  SSTriggerRealtimeGroup   group_ = {};
  SSTriggerHistoryContext  context_ = {};
  SSTriggerHistoryGroup    historyGroup_ = {};
  bool                     historyGroupInitialized_ = false;
  bool                     historyHeapInitialized_ = false;
  SSTriggerTsdbProgress*   progress_ = nullptr;
  SSTriggerCalcRequest     calcRequest_ = {};
};

TEST_F(StreamTriggerRecalcBarrierTest, NoDataReaderStepCommitsWithoutRunner) {
  const uint64_t stepId = BeginReaderStep(81);
  progress_->pullReq.base.type = STRIGGER_PULL_TSDB_META;
  progress_->pullReq.base.progressStepId = stepId;
  progress_->pullReq.base.progressRequestToken = 1;
  context_.status = STRIGGER_CONTEXT_FETCH_META;
  context_.curReaderIdx = 2;
  SSTriggerAHandle responseAhandle = {.streamId = task_.task.streamId,
                                      .taskId = task_.task.taskId,
                                      .sessionId = kHistorySessionId,
                                      .param = &progress_->pullReq.base,
                                      .progressStepId = stepId,
                                      .progressRequestToken = 1};
  SMsgSendInfo     responseSendInfo = {};
  responseSendInfo.param = &responseAhandle;
  SRpcMsg response = {};
  response.msgType = TDMT_STREAM_TRIGGER_PULL_RSP;
  response.code = TSDB_CODE_STREAM_NO_DATA;
  response.info.ahandle = &responseSendInfo;
  int64_t errorTaskId = 0;
  ASSERT_EQ(stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId), TSDB_CODE_SUCCESS);

  auto snapshot = CopyRecalcSnapshot(task_.pRecalcTracker, 81);
  EXPECT_EQ(snapshot.status, STREAM_RECALC_STATUS_FINISHED);
  EXPECT_EQ(snapshot.progressPct, 100);
}

TEST_F(StreamTriggerRecalcBarrierTest, FillHistoryDisabledIsInvalid) {
  bool valid = true;
  EXPECT_EQ(CopyHistoryProgress(&valid), 0);
  EXPECT_FALSE(valid);
}

TEST_F(StreamTriggerRecalcBarrierTest, FillHistoryStartsAtZero) {
  ASSERT_EQ(QueueFillHistory(100, 199), TSDB_CODE_SUCCESS);
  bool valid = false;
  EXPECT_EQ(CopyHistoryProgress(&valid), 0);
  EXPECT_TRUE(valid);
}

TEST_F(StreamTriggerRecalcBarrierTest, CapturedEndDoesNotMoveAfterFirstRequest) {
  ASSERT_EQ(QueueFillHistory(100, 199), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(task_.historyOriginalRangeValid);
  EXPECT_EQ(task_.historyOriginalRange.start, 100);
  EXPECT_EQ(task_.historyOriginalRange.end, 200);

  SetHistoryCutoff(299);
  ASSERT_EQ(stTriggerTaskAddRecalcRequest(&task_, nullptr, nullptr, true, false, false, 0), TSDB_CODE_SUCCESS);
  EXPECT_EQ(task_.historyOriginalRange.start, 100);
  EXPECT_EQ(task_.historyOriginalRange.end, 200);
}

TEST_F(StreamTriggerRecalcBarrierTest, UnfinishedRedeployRestartsAtZero) {
  task_.fillHistory = true;
  task_.fillHistoryStartTime = 100;
  realtime_.haveReadCheckpoint = false;
  realtime_.status = STRIGGER_CONTEXT_IDLE;
  atomic_store_8(&task_.isCheckpointReady, 1);
  EncodeHistoryCheckpoint(false, 7);
  stub_.set(streamReadCheckPoint, readRecalcBarrierCheckpoint);

  ASSERT_EQ(ProcessRealtimeStart(), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stTriggerTaskAddRecalcRequest(&task_, nullptr, nullptr, true, false, false, 0), TSDB_CODE_SUCCESS);
  bool valid = false;
  EXPECT_EQ(CopyHistoryProgress(&valid), 0);
  EXPECT_TRUE(valid);
}

TEST_F(StreamTriggerRecalcBarrierTest, UnmatchedCheckpointDefersDenominatorUntilFreshLastTs) {
  task_.fillHistory = true;
  task_.fillHistoryStartTime = 100;
  realtime_.haveReadCheckpoint = false;
  realtime_.status = STRIGGER_CONTEXT_IDLE;
  atomic_store_8(&task_.isCheckpointReady, 1);
  EncodeHistoryCheckpoint(false, 8);
  stub_.set(streamReadCheckPoint, readRecalcBarrierCheckpoint);

  ASSERT_EQ(ProcessRealtimeStart(), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(task_.historyOriginalRangeValid);
  EXPECT_EQ(tSimpleHashGetSize(task_.pHistoryCutoffTime), 0);
  ASSERT_NE(gRecalcBarrierSendState.pullAhandle, nullptr);
  auto* lastTsRequest = static_cast<SSTriggerAHandle*>(gRecalcBarrierSendState.pullAhandle->param);
  ASSERT_EQ(static_cast<SSTriggerPullRequest*>(lastTsRequest->param)->type, STRIGGER_PULL_LAST_TS);
  SMsgSendInfo* lastTsAhandle = gRecalcBarrierSendState.pullAhandle;
  gRecalcBarrierSendState.pullAhandle = nullptr;

  SStreamTsResponse lastTs = {.ver = 1, .tsInfo = taosArrayInit(1, sizeof(STsInfo))};
  ASSERT_NE(lastTs.tsInfo, nullptr);
  STsInfo entry = {.gId = 11, .ts = 299};
  ASSERT_NE(taosArrayPush(lastTs.tsInfo, &entry), nullptr);
  const int32_t len = tSerializeSStreamTsResponse(nullptr, 0, &lastTs);
  ASSERT_GT(len, 0);
  void* data = rpcMallocCont(len);
  ASSERT_NE(data, nullptr);
  ASSERT_EQ(tSerializeSStreamTsResponse(data, len, &lastTs), len);
  SRpcMsg response = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .pCont = data, .contLen = len, .code = TSDB_CODE_SUCCESS};
  response.info.ahandle = lastTsAhandle;
  int64_t errorTaskId = 0;
  ASSERT_EQ(stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tSimpleHashGetSize(task_.pHistoryCutoffTime), 1);
  destroyAhandle(lastTsAhandle);
  rpcFreeCont(data);
  taosArrayDestroy(lastTs.tsInfo);

  ASSERT_EQ(stTriggerTaskAddRecalcRequest(&task_, nullptr, nullptr, true, false, false, 0), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(task_.historyOriginalRangeValid);
  EXPECT_EQ(task_.historyOriginalRange.start, 100);
  EXPECT_EQ(task_.historyOriginalRange.end, 300);
  bool valid = false;
  EXPECT_EQ(CopyHistoryProgress(&valid), 0);
  EXPECT_TRUE(valid);
}

TEST_F(StreamTriggerRecalcBarrierTest, CheckpointHistoryFinishedRestoresOneHundred) {
  task_.fillHistory = true;
  task_.fillHistoryStartTime = 100;
  realtime_.haveReadCheckpoint = false;
  realtime_.status = STRIGGER_CONTEXT_ACQUIRE_REQUEST;
  atomic_store_8(&task_.isCheckpointReady, 1);
  EncodeHistoryCheckpoint(true, 7);
  stub_.set(streamReadCheckPoint, readRecalcBarrierCheckpoint);

  ASSERT_EQ(ProcessRealtimeStart(), TSDB_CODE_SUCCESS);
  bool valid = false;
  EXPECT_EQ(CopyHistoryProgress(&valid), 100);
  EXPECT_TRUE(valid);
}

TEST_F(StreamTriggerRecalcBarrierTest, MaximumHistoryCutoffUsesSaturatedRange) {
  ASSERT_EQ(QueueFillHistory(TSKEY_MAX - 100, TSKEY_MAX), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(task_.historyOriginalRangeValid);
  EXPECT_EQ(task_.historyOriginalRange.start, TSKEY_MAX - 100);
  EXPECT_EQ(task_.historyOriginalRange.end, TSKEY_MAX);
  bool valid = false;
  EXPECT_EQ(CopyHistoryProgress(&valid), 0);
  EXPECT_TRUE(valid);
}

TEST_F(StreamTriggerRecalcBarrierTest, FirstTsConfirmedPrefixCountsAsComplete) {
  ASSERT_EQ(QueueFillHistory(100, 199), TSDB_CODE_SUCCESS);
  task_.isVirtualTable = true;
  historyGroup_.gid = 11;
  historyGroup_.pTableMetas = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(historyGroup_.pTableMetas, nullptr);
  SSTriggerTableMeta tableMeta = {};
  const int64_t      tableUid = 11;
  ASSERT_EQ(tSimpleHashPut(historyGroup_.pTableMetas, &tableUid, sizeof(tableUid), &tableMeta, sizeof(tableMeta)),
            TSDB_CODE_SUCCESS);
  SSTriggerHistoryGroup* firstTsGroup = &historyGroup_;
  ASSERT_EQ(
      tSimpleHashPut(context_.pGroups, &historyGroup_.gid, sizeof(historyGroup_.gid), &firstTsGroup, POINTER_BYTES),
      TSDB_CODE_SUCCESS);
  ASSERT_EQ(ProcessHistoryStart(), TSDB_CODE_SUCCESS);
  ASSERT_NE(gRecalcBarrierSendState.pullAhandle, nullptr);
  SMsgSendInfo* firstTsAhandle = gRecalcBarrierSendState.pullAhandle;
  gRecalcBarrierSendState.pullAhandle = nullptr;

  SStreamTsResponse firstTs = {.ver = 1, .tsInfo = taosArrayInit(1, sizeof(STsInfo))};
  ASSERT_NE(firstTs.tsInfo, nullptr);
  STsInfo entry = {.gId = 11, .ts = 150};
  ASSERT_NE(taosArrayPush(firstTs.tsInfo, &entry), nullptr);
  const int32_t len = tSerializeSStreamTsResponse(nullptr, 0, &firstTs);
  ASSERT_GT(len, 0);
  void* data = rpcMallocCont(len);
  ASSERT_NE(data, nullptr);
  ASSERT_EQ(tSerializeSStreamTsResponse(data, len, &firstTs), len);

  SRpcMsg response = {};
  response.msgType = TDMT_STREAM_TRIGGER_PULL_RSP;
  response.code = TSDB_CODE_SUCCESS;
  response.pCont = data;
  response.contLen = len;
  response.info.ahandle = firstTsAhandle;
  int64_t errorTaskId = 0;
  ASSERT_EQ(stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId), TSDB_CODE_SUCCESS);

  bool valid = false;
  EXPECT_EQ(CopyHistoryProgress(&valid), 50);
  EXPECT_TRUE(valid);
  destroyAhandle(firstTsAhandle);
  rpcFreeCont(data);
  taosArrayDestroy(firstTs.tsInfo);
  tSimpleHashCleanup(historyGroup_.pTableMetas);
  historyGroup_.pTableMetas = nullptr;
}

TEST_F(StreamTriggerRecalcBarrierTest, PendingRunnerKeepsStepUncommitted) {
  const uint64_t stepId = PrepareFillHistoryStep({100, 200}, {100, 149}, false, 31);
  ASSERT_EQ(ProcessHistoryStart(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(context_.progressStepId, stepId);

  bool valid = false;
  EXPECT_EQ(CopyHistoryProgress(&valid), 0);
  EXPECT_TRUE(valid);

  ASSERT_EQ(ProcessCalcResponse(TSDB_CODE_SUCCESS), TSDB_CODE_SUCCESS);
  EXPECT_EQ(CopyHistoryProgress(&valid), 50);
}

TEST_F(StreamTriggerRecalcBarrierTest, OnePendingGroupKeepsGlobalFrontier) {
  PreparePendingHistoryStep(91, false);
  task_.fillHistory = true;
  task_.historyOriginalRange = {100, 200};
  task_.historyOriginalRangeValid = true;
  context_.isHistory = true;
  context_.historyProgressStepRange = {100, 150};
  ASSERT_EQ(stRecalcTrackerInitHistory(task_.pRecalcTracker, true, {100, 200}, false), TSDB_CODE_SUCCESS);

  ASSERT_EQ(ProcessHistoryStart(), TSDB_CODE_SUCCESS);
  bool valid = false;
  EXPECT_EQ(CopyHistoryProgress(&valid), 0);
  EXPECT_TRUE(valid);
  EXPECT_EQ(context_.stepRange.skey, 100);
  EXPECT_EQ(context_.stepRange.ekey, 149);
}

TEST_F(StreamTriggerRecalcBarrierTest, PendingRunnerDoesNotAdvanceNextHistoryStep) {
  PrepareFillHistoryStep({100, 200}, {100, 149}, false, 37);
  ASSERT_EQ(ProcessHistoryStart(), TSDB_CODE_SUCCESS);

  EXPECT_EQ(context_.stepRange.skey, 100);
  EXPECT_EQ(context_.stepRange.ekey, 149);
  bool valid = false;
  EXPECT_EQ(CopyHistoryProgress(&valid), 0);
  EXPECT_TRUE(valid);
}

TEST_F(StreamTriggerRecalcBarrierTest, LastReaderCompletesBeforeNextHistoryStepStarts) {
  PrepareFillHistoryReaderStep({100, 200}, {100, 149}, 43);
  stub_.set(stRecalcTrackerCompleteReader, captureRecalcBarrierCompleteReader);

  SSTriggerAHandle responseAhandle = {.streamId = task_.task.streamId,
                                      .taskId = task_.task.taskId,
                                      .sessionId = kHistorySessionId,
                                      .param = &progress_->pullReq.base,
                                      .progressStepId = progress_->pullReq.base.progressStepId,
                                      .progressRequestToken = progress_->pullReq.base.progressRequestToken};
  SMsgSendInfo     responseSendInfo = {};
  responseSendInfo.param = &responseAhandle;
  SRpcMsg response = {};
  response.msgType = TDMT_STREAM_TRIGGER_PULL_RSP;
  response.code = TSDB_CODE_STREAM_NO_DATA;
  response.info.ahandle = &responseSendInfo;
  int64_t errorTaskId = 0;
  ASSERT_EQ(stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId), TSDB_CODE_SUCCESS);

  EXPECT_EQ(gRecalcBarrierSendState.completeReaderCalls, 1);
  EXPECT_EQ(gRecalcBarrierSendState.sendCalls, 1);
  EXPECT_EQ(gRecalcBarrierSendState.completeReaderCallsAtLastSend, 1);
  bool valid = false;
  EXPECT_EQ(CopyHistoryProgress(&valid), 50);
  EXPECT_TRUE(valid);
}

TEST_F(StreamTriggerRecalcBarrierTest, ForcedTailWindowsKeepProgressAtNinetyNine) {
  ASSERT_EQ(stRecalcTrackerInitHistory(task_.pRecalcTracker, true, {100, 201}, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stRecalcTrackerCommitHistoryThrough(task_.pRecalcTracker, 200, false), TSDB_CODE_SUCCESS);
  PrepareFillHistoryStep({100, 201}, {200, 200}, true, 41);

  ASSERT_EQ(ProcessHistoryStart(), TSDB_CODE_SUCCESS);
  bool valid = false;
  EXPECT_EQ(CopyHistoryProgress(&valid), 99);
  EXPECT_TRUE(valid);
  EXPECT_EQ(atomic_load_8(&task_.historyFinished), 0);
}

TEST_F(StreamTriggerRecalcBarrierTest, MalformedSuccessfulReaderResponseFailsBeforeCommit) {
  const uint64_t stepId = BeginReaderStep(87);
  progress_->pullReq.base.type = STRIGGER_PULL_FIRST_TS;
  progress_->pullReq.base.progressStepId = stepId;
  progress_->pullReq.base.progressRequestToken = 1;
  context_.status = STRIGGER_CONTEXT_ADJUST_START;
  SSTriggerAHandle responseAhandle = {.streamId = task_.task.streamId,
                                      .taskId = task_.task.taskId,
                                      .sessionId = kHistorySessionId,
                                      .param = &progress_->pullReq.base,
                                      .progressStepId = stepId,
                                      .progressRequestToken = 1};
  SMsgSendInfo     responseSendInfo = {};
  responseSendInfo.param = &responseAhandle;
  uint8_t malformedPayload = 0;
  SRpcMsg response = {};
  response.msgType = TDMT_STREAM_TRIGGER_PULL_RSP;
  response.code = TSDB_CODE_SUCCESS;
  response.pCont = &malformedPayload;
  response.contLen = sizeof(malformedPayload);
  response.info.ahandle = &responseSendInfo;
  int64_t errorTaskId = 0;
  ASSERT_NE(stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId), TSDB_CODE_SUCCESS);

  auto snapshot = CopyRecalcSnapshot(task_.pRecalcTracker, 87);
  EXPECT_EQ(snapshot.status, STREAM_RECALC_STATUS_FAILED);
  EXPECT_EQ(snapshot.progressPct, 0);
}

TEST_F(StreamTriggerRecalcBarrierTest, PendingCalcParamsWaitForRunnerResponseBeforeNextStep) {
  const uint64_t stepId = PreparePendingHistoryStep(88, false);
  ASSERT_EQ(ProcessHistoryStart(), TSDB_CODE_SUCCESS);

  auto snapshot = CopyRecalcSnapshot(task_.pRecalcTracker, 88);
  EXPECT_EQ(snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(snapshot.progressPct, 0);
  EXPECT_EQ(context_.progressStepId, stepId);
  EXPECT_EQ(historyGroup_.pPendingCalcParams.neles, 1);
  EXPECT_EQ(historyGroup_.pPendingParWinCalcParams.neles, 0);

  context_.pCalcReq = &calcRequest_;
  int64_t running = 1;
  ASSERT_EQ(
      tSimpleHashPut(task_.pSessionRunning, &context_.sessionId, sizeof(context_.sessionId), &running, sizeof(running)),
      TSDB_CODE_SUCCESS);
  context_.pMinGroup = &historyGroup_;
  ASSERT_EQ(ProcessHistoryStart(), TSDB_CODE_SUCCESS);
  ASSERT_NE(gRecalcBarrierSendState.retryAhandle, nullptr);
  auto* calcAhandle = static_cast<SSTriggerAHandle*>(gRecalcBarrierSendState.retryAhandle->param);
  EXPECT_EQ(calcAhandle->progressStepId, stepId);
  EXPECT_EQ(historyGroup_.pPendingCalcParams.neles, 0);
  EXPECT_NE(context_.progressStepId, stepId);
  snapshot = CopyRecalcSnapshot(task_.pRecalcTracker, 88);
  EXPECT_EQ(snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(snapshot.progressPct, 0);

  ASSERT_EQ(ProcessCalcResponse(TSDB_CODE_SUCCESS, calcAhandle), TSDB_CODE_SUCCESS);
  snapshot = CopyRecalcSnapshot(task_.pRecalcTracker, 88);
  EXPECT_EQ(snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(snapshot.progressPct, 50);
  EXPECT_NE(context_.progressStepId, 0U);
  EXPECT_NE(context_.progressStepId, stepId);
}

TEST_F(StreamTriggerRecalcBarrierTest, PendingParentWindowCalcWaitsForRunnerBeforeNextStep) {
  const uint64_t stepId = PreparePendingHistoryStep(89, true);
  ASSERT_EQ(ProcessHistoryStart(), TSDB_CODE_SUCCESS);

  auto snapshot = CopyRecalcSnapshot(task_.pRecalcTracker, 89);
  EXPECT_EQ(snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(snapshot.progressPct, 0);
  EXPECT_EQ(context_.progressStepId, stepId);
  EXPECT_EQ(historyGroup_.pPendingCalcParams.neles, 0);
  EXPECT_EQ(historyGroup_.pPendingParWinCalcParams.neles, 1);

  context_.pCalcReq = &calcRequest_;
  int64_t running = 1;
  ASSERT_EQ(
      tSimpleHashPut(task_.pSessionRunning, &context_.sessionId, sizeof(context_.sessionId), &running, sizeof(running)),
      TSDB_CODE_SUCCESS);
  context_.pMinGroup = &historyGroup_;
  ASSERT_EQ(ProcessHistoryStart(), TSDB_CODE_SUCCESS);
  ASSERT_NE(gRecalcBarrierSendState.retryAhandle, nullptr);
  auto* calcAhandle = static_cast<SSTriggerAHandle*>(gRecalcBarrierSendState.retryAhandle->param);
  EXPECT_EQ(calcAhandle->progressStepId, stepId);
  EXPECT_EQ(historyGroup_.pPendingParWinCalcParams.neles, 0);
  EXPECT_NE(context_.progressStepId, stepId);
  snapshot = CopyRecalcSnapshot(task_.pRecalcTracker, 89);
  EXPECT_EQ(snapshot.progressPct, 0);

  ASSERT_EQ(ProcessCalcResponse(TSDB_CODE_SUCCESS, calcAhandle), TSDB_CODE_SUCCESS);
  snapshot = CopyRecalcSnapshot(task_.pRecalcTracker, 89);
  EXPECT_EQ(snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(snapshot.progressPct, 50);
}

TEST_F(StreamTriggerRecalcBarrierTest, RunnerPendingKeepsProgressBelowCommit) {
  BeginRunnerStep({82}, 7);
  auto snapshot = CopyRecalcSnapshot(task_.pRecalcTracker, 82);
  EXPECT_EQ(snapshot.status, STREAM_RECALC_STATUS_RUNNING);
  EXPECT_EQ(snapshot.progressPct, 0);
}

TEST_F(StreamTriggerRecalcBarrierTest, RunnerRetryUsesSameTokenAndCommitsOnce) {
  BeginRunnerStep({83}, 9);
  ASSERT_EQ(ProcessCalcResponse(TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER), TSDB_CODE_SUCCESS);
  ASSERT_NE(gRecalcBarrierSendState.retryAhandle, nullptr);
  auto* retryHandle = static_cast<SSTriggerAHandle*>(gRecalcBarrierSendState.retryAhandle->param);
  EXPECT_EQ(retryHandle->progressStepId, calcRequest_.progressStepId);
  EXPECT_EQ(retryHandle->progressRequestToken, 9U);

  ASSERT_EQ(ProcessCalcResponse(TSDB_CODE_SUCCESS, retryHandle), TSDB_CODE_SUCCESS);
  auto snapshot = CopyRecalcSnapshot(task_.pRecalcTracker, 83);
  EXPECT_EQ(snapshot.status, STREAM_RECALC_STATUS_FINISHED);
  EXPECT_EQ(snapshot.progressPct, 100);
}

TEST_F(StreamTriggerRecalcBarrierTest, DuplicateResponseDoesNotAdvanceTwice) {
  BeginRunnerStep({84}, 11);
  SSTriggerAHandle responseAhandle = {.streamId = task_.task.streamId,
                                      .taskId = task_.task.taskId,
                                      .sessionId = kHistorySessionId,
                                      .param = &calcRequest_,
                                      .progressStepId = calcRequest_.progressStepId,
                                      .progressRequestToken = calcRequest_.progressRequestToken};
  ASSERT_EQ(ProcessCalcResponse(TSDB_CODE_SUCCESS, &responseAhandle), TSDB_CODE_SUCCESS);
  ASSERT_EQ(ProcessCalcResponse(TSDB_CODE_SUCCESS, &responseAhandle), TSDB_CODE_SUCCESS);
  EXPECT_EQ(gRecalcBarrierSendState.releaseCalls, 2);
  auto snapshot = CopyRecalcSnapshot(task_.pRecalcTracker, 84);
  EXPECT_EQ(snapshot.status, STREAM_RECALC_STATUS_FINISHED);
  EXPECT_EQ(snapshot.progressPct, 100);
}

TEST_F(StreamTriggerRecalcBarrierTest, FatalSharedRequestFailsItsContributors) {
  BeginRunnerStep({85, 86}, 13);
  EXPECT_EQ(ProcessCalcResponse(TSDB_CODE_INTERNAL_ERROR), TSDB_CODE_INTERNAL_ERROR);
  EXPECT_EQ(CopyRecalcSnapshot(task_.pRecalcTracker, 85).status, STREAM_RECALC_STATUS_FAILED);
  EXPECT_EQ(CopyRecalcSnapshot(task_.pRecalcTracker, 86).status, STREAM_RECALC_STATUS_FAILED);
}

TEST(StreamTriggerRecalcRangeTest, ClosedMaximumEndSaturatesWithoutOverflow) {
  const SStreamProgressRange range = stTriggerTaskProgressRangeFromClosed({TSKEY_MAX - 1, TSKEY_MAX});
  EXPECT_EQ(range.start, TSKEY_MAX - 1);
  EXPECT_EQ(range.end, TSKEY_MAX);
}

TEST(StreamTriggerRecalcRangeTest, ClosedMaximumSingletonIsEmptyForProgress) {
  const SStreamProgressRange range = stTriggerTaskProgressRangeFromClosed({TSKEY_MAX, TSKEY_MAX});
  EXPECT_EQ(range.start, TSKEY_MAX);
  EXPECT_EQ(range.end, TSKEY_MAX);
}

TEST(StreamTriggerRecalcTokenTest, CalcPoolReleaseAndReuseClearRuntimeTokens) {
  SStreamTriggerTask task = {};
  task.triggerType = STREAM_TRIGGER_COUNT;
  task.pCalcNodes = taosArrayInit_s(sizeof(SSTriggerCalcNode), 1);
  task.runnerList = taosArrayInit_s(sizeof(SStreamRunnerTarget), 1);
  task.pGroupRunning = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  task.pSessionRunning = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(task.pCalcNodes, nullptr);
  ASSERT_NE(task.runnerList, nullptr);
  ASSERT_NE(task.pGroupRunning, nullptr);
  ASSERT_NE(task.pSessionRunning, nullptr);
  auto* runner = static_cast<SStreamRunnerTarget*>(taosArrayGet(task.runnerList, 0));
  runner->addr.taskId = 99;
  auto* node = static_cast<SSTriggerCalcNode*>(taosArrayGet(task.pCalcNodes, 0));
  node->pSlots = taosArrayInit_s(sizeof(SSTriggerCalcSlot), 2);
  ASSERT_NE(node->pSlots, nullptr);
  for (int32_t i = 0; i < 2; ++i) {
    auto* slot = static_cast<SSTriggerCalcSlot*>(taosArrayGet(node->pSlots, i));
    TD_DLIST_APPEND(&node->idleSlots, slot);
  }

  SSTriggerCalcRequest* request = nullptr;
  ASSERT_EQ(stTriggerTaskAcquireRequest(&task, 2, 3, &request), TSDB_CODE_SUCCESS);
  ASSERT_NE(request, nullptr);
  request->progressStepId = 11;
  request->progressRequestToken = 12;
  ASSERT_EQ(stTriggerTaskReleaseRequest(&task, &request, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stTriggerTaskAcquireRequest(&task, 2, 3, &request), TSDB_CODE_SUCCESS);
  ASSERT_NE(request, nullptr);
  EXPECT_EQ(request->progressStepId, 0U);
  EXPECT_EQ(request->progressRequestToken, 0U);
  request->progressStepId = 21;
  request->progressRequestToken = 22;
  ASSERT_EQ(stTriggerTaskReleaseRequest(&task, &request, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(stTriggerTaskAcquireRequest(&task, 2, 3, &request), TSDB_CODE_SUCCESS);
  ASSERT_NE(request, nullptr);
  EXPECT_EQ(request->progressStepId, 0U);
  EXPECT_EQ(request->progressRequestToken, 0U);
  ASSERT_EQ(stTriggerTaskReleaseRequest(&task, &request, true), TSDB_CODE_SUCCESS);

  for (int32_t i = 0; i < 2; ++i) {
    auto* slot = static_cast<SSTriggerCalcSlot*>(taosArrayGet(node->pSlots, i));
    tDestroySTriggerCalcRequest(&slot->req);
  }
  taosArrayDestroy(node->pSlots);
  tSimpleHashCleanup(task.pSessionRunning);
  tSimpleHashCleanup(task.pGroupRunning);
  taosArrayDestroy(task.runnerList);
  taosArrayDestroy(task.pCalcNodes);
}

TEST(StreamTriggerRecalcTokenTest, RuntimeTokensAreNotSerialized) {
  SSTriggerCalcRequest source = {};
  source.streamId = 1;
  source.runnerTaskId = 2;
  source.sessionId = 3;
  source.params = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  source.groupColVals = taosArrayInit(0, sizeof(SStreamGroupValue));
  source.progressStepId = 101;
  source.progressRequestToken = 202;
  ASSERT_NE(source.params, nullptr);
  ASSERT_NE(source.groupColVals, nullptr);
  const int32_t        len = tSerializeSTriggerCalcRequest(nullptr, 0, &source);
  SSTriggerCalcRequest withoutTokens = source;
  withoutTokens.progressStepId = 0;
  withoutTokens.progressRequestToken = 0;
  EXPECT_EQ(tSerializeSTriggerCalcRequest(nullptr, 0, &withoutTokens), len);
  ASSERT_GT(len, 0);
  void* data = taosMemoryMalloc(len);
  ASSERT_NE(data, nullptr);
  ASSERT_EQ(tSerializeSTriggerCalcRequest(data, len, &source), len);
  SSTriggerCalcRequest decoded = {};
  decoded.progressStepId = 303;
  decoded.progressRequestToken = 404;
  ASSERT_EQ(tDeserializeSTriggerCalcRequest(data, len, &decoded), TSDB_CODE_SUCCESS);
  EXPECT_EQ(decoded.progressStepId, 0U);
  EXPECT_EQ(decoded.progressRequestToken, 0U);
  taosMemoryFree(data);
  decoded.progressStepId = 303;
  decoded.progressRequestToken = 404;
  tDestroySTriggerCalcRequest(&decoded);
  EXPECT_EQ(decoded.progressStepId, 0U);
  EXPECT_EQ(decoded.progressRequestToken, 0U);
  tDestroySTriggerCalcRequest(&source);
  EXPECT_EQ(source.progressStepId, 0U);
  EXPECT_EQ(source.progressRequestToken, 0U);

  SSTriggerPullRequestUnion pull = {};
  pull.firstTsReq.base.type = STRIGGER_PULL_FIRST_TS;
  pull.firstTsReq.base.streamId = 1;
  pull.firstTsReq.base.readerTaskId = 2;
  pull.firstTsReq.base.sessionId = 3;
  pull.firstTsReq.base.progressStepId = 505;
  pull.firstTsReq.base.progressRequestToken = 606;
  pull.firstTsReq.gid = 7;
  pull.firstTsReq.startTime = 8;
  pull.firstTsReq.ver = 9;
  const int32_t pullLen = tSerializeSTriggerPullRequest(nullptr, 0, &pull.base);
  ASSERT_GT(pullLen, 0);
  pull.firstTsReq.base.progressStepId = 0;
  pull.firstTsReq.base.progressRequestToken = 0;
  EXPECT_EQ(tSerializeSTriggerPullRequest(nullptr, 0, &pull.base), pullLen);
  pull.firstTsReq.base.progressStepId = 505;
  pull.firstTsReq.base.progressRequestToken = 606;
  data = taosMemoryMalloc(pullLen);
  ASSERT_NE(data, nullptr);
  ASSERT_EQ(tSerializeSTriggerPullRequest(data, pullLen, &pull.base), pullLen);
  SSTriggerPullRequestUnion decodedPull = {};
  decodedPull.base.progressStepId = 707;
  decodedPull.base.progressRequestToken = 808;
  ASSERT_EQ(tDeserializeSTriggerPullRequest(data, pullLen, &decodedPull), TSDB_CODE_SUCCESS);
  EXPECT_EQ(decodedPull.base.progressStepId, 0U);
  EXPECT_EQ(decodedPull.base.progressRequestToken, 0U);
  taosMemoryFree(data);
  decodedPull.base.progressStepId = 707;
  decodedPull.base.progressRequestToken = 808;
  tDestroySTriggerPullRequest(&decodedPull);
  EXPECT_EQ(decodedPull.base.progressStepId, 0U);
  EXPECT_EQ(decodedPull.base.progressRequestToken, 0U);
}

static std::vector<std::string> gTriggerDebugLogs;
static int32_t                  gDebugJobCopyCalls = 0;
static int32_t                  gTerminalTakeCalls = 0;
static int32_t                  gGroupSizeCalls = 0;
static int32_t                  gForbiddenGroupSizeCalls = 0;
static const SSHashObj*         gForbiddenGroups = nullptr;
static int32_t                  gTerminalArrayPushCalls = 0;
static bool                     gTriggerExecuteWorkerPublished = false;

static void captureTriggerDebugLog(const char*, int32_t, int32_t, const char* format, ...) {
  char    buffer[4096] = {0};
  va_list args;
  va_start(args, format);
  int32_t len = vsnprintf(buffer, sizeof(buffer), format, args);
  va_end(args);
  if (len >= 0 && len < sizeof(buffer)) gTriggerDebugLogs.emplace_back(buffer);
}

static int32_t countDebugJobCopies(SStreamRecalcTracker*, SArray** ppJobs) {
  ++gDebugJobCopyCalls;
  *ppJobs = nullptr;
  return TSDB_CODE_SUCCESS;
}

static int32_t countTerminalTakes(SStreamRecalcTracker*, SArray** ppTerminals) {
  ++gTerminalTakeCalls;
  *ppTerminals = nullptr;
  return TSDB_CODE_SUCCESS;
}

static int32_t countGroupSizeCalls(const SSHashObj*) {
  ++gGroupSizeCalls;
  return 999;
}

static int32_t countForbiddenGroupSizeCalls(const SSHashObj* pGroups) {
  if (pGroups == gForbiddenGroups) ++gForbiddenGroupSizeCalls;
  return 0;
}

static int32_t countExecuteOwnerReads(const SSHashObj* pGroups) {
  if (gTriggerExecuteWorkerPublished && pGroups == gForbiddenGroups) ++gForbiddenGroupSizeCalls;
  return 0;
}

static SEpSet* returnNoSyncEndpoint(int32_t) { return nullptr; }

static int32_t publishTriggerWorkerOwnership(void*, EQueueType queueType, SRpcMsg* pMsg) {
  if (queueType == STREAM_TRIGGER_QUEUE) gTriggerExecuteWorkerPublished = true;
  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = nullptr;
  return TSDB_CODE_SUCCESS;
}

static int32_t maximumReaderTimeOfDay(struct timeval* pTime) {
  const int64_t nowMs = INT64_MAX / 1000;
  pTime->tv_sec = nowMs / 1000;
  pTime->tv_usec = nowMs % 1000 * 1000;
  return TSDB_CODE_SUCCESS;
}

static int32_t maximumReaderTimeOfDayNextMs(struct timeval* pTime) {
  const int64_t nowMs = INT64_MAX / 1000 + 1;
  pTime->tv_sec = nowMs / 1000;
  pTime->tv_usec = nowMs % 1000 * 1000;
  return TSDB_CODE_SUCCESS;
}

static int32_t readerTimeOfDayAtNineSeconds(struct timeval* pTime) {
  pTime->tv_sec = 9;
  pTime->tv_usec = 0;
  return TSDB_CODE_SUCCESS;
}

static SArray* failTerminalArrayInit(size_t, size_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

static void* failSecondTerminalArrayPush(SArray* pArray, const void* pData, int32_t nEles) {
  if (++gTerminalArrayPushCalls == 2) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  if (pArray == nullptr || pData == nullptr || nEles != 1 || pArray->size >= pArray->capacity) {
    terrno = TSDB_CODE_INVALID_PARA;
    return nullptr;
  }
  void* pDestination = static_cast<char*>(pArray->pData) + pArray->size * pArray->elemSize;
  std::memcpy(pDestination, pData, pArray->elemSize);
  ++pArray->size;
  return pDestination;
}

class ScopedTriggerDebugLogCapture {
 public:
  ScopedTriggerDebugLogCapture() : previousDebugFlag_(stDebugFlag) {
    gTriggerDebugLogs.clear();
    stub_.set(taosPrintLog, captureTriggerDebugLog);
    stDebugFlag = previousDebugFlag_ | DEBUG_DEBUG | DEBUG_FILE;
  }

  ~ScopedTriggerDebugLogCapture() {
    stDebugFlag = previousDebugFlag_;
    gTriggerDebugLogs.clear();
  }

 private:
  Stub    stub_;
  int32_t previousDebugFlag_;
};

class StreamTriggerDebugTest : public ::testing::Test {
 protected:
  void SetUp() override {
    task_.task.type = STREAM_TRIGGER_TASK;
    task_.task.streamId = 101;
    task_.task.taskId = 202;
    task_.task.seriousId = 303;
    task_.task.nodeId = 4;
    task_.task.status = STREAM_STATUS_RUNNING;
    taosInitRWLatch(&task_.readerProgressLock);
    taosInitRWLatch(&task_.debugGaugesLock);
    task_.pReaderProgressSnapshots = taosArrayInit(0, sizeof(SStreamReaderProgressSnapshot));
    ASSERT_NE(task_.pReaderProgressSnapshots, nullptr);
    ASSERT_EQ(stRecalcTrackerCreate(&task_.pRecalcTracker), TSDB_CODE_SUCCESS);

    task_.commonDebugGauges = {
        .activeRecalcCount = 0,
        .pendingRecalcRequestCount = 4,
        .historyProgressPct = 25,
        .validMask = STREAM_TRIGGER_GAUGE_ACTIVE_RECALC | STREAM_TRIGGER_GAUGE_HISTORY_PROGRESS |
                     STREAM_TRIGGER_GAUGE_PENDING_RECALC_REQUEST,
    };
    task_.realtimeDebugGauges = {
        .present = true,
        .pendingPullRetryCount = 2,
        .pendingCalcRetryCount = 3,
        .lastCheckpointAtMs = 5000,
        .checkpointLoaded = true,
        .recovering = false,
        .realtimeGroupCount = 5,
        .pendingNotifyCount = 7,
        .metaPoolUsed = 8,
        .metaPoolCapacity = 9,
        .metaPoolBytes = 10,
        .tableUidPoolUsed = 11,
        .tableUidPoolCapacity = 12,
        .tableUidPoolBytes = 13,
        .windowPoolUsed = 14,
        .windowPoolCapacity = 15,
        .windowPoolBytes = 16,
        .calcParamPoolUsed = 17,
        .calcParamPoolCapacity = 18,
        .calcParamPoolBytes = 19,
        .validMask = STREAM_TRIGGER_GAUGE_PENDING_PULL_RETRY | STREAM_TRIGGER_GAUGE_PENDING_CALC_RETRY |
                     STREAM_TRIGGER_GAUGE_LAST_CHECKPOINT | STREAM_TRIGGER_GAUGE_CHECKPOINT_LOADED |
                     STREAM_TRIGGER_GAUGE_RECOVERING | STREAM_TRIGGER_GAUGE_REALTIME_GROUP |
                     STREAM_TRIGGER_GAUGE_PENDING_NOTIFY | STREAM_TRIGGER_GAUGE_META_POOL |
                     STREAM_TRIGGER_GAUGE_TABLE_UID_POOL | STREAM_TRIGGER_GAUGE_WINDOW_POOL |
                     STREAM_TRIGGER_GAUGE_CALC_PARAM_POOL,
    };
    task_.historyDebugGauges = {
        .present = true,
        .historyGroupCount = 6,
        .validMask = STREAM_TRIGGER_GAUGE_PENDING_PULL_RETRY | STREAM_TRIGGER_GAUGE_PENDING_CALC_RETRY |
                     STREAM_TRIGGER_GAUGE_HISTORY_GROUP | STREAM_TRIGGER_GAUGE_PENDING_NOTIFY |
                     STREAM_TRIGGER_GAUGE_CALC_PARAM_POOL,
    };

    snapshot_.taskType = STREAM_TRIGGER_TASK;
    snapshot_.statsStartAtMs = 1000;
    snapshot_.uptimeMs = 180000;
    snapshot_.statsWindowMs = 180000;
    snapshot_.period.trigger = {
        .realtimeCheckCount = 1,
        .historyCheckCount = 2,
        .logicalWindowCount = 3,
        .calcRequestCount = 4,
        .readerPullRetryCount = 5,
        .runnerCalcRetryCount = 6,
        .notifyCount = 7,
        .dropCount = 8,
        .failureCount = 9,
        .invalidWalTimeCount = 10,
        .realtimeDuration = {.samples = 2, .totalUs = 3000, .maxUs = 2000, .maxAtMs = 6000},
        .historyDuration = {.samples = 1, .totalUs = 4000, .maxUs = 4000, .maxAtMs = 7000},
    };
    snapshot_.cumulative.trigger.realtimeDuration = {.samples = 3, .totalUs = 6000, .maxUs = 5000, .maxAtMs = 8000};
    snapshot_.cumulative.trigger.historyDuration = {.samples = 4, .totalUs = 9000, .maxUs = 6000, .maxAtMs = 9000};
  }

  void TearDown() override {
    stTaskStatsDestroy(&task_.pStats);
    stRecalcTrackerDestroy(&task_.pRecalcTracker);
    taosArrayDestroy(task_.pReaderProgressSnapshots);
    task_.pReaderProgressSnapshots = nullptr;
  }

  void AddReader(int64_t taskId, int32_t nodeId, int64_t verTime, bool external = false) {
    SStreamReaderProgressSnapshot reader = {
        .taskId = taskId,
        .nodeId = nodeId,
        .startVer = 10,
        .savedVer = 20,
        .doneVer = 30,
        .lastScanVer = 40,
        .verTime = verTime,
        .externalSource = external,
    };
    ASSERT_NE(taosArrayPush(task_.pReaderProgressSnapshots, &reader), nullptr);
  }

  void AddActiveRecalc(int64_t recalcId, bool running = true) {
    SArray* groups = taosArrayInit(1, sizeof(int64_t));
    ASSERT_NE(groups, nullptr);
    int64_t gid = 7;
    ASSERT_NE(taosArrayPush(groups, &gid), nullptr);
    ASSERT_EQ(stRecalcTrackerRegisterJob(task_.pRecalcTracker, recalcId, {100, 200}, groups), TSDB_CODE_SUCCESS);
    taosArrayDestroy(groups);
    if (running) ASSERT_EQ(stRecalcTrackerMarkJobRunning(task_.pRecalcTracker, recalcId), TSDB_CODE_SUCCESS);
  }

  void AddTerminalRecalc(int64_t recalcId) {
    ASSERT_EQ(stRecalcTrackerRegisterJob(task_.pRecalcTracker, recalcId, {100, 100}, nullptr), TSDB_CODE_SUCCESS);
  }

  int32_t EmitPeriod() { return stTriggerTaskLogStats(&task_, &snapshot_); }

  static int32_t CountRecords(const std::string& needle) {
    int32_t count = 0;
    for (const auto& line : gTriggerDebugLogs) {
      if (line.find(needle) != std::string::npos) ++count;
    }
    return count;
  }

  static const std::string& FindRecord(const std::string& needle) {
    for (const auto& line : gTriggerDebugLogs) {
      if (line.find(needle) != std::string::npos) return line;
    }
    static const std::string empty;
    return empty;
  }

  static int32_t CountToken(const std::string& line, const std::string& token) {
    int32_t count = 0;
    size_t  offset = 0;
    while ((offset = line.find(token, offset)) != std::string::npos) {
      ++count;
      offset += token.size();
    }
    return count;
  }

  SStreamTriggerTask        task_ = {};
  SStreamTaskPeriodSnapshot snapshot_ = {};
};

TEST_F(StreamTriggerDebugTest, TriggerPeriodDoesNotInlineReaderOrRecalcArrays) {
  ScopedTriggerDebugLogCapture capture;
  AddReader(11, 1, taosGetTimestampMs() * 1000 - 1000000);
  AddActiveRecalc(41);
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  const auto& period = FindRecord("record=task_period task_type=trigger");
  ASSERT_FALSE(period.empty());
  EXPECT_EQ(period.find("reader_task_id="), std::string::npos);
  EXPECT_EQ(period.find("recalc_id="), std::string::npos);
}

TEST_F(StreamTriggerDebugTest, OneReaderProducesOneReaderProgressRecord) {
  ScopedTriggerDebugLogCapture capture;
  AddReader(11, 1, taosGetTimestampMs() * 1000 - 1000000);
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(CountRecords("record=reader_progress"), 1);
  const auto& reader = FindRecord("record=reader_progress");
  EXPECT_NE(reader.find("reader_task_id=11"), std::string::npos);
  EXPECT_NE(reader.find("ver_time_valid=true"), std::string::npos);
}

TEST_F(StreamTriggerDebugTest, ReaderVersionTimeIsLoggedInUnixMilliseconds) {
  ScopedTriggerDebugLogCapture capture;
  AddReader(11, 1, 9000000);
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  const auto& reader = FindRecord("record=reader_progress");
  ASSERT_FALSE(reader.empty());
  EXPECT_NE(reader.find(" ver_time=9000 "), std::string::npos);
}

TEST_F(StreamTriggerDebugTest, MaximumReaderVersionTimeThatIsFutureWithinMillisecondIsInvalid) {
  ScopedTriggerDebugLogCapture capture;
  Stub                         timeStub;
  timeStub.set(taosGetTimeOfDay, maximumReaderTimeOfDay);
  AddReader(11, 1, INT64_MAX);
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  const auto& reader = FindRecord("record=reader_progress");
  ASSERT_FALSE(reader.empty());
  EXPECT_NE(reader.find(" ver_time=NA "), std::string::npos) << reader;
  EXPECT_NE(reader.find(" ver_time_valid=false "), std::string::npos) << reader;
  EXPECT_NE(reader.find(" reader_lag_ms=NA "), std::string::npos) << reader;
}

TEST_F(StreamTriggerDebugTest, MaximumValidReaderVersionTimeConvertsWithoutOverflow) {
  ScopedTriggerDebugLogCapture capture;
  Stub                         timeStub;
  timeStub.set(taosGetTimeOfDay, maximumReaderTimeOfDayNextMs);
  AddReader(11, 1, INT64_MAX);
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  const auto& reader = FindRecord("record=reader_progress");
  ASSERT_FALSE(reader.empty());
  EXPECT_NE(reader.find(" ver_time=" + std::to_string(INT64_MAX / 1000) + " "), std::string::npos) << reader;
  EXPECT_NE(reader.find(" ver_time_valid=true "), std::string::npos) << reader;
  EXPECT_NE(reader.find(" reader_lag_ms=1 "), std::string::npos) << reader;
}

TEST_F(StreamTriggerDebugTest, SubMillisecondFutureReaderTimesAreInvalid) {
  ScopedTriggerDebugLogCapture capture;
  Stub                         timeStub;
  timeStub.set(taosGetTimeOfDay, readerTimeOfDayAtNineSeconds);
  AddReader(11, 1, 9000001);
  AddReader(12, 2, 9000999);
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  for (int64_t taskId : {11, 12}) {
    const auto& reader = FindRecord("reader_task_id=" + std::to_string(taskId));
    ASSERT_FALSE(reader.empty());
    EXPECT_NE(reader.find(" ver_time=NA "), std::string::npos) << reader;
    EXPECT_NE(reader.find(" ver_time_valid=false "), std::string::npos) << reader;
    EXPECT_NE(reader.find(" reader_lag_ms=NA "), std::string::npos) << reader;
    EXPECT_NE(reader.find(" slowest_reader=false"), std::string::npos) << reader;
  }
}

TEST_F(StreamTriggerDebugTest, InvalidReaderStillPrintsButIsNotSlowest) {
  ScopedTriggerDebugLogCapture capture;
  const int64_t                nowUs = taosGetTimestampMs() * 1000;
  AddReader(11, 1, nowUs - 5000000);
  AddReader(12, 2, nowUs + 60000000);
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(CountRecords("record=reader_progress"), 2);
  const auto& valid = FindRecord("reader_task_id=11");
  const auto& invalid = FindRecord("reader_task_id=12");
  EXPECT_NE(valid.find("slowest_reader=true"), std::string::npos);
  EXPECT_NE(invalid.find("ver_time_valid=false"), std::string::npos);
  EXPECT_NE(invalid.find("ver_time=NA"), std::string::npos);
  EXPECT_NE(invalid.find("reader_lag_ms=NA"), std::string::npos);
  EXPECT_NE(invalid.find("slowest_reader=false"), std::string::npos);
}

TEST_F(StreamTriggerDebugTest, EachActiveRecalcProducesOneProgressRecord) {
  ScopedTriggerDebugLogCapture capture;
  AddActiveRecalc(41, false);
  AddActiveRecalc(42, true);
  AddTerminalRecalc(43);
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(CountRecords("record=recalc_progress"), 2);
  EXPECT_NE(FindRecord("record=recalc_progress").find("fixed_group_count=1"), std::string::npos);
}

TEST_F(StreamTriggerDebugTest, RecalcRecordsUseDistinctStatusKey) {
  ScopedTriggerDebugLogCapture capture;
  AddActiveRecalc(41);
  AddTerminalRecalc(42);
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);

  const auto& progress = FindRecord("record=recalc_progress");
  const auto& terminal = FindRecord("record=recalc_terminal");
  ASSERT_FALSE(progress.empty());
  ASSERT_FALSE(terminal.empty());
  EXPECT_EQ(CountToken(progress, " status="), 1);
  EXPECT_EQ(CountToken(progress, " recalc_status="), 1);
  EXPECT_EQ(CountToken(terminal, " status="), 1);
  EXPECT_EQ(CountToken(terminal, " recalc_status="), 1);
}

TEST_F(StreamTriggerDebugTest, TerminalRecalcPrintsExactlyOnce) {
  ScopedTriggerDebugLogCapture capture;
  AddTerminalRecalc(42);
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(CountRecords("record=recalc_terminal"), 1);
  EXPECT_NE(FindRecord("record=recalc_terminal").find("terminal_at="), std::string::npos);

  gTriggerDebugLogs.clear();
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(CountRecords("record=recalc_terminal"), 0);
}

TEST_F(StreamTriggerDebugTest, TerminalOnlyHeartbeatKeepsIdentityWithoutPeriodRecords) {
  ScopedTriggerDebugLogCapture capture;
  ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_LOGICAL_INPUT, 0, 1000, &task_.pStats),
            TSDB_CODE_SUCCESS);
  AddTerminalRecalc(42);
  ASSERT_EQ(stTriggerTaskLogStats(&task_, nullptr), TSDB_CODE_SUCCESS);
  ASSERT_EQ(gTriggerDebugLogs.size(), 1);
  const auto& terminal = FindRecord("record=recalc_terminal");
  ASSERT_FALSE(terminal.empty());
  EXPECT_NE(terminal.find("stats_start_at=1000"), std::string::npos);
  EXPECT_EQ(CountRecords("record=task_period"), 0);
  EXPECT_EQ(CountRecords("record=reader_progress"), 0);
  EXPECT_EQ(CountRecords("record=recalc_progress"), 0);
  EXPECT_EQ(CountRecords("record=resources"), 0);
}

TEST_F(StreamTriggerDebugTest, NonRotatedHeartbeatDrainsTerminalOnlyOnce) {
  ScopedTriggerDebugLogCapture capture;
  ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_LOGICAL_INPUT, 0, 1000, &task_.pStats),
            TSDB_CODE_SUCCESS);
  AddTerminalRecalc(42);

  ASSERT_EQ(stmMaybeRotateTaskStats(&task_.task, task_.pStats, STREAM_STATS_PERIOD_US / 2, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(CountRecords("record=recalc_terminal"), 1);
  EXPECT_EQ(CountRecords("record=task_period"), 0);

  gTriggerDebugLogs.clear();
  ASSERT_EQ(stmMaybeRotateTaskStats(&task_.task, task_.pStats, STREAM_STATS_PERIOD_US / 2, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(CountRecords("record=recalc_terminal"), 0);
  EXPECT_EQ(CountRecords("record=task_period"), 0);
}

TEST_F(StreamTriggerDebugTest, MissingStatsDoesNotConsumeTerminalEvent) {
  ScopedTriggerDebugLogCapture capture;
  AddTerminalRecalc(42);
  EXPECT_EQ(stTriggerTaskLogStats(&task_, nullptr), TSDB_CODE_INVALID_PARA);
  EXPECT_TRUE(gTriggerDebugLogs.empty());

  ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_LOGICAL_INPUT, 0, 1000, &task_.pStats),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(stTriggerTaskLogStats(&task_, nullptr), TSDB_CODE_SUCCESS);
  ASSERT_EQ(gTriggerDebugLogs.size(), 1);
  EXPECT_NE(gTriggerDebugLogs[0].find("stats_start_at=1000"), std::string::npos);
}

TEST_F(StreamTriggerDebugTest, TerminalCopyFailureRetriesWithoutMarking) {
  AddTerminalRecalc(42);
  SArray* terminals = nullptr;
  {
    Stub stub;
    stub.set(taosArrayInit, failTerminalArrayInit);
    EXPECT_EQ(stRecalcTrackerTakeTerminalEvents(task_.pRecalcTracker, &terminals), TSDB_CODE_OUT_OF_MEMORY);
  }
  EXPECT_EQ(terminals, nullptr);
  ASSERT_EQ(stRecalcTrackerTakeTerminalEvents(task_.pRecalcTracker, &terminals), TSDB_CODE_SUCCESS);
  ASSERT_NE(terminals, nullptr);
  EXPECT_EQ(taosArrayGetSize(terminals), 1);
  taosArrayDestroy(terminals);
}

TEST_F(StreamTriggerDebugTest, BatchTerminalCopyFailureRetriesAllEventsExactlyOnce) {
  ScopedTriggerDebugLogCapture capture;
  ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_LOGICAL_INPUT, 0, 1000, &task_.pStats),
            TSDB_CODE_SUCCESS);
  AddTerminalRecalc(41);
  AddTerminalRecalc(42);
  AddTerminalRecalc(43);

  gTerminalArrayPushCalls = 0;
  {
    Stub stub;
    stub.set(taosArrayAddBatch, failSecondTerminalArrayPush);
    EXPECT_EQ(stTriggerTaskLogStats(&task_, nullptr), TSDB_CODE_OUT_OF_MEMORY);
  }
  EXPECT_TRUE(gTriggerDebugLogs.empty());

  ASSERT_EQ(stTriggerTaskLogStats(&task_, nullptr), TSDB_CODE_SUCCESS);
  EXPECT_EQ(CountRecords("record=recalc_terminal"), 3);
  gTriggerDebugLogs.clear();
  ASSERT_EQ(stTriggerTaskLogStats(&task_, nullptr), TSDB_CODE_SUCCESS);
  EXPECT_EQ(CountRecords("record=recalc_terminal"), 0);
}

TEST_F(StreamTriggerDebugTest, ResourcesUseGaugesWithoutGroupScan) {
  ScopedTriggerDebugLogCapture capture;
  gGroupSizeCalls = 0;
  {
    Stub stub;
    stub.set(tSimpleHashGetSize, countGroupSizeCalls);
    ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  }
  EXPECT_EQ(gGroupSizeCalls, 0);
  const auto& resources = FindRecord("record=resources");
  ASSERT_FALSE(resources.empty());
  EXPECT_NE(resources.find("realtime_group_count=5"), std::string::npos);
  EXPECT_NE(resources.find("calc_param_pool_used=17"), std::string::npos);
  EXPECT_NE(resources.find("calc_param_pool_bytes=19"), std::string::npos);
}

TEST_F(StreamTriggerDebugTest, RealtimeGaugeRefreshNeverReadsHistorySibling) {
  SSTriggerRealtimeContext realtime = {};
  tdListInit(&realtime.retryPullReqs, POINTER_BYTES);
  tdListInit(&realtime.retryCalcReqs, POINTER_BYTES);
  realtime.pGroups = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(realtime.pGroups, nullptr);
  realtime.pNotifyParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  ASSERT_NE(realtime.pNotifyParams, nullptr);

  SSTriggerHistoryContext history = {};
  history.pGroups = reinterpret_cast<SSHashObj*>(1);
  task_.pHistoryContext = &history;
  gForbiddenGroups = history.pGroups;
  gForbiddenGroupSizeCalls = 0;
  {
    Stub stub;
    stub.set(tSimpleHashGetSize, countForbiddenGroupSizeCalls);
    stTriggerTaskPublishRealtimeDebugGauges(&task_, &realtime);
  }
  task_.pHistoryContext = nullptr;
  gForbiddenGroups = nullptr;
  EXPECT_EQ(gForbiddenGroupSizeCalls, 0);

  ScopedTriggerDebugLogCapture capture;
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  EXPECT_NE(FindRecord("record=task_period").find("realtime_session_count=1"), std::string::npos);

  taosArrayDestroy(realtime.pNotifyParams);
  tSimpleHashCleanup(realtime.pGroups);
}

TEST_F(StreamTriggerDebugTest, ExecuteDoesNotReadRealtimeContextAfterWorkerPublication) {
  SSTriggerRealtimeContext realtime = {};
  realtime.pTask = &task_;
  realtime.sessionId = kRealtimeSessionId;
  realtime.pGroups = reinterpret_cast<SSHashObj*>(1);
  task_.pRealtimeContext = &realtime;
  atomic_store_8(&task_.realtimeStarted, 0);

  getSynEpset_f oldGetSyncEndpoint = gStreamMgmt.getSynEpset;
  PutToQueueFp  oldPutToQueue = gStreamMgmt.msgCb.putToQueueFp;
  gStreamMgmt.getSynEpset = returnNoSyncEndpoint;
  gStreamMgmt.msgCb.putToQueueFp = publishTriggerWorkerOwnership;
  gTriggerExecuteWorkerPublished = false;
  gForbiddenGroups = realtime.pGroups;
  gForbiddenGroupSizeCalls = 0;

  SStreamMsg msg = {.msgType = STREAM_MSG_START};
  int32_t    code = TSDB_CODE_SUCCESS;
  {
    Stub stub;
    stub.set(tSimpleHashGetSize, countExecuteOwnerReads);
    code = stTriggerTaskExecute(&task_, &msg);
  }

  gStreamMgmt.getSynEpset = oldGetSyncEndpoint;
  gStreamMgmt.msgCb.putToQueueFp = oldPutToQueue;
  task_.pRealtimeContext = nullptr;
  gForbiddenGroups = nullptr;
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_TRUE(gTriggerExecuteWorkerPublished);
  EXPECT_EQ(gForbiddenGroupSizeCalls, 0);
}

TEST_F(StreamTriggerDebugTest, ErrorExitDoesNotPublishPartiallyInitializedHistoryContext) {
  SSTriggerHistoryContext history = {};
  history.pTask = &task_;
  history.sessionId = kHistorySessionId;
  history.pGroups = reinterpret_cast<SSHashObj*>(1);
  task_.pHistoryContext = &history;
  stTriggerTaskClearRealtimeDebugGauges(&task_);
  stTriggerTaskClearHistoryDebugGauges(&task_);
  ASSERT_FALSE(task_.historyDebugGauges.present);

  SSTriggerCalcRequest request = {};
  request.sessionId = kHistorySessionId;
  request.runnerTaskId = kRunnerTaskId;
  SSTriggerAHandle responseAhandle = {};
  responseAhandle.param = &request;
  SMsgSendInfo responseSendInfo = {};
  responseSendInfo.param = &responseAhandle;
  SRpcMsg response = {};
  response.msgType = TDMT_STREAM_TRIGGER_CALC_RSP;
  response.code = TSDB_CODE_FAILED;
  response.info.ahandle = &responseSendInfo;

  gForbiddenGroups = history.pGroups;
  gForbiddenGroupSizeCalls = 0;
  int64_t errorTaskId = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  {
    Stub stub;
    stub.set(tSimpleHashGetSize, countForbiddenGroupSizeCalls);
    code = stTriggerTaskProcessRsp(&task_.task, &response, &errorTaskId);
  }
  task_.pHistoryContext = nullptr;
  gForbiddenGroups = nullptr;
  EXPECT_EQ(code, TSDB_CODE_FAILED);
  EXPECT_EQ(gForbiddenGroupSizeCalls, 0);
  EXPECT_FALSE(task_.historyDebugGauges.present);

  ScopedTriggerDebugLogCapture capture;
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  const auto& period = FindRecord("record=task_period");
  const auto& resources = FindRecord("record=resources");
  ASSERT_FALSE(period.empty());
  ASSERT_FALSE(resources.empty());
  EXPECT_NE(period.find("history_session_count=0"), std::string::npos) << period;
  EXPECT_NE(resources.find("calc_param_pool_used=NA"), std::string::npos) << resources;
}

TEST_F(StreamTriggerDebugTest, UpdateDoesNotPublishPartiallyInitializedRealtimeContext) {
  SSTriggerRealtimeContext realtime = {};
  realtime.pTask = &task_;
  realtime.sessionId = kRealtimeSessionId;
  realtime.pGroups = reinterpret_cast<SSHashObj*>(1);
  stTriggerTaskClearRealtimeDebugGauges(&task_);
  ASSERT_FALSE(task_.realtimeDebugGauges.present);

  gForbiddenGroups = realtime.pGroups;
  gForbiddenGroupSizeCalls = 0;
  {
    Stub stub;
    stub.set(tSimpleHashGetSize, countForbiddenGroupSizeCalls);
    stTriggerTaskUpdateRealtimeDebugGauges(&task_, &realtime);
  }
  gForbiddenGroups = nullptr;
  EXPECT_FALSE(task_.realtimeDebugGauges.present);
  EXPECT_EQ(gForbiddenGroupSizeCalls, 0);
}

TEST_F(StreamTriggerDebugTest, ClearingRealtimeGaugeComponentDropsOwnedValues) {
  SSTriggerRealtimeContext realtime = {};
  tdListInit(&realtime.retryPullReqs, POINTER_BYTES);
  tdListInit(&realtime.retryCalcReqs, POINTER_BYTES);
  realtime.pGroups = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(realtime.pGroups, nullptr);
  realtime.pNotifyParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  ASSERT_NE(realtime.pNotifyParams, nullptr);
  realtime.metaPool = {.nodeSize = 8, .size = 1, .capacity = 2};

  stTriggerTaskPublishRealtimeDebugGauges(&task_, &realtime);
  stTriggerTaskClearRealtimeDebugGauges(&task_);

  ScopedTriggerDebugLogCapture capture;
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  const auto& period = FindRecord("record=task_period");
  const auto& resources = FindRecord("record=resources");
  EXPECT_NE(period.find("realtime_session_count=0"), std::string::npos);
  EXPECT_NE(resources.find("meta_pool_used=NA"), std::string::npos);

  taosArrayDestroy(realtime.pNotifyParams);
  tSimpleHashCleanup(realtime.pGroups);
}

TEST_F(StreamTriggerDebugTest, InvalidRealtimePoolInvalidatesValidHistoryAggregate) {
  SSTriggerRealtimeContext realtime = {};
  tdListInit(&realtime.retryPullReqs, POINTER_BYTES);
  tdListInit(&realtime.retryCalcReqs, POINTER_BYTES);
  realtime.pGroups = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(realtime.pGroups, nullptr);
  realtime.pNotifyParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  ASSERT_NE(realtime.pNotifyParams, nullptr);
  realtime.calcParamPool = {.nodeSize = 8, .size = 2, .capacity = 1};

  SSTriggerHistoryContext history = {};
  tdListInit(&history.retryPullReqs, POINTER_BYTES);
  tdListInit(&history.retryCalcReqs, POINTER_BYTES);
  history.pGroups = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(history.pGroups, nullptr);
  history.pNotifyParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  ASSERT_NE(history.pNotifyParams, nullptr);
  history.calcParamPool = {.nodeSize = 8, .size = 1, .capacity = 2};

  stTriggerTaskPublishRealtimeDebugGauges(&task_, &realtime);
  stTriggerTaskPublishHistoryDebugGauges(&task_, &history);

  ScopedTriggerDebugLogCapture capture;
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  const auto& resources = FindRecord("record=resources");
  ASSERT_FALSE(resources.empty());
  EXPECT_NE(resources.find("calc_param_pool_used=NA"), std::string::npos);
  EXPECT_NE(resources.find("calc_param_pool_capacity=NA"), std::string::npos);
  EXPECT_NE(resources.find("calc_param_pool_bytes=NA"), std::string::npos);

  taosArrayDestroy(history.pNotifyParams);
  tSimpleHashCleanup(history.pGroups);
  taosArrayDestroy(realtime.pNotifyParams);
  tSimpleHashCleanup(realtime.pGroups);
}

TEST_F(StreamTriggerDebugTest, CrossComponentPoolOverflowInvalidatesAggregate) {
  SSTriggerRealtimeContext realtime = {};
  tdListInit(&realtime.retryPullReqs, POINTER_BYTES);
  tdListInit(&realtime.retryCalcReqs, POINTER_BYTES);
  realtime.pGroups = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(realtime.pGroups, nullptr);
  realtime.pNotifyParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  ASSERT_NE(realtime.pNotifyParams, nullptr);
  realtime.calcParamPool = {.nodeSize = 1, .size = INT64_MAX, .capacity = INT64_MAX};

  SSTriggerHistoryContext history = {};
  tdListInit(&history.retryPullReqs, POINTER_BYTES);
  tdListInit(&history.retryCalcReqs, POINTER_BYTES);
  history.pGroups = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(history.pGroups, nullptr);
  history.pNotifyParams = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  ASSERT_NE(history.pNotifyParams, nullptr);
  history.calcParamPool = {.nodeSize = 1, .size = 1, .capacity = 1};

  stTriggerTaskPublishRealtimeDebugGauges(&task_, &realtime);
  stTriggerTaskPublishHistoryDebugGauges(&task_, &history);

  ScopedTriggerDebugLogCapture capture;
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  const auto& resources = FindRecord("record=resources");
  ASSERT_FALSE(resources.empty());
  EXPECT_NE(resources.find("calc_param_pool_used=NA"), std::string::npos);
  EXPECT_NE(resources.find("calc_param_pool_capacity=NA"), std::string::npos);
  EXPECT_NE(resources.find("calc_param_pool_bytes=NA"), std::string::npos);

  taosArrayDestroy(history.pNotifyParams);
  tSimpleHashCleanup(history.pGroups);
  taosArrayDestroy(realtime.pNotifyParams);
  tSimpleHashCleanup(realtime.pGroups);
}

TEST_F(StreamTriggerDebugTest, DebugOffDoesNotCopyDynamicSnapshots) {
  SStreamTaskStats* stats = nullptr;
  ASSERT_EQ(stTaskStatsCreate(STREAM_TRIGGER_TASK, STREAM_METRIC_LOGICAL_INPUT, 0, 1000, &stats), TSDB_CODE_SUCCESS);
  gDebugJobCopyCalls = 0;
  gTerminalTakeCalls = 0;
  {
    Stub stub;
    stub.set(stRecalcTrackerCopyDebugJobs, countDebugJobCopies);
    stub.set(stRecalcTrackerTakeTerminalEvents, countTerminalTakes);
    ASSERT_EQ(stmMaybeRotateTaskStats(&task_.task, stats, STREAM_STATS_PERIOD_US, false), TSDB_CODE_SUCCESS);
  }
  EXPECT_EQ(gDebugJobCopyCalls, 0);
  EXPECT_EQ(gTerminalTakeCalls, 0);
  stTaskStatsDestroy(&stats);
}

TEST_F(StreamTriggerDebugTest, TriggerRecordsContainCommonIdentity) {
  ScopedTriggerDebugLogCapture capture;
  AddReader(11, 1, taosGetTimestampMs() * 1000 - 1000000);
  AddActiveRecalc(41);
  AddTerminalRecalc(42);
  ASSERT_EQ(EmitPeriod(), TSDB_CODE_SUCCESS);
  ASSERT_EQ(gTriggerDebugLogs.size(), 5);
  for (const auto& line : gTriggerDebugLogs) {
    EXPECT_NE(line.find("stream_id=101"), std::string::npos);
    EXPECT_NE(line.find("task_id=202"), std::string::npos);
    EXPECT_NE(line.find("serious_id=303"), std::string::npos);
    EXPECT_NE(line.find("node_id=4"), std::string::npos);
    EXPECT_NE(line.find("task_type=trigger"), std::string::npos);
    EXPECT_NE(line.find("status=Running"), std::string::npos);
    EXPECT_NE(line.find("stats_start_at=1000"), std::string::npos);
  }
}

}  // namespace
