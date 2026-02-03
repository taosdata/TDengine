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
#include "streamTriggerTask.h"
#include "ttime.h"

/**
 * Helper: Create a PERIOD trigger task for testing
 */
static SStreamTriggerTask* createPeriodTask(int64_t period, char periodUnit, int64_t offset = 0, char offsetUnit = 'd') {
  SStreamTriggerTask* pTask = (SStreamTriggerTask*)taosMemoryCalloc(1, sizeof(SStreamTriggerTask));
  if (!pTask) return NULL;

  auto getDuration = [](int64_t val, char unit, int32_t timePrecision) {
    switch (unit) {
      case 's':
        return convertTimePrecision(val * MILLISECOND_PER_SECOND, TSDB_TIME_PRECISION_MILLI, timePrecision);
      case 'm':
        return convertTimePrecision(val * MILLISECOND_PER_MINUTE, TSDB_TIME_PRECISION_MILLI, timePrecision);
      case 'h':
        return convertTimePrecision(val * MILLISECOND_PER_HOUR, TSDB_TIME_PRECISION_MILLI, timePrecision);
      case 'd':
        return convertTimePrecision(val * MILLISECOND_PER_DAY, TSDB_TIME_PRECISION_MILLI, timePrecision);
      case 'w':
        return convertTimePrecision(val * MILLISECOND_PER_WEEK, TSDB_TIME_PRECISION_MILLI, timePrecision);
      case 'a':
        return convertTimePrecision(val, TSDB_TIME_PRECISION_MILLI, timePrecision);
      case 'u':
        return convertTimePrecision(val, TSDB_TIME_PRECISION_MICRO, timePrecision);
      case 'b':
        return convertTimePrecision(val, TSDB_TIME_PRECISION_NANO, timePrecision);
      default:
        throw std::invalid_argument("Invalid duration unit");
    }
  };

  pTask->triggerType = STREAM_TRIGGER_PERIOD;
  pTask->interval.intervalUnit = periodUnit;
  pTask->interval.slidingUnit = periodUnit;
  pTask->interval.offsetUnit = offsetUnit;
  pTask->interval.precision = TSDB_TIME_PRECISION_NANO;
  pTask->interval.interval = 0;
  if (IS_CALENDAR_TIME_DURATION(periodUnit)) {
    pTask->interval.sliding = period;
  } else {
    pTask->interval.sliding = getDuration(period, periodUnit, TSDB_TIME_PRECISION_NANO);
  }
  pTask->interval.offset = getDuration(offset, offsetUnit, TSDB_TIME_PRECISION_NANO);

  return pTask;
}

/**
 * Helper: Create timestamp from date components
 */
static int64_t makeTimestamp(int year, int month, int day, int hour, int min, int sec) {
  struct tm tm = {0};
  tm.tm_year = year - 1900;
  tm.tm_mon = month - 1;
  tm.tm_mday = day;
  tm.tm_hour = hour;
  tm.tm_min = min;
  tm.tm_sec = sec;
  time_t tt = taosMktime(&tm, getGlobalDefaultTZ());
  return tt * TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
}

/**
 * Helper: Print timestamp for debugging
 */
static void printTimestamp(const char* label, int64_t ts) {
  int64_t   seconds = ts / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
  time_t    tt = (time_t)seconds;
  struct tm tm;
  if (taosLocalTime(&tt, &tm, NULL, 0, getGlobalDefaultTZ()) == NULL) {
    printf("%s: Invalid timestamp\n", label);
    return;
  }

  const char* weekdays[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
  printf("%s: %04d-%02d-%02d %02d:%02d:%02d (%s)\n", label, tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
         tm.tm_min, tm.tm_sec, weekdays[tm.tm_wday]);
}

/**
 * Helper: Check if timestamp is Monday 00:00:00
 */
static bool isMondayMidnight(int64_t ts) {
  int64_t   seconds = ts / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
  time_t    tt = (time_t)seconds;
  struct tm tm;
  if (taosLocalTime(&tt, &tm, NULL, 0, getGlobalDefaultTZ()) == NULL) return false;
  return (tm.tm_wday == 1 && tm.tm_hour == 0 && tm.tm_min == 0 && tm.tm_sec == 0);
}

/**
 * Helper: Check if timestamp is 1st of month 00:00:00
 */
static bool isFirstOfMonth(int64_t ts) {
  int64_t   seconds = ts / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
  time_t    tt = (time_t)seconds;
  struct tm tm;
  if (taosLocalTime(&tt, &tm, NULL, 0, getGlobalDefaultTZ()) == NULL) return false;
  return (tm.tm_mday == 1 && tm.tm_hour == 0 && tm.tm_min == 0 && tm.tm_sec == 0);
}

/**
 * Helper: Check if timestamp is January 1st 00:00:00
 */
static bool isJanuaryFirst(int64_t ts) {
  int64_t   seconds = ts / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
  time_t    tt = (time_t)seconds;
  struct tm tm;
  if (taosLocalTime(&tt, &tm, NULL, 0, getGlobalDefaultTZ()) == NULL) return false;
  return (tm.tm_mon == 0 && tm.tm_mday == 1 && tm.tm_hour == 0 && tm.tm_min == 0 && tm.tm_sec == 0);
}

/**
 * Helper: Check if timestamp is a specific weekday at 00:00:00
 * weekday: 0=Sunday, 1=Monday, 2=Tuesday, ..., 6=Saturday
 */
static bool isWeekdayMidnight(int64_t ts, int weekday) {
  int64_t   seconds = ts / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
  time_t    tt = (time_t)seconds;
  struct tm tm;
  if (taosLocalTime(&tt, &tm, NULL, 0, getGlobalDefaultTZ()) == NULL) return false;
  return (tm.tm_wday == weekday && tm.tm_hour == 0 && tm.tm_min == 0 && tm.tm_sec == 0);
}

/**
 * Helper: Check if timestamp is a specific day of month at 00:00:00
 */
static bool isDayOfMonth(int64_t ts, int day) {
  int64_t   seconds = ts / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
  time_t    tt = (time_t)seconds;
  struct tm tm;
  if (taosLocalTime(&tt, &tm, NULL, 0, getGlobalDefaultTZ()) == NULL) return false;
  return (tm.tm_mday == day && tm.tm_hour == 0 && tm.tm_min == 0 && tm.tm_sec == 0);
}

/**
 * Helper: Check if timestamp has specific time components
 */
static bool hasTimeComponents(int64_t ts, int hour, int min, int sec) {
  int64_t   seconds = ts / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
  time_t    tt = (time_t)seconds;
  struct tm tm;
  if (taosLocalTime(&tt, &tm, NULL, 0, getGlobalDefaultTZ()) == NULL) return false;
  return (tm.tm_hour == hour && tm.tm_min == min && tm.tm_sec == sec);
}

/**
 * Helper: Get day of month from timestamp
 */
static int getDayOfMonth(int64_t ts) {
  int64_t   seconds = ts / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
  time_t    tt = (time_t)seconds;
  struct tm tm;
  if (taosLocalTime(&tt, &tm, NULL, 0, getGlobalDefaultTZ()) == NULL) return -1;
  return tm.tm_mday;
}

// ============================================================================
// Weekly (w) Tests
// ============================================================================

/**
 * Test: PERIOD(1w) aligns to Monday 00:00:00
 */
TEST(StreamTrigger, WeeklyAlignment) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'w');
  ASSERT_NE(pTask, nullptr);

  // Wednesday 2024-01-10 15:30:00 -> should align to Monday 2024-01-08 00:00:00
  int64_t     ts = makeTimestamp(2024, 1, 10, 15, 30, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  printTimestamp("Input time", ts);
  printTimestamp("Window start", win.skey);
  printTimestamp("Window end", win.ekey);

  EXPECT_TRUE(isMondayMidnight(win.skey)) << "Window start should be Monday 00:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1w) on exact boundary (Monday 00:00:00)
 */
TEST(StreamTrigger, WeeklyBoundary) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'w');
  ASSERT_NE(pTask, nullptr);

  // Monday 2024-01-08 00:00:00 (exactly on boundary)
  int64_t     ts = makeTimestamp(2024, 1, 8, 0, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isMondayMidnight(win.skey)) << "Window start should be Monday 00:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1w) next window calculation
 */
TEST(StreamTrigger, WeeklyNextWindow) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'w');
  ASSERT_NE(pTask, nullptr);

  int64_t     ts = makeTimestamp(2024, 1, 10, 15, 30, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Calculate next window
  stTriggerTaskNextTimeWindow(pTask, &win);

  EXPECT_TRUE(isMondayMidnight(win.skey)) << "Next window should also be Monday 00:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(2w) bi-weekly trigger
 */
TEST(StreamTrigger, BiWeekly) {
  SStreamTriggerTask* pTask = createPeriodTask(2, 'w');
  ASSERT_NE(pTask, nullptr);

  int64_t     ts = makeTimestamp(2024, 1, 10, 15, 30, 0);
  STimeWindow win1 = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isMondayMidnight(win1.skey));

  // Next window should be 2 weeks later
  STimeWindow win2 = win1;
  stTriggerTaskNextTimeWindow(pTask, &win2);

  int64_t diff = (win2.skey - win1.skey) / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
  EXPECT_EQ(diff, 14 * 24 * 60 * 60) << "Should be exactly 2 weeks apart";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(4w) monthly-like trigger
 */
TEST(StreamTrigger, FourWeekly) {
  SStreamTriggerTask* pTask = createPeriodTask(4, 'w');
  ASSERT_NE(pTask, nullptr);

  int64_t     ts = makeTimestamp(2024, 1, 10, 15, 30, 0);
  STimeWindow win1 = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isMondayMidnight(win1.skey));

  // Next window should be 4 weeks later
  STimeWindow win2 = win1;
  stTriggerTaskNextTimeWindow(pTask, &win2);

  int64_t diff = (win2.skey - win1.skey) / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
  EXPECT_EQ(diff, 28 * 24 * 60 * 60) << "Should be exactly 4 weeks apart";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1w) across year boundary
 */
TEST(StreamTrigger, WeeklyYearBoundary) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'w');
  ASSERT_NE(pTask, nullptr);

  // Thursday 2024-12-26 -> should align to Monday 2024-12-23
  int64_t     ts = makeTimestamp(2024, 12, 26, 10, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isMondayMidnight(win.skey));

  // Next window should be Monday 2024-12-30
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isMondayMidnight(win.skey));

  // Next window should be Monday 2025-01-06 (crossing year boundary)
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isMondayMidnight(win.skey));

  taosMemoryFree(pTask);
}

// ============================================================================
// Monthly (n) Tests
// ============================================================================

/**
 * Test: PERIOD(1n) aligns to 1st of month 00:00:00
 */
TEST(StreamTrigger, MonthlyAlignment) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n');
  ASSERT_NE(pTask, nullptr);

  // 2024-01-15 12:00:00 -> should align to 2024-01-01 00:00:00
  int64_t     ts = makeTimestamp(2024, 1, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isFirstOfMonth(win.skey)) << "Window start should be 1st of month 00:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1n) on exact boundary (1st of month)
 */
TEST(StreamTrigger, MonthlyBoundary) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n');
  ASSERT_NE(pTask, nullptr);

  // 2024-02-01 00:00:00 (exactly on boundary)
  int64_t     ts = makeTimestamp(2024, 2, 1, 0, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isFirstOfMonth(win.skey));

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1n) next window calculation
 */
TEST(StreamTrigger, MonthlyNextWindow) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n');
  ASSERT_NE(pTask, nullptr);

  int64_t     ts = makeTimestamp(2024, 1, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isFirstOfMonth(win.skey));

  // Next window should be 2024-02-01
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isFirstOfMonth(win.skey));

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(3n) quarterly trigger
 */
TEST(StreamTrigger, Quarterly) {
  SStreamTriggerTask* pTask = createPeriodTask(3, 'n');
  ASSERT_NE(pTask, nullptr);

  int64_t     ts = makeTimestamp(2024, 2, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isFirstOfMonth(win.skey));

  // Next window should be 3 months later
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isFirstOfMonth(win.skey));

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1n) handles February (28/29 days)
 */
TEST(StreamTrigger, MonthlyFebruary) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n');
  ASSERT_NE(pTask, nullptr);

  // 2024-02-15 (leap year) -> should align to 2024-02-01
  int64_t     ts = makeTimestamp(2024, 2, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isFirstOfMonth(win.skey));

  // Next should be 2024-03-01
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isFirstOfMonth(win.skey));

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1n) handles months with different days (30/31)
 */
TEST(StreamTrigger, MonthlyDifferentDays) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n');
  ASSERT_NE(pTask, nullptr);

  // 2024-01-31 -> should align to 2024-01-01
  int64_t     ts = makeTimestamp(2024, 1, 31, 23, 59, 59);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isFirstOfMonth(win.skey));

  // Next should be 2024-02-01 (February has 29 days in 2024)
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isFirstOfMonth(win.skey));

  // Next should be 2024-03-01
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isFirstOfMonth(win.skey));

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1n) across year boundary
 */
TEST(StreamTrigger, MonthlyYearBoundary) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n');
  ASSERT_NE(pTask, nullptr);

  // 2024-12-15 -> should align to 2024-12-01
  int64_t     ts = makeTimestamp(2024, 12, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isFirstOfMonth(win.skey));

  // Next should be 2025-01-01 (crossing year boundary)
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isFirstOfMonth(win.skey));

  taosMemoryFree(pTask);
}

// ============================================================================
// Yearly (y) Tests
// ============================================================================

/**
 * Test: PERIOD(1y) aligns to January 1st 00:00:00
 */
TEST(StreamTrigger, YearlyAlignment) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'y');
  ASSERT_NE(pTask, nullptr);

  // 2024-06-15 12:00:00 -> should align to 2024-01-01 00:00:00
  int64_t     ts = makeTimestamp(2024, 6, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isJanuaryFirst(win.skey)) << "Window start should be January 1st 00:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1y) on exact boundary (January 1st)
 */
TEST(StreamTrigger, YearlyBoundary) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'y');
  ASSERT_NE(pTask, nullptr);

  // 2024-01-01 00:00:00 (exactly on boundary)
  int64_t     ts = makeTimestamp(2024, 1, 1, 0, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isJanuaryFirst(win.skey));

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1y) next window calculation
 */
TEST(StreamTrigger, YearlyNextWindow) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'y');
  ASSERT_NE(pTask, nullptr);

  int64_t     ts = makeTimestamp(2024, 6, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isJanuaryFirst(win.skey));

  // Next window should be 2025-01-01
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isJanuaryFirst(win.skey));

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(2y) bi-yearly trigger
 */
TEST(StreamTrigger, BiYearly) {
  SStreamTriggerTask* pTask = createPeriodTask(2, 'y');
  ASSERT_NE(pTask, nullptr);

  int64_t     ts = makeTimestamp(2024, 6, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isJanuaryFirst(win.skey));

  // Next window should be 2 years later
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isJanuaryFirst(win.skey));

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1y) handles leap year
 */
TEST(StreamTrigger, YearlyLeapYear) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'y');
  ASSERT_NE(pTask, nullptr);

  // 2024-02-29 (leap year) -> should align to 2024-01-01
  int64_t     ts = makeTimestamp(2024, 2, 29, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isJanuaryFirst(win.skey));

  // Next should be 2025-01-01 (non-leap year)
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isJanuaryFirst(win.skey));

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1y) end of year
 */
TEST(StreamTrigger, YearlyEndOfYear) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'y');
  ASSERT_NE(pTask, nullptr);

  // 2024-12-31 23:59:59 -> should align to 2024-01-01
  int64_t     ts = makeTimestamp(2024, 12, 31, 23, 59, 59);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isJanuaryFirst(win.skey));

  // Next should be 2025-01-01
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isJanuaryFirst(win.skey));

  taosMemoryFree(pTask);
}

// ============================================================================
// Edge Cases
// ============================================================================

/**
 * Test: Multiple consecutive next window calls
 */
TEST(StreamTrigger, ConsecutiveNextWindows) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'w');
  ASSERT_NE(pTask, nullptr);

  int64_t     ts = makeTimestamp(2024, 1, 10, 15, 30, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  // Call next window 10 times
  for (int i = 0; i < 10; i++) {
    stTriggerTaskNextTimeWindow(pTask, &win);
    EXPECT_TRUE(isMondayMidnight(win.skey)) << "Window " << i << " should be Monday";
  }

  taosMemoryFree(pTask);
}

/**
 * Test: Sunday before Monday boundary
 */
TEST(StreamTrigger, SundayBeforeMonday) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'w');
  ASSERT_NE(pTask, nullptr);

  // Sunday 2024-01-07 23:59:59 -> should align to Monday 2024-01-01
  int64_t     ts = makeTimestamp(2024, 1, 7, 23, 59, 59);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isMondayMidnight(win.skey));

  taosMemoryFree(pTask);
}

/**
 * Test: Last day of month
 */
TEST(StreamTrigger, LastDayOfMonth) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n');
  ASSERT_NE(pTask, nullptr);

  // 2024-01-31 23:59:59 -> should align to 2024-01-01
  int64_t     ts = makeTimestamp(2024, 1, 31, 23, 59, 59);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isFirstOfMonth(win.skey));

  taosMemoryFree(pTask);
}

// ============================================================================
// Offset Tests - Weekly
// ============================================================================

/**
 * Test: PERIOD(1w, 1d) aligns to Tuesday 00:00:00
 */
TEST(StreamTrigger, WeeklyOffset1Day) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'w', 1, 'd');
  ASSERT_NE(pTask, nullptr);

  // Wednesday 2024-01-10 15:30:00 -> should align to Tuesday 2024-01-09 00:00:00
  int64_t     ts = makeTimestamp(2024, 1, 10, 15, 30, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  printTimestamp("Input time", ts);
  printTimestamp("Window start", win.skey);

  EXPECT_TRUE(isWeekdayMidnight(win.skey, 2)) << "Window start should be Tuesday 00:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1w, 2d) aligns to Wednesday 00:00:00
 */
TEST(StreamTrigger, WeeklyOffset2Days) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'w', 2, 'd');
  ASSERT_NE(pTask, nullptr);

  // Friday 2024-01-12 10:00:00 -> should align to Wednesday 2024-01-10 00:00:00
  int64_t     ts = makeTimestamp(2024, 1, 12, 10, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isWeekdayMidnight(win.skey, 3)) << "Window start should be Wednesday 00:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1w, 6d) aligns to Sunday 00:00:00
 */
TEST(StreamTrigger, WeeklyOffset6Days) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'w', 6, 'd');
  ASSERT_NE(pTask, nullptr);

  // Tuesday 2024-01-09 10:00:00 -> should align to Sunday 2024-01-07 00:00:00
  int64_t     ts = makeTimestamp(2024, 1, 9, 10, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isWeekdayMidnight(win.skey, 0)) << "Window start should be Sunday 00:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1w, 1h) aligns to Monday 01:00:00
 */
TEST(StreamTrigger, WeeklyOffset1Hour) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'w', 1, 'h');
  ASSERT_NE(pTask, nullptr);

  // Wednesday 2024-01-10 15:30:00 -> should align to Monday 2024-01-08 01:00:00
  int64_t     ts = makeTimestamp(2024, 1, 10, 15, 30, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isWeekdayMidnight(win.skey, 1) || hasTimeComponents(win.skey, 1, 0, 0))
      << "Window start should be Monday 01:00:00";
  EXPECT_TRUE(hasTimeComponents(win.skey, 1, 0, 0)) << "Should be at 01:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1w, 12h) aligns to Monday 12:00:00
 */
TEST(StreamTrigger, WeeklyOffset12Hours) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'w', 12, 'h');
  ASSERT_NE(pTask, nullptr);

  // Wednesday 2024-01-10 15:30:00 -> should align to Monday 2024-01-08 12:00:00
  int64_t     ts = makeTimestamp(2024, 1, 10, 15, 30, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(hasTimeComponents(win.skey, 12, 0, 0)) << "Should be at 12:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(2w, 1d) bi-weekly with 1 day offset
 */
TEST(StreamTrigger, BiWeeklyOffset1Day) {
  SStreamTriggerTask* pTask = createPeriodTask(2, 'w', 1, 'd');
  ASSERT_NE(pTask, nullptr);

  int64_t     ts = makeTimestamp(2024, 1, 10, 15, 30, 0);
  STimeWindow win1 = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isWeekdayMidnight(win1.skey, 2)) << "Should align to Tuesday";

  // Next window should be 2 weeks later, still on Tuesday
  STimeWindow win2 = win1;
  stTriggerTaskNextTimeWindow(pTask, &win2);

  EXPECT_TRUE(isWeekdayMidnight(win2.skey, 2)) << "Next window should also be Tuesday";

  int64_t diff = (win2.skey - win1.skey) / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
  EXPECT_EQ(diff, 14 * 24 * 60 * 60) << "Should be exactly 2 weeks apart";

  taosMemoryFree(pTask);
}

// ============================================================================
// Offset Tests - Monthly
// ============================================================================

/**
 * Test: PERIOD(1n, 1d) aligns to 2nd of each month
 */
TEST(StreamTrigger, MonthlyOffset1Day) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n', 1, 'd');
  ASSERT_NE(pTask, nullptr);

  // 2024-01-15 12:00:00 -> should align to 2024-01-02 00:00:00
  int64_t     ts = makeTimestamp(2024, 1, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  printTimestamp("Input time", ts);
  printTimestamp("Window start", win.skey);

  EXPECT_TRUE(isDayOfMonth(win.skey, 2)) << "Window start should be 2nd of month 00:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1n, 2d) aligns to 3rd of each month
 */
TEST(StreamTrigger, MonthlyOffset2Days) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n', 2, 'd');
  ASSERT_NE(pTask, nullptr);

  // 2024-01-15 12:00:00 -> should align to 2024-01-03 00:00:00
  int64_t     ts = makeTimestamp(2024, 1, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isDayOfMonth(win.skey, 3)) << "Window start should be 3rd of month 00:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1n, 10d) aligns to 11th of each month
 */
TEST(StreamTrigger, MonthlyOffset10Days) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n', 10, 'd');
  ASSERT_NE(pTask, nullptr);

  // 2024-01-20 12:00:00 -> should align to 2024-01-11 00:00:00
  int64_t     ts = makeTimestamp(2024, 1, 20, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isDayOfMonth(win.skey, 11)) << "Window start should be 11th of month 00:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1n, 1h) aligns to 1st of month at 01:00:00
 */
TEST(StreamTrigger, MonthlyOffset1Hour) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n', 1, 'h');
  ASSERT_NE(pTask, nullptr);

  // 2024-01-15 12:00:00 -> should align to 2024-01-01 01:00:00
  int64_t     ts = makeTimestamp(2024, 1, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  printTimestamp("Input time", ts);
  printTimestamp("Window start", win.skey);

  EXPECT_TRUE(isDayOfMonth(win.skey, 1)) << "Should be 1st of month";
  EXPECT_TRUE(hasTimeComponents(win.skey, 1, 0, 0)) << "Should be at 01:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1n, 12h) aligns to 1st of month at 12:00:00
 */
TEST(StreamTrigger, MonthlyOffset12Hours) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n', 12, 'h');
  ASSERT_NE(pTask, nullptr);

  // 2024-01-15 18:00:00 -> should align to 2024-01-01 12:00:00
  int64_t     ts = makeTimestamp(2024, 1, 15, 18, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isDayOfMonth(win.skey, 1)) << "Should be 1st of month";
  EXPECT_TRUE(hasTimeComponents(win.skey, 12, 0, 0)) << "Should be at 12:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1n, 1d) next window calculation
 */
TEST(StreamTrigger, MonthlyOffset1DayNextWindow) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n', 1, 'd');
  ASSERT_NE(pTask, nullptr);

  int64_t     ts = makeTimestamp(2024, 1, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isDayOfMonth(win.skey, 2)) << "Should be 2nd of January";

  // Next window should be 2024-02-02
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isDayOfMonth(win.skey, 2)) << "Should be 2nd of February";

  // Next window should be 2024-03-02
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isDayOfMonth(win.skey, 2)) << "Should be 2nd of March";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(3n, 5d) quarterly with 5 day offset
 */
TEST(StreamTrigger, QuarterlyOffset5Days) {
  SStreamTriggerTask* pTask = createPeriodTask(3, 'n', 5, 'd');
  ASSERT_NE(pTask, nullptr);

  int64_t     ts = makeTimestamp(2024, 2, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isDayOfMonth(win.skey, 6)) << "Should be 6th of month (1st + 5 days offset)";

  // Next window should be 3 months later, still on 6th
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isDayOfMonth(win.skey, 6)) << "Should still be 6th of month";

  taosMemoryFree(pTask);
}

// ============================================================================
// Offset Tests - Yearly
// ============================================================================

/**
 * Test: PERIOD(1y, 1d) aligns to January 2nd
 */
TEST(StreamTrigger, YearlyOffset1Day) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'y', 1, 'd');
  ASSERT_NE(pTask, nullptr);

  // 2024-06-15 12:00:00 -> should align to 2024-01-02 00:00:00
  int64_t     ts = makeTimestamp(2024, 6, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  printTimestamp("Input time", ts);
  printTimestamp("Window start", win.skey);

  int day = getDayOfMonth(win.skey);
  EXPECT_EQ(day, 2) << "Window start should be January 2nd";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1y, 31d) aligns to February 1st
 */
TEST(StreamTrigger, YearlyOffset31Days) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'y', 31, 'd');
  ASSERT_NE(pTask, nullptr);

  // 2024-06-15 12:00:00 -> should align to 2024-02-01 00:00:00
  int64_t     ts = makeTimestamp(2024, 6, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  int64_t   seconds = win.skey / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
  time_t    tt = (time_t)seconds;
  struct tm tm;
  taosLocalTime(&tt, &tm, NULL, 0, getGlobalDefaultTZ());

  EXPECT_EQ(tm.tm_mon, 1) << "Should be February (month 1)";
  EXPECT_EQ(tm.tm_mday, 1) << "Should be 1st of month";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1y, 1h) aligns to January 1st at 01:00:00
 */
TEST(StreamTrigger, YearlyOffset1Hour) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'y', 1, 'h');
  ASSERT_NE(pTask, nullptr);

  // 2024-06-15 12:00:00 -> should align to 2024-01-01 01:00:00
  int64_t     ts = makeTimestamp(2024, 6, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  int64_t   seconds = win.skey / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_NANO);
  time_t    tt = (time_t)seconds;
  struct tm tm;
  taosLocalTime(&tt, &tm, NULL, 0, getGlobalDefaultTZ());

  EXPECT_EQ(tm.tm_mon, 0) << "Should be January";
  EXPECT_EQ(tm.tm_mday, 1) << "Should be 1st";
  EXPECT_EQ(tm.tm_hour, 1) << "Should be 01:00:00";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1y, 1d) next window calculation
 */
TEST(StreamTrigger, YearlyOffset1DayNextWindow) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'y', 1, 'd');
  ASSERT_NE(pTask, nullptr);

  int64_t     ts = makeTimestamp(2024, 6, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_EQ(getDayOfMonth(win.skey), 2) << "Should be January 2nd 2024";

  // Next window should be 2025-01-02
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_EQ(getDayOfMonth(win.skey), 2) << "Should be January 2nd 2025";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(2y, 10d) bi-yearly with 10 day offset
 */
TEST(StreamTrigger, BiYearlyOffset10Days) {
  SStreamTriggerTask* pTask = createPeriodTask(2, 'y', 10, 'd');
  ASSERT_NE(pTask, nullptr);

  int64_t     ts = makeTimestamp(2024, 6, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_EQ(getDayOfMonth(win.skey), 11) << "Should be January 11th (1st + 10 days)";

  // Next window should be 2 years later, still on January 11th
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_EQ(getDayOfMonth(win.skey), 11) << "Should still be January 11th";

  taosMemoryFree(pTask);
}

// ============================================================================
// Offset Edge Cases
// ============================================================================

/**
 * Test: PERIOD(1n, 1d) handles February correctly
 */
TEST(StreamTrigger, MonthlyOffsetFebruary) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'n', 1, 'd');
  ASSERT_NE(pTask, nullptr);

  // 2024-02-15 (leap year) -> should align to 2024-02-02
  int64_t     ts = makeTimestamp(2024, 2, 15, 12, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isDayOfMonth(win.skey, 2)) << "Should be 2nd of February";

  // Next should be 2024-03-02
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isDayOfMonth(win.skey, 2)) << "Should be 2nd of March";

  taosMemoryFree(pTask);
}

/**
 * Test: PERIOD(1w, 1d) across year boundary
 */
TEST(StreamTrigger, WeeklyOffsetYearBoundary) {
  SStreamTriggerTask* pTask = createPeriodTask(1, 'w', 1, 'd');
  ASSERT_NE(pTask, nullptr);

  // Thursday 2024-12-26 -> should align to Tuesday 2024-12-24
  int64_t     ts = makeTimestamp(2024, 12, 26, 10, 0, 0);
  STimeWindow win = stTriggerTaskGetTimeWindow(pTask, ts);

  EXPECT_TRUE(isWeekdayMidnight(win.skey, 2)) << "Should be Tuesday";

  // Next window should be Tuesday 2024-12-31
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isWeekdayMidnight(win.skey, 2)) << "Should be Tuesday";

  // Next window should be Tuesday 2025-01-07 (crossing year boundary)
  stTriggerTaskNextTimeWindow(pTask, &win);
  EXPECT_TRUE(isWeekdayMidnight(win.skey, 2)) << "Should be Tuesday";

  taosMemoryFree(pTask);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
