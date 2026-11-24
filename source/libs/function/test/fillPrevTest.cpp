/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include <gtest/gtest.h>

#include "fillPrev.h"
#include "tdatablock.h"
#include "ttime.h"

namespace {
class FillPrevTest : public ::testing::Test {
 protected:
  SFillPrevValue  value = {};
  SFillPrevRange  range = {};
  SColumnInfoData output = {};

  void TearDown() override {
    fillPrevValueReset(&value);
    fillPrevRangeDestroy(&range);
    colDataDestroy(&output);
  }

  bool contains(int64_t earlier, int64_t later) {
    bool result = false;
    EXPECT_EQ(0, fillPrevRangeContains(&range, earlier, later, &result));
    return result;
  }

  void prepare(const SDataType& type) {
    output.info.type = type.type;
    output.info.bytes = type.bytes;
    ASSERT_EQ(colInfoDataEnsureCapacity(&output, 1, true), 0);
  }
};

TEST_F(FillPrevTest, ClosedRangeUsesSourceTimestamp) {
  SDataType type = {TSDB_DATA_TYPE_INT, 0, 0, 4};
  int32_t   initial = 80;
  ASSERT_EQ(fillPrevValueSet(&value, &type, &initial, 8000), 0);
  ASSERT_EQ(fillPrevRangeInit(&range, true, 2, 's', TSDB_TIME_PRECISION_MILLI, nullptr), 0);
  prepare(type);
  for (int64_t ts : {9999, 10000, 10001}) {
    bool valid = contains(8000, ts);
    EXPECT_EQ(valid, ts <= 10000);
    ASSERT_EQ(fillPrevValueWrite(&value, valid, &output, 0), 0);
    EXPECT_EQ(colDataIsNull_s(&output, 0), ts > 10000);
    EXPECT_EQ(value.sourceTs, 8000);
  }
}

TEST_F(FillPrevTest, OwnedVariableValueAndWidthError) {
  SDataType type = {TSDB_DATA_TYPE_VARCHAR, 0, 0, 8};
  prepare(type);
  {
    char text[8] = {};
    varDataSetLen(text, 3);
    memcpy(varDataVal(text), "abc", 3);
    ASSERT_EQ(fillPrevValueSet(&value, &type, text, 17), 0);
    memset(text, 0, sizeof(text));
  }
  char oversized[10] = {};
  varDataSetLen(oversized, 8);
  EXPECT_EQ(fillPrevValueSet(&value, &type, oversized, 18), TSDB_CODE_QRY_NOT_ENOUGH_BUFFER);
  ASSERT_EQ(fillPrevValueWrite(&value, true, &output, 0), 0);
  ASSERT_NE(output.pData, nullptr);
  const char* actual = colDataGetData(&output, 0);
  ASSERT_EQ(varDataLen(actual), 3);
  EXPECT_EQ(memcmp(varDataVal(actual), "abc", 3), 0);
  EXPECT_EQ(value.sourceTs, 17);
}

TEST_F(FillPrevTest, GrowingStringsReuseCapacityWithinDeclaredWidth) {
  char      text[4096 + VARSTR_HEADER_SIZE] = {};
  SDataType type = {TSDB_DATA_TYPE_VARCHAR, 0, 0, sizeof(text)};
  int       growths = 0;
  for (int length = 1; length <= 4096; ++length) {
    varDataSetLen(text, length);
    varDataVal(text)[length - 1] = static_cast<char>('a' + length % 26);
    int previous = value.capacity;
    ASSERT_EQ(0, fillPrevValueSet(&value, &type, text, length));
    if (value.capacity != previous) ++growths;
    ASSERT_GE(value.capacity, varDataTLen(text));
    ASSERT_LE(value.capacity, type.bytes);
  }
  EXPECT_LE(growths, 13);
  EXPECT_EQ(0, memcmp(text, value.data, sizeof(text)));
}

TEST_F(FillPrevTest, NullDoesNotOverwrite) {
  SDataType type = {TSDB_DATA_TYPE_BIGINT, 0, 0, 8};
  int64_t   input = 19;
  prepare(type);
  ASSERT_EQ(fillPrevValueSet(&value, &type, &input, 4), 0);
  ASSERT_EQ(fillPrevValueSet(&value, &type, nullptr, 5), 0);
  ASSERT_EQ(fillPrevValueWrite(&value, true, &output, 0), 0);
  EXPECT_EQ(memcmp(colDataGetData(&output, 0), &input, sizeof(input)), 0);
  EXPECT_EQ(value.sourceTs, 4);
  fillPrevValueReset(&value);
  ASSERT_EQ(fillPrevValueWrite(&value, true, &output, 0), 0);
  EXPECT_TRUE(colDataIsNull_s(&output, 0));
}

TEST_F(FillPrevTest, DecimalAndScalarWidths) {
  int64_t input[] = {123456789, -19};
  for (auto id : {TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT,
                  TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DECIMAL64, TSDB_DATA_TYPE_DECIMAL}) {
    SDataType type = {static_cast<uint8_t>(id), 0, 0, tDataTypes[id].bytes};
    prepare(type);
    ASSERT_EQ(fillPrevValueSet(&value, &type, input, 1), 0);
    ASSERT_EQ(fillPrevValueWrite(&value, true, &output, 0), 0);
    EXPECT_EQ(memcmp(colDataGetData(&output, 0), input, type.bytes), 0) << id;
    colDataDestroy(&output);
    output = {};
  }
}

TEST_F(FillPrevTest, HourConversionBoundaries) {
  const int64_t maxHours = INT64_MAX / MILLISECOND_PER_HOUR;
  const int64_t lastTick = maxHours * MILLISECOND_PER_HOUR;
  ASSERT_EQ(fillPrevRangeInit(&range, true, maxHours, 'h', TSDB_TIME_PRECISION_MILLI, nullptr), 0);
  EXPECT_TRUE(contains(0, lastTick));
  EXPECT_FALSE(contains(0, lastTick + 1));
  ASSERT_EQ(0, fillPrevRangeInit(&range, true, maxHours + 1, 'h', TSDB_TIME_PRECISION_MILLI, nullptr));
  EXPECT_TRUE(contains(-1, INT64_MAX));
  EXPECT_FALSE(contains(INT64_MIN, INT64_MAX));

  // The largest whole microsecond count that fits in unsigned nanoseconds leaves 615 ticks.
  const int64_t maxMicros = UINT64_MAX / 1000;
  ASSERT_EQ(0, fillPrevRangeInit(&range, true, maxMicros, 'u', TSDB_TIME_PRECISION_NANO, nullptr));
  EXPECT_TRUE(contains(INT64_MIN, INT64_MAX - 615));
  EXPECT_FALSE(contains(INT64_MIN, INT64_MAX - 614));
  ASSERT_EQ(0, fillPrevRangeInit(&range, true, maxMicros + 1, 'u', TSDB_TIME_PRECISION_NANO, nullptr));
  EXPECT_TRUE(contains(INT64_MIN, INT64_MAX));

  ASSERT_EQ(0, fillPrevRangeInit(&range, true, INT64_MAX, 'w', TSDB_TIME_PRECISION_NANO, nullptr));
  EXPECT_TRUE(contains(INT64_MIN, INT64_MAX));
}

TEST_F(FillPrevTest, NegativeDurationDoesNotReplaceRange) {
  ASSERT_EQ(fillPrevRangeInit(&range, true, 2, 's', TSDB_TIME_PRECISION_MILLI, nullptr), 0);
  for (char unit : {'s', 'n'}) {
    EXPECT_EQ(fillPrevRangeInit(&range, true, -1, unit, TSDB_TIME_PRECISION_MILLI, nullptr), TSDB_CODE_OUT_OF_RANGE);
    EXPECT_TRUE(contains(0, 2000));
    EXPECT_FALSE(contains(0, 2001));
  }
}

TEST_F(FillPrevTest, NanosecondCalendarRangeAtUpperLimit) {
  const int64_t day = 86400LL * 1000000000;
  const int64_t jan2026 = 1767225600LL * 1000000000;
  ASSERT_EQ(fillPrevRangeInit(&range, true, 300, 'y', TSDB_TIME_PRECISION_NANO, "UTC"), 0);
  EXPECT_TRUE(contains(jan2026, jan2026 + day));
  EXPECT_TRUE(contains(jan2026, INT64_MAX));

  // The last nanosecond is on 2262-04-11 UTC, 31 days after March 11.
  for (char unit : {'n', 'y'}) {
    const int64_t start = INT64_MAX - (unit == 'n' ? 31 : 365) * day;
    ASSERT_EQ(fillPrevRangeInit(&range, true, 1, unit, TSDB_TIME_PRECISION_NANO, "UTC"), 0);
    EXPECT_TRUE(contains(start, INT64_MAX));
    EXPECT_TRUE(contains(start + 1, INT64_MAX));
    EXPECT_TRUE(contains(start - 1, INT64_MAX - 1));
    EXPECT_FALSE(contains(start - 1, INT64_MAX));
    ASSERT_EQ(fillPrevRangeInit(&range, true, INT64_MAX, unit, TSDB_TIME_PRECISION_NANO, "UTC"), 0);
    EXPECT_TRUE(contains(jan2026, INT64_MAX));
  }
}

TEST_F(FillPrevTest, CalendarMonthBeforeEpochExpiresAtTheSameLocalTime) {
  for (int8_t precision : {TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO}) {
    const int64_t ticks = TSDB_TICK_PER_SECOND(precision);
    // One tick before 1969-03-01 UTC is still February 28; one month later is March 28.
    const int64_t start = -26438400LL * ticks - 1;
    const int64_t end = -24019200LL * ticks - 1;
    ASSERT_EQ(0, fillPrevRangeInit(&range, true, 1, 'n', precision, "UTC"));
    EXPECT_TRUE(contains(start, end));
    EXPECT_FALSE(contains(start, end + 1));
  }
}

TEST_F(FillPrevTest, SharedCalendarRangesBorrowTheSameTimezone) {
  timezone_t shared = nullptr;
  ASSERT_EQ(0, fillPrevRangeInitShared(&range, true, 1, 'n', TSDB_TIME_PRECISION_MILLI, "America/New_York", &shared));
  ASSERT_NE(nullptr, shared);
  SFillPrevRange other = {};
  ASSERT_EQ(0, fillPrevRangeInitShared(&other, true, 1, 'y', TSDB_TIME_PRECISION_MILLI, "America/New_York", &shared));
  EXPECT_EQ(shared, range.tz);
  EXPECT_EQ(shared, other.tz);
  fillPrevRangeDestroy(&other);
  EXPECT_TRUE(contains(1707550200000LL, 1710055800000LL));
  fillPrevRangeDestroy(&range);
  tzfree(shared);
}

TEST_F(FillPrevTest, CalendarExpiryAcrossClockChanges) {
  struct Boundary {
    const char* zone;
    int64_t     start;
    int64_t     end;
  };
  const Boundary boundaries[] = {
      // New York 02:30 advances to 03:30 across the missing hour on March 10.
      {"America/New_York", 1707550200, 1710055800},
      // Lord Howe 02:15 advances to 02:45 across the missing half-hour on October 6.
      {"Australia/Lord_Howe", 1725551100, 1728143100},
      // Valid local noon on either side of the New York transition.
      {"America/New_York", 1710003600, 1712678400},
  };
  for (const auto& boundary : boundaries) {
    SCOPED_TRACE(boundary.zone);
    for (int8_t precision : {TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO}) {
      const int64_t ticks = TSDB_TICK_PER_SECOND(precision);
      const int64_t start = boundary.start * ticks + ticks - 1;
      const int64_t end = boundary.end * ticks + ticks - 1;
      ASSERT_EQ(0, fillPrevRangeInit(&range, true, 1, 'n', precision, boundary.zone));
      EXPECT_TRUE(contains(start, start + 86400 * ticks));
      EXPECT_TRUE(contains(start, end));
      EXPECT_FALSE(contains(start, end + 1));
    }
  }
}

TEST_F(FillPrevTest, CalendarExpiryKeepsTimezoneFoldChoice) {
  ASSERT_EQ(0, fillPrevRangeInit(&range, true, 1, 'n', TSDB_TIME_PRECISION_MILLI, "America/New_York"));
  struct tm fold = {};
  fold.tm_year = 2024 - 1900;
  fold.tm_mon = 10;
  fold.tm_mday = 3;
  fold.tm_hour = 1;
  fold.tm_min = 30;
  fold.tm_isdst = -1;
  const time_t chosen = taosMktime(&fold, range.tz);
  ASSERT_TRUE(chosen == 1730611800 || chosen == 1730615400);
  const int64_t start = 1727933400123LL;  // October 3 at 01:30:00.123 EDT.
  const int64_t end = chosen * INT64_C(1000) + 123;
  EXPECT_TRUE(contains(start, end));
  EXPECT_FALSE(contains(start, end + 1));
}

TEST_F(FillPrevTest, CalendarAndPrecisionBoundaries) {
  for (int8_t prec : {TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO}) {
    int64_t ticks = prec == TSDB_TIME_PRECISION_MILLI ? 1000 : prec == TSDB_TIME_PRECISION_MICRO ? 1000000 : 1000000000;
    ASSERT_EQ(fillPrevRangeInit(&range, true, 1, 's', prec, nullptr), 0);
    EXPECT_TRUE(contains(-ticks, 0));
    EXPECT_FALSE(contains(-ticks, 1));
    EXPECT_FALSE(contains(INT64_MIN, INT64_MAX));
    EXPECT_FALSE(contains(1, 0));
    fillPrevRangeDestroy(&range);
  }
  for (int amount : {0, 1}) {
    ASSERT_EQ(fillPrevRangeInit(&range, true, amount, 'b', TSDB_TIME_PRECISION_MILLI, nullptr), 0);
    EXPECT_TRUE(contains(3, 3));
    EXPECT_FALSE(contains(3, 4));
    fillPrevRangeDestroy(&range);
  }
  // UTC midnight on 2024-01-31 and 2024-02-29.
  ASSERT_EQ(fillPrevRangeInit(&range, true, 1, 'n', TSDB_TIME_PRECISION_MILLI, "UTC"), 0);
  EXPECT_TRUE(contains(1706659200000LL, 1709164800000LL));
  EXPECT_FALSE(contains(1706659200000LL, 1709164800001LL));
}
}  // namespace
