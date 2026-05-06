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
#include <inttypes.h>
#include "ttime.h"

/*
 * Test suite for quarter ('q') as parser alias of '3n'.
 * Verifies parseNatualDuration normalization and parseAbsoluteDuration rejection.
 */

class QuarterParseTest : public ::testing::Test {};

/* --- parseNatualDuration: q -> 3n normalization --- */

TEST_F(QuarterParseTest, LowerCaseQ_1q) {
  int64_t duration = 0;
  char    unit = 0;
  int32_t code = parseNatualDuration("1q", 2, &duration, &unit, TSDB_TIME_PRECISION_MILLI, false);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(duration, 3);
  EXPECT_EQ(unit, 'n');
}

TEST_F(QuarterParseTest, UpperCaseQ_1Q) {
  int64_t duration = 0;
  char    unit = 0;
  int32_t code = parseNatualDuration("1Q", 2, &duration, &unit, TSDB_TIME_PRECISION_MILLI, false);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(duration, 3);
  EXPECT_EQ(unit, 'n');
}

TEST_F(QuarterParseTest, MultipleQuarters_4q) {
  int64_t duration = 0;
  char    unit = 0;
  int32_t code = parseNatualDuration("4q", 2, &duration, &unit, TSDB_TIME_PRECISION_MILLI, false);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(duration, 12);
  EXPECT_EQ(unit, 'n');
}

TEST_F(QuarterParseTest, TwoQuarters_2q) {
  int64_t duration = 0;
  char    unit = 0;
  int32_t code = parseNatualDuration("2q", 2, &duration, &unit, TSDB_TIME_PRECISION_MILLI, false);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(duration, 6);
  EXPECT_EQ(unit, 'n');
}

/* overflow protection: INT64_MAX / 3 + 1 should fail */
TEST_F(QuarterParseTest, OverflowProtection) {
  char    buf[64];
  int     len = snprintf(buf, sizeof(buf), "%" PRId64 "q", (int64_t)(INT64_MAX / 3 + 1));
  int64_t duration = 0;
  char    unit = 0;
  int32_t code = parseNatualDuration(buf, len, &duration, &unit, TSDB_TIME_PRECISION_MILLI, false);
  EXPECT_NE(code, TSDB_CODE_SUCCESS);
}

/* INT64_MIN/3 - 1 is a negative literal; negativeAllow=true is required so
 * taosStr2Int64 does not reject the sign before the overflow check runs. */
TEST_F(QuarterParseTest, UnderflowProtection) {
  char    buf[64];
  int     len = snprintf(buf, sizeof(buf), "%" PRId64 "q", (int64_t)(INT64_MIN / 3 - 1));
  int64_t duration = 0;
  char    unit = 0;
  int32_t code = parseNatualDuration(buf, len, &duration, &unit, TSDB_TIME_PRECISION_MILLI, true);
  EXPECT_NE(code, TSDB_CODE_SUCCESS);
}

/* existing month/year units still work */
TEST_F(QuarterParseTest, MonthStillWorks) {
  int64_t duration = 0;
  char    unit = 0;
  int32_t code = parseNatualDuration("3n", 2, &duration, &unit, TSDB_TIME_PRECISION_MILLI, false);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(duration, 3);
  EXPECT_EQ(unit, 'n');
}

TEST_F(QuarterParseTest, YearStillWorks) {
  int64_t duration = 0;
  char    unit = 0;
  int32_t code = parseNatualDuration("1y", 2, &duration, &unit, TSDB_TIME_PRECISION_MILLI, false);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(duration, 1);
  EXPECT_EQ(unit, 'y');
}

/* --- parseAbsoluteDuration: q must be rejected --- */

TEST_F(QuarterParseTest, AbsoluteRejectsLowerQ) {
  int64_t duration = 0;
  char    unit = 0;
  int32_t code = parseAbsoluteDuration("1q", 2, &duration, &unit, TSDB_TIME_PRECISION_MILLI);
  EXPECT_NE(code, TSDB_CODE_SUCCESS);
}

TEST_F(QuarterParseTest, AbsoluteRejectsUpperQ) {
  int64_t duration = 0;
  char    unit = 0;
  int32_t code = parseAbsoluteDuration("1Q", 2, &duration, &unit, TSDB_TIME_PRECISION_MILLI);
  EXPECT_NE(code, TSDB_CODE_SUCCESS);
}
