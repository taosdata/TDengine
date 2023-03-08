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
#include <iostream>

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

TEST(osTimeTests, taosLocalTimeNolock) {
  time_t currentTime;
    // Test when result is NULL
  struct tm* result = taosLocalTimeNolock(NULL, &currentTime, 0);
    // Test when result is not NULL
  struct tm expectedTime;
  result = taosLocalTimeNolock(&expectedTime, &currentTime, 1);
  EXPECT_EQ(expectedTime.tm_year, result->tm_year);
  EXPECT_EQ(expectedTime.tm_mon, result->tm_mon);
  EXPECT_EQ(expectedTime.tm_mday, result->tm_mday);
  EXPECT_EQ(expectedTime.tm_hour, result->tm_hour);
  EXPECT_EQ(expectedTime.tm_min, result->tm_min);
  EXPECT_EQ(expectedTime.tm_sec, result->tm_sec);
  EXPECT_EQ(expectedTime.tm_wday, result->tm_wday);
  EXPECT_EQ(expectedTime.tm_yday, result->tm_yday);
  EXPECT_EQ(expectedTime.tm_isdst, result->tm_isdst);
}
