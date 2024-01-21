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
#include "taos.h"
#include "thash.h"
#include "tsimplehash.h"
#include "executor.h"
#include "executorInt.h"
#include "ttime.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

namespace {
SInterval createInterval(int64_t interval, int64_t sliding, int64_t offset, char intervalUnit, char slidingUnit,
                         char offsetUnit, int8_t precision) {
  SInterval v = {0};
  v.interval = interval;
  v.intervalUnit = intervalUnit;
  v.sliding = sliding;
  v.slidingUnit = slidingUnit;
  v.offset = offset;
  v.offsetUnit = offsetUnit;
  v.precision = precision;
  return v;
}

void printTimeWindow(STimeWindow* pWindow, int8_t precision, int64_t ts) {
  char buf[64] = {0};
  char bufs[64] = {0};
  char bufe[64] = {0};

  taosFormatUtcTime(buf, tListLen(buf), ts, precision);

  taosFormatUtcTime(bufs, tListLen(bufs), pWindow->skey, precision);
  taosFormatUtcTime(bufe, tListLen(bufe), pWindow->ekey, precision);

  printf("%s [%s - %s]\n", buf, bufs, bufe);
}
}  // namespace

TEST(testCase, timewindow_gen) {
  // set correct time zone
  osSetTimezone("UTC");
  int32_t precision = TSDB_TIME_PRECISION_MILLI;
  
  SInterval interval =
      createInterval(10 * 86400 * 1000, 10 * 86400 * 1000, 0, 'd', 'd', 'd', precision);

  int64_t key = 1659312000L * 1000;  // 2022-8-1 00:00:00 // UTC+8  (ms)

  STimeWindow w = {0};
  getInitialStartTimeWindow(&interval, key, &w, true);
  printTimeWindow(&w, precision, key);

  getNextTimeWindow(&interval, &w, TSDB_ORDER_ASC);
  printf("next\n");
  printTimeWindow(&w, precision, key);

  printf("---------------------------------------------------\n");
  SInterval monthInterval =
      createInterval(1, 1, 0, 'n', 'n', 'd', TSDB_TIME_PRECISION_MILLI);
  getInitialStartTimeWindow(&monthInterval, key, &w, true);
  printTimeWindow(&w, precision, key);

  getNextTimeWindow(&monthInterval, &w, TSDB_ORDER_ASC);
  printf("next\n");
  printTimeWindow(&w, precision, key);

  printf("----------------------------------------------------------\n");
  SInterval slidingInterval = createInterval(1, 10*86400*1000, 0, 'n', 'd', 'd', TSDB_TIME_PRECISION_MILLI);
  getInitialStartTimeWindow(&slidingInterval, key, &w, true);
  printTimeWindow(&w, precision, key);

  getNextTimeWindow(&slidingInterval, &w, TSDB_ORDER_ASC);
  printf("next\n");
  printTimeWindow(&w, precision, key);

  getNextTimeWindow(&slidingInterval, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, key);

  getNextTimeWindow(&slidingInterval, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, key);

  getNextTimeWindow(&slidingInterval, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, key);

  getNextTimeWindow(&slidingInterval, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, key);

  getNextTimeWindow(&slidingInterval, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, key);

  getNextTimeWindow(&slidingInterval, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, key);

  getNextTimeWindow(&slidingInterval, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, key);

  printf("-----------------calendar_interval_1n_sliding_1d-------\n");
  SInterval calendar_interval_1n = createInterval(1, 1*86400*1000, 0, 'n', 'd', 'd', TSDB_TIME_PRECISION_MILLI);
  int64_t k1 = 1664409600 * 1000L;
  getInitialStartTimeWindow(&calendar_interval_1n, k1, &w, true);
  printTimeWindow(&w, precision, k1);

  printf("next\n");

  getNextTimeWindow(&calendar_interval_1n, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, key);

  getNextTimeWindow(&calendar_interval_1n, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, key);

  getNextTimeWindow(&calendar_interval_1n, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, key);

  getNextTimeWindow(&calendar_interval_1n, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, key);

  printf("----------------interval_1d_clendar_sliding_1n---------\n");
  SInterval interval_1d_calendar_sliding_1n = createInterval(1*86400*1000L, 1, 0, 'd', 'n', 'd', TSDB_TIME_PRECISION_MILLI);

  k1 = 1664409600 * 1000L;
  getInitialStartTimeWindow(&interval_1d_calendar_sliding_1n, k1, &w, true);
  printTimeWindow(&w, precision, k1);

  printf("next time window:\n");
  getNextTimeWindow(&interval_1d_calendar_sliding_1n, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, k1);

  getNextTimeWindow(&interval_1d_calendar_sliding_1n, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, k1);

  getNextTimeWindow(&interval_1d_calendar_sliding_1n, &w, TSDB_ORDER_ASC);
  printTimeWindow(&w, precision, k1);

  printf("----------------interval_1d_sliding_1d_calendar_offset_1n---------\n");
  SInterval offset_1n = createInterval(10*86400*1000L, 10*86400*1000L, 1, 'd', 'd', 'n', TSDB_TIME_PRECISION_MILLI);
    getInitialStartTimeWindow(&offset_1n, k1, &w, true);
    printTimeWindow(&w, precision, k1);


}

TEST(testCase, timewindow_natural) {
  osSetTimezone("CST");

  int32_t precision = TSDB_TIME_PRECISION_MILLI;

  SInterval interval2 = createInterval(17, 17, 13392000000, 'n', 'n', 0, precision);
  int64_t key = 1648970865984;
  STimeWindow w0 = getAlignQueryTimeWindow(&interval2, key);
  printTimeWindow(&w0, precision, key);
  ASSERT_GE(w0.ekey, key);

  int64_t key1 = 1633446027072;
  STimeWindow w1 = {0};
  getInitialStartTimeWindow(&interval2, key1, &w1, true);
  printTimeWindow(&w1, precision, key1);
  STimeWindow w3 = getAlignQueryTimeWindow(&interval2, key1);
  printf("%ld win %ld, %ld\n", key1, w3.skey, w3.ekey);  

  int64_t key2 = 1648758398208;
  STimeWindow w2 = {0};
  getInitialStartTimeWindow(&interval2, key2, &w2, true);
  printTimeWindow(&w2, precision, key2);
  STimeWindow w4 = getAlignQueryTimeWindow(&interval2, key2);
  printf("%ld win %ld, %ld\n", key2, w3.skey, w3.ekey);

  ASSERT_EQ(w3.skey, w4.skey);
  ASSERT_EQ(w3.ekey, w4.ekey);  
}


TEST(testCase, timewindow_active) {
  osSetTimezone("CST");
  int32_t precision = TSDB_TIME_PRECISION_MILLI;
  int64_t offset = (int64_t)2*365*24*60*60*1000;
  SInterval interval = createInterval(10, 10, offset, 'y', 'y', 0, precision);
  SResultRowInfo dumyInfo = {0};
  dumyInfo.cur.pageId = -1;
  int64_t key = (int64_t)1609430400*1000; // 2021-01-01
  STimeWindow win = getActiveTimeWindow(NULL, &dumyInfo, key, &interval, TSDB_ORDER_ASC);
  printTimeWindow(&win, precision, key);
  printf("%ld win %ld, %ld\n", key, win.skey, win.ekey);
  ASSERT_EQ(win.skey, 1325376000000);
  ASSERT_EQ(win.ekey, 1640908799999);
}
#pragma GCC diagnostic pop