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

TEST(osSleepTests, taosUsleep) {
  const int sleep_time1 = 1000;  // sleep for 1000 microseconds
  taosUsleep(sleep_time1);

  const int sleep_time2 = 0;  // sleep for 0 microseconds
  taosUsleep(sleep_time2);

  const int sleep_time3 = -1;  // sleep for negative time
  taosUsleep(sleep_time3);
}

TEST(osSleepTests, taosSsleep) {
  const int sleep_time1 = 1;  
  taosSsleep(sleep_time1);

  const int sleep_time2 = 0; 
  taosSsleep(sleep_time2);

  const int sleep_time3 = -1;
  taosSsleep(sleep_time3);
}

TEST(osSleepTests, taosMsleep) {
  const int sleep_time1 = 1000;
  taosMsleep(sleep_time1);

  const int sleep_time2 = 0;
  taosMsleep(sleep_time2);

  const int sleep_time3 = -1;  // sleep for negative time
  taosMsleep(sleep_time3);
}
