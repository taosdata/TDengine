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
#include "clientInt.h"
#include "clientMonitor.h"
#include "taoserror.h"
#include "tglobal.h"
#include "thash.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "executor.h"
#include "taos.h"

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(clientMonitorTest, monitorTest) {
  const char* cluster1 = "cluster1";
  const char* cluster2 = "cluster2";
  SEpSet epSet;
  clusterMonitorInit(cluster1, epSet, NULL);
  const char* counterName1 = "slow_query";
  const char* counterName2 = "select_count";
  const char* help1 = "test for slowQuery";
  const char* help2 = "test for selectSQL";
  const char* lables[] = {"lable1"};
  taos_counter_t* c1 = createClusterCounter(cluster1, counterName1, help1, 1, lables);
  ASSERT_TRUE(c1 != NULL);
  taos_counter_t* c2 = createClusterCounter(cluster1, counterName2, help2, 1, lables);
  ASSERT_TRUE(c2 != NULL);
  ASSERT_TRUE(c1 != c2);
  taos_counter_t* c21 = createClusterCounter(cluster2, counterName1, help2, 1, lables);
  ASSERT_TRUE(c21 == NULL);
  clusterMonitorInit(cluster2, epSet, NULL);
  c21 = createClusterCounter(cluster2, counterName1, help2, 1, lables);
  ASSERT_TRUE(c21 != NULL);
  int i = 0;
  while (i < 12) {
      taosMsleep(10);
      ++i;
  }
  clusterMonitorClose(cluster1);
  clusterMonitorClose(cluster2);
}

TEST(clientMonitorTest, sendTest) {
  TAOS*       taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  ASSERT_TRUE(taos != NULL);
  printf("connect taosd sucessfully.\n");

  int64_t rid = *(int64_t *)taos;
  SlowQueryLog(rid, false, -1, 1000);
  int i = 0;
  while (i < 20) {
    SlowQueryLog(rid, false, 0, i * 1000);
    taosMsleep(10);
    ++i;
  }

  taos_close(taos);
}
