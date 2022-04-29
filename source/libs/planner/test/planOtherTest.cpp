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

#include "planTestUtil.h"
#include "planner.h"

using namespace std;

class PlanOtherTest : public PlannerTestBase {};

TEST_F(PlanOtherTest, createTopic) {
  useDb("root", "test");

  run("create topic tp as SELECT * FROM st1");
}

TEST_F(PlanOtherTest, createStream) {
  useDb("root", "test");

  run("create stream if not exists s1 trigger window_close watermark 10s into st1 as select count(*) from t1 "
      "interval(10s)");
}

TEST_F(PlanOtherTest, createSmaIndex) {
  useDb("root", "test");

  run("create sma index index1 on t1 function(max(c1), min(c3 + 10), sum(c4)) interval(10s)");
}

TEST_F(PlanOtherTest, explain) {
  useDb("root", "test");

  run("explain SELECT * FROM t1");

  run("explain analyze SELECT * FROM t1");

  run("explain analyze verbose true ratio 0.01 SELECT * FROM t1");
}