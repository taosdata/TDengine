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

class PlanPartitionByTest : public PlannerTestBase {};

TEST_F(PlanPartitionByTest, basic) {
  useDb("root", "test");

  run("select * from t1 partition by c1");

  run("select ts, c1 + 1 from st1 partition by c1 + 1");

  run("select ts, jtag->'tag1' from st2 partition by jtag->'tag1'");
}

TEST_F(PlanPartitionByTest, withAggFunc) {
  useDb("root", "test");

  run("select count(*) from t1 partition by c1");

  run("select count(*) from st1 partition by c1");

  run("select sample(c1, 2) from st1 partition by c1");

  run("select count(*), c1 from t1 partition by c1");
}

TEST_F(PlanPartitionByTest, withInterval) {
  useDb("root", "test");

  // normal/child table
  run("select count(*) from t1 partition by c1 interval(10s)");

  run("select count(*), c1 from t1 partition by c1 interval(10s)");
  // super table
  run("select count(*) from st1 partition by tag1, tag2 interval(10s)");

  run("select count(*), tag1 from st1 partition by tag1, tag2 interval(10s)");
}

TEST_F(PlanPartitionByTest, withGroupBy) {
  useDb("root", "test");

  run("SELECT COUNT(*) FROM t1 PARTITION BY c1 GROUP BY c2");

  run("SELECT TBNAME, c1 FROM st1 PARTITION BY TBNAME GROUP BY c1");

  run("SELECT COUNT(*) FROM t1 PARTITION BY TBNAME GROUP BY TBNAME");
}

TEST_F(PlanPartitionByTest, withTimeLineFunc) {
  useDb("root", "test");

  run("SELECT TWA(c1) FROM st1 PARTITION BY c1");

  run("SELECT MAVG(c1, 2) FROM st1 PARTITION BY c1");
}

TEST_F(PlanPartitionByTest, withSlimit) {
  useDb("root", "test");

  run("SELECT CSUM(c1) FROM st1 PARTITION BY TBNAME SLIMIT 1");
}
