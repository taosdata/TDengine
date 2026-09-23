/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

TEST_F(PlanPartitionByTest, implicitHavingPlanShape) {
  useDb("root", "test");

  run("SELECT c1 FROM t1 HAVING c1 > 0");
  EXPECT_EQ(1, countRawLogicPlanNode("Partition"));
  EXPECT_EQ(0, countRawLogicPlanNode("Agg"));
  EXPECT_TRUE(rawLogicPlanContains(IMPLICIT_PARTITION_KEY_ALIAS));

  run("SELECT COUNT(*) FROM st1 PARTITION BY TBNAME");
  EXPECT_TRUE(rawLogicPlanHasPartTableAgg());

  run("SELECT COUNT(*) FROM t1 HAVING COUNT(*) > 0");
  EXPECT_EQ(1, countRawLogicPlanNode("Partition"));
  EXPECT_EQ(1, countRawLogicPlanNode("Agg"));
  EXPECT_TRUE(rawLogicPlanContains(IMPLICIT_PARTITION_KEY_ALIAS));
  EXPECT_FALSE(rawLogicPlanHasPartTableAgg());
  EXPECT_FALSE(rawLogicPlanHasPartTagAgg());
}

TEST_F(PlanPartitionByTest, implicitHavingLastKeepsNullGroup) {
  useDb("root", "test");

  run("SELECT LAST(c1) FROM t1 WHERE c2 > 0 "
      "HAVING LAST(c1) IS NULL");
  const string isNotNullOp =
      "\"OpType\":\"" + to_string(OP_TYPE_IS_NOT_NULL) + "\"";
  EXPECT_FALSE(planContains(isNotNullOp));
}
