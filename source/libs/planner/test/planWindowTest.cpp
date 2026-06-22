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

class PlanTest : public PlannerTestBase {};

TEST_F(PlanTest, sqlWindowRowsPlan) {
  useDb("root", "test");
  run("SELECT ts, avg(c1) OVER (PARTITION BY c2 ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t1");
  ASSERT_TRUE(findPlanNode("WindowFunc"));
  ASSERT_TRUE(findPlanNode("Sort"));
}

TEST_F(PlanTest, sqlWindowSharedSpecPlan) {
  useDb("root", "test");
  run("SELECT avg(c1) OVER win, max(c1) OVER win FROM t1 WINDOW win AS (PARTITION BY c2 ORDER BY ts)");
  ASSERT_EQ(1, countPlanNode("WindowFunc"));
  ASSERT_EQ(1, countPlanNode("Sort"));
}

TEST_F(PlanTest, sqlWindowDifferentSpecPlan) {
  useDb("root", "test");
  run("SELECT avg(c1) OVER (PARTITION BY c2 ORDER BY ts), max(c1) OVER (PARTITION BY c3 ORDER BY ts) FROM t1");
  ASSERT_EQ(2, countPlanNode("WindowFunc"));
}

TEST_F(PlanTest, sqlWindowComplexArgumentPlan) {
  useDb("root", "test");
  run("SELECT avg(c1 + 1) OVER (PARTITION BY c2 ORDER BY ts) FROM t1");
  ASSERT_TRUE(findPlanNode("WindowFunc"));
  ASSERT_TRUE(planContains("\"Exprs\":["));
  ASSERT_TRUE(planContains("\"Name\":\"Operator\""));
  ASSERT_TRUE(planContains("\"Funcs\":["));
  ASSERT_TRUE(planContains("\"Name\":\"Function\""));
  ASSERT_TRUE(planContains("\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\""));
}

TEST_F(PlanTest, sqlWindowStaysAboveExchange) {
  useDb("root", "test");
  run("SELECT avg(c1) OVER (PARTITION BY c2 ORDER BY ts) FROM meters");
  ASSERT_TRUE(planNodeAppearsAbove("WindowFunc", "Exchange"));
  ASSERT_TRUE(planNodeAppearsAbove("Sort", "Exchange"));
  ASSERT_FALSE(exchangeSubplansContain("WindowFunc"));
  ASSERT_FALSE(exchangeSubplansContain("Sort"));
}
