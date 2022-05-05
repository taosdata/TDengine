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

class PlanOrderByTest : public PlannerTestBase {};

TEST_F(PlanOrderByTest, basic) {
  useDb("root", "test");

  // order by key is in the projection list
  run("select c1 from t1 order by c1");
  // order by key is not in the projection list
  run("select c1 from t1 order by c2");
}

TEST_F(PlanOrderByTest, expr) {
  useDb("root", "test");

  run("select * from t1 order by c1 + 10, c2");
}

TEST_F(PlanOrderByTest, nullsOrder) {
  useDb("root", "test");

  run("select * from t1 order by c1 desc nulls first");
}
