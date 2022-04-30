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

class PlanIntervalTest : public PlannerTestBase {};

TEST_F(PlanIntervalTest, basic) {
  useDb("root", "test");

  run("SELECT COUNT(*) FROM t1 INTERVAL(10s)");
}

TEST_F(PlanIntervalTest, pseudoCol) {
  useDb("root", "test");

  run("SELECT _WSTARTTS, _WDURATION, _WENDTS, COUNT(*) FROM t1 INTERVAL(10s)");
}

TEST_F(PlanIntervalTest, fill) {
  useDb("root", "test");

  run("SELECT COUNT(*) FROM t1 INTERVAL(10s) FILL(LINEAR)");

  run("SELECT COUNT(*), sum(c1) FROM t1 INTERVAL(10s) FILL(VALUE, 10, 20)");
}
