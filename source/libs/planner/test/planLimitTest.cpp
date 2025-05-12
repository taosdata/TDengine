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

class PlanLimitTest : public PlannerTestBase {};

TEST_F(PlanLimitTest, limit) {
  useDb("root", "test");

  run("select * from t1 limit 2");

  run("select * from t1 limit 5 offset 2");

  run("select * from t1 limit 2, 5");
}

TEST_F(PlanLimitTest, slimit) {
  useDb("root", "test");

  run("select * from t1 partition by c1 slimit 2");

  run("select * from t1 partition by c1 slimit 5 soffset 2");

  run("select * from t1 partition by c1 slimit 2, 5");
}
