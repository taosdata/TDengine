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

class PlanSubqeuryTest : public PlannerTestBase {};

TEST_F(PlanSubqeuryTest, basic) {
  useDb("root", "test");

  run("select * from (select * from t1)");
}

TEST_F(PlanSubqeuryTest, doubleGroupBy) {
  useDb("root", "test");

  run("select count(*) from (select c1 + c3 a, c1 + count(*) b from t1 where c2 = 'abc' group by c1, c3) where a > 100 "
      "group by b");
}
