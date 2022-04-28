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

class PlanGroupByTest : public PlannerTestBase {};

TEST_F(PlanGroupByTest, basic) {
  useDb("root", "test");

  run("select count(*) from t1");

  run("select c1, max(c3), min(c3), count(*) from t1 group by c1");

  run("select c1 + c3, c1 + count(*) from t1 where c2 = 'abc' group by c1, c3");

  run("select c1 + c3, sum(c4 * c5) from t1 where concat(c2, 'wwww') = 'abcwww' group by c1 + c3");

  run("select sum(ceil(c1)) from t1 group by ceil(c1)");
}

TEST_F(PlanGroupByTest, withOrderBy) {
  useDb("root", "test");

  // order by aggfunc
  run("select count(*), sum(c1) from t1 order by sum(c1)");
  // order by alias of aggfunc
  // run("select count(*), sum(c1) a from t1 order by a");
}
