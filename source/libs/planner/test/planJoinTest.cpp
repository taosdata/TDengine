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

class PlanJoinTest : public PlannerTestBase {};

TEST_F(PlanJoinTest, basic) {
  useDb("root", "test");

  run("select t1.c1, t2.c2 from st1s1 t1, st1s2 t2 where t1.ts = t2.ts");

  run("select t1.*, t2.* from st1s1 t1, st1s2 t2 where t1.ts = t2.ts");

  // run("select t1.c1, t2.c1 from st1s1 t1 join st1s2 t2 on t1.ts = t2.ts where t1.c1 > t2.c1 and t1.c2 = 'abc' and "
  //     "t2.c2 = 'qwe'");
}
