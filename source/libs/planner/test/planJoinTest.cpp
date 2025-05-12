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

  run("SELECT t1.c1, t2.c2 FROM st1s1 t1, st1s2 t2 WHERE t1.ts = t2.ts");

  run("SELECT t1.*, t2.* FROM st1s1 t1, st1s2 t2 WHERE t1.ts = t2.ts");

  run("SELECT t1.c1, t2.c1 FROM st1s1 t1 JOIN st1s2 t2 ON t1.ts = t2.ts");

  run("SELECT t1.c1, t2.c1 FROM st1 t1 JOIN st2 t2 ON t1.ts = t2.ts");
}

TEST_F(PlanJoinTest, complex) {
  useDb("root", "test");

  run("SELECT t1.c1, t2.c2 FROM st1s1 t1, st1s2 t2 "
      "WHERE t1.ts = t2.ts AND t1.c1 BETWEEN -10 AND 10 AND t2.c1 BETWEEN -100 AND 100 AND "
      "(t1.c2 LIKE 'nchar%' OR t1.c1 = 0 OR t2.c2 LIKE 'nchar%' OR t2.c1 = 0)");
}

TEST_F(PlanJoinTest, withWhere) {
  useDb("root", "test");

  run("SELECT t1.c1, t2.c1 FROM st1s1 t1 JOIN st1s2 t2 ON t1.ts = t2.ts "
      "WHERE t1.c1 > t2.c1 AND t1.c2 = 'abc' AND t2.c2 = 'qwe'");
}

TEST_F(PlanJoinTest, withAggAndOrderBy) {
  useDb("root", "test");

  run("SELECT t1.ts, TOP(t2.c1, 10) FROM st1s1 t1 JOIN st1s2 t2 ON t1.ts = t2.ts ORDER BY t2.ts");
}

TEST_F(PlanJoinTest, multiJoin) {
  useDb("root", "test");

  run("SELECT t1.c1, t2.c1 FROM st1s1 t1 JOIN st1s2 t2 ON t1.ts = t2.ts JOIN st1s3 t3 ON t1.ts = t3.ts");
}
