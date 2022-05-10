/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, AND/or modify
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

class PlanBasicTest : public PlannerTestBase {};

TEST_F(PlanBasicTest, selectClause) {
  useDb("root", "test");

  run("SELECT * FROM t1");
  run("SELECT 1 FROM t1");
  run("SELECT * FROM st1");
  run("SELECT 1 FROM st1");
}

TEST_F(PlanBasicTest, whereClause) {
  useDb("root", "test");

  run("SELECT * FROM t1 WHERE c1 > 10");

  run("SELECT * FROM t1 WHERE ts > TIMESTAMP '2022-04-01 00:00:00' and ts < TIMESTAMP '2022-04-30 23:59:59'");
}

TEST_F(PlanBasicTest, joinClause) {
  useDb("root", "test");

  run("SELECT t1.c1, t2.c2 FROM st1s1 t1, st1s2 t2 WHERE t1.ts = t2.ts");
  run("SELECT t1.c1, t2.c2 FROM st1s1 t1 JOIN st1s2 t2 ON t1.ts = t2.ts");
}

TEST_F(PlanBasicTest, func) {
  useDb("root", "test");

  run("SELECT DIFF(c1) FROM t1");

  run("SELECT PERCENTILE(c1, 60) FROM t1");
}
