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

  run("SELECT * FROM (SELECT * FROM t1)");

  run("SELECT LAST(c1) FROM (SELECT * FROM t1)");

  run("SELECT c1 FROM (SELECT TODAY() AS c1 FROM t1)");
}

TEST_F(PlanSubqeuryTest, doubleGroupBy) {
  useDb("root", "test");

  run("SELECT COUNT(*) FROM ("
      "SELECT c1 + c3 a, c1 + COUNT(*) b FROM t1 WHERE c2 = 'abc' GROUP BY c1, c3) "
      "WHERE a > 100 GROUP BY b");
}

TEST_F(PlanSubqeuryTest, withSetOperator) {
  useDb("root", "test");

  run("SELECT c1 FROM (SELECT c1 FROM t1 UNION ALL SELECT c1 FROM t1)");

  run("SELECT c1 FROM (SELECT c1 FROM t1 UNION SELECT c1 FROM t1)");
}
