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

  run("SELECT COUNT(*) FROM t1");

  run("SELECT c1, MAX(c3), MIN(c3), COUNT(*) FROM t1 GROUP BY c1");

  run("SELECT c1 + c3, c1 + COUNT(*) FROM t1 WHERE c2 = 'abc' GROUP BY c1, c3");

  run("SELECT c1 + c3, SUM(c4 * c5) FROM t1 WHERE CONCAT(c2, 'wwww') = 'abcwww' GROUP BY c1 + c3");

  run("SELECT SUM(CEIL(c1)) FROM t1 GROUP BY CEIL(c1)");

  run("SELECT COUNT(*) FROM st1");

  run("SELECT c1 FROM st1 GROUP BY c1");

  run("SELECT COUNT(*) FROM st1 GROUP BY c1");

  run("SELECT SUM(c1) FROM st1 GROUP BY c2 HAVING SUM(c1) IS NOT NULL");

  run("SELECT AVG(c1) FROM st1");
}

TEST_F(PlanGroupByTest, withPartitionBy) {
  useDb("root", "test");

  run("SELECT LAST(ts), TBNAME FROM st1 PARTITION BY TBNAME");

  run("SELECT COUNT(*) FROM st1 PARTITION BY c2 GROUP BY c1");
}

TEST_F(PlanGroupByTest, withOrderBy) {
  useDb("root", "test");

  // ORDER BY aggfunc
  run("SELECT COUNT(*), SUM(c1) FROM t1 ORDER BY SUM(c1)");
  // ORDER BY alias of aggfunc
  // run("SELECT COUNT(*), SUM(c1) a FROM t1 ORDER BY a");
}

TEST_F(PlanGroupByTest, multiResFunc) {
  useDb("root", "test");

  run("SELECT LAST(*), FIRST(*) FROM t1");

  run("SELECT LAST(*), FIRST(*) FROM t1 GROUP BY c1");
}

TEST_F(PlanGroupByTest, selectFunc) {
  useDb("root", "test");

  // select function
  run("SELECT MAX(c1), MIN(c1) FROM t1");
  // select function for GROUP BY clause
  run("SELECT MAX(c1), MIN(c1) FROM t1 GROUP BY c1");
  // select function along with the columns of select row
  run("SELECT MAX(c1), c2 FROM t1");
  run("SELECT MAX(c1), t1.* FROM t1");
  // select function along with the columns of select row, and with GROUP BY clause
  run("SELECT MAX(c1), c2 FROM t1 GROUP BY c3");
  run("SELECT MAX(c1), t1.* FROM t1 GROUP BY c3");
}
