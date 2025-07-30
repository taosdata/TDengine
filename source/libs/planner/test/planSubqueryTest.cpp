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

  run("SELECT NOW() FROM t1");

  run("SELECT NOW() FROM (SELECT * FROM t1)");

  run("SELECT * FROM (SELECT NOW() FROM t1)");

  run("SELECT NOW() FROM (SELECT * FROM t1) ORDER BY ts");

  run("SELECT * FROM (SELECT AVG(c1) a FROM st1 INTERVAL(10s)) WHERE a > 1");
}

TEST_F(PlanSubqeuryTest, doubleGroupBy) {
  useDb("root", "test");

  run("SELECT COUNT(*) FROM ("
      "SELECT c1 + c3 a, c1 + COUNT(*) b FROM t1 WHERE c2 = 'abc' GROUP BY c1, c3) "
      "WHERE a > 100 GROUP BY b");
}

TEST_F(PlanSubqeuryTest, innerSetOperator) {
  useDb("root", "test");

  run("SELECT c1 FROM (SELECT c1 FROM t1 UNION ALL SELECT c1 FROM t1)");

  run("SELECT c1 FROM (SELECT c1 FROM t1 UNION SELECT c1 FROM t1)");
}

TEST_F(PlanSubqeuryTest, innerFill) {
  useDb("root", "test");

  run("SELECT cnt FROM (SELECT _WSTART ts, COUNT(*) cnt FROM t1 "
      "WHERE ts > '2022-04-01 00:00:00' and ts < '2022-04-30 23:59:59' INTERVAL(10s) FILL(LINEAR)) "
      "WHERE ts > '2022-04-06 00:00:00'");
}

TEST_F(PlanSubqeuryTest, innerOrderBy) {
  useDb("root", "test");

  run("SELECT c2 FROM (SELECT c2 FROM st1 ORDER BY c1, _rowts)");
}

TEST_F(PlanSubqeuryTest, outerInterval) {
  useDb("root", "test");

  run("SELECT COUNT(*) FROM (SELECT * FROM st1) INTERVAL(5s)");

  run("SELECT COUNT(*) + SUM(c1) FROM (SELECT * FROM st1) INTERVAL(5s)");

  run("SELECT COUNT(*) FROM (SELECT ts, TOP(c1, 10) FROM st1s1) INTERVAL(5s)");
}

TEST_F(PlanSubqeuryTest, outerPartition) {
  useDb("root", "test");

  run("SELECT c1, COUNT(*) FROM (SELECT ts, c1 FROM st1) PARTITION BY c1");
}
