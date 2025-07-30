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

  run("SELECT _WSTART, _WDURATION, _WEND, COUNT(*) FROM t1 INTERVAL(10s)");
}

TEST_F(PlanIntervalTest, fill) {
  useDb("root", "test");

  run("SELECT COUNT(*) FROM t1 WHERE ts > TIMESTAMP '2022-04-01 00:00:00' and ts < TIMESTAMP '2022-04-30 23:59:59' "
      "INTERVAL(10s) FILL(LINEAR)");

  run("SELECT COUNT(*) FROM st1 WHERE ts > TIMESTAMP '2022-04-01 00:00:00' and ts < TIMESTAMP '2022-04-30 23:59:59' "
      "INTERVAL(10s) FILL(LINEAR)");

  run("SELECT COUNT(*), SUM(c1) FROM t1 "
      "WHERE ts > TIMESTAMP '2022-04-01 00:00:00' and ts < TIMESTAMP '2022-04-30 23:59:59' "
      "INTERVAL(10s) FILL(VALUE, 10, 20)");

  run("SELECT _WSTART, TBNAME, COUNT(*) FROM st1 "
      "WHERE ts > '2022-04-01 00:00:00' and ts < '2022-04-30 23:59:59' "
      "PARTITION BY TBNAME INTERVAL(10s) FILL(PREV)");

  run("SELECT COUNT(c1), MAX(c3), COUNT(c1) FROM t1 "
      "WHERE ts > '2022-04-01 00:00:00' and ts < '2022-04-30 23:59:59' INTERVAL(10s) FILL(PREV)");

  run("SELECT COUNT(c1) FROM t1 WHERE ts > '2022-04-01 00:00:00' and ts < '2022-04-30 23:59:59' "
      "PARTITION BY c2 INTERVAL(10s) FILL(PREV) ORDER BY c2");
}

TEST_F(PlanIntervalTest, selectFunc) {
  useDb("root", "test");

  // select function for INTERVAL clause
  run("SELECT MAX(c1), MIN(c1) FROM t1 INTERVAL(10s)");
  // select function along with the columns of select row, and with INTERVAL clause
  run("SELECT MAX(c1), c2 FROM t1 INTERVAL(10s)");

  run("SELECT TOP(c1, 1) FROM t1 INTERVAL(10s) ORDER BY c1");
}

TEST_F(PlanIntervalTest, stable) {
  useDb("root", "test");

  run("SELECT COUNT(*) FROM st1 INTERVAL(10s)");

  run("SELECT _WSTART, COUNT(*) FROM st1 INTERVAL(10s)");

  run("SELECT _WSTART, COUNT(*) FROM st1 PARTITION BY TBNAME INTERVAL(10s)");

  run("SELECT TBNAME, COUNT(*) FROM st1 PARTITION BY TBNAME INTERVAL(10s)");
}
