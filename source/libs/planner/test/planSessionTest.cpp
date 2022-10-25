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

class PlanSessionTest : public PlannerTestBase {};

TEST_F(PlanSessionTest, basic) {
  useDb("root", "test");

  run("select count(*) from t1 session(ts, 10s)");
}

TEST_F(PlanSessionTest, selectFunc) {
  useDb("root", "test");

  // select function for SESSION clause
  run("SELECT MAX(c1), MIN(c1) FROM t1 SESSION(ts, 10s)");
  // select function along with the columns of select row, and with SESSION clause
  run("SELECT MAX(c1), c2 FROM t1 SESSION(ts, 10s)");
}

TEST_F(PlanSessionTest, stable) {
  useDb("root", "test");

  // select function for SESSION clause
  run("SELECT MAX(c1), MIN(c1) FROM st1 SESSION(ts, 10s)");
  // select function along with the columns of select row, and with SESSION clause
  run("SELECT MAX(c1), c2 FROM st1 SESSION(ts, 10s)");
  run("SELECT count(ts) FROM st1 PARTITION BY c1 SESSION(ts, 10s)");
}
