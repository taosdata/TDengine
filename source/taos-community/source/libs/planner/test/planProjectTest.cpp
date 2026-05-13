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

using namespace std;

class PlanProjectTest : public PlannerTestBase {};

TEST_F(PlanProjectTest, basic) {
  useDb("root", "test");

  run("SELECT CEIL(c1) FROM t1");
}

TEST_F(PlanProjectTest, indefiniteRowsFunc) {
  useDb("root", "test");

  run("SELECT MAVG(c1, 10) FROM t1");

  run("SELECT MAVG(CEIL(c1), 20) + 2 FROM t1");
}
