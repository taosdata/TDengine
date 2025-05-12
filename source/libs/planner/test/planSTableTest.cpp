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

class PlanSuperTableTest : public PlannerTestBase {};

TEST_F(PlanSuperTableTest, pseudoCol) {
  useDb("root", "test");

  run("SELECT TBNAME FROM st1");

  run("SELECT TBNAME, tag1, tag2 FROM st1");
}

TEST_F(PlanSuperTableTest, pseudoColOnChildTable) {
  useDb("root", "test");

  run("SELECT TBNAME FROM st1s1");

  run("SELECT TBNAME, tag1, tag2 FROM st1s1");
}

TEST_F(PlanSuperTableTest, orderBy) {
  useDb("root", "test");

  run("SELECT -1 * c1, c1 FROM st1 ORDER BY -1 * c1");
}
