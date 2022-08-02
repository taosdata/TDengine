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

class PlanSysTableTest : public PlannerTestBase {};

TEST_F(PlanSysTableTest, show) {
  useDb("root", "test");

  run("show tables");
  run("show stables");
}

TEST_F(PlanSysTableTest, informationSchema) {
  useDb("root", "information_schema");

  run("SELECT * FROM information_schema.ins_databases WHERE name = 'information_schema'");
}
