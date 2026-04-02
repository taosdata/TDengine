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

TEST_F(PlanSysTableTest, informationSchema) {
  useDb("root", "information_schema");

  run("SELECT * FROM information_schema.ins_databases WHERE name = 'information_schema'");
}

TEST_F(PlanSysTableTest, withAgg) {
  useDb("root", "information_schema");

  run("SELECT COUNT(1) FROM ins_users");
}

TEST_F(PlanSysTableTest, tableCount) {
  useDb("root", "information_schema");

  run("SELECT COUNT(*) FROM ins_tables");

  run("SELECT COUNT(*) FROM ins_tables WHERE db_name = 'test'");

  run("SELECT COUNT(*) FROM ins_tables WHERE db_name = 'test' AND stable_name = 'st1'");

  run("SELECT db_name, COUNT(*) FROM ins_tables GROUP BY db_name");

  run("SELECT db_name, stable_name, COUNT(*) FROM ins_tables GROUP BY db_name, stable_name");
}
