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

class PlanDistinctTest : public PlannerTestBase {};

TEST_F(PlanDistinctTest, basic) {
  useDb("root", "test");

  // distinct single col
  run("select distinct c1 from t1");
  // distinct col list
  run("select distinct c1, c2 from t1");
}

TEST_F(PlanDistinctTest, expr) {
  useDb("root", "test");

  run("select distinct c1, c2 + 10 from t1");
}

TEST_F(PlanDistinctTest, withOrderBy) {
  useDb("root", "test");

  run("select distinct c1 + 10 a from t1 order by a");
}

TEST_F(PlanDistinctTest, withLimit) {
  useDb("root", "test");

  run("SELECT DISTINCT c1 FROM t1 LIMIT 3");
}
