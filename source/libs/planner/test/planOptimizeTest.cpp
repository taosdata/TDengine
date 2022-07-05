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

class PlanOptimizeTest : public PlannerTestBase {};

TEST_F(PlanOptimizeTest, scanPath) {
  useDb("root", "test");

  run("SELECT COUNT(*) FROM t1");

  run("SELECT COUNT(c1) FROM t1");

  run("SELECT COUNT(CAST(c1 AS BIGINT)) FROM t1");

  run("SELECT PERCENTILE(c1, 40), COUNT(*) FROM t1");
}

TEST_F(PlanOptimizeTest, pushDownCondition) {
  useDb("root", "test");

  run("SELECT ts, c1 FROM st1 WHERE tag1 > 4");

  run("SELECT ts, c1 FROM st1 WHERE tag1 > 4 or tag1 < 2");

  run("SELECT ts, c1 FROM st1 WHERE tag1 > 4 AND tag2 = 'hello'");

  run("SELECT ts, c1 FROM st1 WHERE tag1 > 4 AND tag2 = 'hello' AND c1 > 10");

  run("SELECT ts, c1 FROM (SELECT * FROM st1) WHERE tag1 > 4");
}

TEST_F(PlanOptimizeTest, sortPrimaryKey) {
  useDb("root", "test");

  run("SELECT c1 FROM t1 ORDER BY ts");

  run("SELECT c1 FROM t1 ORDER BY ts DESC");

  run("SELECT COUNT(*) FROM t1 INTERVAL(10S) ORDER BY _WSTARTTS DESC");
}

TEST_F(PlanOptimizeTest, PartitionTags) {
  useDb("root", "test");

  run("SELECT c1, tag1 FROM st1 PARTITION BY tag1");

  run("SELECT SUM(c1), tag1 FROM st1 PARTITION BY tag1");

  run("SELECT SUM(c1), tag1 + 10 FROM st1 PARTITION BY tag1 + 10");

  run("SELECT SUM(c1), tag1 FROM st1 GROUP BY tag1");

  run("SELECT SUM(c1), tag1 + 10 FROM st1 GROUP BY tag1 + 10");

  run("SELECT SUM(c1), tbname FROM st1 GROUP BY tbname");
}

TEST_F(PlanOptimizeTest, eliminateProjection) {
  useDb("root", "test");

  run("SELECT c1, sum(c3) FROM t1 GROUP BY c1");
  run("SELECT c1 FROM t1");
  run("SELECT * FROM st1");
  run("SELECT c1 FROM st1s3");
  // run("select 1-abs(c1) from (select unique(c1) c1 from st1s3) order by 1 nulls first");
}
