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
#include "tglobal.h"

using namespace std;

class PlanOtherTest : public PlannerTestBase {};

TEST_F(PlanOtherTest, createTopic) {
  useDb("root", "test");

  run("create topic tp as SELECT * FROM st1");
}

TEST_F(PlanOtherTest, createStream) {
  useDb("root", "test");

  run("create stream if not exists s1 trigger window_close watermark 10s into st3 as select count(*) from t1 "
      "interval(10s)");

  run("CREATE STREAM s1 INTO st3 TAGS(tname VARCHAR(10), id INT) SUBTABLE(CONCAT('new-', tname)) "
      "AS SELECT _WSTART wstart, COUNT(*) cnt FROM st1 PARTITION BY TBNAME tname, c1 id INTERVAL(10S)");

  run("CREATE STREAM s1 INTO st3 TAGS(tname VARCHAR(10), id INT) SUBTABLE(CONCAT('new-', tname)) "
      "AS SELECT _WSTART wstart, COUNT(*) cnt FROM st1 PARTITION BY TBNAME tname, tag1 id INTERVAL(10S)");
}

TEST_F(PlanOtherTest, createStreamUseSTable) {
  useDb("root", "test");

  run("CREATE STREAM IF NOT EXISTS s1 into st3 as SELECT COUNT(*) FROM st1 INTERVAL(10s)");

  run("CREATE STREAM IF NOT EXISTS s1 into st3 as SELECT COUNT(*) FROM st1 PARTITION BY TBNAME INTERVAL(10s)");
}

TEST_F(PlanOtherTest, createSmaIndex) {
  useDb("root", "test");

  run("CREATE SMA INDEX idx1 ON t1 FUNCTION(MAX(c1), MIN(c3 + 10), SUM(c4)) INTERVAL(10s) DELETE_MARK 1000s");

  run("SELECT SUM(c4) FROM t1 INTERVAL(10s)");

  run("SELECT _WSTART, MIN(c3 + 10) FROM t1 "
      "WHERE ts BETWEEN TIMESTAMP '2022-04-01 00:00:00' AND TIMESTAMP '2022-04-30 23:59:59.999' INTERVAL(10s)");

  run("SELECT SUM(c4), MAX(c3) FROM t1 INTERVAL(10s)");

  tsQuerySmaOptimize = 0;
  run("SELECT SUM(c4) FROM t1 INTERVAL(10s)");
}

TEST_F(PlanOtherTest, explain) {
  useDb("root", "test");

  run("explain SELECT * FROM t1");

  run("explain analyze SELECT * FROM t1");

  run("explain analyze verbose true ratio 0.01 SELECT * FROM t1");
}

TEST_F(PlanOtherTest, show) {
  useDb("root", "test");

  run("SHOW DATABASES");

  run("SHOW TABLE DISTRIBUTED t1");

  run("SHOW TABLE DISTRIBUTED st1");

  run("SHOW DNODE 1 VARIABLES");

  run("SHOW DNODE 1 VARIABLES LIKE '%debug%'");

  run("SHOW TAGS FROM st1s1");

  run("SHOW TABLE TAGS FROM st1");
}

TEST_F(PlanOtherTest, delete) {
  useDb("root", "test");

  run("DELETE FROM t1");

  run("DELETE FROM t1 WHERE ts > now - 2d and ts < now - 1d");

  run("DELETE FROM st1");

  run("DELETE FROM st1 WHERE ts > now - 2d and ts < now - 1d AND tag1 = 10");
}

TEST_F(PlanOtherTest, insert) {
  useDb("root", "test");

  run("INSERT INTO t1 SELECT * FROM t1");

  run("INSERT INTO t1 (ts, c1, c2) SELECT ts, c1, c2 FROM st1");

  run("INSERT INTO t1 (ts, c1, c2) SELECT ts, c1, c2 FROM st1s1 UNION ALL SELECT ts, c1, c2 FROM st2");
}
