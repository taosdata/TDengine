/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, AND/or modify
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

class PlanBasicTest : public PlannerTestBase {};

TEST_F(PlanBasicTest, selectClause) {
  useDb("root", "test");

  run("SELECT * FROM t1");

  run("SELECT MAX(c1) c2, c2 FROM t1");

  run("SELECT MAX(c1) c2, c2 FROM st1");
}

TEST_F(PlanBasicTest, whereClause) {
  useDb("root", "test");

  run("SELECT * FROM t1 WHERE c1 > 10");

  run("SELECT * FROM t1 WHERE ts > TIMESTAMP '2022-04-01 00:00:00' and ts < TIMESTAMP '2022-04-30 23:59:59'");

  run("SELECT ts, c1 FROM t1 WHERE ts > NOW AND ts IS NULL AND (c1 > 0 OR c3 < 20)");
}

TEST_F(PlanBasicTest, caseWhen) {
  useDb("root", "test");

  run("SELECT CASE WHEN ts > '2020-1-1 10:10:10' THEN c1 + 10 ELSE c1 - 10 END FROM t1 "
      "WHERE CASE c1 WHEN c2 + 20 THEN c4 - 1 WHEN c2 + 10 THEN c4 - 2 ELSE 10 END > 0");
}

TEST_F(PlanBasicTest, func) {
  useDb("root", "test");

  run("SELECT DIFF(c1) FROM t1");

  run("SELECT PERCENTILE(c1, 60) FROM t1");

  run("SELECT TOP(c1, 60) FROM t1");

  run("SELECT TOP(c1, 60) FROM st1");
}

TEST_F(PlanBasicTest, uniqueFunc) {
  useDb("root", "test");

  run("SELECT UNIQUE(c1) FROM t1");

  run("SELECT UNIQUE(c2 + 10) FROM t1 WHERE c1 > 10");

  run("SELECT UNIQUE(c2 + 10), c2 FROM t1 WHERE c1 > 10");

  run("SELECT UNIQUE(c2 + 10), ts, c2 FROM t1 WHERE c1 > 10");

  run("SELECT UNIQUE(c1) a FROM t1 ORDER BY a");

  run("SELECT ts, UNIQUE(c1) FROM st1 PARTITION BY TBNAME");

  run("SELECT TBNAME, UNIQUE(c1) FROM st1 PARTITION BY TBNAME");
}

TEST_F(PlanBasicTest, tailFunc) {
  useDb("root", "test");

  run("SELECT TAIL(c1, 10) FROM t1");

  run("SELECT TAIL(c2 + 10, 10, 80) FROM t1 WHERE c1 > 10");

  run("SELECT TAIL(c2 + 10, 10, 80) FROM t1 WHERE c1 > 10 PARTITION BY c1");

  run("SELECT TAIL(c2 + 10, 10, 80) FROM t1 WHERE c1 > 10 ORDER BY 1");

  run("SELECT TAIL(c2 + 10, 10, 80) FROM t1 WHERE c1 > 10 LIMIT 5");

  run("SELECT TAIL(c2 + 10, 10, 80) FROM t1 WHERE c1 > 10 PARTITION BY c1 LIMIT 5");

  run("SELECT TAIL(c1, 2, 1) FROM st1s1 UNION ALL SELECT c1 FROM st1s2");

  run("SELECT TAIL(c1, 1) FROM st2 WHERE jtag->'tag1' > 10");
}

TEST_F(PlanBasicTest, interpFunc) {
  useDb("root", "test");

  run("SELECT INTERP(c1) FROM t1 RANGE('2017-7-14 18:00:00', '2017-7-14 19:00:00') EVERY(5s) FILL(LINEAR)");

  run("SELECT _IROWTS, INTERP(c1) FROM t1 RANGE('2017-7-14 18:00:00', '2017-7-14 19:00:00') EVERY(5s) FILL(LINEAR)");

  run("SELECT _IROWTS, INTERP(c1), _ISFILLED FROM t1 RANGE('2017-7-14 18:00:00', '2017-7-14 19:00:00') EVERY(5s) FILL(LINEAR)");

  run("SELECT TBNAME, _IROWTS, INTERP(c1) FROM t1 PARTITION BY TBNAME "
      "RANGE('2017-7-14 18:00:00', '2017-7-14 19:00:00') EVERY(5s) FILL(LINEAR)");
}

TEST_F(PlanBasicTest, lastRowFuncWithoutCache) {
  useDb("root", "test");

  run("SELECT LAST_ROW(c1) FROM t1");

  run("SELECT LAST_ROW(*) FROM t1");

  run("SELECT LAST_ROW(c1, c2) FROM t1");

  run("SELECT LAST_ROW(c1), c2 FROM t1");

  run("SELECT LAST_ROW(c1) FROM st1");

  run("SELECT LAST_ROW(c1) FROM st1 PARTITION BY TBNAME");

  run("SELECT LAST_ROW(c1), SUM(c3) FROM t1");
}

TEST_F(PlanBasicTest, timeLineFunc) {
  useDb("root", "test");

  run("SELECT CSUM(c1) FROM t1");

  run("SELECT CSUM(c1) FROM st1");

  run("SELECT TWA(c1) FROM t1");

  run("SELECT TWA(c1) FROM st1");
}

TEST_F(PlanBasicTest, multiResFunc) {
  useDb("root", "test");

  run("SELECT LAST(*) FROM t1");

  run("SELECT LAST(c1 + 10, c2) FROM st1");
}

TEST_F(PlanBasicTest, sampleFunc) {
  useDb("root", "test");

  run("SELECT SAMPLE(c1, 10) FROM t1");

  run("SELECT SAMPLE(c1, 10) FROM st1");

  run("SELECT SAMPLE(c1, 10) FROM st1 PARTITION BY TBNAME");
}

TEST_F(PlanBasicTest, pseudoColumn) {
  useDb("root", "test");

  run("SELECT _QSTART, _QEND, _QDURATION FROM t1");

  run("SELECT _QSTART, _QEND, _QDURATION FROM t1 WHERE ts BETWEEN '2017-7-14 18:00:00' AND '2017-7-14 19:00:00'");

  run("SELECT _QSTART, _QEND, _QDURATION, _WSTART, _WEND, _WDURATION, COUNT(*) FROM t1 "
      "WHERE ts BETWEEN '2017-7-14 18:00:00' AND '2017-7-14 19:00:00' INTERVAL(10S)");

  run("SELECT _TAGS, * FROM st1s1");
}

TEST_F(PlanBasicTest, indefiniteRowsFunc) {
  useDb("root", "test");

  run("SELECT DIFF(c1) FROM t1");

  run("SELECT DIFF(c1), c2 FROM t1");

  run("SELECT DIFF(c1), DIFF(c3), ts FROM t1");
}

TEST_F(PlanBasicTest, withoutFrom) {
  useDb("root", "test");

  run("SELECT 1");
}
