/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

TEST_F(PlanIntervalTest, smallDataScanSortHint) {
  useDb("root", "test");

  // With the hint, a super-table INTERVAL query has no ORDER BY / Sort node, so the
  // would-be Table Merge Scan must be replaced by a plain Table Scan plus an inserted
  // Sort that provides the ordered input the window requires.
  run("SELECT /*+ smalldata_scan_sort() */ _WSTART, COUNT(*) FROM st1 INTERVAL(10s)");

  run("SELECT /*+ smalldata_scan_sort() */ _WSTART, COUNT(*) FROM st1 PARTITION BY TBNAME INTERVAL(10s)");
}

TEST_F(PlanIntervalTest, fastIntervalHint) {
  useDb("root", "test");

  const char* range = " WHERE ts >= '2022-04-01 00:00:00' AND ts < '2022-04-01 01:00:00' ";
  run(std::string("SELECT /*+ FAST_INTERVAL() */ _WSTART, COUNT(*), AVG(c1) FROM st1") + range + "INTERVAL(10s)");
  ASSERT_TRUE(findPlanNode("MergeAlignedInterval"));
  ASSERT_TRUE(findPlanNode("Exchange"));
  ASSERT_TRUE(planContains("\"FastInterval\":true"));
  ASSERT_FALSE(planContains("\"ExInterval\""));
  ASSERT_FALSE(planContains("\"DelayFreeTask\""));
  ASSERT_TRUE(planContains("\"SeqRecvData\":false"));

  run(std::string("SELECT _WSTART, COUNT(*) FROM st1") + range + "INTERVAL(10s)");
  ASSERT_TRUE(findPlanNode("Merge"));
  ASSERT_FALSE(planContains("\"FastInterval\""));

  run(std::string("SELECT /*+ FAST_INTERVAL() */ _WSTART, COUNT(*) FROM st1") + range + "INTERVAL(10s) SLIDING(5s)");
  ASSERT_TRUE(findPlanNode("Merge"));
  ASSERT_FALSE(planContains("\"FastInterval\""));

  run(std::string("SELECT /*+ FAST_INTERVAL() */ _WSTART, COUNT(*) FROM st1") + range + "INTERVAL(10s, 1s)");
  ASSERT_TRUE(findPlanNode("Merge"));
  ASSERT_FALSE(planContains("\"FastInterval\""));

  run(std::string("SELECT /*+ FAST_INTERVAL() */ _WSTART, COUNT(*) FROM st1") + range + "INTERVAL(10s) FILL(PREV)");
  ASSERT_TRUE(findPlanNode("Merge"));
  ASSERT_FALSE(planContains("\"FastInterval\""));

  run("SELECT /*+ FAST_INTERVAL() */ INTERP(c1) FROM st1 "
      "RANGE('2022-04-01 00:00:00', '2022-04-01 01:00:00') EVERY(10s) FILL(NULL)");
  ASSERT_TRUE(findPlanNode("Merge"));
  ASSERT_FALSE(planContains("\"FastInterval\""));

  run(std::string("SELECT /*+ FAST_INTERVAL() */ _WSTART, COUNT(*) FROM st1") + range + "INTERVAL(1d)");
  ASSERT_TRUE(findPlanNode("Merge"));
  ASSERT_FALSE(planContains("\"FastInterval\""));

  run(std::string("SELECT /*+ SPLIT_SCAN() FAST_INTERVAL() */ TBNAME, _WSTART, AVG(c1) FROM st1") + range +
      "PARTITION BY TBNAME INTERVAL(10s)");
  ASSERT_TRUE(findPlanNode("Exchange"));
  ASSERT_TRUE(exchangeSubplansContain("HashInterval"));
  ASSERT_FALSE(findPlanNode("MergeAlignedInterval"));
  ASSERT_TRUE(planContains("\"FastInterval\":true"));

  run(std::string("SELECT /*+ FAST_INTERVAL() */ TBNAME, _WSTART, AVG(c1) FROM st1") + range +
      "PARTITION BY TBNAME INTERVAL(10s)");
  ASSERT_TRUE(findPlanNode("Exchange"));
  ASSERT_TRUE(exchangeSubplansContain("HashInterval"));
  ASSERT_FALSE(findPlanNode("MergeAlignedInterval"));
  ASSERT_TRUE(planContains("\"FastInterval\":true"));

  run(std::string("SELECT /*+ FAST_INTERVAL() */ _WSTART, COUNT(*) FROM st1") + range +
      "PARTITION BY c1 INTERVAL(10s)");
  ASSERT_TRUE(findPlanNode("Merge"));
  ASSERT_FALSE(planContains("\"FastInterval\""));

  run(std::string("SELECT /*+ FAST_INTERVAL() */ _WSTART, COUNT(*) FROM st1") + range +
      "PARTITION BY tag1 INTERVAL(10s)");
  ASSERT_TRUE(findPlanNode("Merge"));
  ASSERT_FALSE(planContains("\"FastInterval\""));

  run(std::string("SELECT /*+ FAST_INTERVAL() */ _WSTART, COUNT(*) FROM st1") + range +
      "PARTITION BY TBNAME, c1 INTERVAL(10s)");
  ASSERT_FALSE(planContains("\"FastInterval\""));
}

TEST_F(PlanIntervalTest, exIntervalHint) {
  useDb("root", "test");

  const char* range = " WHERE ts >= '2022-04-01 00:00:00' AND ts < '2022-04-01 01:00:00' ";
  run(std::string("SELECT /*+ EX_INTERVAL() */ _WSTART, _WEND, _WDURATION, COUNT(*), AVG(c1), SUM(c1) FROM st1") +
      range + "INTERVAL(10s)");
  // The mock catalog uses three replica endpoints per vgroup, so the
  // single-endpoint EX_INTERVAL eligibility check intentionally falls back.
  ASSERT_TRUE(findPlanNode("Merge"));
  ASSERT_FALSE(planContains("\"ExInterval\":true"));
  ASSERT_FALSE(planContains("\"FastInterval\""));
  ASSERT_FALSE(planContains("\"DelayFreeTask\""));

  run(std::string("SELECT /*+ EX_INTERVAL() */ _WSTART, SUM(c1) FROM st1") + range + "INTERVAL(10s)");
  ASSERT_TRUE(findPlanNode("Merge"));
  ASSERT_FALSE(planContains("\"ExInterval\""));

  run(std::string("SELECT /*+ EX_INTERVAL() */ _WSTART, COUNT(*) FROM st1") + range + "INTERVAL(10s) LIMIT 1");
  ASSERT_TRUE(findPlanNode("Merge"));
  ASSERT_FALSE(planContains("\"ExInterval\""));
  ASSERT_FALSE(planContains("\"DelayFreeTask\""));

  run(std::string("SELECT /*+ EX_INTERVAL() */ _WSTART, COUNT(*) FROM st1") + range +
      "PARTITION BY TBNAME INTERVAL(10s)");
  ASSERT_FALSE(planContains("\"ExInterval\""));
  ASSERT_FALSE(planContains("\"DelayFreeTask\""));
}

TEST_F(PlanIntervalTest, splitScanHint) {
  useDb("root", "test");

  run("SELECT /*+ SPLIT_SCAN() */ _WSTART, COUNT(*) FROM st1 "
      "WHERE ts >= '2022-04-01 00:00:00' AND ts < '2022-04-01 01:00:00' PARTITION BY TBNAME INTERVAL(10s)");
  ASSERT_TRUE(findPlanNode("Exchange"));
  ASSERT_TRUE(exchangeSubplansContain("TableScan"));
}

TEST_F(PlanIntervalTest, streamCalcDisablesIntervalHints) {
  useDb("root", "test");

  const char* range = " WHERE ts >= '2022-04-01 00:00:00' AND ts < '2022-04-01 01:00:00' ";
  for (const char* hint : {"FAST_INTERVAL()", "EX_INTERVAL()"}) {
    runStreamCalc(std::string("SELECT /*+ ") + hint + " */ _WSTART, COUNT(*), AVG(c1) FROM st1" + range +
                  "INTERVAL(10s)");
    ASSERT_TRUE(findPlanNode("Merge"));
    ASSERT_FALSE(planContains("\"FastInterval\":true"));
    ASSERT_FALSE(planContains("\"ExInterval\":true"));
    ASSERT_FALSE(planContains("\"FastInterval\""));
    ASSERT_FALSE(planContains("\"ExInterval\""));
  }
}
