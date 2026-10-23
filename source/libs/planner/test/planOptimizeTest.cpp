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

#include "mockCatalog.h"
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

  run("SELECT LAST(c1) FROM t1");

  run("SELECT LAST(c1) FROM t1 WHERE ts BETWEEN '2022-7-29 11:10:10' AND '2022-7-30 11:10:10' INTERVAL(10S) "
      "FILL(LINEAR)");

  run("SELECT COUNT(TBNAME) FROM t1");
}

TEST_F(PlanOptimizeTest, pushDownCondition) {
  useDb("root", "test");

  run("SELECT ts, c1 FROM st1 WHERE tag1 > 4");

  run("SELECT ts, c1 FROM st1 WHERE TBNAME = 'st1s1'");

  run("SELECT ts, c1 FROM st1 WHERE tag1 > 4 or tag1 < 2");

  run("SELECT ts, c1 FROM st1 WHERE tag1 > 4 AND tag2 = 'hello'");

  run("SELECT ts, c1 FROM st1 WHERE tag1 > 4 AND tag2 = 'hello' AND c1 > 10");

  run("SELECT ts, c1 FROM (SELECT * FROM st1) WHERE tag1 > 4");
}

TEST_F(PlanOptimizeTest, sortPrimaryKey) {
  useDb("root", "test");

  run("SELECT c1 FROM t1 ORDER BY ts");

  run("SELECT c1 FROM st1 ORDER BY ts");

  run("SELECT c1 FROM t1 ORDER BY ts DESC");

  run("SELECT c1 FROM st1 ORDER BY ts DESC");

  run("SELECT COUNT(*) FROM t1 INTERVAL(10S) ORDER BY _WSTART DESC");

  run("SELECT FIRST(c1) FROM t1 WHERE ts BETWEEN '2022-7-29 11:10:10' AND '2022-7-30 11:10:10' INTERVAL(10S) "
      "FILL(LINEAR) ORDER BY _WSTART DESC");

  run("SELECT LAST(c1) FROM t1 WHERE ts BETWEEN '2022-7-29 11:10:10' AND '2022-7-30 11:10:10' INTERVAL(10S) "
      "FILL(LINEAR) ORDER BY _WSTART");
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

TEST_F(PlanOptimizeTest, mergeProjects) {
  useDb("root", "test");

  run("SELECT * FROM (SELECT * FROM t1 WHERE c1 > 10 ORDER BY ts) ORDER BY ts");
}

TEST_F(PlanOptimizeTest, pushDownProjectCond) {
  useDb("root", "test");

  run("select 1-abs(c1) from (select unique(c1) c1 from st1s3) where 1-c1>5 order by 1 nulls first");
}

TEST_F(PlanOptimizeTest, LastRowScan) {
  useDb("root", "cache_db");

  run("SELECT LAST_ROW(c1), c2 FROM t1");

  run("SELECT LAST_ROW(c1), c2, tag1, tbname FROM st1");

  run("SELECT LAST_ROW(c1) FROM st1 PARTITION BY TBNAME");
  ASSERT_TRUE(planContains("\"BatchMultiTableRead\":true"));

  run("SELECT tbname, LAST(c1) FROM st1 GROUP BY tbname");
  ASSERT_FALSE(planContains("\"BatchMultiTableRead\""));

  run("SELECT LAST_ROW(c1), SUM(c3) FROM t1");
  ASSERT_TRUE(planContains("\"BatchMultiTableRead\":true"));

  run("SELECT LAST_ROW(tag1) FROM st1");

  run("SELECT LAST(c1) FROM st1");
  ASSERT_FALSE(planContains("\"BatchMultiTableRead\""));

  run("SELECT LAST(c1), c2 FROM st1");
  ASSERT_FALSE(planContains("\"BatchMultiTableRead\""));
}

TEST(PlanGroupMergeTest, TerminalAndTransparentConsumers) {
  SAggLogicNode agg = {};
  SProjectLogicNode project = {};
  SProjectLogicNode outer = {};
  SSortLogicNode sort = {};
  agg.node.type = QUERY_NODE_LOGIC_PLAN_AGG;
  project.node.type = outer.node.type = QUERY_NODE_LOGIC_PLAN_PROJECT;
  sort.node.type = QUERY_NODE_LOGIC_PLAN_SORT;
  project.node.requireDataOrder = outer.node.requireDataOrder =
      sort.node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  EXPECT_TRUE(planCanMergeOutputGroups(&agg.node));
  agg.node.pParent = &project.node;
  project.node.pParent = &outer.node;
  outer.node.pParent = &sort.node;
  project.node.groupAction = GROUP_ACTION_CLEAR;
  EXPECT_TRUE(planCanMergeOutputGroups(&agg.node));
  agg.node.groupAction = GROUP_ACTION_KEEP;
  EXPECT_FALSE(planCanMergeOutputGroups(&agg.node));
}

TEST(PlanGroupMergeTest, ChecksBeyondClearingProjection) {
  SAggLogicNode agg = {};
  SProjectLogicNode project = {};
  SSortLogicNode sort = {};
  project.node.type = QUERY_NODE_LOGIC_PLAN_PROJECT;
  project.node.groupAction = GROUP_ACTION_CLEAR;
  sort.node.type = QUERY_NODE_LOGIC_PLAN_SORT;
  project.node.requireDataOrder = sort.node.requireDataOrder =
      DATA_ORDER_LEVEL_NONE;
  agg.node.pParent = &project.node;
  project.node.pParent = &sort.node;
  sort.node.groupAction = GROUP_ACTION_KEEP;
  EXPECT_FALSE(planCanMergeOutputGroups(&agg.node));
  sort.node.groupAction = GROUP_ACTION_NONE;
  sort.groupSort = true;
  EXPECT_FALSE(planCanMergeOutputGroups(&agg.node));
  sort.groupSort = false;
  for (auto order : {DATA_ORDER_LEVEL_IN_BLOCK,
                     DATA_ORDER_LEVEL_IN_GROUP, DATA_ORDER_LEVEL_GLOBAL}) {
    sort.node.requireDataOrder = order;
    EXPECT_FALSE(planCanMergeOutputGroups(&agg.node));
  }
}

TEST(PlanGroupMergeTest, DistinguishesInputAndOutputGroupLimits) {
  SAggLogicNode agg = {};
  SProjectLogicNode project = {};
  SLimitNode limit = {};
  project.node.type = QUERY_NODE_LOGIC_PLAN_PROJECT;
  project.node.groupAction = GROUP_ACTION_CLEAR;
  project.node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  agg.node.pParent = &project.node;
  project.node.pLimit = (SNode*)&limit;
  EXPECT_FALSE(planCanMergeOutputGroups(&agg.node));
  project.inputIgnoreGroup = true;
  EXPECT_TRUE(planCanMergeOutputGroups(&agg.node));
  project.node.pSlimit = (SNode*)&limit;
  EXPECT_FALSE(planCanMergeOutputGroups(&agg.node));
}

TEST(PlanGroupMergeTest, RejectsUnprovenConsumers) {
  SAggLogicNode agg = {};
  SProjectLogicNode project = {};
  SLogicNode consumer = {};
  project.node.type = QUERY_NODE_LOGIC_PLAN_PROJECT;
  project.node.groupAction = GROUP_ACTION_CLEAR;
  project.node.requireDataOrder = consumer.requireDataOrder =
      DATA_ORDER_LEVEL_NONE;
  agg.node.pParent = &project.node;
  project.node.pParent = &consumer;
  for (auto type : {QUERY_NODE_LOGIC_PLAN_AGG, QUERY_NODE_LOGIC_PLAN_WINDOW,
                    QUERY_NODE_LOGIC_PLAN_JOIN, QUERY_NODE_LOGIC_PLAN_FILL,
                    QUERY_NODE_LOGIC_PLAN_PARTITION,
                    QUERY_NODE_LOGIC_PLAN_INTERP_FUNC}) {
    consumer.type = type;
    EXPECT_FALSE(planCanMergeOutputGroups(&agg.node));
  }
}

class PlanCacheAggregateTest : public PlannerTestBase {
 protected:
  void useCacheDb(const string& db) {
    g_mockCatalogService->createDatabase(db, false, TSDB_CACHE_MODEL_BOTH);
    g_mockCatalogService
        ->createTableBuilder(db, "st", TSDB_SUPER_TABLE, 3, 1)
        .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
        .addColumn("c1", TSDB_DATA_TYPE_INT)
        .addColumn("c2", TSDB_DATA_TYPE_BINARY, 20)
        .addTag("tag1", TSDB_DATA_TYPE_INT)
        .done();
    g_mockCatalogService->createSubTable(db, "st", "ct1", 2);
    g_mockCatalogService->createSubTable(db, "st", "ct2", 3);
    useDb("root", db);
  }

  static const SLogicNode* findNode(const SLogicNode* node, ENodeType type) {
    if (nodeType(node) == type) return node;
    SNode* child = nullptr;
    FOREACH(child, node->pChildren) {
      const auto* found = findNode((const SLogicNode*)child, type);
      if (found != nullptr) return found;
    }
    return nullptr;
  }

  // Runs check on each hash aggregate (only those directly over a cache
  // scan when cacheOnly is set) and returns how many were checked.
  static int checkAggregates(
      const SPhysiNode* node, bool cacheOnly,
      const std::function<void(const SAggPhysiNode*)>& check) {
    int count = 0;
    const auto* input = (const SPhysiNode*)nodesListGetNode(
        node->pChildren, 0);
    if (nodeType(node) == QUERY_NODE_PHYSICAL_PLAN_HASH_AGG &&
        (!cacheOnly || (input != nullptr &&
                        nodeType(input) ==
                            QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN))) {
      check((const SAggPhysiNode*)node);
      ++count;
    }
    SNode* child = nullptr;
    FOREACH(child, node->pChildren) {
      count += checkAggregates((const SPhysiNode*)child, cacheOnly, check);
    }
    return count;
  }

  static int checkAggregates(
      const SQueryPlan* plan, bool cacheOnly,
      const std::function<void(const SAggPhysiNode*)>& check) {
    int count = 0;
    SNode* level = nullptr;
    FOREACH(level, plan->pSubplans) {
      SNode* subplan = nullptr;
      FOREACH(subplan, ((SNodeListNode*)level)->pNodeList) {
        count += checkAggregates(((SSubplan*)subplan)->pNode, cacheOnly,
                                 check);
      }
    }
    return count;
  }

  void checkPlan(const string& sql, bool cache, bool nonBlocking,
                 int merge = -1, bool hasFilter = false) {
    SCOPED_TRACE(sql);
    setLogicPlanCheck([cache, nonBlocking](const SLogicNode* root,
                                          bool optimized) {
      if (!optimized) return;
      const auto* agg = (const SAggLogicNode*)findNode(
          root, QUERY_NODE_LOGIC_PLAN_AGG);
      const auto* scan = (const SScanLogicNode*)findNode(
          root, QUERY_NODE_LOGIC_PLAN_SCAN);
      ASSERT_NE(agg, nullptr);
      ASSERT_NE(scan, nullptr);
      EXPECT_EQ(scan->scanType == SCAN_TYPE_LAST_ROW, cache);
      EXPECT_EQ(agg->node.forceCreateNonBlockingOptr, nonBlocking);
      if (cache) EXPECT_EQ(scan->groupOrderScan, nonBlocking);
    });
    setPhysiPlanCheck([nonBlocking, merge, hasFilter](
                         const SQueryPlan* plan) {
      int count = checkAggregates(
          plan, false, [=](const SAggPhysiNode* agg) {
            EXPECT_EQ(agg->node.forceCreateNonBlockingOptr, nonBlocking);
            if (merge >= 0) EXPECT_EQ(agg->mergeDataBlock, merge != 0);
            if (hasFilter) EXPECT_NE(agg->node.pConditions, nullptr);
          });
      EXPECT_GT(count, 0);
    });
    run(sql);
  }

  void checkConsumerPlan(const string& sql, bool merge) {
    SCOPED_TRACE(sql);
    setLogicPlanCheck({});
    setPhysiPlanCheck([merge](const SQueryPlan* plan) {
      int count = checkAggregates(
          plan, true, [merge](const SAggPhysiNode* agg) {
            EXPECT_TRUE(agg->node.forceCreateNonBlockingOptr);
            EXPECT_EQ(agg->mergeDataBlock, merge);
          });
      EXPECT_GT(count, 0);
    });
    run(sql);
  }
};

TEST_F(PlanCacheAggregateTest, NonBlockingTablePartitions) {
  useCacheDb("cache_agg_partitions");
  const string queries[] = {
      "SELECT LAST_ROW(c1) FROM st PARTITION BY tbname",
      "SELECT tbname, tag1, LAST_ROW(c1) FROM st "
      "PARTITION BY tbname, tag1",
      "SELECT tbname, LAST_ROW(c1) FROM st GROUP BY tbname",
      "SELECT LAST(c1) FROM st PARTITION BY tbname",
      "SELECT LAST(c1), LAST_ROW(c2) FROM st PARTITION BY tbname",
      "SELECT LAST_ROW(*) FROM st PARTITION BY tbname",
      "SELECT LAST_ROW(c1) FROM st WHERE tag1 > 0 PARTITION BY tbname"};
  for (const auto& sql : queries) {
    checkPlan(sql, true, true, 1);
  }
}

TEST_F(PlanCacheAggregateTest, KeepsAggregateFilter) {
  useCacheDb("cache_agg_filter");
  checkPlan("SELECT value FROM (SELECT LAST_ROW(c1) AS value FROM st "
            "PARTITION BY tbname) WHERE value > 0",
            true, true, 1, true);
}

TEST_F(PlanCacheAggregateTest, PreservesLimitedGroupOutput) {
  useCacheDb("cache_agg_limits");
  checkPlan("SELECT LAST_ROW(c1) FROM st PARTITION BY tbname SLIMIT 2",
            true, true, 0);
  checkPlan("SELECT LAST_ROW(c1) FROM st PARTITION BY tbname LIMIT 1",
            true, true, 0);
}

TEST_F(PlanCacheAggregateTest, OrdinaryNonBlockingOutputKeepsGroups) {
  useCacheDb("cache_agg_ordinary");
  checkPlan("SELECT SUM(c1) FROM st PARTITION BY tbname SLIMIT 2",
            false, true, 0);
}

TEST_F(PlanCacheAggregateTest, WindowJoinAggregateMergesGroups) {
  useCacheDb("cache_agg_window_join");
  // Each window is a group; the aggregate must clear group ids so that a
  // parent LIMIT counts rows globally instead of per window.
  checkPlan("SELECT SUM(r.c1) FROM ct1 l LEFT WINDOW JOIN ct2 r "
            "WINDOW_OFFSET(-1s, 1s)", false, true, 1);
}

TEST_F(PlanCacheAggregateTest, TransparentAncestorsAllowBatching) {
  useCacheDb("cache_agg_transparent");
  checkConsumerPlan(
      "SELECT value + 1 FROM (SELECT value FROM "
      "(SELECT LAST_ROW(c1) AS value FROM st PARTITION BY tbname) "
      "WHERE value > 0) ORDER BY 1", true);
}

TEST_F(PlanCacheAggregateTest, AncestorAggregateKeepsGroupsAcrossSplit) {
  useCacheDb("cache_agg_ancestor");
  checkConsumerPlan(
      "SELECT value % 5, COUNT(*) FROM "
      "(SELECT LAST_ROW(c1) AS value FROM st PARTITION BY tbname) "
      "GROUP BY value % 5", false);
  checkConsumerPlan(
      "SELECT _wstart, COUNT(*) FROM "
      "(SELECT LAST_ROW(ts) AS ts, LAST_ROW(c1) AS value "
      "FROM st PARTITION BY tbname) INTERVAL(1s)", false);
}

TEST_F(PlanCacheAggregateTest, ExcludesOtherGroupShapes) {
  useCacheDb("cache_agg_exclusions");
  checkPlan("SELECT LAST_ROW(c1) FROM st", true, false);
  checkPlan("SELECT LAST_ROW(c1) FROM ct1 PARTITION BY tbname", true, false);
  checkPlan("SELECT LAST_ROW(c1) FROM st PARTITION BY tag1", true, false);
  checkPlan("SELECT LAST_ROW(c1) FROM st "
            "PARTITION BY SUBSTR(tbname, 1, 2)", true, false);
  checkPlan("SELECT LAST_ROW(c1) FROM st PARTITION BY tbname, c2",
            false, false);
  checkPlan("SELECT LAST_ROW(c1) FROM st WHERE ts >= 1000 "
            "PARTITION BY tbname", false, false);
  checkPlan("SELECT LAST_ROW(c1) FROM st WHERE c1 > 0 "
            "PARTITION BY tbname", false, false);
}

TEST_F(PlanOptimizeTest, tagScan) {
  useDb("root", "test");
  run("select tag1 from st1 group by tag1");
  run("select distinct tag1 from st1");
  run("select tag1*tag1 from st1 group by tag1*tag1");
}

TEST_F(PlanOptimizeTest, pushDownLimit) {
  useDb("root", "test");

  run("SELECT c1 FROM t1 LIMIT 1");

  run("SELECT c1 FROM st1 LIMIT 1");

  run("SELECT c1 FROM st1 LIMIT 20 OFFSET 10");
}
