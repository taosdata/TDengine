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

#include <gtest/gtest-spi.h>
#include <set>

#include "mockCatalog.h"
#include "mockCatalogService.h"
#include "planTestUtil.h"
#include "planner.h"
#include "stub.h"
// The C headers define wchar_t, so load the C++ test headers first.
#include "functionMgt.h"

using namespace std;

class PlanOptimizeTest : public PlannerTestBase {};

static bool lastScanUnsupportedPkType = false;

static int32_t lastScanCompositeCatalogGetTableMeta(
    SCatalog*, SRequestConnInfo*, const SName* name, STableMeta** meta) {
  int32_t code = g_mockCatalogService->catalogGetTableMeta(name, meta);
  if (code == TSDB_CODE_SUCCESS && meta != nullptr && *meta != nullptr &&
      strcmp(tNameGetTableName(name), "t1") == 0) {
    (*meta)->tableInfo.numOfPKs = 1;
    (*meta)->schema[1].flags |= COL_IS_KEY;
    if (lastScanUnsupportedPkType) {
      (*meta)->schema[1].type = TSDB_DATA_TYPE_DOUBLE;
      (*meta)->schema[1].bytes = sizeof(double);
    }
  }
  return code;
}

TEST_F(PlanOptimizeTest, lastScanTypeEligibility) {
  useDb("root", "test");
  struct Case {
    const char* sql;
    ELastScanType expected;
  };
  const Case cases[] = {
      {"LAST(c1) FROM t1", LAST_SCAN_LAST_ONLY},
      {"LAST_ROW(c1) FROM t1", LAST_SCAN_LAST_ROW_ONLY},
      {"LAST(c1), LAST_ROW(c2) FROM t1", LAST_SCAN_MIXED},
      {"LAST(c1), SUM(c3) FROM t1", LAST_SCAN_NONE},
      {"LAST_ROW(c1), COUNT(c3) FROM t1", LAST_SCAN_NONE},
      {"c2, LAST(c1) FROM t1", LAST_SCAN_LAST_ONLY},
      {"c2, LAST_ROW(c1) FROM t1", LAST_SCAN_LAST_ROW_ONLY},
      {"LAST(c1) FROM t1 WHERE c3 > 10", LAST_SCAN_LAST_ONLY},
      {"LAST_ROW(c1) FROM t1 WHERE c3 IS NULL", LAST_SCAN_LAST_ROW_ONLY},
      {"LAST(c1) FROM st1 GROUP BY tag1", LAST_SCAN_LAST_ONLY},
      {"LAST_ROW(c1) FROM st1 GROUP BY tbname", LAST_SCAN_LAST_ROW_ONLY},
      {"LAST(c1) FROM t1 GROUP BY c3", LAST_SCAN_NONE},
      {"LAST(c1 + 1) FROM t1", LAST_SCAN_NONE},
  };
  for (const auto& test : cases) {
    SCOPED_TRACE(test.sql);
    setLogicPlanCheck([&](const SLogicNode* root, bool optimized) {
      if (!optimized) return;
      const SScanLogicNode* scan = nullptr;
      std::function<void(const SLogicNode*)> walk;
      walk = [&](const SLogicNode* node) {
        if (nodeType(node) == QUERY_NODE_LOGIC_PLAN_SCAN) {
          scan = (const SScanLogicNode*)node;
        }
        SNode* child = nullptr;
        FOREACH(child, node->pChildren) walk((const SLogicNode*)child);
      };
      walk(root);
      ASSERT_NE(nullptr, scan);
      EXPECT_EQ(test.expected, scan->lastScanType);
      char* json = nullptr;
      ASSERT_EQ(0, nodesNodeToString((const SNode*)scan, false, &json,
                                      nullptr));
      ASSERT_NE(nullptr, json);
      EXPECT_NE(string::npos, string(json).find("\"LastScanType\""));
      EXPECT_EQ(string::npos, string(json).find("\"LastOnlyScan\""));
      taosMemoryFree(json);
    });
    string sql = string("SELECT ") + test.sql;
    const size_t group = sql.find(" GROUP BY");
    const string conjunction = sql.find(" WHERE ") == string::npos
                                   ? " WHERE " : " AND ";
    sql.insert(group == string::npos ? sql.size() : group,
               conjunction + "ts >= 1650000000000 AND ts < 1660000000000");
    run(sql);
  }
}

TEST_F(PlanOptimizeTest, lastScanCompositePrimaryKeyEligibility) {
  Stub stub;
  struct ResetPkType {
    ~ResetPkType() { lastScanUnsupportedPkType = false; }
  } resetPkType;
  stub.set(catalogGetTableMeta, lastScanCompositeCatalogGetTableMeta);
  useDb("root", "test");
  struct Case {
    const char* sql;
    ELastScanType expected;
  };
  const Case cases[] = {
      {"LAST(c3) FROM t1", LAST_SCAN_LAST_ONLY},
      {"LAST_ROW(c3) FROM t1", LAST_SCAN_LAST_ROW_ONLY},
      {"LAST(c3), LAST_ROW(c4) FROM t1", LAST_SCAN_MIXED},
      {"LAST(c3) FROM t1 WHERE c1 > 1", LAST_SCAN_LAST_ONLY},
      {"LAST(c3) FROM t1 GROUP BY c1", LAST_SCAN_NONE},
      {"LAST(c3 + 1) FROM t1", LAST_SCAN_NONE},
  };
  for (const auto& test : cases) {
    SCOPED_TRACE(test.sql);
    setLogicPlanCheck([&](const SLogicNode* root, bool optimized) {
      if (!optimized) return;
      const SScanLogicNode* scan = nullptr;
      std::function<void(const SLogicNode*)> walk;
      walk = [&](const SLogicNode* node) {
        if (nodeType(node) == QUERY_NODE_LOGIC_PLAN_SCAN) {
          scan = (const SScanLogicNode*)node;
        }
        SNode* child = nullptr;
        FOREACH(child, node->pChildren) walk((const SLogicNode*)child);
      };
      walk(root);
      ASSERT_NE(nullptr, scan);
      EXPECT_EQ(test.expected, scan->lastScanType);
      bool foundPk = false;
      SNode* col = nullptr;
      FOREACH(col, scan->pScanCols) {
        if (nodeType(col) == QUERY_NODE_COLUMN &&
            ((SColumnNode*)col)->isPk) foundPk = true;
      }
      EXPECT_TRUE(foundPk);
    });
    string sql = string("SELECT ") + test.sql;
    const size_t group = sql.find(" GROUP BY");
    const string conjunction = sql.find(" WHERE ") == string::npos
                                   ? " WHERE " : " AND ";
    sql.insert(group == string::npos ? sql.size() : group,
               conjunction + "ts >= 1650000000000 AND ts < 1660000000000");
    run(sql);
  }
  lastScanUnsupportedPkType = true;
  setLogicPlanCheck([](const SLogicNode* root, bool optimized) {
    if (!optimized) return;
    const SScanLogicNode* scan = nullptr;
    std::function<void(const SLogicNode*)> walk;
    walk = [&](const SLogicNode* node) {
      if (nodeType(node) == QUERY_NODE_LOGIC_PLAN_SCAN) {
        scan = (const SScanLogicNode*)node;
      }
      SNode* child = nullptr;
      FOREACH(child, node->pChildren) walk((const SLogicNode*)child);
    };
    walk(root);
    ASSERT_NE(nullptr, scan);
    EXPECT_EQ(LAST_SCAN_NONE, scan->lastScanType);
  });
  run("SELECT LAST(c3) FROM t1 WHERE ts >= 1650000000000 "
      "AND ts < 1660000000000");
  lastScanUnsupportedPkType = false;
  stub.reset(catalogGetTableMeta);
}

TEST_F(PlanOptimizeTest, lastScanWindowsKeepBaselineFallback) {
  useDb("root", "test");
  const char* expressions[] = {
      "LAST(c1)", "LAST_ROW(c1)", "LAST(c1), LAST_ROW(c2)"};
  const char* windows[] = {
      "SESSION(ts, 10s)", "STATE_WINDOW(c3)", "INTERVAL(10s)"};
  for (const char* expression : expressions) {
    for (const char* window : windows) {
      const string sql = string("SELECT ") + expression +
                         " FROM t1 WHERE ts >= 1650000000000 "
                         "AND ts < 1660000000000 " + window;
      SCOPED_TRACE(sql);
      bool checked = false;
      setLogicPlanCheck([&](const SLogicNode* root, bool optimized) {
        if (!optimized) return;
        std::function<void(const SLogicNode*)> walk;
        walk = [&](const SLogicNode* node) {
          if (nodeType(node) == QUERY_NODE_LOGIC_PLAN_SCAN) {
            checked = true;
            EXPECT_EQ(LAST_SCAN_NONE,
                      ((const SScanLogicNode*)node)->lastScanType);
          }
          SNode* child = nullptr;
          FOREACH(child, node->pChildren) walk((const SLogicNode*)child);
        };
        walk(root);
      });
      run(sql);
      EXPECT_TRUE(checked);
    }
  }
}

TEST_F(PlanOptimizeTest, lastScanUnsupportedFiltersUseOrdinaryScan) {
  useDb("root", "test");
  const char* expressions[] = {
      "LAST(c1)", "LAST_ROW(c1)", "LAST(c1), LAST_ROW(c2)"};
  const char* filters[] = {"ABS(c3) > 10", "c3 > c1", "c2 LIKE 'a%'"};
  for (const char* expression : expressions) {
    for (const char* filter : filters) {
      const string sql = string("SELECT ") + expression +
                         " FROM t1 WHERE ts >= 1650000000000 "
                         "AND ts < 1660000000000 AND " + filter;
      SCOPED_TRACE(sql);
      bool checked = false;
      setLogicPlanCheck([&](const SLogicNode* root, bool optimized) {
        if (!optimized) return;
        std::function<void(const SLogicNode*)> walk;
        walk = [&](const SLogicNode* node) {
          if (nodeType(node) == QUERY_NODE_LOGIC_PLAN_SCAN) {
            checked = true;
            EXPECT_EQ(LAST_SCAN_NONE,
                      ((const SScanLogicNode*)node)->lastScanType);
          }
          SNode* child = nullptr;
          FOREACH(child, node->pChildren) walk((const SLogicNode*)child);
        };
        walk(root);
      });
      run(sql);
      EXPECT_TRUE(checked);
    }
  }
}

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

  static void checkBranchGlobalAggregate(const SSubplan* branch, int keyCount,
                                        bool& count, bool& cached) {
    ASSERT_NE(branch->pNode, nullptr);
    ASSERT_EQ(nodeType(branch->pNode), QUERY_NODE_PHYSICAL_PLAN_PROJECT);
    ASSERT_EQ(LIST_LENGTH(branch->pNode->pChildren), 1);
    const auto* aggregate = (const SPhysiNode*)nodesListGetNode(
        branch->pNode->pChildren, 0);
    ASSERT_NE(aggregate, nullptr);
    ASSERT_EQ(nodeType(aggregate), QUERY_NODE_PHYSICAL_PLAN_HASH_AGG);
    const auto* global = (const SAggPhysiNode*)aggregate;
    int keys = 0;
    SNode* function = nullptr;
    FOREACH(function, global->pAggFuncs) {
      ASSERT_EQ(nodeType(function), QUERY_NODE_TARGET);
      const auto* expr = ((const STargetNode*)function)->pExpr;
      ASSERT_NE(expr, nullptr);
      ASSERT_EQ(nodeType(expr), QUERY_NODE_FUNCTION);
      const auto* func = (const SFunctionNode*)expr;
      count |= func->funcType == FUNCTION_TYPE_SUM;
      cached |= func->funcType == FUNCTION_TYPE_CACHE_LAST;
      keys += func->funcType == FUNCTION_TYPE_GROUP_KEY;
    }
    EXPECT_NE(count, cached);
    EXPECT_EQ(keys, keyCount);
  }

  static void checkBranchScanSources(const SSubplan* branch, bool cached) {
    ASSERT_NE(branch->pNode, nullptr);
    const auto* aggregate = (const SPhysiNode*)nodesListGetNode(
        branch->pNode->pChildren, 0);
    ASSERT_NE(aggregate, nullptr);
    ASSERT_EQ(LIST_LENGTH(aggregate->pChildren), 1);
    const auto* input = (const SPhysiNode*)nodesListGetNode(
        aggregate->pChildren, 0);
    ASSERT_NE(input, nullptr);
    if (cached) {
      ASSERT_EQ(nodeType(input), QUERY_NODE_PHYSICAL_PLAN_MERGE);
      ASSERT_GT(LIST_LENGTH(input->pChildren), 1);
      input = (const SPhysiNode*)nodesListGetNode(input->pChildren, 0);
      ASSERT_NE(input, nullptr);
    }
    ASSERT_EQ(nodeType(input), QUERY_NODE_PHYSICAL_PLAN_EXCHANGE);
    const auto* source = (const SExchangePhysiNode*)input;
    ASSERT_GT(LIST_LENGTH(branch->pChildren), 1);
    int32_t firstVgroup = -1;
    bool multipleVgroups = false;
    SNode* child = nullptr;
    FOREACH(child, branch->pChildren) {
      const auto* leaf = (const SSubplan*)child;
      EXPECT_GE(leaf->id.groupId, source->srcStartGroupId);
      EXPECT_LE(leaf->id.groupId, source->srcEndGroupId);
      EXPECT_EQ(leaf->subplanType, SUBPLAN_TYPE_SCAN);
      ASSERT_NE(leaf->pNode, nullptr);
      if (firstVgroup == -1) firstVgroup = leaf->execNode.nodeId;
      multipleVgroups |= leaf->execNode.nodeId != firstVgroup;
      if (cached) {
        EXPECT_EQ(nodeType(leaf->pNode),
                  QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN);
      } else {
        EXPECT_GT(checkAggregates(
            leaf->pNode, false, [](const SAggPhysiNode* partial) {
              ASSERT_EQ(LIST_LENGTH(partial->node.pChildren), 1);
              const auto* scan = nodesListGetNode(
                  partial->node.pChildren, 0);
              ASSERT_NE(scan, nullptr);
              EXPECT_EQ(nodeType(scan),
                        QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
            }), 0);
      }
    }
    EXPECT_TRUE(multipleVgroups);
  }

  static void checkExchangeSourceRange(const SExchangePhysiNode* exchange,
                                       const SSubplan* subplan) {
    set<int32_t> childGroups;
    SNode* child = nullptr;
    FOREACH(child, subplan->pChildren) {
      childGroups.insert(((const SSubplan*)child)->id.groupId);
    }
    ASSERT_LE(exchange->srcStartGroupId, exchange->srcEndGroupId);
    for (int64_t group = exchange->srcStartGroupId;
         group <= exchange->srcEndGroupId; ++group) {
      EXPECT_EQ(childGroups.count(static_cast<int32_t>(group)), 1U)
          << "missing source group " << group;
    }
  }

  static void checkExchangeSources(const SQueryPlan* plan) {
    std::function<void(const SPhysiNode*, const SSubplan*)> walk;
    walk = [&walk](const SPhysiNode* node, const SSubplan* subplan) {
      if (nodeType(node) == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
        const auto* exchange = (const SExchangePhysiNode*)node;
        checkExchangeSourceRange(exchange, subplan);
      }
      SNode* child = nullptr;
      FOREACH(child, node->pChildren) {
        walk((const SPhysiNode*)child, subplan);
      }
    };
    SNode* level = nullptr;
    FOREACH(level, plan->pSubplans) {
      SNode* node = nullptr;
      FOREACH(node, ((const SNodeListNode*)level)->pNodeList) {
        const auto* subplan = (const SSubplan*)node;
        walk(subplan->pNode, subplan);
      }
    }
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

TEST_F(PlanCacheAggregateTest, ExchangeSourcesRequireEveryDeclaredGroup) {
  SExchangePhysiNode exchange = {};
  exchange.srcStartGroupId = 2;
  exchange.srcEndGroupId = 4;
  SSubplan parent = {};
  SSubplan children[4] = {};
  const int groups[] = {2, 4, 3, 3};
  SNodeList* list = nullptr;
  ASSERT_EQ(nodesMakeList(&list), 0);
  unique_ptr<SNodeList, decltype(&nodesClearList)> sources(
      list, nodesClearList);
  ASSERT_NE(sources, nullptr);
  parent.pChildren = sources.get();
  for (int i = 0; i < 4; ++i) {
    children[i].id.groupId = groups[i];
    children[i].execNode.nodeId = i;
    ASSERT_EQ(nodesListAppend(sources.get(), (SNode*)&children[i]), 0);
    if (i == 1) {
      EXPECT_NONFATAL_FAILURE(checkExchangeSourceRange(&exchange, &parent),
                              "missing source group 3");
    } else if (i >= 2) {
      checkExchangeSourceRange(&exchange, &parent);
    }
  }
}

TEST_F(PlanCacheAggregateTest, GroupedCacheSplitDefaultsToConcurrentScan) {
  useCacheDb("cache_grouped_hint");
  for (const string& clause : {"PARTITION BY tbname", "GROUP BY tbname"}) {
    const string projection = "tbname, COUNT(*), LAST(c1) FROM st " + clause;
    run("SELECT " + projection);
    EXPECT_GT(countPhysiPlanText("PhysiLastRowScan"), 0);
    EXPECT_GT(countPhysiPlanText("PhysiTableScan"), 0);
    for (const string& hint : {"NO_CONCURRENT_LAST_SCAN()",
                               "no_concurrent_last_scan()",
                               "NO_CONCURRENT_LAST_SCAN ( )"}) {
      SCOPED_TRACE(hint);
      run("SELECT /*+ " + hint + " */ " + projection);
      EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan"), 0);
      EXPECT_GT(countPhysiPlanText("PhysiTableScan"), 0);
    }
    for (const string& hint : {"NO_CONCURRENT_LAST_SCAN",
                               "NO_CONCURRENT_LAST_SCAN(1)",
                               "CONCURRENT_LAST_SCAN()"}) {
      SCOPED_TRACE(hint);
      run("SELECT /*+ " + hint + " */ " + projection);
      EXPECT_GT(countPhysiPlanText("PhysiLastRowScan"), 0);
    }
  }
}

TEST_F(PlanCacheAggregateTest, UngroupedCacheSplitHonorsHint) {
  useCacheDb("cache_ungrouped_hint");
  for (const string& functions : {"COUNT(*), LAST(c1)",
                                  "COUNT(*), LAST_ROW(c2)",
                                  "COUNT(*), LAST(c1), LAST_ROW(c2)"}) {
    for (bool disabled : {false, true}) {
      SCOPED_TRACE(functions);
      SCOPED_TRACE(disabled);
      const string hint = disabled ? "/*+ NO_CONCURRENT_LAST_SCAN() */ " : "";
      run("SELECT " + hint + functions + " FROM st");
      EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan") > 0, !disabled);
      EXPECT_GT(countPhysiPlanText("PhysiTableScan"), 0);
    }
  }
  for (const string& function : {"LAST(c1)", "LAST_ROW(c2)"}) {
    for (const string& hint : {"", "/*+ NO_CONCURRENT_LAST_SCAN() */ "}) {
      run("SELECT " + hint + function + " FROM st");
      EXPECT_GT(countPhysiPlanText("PhysiLastRowScan"), 0);
      EXPECT_EQ(countPhysiPlanText("PhysiTableScan"), 0);
    }
  }
}

TEST_F(PlanCacheAggregateTest, CacheHintBelongsToItsSelect) {
  useCacheDb("cache_grouped_hint_scope");
  for (const string& clause : {"", " PARTITION BY tbname",
                               " GROUP BY tbname"}) {
    SCOPED_TRACE(clause);
    const string inner = "COUNT(*) n, LAST(c1) v FROM st" + clause;
    run("SELECT /*+ NO_CONCURRENT_LAST_SCAN() */ SUM(n), SUM(v) FROM "
        "(SELECT " + inner + ")");
    EXPECT_GT(countPhysiPlanText("PhysiLastRowScan"), 0);
    run("SELECT SUM(n), SUM(v) FROM "
        "(SELECT /*+ NO_CONCURRENT_LAST_SCAN() */ " + inner + ")");
    EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan"), 0);

    const string body = "COUNT(*), LAST(c1) FROM st" + clause;
    const string hinted = "SELECT /*+ NO_CONCURRENT_LAST_SCAN() */ " + body;
    const string plain = "SELECT " + body;
    run(plain);
    const int scans = countPhysiPlanText("PhysiLastRowScan");
    ASSERT_GT(scans, 0);
    run(hinted + " UNION ALL " + plain);
    EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan"), scans);
    run(plain + " UNION ALL " + hinted);
    EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan"), scans);
    run(plain + " UNION ALL " + plain);
    EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan"), 2 * scans);
    run(hinted + " UNION ALL " + hinted);
    EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan"), 0);
  }
}

TEST_F(PlanCacheAggregateTest, NestedCacheHintsKeepSelectScope) {
  useCacheDb("cache_nested_hint_scope");
  setPhysiPlanCheck(checkExchangeSources);
  const string hints[] = {"", "/*+ NO_CONCURRENT_LAST_SCAN() */ "};
  for (const string& clause : {"", " PARTITION BY tbname",
                               " GROUP BY tbname"}) {
    const string body = "COUNT(*) n, LAST_ROW(c1) v FROM st" + clause;
    run("SELECT " + body);
    const int scans = countPhysiPlanText("PhysiLastRowScan");
    ASSERT_GT(scans, 0);
    for (int mask = 0; mask < 8; ++mask) {
      SCOPED_TRACE(clause);
      SCOPED_TRACE(mask);
      const string inner = "SELECT " + hints[(mask >> 2) & 1] + body;
      const string middle = "SELECT " + hints[(mask >> 1) & 1] +
                            "n, v FROM (" + inner + ")";
      run("SELECT " + hints[mask & 1] + "SUM(n), SUM(v) FROM (" +
          middle + ")");
      EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan"),
                (mask & 4) ? 0 : scans);
      EXPECT_GT(countPhysiPlanText("PhysiTableScan"), 0);
    }
  }
}

TEST_F(PlanCacheAggregateTest, CacheHintsInsideUnionSubqueryStayLocal) {
  useCacheDb("cache_union_subquery_hint");
  setPhysiPlanCheck(checkExchangeSources);
  const string hints[] = {"", "/*+ NO_CONCURRENT_LAST_SCAN() */ "};
  for (const string& clause : {"", " PARTITION BY tbname",
                               " GROUP BY tbname"}) {
    const string body = "COUNT(*) n, LAST(c1) v FROM st" + clause;
    run("SELECT " + body);
    const int scans = countPhysiPlanText("PhysiLastRowScan");
    ASSERT_GT(scans, 0);
    for (int mask = 0; mask < 8; ++mask) {
      SCOPED_TRACE(clause);
      SCOPED_TRACE(mask);
      const string left = "SELECT " + hints[(mask >> 1) & 1] + body;
      const string right = "SELECT " + hints[(mask >> 2) & 1] + body;
      run("SELECT " + hints[mask & 1] + "SUM(n), SUM(v) FROM (" +
          left + " UNION ALL " + right + ")");
      const int cached = 2 - ((mask >> 1) & 1) - ((mask >> 2) & 1);
      EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan"), cached * scans);
      EXPECT_GT(countPhysiPlanText("PhysiTableScan"), 0);
    }
  }
}

TEST_F(PlanCacheAggregateTest, MixedCacheUnionKeepsMergeDependencies) {
  useCacheDb("cache_mixed_union_dependencies");
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    checkExchangeSources(plan);
    int columns = 0;
    int remote = 0;
    std::function<void(const SPhysiNode*)> walk;
    walk = [&](const SPhysiNode* node) {
      if (nodeType(node) == QUERY_NODE_PHYSICAL_PLAN_MERGE) {
        if (((const SMergePhysiNode*)node)->type == MERGE_TYPE_COLUMNS) {
          ++columns;
        } else {
          ++remote;
        }
      }
      SNode* child = nullptr;
      FOREACH(child, node->pChildren) walk((const SPhysiNode*)child);
    };
    SNode* level = nullptr;
    FOREACH(level, plan->pSubplans) {
      SNode* subplan = nullptr;
      FOREACH(subplan, ((const SNodeListNode*)level)->pNodeList) {
        walk(((const SSubplan*)subplan)->pNode);
      }
    }
    EXPECT_GT(columns, 0);
    EXPECT_GT(remote, 0);
  });
  const string plain = "SELECT COUNT(*) n, LAST(c1) v FROM st";
  const string grouped = plain + " PARTITION BY tag1";
  for (bool reverse : {false, true}) {
    SCOPED_TRACE(reverse);
    const string body = reverse ? grouped + " UNION ALL " + plain :
                                  plain + " UNION ALL " + grouped;
    run("SELECT SUM(n), SUM(v) FROM (" + body + ")");
  }
}

TEST_F(PlanCacheAggregateTest, GroupedCacheSettingSurvivesLogicalCopies) {
  useCacheDb("cache_grouped_hint_copy");
  for (bool enabled : {true, false}) {
    SCOPED_TRACE(enabled);
    setLogicPlanCheck([enabled](const SLogicNode* root, bool optimized) {
      if (optimized) return;
      const auto* agg = (const SAggLogicNode*)findNode(
          root, QUERY_NODE_LOGIC_PLAN_AGG);
      ASSERT_NE(agg, nullptr);
      EXPECT_EQ(agg->concurrentLastScan, enabled);
      SNode* clone = nullptr;
      ASSERT_EQ(nodesCloneNode((const SNode*)agg, &clone), 0);
      EXPECT_EQ(((const SAggLogicNode*)clone)->concurrentLastScan, enabled);
      char* json = nullptr;
      int32_t length = 0;
      ASSERT_EQ(nodesNodeToString(clone, false, &json, &length), 0);
      ASSERT_NE(json, nullptr);
      EXPECT_EQ(string(json, length).find("\"ConcurrentLastScan\"") !=
                    string::npos, enabled);
      SNode* decoded = nullptr;
      ASSERT_EQ(nodesStringToNode(json, &decoded), 0);
      ASSERT_NE(decoded, nullptr);
      EXPECT_EQ(((const SAggLogicNode*)decoded)->concurrentLastScan, enabled);
      nodesDestroyNode(decoded);
      taosMemoryFree(json);
      nodesDestroyNode(clone);
    });
    const string hint = enabled ? "" : "/*+ NO_CONCURRENT_LAST_SCAN() */ ";
    run("SELECT " + hint + "COUNT(*), LAST(c1) FROM st PARTITION BY tbname");
    EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan") > 0, enabled);
  }
}

TEST_F(PlanCacheAggregateTest, SplitGroupedCacheFunctions) {
  useCacheDb("cache_grouped_split");
  for (const string& clause :
       {"PARTITION BY tbname", "PARTITION BY tag1, tbname",
        "GROUP BY tag1 % 2"}) {
    const int keyCount = clause == "PARTITION BY tag1, tbname" ? 2 : 1;
    setLogicPlanCheck([keyCount](const SLogicNode* root, bool optimized) {
      if (!optimized) return;
      const auto* parent = (const SAggLogicNode*)findNode(
          root, QUERY_NODE_LOGIC_PLAN_AGG);
      ASSERT_NE(parent, nullptr);
      EXPECT_EQ(LIST_LENGTH(parent->pGroupKeys), keyCount);
      int functions = 0;
      SNode* node = nullptr;
      FOREACH(node, parent->pAggFuncs) {
        const auto* function = (const SFunctionNode*)node;
        if (function->funcType != FUNCTION_TYPE_LAST) continue;
        ++functions;
        EXPECT_EQ(LIST_LENGTH(function->pParameterList), 1);
        EXPECT_EQ(nodeType(nodesListGetNode(function->pParameterList, 0)),
                   QUERY_NODE_COLUMN);
        const auto* column =
            (const SExprNode*)nodesListGetNode(function->pParameterList, 0);
        EXPECT_EQ(function->node.resType.type, column->resType.type);
        EXPECT_EQ(function->node.resType.bytes, column->resType.bytes);
        EXPECT_EQ(function->node.resType.precision, column->resType.precision);
        EXPECT_EQ(function->node.resType.scale, column->resType.scale);
      }
      EXPECT_EQ(functions, 3);
      ASSERT_EQ(LIST_LENGTH(parent->node.pChildren), 1);
      const auto* combined = (const SLogicNode*)nodesListGetNode(
          parent->node.pChildren, 0);
      ASSERT_EQ(nodeType(combined), QUERY_NODE_LOGIC_PLAN_PROJECT);
      EXPECT_TRUE(((const SProjectLogicNode*)combined)->isSetOpProj);
      ASSERT_EQ(LIST_LENGTH(combined->pChildren), 2);
      FOREACH(node, combined->pChildren) {
        ASSERT_EQ(nodeType(node), QUERY_NODE_LOGIC_PLAN_PROJECT);
        const auto* branch = (const SProjectLogicNode*)node;
        bool hasTypedNull = false;
        SNode* projection = nullptr;
        FOREACH(projection, branch->pProjections) {
          if (nodeType(projection) != QUERY_NODE_VALUE ||
              !((const SValueNode*)projection)->isNull) {
            continue;
          }
          hasTypedNull = true;
          EXPECT_NE(((const SExprNode*)projection)->resType.type,
                     TSDB_DATA_TYPE_NULL);
          EXPECT_GT(((const SExprNode*)projection)->resType.bytes, 0);
        }
        EXPECT_TRUE(hasTypedNull);
      }
    });
    run("SELECT "
        "COUNT(*), LAST(c1), LAST_ROW(c2) FROM st " + clause);
    EXPECT_GT(countPhysiPlanText("PhysiLastRowScan"), 0);
    EXPECT_GT(countPhysiPlanText("PhysiTableScan"), 0);
  }
}

TEST_F(PlanCacheAggregateTest, GroupedCacheSplitKeepsFallbacks) {
  useCacheDb("cache_grouped_fallback");
  for (const string& suffix :
       {"PARTITION BY c1", "WHERE c1 > 0 PARTITION BY tbname",
        "WHERE ts > 0 PARTITION BY tbname", "PARTITION BY tag1 INTERVAL(1h)"}) {
    run("SELECT "
        "COUNT(*), LAST(c1) FROM st " + suffix);
    EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan"), 0);
  }
  run("SELECT _wstart, SUM(n) FROM "
      "(SELECT "
      "LAST_ROW(ts) AS ts, COUNT(*) AS n FROM st "
      "PARTITION BY tbname) INTERVAL(1s)");
  EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan"), 0);
}

TEST_F(PlanCacheAggregateTest,
       GroupedCacheSplitRejectsNondeterministicKeys) {
  useCacheDb("cache_grouped_volatile");
  for (const string& clause : {"PARTITION BY", "GROUP BY"}) {
    for (const string& key :
         {"tag1 + RAND()", "ABS(tag1 + RAND())", "tag1 + RAND(1)",
          "tag1, RAND()", "tag1 + SLEEP(0)", "tag1 + udf1(tag1)"}) {
      SCOPED_TRACE(clause + " " + key);
      run("SELECT "
          "COUNT(*), LAST(c1) FROM st " + clause + " " + key);
      EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan"), 0);
    }
  }
}

TEST_F(PlanCacheAggregateTest, GroupedCacheSplitKeepsStatementTimeKeys) {
  useCacheDb("cache_grouped_statement_time");
  for (const string& key :
       {"tag1 + CAST(NOW() AS BIGINT)",
        "tag1 + CAST(TODAY() AS BIGINT)"}) {
    SCOPED_TRACE(key);
    run("SELECT "
        "COUNT(*), LAST(c1) FROM st PARTITION BY " + key);
    EXPECT_GT(countPhysiPlanText("PhysiLastRowScan"), 0);
    EXPECT_GT(countPhysiPlanText("PhysiTableScan"), 0);
  }
}

TEST_F(PlanCacheAggregateTest, GroupedCacheSplitKeepsGroupLimits) {
  useCacheDb("cache_grouped_limits");
  for (const string& hint : {"", "/*+ NO_CONCURRENT_LAST_SCAN() */ "}) {
    SCOPED_TRACE(hint);
    run("SELECT " + hint + "tbname, COUNT(*), LAST(c1) FROM st "
        "PARTITION BY tbname SLIMIT 3 SOFFSET 1");
    EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan"), 0);
    EXPECT_GT(countPhysiPlanText("PhysiTableScan"), 0);
  }
}

TEST_F(PlanCacheAggregateTest, GroupedCachePartitionsRunBothAggregatesRemote) {
  useCacheDb("cache_grouped_remote");
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    auto* level = (SNodeListNode*)nodesListGetNode(plan->pSubplans, 0);
    ASSERT_NE(level, nullptr);
    ASSERT_EQ(nodeType(level), QUERY_NODE_NODE_LIST);
    auto* subplan = (SSubplan*)nodesListGetNode(level->pNodeList, 0);
    ASSERT_NE(subplan, nullptr);
    ASSERT_EQ(nodeType(subplan), QUERY_NODE_PHYSICAL_SUBPLAN);
    ASSERT_NE(subplan->pNode, nullptr);
    ASSERT_EQ(nodeType(subplan->pNode), QUERY_NODE_PHYSICAL_PLAN_HASH_AGG);
    const auto* parent = (const SAggPhysiNode*)subplan->pNode;
    EXPECT_EQ(LIST_LENGTH(parent->pGroupKeys), 1);
    EXPECT_EQ(parent->node.requireDataOrder, DATA_ORDER_LEVEL_NONE);
    ASSERT_EQ(LIST_LENGTH(parent->node.pChildren), 1);
    const auto* input = (const SPhysiNode*)nodesListGetNode(
        parent->node.pChildren, 0);
    ASSERT_NE(input, nullptr);
    ASSERT_EQ(nodeType(input), QUERY_NODE_PHYSICAL_PLAN_EXCHANGE);
    EXPECT_EQ(LIST_LENGTH(input->pOutputDataBlockDesc->pSlots), 3);
    const auto* exchange = (const SExchangePhysiNode*)input;
    EXPECT_LT(exchange->srcStartGroupId, exchange->srcEndGroupId);
  });
  run("SELECT "
      "tbname, COUNT(*), LAST(c1) FROM st PARTITION BY tbname");
  EXPECT_GT(countPhysiPlanText("PhysiLastRowScan"), 0);
  EXPECT_GT(countPhysiPlanText("PhysiTableScan"), 0);
}

TEST_F(PlanCacheAggregateTest,
       GroupedCacheSplitFinalizesBranchesBeforeParent) {
  useCacheDb("cache_grouped_finalize");
  for (const string& clause :
       {"PARTITION BY tag1", "GROUP BY tag1 % 2",
        "PARTITION BY tag1, tag1 % 2"}) {
    SCOPED_TRACE(clause);
    const int keyCount = clause == "PARTITION BY tag1, tag1 % 2" ? 2 : 1;
    setPhysiPlanCheck([keyCount](const SQueryPlan* plan) {
      const auto* level = (const SNodeListNode*)nodesListGetNode(
          plan->pSubplans, 0);
      ASSERT_NE(level, nullptr);
      const auto* parent = (const SSubplan*)nodesListGetNode(
          level->pNodeList, 0);
      ASSERT_NE(parent, nullptr);
      ASSERT_NE(parent->pNode, nullptr);
      ASSERT_EQ(nodeType(parent->pNode), QUERY_NODE_PHYSICAL_PLAN_HASH_AGG);
      const auto* final = (const SAggPhysiNode*)parent->pNode;
      EXPECT_EQ(LIST_LENGTH(final->pGroupKeys), keyCount);
      ASSERT_EQ(LIST_LENGTH(final->node.pChildren), 1);
      const auto* combined = (const SPhysiNode*)nodesListGetNode(
          final->node.pChildren, 0);
      ASSERT_NE(combined, nullptr);
      ASSERT_EQ(nodeType(combined), QUERY_NODE_PHYSICAL_PLAN_EXCHANGE);
      const auto* exchange = (const SExchangePhysiNode*)combined;
      EXPECT_LT(exchange->srcStartGroupId, exchange->srcEndGroupId);
      ASSERT_EQ(LIST_LENGTH(parent->pChildren), 2);

      int countBranches = 0;
      int cacheBranches = 0;
      SNode* node = nullptr;
      FOREACH(node, parent->pChildren) {
        const auto* branch = (const SSubplan*)node;
        SCOPED_TRACE(branch->id.groupId);
        EXPECT_GE(branch->id.groupId, exchange->srcStartGroupId);
        EXPECT_LE(branch->id.groupId, exchange->srcEndGroupId);
        bool count = false;
        bool cached = false;
        ASSERT_NO_FATAL_FAILURE(checkBranchGlobalAggregate(branch, keyCount,
                                                           count, cached));
        countBranches += count;
        cacheBranches += cached;
        ASSERT_NO_FATAL_FAILURE(checkBranchScanSources(branch, cached));
      }
      EXPECT_EQ(countBranches, 1);
      EXPECT_EQ(cacheBranches, 1);
    });
    run("SELECT "
        "COUNT(*), LAST(c1) FROM st " + clause);
  }
}

TEST_F(PlanCacheAggregateTest, GroupedCacheSplitKeepsHavingAndKeys) {
  useCacheDb("cache_grouped_having");
  setLogicPlanCheck([](const SLogicNode* root, bool optimized) {
    if (!optimized) return;
    const auto* parent = (const SAggLogicNode*)findNode(
        root, QUERY_NODE_LOGIC_PLAN_AGG);
    ASSERT_NE(parent, nullptr);
    EXPECT_EQ(LIST_LENGTH(parent->pGroupKeys), 2);
    EXPECT_NE(parent->node.pConditions, nullptr);
    ASSERT_EQ(LIST_LENGTH(parent->node.pChildren), 1);
    const auto* combined = (const SLogicNode*)nodesListGetNode(
        parent->node.pChildren, 0);
    ASSERT_NE(combined, nullptr);
    ASSERT_EQ(nodeType(combined), QUERY_NODE_LOGIC_PLAN_PROJECT);
    ASSERT_EQ(LIST_LENGTH(combined->pChildren), 2);
    SNode* node = nullptr;
    FOREACH(node, combined->pChildren) {
      const auto* branch = (const SAggLogicNode*)findNode(
          (const SLogicNode*)node, QUERY_NODE_LOGIC_PLAN_AGG);
      ASSERT_NE(branch, nullptr);
      EXPECT_EQ(branch->node.pConditions, nullptr);
    }
  });
  run("SELECT SUM(c1), LAST_ROW(c2) FROM st "
      "PARTITION BY tag1, tbname "
      "HAVING COUNT(*) > 0 AND LAST_ROW(c2) IS NOT NULL");
  EXPECT_GT(countPhysiPlanText("PhysiLastRowScan"), 0);
  EXPECT_GT(countPhysiPlanText("PhysiTableScan"), 0);
}

TEST_F(PlanCacheAggregateTest, GroupedCacheSplitRequiresCachedFunction) {
  useCacheDb("cache_grouped_function_guard");
  run("SELECT "
      "tbname, COUNT(*), LAST(c1+1) FROM st PARTITION BY tbname");
  EXPECT_EQ(countPhysiPlanText("PhysiLastRowScan"), 0);
  run("SELECT "
      "tbname, COUNT(*), LAST(c1+1), LAST_ROW(c2) "
      "FROM st PARTITION BY tbname");
  EXPECT_GT(countPhysiPlanText("PhysiLastRowScan"), 0);
}

TEST_F(PlanCacheAggregateTest, GroupedCacheTagExpressionKeepsMetadataScan) {
  useCacheDb("cache_grouped_metadata");
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    int scans = 0;
    std::function<void(const SPhysiNode*)> walk;
    walk = [&walk, &scans](const SPhysiNode* root) {
      if (nodeType(root) == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
        ++scans;
        const auto* scan = (const STableScanPhysiNode*)root;
        EXPECT_EQ(scan->dataRequired, FUNC_DATA_REQUIRED_NOT_LOAD);
      }
      SNode* child = nullptr;
      FOREACH(child, root->pChildren) walk((const SPhysiNode*)child);
    };
    SNode* level = nullptr;
    FOREACH(level, plan->pSubplans) {
      SNode* node = nullptr;
      FOREACH(node, ((SNodeListNode*)level)->pNodeList) {
        walk(((const SSubplan*)node)->pNode);
      }
    }
    EXPECT_GT(scans, 0);
  });
  run("SELECT "
      "tag1 % 2, COUNT(*), LAST(c1) FROM st GROUP BY tag1 % 2");
}

TEST_F(PlanCacheAggregateTest, GroupedCacheUnionKeepsBranchDependencies) {
  useCacheDb("cache_grouped_union");
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    std::function<void(const SPhysiNode*, const SSubplan*)> checkSources;
    checkSources = [&checkSources](const SPhysiNode* root,
                                    const SSubplan* subplan) {
      if (nodeType(root) == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
        const auto* exchange = (const SExchangePhysiNode*)root;
        bool found = false;
        SNode* child = nullptr;
        FOREACH(child, subplan->pChildren) {
          int32_t group = ((const SSubplan*)child)->id.groupId;
          found |= group >= exchange->srcStartGroupId &&
                   group <= exchange->srcEndGroupId;
        }
        EXPECT_TRUE(found);
      }
      SNode* child = nullptr;
      FOREACH(child, root->pChildren) {
        checkSources((const SPhysiNode*)child, subplan);
      }
    };
    std::function<bool(const SPhysiNode*)> hasParentAggregate;
    hasParentAggregate = [&hasParentAggregate](const SPhysiNode* root) {
      if (nodeType(root) == QUERY_NODE_PHYSICAL_PLAN_HASH_AGG &&
          LIST_LENGTH(((const SAggPhysiNode*)root)->pGroupKeys) > 0 &&
          LIST_LENGTH(root->pChildren) == 1) {
        const auto* input = (const SPhysiNode*)nodesListGetNode(
            root->pChildren, 0);
        if (nodeType(input) == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
          const auto* exchange = (const SExchangePhysiNode*)input;
          if (exchange->srcStartGroupId < exchange->srcEndGroupId) {
            return true;
          }
        }
      }
      SNode* child = nullptr;
      FOREACH(child, root->pChildren) {
        if (hasParentAggregate((const SPhysiNode*)child)) return true;
      }
      return false;
    };
    int parents = 0;
    SNode* level = nullptr;
    FOREACH(level, plan->pSubplans) {
      SNode* node = nullptr;
      FOREACH(node, ((SNodeListNode*)level)->pNodeList) {
        const auto* subplan = (const SSubplan*)node;
        checkSources(subplan->pNode, subplan);
        if (!hasParentAggregate(subplan->pNode)) continue;
        ++parents;
        EXPECT_GT(LIST_LENGTH(subplan->pChildren), 0);
      }
    }
    EXPECT_EQ(parents, 2);
  });
  for (const string& key : {"tbname", "tag1"}) {
    string sql = "SELECT " + key +
                 ", COUNT(*), LAST(c1) FROM st "
                 "PARTITION BY " + key;
    run(sql + " UNION ALL " + sql);
    EXPECT_GT(countPhysiPlanText("PhysiLastRowScan"), 0);
    EXPECT_GT(countPhysiPlanText("PhysiTableScan"), 0);
  }
}

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
