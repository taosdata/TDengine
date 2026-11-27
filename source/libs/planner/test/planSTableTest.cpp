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

#include <memory>
#include <set>

#include "planTestUtil.h"
#include "planner.h"

using namespace std;

class PlanSuperTableTest : public PlannerTestBase {};

TEST(PlanVirtualScanTest, MaterializationRetriesUnfinishedOrigins) {
  SNode* root = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN, &root));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> owned(root, nodesDestroyNode);
  auto* scan = reinterpret_cast<SVirtualScanLogicNode*>(root);
  scan->hasDeferredScanRange = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &scan->pDeferredPrimaryCond));
  reinterpret_cast<SColumnNode*>(scan->pDeferredPrimaryCond)->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_LIMIT, &scan->deferredSourceLimit));

  SScanLogicNode* sources[2] = {};
  for (auto& source : sources) {
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, reinterpret_cast<SNode**>(&source)));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&scan->pDeferredSourceScans, reinterpret_cast<SNode*>(source)));
  }
  auto addTimestamp = [](SScanLogicNode* source) {
    SNode* col = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &col));
    reinterpret_cast<SColumnNode*>(col)->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&source->pScanCols, col));
  };
  addTimestamp(sources[0]);
  ASSERT_EQ(TSDB_CODE_VTABLE_INVALID_ORIGIN_TS_COL, planMaterializeVirtualScanSources(scan));
  EXPECT_EQ(nullptr, scan->node.pChildren);
  EXPECT_EQ(TSDB_CODE_VTABLE_INVALID_ORIGIN_TS_COL, planMaterializeVirtualScanSources(scan));

  addTimestamp(sources[1]);
  ASSERT_EQ(TSDB_CODE_SUCCESS, planMaterializeVirtualScanSources(scan));
  ASSERT_EQ(2, LIST_LENGTH(scan->node.pChildren));
  EXPECT_EQ(nullptr, scan->pDeferredSourceScans);
  for (auto* source : sources) {
    ASSERT_NE(nullptr, source->pPrimaryCond);
    EXPECT_EQ(QUERY_NODE_COLUMN, nodeType(source->pPrimaryCond));
    EXPECT_NE(nullptr, source->node.pLimit);
    EXPECT_EQ(&scan->node, source->node.pParent);
  }
  EXPECT_EQ(TSDB_CODE_SUCCESS, planMaterializeVirtualScanSources(scan));
}

TEST(PlanVirtualScanTest, MaterializationKeepsLateTagInputAfterOrigins) {
  for (bool twoPass : {false, true}) {
    SCOPED_TRACE(twoPass);
    SNode* root = nullptr;
    ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN, &root));
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> owned(root, nodesDestroyNode);
    auto* scan = reinterpret_cast<SVirtualScanLogicNode*>(root);
    scan->node.precision = TSDB_TIME_PRECISION_MILLI;
    scan->twoPassMode = twoPass;
    scan->deferredScanOrder = twoPass ? ORDER_ASC : ORDER_DESC;
    scan->hasDeferredScanRange = true;
    scan->deferredScanRange = twoPass ? STimeWindow{TSKEY_MIN, 42} : STimeWindow{42, TSKEY_MAX};

    SScanLogicNode* origins[2] = {};
    for (auto& source : origins) {
      ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, reinterpret_cast<SNode**>(&source)));
      source->node.precision = TSDB_TIME_PRECISION_MICRO;
      ASSERT_EQ(0, nodesListMakeStrictAppend(&scan->pDeferredSourceScans, reinterpret_cast<SNode*>(source)));
    }
    SScanLogicNode* tag = nullptr;
    ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, reinterpret_cast<SNode**>(&tag)));
    tag->scanType = SCAN_TYPE_TAG;
    tag->node.pParent = &scan->node;
    scan->hasLocalTag = true;
    ASSERT_EQ(0, nodesListMakeStrictAppend(&scan->node.pChildren, reinterpret_cast<SNode*>(tag)));

    ASSERT_EQ(0, planMaterializeVirtualScanSources(scan));
    ASSERT_EQ(0, planMaterializeVirtualScanSources(scan));
    ASSERT_EQ(3, LIST_LENGTH(scan->node.pChildren));
    for (int32_t i = 0; i < 2; ++i) {
      const auto* source = origins[i];
      EXPECT_EQ(reinterpret_cast<const SNode*>(source), nodesListGetNode(scan->node.pChildren, i));
      EXPECT_EQ(twoPass ? TSKEY_MIN : 42000, source->scanRange.skey);
      EXPECT_EQ(twoPass ? 42000 : TSKEY_MAX, source->scanRange.ekey);
      EXPECT_EQ(scan->deferredScanOrder, source->node.outputTsOrder);
      EXPECT_EQ(twoPass, source->twoPassRescanSource);
      EXPECT_EQ(twoPass, source->scanSeq[0]);
      EXPECT_EQ(!twoPass, source->scanSeq[1]);
    }
    EXPECT_EQ(reinterpret_cast<SNode*>(tag), nodesListGetNode(scan->node.pChildren, 2));
    EXPECT_FALSE(tag->twoPassRescanSource);
  }
}

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

TEST_F(PlanSuperTableTest, multiVgroupScanSharesOneLeafPlan) {
  useDb("root", "test");
  runFullPlan("SELECT COUNT(*), AVG(c1) FROM st1", [](const SQueryPlan* plan) {
    int32_t templates = 0;
    std::set<int32_t> taskIds;
    SNode* level = nullptr;
    FOREACH(level, plan->pSubplans) {
      SNode* node = nullptr;
      FOREACH(node, ((SNodeListNode*)level)->pNodeList) {
        const SSubplan* subplan = (const SSubplan*)node;
        int32_t count = subplanGetTargetCount(subplan);
        if (count == 1) {
          EXPECT_TRUE(taskIds.insert(subplan->id.subplanId).second);
          continue;
        }
        ++templates;
        EXPECT_EQ(SUBPLAN_TYPE_SCAN, subplan->subplanType);
        EXPECT_EQ(0, LIST_LENGTH(subplan->pChildren));
        std::set<int32_t> vgroups;
        for (int32_t i = 0; i < count; ++i) {
          const auto* target = static_cast<const SSubplanTarget*>(taosArrayGet(subplan->pExecTargets, i));
          ASSERT_NE(nullptr, target);
          EXPECT_EQ(subplan->id.subplanId + i, target->subplanId);
          EXPECT_TRUE(taskIds.insert(target->subplanId).second);
          vgroups.insert(target->execNode.nodeId);
        }
        EXPECT_GT(vgroups.size(), 1U);
      }
    }
    EXPECT_EQ(1, templates);
    EXPECT_EQ(static_cast<size_t>(plan->numOfSubplans), taskIds.size());

    for (bool json : {false, true}) {
      SCOPED_TRACE(json ? "JSON" : "TLV");
      char* wire = nullptr;
      int32_t len = 0;
      ASSERT_EQ(0, json ? nodesNodeToString((SNode*)plan, false, &wire, &len)
                        : nodesNodeToMsg((SNode*)plan, &wire, &len));
      SNode* decoded = nullptr;
      int32_t code = json ? nodesStringToNode(wire, &decoded) : nodesMsgToNode(wire, len, &decoded);
      taosMemoryFree(wire);
      ASSERT_EQ(0, code);
      auto* exported = (SQueryPlan*)decoded;
      std::set<int32_t> exportedIds;
      FOREACH(level, exported->pSubplans) {
        SNode* node = nullptr;
        FOREACH(node, ((SNodeListNode*)level)->pNodeList) {
          const SSubplan* subplan = (const SSubplan*)node;
          EXPECT_EQ(nullptr, subplan->pExecTargets);
          exportedIds.insert(subplan->id.subplanId);
          if (subplan->level == 0) EXPECT_EQ(plan->numOfSubplans - 1, LIST_LENGTH(subplan->pChildren));
        }
      }
      EXPECT_EQ(taskIds, exportedIds);
      nodesDestroyNode(decoded);
    }
  });
}

TEST_F(PlanSuperTableTest, exportedRootCountsRetainedSubqueryTasks) {
  useDb("root", "test");
  runFullPlan("SELECT AVG(c1) FROM st1 WHERE c1 > (SELECT AVG(c1) FROM t1)", [](const SQueryPlan* plan) {
    ASSERT_GT(LIST_LENGTH(plan->pChildren), 0);
    for (bool json : {false, true}) {
      SCOPED_TRACE(json ? "JSON" : "TLV");
      char* wire = nullptr;
      int32_t len = 0;
      ASSERT_EQ(0, json ? nodesNodeToString((SNode*)plan, false, &wire, &len)
                        : nodesNodeToMsg((SNode*)plan, &wire, &len));
      SNode* decoded = nullptr;
      int32_t code = json ? nodesStringToNode(wire, &decoded) : nodesMsgToNode(wire, len, &decoded);
      taosMemoryFree(wire);
      ASSERT_EQ(0, code);
      const auto* dag = (const SQueryPlan*)decoded;
      EXPECT_EQ(plan->numOfSubplans, dag->numOfSubplans);
      EXPECT_EQ(LIST_LENGTH(plan->pChildren), LIST_LENGTH(dag->pChildren));
      nodesDestroyNode(decoded);
    }
  });
}
