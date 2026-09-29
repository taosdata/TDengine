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

#include <fstream>
#include <map>
#include <set>
#include <sstream>

#include "planTestUtil.h"

#include "functionMgt.h"
#include "mockCatalogService.h"
#include "planner.h"
#include "streamMsg.h"

using namespace std;

class PlanJoinTest : public PlannerTestBase {};

TEST_F(PlanJoinTest, basic) {
  useDb("root", "test");

  run("SELECT t1.c1, t2.c2 FROM st1s1 t1, st1s2 t2 WHERE t1.ts = t2.ts");

  run("SELECT t1.*, t2.* FROM st1s1 t1, st1s2 t2 WHERE t1.ts = t2.ts");

  run("SELECT t1.c1, t2.c1 FROM st1s1 t1 JOIN st1s2 t2 ON t1.ts = t2.ts");

  run("SELECT t1.c1, t2.c1 FROM st1 t1 JOIN st2 t2 ON t1.ts = t2.ts");
}

TEST_F(PlanJoinTest, complex) {
  useDb("root", "test");

  run("SELECT t1.c1, t2.c2 FROM st1s1 t1, st1s2 t2 "
      "WHERE t1.ts = t2.ts AND t1.c1 BETWEEN -10 AND 10 AND t2.c1 BETWEEN -100 AND 100 AND "
      "(t1.c2 LIKE 'nchar%' OR t1.c1 = 0 OR t2.c2 LIKE 'nchar%' OR t2.c1 = 0)");
}

TEST_F(PlanJoinTest, withWhere) {
  useDb("root", "test");

  run("SELECT t1.c1, t2.c1 FROM st1s1 t1 JOIN st1s2 t2 ON t1.ts = t2.ts "
      "WHERE t1.c1 > t2.c1 AND t1.c2 = 'abc' AND t2.c2 = 'qwe'");
}

TEST_F(PlanJoinTest, withAggAndOrderBy) {
  useDb("root", "test");

  run("SELECT t1.ts, TOP(t2.c1, 10) FROM st1s1 t1 JOIN st1s2 t2 ON t1.ts = t2.ts ORDER BY t2.ts");
}

TEST_F(PlanJoinTest, multiJoin) {
  useDb("root", "test");

  run("SELECT t1.c1, t2.c1 FROM st1s1 t1 JOIN st1s2 t2 ON t1.ts = t2.ts JOIN st1s3 t3 ON t1.ts = t3.ts");

  // Candidate discovery must not rewrite an inner join that is inspected
  // before the outer hash-join candidate is selected.
  run("SELECT t1.c1, t2.c1, t3.c1 "
      "FROM st1s1 t1 JOIN st1s2 t2 ON t1.ts = t2.ts "
      "JOIN st1s3 t3 ON t1.c1 = t3.c1");

  run("SELECT /*+ merge_join() */ t1.c1, t2.c1, t3.c1 "
      "FROM st1s1 t1 JOIN st1s2 t2 ON t1.ts = t2.ts "
      "JOIN st1s3 t3 ON t1.c1 = t3.c1");
  ASSERT_EQ(1, countPlanNode("PhysiMergeJoin"));
  ASSERT_EQ(1, countPlanNode("PhysiHashJoin"));

  // Exercise the opposite nesting/order so candidate selection is not tied
  // to one join-tree traversal shape.
  run("SELECT t1.c1, t2.c1, t3.c1 "
      "FROM st1s1 t1 JOIN (st1s2 t2 JOIN st1s3 t3 ON t2.ts = t3.ts) "
      "ON t1.ts = t2.ts");
}

namespace {
void checkHashJoinInputKeys(const SPhysiNode* node, int* joins, int* thetaJoins = nullptr) {
  if (nodeType(node) == QUERY_NODE_PHYSICAL_PLAN_THETA_JOIN) {
    if (thetaJoins != nullptr) ++*thetaJoins;
    const auto* join = reinterpret_cast<const SThetaJoinPhysiNode*>(node);
    ASSERT_EQ(2, LIST_LENGTH(node->pChildren));
    const auto* left = reinterpret_cast<const SPhysiNode*>(nodesListGetNode(node->pChildren, 0));
    const auto* right = reinterpret_cast<const SPhysiNode*>(nodesListGetNode(node->pChildren, 1));
    const int64_t leftBlockId = left->pOutputDataBlockDesc->dataBlockId;
    const int64_t rightBlockId = right->pOutputDataBlockDesc->dataBlockId;
    EXPECT_EQ(leftBlockId, join->leftPrimSlotId);
    EXPECT_EQ(rightBlockId, join->rightPrimSlotId);
    EXPECT_GT(LIST_LENGTH(join->pTargets), 0);
    SNode* target = nullptr;
    FOREACH(target, join->pTargets) {
      ASSERT_EQ(QUERY_NODE_TARGET, nodeType(target));
      const SNode* expr = reinterpret_cast<STargetNode*>(target)->pExpr;
      ASSERT_NE(nullptr, expr);
      EXPECT_EQ(QUERY_NODE_COLUMN, nodeType(expr));
      if (nodeType(expr) == QUERY_NODE_COLUMN) {
        const auto* column = reinterpret_cast<const SColumnNode*>(expr);
        EXPECT_TRUE(column->dataBlockId == leftBlockId || column->dataBlockId == rightBlockId);
      }
    }
  }
  if (nodeType(node) == QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN) {
    ++*joins;
    const auto* join = reinterpret_cast<const SHashJoinPhysiNode*>(node);
    ASSERT_EQ(2, LIST_LENGTH(node->pChildren));
    const auto* left = reinterpret_cast<const SPhysiNode*>(nodesListGetNode(node->pChildren, 0));
    const auto* right = reinterpret_cast<const SPhysiNode*>(nodesListGetNode(node->pChildren, 1));
    for (int side = 0; side < 2; ++side) {
      SNode* key = nullptr;
      FOREACH(key, side == 0 ? join->pOnLeft : join->pOnRight) {
        ASSERT_EQ(QUERY_NODE_COLUMN, nodeType(key));
        const auto* column = reinterpret_cast<const SColumnNode*>(key);
        EXPECT_EQ(column->dataBlockId,
                  (side == 0 ? left : right)->pOutputDataBlockDesc->dataBlockId);
      }
    }
  }
  SNode* child = nullptr;
  FOREACH(child, node->pChildren) {
    checkHashJoinInputKeys(reinterpret_cast<const SPhysiNode*>(child), joins, thetaJoins);
  }
}

void checkPlanHashJoinInputs(const SQueryPlan* plan) {
  int joins = 0;
  SNode* level = nullptr;
  FOREACH(level, plan->pSubplans) {
    SNode* subplan = nullptr;
    FOREACH(subplan, reinterpret_cast<SNodeListNode*>(level)->pNodeList) {
      checkHashJoinInputKeys(reinterpret_cast<SSubplan*>(subplan)->pNode, &joins);
    }
  }
  EXPECT_GT(joins, 0);
}
}  // namespace

TEST_F(PlanJoinTest, tbnameKeysStayOnJoinInputs) {
  useDb("root", "test");
  runFullPlan("SELECT /*+ hash_join() */ a.c1, b.c1 FROM st1 a JOIN st2 b ON a.tbname = b.tbname",
              checkPlanHashJoinInputs);
  runFullPlan("SELECT /*+ hash_join() */ a.c1, b.c1 FROM st1 a JOIN st2 b ON b.tbname = a.tbname",
              checkPlanHashJoinInputs);
  runFullPlan("SELECT /*+ hash_join() */ a.c1, b.c1, c.c1 FROM st1 a JOIN st2 b ON a.tbname = b.tbname "
              "JOIN st1 c ON b.tbname = c.tbname", checkPlanHashJoinInputs);
}

TEST_F(PlanJoinTest, mergeJoinWrappedTimestampSort) {
  useDb("root", "test");
  run("SELECT /*+ merge_join() */ a.c1, b.c1 FROM st1 a JOIN st2 b "
      "ON timetruncate(a.ts, 1s) = timetruncate(b.ts, 1s)");
  ASSERT_EQ(1, countPlanNode("PhysiMergeJoin"));
}

TEST_F(PlanJoinTest, thetaProjectionAndWindowPlans) {
  useDb("root", "test");
  auto inspect = [](const SQueryPlan* plan) {
    int hashJoins = 0;
    int thetaJoins = 0;
    SNode* level = nullptr;
    FOREACH(level, plan->pSubplans) {
      SNode* subplan = nullptr;
      FOREACH(subplan, reinterpret_cast<SNodeListNode*>(level)->pNodeList) {
        checkHashJoinInputKeys(reinterpret_cast<SSubplan*>(subplan)->pNode, &hashJoins, &thetaJoins);
      }
    }
    EXPECT_GT(thetaJoins, 0);
  };
  runFullPlan("SELECT /*+ join_algo(theta) */ a.c1 + b.c1 AS total FROM st1s1 a JOIN st1s2 b ON a.c1 < b.c1",
              inspect);
  runFullPlan("SELECT /*+ join_algo(theta) */ _wstart, count(*) FROM st1s1 a JOIN st1s2 b "
              "ON a.c1 < b.c1 INTERVAL(1s)", inspect);
}

TEST_F(PlanJoinTest, tbnameConditionOnly) {
  useDb("root", "test");
  run("SELECT c1 FROM st1 WHERE tbname = 'st1s1'");
  run("SELECT /*+ hash_join() */ a.c1, b.c1 FROM st1 a JOIN st2 b ON a.c1 = b.c1 "
      "WHERE a.tbname = 'st1s1'");
}

TEST_F(PlanJoinTest, StreamNestedOnOnlyColumnsBecomeCompositeKeys) {
  useDb("root", "stream_streamdb");
  for (const char* equality : {"b.c1=c.c1", "c.c1=b.c1"}) {
    SCOPED_TRACE(equality);
    runCreateStream(string("create stream stream_streamdb.s_keys interval(1s) sliding(1s) ") +
                    "from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as "
                    "select a.ts, a.c1 from (select _twend ts, c1 from stream_querydb.stream_t1) a "
                    "join (select _twend ts, c1 from stream_querydb.stream_t2) b on a.ts=b.ts "
                    "join (select _twend ts, c1 from stream_querydb.stream_t1) c on b.ts=c.ts and " +
                    equality +
                    " join (select _twend ts, c1 from stream_querydb.stream_t2) d on b.ts=d.ts and b.c1=d.c1");
    int joins = 0, composite = 0;
    checkHashJoins([&](const SHashJoinPhysiNode& join) {
      ++joins;
      EXPECT_TRUE(join.multiGroupJoin);
      EXPECT_EQ(nullptr, join.pFullOnCond);
      EXPECT_EQ(LIST_LENGTH(join.pOnLeft), LIST_LENGTH(join.pOnRight));
      if (LIST_LENGTH(join.pOnLeft) == 2) {
        ++composite;
        EXPECT_STREQ("c1", reinterpret_cast<SColumnNode*>(nodesListGetNode(join.pOnLeft, 1))->colName);
        EXPECT_STREQ("c1", reinterpret_cast<SColumnNode*>(nodesListGetNode(join.pOnRight, 1))->colName);
      }
    });
    EXPECT_EQ(3, joins);
    EXPECT_EQ(2, composite);
  }
}

TEST_F(PlanJoinTest, NestedOnOnlyTagAndColumnBecomeCompositeKeys) {
  useDb("root", "test");
  run("select /*+ hash_join() */ a.ts, a.c1 from st1 a join st1 b on a.ts=b.ts "
      "join st1 c on b.ts=c.ts and b.tag1=c.c1");
  int joins = 0, composite = 0;
  checkHashJoins([&](const SHashJoinPhysiNode& join) {
    ++joins;
    EXPECT_EQ(nullptr, join.pFullOnCond);
    if (LIST_LENGTH(join.pOnLeft) == 2 && LIST_LENGTH(join.pOnRight) == 2) ++composite;
  });
  EXPECT_EQ(2, joins);
  EXPECT_EQ(1, composite);
}

class PlanBroadcastReuseTest : public testing::Test {
 protected:
  SSubplan*                 runner = nullptr;
  SNodeList*                readers = nullptr;
  SExternalWindowPhysiNode* window = nullptr;
  SExchangePhysiNode*       exchange = nullptr;

  template <typename T>
  T* make(ENodeType type) {
    SNode* node = nullptr;
    EXPECT_EQ(0, nodesMakeNode(type, &node));
    return reinterpret_cast<T*>(node);
  }

  void SetUp() override {
    runner = make<SSubplan>(QUERY_NODE_PHYSICAL_SUBPLAN);
    window = make<SExternalWindowPhysiNode>(QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW);
    exchange = make<SExchangePhysiNode>(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE);
    runner->pNode = &window->window.node;
    window->extWinInputMode = EXT_WIN_INPUT_BROADCAST;
    exchange->srcStartGroupId = exchange->srcEndGroupId = 4;
    ASSERT_EQ(0, nodesListMakeAppend(&window->window.node.pChildren, reinterpret_cast<SNode*>(exchange)));
    addReader(14);
  }

  void TearDown() override {
    nodesDestroyNode(reinterpret_cast<SNode*>(runner));
    nodesDestroyList(readers);
  }

  SSubplan* addReader(int32_t id) {
    auto* reader = make<SSubplan>(QUERY_NODE_PHYSICAL_SUBPLAN);
    reader->subplanType = SUBPLAN_TYPE_SCAN;
    reader->id.groupId = 4;
    reader->id.subplanId = id;
    reader->requiresAncestorContext = true;
    auto* scan = make<STableScanPhysiNode>(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
    reader->pNode = &scan->scan.node;
    scan->scan.uid = scan->scan.suid = 42;
    scan->scan.tableType = TSDB_SUPER_TABLE;
    scan->scanSeq[0] = 1;
    scan->ratio = 1;
    scan->scanRange = {INT64_MIN, INT64_MAX};
    auto* col = make<SColumnNode>(QUERY_NODE_COLUMN);
    col->colType = COLUMN_TYPE_COLUMN;
    col->tableId = 42;
    col->colId = 2;
    SNode* scanCol = nullptr;
    EXPECT_EQ(0, nodesCloneNode(reinterpret_cast<SNode*>(col), &scanCol));
    EXPECT_EQ(0, nodesListMakeAppend(&scan->scan.pScanCols, scanCol));
    auto* val = make<SValueNode>(QUERY_NODE_VALUE);
    val->translate = true;
    val->node.resType.type = TSDB_DATA_TYPE_INT;
    val->datum.i = 300102;
    auto* op = make<SOperatorNode>(QUERY_NODE_OPERATOR);
    op->opType = OP_TYPE_EQUAL;
    op->pLeft = reinterpret_cast<SNode*>(col);
    op->pRight = reinterpret_cast<SNode*>(val);
    scan->scan.node.pConditions = reinterpret_cast<SNode*>(op);
    EXPECT_EQ(0, nodesListMakeAppend(&readers, reinterpret_cast<SNode*>(reader)));
    auto* ref = make<SValueNode>(QUERY_NODE_VALUE);
    ref->translate = true;
    ref->node.resType.type = TSDB_DATA_TYPE_BIGINT;
    ref->datum.i = (INT64_C(4) << 32) | id;
    EXPECT_EQ(0, nodesListMakeAppend(&runner->pChildren, reinterpret_cast<SNode*>(ref)));
    return reader;
  }

  STableScanPhysiNode* scan() {
    return reinterpret_cast<STableScanPhysiNode*>(reinterpret_cast<SSubplan*>(nodesListGetNode(readers, 0))->pNode);
  }

  void check(bool expected) {
    // Re-derivation must also clear a stale serialized true bit.
    for (bool initial : {false, true}) {
      window->broadcastInputReusable = initial;
      ASSERT_EQ(0, qSetStreamBroadcastReuse(runner, readers));
      EXPECT_EQ(expected, window->broadcastInputReusable);
    }
  }
};

TEST_F(PlanBroadcastReuseTest, FixedFilterAcrossAllVgroups) {
  addReader(15);
  addReader(16);
  addReader(17);
  // Replay metadata is outside the materialized raw input.
  auto* fn = make<SFunctionNode>(QUERY_NODE_FUNCTION);
  strcpy(fn->functionName, "_twend");
  ASSERT_EQ(0, nodesListMakeAppend(&window->window.pProjs, reinterpret_cast<SNode*>(fn)));
  check(true);
}

TEST_F(PlanBroadcastReuseTest, SameTableDifferentScanIdentity) {
  auto* dependent = addReader(15);
  auto* s = reinterpret_cast<STableScanPhysiNode*>(dependent->pNode);
  s->pPrimaryCond = reinterpret_cast<SNode*>(make<SFunctionNode>(QUERY_NODE_FUNCTION));
  check(false);
  // Remove the dependent identity from this Exchange's group.
  dependent->id.groupId = 6;
  auto* ref = reinterpret_cast<SValueNode*>(nodesListGetNode(runner->pChildren, 1));
  ref->datum.i = (INT64_C(6) << 32) | 15;
  check(true);
}

TEST_F(PlanBroadcastReuseTest, RejectsMissingAndDuplicateIdentity) {
  auto* reader = reinterpret_cast<SSubplan*>(nodesListGetNode(readers, 0));
  reader->id.subplanId = 15;
  check(false);
  reader->id.subplanId = 14;
  SNode* duplicate = nullptr;
  ASSERT_EQ(0, nodesCloneNode(reinterpret_cast<SNode*>(reader), &duplicate));
  ASSERT_EQ(0, nodesListMakeAppend(&readers, duplicate));
  check(false);
}

TEST_F(PlanBroadcastReuseTest, RejectsMissingReferences) {
  nodesDestroyList(runner->pChildren);
  runner->pChildren = nullptr;
  check(false);
}

TEST_F(PlanBroadcastReuseTest, RejectsGroupAndAncestorAndNondeterministicExpressions) {
  auto* op = reinterpret_cast<SOperatorNode*>(scan()->scan.node.pConditions);
  nodesDestroyNode(op->pRight);
  auto* fn = make<SFunctionNode>(QUERY_NODE_FUNCTION);
  op->pRight = reinterpret_cast<SNode*>(fn);
  for (const char* name : {"_placeholder_column", "_twstart", "_twend", "rand", "now"}) {
    strcpy(fn->functionName, name);
    check(false);
  }
}

TEST_F(PlanBroadcastReuseTest, RejectsDynamicSourcesAndUnknownPlans) {
  auto* reader = reinterpret_cast<SSubplan*>(nodesListGetNode(readers, 0));
  reader->dynTbname = true;
  check(false);
  reader->dynTbname = false;
  scan()->scan.useTriggerTableList = true;
  check(false);
  scan()->scan.useTriggerTableList = false;
  scan()->scan.virtualStableScan = true;
  check(false);
  scan()->scan.virtualStableScan = false;
  scan()->scan.node.dynamicOp = true;
  check(false);
  scan()->scan.node.dynamicOp = false;
  reader->pNode->type = QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN;
  check(false);
  reader->pNode->type = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
}

TEST_F(PlanBroadcastReuseTest, RejectsPushedDependencies) {
  SNode** slots[] = {&scan()->pPrimaryCond,
                     &scan()->pTimeRange,
                     &scan()->pExtTimeRange,
                     &scan()->pSubtable,
                     &reinterpret_cast<SSubplan*>(nodesListGetNode(readers, 0))->pTagCond,
                     &reinterpret_cast<SSubplan*>(nodesListGetNode(readers, 0))->pTagIndexCond};
  for (auto** slot : slots) {
    *slot = reinterpret_cast<SNode*>(make<SFunctionNode>(QUERY_NODE_FUNCTION));
    check(false);
    nodesDestroyNode(*slot);
    *slot = nullptr;
  }
  SNodeList** lists[] = {&scan()->pDynamicScanFuncs, &scan()->pGroupTags, &scan()->pTags,
                         &scan()->scan.pScanPseudoCols};
  for (auto** list : lists) {
    ASSERT_EQ(0, nodesListMakeAppend(list, reinterpret_cast<SNode*>(make<SFunctionNode>(QUERY_NODE_FUNCTION))));
    check(false);
    nodesDestroyList(*list);
    *list = nullptr;
  }
}

TEST_F(PlanBroadcastReuseTest, RejectsNonScalarAndNonBroadcast) {
  window->window.indefRowsFunc = true;
  check(false);
  window->window.indefRowsFunc = false;
  window->extWinInputMode = EXT_WIN_INPUT_WINDOW_BOUND;
  check(false);
  window->extWinInputMode = EXT_WIN_INPUT_BROADCAST;
  window->calcWithPartition = true;
  check(false);
}

TEST_F(PlanBroadcastReuseTest, RejectsOtherQueryAndDuplicateReference) {
  auto* reader = reinterpret_cast<SSubplan*>(nodesListGetNode(readers, 0));
  reader->id.queryId = 99;
  check(false);
  reader->id.queryId = 0;
  SNode* duplicate = nullptr;
  ASSERT_EQ(0, nodesCloneNode(nodesListGetNode(runner->pChildren, 0), &duplicate));
  ASSERT_EQ(0, nodesListMakeAppend(&runner->pChildren, duplicate));
  check(false);
}

// Optional real pre-upgrade artifacts supplement the hermetic identity tests.
TEST(PlanBroadcastReuseFixtureTest, PersistedCustomerPlan) {
  const char* runnerPath = getenv("TD_BROADCAST_RUNNER_FIXTURE");
  const char* readerPath = getenv("TD_BROADCAST_READER_FIXTURE");
  if (!runnerPath || !readerPath) GTEST_SKIP();
  auto read = [](const char* path) {
    std::ifstream     file(path);
    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
  };
  SNodeList*        queries = nullptr;
  SNodeList*        readers = nullptr;
  const std::string persisted = read(runnerPath);
  ASSERT_EQ(std::string::npos, persisted.find("BroadcastInputReusable"));
  ASSERT_EQ(0, nodesStringToList(persisted.c_str(), &queries));
  std::unique_ptr<SNodeList, decltype(&nodesDestroyList)> plan(queries, nodesDestroyList);
  ASSERT_EQ(0, nodesStringToList(read(readerPath).c_str(), &readers));
  std::unique_ptr<SNodeList, decltype(&nodesDestroyList)> owned(readers, nodesDestroyList);
  ASSERT_EQ(1, LIST_LENGTH(queries));
  ASSERT_EQ(20, LIST_LENGTH(readers));
  int32_t                          accepted = 0, dependent = 0;
  std::function<void(SPhysiNode*)> check = [&](SPhysiNode* node) {
    if (nodeType(node) == QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW ||
        nodeType(node) == QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL) {
      auto* window = reinterpret_cast<SExternalWindowPhysiNode*>(node);
      auto* child = nodesListGetNode(node->pChildren, 0);
      if (child && nodeType(child) == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
        auto* exchange = reinterpret_cast<SExchangePhysiNode*>(child);
        bool  expected = exchange->srcStartGroupId == 4 || exchange->srcStartGroupId == 5;
        EXPECT_EQ(expected, window->broadcastInputReusable) << exchange->srcStartGroupId;
        std::cout << "Exchange group=" << exchange->srcStartGroupId << " reusable=" << window->broadcastInputReusable
                  << std::endl;
        if (expected) ++accepted;
        if (exchange->srcStartGroupId == 2) ++dependent;
      }
    }
    SNode* child = nullptr;
    FOREACH(child, node->pChildren) { check(reinterpret_cast<SPhysiNode*>(child)); }
  };
  std::function<void(SQueryPlan*)> visit = [&](SQueryPlan* query) {
    SNode* level = nullptr;
    FOREACH(level, query->pSubplans) {
      SNode* sub = nullptr;
      FOREACH(sub, reinterpret_cast<SNodeListNode*>(level)->pNodeList) {
        auto* runner = reinterpret_cast<SSubplan*>(sub);
        ASSERT_EQ(0, qSetStreamBroadcastReuse(runner, readers));
        check(runner->pNode);
      }
    }
    SNode* child = nullptr;
    FOREACH(child, query->pChildren) { visit(reinterpret_cast<SQueryPlan*>(child)); }
  };
  SNode* query = nullptr;
  FOREACH(query, queries) { visit(reinterpret_cast<SQueryPlan*>(query)); }
  EXPECT_EQ(2, accepted);
  EXPECT_EQ(1, dependent);
}

TEST_F(PlanBroadcastReuseTest, RejectsMalformedValuesAndScanIdentity) {
  auto* op = reinterpret_cast<SOperatorNode*>(scan()->scan.node.pConditions);
  auto* val = reinterpret_cast<SValueNode*>(op->pRight);
  val->translate = false;
  check(false);
  val->translate = true;
  val->flag = VALUE_FLAG_VAL_UNSET;
  check(false);
  val->flag = 0;
  auto* col = reinterpret_cast<SColumnNode*>(op->pLeft);
  col->colType = COLUMN_TYPE_PLACE_HOLDER;
  check(false);
  col->colType = COLUMN_TYPE_COLUMN;
  col->tableId = 999;
  check(false);
  col->tableId = 42;
  nodesDestroyNode(op->pRight);
  op->pRight = nullptr;
  check(false);
}

TEST_F(PlanBroadcastReuseTest, HashExternalUsesSameScalarProof) {
  window->window.node.type = QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL;
  check(true);
  exchange->dynTbname = true;
  check(false);
  exchange->dynTbname = false;
  // A distinct merge-aligned executor does not consume this qualification.
  window->window.node.type = QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL;
  check(false);
}

TEST_F(PlanJoinTest, BroadcastScalarPlaceholderFamilies) {
  useDb("root", "stream_streamdb");
  for (const char* projection : {"_tgrpid", "%%1", "%%tbname", "_tlocaltime, _tgrpid", "c1, _tgrpid, %%1, %%tbname"}) {
    SCOPED_TRACE(projection);
    runCreateStream(string("create stream stream_streamdb.s_broadcast interval(1s) sliding(1s) ") +
                    "from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select " +
                    (string(projection) == "_tlocaltime, _tgrpid" ? "" : "ts, ") + projection +
                    " from stream_querydb.stream_t2");
    EXPECT_TRUE(findPlanNode("ExternalWindow"));
    EXPECT_TRUE(planContains("\"ExtWinInputMode\":\"1\""));
  }
}

TEST_F(PlanJoinTest, BroadcastTriggerTimePlaceholders) {
  useDb("root", "stream_streamdb");
  runCreateStream(
      "create stream stream_streamdb.s_broadcast sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _tprev_ts, _tcurrent_ts, _tnext_ts, c1 from stream_querydb.stream_t2");
  EXPECT_TRUE(findPlanNode("ExternalWindow"));
}

TEST_F(PlanJoinTest, BroadcastIdlePlaceholders) {
  useDb("root", "stream_streamdb");
  runCreateStream(
      "create stream stream_streamdb.s_broadcast interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(idle)) "
      "into stream_outdb.stream_out as select _tidlestart, _tidleend, c1 from stream_querydb.stream_t2");
  EXPECT_TRUE(findPlanNode("ExternalWindow"));
}

TEST_F(PlanJoinTest, BroadcastRollupPlaceholders) {
  useDb("root", "stream_streamdb");
  runCreateStream(
      "create stream stream_streamdb.s_broadcast interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 rollup by tag2 into stream_outdb.stream_out as "
      "select ts, %%rollup_tag, _trollup_tbcount, %%1, c1 from stream_querydb.stream_t2");
  EXPECT_TRUE(findPlanNode("ExternalWindow"));
}

TEST_F(PlanJoinTest, BroadcastAncestorProjection) {
  useDb("root", "stream_streamdb");
  runCreateStream(
      "create stream stream_streamdb.s_broadcast window ("
      "state_window(c1) extend(1) as w_order, count_window(2,1)) from stream_triggerdb.st1 partition by tbname "
      "into stream_outdb.stream_out as select w_order._twstart, c1 from stream_querydb.stream_t2");
  EXPECT_TRUE(findPlanNode("ExternalWindow"));
}

TEST_F(PlanJoinTest, BroadcastJoinRequiresCurrentWindowKey) {
  useDb("root", "stream_streamdb");
  const string prefix =
      "create stream stream_streamdb.s_broadcast window ("
      "state_window(c1) extend(1) as w_order, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as "
      "select a.parent_ts, a.current_ts, a.c1 from ";
  for (const char* key : {"_twstart", "_twend"}) {
    SCOPED_TRACE(key);
    const string projection = string("select w_order.") + key + " as parent_ts, " + key + " as current_ts, c1 ";
    const string sources = "(" + projection + "from stream_querydb.stream_t1) a join (" + projection +
                           "from stream_querydb.stream_t2) b on ";
    runCreateStream(prefix + sources + "a.parent_ts = b.parent_ts");
    EXPECT_FALSE(findPlanNode("ExternalWindow"));
    EXPECT_FALSE(planContains("\"MultiGroupJoin\":true"));
    runCreateStream(prefix + sources + "a.current_ts = b.current_ts and a.parent_ts = b.parent_ts");
    EXPECT_EQ(3, countPlanNode("ExternalWindow"));
    EXPECT_TRUE(planContains("\"MultiGroupJoin\":true"));
  }
  runCreateStream(prefix +
                  "(select w_order._twstart as parent_ts, w_leaf._twstart as current_ts, c1 "
                  "from stream_querydb.stream_t1) a join (select w_leaf._twstart as current_ts "
                  "from stream_querydb.stream_t2) b on a.current_ts = b.current_ts");
  EXPECT_FALSE(planContains("\"MultiGroupJoin\":true"));
}

TEST_F(PlanJoinTest, BroadcastPeriodPlaceholders) {
  useDb("root", "stream_streamdb");
  runCreateStream(
      "create stream stream_streamdb.s_broadcast period(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _tprev_localtime, _tnext_localtime, _tlocaltime, c1 from stream_querydb.stream_t2");
  EXPECT_TRUE(findPlanNode("ExternalWindow"));
}

TEST_F(PlanJoinTest, BroadcastKeepsOrdinaryProjectionScope) {
  useDb("root", "stream_streamdb");
  for (const char* projection : {"ts, c1", "ts, abs(c1), _tgrpid", "ts, c1 + 1, _tgrpid"}) {
    SCOPED_TRACE(projection);
    runCreateStream(string("create stream stream_streamdb.s_broadcast interval(1s) sliding(1s) ") +
                    "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select " + projection +
                    " from stream_querydb.stream_t2");
    EXPECT_FALSE(findPlanNode("ExternalWindow"));
  }
}

namespace {
const string localJoinPrefix =
    "create stream stream_streamdb.s_local window ("
    "state_window(c1) extend(1) as w_order,count_window(2,1)) "
    "from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as ";
const string ancestorInput = "select _twend ts,max(c1) seen from %%tbname where ts>=w_order._twstart and ts<=_twstart";
const string leafRange = " where ts>=_twstart and ts<=_twend";

string localJoinSql(const string& right) {
  return localJoinPrefix + "select b.*,a.seen from (" + ancestorInput + ") a join (" + right + ") b on a.ts=b.ts";
}

void visitCalcSubplans(const SQueryPlan& plan, const function<void(const SSubplan&)>& check) {
  SNode* level = nullptr;
  FOREACH(level, plan.pSubplans) {
    SNode* sub = nullptr;
    FOREACH(sub, reinterpret_cast<SNodeListNode*>(level)->pNodeList) {
      ASSERT_EQ(QUERY_NODE_PHYSICAL_SUBPLAN, nodeType(sub));
      check(*reinterpret_cast<SSubplan*>(sub));
    }
  }
  SNode* child = nullptr;
  FOREACH(child, plan.pChildren) {
    ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN, nodeType(child));
    visitCalcSubplans(*reinterpret_cast<SQueryPlan*>(child), check);
  }
}

void visitOperators(const SPhysiNode* node, const function<void(const SPhysiNode&)>& check) {
  ASSERT_NE(nullptr, node);
  check(*node);
  SNode* child = nullptr;
  FOREACH(child, node->pChildren) { visitOperators(reinterpret_cast<SPhysiNode*>(child), check); }
}

void checkLocalJoin(const SQueryPlan& plan, const SArray* readers, EExtWinInputMode mode, bool scalar,
                    const char* fixedTable = nullptr) {
  EXPECT_EQ(1, plan.numOfSubplans);
  ASSERT_NE(nullptr, readers);
  set<int64_t>               identities;
  map<int32_t, set<int32_t>> routes;
  map<int32_t, bool>         ancestorRanges;
  map<int32_t, string>       tables;
  map<int32_t, uint64_t>     uids;
  for (int32_t i = 0; i < taosArrayGetSize(readers); ++i) {
    const auto* reader = static_cast<const SStreamCalcScan*>(taosArrayGet(readers, i));
    SNode*      decoded = nullptr;
    ASSERT_EQ(0, nodesStringToNode(static_cast<const char*>(reader->scanPlan), &decoded));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> guard(decoded, nodesDestroyNode);
    ASSERT_EQ(QUERY_NODE_PHYSICAL_SUBPLAN, nodeType(decoded));
    const auto*   sub = reinterpret_cast<const SSubplan*>(decoded);
    const int32_t group = sub->id.groupId;
    EXPECT_TRUE(identities.insert((int64_t(group) << 32) | sub->id.subplanId).second);
    for (int32_t j = 0; j < taosArrayGetSize(reader->vgList); ++j) {
      routes[group].insert(*static_cast<const int32_t*>(taosArrayGet(reader->vgList, j)));
    }
    ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, nodeType(sub->pNode));
    const auto* scan = reinterpret_cast<const STableScanPhysiNode*>(sub->pNode);
    tables[group] = scan->scan.tableName.tname;
    if (uids.count(group)) EXPECT_EQ(uids[group], scan->scan.uid);
    uids[group] = scan->scan.uid;
    EXPECT_NE(0, scan->scan.uid);
    STableMeta* meta = nullptr;
    ASSERT_EQ(0, g_mockCatalogService->catalogGetTableMeta(&scan->scan.tableName, &meta));
    EXPECT_EQ(meta->uid, scan->scan.uid);
    taosMemoryFree(meta);
    EXPECT_TRUE(sub->requiresAncestorContext);
    bool ancestor = false;
    if (scan->pTimeRange) {
      ASSERT_EQ(QUERY_NODE_TIME_RANGE, nodeType(scan->pTimeRange));
      const auto* range = reinterpret_cast<const STimeRangeNode*>(scan->pTimeRange);
      ASSERT_EQ(QUERY_NODE_OPERATOR, nodeType(range->pStart));
      ASSERT_EQ(QUERY_NODE_OPERATOR, nodeType(range->pEnd));
      const auto* start = reinterpret_cast<const SOperatorNode*>(range->pStart);
      const auto* end = reinterpret_cast<const SOperatorNode*>(range->pEnd);
      EXPECT_EQ(OP_TYPE_GREATER_EQUAL, start->opType);
      EXPECT_EQ(OP_TYPE_LOWER_EQUAL, end->opType);
      ASSERT_EQ(QUERY_NODE_FUNCTION, nodeType(start->pRight));
      ASSERT_EQ(QUERY_NODE_FUNCTION, nodeType(end->pRight));
      const auto* lower = reinterpret_cast<const SFunctionNode*>(start->pRight);
      const auto* upper = reinterpret_cast<const SFunctionNode*>(end->pRight);
      EXPECT_STREQ("_twstart", lower->functionName);
      ancestor = LIST_LENGTH(lower->pParameterList) == 2;
      EXPECT_EQ(ancestor, range->needCalc);
      EXPECT_STREQ(ancestor ? "_twstart" : "_twend", upper->functionName);
      EXPECT_EQ(1, LIST_LENGTH(upper->pParameterList));
      if (ancestor) {
        const SNode* layer = nodesListGetNode(lower->pParameterList, 1);
        ASSERT_EQ(QUERY_NODE_VALUE, nodeType(layer));
        EXPECT_EQ(0, reinterpret_cast<const SValueNode*>(layer)->datum.i);
      }
    } else {
      EXPECT_EQ(EXT_WIN_INPUT_BROADCAST, mode);
      EXPECT_FALSE(sub->dynTbname);
    }
    ancestorRanges[group] = ancestor;
  }
  EXPECT_EQ(2, routes.size());
  int          joins = 0;
  set<int64_t> references;
  visitCalcSubplans(plan, [&](const SSubplan& sub) {
    set<int32_t> referenceGroups;
    SNode*       ref = nullptr;
    FOREACH(ref, sub.pChildren) {
      ASSERT_EQ(QUERY_NODE_VALUE, nodeType(ref));
      const int64_t id = reinterpret_cast<SValueNode*>(ref)->datum.i;
      EXPECT_EQ(1, identities.count(id));
      references.insert(id);
      referenceGroups.insert(uint64_t(id) >> 32);
    }
    visitOperators(sub.pNode, [&](const SPhysiNode& node) {
      if (nodeType(&node) == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
        const auto& exchange = reinterpret_cast<const SExchangePhysiNode&>(node);
        EXPECT_EQ(exchange.srcStartGroupId, exchange.srcEndGroupId);
        EXPECT_EQ(1, referenceGroups.count(exchange.srcStartGroupId));
      }
      if (nodeType(&node) != QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN) return;
      ++joins;
      const auto& join = reinterpret_cast<const SHashJoinPhysiNode&>(node);
      EXPECT_TRUE(join.multiGroupJoin);
      const auto* right = reinterpret_cast<const SPhysiNode*>(nodesListGetNode(node.pChildren, 1));
      while (right && nodeType(right) == QUERY_NODE_PHYSICAL_PLAN_PROJECT) {
        ASSERT_EQ(1, LIST_LENGTH(right->pChildren));
        right = reinterpret_cast<const SPhysiNode*>(nodesListGetNode(right->pChildren, 0));
      }
      ASSERT_NE(nullptr, right);
      ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nodeType(right));
      const auto* window = reinterpret_cast<const SExternalWindowPhysiNode*>(right);
      EXPECT_EQ(mode, window->extWinInputMode);
      EXPECT_EQ(scalar, LIST_LENGTH(window->window.pFuncs) == 0);
      ASSERT_EQ(1, LIST_LENGTH(right->pChildren));
      const SNode* child = nodesListGetNode(right->pChildren, 0);
      ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, nodeType(child));
      int32_t group = reinterpret_cast<const SExchangePhysiNode*>(child)->srcStartGroupId;
      EXPECT_FALSE(ancestorRanges[group]);
      if (fixedTable) {
        EXPECT_EQ(fixedTable, tables[group]);
        SName name = {};
        ASSERT_EQ(0, tNameSetDbName(&name, 0, "stream_querydb", strlen("stream_querydb")));
        ASSERT_EQ(0, tNameAddTbName(&name, fixedTable, strlen(fixedTable)));
        SVgroupInfo vg = {};
        ASSERT_EQ(0, g_mockCatalogService->catalogGetTableHashVgroup(&name, &vg));
        EXPECT_EQ((set<int32_t>{vg.vgId}), routes[group]);
      } else {
        EXPECT_EQ((set<int32_t>{2, 3}), routes[group]);
      }
      for (const auto& route : routes) {
        if (route.first != group) {
          EXPECT_EQ((set<int32_t>{2, 3}), route.second);
          EXPECT_TRUE(ancestorRanges[route.first]);
        }
      }
    });
  });
  EXPECT_EQ(1, joins);
  EXPECT_EQ(identities, references);
}
}  // namespace

TEST_F(PlanJoinTest, StreamLocalWindowJoinNestedAggregation) {
  useDb("root", "stream_streamdb");
  runCreateStream(localJoinSql("select _twend ts,first(c1) first_v,last(c1) last_v from %%tbname" + leafRange),
                  [&](const SQueryPlan& plan, const SArray* readers) {
                    checkLocalJoin(plan, readers, EXT_WIN_INPUT_WINDOW_BOUND, false);
                  });
}

TEST_F(PlanJoinTest, StreamLocalWindowJoinFixedAggregation) {
  for (const char* table : {"stream_t1", "st1s1"}) {
    SCOPED_TRACE(table);
    useDb("root", "stream_streamdb");
    runCreateStream(localJoinSql(string("select _twend ts,first(c1) first_v,last(c1) last_v from stream_querydb.") +
                                 table + leafRange),
                    [&](const SQueryPlan& plan, const SArray* readers) {
                      checkLocalJoin(plan, readers, EXT_WIN_INPUT_WINDOW_BOUND, false, table);
                    });
  }
}

TEST_F(PlanJoinTest, StreamLocalWindowJoinKeepsUnqualifiedScalar) {
  for (const char* table : {"%%tbname", "stream_querydb.stream_t1", "stream_querydb.st1s1"}) {
    SCOPED_TRACE(table);
    useDb("root", "stream_streamdb");
    runCreateStream(localJoinSql(string("select _twend ts,c1 from ") + table + leafRange),
                    [&](const SQueryPlan& plan, const SArray* readers) {
                      EXPECT_EQ(strcmp(table, "%%tbname") == 0 ? 3 : 2, plan.numOfSubplans);
                      EXPECT_FALSE(findPlanNode("ExternalWindow"));
                      EXPECT_FALSE(planContains("\"MultiGroupJoin\":true"));
                    });
  }
}

TEST_F(PlanJoinTest, StreamLocalWindowJoinBroadcast) {
  useDb("root", "stream_streamdb");
  runCreateStream(localJoinSql("select _twend ts,c1,_tgrpid,%%1 from stream_querydb.stream_t2"),
                  [&](const SQueryPlan& plan, const SArray* readers) {
                    checkLocalJoin(plan, readers, EXT_WIN_INPUT_BROADCAST, true, "stream_t2");
                  });
}

TEST_F(PlanJoinTest, StreamLocalWindowJoinKeepsUnqualifiedDerivedWrapper) {
  useDb("root", "stream_streamdb");
  runCreateStream(localJoinSql("select r.ts,r.first_v,r.last_v from (select q.ts,q.first_v,q.last_v+1 last_v from "
                               "(select _twend ts,first(c1) first_v,last(c1) last_v from %%tbname" +
                               leafRange + ") q where q.first_v is not null) r"),
                  [&](const SQueryPlan& plan, const SArray* readers) {
                    EXPECT_EQ(3, plan.numOfSubplans);
                    EXPECT_FALSE(findPlanNode("ExternalWindow"));
                    EXPECT_FALSE(planContains("\"MultiGroupJoin\":true"));
                  });
}

TEST_F(PlanJoinTest, StreamLocalWindowJoinKeepsPositionSensitivePlans) {
  for (const string& right : {"select _twend ts,c1 from %%tbname" + leafRange + " limit 1 offset 1",
                              "select distinct _twend ts,c1 from %%tbname" + leafRange,
                              "select _twend ts,top(c1,2) from %%tbname" + leafRange,
                              "select _twend ts,first(c1) from %%tbname" + leafRange + " partition by c1",
                              string("select _twend ts,last(c1) from %%trows")}) {
    SCOPED_TRACE(right);
    useDb("root", "stream_streamdb");
    runCreateStream(localJoinSql(right), [&](const SQueryPlan& plan, const SArray*) {
      EXPECT_GT(plan.numOfSubplans, 1);
      int joins = 0;
      visitCalcSubplans(plan, [&](const SSubplan& sub) {
        visitOperators(sub.pNode, [&](const SPhysiNode& node) {
          if (nodeType(&node) == QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN ||
              nodeType(&node) == QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN) {
            ++joins;
            EXPECT_EQ(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, nodeType(nodesListGetNode(node.pChildren, 1)));
          }
        });
      });
      EXPECT_EQ(1, joins);
    });
  }
}

TEST_F(PlanJoinTest, StreamLocalWindowJoinKeepsSingleGroupAndOrdinaryQueries) {
  useDb("root", "test");
  run("select a.ts,a.c1,b.c1 from (select ts,c1 from st1s1) a "
      "join (select ts,c1 from st1s2) b on a.ts=b.ts");
  EXPECT_FALSE(planContains("\"MultiGroupJoin\":true"));
  useDb("root", "stream_streamdb");
  string sql = localJoinSql("select _twend ts,last(c1) from stream_querydb.stream_t1" + leafRange);
  sql.replace(sql.find("stream_triggerdb.st1 partition by tbname"), strlen("stream_triggerdb.st1 partition by tbname"),
              "stream_triggerdb.stream_t1");
  sql.replace(sql.find("%%tbname"), strlen("%%tbname"), "stream_triggerdb.stream_t1");
  runCreateStream(sql, [&](const SQueryPlan& plan, const SArray*) { EXPECT_GT(plan.numOfSubplans, 1); });
  EXPECT_FALSE(planContains("\"MultiGroupJoin\":true"));
}

TEST_F(PlanJoinTest, StreamLocalWindowJoinKeepsDynamicSource) {
  useDb("root", "stream_streamdb");
  runCreateStream(
      "create stream stream_streamdb.s_dynamic count_window(2,1) from stream_triggerdb.st1 "
      "partition by tbname,tag1 into stream_outdb.stream_out as select a.ts,b.c2 from "
      "(select _twend ts,last(c1) c1 from %%tbname where ts>=_twstart and ts<=_twend) a join "
      "(select _twend ts,tag1,c2 from stream_querydb.st1 where tag1=%%2) b on a.ts=b.ts",
      [&](const SQueryPlan& plan, const SArray* readers) {
        EXPECT_EQ(1, plan.numOfSubplans);
        int filteredReaders = 0;
        for (int32_t i = 0; i < taosArrayGetSize(readers); ++i) {
          const auto* reader = static_cast<const SStreamCalcScan*>(taosArrayGet(readers, i));
          SNode*      decoded = nullptr;
          ASSERT_EQ(0, nodesStringToNode(static_cast<const char*>(reader->scanPlan), &decoded));
          unique_ptr<SNode, decltype(&nodesDestroyNode)> guard(decoded, nodesDestroyNode);
          const auto*                                    sub = reinterpret_cast<const SSubplan*>(decoded);
          if (!sub->pTagCond) continue;
          ++filteredReaders;
          ASSERT_EQ(QUERY_NODE_OPERATOR, nodeType(sub->pTagCond));
          const auto* cond = reinterpret_cast<const SOperatorNode*>(sub->pTagCond);
          EXPECT_EQ(OP_TYPE_EQUAL, cond->opType);
          ASSERT_EQ(QUERY_NODE_FUNCTION, nodeType(cond->pRight));
          const auto* fn = reinterpret_cast<const SFunctionNode*>(cond->pRight);
          EXPECT_STREQ("_placeholder_column", fn->functionName);
          ASSERT_EQ(2, LIST_LENGTH(fn->pParameterList));
          EXPECT_EQ(2, reinterpret_cast<SValueNode*>(nodesListGetNode(fn->pParameterList, 1))->datum.i);
        }
        EXPECT_EQ(2, filteredReaders);
      });
}

namespace {
// Exercise splitLogicPlan when SQL optimization would erase the project layers or
// SQL qualification rejects a boundary before the splitter can see it.
struct LocalJoinSplitPlan {
  SPlanContext      cxt = {};
  SLogicSubplan*    plan;
  SJoinLogicNode*   join;
  SWindowLogicNode* window;
  SScanLogicNode*   scan;
  SLogicNode*       right;

  template <typename T>
  static T* make(ENodeType type) {
    SNode* node = nullptr;
    EXPECT_EQ(0, nodesMakeNode(type, &node));
    return reinterpret_cast<T*>(node);
  }

  static void append(SLogicNode* parent, SLogicNode* child) {
    EXPECT_EQ(0, nodesListMakeAppend(&parent->pChildren, reinterpret_cast<SNode*>(child)));
    child->pParent = parent;
  }

  explicit LocalJoinSplitPlan(int projects = 0) {
    cxt.streamCxt.isCalc = true;
    cxt.streamCxt.enableMultiGroupCalc = true;
    plan = make<SLogicSubplan>(QUERY_NODE_LOGIC_SUBPLAN);
    plan->id.groupId = cxt.groupId = 1;
    join = make<SJoinLogicNode>(QUERY_NODE_LOGIC_PLAN_JOIN);
    plan->pNode = &join->node;
    join->isSingleTableJoin = join->multiGroupJoin = true;
    join->joinType = JOIN_TYPE_INNER;
    join->subType = JOIN_STYPE_NONE;
    join->joinAlgo = JOIN_ALGO_HASH;
    auto* left = make<SScanLogicNode>(QUERY_NODE_LOGIC_PLAN_SCAN);
    left->tableType = TSDB_NORMAL_TABLE;
    left->scanType = SCAN_TYPE_TABLE;
    append(&join->node, &left->node);
    SLogicNode* parent = &join->node;
    for (int i = 0; i < projects; ++i) {
      auto* project = make<SProjectLogicNode>(QUERY_NODE_LOGIC_PLAN_PROJECT);
      project->ignoreGroupId = true;
      append(parent, &project->node);
      parent = &project->node;
    }
    window = make<SWindowLogicNode>(QUERY_NODE_LOGIC_PLAN_WINDOW);
    window->winType = WINDOW_TYPE_EXTERNAL;
    window->isSingleTable = true;
    append(parent, &window->node);
    scan = make<SScanLogicNode>(QUERY_NODE_LOGIC_PLAN_SCAN);
    scan->tableType = TSDB_NORMAL_TABLE;
    scan->scanType = SCAN_TYPE_TABLE;
    append(&window->node, &scan->node);
    right = reinterpret_cast<SLogicNode*>(nodesListGetNode(join->node.pChildren, 1));
  }

  ~LocalJoinSplitPlan() { nodesDestroyNode(reinterpret_cast<SNode*>(plan)); }

  void check(bool local) {
    ASSERT_EQ(0, splitLogicPlan(&cxt, plan));
    const auto* actual = nodesListGetNode(join->node.pChildren, 1);
    if (local) {
      EXPECT_EQ(reinterpret_cast<SNode*>(right), actual);
      EXPECT_EQ(QUERY_NODE_LOGIC_PLAN_EXCHANGE, nodeType(nodesListGetNode(window->node.pChildren, 0)));
      EXPECT_EQ(2, LIST_LENGTH(plan->pChildren));
    } else {
      EXPECT_EQ(QUERY_NODE_LOGIC_PLAN_EXCHANGE, nodeType(actual));
    }
  }
};
}  // namespace

TEST_F(PlanJoinTest, StreamLocalWindowJoinLogicTransparentProjects) {
  for (int projects : {0, 1, 3}) {
    for (int algorithm : {0, int(EXTERNAL_ALGO_HASH)}) {
      SCOPED_TRACE(projects);
      SCOPED_TRACE(algorithm);
      LocalJoinSplitPlan plan(projects);
      plan.window->windowAlgo = static_cast<EWindowAlgorithm>(algorithm);
      plan.check(true);
    }
  }
}

TEST_F(PlanJoinTest, StreamLocalWindowJoinLogicRefusesBoundaries) {
  const vector<pair<const char*, function<void(LocalJoinSplitPlan&)>>> cases = {
      {"ordinary", [](LocalJoinSplitPlan& p) { p.cxt.streamCxt.isCalc = false; }},
      {"single group", [](LocalJoinSplitPlan& p) { p.cxt.streamCxt.enableMultiGroupCalc = false; }},
      {"unqualified join", [](LocalJoinSplitPlan& p) { p.join->multiGroupJoin = false; }},
      {"outer join", [](LocalJoinSplitPlan& p) { p.join->joinType = JOIN_TYPE_LEFT; }},
      {"merge join", [](LocalJoinSplitPlan& p) { p.join->joinAlgo = JOIN_ALGO_MERGE; }},
      {"split window", [](LocalJoinSplitPlan& p) { p.window->extWinSplit = true; }},
      {"merge window", [](LocalJoinSplitPlan& p) { p.window->windowAlgo = EXTERNAL_ALGO_MERGE; }},
      {"indefinite", [](LocalJoinSplitPlan& p) { p.window->indefRowsFunc = true; }},
      {"subgroups", [](LocalJoinSplitPlan& p) { p.window->calcWithPartition = true; }},
      {"group sort", [](LocalJoinSplitPlan& p) { p.window->needGroupSort = true; }},
      {"multiple tables", [](LocalJoinSplitPlan& p) { p.window->isSingleTable = false; }},
      {"set project", [](LocalJoinSplitPlan& p) { reinterpret_cast<SProjectLogicNode*>(p.right)->isSetOpProj = true; }},
      {"trows", [](LocalJoinSplitPlan& p) { p.scan->placeholderType = SP_PARTITION_ROWS; }},
      {"virtual", [](LocalJoinSplitPlan& p) { p.scan->virtualStableScan = true; }},
      {"virtual normal", [](LocalJoinSplitPlan& p) { p.scan->tableType = TSDB_VIRTUAL_NORMAL_TABLE; }},
      {"virtual child", [](LocalJoinSplitPlan& p) { p.scan->tableType = TSDB_VIRTUAL_CHILD_TABLE; }},
      {"external", [](LocalJoinSplitPlan& p) { p.scan->scanType = SCAN_TYPE_EXTERNAL; }},
      {"stable", [](LocalJoinSplitPlan& p) { p.scan->tableType = TSDB_SUPER_TABLE; }},
      {"sort",
       [](LocalJoinSplitPlan& p) {
         auto* sort = LocalJoinSplitPlan::make<SSortLogicNode>(QUERY_NODE_LOGIC_PLAN_SORT);
         nodesClearList(p.right->pChildren);
         p.right->pChildren = nullptr;
         LocalJoinSplitPlan::append(p.right, &sort->node);
         LocalJoinSplitPlan::append(&sort->node, &p.window->node);
       }},
      {"internal join",
       [](LocalJoinSplitPlan& p) {
         auto* inner = LocalJoinSplitPlan::make<SJoinLogicNode>(QUERY_NODE_LOGIC_PLAN_JOIN);
         inner->joinAlgo = JOIN_ALGO_HASH;
         auto* sibling = LocalJoinSplitPlan::make<SScanLogicNode>(QUERY_NODE_LOGIC_PLAN_SCAN);
         sibling->scanType = SCAN_TYPE_TABLE;
         sibling->tableType = TSDB_NORMAL_TABLE;
         nodesClearList(p.window->node.pChildren);
         p.window->node.pChildren = nullptr;
         LocalJoinSplitPlan::append(&p.window->node, &inner->node);
         LocalJoinSplitPlan::append(&inner->node, &p.scan->node);
         LocalJoinSplitPlan::append(&inner->node, &sibling->node);
       }},
  };
  for (const auto& test : cases) {
    SCOPED_TRACE(test.first);
    LocalJoinSplitPlan plan(1);
    test.second(plan);
    plan.check(false);
  }
  for (int node : {0, 1, 2}) {
    for (int restriction : {0, 1, 2}) {
      SCOPED_TRACE(node);
      SCOPED_TRACE(restriction);
      LocalJoinSplitPlan plan(1);
      SLogicNode*        target = node == 0 ? plan.right : node == 1 ? &plan.window->node : &plan.scan->node;
      if (restriction == 0) {
        target->dynamicOp = true;
      } else {
        auto* limit = LocalJoinSplitPlan::make<SLimitNode>(QUERY_NODE_LIMIT);
        limit->limit = LocalJoinSplitPlan::make<SValueNode>(QUERY_NODE_VALUE);
        limit->limit->translate = true;
        limit->limit->node.resType.type = TSDB_DATA_TYPE_BIGINT;
        limit->limit->datum.i = 1;
        if (restriction == 1)
          target->pLimit = reinterpret_cast<SNode*>(limit);
        else
          target->pSlimit = reinterpret_cast<SNode*>(limit);
      }
      plan.check(false);
    }
  }
}

TEST_F(PlanJoinTest, StreamLocalWindowJoinLogicFunctionStages) {
  auto*                                          full = LocalJoinSplitPlan::make<SFunctionNode>(QUERY_NODE_FUNCTION);
  unique_ptr<SNode, decltype(&nodesDestroyNode)> guard(reinterpret_cast<SNode*>(full), nodesDestroyNode);
  strcpy(full->functionName, "avg");
  auto* column = LocalJoinSplitPlan::make<SColumnNode>(QUERY_NODE_COLUMN);
  column->node.resType.type = TSDB_DATA_TYPE_INT;
  column->node.resType.bytes = sizeof(int32_t);
  ASSERT_EQ(0, nodesListMakeAppend(&full->pParameterList, reinterpret_cast<SNode*>(column)));
  ASSERT_EQ(0, fmGetFuncInfo(full, nullptr, 0));
  SFunctionNode *partial = nullptr, *mid = nullptr, *merge = nullptr;
  ASSERT_EQ(0, fmGetDistMethod(full, &partial, &mid, &merge));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> partialGuard(reinterpret_cast<SNode*>(partial), nodesDestroyNode);
  unique_ptr<SNode, decltype(&nodesDestroyNode)> midGuard(reinterpret_cast<SNode*>(mid), nodesDestroyNode);
  unique_ptr<SNode, decltype(&nodesDestroyNode)> mergeGuard(reinterpret_cast<SNode*>(merge), nodesDestroyNode);
  for (const auto* function : {full, partial, mid, merge}) {
    SCOPED_TRACE(function->functionName);
    LocalJoinSplitPlan plan;
    SNode*             cloned = nullptr;
    ASSERT_EQ(0, nodesCloneNode(reinterpret_cast<const SNode*>(function), &cloned));
    ASSERT_EQ(0, nodesListMakeAppend(&plan.window->pFuncs, cloned));
    // Mid functions have no hasOriginalFunc bit; real distributed windows carry
    // extWinSplit on every phase. Keep that contract in this fixture.
    if (function == mid) plan.window->extWinSplit = true;
    plan.check(function == full);
  }
  LocalJoinSplitPlan distinct;
  SNode*             cloned = nullptr;
  ASSERT_EQ(0, nodesCloneNode(reinterpret_cast<SNode*>(full), &cloned));
  reinterpret_cast<SFunctionNode*>(cloned)->isDistinct = true;
  ASSERT_EQ(0, nodesListMakeAppend(&distinct.window->pFuncs, cloned));
  distinct.check(false);
}

TEST_F(PlanJoinTest, StreamLocalWindowJoinKeepsDistributedAggregation) {
  useDb("root", "stream_streamdb");
  runCreateStream(localJoinSql("select _twend ts,first(c1) from stream_querydb.st1" + leafRange),
                  [&](const SQueryPlan& plan, const SArray*) {
                    EXPECT_GT(plan.numOfSubplans, 1);
                    int merges = 0;
                    visitCalcSubplans(plan, [&](const SSubplan& sub) {
                      visitOperators(sub.pNode, [&](const SPhysiNode& node) {
                        if (nodeType(&node) == QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL) ++merges;
                      });
                    });
                    EXPECT_GT(merges, 0);
                  });
}

TEST_F(PlanJoinTest, StreamLocalWindowJoinLogicScalarSources) {
  for (int source : {0, 1, 2, 3}) {
    SCOPED_TRACE(source);
    LocalJoinSplitPlan plan(2);
    plan.join->joinAlgo = JOIN_ALGO_UNKNOWN;
    plan.scan->tableType = source == 0 ? TSDB_NORMAL_TABLE : source == 1 ? TSDB_CHILD_TABLE : TSDB_SUPER_TABLE;
    if (source >= 2) {
      plan.scan->placeholderType = source == 2 ? SP_PARTITION_TBNAME : static_cast<EStreamPlaceholder>(0);
      plan.scan->phTbnameScan = source == 3;
      plan.scan->pVgroupList =
          static_cast<SVgroupsInfo*>(taosMemoryCalloc(1, sizeof(SVgroupsInfo) + 2 * sizeof(SVgroupInfo)));
      ASSERT_NE(nullptr, plan.scan->pVgroupList);
      plan.scan->pVgroupList->numOfVgroups = 2;
      plan.scan->pVgroupList->vgroups[0].vgId = 2;
      plan.scan->pVgroupList->vgroups[1].vgId = 3;
    }
    plan.scan->node.requireDataOrder = DATA_ORDER_LEVEL_GLOBAL;
    plan.scan->node.resultDataOrder = DATA_ORDER_LEVEL_GLOBAL;
    plan.scan->node.outputTsOrder = ORDER_ASC;
    plan.window->inputHasOrder = true;
    plan.window->extWinInputMode = EXT_WIN_INPUT_WINDOW_BOUND;
    EXPECT_EQ(nullptr, plan.window->pFuncs);
    plan.check(true);
    const auto* exchange =
        reinterpret_cast<const SExchangeLogicNode*>(nodesListGetNode(plan.window->node.pChildren, 0));
    EXPECT_EQ(DATA_ORDER_LEVEL_GLOBAL, exchange->node.requireDataOrder);
    EXPECT_EQ(DATA_ORDER_LEVEL_GLOBAL, exchange->node.resultDataOrder);
    EXPECT_EQ(ORDER_ASC, exchange->node.outputTsOrder);
    EXPECT_TRUE(plan.window->inputHasOrder);
    EXPECT_EQ(EXT_WIN_INPUT_WINDOW_BOUND, plan.window->extWinInputMode);
  }
}
