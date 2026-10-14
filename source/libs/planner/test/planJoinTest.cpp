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
#include "tglobal.h"
#include "stub.h"

using namespace std;

class PlanJoinTest : public PlannerTestBase {
 protected:
  static const SLogicNode* findNode(const SLogicNode* pNode, ENodeType type) {
    if (nodeType(pNode) == type) return pNode;
    SNode* pChild = nullptr;
    FOREACH(pChild, pNode->pChildren) {
      auto* pFound = findNode((const SLogicNode*)pChild, type);
      if (pFound != nullptr) return pFound;
    }
    return nullptr;
  }

  static int countNodes(const SLogicNode* pNode, ENodeType type) {
    int count = nodeType(pNode) == type;
    SNode* pChild = nullptr;
    FOREACH(pChild, pNode->pChildren) {
      count += countNodes((const SLogicNode*)pChild, type);
    }
    return count;
  }

  static int countSorts(const SLogicNode* pNode) {
    return countNodes(pNode, QUERY_NODE_LOGIC_PLAN_SORT);
  }

  static bool hasTarget(const SLogicNode* pNode,
                        const SColumnNode* pRequired) {
    SNode* pTarget = nullptr;
    FOREACH(pTarget, pNode->pTargets) {
      if (nodeType(pTarget) != QUERY_NODE_COLUMN) continue;
      auto* pCol = (const SColumnNode*)pTarget;
      if (strcmp(pCol->tableAlias, pRequired->tableAlias) == 0 &&
          strcmp(pCol->colName, pRequired->colName) == 0) {
        return true;
      }
    }
    return false;
  }

  static const SJoinOutputCol* findJoinOutput(
      const SJoinInputProps* pProps, const char* pTableAlias,
      const char* pColName) {
    size_t numCols = taosArrayGetSize(pProps->pOutputCols);
    for (size_t i = 0; i < numCols; ++i) {
      auto* pOutput = (const SJoinOutputCol*)taosArrayGet(
          pProps->pOutputCols, i);
      auto* pCol = (const SColumnNode*)pOutput->pExpr;
      if (strcmp(pCol->tableAlias, pTableAlias) == 0 &&
          strcmp(pCol->colName, pColName) == 0) {
        return pOutput;
      }
    }
    return nullptr;
  }

  // An ASOF/WINDOW join must end up with a resolved time condition whose
  // every column survives in exactly one direct child's targets. Without
  // that, the physical stage has no direct-child slot to bind it to.
  static void expectTemporalJoinTimeKeyContract(const SLogicNode* pNode) {
    if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_JOIN) {
      auto* pJoin = (const SJoinLogicNode*)pNode;
      if (IS_ASOF_JOIN(pJoin->subType) || IS_WINDOW_JOIN(pJoin->subType)) {
        SNode* pTimeCond = pJoin->pPrimKeyEqCond != nullptr
                               ? pJoin->pPrimKeyEqCond
                               : pJoin->addPrimEqCond;
        ASSERT_NE(pTimeCond, nullptr)
            << "ASOF/WINDOW join has no resolved effective time condition";

        auto* pLeft =
            (const SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
        auto* pRight =
            (const SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
        SNodeList* pColumns = nullptr;
        ASSERT_EQ(nodesCollectColumnsFromNode(pTimeCond, nullptr,
                                              COLLECT_COL_TYPE_ALL, &pColumns),
                  TSDB_CODE_SUCCESS);
        SNode* pColumn = nullptr;
        FOREACH(pColumn, pColumns) {
          ASSERT_EQ(nodeType(pColumn), QUERY_NODE_COLUMN);
          auto* pRequired = (const SColumnNode*)pColumn;
          const int sides = (hasTarget(pLeft, pRequired) ? 1 : 0) +
                            (hasTarget(pRight, pRequired) ? 1 : 0);
          EXPECT_EQ(sides, 1)
              << "effective time column " << pRequired->tableAlias << "."
              << pRequired->colName
              << " must survive in exactly one direct child's targets";
        }
        nodesDestroyList(pColumns);

        EXPECT_NE(pJoin->node.inputTsOrder, TSDB_ORDER_NONE)
            << "ASOF/WINDOW join must settle a direction for its inputs";
      }
    }

    SNode* pChild = nullptr;
    FOREACH(pChild, pNode->pChildren) {
      expectTemporalJoinTimeKeyContract((const SLogicNode*)pChild);
    }
  }

  static void expectTemporalInputNullFilter(const SLogicNode* pRoot,
                                            bool right, const char* alias,
                                            const char* name) {
    auto* pJoin = (const SJoinLogicNode*)findNode(
        pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    ASSERT_TRUE(IS_ASOF_JOIN(pJoin->subType) ||
                IS_WINDOW_JOIN(pJoin->subType));
    auto* pCond = (const SOperatorNode*)(pJoin->pPrimKeyEqCond != nullptr
                                           ? pJoin->pPrimKeyEqCond
                                           : pJoin->addPrimEqCond);
    ASSERT_NE(pCond, nullptr);
    auto* pTime = right ? pCond->pRight : pCond->pLeft;
    ASSERT_EQ(nodeType(pTime), QUERY_NODE_COLUMN);
    EXPECT_STREQ(((const SColumnNode*)pTime)->tableAlias, alias);
    EXPECT_STREQ(((const SColumnNode*)pTime)->colName, name);
    auto* pInput = (const SLogicNode*)nodesListGetNode(
        pJoin->node.pChildren, right ? 1 : 0);
    ASSERT_NE(pInput, nullptr);
    struct FilterContext {
      const SNode* time;
      bool found;
    } context = {pTime, false};
    nodesWalkExpr(
        pInput->pConditions,
        [](SNode* pNode, void* pContext) {
          auto* pFilter = (FilterContext*)pContext;
          if (nodeType(pNode) == QUERY_NODE_OPERATOR) {
            auto* pOp = (const SOperatorNode*)pNode;
            if (pOp->opType == OP_TYPE_IS_NOT_NULL &&
                nodesEqualNode(pOp->pLeft, pFilter->time)) {
              pFilter->found = true;
            }
          }
          return DEAL_RES_CONTINUE;
        },
        &context);
    EXPECT_TRUE(context.found)
        << "nullable time must be filtered at the temporal JOIN input";
  }

  static void expectJoinDirectChildContract(const SLogicNode* pNode) {
    if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_JOIN) {
      auto* pJoin = (const SJoinLogicNode*)pNode;
      auto* pLeft = (const SLogicNode*)nodesListGetNode(
          pJoin->node.pChildren, 0);
      auto* pRight = (const SLogicNode*)nodesListGetNode(
          pJoin->node.pChildren, 1);
      SNodeList* pColumns = nullptr;
      ASSERT_EQ(nodesCollectColumnsFromNode(
                    pJoin->pFullOnCond, nullptr, COLLECT_COL_TYPE_ALL,
                    &pColumns),
                TSDB_CODE_SUCCESS);
      SNode* pColumn = nullptr;
      FOREACH(pColumn, pColumns) {
        ASSERT_EQ(nodeType(pColumn), QUERY_NODE_COLUMN);
        auto* pRequired = (const SColumnNode*)pColumn;
        const int sides = (hasTarget(pLeft, pRequired) ? 1 : 0) +
                          (hasTarget(pRight, pRequired) ? 1 : 0);
        EXPECT_EQ(sides, 1)
            << "ON column " << pRequired->tableAlias << "."
            << pRequired->colName
            << " must survive in exactly one direct child's targets";
      }
      nodesDestroyList(pColumns);
    }

    SNode* pChild = nullptr;
    FOREACH(pChild, pNode->pChildren) {
      expectJoinDirectChildContract((const SLogicNode*)pChild);
    }
  }

  struct PhysiJoinInputIds {
    int64_t left;
    int64_t right;
  };

  static EDealRes checkPhysiJoinColumn(SNode* pNode, void* pContext) {
    if (QUERY_NODE_COLUMN != nodeType(pNode)) {
      return DEAL_RES_CONTINUE;
    }
    auto* pIds = (const PhysiJoinInputIds*)pContext;
    auto* pCol = (const SColumnNode*)pNode;
    const int sides = (pCol->dataBlockId == pIds->left ? 1 : 0) +
                      (pCol->dataBlockId == pIds->right ? 1 : 0);
    EXPECT_EQ(sides, 1)
        << "physical JOIN column " << pCol->tableAlias << "."
        << pCol->colName << " uses block " << pCol->dataBlockId
        << ", direct inputs are " << pIds->left << " and " << pIds->right;
    return DEAL_RES_CONTINUE;
  }

  static void checkPhysiJoinExpr(SNode* pExpr,
                                 const PhysiJoinInputIds* pIds) {
    nodesWalkExpr(pExpr, checkPhysiJoinColumn, (void*)pIds);
  }

  static void checkPhysiJoinExprs(SNodeList* pExprs,
                                  const PhysiJoinInputIds* pIds) {
    nodesWalkExprs(pExprs, checkPhysiJoinColumn, (void*)pIds);
  }

  static int64_t exprBlockId(SNode* pExpr) {
    struct Ctx {
      int64_t id;
      bool    mixed;
      bool    seen;
    } ctx = {0, false, false};
    nodesWalkExpr(
        pExpr,
        [](SNode* pNode, void* pContext) {
          if (QUERY_NODE_COLUMN != nodeType(pNode)) return DEAL_RES_CONTINUE;
          auto* pCtx = (Ctx*)pContext;
          auto* pCol = (const SColumnNode*)pNode;
          if (!pCtx->seen) {
            pCtx->seen = true;
            pCtx->id = pCol->dataBlockId;
          } else if (pCtx->id != pCol->dataBlockId) {
            pCtx->mixed = true;
          }
          return DEAL_RES_CONTINUE;
        },
        &ctx);
    if (!ctx.seen || ctx.mixed) return -1;
    return ctx.id;
  }

  // The two operands of a temporal key condition describe one row of each
  // input, so they must resolve to different child blocks. A key column that
  // was routed to the wrong descriptor lands on the same block as the other
  // operand, and reads a fabricated slot that no child actually produces.
  static void expectPhysiTemporalKeySides(const SSortMergeJoinPhysiNode* pJoin,
                                          int64_t leftId, int64_t rightId) {
    if (nullptr == pJoin->pPrimKeyCond ||
        QUERY_NODE_OPERATOR != nodeType(pJoin->pPrimKeyCond)) {
      return;
    }
    auto*   pOp = (const SOperatorNode*)pJoin->pPrimKeyCond;
    int64_t lhs = exprBlockId(pOp->pLeft);
    int64_t rhs = exprBlockId(pOp->pRight);
    if (lhs < 0 || rhs < 0) return;  // constant or mixed operand

    EXPECT_NE(lhs, rhs)
        << "temporal key operands both resolve to block " << lhs
        << "; direct inputs are " << leftId << " and " << rightId;
    for (SNode* pKey : {pOp->pLeft, pOp->pRight}) {
      if (nodeType(pKey) != QUERY_NODE_COLUMN) continue;
      auto* pCol = (const SColumnNode*)pKey;
      EXPECT_EQ(pCol->slotId, pCol->dataBlockId == leftId
                                 ? pJoin->leftPrimSlotId
                                 : pJoin->rightPrimSlotId);
    }
  }

  static bool isPhysiJoin(const SPhysiNode* pNode) {
    return QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN == nodeType(pNode) ||
           QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN == nodeType(pNode) ||
           QUERY_NODE_PHYSICAL_PLAN_THETA_JOIN == nodeType(pNode);
  }

  static void expectTemporalInputSlot(const SDataBlockDescNode* pDesc,
                                      int32_t slotId, SNode* pExpr) {
    const SSlotDescNode* pFound = nullptr;
    SNode* pSlot = nullptr;
    FOREACH(pSlot, pDesc->pSlots) {
      auto* pDescSlot = (const SSlotDescNode*)pSlot;
      if (pDescSlot->slotId == slotId) pFound = pDescSlot;
    }
    ASSERT_NE(pFound, nullptr);
    EXPECT_EQ(pFound->dataType.type, TSDB_DATA_TYPE_TIMESTAMP);
    if (pExpr != nullptr) {
      ASSERT_EQ(nodeType(pExpr), QUERY_NODE_TARGET);
      auto* pTarget = (const STargetNode*)pExpr;
      EXPECT_EQ(pTarget->dataBlockId, pDesc->dataBlockId);
      EXPECT_EQ(exprBlockId(pTarget->pExpr), pDesc->dataBlockId);
      const SSlotDescNode* pComputed = nullptr;
      FOREACH(pSlot, pDesc->pSlots) {
        auto* pDescSlot = (const SSlotDescNode*)pSlot;
        if (pDescSlot->slotId == pTarget->slotId) pComputed = pDescSlot;
      }
      ASSERT_NE(pComputed, nullptr);
      EXPECT_EQ(pComputed->dataType.precision,
                ((const SExprNode*)pTarget->pExpr)->resType.precision);
    }
  }

  static void expectPhysiJoinDirectChildContract(const SPhysiNode* pNode) {
    if (isPhysiJoin(pNode)) {
      ASSERT_EQ(LIST_LENGTH(pNode->pChildren), 2);
      auto* pLeft = (const SPhysiNode*)nodesListGetNode(pNode->pChildren, 0);
      auto* pRight =
          (const SPhysiNode*)nodesListGetNode(pNode->pChildren, 1);
      ASSERT_NE(pLeft->pOutputDataBlockDesc, nullptr);
      ASSERT_NE(pRight->pOutputDataBlockDesc, nullptr);
      const PhysiJoinInputIds ids = {
          pLeft->pOutputDataBlockDesc->dataBlockId,
          pRight->pOutputDataBlockDesc->dataBlockId,
      };
      ASSERT_NE(ids.left, ids.right);
      checkPhysiJoinExpr(pNode->pConditions, &ids);

      if (QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN == nodeType(pNode)) {
        auto* pJoin = (const SSortMergeJoinPhysiNode*)pNode;
        checkPhysiJoinExpr(pJoin->leftPrimExpr, &ids);
        checkPhysiJoinExpr(pJoin->rightPrimExpr, &ids);
        checkPhysiJoinExprs(pJoin->pEqLeft, &ids);
        checkPhysiJoinExprs(pJoin->pEqRight, &ids);
        checkPhysiJoinExpr(pJoin->pPrimKeyCond, &ids);
        checkPhysiJoinExpr(pJoin->pColEqCond, &ids);
        checkPhysiJoinExpr(pJoin->pColOnCond, &ids);
        checkPhysiJoinExpr(pJoin->pFullOnCond, &ids);
        expectPhysiTemporalKeySides(pJoin, ids.left, ids.right);
        if (IS_ASOF_JOIN(pJoin->subType) || IS_WINDOW_JOIN(pJoin->subType)) {
          expectTemporalInputSlot(pLeft->pOutputDataBlockDesc,
                                  pJoin->leftPrimSlotId,
                                  pJoin->leftPrimExpr);
          expectTemporalInputSlot(pRight->pOutputDataBlockDesc,
                                  pJoin->rightPrimSlotId,
                                  pJoin->rightPrimExpr);
        }
      } else if (QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN == nodeType(pNode)) {
        auto* pJoin = (const SHashJoinPhysiNode*)pNode;
        checkPhysiJoinExprs(pJoin->pOnLeft, &ids);
        checkPhysiJoinExprs(pJoin->pOnRight, &ids);
        checkPhysiJoinExpr(pJoin->pLeftOnCond, &ids);
        checkPhysiJoinExpr(pJoin->pRightOnCond, &ids);
        checkPhysiJoinExpr(pJoin->pFullOnCond, &ids);
        checkPhysiJoinExpr(pJoin->pPrimKeyCond, &ids);
        checkPhysiJoinExpr(pJoin->pColEqCond, &ids);
        checkPhysiJoinExpr(pJoin->pTagEqCond, &ids);
      } else {
        auto* pJoin = (const SThetaJoinPhysiNode*)pNode;
        checkPhysiJoinExprs(pJoin->pOnLeft, &ids);
        checkPhysiJoinExprs(pJoin->pOnRight, &ids);
        checkPhysiJoinExpr(pJoin->pLeftOnCond, &ids);
        checkPhysiJoinExpr(pJoin->pRightOnCond, &ids);
        checkPhysiJoinExpr(pJoin->pFullOnCond, &ids);
        checkPhysiJoinExpr(pJoin->pPrimKeyCond, &ids);
        checkPhysiJoinExpr(pJoin->pColEqCond, &ids);
        checkPhysiJoinExpr(pJoin->pTagEqCond, &ids);
      }
    }

    SNode* pChild = nullptr;
    FOREACH(pChild, pNode->pChildren) {
      expectPhysiJoinDirectChildContract((const SPhysiNode*)pChild);
    }
  }

  static void expectPhysiJoinDirectChildContract(const SQueryPlan* pPlan) {
    SNode* pLevel = nullptr;
    FOREACH(pLevel, pPlan->pSubplans) {
      SNode* pSubplan = nullptr;
      FOREACH(pSubplan, ((SNodeListNode*)pLevel)->pNodeList) {
        auto* pRoot = ((const SSubplan*)pSubplan)->pNode;
        expectPhysiJoinDirectChildContract(pRoot);
      }
    }
    SNode* pChildPlan = nullptr;
    FOREACH(pChildPlan, pPlan->pChildren) {
      expectPhysiJoinDirectChildContract((const SQueryPlan*)pChildPlan);
    }
  }
};

TEST_F(PlanJoinTest, basic) {
  useDb("root", "test");

  run("SELECT t1.c1, t2.c2 FROM st1s1 t1, st1s2 t2 WHERE t1.ts = t2.ts");

  run("SELECT t1.*, t2.* FROM st1s1 t1, st1s2 t2 WHERE t1.ts = t2.ts");

  run("SELECT t1.c1, t2.c1 FROM st1s1 t1 JOIN st1s2 t2 ON t1.ts = t2.ts");

  run("SELECT t1.c1, t2.c1 FROM st1 t1 JOIN st2 t2 ON t1.ts = t2.ts");
}

TEST_F(PlanJoinTest, nestedOrdinaryRightDeepSlots) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  run("SELECT a.ts, b.ts, c.ts, a.c1, b.c1, c.c1 FROM st1s1 a "
      "LEFT JOIN (st1s2 b FULL JOIN st1s3 c ON b.ts = c.ts) "
      "ON a.ts = b.ts");
  EXPECT_TRUE(planContains("\"Name\":\"PhysiHashJoin\""));

  run("SELECT /*+ merge_join() */ a.ts, b.ts, c.ts FROM st1s1 a "
      "LEFT JOIN (st1s2 b FULL JOIN st1s3 c ON b.ts = c.ts) "
      "ON a.ts = b.ts");
  EXPECT_TRUE(findPlanNode("Sort"));
}

TEST_F(PlanJoinTest, hashJoinSubquerySortDoesNotSetOutputOrder) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (!optimized) return;
    auto* pHash = (const SJoinLogicNode*)findNode(
        pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pHash, nullptr);
    ASSERT_EQ(pHash->joinAlgo, JOIN_ALGO_HASH);
    EXPECT_EQ(pHash->node.outputTsOrder, TSDB_ORDER_NONE);
  });
  run("SELECT /*+ HASH_JOIN() */ a.ts FROM "
      "(SELECT ts FROM stream_t2 ORDER BY ts DESC LIMIT 10) a "
      "JOIN (SELECT ts FROM stream_t2 ORDER BY ts DESC LIMIT 10) b "
      "ON a.ts = b.ts");
}

TEST_F(PlanJoinTest, nestedHashInputDoesNotInheritLeafOrder) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  const string input =
      "(SELECT /*+ HASH_JOIN() */ a.ts, a.c1 FROM "
      "(SELECT * FROM stream_t2 ORDER BY ts DESC LIMIT 10) a JOIN "
      "(SELECT * FROM stream_t2 ORDER BY ts DESC LIMIT 10) b "
      "ON a.c1 = 2 - b.c1) q";
  const string peer =
      "(SELECT * FROM stream_t2 ORDER BY ts DESC LIMIT 10) c";
  for (bool right : {false, true}) {
    for (bool merge : {false, true}) {
      setLogicPlanCheck([right, merge](const SLogicNode* pRoot,
                                      bool optimized) {
        if (!optimized) return;
        auto* pJoin = (const SJoinLogicNode*)findNode(
            pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
        ASSERT_NE(pJoin, nullptr);
        EXPECT_EQ(pJoin->joinAlgo, merge ? JOIN_ALGO_MERGE : JOIN_ALGO_HASH);
        auto* pInput = (const SLogicNode*)nodesListGetNode(
            pJoin->node.pChildren, right ? 1 : 0);
        ASSERT_NE(pInput, nullptr);
        if (merge) {
          ASSERT_EQ(nodeType(pInput), QUERY_NODE_LOGIC_PLAN_SORT);
          EXPECT_EQ(pInput->outputTsOrder, ORDER_DESC);
        }
        auto* pHash = (const SJoinLogicNode*)findNode(
            pInput, QUERY_NODE_LOGIC_PLAN_JOIN);
        ASSERT_NE(pHash, nullptr);
        EXPECT_EQ(pHash->joinAlgo, JOIN_ALGO_HASH);
      });
      const string sql = "SELECT " +
          string(merge ? "/*+ MERGE_JOIN() */ " : "") +
          "q.c1, c.c1 FROM " + (right ? peer : input) + " LEFT JOIN " +
          (right ? input : peer) + " ON q.ts = c.ts";
      SCOPED_TRACE(sql);
      run(sql);
    }
  }
}

TEST_F(PlanJoinTest, nestedHashInputBlocksStoredOrderFallback) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (!optimized) return;
    auto* pJoin = (const SJoinLogicNode*)findNode(
        pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    EXPECT_EQ(pJoin->joinAlgo, JOIN_ALGO_HASH);
  });
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  for (const string order : {"ASC", "DESC"}) {
    for (const string condition : {"a.ts = b.ts", "a.c1 = 2 - b.c1"}) {
      const string input =
          "(SELECT /*+ HASH_JOIN() */ a.ts, a.c1 FROM "
          "(SELECT * FROM stream_t2 ORDER BY ts " + order +
          " LIMIT 10) a JOIN "
          "(SELECT * FROM stream_t2 ORDER BY ts " + order +
          " LIMIT 10) b ON " + condition + ") q";
      for (bool right : {false, true}) {
        const string sql = "SELECT q.c1, c.c1 FROM " +
            (right ? "stream_t2 c" : input) + " LEFT JOIN " +
            (right ? input : "stream_t2 c") + " ON q.ts = c.ts";
        SCOPED_TRACE(sql);
        run(sql);
      }
    }
  }
  const string mixedInput =
      "q.c1, c.c1 FROM (SELECT /*+ HASH_JOIN() */ a.ts, a.c1 "
      "FROM ((SELECT * FROM stream_t2 ORDER BY ts ASC LIMIT 10) a "
      "LEFT ASOF JOIN stream_t2 b ON a.ts >= b.ts) "
      "JOIN stream_t2 d ON a.c1 = 2 - d.c1) q "
      "LEFT JOIN stream_t2 c ON q.ts = c.ts";
  run("SELECT " + mixedInput);
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (!optimized) return;
    auto* pJoin = (const SJoinLogicNode*)findNode(
        pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    EXPECT_EQ(pJoin->joinAlgo, JOIN_ALGO_MERGE);
    auto* pInput = nodesListGetNode(pJoin->node.pChildren, 0);
    ASSERT_NE(pInput, nullptr);
    EXPECT_EQ(nodeType(pInput), QUERY_NODE_LOGIC_PLAN_SORT);
  });
  run("SELECT /*+ MERGE_JOIN() */ " + mixedInput);
}

TEST_F(PlanJoinTest, temporalJoinWithFromSubquery) {
  useDb("root", "test");

  run("SELECT l.c1, r.c1 FROM stream_t2 l LEFT ASOF JOIN "
      "(SELECT ts, c1 FROM stream_t2) r");
  ASSERT_TRUE(planContains("\"LeftPrimSlotId\":\"1\""));
  ASSERT_TRUE(planContains("\"RightPrimSlotId\":\"0\""));

  run("SELECT l.c1, r.c1 FROM stream_t2 l LEFT WINDOW JOIN "
      "(SELECT c1, c3 event_ts FROM stream_t2 ORDER BY event_ts) r "
      "WINDOW_OFFSET(-1s, 1s)");
  ASSERT_TRUE(planContains(
      "\"OpType\":\"" + std::to_string(OP_TYPE_IS_NOT_NULL) + "\""));
  ASSERT_TRUE(planContains("\"RightPrimSlotId\":\"1\""));

  run("SELECT l.event_ts, r.event_ts FROM "
      "(SELECT ts event_ts FROM stream_t2 ORDER BY event_ts DESC) l "
      "RIGHT ASOF JOIN "
      "(SELECT ts event_ts FROM stream_t2 ORDER BY event_ts DESC) r");
  ASSERT_TRUE(planContains(
      "\"InputOrder\":\"2\",\"OutputOrder\":\"2\","));
}

TEST_F(PlanJoinTest, temporalJoinPrefersPrimaryTime) {
  useDb("root", "test");
  struct Input {
    string sql;
    string key;
    bool primary;
  };
  const vector<Input> inputs = {
      {"SELECT now() anchor, * FROM stream_t2", "ts", true},
      {"SELECT c3, ts, c1 FROM stream_t2", "ts", true},
      {"SELECT c3 other_ts, ts time_key, c1 FROM stream_t2 "
       "ORDER BY other_ts DESC LIMIT 3", "time_key", true},
      {"SELECT anchor, time_key, c1 FROM (SELECT now() anchor, "
       "ts time_key, c1 FROM stream_t2) s ORDER BY c1 DESC",
       "time_key", true},
      {"SELECT now() anchor, ts first_key, ts second_key, c1 "
       "FROM stream_t2", "first_key", true},
      {"SELECT c3 fallback, ts + 1s shifted, c1 FROM stream_t2",
       "shifted", true},
      {"SELECT anchor, time_key, c1 FROM "
       "(SELECT c3 anchor, ts + 1s time_key, c1 FROM stream_t2) s",
       "time_key", true},
      {"SELECT anchor, time_key, c1 FROM "
       "(SELECT c3 anchor, ts + 1s time_key, c1 FROM stream_t2 "
       "ORDER BY ts LIMIT 2) s", "time_key", true},
      {"SELECT c3 + 1s fallback, c3 other_time, c1 FROM stream_t2",
       "fallback", false},
      {"SELECT c3 fallback, first(ts) time_key, c1 FROM stream_t2",
       "time_key", true},
      {"SELECT c3 fallback, last(ts) time_key, c1 FROM stream_t2",
       "time_key", true},
      {"SELECT c3 fallback, c1 FROM stream_t2", "fallback", false},
      {"SELECT now() fallback, c3, c1 FROM stream_t2", "fallback", false},
  };
  for (const auto& input : inputs) {
    setLogicPlanCheck([&](const SLogicNode* root, bool optimized) {
      if (optimized) return;
      auto* join = (const SJoinLogicNode*)findNode(
          root, QUERY_NODE_LOGIC_PLAN_JOIN);
      ASSERT_NE(join, nullptr);
      auto* condition = (SOperatorNode*)join->addPrimEqCond;
      ASSERT_NE(condition, nullptr);
      for (SNode* key : {condition->pLeft, condition->pRight}) {
        ASSERT_EQ(nodeType(key), QUERY_NODE_COLUMN);
        EXPECT_STREQ(((SColumnNode*)key)->colName, input.key.c_str());
        EXPECT_EQ(((SColumnNode*)key)->isPrimTs, input.primary);
      }
    });
    for (const string join : {"LEFT ASOF", "RIGHT ASOF", "LEFT WINDOW",
                             "RIGHT WINDOW"}) {
      const string suffix = join.find("WINDOW") == string::npos ? "" :
                            " WINDOW_OFFSET(-1s, 1s)";
      const string sql = "SELECT l.c1, r.c1 FROM (" + input.sql +
                         ") l " + join + " JOIN (" + input.sql + ") r" +
                         suffix;
      SCOPED_TRACE(sql);
      run(sql);
    }
    for (bool subqueryOnLeft : {true, false}) {
      setLogicPlanCheck([&](const SLogicNode* root, bool optimized) {
        if (!optimized) return;
        expectTemporalJoinTimeKeyContract(root);
        auto* join = (const SJoinLogicNode*)findNode(
            root, QUERY_NODE_LOGIC_PLAN_JOIN);
        ASSERT_NE(join, nullptr);
        auto* condition = (const SOperatorNode*)join->addPrimEqCond;
        ASSERT_NE(condition, nullptr);
        SNode* key = subqueryOnLeft ? condition->pLeft : condition->pRight;
        ASSERT_EQ(nodeType(key), QUERY_NODE_COLUMN);
        EXPECT_STREQ(((SColumnNode*)key)->tableAlias, "q");
        EXPECT_STREQ(((SColumnNode*)key)->colName, input.key.c_str());
      });
      setPhysiPlanCheck([](const SQueryPlan* plan) {
        expectPhysiJoinDirectChildContract(plan);
      });
      const string subquery = "(" + input.sql + ") q";
      const string nested =
          "(stream_t2 a JOIN stream_t2 b ON a.ts = b.ts)";
      const string left = subqueryOnLeft ? subquery : nested;
      const string right = subqueryOnLeft ? nested : subquery;
      for (const string join : {"LEFT ASOF", "RIGHT ASOF", "LEFT WINDOW",
                               "RIGHT WINDOW"}) {
        const string suffix = join.find("WINDOW") == string::npos ? "" :
                              " WINDOW_OFFSET(-1s, 1s)";
        const string sql = "SELECT q.c1, a.c1 FROM " + left + " " + join +
                           " JOIN " + right + suffix;
        SCOPED_TRACE(sql);
        run(sql);
      }
    }
  }
}

TEST_F(PlanJoinTest, temporalExpressionTimeIgnoresProjectionDepth) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* root, bool optimized) {
    if (!optimized) return;
    expectTemporalJoinTimeKeyContract(root);
    auto* join = (const SJoinLogicNode*)findNode(
        root, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(join, nullptr);
    auto* condition = (const SOperatorNode*)join->addPrimEqCond;
    ASSERT_NE(condition, nullptr);
    ASSERT_EQ(nodeType(condition->pLeft), QUERY_NODE_COLUMN);
    auto* key = (const SColumnNode*)condition->pLeft;
    EXPECT_STREQ(key->tableAlias, "q");
    EXPECT_STREQ(key->colName, "time_key");
  });
  setPhysiPlanCheck([](const SQueryPlan* plan) {
    expectPhysiJoinDirectChildContract(plan);
  });
  for (const string expr : {"a.ts + 1s", "a.ts - 1s",
                            "timetruncate(a.ts, 1s)"}) {
    for (bool joinSource : {false, true}) {
      string source = "SELECT a.c3 anchor, " + expr +
                      " time_key, a.c1 FROM stream_t2 a";
      if (joinSource) {
        source += " JOIN stream_t2 b ON a.c1 = b.c1";
      }
      for (int depth = 0; depth < 3; ++depth) {
        for (const string peer : {
                 "stream_t2 c",
                 "(stream_t2 c JOIN stream_t2 d ON c.c1 = d.c1)"}) {
          for (const string join : {"LEFT ASOF", "RIGHT ASOF",
                                    "LEFT WINDOW", "RIGHT WINDOW"}) {
            string sql = "SELECT q.c1 FROM (" + source + ") q " + join +
                         " JOIN " + peer;
            if (join.find("WINDOW") != string::npos) {
              sql += " WINDOW_OFFSET(-1s, 1s)";
            }
            SCOPED_TRACE(sql);
            run(sql);
          }
        }
        source = "SELECT anchor, time_key, c1 FROM (" + source + ") s";
      }
    }
  }
}

TEST_F(PlanJoinTest, temporalJoinRedundantOutputSort) {
  useDb("root", "test");
  for (const string join : {"LEFT ASOF", "RIGHT ASOF", "LEFT WINDOW",
                            "RIGHT WINDOW"}) {
    const string probe = join.find("LEFT") == 0 ? "l" : "r";
    const string build = probe == "l" ? "r" : "l";
    const string suffix = join.find("WINDOW") == string::npos ? "" :
                          " WINDOW_OFFSET(-1s, 1s)";
    const string sql = "SELECT l.ts, r.ts FROM stream_t2 l " + join +
                       " JOIN stream_t2 r";
    SCOPED_TRACE(join);
    run(sql + suffix + " ORDER BY " + probe + ".ts");
    EXPECT_EQ(countPlanNode("Sort"), 0);
    run(sql + suffix + " ORDER BY " + probe + ".ts DESC");
    EXPECT_EQ(countPlanNode("Sort"), 1);
    run(sql + suffix + " ORDER BY " + build + ".ts");
    EXPECT_EQ(countPlanNode("Sort"), 1);
    run(sql + " ON l.c1 = r.c1" + suffix + " ORDER BY " + probe + ".ts");
    EXPECT_EQ(countPlanNode("Sort"), 1);
    run("SELECT l.ts, r.ts FROM "
        "(SELECT ts FROM stream_t2 ORDER BY ts DESC LIMIT 3) l " + join +
        " JOIN (SELECT ts FROM stream_t2 ORDER BY ts DESC LIMIT 3) r" +
        suffix + " ORDER BY " + probe + ".ts DESC");
    EXPECT_EQ(countPlanNode("Sort"), 0);
  }
  setLogicPlanCheck([](const SLogicNode* root, bool optimized) {
    auto* sort = (const SSortLogicNode*)findNode(
        root, QUERY_NODE_LOGIC_PLAN_SORT);
    ASSERT_NE(sort, nullptr);
    EXPECT_FALSE(sort->joinInputSort);
    auto* key = (SOrderByExprNode*)nodesListGetNode(sort->pSortKeys, 0);
    ASSERT_EQ(nodeType(key->pExpr), QUERY_NODE_COLUMN);
    EXPECT_STREQ(((SColumnNode*)key->pExpr)->tableAlias, "l");
    EXPECT_STREQ(((SColumnNode*)key->pExpr)->colName, "ts");
    auto* join = findNode(root, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(join, nullptr);
    EXPECT_EQ(join->outputTsOrder, TSDB_ORDER_NONE);
  });
  run("SELECT l.ts FROM stream_t2 l LEFT WINDOW JOIN stream_t2 r "
      "WINDOW_TIME(l.c3, r.c3) WINDOW_OFFSET(-1s, 1s) ORDER BY l.ts");
}

TEST_F(PlanJoinTest, postSplitSortIgnoresPrimaryRuleHistory) {
  for (bool skipped : {false, true}) {
    SCOPED_TRACE(skipped);
    SMergeLogicNode* merge = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE, (SNode**)&merge),
              TSDB_CODE_SUCCESS);
    SColumnNode column = {};
    setNodeType(&column, QUERY_NODE_COLUMN);
    column.colId = PRIMARYKEY_TIMESTAMP_COL_ID;
    column.isPrimTs = true;
    merge->node.outputTsOrder = ORDER_ASC;
    merge->node.resultDataOrder = DATA_ORDER_LEVEL_GLOBAL;
    SLogicNode* sort = nullptr;
    ASSERT_EQ(planCreateJoinInputSort((SLogicNode*)merge, (SNode*)&column,
                                      ORDER_ASC, &sort), TSDB_CODE_SUCCESS);
    ((SSortLogicNode*)sort)->skipPKSortOpt = skipped;
    SLogicSubplan subplan = {};
    subplan.pNode = sort;
    ASSERT_EQ(postSplitOptimize(nullptr, &subplan), TSDB_CODE_SUCCESS);
    EXPECT_EQ(nodeType(subplan.pNode), QUERY_NODE_LOGIC_PLAN_MERGE);
    nodesDestroyNode((SNode*)subplan.pNode);
  }
}

TEST_F(PlanJoinTest, correctiveSortSurvivesCloneAndPostSplit) {
  SMergeLogicNode* merge = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_MERGE, (SNode**)&merge),
            TSDB_CODE_SUCCESS);
  SColumnNode column = {};
  setNodeType(&column, QUERY_NODE_COLUMN);
  column.isPrimTs = true;
  merge->node.outputTsOrder = ORDER_ASC;
  merge->node.resultDataOrder = DATA_ORDER_LEVEL_GLOBAL;
  SLogicNode* sort = nullptr;
  ASSERT_EQ(planCreateJoinInputSort((SLogicNode*)merge, (SNode*)&column,
                                    ORDER_ASC, &sort), TSDB_CODE_SUCCESS);
  ((SSortLogicNode*)sort)->joinInputSort = true;
  unique_ptr<SNode, decltype(&nodesDestroyNode)> original((SNode*)sort,
                                                          nodesDestroyNode);
  SNode* copy = nullptr;
  ASSERT_EQ(nodesCloneNode((SNode*)sort, &copy), TSDB_CODE_SUCCESS);
  auto* child = (SLogicNode*)nodesListGetNode(
      ((SLogicNode*)copy)->pChildren, 0);
  child->pParent = (SLogicNode*)copy;
  SLogicSubplan subplan = {};
  subplan.pNode = (SLogicNode*)copy;
  ASSERT_EQ(postSplitOptimize(nullptr, &subplan), TSDB_CODE_SUCCESS);
  EXPECT_EQ(nodeType(subplan.pNode), QUERY_NODE_LOGIC_PLAN_SORT);
  nodesDestroyNode((SNode*)subplan.pNode);

  char* json = nullptr;
  int32_t length = 0;
  ASSERT_EQ(nodesNodeToString((SNode*)sort, false, &json, &length),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesStringToNode(json, &copy), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(((SSortLogicNode*)copy)->joinInputSort);
  nodesDestroyNode(copy);
  taosMemoryFree(json);
}

static int32_t skipTemporalSortElimination(SLogicSubplan*, bool* optimized) {
  *optimized = false;
  return TSDB_CODE_SUCCESS;
}

TEST_F(PlanJoinTest, temporalJoinWithoutRedundantSortRule) {
  Stub stub;
  stub.set(planEliminateTemporalInputSort, skipTemporalSortElimination);
  useDb("root", "test");
  for (const string join : {"LEFT ASOF", "RIGHT ASOF", "LEFT WINDOW",
                            "RIGHT WINDOW"}) {
    SCOPED_TRACE(join);
    run("SELECT l.c1, r.c1 FROM (SELECT ts, c1 FROM stream_t2 "
        "ORDER BY c1 DESC) l " + join + " JOIN stream_t2 r" +
        (join.find("WINDOW") == string::npos ? "" :
         " WINDOW_OFFSET(-1s, 1s)"));
    EXPECT_EQ(countPlanNode("Sort"), 2);
  }
}

TEST_F(PlanJoinTest, temporalJoinFederatedPrecision) {
  const bool previous = tsFederatedQueryEnable;
  struct RestoreFlag {
    bool value;
    ~RestoreFlag() { tsFederatedQueryEnable = value; }
  } restore{previous};
  tsFederatedQueryEnable = true;
  g_mockCatalogService->createExtSource("join_remote", EXT_SOURCE_MYSQL,
                                        "127.0.0.1", 1, "user", "db", "");
  vector<MockCatalogService::MockExtColDef> columns = {
      {"ts", TSDB_DATA_TYPE_TIMESTAMP, true, false, false, "datetime"},
      {"c1", TSDB_DATA_TYPE_INT, false, true, false, "int"}};
  g_mockCatalogService->createExtTable("join_remote", "", "", "metrics",
                                       columns, TSDB_TIME_PRECISION_MICRO);
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  setLogicPlanCheck([](const SLogicNode* root, bool optimized) {
    if (optimized) return;
    auto* join = findNode(root, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(join, nullptr);
    auto* condition = (SOperatorNode*)((SJoinLogicNode*)join)->addPrimEqCond;
    ASSERT_NE(condition, nullptr);
    ASSERT_EQ(nodeType(condition->pLeft), QUERY_NODE_FUNCTION);
    EXPECT_STREQ(((SFunctionNode*)condition->pLeft)->functionName,
                 "__timestamp_scale");
    auto* left = (SLogicNode*)nodesListGetNode(join->pChildren, 0);
    ASSERT_EQ(nodeType(left), QUERY_NODE_LOGIC_PLAN_SCAN);
    EXPECT_EQ(left->pConditions, nullptr);
    auto* right = (SLogicNode*)nodesListGetNode(join->pChildren, 1);
    ASSERT_EQ(nodeType(right), QUERY_NODE_LOGIC_PLAN_SORT);
    auto* scan = (SScanLogicNode*)nodesListGetNode(right->pChildren, 0);
    ASSERT_EQ(nodeType(scan), QUERY_NODE_LOGIC_PLAN_SCAN);
    EXPECT_EQ(scan->scanType, SCAN_TYPE_EXTERNAL);
  });
  run("SELECT l.c1, r.c1 FROM stream_t2 l LEFT WINDOW JOIN "
      "join_remote.metrics r WINDOW_OFFSET(-1s, 1s)");
  run("SELECT l.c1, r.c1 FROM stream_t2 l LEFT ASOF JOIN "
      "join_remote.metrics r");
}

TEST_F(PlanJoinTest, temporalJoinFederatedFilteredOrder) {
  struct RestoreFlag {
    bool value;
    ~RestoreFlag() { tsFederatedQueryEnable = value; }
  } restore{tsFederatedQueryEnable};
  tsFederatedQueryEnable = true;
  g_mockCatalogService->createExtSource("ordered_remote", EXT_SOURCE_INFLUXDB,
                                       "127.0.0.1", 1, "user", "db", "");
  vector<MockCatalogService::MockExtColDef> columns = {
      {"ts", TSDB_DATA_TYPE_TIMESTAMP, true, false, false, "Timestamp"},
      {"c1", TSDB_DATA_TYPE_BIGINT, false, true, false, "Int64"}};
  g_mockCatalogService->createExtTable("ordered_remote", "", "", "metrics",
                                      columns, TSDB_TIME_PRECISION_NANO);
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* root, bool optimized) {
    if (!optimized) return;
    auto* join = findNode(root, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(join, nullptr);
    int filteredInputs = 0;
    SNode* child = nullptr;
    FOREACH(child, join->pChildren) {
      ASSERT_EQ(QUERY_NODE_LOGIC_PLAN_SCAN, nodeType(child));
      auto* scan = (const SScanLogicNode*)child;
      ASSERT_EQ(SCAN_TYPE_EXTERNAL, scan->scanType);
      auto* remote = (const SLogicNode*)scan->pRemoteLogicPlan;
      ASSERT_NE(nullptr, nodesGetRemoteQueryOutputSort(remote));
      EXPECT_EQ(DATA_ORDER_LEVEL_GLOBAL, scan->node.resultDataOrder);
      if (remote->pConditions == nullptr) continue;
      ++filteredInputs;
      char* sql = nullptr;
      ASSERT_EQ(TSDB_CODE_SUCCESS,
                nodesRemotePlanToSQL(remote, EXT_SOURCE_INFLUXDB, nullptr,
                                     &sql));
      string text(sql);
      taosMemoryFree(sql);
      SCOPED_TRACE(text);
      auto where = text.rfind(") _sq WHERE ");
      ASSERT_NE(string::npos, where);
      EXPECT_GT(text.rfind(" ORDER BY "), where);
    }
    EXPECT_EQ(1, filteredInputs);
  });
  for (string side : {"LEFT", "RIGHT"}) {
    string kept = side == "LEFT" ? "l" : "r";
    for (string kind : {"ASOF", "WINDOW"}) {
      run("SELECT l.c1, r.c1 FROM ordered_remote.metrics l " + side + " " +
          kind + " JOIN ordered_remote.metrics r" +
          (kind == "WINDOW" ? " WINDOW_OFFSET(-2m, 2m)" : "") +
          " WHERE " + kept + ".c1 > 20 ORDER BY " + kept + ".ts");
    }
  }
}

TEST_F(PlanJoinTest, temporalJoinFederatedSubqueryTimeSlot) {
  struct RestoreFlag {
    bool value;
    ~RestoreFlag() { tsFederatedQueryEnable = value; }
  } restore{tsFederatedQueryEnable};
  tsFederatedQueryEnable = true;
  g_mockCatalogService->createExtSource("slot_remote", EXT_SOURCE_MYSQL,
                                       "127.0.0.1", 1, "user", "db", "");
  vector<MockCatalogService::MockExtColDef> columns = {
      {"ts", TSDB_DATA_TYPE_TIMESTAMP, true, false, false, "datetime"},
      {"c1", TSDB_DATA_TYPE_INT, false, true, false, "int"}};
  g_mockCatalogService->createExtTable("slot_remote", "", "", "metrics",
                                      columns, TSDB_TIME_PRECISION_MICRO);
  useDb("root", "test");

  auto inspect = [](const SQueryPlan* plan) {
    int joins = 0;
    function<void(const SPhysiNode*)> visit = [&](const SPhysiNode* node) {
      if (nodeType(node) == QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN) {
        ++joins;
        auto* join = (const SSortMergeJoinPhysiNode*)node;
        ASSERT_EQ(2, LIST_LENGTH(node->pChildren));
        bool left = join->joinType == JOIN_TYPE_LEFT;
        for (int side = 0; side < 2; ++side) {
          auto* input = (const SPhysiNode*)nodesListGetNode(
              node->pChildren, side);
          int64_t slotId = side == 0 ? join->leftPrimSlotId :
                                      join->rightPrimSlotId;
          ASSERT_GE(slotId, 0);
          ASSERT_LT(slotId, LIST_LENGTH(input->pOutputDataBlockDesc->pSlots));
          auto* slot = (const SSlotDescNode*)nodesListGetNode(
              input->pOutputDataBlockDesc->pSlots, slotId);
          // The projected time key must not bind to a new reserve slot.
          // FedScan may mirror b.ts into an existing reserve scan slot.
          if (side == (left ? 0 : 1)) EXPECT_TRUE(slot->output);
          EXPECT_EQ(TSDB_DATA_TYPE_TIMESTAMP, slot->dataType.type);
          EXPECT_EQ(TSDB_TIME_PRECISION_MICRO, slot->dataType.precision);
        }

        // The matching key and payload have independent slot bindings. Verify
        // that b.c1 still comes from the non-preserved input.
        auto* other = (const SPhysiNode*)nodesListGetNode(
            node->pChildren, left ? 1 : 0);
        int otherValues = 0;
        SNode* targetNode = nullptr;
        FOREACH(targetNode, join->pTargets) {
          ASSERT_EQ(QUERY_NODE_TARGET, nodeType(targetNode));
          auto* expr = ((const STargetNode*)targetNode)->pExpr;
          if (nodeType(expr) != QUERY_NODE_COLUMN) continue;
          auto* column = (const SColumnNode*)expr;
          if (string(column->tableAlias) != "b" ||
              string(column->colName) != "c1") continue;
          ++otherValues;
          EXPECT_EQ(other->pOutputDataBlockDesc->dataBlockId,
                    column->dataBlockId);
          ASSERT_GE(column->slotId, 0);
          ASSERT_LT(column->slotId,
                    LIST_LENGTH(other->pOutputDataBlockDesc->pSlots));
        }
        EXPECT_EQ(1, otherValues);
        if (join->subType == JOIN_STYPE_WIN) {
          auto* offset = (const SWindowOffsetNode*)join->pWindowOffset;
          ASSERT_NE(nullptr, offset);
          EXPECT_EQ(-120000000, ((SValueNode*)offset->pStartOffset)->datum.i);
          EXPECT_EQ(120000000, ((SValueNode*)offset->pEndOffset)->datum.i);
        }
      }
      SNode* child = nullptr;
      FOREACH(child, node->pChildren) {
        visit((const SPhysiNode*)child);
      }
    };
    SNode* level = nullptr;
    FOREACH(level, plan->pSubplans) {
      SNode* subplan = nullptr;
      FOREACH(subplan, ((SNodeListNode*)level)->pNodeList) {
        visit(((SSubplan*)subplan)->pNode);
      }
    }
    EXPECT_EQ(1, joins);
  };

  for (string order : {"ASC", "DESC"}) {
    string scan = "SELECT ts AS event_time, c1 FROM slot_remote.metrics "
                  "ORDER BY ts " + order + " LIMIT 3 OFFSET 1";
    for (string input : {"(" + scan + ") a",
                         "(SELECT event_time, c1 FROM (" + scan + ") s) a"}) {
      for (string side : {"LEFT", "RIGHT"}) {
        for (string kind : {"ASOF", "ASOF ON", "WINDOW"}) {
          string join = kind == "WINDOW" ? "WINDOW" : "ASOF";
          for (string sort : {" ORDER BY a.event_time, b.ts",
                              " ORDER BY a.c1", ""}) {
            for (string predicate : {"SELECT", "WHERE", "ON"}) {
              string clause = kind == "ASOF ON" ?
                              " ON a.event_time >= b.ts" : "";
              if (predicate == "ON") {
                clause += clause.empty() ? " ON a.c1 = b.c1" :
                                           " AND a.c1 = b.c1";
              }
              if (kind == "WINDOW") clause += " WINDOW_OFFSET(-2m, 2m)";
              string sql = (predicate == "SELECT" ?
                            "SELECT a.c1, b.c1 FROM " :
                            "SELECT a.c1 AS c1 FROM ") +
                  (side == "LEFT" ? input : "slot_remote.metrics b") + " " +
                  side + " " + join + " JOIN " +
                  (side == "LEFT" ? "slot_remote.metrics b" : input) +
                  clause + (predicate == "WHERE" ? " WHERE a.c1 < b.c1" :
                                                  " WHERE a.c1 > 20") + sort;
              SCOPED_TRACE(sql);
              runFullPlan(sql, inspect);
            }
          }
        }
      }
    }
  }
}

TEST_F(PlanJoinTest, temporalJoinHiddenConditionInputSlot) {
  auto make = [](ENodeType type) {
    SNode* node = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(type, &node));
    return node;
  };
  auto column = [&](const char* alias, const char* name, int8_t type) {
    auto* col = (SColumnNode*)make(QUERY_NODE_COLUMN);
    tstrncpy(col->tableAlias, alias, sizeof(col->tableAlias));
    tstrncpy(col->colName, name, sizeof(col->colName));
    tstrncpy(col->node.aliasName, name, sizeof(col->node.aliasName));
    col->node.resType.type = type;
    col->node.resType.bytes = tDataTypes[type].bytes;
    return (SNode*)col;
  };
  for (int test = 0; test < 12; ++test) {
    int condition = test % 3;
    bool missingRight = (test / 3) % 2 != 0;
    bool multipleAliases = test >= 6;
    SCOPED_TRACE(test);
    auto* subplan = (SLogicSubplan*)make(QUERY_NODE_LOGIC_SUBPLAN);
    unique_ptr<SNode, decltype(&nodesDestroyNode)> logicOwner(
        (SNode*)subplan, nodesDestroyNode);
    subplan->subplanType = SUBPLAN_TYPE_MERGE;
    auto* join = (SJoinLogicNode*)make(QUERY_NODE_LOGIC_PLAN_JOIN);
    subplan->pNode = (SLogicNode*)join;
    join->joinType = JOIN_TYPE_LEFT;
    join->subType = JOIN_STYPE_ASOF;
    join->joinAlgo = JOIN_ALGO_MERGE;
    // Keep input ownership but model remote payloads with bare output names.
    for (const char* alias : {"a", "b"}) {
      auto* input = (SExchangeLogicNode*)make(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
      ASSERT_EQ(0, nodesListMakeAppend(&join->node.pChildren, (SNode*)input));
      if (multipleAliases && string(alias) == "b") {
        ASSERT_EQ(0, nodesListMakeAppend(&input->node.pTargets,
                                        column("inner", "other",
                                               TSDB_DATA_TYPE_INT)));
      }
      ASSERT_EQ(0, nodesListMakeAppend(&input->node.pTargets,
                                      column(alias, "ts",
                                             TSDB_DATA_TYPE_TIMESTAMP)));
      if (!missingRight || string(alias) != "b") {
        ASSERT_EQ(0, nodesListMakeAppend(&input->node.pTargets,
                                        column("", "c1", TSDB_DATA_TYPE_INT)));
      }
    }
    auto* time = (SOperatorNode*)make(QUERY_NODE_OPERATOR);
    time->opType = OP_TYPE_GREATER_EQUAL;
    time->pLeft = column("a", "ts", TSDB_DATA_TYPE_TIMESTAMP);
    time->pRight = column("b", "ts", TSDB_DATA_TYPE_TIMESTAMP);
    join->addPrimEqCond = (SNode*)time;
    ASSERT_EQ(0, nodesListMakeAppend(&join->node.pTargets,
                                    column("a", "c1", TSDB_DATA_TYPE_INT)));
    auto* predicate = (SOperatorNode*)make(QUERY_NODE_OPERATOR);
    predicate->opType = OP_TYPE_EQUAL;
    predicate->pLeft = column("a", "c1", TSDB_DATA_TYPE_INT);
    predicate->pRight = column("b", "c1", TSDB_DATA_TYPE_INT);
    SNode** conditions[] = {&join->pFullOnCond, &join->pColOnCond,
                            &join->node.pConditions};
    *conditions[condition] = (SNode*)predicate;

    SQueryLogicPlan logic = {};
    ASSERT_EQ(0, nodesListMakeAppend(&logic.pTopSubplans, (SNode*)subplan));
    unique_ptr<SNodeList, decltype(&nodesClearList)> listOwner(
        logic.pTopSubplans, nodesClearList);
    SSelectStmt ast = {};
    ast.node.type = QUERY_NODE_SELECT_STMT;
    SPlanContext context = {};
    context.pAstRoot = (SNode*)&ast;
    SQueryPlan* plan = nullptr;
    unique_ptr<SArray, decltype(&taosArrayDestroy)> execNodes(
        taosArrayInit(1, sizeof(SQueryNodeAddr)), taosArrayDestroy);
    int32_t code = createPhysiPlan(&context, &logic, &plan, execNodes.get());
    unique_ptr<SNode, decltype(&nodesDestroyNode)> planOwner(
        (SNode*)plan, nodesDestroyNode);
    if (missingRight) {
      EXPECT_EQ(TSDB_CODE_PLAN_SLOT_NOT_FOUND, code);
      continue;
    }
    ASSERT_EQ(TSDB_CODE_SUCCESS, code);
    auto* level = (SNodeListNode*)nodesListGetNode(plan->pSubplans, 0);
    auto* physical = (SSubplan*)nodesListGetNode(level->pNodeList, 0);
    auto* result = (SSortMergeJoinPhysiNode*)physical->pNode;
    auto* right = (SPhysiNode*)nodesListGetNode(result->node.pChildren, 1);
    ASSERT_EQ(2, LIST_LENGTH(result->pTargets));
    auto* target = (STargetNode*)nodesListGetNode(result->pTargets, 1);
    auto* hidden = (SColumnNode*)target->pExpr;
    EXPECT_STREQ("b", hidden->tableAlias);
    EXPECT_EQ(right->pOutputDataBlockDesc->dataBlockId, hidden->dataBlockId);
    EXPECT_EQ(multipleAliases ? 2 : 1, hidden->slotId);
  }
}

TEST_F(PlanJoinTest, temporalJoinWithVirtualTable) {
  useDb("root", "test");

  run("SELECT v.c1, r.c1 FROM v_stream_t2 v LEFT ASOF JOIN "
      "stream_t2 r");
  ASSERT_TRUE(planContains("\"LeftPrimSlotId\":\"1\""));
  ASSERT_TRUE(planContains(
      "\"TableAlias\":\"v\",\"ColName\":\"ts\""));
}

TEST_F(PlanJoinTest, windowJoinWithExplicitTimeColumns) {
  useDb("root", "test");

  run("SELECT l.c1, r.c1 FROM stream_t2 l LEFT WINDOW JOIN "
      "(SELECT ts source_ts, c3 event_ts, c1 FROM stream_t2 "
      "ORDER BY event_ts) r WINDOW_TIME(l.ts, r.event_ts) "
      "WINDOW_OFFSET(-1s, 1s)");
  ASSERT_TRUE(planContains("\"RightPrimSlotId\":\"0\""));
  ASSERT_TRUE(planContains("\"UserAlias\":\"event_ts\""));
}

TEST_F(PlanJoinTest, temporalJoinInputOrderAlignment) {
  useDb("root", "test");
  run("SELECT l.c1, r.c1 FROM "
      "(SELECT ts, c1 FROM stream_t2 ORDER BY c1 LIMIT 3) l "
      "LEFT WINDOW JOIN "
      "(SELECT ts, c1 FROM stream_t2 ORDER BY ts DESC LIMIT 2) r "
      "WINDOW_OFFSET(-1s, 1s)");
  ASSERT_TRUE(planContains("\"InputOrder\":\"2\""));
  ASSERT_TRUE(findPlanNode("Sort"));

  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    ASSERT_EQ(countSorts(pRoot), 2);
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    SNode* pChild = nullptr;
    FOREACH(pChild, pJoin->pChildren) {
      ASSERT_EQ(nodeType(pChild), QUERY_NODE_LOGIC_PLAN_SORT);
      auto* pSort = (SSortLogicNode*)pChild;
      auto* pKey = (SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0);
      ASSERT_EQ(nodeType(pKey->pExpr), QUERY_NODE_COLUMN);
      ASSERT_STREQ(((SColumnNode*)pKey->pExpr)->colName, "c3");
      ASSERT_EQ(pSort->node.outputTsOrder, TSDB_ORDER_NONE);
      ASSERT_EQ(pSort->node.resultDataOrder, DATA_ORDER_LEVEL_NONE);
    }
  });
  run("SELECT l.c1, r.c1 FROM stream_t2 l LEFT WINDOW JOIN "
      "stream_t2 r WINDOW_TIME(l.c3, r.c3) WINDOW_OFFSET(-1s, 1s)");
  ASSERT_TRUE(planContains("\"LeftPrimSlotId\":\"1\""));
  ASSERT_TRUE(planContains("\"RightPrimSlotId\":\"1\""));
}

TEST_F(PlanJoinTest, temporalJoinOrderMatrix) {
  useDb("root", "test");
  const std::string orders[] = {"c1", "ts", "ts DESC"};
  const std::string joins[] = {"LEFT ASOF", "RIGHT ASOF", "LEFT WINDOW",
                               "RIGHT WINDOW"};
  for (const auto& join : joins) {
    for (int left = 0; left < 3; ++left) {
      for (int right = 0; right < 3; ++right) {
        const EOrder order = left != 1 && right != 1 &&
                                     (left == 2 || right == 2) ?
                                 ORDER_DESC : ORDER_ASC;
        const std::string sql =
            "SELECT l.c1, r.c1 FROM (SELECT ts, c1 FROM stream_t2 "
            "ORDER BY " + orders[left] + ") l " + join +
            " JOIN (SELECT ts, c1 FROM stream_t2 ORDER BY " +
            orders[right] + ") r" +
            (join.find("WINDOW") != std::string::npos ?
                 " WINDOW_OFFSET(-1s, 1s)" : "");
        SCOPED_TRACE(sql);
        setLogicPlanCheck([&](const SLogicNode* pRoot, bool optimized) {
          auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
          ASSERT_NE(pJoin, nullptr);
          ASSERT_EQ(pJoin->inputTsOrder, order);
          ASSERT_EQ(pJoin->outputTsOrder, order);
          SNode* pNode = nullptr;
          FOREACH(pNode, pJoin->pChildren) {
            auto* pInput = (SLogicNode*)pNode;
            EXPECT_EQ(pInput->outputTsOrder, order);
            EXPECT_EQ(pInput->resultDataOrder, DATA_ORDER_LEVEL_GLOBAL);
          }
          int extra = (left == 0 || left != order) +
                      (right == 0 || right != order);
          ASSERT_EQ(countSorts(pJoin),
                    optimized ? extra : 2 + extra);
        });
        run(sql);
      }
    }
  }
}

TEST_F(PlanJoinTest, temporalJoinKeepsEffectiveOrderSeparate) {
  useDb("root", "test");
  const string orders[] = {"", " ORDER BY event_ts ASC",
                          " ORDER BY event_ts DESC"};
  for (const string join : {"LEFT ASOF", "RIGHT ASOF", "LEFT WINDOW",
                           "RIGHT WINDOW"}) {
    for (int left = 0; left < 3; ++left) {
      for (int right = 0; right < 3; ++right) {
        const EOrder order = left != 1 && right != 1 &&
                             (left == 2 || right == 2) ?
                             ORDER_DESC : ORDER_ASC;
        setLogicPlanCheck([&](const SLogicNode* root, bool optimized) {
          auto* temporal = findNode(root, QUERY_NODE_LOGIC_PLAN_JOIN);
          ASSERT_NE(temporal, nullptr);
          EXPECT_EQ(temporal->inputTsOrder, order);
          EXPECT_EQ(temporal->outputTsOrder, TSDB_ORDER_NONE);
          const int corrections = (left == 0 || left != order) +
                                  (right == 0 || right != order);
          EXPECT_EQ(countSorts(temporal),
                    (left != 0) + (right != 0) + corrections);
          SNode* node = nullptr;
          FOREACH(node, temporal->pChildren) {
            auto* input = (const SLogicNode*)node;
            while (nodeType(input) == QUERY_NODE_LOGIC_PLAN_PROJECT) {
              EXPECT_EQ(input->outputTsOrder, TSDB_ORDER_NONE);
              EXPECT_EQ(input->resultDataOrder, DATA_ORDER_LEVEL_NONE);
              ASSERT_EQ(LIST_LENGTH(input->pChildren), 1);
              input = (SLogicNode*)nodesListGetNode(input->pChildren, 0);
            }
            ASSERT_EQ(nodeType(input), QUERY_NODE_LOGIC_PLAN_SORT);
            EXPECT_EQ(input->outputTsOrder, TSDB_ORDER_NONE);
            EXPECT_EQ(input->resultDataOrder, DATA_ORDER_LEVEL_NONE);
            auto* key = (SOrderByExprNode*)nodesListGetNode(
                ((SSortLogicNode*)input)->pSortKeys, 0);
            EXPECT_EQ(key->order, order);
          }
        });
        const string input =
            "(SELECT event_ts, c1 FROM (SELECT c3 event_ts, ts, c1 "
            "FROM stream_t2) s";
        const string sql = "SELECT l.c1, r.c1 FROM " + input +
            orders[left] + " LIMIT 5) l " + join + " JOIN " + input +
            orders[right] + " LIMIT 5) r" +
            (join.find("WINDOW") == string::npos ? "" :
             " WINDOW_OFFSET(-1s, 1s)");
        SCOPED_TRACE(sql);
        run(sql);
      }
    }
  }
}

TEST_F(PlanJoinTest, temporalJoinTimeKeyAndLimits) {
  useDb("root", "test");
  for (const auto& order : {"c1 DESC", "ts DESC"}) {
    setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
      auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
      ASSERT_NE(pJoin, nullptr);
      ASSERT_EQ(pJoin->inputTsOrder, ORDER_ASC);
      auto* pLeft = (SLogicNode*)nodesListGetNode(pJoin->pChildren, 0);
      ASSERT_EQ(nodeType(pLeft), QUERY_NODE_LOGIC_PLAN_SORT);
      ASSERT_EQ(pLeft->pLimit, nullptr);
      auto* pProject = (SLogicNode*)nodesListGetNode(pLeft->pChildren, 0);
      ASSERT_EQ(nodeType(pProject), QUERY_NODE_LOGIC_PLAN_PROJECT);
      while (pProject->pLimit == nullptr &&
             LIST_LENGTH(pProject->pChildren) == 1) {
        pProject = (SLogicNode*)nodesListGetNode(pProject->pChildren, 0);
      }
      ASSERT_NE(pProject->pLimit, nullptr);
      ASSERT_EQ(((SLimitNode*)pProject->pLimit)->limit->datum.i, 3);
      ASSERT_EQ(((SLimitNode*)pProject->pLimit)->offset->datum.i, 2);
    });
    run("SELECT l.c1, r.c1 FROM (SELECT ts, c1 FROM stream_t2 ORDER BY " +
        std::string(order) + " LIMIT 3 OFFSET 2) l LEFT WINDOW JOIN "
        "(SELECT ts, c1 FROM stream_t2 ORDER BY ts) r "
        "WINDOW_OFFSET(-1s, 1s) JLIMIT 1 ORDER BY l.ts DESC");
  }

  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    auto* pRight = (SLogicNode*)nodesListGetNode(pJoin->pChildren, 1);
    ASSERT_EQ(nodeType(pRight), QUERY_NODE_LOGIC_PLAN_SORT);
    auto* pKey = (SOrderByExprNode*)nodesListGetNode(
        ((SSortLogicNode*)pRight)->pSortKeys, 0);
    ASSERT_STREQ(((SColumnNode*)pKey->pExpr)->colName, "source_ts");
  });
  run("SELECT l.c1, r.c1 FROM stream_t2 l LEFT WINDOW JOIN "
      "(SELECT ts source_ts, c3 event_ts, c1 FROM stream_t2 "
      "ORDER BY event_ts DESC) r WINDOW_OFFSET(-1s, 1s)");

  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    auto* pLeft = (SSortLogicNode*)nodesListGetNode(pJoin->pChildren, 0);
    ASSERT_EQ(nodeType(pLeft), QUERY_NODE_LOGIC_PLAN_SORT);
    auto* pKey = (SOrderByExprNode*)nodesListGetNode(pLeft->pSortKeys, 0);
    ASSERT_STREQ(((SColumnNode*)pKey->pExpr)->colName, "c3");
  });
  run("SELECT v.c1, r.c1 FROM v_stream_t2 v LEFT WINDOW JOIN "
      "stream_t2 r WINDOW_TIME(v.c3, r.c3) WINDOW_OFFSET(-1s, 1s)");
  ASSERT_TRUE(planContains("\"TableAlias\":\"v\",\"ColName\":\"c3\""));

  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    ASSERT_EQ(pJoin->pLimit, nullptr);
    ASSERT_NE(findNode(pRoot, QUERY_NODE_LOGIC_PLAN_SORT), nullptr);
  });
  run("SELECT l.ts, r.c1 FROM stream_t2 l LEFT WINDOW JOIN stream_t2 r "
      "WINDOW_TIME(l.c3, r.c3) WINDOW_OFFSET(-1s, 1s) "
      "ORDER BY l.ts LIMIT 1");

  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    ASSERT_EQ(pJoin->inputTsOrder, ORDER_DESC);
    ASSERT_EQ(countSorts(pJoin), 2);
  });
  run("SELECT l.c1, r.c1 FROM "
      "(SELECT ts, c3 event_ts, c1 FROM stream_t2 "
      "ORDER BY event_ts DESC) l LEFT WINDOW JOIN "
      "(SELECT ts, c3 event_ts, c1 FROM stream_t2 "
      "ORDER BY event_ts DESC) r WINDOW_TIME(l.event_ts, r.event_ts) "
      "WINDOW_OFFSET(-1s, 1s)");
}

TEST_F(PlanJoinTest, temporalJoinFlexibleScan) {
  useDb("root", "test");
  for (const auto& table : {"stream_t2", "st1"}) {
    for (bool leftScan : {false, true}) {
      const std::string scan = std::string(table);
      const std::string subquery =
          "(SELECT ts, c1 FROM stream_t2 ORDER BY ts DESC LIMIT 2)";
      setLogicPlanCheck([&](const SLogicNode* pRoot, bool optimized) {
        if (!optimized) return;
        auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
        ASSERT_NE(pJoin, nullptr);
        ASSERT_EQ(pJoin->inputTsOrder, ORDER_DESC);
        ASSERT_EQ(countSorts(pJoin), 0);
        auto* pScan = (SScanLogicNode*)nodesListGetNode(pJoin->pChildren,
                                                        leftScan ? 0 : 1);
        ASSERT_EQ(nodeType(pScan), QUERY_NODE_LOGIC_PLAN_SCAN);
        ASSERT_EQ(pScan->scanSeq[0], 0);
        ASSERT_EQ(pScan->scanSeq[1], 1);
        ASSERT_EQ(pScan->node.resultDataOrder, DATA_ORDER_LEVEL_GLOBAL);
      });
      run("SELECT l.c1, r.c1 FROM " + (leftScan ? scan : subquery) +
          " l LEFT ASOF JOIN " + (leftScan ? subquery : scan) + " r");
    }
  }
}

TEST_F(PlanJoinTest, temporalJoinGlobalOrder) {
  useDb("root", "test");
  EDataOrderLevel level = DATA_ORDER_LEVEL_GLOBAL;
  setLogicPlanCheck([&](const SLogicNode* pRoot, bool optimized) {
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    ASSERT_EQ(pJoin->inputTsOrder, ORDER_ASC);
    auto* pLeft = (SLogicNode*)nodesListGetNode(pJoin->pChildren, 0);
    ASSERT_EQ(nodeType(pLeft), QUERY_NODE_LOGIC_PLAN_SORT);
    EXPECT_EQ(pLeft->resultDataOrder, level);
    EXPECT_EQ(pLeft->outputTsOrder, level == DATA_ORDER_LEVEL_GLOBAL ?
                                  ORDER_ASC : TSDB_ORDER_NONE);
    auto* pSort = (SSortLogicNode*)pLeft;
    EXPECT_FALSE(pSort->groupSort);
    auto* pKey = (SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0);
    EXPECT_EQ(pKey->order, ORDER_ASC);
  });
  run("SELECT l.c1, r.c1 FROM (SELECT ts, c1 FROM st1) l "
      "LEFT ASOF JOIN stream_t2 r");
  // Set/group nodes remain outside the physical timestamp lineage proof.
  level = DATA_ORDER_LEVEL_NONE;
  run("SELECT l.c1, r.c1 FROM "
      "(SELECT ts, c1 FROM stream_t2 UNION ALL "
      "SELECT ts, c1 FROM stream_t2) l LEFT ASOF JOIN stream_t2 r");
  run("SELECT l.c1, r.c1 FROM "
      "(SELECT ts, c1 FROM st1 PARTITION BY tbname) l "
      "LEFT ASOF JOIN stream_t2 r");

  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    ASSERT_EQ(pJoin->inputTsOrder, ORDER_ASC);
    auto* pLeft = (SLogicNode*)nodesListGetNode(pJoin->pChildren, 0);
    ASSERT_EQ(nodeType(pLeft), QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN);
    ASSERT_EQ(pLeft->resultDataOrder, DATA_ORDER_LEVEL_GLOBAL);
  });
  run("SELECT v.c1, r.c1 FROM v_stream_t2 v LEFT ASOF JOIN "
      "(SELECT ts, c1 FROM stream_t2 ORDER BY ts DESC LIMIT 2) r");
}

TEST_F(PlanJoinTest, temporalJoinAggregateInputOrder) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    auto* pAgg = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_AGG);
    ASSERT_NE(pAgg, nullptr);
    ASSERT_EQ(pAgg->inputTsOrder, ORDER_ASC);
  });
  run("SELECT first(l.c1), last(r.c1) FROM stream_t2 l "
      "LEFT ASOF JOIN stream_t2 r");
}

TEST_F(PlanJoinTest, asofExplicitTimePrecedesInputOrdering) {
  useDb("root", "test");
  for (const string side : {"LEFT", "RIGHT"}) {
    for (const string op : {">", ">=", "=", "<", "<="}) {
      for (bool reversed : {false, true}) {
        SCOPED_TRACE(side + op + to_string(reversed));
        setLogicPlanCheck([](const SLogicNode* root, bool optimized) {
          auto* join = (const SJoinLogicNode*)findNode(
              root, QUERY_NODE_LOGIC_PLAN_JOIN);
          ASSERT_NE(join, nullptr);
          auto* condition = (SOperatorNode*)join->pPrimKeyEqCond;
          ASSERT_NE(condition, nullptr);
          EXPECT_EQ(join->addPrimEqCond, nullptr);
          EXPECT_STREQ(((SColumnNode*)condition->pLeft)->tableAlias, "l");
          EXPECT_STREQ(((SColumnNode*)condition->pRight)->tableAlias, "r");
          SNode* child = nullptr;
          FOREACH(child, join->node.pChildren) {
            auto* sort = (const SSortLogicNode*)child;
            ASSERT_EQ(nodeType(sort), QUERY_NODE_LOGIC_PLAN_SORT);
            ASSERT_TRUE(sort->joinInputSort);
            auto* key = (SOrderByExprNode*)nodesListGetNode(
                sort->pSortKeys, 0);
            ASSERT_EQ(nodeType(key->pExpr), QUERY_NODE_COLUMN);
            EXPECT_STREQ(((SColumnNode*)key->pExpr)->colName, "time_key");
            EXPECT_EQ(key->order, ORDER_ASC);
          }
        });
        const string input = "(SELECT c3, ts time_key, c1 FROM "
                             "stream_t2 ORDER BY c1 DESC LIMIT 3)";
        const string on = reversed ? "r.time_key " + op + " l.time_key" :
                                     "l.time_key " + op + " r.time_key";
        run("SELECT l.c1, r.c1 FROM " + input + " l " + side +
            " ASOF JOIN " + input + " r ON " + on);
      }
    }
  }
}

TEST_F(PlanJoinTest, temporalJoinTagGroupsWithCorrectiveSort) {
  useDb("root", "test");
  for (const string side : {"LEFT", "RIGHT"}) {
    for (const string keys : {"l.tag3, r.tag3", "l.ts, r.tag3",
                              "l.tag3, r.ts"}) {
      setLogicPlanCheck([](const SLogicNode* root, bool optimized) {
        if (!optimized) return;
        auto* join = findNode(root, QUERY_NODE_LOGIC_PLAN_JOIN);
        ASSERT_NE(join, nullptr);
        SNode* child = nullptr;
        FOREACH(child, join->pChildren) {
          EXPECT_EQ(nodeType(child), QUERY_NODE_LOGIC_PLAN_PARTITION);
        }
      });
      run("SELECT /*+ NO_BATCH_SCAN() */ l.c1, r.c1 FROM st1 l " +
          side + " WINDOW JOIN st1 r ON l.tag1 = r.tag1 "
          "WINDOW_TIME(" + keys + ") WINDOW_OFFSET(-1s, 1s)");
    }
  }
}

TEST_F(PlanJoinTest, asofExplicitTimeInputDirections) {
  useDb("root", "test");
  const string orders[] = {"time_key", "time_key DESC",
                           "c3 DESC LIMIT 3"};
  for (int left = 0; left < 3; ++left) {
    for (int right = 0; right < 3; ++right) {
      EOrder expected = left == 0 || right == 0 ||
                        (left == 2 && right == 2) ? ORDER_ASC : ORDER_DESC;
      setLogicPlanCheck([expected](const SLogicNode* root, bool optimized) {
        auto* join = (const SJoinLogicNode*)findNode(
            root, QUERY_NODE_LOGIC_PLAN_JOIN);
        ASSERT_NE(join, nullptr);
        EXPECT_EQ(join->node.inputTsOrder, expected);
        EXPECT_NE(join->pPrimKeyEqCond, nullptr);
        EXPECT_EQ(join->addPrimEqCond, nullptr);
      });
      for (const string side : {"LEFT", "RIGHT"}) {
        for (bool truncate : {false, true}) {
          string lhs = "l.time_key";
          string rhs = "r.time_key";
          if (truncate) {
            lhs = "timetruncate(" + lhs + ", 1s)";
            rhs = "timetruncate(" + rhs + ", 1s)";
          }
          run("SELECT l.c1, r.c1 FROM (SELECT c3, ts time_key, c1 "
              "FROM stream_t2 ORDER BY " + orders[left] + ") l " +
              side + " ASOF JOIN (SELECT c3, ts time_key, c1 "
              "FROM stream_t2 ORDER BY " + orders[right] + ") r ON " +
              lhs + " >= " + rhs);
        }
      }
    }
  }
}

TEST_F(PlanJoinTest, temporalJoinNonprimaryAggregateOrder) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    auto* pAgg = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_AGG);
    ASSERT_NE(pJoin, nullptr);
    ASSERT_NE(pAgg, nullptr);
    ASSERT_EQ(pJoin->inputTsOrder, ORDER_ASC);
    ASSERT_EQ(pAgg->inputTsOrder, TSDB_ORDER_NONE);
  });
  run("SELECT last_row(r.c1) FROM stream_t2 l LEFT WINDOW JOIN "
      "stream_t2 r WINDOW_TIME(l.c3, r.c3) WINDOW_OFFSET(-1s, 1s)");
  run("SELECT last_row(r.c1) FROM stream_t2 l LEFT WINDOW JOIN "
      "stream_t2 r WINDOW_TIME(l.ts, r.c3) WINDOW_OFFSET(-1s, 1s)");
  run("SELECT last_row(l.c1) FROM stream_t2 l RIGHT WINDOW JOIN "
      "stream_t2 r WINDOW_TIME(l.c3, r.ts) WINDOW_OFFSET(-1s, 1s)");

  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    auto* pAgg = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_AGG);
    ASSERT_NE(pJoin, nullptr);
    ASSERT_NE(pAgg, nullptr);
    ASSERT_EQ(pJoin->inputTsOrder, ORDER_DESC);
    ASSERT_EQ(pAgg->inputTsOrder, TSDB_ORDER_NONE);
  });
  run("SELECT last_row(r.c1) FROM "
      "(SELECT ts, c3, c1 FROM stream_t2 ORDER BY c3 DESC) l "
      "LEFT WINDOW JOIN "
      "(SELECT ts, c3, c1 FROM stream_t2 ORDER BY c3 DESC) r "
      "WINDOW_TIME(l.c3, r.c3) WINDOW_OFFSET(-1s, 1s)");
}

TEST_F(PlanJoinTest, temporalJoinDerivedTimeAggregateOrder) {
  useDb("root", "test");
  for (EOrder order : {ORDER_ASC, ORDER_DESC}) {
    SCOPED_TRACE(order);
    setLogicPlanCheck([order](const SLogicNode* pRoot, bool optimized) {
      SCOPED_TRACE(optimized);
      auto* pJoin = (const SJoinLogicNode*)findNode(
          pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
      auto* pAgg = (const SAggLogicNode*)findNode(
          pRoot, QUERY_NODE_LOGIC_PLAN_AGG);
      ASSERT_NE(pJoin, nullptr);
      ASSERT_NE(pAgg, nullptr);
      EXPECT_EQ(pJoin->node.inputTsOrder, order);
      EXPECT_EQ(pJoin->node.outputTsOrder, TSDB_ORDER_NONE);
      EXPECT_EQ(pAgg->node.inputTsOrder, TSDB_ORDER_NONE);
      SNode* pNode = nullptr;
      FOREACH(pNode, pJoin->node.pChildren) {
        auto* pInput = (SLogicNode*)pNode;
        EXPECT_EQ(pInput->outputTsOrder, TSDB_ORDER_NONE);
        EXPECT_EQ(pInput->resultDataOrder, DATA_ORDER_LEVEL_NONE);
      }
      auto* pFunc = (SFunctionNode*)nodesListGetNode(pAgg->pAggFuncs, 0);
      auto* pTs = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 1);
      ASSERT_NE(pTs, nullptr);
      EXPECT_STREQ(pTs->colName, "ts");
      EXPECT_TRUE(pTs->isPrimTs);
    });
    const string direction = order == ORDER_ASC ? " ASC" : " DESC";
    const string inputs[] = {
        "SELECT ts, timetruncate(ts, 1h) event_ts, c1 FROM stream_t2",
        "SELECT ts, event_ts, c1 FROM (SELECT ts, "
        "timetruncate(ts, 1h) event_ts, c1 FROM stream_t2) s"};
    for (const auto& input : inputs) {
      const string child = "(" + input + " ORDER BY event_ts" +
                           direction + ", c1)";
      run("SELECT last_row(r.c1) FROM " + child + " l LEFT WINDOW JOIN " +
          child + " r WINDOW_TIME(l.event_ts, r.event_ts) "
          "WINDOW_OFFSET(0s, 0s)");
      if (order == ORDER_ASC) {
        const string unsorted = "(" + input + " ORDER BY c1)";
        run("SELECT last_row(r.c1) FROM " + unsorted +
            " l LEFT WINDOW JOIN " + unsorted +
            " r WINDOW_TIME(l.event_ts, r.event_ts) "
            "WINDOW_OFFSET(0s, 0s)");
      }
    }
  }
}

TEST_F(PlanJoinTest, temporalJoinPrimaryTimeAliasAggregateOrder) {
  useDb("root", "test");
  for (EOrder order : {ORDER_ASC, ORDER_DESC}) {
    setLogicPlanCheck([order](const SLogicNode* pRoot, bool optimized) {
      SCOPED_TRACE(optimized);
      auto* pAgg = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_AGG);
      ASSERT_NE(pAgg, nullptr);
      EXPECT_EQ(pAgg->inputTsOrder, order);
    });
    const string direction = order == ORDER_ASC ? " ASC" : " DESC";
    const string child =
        "(SELECT ts, event_ts, c1 FROM (SELECT ts, ts event_ts, c1 "
        "FROM stream_t2) s ORDER BY event_ts" + direction + ")";
    run("SELECT last_row(r.c1) FROM " + child + " l LEFT WINDOW JOIN " +
        child + " r WINDOW_TIME(l.event_ts, r.event_ts) "
        "WINDOW_OFFSET(0s, 0s)");
  }
}

TEST_F(PlanJoinTest, temporalJoinRedundantInputSort) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    EXPECT_EQ(countSorts(pJoin), optimized ? 1 : 2);
    auto* pSort = (const SSortLogicNode*)findNode(
        pJoin, QUERY_NODE_LOGIC_PLAN_SORT);
    ASSERT_NE(pSort, nullptr);
    EXPECT_TRUE(pSort->joinInputSort);
    EXPECT_EQ(pSort->node.outputTsOrder, ORDER_ASC);
    EXPECT_EQ(pSort->node.resultDataOrder, DATA_ORDER_LEVEL_GLOBAL);
  });
  for (const string join : {"LEFT ASOF", "RIGHT ASOF", "LEFT WINDOW",
                            "RIGHT WINDOW"}) {
    const string suffix = join.find("WINDOW") == string::npos ? "" :
                             " WINDOW_OFFSET(-1s, 1s) JLIMIT 1";
    run("SELECT l.c1, r.c1 FROM (SELECT ts, c1 FROM stream_t2 "
        "ORDER BY c1 DESC) l " + join + " JOIN stream_t2 r" + suffix);
    run("SELECT l.c1, r.c1 FROM stream_t2 l " + join +
        " JOIN (SELECT ts time_alias, c1 FROM st1s1 "
        "WHERE c1 > 0 ORDER BY c1 DESC) r" + suffix);
  }
  run("SELECT l.c1, r.c1 FROM "
      "(SELECT time_alias, c1 FROM (SELECT ts time_alias, c1 "
      "FROM stream_t2 ORDER BY c1 DESC) s) l "
      "LEFT ASOF JOIN stream_t2 r");
  run("SELECT l.c2, r.c1 FROM "
      "(SELECT ts, c2 FROM stream_t2 WHERE c1 > 0 ORDER BY c1 DESC) l "
      "LEFT ASOF JOIN stream_t2 r");
}

TEST_F(PlanJoinTest, temporalJoinNonprimaryRedundantSort) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    EXPECT_EQ(countSorts(pJoin), optimized ? 1 : 2);
    EXPECT_EQ(pJoin->inputTsOrder, ORDER_ASC);
    EXPECT_EQ(pJoin->outputTsOrder, TSDB_ORDER_NONE);
    auto* pSort = (const SSortLogicNode*)findNode(
        pJoin, QUERY_NODE_LOGIC_PLAN_SORT);
    ASSERT_NE(pSort, nullptr);
    auto* pOrder = (SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0);
    ASSERT_EQ(nodeType(pOrder->pExpr), QUERY_NODE_COLUMN);
    EXPECT_STREQ(((SColumnNode*)pOrder->pExpr)->colName, "event_ts");
    auto* pAgg = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_AGG);
    if (pAgg != nullptr) EXPECT_EQ(pAgg->inputTsOrder, TSDB_ORDER_NONE);
  });
  for (const string order : {"c1 DESC", "ts DESC, c1", "ts"}) {
    SCOPED_TRACE(order);
    run("SELECT last_row(l.c1) FROM "
        "(SELECT ts, c3 event_ts, c1 FROM stream_t2 ORDER BY " + order +
        ") l LEFT WINDOW JOIN stream_t2 r WINDOW_TIME(l.event_ts, r.ts) "
        "WINDOW_OFFSET(-1s, 1s) JLIMIT 1");
    run("SELECT l.c1, r.c1 FROM stream_t2 l RIGHT WINDOW JOIN "
        "(SELECT ts, c3 event_ts, c1 FROM stream_t2 ORDER BY " + order +
        ") r WINDOW_TIME(l.ts, r.event_ts) WINDOW_OFFSET(-1s, 1s)");
    run("SELECT l.c1, r.c1 FROM "
        "(SELECT c3 event_ts, c1 FROM stream_t2 ORDER BY " + order +
        ") l LEFT ASOF JOIN stream_t2 r");
  }
}

TEST_F(PlanJoinTest, temporalJoinKeepsObservableInputSort) {
  useDb("root", "test");
  struct SortKey {
    string expression;
    int order;
    int nullOrder;
  };
  vector<SortKey> originalKeys;
  setLogicPlanCheck([&](const SLogicNode* pRoot, bool optimized) {
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    auto* left = (const SLogicNode*)nodesListGetNode(pJoin->pChildren, 0);
    ASSERT_EQ(nodeType(left), QUERY_NODE_LOGIC_PLAN_SORT);
    ASSERT_TRUE(((const SSortLogicNode*)left)->joinInputSort);
    auto* child = (const SLogicNode*)nodesListGetNode(left->pChildren, 0);
    auto* inner = (const SSortLogicNode*)findNode(
        child, QUERY_NODE_LOGIC_PLAN_SORT);
    ASSERT_NE(inner, nullptr);
    EXPECT_FALSE(inner->joinInputSort);
    if (!optimized) originalKeys.clear();
    if (optimized) {
      ASSERT_EQ(LIST_LENGTH(inner->pSortKeys), originalKeys.size());
    }
    for (int i = 0; i < LIST_LENGTH(inner->pSortKeys); ++i) {
      auto* key = (SOrderByExprNode*)nodesListGetNode(inner->pSortKeys, i);
      char sql[512] = {};
      int32_t length = 0;
      ASSERT_EQ(nodesNodeToSQL(key->pExpr, sql, sizeof(sql), &length),
                TSDB_CODE_SUCCESS);
      if (!optimized) {
        originalKeys.push_back({sql, key->order, key->nullOrder});
      } else {
        EXPECT_EQ(string(sql), originalKeys[i].expression);
        EXPECT_EQ(key->order, originalKeys[i].order);
        EXPECT_EQ(key->nullOrder, originalKeys[i].nullOrder);
      }
    }
  });
  const string inputs[] = {
      "SELECT ts, c1 FROM stream_t2 ORDER BY c1 DESC LIMIT 3 OFFSET 1",
      "SELECT ts, c1 FROM (SELECT ts, c1 FROM stream_t2 "
      "ORDER BY c1 DESC) s LIMIT 3",
      "SELECT ts, c1 + 1 c1 FROM stream_t2 ORDER BY c1 DESC",
      "SELECT c3 time_alias, c1 FROM stream_t2 "
      "ORDER BY ts DESC, c1 LIMIT 3 OFFSET 1",
      "SELECT timetruncate(ts, 1h) time_alias, c1 "
      "FROM stream_t2 ORDER BY c1 DESC",
      "SELECT ts, c1 FROM st1 ORDER BY c1 DESC",
      "SELECT ts, c1 FROM v_stream_t2 ORDER BY c1 DESC",
      "SELECT ts, c1 FROM stream_t2 UNION ALL "
      "SELECT ts, c1 FROM stream_t2 ORDER BY c1 DESC",
      "SELECT ts, c1 FROM st1 PARTITION BY tbname "
      "ORDER BY c1 DESC SLIMIT 1",
      "SELECT ts, csum(c1) c1 FROM "
      "(SELECT ts, c1 FROM stream_t2 ORDER BY c1 DESC) s"};
  for (const auto& input : inputs) {
    SCOPED_TRACE(input);
    run("SELECT l.c1, r.c1 FROM (" + input +
        ") l LEFT ASOF JOIN stream_t2 r");
  }
  setLogicPlanCheck({});
}

TEST_F(PlanJoinTest, temporalJoinExplicitProjectionBinding) {
  useDb("root", "test");
  for (const string input : {
      "SELECT ts, timetruncate(c3, 1s) event_ts, c1 FROM stream_t2",
      "SELECT ts, c3 event_ts, c1 FROM stream_t2 UNION ALL "
      "SELECT ts, c3 event_ts, c1 FROM stream_t2"}) {
    SCOPED_TRACE(input);
    run("SELECT l.c1, r.c1 FROM (" + input + ") l "
        "LEFT WINDOW JOIN stream_t2 r WINDOW_TIME(l.event_ts, r.ts) "
        "WINDOW_OFFSET(-1s, 1s)");
    EXPECT_TRUE(planContains("\"ColName\":\"event_ts\""));
  }
}

TEST_F(PlanJoinTest, temporalJoinTimestampTag) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    SNode* pChild = nullptr;
    FOREACH(pChild, pJoin->pChildren) {
      auto* pScan = (const SScanLogicNode*)findNode(
          (const SLogicNode*)pChild, QUERY_NODE_LOGIC_PLAN_SCAN);
      ASSERT_NE(pScan, nullptr);
      int tagCount = 0;
      SNode* pPseudoCol = nullptr;
      FOREACH(pPseudoCol, pScan->pScanPseudoCols) {
        if (nodeType(pPseudoCol) == QUERY_NODE_COLUMN &&
            strcmp(((SColumnNode*)pPseudoCol)->colName, "tag3") == 0) {
          ++tagCount;
        }
      }
      ASSERT_EQ(tagCount, 1);
    }
  });
  run("SELECT l.c1, r.c1 FROM st1 l LEFT WINDOW JOIN st1 r "
      "WINDOW_TIME(l.tag3, r.tag3) WINDOW_OFFSET(-1s, 1s)");
  ASSERT_TRUE(planContains("\"TableAlias\":\"l\",\"ColName\":\"tag3\""));
  ASSERT_TRUE(planContains("\"TableAlias\":\"r\",\"ColName\":\"tag3\""));
  run("SELECT l.c1, r.c1, l.tag3, r.tag3 FROM st1 l "
      "LEFT WINDOW JOIN st1 r WINDOW_TIME(l.tag3, r.tag3) "
      "WINDOW_OFFSET(-1s, 1s)");
}

TEST_F(PlanJoinTest, virtualReferencedTagOutsideProjection) {
  useDb("root", "test");
  g_mockCatalogService->createTableBuilder(
      "test", "v_tag_join", TSDB_VIRTUAL_CHILD_TABLE, 2, 1)
      .setPrecision(TSDB_TIME_PRECISION_MILLI)
      .setVgid(1)
      .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
      .addColumn("c1", TSDB_DATA_TYPE_INT)
      .addTag("tag_ts", TSDB_DATA_TYPE_TIMESTAMP).done();
  SColRef column = {};
  column.id = 2;
  column.hasRef = true;
  strcpy(column.refDbName, "test");
  strcpy(column.refTableName, "st1s1");
  strcpy(column.refColName, "c1");
  SColRef tag = column;
  tag.id = 3;
  strcpy(tag.refColName, "tag3");
  g_mockCatalogService->setTableRefs("test", "v_tag_join", {column}, {tag});
  for (const string side : {"LEFT", "RIGHT"}) {
    run("SELECT v.c1, r.c1 FROM v_tag_join v " + side +
        " WINDOW JOIN stream_t2 r WINDOW_TIME(v.tag_ts, r.ts) "
        "WINDOW_OFFSET(-1s, 1s)");
    EXPECT_TRUE(findPlanNode("TagRefSource"));
    setLogicPlanCheck([](const SLogicNode* root, bool optimized) {
      auto* join = (const SJoinLogicNode*)findNode(
          root, QUERY_NODE_LOGIC_PLAN_JOIN);
      ASSERT_NE(join, nullptr);
      auto* condition = (SOperatorNode*)join->addPrimEqCond;
      ASSERT_NE(condition, nullptr);
      auto* time = (SColumnNode*)condition->pLeft;
      ASSERT_EQ(nodeType(time), QUERY_NODE_COLUMN);
      EXPECT_TRUE(time->hasRef);
      EXPECT_STREQ(time->refTableName, "st1s1");
      EXPECT_STREQ(time->refColName, "ts");
    });
    run("SELECT v.c1, r.c1 FROM v_tag_join v " + side +
        " ASOF JOIN st1 r ON v.tag_ts = r.tag3");
    EXPECT_TRUE(findPlanNode("TagRefSource"));
    setLogicPlanCheck({});
  }
  for (const string suffix : {"ORDER BY v.tag_ts",
                              "PARTITION BY v.tag_ts"}) {
    run("SELECT v.c1 FROM v_tag_join v " + suffix);
    EXPECT_TRUE(findPlanNode("TagRefSource"));
  }
  run("SELECT count(v.c1) FROM v_tag_join v GROUP BY v.tag_ts");
  EXPECT_TRUE(findPlanNode("TagRefSource"));
  run("SELECT count(v.c1) FROM v_tag_join v GROUP BY v.tag_ts "
      "HAVING v.tag_ts IS NOT NULL");
  EXPECT_TRUE(findPlanNode("TagRefSource"));
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
    runner->inheritedMultiGroupJoin = true;
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

  void usePartitionPlaceholder(SSubplan* reader) {
    auto* table = reinterpret_cast<STableScanPhysiNode*>(reader->pNode);
    auto* op = reinterpret_cast<SOperatorNode*>(table->scan.node.pConditions);
    nodesDestroyNode(op->pRight);
    auto* fn = make<SFunctionNode>(QUERY_NODE_FUNCTION);
    fn->funcType = FUNCTION_TYPE_PLACEHOLDER_COLUMN;
    tstrncpy(fn->functionName, "_placeholder_column", sizeof(fn->functionName));
    auto* value = make<SValueNode>(QUERY_NODE_VALUE);
    value->isNull = true;
    value->node.resType.type = TSDB_DATA_TYPE_INT;
    EXPECT_EQ(0, nodesListMakeAppend(&fn->pParameterList, reinterpret_cast<SNode*>(value)));
    auto* index = make<SValueNode>(QUERY_NODE_VALUE);
    index->translate = true;
    index->node.resType.type = TSDB_DATA_TYPE_BIGINT;
    index->datum.i = 2;
    EXPECT_EQ(0, nodesListMakeAppend(&fn->pParameterList, reinterpret_cast<SNode*>(index)));
    op->pRight = reinterpret_cast<SNode*>(fn);
  }

  void checkBatch(bool expected) {
    for (bool initial : {false, true}) {
      window->broadcastReaderBatch = initial;
      ASSERT_EQ(0, qSetStreamBroadcastReuse(runner, readers));
      EXPECT_EQ(expected, window->broadcastReaderBatch);
    }
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

TEST_F(PlanBroadcastReuseTest, DimensionBatchRequiresAllStaticReaders) {
  auto* second = addReader(15);
  usePartitionPlaceholder(reinterpret_cast<SSubplan*>(nodesListGetNode(readers, 0)));
  usePartitionPlaceholder(second);
  checkBatch(true);
  EXPECT_FALSE(window->broadcastInputReusable);

  scan()->scan.node.dynamicOp = true;
  checkBatch(false);
  scan()->scan.node.dynamicOp = false;
  reinterpret_cast<STableScanPhysiNode*>(second->pNode)->scan.node.dynamicOp = true;
  checkBatch(false);
}

TEST_F(PlanBroadcastReuseTest, DimensionBatchRequiresStaticMultiGroupQualification) {
  runner->inheritedMultiGroupJoin = false;
  check(true);
  EXPECT_FALSE(window->broadcastReaderBatch);
  usePartitionPlaceholder(reinterpret_cast<SSubplan*>(nodesListGetNode(readers, 0)));
  checkBatch(false);
  runner->inheritedMultiGroupJoin = true;
  checkBatch(true);

  runner->inheritedMultiGroupJoin = false;
  auto* join = make<SHashJoinPhysiNode>(QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN);
  join->multiGroupJoin = true;
  runner->pNode = &join->node;
  ASSERT_EQ(0, nodesListMakeAppend(&join->node.pChildren, reinterpret_cast<SNode*>(window)));
  checkBatch(true);
  join->multiGroupJoin = false;
  checkBatch(false);
}

TEST_F(PlanBroadcastReuseTest, DimensionBatchRejectsUnsupportedShapes) {
  auto* second = addReader(15);
  usePartitionPlaceholder(reinterpret_cast<SSubplan*>(nodesListGetNode(readers, 0)));
  usePartitionPlaceholder(second);
  auto* table = reinterpret_cast<STableScanPhysiNode*>(second->pNode);
  second->dynTbname = true;
  checkBatch(false);
  second->dynTbname = false;
  table->scan.virtualStableScan = true;
  checkBatch(false);
  table->scan.virtualStableScan = false;
  STimeWindow extRange = {0, 1};
  table->pExtScanRange = &extRange;
  checkBatch(false);
  table->pExtScanRange = nullptr;
  table->scan.node.pLimit = reinterpret_cast<SNode*>(make<SLimitNode>(QUERY_NODE_LIMIT));
  checkBatch(false);
  nodesDestroyNode(reinterpret_cast<SNode*>(table->scan.node.pLimit));
  table->scan.node.pLimit = nullptr;
  table->scan.node.pSlimit = reinterpret_cast<SNode*>(make<SLimitNode>(QUERY_NODE_LIMIT));
  checkBatch(false);
  nodesDestroyNode(reinterpret_cast<SNode*>(table->scan.node.pSlimit));
  table->scan.node.pSlimit = nullptr;
  window->window.indefRowsFunc = true;
  checkBatch(false);
  window->window.indefRowsFunc = false;
  window->pSubquery = reinterpret_cast<SNode*>(make<SValueNode>(QUERY_NODE_VALUE));
  checkBatch(false);
  nodesDestroyNode(window->pSubquery);
  window->pSubquery = nullptr;
  exchange->node.type = QUERY_NODE_PHYSICAL_PLAN_MERGE;
  checkBatch(false);
  exchange->node.type = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  exchange->dynTbname = true;
  checkBatch(false);
  exchange->dynTbname = false;
  second->id.subplanId = 16;
  checkBatch(false);
  second->id.subplanId = 15;
  SNode* duplicate = nullptr;
  ASSERT_EQ(0, nodesCloneNode(reinterpret_cast<SNode*>(second), &duplicate));
  ASSERT_EQ(0, nodesListMakeAppend(&readers, duplicate));
  checkBatch(false);
}

TEST_F(PlanBroadcastReuseTest, DimensionBatchAllowsPartitionPlaceholderOnly) {
  auto* reader = reinterpret_cast<SSubplan*>(nodesListGetNode(readers, 0));
  usePartitionPlaceholder(reader);
  checkBatch(true);
  EXPECT_FALSE(window->broadcastInputReusable);
  auto* op = reinterpret_cast<SOperatorNode*>(scan()->scan.node.pConditions);
  auto* fn = reinterpret_cast<SFunctionNode*>(op->pRight);
  fn->funcType = FUNCTION_TYPE_NOW;
  checkBatch(false);
  fn->funcType = FUNCTION_TYPE_PLACEHOLDER_COLUMN;
  scan()->scan.node.dynamicOp = true;
  checkBatch(false);
  scan()->scan.node.dynamicOp = false;
  ASSERT_EQ(
      0, nodesListMakeAppend(&scan()->pDynamicScanFuncs, reinterpret_cast<SNode*>(make<SValueNode>(QUERY_NODE_VALUE))));
  checkBatch(false);
  nodesDestroyList(scan()->pDynamicScanFuncs);
  scan()->pDynamicScanFuncs = nullptr;
  auto* index = reinterpret_cast<SValueNode*>(nodesListGetNode(fn->pParameterList, 1));
  index->datum.i = 0;
  checkBatch(false);
  index->datum.i = 2;
  ASSERT_EQ(0, nodesCloneNode(scan()->scan.node.pConditions, &scan()->pPrimaryCond));
  checkBatch(false);
  nodesDestroyNode(scan()->pPrimaryCond);
  scan()->pPrimaryCond = nullptr;
  window->pWindowConditions = reinterpret_cast<SNode*>(make<SFunctionNode>(QUERY_NODE_FUNCTION));
  checkBatch(false);
}

TEST_F(PlanBroadcastReuseTest, FixedReuseRemainsIndependent) {
  addReader(15);
  check(true);
  EXPECT_TRUE(window->broadcastReaderBatch);
  usePartitionPlaceholder(reinterpret_cast<SSubplan*>(nodesListGetNode(readers, 0)));
  check(false);
  EXPECT_TRUE(window->broadcastReaderBatch);
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

TEST_F(PlanJoinTest, nestedThetaInputOrder) {
  useDb("root", "test");
  const string input =
      "(SELECT /*+ join_algo(theta) */ a.ts, a.c1 FROM st1s1 a "
      "LEFT JOIN st1s2 b ON a.ts = b.ts) q";
  for (const string algorithm : {"merge", "hash"}) {
    for (bool right : {false, true}) {
      SCOPED_TRACE(algorithm + (right ? " right" : " left"));
      setLogicPlanCheck([&](const SLogicNode* root, bool optimized) {
        if (!optimized) return;
        auto* join = (const SJoinLogicNode*)findNode(
            root, QUERY_NODE_LOGIC_PLAN_JOIN);
        ASSERT_NE(join, nullptr);
        auto* child = (const SLogicNode*)nodesListGetNode(
            join->node.pChildren, right ? 1 : 0);
        auto* theta = (const SJoinLogicNode*)findNode(
            child, QUERY_NODE_LOGIC_PLAN_JOIN);
        ASSERT_NE(theta, nullptr);
        EXPECT_EQ(theta->joinAlgo, JOIN_ALGO_THETA);
        EXPECT_EQ(theta->node.outputTsOrder, TSDB_ORDER_NONE);
        if (algorithm == "merge") {
          EXPECT_EQ(join->joinAlgo, JOIN_ALGO_MERGE);
          ASSERT_EQ(nodeType(child), QUERY_NODE_LOGIC_PLAN_SORT);
          EXPECT_EQ(child->outputTsOrder, join->node.inputTsOrder);
        } else {
          EXPECT_EQ(join->joinAlgo, JOIN_ALGO_HASH);
          EXPECT_EQ(countSorts(root), 0);
        }
      });
      const string from = right ? "st1s3 c LEFT JOIN " + input :
                                  input + " LEFT JOIN st1s3 c";
      run("SELECT /*+ join_algo(" + algorithm + ") */ q.c1, c.c1 FROM " +
          from + " ON q.ts = c.ts");
    }
  }
}

TEST_F(PlanJoinTest, thetaOutputKeepsExplicitSort) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* root, bool optimized) {
    if (!optimized) return;
    EXPECT_EQ(countSorts(root), 1);
    auto* join = (const SJoinLogicNode*)findNode(
        root, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(join, nullptr);
    EXPECT_EQ(join->joinAlgo, JOIN_ALGO_THETA);
    EXPECT_EQ(join->node.outputTsOrder, TSDB_ORDER_NONE);
  });
  run("SELECT /*+ join_algo(theta) */ a.ts, a.c1 FROM st1s1 a "
      "LEFT JOIN st1s2 b ON a.ts = b.ts ORDER BY a.ts");
}

TEST_F(PlanJoinTest, nestedThetaDoesNotInheritDescendantSort) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* root, bool optimized) {
    if (!optimized) return;
    auto* join = (const SJoinLogicNode*)findNode(
        root, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(join, nullptr);
    auto* left = (const SLogicNode*)nodesListGetNode(join->node.pChildren, 0);
    ASSERT_EQ(nodeType(left), QUERY_NODE_LOGIC_PLAN_SORT);
    auto* theta = (const SJoinLogicNode*)findNode(
        left, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(theta, nullptr);
    EXPECT_EQ(theta->joinAlgo, JOIN_ALGO_THETA);
    EXPECT_EQ(theta->node.outputTsOrder, TSDB_ORDER_NONE);
  });
  run("SELECT /*+ join_algo(merge) */ q.c1, c.c1 FROM "
      "(SELECT /*+ join_algo(theta) */ a.ts, a.c1 FROM "
      "(SELECT * FROM st1s1 ORDER BY ts LIMIT 10) a "
      "LEFT JOIN st1s2 b ON a.ts = b.ts) q LEFT JOIN "
      "(SELECT * FROM st1s3 ORDER BY ts LIMIT 10) c ON q.ts = c.ts");
}

TEST_F(PlanJoinTest, nestedJoinDirectChildContract) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });

  // Each case pairs a nested-join query with its expected JOIN count.
  const vector<pair<string, int32_t>> cases = {
      {"SELECT a.c2, c.c2 FROM "
       "(st1s1 a JOIN st1s2 b ON a.ts = b.ts) "
       "JOIN st1s3 c ON b.c1 = c.c1",
       2},
      {"SELECT a.c2, d.c2 FROM "
       "((st1s1 a JOIN st1s1 b ON a.ts = b.ts) "
       "JOIN st1s1 c ON b.c1 = c.c1) "
       "JOIN st1s1 d ON c.c1 = d.c1",
       3},
      {"SELECT a.c2, d.c2 FROM st1s1 a JOIN "
       "(st1s1 b JOIN (st1s1 c JOIN st1s1 d ON c.ts = d.ts) "
       "ON b.c1 = c.c1) ON a.c1 = b.c1",
       3},
      {"SELECT a.c2, e.c2 FROM "
       "((st1s1 a JOIN st1s1 b ON a.ts = b.ts) JOIN "
       "(st1s1 c JOIN st1s1 d ON c.ts = d.ts) ON b.c1 = c.c1) "
       "JOIN st1s1 e ON d.c1 = e.c1",
       4},
      {"SELECT a.c2, h.c2 FROM "
       "((st1s1 a JOIN st1s1 b ON a.ts = b.ts) JOIN "
       "(st1s1 c JOIN st1s1 d ON c.ts = d.ts) ON b.c1 = c.c1) "
       "JOIN ((st1s1 e JOIN st1s1 f ON e.ts = f.ts) JOIN "
       "(st1s1 g JOIN st1s1 h ON g.ts = h.ts) ON f.c1 = g.c1) "
       "ON d.c1 = e.c1",
       7}};
  for (const auto& item : cases) {
    SCOPED_TRACE(item.first);
    const int32_t expectedJoins = item.second;
    setLogicPlanCheck([expectedJoins](const SLogicNode* pRoot,
                                      bool optimized) {
      if (optimized) return;
      expectJoinDirectChildContract(pRoot);
      EXPECT_EQ(countNodes(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN), expectedJoins);
    });
    run(item.first);
    // A distributed physical plan may clone one logical JOIN per subplan,
    // so only a lower bound holds here.
    EXPECT_GE(countPlanNode("MergeJoin") + countPlanNode("HashJoin") +
                  countPlanNode("ThetaJoin"),
              expectedJoins);
  }
}

TEST_F(PlanJoinTest, nestedJoinKeepsSubqueryBoundary) {
  useDb("root", "test");

  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (!optimized) {
      expectJoinDirectChildContract(pRoot);
    }
  });
  run("SELECT q.c2, d.c2 FROM "
      "((SELECT a.c1, a.c2 FROM st1s1 a JOIN st1s2 b "
      "ON a.ts = b.ts) q JOIN st1s3 c ON q.c1 = c.c1) "
      "JOIN st1s1 d ON q.c1 = d.c1");
}

TEST_F(PlanJoinTest, nestedJoinFeedsAsofTemporalJoin) {
  useDb("root", "test");

  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    expectJoinDirectChildContract(pRoot);
    expectTemporalJoinTimeKeyContract(pRoot);
  });
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });

  // The explicit ON key a.ts belongs to the nested JOIN on the left, so it
  // must be bound against that JOIN's output rather than a scan below it.
  run("SELECT a.c2, c.c2 FROM "
      "(st1s1 a JOIN st1s2 b ON a.ts = b.ts) "
      "LEFT ASOF JOIN st1s3 c ON a.ts >= c.ts");
}

// The nested input sits on the right and the key is its second relation, so
// neither "first target alias" shortcut in the physical binder identifies it.
// Both operand orders are covered because the planner may swap them.
TEST_F(PlanJoinTest, nestedJoinOnRightBindsNonFirstAliasKey) {
  useDb("root", "test");

  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    expectJoinDirectChildContract(pRoot);
    expectTemporalJoinTimeKeyContract(pRoot);
  });
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });

  for (const string& sql : {
           "SELECT c.c2, a.c2 FROM st1s3 c RIGHT ASOF JOIN "
           "(st1s1 a JOIN st1s2 b ON a.ts = b.ts) ON c.ts <= b.ts",
           "SELECT c.c2, a.c2 FROM st1s3 c RIGHT ASOF JOIN "
           "(st1s1 a JOIN st1s2 b ON a.ts = b.ts) ON b.ts >= c.ts",
           "SELECT c.c2, a.c2 FROM st1s3 c LEFT ASOF JOIN "
           "(st1s1 a JOIN st1s2 b ON a.ts = b.ts) ON c.ts <= b.ts",
       }) {
    SCOPED_TRACE(sql);
    run(sql);
  }
}

TEST_F(PlanJoinTest, nestedJoinFeedsWindowTemporalJoin) {
  useDb("root", "test");

  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (!optimized) return;
    expectJoinDirectChildContract(pRoot);
    expectTemporalJoinTimeKeyContract(pRoot);
  });
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });

  run("SELECT a.c2, c.c2 FROM st1s1 a LEFT WINDOW JOIN "
      "(st1s2 b JOIN st1s3 c ON b.ts = c.ts) "
      "WINDOW_OFFSET(-1s, 1s)");
}

TEST_F(PlanJoinTest, nestedTemporalRetainedCandidates) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  for (const string type : {"JOIN", "LEFT JOIN", "RIGHT JOIN",
                            "LEFT SEMI JOIN", "RIGHT SEMI JOIN",
                            "LEFT ANTI JOIN", "RIGHT ANTI JOIN"}) {
    SCOPED_TRACE(type);
    const string alias = type.find("RIGHT") == 0 ? "c" : "b";
    setLogicPlanCheck([alias](const SLogicNode* pRoot, bool optimized) {
      if (!optimized) return;
      expectTemporalJoinTimeKeyContract(pRoot);
      auto* pJoin = (const SJoinLogicNode*)findNode(
          pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
      ASSERT_NE(pJoin, nullptr);
      ASSERT_NE(pJoin->addPrimEqCond, nullptr);
      auto* pCond = (const SOperatorNode*)pJoin->addPrimEqCond;
      ASSERT_EQ(nodeType(pCond->pRight), QUERY_NODE_COLUMN);
      auto* pTime = (const SColumnNode*)pCond->pRight;
      EXPECT_STREQ(pTime->tableAlias, alias.c_str());
      EXPECT_STREQ(pTime->colName, "ts");
    });
    run("SELECT a.c2 FROM stream_t2 a LEFT WINDOW JOIN "
        "(stream_t2 b " + type + " stream_t2 c ON b.ts = c.ts) "
        "WINDOW_OFFSET(-1s, 1s)");
  }
}

TEST_F(PlanJoinTest, nestedTemporalUsesFirstPublicOrdinaryTime) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (!optimized) return;
    expectTemporalJoinTimeKeyContract(pRoot);
    auto* pJoin = (const SJoinLogicNode*)findNode(
        pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    auto* pCond = (const SOperatorNode*)pJoin->addPrimEqCond;
    ASSERT_NE(pCond, nullptr);
    ASSERT_EQ(nodeType(pCond->pLeft), QUERY_NODE_COLUMN);
    EXPECT_STREQ(((const SColumnNode*)pCond->pLeft)->colName, "first_time");
    auto* pInput = (const SLogicNode*)nodesListGetNode(
        pJoin->node.pChildren, 0);
    ASSERT_EQ(nodeType(pInput), QUERY_NODE_LOGIC_PLAN_SORT);
    auto* pSort = (const SSortLogicNode*)pInput;
    ASSERT_TRUE(pSort->joinInputSort);
    auto* pOrder = (const SOrderByExprNode*)nodesListGetNode(
        pSort->pSortKeys, 0);
    ASSERT_EQ(nodeType(pOrder->pExpr), QUERY_NODE_COLUMN);
    EXPECT_STREQ(((const SColumnNode*)pOrder->pExpr)->colName,
                 "first_time");
    EXPECT_EQ(pOrder->order, pJoin->node.inputTsOrder);
  });
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  run("SELECT q.k, c.c1 FROM "
      "(SELECT a.c3 first_time, b.c3 second_time, a.c1 k "
      "FROM stream_t2 a JOIN stream_t2 b ON a.ts = b.ts "
      "ORDER BY second_time DESC) q LEFT WINDOW JOIN stream_t2 c "
      "WINDOW_OFFSET(-1s, 1s)");
}

TEST_F(PlanJoinTest, nestedTemporalFullOuterCoalesce) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    expectJoinDirectChildContract(pRoot);
    if (optimized) {
      expectTemporalJoinTimeKeyContract(pRoot);
      expectTemporalInputNullFilter(pRoot, false, "q", "event_ts");
    }
  });
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  for (const string join : {"LEFT ASOF", "RIGHT ASOF", "LEFT WINDOW",
                            "RIGHT WINDOW"}) {
    SCOPED_TRACE(join);
    run("SELECT q.k, c.c2 FROM "
        "(SELECT COALESCE(a.ts, b.ts) event_ts, a.c2 k "
        "FROM st1s1 a FULL JOIN st1s2 b ON a.ts = b.ts) q " +
        join + " JOIN st1s3 c" +
        (join.find("WINDOW") == string::npos
             ? " ON q.event_ts >= c.ts" : " WINDOW_OFFSET(-1s, 1s)"));
  }
  run("SELECT q.k, c.c2 FROM "
      "(SELECT COALESCE(a.ts, b.ts) event_ts, a.c2 k "
      "FROM st1s1 a FULL JOIN st1s2 b ON a.ts = b.ts "
      "ORDER BY event_ts DESC) q LEFT WINDOW JOIN st1s3 c "
      "WINDOW_OFFSET(-1s, 1s)");
  run("SELECT q.k, c.c1 FROM "
      "(SELECT COALESCE(a.ts, b.ts) event_ts, a.c1 k FROM "
      "(SELECT c3, ts, c1 FROM stream_t2) a FULL JOIN stream_t2 b "
      "ON a.ts = b.ts) q LEFT WINDOW JOIN stream_t2 c "
      "WINDOW_OFFSET(-1s, 1s)");
  run("SELECT q.k, c.c2 FROM "
      "(SELECT CASE WHEN a.ts IS NOT NULL THEN a.ts "
      "WHEN b.ts IS NOT NULL THEN b.ts END event_ts, a.c2 k "
      "FROM st1s1 a FULL JOIN st1s2 b ON a.ts = b.ts) q "
      "LEFT ASOF JOIN st1s3 c ON q.event_ts >= c.ts");
}

TEST_F(PlanJoinTest, streamJoinPreservesOutputCompatibility) {
  useDb("root", "test");

  runStreamCalc(
      "SELECT COALESCE(a.ts, b.ts) event_ts, a.c2 "
      "FROM st1s1 a FULL JOIN st1s2 b ON a.ts = b.ts "
      "ORDER BY event_ts");
  runStreamCalc(
      "SELECT a.ts, a.c2, b.c2, c.c2 FROM "
      "(st1s1 a JOIN st1s2 b ON a.ts = b.ts) "
      "LEFT JOIN st1s3 c ON b.ts = c.ts ORDER BY a.ts");
  for (const string sql : {
           "SELECT b.ts, a.c2 FROM st1s1 a LEFT JOIN st1s2 b "
           "ON a.ts = b.ts ORDER BY b.ts",
           "SELECT a.ts, a.c2 FROM st1s1 a FULL JOIN st1s2 b "
           "ON a.ts = b.ts ORDER BY a.ts",
           "SELECT COALESCE(a.ts, a.ts) event_ts, a.c2 "
           "FROM st1s1 a FULL JOIN st1s2 b ON a.ts = b.ts "
           "ORDER BY event_ts",
           "SELECT /*+ HASH_JOIN() */ a.ts, a.c2 FROM st1s1 a "
           "LEFT JOIN st1s2 b ON a.ts = b.ts",
           "SELECT CAST(a.c2 AS TIMESTAMP) event_ts, a.c2 "
           "FROM st1s1 a JOIN st1s2 b ON a.ts = b.ts "
           "ORDER BY event_ts",
       }) {
    SCOPED_TRACE(sql);
    runStreamCalc(sql);
  }
}

TEST_F(PlanJoinTest, streamTemporalJoinKeepsInputContract) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (!optimized) return;
    expectTemporalJoinTimeKeyContract(pRoot);
    EXPECT_GT(countSorts(pRoot), 0);
  });
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  runStreamCalc(
      "SELECT q.event_ts, c.c1 FROM "
      "(SELECT a.c3 event_ts, a.c1 FROM stream_t2 a "
      "JOIN stream_t2 b ON a.c1 = b.c1) q "
      "LEFT WINDOW JOIN stream_t2 c WINDOW_OFFSET(-1s, 1s)");
}

TEST_F(PlanJoinTest, nestedTemporalPreservesSubqueryPrimaryTime) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (!optimized) return;
    expectTemporalJoinTimeKeyContract(pRoot);
    auto* pJoin = (const SJoinLogicNode*)findNode(
        pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    auto* pCond = (const SOperatorNode*)pJoin->addPrimEqCond;
    ASSERT_NE(pCond, nullptr);
    auto* pTime = (const SColumnNode*)pCond->pLeft;
    ASSERT_EQ(nodeType(pTime), QUERY_NODE_COLUMN);
    EXPECT_STREQ(pTime->colName, "ts");
    auto* pInput = (const SLogicNode*)nodesListGetNode(
        pJoin->node.pChildren, 0);
    bool filtered = false;
    while (pInput != nullptr) {
      filtered |= pInput->pConditions != nullptr;
      pInput = (const SLogicNode*)nodesListGetNode(pInput->pChildren, 0);
    }
    EXPECT_FALSE(filtered);
  });
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  run("SELECT q.c1, a.c1 FROM "
      "(SELECT c3 event_ts, ts, c1 FROM stream_t2 ORDER BY ts) q "
      "LEFT WINDOW JOIN (stream_t2 a JOIN stream_t2 b ON a.ts = b.ts) "
      "WINDOW_OFFSET(-1s, 1s)");
}

TEST_F(PlanJoinTest, nestedTemporalFiltersAggregatedOneSidedFullTime) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    expectJoinDirectChildContract(pRoot);
    if (optimized) {
      expectTemporalJoinTimeKeyContract(pRoot);
      expectTemporalInputNullFilter(pRoot, false, "q", "event_ts");
    }
  });
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  for (const string expr : {"max(a.ts)", "max(b.ts)"}) {
    for (const string join : {"LEFT ASOF", "RIGHT WINDOW"}) {
      string sql = "SELECT q.event_ts, c.c2 FROM (SELECT " + expr +
          " event_ts FROM st1s1 a FULL JOIN st1s2 b ON a.ts = b.ts) q " +
          join + " JOIN st1s3 c" +
          (join == "LEFT ASOF" ? " ON q.event_ts >= c.ts" :
                                 " WINDOW_OFFSET(-1s, 1s)");
      SCOPED_TRACE(sql);
      run(sql);
    }
  }
}

TEST_F(PlanJoinTest, nestedTemporalPrefersUnorderedPrimaryTime) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  const vector<string> inputs = {
      "((SELECT c3, ts, c1 FROM stream_t2) q "
      "JOIN stream_t2 b ON q.c1 = b.c1)",
      "(SELECT a.c3, a.ts, a.c1 FROM stream_t2 a "
      "JOIN stream_t2 b ON a.c1 = b.c1) q",
      "(SELECT now() anchor, a.ts, a.c1 FROM stream_t2 a "
      "JOIN stream_t2 b ON a.c1 = b.c1) q",
      "(SELECT a.c3, a.ts, a.c1 FROM stream_t2 a "
      "JOIN stream_t2 b ON a.c1 = b.c1 ORDER BY a.c1 DESC LIMIT 2) q",
      "(SELECT a.c3, a.ts, a.c1 FROM "
      "(stream_t2 a JOIN stream_t2 b ON a.c1 = b.c1) "
      "LEFT JOIN stream_t2 d ON a.c1 = d.c1) q",
      "(SELECT a.c3, a.ts + 1s ts, a.c1 FROM "
      "(stream_t2 a JOIN stream_t2 b ON a.c1 = b.c1) "
      "LEFT JOIN stream_t2 d ON a.c1 = d.c1) q",
      "(SELECT a.c3, timetruncate(a.ts, 1s) ts, a.c1 FROM "
      "stream_t2 d RIGHT JOIN "
      "(stream_t2 a JOIN stream_t2 b ON a.c1 = b.c1) "
      "ON a.c1 = d.c1) q",
  };
  for (bool right : {false, true}) {
    setLogicPlanCheck([right](const SLogicNode* pRoot, bool optimized) {
      if (!optimized) return;
      expectTemporalJoinTimeKeyContract(pRoot);
      auto* pJoin = (const SJoinLogicNode*)findNode(
          pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
      ASSERT_NE(pJoin, nullptr);
      auto* pCond = (const SOperatorNode*)pJoin->addPrimEqCond;
      ASSERT_NE(pCond, nullptr);
      auto* pTime = right ? pCond->pRight : pCond->pLeft;
      ASSERT_EQ(nodeType(pTime), QUERY_NODE_COLUMN);
      EXPECT_STREQ(((const SColumnNode*)pTime)->colName, "ts");
      EXPECT_STREQ(((const SColumnNode*)pTime)->tableAlias, "q");
      auto* pInput = (const SLogicNode*)nodesListGetNode(
          pJoin->node.pChildren, right ? 1 : 0);
      ASSERT_EQ(nodeType(pInput), QUERY_NODE_LOGIC_PLAN_SORT);
      auto* pSort = (const SSortLogicNode*)pInput;
      ASSERT_TRUE(pSort->joinInputSort);
      auto* pOrder = (const SOrderByExprNode*)nodesListGetNode(
          pSort->pSortKeys, 0);
      ASSERT_EQ(nodeType(pOrder->pExpr), QUERY_NODE_COLUMN);
      EXPECT_STREQ(((const SColumnNode*)pOrder->pExpr)->colName, "ts");
      EXPECT_STREQ(((const SColumnNode*)pOrder->pExpr)->tableAlias, "q");
      EXPECT_EQ(pOrder->order, pJoin->node.inputTsOrder);
      auto* pHash = (const SJoinLogicNode*)findNode(
          pInput, QUERY_NODE_LOGIC_PLAN_JOIN);
      ASSERT_NE(pHash, nullptr);
      EXPECT_EQ(pHash->joinAlgo, JOIN_ALGO_HASH);
    });
    for (const string& input : inputs) {
      for (const string join : {"LEFT ASOF", "RIGHT ASOF", "LEFT WINDOW",
                                "RIGHT WINDOW"}) {
        string sql = "SELECT q.c1 FROM " +
                     (right ? "stream_t2 c" : input) + " " + join +
                     " JOIN " + (right ? input : "stream_t2 c");
        if (join.find("WINDOW") != string::npos) {
          sql += " WINDOW_OFFSET(-1s, 1s)";
        }
        SCOPED_TRACE(sql);
        run(sql);
      }
    }
  }
}

TEST_F(PlanJoinTest, nestedTemporalPreservesSetOpPositions) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  for (const string operation : {"UNION ALL", "UNION"}) {
    run("SELECT q.c1 FROM (SELECT ts x, c1 FROM st1s1 " + operation +
        " SELECT ts y, c1 FROM st1s2) q LEFT WINDOW JOIN "
        "(st1s2 b JOIN st1s3 c ON b.ts = c.ts) WINDOW_OFFSET(-1s, 1s)");
  }
}

TEST_F(PlanJoinTest, nestedTemporalFiltersOneSidedFullTime) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  for (bool right : {false, true}) {
    setLogicPlanCheck([right](const SLogicNode* pRoot, bool optimized) {
      expectJoinDirectChildContract(pRoot);
      if (optimized) {
        expectTemporalJoinTimeKeyContract(pRoot);
        expectTemporalInputNullFilter(pRoot, right, "q", "event_ts");
      }
    });
    for (const string expr : {"a.ts", "b.ts", "COALESCE(a.ts, a.ts)"}) {
      const string input = "(SELECT " + expr +
          " event_ts, a.c2 k FROM st1s1 a FULL JOIN st1s2 b "
          "ON a.ts = b.ts) q";
      for (const string join : {"LEFT ASOF", "RIGHT ASOF", "LEFT WINDOW",
                                "RIGHT WINDOW"}) {
        const bool window = join.find("WINDOW") != string::npos;
        string sql = "SELECT q.k, c.c2 FROM " +
            (right ? "st1s3 c" : input) + " " + join + " JOIN " +
            (right ? input : "st1s3 c");
        sql += window ? (right ? " WINDOW_TIME(c.ts, q.event_ts)" :
                                 " WINDOW_TIME(q.event_ts, c.ts)") :
                        " ON q.event_ts >= c.ts";
        if (window) sql += " WINDOW_OFFSET(-1s, 1s)";
        SCOPED_TRACE(sql);
        run(sql);
      }
    }
  }
}

TEST_F(PlanJoinTest, nestedTemporalFiltersDirectFullTime) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  const string input = "(st1s1 a FULL JOIN st1s2 b ON a.ts = b.ts)";
  for (bool right : {false, true}) {
    for (const string key : {"", "b"}) {
      const string alias = key.empty() ? "a" : key;
      setLogicPlanCheck([right, alias](const SLogicNode* pRoot,
                                       bool optimized) {
        expectJoinDirectChildContract(pRoot);
        if (optimized) {
          expectTemporalJoinTimeKeyContract(pRoot);
          expectTemporalInputNullFilter(pRoot, right, alias.c_str(), "ts");
        }
      });
      for (const string join : {"LEFT ASOF", "RIGHT ASOF", "LEFT WINDOW",
                                "RIGHT WINDOW"}) {
        const bool window = join.find("WINDOW") != string::npos;
        string sql = "SELECT a.c2, b.c2, c.c2 FROM " +
            (right ? "st1s3 c" : input) + " " + join + " JOIN " +
            (right ? input : "st1s3 c");
        if (!key.empty()) {
          const string time = key + ".ts";
          sql += window ? (right ? " WINDOW_TIME(c.ts, " + time + ")" :
                                   " WINDOW_TIME(" + time + ", c.ts)") :
                          " ON " + time + " >= c.ts";
        }
        if (window) sql += " WINDOW_OFFSET(-1s, 1s)";
        SCOPED_TRACE(sql);
        run(sql);
      }
    }
  }
}

TEST_F(PlanJoinTest, nestedTemporalWithoutSortElimination) {
  Stub stub;
  stub.set(planEliminateTemporalInputSort, skipTemporalSortElimination);
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (optimized) expectTemporalJoinTimeKeyContract(pRoot);
  });
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  run("SELECT a.c2 FROM stream_t2 a LEFT WINDOW JOIN "
      "(stream_t2 b JOIN stream_t2 c ON b.ts = c.ts) "
      "WINDOW_OFFSET(-1s, 1s)");
  run("SELECT a.c2 FROM stream_t2 a LEFT WINDOW JOIN "
      "(stream_t2 b JOIN stream_t2 c ON b.ts = c.ts) "
      "WINDOW_TIME(a.c3, c.c3) WINDOW_OFFSET(-1s, 1s)");
}

TEST_F(PlanJoinTest, nestedTemporalRetainsDuplicatePublicAliases) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  run("SELECT q.x FROM (((SELECT ts x, ts y FROM st1s1) q "
      "JOIN st1s2 b ON q.x = b.ts) JOIN st1s3 c ON b.ts = c.ts) "
      "LEFT ASOF JOIN st1s3 d ON q.y >= d.ts");
}

TEST_F(PlanJoinTest, nestedTemporalFullBelowOrdinaryRoot) {
  useDb("root", "test");
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  run("SELECT a.c2 FROM ((st1s1 a FULL JOIN st1s2 b ON a.ts = b.ts) "
      "JOIN st1s3 c ON a.ts = c.ts) LEFT ASOF JOIN st1s3 d");
}

TEST_F(PlanJoinTest, nestedTemporalRequiresPublicTime) {
  useDb("root", "test");
  try {
    run("SELECT q.k FROM (SELECT a.c1 k FROM stream_t2 a "
        "JOIN stream_t2 b ON a.ts = b.ts) q "
        "LEFT WINDOW JOIN stream_t2 c WINDOW_OFFSET(-1s, 1s)");
    FAIL() << "hidden time column became a public candidate";
  } catch (const std::exception& error) {
    EXPECT_NE(string(error.what()).find(
                  to_string(TSDB_CODE_PAR_INVALID_TIMELINE_QUERY)),
              string::npos) << error.what();
  }
}

TEST_F(PlanJoinTest, nestedTemporalPrunesUnusedCandidates) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (!optimized) return;
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(pJoin, nullptr);
    vector<const SLogicNode*> pending = {pJoin};
    while (!pending.empty()) {
      auto* pNode = pending.back();
      pending.pop_back();
      SNode* pTarget = nullptr;
      FOREACH(pTarget, pNode->pTargets) {
        if (nodeType(pTarget) != QUERY_NODE_COLUMN) continue;
        EXPECT_STRNE(((const SColumnNode*)pTarget)->colName, "c3");
      }
      SNode* pChild = nullptr;
      FOREACH(pChild, pNode->pChildren) {
        pending.push_back((const SLogicNode*)pChild);
      }
    }
  });
  run("SELECT a.c1 FROM (stream_t2 a JOIN stream_t2 b ON a.ts = b.ts) "
      "LEFT WINDOW JOIN stream_t2 c WINDOW_OFFSET(-1s, 1s)");
}

TEST_F(PlanJoinTest, nestedTemporalValidatesSelectedPrecision) {
  useDb("root", "test");
  g_mockCatalogService
      ->createTableBuilder("testus", "nested_nano", TSDB_NORMAL_TABLE, 2)
      .setPrecision(TSDB_TIME_PRECISION_NANO)
      .setVgid(1)
      .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
      .addColumn("c1", TSDB_DATA_TYPE_INT)
      .done();
  const vector<pair<string, int32_t>> invalid = {
      {"SELECT a.c1 FROM st1s1 a LEFT WINDOW JOIN "
       "(testus.nested_nano b JOIN testus.nested_nano c ON b.ts = c.ts) "
       "WINDOW_OFFSET(-1s, 1s)", TSDB_CODE_PAR_NOT_SUPPORT_JOIN},
      {"SELECT a.c1 FROM st1s1 a LEFT WINDOW JOIN "
       "(st1s2 b JOIN st1s3 c ON b.ts = c.ts) WINDOW_OFFSET(-1u, 1u)",
       TSDB_CODE_PAR_INVALID_WIN_OFFSET_UNIT},
      {"SELECT a.c1 FROM (st1s1 a JOIN st1s2 b ON a.ts = b.ts) "
       "RIGHT WINDOW JOIN st1s3 c WINDOW_OFFSET(-1b, 1b)",
       TSDB_CODE_PAR_INVALID_WIN_OFFSET_UNIT}};
  for (const auto& item : invalid) {
    SCOPED_TRACE(item.first);
    try {
      run(item.first);
      FAIL() << "invalid selected precision was accepted";
    } catch (const std::exception& error) {
      EXPECT_NE(string(error.what()).find(to_string(item.second)),
                string::npos) << error.what();
    }
  }
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  run("SELECT a.c1 FROM st1s1 a LEFT ASOF JOIN "
      "(testus.nested_nano b JOIN testus.nested_nano c ON b.ts = c.ts)");
}

TEST_F(PlanJoinTest, joinInputPropertyMatrix) {
  useDb("root", "test");

  struct JoinPropertyCase {
    string sql;
    bool publishLeft;
    bool publishRight;
    bool nullableLeft;
    bool nullableRight;
  };
  const vector<JoinPropertyCase> cases = {
      {"SELECT l.c1, r.c1 FROM st1s1 l JOIN st1s1 r "
       "ON l.ts = r.ts",
       true, true, false, false},
      {"SELECT l.c1, r.c1 FROM st1s1 l LEFT JOIN st1s1 r "
       "ON l.ts = r.ts",
       true, true, false, true},
      {"SELECT l.c1, r.c1 FROM st1s1 l RIGHT JOIN st1s1 r "
       "ON l.ts = r.ts",
       true, true, true, false},
      {"SELECT l.c1, r.c1 FROM st1s1 l FULL JOIN st1s1 r "
       "ON l.ts = r.ts",
       true, true, true, true},
      {"SELECT l.c1, r.c1 FROM st1s1 l LEFT SEMI JOIN st1s1 r "
       "ON l.ts = r.ts",
       true, true, false, false},
      {"SELECT l.c1, r.c1 FROM st1s1 l RIGHT SEMI JOIN st1s1 r "
       "ON l.ts = r.ts",
       true, true, false, false},
      {"SELECT l.c1, r.c1 FROM st1s1 l LEFT ANTI JOIN st1s1 r "
       "ON l.ts = r.ts",
       true, true, false, true},
      {"SELECT l.c1, r.c1 FROM st1s1 l RIGHT ANTI JOIN st1s1 r "
       "ON l.ts = r.ts",
       true, true, true, false},
  };
  for (const auto& item : cases) {
    SCOPED_TRACE(item.sql);
    setJoinPropsCheck([item](const SPlanContext* pCxt,
                             const SLogicNode* pRoot, bool optimized) {
      if (optimized) {
        return;
      }
      auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
      ASSERT_NE(pJoin, nullptr);
      auto* pProps = planGetJoinInputProps(pCxt, pJoin);
      ASSERT_NE(pProps, nullptr);
      ASSERT_NE(pProps->pOrderProps, nullptr);

      bool seenRight = false;
      size_t numCols = taosArrayGetSize(pProps->pOutputCols);
      for (size_t i = 0; i < numCols; ++i) {
        auto* pOutput = (const SJoinOutputCol*)taosArrayGet(
            pProps->pOutputCols, i);
        EXPECT_EQ(pOutput->outputIndex, i);
        if (JOIN_INPUT_SIDE_RIGHT == pOutput->sourceSide) {
          seenRight = true;
          EXPECT_TRUE(item.publishRight);
          EXPECT_EQ(pOutput->nullable,
                    !pOutput->primaryTimestamp || item.nullableRight);
        } else {
          EXPECT_EQ(pOutput->sourceSide, JOIN_INPUT_SIDE_LEFT);
          EXPECT_FALSE(seenRight);
          EXPECT_TRUE(item.publishLeft);
          EXPECT_EQ(pOutput->nullable,
                    !pOutput->primaryTimestamp || item.nullableLeft);
        }
      }

      auto* pLeft = findJoinOutput(pProps, "l", "c1");
      auto* pRight = findJoinOutput(pProps, "r", "c1");
      EXPECT_EQ(nullptr != pLeft, item.publishLeft);
      EXPECT_EQ(nullptr != pRight, item.publishRight);
      if (nullptr != pLeft && nullptr != pRight) {
        EXPECT_NE(pLeft->id.relationId, pRight->id.relationId);
      }
    });
    run(item.sql);
  }
}

TEST_F(PlanJoinTest, semiMatchingTimeNeedsOwnSort) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* root, bool optimized) {
    if (!optimized) return;
    auto* join = (const SJoinLogicNode*)findNode(
        root, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(join, nullptr);
    EXPECT_EQ(join->joinAlgo, JOIN_ALGO_MERGE);
    auto* input = (const SLogicNode*)nodesListGetNode(
        join->node.pChildren, 0);
    ASSERT_EQ(nodeType(input), QUERY_NODE_LOGIC_PLAN_SORT);
    auto* sort = (const SSortLogicNode*)input;
    auto* key = (const SOrderByExprNode*)nodesListGetNode(sort->pSortKeys, 0);
    ASSERT_EQ(nodeType(key->pExpr), QUERY_NODE_COLUMN);
    EXPECT_STREQ(((const SColumnNode*)key->pExpr)->colName, "match_time");
    EXPECT_EQ(countSorts(input), 2);
  });
  for (const string side : {"LEFT", "RIGHT"}) {
    const string source = side == "LEFT" ?
        "st1s1 a LEFT SEMI JOIN st1s1 b" :
        "st1s1 b RIGHT SEMI JOIN st1s1 a";
    run("SELECT /*+ MERGE_JOIN() */ q.c1, c.c1 FROM "
        "(SELECT /*+ HASH_JOIN() */ a.ts driver_time, b.ts match_time, "
        "a.c1 FROM " + source + " ON a.c1 = b.c1 "
        "ORDER BY a.ts LIMIT 3) q JOIN st1s1 c ON q.match_time = c.ts");
  }
}

TEST_F(PlanJoinTest, semiBucketTimeNeedsRawSort) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* root, bool optimized) {
    if (!optimized) return;
    auto* join = (const SJoinLogicNode*)findNode(
        root, QUERY_NODE_LOGIC_PLAN_JOIN);
    ASSERT_NE(join, nullptr);
    auto* input = (const SLogicNode*)nodesListGetNode(
        join->node.pChildren, 0);
    ASSERT_EQ(nodeType(input), QUERY_NODE_LOGIC_PLAN_SORT);
    auto* sort = (const SSortLogicNode*)input;
    auto* key = (const SOrderByExprNode*)nodesListGetNode(sort->pSortKeys, 0);
    ASSERT_EQ(nodeType(key->pExpr), QUERY_NODE_COLUMN);
    EXPECT_STREQ(((const SColumnNode*)key->pExpr)->tableAlias, "b");
    EXPECT_STREQ(((const SColumnNode*)key->pExpr)->colName, "ts");
  });
  for (const string side : {"LEFT", "RIGHT"}) {
    const string source = side == "LEFT" ?
        "st1s1 a LEFT SEMI JOIN st1s1 b" :
        "st1s1 b RIGHT SEMI JOIN st1s1 a";
    run("SELECT /*+ MERGE_JOIN() */ a.c1, c.c1 FROM (" + source +
        " ON timetruncate(a.ts,1m) = timetruncate(b.ts,1m) "
        "AND a.c1 = b.c1) JOIN st1s1 c ON b.ts = c.ts");
  }
}

TEST_F(PlanJoinTest, semiAntiProjectionChoosesTimeCandidate) {
  useDb("root", "test");
  for (const string kind : {"SEMI", "ANTI"}) {
    setLogicPlanCheck([&](const SLogicNode* root, bool optimized) {
      if (!optimized) return;
      auto* join = (const SJoinLogicNode*)findNode(
          root, QUERY_NODE_LOGIC_PLAN_JOIN);
      ASSERT_NE(join, nullptr);
      auto* condition = (SOperatorNode*)(join->pPrimKeyEqCond != nullptr ?
          join->pPrimKeyEqCond : join->addPrimEqCond);
      ASSERT_NE(condition, nullptr);
      ASSERT_EQ(nodeType(condition->pLeft), QUERY_NODE_COLUMN);
      EXPECT_STREQ(((SColumnNode*)condition->pLeft)->colName,
                   kind == "SEMI" ? "match_time" : "driver_time");
    });
    for (bool right : {false, true}) {
      const string source = right ?
          "st1s1 b RIGHT " + kind + " JOIN st1s1 a" :
          "st1s1 a LEFT " + kind + " JOIN st1s1 b";
      for (const string temporal : {"ASOF", "WINDOW"}) {
        run("SELECT q.c1, c.c1 FROM (SELECT b.ts match_time, "
            "a.ts driver_time, a.c1 FROM " + source +
            " ON a.c1 = b.c1) q LEFT " + temporal + " JOIN st1s1 c" +
            (temporal == "ASOF" ? "" : " WINDOW_OFFSET(0s, 0s)"));
      }
    }
  }
}

TEST_F(PlanJoinTest, semiAntiTimeIdentity) {
  useDb("root", "test");
  for (const string kind : {"SEMI", "ANTI"}) {
    for (bool right : {false, true}) {
      setJoinPropsCheck([&](const SPlanContext* pCxt,
                           const SLogicNode* pRoot, bool optimized) {
        auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
        ASSERT_NE(pJoin, nullptr);
        auto* pProps = planGetJoinInputProps(pCxt, pJoin);
        ASSERT_NE(pProps, nullptr);
        auto* pLeft = findJoinOutput(pProps, "a", "ts");
        auto* pRight = findJoinOutput(pProps, "b", "ts");
        ASSERT_NE(pLeft, nullptr);
        ASSERT_NE(pRight, nullptr);
        EXPECT_EQ(pLeft->primaryTimestamp, kind == "SEMI" || !right);
        EXPECT_EQ(pRight->primaryTimestamp, kind == "SEMI" || right);
        EXPECT_EQ(pLeft->nullable, kind == "ANTI" && right);
        EXPECT_EQ(pRight->nullable, kind == "ANTI" && !right);
      });
      run("SELECT a.ts, b.ts FROM st1s1 a " +
          string(right ? "RIGHT " : "LEFT ") + kind +
          " JOIN st1s1 b ON a.c1 = b.c1");
    }
  }
}

TEST_F(PlanJoinTest, semiAntiNestedPeerTimeIdentity) {
  useDb("root", "test");
  for (const string kind : {"SEMI", "ANTI"}) {
    setJoinPropsCheck([&](const SPlanContext* pCxt,
                         const SLogicNode* pRoot, bool optimized) {
      auto* props = planGetJoinInputProps(pCxt, pRoot);
      ASSERT_NE(props, nullptr);
      const SJoinOutputCol* nullable = nullptr;
      const SJoinOutputCol* valid = nullptr;
      for (size_t i = 0; i < taosArrayGetSize(props->pOutputCols); ++i) {
        auto* col = (const SJoinOutputCol*)taosArrayGet(props->pOutputCols, i);
        auto* expr = (const SColumnNode*)col->pExpr;
        if (string(expr->node.userAlias) == "null_time") nullable = col;
        if (string(expr->node.userAlias) == "valid_time") valid = col;
      }
      ASSERT_NE(nullable, nullptr);
      ASSERT_NE(valid, nullptr);
      EXPECT_FALSE(nullable->primaryTimestamp);
      EXPECT_TRUE(nullable->nullable);
      EXPECT_EQ(valid->primaryTimestamp, kind == "SEMI");
      EXPECT_EQ(valid->nullable, kind == "ANTI");
    });
    const string input = "(st1s1 a RIGHT JOIN st1s1 b ON a.ts = b.ts)";
    for (bool right : {false, true}) {
      const string source = right ?
          input + " RIGHT " + kind + " JOIN st1s1 c" :
          "st1s1 c LEFT " + kind + " JOIN " + input;
      run("SELECT q.null_time, q.valid_time FROM "
          "(SELECT a.ts null_time, b.ts valid_time FROM " + source +
          " ON b.ts = c.ts) q");
    }
  }
}

TEST_F(PlanJoinTest, semiAntiKeepsMatchingSidePostFilter) {
  useDb("root", "test");
  for (const string kind : {"SEMI", "ANTI"}) {
    for (bool right : {false, true}) {
      setLogicPlanCheck([&](const SLogicNode* root, bool optimized) {
        if (!optimized) return;
        auto* join = findNode(root, QUERY_NODE_LOGIC_PLAN_JOIN);
        ASSERT_NE(join, nullptr);
        EXPECT_NE(join->pConditions, nullptr);
        auto* peer = (const SLogicNode*)nodesListGetNode(
            join->pChildren, right ? 0 : 1);
        ASSERT_EQ(nodeType(peer), QUERY_NODE_LOGIC_PLAN_SCAN);
        EXPECT_EQ(peer->pConditions, nullptr);
      });
      const string source = right ?
          "st1s1 b RIGHT " + kind + " JOIN st1s1 a" :
          "st1s1 a LEFT " + kind + " JOIN st1s1 b";
      for (const string algo : {"HASH", "MERGE"}) {
        run("SELECT /*+ " + algo + "_JOIN() */ a.c1, b.c2 FROM " +
            source + " ON a.ts = b.ts WHERE b.c2 " +
            (kind == "SEMI" ? "> 5" : "IS NULL"));
      }
    }
  }
}

TEST_F(PlanJoinTest, joinInputPropsRebuiltAfterOptimization) {
  useDb("root", "test");

  setJoinPropsCheck([](const SPlanContext* pCxt,
                       const SLogicNode* pRoot, bool optimized) {
    ASSERT_NE(findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN), nullptr);
    EXPECT_NE(pCxt->pJoinPropsStore, nullptr);
    auto* pJoin = findNode(pRoot, QUERY_NODE_LOGIC_PLAN_JOIN);
    auto* pProps = planGetJoinInputProps(pCxt, pJoin);
    ASSERT_NE(pProps, nullptr);
    EXPECT_EQ(pProps->pNode, pJoin);
  });
  run("SELECT l.c1, r.c1 FROM st1 l JOIN st1 r "
      "ON l.tag1 = r.tag1");
}

TEST_F(PlanJoinTest, nestedSemiAntiConsumesHiddenSide) {
  useDb("root", "test");

  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (!optimized) {
      expectJoinDirectChildContract(pRoot);
    }
  });
  for (const string& sql : {
           "SELECT a.c2 FROM st1s1 a LEFT SEMI JOIN "
           "(st1s2 b JOIN st1s3 c ON b.ts = c.ts) "
           "ON a.c1 = c.c1",
           "SELECT a.c2 FROM st1s1 a LEFT ANTI JOIN "
           "(st1s2 b JOIN st1s3 c ON b.ts = c.ts) "
           "ON a.c1 = c.c1",
           "SELECT c.c2 FROM "
           "(st1s1 a JOIN st1s2 b ON a.ts = b.ts) "
           "RIGHT SEMI JOIN st1s3 c ON a.c1 = c.c1",
           "SELECT c.c2 FROM "
           "(st1s1 a JOIN st1s2 b ON a.ts = b.ts) "
           "RIGHT ANTI JOIN st1s3 c ON a.c1 = c.c1",
       }) {
    SCOPED_TRACE(sql);
    run(sql);
  }
}

// A temporal join whose input hides its JOIN below a FROM subquery or a
// UNION branch, with no outer reference to the inner time column.
TEST_F(PlanJoinTest, nestedTemporalOverSubqueryJoin) {
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (!optimized) expectJoinDirectChildContract(pRoot);
  });
  setPhysiPlanCheck([](const SQueryPlan* pPlan) {
    expectPhysiJoinDirectChildContract(pPlan);
  });
  run("SELECT a.c1 FROM stream_t2 a LEFT ASOF JOIN "
      "(SELECT b.ts, b.c1 FROM stream_t2 b JOIN stream_t2 c "
      "ON b.ts = c.ts) q");
  run("SELECT a.c1 FROM stream_t2 a LEFT ASOF JOIN "
      "(SELECT b.ts, b.c1 FROM stream_t2 b JOIN stream_t2 c "
      "ON b.ts = c.ts UNION ALL SELECT ts, c1 FROM stream_t2) q");
  run("SELECT a.c1 FROM (SELECT b.ts, b.c1 FROM stream_t2 b "
      "JOIN stream_t2 c ON b.ts = c.ts) q LEFT ASOF JOIN stream_t2 a");
  run("SELECT a.c1 FROM stream_t2 a LEFT WINDOW JOIN "
      "(SELECT b.ts, b.c1 FROM stream_t2 b JOIN stream_t2 c "
      "ON b.ts = c.ts) q WINDOW_TIME(a.c3, q.ts) WINDOW_OFFSET(-1s, 1s)");
}
