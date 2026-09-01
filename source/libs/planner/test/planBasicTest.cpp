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

#include "planInt.h"
#include "planner.h"

using namespace std;

class PlanBasicTest : public PlannerTestBase {};

namespace {

template <typename T>
T* makePlanNode(ENodeType type) {
  T* pNode = nullptr;
  if (TSDB_CODE_SUCCESS != nodesMakeNode(type, reinterpret_cast<SNode**>(&pNode))) {
    return nullptr;
  }
  return pNode;
}

SColumnNode* makePlanColumn(const char* name) {
  SColumnNode* pColumn = makePlanNode<SColumnNode>(QUERY_NODE_COLUMN);
  if (pColumn == nullptr) {
    return nullptr;
  }
  pColumn->node.resType.type = TSDB_DATA_TYPE_INT;
  pColumn->node.resType.bytes = sizeof(int32_t);
  pColumn->colType = COLUMN_TYPE_COLUMN;
  tstrncpy(pColumn->colName, name, sizeof(pColumn->colName));
  tstrncpy(pColumn->node.aliasName, name, sizeof(pColumn->node.aliasName));
  tstrncpy(pColumn->node.userAlias, name, sizeof(pColumn->node.userAlias));
  return pColumn;
}

SExchangeLogicNode* makePlanExchange(int32_t groupId, const char* columnName) {
  SExchangeLogicNode* pExchange = makePlanNode<SExchangeLogicNode>(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (pExchange == nullptr) {
    return nullptr;
  }
  pExchange->srcStartGroupId = groupId;
  pExchange->srcEndGroupId = groupId;
  SColumnNode* pColumn = makePlanColumn(columnName);
  if (pColumn == nullptr ||
      TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pExchange->node.pTargets, reinterpret_cast<SNode*>(pColumn))) {
    nodesDestroyNode(reinterpret_cast<SNode*>(pColumn));
    nodesDestroyNode(reinterpret_cast<SNode*>(pExchange));
    return nullptr;
  }
  return pExchange;
}

SMergeLogicNode* makePlanMerge(int32_t groupId, const char* columnName) {
  SMergeLogicNode* pMerge = makePlanNode<SMergeLogicNode>(QUERY_NODE_LOGIC_PLAN_MERGE);
  if (pMerge == nullptr) {
    return nullptr;
  }
  pMerge->srcGroupId = groupId;
  pMerge->srcEndGroupId = groupId;
  pMerge->numOfChannels = 1;
  pMerge->numOfSubplans = 1;
  SColumnNode* pColumn = makePlanColumn(columnName);
  if (pColumn == nullptr ||
      TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pMerge->node.pTargets, reinterpret_cast<SNode*>(pColumn))) {
    nodesDestroyNode(reinterpret_cast<SNode*>(pColumn));
    nodesDestroyNode(reinterpret_cast<SNode*>(pMerge));
    return nullptr;
  }
  return pMerge;
}

SLogicSubplan* makeLogicSubplan(int32_t groupId, int32_t level, SLogicNode* pRoot) {
  SLogicSubplan* pSubplan = makePlanNode<SLogicSubplan>(QUERY_NODE_LOGIC_SUBPLAN);
  if (pSubplan == nullptr) {
    return nullptr;
  }
  pSubplan->id.queryId = 1;
  pSubplan->id.groupId = groupId;
  pSubplan->id.subplanId = groupId;
  pSubplan->level = level;
  pSubplan->subplanType = SUBPLAN_TYPE_MERGE;
  pSubplan->pNode = pRoot;
  return pSubplan;
}

const SSubplan* findPhysicalSubplan(const SQueryPlan* pPlan, int32_t groupId) {
  SNode* pLevelNode = nullptr;
  FOREACH(pLevelNode, pPlan->pSubplans) {
    SNode* pSubplanNode = nullptr;
    FOREACH(pSubplanNode, reinterpret_cast<SNodeListNode*>(pLevelNode)->pNodeList) {
      const SSubplan* pSubplan = reinterpret_cast<const SSubplan*>(pSubplanNode);
      if (pSubplan->id.groupId == groupId) {
        return pSubplan;
      }
    }
  }
  return nullptr;
}

const SSubplan* getPhysicalChild(const SSubplan* pParent, int32_t index, int32_t expectedGroupId) {
  if (pParent == nullptr) {
    return nullptr;
  }
  const SSubplan* pChild = reinterpret_cast<const SSubplan*>(nodesListGetNode(pParent->pChildren, index));
  if (pChild == nullptr) {
    return nullptr;
  }
  EXPECT_EQ(expectedGroupId, pChild->id.groupId);
  EXPECT_EQ(expectedGroupId, pChild->id.subplanId);
  EXPECT_EQ(1, LIST_LENGTH(pChild->pParents));
  if (LIST_LENGTH(pChild->pParents) != 1) {
    return nullptr;
  }
  EXPECT_EQ(pParent, reinterpret_cast<const SSubplan*>(nodesListGetNode(pChild->pParents, 0)));
  return pChild;
}

class MultiParentLogicPlanOwner {
 public:
  MultiParentLogicPlanOwner(SQueryLogicPlan* pPlan, SLogicSubplan* pSecondaryParent)
      : pPlan_(pPlan), pSecondaryParent_(pSecondaryParent) {}

  ~MultiParentLogicPlanOwner() {
    if (pSecondaryParent_ != nullptr) {
      nodesClearList(pSecondaryParent_->pChildren);
      pSecondaryParent_->pChildren = nullptr;
    }
    nodesDestroyNode(reinterpret_cast<SNode*>(pPlan_));
  }

  MultiParentLogicPlanOwner(const MultiParentLogicPlanOwner&) = delete;
  MultiParentLogicPlanOwner& operator=(const MultiParentLogicPlanOwner&) = delete;

 private:
  SQueryLogicPlan* pPlan_;
  SLogicSubplan*   pSecondaryParent_;
};

}  // namespace

TEST(PlanPhysicalCreationTest, MultiGroupJoinQualificationFollowsExactEdgesAcrossCuts) {
  constexpr int32_t kRootGroup = 1;
  constexpr int32_t kExchangeChildGroup = 2;
  constexpr int32_t kMergeChildGroup = 3;
  constexpr int32_t kGrandchildGroup = 4;
  constexpr int32_t kUnrelatedChildGroup = 6;
  constexpr int32_t kUnqualifiedRootGroup = 7;

  SProjectLogicNode* pProject = makePlanNode<SProjectLogicNode>(QUERY_NODE_LOGIC_PLAN_PROJECT);
  SJoinLogicNode*    pJoin = makePlanNode<SJoinLogicNode>(QUERY_NODE_LOGIC_PLAN_JOIN);
  ASSERT_NE(nullptr, pProject);
  ASSERT_NE(nullptr, pJoin);
  pJoin->joinAlgo = JOIN_ALGO_HASH;
  pJoin->joinType = JOIN_TYPE_INNER;
  pJoin->subType = JOIN_STYPE_NONE;
  pJoin->multiGroupJoin = true;

  SExchangeLogicNode* pJoinExchange = makePlanExchange(kExchangeChildGroup, "left_key");
  SMergeLogicNode*    pJoinMerge = makePlanMerge(kMergeChildGroup, "right_key");
  SExchangeLogicNode* pUnrelatedExchange = makePlanExchange(kUnrelatedChildGroup, "unrelated_key");
  ASSERT_NE(nullptr, pJoinExchange);
  ASSERT_NE(nullptr, pJoinMerge);
  ASSERT_NE(nullptr, pUnrelatedExchange);
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pJoin->node.pChildren, reinterpret_cast<SNode*>(pJoinExchange)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pJoin->node.pChildren, reinterpret_cast<SNode*>(pJoinMerge)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pProject->node.pChildren, reinterpret_cast<SNode*>(pJoin)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pProject->node.pChildren, reinterpret_cast<SNode*>(pUnrelatedExchange)));

  SOperatorNode* pEquality = makePlanNode<SOperatorNode>(QUERY_NODE_OPERATOR);
  ASSERT_NE(nullptr, pEquality);
  pEquality->opType = OP_TYPE_EQUAL;
  pEquality->pLeft = reinterpret_cast<SNode*>(makePlanColumn("left_key"));
  pEquality->pRight = reinterpret_cast<SNode*>(makePlanColumn("right_key"));
  ASSERT_NE(nullptr, pEquality->pLeft);
  ASSERT_NE(nullptr, pEquality->pRight);
  pJoin->pColEqCond = reinterpret_cast<SNode*>(pEquality);
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pJoin->node.pTargets, reinterpret_cast<SNode*>(makePlanColumn("left_key"))));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pJoin->node.pTargets, reinterpret_cast<SNode*>(makePlanColumn("right_key"))));

  SLogicSubplan* pRoot = makeLogicSubplan(kRootGroup, 0, reinterpret_cast<SLogicNode*>(pProject));
  SLogicSubplan* pExchangeChild = makeLogicSubplan(
      kExchangeChildGroup, 1, reinterpret_cast<SLogicNode*>(makePlanExchange(kGrandchildGroup, "grandchild_key")));
  SLogicSubplan* pMergeChild =
      makeLogicSubplan(kMergeChildGroup, 1, reinterpret_cast<SLogicNode*>(makePlanExchange(30, "merge_leaf_key")));
  SLogicSubplan* pGrandchild =
      makeLogicSubplan(kGrandchildGroup, 2, reinterpret_cast<SLogicNode*>(makePlanExchange(40, "leaf_key")));
  SLogicSubplan* pUnrelatedChild = makeLogicSubplan(
      kUnrelatedChildGroup, 1, reinterpret_cast<SLogicNode*>(makePlanExchange(60, "unrelated_leaf_key")));
  SProjectLogicNode*  pUnqualifiedProject = makePlanNode<SProjectLogicNode>(QUERY_NODE_LOGIC_PLAN_PROJECT);
  SExchangeLogicNode* pUnqualifiedRoute = makePlanExchange(kExchangeChildGroup, "unqualified_route_key");
  ASSERT_NE(nullptr, pUnqualifiedProject);
  ASSERT_NE(nullptr, pUnqualifiedRoute);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pUnqualifiedProject->node.pChildren,
                                                         reinterpret_cast<SNode*>(pUnqualifiedRoute)));
  SLogicSubplan* pUnqualifiedRoot =
      makeLogicSubplan(kUnqualifiedRootGroup, 0, reinterpret_cast<SLogicNode*>(pUnqualifiedProject));
  ASSERT_NE(nullptr, pRoot);
  ASSERT_NE(nullptr, pExchangeChild);
  ASSERT_NE(nullptr, pMergeChild);
  ASSERT_NE(nullptr, pGrandchild);
  ASSERT_NE(nullptr, pUnrelatedChild);
  ASSERT_NE(nullptr, pUnqualifiedRoot);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pRoot->pChildren, reinterpret_cast<SNode*>(pExchangeChild)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pRoot->pChildren, reinterpret_cast<SNode*>(pMergeChild)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pRoot->pChildren, reinterpret_cast<SNode*>(pUnrelatedChild)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pExchangeChild->pChildren, reinterpret_cast<SNode*>(pGrandchild)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&pExchangeChild->pParents, reinterpret_cast<SNode*>(pRoot)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeAppend(&pExchangeChild->pParents, reinterpret_cast<SNode*>(pUnqualifiedRoot)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeAppend(&pUnqualifiedRoot->pChildren, reinterpret_cast<SNode*>(pExchangeChild)));

  SQueryLogicPlan* pLogicPlan = makePlanNode<SQueryLogicPlan>(QUERY_NODE_LOGIC_PLAN);
  ASSERT_NE(nullptr, pLogicPlan);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pLogicPlan->pTopSubplans, reinterpret_cast<SNode*>(pRoot)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pLogicPlan->pTopSubplans, reinterpret_cast<SNode*>(pUnqualifiedRoot)));
  MultiParentLogicPlanOwner logicPlanOwner(pLogicPlan, pUnqualifiedRoot);

  SNode ast = {};
  ast.type = QUERY_NODE_SELECT_STMT;
  SPlanContext planContext = {};
  planContext.queryId = 1;
  planContext.pAstRoot = &ast;
  SQueryPlan* pPhysicalPlan = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createPhysiPlan(&planContext, pLogicPlan, &pPhysicalPlan, nullptr));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> physicalPlanGuard(reinterpret_cast<SNode*>(pPhysicalPlan),
                                                                   nodesDestroyNode);

  const SSubplan* pPhysicalRoot = findPhysicalSubplan(pPhysicalPlan, kRootGroup);
  const SSubplan* pPhysicalUnqualifiedRoot = findPhysicalSubplan(pPhysicalPlan, kUnqualifiedRootGroup);
  ASSERT_NE(nullptr, pPhysicalRoot);
  ASSERT_NE(nullptr, pPhysicalUnqualifiedRoot);
  EXPECT_EQ(8, pPhysicalPlan->numOfSubplans);
  EXPECT_EQ(kRootGroup, pPhysicalRoot->id.subplanId);
  EXPECT_EQ(kUnqualifiedRootGroup, pPhysicalUnqualifiedRoot->id.subplanId);
  EXPECT_EQ(0, LIST_LENGTH(pPhysicalRoot->pParents));
  EXPECT_EQ(0, LIST_LENGTH(pPhysicalUnqualifiedRoot->pParents));
  EXPECT_FALSE(pPhysicalRoot->inheritedMultiGroupJoin);
  EXPECT_FALSE(pPhysicalUnqualifiedRoot->inheritedMultiGroupJoin);

  const SSubplan* pQualifiedExchangeChild = getPhysicalChild(pPhysicalRoot, 0, kExchangeChildGroup);
  const SSubplan* pQualifiedMergeChild = getPhysicalChild(pPhysicalRoot, 1, kMergeChildGroup);
  const SSubplan* pUnrelatedChildBranch = getPhysicalChild(pPhysicalRoot, 2, kUnrelatedChildGroup);
  const SSubplan* pUnqualifiedExchangeChild = getPhysicalChild(pPhysicalUnqualifiedRoot, 0, kExchangeChildGroup);
  ASSERT_NE(nullptr, pQualifiedExchangeChild);
  ASSERT_NE(nullptr, pQualifiedMergeChild);
  ASSERT_NE(nullptr, pUnrelatedChildBranch);
  ASSERT_NE(nullptr, pUnqualifiedExchangeChild);
  EXPECT_TRUE(pQualifiedExchangeChild->inheritedMultiGroupJoin);
  EXPECT_TRUE(pQualifiedMergeChild->inheritedMultiGroupJoin);
  EXPECT_FALSE(pUnrelatedChildBranch->inheritedMultiGroupJoin);
  EXPECT_FALSE(pUnqualifiedExchangeChild->inheritedMultiGroupJoin);
  EXPECT_NE(pQualifiedExchangeChild, pUnqualifiedExchangeChild);
  EXPECT_EQ(pQualifiedExchangeChild->id.groupId, pUnqualifiedExchangeChild->id.groupId);
  EXPECT_EQ(pQualifiedExchangeChild->id.subplanId, pUnqualifiedExchangeChild->id.subplanId);

  const SSubplan* pQualifiedGrandchild = getPhysicalChild(pQualifiedExchangeChild, 0, kGrandchildGroup);
  const SSubplan* pUnqualifiedGrandchild = getPhysicalChild(pUnqualifiedExchangeChild, 0, kGrandchildGroup);
  ASSERT_NE(nullptr, pQualifiedGrandchild);
  ASSERT_NE(nullptr, pUnqualifiedGrandchild);
  EXPECT_TRUE(pQualifiedGrandchild->inheritedMultiGroupJoin);
  EXPECT_FALSE(pUnqualifiedGrandchild->inheritedMultiGroupJoin);
  EXPECT_NE(pQualifiedGrandchild, pUnqualifiedGrandchild);
  EXPECT_EQ(pQualifiedGrandchild->id.groupId, pUnqualifiedGrandchild->id.groupId);
  EXPECT_EQ(pQualifiedGrandchild->id.subplanId, pUnqualifiedGrandchild->id.subplanId);
}

TEST(PlanPhysicalCreationTest, NestedMultiGroupJoinsQualifyOnlyTheirExactSubplanEdges) {
  constexpr int32_t kRootGroup = 101;
  constexpr int32_t kOuterExchangeGroup = 102;
  constexpr int32_t kInnerExchangeGroup = 103;
  constexpr int32_t kInnerMergeGroup = 104;
  constexpr int32_t kSiblingGroup = 105;
  constexpr int32_t kOuterDescendantGroup = 202;
  constexpr int32_t kInnerExchangeDescendantGroup = 203;
  constexpr int32_t kInnerMergeDescendantGroup = 204;
  constexpr int32_t kSiblingDescendantGroup = 205;
  constexpr int32_t kInnerMarkedRootGroup = 111;
  constexpr int32_t kInnerMarkedOuterExchangeGroup = 112;
  constexpr int32_t kMarkedInnerExchangeGroup = 113;
  constexpr int32_t kMarkedInnerMergeGroup = 114;
  constexpr int32_t kInnerMarkedSiblingGroup = 115;
  constexpr int32_t kInnerMarkedOuterDescendantGroup = 212;
  constexpr int32_t kMarkedInnerExchangeDescendantGroup = 213;
  constexpr int32_t kMarkedInnerMergeDescendantGroup = 214;
  constexpr int32_t kInnerMarkedSiblingDescendantGroup = 215;

  SExchangeLogicNode* pInnerExchange = makePlanExchange(kInnerExchangeGroup, "inner_left_key");
  SMergeLogicNode*    pInnerMerge = makePlanMerge(kInnerMergeGroup, "inner_right_key");
  SJoinLogicNode*     pInnerJoin = makePlanNode<SJoinLogicNode>(QUERY_NODE_LOGIC_PLAN_JOIN);
  ASSERT_NE(nullptr, pInnerExchange);
  ASSERT_NE(nullptr, pInnerMerge);
  ASSERT_NE(nullptr, pInnerJoin);
  pInnerJoin->joinAlgo = JOIN_ALGO_HASH;
  pInnerJoin->joinType = JOIN_TYPE_INNER;
  pInnerJoin->subType = JOIN_STYPE_NONE;
  pInnerJoin->multiGroupJoin = false;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pInnerJoin->node.pChildren, reinterpret_cast<SNode*>(pInnerExchange)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pInnerJoin->node.pChildren, reinterpret_cast<SNode*>(pInnerMerge)));

  SOperatorNode* pInnerEquality = makePlanNode<SOperatorNode>(QUERY_NODE_OPERATOR);
  ASSERT_NE(nullptr, pInnerEquality);
  pInnerEquality->opType = OP_TYPE_EQUAL;
  pInnerEquality->pLeft = reinterpret_cast<SNode*>(makePlanColumn("inner_left_key"));
  pInnerEquality->pRight = reinterpret_cast<SNode*>(makePlanColumn("inner_right_key"));
  ASSERT_NE(nullptr, pInnerEquality->pLeft);
  ASSERT_NE(nullptr, pInnerEquality->pRight);
  pInnerJoin->pColEqCond = reinterpret_cast<SNode*>(pInnerEquality);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pInnerJoin->node.pTargets,
                                                         reinterpret_cast<SNode*>(makePlanColumn("inner_left_key"))));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pInnerJoin->node.pTargets,
                                                         reinterpret_cast<SNode*>(makePlanColumn("inner_right_key"))));

  SExchangeLogicNode* pOuterExchange = makePlanExchange(kOuterExchangeGroup, "outer_right_key");
  SJoinLogicNode*     pOuterJoin = makePlanNode<SJoinLogicNode>(QUERY_NODE_LOGIC_PLAN_JOIN);
  ASSERT_NE(nullptr, pOuterExchange);
  ASSERT_NE(nullptr, pOuterJoin);
  pOuterJoin->joinAlgo = JOIN_ALGO_HASH;
  pOuterJoin->joinType = JOIN_TYPE_INNER;
  pOuterJoin->subType = JOIN_STYPE_NONE;
  pOuterJoin->multiGroupJoin = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pOuterJoin->node.pChildren, reinterpret_cast<SNode*>(pInnerJoin)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pOuterJoin->node.pChildren, reinterpret_cast<SNode*>(pOuterExchange)));

  SOperatorNode* pOuterEquality = makePlanNode<SOperatorNode>(QUERY_NODE_OPERATOR);
  ASSERT_NE(nullptr, pOuterEquality);
  pOuterEquality->opType = OP_TYPE_EQUAL;
  pOuterEquality->pLeft = reinterpret_cast<SNode*>(makePlanColumn("inner_left_key"));
  pOuterEquality->pRight = reinterpret_cast<SNode*>(makePlanColumn("outer_right_key"));
  ASSERT_NE(nullptr, pOuterEquality->pLeft);
  ASSERT_NE(nullptr, pOuterEquality->pRight);
  pOuterJoin->pColEqCond = reinterpret_cast<SNode*>(pOuterEquality);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pOuterJoin->node.pTargets,
                                                         reinterpret_cast<SNode*>(makePlanColumn("inner_left_key"))));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pOuterJoin->node.pTargets,
                                                         reinterpret_cast<SNode*>(makePlanColumn("outer_right_key"))));

  SProjectLogicNode*  pProject = makePlanNode<SProjectLogicNode>(QUERY_NODE_LOGIC_PLAN_PROJECT);
  SExchangeLogicNode* pSiblingExchange = makePlanExchange(kSiblingGroup, "sibling_key");
  ASSERT_NE(nullptr, pProject);
  ASSERT_NE(nullptr, pSiblingExchange);
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pProject->node.pChildren, reinterpret_cast<SNode*>(pOuterJoin)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pProject->node.pChildren, reinterpret_cast<SNode*>(pSiblingExchange)));

  SLogicSubplan* pRoot = makeLogicSubplan(kRootGroup, 0, reinterpret_cast<SLogicNode*>(pProject));
  SLogicSubplan* pOuterChild =
      makeLogicSubplan(kOuterExchangeGroup, 1,
                       reinterpret_cast<SLogicNode*>(makePlanExchange(kOuterDescendantGroup, "outer_descendant_key")));
  SLogicSubplan* pInnerExchangeChild = makeLogicSubplan(
      kInnerExchangeGroup, 1,
      reinterpret_cast<SLogicNode*>(makePlanExchange(kInnerExchangeDescendantGroup, "inner_exchange_descendant_key")));
  SLogicSubplan* pInnerMergeChild = makeLogicSubplan(
      kInnerMergeGroup, 1,
      reinterpret_cast<SLogicNode*>(makePlanExchange(kInnerMergeDescendantGroup, "inner_merge_descendant_key")));
  SLogicSubplan* pSiblingChild = makeLogicSubplan(
      kSiblingGroup, 1,
      reinterpret_cast<SLogicNode*>(makePlanExchange(kSiblingDescendantGroup, "sibling_descendant_key")));
  SLogicSubplan* pOuterDescendant = makeLogicSubplan(
      kOuterDescendantGroup, 2, reinterpret_cast<SLogicNode*>(makePlanExchange(302, "outer_leaf_key")));
  SLogicSubplan* pInnerExchangeDescendant =
      makeLogicSubplan(kInnerExchangeDescendantGroup, 2,
                       reinterpret_cast<SLogicNode*>(makePlanExchange(303, "inner_exchange_leaf_key")));
  SLogicSubplan* pInnerMergeDescendant = makeLogicSubplan(
      kInnerMergeDescendantGroup, 2, reinterpret_cast<SLogicNode*>(makePlanExchange(304, "inner_merge_leaf_key")));
  SLogicSubplan* pSiblingDescendant = makeLogicSubplan(
      kSiblingDescendantGroup, 2, reinterpret_cast<SLogicNode*>(makePlanExchange(305, "sibling_leaf_key")));
  ASSERT_NE(nullptr, pRoot);
  ASSERT_NE(nullptr, pOuterChild);
  ASSERT_NE(nullptr, pInnerExchangeChild);
  ASSERT_NE(nullptr, pInnerMergeChild);
  ASSERT_NE(nullptr, pSiblingChild);
  ASSERT_NE(nullptr, pOuterDescendant);
  ASSERT_NE(nullptr, pInnerExchangeDescendant);
  ASSERT_NE(nullptr, pInnerMergeDescendant);
  ASSERT_NE(nullptr, pSiblingDescendant);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pRoot->pChildren, reinterpret_cast<SNode*>(pOuterChild)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pRoot->pChildren, reinterpret_cast<SNode*>(pInnerExchangeChild)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pRoot->pChildren, reinterpret_cast<SNode*>(pInnerMergeChild)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pRoot->pChildren, reinterpret_cast<SNode*>(pSiblingChild)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pOuterChild->pChildren, reinterpret_cast<SNode*>(pOuterDescendant)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pInnerExchangeChild->pChildren,
                                                         reinterpret_cast<SNode*>(pInnerExchangeDescendant)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pInnerMergeChild->pChildren, reinterpret_cast<SNode*>(pInnerMergeDescendant)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pSiblingChild->pChildren, reinterpret_cast<SNode*>(pSiblingDescendant)));

  SExchangeLogicNode* pMarkedInnerExchange = makePlanExchange(kMarkedInnerExchangeGroup, "marked_inner_left_key");
  SMergeLogicNode*    pMarkedInnerMerge = makePlanMerge(kMarkedInnerMergeGroup, "marked_inner_right_key");
  SJoinLogicNode*     pMarkedInnerJoin = makePlanNode<SJoinLogicNode>(QUERY_NODE_LOGIC_PLAN_JOIN);
  ASSERT_NE(nullptr, pMarkedInnerExchange);
  ASSERT_NE(nullptr, pMarkedInnerMerge);
  ASSERT_NE(nullptr, pMarkedInnerJoin);
  pMarkedInnerJoin->joinAlgo = JOIN_ALGO_HASH;
  pMarkedInnerJoin->joinType = JOIN_TYPE_INNER;
  pMarkedInnerJoin->subType = JOIN_STYPE_NONE;
  pMarkedInnerJoin->multiGroupJoin = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pMarkedInnerJoin->node.pChildren,
                                                         reinterpret_cast<SNode*>(pMarkedInnerExchange)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pMarkedInnerJoin->node.pChildren, reinterpret_cast<SNode*>(pMarkedInnerMerge)));

  SOperatorNode* pMarkedInnerEquality = makePlanNode<SOperatorNode>(QUERY_NODE_OPERATOR);
  ASSERT_NE(nullptr, pMarkedInnerEquality);
  pMarkedInnerEquality->opType = OP_TYPE_EQUAL;
  pMarkedInnerEquality->pLeft = reinterpret_cast<SNode*>(makePlanColumn("marked_inner_left_key"));
  pMarkedInnerEquality->pRight = reinterpret_cast<SNode*>(makePlanColumn("marked_inner_right_key"));
  ASSERT_NE(nullptr, pMarkedInnerEquality->pLeft);
  ASSERT_NE(nullptr, pMarkedInnerEquality->pRight);
  pMarkedInnerJoin->pColEqCond = reinterpret_cast<SNode*>(pMarkedInnerEquality);
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pMarkedInnerJoin->node.pTargets,
                                      reinterpret_cast<SNode*>(makePlanColumn("marked_inner_left_key"))));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pMarkedInnerJoin->node.pTargets,
                                      reinterpret_cast<SNode*>(makePlanColumn("marked_inner_right_key"))));

  SExchangeLogicNode* pInnerMarkedOuterExchange =
      makePlanExchange(kInnerMarkedOuterExchangeGroup, "inner_marked_outer_right_key");
  SJoinLogicNode* pUnmarkedOuterJoin = makePlanNode<SJoinLogicNode>(QUERY_NODE_LOGIC_PLAN_JOIN);
  ASSERT_NE(nullptr, pInnerMarkedOuterExchange);
  ASSERT_NE(nullptr, pUnmarkedOuterJoin);
  pUnmarkedOuterJoin->joinAlgo = JOIN_ALGO_HASH;
  pUnmarkedOuterJoin->joinType = JOIN_TYPE_INNER;
  pUnmarkedOuterJoin->subType = JOIN_STYPE_NONE;
  pUnmarkedOuterJoin->multiGroupJoin = false;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pUnmarkedOuterJoin->node.pChildren, reinterpret_cast<SNode*>(pMarkedInnerJoin)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pUnmarkedOuterJoin->node.pChildren,
                                                         reinterpret_cast<SNode*>(pInnerMarkedOuterExchange)));

  SOperatorNode* pUnmarkedOuterEquality = makePlanNode<SOperatorNode>(QUERY_NODE_OPERATOR);
  ASSERT_NE(nullptr, pUnmarkedOuterEquality);
  pUnmarkedOuterEquality->opType = OP_TYPE_EQUAL;
  pUnmarkedOuterEquality->pLeft = reinterpret_cast<SNode*>(makePlanColumn("marked_inner_left_key"));
  pUnmarkedOuterEquality->pRight = reinterpret_cast<SNode*>(makePlanColumn("inner_marked_outer_right_key"));
  ASSERT_NE(nullptr, pUnmarkedOuterEquality->pLeft);
  ASSERT_NE(nullptr, pUnmarkedOuterEquality->pRight);
  pUnmarkedOuterJoin->pColEqCond = reinterpret_cast<SNode*>(pUnmarkedOuterEquality);
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pUnmarkedOuterJoin->node.pTargets,
                                      reinterpret_cast<SNode*>(makePlanColumn("marked_inner_left_key"))));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pUnmarkedOuterJoin->node.pTargets,
                                      reinterpret_cast<SNode*>(makePlanColumn("inner_marked_outer_right_key"))));

  SProjectLogicNode*  pInnerMarkedProject = makePlanNode<SProjectLogicNode>(QUERY_NODE_LOGIC_PLAN_PROJECT);
  SExchangeLogicNode* pInnerMarkedSiblingExchange =
      makePlanExchange(kInnerMarkedSiblingGroup, "inner_marked_sibling_key");
  ASSERT_NE(nullptr, pInnerMarkedProject);
  ASSERT_NE(nullptr, pInnerMarkedSiblingExchange);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pInnerMarkedProject->node.pChildren,
                                                         reinterpret_cast<SNode*>(pUnmarkedOuterJoin)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pInnerMarkedProject->node.pChildren,
                                                         reinterpret_cast<SNode*>(pInnerMarkedSiblingExchange)));

  SLogicSubplan* pInnerMarkedRoot =
      makeLogicSubplan(kInnerMarkedRootGroup, 0, reinterpret_cast<SLogicNode*>(pInnerMarkedProject));
  SLogicSubplan* pInnerMarkedOuterChild =
      makeLogicSubplan(kInnerMarkedOuterExchangeGroup, 1,
                       reinterpret_cast<SLogicNode*>(
                           makePlanExchange(kInnerMarkedOuterDescendantGroup, "inner_marked_outer_descendant_key")));
  SLogicSubplan* pMarkedInnerExchangeChild =
      makeLogicSubplan(kMarkedInnerExchangeGroup, 1,
                       reinterpret_cast<SLogicNode*>(makePlanExchange(kMarkedInnerExchangeDescendantGroup,
                                                                      "marked_inner_exchange_descendant_key")));
  SLogicSubplan* pMarkedInnerMergeChild =
      makeLogicSubplan(kMarkedInnerMergeGroup, 1,
                       reinterpret_cast<SLogicNode*>(
                           makePlanExchange(kMarkedInnerMergeDescendantGroup, "marked_inner_merge_descendant_key")));
  SLogicSubplan* pInnerMarkedSiblingChild =
      makeLogicSubplan(kInnerMarkedSiblingGroup, 1,
                       reinterpret_cast<SLogicNode*>(makePlanExchange(kInnerMarkedSiblingDescendantGroup,
                                                                      "inner_marked_sibling_descendant_key")));
  SLogicSubplan* pInnerMarkedOuterDescendant =
      makeLogicSubplan(kInnerMarkedOuterDescendantGroup, 2,
                       reinterpret_cast<SLogicNode*>(makePlanExchange(312, "inner_marked_outer_leaf_key")));
  SLogicSubplan* pMarkedInnerExchangeDescendant =
      makeLogicSubplan(kMarkedInnerExchangeDescendantGroup, 2,
                       reinterpret_cast<SLogicNode*>(makePlanExchange(313, "marked_inner_exchange_leaf_key")));
  SLogicSubplan* pMarkedInnerMergeDescendant =
      makeLogicSubplan(kMarkedInnerMergeDescendantGroup, 2,
                       reinterpret_cast<SLogicNode*>(makePlanExchange(314, "marked_inner_merge_leaf_key")));
  SLogicSubplan* pInnerMarkedSiblingDescendant =
      makeLogicSubplan(kInnerMarkedSiblingDescendantGroup, 2,
                       reinterpret_cast<SLogicNode*>(makePlanExchange(315, "inner_marked_sibling_leaf_key")));
  ASSERT_NE(nullptr, pInnerMarkedRoot);
  ASSERT_NE(nullptr, pInnerMarkedOuterChild);
  ASSERT_NE(nullptr, pMarkedInnerExchangeChild);
  ASSERT_NE(nullptr, pMarkedInnerMergeChild);
  ASSERT_NE(nullptr, pInnerMarkedSiblingChild);
  ASSERT_NE(nullptr, pInnerMarkedOuterDescendant);
  ASSERT_NE(nullptr, pMarkedInnerExchangeDescendant);
  ASSERT_NE(nullptr, pMarkedInnerMergeDescendant);
  ASSERT_NE(nullptr, pInnerMarkedSiblingDescendant);
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pInnerMarkedRoot->pChildren, reinterpret_cast<SNode*>(pInnerMarkedOuterChild)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pInnerMarkedRoot->pChildren,
                                                         reinterpret_cast<SNode*>(pMarkedInnerExchangeChild)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pInnerMarkedRoot->pChildren, reinterpret_cast<SNode*>(pMarkedInnerMergeChild)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pInnerMarkedRoot->pChildren,
                                                         reinterpret_cast<SNode*>(pInnerMarkedSiblingChild)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pInnerMarkedOuterChild->pChildren,
                                                         reinterpret_cast<SNode*>(pInnerMarkedOuterDescendant)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pMarkedInnerExchangeChild->pChildren,
                                                         reinterpret_cast<SNode*>(pMarkedInnerExchangeDescendant)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pMarkedInnerMergeChild->pChildren,
                                                         reinterpret_cast<SNode*>(pMarkedInnerMergeDescendant)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pInnerMarkedSiblingChild->pChildren,
                                                         reinterpret_cast<SNode*>(pInnerMarkedSiblingDescendant)));

  SQueryLogicPlan* pLogicPlan = makePlanNode<SQueryLogicPlan>(QUERY_NODE_LOGIC_PLAN);
  ASSERT_NE(nullptr, pLogicPlan);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pLogicPlan->pTopSubplans, reinterpret_cast<SNode*>(pRoot)));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pLogicPlan->pTopSubplans, reinterpret_cast<SNode*>(pInnerMarkedRoot)));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> logicPlanGuard(reinterpret_cast<SNode*>(pLogicPlan), nodesDestroyNode);

  SNode ast = {};
  ast.type = QUERY_NODE_SELECT_STMT;
  SPlanContext planContext = {};
  planContext.queryId = 1;
  planContext.pAstRoot = &ast;
  SQueryPlan* pPhysicalPlan = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createPhysiPlan(&planContext, pLogicPlan, &pPhysicalPlan, nullptr));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> physicalPlanGuard(reinterpret_cast<SNode*>(pPhysicalPlan),
                                                                   nodesDestroyNode);

  const SSubplan* pPhysicalRoot = findPhysicalSubplan(pPhysicalPlan, kRootGroup);
  const SSubplan* pPhysicalInnerMarkedRoot = findPhysicalSubplan(pPhysicalPlan, kInnerMarkedRootGroup);
  ASSERT_NE(nullptr, pPhysicalRoot);
  ASSERT_NE(nullptr, pPhysicalInnerMarkedRoot);
  EXPECT_EQ(18, pPhysicalPlan->numOfSubplans);
  EXPECT_EQ(kRootGroup, pPhysicalRoot->id.subplanId);
  EXPECT_EQ(kInnerMarkedRootGroup, pPhysicalInnerMarkedRoot->id.subplanId);
  EXPECT_EQ(0, LIST_LENGTH(pPhysicalRoot->pParents));
  EXPECT_EQ(0, LIST_LENGTH(pPhysicalInnerMarkedRoot->pParents));
  EXPECT_FALSE(pPhysicalRoot->inheritedMultiGroupJoin);
  EXPECT_FALSE(pPhysicalInnerMarkedRoot->inheritedMultiGroupJoin);

  const SPhysiNode* pPhysicalProject = pPhysicalRoot->pNode;
  ASSERT_NE(nullptr, pPhysicalProject);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_PROJECT, nodeType(pPhysicalProject));
  ASSERT_EQ(2, LIST_LENGTH(pPhysicalProject->pChildren));
  const auto* pPhysicalOuterJoin =
      reinterpret_cast<const SHashJoinPhysiNode*>(nodesListGetNode(pPhysicalProject->pChildren, 0));
  const SPhysiNode* pPhysicalSiblingRoute =
      reinterpret_cast<const SPhysiNode*>(nodesListGetNode(pPhysicalProject->pChildren, 1));
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, nodeType(pPhysicalOuterJoin));
  ASSERT_TRUE(pPhysicalOuterJoin->multiGroupJoin);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, nodeType(pPhysicalSiblingRoute));
  EXPECT_EQ(kSiblingGroup, reinterpret_cast<const SExchangePhysiNode*>(pPhysicalSiblingRoute)->srcStartGroupId);
  EXPECT_EQ(kSiblingGroup, reinterpret_cast<const SExchangePhysiNode*>(pPhysicalSiblingRoute)->srcEndGroupId);

  ASSERT_EQ(2, LIST_LENGTH(pPhysicalOuterJoin->node.pChildren));
  const auto* pPhysicalInnerJoin =
      reinterpret_cast<const SHashJoinPhysiNode*>(nodesListGetNode(pPhysicalOuterJoin->node.pChildren, 0));
  const SPhysiNode* pPhysicalOuterRoute =
      reinterpret_cast<const SPhysiNode*>(nodesListGetNode(pPhysicalOuterJoin->node.pChildren, 1));
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, nodeType(pPhysicalInnerJoin));
  ASSERT_FALSE(pPhysicalInnerJoin->multiGroupJoin);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, nodeType(pPhysicalOuterRoute));
  EXPECT_EQ(kOuterExchangeGroup, reinterpret_cast<const SExchangePhysiNode*>(pPhysicalOuterRoute)->srcStartGroupId);
  EXPECT_EQ(kOuterExchangeGroup, reinterpret_cast<const SExchangePhysiNode*>(pPhysicalOuterRoute)->srcEndGroupId);

  ASSERT_EQ(2, LIST_LENGTH(pPhysicalInnerJoin->node.pChildren));
  const SPhysiNode* pPhysicalInnerExchangeRoute =
      reinterpret_cast<const SPhysiNode*>(nodesListGetNode(pPhysicalInnerJoin->node.pChildren, 0));
  const SPhysiNode* pPhysicalInnerMergeRoute =
      reinterpret_cast<const SPhysiNode*>(nodesListGetNode(pPhysicalInnerJoin->node.pChildren, 1));
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, nodeType(pPhysicalInnerExchangeRoute));
  EXPECT_EQ(kInnerExchangeGroup,
            reinterpret_cast<const SExchangePhysiNode*>(pPhysicalInnerExchangeRoute)->srcStartGroupId);
  EXPECT_EQ(kInnerExchangeGroup,
            reinterpret_cast<const SExchangePhysiNode*>(pPhysicalInnerExchangeRoute)->srcEndGroupId);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_MERGE, nodeType(pPhysicalInnerMergeRoute));
  EXPECT_EQ(kInnerMergeGroup, reinterpret_cast<const SMergePhysiNode*>(pPhysicalInnerMergeRoute)->srcGroupId);
  EXPECT_EQ(kInnerMergeGroup, reinterpret_cast<const SMergePhysiNode*>(pPhysicalInnerMergeRoute)->srcEndGroupId);

  const SSubplan* pPhysicalOuterChild = getPhysicalChild(pPhysicalRoot, 0, kOuterExchangeGroup);
  const SSubplan* pPhysicalInnerExchangeChild = getPhysicalChild(pPhysicalRoot, 1, kInnerExchangeGroup);
  const SSubplan* pPhysicalInnerMergeChild = getPhysicalChild(pPhysicalRoot, 2, kInnerMergeGroup);
  const SSubplan* pPhysicalSiblingChild = getPhysicalChild(pPhysicalRoot, 3, kSiblingGroup);
  ASSERT_NE(nullptr, pPhysicalOuterChild);
  ASSERT_NE(nullptr, pPhysicalInnerExchangeChild);
  ASSERT_NE(nullptr, pPhysicalInnerMergeChild);
  ASSERT_NE(nullptr, pPhysicalSiblingChild);
  EXPECT_TRUE(pPhysicalOuterChild->inheritedMultiGroupJoin);
  EXPECT_TRUE(pPhysicalInnerExchangeChild->inheritedMultiGroupJoin);
  EXPECT_TRUE(pPhysicalInnerMergeChild->inheritedMultiGroupJoin);
  EXPECT_FALSE(pPhysicalSiblingChild->inheritedMultiGroupJoin);

  const SSubplan* pPhysicalOuterDescendant = getPhysicalChild(pPhysicalOuterChild, 0, kOuterDescendantGroup);
  const SSubplan* pPhysicalInnerExchangeDescendant =
      getPhysicalChild(pPhysicalInnerExchangeChild, 0, kInnerExchangeDescendantGroup);
  const SSubplan* pPhysicalInnerMergeDescendant =
      getPhysicalChild(pPhysicalInnerMergeChild, 0, kInnerMergeDescendantGroup);
  const SSubplan* pPhysicalSiblingDescendant = getPhysicalChild(pPhysicalSiblingChild, 0, kSiblingDescendantGroup);
  ASSERT_NE(nullptr, pPhysicalOuterDescendant);
  ASSERT_NE(nullptr, pPhysicalInnerExchangeDescendant);
  ASSERT_NE(nullptr, pPhysicalInnerMergeDescendant);
  ASSERT_NE(nullptr, pPhysicalSiblingDescendant);
  EXPECT_TRUE(pPhysicalOuterDescendant->inheritedMultiGroupJoin);
  EXPECT_TRUE(pPhysicalInnerExchangeDescendant->inheritedMultiGroupJoin);
  EXPECT_TRUE(pPhysicalInnerMergeDescendant->inheritedMultiGroupJoin);
  EXPECT_FALSE(pPhysicalSiblingDescendant->inheritedMultiGroupJoin);

  const SPhysiNode* pPhysicalInnerMarkedProject = pPhysicalInnerMarkedRoot->pNode;
  ASSERT_NE(nullptr, pPhysicalInnerMarkedProject);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_PROJECT, nodeType(pPhysicalInnerMarkedProject));
  ASSERT_EQ(2, LIST_LENGTH(pPhysicalInnerMarkedProject->pChildren));
  const auto* pPhysicalUnmarkedOuterJoin =
      reinterpret_cast<const SHashJoinPhysiNode*>(nodesListGetNode(pPhysicalInnerMarkedProject->pChildren, 0));
  const auto* pPhysicalInnerMarkedSiblingRoute =
      reinterpret_cast<const SExchangePhysiNode*>(nodesListGetNode(pPhysicalInnerMarkedProject->pChildren, 1));
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, nodeType(pPhysicalUnmarkedOuterJoin));
  ASSERT_FALSE(pPhysicalUnmarkedOuterJoin->multiGroupJoin);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, nodeType(pPhysicalInnerMarkedSiblingRoute));
  EXPECT_EQ(kInnerMarkedSiblingGroup, pPhysicalInnerMarkedSiblingRoute->srcStartGroupId);
  EXPECT_EQ(kInnerMarkedSiblingGroup, pPhysicalInnerMarkedSiblingRoute->srcEndGroupId);

  ASSERT_EQ(2, LIST_LENGTH(pPhysicalUnmarkedOuterJoin->node.pChildren));
  const auto* pPhysicalMarkedInnerJoin =
      reinterpret_cast<const SHashJoinPhysiNode*>(nodesListGetNode(pPhysicalUnmarkedOuterJoin->node.pChildren, 0));
  const auto* pPhysicalInnerMarkedOuterRoute =
      reinterpret_cast<const SExchangePhysiNode*>(nodesListGetNode(pPhysicalUnmarkedOuterJoin->node.pChildren, 1));
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, nodeType(pPhysicalMarkedInnerJoin));
  ASSERT_TRUE(pPhysicalMarkedInnerJoin->multiGroupJoin);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, nodeType(pPhysicalInnerMarkedOuterRoute));
  EXPECT_EQ(kInnerMarkedOuterExchangeGroup, pPhysicalInnerMarkedOuterRoute->srcStartGroupId);
  EXPECT_EQ(kInnerMarkedOuterExchangeGroup, pPhysicalInnerMarkedOuterRoute->srcEndGroupId);

  ASSERT_EQ(2, LIST_LENGTH(pPhysicalMarkedInnerJoin->node.pChildren));
  const auto* pPhysicalMarkedInnerExchangeRoute =
      reinterpret_cast<const SExchangePhysiNode*>(nodesListGetNode(pPhysicalMarkedInnerJoin->node.pChildren, 0));
  const auto* pPhysicalMarkedInnerMergeRoute =
      reinterpret_cast<const SMergePhysiNode*>(nodesListGetNode(pPhysicalMarkedInnerJoin->node.pChildren, 1));
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, nodeType(pPhysicalMarkedInnerExchangeRoute));
  EXPECT_EQ(kMarkedInnerExchangeGroup, pPhysicalMarkedInnerExchangeRoute->srcStartGroupId);
  EXPECT_EQ(kMarkedInnerExchangeGroup, pPhysicalMarkedInnerExchangeRoute->srcEndGroupId);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_MERGE, nodeType(pPhysicalMarkedInnerMergeRoute));
  EXPECT_EQ(kMarkedInnerMergeGroup, pPhysicalMarkedInnerMergeRoute->srcGroupId);
  EXPECT_EQ(kMarkedInnerMergeGroup, pPhysicalMarkedInnerMergeRoute->srcEndGroupId);

  const SSubplan* pPhysicalInnerMarkedOuterChild =
      getPhysicalChild(pPhysicalInnerMarkedRoot, 0, kInnerMarkedOuterExchangeGroup);
  const SSubplan* pPhysicalMarkedInnerExchangeChild =
      getPhysicalChild(pPhysicalInnerMarkedRoot, 1, kMarkedInnerExchangeGroup);
  const SSubplan* pPhysicalMarkedInnerMergeChild =
      getPhysicalChild(pPhysicalInnerMarkedRoot, 2, kMarkedInnerMergeGroup);
  const SSubplan* pPhysicalInnerMarkedSiblingChild =
      getPhysicalChild(pPhysicalInnerMarkedRoot, 3, kInnerMarkedSiblingGroup);
  ASSERT_NE(nullptr, pPhysicalInnerMarkedOuterChild);
  ASSERT_NE(nullptr, pPhysicalMarkedInnerExchangeChild);
  ASSERT_NE(nullptr, pPhysicalMarkedInnerMergeChild);
  ASSERT_NE(nullptr, pPhysicalInnerMarkedSiblingChild);
  EXPECT_FALSE(pPhysicalInnerMarkedOuterChild->inheritedMultiGroupJoin);
  EXPECT_TRUE(pPhysicalMarkedInnerExchangeChild->inheritedMultiGroupJoin);
  EXPECT_TRUE(pPhysicalMarkedInnerMergeChild->inheritedMultiGroupJoin);
  EXPECT_FALSE(pPhysicalInnerMarkedSiblingChild->inheritedMultiGroupJoin);

  const SSubplan* pPhysicalInnerMarkedOuterDescendant =
      getPhysicalChild(pPhysicalInnerMarkedOuterChild, 0, kInnerMarkedOuterDescendantGroup);
  const SSubplan* pPhysicalMarkedInnerExchangeDescendant =
      getPhysicalChild(pPhysicalMarkedInnerExchangeChild, 0, kMarkedInnerExchangeDescendantGroup);
  const SSubplan* pPhysicalMarkedInnerMergeDescendant =
      getPhysicalChild(pPhysicalMarkedInnerMergeChild, 0, kMarkedInnerMergeDescendantGroup);
  const SSubplan* pPhysicalInnerMarkedSiblingDescendant =
      getPhysicalChild(pPhysicalInnerMarkedSiblingChild, 0, kInnerMarkedSiblingDescendantGroup);
  ASSERT_NE(nullptr, pPhysicalInnerMarkedOuterDescendant);
  ASSERT_NE(nullptr, pPhysicalMarkedInnerExchangeDescendant);
  ASSERT_NE(nullptr, pPhysicalMarkedInnerMergeDescendant);
  ASSERT_NE(nullptr, pPhysicalInnerMarkedSiblingDescendant);
  EXPECT_FALSE(pPhysicalInnerMarkedOuterDescendant->inheritedMultiGroupJoin);
  EXPECT_TRUE(pPhysicalMarkedInnerExchangeDescendant->inheritedMultiGroupJoin);
  EXPECT_TRUE(pPhysicalMarkedInnerMergeDescendant->inheritedMultiGroupJoin);
  EXPECT_FALSE(pPhysicalInnerMarkedSiblingDescendant->inheritedMultiGroupJoin);
}

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

  run("SELECT ts, c2, UNIQUE(c1) FROM st1 PARTITION BY TBNAME ORDER BY TBNAME, ts, c2");

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

  run("SELECT _rowts, lag(c1, 1) FROM t1 ORDER BY ts");

  run("SELECT TBNAME, _rowts, lag(c1, 1, -1) FROM st1 PARTITION BY TBNAME ORDER BY TBNAME, ts");
  ASSERT_TRUE(aggOutputsMatchFuncs());
}

TEST_F(PlanBasicTest, withoutFrom) {
  useDb("root", "test");

  run("SELECT 1");
}
