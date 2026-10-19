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

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "nodes.h"
#include "plannodes.h"
#include "querynodes.h"

extern "C" void nodesTestSetSubplanAppendFailAfter(int32_t count);

namespace {

using NodePtr = std::unique_ptr<SNode, decltype(&nodesDestroyNode)>;

NodePtr makeJoin(ENodeType type, int32_t joinType, int32_t subType) {
  SNode* pNode = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(type, &pNode));
  if (type == QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN) {
    auto* pJoin = reinterpret_cast<SSortMergeJoinPhysiNode*>(pNode);
    pJoin->joinType = static_cast<EJoinType>(joinType);
    pJoin->subType = static_cast<EJoinSubType>(subType);
  } else {
    auto* pJoin = reinterpret_cast<SHashJoinPhysiNode*>(pNode);
    pJoin->joinType = static_cast<EJoinType>(joinType);
    pJoin->subType = static_cast<EJoinSubType>(subType);
  }
  return NodePtr(pNode, nodesDestroyNode);
}

std::string saveJson(const SNode* pNode) {
  char* pJson = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString(pNode, false, &pJson, nullptr));
  std::string json = pJson ? pJson : "";
  taosMemoryFree(pJson);
  return json;
}

struct JoinCase {
  EJoinType type;
  EJoinSubType subType;
  bool temporal;
};

// These are the normalized combinations produced by the physical planner.
const JoinCase kValidJoins[] = {
    {JOIN_TYPE_INNER, JOIN_STYPE_NONE, false}, {JOIN_TYPE_FULL, JOIN_STYPE_OUTER, false},
    {JOIN_TYPE_LEFT, JOIN_STYPE_OUTER, false}, {JOIN_TYPE_RIGHT, JOIN_STYPE_OUTER, false},
    {JOIN_TYPE_LEFT, JOIN_STYPE_SEMI, false}, {JOIN_TYPE_RIGHT, JOIN_STYPE_SEMI, false},
    {JOIN_TYPE_LEFT, JOIN_STYPE_ANTI, false}, {JOIN_TYPE_RIGHT, JOIN_STYPE_ANTI, false},
    {JOIN_TYPE_LEFT, JOIN_STYPE_ASOF, true}, {JOIN_TYPE_RIGHT, JOIN_STYPE_ASOF, true},
    {JOIN_TYPE_LEFT, JOIN_STYPE_WIN, true}, {JOIN_TYPE_RIGHT, JOIN_STYPE_WIN, true},
};

TEST(PlanDecodeTest, PhysicalJoinJsonAndTlvContracts) {
  for (auto nodeKind : {QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN, QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN,
                        QUERY_NODE_PHYSICAL_PLAN_THETA_JOIN}) {
    for (int32_t type = -1; type <= JOIN_TYPE_MAX_VALUE; ++type) {
      for (int32_t subType = -1; subType <= JOIN_STYPE_MAX_VALUE; ++subType) {
        SCOPED_TRACE(::testing::Message() << "node=" << nodeKind << " join=" << type << " subtype=" << subType);
        bool valid = false;
        for (const auto& entry : kValidJoins) {
          if (entry.type == type && entry.subType == subType &&
              (!entry.temporal || nodeKind == QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN)) valid = true;
        }
        NodePtr source = makeJoin(nodeKind, type, subType);
        for (bool json : {true, false}) {
          SCOPED_TRACE(json ? "JSON" : "TLV");
          SNode* pDecoded = reinterpret_cast<SNode*>(1);
          int32_t code;
          if (json) {
            std::string text = saveJson(source.get());
            code = nodesStringToNode(text.c_str(), &pDecoded);
          } else {
            char* pMsg = nullptr;
            int32_t len = 0;
            ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToMsg(source.get(), &pMsg, &len));
            code = nodesMsgToNode(pMsg, len, &pDecoded);
            taosMemoryFree(pMsg);
          }
          NodePtr decoded(pDecoded, nodesDestroyNode);
          if (valid) {
            ASSERT_EQ(TSDB_CODE_SUCCESS, code);
            ASSERT_NE(nullptr, pDecoded);
            EXPECT_EQ(nodeKind, nodeType(pDecoded));
            if (nodeKind == QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN) {
              auto* pJoin = reinterpret_cast<SSortMergeJoinPhysiNode*>(pDecoded);
              EXPECT_EQ(type, pJoin->joinType);
              EXPECT_EQ(subType, pJoin->subType);
            } else {
              auto* pJoin = reinterpret_cast<SHashJoinPhysiNode*>(pDecoded);
              EXPECT_EQ(type, pJoin->joinType);
              EXPECT_EQ(subType, pJoin->subType);
            }
          } else {
            EXPECT_EQ(json ? TSDB_CODE_INVALID_JSON_FORMAT : TSDB_CODE_INVALID_MSG, code);
            EXPECT_EQ(nullptr, pDecoded);
          }
        }
      }
    }
  }
}

// Use the legacy nested-tree format, including a sibling and another top-level
// tree, so failure cleanup covers transferred and untransferred subplans.
std::string legacyPlanJson(int32_t childLevel, int32_t siblingLevel) {
  SSubplan subplans[5] = {};
  for (int32_t i = 0; i < 5; ++i) {
    subplans[i].type = QUERY_NODE_PHYSICAL_SUBPLAN;
    subplans[i].id.subplanId = i;
  }
  subplans[1].level = childLevel;
  subplans[2].level = 2;
  subplans[3].level = siblingLevel;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&subplans[0].pChildren, reinterpret_cast<SNode*>(&subplans[1])));
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&subplans[1].pChildren, reinterpret_cast<SNode*>(&subplans[2])));
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&subplans[0].pChildren, reinterpret_cast<SNode*>(&subplans[3])));
  SNodeListNode level = {};
  level.node.type = QUERY_NODE_NODE_LIST;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&level.pNodeList, reinterpret_cast<SNode*>(&subplans[0])));
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&level.pNodeList, reinterpret_cast<SNode*>(&subplans[4])));
  SQueryPlan plan = {};
  plan.type = QUERY_NODE_PHYSICAL_PLAN;
  plan.numOfSubplans = 5;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&plan.pSubplans, reinterpret_cast<SNode*>(&level)));
  std::string json = saveJson(reinterpret_cast<SNode*>(&plan));
  nodesClearList(plan.pSubplans);
  nodesClearList(level.pNodeList);
  for (auto& subplan : subplans) nodesClearList(subplan.pChildren);
  return json;
}

TEST(PlanDecodeTest, LegacySubplanTreeRoundTrip) {
  std::string json = legacyPlanJson(1, 1);
  SNode* pDecoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(json.c_str(), &pDecoded));
  NodePtr decoded(pDecoded, nodesDestroyNode);
  auto* pPlan = reinterpret_cast<SQueryPlan*>(pDecoded);
  EXPECT_EQ(5, pPlan->numOfSubplans);
  EXPECT_EQ(3, LIST_LENGTH(pPlan->pSubplans));
  auto* pRoot = reinterpret_cast<SSubplan*>(nodesListGetNode(
      reinterpret_cast<SNodeListNode*>(nodesListGetNode(pPlan->pSubplans, 0))->pNodeList, 0));
  EXPECT_EQ(2, LIST_LENGTH(pRoot->pChildren));
}

TEST(PlanDecodeTest, InvalidLegacySubplanLevelsReturnNull) {
  for (const auto& json : {legacyPlanJson(-1, 1), legacyPlanJson(2, 1), legacyPlanJson(1, 4)}) {
    SNode* pDecoded = reinterpret_cast<SNode*>(1);
    EXPECT_EQ(TSDB_CODE_INVALID_JSON_FORMAT, nodesStringToNode(json.c_str(), &pDecoded));
    EXPECT_EQ(nullptr, pDecoded);
  }
}

TEST(PlanDecodeTest, PartialLegacySubplanTransferFailureReturnsNull) {
  std::string json = legacyPlanJson(1, 1);
  for (int32_t count = 0; count < 5; ++count) {
    SCOPED_TRACE(count);
    nodesTestSetSubplanAppendFailAfter(count);
    SNode* pDecoded = reinterpret_cast<SNode*>(1);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, nodesStringToNode(json.c_str(), &pDecoded));
    EXPECT_EQ(nullptr, pDecoded);
    nodesTestSetSubplanAppendFailAfter(-1);
  }
}

}  // namespace
