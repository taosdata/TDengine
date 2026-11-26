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

template <typename WindowNode>
void checkWindowPartitionEquality(ENodeType type) {
  SNode* raw = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(type, &raw));
  NodePtr source(raw, nodesDestroyNode);
  auto*   window = reinterpret_cast<WindowNode*>(source.get());
  SNode*  col = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &col));
  reinterpret_cast<SColumnNode*>(col)->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&window->pPartitionKeys, col));
  window->numValuePartitionKeys = 1;

  SNode* cloned = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(raw, &cloned));
  NodePtr copy(cloned, nodesDestroyNode);
  EXPECT_TRUE(nodesEqualNode(raw, cloned));
  reinterpret_cast<WindowNode*>(cloned)->numValuePartitionKeys = 0;
  EXPECT_FALSE(nodesEqualNode(raw, cloned));
}

TEST(PlanDecodeTest, WindowPartitionComparisonAffectsEquality) {
  checkWindowPartitionEquality<SWindowFuncLogicNode>(QUERY_NODE_LOGIC_PLAN_WINDOW_FUNC);
  checkWindowPartitionEquality<SWindowFuncPhysiNode>(QUERY_NODE_PHYSICAL_PLAN_WINDOW_FUNC);
}

TEST(PlanDecodeTest, WindowPartitionComparisonSurvivesCloneAndWireFormats) {
  SNode* raw = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_WINDOW_FUNC, &raw));
  NodePtr source(raw, nodesDestroyNode);
  auto*   window = reinterpret_cast<SWindowFuncPhysiNode*>(source.get());
  for (int i = 0; i < 2; ++i) {
    SNode* col = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &col));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&window->pPartitionKeys, col));
  }

  for (int count : {0, 1, 2}) {
    window->numValuePartitionKeys = count;
    auto check = [count](SNode* decoded) {
      NodePtr result(decoded, nodesDestroyNode);
      ASSERT_NE(nullptr, result.get());
      const auto* copy = reinterpret_cast<const SWindowFuncPhysiNode*>(result.get());
      EXPECT_EQ(2, LIST_LENGTH(copy->pPartitionKeys));
      EXPECT_EQ(count, copy->numValuePartitionKeys);
    };

    SNode* decoded = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(raw, &decoded));
    check(decoded);
    std::string json = saveJson(raw);
    EXPECT_EQ(count != 0, json.find("NumValuePartitionKeys") != std::string::npos);
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(json.c_str(), &decoded));
    check(decoded);
    char*   msg = nullptr;
    int32_t len = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToMsg(raw, &msg, &len));
    int32_t code = nodesMsgToNode(msg, len, &decoded);
    taosMemoryFree(msg);
    ASSERT_EQ(TSDB_CODE_SUCCESS, code);
    check(decoded);
  }
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

TEST(PlanDecodeTest, Influx18ReferenceSurvivesCloneJsonAndTlv) {
  for (bool withBinding : {false, true}) {
    SNode* raw = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_EXTERNAL_TABLE, &raw));
    NodePtr source(raw, nodesDestroyNode);
    auto* table = reinterpret_cast<SExtTableNode*>(raw);
    if (withBinding) {
      tstrncpy(table->influxRp, "KeepCase", sizeof(table->influxRp));
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &table->pSeriesCondition));
      tstrncpy(reinterpret_cast<SColumnNode*>(table->pSeriesCondition)->colName, "site", TSDB_COL_NAME_LEN);
      SNode* column = nullptr;
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &column));
      auto* tag = reinterpret_cast<SColumnNode*>(column);
      tstrncpy(tag->colName, "site", sizeof(tag->colName));
      tag->node.resType.type = TSDB_DATA_TYPE_VARCHAR;
      tag->colType = COLUMN_TYPE_TAG;
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&table->pInfluxSchema, column));
    }
    for (int mode : {0, 1, 2}) {
      SNode* copy = nullptr;
      if (mode == 0) {
        ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(raw, &copy));
      } else if (mode == 1) {
        std::string json = saveJson(raw);
        ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(json.c_str(), &copy));
      } else {
        char* msg = nullptr;
        int32_t len = 0;
        ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToMsg(raw, &msg, &len));
        int32_t code = nodesMsgToNode(msg, len, &copy);
        taosMemoryFree(msg);
        ASSERT_EQ(TSDB_CODE_SUCCESS, code);
      }
      NodePtr owner(copy, nodesDestroyNode);
      auto* decoded = reinterpret_cast<SExtTableNode*>(copy);
      ASSERT_NE(nullptr, decoded);
      EXPECT_STREQ(withBinding ? "KeepCase" : "", decoded->influxRp);
      EXPECT_EQ(withBinding, decoded->pSeriesCondition != nullptr);
      EXPECT_EQ(withBinding ? 1 : 0, LIST_LENGTH(decoded->pInfluxSchema));
      if (withBinding) {
        EXPECT_NE(table->pSeriesCondition, decoded->pSeriesCondition);
        auto* tag = reinterpret_cast<SColumnNode*>(nodesListGetNode(decoded->pInfluxSchema, 0));
        EXPECT_STREQ("site", tag->colName);
        EXPECT_EQ(COLUMN_TYPE_TAG, tag->colType);
        EXPECT_EQ(TSDB_DATA_TYPE_VARCHAR, tag->node.resType.type);
      }
    }
  }
}

TEST(PlanDecodeTest, ExternalTableJsonChecksStringBounds) {
  SNode* raw = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_EXTERNAL_TABLE, &raw));
  NodePtr source(raw, nodesDestroyNode);
  auto* table = reinterpret_cast<SExtTableNode*>(raw);
  tstrncpy(table->influxRp, "rp", sizeof(table->influxRp));
  const struct {
    const char* name;
    size_t capacity;
  } fields[] = {
      {"DbName", sizeof(table->table.dbName)}, {"TableName", sizeof(table->table.tableName)},
      {"SourceName", sizeof(table->sourceName)}, {"SchemaName", sizeof(table->schemaName)},
      {"SrcHost", sizeof(table->srcHost)}, {"SrcUser", sizeof(table->srcUser)},
      {"SrcDatabase", sizeof(table->srcDatabase)}, {"SrcSchema", sizeof(table->srcSchema)},
      {"SrcOptions", sizeof(table->srcOptions)}, {"RemoteTableName", sizeof(table->remoteTableName)},
      {"InfluxRp", sizeof(table->influxRp)},
  };
  const std::string original = saveJson(raw);
  for (const auto& field : fields) {
    SCOPED_TRACE(field.name);
    for (size_t length : {field.capacity - 1, field.capacity}) {
      SCOPED_TRACE(length);
      std::string json = original;
      const std::string marker = std::string("\"") + field.name + "\":\"";
      size_t begin = json.find(marker);
      ASSERT_NE(std::string::npos, begin);
      begin += marker.size();
      size_t end = json.find('"', begin);
      ASSERT_NE(std::string::npos, end);
      const std::string value(length, 'x');
      json.replace(begin, end - begin, value);
      SNode* copy = nullptr;
      int32_t code = nodesStringToNode(json.c_str(), &copy);
      NodePtr owner(copy, nodesDestroyNode);
      if (length < field.capacity) {
        ASSERT_EQ(TSDB_CODE_SUCCESS, code);
        ASSERT_NE(nullptr, copy);
        EXPECT_NE(std::string::npos, saveJson(copy).find(marker + value + "\""));
      } else {
        EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, code);
        EXPECT_EQ(nullptr, copy);
      }
    }
  }
}

TEST(PlanDecodeTest, ExternalTableJsonKeepsFirstErrorAndAcceptsMissingFlags) {
  SNode* raw = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_EXTERNAL_TABLE, &raw));
  NodePtr source(raw, nodesDestroyNode);
  auto* table = reinterpret_cast<SExtTableNode*>(raw);
  tstrncpy(table->influxRp, "rp", sizeof(table->influxRp));
  const std::string original = saveJson(raw);
  for (const char* name : {"SrcNameFromPath", "NsFromPathOnly"}) {
    SCOPED_TRACE(name);
    const std::string marker = std::string("\"") + name + "\":false";
    size_t begin = original.find(marker);
    ASSERT_NE(std::string::npos, begin);
    for (bool missing : {true, false}) {
      SCOPED_TRACE(missing);
      std::string json = original;
      if (missing) {
        ASSERT_EQ(',', json[begin + marker.size()]);
        json.erase(begin, marker.size() + 1);
      } else {
        json.replace(begin, marker.size(), std::string("\"") + name + "\":\"false\"");
        const std::string rp = "\"InfluxRp\":\"rp\"";
        size_t rpBegin = json.find(rp);
        ASSERT_NE(std::string::npos, rpBegin);
        json.replace(rpBegin, rp.size(), "\"InfluxRp\":\"" +
                     std::string(sizeof(table->influxRp), 'x') + "\"");
      }
      SNode* copy = nullptr;
      int32_t code = nodesStringToNode(json.c_str(), &copy);
      NodePtr owner(copy, nodesDestroyNode);
      if (missing) {
        ASSERT_EQ(TSDB_CODE_SUCCESS, code);
        ASSERT_NE(nullptr, copy);
        auto* decoded = reinterpret_cast<SExtTableNode*>(copy);
        EXPECT_FALSE(decoded->srcNameFromPath);
        EXPECT_FALSE(decoded->nsFromPathOnly);
      } else {
        EXPECT_EQ(TSDB_CODE_FAILED, code);
        EXPECT_EQ(nullptr, copy);
      }
    }
  }
}

template <typename Plan>
static void checkSubqueryDependencies(ENodeType type) {
  SNode* raw = nullptr;
  ASSERT_EQ(nodesMakeNode(type, &raw), TSDB_CODE_SUCCESS);
  NodePtr source(raw, nodesDestroyNode);
  auto* plan = reinterpret_cast<Plan*>(raw);
  for (bool known : {false, true}) {
    plan->subqueryDepsKnown = known;
    for (int32_t count : {0, 2}) {
      taosArrayDestroy(plan->pSubqueryDeps);
      plan->pSubqueryDeps = taosArrayInit(2, sizeof(int32_t));
      ASSERT_NE(plan->pSubqueryDeps, nullptr);
      for (int32_t i = 0; i < count; ++i) {
        int32_t dependency = i * 3;
        ASSERT_NE(taosArrayPush(plan->pSubqueryDeps, &dependency), nullptr);
      }
      for (bool json : {false, true}) {
        SNode* decodedRaw = nullptr;
        if (json) {
          ASSERT_EQ(nodesStringToNode(saveJson(raw).c_str(), &decodedRaw), TSDB_CODE_SUCCESS);
        } else {
          char* msg = nullptr;
          int32_t len = 0;
          ASSERT_EQ(nodesNodeToMsg(raw, &msg, &len), TSDB_CODE_SUCCESS);
          int32_t code = nodesMsgToNode(msg, len, &decodedRaw);
          taosMemoryFree(msg);
          ASSERT_EQ(code, TSDB_CODE_SUCCESS);
        }
        NodePtr decoded(decodedRaw, nodesDestroyNode);
        auto* copy = reinterpret_cast<Plan*>(decodedRaw);
        EXPECT_EQ(copy->subqueryDepsKnown, known);
        ASSERT_EQ(taosArrayGetSize(copy->pSubqueryDeps), known ? count : 0);
        if (known && count > 0) EXPECT_EQ(*static_cast<int32_t*>(taosArrayGet(copy->pSubqueryDeps, 1)), 3);
      }
    }
  }
}

TEST(PlanDecodeTest, SubqueryDependenciesSurviveJsonAndTlv) {
  checkSubqueryDependencies<SQueryPlan>(QUERY_NODE_PHYSICAL_PLAN);
}

TEST(PlanDecodeTest, TaskDependenciesSurviveJsonAndTlv) {
  checkSubqueryDependencies<SSubplan>(QUERY_NODE_PHYSICAL_SUBPLAN);
}
