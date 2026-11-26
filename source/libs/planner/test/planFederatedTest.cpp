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
#include <vector>

#include "planInt.h"
#include "functionMgt.h"

namespace {

class ExtMetaFixture {
 public:
  ExtMetaFixture(int32_t numCols, int32_t numTags) : columns_(numCols) {
    EXPECT_GE(numCols, numTags);
    memset(&meta_, 0, sizeof(meta_));
    meta_.pCols = columns_.data();
    meta_.numOfCols = numCols;

    memset(&node_, 0, sizeof(node_));
    node_.pExtMeta = &meta_;
    tstrncpy(node_.sourceName, "fake_source", sizeof(node_.sourceName));
    tstrncpy(node_.table.tableName, "fake_table", sizeof(node_.table.tableName));

    for (int32_t i = 0; i < numCols; ++i) {
      std::string name = "c" + std::to_string(i);
      tstrncpy(columns_[i].colName, name.c_str(), sizeof(columns_[i].colName));
      columns_[i].isTag = i >= numCols - numTags;
    }
  }

  ~ExtMetaFixture() { nodesDestroyList(scanCols_); }

  void addReference(int32_t columnIndex) {
    ASSERT_GE(columnIndex, 0);
    ASSERT_LT(columnIndex, meta_.numOfCols);

    SColumnNode* column = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&column));
    tstrncpy(column->colName, columns_[columnIndex].colName, sizeof(column->colName));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&scanCols_, (SNode*)column));
  }

  int32_t check() const { return planCheckRequiredExtColumns(&node_, scanCols_); }

 private:
  std::vector<SExtColumnDef> columns_;
  SExtTableMeta              meta_;
  SExtTableNode              node_;
  SNodeList*                 scanCols_ = NULL;
};

void addReferences(ExtMetaFixture* fixture, int32_t begin, int32_t end) {
  for (int32_t i = begin; i < end; ++i) {
    fixture->addReference(i);
  }
}

SNode* makeTestColumn(const char* name) {
  SColumnNode* pColumn = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pColumn);
  if (TSDB_CODE_SUCCESS != code) {
    ADD_FAILURE() << "failed to create test column, code=" << code;
    return NULL;
  }
  tstrncpy(pColumn->colName, name, sizeof(pColumn->colName));
  pColumn->node.resType.type = TSDB_DATA_TYPE_INT;
  pColumn->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  pColumn->colType = COLUMN_TYPE_COLUMN;
  return (SNode*)pColumn;
}

SNode* makeTestQualifiedColumn(const char* tableAlias, const char* name) {
  SColumnNode* pColumn = (SColumnNode*)makeTestColumn(name);
  if (NULL == pColumn) return NULL;
  tstrncpy(pColumn->tableAlias, tableAlias, sizeof(pColumn->tableAlias));
  return (SNode*)pColumn;
}

SNode* makeTestPredicate(const char* tableAlias) {
  SOperatorNode* pOp = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOp);
  if (TSDB_CODE_SUCCESS != code) {
    ADD_FAILURE() << "failed to create test predicate, code=" << code;
    return NULL;
  }
  pOp->opType = OP_TYPE_GREATER_THAN;
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  pOp->pLeft = makeTestQualifiedColumn(tableAlias, "val");
  if (NULL == pOp->pLeft) {
    nodesDestroyNode((SNode*)pOp);
    return NULL;
  }
  code = nodesMakeValueNodeFromInt64(20, &pOp->pRight);
  if (TSDB_CODE_SUCCESS != code) {
    ADD_FAILURE() << "failed to create test predicate value, code=" << code;
    nodesDestroyNode((SNode*)pOp);
    return NULL;
  }
  return (SNode*)pOp;
}

SScanLogicNode* makeTestScan(const char* tableAlias, SNode* pRemotePlan) {
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> remoteOwner(pRemotePlan, nodesDestroyNode);
  SScanLogicNode* pScan = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pScan);
  if (TSDB_CODE_SUCCESS != code) {
    ADD_FAILURE() << "failed to create test scan, code=" << code;
    return NULL;
  }
  pScan->pRemoteLogicPlan = remoteOwner.release();
  pScan->scanType = SCAN_TYPE_EXTERNAL;
  SNode* pTarget = makeTestQualifiedColumn(tableAlias, "val");
  if (NULL == pTarget) {
    nodesDestroyNode((SNode*)pScan);
    return NULL;
  }
  code = nodesListMakeStrictAppend(&pScan->node.pTargets, pTarget);
  if (TSDB_CODE_SUCCESS != code) {
    ADD_FAILURE() << "failed to append test scan target, code=" << code;
    nodesDestroyNode((SNode*)pScan);
    return NULL;
  }
  return pScan;
}

}  // namespace

TEST(PlanFederatedRequiredColumnsTest, AllowsFiniteProjectionFromWideMetadata) {
  for (int32_t width : {4097, 32768}) {
    ExtMetaFixture fixture(width, 0);
    fixture.addReference(0);
    fixture.addReference(0);
    EXPECT_EQ(TSDB_CODE_SUCCESS, fixture.check()) << "width=" << width;
  }
}

TEST(PlanFederatedRequiredColumnsTest, CountsUniqueReferencesAtTheColumnLimit) {
  ExtMetaFixture fixture(4097, 0);
  addReferences(&fixture, 0, TSDB_MAX_COLUMNS_NON_VIRTUAL);
  fixture.addReference(0);
  EXPECT_EQ(TSDB_CODE_SUCCESS, fixture.check());
}

TEST(PlanFederatedRequiredColumnsTest, RejectsOnlyActualColumnReferenceOverflow) {
  ExtMetaFixture fixture(4097, 0);
  addReferences(&fixture, 0, TSDB_MAX_COLUMNS_NON_VIRTUAL + 1);
  EXPECT_EQ(TSDB_CODE_EXT_REQUIRED_COLUMNS_TOO_MANY, fixture.check());
}

TEST(PlanFederatedRequiredColumnsTest, CountsTagReferencesSeparately) {
  ExtMetaFixture allowed(129, 129);
  addReferences(&allowed, 0, TSDB_MAX_TAGS);
  EXPECT_EQ(TSDB_CODE_SUCCESS, allowed.check());

  ExtMetaFixture rejected(129, 129);
  addReferences(&rejected, 0, TSDB_MAX_TAGS + 1);
  EXPECT_EQ(TSDB_CODE_EXT_REQUIRED_TAGS_TOO_MANY, rejected.check());
}

class PlanFederatedStoredFilterTest : public testing::TestWithParam<EExtSourceType> {
 protected:
  static SNode* makeColumn(const char* name) {
    SColumnNode* column = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&column);
    EXPECT_EQ(TSDB_CODE_SUCCESS, code);
    if (code != TSDB_CODE_SUCCESS) return NULL;
    column->node.resType.type = TSDB_DATA_TYPE_INT;
    column->node.resType.bytes = sizeof(int32_t);
    tstrncpy(column->colName, name, sizeof(column->colName));
    return (SNode*)column;
  }

  static SNode* makeCondition(const char* column) {
    SOperatorNode* condition = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&condition);
    EXPECT_EQ(TSDB_CODE_SUCCESS, code);
    if (code != TSDB_CODE_SUCCESS) return NULL;
    condition->opType = OP_TYPE_EQUAL;
    condition->node.resType.type = TSDB_DATA_TYPE_BOOL;
    condition->node.resType.bytes = sizeof(bool);
    condition->pLeft = makeColumn(column);
    if (condition->pLeft == NULL) {
      nodesDestroyNode((SNode*)condition);
      return NULL;
    }
    code = nodesMakeValueNodeFromInt64(1, &condition->pRight);
    EXPECT_EQ(TSDB_CODE_SUCCESS, code);
    if (code != TSDB_CODE_SUCCESS) {
      nodesDestroyNode((SNode*)condition);
      return NULL;
    }
    return (SNode*)condition;
  }

  void SetUp() override {
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&scan_));
    scan_->scanType = SCAN_TYPE_EXTERNAL;
    SNode* column = makeColumn("value");
    ASSERT_NE(column, nullptr);
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&scan_->pScanCols, column));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneList(scan_->pScanCols, &scan_->node.pTargets));
    // Saved filters already contain the remote column name after validation.
    scan_->pPushedConditions = makeCondition("Device");
    ASSERT_NE(scan_->pPushedConditions, nullptr);

    SExtTableNode* table = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_EXTERNAL_TABLE, (SNode**)&table));
    scan_->pExtTableNode = (SNode*)table;
    table->sourceType = GetParam();
    tstrncpy(table->sourceName, "filter_source", sizeof(table->sourceName));
    tstrncpy(table->table.dbName, "db", sizeof(table->table.dbName));
    tstrncpy(table->table.tableName, "readings", sizeof(table->table.tableName));
    tstrncpy(table->schemaName, "public", sizeof(table->schemaName));
    tstrncpy(table->srcSchema, "public", sizeof(table->srcSchema));
    table->pExtMeta = (SExtTableMeta*)taosMemoryCalloc(1, sizeof(SExtTableMeta));
    ASSERT_NE(table->pExtMeta, nullptr);
    table->pExtMeta->pCols = (SExtColumnDef*)taosMemoryCalloc(1, sizeof(SExtColumnDef));
    ASSERT_NE(table->pExtMeta->pCols, nullptr);
    table->pExtMeta->numOfCols = 1;
    tstrncpy(table->pExtMeta->pCols[0].colName, "Device", TSDB_COL_NAME_LEN);

    subplan_.subplanType = SUBPLAN_TYPE_SCAN;
    subplan_.pNode = (SLogicNode*)scan_;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&logicPlan_.pTopSubplans, (SNode*)&subplan_));
  }

  void TearDown() override {
    nodesDestroyNode((SNode*)scan_);
    nodesClearList(logicPlan_.pTopSubplans);
    nodesDestroyNode((SNode*)plan_);
  }

  void createPlan() {
    SSelectStmt statement = {};
    statement.node.type = QUERY_NODE_SELECT_STMT;
    SPlanContext context = {};
    context.pAstRoot = (SNode*)&statement;
    ASSERT_EQ(TSDB_CODE_SUCCESS, createPhysiPlan(&context, &logicPlan_, &plan_, NULL));
    ASSERT_NE(plan_, nullptr);
    auto* group = (SNodeListNode*)nodesListGetNode(plan_->pSubplans, 0);
    ASSERT_NE(group, nullptr);
    auto* subplan = (SSubplan*)nodesListGetNode(group->pNodeList, 0);
    ASSERT_NE(subplan, nullptr);
    ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN, nodeType(subplan->pNode));
    physical_ = (SFederatedScanPhysiNode*)subplan->pNode;
  }

  void checkRemoteFilter(const SFederatedScanPhysiNode* physical, bool queryFilter) {
    EXPECT_NE(physical->pushdownFlags & FQ_PUSHDOWN_REQUIRED_FILTER, 0u);
    char* sql = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesRemotePlanToSQL((const SLogicNode*)physical->pRemotePlan, GetParam(), NULL, &sql));
    ASSERT_NE(sql, nullptr);
    std::string statement(sql);
    taosMemoryFree(sql);
    EXPECT_NE(statement.find(" WHERE "), std::string::npos) << statement;
    EXPECT_NE(statement.find("Device"), std::string::npos) << statement;
    EXPECT_EQ(statement.find(" AND ") != std::string::npos, queryFilter) << statement;
  }

  SScanLogicNode*          scan_ = NULL;
  SLogicSubplan            subplan_ = {};
  SQueryLogicPlan          logicPlan_ = {};
  SQueryPlan*              plan_ = NULL;
  SFederatedScanPhysiNode* physical_ = NULL;
};

TEST_P(PlanFederatedStoredFilterTest, SqlServerTimestampClassesSurvivePlanTransport) {
  auto *table = reinterpret_cast<SExtTableNode *>(scan_->pExtTableNode);
  table->sourceType = EXT_SOURCE_SQLSERVER;
  taosMemoryFree(table->pExtMeta->pCols);
  const char *types[] = {"datetimeoffset(6)", "datetime2(6)", "datetime", "smalldatetime", "date"};
  const int32_t count = sizeof(types) / sizeof(types[0]);
  table->pExtMeta->pCols = static_cast<SExtColumnDef *>(taosMemoryCalloc(count, sizeof(SExtColumnDef)));
  ASSERT_NE(table->pExtMeta->pCols, nullptr);
  table->pExtMeta->numOfCols = count;
  for (int32_t i = 0; i < count; ++i) {
    auto &column = table->pExtMeta->pCols[i];
    snprintf(column.colName, sizeof(column.colName), "ts%d", i);
    tstrncpy(column.extTypeName, types[i], sizeof(column.extTypeName));
  }
  createPlan();
  ASSERT_NE(physical_, nullptr);
  char *serialized = nullptr;
  ASSERT_EQ(nodesNodeToString(reinterpret_cast<SNode *>(physical_), false, &serialized, nullptr),
            TSDB_CODE_SUCCESS);
  SNode *decoded = nullptr;
  int32_t code = nodesStringToNode(serialized, &decoded);
  taosMemoryFree(serialized);
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> owner(decoded, nodesDestroyNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  auto *restored = reinterpret_cast<SFederatedScanPhysiNode *>(decoded);
  ASSERT_NE(restored, nullptr);
  ASSERT_EQ(restored->numTimestampTypeHints, count);
  for (int32_t i = 0; i < count; ++i) {
    EXPECT_STREQ(restored->pTimestampTypeHints[i].extTypeName, types[i]);
    EXPECT_EQ(restored->pTimestampTypeHints[i].tzClass,
              i == 0 ? EXT_TIMESTAMP_TZ_CLASS_AWARE : EXT_TIMESTAMP_TZ_CLASS_NAIVE);
  }
}

TEST_P(PlanFederatedStoredFilterTest, KeepsFilterRemoteWithoutOptimizer) {
  createPlan();
  ASSERT_NE(physical_, nullptr);
  EXPECT_EQ(physical_->node.pConditions, nullptr);
  checkRemoteFilter(physical_, false);
  EXPECT_NE(scan_->pPushedConditions, nullptr);
  auto* leaf = (SScanLogicNode*)physical_->pRemotePlan;
  EXPECT_EQ(leaf->pPushedConditions, nullptr);
  EXPECT_EQ(LIST_LENGTH(leaf->pScanCols), 1);  // The stored-filter column need not be projected.
}

TEST_P(PlanFederatedStoredFilterTest, DoesNotMixLocalQueryFilterIntoRemoteFilter) {
  scan_->node.pConditions = makeCondition("value");
  ASSERT_NE(scan_->node.pConditions, nullptr);
  createPlan();
  ASSERT_NE(physical_, nullptr);
  ASSERT_NE(physical_->node.pConditions, nullptr);
  checkRemoteFilter(physical_, false);
}

TEST_P(PlanFederatedStoredFilterTest, MergesRemoteQueryFilterAndSurvivesPlanTransport) {
  SScanLogicNode* leaf = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode((SNode*)scan_, (SNode**)&leaf));
  scan_->pRemoteLogicPlan = (SNode*)leaf;
  leaf->node.pConditions = makeCondition("value");
  ASSERT_NE(leaf->node.pConditions, nullptr);
  SSortLogicNode* sort = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT, (SNode**)&sort));
  SOrderByExprNode* order = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&order));
  order->order = ORDER_ASC;
  order->pExpr = makeColumn("value");
  ASSERT_NE(order->pExpr, nullptr);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&sort->pSortKeys, (SNode*)order));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneList(leaf->node.pTargets, &sort->node.pTargets));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&sort->node.pChildren, (SNode*)leaf));
  leaf->node.pParent = (SLogicNode*)sort;
  scan_->pRemoteLogicPlan = (SNode*)sort;
  createPlan();
  ASSERT_NE(physical_, nullptr);
  EXPECT_EQ(physical_->node.pConditions, nullptr);
  checkRemoteFilter(physical_, true);

  SNode* clone = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode((SNode*)physical_, &clone));
  checkRemoteFilter((SFederatedScanPhysiNode*)clone, true);
  nodesDestroyNode(clone);

  for (bool binary : {false, true}) {
    char*   data = NULL;
    int32_t length = 0;
    SNode*  decoded = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS, binary ? nodesNodeToMsg((SNode*)physical_, &data, &length)
                                        : nodesNodeToString((SNode*)physical_, false, &data, &length));
    ASSERT_EQ(TSDB_CODE_SUCCESS, binary ? nodesMsgToNode(data, length, &decoded) : nodesStringToNode(data, &decoded));
    checkRemoteFilter((SFederatedScanPhysiNode*)decoded, true);
    nodesDestroyNode(decoded);
    taosMemoryFree(data);
  }
}

TEST_P(PlanFederatedStoredFilterTest, DoesNotMarkAnUnfilteredScanAsRequired) {
  nodesDestroyNode(scan_->pPushedConditions);
  scan_->pPushedConditions = NULL;
  createPlan();
  ASSERT_NE(physical_, nullptr);
  EXPECT_EQ(physical_->pushdownFlags & FQ_PUSHDOWN_REQUIRED_FILTER, 0u);
  EXPECT_EQ(physical_->node.pConditions, nullptr);
  EXPECT_EQ(((SScanLogicNode*)physical_->pRemotePlan)->node.pConditions, nullptr);
}

TEST_P(PlanFederatedStoredFilterTest, ReferenceKeepsExplicitAxisSeparateFromTimestampValue) {
  scan_->bindingKind = VTABLE_BINDING_REFERENCE;
  scan_->referenceUid = 42;
  scan_->expectedRversion = 3;
  tstrncpy(scan_->referenceAlias, "r1", sizeof(scan_->referenceAlias));
  tstrncpy(scan_->timeColumn, "event_time", sizeof(scan_->timeColumn));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(scan_->pPushedConditions, &scan_->pReferenceFilter));

  auto* value = reinterpret_cast<SColumnNode*>(nodesListGetNode(scan_->pScanCols, 0));
  tstrncpy(value->colName, "event_time", sizeof(value->colName));
  value->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  value->node.resType.bytes = sizeof(int64_t);
  SNode* axisNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(reinterpret_cast<SNode*>(value), &axisNode));
  auto* axis = reinterpret_cast<SColumnNode*>(axisNode);
  axis->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  axis->isPrimTs = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakePushFront(&scan_->pScanCols, axisNode));

  // A projection can expose the ordinary timestamp while the merge still needs
  // the axis as a dependency. Both slots must survive their shared source name.
  nodesDestroyList(scan_->node.pTargets);
  scan_->node.pTargets = nullptr;
  SNode* valueTarget = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(reinterpret_cast<SNode*>(value), &valueTarget));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&scan_->node.pTargets, valueTarget));
  createPlan();
  ASSERT_NE(physical_, nullptr);
  EXPECT_EQ(physical_->pRemotePlan, nullptr);
  EXPECT_EQ(physical_->bindingKind, VTABLE_BINDING_REFERENCE);
  EXPECT_STREQ(physical_->timeColumn, "event_time");
  EXPECT_NE(physical_->pReferenceFilter, nullptr);
  ASSERT_EQ(LIST_LENGTH(physical_->pScanCols), 2);
  auto* outputValueTarget = reinterpret_cast<STargetNode*>(nodesListGetNode(physical_->pScanCols, 0));
  auto* outputAxisTarget = reinterpret_cast<STargetNode*>(nodesListGetNode(physical_->pScanCols, 1));
  ASSERT_EQ(nodeType(outputValueTarget), QUERY_NODE_TARGET);
  ASSERT_EQ(nodeType(outputAxisTarget), QUERY_NODE_TARGET);
  auto* outputValue = reinterpret_cast<SColumnNode*>(outputValueTarget->pExpr);
  auto* outputAxis = reinterpret_cast<SColumnNode*>(outputAxisTarget->pExpr);
  // They read the same source value and share its physical output slot. Runtime
  // mappings must use these slot IDs rather than their two scan-list positions.
  ASSERT_EQ(LIST_LENGTH(physical_->node.pOutputDataBlockDesc->pSlots), 1);
  EXPECT_EQ(outputValueTarget->slotId, outputAxisTarget->slotId);
  EXPECT_EQ(outputValue->colId, 0);
  EXPECT_FALSE(outputValue->isPrimTs);
  EXPECT_EQ(outputAxis->colId, PRIMARYKEY_TIMESTAMP_COL_ID);
  EXPECT_TRUE(outputAxis->isPrimTs);
}

TEST_P(PlanFederatedStoredFilterTest, ReferenceCandidateLimitKeepsOffsetAndRejectsUnsafeShapes) {
  scan_->bindingKind = VTABLE_BINDING_REFERENCE;
  scan_->scanSeq[0] = 1;
  SVirtualScanLogicNode virtualScan = {};
  virtualScan.node.type = QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN;
  SProjectLogicNode project = {};
  project.node.type = QUERY_NODE_LOGIC_PLAN_PROJECT;
  ASSERT_EQ(nodesCloneList(scan_->node.pTargets, &project.pProjections), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeAppend(&virtualScan.node.pChildren, reinterpret_cast<SNode*>(scan_)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeAppend(&project.node.pChildren, reinterpret_cast<SNode*>(&virtualScan)), TSDB_CODE_SUCCESS);
  scan_->node.pParent = &virtualScan.node;
  virtualScan.node.pParent = &project.node;
  SLogicSubplan subplan = {};
  subplan.pNode = &project.node;
  SLimitNode* limit = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_LIMIT, reinterpret_cast<SNode**>(&limit)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeValueNodeFromInt64(1, reinterpret_cast<SNode**>(&limit->limit)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeValueNodeFromInt64(0, reinterpret_cast<SNode**>(&limit->offset)), TSDB_CODE_SUCCESS);
  project.node.pLimit = reinterpret_cast<SNode*>(limit);

  bool optimized = false;
  ASSERT_EQ(planPushDownReferenceLimit(&subplan, &optimized), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(optimized);
  EXPECT_EQ(reinterpret_cast<SLimitNode*>(scan_->node.pLimit)->limit->datum.i, 1);
  nodesDestroyNode(scan_->node.pLimit);
  scan_->node.pLimit = nullptr;
  limit->offset->datum.i = 1;
  optimized = false;
  ASSERT_EQ(planPushDownReferenceLimit(&subplan, &optimized), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(optimized);
  EXPECT_EQ(reinterpret_cast<SLimitNode*>(scan_->node.pLimit)->limit->datum.i, 2);
  EXPECT_EQ(reinterpret_cast<SLimitNode*>(scan_->node.pLimit)->offset->datum.i, 0);
  EXPECT_EQ(limit->offset->datum.i, 1);
  nodesDestroyNode(scan_->node.pLimit);
  scan_->node.pLimit = nullptr;

  project.node.pConditions = makeCondition("value");
  optimized = false;
  EXPECT_EQ(planPushDownReferenceLimit(&subplan, &optimized), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(optimized);
  nodesDestroyNode(project.node.pConditions);
  project.node.pConditions = nullptr;

  SSortLogicNode sort = {};
  sort.node.type = QUERY_NODE_LOGIC_PLAN_SORT;
  SOrderByExprNode* order = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, reinterpret_cast<SNode**>(&order)), TSDB_CODE_SUCCESS);
  order->pExpr = makeColumn("event_time");
  reinterpret_cast<SColumnNode*>(order->pExpr)->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  reinterpret_cast<SColumnNode*>(order->pExpr)->isPrimTs = true;
  order->order = ORDER_ASC;
  ASSERT_EQ(nodesListMakeAppend(&sort.pSortKeys, reinterpret_cast<SNode*>(order)), TSDB_CODE_SUCCESS);
  nodesClearList(project.node.pChildren);
  project.node.pChildren = nullptr;
  ASSERT_EQ(nodesListMakeAppend(&project.node.pChildren, reinterpret_cast<SNode*>(&sort)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeAppend(&sort.node.pChildren, reinterpret_cast<SNode*>(&virtualScan)), TSDB_CODE_SUCCESS);
  sort.node.pParent = &project.node;
  virtualScan.node.pParent = &sort.node;
  optimized = false;
  ASSERT_EQ(planPushDownReferenceLimit(&subplan, &optimized), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(optimized);
  nodesDestroyNode(scan_->node.pLimit);
  scan_->node.pLimit = nullptr;
  order->order = ORDER_DESC;
  optimized = false;
  EXPECT_EQ(planPushDownReferenceLimit(&subplan, &optimized), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(optimized);
  order->order = ORDER_ASC;
  SScanLogicNode secondSource = {};
  ASSERT_EQ(nodesListAppend(virtualScan.node.pChildren, reinterpret_cast<SNode*>(&secondSource)), TSDB_CODE_SUCCESS);
  optimized = false;
  EXPECT_EQ(planPushDownReferenceLimit(&subplan, &optimized), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(optimized);

  scan_->node.pParent = nullptr;
  nodesDestroyNode(project.node.pLimit);
  nodesDestroyList(project.pProjections);
  nodesDestroyList(sort.pSortKeys);
  nodesClearList(project.node.pChildren);
  nodesClearList(sort.node.pChildren);
  nodesClearList(virtualScan.node.pChildren);
}

INSTANTIATE_TEST_SUITE_P(MySqlAndPostgreSql, PlanFederatedStoredFilterTest,
                         testing::Values(EXT_SOURCE_MYSQL, EXT_SOURCE_POSTGRESQL));

TEST(PlanFederatedJoinFallbackTest, DetectsFilteredJoinHiddenByDockingScan) {
  struct TestCase {
    EJoinType joinType;
    bool      nestedOnLeft;
    bool      expectedFallback;
  } cases[] = {
      {JOIN_TYPE_LEFT, true, false},
      {JOIN_TYPE_LEFT, false, true},
      {JOIN_TYPE_RIGHT, true, true},
      {JOIN_TYPE_RIGHT, false, false},
      {JOIN_TYPE_FULL, true, true},
      {JOIN_TYPE_FULL, false, true},
  };

  for (const TestCase& test : cases) {
    SJoinLogicNode* pRemoteJoin = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN, (SNode**)&pRemoteJoin));
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> remoteOwner((SNode*)pRemoteJoin, nodesDestroyNode);
    pRemoteJoin->joinType = JOIN_TYPE_INNER;
    pRemoteJoin->node.pConditions = makeTestColumn("filtered_remote_join");
    ASSERT_NE(nullptr, pRemoteJoin->node.pConditions);

    SScanLogicNode* pNested = makeTestScan("nested", remoteOwner.release());
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> nestedOwner((SNode*)pNested, nodesDestroyNode);
    ASSERT_NE(nullptr, pNested);
    SScanLogicNode* pFlat = makeTestScan("flat", NULL);
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> flatOwner((SNode*)pFlat, nodesDestroyNode);
    ASSERT_NE(nullptr, pFlat);
    SJoinLogicNode* pParent = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN, (SNode**)&pParent));
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> parentOwner((SNode*)pParent, nodesDestroyNode);
    pParent->joinType = test.joinType;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesListMakeStrictAppend(&pParent->node.pChildren,
                                        test.nestedOnLeft ? nestedOwner.release() : flatOwner.release()));
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesListMakeStrictAppend(&pParent->node.pChildren,
                                        test.nestedOnLeft ? flatOwner.release() : nestedOwner.release()));

    EXPECT_EQ(test.expectedFallback, planFqOuterJoinNeedsNestedFallback(pParent));
    EXPECT_EQ((SNode*)pRemoteJoin, pNested->pRemoteLogicPlan);
  }
}

TEST(PlanFederatedJoinFallbackTest, DetectsParentWhereTargetingHiddenJoinLeg) {
  struct TestCase {
    EJoinType joinType;
    bool      nestedOnLeft;
    const char* nestedAlias;
    bool      expectedFallback;
  } cases[] = {
      {JOIN_TYPE_LEFT, false, "right_nested", true},
      {JOIN_TYPE_RIGHT, true, "left_nested", true},
      {JOIN_TYPE_LEFT, true, "left_nested", false},
      {JOIN_TYPE_RIGHT, false, "right_nested", false},
  };

  for (const TestCase& test : cases) {
    SJoinLogicNode* pRemoteJoin = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN, (SNode**)&pRemoteJoin));
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> remoteOwner((SNode*)pRemoteJoin, nodesDestroyNode);
    pRemoteJoin->joinType = JOIN_TYPE_INNER;

    SScanLogicNode* pNested = makeTestScan(test.nestedAlias, remoteOwner.release());
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> nestedOwner((SNode*)pNested, nodesDestroyNode);
    ASSERT_NE(nullptr, pNested);
    SScanLogicNode* pFlat = makeTestScan("flat", NULL);
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> flatOwner((SNode*)pFlat, nodesDestroyNode);
    ASSERT_NE(nullptr, pFlat);

    SJoinLogicNode* pParent = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN, (SNode**)&pParent));
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> parentOwner((SNode*)pParent, nodesDestroyNode);
    pParent->joinType = test.joinType;
    pParent->subType = JOIN_STYPE_OUTER;
    pParent->node.pConditions = makeTestPredicate(test.nestedAlias);
    ASSERT_NE(nullptr, pParent->node.pConditions);
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesListMakeStrictAppend(&pParent->node.pChildren,
                                        test.nestedOnLeft ? nestedOwner.release() : flatOwner.release()));
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesListMakeStrictAppend(&pParent->node.pChildren,
                                        test.nestedOnLeft ? flatOwner.release() : nestedOwner.release()));

    EXPECT_EQ(test.expectedFallback, planFqOuterJoinNeedsNestedFallback(pParent));
    EXPECT_EQ((SNode*)pRemoteJoin, pNested->pRemoteLogicPlan);
  }
}

namespace {

class Influx18PushdownTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const char* names[] = {"time", "tag", "i", "f", "u", "s", "b"};
    for (int32_t i = 0; i < 7; ++i) tstrncpy(columns_[i].colName, names[i], TSDB_COL_NAME_LEN);
    columns_[0].isPrimaryKey = true;
    columns_[1].isTag = true;
    meta_.pCols = columns_;
    meta_.numOfCols = 7;
    table_.pExtMeta = &meta_;
    scan_.pExtTableNode = (SNode*)&table_;
    capability_.dialect = EXT_SQL_DIALECT_INFLUXDB_1_8;
    ASSERT_EQ(TSDB_CODE_SUCCESS, fmFuncMgtInit());
  }

  SNode* column(const char* name, int8_t type = TSDB_DATA_TYPE_BIGINT) {
    SNode* pNode = makeTestColumn(name);
    if (pNode != nullptr) ((SExprNode*)pNode)->resType.type = type;
    return pNode;
  }

  SNode* integer(int64_t value) {
    SNode* pNode = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeValueNodeFromInt64(value, &pNode));
    return pNode;
  }

  SNode* text(const char* value) {
    SValueNode* pNode = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pNode));
    if (pNode == nullptr) return nullptr;
    size_t len = strlen(value);
    pNode->node.resType.type = TSDB_DATA_TYPE_VARCHAR;
    pNode->node.resType.bytes = len + VARSTR_HEADER_SIZE;
    pNode->translate = true;
    pNode->datum.p = (char*)taosMemoryCalloc(1, len + VARSTR_HEADER_SIZE);
    if (pNode->datum.p != nullptr) {
      varDataSetLen(pNode->datum.p, len);
      memcpy(varDataVal(pNode->datum.p), value, len);
    }
    return (SNode*)pNode;
  }

  SNode* operation(EOperatorType type, SNode* pLeft, SNode* pRight = nullptr) {
    SOperatorNode* pNode = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pNode));
    if (pNode == nullptr) {
      nodesDestroyNode(pLeft);
      nodesDestroyNode(pRight);
      return nullptr;
    }
    pNode->opType = type;
    pNode->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
    pNode->pLeft = pLeft;
    pNode->pRight = pRight;
    return (SNode*)pNode;
  }

  SNode* aggregate(const char* name, SNode* pArg, bool distinct = false) {
    SFunctionNode* pNode = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pNode));
    if (pNode == nullptr) {
      nodesDestroyNode(pArg);
      return nullptr;
    }
    tstrncpy(pNode->functionName, name, sizeof(pNode->functionName));
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pNode->pParameterList, pArg));
    EXPECT_EQ(TSDB_CODE_SUCCESS, fmGetFuncInfo(pNode, nullptr, 0));
    pNode->isDistinct = distinct;
    return (SNode*)pNode;
  }

  SNode* disjunction(SNode* pLeft, SNode* pRight) {
    SLogicConditionNode* pNode = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pNode));
    if (pNode == nullptr) {
      nodesDestroyNode(pLeft);
      nodesDestroyNode(pRight);
      return nullptr;
    }
    pNode->condType = LOGIC_COND_TYPE_OR;
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pNode->pParameterList, pLeft));
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pNode->pParameterList, pRight));
    return (SNode*)pNode;
  }

  bool pushable(SNode* pNode) {
    EXPECT_NE(nullptr, pNode);
    bool result = planFqInflux18ExprIsPushable(pNode, &scan_, &capability_, nullptr);
    nodesDestroyNode(pNode);
    return result;
  }

  SExtColumnDef columns_[7] = {};
  SExtTableMeta meta_ = {};
  SExtTableNode table_ = {};
  SScanLogicNode scan_ = {};
  SExtSourceCapability capability_ = {};
};

TEST_F(Influx18PushdownTest, FieldAndTagComparisonsRetainNullAndTypeSemantics) {
  EXPECT_TRUE(pushable(operation(OP_TYPE_EQUAL, column("i"), integer(7))));
  EXPECT_TRUE(pushable(operation(OP_TYPE_GREATER_THAN, column("f", TSDB_DATA_TYPE_DOUBLE), integer(7))));
  EXPECT_FALSE(pushable(operation(OP_TYPE_EQUAL, column("u", TSDB_DATA_TYPE_UBIGINT), integer(7))));
  EXPECT_TRUE(pushable(operation(OP_TYPE_EQUAL, column("tag", TSDB_DATA_TYPE_VARCHAR), text("west"))));
  EXPECT_TRUE(pushable(operation(OP_TYPE_NOT_EQUAL, column("tag", TSDB_DATA_TYPE_VARCHAR), text("west"))));
  EXPECT_FALSE(pushable(operation(OP_TYPE_EQUAL, column("tag", TSDB_DATA_TYPE_VARCHAR), text(""))));
  EXPECT_FALSE(pushable(operation(OP_TYPE_LOWER_THAN, column("tag", TSDB_DATA_TYPE_VARCHAR), text("z"))));
  EXPECT_TRUE(pushable(operation(OP_TYPE_IS_NULL, column("tag", TSDB_DATA_TYPE_VARCHAR))));
  EXPECT_FALSE(pushable(operation(OP_TYPE_IS_NULL, column("s", TSDB_DATA_TYPE_VARCHAR))));
  EXPECT_FALSE(pushable(operation(OP_TYPE_IS_TRUE, column("b", TSDB_DATA_TYPE_BOOL))));
  EXPECT_FALSE(pushable(operation(OP_TYPE_EQUAL, column("i"), column("f", TSDB_DATA_TYPE_DOUBLE))));
}

TEST_F(Influx18PushdownTest, OrRejectsTimeAndMixedColumnRoles) {
  EXPECT_TRUE(pushable(disjunction(operation(OP_TYPE_EQUAL, column("i"), integer(1)),
                                  operation(OP_TYPE_EQUAL, column("f", TSDB_DATA_TYPE_DOUBLE), integer(2)))));
  EXPECT_TRUE(pushable(disjunction(operation(OP_TYPE_EQUAL, column("tag", TSDB_DATA_TYPE_VARCHAR), text("a")),
                                  operation(OP_TYPE_EQUAL, column("tag", TSDB_DATA_TYPE_VARCHAR), text("b")))));
  EXPECT_FALSE(pushable(disjunction(operation(OP_TYPE_EQUAL, column("i"), integer(1)),
                                   operation(OP_TYPE_EQUAL, column("tag", TSDB_DATA_TYPE_VARCHAR), text("a")))));
  EXPECT_FALSE(pushable(disjunction(operation(OP_TYPE_GREATER_THAN, column("time", TSDB_DATA_TYPE_TIMESTAMP), integer(1)),
                                   operation(OP_TYPE_LOWER_THAN, column("time", TSDB_DATA_TYPE_TIMESTAMP), integer(10)))));
}

TEST_F(Influx18PushdownTest, ArithmeticRequiresFloatingOperandsAndNonzeroLiteralDivisor) {
  EXPECT_FALSE(pushable(operation(OP_TYPE_GREATER_THAN,
      operation(OP_TYPE_ADD, column("i"), integer(1)), integer(3))));
  EXPECT_TRUE(pushable(operation(OP_TYPE_GREATER_THAN,
      operation(OP_TYPE_ADD, column("f", TSDB_DATA_TYPE_DOUBLE), integer(1)), integer(3))));
  EXPECT_TRUE(pushable(operation(OP_TYPE_GREATER_THAN,
      operation(OP_TYPE_DIV, column("i"), integer(2)), integer(3))));
  EXPECT_FALSE(pushable(operation(OP_TYPE_GREATER_THAN,
      operation(OP_TYPE_DIV, column("i"), integer(0)), integer(3))));
  EXPECT_FALSE(pushable(operation(OP_TYPE_GREATER_THAN,
      operation(OP_TYPE_DIV, column("i"), column("f", TSDB_DATA_TYPE_DOUBLE)), integer(3))));
  EXPECT_FALSE(pushable(operation(OP_TYPE_GREATER_THAN,
      operation(OP_TYPE_REM, column("i"), integer(2)), integer(3))));
}

TEST_F(Influx18PushdownTest, InListHasBoundedLengthAndLiteralOperands) {
  for (int32_t size : {1, 64, 65}) {
    SNodeListNode* pList = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&pList));
    for (int32_t i = 0; i < size; ++i) {
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pList->pNodeList, integer(i)));
    }
    EXPECT_EQ(size <= 64, pushable(operation(OP_TYPE_IN, column("i"), (SNode*)pList)));
  }
}

TEST_F(Influx18PushdownTest, AggregateAllowlistChecksFieldTypesAndDistinct) {
  for (const char* name : {"count", "sum", "min", "max", "stddev_samp"}) {
    EXPECT_TRUE(pushable(aggregate(name, column("i")))) << name;
    EXPECT_TRUE(pushable(aggregate(name, column("f", TSDB_DATA_TYPE_DOUBLE)))) << name;
  }
  EXPECT_FALSE(pushable(aggregate("avg", column("i"))));
  EXPECT_TRUE(pushable(aggregate("avg", column("f", TSDB_DATA_TYPE_DOUBLE))));
  EXPECT_TRUE(pushable(aggregate("count", column("s", TSDB_DATA_TYPE_VARCHAR))));
  EXPECT_FALSE(pushable(aggregate("count", column("time", TSDB_DATA_TYPE_TIMESTAMP))));
  EXPECT_FALSE(pushable(aggregate("count", column("tag", TSDB_DATA_TYPE_VARCHAR))));
  EXPECT_FALSE(pushable(aggregate("count", column("i"), true)));
  EXPECT_FALSE(pushable(aggregate("sum", column("u", TSDB_DATA_TYPE_UBIGINT))));
  EXPECT_FALSE(pushable(aggregate("spread", column("i"))));
  EXPECT_TRUE(pushable(aggregate("spread", column("f", TSDB_DATA_TYPE_DOUBLE))));
  for (const char* name : {"stddev", "stddev_pop", "variance", "var_pop", "var_samp", "abs"}) {
    EXPECT_FALSE(pushable(aggregate(name, column("f", TSDB_DATA_TYPE_DOUBLE)))) << name;
  }
}

}  // namespace
