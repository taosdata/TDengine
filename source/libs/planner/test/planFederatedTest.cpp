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

#include <string>
#include <vector>

#include "planInt.h"

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

INSTANTIATE_TEST_SUITE_P(MySqlAndPostgreSql, PlanFederatedStoredFilterTest,
                         testing::Values(EXT_SOURCE_MYSQL, EXT_SOURCE_POSTGRESQL));
