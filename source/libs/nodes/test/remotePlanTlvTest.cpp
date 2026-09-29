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

// FQ remotePlan logic-node TLV round-trip tests (T1).
//
// SFederatedScanPhysiNode.pRemotePlan carries a *logic* sub-tree that is
// serialized over the wire (TLV) to the executor.  Before T1, logic-plan
// operators had no TLV codec and were silently dropped on deserialization
// (the JSON path used by EXPLAIN masked the gap).  These tests assert the
// minimal field set survives a nodesNodeToMsg → nodesMsgToNode round-trip.

#include <gtest/gtest.h>
#include <memory>

#include "functionMgt.h"
#include "nodes.h"
#include "plannodes.h"
#include "querynodes.h"

using namespace std;

namespace {

SNode* makeColumn(const char* name) {
  SColumnNode* pCol = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (TSDB_CODE_SUCCESS != code) {
    ADD_FAILURE() << "failed to create column node, code=" << code;
    return NULL;
  }
  snprintf(pCol->colName, sizeof(pCol->colName), "%s", name);
  pCol->node.resType.type = TSDB_DATA_TYPE_INT;
  pCol->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  return (SNode*)pCol;
}

SNode* makeQualifiedColumn(const char* tableAlias, const char* name) {
  SColumnNode* pCol = (SColumnNode*)makeColumn(name);
  if (NULL == pCol) return NULL;
  snprintf(pCol->tableAlias, sizeof(pCol->tableAlias), "%s", tableAlias);
  return (SNode*)pCol;
}

SNode* makeBinaryOp(EOperatorType opType, SNode* pLeft, SNode* pRight) {
  if (NULL == pLeft || NULL == pRight) {
    ADD_FAILURE() << "cannot create binary operator with a missing operand";
    nodesDestroyNode(pLeft);
    nodesDestroyNode(pRight);
    return NULL;
  }
  SOperatorNode* pOp = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOp);
  if (TSDB_CODE_SUCCESS != code) {
    ADD_FAILURE() << "failed to create operator node, code=" << code;
    nodesDestroyNode(pLeft);
    nodesDestroyNode(pRight);
    return NULL;
  }
  pOp->opType = opType;
  pOp->pLeft = pLeft;
  pOp->pRight = pRight;
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  return (SNode*)pOp;
}

// Strict append consumes pNode on both success and failure.
bool appendTestNode(SNodeList** ppList, SNode* pNode) {
  if (NULL == pNode) return false;
  int32_t code = nodesListMakeStrictAppend(ppList, pNode);
  if (TSDB_CODE_SUCCESS == code) return true;
  ADD_FAILURE() << "failed to append test node, code=" << code;
  return false;
}

SScanLogicNode* makeJoinScan(const char* table, const char* alias, bool filtered) {
  SScanLogicNode* pScan = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pScan);
  if (TSDB_CODE_SUCCESS != code) {
    ADD_FAILURE() << "failed to create scan node, code=" << code;
    return NULL;
  }
  pScan->scanType = SCAN_TYPE_EXTERNAL;
  for (const char* name : {"id", "val"}) {
    SNode* pColumn = makeQualifiedColumn(alias, name);
    if (NULL == pColumn) {
      nodesDestroyNode((SNode*)pScan);
      return NULL;
    }
    bool appended = appendTestNode(&pScan->pScanCols, pColumn);
    pColumn = NULL;
    if (!appended) {
      nodesDestroyNode((SNode*)pScan);
      return NULL;
    }
  }

  SExtTableNode* pExt = NULL;
  code = nodesMakeNode(QUERY_NODE_EXTERNAL_TABLE, (SNode**)&pExt);
  if (TSDB_CODE_SUCCESS != code) {
    ADD_FAILURE() << "failed to create external table node, code=" << code;
    nodesDestroyNode((SNode*)pScan);
    return NULL;
  }
  snprintf(pExt->remoteTableName, sizeof(pExt->remoteTableName), "%s", table);
  snprintf(pExt->table.tableAlias, sizeof(pExt->table.tableAlias), "%s", alias);
  pExt->sourceType = EXT_SOURCE_MYSQL;
  pScan->pExtTableNode = (SNode*)pExt;

  if (filtered) {
    SNode* pValue = NULL;
    code = nodesMakeValueNodeFromInt64(20, &pValue);
    if (TSDB_CODE_SUCCESS != code) {
      ADD_FAILURE() << "failed to create filter value, code=" << code;
      nodesDestroyNode((SNode*)pScan);
      return NULL;
    }
    SNode* pColumn = makeQualifiedColumn(alias, "val");
    if (NULL == pColumn) {
      nodesDestroyNode(pValue);
      nodesDestroyNode((SNode*)pScan);
      return NULL;
    }
    pScan->node.pConditions = makeBinaryOp(OP_TYPE_GREATER_THAN, pColumn, pValue);
    if (NULL == pScan->node.pConditions) {
      nodesDestroyNode((SNode*)pScan);
      return NULL;
    }
  }
  return pScan;
}

SJoinLogicNode* makeFilteredNestedJoinPlan(EJoinType joinType, bool nestedOnLeft) {
  SScanLogicNode* pNestedLeft = makeJoinScan("nested_left_t", "a", false);
  SScanLogicNode* pNestedRight = makeJoinScan("nested_right_t", "b", false);
  SJoinLogicNode* pNested = NULL;
  SNode*          pValue = NULL;
  SScanLogicNode* pFlat = NULL;
  SJoinLogicNode* pParent = NULL;
  SNode*          pChild = NULL;
  bool            appended = false;
  if (NULL == pNestedLeft || NULL == pNestedRight) {
    nodesDestroyNode((SNode*)pNestedLeft);
    nodesDestroyNode((SNode*)pNestedRight);
    return NULL;
  }
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN, (SNode**)&pNested);
  if (TSDB_CODE_SUCCESS != code) {
    ADD_FAILURE() << "failed to create nested join, code=" << code;
    nodesDestroyNode((SNode*)pNestedLeft);
    nodesDestroyNode((SNode*)pNestedRight);
    return NULL;
  }
  pNested->joinType = JOIN_TYPE_INNER;
  pNested->subType = JOIN_STYPE_OUTER;
  SNode* pLeftId = makeQualifiedColumn("a", "id");
  SNode* pRightId = makeQualifiedColumn("b", "id");
  if (NULL == pLeftId || NULL == pRightId) {
    nodesDestroyNode(pLeftId);
    nodesDestroyNode(pRightId);
    goto _nested_error;
  }
  pNested->pFullOnCond = makeBinaryOp(OP_TYPE_EQUAL, pLeftId, pRightId);
  if (NULL == pNested->pFullOnCond) goto _nested_error;
  code = nodesMakeValueNodeFromInt64(20, &pValue);
  if (TSDB_CODE_SUCCESS != code) {
    ADD_FAILURE() << "failed to create nested filter value, code=" << code;
    goto _nested_error;
  }
  pNested->node.pConditions =
      makeBinaryOp(OP_TYPE_GREATER_THAN, makeQualifiedColumn("a", "val"), pValue);
  pValue = NULL;
  if (NULL == pNested->node.pConditions) goto _nested_error;
  pChild = (SNode*)pNestedLeft;
  pNestedLeft = NULL;
  appended = appendTestNode(&pNested->node.pChildren, pChild);
  if (!appended) goto _nested_error;
  pChild = (SNode*)pNestedRight;
  pNestedRight = NULL;
  appended = appendTestNode(&pNested->node.pChildren, pChild);
  if (!appended) goto _nested_error;

  pFlat = makeJoinScan("flat_t", "c", false);
  if (NULL == pFlat) goto _nested_error;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN, (SNode**)&pParent);
  if (TSDB_CODE_SUCCESS != code) {
    ADD_FAILURE() << "failed to create parent join, code=" << code;
    goto _nested_error;
  }
  pParent->joinType = joinType;
  pParent->subType = JOIN_STYPE_OUTER;
  pParent->pFullOnCond = makeBinaryOp(OP_TYPE_EQUAL,
      makeQualifiedColumn(nestedOnLeft ? "a" : "c", "id"),
      makeQualifiedColumn(nestedOnLeft ? "c" : "a", "id"));
  if (NULL == pParent->pFullOnCond) goto _parent_error;
  pChild = (SNode*)(nestedOnLeft ? (SLogicNode*)pNested : (SLogicNode*)pFlat);
  if (nestedOnLeft) pNested = NULL;
  else pFlat = NULL;
  appended = appendTestNode(&pParent->node.pChildren, pChild);
  if (!appended) goto _parent_error;
  pChild = (SNode*)(nestedOnLeft ? (SLogicNode*)pFlat : (SLogicNode*)pNested);
  pNested = NULL;
  pFlat = NULL;
  appended = appendTestNode(&pParent->node.pChildren, pChild);
  if (!appended) goto _parent_error;
  if (!appendTestNode(&pParent->node.pTargets, makeQualifiedColumn("a", "id")) ||
      !appendTestNode(&pParent->node.pTargets, makeQualifiedColumn("c", "id"))) {
    goto _parent_error;
  }
  return pParent;

_parent_error:
  nodesDestroyNode((SNode*)pParent);
_nested_error:
  nodesDestroyNode(pValue);
  nodesDestroyNode((SNode*)pFlat);
  nodesDestroyNode((SNode*)pNested);
  nodesDestroyNode((SNode*)pNestedLeft);
  nodesDestroyNode((SNode*)pNestedRight);
  return NULL;
}

SNode* makeTimestampColumn(const char* name) {
  SColumnNode* pCol = (SColumnNode*)makeColumn(name);
  pCol->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  pCol->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;
  pCol->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  pCol->isPrimTs = true;
  return (SNode*)pCol;
}

SNode* makeLimit(int64_t limit, int64_t offset) {
  SLimitNode* pLimit = NULL;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_LIMIT, (SNode**)&pLimit), TSDB_CODE_SUCCESS);
  EXPECT_NE(pLimit, nullptr);
  EXPECT_EQ(nodesMakeValueNodeFromInt64(limit, (SNode**)&pLimit->limit), TSDB_CODE_SUCCESS);
  if (offset > 0) {
    EXPECT_EQ(nodesMakeValueNodeFromInt64(offset, (SNode**)&pLimit->offset), TSDB_CODE_SUCCESS);
  }
  return (SNode*)pLimit;
}

// Build: PROJECT(pProjections=[c1]) -> SORT(pSortKeys=[c1]) -> SCAN(EXTERNAL,
// pScanCols=[c1,c2], pExtTableNode, node.pConditions, LIMIT)
SNode* buildRemotePlanTree() {
  // Leaf external scan
  SScanLogicNode* pScan = NULL;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pScan), TSDB_CODE_SUCCESS);
  pScan->scanType = SCAN_TYPE_EXTERNAL;
  nodesListMakeStrictAppend(&pScan->pScanCols, makeColumn("c1"));
  nodesListMakeStrictAppend(&pScan->pScanCols, makeColumn("c2"));

  SExtTableNode* pExt = NULL;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_EXTERNAL_TABLE, (SNode**)&pExt), TSDB_CODE_SUCCESS);
  snprintf(pExt->sourceName, sizeof(pExt->sourceName), "%s", "mysrc");
  snprintf(pExt->remoteTableName, sizeof(pExt->remoteTableName), "%s", "remote_t");
  pExt->sourceType = 1;
  pScan->pExtTableNode = (SNode*)pExt;

  pScan->node.pConditions = makeColumn("remote_cond");  // minimal WHERE payload consumed by remote SQL rendering

  // Sort above scan
  SSortLogicNode* pSort = NULL;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT, (SNode**)&pSort), TSDB_CODE_SUCCESS);
  nodesListMakeStrictAppend(&pSort->pSortKeys, makeColumn("c1"));
  nodesListMakeStrictAppend(&pSort->node.pChildren, (SNode*)pScan);

  // Project on top, carrying a LIMIT
  SProjectLogicNode* pProj = NULL;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT, (SNode**)&pProj), TSDB_CODE_SUCCESS);
  nodesListMakeStrictAppend(&pProj->pProjections, makeColumn("c1"));
  pProj->isSetOpProj = true;
  pProj->isUnionDedup = false;
  pProj->node.pLimit = makeLimit(10, 5);
  nodesListMakeStrictAppend(&pProj->node.pChildren, (SNode*)pSort);

  return (SNode*)pProj;
}

SNode* buildQuotedExternalScanPlan(const char* database, const char* schema, const char* table,
                                   const char* remoteTable, const char* column) {
  SScanLogicNode* pScan = NULL;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pScan), TSDB_CODE_SUCCESS);
  pScan->scanType = SCAN_TYPE_EXTERNAL;
  nodesListMakeStrictAppend(&pScan->pScanCols, makeColumn(column));

  SExtTableNode* pExt = NULL;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_EXTERNAL_TABLE, (SNode**)&pExt), TSDB_CODE_SUCCESS);
  snprintf(pExt->sourceName, sizeof(pExt->sourceName), "%s", "quoted_source");
  snprintf(pExt->table.dbName, sizeof(pExt->table.dbName), "%s", database);
  snprintf(pExt->schemaName, sizeof(pExt->schemaName), "%s", schema);
  snprintf(pExt->table.tableName, sizeof(pExt->table.tableName), "%s", table);
  snprintf(pExt->remoteTableName, sizeof(pExt->remoteTableName), "%s", remoteTable);
  pScan->pExtTableNode = (SNode*)pExt;

  return (SNode*)pScan;
}

SNode* buildExternalCountPlan(const char* column) {
  SScanLogicNode* pScan = NULL;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pScan), TSDB_CODE_SUCCESS);
  pScan->scanType = SCAN_TYPE_EXTERNAL;
  nodesListMakeStrictAppend(&pScan->pScanCols, makeColumn(column));

  SExtTableNode* pExt = NULL;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_EXTERNAL_TABLE, (SNode**)&pExt), TSDB_CODE_SUCCESS);
  snprintf(pExt->sourceName, sizeof(pExt->sourceName), "%s", "count_source");
  snprintf(pExt->table.tableName, sizeof(pExt->table.tableName), "%s", "count_t");
  pScan->pExtTableNode = (SNode*)pExt;

  SFunctionNode* pCount = NULL;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pCount), TSDB_CODE_SUCCESS);
  snprintf(pCount->functionName, sizeof(pCount->functionName), "%s", "count");
  pCount->funcType = FUNCTION_TYPE_COUNT;
  nodesListMakeStrictAppend(&pCount->pParameterList, makeColumn(column));

  SProjectLogicNode* pProject = NULL;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT, (SNode**)&pProject), TSDB_CODE_SUCCESS);
  nodesListMakeStrictAppend(&pProject->pProjections, (SNode*)pCount);
  nodesListMakeStrictAppend(&pProject->node.pChildren, (SNode*)pScan);
  return (SNode*)pProject;
}

}  // namespace

class RemotePlanTlvTest : public testing::Test {};

TEST_F(RemotePlanTlvTest, projectSortScanRoundTrip) {
  SNode* pTree = buildRemotePlanTree();
  ASSERT_NE(pTree, nullptr);

  char*   pMsg = NULL;
  int32_t len = 0;
  ASSERT_EQ(nodesNodeToMsg(pTree, &pMsg, &len), TSDB_CODE_SUCCESS);
  ASSERT_NE(pMsg, nullptr);
  ASSERT_GT(len, 0);

  SNode* pBack = NULL;
  ASSERT_EQ(nodesMsgToNode(pMsg, len, &pBack), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBack, nullptr);

  // Root: PROJECT
  ASSERT_EQ(nodeType(pBack), QUERY_NODE_LOGIC_PLAN_PROJECT);
  SProjectLogicNode* pProj = (SProjectLogicNode*)pBack;
  EXPECT_EQ(LIST_LENGTH(pProj->pProjections), 1);
  EXPECT_TRUE(pProj->isSetOpProj);
  EXPECT_FALSE(pProj->isUnionDedup);
  ASSERT_NE(pProj->node.pLimit, nullptr);
  EXPECT_EQ(((SLimitNode*)pProj->node.pLimit)->limit != NULL
                ? ((SLimitNode*)pProj->node.pLimit)->limit->datum.i
                : -1,
            10);
  ASSERT_EQ(LIST_LENGTH(pProj->node.pChildren), 1);

  // Child: SORT
  SNode* pSortBack = nodesListGetNode(pProj->node.pChildren, 0);
  ASSERT_EQ(nodeType(pSortBack), QUERY_NODE_LOGIC_PLAN_SORT);
  SSortLogicNode* pSort = (SSortLogicNode*)pSortBack;
  EXPECT_EQ(LIST_LENGTH(pSort->pSortKeys), 1);
  ASSERT_EQ(LIST_LENGTH(pSort->node.pChildren), 1);

  // Leaf: SCAN
  SNode* pScanBack = nodesListGetNode(pSort->node.pChildren, 0);
  ASSERT_EQ(nodeType(pScanBack), QUERY_NODE_LOGIC_PLAN_SCAN);
  SScanLogicNode* pScan = (SScanLogicNode*)pScanBack;
  EXPECT_EQ(pScan->scanType, SCAN_TYPE_EXTERNAL);
  EXPECT_EQ(LIST_LENGTH(pScan->pScanCols), 2);
  ASSERT_NE(pScan->pExtTableNode, nullptr);
  EXPECT_EQ(nodeType(pScan->pExtTableNode), QUERY_NODE_EXTERNAL_TABLE);
  EXPECT_STREQ(((SExtTableNode*)pScan->pExtTableNode)->remoteTableName, "remote_t");
  ASSERT_NE(pScan->node.pConditions, nullptr);
  EXPECT_EQ(nodeType(pScan->node.pConditions), QUERY_NODE_COLUMN);
  EXPECT_STREQ(((SColumnNode*)pScan->node.pConditions)->colName, "remote_cond");

  taosMemoryFree(pMsg);
  nodesDestroyNode(pBack);
  nodesDestroyNode(pTree);
}

TEST_F(RemotePlanTlvTest, runtimeTimeRangeIsRenderedWithoutMutatingRemotePlan) {
  SScanLogicNode* pScan = NULL;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pScan), TSDB_CODE_SUCCESS);
  pScan->scanType = SCAN_TYPE_EXTERNAL;
  ASSERT_EQ(nodesListMakeStrictAppend(&pScan->pScanCols, makeTimestampColumn("ts")), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeStrictAppend(&pScan->node.pTargets, makeTimestampColumn("ts")), TSDB_CODE_SUCCESS);
  SColumnNode* pScanTs = (SColumnNode*)nodesListGetNode(pScan->pScanCols, 0);
  SColumnNode* pTargetTs = (SColumnNode*)nodesListGetNode(pScan->node.pTargets, 0);
  pScanTs->node.resType.precision = TSDB_TIME_PRECISION_NANO;
  pTargetTs->node.resType.precision = TSDB_TIME_PRECISION_NANO;

  SExtTableNode* pExt = NULL;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_EXTERNAL_TABLE, (SNode**)&pExt), TSDB_CODE_SUCCESS);
  snprintf(pExt->sourceName, sizeof(pExt->sourceName), "%s", "mysrc");
  snprintf(pExt->remoteTableName, sizeof(pExt->remoteTableName), "%s", "remote_t");
  pExt->sourceType = EXT_SOURCE_MYSQL;
  pScan->pExtTableNode = (SNode*)pExt;

  STimeWindow range = {};
  range.skey = 1123456789;
  range.ekey = 2123456789;
  SNodesRemoteSQLCtx ctx = {};
  ctx.pRuntimeTimeRange = &range;
  char* pSql = NULL;
  ASSERT_EQ(nodesRemotePlanToSQL((SLogicNode*)pScan, EXT_SOURCE_MYSQL, &ctx, &pSql), TSDB_CODE_SUCCESS);
  ASSERT_NE(pSql, nullptr);
  EXPECT_NE(strstr(pSql, "WHERE ("), nullptr);
  EXPECT_NE(strstr(pSql, " >= "), nullptr);
  EXPECT_NE(strstr(pSql, " <= "), nullptr);
  EXPECT_EQ(strstr(pSql, " < "), nullptr);
  EXPECT_NE(strstr(pSql, ".123456"), nullptr);
  EXPECT_EQ(strstr(pSql, ".123456789"), nullptr);
  taosMemoryFree(pSql);

  pSql = NULL;
  ASSERT_EQ(nodesRemotePlanToSQL((SLogicNode*)pScan, EXT_SOURCE_MYSQL, NULL, &pSql), TSDB_CODE_SUCCESS);
  ASSERT_NE(pSql, nullptr);
  EXPECT_EQ(strstr(pSql, "WHERE ("), nullptr);
  taosMemoryFree(pSql);
  nodesDestroyNode((SNode*)pScan);
}

TEST_F(RemotePlanTlvTest, aggRoundTrip) {
  SAggLogicNode* pAgg = NULL;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG, (SNode**)&pAgg), TSDB_CODE_SUCCESS);
  nodesListMakeStrictAppend(&pAgg->pGroupKeys, makeColumn("g1"));
  nodesListMakeStrictAppend(&pAgg->pAggFuncs, makeColumn("a1"));

  SScanLogicNode* pScan = NULL;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pScan), TSDB_CODE_SUCCESS);
  pScan->scanType = SCAN_TYPE_EXTERNAL;
  nodesListMakeStrictAppend(&pScan->pScanCols, makeColumn("g1"));
  nodesListMakeStrictAppend(&pAgg->node.pChildren, (SNode*)pScan);

  char*   pMsg = NULL;
  int32_t len = 0;
  ASSERT_EQ(nodesNodeToMsg((SNode*)pAgg, &pMsg, &len), TSDB_CODE_SUCCESS);

  SNode* pBack = NULL;
  ASSERT_EQ(nodesMsgToNode(pMsg, len, &pBack), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodeType(pBack), QUERY_NODE_LOGIC_PLAN_AGG);
  SAggLogicNode* pAggBack = (SAggLogicNode*)pBack;
  EXPECT_EQ(LIST_LENGTH(pAggBack->pGroupKeys), 1);
  EXPECT_EQ(LIST_LENGTH(pAggBack->pAggFuncs), 1);
  ASSERT_EQ(LIST_LENGTH(pAggBack->node.pChildren), 1);
  EXPECT_EQ(nodeType(nodesListGetNode(pAggBack->node.pChildren, 0)), QUERY_NODE_LOGIC_PLAN_SCAN);

  taosMemoryFree(pMsg);
  nodesDestroyNode(pBack);
  nodesDestroyNode((SNode*)pAgg);
}

TEST_F(RemotePlanTlvTest, indefRowsFuncRoundTrip) {
  SIndefRowsFuncLogicNode* pIndef = NULL;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC, (SNode**)&pIndef), TSDB_CODE_SUCCESS);
  nodesListMakeStrictAppend(&pIndef->pFuncs, makeColumn("lag_f"));

  SScanLogicNode* pScan = NULL;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pScan), TSDB_CODE_SUCCESS);
  pScan->scanType = SCAN_TYPE_EXTERNAL;
  nodesListMakeStrictAppend(&pIndef->node.pChildren, (SNode*)pScan);

  char*   pMsg = NULL;
  int32_t len = 0;
  ASSERT_EQ(nodesNodeToMsg((SNode*)pIndef, &pMsg, &len), TSDB_CODE_SUCCESS);

  SNode* pBack = NULL;
  ASSERT_EQ(nodesMsgToNode(pMsg, len, &pBack), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodeType(pBack), QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC);
  EXPECT_EQ(LIST_LENGTH(((SIndefRowsFuncLogicNode*)pBack)->pFuncs), 1);

  taosMemoryFree(pMsg);
  nodesDestroyNode(pBack);
  nodesDestroyNode((SNode*)pIndef);
}

TEST_F(RemotePlanTlvTest, escapesQuotedIdentifiersForEveryFederatedDialect) {
  struct RenderCase {
    int8_t      sourceType;
    const char* database;
    const char* schema;
    const char* table;
    const char* remoteTable;
    const char* column;
    const char* expectedSql;
  };

  const RenderCase cases[] = {
      {EXT_SOURCE_MYSQL, "db`name", "", "ignored", "table`name", "select`name",
       "SELECT `select``name` FROM `db``name`.`table``name`"},
      {EXT_SOURCE_MYSQL, "`db``name`", "", "ignored", "`table``name`", "`select``name`",
       "SELECT `select``name` FROM `db``name`.`table``name`"},
      {EXT_SOURCE_POSTGRESQL, "", "schema\"name", "table\"name", "", "select\"name",
       "SELECT \"select\"\"name\" FROM \"schema\"\"name\".\"table\"\"name\""},
      {EXT_SOURCE_INFLUXDB, "", "", "measurement\"name", "", "field\"name",
       "SELECT \"field\"\"name\" FROM \"measurement\"\"name\""},
  };

  for (const RenderCase& test : cases) {
    SCOPED_TRACE(test.sourceType);
    SNode* plan = buildQuotedExternalScanPlan(test.database, test.schema, test.table,
                                              test.remoteTable, test.column);
    ASSERT_NE(plan, nullptr);

    char* sql = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesRemotePlanToSQL((const SLogicNode*)plan, test.sourceType, NULL, &sql));
    ASSERT_NE(sql, nullptr);
    EXPECT_STREQ(test.expectedSql, sql);

    taosMemoryFree(sql);
    nodesDestroyNode(plan);
  }
}

TEST_F(RemotePlanTlvTest, keepsInfluxCountFieldDistinctFromCountStar) {
  struct RenderCase {
    const char* column;
    const char* expectedSql;
  };
  const RenderCase cases[] = {
      {"val", "SELECT COUNT(\"val\") FROM \"count_t\""},
      {"time", "SELECT COUNT(*) FROM \"count_t\""},
      {"_time", "SELECT COUNT(*) FROM \"count_t\""},
  };

  for (const RenderCase& test : cases) {
    SNode* plan = buildExternalCountPlan(test.column);
    ASSERT_NE(plan, nullptr);

    char* sql = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesRemotePlanToSQL((const SLogicNode*)plan, EXT_SOURCE_INFLUXDB, NULL, &sql));
    ASSERT_NE(sql, nullptr);
    EXPECT_STREQ(test.expectedSql, sql);

    taosMemoryFree(sql);
    nodesDestroyNode(plan);
  }
}

TEST_F(RemotePlanTlvTest, rendersFilteredFullJoinLegAsDerivedSource) {
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> pLeft((SNode*)makeJoinScan("left_t", "a", true), nodesDestroyNode);
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> pRight((SNode*)makeJoinScan("right_t", "b", false), nodesDestroyNode);
  ASSERT_NE(pLeft, nullptr);
  ASSERT_NE(pRight, nullptr);

  SJoinLogicNode* pJoin = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN, (SNode**)&pJoin);
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> joinOwner((SNode*)pJoin, nodesDestroyNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  pJoin->joinType = JOIN_TYPE_FULL;
  pJoin->subType = JOIN_STYPE_OUTER;
  pJoin->pFullOnCond = makeBinaryOp(OP_TYPE_EQUAL, makeQualifiedColumn("a", "id"),
                                    makeQualifiedColumn("b", "id"));
  ASSERT_NE(pJoin->pFullOnCond, nullptr);
  ASSERT_EQ(nodesListMakeStrictAppend(&pJoin->node.pChildren, pLeft.release()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeStrictAppend(&pJoin->node.pChildren, pRight.release()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeStrictAppend(&pJoin->node.pTargets, makeQualifiedColumn("a", "id")),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeStrictAppend(&pJoin->node.pTargets, makeQualifiedColumn("b", "id")),
            TSDB_CODE_SUCCESS);

  char* pSql = NULL;
  code = nodesRemotePlanToSQL((SLogicNode*)pJoin, EXT_SOURCE_MYSQL, NULL, &pSql);
  auto freeSql = [](char* sql) { taosMemoryFree(sql); };
  std::unique_ptr<char, decltype(freeSql)> sqlOwner(pSql, freeSql);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pSql, nullptr);
  EXPECT_NE(strstr(pSql, " FROM (SELECT "), nullptr) << pSql;
  const char* pFullJoin = strstr(pSql, " FULL JOIN ");
  ASSERT_NE(pFullJoin, nullptr) << pSql;
  EXPECT_NE(strstr(pSql, "`val` > 20"), nullptr) << pSql;
  EXPECT_EQ(strstr(pFullJoin, "`val` > 20"), nullptr) << pSql;
}

TEST_F(RemotePlanTlvTest, usesUniqueAliasesForFilteredFullJoinLegs) {
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> pLeft((SNode*)makeJoinScan("left_t", "", true), nodesDestroyNode);
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> pRight((SNode*)makeJoinScan("right_t", "", true), nodesDestroyNode);
  ASSERT_NE(pLeft, nullptr);
  ASSERT_NE(pRight, nullptr);

  SJoinLogicNode* pJoin = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN, (SNode**)&pJoin);
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> joinOwner((SNode*)pJoin, nodesDestroyNode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  pJoin->joinType = JOIN_TYPE_FULL;
  pJoin->subType = JOIN_STYPE_OUTER;
  pJoin->pFullOnCond = makeBinaryOp(OP_TYPE_EQUAL, makeQualifiedColumn("", "id"),
                                    makeQualifiedColumn("", "id"));
  ASSERT_NE(pJoin->pFullOnCond, nullptr);
  ASSERT_EQ(nodesListMakeStrictAppend(&pJoin->node.pChildren, pLeft.release()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeStrictAppend(&pJoin->node.pChildren, pRight.release()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeStrictAppend(&pJoin->node.pTargets, makeQualifiedColumn("", "id")),
            TSDB_CODE_SUCCESS);

  char* pSql = NULL;
  code = nodesRemotePlanToSQL((SLogicNode*)pJoin, EXT_SOURCE_MYSQL, NULL, &pSql);
  auto freeSql = [](char* sql) { taosMemoryFree(sql); };
  std::unique_ptr<char, decltype(freeSql)> sqlOwner(pSql, freeSql);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pSql, nullptr);
  EXPECT_NE(strstr(pSql, ") `_sq_l` FULL JOIN ("), nullptr) << pSql;
  EXPECT_NE(strstr(pSql, ") `_sq_r` ON "), nullptr) << pSql;
}

TEST_F(RemotePlanTlvTest, rendersFilteredNestedPreservedLegInParentWhere) {
  for (EJoinType joinType : {JOIN_TYPE_LEFT, JOIN_TYPE_RIGHT}) {
    bool nestedOnLeft = JOIN_TYPE_LEFT == joinType;
    SJoinLogicNode* pJoin = makeFilteredNestedJoinPlan(joinType, nestedOnLeft);
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> joinOwner((SNode*)pJoin, nodesDestroyNode);
    ASSERT_NE(pJoin, nullptr);
    char* pSql = NULL;
    int32_t code = nodesRemotePlanToSQL((SLogicNode*)pJoin, EXT_SOURCE_MYSQL, NULL, &pSql);
    auto freeSql = [](char* sql) { taosMemoryFree(sql); };
    std::unique_ptr<char, decltype(freeSql)> sqlOwner(pSql, freeSql);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    ASSERT_NE(pSql, nullptr);
    const char* pOuterJoin = strstr(pSql, JOIN_TYPE_LEFT == joinType ? " LEFT JOIN " : " RIGHT JOIN ");
    const char* pWhere = strstr(pSql, " WHERE ");
    const char* pFilter = strstr(pSql, "`a`.`val` > 20");
    ASSERT_NE(pOuterJoin, nullptr) << pSql;
    ASSERT_NE(pWhere, nullptr) << pSql;
    ASSERT_NE(pFilter, nullptr) << pSql;
    EXPECT_LT(pOuterJoin, pWhere) << pSql;
    const char* pDuplicateFilter = strstr(pFilter + 1, "`a`.`val` > 20");
    EXPECT_EQ(pDuplicateFilter, nullptr) << pSql;
  }
}

TEST_F(RemotePlanTlvTest, rejectsFilteredNestedNullableLeg) {
  for (EJoinType joinType : {JOIN_TYPE_LEFT, JOIN_TYPE_RIGHT}) {
    bool nestedOnLeft = JOIN_TYPE_RIGHT == joinType;
    SJoinLogicNode* pJoin = makeFilteredNestedJoinPlan(joinType, nestedOnLeft);
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> joinOwner((SNode*)pJoin, nodesDestroyNode);
    ASSERT_NE(pJoin, nullptr);
    char* pSql = NULL;
    int32_t code = nodesRemotePlanToSQL((SLogicNode*)pJoin, EXT_SOURCE_MYSQL, NULL, &pSql);
    auto freeSql = [](char* sql) { taosMemoryFree(sql); };
    std::unique_ptr<char, decltype(freeSql)> sqlOwner(pSql, freeSql);
    EXPECT_EQ(code, TSDB_CODE_EXT_SQL_RENDER_ERROR);
    EXPECT_EQ(pSql, nullptr);
  }
}
