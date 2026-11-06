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

#include "planTestUtil.h"
#include "planner.h"
#include <cstring>

using namespace std;

class PlanSysTableTest : public PlannerTestBase {};

static const SScanLogicNode* findScanNode(const SLogicNode* pNode, const char* tableName) {
  if (pNode == NULL) return NULL;
  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_SCAN) {
    const SScanLogicNode* pScan = (const SScanLogicNode*)pNode;
    if (std::strcmp(pScan->tableName.tname, tableName) == 0) return pScan;
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    const SScanLogicNode* pScan = findScanNode((const SLogicNode*)pChild, tableName);
    if (pScan != NULL) return pScan;
  }
  return NULL;
}

static int32_t countScanNodes(const SLogicNode* pNode, const char* tableName) {
  if (pNode == NULL) return 0;
  int32_t count = 0;
  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_SCAN &&
      std::strcmp(((const SScanLogicNode*)pNode)->tableName.tname, tableName) == 0) {
    ++count;
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    count += countScanNodes((const SLogicNode*)pChild, tableName);
  }
  return count;
}

static const SLimitNode* findLimitNode(const SLogicNode* pNode) {
  if (pNode == NULL) return NULL;
  if (pNode->pLimit != NULL && nodeType(pNode->pLimit) == QUERY_NODE_LIMIT) {
    return (const SLimitNode*)pNode->pLimit;
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    const SLimitNode* pLimit = findLimitNode((const SLogicNode*)pChild);
    if (pLimit != NULL) return pLimit;
  }
  return NULL;
}

TEST_F(PlanSysTableTest, constantProjectionScansRowsOnly) {
  // Constant values are projected above the scan, so the scan needs no output columns.
  useDb("root", "information_schema");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (optimized) return;
    ASSERT_EQ(countScanNodes(pRoot, "ins_users"), 1);
    const SScanLogicNode* pScan = findScanNode(pRoot, "ins_users");
    ASSERT_NE(pScan, nullptr);
    EXPECT_EQ(pScan->scanType, SCAN_TYPE_SYSTEM_TABLE);
    EXPECT_EQ(LIST_LENGTH(pScan->pScanCols), 0);
    EXPECT_EQ(LIST_LENGTH(pScan->node.pTargets), 0);
  });
  run("SELECT 1 FROM ins_users");

  // The planner mock catalog provides st1 in the test database.
  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (optimized) return;
    ASSERT_EQ(countScanNodes(pRoot, "st1"), 1);
    const SScanLogicNode* pScan = findScanNode(pRoot, "st1");
    ASSERT_NE(pScan, nullptr);
    EXPECT_EQ(pScan->scanType, SCAN_TYPE_TAG);
    EXPECT_EQ(LIST_LENGTH(pScan->pScanCols), 0);
    EXPECT_EQ(LIST_LENGTH(pScan->pScanPseudoCols), 0);
    EXPECT_EQ(LIST_LENGTH(pScan->node.pTargets), 0);
  });
  run("SELECT TAGS 1 FROM st1");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (optimized) return;
    ASSERT_EQ(countScanNodes(pRoot, "st1"), 1);
    const SScanLogicNode* pScan = findScanNode(pRoot, "st1");
    ASSERT_NE(pScan, nullptr);
    EXPECT_EQ(LIST_LENGTH(pScan->pScanCols), 0);
    const SLimitNode* pLimit = findLimitNode(pRoot);
    ASSERT_NE(pLimit, nullptr);
    ASSERT_NE(pLimit->limit, nullptr);
    ASSERT_NE(pLimit->offset, nullptr);
    EXPECT_EQ(pLimit->limit->datum.i, 1);
    EXPECT_EQ(pLimit->offset->datum.i, 1);
  });
  run("SELECT TAGS 1 FROM st1 LIMIT 1 OFFSET 1");
}

TEST_F(PlanSysTableTest, constantProjectionSystemScanWithSort) {
  useDb("root", "information_schema");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (optimized) return;
    ASSERT_EQ(countScanNodes(pRoot, "ins_tables"), 1);
    const SScanLogicNode* pScan = findScanNode(pRoot, "ins_tables");
    ASSERT_NE(pScan, nullptr);
    EXPECT_EQ(pScan->scanType, SCAN_TYPE_SYSTEM_TABLE);
    EXPECT_GT(LIST_LENGTH(pScan->pScanCols), 0);
    EXPECT_GT(LIST_LENGTH(pScan->node.pTargets), 0);
  });
  run("SELECT 1 FROM ins_tables ORDER BY table_name LIMIT 2");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (optimized) return;
    ASSERT_EQ(countScanNodes(pRoot, "ins_tables"), 1);
    const SScanLogicNode* pScan = findScanNode(pRoot, "ins_tables");
    ASSERT_NE(pScan, nullptr);
    bool foundDbName = false;
    SNode* pCol = nullptr;
    FOREACH(pCol, pScan->pScanCols) {
      if (nodeType(pCol) != QUERY_NODE_COLUMN) continue;
      foundDbName |= ((SColumnNode*)pCol)->colId == TSDB_INS_USER_STABLES_DBNAME_COLID;
    }
    EXPECT_TRUE(foundDbName);
  });
  run("SELECT 1 FROM ins_tables ORDER BY db_name DESC LIMIT 2");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (optimized) return;
    ASSERT_EQ(countScanNodes(pRoot, "ins_tables"), 1);
    const SScanLogicNode* pScan = findScanNode(pRoot, "ins_tables");
    ASSERT_NE(pScan, nullptr);
    ASSERT_EQ(LIST_LENGTH(pScan->pScanCols), 1);
    const SNode* pCol = nodesListGetNode(pScan->pScanCols, 0);
    ASSERT_EQ(nodeType(pCol), QUERY_NODE_COLUMN);
    EXPECT_STREQ(((const SColumnNode*)pCol)->colName, "table_name");
  });
  run("SELECT 1 FROM ins_tables ORDER BY 1 LIMIT 2");
}

TEST_F(PlanSysTableTest, constantAggregatePartitionHasScanTarget) {
  useDb("root", "information_schema");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (optimized) return;
    const SScanLogicNode* pScan = findScanNode(pRoot, "ins_users");
    ASSERT_NE(pScan, nullptr);
    EXPECT_GT(LIST_LENGTH(pScan->pScanCols), 0);
    EXPECT_GT(LIST_LENGTH(pScan->node.pTargets), 0);
  });
  run("SELECT SUM(1) FROM ins_users HAVING SUM(1) > 0");
  setLogicPlanCheck([](const SLogicNode*, bool) {});
  run("SELECT 1 FROM ins_users PARTITION BY 1");

  useDb("root", "test");
  setLogicPlanCheck([](const SLogicNode* pRoot, bool optimized) {
    if (optimized) return;
    const SScanLogicNode* pScan = findScanNode(pRoot, "st1");
    ASSERT_NE(pScan, nullptr);
    EXPECT_GT(LIST_LENGTH(pScan->pScanCols), 0);
    EXPECT_GT(LIST_LENGTH(pScan->node.pTargets), 0);
  });
  run("SELECT TAGS SUM(1) FROM st1 HAVING SUM(1) > 0");
  setLogicPlanCheck([](const SLogicNode*, bool) {});
  run("SELECT TAGS 1 FROM st1 PARTITION BY 1");
}

TEST_F(PlanSysTableTest, informationSchema) {
  useDb("root", "information_schema");

  run("SELECT * FROM information_schema.ins_databases WHERE name = 'information_schema'");
}

TEST_F(PlanSysTableTest, withAgg) {
  useDb("root", "information_schema");

  run("SELECT COUNT(1) FROM ins_users");
}

TEST_F(PlanSysTableTest, tableCount) {
  useDb("root", "information_schema");

  run("SELECT COUNT(*) FROM ins_tables");

  run("SELECT COUNT(*) FROM ins_tables WHERE db_name = 'test'");

  run("SELECT COUNT(*) FROM ins_tables WHERE db_name = 'test' AND stable_name = 'st1'");

  run("SELECT db_name, COUNT(*) FROM ins_tables GROUP BY db_name");

  run("SELECT db_name, stable_name, COUNT(*) FROM ins_tables GROUP BY db_name, stable_name");
}
