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

// A plain SELECT on a system table must carry SSubplan.showRewrite on its system table scan
// subplans: that flag (QUERY_MSG_MASK_SHOW_REWRITE) is what exempts a vnode subquery from the
// license check, so ins_tables / ins_columns stay readable after the license expired.
TEST_F(PlanSysTableTest, selectMarksSysScanSubplanLicenseExempt) {
  useDb("root", "information_schema");

  for (const char* sql : {"SELECT * FROM information_schema.ins_tables",
                          "SELECT * FROM information_schema.ins_columns", "SELECT COUNT(*) FROM ins_tables"}) {
    runFullPlan(sql, [sql](const SQueryPlan* pPlan) {
      ASSERT_NE(nullptr, pPlan);
      int32_t sysScans = 0;
      SNode*  pLevel = nullptr;
      FOREACH(pLevel, pPlan->pSubplans) {
        SNode* pNode = nullptr;
        FOREACH(pNode, ((SNodeListNode*)pLevel)->pNodeList) {
          const SSubplan* pSubplan = (const SSubplan*)pNode;
          if (pSubplan->pNode != nullptr &&
              (nodeType(pSubplan->pNode) == QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN ||
               nodeType(pSubplan->pNode) == QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN ||
               nodeType(pSubplan->pNode) == QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN_BY_TYPE)) {
            ++sysScans;
            EXPECT_TRUE(pSubplan->showRewrite) << sql;
          }
        }
      }
      EXPECT_GT(sysScans, 0) << sql;
    });
  }
}

// Whitelist, so a scan type added later counts as reading user data.
static bool isSysOrExchangeLeaf(ENodeType type) {
  switch (type) {
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN_BY_TYPE:
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:
      return true;
    default:
      return false;
  }
}

// Number of leaves that are neither a system table scan nor an exchange, i.e. that read user data.
static int32_t countUserTableScans(const SPhysiNode* pNode) {
  if (pNode == nullptr) {
    return 0;
  }
  if (LIST_LENGTH(pNode->pChildren) == 0) {
    return isSysOrExchangeLeaf(nodeType(pNode)) ? 0 : 1;
  }
  int32_t num = 0;
  SNode*  pChild = nullptr;
  FOREACH(pChild, pNode->pChildren) { num += countUserTableScans((const SPhysiNode*)pChild); }
  return num;
}

static int32_t countSysTableScans(const SPhysiNode* pNode) {
  if (pNode == nullptr) {
    return 0;
  }
  int32_t num = 0;
  switch (nodeType(pNode)) {
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN_BY_TYPE:
      num = 1;
      break;
    default:
      break;
  }
  SNode* pChild = nullptr;
  FOREACH(pChild, pNode->pChildren) { num += countSysTableScans((const SPhysiNode*)pChild); }
  return num;
}

static void checkLicenseExemptSubplans(const SQueryPlan* pPlan, const char* sql, int32_t* pExempt,
                                       int32_t* pUserScans) {
  SNode* pLevel = nullptr;
  FOREACH(pLevel, pPlan->pSubplans) {
    SNode* pNode = nullptr;
    FOREACH(pNode, ((SNodeListNode*)pLevel)->pNodeList) {
      const SSubplan* pSubplan = (const SSubplan*)pNode;
      int32_t         num = countUserTableScans(pSubplan->pNode);
      *pUserScans += num;
      if (pSubplan->showRewrite) {
        ++(*pExempt);
        EXPECT_EQ(0, num) << sql << ": user table scan in a license-exempt subplan";
        EXPECT_GT(countSysTableScans(pSubplan->pNode), 0) << sql << ": license-exempt subplan reads no system table";
      }
    }
  }
  // Subqueries are planned as child query plans.
  SNode* pChild = nullptr;
  FOREACH(pChild, pPlan->pChildren) {
    checkLicenseExemptSubplans((const SQueryPlan*)pChild, sql, pExempt, pUserScans);
  }
}

// The license exemption applies to a whole subplan, so a subplan flagged showRewrite must never
// also scan a user table; otherwise user data would stay readable after the license expired.
TEST_F(PlanSysTableTest, licenseExemptSubplanScansOnlySysTables) {
  useDb("root", "test");

  struct {
    const char* sql;
    bool        hasSysScan;
  } cases[] = {
      {"SELECT table_name FROM information_schema.ins_tables UNION ALL SELECT c2 FROM t1", true},
      {"SELECT * FROM t1 WHERE c2 IN (SELECT table_name FROM information_schema.ins_tables)", true},
      {"SELECT COUNT(*) FROM information_schema.ins_tables UNION ALL SELECT COUNT(*) FROM t1", true},
      // The join turns the ins_tables scan into a table merge scan, so no subplan is exempt here.
      {"SELECT a.table_name, b.c1 FROM information_schema.ins_tables a, t1 b", false},
      // User tables only: no subplan may be exempt, including merge subplans whose leaves are all exchanges.
      {"SELECT * FROM st1", false},
      {"SELECT COUNT(*) FROM st1", false},
  };
  for (const auto& c : cases) {
    const char* sql = c.sql;
    bool        hasSysScan = c.hasSysScan;
    runFullPlan(sql, [sql, hasSysScan](const SQueryPlan* pPlan) {
      ASSERT_NE(nullptr, pPlan);
      int32_t exempt = 0, userScans = 0;
      checkLicenseExemptSubplans(pPlan, sql, &exempt, &userScans);
      if (hasSysScan) {
        EXPECT_GT(exempt, 0) << sql;
      } else {
        EXPECT_EQ(0, exempt) << sql;
      }
      EXPECT_GT(userScans, 0) << sql;
    });
  }
}
