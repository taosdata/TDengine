/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef _TD_PLANNER_INT_H_
#define _TD_PLANNER_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "common.h"
#include "tarray.h"
#include "planner.h"
#include "taosmsg.h"

enum LOGIC_PLAN_E {
  LP_SCAN     = 1,
  LP_SESSION  = 2,
  LP_STATE    = 3,
  LP_INTERVAL = 4,
  LP_FILL     = 5,
  LP_AGG      = 6,
  LP_JOIN     = 7,
  LP_PROJECT  = 8,
  LP_DISTINCT = 9,
  LP_ORDER    = 10
};

typedef struct SQueryNodeBasicInfo {
  int32_t   type;          // operator type
  char     *name;          // operator name
} SQueryNodeBasicInfo;

typedef struct SQueryDistPlanNodeInfo {
  bool      stableQuery;   // super table query or not
  int32_t   phase;         // merge|partial
  int32_t   type;          // operator type
  char     *name;          // operator name
  SEpSet   *sourceEp;      // data source epset
} SQueryDistPlanNodeInfo;

typedef struct SQueryTableInfo {
  char       *tableName;
  uint64_t    uid;
  STimeWindow window;
} SQueryTableInfo;

typedef struct SQueryPlanNode {
  SQueryNodeBasicInfo info;
  SSchema            *pSchema;      // the schema of the input SSDatablock
  int32_t             numOfCols;    // number of input columns
  SArray             *pExpr;        // the query functions or sql aggregations
  int32_t             numOfExpr;  // number of result columns, which is also the number of pExprs
  void               *pExtInfo;     // additional information
  // previous operator to generated result for current node to process
  // in case of join, multiple prev nodes exist.
  SArray             *pPrevNodes;   // upstream nodes
  struct SQueryPlanNode  *nextNode;
} SQueryPlanNode;

typedef SSchema SSlotSchema;

typedef struct SDataBlockSchema {
  int32_t             index;
  SSlotSchema        *pSchema;
  int32_t             numOfCols;    // number of columns
} SDataBlockSchema;

typedef struct SPhyNode {
  SQueryNodeBasicInfo info;
  SArray             *pTargets;      // target list to be computed or scanned at this node
  SArray             *pConditions;         // implicitly-ANDed qual conditions
  SDataBlockSchema    targetSchema;
  // children plan to generated result for current node to process
  // in case of join, multiple plan nodes exist.
  SArray             *pChildren;
} SPhyNode;

typedef struct SScanPhyNode {
  SPhyNode node;
  uint64_t     uid;  // unique id of the table
} SScanPhyNode;

typedef SScanPhyNode STagScanPhyNode;

typedef SScanPhyNode SSystemTableScanPhyNode;

typedef struct SMultiTableScanPhyNode {
  SScanPhyNode scan;
  SArray      *pTagsConditions; // implicitly-ANDed tag qual conditions
} SMultiTableScanPhyNode;

typedef SMultiTableScanPhyNode SMultiTableSeqScanPhyNode;

typedef struct SProjectPhyNode {
  SPhyNode node;
} SProjectPhyNode;

/**
 * Optimize the query execution plan, currently not implement yet.
 * @param pQueryNode
 * @return
 */
int32_t optimizeQueryPlan(struct SQueryPlanNode* pQueryNode);

/**
 * Create the query plan according to the bound AST, which is in the form of pQueryInfo
 * @param pQueryInfo
 * @param pQueryNode
 * @return
 */
int32_t createQueryPlan(const struct SQueryStmtInfo* pQueryInfo, struct SQueryPlanNode** pQueryNode);

/**
 * Convert the query plan to string, in order to display it in the shell.
 * @param pQueryNode
 * @return
 */
int32_t queryPlanToString(struct SQueryPlanNode* pQueryNode, char** str);

/**
 * Restore the SQL statement according to the logic query plan.
 * @param pQueryNode
 * @param sql
 * @return
 */
int32_t queryPlanToSql(struct SQueryPlanNode* pQueryNode, char** sql);

/**
 * Convert to physical plan to string to enable to print it out in the shell.
 * @param pPhyNode
 * @param str
 * @return
 */
int32_t phyPlanToString(struct SPhyNode *pPhyNode, char** str);

/**
 * Destroy the query plan object.
 * @return
 */
void* destroyQueryPlan(struct SQueryPlanNode* pQueryNode);

/**
 * Destroy the physical plan.
 * @param pQueryPhyNode
 * @return
 */
void* destroyQueryPhyPlan(struct SPhyNode* pQueryPhyNode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANNER_INT_H_*/