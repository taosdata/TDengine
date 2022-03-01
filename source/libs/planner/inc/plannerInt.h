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

#include "tcommon.h"
#include "tarray.h"
#include "planner.h"
#include "parser.h"
#include "tmsg.h"

#define QNODE_TAGSCAN       1
#define QNODE_TABLESCAN     2
#define QNODE_STREAMSCAN    3
#define QNODE_PROJECT       4
#define QNODE_AGGREGATE     5
#define QNODE_GROUPBY       6
#define QNODE_LIMIT         7
#define QNODE_JOIN          8
#define QNODE_DISTINCT      9
#define QNODE_SORT          10
#define QNODE_UNION         11
#define QNODE_TIMEWINDOW    12
#define QNODE_SESSIONWINDOW 13
#define QNODE_STATEWINDOW   14
#define QNODE_FILL          15
#define QNODE_MODIFY        16

typedef struct SQueryDistPlanNodeInfo {
  bool      stableQuery;   // super table query or not
  int32_t   phase;         // merge|partial
  int32_t   type;          // operator type
  char     *name;          // operator name
  SEpSet   *sourceEp;      // data source epset
} SQueryDistPlanNodeInfo;

typedef struct SQueryTableInfo {
  char           *tableName; // to be deleted
  uint64_t        uid;       // to be deleted
  STableMetaInfo *pMeta;
  STimeWindow     window;
} SQueryTableInfo;

typedef struct SQueryPlanNode {
  SQueryNodeBasicInfo info;
  SSchema            *pSchema;      // the schema of the input SSDatablock
  int32_t             numOfCols;    // number of input columns
  SArray             *pExpr;        // the query functions or sql aggregations
  int32_t             numOfExpr;    // number of result columns, which is also the number of pExprs
  void               *pExtInfo;     // additional information
  // children operator to generated result for current node to process
  // in case of join, multiple prev nodes exist.
  SArray             *pChildren;   // upstream nodes
  struct SQueryPlanNode  *pParent;
} SQueryPlanNode;

typedef struct SDataPayloadInfo {
  int32_t msgType;
  SArray *payload;
} SDataPayloadInfo;

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
int32_t createQueryPlan(const SQueryNode* pNode, struct SQueryPlanNode** pQueryPlan);

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

int32_t createDag(SQueryPlanNode* pQueryNode, struct SCatalog* pCatalog, SQueryDag** pDag, SArray* pNodeList, uint64_t requestId);
void setSubplanExecutionNode(SSubplan* subplan, uint64_t templateId, SDownstreamSource* pSource);
int32_t subPlanToString(const SSubplan *pPhyNode, char** str, int32_t* len);
int32_t stringToSubplan(const char* str, SSubplan** subplan);

/**
 * Destroy the query plan object.
 * @return
 */
void destroyQueryPlan(struct SQueryPlanNode* pQueryNode);

/**
 * Destroy the physical plan.
 * @param pQueryPhyNode
 * @return
 */
void* destroyQueryPhyPlan(struct SPhyNode* pQueryPhyNode);

const char* opTypeToOpName(int32_t type);
int32_t opNameToOpType(const char* name);

const char* dsinkTypeToDsinkName(int32_t type);
int32_t dsinkNameToDsinkType(const char* name);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANNER_INT_H_*/