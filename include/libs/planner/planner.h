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

#ifndef _TD_PLANNER_H_
#define _TD_PLANNER_H_

#ifdef __cplusplus
extern "C" {
#endif

#define QUERY_TYPE_MERGE       1
#define QUERY_TYPE_PARTIAL     2

struct SEpSet;
struct SQueryNode;
struct SQueryPhyNode;
struct SQueryStmtInfo;

typedef struct SSubquery {
  int64_t   queryId;            // the subquery id created by qnode
  int32_t   type;               // QUERY_TYPE_MERGE|QUERY_TYPE_PARTIAL
  int32_t   level;              // the execution level of current subquery, starting from 0.
  SArray   *pUpstream;          // the upstream,from which to fetch the result
  struct SQueryPhyNode *pNode;  // physical plan of current subquery
} SSubquery;

typedef struct SQueryJob {
  SArray  **pSubqueries;
  int32_t   numOfLevels;
  int32_t   currentLevel;
} SQueryJob;


/**
 * Optimize the query execution plan, currently not implement yet.
 * @param pQueryNode
 * @return
 */
int32_t qOptimizeQueryPlan(struct SQueryNode* pQueryNode);

/**
 * Create the query plan according to the bound AST, which is in the form of pQueryInfo
 * @param pQueryInfo
 * @param pQueryNode
 * @return
 */
int32_t qCreateQueryPlan(const struct SQueryStmtInfo* pQueryInfo, struct SQueryNode* pQueryNode);

/**
 * Convert the query plan to string, in order to display it in the shell.
 * @param pQueryNode
 * @return
 */
int32_t qQueryPlanToString(struct SQueryNode* pQueryNode, char** str);

/**
 * Restore the SQL statement according to the logic query plan.
 * @param pQueryNode
 * @param sql
 * @return
 */
int32_t qQueryPlanToSql(struct SQueryNode* pQueryNode, char** sql);

/**
 * Create the physical plan for the query, according to the logic plan.
 * @param pQueryNode
 * @param pPhyNode
 * @return
 */
int32_t qCreatePhysicalPlan(struct SQueryNode* pQueryNode, struct SEpSet* pQnode, struct SQueryPhyNode *pPhyNode);

/**
 * Convert to physical plan to string to enable to print it out in the shell.
 * @param pPhyNode
 * @param str
 * @return
 */
int32_t qPhyPlanToString(struct SQueryPhyNode *pPhyNode, char** str);

/**
 * Destroy the query plan object.
 * @return
 */
void* qDestroyQueryPlan(struct SQueryNode* pQueryNode);

/**
 * Destroy the physical plan.
 * @param pQueryPhyNode
 * @return
 */
void* qDestroyQueryPhyPlan(struct SQueryPhyNode* pQueryPhyNode);

/**
 * Create the query job from the physical execution plan
 * @param pPhyNode
 * @param pJob
 * @return
 */
int32_t qCreateQueryJob(const struct SQueryPhyNode* pPhyNode, struct SQueryJob** pJob);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANNER_H_*/