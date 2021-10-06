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

/**
 * Optimize the query execution plan, currently not implement yet.
 * @param pQueryNode
 * @return
 */
int32_t qOptimizeQueryPlan(SQueryNode* pQueryNode);

/**
 * Create the query plan according to the bound AST, which is in the form of pQueryInfo
 * @param pQueryInfo
 * @param pQueryNode
 * @return
 */
int32_t qCreateQueryPlan(const SQueryInfo* pQueryInfo, SQueryNode* pQueryNode);

/**
 * Convert the query plan to string, in order to display it in the shell.
 * @param pQueryNode
 * @return
 */
int32_t qQueryPlanToString(SQueryNode* pQueryNode, char** str);

/**
 * Restore the SQL statement according to the logic query plan.
 * @param pQueryNode
 * @param sql
 * @return
 */
int32_t qQueryPlanToSql(SQueryNode* pQueryNode, char** sql);

/**
 * Create the physical plan for the query, according to the logic plan.
 * @param pQueryNode
 * @param pPhyNode
 * @return
 */
int32_t qCreatePhysicalPlan(SQueryNode* pQueryNode, SEpSet* pQnode, SQueryPhyNode *pPhyNode);

/**
 * Convert to physical plan to string to enable to print it out in the shell.
 * @param pPhyNode
 * @param str
 * @return
 */
int32_t qPhyPlanToString(SQueryPhyNode *pPhyNode, char** str);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANNER_H_*/