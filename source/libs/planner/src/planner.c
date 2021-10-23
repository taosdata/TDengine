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

#include "os.h"
#include "plannerInt.h"
#include "parser.h"

int32_t qOptimizeQueryPlan(struct SQueryNode* pQueryNode) {
  return 0;
}

int32_t qCreateQueryPlan(const struct SQueryStmtInfo* pQueryInfo, struct SQueryNode* pQueryNode) {
  return 0;
}

int32_t qQueryPlanToString(struct SQueryNode* pQueryNode, char** str) {
  return 0;
}

int32_t qQueryPlanToSql(struct SQueryNode* pQueryNode, char** sql) {
  return 0;
}

int32_t qCreatePhysicalPlan(struct SQueryNode* pQueryNode, struct SEpSet* pQnode, struct SQueryDistPlanNode *pPhyNode) {
  return 0;
}

int32_t qPhyPlanToString(struct SQueryDistPlanNode *pPhyNode, char** str) {
  return 0;
}

void* qDestroyQueryPlan(struct SQueryNode* pQueryNode) {
  return NULL;
}

void* qDestroyQueryPhyPlan(struct SQueryDistPlanNode* pQueryPhyNode) {
  return NULL;
}

int32_t qCreateQueryJob(const struct SQueryDistPlanNode* pPhyNode, struct SQueryJob** pJob) {
  return 0;
}