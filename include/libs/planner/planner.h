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

#include "plannodes.h"

typedef struct SPlanContext {
  uint64_t queryId;
  SNode* pAstRoot;
} SPlanContext;

// Create the physical plan for the query, according to the AST.
int32_t qCreateQueryPlan(SPlanContext* pCxt, SQueryPlan** pPlan, SArray* pExecNodeList);

// Set datasource of this subplan, multiple calls may be made to a subplan.
// @subplan subplan to be schedule
// @groupId id of a group of datasource subplans of this @subplan
// @ep one execution location of this group of datasource subplans 
void qSetSubplanExecutionNode(SSubplan* subplan, int32_t groupId, SDownstreamSourceNode* pSource);

// Convert to subplan to string for the scheduler to send to the executor
int32_t qSubPlanToString(const SSubplan* subplan, char** str, int32_t* len);
int32_t qStringToSubplan(const char* str, SSubplan** subplan);

char* qQueryPlanToString(const SQueryPlan* pPlan);
SQueryPlan* qStringToQueryPlan(const char* pStr);

void qDestroyQueryPlan(SQueryPlan* pPlan);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANNER_H_*/
