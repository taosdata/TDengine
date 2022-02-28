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

#define QUERY_TYPE_MERGE       1
#define QUERY_TYPE_PARTIAL     2
#define QUERY_TYPE_SCAN        3
#define QUERY_TYPE_MODIFY      4

typedef struct SSubplanId {
  uint64_t queryId;
  uint64_t templateId;
  uint64_t subplanId;
} SSubplanId;

typedef struct SSubplan {
  SSubplanId id;           // unique id of the subplan
  int32_t    type;         // QUERY_TYPE_MERGE|QUERY_TYPE_PARTIAL|QUERY_TYPE_SCAN|QUERY_TYPE_MODIFY
  int32_t    msgType;      // message type for subplan, used to denote the send message type to vnode.
  int32_t    level;        // the execution level of current subplan, starting from 0 in a top-down manner.
  SQueryNodeAddr execNode;    // for the scan/modify subplan, the optional execution node
  SArray* pChildren;    // the datasource subplan,from which to fetch the result
  SArray* pParents;     // the data destination subplan, get data from current subplan
  SPhysiNode* pNode;        // physical plan of current subplan
  SDataSinkNode* pDataSink;    // data of the subplan flow into the datasink
} SSubplan;

typedef struct SQueryPlan {
  uint64_t queryId;
  int32_t  numOfSubplans;
  SArray* pSubplans; // SArray*<SArray*<SSubplan*>>. The execution level of subplan, starting from 0.
} SQueryPlan;

typedef struct SPlanContext {
  uint64_t queryId;
  SNode* pAstRoot;
} SPlanContext;

// Create the physical plan for the query, according to the AST.
int32_t qCreateQueryPlan(SPlanContext* pCxt, SQueryPlan** pPlan);

// Set datasource of this subplan, multiple calls may be made to a subplan.
// @subplan subplan to be schedule
// @templateId templateId of a group of datasource subplans of this @subplan
// @ep one execution location of this group of datasource subplans 
void qSetSubplanExecutionNode(SSubplan* subplan, uint64_t templateId, SDownstreamSource* pSource);

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
