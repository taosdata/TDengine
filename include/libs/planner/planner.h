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
#include "taos.h"

typedef struct SPlanContext {
  uint64_t    queryId;
  int32_t     acctId;
  SEpSet      mgmtEpSet;
  SNode*      pAstRoot;
  bool        topicQuery;
  bool        streamQuery;
  bool        rSmaQuery;
  bool        showRewrite;
  bool        isView;
  bool        isAudit;
  int8_t      triggerType;
  int64_t     watermark;
  int64_t     deleteMark;
  int8_t      igExpired;
  int8_t      igCheckUpdate;
  char*       pMsg;
  int32_t     msgLen;
  const char* pUser;
  bool        sysInfo;
  int64_t     allocatorId;
} SPlanContext;

// Create the physical plan for the query, according to the AST.
int32_t qCreateQueryPlan(SPlanContext* pCxt, SQueryPlan** pPlan, SArray* pExecNodeList);

// Set datasource of this subplan, multiple calls may be made to a subplan.
// @pSubplan subplan to be schedule
// @groupId id of a group of datasource subplans of this @pSubplan
// @pSource one execution location of this group of datasource subplans
int32_t qSetSubplanExecutionNode(SSubplan* pSubplan, int32_t groupId, SDownstreamSourceNode* pSource);
int32_t qContinuePlanPostQuery(void *pPostPlan);

void qClearSubplanExecutionNode(SSubplan* pSubplan);

// Convert to subplan to display string for the scheduler to send to the executor
int32_t qSubPlanToString(const SSubplan* pSubplan, char** pStr, int32_t* pLen);
int32_t qStringToSubplan(const char* pStr, SSubplan** pSubplan);

// Convert to subplan to msg for the scheduler to send to the executor
int32_t qSubPlanToMsg(const SSubplan* pSubplan, char** pStr, int32_t* pLen);
int32_t qMsgToSubplan(const char* pStr, int32_t len, SSubplan** pSubplan);

SQueryPlan* qStringToQueryPlan(const char* pStr);

void qDestroyQueryPlan(SQueryPlan* pPlan);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANNER_H_*/
