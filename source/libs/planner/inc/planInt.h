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

#ifndef _TD_PLANNER_IMPL_H_
#define _TD_PLANNER_IMPL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "planner.h"
#include "taoserror.h"

#define QUERY_POLICY_VNODE  1
#define QUERY_POLICY_HYBRID 2
#define QUERY_POLICY_QNODE  3

#define planFatal(param, ...)  qFatal("PLAN: " param, __VA_ARGS__)
#define planError(param, ...)  qError("PLAN: " param, __VA_ARGS__)
#define planWarn(param, ...)   qWarn("PLAN: " param, __VA_ARGS__)
#define planInfo(param, ...)   qInfo("PLAN: " param, __VA_ARGS__)
#define planDebug(param, ...)  qDebug("PLAN: " param, __VA_ARGS__)
#define planDebugL(param, ...) qDebugL("PLAN: " param, __VA_ARGS__)
#define planTrace(param, ...)  qTrace("PLAN: " param, __VA_ARGS__)

int32_t generateUsageErrMsg(char* pBuf, int32_t len, int32_t errCode, ...);

int32_t createLogicPlan(SPlanContext* pCxt, SLogicNode** pLogicNode);
int32_t optimizeLogicPlan(SPlanContext* pCxt, SLogicNode* pLogicNode);
int32_t splitLogicPlan(SPlanContext* pCxt, SLogicNode* pLogicNode, SLogicSubplan** pLogicSubplan);
int32_t scaleOutLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SQueryLogicPlan** pLogicPlan);
int32_t createPhysiPlan(SPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPlan, SArray* pExecNodeList);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANNER_IMPL_H_*/
