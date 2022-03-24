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

#define CHECK_ALLOC(p, res) \
  do { \
    if (NULL == (p)) { \
      pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY; \
      return (res); \
    } \
  } while (0)

#define CHECK_CODE(exec, res) \
  do { \
    int32_t code = (exec); \
    if (TSDB_CODE_SUCCESS != code) { \
      pCxt->errCode = code; \
      return (res); \
    } \
  } while (0)

#define CHECK_CODE_EXT(exec) \
  do { \
    int32_t code = (exec); \
    if (TSDB_CODE_SUCCESS != code) { \
      pCxt->errCode = code; \
      return code; \
    } \
  } while (0)

#define planFatal(param, ...)  qFatal("PLAN: " param, __VA_ARGS__)
#define planError(param, ...)  qError("PLAN: " param, __VA_ARGS__)
#define planWarn(param, ...)   qWarn("PLAN: " param, __VA_ARGS__)
#define planInfo(param, ...)   qInfo("PLAN: " param, __VA_ARGS__)
#define planDebug(param, ...)  qDebug("PLAN: " param, __VA_ARGS__)
#define planTrace(param, ...)  qTrace("PLAN: " param, __VA_ARGS__)

int32_t createLogicPlan(SPlanContext* pCxt, SLogicNode** pLogicNode);
int32_t optimizeLogicPlan(SPlanContext* pCxt, SLogicNode* pLogicNode);
int32_t splitLogicPlan(SPlanContext* pCxt, SLogicNode* pLogicNode, SLogicSubplan** pLogicSubplan);
int32_t scaleOutLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SQueryLogicPlan** pLogicPlan);
int32_t createPhysiPlan(SPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPlan, SArray* pExecNodeList);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANNER_IMPL_H_*/
