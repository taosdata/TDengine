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

#include "planner.h"

#include "planInt.h"

int32_t qCreateQueryPlan(SPlanContext* pCxt, SQueryPlan** pPlan, SArray* pExecNodeList) {
  SLogicNode* pLogicNode = NULL;
  SLogicSubplan* pLogicSubplan = NULL;
  SQueryLogicPlan* pLogicPlan = NULL;

  int32_t code = createLogicPlan(pCxt, &pLogicNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = optimizeLogicPlan(pCxt, pLogicNode);
  }  
  if (TSDB_CODE_SUCCESS == code) {
    code = splitLogicPlan(pCxt, pLogicNode, &pLogicSubplan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = scaleOutLogicPlan(pCxt, pLogicSubplan, &pLogicPlan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createPhysiPlan(pCxt, pLogicPlan, pPlan, pExecNodeList);
  }

  nodesDestroyNode(pLogicNode);
  nodesDestroyNode(pLogicSubplan);
  nodesDestroyNode(pLogicPlan);
  terrno = code;
  return code;
}

static int32_t setSubplanExecutionNode(SPhysiNode* pNode, int32_t groupId, SDownstreamSourceNode* pSource) {
  if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == nodeType(pNode)) {
    SExchangePhysiNode* pExchange = (SExchangePhysiNode*)pNode;
    if (pExchange->srcGroupId == groupId) {
      if (NULL == pExchange->pSrcEndPoints) {
        pExchange->pSrcEndPoints = nodesMakeList();
        if (NULL == pExchange->pSrcEndPoints) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      }
      if (TSDB_CODE_SUCCESS != nodesListStrictAppend(pExchange->pSrcEndPoints, nodesCloneNode(pSource))) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      return TSDB_CODE_SUCCESS;
    }
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    if (TSDB_CODE_SUCCESS != setSubplanExecutionNode((SPhysiNode*)pChild, groupId, pSource)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t qSetSubplanExecutionNode(SSubplan* subplan, int32_t groupId, SDownstreamSourceNode* pSource) {
  return setSubplanExecutionNode(subplan->pNode, groupId, pSource);
}

int32_t qSubPlanToString(const SSubplan* pSubplan, char** pStr, int32_t* pLen) {
  if (SUBPLAN_TYPE_MODIFY == pSubplan->subplanType) {
    SDataInserterNode* insert = (SDataInserterNode*)pSubplan->pDataSink;
    *pLen = insert->size;
    *pStr = insert->pData;
    insert->pData = NULL;
    return TSDB_CODE_SUCCESS;
  }
  return nodesNodeToString((const SNode*)pSubplan, false, pStr, pLen);
}

int32_t qStringToSubplan(const char* pStr, SSubplan** pSubplan) {
  return nodesStringToNode(pStr, (SNode**)pSubplan);
}

char* qQueryPlanToString(const SQueryPlan* pPlan) {
  char* pStr = NULL;
  int32_t len = 0;
  if (TSDB_CODE_SUCCESS != nodesNodeToString(pPlan, false, &pStr, &len)) {
    return NULL;
  }
  return pStr;
}

SQueryPlan* qStringToQueryPlan(const char* pStr) {
  SQueryPlan* pPlan = NULL;
  if (TSDB_CODE_SUCCESS != nodesStringToNode(pStr, (SNode**)&pPlan)) {
    return NULL;
  }
  return pPlan;
}

void qDestroyQueryPlan(SQueryPlan* pPlan) {
  nodesDestroyNode(pPlan);
}
