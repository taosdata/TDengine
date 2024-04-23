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
#include "scalar.h"
#include "tglobal.h"

static void debugPrintNode(SNode* pNode) {
  char* pStr = NULL;
  nodesNodeToString(pNode, false, &pStr, NULL);
  printf("%s\n", pStr);
  taosMemoryFree(pStr);
  return;
}

static void dumpQueryPlan(SQueryPlan* pPlan) {
  if (!tsQueryPlannerTrace) {
    return;
  }
  char* pStr = NULL;
  nodesNodeToString((SNode*)pPlan, false, &pStr, NULL);
  planDebugL("QID:0x%" PRIx64 " Query Plan, JsonPlan: %s", pPlan->queryId, pStr);
  taosMemoryFree(pStr);
}

int32_t qCreateQueryPlan(SPlanContext* pCxt, SQueryPlan** pPlan, SArray* pExecNodeList) {
  SLogicSubplan*   pLogicSubplan = NULL;
  SQueryLogicPlan* pLogicPlan = NULL;

  int32_t code = nodesAcquireAllocator(pCxt->allocatorId);
  if (TSDB_CODE_SUCCESS == code) {
    code = createLogicPlan(pCxt, &pLogicSubplan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = optimizeLogicPlan(pCxt, pLogicSubplan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = splitLogicPlan(pCxt, pLogicSubplan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = scaleOutLogicPlan(pCxt, pLogicSubplan, &pLogicPlan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createPhysiPlan(pCxt, pLogicPlan, pPlan, pExecNodeList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = validateQueryPlan(pCxt, *pPlan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    dumpQueryPlan(*pPlan);
  }
  nodesReleaseAllocator(pCxt->allocatorId);

  nodesDestroyNode((SNode*)pLogicSubplan);
  nodesDestroyNode((SNode*)pLogicPlan);
  terrno = code;
  return code;
}

static int32_t setSubplanExecutionNode(SPhysiNode* pNode, int32_t groupId, SDownstreamSourceNode* pSource) {
  if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == nodeType(pNode)) {
    SExchangePhysiNode* pExchange = (SExchangePhysiNode*)pNode;
    if (groupId >= pExchange->srcStartGroupId && groupId <= pExchange->srcEndGroupId) {
      return nodesListMakeStrictAppend(&pExchange->pSrcEndPoints, nodesCloneNode((SNode*)pSource));
    }
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE == nodeType(pNode)) {
    SMergePhysiNode* pMerge = (SMergePhysiNode*)pNode;
    if (pMerge->srcGroupId == groupId) {
      SExchangePhysiNode* pExchange =
          (SExchangePhysiNode*)nodesListGetNode(pMerge->node.pChildren, pMerge->numOfChannels - 1);
      if (1 == pMerge->numOfChannels) {
        pMerge->numOfChannels = LIST_LENGTH(pMerge->node.pChildren);
      } else {
        --(pMerge->numOfChannels);
      }
      return nodesListMakeStrictAppend(&pExchange->pSrcEndPoints, nodesCloneNode((SNode*)pSource));
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

int32_t qContinuePlanPostQuery(void *pPostPlan) {
  //TODO
  return TSDB_CODE_SUCCESS;
}


int32_t qSetSubplanExecutionNode(SSubplan* subplan, int32_t groupId, SDownstreamSourceNode* pSource) {
  planDebug("QID:0x%" PRIx64 " set subplan execution node, groupId:%d", subplan->id.queryId, groupId);
  return setSubplanExecutionNode(subplan->pNode, groupId, pSource);
}

static void clearSubplanExecutionNode(SPhysiNode* pNode) {
  if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == nodeType(pNode)) {
    SExchangePhysiNode* pExchange = (SExchangePhysiNode*)pNode;
    NODES_DESTORY_LIST(pExchange->pSrcEndPoints);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE == nodeType(pNode)) {
    SMergePhysiNode* pMerge = (SMergePhysiNode*)pNode;
    pMerge->numOfChannels = LIST_LENGTH(pMerge->node.pChildren);
    SNode* pChild = NULL;
    FOREACH(pChild, pMerge->node.pChildren) { NODES_DESTORY_LIST(((SExchangePhysiNode*)pChild)->pSrcEndPoints); }
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) { clearSubplanExecutionNode((SPhysiNode*)pChild); }
}

void qClearSubplanExecutionNode(SSubplan* pSubplan) {
  planDebug("QID:0x%" PRIx64 " clear subplan execution node, groupId:%d", pSubplan->id.queryId, pSubplan->id.groupId);
  clearSubplanExecutionNode(pSubplan->pNode);
}

int32_t qSubPlanToString(const SSubplan* pSubplan, char** pStr, int32_t* pLen) {
  if (SUBPLAN_TYPE_MODIFY == pSubplan->subplanType && NULL == pSubplan->pNode) {
    SDataInserterNode* insert = (SDataInserterNode*)pSubplan->pDataSink;
    *pLen = insert->size;
    *pStr = insert->pData;
    insert->pData = NULL;
    return TSDB_CODE_SUCCESS;
  }
  return nodesNodeToString((const SNode*)pSubplan, false, pStr, pLen);
}

int32_t qStringToSubplan(const char* pStr, SSubplan** pSubplan) { return nodesStringToNode(pStr, (SNode**)pSubplan); }

int32_t qSubPlanToMsg(const SSubplan* pSubplan, char** pStr, int32_t* pLen) {
  if (SUBPLAN_TYPE_MODIFY == pSubplan->subplanType && NULL == pSubplan->pNode) {
    SDataInserterNode* insert = (SDataInserterNode*)pSubplan->pDataSink;
    *pLen = insert->size;
    *pStr = insert->pData;
    insert->pData = NULL;
    return TSDB_CODE_SUCCESS;
  }
  return nodesNodeToMsg((const SNode*)pSubplan, pStr, pLen);
}

int32_t qMsgToSubplan(const char* pStr, int32_t len, SSubplan** pSubplan) {
  return nodesMsgToNode(pStr, len, (SNode**)pSubplan);
}

SQueryPlan* qStringToQueryPlan(const char* pStr) {
  SQueryPlan* pPlan = NULL;
  if (TSDB_CODE_SUCCESS != nodesStringToNode(pStr, (SNode**)&pPlan)) {
    return NULL;
  }
  return pPlan;
}

void qDestroyQueryPlan(SQueryPlan* pPlan) { nodesDestroyNode((SNode*)pPlan); }
