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
#include "tutil.h"

static int32_t debugPrintNode(SNode* pNode) {
  char*   pStr = NULL;
  int32_t code = nodesNodeToString(pNode, false, &pStr, NULL);
  if (TSDB_CODE_SUCCESS == code) {
    (void)printf("%s\n", pStr);
    taosMemoryFree(pStr);
  }
  return code;
}

static int32_t dumpQueryPlan(SQueryPlan* pPlan) {
  int32_t code = 0;
  if (!(qDebugFlag & DEBUG_DEBUG)) {
    return code;
  }

  char* pStr = NULL;
  code = nodesNodeToString((SNode*)pPlan, false, &pStr, NULL);
  if (TSDB_CODE_SUCCESS == code) {
    planDebugL("QID:0x%" PRIx64 ", Query Plan, JsonPlan: %s", pPlan->queryId, pStr);
    taosMemoryFree(pStr);
  }
  return code;
}

static void printPlanNode(SLogicNode *pNode, int32_t level) {
  // print pnode and it's child for each level
  const char *nodename = nodesNodeName(nodeType((pNode)));
  for (int32_t i = 0; i < level; i++) {
    printf("    ");
  }
  printf("%s\n", nodename);
  SNode *tmp = NULL;
  FOREACH(tmp, pNode->pChildren) {
    printPlanNode((SLogicNode *)tmp, level + 1);
  }
  return;
}

static void dumpLogicPlan(SLogicSubplan* pLogicSubplan, int32_t level) {
  for (int32_t i = 0; i < level; i++) {
    printf("    ");
  }
  printf("Sub Plan:\n");
  if (pLogicSubplan->pNode) {
     printPlanNode(pLogicSubplan->pNode, level);
  }
  SNode *pNode = NULL;
  FOREACH(pNode, pLogicSubplan->pChildren) {
    dumpLogicPlan((SLogicSubplan *)pNode, level + 2);
  }
  return;
}

static void initSubQueryPlanContext(SPlanContext* pDst, SPlanContext* pSrc, SNode* pRoot) {
  memcpy(pDst, pSrc, sizeof(*pSrc));

  pDst->groupId++;
  pDst->streamCxt.hasExtWindow = false;
  pDst->hasScan = false;
  
  pDst->pAstRoot = pRoot;
}

static bool isPartitionedExternalWindowSubquery(SNode* pRoot, int32_t subQIdx) {
  if (pRoot == NULL || subQIdx < 0) {
    return false;
  }

  if (QUERY_NODE_SELECT_STMT != nodeType(pRoot)) {
    return false;
  }

  SSelectStmt* pSelect = (SSelectStmt*)pRoot;
  if (pSelect->pWindow == NULL || QUERY_NODE_EXTERNAL_WINDOW != nodeType(pSelect->pWindow)) {
    return false;
  }

  SExternalWindowNode* pExternal = (SExternalWindowNode*)pSelect->pWindow;
  if (pSelect->pPartitionByList == NULL || pExternal->pSubquery == NULL ||
      QUERY_NODE_REMOTE_TABLE != nodeType(pExternal->pSubquery)) {
    return false;
  }

  return ((SRemoteTableNode*)pExternal->pSubquery)->subQIdx == subQIdx;
}

static int32_t createSubQueryPlans(SPlanContext* pSrc, SQueryPlan* pParent, SArray* pExecNodeList) {
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;
  SPlanContext ctx;
  SNodeList* pSubQueries = NULL;
  SNode* pNode = NULL;
  SQueryPlan* pPlan = NULL;
  SNode* pRoot = NULL;

  switch (nodeType(pSrc->pAstRoot)) {
    case QUERY_NODE_EXPLAIN_STMT:
      pRoot = ((SExplainStmt*)pSrc->pAstRoot)->pQuery;
      break;
    case QUERY_NODE_INSERT_STMT:
      pRoot = ((SInsertStmt*)pSrc->pAstRoot)->pQuery;
      break;
    default:
      pRoot = pSrc->pAstRoot;
      break;
  }
  
  switch (nodeType(pRoot)) {
    case QUERY_NODE_SELECT_STMT: {
      SSelectStmt* pSelect = (SSelectStmt*)pRoot;
      pSubQueries = pSelect->pSubQueries;
      break;
    }
    case QUERY_NODE_SET_OPERATOR: {
      SSetOperator* pSet = (SSetOperator*)pRoot;
      pSubQueries = pSet->pSubQueries;
      break;
    }
    default:
      return code;
  }

  int32_t subQIdx = 0;
  FOREACH(pNode, pSubQueries) {
    planDebug("QID:0x%" PRIx64 ", createSubQueryPlans subQIdx:%d, nodeType:%d", pSrc->queryId, subQIdx, nodeType(pNode));
    initSubQueryPlanContext(&ctx, pSrc, pNode);
    ctx.forceNoMergeDataBlock = ctx.forceNoMergeDataBlock || isPartitionedExternalWindowSubquery(pRoot, subQIdx);
    TAOS_CHECK_EXIT(qCreateQueryPlan(&ctx, &pPlan, pExecNodeList));
    TAOS_CHECK_EXIT(nodesListMakeStrictAppend(&pParent->pChildren, (SNode*)pPlan));
    pParent->numOfSubplans += pPlan->numOfSubplans;
    pPlan->subSql = nodesGetSubSql(pNode);
    nodesGetSubQType(pNode, (int32_t*)&pPlan->subQType);
    pSrc->groupId = ++ctx.groupId;
    ++subQIdx;
  }

_exit:

  return code;
}

int32_t qCreateQueryPlan(SPlanContext* pCxt, SQueryPlan** pPlan, SArray* pExecNodeList) {
  SLogicSubplan*   pLogicSubplan = NULL;
  SQueryLogicPlan* pLogicPlan = NULL;
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;

  TAOS_CHECK_EXIT(nodesAcquireAllocator(pCxt->allocatorId));
  TAOS_CHECK_EXIT(createLogicPlan(pCxt, &pLogicSubplan));
  TAOS_CHECK_EXIT(optimizeLogicPlan(pCxt, pLogicSubplan));
  TAOS_CHECK_EXIT(splitLogicPlan(pCxt, pLogicSubplan));
  TAOS_CHECK_EXIT(scaleOutLogicPlan(pCxt, pLogicSubplan, &pLogicPlan));
  TAOS_CHECK_EXIT(createPhysiPlan(pCxt, pLogicPlan, pPlan, pExecNodeList));
  TAOS_CHECK_EXIT(validateQueryPlan(pCxt, *pPlan));
  (void)nodesReleaseAllocator(pCxt->allocatorId);
  TAOS_CHECK_EXIT(createSubQueryPlans(pCxt, *pPlan, pExecNodeList));
  TAOS_CHECK_EXIT(dumpQueryPlan(*pPlan));

_exit:
  if (TSDB_CODE_SUCCESS != code) {
    (void)nodesReleaseAllocator(pCxt->allocatorId);
    planError("QID:0x%" PRIx64 ", qCreateQueryPlan failed at line:%d, code:%s", pCxt->queryId, lino, tstrerror(code));
  }
  nodesDestroyNode((SNode*)pLogicSubplan);
  nodesDestroyNode((SNode*)pLogicPlan);
  
  terrno = code;
  return code;
}

/** Add vgId to exchange's childrenVgIds if not present. */
static int32_t addVgIdToExchange(SExchangePhysiNode* pExchange, int32_t vgId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (NULL == pExchange->childrenVgIds) {
    pExchange->childrenVgIds = taosArrayInit(4, sizeof(int32_t));
    QUERY_CHECK_NULL(pExchange->childrenVgIds, code, lino, _end, terrno);
  }

  int32_t vgCnt = (int32_t)taosArrayGetSize(pExchange->childrenVgIds);
  for (int32_t i = 0; i < vgCnt; ++i) {
    const int32_t* pCurr = taosArrayGet(pExchange->childrenVgIds, i);
    if (pCurr && *pCurr == vgId) {
      return TSDB_CODE_SUCCESS;
    }
  }
  QUERY_CHECK_NULL(taosArrayPush(pExchange->childrenVgIds, &vgId), code, lino,
                   _end, terrno);

_end:
  if (TSDB_CODE_SUCCESS != code) {
    planError("%s, failed to add vgId to exchange at line %d, code:%d",
              __func__, lino, code);
  }
  return code;
}

static int32_t setSubplanExecutionNode(SPhysiNode* pNode, int32_t groupId,
                                       SDownstreamSourceNode* pSource) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == nodeType(pNode)) {
    SExchangePhysiNode* pExchange = (SExchangePhysiNode*)pNode;
    if (groupId >= pExchange->srcStartGroupId && groupId <= pExchange->srcEndGroupId) {
      code = addVgIdToExchange(pExchange, pSource->addr.nodeId);
      if (TSDB_CODE_SUCCESS != code) {
        planError("%s, failed to add vgId to exchange at line %d, code:%d",
                  __func__, __LINE__, code);
        return code;
      }

      SNode* pNew = NULL;
      code = nodesCloneNode((SNode*)pSource, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        return nodesListMakeStrictAppend(&pExchange->pSrcEndPoints, pNew);
      }
    }
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE == nodeType(pNode)) {
    SMergePhysiNode* pMerge = (SMergePhysiNode*)pNode;
    if (pMerge->srcGroupId <= groupId && pMerge->srcEndGroupId >= groupId) {
      SExchangePhysiNode* pMergeExchange =
          (SExchangePhysiNode*)nodesListGetNode(pMerge->node.pChildren, pMerge->numOfChannels - 1);
      if (1 == pMerge->numOfChannels) {
        pMerge->numOfChannels = LIST_LENGTH(pMerge->node.pChildren);
      } else {
        --(pMerge->numOfChannels);
      }
      code = addVgIdToExchange(pMergeExchange, pSource->addr.nodeId);
      if (TSDB_CODE_SUCCESS != code) {
        planError("%s, failed to add vgId to merge exchange at line %d, code:%d",
                  __func__, __LINE__, code);
        return code;
      }

      SNode* pNew = NULL;
      code = nodesCloneNode((SNode*)pSource, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        return nodesListMakeStrictAppend(&pMergeExchange->pSrcEndPoints, pNew);
      }
    }
  }

  SNode* pChild = NULL;
  if (TSDB_CODE_SUCCESS == code) {
  FOREACH(pChild, pNode->pChildren) {
      if (TSDB_CODE_SUCCESS != (code = setSubplanExecutionNode((SPhysiNode*)pChild, groupId, pSource))) {
        return code;
      }
    }
  }
  return code;
}

int32_t qContinuePlanPostQuery(void* pPostPlan) {
  // TODO
  return TSDB_CODE_SUCCESS;
}

int32_t qSetSubplanExecutionNode(SSubplan* subplan, int32_t groupId, SDownstreamSourceNode* pSource) {
  planDebug("QID:0x%" PRIx64 ", set subplan execution node, groupId:%d", subplan->id.queryId, groupId);
  return setSubplanExecutionNode(subplan->pNode, groupId, pSource);
}

static void clearSubplanExecutionNode(SPhysiNode* pNode) {
  if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == nodeType(pNode)) {
    SExchangePhysiNode* pExchange = (SExchangePhysiNode*)pNode;
    NODES_DESTORY_LIST(pExchange->pSrcEndPoints);
    taosArrayDestroy(pExchange->childrenVgIds);
    pExchange->childrenVgIds = NULL;
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE == nodeType(pNode)) {
    SMergePhysiNode* pMerge = (SMergePhysiNode*)pNode;
    pMerge->numOfChannels = LIST_LENGTH(pMerge->node.pChildren);
    SNode* pChild = NULL;
    FOREACH(pChild, pMerge->node.pChildren) {
      NODES_DESTORY_LIST(((SExchangePhysiNode*)pChild)->pSrcEndPoints);
      taosArrayDestroy(((SExchangePhysiNode*)pChild)->childrenVgIds);
      ((SExchangePhysiNode*)pChild)->childrenVgIds = NULL;
    }
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) { clearSubplanExecutionNode((SPhysiNode*)pChild); }
}

void qClearSubplanExecutionNode(SSubplan* pSubplan) {
  planDebug("QID:0x%" PRIx64 ", clear subplan execution node, groupId:%d", pSubplan->id.queryId, pSubplan->id.groupId);
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
  if (NULL == pSubplan) {
    return terrno = TSDB_CODE_INVALID_PARA;
  }
  
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
