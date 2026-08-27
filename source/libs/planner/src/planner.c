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

typedef struct SPlanQueryUnit {
  SPlanContext* pCtx;
  SPlanContext  localCtx;
  bool          ownsCtx;
  bool          isRoot;
  SNode*        pAstRoot;
  SLogicSubplan* pLogicSubplan;
  SQueryLogicPlan* pLogicPlan;
  SQueryPlan*     pPhysiPlan;
  int32_t         origSubQIdx;
  int32_t         finalSubQIdx;
} SPlanQueryUnit;

typedef struct SPlanSubqueryState {
  SNode*          pTopAstRoot;
  SNodeList*      pAllSubQueries;
  int32_t         numSubQueries;
  SPlanQueryUnit  rootUnit;
  SPlanQueryUnit** pSubqueryUnits;  // indexed by original global subQIdx
} SPlanSubqueryState;

typedef struct SPlanRemoteRewriteCtx {
  SPlanSubqueryState* pState;
  const int32_t*      pOldToNew;
  bool                insideRemotePlan;
  int32_t             code;
  uint64_t            queryId;
  int32_t             currentSubQIdx;
} SPlanRemoteRewriteCtx;

typedef struct SPlanRemoteMarkCtx {
  SPlanContext* pCxt;
  int32_t       code;
  int32_t       ownerSubQIdx;
} SPlanRemoteMarkCtx;

static SNode* planExtractQueryRoot(SNode* pAstRoot) {
  switch (nodeType(pAstRoot)) {
    case QUERY_NODE_EXPLAIN_STMT:
      return ((SExplainStmt*)pAstRoot)->pQuery;
    case QUERY_NODE_INSERT_STMT:
      return ((SInsertStmt*)pAstRoot)->pQuery;
    default:
      return pAstRoot;
  }
}

static SNodeList* planExtractSubQueries(SNode* pAstRoot) {
  SNode* pRoot = planExtractQueryRoot(pAstRoot);
  if (NULL == pRoot) {
    return NULL;
  }

  switch (nodeType(pRoot)) {
    case QUERY_NODE_SELECT_STMT:
      return ((SSelectStmt*)pRoot)->pSubQueries;
    case QUERY_NODE_SET_OPERATOR:
      return ((SSetOperator*)pRoot)->pSubQueries;
    default:
      return NULL;
  }
}

static bool isExternalWindowSubquery(SNode* pRoot, int32_t subQIdx);

static SPlanSubqueryState* planGetSubqueryStateImpl(const SPlanContext* pCxt) {
  return (NULL != pCxt) ? (SPlanSubqueryState*)pCxt->pSubqueryState : NULL;
}

static SPlanQueryUnit* planGetQueryUnitImpl(const SPlanContext* pCxt, int32_t subQIdx) {
  SPlanSubqueryState* pState = planGetSubqueryStateImpl(pCxt);
  if (NULL == pState || subQIdx < 0 || subQIdx >= pState->numSubQueries) {
    return NULL;
  }
  return pState->pSubqueryUnits[subQIdx];
}

static SLogicNode* planUnwrapQuantifyRenderLogic(SLogicNode* pRoot) {
  if (NULL == pRoot) {
    return NULL;
  }

  SLogicNode* pCurr = pRoot;
  if (QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pCurr) && pCurr->pChildren != NULL &&
      1 == LIST_LENGTH(pCurr->pChildren)) {
    pCurr = (SLogicNode*)nodesListGetNode(pCurr->pChildren, 0);
  }

  if (NULL == pCurr || QUERY_NODE_LOGIC_PLAN_AGG != nodeType(pCurr)) {
    return pRoot;
  }

  SAggLogicNode* pAgg = (SAggLogicNode*)pCurr;
  if (pAgg->pGroupKeys != NULL && LIST_LENGTH(pAgg->pGroupKeys) > 0) {
    return pRoot;
  }
  if (NULL == pAgg->pAggFuncs || LIST_LENGTH(pAgg->pAggFuncs) != 2 || NULL == pAgg->node.pChildren ||
      LIST_LENGTH(pAgg->node.pChildren) != 1) {
    return pRoot;
  }

  bool hasHasNull = false;
  bool hasMinOrMax = false;
  SNode* pFuncNode = NULL;
  FOREACH(pFuncNode, pAgg->pAggFuncs) {
    if (QUERY_NODE_FUNCTION != nodeType(pFuncNode)) {
      return pRoot;
    }
    const SFunctionNode* pFunc = (const SFunctionNode*)pFuncNode;
    if (0 == strcmp(pFunc->functionName, "_has_null")) {
      hasHasNull = true;
    } else if (0 == strcasecmp(pFunc->functionName, "min") || 0 == strcasecmp(pFunc->functionName, "max")) {
      hasMinOrMax = true;
    }
  }

  if (!hasHasNull || !hasMinOrMax) {
    return pRoot;
  }

  return (SLogicNode*)nodesListGetNode(pAgg->node.pChildren, 0);
}

SLogicNode* planGetSubqueryRenderLogic(const SPlanContext* pCxt, int32_t subQIdx, ENodeType remoteNodeType) {
  SPlanQueryUnit* pUnit = planGetQueryUnitImpl(pCxt, subQIdx);
  if (NULL == pUnit || NULL == pUnit->pLogicSubplan) {
    return NULL;
  }
  return (QUERY_NODE_REMOTE_ROW == remoteNodeType) ? planUnwrapQuantifyRenderLogic(pUnit->pLogicSubplan->pNode)
                                                   : pUnit->pLogicSubplan->pNode;
}

bool planIsSubqueryPushedDown(const SPlanContext* pCxt, int32_t subQIdx) {
  SPlanQueryUnit* pUnit = planGetQueryUnitImpl(pCxt, subQIdx);
  return (NULL != pUnit && NULL != pUnit->pLogicSubplan) ? pUnit->pLogicSubplan->pushedDown : false;
}

bool planIsExternalWindowSubquery(const SPlanContext* pCxt, int32_t subQIdx) {
  SPlanSubqueryState* pState = planGetSubqueryStateImpl(pCxt);
  return (NULL != pState) ? isExternalWindowSubquery(pState->pTopAstRoot, subQIdx) : false;
}

static bool planGetRemoteExprInfo(SNode* pNode, int32_t** ppSubQIdx, EFqSubqRenderMode** ppRenderMode,
                                  SNode*** pppRemotePlan) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_REMOTE_VALUE: {
      SRemoteValueNode* pRemote = (SRemoteValueNode*)pNode;
      *ppSubQIdx = &pRemote->subQIdx;
      *ppRenderMode = &pRemote->renderMode;
      *pppRemotePlan = &pRemote->pRemotePlan;
      return true;
    }
    case QUERY_NODE_REMOTE_VALUE_LIST: {
      SRemoteValueListNode* pRemote = (SRemoteValueListNode*)pNode;
      *ppSubQIdx = &pRemote->subQIdx;
      *ppRenderMode = &pRemote->renderMode;
      *pppRemotePlan = &pRemote->pRemotePlan;
      return true;
    }
    case QUERY_NODE_REMOTE_ROW: {
      SRemoteRowNode* pRemote = (SRemoteRowNode*)pNode;
      *ppSubQIdx = &pRemote->subQIdx;
      *ppRenderMode = &pRemote->renderMode;
      *pppRemotePlan = &pRemote->pRemotePlan;
      return true;
    }
    case QUERY_NODE_REMOTE_ZERO_ROWS: {
      SRemoteZeroRowsNode* pRemote = (SRemoteZeroRowsNode*)pNode;
      *ppSubQIdx = &pRemote->subQIdx;
      *ppRenderMode = &pRemote->renderMode;
      *pppRemotePlan = &pRemote->pRemotePlan;
      return true;
    }
    default:
      return false;
  }
}

static void planWalkLogicExprFields(SLogicNode* pNode, FNodeWalker walker, void* pContext) {
  if (NULL == pNode) {
    return;
  }

  nodesWalkExprs(pNode->pTargets, walker, pContext);
  nodesWalkExpr(pNode->pConditions, walker, pContext);
  nodesWalkExpr(pNode->pLimit, walker, pContext);
  nodesWalkExpr(pNode->pSlimit, walker, pContext);

  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN: {
      SScanLogicNode* pScan = (SScanLogicNode*)pNode;
      nodesWalkExprs(pScan->pScanCols, walker, pContext);
      nodesWalkExprs(pScan->pScanPseudoCols, walker, pContext);
      nodesWalkExpr(pScan->pTimeRange, walker, pContext);
      nodesWalkExpr(pScan->pExtTimeRange, walker, pContext);
      nodesWalkExpr(pScan->pPrimaryCond, walker, pContext);
      nodesWalkExpr(pScan->pTagCond, walker, pContext);
      nodesWalkExpr(pScan->pTagIndexCond, walker, pContext);
      nodesWalkExprs(pScan->pDynamicScanFuncs, walker, pContext);
      nodesWalkExprs(pScan->pGroupTags, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_JOIN: {
      SJoinLogicNode* pJoin = (SJoinLogicNode*)pNode;
      nodesWalkExpr(pJoin->pWindowOffset, walker, pContext);
      nodesWalkExpr(pJoin->pJLimit, walker, pContext);
      nodesWalkExpr(pJoin->addPrimEqCond, walker, pContext);
      nodesWalkExpr(pJoin->pPrimKeyEqCond, walker, pContext);
      nodesWalkExpr(pJoin->pColEqCond, walker, pContext);
      nodesWalkExpr(pJoin->pColOnCond, walker, pContext);
      nodesWalkExpr(pJoin->pTagEqCond, walker, pContext);
      nodesWalkExpr(pJoin->pTagOnCond, walker, pContext);
      nodesWalkExpr(pJoin->pFullOnCond, walker, pContext);
      nodesWalkExprs(pJoin->pLeftEqNodes, walker, pContext);
      nodesWalkExprs(pJoin->pRightEqNodes, walker, pContext);
      nodesWalkExpr(pJoin->pLeftOnCond, walker, pContext);
      nodesWalkExpr(pJoin->pRightOnCond, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_AGG: {
      SAggLogicNode* pAgg = (SAggLogicNode*)pNode;
      nodesWalkExprs(pAgg->pGroupKeys, walker, pContext);
      nodesWalkExprs(pAgg->pAggFuncs, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      nodesWalkExprs(((SProjectLogicNode*)pNode)->pProjections, walker, pContext);
      break;
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
      nodesWalkExprs(((SIndefRowsFuncLogicNode*)pNode)->pFuncs, walker, pContext);
      break;
    case QUERY_NODE_LOGIC_PLAN_INTERP_FUNC: {
      SInterpFuncLogicNode* pInterp = (SInterpFuncLogicNode*)pNode;
      nodesWalkExprs(pInterp->pFuncs, walker, pContext);
      nodesWalkExpr(pInterp->pTimeRange, walker, pContext);
      nodesWalkExpr(pInterp->pFillValues, walker, pContext);
      nodesWalkExpr(pInterp->pTimeSeries, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_FORECAST_FUNC:
    case QUERY_NODE_LOGIC_PLAN_ANALYSIS_FUNC:
      nodesWalkExprs(((SForecastFuncLogicNode*)pNode)->pFuncs, walker, pContext);
      break;
    case QUERY_NODE_LOGIC_PLAN_GROUP_CACHE:
      nodesWalkExprs(((SGroupCacheLogicNode*)pNode)->pGroupCols, walker, pContext);
      break;
    case QUERY_NODE_LOGIC_PLAN_WINDOW: {
      SWindowLogicNode* pWin = (SWindowLogicNode*)pNode;
      nodesWalkExprs(pWin->pFuncs, walker, pContext);
      nodesWalkExpr(pWin->pTimeRange, walker, pContext);
      nodesWalkExpr(pWin->pTspk, walker, pContext);
      nodesWalkExpr(pWin->pTsEnd, walker, pContext);
      nodesWalkExprs(pWin->pStateExprs, walker, pContext);
      nodesWalkExpr(pWin->pStartCond, walker, pContext);
      nodesWalkExpr(pWin->pEndCond, walker, pContext);
      nodesWalkExprs(pWin->pColList, walker, pContext);
      nodesWalkExprs(pWin->pProjs, walker, pContext);
      nodesWalkExprs(pWin->extFill.pFillExprs, walker, pContext);
      nodesWalkExpr(pWin->extFill.pFillValues, walker, pContext);
      nodesWalkExprs(pWin->pAnomalyExpr, walker, pContext);
      nodesWalkExpr(pWin->pSubquery, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_FILL: {
      SFillLogicNode* pFill = (SFillLogicNode*)pNode;
      nodesWalkExprs(pFill->pFillExprs, walker, pContext);
      nodesWalkExprs(pFill->pNotFillExprs, walker, pContext);
      nodesWalkExpr(pFill->pWStartTs, walker, pContext);
      nodesWalkExpr(pFill->pValues, walker, pContext);
      nodesWalkExpr(pFill->pTimeRange, walker, pContext);
      nodesWalkExprs(pFill->pFillNullExprs, walker, pContext);
      nodesWalkExpr(pFill->pSurroundingTime, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_SORT:
      nodesWalkExprs(((SSortLogicNode*)pNode)->pSortKeys, walker, pContext);
      break;
    case QUERY_NODE_LOGIC_PLAN_PARTITION: {
      SPartitionLogicNode* pPart = (SPartitionLogicNode*)pNode;
      nodesWalkExprs(pPart->pPartitionKeys, walker, pContext);
      nodesWalkExprs(pPart->pTags, walker, pContext);
      nodesWalkExpr(pPart->pSubtable, walker, pContext);
      nodesWalkExprs(pPart->pAggFuncs, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN: {
      SVirtualScanLogicNode* pVScan = (SVirtualScanLogicNode*)pNode;
      nodesWalkExprs(pVScan->pScanCols, walker, pContext);
      nodesWalkExprs(pVScan->pScanPseudoCols, walker, pContext);
      nodesWalkExprs(pVScan->pLocalTags, walker, pContext);
      nodesWalkExprs(pVScan->pRefTagCols, walker, pContext);
      nodesWalkExpr(pVScan->pTagFilterCond, walker, pContext);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL:
      nodesWalkExprs(((SDynQueryCtrlLogicNode*)pNode)->vtbScan.pScanCols, walker, pContext);
      break;
    default:
      break;
  }
}

static void planWalkLogicTreeForRewrite(SLogicNode* pNode, FNodeWalker walker, SPlanRemoteRewriteCtx* pCtx) {
  if (NULL == pNode || TSDB_CODE_SUCCESS != pCtx->code) {
    return;
  }

  planWalkLogicExprFields(pNode, walker, pCtx);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    return;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    if (NULL != pScan->pRemoteLogicPlan) {
      bool prev = pCtx->insideRemotePlan;
      pCtx->insideRemotePlan = true;
      planWalkLogicTreeForRewrite((SLogicNode*)pScan->pRemoteLogicPlan, walker, pCtx);
      pCtx->insideRemotePlan = prev;
      if (TSDB_CODE_SUCCESS != pCtx->code) {
        return;
      }
    }
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    planWalkLogicTreeForRewrite((SLogicNode*)pChild, walker, pCtx);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      return;
    }
  }
}

static void planWalkLogicTreeForMark(SLogicNode* pNode, FNodeWalker walker, SPlanRemoteMarkCtx* pCtx) {
  if (NULL == pNode || TSDB_CODE_SUCCESS != pCtx->code) {
    return;
  }

  planWalkLogicExprFields(pNode, walker, pCtx);
  if (TSDB_CODE_SUCCESS != pCtx->code) {
    return;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    if (NULL != pScan->pRemoteLogicPlan) {
      planWalkLogicTreeForMark((SLogicNode*)pScan->pRemoteLogicPlan, walker, pCtx);
      if (TSDB_CODE_SUCCESS != pCtx->code) {
        return;
      }
    }
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    planWalkLogicTreeForMark((SLogicNode*)pChild, walker, pCtx);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      return;
    }
  }
}

static void initSubQueryPlanContext(SPlanContext* pDst, SPlanContext* pSrc, SNode* pRoot,
                                    SPlanSubqueryState* pState, int32_t subQIdx) {
  memcpy(pDst, pSrc, sizeof(*pSrc));

  pDst->groupId++;
  pDst->streamCxt.hasExtWindow = false;
  pDst->hasScan = false;
  pDst->pSubqueryState = pState;
  pDst->subqueryIndex = subQIdx;
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

static bool isExternalWindowSubquery(SNode* pRoot, int32_t subQIdx) {
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
  if (pExternal->pSubquery == NULL || QUERY_NODE_REMOTE_TABLE != nodeType(pExternal->pSubquery)) {
    return false;
  }

  return ((SRemoteTableNode*)pExternal->pSubquery)->subQIdx == subQIdx;
}

static void planDestroyQueryUnit(SPlanQueryUnit* pUnit, bool destroyPhysiPlan) {
  if (NULL == pUnit) {
    return;
  }
  nodesDestroyNode((SNode*)pUnit->pLogicSubplan);
  pUnit->pLogicSubplan = NULL;
  nodesDestroyNode((SNode*)pUnit->pLogicPlan);
  pUnit->pLogicPlan = NULL;
  if (destroyPhysiPlan) {
    nodesDestroyNode((SNode*)pUnit->pPhysiPlan);
    pUnit->pPhysiPlan = NULL;
  }
}

static void planDestroySubqueryState(SPlanSubqueryState* pState, bool destroyPhysiPlans) {
  if (NULL == pState) {
    return;
  }

  planDestroyQueryUnit(&pState->rootUnit, destroyPhysiPlans);
  if (NULL != pState->pSubqueryUnits) {
    for (int32_t i = 0; i < pState->numSubQueries; ++i) {
      SPlanQueryUnit* pUnit = pState->pSubqueryUnits[i];
      if (NULL == pUnit) {
        continue;
      }
      planDestroyQueryUnit(pUnit, destroyPhysiPlans);
      taosMemoryFree(pUnit);
    }
    taosMemoryFree(pState->pSubqueryUnits);
  }
  taosMemoryFree(pState);
}

static int32_t planCreateSubqueryState(SPlanContext* pRootCxt, SPlanSubqueryState** ppState) {
  SPlanSubqueryState* pState = taosMemoryCalloc(1, sizeof(*pState));
  if (NULL == pState) {
    return terrno;
  }

  pState->pTopAstRoot = planExtractQueryRoot(pRootCxt->pAstRoot);
  pState->pAllSubQueries = planExtractSubQueries(pRootCxt->pAstRoot);
  pState->numSubQueries = (NULL != pState->pAllSubQueries) ? LIST_LENGTH(pState->pAllSubQueries) : 0;
  if (pState->numSubQueries > 0) {
    pState->pSubqueryUnits = taosMemoryCalloc((size_t)pState->numSubQueries, sizeof(SPlanQueryUnit*));
    if (NULL == pState->pSubqueryUnits) {
      taosMemoryFree(pState);
      return terrno;
    }
  }

  pState->rootUnit.pCtx = pRootCxt;
  pState->rootUnit.ownsCtx = false;
  pState->rootUnit.isRoot = true;
  pState->rootUnit.pAstRoot = pRootCxt->pAstRoot;
  pState->rootUnit.origSubQIdx = -1;
  pState->rootUnit.finalSubQIdx = -1;

  pRootCxt->pSubqueryState = pState;
  pRootCxt->subqueryIndex = -1;

  planDebug("QID:0x%" PRIx64 ", plan state created, topRootType:%d, totalSubQueries:%d", pRootCxt->queryId,
            pState->pTopAstRoot ? (int)nodeType(pState->pTopAstRoot) : -1, pState->numSubQueries);

  *ppState = pState;
  return TSDB_CODE_SUCCESS;
}

static int32_t planCreateLogicForUnit(SPlanQueryUnit* pUnit) {
  planDebug("QID:0x%" PRIx64 ", create logic plan start, queryRole:%s, origSubQIdx:%d, astType:%d, groupId:%d",
            pUnit->pCtx->queryId, pUnit->isRoot ? "root" : "subquery", pUnit->origSubQIdx,
            pUnit->pAstRoot ? (int)nodeType(pUnit->pAstRoot) : -1, pUnit->pCtx->groupId);
  int32_t code = createLogicPlan(pUnit->pCtx, &pUnit->pLogicSubplan);
  if (TSDB_CODE_SUCCESS == code && NULL != pUnit->pLogicSubplan) {
    planDebug("QID:0x%" PRIx64 ", create logic plan done, queryRole:%s, origSubQIdx:%d, subplanGroupId:%d, rootType:%d",
              pUnit->pCtx->queryId, pUnit->isRoot ? "root" : "subquery", pUnit->origSubQIdx,
              pUnit->pLogicSubplan->id.groupId,
              pUnit->pLogicSubplan->pNode ? (int)nodeType(pUnit->pLogicSubplan->pNode) : -1);
  }
  return code;
}

static int32_t planCreateAllLogicPlans(SPlanContext* pRootCxt, SPlanSubqueryState* pState) {
  int32_t code = planCreateLogicForUnit(&pState->rootUnit);
  if (TSDB_CODE_SUCCESS != code || 0 == pState->numSubQueries) {
    return code;
  }

  SNode* pNode = NULL;
  int32_t subQIdx = 0;
  FOREACH(pNode, pState->pAllSubQueries) {
    SPlanQueryUnit* pUnit = taosMemoryCalloc(1, sizeof(*pUnit));
    if (NULL == pUnit) {
      return terrno;
    }
    pUnit->pCtx = &pUnit->localCtx;
    pUnit->ownsCtx = true;
    pUnit->isRoot = false;
    pUnit->pAstRoot = pNode;
    pUnit->origSubQIdx = subQIdx;
    pUnit->finalSubQIdx = -1;

    initSubQueryPlanContext(pUnit->pCtx, pRootCxt, pNode, pState, subQIdx);
    pUnit->pCtx->forceNoMergeDataBlock =
        pUnit->pCtx->forceNoMergeDataBlock || isPartitionedExternalWindowSubquery(pState->pTopAstRoot, subQIdx);

    planDebug("QID:0x%" PRIx64 ", stage1 register subquery, origSubQIdx:%d, astType:%d, forceNoMerge:%d, inheritedGroupId:%d",
              pRootCxt->queryId, subQIdx, (int)nodeType(pNode), (int)pUnit->pCtx->forceNoMergeDataBlock,
              pUnit->pCtx->groupId);

    code = planCreateLogicForUnit(pUnit);
    if (TSDB_CODE_SUCCESS != code) {
      planDestroyQueryUnit(pUnit, false);
      taosMemoryFree(pUnit);
      return code;
    }

    pState->pSubqueryUnits[subQIdx] = pUnit;
    pRootCxt->groupId = ++pUnit->pCtx->groupId;
    planDebug("QID:0x%" PRIx64 ", stage1 subquery logic registered, origSubQIdx:%d, nextSiblingGroupId:%d",
              pRootCxt->queryId, subQIdx, pRootCxt->groupId);
    ++subQIdx;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t planMarkSubqueryPushedDown(SPlanContext* pCxt, int32_t subQIdx);

static EDealRes planMarkRemoteSubqueryWalker(SNode* pNode, void* pContext) {
  SPlanRemoteMarkCtx* pCtx = (SPlanRemoteMarkCtx*)pContext;
  int32_t*            pSubQIdx = NULL;
  EFqSubqRenderMode*  pMode = NULL;
  SNode**             ppRemotePlan = NULL;
  if (!planGetRemoteExprInfo(pNode, &pSubQIdx, &pMode, &ppRemotePlan)) {
    return DEAL_RES_CONTINUE;
  }
  (void)pMode;
  (void)ppRemotePlan;

  planDebug("QID:0x%" PRIx64 ", mark pushedDown descendant, ownerSubQIdx:%d -> childSubQIdx:%d, nodeType:%d",
            pCtx->pCxt->queryId, pCtx->ownerSubQIdx, *pSubQIdx, (int)nodeType(pNode));
  pCtx->code = planMarkSubqueryPushedDown(pCtx->pCxt, *pSubQIdx);
  return (TSDB_CODE_SUCCESS == pCtx->code) ? DEAL_RES_IGNORE_CHILD : DEAL_RES_END;
}

int32_t planMarkSubqueryPushedDown(SPlanContext* pCxt, int32_t subQIdx) {
  SPlanQueryUnit* pUnit = planGetQueryUnitImpl(pCxt, subQIdx);
  if (NULL == pUnit || NULL == pUnit->pLogicSubplan) {
    planError("QID:0x%" PRIx64 ", invalid pushedDown mark target, subQIdx:%d, unit:%p", pCxt ? pCxt->queryId : 0,
              subQIdx, pUnit);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  if (pUnit->pLogicSubplan->pushedDown) {
    planDebug("QID:0x%" PRIx64 ", pushedDown mark skipped, subQIdx:%d already absorbed", pCxt->queryId, subQIdx);
    return TSDB_CODE_SUCCESS;
  }

  pUnit->pLogicSubplan->pushedDown = true;
  planDebug("QID:0x%" PRIx64 ", pushedDown mark set, subQIdx:%d, logicRootType:%d", pCxt->queryId, subQIdx,
            pUnit->pLogicSubplan->pNode ? (int)nodeType(pUnit->pLogicSubplan->pNode) : -1);

  SPlanRemoteMarkCtx    markCtx = {.pCxt = pCxt, .code = TSDB_CODE_SUCCESS, .ownerSubQIdx = subQIdx};
  planWalkLogicTreeForMark(pUnit->pLogicSubplan->pNode, planMarkRemoteSubqueryWalker, &markCtx);
  return markCtx.code;
}

static int32_t planOptimizeUnit(SPlanQueryUnit* pUnit) {
  if (NULL != pUnit->pLogicSubplan && pUnit->pLogicSubplan->pushedDown) {
    planDebug("QID:0x%" PRIx64 ", stage2 skip optimize, queryRole:%s, origSubQIdx:%d already absorbed",
              pUnit->pCtx->queryId, pUnit->isRoot ? "root" : "subquery", pUnit->origSubQIdx);
    return TSDB_CODE_SUCCESS;
  }

  planDebug("QID:0x%" PRIx64 ", stage2 optimize start, queryRole:%s, origSubQIdx:%d, logicRootType:%d",
            pUnit->pCtx->queryId, pUnit->isRoot ? "root" : "subquery", pUnit->origSubQIdx,
            (NULL != pUnit->pLogicSubplan && NULL != pUnit->pLogicSubplan->pNode) ? (int)nodeType(pUnit->pLogicSubplan->pNode) : -1);
  int32_t code = optimizeLogicPlan(pUnit->pCtx, pUnit->pLogicSubplan);
  if (TSDB_CODE_SUCCESS == code) {
    planDebug("QID:0x%" PRIx64 ", stage2 optimize done, queryRole:%s, origSubQIdx:%d, pushedDown:%d",
              pUnit->pCtx->queryId, pUnit->isRoot ? "root" : "subquery", pUnit->origSubQIdx,
              (int)pUnit->pLogicSubplan->pushedDown);
  }
  return code;
}

static int32_t planOptimizeAllLogicPlans(SPlanSubqueryState* pState) {
  int32_t code = planOptimizeUnit(&pState->rootUnit);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  for (int32_t i = 0; i < pState->numSubQueries; ++i) {
    code = planOptimizeUnit(pState->pSubqueryUnits[i]);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static EDealRes planRewriteRemoteSubqueryWalker(SNode* pNode, void* pContext) {
  SPlanRemoteRewriteCtx* pCtx = (SPlanRemoteRewriteCtx*)pContext;
  int32_t*               pSubQIdx = NULL;
  EFqSubqRenderMode*     pMode = NULL;
  SNode**                ppRemotePlan = NULL;
  if (!planGetRemoteExprInfo(pNode, &pSubQIdx, &pMode, &ppRemotePlan)) {
    return DEAL_RES_CONTINUE;
  }

  int32_t oldIdx = *pSubQIdx;
  if (oldIdx < 0 || oldIdx >= pCtx->pState->numSubQueries) {
    planError("QID:0x%" PRIx64 ", stage3 remote ref invalid, ownerSubQIdx:%d, remoteNodeType:%d, subQIdx:%d, insideRemotePlan:%d",
              pCtx->queryId, pCtx->currentSubQIdx, (int)nodeType(pNode), oldIdx, (int)pCtx->insideRemotePlan);
    pCtx->code = TSDB_CODE_PLAN_INTERNAL_ERROR;
    return DEAL_RES_END;
  }

  int32_t newIdx = pCtx->pOldToNew[oldIdx];
  if (newIdx >= 0) {
    *pSubQIdx = newIdx;
    planDebug("QID:0x%" PRIx64 ", stage3 remote ref remap, ownerSubQIdx:%d, remoteNodeType:%d, oldSubQIdx:%d -> newSubQIdx:%d, insideRemotePlan:%d",
              pCtx->queryId, pCtx->currentSubQIdx, (int)nodeType(pNode), oldIdx, newIdx, (int)pCtx->insideRemotePlan);
  } else if (!pCtx->insideRemotePlan) {
    planError("QID:0x%" PRIx64 ", stage3 remote ref lost kept endpoint, ownerSubQIdx:%d, remoteNodeType:%d, removedSubQIdx:%d outside remote plan",
              pCtx->queryId, pCtx->currentSubQIdx, (int)nodeType(pNode), oldIdx);
    pCtx->code = TSDB_CODE_PLAN_INTERNAL_ERROR;
    return DEAL_RES_END;
  } else {
    planDebug("QID:0x%" PRIx64 ", stage3 remote ref absorbed in remote plan, ownerSubQIdx:%d, remoteNodeType:%d, absorbedSubQIdx:%d kept for trace only",
              pCtx->queryId, pCtx->currentSubQIdx, (int)nodeType(pNode), oldIdx);
  }

  if (NULL != *ppRemotePlan) {
    nodesDestroyNode(*ppRemotePlan);
    *ppRemotePlan = NULL;
  }
  if (*pMode != FQ_SUBQ_RENDER_DIRECT_SQL) {
    SLogicNode* pRenderRoot = planGetSubqueryRenderLogic(pCtx->pState->rootUnit.pCtx, oldIdx, nodeType(pNode));
    if (NULL == pRenderRoot) {
      planError("QID:0x%" PRIx64 ", stage3 remote ref missing render logic, ownerSubQIdx:%d, remoteNodeType:%d, refSubQIdx:%d, renderMode:%d",
                pCtx->queryId, pCtx->currentSubQIdx, (int)nodeType(pNode), oldIdx, (int)*pMode);
      pCtx->code = TSDB_CODE_PLAN_INTERNAL_ERROR;
      return DEAL_RES_END;
    }
    pCtx->code = nodesCloneNode((SNode*)pRenderRoot, ppRemotePlan);
    if (TSDB_CODE_SUCCESS != pCtx->code) {
      planError("QID:0x%" PRIx64 ", stage3 remote ref clone render logic failed, ownerSubQIdx:%d, remoteNodeType:%d, refSubQIdx:%d, code:%s",
                pCtx->queryId, pCtx->currentSubQIdx, (int)nodeType(pNode), oldIdx, tstrerror(pCtx->code));
      return DEAL_RES_END;
    }
    planDebug("QID:0x%" PRIx64 ", stage3 remote ref attach render logic, ownerSubQIdx:%d, remoteNodeType:%d, refSubQIdx:%d, renderMode:%d, renderRootType:%d",
              pCtx->queryId, pCtx->currentSubQIdx, (int)nodeType(pNode), oldIdx, (int)*pMode,
              (int)nodeType((SNode*)pRenderRoot));
  } else {
    planDebug("QID:0x%" PRIx64 ", stage3 remote ref keep direct sql, ownerSubQIdx:%d, remoteNodeType:%d, refSubQIdx:%d",
              pCtx->queryId, pCtx->currentSubQIdx, (int)nodeType(pNode), oldIdx);
  }

  return DEAL_RES_IGNORE_CHILD;
}

static int32_t planRewriteUnitRemoteRefs(SPlanSubqueryState* pState, SPlanQueryUnit* pUnit, const int32_t* pOldToNew) {
  SPlanRemoteRewriteCtx ctx = {
      .pState = pState,
      .pOldToNew = pOldToNew,
      .insideRemotePlan = false,
      .code = TSDB_CODE_SUCCESS,
      .queryId = pUnit->pCtx->queryId,
      .currentSubQIdx = pUnit->origSubQIdx,
  };
  planWalkLogicTreeForRewrite(pUnit->pLogicSubplan->pNode, planRewriteRemoteSubqueryWalker, &ctx);
  return ctx.code;
}

static int32_t planReorderAllSubqueries(SPlanSubqueryState* pState, int32_t** ppOldToNew, int32_t* pRetainedNum) {
  int32_t* pOldToNew = taosMemoryMalloc((size_t)pState->numSubQueries * sizeof(int32_t));
  if (NULL == pOldToNew && pState->numSubQueries > 0) {
    return terrno;
  }

  for (int32_t i = 0; i < pState->numSubQueries; ++i) {
    pOldToNew[i] = -1;
  }

  int32_t kept = 0;
  for (int32_t i = 0; i < pState->numSubQueries; ++i) {
    SPlanQueryUnit* pUnit = pState->pSubqueryUnits[i];
    if (NULL == pUnit || NULL == pUnit->pLogicSubplan) {
      continue;
    }
    if (!pUnit->pLogicSubplan->pushedDown) {
      pOldToNew[i] = kept;
      pUnit->finalSubQIdx = kept++;
      planDebug("QID:0x%" PRIx64 ", stage3 retain subquery, origSubQIdx:%d -> newSubQIdx:%d",
                pState->rootUnit.pCtx->queryId, i, pUnit->finalSubQIdx);
    } else {
      pUnit->finalSubQIdx = -1;
      planDebug("QID:0x%" PRIx64 ", stage3 remove pushedDown subquery, origSubQIdx:%d",
                pState->rootUnit.pCtx->queryId, i);
    }
  }

  int32_t code = planRewriteUnitRemoteRefs(pState, &pState->rootUnit, pOldToNew);
  if (TSDB_CODE_SUCCESS == code) {
    for (int32_t i = 0; i < pState->numSubQueries; ++i) {
      SPlanQueryUnit* pUnit = pState->pSubqueryUnits[i];
      if (NULL != pUnit && NULL != pUnit->pLogicSubplan && !pUnit->pLogicSubplan->pushedDown) {
        code = planRewriteUnitRemoteRefs(pState, pUnit, pOldToNew);
        if (TSDB_CODE_SUCCESS != code) {
          break;
        }
      }
    }
  }

  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(pOldToNew);
    return code;
  }

  *ppOldToNew = pOldToNew;
  *pRetainedNum = kept;
  planDebug("QID:0x%" PRIx64 ", stage3 reorder done, retainedSubQueries:%d, removedSubQueries:%d",
            pState->rootUnit.pCtx->queryId, kept, pState->numSubQueries - kept);
  return TSDB_CODE_SUCCESS;
}

static int32_t planBuildPhysiForUnit(SPlanQueryUnit* pUnit, SArray* pExecNodeList) {
  planDebug("QID:0x%" PRIx64 ", stage4 physi start, queryRole:%s, origSubQIdx:%d",
            pUnit->pCtx->queryId, pUnit->isRoot ? "root" : "subquery", pUnit->origSubQIdx);
  int32_t code = splitLogicPlan(pUnit->pCtx, pUnit->pLogicSubplan);
  if (TSDB_CODE_SUCCESS == code) {
    code = postSplitOptimize(pUnit->pCtx, pUnit->pLogicSubplan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = scaleOutLogicPlan(pUnit->pCtx, pUnit->pLogicSubplan, &pUnit->pLogicPlan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createPhysiPlan(pUnit->pCtx, pUnit->pLogicPlan, &pUnit->pPhysiPlan, pExecNodeList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = validateQueryPlan(pUnit->pCtx, pUnit->pPhysiPlan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    planDebug("QID:0x%" PRIx64 ", stage4 physi done, queryRole:%s, origSubQIdx:%d, physiSubplans:%d",
              pUnit->pCtx->queryId, pUnit->isRoot ? "root" : "subquery", pUnit->origSubQIdx,
              pUnit->pPhysiPlan ? pUnit->pPhysiPlan->numOfSubplans : -1);
  }
  return code;
}

static int32_t planAppendRetainedSubqueryPlans(SPlanSubqueryState* pState) {
  SQueryPlan* pRootPlan = pState->rootUnit.pPhysiPlan;
  for (int32_t i = 0; i < pState->numSubQueries; ++i) {
    SPlanQueryUnit* pUnit = pState->pSubqueryUnits[i];
    if (NULL == pUnit || NULL == pUnit->pLogicSubplan || pUnit->pLogicSubplan->pushedDown) {
      continue;
    }
    pUnit->pPhysiPlan->subSql = nodesGetSubSql(pUnit->pAstRoot);
    nodesGetSubQType(pUnit->pAstRoot, (int32_t*)&pUnit->pPhysiPlan->subQType);
    int32_t code = nodesListMakeStrictAppend(&pRootPlan->pChildren, (SNode*)pUnit->pPhysiPlan);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    pRootPlan->numOfSubplans += pUnit->pPhysiPlan->numOfSubplans;
    planDebug("QID:0x%" PRIx64 ", stage4 attach retained subquery plan, origSubQIdx:%d, newSubQIdx:%d, subplans:%d",
              pState->rootUnit.pCtx->queryId, pUnit->origSubQIdx, pUnit->finalSubQIdx,
              pUnit->pPhysiPlan->numOfSubplans);
    pUnit->pPhysiPlan = NULL;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t qCreateQueryPlan(SPlanContext* pCxt, SQueryPlan** pPlan, SArray* pExecNodeList) {
  SPlanSubqueryState* pState = NULL;
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  bool                allocatorReleased = false;
  int32_t*            pOldToNew = NULL;
  int32_t             retainedSubQueries = 0;

  planDebug("QID:0x%" PRIx64 ", planner start, allocatorId:%" PRId64 ", astType:%d, rootSubQIdx:%d",
            pCxt->queryId, pCxt->allocatorId, pCxt->pAstRoot ? (int)nodeType(pCxt->pAstRoot) : -1, pCxt->subqueryIndex);

  TAOS_CHECK_EXIT(nodesAcquireAllocator(pCxt->allocatorId));
  TAOS_CHECK_EXIT(planCreateSubqueryState(pCxt, &pState));

  // Stage 1: create logic plans for root + all global subqueries.
  TAOS_CHECK_EXIT(planCreateAllLogicPlans(pCxt, pState));

  // Stage 2: optimize root first, then subqueries in global order, skipping absorbed ones.
  TAOS_CHECK_EXIT(planOptimizeAllLogicPlans(pState));

  // Stage 3: remove pushed-down subqueries, renumber surviving subqueries, and attach render logic.
  TAOS_CHECK_EXIT(planReorderAllSubqueries(pState, &pOldToNew, &retainedSubQueries));
  planDebug("QID:0x%" PRIx64 ", stage3 summary, retainedSubQueries:%d, removedSubQueries:%d", pCxt->queryId,
            retainedSubQueries, pState->numSubQueries - retainedSubQueries);

  // Stage 4: build physical plans only for root + surviving subqueries.
  TAOS_CHECK_EXIT(planBuildPhysiForUnit(&pState->rootUnit, pExecNodeList));
  for (int32_t i = 0; i < pState->numSubQueries; ++i) {
    SPlanQueryUnit* pUnit = pState->pSubqueryUnits[i];
    if (NULL == pUnit || NULL == pUnit->pLogicSubplan || pUnit->pLogicSubplan->pushedDown) {
      continue;
    }

    // 3.0 creates a stream calc subquery after the root physical plan has
    // consumed its group IDs. Rebase the retained child context here to keep
    // reader subplans and exchange source ranges in the same ID sequence.
    if (pCxt->streamCxt.isCalc) {
      if (pCxt->groupId >= INT32_MAX - 1) {
        TAOS_CHECK_EXIT(TSDB_CODE_PLAN_INTERNAL_ERROR);
      }
      pUnit->pCtx->groupId = pCxt->groupId + 1;
    }
    TAOS_CHECK_EXIT(planBuildPhysiForUnit(pUnit, pExecNodeList));
    if (pCxt->streamCxt.isCalc) {
      if (pUnit->pCtx->groupId >= INT32_MAX - 1) {
        TAOS_CHECK_EXIT(TSDB_CODE_PLAN_INTERNAL_ERROR);
      }
      pCxt->groupId = pUnit->pCtx->groupId + 1;
    }
  }
  TAOS_CHECK_EXIT(planAppendRetainedSubqueryPlans(pState));

  *pPlan = pState->rootUnit.pPhysiPlan;
  pState->rootUnit.pPhysiPlan = NULL;
  TAOS_CHECK_EXIT(dumpQueryPlan(*pPlan));

  pCxt->pSubqueryState = NULL;
  pCxt->subqueryIndex = -1;
  taosMemoryFree(pOldToNew);
  pOldToNew = NULL;
  planDestroySubqueryState(pState, false);
  pState = NULL;
  (void)nodesReleaseAllocator(pCxt->allocatorId);
  allocatorReleased = true;

_exit:
  if (TSDB_CODE_SUCCESS != code) {
    pCxt->pSubqueryState = NULL;
    pCxt->subqueryIndex = -1;
    taosMemoryFree(pOldToNew);
    planDestroySubqueryState(pState, true);
    if (!allocatorReleased) {
      (void)nodesReleaseAllocator(pCxt->allocatorId);
    }
    planError("QID:0x%" PRIx64 ", qCreateQueryPlan failed at line:%d, code:%s", pCxt->queryId, lino, tstrerror(code));
  }

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
    bool onlyExchangeChildren = true;
    SNode* pMergeChild = NULL;
    FOREACH(pMergeChild, pMerge->node.pChildren) {
      if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE != nodeType(pMergeChild)) {
        onlyExchangeChildren = false;
        break;
      }
    }
    if (onlyExchangeChildren && pMerge->srcGroupId <= groupId && pMerge->srcEndGroupId >= groupId) {
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
      if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == nodeType(pChild)) {
        NODES_DESTORY_LIST(((SExchangePhysiNode*)pChild)->pSrcEndPoints);
        taosArrayDestroy(((SExchangePhysiNode*)pChild)->childrenVgIds);
        ((SExchangePhysiNode*)pChild)->childrenVgIds = NULL;
      }
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
