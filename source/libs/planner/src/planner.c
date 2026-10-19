/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "functionMgt.h"
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

static bool hasSplitScanHint(SNode* pNode) {
  if (NULL == pNode) {
    return false;
  }

  if (QUERY_NODE_EXPLAIN_STMT == nodeType(pNode)) {
    pNode = ((SExplainStmt*)pNode)->pQuery;
  }

  return QUERY_NODE_SELECT_STMT == nodeType(pNode) &&
         getOptHint(((SSelectStmt*)pNode)->pHint, HINT_SPLIT_SCAN);
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
  const SCorrelatedSubqueryNode* pCorrelationCall;  // Borrowed from the state-owned snapshot.
} SPlanQueryUnit;

typedef struct SPlanSubqueryState {
  SNode*          pTopAstRoot;
  SNodeList*      pAllSubQueries;
  int32_t         numSubQueries;
  SPlanQueryUnit  rootUnit;
  SPlanQueryUnit** pSubqueryUnits;  // indexed by original global subQIdx
  SCorrelatedSubqueryNode** pCorrelationCalls;  // Owned snapshots, same indexing.
} SPlanSubqueryState;

typedef struct SPlanCollectCorrelationContext {
  SPlanSubqueryState* pState;
  int32_t             code;
} SPlanCollectCorrelationContext;

static void planWalkQueryCorrelationExprs(SNode* pQuery, FNodeWalker walker, void* pContext);

static void planWalkTableCorrelationExprs(SNode* pTable, FNodeWalker walker, void* pContext) {
  if (NULL == pTable) {
    return;
  }
  if (QUERY_NODE_JOIN_TABLE == nodeType(pTable)) {
    SJoinTableNode* pJoin = (SJoinTableNode*)pTable;
    nodesWalkExpr(pJoin->pWindowOffset, walker, pContext);
    nodesWalkExpr(pJoin->pJLimit, walker, pContext);
    planWalkTableCorrelationExprs(pJoin->pLeft, walker, pContext);
    planWalkTableCorrelationExprs(pJoin->pRight, walker, pContext);
  } else if (QUERY_NODE_TEMP_TABLE == nodeType(pTable)) {
    planWalkQueryCorrelationExprs(((STempTableNode*)pTable)->pSubquery, walker, pContext);
  }
}

static void planWalkQueryCorrelationExprs(SNode* pQuery, FNodeWalker walker, void* pContext) {
  if (NULL == pQuery) {
    return;
  }
  if (QUERY_NODE_SET_OPERATOR == nodeType(pQuery)) {
    SSetOperator* pSet = (SSetOperator*)pQuery;
    nodesWalkExprs(pSet->pProjectionList, walker, pContext);
    nodesWalkExprs(pSet->pOrderByList, walker, pContext);
    for (int32_t i = 0; i < setOperatorChildCount(pSet); ++i) {
      planWalkQueryCorrelationExprs(setOperatorChildAt(pSet, i), walker, pContext);
    }
    return;
  }
  if (QUERY_NODE_SELECT_STMT != nodeType(pQuery)) {
    return;
  }
  SSelectStmt* pSelect = (SSelectStmt*)pQuery;
  nodesWalkSelectStmt(pSelect, SQL_CLAUSE_FROM, walker, pContext);
  nodesWalkExpr(pSelect->pRange, walker, pContext);
  nodesWalkExpr(pSelect->pRangeAround, walker, pContext);
  nodesWalkExpr(pSelect->pEvery, walker, pContext);
  planWalkTableCorrelationExprs(pSelect->pFromTable, walker, pContext);
}

static EDealRes planCollectCorrelationCall(SNode* pNode, void* pContext) {
  if (QUERY_NODE_CORRELATED_SUBQUERY != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }
  SPlanCollectCorrelationContext* pCxt = (SPlanCollectCorrelationContext*)pContext;
  const SCorrelatedSubqueryNode* pCall = (const SCorrelatedSubqueryNode*)pNode;
  if (pCall->templateId < 0 || pCall->templateId >= pCxt->pState->numSubQueries) {
    pCxt->code = TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
    return DEAL_RES_ERROR;
  }
  SCorrelatedSubqueryNode* pPrevious = pCxt->pState->pCorrelationCalls[pCall->templateId];
  if (NULL != pPrevious && (pPrevious->exprId != pCall->exprId || pPrevious->scopeId != pCall->scopeId)) {
    pCxt->code = TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
    return DEAL_RES_ERROR;
  }
  if (NULL == pPrevious) {
    SNode* pSnapshot = NULL;
    pCxt->code = nodesCloneNode((const SNode*)pCall, &pSnapshot);
    if (TSDB_CODE_SUCCESS != pCxt->code) {
      return DEAL_RES_ERROR;
    }
    pCxt->pState->pCorrelationCalls[pCall->templateId] = (SCorrelatedSubqueryNode*)pSnapshot;
  }
  SNode* pBindingNode = NULL;
  FOREACH(pBindingNode, ((SCorrelatedSubqueryNode*)pNode)->pParamBindings) {
    SCorrelationParamBindingNode* pBinding = (SCorrelationParamBindingNode*)pBindingNode;
    if (pBinding->scopeId != pCall->parentScopeId || NULL == pBinding->pSource ||
        QUERY_NODE_OUTER_REF != nodeType(pBinding->pSource)) {
      continue;
    }
    SOuterRefNode* pRef = (SOuterRefNode*)pBinding->pSource;
    if (0 != pRef->tableRefId) {
      continue;
    }
    SColumnNode* pCol = NULL;
    pCxt->code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
    if (TSDB_CODE_SUCCESS != pCxt->code) {
      return DEAL_RES_ERROR;
    }
    pCol->node.resType = pRef->node.resType;
    tstrncpy(pCol->colName, pRef->colName, sizeof(pCol->colName));
    tstrncpy(pCol->node.aliasName, pRef->colName, sizeof(pCol->node.aliasName));
    nodesDestroyNode(pBinding->pSource);
    pBinding->pSource = (SNode*)pCol;
  }
  return DEAL_RES_IGNORE_CHILD;
}

static int32_t planCollectCorrelationCalls(SPlanSubqueryState* pState) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  SPlanCollectCorrelationContext cxt = {.pState = pState, .code = TSDB_CODE_SUCCESS};
  planWalkQueryCorrelationExprs(pState->pTopAstRoot, planCollectCorrelationCall, &cxt);
  PLAN_ERR_JRET(cxt.code);
  SNode* pQuery = NULL;
  FOREACH(pQuery, pState->pAllSubQueries) {
    planWalkQueryCorrelationExprs(pQuery, planCollectCorrelationCall, &cxt);
    PLAN_ERR_JRET(cxt.code);
  }
  return code;

_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

typedef struct SPlanParamizeContext {
  const SCorrelatedSubqueryNode* pCall;
  int32_t                        code;
} SPlanParamizeContext;

static const SCorrelationParamBindingNode* planFindCorrelationParam(
    const SCorrelatedSubqueryNode* pCall, int32_t scopeId, int64_t bindingId) {
  SNode* pNode = NULL;
  FOREACH(pNode, pCall->pParamBindings) {
    const SCorrelationParamBindingNode* pBinding = (const SCorrelationParamBindingNode*)pNode;
    if (pBinding->scopeId == scopeId && pBinding->bindingId == bindingId) {
      return pBinding;
    }
  }
  return NULL;
}

static EDealRes planReplaceOuterRefWithParam(SNode** ppNode, void* pContext) {
  if (QUERY_NODE_CORRELATED_SUBQUERY == nodeType(*ppNode)) {
    return DEAL_RES_IGNORE_CHILD;
  }
  if (QUERY_NODE_OUTER_REF != nodeType(*ppNode)) {
    return DEAL_RES_CONTINUE;
  }

  SPlanParamizeContext* pCxt = (SPlanParamizeContext*)pContext;
  SOuterRefNode* pRef = (SOuterRefNode*)*ppNode;
  const SCorrelationParamBindingNode* pBinding =
      planFindCorrelationParam(pCxt->pCall, pRef->scopeId, pRef->bindingId);

  int32_t code = TSDB_CODE_SUCCESS;
  SCorrelationParamNode* pParam = NULL;
  if (NULL == pBinding) {
    PLAN_ERR_JRET(TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
  }
  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_CORRELATION_PARAM, (SNode**)&pParam));
  pParam->paramId = pBinding->paramId;
  pParam->node.resType = pRef->node.resType;
  tstrncpy(pParam->node.aliasName, pRef->node.aliasName, sizeof(pParam->node.aliasName));
  tstrncpy(pParam->node.userAlias, pRef->node.userAlias, sizeof(pParam->node.userAlias));
  tstrncpy(pParam->node.srcTable, pRef->node.srcTable, sizeof(pParam->node.srcTable));
  pParam->node.projIdx = pRef->node.projIdx;
  pParam->node.relatedTo = pRef->node.relatedTo;
  nodesDestroyNode(*ppNode);
  *ppNode = (SNode*)pParam;
  return DEAL_RES_IGNORE_CHILD;

_return:
  pCxt->code = code;
  planError("%s failed since %s", __func__, tstrerror(code));
  nodesDestroyNode((SNode*)pParam);
  return DEAL_RES_ERROR;
}

static void planParamizeQueryOuterRefs(SNode* pQuery, SPlanParamizeContext* pCxt);

static void planParamizeTableOuterRefs(SNode* pTable, SPlanParamizeContext* pCxt) {
  if (NULL == pTable || TSDB_CODE_SUCCESS != pCxt->code) {
    return;
  }
  if (QUERY_NODE_JOIN_TABLE == nodeType(pTable)) {
    SJoinTableNode* pJoin = (SJoinTableNode*)pTable;
    nodesRewriteExpr(&pJoin->pWindowOffset, planReplaceOuterRefWithParam, pCxt);
    nodesRewriteExpr(&pJoin->pJLimit, planReplaceOuterRefWithParam, pCxt);
    planParamizeTableOuterRefs(pJoin->pLeft, pCxt);
    planParamizeTableOuterRefs(pJoin->pRight, pCxt);
  } else if (QUERY_NODE_TEMP_TABLE == nodeType(pTable)) {
    planParamizeQueryOuterRefs(((STempTableNode*)pTable)->pSubquery, pCxt);
  }
}

static void planParamizeQueryOuterRefs(SNode* pQuery, SPlanParamizeContext* pCxt) {
  if (NULL == pQuery || TSDB_CODE_SUCCESS != pCxt->code) {
    return;
  }
  if (QUERY_NODE_SET_OPERATOR == nodeType(pQuery)) {
    SSetOperator* pSet = (SSetOperator*)pQuery;
    nodesRewriteExprs(pSet->pProjectionList, planReplaceOuterRefWithParam, pCxt);
    nodesRewriteExprs(pSet->pOrderByList, planReplaceOuterRefWithParam, pCxt);
    for (int32_t i = 0; i < setOperatorChildCount(pSet); ++i) {
      planParamizeQueryOuterRefs(setOperatorChildAt(pSet, i), pCxt);
    }
    return;
  }
  if (QUERY_NODE_SELECT_STMT != nodeType(pQuery)) {
    return;
  }
  SSelectStmt* pSelect = (SSelectStmt*)pQuery;
  nodesRewriteSelectStmt(pSelect, SQL_CLAUSE_FROM, planReplaceOuterRefWithParam, pCxt);
  nodesRewriteExpr(&pSelect->pRange, planReplaceOuterRefWithParam, pCxt);
  nodesRewriteExpr(&pSelect->pRangeAround, planReplaceOuterRefWithParam, pCxt);
  nodesRewriteExpr(&pSelect->pEvery, planReplaceOuterRefWithParam, pCxt);
  nodesRewriteExpr(&pSelect->pFill, planReplaceOuterRefWithParam, pCxt);
  planParamizeTableOuterRefs(pSelect->pFromTable, pCxt);
}

static EDealRes planBindInheritedCorrelationSource(SNode* pNode, void* pContext) {
  if (QUERY_NODE_CORRELATED_SUBQUERY != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }
  SPlanParamizeContext* pCxt = (SPlanParamizeContext*)pContext;
  SCorrelatedSubqueryNode* pNested = (SCorrelatedSubqueryNode*)pNode;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SNode* pBindingNode = NULL;
  FOREACH(pBindingNode, pNested->pParamBindings) {
    SCorrelationParamBindingNode* pBinding = (SCorrelationParamBindingNode*)pBindingNode;
    if (NULL == pBinding->pSource || QUERY_NODE_OUTER_REF != nodeType(pBinding->pSource)) {
      continue;
    }
    SOuterRefNode* pRef = (SOuterRefNode*)pBinding->pSource;
    const SCorrelationParamBindingNode* pInherited =
        planFindCorrelationParam(pCxt->pCall, pRef->scopeId, pRef->bindingId);
    QUERY_CHECK_NULL(pInherited, code, lino, _return, TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
    pBinding->sourceKind = CORRELATION_SOURCE_INHERITED;
    pBinding->inheritedParamId = pInherited->paramId;
    nodesDestroyNode(pBinding->pSource);
    pBinding->pSource = NULL;
  }
  return DEAL_RES_IGNORE_CHILD;

_return:
  pCxt->code = code;
  planError("%s failed since %s", __func__, tstrerror(code));
  return DEAL_RES_ERROR;
}

static int32_t planParamizeCorrelationUnit(SPlanQueryUnit* pUnit) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SPlanParamizeContext cxt = {.pCall = pUnit->pCorrelationCall, .code = TSDB_CODE_SUCCESS};
  planParamizeQueryOuterRefs(pUnit->pAstRoot, &cxt);
  PLAN_ERR_JRET(cxt.code);
  planWalkQueryCorrelationExprs(pUnit->pAstRoot, planBindInheritedCorrelationSource, &cxt);
  PLAN_ERR_JRET(cxt.code);
  return code;

_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

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

void planWalkLogicExprFields(SLogicNode* pNode, FNodeWalker walker, void* pContext) {
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

  pDst->pJoinPropsStore = NULL;
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
  planDestroyJoinPropsStore(pUnit->pCtx);
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
  if (NULL != pState->pCorrelationCalls) {
    for (int32_t i = 0; i < pState->numSubQueries; ++i) {
      nodesDestroyNode((SNode*)pState->pCorrelationCalls[i]);
    }
    taosMemoryFree(pState->pCorrelationCalls);
  }
  taosMemoryFree(pState);
}

static int32_t planCreateSubqueryState(SPlanContext* pRootCxt, SPlanSubqueryState** ppState) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SPlanSubqueryState* pState = taosMemoryCalloc(1, sizeof(*pState));
  QUERY_CHECK_NULL(pState, code, lino, _return, terrno);

  pState->pTopAstRoot = planExtractQueryRoot(pRootCxt->pAstRoot);
  pState->pAllSubQueries = planExtractSubQueries(pRootCxt->pAstRoot);
  pState->numSubQueries = (NULL != pState->pAllSubQueries) ? LIST_LENGTH(pState->pAllSubQueries) : 0;
  if (pState->numSubQueries > 0) {
    pState->pSubqueryUnits = taosMemoryCalloc((size_t)pState->numSubQueries, sizeof(SPlanQueryUnit*));
    QUERY_CHECK_NULL(pState->pSubqueryUnits, code, lino, _return, terrno);
    pState->pCorrelationCalls = taosMemoryCalloc((size_t)pState->numSubQueries,
                                                 sizeof(SCorrelatedSubqueryNode*));
    QUERY_CHECK_NULL(pState->pCorrelationCalls, code, lino, _return, terrno);
  }

  pState->rootUnit.pCtx = pRootCxt;
  pState->rootUnit.ownsCtx = false;
  pState->rootUnit.isRoot = true;
  pState->rootUnit.pAstRoot = pRootCxt->pAstRoot;
  pState->rootUnit.origSubQIdx = -1;
  pState->rootUnit.finalSubQIdx = -1;

  pRootCxt->pJoinPropsStore = NULL;
  pRootCxt->pSubqueryState = pState;
  pRootCxt->subqueryIndex = -1;

  planDebug("QID:0x%" PRIx64 ", plan state created, topRootType:%d, totalSubQueries:%d", pRootCxt->queryId,
            pState->pTopAstRoot ? (int)nodeType(pState->pTopAstRoot) : -1, pState->numSubQueries);

  *ppState = pState;
  return code;

_return:
  planError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  planDestroySubqueryState(pState, false);
  return code;
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
    pUnit->pCorrelationCall = pState->pCorrelationCalls[subQIdx];

    initSubQueryPlanContext(pUnit->pCtx, pRootCxt, pNode, pState, subQIdx);
    pUnit->pCtx->forceNoMergeDataBlock =
        pUnit->pCtx->forceNoMergeDataBlock || isPartitionedExternalWindowSubquery(pState->pTopAstRoot, subQIdx);

    planDebug("QID:0x%" PRIx64 ", stage1 register subquery, origSubQIdx:%d, astType:%d, forceNoMerge:%d, inheritedGroupId:%d",
              pRootCxt->queryId, subQIdx, (int)nodeType(pNode), (int)pUnit->pCtx->forceNoMergeDataBlock,
              pUnit->pCtx->groupId);

    if (NULL != pUnit->pCorrelationCall) {
      code = planParamizeCorrelationUnit(pUnit);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = planCreateLogicForUnit(pUnit);
    }
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
    if (NULL != pUnit->pCorrelationCall) {
      pUnit->finalSubQIdx = -1;
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

typedef struct SPlanFinalizeCorrelationContext {
  int32_t code;
} SPlanFinalizeCorrelationContext;

static EDealRes planFinalizeCorrelationExpr(SNode* pNode, void* pContext) {
  SPlanFinalizeCorrelationContext* pCxt = (SPlanFinalizeCorrelationContext*)pContext;
  int32_t                          code = TSDB_CODE_SUCCESS;
  if (QUERY_NODE_OUTER_REF == nodeType(pNode)) {
    PLAN_ERR_JRET(TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
  }
  if (QUERY_NODE_CORRELATED_SUBQUERY != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SCorrelatedSubqueryNode* pCall = (SCorrelatedSubqueryNode*)pNode;
  SNode* pBindingNode = NULL;
  FOREACH(pBindingNode, pCall->pParamBindings) {
    SCorrelationParamBindingNode* pBinding = (SCorrelationParamBindingNode*)pBindingNode;
    if (CORRELATION_SOURCE_INHERITED == pBinding->sourceKind) {
      continue;
    }
    if (NULL == pBinding->pSource || QUERY_NODE_COLUMN != nodeType(pBinding->pSource)) {
      PLAN_ERR_JRET(TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
    }
    SColumnNode* pSource = (SColumnNode*)pBinding->pSource;
    if (pSource->dataBlockId < 0 || pSource->slotId < 0) {
      PLAN_ERR_JRET(TSDB_CODE_PLAN_SLOT_NOT_FOUND);
    }
    pBinding->sourceKind = CORRELATION_SOURCE_BLOCK;
    pBinding->sourceBlockId = pSource->dataBlockId;
    pBinding->sourceSlotId = pSource->slotId;
  }
  return DEAL_RES_IGNORE_CHILD;

_return:
  pCxt->code = code;
  planError("%s failed since %s", __func__, tstrerror(code));
  return DEAL_RES_ERROR;
}

static void planWalkPhysiCorrelationExprs(SPhysiNode* pNode, FNodeWalker walker, void* pContext, int32_t* pCode) {
  if (NULL == pNode || TSDB_CODE_SUCCESS != *pCode) {
    return;
  }

  nodesWalkExpr(pNode->pConditions, walker, pContext);
  if (TSDB_CODE_SUCCESS == *pCode) {
    nodesWalkExpr(pNode->pLimit, walker, pContext);
  }
  if (TSDB_CODE_SUCCESS == *pCode) {
    nodesWalkExpr(pNode->pSlimit, walker, pContext);
  }
  if (TSDB_CODE_SUCCESS != *pCode) {
    return;
  }
  switch (nodeType((SNode*)pNode)) {
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT: {
      nodesWalkExprs(((SProjectPhysiNode*)pNode)->pProjections, walker, pContext);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_HASH_AGG: {
      SAggPhysiNode* pAgg = (SAggPhysiNode*)pNode;
      nodesWalkExprs(pAgg->pExprs, walker, pContext);
      if (TSDB_CODE_SUCCESS == *pCode) {
        nodesWalkExprs(pAgg->pGroupKeys, walker, pContext);
      }
      if (TSDB_CODE_SUCCESS == *pCode) {
        nodesWalkExprs(pAgg->pAggFuncs, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SORT:
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT: {
      SSortPhysiNode* pSort = (SSortPhysiNode*)pNode;
      nodesWalkExprs(pSort->pExprs, walker, pContext);
      if (TSDB_CODE_SUCCESS == *pCode) {
        nodesWalkExprs(pSort->pSortKeys, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION: {
      SPartitionPhysiNode* pPart = (SPartitionPhysiNode*)pNode;
      nodesWalkExprs(pPart->pExprs, walker, pContext);
      if (TSDB_CODE_SUCCESS == *pCode) {
        nodesWalkExprs(pPart->pPartitionKeys, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN: {
      SHashJoinPhysiNode* pJoin = (SHashJoinPhysiNode*)pNode;
      nodesWalkExpr(pJoin->pLeftOnCond, walker, pContext);
      if (TSDB_CODE_SUCCESS == *pCode) {
        nodesWalkExpr(pJoin->pRightOnCond, walker, pContext);
      }
      if (TSDB_CODE_SUCCESS == *pCode) {
        nodesWalkExpr(pJoin->pFullOnCond, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN: {
      SSortMergeJoinPhysiNode* pJoin = (SSortMergeJoinPhysiNode*)pNode;
      nodesWalkExpr(pJoin->pColOnCond, walker, pContext);
      if (TSDB_CODE_SUCCESS == *pCode) {
        nodesWalkExpr(pJoin->pFullOnCond, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW:
    case QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL: {
      SWindowPhysiNode* pWindow = (SWindowPhysiNode*)pNode;
      nodesWalkExprs(pWindow->pExprs, walker, pContext);
      if (TSDB_CODE_SUCCESS == *pCode) {
        nodesWalkExprs(pWindow->pFuncs, walker, pContext);
      }
      if (TSDB_CODE_SUCCESS == *pCode) {
        nodesWalkExprs(pWindow->pProjs, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_WINDOW_FUNC: {
      SWindowFuncPhysiNode* pWindow = (SWindowFuncPhysiNode*)pNode;
      nodesWalkExprs(pWindow->pExprs, walker, pContext);
      if (TSDB_CODE_SUCCESS == *pCode) {
        nodesWalkExprs(pWindow->pPartitionKeys, walker, pContext);
      }
      if (TSDB_CODE_SUCCESS == *pCode) {
        nodesWalkExprs(pWindow->pOrderKeys, walker, pContext);
      }
      if (TSDB_CODE_SUCCESS == *pCode) {
        nodesWalkExprs(pWindow->pFuncs, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC: {
      SIndefRowsFuncPhysiNode* pFunc = (SIndefRowsFuncPhysiNode*)pNode;
      nodesWalkExprs(pFunc->pExprs, walker, pContext);
      if (TSDB_CODE_SUCCESS == *pCode) {
        nodesWalkExprs(pFunc->pFuncs, walker, pContext);
      }
      break;
    }
    default:
      break;
  }

  if (TSDB_CODE_SUCCESS != *pCode) {
    return;
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    planWalkPhysiCorrelationExprs((SPhysiNode*)pChild, walker, pContext, pCode);
  }
}

static int32_t planFinalizeCorrelationPlan(SQueryPlan* pPlan) {
  int32_t                         code = TSDB_CODE_SUCCESS;
  SPlanFinalizeCorrelationContext cxt = {.code = TSDB_CODE_SUCCESS};
  SNode*                          pLevel = NULL;
  FOREACH(pLevel, pPlan->pSubplans) {
    SNode* pSubplanNode = NULL;
    FOREACH(pSubplanNode, ((SNodeListNode*)pLevel)->pNodeList) {
      planWalkPhysiCorrelationExprs(((SSubplan*)pSubplanNode)->pNode, planFinalizeCorrelationExpr, &cxt,
                                    &cxt.code);
      PLAN_ERR_JRET(cxt.code);
    }
  }
  return code;

_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
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
    code = planFinalizeCorrelationPlan(pUnit->pPhysiPlan);
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
    if (NULL == pUnit || NULL == pUnit->pLogicSubplan || pUnit->pLogicSubplan->pushedDown ||
        NULL != pUnit->pCorrelationCall) {
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

static EDealRes planFindDeferredQueryTime(SNode* pNode, void* pContext) {
  if (QUERY_NODE_VALUE == nodeType(pNode) &&
      (((SValueNode*)pNode)->flag & VALUE_FLAG_DEFERRED_QUERY_TIME)) {
    *(bool*)pContext = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t planAppendTimeRangeInit(SCorrelationTemplateNode* pTemplate, const SSelectStmt* pSelect,
                                       const SSubplanId* pSubplanId, const SPhysiNode* pNode,
                                       bool interpRange) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SCorrelationTimeRangeInitNode* pInit = NULL;
  const SNode*                   pRangeExpr = interpRange ? pSelect->pRange : pSelect->pWhere;
  QUERY_CHECK_NULL(pRangeExpr, code, lino, _return, TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
  QUERY_CHECK_NULL(pNode->pOutputDataBlockDesc, code, lino, _return, TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_CORRELATION_TIME_RANGE_INIT, (SNode**)&pInit));
  pInit->scopeId = pTemplate->scopeId;
  pInit->precision = pSelect->precision;
  pInit->interpRange = interpRange;
  pInit->allowNoTimeRange = !interpRange && pSelect->deferredFillAllowNoTimeRange;
  pInit->targetSubplanId = *pSubplanId;
  pInit->targetBlockId = pNode->pOutputDataBlockDesc->dataBlockId;
  pInit->targetNodeType = nodeType((const SNode*)pNode);
  PLAN_ERR_JRET(nodesCloneNode(pRangeExpr, &pInit->pRangeExpr));
  PLAN_ERR_JRET(nodesListMakeStrictAppend(&pTemplate->pTimeRangeInit, (SNode*)pInit));
  pInit = NULL;
  return code;

_return:
  planError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  nodesDestroyNode((SNode*)pInit);
  return code;
}

static int32_t planCollectTimeRangeInitTargets(SCorrelationTemplateNode* pTemplate,
                                                const SSelectStmt* pSelect, const SSubplanId* pSubplanId,
                                                const SPhysiNode* pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == pNode) {
    return code;
  }
  bool interpRange = false;
  bool needsInit = false;
  switch (nodeType((const SNode*)pNode)) {
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      if (pSelect->timeRangeParamDependent) {
        nodesWalkExprs(((SProjectPhysiNode*)pNode)->pProjections, planFindDeferredQueryTime, &needsInit);
      }
      break;
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW:
    case QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL:
      needsInit = pSelect->timeRangeParamDependent;
      break;
    case QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC:
      interpRange = true;
      needsInit = pSelect->interpRangeParamDependent;
      break;
    case QUERY_NODE_PHYSICAL_PLAN_FILL:
      interpRange = pSelect->hasInterpFunc;
      needsInit = interpRange ? pSelect->interpRangeParamDependent : pSelect->timeRangeParamDependent;
      break;
    default:
      break;
  }
  if (needsInit) {
    PLAN_ERR_JRET(planAppendTimeRangeInit(pTemplate, pSelect, pSubplanId, pNode, interpRange));
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    PLAN_ERR_JRET(planCollectTimeRangeInitTargets(pTemplate, pSelect, pSubplanId,
                                                  (const SPhysiNode*)pChild));
  }
  return code;

_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

static int32_t planAttachTimeRangeInit(SCorrelationTemplateNode* pTemplate, const SPlanQueryUnit* pUnit) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (QUERY_NODE_SELECT_STMT != nodeType(pUnit->pAstRoot)) {
    return code;
  }
  const SSelectStmt* pSelect = (const SSelectStmt*)pUnit->pAstRoot;
  if (!pSelect->timeRangeParamDependent && !pSelect->interpRangeParamDependent) {
    return code;
  }
  SNode* pLevel = NULL;
  FOREACH(pLevel, pUnit->pPhysiPlan->pSubplans) {
    SNode* pSubplanNode = NULL;
    FOREACH(pSubplanNode, ((SNodeListNode*)pLevel)->pNodeList) {
      const SSubplan* pSubplan = (const SSubplan*)pSubplanNode;
      PLAN_ERR_JRET(planCollectTimeRangeInitTargets(pTemplate, pSelect, &pSubplan->id, pSubplan->pNode));
    }
  }
  return code;

_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  return code;
}

static int32_t planCreateCorrelationTemplate(SPlanQueryUnit* pUnit, SCorrelationTemplateNode** ppTemplate) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  SCorrelationTemplateNode*      pTemplate = NULL;
  const SCorrelatedSubqueryNode* pCall = pUnit->pCorrelationCall;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_CORRELATION_TEMPLATE, (SNode**)&pTemplate));
  pTemplate->templateId = pUnit->origSubQIdx;
  pTemplate->scopeId = pCall->scopeId;
  pTemplate->resultType = pCall->node.resType;
  const char* pResultName = pCall->node.aliasName;
  if ('\0' == pResultName[0]) {
    snprintf(pTemplate->resultName, sizeof(pTemplate->resultName), "#correlation_result_%d", pTemplate->templateId);
  } else {
    tstrncpy(pTemplate->resultName, pResultName, sizeof(pTemplate->resultName));
  }

  SNode* pBindingNode = NULL;
  FOREACH(pBindingNode, pCall->pParamBindings) {
    const SCorrelationParamBindingNode* pBinding = (const SCorrelationParamBindingNode*)pBindingNode;
    SCorrelationParamNode* pParam = NULL;
    PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_CORRELATION_PARAM, (SNode**)&pParam));
    pParam->paramId = pBinding->paramId;
    pParam->node.resType = pBinding->dataType;
    code = nodesListMakeStrictAppend(&pTemplate->pParams, (SNode*)pParam);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pParam);
      PLAN_ERR_JRET(code);
    }
  }

  PLAN_ERR_JRET(planAttachTimeRangeInit(pTemplate, pUnit));
  if (NULL != pUnit->pPhysiPlan->pPostPlan) {
    PLAN_ERR_JRET(TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
  }
  SNode* pLevel = NULL;
  FOREACH(pLevel, pUnit->pPhysiPlan->pSubplans) {
    SNode* pSubplan = NULL;
    FOREACH(pSubplan, ((SNodeListNode*)pLevel)->pNodeList) {
      ((SSubplan*)pSubplan)->requiresAncestorContext = true;
    }
  }
  pUnit->pPhysiPlan->subSql = nodesGetSubSql(pUnit->pAstRoot);
  nodesGetSubQType(pUnit->pAstRoot, (int32_t*)&pUnit->pPhysiPlan->subQType);
  PLAN_ERR_JRET(nodesNodeToMsg((SNode*)pUnit->pPhysiPlan, &pTemplate->pPlan, &pTemplate->planLen));
  *ppTemplate = pTemplate;
  return code;

_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  nodesDestroyNode((SNode*)pTemplate);
  return code;
}

typedef struct SPlanFindCorrelationCallContext {
  int32_t templateId;
  bool    found;
} SPlanFindCorrelationCallContext;

static EDealRes planFindCorrelationCall(SNode* pNode, void* pContext) {
  if (QUERY_NODE_CORRELATED_SUBQUERY != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }
  SPlanFindCorrelationCallContext* pCxt = pContext;
  if (((SCorrelatedSubqueryNode*)pNode)->templateId == pCxt->templateId) {
    pCxt->found = true;
  }
  return DEAL_RES_IGNORE_CHILD;
}

static bool planPhysiPlanHasCorrelationCall(const SQueryPlan* pPlan, int32_t templateId) {
  SPlanFindCorrelationCallContext cxt = {.templateId = templateId};
  int32_t                         code = TSDB_CODE_SUCCESS;
  SNode*                          pLevel = NULL;
  FOREACH(pLevel, pPlan->pSubplans) {
    SNode* pSubplanNode = NULL;
    FOREACH(pSubplanNode, ((SNodeListNode*)pLevel)->pNodeList) {
      planWalkPhysiCorrelationExprs(((SSubplan*)pSubplanNode)->pNode, planFindCorrelationCall, &cxt, &code);
      if (cxt.found) {
        return true;
      }
    }
  }
  return false;
}

static int32_t planAppendCorrelationTemplates(SPlanSubqueryState* pState) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t numTemplates = 0;
  for (int32_t i = 0; i < pState->numSubQueries; ++i) {
    SPlanQueryUnit* pUnit = pState->pSubqueryUnits[i];
    if (NULL != pUnit && NULL != pUnit->pCorrelationCall && NULL != pUnit->pPhysiPlan) {
      ++numTemplates;
    }
  }
  if (0 == numTemplates) {
    return code;
  }

  SCorrelationTemplateNode** pTemplates = taosMemoryCalloc((size_t)pState->numSubQueries, sizeof(*pTemplates));
  uint8_t* pOwned = taosMemoryCalloc((size_t)pState->numSubQueries, sizeof(*pOwned));
  QUERY_CHECK_NULL(pTemplates, code, lino, _return, terrno);
  QUERY_CHECK_NULL(pOwned, code, lino, _return, terrno);

  for (int32_t i = 0; i < pState->numSubQueries; ++i) {
    SPlanQueryUnit* pUnit = pState->pSubqueryUnits[i];
    if (NULL == pUnit || NULL == pUnit->pCorrelationCall || NULL == pUnit->pPhysiPlan) {
      continue;
    }
    PLAN_ERR_JRET(planCreateCorrelationTemplate(pUnit, &pTemplates[i]));
    nodesDestroyNode((SNode*)pUnit->pPhysiPlan);
    pUnit->pPhysiPlan = NULL;
  }

  for (int32_t i = 0; i < pState->numSubQueries; ++i) {
    if (NULL == pTemplates[i]) {
      continue;
    }
    const SCorrelatedSubqueryNode* pCall = pState->pCorrelationCalls[i];
    SNodeList** ppList = &pState->rootUnit.pPhysiPlan->pCorrelationTemplates;
    for (int32_t j = 0; j < pState->numSubQueries; ++j) {
      if (j != i && NULL != pTemplates[j] && pTemplates[j]->scopeId == pCall->parentScopeId) {
        ppList = &pTemplates[j]->pNestedTemplates;
        break;
      }
    }
    if (ppList == &pState->rootUnit.pPhysiPlan->pCorrelationTemplates) {
      for (int32_t j = 0; j < pState->numSubQueries; ++j) {
        SPlanQueryUnit* pUnit = pState->pSubqueryUnits[j];
        if (NULL == pUnit || NULL != pUnit->pCorrelationCall || NULL == pUnit->pPhysiPlan) {
          continue;
        }
        if (planPhysiPlanHasCorrelationCall(pUnit->pPhysiPlan, i)) {
          ppList = &pUnit->pPhysiPlan->pCorrelationTemplates;
          break;
        }
      }
    }
    PLAN_ERR_JRET(nodesListMakeStrictAppend(ppList, (SNode*)pTemplates[i]));
    pOwned[i] = 1;
  }

_return:
  if (TSDB_CODE_SUCCESS != code) {
    if (lino > 0) {
      planError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    } else {
      planError("%s failed since %s", __func__, tstrerror(code));
    }
    if (NULL != pTemplates) {
      for (int32_t i = 0; i < pState->numSubQueries; ++i) {
        if (NULL != pTemplates[i] && (NULL == pOwned || !pOwned[i])) {
          nodesDestroyNode((SNode*)pTemplates[i]);
        }
      }
    }
  }
  taosMemoryFree(pOwned);
  taosMemoryFree(pTemplates);
  return code;
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
  pCxt->splitScan = hasSplitScanHint(pCxt->pAstRoot);
  TAOS_CHECK_EXIT(planCreateSubqueryState(pCxt, &pState));
  TAOS_CHECK_EXIT(planCollectCorrelationCalls(pState));

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
  TAOS_CHECK_EXIT(planAppendCorrelationTemplates(pState));
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
        SDownstreamSourceNode* pNewSource = (SDownstreamSourceNode*)pNew;
        if (pExchange->localBlockExchange) {
          pNewSource->fetchMsgType |= DOWNSTREAM_SOURCE_LOCAL_BLOCK_FLAG;
          pNewSource->localExec = false;
        }
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
        SDownstreamSourceNode* pNewSource = (SDownstreamSourceNode*)pNew;
        if (pMergeExchange->localBlockExchange) {
          pNewSource->fetchMsgType |= DOWNSTREAM_SOURCE_LOCAL_BLOCK_FLAG;
          pNewSource->localExec = false;
        }
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
    return TSDB_CODE_INVALID_PARA;
  }

  if (SUBPLAN_TYPE_MODIFY == pSubplan->subplanType && NULL == pSubplan->pNode) {
    SDataInserterNode* insert = (SDataInserterNode*)pSubplan->pDataSink;
    *pLen = insert->size;
    *pStr = insert->pData;
    insert->pData = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = nodesNodeToMsg((const SNode*)pSubplan, pStr, pLen);
  if (TSDB_CODE_SUCCESS != code || !pSubplan->requiresAncestorContext) {
    return code;
  }

  // The outer TLV type is deliberately unknown to older nodes, which must reject this plan before execution.
  const int32_t envelopeLen = sizeof(int16_t) + sizeof(int32_t);
  if (*pLen > INT32_MAX - envelopeLen) {
    taosMemoryFreeClear(*pStr);
    *pLen = 0;
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }
  char* pWrapped = taosMemoryMalloc(envelopeLen + *pLen);
  if (NULL == pWrapped) {
    code = terrno;
    taosMemoryFreeClear(*pStr);
    *pLen = 0;
    return code;
  }
  int16_t type = htons(INT16_MAX);
  int32_t length = htonl(*pLen);
  memcpy(pWrapped, &type, sizeof(type));
  memcpy(pWrapped + sizeof(type), &length, sizeof(length));
  memcpy(pWrapped + envelopeLen, *pStr, *pLen);
  taosMemoryFree(*pStr);
  *pStr = pWrapped;
  *pLen += envelopeLen;
  return TSDB_CODE_SUCCESS;
}

int32_t qMsgToSubplan(const char* pStr, int32_t len, SSubplan** pSubplan) {
  if (NULL == pStr || NULL == pSubplan || len <= 0) {
    return TSDB_CODE_QRY_INVALID_MSG;
  }

  const int32_t envelopeLen = sizeof(int16_t) + sizeof(int32_t);
  bool wrapped = false;
  if (len >= sizeof(int16_t)) {
    int16_t type = 0;
    memcpy(&type, pStr, sizeof(type));
    wrapped = INT16_MAX == ntohs(type);
  }
  if (wrapped) {
    if (len < envelopeLen) {
      return TSDB_CODE_QRY_INVALID_MSG;
    }
    int32_t length = 0;
    memcpy(&length, pStr + sizeof(int16_t), sizeof(length));
    if (ntohl(length) != len - envelopeLen) {
      return TSDB_CODE_QRY_INVALID_MSG;
    }
    pStr += envelopeLen;
    len -= envelopeLen;
  }

  int32_t code = nodesMsgToNode(pStr, len, (SNode**)pSubplan);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  if (NULL == *pSubplan) {
    return TSDB_CODE_QRY_INVALID_MSG;
  }
  if (wrapped != (*pSubplan)->requiresAncestorContext) {
    nodesDestroyNode((SNode*)*pSubplan);
    *pSubplan = NULL;
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }
  return TSDB_CODE_SUCCESS;
}

SQueryPlan* qStringToQueryPlan(const char* pStr) {
  SQueryPlan* pPlan = NULL;
  if (TSDB_CODE_SUCCESS != nodesStringToNode(pStr, (SNode**)&pPlan)) {
    return NULL;
  }
  return pPlan;
}

void qDestroyQueryPlan(SQueryPlan* pPlan) { nodesDestroyNode((SNode*)pPlan); }

static bool streamReuseReaderExpr(const SNode* pNode, const SScanPhysiNode* pScan, bool allowPartitionPlaceholder);

static bool streamReuseReaderExprs(const SNodeList* pList, const SScanPhysiNode* pScan,
                                   bool allowPartitionPlaceholder) {
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    if (!streamReuseReaderExpr(pNode, pScan, allowPartitionPlaceholder)) return false;
  }
  return true;
}

static bool streamReusePartitionPlaceholder(const SNode* pNode) {
  if (pNode == NULL || nodeType(pNode) != QUERY_NODE_FUNCTION) return false;
  const SFunctionNode* pFunc = (const SFunctionNode*)pNode;
  if (pFunc->funcType != FUNCTION_TYPE_PLACEHOLDER_COLUMN || pFunc->node.relatedTo != 0 || pFunc->pOver != NULL ||
      LIST_LENGTH(pFunc->pParameterList) != 2)
    return false;
  const SNode* pValue = nodesListGetNode(pFunc->pParameterList, 0);
  const SNode* pIndex = nodesListGetNode(pFunc->pParameterList, 1);
  return pValue != NULL && nodeType(pValue) == QUERY_NODE_VALUE && ((const SValueNode*)pValue)->isNull &&
         pIndex != NULL && nodeType(pIndex) == QUERY_NODE_VALUE && ((const SValueNode*)pIndex)->translate &&
         !((const SValueNode*)pIndex)->isNull && ((const SValueNode*)pIndex)->datum.i > 0;
}

static bool streamReuseReaderExpr(const SNode* pNode, const SScanPhysiNode* pScan, bool allowPartitionPlaceholder) {
  if (pNode == NULL) return true;
  switch (nodeType(pNode)) {
    case QUERY_NODE_TARGET: {
      const STargetNode* pTarget = (const STargetNode*)pNode;
      return pTarget->pExpr != NULL && streamReuseReaderExpr(pTarget->pExpr, pScan, allowPartitionPlaceholder);
    }
    case QUERY_NODE_COLUMN: {
      const SColumnNode* pCol = (const SColumnNode*)pNode;
      return (pCol->colType == COLUMN_TYPE_COLUMN || pCol->colType == COLUMN_TYPE_TAG) && pCol->colId > 0 &&
             (pCol->tableId == pScan->uid || pCol->tableId == pScan->suid) && !pCol->hasDep && !pCol->hasRef &&
             pCol->node.relatedTo == 0;
    }
    case QUERY_NODE_VALUE: {
      const SValueNode* pValue = (const SValueNode*)pNode;
      return pValue->translate && pValue->node.resType.type > TSDB_DATA_TYPE_NULL &&
             pValue->node.resType.type < TSDB_DATA_TYPE_MAX && pValue->placeholderNo == 0 &&
             pValue->node.relatedTo == 0 && !IS_VAL_UNSET(pValue->flag) && !IS_TIME_OFFSET_VAL(pValue->flag) &&
             !IS_RAW_SQL_FRAG(pValue->flag);
    }
    case QUERY_NODE_OPERATOR: {
      const SOperatorNode* pOp = (const SOperatorNode*)pNode;
      if (pOp->node.relatedTo != 0 || pOp->pLeft == NULL) return false;
      if (allowPartitionPlaceholder && pOp->opType == OP_TYPE_EQUAL && pOp->pRight != NULL &&
          (streamReusePartitionPlaceholder(pOp->pLeft) || streamReusePartitionPlaceholder(pOp->pRight))) {
        const SNode* pOther = streamReusePartitionPlaceholder(pOp->pLeft) ? pOp->pRight : pOp->pLeft;
        return pOther != NULL && nodeType(pOther) == QUERY_NODE_COLUMN && streamReuseReaderExpr(pOther, pScan, false);
      }
      switch (pOp->opType) {
        case OP_TYPE_ADD:
        case OP_TYPE_SUB:
        case OP_TYPE_MULTI:
        case OP_TYPE_DIV:
        case OP_TYPE_REM:
        case OP_TYPE_BIT_AND:
        case OP_TYPE_BIT_OR:
        case OP_TYPE_GREATER_THAN:
        case OP_TYPE_GREATER_EQUAL:
        case OP_TYPE_LOWER_THAN:
        case OP_TYPE_LOWER_EQUAL:
        case OP_TYPE_EQUAL:
        case OP_TYPE_NOT_EQUAL:
        case OP_TYPE_LIKE:
        case OP_TYPE_NOT_LIKE:
          return pOp->pRight != NULL && streamReuseReaderExpr(pOp->pLeft, pScan, allowPartitionPlaceholder) &&
                 streamReuseReaderExpr(pOp->pRight, pScan, allowPartitionPlaceholder);
        case OP_TYPE_MINUS:
        case OP_TYPE_IS_NULL:
        case OP_TYPE_IS_NOT_NULL:
          return pOp->pRight == NULL && streamReuseReaderExpr(pOp->pLeft, pScan, allowPartitionPlaceholder);
        default:
          return false;
      }
    }
    case QUERY_NODE_LOGIC_CONDITION: {
      const SLogicConditionNode* pCond = (const SLogicConditionNode*)pNode;
      return pCond->node.relatedTo == 0 &&
             (pCond->condType == LOGIC_COND_TYPE_AND || pCond->condType == LOGIC_COND_TYPE_OR ||
              pCond->condType == LOGIC_COND_TYPE_NOT) &&
             LIST_LENGTH(pCond->pParameterList) > 0 &&
             streamReuseReaderExprs(pCond->pParameterList, pScan, allowPartitionPlaceholder);
    }
    default:
      // Functions (including context placeholders), subqueries and unknown expressions are unproven.
      return false;
  }
}

static bool streamReuseReader(const SSubplan* pReader, bool allowPartitionPlaceholder) {
  if (pReader->subplanType != SUBPLAN_TYPE_SCAN || pReader->dynTbname || pReader->pVTables != NULL ||
      LIST_LENGTH(pReader->pSubQ) != 0 || LIST_LENGTH(pReader->pChildren) != 0 || pReader->pNode == NULL ||
      nodeType(pReader->pNode) != QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    return false;
  }
  const STableScanPhysiNode* pTable = (const STableScanPhysiNode*)pReader->pNode;
  const SScanPhysiNode*      pScan = &pTable->scan;
  const SPhysiNode*          pNode = &pScan->node;
  if (pScan->uid == 0 ||
      (pScan->tableType != TSDB_SUPER_TABLE && pScan->tableType != TSDB_CHILD_TABLE &&
       pScan->tableType != TSDB_NORMAL_TABLE) ||
      pScan->virtualStableScan || pScan->useTriggerTableList || pNode->dynamicOp ||
      LIST_LENGTH(pNode->pChildren) != 0 || pNode->pLimit != NULL || pNode->pSlimit != NULL ||
      LIST_LENGTH(pScan->pScanCols) == 0 || pTable->scanSeq[0] != 1 || pTable->scanSeq[1] != 0 || pTable->ratio != 1 ||
      pTable->scanRange.skey > pTable->scanRange.ekey || pTable->pExtScanRange != NULL || pTable->interval != 0 ||
      pTable->sliding != 0 || (allowPartitionPlaceholder && pTable->pDynamicScanFuncs != NULL)) {
    return false;
  }
  // requiresAncestorContext is a transport requirement, not an expression dependency.
  return streamReuseReaderExpr(pReader->pTagCond, pScan, allowPartitionPlaceholder) &&
         streamReuseReaderExpr(pReader->pTagIndexCond, pScan, allowPartitionPlaceholder) &&
         streamReuseReaderExpr(pNode->pConditions, pScan, allowPartitionPlaceholder) &&
         streamReuseReaderExpr(pTable->pPrimaryCond, pScan, false) &&
         streamReuseReaderExpr(pTable->pTimeRange, pScan, false) &&
         streamReuseReaderExpr(pTable->pExtTimeRange, pScan, false) &&
         streamReuseReaderExpr(pTable->pSubtable, pScan, false) &&
         streamReuseReaderExprs(pScan->pScanCols, pScan, false) &&
         streamReuseReaderExprs(pScan->pScanPseudoCols, pScan, false) &&
         streamReuseReaderExprs(pTable->pDynamicScanFuncs, pScan, false) &&
         streamReuseReaderExprs(pTable->pGroupTags, pScan, false) &&
         streamReuseReaderExprs(pTable->pTags, pScan, false);
}

static bool streamReuseExchange(const SExchangePhysiNode* pExchange, const SSubplan* pRunner, const SNodeList* pReaders,
                                bool allowPartitionPlaceholder) {
  // A direct table input has one execution group, potentially many vgroup Readers.
  if (pExchange->srcStartGroupId != pExchange->srcEndGroupId || pExchange->dynTbname || pExchange->node.dynamicOp ||
      pExchange->node.pConditions != NULL || pExchange->node.pLimit != NULL || pExchange->node.pSlimit != NULL ||
      LIST_LENGTH(pExchange->node.pChildren) != 0)
    return false;
  int32_t references = 0;
  SNode*  pRef = NULL;
  FOREACH(pRef, pRunner->pChildren) {
    if (nodeType(pRef) != QUERY_NODE_VALUE) {
      if (nodeType(pRef) != QUERY_NODE_PHYSICAL_SUBPLAN || ((SSubplan*)pRef)->id.groupId == pExchange->srcStartGroupId)
        return false;
      continue;
    }
    const SValueNode* pValue = (const SValueNode*)pRef;
    if (pValue->node.resType.type != TSDB_DATA_TYPE_BIGINT || !pValue->translate || pValue->isNull ||
        IS_VAL_UNSET(pValue->flag))
      return false;
    int32_t groupId = (int32_t)((uint64_t)pValue->datum.i >> 32);
    int32_t subplanId = (int32_t)((uint64_t)pValue->datum.i & UINT32_MAX);
    if (groupId != pExchange->srcStartGroupId) continue;
    SNode* pOther = NULL;
    FOREACH(pOther, pRunner->pChildren) {
      if (pOther != pRef && nodeType(pOther) == QUERY_NODE_VALUE && ((SValueNode*)pOther)->datum.i == pValue->datum.i)
        return false;
    }
    const SSubplan* pMatch = NULL;
    SNode*          pReaderNode = NULL;
    FOREACH(pReaderNode, pReaders) {
      if (nodeType(pReaderNode) != QUERY_NODE_PHYSICAL_SUBPLAN) return false;
      const SSubplan* pReader = (const SSubplan*)pReaderNode;
      if (pReader->id.queryId == pRunner->id.queryId && pReader->id.groupId == groupId &&
          pReader->id.subplanId == subplanId) {
        if (pMatch != NULL) return false;
        pMatch = pReader;
      }
    }
    if (pMatch == NULL || !streamReuseReader(pMatch, allowPartitionPlaceholder)) return false;
    ++references;
  }
  return references > 0;
}

static void streamSetBroadcastReuse(SPhysiNode* pNode, const SSubplan* pRunner, const SNodeList* pReaders,
                                    bool multiGroupJoinQualified) {
  if (pNode == NULL) return;
  if (nodeType(pNode) == QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN && ((SHashJoinPhysiNode*)pNode)->multiGroupJoin) {
    multiGroupJoinQualified = true;
  }
  if (nodeType(pNode) == QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW ||
      nodeType(pNode) == QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL ||
      nodeType(pNode) == QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL) {
    SExternalWindowPhysiNode* pWindow = (SExternalWindowPhysiNode*)pNode;
    pWindow->broadcastInputReusable = false;
    pWindow->broadcastReaderBatch = false;
    SNode* pChild = nodesListGetNode(pNode->pChildren, 0);
    if (nodeType(pNode) != QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL &&
        pWindow->extWinInputMode == EXT_WIN_INPUT_BROADCAST && !pWindow->calcWithPartition &&
        pWindow->pSubquery == NULL && !pWindow->window.indefRowsFunc && pWindow->window.pFuncs == NULL &&
        LIST_LENGTH(pNode->pChildren) == 1 && pChild != NULL && nodeType(pChild) == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
      pWindow->broadcastInputReusable = streamReuseExchange((SExchangePhysiNode*)pChild, pRunner, pReaders, false);
      if (multiGroupJoinQualified && pWindow->pWindowConditions == NULL) {
        pWindow->broadcastReaderBatch = streamReuseExchange((SExchangePhysiNode*)pChild, pRunner, pReaders, true);
      }
    }
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    streamSetBroadcastReuse((SPhysiNode*)pChild, pRunner, pReaders, multiGroupJoinQualified);
  }
}

int32_t qSetStreamBroadcastReuse(SSubplan* pRunner, const SNodeList* pReaderSubplans) {
  if (pRunner == NULL || nodeType(pRunner) != QUERY_NODE_PHYSICAL_SUBPLAN) return TSDB_CODE_INVALID_PARA;
  streamSetBroadcastReuse(pRunner->pNode, pRunner, pReaderSubplans, pRunner->inheritedMultiGroupJoin);
  return TSDB_CODE_SUCCESS;
}
