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

#include "plannodes.h"
#include "querynodes.h"
#include "tmsg.h"

typedef enum ETraversalOrder {
  TRAVERSAL_PREORDER = 1,
  TRAVERSAL_INORDER,
  TRAVERSAL_POSTORDER,
} ETraversalOrder;

typedef EDealRes (*FNodeDispatcher)(SNode* pNode, ETraversalOrder order, FNodeWalker walker, void* pContext);

static EDealRes walkExpr(SNode* pNode, ETraversalOrder order, FNodeWalker walker, void* pContext);
static EDealRes walkExprs(SNodeList* pNodeList, ETraversalOrder order, FNodeWalker walker, void* pContext);
static EDealRes walkPhysiPlan(SNode* pNode, ETraversalOrder order, FNodeWalker walker, void* pContext);
static EDealRes walkPhysiPlans(SNodeList* pNodeList, ETraversalOrder order, FNodeWalker walker, void* pContext);

static EDealRes walkNode(SNode* pNode, ETraversalOrder order, FNodeWalker walker, void* pContext,
                         FNodeDispatcher dispatcher) {
  if (NULL == pNode) {
    return DEAL_RES_CONTINUE;
  }

  EDealRes res = DEAL_RES_CONTINUE;

  if (TRAVERSAL_PREORDER == order) {
    res = walker(pNode, pContext);
    if (DEAL_RES_CONTINUE != res) {
      return res;
    }
  }

  res = dispatcher(pNode, order, walker, pContext);

  if (DEAL_RES_ERROR != res && DEAL_RES_END != res && TRAVERSAL_POSTORDER == order) {
    res = walker(pNode, pContext);
  }

  return res;
}

static EDealRes dispatchExpr(SNode* pNode, ETraversalOrder order, FNodeWalker walker, void* pContext) {
  EDealRes res = DEAL_RES_CONTINUE;

  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN:
    case QUERY_NODE_VALUE:
    case QUERY_NODE_LIMIT:
      // these node types with no subnodes
      break;
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOpNode = (SOperatorNode*)pNode;
      res = walkExpr(pOpNode->pLeft, order, walker, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pOpNode->pRight, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_LOGIC_CONDITION:
      res = walkExprs(((SLogicConditionNode*)pNode)->pParameterList, order, walker, pContext);
      break;
    case QUERY_NODE_FUNCTION:
      res = walkExprs(((SFunctionNode*)pNode)->pParameterList, order, walker, pContext);
      break;
    case QUERY_NODE_REAL_TABLE:
    case QUERY_NODE_TEMP_TABLE:
    case QUERY_NODE_VIRTUAL_TABLE:
      break;  // todo
    case QUERY_NODE_JOIN_TABLE: {
      SJoinTableNode* pJoinTableNode = (SJoinTableNode*)pNode;
      res = walkExpr(pJoinTableNode->pLeft, order, walker, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pJoinTableNode->pRight, order, walker, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pJoinTableNode->pOnCond, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_GROUPING_SET:
      res = walkExprs(((SGroupingSetNode*)pNode)->pParameterList, order, walker, pContext);
      break;
    case QUERY_NODE_ORDER_BY_EXPR:
      res = walkExpr(((SOrderByExprNode*)pNode)->pExpr, order, walker, pContext);
      break;
    case QUERY_NODE_STATE_WINDOW: {
      SStateWindowNode* pState = (SStateWindowNode*)pNode;
      res = walkExpr(pState->pExpr, order, walker, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pState->pCol, order, walker, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pState->pTrueForLimit, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_SESSION_WINDOW: {
      SSessionWindowNode* pSession = (SSessionWindowNode*)pNode;
      res = walkExpr((SNode*)pSession->pCol, order, walker, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr((SNode*)pSession->pGap, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_INTERVAL_WINDOW: {
      SIntervalWindowNode* pInterval = (SIntervalWindowNode*)pNode;
      res = walkExpr(pInterval->pInterval, order, walker, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pInterval->pOffset, order, walker, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pInterval->pSliding, order, walker, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pInterval->pFill, order, walker, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pInterval->pCol, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_NODE_LIST:
      res = walkExprs(((SNodeListNode*)pNode)->pNodeList, order, walker, pContext);
      break;
    case QUERY_NODE_FILL: {
      SFillNode* pFill = (SFillNode*)pNode;
      res = walkExpr(pFill->pValues, order, walker, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pFill->pWStartTs, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_RAW_EXPR:
      res = walkExpr(((SRawExprNode*)pNode)->pNode, order, walker, pContext);
      break;
    case QUERY_NODE_TARGET:
      res = walkExpr(((STargetNode*)pNode)->pExpr, order, walker, pContext);
      break;
    case QUERY_NODE_WHEN_THEN: {
      SWhenThenNode* pWhenThen = (SWhenThenNode*)pNode;
      res = walkExpr(pWhenThen->pWhen, order, walker, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pWhenThen->pThen, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_CASE_WHEN: {
      SCaseWhenNode* pCaseWhen = (SCaseWhenNode*)pNode;
      res = walkExpr(pCaseWhen->pCase, order, walker, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pCaseWhen->pElse, order, walker, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExprs(pCaseWhen->pWhenThenList, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_EVENT_WINDOW: {
      SEventWindowNode* pEvent = (SEventWindowNode*)pNode;
      res = walkExpr(pEvent->pCol, order, walker, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pEvent->pStartCond, order, walker, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pEvent->pEndCond, order, walker, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pEvent->pTrueForLimit, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_PERIOD_WINDOW: {
      SPeriodWindowNode* pPeriod = (SPeriodWindowNode*)pNode;
      res = walkExpr(pPeriod->pOffset, order, walker, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pPeriod->pPeroid, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_COUNT_WINDOW: {
      SCountWindowNode* pCount = (SCountWindowNode*)pNode;
      res = walkExpr(pCount->pCol, order, walker, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExprs(pCount->pColList, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_ANOMALY_WINDOW: {
      SAnomalyWindowNode* pAnomaly = (SAnomalyWindowNode*)pNode;
      res = walkExpr(pAnomaly->pExpr, order, walker, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = walkExpr(pAnomaly->pCol, order, walker, pContext);
      }
      break;
    }
    default:
      break;
  }

  return res;
}

static EDealRes walkExpr(SNode* pNode, ETraversalOrder order, FNodeWalker walker, void* pContext) {
  return walkNode(pNode, order, walker, pContext, dispatchExpr);
}

static EDealRes walkExprs(SNodeList* pNodeList, ETraversalOrder order, FNodeWalker walker, void* pContext) {
  SNode* node;
  FOREACH(node, pNodeList) {
    EDealRes res = walkExpr(node, order, walker, pContext);
    if (DEAL_RES_ERROR == res || DEAL_RES_END == res) {
      return res;
    }
  }
  return DEAL_RES_CONTINUE;
}

void nodesWalkExpr(SNode* pNode, FNodeWalker walker, void* pContext) {
  EDealRes res;
  res = walkExpr(pNode, TRAVERSAL_PREORDER, walker, pContext);
}

void nodesWalkExprs(SNodeList* pNodeList, FNodeWalker walker, void* pContext) {
  EDealRes res;
  res = walkExprs(pNodeList, TRAVERSAL_PREORDER, walker, pContext);
}

void nodesWalkExprPostOrder(SNode* pNode, FNodeWalker walker, void* pContext) {
  EDealRes res;
  res = walkExpr(pNode, TRAVERSAL_POSTORDER, walker, pContext);
}

void nodesWalkExprsPostOrder(SNodeList* pList, FNodeWalker walker, void* pContext) {
  EDealRes res;
  res = walkExprs(pList, TRAVERSAL_POSTORDER, walker, pContext);
}

static void markOpLeftAndRightAsParam(SOperatorNode* pOp) {
  if (NULL != pOp->pLeft) {
    if (nodeType(pOp->pLeft) == QUERY_NODE_FUNCTION) {
      ((SFunctionNode*)pOp->pLeft)->node.asParam = true;
    }
    if (nodeType(pOp->pLeft) == QUERY_NODE_COLUMN) {
      ((SColumnNode*)pOp->pLeft)->node.asParam = true;
    }
    if (nodeType(pOp->pLeft) == QUERY_NODE_VALUE) {
      ((SValueNode*)pOp->pLeft)->node.asParam = true;
    }
  }
  if (NULL != pOp->pRight) {
    if (nodeType(pOp->pRight) == QUERY_NODE_FUNCTION) {
      ((SFunctionNode*)pOp->pRight)->node.asParam = true;
    }
    if (nodeType(pOp->pRight) == QUERY_NODE_COLUMN) {
      ((SColumnNode*)pOp->pRight)->node.asParam = true;
    }
    if (nodeType(pOp->pRight) == QUERY_NODE_VALUE) {
      ((SValueNode*)pOp->pRight)->node.asParam = true;
    }
  }
}

static void checkParamIsFunc(SFunctionNode* pFunc) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  for (int32_t i = 0; i < numOfParams; ++i) {
    SNode* pPara = nodesListGetNode(pFunc->pParameterList, i);
    if (numOfParams > 1 && nodeType(pPara) == QUERY_NODE_FUNCTION) {
      ((SFunctionNode*)pPara)->node.asParam = true;
    }
    if (nodeType(pPara) == QUERY_NODE_COLUMN) {
      ((SColumnNode*)pPara)->node.asParam = true;
    }
    if (nodeType(pPara) == QUERY_NODE_VALUE) {
      ((SValueNode*)pPara)->node.asParam = true;
    }
  }
}

static EDealRes rewriteExprs(SNodeList* pNodeList, ETraversalOrder order, FNodeRewriter rewriter, void* pContext);

static EDealRes rewriteExpr(SNode** pRawNode, ETraversalOrder order, FNodeRewriter rewriter, void* pContext) {
  if (NULL == pRawNode || NULL == *pRawNode) {
    return DEAL_RES_CONTINUE;
  }

  EDealRes res = DEAL_RES_CONTINUE;

  if (TRAVERSAL_PREORDER == order) {
    res = rewriter(pRawNode, pContext);
    if (DEAL_RES_CONTINUE != res) {
      return res;
    }
  }

  SNode* pNode = *pRawNode;
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN:
    case QUERY_NODE_VALUE:
    case QUERY_NODE_LIMIT:
      // these node types with no subnodes
      break;
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOpNode = (SOperatorNode*)pNode;
      markOpLeftAndRightAsParam(pOpNode);
      res = rewriteExpr(&(pOpNode->pLeft), order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&(pOpNode->pRight), order, rewriter, pContext);
      }
      break;
    }
    case QUERY_NODE_LOGIC_CONDITION:
      res = rewriteExprs(((SLogicConditionNode*)pNode)->pParameterList, order, rewriter, pContext);
      break;
    case QUERY_NODE_FUNCTION: {
      SFunctionNode* pFunc = (SFunctionNode*)pNode;
      checkParamIsFunc(pFunc);
      res = rewriteExprs(pFunc->pParameterList, order, rewriter, pContext);
      break;
    }
    case QUERY_NODE_REAL_TABLE:
    case QUERY_NODE_TEMP_TABLE:
    case QUERY_NODE_VIRTUAL_TABLE:
      break;  // todo
    case QUERY_NODE_JOIN_TABLE: {
      SJoinTableNode* pJoinTableNode = (SJoinTableNode*)pNode;
      res = rewriteExpr(&(pJoinTableNode->pLeft), order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&(pJoinTableNode->pRight), order, rewriter, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&(pJoinTableNode->pOnCond), order, rewriter, pContext);
      }
      break;
    }
    case QUERY_NODE_GROUPING_SET:
      res = rewriteExprs(((SGroupingSetNode*)pNode)->pParameterList, order, rewriter, pContext);
      break;
    case QUERY_NODE_ORDER_BY_EXPR:
      res = rewriteExpr(&(((SOrderByExprNode*)pNode)->pExpr), order, rewriter, pContext);
      break;
    case QUERY_NODE_STATE_WINDOW: {
      SStateWindowNode* pState = (SStateWindowNode*)pNode;
      res = rewriteExpr(&pState->pExpr, order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&pState->pCol, order, rewriter, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&pState->pTrueForLimit, order, rewriter, pContext);
      }
      break;
    }
    case QUERY_NODE_SESSION_WINDOW: {
      SSessionWindowNode* pSession = (SSessionWindowNode*)pNode;
      res = rewriteExpr((SNode**)&pSession->pCol, order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr((SNode**)&pSession->pGap, order, rewriter, pContext);
      }
      break;
    }
    case QUERY_NODE_INTERVAL_WINDOW: {
      SIntervalWindowNode* pInterval = (SIntervalWindowNode*)pNode;
      res = rewriteExpr(&(pInterval->pInterval), order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&(pInterval->pOffset), order, rewriter, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&(pInterval->pSliding), order, rewriter, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&(pInterval->pSOffset), order, rewriter, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&(pInterval->pFill), order, rewriter, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&(pInterval->pCol), order, rewriter, pContext);
      }
      break;
    }
    case QUERY_NODE_NODE_LIST:
      res = rewriteExprs(((SNodeListNode*)pNode)->pNodeList, order, rewriter, pContext);
      break;
    case QUERY_NODE_FILL: {
      SFillNode* pFill = (SFillNode*)pNode;
      res = rewriteExpr(&pFill->pValues, order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&(pFill->pWStartTs), order, rewriter, pContext);
      }
      if (DEAL_RES_ERROR!= res && DEAL_RES_END!= res) {
        res = rewriteExpr(&(pFill->pTimeRange), order, rewriter, pContext);
      }
      break;
    }
    case QUERY_NODE_RAW_EXPR:
      res = rewriteExpr(&(((SRawExprNode*)pNode)->pNode), order, rewriter, pContext);
      break;
    case QUERY_NODE_TARGET:
      res = rewriteExpr(&(((STargetNode*)pNode)->pExpr), order, rewriter, pContext);
      break;
    case QUERY_NODE_WHEN_THEN: {
      SWhenThenNode* pWhenThen = (SWhenThenNode*)pNode;
      res = rewriteExpr(&pWhenThen->pWhen, order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&pWhenThen->pThen, order, rewriter, pContext);
      }
      break;
    }
    case QUERY_NODE_CASE_WHEN: {
      SCaseWhenNode* pCaseWhen = (SCaseWhenNode*)pNode;
      res = rewriteExpr(&pCaseWhen->pCase, order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&pCaseWhen->pElse, order, rewriter, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExprs(pCaseWhen->pWhenThenList, order, rewriter, pContext);
      }
      break;
    }
    case QUERY_NODE_EVENT_WINDOW: {
      SEventWindowNode* pEvent = (SEventWindowNode*)pNode;
      res = rewriteExpr(&pEvent->pCol, order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&pEvent->pStartCond, order, rewriter, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&pEvent->pEndCond, order, rewriter, pContext);
      }
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&pEvent->pTrueForLimit, order, rewriter, pContext);
      }
      break;
    }
    case QUERY_NODE_WINDOW_OFFSET: {
      SWindowOffsetNode* pWin = (SWindowOffsetNode*)pNode;
      res = rewriteExpr(&pWin->pStartOffset, order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&pWin->pEndOffset, order, rewriter, pContext);
      }
      break;
    }
    case QUERY_NODE_COUNT_WINDOW: {
      SCountWindowNode* pCount = (SCountWindowNode*)pNode;
      res = rewriteExpr(&pCount->pCol, order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExprs(pCount->pColList, order, rewriter, pContext);
      }
      break;
    }
    case QUERY_NODE_ANOMALY_WINDOW: {
      SAnomalyWindowNode* pAnomaly = (SAnomalyWindowNode*)pNode;
      res = rewriteExpr(&pAnomaly->pExpr, order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&pAnomaly->pCol, order, rewriter, pContext);
      }
      break;
    }
    case QUERY_NODE_PERIOD_WINDOW: {
      SPeriodWindowNode* pPeriod = (SPeriodWindowNode*)pNode;
      res = rewriteExpr(&pPeriod->pOffset, order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&pPeriod->pPeroid, order, rewriter, pContext);
      }
      break;
    }
    case QUERY_NODE_STREAM_TAG_DEF: {
      SStreamTagDefNode* pTagDef = (SStreamTagDefNode*)pNode;
      res = rewriteExpr(&pTagDef->pTagExpr, order, rewriter, pContext);
      break;
    }
    case QUERY_NODE_TIME_RANGE: {
      STimeRangeNode* pTimeRange = (STimeRangeNode*)pNode;
      res = rewriteExpr(&pTimeRange->pStart, order, rewriter, pContext);
      if (DEAL_RES_ERROR != res && DEAL_RES_END != res) {
        res = rewriteExpr(&pTimeRange->pEnd, order, rewriter, pContext);
      }
      break;
    }

    default:
      break;
  }

  if (DEAL_RES_ERROR != res && DEAL_RES_END != res && TRAVERSAL_POSTORDER == order) {
    res = rewriter(pRawNode, pContext);
  }

  return res;
}

static EDealRes rewriteExprs(SNodeList* pNodeList, ETraversalOrder order, FNodeRewriter rewriter, void* pContext) {
  SNode** pNode;
  FOREACH_FOR_REWRITE(pNode, pNodeList) {
    EDealRes res = rewriteExpr(pNode, order, rewriter, pContext);
    if (DEAL_RES_ERROR == res || DEAL_RES_END == res) {
      return res;
    }
  }
  return DEAL_RES_CONTINUE;
}

void nodesRewriteExpr(SNode** pNode, FNodeRewriter rewriter, void* pContext) {
  (void)rewriteExpr(pNode, TRAVERSAL_PREORDER, rewriter, pContext);
}

void nodesRewriteExprs(SNodeList* pList, FNodeRewriter rewriter, void* pContext) {
  (void)rewriteExprs(pList, TRAVERSAL_PREORDER, rewriter, pContext);
}

void nodesRewriteExprPostOrder(SNode** pNode, FNodeRewriter rewriter, void* pContext) {
  (void)rewriteExpr(pNode, TRAVERSAL_POSTORDER, rewriter, pContext);
}

void nodesRewriteExprsPostOrder(SNodeList* pList, FNodeRewriter rewriter, void* pContext) {
  (void)rewriteExprs(pList, TRAVERSAL_POSTORDER, rewriter, pContext);
}

void nodesWalkSelectStmtImpl(SSelectStmt* pSelect, ESqlClause clause, FNodeWalker walker, void* pContext) {
  if (NULL == pSelect) {
    return;
  }

  switch (clause) {
    case SQL_CLAUSE_FROM:
      nodesWalkExpr(pSelect->pFromTable, walker, pContext);
      nodesWalkExpr(pSelect->pWhere, walker, pContext);
    case SQL_CLAUSE_WHERE:
      nodesWalkExprs(pSelect->pPartitionByList, walker, pContext);
    case SQL_CLAUSE_PARTITION_BY:
      nodesWalkExpr(pSelect->pWindow, walker, pContext);
    case SQL_CLAUSE_WINDOW:
      if (NULL != pSelect->pWindow) {
        if (QUERY_NODE_INTERVAL_WINDOW == nodeType(pSelect->pWindow)) {
          nodesWalkExpr(((SIntervalWindowNode*)pSelect->pWindow)->pFill, walker, pContext);
        }
        else if (QUERY_NODE_COUNT_WINDOW == nodeType(pSelect->pWindow)) {
          nodesWalkExprs(((SCountWindowNode*)pSelect->pWindow)->pColList, walker, pContext);
        }
      }
    case SQL_CLAUSE_FILL:
      nodesWalkExprs(pSelect->pGroupByList, walker, pContext);
    case SQL_CLAUSE_GROUP_BY:
      nodesWalkExpr(pSelect->pHaving, walker, pContext);
    case SQL_CLAUSE_HAVING:
    case SQL_CLAUSE_SELECT:
    case SQL_CLAUSE_DISTINCT:
      nodesWalkExprs(pSelect->pOrderByList, walker, pContext);
    case SQL_CLAUSE_ORDER_BY:
      nodesWalkExprs(pSelect->pProjectionList, walker, pContext);
      nodesWalkExprs(pSelect->pProjectionBindList, walker, pContext);
    default:
      break;
  }

  return;
}

void nodesWalkSelectStmt(SSelectStmt* pSelect, ESqlClause clause, FNodeWalker walker, void* pContext) {
  nodesWalkSelectStmtImpl(pSelect, clause, walker, pContext);
}

void nodesRewriteSelectStmt(SSelectStmt* pSelect, ESqlClause clause, FNodeRewriter rewriter, void* pContext) {
  if (NULL == pSelect) {
    return;
  }

  switch (clause) {
    case SQL_CLAUSE_FROM:
      nodesRewriteExpr(&(pSelect->pFromTable), rewriter, pContext);
      nodesRewriteExpr(&(pSelect->pWhere), rewriter, pContext);
    case SQL_CLAUSE_WHERE:
      nodesRewriteExprs(pSelect->pPartitionByList, rewriter, pContext);
    case SQL_CLAUSE_PARTITION_BY:
      nodesRewriteExpr(&(pSelect->pWindow), rewriter, pContext);
    case SQL_CLAUSE_WINDOW:
      if (NULL != pSelect->pWindow) {
        if (QUERY_NODE_INTERVAL_WINDOW == nodeType(pSelect->pWindow)) {
          nodesRewriteExpr(&(((SIntervalWindowNode*)pSelect->pWindow)->pFill), rewriter, pContext);
        } else if (QUERY_NODE_COUNT_WINDOW == nodeType(pSelect->pWindow)) {
          nodesRewriteExprs(((SCountWindowNode*)pSelect->pWindow)->pColList, rewriter, pContext);
        }
      }
    case SQL_CLAUSE_FILL:
      nodesRewriteExprs(pSelect->pGroupByList, rewriter, pContext);
    case SQL_CLAUSE_GROUP_BY:
      nodesRewriteExpr(&(pSelect->pHaving), rewriter, pContext);
    case SQL_CLAUSE_HAVING:
    case SQL_CLAUSE_SELECT:
    case SQL_CLAUSE_DISTINCT:
      nodesRewriteExprs(pSelect->pOrderByList, rewriter, pContext);
    case SQL_CLAUSE_ORDER_BY:
      nodesRewriteExprs(pSelect->pProjectionList, rewriter, pContext);
      nodesRewriteExprs(pSelect->pProjectionBindList, rewriter, pContext);
    default:
      break;
  }

  return;
}
