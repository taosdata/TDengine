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

#include "nodes.h"

typedef enum ETraversalOrder {
  TRAVERSAL_PREORDER = 1,
  TRAVERSAL_POSTORDER
} ETraversalOrder;

static EDealRes walkList(SNodeList* pNodeList, ETraversalOrder order, FQueryNodeWalker walker, void* pContext);

static EDealRes walkNode(SNode* pNode, ETraversalOrder order, FQueryNodeWalker walker, void* pContext) {
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

  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN:
    case QUERY_NODE_VALUE:
    case QUERY_NODE_LIMIT:
      // these node types with no subnodes
      break;
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOpNode = (SOperatorNode*)pNode;
      res = walkNode(pOpNode->pLeft, order, walker, pContext);
      if (DEAL_RES_ERROR != res) {
        res = walkNode(pOpNode->pRight, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_LOGIC_CONDITION:
      res = walkList(((SLogicConditionNode*)pNode)->pParameterList, order, walker, pContext);
      break;
    case QUERY_NODE_IS_NULL_CONDITION:
      res = walkNode(((SIsNullCondNode*)pNode)->pExpr, order, walker, pContext);
      break;
    case QUERY_NODE_FUNCTION:
      res = walkList(((SFunctionNode*)pNode)->pParameterList, order, walker, pContext);
      break;
    case QUERY_NODE_REAL_TABLE:
    case QUERY_NODE_TEMP_TABLE:
      break; // todo
    case QUERY_NODE_JOIN_TABLE: {
      SJoinTableNode* pJoinTableNode = (SJoinTableNode*)pNode;
      res = walkNode(pJoinTableNode->pLeft, order, walker, pContext);
      if (DEAL_RES_ERROR != res) {
        res = walkNode(pJoinTableNode->pRight, order, walker, pContext);
      }
      if (DEAL_RES_ERROR != res) {
        res = walkNode(pJoinTableNode->pOnCond, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_GROUPING_SET:
      res = walkList(((SGroupingSetNode*)pNode)->pParameterList, order, walker, pContext);
      break;
    case QUERY_NODE_ORDER_BY_EXPR:
      res = walkNode(((SOrderByExprNode*)pNode)->pExpr, order, walker, pContext);
      break;
    case QUERY_NODE_STATE_WINDOW:
      res = walkNode(((SStateWindowNode*)pNode)->pCol, order, walker, pContext);
      break;
    case QUERY_NODE_SESSION_WINDOW:
      res = walkNode(((SSessionWindowNode*)pNode)->pCol, order, walker, pContext);
      break;
    case QUERY_NODE_INTERVAL_WINDOW: {
      SIntervalWindowNode* pInterval = (SIntervalWindowNode*)pNode;
      res = walkNode(pInterval->pInterval, order, walker, pContext);
      if (DEAL_RES_ERROR != res) {
        res = walkNode(pInterval->pOffset, order, walker, pContext);
      }
      if (DEAL_RES_ERROR != res) {
        res = walkNode(pInterval->pSliding, order, walker, pContext);
      }
      if (DEAL_RES_ERROR != res) {
        res = walkNode(pInterval->pFill, order, walker, pContext);
      }
      break;
    }
    case QUERY_NODE_NODE_LIST:
      res = walkList(((SNodeListNode*)pNode)->pNodeList, order, walker, pContext);
      break;
    case QUERY_NODE_FILL:
      res = walkNode(((SFillNode*)pNode)->pValues, order, walker, pContext);
      break;
    case QUERY_NODE_RAW_EXPR:
      res = walkNode(((SRawExprNode*)pNode)->pNode, order, walker, pContext);
      break;
    default:
      break;
  }

  if (DEAL_RES_ERROR != res && TRAVERSAL_POSTORDER == order) {
    res = walker(pNode, pContext);
  }

  return res;
}

static EDealRes walkList(SNodeList* pNodeList, ETraversalOrder order, FQueryNodeWalker walker, void* pContext) {
  SNode* node;
  FOREACH(node, pNodeList) {
    if (DEAL_RES_ERROR == walkNode(node, order, walker, pContext)) {
      return DEAL_RES_ERROR;
    }
  }
  return DEAL_RES_CONTINUE;
}

void nodesWalkNode(SNode* pNode, FQueryNodeWalker walker, void* pContext) {
  (void)walkNode(pNode, TRAVERSAL_PREORDER, walker, pContext);
}

void nodesWalkList(SNodeList* pNodeList, FQueryNodeWalker walker, void* pContext) {
  (void)walkList(pNodeList, TRAVERSAL_PREORDER, walker, pContext);
}

void nodesWalkNodePostOrder(SNode* pNode, FQueryNodeWalker walker, void* pContext) {
  (void)walkNode(pNode, TRAVERSAL_POSTORDER, walker, pContext);
}

void nodesWalkListPostOrder(SNodeList* pList, FQueryNodeWalker walker, void* pContext) {
  (void)walkList(pList, TRAVERSAL_POSTORDER, walker, pContext);
}
