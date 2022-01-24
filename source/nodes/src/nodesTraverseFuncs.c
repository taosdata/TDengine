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

typedef bool (*FQueryNodeWalker)(SNode* pNode, void* pContext);

bool nodesWalkNodeList(SNodeList* pNodeList, FQueryNodeWalker walker, void* pContext) {
  SNode* node;
  FOREACH(node, pNodeList) {
    if (!nodesWalkNode(node, walker, pContext)) {
      return false;
    }
  }
  return true;
}

bool nodesWalkNode(SNode* pNode, FQueryNodeWalker walker, void* pContext) {
  if (NULL == pNode) {
    return true;
  }

  if (!walker(pNode, pContext)) {
    return false;
  }

  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN:
    case QUERY_NODE_VALUE:
    case QUERY_NODE_LIMIT:
      // these node types with no subnodes
      return true;
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOpNode = (SOperatorNode*)pNode;
      if (!nodesWalkNode(pOpNode->pLeft, walker, pContext)) {
        return false;
      }
      return nodesWalkNode(pOpNode->pRight, walker, pContext);
    }
    case QUERY_NODE_LOGIC_CONDITION:
      return nodesWalkNodeList(((SLogicConditionNode*)pNode)->pParameterList, walker, pContext);
    case QUERY_NODE_IS_NULL_CONDITION:
      return nodesWalkNode(((SIsNullCondNode*)pNode)->pExpr, walker, pContext);
    case QUERY_NODE_FUNCTION:
      return nodesWalkNodeList(((SFunctionNode*)pNode)->pParameterList, walker, pContext);
    case QUERY_NODE_REAL_TABLE:
    case QUERY_NODE_TEMP_TABLE:
      return true; // todo
    case QUERY_NODE_JOIN_TABLE: {
      SJoinTableNode* pJoinTableNode = (SJoinTableNode*)pNode;
      if (!nodesWalkNode(pJoinTableNode->pLeft, walker, pContext)) {
        return false;
      }
      if (!nodesWalkNode(pJoinTableNode->pRight, walker, pContext)) {
        return false;
      }
      return nodesWalkNode(pJoinTableNode->pOnCond, walker, pContext);
    }
    case QUERY_NODE_GROUPING_SET:
      return nodesWalkNodeList(((SGroupingSetNode*)pNode)->pParameterList, walker, pContext);
    case QUERY_NODE_ORDER_BY_EXPR:
      return nodesWalkNode(((SOrderByExprNode*)pNode)->pExpr, walker, pContext);
    default:
      break;
  }

  return false;
}

bool nodesWalkStmt(SNode* pNode, FQueryNodeWalker walker, void* pContext) {

}
