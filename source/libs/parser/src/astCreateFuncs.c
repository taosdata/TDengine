
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

#include "astCreateFuncs.h"

#define CHECK_OUT_OF_MEM(p) \
  do { \
    if (NULL == (p)) { \
      pCxt->valid = false; \
      return NULL; \
    } \
  } while (0)

#define CHECK_RAW_EXPR_NODE(node) \
  do { \
    if (NULL == (node) || QUERY_NODE_RAW_EXPR != nodeType(node)) { \
      pCxt->valid = false; \
      return NULL; \
    } \
  } while (0)

SToken nil_token = { .type = TK_NIL, .n = 0, .z = NULL };

static bool checkDbName(SAstCreateContext* pCxt, const SToken* pDbName) {
  if (NULL == pDbName) {
    return true;
  }
  pCxt->valid = pDbName->n < TSDB_DB_NAME_LEN ? true : false;
  return pCxt->valid;
}

static bool checkTableName(SAstCreateContext* pCxt, const SToken* pTableName) {
  if (NULL == pTableName) {
    return true;
  }
  pCxt->valid = pTableName->n < TSDB_TABLE_NAME_LEN ? true : false;
  return pCxt->valid;
}

static bool checkColumnName(SAstCreateContext* pCxt, const SToken* pColumnName) {
  if (NULL == pColumnName) {
    return true;
  }
  pCxt->valid = pColumnName->n < TSDB_COL_NAME_LEN ? true : false;
  return pCxt->valid;
}

SNode* createRawExprNode(SAstCreateContext* pCxt, const SToken* pToken, SNode* pNode) {
  SRawExprNode* target = (SRawExprNode*)nodesMakeNode(QUERY_NODE_RAW_EXPR);
  CHECK_OUT_OF_MEM(target);
  target->p = pToken->z;
  target->n = pToken->n;
  target->pNode = pNode;
  return (SNode*)target;
}

SNode* createRawExprNodeExt(SAstCreateContext* pCxt, const SToken* pStart, const SToken* pEnd, SNode* pNode) {
  SRawExprNode* target = (SRawExprNode*)nodesMakeNode(QUERY_NODE_RAW_EXPR);
  CHECK_OUT_OF_MEM(target);
  target->p = pStart->z;
  target->n = (pEnd->z + pEnd->n) - pStart->z;
  target->pNode = pNode;
  return (SNode*)target;
}

SNode* releaseRawExprNode(SAstCreateContext* pCxt, SNode* pNode) {
  CHECK_RAW_EXPR_NODE(pNode);
  SNode* tmp = ((SRawExprNode*)pNode)->pNode;
  tfree(pNode);
  return tmp;
}

SToken getTokenFromRawExprNode(SAstCreateContext* pCxt, SNode* pNode) {
  SRawExprNode* target = (SRawExprNode*)pNode;
  SToken t = { .type = 0, .z = target->p, .n = target->n};
  return t;
}

SNodeList* createNodeList(SAstCreateContext* pCxt, SNode* pNode) {
  SNodeList* list = nodesMakeList();
  CHECK_OUT_OF_MEM(list);
  return nodesListAppend(list, pNode);
}

SNodeList* addNodeToList(SAstCreateContext* pCxt, SNodeList* pList, SNode* pNode) {
  return nodesListAppend(pList, pNode);
}

SNode* createColumnNode(SAstCreateContext* pCxt, const SToken* pTableAlias, const SToken* pColumnName) {
  if (!checkTableName(pCxt, pTableAlias) || !checkColumnName(pCxt, pColumnName)) {
    return NULL;
  }
  SColumnNode* col = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  CHECK_OUT_OF_MEM(col);
  if (NULL != pTableAlias) {
    strncpy(col->tableAlias, pTableAlias->z, pTableAlias->n);
  }
  strncpy(col->colName, pColumnName->z, pColumnName->n);
  return (SNode*)col;
}

SNode* createValueNode(SAstCreateContext* pCxt, int32_t dataType, const SToken* pLiteral) {
  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
  val->literal = strndup(pLiteral->z, pLiteral->n);
  CHECK_OUT_OF_MEM(val->literal);
  val->node.resType.type = dataType;
  val->node.resType.bytes = tDataTypes[dataType].bytes;
  if (TSDB_DATA_TYPE_TIMESTAMP == dataType) {
    val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  }
  return (SNode*)val;
}

SNode* createDurationValueNode(SAstCreateContext* pCxt, const SToken* pLiteral) {
  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
  val->literal = strndup(pLiteral->z, pLiteral->n);
  CHECK_OUT_OF_MEM(val->literal);
  val->isDuration = true;
  val->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  val->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  return (SNode*)val;
}

SNode* createLogicConditionNode(SAstCreateContext* pCxt, ELogicConditionType type, SNode* pParam1, SNode* pParam2) {
  SLogicConditionNode* cond = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  CHECK_OUT_OF_MEM(cond);
  cond->condType = type;
  cond->pParameterList = nodesMakeList();
  nodesListAppend(cond->pParameterList, pParam1);
  nodesListAppend(cond->pParameterList, pParam2);
  return (SNode*)cond;
}

SNode* createOperatorNode(SAstCreateContext* pCxt, EOperatorType type, SNode* pLeft, SNode* pRight) {
  SOperatorNode* op = (SOperatorNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
  CHECK_OUT_OF_MEM(op);
  op->opType = type;
  op->pLeft = pLeft;
  op->pRight = pRight;
  return (SNode*)op;
}

SNode* createBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight) {
  return createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND,
      createOperatorNode(pCxt, OP_TYPE_GREATER_EQUAL, pExpr, pLeft), createOperatorNode(pCxt, OP_TYPE_LOWER_EQUAL, pExpr, pRight));
}

SNode* createNotBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight) {
  return createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR,
      createOperatorNode(pCxt, OP_TYPE_LOWER_THAN, pExpr, pLeft), createOperatorNode(pCxt, OP_TYPE_GREATER_THAN, pExpr, pRight));
}

SNode* createIsNullCondNode(SAstCreateContext* pCxt, SNode* pExpr, bool isNull) {
  SIsNullCondNode* cond = (SIsNullCondNode*)nodesMakeNode(QUERY_NODE_IS_NULL_CONDITION);
  CHECK_OUT_OF_MEM(cond);
  cond->pExpr = pExpr;
  cond->isNull = isNull;
  return (SNode*)cond;
}

SNode* createFunctionNode(SAstCreateContext* pCxt, const SToken* pFuncName, SNodeList* pParameterList) {
  SFunctionNode* func = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  CHECK_OUT_OF_MEM(func);
  strncpy(func->functionName, pFuncName->z, pFuncName->n);
  func->pParameterList = pParameterList;
  return (SNode*)func;
}

SNode* createNodeListNode(SAstCreateContext* pCxt, SNodeList* pList) {
  SNodeListNode* list = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  CHECK_OUT_OF_MEM(list);
  list->pNodeList = pList;
  return (SNode*)list;
}

SNode* createRealTableNode(SAstCreateContext* pCxt, const SToken* pDbName, const SToken* pTableName, const SToken* pTableAlias) {
  if (!checkDbName(pCxt, pDbName) || !checkTableName(pCxt, pTableName)) {
    return NULL;
  }
  SRealTableNode* realTable = (SRealTableNode*)nodesMakeNode(QUERY_NODE_REAL_TABLE);
  CHECK_OUT_OF_MEM(realTable);
  if (NULL != pDbName) {
    strncpy(realTable->table.dbName, pDbName->z, pDbName->n);
  } else {
    strcpy(realTable->table.dbName, pCxt->pQueryCxt->db);
  }
  if (NULL != pTableAlias && TK_NIL != pTableAlias->type) {
    strncpy(realTable->table.tableAlias, pTableAlias->z, pTableAlias->n);
  } else {
    strncpy(realTable->table.tableAlias, pTableName->z, pTableName->n);
  }
  strncpy(realTable->table.tableName, pTableName->z, pTableName->n);
  return (SNode*)realTable;
}

SNode* createTempTableNode(SAstCreateContext* pCxt, SNode* pSubquery, const SToken* pTableAlias) {
  STempTableNode* tempTable = (STempTableNode*)nodesMakeNode(QUERY_NODE_TEMP_TABLE);
  CHECK_OUT_OF_MEM(tempTable);
  tempTable->pSubquery = pSubquery;
  if (NULL != pTableAlias && TK_NIL != pTableAlias->type) {
    strncpy(tempTable->table.tableAlias, pTableAlias->z, pTableAlias->n);
  }
  return (SNode*)tempTable;
}

SNode* createJoinTableNode(SAstCreateContext* pCxt, EJoinType type, SNode* pLeft, SNode* pRight, SNode* pJoinCond) {
  SJoinTableNode* joinTable = (SJoinTableNode*)nodesMakeNode(QUERY_NODE_JOIN_TABLE);
  CHECK_OUT_OF_MEM(joinTable);
  joinTable->joinType = type;
  joinTable->pLeft = pLeft;
  joinTable->pRight = pRight;
  joinTable->pOnCond = pJoinCond;
  return (SNode*)joinTable;
}

SNode* createLimitNode(SAstCreateContext* pCxt, const SToken* pLimit, const SToken* pOffset) {
  SLimitNode* limitNode = (SLimitNode*)nodesMakeNode(QUERY_NODE_LIMIT);
  CHECK_OUT_OF_MEM(limitNode);
  // limitNode->limit = limit;
  // limitNode->offset = offset;
  return (SNode*)limitNode;
}

SNode* createOrderByExprNode(SAstCreateContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder) {
  SOrderByExprNode* orderByExpr = (SOrderByExprNode*)nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
  CHECK_OUT_OF_MEM(orderByExpr);
  orderByExpr->pExpr = pExpr;
  orderByExpr->order = order;
  if (NULL_ORDER_DEFAULT == nullOrder) {
    nullOrder = (ORDER_ASC == order ? NULL_ORDER_FIRST : NULL_ORDER_LAST);
  }
  orderByExpr->nullOrder = nullOrder;
  return (SNode*)orderByExpr;
}

SNode* createSessionWindowNode(SAstCreateContext* pCxt, SNode* pCol, const SToken* pVal) {
  SSessionWindowNode* session = (SSessionWindowNode*)nodesMakeNode(QUERY_NODE_SESSION_WINDOW);
  CHECK_OUT_OF_MEM(session);
  session->pCol = pCol;
  // session->gap = getInteger(pVal);
  return (SNode*)session;
}

SNode* createStateWindowNode(SAstCreateContext* pCxt, SNode* pCol) {
  SStateWindowNode* state = (SStateWindowNode*)nodesMakeNode(QUERY_NODE_STATE_WINDOW);
  CHECK_OUT_OF_MEM(state);
  state->pCol = pCol;
  return (SNode*)state;
}

SNode* createIntervalWindowNode(SAstCreateContext* pCxt, SNode* pInterval, SNode* pOffset, SNode* pSliding, SNode* pFill) {
  SIntervalWindowNode* interval = (SIntervalWindowNode*)nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW);
  CHECK_OUT_OF_MEM(interval);
  interval->pInterval = pInterval;
  interval->pOffset = pOffset;
  interval->pSliding = pSliding;
  interval->pFill = pFill;
  return (SNode*)interval;
}

SNode* createFillNode(SAstCreateContext* pCxt, EFillMode mode, SNode* pValues) {
  SFillNode* fill = (SFillNode*)nodesMakeNode(QUERY_NODE_FILL);
  CHECK_OUT_OF_MEM(fill);
  fill->mode = mode;
  fill->pValues = pValues;
  return (SNode*)fill;
}

SNode* createGroupingSetNode(SAstCreateContext* pCxt, SNode* pNode) {
  SGroupingSetNode* groupingSet = (SGroupingSetNode*)nodesMakeNode(QUERY_NODE_GROUPING_SET);
  CHECK_OUT_OF_MEM(groupingSet);
  groupingSet->groupingSetType = GP_TYPE_NORMAL;
  groupingSet->pParameterList = nodesMakeList();
  nodesListAppend(groupingSet->pParameterList, pNode);
  return (SNode*)groupingSet;
}

SNode* setProjectionAlias(SAstCreateContext* pCxt, SNode* pNode, const SToken* pAlias) {
  strncpy(((SExprNode*)pNode)->aliasName, pAlias->z, pAlias->n);
  return pNode;
}

SNode* addWhereClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWhere) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWhere = pWhere;
  }
  return pStmt;
}

SNode* addPartitionByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pPartitionByList) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pPartitionByList = pPartitionByList;
  }
  return pStmt;
}

SNode* addWindowClauseClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWindow) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWindow = pWindow;
  }
  return pStmt;
}

SNode* addGroupByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pGroupByList) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pGroupByList = pGroupByList;
  }
  return pStmt;
}

SNode* addHavingClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pHaving) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pHaving = pHaving;
  }
  return pStmt;
}

SNode* addOrderByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pOrderByList) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pOrderByList = pOrderByList;
  }
  return pStmt;
}

SNode* addSlimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pSlimit) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pSlimit = pSlimit;
  }
  return pStmt;
}

SNode* addLimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pLimit) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pLimit = pLimit;
  }
  return pStmt;
}

SNode* createSelectStmt(SAstCreateContext* pCxt, bool isDistinct, SNodeList* pProjectionList, SNode* pTable) {
  SSelectStmt* select = (SSelectStmt*)nodesMakeNode(QUERY_NODE_SELECT_STMT);
  CHECK_OUT_OF_MEM(select);
  select->isDistinct = isDistinct;
  select->pProjectionList = pProjectionList;
  select->pFromTable = pTable;
  return (SNode*)select;
}

SNode* createSetOperator(SAstCreateContext* pCxt, ESetOperatorType type, SNode* pLeft, SNode* pRight) {
  SSetOperator* setOp = (SSetOperator*)nodesMakeNode(QUERY_NODE_SET_OPERATOR);
  CHECK_OUT_OF_MEM(setOp);
  setOp->opType = type;
  setOp->pLeft = pLeft;
  setOp->pRight = pRight;
  return (SNode*)setOp;
}

SNode* createShowStmt(SAstCreateContext* pCxt, EShowStmtType type) {
  SShowStmt* show = (SShowStmt*)nodesMakeNode(QUERY_NODE_SHOW_STMT);
  CHECK_OUT_OF_MEM(show);
  show->showType = type;
  return (SNode*)show;
}
