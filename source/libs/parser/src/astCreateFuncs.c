
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

SNodeList* createNodeList(SAstCreateContext* pCxt, SNode* pNode) {
  SNodeList* list = nodesMakeList();
  CHECK_OUT_OF_MEM(list);
  return acquireRaii(pCxt, AST_CXT_RESOURCE_NODE_LIST, nodesListAppend(list, releaseRaii(pCxt, pNode)));
}

SNodeList* addNodeToList(SAstCreateContext* pCxt, SNodeList* pList, SNode* pNode) {
  return nodesListAppend(pList, releaseRaii(pCxt, pNode));
}

SNode* createColumnNode(SAstCreateContext* pCxt, const SToken* pTableName, const SToken* pColumnName) {
  if (!checkTableName(pCxt, pTableName) || !checkColumnName(pCxt, pColumnName)) {
    return NULL;
  }
  SColumnNode* col = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  CHECK_OUT_OF_MEM(col);
  if (NULL != pTableName) {
    strncpy(col->tableName, pTableName->z, pTableName->n);
  }
  strncpy(col->colName, pColumnName->z, pColumnName->n);
  return acquireRaii(pCxt, AST_CXT_RESOURCE_NODE, col);
}

SNode* createValueNode(SAstCreateContext* pCxt, int32_t dataType, const SToken* pLiteral) {
  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
  // strncpy(col->colName, pColumnName->z, pColumnName->n);
  return acquireRaii(pCxt, AST_CXT_RESOURCE_NODE, val);
}

SNode* addMinusSign(SAstCreateContext* pCxt, SNode* pNode) {
  // todo
}

SNode* createLogicConditionNode(SAstCreateContext* pCxt, ELogicConditionType type, SNode* pParam1, SNode* pParam2) {
  SLogicConditionNode* cond = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  CHECK_OUT_OF_MEM(cond);
  cond->condType = type;
  cond->pParameterList = nodesMakeList();
  nodesListAppend(cond->pParameterList, releaseRaii(pCxt, pParam1));
  nodesListAppend(cond->pParameterList, releaseRaii(pCxt, pParam2));
  return acquireRaii(pCxt, AST_CXT_RESOURCE_NODE, cond);
}

SNode* createRealTableNode(SAstCreateContext* pCxt, const SToken* pDbName, const SToken* pTableName, const SToken* pTableAlias) {
  if (!checkDbName(pCxt, pDbName) || !checkTableName(pCxt, pTableName)) {
    return NULL;
  }
  SRealTableNode* realTable = (SRealTableNode*)nodesMakeNode(QUERY_NODE_REAL_TABLE);
  CHECK_OUT_OF_MEM(realTable);
  if (NULL != pDbName) {
    strncpy(realTable->dbName, pDbName->z, pDbName->n);
  }
  strncpy(realTable->table.tableName, pTableName->z, pTableName->n);
  return acquireRaii(pCxt, AST_CXT_RESOURCE_NODE, realTable);
}

SNode* createTempTableNode(SAstCreateContext* pCxt, SNode* pSubquery, const SToken* pTableAlias) {
  STempTableNode* tempTable = (STempTableNode*)nodesMakeNode(QUERY_NODE_TEMP_TABLE);
  CHECK_OUT_OF_MEM(tempTable);
  tempTable->pSubquery = pSubquery;
  return acquireRaii(pCxt, AST_CXT_RESOURCE_NODE, tempTable);
}

SNode* createJoinTableNode(SAstCreateContext* pCxt, EJoinType type, SNode* pLeft, SNode* pRight, SNode* pJoinCond) {
  SJoinTableNode* joinTable = (SJoinTableNode*)nodesMakeNode(QUERY_NODE_JOIN_TABLE);
  CHECK_OUT_OF_MEM(joinTable);
  joinTable->joinType = type;
  joinTable->pLeft = releaseRaii(pCxt, pLeft);
  joinTable->pRight = releaseRaii(pCxt, pRight);
  joinTable->pOnCond = pJoinCond;
  return acquireRaii(pCxt, AST_CXT_RESOURCE_NODE, joinTable);
}

SNode* createLimitNode(SAstCreateContext* pCxt, SNode* pLimit, SNode* pOffset) {
  SLimitNode* limitNode = (SLimitNode*)nodesMakeNode(QUERY_NODE_LIMIT);
  CHECK_OUT_OF_MEM(limitNode);
  // limitNode->limit = limit;
  // limitNode->offset = offset;
  return acquireRaii(pCxt, AST_CXT_RESOURCE_NODE, limitNode);
}

SNode* createOrderByExprNode(SAstCreateContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder) {
  SOrderByExprNode* orderByExpr = (SOrderByExprNode*)nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
  CHECK_OUT_OF_MEM(orderByExpr);
  orderByExpr->pExpr = releaseRaii(pCxt, pExpr);
  orderByExpr->order = order;
  orderByExpr->nullOrder = nullOrder;
  return acquireRaii(pCxt, AST_CXT_RESOURCE_NODE, orderByExpr);
}

SNode* setProjectionAlias(SAstCreateContext* pCxt, SNode* pNode, const SToken* pAlias) {
  strncpy(((SExprNode*)pNode)->aliasName, pAlias->z, pAlias->n);
  return pNode;
}

SNode* addWhereClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWhere) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWhere = releaseRaii(pCxt, pWhere);
  }
  return pStmt;
}

SNode* addPartitionByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pPartitionByList) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pPartitionByList = releaseRaii(pCxt, pPartitionByList);
  }
  return pStmt;
}

SNode* addWindowClauseClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWindow) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWindow = releaseRaii(pCxt, pWindow);
  }
  return pStmt;
}

SNode* addGroupByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pGroupByList) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pGroupByList = releaseRaii(pCxt, pGroupByList);
  }
  return pStmt;
}

SNode* addHavingClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pHaving) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pHaving = releaseRaii(pCxt, pHaving);
  }
  return pStmt;
}

SNode* addOrderByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pOrderByList) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pOrderByList = releaseRaii(pCxt, pOrderByList);
  }
  return pStmt;
}

SNode* addSlimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pSlimit) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pSlimit = releaseRaii(pCxt, pSlimit);
  }
  return pStmt;
}

SNode* addLimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pLimit) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pLimit = releaseRaii(pCxt, pLimit);
  }
  return pStmt;
}

SNode* createSelectStmt(SAstCreateContext* pCxt, bool isDistinct, SNodeList* pProjectionList, SNode* pTable) {
  SSelectStmt* select = (SSelectStmt*)nodesMakeNode(QUERY_NODE_SELECT_STMT);
  CHECK_OUT_OF_MEM(select);
  select->isDistinct = isDistinct;
  if (NULL == pProjectionList) {
    select->isStar = true;
  }
  select->pProjectionList = releaseRaii(pCxt, pProjectionList);
  select->pFromTable = releaseRaii(pCxt, pTable);
  return acquireRaii(pCxt, AST_CXT_RESOURCE_NODE, select);
}

SNode* createSetOperator(SAstCreateContext* pCxt, ESetOperatorType type, SNode* pLeft, SNode* pRight) {
  SSetOperator* setOp = (SSetOperator*)nodesMakeNode(QUERY_NODE_SET_OPERATOR);
  CHECK_OUT_OF_MEM(setOp);
  setOp->opType = type;
  setOp->pLeft = releaseRaii(pCxt, pLeft);
  setOp->pRight = releaseRaii(pCxt, pRight);
  return acquireRaii(pCxt, AST_CXT_RESOURCE_NODE, setOp);
}

SNode* createShowStmt(SAstCreateContext* pCxt, EShowStmtType type) {
  SShowStmt* show = (SShowStmt*)nodesMakeNode(QUERY_NODE_SHOW_STMT);
  CHECK_OUT_OF_MEM(show);
  show->showType = type;
  return acquireRaii(pCxt, AST_CXT_RESOURCE_NODE, show);
}
