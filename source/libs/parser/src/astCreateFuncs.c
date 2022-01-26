
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

#include "astCreateContext.h"

bool checkTableName(const SToken* pTableName) {
  printf("%p : %d, %d, %s\n", pTableName, pTableName->type, pTableName->n, pTableName->z);
  return pTableName->n < TSDB_TABLE_NAME_LEN ? true : false;
}

SNodeList* addNodeToList(SAstCreateContext* pCxt, SNodeList* pList, SNode* pNode) {

}

SNode* addOrderByList(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pOrderByList) {

}

SNode* addSlimit(SAstCreateContext* pCxt, SNode* pStmt, SNode* pSlimit) {

}

SNode* addLimit(SAstCreateContext* pCxt, SNode* pStmt, SNode* pLimit) {

}

SNode* createColumnNode(SAstCreateContext* pCxt, const SToken* pTableName, const SToken* pColumnName) {

}

SNode* createLimitNode(SAstCreateContext* pCxt, const SToken* pLimit, const SToken* pOffset) {

}

SNodeList* createNodeList(SAstCreateContext* pCxt, SNode* pNode) {

}

SNode* createOrderByExprNode(SAstCreateContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder) {

}

SNode* createRealTableNode(SAstCreateContext* pCxt, const SToken* pDbName, const SToken* pTableName) {
  SRealTableNode* realTable = (SRealTableNode*)nodesMakeNode(QUERY_NODE_REAL_TABLE);
  if (NULL != pDbName) {
    printf("DbName %p : %d, %d, %s\n", pDbName, pDbName->type, pDbName->n, pDbName->z);
    strncpy(realTable->dbName, pDbName->z, pDbName->n);
  }
  printf("TableName %p : %d, %d, %s\n", pTableName, pTableName->type, pTableName->n, pTableName->z);
  strncpy(realTable->table.tableName, pTableName->z, pTableName->n);
  return acquireRaii(pCxt, realTable);
}

SNode* createSelectStmt(SAstCreateContext* pCxt, bool isDistinct, SNodeList* pProjectionList, SNode* pTable) {
  SSelectStmt* select = (SSelectStmt*)nodesMakeNode(QUERY_NODE_SELECT_STMT);
  select->isDistinct = isDistinct;
  if (NULL == pProjectionList) {
    select->isStar = true;
  }
  select->pProjectionList = releaseRaii(pCxt, pProjectionList);
  printf("pTable = %p, name = %s\n", pTable, ((SRealTableNode*)pTable)->table.tableName);
  select->pFromTable = releaseRaii(pCxt, pTable);
  return acquireRaii(pCxt, select);
}

SNode* createSetOperator(SAstCreateContext* pCxt, ESetOperatorType type, SNode* pLeft, SNode* pRight) {

}

SNode* createShowStmt(SAstCreateContext* pCxt, EShowStmtType type) {

}

SNode* setProjectionAlias(SAstCreateContext* pCxt, SNode* pNode, const SToken* pAlias) {

}
