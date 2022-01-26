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
#include "nodesShowStmts.h"
#include "astCreateContext.h"
#include "ttoken.h"

#ifndef _TD_AST_CREATE_FUNCS_H_
#define _TD_AST_CREATE_FUNCS_H_

#ifdef __cplusplus
extern "C" {
#endif

bool checkTableName(const SToken* pTableName);
SNodeList* addNodeToList(SAstCreateContext* pCxt, SNodeList* pList, SNode* pNode);
SNode* addOrderByList(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pOrderByList);
SNode* addSlimit(SAstCreateContext* pCxt, SNode* pStmt, SNode* pSlimit);
SNode* addLimit(SAstCreateContext* pCxt, SNode* pStmt, SNode* pLimit);
SNode* createColumnNode(SAstCreateContext* pCxt, const SToken* pTableName, const SToken* pColumnName);
SNode* createLimitNode(SAstCreateContext* pCxt, const SToken* pLimit, const SToken* pOffset);
SNodeList* createNodeList(SAstCreateContext* pCxt, SNode* pNode);
SNode* createOrderByExprNode(SAstCreateContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder);
SNode* createRealTableNode(SAstCreateContext* pCxt, const SToken* pDbName, const SToken* pTableName);
SNode* createSelectStmt(SAstCreateContext* pCxt, bool isDistinct, SNodeList* pProjectionList, SNode* pTable);
SNode* createSetOperator(SAstCreateContext* pCxt, ESetOperatorType type, SNode* pLeft, SNode* pRight);
SNode* createShowStmt(SAstCreateContext* pCxt, EShowStmtType type);
SNode* setProjectionAlias(SAstCreateContext* pCxt, SNode* pNode, const SToken* pAlias);

#ifdef __cplusplus
}
#endif

#endif /*_TD_AST_CREATE_FUNCS_H_*/
