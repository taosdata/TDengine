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
#include "parser.h"

typedef void* (*FMalloc)(size_t);
typedef void (*FFree)(void*);

typedef struct SAstCreaterContext {
  SParseContext* pQueryCxt;
  bool notSupport;
  bool valid;
  SNode* pRootNode;
  FMalloc mallocFunc;
  FFree freeFunc;
} SAstCreaterContext;

SNodeList* addNodeToList(SAstCreaterContext* pCxt, SNodeList* pList, SNode* pNode);
SNode* createColumnNode(SAstCreaterContext* pCxt, SToken* pDbName, SToken* pTableName, SToken* pColumnName);
SNodeList* createNodeList(SAstCreaterContext* pCxt, SNode* pNode);
SNode* createOrderByExprNode(SAstCreaterContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder);
SNode* createSelectStmt(SAstCreaterContext* pCxt, bool isDistinct, SNodeList* pProjectionList, SNode* pTable);
SNode* createSetOperator(SAstCreaterContext* pCxt, ESetOperatorType type, SNode* pLeft, SNode* pRight);
SNode* createShowStmt(SAstCreaterContext* pCxt, EShowStmtType type);
SNode* setProjectionAlias(SAstCreaterContext* pCxt, SNode* pNode, SToken* pAlias);
