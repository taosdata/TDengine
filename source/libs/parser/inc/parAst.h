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

#ifndef _TD_AST_CREATE_FUNCS_H_
#define _TD_AST_CREATE_FUNCS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "cmdnodes.h"
#include "parser.h"
#include "parToken.h"
#include "parUtil.h"
#include "querynodes.h"

typedef struct SAstCreateContext {
  SParseContext* pQueryCxt;
  SMsgBuf msgBuf;
  bool notSupport;
  bool valid;
  SNode* pRootNode;
} SAstCreateContext;

typedef enum EDatabaseOptionType {
  DB_OPTION_BLOCKS = 0,
  DB_OPTION_CACHE,
  DB_OPTION_CACHELAST,
  DB_OPTION_COMP,
  DB_OPTION_DAYS,
  DB_OPTION_FSYNC,
  DB_OPTION_MAXROWS,
  DB_OPTION_MINROWS,
  DB_OPTION_KEEP,
  DB_OPTION_PRECISION,
  DB_OPTION_QUORUM,
  DB_OPTION_REPLICA,
  DB_OPTION_TTL,
  DB_OPTION_WAL,
  DB_OPTION_VGROUPS,
  DB_OPTION_SINGLESTABLE,
  DB_OPTION_STREAMMODE,

  DB_OPTION_MAX
} EDatabaseOptionType;

typedef enum ETableOptionType {
  TABLE_OPTION_KEEP = 0,
  TABLE_OPTION_TTL,
  TABLE_OPTION_COMMENT,
  TABLE_OPTION_SMA,

  TABLE_OPTION_MAX
} ETableOptionType;

extern SToken nil_token;

void initAstCreateContext(SParseContext* pParseCxt, SAstCreateContext* pCxt);

SNode* createRawExprNode(SAstCreateContext* pCxt, const SToken* pToken, SNode* pNode);
SNode* createRawExprNodeExt(SAstCreateContext* pCxt, const SToken* pStart, const SToken* pEnd, SNode* pNode);
SNode* releaseRawExprNode(SAstCreateContext* pCxt, SNode* pNode);
SToken getTokenFromRawExprNode(SAstCreateContext* pCxt, SNode* pNode);

SNodeList* createNodeList(SAstCreateContext* pCxt, SNode* pNode);
SNodeList* addNodeToList(SAstCreateContext* pCxt, SNodeList* pList, SNode* pNode);

SNode* createColumnNode(SAstCreateContext* pCxt, const SToken* pTableAlias, const SToken* pColumnName);
SNode* createValueNode(SAstCreateContext* pCxt, int32_t dataType, const SToken* pLiteral);
SNode* createDurationValueNode(SAstCreateContext* pCxt, const SToken* pLiteral);
SNode* setProjectionAlias(SAstCreateContext* pCxt, SNode* pNode, const SToken* pAlias);
SNode* createLogicConditionNode(SAstCreateContext* pCxt, ELogicConditionType type, SNode* pParam1, SNode* pParam2);
SNode* createOperatorNode(SAstCreateContext* pCxt, EOperatorType type, SNode* pLeft, SNode* pRight);
SNode* createBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight);
SNode* createNotBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight);
SNode* createFunctionNode(SAstCreateContext* pCxt, const SToken* pFuncName, SNodeList* pParameterList);
SNode* createNodeListNode(SAstCreateContext* pCxt, SNodeList* pList);
SNode* createRealTableNode(SAstCreateContext* pCxt, const SToken* pDbName, const SToken* pTableName, const SToken* pTableAlias);
SNode* createTempTableNode(SAstCreateContext* pCxt, SNode* pSubquery, const SToken* pTableAlias);
SNode* createJoinTableNode(SAstCreateContext* pCxt, EJoinType type, SNode* pLeft, SNode* pRight, SNode* pJoinCond);
SNode* createLimitNode(SAstCreateContext* pCxt, const SToken* pLimit, const SToken* pOffset);
SNode* createOrderByExprNode(SAstCreateContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder);
SNode* createSessionWindowNode(SAstCreateContext* pCxt, SNode* pCol, const SToken* pVal);
SNode* createStateWindowNode(SAstCreateContext* pCxt, SNode* pCol);
SNode* createIntervalWindowNode(SAstCreateContext* pCxt, SNode* pInterval, SNode* pOffset, SNode* pSliding, SNode* pFill);
SNode* createFillNode(SAstCreateContext* pCxt, EFillMode mode, SNode* pValues);
SNode* createGroupingSetNode(SAstCreateContext* pCxt, SNode* pNode);

SNode* addWhereClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWhere);
SNode* addPartitionByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pPartitionByList);
SNode* addWindowClauseClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWindow);
SNode* addGroupByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pGroupByList);
SNode* addHavingClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pHaving);
SNode* addOrderByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pOrderByList);
SNode* addSlimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pSlimit);
SNode* addLimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pLimit);
SNode* createSelectStmt(SAstCreateContext* pCxt, bool isDistinct, SNodeList* pProjectionList, SNode* pTable);
SNode* createSetOperator(SAstCreateContext* pCxt, ESetOperatorType type, SNode* pLeft, SNode* pRight);

SNode* createDefaultDatabaseOptions(SAstCreateContext* pCxt);
SNode* setDatabaseOption(SAstCreateContext* pCxt, SNode* pOptions, EDatabaseOptionType type, const SToken* pVal);
SNode* createCreateDatabaseStmt(SAstCreateContext* pCxt, bool ignoreExists, const SToken* pDbName, SNode* pOptions);
SNode* createDropDatabaseStmt(SAstCreateContext* pCxt, bool ignoreNotExists, const SToken* pDbName);
SNode* createDefaultTableOptions(SAstCreateContext* pCxt);
SNode* setTableOption(SAstCreateContext* pCxt, SNode* pOptions, ETableOptionType type, const SToken* pVal);
SNode* setTableSmaOption(SAstCreateContext* pCxt, SNode* pOptions, SNodeList* pSma);
SNode* createColumnDefNode(SAstCreateContext* pCxt, const SToken* pColName, SDataType dataType, const SToken* pComment);
SDataType createDataType(uint8_t type);
SDataType createVarLenDataType(uint8_t type, const SToken* pLen);
SNode* createCreateTableStmt(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable, SNodeList* pCols, SNodeList* pTags, SNode* pOptions);
SNode* createCreateSubTableClause(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable, SNode* pUseRealTable, SNodeList* pSpecificTags, SNodeList* pValsOfTags);
SNode* createCreateMultiTableStmt(SAstCreateContext* pCxt, SNodeList* pSubTables);
SNode* createDropTableClause(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable);
SNode* createDropTableStmt(SAstCreateContext* pCxt, SNodeList* pTables);
SNode* createDropSuperTableStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable);
SNode* createUseDatabaseStmt(SAstCreateContext* pCxt, const SToken* pDbName);
SNode* createShowStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDbName);
SNode* createCreateUserStmt(SAstCreateContext* pCxt, const SToken* pUserName, const SToken* pPassword);
SNode* createAlterUserStmt(SAstCreateContext* pCxt, const SToken* pUserName, int8_t alterType, const SToken* pVal);
SNode* createDropUserStmt(SAstCreateContext* pCxt, const SToken* pUserName);
SNode* createCreateDnodeStmt(SAstCreateContext* pCxt, const SToken* pFqdn, const SToken* pPort);
SNode* createDropDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode);
SNode* createCreateIndexStmt(SAstCreateContext* pCxt, EIndexType type, const SToken* pIndexName, const SToken* pTableName, SNodeList* pCols, SNode* pOptions);
SNode* createIndexOption(SAstCreateContext* pCxt, SNodeList* pFuncs, SNode* pInterval, SNode* pOffset, SNode* pSliding);
SNode* createCreateQnodeStmt(SAstCreateContext* pCxt, const SToken* pDnodeId);

#ifdef __cplusplus
}
#endif

#endif /*_TD_AST_CREATE_FUNCS_H_*/
