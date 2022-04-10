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
  DB_OPTION_BLOCKS = 1,
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
  DB_OPTION_SINGLE_STABLE,
  DB_OPTION_STREAM_MODE,
  DB_OPTION_RETENTIONS
} EDatabaseOptionType;

typedef enum ETableOptionType {
  TABLE_OPTION_KEEP = 1,
  TABLE_OPTION_TTL,
  TABLE_OPTION_COMMENT,
  TABLE_OPTION_SMA,
  TABLE_OPTION_FILE_FACTOR,
  TABLE_OPTION_DELAY
} ETableOptionType;

typedef struct SAlterOption {
  int32_t type;
  SValueNode* pVal;
  SNodeList* pList;
} SAlterOption;

extern SToken nil_token;

void initAstCreateContext(SParseContext* pParseCxt, SAstCreateContext* pCxt);

SNode* createRawExprNode(SAstCreateContext* pCxt, const SToken* pToken, SNode* pNode);
SNode* createRawExprNodeExt(SAstCreateContext* pCxt, const SToken* pStart, const SToken* pEnd, SNode* pNode);
SNode* releaseRawExprNode(SAstCreateContext* pCxt, SNode* pNode);
SToken getTokenFromRawExprNode(SAstCreateContext* pCxt, SNode* pNode);

SNodeList* createNodeList(SAstCreateContext* pCxt, SNode* pNode);
SNodeList* addNodeToList(SAstCreateContext* pCxt, SNodeList* pList, SNode* pNode);
SNodeList* addValueNodeFromTypeToList(SAstCreateContext* pCxt, SDataType dataType, SNodeList* pList);

SNode* createColumnNode(SAstCreateContext* pCxt, SToken* pTableAlias, SToken* pColumnName);
SNode* createValueNode(SAstCreateContext* pCxt, int32_t dataType, const SToken* pLiteral);
SNode* createDurationValueNode(SAstCreateContext* pCxt, const SToken* pLiteral);
SNode* createDefaultDatabaseCondValue(SAstCreateContext* pCxt);
SNode* setProjectionAlias(SAstCreateContext* pCxt, SNode* pNode, const SToken* pAlias);
SNode* createLogicConditionNode(SAstCreateContext* pCxt, ELogicConditionType type, SNode* pParam1, SNode* pParam2);
SNode* createOperatorNode(SAstCreateContext* pCxt, EOperatorType type, SNode* pLeft, SNode* pRight);
SNode* createBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight);
SNode* createNotBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight);
SNode* createFunctionNode(SAstCreateContext* pCxt, const SToken* pFuncName, SNodeList* pParameterList);
SNode* createNodeListNode(SAstCreateContext* pCxt, SNodeList* pList);
SNode* createNodeListNodeEx(SAstCreateContext* pCxt, SNode* p1, SNode* p2);
SNode* createRealTableNode(SAstCreateContext* pCxt, SToken* pDbName, SToken* pTableName, SToken* pTableAlias);
SNode* createTempTableNode(SAstCreateContext* pCxt, SNode* pSubquery, const SToken* pTableAlias);
SNode* createJoinTableNode(SAstCreateContext* pCxt, EJoinType type, SNode* pLeft, SNode* pRight, SNode* pJoinCond);
SNode* createLimitNode(SAstCreateContext* pCxt, const SToken* pLimit, const SToken* pOffset);
SNode* createOrderByExprNode(SAstCreateContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder);
SNode* createSessionWindowNode(SAstCreateContext* pCxt, SNode* pCol, SNode* pGap);
SNode* createStateWindowNode(SAstCreateContext* pCxt, SNode* pExpr);
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

SNode* createDatabaseOptions(SAstCreateContext* pCxt);
SNode* setDatabaseAlterOption(SAstCreateContext* pCxt, SNode* pOptions, SAlterOption* pAlterOption);
SNode* createCreateDatabaseStmt(SAstCreateContext* pCxt, bool ignoreExists, SToken* pDbName, SNode* pOptions);
SNode* createDropDatabaseStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pDbName);
SNode* createAlterDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName, SNode* pOptions);
SNode* createTableOptions(SAstCreateContext* pCxt);
SNode* setTableAlterOption(SAstCreateContext* pCxt, SNode* pOptions, SAlterOption* pAlterOption);
SNode* createColumnDefNode(SAstCreateContext* pCxt, const SToken* pColName, SDataType dataType, const SToken* pComment);
SDataType createDataType(uint8_t type);
SDataType createVarLenDataType(uint8_t type, const SToken* pLen);
SNode* createCreateTableStmt(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable, SNodeList* pCols, SNodeList* pTags, SNode* pOptions);
SNode* createCreateSubTableClause(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable, SNode* pUseRealTable, SNodeList* pSpecificTags, SNodeList* pValsOfTags);
SNode* createCreateMultiTableStmt(SAstCreateContext* pCxt, SNodeList* pSubTables);
SNode* createDropTableClause(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable);
SNode* createDropTableStmt(SAstCreateContext* pCxt, SNodeList* pTables);
SNode* createDropSuperTableStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable);
SNode* createAlterTableOption(SAstCreateContext* pCxt, SNode* pRealTable, SNode* pOptions);
SNode* createAlterTableAddModifyCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, const SToken* pColName, SDataType dataType);
SNode* createAlterTableDropCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, const SToken* pColName);
SNode* createAlterTableRenameCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, const SToken* pOldColName, const SToken* pNewColName);
SNode* createAlterTableSetTag(SAstCreateContext* pCxt, SNode* pRealTable, const SToken* pTagName, SNode* pVal);
SNode* createUseDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName);
SNode* createShowStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pDbName, SNode* pTbNamePattern);
SNode* createShowCreateDatabaseStmt(SAstCreateContext* pCxt, const SToken* pDbName);
SNode* createShowCreateTableStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pRealTable);
SNode* createCreateUserStmt(SAstCreateContext* pCxt, SToken* pUserName, const SToken* pPassword);
SNode* createAlterUserStmt(SAstCreateContext* pCxt, SToken* pUserName, int8_t alterType, const SToken* pVal);
SNode* createDropUserStmt(SAstCreateContext* pCxt, SToken* pUserName);
SNode* createCreateDnodeStmt(SAstCreateContext* pCxt, const SToken* pFqdn, const SToken* pPort);
SNode* createDropDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode);
SNode* createAlterDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode, const SToken* pConfig, const SToken* pValue);
SNode* createCreateIndexStmt(SAstCreateContext* pCxt, EIndexType type, bool ignoreExists, SToken* pIndexName, SToken* pTableName, SNodeList* pCols, SNode* pOptions);
SNode* createIndexOption(SAstCreateContext* pCxt, SNodeList* pFuncs, SNode* pInterval, SNode* pOffset, SNode* pSliding);
SNode* createDropIndexStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pIndexName, SToken* pTableName);
SNode* createCreateComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId);
SNode* createDropComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId);
SNode* createCreateTopicStmt(SAstCreateContext* pCxt, bool ignoreExists, const SToken* pTopicName, SNode* pQuery, const SToken* pSubscribeDbName);
SNode* createDropTopicStmt(SAstCreateContext* pCxt, bool ignoreNotExists, const SToken* pTopicName);
SNode* createAlterLocalStmt(SAstCreateContext* pCxt, const SToken* pConfig, const SToken* pValue);
SNode* createDefaultExplainOptions(SAstCreateContext* pCxt);
SNode* setExplainVerbose(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pVal);
SNode* setExplainRatio(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pVal);
SNode* createExplainStmt(SAstCreateContext* pCxt, bool analyze, SNode* pOptions, SNode* pQuery);
SNode* createDescribeStmt(SAstCreateContext* pCxt, SNode* pRealTable);
SNode* createResetQueryCacheStmt(SAstCreateContext* pCxt);
SNode* createCompactStmt(SAstCreateContext* pCxt, SNodeList* pVgroups);
SNode* createCreateFunctionStmt(SAstCreateContext* pCxt, bool aggFunc, const SToken* pFuncName, const SToken* pLibPath, SDataType dataType, int32_t bufSize);
SNode* createDropFunctionStmt(SAstCreateContext* pCxt, const SToken* pFuncName);
SNode* createCreateStreamStmt(SAstCreateContext* pCxt, const SToken* pStreamName, const SToken* pTableName, SNode* pQuery);
SNode* createDropStreamStmt(SAstCreateContext* pCxt, const SToken* pStreamName);
SNode* createKillStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pId);
SNode* createMergeVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId1, const SToken* pVgId2);
SNode* createRedistributeVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId, SNodeList* pDnodes);
SNode* createSplitVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId);
SNode* createSyncdbStmt(SAstCreateContext* pCxt, const SToken* pDbName);

#ifdef __cplusplus
}
#endif

#endif /*_TD_AST_CREATE_FUNCS_H_*/
