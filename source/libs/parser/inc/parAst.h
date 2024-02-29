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
#include "parToken.h"
#include "parUtil.h"
#include "parser.h"
#include "querynodes.h"

typedef struct SAstCreateContext {
  SParseContext* pQueryCxt;
  SMsgBuf        msgBuf;
  bool           notSupport;
  SNode*         pRootNode;
  int16_t        placeholderNo;
  SArray*        pPlaceholderValues;
  int32_t        errCode;
} SAstCreateContext;

typedef enum EDatabaseOptionType {
  DB_OPTION_BUFFER = 1,
  DB_OPTION_CACHEMODEL,
  DB_OPTION_CACHESIZE,
  DB_OPTION_COMP,
  DB_OPTION_DAYS,
  DB_OPTION_FSYNC,
  DB_OPTION_MAXROWS,
  DB_OPTION_MINROWS,
  DB_OPTION_KEEP,
  DB_OPTION_PAGES,
  DB_OPTION_PAGESIZE,
  DB_OPTION_TSDB_PAGESIZE,
  DB_OPTION_PRECISION,
  DB_OPTION_REPLICA,
  DB_OPTION_STRICT,
  DB_OPTION_WAL,
  DB_OPTION_VGROUPS,
  DB_OPTION_SINGLE_STABLE,
  DB_OPTION_RETENTIONS,
  DB_OPTION_SCHEMALESS,
  DB_OPTION_WAL_RETENTION_PERIOD,
  DB_OPTION_WAL_RETENTION_SIZE,
  DB_OPTION_WAL_ROLL_PERIOD,
  DB_OPTION_WAL_SEGMENT_SIZE,
  DB_OPTION_STT_TRIGGER,
  DB_OPTION_TABLE_PREFIX,
  DB_OPTION_TABLE_SUFFIX,
  DB_OPTION_KEEP_TIME_OFFSET
} EDatabaseOptionType;

typedef enum ETableOptionType {
  TABLE_OPTION_COMMENT = 1,
  TABLE_OPTION_MAXDELAY,
  TABLE_OPTION_WATERMARK,
  TABLE_OPTION_ROLLUP,
  TABLE_OPTION_TTL,
  TABLE_OPTION_SMA,
  TABLE_OPTION_DELETE_MARK
} ETableOptionType;

typedef struct SAlterOption {
  int32_t    type;
  SToken     val;
  SNodeList* pList;
} SAlterOption;

typedef struct STokenPair {
  SToken first;
  SToken second;
} STokenPair;

typedef struct SShowTablesOption {
  EShowKind kind;
  SToken dbName;
} SShowTablesOption;

extern SToken nil_token;

void initAstCreateContext(SParseContext* pParseCxt, SAstCreateContext* pCxt);

SNode* createRawExprNode(SAstCreateContext* pCxt, const SToken* pToken, SNode* pNode);
SNode* createRawExprNodeExt(SAstCreateContext* pCxt, const SToken* pStart, const SToken* pEnd, SNode* pNode);
SNode* setRawExprNodeIsPseudoColumn(SAstCreateContext* pCxt, SNode* pNode, bool isPseudoColumn);
SNode* releaseRawExprNode(SAstCreateContext* pCxt, SNode* pNode);
SToken getTokenFromRawExprNode(SAstCreateContext* pCxt, SNode* pNode);

SNodeList* createNodeList(SAstCreateContext* pCxt, SNode* pNode);
SNodeList* addNodeToList(SAstCreateContext* pCxt, SNodeList* pList, SNode* pNode);

SNode* createColumnNode(SAstCreateContext* pCxt, SToken* pTableAlias, SToken* pColumnName);
SNode* createValueNode(SAstCreateContext* pCxt, int32_t dataType, const SToken* pLiteral);
SNodeList* createHintNodeList(SAstCreateContext* pCxt, const SToken* pLiteral);
SNode* createIdentifierValueNode(SAstCreateContext* pCxt, SToken* pLiteral);
SNode* createDurationValueNode(SAstCreateContext* pCxt, const SToken* pLiteral);
SNode* createDefaultDatabaseCondValue(SAstCreateContext* pCxt);
SNode* createPlaceholderValueNode(SAstCreateContext* pCxt, const SToken* pLiteral);
SNode* setProjectionAlias(SAstCreateContext* pCxt, SNode* pNode, SToken* pAlias);
SNode* createLogicConditionNode(SAstCreateContext* pCxt, ELogicConditionType type, SNode* pParam1, SNode* pParam2);
SNode* createOperatorNode(SAstCreateContext* pCxt, EOperatorType type, SNode* pLeft, SNode* pRight);
SNode* createBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight);
SNode* createNotBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight);
SNode* createFunctionNode(SAstCreateContext* pCxt, const SToken* pFuncName, SNodeList* pParameterList);
SNode* createCastFunctionNode(SAstCreateContext* pCxt, SNode* pExpr, SDataType dt);
SNode* createNodeListNode(SAstCreateContext* pCxt, SNodeList* pList);
SNode* createNodeListNodeEx(SAstCreateContext* pCxt, SNode* p1, SNode* p2);
SNode* createRealTableNode(SAstCreateContext* pCxt, SToken* pDbName, SToken* pTableName, SToken* pTableAlias);
SNode* createTempTableNode(SAstCreateContext* pCxt, SNode* pSubquery, const SToken* pTableAlias);
SNode* createJoinTableNode(SAstCreateContext* pCxt, EJoinType type, SNode* pLeft, SNode* pRight, SNode* pJoinCond);
SNode* createViewNode(SAstCreateContext* pCxt, SToken* pDbName, SToken* pViewName);
SNode* createLimitNode(SAstCreateContext* pCxt, const SToken* pLimit, const SToken* pOffset);
SNode* createOrderByExprNode(SAstCreateContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder);
SNode* createSessionWindowNode(SAstCreateContext* pCxt, SNode* pCol, SNode* pGap);
SNode* createStateWindowNode(SAstCreateContext* pCxt, SNode* pExpr);
SNode* createEventWindowNode(SAstCreateContext* pCxt, SNode* pStartCond, SNode* pEndCond);
SNode* createCountWindowNode(SAstCreateContext* pCxt, const SToken* pCountToken, const SToken* pSlidingToken);
SNode* createIntervalWindowNode(SAstCreateContext* pCxt, SNode* pInterval, SNode* pOffset, SNode* pSliding,
                                SNode* pFill);
SNode* createFillNode(SAstCreateContext* pCxt, EFillMode mode, SNode* pValues);
SNode* createGroupingSetNode(SAstCreateContext* pCxt, SNode* pNode);
SNode* createInterpTimeRange(SAstCreateContext* pCxt, SNode* pStart, SNode* pEnd);
SNode* createInterpTimePoint(SAstCreateContext* pCxt, SNode* pPoint);
SNode* createWhenThenNode(SAstCreateContext* pCxt, SNode* pWhen, SNode* pThen);
SNode* createCaseWhenNode(SAstCreateContext* pCxt, SNode* pCase, SNodeList* pWhenThenList, SNode* pElse);

SNode* addWhereClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWhere);
SNode* addPartitionByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pPartitionByList);
SNode* addWindowClauseClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWindow);
SNode* addGroupByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pGroupByList);
SNode* addHavingClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pHaving);
SNode* addOrderByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pOrderByList);
SNode* addSlimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pSlimit);
SNode* addLimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pLimit);
SNode* addRangeClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pRange);
SNode* addEveryClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pEvery);
SNode* addFillClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pFill);
SNode* createSelectStmt(SAstCreateContext* pCxt, bool isDistinct, SNodeList* pProjectionList, SNode* pTable, SNodeList* pHint);
SNode* setSelectStmtTagMode(SAstCreateContext* pCxt, SNode* pStmt, bool bSelectTags);
SNode* createSetOperator(SAstCreateContext* pCxt, ESetOperatorType type, SNode* pLeft, SNode* pRight);

SDataType createDataType(uint8_t type);
SDataType createVarLenDataType(uint8_t type, const SToken* pLen);

SNode* createDefaultDatabaseOptions(SAstCreateContext* pCxt);
SNode* createAlterDatabaseOptions(SAstCreateContext* pCxt);
SNode* setDatabaseOption(SAstCreateContext* pCxt, SNode* pOptions, EDatabaseOptionType type, void* pVal);
SNode* setAlterDatabaseOption(SAstCreateContext* pCxt, SNode* pOptions, SAlterOption* pAlterOption);
SNode* createCreateDatabaseStmt(SAstCreateContext* pCxt, bool ignoreExists, SToken* pDbName, SNode* pOptions);
SNode* createDropDatabaseStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pDbName);
SNode* createAlterDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName, SNode* pOptions);
SNode* createFlushDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName);
SNode* createTrimDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName, int32_t maxSpeed);
SNode* createCompactStmt(SAstCreateContext* pCxt, SToken* pDbName, SNode* pStart, SNode* pEnd);
SNode* createDefaultTableOptions(SAstCreateContext* pCxt);
SNode* createAlterTableOptions(SAstCreateContext* pCxt);
SNode* setTableOption(SAstCreateContext* pCxt, SNode* pOptions, ETableOptionType type, void* pVal);
SNode* createColumnDefNode(SAstCreateContext* pCxt, SToken* pColName, SDataType dataType, const SToken* pComment);
SNode* createCreateTableStmt(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable, SNodeList* pCols,
                             SNodeList* pTags, SNode* pOptions);
SNode* createCreateSubTableClause(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable, SNode* pUseRealTable,
                                  SNodeList* pSpecificTags, SNodeList* pValsOfTags, SNode* pOptions);
SNode* createCreateMultiTableStmt(SAstCreateContext* pCxt, SNodeList* pSubTables);
SNode* createDropTableClause(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable);
SNode* createDropTableStmt(SAstCreateContext* pCxt, SNodeList* pTables);
SNode* createDropSuperTableStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable);
SNode* createAlterTableModifyOptions(SAstCreateContext* pCxt, SNode* pRealTable, SNode* pOptions);
SNode* createAlterTableAddModifyCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pColName,
                                    SDataType dataType);
SNode* createAlterTableDropCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pColName);
SNode* createAlterTableRenameCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pOldColName,
                                 SToken* pNewColName);
SNode* createAlterTableSetTag(SAstCreateContext* pCxt, SNode* pRealTable, SToken* pTagName, SNode* pVal);
SNode* setAlterSuperTableType(SNode* pStmt);
SNode* createUseDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName);
SNode* setShowKind(SAstCreateContext* pCxt, SNode* pStmt, EShowKind showKind);
SNode* createShowStmt(SAstCreateContext* pCxt, ENodeType type);
SNode* createShowStmtWithCond(SAstCreateContext* pCxt, ENodeType type, SNode* pDbName, SNode* pTbName,
                              EOperatorType tableCondType);
SNode* createShowTablesStmt(SAstCreateContext* pCxt, SShowTablesOption option, SNode* pTbName, EOperatorType tableCondType);                              
SNode* createShowCreateDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName);
SNode* createShowAliveStmt(SAstCreateContext* pCxt, SNode* pDbName, ENodeType type);
SNode* createShowCreateTableStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pRealTable);
SNode* createShowCreateViewStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pRealTable);
SNode* createShowTableDistributedStmt(SAstCreateContext* pCxt, SNode* pRealTable);
SNode* createShowDnodeVariablesStmt(SAstCreateContext* pCxt, SNode* pDnodeId, SNode* pLikePattern);
SNode* createShowVnodesStmt(SAstCreateContext* pCxt, SNode* pDnodeId, SNode* pDnodeEndpoint);
SNode* createShowTableTagsStmt(SAstCreateContext* pCxt, SNode* pTbName, SNode* pDbName, SNodeList* pTags);
SNode* createCreateUserStmt(SAstCreateContext* pCxt, SToken* pUserName, const SToken* pPassword, int8_t sysinfo);
SNode* addCreateUserStmtWhiteList(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pIpRangesNodeList);
SNode* createAlterUserStmt(SAstCreateContext* pCxt, SToken* pUserName, int8_t alterType, void* pAlterInfo);
SNode* createDropUserStmt(SAstCreateContext* pCxt, SToken* pUserName);
SNode* createCreateDnodeStmt(SAstCreateContext* pCxt, const SToken* pFqdn, const SToken* pPort);
SNode* createDropDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode, bool force, bool unsafe);
SNode* createAlterDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode, const SToken* pConfig, const SToken* pValue);
SNode* createRealTableNodeForIndexName(SAstCreateContext* pCxt, SToken* pDbName, SToken* pIndexName);
SNode* createCreateIndexStmt(SAstCreateContext* pCxt, EIndexType type, bool ignoreExists, SNode* pIndexName,
                             SNode* pRealTable, SNodeList* pCols, SNode* pOptions);
SNode* createIndexOption(SAstCreateContext* pCxt, SNodeList* pFuncs, SNode* pInterval, SNode* pOffset, SNode* pSliding,
                         SNode* pStreamOptions);
SNode* createDropIndexStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pIndexName);
SNode* createCreateComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId);
SNode* createDropComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId);
SNode* createRestoreComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId);
SNode* createCreateTopicStmtUseQuery(SAstCreateContext* pCxt, bool ignoreExists, SToken* pTopicName, SNode* pQuery);
SNode* createCreateTopicStmtUseDb(SAstCreateContext* pCxt, bool ignoreExists, SToken* pTopicName, SToken* pSubDbName,
                                  int8_t withMeta);
SNode* createCreateTopicStmtUseTable(SAstCreateContext* pCxt, bool ignoreExists, SToken* pTopicName, SNode* pRealTable,
                                     int8_t withMeta, SNode* pWhere);
SNode* createDropTopicStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pTopicName);
SNode* createDropCGroupStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pCGroupId, SToken* pTopicName);
SNode* createAlterClusterStmt(SAstCreateContext* pCxt, const SToken* pConfig, const SToken* pValue);
SNode* createAlterLocalStmt(SAstCreateContext* pCxt, const SToken* pConfig, const SToken* pValue);
SNode* createDefaultExplainOptions(SAstCreateContext* pCxt);
SNode* setExplainVerbose(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pVal);
SNode* setExplainRatio(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pVal);
SNode* createExplainStmt(SAstCreateContext* pCxt, bool analyze, SNode* pOptions, SNode* pQuery);
SNode* createDescribeStmt(SAstCreateContext* pCxt, SNode* pRealTable);
SNode* createResetQueryCacheStmt(SAstCreateContext* pCxt);
SNode* createCreateFunctionStmt(SAstCreateContext* pCxt, bool ignoreExists, bool aggFunc, const SToken* pFuncName,
                                const SToken* pLibPath, SDataType dataType, int32_t bufSize, const SToken* pLanguage,
                                bool orReplace);
SNode* createDropFunctionStmt(SAstCreateContext* pCxt, bool ignoreNotExists, const SToken* pFuncName);
SNode* createStreamOptions(SAstCreateContext* pCxt);
SNode* setStreamOptions(SAstCreateContext* pCxt, SNode* pOptions, EStreamOptionsSetFlag setflag, SToken* pToken,
                        SNode* pNode);
SNode* createCreateStreamStmt(SAstCreateContext* pCxt, bool ignoreExists, SToken* pStreamName, SNode* pRealTable,
                              SNode* pOptions, SNodeList* pTags, SNode* pSubtable, SNode* pQuery, SNodeList* pCols);
SNode* createDropStreamStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pStreamName);
SNode* createPauseStreamStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pStreamName);
SNode* createResumeStreamStmt(SAstCreateContext* pCxt, bool ignoreNotExists, bool ignoreUntreated, SToken* pStreamName);
SNode* createKillStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pId);
SNode* createKillQueryStmt(SAstCreateContext* pCxt, const SToken* pQueryId);
SNode* createBalanceVgroupStmt(SAstCreateContext* pCxt);
SNode* createBalanceVgroupLeaderStmt(SAstCreateContext* pCxt, const SToken* pVgId);
SNode* createMergeVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId1, const SToken* pVgId2);
SNode* createRedistributeVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId, SNodeList* pDnodes);
SNode* createSplitVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId);
SNode* createSyncdbStmt(SAstCreateContext* pCxt, const SToken* pDbName);
SNode* createGrantStmt(SAstCreateContext* pCxt, int64_t privileges, STokenPair* pPrivLevel, SToken* pUserName,
                       SNode* pTagCond);
SNode* createRevokeStmt(SAstCreateContext* pCxt, int64_t privileges, STokenPair* pPrivLevel, SToken* pUserName,
                        SNode* pTagCond);
SNode* createDeleteStmt(SAstCreateContext* pCxt, SNode* pTable, SNode* pWhere);
SNode* createInsertStmt(SAstCreateContext* pCxt, SNode* pTable, SNodeList* pCols, SNode* pQuery);
SNode* createCreateViewStmt(SAstCreateContext* pCxt, bool orReplace, SNode* pView, const SToken* pAs, SNode* pQuery);
SNode* createDropViewStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pView);
SNode* createShowCompactDetailsStmt(SAstCreateContext* pCxt, SNode* pCompactIdNode);
SNode* createShowCompactsStmt(SAstCreateContext* pCxt, ENodeType type);
#ifdef __cplusplus
}
#endif

#endif /*_TD_AST_CREATE_FUNCS_H_*/
