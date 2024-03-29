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

#ifndef _TD_CMD_NODES_H_
#define _TD_CMD_NODES_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "query.h"
#include "querynodes.h"

#define DESCRIBE_RESULT_COLS      4
#define DESCRIBE_RESULT_FIELD_LEN (TSDB_COL_NAME_LEN - 1 + VARSTR_HEADER_SIZE)
#define DESCRIBE_RESULT_TYPE_LEN  (20 + VARSTR_HEADER_SIZE)
#define DESCRIBE_RESULT_NOTE_LEN  (16 + VARSTR_HEADER_SIZE)

#define SHOW_CREATE_DB_RESULT_COLS       2
#define SHOW_CREATE_DB_RESULT_FIELD1_LEN (TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE)
#define SHOW_CREATE_DB_RESULT_FIELD2_LEN (TSDB_MAX_BINARY_LEN + VARSTR_HEADER_SIZE)

#define SHOW_CREATE_TB_RESULT_COLS       2
#define SHOW_CREATE_TB_RESULT_FIELD1_LEN (TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE)
#define SHOW_CREATE_TB_RESULT_FIELD2_LEN (TSDB_MAX_ALLOWED_SQL_LEN * 3)

#define SHOW_CREATE_VIEW_RESULT_COLS       2
#define SHOW_CREATE_VIEW_RESULT_FIELD1_LEN (TSDB_VIEW_FNAME_LEN + 4 + VARSTR_HEADER_SIZE)
#define SHOW_CREATE_VIEW_RESULT_FIELD2_LEN (TSDB_MAX_ALLOWED_SQL_LEN + VARSTR_HEADER_SIZE)


#define SHOW_LOCAL_VARIABLES_RESULT_COLS       3
#define SHOW_LOCAL_VARIABLES_RESULT_FIELD1_LEN (TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE)
#define SHOW_LOCAL_VARIABLES_RESULT_FIELD2_LEN (TSDB_CONFIG_VALUE_LEN + VARSTR_HEADER_SIZE)
#define SHOW_LOCAL_VARIABLES_RESULT_FIELD3_LEN (TSDB_CONFIG_SCOPE_LEN + VARSTR_HEADER_SIZE)

#define COMPACT_DB_RESULT_COLS 3
#define COMPACT_DB_RESULT_FIELD1_LEN 32
#define COMPACT_DB_RESULT_FIELD3_LEN 128

#define SHOW_ALIVE_RESULT_COLS 1

#define BIT_FLAG_MASK(n)              (1 << n)
#define BIT_FLAG_SET_MASK(val, mask)  ((val) |= (mask))
#define BIT_FLAG_TEST_MASK(val, mask) (((val) & (mask)) != 0)

#define PRIVILEGE_TYPE_ALL       BIT_FLAG_MASK(0)
#define PRIVILEGE_TYPE_READ      BIT_FLAG_MASK(1)
#define PRIVILEGE_TYPE_WRITE     BIT_FLAG_MASK(2)
#define PRIVILEGE_TYPE_SUBSCRIBE BIT_FLAG_MASK(3)
#define PRIVILEGE_TYPE_ALTER     BIT_FLAG_MASK(4)

typedef struct SDatabaseOptions {
  ENodeType   type;
  int32_t     buffer;
  char        cacheModelStr[TSDB_CACHE_MODEL_STR_LEN];
  int8_t      cacheModel;
  int32_t     cacheLastSize;
  int8_t      compressionLevel;
  int32_t     daysPerFile;
  SValueNode* pDaysPerFile;
  int32_t     fsyncPeriod;
  int32_t     maxRowsPerBlock;
  int32_t     minRowsPerBlock;
  SNodeList*  pKeep;
  int64_t     keep[3];
  int32_t     keepTimeOffset;
  int32_t     pages;
  int32_t     pagesize;
  int32_t     tsdbPageSize;
  char        precisionStr[3];
  int8_t      precision;
  int8_t      replica;
  char        strictStr[TSDB_DB_STRICT_STR_LEN];
  int8_t      strict;
  int8_t      walLevel;
  int32_t     numOfVgroups;
  int8_t      singleStable;
  SNodeList*  pRetentions;
  int8_t      schemaless;
  int32_t     walRetentionPeriod;
  int32_t     walRetentionSize;
  int32_t     walRollPeriod;
  int32_t     walSegmentSize;
  bool        walRetentionPeriodIsSet;
  bool        walRetentionSizeIsSet;
  bool        walRollPeriodIsSet;
  int32_t     sstTrigger;
  int32_t     tablePrefix;
  int32_t     tableSuffix;
} SDatabaseOptions;

typedef struct SCreateDatabaseStmt {
  ENodeType         type;
  char              dbName[TSDB_DB_NAME_LEN];
  bool              ignoreExists;
  SDatabaseOptions* pOptions;
} SCreateDatabaseStmt;

typedef struct SUseDatabaseStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
} SUseDatabaseStmt;

typedef struct SDropDatabaseStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  bool      ignoreNotExists;
} SDropDatabaseStmt;

typedef struct SAlterDatabaseStmt {
  ENodeType         type;
  char              dbName[TSDB_DB_NAME_LEN];
  SDatabaseOptions* pOptions;
} SAlterDatabaseStmt;

typedef struct SFlushDatabaseStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
} SFlushDatabaseStmt;

typedef struct STrimDatabaseStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  int32_t   maxSpeed;
} STrimDatabaseStmt;

typedef struct SCompactDatabaseStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  SNode*    pStart;
  SNode*    pEnd;
} SCompactDatabaseStmt;

typedef struct STableOptions {
  ENodeType  type;
  bool       commentNull;
  char       comment[TSDB_TB_COMMENT_LEN];
  SNodeList* pMaxDelay;
  int64_t    maxDelay1;
  int64_t    maxDelay2;
  SNodeList* pWatermark;
  int64_t    watermark1;
  int64_t    watermark2;
  SNodeList* pDeleteMark;
  int64_t    deleteMark1;
  int64_t    deleteMark2;
  SNodeList* pRollupFuncs;
  int32_t    ttl;
  SNodeList* pSma;
} STableOptions;

typedef struct SColumnDefNode {
  ENodeType type;
  char      colName[TSDB_COL_NAME_LEN];
  SDataType dataType;
  char      comments[TSDB_TB_COMMENT_LEN];
  bool      sma;
} SColumnDefNode;

typedef struct SCreateTableStmt {
  ENodeType      type;
  char           dbName[TSDB_DB_NAME_LEN];
  char           tableName[TSDB_TABLE_NAME_LEN];
  bool           ignoreExists;
  SNodeList*     pCols;
  SNodeList*     pTags;
  STableOptions* pOptions;
} SCreateTableStmt;

typedef struct SCreateSubTableClause {
  ENodeType      type;
  char           dbName[TSDB_DB_NAME_LEN];
  char           tableName[TSDB_TABLE_NAME_LEN];
  char           useDbName[TSDB_DB_NAME_LEN];
  char           useTableName[TSDB_TABLE_NAME_LEN];
  bool           ignoreExists;
  SNodeList*     pSpecificTags;
  SNodeList*     pValsOfTags;
  STableOptions* pOptions;
} SCreateSubTableClause;

typedef struct SCreateMultiTablesStmt {
  ENodeType  type;
  SNodeList* pSubTables;
} SCreateMultiTablesStmt;

typedef struct SDropTableClause {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      tableName[TSDB_TABLE_NAME_LEN];
  bool      ignoreNotExists;
} SDropTableClause;

typedef struct SDropTableStmt {
  ENodeType  type;
  SNodeList* pTables;
} SDropTableStmt;

typedef struct SDropSuperTableStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      tableName[TSDB_TABLE_NAME_LEN];
  bool      ignoreNotExists;
} SDropSuperTableStmt;

typedef struct SAlterTableStmt {
  ENodeType      type;
  char           dbName[TSDB_DB_NAME_LEN];
  char           tableName[TSDB_TABLE_NAME_LEN];
  int8_t         alterType;
  char           colName[TSDB_COL_NAME_LEN];
  char           newColName[TSDB_COL_NAME_LEN];
  STableOptions* pOptions;
  SDataType      dataType;
  SValueNode*    pVal;
} SAlterTableStmt;

typedef struct SCreateUserStmt {
  ENodeType type;
  char      userName[TSDB_USER_LEN];
  char      password[TSDB_USET_PASSWORD_LEN];
  int8_t    sysinfo;
  int32_t numIpRanges;
  SIpV4Range* pIpRanges;

  SNodeList* pNodeListIpRanges;
} SCreateUserStmt;

typedef struct SAlterUserStmt {
  ENodeType type;
  char      userName[TSDB_USER_LEN];
  int8_t    alterType;
  char      password[TSDB_USET_PASSWORD_LEN];
  int8_t    enable;
  int8_t    sysinfo;
  int32_t numIpRanges;
  SIpV4Range* pIpRanges;

  SNodeList* pNodeListIpRanges;
} SAlterUserStmt;

typedef struct SDropUserStmt {
  ENodeType type;
  char      userName[TSDB_USER_LEN];
} SDropUserStmt;

typedef struct SCreateDnodeStmt {
  ENodeType type;
  char      fqdn[TSDB_FQDN_LEN];
  int32_t   port;
} SCreateDnodeStmt;

typedef struct SDropDnodeStmt {
  ENodeType type;
  int32_t   dnodeId;
  char      fqdn[TSDB_FQDN_LEN];
  int32_t   port;
  bool      force;
  bool      unsafe;
} SDropDnodeStmt;

typedef struct SAlterDnodeStmt {
  ENodeType type;
  int32_t   dnodeId;
  char      config[TSDB_DNODE_CONFIG_LEN];
  char      value[TSDB_DNODE_VALUE_LEN];
} SAlterDnodeStmt;

typedef struct SShowStmt {
  ENodeType     type;
  SNode*        pDbName;  // SValueNode
  SNode*        pTbName;  // SValueNode
  EOperatorType tableCondType;
  EShowKind     showKind; // show databases: user/system, show tables: normal/child, others NULL
} SShowStmt;

typedef struct SShowCreateDatabaseStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      dbFName[TSDB_DB_FNAME_LEN];
  void*     pCfg;  // SDbCfgInfo
} SShowCreateDatabaseStmt;

typedef struct SShowAliveStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
} SShowAliveStmt;

typedef struct SShowCreateTableStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      tableName[TSDB_TABLE_NAME_LEN];
  void*     pDbCfg;     // SDbCfgInfo
  void*     pTableCfg;  // STableCfg
} SShowCreateTableStmt;

typedef struct SShowCreateViewStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      viewName[TSDB_VIEW_NAME_LEN];
  void*     pViewMeta;
} SShowCreateViewStmt;

typedef struct SShowTableDistributedStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      tableName[TSDB_TABLE_NAME_LEN];
} SShowTableDistributedStmt;

typedef struct SShowDnodeVariablesStmt {
  ENodeType type;
  SNode*    pDnodeId;
  SNode*    pLikePattern;
} SShowDnodeVariablesStmt;

typedef struct SShowVnodesStmt {
  ENodeType type;
  SNode*    pDnodeId;
  SNode*    pDnodeEndpoint;
} SShowVnodesStmt;

typedef struct SShowTableTagsStmt {
  ENodeType  type;
  SNode*     pDbName;  // SValueNode
  SNode*     pTbName;  // SValueNode
  SNodeList* pTags;
} SShowTableTagsStmt;

typedef struct SShowCompactsStmt {
  ENodeType type;
} SShowCompactsStmt;

typedef struct SShowCompactDetailsStmt {
  ENodeType type;
  SNode* pCompactId;
} SShowCompactDetailsStmt;

typedef enum EIndexType { INDEX_TYPE_SMA = 1, INDEX_TYPE_FULLTEXT, INDEX_TYPE_NORMAL } EIndexType;

typedef struct SIndexOptions {
  ENodeType  type;
  SNodeList* pFuncs;
  SNode*     pInterval;
  SNode*     pOffset;
  SNode*     pSliding;
  int8_t     tsPrecision;
  SNode*     pStreamOptions;
} SIndexOptions;

typedef struct SCreateIndexStmt {
  ENodeType       type;
  EIndexType      indexType;
  bool            ignoreExists;
  char            indexDbName[TSDB_DB_NAME_LEN];
  char            indexName[TSDB_INDEX_NAME_LEN];
  char            dbName[TSDB_DB_NAME_LEN];
  char            tableName[TSDB_TABLE_NAME_LEN];
  SNodeList*      pCols;
  SIndexOptions*  pOptions;
  SNode*          pPrevQuery;
  SMCreateSmaReq* pReq;
} SCreateIndexStmt;

typedef struct SDropIndexStmt {
  ENodeType type;
  bool      ignoreNotExists;
  char      indexDbName[TSDB_DB_NAME_LEN];
  char      indexName[TSDB_INDEX_NAME_LEN];
} SDropIndexStmt;

typedef struct SCreateComponentNodeStmt {
  ENodeType type;
  int32_t   dnodeId;
} SCreateComponentNodeStmt;

typedef struct SDropComponentNodeStmt {
  ENodeType type;
  int32_t   dnodeId;
} SDropComponentNodeStmt;

typedef struct SRestoreComponentNodeStmt {
  ENodeType type;
  int32_t   dnodeId;
} SRestoreComponentNodeStmt;

typedef struct SCreateTopicStmt {
  ENodeType type;
  char      topicName[TSDB_TOPIC_NAME_LEN];
  char      subDbName[TSDB_DB_NAME_LEN];
  char      subSTbName[TSDB_TABLE_NAME_LEN];
  bool      ignoreExists;
  int8_t    withMeta;
  SNode*    pQuery;
  SNode*    pWhere;
} SCreateTopicStmt;

typedef struct SDropTopicStmt {
  ENodeType type;
  char      topicName[TSDB_TOPIC_NAME_LEN];
  bool      ignoreNotExists;
} SDropTopicStmt;

typedef struct SDropCGroupStmt {
  ENodeType type;
  char      topicName[TSDB_TOPIC_NAME_LEN];
  char      cgroup[TSDB_CGROUP_LEN];
  bool      ignoreNotExists;
} SDropCGroupStmt;

typedef struct SAlterClusterStmt {
  ENodeType type;
  char      config[TSDB_DNODE_CONFIG_LEN];
  char      value[TSDB_CLUSTER_VALUE_LEN];
} SAlterClusterStmt;

typedef struct SAlterLocalStmt {
  ENodeType type;
  char      config[TSDB_DNODE_CONFIG_LEN];
  char      value[TSDB_DNODE_VALUE_LEN];
} SAlterLocalStmt;

typedef struct SDescribeStmt {
  ENodeType   type;
  char        dbName[TSDB_DB_NAME_LEN];
  char        tableName[TSDB_TABLE_NAME_LEN];
  STableMeta* pMeta;
} SDescribeStmt;

typedef struct SKillStmt {
  ENodeType type;
  int32_t   targetId;
} SKillStmt;

typedef struct SKillQueryStmt {
  ENodeType type;
  char      queryId[TSDB_QUERY_ID_LEN];
} SKillQueryStmt;

typedef enum EStreamOptionsSetFlag {
  SOPT_TRIGGER_TYPE_SET = BIT_FLAG_MASK(0),
  SOPT_WATERMARK_SET = BIT_FLAG_MASK(1),
  SOPT_DELETE_MARK_SET = BIT_FLAG_MASK(2),
  SOPT_FILL_HISTORY_SET = BIT_FLAG_MASK(3),
  SOPT_IGNORE_EXPIRED_SET = BIT_FLAG_MASK(4),
  SOPT_IGNORE_UPDATE_SET = BIT_FLAG_MASK(5),
} EStreamOptionsSetFlag;

typedef struct SStreamOptions {
  ENodeType type;
  int8_t    triggerType;
  SNode*    pDelay;
  SNode*    pWatermark;
  SNode*    pDeleteMark;
  int8_t    fillHistory;
  int8_t    ignoreExpired;
  int8_t    ignoreUpdate;
  int64_t   setFlag;
} SStreamOptions;

typedef struct SCreateStreamStmt {
  ENodeType           type;
  char                streamName[TSDB_TABLE_NAME_LEN];
  char                targetDbName[TSDB_DB_NAME_LEN];
  char                targetTabName[TSDB_TABLE_NAME_LEN];
  bool                ignoreExists;
  SStreamOptions*     pOptions;
  SNode*              pQuery;
  SNode*              pPrevQuery;
  SNodeList*          pTags;
  SNode*              pSubtable;
  SNodeList*          pCols;
  SCMCreateStreamReq* pReq;
} SCreateStreamStmt;

typedef struct SDropStreamStmt {
  ENodeType type;
  char      streamName[TSDB_TABLE_NAME_LEN];
  bool      ignoreNotExists;
} SDropStreamStmt;

typedef struct SPauseStreamStmt {
  ENodeType type;
  char      streamName[TSDB_TABLE_NAME_LEN];
  bool      ignoreNotExists;
} SPauseStreamStmt;

typedef struct SResumeStreamStmt {
  ENodeType type;
  char      streamName[TSDB_TABLE_NAME_LEN];
  bool      ignoreNotExists;
  bool      ignoreUntreated;
} SResumeStreamStmt;

typedef struct SCreateFunctionStmt {
  ENodeType type;
  bool      orReplace;
  bool      ignoreExists;
  char      funcName[TSDB_FUNC_NAME_LEN];
  bool      isAgg;
  char      libraryPath[PATH_MAX];
  SDataType outputDt;
  int32_t   bufSize;
  int8_t    language;
} SCreateFunctionStmt;

typedef struct SDropFunctionStmt {
  ENodeType type;
  char      funcName[TSDB_FUNC_NAME_LEN];
  bool      ignoreNotExists;
} SDropFunctionStmt;

typedef struct SCreateViewStmt {
  ENodeType           type;
  char                dbName[TSDB_DB_NAME_LEN];
  char                viewName[TSDB_VIEW_NAME_LEN];
  char*               pQuerySql;
  bool                orReplace;
  SNode*              pQuery;
  SCMCreateViewReq    createReq;
} SCreateViewStmt;

typedef struct SDropViewStmt {
  ENodeType  type;
  char       dbName[TSDB_DB_NAME_LEN];
  char       viewName[TSDB_VIEW_NAME_LEN];
  bool       ignoreNotExists;
} SDropViewStmt;

typedef struct SGrantStmt {
  ENodeType type;
  char      userName[TSDB_USER_LEN];
  char      objName[TSDB_DB_NAME_LEN];  // db or topic
  char      tabName[TSDB_TABLE_NAME_LEN];
  int64_t   privileges;
  SNode*    pTagCond;
} SGrantStmt;

typedef SGrantStmt SRevokeStmt;

typedef struct SBalanceVgroupStmt {
  ENodeType type;
} SBalanceVgroupStmt;

typedef struct SBalanceVgroupLeaderStmt {
  ENodeType type;
  int32_t   vgId;
} SBalanceVgroupLeaderStmt;

typedef struct SMergeVgroupStmt {
  ENodeType type;
  int32_t   vgId1;
  int32_t   vgId2;
} SMergeVgroupStmt;

typedef struct SRedistributeVgroupStmt {
  ENodeType  type;
  int32_t    vgId;
  int32_t    dnodeId1;
  int32_t    dnodeId2;
  int32_t    dnodeId3;
  SNodeList* pDnodes;
} SRedistributeVgroupStmt;

typedef struct SSplitVgroupStmt {
  ENodeType type;
  int32_t   vgId;
} SSplitVgroupStmt;

#ifdef __cplusplus
}
#endif

#endif /*_TD_CMD_NODES_H_*/
