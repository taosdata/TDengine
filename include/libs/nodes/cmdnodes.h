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

#define DESCRIBE_RESULT_COLS               4
#define DESCRIBE_RESULT_COLS_COMPRESS      7
#define DESCRIBE_RESULT_COLS_REF           5
#define DESCRIBE_RESULT_FIELD_LEN          (TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_COL_FNAME_LEN + VARSTR_HEADER_SIZE)
#define DESCRIBE_RESULT_TYPE_LEN           (20 + VARSTR_HEADER_SIZE)
#define DESCRIBE_RESULT_NOTE_LEN           (16 + VARSTR_HEADER_SIZE)
#define DESCRIBE_RESULT_COPRESS_OPTION_LEN (TSDB_CL_COMPRESS_OPTION_LEN + VARSTR_HEADER_SIZE)
#define DESCRIBE_RESULT_COL_REF_LEN         (TSDB_COL_FNAME_LEN + VARSTR_HEADER_SIZE)

#define SHOW_CREATE_DB_RESULT_COLS       2
#define SHOW_CREATE_DB_RESULT_FIELD1_LEN (TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE)
#define SHOW_CREATE_DB_RESULT_FIELD2_LEN (TSDB_MAX_BINARY_LEN + VARSTR_HEADER_SIZE)

#define SHOW_CREATE_TB_RESULT_COLS       2
#define SHOW_CREATE_TB_RESULT_FIELD1_LEN (TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE)
#define SHOW_CREATE_TB_RESULT_FIELD2_LEN (TSDB_MAX_ALLOWED_SQL_LEN * 3)

#define SHOW_CREATE_VIEW_RESULT_COLS       2
#define SHOW_CREATE_VIEW_RESULT_FIELD1_LEN (TSDB_VIEW_FNAME_LEN + 4 + VARSTR_HEADER_SIZE)
#define SHOW_CREATE_VIEW_RESULT_FIELD2_LEN (TSDB_MAX_ALLOWED_SQL_LEN + VARSTR_HEADER_SIZE)

#define SHOW_LOCAL_VARIABLES_RESULT_COLS       5
#define SHOW_LOCAL_VARIABLES_RESULT_FIELD1_LEN (TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE)
#define SHOW_LOCAL_VARIABLES_RESULT_FIELD2_LEN (TSDB_CONFIG_PATH_LEN + VARSTR_HEADER_SIZE)
#define SHOW_LOCAL_VARIABLES_RESULT_FIELD3_LEN (TSDB_CONFIG_SCOPE_LEN + VARSTR_HEADER_SIZE)
#define SHOW_LOCAL_VARIABLES_RESULT_FIELD4_LEN (TSDB_CONFIG_CATEGORY_LEN + VARSTR_HEADER_SIZE)
#define SHOW_LOCAL_VARIABLES_RESULT_FIELD5_LEN (TSDB_CONFIG_INFO_LEN + VARSTR_HEADER_SIZE)

#define COMPACT_DB_RESULT_COLS       3
#define COMPACT_DB_RESULT_FIELD1_LEN 32
#define COMPACT_DB_RESULT_FIELD3_LEN 128

#define SCAN_DB_RESULT_COLS       3
#define SCAN_DB_RESULT_FIELD1_LEN 32
#define SCAN_DB_RESULT_FIELD3_LEN 128

#define SHOW_ALIVE_RESULT_COLS 1

#define BIT_FLAG_MASK(n)               (1 << n)
#define BIT_FLAG_SET_MASK(val, mask)   ((val) |= (mask))
#define BIT_FLAG_UNSET_MASK(val, mask) ((val) &= ~(mask))
#define BIT_FLAG_TEST_MASK(val, mask)  (((val) & (mask)) != 0)

#define PRIVILEGE_TYPE_ALL       BIT_FLAG_MASK(0)
#define PRIVILEGE_TYPE_READ      BIT_FLAG_MASK(1)
#define PRIVILEGE_TYPE_WRITE     BIT_FLAG_MASK(2)
#define PRIVILEGE_TYPE_SUBSCRIBE BIT_FLAG_MASK(3)
#define PRIVILEGE_TYPE_ALTER     BIT_FLAG_MASK(4)

#define EVENT_NONE               0
#define EVENT_WINDOW_CLOSE       BIT_FLAG_MASK(0)
#define EVENT_WINDOW_OPEN        BIT_FLAG_MASK(1)

#define NOTIFY_NONE              0
#define NOTIFY_HISTORY           BIT_FLAG_MASK(0)
#define NOTIFY_ON_FAILURE_PAUSE  BIT_FLAG_MASK(1)
#define NOTIFY_HAS_FILTER        BIT_FLAG_MASK(2)
#define CALC_SLIDING_OVERLAP     BIT_FLAG_MASK(3)

typedef struct SDatabaseOptions {
  ENodeType   type;
  int32_t     buffer;
  char        cacheModelStr[TSDB_CACHE_MODEL_STR_LEN];
  int8_t      cacheModel;
  int32_t     cacheLastSize;
  int8_t      compressionLevel;
  int8_t      encryptAlgorithm;
  int32_t     daysPerFile;
  char        dnodeListStr[TSDB_DNODE_LIST_LEN];
  char        encryptAlgorithmStr[TSDB_ENCRYPT_ALGO_STR_LEN];
  SValueNode* pDaysPerFile;
  int32_t     fsyncPeriod;
  int32_t     maxRowsPerBlock;
  int32_t     minRowsPerBlock;
  SNodeList*  pKeep;
  int64_t     keep[3];
  SValueNode* pKeepTimeOffsetNode;
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
  int32_t     ssChunkSize;
  int32_t     ssKeepLocal;
  SValueNode* ssKeepLocalStr;
  int8_t      ssCompact;
  int8_t      withArbitrator;
  // for auto-compact
  int32_t     compactTimeOffset;  // hours
  int32_t     compactInterval;    // minutes
  int32_t     compactStartTime;   // minutes
  int32_t     compactEndTime;     // minutes
  SValueNode* pCompactTimeOffsetNode;
  SValueNode* pCompactIntervalNode;
  SNodeList*  pCompactTimeRangeList;
  // for cache
  SDbCfgInfo* pDbCfg;
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
  bool      force;
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

typedef struct SSsMigrateDatabaseStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
} SSsMigrateDatabaseStmt;

typedef struct SCompactDatabaseStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  SNode*    pStart;
  SNode*    pEnd;
  bool      metaOnly;
  bool      force;
} SCompactDatabaseStmt;

typedef struct SRollupDatabaseStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  SNode*    pStart;
  SNode*    pEnd;
} SRollupDatabaseStmt;

typedef struct SScanDatabaseStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  SNode*    pStart;
  SNode*    pEnd;
} SScanDatabaseStmt;

typedef struct SCreateMountStmt {
  ENodeType type;
  int32_t   dnodeId;
  bool      ignoreExists;
  char      mountName[TSDB_MOUNT_NAME_LEN];
  char      mountPath[TSDB_MOUNT_PATH_LEN];
} SCreateMountStmt;

typedef struct SDropMountStmt {
  ENodeType type;
  char      mountName[TSDB_MOUNT_NAME_LEN];
  bool      ignoreNotExists;
} SDropMountStmt;

typedef struct SCompactVgroupsStmt {
  ENodeType  type;
  SNode*     pDbName;
  SNodeList* vgidList;
  SNode*     pStart;
  SNode*     pEnd;
  bool       metaOnly;
  bool       force;
} SCompactVgroupsStmt;

typedef struct SRollupVgroupsStmt {
  ENodeType  type;
  SNode*     pDbName;
  SNodeList* vgidList;
  SNode*     pStart;
  SNode*     pEnd;
} SRollupVgroupsStmt;

typedef struct SScanVgroupsStmt {
  ENodeType  type;
  SNode*     pDbName;
  SNodeList* vgidList;
  SNode*     pStart;
  SNode*     pEnd;
} SScanVgroupsStmt;

typedef struct STableOptions {
  ENodeType  type;
  bool       virtualStb;
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
  SValueNode* pKeepNode;
  int32_t     keep;
} STableOptions;

typedef struct SColumnOptions {
  ENodeType type;
  bool      commentNull;
  char      comment[TSDB_CL_COMMENT_LEN];
  char      encode[TSDB_CL_COMPRESS_OPTION_LEN];
  char      compress[TSDB_CL_COMPRESS_OPTION_LEN];
  char      compressLevel[TSDB_CL_COMPRESS_OPTION_LEN];
  bool      bPrimaryKey;
  bool      hasRef;
  char      refDb[TSDB_DB_NAME_LEN];
  char      refTable[TSDB_TABLE_NAME_LEN];
  char      refColumn[TSDB_COL_NAME_LEN];
} SColumnOptions;

typedef struct SColumnDefNode {
  ENodeType type;
  char      colName[TSDB_COL_NAME_LEN];
  SDataType dataType;
  SNode*    pOptions;
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

typedef struct SCreateVTableStmt {
  ENodeType      type;
  char           dbName[TSDB_DB_NAME_LEN];
  char           tableName[TSDB_TABLE_NAME_LEN];
  bool           ignoreExists;
  SNodeList*     pCols;
} SCreateVTableStmt;

typedef struct SCreateVSubTableStmt {
  ENodeType      type;
  char           dbName[TSDB_DB_NAME_LEN];
  char           tableName[TSDB_TABLE_NAME_LEN];
  char           useDbName[TSDB_DB_NAME_LEN];
  char           useTableName[TSDB_TABLE_NAME_LEN];
  bool           ignoreExists;
  SNodeList*     pSpecificTags;
  SNodeList*     pValsOfTags;
  SNodeList*     pSpecificColRefs;
  SNodeList*     pColRefs;
} SCreateVSubTableStmt;

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

typedef struct SCreateSubTableFromFileClause {
  ENodeType  type;
  char       useDbName[TSDB_DB_NAME_LEN];
  char       useTableName[TSDB_TABLE_NAME_LEN];
  bool       ignoreExists;
  SNodeList* pSpecificTags;
  char       filePath[PATH_MAX];
} SCreateSubTableFromFileClause;

typedef struct SCreateMultiTablesStmt {
  ENodeType  type;
  SNodeList* pSubTables;
} SCreateMultiTablesStmt;

typedef struct SDropTableClause {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      tableName[TSDB_TABLE_NAME_LEN];
  bool      ignoreNotExists;
  SArray*   pTsmas;
} SDropTableClause;

typedef struct SDropTableStmt {
  ENodeType  type;
  SNodeList* pTables;
  bool       withTsma;
  bool       withOpt;
} SDropTableStmt;

typedef struct SDropSuperTableStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      tableName[TSDB_TABLE_NAME_LEN];
  bool      ignoreNotExists;
  bool      withOpt;
} SDropSuperTableStmt;

typedef struct SDropVirtualTableStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      tableName[TSDB_TABLE_NAME_LEN];
  bool      ignoreNotExists;
  bool      withOpt;
} SDropVirtualTableStmt;

typedef struct SAlterTableStmt {
  ENodeType       type;
  char            dbName[TSDB_DB_NAME_LEN];
  char            tableName[TSDB_TABLE_NAME_LEN];
  int8_t          alterType;
  char            colName[TSDB_COL_NAME_LEN];
  char            newColName[TSDB_COL_NAME_LEN];
  STableOptions*  pOptions;
  SDataType       dataType;
  SValueNode*     pVal;
  SColumnOptions* pColOptions;
  SNodeList*      pNodeListTagValue;
  char            refDbName[TSDB_DB_NAME_LEN];
  char            refTableName[TSDB_TABLE_NAME_LEN];
  char            refColName[TSDB_COL_NAME_LEN];
} SAlterTableStmt;

typedef struct SAlterTableMultiStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      tableName[TSDB_TABLE_NAME_LEN];
  int8_t    alterType;

  SNodeList* pNodeListTagValue;
} SAlterTableMultiStmt;

typedef struct SCreateUserStmt {
  ENodeType   type;
  char        userName[TSDB_USER_LEN];
  char        password[TSDB_USET_PASSWORD_LONGLEN];
  int8_t      sysinfo;
  int8_t      createDb;
  int8_t      isImport;
  int32_t     numIpRanges;
  SIpRange*   pIpRanges;

  SNodeList* pNodeListIpRanges;
} SCreateUserStmt;

typedef struct SAlterUserStmt {
  ENodeType   type;
  char        userName[TSDB_USER_LEN];
  int8_t      alterType;
  char        password[TSDB_USET_PASSWORD_LONGLEN];
  int8_t      enable;
  int8_t      sysinfo;
  int8_t      createdb;
  int32_t     numIpRanges;
  SIpRange*   pIpRanges;

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

typedef struct {
  ENodeType type;
  char      url[TSDB_ANALYTIC_ANODE_URL_LEN + 3];
} SCreateAnodeStmt;

typedef struct {
  ENodeType type;
  int32_t   anodeId;
} SDropAnodeStmt;

typedef struct {
  ENodeType type;
  int32_t   anodeId;
} SUpdateAnodeStmt;

typedef struct SBnodeOptions {
  ENodeType type;
  char      protoStr[TSDB_BNODE_OPT_PROTO_STR_LEN];
  int8_t    proto;
} SBnodeOptions;

typedef struct {
  ENodeType      type;
  int32_t        dnodeId;
  SBnodeOptions* pOptions;
} SCreateBnodeStmt;

typedef struct {
  ENodeType type;
  int32_t   dnodeId;
} SDropBnodeStmt;

typedef struct {
  ENodeType type;
  int32_t   dnodeId;
} SUpdateBnodeStmt;

typedef struct SShowStmt {
  ENodeType     type;
  SNode*        pDbName;  // SValueNode
  SNode*        pTbName;  // SValueNode
  EOperatorType tableCondType;
  EShowKind     showKind;  // show databases: user/system, show tables: normal/child, others NULL
  bool          withFull;  // for show users full;
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

typedef struct SShowCreateRsmaStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      rsmaName[TSDB_TABLE_NAME_LEN];
  void*     pRsmaMeta;  // SRsmaInfoRsp;
  void*     pTableCfg;  // STableCfg
} SShowCreateRsmaStmt;

typedef struct SShowTableDistributedStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      tableName[TSDB_TABLE_NAME_LEN];
} SShowTableDistributedStmt;

typedef struct SShowDBUsageStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
} SShowDBUsageStmt;

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

typedef struct SShowScansStmt {
  ENodeType type;
} SShowScansStmt;

typedef struct SShowCompactDetailsStmt {
  ENodeType type;
  SNode*    pId;
} SShowCompactDetailsStmt;

typedef SShowCompactDetailsStmt SShowRetentionDetailsStmt;

typedef struct SShowScanDetailsStmt {
  ENodeType type;
  SNode*    pScanId;
} SShowScanDetailsStmt;

typedef struct SShowSsMigratesStmt {
  ENodeType type;
} SShowSsMigratesStmt;

typedef struct SShowTransactionDetailsStmt {
  ENodeType type;
  SNode*    pTransactionId;
} SShowTransactionDetailsStmt;

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
  bool      force;
} SDropTopicStmt;

typedef struct SDropCGroupStmt {
  ENodeType type;
  char      topicName[TSDB_TOPIC_NAME_LEN];
  char      cgroup[TSDB_CGROUP_LEN];
  bool      ignoreNotExists;
  bool      force;
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

typedef enum EStreamNotifyEventType {
  SNOTIFY_EVENT_WINDOW_INVALIDATION = 0,
  SNOTIFY_EVENT_WINDOW_OPEN = BIT_FLAG_MASK(0),
  SNOTIFY_EVENT_WINDOW_CLOSE = BIT_FLAG_MASK(1),
} EStreamNotifyEventType;

typedef struct SStreamNotifyOptions {
  ENodeType                    type;
  SNodeList*                   pAddrUrls;
  SNode*                       pWhere;
  int64_t                      eventType;
  int64_t                      notifyType;
} SStreamNotifyOptions;

typedef struct SCreateStreamStmt {
  ENodeType             type;
  char                  streamDbName[TSDB_DB_NAME_LEN];
  char                  streamName[TSDB_TABLE_NAME_LEN];
  char                  targetDbName[TSDB_DB_NAME_LEN];
  char                  targetTabName[TSDB_TABLE_NAME_LEN];
  bool                  ignoreExists;
  SNode*                pTrigger; // SStreamTriggerNode
  SNode*                pQuery;
  SNode*                pSubtable;
  SNodeList*            pTags; // SStreamTagDefNode
  SNodeList*            pCols; // SColumnDefNode
} SCreateStreamStmt;

typedef struct SDropStreamStmt {
  ENodeType type;
  char      streamDbName[TSDB_DB_NAME_LEN];
  char      streamName[TSDB_TABLE_NAME_LEN];
  bool      ignoreNotExists;
} SDropStreamStmt;

typedef struct SPauseStreamStmt {
  ENodeType type;
  char      streamDbName[TSDB_DB_NAME_LEN];
  char      streamName[TSDB_TABLE_NAME_LEN];
  bool      ignoreNotExists;
} SPauseStreamStmt;

typedef struct SResumeStreamStmt {
  ENodeType type;
  char      streamDbName[TSDB_DB_NAME_LEN];
  char      streamName[TSDB_TABLE_NAME_LEN];
  bool      ignoreNotExists;
  bool      ignoreUntreated;
} SResumeStreamStmt;

typedef struct SRecalcStreamStmt {
  ENodeType type;
  char      streamDbName[TSDB_DB_NAME_LEN];
  char      streamName[TSDB_TABLE_NAME_LEN];
  SNode*    pRange;
} SRecalcStreamStmt;

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
  ENodeType        type;
  char             dbName[TSDB_DB_NAME_LEN];
  char             viewName[TSDB_VIEW_NAME_LEN];
  char*            pQuerySql;
  bool             orReplace;
  SNode*           pQuery;
  SCMCreateViewReq createReq;
} SCreateViewStmt;

typedef struct SDropViewStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      viewName[TSDB_VIEW_NAME_LEN];
  bool      ignoreNotExists;
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

typedef struct SAssignLeaderStmt {
  ENodeType type;
} SAssignLeaderStmt;

typedef struct SBalanceVgroupLeaderStmt {
  ENodeType type;
  int32_t   vgId;
  char      dbName[TSDB_DB_NAME_LEN];
} SBalanceVgroupLeaderStmt;

typedef struct SSetVgroupKeepVersionStmt {
  ENodeType type;
  int32_t   vgId;
  int64_t   keepVersion;
} SSetVgroupKeepVersionStmt;

typedef struct STrimDbWalStmt {
  ENodeType type;
  char      dbName[TSDB_DB_FNAME_LEN];
} STrimDbWalStmt;

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
  bool      force;
} SSplitVgroupStmt;

typedef struct STSMAOptions {
  ENodeType  type;
  SNodeList* pFuncs;
  SNodeList* pCols;
  SNode*     pInterval;
  uint8_t    tsPrecision;
  bool       recursiveTsma;  // true if create recursive tsma
} STSMAOptions;

typedef struct SCreateTSMAStmt {
  ENodeType       type;
  bool            ignoreExists;
  char            tsmaName[TSDB_TABLE_NAME_LEN];
  char            dbName[TSDB_DB_NAME_LEN];
  char            tableName[TSDB_TABLE_NAME_LEN];  // base tb name or base tsma name
  char            originalTbName[TSDB_TABLE_NAME_LEN];
  STSMAOptions*   pOptions;
  uint8_t         precision;
} SCreateTSMAStmt;

typedef struct SDropTSMAStmt {
  ENodeType type;
  bool      ignoreNotExists;
  char      dbName[TSDB_DB_NAME_LEN];
  char      tsmaName[TSDB_TABLE_NAME_LEN];
} SDropTSMAStmt;

typedef struct SCreateRsmaStmt {
  ENodeType  type;
  bool       ignoreExists;
  char       rsmaName[TSDB_TABLE_NAME_LEN];
  char       dbName[TSDB_DB_NAME_LEN];
  char       tableName[TSDB_TABLE_NAME_LEN];
  SNodeList* pFuncs;
  SNodeList* pIntervals;
} SCreateRsmaStmt;

typedef struct SDropRsmaStmt {
  ENodeType type;
  bool      ignoreNotExists;
  char      dbName[TSDB_DB_NAME_LEN];
  char      rsmaName[TSDB_TABLE_NAME_LEN];
} SDropRsmaStmt;

typedef struct SAlterRsmaStmt {
  ENodeType  type;
  char       dbName[TSDB_DB_NAME_LEN];
  char       rsmaName[TSDB_TABLE_NAME_LEN];
  bool       ignoreNotExists;
  int8_t     alterType;
  SNodeList* pFuncs;
} SAlterRsmaStmt;

#ifdef __cplusplus
}
#endif

#endif /*_TD_CMD_NODES_H_*/
