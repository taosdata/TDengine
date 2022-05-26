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
#define DESCRIBE_RESULT_NOTE_LEN  (8 + VARSTR_HEADER_SIZE)

typedef struct SDatabaseOptions {
  ENodeType   type;
  int32_t     buffer;
  int8_t      cachelast;
  int8_t      compressionLevel;
  int32_t     daysPerFile;
  SValueNode* pDaysPerFile;
  int32_t     fsyncPeriod;
  int32_t     maxRowsPerBlock;
  int32_t     minRowsPerBlock;
  SNodeList*  pKeep;
  int32_t     keep[3];
  int32_t     pages;
  int32_t     pagesize;
  char        precisionStr[3];
  int8_t      precision;
  int8_t      replica;
  int8_t      strict;
  int8_t      walLevel;
  int32_t     numOfVgroups;
  int8_t      singleStable;
  SNodeList*  pRetentions;
  int8_t      schemaless;
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

typedef struct STableOptions {
  ENodeType  type;
  char       comment[TSDB_TB_COMMENT_LEN];
  int32_t    delay;
  float      filesFactor;
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
  ENodeType  type;
  char       dbName[TSDB_DB_NAME_LEN];
  char       tableName[TSDB_TABLE_NAME_LEN];
  char       useDbName[TSDB_DB_NAME_LEN];
  char       useTableName[TSDB_TABLE_NAME_LEN];
  bool       ignoreExists;
  SNodeList* pSpecificTags;
  SNodeList* pValsOfTags;
} SCreateSubTableClause;

typedef struct SCreateMultiTableStmt {
  ENodeType  type;
  SNodeList* pSubTables;
} SCreateMultiTableStmt;

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
  char      useName[TSDB_USER_LEN];
  char      password[TSDB_USET_PASSWORD_LEN];
} SCreateUserStmt;

typedef struct SAlterUserStmt {
  ENodeType type;
  char      useName[TSDB_USER_LEN];
  char      password[TSDB_USET_PASSWORD_LEN];
  int8_t    alterType;
} SAlterUserStmt;

typedef struct SDropUserStmt {
  ENodeType type;
  char      useName[TSDB_USER_LEN];
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
} SDropDnodeStmt;

typedef struct SAlterDnodeStmt {
  ENodeType type;
  int32_t   dnodeId;
  char      config[TSDB_DNODE_CONFIG_LEN];
  char      value[TSDB_DNODE_VALUE_LEN];
} SAlterDnodeStmt;

typedef struct SShowStmt {
  ENodeType type;
  SNode*    pDbName;         // SValueNode
  SNode*    pTbNamePattern;  // SValueNode
} SShowStmt;

typedef struct SShowCreatStmt {
  ENodeType type;
  char      dbName[TSDB_DB_NAME_LEN];
  char      tableName[TSDB_TABLE_NAME_LEN];
} SShowCreatStmt;

typedef enum EIndexType { INDEX_TYPE_SMA = 1, INDEX_TYPE_FULLTEXT } EIndexType;

typedef struct SIndexOptions {
  ENodeType  type;
  SNodeList* pFuncs;
  SNode*     pInterval;
  SNode*     pOffset;
  SNode*     pSliding;
} SIndexOptions;

typedef struct SCreateIndexStmt {
  ENodeType      type;
  EIndexType     indexType;
  bool           ignoreExists;
  char           indexName[TSDB_INDEX_NAME_LEN];
  char           tableName[TSDB_TABLE_NAME_LEN];
  SNodeList*     pCols;
  SIndexOptions* pOptions;
} SCreateIndexStmt;

typedef struct SDropIndexStmt {
  ENodeType type;
  bool      ignoreNotExists;
  char      indexName[TSDB_INDEX_NAME_LEN];
  char      tableName[TSDB_TABLE_NAME_LEN];
} SDropIndexStmt;

typedef struct SCreateComponentNodeStmt {
  ENodeType type;
  int32_t   dnodeId;
} SCreateComponentNodeStmt;

typedef struct SDropComponentNodeStmt {
  ENodeType type;
  int32_t   dnodeId;
} SDropComponentNodeStmt;

typedef struct STopicOptions {
  ENodeType type;
  bool      withTable;
  bool      withSchema;
  bool      withTag;
} STopicOptions;

typedef struct SCreateTopicStmt {
  ENodeType      type;
  char           topicName[TSDB_TABLE_NAME_LEN];
  char           subscribeDbName[TSDB_DB_NAME_LEN];
  bool           ignoreExists;
  SNode*         pQuery;
  STopicOptions* pOptions;
} SCreateTopicStmt;

typedef struct SDropTopicStmt {
  ENodeType type;
  char      topicName[TSDB_TABLE_NAME_LEN];
  bool      ignoreNotExists;
} SDropTopicStmt;

typedef struct SDropCGroupStmt {
  ENodeType type;
  char      topicName[TSDB_TABLE_NAME_LEN];
  char      cgroup[TSDB_CGROUP_LEN];
  bool      ignoreNotExists;
} SDropCGroupStmt;

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

typedef struct SStreamOptions {
  ENodeType type;
  int8_t    triggerType;
  SNode*    pWatermark;
} SStreamOptions;

typedef struct SCreateStreamStmt {
  ENodeType       type;
  char            streamName[TSDB_TABLE_NAME_LEN];
  char            targetDbName[TSDB_DB_NAME_LEN];
  char            targetTabName[TSDB_TABLE_NAME_LEN];
  bool            ignoreExists;
  SStreamOptions* pOptions;
  SNode*          pQuery;
} SCreateStreamStmt;

typedef struct SDropStreamStmt {
  ENodeType type;
  char      streamName[TSDB_TABLE_NAME_LEN];
  bool      ignoreNotExists;
} SDropStreamStmt;

typedef struct SCreateFunctionStmt {
  ENodeType type;
  bool      ignoreExists;
  char      funcName[TSDB_FUNC_NAME_LEN];
  bool      isAgg;
  char      libraryPath[PATH_MAX];
  SDataType outputDt;
  int32_t   bufSize;
} SCreateFunctionStmt;

typedef struct SDropFunctionStmt {
  ENodeType type;
  char      funcName[TSDB_FUNC_NAME_LEN];
  bool      ignoreNotExists;
} SDropFunctionStmt;

#define PRIVILEGE_TYPE_MASK(n) (1 << n)

#define PRIVILEGE_TYPE_ALL   PRIVILEGE_TYPE_MASK(0)
#define PRIVILEGE_TYPE_READ  PRIVILEGE_TYPE_MASK(1)
#define PRIVILEGE_TYPE_WRITE PRIVILEGE_TYPE_MASK(2)

#define PRIVILEGE_TYPE_TEST_MASK(val, mask) (((val) & (mask)) != 0)

typedef struct SGrantStmt {
  ENodeType type;
  char      userName[TSDB_USER_LEN];
  char      dbName[TSDB_DB_NAME_LEN];
  int64_t   privileges;
} SGrantStmt;

typedef SGrantStmt SRevokeStmt;

#ifdef __cplusplus
}
#endif

#endif /*_TD_CMD_NODES_H_*/
