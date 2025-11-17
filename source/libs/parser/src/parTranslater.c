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

#include "parTranslater.h"
#include <stdint.h>
#include "nodes.h"
#include "parInt.h"
#include "parUtil.h"
#include "query.h"
#include "querynodes.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdatablock.h"

#include "catalog.h"
#include "cmdnodes.h"
#include "decimal.h"
#include "filter.h"
#include "functionMgt.h"
#include "parUtil.h"
#include "scalar.h"
#include "systable.h"
#include "tanalytics.h"
#include "tcol.h"
#include "tglobal.h"
#include "tmsg.h"
#include "ttime.h"
#include "decimal.h"
#include "planner.h"

#define generateDealNodeErrMsg(pCxt, code, ...) \
  (pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, code, ##__VA_ARGS__), DEAL_RES_ERROR)

#define SYSTABLE_SHOW_TYPE_OFFSET QUERY_NODE_SHOW_DNODES_STMT

#define CHECK_RES_OUT_OF_MEM(p)      \
  do {                               \
    int32_t code = (p);              \
    if (TSDB_CODE_SUCCESS != code) { \
      return code;                   \
    }                                \
  } while (0)

#define CHECK_POINTER_OUT_OF_MEM(p) \
  do {                              \
    if (NULL == (p)) {              \
      return code;                  \
    }                               \
  } while (0)

typedef struct SRewriteTbNameContext {
  int32_t errCode;
  char*   pTbName;
} SRewriteTbNameContext;

typedef struct SBuildTopicContext {
  bool        colExists;
  bool        colNotFound;
  STableMeta* pMeta;
  SNodeList*  pTags;
  int32_t     code;
} SBuildTopicContext;

typedef struct SFullDatabaseName {
  char fullDbName[TSDB_DB_FNAME_LEN];
} SFullDatabaseName;

typedef struct SSysTableShowAdapter {
  ENodeType   showType;
  const char* pDbName;
  const char* pTableName;
  int32_t     numOfShowCols;
  const char* pShowCols[3];
} SSysTableShowAdapter;

typedef struct SCollectJoinCondsContext {
  bool inOp;

  int32_t primCondNum;
  int32_t logicAndNum;
  int32_t logicOrNum;
  int32_t eqCondNum;
  int32_t neqCondNum;
  bool    primDisorder;
  int32_t code;
} SCollectJoinCondsContext;

typedef struct SCollectWindowsPseudocolumnsContext {
  int32_t    code;
  SNodeList* pCols;
} SCollectWindowsPseudocolumnsContext;

// clang-format off
static const SSysTableShowAdapter sysTableShowAdapter[] = {
  {
    .showType = QUERY_NODE_SHOW_DNODES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_DNODES,

    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_MNODES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_MNODES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_MODULES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_MODULES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_QNODES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_QNODES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_SNODES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_SNODES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_BACKUP_NODES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_BACKUP_NODES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_ARBGROUPS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_ARBGROUPS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_CLUSTER_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_CLUSTER,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_DATABASES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_DATABASES,
    .numOfShowCols = 1,
    .pShowCols = {"name"}
  },
  {
    .showType = QUERY_NODE_SHOW_FUNCTIONS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_FUNCTIONS,
    .numOfShowCols = 1,
    .pShowCols = {"name"}
  },
  {
    .showType = QUERY_NODE_SHOW_INDEXES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_INDEXES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_STABLES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_STABLES,
    .numOfShowCols = 1,
    .pShowCols = {"stable_name"}
  },
  {
    .showType = QUERY_NODE_SHOW_STREAMS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_STREAMS,
    .numOfShowCols = 3,
    .pShowCols = {"stream_name","status","message"}
  },
  {
    .showType = QUERY_NODE_SHOW_TABLES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_TABLES,
    .numOfShowCols = 1,
    .pShowCols = {"table_name"}
  },
  {
    .showType = QUERY_NODE_SHOW_TAGS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_TAGS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_USERS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_USERS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_USERS_FULL_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_USERS_FULL,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_LICENCES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_LICENCES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_VGROUPS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_VGROUPS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_TOPICS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_TOPICS,
    .numOfShowCols = 1,
    .pShowCols = {"topic_name"}
  },
  {
    .showType = QUERY_NODE_SHOW_CONSUMERS_STMT,
    .pDbName = TSDB_PERFORMANCE_SCHEMA_DB,
    .pTableName = TSDB_PERFS_TABLE_CONSUMERS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_CONNECTIONS_STMT,
    .pDbName = TSDB_PERFORMANCE_SCHEMA_DB,
    .pTableName = TSDB_PERFS_TABLE_CONNECTIONS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_QUERIES_STMT,
    .pDbName = TSDB_PERFORMANCE_SCHEMA_DB,
    .pTableName = TSDB_PERFS_TABLE_QUERIES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_APPS_STMT,
    .pDbName = TSDB_PERFORMANCE_SCHEMA_DB,
    .pTableName = TSDB_PERFS_TABLE_APPS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_VARIABLES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_CONFIGS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_DNODE_VARIABLES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_DNODE_VARIABLES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_TRANSACTIONS_STMT,
    .pDbName = TSDB_PERFORMANCE_SCHEMA_DB,
    .pTableName = TSDB_PERFS_TABLE_TRANS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_SUBSCRIPTIONS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_SUBSCRIPTIONS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_VNODES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_VNODES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_USER_PRIVILEGES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_USER_PRIVILEGES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_VIEWS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_VIEWS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_COMPACTS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_COMPACTS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_COMPACT_DETAILS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_COMPACT_DETAILS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_GRANTS_FULL_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_GRANTS_FULL,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_GRANTS_LOGS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_GRANTS_LOGS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_CLUSTER_MACHINES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_MACHINES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_ENCRYPTIONS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_ENCRYPTIONS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_TSMAS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_TSMAS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {  .showType = QUERY_NODE_SHOW_ANODES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_ANODES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_ANODES_FULL_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_ANODES_FULL,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_USAGE_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_DISK_USAGE,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { 
    .showType = QUERY_NODE_SHOW_FILESETS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_FILESETS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  }, 
  { 
    .showType = QUERY_NODE_SHOW_TRANSACTION_DETAILS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_TRANSACTION_DETAILS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  }, 
  {
    .showType = QUERY_NODE_SHOW_VTABLES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_TABLES,
    .numOfShowCols = 1,
    .pShowCols = {"table_name"}
  },
  {
    .showType = QUERY_NODE_SHOW_BNODES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_BNODES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_MOUNTS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_MOUNTS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  {
    .showType = QUERY_NODE_SHOW_SSMIGRATES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_SSMIGRATES,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_SCANS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_SCANS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_SCAN_DETAILS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_SCAN_DETAILS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_RSMAS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_RSMAS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_RETENTIONS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_RETENTIONS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
  { .showType = QUERY_NODE_SHOW_RETENTION_DETAILS_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_RETENTION_DETAILS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  }
};
// clang-format on

static int32_t  translateSubquery(STranslateContext* pCxt, SNode* pNode);
static int32_t  translateQuery(STranslateContext* pCxt, SNode* pNode);
static EDealRes translateValue(STranslateContext* pCxt, SValueNode* pVal);
static EDealRes translateFunction(STranslateContext* pCxt, SFunctionNode** pFunc);
static int32_t  createSimpleSelectStmtFromProjList(const char* pDb, const char* pTable, SNodeList* pProjectionList,
                                                   SSelectStmt** pStmt);
static int32_t  setQuery(STranslateContext* pCxt, SQuery* pQuery);
static int32_t  setRefreshMeta(STranslateContext* pCxt, SQuery* pQuery);

static int32_t createTsOperatorNode(EOperatorType opType, const SNode* pRight, SNode** pOp);
static int32_t createOperatorNode(EOperatorType opType, const char* pColName, const SNode* pRight, SNode** pOp);
static int32_t createOperatorNodeByNode(EOperatorType opType, const SNode* pLeft, const SNode* pRight, SNode** pOp);
static int32_t createIsOperatorNodeByNode(EOperatorType opType, SNode* pNode, SNode** pOp);
static int32_t insertCondIntoSelectStmt(SSelectStmt* pSelect, SNode** pCond);
static int32_t extractCondFromCountWindow(STranslateContext* pCxt, SCountWindowNode* pCountWindow, SNode** pCond);

static bool isWindowJoinStmt(SSelectStmt* pSelect) {
  return (QUERY_NODE_JOIN_TABLE == nodeType(pSelect->pFromTable)) &&
         IS_WINDOW_JOIN(((SJoinTableNode*)pSelect->pFromTable)->subType);
}

static int32_t replacePsedudoColumnFuncWithColumn(STranslateContext* pCxt, SNode** ppNode);

static bool afterGroupBy(ESqlClause clause) { return clause > SQL_CLAUSE_GROUP_BY; }

static bool beforeHaving(ESqlClause clause) { return clause < SQL_CLAUSE_HAVING; }

static bool afterHaving(ESqlClause clause) { return clause > SQL_CLAUSE_HAVING; }

static bool beforeWindow(ESqlClause clause) { return clause < SQL_CLAUSE_WINDOW; }

static bool hasSameTableAlias(SArray* pTables) {
  if (taosArrayGetSize(pTables) < 2) {
    return false;
  }
  STableNode* pTable0 = taosArrayGetP(pTables, 0);
  for (int32_t i = 1; i < taosArrayGetSize(pTables); ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    if (0 == strcmp(pTable0->tableAlias, pTable->tableAlias)) {
      return true;
    }
  }
  return false;
}

static int32_t addNamespace(STranslateContext* pCxt, void* pTable) {
  int32_t code = 0;
  size_t  currTotalLevel = taosArrayGetSize(pCxt->pNsLevel);
  if (currTotalLevel > pCxt->currLevel) {
    SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
    if (NULL == taosArrayPush(pTables, &pTable)) {
      return terrno;
    }
    if (hasSameTableAlias(pTables)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_UNIQUE_TABLE_ALIAS,
                                     "Not unique table/alias: '%s'", ((STableNode*)pTable)->tableAlias);
    }
  } else {
    do {
      SArray* pTables = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
      if (NULL == pTables) {
        return terrno;
      }
      if (pCxt->currLevel == currTotalLevel) {
        if (NULL == taosArrayPush(pTables, &pTable)) {
          taosArrayDestroy(pTables);
          return terrno;
        }
        if (hasSameTableAlias(pTables)) {
          taosArrayDestroy(pTables);
          return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_UNIQUE_TABLE_ALIAS,
                                         "Not unique table/alias: '%s'", ((STableNode*)pTable)->tableAlias);
        }
      }
      if (NULL == taosArrayPush(pCxt->pNsLevel, &pTables)) {
        code = terrno;
        taosArrayDestroy(pTables);
        break;
      }
      ++currTotalLevel;
    } while (currTotalLevel <= pCxt->currLevel);
  }
  return code;
}

static int32_t collectUseDatabaseImpl(const char* pFullDbName, SHashObj* pDbs) {
  SFullDatabaseName name = {0};
  snprintf(name.fullDbName, sizeof(name.fullDbName), "%s", pFullDbName);
  return taosHashPut(pDbs, pFullDbName, strlen(pFullDbName), &name, sizeof(SFullDatabaseName));
}

static int32_t collectUseDatabase(const SName* pName, SHashObj* pDbs) {
  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(pName, dbFName);
  return collectUseDatabaseImpl(dbFName, pDbs);
}

static int32_t collectUseTable(const SName* pName, SHashObj* pTable) {
  char    fullName[TSDB_TABLE_FNAME_LEN];
  int32_t code = tNameExtractFullName(pName, fullName);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  return taosHashPut(pTable, fullName, strlen(fullName), pName, sizeof(SName));
}

#ifdef BUILD_NO_CALL
static int32_t getViewMetaImpl(SParseContext* pParCxt, SParseMetaCache* pMetaCache, const SName* pName,
                               STableMeta** pMeta) {
#ifndef TD_ENTERPRISE
  return TSDB_CODE_PAR_TABLE_NOT_EXIST;
#endif

  int32_t code = TSDB_CODE_SUCCESS;

  if (pParCxt->async) {
    code = getViewMetaFromCache(pMetaCache, pName, pMeta);
  } else {
    SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                             .requestId = pParCxt->requestId,
                             .requestObjRefId = pParCxt->requestRid,
                             .mgmtEps = pParCxt->mgmtEpSet};
    code = catalogGetViewMeta(pParCxt->pCatalog, &conn, pName, pMeta);
  }

  if (TSDB_CODE_SUCCESS != code && TSDB_CODE_PAR_TABLE_NOT_EXIST != code) {
    parserError("QID:0x%" PRIx64 ", catalogGetViewMeta error, code:%s, dbName:%s, viewName:%s", pParCxt->requestId,
                tstrerror(code), pName->dbname, pName->tname);
  }
  return code;
}
#endif

static int32_t getTargetNameImpl(SParseContext* pParCxt, SParseMetaCache* pMetaCache, const SName* pName,
                                 char* pTbName) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pParCxt->async) {
    code = getTableNameFromCache(pMetaCache, pName, pTbName);
  } else {
    code = TSDB_CODE_PAR_INTERNAL_ERROR;
  }
  if (TSDB_CODE_SUCCESS != code && TSDB_CODE_PAR_TABLE_NOT_EXIST != code) {
    parserError("QID:0x%" PRIx64 ", catalogGetTableMeta error, code:%s, dbName:%s, tbName:%s", pParCxt->requestId,
                tstrerror(code), pName->dbname, pName->tname);
  }
  return code;
}

static int32_t getTargetName(STranslateContext* pCxt, const SName* pName, char* pTbName) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = collectUseDatabase(pName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(pName, pCxt->pTables);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = getTargetNameImpl(pParCxt, pCxt->pMetaCache, pName, pTbName);
  }
  if (TSDB_CODE_SUCCESS != code && TSDB_CODE_PAR_TABLE_NOT_EXIST != code) {
    parserError("QID:0x%" PRIx64 ", catalogGetTableMeta error, code:%s, dbName:%s, tbName:%s",
                pCxt->pParseCxt->requestId, tstrerror(code), pName->dbname, pName->tname);
  }
  return code;
}

static int32_t rewriteDropTableWithMetaCache(STranslateContext* pCxt) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SParseContext*   pParCxt = pCxt->pParseCxt;
  SParseMetaCache* pMetaCache = pCxt->pMetaCache;
  int32_t          tbMetaSize = taosHashGetSize(pMetaCache->pTableMeta);
  int32_t          tbMetaExSize = taosHashGetSize(pMetaCache->pTableName);

  if (tbMetaSize > 0 || tbMetaExSize <= 0) {
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }
  if (!pMetaCache->pTableMeta &&
      !(pMetaCache->pTableMeta =
            taosHashInit(tbMetaExSize, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK))) {
    return terrno;
  }

  SMetaRes** ppMetaRes = NULL;
  char       dbName[TSDB_DB_NAME_LEN] = {0};
  while ((ppMetaRes = taosHashIterate(pMetaCache->pTableName, ppMetaRes))) {
    if (!(*ppMetaRes)) {
      taosHashCancelIterate(pMetaCache->pTableName, ppMetaRes);
      return TSDB_CODE_PAR_INTERNAL_ERROR;
    }

    char*       pKey = taosHashGetKey(ppMetaRes, NULL);
    STableMeta* pMeta = (STableMeta*)(*ppMetaRes)->pRes;
    if (!pMeta) {
      taosHashCancelIterate(pMetaCache->pTableName, ppMetaRes);
      return TSDB_CODE_PAR_INTERNAL_ERROR;
    }
    char* pDbStart = strstr(pKey, ".");
    char* pDbEnd = pDbStart ? strstr(pDbStart + 1, ".") : NULL;
    if (!pDbEnd) {
      taosHashCancelIterate(pMetaCache->pTableName, ppMetaRes);
      return TSDB_CODE_PAR_INTERNAL_ERROR;
    }
    tstrncpy(dbName, pDbStart + 1, pDbEnd - pDbStart);

    int32_t metaSize =
        sizeof(STableMeta) + sizeof(SSchema) * (pMeta->tableInfo.numOfColumns + pMeta->tableInfo.numOfTags);
    int32_t schemaExtSize =
        (withExtSchema(pMeta->tableType) && pMeta->schemaExt) ? sizeof(SSchemaExt) * pMeta->tableInfo.numOfColumns : 0;
    int32_t     colRefSize = (hasRefCol(pMeta->tableType) && pMeta->colRef) ? sizeof(SColRef) * pMeta->numOfColRefs : 0;
    const char* pTbName = (const char*)pMeta + metaSize + schemaExtSize + colRefSize;

    SName name = {0};
    toName(pParCxt->acctId, dbName, pTbName, &name);

    char fullName[TSDB_TABLE_FNAME_LEN];
    code = tNameExtractFullName(&name, fullName);
    if (TSDB_CODE_SUCCESS != code) {
      taosHashCancelIterate(pMetaCache->pTableName, ppMetaRes);
      return code;
    }

    if ((code = taosHashPut(pMetaCache->pTableMeta, fullName, strlen(fullName), ppMetaRes, POINTER_BYTES))) {
      taosHashCancelIterate(pMetaCache->pTableName, ppMetaRes);
      return code;
    }
  }
  return code;
}

int32_t getTargetMetaImpl(SParseContext* pParCxt, SParseMetaCache* pMetaCache, const SName* pName, STableMeta** pMeta,
                          bool couldBeView) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pParCxt->async) {
    code = getTableMetaFromCache(pMetaCache, pName, pMeta);
#ifdef TD_ENTERPRISE
    if ((TSDB_CODE_PAR_TABLE_NOT_EXIST == code || TSDB_CODE_PAR_INTERNAL_ERROR == code) && couldBeView) {
      int32_t origCode = code;
      code = getViewMetaFromCache(pMetaCache, pName, pMeta);
      if (TSDB_CODE_SUCCESS != code) {
        code = origCode;
      }
    }
#endif
  } else {
    SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                             .requestId = pParCxt->requestId,
                             .requestObjRefId = pParCxt->requestRid,
                             .mgmtEps = pParCxt->mgmtEpSet};
    code = catalogGetTableMeta(pParCxt->pCatalog, &conn, pName, pMeta);
  }

  if (TSDB_CODE_SUCCESS != code && TSDB_CODE_PAR_TABLE_NOT_EXIST != code) {
    parserError("QID:0x%" PRIx64 ", catalogGetTableMeta error, code:%s, dbName:%s, tbName:%s", pParCxt->requestId,
                tstrerror(code), pName->dbname, pName->tname);
  }
  return code;
}

static int32_t getTargetMeta(STranslateContext* pCxt, const SName* pName, STableMeta** pMeta, bool couldBeView) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = collectUseDatabase(pName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(pName, pCxt->pTables);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = getTargetMetaImpl(pParCxt, pCxt->pMetaCache, pName, pMeta, couldBeView);
  }
  if (TSDB_CODE_SUCCESS != code && TSDB_CODE_PAR_TABLE_NOT_EXIST != code) {
    parserError("QID:0x%" PRIx64 ", catalogGetTableMeta error, code:%s, dbName:%s, tbName:%s",
                pCxt->pParseCxt->requestId, tstrerror(code), pName->dbname, pName->tname);
  }
  return code;
}

static int32_t getTableMeta(STranslateContext* pCxt, const char* pDbName, const char* pTableName, STableMeta** pMeta) {
  SName name = {0};
  toName(pCxt->pParseCxt->acctId, pDbName, pTableName, &name);
  return getTargetMeta(pCxt, &name, pMeta, false);
}

static int32_t getTableCfg(STranslateContext* pCxt, const SName* pName, STableCfg** pCfg) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = collectUseDatabase(pName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(pName, pCxt->pTables);
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (pParCxt->async) {
      code = getTableCfgFromCache(pCxt->pMetaCache, pName, pCfg);
    } else {
      SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                               .requestId = pParCxt->requestId,
                               .requestObjRefId = pParCxt->requestRid,
                               .mgmtEps = pParCxt->mgmtEpSet};
      code = catalogRefreshGetTableCfg(pParCxt->pCatalog, &conn, pName, pCfg);
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("QID:0x%" PRIx64 ", catalogRefreshGetTableCfg error, code:%s, dbName:%s, tbName:%s",
                pCxt->pParseCxt->requestId, tstrerror(code), pName->dbname, pName->tname);
  }
  return code;
}

static int32_t refreshGetTableMeta(STranslateContext* pCxt, const char* pDbName, const char* pTableName,
                                   STableMeta** pMeta) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  SName          name = {0};
  toName(pCxt->pParseCxt->acctId, pDbName, pTableName, &name);
  int32_t code = TSDB_CODE_SUCCESS;
  if (pParCxt->async) {
    code = getTableMetaFromCache(pCxt->pMetaCache, &name, pMeta);
  } else {
    SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                             .requestId = pParCxt->requestId,
                             .requestObjRefId = pParCxt->requestRid,
                             .mgmtEps = pParCxt->mgmtEpSet};

    code = catalogRefreshGetTableMeta(pParCxt->pCatalog, &conn, &name, pMeta, false);
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("QID:0x%" PRIx64 ", catalogRefreshGetTableMeta error, code:%s, dbName:%s, tbName:%s",
                pCxt->pParseCxt->requestId, tstrerror(code), pDbName, pTableName);
  }
  return code;
}

static int32_t getDBVgInfoImpl(STranslateContext* pCxt, const SName* pName, SArray** pVgInfo) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  char           fullDbName[TSDB_DB_FNAME_LEN];
  (void)tNameGetFullDbName(pName, fullDbName);
  int32_t code = collectUseDatabaseImpl(fullDbName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    if (pParCxt->async) {
      code = getDbVgInfoFromCache(pCxt->pMetaCache, fullDbName, pVgInfo);
    } else {
      SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                               .requestId = pParCxt->requestId,
                               .requestObjRefId = pParCxt->requestRid,
                               .mgmtEps = pParCxt->mgmtEpSet};
      code = catalogGetDBVgList(pParCxt->pCatalog, &conn, fullDbName, pVgInfo);
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("QID:0x%" PRIx64 ", catalogGetDBVgList error, code:%s, dbFName:%s", pCxt->pParseCxt->requestId,
                tstrerror(code), fullDbName);
  }
  return code;
}

static int32_t getDBVgInfo(STranslateContext* pCxt, const char* pDbName, SArray** pVgInfo) {
  SName   name;
  int32_t code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pDbName, strlen(pDbName));
  if (TSDB_CODE_SUCCESS != code) return code;
  char dbFname[TSDB_DB_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(&name, dbFname);
  return getDBVgInfoImpl(pCxt, &name, pVgInfo);
}

static int32_t getTableHashVgroupImpl(STranslateContext* pCxt, const SName* pName, SVgroupInfo* pInfo) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = collectUseDatabase(pName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(pName, pCxt->pTables);
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (pParCxt->async) {
      if (pCxt->withOpt) {
        code = getDbTableVgroupFromCache(pCxt->pMetaCache, pName, pInfo);
      } else {
        code = getTableVgroupFromCache(pCxt->pMetaCache, pName, pInfo);
      }
    } else {
      SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                               .requestId = pParCxt->requestId,
                               .requestObjRefId = pParCxt->requestRid,
                               .mgmtEps = pParCxt->mgmtEpSet};
      code = catalogGetTableHashVgroup(pParCxt->pCatalog, &conn, pName, pInfo);
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("QID:0x%" PRIx64 ", catalogGetTableHashVgroup error, code:%s, dbName:%s, tbName:%s",
                pCxt->pParseCxt->requestId, tstrerror(code), pName->dbname, pName->tname);
  }
  return code;
}

static int32_t getTableHashVgroup(STranslateContext* pCxt, const char* pDbName, const char* pTableName,
                                  SVgroupInfo* pInfo) {
  SName name = {0};
  toName(pCxt->pParseCxt->acctId, pDbName, pTableName, &name);
  return getTableHashVgroupImpl(pCxt, &name, pInfo);
}

static int32_t getTableVgId(STranslateContext* pCxt, const char* pDbName, const char* pTableName, int32_t *vgId) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SVgroupInfo info = {0};
  PAR_ERR_RET(getTableHashVgroup(pCxt, pDbName, pTableName, &info));
  *vgId = info.vgId;
  return code;
}

static int32_t getDBVgVersion(STranslateContext* pCxt, const char* pDbFName, int32_t* pVersion, int64_t* pDbId,
                              int32_t* pTableNum, int64_t* pStateTs) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = collectUseDatabaseImpl(pDbFName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    if (pParCxt->async) {
      code = getDbVgVersionFromCache(pCxt->pMetaCache, pDbFName, pVersion, pDbId, pTableNum, pStateTs);
    } else {
      code = catalogGetDBVgVersion(pParCxt->pCatalog, pDbFName, pVersion, pDbId, pTableNum, pStateTs);
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("QID:0x%" PRIx64 ", catalogGetDBVgVersion error, code:%s, dbFName:%s", pCxt->pParseCxt->requestId,
                tstrerror(code), pDbFName);
  }
  return code;
}

static int32_t getDBCfg(STranslateContext* pCxt, const char* pDbName, SDbCfgInfo* pInfo) {
  if (IS_SYS_DBNAME(pDbName)) {
    return TSDB_CODE_SUCCESS;
  }

  SParseContext* pParCxt = pCxt->pParseCxt;
  SName          name;
  int32_t        code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pDbName, strlen(pDbName));
  if (TSDB_CODE_SUCCESS != code) return code;
  char dbFname[TSDB_DB_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(&name, dbFname);
  code = collectUseDatabaseImpl(dbFname, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    if (pParCxt->async) {
      code = getDbCfgFromCache(pCxt->pMetaCache, dbFname, pInfo);
    } else {
      SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                               .requestId = pParCxt->requestId,
                               .requestObjRefId = pParCxt->requestRid,
                               .mgmtEps = pParCxt->mgmtEpSet};

      code = catalogGetDBCfg(pParCxt->pCatalog, &conn, dbFname, pInfo);
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("QID:0x%" PRIx64 ", catalogGetDBCfg error, code:%s, dbFName:%s", pCxt->pParseCxt->requestId,
                tstrerror(code), dbFname);
  }
  return code;
}

static int32_t getUdfInfo(STranslateContext* pCxt, SFunctionNode* pFunc) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  SFuncInfo      funcInfo = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  if (pParCxt->async) {
    code = getUdfInfoFromCache(pCxt->pMetaCache, pFunc->functionName, &funcInfo);
  } else {
    SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                             .requestId = pParCxt->requestId,
                             .requestObjRefId = pParCxt->requestRid,
                             .mgmtEps = pParCxt->mgmtEpSet};

    code = catalogGetUdfInfo(pParCxt->pCatalog, &conn, pFunc->functionName, &funcInfo);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pFunc->funcType = FUNCTION_TYPE_UDF;
    pFunc->funcId = TSDB_FUNC_TYPE_AGGREGATE == funcInfo.funcType ? FUNC_AGGREGATE_UDF_ID : FUNC_SCALAR_UDF_ID;
    pFunc->node.resType.type = funcInfo.outputType;
    pFunc->node.resType.bytes = funcInfo.outputLen;
    pFunc->udfBufSize = funcInfo.bufSize;
    tFreeSFuncInfo(&funcInfo);
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("QID:0x%" PRIx64 ", catalogGetUdfInfo error, code:%s, funcName:%s", pCxt->pParseCxt->requestId,
                tstrerror(code), pFunc->functionName);
  }
  return code;
}

# if 0
static int32_t getTableIndex(STranslateContext* pCxt, const SName* pName, SArray** pIndexes) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = collectUseDatabase(pName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(pName, pCxt->pTables);
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (pParCxt->async) {
      code = getTableIndexFromCache(pCxt->pMetaCache, pName, pIndexes);
    } else {
      SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                               .requestId = pParCxt->requestId,
                               .requestObjRefId = pParCxt->requestRid,
                               .mgmtEps = pParCxt->mgmtEpSet};

      code = catalogGetTableIndex(pParCxt->pCatalog, &conn, pName, pIndexes);
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("QID:0x%" PRIx64 ", getTableIndex error, code:%s, dbName:%s, tbName:%s", pCxt->pParseCxt->requestId,
                tstrerror(code), pName->dbname, pName->tname);
  }
  return code;
}
#endif

static int32_t getDnodeList(STranslateContext* pCxt, SArray** pDnodes) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = TSDB_CODE_SUCCESS;
  if (pParCxt->async) {
    code = getDnodeListFromCache(pCxt->pMetaCache, pDnodes);
  } else {
    SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                             .requestId = pParCxt->requestId,
                             .requestObjRefId = pParCxt->requestRid,
                             .mgmtEps = pParCxt->mgmtEpSet};
    code = catalogGetDnodeList(pParCxt->pCatalog, &conn, pDnodes);
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("QID:0x%" PRIx64 ", getDnodeList error, code:%s", pCxt->pParseCxt->requestId, tstrerror(code));
  }
  return code;
}

static int32_t getTableTsmas(STranslateContext* pCxt, const SName* pName, SArray** ppTsmas) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = 0;
  if (pParCxt->async) {
    code = getTableTsmasFromCache(pCxt->pMetaCache, pName, ppTsmas);
  } else {
    SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                             .requestId = pParCxt->requestId,
                             .requestObjRefId = pParCxt->requestRid,
                             .mgmtEps = pParCxt->mgmtEpSet};
    code = catalogGetTableTsmas(pParCxt->pCatalog, &conn, pName, ppTsmas);
  }
  if (code)
    parserError("QID:0x%" PRIx64 ", get table tsma for : %s.%s error, code:%s", pCxt->pParseCxt->requestId,
                pName->dbname, pName->tname, tstrerror(code));
  return code;
}

static int32_t getTsma(STranslateContext* pCxt, const SName* pName, STableTSMAInfo** pTsma) {
  int32_t        code = 0;
  SParseContext* pParCxt = pCxt->pParseCxt;
  if (pParCxt->async) {
    code = getTsmaFromCache(pCxt->pMetaCache, pName, pTsma);
  } else {
    SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                             .requestId = pParCxt->requestId,
                             .requestObjRefId = pParCxt->requestRid,
                             .mgmtEps = pParCxt->mgmtEpSet};
    code = catalogGetTsma(pParCxt->pCatalog, &conn, pName, pTsma);
  }
  if (code)
    parserError("QID:0x%" PRIx64 ", get tsma for: %s.%s error, code:%s", pCxt->pParseCxt->requestId, pName->dbname,
                pName->tname, tstrerror(code));
  return code;
}

static void initStreamInfo(SParseStreamInfo* pInfo) {
  pInfo->placeHolderBitmap = 0;
  pInfo->calcClause = false;
  pInfo->triggerClause = false;
  pInfo->outTableClause = false;
  pInfo->extLeftEq = false;
  pInfo->extRightEq = false;
  pInfo->triggerPartitionList = NULL;
  pInfo->triggerTbl = NULL;
}

static int32_t initTranslateContext(SParseContext* pParseCxt, SParseMetaCache* pMetaCache, STranslateContext* pCxt) {
  pCxt->pParseCxt = pParseCxt;
  pCxt->errCode = TSDB_CODE_SUCCESS;
  pCxt->msgBuf.buf = pParseCxt->pMsg;
  pCxt->msgBuf.len = pParseCxt->msgLen;
  pCxt->pNsLevel = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  pCxt->currLevel = 0;
  pCxt->levelNo = 0;
  pCxt->currClause = 0;
  pCxt->pMetaCache = pMetaCache;
  pCxt->pDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  pCxt->pTables = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  pCxt->pTargetTables = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  initStreamInfo(&pCxt->streamInfo);
  if (NULL == pCxt->pNsLevel || NULL == pCxt->pDbs || NULL == pCxt->pTables || NULL == pCxt->pTargetTables) {
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t resetHighLevelTranslateNamespace(STranslateContext* pCxt) {
  if (NULL != pCxt->pNsLevel) {
    size_t  size = taosArrayGetSize(pCxt->pNsLevel);
    int32_t levelNum = size - pCxt->currLevel;
    if (levelNum <= 0) {
      return TSDB_CODE_SUCCESS;
    }

    for (int32_t i = size - 1; i >= pCxt->currLevel; --i) {
      taosArrayDestroy(taosArrayGetP(pCxt->pNsLevel, i));
    }
    taosArrayPopTailBatch(pCxt->pNsLevel, levelNum);

    return TSDB_CODE_SUCCESS;
  }
  pCxt->pNsLevel = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  if (NULL == pCxt->pNsLevel) {
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t resetTranslateNamespace(STranslateContext* pCxt) {
  if (NULL != pCxt->pNsLevel) {
    size_t size = taosArrayGetSize(pCxt->pNsLevel);
    for (size_t i = 0; i < size; ++i) {
      taosArrayDestroy(taosArrayGetP(pCxt->pNsLevel, i));
    }
    taosArrayDestroy(pCxt->pNsLevel);
  }
  pCxt->pNsLevel = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  if (NULL == pCxt->pNsLevel) {
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static void destroyTranslateContext(STranslateContext* pCxt) {
  if (NULL != pCxt->pNsLevel) {
    size_t size = taosArrayGetSize(pCxt->pNsLevel);
    for (size_t i = 0; i < size; ++i) {
      taosArrayDestroy(taosArrayGetP(pCxt->pNsLevel, i));
    }
    taosArrayDestroy(pCxt->pNsLevel);
  }

  if (NULL != pCxt->pCmdMsg) {
    taosMemoryFreeClear(pCxt->pCmdMsg->pMsg);
    taosMemoryFreeClear(pCxt->pCmdMsg);
  }

  taosHashCleanup(pCxt->pDbs);
  taosHashCleanup(pCxt->pTables);
  taosHashCleanup(pCxt->pTargetTables);
}

static bool isSelectStmt(SNode* pCurrStmt) {
  return NULL != pCurrStmt && QUERY_NODE_SELECT_STMT == nodeType(pCurrStmt);
}

static bool isDeleteStmt(SNode* pCurrStmt) {
  return NULL != pCurrStmt && QUERY_NODE_DELETE_STMT == nodeType(pCurrStmt);
}

static bool isSetOperator(SNode* pCurrStmt) {
  return NULL != pCurrStmt && QUERY_NODE_SET_OPERATOR == nodeType(pCurrStmt);
}

static bool isCreateTSMAStmt(SNode* pCurrStmt) {
  return NULL != pCurrStmt && QUERY_NODE_CREATE_TSMA_STMT == nodeType(pCurrStmt);
}

static SNodeList* getProjectListFromCurrStmt(SNode* pCurrStmt) {
  if (isSelectStmt(pCurrStmt)) {
    return ((SSelectStmt*)pCurrStmt)->pProjectionList;
  }
  if (isSetOperator(pCurrStmt)) {
    return ((SSetOperator*)pCurrStmt)->pProjectionList;
  }
  return NULL;
}

static uint8_t getPrecisionFromCurrStmt(SNode* pCurrStmt, uint8_t defaultVal) {
  if (isSelectStmt(pCurrStmt)) {
    return ((SSelectStmt*)pCurrStmt)->precision;
  }
  if (isSetOperator(pCurrStmt)) {
    return ((SSetOperator*)pCurrStmt)->precision;
  }
  if (isDeleteStmt(pCurrStmt)) {
    return ((SDeleteStmt*)pCurrStmt)->precision;
  }
  if (isCreateTSMAStmt(pCurrStmt)) {
    return ((SCreateTSMAStmt*)pCurrStmt)->precision;
  }
  return defaultVal;
}

static bool isAliasColumn(const SNode* pNode) {
  return (QUERY_NODE_COLUMN == nodeType(pNode) && ('\0' == ((SColumnNode*)pNode)->tableAlias[0]));
}

static bool isAggFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsAggFunc(((SFunctionNode*)pNode)->funcId));
}

#ifdef BUILD_NO_CALL
static bool isSelectFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsSelectFunc(((SFunctionNode*)pNode)->funcId));
}
#endif

static bool isWindowPseudoColumnFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsWindowPseudoColumnFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isInterpFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsInterpFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isInterpPseudoColumnFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsInterpPseudoColumnFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isForecastFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsForecastFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isForecastPseudoColumnFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsAnalysisPseudoColumnFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isColsFunctionResult(const SNode* pNode) {
  return ((nodesIsExprNode(pNode)) && (isRelatedToOtherExpr((SExprNode*)pNode)));
}

static bool isInvalidColsBindFunction(const SFunctionNode* pFunc) {
  return (pFunc->node.bindExprID != 0 && (!fmIsSelectFunc(pFunc->funcId) || fmIsMultiRowsFunc(pFunc->funcId) ||
                                          fmIsIndefiniteRowsFunc(pFunc->funcId)));
}

#ifdef BUILD_NO_CALL
static bool isTimelineFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsTimelineFunc(((SFunctionNode*)pNode)->funcId));
}
#endif

static bool isImplicitTsFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsImplicitTsFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isPkFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsPrimaryKeyFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isScanPseudoColumnFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsScanPseudoColumnFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isIndefiniteRowsFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsIndefiniteRowsFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isVectorFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsVectorFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isColsFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsSelectColsFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isDistinctOrderBy(STranslateContext* pCxt) {
  return (SQL_CLAUSE_ORDER_BY == pCxt->currClause && isSelectStmt(pCxt->pCurrStmt) &&
          ((SSelectStmt*)pCxt->pCurrStmt)->isDistinct);
}

static bool belongTable(const SColumnNode* pCol, const STableNode* pTable) {
  int cmp = 0;
  if ('\0' != pCol->dbName[0]) {
    cmp = strcmp(pCol->dbName, pTable->dbName);
  }
  if (0 == cmp) {
    cmp = strcmp(pCol->tableAlias, pTable->tableAlias);
  }
  return (0 == cmp);
}

static SNodeList* getProjectList(const SNode* pNode) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pNode)) {
    return ((SSelectStmt*)pNode)->pProjectionList;
  } else if (QUERY_NODE_SET_OPERATOR == nodeType(pNode)) {
    return ((SSetOperator*)pNode)->pProjectionList;
  }
  return NULL;
}

static bool isBlockTimeLineQuery(SNode* pStmt) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    return (TIME_LINE_MULTI == ((SSelectStmt*)pStmt)->timeLineCurMode) ||
           (TIME_LINE_GLOBAL == ((SSelectStmt*)pStmt)->timeLineCurMode) ||
           (TIME_LINE_BLOCK == ((SSelectStmt*)pStmt)->timeLineCurMode);
  } else if (QUERY_NODE_SET_OPERATOR == nodeType(pStmt)) {
    return TIME_LINE_GLOBAL == ((SSetOperator*)pStmt)->timeLineResMode;
  } else {
    return false;
  }
}

static bool isTimeLineQuery(SNode* pStmt) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    return (TIME_LINE_MULTI == ((SSelectStmt*)pStmt)->timeLineCurMode) ||
           (TIME_LINE_GLOBAL == ((SSelectStmt*)pStmt)->timeLineCurMode);
  } else if (QUERY_NODE_SET_OPERATOR == nodeType(pStmt)) {
    return (TIME_LINE_MULTI == ((SSetOperator*)pStmt)->timeLineResMode) ||
           (TIME_LINE_GLOBAL == ((SSetOperator*)pStmt)->timeLineResMode);
  } else {
    return false;
  }
}

static bool isGlobalTimeLineQuery(SNode* pStmt) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    return TIME_LINE_GLOBAL == ((SSelectStmt*)pStmt)->timeLineResMode;
  } else if (QUERY_NODE_SET_OPERATOR == nodeType(pStmt)) {
    return TIME_LINE_GLOBAL == ((SSetOperator*)pStmt)->timeLineResMode;
  } else {
    return false;
  }
}

static bool isCurGlobalTimeLineQuery(SNode* pStmt) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    return TIME_LINE_GLOBAL == ((SSelectStmt*)pStmt)->timeLineCurMode;
  } else if (QUERY_NODE_SET_OPERATOR == nodeType(pStmt)) {
    return TIME_LINE_GLOBAL == ((SSetOperator*)pStmt)->timeLineResMode;
  } else {
    return false;
  }
}

static bool isBlockTimeLineAlignedQuery(SNode* pStmt) {
  SSelectStmt* pSelect = (SSelectStmt*)pStmt;
  if (!isBlockTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    return false;
  }
  if (QUERY_NODE_SELECT_STMT != nodeType(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    return false;
  }
  SSelectStmt* pSub = (SSelectStmt*)((STempTableNode*)pSelect->pFromTable)->pSubquery;
  if (nodesListMatch(pSelect->pPartitionByList, pSub->pPartitionByList)) {
    return true;
  }
  return false;
}

int32_t buildPartitionListFromOrderList(SNodeList* pOrderList, int32_t nodesNum, SNodeList** ppOut) {
  *ppOut = NULL;
  SNodeList* pPartitionList = NULL;
  SNode*     pNode = NULL;
  if (pOrderList->length <= nodesNum) {
    return TSDB_CODE_SUCCESS;
  }

  pNode = nodesListGetNode(pOrderList, nodesNum);
  SOrderByExprNode* pOrder = (SOrderByExprNode*)pNode;
  if (!isPrimaryKeyImpl(pOrder->pExpr)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;
  for (int32_t i = 0; i < nodesNum; ++i) {
    pNode = nodesListGetNode(pOrderList, i);
    pOrder = (SOrderByExprNode*)pNode;
    SNode* pClonedNode = NULL;
    code = nodesCloneNode(pOrder->pExpr, &pClonedNode);
    if (TSDB_CODE_SUCCESS != code) break;
    code = nodesListMakeStrictAppend(&pPartitionList, pClonedNode);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  if (TSDB_CODE_SUCCESS == code) *ppOut = pPartitionList;

  return code;
}

static int32_t isTimeLineAlignedQuery(SNode* pStmt, bool* pRes) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SSelectStmt* pSelect = (SSelectStmt*)pStmt;
  if (!isTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    *pRes = false;
    return code;
  }
  if (QUERY_NODE_SELECT_STMT == nodeType(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    SSelectStmt* pSub = (SSelectStmt*)((STempTableNode*)pSelect->pFromTable)->pSubquery;
    if (pSelect->pPartitionByList) {
      if (!pSub->timeLineFromOrderBy && nodesListMatch(pSelect->pPartitionByList, pSub->pPartitionByList)) {
        *pRes = true;
        return code;
      }
      if (pSub->timeLineFromOrderBy && pSub->pOrderByList->length > 1) {
        SNodeList* pPartitionList = NULL;
        code = buildPartitionListFromOrderList(pSub->pOrderByList, pSelect->pPartitionByList->length, &pPartitionList);
        if (TSDB_CODE_SUCCESS == code) {
          bool match = nodesListMatch(pSelect->pPartitionByList, pPartitionList);
          nodesDestroyList(pPartitionList);

          if (match) {
            *pRes = true;
            return code;
          }
        }
      }
    }
  }
  if (TSDB_CODE_SUCCESS == code &&
      QUERY_NODE_SET_OPERATOR == nodeType(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    SSetOperator* pSub = (SSetOperator*)((STempTableNode*)pSelect->pFromTable)->pSubquery;
    if (pSelect->pPartitionByList && pSub->timeLineFromOrderBy && pSub->pOrderByList->length > 1) {
      SNodeList* pPartitionList = NULL;
      code = buildPartitionListFromOrderList(pSub->pOrderByList, pSelect->pPartitionByList->length, &pPartitionList);
      if (TSDB_CODE_SUCCESS == code) {
        bool match = nodesListMatch(pSelect->pPartitionByList, pPartitionList);
        nodesDestroyList(pPartitionList);

        if (match) {
          *pRes = true;
          return code;
        }
      }
    }
  }

  *pRes = false;
  return code;
}

bool isPrimaryKeyImpl(SNode* pExpr) {
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    return ((PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pExpr)->colId) && ((SColumnNode*)pExpr)->isPrimTs);
  } else if (QUERY_NODE_FUNCTION == nodeType(pExpr)) {
    SFunctionNode* pFunc = (SFunctionNode*)pExpr;
    if (FUNCTION_TYPE_SELECT_VALUE == pFunc->funcType || FUNCTION_TYPE_GROUP_KEY == pFunc->funcType ||
        FUNCTION_TYPE_FIRST == pFunc->funcType || FUNCTION_TYPE_LAST == pFunc->funcType ||
        FUNCTION_TYPE_LAST_ROW == pFunc->funcType || FUNCTION_TYPE_TIMETRUNCATE == pFunc->funcType) {
      return isPrimaryKeyImpl(nodesListGetNode(pFunc->pParameterList, 0));
    } else if (FUNCTION_TYPE_WSTART == pFunc->funcType || FUNCTION_TYPE_WEND == pFunc->funcType ||
               FUNCTION_TYPE_IROWTS == pFunc->funcType || FUNCTION_TYPE_IROWTS_ORIGIN == pFunc->funcType ||
               FUNCTION_TYPE_FORECAST_ROWTS == pFunc->funcType || FUNCTION_TYPE_IMPUTATION_ROWTS == pFunc->funcType ||
               FUNCTION_TYPE_TPREV_TS == pFunc->funcType || FUNCTION_TYPE_TCURRENT_TS == pFunc->funcType ||
               FUNCTION_TYPE_TNEXT_TS == pFunc->funcType || FUNCTION_TYPE_TWSTART == pFunc->funcType ||
               FUNCTION_TYPE_TWEND == pFunc->funcType || FUNCTION_TYPE_TPREV_LOCALTIME == pFunc->funcType ||
               FUNCTION_TYPE_TNEXT_LOCALTIME == pFunc->funcType || FUNCTION_TYPE_TLOCALTIME == pFunc->funcType) {
      return true;
    }
  } else if (QUERY_NODE_OPERATOR == nodeType(pExpr)) {
    SOperatorNode* pOper = (SOperatorNode*)pExpr;
    if (OP_TYPE_ADD != pOper->opType && OP_TYPE_SUB != pOper->opType) {
      return false;
    }
    if (!isPrimaryKeyImpl(pOper->pLeft)) {
      return false;
    }
    if (QUERY_NODE_VALUE != nodeType(pOper->pRight)) {
      return false;
    }
    return true;
  }
  return false;
}

static bool isPrimaryKey(STempTableNode* pTable, SNode* pExpr) {
  if (!isTimeLineQuery(pTable->pSubquery)) {
    return false;
  }
  return isPrimaryKeyImpl(pExpr);
}

static bool hasPkInTable(const STableMeta* pTableMeta) {
  bool hasPK = pTableMeta->tableInfo.numOfColumns >= 2 && pTableMeta->schema[1].flags & COL_IS_KEY;
  if (hasPK) {
    uDebug("has primary key, %s", pTableMeta->schema[1].name);
  } else {
    uDebug("no primary key, %s", pTableMeta->schema[1].name);
  }
  return hasPK;
}

static void setVtbColumnInfoBySchema(const SVirtualTableNode* pTable, const SSchema* pColSchema, int32_t tagFlag,
                                     SColumnNode* pCol) {
  strcpy(pCol->dbName, pTable->table.dbName);
  strcpy(pCol->tableAlias, pTable->table.tableAlias);
  strcpy(pCol->tableName, pTable->table.tableName);
  strcpy(pCol->colName, pColSchema->name);
  if ('\0' == pCol->node.aliasName[0]) {
    strcpy(pCol->node.aliasName, pColSchema->name);
  }
  if ('\0' == pCol->node.userAlias[0]) {
    strcpy(pCol->node.userAlias, pColSchema->name);
  }
  pCol->tableId = pTable->pMeta->uid;
  pCol->tableType = pTable->pMeta->tableType;
  pCol->colId = pColSchema->colId;
  pCol->colType = (tagFlag >= 0 ? COLUMN_TYPE_TAG : COLUMN_TYPE_COLUMN);
  pCol->hasIndex = false;
  pCol->node.resType.type = pColSchema->type;
  pCol->node.resType.bytes = pColSchema->bytes;
  if (TSDB_DATA_TYPE_TIMESTAMP == pCol->node.resType.type) {
    pCol->node.resType.precision = pTable->pMeta->tableInfo.precision;
  }
  pCol->tableHasPk = false;
  pCol->isPk = false;
  pCol->numOfPKs = 0;
  for (int32_t i = 0; i < pTable->pMeta->numOfColRefs; i++) {
    if (pTable->pMeta->colRef[i].hasRef && pTable->pMeta->colRef[i].id == pCol->colId) {
      pCol->hasRef = true;
      tstrncpy(pCol->refDbName, pTable->pMeta->colRef[i].refDbName, TSDB_DB_NAME_LEN);
      tstrncpy(pCol->refTableName, pTable->pMeta->colRef[i].refTableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(pCol->refColName, pTable->pMeta->colRef[i].refColName, TSDB_COL_NAME_LEN);
    }
  }
}

static void setColumnInfoBySchema(const SRealTableNode* pTable, const SSchema* pColSchema, int32_t tagFlag,
                                  SColumnNode* pCol, const SSchemaExt* pExtSchema) {
  tstrncpy(pCol->dbName, pTable->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pCol->tableAlias, pTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
  tstrncpy(pCol->tableName, pTable->table.tableName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pCol->colName, pColSchema->name, TSDB_COL_NAME_LEN);
  if ('\0' == pCol->node.aliasName[0]) {
    tstrncpy(pCol->node.aliasName, pColSchema->name, TSDB_COL_NAME_LEN);
  }
  if ('\0' == pCol->node.userAlias[0]) {
    tstrncpy(pCol->node.userAlias, pColSchema->name, TSDB_COL_NAME_LEN);
  }
  pCol->tableId = pTable->pMeta->uid;
  pCol->tableType = pTable->pMeta->tableType;
  pCol->colId = pColSchema->colId;
  pCol->colType = (tagFlag >= 0 ? COLUMN_TYPE_TAG : COLUMN_TYPE_COLUMN);
  pCol->hasIndex = (pColSchema != NULL && IS_IDX_ON(pColSchema));
  pCol->node.resType.type = pColSchema->type;
  pCol->node.resType.bytes = pColSchema->bytes;
  if (TSDB_DATA_TYPE_TIMESTAMP == pCol->node.resType.type) {
    pCol->node.resType.precision = pTable->pMeta->tableInfo.precision;
  }
  pCol->tableHasPk = hasPkInTable(pTable->pMeta);
  pCol->isPk = (pCol->tableHasPk) && (pColSchema->flags & COL_IS_KEY);
  pCol->numOfPKs = pTable->pMeta->tableInfo.numOfPKs;

  if (pExtSchema) fillTypeFromTypeMod(&pCol->node.resType, pExtSchema->typeMod);
}

static int32_t setColumnInfoByExpr(STempTableNode* pTable, SExprNode* pExpr, SColumnNode** pColRef, bool joinSrc) {
  SColumnNode* pCol = *pColRef;

  if (NULL == pExpr->pAssociation) {
    pExpr->pAssociation = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SAssociationNode));
    if (!pExpr->pAssociation) return terrno;
  }
  SAssociationNode assNode;
  assNode.pPlace = (SNode**)pColRef;
  assNode.pAssociationNode = (SNode*)*pColRef;
  if (NULL == taosArrayPush(pExpr->pAssociation, &assNode)) {
    return terrno;
  }

  tstrncpy(pCol->tableAlias, pTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
  pCol->isPrimTs = isPrimaryKeyImpl((SNode*)pExpr);
  pCol->colId = pCol->isPrimTs ? PRIMARYKEY_TIMESTAMP_COL_ID : 0;
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    pCol->colType = ((SColumnNode*)pExpr)->colType;
    // strcpy(pCol->dbName, ((SColumnNode*)pExpr)->dbName);
    // strcpy(pCol->tableName, ((SColumnNode*)pExpr)->tableName);
  }
  tstrncpy(pCol->colName, pExpr->aliasName, TSDB_COL_NAME_LEN);
  if ('\0' == pCol->node.aliasName[0]) {
    tstrncpy(pCol->node.aliasName, pExpr->aliasName, TSDB_COL_NAME_LEN);
  }
  if ('\0' == pCol->node.userAlias[0]) {
    tstrncpy(pCol->node.userAlias, pExpr->userAlias, TSDB_COL_NAME_LEN);
  }
  pCol->node.resType = pExpr->resType;
  pCol->node.joinSrc = pTable->table.inJoin && joinSrc;
  return TSDB_CODE_SUCCESS;
}

static void setColumnPrimTs(STranslateContext* pCxt, SColumnNode* pCol, const STableNode* pTable) {
  if (PRIMARYKEY_TIMESTAMP_COL_ID != pCol->colId) {
    return;
  }

  bool            joinQuery = false;
  SJoinTableNode* pJoinTable = NULL;
  if (QUERY_NODE_SELECT_STMT == nodeType(pCxt->pCurrStmt) && NULL != ((SSelectStmt*)pCxt->pCurrStmt)->pFromTable &&
      QUERY_NODE_JOIN_TABLE == nodeType(((SSelectStmt*)pCxt->pCurrStmt)->pFromTable)) {
    joinQuery = true;
    pJoinTable = (SJoinTableNode*)((SSelectStmt*)pCxt->pCurrStmt)->pFromTable;
  }

  pCol->isPrimTs = true;

  if (!joinQuery) {
    return;
  }

  switch (pJoinTable->joinType) {
    case JOIN_TYPE_INNER:
      pCol->isPrimTs = true;
      break;
    case JOIN_TYPE_LEFT:
      if (!IS_SEMI_JOIN(pJoinTable->subType) &&
          0 != strcmp(pTable->tableAlias, ((STableNode*)pJoinTable->pLeft)->tableAlias)) {
        pCol->isPrimTs = false;
      }
      break;
    case JOIN_TYPE_RIGHT:
      if (!IS_SEMI_JOIN(pJoinTable->subType) &&
          0 != strcmp(pTable->tableAlias, ((STableNode*)pJoinTable->pRight)->tableAlias)) {
        pCol->isPrimTs = false;
      }
      break;
    default:
      pCol->isPrimTs = false;
      break;
  }
}

static bool inStreamTriggerClause(STranslateContext* pCxt) {
  return pCxt->streamInfo.triggerClause;
}

static bool inStreamCalcClause(STranslateContext* pCxt) {
  return pCxt->streamInfo.calcClause;
}

static bool inStreamOutTableClause(STranslateContext* pCxt) {
  return pCxt->streamInfo.outTableClause;
}

static SNodeList* getStreamTriggerPartition(STranslateContext* pCxt) {
  return pCxt->streamInfo.triggerPartitionList;
}

static SNode* getStreamTriggerTable(STranslateContext* pCxt) {
  return pCxt->streamInfo.triggerTbl;
}

static int32_t createColumnsByTable(STranslateContext* pCxt, const STableNode* pTable, bool igTags, SNodeList* pList,
                                    bool skipProjRef) {
  int32_t code = 0;
  if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    const STableMeta* pMeta = ((SRealTableNode*)pTable)->pMeta;
    int32_t           nums = pMeta->tableInfo.numOfColumns +
                   (igTags ? 0
                           : ((TSDB_SUPER_TABLE == pMeta->tableType || inStreamTriggerClause(pCxt) ||((SRealTableNode*)pTable)->stbRewrite)
                                  ? pMeta->tableInfo.numOfTags
                                  : 0));
    for (int32_t i = 0; i < nums; ++i) {
      if (invisibleColumn(pCxt->pParseCxt->enableSysInfo, pMeta->tableType, pMeta->schema[i].flags)) {
        pCxt->pParseCxt->hasInvisibleCol = true;
        continue;
      }
      SColumnNode* pCol = NULL;
      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
      if (TSDB_CODE_SUCCESS != code) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, code);
      }
      SSchemaExt* pSchemaExt =
          pMeta->schemaExt ? (i >= pMeta->tableInfo.numOfColumns ? NULL : (pMeta->schemaExt + i)) : NULL;
      setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema + i, (i - pMeta->tableInfo.numOfColumns), pCol,
                            pSchemaExt);
      setColumnPrimTs(pCxt, pCol, pTable);
      code = nodesListStrictAppend(pList, (SNode*)pCol);
    }
  } else if (QUERY_NODE_TEMP_TABLE == nodeType(pTable)) {
    STempTableNode* pTempTable = (STempTableNode*)pTable;
    SNodeList*      pProjectList = getProjectList(pTempTable->pSubquery);
    SNode*          pNode;
    FOREACH(pNode, pProjectList) {
      SColumnNode* pCol = NULL;
      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
      if (TSDB_CODE_SUCCESS != code) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, code);
      }
      code = nodesListStrictAppend(pList, (SNode*)pCol);
      if (TSDB_CODE_SUCCESS == code) {
        SListCell* pCell = nodesListGetCell(pList, LIST_LENGTH(pList) - 1);
        code = setColumnInfoByExpr(pTempTable, (SExprNode*)pNode, (SColumnNode**)&pCell->pNode, true);
      }
      if (TSDB_CODE_SUCCESS == code) {
        if (!skipProjRef)
          pCol->projRefIdx = ((SExprNode*)pNode)->projIdx;  // only set proj ref when select * from (select ...)
      } else {
        break;
      }
    }
  } else if (QUERY_NODE_VIRTUAL_TABLE == nodeType(pTable)) {
    const STableMeta* pMeta = ((SVirtualTableNode*)pTable)->pMeta;
    int32_t           nums = pMeta->tableInfo.numOfColumns +
                   (igTags ? 0 : (TSDB_SUPER_TABLE == pMeta->tableType ? pMeta->tableInfo.numOfTags : 0));
    for (int32_t i = 0; i < nums; ++i) {
      SColumnNode* pCol = NULL;
      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
      if (TSDB_CODE_SUCCESS != code) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, code);
      }
      setVtbColumnInfoBySchema((SVirtualTableNode*)pTable, pMeta->schema + i, (i - pMeta->tableInfo.numOfColumns),
                               pCol);
      setColumnPrimTs(pCxt, pCol, pTable);
      code = nodesListStrictAppend(pList, (SNode*)pCol);
    }
  }
  return code;
}

static bool isInternalPrimaryKey(const SColumnNode* pCol) {
  return PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId &&
         (0 == strcmp(pCol->colName, ROWTS_PSEUDO_COLUMN_NAME) || 0 == strcmp(pCol->colName, C0_PSEUDO_COLUMN_NAME));
}

static int32_t createTbnameFunctionNode(SColumnNode* pCol, SFunctionNode** pFuncNode) {
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)pFuncNode);
  if (TSDB_CODE_SUCCESS != code) return code;

  tstrncpy((*pFuncNode)->functionName, "tbname", TSDB_FUNC_NAME_LEN);
  (*pFuncNode)->node.resType.type = TSDB_DATA_TYPE_BINARY;
  (*pFuncNode)->node.resType.bytes = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
  (*pFuncNode)->funcType = FUNCTION_TYPE_TBNAME;
  if (pCol->tableAlias[0] != '\0') {
    snprintf((*pFuncNode)->node.userAlias, sizeof((*pFuncNode)->node.userAlias), "%s.tbname", pCol->tableAlias);
  } else {
    snprintf((*pFuncNode)->node.userAlias, sizeof((*pFuncNode)->node.userAlias), "tbname");
  }

  tstrncpy((*pFuncNode)->node.aliasName, (*pFuncNode)->functionName, TSDB_COL_NAME_LEN);
  return TSDB_CODE_SUCCESS;
}

static int32_t findAndSetRealTableColumn(STranslateContext* pCxt, SColumnNode** pColRef, STableNode* pTable, bool* pFound) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SColumnNode* pCol = *pColRef;
  *pFound = false;

  const STableMeta* pMeta = ((SRealTableNode*)pTable)->pMeta;
  if (isInternalPrimaryKey(pCol)) {
    if (TSDB_SYSTEM_TABLE == pMeta->tableType) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pCol->colName);
    }

    setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema, -1, pCol, NULL);
    pCol->isPrimTs = true;
    *pFound = true;
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_SUPER_TABLE == pMeta->tableType && pCxt->pCurrStmt->type == QUERY_NODE_INSERT_STMT) {
    if (0 == strcmp(pCol->colName, "tbname")) {
      SFunctionNode* tbnameFuncNode = NULL;
      code = createTbnameFunctionNode(pCol, &tbnameFuncNode);
      if (TSDB_CODE_SUCCESS != code) return code;

      nodesDestroyNode((SNode*)*pColRef);
      *pColRef = (SColumnNode*)tbnameFuncNode;
      *pFound = true;
      return TSDB_CODE_SUCCESS;
    }
  }

  int32_t nums = pMeta->tableInfo.numOfTags + pMeta->tableInfo.numOfColumns;
  for (int32_t i = 0; i < nums; ++i) {
    if (0 == strcmp(pCol->colName, pMeta->schema[i].name) &&
        !invisibleColumn(pCxt->pParseCxt->enableSysInfo, pMeta->tableType, pMeta->schema[i].flags)) {
      SSchemaExt* pSchemaExt =
          pMeta->schemaExt ? (i >= pMeta->tableInfo.numOfColumns ? NULL : (pMeta->schemaExt + i)) : NULL;
      setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema + i, (i - pMeta->tableInfo.numOfColumns), pCol,
                            pSchemaExt);
      setColumnPrimTs(pCxt, pCol, pTable);
      *pFound = true;
      break;
    }
  }

  if (pCxt->showRewrite && pMeta->tableType == TSDB_SYSTEM_TABLE) {
    if (strncmp(pCol->dbName, TSDB_INFORMATION_SCHEMA_DB, strlen(TSDB_INFORMATION_SCHEMA_DB)) == 0 &&
        strncmp(pCol->tableName, TSDB_INS_DISK_USAGE, strlen(TSDB_INS_DISK_USAGE)) == 0 &&
        strncmp(pCol->colName, "db_name", strlen("db_name")) == 0) {
      pCol->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
      pCol->node.resType.bytes = 8;
      pCxt->skipCheck = true;
      ((SSelectStmt*)pCxt->pCurrStmt)->mixSysTableAndActualTable = true;
    }
  }
  return code;
}

static int32_t findAndSetTempTableColumn(STranslateContext* pCxt, SColumnNode** pColRef, STableNode* pTable,
                                         bool* pFound) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SColumnNode*    pCol = *pColRef;
  STempTableNode* pTempTable = (STempTableNode*)pTable;
  SNodeList*      pProjectList = getProjectList(pTempTable->pSubquery);
  SNode*          pNode;
  SExprNode*      pFoundExpr = NULL;
  FOREACH(pNode, pProjectList) {
    SExprNode* pExpr = (SExprNode*)pNode;
    if (0 == strcmp(pCol->colName, pExpr->aliasName)) {
      if (*pFound) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_AMBIGUOUS_COLUMN, pCol->colName);
      }
      pFoundExpr = pExpr;
      *pFound = true;
    } else if (isPrimaryKeyImpl(pNode) && isInternalPrimaryKey(pCol)) {
      pFoundExpr = pExpr;
      pCol->isPrimTs = true;
      *pFound = true;
    }
  }
  if (pFoundExpr) {
    code = setColumnInfoByExpr(pTempTable, pFoundExpr, pColRef, SQL_CLAUSE_FROM != pCxt->currClause);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return code;
}

static int32_t findAndSetVirtualTableColumn(STranslateContext* pCxt, SColumnNode** pColRef, STableNode* pTable,
                                            bool* pFound) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SColumnNode* pCol = *pColRef;

  const STableMeta* pMeta = ((SVirtualTableNode*)pTable)->pMeta;
  if (isInternalPrimaryKey(pCol)) {
    setVtbColumnInfoBySchema((SVirtualTableNode*)pTable, pMeta->schema, -1, pCol);
    pCol->isPrimTs = true;
    *pFound = true;
    return TSDB_CODE_SUCCESS;
  }

  int32_t nums = pMeta->tableInfo.numOfTags + pMeta->tableInfo.numOfColumns;
  for (int32_t i = 0; i < nums; ++i) {
    if (0 == strcmp(pCol->colName, pMeta->schema[i].name)) {
      setVtbColumnInfoBySchema((SVirtualTableNode*)pTable, pMeta->schema + i, (i - pMeta->tableInfo.numOfColumns),
                               pCol);
      setColumnPrimTs(pCxt, pCol, pTable);
      *pFound = true;
      break;
    }
  }

  return code;
}

static int32_t findAndSetColumn(STranslateContext* pCxt, SColumnNode** pColRef, STableNode* pTable, bool* pFound,
                                bool keepOriginTable) {
  SColumnNode* pCol = *pColRef;
  *pFound = false;
  bool            joinQuery = false;
  SJoinTableNode* pJoinTable = NULL;

  if (QUERY_NODE_SELECT_STMT == nodeType(pCxt->pCurrStmt) && NULL != ((SSelectStmt*)pCxt->pCurrStmt)->pFromTable &&
      QUERY_NODE_JOIN_TABLE == nodeType(((SSelectStmt*)pCxt->pCurrStmt)->pFromTable)) {
    joinQuery = true;
    pJoinTable = (SJoinTableNode*)((SSelectStmt*)pCxt->pCurrStmt)->pFromTable;
    if (isInternalPrimaryKey(pCol) && (!IS_WINDOW_JOIN(pJoinTable->subType) || !keepOriginTable)) {
      switch (pJoinTable->joinType) {
        case JOIN_TYPE_LEFT:
          pTable = (STableNode*)pJoinTable->pLeft;
          break;
        case JOIN_TYPE_RIGHT:
          pTable = (STableNode*)pJoinTable->pRight;
          break;
        default:
          break;
      }
    }
  }

  int32_t code = 0;
  switch (nodeType(pTable)) {
    case QUERY_NODE_REAL_TABLE:
      code = findAndSetRealTableColumn(pCxt, pColRef, pTable, pFound);
      break;
    case QUERY_NODE_TEMP_TABLE:
      code = findAndSetTempTableColumn(pCxt, pColRef, pTable, pFound);
      break;
    case QUERY_NODE_VIRTUAL_TABLE:
      code = findAndSetVirtualTableColumn(pCxt, pColRef, pTable, pFound);
      break;
    default:
      code = TSDB_CODE_PAR_INVALID_TABLE_TYPE;
      break;
  }

  return code;
}

static EDealRes translateColumnWithPrefix(STranslateContext* pCxt, SColumnNode** pCol) {
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t  nums = taosArrayGetSize(pTables);
  bool    foundTable = false;
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    if (belongTable((*pCol), pTable)) {
      foundTable = true;
      bool foundCol = false;
      pCxt->errCode = findAndSetColumn(pCxt, pCol, pTable, &foundCol, false);
      if (TSDB_CODE_SUCCESS != pCxt->errCode) {
        return DEAL_RES_ERROR;
      }
      if (foundCol) {
        break;
      }
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_COLUMN, (*pCol)->colName);
    }
  }
  if (!foundTable) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_TABLE_NOT_EXIST, (*pCol)->tableAlias);
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes translateColumnWithoutPrefix(STranslateContext* pCxt, SColumnNode** pCol) {
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t  nums = taosArrayGetSize(pTables);
  bool    found = false;
  bool    isInternalPk = isInternalPrimaryKey(*pCol);
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    bool        foundCol = false;
    pCxt->errCode = findAndSetColumn(pCxt, pCol, pTable, &foundCol, false);
    if (TSDB_CODE_SUCCESS != pCxt->errCode) {
      return DEAL_RES_ERROR;
    }
    if (foundCol) {
      if (found) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_AMBIGUOUS_COLUMN, (*pCol)->colName);
      }
      found = true;
    }
    if (isInternalPk) {  // this value should be updated
      break;
    }
  }
  if (!found) {
    if (isInternalPk) {
      if (isSelectStmt(pCxt->pCurrStmt) && NULL != ((SSelectStmt*)pCxt->pCurrStmt)->pWindow) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_NOT_ALLOWED_WIN_QUERY);
      }
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_INTERNAL_PK);
    } else {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_COLUMN, (*pCol)->colName);
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t getFuncInfo(STranslateContext* pCxt, SFunctionNode* pFunc);

static EDealRes translateColumnUseAlias(STranslateContext* pCxt, SColumnNode** pCol, bool* pFound) {
  SNodeList* pProjectionList = getProjectListFromCurrStmt(pCxt->pCurrStmt);
  SNode*     pNode;
  SNode*     pFoundNode = NULL;
  *pFound = false;
  FOREACH(pNode, pProjectionList) {
    SExprNode* pExpr = (SExprNode*)pNode;
    if (0 == strcmp((*pCol)->colName, pExpr->userAlias)) {
      if (true == *pFound) {
        if (nodesEqualNode(pFoundNode, pNode)) {
          continue;
        }
        pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ORDERBY_AMBIGUOUS, (*pCol)->colName);
        return DEAL_RES_ERROR;
      }
      *pFound = true;
      pFoundNode = pNode;
    }
  }
  if (*pFound) {
    if (nodeType(pFoundNode) == QUERY_NODE_FUNCTION && fmIsPlaceHolderFunc(((SFunctionNode*)pFoundNode)->funcId)) {
      if (pCxt->currClause != SQL_CLAUSE_WHERE && pCxt->currClause!= SQL_CLAUSE_SELECT) {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PLACE_HOLDER,
                                                "stream placeholder should only appear in select and where clause");
        return DEAL_RES_ERROR;
      }
    }
    SNode*  pNew = NULL;
    int32_t code = nodesCloneNode(pFoundNode, &pNew);
    if (NULL == pNew) {
      pCxt->errCode = code;
      return DEAL_RES_ERROR;
    }
    nodesDestroyNode(*(SNode**)pCol);
    *(SNode**)pCol = (SNode*)pNew;
  }
  return DEAL_RES_CONTINUE;
}

static void biMakeAliasNameInMD5(char* pExprStr, int32_t len, char* pAlias) {
  T_MD5_CTX ctx;
  tMD5Init(&ctx);
  tMD5Update(&ctx, pExprStr, len);
  tMD5Final(&ctx);
  char* p = pAlias;
  for (uint8_t i = 0; i < tListLen(ctx.digest); ++i) {
    snprintf(p, len + 1 - 2 * i, "%02x", ctx.digest[i]);
    p += 2;
  }
}

static int32_t biMakeTbnameProjectAstNode(char* funcName, char* tableAlias, SNode** pOutNode) {
  int32_t     code = 0;
  SValueNode* valNode = NULL;
  if (tableAlias != NULL) {
    SValueNode* n = NULL;
    code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&n);
    if (TSDB_CODE_SUCCESS != code) return code;
    n->literal = tstrdup(tableAlias);
    if (!n->literal) {
      nodesDestroyNode((SNode*)n);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    n->node.resType.type = TSDB_DATA_TYPE_BINARY;
    n->node.resType.bytes = strlen(n->literal);
    n->translate = false;
    valNode = n;
  }

  SFunctionNode* tbNameFunc = NULL;
  code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&tbNameFunc);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)valNode);
    valNode = NULL;
  }
  if (TSDB_CODE_SUCCESS == code) {
    tstrncpy(tbNameFunc->functionName, "tbname", TSDB_FUNC_NAME_LEN);
    if (valNode != NULL) {
      code = nodesListMakeStrictAppend(&tbNameFunc->pParameterList, (SNode*)valNode);
      valNode = NULL;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    snprintf(tbNameFunc->node.userAlias, sizeof(tbNameFunc->node.userAlias), (tableAlias) ? "%s.tbname" : "%stbname",
             (tableAlias) ? tableAlias : "");
    tstrncpy(tbNameFunc->node.aliasName, tbNameFunc->functionName, TSDB_COL_NAME_LEN);
    if (funcName == NULL) {
      *pOutNode = (SNode*)tbNameFunc;
      return code;
    } else {
      SFunctionNode* multiResFunc = NULL;
      code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&multiResFunc);

      if (TSDB_CODE_SUCCESS == code) {
        tstrncpy(multiResFunc->functionName, funcName, TSDB_FUNC_NAME_LEN);
        code = nodesListMakeStrictAppend(&multiResFunc->pParameterList, (SNode*)tbNameFunc);
        if (TSDB_CODE_SUCCESS != code) tbNameFunc = NULL;
      }

      if (TSDB_CODE_SUCCESS == code) {
        if (tsKeepColumnName) {
          snprintf(multiResFunc->node.userAlias, sizeof(tbNameFunc->node.userAlias),
                   (tableAlias) ? "%s.tbname" : "%stbname", (tableAlias) ? tableAlias : "");
          tstrncpy(multiResFunc->node.aliasName, tbNameFunc->functionName, TSDB_COL_NAME_LEN);
        } else {
          snprintf(multiResFunc->node.userAlias, sizeof(multiResFunc->node.userAlias),
                   tableAlias ? "%s(%s.tbname)" : "%s(%stbname)", funcName, tableAlias ? tableAlias : "");
          biMakeAliasNameInMD5(multiResFunc->node.userAlias, strlen(multiResFunc->node.userAlias),
                               multiResFunc->node.aliasName);
        }
        *pOutNode = (SNode*)multiResFunc;
      } else {
        nodesDestroyNode((SNode*)multiResFunc);
      }
    }
  }
  if (TSDB_CODE_SUCCESS != code) nodesDestroyNode((SNode*)tbNameFunc);
  return code;
}

static int32_t biRewriteSelectFuncParamStar(STranslateContext* pCxt, SSelectStmt* pSelect, SNode* pNode,
                                            SListCell* pSelectListCell) {
  SNodeList* pTbnameNodeList = NULL;
  int32_t    code = nodesMakeList(&pTbnameNodeList);
  if (!pTbnameNodeList) return code;

  SFunctionNode* pFunc = (SFunctionNode*)pNode;
  if (strcasecmp(pFunc->functionName, "last") == 0 || strcasecmp(pFunc->functionName, "last_row") == 0 ||
      strcasecmp(pFunc->functionName, "first") == 0) {
    SNodeList* pParams = pFunc->pParameterList;
    SNode*     pPara = NULL;
    FOREACH(pPara, pParams) {
      if (nodesIsStar(pPara)) {
        SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
        size_t  n = taosArrayGetSize(pTables);
        for (int32_t i = 0; i < n && TSDB_CODE_SUCCESS == code; ++i) {
          STableNode* pTable = taosArrayGetP(pTables, i);
          if (nodeType(pTable) == QUERY_NODE_REAL_TABLE && ((SRealTableNode*)pTable)->pMeta != NULL &&
              ((SRealTableNode*)pTable)->pMeta->tableType == TSDB_SUPER_TABLE) {
            SNode* pTbnameNode = NULL;
            code = biMakeTbnameProjectAstNode(pFunc->functionName, NULL, &pTbnameNode);
            if (TSDB_CODE_SUCCESS == code) code = nodesListStrictAppend(pTbnameNodeList, pTbnameNode);
          }
        }
        if (TSDB_CODE_SUCCESS == code && LIST_LENGTH(pTbnameNodeList) > 0) {
          nodesListInsertListAfterPos(pSelect->pProjectionList, pSelectListCell, pTbnameNodeList);
          pTbnameNodeList = NULL;
        }
      } else if (nodesIsTableStar(pPara)) {
        char*       pTableAlias = ((SColumnNode*)pPara)->tableAlias;
        STableNode* pTable = NULL;
        code = findTable(pCxt, pTableAlias, &pTable);
        if (TSDB_CODE_SUCCESS == code && nodeType(pTable) == QUERY_NODE_REAL_TABLE &&
            ((SRealTableNode*)pTable)->pMeta != NULL &&
            ((SRealTableNode*)pTable)->pMeta->tableType == TSDB_SUPER_TABLE) {
          SNode* pTbnameNode = NULL;
          code = biMakeTbnameProjectAstNode(pFunc->functionName, pTableAlias, &pTbnameNode);
          if (TSDB_CODE_SUCCESS == code) code = nodesListStrictAppend(pTbnameNodeList, pTbnameNode);
        }
        if (TSDB_CODE_SUCCESS == code && LIST_LENGTH(pTbnameNodeList) > 0) {
          nodesListInsertListAfterPos(pSelect->pProjectionList, pSelectListCell, pTbnameNodeList);
          pTbnameNodeList = NULL;
        }
      }
      if (TSDB_CODE_SUCCESS != code) break;
    }
  }
  nodesDestroyList(pTbnameNodeList);
  return code;
}

// after translate from
// before translate select list
int32_t biRewriteSelectStar(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SNode*     pNode = NULL;
  SNodeList* pTbnameNodeList = NULL;
  code = nodesMakeList(&pTbnameNodeList);
  if (!pTbnameNodeList) return code;
  WHERE_EACH(pNode, pSelect->pProjectionList) {
    if (nodesIsStar(pNode)) {
      SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
      size_t  n = taosArrayGetSize(pTables);
      for (int32_t i = 0; i < n && TSDB_CODE_SUCCESS == code; ++i) {
        STableNode* pTable = taosArrayGetP(pTables, i);
        if (nodeType(pTable) == QUERY_NODE_REAL_TABLE && ((SRealTableNode*)pTable)->pMeta != NULL &&
            ((SRealTableNode*)pTable)->pMeta->tableType == TSDB_SUPER_TABLE) {
          SNode* pTbnameNode = NULL;
          code = biMakeTbnameProjectAstNode(NULL, NULL, &pTbnameNode);
          if (TSDB_CODE_SUCCESS == code) code = nodesListStrictAppend(pTbnameNodeList, pTbnameNode);
        }
      }
      if (TSDB_CODE_SUCCESS == code && LIST_LENGTH(pTbnameNodeList) > 0) {
        nodesListInsertListAfterPos(pSelect->pProjectionList, cell, pTbnameNodeList);
        pTbnameNodeList = NULL;
      }
    } else if (nodesIsTableStar(pNode)) {
      char*       pTableAlias = ((SColumnNode*)pNode)->tableAlias;
      STableNode* pTable = NULL;
      code = findTable(pCxt, pTableAlias, &pTable);
      if (TSDB_CODE_SUCCESS == code && nodeType(pTable) == QUERY_NODE_REAL_TABLE &&
          ((SRealTableNode*)pTable)->pMeta != NULL && ((SRealTableNode*)pTable)->pMeta->tableType == TSDB_SUPER_TABLE) {
        SNode* pTbnameNode = NULL;
        code = biMakeTbnameProjectAstNode(NULL, pTableAlias, &pTbnameNode);
        if (TSDB_CODE_SUCCESS == code) code = nodesListStrictAppend(pTbnameNodeList, pTbnameNode);
      }
      if (TSDB_CODE_SUCCESS == code && LIST_LENGTH(pTbnameNodeList) > 0) {
        nodesListInsertListAfterPos(pSelect->pProjectionList, cell, pTbnameNodeList);
        pTbnameNodeList = NULL;
      }
    } else if (nodeType(pNode) == QUERY_NODE_FUNCTION) {
      code = biRewriteSelectFuncParamStar(pCxt, pSelect, pNode, cell);
    }
    if (TSDB_CODE_SUCCESS != code) break;
    WHERE_NEXT;
  }
  nodesDestroyList(pTbnameNodeList);

  return code;
}

int32_t biRewriteToTbnameFunc(STranslateContext* pCxt, SNode** ppNode, bool* pRet) {
  SColumnNode* pCol = (SColumnNode*)(*ppNode);
  if ((strcasecmp(pCol->colName, "tbname") == 0) && ((SSelectStmt*)pCxt->pCurrStmt)->pFromTable &&
      QUERY_NODE_REAL_TABLE == nodeType(((SSelectStmt*)pCxt->pCurrStmt)->pFromTable)) {
    SFunctionNode* tbnameFuncNode = NULL;
    int32_t        code = biMakeTbnameProjectAstNode(NULL, (pCol->tableAlias[0] != '\0') ? pCol->tableAlias : NULL,
                                                     (SNode**)&tbnameFuncNode);
    if (TSDB_CODE_SUCCESS != code) return code;
    tbnameFuncNode->node.resType = pCol->node.resType;
    tstrncpy(tbnameFuncNode->node.aliasName, pCol->node.aliasName, TSDB_COL_NAME_LEN);
    tstrncpy(tbnameFuncNode->node.userAlias, pCol->node.userAlias, TSDB_COL_NAME_LEN);

    nodesDestroyNode(*ppNode);
    *ppNode = (SNode*)tbnameFuncNode;
    *pRet = true;
    return TSDB_CODE_SUCCESS;
  }

  *pRet = false;
  return TSDB_CODE_SUCCESS;
}

int32_t biCheckCreateTableTbnameCol(STranslateContext* pCxt, SNodeList* pTags, SNodeList* pCols) {
  if (pTags) {
    SNode* pNode = NULL;
    FOREACH(pNode, pTags) {
      SColumnDefNode* pTag = (SColumnDefNode*)pNode;
      if (strcasecmp(pTag->colName, "tbname") == 0) {
        int32_t code = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAG_NAME,
                                               "tbname can not used for tags in BI mode");
        return code;
      }
    }
  }
  if (pCols) {
    SNode* pNode = NULL;
    FOREACH(pNode, pCols) {
      SColumnDefNode* pCol = (SColumnDefNode*)pNode;
      if (strcasecmp(pCol->colName, "tbname") == 0) {
        int32_t code = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN,
                                               "tbname can not used for columns in BI mode");
        return code;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool clauseSupportAlias(ESqlClause clause) {
  return SQL_CLAUSE_GROUP_BY == clause || SQL_CLAUSE_PARTITION_BY == clause || SQL_CLAUSE_ORDER_BY == clause;
}

static EDealRes translateColumnInGroupByClause(STranslateContext* pCxt, SColumnNode** pCol, bool* translateAsAlias) {
  *translateAsAlias = false;
  // count(*)/first(*)/last(*) and so on
  if (0 == strcmp((*pCol)->colName, "*")) {
    return DEAL_RES_CONTINUE;
  }

  if (pCxt->pParseCxt->biMode) {
    SNode** ppNode = (SNode**)pCol;
    bool    ret;
    pCxt->errCode = biRewriteToTbnameFunc(pCxt, ppNode, &ret);
    if (TSDB_CODE_SUCCESS != pCxt->errCode) return DEAL_RES_ERROR;
    if (ret) {
      return translateFunction(pCxt, (SFunctionNode**)ppNode);
    }
  }

  EDealRes res = DEAL_RES_CONTINUE;
  if ('\0' != (*pCol)->tableAlias[0]) {
    res = translateColumnWithPrefix(pCxt, pCol);
  } else {
    bool found = false;
    res = translateColumnWithoutPrefix(pCxt, pCol);
    if (!(*pCol)->node.asParam && res != DEAL_RES_CONTINUE && res != DEAL_RES_END &&
        pCxt->errCode != TSDB_CODE_PAR_AMBIGUOUS_COLUMN) {
      res = translateColumnUseAlias(pCxt, pCol, &found);
      *translateAsAlias = true;
    }
  }
  return res;
}

static EDealRes translateColumn(STranslateContext* pCxt, SColumnNode** pCol) {
  if (NULL == pCxt->pCurrStmt ||
      (isSelectStmt(pCxt->pCurrStmt) && NULL == ((SSelectStmt*)pCxt->pCurrStmt)->pFromTable)) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_COLUMN, (*pCol)->colName);
  }

  // count(*)/first(*)/last(*) and so on
  if (0 == strcmp((*pCol)->colName, "*")) {
    return DEAL_RES_CONTINUE;
  }

  if (pCxt->pParseCxt->biMode) {
    SNode** ppNode = (SNode**)pCol;
    bool    ret;
    pCxt->errCode = biRewriteToTbnameFunc(pCxt, ppNode, &ret);
    if (TSDB_CODE_SUCCESS != pCxt->errCode) return DEAL_RES_ERROR;
    if (ret) {
      return translateFunction(pCxt, (SFunctionNode**)ppNode);
    }
  }

  EDealRes res = DEAL_RES_CONTINUE;
  if ('\0' != (*pCol)->tableAlias[0]) {
    res = translateColumnWithPrefix(pCxt, pCol);
  } else {
    bool found = false;
    if (pCxt->currClause == SQL_CLAUSE_ORDER_BY) {
      if ((isSelectStmt(pCxt->pCurrStmt) && !(*pCol)->node.asParam) ||
          isSetOperator(pCxt->pCurrStmt)) {
        // match column in alias first if column is 'bare' in select stmt
        // or it is in set operator
        res = translateColumnUseAlias(pCxt, pCol, &found);
      }
    }
    if (DEAL_RES_ERROR != res && !found) {
      if (isSetOperator(pCxt->pCurrStmt)) {
        res = generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_ORDERBY_UNKNOWN_EXPR, (*pCol)->colName);
      } else {
        // match column in table if not found or column is part of an expression in select stmt
        res = translateColumnWithoutPrefix(pCxt, pCol);
      }
    }
    if (clauseSupportAlias(pCxt->currClause) && !(*pCol)->node.asParam && res != DEAL_RES_CONTINUE &&
        res != DEAL_RES_END) {
      res = translateColumnUseAlias(pCxt, pCol, &found);
    }
  }
  return res;
}

static int32_t parseTimeFromValueNode(STranslateContext* pCxt, SValueNode* pVal) {
  if (IS_NUMERIC_TYPE(pVal->node.resType.type) || TSDB_DATA_TYPE_BOOL == pVal->node.resType.type) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
    int64_t value = 0;
    if (IS_UNSIGNED_NUMERIC_TYPE(pVal->node.resType.type)) {
      value = pVal->datum.u;
    } else if (IS_FLOAT_TYPE(pVal->node.resType.type)) {
      value = pVal->datum.d;
    } else if (TSDB_DATA_TYPE_BOOL == pVal->node.resType.type) {
      value = pVal->datum.b;
    } else {
      value = pVal->datum.i;
    }
    pVal->datum.i = value;
    return TSDB_CODE_SUCCESS;
  } else if (IS_VAR_DATA_TYPE(pVal->node.resType.type) || TSDB_DATA_TYPE_TIMESTAMP == pVal->node.resType.type) {
    if (TSDB_CODE_SUCCESS == taosParseTime(pVal->literal, &pVal->datum.i, strlen(pVal->literal),
                                           pVal->node.resType.precision, pVal->tz)) {
      return TSDB_CODE_SUCCESS;
    }
    char* pEnd = NULL;
    pVal->datum.i = taosStr2Int64(pVal->literal, &pEnd, 10);
    return (NULL != pEnd && '\0' == *pEnd) ? TSDB_CODE_SUCCESS : TSDB_CODE_PAR_WRONG_VALUE_TYPE;
  } else {
    return TSDB_CODE_PAR_WRONG_VALUE_TYPE;
  }
}

static int32_t parseBoolFromValueNode(STranslateContext* pCxt, SValueNode* pVal) {
  if (IS_VAR_DATA_TYPE(pVal->node.resType.type) || TSDB_DATA_TYPE_BOOL == pVal->node.resType.type) {
    pVal->datum.b = (0 == strcasecmp(pVal->literal, "true"));
    return TSDB_CODE_SUCCESS;
  } else if (IS_INTEGER_TYPE(pVal->node.resType.type)) {
    pVal->datum.b = (0 != taosStr2Int64(pVal->literal, NULL, 10));
    return TSDB_CODE_SUCCESS;
  } else if (IS_FLOAT_TYPE(pVal->node.resType.type)) {
    pVal->datum.b = (0 != taosStr2Double(pVal->literal, NULL));
    return TSDB_CODE_SUCCESS;
  } else {
    return TSDB_CODE_PAR_WRONG_VALUE_TYPE;
  }
}

static EDealRes translateDurationValue(STranslateContext* pCxt, SValueNode* pVal) {
  if (strncmp(pVal->literal, AUTO_DURATION_LITERAL, strlen(AUTO_DURATION_LITERAL) + 1) == 0) {
    pVal->datum.i = AUTO_DURATION_VALUE;
    pVal->unit = getPrecisionUnit(pVal->node.resType.precision);
  } else if (parseNatualDuration(pVal->literal, strlen(pVal->literal), &pVal->datum.i, &pVal->unit,
                                 pVal->node.resType.precision, true) != TSDB_CODE_SUCCESS) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
  }
  *(int64_t*)&pVal->typeData = pVal->datum.i;
  return DEAL_RES_CONTINUE;
}

static EDealRes translateTimeOffsetValue(STranslateContext* pCxt, SValueNode* pVal) {
  if (parseNatualDuration(pVal->literal, strlen(pVal->literal), &pVal->datum.i, &pVal->unit,
                          pVal->node.resType.precision, true) != TSDB_CODE_SUCCESS) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
  }
  *(int64_t*)&pVal->typeData = pVal->datum.i;
  return DEAL_RES_CONTINUE;
}

static EDealRes translateNormalValue(STranslateContext* pCxt, SValueNode* pVal, SDataType targetDt, bool strict) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (targetDt.type) {
    case TSDB_DATA_TYPE_BOOL:
      if (TSDB_CODE_SUCCESS != parseBoolFromValueNode(pCxt, pVal)) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(bool*)&pVal->typeData = pVal->datum.b;
      break;
    case TSDB_DATA_TYPE_TINYINT: {
      code = toInteger(pVal->literal, strlen(pVal->literal), 10, &pVal->datum.i);
      if (strict && (TSDB_CODE_SUCCESS != code || !IS_VALID_TINYINT(pVal->datum.i))) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(int8_t*)&pVal->typeData = pVal->datum.i;
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      code = toInteger(pVal->literal, strlen(pVal->literal), 10, &pVal->datum.i);
      if (strict && (TSDB_CODE_SUCCESS != code || !IS_VALID_SMALLINT(pVal->datum.i))) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(int16_t*)&pVal->typeData = pVal->datum.i;
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      code = toInteger(pVal->literal, strlen(pVal->literal), 10, &pVal->datum.i);
      if (strict && (TSDB_CODE_SUCCESS != code || !IS_VALID_INT(pVal->datum.i))) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(int32_t*)&pVal->typeData = pVal->datum.i;
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      code = toInteger(pVal->literal, strlen(pVal->literal), 10, &pVal->datum.i);
      if (strict && TSDB_CODE_SUCCESS != code) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(int64_t*)&pVal->typeData = pVal->datum.i;
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      code = toUInteger(pVal->literal, strlen(pVal->literal), 10, &pVal->datum.u);
      if (strict && (TSDB_CODE_SUCCESS != code || pVal->datum.u > UINT8_MAX)) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(uint8_t*)&pVal->typeData = pVal->datum.u;
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      code = toUInteger(pVal->literal, strlen(pVal->literal), 10, &pVal->datum.u);
      if (strict && (TSDB_CODE_SUCCESS != code || pVal->datum.u > UINT16_MAX)) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(uint16_t*)&pVal->typeData = pVal->datum.u;
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      code = toUInteger(pVal->literal, strlen(pVal->literal), 10, &pVal->datum.u);
      if (strict && (TSDB_CODE_SUCCESS != code || pVal->datum.u > UINT32_MAX)) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(uint32_t*)&pVal->typeData = pVal->datum.u;
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      code = toUInteger(pVal->literal, strlen(pVal->literal), 10, &pVal->datum.u);
      if (strict && TSDB_CODE_SUCCESS != code) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(uint64_t*)&pVal->typeData = pVal->datum.u;
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      pVal->datum.d = taosStr2Double(pVal->literal, NULL);
      if (strict && !IS_VALID_FLOAT(pVal->datum.d)) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(float*)&pVal->typeData = pVal->datum.d;
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      pVal->datum.d = taosStr2Double(pVal->literal, NULL);
      if (strict && (((pVal->datum.d == HUGE_VAL || pVal->datum.d == -HUGE_VAL) && ERRNO == ERANGE) ||
                     isinf(pVal->datum.d) || isnan(pVal->datum.d))) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(double*)&pVal->typeData = pVal->datum.d;
      break;
    }
    case TSDB_DATA_TYPE_VARBINARY: {
      if (pVal->node.resType.type != TSDB_DATA_TYPE_BINARY) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }

      void*    data = NULL;
      uint32_t size = 0;
      uint32_t vlen = strlen(pVal->literal);
      bool     isHexChar = isHex(pVal->literal, vlen);
      if (isHexChar) {
        if (!isValidateHex(pVal->literal, vlen)) {
          return TSDB_CODE_PAR_INVALID_VARBINARY;
        }
        if (taosHex2Ascii(pVal->literal, vlen, &data, &size) < 0) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      } else {
        size = pVal->node.resType.bytes;
        data = pVal->literal;
      }

      if (size + VARSTR_HEADER_SIZE > targetDt.bytes) {
        if (isHexChar) taosMemoryFree(data);
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_VALUE_TOO_LONG, pVal->literal);
      }
      pVal->datum.p = taosMemoryCalloc(1, size + VARSTR_HEADER_SIZE);
      if (NULL == pVal->datum.p) {
        if (isHexChar) taosMemoryFree(data);
        return generateDealNodeErrMsg(pCxt, terrno);
      }
      varDataSetLen(pVal->datum.p, size);
      memcpy(varDataVal(pVal->datum.p), data, size);
      if (isHexChar) taosMemoryFree(data);
      break;
    }
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY: {
      int32_t vlen = IS_VAR_DATA_TYPE(pVal->node.resType.type) ? pVal->node.resType.bytes : strlen(pVal->literal);
      if (strict && (vlen > targetDt.bytes - VARSTR_HEADER_SIZE)) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_VALUE_TOO_LONG, pVal->literal);
      }
      int32_t len = TMIN(targetDt.bytes - VARSTR_HEADER_SIZE, vlen);
      pVal->datum.p = taosMemoryCalloc(1, len + VARSTR_HEADER_SIZE + 1);
      if (NULL == pVal->datum.p) {
        return generateDealNodeErrMsg(pCxt, terrno);
      }
      varDataSetLen(pVal->datum.p, len);
      strncpy(varDataVal(pVal->datum.p), pVal->literal, len);
      break;
    }
    case TSDB_DATA_TYPE_TIMESTAMP: {
      if (TSDB_CODE_SUCCESS != parseTimeFromValueNode(pCxt, pVal)) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(int64_t*)&pVal->typeData = pVal->datum.i;
      break;
    }
    case TSDB_DATA_TYPE_NCHAR: {
      pVal->datum.p = taosMemoryCalloc(1, targetDt.bytes + 1);
      if (NULL == pVal->datum.p) {
        return generateDealNodeErrMsg(pCxt, terrno);
      }

      int32_t len = 0;
      if (!taosMbsToUcs4(pVal->literal, strlen(pVal->literal), (TdUcs4*)varDataVal(pVal->datum.p),
                         targetDt.bytes - VARSTR_HEADER_SIZE, &len, pCxt->pParseCxt->charsetCxt)) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      varDataSetLen(pVal->datum.p, len);
      break;
    }
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes translateValueImpl(STranslateContext* pCxt, SValueNode* pVal, SDataType targetDt, bool strict) {
  if (pVal->placeholderNo > 0 || pVal->isNull) {
    return DEAL_RES_CONTINUE;
  }

  if (TSDB_DATA_TYPE_NULL == pVal->node.resType.type) {
    pVal->translate = true;
    pVal->isNull = true;
    return DEAL_RES_CONTINUE;
  }

  pVal->node.resType.precision = getPrecisionFromCurrStmt(pCxt->pCurrStmt, targetDt.precision);

  EDealRes res = DEAL_RES_CONTINUE;
  if (IS_DURATION_VAL(pVal->flag)) {
    res = translateDurationValue(pCxt, pVal);
  } else if (IS_TIME_OFFSET_VAL(pVal->flag)) {
    res = translateTimeOffsetValue(pCxt, pVal);
  } else {
    res = translateNormalValue(pCxt, pVal, targetDt, strict);
  }
  pVal->node.resType.type = targetDt.type;
  pVal->node.resType.bytes = targetDt.bytes;
  pVal->node.resType.scale = pVal->unit;
  pVal->translate = true;
  if (!strict && TSDB_DATA_TYPE_UBIGINT == pVal->node.resType.type && pVal->datum.u <= INT64_MAX) {
    pVal->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  }
  return res;
}

static int32_t calcTypeBytes(SDataType dt) {
  if (TSDB_DATA_TYPE_BINARY == dt.type || TSDB_DATA_TYPE_VARBINARY == dt.type || TSDB_DATA_TYPE_GEOMETRY == dt.type) {
    return dt.bytes + VARSTR_HEADER_SIZE;
  } else if (TSDB_DATA_TYPE_NCHAR == dt.type) {
    return dt.bytes * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
  } else if (IS_STR_DATA_BLOB(dt.type)) {
    return dt.bytes + BLOBSTR_HEADER_SIZE;
  } else {
    return dt.bytes;
  }
}

static EDealRes translateValue(STranslateContext* pCxt, SValueNode* pVal) {
  if (pVal->translate) {
    return TSDB_CODE_SUCCESS;
  }

  SDataType dt = pVal->node.resType;
  dt.bytes = calcTypeBytes(dt);
  return translateValueImpl(pCxt, pVal, dt, false);
}

static int32_t doTranslateValue(STranslateContext* pCxt, SValueNode* pVal) {
  return DEAL_RES_ERROR == translateValue(pCxt, pVal) ? pCxt->errCode : TSDB_CODE_SUCCESS;
}

static bool isMultiResFunc(SNode* pNode) {
  if (NULL == pNode) {
    return false;
  }
  if (QUERY_NODE_FUNCTION != nodeType(pNode) || !fmIsMultiResFunc(((SFunctionNode*)pNode)->funcId)) {
    return false;
  }
  if (FUNCTION_TYPE_TAGS == ((SFunctionNode*)pNode)->funcType) {
    return true;
  }
  SNodeList* pParameterList = ((SFunctionNode*)pNode)->pParameterList;
  if (LIST_LENGTH(pParameterList) > 1) {
    return true;
  }
  SNode* pParam = nodesListGetNode(pParameterList, 0);
  return (QUERY_NODE_COLUMN == nodeType(pParam) ? 0 == strcmp(((SColumnNode*)pParam)->colName, "*") : false);
}

static bool dataTypeEqual(const SDataType* l, const SDataType* r) {
  return (l->type == r->type && l->bytes == r->bytes && l->precision == r->precision && l->scale == r->scale);
}

// 0 means equal, 1 means the left shall prevail, -1 means the right shall prevail
static int32_t dataTypeComp(const SDataType* l, const SDataType* r) {
  if (l->type == TSDB_DATA_TYPE_NULL) return -1;
  if (l->type != r->type) {
    return 1;
  }

  if (l->bytes != r->bytes) {
    return l->bytes > r->bytes ? 1 : -1;
  }

  return (l->precision == r->precision && l->scale == r->scale) ? 0 : 1;
}

static EDealRes translateOperator(STranslateContext* pCxt, SOperatorNode* pOp) {
  if (isMultiResFunc(pOp->pLeft)) {
    generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_NOT_SUPPORT_MULTI_RESULT, ((SExprNode*)(pOp->pLeft))->userAlias);
    return DEAL_RES_ERROR;
  }
  if (isMultiResFunc(pOp->pRight)) {
    generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_NOT_SUPPORT_MULTI_RESULT, ((SExprNode*)(pOp->pRight))->userAlias);
    return DEAL_RES_ERROR;
  }

  int32_t res = scalarGetOperatorResultType(pOp);
  if (TSDB_CODE_SUCCESS != res) {
    pCxt->errCode = res;
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}

static EDealRes haveVectorFunction(SNode* pNode, void* pContext) {
  if (isAggFunc(pNode) || isIndefiniteRowsFunc(pNode) || isWindowPseudoColumnFunc(pNode) ||
      isInterpPseudoColumnFunc(pNode) || isForecastPseudoColumnFunc(pNode) || isColsFunctionResult(pNode)) {
    *((bool*)pContext) = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

int32_t findTable(STranslateContext* pCxt, const char* pTableAlias, STableNode** pOutput) {
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t  nums = taosArrayGetSize(pTables);
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    if (NULL == pTableAlias || 0 == strcmp(pTable->tableAlias, pTableAlias)) {
      *pOutput = pTable;
      return TSDB_CODE_SUCCESS;
    }
  }
  return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TABLE_NOT_EXIST, pTableAlias);
}

static bool isCountStar(SFunctionNode* pFunc) {
  if (FUNCTION_TYPE_COUNT != pFunc->funcType || 1 != LIST_LENGTH(pFunc->pParameterList)) {
    return false;
  }
  SNode* pPara = nodesListGetNode(pFunc->pParameterList, 0);
  return (QUERY_NODE_COLUMN == nodeType(pPara) && 0 == strcmp(((SColumnNode*)pPara)->colName, "*"));
}

static int32_t rewriteCountStarAsCount1(STranslateContext* pCxt, SFunctionNode* pCount) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SValueNode* pVal = NULL;
  code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  pVal->node.resType.type = TSDB_DATA_TYPE_INT;
  pVal->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  const int32_t val = 1;
  code = nodesSetValueNodeValue(pVal, (void*)&val);
  if (TSDB_CODE_SUCCESS == code) {
    pVal->translate = true;
    (void)nodesListErase(pCount->pParameterList, nodesListGetCell(pCount->pParameterList, 0));
    code = nodesListAppend(pCount->pParameterList, (SNode*)pVal);
  }
  return code;
}

STableNode* getJoinProbeTable(STranslateContext* pCxt) {
  if (QUERY_NODE_SELECT_STMT != nodeType(pCxt->pCurrStmt)) {
    return NULL;
  }
  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  if (NULL == pSelect->pFromTable || QUERY_NODE_JOIN_TABLE != nodeType(pSelect->pFromTable)) {
    return NULL;
  }

  SJoinTableNode* pJoin = (SJoinTableNode*)pSelect->pFromTable;
  switch (pJoin->joinType) {
    case JOIN_TYPE_INNER:
    case JOIN_TYPE_LEFT:
      return (STableNode*)pJoin->pLeft;
    case JOIN_TYPE_RIGHT:
      return (STableNode*)pJoin->pRight;
    default:
      break;
  }

  return NULL;
}

// count(*) is rewritten as count(ts) for scannning optimization
static int32_t rewriteCountStar(STranslateContext* pCxt, SFunctionNode* pCount) {
  SColumnNode* pCol = (SColumnNode*)nodesListGetNode(pCount->pParameterList, 0);
  STableNode*  pTable = NULL;
  SArray*      pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t       nums = taosArrayGetSize(pTables);
  int32_t      code = 0;
  if ('\0' == pCol->tableAlias[0] && nums > 1) {
    pTable = getJoinProbeTable(pCxt);
  } else {
    PAR_ERR_JRET(findTable(pCxt, ('\0' == pCol->tableAlias[0] ? NULL : pCol->tableAlias), &pTable));
  }

  if (NULL != pTable && QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    setColumnInfoBySchema((SRealTableNode*)pTable, ((SRealTableNode*)pTable)->pMeta->schema, -1, pCol, NULL);
  } else if (NULL != pTable && QUERY_NODE_VIRTUAL_TABLE == nodeType(pTable)) {
    setVtbColumnInfoBySchema((SVirtualTableNode*)pTable, ((SVirtualTableNode*)pTable)->pMeta->schema, -1, pCol);
    pCol->isPrimTs = true;
  } else {
    PAR_ERR_JRET(rewriteCountStarAsCount1(pCxt, pCount));
  }

  return code;
_return:
  parserError("%s failed to rewrite count(*), code:%d", __func__, code);
  return code;
}

static bool isCountNotNullValue(SFunctionNode* pFunc) {
  if (FUNCTION_TYPE_COUNT != pFunc->funcType || 1 != LIST_LENGTH(pFunc->pParameterList)) {
    return false;
  }
  SNode* pPara = nodesListGetNode(pFunc->pParameterList, 0);
  return (QUERY_NODE_VALUE == nodeType(pPara) && !((SValueNode*)pPara)->isNull);
}

// count(1) is rewritten as count(ts) for scannning optimization
static int32_t rewriteCountNotNullValue(STranslateContext* pCxt, SFunctionNode* pCount) {
  SValueNode*  pValue = (SValueNode*)nodesListGetNode(pCount->pParameterList, 0);
  STableNode*  pTable = NULL;
  bool         freeCol = false;
  SColumnNode* pCol = NULL;
  int32_t      code = TSDB_CODE_SUCCESS;

  PAR_ERR_JRET(findTable(pCxt, NULL, &pTable));
  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol));
  freeCol = true;
  if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    setColumnInfoBySchema((SRealTableNode*)pTable, ((SRealTableNode*)pTable)->pMeta->schema, -1, pCol, NULL);
  } else if (QUERY_NODE_VIRTUAL_TABLE == nodeType(pTable)) {
    setVtbColumnInfoBySchema((SVirtualTableNode*)pTable, ((SVirtualTableNode*)pTable)->pMeta->schema, -1, pCol);
    pCol->isPrimTs = true;
  } else {
    goto _return;
  }
  NODES_DESTORY_LIST(pCount->pParameterList);
  PAR_ERR_JRET(nodesListMakeAppend(&pCount->pParameterList, (SNode*)pCol));
  freeCol = false;

  return code;
_return:
  if (freeCol) {
    nodesDestroyNode((SNode*)pCol);
  }
  parserError("%s failed to rewrite count(1), code:%d", __func__, code);
  return code;
}

static bool isCountTbname(SFunctionNode* pFunc) {
  if (FUNCTION_TYPE_COUNT != pFunc->funcType || 1 != LIST_LENGTH(pFunc->pParameterList)) {
    return false;
  }
  SNode* pPara = nodesListGetNode(pFunc->pParameterList, 0);
  return (QUERY_NODE_FUNCTION == nodeType(pPara) && FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pPara)->funcType);
}

// count(tbname) is rewritten as count(ts) for scannning optimization
static int32_t rewriteCountTbname(STranslateContext* pCxt, SFunctionNode* pCount) {
  SFunctionNode* pTbname = (SFunctionNode*)nodesListGetNode(pCount->pParameterList, 0);
  const char*    pTableAlias = NULL;
  STableNode*    pTable = NULL;
  SColumnNode*   pCol = NULL;
  bool           freeCol = false;
  int32_t        code = TSDB_CODE_SUCCESS;

  if (LIST_LENGTH(pTbname->pParameterList) > 0) {
    pTableAlias = ((SValueNode*)nodesListGetNode(pTbname->pParameterList, 0))->literal;
  }

  PAR_ERR_JRET(findTable(pCxt, pTableAlias, &pTable));

  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol));
  freeCol = true;

  if (QUERY_NODE_VIRTUAL_TABLE == nodeType(pTable)) {
    setVtbColumnInfoBySchema((SVirtualTableNode*)pTable, ((SVirtualTableNode*)pTable)->pMeta->schema, -1, pCol);
    pCol->isPrimTs = true;
  } else {
    setColumnInfoBySchema((SRealTableNode*)pTable, ((SRealTableNode*)pTable)->pMeta->schema, -1, pCol, NULL);
  }

  NODES_DESTORY_LIST(pCount->pParameterList);
  PAR_ERR_JRET(nodesListMakeAppend(&pCount->pParameterList, (SNode*)pCol));
  freeCol = false;

  return code;
_return:
  if (freeCol) {
    nodesDestroyNode((SNode*)pCol);
  }
  parserError("%s failed to rewrite count(tbname), code:%d", __func__, code);
  return code;
}

static bool hasInvalidFuncNesting(SFunctionNode* pFunc) {
  bool hasInvalidFunc = false;
  nodesWalkExprs(pFunc->pParameterList, haveVectorFunction, &hasInvalidFunc);
  return hasInvalidFunc;
}

static int32_t getFuncInfo(STranslateContext* pCxt, SFunctionNode* pFunc) {
  // the time precision of the function execution environment
  pFunc->dual = pCxt->dual;
  if (!IS_DECIMAL_TYPE(pFunc->node.resType.type))
    pFunc->node.resType.precision = getPrecisionFromCurrStmt(pCxt->pCurrStmt, TSDB_TIME_PRECISION_MILLI);
  int32_t code = fmGetFuncInfo(pFunc, pCxt->msgBuf.buf, pCxt->msgBuf.len);
  if (TSDB_CODE_FUNC_NOT_BUILTIN_FUNTION == code) {
    code = getUdfInfo(pCxt, pFunc);
  }
  return code;
}

static int32_t translateAggFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsAggFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (beforeHaving(pCxt->currClause)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION);
  }
  if (hasInvalidFuncNesting(pFunc)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_AGG_FUNC_NESTING);
  }
  // The auto-generated COUNT function in the DELETE statement is legal
  if (isSelectStmt(pCxt->pCurrStmt) &&
      (((SSelectStmt*)pCxt->pCurrStmt)->hasIndefiniteRowsFunc || ((SSelectStmt*)pCxt->pCurrStmt)->hasMultiRowsFunc)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  }

  if (isCountStar(pFunc)) {
    return rewriteCountStar(pCxt, pFunc);
  }
  if (isCountNotNullValue(pFunc) && !pCxt->dual) {
    return rewriteCountNotNullValue(pCxt, pFunc);
  }
  if (isCountTbname(pFunc)) {
    return rewriteCountTbname(pCxt, pFunc);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateIndefiniteRowsFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsIndefiniteRowsFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!isSelectStmt(pCxt->pCurrStmt) || SQL_CLAUSE_SELECT != pCxt->currClause) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  }
  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  if (pSelect->hasAggFuncs || pSelect->hasMultiRowsFunc) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  }
  if (pSelect->hasIndefiniteRowsFunc &&
      (FUNC_RETURN_ROWS_INDEFINITE == pSelect->returnRows || pSelect->returnRows != fmGetFuncReturnRows(pFunc)) &&
      (pSelect->lastProcessByRowFuncId == -1 || !fmIsProcessByRowFunc(pFunc->funcId))) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  }
  if (pSelect->lastProcessByRowFuncId != -1 && pSelect->lastProcessByRowFuncId != pFunc->funcId) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_DIFFERENT_BY_ROW_FUNC);
  }
  if (NULL != pSelect->pWindow || NULL != pSelect->pGroupByList) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s function is not supported in window query or group query", pFunc->functionName);
  }
  if (hasInvalidFuncNesting(pFunc)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_AGG_FUNC_NESTING);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateMultiRowsFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsMultiRowsFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!isSelectStmt(pCxt->pCurrStmt) || SQL_CLAUSE_SELECT != pCxt->currClause ||
      ((SSelectStmt*)pCxt->pCurrStmt)->hasIndefiniteRowsFunc || ((SSelectStmt*)pCxt->pCurrStmt)->hasAggFuncs ||
      ((SSelectStmt*)pCxt->pCurrStmt)->hasMultiRowsFunc) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  }
  if (hasInvalidFuncNesting(pFunc)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_AGG_FUNC_NESTING);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateInterpFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsInterpFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }

  if (!isSelectStmt(pCxt->pCurrStmt) || (SQL_CLAUSE_SELECT != pCxt->currClause && SQL_CLAUSE_ORDER_BY != pCxt->currClause)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  }
  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  SNode*       pTable = pSelect->pFromTable;

  if (pSelect->hasAggFuncs || pSelect->hasMultiRowsFunc || pSelect->hasIndefiniteRowsFunc) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  }

  if (pSelect->hasInterpFunc &&
      (FUNC_RETURN_ROWS_INDEFINITE == pSelect->returnRows || pSelect->returnRows != fmGetFuncReturnRows(pFunc))) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s ignoring null value options cannot be used when applying to multiple columns",
                                   pFunc->functionName);
  }

  if (NULL != pSelect->pWindow || NULL != pSelect->pGroupByList) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s function is not supported in window query or group query", pFunc->functionName);
  }
  if (hasInvalidFuncNesting(pFunc)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_AGG_FUNC_NESTING);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateInterpPseudoColumnFunc(STranslateContext* pCxt, SNode** ppNode, bool* pRewriteToColumn) {
  SFunctionNode* pFunc = (SFunctionNode*)(*ppNode);
  if (!fmIsInterpPseudoColumnFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!isSelectStmt(pCxt->pCurrStmt)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s must be used in select statements", pFunc->functionName);
  }
  if (pCxt->currClause == SQL_CLAUSE_WHERE) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_INTERP_CLAUSE,
                                   "%s is not allowed in where clause", pFunc->functionName);
  }

  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  SNode*       pNode = NULL;
  bool         bFound = false;
  FOREACH(pNode, pSelect->pProjectionList) {
    if (nodeType(pNode) == QUERY_NODE_FUNCTION && strcasecmp(((SFunctionNode*)pNode)->functionName, "interp") == 0) {
      bFound = true;
      break;
    }
  }
  if (!bFound) {
    *pRewriteToColumn = true;
    int32_t code = replacePsedudoColumnFuncWithColumn(pCxt, ppNode);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    (void)translateColumn(pCxt, (SColumnNode**)ppNode);
    return pCxt->errCode;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateForecastFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsForecastFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!isSelectStmt(pCxt->pCurrStmt) || SQL_CLAUSE_SELECT != pCxt->currClause) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  }
  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  SNode*       pTable = pSelect->pFromTable;

  if (pSelect->hasAggFuncs || pSelect->hasMultiRowsFunc || pSelect->hasIndefiniteRowsFunc) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  }

  if (pSelect->hasForecastFunc &&
      (FUNC_RETURN_ROWS_INDEFINITE == pSelect->returnRows || pSelect->returnRows != fmGetFuncReturnRows(pFunc))) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s ignoring null value options cannot be used when applying to multiple columns",
                                   pFunc->functionName);
  }

  if (NULL != pSelect->pWindow || NULL != pSelect->pGroupByList) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s function is not supported in window query or group query", pFunc->functionName);
  }
  if (hasInvalidFuncNesting(pFunc)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_AGG_FUNC_NESTING);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateAnalysisPseudoColumnFunc(STranslateContext* pCxt, SNode** ppNode, bool* pRewriteToColumn) {
  SFunctionNode* pFunc = (SFunctionNode*)(*ppNode);
  SSelectStmt*   pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  SNode*         pNode = NULL;
  bool           bFound = false;
  int32_t        funcType = pFunc->funcType;

  if (!fmIsAnalysisPseudoColumnFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }

  if (!isSelectStmt(pCxt->pCurrStmt)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s must be used in select statements", pFunc->functionName);
  }

  if (pCxt->currClause == SQL_CLAUSE_WHERE) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s is not allowed in where clause", pFunc->functionName);
  }

  FOREACH(pNode, pSelect->pProjectionList) {
    if (nodeType(pNode) != QUERY_NODE_FUNCTION) {
      continue;
    } 
    
    if ((funcType == FUNCTION_TYPE_FORECAST_ROWTS || funcType == FUNCTION_TYPE_FORECAST_HIGH ||
         funcType == FUNCTION_TYPE_FORECAST_LOW) && strcasecmp(((SFunctionNode*)pNode)->functionName, "forecast") == 0) {
      bFound = true;
      break;
    }

    if ((funcType == FUNCTION_TYPE_IMPUTATION_ROWTS || funcType == FUNCTION_TYPE_IMPUTATION_MARK) && 
        strcasecmp(((SFunctionNode*)pNode)->functionName, "imputation") == 0) {
      bFound = true;
      break;
    } 

    if (funcType == FUNCTION_TYPE_ANOMALY_MARK && pSelect->pWindow->type == QUERY_NODE_ANOMALY_WINDOW) {
      bFound = true;
      break;
    }
  }

  if (!bFound) {
    *pRewriteToColumn = true;
    int32_t code = replacePsedudoColumnFuncWithColumn(pCxt, ppNode);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    
    (void)translateColumn(pCxt, (SColumnNode**)ppNode);
    return pCxt->errCode;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateTimelineFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsTimelineFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!isSelectStmt(pCxt->pCurrStmt)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s function must be used in select statements", pFunc->functionName);
  }
  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  bool         isTimelineAlignedQuery = false;
  if ((NULL != pSelect->pFromTable && QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
       !isGlobalTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery))) {
    int32_t code = isTimeLineAlignedQuery(pCxt->pCurrStmt, &isTimelineAlignedQuery);
    if (TSDB_CODE_SUCCESS != code) return code;
    if (!isTimelineAlignedQuery)
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                     "%s function requires valid time series input", pFunc->functionName);
  }
  if (NULL != pSelect->pFromTable && QUERY_NODE_JOIN_TABLE == nodeType(pSelect->pFromTable) &&
      (TIME_LINE_GLOBAL != pSelect->timeLineCurMode && TIME_LINE_MULTI != pSelect->timeLineCurMode)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s function requires valid time series input", pFunc->functionName);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateDateTimeFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsDateTimeFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }

  if (!isSelectStmt(pCxt->pCurrStmt)) {
    return TSDB_CODE_SUCCESS;
  }

  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  pFunc->node.resType.precision = pSelect->precision;

  return TSDB_CODE_SUCCESS;
}

static bool hasFillClause(SNode* pCurrStmt) {
  if (!isSelectStmt(pCurrStmt)) {
    return false;
  }
  SSelectStmt* pSelect = (SSelectStmt*)pCurrStmt;
  return NULL != pSelect->pWindow && QUERY_NODE_INTERVAL_WINDOW == nodeType(pSelect->pWindow) &&
         NULL != ((SIntervalWindowNode*)pSelect->pWindow)->pFill;
}

static int32_t createTbnameFunction(SFunctionNode** ppFunc) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  tstrncpy(pFunc->functionName, "tbname", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.aliasName, "tbname", TSDB_COL_NAME_LEN);
  tstrncpy(pFunc->node.userAlias, "tbname", TSDB_COL_NAME_LEN);
  pFunc->node.resType.type = TSDB_DATA_TYPE_BINARY;
  pFunc->node.resType.bytes = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
  *ppFunc = pFunc;
  return code;
}

static EDealRes translatePlaceHolderFunc(STranslateContext* pCxt, SNode** pFunc) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SNode*         extraValue = NULL;
  SFunctionNode* pFuncNode = (SFunctionNode*)(*pFunc);

  if (!inStreamCalcClause(pCxt) && !inStreamOutTableClause(pCxt)) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "stream placeholder should only appear in create stream's query part"));
  }

  if (pCxt->currClause != SQL_CLAUSE_SELECT && pCxt->currClause != SQL_CLAUSE_WHERE) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "stream placeholder should only appear in select and where clause"));
  }

  switch (pFuncNode->funcType) {
    case FUNCTION_TYPE_TPREV_TS: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PREV_TS);
      PAR_ERR_JRET(nodesMakeValueNodeFromTimestamp(0, &extraValue));
      break;
    }
    case FUNCTION_TYPE_TCURRENT_TS: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_CURRENT_TS);
      PAR_ERR_JRET(nodesMakeValueNodeFromTimestamp(0, &extraValue));
      break;
    }
    case FUNCTION_TYPE_TNEXT_TS: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_NEXT_TS);
      PAR_ERR_JRET(nodesMakeValueNodeFromTimestamp(0, &extraValue));
      break;
    }
    case FUNCTION_TYPE_TWSTART: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_WSTART);
      PAR_ERR_JRET(nodesMakeValueNodeFromTimestamp(0, &extraValue));
      break;
    }
    case FUNCTION_TYPE_TWEND: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_WEND);
      PAR_ERR_JRET(nodesMakeValueNodeFromTimestamp(0, &extraValue));
      break;
    }
    case FUNCTION_TYPE_TWDURATION: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_WDURATION);
      PAR_ERR_JRET(nodesMakeValueNodeFromInt64(0, &extraValue));
      break;
    }
    case FUNCTION_TYPE_TWROWNUM: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_WROWNUM);
      PAR_ERR_JRET(nodesMakeValueNodeFromInt64(0, &extraValue));
      break;
    }
    case FUNCTION_TYPE_TPREV_LOCALTIME: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PREV_LOCAL);
      PAR_ERR_JRET(nodesMakeValueNodeFromTimestamp(0, &extraValue));
      break;
    }
    case FUNCTION_TYPE_TNEXT_LOCALTIME: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_NEXT_LOCAL);
      PAR_ERR_JRET(nodesMakeValueNodeFromTimestamp(0, &extraValue));
      break;
    }
    case FUNCTION_TYPE_TLOCALTIME: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_LOCALTIME);
      PAR_ERR_JRET(nodesMakeValueNodeFromTimestamp(0, &extraValue));
      break;
    }
    case FUNCTION_TYPE_TGRPID: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_GRPID);
      PAR_ERR_JRET(nodesMakeValueNodeFromInt64(0, &extraValue));
      break;
    }
    case FUNCTION_TYPE_PLACEHOLDER_TBNAME: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PARTITION_TBNAME);
      PAR_ERR_JRET(nodesMakeValueNodeFromString("", (SValueNode**)&extraValue));
      ((SValueNode*)extraValue)->node.resType.bytes = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
      break;
    }
    case FUNCTION_TYPE_PLACEHOLDER_COLUMN: {
      SNodeList* pPartitionList = getStreamTriggerPartition(pCxt);
      if (!pPartitionList) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "use %%n without partition list in trigger"));
      }
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PARTITION_IDX);
      SValueNode* pIndex = (SValueNode*)nodesListGetNode(pFuncNode->pParameterList, 0);
      if (pIndex == NULL) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "%%n : partition index is required"));
      }
      int64_t     index = *(int64_t*)nodesGetValueFromNode(pIndex);
      if (index < 1) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "%%n : partition index must be greater than 1"));
      }
      SExprNode*  pExpr = (SExprNode*)nodesListGetNode(pPartitionList, (int32_t)index - 1);
      if (pExpr == NULL) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "%%n : partition index out of range"));
      }

      PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&extraValue));
      ((SValueNode*)extraValue)->node.resType = pExpr->resType;
      ((SValueNode*)extraValue)->isNull = true;

      pFuncNode->node.resType = pExpr->resType;
      break;
    }
    default:
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "unsupported placeholder function %s", pFuncNode->functionName));
  }

  ((SValueNode*)extraValue)->notReserved = true;
  PAR_ERR_JRET(nodesListMakePushFront(&pFuncNode->pParameterList, extraValue));
  return DEAL_RES_CONTINUE;
_return:
  pCxt->errCode = code;
  parserError("translatePlaceHolderFunc failed with code %d", code);
  nodesDestroyNode(extraValue);
  return DEAL_RES_ERROR;
}
static int32_t translateForbidFillFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsForbidFillFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (hasFillClause(pCxt->pCurrStmt)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_FILL_NOT_ALLOWED_FUNC, pFunc->functionName);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateForbidSysTableFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsForbidSysTableFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }

  if (isSelectStmt(pCxt->pCurrStmt)) {
    SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
    SNode*       pTable = pSelect->pFromTable;
    if (NULL != pTable && QUERY_NODE_REAL_TABLE == nodeType(pTable) &&
        TSDB_SYSTEM_TABLE == ((SRealTableNode*)pTable)->pMeta->tableType) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYSTABLE_NOT_ALLOWED_FUNC, pFunc->functionName);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool fromSingleTable(SNode* table) {
  if (NULL == table) return false;
  if (table->type == QUERY_NODE_REAL_TABLE && ((SRealTableNode*)table)->pMeta) {
    int8_t type = ((SRealTableNode*)table)->pMeta->tableType;
    if (type == TSDB_CHILD_TABLE || type == TSDB_NORMAL_TABLE || type == TSDB_SYSTEM_TABLE) {
      return true;
    }
    if (((SRealTableNode*)table)->asSingleTable) {
      return true;
    }
  }
  return false;
}

static int32_t translateRepeatScanFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsRepeatScanFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!isSelectStmt(pCxt->pCurrStmt)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_ONLY_SUPPORT_SINGLE_TABLE,
                                   "%s is only supported in single table query", pFunc->functionName);
  }
  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  SNode*       pTable = pSelect->pFromTable;
  // select percentile() without from clause is also valid
  if ((NULL != pTable && (QUERY_NODE_REAL_TABLE != nodeType(pTable) ||
                          (TSDB_CHILD_TABLE != ((SRealTableNode*)pTable)->pMeta->tableType &&
                           TSDB_NORMAL_TABLE != ((SRealTableNode*)pTable)->pMeta->tableType)))) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_ONLY_SUPPORT_SINGLE_TABLE,
                                   "%s is only supported in single table query", pFunc->functionName);
  }
  if (NULL != pSelect->pPartitionByList) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s function is not supported in partition query", pFunc->functionName);
  }

  if (NULL != pSelect->pWindow) {
    if (QUERY_NODE_EVENT_WINDOW == nodeType(pSelect->pWindow) ||
        QUERY_NODE_COUNT_WINDOW == nodeType(pSelect->pWindow)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                     "%s function is not supported in count/event window", pFunc->functionName);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateBlockDistFunc(STranslateContext* pCtx, SFunctionNode* pFunc) {
  if (!fmIsBlockDistFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!isSelectStmt(pCtx->pCurrStmt)) {
    return generateSyntaxErrMsgExt(&pCtx->msgBuf, TSDB_CODE_PAR_ONLY_SUPPORT_SINGLE_TABLE,
                                   "%s is only supported in single table query", pFunc->functionName);
  }
  SSelectStmt* pSelect = (SSelectStmt*)pCtx->pCurrStmt;
  SNode*       pTable = pSelect->pFromTable;
  if (NULL != pTable && (QUERY_NODE_REAL_TABLE != nodeType(pTable) ||
                         (TSDB_SUPER_TABLE != ((SRealTableNode*)pTable)->pMeta->tableType &&
                          TSDB_CHILD_TABLE != ((SRealTableNode*)pTable)->pMeta->tableType &&
                          TSDB_NORMAL_TABLE != ((SRealTableNode*)pTable)->pMeta->tableType))) {
    return generateSyntaxErrMsgExt(&pCtx->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s is only supported on super table, child table or normal table",
                                   pFunc->functionName);
  }
  return TSDB_CODE_SUCCESS;
}
static int32_t translateDBUsageFunc(STranslateContext* pCtx, SFunctionNode* pFunc) {
  if (!fmIsDBUsageFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!isSelectStmt(pCtx->pCurrStmt)) {
    return generateSyntaxErrMsgExt(&pCtx->msgBuf, TSDB_CODE_PAR_ONLY_SUPPORT_SINGLE_TABLE,
                                   "%s is only supported in single table query", pFunc->functionName);
  }
  SSelectStmt* pSelect = (SSelectStmt*)pCtx->pCurrStmt;
  SNode*       pTable = pSelect->pFromTable;
  // if (NULL != pTable && (QUERY_NODE_REAL_TABLE != nodeType(pTable) ||
  //                        (TSDB_SUPER_TABLE != ((SRealTableNode*)pTable)->pMeta->tableType &&
  //                         TSDB_CHILD_TABLE != ((SRealTableNode*)pTable)->pMeta->tableType &&
  //                         TSDB_NORMAL_TABLE != ((SRealTableNode*)pTable)->pMeta->tableType))) {
  //   return generateSyntaxErrMsgExt(&pCtx->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
  //                                  "%s is only supported on super table, child table or normal table",
  //                                  pFunc->functionName);
  // }
  return TSDB_CODE_SUCCESS;
}

static bool isStarParam(SNode* pNode) { return nodesIsStar(pNode) || nodesIsTableStar(pNode); }

static int32_t translateMultiResFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsMultiResFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (SQL_CLAUSE_SELECT != pCxt->currClause) {
    SNode* pPara = nodesListGetNode(pFunc->pParameterList, 0);
    if (isStarParam(pPara)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                     "%s(*) is only supported in selected list", pFunc->functionName);
    }
  }
  if (tsKeepColumnName && 1 == LIST_LENGTH(pFunc->pParameterList) && !pFunc->node.asAlias && !pFunc->node.asParam) {
    tstrncpy(pFunc->node.aliasName, ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->aliasName,
             TSDB_COL_NAME_LEN);
    tstrncpy(pFunc->node.userAlias, ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->userAlias,
             TSDB_COL_NAME_LEN);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getMultiResFuncNum(SNodeList* pParameterList) {
  if (1 == LIST_LENGTH(pParameterList)) {
    return isStarParam(nodesListGetNode(pParameterList, 0)) ? 2 : 1;
  }
  return LIST_LENGTH(pParameterList);
}

static int32_t calcSelectFuncNum(SFunctionNode* pFunc, int32_t currSelectFuncNum) {
  if (fmIsCumulativeFunc(pFunc->funcId)) {
    return currSelectFuncNum > 0 ? currSelectFuncNum : 1;
  }
  if (fmIsSelectColsFunc(pFunc->funcId)) {
    return currSelectFuncNum;
  }
  return currSelectFuncNum + ((fmIsMultiResFunc(pFunc->funcId) && !fmIsLastRowFunc(pFunc->funcId))
                                  ? getMultiResFuncNum(pFunc->pParameterList)
                                  : 1);
}

static void setFuncClassification(STranslateContext* pCxt, SFunctionNode* pFunc) {
  SNode* pCurrStmt = pCxt->pCurrStmt;
  if (NULL != pCurrStmt && QUERY_NODE_SELECT_STMT == nodeType(pCurrStmt)) {
    SSelectStmt* pSelect = (SSelectStmt*)pCurrStmt;
    pSelect->hasAggFuncs = pSelect->hasAggFuncs ? true : fmIsAggFunc(pFunc->funcId);
    pSelect->hasCountFunc = pSelect->hasCountFunc ? true : (FUNCTION_TYPE_COUNT == pFunc->funcType);
    pSelect->hasRepeatScanFuncs = pSelect->hasRepeatScanFuncs ? true : fmIsRepeatScanFunc(pFunc->funcId);

    if (fmIsIndefiniteRowsFunc(pFunc->funcId)) {
      pSelect->hasIndefiniteRowsFunc = true;
      pSelect->returnRows = fmGetFuncReturnRows(pFunc);
    } else if (fmIsInterpFunc(pFunc->funcId)) {
      pSelect->returnRows = fmGetFuncReturnRows(pFunc);
    } else if (fmIsForecastFunc(pFunc->funcId)) {
      pSelect->returnRows = fmGetFuncReturnRows(pFunc);
    }
    if (fmIsProcessByRowFunc(pFunc->funcId)) {
      pSelect->lastProcessByRowFuncId = pFunc->funcId;
    }

    pSelect->hasMultiRowsFunc = pSelect->hasMultiRowsFunc ? true : fmIsMultiRowsFunc(pFunc->funcId);
    if (fmIsSelectFunc(pFunc->funcId)) {
      pSelect->hasSelectFunc = true;
      pSelect->selectFuncNum = calcSelectFuncNum(pFunc, pSelect->selectFuncNum);
    } else if (fmIsVectorFunc(pFunc->funcId)) {
      pSelect->hasOtherVectorFunc = true;
    }
    pSelect->hasUniqueFunc = pSelect->hasUniqueFunc ? true : (FUNCTION_TYPE_UNIQUE == pFunc->funcType);
    pSelect->hasTailFunc = pSelect->hasTailFunc ? true : (FUNCTION_TYPE_TAIL == pFunc->funcType);
    pSelect->hasInterpFunc = pSelect->hasInterpFunc ? true : (FUNCTION_TYPE_INTERP == pFunc->funcType);
    pSelect->hasTwaOrElapsedFunc = pSelect->hasTwaOrElapsedFunc ? true
                                                                : (FUNCTION_TYPE_TWA == pFunc->funcType ||
                                                                   FUNCTION_TYPE_ELAPSED == pFunc->funcType);
    pSelect->hasInterpPseudoColFunc = pSelect->hasInterpPseudoColFunc || fmIsInterpPseudoColumnFunc(pFunc->funcId);
    pSelect->hasForecastFunc = pSelect->hasForecastFunc || (FUNCTION_TYPE_FORECAST == pFunc->funcType);
    pSelect->hasGenericAnalysisFunc =
        pSelect->hasGenericAnalysisFunc || (FUNCTION_TYPE_IMPUTATION == pFunc->funcType) ||
        (FUNCTION_TYPE_DTW == pFunc->funcType) || (FUNCTION_TYPE_DTW_PATH == pFunc->funcType) ||
        (FUNCTION_TYPE_TLCC == pFunc->funcType) || (FUNCTION_TYPE_ANOMALYCHECK == pFunc->funcType);
    pSelect->hasForecastPseudoColFunc =
        pSelect->hasForecastPseudoColFunc
            || fmIsAnalysisPseudoColumnFunc(pFunc->funcId) && (pFunc->funcType != FUNCTION_TYPE_ANOMALY_MARK);
    pSelect->hasLastRowFunc = pSelect->hasLastRowFunc ? true : (FUNCTION_TYPE_LAST_ROW == pFunc->funcType);
    pSelect->hasLastFunc = pSelect->hasLastFunc ? true : (FUNCTION_TYPE_LAST == pFunc->funcType);
    pSelect->hasTimeLineFunc = pSelect->hasTimeLineFunc ? true : fmIsTimelineFunc(pFunc->funcId);
    pSelect->hasUdaf = pSelect->hasUdaf ? true : fmIsUserDefinedFunc(pFunc->funcId) && fmIsAggFunc(pFunc->funcId);
    if (SQL_CLAUSE_SELECT == pCxt->currClause) {
      pSelect->onlyHasKeepOrderFunc = pSelect->onlyHasKeepOrderFunc ? fmIsKeepOrderFunc(pFunc->funcId) : false;
    }
  }
}

static int32_t rewriteFuncToValue(STranslateContext* pCxt, char** pLiteral, SNode** pNode) {
  SValueNode* pVal = NULL;
  int32_t     code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  tstrncpy(pVal->node.aliasName, ((SExprNode*)*pNode)->aliasName, TSDB_COL_NAME_LEN);
  tstrncpy(pVal->node.userAlias, ((SExprNode*)*pNode)->userAlias, TSDB_COL_NAME_LEN);
  pVal->node.resType = ((SExprNode*)*pNode)->resType;
  if (NULL == pLiteral || NULL == *pLiteral) {
    pVal->isNull = true;
  } else {
    pVal->literal = *pLiteral;
    if (IS_VAR_DATA_TYPE(pVal->node.resType.type)) {
      pVal->node.resType.bytes = strlen(*pLiteral);
    }
    *pLiteral = NULL;
  }
  if (DEAL_RES_ERROR != translateValue(pCxt, pVal)) {
    nodesDestroyNode(*pNode);
    *pNode = (SNode*)pVal;
  } else {
    nodesDestroyNode((SNode*)pVal);
  }
  return pCxt->errCode;
}

static int32_t rewriteDatabaseFunc(STranslateContext* pCxt, SNode** pNode) {
  char* pCurrDb = NULL;
  if (NULL != pCxt->pParseCxt->db) {
    pCurrDb = taosStrdup((void*)pCxt->pParseCxt->db);
    if (NULL == pCurrDb) {
      return terrno;
    }
  }
  int32_t code = rewriteFuncToValue(pCxt, &pCurrDb, pNode);
  if (TSDB_CODE_SUCCESS != code) taosMemoryFree(pCurrDb);
  return code;
}

static int32_t rewriteClentVersionFunc(STranslateContext* pCxt, SNode** pNode) {
  char* pVer = taosStrdup((void*)td_version);
  if (NULL == pVer) {
    return terrno;
  }
  int32_t code = rewriteFuncToValue(pCxt, &pVer, pNode);
  if (TSDB_CODE_SUCCESS != code) taosMemoryFree(pVer);
  return code;
}

static int32_t rewriteServerVersionFunc(STranslateContext* pCxt, SNode** pNode) {
  char* pVer = taosStrdup((void*)pCxt->pParseCxt->svrVer);
  if (NULL == pVer) {
    return terrno;
  }
  int32_t code = rewriteFuncToValue(pCxt, &pVer, pNode);
  if (TSDB_CODE_SUCCESS != code) taosMemoryFree(pVer);
  return code;
}

static int32_t rewriteServerStatusFunc(STranslateContext* pCxt, SNode** pNode) {
  if (pCxt->pParseCxt->nodeOffline) {
    return TSDB_CODE_RPC_NETWORK_UNAVAIL;
  }
  char* pStatus = taosStrdup((void*)"1");
  if (!pStatus) {
    return terrno;
  }
  int32_t code = rewriteFuncToValue(pCxt, &pStatus, pNode);
  if (TSDB_CODE_SUCCESS != code) taosMemoryFree(pStatus);
  return code;
}

static int32_t rewriteUserFunc(STranslateContext* pCxt, SNode** pNode) {
  char    userConn[TSDB_USER_LEN + 1 + TSDB_FQDN_LEN] = {0};  // format 'user@host'
  int32_t len = tsnprintf(userConn, sizeof(userConn), "%s@", pCxt->pParseCxt->pUser);
  if (TSDB_CODE_SUCCESS != taosGetFqdn(userConn + len)) {
    return terrno;
  }
  char* pUserConn = taosStrdup((void*)userConn);
  if (NULL == pUserConn) {
    return terrno;
  }
  int32_t code = rewriteFuncToValue(pCxt, &pUserConn, pNode);
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(pUserConn);
  }
  return code;
}

static int32_t rewriteSystemInfoFunc(STranslateContext* pCxt, SNode** pNode) {
  switch (((SFunctionNode*)*pNode)->funcType) {
    case FUNCTION_TYPE_DATABASE:
      return rewriteDatabaseFunc(pCxt, pNode);
    case FUNCTION_TYPE_CLIENT_VERSION:
      return rewriteClentVersionFunc(pCxt, pNode);
    case FUNCTION_TYPE_SERVER_VERSION:
      return rewriteServerVersionFunc(pCxt, pNode);
    case FUNCTION_TYPE_SERVER_STATUS:
      return rewriteServerStatusFunc(pCxt, pNode);
    case FUNCTION_TYPE_CURRENT_USER:
    case FUNCTION_TYPE_USER:
      return rewriteUserFunc(pCxt, pNode);
    default:
      break;
  }
  return TSDB_CODE_PAR_INTERNAL_ERROR;
}

static int32_t replacePsedudoColumnFuncWithColumn(STranslateContext* pCxt, SNode** ppNode) {
  SColumnNode* pCol = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  SExprNode* pOldExpr = (SExprNode*)(*ppNode);
  // rewrite a.tbname == tbname(a)
  if (nodeType(*ppNode) == QUERY_NODE_FUNCTION && ((SFunctionNode*)(*ppNode))->funcType == FUNCTION_TYPE_TBNAME) {
    SFunctionNode* pFunc = (SFunctionNode*)(*ppNode);
    if (0 != LIST_LENGTH(pFunc->pParameterList)) {
      SValueNode* pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 0);
      pCol->node.resType = pOldExpr->resType;
      tstrncpy(pCol->node.aliasName, pVal->literal, TSDB_COL_NAME_LEN);
      tstrncpy(pCol->colName, pFunc->functionName, TSDB_COL_NAME_LEN);
      tstrncpy(pCol->node.aliasName, pCol->colName, TSDB_COL_NAME_LEN);
      tstrncpy(pCol->node.userAlias, pCol->colName, TSDB_COL_NAME_LEN);
      nodesDestroyNode(*ppNode);
      *ppNode = (SNode*)pCol;

      return TSDB_CODE_SUCCESS;
    }
  }
  pCol->node.resType = pOldExpr->resType;
  tstrncpy(pCol->node.aliasName, pOldExpr->aliasName, TSDB_COL_NAME_LEN);
  tstrncpy(pCol->node.userAlias, pOldExpr->userAlias, TSDB_COL_NAME_LEN);
  if (nodeType(*ppNode) == QUERY_NODE_FUNCTION) {
    tstrncpy(pCol->colName, ((SFunctionNode*)(*ppNode))->functionName, TSDB_COL_NAME_LEN);
  } else {
    tstrncpy(pCol->colName, pOldExpr->aliasName, TSDB_COL_NAME_LEN);
  }

  nodesDestroyNode(*ppNode);
  *ppNode = (SNode*)pCol;

  return TSDB_CODE_SUCCESS;
}

static int32_t rewriteToColumnAndRetranslate(STranslateContext* pCxt, SNode** ppNode, int32_t errCode) {
  int32_t code = replacePsedudoColumnFuncWithColumn(pCxt, ppNode);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  (void)translateColumn(pCxt, (SColumnNode**)ppNode);
  if (pCxt->errCode != TSDB_CODE_SUCCESS) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, errCode);
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t translateWindowPseudoColumnFunc(STranslateContext* pCxt, SNode** ppNode, bool* pRewriteToColumn) {
  SFunctionNode* pFunc = (SFunctionNode*)(*ppNode);
  if (!fmIsWindowPseudoColumnFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (isSetOperator(pCxt->pCurrStmt)) {
    *pRewriteToColumn = true;
    return rewriteToColumnAndRetranslate(pCxt, ppNode, TSDB_CODE_PAR_INVALID_WINDOW_PC);
  }
  if (!isSelectStmt(pCxt->pCurrStmt)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_WINDOW_PC);
  }
  if (((SSelectStmt*)pCxt->pCurrStmt)->pWindow && beforeWindow(pCxt->currClause)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_WINDOW_PC, "There mustn't be %s",
                                   pFunc->functionName);
  }
  if (NULL == ((SSelectStmt*)pCxt->pCurrStmt)->pWindow) {
    *pRewriteToColumn = true;
    return rewriteToColumnAndRetranslate(pCxt, ppNode, TSDB_CODE_PAR_INVALID_WINDOW_PC);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateScanPseudoColumnFunc(STranslateContext* pCxt, SNode** ppNode, bool* pRewriteToColumn) {
  SFunctionNode* pFunc = (SFunctionNode*)(*ppNode);
  if (!fmIsScanPseudoColumnFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (0 == LIST_LENGTH(pFunc->pParameterList)) {
    if (pFunc->funcType == FUNCTION_TYPE_FORECAST_LOW || pFunc->funcType == FUNCTION_TYPE_FORECAST_HIGH) {
      return TSDB_CODE_SUCCESS;
    }
    if (isSetOperator(pCxt->pCurrStmt)) {
      *pRewriteToColumn = true;
      return rewriteToColumnAndRetranslate(pCxt, ppNode, TSDB_CODE_PAR_INVALID_TBNAME);
    }
    if (!isSelectStmt(pCxt->pCurrStmt) || NULL == ((SSelectStmt*)pCxt->pCurrStmt)->pFromTable) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TBNAME);
    }
    if (QUERY_NODE_REAL_TABLE != nodeType(((SSelectStmt*)pCxt->pCurrStmt)->pFromTable) &&
        QUERY_NODE_VIRTUAL_TABLE != nodeType(((SSelectStmt*)pCxt->pCurrStmt)->pFromTable)) {
      *pRewriteToColumn = true;
      return rewriteToColumnAndRetranslate(pCxt, ppNode, TSDB_CODE_PAR_INVALID_TBNAME);
    }
  } else {
    SValueNode* pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 0);
    STableNode* pTable = NULL;
    pCxt->errCode = findTable(pCxt, pVal->literal, &pTable);
    if (TSDB_CODE_SUCCESS != pCxt->errCode || (NULL == pTable)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TBNAME);
    }
    if (nodeType(pTable) != QUERY_NODE_REAL_TABLE) {
      *pRewriteToColumn = true;
      return rewriteToColumnAndRetranslate(pCxt, ppNode, TSDB_CODE_PAR_INVALID_TBNAME);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateNormalFunction(STranslateContext* pCxt, SNode** ppNode) {
  SFunctionNode* pFunc = (SFunctionNode*)(*ppNode);
  int32_t        code = translateAggFunc(pCxt, pFunc);
  if (TSDB_CODE_SUCCESS == code) {
    bool bRewriteToColumn = false;
    code = translateScanPseudoColumnFunc(pCxt, ppNode, &bRewriteToColumn);
    if (bRewriteToColumn) {
      return code;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateIndefiniteRowsFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateForbidFillFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    bool bRewriteToColumn = false;
    code = translateWindowPseudoColumnFunc(pCxt, ppNode, &bRewriteToColumn);
    if (bRewriteToColumn) {
      return code;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateForbidSysTableFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateRepeatScanFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateMultiResFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateMultiRowsFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateInterpFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    bool bRewriteToColumn = false;
    code = translateInterpPseudoColumnFunc(pCxt, ppNode, &bRewriteToColumn);
    if (bRewriteToColumn) {
      return code;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateForecastFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    bool bRewriteToColumn = false;
    code = translateAnalysisPseudoColumnFunc(pCxt, ppNode, &bRewriteToColumn);
    if (bRewriteToColumn) {
      return code;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateTimelineFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateDateTimeFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateBlockDistFunc(pCxt, pFunc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = translateDBUsageFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    setFuncClassification(pCxt, pFunc);
  }
  return code;
}

static int32_t rewriteQueryTimeFunc(STranslateContext* pCxt, int64_t val, SNode** pNode) {
  if (INT64_MIN == val || INT64_MAX == val) {
    return rewriteFuncToValue(pCxt, NULL, pNode);
  }

  char* pStr = taosMemoryCalloc(1, 20);
  if (NULL == pStr) {
    return terrno;
  }
  snprintf(pStr, 20, "%" PRId64, val);
  int32_t code = rewriteFuncToValue(pCxt, &pStr, pNode);
  if (TSDB_CODE_SUCCESS != code) taosMemoryFree(pStr);
  return code;
}

static int32_t rewriteQstartFunc(STranslateContext* pCxt, SNode** pNode) {
  return rewriteQueryTimeFunc(pCxt, ((SSelectStmt*)pCxt->pCurrStmt)->timeRange.skey, pNode);
}

static int32_t rewriteQendFunc(STranslateContext* pCxt, SNode** pNode) {
  return rewriteQueryTimeFunc(pCxt, ((SSelectStmt*)pCxt->pCurrStmt)->timeRange.ekey, pNode);
}

static int32_t rewriteQdurationFunc(STranslateContext* pCxt, SNode** pNode) {
  STimeWindow range = ((SSelectStmt*)pCxt->pCurrStmt)->timeRange;
  if (INT64_MIN == range.skey || INT64_MAX == range.ekey) {
    return rewriteQueryTimeFunc(pCxt, INT64_MIN, pNode);
  }
  return rewriteQueryTimeFunc(pCxt, range.ekey - range.skey + 1, pNode);
}

static int32_t rewriteClientPseudoColumnFunc(STranslateContext* pCxt, SNode** pNode) {
  if (NULL == pCxt->pCurrStmt || QUERY_NODE_SELECT_STMT != nodeType(pCxt->pCurrStmt) ||
      pCxt->currClause <= SQL_CLAUSE_WHERE) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC, "Illegal pseudo column");
  }
  if (inStreamCalcClause(pCxt)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_STREAM_NOT_ALLOWED_FUNC,
                                   "%s is not support in stream calc query", ((SFunctionNode*)*pNode)->functionName);
  }
  switch (((SFunctionNode*)*pNode)->funcType) {
    case FUNCTION_TYPE_QSTART:
      return rewriteQstartFunc(pCxt, pNode);
    case FUNCTION_TYPE_QEND:
      return rewriteQendFunc(pCxt, pNode);
    case FUNCTION_TYPE_QDURATION:
      return rewriteQdurationFunc(pCxt, pNode);
    default:
      break;
  }
  return TSDB_CODE_PAR_INTERNAL_ERROR;
}

static int32_t translateFunctionImpl(STranslateContext* pCxt, SFunctionNode** pFunc) {
  if (fmIsSystemInfoFunc((*pFunc)->funcId)) {
    return rewriteSystemInfoFunc(pCxt, (SNode**)pFunc);
  }
  if (fmIsClientPseudoColumnFunc((*pFunc)->funcId)) {
    return rewriteClientPseudoColumnFunc(pCxt, (SNode**)pFunc);
  }
  if (fmIsPlaceHolderFunc((*pFunc)->funcId)) {
    return translatePlaceHolderFunc(pCxt, (SNode**)pFunc);
  }
  return translateNormalFunction(pCxt, (SNode**)pFunc);
}

static EDealRes translateFunction(STranslateContext* pCxt, SFunctionNode** pFunc) {
  SNode* pParam = NULL;
  if (strcmp((*pFunc)->functionName, "tbname") == 0 && (*pFunc)->pParameterList != NULL) {
    pParam = nodesListGetNode((*pFunc)->pParameterList, 0);
    if (pParam && nodeType(pParam) == QUERY_NODE_VALUE) {
      if (pCxt && pCxt->pCurrStmt && pCxt->pCurrStmt->type == QUERY_NODE_SELECT_STMT &&
          ((SSelectStmt*)pCxt->pCurrStmt)->pFromTable &&
          nodeType(((SSelectStmt*)pCxt->pCurrStmt)->pFromTable) == QUERY_NODE_REAL_TABLE) {
        SRealTableNode* pRealTable = (SRealTableNode*)((SSelectStmt*)pCxt->pCurrStmt)->pFromTable;
        if (strcmp(((SValueNode*)pParam)->literal, pRealTable->table.tableName) == 0) {
          NODES_DESTORY_LIST((*pFunc)->pParameterList);
          (*pFunc)->pParameterList = NULL;
        }
      }
    }
  }
  FOREACH(pParam, (*pFunc)->pParameterList) {
    if (isMultiResFunc(pParam)) {
      pCxt->errCode = TSDB_CODE_FUNC_FUNTION_PARA_NUM;
      return DEAL_RES_ERROR;
    }
  }

  pCxt->errCode = getFuncInfo(pCxt, *pFunc);
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    if ((SQL_CLAUSE_GROUP_BY == pCxt->currClause || SQL_CLAUSE_PARTITION_BY == pCxt->currClause) &&
        fmIsVectorFunc((*pFunc)->funcId)) {
      pCxt->errCode = TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION;
    }
  }

  if (isInvalidColsBindFunction(*pFunc)) {
    pCxt->errCode = TSDB_CODE_PAR_INVALID_COLS_SELECTFUNC;
    return DEAL_RES_ERROR;
  }

  if (fmIsPlaceHolderFunc((*pFunc)->funcId)) {
    return translatePlaceHolderFunc(pCxt, (SNode**)pFunc);
  }

  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pCxt->errCode = translateFunctionImpl(pCxt, pFunc);
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
}

static EDealRes translateExprSubquery(STranslateContext* pCxt, SNode* pNode) {
  return (TSDB_CODE_SUCCESS == translateSubquery(pCxt, pNode) ? DEAL_RES_CONTINUE : DEAL_RES_ERROR);
}

static EDealRes translateLogicCond(STranslateContext* pCxt, SLogicConditionNode* pCond) {
  pCond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pCond->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  return DEAL_RES_CONTINUE;
}

static int32_t createCastFunc(STranslateContext* pCxt, SNode* pExpr, SDataType dt, SNode** pCast) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  tstrncpy(pFunc->functionName, "cast", TSDB_FUNC_NAME_LEN);
  pFunc->node.resType = dt;
  if (TSDB_CODE_SUCCESS != nodesListMakeAppend(&pFunc->pParameterList, pExpr)) {
    nodesDestroyNode((SNode*)pFunc);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  code = getFuncInfo(pCxt, pFunc);
  pFunc->node.resType = dt;
  if (TSDB_CODE_SUCCESS != code) {
    nodesClearList(pFunc->pParameterList);
    pFunc->pParameterList = NULL;
    nodesDestroyNode((SNode*)pFunc);
    return code;
  }
  *pCast = (SNode*)pFunc;
  return TSDB_CODE_SUCCESS;
}

static EDealRes translateWhenThen(STranslateContext* pCxt, SWhenThenNode* pWhenThen) {
  pWhenThen->node.resType = ((SExprNode*)pWhenThen->pThen)->resType;
  return DEAL_RES_CONTINUE;
}

static bool isCondition(const SNode* pNode) {
  if (QUERY_NODE_OPERATOR == nodeType(pNode)) {
    return nodesIsComparisonOp((const SOperatorNode*)pNode);
  }
  return (QUERY_NODE_LOGIC_CONDITION == nodeType(pNode));
}

static int32_t rewriteIsTrue(SNode* pSrc, SNode** pIsTrue) {
  SOperatorNode* pOp = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOp);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  pOp->opType = OP_TYPE_IS_TRUE;
  pOp->pLeft = pSrc;
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  *pIsTrue = (SNode*)pOp;
  return TSDB_CODE_SUCCESS;
}

extern int8_t  gDisplyTypes[TSDB_DATA_TYPE_MAX][TSDB_DATA_TYPE_MAX];
static int32_t selectCommonType(SDataType* commonType, const SDataType* newType) {
  if (commonType->type < TSDB_DATA_TYPE_NULL || commonType->type >= TSDB_DATA_TYPE_MAX ||
      newType->type < TSDB_DATA_TYPE_NULL || newType->type >= TSDB_DATA_TYPE_MAX) {
    return TSDB_CODE_INVALID_PARA;
  }
  int8_t type1 = commonType->type;
  int8_t type2 = newType->type;
  int8_t resultType;
  if (type1 < type2) {
    resultType = gDisplyTypes[type1][type2];
  } else {
    resultType = gDisplyTypes[type2][type1];
  }

  if (resultType == -1) {
    return TSDB_CODE_SCALAR_CONVERT_ERROR;
  }

  if (commonType->type == newType->type) {
    if (IS_DECIMAL_TYPE(commonType->type)) {
      if ((commonType->precision - commonType->scale) < (newType->precision - newType->scale)) {
        commonType->precision = newType->precision;
        commonType->scale = newType->scale;
      }
    }
    commonType->bytes = TMAX(commonType->bytes, newType->bytes);
    return TSDB_CODE_SUCCESS;
  }

  if ((resultType == TSDB_DATA_TYPE_VARCHAR) &&
      (IS_MATHABLE_TYPE(commonType->type) || IS_MATHABLE_TYPE(newType->type))) {
    commonType->bytes = TMAX(TMAX(commonType->bytes, newType->bytes), QUERY_NUMBER_MAX_DISPLAY_LEN);
  } else if ((resultType == TSDB_DATA_TYPE_NCHAR) &&
             (IS_MATHABLE_TYPE(commonType->type) || IS_MATHABLE_TYPE(newType->type))) {
    commonType->bytes = TMAX(TMAX(commonType->bytes, newType->bytes), QUERY_NUMBER_MAX_DISPLAY_LEN * TSDB_NCHAR_SIZE);
  } else if (IS_DECIMAL_TYPE(resultType)) {
    if ((commonType->precision - commonType->scale) < (newType->precision - newType->scale)) {
      commonType->precision = newType->precision;
      commonType->scale = newType->scale;
    }
  } else if (!IS_NULL_TYPE(newType->type)) {
    commonType->bytes = TMAX(TMAX(commonType->bytes, newType->bytes), TYPE_BYTES[resultType]);
  }

  commonType->type = resultType;

  return TSDB_CODE_SUCCESS;
}

static EDealRes translateCaseWhen(STranslateContext* pCxt, SCaseWhenNode* pCaseWhen) {
  bool   allNullThen = true;
  SNode* pNode = NULL;
  FOREACH(pNode, pCaseWhen->pWhenThenList) {
    SWhenThenNode* pWhenThen = (SWhenThenNode*)pNode;
    if (NULL == pCaseWhen->pCase && !isCondition(pWhenThen->pWhen)) {
      SNode* pIsTrue = NULL;
      pCxt->errCode = rewriteIsTrue(pWhenThen->pWhen, &pIsTrue);
      if (TSDB_CODE_SUCCESS != pCxt->errCode) {
        return DEAL_RES_ERROR;
      }
      pWhenThen->pWhen = pIsTrue;
    }

    SExprNode* pThenExpr = (SExprNode*)pNode;
    if (TSDB_DATA_TYPE_NULL == pThenExpr->resType.type) {
      continue;
    }
    allNullThen = false;
    pCxt->errCode = selectCommonType(&pCaseWhen->node.resType, &pThenExpr->resType);
    if (TSDB_CODE_SUCCESS != pCxt->errCode) {
      return DEAL_RES_ERROR;
    }
  }

  SExprNode* pElseExpr = (SExprNode*)pCaseWhen->pElse;
  if (NULL != pElseExpr) {
    pCxt->errCode = selectCommonType(&pCaseWhen->node.resType, &pElseExpr->resType);
    if (TSDB_CODE_SUCCESS != pCxt->errCode) {
      return DEAL_RES_ERROR;
    }
  }

  if (allNullThen) {
    if (NULL != pCaseWhen->pElse) {
      pCaseWhen->node.resType = ((SExprNode*)pCaseWhen->pElse)->resType;
    } else {
      pCaseWhen->node.resType.type = TSDB_DATA_TYPE_NULL;
      pCaseWhen->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_NULL].bytes;
      return DEAL_RES_CONTINUE;
    }
  }

  FOREACH(pNode, pCaseWhen->pWhenThenList) {
    SWhenThenNode* pWhenThen = (SWhenThenNode*)pNode;
    if (!dataTypeEqual(&pCaseWhen->node.resType, &((SExprNode*)pNode)->resType)) {
      SNode* pCastFunc = NULL;
      pCxt->errCode = createCastFunc(pCxt, pWhenThen->pThen, pCaseWhen->node.resType, &pCastFunc);
      if (TSDB_CODE_SUCCESS != pCxt->errCode) {
        return DEAL_RES_ERROR;
      }
      pWhenThen->pThen = pCastFunc;
      pWhenThen->node.resType = pCaseWhen->node.resType;
    }
  }

  if (NULL != pCaseWhen->pElse && !dataTypeEqual(&pCaseWhen->node.resType, &((SExprNode*)pCaseWhen->pElse)->resType)) {
    SNode* pCastFunc = NULL;
    pCxt->errCode = createCastFunc(pCxt, pCaseWhen->pElse, pCaseWhen->node.resType, &pCastFunc);
    if (TSDB_CODE_SUCCESS != pCxt->errCode) {
      return DEAL_RES_ERROR;
    }
    pCaseWhen->pElse = pCastFunc;
    ((SExprNode*)pCaseWhen->pElse)->resType = pCaseWhen->node.resType;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes doTranslateExpr(SNode** pNode, void* pContext) {
  STranslateContext* pCxt = (STranslateContext*)pContext;
  switch (nodeType(*pNode)) {
    case QUERY_NODE_COLUMN:
      return translateColumn(pCxt, (SColumnNode**)pNode);
    case QUERY_NODE_VALUE:
      return translateValue(pCxt, (SValueNode*)*pNode);
    case QUERY_NODE_OPERATOR:
      return translateOperator(pCxt, (SOperatorNode*)*pNode);
    case QUERY_NODE_FUNCTION:
      return translateFunction(pCxt, (SFunctionNode**)pNode);
    case QUERY_NODE_LOGIC_CONDITION:
      return translateLogicCond(pCxt, (SLogicConditionNode*)*pNode);
    case QUERY_NODE_TEMP_TABLE:
      return translateExprSubquery(pCxt, ((STempTableNode*)*pNode)->pSubquery);
    case QUERY_NODE_WHEN_THEN:
      return translateWhenThen(pCxt, (SWhenThenNode*)*pNode);
    case QUERY_NODE_CASE_WHEN:
      return translateCaseWhen(pCxt, (SCaseWhenNode*)*pNode);
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t translateExpr(STranslateContext* pCxt, SNode** pNode) {
  nodesRewriteExprPostOrder(pNode, doTranslateExpr, pCxt);
  return pCxt->errCode;
}

static int32_t translateExprList(STranslateContext* pCxt, SNodeList* pList) {
  nodesRewriteExprsPostOrder(pList, doTranslateExpr, pCxt);
  return pCxt->errCode;
}

static SNodeList* getGroupByList(STranslateContext* pCxt) {
  if (isDistinctOrderBy(pCxt)) {
    return ((SSelectStmt*)pCxt->pCurrStmt)->pProjectionList;
  }
  return ((SSelectStmt*)pCxt->pCurrStmt)->pGroupByList;
}

static SNode* getGroupByNode(SNode* pNode) {
  if (QUERY_NODE_GROUPING_SET == nodeType(pNode)) {
    return nodesListGetNode(((SGroupingSetNode*)pNode)->pParameterList, 0);
  }
  return pNode;
}

static int32_t getGroupByErrorCode(STranslateContext* pCxt) {
  if (isDistinctOrderBy(pCxt)) {
    return TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION;
  }
  if (isSelectStmt(pCxt->pCurrStmt) && NULL != ((SSelectStmt*)pCxt->pCurrStmt)->pGroupByList) {
    return TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION;
  }
  return TSDB_CODE_PAR_INVALID_OPTR_USAGE;
}

static EDealRes rewriteColToSelectValFunc(STranslateContext* pCxt, SNode** pNode) {
  SFunctionNode* pFunc = NULL;
  SExprNode*     p = (SExprNode*)*pNode;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    pCxt->errCode = code;
    return DEAL_RES_ERROR;
  }

  tstrncpy(pFunc->functionName, "_select_value", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.aliasName, p->aliasName, TSDB_COL_NAME_LEN);
  tstrncpy(pFunc->node.userAlias, p->userAlias, TSDB_COL_NAME_LEN);

  // "_select_value" is an parameter of function, not in the projectionList, and user does not assign an alias name
  // let's rewrite the aliasName, to avoid the column name duplicate problem
  if ((strncmp(p->aliasName, p->userAlias, tListLen(p->aliasName)) == 0) && p->asParam && p->projIdx == 0) {
    uint64_t hashVal = MurmurHash3_64(p->aliasName, strlen(p->aliasName));
    snprintf(pFunc->node.aliasName, TSDB_COL_NAME_LEN, "%" PRIu64, hashVal);
  }
  
  pFunc->node.relatedTo = p->relatedTo;
  pFunc->node.bindExprID = p->bindExprID;
  pCxt->errCode = nodesListMakeAppend(&pFunc->pParameterList, *pNode);
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pCxt->errCode = getFuncInfo(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    *pNode = (SNode*)pFunc;
    ((SSelectStmt*)pCxt->pCurrStmt)->hasSelectValFunc = true;
  } else {
    nodesDestroyNode((SNode*)pFunc);
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR;
}

static EDealRes rewriteExprToGroupKeyFunc(STranslateContext* pCxt, SNode** pNode) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    pCxt->errCode = code;
    return DEAL_RES_ERROR;
  }

  tstrncpy(pFunc->functionName, "_group_key", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.aliasName, ((SExprNode*)*pNode)->aliasName, TSDB_COL_NAME_LEN);
  tstrncpy(pFunc->node.userAlias, ((SExprNode*)*pNode)->userAlias, TSDB_COL_NAME_LEN);
  pCxt->errCode = nodesListMakeAppend(&pFunc->pParameterList, *pNode);
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    *pNode = (SNode*)pFunc;
    pCxt->errCode = fmGetFuncInfo(pFunc, pCxt->msgBuf.buf, pCxt->msgBuf.len);
  }

  return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
}

static EDealRes rewriteExprToSelectTagFunc(STranslateContext* pCxt, SNode** pNode) {
  SFunctionNode* pFunc = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return DEAL_RES_ERROR;
  }

  tstrncpy(pFunc->functionName, "_group_const_value", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.aliasName, ((SExprNode*)*pNode)->aliasName, TSDB_COL_NAME_LEN);
  tstrncpy(pFunc->node.userAlias, ((SExprNode*)*pNode)->userAlias, TSDB_COL_NAME_LEN);
  pCxt->errCode = nodesListMakeAppend(&pFunc->pParameterList, *pNode);
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    *pNode = (SNode*)pFunc;
    pCxt->errCode = fmGetFuncInfo(pFunc, pCxt->msgBuf.buf, pCxt->msgBuf.len);
  }

  return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
}

static bool isWindowJoinProbeTablePrimCol(SSelectStmt* pSelect, SNode* pNode) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return false;
  }

  SColumnNode*    pCol = (SColumnNode*)pNode;
  SJoinTableNode* pJoinTable = (SJoinTableNode*)pSelect->pFromTable;
  SRealTableNode* pProbeTable = NULL;
  switch (pJoinTable->joinType) {
    case JOIN_TYPE_LEFT:
      pProbeTable = (SRealTableNode*)pJoinTable->pLeft;
      break;
    case JOIN_TYPE_RIGHT:
      pProbeTable = (SRealTableNode*)pJoinTable->pRight;
      break;
    default:
      return false;
  }

  if (pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID && 0 == strcmp(pCol->dbName, pProbeTable->table.dbName) &&
      0 == strcmp(pCol->tableAlias, pProbeTable->table.tableAlias)) {
    return true;
  }

  return false;
}

static bool isWindowJoinProbeTableCol(SSelectStmt* pSelect, SNode* pNode) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return false;
  }

  SColumnNode*    pCol = (SColumnNode*)pNode;
  SJoinTableNode* pJoinTable = (SJoinTableNode*)pSelect->pFromTable;
  SRealTableNode* pProbeTable = NULL;
  switch (pJoinTable->joinType) {
    case JOIN_TYPE_LEFT:
      pProbeTable = (SRealTableNode*)pJoinTable->pLeft;
      break;
    case JOIN_TYPE_RIGHT:
      pProbeTable = (SRealTableNode*)pJoinTable->pRight;
      break;
    default:
      return false;
  }

  if (0 == strcmp(pCol->dbName, pProbeTable->table.dbName) &&
      0 == strcmp(pCol->tableAlias, pProbeTable->table.tableAlias)) {
    return true;
  }

  return false;
}

typedef struct SCheckColContaisCtx {
  SNode* pTarget;
  bool   contains;
} SCheckColContaisCtx;

static EDealRes checkColContains(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SCheckColContaisCtx* pCtx = (SCheckColContaisCtx*)pContext;
  if (nodesEqualNode(pCtx->pTarget, pNode)) {
    pCtx->contains = true;
    return DEAL_RES_END;
  }

  return DEAL_RES_CONTINUE;
}

static bool isWindowJoinGroupCol(SSelectStmt* pSelect, SNode* pNode) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return false;
  }

  SCheckColContaisCtx ctx = {.pTarget = pNode, .contains = false};
  SJoinTableNode*     pJoinTable = (SJoinTableNode*)pSelect->pFromTable;

  nodesWalkExpr(pJoinTable->pOnCond, checkColContains, &ctx);

  return ctx.contains;
}

static bool isWindowJoinSubTbTag(SSelectStmt* pSelect, SNode* pNode) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return false;
  }
  SColumnNode* pCol = (SColumnNode*)pNode;
  if (COLUMN_TYPE_TAG != pCol->colType) {
    return false;
  }

  SJoinTableNode* pJoinTable = (SJoinTableNode*)pSelect->pFromTable;
  SRealTableNode* pProbeTable = NULL;
  SRealTableNode* pBuildTable = NULL;
  switch (pJoinTable->joinType) {
    case JOIN_TYPE_LEFT:
      pProbeTable = (SRealTableNode*)pJoinTable->pLeft;
      pBuildTable = (SRealTableNode*)pJoinTable->pRight;
      break;
    case JOIN_TYPE_RIGHT:
      pProbeTable = (SRealTableNode*)pJoinTable->pRight;
      pBuildTable = (SRealTableNode*)pJoinTable->pLeft;
      break;
    default:
      return false;
  }

  SRealTableNode* pTargetTable = pProbeTable;
  if (0 != taosStrcasecmp(pCol->tableAlias, pProbeTable->table.tableAlias)) {
    pTargetTable = pBuildTable;
  }

  if (TSDB_CHILD_TABLE != pTargetTable->pMeta->tableType && TSDB_NORMAL_TABLE != pTargetTable->pMeta->tableType) {
    return false;
  }

  return true;
}

static bool isWindowJoinSubTbname(SSelectStmt* pSelect, SNode* pNode) {
  if (QUERY_NODE_FUNCTION != nodeType(pNode)) {
    return false;
  }

  SFunctionNode* pFuncNode = (SFunctionNode*)pNode;
  if (FUNCTION_TYPE_TBNAME != pFuncNode->funcType) {
    return false;
  }

  SJoinTableNode* pJoinTable = (SJoinTableNode*)pSelect->pFromTable;
  SRealTableNode* pProbeTable = NULL;
  SRealTableNode* pBuildTable = NULL;
  switch (pJoinTable->joinType) {
    case JOIN_TYPE_LEFT:
      pProbeTable = (SRealTableNode*)pJoinTable->pLeft;
      pBuildTable = (SRealTableNode*)pJoinTable->pRight;
      break;
    case JOIN_TYPE_RIGHT:
      pProbeTable = (SRealTableNode*)pJoinTable->pRight;
      pBuildTable = (SRealTableNode*)pJoinTable->pLeft;
      break;
    default:
      return false;
  }

  SRealTableNode* pTargetTable = pProbeTable;
  bool            isProbeTable = true;
  SValueNode*     pVal = (SValueNode*)nodesListGetNode(pFuncNode->pParameterList, 0);
  if (NULL != pVal && 0 != taosStrcasecmp(pVal->literal, pProbeTable->table.tableAlias)) {
    pTargetTable = pBuildTable;
    isProbeTable = false;
  }

  if (!isProbeTable && TSDB_CHILD_TABLE != pTargetTable->pMeta->tableType &&
      TSDB_NORMAL_TABLE != pTargetTable->pMeta->tableType) {
    return false;
  }

  return true;
}

static bool isTbnameFuction(SNode* pNode) {
  return QUERY_NODE_FUNCTION == nodeType(pNode) && FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pNode)->funcType;
}

static bool hasTbnameFunction(SNodeList* pPartitionByList) {
  SNode* pPartKey = NULL;
  FOREACH(pPartKey, pPartitionByList) {
    if (isTbnameFuction(pPartKey)) {
      return true;
    }
  }
  return false;
}

static bool IsEqualTbNameFuncNode(SSelectStmt* pSelect, SNode* pFunc1, SNode* pFunc2) {
  if (isTbnameFuction(pFunc1) && isTbnameFuction(pFunc2)) {
    SValueNode* pVal1 = (SValueNode*)nodesListGetNode(((SFunctionNode*)pFunc1)->pParameterList, 0);
    SValueNode* pVal2 = (SValueNode*)nodesListGetNode(((SFunctionNode*)pFunc1)->pParameterList, 0);
    if (!pVal1 && !pVal2) {
      return true;
    } else if (pVal1 && pVal2) {
      return strcmp(pVal1->literal, pVal2->literal) == 0;
    }

    if (pSelect->pFromTable &&
        (pSelect->pFromTable->type == QUERY_NODE_REAL_TABLE || pSelect->pFromTable->type == QUERY_NODE_TEMP_TABLE)) {
      STableNode* pTable = (STableNode*)pSelect->pFromTable;
      return true;
    } else {
      return false;
    }
  }
  return false;
}

static EDealRes doCheckExprForGroupBy(SNode** pNode, void* pContext) {
  STranslateContext* pCxt = (STranslateContext*)pContext;
  SSelectStmt*       pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  if (!nodesIsExprNode(*pNode) || isAliasColumn(*pNode)) {
    return DEAL_RES_CONTINUE;
  }
  if (isVectorFunc(*pNode) && !isDistinctOrderBy(pCxt)) {
    return DEAL_RES_IGNORE_CHILD;
  }
  bool   isSingleTable = fromSingleTable(((SSelectStmt*)pCxt->pCurrStmt)->pFromTable);
  SNode* pGroupNode = NULL;
  FOREACH(pGroupNode, getGroupByList(pCxt)) {
    SNode* pActualNode = getGroupByNode(pGroupNode);
    if (nodesEqualNode(pActualNode, *pNode)) {
      return DEAL_RES_IGNORE_CHILD;
    }
    if (IsEqualTbNameFuncNode(pSelect, pActualNode, *pNode)) {
      return rewriteExprToGroupKeyFunc(pCxt, pNode);
    }
    if ((isTbnameFuction(pActualNode) || isSingleTable) && QUERY_NODE_COLUMN == nodeType(*pNode) &&
        ((SColumnNode*)*pNode)->colType == COLUMN_TYPE_TAG) {
      return rewriteExprToSelectTagFunc(pCxt, pNode);
    }
    if (isSingleTable && isTbnameFuction(*pNode)) {
      return rewriteExprToSelectTagFunc(pCxt, pNode);
    }
  }
  SNode* pPartKey = NULL;
  bool   partionByTbname = hasTbnameFunction(pSelect->pPartitionByList);
  FOREACH(pPartKey, pSelect->pPartitionByList) {
    if (nodesEqualNode(pPartKey, *pNode)) {
      return (pSelect->hasAggFuncs || pSelect->pWindow) ? rewriteExprToGroupKeyFunc(pCxt, pNode)
                                                        : DEAL_RES_IGNORE_CHILD;
    }
    if ((partionByTbname) && QUERY_NODE_COLUMN == nodeType(*pNode) &&
        ((SColumnNode*)*pNode)->colType == COLUMN_TYPE_TAG) {
      return rewriteExprToGroupKeyFunc(pCxt, pNode);
    }
    if (IsEqualTbNameFuncNode(pSelect, pPartKey, *pNode)) {
      return rewriteExprToGroupKeyFunc(pCxt, pNode);
    }
  }
  if (NULL != pSelect->pWindow && QUERY_NODE_STATE_WINDOW == nodeType(pSelect->pWindow)) {
    if (nodesEqualNode(((SStateWindowNode*)pSelect->pWindow)->pExpr, *pNode)) {
      pSelect->hasStateKey = true;
      return rewriteExprToGroupKeyFunc(pCxt, pNode);
    }
  }

  if (isScanPseudoColumnFunc(*pNode)) {
    if (((pSelect->selectFuncNum > 1 && pCxt->stableQuery) ||
         (isDistinctOrderBy(pCxt) && pCxt->currClause == SQL_CLAUSE_ORDER_BY)) &&
        !isRelatedToOtherExpr((SExprNode*)*pNode)) {
      return generateDealNodeErrMsg(pCxt, getGroupByErrorCode(pCxt), ((SExprNode*)(*pNode))->userAlias);
    }
  }

  if (QUERY_NODE_COLUMN == nodeType(*pNode)) {
    if (((pSelect->selectFuncNum > 1) || (isDistinctOrderBy(pCxt) && pCxt->currClause == SQL_CLAUSE_ORDER_BY)) &&
        !isRelatedToOtherExpr((SExprNode*)*pNode)) {
      return generateDealNodeErrMsg(pCxt, getGroupByErrorCode(pCxt), ((SExprNode*)(*pNode))->userAlias);
    }
  }

  if (isScanPseudoColumnFunc(*pNode) || QUERY_NODE_COLUMN == nodeType(*pNode)) {
    if (isWindowJoinStmt(pSelect) &&
        (isWindowJoinProbeTableCol(pSelect, *pNode) || isWindowJoinGroupCol(pSelect, *pNode) ||
         (isWindowJoinSubTbname(pSelect, *pNode)) || isWindowJoinSubTbTag(pSelect, *pNode))) {
      return rewriteExprToGroupKeyFunc(pCxt, pNode);
    }

    if ((pSelect->hasOtherVectorFunc || !pSelect->hasSelectFunc) && !isRelatedToOtherExpr((SExprNode*)*pNode)) {
      return generateDealNodeErrMsg(pCxt, getGroupByErrorCode(pCxt), ((SExprNode*)(*pNode))->userAlias);
    }

    return rewriteColToSelectValFunc(pCxt, pNode);
  }
  if (isVectorFunc(*pNode) && isDistinctOrderBy(pCxt)) {
    return generateDealNodeErrMsg(pCxt, getGroupByErrorCode(pCxt), ((SExprNode*)(*pNode))->userAlias);
  }
  return DEAL_RES_CONTINUE;
}

static int32_t checkExprForGroupBy(STranslateContext* pCxt, SNode** pNode) {
  nodesRewriteExpr(pNode, doCheckExprForGroupBy, pCxt);
  return pCxt->errCode;
}

static int32_t checkExprListForGroupBy(STranslateContext* pCxt, SSelectStmt* pSelect, SNodeList* pList) {
  if (NULL == getGroupByList(pCxt) && NULL == pSelect->pWindow &&
      (!isWindowJoinStmt(pSelect) || (!pSelect->hasAggFuncs && !pSelect->hasIndefiniteRowsFunc))) {
    return TSDB_CODE_SUCCESS;
  }
  nodesRewriteExprs(pList, doCheckExprForGroupBy, pCxt);
  return pCxt->errCode;
}

static EDealRes rewriteColsToSelectValFuncImpl(SNode** pNode, void* pContext) {
  if (isAggFunc(*pNode) || isIndefiniteRowsFunc(*pNode)) {
    return DEAL_RES_IGNORE_CHILD;
  }
  if (isScanPseudoColumnFunc(*pNode) || QUERY_NODE_COLUMN == nodeType(*pNode)) {
    return rewriteColToSelectValFunc((STranslateContext*)pContext, pNode);
  }
  return DEAL_RES_CONTINUE;
}

static int32_t rewriteColsToSelectValFunc(STranslateContext* pCxt, SSelectStmt* pSelect) {
  nodesRewriteExprs(pSelect->pProjectionList, rewriteColsToSelectValFuncImpl, pCxt);
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    nodesRewriteExprs(pSelect->pOrderByList, rewriteColsToSelectValFuncImpl, pCxt);
  }
  return pCxt->errCode;
}

typedef struct CheckAggColCoexistCxt {
  STranslateContext* pTranslateCxt;
  bool               existCol;
  bool               hasColFunc;
  SNodeList*         pColList;
} CheckAggColCoexistCxt;

static EDealRes doCheckAggColCoexist(SNode** pNode, void* pContext) {
  CheckAggColCoexistCxt* pCxt = (CheckAggColCoexistCxt*)pContext;
  if (isVectorFunc(*pNode)) {
    return DEAL_RES_IGNORE_CHILD;
  }
  if (isColsFunctionResult(*pNode)) {
    pCxt->hasColFunc = true;
  }

  SNode* pPartKey = NULL;
  bool   partionByTbname = false;
  if (fromSingleTable(((SSelectStmt*)pCxt->pTranslateCxt->pCurrStmt)->pFromTable) ||
      hasTbnameFunction(((SSelectStmt*)pCxt->pTranslateCxt->pCurrStmt)->pPartitionByList)) {
    partionByTbname = true;
  }
  FOREACH(pPartKey, ((SSelectStmt*)pCxt->pTranslateCxt->pCurrStmt)->pPartitionByList) {
    if (nodesEqualNode(pPartKey, *pNode)) {
      return rewriteExprToGroupKeyFunc(pCxt->pTranslateCxt, pNode);
    }
  }
  if (partionByTbname &&
      (QUERY_NODE_FUNCTION == nodeType(*pNode) && FUNCTION_TYPE_TBNAME == ((SFunctionNode*)*pNode)->funcType)) {
    return rewriteExprToGroupKeyFunc(pCxt->pTranslateCxt, pNode);
  }
  if (partionByTbname &&
      ((QUERY_NODE_COLUMN == nodeType(*pNode) && ((SColumnNode*)*pNode)->colType == COLUMN_TYPE_TAG))) {
    return rewriteExprToSelectTagFunc(pCxt->pTranslateCxt, pNode);
  }
  if ((isScanPseudoColumnFunc(*pNode) || QUERY_NODE_COLUMN == nodeType(*pNode)) &&
      ((!nodesIsExprNode(*pNode) || !isRelatedToOtherExpr((SExprNode*)*pNode)))) {
    pCxt->existCol = true;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes doCheckGetAggColCoexist(SNode** pNode, void* pContext) {
  CheckAggColCoexistCxt* pCxt = (CheckAggColCoexistCxt*)pContext;
  int32_t                code = 0;
  if (isVectorFunc(*pNode)) {
    return DEAL_RES_IGNORE_CHILD;
  }
  if (isScanPseudoColumnFunc(*pNode) || QUERY_NODE_COLUMN == nodeType(*pNode)) {
    pCxt->existCol = true;
    code = nodesListMakeStrictAppend(&pCxt->pColList, *pNode);
    if (TSDB_CODE_SUCCESS != code) {
      pCxt->pTranslateCxt->errCode = code;
      return DEAL_RES_ERROR;
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t checkIsEmptyResult(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (pSelect->timeRange.skey > pSelect->timeRange.ekey && !pSelect->hasCountFunc) {
    pSelect->isEmptyResult = true;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t resetSelectFuncNumWithoutDup(SSelectStmt* pSelect) {
  if (pSelect->selectFuncNum <= 1) return TSDB_CODE_SUCCESS;
  pSelect->selectFuncNum = 0;
  pSelect->lastProcessByRowFuncId = -1;
  SNodeList* pNodeList = NULL;
  int32_t    code = nodesMakeList(&pNodeList);
  if (TSDB_CODE_SUCCESS != code) return code;
  code = nodesCollectSelectFuncs(pSelect, SQL_CLAUSE_FROM, NULL, fmIsSelectFunc, pNodeList);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pNodeList);
    return code;
  }
  SNode* pNode = NULL;
  FOREACH(pNode, pNodeList) {
    pSelect->selectFuncNum = calcSelectFuncNum((SFunctionNode*)pNode, pSelect->selectFuncNum);
  }
  nodesDestroyList(pNodeList);
  return TSDB_CODE_SUCCESS;
}

static int32_t checkAggColCoexist(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL != pSelect->pGroupByList || NULL != pSelect->pWindow || isWindowJoinStmt(pSelect) ||
      (!pSelect->hasAggFuncs && !pSelect->hasIndefiniteRowsFunc && !pSelect->hasInterpFunc &&
       !pSelect->hasForecastFunc)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!pSelect->onlyHasKeepOrderFunc) {
    pSelect->timeLineResMode = TIME_LINE_NONE;
  }
  CheckAggColCoexistCxt cxt = {.pTranslateCxt = pCxt, .existCol = false, .hasColFunc = false};
  nodesRewriteExprs(pSelect->pProjectionList, doCheckAggColCoexist, &cxt);
  if (!pSelect->isDistinct) {
    nodesRewriteExprs(pSelect->pOrderByList, doCheckAggColCoexist, &cxt);
  }
  if (((!cxt.existCol && 0 < pSelect->selectFuncNum) || (cxt.existCol && 1 == pSelect->selectFuncNum)) &&
      !pSelect->hasOtherVectorFunc) {
    return rewriteColsToSelectValFunc(pCxt, pSelect);
  }
  if (cxt.existCol) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_SINGLE_GROUP);
  }
  if (cxt.hasColFunc) {
    return rewriteColsToSelectValFunc(pCxt, pSelect);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkWinJoinAggColCoexist(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (!isWindowJoinStmt(pSelect) || (!pSelect->hasAggFuncs && !pSelect->hasIndefiniteRowsFunc &&
                                     !pSelect->hasInterpFunc && !pSelect->hasForecastFunc)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!pSelect->onlyHasKeepOrderFunc) {
    pSelect->timeLineResMode = TIME_LINE_NONE;
  }
  CheckAggColCoexistCxt cxt = {.pTranslateCxt = pCxt, .existCol = false, .pColList = NULL};
  nodesRewriteExprs(pSelect->pProjectionList, doCheckGetAggColCoexist, &cxt);
  if (!pSelect->isDistinct) {
    nodesRewriteExprs(pSelect->pOrderByList, doCheckGetAggColCoexist, &cxt);
  }
  if (((!cxt.existCol && 0 < pSelect->selectFuncNum) || (cxt.existCol && 1 == pSelect->selectFuncNum)) &&
      !pSelect->hasOtherVectorFunc) {
    return rewriteColsToSelectValFunc(pCxt, pSelect);
  }

  if (cxt.existCol) {
    bool   allProbeTableCols = true;
    SNode* pNode = NULL;
    FOREACH(pNode, cxt.pColList) {
      if (isWindowJoinProbeTableCol(pSelect, pNode) || isWindowJoinGroupCol(pSelect, pNode) ||
          (isWindowJoinSubTbname(pSelect, pNode)) || isWindowJoinSubTbTag(pSelect, pNode)) {
        continue;
      }

      allProbeTableCols = false;
      break;
    }

    if (!allProbeTableCols) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_WIN_FUNC);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkHavingGroupBy(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == getGroupByList(pCxt) && NULL == pSelect->pPartitionByList && NULL == pSelect->pWindow &&
      !isWindowJoinStmt(pSelect)) {
    return code;
  }
  if (NULL != pSelect->pHaving) {
    pCxt->currClause = SQL_CLAUSE_HAVING;
    code = checkExprForGroupBy(pCxt, &pSelect->pHaving);
  }
  /*
    if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pProjectionList) {
      code = checkExprListForGroupBy(pCxt, pSelect, pSelect->pProjectionList);
    }
    if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pOrderByList) {
      code = checkExprListForGroupBy(pCxt, pSelect, pSelect->pOrderByList);
    }
  */
  return code;
}

static EDealRes searchAggFuncNode(SNode* pNode, void* pContext) {
  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (fmIsAggFunc(pFunc->funcId)) {
      *(bool*)pContext = true;
      return DEAL_RES_END;
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t checkWindowGrpFuncCoexist(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL != pSelect->pWindow && !pSelect->hasAggFuncs && !pSelect->hasStateKey) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NO_VALID_FUNC_IN_WIN);
  }
  if (isWindowJoinStmt(pSelect)) {
    if (!pSelect->hasAggFuncs && NULL != pSelect->pHaving) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_WJOIN_HAVING_EXPR);
    }
    /*
        if (NULL != pSelect->pHaving) {
          bool hasFunc = false;
          nodesWalkExpr(pSelect->pHaving, searchAggFuncNode, &hasFunc);
          if (!hasFunc) {
            return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_WJOIN_HAVING_EXPR);
          }
        }
    */
    if (pSelect->hasAggFuncs) {
      return checkExprListForGroupBy(pCxt, pSelect, pSelect->pProjectionList);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t toVgroupsInfo(SArray* pVgs, SVgroupsInfo** pVgsInfo) {
  size_t vgroupNum = taosArrayGetSize(pVgs);
  *pVgsInfo = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo) * vgroupNum);
  if (NULL == *pVgsInfo) {
    return terrno;
  }
  (*pVgsInfo)->numOfVgroups = vgroupNum;
  for (int32_t i = 0; i < vgroupNum; ++i) {
    SVgroupInfo* vg = taosArrayGet(pVgs, i);
    (*pVgsInfo)->vgroups[i] = *vg;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t addMnodeToVgroupList(const SEpSet* pEpSet, SArray** pVgroupList) {
  if (NULL == *pVgroupList) {
    *pVgroupList = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SVgroupInfo));
    if (NULL == *pVgroupList) {
      return terrno;
    }
  }
  SVgroupInfo vg = {.vgId = MNODE_HANDLE};
  memcpy(&vg.epSet, pEpSet, sizeof(SEpSet));
  if (NULL == taosArrayPush(*pVgroupList, &vg)) {
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t dnodeToVgroupsInfo(SArray* pDnodes, SVgroupsInfo** pVgsInfo) {
  size_t ndnode = taosArrayGetSize(pDnodes);
  *pVgsInfo = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo) * ndnode);
  if (NULL == *pVgsInfo) {
    return terrno;
  }
  (*pVgsInfo)->numOfVgroups = ndnode;
  for (int32_t i = 0; i < ndnode; ++i) {
    SDNodeAddr* pDnodeAddr = taosArrayGet(pDnodes, i);
    (*pVgsInfo)->vgroups[i].vgId = pDnodeAddr->nodeId;
    memcpy(&((*pVgsInfo)->vgroups[i].epSet), &pDnodeAddr->epSet, sizeof(SEpSet));
  }
  return TSDB_CODE_SUCCESS;
}

static bool sysTableFromVnode(const char* pTable) {
  return ((0 == strcmp(pTable, TSDB_INS_TABLE_TABLES)) || (0 == strcmp(pTable, TSDB_INS_TABLE_TAGS)) ||
          (0 == strcmp(pTable, TSDB_INS_TABLE_COLS)) || 0 == strcmp(pTable, TSDB_INS_TABLE_VC_COLS) ||
          0 == strcmp(pTable, TSDB_INS_DISK_USAGE) || (0 == strcmp(pTable, TSDB_INS_TABLE_FILESETS)));
}

static bool sysTableFromDnode(const char* pTable) { return 0 == strcmp(pTable, TSDB_INS_TABLE_DNODE_VARIABLES); }

static int32_t getVnodeSysTableVgroupListImpl(STranslateContext* pCxt, SName* pTargetName, SName* pName,
                                              SArray** pVgroupList) {
  if (0 == pTargetName->type) {
    return getDBVgInfoImpl(pCxt, pName, pVgroupList);
  }

  if (0 == strcmp(pTargetName->dbname, TSDB_INFORMATION_SCHEMA_DB) ||
      0 == strcmp(pTargetName->dbname, TSDB_PERFORMANCE_SCHEMA_DB)) {
    pTargetName->type = 0;
    return TSDB_CODE_SUCCESS;
  }

  if (TSDB_DB_NAME_T == pTargetName->type) {
    int32_t code = getDBVgInfoImpl(pCxt, pTargetName, pVgroupList);
    if (!pCxt->showRewrite && (TSDB_CODE_MND_DB_NOT_EXIST == code || TSDB_CODE_MND_DB_IN_CREATING == code ||
                               TSDB_CODE_MND_DB_IN_DROPPING == code)) {
      // system table query should not report errors
      code = TSDB_CODE_SUCCESS;
    }
    return code;
  }

  SVgroupInfo vgInfo = {0};
  int32_t     code = getTableHashVgroupImpl(pCxt, pTargetName, &vgInfo);
  if (TSDB_CODE_SUCCESS == code) {
    *pVgroupList = taosArrayInit(1, sizeof(SVgroupInfo));
    if (NULL == *pVgroupList) {
      code = terrno;
    } else {
      if (NULL == taosArrayPush(*pVgroupList, &vgInfo)) {
        code = terrno;
      }
    }
  } else if (TSDB_CODE_MND_DB_NOT_EXIST == code || TSDB_CODE_MND_DB_IN_CREATING == code ||
             TSDB_CODE_MND_DB_IN_DROPPING == code) {
    // system table query should not report errors
    code = TSDB_CODE_SUCCESS;
  }
  return code;
}

static int32_t getVnodeSysTableVgroupList(STranslateContext* pCxt, SName* pName, SArray** pVgs, bool* pHasUserDbCond) {
  if (!isSelectStmt(pCxt->pCurrStmt)) {
    return TSDB_CODE_SUCCESS;
  }
  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  SName        targetName = {0};
  int32_t      code = getVnodeSysTableTargetName(pCxt->pParseCxt->acctId, pSelect->pWhere, &targetName);
  if (TSDB_CODE_SUCCESS == code) {
    code = getVnodeSysTableVgroupListImpl(pCxt, &targetName, pName, pVgs);
  }
  *pHasUserDbCond = (0 != targetName.type && taosArrayGetSize(*pVgs) > 0);
  return code;
}

static int32_t setVnodeSysTableVgroupList(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  bool    hasUserDbCond = false;
  SArray* pVgs = NULL;
  int32_t code = getVnodeSysTableVgroupList(pCxt, pName, &pVgs, &hasUserDbCond);

  if (TSDB_CODE_SUCCESS == code && 0 == strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_TAGS) &&
      isSelectStmt(pCxt->pCurrStmt) && 0 == taosArrayGetSize(pVgs)) {
    ((SSelectStmt*)pCxt->pCurrStmt)->isEmptyResult = true;
  }

  if (TSDB_CODE_SUCCESS == code &&
      ((0 == strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_TABLES) && !hasUserDbCond) ||
       0 == strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_COLS) ||
       (0 == strcmp(pRealTable->table.tableName, TSDB_INS_DISK_USAGE) && !hasUserDbCond) ||
       0 == strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_FILESETS))) {
    code = addMnodeToVgroupList(&pCxt->pParseCxt->mgmtEpSet, &pVgs);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = toVgroupsInfo(pVgs, &pRealTable->pVgroupList);
  }
  taosArrayDestroy(pVgs);

  return code;
}

static int32_t setDnodeSysTableVgroupList(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  SArray* pDnodes = NULL;
  int32_t code = getDnodeList(pCxt, &pDnodes);
  if (TSDB_CODE_SUCCESS == code) {
    code = dnodeToVgroupsInfo(pDnodes, &pRealTable->pVgroupList);
  }
  taosArrayDestroy(pDnodes);
  return code;
}

static int32_t setSysTableVgroupList(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  if (sysTableFromVnode(pRealTable->table.tableName)) {
    return setVnodeSysTableVgroupList(pCxt, pName, pRealTable);
  } else if (sysTableFromDnode(pRealTable->table.tableName)) {
    return setDnodeSysTableVgroupList(pCxt, pName, pRealTable);
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t setSuperTableVgroupList(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  SArray* vgroupList = NULL;
  int32_t code = getDBVgInfoImpl(pCxt, pName, &vgroupList);
  if (TSDB_CODE_SUCCESS == code) {
    code = toVgroupsInfo(vgroupList, &pRealTable->pVgroupList);
  }
  taosArrayDestroy(vgroupList);
  return code;
}

static int32_t setTrowsTableVgroupList(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  SArray* vgroupList = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  PAR_ERR_JRET(getDBVgInfoImpl(pCxt, pName, &vgroupList));
  pRealTable->pVgroupList = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo));
  if (NULL == pRealTable->pVgroupList) {
    PAR_ERR_JRET(terrno);
  }
  pRealTable->pVgroupList->numOfVgroups = 1;
  SVgroupInfo* vg = taosArrayGet(vgroupList, 0);
  if (vg == NULL) {
    PAR_ERR_JRET(terrno);
  }
  memcpy(pRealTable->pVgroupList->vgroups, vg, sizeof(SVgroupInfo));

_return:
  taosArrayDestroy(vgroupList);
  return code;
}

static int32_t setVSuperTableVgroupList(STranslateContext* pCxt, SName* pName, SVirtualTableNode* pVirtualTable) {
  SArray* vgroupList = NULL;
  int32_t code = getDBVgInfoImpl(pCxt, pName, &vgroupList);
  if (TSDB_CODE_SUCCESS == code) {
    taosMemoryFreeClear(pVirtualTable->pVgroupList);
    code = toVgroupsInfo(vgroupList, &pVirtualTable->pVgroupList);
  }
  taosArrayDestroy(vgroupList);
  return code;
}

static int32_t setVSuperTableRefScanVgroupList(STranslateContext* pCxt, SName* pName, SRealTableNode* pRefScanTable) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SArray*    vgroupList = NULL;
  SSHashObj* dbNameHash = NULL;
  SArray*    tmpVgroupList = NULL;
  bool       tmpasyn = pCxt->pParseCxt->async;
  SName      dbName;

  vgroupList = taosArrayInit(1, sizeof(SVgroupInfo));
  QUERY_CHECK_NULL(vgroupList, code, lino, _return, terrno);


  SArray* pVStbRefs = NULL;
  PAR_ERR_JRET(getVStbRefDbsFromCache(pCxt->pMetaCache, pName, &pVStbRefs));
  dbNameHash = tSimpleHashInit(taosArrayGetSize(pVStbRefs), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  QUERY_CHECK_NULL(dbNameHash, code, lino, _return, terrno);

  // set async to false since other db's info won't be collect during collectMetaKey
  pCxt->pParseCxt->async = false;
  // travel all vnodes' response
  for (int32_t i = 0; i < taosArrayGetSize(pVStbRefs); i++) {
    SVStbRefDbsRsp *pRsp = taosArrayGet(pVStbRefs, i);
    QUERY_CHECK_NULL(pRsp, code, lino, _return, terrno);

    // travel all ref dbs in one vnode's response
    for (int32_t j = 0; j < taosArrayGetSize(pRsp->pDbs); j++) {
      char* pDb = taosArrayGetP(pRsp->pDbs, j);
      QUERY_CHECK_NULL(pDb, code, lino, _return, terrno);

      // do not put duplicate ref db's vgroup into vgroupList
      if (!tSimpleHashGet(dbNameHash, pDb, strlen(pDb))) {
        PAR_ERR_JRET(tSimpleHashPut(dbNameHash, pDb, strlen(pDb), NULL, 0));
        PAR_ERR_JRET(tNameSetDbName(&dbName, pName->acctId, pDb, strlen(pDb)));
        PAR_ERR_JRET(getDBVgInfoImpl(pCxt, &dbName, &tmpVgroupList));
        QUERY_CHECK_NULL(taosArrayAddAll(vgroupList, tmpVgroupList), code, lino, _return, terrno);
        taosArrayDestroy(tmpVgroupList);
        tmpVgroupList = NULL;
      }
    }
  }

  pCxt->pParseCxt->async = tmpasyn;
  taosMemoryFreeClear(pRefScanTable->pVgroupList);
  PAR_ERR_JRET(toVgroupsInfo(vgroupList, &pRefScanTable->pVgroupList));

_return:
  if (code) {
    qError("%s failed, code:%d", __func__, code);
  }
  taosArrayDestroy(tmpVgroupList);
  taosArrayDestroy(vgroupList);
  tSimpleHashCleanup(dbNameHash);
  return code;
}

static int32_t setVirtualTableVgroupList(STranslateContext* pCxt, SName* pName, SVirtualTableNode* pVirtualTable) {
  pVirtualTable->pVgroupList = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo));
  if (NULL == pVirtualTable->pVgroupList) {
    return terrno;
  }
  pVirtualTable->pVgroupList->numOfVgroups = 1;
  return getTableHashVgroupImpl(pCxt, pName, pVirtualTable->pVgroupList->vgroups);
}

static int32_t setNormalTableVgroupList(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  pRealTable->pVgroupList = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo));
  if (NULL == pRealTable->pVgroupList) {
    return terrno;
  }
  pRealTable->pVgroupList->numOfVgroups = 1;
  return getTableHashVgroupImpl(pCxt, pName, pRealTable->pVgroupList->vgroups);
}

static int32_t setTableVgroupList(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  if (pCxt->pParseCxt->topicQuery) {
    return TSDB_CODE_SUCCESS;
  }

  if (TSDB_SUPER_TABLE == pRealTable->pMeta->tableType) {
    if (pRealTable->placeholderType == SP_PARTITION_ROWS) {
      return setTrowsTableVgroupList(pCxt, pName, pRealTable);
    } else {
      return setSuperTableVgroupList(pCxt, pName, pRealTable);
    }
  }

  if (TSDB_SYSTEM_TABLE == pRealTable->pMeta->tableType) {
    return setSysTableVgroupList(pCxt, pName, pRealTable);
  }

  return setNormalTableVgroupList(pCxt, pName, pRealTable);
}

static uint8_t getStmtPrecision(SNode* pStmt) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    return ((SSelectStmt*)pStmt)->precision;
  } else if (QUERY_NODE_SET_OPERATOR == nodeType(pStmt)) {
    return ((SSetOperator*)pStmt)->precision;
  }
  return 0;
}

static bool stmtIsSingleTable(SNode* pStmt) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    SSelectStmt* pSelect = (SSelectStmt*)pStmt;
    return NULL == pSelect->pFromTable || ((STableNode*)pSelect->pFromTable)->singleTable;
  }
  return false;
}

static uint8_t calcPrecision(uint8_t lp, uint8_t rp) { return (lp > rp ? rp : lp); }

static uint8_t calcJoinTablePrecision(SJoinTableNode* pJoinTable) {
  return calcPrecision(((STableNode*)pJoinTable->pLeft)->precision, ((STableNode*)pJoinTable->pRight)->precision);
}

static bool joinTableIsSingleTable(SJoinTableNode* pJoinTable) {
  return (((STableNode*)pJoinTable->pLeft)->singleTable && ((STableNode*)pJoinTable->pRight)->singleTable);
}

static bool isSingleTable(SRealTableNode* pRealTable) {
  int8_t tableType = pRealTable->pMeta->tableType;
  if (TSDB_SYSTEM_TABLE == tableType) {
    return 0 != strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_TABLES) &&
           0 != strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_TAGS) &&
           0 != strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_COLS) &&
           0 != strcmp(pRealTable->table.tableName, TSDB_INS_DISK_USAGE) &&
           0 != strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_FILESETS) &&
           0 != strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_VC_COLS);
  }
  return (TSDB_CHILD_TABLE == tableType || TSDB_NORMAL_TABLE == tableType || TSDB_VIRTUAL_CHILD_TABLE == tableType ||
          TSDB_VIRTUAL_NORMAL_TABLE == tableType);
}

static int32_t setTableTsmas(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  int32_t code = 0;
  if (QUERY_SMA_OPTIMIZE_DISABLE == tsQuerySmaOptimize) {
    return TSDB_CODE_SUCCESS;
  }
  if (isSelectStmt(pCxt->pCurrStmt) && pRealTable->pMeta->tableType != TSDB_SYSTEM_TABLE) {
    code = getTableTsmas(pCxt, pName, &pRealTable->pTsmas);
    // if select from a child table, fetch it's corresponding tsma target child table infos
    if (TSDB_CODE_SUCCESS == code && pRealTable->pTsmas &&
        (pRealTable->pMeta->tableType == TSDB_CHILD_TABLE || pRealTable->pMeta->tableType == TSDB_NORMAL_TABLE)) {
      if (pRealTable->tsmaTargetTbVgInfo) {
        taosArrayDestroyP(pRealTable->tsmaTargetTbVgInfo, NULL);
        pRealTable->tsmaTargetTbVgInfo = NULL;
      }
      char buf[TSDB_TABLE_FNAME_LEN + TSDB_TABLE_NAME_LEN + 1];
      for (int32_t i = 0; i < pRealTable->pTsmas->size; ++i) {
        STableTSMAInfo* pTsma = taosArrayGetP(pRealTable->pTsmas, i);
        SName           tsmaTargetTbName = {0};
        SVgroupInfo     vgInfo = {0};
        bool            exists = false;
        toName(pCxt->pParseCxt->acctId, pRealTable->table.dbName, "", &tsmaTargetTbName);
        int32_t len = tsnprintf(buf, TSDB_TABLE_FNAME_LEN + TSDB_TABLE_NAME_LEN, "%s.%s_%s", pTsma->dbFName,
                                pTsma->name, pRealTable->table.tableName);
        len = taosCreateMD5Hash(buf, len);
        tstrncpy(tsmaTargetTbName.tname, buf, TSDB_TABLE_NAME_LEN);
        code = collectUseTable(&tsmaTargetTbName, pCxt->pTargetTables);
        if (TSDB_CODE_SUCCESS == code)
          code = catalogGetCachedTableHashVgroup(pCxt->pParseCxt->pCatalog, &tsmaTargetTbName, &vgInfo, &exists);
        if (TSDB_CODE_SUCCESS == code) {
          if (!pRealTable->tsmaTargetTbVgInfo) {
            pRealTable->tsmaTargetTbVgInfo = taosArrayInit(pRealTable->pTsmas->size, POINTER_BYTES);
            if (!pRealTable->tsmaTargetTbVgInfo) {
              code = terrno;
              break;
            }
          }
          SVgroupsInfo* pVgpsInfo = taosMemoryCalloc(1, sizeof(int32_t) + sizeof(SVgroupInfo));
          if (!pVgpsInfo) {
            code = terrno;
            break;
          }
          pVgpsInfo->numOfVgroups = 1;
          pVgpsInfo->vgroups[0] = vgInfo;
          if (NULL == taosArrayPush(pRealTable->tsmaTargetTbVgInfo, &pVgpsInfo)) {
            code = terrno;
            break;
          }
        } else {
          break;
        }

        STableMeta* pTableMeta = NULL;
        if (code == TSDB_CODE_SUCCESS) {
          SRequestConnInfo conn = {.pTrans = pCxt->pParseCxt->pTransporter,
                                   .requestId = pCxt->pParseCxt->requestId,
                                   .requestObjRefId = pCxt->pParseCxt->requestRid,
                                   .mgmtEps = pCxt->pParseCxt->mgmtEpSet};
          code = catalogGetTableMeta(pCxt->pParseCxt->pCatalog, &conn, &tsmaTargetTbName, &pTableMeta);
        }
        STsmaTargetTbInfo ctbInfo = {0};
        if (!pRealTable->tsmaTargetTbInfo) {
          pRealTable->tsmaTargetTbInfo = taosArrayInit(pRealTable->pTsmas->size, sizeof(STsmaTargetTbInfo));
          if (!pRealTable->tsmaTargetTbInfo) {
            code = terrno;
            break;
          }
        }
        if (code == TSDB_CODE_SUCCESS) {
          snprintf(ctbInfo.tableName, TSDB_TABLE_NAME_LEN, "%s", tsmaTargetTbName.tname);
          ctbInfo.uid = pTableMeta->uid;
          taosMemoryFree(pTableMeta);
        } else if (TSDB_CODE_PAR_TABLE_NOT_EXIST == code) {
          // ignore table not exists error
          code = TSDB_CODE_SUCCESS;
        }
        if (NULL == taosArrayPush(pRealTable->tsmaTargetTbInfo, &ctbInfo)) {
          code = terrno;
          break;
        }
      }
    }
  }
  return code;
}

static int32_t setTableCacheLastMode(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if ((!pSelect->hasLastRowFunc && !pSelect->hasLastFunc) || QUERY_NODE_REAL_TABLE != nodeType(pSelect->pFromTable) ||
      TSDB_SYSTEM_TABLE == ((SRealTableNode*)pSelect->pFromTable)->pMeta->tableType) {
    return TSDB_CODE_SUCCESS;
  }

  SRealTableNode* pTable = (SRealTableNode*)pSelect->pFromTable;
  SDbCfgInfo      dbCfg = {0};
  int32_t         code = getDBCfg(pCxt, pTable->table.dbName, &dbCfg);
  if (TSDB_CODE_SUCCESS == code) {
    pTable->cacheLastMode = dbCfg.cacheLast;
  }
  return code;
}

static EDealRes doTranslateTbName(SNode** pNode, void* pContext) {
  switch (nodeType(*pNode)) {
    case QUERY_NODE_FUNCTION: {
      SFunctionNode* pFunc = (SFunctionNode*)*pNode;
      if (FUNCTION_TYPE_TBNAME == pFunc->funcType) {
        SRewriteTbNameContext* pCxt = (SRewriteTbNameContext*)pContext;
        SValueNode*            pVal = NULL;
        int32_t                code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);
        if (TSDB_CODE_SUCCESS != code) {
          pCxt->errCode = code;
          return DEAL_RES_ERROR;
        }

        int32_t tbLen = strlen(pCxt->pTbName);
        pVal->literal = taosStrdup(pCxt->pTbName);
        if (NULL == pVal->literal) {
          pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
          nodesDestroyNode((SNode*)pVal);
          return DEAL_RES_ERROR;
        }
        pVal->translate = true;
        pVal->node.resType.type = TSDB_DATA_TYPE_BINARY;
        pVal->node.resType.bytes = tbLen + VARSTR_HEADER_SIZE;
        pVal->datum.p = taosMemoryCalloc(1, tbLen + VARSTR_HEADER_SIZE + 1);
        if (!pVal->datum.p) {
          pCxt->errCode = terrno;
          nodesDestroyNode((SNode*)pVal);
          return DEAL_RES_ERROR;
        }
        varDataSetLen(pVal->datum.p, tbLen);
        tstrncpy(varDataVal(pVal->datum.p), pVal->literal, tbLen + 1);
        tstrncpy(pVal->node.userAlias, pFunc->node.userAlias, TSDB_COL_NAME_LEN);
        tstrncpy(pVal->node.aliasName, pFunc->node.aliasName, TSDB_COL_NAME_LEN);
        nodesDestroyNode(*pNode);
        *pNode = (SNode*)pVal;
      }
      break;
    }
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t replaceTbName(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (QUERY_NODE_REAL_TABLE != nodeType(pSelect->pFromTable)) {
    return TSDB_CODE_SUCCESS;
  }

  SRealTableNode* pTable = (SRealTableNode*)pSelect->pFromTable;
  if (TSDB_CHILD_TABLE != pTable->pMeta->tableType && TSDB_NORMAL_TABLE != pTable->pMeta->tableType &&
      TSDB_SYSTEM_TABLE != pTable->pMeta->tableType) {
    return TSDB_CODE_SUCCESS;
  }

  SNode**               pNode = NULL;
  SRewriteTbNameContext pRewriteCxt = {0};
  pRewriteCxt.pTbName = pTable->table.tableName;

  nodesRewriteExprPostOrder(&pSelect->pWhere, doTranslateTbName, &pRewriteCxt);

  return pRewriteCxt.errCode;
}

static int32_t addPrimJoinEqCond(SNode** pCond, SRealTableNode* leftTable, SRealTableNode* rightTable,
                                 EJoinType joinType, EJoinSubType subType) {
  struct STableMeta* pLMeta = leftTable->pMeta;
  struct STableMeta* pRMeta = rightTable->pMeta;

  *pCond = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR, pCond);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  SOperatorNode* pOp = (SOperatorNode*)*pCond;
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  if (IS_WINDOW_JOIN(subType)) {
    pOp->opType = OP_TYPE_EQUAL;
  } else if (JOIN_TYPE_LEFT == joinType) {
    pOp->opType = OP_TYPE_GREATER_EQUAL;
  } else {
    pOp->opType = OP_TYPE_LOWER_EQUAL;
  }

  SColumnNode* pLeft = NULL;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pLeft);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(*pCond);
    return code;
  }
  pLeft->node.resType.type = pLMeta->schema[0].type;
  pLeft->node.resType.bytes = pLMeta->schema[0].bytes;
  pLeft->tableId = pLMeta->uid;
  pLeft->colId = pLMeta->schema[0].colId;
  pLeft->colType = COLUMN_TYPE_COLUMN;
  tstrncpy(pLeft->tableName, leftTable->table.tableName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pLeft->tableAlias, leftTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
  tstrncpy(pLeft->colName, pLMeta->schema[0].name, TSDB_COL_NAME_LEN);

  pOp->pLeft = (SNode*)pLeft;

  SColumnNode* pRight = NULL;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pRight);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(*pCond);
    return code;
  }
  pRight->node.resType.type = pRMeta->schema[0].type;
  pRight->node.resType.bytes = pRMeta->schema[0].bytes;
  pRight->tableId = pRMeta->uid;
  pRight->colId = pRMeta->schema[0].colId;
  pRight->colType = COLUMN_TYPE_COLUMN;
  tstrncpy(pRight->tableName, rightTable->table.tableName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pRight->tableAlias, rightTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
  tstrncpy(pRight->colName, pRMeta->schema[0].name, TSDB_COL_NAME_LEN);

  pOp->pRight = (SNode*)pRight;

  return TSDB_CODE_SUCCESS;
}

static bool getJoinContais(SNode* pNode) {
  if (QUERY_NODE_REAL_TABLE == nodeType(pNode)) {
    return false;
  }
  if (QUERY_NODE_JOIN_TABLE == nodeType(pNode)) {
    return true;
  }
  if (QUERY_NODE_TEMP_TABLE == nodeType(pNode)) {
    pNode = ((STempTableNode*)pNode)->pSubquery;
  }

  switch (nodeType(pNode)) {
    case QUERY_NODE_REAL_TABLE:
      return false;
    case QUERY_NODE_JOIN_TABLE:
      return true;
    case QUERY_NODE_TEMP_TABLE:
      pNode = ((STempTableNode*)pNode)->pSubquery;
      break;
    default:
      break;
  }

  switch (nodeType(pNode)) {
    case QUERY_NODE_SELECT_STMT: {
      SSelectStmt* pSelect = (SSelectStmt*)pNode;
      return pSelect->joinContains;
    }
    case QUERY_NODE_SET_OPERATOR: {
      SSetOperator* pSet = (SSetOperator*)pNode;
      return pSet->joinContains;
    }
    default:
      break;
  }

  return false;
}

static bool getBothJoinContais(SNode* pLeft, SNode* pRight) {
  bool joinContains = false;

  if (NULL != pLeft) {
    joinContains = getJoinContais(pLeft);
  }

  if (NULL != pRight && !joinContains) {
    joinContains = getJoinContais(pRight);
  }

  return joinContains;
}

static int32_t checkJoinTable(STranslateContext* pCxt, SJoinTableNode* pJoinTable) {
  if (JOIN_STYPE_NONE != pJoinTable->subType && getBothJoinContais(pJoinTable->pLeft, pJoinTable->pRight)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_SUPPORT_JOIN, "unsupported nested join type");
  }

  if (IS_ASOF_JOIN(pJoinTable->subType) || IS_WINDOW_JOIN(pJoinTable->subType)) {
    if (QUERY_NODE_REAL_TABLE != nodeType(pJoinTable->pLeft) || QUERY_NODE_REAL_TABLE != nodeType(pJoinTable->pRight)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
                                     "Only support ASOF/WINDOW join between tables");
    }

    SRealTableNode* pLeft = (SRealTableNode*)pJoinTable->pLeft;
    if (TSDB_SUPER_TABLE != pLeft->pMeta->tableType && TSDB_CHILD_TABLE != pLeft->pMeta->tableType &&
        TSDB_NORMAL_TABLE != pLeft->pMeta->tableType) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
                                     "Unsupported ASOF/WINDOW join table type");
    }

    SRealTableNode* pRight = (SRealTableNode*)pJoinTable->pRight;
    if (TSDB_SUPER_TABLE != pRight->pMeta->tableType && TSDB_CHILD_TABLE != pRight->pMeta->tableType &&
        TSDB_NORMAL_TABLE != pRight->pMeta->tableType) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
                                     "Unsupported ASOF/WINDOW join table type");
    }

    if (IS_WINDOW_JOIN(pJoinTable->subType)) {
      if (pLeft->table.precision != pRight->table.precision) {
        return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
                                       "Same database precision required in WINDOW join");
      }
      SWindowOffsetNode* pWinOffset = (SWindowOffsetNode*)pJoinTable->pWindowOffset;
      SValueNode*        pStart = (SValueNode*)pWinOffset->pStartOffset;
      SValueNode*        pEnd = (SValueNode*)pWinOffset->pEndOffset;
      switch (pLeft->table.precision) {
        case TSDB_TIME_PRECISION_MILLI:
          if (TIME_UNIT_NANOSECOND == pStart->unit || TIME_UNIT_MICROSECOND == pStart->unit) {
            return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_WIN_OFFSET_UNIT, pStart->unit);
          }
          if (TIME_UNIT_NANOSECOND == pEnd->unit || TIME_UNIT_MICROSECOND == pEnd->unit) {
            return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_WIN_OFFSET_UNIT, pEnd->unit);
          }
          break;
        case TSDB_TIME_PRECISION_MICRO:
          if (TIME_UNIT_NANOSECOND == pStart->unit) {
            return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_WIN_OFFSET_UNIT, pStart->unit);
          }
          if (TIME_UNIT_NANOSECOND == pEnd->unit) {
            return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_WIN_OFFSET_UNIT, pEnd->unit);
          }
          break;
        default:
          break;
      }
    }

    int32_t code =
        addPrimJoinEqCond(&pJoinTable->addPrimCond, pLeft, pRight, pJoinTable->joinType, pJoinTable->subType);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  if (IS_WINDOW_JOIN(pJoinTable->subType)) {
    SSelectStmt* pCurrSmt = (SSelectStmt*)(pCxt->pCurrStmt);
    if (NULL != pCurrSmt->pWindow || NULL != pCurrSmt->pPartitionByList || NULL != pCurrSmt->pGroupByList) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
                                     "No WINDOW/GROUP BY/PARTITION BY allowed in WINDOW join");
    }
  }

  if (QUERY_NODE_TEMP_TABLE == nodeType(pJoinTable->pLeft) &&
      !isGlobalTimeLineQuery(((STempTableNode*)pJoinTable->pLeft)->pSubquery)) {
    if (IS_ASOF_JOIN(pJoinTable->subType) || IS_WINDOW_JOIN(pJoinTable->subType)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
                                     "Join requires valid time series input");
    }
    pJoinTable->leftNoOrderedSubQuery = true;
  }

  if (QUERY_NODE_TEMP_TABLE == nodeType(pJoinTable->pRight) &&
      !isGlobalTimeLineQuery(((STempTableNode*)pJoinTable->pRight)->pSubquery)) {
    if (IS_ASOF_JOIN(pJoinTable->subType) || IS_WINDOW_JOIN(pJoinTable->subType)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
                                     "Join requires valid time series input");
    }
    pJoinTable->rightNoOrderedSubQuery = true;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateJoinTableImpl(STranslateContext* pCxt, SJoinTableNode* pJoinTable) {
  int32_t       code = TSDB_CODE_SUCCESS;
  EJoinType     type = pJoinTable->joinType;
  EJoinSubType* pSType = &pJoinTable->subType;
  SSelectStmt*  pCurrSmt = (SSelectStmt*)(pCxt->pCurrStmt);

  switch (type) {
    case JOIN_TYPE_INNER:
      if (*pSType == JOIN_STYPE_OUTER || *pSType == JOIN_STYPE_SEMI || *pSType == JOIN_STYPE_ANTI ||
          *pSType == JOIN_STYPE_ASOF || *pSType == JOIN_STYPE_WIN) {
        return buildInvalidOperationMsg(&pCxt->msgBuf, "not supported join type");
      }
      break;
    case JOIN_TYPE_FULL:
      if (*pSType == JOIN_STYPE_SEMI || *pSType == JOIN_STYPE_ANTI || *pSType == JOIN_STYPE_ASOF ||
          *pSType == JOIN_STYPE_WIN) {
        return buildInvalidOperationMsg(&pCxt->msgBuf, "not supported join type");
      }
    // fall down
    default:
      if (*pSType == JOIN_STYPE_NONE) {
        *pSType = JOIN_STYPE_OUTER;
      }
      break;
  }

  if (NULL != pJoinTable->pWindowOffset) {
    if (*pSType != JOIN_STYPE_WIN) {
      return buildInvalidOperationMsg(&pCxt->msgBuf, "WINDOW_OFFSET only supported for WINDOW join");
    }
    code = translateExpr(pCxt, &pJoinTable->pWindowOffset);
    if (TSDB_CODE_SUCCESS == code) {
      SValueNode* pStart = (SValueNode*)((SWindowOffsetNode*)pJoinTable->pWindowOffset)->pStartOffset;
      SValueNode* pEnd = (SValueNode*)((SWindowOffsetNode*)pJoinTable->pWindowOffset)->pEndOffset;
      if (TIME_UNIT_MONTH == pStart->unit || TIME_UNIT_YEAR == pStart->unit) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_WIN_OFFSET_UNIT, pStart->unit);
      }
      if (TIME_UNIT_MONTH == pEnd->unit || TIME_UNIT_YEAR == pEnd->unit) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_WIN_OFFSET_UNIT, pEnd->unit);
      }
      if (pStart->datum.i > pEnd->datum.i) {
        TSWAP(((SWindowOffsetNode*)pJoinTable->pWindowOffset)->pStartOffset,
              ((SWindowOffsetNode*)pJoinTable->pWindowOffset)->pEndOffset);
      }
    }
  } else if (*pSType == JOIN_STYPE_WIN) {
    return buildInvalidOperationMsg(&pCxt->msgBuf, "WINDOW_OFFSET required for WINDOW join");
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pJoinTable->pJLimit && NULL != ((SLimitNode*)pJoinTable->pJLimit)->limit) {
    if (*pSType != JOIN_STYPE_ASOF && *pSType != JOIN_STYPE_WIN) {
      return buildInvalidOperationMsgExt(&pCxt->msgBuf, "JLIMIT not supported for %s join",
                                         getFullJoinTypeString(type, *pSType));
    }
    SLimitNode* pJLimit = (SLimitNode*)pJoinTable->pJLimit;
    code = translateExpr(pCxt, (SNode**)&pJLimit->limit);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    if (pJLimit->limit->datum.i > JOIN_JLIMIT_MAX_VALUE || pJLimit->limit->datum.i < 0) {
      return buildInvalidOperationMsg(&pCxt->msgBuf, "JLIMIT value is out of valid range [0, 1024]");
    }
    if (0 == pJLimit->limit->datum.i) {
      pCurrSmt->isEmptyResult = true;
    }
  }

  return code;
}

EDealRes joinCondsValidater(SNode* pNode, void* pContext) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_CONDITION: {
      SLogicConditionNode* pLogic = (SLogicConditionNode*)pNode;
      if (LOGIC_COND_TYPE_AND != pLogic->condType) {
        break;
      }
      return DEAL_RES_CONTINUE;
    }
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOp = (SOperatorNode*)pNode;
      if (OP_TYPE_EQUAL < pOp->opType || OP_TYPE_GREATER_THAN > pOp->opType) {
        break;
      }
      if ((QUERY_NODE_COLUMN != nodeType(pOp->pLeft) && QUERY_NODE_FUNCTION != nodeType(pOp->pLeft) &&
           !(QUERY_NODE_OPERATOR == nodeType(pOp->pLeft) &&
             OP_TYPE_JSON_GET_VALUE == ((SOperatorNode*)pOp->pLeft)->opType)) ||
          (QUERY_NODE_COLUMN != nodeType(pOp->pRight) && QUERY_NODE_FUNCTION != nodeType(pOp->pRight) &&
           !(QUERY_NODE_OPERATOR == nodeType(pOp->pRight) &&
             OP_TYPE_JSON_GET_VALUE == ((SOperatorNode*)pOp->pRight)->opType))) {
        break;
      }
      if (QUERY_NODE_COLUMN == nodeType(pOp->pLeft)) {
        SColumnNode* pCol = (SColumnNode*)pOp->pLeft;
        if (PRIMARYKEY_TIMESTAMP_COL_ID != pCol->colId && OP_TYPE_EQUAL != pOp->opType) {
          break;
        }
      }
      if (QUERY_NODE_COLUMN == nodeType(pOp->pRight)) {
        SColumnNode* pCol = (SColumnNode*)pOp->pRight;
        if (PRIMARYKEY_TIMESTAMP_COL_ID != pCol->colId && OP_TYPE_EQUAL != pOp->opType) {
          break;
        }
      }
      if (QUERY_NODE_FUNCTION == nodeType(pOp->pLeft) &&
          FUNCTION_TYPE_TIMETRUNCATE == ((SFunctionNode*)pOp->pLeft)->funcType) {
        SFunctionNode* pFunc = (SFunctionNode*)pOp->pLeft;
        SNode*         pParam = nodesListGetNode(pFunc->pParameterList, 0);
        if (QUERY_NODE_COLUMN != nodeType(pParam)) {
          break;
        }
        SColumnNode* pCol = (SColumnNode*)pParam;
        if (PRIMARYKEY_TIMESTAMP_COL_ID != pCol->colId) {
          break;
        }
      }
      if (QUERY_NODE_FUNCTION == nodeType(pOp->pRight) &&
          FUNCTION_TYPE_TIMETRUNCATE == ((SFunctionNode*)pOp->pRight)->funcType) {
        SFunctionNode* pFunc = (SFunctionNode*)pOp->pRight;
        SNode*         pParam = nodesListGetNode(pFunc->pParameterList, 0);
        if (QUERY_NODE_COLUMN != nodeType(pParam)) {
          break;
        }
        SColumnNode* pCol = (SColumnNode*)pParam;
        if (PRIMARYKEY_TIMESTAMP_COL_ID != pCol->colId) {
          break;
        }
      }
      return DEAL_RES_IGNORE_CHILD;
    }
    default:
      break;
  }

  *(int32_t*)pContext = TSDB_CODE_QRY_INVALID_JOIN_CONDITION;
  return DEAL_RES_ERROR;
}

int32_t validateJoinConds(STranslateContext* pCxt, SJoinTableNode* pJoinTable) {
  if (JOIN_STYPE_ASOF != pJoinTable->subType && JOIN_STYPE_WIN != pJoinTable->subType) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;
  nodesWalkExpr(pJoinTable->pOnCond, joinCondsValidater, &code);

  return code;
}

static int32_t cloneVgroups(SVgroupsInfo **pDst, SVgroupsInfo* pSrc) {
  if (pSrc == NULL) {
    *pDst = NULL;
    return TSDB_CODE_SUCCESS;
  }
  int32_t len = VGROUPS_INFO_SIZE(pSrc);
  *pDst = taosMemoryMalloc(len);
  if (NULL == *pDst) {
    return terrno;
  }
  memcpy(*pDst, pSrc, len);
  return TSDB_CODE_SUCCESS;
}

static int32_t makeVtableMetaScanTable(STranslateContext* pCxt, SRealTableNode** pScan) {
  int32_t code = TSDB_CODE_SUCCESS;
  bool tmpAsync = pCxt->pParseCxt->async;
  pCxt->pParseCxt->async = false;
  pCxt->refTable = true;
  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)pScan));
  tstrncpy((*pScan)->table.dbName, TSDB_INFORMATION_SCHEMA_DB, sizeof((*pScan)->table.dbName));
  tstrncpy((*pScan)->table.tableName, TSDB_INS_TABLE_VC_COLS, sizeof((*pScan)->table.tableName));
  tstrncpy((*pScan)->table.tableAlias, TSDB_INS_TABLE_VC_COLS, sizeof((*pScan)->table.tableAlias));
  PAR_ERR_JRET(translateTable(pCxt, (SNode**)pScan, false));
  pCxt->refTable = false;
  pCxt->pParseCxt->async = tmpAsync;
  return code;

_return:
  parserError("make vtable meta table failed");
  return code;
}

static int32_t translateVirtualSuperTable(STranslateContext* pCxt, SNode** pTable, SName* pName,
                                          SVirtualTableNode* pVTable) {
  SRealTableNode*    pRealTable = (SRealTableNode*)*pTable;
  STableMeta*        pMeta = pRealTable->pMeta;
  int32_t            code = TSDB_CODE_SUCCESS;
  SRealTableNode*    pInsCols = NULL;

  if (!pMeta->virtualStb) {
    PAR_ERR_JRET(TSDB_CODE_PAR_INVALID_TABLE_TYPE);
  }

  PAR_ERR_JRET(getTargetMeta(pCxt, pName, &(pVTable->pMeta), true));
  PAR_ERR_JRET(setVSuperTableVgroupList(pCxt, pName, pVTable));
  PAR_ERR_JRET(setVSuperTableRefScanVgroupList(pCxt, pName, pRealTable));
  if (pRealTable->pVgroupList->numOfVgroups == 0) {
    // no vgroups, means virtual super table do not have child table, make a fake one is ok.
    PAR_ERR_JRET(cloneVgroups(&pRealTable->pVgroupList, pVTable->pVgroupList));
  }
  PAR_ERR_JRET(nodesListMakeAppend(&pVTable->refTables, (SNode*)pRealTable));

  PAR_ERR_JRET(makeVtableMetaScanTable(pCxt, &pInsCols));
  PAR_ERR_JRET(nodesListMakeAppend(&pVTable->refTables, (SNode*)pInsCols));

  *pTable = (SNode*)pVTable;

  return code;
_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("translateVirtualSuperTable failed, code:%d, errmsg:%s", code, tstrerror(code));
  }
  nodesDestroyNode((SNode*)pInsCols);
  return code;
}

static void setTableNameByColRef(SRealTableNode* pTable, SColRef* pRef) {
  tstrncpy(pTable->table.dbName, pRef->refDbName, sizeof(pTable->table.dbName));
  tstrncpy(pTable->table.tableName, pRef->refTableName, sizeof(pTable->table.tableName));
  tstrncpy(pTable->table.tableAlias, pRef->refTableName, sizeof(pTable->table.tableAlias));
}

static int32_t translateVirtualNormalChildTableInStream(STranslateContext* pCxt, SNode** pTable, SName* pName,
                                                        SVirtualTableNode* pVTable) {
  int32_t            code = TSDB_CODE_SUCCESS;
  SHashObj*          pTableVgHash = NULL;
  SRealTableNode*    pRealTable = (SRealTableNode*)*pTable;
  STableMeta*        pMeta = pRealTable->pMeta;
  int32_t            lino = 0;
  SRealTableNode*    pInsCols = NULL;
  SArray*            tmpVgroupList = NULL;

  PAR_ERR_JRET(getTargetMeta(pCxt, pName, &(pVTable->pMeta), true));
  PAR_ERR_JRET(setVirtualTableVgroupList(pCxt, pName, pVTable));
  pTableVgHash =
      taosHashInit(pMeta->numOfColRefs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pTableVgHash, code, lino, _return, terrno);

  tmpVgroupList = taosArrayInit(1, sizeof(SVgroupInfo));
  QUERY_CHECK_NULL(tmpVgroupList, code, lino, _return, terrno);

  bool tmpAsync = pCxt->pParseCxt->async;
  pCxt->pParseCxt->async = false;
  for (int32_t i = 0; i < pMeta->numOfColRefs; i++) {
    if (pMeta->colRef[i].hasRef) {
      SVgroupInfo  refVgInfo = {0};
      SName        refName = {0};
      toName(pCxt->pParseCxt->acctId, pMeta->colRef[i].refDbName, pMeta->colRef[i].refTableName, &refName);
      PAR_ERR_JRET(getTableHashVgroupImpl(pCxt, &refName, &refVgInfo));
      if (taosHashGet(pTableVgHash, &refVgInfo.vgId, sizeof(refVgInfo.vgId)) == NULL) {
        PAR_ERR_JRET(taosHashPut(pTableVgHash, &refVgInfo.vgId, sizeof(refVgInfo.vgId), NULL, 0));
        void *px = taosArrayPush(tmpVgroupList, &refVgInfo);
        QUERY_CHECK_NULL(px, code, lino, _return, terrno);
      }
    }
  }
  pCxt->pParseCxt->async = tmpAsync;

  taosMemoryFreeClear(pRealTable->pVgroupList);
  PAR_ERR_JRET(toVgroupsInfo(tmpVgroupList, &pRealTable->pVgroupList));
  if (pRealTable->pVgroupList->numOfVgroups == 0) {
    // no vgroups, means virtual table do not have origin table, make a fake one is ok.
    PAR_ERR_JRET(cloneVgroups(&pRealTable->pVgroupList, pVTable->pVgroupList));
  }
  PAR_ERR_JRET(nodesListMakeAppend(&pVTable->refTables, (SNode*)pRealTable));

  PAR_ERR_JRET(makeVtableMetaScanTable(pCxt, &pInsCols));
  PAR_ERR_JRET(nodesListMakeAppend(&pVTable->refTables, (SNode*)pInsCols));

  *pTable = (SNode*)pVTable;

  taosHashCleanup(pTableVgHash);
  taosArrayDestroy(tmpVgroupList);
  return code;

_return:
  qError("translateVirtualNormalChildTableInStream failed, lino:%d, code:%d, errmsg:%s", lino, code, tstrerror(code));
  taosHashCleanup(pTableVgHash);
  taosArrayDestroy(tmpVgroupList);
  nodesDestroyNode((SNode*)pInsCols);
  return code;
}

static int32_t translateVirtualNormalChildTable(STranslateContext* pCxt, SNode** pTable, SName* pName,
                                                SVirtualTableNode* pVTable) {
  int32_t            code = TSDB_CODE_SUCCESS;
  SHashObj*          pTableNameHash = NULL;
  SRealTableNode*    pRealTable = (SRealTableNode*)*pTable;
  STableMeta*        pMeta = pRealTable->pMeta;
  int32_t            lino = 0;
  SRealTableNode*    pRTNode = NULL;

  TSWAP(pVTable->pMeta, pRealTable->pMeta);
  TSWAP(pVTable->pVgroupList, pRealTable->pVgroupList);

  pTableNameHash =
      taosHashInit(pMeta->numOfColRefs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pTableNameHash, code, lino, _return, terrno);

  bool tmpAsync = pCxt->pParseCxt->async;
  pCxt->pParseCxt->async = false;
  pCxt->refTable = true;
  for (int32_t i = 0; i < pMeta->numOfColRefs; i++) {
    if (pMeta->colRef[i].hasRef) {
      char tableNameKey[TSDB_TABLE_FNAME_LEN] = {0};
      strcat(tableNameKey, pMeta->colRef[i].refDbName);
      strcat(tableNameKey, ".");
      strcat(tableNameKey, pMeta->colRef[i].refTableName);
      if (taosHashGet(pTableNameHash, tableNameKey, strlen(tableNameKey)) == NULL) {
        PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)&pRTNode));
        setTableNameByColRef(pRTNode, &pMeta->colRef[i]);
        PAR_ERR_JRET(translateTable(pCxt, (SNode**)&pRTNode, false));
        PAR_ERR_JRET(nodesListMakeAppend(&pVTable->refTables, (SNode*)pRTNode));
        PAR_ERR_JRET(taosHashPut(pTableNameHash, tableNameKey, strlen(tableNameKey), NULL, 0));
      }
    }
  }
  pCxt->refTable = false;
  pCxt->pParseCxt->async = tmpAsync;
  nodesDestroyNode(*pTable);
  *pTable = (SNode*)pVTable;

  taosHashCleanup(pTableNameHash);
  return code;

_return:
  qError("translateVirtualNormalChildTable failed, lino:%d, code:%d, errmsg:%s", lino, code, tstrerror(code));
  taosHashCleanup(pTableNameHash);
  nodesDestroyNode((SNode*)pRTNode);
  return code;
}

static int32_t translateVirtualTable(STranslateContext* pCxt, SNode** pTable, SName* pName) {
  SRealTableNode*    pRealTable = (SRealTableNode*)*pTable;
  int32_t            code = TSDB_CODE_SUCCESS;
  STableMeta*        pMeta = pRealTable->pMeta;
  SVirtualTableNode* pVTable = NULL;

  if (!isSelectStmt(pCxt->pCurrStmt)) {
    // virtual table only support select operation
    PAR_ERR_JRET(TSDB_CODE_TSC_INVALID_OPERATION);
  }
  if (pCxt->pParseCxt->stmtBindVersion > 0) {
    PAR_ERR_JRET(TSDB_CODE_VTABLE_NOT_SUPPORT_STMT);
  }
  if (pCxt->pParseCxt->topicQuery) {
    PAR_ERR_JRET(TSDB_CODE_VTABLE_NOT_SUPPORT_TOPIC);
  }

  PAR_ERR_RET(nodesMakeNode(QUERY_NODE_VIRTUAL_TABLE, (SNode**)&pVTable));
  tstrncpy(pVTable->table.dbName, pRealTable->table.dbName, sizeof(pVTable->table.dbName));
  tstrncpy(pVTable->table.tableName, pRealTable->table.tableName, sizeof(pVTable->table.tableName));
  tstrncpy(pVTable->table.tableAlias, pRealTable->table.tableAlias, sizeof(pVTable->table.tableAlias));

  pVTable->table.precision = pRealTable->pMeta->tableInfo.precision;
  pVTable->table.singleTable = false;

  switch (pMeta->tableType) {
    case TSDB_SUPER_TABLE:
      PAR_ERR_JRET(translateVirtualSuperTable(pCxt, pTable, pName, pVTable));
      break;
    case TSDB_VIRTUAL_CHILD_TABLE:
    case TSDB_VIRTUAL_NORMAL_TABLE: {
      if (inStreamCalcClause(pCxt)) {
        PAR_ERR_JRET(translateVirtualNormalChildTableInStream(pCxt, pTable, pName, pVTable));
      } else {
        PAR_ERR_JRET(translateVirtualNormalChildTable(pCxt, pTable, pName, pVTable));
      }
      break;
    }
    default:
      PAR_ERR_JRET(TSDB_CODE_PAR_INVALID_TABLE_TYPE);
      break;
  }

  ((SVirtualTableNode*)*pTable)->table.singleTable = true;
  PAR_ERR_JRET(addNamespace(pCxt, *pTable));
  return code;
_return:
  qError("translateVirtualTable failed, code:%d, errmsg:%s", code, tstrerror(code));
  nodesDestroyNode((SNode*)pVTable);
  return code;
}

static int32_t translateAudit(STranslateContext* pCxt, SRealTableNode* pRealTable, SName* pName) {
  if (pRealTable->pMeta->tableType == TSDB_SUPER_TABLE) {
    if (IS_AUDIT_DBNAME(pName->dbname) && IS_AUDIT_STB_NAME(pName->tname)) {
      pCxt->pParseCxt->isAudit = true;
    }
  } else if (pRealTable->pMeta->tableType == TSDB_CHILD_TABLE) {
    if (IS_AUDIT_DBNAME(pName->dbname) && IS_AUDIT_CTB_NAME(pName->tname)) {
      pCxt->pParseCxt->isAudit = true;
    }
  }
  return 0;
}

static bool isJoinTagEqualOnCond(SNode* pCond, char* leftTableAlias, char* rightTableAlias) {
  if (QUERY_NODE_OPERATOR != nodeType(pCond)) {
    return false;
  }
  SOperatorNode* pOper = (SOperatorNode*)pCond;
  if (QUERY_NODE_COLUMN != nodeType(pOper->pLeft) || NULL == pOper->pRight ||
      QUERY_NODE_COLUMN != nodeType(pOper->pRight)) {
    return false;
  }
  SColumnNode* pLeft = (SColumnNode*)(pOper->pLeft);
  SColumnNode* pRight = (SColumnNode*)(pOper->pRight);

  if ((COLUMN_TYPE_TAG != pLeft->colType) || (COLUMN_TYPE_TAG != pRight->colType)) {
    return false;
  }

  if (OP_TYPE_EQUAL != pOper->opType) {
    return false;
  }

  if (pLeft->node.resType.type != pRight->node.resType.type ||
      pLeft->node.resType.bytes != pRight->node.resType.bytes) {
    return false;
  }
  bool isEqual = false;
  if (0 == strcmp(pLeft->tableAlias, leftTableAlias)) {
    isEqual = (0 == strcmp(pRight->tableAlias, rightTableAlias));
  } else if (0 == strcmp(pLeft->tableAlias, rightTableAlias)) {
    isEqual = (0 == strcmp(pRight->tableAlias, leftTableAlias));
  }

  return isEqual;
}

static bool joinTagEqCondContains(SNode* pCond, char* leftTableAlias, char* rightTableAlias) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pCond)) {
    SLogicConditionNode* pLogic = (SLogicConditionNode*)pCond;
    if (LOGIC_COND_TYPE_AND != pLogic->condType) {
      return false;
    }

    FOREACH(pCond, pLogic->pParameterList) {
      if (isJoinTagEqualOnCond(pCond, leftTableAlias, rightTableAlias)) {
        return true;
      }
    }

    return false;
  }

  if (QUERY_NODE_OPERATOR == nodeType(pCond)) {
    return isJoinTagEqualOnCond(pCond, leftTableAlias, rightTableAlias);
  }

  return false;
}

static bool innerJoinTagEqCondContains(SJoinTableNode* pJoinTable, SNode* pWhere) {
  bool            condContains = false;
  SRealTableNode* pLeftTable = (SRealTableNode*)pJoinTable->pLeft;
  SRealTableNode* pRightTable = (SRealTableNode*)pJoinTable->pRight;

  if (NULL != pJoinTable->pOnCond) {
    condContains =
        joinTagEqCondContains(pJoinTable->pOnCond, pLeftTable->table.tableAlias, pRightTable->table.tableAlias);
  }
  if (NULL != pWhere && !condContains) {
    condContains = joinTagEqCondContains(pWhere, pLeftTable->table.tableAlias, pRightTable->table.tableAlias);
  }

  return condContains;
}

static bool joinNonPrimColCondContains(SJoinTableNode* pJoinTable) {
  if (NULL == pJoinTable->pOnCond) {
    return false;
  }

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pJoinTable->pOnCond)) {
    SLogicConditionNode* pLogic = (SLogicConditionNode*)pJoinTable->pOnCond;
    if (LOGIC_COND_TYPE_AND != pLogic->condType) {
      return false;
    }

    SNode* pNode = NULL;
    FOREACH(pNode, pLogic->pParameterList) {
      if (QUERY_NODE_OPERATOR != nodeType(pNode)) {
        continue;
      }
      SOperatorNode* pOp = (SOperatorNode*)pNode;
      if (OP_TYPE_EQUAL != pOp->opType) {
        continue;
      }
      if (QUERY_NODE_COLUMN != nodeType(pOp->pLeft) || NULL == pOp->pRight ||
          QUERY_NODE_COLUMN != nodeType(pOp->pRight)) {
        continue;
      }
      if (isPrimaryKeyImpl(pOp->pLeft) || isPrimaryKeyImpl(pOp->pRight)) {
        continue;
      }
      return true;
    }

    return false;
  }

  if (QUERY_NODE_OPERATOR == nodeType(pJoinTable->pOnCond)) {
    SOperatorNode* pOp = (SOperatorNode*)pJoinTable->pOnCond;
    if (OP_TYPE_EQUAL != pOp->opType) {
      return false;
    }
    if (QUERY_NODE_COLUMN != nodeType(pOp->pLeft) || NULL == pOp->pRight ||
        QUERY_NODE_COLUMN != nodeType(pOp->pRight)) {
      return false;
    }
    if (isPrimaryKeyImpl(pOp->pLeft) || isPrimaryKeyImpl(pOp->pRight)) {
      return false;
    }

    return true;
  }

  return false;
}

static int32_t setJoinTimeLineResMode(STranslateContext* pCxt) {
  SSelectStmt* pCurrSmt = (SSelectStmt*)(pCxt->pCurrStmt);
  if (QUERY_NODE_JOIN_TABLE != nodeType(pCurrSmt->pFromTable)) {
    return TSDB_CODE_SUCCESS;
  }

  SJoinTableNode* pJoinTable = (SJoinTableNode*)pCurrSmt->pFromTable;
  if (JOIN_TYPE_FULL == pJoinTable->joinType) {
    pCurrSmt->timeLineResMode = TIME_LINE_NONE;
    pCurrSmt->timeLineCurMode = TIME_LINE_NONE;
    return TSDB_CODE_SUCCESS;
  }

  if (IS_ASOF_JOIN(pJoinTable->subType) || IS_WINDOW_JOIN(pJoinTable->subType)) {
    if (joinNonPrimColCondContains(pJoinTable)) {
      if (TIME_LINE_NONE != pCurrSmt->timeLineResMode) {
        pCurrSmt->timeLineResMode = TIME_LINE_BLOCK;
      }
      pCurrSmt->timeLineCurMode = TIME_LINE_BLOCK;
    }
    return TSDB_CODE_SUCCESS;
  }

  if (pJoinTable->table.singleTable || pJoinTable->hasSubQuery || pJoinTable->isLowLevelJoin) {
    return TSDB_CODE_SUCCESS;
  }

  if (JOIN_STYPE_NONE == pJoinTable->subType && innerJoinTagEqCondContains(pJoinTable, pCurrSmt->pWhere)) {
    if (TIME_LINE_NONE != pCurrSmt->timeLineResMode) {
      pCurrSmt->timeLineResMode = TIME_LINE_BLOCK;
    }
    pCurrSmt->timeLineCurMode = TIME_LINE_BLOCK;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mergeInnerJoinConds(SNode** ppDst, SNode** ppSrc) {
  SNode*  pNew = NULL;
  int32_t code = TSDB_CODE_SUCCESS;

  while (true) {
    if (QUERY_NODE_LOGIC_CONDITION == nodeType(*ppDst) &&
        ((SLogicConditionNode*)*ppDst)->condType == LOGIC_COND_TYPE_AND) {
      SLogicConditionNode* pLogic = (SLogicConditionNode*)*ppDst;
      if (QUERY_NODE_LOGIC_CONDITION == nodeType(*ppSrc) &&
          ((SLogicConditionNode*)*ppSrc)->condType == LOGIC_COND_TYPE_AND) {
        SLogicConditionNode* pSrcLogic = (SLogicConditionNode*)*ppSrc;
        code = nodesListMakeStrictAppendList(&pLogic->pParameterList, pSrcLogic->pParameterList);
        if (TSDB_CODE_SUCCESS == code) {
          pSrcLogic->pParameterList = NULL;
          nodesDestroyNode(*ppSrc);
          *ppSrc = NULL;
        }
      } else {
        code = nodesListMakeStrictAppend(&pLogic->pParameterList, *ppSrc);
        if (TSDB_CODE_SUCCESS == code) {
          *ppSrc = NULL;
        }
      }

      return code;
    }

    if (TSDB_CODE_SUCCESS == code && QUERY_NODE_LOGIC_CONDITION == nodeType(*ppSrc) &&
        ((SLogicConditionNode*)*ppSrc)->condType == LOGIC_COND_TYPE_AND) {
      SNode* pTmp = *ppDst;
      *ppDst = *ppSrc;
      *ppSrc = pTmp;
      continue;
    }

    if (TSDB_CODE_SUCCESS == code) {
      code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, &pNew);
    }
    if (TSDB_CODE_SUCCESS == code) {
      SLogicConditionNode* pLogic = (SLogicConditionNode*)pNew;
      pLogic->condType = LOGIC_COND_TYPE_AND;
      pLogic->node.resType.type = TSDB_DATA_TYPE_BOOL;
      pLogic->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
      code = nodesListMakeStrictAppend(&pLogic->pParameterList, *ppSrc);
      if (TSDB_CODE_SUCCESS == code) {
        *ppSrc = *ppDst;
        *ppDst = pNew;
        continue;
      }
    }

    if (code) {
      break;
    }
  }

  return code;
}

int32_t splitJoinColPrimaryCond(SNode** ppSrc, SNode** ppDst) {
  if (NULL == *ppSrc) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;
  switch (nodeType(*ppSrc)) {
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOp = (SOperatorNode*)*ppSrc;
      if (OP_TYPE_EQUAL != pOp->opType) {
        break;
      }
      if (nodesContainsColumn(pOp->pLeft) && nodesContainsColumn(pOp->pRight)) {
        TSWAP(*ppSrc, *ppDst);
      }
      break;
    }
    case QUERY_NODE_LOGIC_CONDITION: {
      SLogicConditionNode* pLogic = (SLogicConditionNode*)*ppSrc;
      if (LOGIC_COND_TYPE_AND != pLogic->condType) {
        break;
      }
      SNode* pTmp = NULL;
      SNode* pTmpRes = NULL;
      WHERE_EACH(pTmp, pLogic->pParameterList) {
        code = splitJoinColPrimaryCond(&pTmp, &pTmpRes);
        if (code) {
          break;
        }
        if (NULL == pTmp && NULL != pTmpRes) {
          cell->pNode = NULL;
          ERASE_NODE(pLogic->pParameterList);
          code = nodesMergeNode(ppDst, &pTmpRes);
          if (code) {
            break;
          }

          continue;
        }

        WHERE_NEXT;
      }
      if (pLogic->pParameterList->length <= 0) {
        nodesDestroyNode(*ppSrc);
        *ppSrc = NULL;
      }
      break;
    }
    default:
      break;
  }

  return code;
}

static bool isVirtualTable(STableMeta* meta) {
  if (meta->tableType == TSDB_VIRTUAL_CHILD_TABLE || meta->tableType == TSDB_VIRTUAL_NORMAL_TABLE) {
    return true;
  } else {
    return false;
  }
}

static bool isVirtualSTable(STableMeta* meta) {
  return meta->virtualStb;
}

static int32_t translateRealTable(STranslateContext* pCxt, SNode** pTable, bool inJoin) {
  SSelectStmt*    pCurrSmt = (SSelectStmt*)(pCxt->pCurrStmt);
  SRealTableNode* pRealTable = (SRealTableNode*)*pTable;
  int32_t         code = TSDB_CODE_SUCCESS;

  pRealTable->ratio = (NULL != pCxt->pExplainOpt ? pCxt->pExplainOpt->ratio : 1.0);
  // The SRealTableNode created through ROLLUP already has STableMeta.
  if (NULL == pRealTable->pMeta) {
    SName name = {0};
    toName(pCxt->pParseCxt->acctId, pRealTable->table.dbName, pRealTable->table.tableName, &name);
    if (pCxt->refTable) {
      PAR_ERR_JRET(collectUseTable(&name, pCxt->pTargetTables));
    }
    if (inStreamCalcClause(pCxt) && !pCxt->refTable) {
      char fullDbName[TSDB_DB_FNAME_LEN] = {0};
      PAR_ERR_JRET(tNameGetFullDbName(&name, fullDbName));
      if (pCxt->streamInfo.calcDbs) {
        PAR_ERR_JRET(taosHashPut(pCxt->streamInfo.calcDbs, fullDbName, TSDB_DB_FNAME_LEN, NULL, 0));
      }
    }
    PAR_ERR_JRET(getTargetMeta(pCxt, &name, &(pRealTable->pMeta), true));

#ifdef TD_ENTERPRISE
    if (TSDB_VIEW_TABLE == pRealTable->pMeta->tableType && (!pCurrSmt->tagScan || pCxt->pParseCxt->biMode)) {
      PAR_RET(translateView(pCxt, pTable, &name, inJoin));
    }
    PAR_ERR_JRET(translateAudit(pCxt, pRealTable, &name));
#endif
    PAR_ERR_JRET(setTableVgroupList(pCxt, &name, pRealTable));

    // create stream trigger's plan will treat virtual table as real table
    if (pRealTable->placeholderType != SP_PARTITION_ROWS && !inStreamTriggerClause(pCxt) &&
        (isVirtualTable(pRealTable->pMeta) || isVirtualSTable(pRealTable->pMeta))) {
      PAR_RET(translateVirtualTable(pCxt, pTable, &name));
    }

    PAR_ERR_JRET(setTableTsmas(pCxt, &name, pRealTable));
  }

  pRealTable->table.precision = pRealTable->pMeta->tableInfo.precision;
  pRealTable->table.singleTable = isSingleTable(pRealTable);
  if (TSDB_SUPER_TABLE == pRealTable->pMeta->tableType) {
    pCxt->stableQuery = true;
  }
  if (!pCxt->refTable) {
    if (TSDB_SYSTEM_TABLE == pRealTable->pMeta->tableType) {
      if (isSelectStmt(pCxt->pCurrStmt)) {
        ((SSelectStmt*)pCxt->pCurrStmt)->timeLineResMode = TIME_LINE_NONE;
        ((SSelectStmt*)pCxt->pCurrStmt)->timeLineCurMode = TIME_LINE_NONE;
      } else if (isDeleteStmt(pCxt->pCurrStmt)) {
        PAR_ERR_JRET(TSDB_CODE_TSC_INVALID_OPERATION);
      }
    }
    PAR_ERR_JRET(addNamespace(pCxt, pRealTable));
  }
  return code;
_return:
  parserError("translateRealTable failed, code:%d, errmsg:%s", code, tstrerror(code));
  return code;
}

static int32_t translateTempTable(STranslateContext* pCxt, SNode** pTable, bool inJoin) {
  SSelectStmt*    pCurrSmt = (SSelectStmt*)(pCxt->pCurrStmt);
  STempTableNode* pTempTable = (STempTableNode*)*pTable;
  SSelectStmt*    pSubStmt = (SSelectStmt*)pTempTable->pSubquery;
  int32_t         code = TSDB_CODE_SUCCESS;

  PAR_ERR_JRET(translateSubquery(pCxt, pTempTable->pSubquery));

  if (QUERY_NODE_SELECT_STMT == nodeType(pTempTable->pSubquery) && isSelectStmt(pCxt->pCurrStmt)) {
    if(pSubStmt->isEmptyResult) {
      pCurrSmt->isEmptyResult = true;
    }

    pCurrSmt->joinContains = pSubStmt->joinContains;
    pCurrSmt->timeLineResMode = pSubStmt->timeLineResMode;
    pCurrSmt->timeLineCurMode = pSubStmt->timeLineResMode;
  }

  pCurrSmt->joinContains = (getJoinContais(pTempTable->pSubquery) ? true : false);
  pTempTable->table.precision = getStmtPrecision(pTempTable->pSubquery);
  pTempTable->table.singleTable = stmtIsSingleTable(pTempTable->pSubquery);
  PAR_ERR_JRET(addNamespace(pCxt, pTempTable));

  return code;
_return:
  parserError("translateTempTable failed, code:%d, errmsg:%s", code, tstrerror(code));
  return code;
}

static int32_t translateJoinTable(STranslateContext* pCxt, SNode** pTable, bool inJoin) {
  SJoinTableNode* pJoinTable = (SJoinTableNode*)*pTable;
  SSelectStmt*    pCurrSmt = (SSelectStmt*)(pCxt->pCurrStmt);
  SNode*          pPrimCond = NULL;
  int32_t         code = TSDB_CODE_SUCCESS;

  PAR_ERR_JRET(translateJoinTableImpl(pCxt, pJoinTable));
  PAR_ERR_JRET(translateTable(pCxt, &pJoinTable->pLeft, true));
  PAR_ERR_JRET(translateTable(pCxt, &pJoinTable->pRight, true));
  PAR_ERR_JRET(checkJoinTable(pCxt, pJoinTable));

  if (!inJoin && pCurrSmt->pWhere && JOIN_TYPE_INNER == pJoinTable->joinType) {
    PAR_ERR_JRET(splitJoinColPrimaryCond(&pCurrSmt->pWhere, &pPrimCond));
    if (pPrimCond) {
      if (pJoinTable->pOnCond) {
        PAR_ERR_JRET(mergeInnerJoinConds(&pJoinTable->pOnCond, &pPrimCond));
      } else {
        pJoinTable->pOnCond = pPrimCond;
        pPrimCond = NULL;
      }
    }

  }
  pJoinTable->table.precision = calcJoinTablePrecision(pJoinTable);
  pJoinTable->table.singleTable = joinTableIsSingleTable(pJoinTable);
  pCurrSmt->precision = pJoinTable->table.precision;
  PAR_ERR_JRET(translateExpr(pCxt, &pJoinTable->pOnCond));
  pJoinTable->hasSubQuery = (nodeType(pJoinTable->pLeft) != QUERY_NODE_REAL_TABLE) ||
                            (nodeType(pJoinTable->pRight) != QUERY_NODE_REAL_TABLE);
  if (nodeType(pJoinTable->pLeft) == QUERY_NODE_JOIN_TABLE) {
    ((SJoinTableNode*)pJoinTable->pLeft)->isLowLevelJoin = true;
  }
  if (nodeType(pJoinTable->pRight) == QUERY_NODE_JOIN_TABLE) {
    ((SJoinTableNode*)pJoinTable->pRight)->isLowLevelJoin = true;
  }
  PAR_ERR_JRET(validateJoinConds(pCxt, pJoinTable));
  pCurrSmt->joinContains = true;

_return:
  if (TSDB_CODE_SUCCESS != code) {
    parserError("translateJoinTable failed, code:%d, errmsg:%s", code, tstrerror(code));
  }
  nodesDestroyNode(pPrimCond);
  return code;
}

static int32_t translatePlaceHolderTable(STranslateContext* pCxt, SNode** pTable, bool inJoin) {
  SPlaceHolderTableNode *pPlaceHolderTable = (SPlaceHolderTableNode*)*pTable;
  SRealTableNode        *newPlaceHolderTable = NULL;
  SRealTableNode        *pTriggerTable = (SRealTableNode*)getStreamTriggerTable(pCxt);
  SNodeList             *pPartitionList = getStreamTriggerPartition(pCxt);
  int32_t               code = TSDB_CODE_SUCCESS;

  if (!inStreamCalcClause(pCxt)) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION,
                                         "stream place holder should only appear in create stream's query part"));
  }

  if (!pTriggerTable) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION, "create stream trigger table is NULL"));
  }

  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)&newPlaceHolderTable));
  newPlaceHolderTable->placeholderType = pPlaceHolderTable->placeholderType;

  tstrncpy(newPlaceHolderTable->table.dbName, pTriggerTable->table.dbName, sizeof(newPlaceHolderTable->table.dbName));
  tstrncpy(newPlaceHolderTable->table.tableName, pTriggerTable->table.tableName, sizeof(newPlaceHolderTable->table.tableName));
  if (pPlaceHolderTable->table.tableAlias[0]) {
    tstrncpy(newPlaceHolderTable->table.tableAlias, pPlaceHolderTable->table.tableAlias, sizeof(newPlaceHolderTable->table.tableAlias));
  } else {
    tstrncpy(newPlaceHolderTable->table.tableAlias, pTriggerTable->table.tableAlias, sizeof(newPlaceHolderTable->table.tableAlias));
  }

  PAR_ERR_JRET(translateTable(pCxt, (SNode**)&newPlaceHolderTable, false));

  switch (pPlaceHolderTable->placeholderType) {
    case SP_PARTITION_TBNAME: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PARTITION_TBNAME);
      if (newPlaceHolderTable->pMeta->tableType == TSDB_SUPER_TABLE) {
        newPlaceHolderTable->asSingleTable = true;
        newPlaceHolderTable->table.singleTable = true;
      }
      break;
    }
    case SP_PARTITION_ROWS: {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS);
      if (hasTbnameFunction(pPartitionList) &&
          newPlaceHolderTable->pMeta->tableType == TSDB_SUPER_TABLE) {
        newPlaceHolderTable->asSingleTable = true;
        newPlaceHolderTable->table.singleTable = true;
      }
      if (inJoin) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                             "%%%%trows should not appear in join condition"));
      }
      break;
    }
    default: {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY, "invalid placeholder table type"));
      break;
    }
  }
  nodesDestroyNode(*pTable);
  *pTable = (SNode*)newPlaceHolderTable;

  return code;
_return:
  nodesDestroyNode((SNode*)newPlaceHolderTable);
  parserError("translatePlaceHolderTable failed, code:%d, errmsg:%s", code, tstrerror(code));
  return code;
}

int32_t translateTable(STranslateContext* pCxt, SNode** pTable, bool inJoin) {
  SSelectStmt* pCurrSmt = (SSelectStmt*)(pCxt->pCurrStmt);
  int32_t      code = TSDB_CODE_SUCCESS;

  ((STableNode*)*pTable)->inJoin = inJoin;

  switch (nodeType(*pTable)) {
    case QUERY_NODE_REAL_TABLE: {
      PAR_ERR_JRET(translateRealTable(pCxt, pTable, inJoin));
      break;
    }
    case QUERY_NODE_TEMP_TABLE: {
      PAR_ERR_JRET(translateTempTable(pCxt, pTable, inJoin));
      break;
    }
    case QUERY_NODE_JOIN_TABLE: {
      PAR_ERR_JRET(translateJoinTable(pCxt, pTable, inJoin));
      break;
    }
    case QUERY_NODE_PLACE_HOLDER_TABLE: {
      PAR_ERR_JRET(translatePlaceHolderTable(pCxt, pTable, inJoin));
      break;
    }
    default:
      break;
  }
_return:
  return code;
}

static int32_t createAllColumns(STranslateContext* pCxt, bool igTags, SNodeList** pCols) {
  int32_t code = nodesMakeList(pCols);
  if (NULL == *pCols) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, code);
  }
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t  nums = taosArrayGetSize(pTables);
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    code = createColumnsByTable(pCxt, pTable, igTags, *pCols, nums > 1);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t createMultiResFunc(SFunctionNode* pSrcFunc, SExprNode* pExpr, SNode** ppNodeOut) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  code = nodesMakeList(&pFunc->pParameterList);
  SNode* pClonedNode = NULL;
  if (NULL == pFunc->pParameterList || TSDB_CODE_SUCCESS != (code = nodesCloneNode((SNode*)pExpr, &pClonedNode)) ||
      TSDB_CODE_SUCCESS != (code = nodesListStrictAppend(pFunc->pParameterList, pClonedNode))) {
    nodesDestroyNode((SNode*)pFunc);
    return code;
  }

  pFunc->node.resType = pExpr->resType;
  pFunc->funcId = pSrcFunc->funcId;
  pFunc->funcType = pSrcFunc->funcType;
  tstrncpy(pFunc->functionName, pSrcFunc->functionName, TSDB_FUNC_NAME_LEN);
  char    buf[TSDB_FUNC_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN + TSDB_NAME_DELIMITER_LEN + 3] = {0};
  int32_t len = 0;
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    SColumnNode* pCol = (SColumnNode*)pExpr;
    if (tsKeepColumnName) {
      tstrncpy(pFunc->node.userAlias, pCol->colName, TSDB_COL_NAME_LEN);
      tstrncpy(pFunc->node.aliasName, pCol->colName, TSDB_COL_NAME_LEN);
    } else {
      len = tsnprintf(buf, sizeof(buf) - 1, "%s(%s.%s)", pSrcFunc->functionName, pCol->tableAlias, pCol->colName);
      (void)taosHashBinary(buf, len);
      tstrncpy(pFunc->node.aliasName, buf, TSDB_COL_NAME_LEN);
      len = tsnprintf(buf, sizeof(buf) - 1, "%s(%s)", pSrcFunc->functionName, pCol->colName);
      // note: userAlias could be truncated here
      tstrncpy(pFunc->node.userAlias, buf, TSDB_COL_NAME_LEN);
    }
  } else {
    len = tsnprintf(buf, sizeof(buf) - 1, "%s(%s)", pSrcFunc->functionName, pExpr->aliasName);
    (void)taosHashBinary(buf, len);
    tstrncpy(pFunc->node.aliasName, buf, TSDB_COL_NAME_LEN);
    len = tsnprintf(buf, sizeof(buf) - 1, "%s(%s)", pSrcFunc->functionName, pExpr->userAlias);
    // note: userAlias could be truncated here
    tstrncpy(pFunc->node.userAlias, buf, TSDB_COL_NAME_LEN);
  }
  *ppNodeOut = (SNode*)pFunc;
  return code;
}

static int32_t createTableAllCols(STranslateContext* pCxt, SColumnNode* pCol, bool igTags, SNodeList** pOutput) {
  STableNode* pTable = NULL;
  int32_t     code = findTable(pCxt, pCol->tableAlias, &pTable);
  if (TSDB_CODE_SUCCESS == code && NULL == *pOutput) {
    code = nodesMakeList(pOutput);
    if (NULL == *pOutput) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, code);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnsByTable(pCxt, pTable, igTags, *pOutput, false);
  }
  return code;
}

static int32_t createMultiResFuncsParas(STranslateContext* pCxt, SNodeList* pSrcParas, SNodeList** pOutput) {
  int32_t code = TSDB_CODE_SUCCESS;

  SNodeList* pExprs = NULL;
  SNode*     pPara = NULL;
  FOREACH(pPara, pSrcParas) {
    if (nodesIsStar(pPara)) {
      code = createAllColumns(pCxt, !tsMultiResultFunctionStarReturnTags, &pExprs);
    } else if (nodesIsTableStar(pPara)) {
      code = createTableAllCols(pCxt, (SColumnNode*)pPara, !tsMultiResultFunctionStarReturnTags, &pExprs);
    } else {
      SNode* pClonedNode = NULL;
      code = nodesCloneNode(pPara, &pClonedNode);
      if (TSDB_CODE_SUCCESS != code) break;
      code = nodesListMakeStrictAppend(&pExprs, pClonedNode);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pExprs;
  } else {
    nodesDestroyList(pExprs);
  }

  return code;
}

static int32_t createMultiResFuncs(SFunctionNode* pSrcFunc, SNodeList* pExprs, SNodeList** pOutput) {
  SNodeList* pFuncs = NULL;
  int32_t    code = nodesMakeList(&pFuncs);
  if (NULL == pFuncs) {
    return code;
  }

  SNode* pExpr = NULL;
  FOREACH(pExpr, pExprs) {
    SNode* pNode = NULL;
    code = createMultiResFunc(pSrcFunc, (SExprNode*)pExpr, &pNode);
    if (TSDB_CODE_SUCCESS != code) break;
    code = nodesListStrictAppend(pFuncs, pNode);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pFuncs;
  } else {
    nodesDestroyList(pFuncs);
  }

  return code;
}

static int32_t createMultiResFuncsFromStar(STranslateContext* pCxt, SFunctionNode* pSrcFunc, SNodeList** pOutput) {
  SNodeList* pExprs = NULL;
  int32_t    code = createMultiResFuncsParas(pCxt, pSrcFunc->pParameterList, &pExprs);
  if (TSDB_CODE_SUCCESS == code) {
    code = createMultiResFuncs(pSrcFunc, pExprs, pOutput);
  }

  nodesDestroyList(pExprs);
  return code;
}

static int32_t createTags(STranslateContext* pCxt, SNodeList** pOutput) {
  if (QUERY_NODE_REAL_TABLE != nodeType(((SSelectStmt*)pCxt->pCurrStmt)->pFromTable)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAGS_PC,
                                   "The _TAGS pseudo column can only be used for child table and super table queries");
  }

  SRealTableNode*   pTable = (SRealTableNode*)(((SSelectStmt*)pCxt->pCurrStmt)->pFromTable);
  const STableMeta* pMeta = pTable->pMeta;
  if (TSDB_SUPER_TABLE != pMeta->tableType && TSDB_CHILD_TABLE != pMeta->tableType) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAGS_PC,
                                "The _TAGS pseudo column can only be used for child table and super table queries");
  }

  SSchema* pTagsSchema = getTableTagSchema(pMeta);
  for (int32_t i = 0; i < pMeta->tableInfo.numOfTags; ++i) {
    SColumnNode* pCol = NULL;
    int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
    if (TSDB_CODE_SUCCESS != code) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_OUT_OF_MEMORY);
    }
    setColumnInfoBySchema(pTable, pTagsSchema + i, 1, pCol, NULL);
    if (TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(pOutput, (SNode*)pCol)) {
      NODES_DESTORY_LIST(*pOutput);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateStar(STranslateContext* pCxt, SSelectStmt* pSelect) {
  SNode* pNode = NULL;
  WHERE_EACH(pNode, pSelect->pProjectionList) {
    int32_t code = TSDB_CODE_SUCCESS;
    if (nodesIsStar(pNode)) {
      SNodeList* pCols = NULL;
      code = createAllColumns(pCxt, false, &pCols);
      if (TSDB_CODE_SUCCESS == code) {
        SNode* tmp = NULL;
        FOREACH(tmp, pCols) {
          ((SExprNode*)tmp)->bindExprID = ((SExprNode*)pNode)->bindExprID;
          ((SExprNode*)tmp)->relatedTo = ((SExprNode*)pNode)->relatedTo;
        }
        INSERT_LIST(pSelect->pProjectionList, pCols);
        ERASE_NODE(pSelect->pProjectionList);
        continue;
      }
    } else if (isMultiResFunc(pNode)) {
      SNodeList* pNodeList = NULL;
      if (FUNCTION_TYPE_TAGS == ((SFunctionNode*)pNode)->funcType) {
        code = createTags(pCxt, &pNodeList);
      } else {
        code = createMultiResFuncsFromStar(pCxt, (SFunctionNode*)pNode, &pNodeList);
      }
      if (TSDB_CODE_SUCCESS == code) {
        INSERT_LIST(pSelect->pProjectionList, pNodeList);
        ERASE_NODE(pSelect->pProjectionList);
        continue;
      }
    } else if (nodesIsTableStar(pNode)) {
      SNodeList* pCols = NULL;
      code = createTableAllCols(pCxt, (SColumnNode*)pNode, false, &pCols);
      if (TSDB_CODE_SUCCESS == code) {
        INSERT_LIST(pSelect->pProjectionList, pCols);
        ERASE_NODE(pSelect->pProjectionList);
        continue;
      }
    }
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    WHERE_NEXT;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getPositionValue(const SValueNode* pVal) {
  switch (pVal->node.resType.type) {
    case TSDB_DATA_TYPE_NULL:
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_GEOMETRY:
      return -1;
    case TSDB_DATA_TYPE_BOOL:
      return -1;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
      return pVal->datum.i;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      return -1;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      return pVal->datum.u;
    default:
      break;
  }
  return -1;
}

static int32_t translateClausePosition(STranslateContext* pCxt, SNodeList* pProjectionList, SNodeList* pClauseList,
                                       bool* pOther) {
  *pOther = false;
  SNode* pNode = NULL;
  WHERE_EACH(pNode, pClauseList) {
    SNode* pExpr = NULL;
    switch (pNode->type) {
      case QUERY_NODE_GROUPING_SET:
        pExpr = getGroupByNode(pNode);
        break;
      case QUERY_NODE_ORDER_BY_EXPR:
        pExpr = ((SOrderByExprNode*)pNode)->pExpr;
        break;
      default:
        pExpr = pNode;
        break;
    }
    if (QUERY_NODE_VALUE == nodeType(pExpr)) {
      SValueNode* pVal = (SValueNode*)pExpr;
      pVal->node.asPosition = false;
      if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
        return pCxt->errCode;
      }
      int32_t pos = getPositionValue(pVal);
      if (pos < 0) {
        pVal->node.asPosition = false;
      } else if (0 == pos || pos > LIST_LENGTH(pProjectionList)) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT);
      } else {
        pVal->node.asPosition = true;
      }
    } else {
      *pOther = true;
    }
    WHERE_NEXT;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t rewriteColsFunction(STranslateContext* pCxt, ESqlClause clause, SNodeList** nodeList,
                                   SNodeList** selectFuncList);
static int32_t rewriteHavingColsNode(STranslateContext* pCxt, SNode** pNode, SNodeList** selectFuncList);

static int32_t prepareColumnExpansion(STranslateContext* pCxt, ESqlClause clause, SSelectStmt* pSelect) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t len = LIST_LENGTH(pSelect->pProjectionBindList);
  if (clause == SQL_CLAUSE_SELECT) {
    code = rewriteColsFunction(pCxt, clause, &pSelect->pProjectionList, &pSelect->pProjectionBindList);
  } else if (clause == SQL_CLAUSE_HAVING) {
    code = rewriteHavingColsNode(pCxt, &pSelect->pHaving, &pSelect->pProjectionBindList);
  } else if (clause == SQL_CLAUSE_ORDER_BY) {
    code = rewriteColsFunction(pCxt, clause, &pSelect->pOrderByList, &pSelect->pProjectionBindList);
  } else {
    code =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, "Invalid clause for column expansion");
  }
  if (TSDB_CODE_SUCCESS == code && LIST_LENGTH(pSelect->pProjectionBindList) > len) {
    code = translateExprList(pCxt, pSelect->pProjectionBindList);
  }
  if (pSelect->pProjectionBindList != NULL) {
    pSelect->hasAggFuncs = true;
  }
  return code;
}

static int32_t translateOrderBy(STranslateContext* pCxt, SSelectStmt* pSelect) {
  bool    other;
  int32_t code = prepareColumnExpansion(pCxt, SQL_CLAUSE_ORDER_BY, pSelect);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  code = translateClausePosition(pCxt, pSelect->pProjectionList, pSelect->pOrderByList, &other);
  if (TSDB_CODE_SUCCESS == code) {
    if (0 == LIST_LENGTH(pSelect->pOrderByList)) {
      NODES_DESTORY_LIST(pSelect->pOrderByList);
      return TSDB_CODE_SUCCESS;
    }
    if (!other) {
      return TSDB_CODE_SUCCESS;
    }
    pCxt->currClause = SQL_CLAUSE_ORDER_BY;
    code = translateExprList(pCxt, pSelect->pOrderByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkExprListForGroupBy(pCxt, pSelect, pSelect->pOrderByList);
  }
  return code;
}

static EDealRes needFillImpl(SNode* pNode, void* pContext) {
  if ((isAggFunc(pNode) || isInterpFunc(pNode)) && FUNCTION_TYPE_GROUP_KEY != ((SFunctionNode*)pNode)->funcType &&
      FUNCTION_TYPE_GROUP_CONST_VALUE != ((SFunctionNode*)pNode)->funcType) {
    *(bool*)pContext = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static bool needFill(SNode* pNode) {
  bool hasFillFunc = false;
  nodesWalkExpr(pNode, needFillImpl, &hasFillFunc);
  return hasFillFunc;
}

static int32_t convertFillValue(STranslateContext* pCxt, SDataType dt, SNodeList* pValues, int32_t index) {
  SListCell* pCell = nodesListGetCell(pValues, index);
  if (dataTypeEqual(&dt, &((SExprNode*)pCell->pNode)->resType) && (QUERY_NODE_VALUE == nodeType(pCell->pNode))) {
    return TSDB_CODE_SUCCESS;
  }
  SNode*  pCastFunc = NULL;
  int32_t code = createCastFunc(pCxt, pCell->pNode, dt, &pCastFunc);
  if (TSDB_CODE_SUCCESS == code) {
    code = scalarCalculateConstants(pCastFunc, &pCell->pNode);
  }
  if (TSDB_CODE_SUCCESS == code && QUERY_NODE_VALUE != nodeType(pCell->pNode)) {
    code =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, "Fill value can only accept constant");
  } else if (TSDB_CODE_SUCCESS != code) {
    code = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, "Filled data type mismatch");
  }
  return code;
}

static int32_t doCheckFillValues(STranslateContext* pCxt, SFillNode* pFill, SNodeList* pProjectionList) {
  int32_t        fillNo = 0;
  SNodeListNode* pFillValues = (SNodeListNode*)pFill->pValues;
  SNode*         pProject = NULL;
  if (!pFillValues)
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, "Filled values number mismatch");
  FOREACH(pProject, pProjectionList) {
    if (needFill(pProject)) {
      if (fillNo >= LIST_LENGTH(pFillValues->pNodeList)) {
        return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, "Filled values number mismatch");
      }
      int32_t code = convertFillValue(pCxt, ((SExprNode*)pProject)->resType, pFillValues->pNodeList, fillNo);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }

      ++fillNo;
    }
  }
  if (fillNo != LIST_LENGTH(pFillValues->pNodeList)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, "Filled values number mismatch");
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkFillValues(STranslateContext* pCxt, SFillNode* pFill, SNodeList* pProjectionList) {
  if (FILL_MODE_VALUE != pFill->mode && FILL_MODE_VALUE_F != pFill->mode) {
    return TSDB_CODE_SUCCESS;
  }
  return doCheckFillValues(pCxt, pFill, pProjectionList);
  return TSDB_CODE_SUCCESS;
}

static int32_t translateFillValues(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pWindow || QUERY_NODE_INTERVAL_WINDOW != nodeType(pSelect->pWindow) ||
      NULL == ((SIntervalWindowNode*)pSelect->pWindow)->pFill) {
    return TSDB_CODE_SUCCESS;
  }
  return checkFillValues(pCxt, (SFillNode*)((SIntervalWindowNode*)pSelect->pWindow)->pFill, pSelect->pProjectionList);
}

static int32_t rewriteProjectAlias(SNodeList* pProjectionList) {
  int32_t no = 1;
  SNode*  pProject = NULL;
  FOREACH(pProject, pProjectionList) {
    SExprNode* pExpr = (SExprNode*)pProject;
    if ('\0' == pExpr->userAlias[0]) {
      tstrncpy(pExpr->userAlias, pExpr->aliasName, TSDB_COL_NAME_LEN);
    }
    rewriteExprAliasName(pExpr, no++);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkProjectAlias(STranslateContext* pCxt, SNodeList* pProjectionList, SHashObj** pOutput) {
  SHashObj* pUserAliasSet = taosHashInit(LIST_LENGTH(pProjectionList),
                                         taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (!pUserAliasSet) return terrno;
  SNode*  pProject = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  FOREACH(pProject, pProjectionList) {
    SExprNode* pExpr = (SExprNode*)pProject;
    if (NULL != taosHashGet(pUserAliasSet, pExpr->userAlias, strlen(pExpr->userAlias))) {
      taosHashCleanup(pUserAliasSet);
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_AMBIGUOUS_COLUMN, pExpr->userAlias);
    }
    code = taosHashPut(pUserAliasSet, pExpr->userAlias, strlen(pExpr->userAlias), &pExpr, POINTER_BYTES);
    if (TSDB_CODE_SUCCESS != code) break;
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pOutput) {
    *pOutput = pUserAliasSet;
  } else {
    taosHashCleanup(pUserAliasSet);
  }
  return code;
}

static int32_t translateProjectionList(STranslateContext* pCxt, SSelectStmt* pSelect) {
  SNode*  pNode;
  int32_t projIdx = 1;
  FOREACH(pNode, pSelect->pProjectionList) { ((SExprNode*)pNode)->projIdx = projIdx++; }

  if (!pSelect->isSubquery) {
    return rewriteProjectAlias(pSelect->pProjectionList);
  }
  return TSDB_CODE_SUCCESS;
}

typedef struct SReplaceGroupByAliasCxt {
  STranslateContext* pTranslateCxt;
  SNodeList*         pProjectionList;
} SReplaceGroupByAliasCxt;

static EDealRes translateGroupPartitionByImpl(SNode** pNode, void* pContext) {
  SReplaceGroupByAliasCxt* pCxt = pContext;
  SNodeList*               pProjectionList = pCxt->pProjectionList;
  SNode*                   pProject = NULL;
  int32_t                  code = TSDB_CODE_SUCCESS;
  STranslateContext*       pTransCxt = pCxt->pTranslateCxt;
  if (QUERY_NODE_VALUE == nodeType(*pNode)) {
    SValueNode* pVal = (SValueNode*)*pNode;
    if (DEAL_RES_ERROR == translateValue(pTransCxt, pVal)) {
      return DEAL_RES_CONTINUE;
    }
    if (!pVal->node.asPosition) {
      return DEAL_RES_CONTINUE;
    }
    int32_t pos = getPositionValue(pVal);
    if (0 < pos && pos <= LIST_LENGTH(pProjectionList)) {
      SNode* pNew = NULL;
      code = nodesCloneNode(nodesListGetNode(pProjectionList, pos - 1), (SNode**)&pNew);
      if (TSDB_CODE_SUCCESS != code) {
        pCxt->pTranslateCxt->errCode = code;
        return DEAL_RES_ERROR;
      }
      nodesDestroyNode(*pNode);
      *pNode = pNew;
    }
    code = translateExpr(pTransCxt, pNode);
    if (TSDB_CODE_SUCCESS != code) {
      pTransCxt->errCode = code;
      return DEAL_RES_ERROR;
    }
    return DEAL_RES_CONTINUE;
  } else if (QUERY_NODE_COLUMN == nodeType(*pNode)) {
    bool     asAlias = false;
    EDealRes res = translateColumnInGroupByClause(pTransCxt, (SColumnNode**)pNode, &asAlias);
    if (DEAL_RES_ERROR == res) {
      return DEAL_RES_ERROR;
    }
    pTransCxt->errCode = TSDB_CODE_SUCCESS;
    if (nodeType(*pNode) == QUERY_NODE_COLUMN && !asAlias) {
      return DEAL_RES_CONTINUE;
    }
    code = translateExpr(pTransCxt, pNode);
    if (TSDB_CODE_SUCCESS != code) {
      pTransCxt->errCode = code;
      return DEAL_RES_ERROR;
    }
    return DEAL_RES_CONTINUE;
  }
  return doTranslateExpr(pNode, pTransCxt);
}

static int32_t translateGroupByList(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pGroupByList) {
    return TSDB_CODE_SUCCESS;
  }
  SReplaceGroupByAliasCxt cxt = {.pTranslateCxt = pCxt, .pProjectionList = pSelect->pProjectionList};
  nodesRewriteExprsPostOrder(pSelect->pGroupByList, translateGroupPartitionByImpl, &cxt);

  return pCxt->errCode;
}

static int32_t translatePartitionByList(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pPartitionByList) {
    return TSDB_CODE_SUCCESS;
  }

  SReplaceGroupByAliasCxt cxt = {.pTranslateCxt = pCxt, .pProjectionList = pSelect->pProjectionList};
  nodesRewriteExprsPostOrder(pSelect->pPartitionByList, translateGroupPartitionByImpl, &cxt);

  return pCxt->errCode;
}

static int32_t translateSelectList(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->currClause = SQL_CLAUSE_SELECT;
  int32_t code = prepareColumnExpansion(pCxt, SQL_CLAUSE_SELECT, pSelect);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExprList(pCxt, pSelect->pProjectionList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateStar(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateProjectionList(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkExprListForGroupBy(pCxt, pSelect, pSelect->pProjectionList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateFillValues(pCxt, pSelect);
  }
  if (NULL == pSelect->pProjectionList || 0 >= pSelect->pProjectionList->length) {
    if (pCxt->pParseCxt->hasInvisibleCol) {
      code = TSDB_CODE_PAR_PERMISSION_DENIED;
    } else {
      code = TSDB_CODE_PAR_INVALID_SELECTED_EXPR;
    }
  }
  return code;
}

static int32_t translateHaving(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == pSelect->pGroupByList && NULL == pSelect->pPartitionByList && NULL == pSelect->pWindow &&
      !isWindowJoinStmt(pSelect) && NULL != pSelect->pHaving) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);
  }
  pCxt->currClause = SQL_CLAUSE_HAVING;
  if (NULL != pSelect->pHaving) {
    code = prepareColumnExpansion(pCxt, SQL_CLAUSE_HAVING, pSelect);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  if (isWindowJoinStmt(pSelect)) {
    if (NULL != pSelect->pHaving) {
      bool hasFunc = false;
      nodesWalkExpr(pSelect->pHaving, searchAggFuncNode, &hasFunc);
      if (!hasFunc) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_WJOIN_HAVING_EXPR);
      }
    }
  }
  return translateExpr(pCxt, &pSelect->pHaving);
}

static int32_t translateGroupBy(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pGroupByList) {
    return TSDB_CODE_SUCCESS;
  }
  if (NULL != pSelect->pWindow) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GROUPBY_WINDOW_COEXIST);
  }
  bool other;
  pCxt->currClause = SQL_CLAUSE_GROUP_BY;
  int32_t code = translateClausePosition(pCxt, pSelect->pProjectionList, pSelect->pGroupByList, &other);
  if (TSDB_CODE_SUCCESS == code) {
    if (0 == LIST_LENGTH(pSelect->pGroupByList)) {
      NODES_DESTORY_LIST(pSelect->pGroupByList);
      return TSDB_CODE_SUCCESS;
    }
    pSelect->timeLineResMode = TIME_LINE_NONE;
    code = translateGroupByList(pCxt, pSelect);
  }
  return code;
}

static int32_t getTimeRange(SNode** pPrimaryKeyCond, STimeWindow* pTimeRange, bool* pIsStrict) {
  SNode*  pNew = NULL;
  int32_t code = scalarCalculateConstants(*pPrimaryKeyCond, &pNew);
  if (TSDB_CODE_SUCCESS == code) {
    *pPrimaryKeyCond = pNew;
    if (nodeType(pNew) == QUERY_NODE_VALUE) {
      TAOS_SET_OBJ_ALIGNED(pTimeRange, TSWINDOW_INITIALIZER);
    } else {
      code = filterGetTimeRange(*pPrimaryKeyCond, pTimeRange, pIsStrict);
    }
  }
  return code;
}
typedef struct SConditionOnlyPhAndConstContext {
  bool onlyPhAndConst;
  bool hasPhOrConst;
  bool onlyConst;
} SConditionOnlyPhAndConstContext;

static EDealRes conditionOnlyPhAndConstImpl(SNode* pNode, void* pContext) {
  SConditionOnlyPhAndConstContext* pCxt = (SConditionOnlyPhAndConstContext*)pContext;
  if (nodeType(pNode) == QUERY_NODE_VALUE) {
    pCxt->onlyPhAndConst &= true;
    pCxt->hasPhOrConst = true;
    pCxt->onlyConst &= true;
  } else if (nodeType(pNode) == QUERY_NODE_FUNCTION) {
    SFunctionNode *pFunc = (SFunctionNode*)pNode;
    if (fmIsPlaceHolderFunc(pFunc->funcId)) {
      pCxt->onlyPhAndConst &= true;
      pCxt->hasPhOrConst = true;
      pCxt->onlyConst = false;
    } else if (pFunc->funcType == FUNCTION_TYPE_NOW || pFunc->funcType == FUNCTION_TYPE_TODAY || fmIsScalarFunc(pFunc->funcId)) {
      pCxt->onlyPhAndConst &= true;
      pCxt->hasPhOrConst = true;
      pCxt->onlyConst &= true;
    } else {
      pCxt->onlyPhAndConst = false;
      pCxt->onlyConst = false;
    }
  }
  return DEAL_RES_CONTINUE;
}

typedef struct SFilterExtractTsContext {
  SNodeList* pStart;
  SNodeList* pEnd;
  bool       onlyTsConst;
} SFilterExtractTsContext;

static bool filterExtractTsNeedCollect(SNode* pLeft, SNode* pRight, bool* pOnlyTsConst) {
  if (nodeType(pLeft) == QUERY_NODE_COLUMN) {
    SColumnNode* pCol = (SColumnNode*)pLeft;
    if (pCol->colType != COLUMN_TYPE_COLUMN || pCol->colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
      return false;
    }

    SConditionOnlyPhAndConstContext cxt = {true, false, true};
    nodesWalkExpr(pRight, conditionOnlyPhAndConstImpl, &cxt);
    *pOnlyTsConst &= cxt.onlyConst;
    if (cxt.onlyPhAndConst && cxt.hasPhOrConst && (((SExprNode*)pRight)->resType.type == TSDB_DATA_TYPE_TIMESTAMP)) {
      return true;
    } else {
      return false;
    }
  }
  return false;
}

EDealRes filterExtractTsCondImpl(SNode** pNode, void* pContext) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  SFilterExtractTsContext *pCxt = (SFilterExtractTsContext*)pContext;
  switch(nodeType(*pNode)) {
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOperator = (SOperatorNode*)*pNode;
      PAR_ERR_JRET(scalarConvertOpValueNodeTs(pOperator));
      if (pOperator->opType == OP_TYPE_LOWER_EQUAL ||
          pOperator->opType == OP_TYPE_LOWER_THAN ||
          pOperator->opType == OP_TYPE_GREATER_EQUAL ||
          pOperator->opType == OP_TYPE_GREATER_THAN) {
        if (filterExtractTsNeedCollect(pOperator->pLeft, pOperator->pRight, &pCxt->onlyTsConst)) {
          SValueNode *pVal = NULL;
          if (pOperator->opType == OP_TYPE_LOWER_EQUAL || pOperator->opType == OP_TYPE_LOWER_THAN) {
            PAR_ERR_JRET(nodesListMakeAppend(&pCxt->pEnd, *pNode));
          } else {
            PAR_ERR_JRET(nodesListMakeAppend(&pCxt->pStart, *pNode));
          }
          PAR_ERR_JRET(nodesMakeValueNodeFromBool(true, &pVal));
          *pNode = (SNode*)pVal;
          return DEAL_RES_IGNORE_CHILD;
        } else if (filterExtractTsNeedCollect(pOperator->pRight, pOperator->pLeft, &pCxt->onlyTsConst)) {
          SValueNode *pVal = NULL;
          if (pOperator->opType == OP_TYPE_LOWER_EQUAL || pOperator->opType == OP_TYPE_LOWER_THAN) {
            TSWAP(pOperator->pLeft, pOperator->pRight);
            pOperator->opType = (pOperator->opType == OP_TYPE_LOWER_EQUAL ? OP_TYPE_GREATER_EQUAL : OP_TYPE_GREATER_THAN);
            PAR_ERR_JRET(nodesListMakeAppend(&pCxt->pStart, *pNode));
          } else {
            TSWAP(pOperator->pLeft, pOperator->pRight);
            pOperator->opType = (pOperator->opType == OP_TYPE_GREATER_EQUAL ? OP_TYPE_LOWER_EQUAL : OP_TYPE_LOWER_THAN);
            PAR_ERR_JRET(nodesListMakeAppend(&pCxt->pEnd, *pNode));
          }
          PAR_ERR_JRET(nodesMakeValueNodeFromBool(true, &pVal));
          *pNode = (SNode*)pVal;
          return DEAL_RES_IGNORE_CHILD;
        } else {
          return DEAL_RES_CONTINUE;
        }
      } else if (pOperator->opType == OP_TYPE_EQUAL) {
        if (filterExtractTsNeedCollect(pOperator->pLeft, pOperator->pRight, &pCxt->onlyTsConst)) {
          SNode*      startNode = NULL;
          SNode*      endNode = NULL;
          SValueNode* pVal = NULL;
          PAR_ERR_JRET(nodesCloneNode(*pNode, &startNode));
          PAR_ERR_JRET(nodesCloneNode(*pNode, &endNode));
          ((SOperatorNode*)startNode)->opType = OP_TYPE_GREATER_EQUAL;
          ((SOperatorNode*)endNode)->opType = OP_TYPE_LOWER_EQUAL;
          PAR_ERR_JRET(nodesListMakeAppend(&pCxt->pStart, startNode));
          PAR_ERR_JRET(nodesListMakeAppend(&pCxt->pEnd, endNode));
          PAR_ERR_JRET(nodesMakeValueNodeFromBool(true, &pVal));
          *pNode = (SNode*)pVal;
          return DEAL_RES_IGNORE_CHILD;
        } else if (filterExtractTsNeedCollect(pOperator->pRight, pOperator->pLeft, &pCxt->onlyTsConst)) {
          SNode*      startNode = NULL;
          SNode*      endNode = NULL;
          SValueNode* pVal = NULL;
          PAR_ERR_JRET(nodesCloneNode(*pNode, &startNode));
          PAR_ERR_JRET(nodesCloneNode(*pNode, &endNode));
          ((SOperatorNode*)startNode)->opType = OP_TYPE_GREATER_EQUAL;
          ((SOperatorNode*)endNode)->opType = OP_TYPE_LOWER_EQUAL;
          TSWAP(((SOperatorNode*)startNode)->pLeft, ((SOperatorNode*)startNode)->pRight);
          TSWAP(((SOperatorNode*)endNode)->pLeft, ((SOperatorNode*)endNode)->pRight);
          PAR_ERR_JRET(nodesListMakeAppend(&pCxt->pStart, startNode));
          PAR_ERR_JRET(nodesListMakeAppend(&pCxt->pEnd, endNode));
          PAR_ERR_JRET(nodesMakeValueNodeFromBool(true, &pVal));
          *pNode = (SNode*)pVal;
          return DEAL_RES_IGNORE_CHILD;
        } else {
          return DEAL_RES_CONTINUE;
        }
      } else {
        return DEAL_RES_CONTINUE;
      }
    }
    case QUERY_NODE_LOGIC_CONDITION: {
      SLogicConditionNode* pCond = (SLogicConditionNode*)*pNode;
      switch (pCond->condType) {
        case LOGIC_COND_TYPE_OR:
        case LOGIC_COND_TYPE_NOT:
          return DEAL_RES_IGNORE_CHILD;
        default:
          return DEAL_RES_CONTINUE;
      }
    }
    default:
      return DEAL_RES_CONTINUE;
  }
_return:
  return DEAL_RES_ERROR;
}

static bool tsRangeSameToWindowRange(SNode* pCond, bool start, bool equal) {
  if (nodeType(pCond) != QUERY_NODE_LOGIC_CONDITION && nodeType(pCond) != QUERY_NODE_OPERATOR) {
    return false;
  }

  SOperatorNode *pOperator = NULL;

  if (nodeType(pCond) == QUERY_NODE_LOGIC_CONDITION) {
    SLogicConditionNode *pLogicCond = (SLogicConditionNode*)pCond;
    if (pLogicCond->condType != LOGIC_COND_TYPE_AND || LIST_LENGTH(pLogicCond->pParameterList) != 1) {
      return false;
    }

    SNode *pOp = nodesListGetNode(pLogicCond->pParameterList, 0);
    if (!pOp || nodeType(pOp) != QUERY_NODE_OPERATOR) {
      return false;
    }

    pOperator = (SOperatorNode*)pOp;
  } else {
    pOperator = (SOperatorNode*)pCond;
  }


  if(!pOperator->pRight || nodeType(pOperator->pRight) != QUERY_NODE_FUNCTION) {
    return false;
  }

  SFunctionNode *pFunc = (SFunctionNode*)pOperator->pRight;

  if (start) {
    if (pFunc->funcType != FUNCTION_TYPE_TWSTART) {
      return false;
    }
    return equal ? (pOperator->opType == OP_TYPE_GREATER_EQUAL) : (pOperator->opType == OP_TYPE_GREATER_THAN);
  } else {
    if (pFunc->funcType != FUNCTION_TYPE_TWEND) {
      return false;
    }
    return equal ? (pOperator->opType == OP_TYPE_LOWER_EQUAL) : (pOperator->opType == OP_TYPE_LOWER_THAN);
  }
}

static int32_t filterExtractTsCond(SNode** pCond, SNode** pTimeRangeExpr, bool leftEq, bool rightEq, bool *onlyTsConst) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  SNode*                  pNew = NULL;
  SFilterExtractTsContext pCxt = {.onlyTsConst = true};

  nodesRewriteExpr(pCond, filterExtractTsCondImpl, &pCxt);
  *onlyTsConst = pCxt.onlyTsConst;
  if (!pCxt.pStart && !pCxt.pEnd) {
    return TSDB_CODE_SUCCESS;
  }
  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_TIME_RANGE, pTimeRangeExpr));
  STimeRangeNode* pRange = (STimeRangeNode*)*pTimeRangeExpr;
  bool leftSame = false;
  bool rightSame = false;
  if (pCxt.pStart) {
    PAR_ERR_JRET(nodesMergeConds(&pRange->pStart, &pCxt.pStart));
    leftSame = tsRangeSameToWindowRange(pRange->pStart, true, leftEq);
  }
  if (pCxt.pEnd) {
    PAR_ERR_JRET(nodesMergeConds(&pRange->pEnd, &pCxt.pEnd));
    rightSame = tsRangeSameToWindowRange(pRange->pEnd, false, rightEq);
  }
  if (leftSame && rightSame) {
    pRange->needCalc = false;
  } else {
    pRange->needCalc = true;
  }
  return code;
_return:
  nodesDestroyNode(*pTimeRangeExpr);
  return code;
}

static int32_t getQueryTimeRange(STranslateContext* pCxt, SNode** pWhere, STimeWindow* pTimeRange,
                                 SNode** pTimeRangeExpr, SNode* pFromTable) {
  if (NULL == *pWhere) {
    TAOS_SET_OBJ_ALIGNED(pTimeRange, TSWINDOW_INITIALIZER);
    return TSDB_CODE_SUCCESS;
  }

  SNode*  pCond = NULL;
  SNode*  pPrimaryKeyCond = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  PAR_ERR_JRET(nodesCloneNode(*pWhere, &pCond));

  bool extractJoinCond = true;
  if (nodeType(pFromTable) == QUERY_NODE_JOIN_TABLE) {
    SJoinTableNode *pJoinTable = (SJoinTableNode *)pFromTable;
    if (pJoinTable->subType == JOIN_STYPE_ASOF) {
      extractJoinCond = false;
    }
  }
  bool onlyTsConst = false;
  if (inStreamCalcClause(pCxt) && nodeType(pFromTable) != QUERY_NODE_TEMP_TABLE && extractJoinCond) {
    PAR_ERR_JRET(filterExtractTsCond(&pCond, pTimeRangeExpr, pCxt->streamInfo.extLeftEq, pCxt->streamInfo.extRightEq, &onlyTsConst));
    // some node may be replaced
    if (onlyTsConst) {
      nodesDestroyNode(pCond);
      PAR_ERR_JRET(nodesCloneNode(*pWhere, &pCond));
      nodesDestroyNode(*pTimeRangeExpr);
      *pTimeRangeExpr = NULL;
      // we can extract time range to pTimerange but not time range expr
    } else {
      TSWAP(*pWhere, pCond);
      goto _return;
    }
  }

  PAR_ERR_JRET(filterPartitionCond(&pCond, &pPrimaryKeyCond, NULL, NULL, NULL));

  if (NULL != pPrimaryKeyCond) {
    bool isStrict = false;
    PAR_ERR_JRET(getTimeRange(&pPrimaryKeyCond, pTimeRange, &isStrict));
  } else {
    TAOS_SET_OBJ_ALIGNED(pTimeRange, TSWINDOW_INITIALIZER);
  }

_return:
  nodesDestroyNode(pCond);
  nodesDestroyNode(pPrimaryKeyCond);
  return code;
}

static int32_t checkFill(STranslateContext* pCxt, SFillNode* pFill, SValueNode* pInterval, bool isInterpFill, uint8_t precision) {
  if (FILL_MODE_NONE == pFill->mode) {
    if (isInterpFill) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, "Unsupported fill type");
    }

    return TSDB_CODE_SUCCESS;
  }

  if (TSWINDOW_IS_EQUAL(pFill->timeRange, TSWINDOW_INITIALIZER) && !pFill->pTimeRange) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE);
  }

  if (pFill->pTimeRange &&
      (!((STimeRangeNode*)pFill->pTimeRange)->pStart ||
       !((STimeRangeNode*)pFill->pTimeRange)->pEnd)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE, "Start and End time of query range required ");
  }

  if (TSWINDOW_IS_EQUAL(pFill->timeRange, TSWINDOW_DESC_INITIALIZER)) {
    return TSDB_CODE_SUCCESS;
  }

  // interp FILL clause
  if (NULL == pInterval) {
    return TSDB_CODE_SUCCESS;
  }
  int64_t timeRange = 0;
  int64_t intervalRange = 0;
  int64_t res = int64SafeSub(pFill->timeRange.skey, pFill->timeRange.ekey);
  timeRange = res < 0 ? res == INT64_MIN ? INT64_MAX : -res : res;
  if (IS_CALENDAR_TIME_DURATION(pInterval->unit)) {
    int64_t f = 1;
    int64_t tickPerDay = convertTimePrecision(MILLISECOND_PER_DAY, TSDB_TIME_PRECISION_MILLI, precision);
    if (pInterval->unit == 'n') {
      f = 30LL * tickPerDay;
    } else if (pInterval->unit == 'y') {
      f = 365LL * tickPerDay;
    }
    intervalRange = pInterval->datum.i * f;
  } else {
    intervalRange = pInterval->datum.i;
  }
  if ((timeRange / intervalRange) >= MAX_INTERVAL_TIME_WINDOW && !inStreamCalcClause(pCxt)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateFill(STranslateContext* pCxt, SSelectStmt* pSelect, SIntervalWindowNode* pInterval) {
  if (NULL == pInterval->pFill) {
    return TSDB_CODE_SUCCESS;
  }

  ((SFillNode*)pInterval->pFill)->timeRange = pSelect->timeRange;
  PAR_ERR_RET(nodesCloneNode(pSelect->pTimeRange, &((SFillNode*)pInterval->pFill)->pTimeRange));
  return checkFill(pCxt, (SFillNode*)pInterval->pFill, (SValueNode*)pInterval->pInterval, false, pSelect->precision);
}

static int32_t getMonthsFromTimeVal(int64_t val, int32_t fromPrecision, char unit, double* pMonth) {
  int64_t days = -1;
  int32_t code = convertTimeFromPrecisionToUnit(val, fromPrecision, 'd', &days);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  switch (unit) {
    case 'b':
    case 'u':
    case 'a':
    case 's':
    case 'm':
    case 'h':
    case 'd':
    case 'w':
      *pMonth = days / 28.0;
      return code;
    case 'n':
      *pMonth = val;
      return code;
    case 'y':
      *pMonth = val * 12;
      return code;
    default:
      code = TSDB_CODE_INVALID_PARA;
      break;
  }
  return code;
}

static const char* getPrecisionStr(uint8_t precision) {
  switch (precision) {
    case TSDB_TIME_PRECISION_MILLI:
      return TSDB_TIME_PRECISION_MILLI_STR;
    case TSDB_TIME_PRECISION_MICRO:
      return TSDB_TIME_PRECISION_MICRO_STR;
    case TSDB_TIME_PRECISION_NANO:
      return TSDB_TIME_PRECISION_NANO_STR;
    default:
      break;
  }
  return "unknown";
}

static int64_t getPrecisionMultiple(uint8_t precision) {
  switch (precision) {
    case TSDB_TIME_PRECISION_MILLI:
      return 1;
    case TSDB_TIME_PRECISION_MICRO:
      return 1000;
    case TSDB_TIME_PRECISION_NANO:
      return 1000000;
    default:
      break;
  }
  return 1;
}

static void convertVarDuration(SValueNode* pOffset, uint8_t precision) {
  const int64_t factors[3] = {NANOSECOND_PER_MSEC, NANOSECOND_PER_USEC, 1};
  const int8_t  units[3] = {TIME_UNIT_MILLISECOND, TIME_UNIT_MICROSECOND, TIME_UNIT_NANOSECOND};

  if (pOffset->unit == 'n') {
    pOffset->datum.i = pOffset->datum.i * 31 * (NANOSECOND_PER_DAY / factors[precision]);
  } else {
    pOffset->datum.i = pOffset->datum.i * 365 * (NANOSECOND_PER_DAY / factors[precision]);
  }

  pOffset->unit = units[precision];
}

static const int64_t tsdbMaxKeepMS = (int64_t)60 * 1000 * TSDB_MAX_KEEP;

static int32_t checkIntervalWindow(STranslateContext* pCxt, SIntervalWindowNode* pInterval) {
  uint8_t precision = ((SColumnNode*)pInterval->pCol)->node.resType.precision;

  SValueNode* pInter = (SValueNode*)pInterval->pInterval;
  bool        valInter = IS_CALENDAR_TIME_DURATION(pInter->unit);
  if (pInter->datum.i <= 0 || (!valInter && pInter->datum.i < tsMinIntervalTime)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_VALUE_TOO_SMALL, tsMinIntervalTime,
                                getPrecisionStr(precision));
  } else if (pInter->datum.i / getPrecisionMultiple(precision) > tsdbMaxKeepMS) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_VALUE_TOO_BIG, 1000, "years");
  }

  if (NULL != pInterval->pOffset) {
    SValueNode* pOffset = (SValueNode*)pInterval->pOffset;
    if (pOffset->datum.i == AUTO_DURATION_VALUE) {
      if (pOffset->unit != getPrecisionUnit(precision)) {
        parserError("invalid offset unit %d for auto offset with precision %u", pOffset->unit, precision);
        return TSDB_CODE_INVALID_PARA;
      }
    } else if (pOffset->datum.i < 0) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_NEGATIVE);
    }
    if (pInter->unit == 'n' && pOffset->unit == 'y') {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_UNIT);
    }
    bool fixed = !IS_CALENDAR_TIME_DURATION(pOffset->unit) && !valInter;
    if (fixed && pOffset->datum.i >= pInter->datum.i) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG);
    }
    if (!fixed) {
      double  offsetMonth = 0, intervalMonth = 0;
      int32_t code = getMonthsFromTimeVal(pOffset->datum.i, precision, pOffset->unit, &offsetMonth);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      code = getMonthsFromTimeVal(pInter->datum.i, precision, pInter->unit, &intervalMonth);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (offsetMonth > intervalMonth) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG);
      }
    }

    if (pOffset->unit == 'n' || pOffset->unit == 'y') {
      convertVarDuration(pOffset, precision);
    }
  }

  if (NULL != pInterval->pSliding) {
    const static int32_t INTERVAL_SLIDING_FACTOR = 100;

    SValueNode* pSliding = (SValueNode*)pInterval->pSliding;
    if (IS_CALENDAR_TIME_DURATION(pSliding->unit)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SLIDING_UNIT);
    }
    if ((pSliding->datum.i <
         convertTimePrecision(tsMinSlidingTime, TSDB_TIME_PRECISION_MILLI, pSliding->node.resType.precision)) ||
        (pInter->datum.i / pSliding->datum.i > INTERVAL_SLIDING_FACTOR)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SLIDING_TOO_SMALL);
    }
    if (valInter) {
      double  slidingMonth = 0, intervalMonth = 0;
      int32_t code = getMonthsFromTimeVal(pSliding->datum.i, precision, pSliding->unit, &slidingMonth);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      code = getMonthsFromTimeVal(pInter->datum.i, precision, pInter->unit, &intervalMonth);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (slidingMonth > intervalMonth) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SLIDING_TOO_BIG);
      }
    }
    if (!valInter && pSliding->datum.i > pInter->datum.i) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SLIDING_TOO_BIG);
    }
  }

  return TSDB_CODE_SUCCESS;
}

// check left time is greater than right time
static int32_t checkTimeGreater(SValueNode* pLeftTime, SValueNode* pRightTime, uint8_t precision, bool greateEqual, bool* res) {
  bool needConvert = IS_CALENDAR_TIME_DURATION(pLeftTime->unit) || IS_CALENDAR_TIME_DURATION(pRightTime->unit);
  *res = true;
  if (needConvert) {
    double  leftMonth = 0, rightMonth = 0;
    PAR_ERR_RET(getMonthsFromTimeVal(pLeftTime->datum.i, precision, pLeftTime->unit, &leftMonth));
    PAR_ERR_RET(getMonthsFromTimeVal(pRightTime->datum.i, precision, pRightTime->unit, &rightMonth));
    if (greateEqual) {
      if (leftMonth < rightMonth) {
        *res = false;
      }
    } else {
      if (leftMonth <= rightMonth) {
        *res = false;
      }
    }
  } else {
    if (greateEqual) {
      if (pLeftTime->datum.i < pRightTime->datum.i) {
        *res = false;
      }
    } else {
      if (pLeftTime->datum.i <= pRightTime->datum.i) {
       *res = false;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkStreamIntervalWindow(STranslateContext* pCxt, SIntervalWindowNode* pInterval) {
  int32_t     code = TSDB_CODE_SUCCESS;
  uint8_t     precision = ((SColumnNode*)pInterval->pCol)->node.resType.precision;
  SValueNode* pInter = (SValueNode*)pInterval->pInterval;
  SValueNode* pOffset = (SValueNode*)pInterval->pOffset;
  SValueNode* pSliding = (SValueNode*)pInterval->pSliding;
  SValueNode* pSOffset = (SValueNode*)pInterval->pSOffset;

  if (pInter) {
    bool valInter = IS_CALENDAR_TIME_DURATION(pInter->unit);
    if (pInter->datum.i <= 0 || (!valInter && pInter->datum.i < tsMinIntervalTime)) {
      PAR_ERR_RET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_VALUE_TOO_SMALL, tsMinIntervalTime,
                                       getPrecisionStr(precision)));
    } else if (pInter->datum.i / getPrecisionMultiple(precision) > tsdbMaxKeepMS) {
      PAR_ERR_RET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_VALUE_TOO_BIG, 1000, "years"));
    }
  }

  if (pOffset) {
    if(!pInter) {
      PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_INTERVAL_OFFSET, "Interval offset without interval"));
    }

    if (pOffset->datum.i < 0) {
      PAR_ERR_RET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_NEGATIVE));
    }

    if (pInter->unit == 'n' && pOffset->unit == 'y') {
      PAR_ERR_RET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_UNIT));
    }

    bool    greater = false;
    code = checkTimeGreater(pInter, pOffset, precision, false, &greater);
    if ((code != TSDB_CODE_SUCCESS)) {
      return code;
    } else if (!greater) {
      PAR_ERR_RET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG));
    }
  }

  if (pSliding) {
    const static int32_t INTERVAL_SLIDING_FACTOR = 100;
    if (IS_CALENDAR_TIME_DURATION(pSliding->unit)) {
      PAR_ERR_RET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SLIDING_UNIT));
    }
  } else {
    PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                     "Sliding window is required for stream query"));
  }

  if (pSOffset) {
    if (!pSliding) {
      PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_SLIDING_OFFSET, "Sliding offset without sliding"));
    }

    if (pInter) {
      PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_SLIDING_OFFSET, "Sliding offset cannot be used with interval"));
    }

    if (pSOffset->datum.i < 0) {
      PAR_ERR_RET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_NEGATIVE));
    }

    if (pSOffset->unit != 'a' && pSOffset->unit != 's' &&
        pSOffset->unit != 'm' && pSOffset->unit != 'h') {
      PAR_ERR_RET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_UNIT));
    }

    bool greater = false;
    code = checkTimeGreater(pSliding, pSOffset, precision, false, &greater);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    } else if (!greater) {
      PAR_ERR_RET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG));
    }
  }

  return TSDB_CODE_SUCCESS;
}

void tryCalcIntervalAutoOffset(SIntervalWindowNode *pInterval) {
  SValueNode* pOffset = (SValueNode*)pInterval->pOffset;
  uint8_t     precision = ((SColumnNode*)pInterval->pCol)->node.resType.precision;
  SValueNode* pInter = (SValueNode*)pInterval->pInterval;
  SValueNode* pSliding = (SValueNode*)pInterval->pSliding;

  if (pOffset == NULL || pOffset->datum.i != AUTO_DURATION_VALUE) {
    return;
  }

  // ignore auto offset if not applicable
  if (pInterval->timeRange.skey == INT64_MIN) {
    pOffset->datum.i = 0;
    return;
  }

  SInterval interval = {.interval = pInter->datum.i,
                        .sliding = (pSliding != NULL) ? pSliding->datum.i : pInter->datum.i,
                        .intervalUnit = pInter->unit,
                        .slidingUnit = (pSliding != NULL) ? pSliding->unit : pInter->unit,
                        .offset = pOffset->datum.i,
                        .precision = precision,
                        .timezone = pInterval->timezone,
                        .timeRange = pInterval->timeRange};

  /**
   * Considering that the client and server may be in different time zones,
   * these situations need to be deferred to the server for calculation.
   */
  if (IS_CALENDAR_TIME_DURATION(interval.intervalUnit) || interval.intervalUnit == 'd' ||
      interval.intervalUnit == 'w' || IS_CALENDAR_TIME_DURATION(interval.slidingUnit) || interval.slidingUnit == 'd' ||
      interval.slidingUnit == 'w') {
    return;
  }

  calcIntervalAutoOffset(&interval);
  pOffset->datum.i = interval.offset;
}

static int32_t translateIntervalWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  SIntervalWindowNode* pInterval = (SIntervalWindowNode*)pSelect->pWindow;
  int32_t              code = TSDB_CODE_SUCCESS;

  pInterval->timeRange = pSelect->timeRange;
  tryCalcIntervalAutoOffset(pInterval);

  code = checkIntervalWindow(pCxt, pInterval);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateFill(pCxt, pSelect, pInterval);
  }
  return code;
}

static const int64_t periodLowerBound = 10;
static const int64_t periodUpperBound = (int64_t) 3650 * 24 * 60 * 60 * 1000; // 10 years in milliseconds
static const int64_t offsetUpperBound = (int64_t) 24 * 60 * 60 * 1000; // 1 day in milliseconds

static int32_t checkPeriodWindow(STranslateContext* pCxt, SPeriodWindowNode* pPeriod) {
  uint8_t     precision = TSDB_TIME_PRECISION_MILLI;
  SValueNode* pPer = (SValueNode*)pPeriod->pPeroid;
  SValueNode* pOffset = (SValueNode*)pPeriod->pOffset;

  if (pPer) {
    if (pPer->unit != 'a' && pPer->unit != 's' &&
        pPer->unit != 'm' && pPer->unit != 'h' && pPer->unit != 'd') {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PERIOD_UNIT, pPer->unit);
    }
    if (pPer->datum.i / getPrecisionMultiple(precision) < periodLowerBound ||
        pPer->datum.i / getPrecisionMultiple(precision) > periodUpperBound) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PERIOD_RANGE, "Period value out of range [10a, 3650d]");
    }
  }

  if (pOffset) {
    if(!pPer) {
      PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER, "Offset without period"));
    }

    if (pOffset->datum.i < 0) {
      PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER, "Negative period offset value"));
    }

    if (pOffset->unit != 'a' && pOffset->unit != 's' &&
        pOffset->unit != 'm' && pOffset->unit != 'h') {
      PAR_ERR_RET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_WIN_OFFSET_UNIT, pOffset->unit));
    }

    if (pOffset->datum.i > offsetUpperBound) {
      PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER, "Period offset value should less than 1d"));
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkStateExpr(STranslateContext* pCxt, SNode* pNode) {
  int32_t type = ((SExprNode*)pNode)->resType.type;
  if (!IS_INTEGER_TYPE(type) && type != TSDB_DATA_TYPE_BOOL && !IS_VAR_DATA_TYPE(type)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STATE_WIN_TYPE);
  }

  if (QUERY_NODE_COLUMN == nodeType(pNode) && COLUMN_TYPE_TAG == ((SColumnNode*)pNode)->colType) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STATE_WIN_COL);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkStateExtend(STranslateContext *pCxt, SNode *pNode) {
  SValueNode *pExtend = (SValueNode *)pNode;
  if (pExtend == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t option = pExtend->datum.i;
  if (option != STATE_WIN_EXTEND_OPTION_DEFAULT &&
      option != STATE_WIN_EXTEND_OPTION_FORWARD &&
      option != STATE_WIN_EXTEND_OPTION_BACKWARD) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STATE_WIN_EXTEND);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkTrueForLimit(STranslateContext *pCxt, SNode *pNode) {
  SValueNode *pTrueForLimit = (SValueNode *)pNode;
  if (pTrueForLimit == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  if (pTrueForLimit->datum.i < 0) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TRUE_FOR_NEGATIVE);
  }
  if (IS_CALENDAR_TIME_DURATION(pTrueForLimit->unit)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TRUE_FOR_UNIT);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkAndConvertZerothValue(STranslateContext* pCxt, SStateWindowNode* pStateWin) {
    if (NULL == pStateWin->pZeroth) {
      return TSDB_CODE_SUCCESS;
    }
    if (QUERY_NODE_VALUE != nodeType(pStateWin->pZeroth)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, "Zeroth value can only accept constant");
    }

    SDataType targetDt = ((SExprNode*)pStateWin->pExpr)->resType;
    SDataType zerothDt = ((SExprNode*)pStateWin->pZeroth)->resType;
    if (targetDt.type == zerothDt.type) {
      // if have same type, no need to cast
      return TSDB_CODE_SUCCESS;
    }

    // need cast zeroth value to target type
    SNode*  pCastFunc = NULL;
    int32_t code = createCastFunc(pCxt, pStateWin->pZeroth, targetDt, &pCastFunc);
    if (TSDB_CODE_SUCCESS == code) {
      code = scalarCalculateConstants(pCastFunc, &pStateWin->pZeroth);
    }
    if (TSDB_CODE_SUCCESS != code) {
      code = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, "Zeroth value type mismatch");
    }

    return code;
}

static int32_t checkStateWindow(STranslateContext* pCxt, SStateWindowNode* pStateWin) {
  PAR_ERR_RET(checkStateExpr(pCxt, pStateWin->pExpr));
  PAR_ERR_RET(checkStateExtend(pCxt, pStateWin->pExtend));
  PAR_ERR_RET(checkTrueForLimit(pCxt, pStateWin->pTrueForLimit));
  PAR_ERR_RET(checkAndConvertZerothValue(pCxt, pStateWin));
  return TSDB_CODE_SUCCESS;
}

static int32_t translateZerothState(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = TSDB_CODE_SUCCESS;
  SStateWindowNode* pStateWin = (SStateWindowNode*)pSelect->pWindow;

  if (NULL != pStateWin->pZeroth) {
    // create a new 'NOT EQUAL' operator
    SOperatorNode* notEqualOp = NULL;
    code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&notEqualOp);
    if (TSDB_CODE_SUCCESS != code) {
      parserError("failed to create 'NOT EQUAL' operator at %s since %s", __func__, tstrerror(code));
      return code;
    }
    notEqualOp->opType = OP_TYPE_NOT_EQUAL;
    code = nodesCloneNode(pStateWin->pExpr, &notEqualOp->pLeft);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesCloneNode(pStateWin->pZeroth, &notEqualOp->pRight);
    }

    if (TSDB_CODE_SUCCESS != code) {
      parserError("failed to clone nodes for zeroth state at %s since %s", __func__, tstrerror(code));
      nodesDestroyNode((SNode*)notEqualOp);
      return code;
    }

    // merge the 'NOT EQUAL' operator to having clause
    SNode* pNewCond = (SNode*)notEqualOp;
    code = nodesMergeNode(&pSelect->pHaving, &pNewCond);
    if (code != TSDB_CODE_SUCCESS) {
      parserError("failed to merge NOT EQUAL operator to having clause at %s since %s", __func__, tstrerror(code));
      nodesDestroyNode((SNode*)notEqualOp);
      return code;
    }
  }

  return code;
}

static int32_t translateStateWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = 0;
  if (QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
      !isGlobalTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TIMELINE_QUERY,
                                   "STATE_WINDOW requires valid time series input");
  }
  SStateWindowNode* pStateWin = (SStateWindowNode*)pSelect->pWindow;
  code = checkStateWindow(pCxt, pStateWin);
  if (TSDB_CODE_SUCCESS != code) {
    parserError("failed to check state window at %s since %s", __func__, tstrerror(code));
    return code;
  }

  code = translateZerothState(pCxt, pSelect);
  if (TSDB_CODE_SUCCESS != code) {
    parserError("failed to translate state zeroth at %s since %s", __func__, tstrerror(code));
  }
  return code;
}

static int32_t checkSessionWindow(STranslateContext* pCxt, SSessionWindowNode* pSession) {
  if ('y' == pSession->pGap->unit || 'n' == pSession->pGap->unit || 0 == pSession->pGap->datum.i) {
    PAR_ERR_RET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SESSION_GAP));
  }
  if (!isPrimaryKeyImpl((SNode*)pSession->pCol)) {
    PAR_ERR_RET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SESSION_COL));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSessionWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
      !isGlobalTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TIMELINE_QUERY,
                                   "SESSION requires valid time series input");
  }

  return checkSessionWindow(pCxt, (SSessionWindowNode*)pSelect->pWindow);
}

static EDealRes collectWindowsPseudocolumns(SNode* pNode, void* pContext) {
  SCollectWindowsPseudocolumnsContext* pCtx = pContext;
  SNodeList*                           pCols = pCtx->pCols;
  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (FUNCTION_TYPE_WSTART == pFunc->funcType || FUNCTION_TYPE_WEND == pFunc->funcType ||
        FUNCTION_TYPE_WDURATION == pFunc->funcType) {
      SNode* pClonedNode = NULL;
      if (TSDB_CODE_SUCCESS != (pCtx->code = nodesCloneNode(pNode, &pClonedNode))) return DEAL_RES_ERROR;
      if (TSDB_CODE_SUCCESS != (pCtx->code = nodesListStrictAppend(pCols, pClonedNode))) {
        return DEAL_RES_ERROR;
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t checkWindowsConditonValid(SEventWindowNode* pEventWindowNode) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SNodeList*  pCols = NULL;

  PAR_ERR_JRET(nodesMakeList(&pCols));

  SCollectWindowsPseudocolumnsContext ctx = {.code = 0, .pCols = pCols};
  nodesWalkExpr(pEventWindowNode->pStartCond, collectWindowsPseudocolumns, &ctx);
  if (TSDB_CODE_SUCCESS == ctx.code && pCols->length > 0) {
    PAR_ERR_JRET(TSDB_CODE_QRY_INVALID_WINDOW_CONDITION);
  }

  nodesWalkExpr(pEventWindowNode->pEndCond, collectWindowsPseudocolumns, &ctx);
  if (TSDB_CODE_SUCCESS == ctx.code && pCols->length > 0) {
    PAR_ERR_JRET(TSDB_CODE_QRY_INVALID_WINDOW_CONDITION);
  }

_return:
  nodesDestroyList(pCols);
  return code;
}

static int32_t checkEventWindow(STranslateContext* pCxt, SEventWindowNode* pEvent) {
  PAR_ERR_RET(checkTrueForLimit(pCxt, pEvent->pTrueForLimit));
  PAR_RET(checkWindowsConditonValid(pEvent));
}

static int32_t translateEventWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
      !isGlobalTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TIMELINE_QUERY,
                                   "EVENT_WINDOW requires valid time series input");
  }
  return checkEventWindow(pCxt, (SEventWindowNode*)pSelect->pWindow);
}

static int32_t checkCountWindow(STranslateContext* pCxt, SCountWindowNode* pCountWin, bool streamTrigger) {
  if (pCountWin->windowCount < (streamTrigger ? 1 : 2)) {
    PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                        "Size of Count window must exceed 1."));
  }

  if (pCountWin->windowSliding < (streamTrigger ? 0 : 1)) {
    PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                        "Size of Count window must exceed 0."));
  }

  if (pCountWin->windowSliding > pCountWin->windowCount && !streamTrigger) {
    PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                        "sliding value no larger than the count value."));
  }

  if (pCountWin->windowCount >= INT32_MAX) {
    PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                        "Size of Count window must less than 2147483647(INT32_MAX)."));
  }

  if (streamTrigger) {
    SNode* pNode = NULL;
    FOREACH(pNode, pCountWin->pColList) {
      if (nodeType(pNode) != QUERY_NODE_COLUMN) {
        PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                            "COUNT_WINDOW only support on column."));
      } else {
        SColumnNode* pCol = (SColumnNode*)pNode;
        if (COLUMN_TYPE_TAG == pCol->colType) {
          PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                              "COUNT_WINDOW not support on tag column."));
        }
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateCountWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
      !isGlobalTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TIMELINE_QUERY,
                                   "COUNT_WINDOW requires valid time series input");
  }

  SNode* pLogicCond = NULL;
  PAR_ERR_JRET(extractCondFromCountWindow(pCxt, (SCountWindowNode*)pSelect->pWindow, &pLogicCond));

  if (pLogicCond) {
    PAR_ERR_JRET(translateExpr(pCxt, &pLogicCond));
    PAR_ERR_JRET(insertCondIntoSelectStmt(pSelect, &pLogicCond));
  }

  return checkCountWindow(pCxt, (SCountWindowNode*)pSelect->pWindow, false);

_return:
  if (pLogicCond) {
    nodesDestroyNode(pLogicCond);
  }
  return code;
}

static int32_t checkAnomalyExpr(STranslateContext* pCxt, SNode* pNode) {
  int32_t type = ((SExprNode*)pNode)->resType.type;
  if (!IS_MATHABLE_TYPE(type)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ANOMALY_WIN_TYPE,
                                   "ANOMALY_WINDOW only support mathable column");
  }

  if (QUERY_NODE_COLUMN == nodeType(pNode) && COLUMN_TYPE_TAG == ((SColumnNode*)pNode)->colType) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ANOMALY_WIN_COL,
                                   "ANOMALY_WINDOW not support on tag column");
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateAnomalyWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
      !isGlobalTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TIMELINE_QUERY,
                                   "ANOMALY_WINDOW requires valid time series input");
  }

  SAnomalyWindowNode* pAnomaly = (SAnomalyWindowNode*)pSelect->pWindow;
  int32_t             code = checkAnomalyExpr(pCxt, pAnomaly->pExpr);
  if (TSDB_CODE_SUCCESS == code) {
    if (!taosAnalyGetOptStr(pAnomaly->anomalyOpt, "algo", NULL, 0) != 0) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ANOMALY_WIN_OPT,
                                     "ANOMALY_WINDOW option should include algo field");
    }
  }

  return code;
}

static int32_t translatePeriodWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  SPeriodWindowNode* pPeriod = (SPeriodWindowNode*)pSelect->pWindow;
  int32_t code = TSDB_CODE_SUCCESS;

  // TODO(smj) : check period window
  return code;
}

static int32_t translateSpecificWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  switch (nodeType(pSelect->pWindow)) {
    case QUERY_NODE_STATE_WINDOW:
      return translateStateWindow(pCxt, pSelect);
    case QUERY_NODE_SESSION_WINDOW:
      return translateSessionWindow(pCxt, pSelect);
    case QUERY_NODE_INTERVAL_WINDOW:
      return translateIntervalWindow(pCxt, pSelect);
    case QUERY_NODE_EVENT_WINDOW:
      return translateEventWindow(pCxt, pSelect);
    case QUERY_NODE_COUNT_WINDOW:
      return translateCountWindow(pCxt, pSelect);
    case QUERY_NODE_ANOMALY_WINDOW:
      return translateAnomalyWindow(pCxt, pSelect);
    case QUERY_NODE_PERIOD_WINDOW:
      return translatePeriodWindow(pCxt, pSelect);
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pWindow) {
    return TSDB_CODE_SUCCESS;
  }
  if (pSelect->pFromTable->type == QUERY_NODE_REAL_TABLE &&
      ((SRealTableNode*)pSelect->pFromTable)->pMeta->tableType == TSDB_SYSTEM_TABLE) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYSTABLE_NOT_ALLOWED, "WINDOW");
  }
  int32_t code = 0;
  if (QUERY_NODE_INTERVAL_WINDOW != nodeType(pSelect->pWindow)) {
    if (NULL != pSelect->pFromTable && QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
        !isGlobalTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
      bool isTimelineAlignedQuery = false;
      code = isTimeLineAlignedQuery(pCxt->pCurrStmt, &isTimelineAlignedQuery);
      if (TSDB_CODE_SUCCESS != code) return code;
      if (!isTimelineAlignedQuery) return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_NOT_ALLOWED_WIN_QUERY);
    }
    if (NULL != pSelect->pFromTable && QUERY_NODE_JOIN_TABLE == nodeType(pSelect->pFromTable) &&
        (TIME_LINE_GLOBAL != pSelect->timeLineCurMode && TIME_LINE_MULTI != pSelect->timeLineCurMode)) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_NOT_ALLOWED_WIN_QUERY);
    }
  }

  if (QUERY_NODE_INTERVAL_WINDOW == nodeType(pSelect->pWindow)) {
    if (NULL != pSelect->pFromTable && QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
        !isBlockTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
      bool isTimelineAlignedQuery = false;
      code = isTimeLineAlignedQuery(pCxt->pCurrStmt, &isTimelineAlignedQuery);
      if (TSDB_CODE_SUCCESS != code) return code;
      if (!isTimelineAlignedQuery) return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_NOT_ALLOWED_WIN_QUERY);
    }
    if (NULL != pSelect->pFromTable && QUERY_NODE_JOIN_TABLE == nodeType(pSelect->pFromTable) &&
        (TIME_LINE_NONE == pSelect->timeLineCurMode)) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_NOT_ALLOWED_WIN_QUERY);
    }
  }

  pCxt->currClause = SQL_CLAUSE_WINDOW;
  code = translateExpr(pCxt, &pSelect->pWindow);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateSpecificWindow(pCxt, pSelect);
  }
  return code;
}

static int32_t createDefaultFillNode(STranslateContext* pCxt, SNode** pOutput) {
  SFillNode* pFill = NULL;
  int32_t    code = nodesMakeNode(QUERY_NODE_FILL, (SNode**)&pFill);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  pFill->mode = FILL_MODE_NONE;

  SColumnNode* pCol = NULL;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pFill);
    return code;
  }
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  tstrncpy(pCol->colName, ROWTS_PSEUDO_COLUMN_NAME, TSDB_COL_NAME_LEN);
  pFill->pWStartTs = (SNode*)pCol;

  *pOutput = (SNode*)pFill;
  return TSDB_CODE_SUCCESS;
}

static int32_t createDefaultEveryNode(STranslateContext* pCxt, SNode** pOutput) {
  SValueNode* pEvery = NULL;
  int32_t     code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pEvery);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  pEvery->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pEvery->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  pEvery->flag |= VALUE_FLAG_IS_DURATION;
  pEvery->literal = taosStrdup("1s");
  if (!pEvery->literal) {
    code = terrno;
    nodesDestroyNode((SNode*)pEvery);
    return code;
  }

  *pOutput = (SNode*)pEvery;
  return TSDB_CODE_SUCCESS;
}

static int32_t checkEvery(STranslateContext* pCxt, SValueNode* pInterval) {
  int32_t len = strlen(pInterval->literal);

  char* unit = &pInterval->literal[len - 1];
  if (*unit == 'n' || *unit == 'y') {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE,
                                   "Unsupported time unit in EVERY clause");
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateInterpEvery(STranslateContext* pCxt, SNode** pEvery) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == *pEvery) {
    code = createDefaultEveryNode(pCxt, pEvery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkEvery(pCxt, (SValueNode*)(*pEvery));
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExpr(pCxt, pEvery);
  }

  int64_t interval = ((SValueNode*)(*pEvery))->datum.i;
  if (interval == 0) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE,
                                   "Unsupported time unit in EVERY clause");
  }

  return code;
}

static EDealRes hasRowTsOriginFuncWalkNode(SNode* pNode, void* ctx) {
  bool* hasRowTsOriginFunc = ctx;
  if (nodeType(pNode) == QUERY_NODE_FUNCTION) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (fmIsRowTsOriginFunc(pFunc->funcId)) {
      *hasRowTsOriginFunc = true;
      return DEAL_RES_END;
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t translateInterpFill(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pSelect->pFill) {
    code = createDefaultFillNode(pCxt, &pSelect->pFill);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExpr(pCxt, &pSelect->pFill);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = getQueryTimeRange(pCxt, &pSelect->pRange, &(((SFillNode*)pSelect->pFill)->timeRange), &(((SFillNode*)pSelect->pFill)->pTimeRange), pSelect->pFromTable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkFill(pCxt, (SFillNode*)pSelect->pFill, (SValueNode*)pSelect->pEvery, true, pSelect->precision);
  }
  bool hasRowTsOriginFunc = false;
  nodesWalkExprs(pSelect->pProjectionList, hasRowTsOriginFuncWalkNode, &hasRowTsOriginFunc);
  if (TSDB_CODE_SUCCESS == code) {
    SFillNode* pFill = (SFillNode*)pSelect->pFill;
    if (pSelect->pRangeAround) {
      if (pFill->mode != FILL_MODE_PREV && pFill->mode != FILL_MODE_NEXT && pFill->mode != FILL_MODE_NEAR) {
        return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE,
                                       "Range with interval can only used with fill PREV/NEXT/NEAR");
      }
      if (TSDB_CODE_SUCCESS == code) code = doCheckFillValues(pCxt, pFill, pSelect->pProjectionList);
    } else {
      if (FILL_MODE_PREV == pFill->mode || FILL_MODE_NEXT == pFill->mode || FILL_MODE_NEAR == pFill->mode) {
        if (pFill->pValues) {
          return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, "Can't specify fill values");
        }
      } else {
        if (hasRowTsOriginFunc)
          return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_FILL_NOT_ALLOWED_FUNC,
                                      "_irowts_origin can only be used with FILL PREV/NEXT/NEAR");
      }
      code = checkFillValues(pCxt, pFill, pSelect->pProjectionList);
    }
  }

  return code;
}

static int32_t translateInterpAround(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = 0;
  if (pSelect->pRangeAround) {
    SRangeAroundNode* pAround = (SRangeAroundNode*)pSelect->pRangeAround;
    code = translateExpr(pCxt, &pAround->pInterval);
    if (TSDB_CODE_SUCCESS == code) {
      if (nodeType(pAround->pInterval) == QUERY_NODE_VALUE &&
          ((SValueNode*)pAround->pInterval)->flag & VALUE_FLAG_IS_DURATION) {
        SValueNode* pVal = (SValueNode*)pAround->pInterval;
        if (pVal->datum.i <= 0) {
          return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                         "Range interval must be greater than 0");
        }
        int8_t unit = pVal->unit;
        if (unit == TIME_UNIT_YEAR || unit == TIME_UNIT_MONTH) {
          return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                         "Unsupported time unit in RANGE clause");
        }
      } else {
        return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Invalid range interval");
      }
    }
  }
  return code;
}

static int32_t translateInterp(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (!pSelect->hasInterpFunc) {
    if (NULL != pSelect->pRange || NULL != pSelect->pEvery || NULL != pSelect->pFill) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_INTERP_CLAUSE);
    }
    if (pSelect->hasInterpPseudoColFunc) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                     "Has Interp pseudo column(s) but missing interp function");
    }
    return TSDB_CODE_SUCCESS;
  }

  if ((NULL != pSelect->pFromTable) && (QUERY_NODE_JOIN_TABLE == nodeType(pSelect->pFromTable))) {
    SJoinTableNode* pJoinTable = (SJoinTableNode*)pSelect->pFromTable;
    if (IS_WINDOW_JOIN(pJoinTable->subType)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_INTERP_CLAUSE,
                                     "Interp not supported to be used in WINDOW join");
    }
  }

  int32_t code = 0;
  if (NULL == pSelect->pRange || NULL == pSelect->pEvery || NULL == pSelect->pFill) {
    if (pSelect->pRange != NULL && QUERY_NODE_OPERATOR == nodeType(pSelect->pRange) && pSelect->pEvery == NULL) {
      // single point interp every can be omitted
    } else {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_INTERP_CLAUSE,
                                     "Missing RANGE clause, EVERY clause or FILL clause");
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = translateInterpAround(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExpr(pCxt, &pSelect->pRange);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateInterpEvery(pCxt, &pSelect->pEvery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateInterpFill(pCxt, pSelect);
  }
  return code;
}

static int32_t translateForecast(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (!(pSelect->hasForecastFunc || pSelect->hasGenericAnalysisFunc)) {
    if (pSelect->hasForecastPseudoColFunc) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                     "has analysis pseudo column(s) but missing analysis function");
    }
    return TSDB_CODE_SUCCESS;
  }

  if ((NULL != pSelect->pFromTable) && (QUERY_NODE_JOIN_TABLE == nodeType(pSelect->pFromTable))) {
    SJoinTableNode* pJoinTable = (SJoinTableNode*)pSelect->pFromTable;
    if (IS_WINDOW_JOIN(pJoinTable->subType)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FORECAST_CLAUSE,
                                     "Forecast not supported to be used in WINDOW join");
    }
  }

  return 0;
}

static int32_t removeConstantValueFromList(SNodeList** pList) {
  SNode* pNode = NULL;
  WHERE_EACH(pNode, *pList) {
    if (nodeType(pNode) == QUERY_NODE_VALUE ||
        (nodeType(pNode) == QUERY_NODE_FUNCTION && fmIsConstantResFunc((SFunctionNode*)pNode) &&
         fmIsScalarFunc(((SFunctionNode*)pNode)->funcId))) {
      ERASE_NODE(*pList);
      continue;
    }
    WHERE_NEXT;
  }

  if (*pList && (*pList)->length <= 0) {
    nodesDestroyList(*pList);
    *pList = NULL;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translatePartitionBy(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->currClause = SQL_CLAUSE_PARTITION_BY;
  int32_t code = TSDB_CODE_SUCCESS;

  if (pSelect->pPartitionByList) {
    bool other;
    code = translateClausePosition(pCxt, pSelect->pProjectionList, pSelect->pPartitionByList, &other);
  }

  if (TSDB_CODE_SUCCESS == code && pSelect->pPartitionByList) {
    int8_t typeType = getTableTypeFromTableNode(pSelect->pFromTable);
    SNode* pPar = nodesListGetNode(pSelect->pPartitionByList, 0);
    if (!((TSDB_NORMAL_TABLE == typeType || TSDB_CHILD_TABLE == typeType) && 1 == pSelect->pPartitionByList->length &&
          (QUERY_NODE_FUNCTION == nodeType(pPar) && FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pPar)->funcType))) {
      pSelect->timeLineResMode = TIME_LINE_MULTI;
    }
    code = translatePartitionByList(pCxt, pSelect);
  }
  return code;
}

typedef struct SEqCondTbNameTableInfo {
  SRealTableNode* pRealTable;
  SArray*         aTbnames;
} SEqCondTbNameTableInfo;

//[tableAlias.]tbname = tbNamVal
static int32_t isOperatorEqTbnameCond(STranslateContext* pCxt, SOperatorNode* pOperator, char** ppTableAlias,
                                      SArray** ppTabNames, bool* pRet) {
  if (pOperator->opType != OP_TYPE_EQUAL) {
    *pRet = false;
    return TSDB_CODE_SUCCESS;
  }

  SFunctionNode* pTbnameFunc = NULL;
  SValueNode*    pValueNode = NULL;
  if (nodeType(pOperator->pLeft) == QUERY_NODE_FUNCTION &&
      ((SFunctionNode*)(pOperator->pLeft))->funcType == FUNCTION_TYPE_TBNAME &&
      nodeType(pOperator->pRight) == QUERY_NODE_VALUE) {
    pTbnameFunc = (SFunctionNode*)pOperator->pLeft;
    pValueNode = (SValueNode*)pOperator->pRight;
  } else if (nodeType(pOperator->pRight) == QUERY_NODE_FUNCTION &&
             ((SFunctionNode*)(pOperator->pRight))->funcType == FUNCTION_TYPE_TBNAME &&
             nodeType(pOperator->pLeft) == QUERY_NODE_VALUE) {
    pTbnameFunc = (SFunctionNode*)pOperator->pRight;
    pValueNode = (SValueNode*)pOperator->pLeft;
  } else {
    *pRet = false;
    return TSDB_CODE_SUCCESS;
  }

  char* pTableAlias = NULL;
  if (LIST_LENGTH(pTbnameFunc->pParameterList) == 0) {
  } else if (LIST_LENGTH(pTbnameFunc->pParameterList) == 1) {
    SNode* pQualNode = nodesListGetNode(pTbnameFunc->pParameterList, 0);
    if (nodeType(pQualNode) != QUERY_NODE_VALUE) return false;
    SValueNode* pQualValNode = (SValueNode*)pQualNode;
    pTableAlias = pQualValNode->literal;
  } else {
    *pRet = false;
    return TSDB_CODE_SUCCESS;
  }
  SArray* pTabNames = NULL;
  pTabNames = taosArrayInit(1, sizeof(void*));
  if (!pTabNames) {
    return terrno;
  }
  if (NULL == taosArrayPush(pTabNames, &(pValueNode->literal))) {
    taosArrayDestroy(pTabNames);
    return terrno;
  }
  *ppTableAlias = pTableAlias;
  *ppTabNames = pTabNames;
  *pRet = true;
  return TSDB_CODE_SUCCESS;
}

//[tableAlias.]tbname in (value1, value2, ...)
static int32_t isOperatorTbnameInCond(STranslateContext* pCxt, SOperatorNode* pOperator, char** ppTableAlias,
                                      SArray** ppTbNames, bool* pRet) {
  if (pOperator->opType != OP_TYPE_IN) return false;
  if (nodeType(pOperator->pLeft) != QUERY_NODE_FUNCTION ||
      ((SFunctionNode*)(pOperator->pLeft))->funcType != FUNCTION_TYPE_TBNAME ||
      nodeType(pOperator->pRight) != QUERY_NODE_NODE_LIST) {
    *pRet = false;
    return TSDB_CODE_SUCCESS;
  }

  SFunctionNode* pTbnameFunc = (SFunctionNode*)pOperator->pLeft;
  if (LIST_LENGTH(pTbnameFunc->pParameterList) == 0) {
    *ppTableAlias = NULL;
  } else if (LIST_LENGTH(pTbnameFunc->pParameterList) == 1) {
    SNode* pQualNode = nodesListGetNode(pTbnameFunc->pParameterList, 0);
    if (nodeType(pQualNode) != QUERY_NODE_VALUE) return false;
    SValueNode* pQualValNode = (SValueNode*)pQualNode;
    *ppTableAlias = pQualValNode->literal;
  } else {
    *pRet = false;
    return TSDB_CODE_SUCCESS;
  }
  SNodeListNode* pValueListNode = (SNodeListNode*)pOperator->pRight;
  *ppTbNames = taosArrayInit(LIST_LENGTH(pValueListNode->pNodeList), sizeof(void*));
  if (!*ppTbNames) return terrno;
  SNodeList* pValueNodeList = pValueListNode->pNodeList;
  SNode*     pValNode = NULL;
  FOREACH(pValNode, pValueNodeList) {
    if (nodeType(pValNode) != QUERY_NODE_VALUE) {
      *pRet = false;
      return TSDB_CODE_SUCCESS;
    }
    if (NULL == taosArrayPush(*ppTbNames, &((SValueNode*)pValNode)->literal)) {
      taosArrayDestroy(*ppTbNames);
      *ppTbNames = NULL;
      return terrno;
    }
  }
  *pRet = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t findEqCondTbNameInOperatorNode(STranslateContext* pCxt, SNode* pWhere, SEqCondTbNameTableInfo* pInfo,
                                              bool* pRet) {
  int32_t code = TSDB_CODE_SUCCESS;
  char*   pTableAlias = NULL;
  bool    eqTbnameCond = false, tbnameInCond = false;
  code = isOperatorEqTbnameCond(pCxt, (SOperatorNode*)pWhere, &pTableAlias, &pInfo->aTbnames, &eqTbnameCond);
  if (TSDB_CODE_SUCCESS == code) {
    code = isOperatorTbnameInCond(pCxt, (SOperatorNode*)pWhere, &pTableAlias, &pInfo->aTbnames, &tbnameInCond);
  }
  if (TSDB_CODE_SUCCESS != code) return code;
  if (eqTbnameCond || tbnameInCond) {
    STableNode* pTable;
    if (pTableAlias == NULL) {
      pTable = (STableNode*)((SSelectStmt*)(pCxt->pCurrStmt))->pFromTable;
    } else {
      code = findTable(pCxt, pTableAlias, &pTable);
    }
    if (code == TSDB_CODE_SUCCESS && nodeType(pTable) == QUERY_NODE_REAL_TABLE && ((SRealTableNode*)pTable)->pMeta &&
        ((SRealTableNode*)pTable)->pMeta->tableType == TSDB_SUPER_TABLE) {
      pInfo->pRealTable = (SRealTableNode*)pTable;
      *pRet = true;
      return TSDB_CODE_SUCCESS;
    }
    taosArrayDestroy(pInfo->aTbnames);
    pInfo->aTbnames = NULL;
  }
  *pRet = false;
  return TSDB_CODE_SUCCESS;
}

static bool isTableExistInTableTbnames(SArray* aTableTbNames, SRealTableNode* pTable) {
  for (int i = 0; i < taosArrayGetSize(aTableTbNames); ++i) {
    SEqCondTbNameTableInfo* info = taosArrayGet(aTableTbNames, i);
    if (info->pRealTable == pTable) {
      return true;
    }
  }
  return false;
}

static int32_t findEqualCondTbnameInLogicCondAnd(STranslateContext* pCxt, SNode* pWhere, SArray* aTableTbnames) {
  SNode*  pTmpNode = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  FOREACH(pTmpNode, ((SLogicConditionNode*)pWhere)->pParameterList) {
    if (nodeType(pTmpNode) == QUERY_NODE_OPERATOR) {
      SEqCondTbNameTableInfo info = {0};
      bool                   bIsEqTbnameCond;
      code = findEqCondTbNameInOperatorNode(pCxt, pTmpNode, &info, &bIsEqTbnameCond);
      if (TSDB_CODE_SUCCESS != code) break;
      if (bIsEqTbnameCond) {
        if (!isTableExistInTableTbnames(aTableTbnames, info.pRealTable)) {
          // TODO: intersect tbNames of same table? speed
          if (NULL == taosArrayPush(aTableTbnames, &info)) {
            code = terrno;
            break;
          }
        } else {
          taosArrayDestroy(info.aTbnames);
        }
      }
    }
    // TODO: logic cond
  }
  return code;
}

static int32_t unionEqualCondTbnamesOfSameTable(SArray* aTableTbnames, SEqCondTbNameTableInfo* pInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  bool    bFoundTable = false;
  for (int i = 0; i < taosArrayGetSize(aTableTbnames); ++i) {
    SEqCondTbNameTableInfo* info = taosArrayGet(aTableTbnames, i);
    if (info->pRealTable == pInfo->pRealTable) {
      if (NULL == taosArrayAddAll(info->aTbnames, pInfo->aTbnames)) {
        code = terrno;
        break;
      }
      taosArrayDestroy(pInfo->aTbnames);
      pInfo->aTbnames = NULL;
      bFoundTable = true;
      break;
    }
  }
  if (TSDB_CODE_SUCCESS == code && !bFoundTable) {
    if (NULL == taosArrayPush(aTableTbnames, pInfo)) {
      code = terrno;
    }
  }
  return code;
}

static int32_t findEqualCondTbnameInLogicCondOr(STranslateContext* pCxt, SNode* pWhere, SArray* aTableTbnames) {
  int32_t code = TSDB_CODE_SUCCESS;
  bool    bAllTbName = true;
  SNode*  pTmpNode = NULL;
  FOREACH(pTmpNode, ((SLogicConditionNode*)pWhere)->pParameterList) {
    // TODO: logic cond
    if (nodeType(pTmpNode) == QUERY_NODE_OPERATOR) {
      SEqCondTbNameTableInfo info = {0};
      bool                   bIsEqTbnameCond;
      code = findEqCondTbNameInOperatorNode(pCxt, pTmpNode, &info, &bIsEqTbnameCond);
      if (TSDB_CODE_SUCCESS != code) break;
      if (!bIsEqTbnameCond) {
        bAllTbName = false;
        break;
      } else {
        code = unionEqualCondTbnamesOfSameTable(aTableTbnames, &info);
        if (TSDB_CODE_SUCCESS != code) break;
      }
    } else {
      bAllTbName = false;
      break;
    }
  }
  if (TSDB_CODE_SUCCESS == code && !bAllTbName) {
    for (int i = 0; i < taosArrayGetSize(aTableTbnames); ++i) {
      SEqCondTbNameTableInfo* pInfo = taosArrayGet(aTableTbnames, i);
      taosArrayDestroy(pInfo->aTbnames);
      pInfo->aTbnames = NULL;
    }
    taosArrayClear(aTableTbnames);
  }
  return code;
}

static int32_t findEqualCondTbname(STranslateContext* pCxt, SNode* pWhere, SArray* aTableTbnames) {
  int32_t code = TSDB_CODE_SUCCESS;
  // TODO: optimize nested and/or condition. now only the fist level is processed.
  if (nodeType(pWhere) == QUERY_NODE_OPERATOR) {
    SEqCondTbNameTableInfo info = {0};
    bool                   bIsEqTbnameCond;
    code = findEqCondTbNameInOperatorNode(pCxt, pWhere, &info, &bIsEqTbnameCond);
    if (TSDB_CODE_SUCCESS != code) return code;
    if (bIsEqTbnameCond) {
      if (NULL == taosArrayPush(aTableTbnames, &info)) {
        return terrno;
      }
    }
  } else if (nodeType(pWhere) == QUERY_NODE_LOGIC_CONDITION) {
    if (((SLogicConditionNode*)pWhere)->condType == LOGIC_COND_TYPE_AND) {
      code = findEqualCondTbnameInLogicCondAnd(pCxt, pWhere, aTableTbnames);
    } else if (((SLogicConditionNode*)pWhere)->condType == LOGIC_COND_TYPE_OR) {
      code = findEqualCondTbnameInLogicCondOr(pCxt, pWhere, aTableTbnames);
    }
  }
  return code;
}

static void findVgroupsFromEqualTbname(STranslateContext* pCxt, SArray* aTbnames, const char* dbName,
                                       int32_t numOfVgroups, SVgroupsInfo* vgsInfo) {
  int32_t nVgroups = 0;
  int32_t nTbls = taosArrayGetSize(aTbnames);

  if (nTbls >= numOfVgroups) {
    vgsInfo->numOfVgroups = 0;
    return;
  }

  for (int j = 0; j < nTbls; ++j) {
    SName snameTb = {0};
    char* tbName = taosArrayGetP(aTbnames, j);
    toName(pCxt->pParseCxt->acctId, dbName, tbName, &snameTb);
    SVgroupInfo vgInfo = {0};
    bool        bExists;
    int32_t     code = catalogGetCachedTableHashVgroup(pCxt->pParseCxt->pCatalog, &snameTb, &vgInfo, &bExists);
    if (code == TSDB_CODE_SUCCESS && bExists) {
      bool bFoundVg = false;
      for (int32_t k = 0; k < nVgroups; ++k) {
        if (vgsInfo->vgroups[k].vgId == vgInfo.vgId) {
          bFoundVg = true;
          break;
        }
      }
      if (!bFoundVg) {
        vgsInfo->vgroups[nVgroups] = vgInfo;
        ++nVgroups;
        vgsInfo->numOfVgroups = nVgroups;
      }
    } else {
      vgsInfo->numOfVgroups = 0;
      break;
    }
  }
}

static int32_t replaceToChildTableQuery(STranslateContext* pCxt, SEqCondTbNameTableInfo* pInfo) {
  SName           snameTb = {0};
  int32_t         code = 0;
  SRealTableNode* pRealTable = pInfo->pRealTable;
  char*           tbName = taosArrayGetP(pInfo->aTbnames, 0);
  toName(pCxt->pParseCxt->acctId, pRealTable->table.dbName, tbName, &snameTb);

  STableMeta* pMeta = NULL;
  TAOS_CHECK_RETURN(catalogGetCachedTableMeta(pCxt->pParseCxt->pCatalog, &snameTb, &pMeta));
  if (NULL == pMeta || TSDB_CHILD_TABLE != pMeta->tableType || pMeta->suid != pRealTable->pMeta->suid) {
    goto _return;
  }

  pRealTable->pMeta->uid = pMeta->uid;
  pRealTable->pMeta->vgId = pMeta->vgId;
  pRealTable->pMeta->tableType = pMeta->tableType;
  tstrncpy(pRealTable->table.tableName, tbName, sizeof(pRealTable->table.tableName));

  pRealTable->stbRewrite = true;

  if (pRealTable->pTsmas) {
    // if select from a child table, fetch it's corresponding tsma target child table infos
    char buf[TSDB_TABLE_FNAME_LEN + TSDB_TABLE_NAME_LEN + 1];
    for (int32_t i = 0; i < pRealTable->pTsmas->size; ++i) {
      STableTSMAInfo* pTsma = taosArrayGetP(pRealTable->pTsmas, i);
      SName           tsmaTargetTbName = {0};
      toName(pCxt->pParseCxt->acctId, pRealTable->table.dbName, "", &tsmaTargetTbName);
      int32_t len = tsnprintf(buf, TSDB_TABLE_FNAME_LEN + TSDB_TABLE_NAME_LEN, "%s.%s_%s", pTsma->dbFName, pTsma->name,
                              pRealTable->table.tableName);
      len = taosCreateMD5Hash(buf, len);
      tstrncpy(tsmaTargetTbName.tname, buf, TSDB_TABLE_NAME_LEN);
      STsmaTargetTbInfo ctbInfo = {0};
      if (!pRealTable->tsmaTargetTbInfo) {
        pRealTable->tsmaTargetTbInfo = taosArrayInit(pRealTable->pTsmas->size, sizeof(STsmaTargetTbInfo));
        if (!pRealTable->tsmaTargetTbInfo) {
          code = terrno;
          break;
        }
      }
      snprintf(ctbInfo.tableName, TSDB_TABLE_NAME_LEN, "%s", tsmaTargetTbName.tname);
      ctbInfo.uid = pMeta->uid;

      if (NULL == taosArrayPush(pRealTable->tsmaTargetTbInfo, &ctbInfo)) {
        code = terrno;
        goto _return;
      }
    }
  }

_return:

  taosMemoryFree(pMeta);
  return code;
}

static int32_t setEqualTbnameTableVgroups(STranslateContext* pCxt, SSelectStmt* pSelect, SArray* aTables) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 aTableNum = taosArrayGetSize(aTables);
  int32_t                 nTbls = 0;
  bool                    stableQuery = false;
  SEqCondTbNameTableInfo* pInfo = NULL;

  parserDebug("start to update stable vg for tbname optimize, aTableNum:%d", aTableNum);
  for (int i = 0; i < aTableNum; ++i) {
    pInfo = taosArrayGet(aTables, i);
    int32_t numOfVgs = pInfo->pRealTable->pVgroupList->numOfVgroups;
    nTbls = taosArrayGetSize(pInfo->aTbnames);

    SVgroupsInfo* vgsInfo = taosMemoryMalloc(sizeof(SVgroupsInfo) + nTbls * sizeof(SVgroupInfo));
    if (!vgsInfo) {
      code = terrno;
      break;
    }
    findVgroupsFromEqualTbname(pCxt, pInfo->aTbnames, pInfo->pRealTable->table.dbName, numOfVgs, vgsInfo);
    if (vgsInfo->numOfVgroups != 0) {
      taosMemoryFree(pInfo->pRealTable->pVgroupList);
      pInfo->pRealTable->pVgroupList = vgsInfo;
    } else {
      taosMemoryFree(vgsInfo);
    }
    stableQuery = pInfo->pRealTable->pMeta->tableType == TSDB_SUPER_TABLE;
    vgsInfo = NULL;

    if (pInfo->pRealTable->pTsmas) {
      pInfo->pRealTable->tsmaTargetTbVgInfo = taosArrayInit(pInfo->pRealTable->pTsmas->size, POINTER_BYTES);
      if (!pInfo->pRealTable->tsmaTargetTbVgInfo) return terrno;

      for (int32_t j = 0; j < pInfo->pRealTable->pTsmas->size; ++j) {
        STableTSMAInfo* pTsma = taosArrayGetP(pInfo->pRealTable->pTsmas, j);
        SArray*         pTbNames = taosArrayInit(pInfo->aTbnames->size, POINTER_BYTES);
        if (!pTbNames) return terrno;

        for (int32_t k = 0; k < pInfo->aTbnames->size; ++k) {
          const char* pTbName = taosArrayGetP(pInfo->aTbnames, k);
          char*       pNewTbName = taosMemoryCalloc(1, TSDB_TABLE_FNAME_LEN + TSDB_TABLE_NAME_LEN + 1);
          if (!pNewTbName) {
            code = terrno;
            break;
          }
          if (NULL == taosArrayPush(pTbNames, &pNewTbName)) {
            taosMemoryFreeClear(pNewTbName);
            code = terrno;
            break;
          }
          snprintf(pNewTbName, TSDB_TABLE_FNAME_LEN + TSDB_TABLE_NAME_LEN + 1, "%s.%s_%s", pTsma->dbFName, pTsma->name,
                   pTbName);
          int32_t len = taosCreateMD5Hash(pNewTbName, strlen(pNewTbName));
        }
        if (TSDB_CODE_SUCCESS == code) {
          vgsInfo = taosMemoryMalloc(sizeof(SVgroupsInfo) + nTbls * sizeof(SVgroupInfo));
          if (!vgsInfo) code = terrno;
        }
        if (TSDB_CODE_SUCCESS == code) {
          findVgroupsFromEqualTbname(pCxt, pTbNames, pInfo->pRealTable->table.dbName, numOfVgs, vgsInfo);
          if (vgsInfo->numOfVgroups != 0) {
            if (NULL == taosArrayPush(pInfo->pRealTable->tsmaTargetTbVgInfo, &vgsInfo)) {
              taosMemoryFreeClear(vgsInfo);
              code = terrno;
            }
          } else {
            taosMemoryFreeClear(vgsInfo);
          }
        }
        taosArrayDestroyP(pTbNames, NULL);
        if (code) break;
      }
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }

  parserDebug("before ctbname optimize, code:%d, aTableNum:%d, nTbls:%d, stableQuery:%d", code, aTableNum, nTbls,
              stableQuery);

  if (TSDB_CODE_SUCCESS == code && 1 == aTableNum && 1 == nTbls && stableQuery && NULL == pInfo->pRealTable->pTsmas) {
    code = replaceToChildTableQuery(pCxt, pInfo);
  }

  return code;
}

static int32_t setTableVgroupsFromEqualTbnameCond(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = TSDB_CODE_SUCCESS;
  SArray* aTables = taosArrayInit(1, sizeof(SEqCondTbNameTableInfo));
  code = findEqualCondTbname(pCxt, pSelect->pWhere, aTables);
  if (code == TSDB_CODE_SUCCESS) {
    code = setEqualTbnameTableVgroups(pCxt, pSelect, aTables);
  }
  for (int i = 0; i < taosArrayGetSize(aTables); ++i) {
    SEqCondTbNameTableInfo* pInfo = taosArrayGet(aTables, i);
    taosArrayDestroy(pInfo->aTbnames);
  }
  taosArrayDestroy(aTables);
  return code;
}

static int32_t translateWhere(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->currClause = SQL_CLAUSE_WHERE;
  int32_t code = TSDB_CODE_SUCCESS;
  if (pSelect->pWhere && BIT_FLAG_TEST_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS) && inStreamCalcClause(pCxt)) {
    PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                        "%%%%trows can not be used with WHERE clause."));
  }
  PAR_ERR_RET(translateExpr(pCxt, &pSelect->pWhere));
  PAR_ERR_RET(getQueryTimeRange(pCxt, &pSelect->pWhere, &pSelect->timeRange, &pSelect->pTimeRange, pSelect->pFromTable));
  if (pSelect->pWhere != NULL && pCxt->pParseCxt->topicQuery == false) {
    PAR_ERR_RET(setTableVgroupsFromEqualTbnameCond(pCxt, pSelect));
  }
  return code;
}

static int32_t translateFrom(STranslateContext* pCxt, SNode** pTable) {
  pCxt->currClause = SQL_CLAUSE_FROM;
  return translateTable(pCxt, pTable, false);
}

static int32_t checkLimit(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = 0;

  if (pSelect->pLimit && pSelect->pLimit->limit) {
    code = translateExpr(pCxt, (SNode**)&pSelect->pLimit->limit);
  }
  if (TSDB_CODE_SUCCESS == code && pSelect->pLimit && pSelect->pLimit->offset) {
    code = translateExpr(pCxt, (SNode**)&pSelect->pLimit->offset);
  }
  if (TSDB_CODE_SUCCESS == code && pSelect->pSlimit && pSelect->pSlimit->limit) {
    code = translateExpr(pCxt, (SNode**)&pSelect->pSlimit->limit);
  }
  if (TSDB_CODE_SUCCESS == code && pSelect->pSlimit && pSelect->pSlimit->offset) {
    code = translateExpr(pCxt, (SNode**)&pSelect->pSlimit->offset);
  }

  if ((TSDB_CODE_SUCCESS == code) &&
      ((NULL != pSelect->pLimit && pSelect->pLimit->offset && pSelect->pLimit->offset->datum.i < 0) ||
       (NULL != pSelect->pSlimit && pSelect->pSlimit->offset && pSelect->pSlimit->offset->datum.i < 0))) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OFFSET_LESS_ZERO);
  }

  if ((TSDB_CODE_SUCCESS == code) && NULL != pSelect->pSlimit &&
      (NULL == pSelect->pPartitionByList && NULL == pSelect->pGroupByList)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SLIMIT_LEAK_PARTITION_GROUP_BY);
  }

  return code;
}

static int32_t createPrimaryKeyColByTable(STranslateContext* pCxt, STableNode* pTable, SNode** pPrimaryKey) {
  SColumnNode* pCol = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  tstrncpy(pCol->colName, ROWTS_PSEUDO_COLUMN_NAME, TSDB_COL_NAME_LEN);
  bool found = false;
  code = findAndSetColumn(pCxt, &pCol, pTable, &found, true);
  if (TSDB_CODE_SUCCESS != code || !found) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_VALID_PRIM_TS_REQUIRED);
  }
  *pPrimaryKey = (SNode*)pCol;
  return TSDB_CODE_SUCCESS;
}

static int32_t tranCreatePrimaryKeyCol(STranslateContext* pCxt, const char* tableAlias, SNode** pPrimaryKey) {
  STableNode* pTable = NULL;
  int32_t     code = findTable(pCxt, tableAlias, &pTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = createPrimaryKeyColByTable(pCxt, pTable, pPrimaryKey);
  }
  return code;
}

static EDealRes collectTableAlias(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SColumnNode* pCol = (SColumnNode*)pNode;
  if (NULL == *(void**)pContext) {
    SSHashObj* pHash = tSimpleHashInit(3, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    if (NULL == pHash) {
      return DEAL_RES_ERROR;
    }
    *(SSHashObj**)pContext = pHash;
  }

  if (TSDB_CODE_SUCCESS != tSimpleHashPut(*(SSHashObj**)pContext, pCol->tableAlias, strlen(pCol->tableAlias),
                                          pCol->tableAlias, sizeof(pCol->tableAlias))) {
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}

static EDealRes appendTsForImplicitTsFuncImpl(SNode* pNode, void* pContext) {
  STranslateContext* pCxt = pContext;
  if (isImplicitTsFunc(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (!isSelectStmt(pCxt->pCurrStmt)) {
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                              "%s function must be used in select statements", pFunc->functionName);
      return DEAL_RES_ERROR;
    }

    /*
        SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
        if ((NULL != pSelect->pFromTable && QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
            !isGlobalTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery) &&
            !isTimeLineAlignedQuery(pCxt->pCurrStmt)) ||
            (NULL != pSelect->pFromTable && QUERY_NODE_JOIN_TABLE == nodeType(pSelect->pFromTable) &&
            (TIME_LINE_GLOBAL != pSelect->timeLineCurMode && TIME_LINE_MULTI != pSelect->timeLineCurMode))) {
          pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                         "%s function requires valid time series input", pFunc->functionName);
          return DEAL_RES_ERROR;
        }
    */

    SNode*     pPrimaryKey = NULL;
    SSHashObj* pTableAlias = NULL;
    nodesWalkExprs(pFunc->pParameterList, collectTableAlias, &pTableAlias);
    if (NULL == pTableAlias) {
      pCxt->errCode = tranCreatePrimaryKeyCol(pCxt, NULL, &pPrimaryKey);
    } else {
      if (tSimpleHashGetSize(pTableAlias) > 1) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TIMELINE_FUNC);
      }
      char*   tableAlias = NULL;
      int32_t iter = 0;
      tableAlias = tSimpleHashIterate(pTableAlias, tableAlias, &iter);
      pCxt->errCode = tranCreatePrimaryKeyCol(pCxt, tableAlias, &pPrimaryKey);
      tSimpleHashCleanup(pTableAlias);
    }
    if (TSDB_CODE_SUCCESS == pCxt->errCode) {
      pCxt->errCode = nodesListMakeStrictAppend(&pFunc->pParameterList, pPrimaryKey);
    }
    return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t appendTsForImplicitTsFunc(STranslateContext* pCxt, SSelectStmt* pSelect) {
  nodesWalkSelectStmt(pSelect, SQL_CLAUSE_FROM, appendTsForImplicitTsFuncImpl, pCxt);
  return pCxt->errCode;
}

static int32_t createPkColByTable(STranslateContext* pCxt, SRealTableNode* pTable, SNode** pPk) {
  SColumnNode* pCol = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  pCol->colId = pTable->pMeta->schema[1].colId;
  tstrncpy(pCol->colName, pTable->pMeta->schema[1].name, TSDB_COL_NAME_LEN);
  bool found = false;
  code = findAndSetColumn(pCxt, &pCol, (STableNode*)pTable, &found, true);
  if (TSDB_CODE_SUCCESS != code || !found) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTERNAL_ERROR);
  }
  *pPk = (SNode*)pCol;
  return TSDB_CODE_SUCCESS;
}

static EDealRes hasPkColImpl(SNode* pNode, void* pContext) {
  if (nodeType(pNode) == QUERY_NODE_COLUMN && ((SColumnNode*)pNode)->tableHasPk) {
    *(bool*)pContext = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static bool hasPkCol(SNode* pNode) {
  bool hasPk = false;
  nodesWalkExprPostOrder(pNode, hasPkColImpl, &hasPk);
  return hasPk;
}

static EDealRes appendPkForPkFuncImpl(SNode* pNode, void* pContext) {
  STranslateContext* pCxt = pContext;
  STableNode*        pTable = NULL;
  int32_t            code = findTable(pCxt, NULL, &pTable);
  if (TSDB_CODE_SUCCESS == code && QUERY_NODE_REAL_TABLE == nodeType(pTable) && isPkFunc(pNode) && hasPkCol(pNode)) {
    SFunctionNode*  pFunc = (SFunctionNode*)pNode;
    SRealTableNode* pRealTable = (SRealTableNode*)pTable;
    SNode*          pPk = NULL;
    pCxt->errCode = createPkColByTable(pCxt, pRealTable, &pPk);
    if (TSDB_CODE_SUCCESS == pCxt->errCode) {
      pCxt->errCode = nodesListMakeStrictAppend(&pFunc->pParameterList, pPk);
    }
    if (TSDB_CODE_SUCCESS == pCxt->errCode) {
      pFunc->hasPk = true;
      pFunc->pkBytes = ((SColumnNode*)pPk)->node.resType.bytes;
    }
    return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t appendPkParamForPkFunc(STranslateContext* pCxt, SSelectStmt* pSelect) {
  nodesWalkSelectStmt(pSelect, SQL_CLAUSE_FROM, appendPkForPkFuncImpl, pCxt);
  return pCxt->errCode;
}

typedef struct SReplaceOrderByAliasCxt {
  STranslateContext* pTranslateCxt;
  SNodeList*         pProjectionList;
  bool               nameMatch;
  bool               notFound;
} SReplaceOrderByAliasCxt;

static EDealRes replaceOrderByAliasImpl(SNode** pNode, void* pContext) {
  SReplaceOrderByAliasCxt* pCxt = pContext;
  SNodeList*               pProjectionList = pCxt->pProjectionList;
  SNode*                   pProject = NULL;
  if (QUERY_NODE_COLUMN == nodeType(*pNode)) {
    FOREACH(pProject, pProjectionList) {
      SExprNode* pExpr = (SExprNode*)pProject;
      if (0 == strcmp(((SColumnNode*)*pNode)->colName, pExpr->userAlias)) {
        if (!pCxt->nameMatch && (nodeType(*pNode) != nodeType(pProject) ||
                                 (QUERY_NODE_COLUMN == nodeType(pProject) && !nodesEqualNode(*pNode, pProject)))) {
          continue;
        }
        SNode*  pNew = NULL;
        int32_t code = nodesCloneNode(pProject, &pNew);
        if (NULL == pNew) {
          pCxt->pTranslateCxt->errCode = code;
          return DEAL_RES_ERROR;
        }
        nodesDestroyNode(*pNode);
        *pNode = pNew;
        return DEAL_RES_CONTINUE;
      }
    }

    pCxt->notFound = true;
    if (pCxt->nameMatch) {
      // when nameMatch is true, columns MUST be found in projection list!
      return generateDealNodeErrMsg(pCxt->pTranslateCxt, TSDB_CODE_PAR_ORDERBY_UNKNOWN_EXPR, ((SColumnNode*)*pNode)->colName);
    }
  } else if (QUERY_NODE_ORDER_BY_EXPR == nodeType(*pNode)) {
    STranslateContext* pTransCxt = pCxt->pTranslateCxt;
    SNode*             pExpr = ((SOrderByExprNode*)*pNode)->pExpr;
    if (QUERY_NODE_VALUE == nodeType(pExpr)) {
      SValueNode* pVal = (SValueNode*)pExpr;
      if (DEAL_RES_ERROR == translateValue(pTransCxt, pVal)) {
        return pTransCxt->errCode;
      }
      int32_t pos = getPositionValue(pVal);
      if (0 < pos && pos <= LIST_LENGTH(pProjectionList)) {
        SNode*  pNew = NULL;
        int32_t code = nodesCloneNode(nodesListGetNode(pProjectionList, pos - 1), &pNew);
        if (NULL == pNew) {
          pCxt->pTranslateCxt->errCode = code;
          return DEAL_RES_ERROR;
        }
        ((SOrderByExprNode*)*pNode)->pExpr = pNew;
        nodesDestroyNode(pExpr);
        return DEAL_RES_CONTINUE;
      }
    }
  }

  return DEAL_RES_CONTINUE;
}

static int32_t replaceOrderByAlias(STranslateContext* pCxt, SNodeList* pProjectionList, SNodeList* pOrderByList,
                                   bool checkExists, bool nameMatch) {
  if (NULL == pOrderByList) {
    return TSDB_CODE_SUCCESS;
  }
  SReplaceOrderByAliasCxt cxt = {
      .pTranslateCxt = pCxt, .pProjectionList = pProjectionList, .nameMatch = nameMatch, .notFound = false};
  nodesRewriteExprsPostOrder(pOrderByList, replaceOrderByAliasImpl, &cxt);
  if (checkExists && cxt.notFound) {
    return TSDB_CODE_PAR_ORDERBY_UNKNOWN_EXPR;
  }

  return pCxt->errCode;
}

static void resetResultTimeline(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pOrderByList) {
    if (inStreamCalcClause(pCxt)) {
      SNode* pNode = nodesListGetNode(pSelect->pProjectionList, 0);
      if (pNode && QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsPlaceHolderFunc(((SFunctionNode*)pNode)->funcId) && isPrimaryKeyImpl(pNode)) {
        pSelect->timeLineResMode = TIME_LINE_GLOBAL;
      }
    }
    
    return;
  }
  SNode* pOrder = ((SOrderByExprNode*)nodesListGetNode(pSelect->pOrderByList, 0))->pExpr;
  if ((QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) && isPrimaryKeyImpl(pOrder)) ||
      (QUERY_NODE_TEMP_TABLE != nodeType(pSelect->pFromTable) && isPrimaryKeyImpl(pOrder))) {
    pSelect->timeLineResMode = TIME_LINE_GLOBAL;
    return;
  } else if (pSelect->pOrderByList->length > 1) {
    for (int32_t i = 1; i < pSelect->pOrderByList->length; ++i) {
      pOrder = ((SOrderByExprNode*)nodesListGetNode(pSelect->pOrderByList, i))->pExpr;
      if (isPrimaryKeyImpl(pOrder)) {
        pSelect->timeLineResMode = TIME_LINE_MULTI;
        pSelect->timeLineFromOrderBy = true;
        return;
      }
    }
  }

  pSelect->timeLineResMode = TIME_LINE_NONE;
}

static int32_t replaceOrderByAliasForSelect(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = replaceOrderByAlias(pCxt, pSelect->pProjectionList, pSelect->pOrderByList, false, false);
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    resetResultTimeline(pCxt, pSelect);
  }
  return code;
}

static int32_t translateSelectWithoutFrom(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->pCurrStmt = (SNode*)pSelect;
  pCxt->currClause = SQL_CLAUSE_SELECT;
  pCxt->dual = true;
  return translateExprList(pCxt, pSelect->pProjectionList);
}
typedef struct SCheckColsFuncCxt {
  bool        hasColsFunc;
  SNodeList** selectFuncList;
  int32_t     status;
  ESqlClause  clause;
} SCheckColsFuncCxt;

static bool isColsFuncByName(SFunctionNode* pFunc) {
  if (strcasecmp(pFunc->functionName, "cols") != 0) {
    return false;
  }
  return true;
}

static bool isMultiColsFuncNode(SNode* pNode) {
  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (isColsFuncByName(pFunc)) {
      if (pFunc->pParameterList->length > 2) {
        return true;
      }
    }
  }
  return false;
}

typedef struct SBindTupleFuncCxt {
  SNode*  root;
  int32_t bindExprID;
} SBindTupleFuncCxt;

static EDealRes pushDownBindSelectFunc(SNode** pNode, void* pContext) {
  SBindTupleFuncCxt* pCxt = pContext;
  if (nodesIsExprNode(*pNode)) {
    SExprNode* pExpr = (SExprNode*)*pNode;
    pExpr->relatedTo = pCxt->bindExprID;
    if (nodeType(*pNode) != QUERY_NODE_COLUMN) {
      return DEAL_RES_CONTINUE;
    }

    if (*pNode != pCxt->root) {
      int len = strlen(pExpr->aliasName);
      if (len + TSDB_COL_NAME_EXLEN >= TSDB_COL_NAME_LEN) {
        char buffer[TSDB_COL_NAME_EXLEN + TSDB_COL_NAME_LEN + 1] = {0};
        (void)tsnprintf(buffer, sizeof(buffer), "%s.%d", pExpr->aliasName, pExpr->relatedTo);
        uint64_t hashVal = MurmurHash3_64(buffer, TSDB_COL_NAME_EXLEN + TSDB_COL_NAME_LEN + 1);
        (void)tsnprintf(pExpr->aliasName, TSDB_COL_NAME_EXLEN, "%" PRIu64, hashVal);
      } else {
        (void)tsnprintf(pExpr->aliasName + len, TSDB_COL_NAME_EXLEN, ".%d", pExpr->relatedTo);
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t getSelectFuncIndex(SNodeList* FuncNodeList, SNode* pSelectFunc) {
  SNode*  pNode = NULL;
  int32_t selectFuncIndex = 0;
  FOREACH(pNode, FuncNodeList) {
    ++selectFuncIndex;
    if (nodesEqualNode(pNode, pSelectFunc)) {
      return selectFuncIndex;
    }
  }
  return 0;
}

static EDealRes checkHasColsFunc(SNode** pNode, void* pContext) {
  if (QUERY_NODE_FUNCTION == nodeType(*pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)*pNode;
    if (isColsFuncByName(pFunc)) {
      *(bool*)pContext = true;
      return DEAL_RES_END;
    }
  }
  return DEAL_RES_CONTINUE;
}

static bool isStarColumn(SNode* pNode) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (strcmp(pCol->colName, "*") == 0) {
      return true;
    }
  }
  return false;
}

static int32_t checkMultColsFuncParam(SNodeList* pParameterList) {
  if (!pParameterList || pParameterList->length < 2) {
    return TSDB_CODE_PAR_INVALID_COLS_FUNCTION;
  }
  int32_t index = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pParameterList) {
    if (index == 0) {  // the first parameter is select function
      if (QUERY_NODE_FUNCTION != nodeType(pNode) || isColsFuncByName((SFunctionNode*)pNode)) {
        return TSDB_CODE_PAR_INVALID_COLS_FUNCTION;
      }
      SFunctionNode* pFunc = (SFunctionNode*)pNode;
      // pFunc->funcId is zero at here, so need to check at * step
      // if(!fmIsSelectFunc(pFunc->funcId)) {
      //   return TSDB_CODE_PAR_INVALID_COLS_FUNCTION;
      // }
      SNode* pTmpNode = NULL;
      FOREACH(pTmpNode, pFunc->pParameterList) {
        bool hasColsFunc = false;
        nodesRewriteExpr(&pTmpNode, checkHasColsFunc, (void*)&hasColsFunc);
        if (hasColsFunc) {
          return TSDB_CODE_PAR_INVALID_COLS_FUNCTION;
        }
      }
    } else {
      bool hasColsFunc = false;
      nodesRewriteExpr(&pNode, checkHasColsFunc, &hasColsFunc);
      if (hasColsFunc) {
        return TSDB_CODE_PAR_INVALID_COLS_FUNCTION;
      }
    }
    ++index;
  }
  return TSDB_CODE_SUCCESS;
}

static EDealRes rewriteSingleColsFunc(SNode** pNode, void* pContext) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (QUERY_NODE_FUNCTION != nodeType(*pNode)) {
    return DEAL_RES_CONTINUE;
  }
  SCheckColsFuncCxt* pCxt = pContext;
  SFunctionNode*     pFunc = (SFunctionNode*)*pNode;
  if (isColsFuncByName(pFunc)) {
    if (pFunc->pParameterList->length > 2) {
      pCxt->status = TSDB_CODE_PAR_INVALID_COLS_FUNCTION;
      return DEAL_RES_ERROR;
    }
    SNode* pSelectFunc = nodesListGetNode(pFunc->pParameterList, 0);
    SNode* pExpr = nodesListGetNode(pFunc->pParameterList, 1);
    if (nodeType(pSelectFunc) != QUERY_NODE_FUNCTION || isColsFuncByName((SFunctionNode*)pSelectFunc)) {
      pCxt->status = TSDB_CODE_PAR_INVALID_COLS_SELECTFUNC;
      parserError("%s Invalid cols function, the first parameter must be a select function", __func__);
      return DEAL_RES_ERROR;
    }
    if (pCxt->clause != SQL_CLAUSE_SELECT && isStarColumn(pExpr)) {
      pCxt->status = TSDB_CODE_PAR_INVALID_COLS_FUNCTION;
      parserError("%s Invalid cols function, the parameters '*' is invalid.", __func__);
      return DEAL_RES_ERROR;
    }
    if (pFunc->node.asAlias) {
      if (((SExprNode*)pExpr)->asAlias) {
        pCxt->status = TSDB_CODE_PAR_INVALID_COLS_ALIAS;
        parserError("%s Invalid using alias for cols function", __func__);
        return DEAL_RES_ERROR;
      } else {
        ((SExprNode*)pExpr)->asAlias = true;
        tstrncpy(((SExprNode*)pExpr)->userAlias, pFunc->node.userAlias, TSDB_COL_NAME_LEN);
      }
    }
    if (*pCxt->selectFuncList == NULL) {
      code = nodesMakeList(pCxt->selectFuncList);
      if (NULL == *pCxt->selectFuncList) {
        pCxt->status = code;
        return DEAL_RES_ERROR;
      }
    }
    int32_t selectFuncCount = (*pCxt->selectFuncList)->length;
    int32_t selectFuncIndex = getSelectFuncIndex(*pCxt->selectFuncList, pSelectFunc);
    if (selectFuncIndex == 0) {
      ++selectFuncCount;
      selectFuncIndex = selectFuncCount;
      SNode* pNewNode = NULL;
      code = nodesCloneNode(pSelectFunc, &pNewNode);
      if (code) goto _end;
      ((SExprNode*)pNewNode)->bindExprID = selectFuncIndex;
      code = nodesListMakeStrictAppend(pCxt->selectFuncList, pNewNode);
      if (code) goto _end;
    }

    SNode* pNewNode = NULL;
    code = nodesCloneNode(pExpr, &pNewNode);
    if (code) goto _end;
    if (nodesIsExprNode(pNewNode)) {
      SBindTupleFuncCxt pCxt = {pNewNode, selectFuncIndex};
      nodesRewriteExpr(&pNewNode, pushDownBindSelectFunc, &pCxt);
    } else {
      pCxt->status = TSDB_CODE_PAR_INVALID_COLS_FUNCTION;
      parserError("%s Invalid cols function, the first parameter must be a select function", __func__);
      return DEAL_RES_ERROR;
    }
    nodesDestroyNode(*pNode);
    *pNode = pNewNode;
  }
  return DEAL_RES_CONTINUE;
_end:
  pCxt->status = code;
  return DEAL_RES_ERROR;
}

static int32_t rewriteHavingColsNode(STranslateContext* pCxt, SNode** pNode, SNodeList** selectFuncList) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (!pNode || *pNode == NULL) return code;
  if (isMultiColsFuncNode(*pNode)) {
    parserWarn("%s Invalid using multi cols func in having.", __func__);
    return TSDB_CODE_PAR_INVALID_COLS_FUNCTION;
  } else {
    SCheckColsFuncCxt pSelectFuncCxt = {false, selectFuncList, TSDB_CODE_SUCCESS};
    nodesRewriteExpr(pNode, rewriteSingleColsFunc, &pSelectFuncCxt);
    if (pSelectFuncCxt.status != TSDB_CODE_SUCCESS) {
      return pSelectFuncCxt.status;
    }
  }
  return code;
}

static int32_t rewriteColsFunction(STranslateContext* pCxt, ESqlClause clause, SNodeList** nodeList,
                                   SNodeList** selectFuncList) {
  int32_t code = TSDB_CODE_SUCCESS;
  bool    needRewrite = false;
  SNode** pNode = NULL;
  FOREACH_FOR_REWRITE(pNode, *nodeList) {
    if (isMultiColsFuncNode(*pNode)) {
      code = checkMultColsFuncParam(((SFunctionNode*)*pNode)->pParameterList);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      needRewrite = true;
    } else {
      SCheckColsFuncCxt pSelectFuncCxt = {false, selectFuncList, TSDB_CODE_SUCCESS, clause};
      nodesRewriteExpr(pNode, rewriteSingleColsFunc, &pSelectFuncCxt);
      if (pSelectFuncCxt.status != TSDB_CODE_SUCCESS) {
        return pSelectFuncCxt.status;
      }
    }
  }

  SNodeList* pNewNodeList = NULL;
  SNode*     pNewNode = NULL;
  if (needRewrite) {
    code = nodesMakeList(&pNewNodeList);
    if (NULL == pNewNodeList) {
      return code;
    }
    if (*selectFuncList == NULL) {
      code = nodesMakeList(selectFuncList);
      if (NULL == *selectFuncList) {
        nodesDestroyList(pNewNodeList);
        return code;
      }
    }

    int32_t nums = 0;
    int32_t selectFuncCount = (*selectFuncList)->length;
    SNode*  pTmpNode = NULL;
    FOREACH(pTmpNode, *nodeList) {
      if (isMultiColsFuncNode(pTmpNode)) {
        SFunctionNode* pFunc = (SFunctionNode*)pTmpNode;
        if (pFunc->node.asAlias) {
          code = TSDB_CODE_PAR_INVALID_COLS_ALIAS;
          parserError("%s Invalid using alias for cols function", __func__);
          goto _end;
        }

        SNode* pSelectFunc = nodesListGetNode(pFunc->pParameterList, 0);
        if (nodeType(pSelectFunc) != QUERY_NODE_FUNCTION) {
          code = TSDB_CODE_PAR_INVALID_COLS_FUNCTION;
          parserError("%s Invalid cols function, the first parameter must be a select function", __func__);
          goto _end;
        }
        int32_t selectFuncIndex = getSelectFuncIndex(*selectFuncList, pSelectFunc);
        if (selectFuncIndex == 0) {
          ++selectFuncCount;
          selectFuncIndex = selectFuncCount;
          code = nodesCloneNode(pSelectFunc, &pNewNode);
          if (TSDB_CODE_SUCCESS != code) goto _end;
          ((SExprNode*)pNewNode)->bindExprID = selectFuncIndex;
          code = nodesListMakeStrictAppend(selectFuncList, pNewNode);
          if (TSDB_CODE_SUCCESS != code) goto _end;
        }
        // start from index 1, because the first parameter is select function which needn't to output.
        for (int i = 1; i < pFunc->pParameterList->length; ++i) {
          SNode* pExpr = nodesListGetNode(pFunc->pParameterList, i);

          if (clause != SQL_CLAUSE_SELECT && isStarColumn(pExpr)) {
            code = TSDB_CODE_PAR_INVALID_COLS_FUNCTION;
            parserError("%s Invalid cols function, the parameters '*' is invalid.", __func__);
            goto _end;
          }

          code = nodesCloneNode(pExpr, &pNewNode);
          if (TSDB_CODE_SUCCESS != code) goto _end;
          if (nodesIsExprNode(pNewNode)) {
            SBindTupleFuncCxt pCxt = {pNewNode, selectFuncIndex};
            nodesRewriteExpr(&pNewNode, pushDownBindSelectFunc, &pCxt);
          } else {
            code = TSDB_CODE_PAR_INVALID_COLS_FUNCTION;
            parserError("%s Invalid cols function, the first parameter must be a select function", __func__);
            goto _end;
          }
          if (TSDB_CODE_SUCCESS != code) goto _end;
          code = nodesListMakeStrictAppend(&pNewNodeList, pNewNode);
          if (TSDB_CODE_SUCCESS != code) goto _end;
        }
        continue;
      }

      code = nodesCloneNode(pTmpNode, &pNewNode);
      if (TSDB_CODE_SUCCESS != code) goto _end;
      code = nodesListMakeStrictAppend(&pNewNodeList, pNewNode);
      if (TSDB_CODE_SUCCESS != code) goto _end;
    }
    nodesDestroyList(*nodeList);
    *nodeList = pNewNodeList;
    return TSDB_CODE_SUCCESS;
  }
_end:
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNewNode);
    nodesDestroyList(pNewNodeList);
  }
  return code;
}

static int32_t translateSelectFrom(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->pCurrStmt = (SNode*)pSelect;
  pCxt->dual = false;
  int32_t code = translateFrom(pCxt, &pSelect->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    pSelect->precision = ((STableNode*)pSelect->pFromTable)->precision;
    code = translateWhere(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setJoinTimeLineResMode(pCxt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translatePartitionBy(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateWindow(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateGroupBy(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateHaving(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code && pCxt->pParseCxt->biMode != 0) {
    code = biRewriteSelectStar(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateSelectList(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkHavingGroupBy(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateOrderBy(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkIsEmptyResult(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = resetSelectFuncNumWithoutDup(pSelect);
    if (TSDB_CODE_SUCCESS == code) code = checkAggColCoexist(pCxt, pSelect);
  }
  /*
    if (TSDB_CODE_SUCCESS == code) {
      code = checkWinJoinAggColCoexist(pCxt, pSelect);
    }
  */
  if (TSDB_CODE_SUCCESS == code) {
    code = checkWindowGrpFuncCoexist(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkLimit(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateInterp(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateForecast(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = appendTsForImplicitTsFunc(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = appendPkParamForPkFunc(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceOrderByAliasForSelect(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setTableCacheLastMode(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceTbName(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (pSelect->pPartitionByList) {
      code = removeConstantValueFromList(&pSelect->pPartitionByList);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (pSelect->pGroupByList) {
      code = removeConstantValueFromList(&pSelect->pGroupByList);
    }
  }
  return code;
}

static int32_t translateSelect(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (pCxt->pParseCxt && pCxt->pParseCxt->setQueryFp) {
    (*pCxt->pParseCxt->setQueryFp)(pCxt->pParseCxt->requestRid);
  }

  if (NULL == pSelect->pFromTable) {
    return translateSelectWithoutFrom(pCxt, pSelect);
  } else {
    return translateSelectFrom(pCxt, pSelect);
  }
}

static SNode* createSetOperProject(const char* pTableAlias, SNode* pNode) {
  SColumnNode* pCol = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (TSDB_CODE_SUCCESS != code) {
    return NULL;
  }
  pCol->node.resType = ((SExprNode*)pNode)->resType;
  snprintf(pCol->tableAlias, sizeof(pCol->tableAlias), "%s", pTableAlias);
  snprintf(pCol->colName, sizeof(pCol->colName), "%s", ((SExprNode*)pNode)->aliasName);
  snprintf(pCol->node.aliasName, sizeof(pCol->node.aliasName), "%s", pCol->colName);
  snprintf(pCol->node.userAlias, sizeof(pCol->node.userAlias), "%s", ((SExprNode*)pNode)->userAlias);
  return (SNode*)pCol;
}

static bool isUionOperator(SNode* pNode) {
  return QUERY_NODE_SET_OPERATOR == nodeType(pNode) && (((SSetOperator*)pNode)->opType == SET_OP_TYPE_UNION ||
                                                        ((SSetOperator*)pNode)->opType == SET_OP_TYPE_UNION_ALL);
}

static int32_t pushdownCastForUnion(STranslateContext* pCxt, SNode* pNode, SExprNode* pExpr, int pos) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (isUionOperator(pNode)) {
    SSetOperator* pSetOperator = (SSetOperator*)pNode;
    SNodeList*    pLeftProjections = getProjectList(pSetOperator->pLeft);
    SNodeList*    pRightProjections = getProjectList(pSetOperator->pRight);
    if (LIST_LENGTH(pLeftProjections) != LIST_LENGTH(pRightProjections)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INCORRECT_NUM_OF_COL);
    }

    SNode*  pLeft = NULL;
    SNode*  pRight = NULL;
    int32_t index = 0;
    FORBOTH(pLeft, pLeftProjections, pRight, pRightProjections) {
      ++index;
      if (index < pos) {
        continue;
      }
      SNode* pRightFunc = NULL;
      code = createCastFunc(pCxt, pRight, pExpr->resType, &pRightFunc);
      if (TSDB_CODE_SUCCESS != code || NULL == pRightFunc) {
        return code;
      }
      REPLACE_LIST2_NODE(pRightFunc);
      code = pushdownCastForUnion(pCxt, pSetOperator->pRight, (SExprNode*)pRightFunc, index);
      if (TSDB_CODE_SUCCESS != code) return code;

      SNode* pLeftFunc = NULL;
      code = createCastFunc(pCxt, pLeft, pExpr->resType, &pLeftFunc);
      if (TSDB_CODE_SUCCESS != code || NULL == pLeftFunc) {
        return code;
      }
      REPLACE_LIST1_NODE(pLeftFunc);
      code = pushdownCastForUnion(pCxt, pSetOperator->pLeft, (SExprNode*)pLeftFunc, index);
      if (TSDB_CODE_SUCCESS != code) return code;
      break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSetOperProject(STranslateContext* pCxt, SSetOperator* pSetOperator) {
  SNodeList* pLeftProjections = getProjectList(pSetOperator->pLeft);
  SNodeList* pRightProjections = getProjectList(pSetOperator->pRight);
  if (LIST_LENGTH(pLeftProjections) != LIST_LENGTH(pRightProjections)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INCORRECT_NUM_OF_COL);
  }

  SNode*  pLeft = NULL;
  SNode*  pRight = NULL;
  int32_t index = 0;
  FORBOTH(pLeft, pLeftProjections, pRight, pRightProjections) {
    SExprNode* pLeftExpr = (SExprNode*)pLeft;
    SExprNode* pRightExpr = (SExprNode*)pRight;
    ++index;
    int32_t comp = dataTypeComp(&pLeftExpr->resType, &pRightExpr->resType);
    if (comp > 0) {
      SNode*  pRightFunc = NULL;
      int32_t code = createCastFunc(pCxt, pRight, pLeftExpr->resType, &pRightFunc);
      if (TSDB_CODE_SUCCESS != code || NULL == pRightFunc) {  // deal scan coverity
        return code;
      }
      REPLACE_LIST2_NODE(pRightFunc);
      pRightExpr = (SExprNode*)pRightFunc;
      code = pushdownCastForUnion(pCxt, pSetOperator->pRight, pRightExpr, index);
      if (TSDB_CODE_SUCCESS != code) return code;
    } else if (comp < 0) {
      SNode*  pLeftFunc = NULL;
      int32_t code = createCastFunc(pCxt, pLeft, pRightExpr->resType, &pLeftFunc);
      if (TSDB_CODE_SUCCESS != code || NULL == pLeftFunc) {  // deal scan coverity
        return code;
      }
      REPLACE_LIST1_NODE(pLeftFunc);
      SExprNode* pLeftFuncExpr = (SExprNode*)pLeftFunc;
      snprintf(pLeftFuncExpr->aliasName, sizeof(pLeftFuncExpr->aliasName), "%s", pLeftExpr->aliasName);
      snprintf(pLeftFuncExpr->userAlias, sizeof(pLeftFuncExpr->userAlias), "%s", pLeftExpr->userAlias);
      pLeft = pLeftFunc;
      pLeftExpr = pLeftFuncExpr;
      code = pushdownCastForUnion(pCxt, pSetOperator->pLeft, pLeftExpr, index);
      if (TSDB_CODE_SUCCESS != code) return code;
    }
    snprintf(pRightExpr->aliasName, sizeof(pRightExpr->aliasName), "%s", pLeftExpr->aliasName);
    SNode* pProj = createSetOperProject(pSetOperator->stmtName, pLeft);
    bool   isLeftPrimTs = isPrimaryKeyImpl(pLeft);
    bool   isRightPrimTs = isPrimaryKeyImpl(pRight);

    if (isLeftPrimTs && isRightPrimTs) {
      SColumnNode* pFCol = (SColumnNode*)pProj;
      pFCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
      pFCol->isPrimTs = true;
    }
    if (TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pSetOperator->pProjectionList, pProj)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static uint8_t calcSetOperatorPrecision(SSetOperator* pSetOperator) {
  return calcPrecision(getStmtPrecision(pSetOperator->pLeft), getStmtPrecision(pSetOperator->pRight));
}

static int32_t translateSetOperOrderBy(STranslateContext* pCxt, SSetOperator* pSetOperator) {
  if (NULL == pSetOperator->pOrderByList || pSetOperator->pOrderByList->length <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  bool    other;
  int32_t code = translateClausePosition(pCxt, pSetOperator->pProjectionList, pSetOperator->pOrderByList, &other);
  if (TSDB_CODE_SUCCESS == code) {
    if (other) {
      pCxt->currClause = SQL_CLAUSE_ORDER_BY;
      pCxt->pCurrStmt = (SNode*)pSetOperator;
      code = translateExprList(pCxt, pSetOperator->pOrderByList);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceOrderByAlias(pCxt, pSetOperator->pProjectionList, pSetOperator->pOrderByList, true, true);
  }
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pOrder = ((SOrderByExprNode*)nodesListGetNode(pSetOperator->pOrderByList, 0))->pExpr;
    if (isPrimaryKeyImpl(pOrder)) {
      pSetOperator->timeLineResMode = TIME_LINE_GLOBAL;
      return code;
    } else if (pSetOperator->pOrderByList->length > 1) {
      for (int32_t i = 1; i < pSetOperator->pOrderByList->length; ++i) {
        pOrder = ((SOrderByExprNode*)nodesListGetNode(pSetOperator->pOrderByList, i))->pExpr;
        if (isPrimaryKeyImpl(pOrder)) {
          pSetOperator->timeLineResMode = TIME_LINE_MULTI;
          pSetOperator->timeLineFromOrderBy = true;
          return code;
        }
      }
    }

    pSetOperator->timeLineResMode = TIME_LINE_NONE;
  }
  return code;
}

static int32_t checkSetOperLimit(STranslateContext* pCxt, SLimitNode* pLimit) {
  int32_t code = 0;
  if (pLimit && pLimit->limit) {
    code = translateExpr(pCxt, (SNode**)&pLimit->limit);
  }
  if (TSDB_CODE_SUCCESS == code && pLimit && pLimit->offset) {
    code = translateExpr(pCxt, (SNode**)&pLimit->offset);
  }
  if (TSDB_CODE_SUCCESS == code && (NULL != pLimit && NULL != pLimit->offset && pLimit->offset->datum.i < 0)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OFFSET_LESS_ZERO);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSetOperator(STranslateContext* pCxt, SSetOperator* pSetOperator) {
  if (pCxt->pParseCxt && pCxt->pParseCxt->setQueryFp) {
    (*pCxt->pParseCxt->setQueryFp)(pCxt->pParseCxt->requestRid);
  }

  bool    hasTrows = false;
  int32_t code = TSDB_CODE_SUCCESS;
  PAR_ERR_RET(translateQuery(pCxt, pSetOperator->pLeft));

  if (inStreamCalcClause(pCxt)) {
    hasTrows = BIT_FLAG_TEST_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS);
    BIT_FLAG_UNSET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS);
  }

  PAR_ERR_RET(resetHighLevelTranslateNamespace(pCxt));
  PAR_ERR_RET(translateQuery(pCxt, pSetOperator->pRight));

  if (inStreamCalcClause(pCxt) && hasTrows) {
    if (BIT_FLAG_TEST_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS)) {
      PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_QUERY,
                                          "%%%%trows can not appear multi times in union query"));
    } else {
      BIT_FLAG_SET_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS);
    }
  }

  pSetOperator->joinContains = getBothJoinContais(pSetOperator->pLeft, pSetOperator->pRight);
  pSetOperator->precision = calcSetOperatorPrecision(pSetOperator);

  PAR_ERR_RET(translateSetOperProject(pCxt, pSetOperator));
  PAR_ERR_RET(translateSetOperOrderBy(pCxt, pSetOperator));
  PAR_ERR_RET(checkSetOperLimit(pCxt, (SLimitNode*)pSetOperator->pLimit));

  return code;
}

static int32_t partitionDeleteWhere(STranslateContext* pCxt, SDeleteStmt* pDelete) {
  if (NULL == pDelete->pWhere) {
    TAOS_SET_OBJ_ALIGNED(&pDelete->timeRange, TSWINDOW_INITIALIZER);
    return TSDB_CODE_SUCCESS;
  }
  SNode*  pPrimaryKeyCond = NULL;
  SNode*  pOtherCond = NULL;
  int32_t code = filterPartitionCond(&pDelete->pWhere, &pPrimaryKeyCond, NULL, &pDelete->pTagCond, &pOtherCond);
  if (TSDB_CODE_SUCCESS == code && NULL != pOtherCond) {
    code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DELETE_WHERE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    bool isStrict = false;
    if (NULL != pPrimaryKeyCond) {
      code = getTimeRange(&pPrimaryKeyCond, &pDelete->timeRange, &isStrict);
      if (TSDB_CODE_SUCCESS == code && !isStrict) {
        code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DELETE_WHERE);
      }
    } else {
      TAOS_SET_OBJ_ALIGNED(&pDelete->timeRange, TSWINDOW_INITIALIZER);
    }
  }
  nodesDestroyNode(pPrimaryKeyCond);
  nodesDestroyNode(pOtherCond);
  return code;
}

static int32_t translateDeleteWhere(STranslateContext* pCxt, SDeleteStmt* pDelete) {
  pCxt->currClause = SQL_CLAUSE_WHERE;
  int32_t code = translateExpr(pCxt, &pDelete->pWhere);
  if (TSDB_CODE_SUCCESS == code) {
    code = partitionDeleteWhere(pCxt, pDelete);
  }
  return code;
}

static int32_t translateDelete(STranslateContext* pCxt, SDeleteStmt* pDelete) {
  const char* dbName = ((STableNode*)pDelete->pFromTable)->dbName;
  if (IS_SYS_DBNAME(dbName)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION,
                                   "Cannot delete from system database: `%s`", dbName);
  }
  pCxt->pCurrStmt = (SNode*)pDelete;
  int32_t code = translateFrom(pCxt, &pDelete->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    pDelete->precision = ((STableNode*)pDelete->pFromTable)->precision;
    code = translateDeleteWhere(pCxt, pDelete);
  }
  pCxt->currClause = SQL_CLAUSE_SELECT;
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExpr(pCxt, &pDelete->pCountFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExpr(pCxt, &pDelete->pFirstFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExpr(pCxt, &pDelete->pLastFunc);
  }
  return code;
}

static int32_t translateInsertCols(STranslateContext* pCxt, SInsertStmt* pInsert) {
  if (NULL == pInsert->pCols) {
    return createAllColumns(pCxt, false, &pInsert->pCols);
  }
  return translateExprList(pCxt, pInsert->pCols);
}

static int32_t translateInsertQuery(STranslateContext* pCxt, SInsertStmt* pInsert) {
  int32_t code = resetTranslateNamespace(pCxt);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateQuery(pCxt, pInsert->pQuery);
  }
  return code;
}

static int32_t addOrderByPrimaryKeyToQueryImpl(STranslateContext* pCxt, SNode* pPrimaryKeyExpr,
                                               SNodeList** pOrderByList) {
  SOrderByExprNode* pOrderByExpr = NULL;
  int32_t           code = nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrderByExpr);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  pOrderByExpr->nullOrder = NULL_ORDER_FIRST;
  pOrderByExpr->order = ORDER_ASC;
  code = nodesCloneNode(pPrimaryKeyExpr, &pOrderByExpr->pExpr);
  if (NULL == pOrderByExpr->pExpr) {
    nodesDestroyNode((SNode*)pOrderByExpr);
    return code;
  }
  // NODES_DESTORY_LIST(*pOrderByList);
  return nodesListMakeStrictAppend(pOrderByList, (SNode*)pOrderByExpr);
}

static int32_t addOrderByPrimaryKeyToQuery(STranslateContext* pCxt, SNode* pPrimaryKeyExpr, SNode* pStmt) {
  SNodeList* pOrederList = ((SSelectStmt*)pStmt)->pOrderByList;
  if (pOrederList && pOrederList->length > 0) {
    return TSDB_CODE_SUCCESS;
  }
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    return addOrderByPrimaryKeyToQueryImpl(pCxt, pPrimaryKeyExpr, &((SSelectStmt*)pStmt)->pOrderByList);
  }
  return addOrderByPrimaryKeyToQueryImpl(pCxt, pPrimaryKeyExpr, &((SSetOperator*)pStmt)->pOrderByList);
}

static int32_t translateInsertProject(STranslateContext* pCxt, SInsertStmt* pInsert) {
  SNodeList* pProjects = getProjectList(pInsert->pQuery);
  if (LIST_LENGTH(pInsert->pCols) != LIST_LENGTH(pProjects)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM, "Illegal number of columns");
  }

  SNode*  pPrimaryKeyExpr = NULL;
  SNode*  pBoundCol = NULL;
  SNode*  pProj = NULL;
  int16_t numOfTargetPKs = 0;
  int16_t numOfBoundPKs = 0;
  FORBOTH(pBoundCol, pInsert->pCols, pProj, pProjects) {
    SColumnNode* pCol = (SColumnNode*)pBoundCol;
    SExprNode*   pExpr = (SExprNode*)pProj;
    if (!dataTypeEqual(&pCol->node.resType, &pExpr->resType)) {
      SNode*  pFunc = NULL;
      int32_t code = createCastFunc(pCxt, pProj, pCol->node.resType, &pFunc);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      REPLACE_LIST2_NODE(pFunc);
      pExpr = (SExprNode*)pFunc;
    }
    snprintf(pExpr->aliasName, sizeof(pExpr->aliasName), "%s", pCol->colName);
    if (PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId) {
      pPrimaryKeyExpr = (SNode*)pExpr;
      numOfTargetPKs = pCol->numOfPKs;
    }
    if (pCol->isPk) ++numOfBoundPKs;
  }

  if (NULL == pPrimaryKeyExpr) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM,
                                   "Primary timestamp column should not be null");
  }

  if (numOfBoundPKs != numOfTargetPKs) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Primary key column should not be none");
  }

  return addOrderByPrimaryKeyToQuery(pCxt, pPrimaryKeyExpr, pInsert->pQuery);
}

static int32_t translateInsertTable(STranslateContext* pCxt, SNode** pTable) {
  int32_t code = translateFrom(pCxt, pTable);
  if (TSDB_CODE_SUCCESS == code && TSDB_CHILD_TABLE != ((SRealTableNode*)*pTable)->pMeta->tableType &&
      TSDB_SUPER_TABLE != ((SRealTableNode*)*pTable)->pMeta->tableType &&
      TSDB_NORMAL_TABLE != ((SRealTableNode*)*pTable)->pMeta->tableType) {
    // code = buildInvalidOperationMsg(&pCxt->msgBuf, "insert data into super table is not supported");
  }
  return code;
}
static int32_t translateInsert(STranslateContext* pCxt, SInsertStmt* pInsert) {
  pCxt->pCurrStmt = (SNode*)pInsert;
  int32_t code = translateInsertTable(pCxt, &pInsert->pTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateInsertCols(pCxt, pInsert);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateInsertQuery(pCxt, pInsert);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateInsertProject(pCxt, pInsert);
  }
  return code;
}

static int64_t getUnitPerMinute(uint8_t precision) {
  switch (precision) {
    case TSDB_TIME_PRECISION_MILLI:
      return MILLISECOND_PER_MINUTE;
    case TSDB_TIME_PRECISION_MICRO:
      return MILLISECOND_PER_MINUTE * 1000LL;
    case TSDB_TIME_PRECISION_NANO:
      return NANOSECOND_PER_MINUTE;
    default:
      break;
  }
  return MILLISECOND_PER_MINUTE;
}

static int64_t getBigintFromValueNode(SValueNode* pVal) {
  if (IS_DURATION_VAL(pVal->flag)) {
    return pVal->datum.i / getUnitPerMinute(pVal->node.resType.precision);
  }
  return pVal->datum.i;
}

static int32_t buildCreateDbRetentions(const SNodeList* pRetentions, SCreateDbReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL != pRetentions) {
    pReq->pRetensions = taosArrayInit(LIST_LENGTH(pRetentions), sizeof(SRetention));
    if (NULL == pReq->pRetensions) {
      return terrno;
    }
    SValueNode* pFreq = NULL;
    SValueNode* pKeep = NULL;
    SNode*      pNode = NULL;
    int32_t     index = 0;
    FOREACH(pNode, pRetentions) {
      pFreq = (SValueNode*)nodesListGetNode(((SNodeListNode*)pNode)->pNodeList, 0);
      pKeep = (SValueNode*)nodesListGetNode(((SNodeListNode*)pNode)->pNodeList, 1);
      SRetention retention = {
          .freq = pFreq->datum.i, .freqUnit = pFreq->unit, .keep = pKeep->datum.i, .keepUnit = pKeep->unit};
      if (NULL == taosArrayPush(pReq->pRetensions, &retention)) {
        code = terrno;
        break;
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      pReq->numOfRetensions = taosArrayGetSize(pReq->pRetensions);
    }
  }
  return code;
}

static int32_t buildCreateDbReq(STranslateContext* pCxt, SCreateDatabaseStmt* pStmt, SCreateDbReq* pReq) {
  SName   name = {0};
  int32_t code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, pReq->db);
  pReq->numOfVgroups = pStmt->pOptions->numOfVgroups;
  pReq->numOfStables = pStmt->pOptions->singleStable;
  pReq->buffer = pStmt->pOptions->buffer;
  pReq->pageSize = pStmt->pOptions->pagesize;
  pReq->pages = pStmt->pOptions->pages;
  pReq->daysPerFile = pStmt->pOptions->daysPerFile;
  pReq->daysToKeep0 = pStmt->pOptions->keep[0];
  pReq->daysToKeep1 = pStmt->pOptions->keep[1];
  pReq->daysToKeep2 = pStmt->pOptions->keep[2];
  pReq->minRows = pStmt->pOptions->minRowsPerBlock;
  pReq->maxRows = pStmt->pOptions->maxRowsPerBlock;
  pReq->walFsyncPeriod = pStmt->pOptions->fsyncPeriod;
  pReq->walLevel = pStmt->pOptions->walLevel;
  pReq->precision = pStmt->pOptions->precision;
  pReq->compression = pStmt->pOptions->compressionLevel;
  pReq->replications = pStmt->pOptions->replica;
  pReq->strict = pStmt->pOptions->strict;
  pReq->cacheLast = pStmt->pOptions->cacheModel;
  pReq->cacheLastSize = pStmt->pOptions->cacheLastSize;
  pReq->schemaless = pStmt->pOptions->schemaless;
  pReq->walRetentionPeriod = pStmt->pOptions->walRetentionPeriod;
  pReq->walRetentionSize = pStmt->pOptions->walRetentionSize;
  pReq->walRollPeriod = pStmt->pOptions->walRollPeriod;
  pReq->walSegmentSize = pStmt->pOptions->walSegmentSize;
  pReq->sstTrigger = pStmt->pOptions->sstTrigger;
  pReq->hashPrefix = pStmt->pOptions->tablePrefix;
  pReq->hashSuffix = pStmt->pOptions->tableSuffix;
  pReq->tsdbPageSize = pStmt->pOptions->tsdbPageSize;
  pReq->keepTimeOffset = pStmt->pOptions->keepTimeOffset;
  pReq->ssChunkSize = pStmt->pOptions->ssChunkSize;
  pReq->ssKeepLocal = pStmt->pOptions->ssKeepLocal;
  pReq->ssCompact = pStmt->pOptions->ssCompact;
  pReq->ignoreExist = pStmt->ignoreExists;
  pReq->withArbitrator = pStmt->pOptions->withArbitrator;
  pReq->encryptAlgorithm = pStmt->pOptions->encryptAlgorithm;
  tstrncpy(pReq->dnodeListStr, pStmt->pOptions->dnodeListStr, TSDB_DNODE_LIST_LEN);

  // auto-compact options
  pReq->compactInterval = pStmt->pOptions->compactInterval;
  pReq->compactStartTime = pStmt->pOptions->compactStartTime;
  pReq->compactEndTime = pStmt->pOptions->compactEndTime;
  pReq->compactTimeOffset = pStmt->pOptions->compactTimeOffset;

  return buildCreateDbRetentions(pStmt->pOptions->pRetentions, pReq);
}

static int32_t checkRangeOption(STranslateContext* pCxt, int32_t code, const char* pName, int64_t val, int64_t minVal,
                                int64_t maxVal, bool skipUndef) {
  if (skipUndef ? ((val >= 0 || val < -2) && (val < minVal || val > maxVal)) : (val < minVal || val > maxVal)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, code,
                                   "Invalid option %s: %" PRId64 ", valid range: [%" PRId64 ", %" PRId64 "]", pName,
                                   val, minVal, maxVal);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbRangeOption(STranslateContext* pCxt, const char* pName, int64_t val, int64_t minVal,
                                  int64_t maxVal) {
  return checkRangeOption(pCxt, TSDB_CODE_PAR_INVALID_DB_OPTION, pName, val, minVal, maxVal, true);
}

static int32_t checkTableRangeOption(STranslateContext* pCxt, const char* pName, int64_t val, int64_t minVal,
                                     int64_t maxVal) {
  return checkRangeOption(pCxt, TSDB_CODE_PAR_INVALID_TABLE_OPTION, pName, val, minVal, maxVal, true);
}

static int32_t checkDbSsKeepLocalOption(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
  if (NULL != pOptions->ssKeepLocalStr) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pOptions->ssKeepLocalStr)) {
      return pCxt->errCode;
    }
    if (TIME_UNIT_MINUTE != pOptions->ssKeepLocalStr->unit && TIME_UNIT_HOUR != pOptions->ssKeepLocalStr->unit &&
        TIME_UNIT_DAY != pOptions->ssKeepLocalStr->unit) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option ss_keeplocal unit: %c, only %c, %c, %c allowed",
                                     pOptions->ssKeepLocalStr->unit, TIME_UNIT_MINUTE, TIME_UNIT_HOUR, TIME_UNIT_DAY);
    }
    pOptions->ssKeepLocal = getBigintFromValueNode(pOptions->ssKeepLocalStr);
  }
  return checkDbRangeOption(pCxt, "ssKeepLocal", pOptions->ssKeepLocal, TSDB_MIN_SS_KEEP_LOCAL, TSDB_MAX_SS_KEEP_LOCAL);
}

static int32_t checkDbDaysOption(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
  if (NULL != pOptions->pDaysPerFile) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pOptions->pDaysPerFile)) {
      return pCxt->errCode;
    }
    if (TIME_UNIT_MINUTE != pOptions->pDaysPerFile->unit && TIME_UNIT_HOUR != pOptions->pDaysPerFile->unit &&
        TIME_UNIT_DAY != pOptions->pDaysPerFile->unit) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option duration unit: %c, only %c, %c, %c allowed",
                                     pOptions->pDaysPerFile->unit, TIME_UNIT_MINUTE, TIME_UNIT_HOUR, TIME_UNIT_DAY);
    }
    pOptions->daysPerFile = getBigintFromValueNode(pOptions->pDaysPerFile);
  }
  return checkDbRangeOption(pCxt, "daysPerFile", pOptions->daysPerFile, TSDB_MIN_DAYS_PER_FILE, TSDB_MAX_DAYS_PER_FILE);
}

static int32_t checkDbKeepOption(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
  if (NULL == pOptions->pKeep) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t numOfKeep = LIST_LENGTH(pOptions->pKeep);
  if (numOfKeep > 3 || numOfKeep < 1) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION, "Invalid number of keep options");
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pOptions->pKeep) {
    SValueNode* pVal = (SValueNode*)pNode;
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
    if (IS_DURATION_VAL(pVal->flag) && TIME_UNIT_MINUTE != pVal->unit && TIME_UNIT_HOUR != pVal->unit &&
        TIME_UNIT_DAY != pVal->unit) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option keep unit: %c, only m, h, d allowed", pVal->unit);
    }
    if (!IS_DURATION_VAL(pVal->flag)) {
      pVal->datum.i = pVal->datum.i * 1440;
    }
  }

  pOptions->keep[0] = getBigintFromValueNode((SValueNode*)nodesListGetNode(pOptions->pKeep, 0));

  if (numOfKeep < 2) {
    pOptions->keep[1] = pOptions->keep[0];
  } else {
    pOptions->keep[1] = getBigintFromValueNode((SValueNode*)nodesListGetNode(pOptions->pKeep, 1));
  }
  if (numOfKeep < 3) {
    pOptions->keep[2] = pOptions->keep[1];
  } else {
    pOptions->keep[2] = getBigintFromValueNode((SValueNode*)nodesListGetNode(pOptions->pKeep, 2));
  }

  int64_t tsdbMaxKeep = TSDB_MAX_KEEP;
  if (pOptions->precision == TSDB_TIME_PRECISION_NANO) {
    tsdbMaxKeep = TSDB_MAX_KEEP_NS;
  }

  if (pOptions->keep[0] < TSDB_MIN_KEEP || pOptions->keep[1] < TSDB_MIN_KEEP || pOptions->keep[2] < TSDB_MIN_KEEP ||
      pOptions->keep[0] > tsdbMaxKeep || pOptions->keep[1] > tsdbMaxKeep || pOptions->keep[2] > tsdbMaxKeep) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option keep: %" PRId64 ", %" PRId64 ", %" PRId64
                                   " valid range: [%dm, %" PRId64 "m]",
                                   pOptions->keep[0], pOptions->keep[1], pOptions->keep[2], TSDB_MIN_KEEP, tsdbMaxKeep);
  }

  if (!((pOptions->keep[0] <= pOptions->keep[1]) && (pOptions->keep[1] <= pOptions->keep[2]))) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid keep value, should be keep0 <= keep1 <= keep2");
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbKeepTimeOffsetOption(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
  if (pOptions->pKeepTimeOffsetNode) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pOptions->pKeepTimeOffsetNode)) {
      return pCxt->errCode;
    }
    if (TIME_UNIT_HOUR != pOptions->pKeepTimeOffsetNode->unit) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option keep_time_offset unit: %c, only %c allowed",
                                     pOptions->pKeepTimeOffsetNode->unit, TIME_UNIT_HOUR);
    }
    pOptions->keepTimeOffset = getBigintFromValueNode(pOptions->pKeepTimeOffsetNode) / 60;
  }
  if (pOptions->keepTimeOffset < TSDB_MIN_KEEP_TIME_OFFSET || pOptions->keepTimeOffset > TSDB_MAX_KEEP_TIME_OFFSET) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option keep_time_offset: %d"
                                   " valid range: [%d, %d]",
                                   pOptions->keepTimeOffset, TSDB_MIN_KEEP_TIME_OFFSET, TSDB_MAX_KEEP_TIME_OFFSET);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbCacheModelOption(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
  if ('\0' != pOptions->cacheModelStr[0]) {
    if (0 == strcasecmp(pOptions->cacheModelStr, TSDB_CACHE_MODEL_NONE_STR)) {
      pOptions->cacheModel = TSDB_CACHE_MODEL_NONE;
    } else if (0 == strcasecmp(pOptions->cacheModelStr, TSDB_CACHE_MODEL_LAST_ROW_STR)) {
      pOptions->cacheModel = TSDB_CACHE_MODEL_LAST_ROW;
    } else if (0 == strcasecmp(pOptions->cacheModelStr, TSDB_CACHE_MODEL_LAST_VALUE_STR)) {
      pOptions->cacheModel = TSDB_CACHE_MODEL_LAST_VALUE;
    } else if (0 == strcasecmp(pOptions->cacheModelStr, TSDB_CACHE_MODEL_BOTH_STR)) {
      pOptions->cacheModel = TSDB_CACHE_MODEL_BOTH;
    } else {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION, "Invalid option cacheModel: %s",
                                     pOptions->cacheModelStr);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbEncryptAlgorithmOption(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
  if ('\0' != pOptions->encryptAlgorithmStr[0]) {
    if (0 == strcasecmp(pOptions->encryptAlgorithmStr, TSDB_ENCRYPT_ALGO_NONE_STR)) {
      pOptions->encryptAlgorithm = TSDB_ENCRYPT_ALGO_NONE;
    } else if (0 == strcasecmp(pOptions->encryptAlgorithmStr, TSDB_ENCRYPT_ALGO_SM4_STR)) {
      pOptions->encryptAlgorithm = TSDB_ENCRYPT_ALGO_SM4;
    } else {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option encrypt_algorithm: %s", pOptions->encryptAlgorithmStr);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbPrecisionOption(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
  if ('\0' != pOptions->precisionStr[0]) {
    if (0 == strcasecmp(pOptions->precisionStr, TSDB_TIME_PRECISION_MILLI_STR)) {
      pOptions->precision = TSDB_TIME_PRECISION_MILLI;
    } else if (0 == strcasecmp(pOptions->precisionStr, TSDB_TIME_PRECISION_MICRO_STR)) {
      pOptions->precision = TSDB_TIME_PRECISION_MICRO;
    } else if (0 == strcasecmp(pOptions->precisionStr, TSDB_TIME_PRECISION_NANO_STR)) {
      pOptions->precision = TSDB_TIME_PRECISION_NANO;
    } else {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION, "Invalid option precision: %s",
                                     pOptions->precisionStr);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbStrictOption(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
  if ('\0' != pOptions->strictStr[0]) {
    if (0 == strcasecmp(pOptions->strictStr, TSDB_DB_STRICT_OFF_STR)) {
      pOptions->strict = TSDB_DB_STRICT_OFF;
    } else if (0 == strcasecmp(pOptions->strictStr, TSDB_DB_STRICT_ON_STR)) {
      pOptions->strict = TSDB_DB_STRICT_ON;
    } else {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION, "Invalid option strict: %s",
                                     pOptions->strictStr);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbEnumOption(STranslateContext* pCxt, const char* pName, int32_t val, int32_t v1, int32_t v2) {
  if (val >= 0 && val != v1 && val != v2) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option %s: %d, only %d, %d allowed", pName, val, v1, v2);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbEnumOption3(STranslateContext* pCxt, const char* pName, int32_t val, int32_t v1, int32_t v2,
                                  int32_t v3) {
  if (val >= 0 && val != v1 && val != v2 && val != v3) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option %s: %d, only %d, %d, %d allowed", pName, val, v1, v2, v3);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbRetentionsOption(STranslateContext* pCxt, SNodeList* pRetentions, int8_t precision) {
  if (NULL == pRetentions) {
    return TSDB_CODE_SUCCESS;
  }

  if (LIST_LENGTH(pRetentions) > 3) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION, "Invalid option retentions");
  }

  SValueNode* pPrevFreq = NULL;
  SValueNode* pPrevKeep = NULL;
  SNode*      pRetention = NULL;
  bool        firstFreq = true;
  FOREACH(pRetention, pRetentions) {
    SNode* pNode = NULL;
    FOREACH(pNode, ((SNodeListNode*)pRetention)->pNodeList) {
      SValueNode* pVal = (SValueNode*)pNode;
      if (firstFreq) {
        firstFreq = false;
        if (pVal->literal[0] != '-' || strlen(pVal->literal) != 1) {
          return generateSyntaxErrMsgExt(
              &pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
              "Invalid option retentions(freq): %s, the interval of 1st retention level should be '-'", pVal->literal);
        }
        pVal->unit = TIME_UNIT_SECOND;  // assign minimum unit
        pVal->datum.i = 0;              // assign minimum value
        continue;
      }
      if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
        return pCxt->errCode;
      }
    }

    SValueNode* pFreq = (SValueNode*)nodesListGetNode(((SNodeListNode*)pRetention)->pNodeList, 0);
    SValueNode* pKeep = (SValueNode*)nodesListGetNode(((SNodeListNode*)pRetention)->pNodeList, 1);

    if (!IS_DURATION_VAL(pFreq->flag) || !IS_DURATION_VAL(pKeep->flag)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Retentions freq/keep should have unit");
    }

    // check unit
    if (IS_DURATION_VAL(pFreq->flag) && TIME_UNIT_SECOND != pFreq->unit && TIME_UNIT_MINUTE != pFreq->unit &&
        TIME_UNIT_HOUR != pFreq->unit && TIME_UNIT_DAY != pFreq->unit && TIME_UNIT_WEEK != pFreq->unit) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option retentions(freq): %s, only s, m, h, d, w allowed", pFreq->literal);
    }

    if (IS_DURATION_VAL(pKeep->flag) && TIME_UNIT_MINUTE != pKeep->unit && TIME_UNIT_HOUR != pKeep->unit &&
        TIME_UNIT_DAY != pKeep->unit) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option retentions(keep): %s, only m, h, d allowed", pKeep->literal);
    }

    // check value range
    if (pPrevFreq != NULL && pFreq->datum.i <= 0) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option retentions(freq): %s should larger than 0", pFreq->literal);
    }
    int64_t keepMinute = pKeep->datum.i / getUnitPerMinute(pKeep->node.resType.precision);
    int64_t tsdbMaxKeep = TSDB_TIME_PRECISION_NANO == precision ? TSDB_MAX_KEEP_NS : TSDB_MAX_KEEP;
    if (keepMinute < TSDB_MIN_KEEP || keepMinute > tsdbMaxKeep) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option retentions(keep): %" PRId64 "m, valid range: [%" PRIi64
                                     "m, %" PRId64 "m]",
                                     keepMinute, TSDB_MIN_KEEP, tsdbMaxKeep);
    }

    // check relationships
    if (pFreq->datum.i >= pKeep->datum.i) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option retentions(freq/keep): %s should larger than %s", pKeep->literal,
                                     pFreq->literal);
    }

    if (NULL != pPrevFreq && pPrevFreq->datum.i >= pFreq->datum.i) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option retentions(freq): %s should larger than %s", pFreq->literal,
                                     pPrevFreq->literal);
    }

    if (NULL != pPrevKeep && pPrevKeep->datum.i > pKeep->datum.i) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option retentions(keep): %s should not larger than %s",
                                     pPrevKeep->literal, pKeep->literal);
    }

    pPrevFreq = pFreq;
    pPrevKeep = pKeep;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbTbPrefixSuffixOptions(STranslateContext* pCxt, int32_t tbPrefix, int32_t tbSuffix) {
  if (tbPrefix < TSDB_MIN_HASH_PREFIX || tbPrefix > TSDB_MAX_HASH_PREFIX) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option table_prefix: %d valid range: [%d, %d]", tbPrefix,
                                   TSDB_MIN_HASH_PREFIX, TSDB_MAX_HASH_PREFIX);
  }

  if (tbSuffix < TSDB_MIN_HASH_SUFFIX || tbSuffix > TSDB_MAX_HASH_SUFFIX) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option table_suffix: %d valid range: [%d, %d]", tbSuffix,
                                   TSDB_MIN_HASH_SUFFIX, TSDB_MAX_HASH_SUFFIX);
  }

  if ((tbPrefix * tbSuffix) < 0) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option table_prefix & table_suffix: mixed usage not allowed");
  }

  if ((tbPrefix + tbSuffix) >= (TSDB_TABLE_NAME_LEN - 1)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option table_prefix & table_suffix: exceed max table name length");
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t translateGetDbCfg(STranslateContext* pCxt, const char* pDbName, SDbCfgInfo** ppDbCfg) {
  if (*ppDbCfg) {
    return TSDB_CODE_SUCCESS;
  }
  if (!(*ppDbCfg = taosMemoryCalloc(1, sizeof(SDbCfgInfo)))) {
    return terrno;
  }
  return getDBCfg(pCxt, pDbName, *ppDbCfg);
}

static int32_t checkOptionsDependency(STranslateContext* pCxt, const char* pDbName, SDatabaseOptions* pOptions) {
  int32_t daysPerFile = pOptions->daysPerFile;
  int32_t ssKeepLocal = pOptions->ssKeepLocal;
  int64_t daysToKeep0 = pOptions->keep[0];
  if (-1 == daysPerFile && -1 == daysToKeep0) {
    return TSDB_CODE_SUCCESS;
  } else if (-1 == daysPerFile || -1 == daysToKeep0) {
    TAOS_CHECK_RETURN(translateGetDbCfg(pCxt, pDbName, &pOptions->pDbCfg));
    daysPerFile = (-1 == daysPerFile ? pOptions->pDbCfg->daysPerFile : daysPerFile);
    daysToKeep0 = (-1 == daysToKeep0 ? pOptions->pDbCfg->daysToKeep0 : daysToKeep0);
  }
  if (daysPerFile > daysToKeep0 / 3) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid duration value, should be keep2 >= keep1 >= keep0 >= 3 * duration");
  }
  if (ssKeepLocal > 0 && daysPerFile > ssKeepLocal / 3) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid parameters, should be ss_keeplocal >= 3 * duration");
  }

  if ((pOptions->replica == 2) ^ (pOptions->withArbitrator == TSDB_MAX_DB_WITH_ARBITRATOR)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option, with_arbitrator should be used with replica 2");
  }

  if (pOptions->replica > 1 && pOptions->walLevel == 0) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option, wal_level 0 should be used with replica 1");
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbCompactIntervalOption(STranslateContext* pCxt, const char* pDbName, SDatabaseOptions* pOptions) {
  int32_t code = 0;
  int32_t keep2 = pOptions->keep[2];

  if (NULL != pOptions->pCompactIntervalNode) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pOptions->pCompactIntervalNode)) {
      return pCxt->errCode;
    }
    if (TIME_UNIT_MINUTE != pOptions->pCompactIntervalNode->unit &&
        TIME_UNIT_HOUR != pOptions->pCompactIntervalNode->unit &&
        TIME_UNIT_DAY != pOptions->pCompactIntervalNode->unit) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option compact_interval unit: %c, only %c, %c, %c allowed",
                                     pOptions->pCompactIntervalNode->unit, TIME_UNIT_MINUTE, TIME_UNIT_HOUR,
                                     TIME_UNIT_DAY);
    }
    int64_t interval = getBigintFromValueNode(pOptions->pCompactIntervalNode);
    if (interval != 0) {
      if (keep2 == -1) {  // alter db
        TAOS_CHECK_RETURN(translateGetDbCfg(pCxt, pDbName, &pOptions->pDbCfg));
        keep2 = pOptions->pDbCfg->daysToKeep2;
      }
      code = checkDbRangeOption(pCxt, "compact_interval", interval, TSDB_MIN_COMPACT_INTERVAL, keep2);
      TAOS_CHECK_RETURN(code);
    }
    pOptions->compactInterval = (int32_t)interval;
  } else if (pOptions->compactInterval > 0) {
    int64_t interval = (int64_t)pOptions->compactInterval * 1440;  // convert to minutes
    if (keep2 == -1) {                                             // alter db
      TAOS_CHECK_RETURN(translateGetDbCfg(pCxt, pDbName, &pOptions->pDbCfg));
      keep2 = pOptions->pDbCfg->daysToKeep2;
    }
    code = checkDbRangeOption(pCxt, "compact_interval", interval, TSDB_MIN_COMPACT_INTERVAL, keep2);
    TAOS_CHECK_RETURN(code);
    pOptions->compactInterval = (int32_t)interval;
  }
  return code;
}

static int32_t checkDbCompactTimeRangeOption(STranslateContext* pCxt, const char* pDbName, SDatabaseOptions* pOptions) {
  if (NULL == pOptions->pCompactTimeRangeList) {
    return TSDB_CODE_SUCCESS;
  }

  if (LIST_LENGTH(pOptions->pCompactTimeRangeList) != 2) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option compact_time_range, should have 2 values");
  }

  SValueNode* pStart = (SValueNode*)nodesListGetNode(pOptions->pCompactTimeRangeList, 0);
  SValueNode* pEnd = (SValueNode*)nodesListGetNode(pOptions->pCompactTimeRangeList, 1);
  if (DEAL_RES_ERROR == translateValue(pCxt, pStart)) {
    return pCxt->errCode;
  }
  if (DEAL_RES_ERROR == translateValue(pCxt, pEnd)) {
    return pCxt->errCode;
  }
  if (IS_DURATION_VAL(pStart->flag)) {
    if (TIME_UNIT_MINUTE != pStart->unit && TIME_UNIT_HOUR != pStart->unit && TIME_UNIT_DAY != pStart->unit) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option compact_time_range start unit: %c, only %c, %c, %c allowed",
                                     pStart->unit, TIME_UNIT_MINUTE, TIME_UNIT_HOUR, TIME_UNIT_DAY);
    }
  } else {
    pStart->datum.i *= 1440;
  }
  if (IS_DURATION_VAL(pEnd->flag)) {
    if (TIME_UNIT_MINUTE != pEnd->unit && TIME_UNIT_HOUR != pEnd->unit && TIME_UNIT_DAY != pEnd->unit) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option compact_time_range end unit: %c, only %c, %c, %c allowed",
                                     pEnd->unit, TIME_UNIT_MINUTE, TIME_UNIT_HOUR, TIME_UNIT_DAY);
    }
  } else {
    pEnd->datum.i *= 1440;
  }
  pOptions->compactStartTime = getBigintFromValueNode(pStart);
  pOptions->compactEndTime = getBigintFromValueNode(pEnd);

  if (pOptions->compactStartTime == 0 && pOptions->compactEndTime == 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (pOptions->compactStartTime >= pOptions->compactEndTime) {
    return generateSyntaxErrMsgExt(
        &pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
        "Invalid option compact_time_range: %dm,%dm, start time should be less than end time",
        pOptions->compactStartTime, pOptions->compactEndTime);
  }

  int32_t keep2 = pOptions->keep[2];
  int32_t days = pOptions->daysPerFile;
  if (keep2 == -1 || days == -1) {  // alter db
    TAOS_CHECK_RETURN(translateGetDbCfg(pCxt, pDbName, &pOptions->pDbCfg));
    keep2 = pOptions->pDbCfg->daysToKeep2;
    days = pOptions->pDbCfg->daysPerFile;
  }
  if (pOptions->compactStartTime < -keep2 || pOptions->compactStartTime > -days) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option compact_time_range: %dm, start time should be in range: [%dm, %dm]",
                                   pOptions->compactStartTime, -keep2, -days);
  }
  if (pOptions->compactEndTime < -keep2 || pOptions->compactEndTime > -days) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option compact_time_range: %dm, end time should be in range: [%dm, %dm]",
                                   pOptions->compactEndTime, -keep2, -days);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbCompactTimeOffsetOption(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
  if (pOptions->pCompactTimeOffsetNode) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pOptions->pCompactTimeOffsetNode)) {
      return pCxt->errCode;
    }
    if (TIME_UNIT_HOUR != pOptions->pCompactTimeOffsetNode->unit) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option compact_time_offset unit: %c, only %c allowed",
                                     pOptions->pCompactTimeOffsetNode->unit, TIME_UNIT_HOUR);
    }
    pOptions->compactTimeOffset = getBigintFromValueNode(pOptions->pCompactTimeOffsetNode) / 60;
  }
  return checkDbRangeOption(pCxt, "compact_time_offset", pOptions->compactTimeOffset, TSDB_MIN_COMPACT_TIME_OFFSET,
                            TSDB_MAX_COMPACT_TIME_OFFSET);
}

static int32_t checkDatabaseOptions(STranslateContext* pCxt, const char* pDbName, SDatabaseOptions* pOptions) {
  int32_t code =
      checkDbRangeOption(pCxt, "buffer", pOptions->buffer, TSDB_MIN_BUFFER_PER_VNODE, TSDB_MAX_BUFFER_PER_VNODE);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbCacheModelOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEncryptAlgorithmOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code =
        checkDbRangeOption(pCxt, "cacheSize", pOptions->cacheLastSize, TSDB_MIN_DB_CACHE_SIZE, TSDB_MAX_DB_CACHE_SIZE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code =
        checkDbRangeOption(pCxt, "compression", pOptions->compressionLevel, TSDB_MIN_COMP_LEVEL, TSDB_MAX_COMP_LEVEL);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbDaysOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "fsyncPeriod", pOptions->fsyncPeriod, TSDB_MIN_FSYNC_PERIOD, TSDB_MAX_FSYNC_PERIOD);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "maxRowsPerBlock", pOptions->maxRowsPerBlock, TSDB_MIN_MAXROWS_FBLOCK,
                              TSDB_MAX_MAXROWS_FBLOCK);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "minRowsPerBlock", pOptions->minRowsPerBlock, TSDB_MIN_MINROWS_FBLOCK,
                              TSDB_MAX_MINROWS_FBLOCK);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbPrecisionOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbKeepOption(pCxt, pOptions);  // use precision
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbKeepTimeOffsetOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "pages", pOptions->pages, TSDB_MIN_PAGES_PER_VNODE, TSDB_MAX_PAGES_PER_VNODE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "pagesize", pOptions->pagesize, TSDB_MIN_PAGESIZE_PER_VNODE,
                              TSDB_MAX_PAGESIZE_PER_VNODE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "tsdbPagesize", pOptions->tsdbPageSize, TSDB_MIN_TSDB_PAGESIZE,
                              TSDB_MAX_TSDB_PAGESIZE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "replications", pOptions->replica, TSDB_MIN_DB_REPLICA, TSDB_MAX_DB_REPLICA);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbStrictOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption3(pCxt, "walLevel", pOptions->walLevel, TSDB_MIN_WAL_LEVEL, TSDB_DEFAULT_WAL_LEVEL,
                              TSDB_MAX_WAL_LEVEL);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "vgroups", pOptions->numOfVgroups, TSDB_MIN_VNODES_PER_DB, TSDB_MAX_VNODES_PER_DB);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "singleStable", pOptions->singleStable, TSDB_DB_SINGLE_STABLE_ON,
                             TSDB_DB_SINGLE_STABLE_OFF);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRetentionsOption(pCxt, pOptions->pRetentions, pOptions->precision);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "schemaless", pOptions->schemaless, TSDB_DB_SCHEMALESS_ON, TSDB_DB_SCHEMALESS_OFF);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "walRetentionPeriod", pOptions->walRetentionPeriod,
                              TSDB_DB_MIN_WAL_RETENTION_PERIOD, INT32_MAX);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "walRetentionSize", pOptions->walRetentionSize, TSDB_DB_MIN_WAL_RETENTION_SIZE,
                              INT32_MAX);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "walRollPeriod", pOptions->walRollPeriod, TSDB_DB_MIN_WAL_ROLL_PERIOD, INT32_MAX);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code =
        checkDbRangeOption(pCxt, "walSegmentSize", pOptions->walSegmentSize, TSDB_DB_MIN_WAL_SEGMENT_SIZE, INT32_MAX);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "sstTrigger", pOptions->sstTrigger, TSDB_MIN_STT_TRIGGER, TSDB_MAX_STT_TRIGGER);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "withArbitrator", pOptions->withArbitrator, TSDB_MIN_DB_WITH_ARBITRATOR,
                             TSDB_MAX_DB_WITH_ARBITRATOR);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "encryptAlgorithm", pOptions->encryptAlgorithm, TSDB_MIN_ENCRYPT_ALGO,
                             TSDB_MAX_ENCRYPT_ALGO);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbTbPrefixSuffixOptions(pCxt, pOptions->tablePrefix, pOptions->tableSuffix);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbSsKeepLocalOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkOptionsDependency(pCxt, pDbName, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "ss_chunkpages", pOptions->ssChunkSize, TSDB_MIN_SS_CHUNK_SIZE,
                              TSDB_MAX_SS_CHUNK_SIZE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "ss_compact", pOptions->ssCompact, TSDB_MIN_SS_COMPACT, TSDB_MAX_SS_COMPACT);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbCompactIntervalOption(pCxt, pDbName, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbCompactTimeRangeOption(pCxt, pDbName, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbCompactTimeOffsetOption(pCxt, pOptions);
  }
  return code;
}

static int32_t checkCreateDatabase(STranslateContext* pCxt, SCreateDatabaseStmt* pStmt) {
  if (NULL != strchr(pStmt->dbName, '.')) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME,
                                   "The database name cannot contain '.'");
  }
  if (IS_SYS_DBNAME(pStmt->dbName)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION,
                                   "Cannot create system database: `%s`", pStmt->dbName);
  }
  return checkDatabaseOptions(pCxt, pStmt->dbName, pStmt->pOptions);
}

static int32_t checkCreateMount(STranslateContext* pCxt, SCreateMountStmt* pStmt) {
  if (NULL != strchr(pStmt->mountName, '_')) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME,
                                   "The mount name cannot contain '_'");
  }
  if (IS_SYS_DBNAME(pStmt->mountName)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION,
                                   "The mount name cannot equal system database: `%s`", pStmt->mountName);
  }

  if (pStmt->mountPath[0] == '\0') {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_INPUT, "The mount path is invalid");
  }

  return checkRangeOption(pCxt, TSDB_CODE_OUT_OF_RANGE, "dnodeId", pStmt->dnodeId, 1, INT32_MAX, false);
}

#define FILL_CMD_SQL(sql, sqlLen, pCmdReq, CMD_TYPE, genericCmd) \
  CMD_TYPE* pCmdReq = genericCmd;                                \
  char*     cmdSql = taosMemoryMalloc(sqlLen);                   \
  if (cmdSql == NULL) {                                          \
    return terrno;                                               \
  }                                                              \
  memcpy(cmdSql, sql, sqlLen);                                   \
  pCmdReq->sqlLen = sqlLen;                                      \
  pCmdReq->sql = cmdSql;

static int32_t fillCmdSql(STranslateContext* pCxt, int16_t msgType, void* pReq) {
  const char* sql = pCxt->pParseCxt->pSql;
  size_t      sqlLen = pCxt->pParseCxt->sqlLen;

  switch (msgType) {
    case TDMT_MND_CREATE_DB: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SCreateDbReq, pReq);
      break;
    }
    case TDMT_MND_ALTER_DB: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SAlterDbReq, pReq);
      break;
    }
    case TDMT_MND_DROP_DB: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SDropDbReq, pReq);
      break;
    }
    case TDMT_MND_COMPACT_DB: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SCompactDbReq, pReq);
      break;
    }
    case TDMT_MND_TRIM_DB: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, STrimDbReq, pReq);
      break;
    }
    case TDMT_MND_SCAN_DB: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SScanDbReq, pReq);
      break;
    }
#ifdef USE_MOUNT
    case TDMT_MND_CREATE_MOUNT: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SCreateMountReq, pReq);
      break;
    }
    case TDMT_MND_DROP_MOUNT: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SDropMountReq, pReq);
      break;
    }
#endif
    case TDMT_MND_TMQ_DROP_TOPIC: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMDropTopicReq, pReq);
      break;
    }

    case TDMT_MND_BALANCE_VGROUP_LEADER: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SBalanceVgroupLeaderReq, pReq);
      break;
    }
    case TDMT_MND_BALANCE_VGROUP: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SBalanceVgroupReq, pReq);
      break;
    }
    case TDMT_MND_ARB_ASSIGN_LEADER: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SAssignLeaderReq, pReq);
      break;
    }
    case TDMT_MND_REDISTRIBUTE_VGROUP: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SRedistributeVgroupReq, pReq);
      break;
    }
    case TDMT_MND_CREATE_STB: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMCreateStbReq, pReq);
      break;
    }
    case TDMT_MND_DROP_STB: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMDropStbReq, pReq);
      break;
    }
    case TDMT_MND_ALTER_STB: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMAlterStbReq, pReq);
      break;
    }

    case TDMT_MND_DROP_USER: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SDropUserReq, pReq);
      break;
    }
    case TDMT_MND_CREATE_USER: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SCreateUserReq, pReq);
      break;
    }
    case TDMT_MND_ALTER_USER: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SAlterUserReq, pReq);
      break;
    }

    case TDMT_MND_CREATE_QNODE: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMCreateQnodeReq, pReq);
      break;
    }
    case TDMT_MND_DROP_QNODE: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMDropQnodeReq, pReq);
      break;
    }

    case TDMT_MND_CREATE_ANODE: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMCreateAnodeReq, pReq);
      break;
    }
    case TDMT_MND_DROP_ANODE: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMDropAnodeReq, pReq);
      break;
    }
    case TDMT_MND_UPDATE_ANODE: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMUpdateAnodeReq, pReq);
      break;
    }

    case TDMT_MND_CREATE_BNODE: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMCreateBnodeReq, pReq);
      break;
    }
    case TDMT_MND_DROP_BNODE: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMDropBnodeReq, pReq);
      break;
    }

    case TDMT_MND_CREATE_MNODE: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMCreateMnodeReq, pReq);
      break;
    }
    case TDMT_MND_DROP_MNODE: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMDropMnodeReq, pReq);
      break;
    }

    case TDMT_MND_CREATE_DNODE: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SCreateDnodeReq, pReq);
      break;
    }
    case TDMT_MND_DROP_DNODE: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SDropDnodeReq, pReq);
      break;
    }
    case TDMT_MND_RESTORE_DNODE: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SRestoreDnodeReq, pReq);
      break;
    }
    case TDMT_MND_CONFIG_DNODE: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMCfgDnodeReq, pReq);
      break;
    }

    case TDMT_MND_CONFIG_CLUSTER: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMCfgClusterReq, pReq);
      break;
    }
    case TDMT_MND_CREATE_ENCRYPT_KEY: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMCfgDnodeReq, pReq);
      break;
    }

    default: {
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

typedef int32_t (*FSerializeFunc)(void* pBuf, int32_t bufLen, void* pReq);

static int32_t buildCmdMsg(STranslateContext* pCxt, int16_t msgType, FSerializeFunc func, void* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  code = fillCmdSql(pCxt, msgType, pReq);
  if (TSDB_CODE_SUCCESS != code) return code;
  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return terrno;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = msgType;
  pCxt->pCmdMsg->msgLen = func(NULL, 0, pReq);
  if (pCxt->pCmdMsg->msgLen < 0) {
    taosMemoryFreeClear(pCxt->pCmdMsg);
    return terrno;
  }
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    taosMemoryFreeClear(pCxt->pCmdMsg);
    return terrno;
  }
  if (-1 == func(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, pReq)) {
    taosMemoryFreeClear(pCxt->pCmdMsg->pMsg);
    taosMemoryFreeClear(pCxt->pCmdMsg);
    code = TSDB_CODE_INVALID_MSG;
  }
  return code;
}

static int32_t translateCreateDatabase(STranslateContext* pCxt, SCreateDatabaseStmt* pStmt) {
  SCreateDbReq createReq = {0};
  int32_t      code = checkCreateDatabase(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateDbReq(pCxt, pStmt, &createReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_DB, (FSerializeFunc)tSerializeSCreateDbReq, &createReq);
  }
  tFreeSCreateDbReq(&createReq);
  return code;
}

static int32_t translateDropDatabase(STranslateContext* pCxt, SDropDatabaseStmt* pStmt) {
  if (IS_SYS_DBNAME(pStmt->dbName)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION, "Cannot drop system database: `%s`",
                                   pStmt->dbName);
  }
  SDropDbReq dropReq = {0};
  SName      name = {0};
  int32_t    code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, dropReq.db);
  dropReq.ignoreNotExists = pStmt->ignoreNotExists;
  dropReq.force = pStmt->force;

  code = buildCmdMsg(pCxt, TDMT_MND_DROP_DB, (FSerializeFunc)tSerializeSDropDbReq, &dropReq);
  tFreeSDropDbReq(&dropReq);
  return code;
}

static int32_t buildAlterDbReq(STranslateContext* pCxt, SAlterDatabaseStmt* pStmt, SAlterDbReq* pReq) {
  SName   name = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, pReq->db);
  pReq->buffer = pStmt->pOptions->buffer;
  pReq->pageSize = -1;
  pReq->pages = pStmt->pOptions->pages;
  pReq->daysPerFile = -1;
  pReq->daysToKeep0 = pStmt->pOptions->keep[0];
  pReq->daysToKeep1 = pStmt->pOptions->keep[1];
  pReq->daysToKeep2 = pStmt->pOptions->keep[2];
  pReq->keepTimeOffset = pStmt->pOptions->keepTimeOffset;
  pReq->walFsyncPeriod = pStmt->pOptions->fsyncPeriod;
  pReq->walLevel = pStmt->pOptions->walLevel;
  pReq->strict = pStmt->pOptions->strict;
  pReq->cacheLast = pStmt->pOptions->cacheModel;
  pReq->cacheLastSize = pStmt->pOptions->cacheLastSize;
  pReq->replications = pStmt->pOptions->replica;
  pReq->sstTrigger = pStmt->pOptions->sstTrigger;
  pReq->minRows = pStmt->pOptions->minRowsPerBlock;
  pReq->walRetentionPeriod = pStmt->pOptions->walRetentionPeriod;
  pReq->walRetentionSize = pStmt->pOptions->walRetentionSize;
  pReq->ssKeepLocal = pStmt->pOptions->ssKeepLocal;
  pReq->ssCompact = pStmt->pOptions->ssCompact;
  pReq->withArbitrator = pStmt->pOptions->withArbitrator;
  pReq->compactInterval = pStmt->pOptions->compactInterval;
  pReq->compactStartTime = pStmt->pOptions->compactStartTime;
  pReq->compactEndTime = pStmt->pOptions->compactEndTime;
  pReq->compactTimeOffset = pStmt->pOptions->compactTimeOffset;
  return code;
}

static int32_t translateAlterDatabase(STranslateContext* pCxt, SAlterDatabaseStmt* pStmt) {
  if (IS_SYS_DBNAME(pStmt->dbName)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION, "Cannot alter system database: `%s`",
                                   pStmt->dbName);
  }
  if (pStmt->pOptions->walLevel == 0) {
    TAOS_CHECK_RETURN(translateGetDbCfg(pCxt, pStmt->dbName, &pStmt->pOptions->pDbCfg));
    if (pStmt->pOptions->pDbCfg->replications > 1) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option, wal_level 0 should be used with replica 1");
    }
  }
#if 0
  if (pStmt->pOptions->replica > 1 && pStmt->pOptions->walLevel < 1) {
    SDbCfgInfo dbCfg = {0};
    dbCfg.walLevel = -1;
    int32_t code = getDBCfg(pCxt, pStmt->dbName, &dbCfg);
    if (TSDB_CODE_SUCCESS == code && dbCfg.walLevel == 0) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option, wal_level 0 should be used with replica 1");
    }
  }
#endif
  int32_t code = checkDatabaseOptions(pCxt, pStmt->dbName, pStmt->pOptions);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (pStmt->pOptions->encryptAlgorithm != -1) {
    code = TSDB_CODE_MND_ENCRYPT_NOT_ALLOW_CHANGE;
    return code;
  }

  SAlterDbReq alterReq = {0};
  code = buildAlterDbReq(pCxt, pStmt, &alterReq);

  if (TSDB_CODE_SUCCESS == code)
    code = buildCmdMsg(pCxt, TDMT_MND_ALTER_DB, (FSerializeFunc)tSerializeSAlterDbReq, &alterReq);
  tFreeSAlterDbReq(&alterReq);
  return code;
}

static int32_t translateTrimDatabase(STranslateContext* pCxt, STrimDatabaseStmt* pStmt) {
  STrimDbReq req = {.maxSpeed = pStmt->maxSpeed,
                    .tw.skey = INT64_MIN,
                    .tw.ekey = taosGetTimestampMs() / 1000,
                    .optrType = TSDB_OPTR_NORMAL,
                    .triggerType = TSDB_TRIGGER_MANUAL};
  SName      name = {0};
  int32_t    code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, req.db);
  code = buildCmdMsg(pCxt, TDMT_MND_TRIM_DB, (FSerializeFunc)tSerializeSTrimDbReq, &req);
  tFreeSTrimDbReq(&req);
  return code;
}


static int32_t translateTrimDbWal(STranslateContext* pCxt, STrimDbWalStmt* pStmt) {
  STrimDbReq req = {.maxSpeed = 0};  // WAL trim doesn't need maxSpeed
  SName      name = {0};
  int32_t    code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, req.db);
  return buildCmdMsg(pCxt, TDMT_MND_TRIM_DB_WAL, (FSerializeFunc)tSerializeSTrimDbReq, &req);
}

static int32_t checkColumnOptions(SNodeList* pList, bool virtual) {
  SNode* pNode;
  FOREACH(pNode, pList) {
    SColumnDefNode* pCol = (SColumnDefNode*)pNode;
    if (!pCol->pOptions) return TSDB_CODE_TSC_ENCODE_PARAM_NULL;
    if (!checkColumnEncodeOrSetDefault(pCol->dataType.type, ((SColumnOptions*)pCol->pOptions)->encode))
      return TSDB_CODE_TSC_ENCODE_PARAM_ERROR;
    if (!checkColumnCompressOrSetDefault(pCol->dataType.type, ((SColumnOptions*)pCol->pOptions)->compress))
      return TSDB_CODE_TSC_COMPRESS_PARAM_ERROR;
    if (!checkColumnLevelOrSetDefault(pCol->dataType.type, ((SColumnOptions*)pCol->pOptions)->compressLevel))
      return TSDB_CODE_TSC_COMPRESS_LEVEL_ERROR;
    if (!virtual && ((SColumnOptions*)pCol->pOptions)->hasRef)
      return TSDB_CODE_PAR_INVALID_COLUMN_REF;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkColumnType(SNodeList* pList, int8_t virtualTable) {
  int32_t code = 0;
  SNode*  pNode;
  int32_t blobColNum = 0;
  FOREACH(pNode, pList) {
    SColumnDefNode* pCol = (SColumnDefNode*)pNode;
    if (virtualTable && IS_DECIMAL_TYPE(pCol->dataType.type)) {
      code = TSDB_CODE_VTABLE_NOT_SUPPORT_DATA_TYPE;
      break;
    }

    if (pCol->pOptions && ((SColumnOptions*)pCol->pOptions)->bPrimaryKey && IS_STR_DATA_BLOB(pCol->dataType.type)) {
      code = TSDB_CODE_BLOB_NOT_SUPPORT_PRIMARY_KEY;
      break;
    }
    SFieldWithOptions field = {0};
    if (pCol->pOptions && ((SColumnOptions*)pCol->pOptions)->bPrimaryKey) {
      field.flags |= COL_IS_KEY;
    }

    if (IS_STR_DATA_BLOB(pCol->dataType.type)) {
      if (virtualTable) {
        code = TSDB_CODE_VTABLE_NOT_SUPPORT_DATA_TYPE;
      }
      if ((field.flags & COL_IS_KEY) != 0) {
        code = TSDB_CODE_BLOB_NOT_SUPPORT_PRIMARY_KEY;
      }
      blobColNum++;

      if (blobColNum > 1) {
        code = TSDB_CODE_BLOB_ONLY_ONE_COLUMN_ALLOWED;
        break;
      }
    }
  }
  return code;
}

static int32_t checkTableKeepOption(STranslateContext* pCxt, STableOptions* pOptions, bool createStable,
                                    int32_t daysToKeep2) {
  if (pOptions == NULL || (pOptions->keep == -1 && pOptions->pKeepNode == NULL)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!createStable) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
                                   "KEEP parameter is not allowed when creating normal table");
  }
  if (pOptions && pOptions->pKeepNode) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pOptions->pKeepNode)) {
      return pCxt->errCode;
    }
    if (pOptions->pKeepNode->unit != TIME_UNIT_DAY && pOptions->pKeepNode->unit != TIME_UNIT_HOUR &&
        pOptions->pKeepNode->unit != TIME_UNIT_MINUTE) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option keep unit: %c, only %c, %c, %c allowed", pOptions->pKeepNode->unit,
                                     TIME_UNIT_DAY, TIME_UNIT_HOUR, TIME_UNIT_MINUTE);
    }
    pOptions->keep = pOptions->pKeepNode->datum.i / 60 / 1000;
  }

  if (pOptions->keep < TSDB_MIN_KEEP || pOptions->keep > TSDB_MAX_KEEP) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_VALUE_OUT_OF_RANGE,
                                   "Invalid option keep value: %lld, should be in range [%d, %d]", pOptions->keep,
                                   TSDB_MIN_KEEP, TSDB_MAX_KEEP);
  }
  if (pOptions->keep > daysToKeep2) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_VALUE_OUT_OF_RANGE,
                                   "Invalid option keep value: %lld, should less than db config daysToKeep2: %d",
                                   pOptions->keep, daysToKeep2);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSsMigrateDatabase(STranslateContext* pCxt, SSsMigrateDatabaseStmt* pStmt) {
  SSsMigrateDbReq req = {0};
  SName           name = {0};
  int32_t         code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, req.db);
  return buildCmdMsg(pCxt, TDMT_MND_SSMIGRATE_DB, (FSerializeFunc)tSerializeSSsMigrateDbReq, &req);
}

#ifdef USE_MOUNT
static int32_t translateCreateMount(STranslateContext* pCxt, SCreateMountStmt* pStmt) {
  int32_t         code = 0, lino = 0;
  SCreateMountReq createReq = {0};

  TAOS_CHECK_EXIT(checkCreateMount(pCxt, pStmt));
  createReq.nMounts = 1;
  TSDB_CHECK_NULL((createReq.dnodeIds = taosMemoryMalloc(sizeof(int32_t) * createReq.nMounts)), code, lino, _exit,
                  terrno);
  TSDB_CHECK_NULL((createReq.mountPaths = taosMemoryMalloc(sizeof(char*) * createReq.nMounts)), code, lino, _exit,
                  terrno);
  TAOS_UNUSED(snprintf(createReq.mountName, sizeof(createReq.mountName), "%s", pStmt->mountName));
  createReq.ignoreExist = pStmt->ignoreExists;
  createReq.dnodeIds[0] = pStmt->dnodeId;
  int32_t j = strlen(pStmt->mountPath) - 1;
  while (j > 0 && (pStmt->mountPath[j] == '/' || pStmt->mountPath[j] == '\\')) {
    pStmt->mountPath[j--] = '\0';  // remove trailing slashes
  }
  TSDB_CHECK_NULL((createReq.mountPaths[0] = taosMemoryMalloc(strlen(pStmt->mountPath) + 1)), code, lino, _exit,
                  terrno);
  TAOS_UNUSED(sprintf(createReq.mountPaths[0], "%s", pStmt->mountPath));

  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_MOUNT, (FSerializeFunc)tSerializeSCreateMountReq, &createReq);
  }
_exit:
  tFreeSCreateMountReq(&createReq);
  return code;
}

static int32_t translateDropMount(STranslateContext* pCxt, SDropMountStmt* pStmt) {
  if (pStmt->mountName[0] == '\0' || IS_SYS_DBNAME(pStmt->mountName)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION, "Invalid mount name: `%s`",
                                   pStmt->mountName);
  }
  int32_t       code = 0;
  SDropMountReq dropReq = {0};
  TAOS_UNUSED(snprintf(dropReq.mountName, sizeof(dropReq.mountName), "%s", pStmt->mountName));
  dropReq.ignoreNotExists = pStmt->ignoreNotExists;

  code = buildCmdMsg(pCxt, TDMT_MND_DROP_MOUNT, (FSerializeFunc)tSerializeSDropMountReq, &dropReq);
  tFreeSDropMountReq(&dropReq);
  return code;
}
#endif

static int32_t columnDefNodeToField(SNodeList* pList, SArray** pArray, bool calBytes, bool virtualTable) {
  *pArray = taosArrayInit(LIST_LENGTH(pList), sizeof(SFieldWithOptions));
  if (!pArray) return terrno;

  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode;
  FOREACH(pNode, pList) {
    SColumnDefNode* pCol = (SColumnDefNode*)pNode;
    if (virtualTable && IS_DECIMAL_TYPE(pCol->dataType.type)) {
      code = TSDB_CODE_VTABLE_NOT_SUPPORT_DATA_TYPE;
      break;
    }

    if (pCol->pOptions && ((SColumnOptions*)pCol->pOptions)->bPrimaryKey && IS_STR_DATA_BLOB(pCol->dataType.type)) {
      code = TSDB_CODE_BLOB_NOT_SUPPORT_PRIMARY_KEY;
      break;
    }

    SFieldWithOptions field = {.type = pCol->dataType.type, .bytes = calcTypeBytes(pCol->dataType)};
    if (calBytes) {
      field.bytes = calcTypeBytes(pCol->dataType);
    } else {
      field.bytes = pCol->dataType.bytes;
    }
    field.typeMod = calcTypeMod(&pCol->dataType);
    tstrncpy(field.name, pCol->colName, TSDB_COL_NAME_LEN);
    if (pCol->pOptions) {
      setColEncode(&field.compress, columnEncodeVal(((SColumnOptions*)pCol->pOptions)->encode));
      setColCompress(&field.compress, columnCompressVal(((SColumnOptions*)pCol->pOptions)->compress));
      setColLevel(&field.compress, columnLevelVal(((SColumnOptions*)pCol->pOptions)->compressLevel));
    }
    if (pCol->sma) {
      field.flags |= COL_SMA_ON;
    }
    if (pCol->pOptions && ((SColumnOptions*)pCol->pOptions)->bPrimaryKey) {
      field.flags |= COL_IS_KEY;
    }

    if (field.typeMod != 0) {
      field.flags |= COL_HAS_TYPE_MOD;
    }
    if (NULL == taosArrayPush(*pArray, &field)) {
      code = terrno;
      break;
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    taosArrayDestroy(*pArray);
    *pArray = NULL;
  }
  return code;
}

static int32_t tagDefNodeToField(SNodeList* pList, SArray** pArray, bool calBytes, bool virtualTable) {
  *pArray = taosArrayInit(LIST_LENGTH(pList), sizeof(SField));
  if (!*pArray) return terrno;
  SNode* pNode;
  FOREACH(pNode, pList) {
    SColumnDefNode* pCol = (SColumnDefNode*)pNode;
    SField          field = {
                 .type = pCol->dataType.type,
    };
    if (virtualTable && IS_DECIMAL_TYPE(pCol->dataType.type)) {
      taosArrayDestroy(*pArray);
      *pArray = NULL;
      return TSDB_CODE_VTABLE_NOT_SUPPORT_DATA_TYPE;
    }

    if (IS_STR_DATA_BLOB(pCol->dataType.type)) {
      taosArrayDestroy(*pArray);
      *pArray = NULL;
      return TSDB_CODE_BLOB_NOT_SUPPORT_TAG;
    }
    if (calBytes) {
      field.bytes = calcTypeBytes(pCol->dataType);
    } else {
      field.bytes = pCol->dataType.bytes;
    }
    tstrncpy(field.name, pCol->colName, TSDB_COL_NAME_LEN);
    if (pCol->sma) {
      field.flags |= COL_SMA_ON;
    }
    if (NULL == taosArrayPush(*pArray, &field)) {
      taosArrayDestroy(*pArray);
      *pArray = NULL;
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t streamTagDefNodeToField(SNodeList* pList, SArray** pArray, bool calBytes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode;
  *pArray = taosArrayInit(LIST_LENGTH(pList), sizeof(SField));
  if (NULL == *pArray) {
    PAR_ERR_JRET(terrno);
  }
  FOREACH(pNode, pList) {
    SStreamTagDefNode* pTag = (SStreamTagDefNode*)pNode;
    SFieldWithOptions  field = {
         .type = pTag->dataType.type,
         .compress = 0,
         .flags = 0,
         .bytes = calBytes ? calcTypeBytes(pTag->dataType) : pTag->dataType.bytes,
         .typeMod = calcTypeMod(&pTag->dataType)};

    tstrncpy(field.name, pTag->tagName, TSDB_COL_NAME_LEN);
    if (NULL == taosArrayPush(*pArray, &field)) {
      PAR_ERR_JRET(terrno);
    }
  }
  return code;
_return:
  parserError("%s failed, code:%d", __func__, code);
  return code;
}

static SColumnDefNode* findColDef(SNodeList* pCols, const SColumnNode* pCol) {
  SNode* pColDef = NULL;
  FOREACH(pColDef, pCols) {
    if (0 == strcmp(pCol->colName, ((SColumnDefNode*)pColDef)->colName)) {
      return (SColumnDefNode*)pColDef;
    }
  }
  return NULL;
}

static int32_t checkTableSmaOption(STranslateContext* pCxt, SCreateTableStmt* pStmt) {
  if (NULL != pStmt->pOptions->pSma) {
    SNode* pNode = NULL;
    FOREACH(pNode, pStmt->pCols) { ((SColumnDefNode*)pNode)->sma = false; }
    FOREACH(pNode, pStmt->pOptions->pSma) {
      SColumnNode*    pSmaCol = (SColumnNode*)pNode;
      SColumnDefNode* pColDef = findColDef(pStmt->pCols, pSmaCol);
      if (NULL == pColDef) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pSmaCol->colName);
      }
      pSmaCol->node.resType = pColDef->dataType;
      pColDef->sma = true;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool validRollupFunc(const char* pFunc) {
  static const char*   rollupFuncs[] = {"avg", "sum", "min", "max", "last", "first"};
  static const int32_t numOfRollupFuncs = (sizeof(rollupFuncs) / sizeof(char*));
  for (int i = 0; i < numOfRollupFuncs; ++i) {
    if (0 == strcmp(rollupFuncs[i], pFunc)) {
      return true;
    }
  }
  return false;
}

static bool aggrRollupFunc(const char* pFunc) {
  static const char*   aggrRollupFuncs[] = {"avg", "sum"};
  static const int32_t numOfAggrRollupFuncs = (sizeof(aggrRollupFuncs) / sizeof(char*));
  for (int i = 0; i < numOfAggrRollupFuncs; ++i) {
    if (0 == strcmp(aggrRollupFuncs[i], pFunc)) {
      return true;
    }
  }
  return false;
}

static int32_t checkTableRollupOption(STranslateContext* pCxt, SNodeList* pFuncs, bool createStable,
                                      SDbCfgInfo* pDbCfg) {
  if (NULL == pFuncs) {
    if (NULL != pDbCfg->pRetensions) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
                                     "To create a super table in databases configured with the 'RETENTIONS' option, "
                                     "the 'ROLLUP' option must be present");
    }
    return TSDB_CODE_SUCCESS;
  }

  if (!createStable || NULL == pDbCfg->pRetensions) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
                                   "Invalid option rollup: Only supported for create super table in databases "
                                   "configured with the 'RETENTIONS' option");
  }
  if (1 != LIST_LENGTH(pFuncs)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
                                   "Invalid option rollup: only one function is allowed");
  }
  const char* pFunc = ((SFunctionNode*)nodesListGetNode(pFuncs, 0))->functionName;
  if (!validRollupFunc(pFunc)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
                                   "Invalid option rollup: %s function is not supported", pFunc);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDecimalDataType(STranslateContext* pCxt, SDataType pDataType) {
  if (IS_DECIMAL_TYPE(pDataType.type)) {
    if (pDataType.precision < TSDB_DECIMAL_MIN_PRECISION || pDataType.precision > TSDB_DECIMAL_MAX_PRECISION ||
        pDataType.precision < pDataType.scale) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN,
                                     "Invalid column type DECIMAL(%d,%d) , invalid precision or scale",
                                     pDataType.precision, pDataType.scale);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkTableTagsSchema(STranslateContext* pCxt, SHashObj* pHash, SNodeList* pTags, bool calBytes) {
  int32_t ntags = LIST_LENGTH(pTags);
  if (0 == ntags) {
    return TSDB_CODE_SUCCESS;
  } else if (ntags > TSDB_MAX_TAGS) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAGS_NUM);
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t tagsSize = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pTags) {
    SColumnDefNode* pTag = (SColumnDefNode*)pNode;
    int32_t         len = strlen(pTag->colName);
    if (NULL != taosHashGet(pHash, pTag->colName, len)) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DUPLICATED_COLUMN);
    }
    if (TSDB_CODE_SUCCESS == code && pTag->dataType.type == TSDB_DATA_TYPE_JSON && ntags > 1) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ONLY_ONE_JSON_TAG);
    }
    if (TSDB_CODE_SUCCESS == code) {
      if ((TSDB_DATA_TYPE_VARCHAR == pTag->dataType.type && (calBytes ? calcTypeBytes(pTag->dataType) : pTag->dataType.bytes) > TSDB_MAX_TAGS_LEN) ||
          (TSDB_DATA_TYPE_VARBINARY == pTag->dataType.type && (calBytes ? calcTypeBytes(pTag->dataType) : pTag->dataType.bytes) > TSDB_MAX_TAGS_LEN) ||
          (TSDB_DATA_TYPE_NCHAR == pTag->dataType.type && (calBytes ? calcTypeBytes(pTag->dataType) : pTag->dataType.bytes) > TSDB_MAX_TAGS_LEN) ||
          (TSDB_DATA_TYPE_GEOMETRY == pTag->dataType.type && (calBytes ? calcTypeBytes(pTag->dataType) : pTag->dataType.bytes) > TSDB_MAX_TAGS_LEN)) {
        code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = taosHashPut(pHash, pTag->colName, len, &pTag, POINTER_BYTES);
    }
    if (TSDB_CODE_SUCCESS == code) {
      tagsSize += (calBytes ? calcTypeBytes(pTag->dataType) : pTag->dataType.bytes);
    } else {
      break;
    }
    if (TSDB_CODE_SUCCESS == code) {
      if (IS_DECIMAL_TYPE(pTag->dataType.type)) {
        code =
            generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, "Decimal type is not allowed for tag");
      }
    }
  }

  if (TSDB_CODE_SUCCESS == code && tagsSize > TSDB_MAX_TAGS_LEN) {
    code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAGS_LENGTH, TSDB_MAX_TAGS_LEN);
  }

  return code;
}

static int32_t checkTableColsSchema(STranslateContext* pCxt, SHashObj* pHash, int32_t ntags, SNodeList* pCols,
                                    SNodeList* pRollupFuncs, bool calBytes) {
  int32_t ncols = LIST_LENGTH(pCols);
  if (ncols < TSDB_MIN_COLUMNS) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
  } else if (ncols + ntags > TSDB_MAX_COLUMNS) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TOO_MANY_COLUMNS);
  }

  int32_t code = TSDB_CODE_SUCCESS;

  int32_t colIndex = 0;
  int32_t rowSize = 0;
  SNode*  pNode = NULL;
  char*   pFunc = NULL;
  bool    isAggrRollup = false;

  if (pRollupFuncs) {
    pFunc = ((SFunctionNode*)nodesListGetNode(pRollupFuncs, 0))->functionName;
    isAggrRollup = aggrRollupFunc(pFunc);
  }
  FOREACH(pNode, pCols) {
    SColumnDefNode* pCol = (SColumnDefNode*)pNode;
    if (0 == colIndex) {
      if (TSDB_DATA_TYPE_TIMESTAMP != pCol->dataType.type) {
        code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FIRST_COLUMN);
      }
    }
    if (TSDB_CODE_SUCCESS == code && pCol->pOptions && ((SColumnOptions*)pCol->pOptions)->bPrimaryKey &&
        colIndex != 1) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SECOND_COL_PK);
    }
    if (TSDB_CODE_SUCCESS == code && pCol->pOptions && ((SColumnOptions*)pCol->pOptions)->bPrimaryKey &&
        !(TSDB_DATA_TYPE_INT == pCol->dataType.type || TSDB_DATA_TYPE_UINT == pCol->dataType.type ||
          TSDB_DATA_TYPE_BIGINT == pCol->dataType.type || TSDB_DATA_TYPE_UBIGINT == pCol->dataType.type ||
          TSDB_DATA_TYPE_VARCHAR == pCol->dataType.type)) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_COL_PK_TYPE);
    }
    if (TSDB_CODE_SUCCESS == code && pCol->dataType.type == TSDB_DATA_TYPE_JSON) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COL_JSON);
    }
    int32_t len = strlen(pCol->colName);
    if (TSDB_CODE_SUCCESS == code && NULL != taosHashGet(pHash, pCol->colName, len)) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DUPLICATED_COLUMN);
    }
    if (TSDB_CODE_SUCCESS == code) {
      if ((TSDB_DATA_TYPE_VARCHAR == pCol->dataType.type && (calBytes ? calcTypeBytes(pCol->dataType) : pCol->dataType.bytes) > TSDB_MAX_BINARY_LEN) ||
          (TSDB_DATA_TYPE_VARBINARY == pCol->dataType.type && (calBytes ? calcTypeBytes(pCol->dataType) : pCol->dataType.bytes) > TSDB_MAX_BINARY_LEN) ||
          (TSDB_DATA_TYPE_NCHAR == pCol->dataType.type && (calBytes ? calcTypeBytes(pCol->dataType) : pCol->dataType.bytes) > TSDB_MAX_NCHAR_LEN) ||
          (TSDB_DATA_TYPE_GEOMETRY == pCol->dataType.type && (calBytes ? calcTypeBytes(pCol->dataType) : pCol->dataType.bytes) > TSDB_MAX_GEOMETRY_LEN)) {
        code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
      }
    }

    if (TSDB_CODE_SUCCESS == code && isAggrRollup && 0 != colIndex) {
      if (pCol->dataType.type != TSDB_DATA_TYPE_FLOAT && pCol->dataType.type != TSDB_DATA_TYPE_DOUBLE) {
        code =
            generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN,
                                    "Invalid column type: %s, only float/double allowed for %s", pCol->colName, pFunc);
      }
    }

    if (TSDB_CODE_SUCCESS == code && IS_DECIMAL_TYPE(pCol->dataType.type)) {
      code = checkDecimalDataType(pCxt, pCol->dataType);
    }

    if (TSDB_CODE_SUCCESS == code) {
      code = taosHashPut(pHash, pCol->colName, len, &pCol, POINTER_BYTES);
    }
    if (TSDB_CODE_SUCCESS == code) {
      rowSize += (calBytes ? calcTypeBytes(pCol->dataType) : pCol->dataType.bytes);
    } else {
      break;
    }
    // next column
    ++colIndex;
  }

  if (TSDB_CODE_SUCCESS == code && rowSize > TSDB_MAX_BYTES_PER_ROW) {
    code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ROW_LENGTH, TSDB_MAX_BYTES_PER_ROW);
  }

  return code;
}

static int32_t checkTableSchemaImpl(STranslateContext* pCxt, SNodeList* pTags, SNodeList* pCols,
                                    SNodeList* pRollupFuncs, bool calBytes) {
  SHashObj* pHash = taosHashInit(LIST_LENGTH(pTags) + LIST_LENGTH(pCols),
                                 taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == pHash) {
    return terrno;
  }

  int32_t code = checkTableTagsSchema(pCxt, pHash, pTags, calBytes);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableColsSchema(pCxt, pHash, LIST_LENGTH(pTags), pCols, pRollupFuncs, calBytes);
  }

  taosHashCleanup(pHash);
  return code;
}

static int32_t checkTableSchema(STranslateContext* pCxt, SCreateTableStmt* pStmt) {
  return checkTableSchemaImpl(pCxt, pStmt->pTags, pStmt->pCols, pStmt->pOptions->pRollupFuncs, true);
}

static int32_t checkVTableSchema(STranslateContext* pCxt, SCreateVTableStmt* pStmt) {
  return checkTableSchemaImpl(pCxt, NULL, pStmt->pCols, NULL, true);
}

static int32_t getTableDelayOrWatermarkOption(STranslateContext* pCxt, const char* pName, int64_t minVal,
                                              int64_t maxVal, SValueNode* pVal, int64_t* pMaxDelay) {
  int32_t code = (DEAL_RES_ERROR == translateValue(pCxt, pVal) ? pCxt->errCode : TSDB_CODE_SUCCESS);
  if (TSDB_CODE_SUCCESS == code && TIME_UNIT_MILLISECOND != pVal->unit && TIME_UNIT_SECOND != pVal->unit &&
      TIME_UNIT_MINUTE != pVal->unit) {
    code = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
                                   "Invalid option %s unit: %c, only %c, %c, %c allowed", pName, pVal->unit,
                                   TIME_UNIT_MILLISECOND, TIME_UNIT_SECOND, TIME_UNIT_MINUTE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableRangeOption(pCxt, pName, pVal->datum.i, minVal, maxVal);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pMaxDelay = pVal->datum.i;
  }
  return code;
}

static int32_t getTableMaxDelayOption(STranslateContext* pCxt, SValueNode* pVal, int64_t* pMaxDelay) {
  return getTableDelayOrWatermarkOption(pCxt, "maxDelay", TSDB_MIN_ROLLUP_MAX_DELAY, TSDB_MAX_ROLLUP_MAX_DELAY, pVal,
                                        pMaxDelay);
}

static int32_t checkTableMaxDelayOption(STranslateContext* pCxt, STableOptions* pOptions, bool createStable,
                                        SDbCfgInfo* pDbCfg) {
  if (NULL == pOptions->pMaxDelay) {
    return TSDB_CODE_SUCCESS;
  }

  if (!createStable || NULL == pDbCfg->pRetensions) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
                                   "Invalid option maxdelay: Only supported for create super table in databases "
                                   "configured with the 'RETENTIONS' option");
  }

  if (LIST_LENGTH(pOptions->pMaxDelay) > 2) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION, "Invalid option maxdelay");
  }

  int32_t code =
      getTableMaxDelayOption(pCxt, (SValueNode*)nodesListGetNode(pOptions->pMaxDelay, 0), &pOptions->maxDelay1);
  if (TSDB_CODE_SUCCESS == code && 2 == LIST_LENGTH(pOptions->pMaxDelay)) {
    code = getTableMaxDelayOption(pCxt, (SValueNode*)nodesListGetNode(pOptions->pMaxDelay, 1), &pOptions->maxDelay2);
  }

  return code;
}

static int32_t getTableWatermarkOption(STranslateContext* pCxt, SValueNode* pVal, int64_t* pMaxDelay) {
  return getTableDelayOrWatermarkOption(pCxt, "watermark", TSDB_MIN_ROLLUP_WATERMARK, TSDB_MAX_ROLLUP_WATERMARK, pVal,
                                        pMaxDelay);
}

static int32_t checkTableWatermarkOption(STranslateContext* pCxt, STableOptions* pOptions, bool createStable,
                                         SDbCfgInfo* pDbCfg) {
  if (NULL == pOptions->pWatermark) {
    return TSDB_CODE_SUCCESS;
  }

  if (!createStable || NULL == pDbCfg->pRetensions) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
                                   "Invalid option watermark: Only supported for create super table in databases "
                                   "configured with the 'RETENTIONS' option");
  }

  if (LIST_LENGTH(pOptions->pWatermark) > 2) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION, "Invalid option watermark");
  }

  int32_t code =
      getTableWatermarkOption(pCxt, (SValueNode*)nodesListGetNode(pOptions->pWatermark, 0), &pOptions->watermark1);
  if (TSDB_CODE_SUCCESS == code && 2 == LIST_LENGTH(pOptions->pWatermark)) {
    code = getTableWatermarkOption(pCxt, (SValueNode*)nodesListGetNode(pOptions->pWatermark, 1), &pOptions->watermark2);
  }

  return code;
}

static int32_t getTableDeleteMarkOption(STranslateContext* pCxt, SValueNode* pVal, int64_t* pMaxDelay) {
  return getTableDelayOrWatermarkOption(pCxt, "delete_mark", TSDB_MIN_ROLLUP_DELETE_MARK, TSDB_MAX_ROLLUP_DELETE_MARK,
                                        pVal, pMaxDelay);
}

static int32_t checkTableDeleteMarkOption(STranslateContext* pCxt, STableOptions* pOptions, bool createStable,
                                          SDbCfgInfo* pDbCfg) {
  if (NULL == pOptions->pDeleteMark) {
    return TSDB_CODE_SUCCESS;
  }

  if (!createStable || NULL == pDbCfg->pRetensions) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
                                   "Invalid option delete_mark: Only supported for create super table in databases "
                                   "configured with the 'RETENTIONS' option");
  }

  if (LIST_LENGTH(pOptions->pDeleteMark) > 2) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION, "Invalid option delete_mark");
  }

  int32_t code =
      getTableDeleteMarkOption(pCxt, (SValueNode*)nodesListGetNode(pOptions->pDeleteMark, 0), &pOptions->deleteMark1);
  if (TSDB_CODE_SUCCESS == code && 2 == LIST_LENGTH(pOptions->pDeleteMark)) {
    code =
        getTableDeleteMarkOption(pCxt, (SValueNode*)nodesListGetNode(pOptions->pDeleteMark, 1), &pOptions->deleteMark2);
  }

  return code;
}

static int32_t checkCreateTable(STranslateContext* pCxt, SCreateTableStmt* pStmt, bool createStable) {
  if (NULL != strchr(pStmt->tableName, '.')) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME,
                                   "The table name cannot contain '.'");
  }

  if (IS_SYS_DBNAME(pStmt->dbName)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION,
                                   "Cannot create table of system database: `%s`.`%s`", pStmt->dbName,
                                   pStmt->tableName);
  }

  SDbCfgInfo dbCfg = {0};
  int32_t    code = getDBCfg(pCxt, pStmt->dbName, &dbCfg);
  if (TSDB_CODE_SUCCESS == code && !createStable && NULL != dbCfg.pRetensions) {
    code = generateSyntaxErrMsgExt(
        &pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
        "Only super table creation is supported in databases configured with the 'RETENTIONS' option");
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableMaxDelayOption(pCxt, pStmt->pOptions, createStable, &dbCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableWatermarkOption(pCxt, pStmt->pOptions, createStable, &dbCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableDeleteMarkOption(pCxt, pStmt->pOptions, createStable, &dbCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableRollupOption(pCxt, pStmt->pOptions->pRollupFuncs, createStable, &dbCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableSmaOption(pCxt, pStmt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableSchema(pCxt, pStmt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkColumnOptions(pStmt->pCols, false);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = checkColumnType(pStmt->pCols, 0);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableKeepOption(pCxt, pStmt->pOptions, createStable, dbCfg.daysToKeep2);
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (createStable && pStmt->pOptions->ttl != 0) {
      code = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
                                     "Only supported for create non-super table in databases "
                                     "configured with the 'TTL' option");
    }
  }
  if (pCxt->pParseCxt->biMode != 0 && TSDB_CODE_SUCCESS == code) {
    code = biCheckCreateTableTbnameCol(pCxt, pStmt->pTags, pStmt->pCols);
  }
  return code;
}

static void toSchema(const SColumnDefNode* pCol, col_id_t colId, SSchema* pSchema) {
  int8_t flags = 0;
  if (pCol->sma) {
    flags |= COL_SMA_ON;
  }
  if (pCol->pOptions && ((SColumnOptions*)pCol->pOptions)->bPrimaryKey) {
    flags |= COL_IS_KEY;
  }
  if (IS_DECIMAL_TYPE(pCol->dataType.type)) {
    flags |= COL_HAS_TYPE_MOD;
  }
  pSchema->colId = colId;
  pSchema->type = pCol->dataType.type;
  pSchema->bytes = calcTypeBytes(pCol->dataType);
  pSchema->flags = flags;
  tstrncpy(pSchema->name, pCol->colName, TSDB_COL_NAME_LEN);
}

typedef struct SSampleAstInfo {
  const char* pDbName;
  const char* pTableName;
  SNodeList*  pFuncs;
  SNode*      pInterval;
  SNode*      pOffset;
  SNode*      pSliding;
  SNodeList*  pPartitionByList;
  STableMeta* pRollupTableMeta;
  bool        createSmaIndex;
  SNodeList*  pTags;
  SNode*      pSubTable;
} SSampleAstInfo;

static int32_t buildTableForSampleAst(SSampleAstInfo* pInfo, SNode** pOutput) {
  SRealTableNode* pTable = NULL;
  int32_t         code = nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)&pTable);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  snprintf(pTable->table.dbName, sizeof(pTable->table.dbName), "%s", pInfo->pDbName);
  snprintf(pTable->table.tableName, sizeof(pTable->table.tableName), "%s", pInfo->pTableName);
  snprintf(pTable->table.tableAlias, sizeof(pTable->table.tableAlias), "%s", pInfo->pTableName);
  TSWAP(pTable->pMeta, pInfo->pRollupTableMeta);
  *pOutput = (SNode*)pTable;
  return TSDB_CODE_SUCCESS;
}

static int32_t addWstartToSampleProjects(SNodeList* pProjectionList) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  tstrncpy(pFunc->functionName, "_wstart", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.userAlias, "_wstart", TSDB_FUNC_NAME_LEN);
  return nodesListPushFront(pProjectionList, (SNode*)pFunc);
}

static int32_t addWendToSampleProjects(SNodeList* pProjectionList) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  tstrncpy(pFunc->functionName, "_wend", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.userAlias, "_wend", TSDB_FUNC_NAME_LEN);
  return nodesListAppend(pProjectionList, (SNode*)pFunc);
}

static int32_t addWdurationToSampleProjects(SNodeList* pProjectionList) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  tstrncpy(pFunc->functionName, "_wduration", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.userAlias, "_wduration", TSDB_FUNC_NAME_LEN);
  return nodesListAppend(pProjectionList, (SNode*)pFunc);
}


static int32_t addTWstartToSampleProjects(SNodeList* pProjectionList) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  tstrncpy(pFunc->functionName, "_twstart", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.userAlias, "_twstart", TSDB_FUNC_NAME_LEN);
  return nodesListPushFront(pProjectionList, (SNode*)pFunc);
}

static int32_t addTWendToSampleProjects(SNodeList* pProjectionList) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  tstrncpy(pFunc->functionName, "_twend", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.userAlias, "_twend", TSDB_FUNC_NAME_LEN);
  return nodesListAppend(pProjectionList, (SNode*)pFunc);
}

static int32_t addTWdurationToSampleProjects(SNodeList* pProjectionList) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  tstrncpy(pFunc->functionName, "_twduration", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.userAlias, "_twduration", TSDB_FUNC_NAME_LEN);
  return nodesListAppend(pProjectionList, (SNode*)pFunc);
}

static int32_t buildProjectsForSampleAst(SSampleAstInfo* pInfo, SNodeList** pList, int32_t* pProjectionTotalLen) {
  SNodeList* pProjectionList = pInfo->pFuncs;
  pInfo->pFuncs = NULL;

  int32_t code = addWstartToSampleProjects(pProjectionList);
  if (TSDB_CODE_SUCCESS == code && pInfo->createSmaIndex) {
    code = addWendToSampleProjects(pProjectionList);
    if (TSDB_CODE_SUCCESS == code) {
      code = addWdurationToSampleProjects(pProjectionList);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    SNode* pProject = NULL;
    if (pProjectionTotalLen) *pProjectionTotalLen = 0;
    FOREACH(pProject, pProjectionList) {
      snprintf(((SExprNode*)pProject)->aliasName, TSDB_COL_NAME_LEN, "#%p", pProject);
      if (pProjectionTotalLen) *pProjectionTotalLen += ((SExprNode*)pProject)->resType.bytes;
    }
    *pList = pProjectionList;
  } else {
    nodesDestroyList(pProjectionList);
  }
  return code;
}

static int32_t buildIntervalForSampleAst(SSampleAstInfo* pInfo, SNode** pOutput) {
  SIntervalWindowNode* pInterval = NULL;
  int32_t              code = nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW, (SNode**)&pInterval);
  if (NULL == pInterval) {
    return code;
  }
  TSWAP(pInterval->pInterval, pInfo->pInterval);
  TSWAP(pInterval->pOffset, pInfo->pOffset);
  TSWAP(pInterval->pSliding, pInfo->pSliding);

  SValueNode* pOffset = (SValueNode*)pInterval->pOffset;
  if (pOffset && pOffset->datum.i < 0) {
    parserError("%s failed for invalid interval offset %" PRId64, __func__, pOffset->datum.i);
    return TSDB_CODE_INVALID_PARA;
  }

  pInterval->pCol = NULL;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pInterval->pCol);
  if (NULL == pInterval->pCol) {
    nodesDestroyNode((SNode*)pInterval);
    return code;
  }
  ((SColumnNode*)pInterval->pCol)->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  tstrncpy(((SColumnNode*)pInterval->pCol)->colName, ROWTS_PSEUDO_COLUMN_NAME, TSDB_COL_NAME_LEN);
  *pOutput = (SNode*)pInterval;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildSampleAst(STranslateContext* pCxt, SSampleAstInfo* pInfo, char** pAst, int32_t* pLen, char** pExpr,
                              int32_t* pExprLen, int32_t* pProjectionTotalLen) {
  SSelectStmt* pSelect = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_SELECT_STMT, (SNode**)&pSelect);
  if (NULL == pSelect) {
    return code;
  }
  snprintf(pSelect->stmtName, TSDB_TABLE_NAME_LEN, "%p", pSelect);
  code = buildTableForSampleAst(pInfo, &pSelect->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildProjectsForSampleAst(pInfo, &pSelect->pProjectionList, pProjectionTotalLen);
  }
  if (TSDB_CODE_SUCCESS == code) {
    TSWAP(pSelect->pPartitionByList, pInfo->pPartitionByList);
    code = buildIntervalForSampleAst(pInfo, &pSelect->pWindow);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateQuery(pCxt, (SNode*)pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesNodeToString((SNode*)pSelect, false, pAst, pLen);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pExpr) {
    code = nodesListToString(pSelect->pProjectionList, false, pExpr, pExprLen);
  }
  nodesDestroyNode((SNode*)pSelect);
  return code;
}

static void clearSampleAstInfo(SSampleAstInfo* pInfo) {
  nodesDestroyList(pInfo->pFuncs);
  nodesDestroyNode(pInfo->pInterval);
  nodesDestroyNode(pInfo->pOffset);
  nodesDestroyNode(pInfo->pSliding);
  nodesDestroyNode(pInfo->pSubTable);
  nodesDestroyList(pInfo->pTags);
}

static int32_t makeIntervalVal(SRetention* pRetension, int8_t precision, SNode** ppNode) {
  SValueNode* pVal = NULL;
  int32_t     code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);
  if (NULL == pVal) {
    return code;
  }
  int64_t timeVal = -1;
  code = convertTimeFromPrecisionToUnit(pRetension->freq, precision, pRetension->freqUnit, &timeVal);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pVal);
    return code;
  }
  char    buf[20] = {0};
  int32_t len = tsnprintf(buf, sizeof(buf), "%" PRId64 "%c", timeVal, pRetension->freqUnit);
  pVal->literal = taosStrndup(buf, len);
  if (NULL == pVal->literal) {
    nodesDestroyNode((SNode*)pVal);
    return terrno;
  }
  pVal->flag |= VALUE_FLAG_IS_DURATION;
  pVal->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pVal->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  pVal->node.resType.precision = precision;
  *ppNode = (SNode*)pVal;
  return code;
}

static int32_t createColumnFromDef(SColumnDefNode* pDef, SNode** ppCol) {
  SColumnNode* pCol = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return code;
  }
  tstrncpy(pCol->colName, pDef->colName, TSDB_COL_NAME_LEN);
  *ppCol = (SNode*)pCol;
  return code;
}

static int32_t createRollupFunc(SNode* pSrcFunc, SColumnDefNode* pColDef, SNode** ppRollupFunc) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesCloneNode(pSrcFunc, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  SNode* pCol = NULL;
  code = createColumnFromDef(pColDef, &pCol);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pFunc->pParameterList, pCol);
    pCol = NULL;
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pFunc);
  } else {
    *ppRollupFunc = (SNode*)pFunc;
  }
  return code;
}

static int32_t createRollupFuncs(SCreateTableStmt* pStmt, SNodeList** ppList) {
  SNodeList* pFuncs = NULL;
  int32_t    code = nodesMakeList(&pFuncs);
  if (NULL == pFuncs) {
    return code;
  }

  SNode* pFunc = NULL;
  FOREACH(pFunc, pStmt->pOptions->pRollupFuncs) {
    SNode* pCol = NULL;
    bool   primaryKey = true;
    FOREACH(pCol, pStmt->pCols) {
      if (primaryKey) {
        primaryKey = false;
        continue;
      }
      SNode* pRollupFunc = NULL;
      code = createRollupFunc(pFunc, (SColumnDefNode*)pCol, &pRollupFunc);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyList(pFuncs);
        return code;
      }
      code = nodesListStrictAppend(pFuncs, pRollupFunc);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyList(pFuncs);
        return code;
      }
    }
  }
  *ppList = pFuncs;

  return code;
  ;
}

static int32_t createRollupTableMeta(SCreateTableStmt* pStmt, int8_t precision, STableMeta** ppTbMeta) {
  int32_t     numOfField = LIST_LENGTH(pStmt->pCols) + LIST_LENGTH(pStmt->pTags);
  STableMeta* pMeta = taosMemoryCalloc(1, sizeof(STableMeta) + numOfField * sizeof(SSchema));
  if (NULL == pMeta) {
    return terrno;
  }
  pMeta->tableType = TSDB_SUPER_TABLE;
  pMeta->tableInfo.numOfTags = LIST_LENGTH(pStmt->pTags);
  pMeta->tableInfo.precision = precision;
  pMeta->tableInfo.numOfColumns = LIST_LENGTH(pStmt->pCols);

  int32_t index = 0;
  SNode*  pCol = NULL;
  FOREACH(pCol, pStmt->pCols) {
    toSchema((SColumnDefNode*)pCol, index + 1, pMeta->schema + index);
    ++index;
  }
  SNode* pTag = NULL;
  FOREACH(pTag, pStmt->pTags) {
    toSchema((SColumnDefNode*)pTag, index + 1, pMeta->schema + index);
    ++index;
  }

  *ppTbMeta = pMeta;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildSampleAstInfoByTable(STranslateContext* pCxt, SCreateTableStmt* pStmt, SRetention* pRetension,
                                         int8_t precision, SSampleAstInfo* pInfo) {
  pInfo->pDbName = pStmt->dbName;
  pInfo->pTableName = pStmt->tableName;
  int32_t code = createRollupFuncs(pStmt, &pInfo->pFuncs);
  if (TSDB_CODE_SUCCESS != code) return code;
  pInfo->pInterval = NULL;
  code = makeIntervalVal(pRetension, precision, (SNode**)&pInfo->pInterval);
  if (TSDB_CODE_SUCCESS != code) return code;
  pInfo->pRollupTableMeta = NULL;
  code = createRollupTableMeta(pStmt, precision, &pInfo->pRollupTableMeta);
  if (TSDB_CODE_SUCCESS != code) return code;
  SFunctionNode* pFunc = NULL;
  code = createTbnameFunction(&pFunc);
  if (!pFunc) return code;
  return nodesListMakeStrictAppend(&pInfo->pPartitionByList, (SNode*)pFunc);
}

static int32_t getRollupAst(STranslateContext* pCxt, SCreateTableStmt* pStmt, SRetention* pRetension, int8_t precision,
                            char** pAst, int32_t* pLen) {
  SSampleAstInfo info = {0};
  int32_t        code = buildSampleAstInfoByTable(pCxt, pStmt, pRetension, precision, &info);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildSampleAst(pCxt, &info, pAst, pLen, NULL, NULL, NULL);
  }
  clearSampleAstInfo(&info);
  return code;
}

static int32_t buildRollupAst(STranslateContext* pCxt, SCreateTableStmt* pStmt, SMCreateStbReq* pReq) {
  SDbCfgInfo dbCfg = {0};
  int32_t    code = getDBCfg(pCxt, pStmt->dbName, &dbCfg);
  int32_t    num = taosArrayGetSize(dbCfg.pRetensions);
  if (TSDB_CODE_SUCCESS != code || num < 2) {
    return code;
  }
  for (int32_t i = 1; i < num; ++i) {
    SRetention*       pRetension = taosArrayGet(dbCfg.pRetensions, i);
    STranslateContext cxt = {0};
    code = initTranslateContext(pCxt->pParseCxt, pCxt->pMetaCache, &cxt);
    if (TSDB_CODE_SUCCESS == code) {
      code = getRollupAst(&cxt, pStmt, pRetension, dbCfg.precision, 1 == i ? &pReq->pAst1 : &pReq->pAst2,
                          1 == i ? &pReq->ast1Len : &pReq->ast2Len);
    }
    destroyTranslateContext(&cxt);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  return code;
}

static int32_t buildRollupFuncs(SNodeList* pFuncs, SArray** pArray) {
  if (NULL == pFuncs) {
    return TSDB_CODE_SUCCESS;
  }
  *pArray = taosArrayInit(LIST_LENGTH(pFuncs), TSDB_FUNC_NAME_LEN);
  if (!*pArray) return terrno;
  SNode* pNode;
  FOREACH(pNode, pFuncs) {
    if (NULL == taosArrayPush(*pArray, ((SFunctionNode*)pNode)->functionName)) {
      taosArrayDestroy(*pArray);
      *pArray = NULL;
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildCreateStbReq(STranslateContext* pCxt, SCreateTableStmt* pStmt, SMCreateStbReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  SName   tableName = {0};
  pReq->igExists = pStmt->ignoreExists;
  pReq->delay1 = pStmt->pOptions->maxDelay1;
  pReq->delay2 = pStmt->pOptions->maxDelay2;
  pReq->watermark1 = pStmt->pOptions->watermark1;
  pReq->watermark2 = pStmt->pOptions->watermark2;
  pReq->deleteMark1 = pStmt->pOptions->deleteMark1;
  pReq->deleteMark2 = pStmt->pOptions->deleteMark2;
  pReq->keep = pStmt->pOptions->keep;
  pReq->colVer = 1;
  pReq->tagVer = 1;
  pReq->source = TD_REQ_FROM_APP;
  // columnDefNodeToField(pStmt->pCols, &pReq->pColumns, true);
  // columnDefNodeToField(pStmt->pTags, &pReq->pTags, true);
  code = columnDefNodeToField(pStmt->pCols, &pReq->pColumns, true, pStmt->pOptions->virtualStb);
  if (TSDB_CODE_SUCCESS == code)
    code = tagDefNodeToField(pStmt->pTags, &pReq->pTags, true, pStmt->pOptions->virtualStb);
  if (TSDB_CODE_SUCCESS == code) {
    pReq->numOfColumns = LIST_LENGTH(pStmt->pCols);
    pReq->numOfTags = LIST_LENGTH(pStmt->pTags);
    if (pStmt->pOptions->commentNull == false) {
      pReq->pComment = taosStrdup(pStmt->pOptions->comment);
      if (NULL == pReq->pComment) {
        return terrno;
      }
      pReq->commentLen = strlen(pStmt->pOptions->comment);
    } else {
      pReq->commentLen = -1;
    }
    code = buildRollupFuncs(pStmt->pOptions->pRollupFuncs, &pReq->pFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pReq->numOfFuncs = taosArrayGetSize(pReq->pFuncs);
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tableName);
    code = tNameExtractFullName(&tableName, pReq->name);
  }
  if (TSDB_CODE_SUCCESS == code) code = collectUseTable(&tableName, pCxt->pTables);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(&tableName, pCxt->pTargetTables);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildRollupAst(pCxt, pStmt, pReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pReq->virtualStb = pStmt->pOptions->virtualStb;
  }
  return code;
}

static int32_t translateCreateSuperTable(STranslateContext* pCxt, SCreateTableStmt* pStmt) {
  SMCreateStbReq createReq = {0};
  int32_t        code = checkCreateTable(pCxt, pStmt, true);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateStbReq(pCxt, pStmt, &createReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_STB, (FSerializeFunc)tSerializeSMCreateStbReq, &createReq);
  }
  tFreeSMCreateStbReq(&createReq);
  return code;
}

static int32_t doTranslateDropSuperTable(STranslateContext* pCxt, const SName* pTableName, bool ignoreNotExists) {
  int32_t code = collectUseTable(pTableName, pCxt->pTargetTables);
  if (TSDB_CODE_SUCCESS == code) {
    SMDropStbReq dropReq = {0};
    code = tNameExtractFullName(pTableName, dropReq.name);
    if (TSDB_CODE_SUCCESS == code) {
      dropReq.igNotExists = ignoreNotExists;
      code = buildCmdMsg(pCxt, TDMT_MND_DROP_STB, (FSerializeFunc)tSerializeSMDropStbReq, &dropReq);
    }
    tFreeSMDropStbReq(&dropReq);
  }
  return code;
}

static int32_t translateDropTable(STranslateContext* pCxt, SDropTableStmt* pStmt) {
  SDropTableClause* pClause = (SDropTableClause*)nodesListGetNode(pStmt->pTables, 0);
  SName             tableName = {0};
  if (pStmt->withTsma) return TSDB_CODE_SUCCESS;
  toName(pCxt->pParseCxt->acctId, pClause->dbName, pClause->tableName, &tableName);
  return doTranslateDropSuperTable(pCxt, &tableName, pClause->ignoreNotExists);
}

static int32_t translateDropSuperTable(STranslateContext* pCxt, SDropSuperTableStmt* pStmt) {
  SName tableName = {0};
  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tableName);
  return doTranslateDropSuperTable(pCxt, &tableName, pStmt->ignoreNotExists);
}

static int32_t buildAlterSuperTableReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, SMAlterStbReq* pAlterReq) {
  SName tableName = {0};
  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tableName);
  int32_t code = tNameExtractFullName(&tableName, pAlterReq->name);
  if (TSDB_CODE_SUCCESS != code) return code;
  pAlterReq->alterType = pStmt->alterType;

  if (TSDB_ALTER_TABLE_UPDATE_OPTIONS == pStmt->alterType) {
    //    pAlterReq->ttl = pStmt->pOptions->ttl;
    if (pStmt->pOptions->commentNull == false) {
      pAlterReq->comment = taosStrdup(pStmt->pOptions->comment);
      if (NULL == pAlterReq->comment) {
        return terrno;
      }
      pAlterReq->commentLen = strlen(pStmt->pOptions->comment);
    } else {
      pAlterReq->commentLen = -1;
    }

    if (pStmt->pOptions->keep > 0) {
      pAlterReq->keep = pStmt->pOptions->keep;
    }

    return TSDB_CODE_SUCCESS;
  }

  pAlterReq->pFields = taosArrayInit(2, sizeof(TAOS_FIELD));
  if (NULL == pAlterReq->pFields) {
    return terrno;
  }
  pAlterReq->pTypeMods = taosArrayInit(2, sizeof(STypeMod));
  if (!pAlterReq->pTypeMods) return terrno;
  STypeMod typeMod = calcTypeMod(&pStmt->dataType);

  switch (pStmt->alterType) {
    case TSDB_ALTER_TABLE_ADD_COLUMN:
      if (NULL == taosArrayPush(pAlterReq->pTypeMods, &typeMod)) {
        return terrno;
      }  // fall through
    case TSDB_ALTER_TABLE_ADD_TAG:
    case TSDB_ALTER_TABLE_DROP_TAG:
    case TSDB_ALTER_TABLE_DROP_COLUMN:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES: {
      TAOS_FIELD field = {.type = pStmt->dataType.type, .bytes = calcTypeBytes(pStmt->dataType)};
      tstrncpy(field.name, pStmt->colName, 65);
      if (NULL == taosArrayPush(pAlterReq->pFields, &field)) {
        return terrno;
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME: {
      TAOS_FIELD oldField = {0};
      tstrncpy(oldField.name, pStmt->colName, 65);
      if (NULL == taosArrayPush(pAlterReq->pFields, &oldField)) {
        return terrno;
      }
      TAOS_FIELD newField = {0};
      tstrncpy(newField.name, pStmt->newColName, 65);
      if (NULL == taosArrayPush(pAlterReq->pFields, &newField)) {
        return terrno;
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS: {
      TAOS_FIELD field = {0};
      tstrncpy(field.name, pStmt->colName, 65);
      if (!checkColumnEncode(pStmt->pColOptions->encode)) return TSDB_CODE_TSC_ENCODE_PARAM_ERROR;
      if (!checkColumnCompress(pStmt->pColOptions->compress)) return TSDB_CODE_TSC_COMPRESS_PARAM_ERROR;
      if (!checkColumnLevel(pStmt->pColOptions->compressLevel)) return TSDB_CODE_TSC_COMPRESS_LEVEL_ERROR;
      int32_t code =
          setColCompressByOption(pStmt->dataType.type, columnEncodeVal(pStmt->pColOptions->encode),
                                 columnCompressVal(pStmt->pColOptions->compress),
                                 columnLevelVal(pStmt->pColOptions->compressLevel), false, (uint32_t*)&field.bytes);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      if (NULL == taosArrayPush(pAlterReq->pFields, &field)) {
        return terrno;
      }
      break;
    }
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION: {
      taosArrayDestroy(pAlterReq->pFields);

      pAlterReq->pFields = taosArrayInit(1, sizeof(SFieldWithOptions));
      if (!pAlterReq->pFields) return terrno;
      SFieldWithOptions field = {.type = pStmt->dataType.type, .bytes = calcTypeBytes(pStmt->dataType)};
      // TAOS_FIELD        field = {.type = pStmt->dataType.type, .bytes = calcTypeBytes(pStmt->dataType)};
      tstrncpy(field.name, pStmt->colName, TSDB_COL_NAME_LEN);
      if (pStmt->pColOptions != NULL) {
        if (!checkColumnEncodeOrSetDefault(pStmt->dataType.type, pStmt->pColOptions->encode))
          return TSDB_CODE_TSC_ENCODE_PARAM_ERROR;
        if (!checkColumnCompressOrSetDefault(pStmt->dataType.type, pStmt->pColOptions->compress))
          return TSDB_CODE_TSC_COMPRESS_PARAM_ERROR;
        if (!checkColumnLevelOrSetDefault(pStmt->dataType.type, pStmt->pColOptions->compressLevel))
          return TSDB_CODE_TSC_COMPRESS_LEVEL_ERROR;
        int32_t code = setColCompressByOption(pStmt->dataType.type, columnEncodeVal(pStmt->pColOptions->encode),
                                              columnCompressVal(pStmt->pColOptions->compress),
                                              columnLevelVal(pStmt->pColOptions->compressLevel), false,
                                              (uint32_t*)&field.compress);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      if (NULL == taosArrayPush(pAlterReq->pFields, &field)) return terrno;
      if (NULL == taosArrayPush(pAlterReq->pTypeMods, &typeMod)) return terrno;
      break;
    }
    default:
      break;
  }

  pAlterReq->numOfFields = taosArrayGetSize(pAlterReq->pFields);
  return TSDB_CODE_SUCCESS;
}

static const SSchema* getColSchema(const STableMeta* pTableMeta, const char* pColName) {
  int32_t numOfFields = getNumOfTags(pTableMeta) + getNumOfColumns(pTableMeta);
  for (int32_t i = 0; i < numOfFields; ++i) {
    const SSchema* pSchema = pTableMeta->schema + i;
    if (0 == strcmp(pColName, pSchema->name)) {
      return pSchema;
    }
  }
  return NULL;
}

static const SSchema* getNormalColSchema(const STableMeta* pTableMeta, const char* pColName) {
  int32_t  numOfCols = getNumOfColumns(pTableMeta);
  SSchema* pColsSchema = getTableColumnSchema(pTableMeta);
  for (int32_t i = 0; i < numOfCols; ++i) {
    const SSchema* pSchema = pColsSchema + i;
    if (0 == strcmp(pColName, pSchema->name)) {
      return pSchema;
    }
  }
  return NULL;
}

static const col_id_t getNormalColSchemaIndex(const STableMeta* pTableMeta, const char* pColName) {
  int32_t  numOfCols = getNumOfColumns(pTableMeta);
  SSchema* pColsSchema = getTableColumnSchema(pTableMeta);
  for (int32_t i = 0; i < numOfCols; ++i) {
    const SSchema* pSchema = pColsSchema + i;
    if (0 == strcmp(pColName, pSchema->name)) {
      return (col_id_t)i;
    }
  }
  return -1;
}

static SSchema* getTagSchema(const STableMeta* pTableMeta, const char* pTagName) {
  int32_t  numOfTags = getNumOfTags(pTableMeta);
  SSchema* pTagsSchema = getTableTagSchema(pTableMeta);
  for (int32_t i = 0; i < numOfTags; ++i) {
    SSchema* pSchema = pTagsSchema + i;
    if (0 == strcmp(pTagName, pSchema->name)) {
      return pSchema;
    }
  }
  return NULL;
}

static int32_t checkAlterTableByColumnType(STranslateContext* pCxt, SAlterTableStmt* pStmt,
                                           const STableMeta* pTableMeta) {
  int32_t code = 0;
  if (!IS_STR_DATA_BLOB(pStmt->dataType.type)) {
    return code;
  }

  if (pTableMeta->virtualStb) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_VTABLE_NOT_SUPPORT_DATA_TYPE);
  }
  if (pStmt->alterType != TSDB_ALTER_TABLE_DROP_COLUMN && pStmt->alterType != TSDB_ALTER_TABLE_ADD_COLUMN) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_BLOB_NOT_SUPPORT);
  }
  int8_t blobCnt = 0;
  if (pStmt->alterType == TSDB_ALTER_TABLE_ADD_COLUMN) {
    int32_t numOfFields = getNumOfColumns(pTableMeta);
    for (int32_t i = 0; i < numOfFields; ++i) {
      const SSchema* pSchema = pTableMeta->schema + i;
      if (IS_STR_DATA_BLOB(pSchema->type)) {
        blobCnt++;
      }
      if (blobCnt > 0) {
        code = TSDB_CODE_BLOB_ONLY_ONE_COLUMN_ALLOWED;
        break;
      }
    }
  }

  return code;
}
static int32_t checkAlterSuperTableBySchema(STranslateContext* pCxt, SAlterTableStmt* pStmt,
                                            const STableMeta* pTableMeta) {
  if (pTableMeta->virtualStb && IS_DECIMAL_TYPE(pStmt->dataType.type)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_VTABLE_NOT_SUPPORT_DATA_TYPE);
  }

  SSchema* pTagsSchema = getTableTagSchema(pTableMeta);
  if (getNumOfTags(pTableMeta) == 1 && pTagsSchema->type == TSDB_DATA_TYPE_JSON &&
      (pStmt->alterType == TSDB_ALTER_TABLE_ADD_TAG || pStmt->alterType == TSDB_ALTER_TABLE_DROP_TAG ||
       pStmt->alterType == TSDB_ALTER_TABLE_UPDATE_TAG_BYTES)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ONLY_ONE_JSON_TAG);
  }

  int32_t tagsLen = 0;
  for (int32_t i = 0; i < pTableMeta->tableInfo.numOfTags; ++i) {
    tagsLen += pTagsSchema[i].bytes;
  }

  if (TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES == pStmt->alterType ||
      TSDB_ALTER_TABLE_UPDATE_TAG_BYTES == pStmt->alterType || TSDB_ALTER_TABLE_DROP_COLUMN == pStmt->alterType ||
      TSDB_ALTER_TABLE_DROP_TAG == pStmt->alterType) {
    if (TSDB_SUPER_TABLE != pTableMeta->tableType) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE, "Table is not super table");
    }

    const SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
    if (NULL == pSchema) {
      return generateSyntaxErrMsg(
          &pCxt->msgBuf,
          (TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES == pStmt->alterType || TSDB_ALTER_TABLE_DROP_COLUMN == pStmt->alterType)
              ? TSDB_CODE_PAR_INVALID_COLUMN
              : TSDB_CODE_PAR_INVALID_TAG_NAME,
          pStmt->colName);
    }
    if (hasPkInTable(pTableMeta) && (pSchema->flags & COL_IS_KEY)) {
      if (TSDB_ALTER_TABLE_DROP_COLUMN == pStmt->alterType ||
          TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES == pStmt->alterType ||
          TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME == pStmt->alterType) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PK_OP, pStmt->colName);
      }
    }
    if ((TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES == pStmt->alterType ||
         TSDB_ALTER_TABLE_UPDATE_TAG_BYTES == pStmt->alterType) &&
        (!IS_VAR_DATA_TYPE(pSchema->type) || pSchema->type != pStmt->dataType.type ||
         pSchema->bytes >= calcTypeBytes(pStmt->dataType))) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_MODIFY_COL);
    }

    if (TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES == pStmt->alterType) {
      if ((TSDB_DATA_TYPE_VARCHAR == pStmt->dataType.type && calcTypeBytes(pStmt->dataType) > TSDB_MAX_BINARY_LEN) ||
          (TSDB_DATA_TYPE_VARBINARY == pStmt->dataType.type && calcTypeBytes(pStmt->dataType) > TSDB_MAX_BINARY_LEN) ||
          (TSDB_DATA_TYPE_NCHAR == pStmt->dataType.type && calcTypeBytes(pStmt->dataType) > TSDB_MAX_NCHAR_LEN)) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
      }

      if (pTableMeta->tableInfo.rowSize + calcTypeBytes(pStmt->dataType) - pSchema->bytes > TSDB_MAX_BYTES_PER_ROW) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ROW_LENGTH, TSDB_MAX_BYTES_PER_ROW);
      }
    }

    if (TSDB_ALTER_TABLE_UPDATE_TAG_BYTES == pStmt->alterType) {
      if (calcTypeBytes(pStmt->dataType) > TSDB_MAX_TAGS_LEN) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
      }

      if (tagsLen + calcTypeBytes(pStmt->dataType) - pSchema->bytes > TSDB_MAX_TAGS_LEN) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAGS_LENGTH, TSDB_MAX_TAGS_LEN);
      }
    }
  }

  if (TSDB_ALTER_TABLE_ADD_COLUMN == pStmt->alterType) {
    if (TSDB_MAX_COLUMNS == pTableMeta->tableInfo.numOfColumns) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TOO_MANY_COLUMNS);
    }

    if ((TSDB_DATA_TYPE_VARCHAR == pStmt->dataType.type && calcTypeBytes(pStmt->dataType) > TSDB_MAX_BINARY_LEN) ||
        (TSDB_DATA_TYPE_VARBINARY == pStmt->dataType.type && calcTypeBytes(pStmt->dataType) > TSDB_MAX_BINARY_LEN) ||
        (TSDB_DATA_TYPE_NCHAR == pStmt->dataType.type && calcTypeBytes(pStmt->dataType) > TSDB_MAX_NCHAR_LEN)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
    }

    if (pTableMeta->tableInfo.rowSize + calcTypeBytes(pStmt->dataType) > TSDB_MAX_BYTES_PER_ROW) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ROW_LENGTH, TSDB_MAX_BYTES_PER_ROW);
    }
  }

  if (TSDB_ALTER_TABLE_ADD_TAG == pStmt->alterType) {
    if (TSDB_MAX_TAGS == pTableMeta->tableInfo.numOfTags) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAGS_NUM);
    }

    if (tagsLen + calcTypeBytes(pStmt->dataType) > TSDB_MAX_TAGS_LEN) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAGS_LENGTH, TSDB_MAX_TAGS_LEN);
    }
  }

  if (getNumOfTags(pTableMeta) == 1 && pStmt->alterType == TSDB_ALTER_TABLE_DROP_TAG) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE, "the only tag cannot be dropped");
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkAlterSuperTable(STranslateContext* pCxt, SAlterTableStmt* pStmt) {
  if (IS_SYS_DBNAME(pStmt->dbName)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION,
                                   "Cannot alter table of system database: `%s`.`%s`", pStmt->dbName, pStmt->tableName);
  }

  if (TSDB_ALTER_TABLE_UPDATE_TAG_VAL == pStmt->alterType ||
      TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL == pStmt->alterType) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE,
                                   "Set tag value only available for child table");
  }
  if (TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF == pStmt->alterType) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE,
                                   "Add column with column reference only available for virtual normal table");
  }

  if (TSDB_ALTER_TABLE_ALTER_COLUMN_REF == pStmt->alterType) {
    return generateSyntaxErrMsgExt(
        &pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE,
        "alter column reference only available for virtual normal table and virtual child table");
  }

  if (TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME == pStmt->alterType) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE,
                                   "Rename column only available for normal table");
  }

  if (pStmt->alterType == TSDB_ALTER_TABLE_UPDATE_OPTIONS && -1 != pStmt->pOptions->ttl) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }

  if (pStmt->dataType.type == TSDB_DATA_TYPE_JSON && pStmt->alterType == TSDB_ALTER_TABLE_ADD_TAG) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ONLY_ONE_JSON_TAG);
  }

  if (pStmt->dataType.type == TSDB_DATA_TYPE_JSON && pStmt->alterType == TSDB_ALTER_TABLE_ADD_COLUMN) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COL_JSON);
  }

  SDbCfgInfo dbCfg = {0};
  int32_t    code = getDBCfg(pCxt, pStmt->dbName, &dbCfg);
  if (TSDB_CODE_SUCCESS == code && NULL != dbCfg.pRetensions &&
      (TSDB_ALTER_TABLE_ADD_COLUMN == pStmt->alterType || TSDB_ALTER_TABLE_DROP_COLUMN == pStmt->alterType ||
       TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES == pStmt->alterType)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE,
                                   "Modifying the table schema is not supported in databases "
                                   "configured with the 'RETENTIONS' option");
  }
  STableMeta* pTableMeta = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getTableMeta(pCxt, pStmt->dbName, pStmt->tableName, &pTableMeta);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkAlterSuperTableBySchema(pCxt, pStmt, pTableMeta);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkAlterTableByColumnType(pCxt, pStmt, pTableMeta);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableKeepOption(pCxt, pStmt->pOptions, true, dbCfg.daysToKeep2);
  }
  taosMemoryFree(pTableMeta);
  return code;
}

static int32_t translateAlterSuperTable(STranslateContext* pCxt, SAlterTableStmt* pStmt) {
  SMAlterStbReq alterReq = {0};
  int32_t       code = checkAlterSuperTable(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildAlterSuperTableReq(pCxt, pStmt, &alterReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_ALTER_STB, (FSerializeFunc)tSerializeSMAlterStbReq, &alterReq);
  }
  tFreeSMAltertbReq(&alterReq);
  return code;
}

static int32_t translateUseDatabase(STranslateContext* pCxt, SUseDatabaseStmt* pStmt) {
  SUseDbReq usedbReq = {0};
  SName     name = {0};
  int32_t   code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS == code) {
    code = tNameExtractFullName(&name, usedbReq.db);
  }
  if (TSDB_CODE_SUCCESS == code)
    code =
        getDBVgVersion(pCxt, usedbReq.db, &usedbReq.vgVersion, &usedbReq.dbId, &usedbReq.numOfTable, &usedbReq.stateTs);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_USE_DB, (FSerializeFunc)tSerializeSUseDbReq, &usedbReq);
  }
  return code;
}

static int32_t translateCreateUser(STranslateContext* pCxt, SCreateUserStmt* pStmt) {
  int32_t        code = 0;
  SCreateUserReq createReq = {0};
  if ((code = checkRangeOption(pCxt, TSDB_CODE_INVALID_OPTION, "sysinfo", pStmt->sysinfo, 0, 1, false))) {
    return code;
  }
  tstrncpy(createReq.user, pStmt->userName, TSDB_USER_LEN);
  createReq.createType = 0;
  createReq.superUser = 0;
  createReq.sysInfo = pStmt->sysinfo;
  createReq.enable = 1;
  createReq.isImport = pStmt->isImport;
  createReq.createDb = pStmt->createDb;

  if (pStmt->isImport == 1) {
    tstrncpy(createReq.pass, pStmt->password, TSDB_USET_PASSWORD_LEN);
  } else {
    taosEncryptPass_c((uint8_t*)pStmt->password, strlen(pStmt->password), createReq.pass);
  }
  createReq.passIsMd5 = 1;

  createReq.numIpRanges = pStmt->numIpRanges;
  if (pStmt->numIpRanges > 0) {
    createReq.pIpRanges = taosMemoryCalloc(1, createReq.numIpRanges * sizeof(SIpV4Range));
    if (!createReq.pIpRanges) {
      return terrno;
    }

    createReq.pIpDualRanges = taosMemoryMalloc(createReq.numIpRanges * sizeof(SIpRange));
    if (!createReq.pIpDualRanges) {
      tFreeSCreateUserReq(&createReq);
      return terrno;
    }

    memcpy(createReq.pIpDualRanges, pStmt->pIpRanges, sizeof(SIpRange) * createReq.numIpRanges);
  }
  code = buildCmdMsg(pCxt, TDMT_MND_CREATE_USER, (FSerializeFunc)tSerializeSCreateUserReq, &createReq);
  tFreeSCreateUserReq(&createReq);
  return code;
}

static int32_t checkAlterUser(STranslateContext* pCxt, SAlterUserStmt* pStmt) {
  int32_t code = 0;
  switch (pStmt->alterType) {
    case TSDB_ALTER_USER_ENABLE:
      code = checkRangeOption(pCxt, TSDB_CODE_INVALID_OPTION, "enable", pStmt->enable, 0, 1, false);
      break;
    case TSDB_ALTER_USER_SYSINFO:
      code = checkRangeOption(pCxt, TSDB_CODE_INVALID_OPTION, "sysinfo", pStmt->sysinfo, 0, 1, false);
      break;
    case TSDB_ALTER_USER_CREATEDB:
      code = checkRangeOption(pCxt, TSDB_CODE_INVALID_OPTION, "createdb", pStmt->createdb, 0, 1, false);
      break;
  }
  return code;
}

static int32_t translateAlterUser(STranslateContext* pCxt, SAlterUserStmt* pStmt) {
  int32_t       code = 0;
  SAlterUserReq alterReq = {0};
  if ((code = checkAlterUser(pCxt, pStmt))) {
    return code;
  }
  tstrncpy(alterReq.user, pStmt->userName, TSDB_USER_LEN);
  alterReq.alterType = pStmt->alterType;
  alterReq.superUser = 0;
  alterReq.enable = pStmt->enable;
  alterReq.sysInfo = pStmt->sysinfo;
  alterReq.createdb = pStmt->createdb ? 1 : 0;

  int32_t len = strlen(pStmt->password);
  if (len > 0) {
    taosEncryptPass_c((uint8_t*)pStmt->password, len, alterReq.pass);
    alterReq.passIsMd5 = 1;
  }

  if (NULL != pCxt->pParseCxt->db) {
    snprintf(alterReq.objname, sizeof(alterReq.objname), "%s", pCxt->pParseCxt->db);
  }

  alterReq.numIpRanges = pStmt->numIpRanges;
  if (pStmt->numIpRanges > 0) {
    alterReq.pIpRanges = taosMemoryCalloc(1, alterReq.numIpRanges * sizeof(SIpV4Range));
    if (!alterReq.pIpRanges) {
      tFreeSAlterUserReq(&alterReq);
      return terrno;
    }

    alterReq.pIpDualRanges = taosMemoryMalloc(alterReq.numIpRanges * sizeof(SIpRange));
    if (!alterReq.pIpDualRanges) {
      tFreeSAlterUserReq(&alterReq);
      return terrno;
    }
    memcpy(alterReq.pIpDualRanges, pStmt->pIpRanges, sizeof(SIpRange) * alterReq.numIpRanges);
  }
  code = buildCmdMsg(pCxt, TDMT_MND_ALTER_USER, (FSerializeFunc)tSerializeSAlterUserReq, &alterReq);
  tFreeSAlterUserReq(&alterReq);
  return code;
}

static int32_t translateDropUser(STranslateContext* pCxt, SDropUserStmt* pStmt) {
  SDropUserReq dropReq = {0};
  tstrncpy(dropReq.user, pStmt->userName, TSDB_USER_LEN);

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_DROP_USER, (FSerializeFunc)tSerializeSDropUserReq, &dropReq);
  tFreeSDropUserReq(&dropReq);
  return code;
}

static int32_t translateCreateAnode(STranslateContext* pCxt, SCreateAnodeStmt* pStmt) {
  SMCreateAnodeReq createReq = {0};
  createReq.urlLen = strlen(pStmt->url) + 1;
  if (createReq.urlLen > TSDB_ANALYTIC_ANODE_URL_LEN) {
    return TSDB_CODE_MND_ANODE_TOO_LONG_URL;
  }

  createReq.url = taosMemoryCalloc(createReq.urlLen, 1);
  if (createReq.url == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  tstrncpy(createReq.url, pStmt->url, createReq.urlLen);

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_CREATE_ANODE, (FSerializeFunc)tSerializeSMCreateAnodeReq, &createReq);
  tFreeSMCreateAnodeReq(&createReq);
  return code;
}

static int32_t translateDropAnode(STranslateContext* pCxt, SDropAnodeStmt* pStmt) {
  SMDropAnodeReq dropReq = {0};
  dropReq.anodeId = pStmt->anodeId;

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_DROP_ANODE, (FSerializeFunc)tSerializeSMDropAnodeReq, &dropReq);
  tFreeSMDropAnodeReq(&dropReq);
  return code;
}

static int32_t translateUpdateAnode(STranslateContext* pCxt, SUpdateAnodeStmt* pStmt) {
  SMUpdateAnodeReq updateReq = {0};
  updateReq.anodeId = pStmt->anodeId;

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_UPDATE_ANODE, (FSerializeFunc)tSerializeSMUpdateAnodeReq, &updateReq);
  tFreeSMUpdateAnodeReq(&updateReq);
  return code;
}

static int32_t checkCreateBnode(STranslateContext* pCxt, SCreateBnodeStmt* pStmt) {
  SBnodeOptions* pOptions = pStmt->pOptions;

  if ('\0' != pOptions->protoStr[0]) {
    if (0 == strcasecmp(pOptions->protoStr, TSDB_BNODE_OPT_PROTO_STR_MQTT)) {
      pOptions->proto = TSDB_BNODE_OPT_PROTO_MQTT;
    } else {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_BNODE_OPTION, "Invalid option protocol: %s",
                                     pOptions->protoStr);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateCreateBnode(STranslateContext* pCxt, SCreateBnodeStmt* pStmt) {
  SMCreateBnodeReq createReq = {.dnodeId = pStmt->dnodeId};

  int32_t code = checkCreateBnode(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    createReq.bnodeProto = pStmt->pOptions->proto;

    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_BNODE, (FSerializeFunc)tSerializeSMCreateBnodeReq, &createReq);
  }

  tFreeSMCreateBnodeReq(&createReq);
  return code;
}

static int32_t translateDropBnode(STranslateContext* pCxt, SDropBnodeStmt* pStmt) {
  SMDropBnodeReq dropReq = {0};
  dropReq.dnodeId = pStmt->dnodeId;

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_DROP_BNODE, (FSerializeFunc)tSerializeSMDropBnodeReq, &dropReq);
  tFreeSMDropBnodeReq(&dropReq);
  return code;
}

static int32_t translateCreateDnode(STranslateContext* pCxt, SCreateDnodeStmt* pStmt) {
  SCreateDnodeReq createReq = {0};
  tstrncpy(createReq.fqdn, pStmt->fqdn, TSDB_FQDN_LEN);
  createReq.port = pStmt->port;

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_CREATE_DNODE, (FSerializeFunc)tSerializeSCreateDnodeReq, &createReq);
  tFreeSCreateDnodeReq(&createReq);
  return code;
}

static int32_t translateDropDnode(STranslateContext* pCxt, SDropDnodeStmt* pStmt) {
  SDropDnodeReq dropReq = {0};
  dropReq.dnodeId = pStmt->dnodeId;
  tstrncpy(dropReq.fqdn, pStmt->fqdn, TSDB_FQDN_LEN);
  dropReq.port = pStmt->port;
  dropReq.force = pStmt->force;
  dropReq.unsafe = pStmt->unsafe;

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_DROP_DNODE, (FSerializeFunc)tSerializeSDropDnodeReq, &dropReq);
  tFreeSDropDnodeReq(&dropReq);
  return code;
}

#define MIN_MAX_COMPACT_TASKS 1
#define MAX_MAX_COMPACT_TASKS 100

static int32_t translateAlterDnode(STranslateContext* pCxt, SAlterDnodeStmt* pStmt) {
  SMCfgDnodeReq cfgReq = {0};
  cfgReq.dnodeId = pStmt->dnodeId;
  tstrncpy(cfgReq.config, pStmt->config, TSDB_DNODE_CONFIG_LEN);
  tstrncpy(cfgReq.value, pStmt->value, TSDB_DNODE_VALUE_LEN);

  int32_t code = 0;

  const char* validConfigs[] = {
      "encrypt_key",
  };
  if (0 == taosStrncasecmp(cfgReq.config, validConfigs[0], strlen(validConfigs[0]) + 1)) {
    int32_t klen = strlen(cfgReq.value);
    if (klen > ENCRYPT_KEY_LEN || klen < ENCRYPT_KEY_LEN_MIN) {
      tFreeSMCfgDnodeReq(&cfgReq);
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_DNODE_INVALID_ENCRYPT_KLEN,
                                     "Invalid encryption key length: %d, valid range [%d,%d]", klen,
                                     ENCRYPT_KEY_LEN_MIN, ENCRYPT_KEY_LEN);
    }
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_ENCRYPT_KEY, (FSerializeFunc)tSerializeSMCfgDnodeReq, &cfgReq);
  } else {
    code = buildCmdMsg(pCxt, TDMT_MND_CONFIG_DNODE, (FSerializeFunc)tSerializeSMCfgDnodeReq, &cfgReq);
  }

  tFreeSMCfgDnodeReq(&cfgReq);
  return code;
}

static int32_t translateRestoreDnode(STranslateContext* pCxt, SRestoreComponentNodeStmt* pStmt) {
  SRestoreDnodeReq restoreReq = {0};
  restoreReq.dnodeId = pStmt->dnodeId;
  switch (nodeType((SNode*)pStmt)) {
    case QUERY_NODE_RESTORE_DNODE_STMT:
      restoreReq.restoreType = RESTORE_TYPE__ALL;
      break;
    case QUERY_NODE_RESTORE_QNODE_STMT:
      restoreReq.restoreType = RESTORE_TYPE__QNODE;
      break;
    case QUERY_NODE_RESTORE_MNODE_STMT:
      restoreReq.restoreType = RESTORE_TYPE__MNODE;
      break;
    case QUERY_NODE_RESTORE_VNODE_STMT:
      restoreReq.restoreType = RESTORE_TYPE__VNODE;
      break;
    default:
      return -1;
  }

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_RESTORE_DNODE, (FSerializeFunc)tSerializeSRestoreDnodeReq, &restoreReq);
  tFreeSRestoreDnodeReq(&restoreReq);
  return code;
}

static int32_t translateAlterCluster(STranslateContext* pCxt, SAlterClusterStmt* pStmt) {
  SMCfgClusterReq cfgReq = {0};
  tstrncpy(cfgReq.config, pStmt->config, TSDB_DNODE_CONFIG_LEN);
  tstrncpy(cfgReq.value, pStmt->value, TSDB_CLUSTER_VALUE_LEN);

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_CONFIG_CLUSTER, (FSerializeFunc)tSerializeSMCfgClusterReq, &cfgReq);
  tFreeSMCfgClusterReq(&cfgReq);
  return code;
}


static int32_t getSmaIndexSql(STranslateContext* pCxt, char** pSql, int32_t* pLen) {
  *pSql = taosStrdup(pCxt->pParseCxt->pSql);
  if (NULL == *pSql) {
    return terrno;
  }
  *pLen = pCxt->pParseCxt->sqlLen + 1;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildCreateFullTextReq(STranslateContext* pCxt, SCreateIndexStmt* pStmt, SMCreateFullTextReq* pReq) {
  // impl later
  return TSDB_CODE_SUCCESS;
}

static int32_t buildCreateTagIndexReq(STranslateContext* pCxt, SCreateIndexStmt* pStmt, SCreateTagIndexReq* pReq) {
  SName name = {0};
  toName(pCxt->pParseCxt->acctId, pStmt->indexDbName, pStmt->indexName, &name);
  int32_t code = tNameExtractFullName(&name, pReq->idxName);
  if (TSDB_CODE_SUCCESS == code) {
    memset(&name, 0, sizeof(SName));
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);
    code = tNameExtractFullName(&name, pReq->stbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    memset(&name, 0, sizeof(SName));
    code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  }
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, pReq->dbFName);

  SNode* pNode = NULL;
  if (LIST_LENGTH(pStmt->pCols) != 1) {
    return TSDB_CODE_PAR_INVALID_TAGS_NUM;
  }
  FOREACH(pNode, pStmt->pCols) {
    SColumnNode* p = (SColumnNode*)pNode;
    memcpy(pReq->colName, p->colName, sizeof(p->colName));
  }

  // impl later
  return TSDB_CODE_SUCCESS;
}

static int32_t translateCreateFullTextIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt) {
  SMCreateFullTextReq createFTReq = {0};
  int32_t             code = buildCreateFullTextReq(pCxt, pStmt, &createFTReq);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_INDEX, (FSerializeFunc)tSerializeSMCreateFullTextReq, &createFTReq);
  }
  tFreeSMCreateFullTextReq(&createFTReq);
  return code;
}

static int32_t translateCreateNormalIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt) {
  int32_t     code = 0;
  SName       name = {0};
  STableMeta* pMeta = NULL;
  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);
  code = getTargetMeta(pCxt, &name, &pMeta, false);
  if (code) {
    taosMemoryFree(pMeta);
    return code;
  }

  if (LIST_LENGTH(pStmt->pCols) != 1) {
    taosMemoryFree(pMeta);
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAGS_NUM, "Only one tag is allowed");
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pStmt->pCols) {
    const SSchema* pSchema = getTagSchema(pMeta, ((SColumnNode*)pNode)->colName);
    if (!pSchema) {
      taosMemoryFree(pMeta);
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAG_NAME, ((SColumnNode*)pNode)->colName);
    }
  }

  SCreateTagIndexReq createTagIdxReq = {0};
  code = buildCreateTagIndexReq(pCxt, pStmt, &createTagIdxReq);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_INDEX, (FSerializeFunc)tSerializeSCreateTagIdxReq, &createTagIdxReq);
  }

  taosMemoryFree(pMeta);
  return code;
}

static int32_t translateCreateIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt) {
  if (INDEX_TYPE_FULLTEXT == pStmt->indexType) {
    return translateCreateFullTextIndex(pCxt, pStmt);
  } else if (INDEX_TYPE_NORMAL == pStmt->indexType) {
    return translateCreateNormalIndex(pCxt, pStmt);
  }
  return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_SMA_INDEX,
                                 "Unsupported index type", pStmt->indexType);
}

static int32_t translateDropIndex(STranslateContext* pCxt, SDropIndexStmt* pStmt) {
  SMDropSmaReq dropSmaReq = {0};
  SName        name = {0};
  toName(pCxt->pParseCxt->acctId, pStmt->indexDbName, pStmt->indexName, &name);
  int32_t code = tNameExtractFullName(&name, dropSmaReq.name);
  if (TSDB_CODE_SUCCESS != code) return code;
  dropSmaReq.igNotExists = pStmt->ignoreNotExists;
  return buildCmdMsg(pCxt, TDMT_MND_DROP_SMA, (FSerializeFunc)tSerializeSMDropSmaReq, &dropSmaReq);
}

static int16_t getCreateComponentNodeMsgType(ENodeType type) {
  switch (type) {
    case QUERY_NODE_CREATE_QNODE_STMT:
      return TDMT_MND_CREATE_QNODE;
    case QUERY_NODE_CREATE_BACKUP_NODE_STMT:
      return TDMT_MND_CREATE_BACKUP_NODE;
    case QUERY_NODE_CREATE_SNODE_STMT:
      return TDMT_MND_CREATE_SNODE;
    case QUERY_NODE_CREATE_MNODE_STMT:
      return TDMT_MND_CREATE_MNODE;
    default:
      break;
  }
  return -1;
}

static int32_t translateCreateComponentNode(STranslateContext* pCxt, SCreateComponentNodeStmt* pStmt) {
  SMCreateQnodeReq createReq = {.dnodeId = pStmt->dnodeId};
  int32_t          code = buildCmdMsg(pCxt, getCreateComponentNodeMsgType(nodeType(pStmt)),
                                      (FSerializeFunc)tSerializeSCreateDropMQSNodeReq, &createReq);
  tFreeSMCreateQnodeReq(&createReq);
  return code;
}

static int16_t getDropComponentNodeMsgType(ENodeType type) {
  switch (type) {
    case QUERY_NODE_DROP_QNODE_STMT:
      return TDMT_MND_DROP_QNODE;
    case QUERY_NODE_DROP_BACKUP_NODE_STMT:
      return TDMT_MND_DROP_BACKUP_NODE;
    case QUERY_NODE_DROP_SNODE_STMT:
      return TDMT_MND_DROP_SNODE;
    case QUERY_NODE_DROP_MNODE_STMT:
      return TDMT_MND_DROP_MNODE;
    default:
      break;
  }
  return -1;
}

static int32_t translateDropComponentNode(STranslateContext* pCxt, SDropComponentNodeStmt* pStmt) {
  SDDropQnodeReq dropReq = {.dnodeId = pStmt->dnodeId};
  int32_t        code = buildCmdMsg(pCxt, getDropComponentNodeMsgType(nodeType(pStmt)),
                                    (FSerializeFunc)tSerializeSCreateDropMQSNodeReq, &dropReq);
  tFreeSDDropQnodeReq(&dropReq);
  return code;
}

static int32_t checkTopicQuery(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (pSelect->hasAggFuncs || pSelect->hasForecastFunc || pSelect->hasInterpFunc || pSelect->hasIndefiniteRowsFunc) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TOPIC_QUERY);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildCreateTopicReq(STranslateContext* pCxt, SCreateTopicStmt* pStmt, SCMCreateTopicReq* pReq) {
  snprintf(pReq->name, sizeof(pReq->name), "%d.%s", pCxt->pParseCxt->acctId, pStmt->topicName);
  pReq->igExists = pStmt->ignoreExists;
  pReq->withMeta = pStmt->withMeta;

  pReq->sql = taosStrdup(pCxt->pParseCxt->pSql);
  if (NULL == pReq->sql) {
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SName   name = {0};
  if ('\0' != pStmt->subSTbName[0]) {
    pReq->subType = TOPIC_SUB_TYPE__TABLE;
    toName(pCxt->pParseCxt->acctId, pStmt->subDbName, pStmt->subSTbName, &name);
    (void)tNameGetFullDbName(&name, pReq->subDbName);
    if (TSDB_CODE_SUCCESS == code) {
      code = tNameExtractFullName(&name, pReq->subStbName);
      if (TSDB_CODE_SUCCESS == code && pStmt->pQuery != NULL) {
        code = nodesNodeToString(pStmt->pQuery, false, &pReq->ast, NULL);
      }
    }
  } else if ('\0' != pStmt->subDbName[0]) {
    pReq->subType = TOPIC_SUB_TYPE__DB;
    code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->subDbName, strlen(pStmt->subDbName));
    if (TSDB_CODE_SUCCESS == code) (void)tNameGetFullDbName(&name, pReq->subDbName);
  } else {
    pReq->subType = TOPIC_SUB_TYPE__COLUMN;
    char* dbName = ((SRealTableNode*)(((SSelectStmt*)pStmt->pQuery)->pFromTable))->table.dbName;
    code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, dbName, strlen(dbName));
    if (TSDB_CODE_SUCCESS == code) {
      (void)tNameGetFullDbName(&name, pReq->subDbName);
      pCxt->pParseCxt->topicQuery = true;
      code = translateQuery(pCxt, pStmt->pQuery);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = checkTopicQuery(pCxt, (SSelectStmt*)pStmt->pQuery);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesNodeToString(pStmt->pQuery, false, &pReq->ast, NULL);
    }
  }

  return code;
}

static int32_t addTagList(SNodeList** ppList, SNode* pNode) {
  int32_t code = 0;
  if (NULL == *ppList) {
    code = nodesMakeList(ppList);
    if (!*ppList) return code;
  }

  code = nodesListAppend(*ppList, pNode);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(*ppList);
    *ppList = NULL;
  }

  return code;
}

static EDealRes checkColumnTagsInCond(SNode* pNode, void* pContext) {
  SBuildTopicContext* pCxt = (SBuildTopicContext*)pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    ETableColumnType type;
    getColumnTypeFromMeta(pCxt->pMeta, ((SColumnNode*)pNode)->colName, &type);
    if (type == TCOL_TYPE_COLUMN) {
      pCxt->colExists = true;
      return DEAL_RES_ERROR;
    } else if (type == TCOL_TYPE_TAG) {
      SNode* pNew = NULL;
      pCxt->code = nodesCloneNode(pNode, &pNew);
      if (TSDB_CODE_SUCCESS != pCxt->code) return DEAL_RES_ERROR;
      pCxt->code = addTagList(&pCxt->pTags, pNew);
      if (TSDB_CODE_SUCCESS != pCxt->code) {
        nodesDestroyNode(pNew);
        return DEAL_RES_ERROR;
      }
    } else {
      pCxt->colNotFound = true;
      return DEAL_RES_ERROR;
    }
  } else if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (0 == strcasecmp(pFunc->functionName, "tbname")) {
      SNode* pNew = NULL;
      pCxt->code = nodesCloneNode(pNode, &pNew);
      if (TSDB_CODE_SUCCESS != pCxt->code) return DEAL_RES_ERROR;
      if (TSDB_CODE_SUCCESS != (pCxt->code = addTagList(&pCxt->pTags, pNew))) {
        nodesDestroyNode(pNew);
        return DEAL_RES_ERROR;
      }
    }
  }

  return DEAL_RES_CONTINUE;
}

static int32_t checkCollectTopicTags(STranslateContext* pCxt, SCreateTopicStmt* pStmt, STableMeta* pMeta,
                                     SNodeList** ppProjection) {
  SBuildTopicContext colCxt = {.colExists = false, .colNotFound = false, .pMeta = pMeta, .pTags = NULL};
  nodesWalkExprPostOrder(pStmt->pWhere, checkColumnTagsInCond, &colCxt);
  if (colCxt.colNotFound) {
    nodesDestroyList(colCxt.pTags);
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Invalid column name");
  } else if (colCxt.colExists) {
    nodesDestroyList(colCxt.pTags);
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Columns are forbidden in where clause");
  }
  if (NULL == colCxt.pTags) {  // put one column to select
                               //    for (int32_t i = 0; i < pMeta->tableInfo.numOfColumns; ++i) {
    SSchema*     column = &pMeta->schema[0];
    SColumnNode* col = NULL;
    int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&col);
    if (NULL == col) {
      return code;
    }
    tstrncpy(col->colName, column->name, TSDB_COL_NAME_LEN);
    tstrncpy(col->node.aliasName, col->colName, TSDB_COL_NAME_LEN);
    tstrncpy(col->node.userAlias, col->colName, TSDB_COL_NAME_LEN);
    code = addTagList(&colCxt.pTags, (SNode*)col);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)col);
      return code;
    }
  }

  *ppProjection = colCxt.pTags;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildQueryForTableTopic(STranslateContext* pCxt, SCreateTopicStmt* pStmt, SNode** ppSelect) {
  SParseContext*   pParCxt = pCxt->pParseCxt;
  SRequestConnInfo connInfo = {.pTrans = pParCxt->pTransporter,
                               .requestId = pParCxt->requestId,
                               .requestObjRefId = pParCxt->requestRid,
                               .mgmtEps = pParCxt->mgmtEpSet};
  SName            name = {0};
  STableMeta*      pMeta = NULL;
  toName(pParCxt->acctId, pStmt->subDbName, pStmt->subSTbName, &name);
  int32_t code = getTargetMeta(pCxt, &name, &pMeta, false);
  if (code) {
    taosMemoryFree(pMeta);
    return code;
  }
  if (TSDB_SUPER_TABLE != pMeta->tableType) {
    taosMemoryFree(pMeta);
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Only supertable table can be used");
  }

  SNodeList*      pProjection = NULL;
  SRealTableNode* realTable = NULL;
  code = checkCollectTopicTags(pCxt, pStmt, pMeta, &pProjection);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)&realTable);
    if (realTable) {
      tstrncpy(realTable->table.dbName, pStmt->subDbName, TSDB_DB_NAME_LEN);
      tstrncpy(realTable->table.tableName, pStmt->subSTbName, TSDB_TABLE_NAME_LEN);
      tstrncpy(realTable->table.tableAlias, pStmt->subSTbName, TSDB_TABLE_NAME_LEN);
      code = createSelectStmtImpl(true, pProjection, (SNode*)realTable, NULL, ppSelect);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pProjection = NULL;
      realTable = NULL;
      SNode** ppWhere = &((SSelectStmt*)*ppSelect)->pWhere;
      code = nodesCloneNode(pStmt->pWhere, ppWhere);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pCxt->pParseCxt->topicQuery = true;
      code = translateQuery(pCxt, *ppSelect);
    }
  }
  nodesDestroyNode((SNode*)realTable);
  nodesDestroyList(pProjection);
  taosMemoryFree(pMeta);
  return code;
}

static int32_t checkCreateTopic(STranslateContext* pCxt, SCreateTopicStmt* pStmt) {
  if (NULL == pStmt->pQuery && NULL == pStmt->pWhere) {
    return TSDB_CODE_SUCCESS;
  }

  if (pStmt->pWhere) {
    return buildQueryForTableTopic(pCxt, pStmt, &pStmt->pQuery);
  } else if (QUERY_NODE_SELECT_STMT == nodeType(pStmt->pQuery)) {
    SSelectStmt* pSelect = (SSelectStmt*)pStmt->pQuery;
    if (!pSelect->isDistinct &&
        (NULL != pSelect->pFromTable && QUERY_NODE_REAL_TABLE == nodeType(pSelect->pFromTable)) &&
        NULL == pSelect->pGroupByList && NULL == pSelect->pLimit && NULL == pSelect->pSlimit &&
        NULL == pSelect->pOrderByList && NULL == pSelect->pPartitionByList) {
      return TSDB_CODE_SUCCESS;
    }
  }

  return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TOPIC_QUERY);
}

static int32_t translateCreateTopic(STranslateContext* pCxt, SCreateTopicStmt* pStmt) {
  SCMCreateTopicReq createReq = {0};
  int32_t           code = checkCreateTopic(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateTopicReq(pCxt, pStmt, &createReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_TMQ_CREATE_TOPIC, (FSerializeFunc)tSerializeSCMCreateTopicReq, &createReq);
  }
  tFreeSCMCreateTopicReq(&createReq);
  return code;
}

static int32_t translateDropTopic(STranslateContext* pCxt, SDropTopicStmt* pStmt) {
  SMDropTopicReq dropReq = {0};

  snprintf(dropReq.name, sizeof(dropReq.name), "%d.%s", pCxt->pParseCxt->acctId, pStmt->topicName);
  dropReq.igNotExists = pStmt->ignoreNotExists;
  dropReq.force = pStmt->force;

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_TMQ_DROP_TOPIC, (FSerializeFunc)tSerializeSMDropTopicReq, &dropReq);
  tFreeSMDropTopicReq(&dropReq);
  return code;
}

static int32_t translateDropCGroup(STranslateContext* pCxt, SDropCGroupStmt* pStmt) {
  SMDropCgroupReq dropReq = {0};

  SName   name;
  int32_t code = TSDB_CODE_SUCCESS;
  code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->topicName, strlen(pStmt->topicName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, dropReq.topic);
  dropReq.igNotExists = pStmt->ignoreNotExists;
  dropReq.force = pStmt->force;
  tstrncpy(dropReq.cgroup, pStmt->cgroup, TSDB_CGROUP_LEN);

  return buildCmdMsg(pCxt, TDMT_MND_TMQ_DROP_CGROUP, (FSerializeFunc)tSerializeSMDropCgroupReq, &dropReq);
}

static int32_t translateAlterLocal(STranslateContext* pCxt, SAlterLocalStmt* pStmt) {
  // The statement is executed directly on the client without constructing a message.
  if ('\0' != pStmt->value[0]) {
    return TSDB_CODE_SUCCESS;
  }
  char* p = strchr(pStmt->config, ' ');
  if (NULL != p) {
    *p = 0;
    tstrncpy(pStmt->value, p + 1, sizeof(pStmt->value));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateExplain(STranslateContext* pCxt, SExplainStmt* pStmt) {
  if (pStmt->analyze) {
    pCxt->pExplainOpt = pStmt->pOptions;
  }
  return translateQuery(pCxt, pStmt->pQuery);
}

static int32_t translateDescribe(STranslateContext* pCxt, SDescribeStmt* pStmt) {
  int32_t code = refreshGetTableMeta(pCxt, pStmt->dbName, pStmt->tableName, &pStmt->pMeta);
#ifdef TD_ENTERPRISE
  if (TSDB_CODE_PAR_TABLE_NOT_EXIST == code) {
    int32_t origCode = code;
    SName   name = {0};
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);
    SViewMeta* pMeta = NULL;
    code = getViewMetaFromMetaCache(pCxt, &name, &pMeta);
    if (TSDB_CODE_SUCCESS != code) {
      code = origCode;
    } else {
      SParseSqlRes res = {.resType = PARSE_SQL_RES_SCHEMA};
      char         dbFName[TSDB_DB_FNAME_LEN];
      (void)tNameGetFullDbName(&name, dbFName);
      code = (*pCxt->pParseCxt->parseSqlFp)(pCxt->pParseCxt->parseSqlParam, name.dbname, pMeta->querySql, false,
                                            pMeta->user, &res);
      if (TSDB_CODE_SUCCESS == code) {
        code = collectUseTable(&name, pCxt->pTargetTables);
      }
      if (TSDB_CODE_SUCCESS == code) {
        SViewMeta viewMeta = {0};
        viewMeta.viewId = pMeta->viewId;
        viewMeta.precision = res.schemaRes.precision;
        viewMeta.type = pMeta->type;
        viewMeta.version = pMeta->version;
        viewMeta.numOfCols = res.schemaRes.numOfCols;
        viewMeta.pSchema = res.schemaRes.pSchema;
        code = buildTableMetaFromViewMeta(&pStmt->pMeta, &viewMeta);
        parserDebug("rebuild view meta, view:%s.%s, numOfCols:%d, code:0x%x", dbFName, pStmt->tableName,
                    viewMeta.numOfCols, code);
      }
      taosMemoryFree(res.schemaRes.pSchema);
    }
  }
#endif

  return code;
}

static int32_t translateTimeRange(STranslateContext* pCxt, const char* dbName, SNode* pStart, SNode* pEnd,
                                  STimeWindow* timeRange) {
  SDbCfgInfo dbCfg = {0};
  int32_t    code = getDBCfg(pCxt, dbName, &dbCfg);
  if (TSDB_CODE_SUCCESS == code && NULL != pStart) {
    ((SValueNode*)pStart)->node.resType.precision = dbCfg.precision;
    ((SValueNode*)pStart)->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    code = doTranslateValue(pCxt, (SValueNode*)pStart);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pEnd) {
    ((SValueNode*)pEnd)->node.resType.precision = dbCfg.precision;
    ((SValueNode*)pEnd)->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    code = doTranslateValue(pCxt, (SValueNode*)pEnd);
  }
  if (TSDB_CODE_SUCCESS == code) {
    timeRange->skey = NULL != pStart ? ((SValueNode*)pStart)->datum.i : INT64_MIN;
    timeRange->ekey = NULL != pEnd ? ((SValueNode*)pEnd)->datum.i : INT64_MAX;
  }
  return code;
}

static int32_t translateCompactDb(STranslateContext* pCxt, SCompactDatabaseStmt* pStmt) {
  SCompactDbReq compactReq = {
      .metaOnly = pStmt->metaOnly,
      .force = pStmt->force,
  };
  SName   name;
  int32_t code = TSDB_CODE_SUCCESS;
  code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS != code) return code;

  (void)tNameGetFullDbName(&name, compactReq.db);
  code = translateTimeRange(pCxt, pStmt->dbName, pStmt->pStart, pStmt->pEnd, &compactReq.timeRange);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_COMPACT_DB, (FSerializeFunc)tSerializeSCompactDbReq, &compactReq);
  }
  tFreeSCompactDbReq(&compactReq);
  return code;
}


#ifdef TD_ENTERPRISE
static int32_t translateRollupAdjustTimeRange(STranslateContext* pCxt, const char* dbName, STrimDbReq* req) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SDbCfgInfo dbCfg = {0};
  code = getDBCfg(pCxt, dbName, &dbCfg);
  if (TSDB_CODE_SUCCESS == code) {
    if (req->tw.skey != INT64_MIN) {
      req->tw.skey = convertTimePrecision(req->tw.skey, dbCfg.precision, TSDB_TIME_PRECISION_MILLI);
      req->tw.skey /= 1000;  // convert to second if specified
    }
    if (req->tw.ekey != INT64_MAX) {
      req->tw.ekey = convertTimePrecision(req->tw.ekey, dbCfg.precision, TSDB_TIME_PRECISION_MILLI);
      req->tw.ekey /= 1000;  // convert to second if specified
    } else {
      req->tw.ekey = taosGetTimestampMs() / 1000;
    }
  }
  return code;
}
#endif

static int32_t translateRollupDb(STranslateContext* pCxt, SRollupDatabaseStmt* pStmt) {
#ifdef TD_ENTERPRISE
  int32_t    code = TSDB_CODE_SUCCESS;
  STrimDbReq req = {.optrType = TSDB_OPTR_ROLLUP, .triggerType = TSDB_TRIGGER_MANUAL};
  SName      name;
  code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, req.db);
  code = translateTimeRange(pCxt, pStmt->dbName, pStmt->pStart, pStmt->pEnd, &req.tw);

  if (TSDB_CODE_SUCCESS == code) {
    code = translateRollupAdjustTimeRange(pCxt, pStmt->dbName, &req);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_TRIM_DB, (FSerializeFunc)tSerializeSTrimDbReq, &req);
  }
  tFreeSTrimDbReq(&req);
  return code;
#else
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

static int32_t translateScanDb(STranslateContext* pCxt, SScanDatabaseStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  SName   name;

  SScanDbReq scanReq = {0};
  code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS != code) return code;

  (void)tNameGetFullDbName(&name, scanReq.db);
  code = translateTimeRange(pCxt, pStmt->dbName, pStmt->pStart, pStmt->pEnd, &scanReq.timeRange);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_SCAN_DB, (FSerializeFunc)tSerializeSScanDbReq, &scanReq);
  }
  tFreeSScanDbReq(&scanReq);

  return code;
}

static int32_t translateVgroupList(STranslateContext* pCxt, SNodeList* vgroupList, SArray** ppVgroups) {
  int32_t   code = TSDB_CODE_SUCCESS;
  SHashObj* pHash = NULL;
  int32_t   numOfVgroups = LIST_LENGTH(vgroupList);

  (*ppVgroups) = taosArrayInit(numOfVgroups, sizeof(int64_t));
  if (NULL == *ppVgroups) {
    return terrno;
  }

  pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (NULL == pHash) {
    code = terrno;
  }

  if (TSDB_CODE_SUCCESS == code) {
    SNode* pNode = NULL;
    FOREACH(pNode, vgroupList) {
      SValueNode* pVal = (SValueNode*)pNode;
      if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
        code = TSDB_CODE_VND_INVALID_VGROUP_ID;
        break;
      }

      int64_t vgroupId = getBigintFromValueNode(pVal);

      if (NULL != taosHashGet(pHash, &vgroupId, sizeof(vgroupId))) {
        code = TSDB_CODE_PAR_INVALID_VGID_LIST;
        break;
      }

      code = taosHashPut(pHash, &vgroupId, sizeof(vgroupId), NULL, 0);
      if (code) {
        break;
      }

      if (NULL == taosArrayPush(*ppVgroups, &vgroupId)) {
        code = terrno;
        break;
      }
    }
  }

  taosHashCleanup(pHash);
  if (code) {
    taosArrayDestroy(*ppVgroups);
    *ppVgroups = NULL;
  }
  return code;
}

static int32_t translateCompactVgroups(STranslateContext* pCxt, SCompactVgroupsStmt* pStmt) {
  int32_t       code = TSDB_CODE_SUCCESS;
  SName         name;
  SCompactDbReq req = {
      .metaOnly = pStmt->metaOnly,
      .force = pStmt->force,
  };

  code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, ((SValueNode*)pStmt->pDbName)->literal,
                        strlen(((SValueNode*)pStmt->pDbName)->literal));
  if (TSDB_CODE_SUCCESS == code) {
    (void)tNameGetFullDbName(&name, req.db);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code =
        translateTimeRange(pCxt, ((SValueNode*)pStmt->pDbName)->literal, pStmt->pStart, pStmt->pEnd, &req.timeRange);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = translateVgroupList(pCxt, pStmt->vgidList, &req.vgroupIds);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_COMPACT_DB, (FSerializeFunc)tSerializeSCompactDbReq, &req);
  }

  tFreeSCompactDbReq(&req);
  return code;
}

static int32_t translateRollupVgroups(STranslateContext* pCxt, SRollupVgroupsStmt* pStmt) {
#ifdef TD_ENTERPRISE
  int32_t    code = TSDB_CODE_SUCCESS;
  SName      name;
  STrimDbReq req = {.optrType = TSDB_OPTR_ROLLUP, .triggerType = TSDB_TRIGGER_MANUAL};

  code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, ((SValueNode*)pStmt->pDbName)->literal,
                        strlen(((SValueNode*)pStmt->pDbName)->literal));
  if (TSDB_CODE_SUCCESS == code) {
    (void)tNameGetFullDbName(&name, req.db);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = translateTimeRange(pCxt, ((SValueNode*)pStmt->pDbName)->literal, pStmt->pStart, pStmt->pEnd, &req.tw);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = translateRollupAdjustTimeRange(pCxt, ((SValueNode*)pStmt->pDbName)->literal, &req);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = translateVgroupList(pCxt, pStmt->vgidList, &req.vgroupIds);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_TRIM_DB, (FSerializeFunc)tSerializeSTrimDbReq, &req);
  }

  tFreeSTrimDbReq(&req);
  return code;
#else
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

static int32_t translateScanVgroups(STranslateContext* pCxt, SScanVgroupsStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  SName   name;

  SScanDbReq req = {0};

  code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, ((SValueNode*)pStmt->pDbName)->literal,
                        strlen(((SValueNode*)pStmt->pDbName)->literal));
  if (TSDB_CODE_SUCCESS == code) {
    (void)tNameGetFullDbName(&name, req.db);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = translateTimeRange(pCxt, ((SValueNode*)pStmt->pDbName)->literal, pStmt->pStart, pStmt->pEnd, &req.timeRange);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = translateVgroupList(pCxt, pStmt->vgidList, &req.vgroupIds);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_SCAN_DB, (FSerializeFunc)tSerializeSScanDbReq, &req);
  }

  tFreeSScanDbReq(&req);
  return code;
}

static int32_t translateKillConnection(STranslateContext* pCxt, SKillStmt* pStmt) {
  SKillConnReq killReq = {0};
  killReq.connId = pStmt->targetId;
  return buildCmdMsg(pCxt, TDMT_MND_KILL_CONN, (FSerializeFunc)tSerializeSKillConnReq, &killReq);
}

static int32_t translateKillCompact(STranslateContext* pCxt, SKillStmt* pStmt) {
  SKillCompactReq killReq = {0};
  killReq.compactId = pStmt->targetId;
  return buildCmdMsg(pCxt, TDMT_MND_KILL_COMPACT, (FSerializeFunc)tSerializeSKillCompactReq, &killReq);
}

static int32_t translateKillRetention(STranslateContext* pCxt, SKillStmt* pStmt) {
  SKillRetentionReq killReq = {0};  // same as SKillCompactReq
  killReq.id = pStmt->targetId;
  return buildCmdMsg(pCxt, TDMT_MND_KILL_TRIM, (FSerializeFunc)tSerializeSKillCompactReq, &killReq);
}
static int32_t translateKillScan(STranslateContext* pCxt, SKillStmt* pStmt) {
  SKillScanReq killReq = {0};
  killReq.scanId = pStmt->targetId;
  return buildCmdMsg(pCxt, TDMT_MND_KILL_SCAN, (FSerializeFunc)tSerializeSKillScanReq, &killReq);
}

static int32_t translateKillQuery(STranslateContext* pCxt, SKillQueryStmt* pStmt) {
  SKillQueryReq killReq = {0};
  tstrncpy(killReq.queryStrId, pStmt->queryId, TSDB_QUERY_ID_LEN);
  return buildCmdMsg(pCxt, TDMT_MND_KILL_QUERY, (FSerializeFunc)tSerializeSKillQueryReq, &killReq);
}

static int32_t translateKillTransaction(STranslateContext* pCxt, SKillStmt* pStmt) {
  SKillTransReq killReq = {0};
  killReq.transId = pStmt->targetId;
  return buildCmdMsg(pCxt, TDMT_MND_KILL_TRANS, (FSerializeFunc)tSerializeSKillTransReq, &killReq);
}

static int32_t translateKillSsMigrate(STranslateContext* pCxt, SKillStmt* pStmt) {
  SKillSsMigrateReq killReq = {0};
  killReq.ssMigrateId = pStmt->targetId;
  return buildCmdMsg(pCxt, TDMT_MND_KILL_SSMIGRATE, (FSerializeFunc)tSerializeSKillSsMigrateReq, &killReq);
}

static bool isSlidingWindow(SNode* pWindow) {
  if (nodeType(pWindow) != QUERY_NODE_INTERVAL_WINDOW) {
    return false;
  }
  SIntervalWindowNode* pInterval = (SIntervalWindowNode*)pWindow;
  return pInterval->pInterval == NULL;
}

static int32_t checkCreateStream(STranslateContext* pCxt, SCreateStreamStmt* pStmt) {
  SStreamTriggerNode*    pTrigger = (SStreamTriggerNode*)pStmt->pTrigger;
  SStreamTriggerOptions* pTriggerOptions = (SStreamTriggerOptions*)pTrigger->pOptions;
  SStreamNotifyOptions*  pNotifyOptions = (SStreamNotifyOptions*)pTrigger->pNotify;
  int32_t                code = TSDB_CODE_SUCCESS;

  if (!pTrigger->pTriggerWindow) {
    PAR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_INVALID_PARA, "Trigger window can not be null"));
  }

  if (NULL != strchr(pStmt->streamName, '.')) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME,
                                         "The stream name cannot contain '.'"));
  }

  if (pStmt->pQuery && !isSelectStmt(pStmt->pQuery) && !isSetOperator(pStmt->pQuery)) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_QUERY,
                                         "Stream query must be a select or union statement"));
  }

  if (pTrigger->pTrigerTable == NULL && nodeType(pTrigger->pTriggerWindow) != QUERY_NODE_PERIOD_WINDOW) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_NO_TRIGGER_TABLE,
                                         "Trigger table must be specified when trigger window is not period window"));
  }

  if (pTrigger->pTrigerTable == NULL && pTriggerOptions&& pTriggerOptions->pPreFilter) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PRE_FILTER,
                                         "PRE_FILTER can only be specified when trigger table is specified"));
  }

  if (pTrigger->pTrigerTable == NULL && LIST_LENGTH(pTrigger->pPartitionList) != 0) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PARTITION,
                                         "PARTITION BY list can only be specified when trigger table is specified"));
  }

  if (pTrigger->pPartitionList == NULL && pStmt->pSubtable) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_SUBTABLE,
                                         "OUTPUT_SUBTABLE can only be specified when partition list is specified"));
  }

  if (pTrigger->pPartitionList == NULL && pStmt->pTags) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TAGS,
                                         "Tags can only be specified when partition list is specified"));
  }

  if (strlen(pStmt->targetDbName) == 0 && strlen(pStmt->targetTabName) == 0) {
    if ((pTrigger->pNotify && !pStmt->pQuery) || (pTriggerOptions && pTriggerOptions->calcNotifyOnly)) {
      // *only notify no query* or *query res only notify no save*
      // out table can be null. do nothing here.
    } else {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE, "Out table not specified"));
    }
  }

  if (pStmt->pQuery == NULL) {
    if (strlen(pStmt->targetDbName) == 0 && strlen(pStmt->targetTabName) == 0 && pStmt->pCols == NULL && pStmt->pTags == NULL) {
      // a stream with no query, no out table, no columns and no tags, which is valid
    } else {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE,
                                           "Can not specify out table when no query in stream"));
    }
  }

  if (pStmt->pQuery == NULL && pNotifyOptions && pNotifyOptions->pWhere) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_NOTIFY_COND,
                                         "Noify WHERE clause can only be specified when query is specified"));
  }

  if (pNotifyOptions && pNotifyOptions->eventType == EVENT_NONE) {
    if (nodeType(pTrigger->pTriggerWindow) != QUERY_NODE_PERIOD_WINDOW && nodeType(pTrigger->pTriggerWindow) != QUERY_NODE_INTERVAL_WINDOW) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_NOTIFY,
          "Notify type must be specified when trigger window is not period or sliding window without interval"));
    }
    if (nodeType(pTrigger->pTriggerWindow) == QUERY_NODE_INTERVAL_WINDOW) {
      SIntervalWindowNode* pIntervalWindow = (SIntervalWindowNode*)pTrigger->pTriggerWindow;
      if (pIntervalWindow->pInterval != NULL) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_NOTIFY,
            "Notify type must be specified when trigger window is not period or sliding window without interval"));
      }
    }
  }

  if (nodeType(pTrigger->pTriggerWindow) == QUERY_NODE_PERIOD_WINDOW) {
    if (pTriggerOptions && (pTriggerOptions->fillHistoryFirst || pTriggerOptions->fillHistory)) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER,
                                           "Fill history is not supported when trigger is period"));
    }
  }

  if (nodeType(pTrigger->pTriggerWindow) != QUERY_NODE_INTERVAL_WINDOW && nodeType(pTrigger->pTriggerWindow) != QUERY_NODE_PERIOD_WINDOW) {
    if (pTriggerOptions && pTriggerOptions->ignoreNoDataTrigger) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER,
          "ignore_nodata_trigger is not supported when trigger is not interval, sliding or period"));
    }
  }

  if (nodeType(pTrigger->pTriggerWindow) == QUERY_NODE_COUNT_WINDOW ||
      nodeType(pTrigger->pTriggerWindow) == QUERY_NODE_PERIOD_WINDOW ||
      isSlidingWindow(pTrigger->pTriggerWindow)) {
    if (pTriggerOptions && pTriggerOptions->deleteRecalc) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER,
                                           "delete recalc is not supported when trigger is count window, "
                                           "period window or sliding window"));
    }
  }

  SDbCfgInfo dbCfg = {0};
  PAR_ERR_JRET(getDBCfg(pCxt, pStmt->streamDbName, &dbCfg));
  if (strlen(pStmt->targetDbName) != 0) {
    PAR_ERR_JRET(getDBCfg(pCxt, pStmt->targetDbName, &dbCfg));
  }

  return code;

_return:
  parserError("checkCreateStream failed, db:%s, stream:%s, code:%d, errmsg:%s", pStmt->streamDbName,
              pStmt->streamName, code, pCxt->msgBuf.buf);
  return code;
}

enum {
  SLOT_KEY_TYPE_ALL = 1,
  SLOT_KEY_TYPE_COLNAME = 2,
};

static int32_t streamGetSlotKeyHelper(SNode* pNode, const char* pPreName, const char* name, char** ppKey, int32_t callocLen,
                                int32_t* pLen, uint16_t extraBufLen, int8_t slotKeyType) {
  int32_t code = 0;
  *ppKey = taosMemoryCalloc(1, callocLen);
  if (!*ppKey) {
    return terrno;
  }
  if (slotKeyType == SLOT_KEY_TYPE_ALL) {
    TAOS_STRNCAT(*ppKey, pPreName, TSDB_TABLE_NAME_LEN);
    TAOS_STRNCAT(*ppKey, ".", 2);
    TAOS_STRNCAT(*ppKey, name, TSDB_COL_NAME_LEN);
    *pLen = taosHashBinary(*ppKey, strlen(*ppKey));
  } else {
    TAOS_STRNCAT(*ppKey, name, TSDB_COL_NAME_LEN);
    *pLen = strlen(*ppKey);
  }

  return code;
}

static int32_t streamGetSlotKey(SNode* pNode, char** ppKey, int32_t* pLen, uint16_t extraBufLen) {
  int32_t code = 0;
  int32_t callocLen = 0;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if ('\0' == pCol->tableAlias[0]) {
      callocLen = TSDB_COL_NAME_LEN + 1 + extraBufLen;
      return streamGetSlotKeyHelper(pNode, NULL, pCol->colName, ppKey, callocLen, pLen, extraBufLen, SLOT_KEY_TYPE_COLNAME);
    }
    callocLen = TSDB_TABLE_NAME_LEN + 1 + TSDB_COL_NAME_LEN + 1 + extraBufLen;
    return streamGetSlotKeyHelper(pNode, pCol->tableAlias, pCol->colName, ppKey, callocLen, pLen, extraBufLen, SLOT_KEY_TYPE_ALL);
  } else if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (FUNCTION_TYPE_TBNAME == pFunc->funcType) {
      SValueNode* pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 0);
      if (pVal) {
        callocLen = strlen(pVal->literal) + 1 + TSDB_COL_NAME_LEN + 1 + extraBufLen;
        return streamGetSlotKeyHelper(pNode, pVal->literal, ((SExprNode*)pNode)->aliasName, ppKey, callocLen, pLen, extraBufLen, SLOT_KEY_TYPE_ALL);
      }
    }
  }

  callocLen = TSDB_COL_NAME_LEN + 1 + extraBufLen;
  return streamGetSlotKeyHelper(pNode, NULL, ((SExprNode*)pNode)->aliasName, ppKey, callocLen, pLen, extraBufLen,
                                SLOT_KEY_TYPE_COLNAME);
}

typedef struct SStreamSetSlotIdCxt {
  int32_t    errCode;
  SHashObj*  pHash;
  SNodeList* pCollect;
} SStreamSetSlotIdCxt;

static EDealRes doStreamSetSlotId(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode) && 0 != strcmp(((SColumnNode*)pNode)->colName, "*")) {
    SStreamSetSlotIdCxt* pCxt = (SStreamSetSlotIdCxt*)pContext;
    char*                name = NULL;
    int32_t              len = 0;
    pCxt->errCode = streamGetSlotKey(pNode, &name, &len, 64);
    if (TSDB_CODE_SUCCESS != pCxt->errCode) {
      return DEAL_RES_ERROR;
    }

    int16_t* slotId = taosHashGet(pCxt->pHash, name, len);
    // pIndex is definitely not NULL, otherwise it is a bug
    if (NULL == slotId) {
      parserError("doStreamSetSlotId failed, invalid slot name %s", name);
      pCxt->errCode = TSDB_CODE_PLAN_SLOT_NOT_FOUND;
      taosMemoryFree(name);
      return DEAL_RES_ERROR;
    }
    ((SColumnNode*)pNode)->slotId = *slotId;
    if (pCxt->pCollect) {
      SNode *tmpNode = NULL;
      pCxt->errCode = nodesCloneNode(pNode, &tmpNode);
      if (TSDB_CODE_SUCCESS != pCxt->errCode) {
        return DEAL_RES_ERROR;
      }
      pCxt->errCode = nodesListAppend(pCxt->pCollect, tmpNode);
      if (TSDB_CODE_SUCCESS != pCxt->errCode) {
        return DEAL_RES_ERROR;
      }
    }
    taosMemoryFree(name);
    return DEAL_RES_IGNORE_CHILD;
  } else if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode*       pFunc = (SFunctionNode*)pNode;
    SStreamSetSlotIdCxt* pCxt = (SStreamSetSlotIdCxt*)pContext;
    if (FUNCTION_TYPE_TBNAME == pFunc->funcType) {
      if (pCxt->pCollect) {
        SNode *tmpNode = NULL;
        pCxt->errCode = nodesCloneNode(pNode, &tmpNode);
        if (TSDB_CODE_SUCCESS != pCxt->errCode) {
          return DEAL_RES_ERROR;
        }
        pCxt->errCode = nodesListAppend(pCxt->pCollect, tmpNode);
        if (TSDB_CODE_SUCCESS != pCxt->errCode) {
          return DEAL_RES_ERROR;
        }
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t createStreamSetNodeSlotId(SNode* pNode, SHashObj* slotHash, SNodeList* pCollect) {
  SStreamSetSlotIdCxt slotCxt = {.errCode = TSDB_CODE_SUCCESS, .pHash = slotHash, .pCollect = pCollect};
  nodesWalkExpr(pNode, doStreamSetSlotId, &slotCxt);
  PAR_RET(slotCxt.errCode);
}

static int32_t createStreamSetListSlotId(SNodeList* pNodeList, SHashObj* slotHash, SNodeList* pCollect) {
  SStreamSetSlotIdCxt slotCxt = {.errCode = TSDB_CODE_SUCCESS, .pHash = slotHash, .pCollect = pCollect};
  nodesWalkExprs(pNodeList, doStreamSetSlotId, &slotCxt);
  PAR_RET(slotCxt.errCode);
}

typedef struct SRewriteTagSubtableExprCxt {
  int32_t            code;
  bool               found;
  bool               onlyValue;
  SNodeList*         pPartitionByList;
  STranslateContext* pCxt;
} SRewriteTagSubtableExprCxt;

static EDealRes rewriteTagSubtableExpr(SNode** pNode, void* pContext) {
  SRewriteTagSubtableExprCxt* pCxt = (SRewriteTagSubtableExprCxt*)pContext;
  SNode*                      pPar = NULL;
  int32_t                     index = 1;
  int32_t                     code = TSDB_CODE_SUCCESS;
  FOREACH(pPar, pCxt->pPartitionByList) {
    if (nodeType(*pNode) == QUERY_NODE_VALUE) {
      pCxt->onlyValue &= true;
      return DEAL_RES_CONTINUE;
    }
    if (nodeType(*pNode) == QUERY_NODE_FUNCTION) {
      SFunctionNode *pFunc = (SFunctionNode*)*pNode;
      if (pFunc->funcType == FUNCTION_TYPE_GROUP_ID) {
        pCxt->onlyValue &= true;
        return DEAL_RES_CONTINUE;
      }
      if (pFunc->funcType == FUNCTION_TYPE_TBNAME) {
        pCxt->onlyValue &= false;
      }
    }
    if (nodeType(*pNode) == QUERY_NODE_COLUMN) {
      pCxt->onlyValue &= false;
    }
    if (nodesEqualNode(*pNode, pPar)) {
      SNodeList*     pParamList = NULL;
      SFunctionNode* pFunc = NULL;
      SValueNode*    pVal = NULL;

      PAR_ERR_JRET(nodesMakeValueNodeFromInt32(index, (SNode**)&pVal));
      PAR_ERR_JRET(nodesListMakeStrictAppend(&pParamList, (SNode*)pVal));
      PAR_ERR_JRET(createFunction("_placeholder_column", pParamList, &pFunc));
      nodesDestroyNode((SNode*)*pNode);
      *pNode = (SNode*)pFunc;

      EDealRes res = translateFunction(pCxt->pCxt, (SFunctionNode**)pNode);
      if (res == DEAL_RES_ERROR) {
        PAR_ERR_JRET(pCxt->pCxt->errCode);
      }
      pCxt->found = true;
      return DEAL_RES_IGNORE_CHILD;
    }
    index++;
  }
  return DEAL_RES_CONTINUE;
_return:
  pCxt->code = code;
  return DEAL_RES_ERROR;
}

static int32_t translateCreateStreamTagSubtableExpr(STranslateContext* pCxt, SNodeList *pPartitionByList, SNode** pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  pCxt->streamInfo.outTableClause = true;
  pCxt->streamInfo.triggerPartitionList = pPartitionByList;
  pCxt->currClause = SQL_CLAUSE_SELECT;
  PAR_ERR_JRET(translateExpr(pCxt, pNode));

  SRewriteTagSubtableExprCxt cxt = {.code = TSDB_CODE_SUCCESS, .pCxt = pCxt, .pPartitionByList = pPartitionByList, .found = false, .onlyValue = true};
  nodesRewriteExpr(pNode, rewriteTagSubtableExpr, &cxt);
  pCxt->streamInfo.outTableClause = false;
  pCxt->streamInfo.triggerPartitionList = NULL;
  PAR_ERR_JRET(cxt.code);

  if (!cxt.found && !cxt.onlyValue) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_SUBTABLE, "output_subtable/tag_expr must use expr from partition list."));
  }
  return code;
_return:
  parserError("createStreamReqBuildOutSubtable failed, code:%d, errmsg:%s", code, pCxt->msgBuf.buf);
  return code;
}

static int32_t createStreamReqBuildOutSubtable(STranslateContext* pCxt, const char* streamDb, const char* streamName,
                                               const char* outDbName, const char* outTableName,
                                               SNode* pSubtable, SHashObj* pTriggerSlotHash,
                                               SNodeList* pPartitionByList, char** subTblNameExpr) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pSubtableExpr = NULL;
  if (NULL == pPartitionByList) {
    *subTblNameExpr = NULL;
    return code;  // no partition by list, no subtable name expression
  }

  if (pSubtable) {
    PAR_ERR_JRET(nodesCloneNode(pSubtable, &pSubtableExpr));
  } else {
    // default rule : '_t' + md5(stream_full_name + '.' + outtable_full_name) + '_' + cast (_grpid() as varchar(20))
    SFunctionNode* pConcatFunc = NULL;
    SFunctionNode* pGrpIdFunc = NULL;
    SFunctionNode* pCastFunc = NULL;
    SFunctionNode* pMd5Func = NULL;
    SValueNode*    pNameValue = NULL;
    SValueNode*    pPrefixValue = NULL;
    SValueNode*    pDashValue = NULL;
    SValueNode*    pGrpFuncVal = NULL;

    PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pConcatFunc));
    pConcatFunc->funcId = fmGetFuncId("concat");
    pConcatFunc->funcType = FUNCTION_TYPE_CONCAT;
    snprintf(pConcatFunc->functionName, TSDB_FUNC_NAME_LEN, "concat");

    PAR_ERR_JRET(nodesMakeValueNodeFromString("_t", &pPrefixValue));
    PAR_ERR_JRET(nodesListMakeStrictAppend(&pConcatFunc->pParameterList, (SNode*)pPrefixValue));

    PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pMd5Func));
    pMd5Func->funcId = fmGetFuncId("md5");
    pMd5Func->funcType = FUNCTION_TYPE_MD5;
    snprintf(pMd5Func->functionName, TSDB_FUNC_NAME_LEN, "md5");

    char* streamFName = taosMemoryCalloc(1, TSDB_TABLE_FNAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_TABLE_FNAME_LEN);
    if (NULL == streamFName) {
      PAR_ERR_JRET(terrno);
    }
    TAOS_STRNCAT(streamFName, streamDb, TSDB_DB_NAME_LEN);
    TAOS_STRNCAT(streamFName, ".", 2);
    TAOS_STRNCAT(streamFName, streamName, TSDB_TABLE_NAME_LEN);
    TAOS_STRNCAT(streamFName, ".", 2);
    TAOS_STRNCAT(streamFName, outDbName, TSDB_DB_NAME_LEN);
    TAOS_STRNCAT(streamFName, ".", 2);
    TAOS_STRNCAT(streamFName, outTableName, TSDB_TABLE_NAME_LEN);

    code = nodesMakeValueNodeFromString(streamFName, &pNameValue);
    taosMemoryFree(streamFName);
    PAR_ERR_JRET(code);
    PAR_ERR_JRET(nodesListMakeStrictAppend(&pMd5Func->pParameterList, (SNode*)pNameValue));
    PAR_ERR_JRET(nodesListMakeStrictAppend(&pConcatFunc->pParameterList, (SNode*)pMd5Func));

    PAR_ERR_JRET(nodesMakeValueNodeFromString("_", &pDashValue));
    PAR_ERR_JRET(nodesListMakeStrictAppend(&pConcatFunc->pParameterList, (SNode*)pDashValue));

    PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pGrpIdFunc));
    pGrpIdFunc->funcId = fmGetFuncId("_tgrpid");
    pGrpIdFunc->funcType = FUNCTION_TYPE_TGRPID;
    snprintf(pGrpIdFunc->functionName, TSDB_FUNC_NAME_LEN, "_tgrpid");

    PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pCastFunc));
    pCastFunc->funcId = fmGetFuncId("cast");
    pCastFunc->funcType = FUNCTION_TYPE_CAST;
    pCastFunc->node.resType.type = TSDB_DATA_TYPE_BINARY;
    pCastFunc->node.resType.bytes = 20 + VARSTR_HEADER_SIZE;
    snprintf(pCastFunc->functionName, TSDB_FUNC_NAME_LEN, "cast");
    PAR_ERR_JRET(nodesListMakeStrictAppend(&pCastFunc->pParameterList, (SNode*)pGrpIdFunc));

    PAR_ERR_JRET(nodesListMakeStrictAppend(&pConcatFunc->pParameterList, (SNode*)pCastFunc));

    pSubtableExpr = (SNode*)pConcatFunc;
  }

  PAR_ERR_JRET(translateCreateStreamTagSubtableExpr(pCxt, pPartitionByList, &pSubtableExpr));
  if (!nodesIsExprNode(pSubtableExpr) || ((SExprNode*)pSubtableExpr)->resType.type != TSDB_DATA_TYPE_BINARY) {
    nodesDestroyNode(pSubtableExpr);
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE,
                                   "Subtable name expression must be a binary type expression");
  }
  PAR_ERR_JRET(createStreamSetNodeSlotId(pSubtableExpr, pTriggerSlotHash, NULL));
  PAR_ERR_JRET(nodesNodeToString(pSubtableExpr, false, subTblNameExpr, NULL));
  nodesDestroyNode(pSubtableExpr);

  return code;

_return:
  nodesDestroyNode(pSubtableExpr);
  // TODO(smj) : free node
  parserError("%s failed, code:%d", __func__, code);
  return code;
}

static int32_t createStreamReqBuildStreamTagExprStr(STranslateContext* pCxt, SNodeList* pList,
                                                    SNodeList* pPartitionByList,
                                                    SHashObj* pTriggerSlotHash, char** tagValueExpr) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pExprList = NULL;
  int32_t    pExprListLen = 0;
  SNode*     pNode = NULL;
  SNode*     tmpExpr = NULL;

  if (LIST_LENGTH(pList) == 0) {
    return code;
  }

  PAR_ERR_JRET(nodesMakeList(&pExprList));

  FOREACH(pNode, pList) {
    SStreamTagDefNode* pTag = (SStreamTagDefNode*)pNode;
    if (pTag->pTagExpr) {
      PAR_ERR_JRET(translateCreateStreamTagSubtableExpr(pCxt, pPartitionByList, &pTag->pTagExpr));
      SExprNode* pTagExpr = (SExprNode*)pTag->pTagExpr;
      if (pTagExpr->resType.type != pTag->dataType.type ||
          pTagExpr->resType.precision != pTag->dataType.precision ||
          pTagExpr->resType.scale != pTag->dataType.scale) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE,
                    "Tag data type does not match tag expression data type: %s, %d", pTag->tagName,
                    pTagExpr->resType.type));
      }
      PAR_ERR_JRET(createStreamSetNodeSlotId(pTag->pTagExpr, pTriggerSlotHash, NULL));
      PAR_ERR_JRET(nodesCloneNode(pTag->pTagExpr, &tmpExpr));
      PAR_ERR_JRET(nodesListMakeAppend(&pExprList, tmpExpr));
      tmpExpr = NULL;  // reset tmpExpr to avoid double free
    } else {
      PAR_ERR_JRET(TSDB_CODE_STREAM_INVALID_OUT_TABLE);
    }
  }

  PAR_ERR_JRET(nodesListToString(pExprList, false, tagValueExpr, &pExprListLen));

_return:
  if (code) {
    parserError("%s failed, code:%d", __func__, code);
  }
  nodesDestroyNode(tmpExpr);
  nodesDestroyList(pExprList);
  return code;
}

static int32_t checkExpiredTime(STranslateContext* pCxt, SValueNode* pTime) {
  int32_t len = (int32_t)strlen(pTime->literal);
  char*   unit = &pTime->literal[len - 1];
  if (*unit != 'a' && *unit != 's' && *unit != 'm' && *unit != 'h' && *unit != 'd') {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TIME_UNIT,
                                   "Unsupported time unit in EXPIRED_TIME: %s", pTime->literal);
  }

  return TSDB_CODE_SUCCESS;
}

static const int64_t maxDelayLowerBound = (int64_t)  3 * 1000; // 3s in milliseconds

static int32_t checkDelayTime(STranslateContext* pCxt, SValueNode* pTime) {
  int32_t len = (int32_t)strlen(pTime->literal);
  char*   unit = &pTime->literal[len - 1];
  if (*unit != 's' && *unit != 'm' && *unit != 'h' && *unit != 'd') {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TIME_UNIT,
                                   "Unsupported time unit in MAX_DELAY_TIME: %s", pTime->literal);
  }
  if (pTime->datum.i < maxDelayLowerBound) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TIME_UNIT,
                                   "MAX_DELAY must be greater than or equal to 3 seconds");
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t createStreamReqBuildTriggerOptions(STranslateContext* pCxt, SCreateStreamStmt* pStmt,
                                                  SStreamTriggerOptions* pOptions, SCMCreateStreamReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  parserDebug("translate create stream req start build trigger options, streamId:%"PRId64, pReq->streamId);

  if (!pOptions) {
    parserDebug("no trigger options in create stream req");
    return code;
  }

  pCxt->currClause = SQL_CLAUSE_SELECT;
  pReq->igExists = (int8_t)pStmt->ignoreExists;

  if (pOptions->pExpiredTime) {
    PAR_ERR_JRET(checkExpiredTime(pCxt, (SValueNode*)pOptions->pExpiredTime));
    PAR_ERR_JRET(translateExpr(pCxt, &pOptions->pExpiredTime));
    pReq->expiredTime = ((SValueNode*)pOptions->pExpiredTime)->datum.i;
  }

  if (pOptions->pMaxDelay) {
    SNode* tmpStmt = pCxt->pCurrStmt;
    pCxt->pCurrStmt = NULL;
    // since max delay do not need to translate with trigger table's precision, set currStmt to NULL, so we can use
    // default precision 'ms'
    PAR_ERR_JRET(translateExpr(pCxt, &pOptions->pMaxDelay));
    pCxt->pCurrStmt = tmpStmt;
    PAR_ERR_JRET(checkDelayTime(pCxt, (SValueNode*)pOptions->pMaxDelay));
    pReq->maxDelay = ((SValueNode*)pOptions->pMaxDelay)->datum.i;
  }

  if (pOptions->pWaterMark) {
    PAR_ERR_JRET(translateExpr(pCxt, &pOptions->pWaterMark));
    pReq->watermark = ((SValueNode*)pOptions->pWaterMark)->datum.i;
  }

  if (pOptions->pExpiredTime && pOptions->pWaterMark) {
    if (pReq->expiredTime <= pReq->watermark) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER,
                                           "EXPIRED_TIME must be greater than WATERMARK"));
    }
  }

  if (pOptions->pFillHisStartTime) {
    STimeWindow range = {.skey = 0, .ekey = 0};
    PAR_ERR_JRET(translateTimeRange(pCxt, pStmt->streamDbName, pOptions->pFillHisStartTime, NULL, &range));
    pReq->fillHistoryStartTime = range.skey;
  }

  if (pOptions->pEventType) {
    pReq->eventTypes = (pOptions->pEventType == EVENT_NONE ? EVENT_WINDOW_CLOSE : pOptions->pEventType);
  }

  pReq->igDisorder = (int8_t)pOptions->ignoreDisorder;
  pReq->deleteReCalc = (int8_t)pOptions->deleteRecalc;
  pReq->deleteOutTbl = (int8_t)pOptions->deleteOutputTable;
  pReq->fillHistory = (int8_t)pOptions->fillHistory;
  pReq->fillHistoryFirst = (int8_t)pOptions->fillHistoryFirst;
  pReq->calcNotifyOnly = (int8_t)pOptions->calcNotifyOnly;
  pReq->lowLatencyCalc = (int8_t)pOptions->lowLatencyCalc;
  pReq->igNoDataTrigger = (int8_t)pOptions->ignoreNoDataTrigger;

  return code;

_return:
  parserError("%s failed, code:%d", __func__, code);
  return code;
}

static int32_t createStreamReqBuildNotifyOptions(STranslateContext* pCxt, SStreamNotifyOptions* pNotifyOptions,
                                                 SNode** pNotifyCond, SCMCreateStreamReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;

  parserDebug("translate create stream req start build notify options, streamId:%"PRId64, pReq->streamId);

  if (!pNotifyOptions) {
    parserDebug("no notify options in create stream req");
    return code;
  }

  if (LIST_LENGTH(pNotifyOptions->pAddrUrls) < 1) {
    PAR_ERR_JRET(
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_NOTIFY, "NOTIFY URL must be specified"));
  }

  pReq->pNotifyAddrUrls = taosArrayInit(pNotifyOptions->pAddrUrls->length, POINTER_BYTES);
  if (pReq->pNotifyAddrUrls == NULL) {
    PAR_ERR_JRET(terrno);
  }

  FOREACH(pNode, pNotifyOptions->pAddrUrls) {
    char *url = taosStrndup(((SValueNode*)pNode)->literal, TSDB_STREAM_NOTIFY_URL_LEN);
    if (taosArrayPush(pReq->pNotifyAddrUrls, &url) == NULL) {
      taosMemoryFreeClear(url);
      PAR_ERR_JRET(terrno);
    }
  }

  pReq->notifyEventTypes = (int32_t)pNotifyOptions->eventType;
  // Due to historical reasons, bit positions were not initially used in notifyHistory, and for compatibility, they continue to be retained. 
  pReq->notifyHistory = BIT_FLAG_TEST_MASK(pNotifyOptions->notifyType, NOTIFY_HISTORY); 
  pReq->addOptions = BIT_FLAG_TEST_MASK(pNotifyOptions->notifyType, NOTIFY_ON_FAILURE_PAUSE);
  pReq->addOptions |= (pNotifyOptions->pWhere != NULL) ? NOTIFY_HAS_FILTER : 0;
  PAR_ERR_JRET(nodesCloneNode(pNotifyOptions->pWhere, pNotifyCond));

  return code;

_return:
  // pAddrUrls will be free when pReq is destroyed
  parserError("createStreamReqBuildNotifyOptions failed, code:%d, streamId:%"PRId64, code, pReq->streamId);
  return code;
}

static int32_t createStreamCheckOutTags(STranslateContext* pCxt, SNodeList* pTags, STableMeta* pMeta) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t tagIndex = pMeta->tableInfo.numOfColumns;
  SNode*  pNode = NULL;

  if (!pTags) {
    return code;
  }

  if (LIST_LENGTH(pTags) != pMeta->tableInfo.numOfTags) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE, "Out table tag count mismatch");
  }

  FOREACH(pNode, pTags) {
    SStreamTagDefNode* pTagDef = (SStreamTagDefNode*)pNode;
    int8_t  scale = 0;
    int8_t  precision = 0;
    int32_t bytes = 0;
    if (tagIndex >= pMeta->tableInfo.numOfColumns + pMeta->tableInfo.numOfTags) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE, "Out table tag count mismatch");
    }
    // tags not support decimal, so scale and precision are always 0
    if (pTagDef->dataType.type != pMeta->schema[tagIndex].type ||
        strcmp(pTagDef->tagName, pMeta->schema[tagIndex].name) != 0) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE, "Out table tag type mismatch");
    }
    tagIndex++;
  }

  return code;
}

static int32_t createStreamCheckOutCols(STranslateContext* pCxt, SNodeList* pCols, STableMeta* pMeta) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t colIndex = 0;
  SNode*  pNode = NULL;

  if (!pCols) {
    return code;
  }

  if (LIST_LENGTH(pCols) != pMeta->tableInfo.numOfColumns) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE, "Out table cols count mismatch");
  }

  FOREACH(pNode, pCols) {
    SColumnDefNode* pColDef = (SColumnDefNode*)pNode;
    SColumnOptions* pColOptions = (SColumnOptions*)pColDef->pOptions;
    int8_t          scale = 0;
    int8_t          precision = 0;
    int32_t         bytes = 0;
    if (pMeta->schemaExt) {
      extractTypeFromTypeMod(pMeta->schema[colIndex].type, pMeta->schemaExt[colIndex].typeMod, &precision, &scale, &bytes);
    }

    if (pColDef->dataType.type != pMeta->schema[colIndex].type ||
        pColDef->dataType.bytes != pMeta->schema[colIndex].bytes || pColDef->dataType.scale != scale ||
        pColDef->dataType.precision != precision) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE, "Out table cols type mismatch");
    }

    if (strncmp(pColDef->colName, pMeta->schema[colIndex].name, strlen(pColDef->colName)) != 0) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE, "Out table cols name mismatch");
    }
    colIndex++;
  }

  return code;
}

static int32_t createStreamReqBuildOutTable(STranslateContext* pCxt, SCreateStreamStmt* pStmt,
                                            SHashObj* pTriggerSlotHash, SCMCreateStreamReq* pReq) {
  int32_t         code = TSDB_CODE_SUCCESS;
  STableMeta*     pMeta = NULL;

  parserDebug("translate create stream req start build out table info, streamId:%"PRId64, pReq->streamId);

  if (strlen(pStmt->targetDbName) == 0 && strlen(pStmt->targetTabName) == 0) {
    parserDebug("no out table in create stream req");
    return code;
  }

  pReq->outDB = taosMemoryMalloc(TSDB_DB_FNAME_LEN);
  pReq->outTblName = taosStrdup(pStmt->targetTabName);
  if (NULL == pReq->outDB || NULL == pReq->outTblName) {
    PAR_ERR_RET(terrno);
  }
  (void)snprintf(pReq->outDB, TSDB_DB_FNAME_LEN, "%d.%s", pCxt->pParseCxt->acctId, pStmt->targetDbName);

  code = getTableMeta(pCxt, pStmt->targetDbName, pStmt->targetTabName, &pMeta);
  if (TSDB_CODE_PAR_TABLE_NOT_EXIST == code || TSDB_CODE_PAR_INTERNAL_ERROR == code) {
    if (((SStreamTriggerNode*)pStmt->pTrigger)->pPartitionList) {
      // create stb
      pReq->outStbExists = false;
      pReq->outTblType = TSDB_SUPER_TABLE;
      pReq->outStbUid = 0;
      pReq->outStbSversion = 1;
      if (!pStmt->pTags || !pStmt->pCols) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE,
                                             "Missing out tags or cols when out table is super table"));
      }
    } else {
      // create normal table
      pReq->outStbExists = false;
      pReq->outTblType = TSDB_NORMAL_TABLE;
      pReq->outStbUid = 0;
      pReq->outStbSversion = 1;
      if (pStmt->pTags) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE,
                                             "Can not specify out tags when out table is normal table"));
      }
      if (!pStmt->pCols) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE,
                                             "Missing out cols when out table is normal table"));
      }
    }
    code = TSDB_CODE_SUCCESS;
  } else if (TSDB_CODE_SUCCESS == code) {
    if (((SStreamTriggerNode*)pStmt->pTrigger)->pPartitionList) {
      // create stb
      if (pMeta->tableType != TSDB_SUPER_TABLE) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE,
                                             "Out super table exists and is not super table"));
      }
      pReq->outStbExists = true;
      pReq->outTblType = TSDB_SUPER_TABLE;
      pReq->outStbUid = pMeta->suid;
      pReq->outStbSversion = pMeta->sversion;
      PAR_ERR_JRET(createStreamCheckOutTags(pCxt, pStmt->pTags, pMeta));
      PAR_ERR_JRET(createStreamCheckOutCols(pCxt, pStmt->pCols, pMeta));
    } else {
      if (pMeta->tableType != TSDB_NORMAL_TABLE) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE,
                                             "Out table exists and is not normal table"));
      }
      // create normal table
      pReq->outStbExists = false;
      pReq->outTblType = TSDB_NORMAL_TABLE;
      pReq->outStbUid = 0;
      pReq->outStbSversion = 1;
      PAR_ERR_JRET(createStreamCheckOutCols(pCxt, pStmt->pCols, pMeta));
    }
  } else {
    PAR_ERR_JRET(code);
  }

  PAR_ERR_JRET(checkTableSchemaImpl(pCxt, pStmt->pTags, pStmt->pCols, NULL, false));
  if (pReq->outTblType == TSDB_SUPER_TABLE) {
    PAR_ERR_JRET(streamTagDefNodeToField(pStmt->pTags, &pReq->outTags, false)); // tag bytes has been calculated in createStreamReqSetDefaultTag
    PAR_ERR_JRET(createStreamReqBuildStreamTagExprStr(pCxt, pStmt->pTags, ((SStreamTriggerNode*)pStmt->pTrigger)->pPartitionList, pTriggerSlotHash, (char**)&pReq->tagValueExpr));
  } else {
    PAR_ERR_JRET(getTableVgId(pCxt, pStmt->targetDbName, pStmt->targetTabName, &pReq->outTblVgId));
  }

  PAR_ERR_JRET(columnDefNodeToField(pStmt->pCols, &pReq->outCols, false, false));
  PAR_ERR_JRET(createStreamReqBuildOutSubtable(pCxt, pStmt->streamDbName, pStmt->streamName, pStmt->targetDbName, pStmt->targetTabName, pStmt->pSubtable, pTriggerSlotHash, ((SStreamTriggerNode*)pStmt->pTrigger)->pPartitionList, (char**)&pReq->subTblNameExpr));

_return:

  if (code) {
    parserError("%s failed, code:%d", __func__, code);
  }
  taosMemoryFreeClear(pMeta);
  return code;
}

static int32_t createStreamReqBuildTriggerTableInfo(STranslateContext* pCxt, SRealTableNode* pTriggerTable, STableMeta* pMeta, SCMCreateStreamReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  switch (pMeta->tableType) {
    case TSDB_SUPER_TABLE:
      if (isVirtualSTable(pMeta)) {
        BIT_FLAG_SET_MASK(pReq->flags, CREATE_STREAM_FLAG_TRIGGER_VIRTUAL_STB);
      }
      break;
    case TSDB_CHILD_TABLE:
    case TSDB_NORMAL_TABLE:
    case TSDB_VIRTUAL_CHILD_TABLE:
    case TSDB_VIRTUAL_NORMAL_TABLE:
      break;
    default:
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER,
                                           "Invalid trigger table type %d", pMeta->tableType));
  }

  PAR_ERR_JRET(getTableVgId(pCxt, pTriggerTable->table.dbName, pTriggerTable->table.tableName, &pReq->triggerTblVgId));

  pReq->triggerDB = taosMemoryMalloc(TSDB_DB_FNAME_LEN);
  pReq->triggerTblName = taosStrdup(pTriggerTable->table.tableName);
  if (NULL == pReq->triggerDB || NULL == pReq->triggerTblName) {
    PAR_ERR_RET(terrno);
  }

  (void)snprintf(pReq->triggerDB, TSDB_DB_FNAME_LEN, "%d.%s", pCxt->pParseCxt->acctId, pTriggerTable->table.dbName);

  pReq->triggerTblType = pMeta->tableType;
  pReq->triggerTblUid = pMeta->uid;
  pReq->triggerTblSuid = pMeta->suid;
  pReq->triggerPrec = pMeta->tableInfo.precision;

  return code;

_return:
  parserError("%s failed, code:%d", __func__, code);
  return code;
}

static int64_t createStreamReqWindowGetBigInt(SNode* pVal) {
  return pVal ? ((SValueNode*)pVal)->datum.i : 0;
}

static int8_t createStreamReqWindowGetUnit(SNode* pVal) {
  return pVal ? ((SValueNode*)pVal)->unit : 0;
}

static int32_t createStreamReqBuildTriggerSessionWindow(STranslateContext* pCxt, SSessionWindowNode* pTriggerWindow, SCMCreateStreamReq* pReq) {
  pReq->triggerType = WINDOW_TYPE_SESSION;
  PAR_ERR_RET(checkSessionWindow(pCxt, pTriggerWindow));
  pReq->trigger.session.slotId = pTriggerWindow->pCol->slotId;
  pReq->trigger.session.sessionVal = createStreamReqWindowGetBigInt((SNode*)pTriggerWindow->pGap);
  return TSDB_CODE_SUCCESS;
}

static int32_t createStreamReqBuildTriggerIntervalWindow(STranslateContext* pCxt, SIntervalWindowNode* pTriggerWindow, SCMCreateStreamReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  pReq->triggerType = WINDOW_TYPE_INTERVAL;
  PAR_ERR_RET(checkStreamIntervalWindow(pCxt, pTriggerWindow));
  if (pTriggerWindow->pInterval && pTriggerWindow->pSliding) {
    bool greater = false;
    code = checkTimeGreater((SValueNode*)pTriggerWindow->pInterval, (SValueNode*)pTriggerWindow->pSliding,
                       ((SColumnNode*)pTriggerWindow->pCol)->node.resType.precision, false, &greater);
    PAR_ERR_RET(code);
    pReq->trigger.sliding.overlap = greater;
  } else {
    pReq->trigger.sliding.overlap = false;
  }
  pReq->trigger.sliding.precision = ((SColumnNode*)pTriggerWindow->pCol)->node.resType.precision;
  pReq->trigger.sliding.intervalUnit = createStreamReqWindowGetUnit(pTriggerWindow->pInterval);
  pReq->trigger.sliding.offsetUnit = createStreamReqWindowGetUnit(pTriggerWindow->pOffset);
  pReq->trigger.sliding.slidingUnit = createStreamReqWindowGetUnit(pTriggerWindow->pSliding);
  pReq->trigger.sliding.soffsetUnit = createStreamReqWindowGetUnit(pTriggerWindow->pSOffset);
  pReq->trigger.sliding.interval = createStreamReqWindowGetBigInt(pTriggerWindow->pInterval);
  pReq->trigger.sliding.offset = createStreamReqWindowGetBigInt(pTriggerWindow->pOffset);
  pReq->trigger.sliding.sliding = createStreamReqWindowGetBigInt(pTriggerWindow->pSliding);
  pReq->trigger.sliding.soffset = createStreamReqWindowGetBigInt(pTriggerWindow->pSOffset);
  return TSDB_CODE_SUCCESS;
}

static int32_t createStreamReqBuildTriggerEventWindow(STranslateContext* pCxt, SEventWindowNode* pTriggerWindow, SCMCreateStreamReq* pReq) {
  pReq->triggerType = WINDOW_TYPE_EVENT;
  PAR_ERR_RET(checkEventWindow(pCxt, pTriggerWindow));
  PAR_ERR_RET(nodesNodeToString(pTriggerWindow->pStartCond, false, (char**)&pReq->trigger.event.startCond, NULL));
  PAR_ERR_RET(nodesNodeToString(pTriggerWindow->pEndCond, false, (char**)&pReq->trigger.event.endCond, NULL));
  pReq->trigger.event.trueForDuration = createStreamReqWindowGetBigInt(pTriggerWindow->pTrueForLimit);
  return TSDB_CODE_SUCCESS;
}

static int32_t createStreamReqBuildTriggerPeriodWindow(STranslateContext* pCxt, SPeriodWindowNode* pTriggerWindow, SCMCreateStreamReq* pReq) {
  pReq->triggerType = WINDOW_TYPE_PERIOD;
  PAR_ERR_RET(checkPeriodWindow(pCxt, pTriggerWindow));
  pReq->trigger.period.period = createStreamReqWindowGetBigInt(pTriggerWindow->pPeroid);
  pReq->trigger.period.offset = createStreamReqWindowGetBigInt(pTriggerWindow->pOffset);
  pReq->trigger.period.periodUnit = createStreamReqWindowGetUnit(pTriggerWindow->pPeroid);
  pReq->trigger.period.offsetUnit = createStreamReqWindowGetUnit(pTriggerWindow->pOffset);
  pReq->trigger.period.precision = TSDB_TIME_PRECISION_MILLI;
  return TSDB_CODE_SUCCESS;
}

static int32_t createStreamReqBuildTriggerCountWindow(STranslateContext* pCxt, SCountWindowNode* pTriggerWindow, SCMCreateStreamReq* pReq) {
  pReq->triggerType = WINDOW_TYPE_COUNT;
  PAR_ERR_RET(checkCountWindow(pCxt, pTriggerWindow, true));
  pReq->trigger.count.sliding = pTriggerWindow->windowSliding;
  pReq->trigger.count.countVal = pTriggerWindow->windowCount;
  pReq->trigger.count.condCols = NULL;
  return TSDB_CODE_SUCCESS;
}

static int32_t createStreamReqBuildTriggerStateWindow(STranslateContext* pCxt, SStateWindowNode* pTriggerWindow, SCMCreateStreamReq* pReq) {
  pReq->triggerType = WINDOW_TYPE_STATE;
  PAR_ERR_RET(checkStateWindow(pCxt, pTriggerWindow));
  pReq->trigger.stateWin.slotId = ((SColumnNode*)pTriggerWindow->pExpr)->slotId;
  pReq->trigger.stateWin.extend = createStreamReqWindowGetBigInt(pTriggerWindow->pExtend);
  pReq->trigger.stateWin.trueForDuration = createStreamReqWindowGetBigInt(pTriggerWindow->pTrueForLimit);
  if (NULL != pTriggerWindow->pZeroth) {
    PAR_ERR_RET(nodesNodeToString(pTriggerWindow->pZeroth, false, (char**)&pReq->trigger.stateWin.zeroth, NULL));
  }
  PAR_ERR_RET(nodesNodeToString(pTriggerWindow->pExpr, false, (char**)&pReq->trigger.stateWin.expr, NULL));
  return TSDB_CODE_SUCCESS;
}

static int32_t createStreamReqBuildTriggerBuildWindowInfo(STranslateContext* pCxt, SNode* pTriggerWindow, SCMCreateStreamReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch(nodeType(pTriggerWindow)) {
    case QUERY_NODE_SESSION_WINDOW:
      PAR_ERR_JRET(createStreamReqBuildTriggerSessionWindow(pCxt, (SSessionWindowNode*)pTriggerWindow, pReq));
      break;
    case QUERY_NODE_INTERVAL_WINDOW:
      PAR_ERR_JRET(createStreamReqBuildTriggerIntervalWindow(pCxt, (SIntervalWindowNode*)pTriggerWindow, pReq));
      break;
    case QUERY_NODE_EVENT_WINDOW:
      PAR_ERR_JRET(createStreamReqBuildTriggerEventWindow(pCxt, (SEventWindowNode*)pTriggerWindow, pReq));
      break;
    case QUERY_NODE_PERIOD_WINDOW:
      PAR_ERR_JRET(createStreamReqBuildTriggerPeriodWindow(pCxt, (SPeriodWindowNode*)pTriggerWindow, pReq));
      break;
    case QUERY_NODE_COUNT_WINDOW:
      PAR_ERR_JRET(createStreamReqBuildTriggerCountWindow(pCxt, (SCountWindowNode*)pTriggerWindow, pReq));
      break;
    case QUERY_NODE_STATE_WINDOW:
      PAR_ERR_JRET(createStreamReqBuildTriggerStateWindow(pCxt, (SStateWindowNode*)pTriggerWindow, pReq));
      break;
    default:
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER, "Unsupported trigger window type: %d", nodeType(pTriggerWindow)));
  }
  return code;
_return:
  parserError("%s failed, code:%d", __func__, code);
  return code;
}

static void findTsSlotId(SScanPhysiNode* pScanNode, int16_t* pTsSlotId) {
  SNode* pNode = NULL;
  FOREACH(pNode, pScanNode->pScanCols) {
    STargetNode *pTarget = (STargetNode*)pNode;
    if (nodeType(pTarget->pExpr) == QUERY_NODE_COLUMN) {
      if (((SColumnNode*)pTarget->pExpr)->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        *pTsSlotId = pTarget->slotId;
        break;
      }
    }
  }
}

static int32_t createStreamReqBuildTriggerBuildPlan(STranslateContext* pCxt, SSelectStmt* pTriggerSelect,
                                               SCMCreateStreamReq* pReq, SHashObj **pTriggerSlotHash,
                                               SNode* pTriggerWindow, SNodeList* pTriggerPartition) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SQueryPlan*         pTriggerPlan = NULL;
  SScanPhysiNode*     pScanNode = NULL;
  SNode*              pNode = NULL;
  SDataBlockDescNode* pScanTuple = NULL;
  SPlanContext        cxt = {.pAstRoot = (SNode*)pTriggerSelect, .streamTriggerQuery = true};
  SNode*              pTriggerFilter = NULL;

  SNodeList*          pTriggerCols = NULL;
  SNodeList*          pPartitionCols = NULL;
  SNodeList*          pFilterCols = NULL;
  int32_t             pTriggerColsLen = 0;
  int32_t             pPartitionColsLen = 0;
  int32_t             pPartitionListLen = 0;
  int32_t             pFilterColsLen = 0;

  PAR_ERR_JRET(qCreateQueryPlan(&cxt, &pTriggerPlan, NULL));
  if (!cxt.streamTriggerScanSubplan) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER, "Trigger query scan subplan is NULL"));
  }
  pScanNode = (SScanPhysiNode*)((SSubplan*)cxt.streamTriggerScanSubplan)->pNode;
  pScanTuple = (pScanNode->node.pOutputDataBlockDesc);

  PAR_ERR_JRET(nodesCloneNode(pScanNode->node.pConditions, &pTriggerFilter));
  if ((pReq->triggerTblType == TSDB_VIRTUAL_NORMAL_TABLE ||
       pReq->triggerTblType == TSDB_VIRTUAL_CHILD_TABLE ||
       BIT_FLAG_TEST_MASK(pReq->flags, CREATE_STREAM_FLAG_TRIGGER_VIRTUAL_STB)) &&
      pTriggerFilter) {
    // need extract conditions.
    nodesDestroyNode(pScanNode->node.pConditions);
    pScanNode->node.pConditions = NULL;
  }

  *pTriggerSlotHash = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (*pTriggerSlotHash == NULL) {
    PAR_ERR_JRET(terrno);
  }

  findTsSlotId(pScanNode, &pReq->triTsSlotId);

  if (pReq->triTsSlotId == -1) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_QUERY, "Can not find timestamp primary key in trigger query scan"));
  }

  FOREACH(pNode, pScanTuple->pSlots) {
    SSlotDescNode *pSlot = (SSlotDescNode*)pNode;
    PAR_ERR_JRET(taosHashPut(*pTriggerSlotHash, pSlot->name, strlen(pSlot->name), &pSlot->slotId, sizeof(int16_t)));
  }

  PAR_ERR_JRET(nodesMakeList(&pTriggerCols));
  PAR_ERR_JRET(nodesMakeList(&pPartitionCols));
  PAR_ERR_JRET(nodesMakeList(&pFilterCols));

  // collect trigger cols and set slot id
  PAR_ERR_JRET(createStreamSetNodeSlotId(pTriggerWindow, *pTriggerSlotHash, pTriggerCols));

  // collect partition cols and set slot id
  PAR_ERR_JRET(createStreamSetListSlotId(pTriggerPartition, *pTriggerSlotHash, pPartitionCols));

  // collect filter cols and set slot id
  PAR_ERR_JRET(createStreamSetNodeSlotId(pTriggerFilter, *pTriggerSlotHash, pFilterCols));

  PAR_ERR_JRET(nodesNodeToString(cxt.streamTriggerScanSubplan, false, (char**)&pReq->triggerScanPlan, NULL));
  PAR_ERR_JRET(nodesListToString(pTriggerCols, false, (char**)&pReq->triggerCols, &pTriggerColsLen));
  PAR_ERR_JRET(nodesListToString(pFilterCols, false, (char**)&pReq->triggerFilterCols, &pFilterColsLen));
  if (pTriggerPartition) {
    PAR_ERR_JRET(nodesListToString(pTriggerPartition, false, (char**)&pReq->partitionCols, &pPartitionListLen));
  }

  if ((pReq->triggerTblType == TSDB_VIRTUAL_NORMAL_TABLE ||
       pReq->triggerTblType == TSDB_VIRTUAL_CHILD_TABLE ||
       BIT_FLAG_TEST_MASK(pReq->flags, CREATE_STREAM_FLAG_TRIGGER_VIRTUAL_STB)) &&
      pTriggerFilter) {
    // need extract conditions.
    PAR_ERR_JRET(nodesNodeToString(pTriggerFilter, false, (char**)&pReq->triggerPrevFilter, NULL));
  }

_return:
  nodesDestroyList(pTriggerCols);
  nodesDestroyList(pPartitionCols);
  nodesDestroyList(pFilterCols);
  nodesDestroyNode(pTriggerFilter);
  nodesDestroyNode((SNode*)pTriggerPlan);
  if (code) {
    parserError("%s failed, code:%d", __func__, code);
  }
  return code;
}

static int32_t createStreamReqSetDefaultTag(STranslateContext* pCxt, SCreateStreamStmt* pStmt,
                                            SNodeList* pTriggerPartition, SCMCreateStreamReq* pReq) {
  int32_t            code = TSDB_CODE_SUCCESS;
  SNode*             pNode = NULL;
  SStreamTagDefNode* pTagDef = NULL;

  if (!pTriggerPartition) {
    return code;
  }

  if (pStmt->pTags) {
    FOREACH(pNode, pStmt->pTags) {
      SStreamTagDefNode *pDef = (SStreamTagDefNode*)pNode;
      pDef->dataType.bytes = calcTypeBytes(pDef->dataType);
    }
    return code;
  }

  FOREACH(pNode, pTriggerPartition) {
    PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_STREAM_TAG_DEF, (SNode**)&pTagDef));
    switch (nodeType(pNode)) {
      case QUERY_NODE_FUNCTION: {
        SFunctionNode *pFunc = (SFunctionNode*)pNode;
        if (pFunc->funcType == FUNCTION_TYPE_TBNAME) {
          tstrncpy(pTagDef->tagName, "tag_tbname", TSDB_COL_NAME_LEN);
          pTagDef->dataType = pFunc->node.resType;
          break;
        } else {
          tstrncpy(pTagDef->tagName, pFunc->node.userAlias, TSDB_COL_NAME_LEN);
          pTagDef->dataType = pFunc->node.resType;
          break;
        }
      }
      default: {
        SExprNode* pExpr = (SExprNode*)pNode;
        tstrncpy(pTagDef->tagName, pExpr->userAlias, TSDB_COL_NAME_LEN);
        pTagDef->dataType = pExpr->resType;
        break;
      }
    }

    PAR_ERR_JRET(nodesCloneNode((SNode*)pNode, &pTagDef->pTagExpr));
    PAR_ERR_JRET(nodesListMakeAppend(&pStmt->pTags, (SNode*)pTagDef));
  }
  return code;
_return:
  parserError("%s failed, code:%d", __func__, code);
  nodesDestroyNode((SNode*)pTagDef);
  return code;
}

static int32_t createStreamReqSetDefaultOutCols(STranslateContext* pCxt, SCreateStreamStmt* pStmt,
                                                SNodeList* pCalcProjection, SCMCreateStreamReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  int32_t index = 0;
  bool    pColExists = false;
  int32_t bound = LIST_LENGTH(pCalcProjection);

  parserDebug("translate create stream req start set default output table's cols");
  if (pReq->addOptions & NOTIFY_HAS_FILTER) {
    bound--;  // ignore notify where condition in calculation projection
  }

  if (pStmt->pCols) {
    pColExists = true;
    if (LIST_LENGTH(pStmt->pCols) < TSDB_MIN_COLUMNS) {
      PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE, "The number of columns in stream output must be greater than 1"));
    }
    if (pStmt->pQuery) {
      if (LIST_LENGTH(pStmt->pCols) != bound) {
        PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE, "The number of columns in stream output must match the number of query"));
      }
    }
  }

  FOREACH(pNode, pCalcProjection) {
    SExprNode*      pExpr = (SExprNode*)pNode;
    SColumnDefNode* pColDef = NULL;
    // TODO(smj) : maybe remove this, since this will be checked later
    if (index == 0) {
      if (pExpr->resType.type != TSDB_DATA_TYPE_TIMESTAMP) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE, "The first column of stream output must be timestamp"));
      }
    }

    if (index >= bound) {
      break;
    }

    if (pColExists) {
      pColDef = (SColumnDefNode*)nodesListGetNode(pStmt->pCols, index);
    } else {
      PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_COLUMN_DEF, (SNode**)&pColDef));
      if (nodeType(pExpr) == QUERY_NODE_FUNCTION) {
        SFunctionNode *pFunc = (SFunctionNode*)pExpr;
        if (pFunc->funcType == FUNCTION_TYPE_PLACEHOLDER_COLUMN && strcmp(pExpr->userAlias, "_placeholder_column") == 0) {
          // %%n
          TAOS_STRCAT(pColDef->colName, "%%");
          TAOS_STRCAT(pColDef->colName, ((SValueNode*)(pFunc->pParameterList->pTail->pNode))->literal);
        } else if (pFunc->funcType == FUNCTION_TYPE_PLACEHOLDER_TBNAME && strcmp(pExpr->userAlias, "_placeholder_tbname") == 0) {
          // %%tbname
          TAOS_STRCAT(pColDef->colName, "%%tbname");
        } else {
          tstrncpy(pColDef->colName, pExpr->userAlias, TSDB_COL_NAME_LEN);
        }
      } else {
        tstrncpy(pColDef->colName, pExpr->userAlias, TSDB_COL_NAME_LEN);
      }
    }

    pColDef->dataType = pExpr->resType;

    if (!pColExists) {
      code = nodesListMakeAppend(&pStmt->pCols, (SNode*)pColDef);
      if (code != TSDB_CODE_SUCCESS) {
        nodesDestroyNode((SNode*)pColDef);
        PAR_ERR_JRET(code);
      }
    }
    index++;
  }

  if (LIST_LENGTH(pCalcProjection) < TSDB_MIN_COLUMNS) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_OUT_TABLE, "The number of columns in stream output must be greater than 1"));
  }

  return code;
_return:
  parserError("createStreamReqSetDefaultOutCols failed, code:%d", code);
  return code;
}

typedef struct SStreamForceOutputCxt {
  bool    needFill;
} SStreamForceOutputCxt;

static EDealRes placeHolderNeedFill(SNode *pNode, void *pContext) {
  SStreamForceOutputCxt* cxt = (SStreamForceOutputCxt*)pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_VALUE: {
      cxt->needFill &= true;
      return DEAL_RES_CONTINUE;
    }
    case QUERY_NODE_FUNCTION: {
      SFunctionNode* pFunc = (SFunctionNode*)pNode;
      if (fmIsPlaceHolderFunc(pFunc->funcId)) {
        cxt->needFill &= true;
      }
      return DEAL_RES_CONTINUE;
    }
    case QUERY_NODE_COLUMN:
      cxt->needFill &= false;
    default:
      break;
  }

  return DEAL_RES_CONTINUE;
}

static int32_t createStreamReqBuildForceOutput(STranslateContext* pCxt, SCreateStreamStmt* pStmt, SCMCreateStreamReq* pReq) {
  int32_t                code = TSDB_CODE_SUCCESS;
  SNode*                 pNode = NULL;
  SValueNode*            pVal = NULL;
  SNodeList*             pProjection = ((SSelectStmt*)pStmt->pQuery)->pProjectionList;
  SStreamTriggerOptions* pOptions = (SStreamTriggerOptions*)((SStreamTriggerNode*)pStmt->pTrigger)->pOptions;
  bool                   forceOut = pOptions ? pOptions->forceOutput : false;

  if (!forceOut) {
    return code;
  }

  pReq->forceOutCols = taosArrayInit(LIST_LENGTH(pProjection), sizeof(SStreamOutCol));
  if (pReq->forceOutCols == NULL) {
    PAR_ERR_JRET(terrno);
  }

  int32_t index = 0;
  FOREACH(pNode, pProjection) {
    SStreamOutCol         pOutCol = {0};
    SStreamForceOutputCxt cxt = {.needFill = true};
    nodesWalkExpr(pNode, placeHolderNeedFill, &cxt);
    if (cxt.needFill) {
      PAR_ERR_JRET(nodesNodeToString(pNode, false, (char**)&pOutCol.expr, NULL));
    } else {
      if (index == 0) {
        taosArrayDestroyEx(pReq->forceOutCols, tFreeStreamOutCol);
        pReq->forceOutCols = NULL;
        return code;
      }
      PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal));
      pVal->node.resType = ((SExprNode*)pNode)->resType;
      pVal->isNull = true;
      PAR_ERR_JRET(nodesNodeToString((SNode*)pVal, false, (char**)&pOutCol.expr, NULL));
      nodesDestroyNode((SNode*)pVal);
    }
    pOutCol.type = ((SExprNode*)pNode)->resType;
    if (NULL == taosArrayPush(pReq->forceOutCols, &pOutCol)) {
      PAR_ERR_JRET(terrno);
    }
    index++;
  }

  return code;
_return:
  parserError("createStreamReqBuildForceOutput failed, code:%d", code);
  nodesDestroyNode((SNode*)pVal);
  return code;
}

static int32_t extractCondFromCountWindow(STranslateContext* pCxt, SCountWindowNode* pCountWindow, SNode** pCond) {
  if (LIST_LENGTH(pCountWindow->pColList) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pCondList = NULL;
  SNode*     pNode = NULL;
  SNode*     pLogicCond = NULL;

  FOREACH(pNode, pCountWindow->pColList) {
    if (QUERY_NODE_COLUMN == nodeType(pNode)) {
      SColumnNode* pCol = (SColumnNode*)pNode;
      SNode*       pNameCond = NULL;
      PAR_ERR_RET(createOperatorNode(OP_TYPE_IS_NOT_NULL, pCol->colName, (SNode*)pCol, &pNameCond));
      PAR_ERR_RET(nodesListMakeAppend(&pCondList, pNameCond));
    } else {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TIMELINE_QUERY,
                                     "COUNT_WINDOW has invalid col name input");
    }
  }

  PAR_ERR_RET(nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, &pLogicCond));
  ((SLogicConditionNode*)pLogicCond)->pParameterList = pCondList;
  ((SLogicConditionNode*)pLogicCond)->condType = LOGIC_COND_TYPE_OR;

  *pCond = pLogicCond;
  return TSDB_CODE_SUCCESS;
}

static int32_t extractCondFromStateWindow(STranslateContext* pCxt, SStateWindowNode* pStateWindow, SNode** pCond) {
  if (!pStateWindow->pExpr) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER,
                                   "STATE_WINDOW has invalid col name input");
  }

  SNodeList* pCondList = NULL;
  SNode*     pLogicCond = NULL;

  SExprNode* pExpr = (SExprNode*)pStateWindow->pExpr;
  SNode*     pExprCond = NULL;
  PAR_ERR_RET(createIsOperatorNodeByNode(OP_TYPE_IS_NOT_NULL, (SNode*)pExpr, &pExprCond));
  PAR_ERR_RET(nodesListMakeAppend(&pCondList, pExprCond));

  PAR_ERR_RET(nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, &pLogicCond));
  ((SLogicConditionNode*)pLogicCond)->pParameterList = pCondList;
  ((SLogicConditionNode*)pLogicCond)->condType = LOGIC_COND_TYPE_AND;

  *pCond = pLogicCond;
  return TSDB_CODE_SUCCESS;
}

static int32_t createSimpleSelectStmtImpl(const char* pDb, const char* pTable, SNodeList* pProjectionList, SSelectStmt** pStmt);
static int32_t createSimpleSelectStmtFromCols(const char* pDb, const char* pTable, int32_t numOfProjs, const char* const pProjCol[], SSelectStmt** pStmt);

static int32_t createStreamReqBuildTriggerSelect(STranslateContext* pCxt, SRealTableNode* pTriggerTable, SSelectStmt** pTriggerSelect, SCreateStreamStmt* pStmt) {
  int32_t                code = TSDB_CODE_SUCCESS;
  SFunctionNode*         pFunc = NULL;
  SStreamTriggerNode*    pTrigger = (SStreamTriggerNode*)pStmt->pTrigger;
  SStreamTriggerOptions* pOptions = (SStreamTriggerOptions*)pTrigger->pOptions;
  SNode*                 pPreFilter = NULL;
  if (pOptions) {
    pPreFilter = pOptions->pPreFilter;
  }
  PAR_ERR_JRET(createSimpleSelectStmtImpl(pTriggerTable->table.dbName, pTriggerTable->table.tableName, NULL, pTriggerSelect));
  PAR_ERR_JRET(nodesCollectColumnsFromNode(pStmt->pTrigger, NULL, COLLECT_COL_TYPE_COL, &((SSelectStmt*)*pTriggerSelect)->pProjectionList));
  if (pPreFilter) {
    PAR_ERR_JRET(nodesCollectColumnsFromNode(pPreFilter, NULL, COLLECT_COL_TYPE_TAG, &((SSelectStmt*)*pTriggerSelect)->pProjectionList));
  }
  PAR_ERR_JRET(createTbnameFunction(&pFunc));
  PAR_ERR_JRET(nodesListMakeStrictAppend(&((SSelectStmt*)*pTriggerSelect)->pProjectionList, (SNode*)pFunc));

  return code;
_return:
  nodesDestroyNode((SNode*)pFunc);
  parserError("%s failed, code:%d", __func__, code);
  return code;
}

static int32_t createStreamReqBuildTriggerTranslateSelect(STranslateContext* pCxt, SStreamTriggerNode* pTrigger,
                                                          SSelectStmt* pTriggerSelect, SNode** pTriggerFilter) {
  int32_t code = TSDB_CODE_SUCCESS;

  PAR_ERR_JRET(nodesCloneNode(pTrigger->pTrigerTable, &pTriggerSelect->pFromTable));
  PAR_ERR_JRET(nodesCloneNode(*pTriggerFilter, &pTriggerSelect->pWhere));

  pCxt->pCurrStmt = (SNode*)pTriggerSelect;
  pCxt->streamInfo.triggerClause = true;
  pCxt->currClause = SQL_CLAUSE_SELECT;
  PAR_ERR_JRET(translateSelect(pCxt, pTriggerSelect));
  pCxt->streamInfo.triggerClause = false;
  *pTriggerFilter = NULL;

  return code;
_return:
  parserError("%s failed, code:%d", __func__, code);
  return code;
}

static int32_t createStreamReqBulidTriggerExtractCondFromWindow(STranslateContext* pCxt, SNode* pTriggerWindow, SNode** pTriggerFilter) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pLogicCond = NULL;

  switch (nodeType(pTriggerWindow)) {
    case QUERY_NODE_COUNT_WINDOW: {
      PAR_ERR_JRET(extractCondFromCountWindow(pCxt, (SCountWindowNode*)pTriggerWindow, &pLogicCond));
      if (pLogicCond) {
        PAR_ERR_JRET(nodesMergeNode(pTriggerFilter, &pLogicCond));
      }
      break;
    }
    default:
      break;
  }

_return:
  if (code) {
    parserError("%s failed, code:%d", __func__, code);
  }
  nodesDestroyNode((SNode*)pLogicCond);
  return code;
}

typedef struct SCheckPartitionCxt {
  int32_t            code;
  STranslateContext* pCxt;
} SCheckPartitionCxt;


static EDealRes createStreamReqBuildTriggerCheckPartitionWalker(SNode* pNode, void* pContext) {
  SCheckPartitionCxt *pCxt = (SCheckPartitionCxt*)pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN: {
      SColumnNode* pCol = (SColumnNode*)pNode;
      if (pCol->colType != COLUMN_TYPE_TAG) {
        pCxt->code = generateSyntaxErrMsgExt(&pCxt->pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER, "only tag can be used in partition");
        return DEAL_RES_ERROR;
      }
      break;
    }
    case QUERY_NODE_FUNCTION: {
      SFunctionNode *pFunction = (SFunctionNode*)pNode;
      if (!fmIsScalarFunc(pFunction->funcId) && pFunction->funcType != FUNCTION_TYPE_TBNAME) {
        pCxt->code = generateSyntaxErrMsgExt(&pCxt->pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER, "only scalar function and tbname can be used in partition");
        return DEAL_RES_ERROR;
      }
      break;
    }
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t createStreamReqBuildTriggerCheckPartition(STranslateContext* pCxt, SNodeList* pTriggerPartition, STableMeta* pTriggerTableMeta, SNode* pTriggerWindow) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;

  SCheckPartitionCxt cxt = {.code = TSDB_CODE_SUCCESS, .pCxt = pCxt};
  nodesWalkExprs(pTriggerPartition, createStreamReqBuildTriggerCheckPartitionWalker, &cxt);
  PAR_ERR_JRET(cxt.code);

  if (pTriggerTableMeta->tableType == TSDB_SUPER_TABLE && nodeType(pTriggerWindow) != QUERY_NODE_INTERVAL_WINDOW &&
      nodeType(pTriggerWindow) != QUERY_NODE_SESSION_WINDOW && nodeType(pTriggerWindow) != QUERY_NODE_PERIOD_WINDOW &&
      (LIST_LENGTH(pTriggerPartition) == 0 || !hasTbnameFunction(pTriggerPartition))) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(
        &pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER,
        "Partition by tbname is required for super table trigger when trigger window is not interval and session"));
  }

  return code;
_return:
  parserError("%s failed, code:%d", __func__, code);
  return code;
}

static int32_t createStreamReqBuildTriggerTranslateWindow(STranslateContext* pCxt, SNode** pTriggerWindow) {
  int32_t code = TSDB_CODE_SUCCESS;

  pCxt->currClause = SQL_CLAUSE_WINDOW;
  PAR_ERR_JRET(translateExpr(pCxt, pTriggerWindow));

_return:
  if (code) {
    parserError("%s failed, code:%d", __func__, code);
  }
  return code;
}

static int32_t createStreamReqBuildTriggerTranslatePartition(STranslateContext* pCxt, SNodeList* pTriggerPartition, STableMeta* pTriggerTableMeta, SNode* pTriggerWindow) {
  int32_t code = TSDB_CODE_SUCCESS;

  pCxt->currClause = SQL_CLAUSE_PARTITION_BY;
  PAR_ERR_JRET(translateExprList(pCxt, pTriggerPartition));

  PAR_ERR_JRET(createStreamReqBuildTriggerCheckPartition(pCxt, pTriggerPartition, pTriggerTableMeta, pTriggerWindow));
_return:
  if (code) {
    parserError("%s failed, code:%d", __func__, code);
  }
  return code;
}

static int32_t createStreamReqBuildTriggerAst(STranslateContext* pCxt, SCreateStreamStmt* pStmt,
                                              SSelectStmt** pTriggerSelect, SHashObj **pTriggerSlotHash,
                                              SNode** pTriggerFilter, SCMCreateStreamReq* pReq) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SStreamTriggerNode*  pTrigger = (SStreamTriggerNode*)pStmt->pTrigger;
  SNode**              pTriggerWindow = &pTrigger->pTriggerWindow;
  SNodeList*           pTriggerPartition = pTrigger->pPartitionList;
  STableMeta*          pTriggerTableMeta = NULL;
  SRealTableNode*      pTriggerTable = (SRealTableNode*)pTrigger->pTrigerTable;

  parserDebug("translate create stream req start build trigger info, streamId:%"PRId64, pReq->streamId);

  if (!pTriggerTable) {
    // no trigger table, only translate trigger window
    parserDebug("no trigger table in create stream req, streamId:%"PRId64, pReq->streamId);
    PAR_ERR_JRET(createStreamReqBuildTriggerTranslateWindow(pCxt, pTriggerWindow));
    PAR_RET(createStreamReqBuildTriggerBuildWindowInfo(pCxt, *pTriggerWindow, pReq));
  }

  PAR_ERR_JRET(createStreamReqBulidTriggerExtractCondFromWindow(pCxt, *pTriggerWindow, pTriggerFilter));

  pReq->triggerHasPF = (*pTriggerFilter != NULL);

  PAR_ERR_JRET(getTableMeta(pCxt, pTriggerTable->table.dbName, pTriggerTable->table.tableName, &pTriggerTableMeta));

  SNodeList *projList = NULL;
  PAR_ERR_JRET(createStreamReqBuildTriggerTableInfo(pCxt, pTriggerTable, pTriggerTableMeta, pReq));
  PAR_ERR_JRET(createStreamReqBuildTriggerSelect(pCxt, pTriggerTable, pTriggerSelect, pStmt));

  PAR_ERR_JRET(createStreamReqBuildTriggerTranslateSelect(pCxt, pTrigger, *pTriggerSelect, pTriggerFilter));
  PAR_ERR_JRET(createStreamReqBuildTriggerTranslateWindow(pCxt, pTriggerWindow));
  PAR_ERR_JRET(createStreamReqBuildTriggerTranslatePartition(pCxt, pTriggerPartition, pTriggerTableMeta, *pTriggerWindow));

_return:
  if (code) {
    parserError("%s failed, code:%d", __func__, code);
  }
  taosMemoryFreeClear(pTriggerTableMeta);
  return code;
}

static int32_t createStreamReqCheckPlaceHolder(STranslateContext* pCxt, SCMCreateStreamReq* pReq, int32_t placeHolderBitmap, SNodeList *pTriggerPartition) {
  int32_t  code = TSDB_CODE_SUCCESS;
  if (BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_CURRENT_TS) ||
      BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_PREV_TS) ||
      BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_NEXT_TS)) {
    if (!(pReq->triggerType == WINDOW_TYPE_INTERVAL && pReq->trigger.sliding.interval == 0)) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "_tcurrent_ts/_tprev_ts/_tnext_ts can only be used in sliding window"));
    }
  }

  if (BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_WSTART) || BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_WEND) ||
      BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_WDURATION) || BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_WROWNUM)) {
    if (pReq->triggerType == WINDOW_TYPE_PERIOD || (pReq->triggerType == WINDOW_TYPE_INTERVAL && pReq->trigger.sliding.interval == 0)) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "_twstart/_twend/_twduration/_twrownum can not be used in period window and sliding window without interval"));
    }
  }

  if (BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_PREV_LOCAL) || BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_NEXT_LOCAL)) {
    if (pReq->triggerType != WINDOW_TYPE_PERIOD) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "_tprev_localtime/_tnext_localtime can only be used in period window"));
    }
  }

  if (BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_PARTITION_TBNAME)) {
    if (!hasTbnameFunction(pTriggerPartition)) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "%%%%tbname can only be used when partition with tbname"));
    }
  }

  if (BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS)) {
    if (pReq->eventTypes != EVENT_WINDOW_CLOSE) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "%%%%trows can only be used when event type is window close"));
    }
  }

  return code;
_return:
  parserError("createStreamReqCheckPlaceHolder failed, code:%d", code);
  return code;
}

static int32_t createStreamReqBuildTriggerPlan(STranslateContext* pCxt, SCreateStreamStmt* pStmt,
                                               SSelectStmt** pTriggerSelect, SHashObj **pTriggerSlotHash,
                                               SCMCreateStreamReq* pReq) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SStreamTriggerNode*  pTrigger = (SStreamTriggerNode*)pStmt->pTrigger;
  SNode*               pTriggerWindow = pTrigger->pTriggerWindow;
  SNodeList*           pTriggerPartition = pTrigger->pPartitionList;
  SRealTableNode*      pTriggerTable = (SRealTableNode*)pTrigger->pTrigerTable;

  parserDebug("translate create stream req start build trigger info, streamId:%"PRId64, pReq->streamId);

  if (!pTriggerTable) {
    // no trigger table, only translate trigger window
    PAR_RET(TSDB_CODE_SUCCESS);
  }

  PAR_ERR_JRET(createStreamReqBuildTriggerBuildPlan(pCxt, *pTriggerSelect, pReq, pTriggerSlotHash, pTriggerWindow, pTriggerPartition));
  PAR_ERR_JRET(createStreamReqBuildTriggerBuildWindowInfo(pCxt, pTriggerWindow, pReq));
  PAR_ERR_JRET(createStreamReqCheckPlaceHolder(pCxt, pReq, pReq->placeHolderBitmap, pTriggerPartition));
  PAR_ERR_JRET(createStreamReqSetDefaultTag(pCxt, pStmt, pTriggerPartition, pReq));

_return:
  if (code) {
    parserError("%s failed, code:%d", __func__, code);
  }
  return code;
}

static void eliminateNodeFromList(SNode* pTarget, SNodeList* pList) {
  SNode*  pNode = NULL;
  WHERE_EACH(pNode, pList) {
    if (nodesEqualNode(pNode, pTarget)) {
      REPLACE_NODE(NULL);
      ERASE_NODE(pList);
      break;
    }
    WHERE_NEXT;
  }
}

static int32_t replaceSubPlanFromList(SNode* pTarget, SNodeList* pList) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  SNode*  pValue = NULL;
  FOREACH(pNode, pList) {
    if (nodesEqualNode(pNode, pTarget)) {
      int64_t subplanId = (int64_t)((SSubplan*)pTarget)->id.groupId << 32 | ((SSubplan*)pTarget)->id.subplanId;
      PAR_ERR_RET(nodesMakeValueNodeFromInt64(subplanId, &pValue));
      REPLACE_NODE(pValue);
      break;
    }
  }
  return code;
}

typedef struct SCheckNotifyCondContext {
  STranslateContext *pTransCxt;
  bool               valid;
} SCheckNotifyCondContext;

static EDealRes doCheckNotifyCond(SNode** pNode, void* pContext) {
  SCheckNotifyCondContext* pCxt = (SCheckNotifyCondContext*)pContext;
  SSelectStmt*             pSelect = (SSelectStmt*)pCxt->pTransCxt->pCurrStmt;
  SNode*                   pProj = NULL;

  if (nodeType(*pNode) == QUERY_NODE_VALUE) {
    return DEAL_RES_CONTINUE;
  }

  FOREACH(pProj, pSelect->pProjectionList) {
    if (nodesEqualNode(pProj, *pNode)) {
      return DEAL_RES_IGNORE_CHILD;
    }
    if (nodesIsStar(pProj) && nodeType(pProj) == QUERY_NODE_COLUMN) {
      // if projection is *, then all columns are valid
      return DEAL_RES_IGNORE_CHILD;
    }
    if (strcmp(((SExprNode*)pProj)->userAlias, ((SExprNode*)*pNode)->aliasName) == 0) {
      SNode*  tmpNode = NULL;
      int32_t code = nodesCloneNode(pProj, &tmpNode);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode(tmpNode);
        parserError("%s failed, code:%d", __func__, code);
        return DEAL_RES_ERROR;
      }
      nodesDestroyNode(*pNode);
      *pNode = tmpNode;
      return DEAL_RES_IGNORE_CHILD;
    }
  }

  if (nodeType(*pNode) == QUERY_NODE_COLUMN) {
    pCxt->valid = false;
    return DEAL_RES_ERROR;
  }

  return DEAL_RES_CONTINUE;
}

static int32_t getExtWindowBorder(STranslateContext* pCxt, SNode* pTriggerWindow, bool* eqLeft, bool* eqRight) {
  switch(nodeType(pTriggerWindow)) {
    case QUERY_NODE_INTERVAL_WINDOW: {
      *eqLeft = true;
      *eqRight = false;
      break;
    }
    case QUERY_NODE_COUNT_WINDOW:
    case QUERY_NODE_EVENT_WINDOW:
    case QUERY_NODE_SESSION_WINDOW:
    case QUERY_NODE_PERIOD_WINDOW:
    case QUERY_NODE_STATE_WINDOW: {
      *eqLeft = true;
      *eqRight = true;
      break;
    }
    default:
      PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_TRIGGER, "Invalid trigger window type %d", nodeType(pTriggerWindow)));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateStreamCalcQuery(STranslateContext* pCxt, SNodeList* pTriggerPartition, SNode* pTriggerTbl,
                                        SNode* pStreamCalcQuery, SNode* pNotifyCond, SNode* pTriggerWindow) {
  int32_t    code = TSDB_CODE_SUCCESS;
  ESqlClause currClause = pCxt->currClause;
  SNode*     pCurrStmt = pCxt->pCurrStmt;
  int32_t    currLevel = pCxt->currLevel;

  parserDebug("translate create stream req start translate calculate query");

  PAR_ERR_JRET(getExtWindowBorder(pCxt, pTriggerWindow, &pCxt->streamInfo.extLeftEq, &pCxt->streamInfo.extRightEq));

  pCxt->currLevel = ++(pCxt->levelNo);
  pCxt->currClause = SQL_CLAUSE_SELECT;
  pCxt->streamInfo.triggerTbl = pTriggerTbl;
  pCxt->streamInfo.triggerPartitionList = pTriggerPartition;
  pCxt->streamInfo.calcClause = true;
  pCxt->pCurrStmt = (SNode*)pStreamCalcQuery;

  if (pNotifyCond) {
    SCheckNotifyCondContext checkNotifyCondCxt = {.pTransCxt = pCxt, .valid = true};
    nodesRewriteExpr(&pNotifyCond, doCheckNotifyCond, &checkNotifyCondCxt);
    if (!checkNotifyCondCxt.valid) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_NOTIFY_COND, "notify condition can only contain expr from query clause"));
    }

    switch (nodeType(pStreamCalcQuery)) {
      case QUERY_NODE_SET_OPERATOR: {
        PAR_ERR_JRET(nodesListMakeAppend(&((SSetOperator*)pStreamCalcQuery)->pProjectionList, pNotifyCond));
        break;
      }
      case QUERY_NODE_SELECT_STMT: {
        PAR_ERR_JRET(nodesListMakeAppend(&((SSelectStmt*)pStreamCalcQuery)->pProjectionList, pNotifyCond));
        break;
      }
      default: {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_QUERY, "Stream query must be a select or union statement"));
      }
    }

  }

  PAR_ERR_JRET(translateQuery(pCxt, pStreamCalcQuery));
  pCxt->streamInfo.calcClause = false;
  pCxt->streamInfo.triggerTbl = NULL;
  pCxt->streamInfo.triggerPartitionList = NULL;

  pCxt->currClause = currClause;
  pCxt->pCurrStmt = pCurrStmt;
  pCxt->currLevel = currLevel;

  return code;
_return:
  parserError("translateStreamCalcQuery failed, code:%d", code);
  return code;
}

static bool findNodeInList(SNode* pTarget, SNodeList* pList) {
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    if (nodesEqualNode(pNode, pTarget)) {
      return true;
    }
  }
  return false;
}

static int32_t createStreamReqBuildCalcDb(STranslateContext* pCxt, SHashObj* pDbs, SCMCreateStreamReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  char*   dbName = NULL;
  char*   nameDup = NULL;

  pReq->calcDB = taosArrayInit(1, POINTER_BYTES);
  if (pReq->calcDB == NULL) {
    PAR_ERR_JRET(terrno);
  }

  void *pIter = taosHashIterate(pDbs, NULL);
  while (pIter) {
    size_t klen = 0;
    dbName = taosHashGetKey(pIter, &klen);
    nameDup = taosMemoryCalloc(1, klen + 1);
    if (NULL == nameDup || NULL == dbName) {
      PAR_ERR_JRET(terrno);
    }

    (void)memcpy(nameDup, dbName, klen);

    if (taosArrayPush(pReq->calcDB, &nameDup) == NULL) {
      PAR_ERR_JRET(terrno);
    }
    pIter = taosHashIterate(pDbs, pIter);
  }
  return code;

_return:
  parserError("createStreamReqBuildCalcDb failed, code:%d", code);
  taosMemoryFreeClear(nameDup);
  return code;
}

static int32_t createStreamReqBuildCalcPlan(STranslateContext* pCxt, SQueryPlan* calcPlan,
                                            SArray* pVgArray, SCMCreateStreamReq* pReq) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SHashObj*        pPlanMap = NULL;
  SStreamCalcScan* pCalcScan = NULL;
  bool             cutoff = false;

  parserDebug("translate create stream req start build calculate plan");

  pReq->calcScanPlanList = taosArrayInit(1, sizeof(SStreamCalcScan));
  pPlanMap = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (NULL == pReq->calcScanPlanList || NULL == pPlanMap) {
    PAR_ERR_JRET(terrno);
  }

  pReq->calcTsSlotId = -1;
  for (int32_t i = 0; i < taosArrayGetSize(pVgArray); i++) {
    pCalcScan = taosArrayGet(pVgArray, i);
    if (pCalcScan == NULL) {
      PAR_ERR_JRET(terrno);
    }
    cutoff = false;
    SSubplan* pScanSubPlan = (SSubplan*)pCalcScan->scanPlan;
    SNode*    pTargetNode = (SNode*)pScanSubPlan->pNode;
    SNode*    pNode = NULL;
    int64_t   hashKey = (int64_t) pScanSubPlan->id.groupId << 32 | pScanSubPlan->id.subplanId;

    if (((SPhysiNode*)pTargetNode)->pParent) {
      eliminateNodeFromList(pTargetNode, ((SPhysiNode*)pTargetNode)->pParent->pChildren);
      ((SPhysiNode*)pTargetNode)->pParent = NULL;
    }

    if (pScanSubPlan->pParents) {
      FOREACH(pNode, pScanSubPlan->pParents) {
        PAR_ERR_JRET(replaceSubPlanFromList((SNode*)pScanSubPlan, ((SSubplan*)pNode)->pChildren));
      }
      pScanSubPlan->pParents = NULL;
    }

    WHERE_EACH(pNode, calcPlan->pSubplans) {
      SNodeListNode* pGroup = (SNodeListNode*)pNode;
      if (findNodeInList((SNode*)pScanSubPlan, pGroup->pNodeList)) {
        eliminateNodeFromList((SNode*)pScanSubPlan, pGroup->pNodeList);
        if (LIST_LENGTH(pGroup->pNodeList) == 0) {
          REPLACE_NODE(NULL);
          ERASE_NODE(calcPlan->pSubplans);
        }
        calcPlan->numOfSubplans--;
        break;
      }
      WHERE_NEXT;
    }

    cutoff = true;

    if (taosHashGet(pPlanMap, &hashKey, sizeof(int64_t)) != NULL) {
      continue;
    } else {
      PAR_ERR_JRET(taosHashPut(pPlanMap, &hashKey, sizeof(int64_t), NULL, 0));
    }

    if (pCalcScan->readFromCache) {
      findTsSlotId((SScanPhysiNode*)pScanSubPlan->pNode, &pReq->calcTsSlotId);
      if (pReq->calcTsSlotId == -1) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_QUERY, "Can not find timestamp primary key in trigger query scan"));
      }
    }

    SStreamCalcScan pNewScan = {0};
    pNewScan.readFromCache = pCalcScan->readFromCache;
    TSWAP(pNewScan.vgList, pCalcScan->vgList);
    PAR_ERR_JRET(nodesNodeToString(pCalcScan->scanPlan, false, (char**)&pNewScan.scanPlan, NULL));
    if (NULL == taosArrayPush(pReq->calcScanPlanList, &pNewScan)) {
      PAR_ERR_JRET(terrno);
    }
    taosArrayDestroy(pCalcScan->vgList);
    nodesDestroyNode(pCalcScan->scanPlan);
    cutoff = false;
  }

  pReq->numOfCalcSubplan = calcPlan->numOfSubplans;
  PAR_ERR_JRET(nodesNodeToString((SNode*)calcPlan, false, (char**)&pReq->calcPlan, NULL));

  taosHashCleanup(pPlanMap);
  return code;
_return:
  if (cutoff) {
    taosArrayDestroy(pCalcScan->vgList);
    nodesDestroyNode(pCalcScan->scanPlan);
  }
  parserError("createStreamReqBuildCalcPlan failed, code:%d", code);
  taosHashCleanup(pPlanMap);
  return code;
}

static int32_t createStreamReqBuildCalc(STranslateContext* pCxt, SCreateStreamStmt* pStmt,
                                        SNodeList *pTriggerPartition, SSelectStmt* pTriggerSelect, SNode* pTriggerWindow, SNode* pNotifyCond,
                                        SCMCreateStreamReq* pReq) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SQueryPlan*  calcPlan = NULL;
  SArray*      pVgArray = NULL;
  SHashObj*    pDbs = NULL;
  SNodeList*   pProjectionList = NULL;

  parserDebug("translate create stream req start build calculate part, streamId:%"PRId64, pReq->streamId);

  if (!pStmt->pQuery) {
    parserDebug("no query in create stream req");
    return code;
  }

  pVgArray = taosArrayInit(1, sizeof(SStreamCalcScan));
  pDbs = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (NULL == pDbs || NULL == pVgArray) {
    PAR_ERR_JRET(terrno);
  }

  pCxt->streamInfo.calcDbs = pDbs;
  PAR_ERR_JRET(translateStreamCalcQuery(pCxt, pTriggerPartition, pTriggerSelect ? pTriggerSelect->pFromTable : NULL, pStmt->pQuery, pNotifyCond, pTriggerWindow));

  pReq->placeHolderBitmap = pCxt->streamInfo.placeHolderBitmap;
  if (BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS) &&
      (pReq->triggerTblType == TSDB_VIRTUAL_NORMAL_TABLE ||
       pReq->triggerTblType == TSDB_VIRTUAL_CHILD_TABLE ||
       BIT_FLAG_TEST_MASK(pReq->flags, CREATE_STREAM_FLAG_TRIGGER_VIRTUAL_STB))) {
    if (pStmt->pTrigger && ((SStreamTriggerNode*)pStmt->pTrigger)->pOptions &&
        ((SStreamTriggerOptions*)((SStreamTriggerNode*)pStmt->pTrigger)->pOptions)->pPreFilter) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_STREAM_INVALID_QUERY, "Not support pre_filter when trigger table is virtual table and using %%trows in stream query."));
    }
  }

  pProjectionList = nodeType(pStmt->pQuery) == QUERY_NODE_SELECT_STMT ?
                                                                      ((SSelectStmt*)pStmt->pQuery)->pProjectionList :
                                                                      ((SSetOperator*)pStmt->pQuery)->pProjectionList;

  PAR_ERR_JRET(createStreamReqSetDefaultOutCols(pCxt, pStmt, pProjectionList, pReq));

  PAR_ERR_JRET(createStreamReqBuildForceOutput(pCxt, pStmt, pReq));

  SQuery pQuery = {.pRoot = pStmt->pQuery};
  PAR_ERR_JRET(calculateConstant(pCxt->pParseCxt, &pQuery));

  SPlanContext calcCxt = {.acctId = pCxt->pParseCxt->acctId,
                          .mgmtEpSet = pCxt->pParseCxt->mgmtEpSet,
                          .pAstRoot = pStmt->pQuery,
                          .pMsg = pCxt->pParseCxt->pMsg,
                          .msgLen = pCxt->pParseCxt->msgLen,
                          .streamCalcQuery = true,
                          .streamTriggerWinType = nodeType(pTriggerWindow),
                          .pStreamCalcVgArray = pVgArray,
                          .streamTriggerScanList = NULL};

  if (nodeType(pTriggerWindow) == QUERY_NODE_INTERVAL_WINDOW) {
    SIntervalWindowNode* pIntervalWindow = (SIntervalWindowNode*)pTriggerWindow;
    if (!pIntervalWindow->pInterval) {
      calcCxt.streamTriggerWinType = QUERY_NODE_SLIDING_WINDOW;
    }
  }

  PAR_ERR_JRET(qCreateQueryPlan(&calcCxt, &calcPlan, NULL));
  pReq->vtableCalc = (int8_t)calcCxt.streamVtableCalc;

  if (BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS) &&
      LIST_LENGTH(calcCxt.streamTriggerScanList) > 0) {
    // need collect scan cols and put into trigger's scan list
    PAR_ERR_JRET(nodesListAppendList(pTriggerSelect->pProjectionList, calcCxt.streamTriggerScanList));
    SNode *pCol = NULL;
    FOREACH(pCol, pTriggerSelect->pProjectionList) {
      if (nodeType(pCol) == QUERY_NODE_COLUMN) {
        SColumnNode* pColumn = (SColumnNode*)pCol;
        tstrncpy(pColumn->tableAlias, pColumn->tableName, TSDB_TABLE_NAME_LEN);
      }
    }
  }

  PAR_ERR_JRET(createStreamReqBuildCalcDb(pCxt, pDbs, pReq));

  PAR_ERR_JRET(createStreamReqBuildCalcPlan(pCxt, calcPlan, pVgArray, pReq));

_return:
  if (code) {
    parserError("%s failed, code:%d", __func__, code);
  }
  taosArrayDestroy(pVgArray);
  taosHashCleanup(pDbs);
  nodesDestroyNode((SNode*)calcPlan);
  return code;
}


static int32_t createStreamReqBuildDefaultReq(STranslateContext* pCxt, SCreateStreamStmt* pStmt, SCMCreateStreamReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  pReq->expiredTime = 0;
  pReq->maxDelay = 0;
  pReq->watermark = 0;
  pReq->fillHistoryStartTime = 0;
  pReq->eventTypes = EVENT_WINDOW_CLOSE;
  pReq->igDisorder = 0;
  pReq->deleteReCalc = 0;
  pReq->deleteOutTbl = 0;
  pReq->fillHistory = 0;
  pReq->fillHistoryFirst = 0;
  pReq->calcNotifyOnly = 0;
  pReq->lowLatencyCalc = 0;
  pReq->igNoDataTrigger = 0;
  pReq->flags = CREATE_STREAM_FLAG_NONE;
  pReq->placeHolderBitmap = PLACE_HOLDER_NONE;
  pReq->triTsSlotId = -1;

  return TSDB_CODE_SUCCESS;
}

static int32_t createStreamReqBuildNameAndId(STranslateContext* pCxt, SCreateStreamStmt* pStmt, SCMCreateStreamReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  SName   streamName;

  parserDebug("translate create stream req start build stream name and id, streamName:%s.%s", pStmt->streamDbName, pStmt->streamName);
  PAR_ERR_JRET(taosGetSystemUUIDU64(&pReq->streamId));

  // name and sql
  pReq->streamDB = taosMemoryMalloc(TSDB_DB_FNAME_LEN);
  pReq->name = taosMemoryCalloc(1, TSDB_STREAM_FNAME_LEN);
  pReq->sql = taosStrdup(pCxt->pParseCxt->pSql);

  if (NULL == pReq->sql || NULL == pReq->streamDB || NULL == pReq->name) {
    parserError("buildCreateStreamReq failed to allocate memory, streamDb:%p, streamName:%p sql:%p",
                pReq->streamDB, pReq->name, pReq->sql);
    PAR_ERR_JRET(terrno);
  }

  toName(pCxt->pParseCxt->acctId, pStmt->streamDbName, pStmt->streamName, &streamName);
  PAR_ERR_JRET(tNameExtractFullName(&streamName, pReq->name));
  PAR_ERR_JRET(tNameGetFullDbName(&streamName, pReq->streamDB));

  return code;
_return:
  parserError("buildCreateStreamReq failed, code:%d, streamId:%"PRId64", streamName:%s", code, pReq->streamId, pReq->name);
  return code;
}

static int32_t buildCreateStreamReq(STranslateContext* pCxt, SCreateStreamStmt* pStmt, SCMCreateStreamReq* pReq) {
  int32_t                code = TSDB_CODE_SUCCESS;
  SStreamTriggerNode*    pTrigger = (SStreamTriggerNode*)pStmt->pTrigger;
  SStreamTriggerOptions* pTriggerOptions = (SStreamTriggerOptions*)pTrigger->pOptions;
  SStreamNotifyOptions*  pNotifyOptions = (SStreamNotifyOptions*)pTrigger->pNotify;
  SNode*                 pTriggerWindow = pTrigger->pTriggerWindow;
  SSelectStmt*           pTriggerSelect = NULL;
  SHashObj*              pTriggerSlotHash = NULL;
  SNode*                 pNotifyCond = NULL;
  SNode*                 pTriggerFilter = ((SStreamTriggerOptions*)pTrigger->pOptions) ? ((SStreamTriggerOptions*)pTrigger->pOptions)->pPreFilter : NULL;

  PAR_ERR_JRET(createStreamReqBuildDefaultReq(pCxt, pStmt, pReq));
  PAR_ERR_JRET(createStreamReqBuildNameAndId(pCxt, pStmt, pReq));
  PAR_ERR_JRET(createStreamReqBuildNotifyOptions(pCxt, pNotifyOptions, &pNotifyCond, pReq));
  PAR_ERR_JRET(createStreamReqBuildTriggerAst(pCxt, pStmt, &pTriggerSelect, &pTriggerSlotHash, &pTriggerFilter, pReq));
  PAR_ERR_JRET(createStreamReqBuildTriggerOptions(pCxt, pStmt, pTriggerOptions, pReq));
  PAR_ERR_JRET(createStreamReqBuildCalc(pCxt, pStmt, pTrigger->pPartitionList, pTriggerSelect, pTriggerWindow, pNotifyCond, pReq));
  PAR_ERR_JRET(createStreamReqBuildTriggerPlan(pCxt, pStmt, &pTriggerSelect, &pTriggerSlotHash, pReq));
  PAR_ERR_JRET(createStreamReqBuildOutTable(pCxt, pStmt, pTriggerSlotHash, pReq));

_return:
  if (code) {
    parserError("buildCreateStreamReq failed, code:%d", code);
  }
  taosHashCleanup(pTriggerSlotHash);
  nodesDestroyNode((SNode*)pTriggerSelect);
  nodesDestroyNode(pNotifyCond);
  return code;
}

static int32_t translateCreateStream(STranslateContext* pCxt, SCreateStreamStmt* pStmt) {
  int32_t            code = TSDB_CODE_SUCCESS;
  SCMCreateStreamReq createReq = {0};

  PAR_ERR_JRET(checkCreateStream(pCxt, pStmt));
  PAR_ERR_JRET(buildCreateStreamReq(pCxt, pStmt, &createReq));
  PAR_ERR_JRET(buildCmdMsg(pCxt, TDMT_MND_CREATE_STREAM, (FSerializeFunc)tSerializeSCMCreateStreamReq, &createReq));

_return:
  if (code) {
    parserError("translateCreateStream failed, code:%d", code);
  }
  tFreeSCMCreateStreamReq(&createReq);
  return code;
}

static int32_t translateDropStream(STranslateContext* pCxt, SDropStreamStmt* pStmt) {
  SMDropStreamReq req = {0};
  SName           name;
  int32_t         code = TSDB_CODE_SUCCESS;

  req.name = taosMemoryCalloc(1, TSDB_STREAM_FNAME_LEN);
  if (NULL == req.name) {
    PAR_ERR_JRET(terrno);
  }

  toName(pCxt->pParseCxt->acctId, pStmt->streamDbName, pStmt->streamName, &name);
  PAR_ERR_JRET(tNameExtractFullName(&name, req.name));
  req.igNotExists = (int8_t)pStmt->ignoreNotExists;
  PAR_ERR_JRET(buildCmdMsg(pCxt, TDMT_MND_DROP_STREAM, (FSerializeFunc)tSerializeSMDropStreamReq, &req));

_return:
  tFreeMDropStreamReq(&req);
  return code;
}

static int32_t translatePauseStream(STranslateContext* pCxt, SPauseStreamStmt* pStmt) {
  SMPauseStreamReq req = {0};
  SName            name;
  int32_t          code = TSDB_CODE_SUCCESS;

  req.name = taosMemoryCalloc(1, TSDB_STREAM_FNAME_LEN);
  if (NULL == req.name) {
    PAR_ERR_JRET(terrno);
  }

  toName(pCxt->pParseCxt->acctId, pStmt->streamDbName, pStmt->streamName, &name);
  PAR_ERR_JRET(tNameExtractFullName(&name, req.name));
  req.igNotExists = (int8_t)pStmt->ignoreNotExists;
  PAR_ERR_JRET(buildCmdMsg(pCxt, TDMT_MND_STOP_STREAM, (FSerializeFunc)tSerializeSMPauseStreamReq, &req));

_return:
  tFreeMPauseStreamReq(&req);
  return code;
}

static int32_t translateResumeStream(STranslateContext* pCxt, SResumeStreamStmt* pStmt) {
  SMResumeStreamReq req = {0};
  SName             name;
  int32_t           code = TSDB_CODE_SUCCESS;

  req.name = taosMemoryCalloc(1, TSDB_STREAM_FNAME_LEN);
  if (NULL == req.name) {
    PAR_ERR_JRET(terrno);
  }

  toName(pCxt->pParseCxt->acctId, pStmt->streamDbName, pStmt->streamName, &name);
  PAR_ERR_JRET(tNameExtractFullName(&name, req.name));

  req.igNotExists = (int8_t)pStmt->ignoreNotExists;
  req.igUntreated = (int8_t)pStmt->ignoreUntreated;
  PAR_ERR_JRET(buildCmdMsg(pCxt, TDMT_MND_START_STREAM, (FSerializeFunc)tSerializeSMResumeStreamReq, &req));

_return:
  tFreeMResumeStreamReq(&req);
  return code;
}

static int32_t translateRecalcStream(STranslateContext* pCxt, SRecalcStreamStmt* pStmt) {
  SMRecalcStreamReq req = {0};
  SName             name;
  int32_t           code = TSDB_CODE_SUCCESS;

  req.name = taosMemoryCalloc(1, TSDB_STREAM_FNAME_LEN);
  if (NULL == req.name) {
    PAR_ERR_JRET(terrno);
  }

  toName(pCxt->pParseCxt->acctId, pStmt->streamDbName, pStmt->streamName, &name);
  PAR_ERR_JRET(tNameExtractFullName(&name, req.name));
  req.calcAll = (int8_t)((SStreamCalcRangeNode*)pStmt->pRange)->calcAll;
  PAR_ERR_JRET(translateTimeRange(pCxt, pStmt->streamDbName, ((SStreamCalcRangeNode*)pStmt->pRange)->pStart, ((SStreamCalcRangeNode*)pStmt->pRange)->pEnd, &req.timeRange));
  PAR_ERR_JRET(buildCmdMsg(pCxt, TDMT_MND_RECALC_STREAM, (FSerializeFunc)tSerializeSMRecalcStreamReq, &req));

_return:
  tFreeMRecalcStreamReq(&req);
  return code;
}

static int32_t validateCreateView(STranslateContext* pCxt, SCreateViewStmt* pStmt) {
  if (QUERY_NODE_SELECT_STMT != nodeType(pStmt->pQuery) && QUERY_NODE_SET_OPERATOR != nodeType(pStmt->pQuery)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VIEW_QUERY, "Invalid view query type");
  }

  /*
    STableMeta* pMetaCache = NULL;
    int32_t code = getTableMeta(pCxt, pStmt->dbName, pStmt->viewName, &pMetaCache);
    if (TSDB_CODE_SUCCESS == code) {
      taosMemoryFreeClear(pMetaCache);
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_VIEW_CONFLICT_WITH_TABLE, "View name is conflict with
    table");
    }
  */

  return TSDB_CODE_SUCCESS;
}

static int32_t translateCreateView(STranslateContext* pCxt, SCreateViewStmt* pStmt) {
#ifndef TD_ENTERPRISE
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif

  SParseSqlRes res = {.resType = PARSE_SQL_RES_SCHEMA};
  SName        name = {0};
  char         dbFName[TSDB_DB_FNAME_LEN];
  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->viewName, &name);
  (void)tNameGetFullDbName(&name, dbFName);

  int32_t code = validateCreateView(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = (*pCxt->pParseCxt->parseSqlFp)(pCxt->pParseCxt->parseSqlParam, pStmt->dbName, pStmt->pQuerySql, false, NULL,
                                          &res);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(&name, pCxt->pTargetTables);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pStmt->createReq.precision = res.schemaRes.precision;
    pStmt->createReq.numOfCols = res.schemaRes.numOfCols;
    pStmt->createReq.pSchema = res.schemaRes.pSchema;
    tstrncpy(pStmt->createReq.name, pStmt->viewName, TSDB_VIEW_NAME_LEN);
    tstrncpy(pStmt->createReq.dbFName, dbFName, sizeof(pStmt->createReq.dbFName));
    snprintf(pStmt->createReq.fullname, sizeof(pStmt->createReq.fullname) - 1, "%s.%s", pStmt->createReq.dbFName,
             pStmt->viewName);
    TSWAP(pStmt->createReq.querySql, pStmt->pQuerySql);
    pStmt->createReq.orReplace = pStmt->orReplace;
    pStmt->createReq.sql = tstrdup(pCxt->pParseCxt->pSql);
    if (NULL == pStmt->createReq.sql) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_VIEW, (FSerializeFunc)tSerializeSCMCreateViewReq, &pStmt->createReq);
  }

  tFreeSCMCreateViewReq(&pStmt->createReq);
  return code;
}

static int32_t translateDropView(STranslateContext* pCxt, SDropViewStmt* pStmt) {
#ifndef TD_ENTERPRISE
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif

  SCMDropViewReq dropReq = {0};
  SName          name = {0};
  int32_t        code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS == code) {
    (void)tNameGetFullDbName(&name, dropReq.dbFName);
    tstrncpy(dropReq.name, pStmt->viewName, TSDB_VIEW_NAME_LEN);
    snprintf(dropReq.fullname, sizeof(dropReq.fullname) - 1, "%s.%s", dropReq.dbFName, dropReq.name);
    dropReq.sql = (char*)pCxt->pParseCxt->pSql;
    if (NULL == dropReq.sql) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    dropReq.igNotExists = pStmt->ignoreNotExists;
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->viewName, &name);
    code = collectUseTable(&name, pCxt->pTargetTables);
  }

  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  return buildCmdMsg(pCxt, TDMT_MND_DROP_VIEW, (FSerializeFunc)tSerializeSCMDropViewReq, &dropReq);
}

static int32_t readFromFile(char* pName, int32_t* len, char** buf) {
  int64_t filesize = 0;
  if (taosStatFile(pName, &filesize, NULL, NULL) < 0) {
    return terrno;
  }

  *len = filesize;

  if (*len <= 0) {
    return TSDB_CODE_TSC_FILE_EMPTY;
  }

  *buf = taosMemoryCalloc(1, *len);
  if (*buf == NULL) {
    return terrno;
  }

  TdFilePtr tfile = taosOpenFile(pName, O_RDONLY | O_BINARY);
  if (NULL == tfile) {
    taosMemoryFreeClear(*buf);
    return terrno;
  }

  int64_t s = taosReadFile(tfile, *buf, *len);
  if (s != *len) {
    int32_t code = taosCloseFile(&tfile);
    parserError("failed to close file: %s in %s:%d, err: %s", pName, __func__, __LINE__, tstrerror(code));
    taosMemoryFreeClear(*buf);
    return TSDB_CODE_APP_ERROR;
  }
  return taosCloseFile(&tfile);
}

static int32_t translateCreateFunction(STranslateContext* pCxt, SCreateFunctionStmt* pStmt) {
  if (fmIsBuiltinFunc(pStmt->funcName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FUNCTION_NAME);
  }

  if (TSDB_DATA_TYPE_JSON == pStmt->outputDt.type || TSDB_DATA_TYPE_VARBINARY == pStmt->outputDt.type ||
      TSDB_DATA_TYPE_DECIMAL == pStmt->outputDt.type || TSDB_DATA_TYPE_BLOB == pStmt->outputDt.type ||
      TSDB_DATA_TYPE_MEDIUMBLOB == pStmt->outputDt.type) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Unsupported output type for UDF");
  }

  if (!pStmt->isAgg && pStmt->bufSize > 0) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "BUFSIZE can only be used with UDAF");
  }

  SCreateFuncReq req = {0};
  tstrncpy(req.name, pStmt->funcName, TSDB_FUNC_NAME_LEN);
  req.orReplace = pStmt->orReplace;
  req.igExists = pStmt->ignoreExists;
  req.funcType = pStmt->isAgg ? TSDB_FUNC_TYPE_AGGREGATE : TSDB_FUNC_TYPE_SCALAR;
  req.scriptType = pStmt->language;
  req.outputType = pStmt->outputDt.type;
  pStmt->outputDt.bytes = calcTypeBytes(pStmt->outputDt);
  req.outputLen = pStmt->outputDt.bytes;
  req.bufSize = pStmt->bufSize;
  int32_t code = readFromFile(pStmt->libraryPath, &req.codeLen, &req.pCode);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_FUNC, (FSerializeFunc)tSerializeSCreateFuncReq, &req);
  }
  tFreeSCreateFuncReq(&req);
  return code;
}

static int32_t translateDropFunction(STranslateContext* pCxt, SDropFunctionStmt* pStmt) {
  SDropFuncReq req = {0};
  tstrncpy(req.name, pStmt->funcName, TSDB_FUNC_NAME_LEN);
  req.igNotExists = pStmt->ignoreNotExists;
  return buildCmdMsg(pCxt, TDMT_MND_DROP_FUNC, (FSerializeFunc)tSerializeSDropFuncReq, &req);
}

static int32_t createRealTableForGrantTable(SGrantStmt* pStmt, SRealTableNode** pTable) {
  SRealTableNode* pRealTable = NULL;
  int32_t         code = nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)&pRealTable);
  if (NULL == pRealTable) {
    return code;
  }
  tstrncpy(pRealTable->table.dbName, pStmt->objName, TSDB_DB_NAME_LEN);
  tstrncpy(pRealTable->table.tableName, pStmt->tabName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pRealTable->table.tableAlias, pStmt->tabName, TSDB_TABLE_NAME_LEN);
  *pTable = pRealTable;
  return TSDB_CODE_SUCCESS;
}

static int32_t translateGrantTagCond(STranslateContext* pCxt, SGrantStmt* pStmt, SAlterUserReq* pReq) {
  SRealTableNode* pTable = NULL;
  if ('\0' == pStmt->tabName[0] || '*' == pStmt->tabName[0]) {
    if (pStmt->pTagCond) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                     "The With clause can only be used for table level privilege");
    } else {
      return TSDB_CODE_SUCCESS;
    }
  }

  int32_t code = createRealTableForGrantTable(pStmt, &pTable);
  if (TSDB_CODE_SUCCESS == code) {
    SName name = {0};
    toName(pCxt->pParseCxt->acctId, pTable->table.dbName, pTable->table.tableName, &name);
    code = getTargetMeta(pCxt, &name, &(pTable->pMeta), false);
    if (code) {
      nodesDestroyNode((SNode*)pTable);
      return code;
    }

    if (TSDB_SUPER_TABLE != pTable->pMeta->tableType && TSDB_NORMAL_TABLE != pTable->pMeta->tableType &&
        TSDB_VIRTUAL_NORMAL_TABLE != pTable->pMeta->tableType) {
      nodesDestroyNode((SNode*)pTable);
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                     "Only supertable and normal table can be granted");
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL == pStmt->pTagCond) {
    nodesDestroyNode((SNode*)pTable);
    return TSDB_CODE_SUCCESS;
  }

  pCxt->pCurrStmt = (SNode*)pStmt;

  if (TSDB_CODE_SUCCESS == code) {
    code = addNamespace(pCxt, pTable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExpr(pCxt, &pStmt->pTagCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesNodeToString(pStmt->pTagCond, false, &pReq->tagCond, &pReq->tagCondLen);
  }
  nodesDestroyNode((SNode*)pTable);
  return code;
}

static int32_t translateGrant(STranslateContext* pCxt, SGrantStmt* pStmt) {
  int32_t       code = 0;
  SAlterUserReq req = {0};
  req.alterType = TSDB_ALTER_USER_ADD_PRIVILEGES;
  req.privileges = pStmt->privileges;
#ifdef TD_ENTERPRISE
  if (0 != pStmt->tabName[0]) {
    SName       name = {0};
    STableMeta* pTableMeta = NULL;
    toName(pCxt->pParseCxt->acctId, pStmt->objName, pStmt->tabName, &name);
    code = getTargetMeta(pCxt, &name, &pTableMeta, true);
    if (TSDB_CODE_SUCCESS != code) {
      if (TSDB_CODE_PAR_TABLE_NOT_EXIST != code) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GET_META_ERROR, tstrerror(code));
      }
    } else if (TSDB_VIEW_TABLE == pTableMeta->tableType) {
      req.isView = true;
    }
    taosMemoryFree(pTableMeta);
  }
#endif

  tstrncpy(req.user, pStmt->userName, TSDB_USER_LEN);
  snprintf(req.objname, TSDB_DB_FNAME_LEN, "%d.%s", pCxt->pParseCxt->acctId, pStmt->objName);
  snprintf(req.tabName, TSDB_TABLE_NAME_LEN, "%s", pStmt->tabName);
  if (!req.isView) {
    code = translateGrantTagCond(pCxt, pStmt, &req);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_ALTER_USER, (FSerializeFunc)tSerializeSAlterUserReq, &req);
  }
  tFreeSAlterUserReq(&req);
  return code;
}

static int32_t translateRevoke(STranslateContext* pCxt, SRevokeStmt* pStmt) {
  int32_t       code = 0;
  SAlterUserReq req = {0};
  req.alterType = TSDB_ALTER_USER_DEL_PRIVILEGES;
  req.privileges = pStmt->privileges;

#ifdef TD_ENTERPRISE
  if (0 != pStmt->tabName[0]) {
    SName       name = {0};
    STableMeta* pTableMeta = NULL;
    toName(pCxt->pParseCxt->acctId, pStmt->objName, pStmt->tabName, &name);
    code = getTargetMeta(pCxt, &name, &pTableMeta, true);
    if (TSDB_CODE_SUCCESS != code) {
      if (TSDB_CODE_PAR_TABLE_NOT_EXIST != code) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GET_META_ERROR, tstrerror(code));
      }
    } else if (TSDB_VIEW_TABLE == pTableMeta->tableType) {
      req.isView = true;
    }
    taosMemoryFree(pTableMeta);
  }
#endif

  tstrncpy(req.user, pStmt->userName, TSDB_USER_LEN);
  snprintf(req.objname, TSDB_DB_FNAME_LEN, "%d.%s", pCxt->pParseCxt->acctId, pStmt->objName);
  snprintf(req.tabName, TSDB_TABLE_NAME_LEN, "%s", pStmt->tabName);
  code = buildCmdMsg(pCxt, TDMT_MND_ALTER_USER, (FSerializeFunc)tSerializeSAlterUserReq, &req);
  tFreeSAlterUserReq(&req);
  return code;
}

static int32_t translateBalanceVgroup(STranslateContext* pCxt, SBalanceVgroupStmt* pStmt) {
  SBalanceVgroupReq req = {0};
  int32_t code = buildCmdMsg(pCxt, TDMT_MND_BALANCE_VGROUP, (FSerializeFunc)tSerializeSBalanceVgroupReq, &req);
  tFreeSBalanceVgroupReq(&req);
  return code;
}

static int32_t translateAssignLeader(STranslateContext* pCxt, SAssignLeaderStmt* pStmt) {
  SAssignLeaderReq req = {0};
  int32_t code = buildCmdMsg(pCxt, TDMT_MND_ARB_ASSIGN_LEADER, (FSerializeFunc)tSerializeSAssignLeaderReq, &req);
  tFreeSAssignLeaderReq(&req);
  return code;
}

static int32_t translateBalanceVgroupLeader(STranslateContext* pCxt, SBalanceVgroupLeaderStmt* pStmt) {
  SBalanceVgroupLeaderReq req = {0};
  req.vgId = pStmt->vgId;
  tstrncpy(req.db, pStmt->dbName, TSDB_DB_FNAME_LEN);
  int32_t code =
      buildCmdMsg(pCxt, TDMT_MND_BALANCE_VGROUP_LEADER, (FSerializeFunc)tSerializeSBalanceVgroupLeaderReq, &req);
  tFreeSBalanceVgroupLeaderReq(&req);
  return code;
}

static int32_t translateSetVgroupKeepVersion(STranslateContext* pCxt, SSetVgroupKeepVersionStmt* pStmt) {
  SMndSetVgroupKeepVersionReq req = {0};
  req.vgId = pStmt->vgId;
  req.keepVersion = pStmt->keepVersion;
  int32_t code =
      buildCmdMsg(pCxt, TDMT_MND_SET_VGROUP_KEEP_VERSION, (FSerializeFunc)tSerializeSMndSetVgroupKeepVersionReq, &req);
  return code;
}

static int32_t translateMergeVgroup(STranslateContext* pCxt, SMergeVgroupStmt* pStmt) {
  SMergeVgroupReq req = {.vgId1 = pStmt->vgId1, .vgId2 = pStmt->vgId2};
  return buildCmdMsg(pCxt, TDMT_MND_MERGE_VGROUP, (FSerializeFunc)tSerializeSMergeVgroupReq, &req);
}

static int32_t checkDnodeIds(STranslateContext* pCxt, SRedistributeVgroupStmt* pStmt) {
  int32_t numOfDnodes = LIST_LENGTH(pStmt->pDnodes);
  if (numOfDnodes > 3 || numOfDnodes < 1) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_REDISTRIBUTE_VG);
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pStmt->pDnodes) {
    SValueNode* pVal = (SValueNode*)pNode;
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
  }

  pStmt->dnodeId1 = getBigintFromValueNode((SValueNode*)nodesListGetNode(pStmt->pDnodes, 0));
  pStmt->dnodeId2 = -1;
  pStmt->dnodeId3 = -1;
  if (numOfDnodes > 1) {
    pStmt->dnodeId2 = getBigintFromValueNode((SValueNode*)nodesListGetNode(pStmt->pDnodes, 1));
  }
  if (numOfDnodes > 2) {
    pStmt->dnodeId3 = getBigintFromValueNode((SValueNode*)nodesListGetNode(pStmt->pDnodes, 2));
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateRedistributeVgroup(STranslateContext* pCxt, SRedistributeVgroupStmt* pStmt) {
  SRedistributeVgroupReq req = {.vgId = pStmt->vgId};
  int32_t                code = checkDnodeIds(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    req.dnodeId1 = pStmt->dnodeId1;
    req.dnodeId2 = pStmt->dnodeId2;
    req.dnodeId3 = pStmt->dnodeId3;
    code = buildCmdMsg(pCxt, TDMT_MND_REDISTRIBUTE_VGROUP, (FSerializeFunc)tSerializeSRedistributeVgroupReq, &req);
  }
  tFreeSRedistributeVgroupReq(&req);
  return code;
}

static int32_t translateSplitVgroup(STranslateContext* pCxt, SSplitVgroupStmt* pStmt) {
  SSplitVgroupReq req = {.vgId = pStmt->vgId, .force = pStmt->force};
  return buildCmdMsg(pCxt, TDMT_MND_SPLIT_VGROUP, (FSerializeFunc)tSerializeSSplitVgroupReq, &req);
}

static int32_t translateShowVariables(STranslateContext* pCxt, SShowStmt* pStmt) {
  SShowVariablesReq req = {0};
  req.opType = pStmt->tableCondType;
  if (req.opType == OP_TYPE_LIKE && pStmt->pTbName) {
    req.valLen = strlen(((SValueNode*)pStmt->pTbName)->literal);
    if (req.valLen > 0) {
      req.val = taosStrdupi(((SValueNode*)pStmt->pTbName)->literal);
    }
  }
  int32_t code = buildCmdMsg(pCxt, TDMT_MND_SHOW_VARIABLES, (FSerializeFunc)tSerializeSShowVariablesReq, &req);
  tFreeSShowVariablesReq(&req);
  return code;
}

static int32_t translateShowCreateDatabase(STranslateContext* pCxt, SShowCreateDatabaseStmt* pStmt) {
  pStmt->pCfg = taosMemoryCalloc(1, sizeof(SDbCfgInfo));
  if (NULL == pStmt->pCfg) {
    return terrno;
  }

  SName   name;
  int32_t code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  (void)tNameGetFullDbName(&name, pStmt->dbFName);
  if (TSDB_CODE_SUCCESS == code) return getDBCfg(pCxt, pStmt->dbName, (SDbCfgInfo*)pStmt->pCfg);
  return code;
}

static int32_t translateShowCreateTable(STranslateContext* pCxt, SShowCreateTableStmt* pStmt, bool showVtable) {
  pStmt->pDbCfg = taosMemoryCalloc(1, sizeof(SDbCfgInfo));
  if (NULL == pStmt->pDbCfg) {
    return terrno;
  }
  int32_t code = TSDB_CODE_SUCCESS;
  PAR_ERR_RET(getDBCfg(pCxt, pStmt->dbName, (SDbCfgInfo*)pStmt->pDbCfg));
  SName name = {0};
  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);
  PAR_ERR_RET(getTableCfg(pCxt, &name, (STableCfg**)&pStmt->pTableCfg));

  bool isVtb = (((STableCfg*)pStmt->pTableCfg)->tableType == TSDB_VIRTUAL_CHILD_TABLE ||
                ((STableCfg*)pStmt->pTableCfg)->tableType == TSDB_VIRTUAL_NORMAL_TABLE ||
                ((STableCfg*)pStmt->pTableCfg)->virtualStb);
  if (!isVtb && showVtable) {
    PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_TYPE,
                                        "table type is normal table, but use show create virtual table"));

  }

  return code;
}

static int32_t translateShowCreateVTable(STranslateContext* pCxt, SShowCreateTableStmt* pStmt) {
  return translateShowCreateTable(pCxt, pStmt, true);
}

static int32_t translateShowCreateView(STranslateContext* pCxt, SShowCreateViewStmt* pStmt) {
#ifndef TD_ENTERPRISE
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  SName name = {0};
  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->viewName, &name);
  return getViewMetaFromMetaCache(pCxt, &name, (SViewMeta**)&pStmt->pViewMeta);
#endif
}

#ifdef TD_ENTERPRISE
static int32_t getRsma(STranslateContext* pCxt, const char* name, SRsmaInfoRsp** pInfo) {
  int32_t          code = 0;
  SParseContext*   pParCxt = pCxt->pParseCxt;
  SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                           .requestId = pParCxt->requestId,
                           .requestObjRefId = pParCxt->requestRid,
                           .mgmtEps = pParCxt->mgmtEpSet};
  code = catalogGetRsma(pParCxt->pCatalog, &conn, name, pInfo);
  if (code) {
    parserError("QID:0x%" PRIx64 ", get rsma %s error, code:%s", pCxt->pParseCxt->requestId, name, tstrerror(code));
  }
  return code;
}
#endif

static int32_t translateShowCreateRsma(STranslateContext* pCxt, SShowCreateRsmaStmt* pStmt) {
#ifdef TD_ENTERPRISE
  int32_t code = 0, lino = 0;
  pStmt->pRsmaMeta = NULL;
  TAOS_CHECK_EXIT(getRsma(pCxt, pStmt->rsmaName, (SRsmaInfoRsp**)&pStmt->pRsmaMeta));
_exit:
  return code;
#else
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

static int32_t createColumnNodeWithName(const char* name, SNode** ppCol) {
  SColumnNode* pCol = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (!pCol) return code;
  tstrncpy(pCol->colName, name, TSDB_COL_NAME_LEN);
  tstrncpy(pCol->node.aliasName, name, TSDB_COL_NAME_LEN);
  tstrncpy(pCol->node.userAlias, name, TSDB_COL_NAME_LEN);
  *ppCol = (SNode*)pCol;
  return code;
}

static int32_t compareTsmaColWithColId(SNode* pNode1, SNode* pNode2) {
  SColumnNode* pCol1 = (SColumnNode*)pNode1;
  SColumnNode* pCol2 = (SColumnNode*)pNode2;
  if (pCol1->colId < pCol2->colId)
    return -1;
  else if (pCol1->colId > pCol2->colId)
    return 1;
  else
    return 0;
}

static int32_t compareTsmaFuncWithFuncAndColId(SNode* pNode1, SNode* pNode2) {
  SFunctionNode* pFunc1 = (SFunctionNode*)pNode1;
  SFunctionNode* pFunc2 = (SFunctionNode*)pNode2;
  if (pFunc1->funcId < pFunc2->funcId)
    return -1;
  else if (pFunc1->funcId > pFunc2->funcId)
    return 1;
  else {
    SNode* pCol1 = pFunc1->pParameterList->pHead->pNode;
    SNode* pCol2 = pFunc2->pParameterList->pHead->pNode;
    return compareTsmaColWithColId(pCol1, pCol2);
  }
}

// pFuncs are already sorted by funcId and colId
static void deduplicateTsmaFuncs(SNodeList* pFuncs) {
  SNode*     pLast = NULL;
  SNode*     pFunc = NULL;
  SNodeList* pRes = NULL;
  FOREACH(pFunc, pFuncs) {
    if (pLast) {
      if (compareTsmaFuncWithFuncAndColId(pLast, pFunc) == 0) {
        ERASE_NODE(pFuncs);
        continue;
      } else {
        pLast = pFunc;
      }
    } else {
      pLast = pFunc;
    }
  }
}

static int32_t buildCreateTsmaReqBuildStreamSubTableName(SCreateTSMAStmt* pStmt, SMCreateSmaReq* pReq, int32_t numOfTags,
                                                     const SSchema* pTags, SNode** pSubTable) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SFunctionNode* pMd5Func = NULL;
  SFunctionNode* pConcatFunc = NULL;
  SValueNode*    pTsmaName = NULL;
  SValueNode*    pDash = NULL;
  SNode*         pTbname = NULL;

  if (pStmt->pOptions->recursiveTsma) {
    if (pTags && numOfTags > 0) {
      code = createColumnNodeWithName(pTags[numOfTags - 1].name, &pTbname);
      QUERY_CHECK_CODE(code, lino, _return);
    }
  } else {
    code = createTbnameFunction((SFunctionNode**)&pTbname);
    QUERY_CHECK_CODE(code, lino, _return);
    snprintf(((SFunctionNode*)pTbname)->node.userAlias, TSDB_COL_NAME_LEN, "tbname");
  }

  code = nodesMakeValueNodeFromString(pReq->name, &pTsmaName);
  QUERY_CHECK_CODE(code, lino, _return);
  code = nodesMakeValueNodeFromString("_", &pDash);
  QUERY_CHECK_CODE(code, lino, _return);

  // not recursive tsma, md5(concat('1.test.tsma1_', tbname))
  // recursive tsma, md5(concat('1.test.tsma1_', `tbname`)), `tbname` is the last tag

  code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pConcatFunc);
  QUERY_CHECK_CODE(code, lino, _return);
  snprintf(pConcatFunc->functionName, TSDB_FUNC_NAME_LEN, "%s", "concat");

  code = nodesListMakeStrictAppend(&pConcatFunc->pParameterList, (SNode*)pTsmaName);
  QUERY_CHECK_CODE(code, lino, _return);
  code = nodesListMakeStrictAppend(&pConcatFunc->pParameterList, (SNode*)pDash);
  QUERY_CHECK_CODE(code, lino, _return1);
  code = nodesListMakeStrictAppend(&pConcatFunc->pParameterList, (SNode*)pTbname);
  QUERY_CHECK_CODE(code, lino, _return2);


  code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pMd5Func);
  QUERY_CHECK_CODE(code, lino, _return3);
  snprintf(pMd5Func->functionName, TSDB_FUNC_NAME_LEN, "%s", "md5");
  code = nodesListMakeAppend(&pMd5Func->pParameterList, (SNode*)pConcatFunc);
  QUERY_CHECK_CODE(code, lino, _return4);

  *pSubTable = (SNode*)pMd5Func;
  return code;

_return:
  nodesDestroyNode((SNode*)pTsmaName);
_return1:
  nodesDestroyNode((SNode*)pDash);
_return2:
  nodesDestroyNode((SNode*)pTbname);
_return3:
  nodesDestroyNode((SNode*)pConcatFunc);
_return4:
  nodesDestroyNode((SNode*)pMd5Func);
  parserError("%s error, code:%s, line:%d",  __FUNCTION__ , tstrerror(code), lino);
  return code;
}

static int32_t buildProjectsForTSMAQuery(SNodeList* pFunc) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    pProjectionTotalLen = 0;
  SNodeList* pProjectionList = pFunc;

  PAR_ERR_RET(addTWstartToSampleProjects(pProjectionList));
  PAR_ERR_RET(addTWendToSampleProjects(pProjectionList));
  PAR_ERR_RET(addTWdurationToSampleProjects(pProjectionList));

  SNode* pProject = NULL;
  FOREACH(pProject, pProjectionList) {
    snprintf(((SExprNode*)pProject)->aliasName, TSDB_COL_NAME_LEN, "#%p", pProject);
    pProjectionTotalLen += ((SExprNode*)pProject)->resType.bytes;
  }

  if (pProjectionTotalLen > TSDB_MAX_BYTES_PER_ROW) {
    PAR_ERR_RET(TSDB_CODE_PAR_INVALID_ROW_LENGTH);
  }

  return code;
}

static int32_t buildTriggerWindowForCreateStream(SNode *pInt, SNode** pOutput) {
  SIntervalWindowNode* pInterval = NULL;
  int32_t              code = TSDB_CODE_SUCCESS;

  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW, (SNode**)&pInterval));
  PAR_ERR_JRET(nodesCloneNode(pInt, &pInterval->pInterval));
  PAR_ERR_JRET(nodesCloneNode(pInt, &pInterval->pSliding));
  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pInterval->pCol));

  ((SColumnNode*)pInterval->pCol)->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  tstrncpy(((SColumnNode*)pInterval->pCol)->colName, ROWTS_PSEUDO_COLUMN_NAME, TSDB_COL_NAME_LEN);
  *pOutput = (SNode*)pInterval;
  return code;
_return:
  nodesDestroyNode((SNode*)pInterval);
  return code;
}

static int32_t buildCreateTSMAReqBuildStreamQueryCondition(STranslateContext* pCxt, SCreateTSMAStmt* pStmt, int32_t numOfTags, SNode** pCondition) {

  int32_t        code = TSDB_CODE_SUCCESS;
  SNode*         pCond = NULL;

  SNode*         tsStartCond = NULL;
  SNode*         tsEndCond = NULL;
  SNode*         tbnameCond = NULL;
  SFunctionNode* twstartFunc = NULL;
  SFunctionNode* twendFunc = NULL;
  SFunctionNode* tbnameFunc = NULL;
  SFunctionNode* phTbFunc = NULL;
  SFunctionNode* phColFunc = NULL;
  SNode*         pColId = NULL;

  bool           freeColId = false;
  bool           freePartCond = true;

  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&twstartFunc));
  tstrncpy(twstartFunc->functionName, "_twstart", TSDB_FUNC_NAME_LEN);
  tstrncpy(twstartFunc->node.userAlias, "_twstart", TSDB_FUNC_NAME_LEN);

  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&twendFunc));
  tstrncpy(twendFunc->functionName, "_twend", TSDB_FUNC_NAME_LEN);
  tstrncpy(twendFunc->node.userAlias, "_twend", TSDB_FUNC_NAME_LEN);

  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&tbnameFunc));
  tstrncpy(tbnameFunc->functionName, "tbname", TSDB_FUNC_NAME_LEN);
  tstrncpy(tbnameFunc->node.userAlias, "tbname", TSDB_FUNC_NAME_LEN);
  tstrncpy(tbnameFunc->node.aliasName, "tbname", TSDB_FUNC_NAME_LEN);

  if (pStmt->pOptions->recursiveTsma) {
    PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&phColFunc));
    tstrncpy(phColFunc->functionName, "_placeholder_column", TSDB_FUNC_NAME_LEN);

    PAR_ERR_JRET(nodesMakeValueNodeFromInt32(numOfTags, &pColId));
    ((SValueNode*)pColId)->notReserved = true;
    freeColId = true;

    PAR_ERR_JRET(nodesListMakeAppend(&phColFunc->pParameterList, pColId));
    freeColId = false;
    snprintf(phColFunc->node.userAlias, sizeof(phColFunc->node.userAlias), "%%%%%s", ((SValueNode*)pColId)->literal);

    // where tbname == %%n
    PAR_ERR_JRET(createOperatorNode(OP_TYPE_EQUAL, "tbname", (SNode*)phColFunc, &tbnameCond));
  } else {
    PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&phTbFunc));
    tstrncpy(phTbFunc->functionName, "_placeholder_tbname", TSDB_FUNC_NAME_LEN);
    tstrncpy(phTbFunc->node.userAlias, "%%tbname", TSDB_COL_NAME_LEN);

    // where tbname == %%tbname
    PAR_ERR_JRET(createOperatorNodeByNode(OP_TYPE_EQUAL, (SNode*)phTbFunc, (SNode*)tbnameFunc, &tbnameCond));
  }

  // ts >= _twstart
  PAR_ERR_JRET(createTsOperatorNode(OP_TYPE_GREATER_EQUAL, (SNode*)twstartFunc, &tsStartCond));
  // ts < _twend
  PAR_ERR_JRET(createTsOperatorNode(OP_TYPE_LOWER_THAN, (SNode*)twendFunc, &tsEndCond));

  PAR_ERR_RET(nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pCond));
  ((SLogicConditionNode*)pCond)->condType = LOGIC_COND_TYPE_AND;
  // ts >= _twstart && ts < _twend && tbname == %%tbname for TSMA
  // ts >= _twstart && ts < _twend && tbname == %%n for recursive TSMA
  PAR_ERR_JRET(nodesListMakeAppend(&((SLogicConditionNode*)pCond)->pParameterList, tsStartCond));
  PAR_ERR_JRET(nodesListMakeAppend(&((SLogicConditionNode*)pCond)->pParameterList, tsEndCond));
  PAR_ERR_JRET(nodesListMakeAppend(&((SLogicConditionNode*)pCond)->pParameterList, tbnameCond));
  freePartCond = false;

  *pCondition = pCond;
  return code;
_return:
  parserError("buildCreateTSMAReqBuildStreamQueryCondition failed, code:%d", code);

  if (freeColId) {
    nodesDestroyNode(pColId);
  }
  if (freePartCond) {
    nodesDestroyNode(tsStartCond);
    nodesDestroyNode(tsEndCond);
    nodesDestroyNode(tbnameCond);
  } else {
    nodesDestroyNode(pCond);
  }
  nodesDestroyNode((SNode*)twstartFunc);
  nodesDestroyNode((SNode*)twendFunc);
  nodesDestroyNode((SNode*)tbnameFunc);
  nodesDestroyNode((SNode*)phTbFunc);
  nodesDestroyNode((SNode*)phColFunc);

  return code;
}

static int32_t buildTriggerOptionForCreateStream(SStreamTriggerOptions** ppOptions) {
  int32_t                code = TSDB_CODE_SUCCESS;
  SStreamTriggerOptions* pOptions = NULL;

  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_STREAM_TRIGGER_OPTIONS, (SNode**)&pOptions));
  pOptions->fillHistoryFirst = true;
  pOptions->deleteOutputTable = true;
  pOptions->deleteRecalc = true;
  PAR_ERR_JRET(nodesMakeDurationValueNodeFromString("3s", (SValueNode**)&pOptions->pMaxDelay));

  *ppOptions = pOptions;
  return code;
_return:
  nodesDestroyNode((SNode*)pOptions);
  parserError("%s failed, code:%d", __FUNCTION__ , code);
  return code;
}

static int32_t buildTriggerPartitionForCreateStream(SNodeList** ppTriggerPartition, SCreateTSMAStmt* pStmt, int32_t numOfTags, const SSchema* pTags) {
  int32_t                code = TSDB_CODE_SUCCESS;
  SNodeList*             pTriggerPartition = NULL;
  SNode*                 pTagCol = NULL;
  SNode*                 tbnameFunc = NULL;

  // append partition by tags
  for (int32_t idx = 0; idx < numOfTags; ++idx) {
    PAR_ERR_JRET(createColumnNodeWithName(pTags[idx].name, &pTagCol));
    PAR_ERR_JRET(nodesListMakeStrictAppend(&pTriggerPartition, pTagCol));
    pTagCol = NULL;
  }

  if (!pStmt->pOptions->recursiveTsma) {
    PAR_ERR_JRET(createTbnameFunction((SFunctionNode**)&tbnameFunc));
    snprintf(((SFunctionNode*)tbnameFunc)->node.userAlias, TSDB_COL_NAME_LEN, "tbname");
    PAR_ERR_JRET(nodesListMakeStrictAppend(&pTriggerPartition, (SNode*)tbnameFunc));
    tbnameFunc = NULL;
  }

  *ppTriggerPartition = pTriggerPartition;
  return code;
_return:
  parserError("%s failed code:%d", __FUNCTION__ , code);
  nodesDestroyNode(tbnameFunc);
  nodesDestroyNode(pTagCol);
  nodesDestroyList(pTriggerPartition);
  return code;
}

static int32_t buildCreateTSMAReqBuildStreamTriggerAndTag(STranslateContext* pCxt, SCreateTSMAStmt* pStmt, SMCreateSmaReq* pReq, const char* tbName,
                                                    int32_t numOfTags, const SSchema* pTags, SStreamTriggerNode** ppTrigger) {
  int32_t                code = TSDB_CODE_SUCCESS;
  SStreamTriggerNode*    pTrigger = NULL;
  SRealTableNode*        pTriggerTable = NULL;
  SIntervalWindowNode*   pInterval = NULL;
  SStreamTriggerOptions* pOptions = NULL;
  SNodeList*             pTriggerPartition = NULL;

  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)&pTriggerTable));
  tstrncpy(pTriggerTable->table.dbName, pStmt->dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pTriggerTable->table.tableName, tbName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pTriggerTable->table.tableAlias, tbName, TSDB_TABLE_NAME_LEN);

  PAR_ERR_JRET(buildTriggerWindowForCreateStream(pStmt->pOptions->pInterval, (SNode**)&pInterval));
  PAR_ERR_JRET(buildTriggerOptionForCreateStream(&pOptions));
  PAR_ERR_JRET(buildTriggerPartitionForCreateStream(&pTriggerPartition, pStmt, numOfTags, pTags));
  
  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_STREAM_TRIGGER, (SNode**)&pTrigger));
  pTrigger->pPartitionList = pTriggerPartition;
  pTrigger->pTriggerWindow = (SNode*)pInterval;
  pTrigger->pTrigerTable = (SNode*)pTriggerTable;
  pTrigger->pOptions = (SNode*)pOptions;

  *ppTrigger = pTrigger;
  return code;
  
_return:
  parserError("%s failed, code:%d", __FUNCTION__ , code);
  if (pTrigger) {
    nodesDestroyNode((SNode*)pTrigger);
  } else {
    nodesDestroyNode((SNode*)pTriggerTable);
    nodesDestroyNode((SNode*)pInterval);
    nodesDestroyNode((SNode*)pOptions);
    nodesDestroyList(pTriggerPartition);
  }
  return code;
}

static int32_t buildCreateTSMAReqBuildStreamQuery(STranslateContext* pCxt, SStreamTriggerNode* pTrigger,
                                                  SCreateTSMAStmt* pStmt, int32_t numOfTags, const SSchema* pTags,
                                                  SSelectStmt **ppStreamQuery) {
  int32_t            code = TSDB_CODE_SUCCESS;
  SNode*             pFromTable = NULL;
  SNodeList*         pFuncList = NULL;
  SSelectStmt*       pStreamQuery = NULL;

  PAR_ERR_JRET(nodesCloneNode((SNode*)pTrigger->pTrigerTable, (SNode**)&pFromTable));

  PAR_ERR_JRET(nodesCloneList(pStmt->pOptions->pFuncs, &pFuncList));
  if (!pStmt->pOptions->recursiveTsma) {
    PAR_ERR_JRET(fmCreateStateFuncs(pFuncList));
  }

  PAR_ERR_JRET(buildProjectsForTSMAQuery(pFuncList));

  PAR_ERR_JRET(createSelectStmtImpl(false, pFuncList, (SNode*)pFromTable, NULL, (SNode**)&pStreamQuery));
  PAR_ERR_JRET(buildCreateTSMAReqBuildStreamQueryCondition(pCxt, pStmt, numOfTags, &pStreamQuery->pWhere));

  *ppStreamQuery = pStreamQuery;
  return code;

_return:
  parserError("%s failed, code:%d", __FUNCTION__ , code);
  if (pStreamQuery) {
    nodesDestroyNode((SNode*)pStreamQuery);
  } else {
    nodesDestroyList(pFuncList);
    nodesDestroyNode(pFromTable);
  }
  return code;
}

static int32_t buildCreateTSMAReqBuildStreamOutTags(SStreamTriggerNode* pTrigger, SCreateTSMAStmt* pStmt,
                                                    int32_t numOfTags, const SSchema* pTags, SNodeList** ppTagList) {
  int32_t            code = TSDB_CODE_SUCCESS;
  SNodeList*         pTagList = NULL;
  SStreamTagDefNode* pTagDef = NULL;
  SNode*             pNode = NULL;
  int32_t            idx = 0;

  FOREACH(pNode, pTrigger->pPartitionList) {
    PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_STREAM_TAG_DEF, (SNode**)&pTagDef));
    PAR_ERR_JRET(nodesCloneNode(pNode, (SNode**)&pTagDef->pTagExpr));
    PAR_ERR_JRET(nodesListMakeStrictAppend(&pTagList, (SNode*)pTagDef));
    if (!pStmt->pOptions->recursiveTsma && idx == numOfTags) {
      tstrncpy(pTagDef->tagName, "tbname", TSDB_COL_NAME_LEN);
      pTagDef->dataType.type = TSDB_DATA_TYPE_VARCHAR;
      pTagDef->dataType.bytes = TSDB_TABLE_FNAME_LEN - 1;
      break;
    } else if (pStmt->pOptions->recursiveTsma && idx == numOfTags - 1){
      tstrncpy(pTagDef->tagName, "tbname", TSDB_COL_NAME_LEN);
      pTagDef->dataType.type = TSDB_DATA_TYPE_VARCHAR;
      pTagDef->dataType.bytes = pTags[idx].bytes - 2;
      break;
    } else {
      pTagDef->dataType.type = pTags[idx].type;
      pTagDef->dataType.bytes = pTags[idx].bytes;
      tstrncpy(pTagDef->tagName, pTags[idx].name, TSDB_COL_NAME_LEN);
    }
    idx++;
    pTagDef = NULL;
  }

  *ppTagList = pTagList;
  return code;

_return:
  parserError("%s failed, code:%d", __FUNCTION__ , code);
  nodesDestroyNode((SNode*)pTagDef);
  nodesDestroyList(pTagList);
  return code;
}

static int32_t buildCreateTSMAReqBuildCreateDropStreamReq(STranslateContext* pCxt, SCreateStreamStmt* pCreateStream, SMCreateSmaReq* pReq) {
  SCMCreateStreamReq createreq = {0};
  SMDropStreamReq    dropreq = {0};
  int32_t            code = TSDB_CODE_SUCCESS;

  PAR_ERR_JRET(checkCreateStream(pCxt, pCreateStream));

  ESqlClause currClause = pCxt->currClause;
  SNode*     pCurrStmt = pCxt->pCurrStmt;
  int32_t    currLevel = pCxt->currLevel;
  pCxt->currLevel = ++(pCxt->levelNo);

  PAR_ERR_JRET(buildCreateStreamReq(pCxt, pCreateStream, &createreq));

  pCxt->currClause = currClause;
  pCxt->pCurrStmt = pCurrStmt;
  pCxt->currLevel = currLevel;

  createreq.tsmaId = pReq->uid;
  pReq->streamReqLen = tSerializeSCMCreateStreamReq(NULL, 0, &createreq);
  pReq->createStreamReq = taosMemoryCalloc(1, pReq->streamReqLen);
  if (!pReq->createStreamReq) {
    PAR_ERR_JRET(terrno);
  }
  if (pReq->streamReqLen != tSerializeSCMCreateStreamReq(pReq->createStreamReq,
                                                         pReq->streamReqLen,
                                                         &createreq)) {
    PAR_ERR_JRET(TSDB_CODE_INVALID_MSG);
  }

  dropreq.name = taosStrdup(createreq.name);
  dropreq.igNotExists = false;

  pReq->dropStreamReqLen = tSerializeSMDropStreamReq(NULL, 0, &dropreq);
  pReq->dropStreamReq = taosMemoryCalloc(1, pReq->dropStreamReqLen);
  if (!pReq->dropStreamReq) {
    PAR_ERR_JRET(terrno);
  }
  if (pReq->dropStreamReqLen != tSerializeSMDropStreamReq(pReq->dropStreamReq,
                                                          pReq->dropStreamReqLen,
                                                          &dropreq)) {
    PAR_ERR_JRET(TSDB_CODE_INVALID_MSG);
  }

_return:
  if (code) {
    parserError("%s failed, code:%d", __FUNCTION__ , code);
  }
  tFreeSCMCreateStreamReq(&createreq);
  tFreeMDropStreamReq(&dropreq);

  return code;
}

static int32_t buildCreateTSMAReqBuildStreamReq(STranslateContext* pCxt, SCreateTSMAStmt* pStmt, SMCreateSmaReq* pReq, const char* tbName,
                            int32_t numOfTags, const SSchema* pTags) {
  int32_t            code = TSDB_CODE_SUCCESS;
  SSelectStmt*       pSelect = NULL;
  SSelectStmt*       pStreamQuery = NULL;
  SCreateStreamStmt* pCreateStream = NULL;
  SNode*             pSubTable = NULL;
  SStreamTriggerNode* pTrigger = NULL;
  SNodeList*          pTagList = NULL;

  PAR_ERR_JRET(buildCreateTSMAReqBuildStreamTriggerAndTag(pCxt, pStmt, pReq, tbName, numOfTags, pTags, &pTrigger));
  PAR_ERR_JRET(buildCreateTsmaReqBuildStreamSubTableName(pStmt, pReq, numOfTags, pTags, (SNode**)&pSubTable));
  PAR_ERR_JRET(buildCreateTSMAReqBuildStreamQuery(pCxt, pTrigger, pStmt, numOfTags, pTags, &pStreamQuery));
  PAR_ERR_JRET(buildCreateTSMAReqBuildStreamOutTags(pTrigger, pStmt, numOfTags, pTags, &pTagList));

  PAR_ERR_JRET(nodesCloneNode((SNode*)pStreamQuery, (SNode**)&pSelect));
  pCxt->streamInfo.triggerTbl = pTrigger->pTrigerTable;
  pCxt->streamInfo.calcClause = true;
  pCxt->streamInfo.triggerPartitionList = pTrigger->pPartitionList;
  PAR_ERR_JRET(translateQuery(pCxt, (SNode*)pSelect));
  pCxt->streamInfo.triggerPartitionList = NULL;
  pCxt->streamInfo.calcClause = false;
  pCxt->streamInfo.triggerTbl = NULL;
  PAR_ERR_JRET(nodesNodeToString((SNode*)pSelect, false, &pReq->ast, &pReq->astLen));
  PAR_ERR_JRET(nodesListToString(pSelect->pProjectionList, false, &pReq->expr, &pReq->exprLen));


  PAR_ERR_JRET(nodesMakeNode(QUERY_NODE_CREATE_STREAM_STMT, (SNode**)&pCreateStream));
  tstrncpy(pCreateStream->streamName, pStmt->tsmaName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pCreateStream->streamDbName, pStmt->dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pCreateStream->targetDbName, pStmt->dbName, TSDB_DB_NAME_LEN);
  snprintf(pCreateStream->targetTabName, TSDB_TABLE_NAME_LEN, "%s" TSMA_RES_STB_POSTFIX, pStmt->tsmaName);

  pCreateStream->pTags = pTagList;
  pCreateStream->pSubtable = pSubTable;
  pCreateStream->pTrigger = (SNode*)pTrigger;
  pCreateStream->pQuery = (SNode*)pStreamQuery;

  PAR_ERR_JRET(buildCreateTSMAReqBuildCreateDropStreamReq(pCxt, pCreateStream, pReq));

_return:
  if (code) {
    parserError("buildCreateTSMAReqBuildStreamReq failed, code:%d", code);
  }
  nodesDestroyNode((SNode*)pSelect);
  if (pCreateStream) {
    nodesDestroyNode((SNode*)pCreateStream);
  } else {
    nodesDestroyNode((SNode*)pSubTable);
    nodesDestroyNode((SNode*)pTrigger);
    nodesDestroyNode((SNode*)pStreamQuery);
    nodesDestroyList(pTagList);
  }
  return code;
}

static int32_t createColumnBySchema(const SSchema* pSchema, SColumnNode** ppCol) {
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)ppCol);
  if (!*ppCol) return code;

  (*ppCol)->colId = pSchema->colId;
  (*ppCol)->node.resType.type = pSchema->type;
  (*ppCol)->node.resType.bytes = pSchema->bytes;
  tstrncpy((*ppCol)->colName, pSchema->name, TSDB_COL_NAME_LEN);
  return TSDB_CODE_SUCCESS;
}

static int32_t buildCreateTSMAReqRewriteFuncs(STranslateContext* pCxt, SCreateTSMAStmt* pStmt, int32_t columnNum,
                                              int32_t tagNum, const SSchema* pCols) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SNode*         pNode;
  SFunctionNode* pFunc = NULL;
  STSMAOptions* pOptions = pStmt->pOptions;

  if (pOptions->recursiveTsma) {
    int32_t        i = 0;
    SColumnNode*   pCol = NULL;
    FOREACH(pNode, pOptions->pFuncs) {
      // rewrite all func parameters with tsma dest tb cols
      pFunc = (SFunctionNode*)pNode;
      const SSchema* pSchema = pCols + i;
      PAR_ERR_JRET(createColumnBySchema(pSchema, &pCol));
      (void)nodesListErase(pFunc->pParameterList, pFunc->pParameterList->pHead);
      PAR_ERR_JRET(nodesListPushFront(pFunc->pParameterList, (SNode*)pCol));

      snprintf(pFunc->node.userAlias, TSDB_COL_NAME_LEN, "%s", pSchema->name);
      // for first or last, the second param will be pk ts col, here we should remove it
      if (fmIsImplicitTsFunc(pFunc->funcId) && LIST_LENGTH(pFunc->pParameterList) == 2) {
        (void)nodesListErase(pFunc->pParameterList, pFunc->pParameterList->pTail);
      }
      ++i;
    }
    // recursive tsma, create func list from base tsma
    PAR_ERR_JRET(fmCreateStateMergeFuncs(pOptions->pFuncs));
  } else {
    FOREACH(pNode, pOptions->pFuncs) {
      pFunc = (SFunctionNode*)pNode;
      if (!pFunc->pParameterList || LIST_LENGTH(pFunc->pParameterList) != 1 ||
          nodeType(pFunc->pParameterList->pHead->pNode) != QUERY_NODE_COLUMN) {
        PAR_ERR_JRET(TSDB_CODE_TSMA_INVALID_FUNC_PARAM);
      }
      SColumnNode* pCol = (SColumnNode*)pFunc->pParameterList->pHead->pNode;
      int32_t      i = 0;
      for (; i < columnNum; ++i) {
        if (strcmp(pCols[i].name, pCol->colName) == 0) {
          pCol->colId = pCols[i].colId;
          pCol->node.resType.type = pCols[i].type;
          pCol->node.resType.bytes = pCols[i].bytes;
          break;
        }
      }
      if (i == columnNum) {
        PAR_ERR_JRET(TSDB_CODE_TSMA_INVALID_FUNC_PARAM);
      }
      PAR_ERR_JRET(fmGetFuncInfo(pFunc, NULL, 0));
      if (!fmIsTSMASupportedFunc(pFunc->funcId)) {
        PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSMA_UNSUPPORTED_FUNC, "Tsma func not supported"));
      }

      pCol = (SColumnNode*)pFunc->pParameterList->pHead->pNode;
      snprintf(pFunc->node.userAlias, TSDB_COL_NAME_LEN, "%s(%s)", pFunc->functionName, pCol->colName);
    }
  }
  nodesSortList(&pStmt->pOptions->pFuncs, compareTsmaFuncWithFuncAndColId);
  deduplicateTsmaFuncs(pStmt->pOptions->pFuncs);

  if (!pStmt->pOptions->recursiveTsma) {
    if (LIST_LENGTH(pStmt->pOptions->pFuncs) + tagNum + TSMA_RES_STB_EXTRA_COLUMN_NUM > TSDB_MAX_COLUMNS) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_TOO_MANY_COLUMNS, "Too many columns for TSMA"));
    }
  }

  return code;
_return:
  parserError("QID:0x%" PRIx64 ", %s error, code:%s", pCxt->pParseCxt->requestId, __FUNCTION__ , tstrerror(code));
  return code;
}

#define TSMA_MIN_INTERVAL_MS 1000 * 60                              // 1m
#define TSMA_MAX_INTERVAL_MS (60UL * 60UL * 1000UL * 24UL * 365UL)  // 1y

static int32_t buildCreateTSMAReqSetInterval(STranslateContext* pCxt, SCreateTSMAStmt* pStmt, SMCreateSmaReq* pReq) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SDbCfgInfo   pDbInfo = {0};

  PAR_ERR_JRET(getDBCfg(pCxt, pStmt->dbName, &pDbInfo));
  pStmt->precision = pDbInfo.precision;

  PAR_ERR_JRET(translateExpr(pCxt, &pStmt->pOptions->pInterval));

  pReq->interval = ((SValueNode*)pStmt->pOptions->pInterval)->datum.i;
  pReq->intervalUnit = ((SValueNode*)pStmt->pOptions->pInterval)->unit;

  if (!IS_CALENDAR_TIME_DURATION(pReq->intervalUnit)) {
    int64_t factor = TSDB_TICK_PER_SECOND(pDbInfo.precision) / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_MILLI);
    if (pReq->interval > TSMA_MAX_INTERVAL_MS * factor || pReq->interval < TSMA_MIN_INTERVAL_MS * factor) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSMA_INVALID_INTERVAL,
                                           "Invalid tsma interval, 1m ~ 1y is allowed"));
    }
  } else {
    if (pReq->intervalUnit == TIME_UNIT_MONTH && (pReq->interval < 1 || pReq->interval > 12)) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSMA_INVALID_INTERVAL,
                                           "Invalid tsma interval, 1m ~ 1y is allowed"));
    }
    if (pReq->intervalUnit == TIME_UNIT_YEAR && (pReq->interval != 1)) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSMA_INVALID_INTERVAL,
                                           "Invalid tsma interval, 1m ~ 1y is allowed"));
    }
  }
  return code;
_return:
  parserError("QID:0x%" PRIx64 ", %s error, code:%s", pCxt->pParseCxt->requestId, __FUNCTION__ , tstrerror(code));
  return code;
}

static int32_t buildCreateTSMAReqProcessRecursiveFuncs(STranslateContext* pCxt, SCreateTSMAStmt* pStmt, STableTSMAInfo* pRecursiveTsma) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  SNode*  pSelect = NULL;
  SNode*  pNew = NULL;
  bool    needFree = false;
  PAR_ERR_JRET(nodesStringToNode(pRecursiveTsma->ast, &pSelect));
  FOREACH(pNode, ((SSelectStmt*)pSelect)->pProjectionList) {
    SFunctionNode* pFuncNode = (SFunctionNode*)pNode;
    if (fmIsTSMASupportedFunc(pFuncNode->funcId)) {
      PAR_ERR_JRET(nodesCloneNode(pNode, &pNew));
      needFree = true;
      PAR_ERR_JRET(nodesListMakeStrictAppend(&pStmt->pOptions->pFuncs, pNew));
      needFree = false;
    }
  }

_return:
  if (code) {
    parserError("QID:0x%" PRIx64 ", %s error, code:%s", pCxt->pParseCxt->requestId, __FUNCTION__ , tstrerror(code));
  }
  if (needFree) {
    nodesDestroyNode(pNew);
  }
  nodesDestroyNode(pSelect);
  nodesDestroyNode(pNode);
  return code;
}

static int32_t buildCreateTSMAReqBuildNameAndUid(STranslateContext* pCxt, SCreateTSMAStmt* pStmt, SName* useTbName, SMCreateSmaReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  SName   name = {0};

  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tsmaName, &name);
  PAR_ERR_JRET(tNameExtractFullName(&name, pReq->name));

  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, useTbName);
  PAR_ERR_JRET(tNameExtractFullName(useTbName, pReq->stb));
  PAR_ERR_JRET(getSmaIndexSql(pCxt, &pReq->sql, &pReq->sqlLen));

  pReq->igExists = pStmt->ignoreExists;
  PAR_ERR_JRET(taosGetSystemUUIDU64(&pReq->uid));

  return code;
_return:
  parserError("QID:0x%" PRIx64 ", %s error, code:%s", pCxt->pParseCxt->requestId, __FUNCTION__ , tstrerror(code));
  return code;
}

static int32_t buildCreateTSMAReq(STranslateContext* pCxt, SCreateTSMAStmt* pStmt, SMCreateSmaReq* pReq, SName* useTbName) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SName           name = {0};
  STableMeta*     pTableMeta = NULL;
  STableTSMAInfo* pRecursiveTsma = NULL;
  int32_t         numOfCols = 0, numOfTags = 0;
  SSchema *       pCols = NULL, *pTags = NULL;

  PAR_ERR_JRET(buildCreateTSMAReqBuildNameAndUid(pCxt, pStmt, useTbName, pReq));

  PAR_ERR_JRET(buildCreateTSMAReqSetInterval(pCxt, pStmt, pReq));

  if (pStmt->pOptions->recursiveTsma) {
    // useTbName is base tsma name
    PAR_ERR_JRET(getTsma(pCxt, useTbName, &pRecursiveTsma));
    pReq->recursiveTsma = true;
    PAR_ERR_JRET(tNameExtractFullName(useTbName, pReq->baseTsmaName));
    SValueNode* pInterval = (SValueNode*)pStmt->pOptions->pInterval;
    if (!checkRecursiveTsmaInterval(pRecursiveTsma->interval, pRecursiveTsma->unit, pInterval->datum.i,
                                    pInterval->unit, pStmt->precision, true)) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSMA_INVALID_RECURSIVE_INTERVAL, "Invalid recursive tsma interval"));
    }

    PAR_ERR_JRET(buildCreateTSMAReqProcessRecursiveFuncs(pCxt, pStmt, pRecursiveTsma));

    memset(useTbName, 0, sizeof(SName));
    memcpy(pStmt->originalTbName, pRecursiveTsma->tb, TSDB_TABLE_NAME_LEN);
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pRecursiveTsma->tb, useTbName);
    PAR_ERR_JRET(tNameExtractFullName(useTbName, pReq->stb));

    numOfCols = pRecursiveTsma->pUsedCols->size;
    numOfTags = pRecursiveTsma->pTags ? pRecursiveTsma->pTags->size : 0;
    pCols = pRecursiveTsma->pUsedCols->pData;
    pTags = pRecursiveTsma->pTags ? pRecursiveTsma->pTags->pData : NULL;
    PAR_ERR_JRET(getTableMeta(pCxt, pStmt->dbName, pRecursiveTsma->targetTb, &pTableMeta));
  } else {
    PAR_ERR_JRET(getTableMeta(pCxt, pStmt->dbName, pStmt->tableName, &pTableMeta));
    if (pTableMeta->tableType == TSDB_CHILD_TABLE) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSMA_INVALID_TB, "Invalid table to create tsma, only stable or normal table allowed"));
    }
    pReq->normSourceTbUid = (pTableMeta->tableType == TSDB_NORMAL_TABLE) ? pTableMeta->uid : 0;
    numOfCols = pTableMeta->tableInfo.numOfColumns;
    numOfTags = pTableMeta->tableInfo.numOfTags;
    pCols = pTableMeta->schema;
    pTags = pTableMeta->schema + numOfCols;
  }

  pReq->deleteMark = convertTimePrecision(tsmaDataDeleteMark, TSDB_TIME_PRECISION_MILLI, pTableMeta->tableInfo.precision);

  PAR_ERR_JRET(buildCreateTSMAReqRewriteFuncs(pCxt, pStmt, numOfCols, numOfTags, pCols));

  PAR_ERR_JRET(buildCreateTSMAReqBuildStreamReq(pCxt, pStmt, pReq, pStmt->pOptions->recursiveTsma ? pRecursiveTsma->targetTb : pStmt->tableName, numOfTags, pTags));

_return:
  if (code) {
    parserError("buildCreateTSMAReq failed, code:%d", code);
  }
  taosMemoryFree(pTableMeta);
  return code;
}

static int32_t translateCreateTSMA(STranslateContext* pCxt, SCreateTSMAStmt* pStmt) {
  pCxt->pCurrStmt = (SNode*)pStmt;
  int32_t code = TSDB_CODE_SUCCESS;

  SName          useTbName = {0};
  SMCreateSmaReq pReq = {0};

  PAR_ERR_JRET(buildCreateTSMAReq(pCxt, pStmt, &pReq, &useTbName));
  PAR_ERR_JRET(collectUseTable(&useTbName, pCxt->pTargetTables));
  PAR_ERR_JRET(buildCmdMsg(pCxt, TDMT_MND_CREATE_TSMA, (FSerializeFunc)tSerializeSMCreateSmaReq, &pReq));

_return:
  if (code) {
    parserError("translateCreateTSMA failed, code:%d", code);
  }
  tFreeSMCreateSmaReq(&pReq);
  return code;
}

static int32_t translateDropTSMA(STranslateContext* pCxt, SDropTSMAStmt* pStmt) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SMDropSmaReq    dropReq = {0};
  SMDropStreamReq dropStreamReq = {0};
  SName           name = {0};
  STableTSMAInfo* pTsma = NULL;

  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tsmaName, &name);
  PAR_ERR_JRET(tNameExtractFullName(&name, dropReq.name));
  dropReq.igNotExists = pStmt->ignoreNotExists;

  dropStreamReq.name = taosMemoryCalloc(1, TSDB_STREAM_FNAME_LEN);
  if (NULL == dropStreamReq.name) {
    PAR_ERR_JRET(terrno);
  }
  PAR_ERR_JRET(tNameExtractFullName(&name, dropStreamReq.name));


  PAR_ERR_JRET(getTsma(pCxt, &name, &pTsma));
  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pTsma->tb, &name);
  PAR_ERR_JRET(collectUseTable(&name, pCxt->pTargetTables));

  dropStreamReq.igNotExists = false;

  dropReq.dropStreamReqLen = tSerializeSMDropStreamReq(NULL, 0, &dropStreamReq);
  dropReq.dropStreamReq = taosMemoryCalloc(1, dropReq.dropStreamReqLen);
  if (!dropReq.dropStreamReq) {
    PAR_ERR_JRET(terrno);
  }
  if (dropReq.dropStreamReqLen != tSerializeSMDropStreamReq(dropReq.dropStreamReq,
                                                            dropReq.dropStreamReqLen,
                                                            &dropStreamReq)) {
    PAR_ERR_JRET(TSDB_CODE_INVALID_MSG);
  }

  PAR_ERR_JRET(buildCmdMsg(pCxt, TDMT_MND_DROP_TSMA, (FSerializeFunc)tSerializeSMDropSmaReq, &dropReq));

_return:
  if (code) {
    parserError("translateDropTSMA failed, code:%d", code);
  }
  tFreeMDropStreamReq(&dropStreamReq);
  tFreeSMDropSmaReq(&dropReq);
  return code;
}
#ifdef TD_ENTERPRISE
static int32_t compareRsmaFuncWithColId(SNode* pNode1, SNode* pNode2) {
  SFunctionNode* pFunc1 = (SFunctionNode*)pNode1;
  SFunctionNode* pFunc2 = (SFunctionNode*)pNode2;
  SNode*         pCol1 = pFunc1->pParameterList->pHead->pNode;
  SNode*         pCol2 = pFunc2->pParameterList->pHead->pNode;
  return compareTsmaColWithColId(pCol1, pCol2);
}

static int32_t rewriteRsmaFuncs(STranslateContext* pCxt, SNodeList** ppFuncs, int32_t columnNum, const SSchema* pCols) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t nFuncs = LIST_LENGTH(*ppFuncs);

  SNode*         pNode;
  SFunctionNode* pFunc = NULL;
  const SSchema* pSchema = NULL;
  int32_t        i = 0, j = 0, k = 0;
  int8_t         flags = 0;
  FOREACH(pNode, *ppFuncs) {
    pFunc = (SFunctionNode*)pNode;
    if (!pFunc->pParameterList || LIST_LENGTH(pFunc->pParameterList) != 1 ||
        nodeType(pFunc->pParameterList->pHead->pNode) != QUERY_NODE_COLUMN) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_RSMA_INVALID_FUNC_PARAM,
                                     "Invalid func param for rsma, only one non-primary key column allowed: %s",
                                     pFunc->functionName);
    }

    SColumnNode* pCol = (SColumnNode*)pFunc->pParameterList->pHead->pNode;
    if (!pCol) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_RSMA_INVALID_FUNC_PARAM,
                                     "Invalid func param for rsma since column node is NULL: %s", pFunc->functionName);
    }

    j = i;
    pSchema = NULL;
    for (; i < columnNum; ++i) {
      if (strcasecmp(pCols[i].name, pCol->colName) == 0) {
        pSchema = pCols + i;
        break;
      }
    }
    if (!pSchema) {
      for (k = 0; k < j; ++k) {
        if (strcasecmp(pCols[k].name, pCol->colName) == 0) {
          pSchema = pCols + k;
          break;
        }
      }
    }

    if (!pSchema) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_RSMA_INVALID_FUNC_PARAM,
                                     "Invalid func param for rsma since column not exist: %s(%s)", pFunc->functionName,
                                     pCol->colName);
    }
    if (pSchema->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_RSMA_INVALID_FUNC_PARAM,
                                     "Invalid func param for rsma since column is primary key: %s(%s)",
                                     pFunc->functionName, pCol->colName);
    }
    pCol->colId = pSchema->colId;
    pCol->node.resType.type = pSchema->type;
    pCol->node.resType.bytes = pSchema->bytes;
    if ((code = fmGetFuncInfo(pFunc, pCxt->msgBuf.buf, pCxt->msgBuf.len)) != TSDB_CODE_SUCCESS) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, code, "%s: %s(%s)", tstrerror(code), pFunc->functionName,
                                     pCol->colName);
    }
    if (!fmIsRsmaSupportedFunc(pFunc->funcId)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_RSMA_UNSUPPORTED_FUNC, "Invalid func for rsma: %s",
                                     pFunc->functionName);
    }
    if (((pSchema->flags) & COL_IS_KEY)) {
      if (pFunc->funcType != FUNCTION_TYPE_FIRST && pFunc->funcType != FUNCTION_TYPE_LAST) {
        return generateSyntaxErrMsgExt(
            &pCxt->msgBuf, TSDB_CODE_RSMA_INVALID_FUNC_PARAM,
            "Invalid func param for rsma since composite key column can only be first/last: %s(%s)",
            pFunc->functionName, pCol->colName);
      }
    }
    (void)snprintf(pFunc->node.userAlias, TSDB_COL_NAME_LEN, "%s(%s)", pFunc->functionName, pCol->colName);
  }

  if (nFuncs > 1) {
    nodesSortList(ppFuncs, compareRsmaFuncWithColId);
    col_id_t lastColId = -1;
    FOREACH(pNode, *ppFuncs) {
      SFunctionNode* pFuncNode = (SFunctionNode*)pNode;
      SColumnNode*   pColNode = (SColumnNode*)pFuncNode->pParameterList->pHead->pNode;
      if (pColNode->colId == lastColId) {
        return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_DUPLICATED_COLUMN,
                                       "Duplicated column not allowed for rsma: %s", pColNode->colName);
      }
      lastColId = pColNode->colId;
    }
  }
_return:
  return code;
}

static int32_t buildCreateRsmaReq(STranslateContext* pCxt, SCreateRsmaStmt* pStmt, SMCreateRsmaReq* pReq,
                                  SName* useTbName) {
  SName       name = {0};
  SDbCfgInfo  pDbInfo = {0};
  int32_t     code = 0, lino = 0;
  SNode*      pNode = NULL;
  STableMeta* pTableMeta = NULL;

  if (IS_SYS_DBNAME(pStmt->dbName)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OPS_NOT_SUPPORT,
                                   "Cannot create rsma on system table: `%s`.`%s`", pStmt->dbName, pStmt->tableName);
  }
  (void)snprintf(pReq->name, TSDB_TABLE_NAME_LEN, "%s", pStmt->rsmaName);

  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, useTbName);
  PAR_ERR_JRET(tNameExtractFullName(useTbName, pReq->tbFName));
  pReq->tbType = TSDB_SUPER_TABLE;

  pReq->igExists = pStmt->ignoreExists;
  PAR_ERR_JRET(getDBCfg(pCxt, pStmt->dbName, &pDbInfo));

  if (LIST_LENGTH(pStmt->pIntervals) < 1 || LIST_LENGTH(pStmt->pIntervals) > 2) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OPS_NOT_SUPPORT,
                                   "Invalid interval count for rsma, should be in range [1, 2]");
  }

  int64_t durationInPrecision = 0;
  PAR_ERR_JRET(getDuration(pDbInfo.daysPerFile, TIME_UNIT_MINUTE, &durationInPrecision, pDbInfo.precision));

  int32_t idx = 0;
  pReq->intervalUnit = getPrecisionUnit(pDbInfo.precision);
  FOREACH(pNode, pStmt->pIntervals) {
    SValueNode* pVal = (SValueNode*)pNode;
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
    if (pVal->unit == 0 || pVal->unit == TIME_UNIT_WEEK || pVal->unit == TIME_UNIT_MONTH || pVal->unit == TIME_UNIT_YEAR) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OPS_NOT_SUPPORT, "Invalid interval unit for rsma: %c",
                                     pVal->unit);
    }
    pReq->interval[idx] = pVal->datum.i;

    if (pReq->interval[idx] < 0 || pReq->interval[idx] > durationInPrecision) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OUT_OF_RANGE, "Invalid interval value for rsma: %" PRId64 ", valid range [0, %" PRId64 "]",
                                     pReq->interval[idx], durationInPrecision);
    }
    if ((pReq->interval[idx] > 0) && (durationInPrecision % pReq->interval[idx])) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OPS_NOT_SUPPORT,
                                     "Interval value for rsma, should be a divisor of DB duration: %" PRId64
                                     "%c, %" PRId64,
                                     pVal->datum.i, pVal->unit, durationInPrecision);
    }
    ++idx;
  }
  if (pReq->interval[0] == 0 && pReq->interval[1] == 0) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OPS_NOT_SUPPORT,
                                   "At least one interval value for rsma should be greater than 0");
  }
  if (pReq->interval[0] > 0 && pReq->interval[1] > 0) {
    if (pReq->interval[1] <= pReq->interval[0]) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OPS_NOT_SUPPORT,
                                     "Second interval value for rsma should be greater than first interval: %" PRId64
                                     ",%" PRId64,
                                     pReq->interval[0], pReq->interval[1]);
    }
    if ((pReq->interval[1] % pReq->interval[0]) != 0) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OPS_NOT_SUPPORT,
                                     "Second interval value for rsma should be a multiple of first interval: %" PRId64
                                     ",%" PRId64,
                                     pReq->interval[0], pReq->interval[1]);
    }
  }

  int32_t  numOfCols = 0;
  SSchema* pCols = NULL;
  int32_t  nFuncs = LIST_LENGTH(pStmt->pFuncs);
  PAR_ERR_JRET(getTableMeta(pCxt, pStmt->dbName, pStmt->tableName, &pTableMeta));
  numOfCols = pTableMeta->tableInfo.numOfColumns;
  pCols = pTableMeta->schema;
  if (nFuncs < 0 || nFuncs > (numOfCols - 1)) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OPS_NOT_SUPPORT,
                                         "Invalid func count for rsma, should be in range [0, %d]", numOfCols - 1));
  }
  if (pTableMeta->tableType != TSDB_SUPER_TABLE) {
    PAR_ERR_JRET(
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OPS_NOT_SUPPORT, "Rsma must be created on super table"));
  }

  for (int32_t c = 1; c < numOfCols; ++c) {
    int8_t type = pCols[c].type;
    if (type == TSDB_DATA_TYPE_BLOB || type == TSDB_DATA_TYPE_MEDIUMBLOB) {
      PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OPS_NOT_SUPPORT,
                                           "Rsma does not support column type %" PRIi8 " currently", type));
    }
  }

  pReq->tbUid = pTableMeta->uid;

  if (nFuncs > 0) {
    PAR_ERR_JRET(rewriteRsmaFuncs(pCxt, &pStmt->pFuncs, numOfCols, pCols));
    pReq->funcColIds = taosMemoryCalloc(nFuncs, sizeof(col_id_t));
    pReq->funcIds = taosMemoryCalloc(nFuncs, sizeof(int32_t));
    if (!pReq->funcColIds || !pReq->funcIds) {
      PAR_ERR_JRET(terrno);
    }

    int32_t        idx = 0;
    SFunctionNode* pFunc = NULL;
    FOREACH(pNode, pStmt->pFuncs) {
      pFunc = (SFunctionNode*)pNode;
      pReq->funcColIds[idx] = ((SColumnNode*)pFunc->pParameterList->pHead->pNode)->colId;
      pReq->funcIds[idx] = pFunc->funcId;
      ++idx;
    }
    pReq->nFuncs = nFuncs;
  }

_return:
  taosMemoryFreeClear(pTableMeta);
  TAOS_RETURN(code);
}

static int32_t buildAlterRsmaReq(STranslateContext* pCxt, SAlterRsmaStmt* pStmt, SRsmaInfoRsp* pRsmaInfo,
                                 SMAlterRsmaReq* pReq, SName* useTbName) {
  SName       name = {0};
  SDbCfgInfo  pDbInfo = {0};
  int32_t     code = 0, lino = 0;
  SNode*      pNode = NULL;
  STableMeta* pTableMeta = NULL;

  (void)snprintf(pReq->name, TSDB_TABLE_NAME_LEN, "%s", pStmt->rsmaName);
  pReq->tbType = TSDB_SUPER_TABLE;
  pReq->igNotExists = pStmt->ignoreNotExists;
  TAOS_CHECK_EXIT(tNameFromString(&name, pRsmaInfo->tbFName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));
  toName(pCxt->pParseCxt->acctId, pStmt->dbName, name.tname, useTbName);
  TAOS_CHECK_EXIT(getDBCfg(pCxt, pStmt->dbName, &pDbInfo));

  int32_t  numOfCols = 0;
  SSchema* pCols = NULL;
  int32_t  nFuncs = LIST_LENGTH(pStmt->pFuncs);
  bool     async = pCxt->pParseCxt->async;
  pCxt->pParseCxt->async = false;
  // get table meta in sync since tbName is unknown during parser phase
  code = getTableMeta(pCxt, pStmt->dbName, name.tname, &pTableMeta);
  pCxt->pParseCxt->async = async;
  TAOS_CHECK_EXIT(code);
  numOfCols = pTableMeta->tableInfo.numOfColumns;
  pCols = pTableMeta->schema;

  if (pReq->alterType == TSDB_ALTER_RSMA_FUNCTION) {
    if (nFuncs <= 0 || nFuncs > (numOfCols - 1)) {
      TAOS_CHECK_EXIT(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OPS_NOT_SUPPORT,
                                              "Invalid func count for rsma, should be in range [1, %d]",
                                              numOfCols - 1));
    }

    TAOS_CHECK_EXIT(rewriteRsmaFuncs(pCxt, &pStmt->pFuncs, numOfCols, pCols));

    if (pRsmaInfo->nFuncs > 0) {
      FOREACH(pNode, pStmt->pFuncs) {
        SFunctionNode* pFuncNode = (SFunctionNode*)pNode;
        SColumnNode*   pColNode = (SColumnNode*)pFuncNode->pParameterList->pHead->pNode;
        int32_t        i = 0;
        while (i < pRsmaInfo->nFuncs) {
          if (pColNode->colId == pRsmaInfo->funcColIds[i]) {
            TAOS_CHECK_EXIT(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OPS_NOT_SUPPORT,
                                                    "Rsma func already specified for column: %s", pColNode->colName));
          } else if (pColNode->colId > pRsmaInfo->funcColIds[i]) {
            ++i;
          } else {
            break;
          }
        }
      }
    }

    pReq->funcColIds = taosMemoryCalloc(nFuncs, sizeof(col_id_t));
    pReq->funcIds = taosMemoryCalloc(nFuncs, sizeof(int32_t));
    if (!pReq->funcColIds || !pReq->funcIds) {
      TAOS_CHECK_EXIT(terrno);
    }

    int32_t        idx = 0;
    SFunctionNode* pFunc = NULL;
    FOREACH(pNode, pStmt->pFuncs) {
      pFunc = (SFunctionNode*)pNode;
      pReq->funcColIds[idx] = ((SColumnNode*)pFunc->pParameterList->pHead->pNode)->colId;
      pReq->funcIds[idx] = pFunc->funcId;
      ++idx;
    }
    pReq->nFuncs = nFuncs;
  } else {
    TAOS_CHECK_EXIT(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_OPS_NOT_SUPPORT,
                                            "Invalid alter type for rsma: %" PRIi8, pReq->alterType));
  }

_exit:
  taosMemoryFreeClear(pTableMeta);
  TAOS_RETURN(code);
}

#endif

static int32_t translateCreateRsma(STranslateContext* pCxt, SCreateRsmaStmt* pStmt) {
#ifdef TD_ENTERPRISE
  int32_t code = TSDB_CODE_SUCCESS;
  pCxt->pCurrStmt = (SNode*)pStmt;

  SName           useTbName = {0};
  SMCreateRsmaReq req = {0};

  PAR_ERR_JRET(buildCreateRsmaReq(pCxt, pStmt, &req, &useTbName));
  PAR_ERR_JRET(collectUseTable(&useTbName, pCxt->pTargetTables));
  PAR_ERR_JRET(buildCmdMsg(pCxt, TDMT_MND_CREATE_RSMA, (FSerializeFunc)tSerializeSMCreateRsmaReq, &req));
_return:
  tFreeSMCreateRsmaReq(&req);
  return code;
#else
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

static int32_t translateDropRsma(STranslateContext* pCxt, SDropRsmaStmt* pStmt) {
#ifdef TD_ENTERPRISE
  int32_t      code = TSDB_CODE_SUCCESS;
  SMDropSmaReq dropReq = {0};
  SName        name = {0};

  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->rsmaName, &name);
  PAR_ERR_JRET(tNameExtractFullName(&name, dropReq.name));
  (void)snprintf(dropReq.name, TSDB_TABLE_NAME_LEN, "%s", name.tname);
  dropReq.igNotExists = pStmt->ignoreNotExists;

  PAR_ERR_JRET(buildCmdMsg(pCxt, TDMT_MND_DROP_RSMA, (FSerializeFunc)tSerializeSMDropRsmaReq, &dropReq));
_return:
  return code;
#else
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

static int32_t translateAlterRsma(STranslateContext* pCxt, SAlterRsmaStmt* pStmt) {
#ifdef TD_ENTERPRISE
  int32_t        code = 0;
  SMAlterRsmaReq req = {0};
  req.alterType = pStmt->alterType;
  SRsmaInfoRsp* pRsmaInfo = NULL;
  SName         useTbName = {0};

  PAR_ERR_JRET(getRsma(pCxt, pStmt->rsmaName, &pRsmaInfo));
  PAR_ERR_JRET(buildAlterRsmaReq(pCxt, pStmt, pRsmaInfo, &req, &useTbName));
  PAR_ERR_JRET(collectUseTable(&useTbName, pCxt->pTargetTables));
  PAR_ERR_JRET(buildCmdMsg(pCxt, TDMT_MND_ALTER_RSMA, (FSerializeFunc)tSerializeSMAlterRsmaReq, &req));
_return:
  if (pRsmaInfo) {
    tFreeRsmaInfoRsp(pRsmaInfo, true);
    taosMemoryFreeClear(pRsmaInfo);
  }
  tFreeSMAlterRsmaReq(&req);
  return code;
#else
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

  static int32_t translateQuery(STranslateContext * pCxt, SNode * pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pNode)) {
    case QUERY_NODE_SELECT_STMT:
      code = translateSelect(pCxt, (SSelectStmt*)pNode);
      break;
    case QUERY_NODE_SET_OPERATOR:
      code = translateSetOperator(pCxt, (SSetOperator*)pNode);
      break;
    case QUERY_NODE_DELETE_STMT:
      code = translateDelete(pCxt, (SDeleteStmt*)pNode);
      break;
    case QUERY_NODE_INSERT_STMT:
      code = translateInsert(pCxt, (SInsertStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_DATABASE_STMT:
      code = translateCreateDatabase(pCxt, (SCreateDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_DROP_DATABASE_STMT:
      code = translateDropDatabase(pCxt, (SDropDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_ALTER_DATABASE_STMT:
      code = translateAlterDatabase(pCxt, (SAlterDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_TRIM_DATABASE_STMT:
      code = translateTrimDatabase(pCxt, (STrimDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_SSMIGRATE_DATABASE_STMT:
      code = translateSsMigrateDatabase(pCxt, (SSsMigrateDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_TRIM_DATABASE_WAL_STMT:
      code = translateTrimDbWal(pCxt, (STrimDbWalStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_TABLE_STMT:
      code = translateCreateSuperTable(pCxt, (SCreateTableStmt*)pNode);
      break;
    case QUERY_NODE_DROP_TABLE_STMT:
      code = translateDropTable(pCxt, (SDropTableStmt*)pNode);
      break;
    case QUERY_NODE_DROP_SUPER_TABLE_STMT:
      code = translateDropSuperTable(pCxt, (SDropSuperTableStmt*)pNode);
      break;
    case QUERY_NODE_ALTER_TABLE_STMT:
    case QUERY_NODE_ALTER_SUPER_TABLE_STMT:
      code = translateAlterSuperTable(pCxt, (SAlterTableStmt*)pNode);
      break;
#ifdef USE_MOUNT
    case QUERY_NODE_CREATE_MOUNT_STMT:
      code = translateCreateMount(pCxt, (SCreateMountStmt*)pNode);
      break;
    case QUERY_NODE_DROP_MOUNT_STMT:
      code = translateDropMount(pCxt, (SDropMountStmt*)pNode);
      break;
#endif
    case QUERY_NODE_CREATE_USER_STMT:
      code = translateCreateUser(pCxt, (SCreateUserStmt*)pNode);
      break;
    case QUERY_NODE_ALTER_USER_STMT:
      code = translateAlterUser(pCxt, (SAlterUserStmt*)pNode);
      break;
    case QUERY_NODE_DROP_USER_STMT:
      code = translateDropUser(pCxt, (SDropUserStmt*)pNode);
      break;
    case QUERY_NODE_USE_DATABASE_STMT:
      code = translateUseDatabase(pCxt, (SUseDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_DNODE_STMT:
      code = translateCreateDnode(pCxt, (SCreateDnodeStmt*)pNode);
      break;
    case QUERY_NODE_DROP_DNODE_STMT:
      code = translateDropDnode(pCxt, (SDropDnodeStmt*)pNode);
      break;
    case QUERY_NODE_ALTER_DNODE_STMT:
      code = translateAlterDnode(pCxt, (SAlterDnodeStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_ANODE_STMT:
      code = translateCreateAnode(pCxt, (SCreateAnodeStmt*)pNode);
      break;
    case QUERY_NODE_DROP_ANODE_STMT:
      code = translateDropAnode(pCxt, (SDropAnodeStmt*)pNode);
      break;
    case QUERY_NODE_UPDATE_ANODE_STMT:
      code = translateUpdateAnode(pCxt, (SUpdateAnodeStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_BNODE_STMT:
      code = translateCreateBnode(pCxt, (SCreateBnodeStmt*)pNode);
      break;
    case QUERY_NODE_DROP_BNODE_STMT:
      code = translateDropBnode(pCxt, (SDropBnodeStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_INDEX_STMT:
      code = translateCreateIndex(pCxt, (SCreateIndexStmt*)pNode);
      break;
    case QUERY_NODE_DROP_INDEX_STMT:
      code = translateDropIndex(pCxt, (SDropIndexStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_QNODE_STMT:
    case QUERY_NODE_CREATE_BACKUP_NODE_STMT:
    case QUERY_NODE_CREATE_SNODE_STMT:
    case QUERY_NODE_CREATE_MNODE_STMT:
      code = translateCreateComponentNode(pCxt, (SCreateComponentNodeStmt*)pNode);
      break;
    case QUERY_NODE_DROP_QNODE_STMT:
    case QUERY_NODE_DROP_BACKUP_NODE_STMT:
    case QUERY_NODE_DROP_SNODE_STMT:
    case QUERY_NODE_DROP_MNODE_STMT:
      code = translateDropComponentNode(pCxt, (SDropComponentNodeStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_TOPIC_STMT:
      code = translateCreateTopic(pCxt, (SCreateTopicStmt*)pNode);
      break;
    case QUERY_NODE_DROP_TOPIC_STMT:
      code = translateDropTopic(pCxt, (SDropTopicStmt*)pNode);
      break;
    case QUERY_NODE_DROP_CGROUP_STMT:
      code = translateDropCGroup(pCxt, (SDropCGroupStmt*)pNode);
      break;
    case QUERY_NODE_ALTER_LOCAL_STMT:
      code = translateAlterLocal(pCxt, (SAlterLocalStmt*)pNode);
      break;
    case QUERY_NODE_EXPLAIN_STMT:
      code = translateExplain(pCxt, (SExplainStmt*)pNode);
      break;
    case QUERY_NODE_DESCRIBE_STMT:
      code = translateDescribe(pCxt, (SDescribeStmt*)pNode);
      break;
    case QUERY_NODE_COMPACT_DATABASE_STMT:
      code = translateCompactDb(pCxt, (SCompactDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_ROLLUP_DATABASE_STMT:
      code = translateRollupDb(pCxt, (SRollupDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_SCAN_DATABASE_STMT:
      code = translateScanDb(pCxt, (SScanDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_COMPACT_VGROUPS_STMT:
      code = translateCompactVgroups(pCxt, (SCompactVgroupsStmt*)pNode);
      break;
    case QUERY_NODE_ROLLUP_VGROUPS_STMT:
      code = translateRollupVgroups(pCxt, (SRollupVgroupsStmt*)pNode);
      break;
    case QUERY_NODE_SCAN_VGROUPS_STMT:
      code = translateScanVgroups(pCxt, (SScanVgroupsStmt*)pNode);
      break;
    case QUERY_NODE_ALTER_CLUSTER_STMT:
      code = translateAlterCluster(pCxt, (SAlterClusterStmt*)pNode);
      break;
    case QUERY_NODE_KILL_CONNECTION_STMT:
      code = translateKillConnection(pCxt, (SKillStmt*)pNode);
      break;
    case QUERY_NODE_KILL_COMPACT_STMT:
      code = translateKillCompact(pCxt, (SKillStmt*)pNode);
      break;
    case QUERY_NODE_KILL_RETENTION_STMT:
      code = translateKillRetention(pCxt, (SKillStmt*)pNode);
      break;
    case QUERY_NODE_KILL_SCAN_STMT:
      code = translateKillScan(pCxt, (SKillStmt*)pNode);
      break;
    case QUERY_NODE_KILL_QUERY_STMT:
      code = translateKillQuery(pCxt, (SKillQueryStmt*)pNode);
      break;
    case QUERY_NODE_KILL_TRANSACTION_STMT:
      code = translateKillTransaction(pCxt, (SKillStmt*)pNode);
      break;
    case QUERY_NODE_KILL_SSMIGRATE_STMT:
      code = translateKillSsMigrate(pCxt, (SKillStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_STREAM_STMT:
      code = translateCreateStream(pCxt, (SCreateStreamStmt*)pNode);
      break;
    case QUERY_NODE_DROP_STREAM_STMT:
      code = translateDropStream(pCxt, (SDropStreamStmt*)pNode);
      break;
    case QUERY_NODE_PAUSE_STREAM_STMT:
      code = translatePauseStream(pCxt, (SPauseStreamStmt*)pNode);
      break;
    case QUERY_NODE_RESUME_STREAM_STMT:
      code = translateResumeStream(pCxt, (SResumeStreamStmt*)pNode);
      break;
    case QUERY_NODE_RECALCULATE_STREAM_STMT:
      code = translateRecalcStream(pCxt, (SRecalcStreamStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_FUNCTION_STMT:
      code = translateCreateFunction(pCxt, (SCreateFunctionStmt*)pNode);
      break;
    case QUERY_NODE_DROP_FUNCTION_STMT:
      code = translateDropFunction(pCxt, (SDropFunctionStmt*)pNode);
      break;
    case QUERY_NODE_GRANT_STMT:
      code = translateGrant(pCxt, (SGrantStmt*)pNode);
      break;
    case QUERY_NODE_REVOKE_STMT:
      code = translateRevoke(pCxt, (SRevokeStmt*)pNode);
      break;
    case QUERY_NODE_BALANCE_VGROUP_STMT:
      code = translateBalanceVgroup(pCxt, (SBalanceVgroupStmt*)pNode);
      break;
    case QUERY_NODE_ASSIGN_LEADER_STMT:
      code = translateAssignLeader(pCxt, (SAssignLeaderStmt*)pNode);
      break;
    case QUERY_NODE_BALANCE_VGROUP_LEADER_STMT:
    case QUERY_NODE_BALANCE_VGROUP_LEADER_DATABASE_STMT:
      code = translateBalanceVgroupLeader(pCxt, (SBalanceVgroupLeaderStmt*)pNode);
      break;
    case QUERY_NODE_SET_VGROUP_KEEP_VERSION_STMT:
      code = translateSetVgroupKeepVersion(pCxt, (SSetVgroupKeepVersionStmt*)pNode);
      break;
    case QUERY_NODE_MERGE_VGROUP_STMT:
      code = translateMergeVgroup(pCxt, (SMergeVgroupStmt*)pNode);
      break;
    case QUERY_NODE_REDISTRIBUTE_VGROUP_STMT:
      code = translateRedistributeVgroup(pCxt, (SRedistributeVgroupStmt*)pNode);
      break;
    case QUERY_NODE_SPLIT_VGROUP_STMT:
      code = translateSplitVgroup(pCxt, (SSplitVgroupStmt*)pNode);
      break;
    case QUERY_NODE_SHOW_VARIABLES_STMT:
      code = translateShowVariables(pCxt, (SShowStmt*)pNode);
      break;
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
      code = translateShowCreateDatabase(pCxt, (SShowCreateDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      code = translateShowCreateTable(pCxt, (SShowCreateTableStmt*)pNode, false);
      break;
    case QUERY_NODE_SHOW_CREATE_VTABLE_STMT:
      code = translateShowCreateVTable(pCxt, (SShowCreateTableStmt*)pNode);
      break;
    case QUERY_NODE_SHOW_CREATE_VIEW_STMT:
      code = translateShowCreateView(pCxt, (SShowCreateViewStmt*)pNode);
      break;
    case QUERY_NODE_SHOW_CREATE_RSMA_STMT:
      code = translateShowCreateRsma(pCxt, (SShowCreateRsmaStmt*)pNode);
      break;
    case QUERY_NODE_RESTORE_DNODE_STMT:
    case QUERY_NODE_RESTORE_QNODE_STMT:
    case QUERY_NODE_RESTORE_MNODE_STMT:
    case QUERY_NODE_RESTORE_VNODE_STMT:
      code = translateRestoreDnode(pCxt, (SRestoreComponentNodeStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_VIEW_STMT:
      code = translateCreateView(pCxt, (SCreateViewStmt*)pNode);
      break;
    case QUERY_NODE_DROP_VIEW_STMT:
      code = translateDropView(pCxt, (SDropViewStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_TSMA_STMT:
      code = translateCreateTSMA(pCxt, (SCreateTSMAStmt*)pNode);
      break;
    case QUERY_NODE_SHOW_CREATE_TSMA_STMT:
      break;
    case QUERY_NODE_DROP_TSMA_STMT:
      code = translateDropTSMA(pCxt, (SDropTSMAStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_RSMA_STMT:
      code = translateCreateRsma(pCxt, (SCreateRsmaStmt*)pNode);
      break;
    case QUERY_NODE_DROP_RSMA_STMT:
      code = translateDropRsma(pCxt, (SDropRsmaStmt*)pNode);
      break;
    case QUERY_NODE_ALTER_RSMA_STMT:
      code = translateAlterRsma(pCxt, (SAlterRsmaStmt*)pNode);
      break;
    default:
      break;
  }
  return code;
}

static int32_t translateSubquery(STranslateContext* pCxt, SNode* pNode) {
  ESqlClause currClause = pCxt->currClause;
  SNode*     pCurrStmt = pCxt->pCurrStmt;
  int32_t    currLevel = pCxt->currLevel;
  pCxt->currLevel = ++(pCxt->levelNo);
  int32_t code = translateQuery(pCxt, pNode);
  pCxt->currClause = currClause;
  pCxt->pCurrStmt = pCurrStmt;
  pCxt->currLevel = currLevel;
  return code;
}

static int32_t extractQueryResultSchema(const SNodeList* pProjections, int32_t* numOfCols, SSchema** pSchema,
                                        SExtSchema** ppExtSchemas) {
  *numOfCols = LIST_LENGTH(pProjections);
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return terrno;
  }
  if (ppExtSchemas) *ppExtSchemas = taosMemoryCalloc(*numOfCols, sizeof(SExtSchema));
  if (ppExtSchemas && *ppExtSchemas == NULL) {
    taosMemoryFreeClear(*pSchema);
    return terrno;
  }

  SNode*  pNode;
  int32_t index = 0;
  FOREACH(pNode, pProjections) {
    SExprNode* pExpr = (SExprNode*)pNode;
    if (TSDB_DATA_TYPE_NULL == pExpr->resType.type) {
      (*pSchema)[index].type = TSDB_DATA_TYPE_VARCHAR;
      (*pSchema)[index].bytes = VARSTR_HEADER_SIZE;
    } else {
      (*pSchema)[index].type = pExpr->resType.type;
      (*pSchema)[index].bytes = pExpr->resType.bytes;
      if (ppExtSchemas) {
        (*ppExtSchemas)[index].typeMod = calcTypeMod(&pExpr->resType);
      }
    }
    (*pSchema)[index].colId = index + 1;
    if ('\0' != pExpr->userAlias[0]) {
      tstrncpy((*pSchema)[index].name, pExpr->userAlias, TSDB_COL_NAME_LEN);
    } else {
      tstrncpy((*pSchema)[index].name, pExpr->aliasName, TSDB_COL_NAME_LEN);
    }
    index += 1;
  }

  return TSDB_CODE_SUCCESS;
}

static int8_t extractResultTsPrecision(const SSelectStmt* pSelect) { return pSelect->precision; }

static int32_t extractExplainResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = 1;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return terrno;
  }
  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = TSDB_EXPLAIN_RESULT_ROW_SIZE;
  tstrncpy((*pSchema)[0].name, TSDB_EXPLAIN_RESULT_COLUMN_NAME, TSDB_COL_NAME_LEN);
  return TSDB_CODE_SUCCESS;
}

static int32_t extractDescribeResultSchema(STableMeta* pMeta, int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = DESCRIBE_RESULT_COLS;
  if (pMeta) {
    if (withExtSchema(pMeta->tableType)) {
      *numOfCols = DESCRIBE_RESULT_COLS_COMPRESS;
    } else if (hasRefCol(pMeta->tableType)) {
      *numOfCols = DESCRIBE_RESULT_COLS_REF;
    } else {
      // DESCRIBE_RESULT_COLS
    }
  }
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return terrno;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = DESCRIBE_RESULT_FIELD_LEN;
  tstrncpy((*pSchema)[0].name, "field", TSDB_COL_NAME_LEN);

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = DESCRIBE_RESULT_TYPE_LEN;
  tstrncpy((*pSchema)[1].name, "type", TSDB_COL_NAME_LEN);

  (*pSchema)[2].type = TSDB_DATA_TYPE_INT;
  (*pSchema)[2].bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  tstrncpy((*pSchema)[2].name, "length", TSDB_COL_NAME_LEN);

  (*pSchema)[3].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[3].bytes = DESCRIBE_RESULT_NOTE_LEN;
  tstrncpy((*pSchema)[3].name, "note", TSDB_COL_NAME_LEN);

  if (pMeta) {
    if (withExtSchema(pMeta->tableType)) {
      (*pSchema)[4].type = TSDB_DATA_TYPE_BINARY;
      (*pSchema)[4].bytes = DESCRIBE_RESULT_COPRESS_OPTION_LEN;
      tstrncpy((*pSchema)[4].name, "encode", TSDB_COL_NAME_LEN);

      (*pSchema)[5].type = TSDB_DATA_TYPE_BINARY;
      (*pSchema)[5].bytes = DESCRIBE_RESULT_COPRESS_OPTION_LEN;
      tstrncpy((*pSchema)[5].name, "compress", TSDB_COL_NAME_LEN);

      (*pSchema)[6].type = TSDB_DATA_TYPE_BINARY;
      (*pSchema)[6].bytes = DESCRIBE_RESULT_COPRESS_OPTION_LEN;
      tstrncpy((*pSchema)[6].name, "level", TSDB_COL_NAME_LEN);
    } else if (hasRefCol(pMeta->tableType)) {
      (*pSchema)[4].type = TSDB_DATA_TYPE_BINARY;
      (*pSchema)[4].bytes = DESCRIBE_RESULT_COL_REF_LEN;
      tstrncpy((*pSchema)[4].name, "ref", TSDB_COL_NAME_LEN);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extractShowCreateDatabaseResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = 2;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return terrno;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = TSDB_DB_NAME_LEN;
  tstrncpy((*pSchema)[0].name, "Database", TSDB_COL_NAME_LEN);

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = TSDB_MAX_BINARY_LEN;
  tstrncpy((*pSchema)[1].name, "Create Database", TSDB_COL_NAME_LEN);

  return TSDB_CODE_SUCCESS;
}

static int32_t extractShowCreateTableResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = 2;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return terrno;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = SHOW_CREATE_TB_RESULT_FIELD1_LEN;
  tstrncpy((*pSchema)[0].name, "Table", TSDB_COL_NAME_LEN);

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = SHOW_CREATE_TB_RESULT_FIELD2_LEN;
  tstrncpy((*pSchema)[1].name, "Create Table", TSDB_COL_NAME_LEN);

  return TSDB_CODE_SUCCESS;
}

static int32_t extractShowCreateRsmaResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = 2;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return terrno;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = SHOW_CREATE_TB_RESULT_FIELD1_LEN;
  tstrncpy((*pSchema)[0].name, "RSMA", TSDB_COL_NAME_LEN);

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = SHOW_CREATE_TB_RESULT_FIELD2_LEN;
  tstrncpy((*pSchema)[1].name, "Create RSMA", TSDB_COL_NAME_LEN);

  return TSDB_CODE_SUCCESS;
}

static int32_t extractShowCreateVTableResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = 2;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return terrno;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = SHOW_CREATE_TB_RESULT_FIELD1_LEN;
  tstrncpy((*pSchema)[0].name, "Virtual Table", TSDB_COL_NAME_LEN);

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = SHOW_CREATE_TB_RESULT_FIELD2_LEN;
  tstrncpy((*pSchema)[1].name, "Create Virtual Table", TSDB_COL_NAME_LEN);

  return TSDB_CODE_SUCCESS;
}

static int32_t extractShowCreateViewResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = SHOW_CREATE_VIEW_RESULT_COLS;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return terrno;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = SHOW_CREATE_VIEW_RESULT_FIELD1_LEN;
  tstrncpy((*pSchema)[0].name, "View", TSDB_COL_NAME_LEN);

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = SHOW_CREATE_VIEW_RESULT_FIELD2_LEN;
  tstrncpy((*pSchema)[1].name, "Create View", TSDB_COL_NAME_LEN);

  return TSDB_CODE_SUCCESS;
}

static int32_t extractShowVariablesResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = SHOW_LOCAL_VARIABLES_RESULT_COLS;  // SHOW_VARIABLES_RESULT_COLS
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return terrno;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = TSDB_CONFIG_OPTION_LEN;
  tstrncpy((*pSchema)[0].name, "name", TSDB_COL_NAME_LEN);

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = TSDB_CONFIG_PATH_LEN;
  tstrncpy((*pSchema)[1].name, "value", TSDB_COL_NAME_LEN);

  (*pSchema)[2].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[2].bytes = TSDB_CONFIG_SCOPE_LEN;
  tstrncpy((*pSchema)[2].name, "scope", TSDB_COL_NAME_LEN);

  (*pSchema)[3].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[3].bytes = TSDB_CONFIG_CATEGORY_LEN;
  strcpy((*pSchema)[3].name, "category");

  (*pSchema)[4].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[4].bytes = TSDB_CONFIG_INFO_LEN;
  strcpy((*pSchema)[4].name, "info");

  return TSDB_CODE_SUCCESS;
}

static int32_t extractCompactDbResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = COMPACT_DB_RESULT_COLS;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return terrno;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = COMPACT_DB_RESULT_FIELD1_LEN;
  tstrncpy((*pSchema)[0].name, "result", TSDB_COL_NAME_LEN);

  (*pSchema)[1].type = TSDB_DATA_TYPE_INT;
  (*pSchema)[1].bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  tstrncpy((*pSchema)[1].name, "id", TSDB_COL_NAME_LEN);

  (*pSchema)[2].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[2].bytes = COMPACT_DB_RESULT_FIELD3_LEN;
  tstrncpy((*pSchema)[2].name, "reason", TSDB_COL_NAME_LEN);

  return TSDB_CODE_SUCCESS;
}

static int32_t extractScanDbResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = SCAN_DB_RESULT_COLS;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return terrno;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = SCAN_DB_RESULT_FIELD1_LEN;
  tstrncpy((*pSchema)[0].name, "result", TSDB_COL_NAME_LEN);

  (*pSchema)[1].type = TSDB_DATA_TYPE_INT;
  (*pSchema)[1].bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  tstrncpy((*pSchema)[1].name, "id", TSDB_COL_NAME_LEN);

  (*pSchema)[2].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[2].bytes = SCAN_DB_RESULT_FIELD3_LEN;
  tstrncpy((*pSchema)[2].name, "reason", TSDB_COL_NAME_LEN);

  return TSDB_CODE_SUCCESS;
}

int32_t extractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema, SExtSchema** ppExtSchemas) {
  if (NULL == pRoot) {
    return TSDB_CODE_SUCCESS;
  }

  switch (nodeType(pRoot)) {
    case QUERY_NODE_SELECT_STMT:
    case QUERY_NODE_SET_OPERATOR:
      return extractQueryResultSchema(getProjectList(pRoot), numOfCols, pSchema, ppExtSchemas);
    case QUERY_NODE_EXPLAIN_STMT:
      return extractExplainResultSchema(numOfCols, pSchema);
    case QUERY_NODE_DESCRIBE_STMT: {
      SDescribeStmt* pNode = (SDescribeStmt*)pRoot;
      return extractDescribeResultSchema(pNode->pMeta, numOfCols, pSchema);
    }
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
      return extractShowCreateDatabaseResultSchema(numOfCols, pSchema);
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      return extractShowCreateTableResultSchema(numOfCols, pSchema);
    case QUERY_NODE_SHOW_CREATE_VTABLE_STMT:
      return extractShowCreateVTableResultSchema(numOfCols, pSchema);
    case QUERY_NODE_SHOW_CREATE_VIEW_STMT:
      return extractShowCreateViewResultSchema(numOfCols, pSchema);
    case QUERY_NODE_SHOW_CREATE_RSMA_STMT:
      return extractShowCreateRsmaResultSchema(numOfCols, pSchema);
    case QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT:
    case QUERY_NODE_SHOW_VARIABLES_STMT:
      return extractShowVariablesResultSchema(numOfCols, pSchema);
    case QUERY_NODE_COMPACT_DATABASE_STMT:
    case QUERY_NODE_COMPACT_VGROUPS_STMT:
    case QUERY_NODE_ROLLUP_DATABASE_STMT:
    case QUERY_NODE_ROLLUP_VGROUPS_STMT:
    case QUERY_NODE_TRIM_DATABASE_STMT:
      return extractCompactDbResultSchema(numOfCols, pSchema);
    case QUERY_NODE_SCAN_DATABASE_STMT:
    case QUERY_NODE_SCAN_VGROUPS_STMT:
      return extractScanDbResultSchema(numOfCols, pSchema);
    default:
      break;
  }

  return TSDB_CODE_FAILED;
}

static int32_t createStarCol(SNode** ppNode) {
  SColumnNode* pCol = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return code;
  }
  tstrncpy(pCol->colName, "*", TSDB_COL_NAME_LEN);
  *ppNode = (SNode*)pCol;
  return code;
}

static int32_t createProjectCol(const char* pProjCol, SNode** ppNode) {
  SColumnNode* pCol = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return code;
  }
  snprintf(pCol->colName, sizeof(pCol->colName), "%s", pProjCol);
  *ppNode = (SNode*)pCol;
  return code;
}

static int32_t createProjectCols(int32_t ncols, const char* const pCols[], SNodeList** pResList) {
  SNodeList* pProjections = NULL;
  int32_t    code = TSDB_CODE_SUCCESS;
  if (0 == ncols) {
    SNode* pStar = NULL;
    code = createStarCol(&pStar);
    if (TSDB_CODE_SUCCESS != code) return code;
    code = nodesListMakeStrictAppend(&pProjections, pStar);
    if (TSDB_CODE_SUCCESS == code)
      *pResList = pProjections;
    else
      nodesDestroyList(pProjections);
    return code;
  }
  for (int32_t i = 0; i < ncols; ++i) {
    SNode* pPrjCol = NULL;
    code = createProjectCol(pCols[i], (SNode**)&pPrjCol);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(pProjections);
      break;
    }
    code = nodesListMakeStrictAppend(&pProjections, pPrjCol);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(pProjections);
      break;
    }
  }
  if (TSDB_CODE_SUCCESS == code) *pResList = pProjections;
  return code;
}

static int32_t createSimpleSelectStmtImpl(const char* pDb, const char* pTable, SNodeList* pProjectionList,
                                          SSelectStmt** pStmt) {
  SSelectStmt* pSelect = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_SELECT_STMT, (SNode**)&pSelect);
  if (NULL == pSelect) {
    return code;
  }
  snprintf(pSelect->stmtName, TSDB_TABLE_NAME_LEN, "%p", pSelect);

  SRealTableNode* pRealTable = NULL;
  code = nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)&pRealTable);
  if (NULL == pRealTable) {
    nodesDestroyNode((SNode*)pSelect);
    return code;
  }
  snprintf(pRealTable->table.dbName, sizeof(pRealTable->table.dbName), "%s", pDb);
  snprintf(pRealTable->table.tableName, sizeof(pRealTable->table.tableName), "%s", pTable);
  snprintf(pRealTable->table.tableAlias, sizeof(pRealTable->table.tableAlias), "%s", pTable);
  pSelect->pFromTable = (SNode*)pRealTable;
  pSelect->pProjectionList = pProjectionList;

  *pStmt = pSelect;

  return TSDB_CODE_SUCCESS;
}

static int32_t createSimpleSelectStmtFromCols(const char* pDb, const char* pTable, int32_t numOfProjs,
                                              const char* const pProjCol[], SSelectStmt** pStmt) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SNodeList* pProjectionList = NULL;
  if (numOfProjs >= 0) {
    PAR_ERR_RET(createProjectCols(numOfProjs, pProjCol, &pProjectionList));
  }

  return createSimpleSelectStmtImpl(pDb, pTable, pProjectionList, pStmt);
}

static int32_t createSimpleSelectStmtFromProjList(const char* pDb, const char* pTable, SNodeList* pProjectionList,
                                                  SSelectStmt** pStmt) {
  return createSimpleSelectStmtImpl(pDb, pTable, pProjectionList, pStmt);
}

static int32_t createSelectStmtForShow(ENodeType showType, SSelectStmt** pStmt) {
  const SSysTableShowAdapter* pShow = &sysTableShowAdapter[showType - SYSTABLE_SHOW_TYPE_OFFSET];
  return createSimpleSelectStmtFromCols(pShow->pDbName, pShow->pTableName, pShow->numOfShowCols, pShow->pShowCols,
                                        pStmt);
}

static int32_t createSelectStmtForShowTableDist(SShowTableDistributedStmt* pStmt, SSelectStmt** pOutput) {
  return createSimpleSelectStmtFromCols(pStmt->dbName, pStmt->tableName, 0, NULL, pOutput);
}
static int32_t createSelectStmtForShowDBUsage(SShowStmt* pStmt, SSelectStmt** pOutput) {
  int32_t                     type = nodeType(pStmt);
  const SSysTableShowAdapter* pShow = &sysTableShowAdapter[type - SYSTABLE_SHOW_TYPE_OFFSET];
  return createSimpleSelectStmtFromCols(pShow->pDbName, pShow->pTableName, pShow->numOfShowCols, pShow->pShowCols,
                                        pOutput);
}

static int32_t createTsOperatorNode(EOperatorType opType, const SNode* pRight, SNode** pOp) {
  if (NULL == pRight) {
    return TSDB_CODE_SUCCESS;
  }

  SOperatorNode* pOper = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOper);
  if (NULL == pOper) {
    return code;
  }

  pOper->opType = opType;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pOper->pLeft);
  ((SColumnNode*)pOper->pLeft)->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  ((SColumnNode*)pOper->pLeft)->isPrimTs = true;
  snprintf(((SColumnNode*)pOper->pLeft)->colName, sizeof(((SColumnNode*)pOper->pLeft)->colName), "%s", "_c0");

  code = nodesCloneNode(pRight, (SNode**)&pOper->pRight);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pOper);
    return code;
  }

  *pOp = (SNode*)pOper;
  return TSDB_CODE_SUCCESS;
}

static int32_t createOperatorNode(EOperatorType opType, const char* pColName, const SNode* pRight, SNode** pOp) {
  if (NULL == pRight) {
    return TSDB_CODE_SUCCESS;
  }

  SOperatorNode* pOper = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOper);
  if (NULL == pOper) {
    return code;
  }

  pOper->opType = opType;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pOper->pLeft);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pOper);
    return code;
  }
  code = nodesCloneNode(pRight, (SNode**)&pOper->pRight);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pOper);
    return code;
  }
  snprintf(((SColumnNode*)pOper->pLeft)->colName, sizeof(((SColumnNode*)pOper->pLeft)->colName), "%s", pColName);

  *pOp = (SNode*)pOper;
  return TSDB_CODE_SUCCESS;
}

static int32_t createParOperatorNode(EOperatorType opType, const char* pLeftCol, const char* pRightCol,
                                     SNode** ppResOp) {
  SOperatorNode* pOper = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOper);
  if (TSDB_CODE_SUCCESS != code) return code;

  pOper->opType = opType;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pOper->pLeft);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pOper);
    return code;
  }
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pOper->pRight);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pOper);
    return code;
  }
  tstrncpy(((SColumnNode*)pOper->pLeft)->colName, pLeftCol, TSDB_COL_NAME_LEN);
  tstrncpy(((SColumnNode*)pOper->pRight)->colName, pRightCol, TSDB_COL_NAME_LEN);

  *ppResOp = (SNode*)pOper;
  return TSDB_CODE_SUCCESS;
}

static int32_t createOperatorNodeByNode(EOperatorType opType, const SNode* pLeft, const SNode* pRight, SNode** pOp) {
  if (NULL == pRight) {
    return TSDB_CODE_SUCCESS;
  }

  SOperatorNode* pOper = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOper);
  if (NULL == pOper) {
    return code;
  }

  pOper->opType = opType;
  code = nodesCloneNode(pLeft, (SNode**)&pOper->pLeft);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pOper);
    return code;
  }
  code = nodesCloneNode(pRight, (SNode**)&pOper->pRight);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pOper);
    return code;
  }

  *pOp = (SNode*)pOper;
  return TSDB_CODE_SUCCESS;
}

static int32_t createIsOperatorNode(EOperatorType opType, const char* pColName, SNode** pOp) {
  SOperatorNode* pOper = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOper);
  if (NULL == pOper) {
    return code;
  }

  pOper->opType = opType;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pOper->pLeft);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pOper);
    return code;
  }
  pOper->pRight = NULL;

  snprintf(((SColumnNode*)pOper->pLeft)->colName, sizeof(((SColumnNode*)pOper->pLeft)->colName), "%s", pColName);

  *pOp = (SNode*)pOper;
  return TSDB_CODE_SUCCESS;
}

static int32_t createIsOperatorNodeByNode(EOperatorType opType, SNode* pNode, SNode** pOp) {
  SOperatorNode* pOper = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOper);
  if (NULL == pOper) {
    return code;
  }

  pOper->opType = opType;
  code = nodesCloneNode(pNode, (SNode**)&pOper->pLeft);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pOper);
    return code;
  }
  pOper->pRight = NULL;

  *pOp = (SNode*)pOper;
  return TSDB_CODE_SUCCESS;
}

static const char* getTbNameColName(ENodeType type) {
  const char* colName;
  switch (type) {
    case QUERY_NODE_SHOW_VIEWS_STMT:
      colName = "view_name";
      break;
    case QUERY_NODE_SHOW_STABLES_STMT:
      colName = "stable_name";
      break;
    default:
      colName = "table_name";
      break;
  }
  return colName;
}

static int32_t createLogicCondNode(SNode** pCond1, SNode** pCond2, SNode** pCond, ELogicConditionType logicCondType) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SLogicConditionNode* pCondition = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pCondition);
  if (NULL == pCondition) {
    return code;
  }
  pCondition->condType = logicCondType;
  code = nodesMakeList(&pCondition->pParameterList);
  if (NULL == pCondition->pParameterList) {
    nodesDestroyNode((SNode*)pCondition);
    return code;
  }
  code = nodesListAppend(pCondition->pParameterList, *pCond1);
  if (TSDB_CODE_SUCCESS == code) {
    *pCond1 = NULL;
    code = nodesListAppend(pCondition->pParameterList, *pCond2);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pCond2 = NULL;
    *pCond = (SNode*)pCondition;
  } else {
    nodesDestroyNode((SNode*)pCondition);
  }

  return code;
}

static int32_t insertCondIntoSelectStmt(SSelectStmt* pSelect, SNode** pCond) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pSelect->pWhere == NULL) {
    pSelect->pWhere = *pCond;
    *pCond = NULL;
  } else {
    SNodeList* pLogicCondListWhere = NULL;
    SNodeList* pLogicCondList2 = NULL;
    if (nodeType(pSelect->pWhere) == QUERY_NODE_LOGIC_CONDITION &&
        ((SLogicConditionNode*)pSelect->pWhere)->condType == LOGIC_COND_TYPE_AND) {
      pLogicCondListWhere = ((SLogicConditionNode*)pSelect->pWhere)->pParameterList;
      ((SLogicConditionNode*)pSelect->pWhere)->pParameterList = NULL;
    } else {
      code = nodesListMakeAppend(&pLogicCondListWhere, pSelect->pWhere);
      if (TSDB_CODE_SUCCESS == code) {
        pSelect->pWhere = NULL;
      }
    }

    if (TSDB_CODE_SUCCESS == code) {
      if (nodeType(*pCond) == QUERY_NODE_LOGIC_CONDITION &&
          ((SLogicConditionNode*)(*pCond))->condType == LOGIC_COND_TYPE_AND) {
        pLogicCondList2 = ((SLogicConditionNode*)(*pCond))->pParameterList;
        ((SLogicConditionNode*)(*pCond))->pParameterList = NULL;
      } else {
        code = nodesListMakeAppend(&pLogicCondList2, (*pCond));
        if (TSDB_CODE_SUCCESS == code) {
          *pCond = NULL;
        }
      }
    }

    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListAppendList(pLogicCondListWhere, pLogicCondList2);
    }

    if (TSDB_CODE_SUCCESS == code) {
      SLogicConditionNode* pWhere = NULL;
      code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pWhere);
      if (pWhere) {
        pWhere->condType = LOGIC_COND_TYPE_AND;
        pWhere->pParameterList = pLogicCondListWhere;

        pSelect->pWhere = (SNode*)pWhere;
      }
    }
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(pLogicCondListWhere);
      nodesDestroyList(pLogicCondList2);
    }
  }
  return code;
}

static int32_t addShowUserDatabasesCond(SSelectStmt* pSelect) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SNode*      pNameCond1 = NULL;
  SNode*      pNameCond2 = NULL;
  SNode*      pNameCond = NULL;
  SValueNode *pValNode1 = NULL, *pValNode2 = NULL;
  code = nodesMakeValueNodeFromString(TSDB_INFORMATION_SCHEMA_DB, &pValNode1);

  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMakeValueNodeFromString(TSDB_PERFORMANCE_SCHEMA_DB, &pValNode2);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createOperatorNode(OP_TYPE_NOT_EQUAL, "name", (SNode*)pValNode1, &pNameCond1);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createOperatorNode(OP_TYPE_NOT_EQUAL, "name", (SNode*)pValNode2, &pNameCond2);
  }
  nodesDestroyNode((SNode*)pValNode2);
  nodesDestroyNode((SNode*)pValNode1);
  if (TSDB_CODE_SUCCESS == code) code = createLogicCondNode(&pNameCond1, &pNameCond2, &pNameCond, LOGIC_COND_TYPE_AND);

  if (TSDB_CODE_SUCCESS == code) code = insertCondIntoSelectStmt(pSelect, &pNameCond);

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNameCond1);
    nodesDestroyNode(pNameCond2);
    nodesDestroyNode(pNameCond);
  }
  return code;
}

static int32_t addShowSystemDatabasesCond(SSelectStmt* pSelect) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SNode*      pNameCond1 = NULL;
  SNode*      pNameCond2 = NULL;
  SValueNode *pValNode1 = NULL, *pValNode2 = NULL;
  SNode*      pNameCond = NULL;
  code = nodesMakeValueNodeFromString(TSDB_INFORMATION_SCHEMA_DB, &pValNode1);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMakeValueNodeFromString(TSDB_PERFORMANCE_SCHEMA_DB, &pValNode2);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createOperatorNode(OP_TYPE_EQUAL, "name", (SNode*)pValNode1, &pNameCond1);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createOperatorNode(OP_TYPE_EQUAL, "name", (SNode*)pValNode2, &pNameCond2);
  }
  nodesDestroyNode((SNode*)pValNode2);
  nodesDestroyNode((SNode*)pValNode1);
  if (TSDB_CODE_SUCCESS == code) {
    code = createLogicCondNode(&pNameCond1, &pNameCond2, &pNameCond, LOGIC_COND_TYPE_OR);
  }

  if (TSDB_CODE_SUCCESS == code) code = insertCondIntoSelectStmt(pSelect, &pNameCond);

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNameCond1);
    nodesDestroyNode(pNameCond2);
    nodesDestroyNode(pNameCond);
  }
  return code;
}

static int32_t addShowNormalTablesCond(SSelectStmt* pSelect, bool equal) {
  SNode*      pTypeCond = NULL;
  int32_t     code = TSDB_CODE_SUCCESS;
  SValueNode* pValNode1 = NULL;
  code = nodesMakeValueNodeFromString("NORMAL_TABLE", &pValNode1);

  if (TSDB_CODE_SUCCESS == code)
    code = createOperatorNode(equal ? OP_TYPE_EQUAL : OP_TYPE_NOT_EQUAL, "type", (SNode*)pValNode1, &pTypeCond);

  nodesDestroyNode((SNode*)pValNode1);

  if (TSDB_CODE_SUCCESS == code) code = insertCondIntoSelectStmt(pSelect, &pTypeCond);
  if (TSDB_CODE_SUCCESS != code) nodesDestroyNode(pTypeCond);
  return code;
}

static int32_t addShowChildTablesCond(SSelectStmt* pSelect, bool equal) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SNode*      pTypeCond = NULL;
  SValueNode* pValNode1 = NULL;
  code = nodesMakeValueNodeFromString("CHILD_TABLE", &pValNode1);

  if (TSDB_CODE_SUCCESS == code)
    code = createOperatorNode(equal ? OP_TYPE_EQUAL : OP_TYPE_NOT_EQUAL, "type", (SNode*)pValNode1, &pTypeCond);

  nodesDestroyNode((SNode*)pValNode1);

  if (TSDB_CODE_SUCCESS == code) code = insertCondIntoSelectStmt(pSelect, &pTypeCond);
  if (TSDB_CODE_SUCCESS != code) nodesDestroyNode(pTypeCond);
  return code;
}

static int32_t addShowVirtualNormalTablesCond(SSelectStmt* pSelect, bool equal) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SNode*      pTypeCond = NULL;
  SValueNode* pValNode1 = NULL;
  PAR_ERR_JRET(nodesMakeValueNodeFromString("VIRTUAL_NORMAL_TABLE", &pValNode1));

  PAR_ERR_JRET(createOperatorNode(equal ? OP_TYPE_EQUAL : OP_TYPE_NOT_EQUAL, "type", (SNode*)pValNode1, &pTypeCond));

  PAR_ERR_JRET(insertCondIntoSelectStmt(pSelect, &pTypeCond));

_return:
  nodesDestroyNode((SNode*)pValNode1);
  nodesDestroyNode(pTypeCond);
  return code;
}

static int32_t addShowVirtualChildTablesCond(SSelectStmt* pSelect, bool equal) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SNode*      pTypeCond = NULL;
  SValueNode* pValNode1 = NULL;
  PAR_ERR_JRET(nodesMakeValueNodeFromString("VIRTUAL_CHILD_TABLE", &pValNode1));

  PAR_ERR_JRET(createOperatorNode(equal ? OP_TYPE_EQUAL : OP_TYPE_NOT_EQUAL, "type", (SNode*)pValNode1, &pTypeCond));

  PAR_ERR_JRET(insertCondIntoSelectStmt(pSelect, &pTypeCond));

_return:
  nodesDestroyNode((SNode*)pValNode1);
  nodesDestroyNode(pTypeCond);
  return code;
}

static int32_t addShowVirtualSuperTablesCond(SSelectStmt* pSelect, bool equal) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SNode*      pTypeCond = NULL;
  SValueNode* pValNode1 = NULL;
  PAR_ERR_JRET(nodesMakeValueNodeFromBool(true, &pValNode1));

  PAR_ERR_JRET(
      createOperatorNode(equal ? OP_TYPE_EQUAL : OP_TYPE_NOT_EQUAL, "isvirtual", (SNode*)pValNode1, &pTypeCond));

  PAR_ERR_JRET(insertCondIntoSelectStmt(pSelect, &pTypeCond));

_return:
  nodesDestroyNode((SNode*)pValNode1);
  nodesDestroyNode(pTypeCond);
  return code;
}

static int32_t addShowKindCond(const SShowStmt* pShow, SSelectStmt* pSelect) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (pShow->type) {
    case QUERY_NODE_SHOW_DATABASES_STMT: {
      if (pShow->showKind == SHOW_KIND_DATABASES_USER) {
        PAR_ERR_RET(addShowUserDatabasesCond(pSelect));
      } else if (pShow->showKind == SHOW_KIND_DATABASES_SYSTEM) {
        PAR_ERR_RET(addShowSystemDatabasesCond(pSelect));
      } else {
        PAR_RET(TSDB_CODE_SUCCESS);
      }
      break;
    }
    case QUERY_NODE_SHOW_TABLES_STMT: {
      if (pShow->showKind == SHOW_KIND_TABLES_NORMAL) {
        PAR_ERR_RET(addShowNormalTablesCond(pSelect, true));
      } else if (pShow->showKind == SHOW_KIND_TABLES_CHILD) {
        PAR_ERR_RET(addShowChildTablesCond(pSelect, true));
      } else {
        PAR_ERR_RET(addShowVirtualNormalTablesCond(pSelect, false));
        PAR_ERR_RET(addShowVirtualChildTablesCond(pSelect, false));
      }
      break;
    }
    case QUERY_NODE_SHOW_VTABLES_STMT: {
      if (pShow->showKind == SHOW_KIND_TABLES_NORMAL) {
        PAR_ERR_RET(addShowVirtualNormalTablesCond(pSelect, true));
      } else if (pShow->showKind == SHOW_KIND_TABLES_CHILD) {
        PAR_ERR_RET(addShowVirtualChildTablesCond(pSelect, true));
      } else {
        PAR_ERR_RET(addShowNormalTablesCond(pSelect, false));
        PAR_ERR_RET(addShowChildTablesCond(pSelect, false));
      }
      break;
    }
    case QUERY_NODE_SHOW_STABLES_STMT: {
      if (pShow->showKind == SHOW_KIND_TABLES_NORMAL) {
        PAR_ERR_RET(addShowVirtualSuperTablesCond(pSelect, false));
      } else if (pShow->showKind == SHOW_KIND_TABLES_VIRTUAL) {
        PAR_ERR_RET(addShowVirtualSuperTablesCond(pSelect, true));
      } else {
        PAR_RET(TSDB_CODE_SUCCESS);
      }
      break;
    }
    default:
      PAR_RET(TSDB_CODE_SUCCESS);
  }

  return code;
}

static int32_t createShowCondition(const SShowStmt* pShow, SSelectStmt* pSelect) {
  SNode* pDbCond = NULL;
  SNode* pTbCond = NULL;
  if (TSDB_CODE_SUCCESS != createOperatorNode(OP_TYPE_EQUAL, "db_name", pShow->pDbName, &pDbCond) ||
      TSDB_CODE_SUCCESS !=
          createOperatorNode(pShow->tableCondType, getTbNameColName(nodeType(pShow)), pShow->pTbName, &pTbCond)) {
    nodesDestroyNode(pDbCond);
    nodesDestroyNode(pTbCond);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (NULL != pDbCond && NULL != pTbCond) {
    if (TSDB_CODE_SUCCESS != createLogicCondNode(&pDbCond, &pTbCond, &pSelect->pWhere, LOGIC_COND_TYPE_AND)) {
      nodesDestroyNode(pDbCond);
      nodesDestroyNode(pTbCond);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    pSelect->pWhere = (NULL == pDbCond ? pTbCond : pDbCond);
  }

  int32_t code = addShowKindCond(pShow, pSelect);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (NULL != pShow->pDbName) {
    tstrncpy(((SRealTableNode*)pSelect->pFromTable)->qualDbName, ((SValueNode*)pShow->pDbName)->literal,
             TSDB_DB_NAME_LEN);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t rewriteShow(STranslateContext* pCxt, SQuery* pQuery) {
  SSelectStmt* pStmt = NULL;
  int32_t      code = createSelectStmtForShow(nodeType(pQuery->pRoot), &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = createShowCondition((SShowStmt*)pQuery->pRoot, pStmt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->showRewrite = true;
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  } else {
    nodesDestroyNode((SNode*)pStmt);
  }
  return code;
}

static int32_t rewriteShowStreams(STranslateContext* pCxt, SQuery* pQuery) {
  SNode* pDbNode = ((SShowStmt*)pQuery->pRoot)->pDbName;
  if (nodeType(pDbNode) == QUERY_NODE_VALUE) {
    SArray* pVgs = NULL;
    int32_t code = getDBVgInfo(pCxt, ((SValueNode*)pDbNode)->literal, &pVgs);
    taosArrayDestroy(pVgs);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return rewriteShow(pCxt, pQuery);
}

static int32_t rewriteShowVtables(STranslateContext* pCxt, SQuery* pQuery) {
  return rewriteShow(pCxt, pQuery);
}

static int32_t checkShowVgroups(STranslateContext* pCxt, SShowStmt* pShow) {
  // just to verify whether the database exists
  SDbCfgInfo dbCfg = {0};
  return getDBCfg(pCxt, ((SValueNode*)pShow->pDbName)->literal, &dbCfg);
}

static int32_t rewriteShowVgroups(STranslateContext* pCxt, SQuery* pQuery) {
  int32_t code = checkShowVgroups(pCxt, (SShowStmt*)pQuery->pRoot);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteShow(pCxt, pQuery);
  }
  return code;
}

static int32_t checkShowTags(STranslateContext* pCxt, const SShowStmt* pShow) {
  int32_t     code = 0;
  SName       name = {0};
  STableMeta* pTableMeta = NULL;
  toName(pCxt->pParseCxt->acctId, ((SValueNode*)pShow->pDbName)->literal, ((SValueNode*)pShow->pTbName)->literal,
         &name);
  code = getTargetMeta(pCxt, &name, &pTableMeta, true);
  if (TSDB_CODE_SUCCESS != code) {
    code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GET_META_ERROR, tstrerror(code));
    goto _exit;
  }
  if ((TSDB_SUPER_TABLE != pTableMeta->tableType && TSDB_CHILD_TABLE != pTableMeta->tableType) &&
      (pTableMeta->virtualStb != 1 && pTableMeta->tableType != TSDB_VIRTUAL_CHILD_TABLE)) {
    code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAGS_PC,
                                "The _TAGS pseudo column can only be used for child table and super table queries");
    goto _exit;
  }

_exit:
  taosMemoryFreeClear(pTableMeta);
  return code;
}

static int32_t rewriteShowTags(STranslateContext* pCxt, SQuery* pQuery) {
  int32_t code = checkShowTags(pCxt, (SShowStmt*)pQuery->pRoot);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteShow(pCxt, pQuery);
  }
  return code;
}

static int32_t createTagsFunction(SFunctionNode** ppNode) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  tstrncpy(pFunc->functionName, "_tags", TSDB_FUNC_NAME_LEN);
  *ppNode = pFunc;
  return code;
}

static int32_t createShowTableTagsProjections(SNodeList** pProjections, SNodeList** pTags) {
  if (NULL != *pTags) {
    TSWAP(*pProjections, *pTags);
    return TSDB_CODE_SUCCESS;
  }
  SFunctionNode* pTbNameFunc = NULL;
  int32_t        code = createTbnameFunction(&pTbNameFunc);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(pProjections, (SNode*)pTbNameFunc);
  }
  SFunctionNode* pTagsFunc = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = createTagsFunction(&pTagsFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(*pProjections, (SNode*)pTagsFunc);
  }
  return code;
}

static int32_t rewriteShowStableTags(STranslateContext* pCxt, SQuery* pQuery) {
  SShowTableTagsStmt* pShow = (SShowTableTagsStmt*)pQuery->pRoot;
  SSelectStmt*        pSelect = NULL;
  int32_t             code = createSimpleSelectStmtFromCols(((SValueNode*)pShow->pDbName)->literal,
                                                            ((SValueNode*)pShow->pTbName)->literal, -1, NULL, &pSelect);
  if (TSDB_CODE_SUCCESS == code) {
    code = createShowTableTagsProjections(&pSelect->pProjectionList, &pShow->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->showRewrite = true;
    pQuery->showRewrite = true;
    pSelect->tagScan = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pSelect;
  } else {
    nodesDestroyNode((SNode*)pSelect);
  }
  return code;
}

static int32_t rewriteShowDnodeVariables(STranslateContext* pCxt, SQuery* pQuery) {
  SShowDnodeVariablesStmt* pStmt = (SShowDnodeVariablesStmt*)pQuery->pRoot;
  SNode*                   pDnodeCond = NULL;
  SNode*                   pLikeCond = NULL;
  SSelectStmt*             pSelect = NULL;
  int32_t                  code = createSelectStmtForShow(nodeType(pQuery->pRoot), &pSelect);
  if (TSDB_CODE_SUCCESS == code) {
    code = createOperatorNode(OP_TYPE_EQUAL, "dnode_id", pStmt->pDnodeId, &pDnodeCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createOperatorNode(OP_TYPE_LIKE, "name", pStmt->pLikePattern, &pLikeCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pLikeCond) {
      code = createLogicCondNode(&pDnodeCond, &pLikeCond, &pSelect->pWhere, LOGIC_COND_TYPE_AND);
    } else {
      pSelect->pWhere = pDnodeCond;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->showRewrite = true;
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pSelect;
  }
  return code;
}

static int32_t rewriteShowVnodes(STranslateContext* pCxt, SQuery* pQuery) {
  SShowVnodesStmt* pShow = (SShowVnodesStmt*)(pQuery->pRoot);
  SSelectStmt*     pStmt = NULL;
  int32_t          code = createSelectStmtForShow(QUERY_NODE_SHOW_VNODES_STMT, &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pShow->pDnodeId) {
      code = createOperatorNode(OP_TYPE_EQUAL, "dnode_id", pShow->pDnodeId, &pStmt->pWhere);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->showRewrite = true;
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  }
  return code;
}

static int32_t createBlockDistInfoFunc(SFunctionNode** ppNode) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }

  tstrncpy(pFunc->functionName, "_block_dist_info", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.aliasName, "_block_dist_info", TSDB_COL_NAME_LEN);
  *ppNode = pFunc;
  return code;
}

static int32_t createBlockDistFunc(SFunctionNode** ppNode) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }

  tstrncpy(pFunc->functionName, "_block_dist", TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.aliasName, "_block_dist", TSDB_COL_NAME_LEN);
  SFunctionNode* pFuncNew = NULL;
  code = createBlockDistInfoFunc(&pFuncNew);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pFunc->pParameterList, (SNode*)pFuncNew);
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pFunc);
    return code;
  }
  *ppNode = pFunc;
  return code;
}

static int32_t rewriteShowTableDist(STranslateContext* pCxt, SQuery* pQuery) {
  SSelectStmt* pStmt = NULL;
  int32_t      code = createSelectStmtForShowTableDist((SShowTableDistributedStmt*)pQuery->pRoot, &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    NODES_DESTORY_LIST(pStmt->pProjectionList);
    SFunctionNode* pFuncNew = NULL;
    code = createBlockDistFunc(&pFuncNew);
    if (TSDB_CODE_SUCCESS == code) code = nodesListMakeStrictAppend(&pStmt->pProjectionList, (SNode*)pFuncNew);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->showRewrite = true;
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  }
  return code;
}

static int32_t createBlockDBUsageInfoFunc(SFunctionNode** ppNode) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }

  strcpy(pFunc->functionName, "_db_usage_info");
  strcpy(pFunc->node.aliasName, "_db_usage_info");
  *ppNode = pFunc;
  return code;
}
static int32_t createDBUsageFunc(SFunctionNode** ppNode) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }

  strcpy(pFunc->functionName, "_db_usage");
  strcpy(pFunc->node.aliasName, "_db_usage");
  SFunctionNode* pFuncNew = NULL;
  code = createBlockDBUsageInfoFunc(&pFuncNew);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pFunc->pParameterList, (SNode*)pFuncNew);
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pFunc);
    return code;
  }
  *ppNode = pFunc;
  return code;
}
static int32_t rewriteShowDBUsage(STranslateContext* pCtx, SQuery* pQuery) {
  SSelectStmt* pStmt = NULL;
  int32_t      code = createSelectStmtForShowDBUsage((SShowStmt*)pQuery->pRoot, &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    NODES_DESTORY_LIST(pStmt->pProjectionList);
    SFunctionNode* pFuncNew = NULL;
    code = createDBUsageFunc(&pFuncNew);
    if (TSDB_CODE_SUCCESS == code) code = nodesListMakeStrictAppend(&pStmt->pProjectionList, (SNode*)pFuncNew);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = createShowCondition((SShowStmt*)pQuery->pRoot, pStmt);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pCtx->showRewrite = true;
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  }
  return code;
}

typedef struct SVgroupCreateTableBatch {
  SVCreateTbBatchReq req;
  SVgroupInfo        info;
  char               dbName[TSDB_DB_NAME_LEN];
} SVgroupCreateTableBatch;

static int32_t buildVirtualTableBatchReq(const SCreateVTableStmt* pStmt, const SVgroupInfo* pVgroupInfo,
                                         SVgroupCreateTableBatch* pBatch) {
  int32_t       code = TSDB_CODE_SUCCESS;
  SVCreateTbReq req = {0};
  SNode*        pCol;
  col_id_t      index = 0;

  req.type = TD_VIRTUAL_NORMAL_TABLE;
  req.name = taosStrdup(pStmt->tableName);
  req.ntb.schemaRow.nCols = LIST_LENGTH(pStmt->pCols);
  req.ntb.schemaRow.version = 1;
  req.ntb.schemaRow.pSchema = taosMemoryCalloc(req.ntb.schemaRow.nCols, sizeof(SSchema));
  if (NULL == req.name || NULL == req.ntb.schemaRow.pSchema) {
    PAR_ERR_JRET(terrno);
  }
  if (pStmt->ignoreExists) {
    req.flags |= TD_CREATE_IF_NOT_EXISTS;
  }
  PAR_ERR_JRET(tInitDefaultSColRefWrapperByCols(&req.colRef, req.ntb.schemaRow.nCols));

  FOREACH(pCol, pStmt->pCols) {
    SColumnDefNode* pColDef = (SColumnDefNode*)pCol;
    SSchema*        pSchema = req.ntb.schemaRow.pSchema + index;
    toSchema(pColDef, index + 1, pSchema);
    if (pColDef->pOptions && ((SColumnOptions*)pColDef->pOptions)->hasRef) {
      PAR_ERR_JRET(setColRef(&req.colRef.pColRef[index], index + 1, ((SColumnOptions*)pColDef->pOptions)->refColumn,
                             ((SColumnOptions*)pColDef->pOptions)->refTable,
                             ((SColumnOptions*)pColDef->pOptions)->refDb));
    }
    ++index;
  }

  pBatch->info = *pVgroupInfo;
  (void)strcpy(pBatch->dbName, pStmt->dbName);
  pBatch->req.pArray = taosArrayInit(1, sizeof(struct SVCreateTbReq));
  if (NULL == pBatch->req.pArray || NULL == taosArrayPush(pBatch->req.pArray, &req)) {
    PAR_ERR_JRET(terrno);
  }

  return code;
_return:
  tdDestroySVCreateTbReq(&req);
  taosArrayDestroy(pBatch->req.pArray);
  return code;
}

static int32_t buildVirtualSubTableBatchReq(const SCreateVSubTableStmt* pStmt, STableMeta* pStbMeta, SArray* tagName,
                                            uint8_t tagNum, const STag* pTag, const SVgroupInfo* pVgroupInfo,
                                            SVgroupCreateTableBatch* pBatch) {
  int32_t       code = TSDB_CODE_SUCCESS;
  SVCreateTbReq req = {0};
  SNode*        pCol;

  req.type = TD_VIRTUAL_CHILD_TABLE;
  req.name = taosStrdup(pStmt->tableName);
  req.ttl = 0;
  req.commentLen = -1;
  req.ctb.suid = pStbMeta->uid;
  req.ctb.tagNum = tagNum;
  req.ctb.stbName = taosStrdup(pStmt->useTableName);
  req.ctb.pTag = (uint8_t*)pTag;
  req.ctb.tagName = taosArrayDup(tagName, NULL);
  if (pStmt->ignoreExists) {
    req.flags |= TD_CREATE_IF_NOT_EXISTS;
  }

  if (!req.name || !req.ctb.stbName || !req.ctb.tagName) {
    PAR_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  PAR_ERR_RET(tInitDefaultSColRefWrapperByCols(&req.colRef, pStbMeta->tableInfo.numOfColumns));

  if (pStmt->pSpecificColRefs) {
    FOREACH(pCol, pStmt->pSpecificColRefs) {
      SColumnRefNode* pColRef = (SColumnRefNode*)pCol;
      col_id_t        schemaIdx = getNormalColSchemaIndex(pStbMeta, pColRef->colName);
      if (schemaIdx == -1) {
        PAR_ERR_JRET(TSDB_CODE_PAR_INVALID_COLUMN);
      }
      const SSchema* pSchema = getTableColumnSchema(pStbMeta) + schemaIdx;
      PAR_ERR_JRET(setColRef(&req.colRef.pColRef[schemaIdx], pSchema->colId, pColRef->refColName, pColRef->refTableName,
                             pColRef->refDbName));
    }
  } else if (pStmt->pColRefs) {
    col_id_t index = 1;  // start from second column, don't set column ref for ts column
    FOREACH(pCol, pStmt->pColRefs) {
      SColumnRefNode* pColRef = (SColumnRefNode*)pCol;
      PAR_ERR_JRET(setColRef(&req.colRef.pColRef[index], index + 1, pColRef->refColName, pColRef->refTableName,
                             pColRef->refDbName));
      index++;
    }
  } else {
    // no column reference.
  }

  pBatch->info = *pVgroupInfo;
  (void)strcpy(pBatch->dbName, pStmt->dbName);
  pBatch->req.pArray = taosArrayInit(1, sizeof(struct SVCreateTbReq));
  if (NULL == pBatch->req.pArray || NULL == taosArrayPush(pBatch->req.pArray, &req)) {
    PAR_ERR_JRET(terrno);
  }

  return code;
_return:
  tdDestroySVCreateTbReq(&req);
  taosArrayDestroy(pBatch->req.pArray);
  return code;
}

static int32_t buildNormalTableBatchReq(const SCreateTableStmt* pStmt, const SVgroupInfo* pVgroupInfo,
                                        SVgroupCreateTableBatch* pBatch) {
  SVCreateTbReq req = {0};
  req.type = TD_NORMAL_TABLE;
  req.name = taosStrdup(pStmt->tableName);
  if (!req.name) {
    tdDestroySVCreateTbReq(&req);
    return terrno;
  }
  req.ttl = pStmt->pOptions->ttl;
  if (pStmt->pOptions->commentNull == false) {
    req.comment = taosStrdup(pStmt->pOptions->comment);
    if (NULL == req.comment) {
      tdDestroySVCreateTbReq(&req);
      return terrno;
    }
    req.commentLen = strlen(pStmt->pOptions->comment);
  } else {
    req.commentLen = -1;
  }
  req.ntb.schemaRow.nCols = LIST_LENGTH(pStmt->pCols);
  req.ntb.schemaRow.version = 1;
  req.ntb.schemaRow.pSchema = taosMemoryCalloc(req.ntb.schemaRow.nCols, sizeof(SSchema));
  if (NULL == req.name || NULL == req.ntb.schemaRow.pSchema) {
    tdDestroySVCreateTbReq(&req);
    return terrno;
  }
  if (pStmt->ignoreExists) {
    req.flags |= TD_CREATE_IF_NOT_EXISTS;
  }
  SNode*   pCol;
  col_id_t index = 0;
  int32_t  code = tInitDefaultSColCmprWrapperByCols(&req.colCmpr, req.ntb.schemaRow.nCols);
  if (TSDB_CODE_SUCCESS != code) {
    tdDestroySVCreateTbReq(&req);
    return code;
  }
  FOREACH(pCol, pStmt->pCols) {
    SColumnDefNode* pColDef = (SColumnDefNode*)pCol;
    SSchema*        pScheam = req.ntb.schemaRow.pSchema + index;
    toSchema(pColDef, index + 1, pScheam);
    if (pColDef->pOptions) {
      req.colCmpr.pColCmpr[index].id = index + 1;
      code = setColCompressByOption(pScheam->type, columnEncodeVal(((SColumnOptions*)pColDef->pOptions)->encode),
                                    columnCompressVal(((SColumnOptions*)pColDef->pOptions)->compress),
                                    columnLevelVal(((SColumnOptions*)pColDef->pOptions)->compressLevel), true,
                                    &req.colCmpr.pColCmpr[index].alg);
      if (code != TSDB_CODE_SUCCESS) {
        tdDestroySVCreateTbReq(&req);
        return code;
      }
    }
    if (IS_DECIMAL_TYPE(pColDef->dataType.type)) {
      if (!req.pExtSchemas) {
        req.pExtSchemas = taosMemoryCalloc(pStmt->pCols->length, sizeof(SExtSchema));
        if (NULL == req.pExtSchemas) {
          tdDestroySVCreateTbReq(&req);
          return terrno;
        }
      }
      req.pExtSchemas[index].typeMod = calcTypeMod(&pColDef->dataType);
    }
    ++index;
  }
  pBatch->info = *pVgroupInfo;
  tstrncpy(pBatch->dbName, pStmt->dbName, TSDB_DB_NAME_LEN);
  pBatch->req.pArray = taosArrayInit(1, sizeof(struct SVCreateTbReq));
  if (NULL == pBatch->req.pArray) {
    tdDestroySVCreateTbReq(&req);
    return terrno;
  }
  if (NULL == taosArrayPush(pBatch->req.pArray, &req)) {
    tdDestroySVCreateTbReq(&req);
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t serializeVgroupCreateTableBatch(SVgroupCreateTableBatch* pTbBatch, SArray* pBufArray) {
  int      tlen;
  SEncoder encoder = {0};

  int32_t ret = 0;
  tEncodeSize(tEncodeSVCreateTbBatchReq, &pTbBatch->req, tlen, ret);
  if (TSDB_CODE_SUCCESS != ret) {
    return ret;
  }
  if (tlen >= TSDB_MAX_MSG_SIZE - sizeof(SMsgHead)) {
    return TSDB_CODE_INVALID_MSG_LEN;
  }

  tlen += sizeof(SMsgHead);
  void* buf = taosMemoryMalloc(tlen);
  if (NULL == buf) {
    return terrno;
  }
  ((SMsgHead*)buf)->vgId = htonl(pTbBatch->info.vgId);
  ((SMsgHead*)buf)->contLen = htonl(tlen);
  void* pBuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  tEncoderInit(&encoder, pBuf, tlen - sizeof(SMsgHead));
  ret = tEncodeSVCreateTbBatchReq(&encoder, &pTbBatch->req);
  tEncoderClear(&encoder);
  if (TSDB_CODE_SUCCESS != ret) {
    taosMemoryFreeClear(buf);
    return ret;
  }

  SVgDataBlocks* pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
  if (NULL == pVgData) {
    taosMemoryFreeClear(buf);
    return terrno;
  }
  pVgData->vg = pTbBatch->info;
  pVgData->pData = buf;
  pVgData->size = tlen;
  pVgData->numOfTables = (int32_t)taosArrayGetSize(pTbBatch->req.pArray);
  if (NULL == taosArrayPush(pBufArray, &pVgData)) {
    ret = terrno;
    taosMemoryFreeClear(buf);
    taosMemoryFreeClear(pVgData);
  }

  return ret;
}

static void destroyCreateTbReqBatch(void* data) {
  SVgroupCreateTableBatch* pTbBatch = (SVgroupCreateTableBatch*)data;
  size_t                   size = taosArrayGetSize(pTbBatch->req.pArray);
  for (int32_t i = 0; i < size; ++i) {
    SVCreateTbReq* pTableReq = taosArrayGet(pTbBatch->req.pArray, i);
    tdDestroySVCreateTbReq(pTableReq);
  }

  taosArrayDestroy(pTbBatch->req.pArray);
}

int32_t rewriteToVnodeModifyOpStmt(SQuery* pQuery, SArray* pBufArray) {
  SVnodeModifyOpStmt* pNewStmt = NULL;
  int32_t             code = nodesMakeNode(QUERY_NODE_VNODE_MODIFY_STMT, (SNode**)&pNewStmt);
  if (pNewStmt == NULL) {
    return code;
  }
  pNewStmt->sqlNodeType = nodeType(pQuery->pRoot);
  pNewStmt->pDataBlocks = pBufArray;
  nodesDestroyNode(pQuery->pRoot);
  pQuery->pRoot = (SNode*)pNewStmt;
  return TSDB_CODE_SUCCESS;
}

static void destroyCreateTbReqArray(SArray* pArray) {
  size_t size = taosArrayGetSize(pArray);
  for (size_t i = 0; i < size; ++i) {
    SVgDataBlocks* pVg = taosArrayGetP(pArray, i);
    taosMemoryFreeClear(pVg->pData);
    taosMemoryFreeClear(pVg);
  }
  taosArrayDestroy(pArray);
}

static int32_t buildCreateTableDataBlock(int32_t acctId, const SCreateTableStmt* pStmt, const SVgroupInfo* pInfo,
                                         SArray** pBufArray) {
  *pBufArray = taosArrayInit(1, POINTER_BYTES);
  if (NULL == *pBufArray) {
    return terrno;
  }

  SVgroupCreateTableBatch tbatch = {0};
  int32_t                 code = buildNormalTableBatchReq(pStmt, pInfo, &tbatch);
  if (TSDB_CODE_SUCCESS == code) {
    code = serializeVgroupCreateTableBatch(&tbatch, *pBufArray);
  }

  destroyCreateTbReqBatch(&tbatch);
  if (TSDB_CODE_SUCCESS != code) {
    destroyCreateTbReqArray(*pBufArray);
  }
  return code;
}

static int32_t rewriteCreateTable(STranslateContext* pCxt, SQuery* pQuery) {
  SCreateTableStmt* pStmt = (SCreateTableStmt*)pQuery->pRoot;

  int32_t     code = checkCreateTable(pCxt, pStmt, false);
  SVgroupInfo info = {0};
  SName       name = {0};
  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);
  if (TSDB_CODE_SUCCESS == code) {
    code = getTableHashVgroupImpl(pCxt, &name, &info);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(&name, pCxt->pTargetTables);
  }
  SArray* pBufArray = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateTableDataBlock(pCxt->pParseCxt->acctId, pStmt, &info, &pBufArray);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
    if (TSDB_CODE_SUCCESS != code) {
      destroyCreateTbReqArray(pBufArray);
    }
  }

  return code;
}

static int32_t addCreateTbReqIntoVgroup(SHashObj* pVgroupHashmap, const char* dbName, uint64_t suid,
                                        const char* sTableName, const char* tableName, SArray* tagName, uint8_t tagNum,
                                        const STag* pTag, int32_t ttl, const char* comment, bool ignoreExists,
                                        SVgroupInfo* pVgInfo) {
  struct SVCreateTbReq req = {0};
  req.type = TD_CHILD_TABLE;
  req.name = taosStrdup(tableName);
  req.ttl = ttl;
  if (comment != NULL) {
    req.comment = taosStrdup(comment);
    req.commentLen = strlen(comment);
  } else {
    req.commentLen = -1;
  }
  req.ctb.suid = suid;
  req.ctb.tagNum = tagNum;
  req.ctb.stbName = taosStrdup(sTableName);
  req.ctb.pTag = (uint8_t*)pTag;
  req.ctb.tagName = taosArrayDup(tagName, NULL);
  if (ignoreExists) {
    req.flags |= TD_CREATE_IF_NOT_EXISTS;
  }

  if (!req.name || !req.ctb.stbName || !req.ctb.tagName || (comment && !req.comment)) {
    tdDestroySVCreateTbReq(&req);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t                  code = TSDB_CODE_SUCCESS;
  SVgroupCreateTableBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId));
  if (pTableBatch == NULL) {
    SVgroupCreateTableBatch tBatch = {0};
    tBatch.info = *pVgInfo;
    tstrncpy(tBatch.dbName, dbName, TSDB_DB_NAME_LEN);

    tBatch.req.pArray = taosArrayInit(4, sizeof(struct SVCreateTbReq));
    if (!tBatch.req.pArray) {
      code = terrno;
    } else if (NULL == taosArrayPush(tBatch.req.pArray, &req)) {
      taosArrayDestroy(tBatch.req.pArray);
      code = terrno;
    } else {
      code = taosHashPut(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId), &tBatch, sizeof(tBatch));
      if (TSDB_CODE_SUCCESS != code) {
        taosArrayDestroy(tBatch.req.pArray);
      }
    }
  } else {  // add to the correct vgroup
    if (NULL == taosArrayPush(pTableBatch->req.pArray, &req)) {
      code = terrno;
    }
  }

  if (TSDB_CODE_SUCCESS != code) {
    tdDestroySVCreateTbReq(&req);
  }

  return code;
}

static int32_t buildKVRowForBindTags(STranslateContext* pCxt, SNodeList* pSpecificTags, SNodeList* pValsOfTags,
                                     STableMeta* pSuperTableMeta, STag** ppTag, SArray* tagName) {
  int32_t numOfTags = getNumOfTags(pSuperTableMeta);
  if (LIST_LENGTH(pValsOfTags) != LIST_LENGTH(pSpecificTags) || numOfTags < LIST_LENGTH(pValsOfTags)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TAGS_NOT_MATCHED);
  }

  SArray* pTagArray = taosArrayInit(LIST_LENGTH(pValsOfTags), sizeof(STagVal));
  if (NULL == pTagArray) {
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  bool        isJson = false;
  SNode *     pTagNode = NULL, *pNode = NULL;
  uint8_t     precision = pSuperTableMeta->tableInfo.precision;
  SToken      token;
  char        tokenBuf[TSDB_MAX_TAGS_LEN];
  const char* tagStr = NULL;
  FORBOTH(pTagNode, pSpecificTags, pNode, pValsOfTags) {
    tagStr = ((SValueNode*)pNode)->literal;
    NEXT_TOKEN_WITH_PREV(tagStr, token);

    SSchema* pSchema = NULL;
    if (TSDB_CODE_SUCCESS == code) {
      if ((pSchema = getTagSchema(pSuperTableMeta, ((SColumnNode*)pTagNode)->colName))) {
        code = checkAndTrimValue(&token, tokenBuf, &pCxt->msgBuf, pSchema->type);
        if (TSDB_CODE_SUCCESS == code && TK_NK_VARIABLE == token.type) {
          code = TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
        }
      } else {
        code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAG_NAME, ((SColumnNode*)pTagNode)->colName);
      }
    }

    if (TSDB_CODE_SUCCESS == code) {
      if (pSchema->type == TSDB_DATA_TYPE_JSON) {
        isJson = true;
      }
      code = parseTagValue(&pCxt->msgBuf, &tagStr, precision, pSchema, &token, tagName, pTagArray, ppTag,
                           pCxt->pParseCxt->timezone, pCxt->pParseCxt->charsetCxt);
    }

    if (TSDB_CODE_SUCCESS == code) {
      NEXT_VALID_TOKEN(tagStr, token);
      if (token.n != 0) {
        code = buildSyntaxErrMsg(&pCxt->msgBuf, "not expected tags values", token.z);
      }
    }

    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  if (TSDB_CODE_SUCCESS == code && !isJson) {
    code = tTagNew(pTagArray, 1, false, ppTag);
  }

  for (int i = 0; i < taosArrayGetSize(pTagArray); ++i) {
    STagVal* p = (STagVal*)taosArrayGet(pTagArray, i);
    if (IS_VAR_DATA_TYPE(p->type)) {
      taosMemoryFreeClear(p->pData);
    }
  }
  taosArrayDestroy(pTagArray);
  return code;
}

static int32_t buildKVRowForAllTags(STranslateContext* pCxt, SNodeList* pValsOfTags, STableMeta* pSuperTableMeta,
                                    STag** ppTag, SArray* tagName) {
  if (getNumOfTags(pSuperTableMeta) != LIST_LENGTH(pValsOfTags)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TAGS_NOT_MATCHED);
  }

  SArray* pTagArray = taosArrayInit(LIST_LENGTH(pValsOfTags), sizeof(STagVal));
  if (NULL == pTagArray) {
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  bool        isJson = false;
  SNode*      pNode;
  uint8_t     precision = pSuperTableMeta->tableInfo.precision;
  SSchema*    pTagSchema = getTableTagSchema(pSuperTableMeta);
  SToken      token;
  char        tokenBuf[TSDB_MAX_TAGS_LEN];
  const char* tagStr = NULL;
  FOREACH(pNode, pValsOfTags) {
    tagStr = ((SValueNode*)pNode)->literal;
    NEXT_TOKEN_WITH_PREV(tagStr, token);

    code = checkAndTrimValue(&token, tokenBuf, &pCxt->msgBuf, pTagSchema->type);
    if (TSDB_CODE_SUCCESS == code && TK_NK_VARIABLE == token.type) {
      code = buildSyntaxErrMsg(&pCxt->msgBuf, "not expected tags values", token.z);
    }

    if (TSDB_CODE_SUCCESS == code) {
      if (pTagSchema->type == TSDB_DATA_TYPE_JSON) {
        isJson = true;
      }
      code = parseTagValue(&pCxt->msgBuf, &tagStr, precision, pTagSchema, &token, tagName, pTagArray, ppTag,
                           pCxt->pParseCxt->timezone, pCxt->pParseCxt->charsetCxt);
    }

    if (TSDB_CODE_SUCCESS == code) {
      NEXT_VALID_TOKEN(tagStr, token);
      if (token.n != 0) {
        code = buildSyntaxErrMsg(&pCxt->msgBuf, "not expected tags values", token.z);
      }
    }

    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    ++pTagSchema;
  }

  if (TSDB_CODE_SUCCESS == code && !isJson) {
    code = tTagNew(pTagArray, 1, false, ppTag);
  }

  for (int32_t i = 0; i < TARRAY_SIZE(pTagArray); ++i) {
    STagVal* p = (STagVal*)TARRAY_GET_ELEM(pTagArray, i);
    if (IS_VAR_DATA_TYPE(p->type)) {
      taosMemoryFreeClear(p->pData);
    }
  }
  taosArrayDestroy(pTagArray);
  return code;
}

static int32_t checkCreateSubTable(STranslateContext* pCxt, SCreateSubTableClause* pStmt) {
  if (0 != strcmp(pStmt->dbName, pStmt->useDbName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_CORRESPONDING_STABLE_ERR);
  }
  if (NULL != strchr(pStmt->tableName, '.')) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME,
                                   "The table name cannot contain '.'");
  }
  if (pStmt->pOptions && (pStmt->pOptions->keep >= 0 || pStmt->pOptions->pKeepNode != NULL)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
                                   "child table cannot set keep duration");
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkCreateVSubTable(STranslateContext* pCxt, SCreateVSubTableStmt* pStmt) {
  if (0 != strcmp(pStmt->dbName, pStmt->useDbName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_CORRESPONDING_STABLE_ERR);
  }
  if (NULL != strchr(pStmt->tableName, '.')) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME,
                                   "The table name cannot contain '.'");
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t rewriteCreateSubTable(STranslateContext* pCxt, SCreateSubTableClause* pStmt, SHashObj* pVgroupHashmap) {
  int32_t code = checkCreateSubTable(pCxt, pStmt);

  STableMeta* pSuperTableMeta = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getTableMeta(pCxt, pStmt->useDbName, pStmt->useTableName, &pSuperTableMeta);
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (pSuperTableMeta->virtualStb) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_MISMATCH_STABLE_TYPE);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    SName name = {0};
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);
    code = collectUseTable(&name, pCxt->pTargetTables);
  }

  STag*   pTag = NULL;
  SArray* tagName = taosArrayInit(8, TSDB_COL_NAME_LEN);

  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pStmt->pSpecificTags) {
      code = buildKVRowForBindTags(pCxt, pStmt->pSpecificTags, pStmt->pValsOfTags, pSuperTableMeta, &pTag, tagName);
    } else {
      code = buildKVRowForAllTags(pCxt, pStmt->pValsOfTags, pSuperTableMeta, &pTag, tagName);
    }
  }

  SVgroupInfo info = {0};
  if (TSDB_CODE_SUCCESS == code) {
    code = getTableHashVgroup(pCxt, pStmt->dbName, pStmt->tableName, &info);
  }
  if (TSDB_CODE_SUCCESS == code) {
    const char* comment = pStmt->pOptions->commentNull ? NULL : pStmt->pOptions->comment;
    code = addCreateTbReqIntoVgroup(pVgroupHashmap, pStmt->dbName, pSuperTableMeta->uid, pStmt->useTableName,
                                    pStmt->tableName, tagName, pSuperTableMeta->tableInfo.numOfTags, pTag,
                                    pStmt->pOptions->ttl, comment, pStmt->ignoreExists, &info);
  } else {
    taosMemoryFree(pTag);
  }

  taosArrayDestroy(tagName);
  taosMemoryFreeClear(pSuperTableMeta);
  return code;
}

static int32_t buildTagIndexForBindTags(SMsgBuf* pMsgBuf, SCreateSubTableFromFileClause* pStmt,
                                        STableMeta* pSuperTableMeta, SArray* aTagIndexs) {
  int32_t code = TSDB_CODE_SUCCESS;

  int32_t  numOfTags = getNumOfTags(pSuperTableMeta);
  SSchema* pSchema = getTableTagSchema(pSuperTableMeta);

  SHashObj* pIdxHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pIdxHash) {
    return terrno;
  }

  bool tbnameFound = false;

  SNode* pTagNode;
  FOREACH(pTagNode, pStmt->pSpecificTags) {
    int32_t idx = -1;

    do {
      if (QUERY_NODE_COLUMN == nodeType(pTagNode)) {
        SColumnNode* pColNode = (SColumnNode*)pTagNode;
        for (int32_t index = 0; index < numOfTags; index++) {
          if (strlen(pSchema[index].name) == strlen(pColNode->colName) &&
              strcmp(pColNode->colName, pSchema[index].name) == 0) {
            idx = index;
            break;
          }
        }

        if (idx < 0) {
          code = generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_INVALID_TAG_NAME, pColNode->colName);
          break;
        }

        if (NULL != taosHashGet(pIdxHash, &idx, sizeof(idx))) {
          code = generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_TAG_NAME_DUPLICATED, pColNode->colName);
          break;
        }
      } else if (QUERY_NODE_FUNCTION == nodeType(pTagNode)) {
        SFunctionNode* funcNode = (SFunctionNode*)pTagNode;
        if (strlen("tbname") != strlen(funcNode->functionName) || strcmp("tbname", funcNode->functionName) != 0) {
          code = generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_INVALID_TAG_NAME, funcNode->functionName);
        }

        idx = numOfTags + 1;
        tbnameFound = true;

        if (NULL != taosHashGet(pIdxHash, &idx, sizeof(idx))) {
          code = generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_TAG_NAME_DUPLICATED, funcNode->functionName);
          break;
        }
      } else {
        code = generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_INVALID_TAG_NAME, "invalid node type");
        break;
      }
    } while (0);

    if (code) break;

    if (taosHashPut(pIdxHash, &idx, sizeof(idx), NULL, 0) != 0) {
      code = terrno;
      goto _OUT;
    }

    if (NULL == taosArrayPush(aTagIndexs, &idx)) {
      code = terrno;
      goto _OUT;
    }
  }

  if (TSDB_CODE_SUCCESS == code && !tbnameFound) {
    code = generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_TBNAME_ERROR);
  }

_OUT:
  taosHashCleanup(pIdxHash);
  return code;
}

typedef struct SParseFileContext {
  SArray*     aTagNames;
  bool        tagNameFilled;
  STableMeta* pStbMeta;
  SArray*     aTagIndexs;

  char    tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW];
  SArray* aCreateTbData;

  // per line
  const char* pSql;
  SArray*     aTagVals;
  STag*       pTag;
  SName       ctbName;
  SVgroupInfo vg;
} SParseFileContext;

static int32_t fillVgroupInfo(SParseContext* pParseCxt, const SName* pName, SVgroupInfo* pVgInfo) {
  SVgroupInfo      vg;
  SRequestConnInfo conn = {.pTrans = pParseCxt->pTransporter,
                           .requestId = pParseCxt->requestId,
                           .requestObjRefId = pParseCxt->requestRid,
                           .mgmtEps = pParseCxt->mgmtEpSet};

  int32_t code = catalogGetTableHashVgroup(pParseCxt->pCatalog, &conn, pName, &vg);
  if (code == TSDB_CODE_SUCCESS) {
    *pVgInfo = vg;
  } else {
    parserError("QID:0x%" PRIx64 ", catalogGetTableHashVgroup error, code:%s, dbName:%s, tbName:%s",
                pParseCxt->requestId, tstrerror(code), pName->dbname, pName->tname);
  }

  return code;
}

static int32_t parseOneStbRow(SMsgBuf* pMsgBuf, SParseFileContext* pParFileCxt, timezone_t tz, void* charsetCxt) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int      sz = taosArrayGetSize(pParFileCxt->aTagIndexs);
  int32_t  numOfTags = getNumOfTags(pParFileCxt->pStbMeta);
  uint8_t  precision = getTableInfo(pParFileCxt->pStbMeta).precision;
  SSchema* pSchemas = getTableTagSchema(pParFileCxt->pStbMeta);
  bool     isJson = false;
  for (int i = 0; i < sz; i++) {
    const char* pSql = pParFileCxt->pSql;

    int32_t pos = 0;
    SToken  token = tStrGetToken(pSql, &pos, true, NULL);
    pParFileCxt->pSql += pos;

    if (TK_NK_RP == token.type) {
      code = generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
      break;
    }

    int16_t index = *(int16_t*)taosArrayGet(pParFileCxt->aTagIndexs, i);
    if (index < numOfTags) {
      // parse tag
      const SSchema* pTagSchema = &pSchemas[index];

      isJson = (pTagSchema->type == TSDB_DATA_TYPE_JSON);
      code = checkAndTrimValue(&token, pParFileCxt->tmpTokenBuf, pMsgBuf, pTagSchema->type);
      if (TSDB_CODE_SUCCESS == code && TK_NK_VARIABLE == token.type) {
        code = buildInvalidOperationMsg(pMsgBuf, "not expected row value");
      }
      if (TSDB_CODE_SUCCESS == code) {
        SArray* aTagNames = pParFileCxt->tagNameFilled ? NULL : pParFileCxt->aTagNames;
        code = parseTagValue(pMsgBuf, &pParFileCxt->pSql, precision, (SSchema*)pTagSchema, &token, aTagNames,
                             pParFileCxt->aTagVals, &pParFileCxt->pTag, tz, charsetCxt);
      }
    } else {
      // parse tbname
      code = checkAndTrimValue(&token, pParFileCxt->tmpTokenBuf, pMsgBuf, TSDB_DATA_TYPE_BINARY);
      if (TK_NK_VARIABLE == token.type) {
        code = buildInvalidOperationMsg(pMsgBuf, "not expected tbname");
      }

      if (TSDB_CODE_SUCCESS == code) {
        bool bFoundTbName = false;
        code = parseTbnameToken(pMsgBuf, pParFileCxt->ctbName.tname, &token, &bFoundTbName);
      }
    }

    if (TSDB_CODE_SUCCESS != code) break;
  }

  if (TSDB_CODE_SUCCESS == code) {
    pParFileCxt->tagNameFilled = true;
    if (!isJson) {
      code = tTagNew(pParFileCxt->aTagVals, 1, false, &pParFileCxt->pTag);
    }
  }

  return code;
}

typedef struct {
  SName       ctbName;
  SArray*     aTagNames;
  STag*       pTag;
  SVgroupInfo vg;
} SCreateTableData;

static void clearTagValArrayFp(void* data) {
  STagVal* p = (STagVal*)data;
  if (IS_VAR_DATA_TYPE(p->type)) {
    taosMemoryFreeClear(p->pData);
  }
}

static void clearCreateTbArrayFp(void* data) {
  SCreateTableData* p = (SCreateTableData*)data;
  taosMemoryFreeClear(p->pTag);
}

static int32_t parseCsvFile(SMsgBuf* pMsgBuf, SParseContext* pParseCxt, SParseFileContext* pParFileCxt, TdFilePtr fp,
                            int32_t maxLineCount) {
  int32_t code = TSDB_CODE_SUCCESS;

  char*   pLine = NULL;
  int64_t readLen = 0;
  int32_t lineCount = 0;
  while (TSDB_CODE_SUCCESS == code && (readLen = taosGetLineFile(fp, &pLine)) != -1) {
    if (('\r' == pLine[readLen - 1]) || ('\n' == pLine[readLen - 1])) {
      pLine[--readLen] = '\0';
    }

    if (readLen == 0) continue;

    if (pLine[0] == '#') continue;  // ignore comment line begins with '#'

    (void)strtolower(pLine, pLine);
    pParFileCxt->pSql = pLine;

    code = parseOneStbRow(pMsgBuf, pParFileCxt, pParseCxt->timezone, pParseCxt->charsetCxt);

    if (TSDB_CODE_SUCCESS == code) {
      code = fillVgroupInfo(pParseCxt, &pParFileCxt->ctbName, &pParFileCxt->vg);
    }

    if (TSDB_CODE_SUCCESS == code) {
      SCreateTableData data = {.ctbName = pParFileCxt->ctbName,
                               .aTagNames = pParFileCxt->aTagNames,
                               .pTag = pParFileCxt->pTag,
                               .vg = pParFileCxt->vg};

      if (NULL == taosArrayPush(pParFileCxt->aCreateTbData, &data)) {
        taosMemoryFreeClear(pParFileCxt->pTag);
        code = terrno;
      }
    } else {
      taosMemoryFreeClear(pParFileCxt->pTag);
    }

    pParFileCxt->pTag = NULL;
    taosArrayClearEx(pParFileCxt->aTagVals, clearTagValArrayFp);
    lineCount++;
    if (lineCount == maxLineCount) break;
  }

  if (TSDB_CODE_SUCCESS != code) {
    taosArrayClearEx(pParFileCxt->aCreateTbData, clearCreateTbArrayFp);
  }

  taosMemoryFree(pLine);
  return code;
}

static void destructParseFileContext(SParseFileContext** ppParFileCxt) {
  if (NULL == ppParFileCxt || NULL == *ppParFileCxt) {
    return;
  }

  SParseFileContext* pParFileCxt = *ppParFileCxt;

  taosArrayDestroy(pParFileCxt->aTagNames);
  taosMemoryFreeClear(pParFileCxt->pStbMeta);
  taosArrayDestroy(pParFileCxt->aTagIndexs);
  taosArrayDestroy(pParFileCxt->aCreateTbData);
  taosArrayDestroy(pParFileCxt->aTagVals);
  taosMemoryFree(pParFileCxt);

  *ppParFileCxt = NULL;

  return;
}

static int32_t constructParseFileContext(SCreateSubTableFromFileClause* pStmt, STableMeta* pSuperTableMeta,
                                         int32_t acctId, SParseFileContext** ppParFileCxt) {
  int32_t code = TSDB_CODE_SUCCESS;

  SParseFileContext* pParFileCxt = taosMemoryCalloc(1, sizeof(SParseFileContext));
  if (!pParFileCxt) return terrno;
  pParFileCxt->pStbMeta = pSuperTableMeta;
  pParFileCxt->tagNameFilled = false;
  pParFileCxt->pTag = NULL;
  pParFileCxt->ctbName.type = TSDB_TABLE_NAME_T;
  pParFileCxt->ctbName.acctId = acctId;
  tstrncpy(pParFileCxt->ctbName.dbname, pStmt->useDbName, TSDB_DB_NAME_LEN);

  if (NULL == pParFileCxt->aTagNames) {
    pParFileCxt->aTagNames = taosArrayInit(8, TSDB_COL_NAME_LEN);
    if (NULL == pParFileCxt->aTagNames) {
      code = terrno;
      goto _ERR;
    }
  }

  if (NULL == pParFileCxt->aCreateTbData) {
    pParFileCxt->aCreateTbData = taosArrayInit(16, sizeof(SCreateTableData));
    if (NULL == pParFileCxt->aCreateTbData) {
      code = terrno;
      goto _ERR;
    }
  }

  if (NULL == pParFileCxt->aTagIndexs) {
    pParFileCxt->aTagIndexs = taosArrayInit(pStmt->pSpecificTags->length, sizeof(int16_t));
    if (!pParFileCxt->aTagIndexs) {
      code = terrno;
      goto _ERR;
    }
  }

  if (NULL == pParFileCxt->aTagVals) {
    pParFileCxt->aTagVals = taosArrayInit(8, sizeof(STagVal));
    if (!pParFileCxt->aTagVals) {
      code = terrno;
      goto _ERR;
    }
  }

  *ppParFileCxt = pParFileCxt;

  return code;

_ERR:
  destructParseFileContext(&pParFileCxt);

  return code;
}

typedef struct SCreateTbInfo {
  bool ignoreExists;
  char useDbName[TSDB_DB_NAME_LEN];
  char useTableName[TSDB_TABLE_NAME_LEN];
} SCreateTbInfo;

static int32_t prepareReadCsvFile(STranslateContext* pCxt, SCreateSubTableFromFileClause* pCreateStmt,
                                  SVnodeModifyOpStmt* pModifyStmt) {
  int32_t code = 0;

  TdFilePtr          fp = NULL;
  SCreateTbInfo*     pCreateInfo = NULL;
  SParseFileContext* pParFileCxt = NULL;

  if (NULL == pModifyStmt->fp) {
    fp = taosOpenFile(pCreateStmt->filePath, TD_FILE_READ | TD_FILE_STREAM);
    if (NULL == fp) {
      code = terrno;
      goto _ERR;
    }
  }

  {
    pCreateInfo = taosMemoryCalloc(1, sizeof(SCreateTbInfo));
    if (NULL == pCreateInfo) {
      code = terrno;
      goto _ERR;
    }

    pCreateInfo->ignoreExists = pCreateStmt->ignoreExists;
    tstrncpy(pCreateInfo->useDbName, pCreateStmt->useDbName, TSDB_DB_NAME_LEN);
    tstrncpy(pCreateInfo->useTableName, pCreateStmt->useTableName, TSDB_TABLE_NAME_LEN);
  }

  {
    STableMeta* pSuperTableMeta = NULL;
    code = getTableMeta(pCxt, pCreateStmt->useDbName, pCreateStmt->useTableName, &pSuperTableMeta);
    if (TSDB_CODE_SUCCESS != code) goto _ERR;

    code = constructParseFileContext(pCreateStmt, pSuperTableMeta, pCxt->pParseCxt->acctId, &pParFileCxt);
    if (TSDB_CODE_SUCCESS != code) goto _ERR;

    code = buildTagIndexForBindTags(&pCxt->msgBuf, pCreateStmt, pParFileCxt->pStbMeta, pParFileCxt->aTagIndexs);
    if (TSDB_CODE_SUCCESS != code) goto _ERR;
  }

  pModifyStmt->fp = fp;
  pModifyStmt->fileProcessing = false;
  pModifyStmt->pCreateTbInfo = pCreateInfo;
  pModifyStmt->pParFileCxt = pParFileCxt;

  return code;

_ERR:
  TAOS_UNUSED(taosCloseFile(&fp));
  taosMemoryFreeClear(pCreateInfo);
  destructParseFileContext(&pParFileCxt);
  return code;
}

static void resetParseFileContext(SParseFileContext* pParFileCxt) {
  taosArrayClear(pParFileCxt->aCreateTbData);
  taosArrayClearEx(pParFileCxt->aTagVals, clearTagValArrayFp);
}

static int32_t createSubTableFromFile(SMsgBuf* pMsgBuf, SParseContext* pParseCxt, SVnodeModifyOpStmt* pModifyStmt) {
  int32_t code = 0;

  SCreateTbInfo*     pCreateInfo = pModifyStmt->pCreateTbInfo;
  SParseFileContext* pParFileCxt = pModifyStmt->pParFileCxt;

  if (TSDB_CODE_SUCCESS == code) {
    code = parseCsvFile(pMsgBuf, pParseCxt, pParFileCxt, pModifyStmt->fp, tsMaxInsertBatchRows);
  }

  STableMeta* pSuperTableMeta = pParFileCxt->pStbMeta;
  if (TSDB_CODE_SUCCESS == code) {
    int sz = taosArrayGetSize(pParFileCxt->aCreateTbData);
    for (int i = 0; i < sz; i++) {
      SCreateTableData* pData = taosArrayGet(pParFileCxt->aCreateTbData, i);

      // code = collectUseTable(&pData->ctbName, pCxt->pTargetTables);
      // if (TSDB_CODE_SUCCESS != code) {
      //   taosMemoryFree(pData->pTag);
      // }

      code = addCreateTbReqIntoVgroup(pModifyStmt->pVgroupsHashObj, pCreateInfo->useDbName, pSuperTableMeta->uid,
                                      pCreateInfo->useTableName, pData->ctbName.tname, pData->aTagNames,
                                      pSuperTableMeta->tableInfo.numOfTags, pData->pTag, TSDB_DEFAULT_TABLE_TTL, NULL,
                                      pCreateInfo->ignoreExists, &pData->vg);
    }

    if (TSDB_CODE_SUCCESS == code) {
      if (sz > 0) {
        pModifyStmt->fileProcessing = true;
      } else {
        pModifyStmt->fileProcessing = false;
      }
    }
  }

  resetParseFileContext(pModifyStmt->pParFileCxt);

  return code;
}

int32_t serializeVgroupsCreateTableBatch(SHashObj* pVgroupHashmap, SArray** pOut) {
  SArray* pBufArray = taosArrayInit(taosHashGetSize(pVgroupHashmap), sizeof(void*));
  if (NULL == pBufArray) {
    return terrno;
  }

  int32_t                  code = TSDB_CODE_SUCCESS;
  SVgroupCreateTableBatch* pTbBatch = NULL;
  do {
    pTbBatch = taosHashIterate(pVgroupHashmap, pTbBatch);
    if (pTbBatch == NULL) {
      break;
    }

    code = serializeVgroupCreateTableBatch(pTbBatch, pBufArray);
    if (TSDB_CODE_SUCCESS != code) {
      parserError("failed to serialize create table batch msg, since:%s", tstrerror(code));
      taosHashCancelIterate(pVgroupHashmap, pTbBatch);
      break;
    }
  } while (true);

  if (TSDB_CODE_SUCCESS != code) {
    taosArrayDestroy(pBufArray);
  } else {
    *pOut = pBufArray;
  }

  return code;
}

static int32_t rewriteCreateMultiTable(STranslateContext* pCxt, SQuery* pQuery) {
  SCreateMultiTablesStmt* pStmt = (SCreateMultiTablesStmt*)pQuery->pRoot;

  SHashObj* pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pVgroupHashmap) {
    return terrno;
  }

  taosHashSetFreeFp(pVgroupHashmap, destroyCreateTbReqBatch);
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode;
  FOREACH(pNode, pStmt->pSubTables) {
    SCreateSubTableClause* pClause = (SCreateSubTableClause*)pNode;
    code = rewriteCreateSubTable(pCxt, pClause, pVgroupHashmap);
    if (TSDB_CODE_SUCCESS != code) {
      taosHashCleanup(pVgroupHashmap);
      return code;
    }
  }

  SArray* pBufArray = NULL;
  code = serializeVgroupsCreateTableBatch(pVgroupHashmap, &pBufArray);
  taosHashCleanup(pVgroupHashmap);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  return rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
}

static int32_t rewriteCreateTableFromFile(STranslateContext* pCxt, SQuery* pQuery) {
  SVnodeModifyOpStmt* pModifyStmt = NULL;
  int32_t             code = nodesMakeNode(QUERY_NODE_VNODE_MODIFY_STMT, (SNode**)&pModifyStmt);
  if (pModifyStmt == NULL) {
    return code;
  }
  pModifyStmt->sqlNodeType = nodeType(pQuery->pRoot);
  pModifyStmt->fileProcessing = false;
  pModifyStmt->destroyParseFileCxt = destructParseFileContext;
  pModifyStmt->pVgroupsHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (NULL == pModifyStmt->pVgroupsHashObj) {
    return terrno;
  }
  taosHashSetFreeFp(pModifyStmt->pVgroupsHashObj, destroyCreateTbReqBatch);

  SCreateSubTableFromFileClause* pCreateStmt = (SCreateSubTableFromFileClause*)pQuery->pRoot;

  code = prepareReadCsvFile(pCxt, pCreateStmt, pModifyStmt);
  if (TSDB_CODE_SUCCESS != code) {
    taosHashCleanup(pModifyStmt->pVgroupsHashObj);
    return code;
  }

  code = createSubTableFromFile(&pCxt->msgBuf, pCxt->pParseCxt, pModifyStmt);
  if (TSDB_CODE_SUCCESS != code) {
    taosHashCleanup(pModifyStmt->pVgroupsHashObj);
    return code;
  }

  SArray* pBufArray = NULL;
  code = serializeVgroupsCreateTableBatch(pModifyStmt->pVgroupsHashObj, &pBufArray);
  taosHashClear(pModifyStmt->pVgroupsHashObj);
  if (TSDB_CODE_SUCCESS != code) {
    if (TSDB_CODE_INVALID_MSG_LEN == code) {
      parserError("maxInsertBatchRows may need to be reduced, current:%d", tsMaxInsertBatchRows);
    }
    taosHashCleanup(pModifyStmt->pVgroupsHashObj);
    return code;
  }

  pModifyStmt->pDataBlocks = pBufArray;
  nodesDestroyNode(pQuery->pRoot);
  pQuery->pRoot = (SNode*)pModifyStmt;

  return TSDB_CODE_SUCCESS;
}

int32_t continueCreateTbFromFile(SParseContext* pParseCxt, SQuery** pQuery) {
  SVnodeModifyOpStmt* pModifyStmt = (SVnodeModifyOpStmt*)(*pQuery)->pRoot;

  SMsgBuf tmpBuf = {0};
  tmpBuf.buf = taosMemoryMalloc(1024);
  if (!tmpBuf.buf) {
    return terrno;
  }
  int32_t code = createSubTableFromFile(&tmpBuf, pParseCxt, pModifyStmt);
  if (TSDB_CODE_SUCCESS != code) goto _OUT;

  SArray* pBufArray = NULL;
  code = serializeVgroupsCreateTableBatch(pModifyStmt->pVgroupsHashObj, &pBufArray);
  taosHashClear(pModifyStmt->pVgroupsHashObj);
  if (TSDB_CODE_SUCCESS != code) {
    goto _OUT;
  }

  pModifyStmt->pDataBlocks = pBufArray;
  (*pQuery)->execStage = QUERY_EXEC_STAGE_SCHEDULE;
  if (!pModifyStmt->fileProcessing) {
    (*pQuery)->execMode = QUERY_EXEC_MODE_EMPTY_RESULT;
  }
  code = TSDB_CODE_SUCCESS;

_OUT:
  taosMemoryFreeClear(tmpBuf.buf);
  return code;
}

typedef struct SVgroupDropTableBatch {
  SVDropTbBatchReq req;
  SVgroupInfo      info;
  char             dbName[TSDB_DB_NAME_LEN];
} SVgroupDropTableBatch;

static int32_t addDropTbReqIntoVgroup(SHashObj* pVgroupHashmap, SVgroupInfo* pVgInfo, SVDropTbReq* pReq) {
  SVgroupDropTableBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId));
  int32_t                code = TSDB_CODE_SUCCESS;
  if (NULL == pTableBatch) {
    SVgroupDropTableBatch tBatch = {0};
    tBatch.info = *pVgInfo;
    tBatch.req.pArray = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SVDropTbReq));
    if (NULL == taosArrayPush(tBatch.req.pArray, pReq)) {
      taosArrayDestroy(tBatch.req.pArray);
      tBatch.req.pArray = NULL;
      return terrno;
    }

    code = taosHashPut(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId), &tBatch, sizeof(tBatch));
    if (TSDB_CODE_SUCCESS != code) {
      taosArrayDestroy(tBatch.req.pArray);
      tBatch.req.pArray = NULL;
      return code;
    }
  } else {  // add to the correct vgroup
    if (NULL == taosArrayPush(pTableBatch->req.pArray, pReq)) {
      return terrno;
    }
  }
  return code;
}

static int32_t buildDropTableVgroupHashmap(STranslateContext* pCxt, SDropTableClause* pClause, const SName* name,
                                           int8_t* tableType, SHashObj* pVgroupHashmap) {
  STableMeta* pTableMeta = NULL;
  int32_t     code = getTargetMeta(pCxt, name, &pTableMeta, false);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(name, pCxt->pTargetTables);
    *tableType = pTableMeta->tableType;
  }

  if (TSDB_CODE_SUCCESS == code && TSDB_SUPER_TABLE == pTableMeta->tableType) {
    goto over;
  }

  if (TSDB_CODE_PAR_TABLE_NOT_EXIST == code && pClause->ignoreNotExists) {
    code = TSDB_CODE_SUCCESS;
    goto over;
  }

  SVgroupInfo info = {0};
  if (TSDB_CODE_SUCCESS == code) {
    code = getTableHashVgroup(pCxt, pClause->dbName, pClause->tableName, &info);
  }
  if (TSDB_CODE_SUCCESS == code) {
    SVDropTbReq req = {.suid = pTableMeta->suid, .igNotExists = pClause->ignoreNotExists};
    req.isVirtual = isVirtualTable(pTableMeta);
    req.name = pClause->tableName;
    code = addDropTbReqIntoVgroup(pVgroupHashmap, &info, &req);
  }

over:
  taosMemoryFreeClear(pTableMeta);
  return code;
}

static int32_t buildDropVirtualTableVgroupHashmap(STranslateContext* pCxt, SDropVirtualTableStmt* pStmt,
                                                  const SName* name, int8_t* tableType, SHashObj* pVgroupHashmap) {
  STableMeta* pTableMeta = NULL;
  int32_t     code = getTargetMeta(pCxt, name, &pTableMeta, false);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(name, pCxt->pTargetTables);
    *tableType = pTableMeta->tableType;
  }

  if (TSDB_CODE_PAR_TABLE_NOT_EXIST == code && pStmt->ignoreNotExists) {
    PAR_RET(TSDB_CODE_SUCCESS);
  }
  PAR_ERR_JRET(code);

  if (!isVirtualTable(pTableMeta) && !isVirtualSTable(pTableMeta)) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DROP_VTABLE, "Cannot drop non-virtual table using DROP VTABLE"););
  }

  SVgroupInfo info = {0};
  PAR_ERR_JRET(getTableHashVgroup(pCxt, pStmt->dbName, pStmt->tableName, &info));

  SVDropTbReq req = {.suid = pTableMeta->suid, .igNotExists = pStmt->ignoreNotExists, .isVirtual = true};
  req.name = pStmt->tableName;
  PAR_ERR_JRET(addDropTbReqIntoVgroup(pVgroupHashmap, &info, &req));

_return:
  taosMemoryFreeClear(pTableMeta);
  return code;
}

static void destroyDropTbReqBatch(void* data) {
  SVgroupDropTableBatch* pTbBatch = (SVgroupDropTableBatch*)data;
  taosArrayDestroy(pTbBatch->req.pArray);
}

static int32_t serializeVgroupDropTableBatch(SVgroupDropTableBatch* pTbBatch, SArray* pBufArray) {
  int      tlen;
  SEncoder coder = {0};

  int32_t ret = 0;
  tEncodeSize(tEncodeSVDropTbBatchReq, &pTbBatch->req, tlen, ret);
  if (TSDB_CODE_SUCCESS != ret) return ret;
  tlen += sizeof(SMsgHead);
  void* buf = taosMemoryMalloc(tlen);
  if (NULL == buf) {
    return terrno;
  }
  ((SMsgHead*)buf)->vgId = htonl(pTbBatch->info.vgId);
  ((SMsgHead*)buf)->contLen = htonl(tlen);
  void* pBuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  tEncoderInit(&coder, pBuf, tlen - sizeof(SMsgHead));
  ret = tEncodeSVDropTbBatchReq(&coder, &pTbBatch->req);
  tEncoderClear(&coder);
  if (TSDB_CODE_SUCCESS != ret) {
    taosMemoryFreeClear(buf);
    return ret;
  }

  SVgDataBlocks* pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
  if (NULL == pVgData) {
    taosMemoryFreeClear(buf);
    return terrno;
  }
  pVgData->vg = pTbBatch->info;
  pVgData->pData = buf;
  pVgData->size = tlen;
  pVgData->numOfTables = (int32_t)taosArrayGetSize(pTbBatch->req.pArray);
  if (NULL == taosArrayPush(pBufArray, &pVgData)) {
    ret = terrno;
    taosMemoryFreeClear(pVgData);
    taosMemoryFreeClear(buf);
  }

  return ret;
}

int32_t serializeVgroupsDropTableBatch(SHashObj* pVgroupHashmap, SArray** pOut) {
  SArray* pBufArray = taosArrayInit(taosHashGetSize(pVgroupHashmap), sizeof(void*));
  if (NULL == pBufArray) {
    return terrno;
  }

  int32_t                code = TSDB_CODE_SUCCESS;
  SVgroupDropTableBatch* pTbBatch = NULL;
  do {
    pTbBatch = taosHashIterate(pVgroupHashmap, pTbBatch);
    if (pTbBatch == NULL) {
      break;
    }

    code = serializeVgroupDropTableBatch(pTbBatch, pBufArray);
    if (TSDB_CODE_SUCCESS != code) {
      taosHashCancelIterate(pVgroupHashmap, pTbBatch);
      taosArrayDestroy(pBufArray);
      break;
    }
  } while (true);

  *pOut = pBufArray;
  return code;
}

static int32_t rewriteDropTableWithOpt(STranslateContext* pCxt, SQuery* pQuery) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SDropTableStmt* pStmt = (SDropTableStmt*)pQuery->pRoot;
  if (!pStmt->withOpt) return code;
  pCxt->withOpt = true;

  SNode* pNode = NULL;
  char   pTableName[TSDB_TABLE_NAME_LEN] = {0};
  FOREACH(pNode, pStmt->pTables) {
    SDropTableClause* pClause = (SDropTableClause*)pNode;
    for (int32_t i = 0; i < TSDB_TABLE_NAME_LEN; i++) {
      if (pClause->tableName[i] == '\0') {
        break;
      }
      if (!isdigit(pClause->tableName[i])) {
        return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_TABLE_NOT_EXIST, "Table does not exist: `%s`.`%s`",
                                       pClause->dbName, pClause->tableName);
      }
    }
    SName name = {0};
    toName(pCxt->pParseCxt->acctId, pClause->dbName, pClause->tableName, &name);
    code = getTargetName(pCxt, &name, pTableName);
    if (TSDB_CODE_SUCCESS != code) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, code, "%s: db:`%s`, tbuid:`%s`", tstrerror(code), pClause->dbName,
                                     pClause->tableName);
    }
    tstrncpy(pClause->tableName, pTableName, TSDB_TABLE_NAME_LEN);  // rewrite table uid to table name
  }

  code = rewriteDropTableWithMetaCache(pCxt);

  TAOS_RETURN(code);
}

static int32_t rewriteDropTable(STranslateContext* pCxt, SQuery* pQuery) {
  SDropTableStmt* pStmt = (SDropTableStmt*)pQuery->pRoot;
  int8_t          tableType;
  SNode*          pNode;
  SArray*         pTsmas = NULL;

  FOREACH(pNode, pStmt->pTables) {
    SDropTableClause* pClause = (SDropTableClause*)pNode;
    if (IS_SYS_DBNAME(pClause->dbName)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION,
                                     "Cannot drop table of system database: `%s`.`%s`", pClause->dbName,
                                     pClause->tableName);
    }
  }

  TAOS_CHECK_RETURN(rewriteDropTableWithOpt(pCxt, pQuery));

  SHashObj* pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pVgroupHashmap) {
    return terrno;
  }

  taosHashSetFreeFp(pVgroupHashmap, destroyDropTbReqBatch);
  FOREACH(pNode, pStmt->pTables) {
    SDropTableClause* pClause = (SDropTableClause*)pNode;
    SName             name = {0};
    toName(pCxt->pParseCxt->acctId, pClause->dbName, pClause->tableName, &name);
    int32_t code = buildDropTableVgroupHashmap(pCxt, pClause, &name, &tableType, pVgroupHashmap);
    if (TSDB_CODE_SUCCESS != code) {
      taosHashCleanup(pVgroupHashmap);
      return code;
    }
    if (tableType == TSDB_SUPER_TABLE && LIST_LENGTH(pStmt->pTables) > 1) {
      taosHashCleanup(pVgroupHashmap);
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DROP_STABLE);
    }
    if (pCxt->withOpt) continue;
    if (pCxt->pMetaCache) code = getTableTsmasFromCache(pCxt->pMetaCache, &name, &pTsmas);
    if (TSDB_CODE_SUCCESS != code) {
      taosHashCleanup(pVgroupHashmap);
      return code;
    }
    if (!pStmt->withTsma) {
      pStmt->withTsma = pTsmas && pTsmas->size > 0;
    }
    pClause->pTsmas = pTsmas;
    if (tableType == TSDB_NORMAL_TABLE && pTsmas && pTsmas->size > 0) {
      taosHashCleanup(pVgroupHashmap);
      return TSDB_CODE_TSMA_MUST_BE_DROPPED;
    }
  }

  if (tableType == TSDB_SUPER_TABLE || 0 == taosHashGetSize(pVgroupHashmap)) {
    taosHashCleanup(pVgroupHashmap);
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;
  if (pStmt->withTsma) {
    if (code == TSDB_CODE_SUCCESS) {
      SMDropTbsReq req = {0};
      req.pVgReqs = taosArrayInit(taosHashGetSize(pVgroupHashmap), sizeof(SMDropTbReqsOnSingleVg));

      SVgroupDropTableBatch* pTbBatch = NULL;
      do {
        pTbBatch = taosHashIterate(pVgroupHashmap, pTbBatch);
        if (pTbBatch == NULL) {
          break;
        }

        SMDropTbReqsOnSingleVg reqOnVg = {0};
        reqOnVg.vgInfo = pTbBatch->info;
        reqOnVg.pTbs = pTbBatch->req.pArray;
        if (NULL == taosArrayPush(req.pVgReqs, &reqOnVg)) {
          taosHashCancelIterate(pVgroupHashmap, pTbBatch);
          code = terrno;
          break;
        }
      } while (true);
      if (TSDB_CODE_SUCCESS == code) {
        code = buildCmdMsg(pCxt, TDMT_MND_DROP_TB_WITH_TSMA, (FSerializeFunc)tSerializeSMDropTbsReq, &req);
      }
      taosArrayDestroy(req.pVgReqs);
    }
    taosHashCleanup(pVgroupHashmap);
    return code;
  }

  SArray* pBufArray = NULL;
  code = serializeVgroupsDropTableBatch(pVgroupHashmap, &pBufArray);
  taosHashCleanup(pVgroupHashmap);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  return rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
}

static int32_t rewriteDropVirtualTableWithOpt(STranslateContext* pCxt, SQuery* pQuery) {
  int32_t                code = TSDB_CODE_SUCCESS;
  SDropVirtualTableStmt* pStmt = (SDropVirtualTableStmt*)pQuery->pRoot;
  if (!pStmt->withOpt) {
    PAR_RET(code);
  }

  SNode* pNode = NULL;
  char   pTableName[TSDB_TABLE_NAME_LEN] = {0};

  for (int32_t i = 0; i < TSDB_TABLE_NAME_LEN; i++) {
    if (pStmt->tableName[i] == '\0') {
      break;
    }
    if (!isdigit(pStmt->tableName[i])) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_TABLE_NOT_EXIST, "Table does not exist: `%s`.`%s`",
                                     pStmt->dbName, pStmt->tableName);
    }
  }

  SName name = {0};
  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);
  PAR_ERR_RET(getTargetName(pCxt, &name, pTableName));
  tstrncpy(pStmt->tableName, pTableName, TSDB_TABLE_NAME_LEN);  // rewrite table uid to table name

  PAR_RET(rewriteDropTableWithMetaCache(pCxt));
}

static int32_t rewriteDropVirtualTable(STranslateContext* pCxt, SQuery* pQuery) {
  SDropVirtualTableStmt* pStmt = (SDropVirtualTableStmt*)pQuery->pRoot;
  int8_t                 tableType;
  SNode*                 pNode;
  SArray*                pBufArray = NULL;
  int32_t                code = TSDB_CODE_SUCCESS;
  SName                  name = {0};
  SHashObj*              pVgroupHashmap = NULL;

  PAR_ERR_JRET(rewriteDropVirtualTableWithOpt(pCxt, pQuery));

  pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pVgroupHashmap) {
    return terrno;
  }

  taosHashSetFreeFp(pVgroupHashmap, destroyDropTbReqBatch);

  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);
  PAR_ERR_JRET(buildDropVirtualTableVgroupHashmap(pCxt, pStmt, &name, &tableType, pVgroupHashmap));
  if (0 == taosHashGetSize(pVgroupHashmap)) {
    taosHashCleanup(pVgroupHashmap);
    return TSDB_CODE_SUCCESS;
  }
  PAR_ERR_JRET(serializeVgroupsDropTableBatch(pVgroupHashmap, &pBufArray));
  PAR_ERR_JRET(rewriteToVnodeModifyOpStmt(pQuery, pBufArray));

_return:
  taosHashCleanup(pVgroupHashmap);
  return code;
}

static int32_t rewriteDropSuperTablewithOpt(STranslateContext* pCxt, SQuery* pQuery) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SDropSuperTableStmt* pStmt = (SDropSuperTableStmt*)pQuery->pRoot;
  if (!pStmt->withOpt) return code;
  pCxt->withOpt = true;

  for (int32_t i = 0; i < TSDB_TABLE_NAME_LEN; i++) {
    if (pStmt->tableName[i] == '\0') {
      break;
    }
    if (!isdigit(pStmt->tableName[i])) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_TABLE_NOT_EXIST, "STable not exist: `%s`.`%s`",
                                     pStmt->dbName, pStmt->tableName);
    }
  }

  char  pTableName[TSDB_TABLE_NAME_LEN] = {0};
  SName name = {0};
  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);
  code = getTargetName(pCxt, &name, pTableName);
  if (TSDB_CODE_SUCCESS != code) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, code, "%s: db:`%s`, tbuid:`%s`",
                                   (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || code == TSDB_CODE_TDB_TABLE_NOT_EXIST)
                                       ? "STable not exist"
                                       : tstrerror(code),
                                   pStmt->dbName, pStmt->tableName);
  }
  tstrncpy(pStmt->tableName, pTableName, TSDB_TABLE_NAME_LEN);  // rewrite table uid to table name

  code = rewriteDropTableWithMetaCache(pCxt);

  TAOS_RETURN(code);
}

static int32_t rewriteDropSuperTable(STranslateContext* pCxt, SQuery* pQuery) {
  SDropSuperTableStmt* pStmt = (SDropSuperTableStmt*)pQuery->pRoot;
  if (IS_SYS_DBNAME(pStmt->dbName)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION,
                                   "Cannot drop table of system database: `%s`.`%s`", pStmt->dbName, pStmt->tableName);
  }
  TAOS_CHECK_RETURN(rewriteDropSuperTablewithOpt(pCxt, pQuery));
  TAOS_RETURN(0);
}

static int32_t buildUpdateTagValReqImpl2(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                         char* colName, SMultiTagUpateVal* pReq) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  SSchema* pSchema = getTagSchema(pTableMeta, colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE, "Invalid tag name: %s", colName);
  }

  if (pSchema->flags & COL_REF_BY_STM) {
    return TSDB_CODE_PAR_COL_TAG_REF_BY_STM;
  }

  pReq->tagName = taosStrdup(colName);
  if (NULL == pReq->tagName) {
    TAOS_CHECK_GOTO(terrno, &lino, _err);
  }

  pReq->pTagArray = taosArrayInit(1, sizeof(STagVal));
  if (NULL == pReq->pTagArray) {
    TAOS_CHECK_GOTO(terrno, &lino, _err);
  }
  pReq->colId = pSchema->colId;
  pReq->tagType = pSchema->type;

  STag*       pTag = NULL;
  SToken      token;
  char        tokenBuf[TSDB_MAX_TAGS_LEN];
  const char* tagStr = pStmt->pVal->literal;
  NEXT_TOKEN_WITH_PREV(tagStr, token);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkAndTrimValue(&token, tokenBuf, &pCxt->msgBuf, pSchema->type);
    if (TSDB_CODE_SUCCESS == code && TK_NK_VARIABLE == token.type) {
      code = buildSyntaxErrMsg(&pCxt->msgBuf, "not expected tags values", token.z);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = parseTagValue(&pCxt->msgBuf, &tagStr, pTableMeta->tableInfo.precision, pSchema, &token, NULL,
                         pReq->pTagArray, &pTag, pCxt->pParseCxt->timezone, pCxt->pParseCxt->charsetCxt);
    if (pSchema->type == TSDB_DATA_TYPE_JSON && token.type == TK_NULL && code == TSDB_CODE_SUCCESS) {
      pReq->tagFree = true;
    }
  }
  if (TSDB_CODE_SUCCESS == code && tagStr) {
    NEXT_VALID_TOKEN(tagStr, token);
    if (token.n != 0) {
      code = buildSyntaxErrMsg(&pCxt->msgBuf, "not expected tags values", token.z);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (pSchema->type == TSDB_DATA_TYPE_JSON) {
      code = buildSyntaxErrMsg(&pCxt->msgBuf, "not expected tags values ", token.z);
    } else {
      STagVal* pTagVal = taosArrayGet(pReq->pTagArray, 0);
      if (pTagVal) {
        pReq->isNull = false;
        if (IS_VAR_DATA_TYPE(pSchema->type)) {
          pReq->nTagVal = pTagVal->nData;
          pReq->pTagVal = pTagVal->pData;
        } else {
          pReq->nTagVal = pSchema->bytes;
          pReq->pTagVal = (uint8_t*)&pTagVal->i64;
        }
      } else {
        pReq->isNull = true;
      }
    }
  }
_err:
  if (code != 0) {
    taosArrayDestroy(pReq->pTagArray);
    taosMemoryFree(pReq->tagName);
  }
  return code;
}
static int32_t buildUpdateTagValReqImpl(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                        char* colName, SVAlterTbReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  SSchema* pSchema = getTagSchema(pTableMeta, colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE, "Invalid tag name: %s", colName);
  }

  if (pSchema->flags & COL_REF_BY_STM) {
    return TSDB_CODE_PAR_COL_TAG_REF_BY_STM;
  }

  pReq->tagName = taosStrdup(colName);
  if (NULL == pReq->tagName) {
    return terrno;
  }
  pReq->pTagArray = taosArrayInit(1, sizeof(STagVal));
  if (NULL == pReq->pTagArray) {
    return terrno;
  }
  pReq->colId = pSchema->colId;
  pReq->tagType = pSchema->type;

  STag*       pTag = NULL;
  SToken      token;
  char        tokenBuf[TSDB_MAX_TAGS_LEN];
  const char* tagStr = pStmt->pVal->literal;
  NEXT_TOKEN_WITH_PREV(tagStr, token);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkAndTrimValue(&token, tokenBuf, &pCxt->msgBuf, pSchema->type);
    if (TSDB_CODE_SUCCESS == code && TK_NK_VARIABLE == token.type) {
      code = buildSyntaxErrMsg(&pCxt->msgBuf, "not expected tags values", token.z);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = parseTagValue(&pCxt->msgBuf, &tagStr, pTableMeta->tableInfo.precision, pSchema, &token, NULL,
                         pReq->pTagArray, &pTag, pCxt->pParseCxt->timezone, pCxt->pParseCxt->charsetCxt);
    if (pSchema->type == TSDB_DATA_TYPE_JSON && token.type == TK_NULL && code == TSDB_CODE_SUCCESS) {
      pReq->tagFree = true;
    }
  }
  if (TSDB_CODE_SUCCESS == code && tagStr) {
    NEXT_VALID_TOKEN(tagStr, token);
    if (token.n != 0) {
      code = buildSyntaxErrMsg(&pCxt->msgBuf, "not expected tags values", token.z);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (pSchema->type == TSDB_DATA_TYPE_JSON) {
      pReq->nTagVal = pTag->len;
      pReq->pTagVal = (uint8_t*)pTag;
      pStmt->pVal->datum.p = (char*)pTag;  // for free
    } else {
      STagVal* pTagVal = taosArrayGet(pReq->pTagArray, 0);
      if (pTagVal) {
        pReq->isNull = false;
        if (IS_VAR_DATA_TYPE(pSchema->type)) {
          pReq->nTagVal = pTagVal->nData;
          pReq->pTagVal = pTagVal->pData;
        } else {
          pReq->nTagVal = pSchema->bytes;
          pReq->pTagVal = (uint8_t*)&pTagVal->i64;
        }
      } else {
        pReq->isNull = true;
      }
    }
  }

  return code;
}
static int32_t buildUpdateTagValReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                    SVAlterTbReq* pReq) {
  SName   tbName = {0};
  SArray* pTsmas = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  if (pCxt->pMetaCache) {
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tbName);
    code = getTableTsmasFromCache(pCxt->pMetaCache, &tbName, &pTsmas);
    if (code != TSDB_CODE_SUCCESS) return code;
    if (pTsmas && pTsmas->size > 0) return TSDB_CODE_TSMA_MUST_BE_DROPPED;
  }
  return buildUpdateTagValReqImpl(pCxt, pStmt, pTableMeta, pStmt->colName, pReq);
}

static int32_t buildUpdateMultiTagValReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                         SVAlterTbReq* pReq) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  SName     tbName = {0};
  SArray*   pTsmas = NULL;
  SHashObj* pUnique = NULL;
  if (pCxt->pMetaCache) {
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tbName);
    code = getTableTsmasFromCache(pCxt->pMetaCache, &tbName, &pTsmas);
    if (code != TSDB_CODE_SUCCESS) return code;
    if (pTsmas && pTsmas->size > 0) return TSDB_CODE_TSMA_MUST_BE_DROPPED;
  }
  SNodeList* pNodeList = pStmt->pNodeListTagValue;
  if (pNodeList == NULL) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }

  int32_t nTagValues = pNodeList->length;
  if (nTagValues == 1) {
    SAlterTableStmt* head = (SAlterTableStmt*)pNodeList->pHead->pNode;
    pReq->action = TSDB_ALTER_TABLE_UPDATE_TAG_VAL;
    return buildUpdateTagValReqImpl(pCxt, head, pTableMeta, head->colName, pReq);
  } else {
    pReq->pMultiTag = taosArrayInit(nTagValues, sizeof(SMultiTagUpateVal));
    if (pReq->pMultiTag == NULL) {
      return terrno;
    }

    pUnique = taosHashInit(nTagValues, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    if (pUnique == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _err);
    }

    SAlterTableStmt* pTagStmt = NULL;
    SNode*           pNode = NULL;
    int8_t           dummpy = 0;
    FOREACH(pNode, pNodeList) {
      SMultiTagUpateVal val = {0};
      pTagStmt = (SAlterTableStmt*)pNode;

      SMultiTagUpateVal* p = taosHashGet(pUnique, pTagStmt->colName, strlen(pTagStmt->colName));
      if (p) {
        code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DUPLICATED_COLUMN);
        TAOS_CHECK_GOTO(code, &lino, _err);
      }

      code = taosHashPut(pUnique, pTagStmt->colName, strlen(pTagStmt->colName), &dummpy, sizeof(dummpy));
      TAOS_CHECK_GOTO(code, &lino, _err);

      code = buildUpdateTagValReqImpl2(pCxt, pTagStmt, pTableMeta, pTagStmt->colName, &val);
      TAOS_CHECK_GOTO(code, &lino, _err);

      if (taosArrayPush(pReq->pMultiTag, &val) == NULL) {
        tfreeMultiTagUpateVal((void*)&val);
        TAOS_CHECK_GOTO(terrno, &lino, _err);
      }
    }
  }
_err:
  taosHashCleanup(pUnique);
  return code;
}

static int32_t checkColRef(STranslateContext* pCxt, char* pRefDbName, char* pRefTableName, char* pRefColName,
                           SDataType type) {
  STableMeta* pRefTableMeta = NULL;
  int32_t     code = TSDB_CODE_SUCCESS;

  PAR_ERR_JRET(getTableMeta(pCxt, pRefDbName, pRefTableName, &pRefTableMeta));

  // org table cannot has composite primary key
  if (pRefTableMeta->tableInfo.numOfColumns > 1 && pRefTableMeta->schema[1].flags & COL_IS_KEY) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_REF_COLUMN, "virtual table's column reference can not from table with composite key"));
  }

  // org table must be child table or normal table
  if (pRefTableMeta->tableType != TSDB_NORMAL_TABLE && pRefTableMeta->tableType != TSDB_CHILD_TABLE) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_REF_COLUMN, "virtual table's column reference can only be normal table or child table"));
  }

  const SSchema* pRefCol = getNormalColSchema(pRefTableMeta, pRefColName);
  if (NULL == pRefCol) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_REF_COLUMN, "virtual table's column reference column not exist"));
  }

  if (pRefCol->type != type.type || pRefCol->bytes != type.bytes) {
    PAR_ERR_JRET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_REF_COLUMN_TYPE, "virtual table's column type and reference column type not match"));
  }

_return:
  taosMemoryFreeClear(pRefTableMeta);
  return code;
}

static int32_t buildAddColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                              SVAlterTbReq* pReq) {
  // only super and normal and virtual normal support
  if (TSDB_CHILD_TABLE == pTableMeta->tableType || TSDB_VIRTUAL_CHILD_TABLE == pTableMeta->tableType) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }

  if (NULL != getColSchema(pTableMeta, pStmt->colName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DUPLICATED_COLUMN);
  }

  if (isVirtualTable(pTableMeta) && IS_DECIMAL_TYPE(pStmt->dataType.type)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_VTABLE_NOT_SUPPORT_DATA_TYPE);
  }

  if ((TSDB_DATA_TYPE_VARCHAR == pStmt->dataType.type && calcTypeBytes(pStmt->dataType) > TSDB_MAX_BINARY_LEN) ||
      (TSDB_DATA_TYPE_VARBINARY == pStmt->dataType.type && calcTypeBytes(pStmt->dataType) > TSDB_MAX_BINARY_LEN) ||
      (TSDB_DATA_TYPE_NCHAR == pStmt->dataType.type && calcTypeBytes(pStmt->dataType) > TSDB_MAX_NCHAR_LEN)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
  }

  if (TSDB_MAX_COLUMNS == pTableMeta->tableInfo.numOfColumns) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TOO_MANY_COLUMNS);
  }

  if (pTableMeta->tableInfo.rowSize + calcTypeBytes(pStmt->dataType) > TSDB_MAX_BYTES_PER_ROW) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ROW_LENGTH, TSDB_MAX_BYTES_PER_ROW);
  }

  if (pStmt->alterType == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF) {
    if (TSDB_VIRTUAL_NORMAL_TABLE != pTableMeta->tableType) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
    }

    // check ref column exists and check type
    PAR_ERR_RET(checkColRef(pCxt, pStmt->refDbName, pStmt->refTableName, pStmt->refColName,
                            (SDataType){.type = pStmt->dataType.type, .bytes = calcTypeBytes(pStmt->dataType)}));

    pReq->type = pStmt->dataType.type;
    pReq->bytes = calcTypeBytes(pStmt->dataType);
    pReq->colName = taosStrdup(pStmt->colName);
    pReq->refDbName = taosStrdup(pStmt->refDbName);
    pReq->refColName = taosStrdup(pStmt->refColName);
    pReq->refTbName = taosStrdup(pStmt->refTableName);
    if (pReq->colName == NULL || pReq->refDbName == NULL || pReq->refColName == NULL || pReq->refTbName == NULL) {
      return terrno;
    }
  } else {
    pReq->colName = taosStrdup(pStmt->colName);
    if (pReq->colName == NULL) {
      return terrno;
    }
    pReq->type = pStmt->dataType.type;
    pReq->flags = COL_SMA_ON;
    pReq->bytes = calcTypeBytes(pStmt->dataType);
    if (pStmt->pColOptions != NULL) {
      if (!checkColumnEncodeOrSetDefault(pReq->type, pStmt->pColOptions->encode))
        return TSDB_CODE_TSC_ENCODE_PARAM_ERROR;
      if (!checkColumnCompressOrSetDefault(pReq->type, pStmt->pColOptions->compress))
        return TSDB_CODE_TSC_COMPRESS_PARAM_ERROR;
      if (!checkColumnLevelOrSetDefault(pReq->type, pStmt->pColOptions->compressLevel))
        return TSDB_CODE_TSC_COMPRESS_LEVEL_ERROR;
      int8_t code = setColCompressByOption(pReq->type, columnEncodeVal(pStmt->pColOptions->encode),
                                           columnCompressVal(pStmt->pColOptions->compress),
                                           columnLevelVal(pStmt->pColOptions->compressLevel), true, &pReq->compress);
    }
  }

  pReq->typeMod = 0;
  if (IS_DECIMAL_TYPE(pStmt->dataType.type)) {
    pReq->typeMod = decimalCalcTypeMod(pStmt->dataType.precision, pStmt->dataType.scale);
    pReq->flags |= COL_HAS_TYPE_MOD;
  }

  return checkAlterTableByColumnType(pCxt, pStmt, pTableMeta);
}

static int32_t buildDropColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, const STableMeta* pTableMeta,
                               SVAlterTbReq* pReq) {
  if (TSDB_CHILD_TABLE == pTableMeta->tableType || TSDB_VIRTUAL_CHILD_TABLE == pTableMeta->tableType) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }

  if (2 == getNumOfColumns(pTableMeta)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DROP_COL);
  }
  const SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pStmt->colName);
  } else if (PRIMARYKEY_TIMESTAMP_COL_ID == pSchema->colId) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_CANNOT_DROP_PRIMARY_KEY);
  }

  pReq->colName = taosStrdup(pStmt->colName);
  if (NULL == pReq->colName) {
    return terrno;
  }
  pReq->colId = pSchema->colId;

  return TSDB_CODE_SUCCESS;
}

static int32_t findColRefIndex(SColRef* pColRef, const STableMeta* pTableMeta, col_id_t colId) {
  for (int32_t i = 0; i < pTableMeta->numOfColRefs; i++) {
    if (pColRef[i].hasRef && pColRef[i].id == colId) {
      return i;
    }
  }
  return -1;
}

static int32_t buildUpdateColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, const STableMeta* pTableMeta,
                                 SVAlterTbReq* pReq) {
  if (TSDB_CHILD_TABLE == pTableMeta->tableType || TSDB_VIRTUAL_CHILD_TABLE == pTableMeta->tableType) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }

  pReq->colModBytes = calcTypeBytes(pStmt->dataType);
  pReq->colModType = pStmt->dataType.type;
  const SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pStmt->colName);
  } else if (!IS_VAR_DATA_TYPE(pSchema->type) || pSchema->type != pStmt->dataType.type ||
             pSchema->bytes >= pReq->colModBytes) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_MODIFY_COL);
  }

  if ((TSDB_DATA_TYPE_VARCHAR == pStmt->dataType.type && calcTypeBytes(pStmt->dataType) > TSDB_MAX_BINARY_LEN) ||
      (TSDB_DATA_TYPE_VARBINARY == pStmt->dataType.type && calcTypeBytes(pStmt->dataType) > TSDB_MAX_BINARY_LEN) ||
      (TSDB_DATA_TYPE_NCHAR == pStmt->dataType.type && calcTypeBytes(pStmt->dataType) > TSDB_MAX_NCHAR_LEN)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
  }

  if (pTableMeta->tableInfo.rowSize + pReq->colModBytes - pSchema->bytes > TSDB_MAX_BYTES_PER_ROW) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ROW_LENGTH, TSDB_MAX_BYTES_PER_ROW);
  }

  if (TSDB_VIRTUAL_NORMAL_TABLE == pTableMeta->tableType) {
    int32_t index = findColRefIndex(pTableMeta->colRef, pTableMeta, pSchema->colId);
    if (index != -1) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
    }
  }

  pReq->colName = taosStrdup(pStmt->colName);
  if (NULL == pReq->colName) {
    return terrno;
  }
  pReq->colId = pSchema->colId;

  return TSDB_CODE_SUCCESS;
}

static int32_t buildRenameColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                 SVAlterTbReq* pReq) {
  if (TSDB_CHILD_TABLE == pTableMeta->tableType || TSDB_VIRTUAL_CHILD_TABLE == pTableMeta->tableType) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }

  if (NULL == getColSchema(pTableMeta, pStmt->colName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pStmt->colName);
  }
  if (NULL != getColSchema(pTableMeta, pStmt->newColName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DUPLICATED_COLUMN);
  }
  if (TSDB_NORMAL_TABLE == pTableMeta->tableType) {
    SArray* pTsmas = NULL;
    SName   tbName = {0};
    int32_t code = 0;
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tbName);
    if (pCxt->pMetaCache) code = getTableTsmasFromCache(pCxt->pMetaCache, &tbName, &pTsmas);
    if (TSDB_CODE_SUCCESS != code) return code;
    if (pTsmas && pTsmas->size > 0) {
      return TSDB_CODE_TSMA_MUST_BE_DROPPED;
    }
  }

  pReq->colName = taosStrdup(pStmt->colName);
  pReq->colNewName = taosStrdup(pStmt->newColName);
  if (NULL == pReq->colName || NULL == pReq->colNewName) {
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildUpdateOptionsReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, SVAlterTbReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (-1 != pStmt->pOptions->ttl) {
    pReq->updateTTL = true;
    pReq->newTTL = pStmt->pOptions->ttl;
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (pStmt->pOptions->commentNull == false) {
      pReq->newComment = taosStrdup(pStmt->pOptions->comment);
      if (NULL == pReq->newComment) {
        code = terrno;
      } else {
        pReq->newCommentLen = strlen(pReq->newComment);
      }
    } else {
      pReq->newCommentLen = -1;
    }
  }

  return code;
}

static int buildAlterTableColumnCompress(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                         SVAlterTbReq* pReq) {
  if (TSDB_CHILD_TABLE == pTableMeta->tableType || isVirtualTable(pTableMeta)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }

  const SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pStmt->colName);
  }

  pReq->colName = taosStrdup(pStmt->colName);
  pReq->colId = pSchema->colId;
  if (NULL == pReq->colName) {
    return terrno;
  }

  if (!checkColumnEncode(pStmt->pColOptions->encode)) return TSDB_CODE_TSC_ENCODE_PARAM_ERROR;
  if (!checkColumnCompress(pStmt->pColOptions->compress)) return TSDB_CODE_TSC_COMPRESS_PARAM_ERROR;
  if (!checkColumnLevel(pStmt->pColOptions->compressLevel)) return TSDB_CODE_TSC_COMPRESS_LEVEL_ERROR;
  int8_t code = setColCompressByOption(pSchema->type, columnEncodeVal(pStmt->pColOptions->encode),
                                       columnCompressVal(pStmt->pColOptions->compress),
                                       columnLevelVal(pStmt->pColOptions->compressLevel), true, &pReq->compress);
  return code;
}

static int buildAlterTableColumnRef(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                    SVAlterTbReq* pReq) {
  if (!isVirtualTable(pTableMeta)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }
  int32_t        code = TSDB_CODE_SUCCESS;
  const SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pStmt->colName);
  }

  PAR_ERR_JRET(checkColRef(pCxt, pStmt->refDbName, pStmt->refTableName, pStmt->refColName,
                           (SDataType){.type = pSchema->type, .bytes = pSchema->bytes}));

  pReq->colName = taosStrdup(pStmt->colName);
  pReq->refDbName = taosStrdup(pStmt->refDbName);
  pReq->refTbName = taosStrdup(pStmt->refTableName);
  pReq->refColName = taosStrdup(pStmt->refColName);
  if (NULL == pReq->colName || NULL == pReq->refDbName || NULL == pReq->refTbName || NULL == pReq->refColName) {
    return terrno;
  }

_return:
  return code;
}

static int buildRemoveTableColumnRef(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                     SVAlterTbReq* pReq) {
  if (!isVirtualTable(pTableMeta)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }

  int32_t code = TSDB_CODE_SUCCESS;

  const SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pStmt->colName);
  }

  pReq->colName = taosStrdup(pStmt->colName);
  if (NULL == pReq->colName) {
    return terrno;
  }

  return code;
}

static int32_t buildAlterTbReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                               SVAlterTbReq* pReq) {
  pReq->tbName = taosStrdup(pStmt->tableName);
  if (NULL == pReq->tbName) {
    return terrno;
  }
  pReq->action = pStmt->alterType;

  switch (pStmt->alterType) {
    case TSDB_ALTER_TABLE_ADD_TAG:
    case TSDB_ALTER_TABLE_DROP_TAG:
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
    case TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL:
      return buildUpdateMultiTagValReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL:
      return buildUpdateTagValReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_ADD_COLUMN:
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION:
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF:
      return buildAddColReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      return buildDropColReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      return buildUpdateColReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      return buildUpdateOptionsReq(pCxt, pStmt, pReq);
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      return buildRenameColReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS:
      return buildAlterTableColumnCompress(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_ALTER_COLUMN_REF:
      return buildAlterTableColumnRef(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_REMOVE_COLUMN_REF:
      return buildRemoveTableColumnRef(pCxt, pStmt, pTableMeta, pReq);
    default:
      break;
  }

  return TSDB_CODE_FAILED;
}

static int32_t serializeAlterTbReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, SVAlterTbReq* pReq,
                                   SArray* pArray) {
  SVgroupInfo vg = {0};
  int32_t     code = getTableHashVgroup(pCxt, pStmt->dbName, pStmt->tableName, &vg);
  int         tlen = 0;
  if (TSDB_CODE_SUCCESS == code) {
    tEncodeSize(tEncodeSVAlterTbReq, pReq, tlen, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tlen += sizeof(SMsgHead);
    void* pMsg = taosMemoryMalloc(tlen);
    if (NULL == pMsg) {
      return terrno;
    }
    ((SMsgHead*)pMsg)->vgId = htonl(vg.vgId);
    ((SMsgHead*)pMsg)->contLen = htonl(tlen);
    void*    pBuf = POINTER_SHIFT(pMsg, sizeof(SMsgHead));
    SEncoder coder = {0};
    tEncoderInit(&coder, pBuf, tlen - sizeof(SMsgHead));
    if (TSDB_CODE_SUCCESS != tEncodeSVAlterTbReq(&coder, pReq)) {
      taosMemoryFree(pMsg);
      return terrno;
    }
    tEncoderClear(&coder);

    SVgDataBlocks* pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
    if (NULL == pVgData) {
      taosMemoryFree(pMsg);
      return terrno;
    }
    pVgData->vg = vg;
    pVgData->pData = pMsg;
    pVgData->size = tlen;
    pVgData->numOfTables = 1;
    if (NULL == taosArrayPush(pArray, &pVgData)) {
      code = terrno;
    }
  }

  return code;
}

static int32_t buildModifyVnodeArray(STranslateContext* pCxt, SAlterTableStmt* pStmt, SVAlterTbReq* pReq,
                                     SArray** pArray) {
  SArray* pTmpArray = taosArrayInit(1, sizeof(void*));
  if (NULL == pTmpArray) {
    return terrno;
  }

  int32_t code = serializeAlterTbReq(pCxt, pStmt, pReq, pTmpArray);
  if (TSDB_CODE_SUCCESS == code) {
    *pArray = pTmpArray;
  } else {
    taosArrayDestroy(pTmpArray);
  }

  return code;
}

static void destoryAlterTbReq(SVAlterTbReq* pReq) {
  taosMemoryFree(pReq->tbName);
  taosMemoryFree(pReq->colName);
  taosMemoryFree(pReq->colNewName);
  taosMemoryFree(pReq->tagName);
  taosMemoryFree(pReq->newComment);
  taosMemoryFree(pReq->refDbName);
  taosMemoryFree(pReq->refTbName);
  taosMemoryFree(pReq->refColName);
  for (int i = 0; i < taosArrayGetSize(pReq->pTagArray); ++i) {
    STagVal* p = (STagVal*)taosArrayGet(pReq->pTagArray, i);
    if (IS_VAR_DATA_TYPE(p->type)) {
      taosMemoryFreeClear(p->pData);
    }
  }
  if (pReq->action == TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL) {
    taosArrayDestroyEx(pReq->pMultiTag, tfreeMultiTagUpateVal);
  }

  taosArrayDestroy(pReq->pTagArray);
  if (pReq->tagFree) tTagFree((STag*)pReq->pTagVal);
}

static int32_t rewriteAlterTableImpl(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                     SQuery* pQuery, bool isVirtual) {
  if (isVirtual) {
    if (!isVirtualTable(pTableMeta) && !isVirtualSTable(pTableMeta)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE,
                                     "can not alter non-virtual table using ALTER VTABLE");
    }
  }

  if (TSDB_SUPER_TABLE == pTableMeta->tableType) {
    return TSDB_CODE_SUCCESS;
  } else if (TSDB_CHILD_TABLE != pTableMeta->tableType && TSDB_NORMAL_TABLE != pTableMeta->tableType &&
             !isVirtualTable(pTableMeta)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }
  if (pStmt->pOptions && (pStmt->pOptions->keep >= 0 || pStmt->pOptions->pKeepNode != NULL)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
                                   "only super table can alter keep duration");
  }

  const SSchema* pSchema = getNormalColSchema(pTableMeta, pStmt->colName);
  if (hasPkInTable(pTableMeta) && pSchema && (pSchema->flags & COL_IS_KEY) &&
      (TSDB_ALTER_TABLE_DROP_COLUMN == pStmt->alterType || TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES == pStmt->alterType ||
       TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME == pStmt->alterType)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PK_OP);
  }

  SVAlterTbReq req = {0};
  int32_t      code = buildAlterTbReq(pCxt, pStmt, pTableMeta, &req);

  SArray* pArray = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = buildModifyVnodeArray(pCxt, pStmt, &req, &pArray);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteToVnodeModifyOpStmt(pQuery, pArray);
  }
  destoryAlterTbReq(&req);
  return code;
}

static int32_t rewriteAlterTable(STranslateContext* pCxt, SQuery* pQuery, bool isVirtual) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SAlterTableStmt* pStmt = (SAlterTableStmt*)pQuery->pRoot;

  if (IS_SYS_DBNAME(pStmt->dbName)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION,
                                   "Cannot alter table of system database: `%s`.`%s`", pStmt->dbName, pStmt->tableName);
  }

  if (pStmt->dataType.type == TSDB_DATA_TYPE_JSON && pStmt->alterType == TSDB_ALTER_TABLE_ADD_COLUMN) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COL_JSON);
  }

  STableMeta* pTableMeta = NULL;
  PAR_ERR_JRET(getTableMeta(pCxt, pStmt->dbName, pStmt->tableName, &pTableMeta));
  PAR_ERR_JRET(rewriteAlterTableImpl(pCxt, pStmt, pTableMeta, pQuery, isVirtual));

_return:
  taosMemoryFree(pTableMeta);
  return code;
}

static int32_t rewriteAlterVirtualTable(STranslateContext* pCxt, SQuery* pQuery) {
  return rewriteAlterTable(pCxt, pQuery, true);
}

static int32_t buildCreateVTableDataBlock(const SCreateVTableStmt* pStmt, const SVgroupInfo* pInfo, SArray* pBufArray) {
  SVgroupCreateTableBatch tbatch = {0};
  int32_t                 code = TSDB_CODE_SUCCESS;
  PAR_ERR_JRET(buildVirtualTableBatchReq(pStmt, pInfo, &tbatch));
  PAR_ERR_JRET(serializeVgroupCreateTableBatch(&tbatch, pBufArray));

_return:
  destroyCreateTbReqBatch(&tbatch);
  return code;
}

static int32_t buildCreateVSubTableDataBlock(const SCreateVSubTableStmt* pStmt, const SVgroupInfo* pInfo,
                                             SArray* pBufArray, STableMeta* pStbMeta, SArray* tagName, uint8_t tagNum,
                                             const STag* pTag) {
  SVgroupCreateTableBatch tbatch = {0};
  int32_t                 code = TSDB_CODE_SUCCESS;
  PAR_ERR_JRET(buildVirtualSubTableBatchReq(pStmt, pStbMeta, tagName, tagNum, pTag, pInfo, &tbatch));
  PAR_ERR_JRET(serializeVgroupCreateTableBatch(&tbatch, pBufArray));

_return:
  destroyCreateTbReqBatch(&tbatch);
  return code;
}

static int32_t checkCreateVirtualTable(STranslateContext* pCxt, SCreateVTableStmt* pStmt) {
  if (NULL != strchr(pStmt->tableName, '.')) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME,
                                   "The table name cannot contain '.'");
  }

  if (IS_SYS_DBNAME(pStmt->dbName)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_TSC_INVALID_OPERATION,
                                   "Cannot create table of system database: `%s`.`%s`", pStmt->dbName,
                                   pStmt->tableName);
  }

  PAR_ERR_RET(checkVTableSchema(pCxt, pStmt));

  PAR_ERR_RET(checkColumnOptions(pStmt->pCols, true));

  PAR_ERR_RET(checkColumnType(pStmt->pCols, 1));

  PAR_ERR_RET(checkColumnType(pStmt->pCols, 1));

  PAR_ERR_RET(checkColumnType(pStmt->pCols, 1));

  if (pCxt->pParseCxt->biMode != 0) {
    PAR_ERR_RET(biCheckCreateTableTbnameCol(pCxt, NULL, pStmt->pCols));
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t rewriteCreateVirtualTable(STranslateContext* pCxt, SQuery* pQuery) {
  SCreateVTableStmt* pStmt = (SCreateVTableStmt*)pQuery->pRoot;
  int32_t            code = TSDB_CODE_SUCCESS;
  SVgroupInfo        info = {0};
  SName              name = {0};
  SArray*            pBufArray = NULL;
  SNode*             pNode = NULL;
  int32_t            index = 0;

  PAR_ERR_JRET(checkCreateVirtualTable(pCxt, pStmt));

  pBufArray = taosArrayInit(1, POINTER_BYTES);
  if (NULL == pBufArray) {
    PAR_ERR_JRET(terrno);
  }

  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);

  FOREACH(pNode, pStmt->pCols) {
    SColumnDefNode* pColNode = (SColumnDefNode*)pNode;
    SColumnOptions* pColOptions = (SColumnOptions*)pColNode->pOptions;
    if (pColOptions->hasRef) {
      if (index == 0) {
        PAR_ERR_JRET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_VTABLE_PRIMTS_HAS_REF));
      }
      if (IS_DECIMAL_TYPE(pColNode->dataType.type)) {
        PAR_ERR_JRET(generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_VTABLE_NOT_SUPPORT_DATA_TYPE));
      }
      PAR_ERR_JRET(
          checkColRef(pCxt, pColOptions->refDb, pColOptions->refTable, pColOptions->refColumn,
                      (SDataType){.type = pColNode->dataType.type, .bytes = calcTypeBytes(pColNode->dataType)}));
    }
    index++;
  }

  PAR_ERR_JRET(getTableHashVgroupImpl(pCxt, &name, &info));
  PAR_ERR_JRET(collectUseTable(&name, pCxt->pTargetTables));

  PAR_ERR_JRET(buildCreateVTableDataBlock(pStmt, &info, pBufArray));
  PAR_ERR_JRET(rewriteToVnodeModifyOpStmt(pQuery, pBufArray));

  return code;
_return:
  destroyCreateTbReqArray(pBufArray);
  return code;
}

static int32_t rewriteCreateVirtualSubTable(STranslateContext* pCxt, SQuery* pQuery) {
  int32_t               code = TSDB_CODE_SUCCESS;
  SCreateVSubTableStmt* pStmt = (SCreateVSubTableStmt*)pQuery->pRoot;
  SVgroupInfo           info = {0};
  SName                 name = {0};
  SArray*               pBufArray = NULL;
  STableMeta*           pSuperTableMeta = NULL;
  STag*                 pTag = NULL;
  SArray*               tagName = NULL;
  SNode*                pCol = NULL;

  PAR_ERR_JRET(checkCreateVSubTable(pCxt, pStmt));

  pBufArray = taosArrayInit(1, POINTER_BYTES);
  tagName = taosArrayInit(8, TSDB_COL_NAME_LEN);
  if (NULL == pBufArray || NULL == tagName) {
    PAR_ERR_JRET(terrno);
  }

  PAR_ERR_JRET(getTableMeta(pCxt, pStmt->useDbName, pStmt->useTableName, &pSuperTableMeta));

  if (!pSuperTableMeta->virtualStb) {
    PAR_ERR_JRET(TSDB_CODE_VTABLE_NOT_VIRTUAL_SUPER_TABLE);
  }

  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);

  if (pStmt->pSpecificColRefs) {
    FOREACH(pCol, pStmt->pSpecificColRefs) {
      SColumnRefNode* pColRef = (SColumnRefNode*)pCol;
      const SSchema*  pSchema = getColSchema(pSuperTableMeta, pColRef->colName);
      if (NULL == pSchema) {
        PAR_ERR_JRET(TSDB_CODE_PAR_INVALID_COLUMN);
      }
      if (pSchema->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        PAR_ERR_JRET(TSDB_CODE_VTABLE_PRIMTS_HAS_REF);
      }
      PAR_ERR_JRET(checkColRef(pCxt, pColRef->refDbName, pColRef->refTableName, pColRef->refColName,
                               (SDataType){.type = pSchema->type, .bytes = pSchema->bytes}));
    }
  } else if (pStmt->pColRefs) {
    int32_t index = 1;
    FOREACH(pCol, pStmt->pColRefs) {
      SColumnRefNode* pColRef = (SColumnRefNode*)pCol;
      PAR_ERR_JRET(checkColRef(
          pCxt, pColRef->refDbName, pColRef->refTableName, pColRef->refColName,
          (SDataType){.type = pSuperTableMeta->schema[index].type, .bytes = pSuperTableMeta->schema[index].bytes}));
      index++;
    }
  } else {
    // no column reference, do nothing
  }

  PAR_ERR_JRET(getTableHashVgroupImpl(pCxt, &name, &info));
  PAR_ERR_JRET(collectUseTable(&name, pCxt->pTargetTables));

  if (NULL != pStmt->pSpecificTags) {
    PAR_ERR_JRET(
        buildKVRowForBindTags(pCxt, pStmt->pSpecificTags, pStmt->pValsOfTags, pSuperTableMeta, &pTag, tagName));
  } else {
    PAR_ERR_JRET(buildKVRowForAllTags(pCxt, pStmt->pValsOfTags, pSuperTableMeta, &pTag, tagName));
  }

  PAR_ERR_JRET(buildCreateVSubTableDataBlock(pStmt, &info, pBufArray, pSuperTableMeta, tagName,
                                             taosArrayGetSize(tagName), pTag));
  PAR_ERR_JRET(rewriteToVnodeModifyOpStmt(pQuery, pBufArray));

  taosMemoryFreeClear(pSuperTableMeta);
  taosArrayDestroy(tagName);
  return code;
_return:
  destroyCreateTbReqArray(pBufArray);
  taosArrayDestroy(tagName);
  taosMemoryFreeClear(pSuperTableMeta);
  return code;
}

static int32_t serializeFlushVgroup(SVgroupInfo* pVg, SArray* pBufArray) {
  int32_t len = sizeof(SMsgHead);
  void*   buf = taosMemoryMalloc(len);
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == buf) {
    return terrno;
  }
  ((SMsgHead*)buf)->vgId = htonl(pVg->vgId);
  ((SMsgHead*)buf)->contLen = htonl(len);

  SVgDataBlocks* pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
  if (NULL == pVgData) {
    taosMemoryFree(buf);
    return terrno;
  }
  pVgData->vg = *pVg;
  pVgData->pData = buf;
  pVgData->size = len;
  if (NULL == taosArrayPush(pBufArray, &pVgData)) {
    code = terrno;
  }

  return code;
}

static int32_t serializeFlushDb(SArray* pVgs, SArray** pOutput) {
  int32_t numOfVgs = taosArrayGetSize(pVgs);

  SArray* pBufArray = taosArrayInit(numOfVgs, sizeof(void*));
  if (NULL == pBufArray) {
    return terrno;
  }

  for (int32_t i = 0; i < numOfVgs; ++i) {
    int32_t code = serializeFlushVgroup((SVgroupInfo*)taosArrayGet(pVgs, i), pBufArray);
    if (TSDB_CODE_SUCCESS != code) {
      taosArrayDestroy(pBufArray);
      return code;
    }
  }

  *pOutput = pBufArray;
  return TSDB_CODE_SUCCESS;
}

static int32_t rewriteFlushDatabase(STranslateContext* pCxt, SQuery* pQuery) {
  SFlushDatabaseStmt* pStmt = (SFlushDatabaseStmt*)pQuery->pRoot;

  SArray* pBufArray = NULL;
  SArray* pVgs = NULL;
  int32_t code = getDBVgInfo(pCxt, pStmt->dbName, &pVgs);
  if (TSDB_CODE_SUCCESS == code) {
    code = serializeFlushDb(pVgs, &pBufArray);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
  }
  if (TSDB_CODE_SUCCESS != code) {
    taosArrayDestroy(pBufArray);
  }
  taosArrayDestroy(pVgs);
  return code;
}

static int32_t rewriteShowCompacts(STranslateContext* pCxt, SQuery* pQuery) {
  SShowCompactsStmt* pShow = (SShowCompactsStmt*)(pQuery->pRoot);
  SSelectStmt*       pStmt = NULL;
  int32_t            code = createSelectStmtForShow(QUERY_NODE_SHOW_COMPACTS_STMT, &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->showRewrite = true;
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  }
  return code;
}

static int32_t rewriteShowScans(STranslateContext* pCxt, SQuery* pQuery) {
  SShowScansStmt* pShow = (SShowScansStmt*)(pQuery->pRoot);
  SSelectStmt*    pStmt = NULL;
  int32_t         code = createSelectStmtForShow(QUERY_NODE_SHOW_SCANS_STMT, &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->showRewrite = true;
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  }
  return code;
}

static int32_t rewriteShowCompactDetailsStmt(STranslateContext* pCxt, SQuery* pQuery) {
  SShowCompactDetailsStmt* pShow = (SShowCompactDetailsStmt*)(pQuery->pRoot);
  SSelectStmt*             pStmt = NULL;
  int32_t                  code = createSelectStmtForShow(QUERY_NODE_SHOW_COMPACT_DETAILS_STMT, &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pShow->pId) {
      code = createOperatorNode(OP_TYPE_EQUAL, "compact_id", pShow->pId, &pStmt->pWhere);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->showRewrite = true;
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  }
  return code;
}

static int32_t rewriteShowRetentionDetailsStmt(STranslateContext* pCxt, SQuery* pQuery) {
  SShowRetentionDetailsStmt* pShow = (SShowRetentionDetailsStmt*)(pQuery->pRoot);
  SSelectStmt*               pStmt = NULL;
  int32_t                    code = createSelectStmtForShow(QUERY_NODE_SHOW_RETENTION_DETAILS_STMT, &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pShow->pId) {
      code = createOperatorNode(OP_TYPE_EQUAL, "retention_id", pShow->pId, &pStmt->pWhere);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->showRewrite = true;
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  }
  return code;
}

static int32_t rewriteShowScanDetailsStmt(STranslateContext* pCxt, SQuery* pQuery) {
  SShowScanDetailsStmt* pShow = (SShowScanDetailsStmt*)(pQuery->pRoot);
  SSelectStmt*          pStmt = NULL;
  int32_t               code = createSelectStmtForShow(QUERY_NODE_SHOW_SCAN_DETAILS_STMT, &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pShow->pScanId) {
      code = createOperatorNode(OP_TYPE_EQUAL, "scan_id", pShow->pScanId, &pStmt->pWhere);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->showRewrite = true;
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  }
  return code;
}

static int32_t rewriteShowSsMigrates(STranslateContext* pCxt, SQuery* pQuery) {
  SShowSsMigratesStmt* pShow = (SShowSsMigratesStmt*)(pQuery->pRoot);
  SSelectStmt*         pStmt = NULL;
  int32_t              code = createSelectStmtForShow(QUERY_NODE_SHOW_SSMIGRATES_STMT, &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->showRewrite = true;
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  }
  return code;
}

static int32_t rewriteShowTransactionDetailsStmt(STranslateContext* pCxt, SQuery* pQuery) {
  SShowTransactionDetailsStmt* pShow = (SShowTransactionDetailsStmt*)(pQuery->pRoot);
  SSelectStmt*                 pStmt = NULL;
  int32_t                      code = createSelectStmtForShow(QUERY_NODE_SHOW_TRANSACTION_DETAILS_STMT, &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pShow->pTransactionId) {
      code = createOperatorNode(OP_TYPE_EQUAL, "transaction_id", pShow->pTransactionId, &pStmt->pWhere);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->showRewrite = true;
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  }
  return code;
}

static int32_t createParWhenThenNode(SNode* pWhen, SNode* pThen, SNode** ppResWhenThen) {
  SWhenThenNode* pWThen = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_WHEN_THEN, (SNode**)&pWThen);
  if (TSDB_CODE_SUCCESS != code) return code;

  pWThen->pWhen = pWhen;
  pWThen->pThen = pThen;
  *ppResWhenThen = (SNode*)pWThen;
  return TSDB_CODE_SUCCESS;
}

static int32_t createParCaseWhenNode(SNode* pCase, SNodeList* pWhenThenList, SNode* pElse, const char* pAias,
                                     SNode** ppResCaseWhen) {
  SCaseWhenNode* pCaseWhen = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_CASE_WHEN, (SNode**)&pCaseWhen);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  pCaseWhen->pCase = pCase;
  pCaseWhen->pWhenThenList = pWhenThenList;
  pCaseWhen->pElse = pElse;
  if (pAias) {
    tstrncpy(pCaseWhen->node.aliasName, pAias, TSDB_COL_NAME_LEN);
    tstrncpy(pCaseWhen->node.userAlias, pAias, TSDB_COL_NAME_LEN);
  }
  *ppResCaseWhen = (SNode*)pCaseWhen;
  return TSDB_CODE_SUCCESS;
}

static int32_t createParFunctionNode(const char* pFunName, const char* pAias, SNodeList* pParameterList,
                                     SNode** ppResFunc) {
  SFunctionNode* pFunc = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  tstrncpy(pFunc->functionName, pFunName, TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.aliasName, pAias, TSDB_COL_NAME_LEN);
  tstrncpy(pFunc->node.userAlias, pAias, TSDB_COL_NAME_LEN);
  pFunc->pParameterList = pParameterList;
  *ppResFunc = (SNode*)pFunc;
  return TSDB_CODE_SUCCESS;
}

static int32_t createParListNode(SNode* pItem, SNodeList** ppResList) {
  SNodeList* pList = NULL;
  int32_t    code = nodesMakeList(&pList);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  CHECK_RES_OUT_OF_MEM(nodesListAppend(pList, pItem));
  *ppResList = pList;
  return TSDB_CODE_SUCCESS;
}

static int32_t createParTempTableNode(SSelectStmt* pSubquery, SNode** ppResTempTable) {
  STempTableNode* pTempTable = NULL;
  int32_t         code = nodesMakeNode(QUERY_NODE_TEMP_TABLE, (SNode**)&pTempTable);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  pTempTable->pSubquery = (SNode*)pSubquery;
  taosRandStr(pTempTable->table.tableAlias, 8);
  tstrncpy(pSubquery->stmtName, pTempTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
  pSubquery->isSubquery = true;
  *ppResTempTable = (SNode*)pTempTable;
  return TSDB_CODE_SUCCESS;
}

static int32_t rewriteShowAliveStmt(STranslateContext* pCxt, SQuery* pQuery) {
  int32_t code = TSDB_CODE_SUCCESS;
  char*   pDbName = ((SShowAliveStmt*)pQuery->pRoot)->dbName;
  if (pDbName && pDbName[0] != 0) {
    SDbCfgInfo dbCfg = {0};
    code = getDBCfg(pCxt, pDbName, &dbCfg);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  SValueNode* pValNode = NULL;
  code = nodesMakeValueNodeFromString("leader", &pValNode);
  if (TSDB_CODE_SUCCESS != code) return code;

  SNode* pCond1 = NULL;
  SNode* pCond2 = NULL;
  SNode* pCond3 = NULL;
  SNode* pCond4 = NULL;
  code = createOperatorNode(OP_TYPE_EQUAL, "v1_status", (SNode*)pValNode, &pCond1);
  if (TSDB_CODE_SUCCESS == code) code = createOperatorNode(OP_TYPE_EQUAL, "v2_status", (SNode*)pValNode, &pCond2);
  if (TSDB_CODE_SUCCESS == code) code = createOperatorNode(OP_TYPE_EQUAL, "v3_status", (SNode*)pValNode, &pCond3);
  if (TSDB_CODE_SUCCESS == code) code = createOperatorNode(OP_TYPE_EQUAL, "v4_status", (SNode*)pValNode, &pCond4);
  nodesDestroyNode((SNode*)pValNode);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pCond1);
    nodesDestroyNode(pCond2);
    nodesDestroyNode(pCond3);
    nodesDestroyNode(pCond4);
    return code;
  }
  // pCond1-4 need to free if error

  SNode* pTemp1 = NULL;
  SNode* pTemp2 = NULL;
  SNode* pFullCond = NULL;
  code = createLogicCondNode(&pCond1, &pCond2, &pTemp1, LOGIC_COND_TYPE_OR);
  if (TSDB_CODE_SUCCESS == code) code = createLogicCondNode(&pTemp1, &pCond3, &pTemp2, LOGIC_COND_TYPE_OR);
  if (TSDB_CODE_SUCCESS == code) code = createLogicCondNode(&pTemp2, &pCond4, &pFullCond, LOGIC_COND_TYPE_OR);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pCond1);
    nodesDestroyNode(pCond2);
    nodesDestroyNode(pCond3);
    nodesDestroyNode(pCond4);
    nodesDestroyNode(pTemp1);
    nodesDestroyNode(pTemp2);
    nodesDestroyNode(pFullCond);
    return code;
  }

  // only pFullCond needs to free if err

  SNode* pThen = NULL;
  code = nodesMakeValueNodeFromInt32(1, &pThen);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pFullCond);
    return code;
  }

  // pFullCond and pThen need to free

  SNode* pWhenThen = NULL;
  code = createParWhenThenNode(pFullCond, pThen, &pWhenThen);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pFullCond);
    nodesDestroyNode(pThen);
    return code;
  }
  // pWhenThen needs to free

  SNodeList* pWhenThenlist = NULL;
  code = createParListNode(pWhenThen, &pWhenThenlist);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pWhenThen);
    return code;
  }

  // pWhenThenlist needs to free

  SNode* pElse = NULL;
  code = nodesMakeValueNodeFromInt32(0, &pElse);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pWhenThenlist);
    return code;
  }

  // pWhenThenlist and pElse need to free

  // case when (v1_status = "leader" or v2_status = "leader"  or v3_status = "leader" or v4_status = "leader")  then 1
  // else 0 end
  SNode* pCaseWhen = NULL;
  code = createParCaseWhenNode(NULL, pWhenThenlist, pElse, NULL, &pCaseWhen);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pWhenThenlist);
    nodesDestroyNode(pElse);
    return code;
  }

  // pCaseWhen needs to free

  SNodeList* pParaList = NULL;
  code = createParListNode(pCaseWhen, &pParaList);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pCaseWhen);
    return code;
  }

  // pParaList needs to free

  // sum( case when ... end) as leader_col
  SNode*      pSumFun = NULL;
  const char* pSumColAlias = "leader_col";
  code = createParFunctionNode("sum", pSumColAlias, pParaList, &pSumFun);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pParaList);
    return code;
  }

  // pSumFun needs to free

  SNode* pPara1 = NULL;
  code = nodesMakeValueNodeFromInt32(1, &pPara1);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pSumFun);
    return code;
  }

  // pSumFun and pPara1 need to free
  pParaList = NULL;
  code = createParListNode(pPara1, &pParaList);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pSumFun);
    nodesDestroyNode(pPara1);
    return code;
  }

  // pSumFun and pParaList need to free

  // count(1) as count_col
  SNode*      pCountFun = NULL;
  const char* pCountColAlias = "count_col";
  code = createParFunctionNode("count", pCountColAlias, pParaList, &pCountFun);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pSumFun);
    nodesDestroyList(pParaList);
    return code;
  }

  // pSumFun and pCountFun need to free

  SNodeList* pProjList = NULL;
  code = createParListNode(pSumFun, &pProjList);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pSumFun);
    nodesDestroyNode(pCountFun);
    return code;
  }

  // pProjList and pCountFun need to free

  code = nodesListStrictAppend(pProjList, pCountFun);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pProjList);
    return code;
  }

  // pProjList needs to free

  SSelectStmt* pSubSelect = NULL;
  // select sum( case when .... end) as leader_col, count(*) as count_col from information_schema.ins_vgroups
  code = createSimpleSelectStmtFromProjList(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_VGROUPS, pProjList, &pSubSelect);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pProjList);
    return code;
  }

  // pSubSelect needs to free

  if (pDbName && pDbName[0] != 0) {
    // for show db.alive
    // select sum( case when .... end) as leader_col, count(*) as count_col from information_schema.ins_vgroups where
    // db_name = "..."
    SNode* pDbCond = NULL;
    code = nodesMakeValueNodeFromString(pDbName, &pValNode);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pSubSelect);
      return code;
    }
    // pSubSelect and pValNode need to free

    code = createOperatorNode(OP_TYPE_EQUAL, "db_name", (SNode*)pValNode, &pDbCond);
    nodesDestroyNode((SNode*)pValNode);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pSubSelect);
      return code;
    }

    pCxt->showRewrite = false;
    pQuery->showRewrite = false;
    pSubSelect->pWhere = pDbCond;
    // pSubSelect need to free
  }

  // pSubSelect need to free

  pCond1 = NULL;
  code = createParOperatorNode(OP_TYPE_EQUAL, pSumColAlias, pCountColAlias, &pCond1);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    return code;
  }
  // pSubSelect and pCond1 need to free

  pCond2 = NULL;
  SNode* pTempVal = NULL;
  code = nodesMakeValueNodeFromInt32(0, &pTempVal);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyNode(pCond1);
    return code;
  }
  // pSubSelect, pCond1, pTempVal need to free

  code = createOperatorNode(OP_TYPE_GREATER_THAN, pSumColAlias, pTempVal, &pCond2);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyNode(pCond1);
    nodesDestroyNode(pTempVal);
    return code;
  }
  // pSubSelect, pCond1, pCond2, pTempVal need to free

  // leader_col = count_col and leader_col > 0
  pTemp1 = NULL;
  code = createLogicCondNode(&pCond1, &pCond2, &pTemp1, LOGIC_COND_TYPE_AND);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyNode(pCond1);
    nodesDestroyNode(pCond2);
    nodesDestroyNode(pTempVal);
    return code;
  }

  SNode* pCondIsNULL = NULL;
  code = createIsOperatorNode(OP_TYPE_IS_NULL, pSumColAlias, &pCondIsNULL);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyNode(pTemp1);
    nodesDestroyNode(pTempVal);
    return code;
  }

  SNode* pCondFull1 = NULL;
  code = createLogicCondNode(&pTemp1, &pCondIsNULL, &pCondFull1, LOGIC_COND_TYPE_OR);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyNode(pTemp1);
    nodesDestroyNode(pTempVal);
    nodesDestroyNode(pCondIsNULL);
    return code;
  }

  // pSubSelect, pCondFull1, pTempVal need to free

  pThen = NULL;
  code = nodesMakeValueNodeFromInt32(1, &pThen);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyNode(pCondFull1);
    nodesDestroyNode(pTempVal);
    return code;
  }
  // pSubSelect, pCondFull1, pThen, pTempVal need to free

  pWhenThen = NULL;
  code = createParWhenThenNode(pCondFull1, pThen, &pWhenThen);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyNode(pCondFull1);
    nodesDestroyNode(pThen);
    nodesDestroyNode(pTempVal);
    return code;
  }
  // pSubSelect, pWhenThen, pTempVal need to free

  pWhenThenlist = NULL;
  code = createParListNode(pWhenThen, &pWhenThenlist);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyNode(pWhenThen);
    nodesDestroyNode(pTempVal);
    return code;
  }

  // pSubSelect, pWhenThenlist, pTempVal need to free

  pCond1 = NULL;
  code = createParOperatorNode(OP_TYPE_LOWER_THAN, pSumColAlias, pCountColAlias, &pCond1);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyList(pWhenThenlist);
    nodesDestroyNode(pTempVal);
    return code;
  }
  // pSubSelect, pWhenThenlist, pCond1, pTempVal need to free

  pCond2 = NULL;
  code = createOperatorNode(OP_TYPE_GREATER_THAN, pSumColAlias, pTempVal, &pCond2);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyList(pWhenThenlist);
    nodesDestroyNode(pTempVal);
    nodesDestroyNode(pCond1);
    return code;
  }
  // pSubSelect, pWhenThenlist, pCond1, pTempVal, pCond2 need to free

  // leader_col < count_col and leader_col > 0
  pTemp2 = NULL;
  code = createLogicCondNode(&pCond1, &pCond2, &pTemp2, LOGIC_COND_TYPE_AND);
  nodesDestroyNode((SNode*)pTempVal);

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyList(pWhenThenlist);
    nodesDestroyNode(pCond1);
    nodesDestroyNode(pCond2);
    return code;
  }

  // pSubSelect, pWhenThenlist, pTemp2 need to free

  pThen = NULL;
  code = nodesMakeValueNodeFromInt32(2, &pThen);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyList(pWhenThenlist);
    nodesDestroyNode(pTemp2);
    return code;
  }
  // pSubSelect, pWhenThenlist, pTemp2, pThen need to free

  pWhenThen = NULL;
  code = createParWhenThenNode(pTemp2, pThen, &pWhenThen);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyList(pWhenThenlist);
    nodesDestroyNode(pTemp2);
    nodesDestroyNode(pThen);
    return code;
  }
  // pSubSelect, pWhenThenlist, pWhenThen need to free

  code = nodesListStrictAppend(pWhenThenlist, pWhenThen);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyList(pWhenThenlist);
    return code;
  }
  // pSubSelect, pWhenThenlist need to free

  // case when leader_col = count_col and leader_col > 0 then 1 when leader_col < count_col and leader_col > 0 then 2
  // else 0 end as status
  pElse = NULL;
  code = nodesMakeValueNodeFromInt32(0, &pElse);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyList(pWhenThenlist);
    return code;
  }
  // pSubSelect, pWhenThenlist, pElse need to free

  pCaseWhen = NULL;
  code = createParCaseWhenNode(NULL, pWhenThenlist, pElse, "status", &pCaseWhen);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyList(pWhenThenlist);
    nodesDestroyNode(pElse);
    return code;
  }
  // pSubSelect, pCaseWhen need to free

  pProjList = NULL;
  code = createParListNode(pCaseWhen, &pProjList);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyNode(pCaseWhen);
    return code;
  }
  // pSubSelect, pProjList need to free

  SNode* pTempTblNode = NULL;
  code = createParTempTableNode(pSubSelect, &pTempTblNode);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pSubSelect);
    nodesDestroyList(pProjList);
    return code;
  }
  // pTempTblNode, pProjList need to free

  SSelectStmt* pStmt = NULL;
  code = nodesMakeNode(QUERY_NODE_SELECT_STMT, (SNode**)&pStmt);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pTempTblNode);
    nodesDestroyList(pProjList);
    return code;
  }

  pStmt->pProjectionList = pProjList;
  pStmt->pFromTable = pTempTblNode;
  snprintf(pStmt->stmtName, TSDB_TABLE_NAME_LEN, "%p", pStmt);

  nodesDestroyNode(pQuery->pRoot);
  pQuery->pRoot = (SNode*)pStmt;
  return TSDB_CODE_SUCCESS;
}

static int32_t rewriteQuery(STranslateContext* pCxt, SQuery* pQuery) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pQuery->pRoot)) {
    case QUERY_NODE_SHOW_LICENCES_STMT:
    case QUERY_NODE_SHOW_DATABASES_STMT:
    case QUERY_NODE_SHOW_TABLES_STMT:
    case QUERY_NODE_SHOW_STABLES_STMT:
    case QUERY_NODE_SHOW_USERS_STMT:
    case QUERY_NODE_SHOW_USERS_FULL_STMT:
    case QUERY_NODE_SHOW_DNODES_STMT:
    case QUERY_NODE_SHOW_MNODES_STMT:
    case QUERY_NODE_SHOW_MODULES_STMT:
    case QUERY_NODE_SHOW_QNODES_STMT:
    case QUERY_NODE_SHOW_ANODES_STMT:
    case QUERY_NODE_SHOW_ANODES_FULL_STMT:
    case QUERY_NODE_SHOW_FUNCTIONS_STMT:
    case QUERY_NODE_SHOW_INDEXES_STMT:
    case QUERY_NODE_SHOW_BACKUP_NODES_STMT:
    case QUERY_NODE_SHOW_SNODES_STMT:
    case QUERY_NODE_SHOW_BNODES_STMT:
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
    case QUERY_NODE_SHOW_QUERIES_STMT:
    case QUERY_NODE_SHOW_CLUSTER_STMT:
    case QUERY_NODE_SHOW_TOPICS_STMT:
    case QUERY_NODE_SHOW_TRANSACTIONS_STMT:
    case QUERY_NODE_SHOW_APPS_STMT:
    case QUERY_NODE_SHOW_CONSUMERS_STMT:
    case QUERY_NODE_SHOW_SUBSCRIPTIONS_STMT:
    case QUERY_NODE_SHOW_USER_PRIVILEGES_STMT:
    case QUERY_NODE_SHOW_VIEWS_STMT:
    case QUERY_NODE_SHOW_GRANTS_FULL_STMT:
    case QUERY_NODE_SHOW_GRANTS_LOGS_STMT:
    case QUERY_NODE_SHOW_CLUSTER_MACHINES_STMT:
    case QUERY_NODE_SHOW_ARBGROUPS_STMT:
    case QUERY_NODE_SHOW_ENCRYPTIONS_STMT:
    case QUERY_NODE_SHOW_TSMAS_STMT:
    case QUERY_NODE_SHOW_MOUNTS_STMT:
    case QUERY_NODE_SHOW_RSMAS_STMT:
    case QUERY_NODE_SHOW_RETENTIONS_STMT:
      code = rewriteShow(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_STREAMS_STMT:
      code = rewriteShowStreams(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_VTABLES_STMT:
      code = rewriteShowVtables(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_USAGE_STMT:
      code = rewriteShowDBUsage(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_TAGS_STMT:
      code = rewriteShowTags(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_VGROUPS_STMT:
      code = rewriteShowVgroups(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_TABLE_TAGS_STMT:
      code = rewriteShowStableTags(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_DNODE_VARIABLES_STMT:
      code = rewriteShowDnodeVariables(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_VNODES_STMT:
      code = rewriteShowVnodes(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_TABLE_DISTRIBUTED_STMT:
      code = rewriteShowTableDist(pCxt, pQuery);
      break;
    case QUERY_NODE_CREATE_TABLE_STMT:
      if (NULL == ((SCreateTableStmt*)pQuery->pRoot)->pTags) {
        code = rewriteCreateTable(pCxt, pQuery);
      }
      break;
    case QUERY_NODE_CREATE_VIRTUAL_TABLE_STMT:
      code = rewriteCreateVirtualTable(pCxt, pQuery);
      break;
    case QUERY_NODE_CREATE_VIRTUAL_SUBTABLE_STMT:
      code = rewriteCreateVirtualSubTable(pCxt, pQuery);
      break;
    case QUERY_NODE_CREATE_MULTI_TABLES_STMT:
      code = rewriteCreateMultiTable(pCxt, pQuery);
      break;
    case QUERY_NODE_CREATE_SUBTABLE_FROM_FILE_CLAUSE:
      code = rewriteCreateTableFromFile(pCxt, pQuery);
      break;
    case QUERY_NODE_DROP_TABLE_STMT:
      code = rewriteDropTable(pCxt, pQuery);
      break;
    case QUERY_NODE_DROP_SUPER_TABLE_STMT:
      code = rewriteDropSuperTable(pCxt, pQuery);
      break;
    case QUERY_NODE_DROP_VIRTUAL_TABLE_STMT:
      code = rewriteDropVirtualTable(pCxt, pQuery);
      break;
    case QUERY_NODE_ALTER_TABLE_STMT:
      code = rewriteAlterTable(pCxt, pQuery, false);
      break;
    case QUERY_NODE_ALTER_VIRTUAL_TABLE_STMT:
      code = rewriteAlterVirtualTable(pCxt, pQuery);
      break;
    case QUERY_NODE_FLUSH_DATABASE_STMT:
      code = rewriteFlushDatabase(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_COMPACTS_STMT:
      code = rewriteShowCompacts(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_SCANS_STMT:
      code = rewriteShowScans(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_COMPACT_DETAILS_STMT:
      code = rewriteShowCompactDetailsStmt(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_RETENTION_DETAILS_STMT:
      code = rewriteShowRetentionDetailsStmt(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_SCAN_DETAILS_STMT:
      code = rewriteShowScanDetailsStmt(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_SSMIGRATES_STMT:
      code = rewriteShowSsMigrates(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_TRANSACTION_DETAILS_STMT:
      code = rewriteShowTransactionDetailsStmt(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_DB_ALIVE_STMT:
    case QUERY_NODE_SHOW_CLUSTER_ALIVE_STMT:
      code = rewriteShowAliveStmt(pCxt, pQuery);
      break;
    default:
      break;
  }
  return code;
}

static int32_t toMsgType(ENodeType type) {
  switch (type) {
    case QUERY_NODE_CREATE_TABLE_STMT:
    case QUERY_NODE_CREATE_VIRTUAL_TABLE_STMT:
    case QUERY_NODE_CREATE_VIRTUAL_SUBTABLE_STMT:
      return TDMT_VND_CREATE_TABLE;
    case QUERY_NODE_ALTER_TABLE_STMT:
    case QUERY_NODE_ALTER_VIRTUAL_TABLE_STMT:
      return TDMT_VND_ALTER_TABLE;
    case QUERY_NODE_DROP_TABLE_STMT:
    case QUERY_NODE_DROP_VIRTUAL_TABLE_STMT:
      return TDMT_VND_DROP_TABLE;
    default:
      break;
  }
  return TDMT_VND_CREATE_TABLE;
}

static int32_t setRefreshMeta(STranslateContext* pCxt, SQuery* pQuery) {
  if (NULL != pCxt->pDbs) {
    taosArrayDestroy(pQuery->pDbList);
    pQuery->pDbList = taosArrayInit(taosHashGetSize(pCxt->pDbs), TSDB_DB_FNAME_LEN);
    if (NULL == pQuery->pDbList) {
      return terrno;
    }
    SFullDatabaseName* pDb = taosHashIterate(pCxt->pDbs, NULL);
    while (NULL != pDb) {
      if (NULL == taosArrayPush(pQuery->pDbList, pDb->fullDbName)) {
        taosHashCancelIterate(pCxt->pDbs, pDb);
        return terrno;
      }
      pDb = taosHashIterate(pCxt->pDbs, pDb);
    }
  }

  if (NULL != pCxt->pTables) {
    taosArrayDestroy(pQuery->pTableList);
    pQuery->pTableList = taosArrayInit(taosHashGetSize(pCxt->pTables), sizeof(SName));
    if (NULL == pQuery->pTableList) {
      return terrno;
    }
    SName* pTable = taosHashIterate(pCxt->pTables, NULL);
    while (NULL != pTable) {
      if (NULL == taosArrayPush(pQuery->pTableList, pTable)) {
        taosHashCancelIterate(pCxt->pTables, pTable);
        return terrno;
      }
      pTable = taosHashIterate(pCxt->pTables, pTable);
    }
  }

  if (NULL != pCxt->pTargetTables) {
    taosArrayDestroy(pQuery->pTargetTableList);
    pQuery->pTargetTableList = taosArrayInit(taosHashGetSize(pCxt->pTargetTables), sizeof(SName));
    if (NULL == pQuery->pTargetTableList) {
      return terrno;
    }
    SName* pTable = taosHashIterate(pCxt->pTargetTables, NULL);
    while (NULL != pTable) {
      if (NULL == taosArrayPush(pQuery->pTargetTableList, pTable)) {
        taosHashCancelIterate(pCxt->pTargetTables, pTable);
        return terrno;
      }
      pTable = taosHashIterate(pCxt->pTargetTables, pTable);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setQuery(STranslateContext* pCxt, SQuery* pQuery) {
  switch (nodeType(pQuery->pRoot)) {
    case QUERY_NODE_SELECT_STMT:
      if (NULL == ((SSelectStmt*)pQuery->pRoot)->pFromTable) {
        pQuery->execMode = QUERY_EXEC_MODE_LOCAL;
        pQuery->haveResultSet = true;
        break;
      }
    case QUERY_NODE_SET_OPERATOR:
    case QUERY_NODE_EXPLAIN_STMT:
      pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
      pQuery->haveResultSet = true;
      pQuery->msgType = TDMT_SCH_QUERY;
      break;
    case QUERY_NODE_DELETE_STMT:
      pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
      pQuery->msgType = TDMT_VND_DELETE;
      break;
    case QUERY_NODE_INSERT_STMT:
      pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
      pQuery->msgType = TDMT_VND_SUBMIT;
      break;
    case QUERY_NODE_VNODE_MODIFY_STMT:
      pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
      pQuery->msgType = toMsgType(((SVnodeModifyOpStmt*)pQuery->pRoot)->sqlNodeType);
      break;
    case QUERY_NODE_DESCRIBE_STMT:
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_VTABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_VIEW_STMT:
    case QUERY_NODE_SHOW_CREATE_RSMA_STMT:
    case QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT:
      pQuery->execMode = QUERY_EXEC_MODE_LOCAL;
      pQuery->haveResultSet = true;
      break;
    case QUERY_NODE_RESET_QUERY_CACHE_STMT:
    case QUERY_NODE_ALTER_LOCAL_STMT:
      pQuery->execMode = QUERY_EXEC_MODE_LOCAL;
      break;
    case QUERY_NODE_SHOW_VARIABLES_STMT:
    case QUERY_NODE_COMPACT_DATABASE_STMT:
    case QUERY_NODE_ROLLUP_DATABASE_STMT:
    case QUERY_NODE_SCAN_DATABASE_STMT:
    case QUERY_NODE_TRIM_DATABASE_STMT:
    case QUERY_NODE_COMPACT_VGROUPS_STMT:
    case QUERY_NODE_ROLLUP_VGROUPS_STMT:
    case QUERY_NODE_SCAN_VGROUPS_STMT:
      pQuery->haveResultSet = true;
      pQuery->execMode = QUERY_EXEC_MODE_RPC;
      if (NULL != pCxt->pCmdMsg) {
        TSWAP(pQuery->pCmdMsg, pCxt->pCmdMsg);
        pQuery->msgType = pQuery->pCmdMsg->msgType;
      }
      break;
    default:
      pQuery->haveResultSet = false;
      pQuery->execMode = QUERY_EXEC_MODE_RPC;
      if (NULL != pCxt->pCmdMsg) {
        TSWAP(pQuery->pCmdMsg, pCxt->pCmdMsg);
        pQuery->msgType = pQuery->pCmdMsg->msgType;
      }
      break;
  }

  pQuery->stableQuery = pCxt->stableQuery;

  if (pQuery->haveResultSet) {
    taosMemoryFreeClear(pQuery->pResSchema);
    taosMemoryFreeClear(pQuery->pResExtSchema);
    if (TSDB_CODE_SUCCESS !=
        extractResultSchema(pQuery->pRoot, &pQuery->numOfResCols, &pQuery->pResSchema, &pQuery->pResExtSchema)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    if (nodeType(pQuery->pRoot) == QUERY_NODE_SELECT_STMT) {
      pQuery->precision = extractResultTsPrecision((SSelectStmt*)pQuery->pRoot);
    } else if (nodeType(pQuery->pRoot) == QUERY_NODE_SET_OPERATOR) {
      pQuery->precision = ((SSetOperator*)pQuery->pRoot)->precision;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t translate(SParseContext* pParseCxt, SQuery* pQuery, SParseMetaCache* pMetaCache) {
  STranslateContext cxt = {0};

  int32_t code = initTranslateContext(pParseCxt, pMetaCache, &cxt);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteQuery(&cxt, pQuery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateQuery(&cxt, pQuery->pRoot);
  }
  if (TSDB_CODE_SUCCESS == code && (cxt.pPrevRoot || cxt.pPostRoot)) {
    pQuery->pPrevRoot = cxt.pPrevRoot;
    pQuery->pPostRoot = cxt.pPostRoot;
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setQuery(&cxt, pQuery);
  }
  int32_t tmpCode = setRefreshMeta(&cxt, pQuery);
  if (TSDB_CODE_SUCCESS == code) code = tmpCode;
  destroyTranslateContext(&cxt);
  return code;
}
