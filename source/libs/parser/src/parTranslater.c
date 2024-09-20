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
#include "parInt.h"
#include "tdatablock.h"

#include "catalog.h"
#include "cmdnodes.h"
#include "filter.h"
#include "functionMgt.h"
#include "parUtil.h"
#include "scalar.h"
#include "systable.h"
#include "tcol.h"
#include "tglobal.h"
#include "ttime.h"

#define generateDealNodeErrMsg(pCxt, code, ...) \
  (pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, code, ##__VA_ARGS__), DEAL_RES_ERROR)

#define SYSTABLE_SHOW_TYPE_OFFSET QUERY_NODE_SHOW_DNODES_STMT

#define CHECK_RES_OUT_OF_MEM(p)       \
  do {                                \
    int32_t code = (p);               \
    if (TSDB_CODE_SUCCESS != code) {  \
      return code;                    \
    }                                 \
  } while (0)

#define CHECK_POINTER_OUT_OF_MEM(p)   \
  do {                                \
    if (NULL == (p)) {                \
      return code;                    \
    }                                 \
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
  const char* pShowCols[2];
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
    .showType = QUERY_NODE_SHOW_BNODES_STMT,
    .pDbName = TSDB_INFORMATION_SCHEMA_DB,
    .pTableName = TSDB_INS_TABLE_BNODES,
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
    .numOfShowCols = 1,
    .pShowCols = {"stream_name"}
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
  {
    .showType = QUERY_NODE_SHOW_ANODES_STMT,
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
};
// clang-format on

static int32_t  translateSubquery(STranslateContext* pCxt, SNode* pNode);
static int32_t  translateQuery(STranslateContext* pCxt, SNode* pNode);
static EDealRes translateValue(STranslateContext* pCxt, SValueNode* pVal);
static EDealRes translateFunction(STranslateContext* pCxt, SFunctionNode** pFunc);
static int32_t  createSimpleSelectStmtFromProjList(const char* pDb, const char* pTable, SNodeList* pProjectionList,
                                                   SSelectStmt** pStmt);
static int32_t  createLastTsSelectStmt(char* pDb, const char* pTable, const char* pkColName, SNode** pQuery);
static int32_t  setQuery(STranslateContext* pCxt, SQuery* pQuery);
static int32_t  setRefreshMeta(STranslateContext* pCxt, SQuery* pQuery);

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
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (hasSameTableAlias(pTables)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_UNIQUE_TABLE_ALIAS,
                                     "Not unique table/alias: '%s'", ((STableNode*)pTable)->tableAlias);
    }
  } else {
    do {
      SArray* pTables = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
      if (NULL == pTables) {
        return TSDB_CODE_OUT_OF_MEMORY;
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
        code = TSDB_CODE_OUT_OF_MEMORY;
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
  char fullName[TSDB_TABLE_FNAME_LEN];
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
    parserError("0x%" PRIx64 " catalogGetViewMeta error, code:%s, dbName:%s, viewName:%s", pParCxt->requestId,
                tstrerror(code), pName->dbname, pName->tname);
  }
  return code;
}
#endif

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
    parserError("0x%" PRIx64 " catalogGetTableMeta error, code:%s, dbName:%s, tbName:%s", pParCxt->requestId,
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
    parserError("0x%" PRIx64 " catalogGetTableMeta error, code:%s, dbName:%s, tbName:%s", pCxt->pParseCxt->requestId,
                tstrerror(code), pName->dbname, pName->tname);
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
    parserError("0x%" PRIx64 " catalogRefreshGetTableCfg error, code:%s, dbName:%s, tbName:%s",
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
    parserError("0x%" PRIx64 " catalogRefreshGetTableMeta error, code:%s, dbName:%s, tbName:%s",
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
    parserError("0x%" PRIx64 " catalogGetDBVgList error, code:%s, dbFName:%s", pCxt->pParseCxt->requestId,
                tstrerror(code), fullDbName);
  }
  return code;
}

static int32_t getDBVgInfo(STranslateContext* pCxt, const char* pDbName, SArray** pVgInfo) {
  SName name;
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
      code = getTableVgroupFromCache(pCxt->pMetaCache, pName, pInfo);
    } else {
      SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                               .requestId = pParCxt->requestId,
                               .requestObjRefId = pParCxt->requestRid,
                               .mgmtEps = pParCxt->mgmtEpSet};
      code = catalogGetTableHashVgroup(pParCxt->pCatalog, &conn, pName, pInfo);
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("0x%" PRIx64 " catalogGetTableHashVgroup error, code:%s, dbName:%s, tbName:%s",
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
    parserError("0x%" PRIx64 " catalogGetDBVgVersion error, code:%s, dbFName:%s", pCxt->pParseCxt->requestId,
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
  int32_t code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pDbName, strlen(pDbName));
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
    parserError("0x%" PRIx64 " catalogGetDBCfg error, code:%s, dbFName:%s", pCxt->pParseCxt->requestId, tstrerror(code),
                dbFname);
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
    parserError("0x%" PRIx64 " catalogGetUdfInfo error, code:%s, funcName:%s", pCxt->pParseCxt->requestId,
                tstrerror(code), pFunc->functionName);
  }
  return code;
}

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
    parserError("0x%" PRIx64 " getTableIndex error, code:%s, dbName:%s, tbName:%s", pCxt->pParseCxt->requestId,
                tstrerror(code), pName->dbname, pName->tname);
  }
  return code;
}

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
    parserError("0x%" PRIx64 " getDnodeList error, code:%s", pCxt->pParseCxt->requestId, tstrerror(code));
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
    parserError("0x%" PRIx64 " get table tsma for : %s.%s error, code:%s", pCxt->pParseCxt->requestId, pName->dbname,
                pName->tname, tstrerror(code));
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
    parserError("0x%" PRIx64 " get tsma for: %s.%s error, code:%s", pCxt->pParseCxt->requestId, pName->dbname,
                pName->tname, tstrerror(code));
  return code;
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
  if (NULL == pCxt->pNsLevel || NULL == pCxt->pDbs || NULL == pCxt->pTables || NULL == pCxt->pTargetTables) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
    return TSDB_CODE_OUT_OF_MEMORY;
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
    return TSDB_CODE_OUT_OF_MEMORY;
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
  if (NULL != pCurrStmt && QUERY_NODE_CREATE_STREAM_STMT == nodeType(pCurrStmt)) {
    return getPrecisionFromCurrStmt(((SCreateStreamStmt*)pCurrStmt)->pQuery, defaultVal);
  }
  if (isDeleteStmt(pCurrStmt)) {
    return ((SDeleteStmt*)pCurrStmt)->precision;
  }
  if (pCurrStmt && nodeType(pCurrStmt) == QUERY_NODE_CREATE_TSMA_STMT)
    return ((SCreateTSMAStmt*)pCurrStmt)->precision;
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

int32_t buildPartitionListFromOrderList(SNodeList* pOrderList, int32_t nodesNum, SNodeList**ppOut) {
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
  if(TSDB_CODE_SUCCESS == code)
    *ppOut = pPartitionList;

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
  if (TSDB_CODE_SUCCESS == code && QUERY_NODE_SET_OPERATOR == nodeType(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
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
               FUNCTION_TYPE_IROWTS == pFunc->funcType) {
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

static void setColumnInfoBySchema(const SRealTableNode* pTable, const SSchema* pColSchema, int32_t tagFlag,
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
  pCol->hasIndex = (pColSchema != NULL && IS_IDX_ON(pColSchema));
  pCol->node.resType.type = pColSchema->type;
  pCol->node.resType.bytes = pColSchema->bytes;
  if (TSDB_DATA_TYPE_TIMESTAMP == pCol->node.resType.type) {
    pCol->node.resType.precision = pTable->pMeta->tableInfo.precision;
  }
  pCol->tableHasPk = hasPkInTable(pTable->pMeta);
  pCol->isPk = (pCol->tableHasPk) && (pColSchema->flags & COL_IS_KEY);
  pCol->numOfPKs = pTable->pMeta->tableInfo.numOfPKs;
}

static int32_t setColumnInfoByExpr(STempTableNode* pTable, SExprNode* pExpr, SColumnNode** pColRef) {
  SColumnNode* pCol = *pColRef;

  if (NULL == pExpr->pAssociation) {
    pExpr->pAssociation = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SAssociationNode));
    if (!pExpr->pAssociation) return TSDB_CODE_OUT_OF_MEMORY;
  }
  SAssociationNode assNode;
  assNode.pPlace = (SNode**)pColRef;
  assNode.pAssociationNode = (SNode*)*pColRef;
  if (NULL == taosArrayPush(pExpr->pAssociation, &assNode)) {
    return terrno;
  }

  strcpy(pCol->tableAlias, pTable->table.tableAlias);
  pCol->isPrimTs = isPrimaryKeyImpl((SNode*)pExpr);
  pCol->colId = pCol->isPrimTs ? PRIMARYKEY_TIMESTAMP_COL_ID : 0;
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    pCol->colType = ((SColumnNode*)pExpr)->colType;
    // strcpy(pCol->dbName, ((SColumnNode*)pExpr)->dbName);
    // strcpy(pCol->tableName, ((SColumnNode*)pExpr)->tableName);
  }
  strcpy(pCol->colName, pExpr->aliasName);
  if ('\0' == pCol->node.aliasName[0]) {
    strcpy(pCol->node.aliasName, pCol->colName);
  }
  if ('\0' == pCol->node.userAlias[0]) {
    strcpy(pCol->node.userAlias, pExpr->userAlias);
  }
  pCol->node.resType = pExpr->resType;
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

static int32_t createColumnsByTable(STranslateContext* pCxt, const STableNode* pTable, bool igTags, SNodeList* pList, bool skipProjRef) {
  int32_t code = 0;
  if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    const STableMeta* pMeta = ((SRealTableNode*)pTable)->pMeta;
    int32_t           nums = pMeta->tableInfo.numOfColumns +
                   (igTags ? 0 : ((TSDB_SUPER_TABLE == pMeta->tableType || ((SRealTableNode*)pTable)->stbRewrite) ? pMeta->tableInfo.numOfTags : 0));
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
      setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema + i, (i - pMeta->tableInfo.numOfColumns), pCol);
      setColumnPrimTs(pCxt, pCol, pTable);
      code = nodesListStrictAppend(pList, (SNode*)pCol);
    }
  } else {
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
        code = setColumnInfoByExpr(pTempTable, (SExprNode*)pNode, (SColumnNode**)&pCell->pNode);
      }
      if (TSDB_CODE_SUCCESS == code) {
        if (!skipProjRef) pCol->projRefIdx = ((SExprNode*)pNode)->projIdx; // only set proj ref when select * from (select ...)
      } else {
        break;
      }
    }
  }
  return code;
}

static bool isInternalPrimaryKey(const SColumnNode* pCol) {
  return PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId &&
         (0 == strcmp(pCol->colName, ROWTS_PSEUDO_COLUMN_NAME) || 0 == strcmp(pCol->colName, C0_PSEUDO_COLUMN_NAME));
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
  if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    const STableMeta* pMeta = ((SRealTableNode*)pTable)->pMeta;
    if (isInternalPrimaryKey(pCol)) {
      if (TSDB_SYSTEM_TABLE == pMeta->tableType) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pCol->colName);
      }

      setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema, -1, pCol);
      pCol->isPrimTs = true;
      *pFound = true;
      return TSDB_CODE_SUCCESS;
    }
    int32_t nums = pMeta->tableInfo.numOfTags + pMeta->tableInfo.numOfColumns;
    for (int32_t i = 0; i < nums; ++i) {
      if (0 == strcmp(pCol->colName, pMeta->schema[i].name) &&
          !invisibleColumn(pCxt->pParseCxt->enableSysInfo, pMeta->tableType, pMeta->schema[i].flags)) {
        setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema + i, (i - pMeta->tableInfo.numOfColumns), pCol);
        setColumnPrimTs(pCxt, pCol, pTable);
        *pFound = true;
        break;
      }
    }
  } else {
    STempTableNode* pTempTable = (STempTableNode*)pTable;
    SNodeList*      pProjectList = getProjectList(pTempTable->pSubquery);
    SNode*          pNode;
    FOREACH(pNode, pProjectList) {
      SExprNode* pExpr = (SExprNode*)pNode;
      if (0 == strcmp(pCol->colName, pExpr->aliasName)) {
        if (*pFound) {
          return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_AMBIGUOUS_COLUMN, pCol->colName);
        }
        code = setColumnInfoByExpr(pTempTable, pExpr, pColRef);
        if (TSDB_CODE_SUCCESS != code) {
          break;
        }
        *pFound = true;
      } else if (isPrimaryKeyImpl(pNode) && isInternalPrimaryKey(pCol)) {
        code = setColumnInfoByExpr(pTempTable, pExpr, pColRef);
        if (TSDB_CODE_SUCCESS != code) break;
        pCol->isPrimTs = true;
        *pFound = true;
      }
    }
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
    if (isInternalPk) {
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
    if (QUERY_NODE_FUNCTION == nodeType(pFoundNode) && (SQL_CLAUSE_GROUP_BY == pCxt->currClause || SQL_CLAUSE_PARTITION_BY == pCxt->currClause)) {
      pCxt->errCode = getFuncInfo(pCxt, (SFunctionNode*)pFoundNode);
      if (TSDB_CODE_SUCCESS == pCxt->errCode) {
        if (fmIsVectorFunc(((SFunctionNode*)pFoundNode)->funcId)) {
          pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION, (*pCol)->colName);
          return DEAL_RES_ERROR;
        } else if (fmIsPseudoColumnFunc(((SFunctionNode*)pFoundNode)->funcId)) {
          if ('\0' != (*pCol)->tableAlias[0]) {
            return translateColumnWithPrefix(pCxt, pCol);
          } else {
            return translateColumnWithoutPrefix(pCxt, pCol);
          }
        } else {
          /* Do nothing and replace old node with found node. */
        }
      } else {
        return DEAL_RES_ERROR;
      }
    }
    SNode* pNew = NULL;
    int32_t code = nodesCloneNode(pFoundNode, &pNew);
    if (NULL == pNew) {
      pCxt->errCode = code;
      return DEAL_RES_ERROR;
    }
    nodesDestroyNode(*(SNode**)pCol);
    *(SNode**)pCol = (SNode*)pNew;
    if (QUERY_NODE_COLUMN == nodeType(pFoundNode)) {
      pCxt->errCode = TSDB_CODE_SUCCESS;
      if ('\0' != (*pCol)->tableAlias[0]) {
        return translateColumnWithPrefix(pCxt, pCol);
      } else {
        return translateColumnWithoutPrefix(pCxt, pCol);
      }
    }
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
    sprintf(p, "%02x", ctx.digest[i]);
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
    strncpy(tbNameFunc->node.aliasName, tbNameFunc->functionName, TSDB_COL_NAME_LEN);
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
          strcpy(multiResFunc->node.aliasName, tbNameFunc->functionName);
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
  int32_t code = nodesMakeList(&pTbnameNodeList);
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
            if (TSDB_CODE_SUCCESS == code)
              code = nodesListStrictAppend(pTbnameNodeList, pTbnameNode);
          }
        }
        if (TSDB_CODE_SUCCESS == code && LIST_LENGTH(pTbnameNodeList) > 0) {
          nodesListInsertListAfterPos(pSelect->pProjectionList, pSelectListCell, pTbnameNodeList);
          pTbnameNodeList = NULL;
        }
      } else if (nodesIsTableStar(pPara)) {
        char*       pTableAlias = ((SColumnNode*)pPara)->tableAlias;
        STableNode* pTable = NULL;
        int32_t     code = findTable(pCxt, pTableAlias, &pTable);
        if (TSDB_CODE_SUCCESS == code && nodeType(pTable) == QUERY_NODE_REAL_TABLE &&
            ((SRealTableNode*)pTable)->pMeta != NULL &&
            ((SRealTableNode*)pTable)->pMeta->tableType == TSDB_SUPER_TABLE) {
          SNode* pTbnameNode = NULL;
          code = biMakeTbnameProjectAstNode(pFunc->functionName, pTableAlias, &pTbnameNode);
          if (TSDB_CODE_SUCCESS == code)
            code = nodesListStrictAppend(pTbnameNodeList, pTbnameNode);
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
          if (TSDB_CODE_SUCCESS == code)
            code = nodesListStrictAppend(pTbnameNodeList, pTbnameNode);
        }
      }
      if (TSDB_CODE_SUCCESS == code && LIST_LENGTH(pTbnameNodeList) > 0) {
        nodesListInsertListAfterPos(pSelect->pProjectionList, cell, pTbnameNodeList);
        pTbnameNodeList = NULL;
      }
    } else if (nodesIsTableStar(pNode)) {
      char*       pTableAlias = ((SColumnNode*)pNode)->tableAlias;
      STableNode* pTable = NULL;
      int32_t     code = findTable(pCxt, pTableAlias, &pTable);
      if (TSDB_CODE_SUCCESS == code && nodeType(pTable) == QUERY_NODE_REAL_TABLE &&
          ((SRealTableNode*)pTable)->pMeta != NULL && ((SRealTableNode*)pTable)->pMeta->tableType == TSDB_SUPER_TABLE) {
        SNode* pTbnameNode = NULL;
        code = biMakeTbnameProjectAstNode(NULL, pTableAlias, &pTbnameNode);
        if (TSDB_CODE_SUCCESS ==code)
          code = nodesListStrictAppend(pTbnameNodeList, pTbnameNode);
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
    strcpy(tbnameFuncNode->node.aliasName, pCol->node.aliasName);
    strcpy(tbnameFuncNode->node.userAlias, pCol->node.userAlias);

    nodesDestroyNode(*ppNode);
    *ppNode = (SNode*)tbnameFuncNode;
    *pRet = true;
    return TSDB_CODE_SUCCESS;
  }

  *pRet = false;
  return TSDB_CODE_SUCCESS;
}

int32_t biCheckCreateTableTbnameCol(STranslateContext* pCxt, SCreateTableStmt* pStmt) {
  if (pStmt->pTags) {
    SNode* pNode = NULL;
    FOREACH(pNode, pStmt->pTags) {
      SColumnDefNode* pTag = (SColumnDefNode*)pNode;
      if (strcasecmp(pTag->colName, "tbname") == 0) {
        int32_t code = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAG_NAME,
                                               "tbname can not used for tags in BI mode");
        return code;
      }
    }
  }
  if (pStmt->pCols) {
    SNode* pNode = NULL;
    FOREACH(pNode, pStmt->pCols) {
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
  return SQL_CLAUSE_GROUP_BY == clause ||
         SQL_CLAUSE_PARTITION_BY == clause ||
         SQL_CLAUSE_ORDER_BY == clause;
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
    bool ret;
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
    if ((pCxt->currClause == SQL_CLAUSE_ORDER_BY) &&
        !(*pCol)->node.asParam) {
      res = translateColumnUseAlias(pCxt, pCol, &found);
    }
    if (DEAL_RES_ERROR != res && !found) {
      if (isSetOperator(pCxt->pCurrStmt)) {
        res = generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_COLUMN, (*pCol)->colName);
      } else {
        res = translateColumnWithoutPrefix(pCxt, pCol);
      }
    }
    if (clauseSupportAlias(pCxt->currClause) &&
        !(*pCol)->node.asParam &&
        res != DEAL_RES_CONTINUE &&
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
    if (TSDB_CODE_SUCCESS == taosParseTime(pVal->literal, &pVal->datum.i, pVal->node.resType.bytes,
                                           pVal->node.resType.precision, tsDaylight)) {
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
  if (parseNatualDuration(pVal->literal, strlen(pVal->literal), &pVal->datum.i, &pVal->unit,
                          pVal->node.resType.precision, false) != TSDB_CODE_SUCCESS) {
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
      if (strict && (((pVal->datum.d == HUGE_VAL || pVal->datum.d == -HUGE_VAL) && errno == ERANGE) ||
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
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
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
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
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
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
      }

      int32_t len = 0;
      if (!taosMbsToUcs4(pVal->literal, strlen(pVal->literal), (TdUcs4*)varDataVal(pVal->datum.p),
                         targetDt.bytes - VARSTR_HEADER_SIZE, &len)) {
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
      isInterpPseudoColumnFunc(pNode)) {
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
    code = findTable(pCxt, ('\0' == pCol->tableAlias[0] ? NULL : pCol->tableAlias), &pTable);
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pTable && QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
      setColumnInfoBySchema((SRealTableNode*)pTable, ((SRealTableNode*)pTable)->pMeta->schema, -1, pCol);
    } else {
      code = rewriteCountStarAsCount1(pCxt, pCount);
    }
  }

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
  SValueNode* pValue = (SValueNode*)nodesListGetNode(pCount->pParameterList, 0);
  STableNode* pTable = NULL;
  int32_t     code = findTable(pCxt, NULL, &pTable);
  if (TSDB_CODE_SUCCESS == code && QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    SColumnNode* pCol = NULL;
    code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
    if (TSDB_CODE_SUCCESS == code) {
      setColumnInfoBySchema((SRealTableNode*)pTable, ((SRealTableNode*)pTable)->pMeta->schema, -1, pCol);
      NODES_DESTORY_LIST(pCount->pParameterList);
      code = nodesListMakeAppend(&pCount->pParameterList, (SNode*)pCol);
    }
  }
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
  if (LIST_LENGTH(pTbname->pParameterList) > 0) {
    pTableAlias = ((SValueNode*)nodesListGetNode(pTbname->pParameterList, 0))->literal;
  }
  STableNode* pTable = NULL;
  int32_t     code = findTable(pCxt, pTableAlias, &pTable);
  if (TSDB_CODE_SUCCESS == code) {
    SColumnNode* pCol = NULL;
    code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
    if (TSDB_CODE_SUCCESS == code) {
      setColumnInfoBySchema((SRealTableNode*)pTable, ((SRealTableNode*)pTable)->pMeta->schema, -1, pCol);
      NODES_DESTORY_LIST(pCount->pParameterList);
      code = nodesListMakeAppend(&pCount->pParameterList, (SNode*)pCol);
    }
  }
  return code;
}

static bool hasInvalidFuncNesting(SNodeList* pParameterList) {
  bool hasInvalidFunc = false;
  nodesWalkExprs(pParameterList, haveVectorFunction, &hasInvalidFunc);
  return hasInvalidFunc;
}

static int32_t getFuncInfo(STranslateContext* pCxt, SFunctionNode* pFunc) {
  // the time precision of the function execution environment
  pFunc->dual = pCxt->dual;
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
  if (hasInvalidFuncNesting(pFunc->pParameterList)) {
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
  if (isCountNotNullValue(pFunc)) {
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
  if (hasInvalidFuncNesting(pFunc->pParameterList)) {
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
  if (hasInvalidFuncNesting(pFunc->pParameterList)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_AGG_FUNC_NESTING);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateInterpFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsInterpFunc(pFunc->funcId)) {
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
  if (hasInvalidFuncNesting(pFunc->pParameterList)) {
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

static int32_t translateTimelineFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsTimelineFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!isSelectStmt(pCxt->pCurrStmt)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s function must be used in select statements", pFunc->functionName);
  }
  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  bool isTimelineAlignedQuery = false;
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

static int32_t translateForbidFillFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsForbidFillFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (hasFillClause(pCxt->pCurrStmt)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_FILL_NOT_ALLOWED_FUNC, pFunc->functionName);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateForbidStreamFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsForbidStreamFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (pCxt->createStream) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_STREAM_NOT_ALLOWED_FUNC, pFunc->functionName);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateForbidSysTableFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsForbidSysTableFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }

  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  SNode*       pTable = pSelect->pFromTable;
  if (NULL != pTable && QUERY_NODE_REAL_TABLE == nodeType(pTable) &&
      TSDB_SYSTEM_TABLE == ((SRealTableNode*)pTable)->pMeta->tableType) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYSTABLE_NOT_ALLOWED_FUNC, pFunc->functionName);
  }
  return TSDB_CODE_SUCCESS;
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
    if (QUERY_NODE_EVENT_WINDOW == nodeType(pSelect->pWindow) ||  QUERY_NODE_COUNT_WINDOW == nodeType(pSelect->pWindow)) {
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

static bool isStarParam(SNode* pNode) { return nodesIsStar(pNode) || nodesIsTableStar(pNode); }

static int32_t translateMultiResFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsMultiResFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (SQL_CLAUSE_SELECT != pCxt->currClause) {
    SNode* pPara = nodesListGetNode(pFunc->pParameterList, 0);
    if (isStarParam(pPara)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                     "%s(*) is only supported in SELECTed list", pFunc->functionName);
    }
  }
  if (tsKeepColumnName && 1 == LIST_LENGTH(pFunc->pParameterList) && !pFunc->node.asAlias && !pFunc->node.asParam) {
    strcpy(pFunc->node.userAlias, ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->userAlias);
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
    pSelect->hasInterpPseudoColFunc =
        pSelect->hasInterpPseudoColFunc ? true : fmIsInterpPseudoColumnFunc(pFunc->funcId);
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
  int32_t code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  strcpy(pVal->node.aliasName, ((SExprNode*)*pNode)->aliasName);
  strcpy(pVal->node.userAlias, ((SExprNode*)*pNode)->userAlias);
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
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  int32_t code = rewriteFuncToValue(pCxt, &pCurrDb, pNode);
  if (TSDB_CODE_SUCCESS != code) taosMemoryFree(pCurrDb);
  return code;
}

static int32_t rewriteClentVersionFunc(STranslateContext* pCxt, SNode** pNode) {
  char* pVer = taosStrdup((void*)version);
  if (NULL == pVer) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = rewriteFuncToValue(pCxt, &pVer, pNode);
  if (TSDB_CODE_SUCCESS != code) taosMemoryFree(pVer);
  return code;
}

static int32_t rewriteServerVersionFunc(STranslateContext* pCxt, SNode** pNode) {
  char* pVer = taosStrdup((void*)pCxt->pParseCxt->svrVer);
  if (NULL == pVer) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
  int32_t code = rewriteFuncToValue(pCxt, &pStatus, pNode);
  if (TSDB_CODE_SUCCESS != code) taosMemoryFree(pStatus);
  return code;
}

static int32_t rewriteUserFunc(STranslateContext* pCxt, SNode** pNode) {
  char    userConn[TSDB_USER_LEN + 1 + TSDB_FQDN_LEN] = {0};  // format 'user@host'
  int32_t len = snprintf(userConn, sizeof(userConn), "%s@", pCxt->pParseCxt->pUser);
  if (TSDB_CODE_SUCCESS != taosGetFqdn(userConn + len)) {
    return terrno;
  }
  char* pUserConn = taosStrdup((void*)userConn);
  if (NULL == pUserConn) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
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
      strcpy(pCol->tableAlias, pVal->literal);
      strcpy(pCol->colName, pFunc->functionName);
      strcpy(pCol->node.aliasName, pCol->colName);
      strcpy(pCol->node.userAlias, pCol->colName);
      nodesDestroyNode(*ppNode);
      *ppNode = (SNode*)pCol;

      return TSDB_CODE_SUCCESS;
    }
  }
  pCol->node.resType = pOldExpr->resType;
  strcpy(pCol->node.aliasName, pOldExpr->aliasName);
  strcpy(pCol->node.userAlias, pOldExpr->userAlias);
  strcpy(pCol->colName, pOldExpr->aliasName);

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
    if (!isSelectStmt(pCxt->pCurrStmt) || NULL == ((SSelectStmt*)pCxt->pCurrStmt)->pFromTable) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TBNAME);
    }
    if (QUERY_NODE_REAL_TABLE != nodeType(((SSelectStmt*)pCxt->pCurrStmt)->pFromTable)) {
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
    code = translateForbidStreamFunc(pCxt, pFunc);
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
    code = translateTimelineFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateDateTimeFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateBlockDistFunc(pCxt, pFunc);
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
  snprintf(pStr, 20, "%" PRId64 "", val);
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
    if ((SQL_CLAUSE_GROUP_BY == pCxt->currClause ||
         SQL_CLAUSE_PARTITION_BY == pCxt->currClause) &&
        fmIsVectorFunc((*pFunc)->funcId)) {
      pCxt->errCode = TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION;
    }
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
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  strcpy(pFunc->functionName, "cast");
  pFunc->node.resType = dt;
  if (TSDB_CODE_SUCCESS != nodesListMakeAppend(&pFunc->pParameterList, pExpr)) {
    nodesDestroyNode((SNode*)pFunc);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  code = getFuncInfo(pCxt, pFunc);
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
  int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOp);
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

static EDealRes translateCaseWhen(STranslateContext* pCxt, SCaseWhenNode* pCaseWhen) {
  bool   first = true;
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
    if (first || dataTypeComp(&pCaseWhen->node.resType, &pThenExpr->resType) < 0) {
      pCaseWhen->node.resType = pThenExpr->resType;
    }
    first = false;
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
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    pCxt->errCode = code;
    return DEAL_RES_ERROR;
  }
  strcpy(pFunc->functionName, "_select_value");
  strcpy(pFunc->node.aliasName, ((SExprNode*)*pNode)->aliasName);
  strcpy(pFunc->node.userAlias, ((SExprNode*)*pNode)->userAlias);
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
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    pCxt->errCode = code;
    return DEAL_RES_ERROR;
  }

  strcpy(pFunc->functionName, "_group_key");
  strcpy(pFunc->node.aliasName, ((SExprNode*)*pNode)->aliasName);
  strcpy(pFunc->node.userAlias, ((SExprNode*)*pNode)->userAlias);
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

  strcpy(pFunc->functionName, "_group_const_value");
  strcpy(pFunc->node.aliasName, ((SExprNode*)*pNode)->aliasName);
  strcpy(pFunc->node.userAlias, ((SExprNode*)*pNode)->userAlias);
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
  if (0 != strcasecmp(pCol->tableAlias, pProbeTable->table.tableAlias)) {
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
  if (NULL != pVal && 0 != strcasecmp(pVal->literal, pProbeTable->table.tableAlias)) {
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

static bool fromSingleTable(SNode* table) {
  if (NULL == table) return false;
  if (table->type == QUERY_NODE_REAL_TABLE && ((SRealTableNode*)table)->pMeta) {
    int8_t type = ((SRealTableNode*)table)->pMeta->tableType;
    if (type == TSDB_CHILD_TABLE || type == TSDB_NORMAL_TABLE || type == TSDB_SYSTEM_TABLE) {
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
  SNode* pGroupNode = NULL;
  FOREACH(pGroupNode, getGroupByList(pCxt)) {
    SNode* pActualNode = getGroupByNode(pGroupNode);
    if (nodesEqualNode(pActualNode, *pNode)) {
      return DEAL_RES_IGNORE_CHILD;
    }
    if (IsEqualTbNameFuncNode(pSelect, pActualNode, *pNode)) {
      return rewriteExprToGroupKeyFunc(pCxt, pNode);
    }
    if (isTbnameFuction(pActualNode) && QUERY_NODE_COLUMN == nodeType(*pNode) &&
        ((SColumnNode*)*pNode)->colType == COLUMN_TYPE_TAG) {
      return rewriteExprToSelectTagFunc(pCxt, pNode);
    }
  }
  SNode* pPartKey = NULL;
  bool   partionByTbname = hasTbnameFunction(pSelect->pPartitionByList);
  FOREACH(pPartKey, pSelect->pPartitionByList) {
    if (nodesEqualNode(pPartKey, *pNode)) {
      return pCxt->currClause == SQL_CLAUSE_HAVING ? DEAL_RES_IGNORE_CHILD : rewriteExprToGroupKeyFunc(pCxt, pNode);
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

  if (isScanPseudoColumnFunc(*pNode) || QUERY_NODE_COLUMN == nodeType(*pNode)) {
    if (pSelect->selectFuncNum > 1 || (isDistinctOrderBy(pCxt) && pCxt->currClause == SQL_CLAUSE_ORDER_BY)) {
      return generateDealNodeErrMsg(pCxt, getGroupByErrorCode(pCxt), ((SExprNode*)(*pNode))->userAlias);
    }
    if (isWindowJoinStmt(pSelect) &&
        (isWindowJoinProbeTableCol(pSelect, *pNode) || isWindowJoinGroupCol(pSelect, *pNode) ||
         (isWindowJoinSubTbname(pSelect, *pNode)) || isWindowJoinSubTbTag(pSelect, *pNode))) {
      return rewriteExprToGroupKeyFunc(pCxt, pNode);
    }

    if (pSelect->hasOtherVectorFunc || !pSelect->hasSelectFunc) {
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
  if (TSDB_CODE_SUCCESS == pCxt->errCode && !pSelect->isDistinct) {
    nodesRewriteExprs(pSelect->pOrderByList, rewriteColsToSelectValFuncImpl, pCxt);
  }
  return pCxt->errCode;
}

typedef struct CheckAggColCoexistCxt {
  STranslateContext* pTranslateCxt;
  bool               existCol;
  SNodeList*         pColList;
} CheckAggColCoexistCxt;

static EDealRes doCheckAggColCoexist(SNode** pNode, void* pContext) {
  CheckAggColCoexistCxt* pCxt = (CheckAggColCoexistCxt*)pContext;
  if (isVectorFunc(*pNode)) {
    return DEAL_RES_IGNORE_CHILD;
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
  if (isScanPseudoColumnFunc(*pNode) || QUERY_NODE_COLUMN == nodeType(*pNode)) {
    pCxt->existCol = true;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes doCheckGetAggColCoexist(SNode** pNode, void* pContext) {
  CheckAggColCoexistCxt* pCxt = (CheckAggColCoexistCxt*)pContext;
  int32_t code = 0;
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
  int32_t code = nodesMakeList(&pNodeList);
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
      (!pSelect->hasAggFuncs && !pSelect->hasIndefiniteRowsFunc && !pSelect->hasInterpFunc)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!pSelect->onlyHasKeepOrderFunc) {
    pSelect->timeLineResMode = TIME_LINE_NONE;
  }
  CheckAggColCoexistCxt cxt = {.pTranslateCxt = pCxt, .existCol = false};
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
  return TSDB_CODE_SUCCESS;
}

static int32_t checkWinJoinAggColCoexist(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (!isWindowJoinStmt(pSelect) ||
      (!pSelect->hasAggFuncs && !pSelect->hasIndefiniteRowsFunc && !pSelect->hasInterpFunc)) {
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
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  SVgroupInfo vg = {.vgId = MNODE_HANDLE};
  memcpy(&vg.epSet, pEpSet, sizeof(SEpSet));
  if (NULL == taosArrayPush(*pVgroupList, &vg)) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
    memcpy(&((*pVgsInfo)->vgroups[i].epSet), taosArrayGet(pDnodes, i), sizeof(SEpSet));
  }
  return TSDB_CODE_SUCCESS;
}

static bool sysTableFromVnode(const char* pTable) {
  return ((0 == strcmp(pTable, TSDB_INS_TABLE_TABLES)) || (0 == strcmp(pTable, TSDB_INS_TABLE_TAGS)) ||
          (0 == strcmp(pTable, TSDB_INS_TABLE_COLS)));
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
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      if (NULL == taosArrayPush(*pVgroupList, &vgInfo)) {
        code = TSDB_CODE_OUT_OF_MEMORY;
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
          (0 == strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_TABLES) && !hasUserDbCond) ||
      0 == strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_COLS)) {
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
    return setSuperTableVgroupList(pCxt, pName, pRealTable);
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
           0 != strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_COLS);
  }
  return (TSDB_CHILD_TABLE == tableType || TSDB_NORMAL_TABLE == tableType);
}

static int32_t setTableIndex(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  if (pCxt->createStream || QUERY_SMA_OPTIMIZE_DISABLE == tsQuerySmaOptimize) {
    return TSDB_CODE_SUCCESS;
  }
  if (0 && isSelectStmt(pCxt->pCurrStmt) && NULL != ((SSelectStmt*)pCxt->pCurrStmt)->pWindow &&
      QUERY_NODE_INTERVAL_WINDOW == nodeType(((SSelectStmt*)pCxt->pCurrStmt)->pWindow)) {
    return getTableIndex(pCxt, pName, &pRealTable->pSmaIndexes);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t setTableTsmas(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  int32_t code = 0;
  if (pCxt->createStream || QUERY_SMA_OPTIMIZE_DISABLE == tsQuerySmaOptimize) {
    return TSDB_CODE_SUCCESS;
  }
  if (isSelectStmt(pCxt->pCurrStmt) && pRealTable->pMeta->tableType != TSDB_SYSTEM_TABLE) {
    code = getTableTsmas(pCxt, pName, &pRealTable->pTsmas);
    // if select from a child table, fetch it's corresponding tsma target child table infos
    if (TSDB_CODE_SUCCESS == code && pRealTable->pTsmas &&
        (pRealTable->pMeta->tableType == TSDB_CHILD_TABLE || pRealTable->pMeta->tableType == TSDB_NORMAL_TABLE)) {
      if (pRealTable->tsmaTargetTbVgInfo) {
        taosArrayDestroyP(pRealTable->tsmaTargetTbVgInfo, taosMemoryFree);
        pRealTable->tsmaTargetTbVgInfo = NULL;
      }
      char buf[TSDB_TABLE_FNAME_LEN + TSDB_TABLE_NAME_LEN + 1];
      for (int32_t i = 0; i < pRealTable->pTsmas->size; ++i) {
        STableTSMAInfo* pTsma = taosArrayGetP(pRealTable->pTsmas, i);
        SName           tsmaTargetTbName = {0};
        SVgroupInfo     vgInfo = {0};
        bool            exists = false;
        toName(pCxt->pParseCxt->acctId, pRealTable->table.dbName, "", &tsmaTargetTbName);
        int32_t len = snprintf(buf, TSDB_TABLE_FNAME_LEN + TSDB_TABLE_NAME_LEN, "%s.%s_%s", pTsma->dbFName, pTsma->name,
                               pRealTable->table.tableName);
        len = taosCreateMD5Hash(buf, len);
        strncpy(tsmaTargetTbName.tname, buf, MD5_OUTPUT_LEN);
        code = collectUseTable(&tsmaTargetTbName, pCxt->pTargetTables);
        if (TSDB_CODE_SUCCESS == code)
          code = catalogGetCachedTableHashVgroup(pCxt->pParseCxt->pCatalog, &tsmaTargetTbName, &vgInfo, &exists);
        if (TSDB_CODE_SUCCESS == code) {
          if (!pRealTable->tsmaTargetTbVgInfo) {
            pRealTable->tsmaTargetTbVgInfo = taosArrayInit(pRealTable->pTsmas->size, POINTER_BYTES);
            if (!pRealTable->tsmaTargetTbVgInfo) {
              code = TSDB_CODE_OUT_OF_MEMORY;
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
            code = TSDB_CODE_OUT_OF_MEMORY;
            break;
          }
        }
        if (code == TSDB_CODE_SUCCESS) {
          sprintf(ctbInfo.tableName, "%s", tsmaTargetTbName.tname);
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
        int32_t code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);
        if (TSDB_CODE_SUCCESS != code) {
          pCxt->errCode = code;
          return DEAL_RES_ERROR;
        }

        int32_t tbLen = strlen(pCxt->pTbName);
        pVal->literal = taosStrdup(pCxt->pTbName);
        if (NULL == pVal->literal) {
          pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
          return DEAL_RES_ERROR;
        }
        pVal->translate = true;
        pVal->node.resType.type = TSDB_DATA_TYPE_BINARY;
        pVal->node.resType.bytes = tbLen + VARSTR_HEADER_SIZE;
        pVal->datum.p = taosMemoryCalloc(1, tbLen + VARSTR_HEADER_SIZE + 1);
        varDataSetLen(pVal->datum.p, tbLen);
        strncpy(varDataVal(pVal->datum.p), pVal->literal, tbLen);
        strcpy(pVal->node.userAlias, pFunc->node.userAlias);
        strcpy(pVal->node.aliasName, pFunc->node.aliasName);

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
  strcpy(pLeft->tableName, leftTable->table.tableName);
  strcpy(pLeft->tableAlias, leftTable->table.tableAlias);
  strcpy(pLeft->colName, pLMeta->schema[0].name);

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
  strcpy(pRight->tableName, rightTable->table.tableName);
  strcpy(pRight->tableAlias, rightTable->table.tableAlias);
  strcpy(pRight->colName, pRMeta->schema[0].name);

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

  if ((QUERY_NODE_TEMP_TABLE == nodeType(pJoinTable->pLeft) &&
       !isGlobalTimeLineQuery(((STempTableNode*)pJoinTable->pLeft)->pSubquery)) ||
      (QUERY_NODE_TEMP_TABLE == nodeType(pJoinTable->pRight) &&
       !isGlobalTimeLineQuery(((STempTableNode*)pJoinTable->pRight)->pSubquery))) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_SUPPORT_JOIN,
                                   "Join requires valid time series input");
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateJoinTable(STranslateContext* pCxt, SJoinTableNode* pJoinTable) {
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

  if (TSDB_CODE_SUCCESS == code && NULL != pJoinTable->pJLimit) {
    if (*pSType != JOIN_STYPE_ASOF && *pSType != JOIN_STYPE_WIN) {
      return buildInvalidOperationMsgExt(&pCxt->msgBuf, "JLIMIT not supported for %s join",
                                         getFullJoinTypeString(type, *pSType));
    }
    SLimitNode* pJLimit = (SLimitNode*)pJoinTable->pJLimit;
    if (pJLimit->limit > JOIN_JLIMIT_MAX_VALUE || pJLimit->limit < 0) {
      return buildInvalidOperationMsg(&pCxt->msgBuf, "JLIMIT value is out of valid range [0, 1024]");
    }
    if (0 == pJLimit->limit) {
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

int32_t translateTable(STranslateContext* pCxt, SNode** pTable, SNode* pJoinParent) {
  SSelectStmt* pCurrSmt = (SSelectStmt*)(pCxt->pCurrStmt);
  int32_t      code = TSDB_CODE_SUCCESS;
  switch (nodeType(*pTable)) {
    case QUERY_NODE_REAL_TABLE: {
      SRealTableNode* pRealTable = (SRealTableNode*)*pTable;
      pRealTable->ratio = (NULL != pCxt->pExplainOpt ? pCxt->pExplainOpt->ratio : 1.0);
      // The SRealTableNode created through ROLLUP already has STableMeta.
      if (NULL == pRealTable->pMeta) {
        SName name = {0};
        toName(pCxt->pParseCxt->acctId, pRealTable->table.dbName, pRealTable->table.tableName, &name);
        code = getTargetMeta(pCxt, &name, &(pRealTable->pMeta), true);
        if (TSDB_CODE_SUCCESS != code) {
          (void)generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GET_META_ERROR, tstrerror(code));
          return code;
        }
#ifdef TD_ENTERPRISE
        if (TSDB_VIEW_TABLE == pRealTable->pMeta->tableType && (!pCurrSmt->tagScan || pCxt->pParseCxt->biMode)) {
          return translateView(pCxt, pTable, &name);
        }
        code = translateAudit(pCxt, pRealTable, &name);
#endif
        if (TSDB_CODE_SUCCESS == code)
          code = setTableVgroupList(pCxt, &name, pRealTable);
        if (TSDB_CODE_SUCCESS == code) {
          code = setTableIndex(pCxt, &name, pRealTable);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = setTableTsmas(pCxt, &name, pRealTable);
        }
      }
      if (TSDB_CODE_SUCCESS == code) {
        pRealTable->table.precision = pRealTable->pMeta->tableInfo.precision;
        pRealTable->table.singleTable = isSingleTable(pRealTable);
        if (TSDB_SUPER_TABLE == pRealTable->pMeta->tableType) {
          pCxt->stableQuery = true;
        }
        if (TSDB_SYSTEM_TABLE == pRealTable->pMeta->tableType) {
          if (isSelectStmt(pCxt->pCurrStmt)) {
            ((SSelectStmt*)pCxt->pCurrStmt)->timeLineResMode = TIME_LINE_NONE;
            ((SSelectStmt*)pCxt->pCurrStmt)->timeLineCurMode = TIME_LINE_NONE;
          } else if (isDeleteStmt(pCxt->pCurrStmt)) {
            code = TSDB_CODE_TSC_INVALID_OPERATION;
            break;
          }
        }
        code = addNamespace(pCxt, pRealTable);
      }
      break;
    }
    case QUERY_NODE_TEMP_TABLE: {
      STempTableNode* pTempTable = (STempTableNode*)*pTable;
      code = translateSubquery(pCxt, pTempTable->pSubquery);
      if (TSDB_CODE_SUCCESS == code) {
        if (QUERY_NODE_SELECT_STMT == nodeType(pTempTable->pSubquery) &&
            ((SSelectStmt*)pTempTable->pSubquery)->isEmptyResult && isSelectStmt(pCxt->pCurrStmt)) {
          ((SSelectStmt*)pCxt->pCurrStmt)->isEmptyResult = true;
        }
        if (QUERY_NODE_SELECT_STMT == nodeType(pTempTable->pSubquery) && isSelectStmt(pCxt->pCurrStmt)) {
          pCurrSmt->joinContains = ((SSelectStmt*)pTempTable->pSubquery)->joinContains;
          SSelectStmt* pSubStmt = (SSelectStmt*)pTempTable->pSubquery;
          pCurrSmt->timeLineResMode = pSubStmt->timeLineResMode;
          pCurrSmt->timeLineCurMode = pSubStmt->timeLineResMode;
        }

        pCurrSmt->joinContains = (getJoinContais(pTempTable->pSubquery) ? true : false);
        pTempTable->table.precision = getStmtPrecision(pTempTable->pSubquery);
        pTempTable->table.singleTable = stmtIsSingleTable(pTempTable->pSubquery);
        code = addNamespace(pCxt, pTempTable);
      }
      break;
    }
    case QUERY_NODE_JOIN_TABLE: {
      SJoinTableNode* pJoinTable = (SJoinTableNode*)*pTable;
      code = translateJoinTable(pCxt, pJoinTable);
      if (TSDB_CODE_SUCCESS == code) {
        code = translateTable(pCxt, &pJoinTable->pLeft, (SNode*)pJoinTable);
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = translateTable(pCxt, &pJoinTable->pRight, (SNode*)pJoinTable);
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = checkJoinTable(pCxt, pJoinTable);
      }
      if (TSDB_CODE_SUCCESS == code) {
        pJoinTable->table.precision = calcJoinTablePrecision(pJoinTable);
        pJoinTable->table.singleTable = joinTableIsSingleTable(pJoinTable);
        code = translateExpr(pCxt, &pJoinTable->pOnCond);
      }
      if (TSDB_CODE_SUCCESS == code) {
        pJoinTable->hasSubQuery = (nodeType(pJoinTable->pLeft) != QUERY_NODE_REAL_TABLE) ||
                                  (nodeType(pJoinTable->pRight) != QUERY_NODE_REAL_TABLE);
        if (nodeType(pJoinTable->pLeft) == QUERY_NODE_JOIN_TABLE) {
          ((SJoinTableNode*)pJoinTable->pLeft)->isLowLevelJoin = true;
        }
        if (nodeType(pJoinTable->pRight) == QUERY_NODE_JOIN_TABLE) {
          ((SJoinTableNode*)pJoinTable->pRight)->isLowLevelJoin = true;
        }
        code = validateJoinConds(pCxt, pJoinTable);
      }
      pCurrSmt->joinContains = true;
      break;
    }
    default:
      break;
  }
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
    int32_t     code = createColumnsByTable(pCxt, pTable, igTags, *pCols, nums > 1);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t createMultiResFunc(SFunctionNode* pSrcFunc, SExprNode* pExpr, SNode** ppNodeOut) {
  SFunctionNode* pFunc = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
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
  strcpy(pFunc->functionName, pSrcFunc->functionName);
  char    buf[TSDB_FUNC_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN + TSDB_NAME_DELIMITER_LEN + 3] = {0};
  int32_t len = 0;
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    SColumnNode* pCol = (SColumnNode*)pExpr;
    if (tsKeepColumnName) {
      strcpy(pFunc->node.userAlias, pCol->colName);
      strcpy(pFunc->node.aliasName, pCol->colName);
    } else {
      len = snprintf(buf, sizeof(buf) - 1, "%s(%s.%s)", pSrcFunc->functionName, pCol->tableAlias, pCol->colName);
      (void)taosHashBinary(buf, len);
      strncpy(pFunc->node.aliasName, buf, TSDB_COL_NAME_LEN - 1);
      len = snprintf(buf, sizeof(buf) - 1, "%s(%s)", pSrcFunc->functionName, pCol->colName);
      // note: userAlias could be truncated here
      strncpy(pFunc->node.userAlias, buf, TSDB_COL_NAME_LEN - 1);
    }
  } else {
    len = snprintf(buf, sizeof(buf) - 1, "%s(%s)", pSrcFunc->functionName, pExpr->aliasName);
    (void)taosHashBinary(buf, len);
    strncpy(pFunc->node.aliasName, buf, TSDB_COL_NAME_LEN - 1);
    len = snprintf(buf, sizeof(buf) - 1, "%s(%s)", pSrcFunc->functionName, pExpr->userAlias);
    // note: userAlias could be truncated here
    strncpy(pFunc->node.userAlias, buf, TSDB_COL_NAME_LEN - 1);
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
  int32_t code = nodesMakeList(&pFuncs);
  if (NULL == pFuncs) {
    return code;
  }

  SNode*  pExpr = NULL;
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
    int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
    if (TSDB_CODE_SUCCESS != code) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_OUT_OF_MEMORY);
    }
    setColumnInfoBySchema(pTable, pTagsSchema + i, 1, pCol);
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

static int32_t translateOrderBy(STranslateContext* pCxt, SSelectStmt* pSelect) {
  bool    other;
  int32_t code = translateClausePosition(pCxt, pSelect->pProjectionList, pSelect->pOrderByList, &other);
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
  if ((isAggFunc(pNode) || isInterpFunc(pNode)) && FUNCTION_TYPE_GROUP_KEY != ((SFunctionNode*)pNode)->funcType
  && FUNCTION_TYPE_GROUP_CONST_VALUE != ((SFunctionNode*)pNode)->funcType) {
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

static int32_t checkFillValues(STranslateContext* pCxt, SFillNode* pFill, SNodeList* pProjectionList) {
  if (FILL_MODE_VALUE != pFill->mode && FILL_MODE_VALUE_F != pFill->mode) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t        fillNo = 0;
  SNodeListNode* pFillValues = (SNodeListNode*)pFill->pValues;
  SNode*         pProject = NULL;
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
      strcpy(pExpr->userAlias, pExpr->aliasName);
    }
    sprintf(pExpr->aliasName, "#expr_%d", no++);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkProjectAlias(STranslateContext* pCxt, SNodeList* pProjectionList, SHashObj** pOutput) {
  SHashObj* pUserAliasSet = taosHashInit(LIST_LENGTH(pProjectionList),
                                         taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (!pUserAliasSet) return TSDB_CODE_OUT_OF_MEMORY;
  SNode*    pProject = NULL;
  int32_t   code = TSDB_CODE_SUCCESS;
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
  if (!pSelect->isSubquery) {
    return rewriteProjectAlias(pSelect->pProjectionList);
  } else {
    SNode* pNode;
    int32_t projIdx = 1;
    FOREACH(pNode, pSelect->pProjectionList) {
      ((SExprNode*)pNode)->projIdx = projIdx++;
    }
    return TSDB_CODE_SUCCESS;
  }
}

typedef struct SReplaceGroupByAliasCxt {
  STranslateContext* pTranslateCxt;
  SNodeList*         pProjectionList;
} SReplaceGroupByAliasCxt;

static EDealRes replaceGroupByAliasImpl(SNode** pNode, void* pContext) {
  SReplaceGroupByAliasCxt* pCxt = pContext;
  SNodeList*               pProjectionList = pCxt->pProjectionList;
  SNode*                   pProject = NULL;
  if (QUERY_NODE_VALUE == nodeType(*pNode)) {
    STranslateContext* pTransCxt = pCxt->pTranslateCxt;
    SValueNode* pVal = (SValueNode*) *pNode;
    if (DEAL_RES_ERROR == translateValue(pTransCxt, pVal)) {
      return DEAL_RES_CONTINUE;
    }
    if (!pVal->node.asPosition) {
      return DEAL_RES_CONTINUE;
    }
    int32_t pos = getPositionValue(pVal);
    if (0 < pos && pos <= LIST_LENGTH(pProjectionList)) {
      SNode* pNew = NULL;
      int32_t code = nodesCloneNode(nodesListGetNode(pProjectionList, pos - 1), (SNode**)&pNew);
      if (TSDB_CODE_SUCCESS != code) {
        pCxt->pTranslateCxt->errCode = code;
        return DEAL_RES_ERROR;
      }
      nodesDestroyNode(*pNode);
      *pNode = pNew;
      return DEAL_RES_CONTINUE;
    } else {
      return DEAL_RES_CONTINUE;
    }
  } else if (QUERY_NODE_COLUMN == nodeType(*pNode)) {
    STranslateContext* pTransCxt = pCxt->pTranslateCxt;
    return translateColumn(pTransCxt, (SColumnNode**)pNode);
  }

  return DEAL_RES_CONTINUE;
}

static int32_t replaceGroupByAlias(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pGroupByList) {
    return TSDB_CODE_SUCCESS;
  }
  SReplaceGroupByAliasCxt cxt = {
      .pTranslateCxt = pCxt, .pProjectionList = pSelect->pProjectionList};
  nodesRewriteExprsPostOrder(pSelect->pGroupByList, replaceGroupByAliasImpl, &cxt);

  return pCxt->errCode;
}

static int32_t replacePartitionByAlias(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pPartitionByList) {
    return TSDB_CODE_SUCCESS;
  }
  SReplaceGroupByAliasCxt cxt = {
      .pTranslateCxt = pCxt, .pProjectionList = pSelect->pProjectionList};
  nodesRewriteExprsPostOrder(pSelect->pPartitionByList, replaceGroupByAliasImpl, &cxt);

  return pCxt->errCode;
}

static int32_t translateSelectList(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->currClause = SQL_CLAUSE_SELECT;
  int32_t code = translateExprList(pCxt, pSelect->pProjectionList);
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
  if (NULL == pSelect->pGroupByList && NULL == pSelect->pPartitionByList && NULL == pSelect->pWindow &&
      !isWindowJoinStmt(pSelect) && NULL != pSelect->pHaving) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);
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
  pCxt->currClause = SQL_CLAUSE_HAVING;
  int32_t code = translateExpr(pCxt, &pSelect->pHaving);
  return code;
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
    code = replaceGroupByAlias(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pSelect->timeLineResMode = TIME_LINE_NONE;
    code = translateExprList(pCxt, pSelect->pGroupByList);
  }
  return code;
}

static int32_t getTimeRange(SNode** pPrimaryKeyCond, STimeWindow* pTimeRange, bool* pIsStrict) {
  SNode*  pNew = NULL;
  int32_t code = scalarCalculateConstants(*pPrimaryKeyCond, &pNew);
  if (TSDB_CODE_SUCCESS == code) {
    *pPrimaryKeyCond = pNew;
    code = filterGetTimeRange(*pPrimaryKeyCond, pTimeRange, pIsStrict);
  }
  return code;
}

static int32_t getQueryTimeRange(STranslateContext* pCxt, SNode* pWhere, STimeWindow* pTimeRange) {
  if (NULL == pWhere) {
    *pTimeRange = TSWINDOW_INITIALIZER;
    return TSDB_CODE_SUCCESS;
  }

  SNode* pCond = NULL;
  int32_t code = nodesCloneNode(pWhere, &pCond);
  if (NULL == pCond) {
    return code;
  }

  SNode* pPrimaryKeyCond = NULL;
  code = filterPartitionCond(&pCond, &pPrimaryKeyCond, NULL, NULL, NULL);

  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pPrimaryKeyCond) {
      bool isStrict = false;
      code = getTimeRange(&pPrimaryKeyCond, pTimeRange, &isStrict);
    } else {
      *pTimeRange = TSWINDOW_INITIALIZER;
    }
  }
  nodesDestroyNode(pCond);
  nodesDestroyNode(pPrimaryKeyCond);
  return code;
}

static int32_t checkFill(STranslateContext* pCxt, SFillNode* pFill, SValueNode* pInterval, bool isInterpFill) {
  if (FILL_MODE_NONE == pFill->mode) {
    if (isInterpFill) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, "Unsupported fill type");
    }

    return TSDB_CODE_SUCCESS;
  }

  if (!pCxt->createStream && TSWINDOW_IS_EQUAL(pFill->timeRange, TSWINDOW_INITIALIZER)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE);
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
  if (!pCxt->createStream) {
    int64_t res = int64SafeSub(pFill->timeRange.skey, pFill->timeRange.ekey);
    timeRange = res < 0 ? res == INT64_MIN ? INT64_MAX : -res : res;
    if (IS_CALENDAR_TIME_DURATION(pInterval->unit)) {
      int64_t f = 1;
      if (pInterval->unit == 'n') {
        f = 30LL * MILLISECOND_PER_DAY;
      } else if (pInterval->unit == 'y') {
        f = 365LL * MILLISECOND_PER_DAY;
      }
      intervalRange = pInterval->datum.i * f;
    } else {
      intervalRange = pInterval->datum.i;
    }
    if ((timeRange / intervalRange) >= MAX_INTERVAL_TIME_WINDOW) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateFill(STranslateContext* pCxt, SSelectStmt* pSelect, SIntervalWindowNode* pInterval) {
  if (NULL == pInterval->pFill) {
    return TSDB_CODE_SUCCESS;
  }

  ((SFillNode*)pInterval->pFill)->timeRange = pSelect->timeRange;
  return checkFill(pCxt, (SFillNode*)pInterval->pFill, (SValueNode*)pInterval->pInterval, false);
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
    if (pOffset->datum.i <= 0) {
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
      double offsetMonth = 0, intervalMonth = 0;
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
      double slidingMonth = 0, intervalMonth = 0;
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

static int32_t translateIntervalWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  SIntervalWindowNode* pInterval = (SIntervalWindowNode*)pSelect->pWindow;
  int32_t              code = checkIntervalWindow(pCxt, pInterval);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateFill(pCxt, pSelect, pInterval);
  }
  return code;
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

static int32_t checkStateWindowForStream(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (!pCxt->createStream) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_SUPER_TABLE == ((SRealTableNode*)pSelect->pFromTable)->pMeta->tableType &&
      !hasTbnameFunction(pSelect->pPartitionByList)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY, "Unsupported stream query");
  }
  if ((SRealTableNode*)pSelect->pFromTable && hasPkInTable(((SRealTableNode*)pSelect->pFromTable)->pMeta)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                   "Source table of State window must not has primary key");
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateStateWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
      !isGlobalTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TIMELINE_QUERY,
                                   "STATE_WINDOW requires valid time series input");
  }

  SStateWindowNode* pState = (SStateWindowNode*)pSelect->pWindow;
  int32_t           code = checkStateExpr(pCxt, pState->pExpr);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkStateWindowForStream(pCxt, pSelect);
  }
  return code;
}

static int32_t translateSessionWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
      !isGlobalTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TIMELINE_QUERY,
                                   "SESSION requires valid time series input");
  }

  SSessionWindowNode* pSession = (SSessionWindowNode*)pSelect->pWindow;
  if ('y' == pSession->pGap->unit || 'n' == pSession->pGap->unit || 0 == pSession->pGap->datum.i) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SESSION_GAP);
  }
  if (!isPrimaryKeyImpl((SNode*)pSession->pCol)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SESSION_COL);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateEventWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
      !isGlobalTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TIMELINE_QUERY,
                                   "EVENT_WINDOW requires valid time series input");
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateCountWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  SCountWindowNode* pCountWin = (SCountWindowNode*)pSelect->pWindow;
  if (pCountWin->windowCount <= 1) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                   "Size of Count window must exceed 1.");
  }

  if (pCountWin->windowSliding <= 0) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                   "Size of Count window must exceed 0.");
  }

  if (pCountWin->windowSliding > pCountWin->windowCount) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                   "sliding value no larger than the count value.");
  }

  if (pCountWin->windowCount > INT32_MAX) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                   "Size of Count window must less than 2147483647(INT32_MAX).");
  }
  if (QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
      !isGlobalTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TIMELINE_QUERY,
                                   "COUNT_WINDOW requires valid time series input");
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkAnomalyExpr(STranslateContext* pCxt, SNode* pNode) {
  int32_t type = ((SExprNode*)pNode)->resType.type;
  if (!IS_MATHABLE_TYPE(type)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ANOMALY_WIN_TYPE);
  }

  if (QUERY_NODE_COLUMN == nodeType(pNode) && COLUMN_TYPE_TAG == ((SColumnNode*)pNode)->colType) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ANOMALY_WIN_COL);
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
    char *pos1 = strstr(pAnomaly->anomalyOpt, "func=");
    if (pos1 == NULL) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ANOMALY_WIN_OPT);
    }
  }

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
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
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

static int32_t checkWindowsConditonValid(SNode* pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (QUERY_NODE_EVENT_WINDOW != nodeType(pNode)) return code;

  SEventWindowNode* pEventWindowNode = (SEventWindowNode*)pNode;
  SNodeList*        pCols = NULL;
  code = nodesMakeList(&pCols);
  if (NULL == pCols) {
    return code;
  }
  SCollectWindowsPseudocolumnsContext ctx = {.code = 0, .pCols = pCols};
  nodesWalkExpr(pEventWindowNode->pStartCond, collectWindowsPseudocolumns, &ctx);
  if (TSDB_CODE_SUCCESS == ctx.code && pCols->length > 0) {
    code = TSDB_CODE_QRY_INVALID_WINDOW_CONDITION;
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesWalkExpr(pEventWindowNode->pEndCond, collectWindowsPseudocolumns, &ctx);
    if (TSDB_CODE_SUCCESS == ctx.code && pCols->length > 0) {
      code = TSDB_CODE_QRY_INVALID_WINDOW_CONDITION;
    }
  }

  nodesDestroyList(pCols);
  return code;
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
  if (TSDB_CODE_SUCCESS == code) {
    code = checkWindowsConditonValid(pSelect->pWindow);
  }
  return code;
}

static int32_t createDefaultFillNode(STranslateContext* pCxt, SNode** pOutput) {
  SFillNode* pFill = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_FILL, (SNode**)&pFill);
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
  strcpy(pCol->colName, ROWTS_PSEUDO_COLUMN_NAME);
  pFill->pWStartTs = (SNode*)pCol;

  *pOutput = (SNode*)pFill;
  return TSDB_CODE_SUCCESS;
}

static int32_t createDefaultEveryNode(STranslateContext* pCxt, SNode** pOutput) {
  SValueNode* pEvery = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pEvery);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  pEvery->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pEvery->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  pEvery->flag |= VALUE_FLAG_IS_DURATION;
  pEvery->literal = taosStrdup("1s");

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

static int32_t translateInterpFill(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pSelect->pFill) {
    code = createDefaultFillNode(pCxt, &pSelect->pFill);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExpr(pCxt, &pSelect->pFill);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = getQueryTimeRange(pCxt, pSelect->pRange, &(((SFillNode*)pSelect->pFill)->timeRange));
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkFill(pCxt, (SFillNode*)pSelect->pFill, (SValueNode*)pSelect->pEvery, true);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkFillValues(pCxt, (SFillNode*)pSelect->pFill, pSelect->pProjectionList);
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

  if (NULL == pSelect->pRange || NULL == pSelect->pEvery || NULL == pSelect->pFill) {
    if (pSelect->pRange != NULL && QUERY_NODE_OPERATOR == nodeType(pSelect->pRange) && pSelect->pEvery == NULL) {
      // single point interp every can be omitted
    } else {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_INTERP_CLAUSE,
                                     "Missing RANGE clause, EVERY clause or FILL clause");
    }
  }

  int32_t code = translateExpr(pCxt, &pSelect->pRange);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateInterpEvery(pCxt, &pSelect->pEvery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateInterpFill(pCxt, pSelect);
  }
  return code;
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
    code = replacePartitionByAlias(pCxt, pSelect);
    if (TSDB_CODE_SUCCESS == code) {
      code = translateExprList(pCxt, pSelect->pPartitionByList);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExprList(pCxt, pSelect->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExpr(pCxt, &pSelect->pSubtable);
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
  if (!*ppTbNames) return TSDB_CODE_OUT_OF_MEMORY;
  SNodeList*     pValueNodeList = pValueListNode->pNodeList;
  SNode*         pValNode = NULL;
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

static int32_t findEqCondTbNameInOperatorNode(STranslateContext* pCxt, SNode* pWhere, SEqCondTbNameTableInfo* pInfo, bool* pRet) {
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
            code = TSDB_CODE_OUT_OF_MEMORY;
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
  bool bFoundTable = false;
  for (int i = 0; i < taosArrayGetSize(aTableTbnames); ++i) {
    SEqCondTbNameTableInfo* info = taosArrayGet(aTableTbnames, i);
    if (info->pRealTable == pInfo->pRealTable) {
      if (NULL == taosArrayAddAll(info->aTbnames, pInfo->aTbnames)) {
        code = TSDB_CODE_OUT_OF_MEMORY;
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
      code = TSDB_CODE_OUT_OF_MEMORY;
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
        return TSDB_CODE_OUT_OF_MEMORY;
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
  SName snameTb = {0};
  int32_t code = 0;
  SRealTableNode* pRealTable = pInfo->pRealTable;
  char* tbName = taosArrayGetP(pInfo->aTbnames, 0);
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
      int32_t len = snprintf(buf, TSDB_TABLE_FNAME_LEN + TSDB_TABLE_NAME_LEN, "%s.%s_%s", pTsma->dbFName, pTsma->name,
                             pRealTable->table.tableName);
      len = taosCreateMD5Hash(buf, len);
      strncpy(tsmaTargetTbName.tname, buf, MD5_OUTPUT_LEN);
      STsmaTargetTbInfo ctbInfo = {0};
      if (!pRealTable->tsmaTargetTbInfo) {
        pRealTable->tsmaTargetTbInfo = taosArrayInit(pRealTable->pTsmas->size, sizeof(STsmaTargetTbInfo));
        if (!pRealTable->tsmaTargetTbInfo) {
          code = terrno;
          break;
        }
      }
      sprintf(ctbInfo.tableName, "%s", tsmaTargetTbName.tname);
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
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t aTableNum = taosArrayGetSize(aTables);
  int32_t nTbls = 0;
  bool    stableQuery = false;
  SEqCondTbNameTableInfo* pInfo = NULL;

  qDebug("start to update stable vg for tbname optimize, aTableNum:%d", aTableNum);
  for (int i = 0; i < aTableNum; ++i) {
    pInfo = taosArrayGet(aTables, i);
    int32_t                 numOfVgs = pInfo->pRealTable->pVgroupList->numOfVgroups;
    nTbls = taosArrayGetSize(pInfo->aTbnames);

    SVgroupsInfo* vgsInfo = taosMemoryMalloc(sizeof(SVgroupsInfo) + nTbls * sizeof(SVgroupInfo));
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
      if (!pInfo->pRealTable->tsmaTargetTbVgInfo) return TSDB_CODE_OUT_OF_MEMORY;

      for (int32_t i = 0; i < pInfo->pRealTable->pTsmas->size; ++i) {
        STableTSMAInfo* pTsma = taosArrayGetP(pInfo->pRealTable->pTsmas, i);
        SArray*         pTbNames = taosArrayInit(pInfo->aTbnames->size, POINTER_BYTES);
        if (!pTbNames) return TSDB_CODE_OUT_OF_MEMORY;

        for (int32_t k = 0; k < pInfo->aTbnames->size; ++k) {
          const char* pTbName = taosArrayGetP(pInfo->aTbnames, k);
          char*       pNewTbName = taosMemoryCalloc(1, TSDB_TABLE_FNAME_LEN + TSDB_TABLE_NAME_LEN + 1);
          if (!pNewTbName) {
            code = terrno;
            break;
          }
          if (NULL == taosArrayPush(pTbNames, &pNewTbName)) {
            code = terrno;
            break;
          }
          sprintf(pNewTbName, "%s.%s_%s", pTsma->dbFName, pTsma->name, pTbName);
          int32_t len = taosCreateMD5Hash(pNewTbName, strlen(pNewTbName));
        }
        if (TSDB_CODE_SUCCESS == code) {
          vgsInfo = taosMemoryMalloc(sizeof(SVgroupsInfo) + nTbls * sizeof(SVgroupInfo));
          if (!vgsInfo) code = TSDB_CODE_OUT_OF_MEMORY;
        }
        if (TSDB_CODE_SUCCESS == code) {
          findVgroupsFromEqualTbname(pCxt, pTbNames, pInfo->pRealTable->table.dbName, numOfVgs, vgsInfo);
          if (vgsInfo->numOfVgroups != 0) {
            if (NULL == taosArrayPush(pInfo->pRealTable->tsmaTargetTbVgInfo, &vgsInfo)) {
              code = terrno;
            }
          } else {
            taosMemoryFree(vgsInfo);
          }
        }
        taosArrayDestroyP(pTbNames, taosMemoryFree);
        if (code) break;
      }
    }
  }

  qDebug("before ctbname optimize, code:%d, aTableNum:%d, nTbls:%d, stableQuery:%d", code, aTableNum, nTbls, stableQuery);

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
  int32_t code = translateExpr(pCxt, &pSelect->pWhere);
  if (TSDB_CODE_SUCCESS == code) {
    code = getQueryTimeRange(pCxt, pSelect->pWhere, &pSelect->timeRange);
  }
  if (pSelect->pWhere != NULL && pCxt->pParseCxt->topicQuery == false) {
    code = setTableVgroupsFromEqualTbnameCond(pCxt, pSelect);
  }
  return code;
}

static int32_t translateFrom(STranslateContext* pCxt, SNode** pTable) {
  pCxt->currClause = SQL_CLAUSE_FROM;
  return translateTable(pCxt, pTable, NULL);
}

static int32_t checkLimit(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if ((NULL != pSelect->pLimit && pSelect->pLimit->offset < 0) ||
      (NULL != pSelect->pSlimit && pSelect->pSlimit->offset < 0)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OFFSET_LESS_ZERO);
  }

  if (NULL != pSelect->pSlimit && (NULL == pSelect->pPartitionByList && NULL == pSelect->pGroupByList)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SLIMIT_LEAK_PARTITION_GROUP_BY);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t createPrimaryKeyColByTable(STranslateContext* pCxt, STableNode* pTable, SNode** pPrimaryKey) {
  SColumnNode* pCol = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  strcpy(pCol->colName, ROWTS_PSEUDO_COLUMN_NAME);
  bool    found = false;
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

  if (TSDB_CODE_SUCCESS != tSimpleHashPut(*(SSHashObj**)pContext, pCol->tableAlias, strlen(pCol->tableAlias), pCol->tableAlias,
                 sizeof(pCol->tableAlias))) {
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
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  pCol->colId = pTable->pMeta->schema[1].colId;
  strcpy(pCol->colName, pTable->pMeta->schema[1].name);
  bool    found = false;
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
        SNode* pNew = NULL;
        int32_t code = nodesCloneNode(pProject, &pNew);
        if (NULL == pNew) {
          pCxt->pTranslateCxt->errCode = code;
          return DEAL_RES_ERROR;
        }
        ((SExprNode*)pNew)->orderAlias = true;
        nodesDestroyNode(*pNode);
        *pNode = pNew;
        return DEAL_RES_CONTINUE;
      }
    }

    pCxt->notFound = true;
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
        SNode* pNew = NULL;
        int32_t code = nodesCloneNode(nodesListGetNode(pProjectionList, pos - 1), &pNew);
        if (NULL == pNew) {
          pCxt->pTranslateCxt->errCode = code;
          return DEAL_RES_ERROR;
        }
        ((SExprNode*)pNew)->orderAlias = true;
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

static void resetResultTimeline(SSelectStmt* pSelect) {
  if (NULL == pSelect->pOrderByList) {
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
    resetResultTimeline(pSelect);
  }
  return code;
}

static int32_t translateSelectWithoutFrom(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->pCurrStmt = (SNode*)pSelect;
  pCxt->currClause = SQL_CLAUSE_SELECT;
  pCxt->dual = true;
  return translateExprList(pCxt, pSelect->pProjectionList);
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
    if (TSDB_CODE_SUCCESS == code)
      code = checkAggColCoexist(pCxt, pSelect);
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
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
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

static int32_t translateSetOperProject(STranslateContext* pCxt, SSetOperator* pSetOperator) {
  SNodeList* pLeftProjections = getProjectList(pSetOperator->pLeft);
  SNodeList* pRightProjections = getProjectList(pSetOperator->pRight);
  if (LIST_LENGTH(pLeftProjections) != LIST_LENGTH(pRightProjections)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INCORRECT_NUM_OF_COL);
  }

  SNode* pLeft = NULL;
  SNode* pRight = NULL;
  FORBOTH(pLeft, pLeftProjections, pRight, pRightProjections) {
    SExprNode* pLeftExpr = (SExprNode*)pLeft;
    SExprNode* pRightExpr = (SExprNode*)pRight;
    int32_t    comp = dataTypeComp(&pLeftExpr->resType, &pRightExpr->resType);
    if (comp > 0) {
      SNode*  pRightFunc = NULL;
      int32_t code = createCastFunc(pCxt, pRight, pLeftExpr->resType, &pRightFunc);
      if (TSDB_CODE_SUCCESS != code || NULL == pRightFunc) {  // deal scan coverity
        return code;
      }
      REPLACE_LIST2_NODE(pRightFunc);
      pRightExpr = (SExprNode*)pRightFunc;
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
  /*
    if (TSDB_CODE_SUCCESS == code) {
      if (other) {
        pCxt->currClause = SQL_CLAUSE_ORDER_BY;
        pCxt->pCurrStmt = (SNode*)pSetOperator;
        code = translateExprList(pCxt, pSetOperator->pOrderByList);
      }
    }
  */
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
  if ((NULL != pLimit && pLimit->offset < 0)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OFFSET_LESS_ZERO);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSetOperator(STranslateContext* pCxt, SSetOperator* pSetOperator) {
  if (pCxt->pParseCxt && pCxt->pParseCxt->setQueryFp) {
    (*pCxt->pParseCxt->setQueryFp)(pCxt->pParseCxt->requestRid);
  }

  int32_t code = translateQuery(pCxt, pSetOperator->pLeft);
  if (TSDB_CODE_SUCCESS == code) {
    code = resetHighLevelTranslateNamespace(pCxt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateQuery(pCxt, pSetOperator->pRight);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pSetOperator->joinContains = getBothJoinContais(pSetOperator->pLeft, pSetOperator->pRight);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pSetOperator->precision = calcSetOperatorPrecision(pSetOperator);
    code = translateSetOperProject(pCxt, pSetOperator);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateSetOperOrderBy(pCxt, pSetOperator);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkSetOperLimit(pCxt, (SLimitNode*)pSetOperator->pLimit);
  }
  return code;
}

static int32_t partitionDeleteWhere(STranslateContext* pCxt, SDeleteStmt* pDelete) {
  if (NULL == pDelete->pWhere) {
    pDelete->timeRange = TSWINDOW_INITIALIZER;
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
    code = getTimeRange(&pPrimaryKeyCond, &pDelete->timeRange, &isStrict);
    if (TSDB_CODE_SUCCESS == code && !isStrict) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DELETE_WHERE);
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
  int32_t code = nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrderByExpr);
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
  ((SExprNode*)pOrderByExpr->pExpr)->orderAlias = true;
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
      TSDB_NORMAL_TABLE != ((SRealTableNode*)*pTable)->pMeta->tableType) {
    code = buildInvalidOperationMsg(&pCxt->msgBuf, "insert data into super table is not supported");
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
      return TSDB_CODE_OUT_OF_MEMORY;
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
        code = TSDB_CODE_OUT_OF_MEMORY;
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
  SName name = {0};
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
  pReq->s3ChunkSize = pStmt->pOptions->s3ChunkSize;
  pReq->s3KeepLocal = pStmt->pOptions->s3KeepLocal;
  pReq->s3Compact = pStmt->pOptions->s3Compact;
  pReq->ignoreExist = pStmt->ignoreExists;
  pReq->withArbitrator = pStmt->pOptions->withArbitrator;
  pReq->encryptAlgorithm = pStmt->pOptions->encryptAlgorithm;
  return buildCreateDbRetentions(pStmt->pOptions->pRetentions, pReq);
}

static int32_t checkRangeOption(STranslateContext* pCxt, int32_t code, const char* pName, int64_t val, int64_t minVal,
                                int64_t maxVal, bool skipUndef) {
  if (skipUndef ? ((val >= 0) && (val < minVal || val > maxVal)) : (val < minVal || val > maxVal)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, code,
                                   "Invalid option %s: %" PRId64 ", valid range: [%" PRId64 ", %" PRId64 "]", pName,
                                   val, minVal, maxVal);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbRangeOption(STranslateContext* pCxt, const char* pName, int32_t val, int32_t minVal,
                                  int32_t maxVal) {
  return checkRangeOption(pCxt, TSDB_CODE_PAR_INVALID_DB_OPTION, pName, val, minVal, maxVal, true);
}

static int32_t checkTableRangeOption(STranslateContext* pCxt, const char* pName, int64_t val, int64_t minVal,
                                     int64_t maxVal) {
  return checkRangeOption(pCxt, TSDB_CODE_PAR_INVALID_TABLE_OPTION, pName, val, minVal, maxVal, true);
}

static int32_t checkDbS3KeepLocalOption(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
  if (NULL != pOptions->s3KeepLocalStr) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pOptions->s3KeepLocalStr)) {
      return pCxt->errCode;
    }
    if (TIME_UNIT_MINUTE != pOptions->s3KeepLocalStr->unit && TIME_UNIT_HOUR != pOptions->s3KeepLocalStr->unit &&
        TIME_UNIT_DAY != pOptions->s3KeepLocalStr->unit) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option s3_keeplocal unit: %c, only %c, %c, %c allowed",
                                     pOptions->s3KeepLocalStr->unit, TIME_UNIT_MINUTE, TIME_UNIT_HOUR, TIME_UNIT_DAY);
    }
    pOptions->s3KeepLocal = getBigintFromValueNode(pOptions->s3KeepLocalStr);
  }
  return checkDbRangeOption(pCxt, "s3KeepLocal", pOptions->s3KeepLocal, TSDB_MIN_S3_KEEP_LOCAL, TSDB_MAX_S3_KEEP_LOCAL);
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

static int32_t checkOptionsDependency(STranslateContext* pCxt, const char* pDbName, SDatabaseOptions* pOptions) {
  int32_t daysPerFile = pOptions->daysPerFile;
  int32_t s3KeepLocal = pOptions->s3KeepLocal;
  int64_t daysToKeep0 = pOptions->keep[0];
  if (-1 == daysPerFile && -1 == daysToKeep0) {
    return TSDB_CODE_SUCCESS;
  } else if (-1 == daysPerFile || -1 == daysToKeep0) {
    SDbCfgInfo dbCfg = {0};
    int32_t    code = getDBCfg(pCxt, pDbName, &dbCfg);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    daysPerFile = (-1 == daysPerFile ? dbCfg.daysPerFile : daysPerFile);
    daysToKeep0 = (-1 == daysToKeep0 ? dbCfg.daysToKeep0 : daysToKeep0);
  }
  if (daysPerFile > daysToKeep0 / 3) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid duration value, should be keep2 >= keep1 >= keep0 >= 3 * duration");
  }
  if (s3KeepLocal > 0 && daysPerFile > s3KeepLocal / 3) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid parameters, should be s3_keeplocal >= 3 * duration");
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
    code = checkDbS3KeepLocalOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkOptionsDependency(pCxt, pDbName, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code =
        checkDbRangeOption(pCxt, "s3_chunksize", pOptions->s3ChunkSize, TSDB_MIN_S3_CHUNK_SIZE, TSDB_MAX_S3_CHUNK_SIZE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "s3_compact", pOptions->s3Compact, TSDB_MIN_S3_COMPACT, TSDB_MAX_S3_COMPACT);
  }
  return code;
}

static int32_t checkCreateDatabase(STranslateContext* pCxt, SCreateDatabaseStmt* pStmt) {
  if (NULL != strchr(pStmt->dbName, '.')) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME,
                                   "The database name cannot contain '.'");
  }
  return checkDatabaseOptions(pCxt, pStmt->dbName, pStmt->pOptions);
}

#define FILL_CMD_SQL(sql, sqlLen, pCmdReq, CMD_TYPE, genericCmd) \
  CMD_TYPE* pCmdReq = genericCmd;                                \
  char*     cmdSql = taosMemoryMalloc(sqlLen);                   \
  if (cmdSql == NULL) {                                          \
    return TSDB_CODE_OUT_OF_MEMORY;                              \
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

    case TDMT_MND_DROP_STREAM: {
      FILL_CMD_SQL(sql, sqlLen, pCmdReq, SMDropStreamReq, pReq);
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
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = msgType;
  pCxt->pCmdMsg->msgLen = func(NULL, 0, pReq);
  if (pCxt->pCmdMsg->msgLen < 0) {
    return terrno;
  }
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    taosMemoryFreeClear(pCxt->pCmdMsg);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (-1 == func(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, pReq)) {
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
  SDropDbReq dropReq = {0};
  SName      name = {0};
  int32_t code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, dropReq.db);
  dropReq.ignoreNotExists = pStmt->ignoreNotExists;

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
  pReq->s3KeepLocal = pStmt->pOptions->s3KeepLocal;
  pReq->s3Compact = pStmt->pOptions->s3Compact;
  pReq->withArbitrator = pStmt->pOptions->withArbitrator;
  return code;
}

static int32_t translateAlterDatabase(STranslateContext* pCxt, SAlterDatabaseStmt* pStmt) {
  if (pStmt->pOptions->walLevel == 0) {
    SDbCfgInfo dbCfg = {0};
    int32_t    code = getDBCfg(pCxt, pStmt->dbName, &dbCfg);
    if (TSDB_CODE_SUCCESS == code && dbCfg.replications > 1) {
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
  STrimDbReq req = {.maxSpeed = pStmt->maxSpeed};
  SName      name = {0};
  int32_t code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, req.db);
  return buildCmdMsg(pCxt, TDMT_MND_TRIM_DB, (FSerializeFunc)tSerializeSTrimDbReq, &req);
}

// <<<<<<< HEAD
// static int32_t columnDefNodeToField(SNodeList* pList, SArray** pArray, bool calBytes) {
// =======
static int32_t checkColumnOptions(SNodeList* pList) {
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
  }
  return TSDB_CODE_SUCCESS;
}
static int32_t translateS3MigrateDatabase(STranslateContext* pCxt, SS3MigrateDatabaseStmt* pStmt) {
  SS3MigrateDbReq req = {0};
  SName           name = {0};
  int32_t         code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, req.db);
  return buildCmdMsg(pCxt, TDMT_MND_S3MIGRATE_DB, (FSerializeFunc)tSerializeSS3MigrateDbReq, &req);
}

static int32_t columnDefNodeToField(SNodeList* pList, SArray** pArray, bool calBytes) {
  *pArray = taosArrayInit(LIST_LENGTH(pList), sizeof(SFieldWithOptions));
  if (!pArray) return TSDB_CODE_OUT_OF_MEMORY;

  int32_t code = TSDB_CODE_SUCCESS;
  SNode* pNode;
  FOREACH(pNode, pList) {
    SColumnDefNode*   pCol = (SColumnDefNode*)pNode;
    SFieldWithOptions field = {.type = pCol->dataType.type, .bytes = calcTypeBytes(pCol->dataType)};
    if (calBytes) {
      field.bytes = calcTypeBytes(pCol->dataType);
    } else {
      field.bytes = pCol->dataType.bytes;
    }

    strcpy(field.name, pCol->colName);
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
    if (NULL == taosArrayPush(*pArray, &field)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      break;
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    taosArrayDestroy(*pArray);
    *pArray = NULL;
  }
  return code;
}

static int32_t tagDefNodeToField(SNodeList* pList, SArray** pArray, bool calBytes) {
  *pArray = taosArrayInit(LIST_LENGTH(pList), sizeof(SField));
  if (!*pArray) return TSDB_CODE_OUT_OF_MEMORY;
  SNode* pNode;
  FOREACH(pNode, pList) {
    SColumnDefNode* pCol = (SColumnDefNode*)pNode;
    SField          field = {
                 .type = pCol->dataType.type,
    };
    if (calBytes) {
      field.bytes = calcTypeBytes(pCol->dataType);
    } else {
      field.bytes = pCol->dataType.bytes;
    }
    strcpy(field.name, pCol->colName);
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

static int32_t checkTableTagsSchema(STranslateContext* pCxt, SHashObj* pHash, SNodeList* pTags) {
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
      if ((TSDB_DATA_TYPE_VARCHAR == pTag->dataType.type && calcTypeBytes(pTag->dataType) > TSDB_MAX_TAGS_LEN) ||
          (TSDB_DATA_TYPE_VARBINARY == pTag->dataType.type && calcTypeBytes(pTag->dataType) > TSDB_MAX_TAGS_LEN) ||
          (TSDB_DATA_TYPE_NCHAR == pTag->dataType.type && calcTypeBytes(pTag->dataType) > TSDB_MAX_TAGS_LEN) ||
          (TSDB_DATA_TYPE_GEOMETRY == pTag->dataType.type && calcTypeBytes(pTag->dataType) > TSDB_MAX_TAGS_LEN)) {
        code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = taosHashPut(pHash, pTag->colName, len, &pTag, POINTER_BYTES);
    }
    if (TSDB_CODE_SUCCESS == code) {
      tagsSize += calcTypeBytes(pTag->dataType);
    } else {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code && tagsSize > TSDB_MAX_TAGS_LEN) {
    code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAGS_LENGTH, TSDB_MAX_TAGS_LEN);
  }

  return code;
}

static int32_t checkTableColsSchema(STranslateContext* pCxt, SHashObj* pHash, int32_t ntags, SNodeList* pCols,
                                    SNodeList* pRollupFuncs) {
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
      if ((TSDB_DATA_TYPE_VARCHAR == pCol->dataType.type && calcTypeBytes(pCol->dataType) > TSDB_MAX_BINARY_LEN) ||
          (TSDB_DATA_TYPE_VARBINARY == pCol->dataType.type && calcTypeBytes(pCol->dataType) > TSDB_MAX_BINARY_LEN) ||
          (TSDB_DATA_TYPE_NCHAR == pCol->dataType.type && calcTypeBytes(pCol->dataType) > TSDB_MAX_NCHAR_LEN) ||
          (TSDB_DATA_TYPE_GEOMETRY == pCol->dataType.type && calcTypeBytes(pCol->dataType) > TSDB_MAX_GEOMETRY_LEN)) {
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

    if (TSDB_CODE_SUCCESS == code) {
      code = taosHashPut(pHash, pCol->colName, len, &pCol, POINTER_BYTES);
    }
    if (TSDB_CODE_SUCCESS == code) {
      rowSize += calcTypeBytes(pCol->dataType);
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
                                    SNodeList* pRollupFuncs) {
  SHashObj* pHash = taosHashInit(LIST_LENGTH(pTags) + LIST_LENGTH(pCols),
                                 taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == pHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = checkTableTagsSchema(pCxt, pHash, pTags);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableColsSchema(pCxt, pHash, LIST_LENGTH(pTags), pCols, pRollupFuncs);
  }

  taosHashCleanup(pHash);
  return code;
}

static int32_t checkTableSchema(STranslateContext* pCxt, SCreateTableStmt* pStmt) {
  return checkTableSchemaImpl(pCxt, pStmt->pTags, pStmt->pCols, pStmt->pOptions->pRollupFuncs);
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
    code = checkColumnOptions(pStmt->pCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (createStable && pStmt->pOptions->ttl != 0) {
      code = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TABLE_OPTION,
                                     "Only supported for create non-super table in databases "
                                     "configured with the 'TTL' option");
    }
  }
  if (pCxt->pParseCxt->biMode != 0 && TSDB_CODE_SUCCESS == code) {
    code = biCheckCreateTableTbnameCol(pCxt, pStmt);
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
  pSchema->colId = colId;
  pSchema->type = pCol->dataType.type;
  pSchema->bytes = calcTypeBytes(pCol->dataType);
  pSchema->flags = flags;
  strcpy(pSchema->name, pCol->colName);
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
  int32_t code = nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)&pTable);
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
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  strcpy(pFunc->functionName, "_wstart");
  strcpy(pFunc->node.userAlias, "_wstart");
  return nodesListPushFront(pProjectionList, (SNode*)pFunc);
}

static int32_t addWendToSampleProjects(SNodeList* pProjectionList) {
  SFunctionNode* pFunc = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  strcpy(pFunc->functionName, "_wend");
  strcpy(pFunc->node.userAlias, "_wend");
  return nodesListAppend(pProjectionList, (SNode*)pFunc);
}

static int32_t addWdurationToSampleProjects(SNodeList* pProjectionList) {
  SFunctionNode* pFunc = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  strcpy(pFunc->functionName, "_wduration");
  strcpy(pFunc->node.userAlias, "_wduration");
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
      sprintf(((SExprNode*)pProject)->aliasName, "#%p", pProject);
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
  int32_t code = nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW, (SNode**)&pInterval);
  if (NULL == pInterval) {
    return code;
  }
  TSWAP(pInterval->pInterval, pInfo->pInterval);
  TSWAP(pInterval->pOffset, pInfo->pOffset);
  TSWAP(pInterval->pSliding, pInfo->pSliding);
  pInterval->pCol = NULL;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pInterval->pCol);
  if (NULL == pInterval->pCol) {
    nodesDestroyNode((SNode*)pInterval);
    return code;
  }
  ((SColumnNode*)pInterval->pCol)->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  strcpy(((SColumnNode*)pInterval->pCol)->colName, ROWTS_PSEUDO_COLUMN_NAME);
  *pOutput = (SNode*)pInterval;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildSampleAst(STranslateContext* pCxt, SSampleAstInfo* pInfo, char** pAst, int32_t* pLen, char** pExpr,
                              int32_t* pExprLen, int32_t* pProjectionTotalLen) {
  SSelectStmt* pSelect = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_SELECT_STMT, (SNode**)&pSelect);
  if (NULL == pSelect) {
    return code;
  }
  sprintf(pSelect->stmtName, "%p", pSelect);

  code = buildTableForSampleAst(pInfo, &pSelect->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildProjectsForSampleAst(pInfo, &pSelect->pProjectionList, pProjectionTotalLen);
  }
  if (TSDB_CODE_SUCCESS == code) {
    TSWAP(pInfo->pSubTable, pSelect->pSubtable);
    TSWAP(pInfo->pTags, pSelect->pTags);
    TSWAP(pSelect->pPartitionByList, pInfo->pPartitionByList);
    code = buildIntervalForSampleAst(pInfo, &pSelect->pWindow);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->createStream = true;
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
  int32_t code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);
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
  int32_t len = snprintf(buf, sizeof(buf), "%" PRId64 "%c", timeVal, pRetension->freqUnit);
  pVal->literal = strndup(buf, len);
  if (NULL == pVal->literal) {
    nodesDestroyNode((SNode*)pVal);
    return TSDB_CODE_OUT_OF_MEMORY;
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
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return code;
  }
  strcpy(pCol->colName, pDef->colName);
  *ppCol = (SNode*)pCol;
  return code;
}

static int32_t createRollupFunc(SNode* pSrcFunc, SColumnDefNode* pColDef, SNode** ppRollupFunc) {
  SFunctionNode* pFunc = NULL;
  int32_t code = nodesCloneNode(pSrcFunc, (SNode**)&pFunc);
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
  int32_t code = nodesMakeList(&pFuncs);
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

  return code;;
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

static int32_t createTbnameFunction(SFunctionNode** ppFunc) {
  SFunctionNode* pFunc = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  strcpy(pFunc->functionName, "tbname");
  strcpy(pFunc->node.aliasName, "tbname");
  strcpy(pFunc->node.userAlias, "tbname");
  *ppFunc = pFunc;
  return code;
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
  if (!*pArray) return TSDB_CODE_OUT_OF_MEMORY;
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
  pReq->colVer = 1;
  pReq->tagVer = 1;
  pReq->source = TD_REQ_FROM_APP;
  // columnDefNodeToField(pStmt->pCols, &pReq->pColumns, true);
  // columnDefNodeToField(pStmt->pTags, &pReq->pTags, true);
  code = columnDefNodeToField(pStmt->pCols, &pReq->pColumns, true);
  if (TSDB_CODE_SUCCESS == code)
    code = tagDefNodeToField(pStmt->pTags, &pReq->pTags, true);
  if (TSDB_CODE_SUCCESS == code) {
    pReq->numOfColumns = LIST_LENGTH(pStmt->pCols);
    pReq->numOfTags = LIST_LENGTH(pStmt->pTags);
    if (pStmt->pOptions->commentNull == false) {
      pReq->pComment = taosStrdup(pStmt->pOptions->comment);
      if (NULL == pReq->pComment) {
        return TSDB_CODE_OUT_OF_MEMORY;
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
  if (TSDB_CODE_SUCCESS == code)
    code = collectUseTable(&tableName, pCxt->pTables);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(&tableName, pCxt->pTargetTables);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildRollupAst(pCxt, pStmt, pReq);
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
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      pAlterReq->commentLen = strlen(pStmt->pOptions->comment);
    } else {
      pAlterReq->commentLen = -1;
    }

    return TSDB_CODE_SUCCESS;
  }

  pAlterReq->pFields = taosArrayInit(2, sizeof(TAOS_FIELD));
  if (NULL == pAlterReq->pFields) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  switch (pStmt->alterType) {
    case TSDB_ALTER_TABLE_ADD_COLUMN:
    case TSDB_ALTER_TABLE_ADD_TAG:
    case TSDB_ALTER_TABLE_DROP_TAG:
    case TSDB_ALTER_TABLE_DROP_COLUMN:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES: {
      TAOS_FIELD field = {.type = pStmt->dataType.type, .bytes = calcTypeBytes(pStmt->dataType)};
      strcpy(field.name, pStmt->colName);
      if (NULL == taosArrayPush(pAlterReq->pFields, &field)) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME: {
      TAOS_FIELD oldField = {0};
      strcpy(oldField.name, pStmt->colName);
      if (NULL == taosArrayPush(pAlterReq->pFields, &oldField)) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      TAOS_FIELD newField = {0};
      strcpy(newField.name, pStmt->newColName);
      if (NULL == taosArrayPush(pAlterReq->pFields, &newField)) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS: {
      TAOS_FIELD field = {0};
      strcpy(field.name, pStmt->colName);
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
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      break;
    }
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION: {
      taosArrayDestroy(pAlterReq->pFields);

      pAlterReq->pFields = taosArrayInit(1, sizeof(SFieldWithOptions));
      if (!pAlterReq->pFields) return TSDB_CODE_OUT_OF_MEMORY;
      SFieldWithOptions field = {.type = pStmt->dataType.type, .bytes = calcTypeBytes(pStmt->dataType)};
      // TAOS_FIELD        field = {.type = pStmt->dataType.type, .bytes = calcTypeBytes(pStmt->dataType)};
      strcpy(field.name, pStmt->colName);
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
      if (NULL == taosArrayPush(pAlterReq->pFields, &field)) return TSDB_CODE_OUT_OF_MEMORY;
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

static int32_t checkAlterSuperTableBySchema(STranslateContext* pCxt, SAlterTableStmt* pStmt,
                                            const STableMeta* pTableMeta) {
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
  if (TSDB_ALTER_TABLE_UPDATE_TAG_VAL == pStmt->alterType) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE,
                                   "Set tag value only available for child table");
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
  int32_t code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS == code) {
    code = tNameExtractFullName(&name, usedbReq.db);
  }
  if (TSDB_CODE_SUCCESS == code)
    code = getDBVgVersion(pCxt, usedbReq.db, &usedbReq.vgVersion, &usedbReq.dbId, &usedbReq.numOfTable, &usedbReq.stateTs);
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
  strcpy(createReq.user, pStmt->userName);
  createReq.createType = 0;
  createReq.superUser = 0;
  createReq.sysInfo = pStmt->sysinfo;
  createReq.enable = 1;
  strcpy(createReq.pass, pStmt->password);
  createReq.isImport = pStmt->isImport;
  createReq.createDb = pStmt->createDb;

  createReq.numIpRanges = pStmt->numIpRanges;
  if (pStmt->numIpRanges > 0) {
    createReq.pIpRanges = taosMemoryMalloc(createReq.numIpRanges * sizeof(SIpV4Range));
    memcpy(createReq.pIpRanges, pStmt->pIpRanges, sizeof(SIpV4Range) * createReq.numIpRanges);
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
  strcpy(alterReq.user, pStmt->userName);
  alterReq.alterType = pStmt->alterType;
  alterReq.superUser = 0;
  alterReq.enable = pStmt->enable;
  alterReq.sysInfo = pStmt->sysinfo;
  alterReq.createdb = pStmt->createdb ? 1 : 0;
  snprintf(alterReq.pass, sizeof(alterReq.pass), "%s", pStmt->password);
  if (NULL != pCxt->pParseCxt->db) {
    snprintf(alterReq.objname, sizeof(alterReq.objname), "%s", pCxt->pParseCxt->db);
  }

  alterReq.numIpRanges = pStmt->numIpRanges;
  if (pStmt->numIpRanges > 0) {
    alterReq.pIpRanges = taosMemoryMalloc(alterReq.numIpRanges * sizeof(SIpV4Range));
    memcpy(alterReq.pIpRanges, pStmt->pIpRanges, sizeof(SIpV4Range) * alterReq.numIpRanges);
  }
  code = buildCmdMsg(pCxt, TDMT_MND_ALTER_USER, (FSerializeFunc)tSerializeSAlterUserReq, &alterReq);
  tFreeSAlterUserReq(&alterReq);
  return code;
}

static int32_t translateDropUser(STranslateContext* pCxt, SDropUserStmt* pStmt) {
  SDropUserReq dropReq = {0};
  strcpy(dropReq.user, pStmt->userName);

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_DROP_USER, (FSerializeFunc)tSerializeSDropUserReq, &dropReq);
  tFreeSDropUserReq(&dropReq);
  return code;
}

static int32_t translateCreateAnode(STranslateContext* pCxt, SCreateAnodeStmt* pStmt) {
  // #ifndef TD_ENTERPRISE
  //   return TSDB_CODE_OPS_NOT_SUPPORT;
  // #endif

  SMCreateAnodeReq createReq = {0};
  createReq.urlLen = strlen(pStmt->url) + 1;
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

static int32_t translateCreateDnode(STranslateContext* pCxt, SCreateDnodeStmt* pStmt) {
  SCreateDnodeReq createReq = {0};
  strcpy(createReq.fqdn, pStmt->fqdn);
  createReq.port = pStmt->port;

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_CREATE_DNODE, (FSerializeFunc)tSerializeSCreateDnodeReq, &createReq);
  tFreeSCreateDnodeReq(&createReq);
  return code;
}

static int32_t translateDropDnode(STranslateContext* pCxt, SDropDnodeStmt* pStmt) {
  SDropDnodeReq dropReq = {0};
  dropReq.dnodeId = pStmt->dnodeId;
  strcpy(dropReq.fqdn, pStmt->fqdn);
  dropReq.port = pStmt->port;
  dropReq.force = pStmt->force;
  dropReq.unsafe = pStmt->unsafe;

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_DROP_DNODE, (FSerializeFunc)tSerializeSDropDnodeReq, &dropReq);
  tFreeSDropDnodeReq(&dropReq);
  return code;
}

static int32_t translateAlterDnode(STranslateContext* pCxt, SAlterDnodeStmt* pStmt) {
  SMCfgDnodeReq cfgReq = {0};
  cfgReq.dnodeId = pStmt->dnodeId;
  strcpy(cfgReq.config, pStmt->config);
  strcpy(cfgReq.value, pStmt->value);

  int32_t code = 0;
  if (0 == strncasecmp(cfgReq.config, "encrypt_key", 12)) {
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
  strcpy(cfgReq.config, pStmt->config);
  strcpy(cfgReq.value, pStmt->value);

  int32_t code = buildCmdMsg(pCxt, TDMT_MND_CONFIG_CLUSTER, (FSerializeFunc)tSerializeSMCfgClusterReq, &cfgReq);
  tFreeSMCfgClusterReq(&cfgReq);
  return code;
}

static int32_t getSmaIndexDstVgId(STranslateContext* pCxt, const char* pDbName, const char* pTableName,
                                  int32_t* pVgId) {
  SVgroupInfo vg = {0};
  int32_t     code = getTableHashVgroup(pCxt, pDbName, pTableName, &vg);
  if (TSDB_CODE_SUCCESS == code) {
    *pVgId = vg.vgId;
  }
  return code;
}

static int32_t getSmaIndexSql(STranslateContext* pCxt, char** pSql, int32_t* pLen) {
  *pSql = taosStrdup(pCxt->pParseCxt->pSql);
  if (NULL == *pSql) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pLen = pCxt->pParseCxt->sqlLen + 1;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildSampleAstInfoByIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt, SSampleAstInfo* pInfo) {
  pInfo->createSmaIndex = true;
  pInfo->pDbName = pStmt->dbName;
  pInfo->pTableName = pStmt->tableName;
  int32_t code = nodesCloneList(pStmt->pOptions->pFuncs, &pInfo->pFuncs);
  if (TSDB_CODE_SUCCESS != code) return code;
  code = nodesCloneNode(pStmt->pOptions->pInterval, &pInfo->pInterval);
  if (TSDB_CODE_SUCCESS != code) return code;
  code = nodesCloneNode(pStmt->pOptions->pOffset, &pInfo->pOffset);
  if (TSDB_CODE_SUCCESS != code) return code;
  code = nodesCloneNode(pStmt->pOptions->pSliding, &pInfo->pSliding);
  if (TSDB_CODE_SUCCESS != code) return code;
  return TSDB_CODE_SUCCESS;
}

static int32_t getSmaIndexAst(STranslateContext* pCxt, SCreateIndexStmt* pStmt, char** pAst, int32_t* pLen,
                              char** pExpr, int32_t* pExprLen) {
  SSampleAstInfo info = {0};
  int32_t        code = buildSampleAstInfoByIndex(pCxt, pStmt, &info);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildSampleAst(pCxt, &info, pAst, pLen, pExpr, pExprLen, NULL);
  }
  clearSampleAstInfo(&info);
  return code;
}

static int32_t buildCreateSmaReq(STranslateContext* pCxt, SCreateIndexStmt* pStmt, SMCreateSmaReq* pReq) {
  SName name = {0};
  toName(pCxt->pParseCxt->acctId, pStmt->indexDbName, pStmt->indexName, &name);
  int32_t code = tNameExtractFullName(&name, pReq->name);
  if (TSDB_CODE_SUCCESS == code) {
    memset(&name, 0, sizeof(SName));
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);
    code = tNameExtractFullName(&name, pReq->stb);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pReq->igExists = pStmt->ignoreExists;
    pReq->interval = ((SValueNode*)pStmt->pOptions->pInterval)->datum.i;
    pReq->intervalUnit = ((SValueNode*)pStmt->pOptions->pInterval)->unit;
    pReq->offset = (NULL != pStmt->pOptions->pOffset ? ((SValueNode*)pStmt->pOptions->pOffset)->datum.i : 0);
    pReq->sliding =
      (NULL != pStmt->pOptions->pSliding ? ((SValueNode*)pStmt->pOptions->pSliding)->datum.i : pReq->interval);
    pReq->slidingUnit =
      (NULL != pStmt->pOptions->pSliding ? ((SValueNode*)pStmt->pOptions->pSliding)->unit : pReq->intervalUnit);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pStmt->pOptions->pStreamOptions) {
    SStreamOptions* pStreamOpt = (SStreamOptions*)pStmt->pOptions->pStreamOptions;
    if (NULL != pStreamOpt->pDelay) {
      code = getTableMaxDelayOption(pCxt, (SValueNode*)pStreamOpt->pDelay, &pReq->maxDelay);
    } else {
      pReq->maxDelay = -1;
    }
    if (TSDB_CODE_SUCCESS == code) {
      if (NULL != pStreamOpt->pWatermark) {
        code = getTableWatermarkOption(pCxt, (SValueNode*)pStreamOpt->pWatermark, &pReq->watermark);
      } else {
        pReq->watermark = TSDB_DEFAULT_ROLLUP_WATERMARK;
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      if (NULL != pStreamOpt->pDeleteMark) {
        code = getTableDeleteMarkOption(pCxt, (SValueNode*)pStreamOpt->pDeleteMark, &pReq->deleteMark);
      } else {
        pReq->deleteMark = TSDB_DEFAULT_ROLLUP_DELETE_MARK;
      }
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = getSmaIndexDstVgId(pCxt, pStmt->dbName, pStmt->tableName, &pReq->dstVgId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = getSmaIndexSql(pCxt, &pReq->sql, &pReq->sqlLen);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = getSmaIndexAst(pCxt, pStmt, &pReq->ast, &pReq->astLen, &pReq->expr, &pReq->exprLen);
  }
  if (TSDB_CODE_SUCCESS == code) {
    STableMeta* pMetaCache = NULL;
    code = getTableMeta(pCxt, pStmt->dbName, pStmt->tableName, &pMetaCache);
    if (TSDB_CODE_SUCCESS == code) {
      pStmt->pOptions->tsPrecision = pMetaCache->tableInfo.precision;
      code = createLastTsSelectStmt(pStmt->dbName, pStmt->tableName, pMetaCache->schema[0].name, &pStmt->pPrevQuery);
    }
    taosMemoryFreeClear(pMetaCache);
  }

  return code;
}

static int32_t checkCreateSmaIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt) {
  SDbCfgInfo dbCfg = {0};
  int32_t    code = getDBCfg(pCxt, pStmt->dbName, &dbCfg);
  if (TSDB_CODE_SUCCESS == code && NULL != dbCfg.pRetensions) {
    code = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_SMA_INDEX,
                                   "Tables configured with the 'ROLLUP' option do not support creating sma index");
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = doTranslateValue(pCxt, (SValueNode*)pStmt->pOptions->pInterval);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pStmt->pOptions->pOffset) {
    code = doTranslateValue(pCxt, (SValueNode*)pStmt->pOptions->pOffset);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pStmt->pOptions->pSliding) {
    code = doTranslateValue(pCxt, (SValueNode*)pStmt->pOptions->pSliding);
  }

  return code;
}

static int32_t translateCreateSmaIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt) {
  int32_t code = checkCreateSmaIndex(pCxt, pStmt);
  pStmt->pReq = taosMemoryCalloc(1, sizeof(SMCreateSmaReq));
  if (pStmt->pReq == NULL) code = terrno;
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateSmaReq(pCxt, pStmt, pStmt->pReq);
  }
  TSWAP(pCxt->pPrevRoot, pStmt->pPrevQuery);
  return code;
}

int32_t createIntervalFromCreateSmaIndexStmt(SCreateIndexStmt* pStmt, SInterval* pInterval) {
  pInterval->interval = ((SValueNode*)pStmt->pOptions->pInterval)->datum.i;
  pInterval->intervalUnit = ((SValueNode*)pStmt->pOptions->pInterval)->unit;
  pInterval->offset = NULL != pStmt->pOptions->pOffset ? ((SValueNode*)pStmt->pOptions->pOffset)->datum.i : 0;
  pInterval->sliding =
      NULL != pStmt->pOptions->pSliding ? ((SValueNode*)pStmt->pOptions->pSliding)->datum.i : pInterval->interval;
  pInterval->slidingUnit =
      NULL != pStmt->pOptions->pSliding ? ((SValueNode*)pStmt->pOptions->pSliding)->unit : pInterval->intervalUnit;
  pInterval->precision = pStmt->pOptions->tsPrecision;
  return TSDB_CODE_SUCCESS;
}

int32_t translatePostCreateSmaIndex(SParseContext* pParseCxt, SQuery* pQuery, SSDataBlock* pBlock) {
  int32_t           code = TSDB_CODE_SUCCESS;
  SCreateIndexStmt* pStmt = (SCreateIndexStmt*)pQuery->pRoot;
  int64_t           lastTs = 0;
  SInterval         interval = {0};
  STranslateContext pCxt = {0};
  code = initTranslateContext(pParseCxt, NULL, &pCxt);
  if (TSDB_CODE_SUCCESS == code) {
    code = createIntervalFromCreateSmaIndexStmt(pStmt, &interval);
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (pBlock != NULL && pBlock->info.rows >= 1) {
      SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, 0);
      lastTs = *(int64_t*)colDataGetData(pColInfo, 0);
    } else if (interval.interval > 0) {
      lastTs = taosGetTimestamp(interval.precision);
    } else {
      lastTs = taosGetTimestampMs();
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (interval.interval > 0) {
      pStmt->pReq->lastTs = taosTimeTruncate(lastTs, &interval);
    } else {
      pStmt->pReq->lastTs = lastTs;
    }
    code = buildCmdMsg(&pCxt, TDMT_MND_CREATE_SMA, (FSerializeFunc)tSerializeSMCreateSmaReq, pStmt->pReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setQuery(&pCxt, pQuery);
  }
  int32_t tmpCode = setRefreshMeta(&pCxt, pQuery);
  if (TSDB_CODE_SUCCESS == code) code = tmpCode;
  destroyTranslateContext(&pCxt);
  tFreeSMCreateSmaReq(pStmt->pReq);
  taosMemoryFreeClear(pStmt->pReq);
  return code;
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
_exit:
  taosMemoryFree(pMeta);
  return code;
}

static int32_t translateCreateIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt) {
  if (INDEX_TYPE_FULLTEXT == pStmt->indexType) {
    return translateCreateFullTextIndex(pCxt, pStmt);
  } else if (INDEX_TYPE_NORMAL == pStmt->indexType) {
    return translateCreateNormalIndex(pCxt, pStmt);
  }
  return translateCreateSmaIndex(pCxt, pStmt);
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
    case QUERY_NODE_CREATE_BNODE_STMT:
      return TDMT_MND_CREATE_BNODE;
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
    case QUERY_NODE_DROP_BNODE_STMT:
      return TDMT_MND_DROP_BNODE;
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
  if (pSelect->hasAggFuncs || pSelect->hasInterpFunc || pSelect->hasIndefiniteRowsFunc) {
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
    return TSDB_CODE_OUT_OF_MEMORY;
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
    if (TSDB_CODE_SUCCESS == code)
      (void)tNameGetFullDbName(&name, pReq->subDbName);
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
    int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&col);
    if (NULL == col) {
      return code;
    }
    strcpy(col->colName, column->name);
    strcpy(col->node.aliasName, col->colName);
    strcpy(col->node.userAlias, col->colName);
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

  SNodeList* pProjection = NULL;
  SRealTableNode* realTable = NULL;
  code = checkCollectTopicTags(pCxt, pStmt, pMeta, &pProjection);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)&realTable);
    if (realTable) {
      strcpy(realTable->table.dbName, pStmt->subDbName);
      strcpy(realTable->table.tableName, pStmt->subSTbName);
      strcpy(realTable->table.tableAlias, pStmt->subSTbName);
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
  strcpy(dropReq.cgroup, pStmt->cgroup);

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

static int32_t translateCompactRange(STranslateContext* pCxt, SCompactDatabaseStmt* pStmt, SCompactDbReq* pReq) {
  SDbCfgInfo dbCfg = {0};
  int32_t    code = getDBCfg(pCxt, pStmt->dbName, &dbCfg);
  if (TSDB_CODE_SUCCESS == code && NULL != pStmt->pStart) {
    ((SValueNode*)pStmt->pStart)->node.resType.precision = dbCfg.precision;
    ((SValueNode*)pStmt->pStart)->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    code = doTranslateValue(pCxt, (SValueNode*)pStmt->pStart);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pStmt->pEnd) {
    ((SValueNode*)pStmt->pEnd)->node.resType.precision = dbCfg.precision;
    ((SValueNode*)pStmt->pEnd)->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    code = doTranslateValue(pCxt, (SValueNode*)pStmt->pEnd);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pReq->timeRange.skey = NULL != pStmt->pStart ? ((SValueNode*)pStmt->pStart)->datum.i : INT64_MIN;
    pReq->timeRange.ekey = NULL != pStmt->pEnd ? ((SValueNode*)pStmt->pEnd)->datum.i : INT64_MAX;
  }
  return code;
}

static int32_t translateCompact(STranslateContext* pCxt, SCompactDatabaseStmt* pStmt) {
  SCompactDbReq compactReq = {0};
  SName         name;
  int32_t       code = TSDB_CODE_SUCCESS;
  code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS != code) return code;

  (void)tNameGetFullDbName(&name, compactReq.db);
  code = translateCompactRange(pCxt, pStmt, &compactReq);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_COMPACT_DB, (FSerializeFunc)tSerializeSCompactDbReq, &compactReq);
  }
  tFreeSCompactDbReq(&compactReq);
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

static int32_t translateKillQuery(STranslateContext* pCxt, SKillQueryStmt* pStmt) {
  SKillQueryReq killReq = {0};
  strcpy(killReq.queryStrId, pStmt->queryId);
  return buildCmdMsg(pCxt, TDMT_MND_KILL_QUERY, (FSerializeFunc)tSerializeSKillQueryReq, &killReq);
}

static int32_t translateKillTransaction(STranslateContext* pCxt, SKillStmt* pStmt) {
  SKillTransReq killReq = {0};
  killReq.transId = pStmt->targetId;
  return buildCmdMsg(pCxt, TDMT_MND_KILL_TRANS, (FSerializeFunc)tSerializeSKillTransReq, &killReq);
}

static bool crossTableWithoutAggOper(SSelectStmt* pSelect) {
  return NULL == pSelect->pWindow && !pSelect->hasAggFuncs && !pSelect->hasIndefiniteRowsFunc &&
         !pSelect->hasInterpFunc && TSDB_SUPER_TABLE == ((SRealTableNode*)pSelect->pFromTable)->pMeta->tableType &&
         !hasTbnameFunction(pSelect->pPartitionByList);
}

static bool crossTableWithUdaf(SSelectStmt* pSelect) {
  return pSelect->hasUdaf && TSDB_SUPER_TABLE == ((SRealTableNode*)pSelect->pFromTable)->pMeta->tableType &&
         !hasTbnameFunction(pSelect->pPartitionByList);
}

static int32_t checkCreateStream(STranslateContext* pCxt, SCreateStreamStmt* pStmt) {
  if (NULL == pStmt->pQuery) {
    return TSDB_CODE_SUCCESS;
  }

  if (QUERY_NODE_SELECT_STMT != nodeType(pStmt->pQuery) || NULL == ((SSelectStmt*)pStmt->pQuery)->pFromTable ||
      QUERY_NODE_REAL_TABLE != nodeType(((SSelectStmt*)pStmt->pQuery)->pFromTable)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY, "Unsupported stream query");
  }

#ifdef TD_ENTERPRISE
  SRealTableNode* pRealTable = (SRealTableNode*)((SSelectStmt*)pStmt->pQuery)->pFromTable;
  SName           name = {0};
  STableMeta*     pMeta = NULL;
  int8_t          tableType = 0;
  toName(pCxt->pParseCxt->acctId, pRealTable->table.dbName, pRealTable->table.tableName, &name);
  int32_t code = getTargetMeta(pCxt, &name, &pMeta, true);
  if (NULL != pMeta) {
    tableType = pMeta->tableType;
    taosMemoryFree(pMeta);
  }
  if (TSDB_CODE_SUCCESS != code) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GET_META_ERROR, tstrerror(code));
  }
  if (TSDB_VIEW_TABLE == tableType) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY, "Unsupported stream query");
  }
#endif

  return TSDB_CODE_SUCCESS;
}

static void getSourceDatabase(SNode* pStmt, int32_t acctId, char* pDbFName) {
  SName name = {.type = TSDB_DB_NAME_T, .acctId = acctId};
  strcpy(name.dbname, ((SRealTableNode*)(((SSelectStmt*)pStmt)->pFromTable))->table.dbName);
  (void)tNameGetFullDbName(&name, pDbFName);
}

static void getStreamQueryFirstProjectAliasName(SHashObj* pUserAliasSet, char* aliasName, int32_t len) {
  if (NULL == taosHashGet(pUserAliasSet, "_wstart", strlen("_wstart"))) {
    snprintf(aliasName, len, "%s", "_wstart");
    return;
  }
  if (NULL == taosHashGet(pUserAliasSet, "ts", strlen("ts"))) {
    snprintf(aliasName, len, "%s", "ts");
    return;
  }
  do {
    taosRandStr(aliasName, len - 1);
    aliasName[len - 1] = '\0';
  } while (NULL != taosHashGet(pUserAliasSet, aliasName, strlen(aliasName)));
  return;
}

static int32_t setColumnDefNodePrimaryKey(SColumnDefNode* pNode, bool isPk) {
  int32_t code = 0;
  if (!pNode) return code;
  if (!isPk && !pNode->pOptions) return code;
  if (!pNode->pOptions) {
    code = nodesMakeNode(QUERY_NODE_COLUMN_OPTIONS, &pNode->pOptions);
  }
  if (TSDB_CODE_SUCCESS ==code) ((SColumnOptions*)pNode->pOptions)->bPrimaryKey = isPk;
  return code;
}

static int32_t addWstartTsToCreateStreamQueryImpl(STranslateContext* pCxt, SSelectStmt* pSelect,
                                                  SHashObj* pUserAliasSet, SNodeList* pCols, SCMCreateStreamReq* pReq) {
  SNode* pProj = nodesListGetNode(pSelect->pProjectionList, 0);
  if (NULL == pSelect->pWindow ||
      (QUERY_NODE_FUNCTION == nodeType(pProj) && 0 == strcmp("_wstart", ((SFunctionNode*)pProj)->functionName))) {
    return TSDB_CODE_SUCCESS;
  }
  SFunctionNode* pFunc = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  strcpy(pFunc->functionName, "_wstart");
  getStreamQueryFirstProjectAliasName(pUserAliasSet, pFunc->node.aliasName, sizeof(pFunc->node.aliasName));
  code = getFuncInfo(pCxt, pFunc);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListPushFront(pSelect->pProjectionList, (SNode*)pFunc);
  }

  if (TSDB_CODE_SUCCESS == code && STREAM_CREATE_STABLE_TRUE == pReq->createStb) {
    SColumnDefNode* pColDef = NULL;
    code = nodesMakeNode(QUERY_NODE_COLUMN_DEF, (SNode**)&pColDef);
    if (TSDB_CODE_SUCCESS == code) {
      strcpy(pColDef->colName, pFunc->node.aliasName);
      pColDef->dataType = pFunc->node.resType;
      pColDef->sma = true;
      code = setColumnDefNodePrimaryKey(pColDef, false);
    }
    if (TSDB_CODE_SUCCESS == code) code = nodesListPushFront(pCols, (SNode*)pColDef);
    if (TSDB_CODE_SUCCESS != code) nodesDestroyNode((SNode*)pColDef);
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pFunc);
  }
  return code;
}

static int32_t addWstartTsToCreateStreamQuery(STranslateContext* pCxt, SNode* pStmt, SNodeList* pCols,
                                              SCMCreateStreamReq* pReq) {
  SSelectStmt* pSelect = (SSelectStmt*)pStmt;
  SHashObj*    pUserAliasSet = NULL;
  int32_t      code = checkProjectAlias(pCxt, pSelect->pProjectionList, &pUserAliasSet);
  if (TSDB_CODE_SUCCESS == code) {
    code = addWstartTsToCreateStreamQueryImpl(pCxt, pSelect, pUserAliasSet, pCols, pReq);
  }
  taosHashCleanup(pUserAliasSet);
  return code;
}

static const char* getTagNameForCreateStreamTag(SNode* pTag) {
  if (QUERY_NODE_COLUMN_DEF == nodeType(pTag)) {
    return ((SColumnDefNode*)pTag)->colName;
  }
  return ((SColumnNode*)pTag)->colName;
}

static int32_t addTagsToCreateStreamQuery(STranslateContext* pCxt, SCreateStreamStmt* pStmt, SSelectStmt* pSelect) {
  if (NULL == pStmt->pTags) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pTag = NULL;
  FOREACH(pTag, pStmt->pTags) {
    bool   found = false;
    SNode* pPart = NULL;
    FOREACH(pPart, pSelect->pPartitionByList) {
      if (0 == strcmp(getTagNameForCreateStreamTag(pTag), ((SExprNode*)pPart)->userAlias)) {
        SNode* pNew = NULL;
        int32_t code = nodesCloneNode(pPart, &pNew);
        if (TSDB_CODE_SUCCESS != code) return code;
        if (TSDB_CODE_SUCCESS != (code = nodesListMakeStrictAppend(&pSelect->pTags, pNew))) {
          return code;
        }
        found = true;
        break;
      }
    }
    if (!found) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, getTagNameForCreateStreamTag(pTag));
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t createNullValue(SNode** ppNode) {
  SValueNode* pValue = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pValue);
  if (NULL == pValue) {
    return code;
  }
  pValue->isNull = true;
  pValue->node.resType.type = TSDB_DATA_TYPE_NULL;
  pValue->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_NULL].bytes;
  *ppNode = (SNode*)pValue;
  return code;
}

static int32_t addNullTagsForExistTable(STranslateContext* pCxt, STableMeta* pMeta, SSelectStmt* pSelect) {
  int32_t numOfTags = getNumOfTags(pMeta);
  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < numOfTags; ++i) {
    SNode* pNull = NULL;
    code = createNullValue(&pNull);
    if (TSDB_CODE_SUCCESS == code)
      code = nodesListMakeStrictAppend(&pSelect->pTags, pNull);
  }
  return code;
}

typedef struct SRewriteSubtableCxt {
  STranslateContext* pCxt;
  SNodeList*         pPartitionList;
} SRewriteSubtableCxt;

static EDealRes rewriteSubtable(SNode** pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(*pNode)) {
    SRewriteSubtableCxt* pCxt = pContext;
    bool                 found = false;
    SNode*               pPart = NULL;
    FOREACH(pPart, pCxt->pPartitionList) {
      if (0 == strcmp(((SColumnNode*)*pNode)->colName, ((SExprNode*)pPart)->userAlias)) {
        SNode* pNew = NULL;
        int32_t code = nodesCloneNode(pPart, &pNew);
        if (NULL == pNew) {
          pCxt->pCxt->errCode = code;
          return DEAL_RES_ERROR;
        }
        nodesDestroyNode(*pNode);
        *pNode = pNew;
        found = true;
        break;
      }
    }
    if (!found) {
      return generateDealNodeErrMsg(pCxt->pCxt, TSDB_CODE_PAR_INVALID_COLUMN, ((SColumnNode*)*pNode)->colName);
    }
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t addSubtableNameToCreateStreamQuery(STranslateContext* pCxt, SCreateStreamStmt* pStmt,
                                                  SSelectStmt* pSelect) {
  if (NULL == pStmt->pSubtable) {
    return TSDB_CODE_SUCCESS;
  }
  if (NULL == pSelect->pSubtable) {
    pSelect->pSubtable = NULL;
    int32_t code = nodesCloneNode(pStmt->pSubtable, &pSelect->pSubtable);
    if (NULL == pSelect->pSubtable) {
      return code;
    }
  }

  SRewriteSubtableCxt cxt = {.pCxt = pCxt, .pPartitionList = pSelect->pPartitionByList};
  nodesRewriteExpr(&pSelect->pSubtable, rewriteSubtable, &cxt);
  return pCxt->errCode;
}

static int32_t addNullTagsForCreateTable(STranslateContext* pCxt, SCreateStreamStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < LIST_LENGTH(pStmt->pTags); ++i) {
    SNode* pNull = NULL;
    code = createNullValue(&pNull);
    if (TSDB_CODE_SUCCESS == code)
      code = nodesListMakeStrictAppend(&((SSelectStmt*)pStmt->pQuery)->pTags, pNull);
  }
  return code;
}

static int32_t addNullTagsToCreateStreamQuery(STranslateContext* pCxt, STableMeta* pMeta, SCreateStreamStmt* pStmt) {
  if (NULL == pMeta) {
    return addNullTagsForCreateTable(pCxt, pStmt);
  }
  return addNullTagsForExistTable(pCxt, pMeta, (SSelectStmt*)pStmt->pQuery);
}

static int32_t addColDefNodeByProj(SNodeList** ppCols, const SNode* pProject, int8_t flags) {
  const SExprNode*      pExpr = (const SExprNode*)pProject;
  SColumnDefNode* pColDef = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN_DEF, (SNode**)&pColDef);
  if (TSDB_CODE_SUCCESS != code) return code;
  strcpy(pColDef->colName, pExpr->userAlias);
  pColDef->dataType = pExpr->resType;
  pColDef->sma = flags & COL_SMA_ON;
  code = setColumnDefNodePrimaryKey(pColDef, flags & COL_IS_KEY);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pColDef);
  } else {
    code = nodesListMakeStrictAppend(ppCols, (SNode*)pColDef);
  }
  return code;
}

static int32_t addColsToCreateStreamQuery(STranslateContext* pCxt, SCreateStreamStmt* pStmt, SCMCreateStreamReq* pReq) {
  if (STREAM_CREATE_STABLE_FALSE == pReq->createStb) {
    return TSDB_CODE_SUCCESS;
  }
  SSelectStmt* pSelect = (SSelectStmt*)pStmt->pQuery;
  SNode*       pProject = NULL;
  SNode*       pNode = NULL;
  if (0 != LIST_LENGTH(pStmt->pCols)) {
    if (LIST_LENGTH(pStmt->pCols) != LIST_LENGTH(pSelect->pProjectionList)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
    }
    FORBOTH(pNode, pStmt->pCols, pProject, pSelect->pProjectionList) {
      SExprNode*      pExpr = (SExprNode*)pProject;
      SColumnDefNode* pColDef = (SColumnDefNode*)pNode;
      pColDef->dataType = pExpr->resType;
    }
    return TSDB_CODE_SUCCESS;
  }

  FOREACH(pProject, pSelect->pProjectionList) {
    int32_t code = addColDefNodeByProj(&pStmt->pCols, pProject, 0);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t addSubtableInfoToCreateStreamQuery(STranslateContext* pCxt, STableMeta* pMeta,
                                                  SCreateStreamStmt* pStmt) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SSelectStmt* pSelect = (SSelectStmt*)pStmt->pQuery;
  if (NULL == pSelect->pPartitionByList) {
    code = addNullTagsToCreateStreamQuery(pCxt, pMeta, pStmt);
  } else {
    code = addTagsToCreateStreamQuery(pCxt, pStmt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addSubtableNameToCreateStreamQuery(pCxt, pStmt, pSelect);
  }

  return code;
}

#ifdef BUILD_NO_CALL
static bool isEventWindowQuery(SSelectStmt* pSelect) {
  return NULL != pSelect->pWindow && QUERY_NODE_EVENT_WINDOW == nodeType(pSelect->pWindow);
}
#endif

static bool hasJsonTypeProjection(SSelectStmt* pSelect) {
  SNode* pProj = NULL;
  FOREACH(pProj, pSelect->pProjectionList) {
    if (TSDB_DATA_TYPE_JSON == ((SExprNode*)pProj)->resType.type) {
      return true;
    }
  }
  return false;
}

static EDealRes hasColumnOrPseudoColumn(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    *(bool*)pContext = true;
    return DEAL_RES_END;
  }
  if (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsPseudoColumnFunc(((SFunctionNode*)pNode)->funcId)) {
    *(bool*)pContext = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t subtableExprHasColumnOrPseudoColumn(SNode* pNode) {
  bool hasColumn = false;
  nodesWalkExprPostOrder(pNode, hasColumnOrPseudoColumn, &hasColumn);
  return hasColumn;
}

static int32_t checkStreamQuery(STranslateContext* pCxt, SCreateStreamStmt* pStmt) {
  SSelectStmt* pSelect = (SSelectStmt*)pStmt->pQuery;
  if ((SRealTableNode*)pSelect->pFromTable && ((SRealTableNode*)pSelect->pFromTable)->pMeta &&
      TSDB_SUPER_TABLE == ((SRealTableNode*)pSelect->pFromTable)->pMeta->tableType &&
      !hasTbnameFunction(pSelect->pPartitionByList) && pSelect->pWindow != NULL &&
      pSelect->pWindow->type == QUERY_NODE_EVENT_WINDOW) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                   "Event window for stream on super table must patitioned by table name");
  }

  if (pSelect->pWindow != NULL && pSelect->pWindow->type == QUERY_NODE_EVENT_WINDOW &&
      (SRealTableNode*)pSelect->pFromTable && hasPkInTable(((SRealTableNode*)pSelect->pFromTable)->pMeta)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                   "Source table of Event window must not has primary key");
  }

  if (TSDB_DATA_TYPE_TIMESTAMP != ((SExprNode*)nodesListGetNode(pSelect->pProjectionList, 0))->resType.type ||
      !isTimeLineQuery(pStmt->pQuery) || crossTableWithoutAggOper(pSelect) || NULL != pSelect->pOrderByList ||
      crossTableWithUdaf(pSelect) || hasJsonTypeProjection(pSelect)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY, "Unsupported stream query");
  }
  if (NULL != pSelect->pSubtable && TSDB_DATA_TYPE_VARCHAR != ((SExprNode*)pSelect->pSubtable)->resType.type) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                   "SUBTABLE expression must be of VARCHAR type");
  }
  if (NULL != pSelect->pSubtable && 0 == LIST_LENGTH(pSelect->pPartitionByList) &&
      subtableExprHasColumnOrPseudoColumn(pSelect->pSubtable)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                   "SUBTABLE expression must not has column when no partition by clause");
  }

  if (NULL == pSelect->pWindow && STREAM_TRIGGER_AT_ONCE != pStmt->pOptions->triggerType) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                   "The trigger mode of non window query can only be AT_ONCE");
  }

  if (pSelect->pWindow != NULL && pSelect->pWindow->type == QUERY_NODE_COUNT_WINDOW) {
    if ((SRealTableNode*)pSelect->pFromTable && ((SRealTableNode*)pSelect->pFromTable)->pMeta &&
        TSDB_SUPER_TABLE == ((SRealTableNode*)pSelect->pFromTable)->pMeta->tableType &&
        !hasTbnameFunction(pSelect->pPartitionByList)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                     "Count window for stream on super table must patitioned by table name");
    }

    int64_t watermark = 0;
    if (pStmt->pOptions->pWatermark) {
      if (DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pStmt->pOptions->pWatermark)) {
        return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY, "Invalid watermark value.");
      }
      watermark = ((SValueNode*)pStmt->pOptions->pWatermark)->datum.i;
    }
    if (watermark <= 0) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                     "Watermark of Count window must exceed 0.");
    }

    if (pStmt->pOptions->ignoreExpired != 1) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                     "Ignore expired data of Count window must be 1.");
    }

    if ((SRealTableNode*)pSelect->pFromTable && hasPkInTable(((SRealTableNode*)pSelect->pFromTable)->pMeta)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                     "Source table of Count window must not has primary key");
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t adjustDataTypeOfProjections(STranslateContext* pCxt, const STableMeta* pMeta, SNodeList* pProjections,
                                           SNodeList** ppCols) {
  if (getNumOfColumns(pMeta) != LIST_LENGTH(pProjections)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM, "Illegal number of columns");
  }

  SSchema* pSchemas = getTableColumnSchema(pMeta);
  int32_t  index = 0;
  SNode*   pProj = NULL;
  FOREACH(pProj, pProjections) {
    SSchema*  pSchema = pSchemas + index++;
    SDataType dt = {.type = pSchema->type, .bytes = pSchema->bytes};
    if (!dataTypeEqual(&dt, &((SExprNode*)pProj)->resType)) {
      SNode*  pFunc = NULL;
      int32_t code = createCastFunc(pCxt, pProj, dt, &pFunc);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      REPLACE_NODE(pFunc);
    }
    SColumnDefNode* pColDef = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_COLUMN_DEF, (SNode**)&pColDef);
    if (TSDB_CODE_SUCCESS != code) return code;
    strcpy(pColDef->colName, pSchema->name);
    pColDef->dataType = dt;
    pColDef->sma = pSchema->flags & COL_SMA_ON;
    code = setColumnDefNodePrimaryKey(pColDef, pSchema->flags & COL_IS_KEY);
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyNode((SNode*)pColDef);
      return code;
    }
    code = nodesListMakeStrictAppend(ppCols, (SNode*)pColDef);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

typedef struct SProjColPos {
  int32_t colId;
  int8_t  flags;
  SNode*  pProj;
} SProjColPos;

static int32_t projColPosCompar(const void* l, const void* r) {
  if (((SProjColPos*)l)->colId < ((SProjColPos*)r)->colId) {
    return -1;
  }
  return ((SProjColPos*)l)->colId == ((SProjColPos*)r)->colId ? 0 : 1;
}

static void projColPosDelete(void* p) { nodesDestroyNode(((SProjColPos*)p)->pProj); }

static int32_t addProjToProjColPos(STranslateContext* pCxt, const SSchema* pSchema, SNode* pProj, SArray* pProjColPos) {
  SNode* pNewProj = NULL;
  int32_t code = nodesCloneNode(pProj, &pNewProj);
  if (NULL == pNewProj) {
    return code;
  }

  SDataType dt = {.type = pSchema->type, .bytes = pSchema->bytes};
  if (!dataTypeEqual(&dt, &((SExprNode*)pNewProj)->resType)) {
    SNode* pFunc = NULL;
    code = createCastFunc(pCxt, pNewProj, dt, &pFunc);
    strcpy(((SExprNode*)pFunc)->userAlias, ((SExprNode*)pNewProj)->userAlias);
    pNewProj = pFunc;
  }
  if (TSDB_CODE_SUCCESS == code) {
    SProjColPos pos = {.colId = pSchema->colId, .pProj = pNewProj, .flags = pSchema->flags};
    code = (NULL == taosArrayPush(pProjColPos, &pos) ? TSDB_CODE_OUT_OF_MEMORY : TSDB_CODE_SUCCESS);
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNewProj);
  }
  return code;
}

static int32_t setFillNullCols(SArray* pProjColPos, const STableMeta* pMeta, SCMCreateStreamReq* pReq) {
  int32_t numOfBoundCols = taosArrayGetSize(pProjColPos);
  pReq->fillNullCols = taosArrayInit(pMeta->tableInfo.numOfColumns - numOfBoundCols, sizeof(SColLocation));
  if (NULL == pReq->fillNullCols) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  const SSchema* pSchemas = getTableColumnSchema(pMeta);
  int32_t        indexOfBoundCols = 0;
  int32_t        code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < pMeta->tableInfo.numOfColumns; ++i) {
    const SSchema* pSchema = pSchemas + i;
    if (indexOfBoundCols < numOfBoundCols) {
      SProjColPos* pPos = taosArrayGet(pProjColPos, indexOfBoundCols);
      if (pSchema->colId == pPos->colId) {
        ++indexOfBoundCols;
        continue;
      }
    }
    SColLocation colLoc = {.colId = pSchema->colId, .slotId = i, .type = pSchema->type};
    if (NULL == taosArrayPush(pReq->fillNullCols, &colLoc)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      break;
    }
  }
  return code;
}

static int32_t adjustOrderOfProjections(STranslateContext* pCxt, SNodeList** ppCols, const STableMeta* pMeta,
                                        SNodeList** pProjections, SCMCreateStreamReq* pReq) {
  if (LIST_LENGTH((*ppCols)) != LIST_LENGTH(*pProjections)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM, "Illegal number of columns");
  }

  SArray* pProjColPos = taosArrayInit(LIST_LENGTH((*ppCols)), sizeof(SProjColPos));
  if (NULL == pProjColPos) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  bool    hasTsKey = false;
  bool    hasPrimaryKey = false;
  SNode*  pCol = NULL;
  SNode*  pProj = NULL;
  FORBOTH(pCol, (*ppCols), pProj, *pProjections) {
    const SSchema* pSchema = getNormalColSchema(pMeta, ((SColumnDefNode*)pCol)->colName);
    if (NULL == pSchema) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, ((SColumnDefNode*)pCol)->colName);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = addProjToProjColPos(pCxt, pSchema, pProj, pProjColPos);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    if (PRIMARYKEY_TIMESTAMP_COL_ID == pSchema->colId) {
      hasTsKey = true;
    }

    if (pSchema->flags & COL_IS_KEY) {
      hasPrimaryKey = true;
    }
  }

  if (TSDB_CODE_SUCCESS == code && !hasTsKey) {
    code = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM,
                                   "Primary timestamp column  of dest table can not be null");
  }

  if (TSDB_CODE_SUCCESS == code && !hasPrimaryKey && hasPkInTable(pMeta)) {
    code = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM,
                                   "Primary key column name must be defined in existed-stable field");
  }

  SNodeList* pNewProjections = NULL;
  SNodeList* pNewCols = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    taosArraySort(pProjColPos, projColPosCompar);
    int32_t num = taosArrayGetSize(pProjColPos);
    code = nodesMakeList(&pNewProjections);
    if (TSDB_CODE_SUCCESS != code) return code;
    code = nodesMakeList(&pNewCols);
    if (TSDB_CODE_SUCCESS != code) {
      code = code;
    }
    for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < num; ++i) {
      SProjColPos* pPos = taosArrayGet(pProjColPos, i);
      code = nodesListStrictAppend(pNewProjections, pPos->pProj);
      if (TSDB_CODE_SUCCESS == code) {
        code = addColDefNodeByProj(&pNewCols, pPos->pProj, pPos->flags);
      }
      pPos->pProj = NULL;
    }
  }

  if (TSDB_CODE_SUCCESS == code && pMeta->tableInfo.numOfColumns > LIST_LENGTH((*ppCols))) {
    code = setFillNullCols(pProjColPos, pMeta, pReq);
  }

  if (TSDB_CODE_SUCCESS == code) {
    taosArrayDestroy(pProjColPos);
    nodesDestroyList(*pProjections);
    nodesDestroyList(*ppCols);
    *pProjections = pNewProjections;
    *ppCols = pNewCols;
  } else {
    taosArrayDestroyEx(pProjColPos, projColPosDelete);
    nodesDestroyList(pNewProjections);
    nodesDestroyList(pNewCols);
  }

  return code;
}

static int32_t adjustProjectionsForExistTable(STranslateContext* pCxt, SCreateStreamStmt* pStmt,
                                              const STableMeta* pMeta, SCMCreateStreamReq* pReq) {
  SSelectStmt* pSelect = (SSelectStmt*)pStmt->pQuery;
  if (NULL == pStmt->pCols) {
    return adjustDataTypeOfProjections(pCxt, pMeta, pSelect->pProjectionList, &pStmt->pCols);
  }
  return adjustOrderOfProjections(pCxt, &pStmt->pCols, pMeta, &pSelect->pProjectionList, pReq);
}

static bool isGroupIdTagStream(const STableMeta* pMeta, SNodeList* pTags) {
  return (NULL == pTags && 1 == pMeta->tableInfo.numOfTags && TSDB_DATA_TYPE_UBIGINT == getTableTagSchema(pMeta)->type);
}

static int32_t adjustDataTypeOfTags(STranslateContext* pCxt, const STableMeta* pMeta, SNodeList* pTags) {
  if (isGroupIdTagStream(pMeta, pTags)) {
    return TSDB_CODE_SUCCESS;
  }

  if (getNumOfTags(pMeta) != LIST_LENGTH(pTags)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM, "Illegal number of tags");
  }

  SSchema* pSchemas = getTableTagSchema(pMeta);
  int32_t  index = 0;
  SNode*   pTag = NULL;
  FOREACH(pTag, pTags) {
    SSchema*  pSchema = pSchemas + index++;
    SDataType dt = {.type = pSchema->type, .bytes = pSchema->bytes};
    if (!dataTypeEqual(&dt, &((SExprNode*)pTag)->resType)) {
      SNode*  pFunc = NULL;
      int32_t code = createCastFunc(pCxt, pTag, dt, &pFunc);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      REPLACE_NODE(pFunc);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t adjustOrderOfTags(STranslateContext* pCxt, SNodeList* pTags, const STableMeta* pMeta,
                                 SNodeList** pTagExprs, SCMCreateStreamReq* pReq) {
  if (LIST_LENGTH(pTags) != LIST_LENGTH(*pTagExprs)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM, "Illegal number of tags");
  }

  SArray* pTagPos = taosArrayInit(LIST_LENGTH(pTags), sizeof(SProjColPos));
  if (NULL == pTagPos) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pTag = NULL;
  SNode*  pTagExpr = NULL;
  FORBOTH(pTag, pTags, pTagExpr, *pTagExprs) {
    const SSchema* pSchema = getTagSchema(pMeta, ((SColumnDefNode*)pTag)->colName);
    if (NULL == pSchema) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAG_NAME, ((SColumnDefNode*)pTag)->colName);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = addProjToProjColPos(pCxt, pSchema, pTagExpr, pTagPos);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  SNodeList* pNewTagExprs = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    taosArraySort(pTagPos, projColPosCompar);
    int32_t        indexOfBoundTags = 0;
    int32_t        numOfBoundTags = taosArrayGetSize(pTagPos);
    int32_t        numOfTags = getNumOfTags(pMeta);
    const SSchema* pTagsSchema = getTableTagSchema(pMeta);
    code = nodesMakeList(&pNewTagExprs);
    if (NULL == pNewTagExprs) {
      code = code;
    }
    for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < numOfTags; ++i) {
      const SSchema* pTagSchema = pTagsSchema + i;
      if (indexOfBoundTags < numOfBoundTags) {
        SProjColPos* pPos = taosArrayGet(pTagPos, indexOfBoundTags);
        if (pPos->colId == pTagSchema->colId) {
          ++indexOfBoundTags;
          code = nodesListStrictAppend(pNewTagExprs, pPos->pProj);
          pPos->pProj = NULL;
          continue;
        }
      }
      SNode* pNull = NULL;
      code = createNullValue(&pNull);
      if (TSDB_CODE_SUCCESS == code)
        code = nodesListStrictAppend(pNewTagExprs, pNull);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    taosArrayDestroy(pTagPos);
    nodesDestroyList(*pTagExprs);
    *pTagExprs = pNewTagExprs;
  } else {
    taosArrayDestroyEx(pTagPos, projColPosDelete);
    nodesDestroyList(pNewTagExprs);
  }

  return code;
}

static int32_t adjustTagsForExistTable(STranslateContext* pCxt, SCreateStreamStmt* pStmt, const STableMeta* pMeta,
                                       SCMCreateStreamReq* pReq) {
  SSelectStmt* pSelect = (SSelectStmt*)pStmt->pQuery;
  if (NULL == pSelect->pPartitionByList) {
    return TSDB_CODE_SUCCESS;
  }
  if (NULL == pStmt->pTags) {
    return adjustDataTypeOfTags(pCxt, pMeta, pSelect->pTags);
  }
  return adjustOrderOfTags(pCxt, pStmt->pTags, pMeta, &pSelect->pTags, pReq);
}

static int32_t adjustTagsForCreateTable(STranslateContext* pCxt, SCreateStreamStmt* pStmt, SCMCreateStreamReq* pReq) {
  SSelectStmt* pSelect = (SSelectStmt*)pStmt->pQuery;
  if (NULL == pSelect->pPartitionByList || NULL == pSelect->pTags) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pTagDef = NULL;
  SNode* pTagExpr = NULL;
  FORBOTH(pTagDef, pStmt->pTags, pTagExpr, pSelect->pTags) {
    SColumnDefNode* pDef = (SColumnDefNode*)pTagDef;
    if (!dataTypeEqual(&pDef->dataType, &((SExprNode*)pTagExpr)->resType)) {
      SNode*    pFunc = NULL;
      SDataType defType = pDef->dataType;
      defType.bytes = calcTypeBytes(defType);
      int32_t code = createCastFunc(pCxt, pTagExpr, defType, &pFunc);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      REPLACE_LIST2_NODE(pFunc);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t adjustTags(STranslateContext* pCxt, SCreateStreamStmt* pStmt, const STableMeta* pMeta,
                          SCMCreateStreamReq* pReq) {
  if (NULL == pMeta) {
    return adjustTagsForCreateTable(pCxt, pStmt, pReq);
  }
  return adjustTagsForExistTable(pCxt, pStmt, pMeta, pReq);
}

static bool isTagDef(SNodeList* pTags) {
  if (NULL == pTags) {
    return false;
  }
  SColumnDefNode* pColDef = (SColumnDefNode*)nodesListGetNode(pTags, 0);
  return TSDB_DATA_TYPE_NULL != pColDef->dataType.type;
}

static bool isTagBound(SNodeList* pTags) {
  if (NULL == pTags) {
    return false;
  }
  SColumnDefNode* pColDef = (SColumnDefNode*)nodesListGetNode(pTags, 0);
  return TSDB_DATA_TYPE_NULL == pColDef->dataType.type;
}

static int32_t translateStreamTargetTable(STranslateContext* pCxt, SCreateStreamStmt* pStmt, SCMCreateStreamReq* pReq,
                                          STableMeta** pMeta) {
  int32_t code = getTableMeta(pCxt, pStmt->targetDbName, pStmt->targetTabName, pMeta);
  if (TSDB_CODE_PAR_TABLE_NOT_EXIST == code) {
    if (isTagBound(pStmt->pTags)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TABLE_NOT_EXIST, pStmt->targetTabName);
    }
    pReq->createStb = STREAM_CREATE_STABLE_TRUE;
    pReq->targetStbUid = 0;
    return TSDB_CODE_SUCCESS;
  } else if (TSDB_CODE_SUCCESS == code) {
    if (isTagDef(pStmt->pTags)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY, "Table already exist: %s",
                                     pStmt->targetTabName);
    }
    if (TSDB_SUPER_TABLE != (*pMeta)->tableType) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                     "Stream can only be written to super table");
    }
    pReq->createStb = STREAM_CREATE_STABLE_FALSE;
    pReq->targetStbUid = (*pMeta)->suid;
  }
  return code;
}

static int32_t createLastTsSelectStmt(char* pDb, const char* pTable, const char* pkColName, SNode** pQuery) {
  SColumnNode* col = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&col);
  if (NULL == col) {
    return code;
  }

  tstrncpy(col->tableAlias, pTable, tListLen(col->tableAlias));
  tstrncpy(col->colName, pkColName, tListLen(col->colName));
  SNodeList* pParameterList = NULL;
  code = nodesMakeList(&pParameterList);
  if (NULL == pParameterList) {
    nodesDestroyNode((SNode*)col);
    return code;
  }

  code = nodesListStrictAppend(pParameterList, (SNode*)col);
  if (code) {
    nodesDestroyList(pParameterList);
    return code;
  }

  SNode* pFunc = NULL;
  code = createFunction("last", pParameterList, (SFunctionNode**)&pFunc);
  if (code) {
    nodesDestroyList(pParameterList);
    return terrno;
  }

  SNodeList* pProjectionList = NULL;
  code = nodesMakeList(&pProjectionList);
  if (NULL == pProjectionList) {
    nodesDestroyNode(pFunc);
    return code;
  }

  code = nodesListStrictAppend(pProjectionList, pFunc);
  if (code) {
    nodesDestroyList(pProjectionList);
    return code;
  }

  SFunctionNode* pFunc1 = NULL;
  code = createFunction("_vgid", NULL, &pFunc1);
  if (code) {
    nodesDestroyList(pProjectionList);
    return terrno;
  }

  snprintf(pFunc1->node.aliasName, sizeof(pFunc1->node.aliasName), "%s.%p", pFunc1->functionName, pFunc1);
  code = nodesListStrictAppend(pProjectionList, (SNode*)pFunc1);
  if (code) {
    nodesDestroyList(pProjectionList);
    return code;
  }

  SFunctionNode* pFunc2 = NULL;
  code = createFunction("_vgver", NULL, &pFunc2);
  if (code) {
    nodesDestroyList(pProjectionList);
    return terrno;
  }

  snprintf(pFunc2->node.aliasName, sizeof(pFunc2->node.aliasName), "%s.%p", pFunc2->functionName, pFunc2);
  code = nodesListStrictAppend(pProjectionList, (SNode*)pFunc2);
  if (code) {
    nodesDestroyList(pProjectionList);
    return code;
  }

  code = createSimpleSelectStmtFromProjList(pDb, pTable, pProjectionList, (SSelectStmt**)pQuery);
  if (code) {
    nodesDestroyList(pProjectionList);
    return code;
  }

  SSelectStmt** pSelect1 = (SSelectStmt**)pQuery;
  code = nodesMakeList(&(*pSelect1)->pGroupByList);
  if (NULL == (*pSelect1)->pGroupByList) {
    return code;
  }

  SGroupingSetNode* pNode1 = NULL;
  code = nodesMakeNode(QUERY_NODE_GROUPING_SET, (SNode**)&pNode1);
  if (NULL == pNode1) {
    return code;
  }

  pNode1->groupingSetType = GP_TYPE_NORMAL;
  code = nodesMakeList(&pNode1->pParameterList);
  if (NULL == pNode1->pParameterList) {
    nodesDestroyNode((SNode*)pNode1);
    return code;
  }

  SNode* pNew = NULL;
  code = nodesCloneNode((SNode*)pFunc1, &pNew);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pNode1);
    return code;
  }
  code = nodesListStrictAppend(pNode1->pParameterList, pNew);
  if (code) {
    nodesDestroyNode((SNode*)pNode1);
    return code;
  }

  code = nodesListAppend((*pSelect1)->pGroupByList, (SNode*)pNode1);
  if (code) {
    return code;
  }

  SGroupingSetNode* pNode2 = NULL;
  code = nodesMakeNode(QUERY_NODE_GROUPING_SET, (SNode**)&pNode2);
  if (NULL == pNode2) {
    return code;
  }

  pNode2->groupingSetType = GP_TYPE_NORMAL;
  code = nodesMakeList(&pNode2->pParameterList);
  if (NULL == pNode2->pParameterList) {
    nodesDestroyNode((SNode*)pNode2);
    return code;
  }

  pNew = NULL;
  code = nodesCloneNode((SNode*)pFunc2, &pNew);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pNode2);
    return code;
  }
  code = nodesListStrictAppend(pNode2->pParameterList, pNew);
  if (code) {
    nodesDestroyNode((SNode*)pNode2);
    return code;
  }

  return nodesListStrictAppend((*pSelect1)->pGroupByList, (SNode*)pNode2);
}

static int32_t checkAndAdjStreamDestTableSchema(STranslateContext* pCxt, SCreateStreamStmt* pStmt,
                                                SCMCreateStreamReq* pReq) {
  SSelectStmt*    pSelect = (SSelectStmt*)pStmt->pQuery;
  SNode*          pNode = nodesListGetNode(pStmt->pCols, 0);
  SColumnDefNode* pCol = (SColumnDefNode*)pNode;
  if (pCol && pCol->dataType.type != TSDB_DATA_TYPE_TIMESTAMP) {
    pCol->dataType = (SDataType){.type = TSDB_DATA_TYPE_TIMESTAMP,
                                 .precision = 0,
                                 .scale = 0,
                                 .bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes};
  }
  int32_t code = checkTableSchemaImpl(pCxt, pStmt->pTags, pStmt->pCols, NULL);
  if (TSDB_CODE_SUCCESS == code && NULL == pSelect->pWindow &&
      ((SRealTableNode*)pSelect->pFromTable && hasPkInTable(((SRealTableNode*)pSelect->pFromTable)->pMeta))) {
    if (1 >= LIST_LENGTH(pStmt->pCols) || 1 >= LIST_LENGTH(pSelect->pProjectionList)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY);
    }

    SNode* pProj = nodesListGetNode(pSelect->pProjectionList, 1);
    if (QUERY_NODE_COLUMN != nodeType(pProj) ||
        0 != strcmp(((SColumnNode*)pProj)->colName, ((SRealTableNode*)pSelect->pFromTable)->pMeta->schema[1].name)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                     "Source table has primary key, result must has primary key");
    }

    pNode = nodesListGetNode(pStmt->pCols, 1);
    pCol = (SColumnDefNode*)pNode;
    if (STREAM_CREATE_STABLE_TRUE == pReq->createStb) {
      int32_t code = setColumnDefNodePrimaryKey(pCol, true);
      if (TSDB_CODE_SUCCESS != code) return code;
    }
    if (!pCol->pOptions || !((SColumnOptions*)pCol->pOptions)->bPrimaryKey) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                     "Source table has primary key, dest table must has primary key");
    }
  }
  return code;
}

static int32_t buildCreateStreamQuery(STranslateContext* pCxt, SCreateStreamStmt* pStmt, SCMCreateStreamReq* pReq) {
  pCxt->createStream = true;
  STableMeta* pMeta = NULL;
  int32_t     code = translateStreamTargetTable(pCxt, pStmt, pReq, &pMeta);
  if (TSDB_CODE_SUCCESS == code) {
    code = addSubtableInfoToCreateStreamQuery(pCxt, pMeta, pStmt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateQuery(pCxt, pStmt->pQuery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addColsToCreateStreamQuery(pCxt, pStmt, pReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addWstartTsToCreateStreamQuery(pCxt, pStmt->pQuery, pStmt->pCols, pReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkStreamQuery(pCxt, pStmt);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pMeta) {
    code = adjustProjectionsForExistTable(pCxt, pStmt, pMeta, pReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = adjustTags(pCxt, pStmt, pMeta, pReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkAndAdjStreamDestTableSchema(pCxt, pStmt, pReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    getSourceDatabase(pStmt->pQuery, pCxt->pParseCxt->acctId, pReq->sourceDB);
    code = nodesNodeToString(pStmt->pQuery, false, &pReq->ast, NULL);
  }
  if (TSDB_CODE_SUCCESS == code && pStmt->pOptions->fillHistory) {
    SRealTableNode* pTable = (SRealTableNode*)(((SSelectStmt*)pStmt->pQuery)->pFromTable);
    code = createLastTsSelectStmt(pTable->table.dbName, pTable->table.tableName, pTable->pMeta->schema[0].name,
                                  &pStmt->pPrevQuery);
    /*
        if (TSDB_CODE_SUCCESS == code) {
          STranslateContext cxt = {0};
          int32_t code = initTranslateContext(pCxt->pParseCxt, pCxt->pMetaCache, &cxt);
          code = translateQuery(&cxt, pStmt->pPrevQuery);
          destroyTranslateContext(&cxt);
        }
    */
  }
  taosMemoryFree(pMeta);
  return code;
}

static int32_t translateStreamOptions(STranslateContext* pCxt, SCreateStreamStmt* pStmt) {
  pCxt->pCurrStmt = (SNode*)pStmt;
  SStreamOptions* pOptions = pStmt->pOptions;
  if ((NULL != pOptions->pWatermark && (DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pOptions->pWatermark))) ||
      (NULL != pOptions->pDeleteMark && (DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pOptions->pDeleteMark))) ||
      (NULL != pOptions->pDelay && (DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pOptions->pDelay)))) {
    return pCxt->errCode;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildCreateStreamReq(STranslateContext* pCxt, SCreateStreamStmt* pStmt, SCMCreateStreamReq* pReq) {
  pReq->igExists = pStmt->ignoreExists;

  SName   name;
  int32_t code = TSDB_CODE_SUCCESS;
  code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->streamName, strlen(pStmt->streamName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, pReq->name);

  if ('\0' != pStmt->targetTabName[0]) {
    strcpy(name.dbname, pStmt->targetDbName);
    strcpy(name.tname, pStmt->targetTabName);
    code = tNameExtractFullName(&name, pReq->targetStbFullName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateStreamQuery(pCxt, pStmt, pReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pReq->sql = taosStrdup(pCxt->pParseCxt->pSql);
    if (NULL == pReq->sql) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = translateStreamOptions(pCxt, pStmt);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pReq->triggerType = pStmt->pOptions->triggerType;
    pReq->maxDelay = (NULL != pStmt->pOptions->pDelay ? ((SValueNode*)pStmt->pOptions->pDelay)->datum.i : 0);
    pReq->watermark = (NULL != pStmt->pOptions->pWatermark ? ((SValueNode*)pStmt->pOptions->pWatermark)->datum.i : 0);
    pReq->deleteMark =
        (NULL != pStmt->pOptions->pDeleteMark ? ((SValueNode*)pStmt->pOptions->pDeleteMark)->datum.i : 0);
    pReq->fillHistory = pStmt->pOptions->fillHistory;
    pReq->igExpired = pStmt->pOptions->ignoreExpired;
    pReq->igUpdate = pStmt->pOptions->ignoreUpdate;
    if (pReq->createStb) {
      pReq->numOfTags = LIST_LENGTH(pStmt->pTags);
      code = tagDefNodeToField(pStmt->pTags, &pReq->pTags, true);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = columnDefNodeToField(pStmt->pCols, &pReq->pCols, false);
    }
  }

  return code;
}

static int32_t translateCreateStream(STranslateContext* pCxt, SCreateStreamStmt* pStmt) {
  SCMCreateStreamReq createReq = {0};

  int32_t code = checkCreateStream(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateStreamReq(pCxt, pStmt, &createReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL == pStmt->pPrevQuery) {
      code = buildCmdMsg(pCxt, TDMT_MND_CREATE_STREAM, (FSerializeFunc)tSerializeSCMCreateStreamReq, &createReq);
    } else {
      pStmt->pReq = taosMemoryMalloc(sizeof(createReq));
      if (NULL == pStmt->pReq) {
        code = TSDB_CODE_OUT_OF_MEMORY;
      } else {
        memcpy(pStmt->pReq, &createReq, sizeof(createReq));
        memset(&createReq, 0, sizeof(createReq));
        TSWAP(pCxt->pPrevRoot, pStmt->pPrevQuery);
      }
    }
  }

  tFreeSCMCreateStreamReq(&createReq);
  return code;
}

static int32_t buildIntervalForCreateStream(SCreateStreamStmt* pStmt, SInterval* pInterval) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (QUERY_NODE_SELECT_STMT != nodeType(pStmt->pQuery)) {
    return code;
  }
  SSelectStmt* pSelect = (SSelectStmt*)pStmt->pQuery;
  if (NULL == pSelect->pWindow || QUERY_NODE_INTERVAL_WINDOW != nodeType(pSelect->pWindow)) {
    return code;
  }

  SIntervalWindowNode* pWindow = (SIntervalWindowNode*)pSelect->pWindow;
  pInterval->interval = ((SValueNode*)pWindow->pInterval)->datum.i;
  pInterval->intervalUnit = ((SValueNode*)pWindow->pInterval)->unit;
  pInterval->offset = (NULL != pWindow->pOffset ? ((SValueNode*)pWindow->pOffset)->datum.i : 0);
  pInterval->sliding = (NULL != pWindow->pSliding ? ((SValueNode*)pWindow->pSliding)->datum.i : pInterval->interval);
  pInterval->slidingUnit =
      (NULL != pWindow->pSliding ? ((SValueNode*)pWindow->pSliding)->unit : pInterval->intervalUnit);
  pInterval->precision = ((SColumnNode*)pWindow->pCol)->node.resType.precision;

  return code;
}

// ts, vgroup_id, vgroup_version
static int32_t createStreamReqVersionInfo(SSDataBlock* pBlock, SArray** pArray, int64_t* lastTs, SInterval* pInterval) {
  *pArray = taosArrayInit(pBlock->info.rows, sizeof(SVgroupVer));
  if (*pArray == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (pBlock->info.rows > 0) {
    *lastTs = pBlock->info.window.ekey;
    SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 1);
    SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, 2);

    for (int32_t i = 0; i < pBlock->info.rows; ++i) {
      SVgroupVer v = {.vgId = *(int32_t*)colDataGetData(pCol1, i), .ver = *(int64_t*)colDataGetData(pCol2, i)};
      if((taosArrayPush(*pArray, &v)) == NULL) {
        taosArrayDestroy(*pArray);
        return terrno;
      }
    }
  } else {
    int32_t precision = (pInterval->interval > 0) ? pInterval->precision : TSDB_TIME_PRECISION_MILLI;
    *lastTs = taosGetTimestamp(precision);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t translatePostCreateStream(SParseContext* pParseCxt, SQuery* pQuery, SSDataBlock* pBlock) {
  SCreateStreamStmt* pStmt = (SCreateStreamStmt*)pQuery->pRoot;
  STranslateContext  cxt = {0};
  SInterval          interval = {0};
  int64_t            lastTs = 0;

  int32_t code = initTranslateContext(pParseCxt, NULL, &cxt);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildIntervalForCreateStream(pStmt, &interval);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = createStreamReqVersionInfo(pBlock, &pStmt->pReq->pVgroupVerList, &lastTs, &interval);
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (interval.interval > 0) {
      pStmt->pReq->lastTs = taosTimeAdd(taosTimeTruncate(lastTs, &interval), interval.interval, interval.intervalUnit,
                                        interval.precision);
    } else {
      pStmt->pReq->lastTs = lastTs + 1;  // start key of the next time window
    }
    code = buildCmdMsg(&cxt, TDMT_MND_CREATE_STREAM, (FSerializeFunc)tSerializeSCMCreateStreamReq, pStmt->pReq);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setQuery(&cxt, pQuery);
  }

  int32_t tmpCode = setRefreshMeta(&cxt, pQuery);
  if (TSDB_CODE_SUCCESS == code) code = tmpCode;
  destroyTranslateContext(&cxt);

  tFreeSCMCreateStreamReq(pStmt->pReq);
  taosMemoryFreeClear(pStmt->pReq);

  return code;
}

static int32_t translateDropStream(STranslateContext* pCxt, SDropStreamStmt* pStmt) {
  SMDropStreamReq dropReq = {0};
  SName           name;
  int32_t code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->streamName, strlen(pStmt->streamName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, dropReq.name);
  dropReq.igNotExists = pStmt->ignoreNotExists;
  code = buildCmdMsg(pCxt, TDMT_MND_DROP_STREAM, (FSerializeFunc)tSerializeSMDropStreamReq, &dropReq);
  tFreeMDropStreamReq(&dropReq);
  return code;
}

static int32_t translatePauseStream(STranslateContext* pCxt, SPauseStreamStmt* pStmt) {
  SMPauseStreamReq req = {0};
  SName            name;
  int32_t code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->streamName, strlen(pStmt->streamName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, req.name);
  req.igNotExists = pStmt->ignoreNotExists;
  return buildCmdMsg(pCxt, TDMT_MND_PAUSE_STREAM, (FSerializeFunc)tSerializeSMPauseStreamReq, &req);
}

static int32_t translateResumeStream(STranslateContext* pCxt, SResumeStreamStmt* pStmt) {
  SMResumeStreamReq req = {0};
  SName             name;
  int32_t code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->streamName, strlen(pStmt->streamName));
  if (TSDB_CODE_SUCCESS != code) return code;
  (void)tNameGetFullDbName(&name, req.name);
  req.igNotExists = pStmt->ignoreNotExists;
  req.igUntreated = pStmt->ignoreUntreated;
  return buildCmdMsg(pCxt, TDMT_MND_RESUME_STREAM, (FSerializeFunc)tSerializeSMResumeStreamReq, &req);
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
    strncpy(pStmt->createReq.name, pStmt->viewName, sizeof(pStmt->createReq.name) - 1);
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
  int32_t code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  if (TSDB_CODE_SUCCESS == code) {
    (void)tNameGetFullDbName(&name, dropReq.dbFName);
    strncpy(dropReq.name, pStmt->viewName, sizeof(dropReq.name) - 1);
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
    qError("failed to close file: %s in %s:%d, err: %s", pName, __func__, __LINE__, tstrerror(code));
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
  strcpy(req.name, pStmt->funcName);
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
  strcpy(req.name, pStmt->funcName);
  req.igNotExists = pStmt->ignoreNotExists;
  return buildCmdMsg(pCxt, TDMT_MND_DROP_FUNC, (FSerializeFunc)tSerializeSDropFuncReq, &req);
}

static int32_t createRealTableForGrantTable(SGrantStmt* pStmt, SRealTableNode** pTable) {
  SRealTableNode* pRealTable = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)&pRealTable);
  if (NULL == pRealTable) {
    return code;
  }
  strcpy(pRealTable->table.dbName, pStmt->objName);
  strcpy(pRealTable->table.tableName, pStmt->tabName);
  strcpy(pRealTable->table.tableAlias, pStmt->tabName);
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

    if (TSDB_SUPER_TABLE != pTable->pMeta->tableType && TSDB_NORMAL_TABLE != pTable->pMeta->tableType) {
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

  strcpy(req.user, pStmt->userName);
  sprintf(req.objname, "%d.%s", pCxt->pParseCxt->acctId, pStmt->objName);
  sprintf(req.tabName, "%s", pStmt->tabName);
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

  strcpy(req.user, pStmt->userName);
  sprintf(req.objname, "%d.%s", pCxt->pParseCxt->acctId, pStmt->objName);
  sprintf(req.tabName, "%s", pStmt->tabName);
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

static int32_t translateBalanceVgroupLeader(STranslateContext* pCxt, SBalanceVgroupLeaderStmt* pStmt) {
  SBalanceVgroupLeaderReq req = {0};
  req.vgId = pStmt->vgId;
  strcpy(req.db, pStmt->dbName);
  int32_t code =
      buildCmdMsg(pCxt, TDMT_MND_BALANCE_VGROUP_LEADER, (FSerializeFunc)tSerializeSBalanceVgroupLeaderReq, &req);
  tFreeSBalanceVgroupLeaderReq(&req);
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
  SSplitVgroupReq req = {.vgId = pStmt->vgId};
  return buildCmdMsg(pCxt, TDMT_MND_SPLIT_VGROUP, (FSerializeFunc)tSerializeSSplitVgroupReq, &req);
}

static int32_t translateShowVariables(STranslateContext* pCxt, SShowStmt* pStmt) {
  SShowVariablesReq req = {0};
  return buildCmdMsg(pCxt, TDMT_MND_SHOW_VARIABLES, (FSerializeFunc)tSerializeSShowVariablesReq, &req);
}

static int32_t translateShowCreateDatabase(STranslateContext* pCxt, SShowCreateDatabaseStmt* pStmt) {
  pStmt->pCfg = taosMemoryCalloc(1, sizeof(SDbCfgInfo));
  if (NULL == pStmt->pCfg) {
    return terrno;
  }

  SName name;
  int32_t code = tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  (void)tNameGetFullDbName(&name, pStmt->dbFName);
  if (TSDB_CODE_SUCCESS == code)
    return getDBCfg(pCxt, pStmt->dbName, (SDbCfgInfo*)pStmt->pCfg);
  return code;
}

static int32_t translateShowCreateTable(STranslateContext* pCxt, SShowCreateTableStmt* pStmt) {
  pStmt->pDbCfg = taosMemoryCalloc(1, sizeof(SDbCfgInfo));
  if (NULL == pStmt->pDbCfg) {
    return terrno;
  }
  int32_t code = getDBCfg(pCxt, pStmt->dbName, (SDbCfgInfo*)pStmt->pDbCfg);
  if (TSDB_CODE_SUCCESS == code) {
    SName name = {0};
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);
    code = getTableCfg(pCxt, &name, (STableCfg**)&pStmt->pTableCfg);
  }
  return code;
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

static int32_t createColumnNodeWithName(const char* name, SNode** ppCol) {
  SColumnNode* pCol = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
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

static int32_t buildTSMAAstStreamSubTable(SCreateTSMAStmt* pStmt, SMCreateSmaReq* pReq, const SNode* pTbname,
                                          SNode** pSubTable) {
  int32_t        code = 0;
  SFunctionNode* pMd5Func = NULL;
  code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pMd5Func);
  if (TSDB_CODE_SUCCESS != code) goto _end;
  SFunctionNode* pConcatFunc = NULL;
  code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pConcatFunc);
  if (TSDB_CODE_SUCCESS != code) goto _end;
  SValueNode*    pVal = NULL;
  code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);
  if (TSDB_CODE_SUCCESS != code) goto _end;

  sprintf(pMd5Func->functionName, "%s", "md5");
  sprintf(pConcatFunc->functionName, "%s", "concat");
  pVal->literal = taosMemoryMalloc(TSDB_TABLE_FNAME_LEN + 1);
  if (!pVal->literal) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }
  sprintf(pVal->literal, "%s_", pReq->name);
  pVal->node.resType.type = TSDB_DATA_TYPE_VARCHAR;
  pVal->node.resType.bytes = strlen(pVal->literal);
  code = nodesListMakeAppend(&pConcatFunc->pParameterList, (SNode*)pVal);
  if (code != TSDB_CODE_SUCCESS) goto _end;
  pVal = NULL;

  // not recursive tsma, md5(concat('1.test.tsma1_', tbname))
  // recursive tsma, md5(concat('1.test.tsma1_', `tbname`)), `tbname` is the last tag
  SNode* pNew = NULL;
  code = nodesCloneNode(pTbname, &pNew);
  if (TSDB_CODE_SUCCESS != code) goto _end;
  code = nodesListStrictAppend(pConcatFunc->pParameterList, pNew);
  if (code != TSDB_CODE_SUCCESS) goto _end;

  code = nodesListMakeAppend(&pMd5Func->pParameterList, (SNode*)pConcatFunc);
  if (code != TSDB_CODE_SUCCESS) goto _end;
  pConcatFunc = NULL;
  *pSubTable = (SNode*)pMd5Func;

_end:
  if (code) {
    if (pMd5Func) nodesDestroyNode((SNode*)pMd5Func);
    if (pConcatFunc) nodesDestroyNode((SNode*)pConcatFunc);
    if (pVal) nodesDestroyNode((SNode*)pVal);
  }
  return code;
}

static int32_t buildTSMAAst(STranslateContext* pCxt, SCreateTSMAStmt* pStmt, SMCreateSmaReq* pReq, const char* tbName,
                            int32_t numOfTags, const SSchema* pTags) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SSampleAstInfo info = {0};
  info.createSmaIndex = true;
  info.pDbName = pStmt->dbName;
  info.pTableName = tbName;
  code = nodesCloneList(pStmt->pOptions->pFuncs, &info.pFuncs);
  if (TSDB_CODE_SUCCESS == code)
    code = nodesCloneNode(pStmt->pOptions->pInterval, &info.pInterval);

  SFunctionNode* pTbnameFunc = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    // append partition by tbname
    code = createTbnameFunction(&pTbnameFunc);
    if (pTbnameFunc) {
      sprintf(pTbnameFunc->node.userAlias, "tbname");
      code = nodesListMakeStrictAppend(&info.pPartitionByList, (SNode*)pTbnameFunc);
    } else {
      code = code;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    int32_t partitionTagNum = pStmt->pOptions->recursiveTsma ? numOfTags - 1 : numOfTags;
    // append partition by tags
    SNode* pTagCol = NULL;
    for (int32_t idx = 0; idx < partitionTagNum; ++idx) {
      pTagCol = NULL;
      code = createColumnNodeWithName(pTags[idx].name, &pTagCol);
      if (!pTagCol) {
        break;
      }
      code = nodesListAppend(info.pPartitionByList, pTagCol);
      if (TSDB_CODE_SUCCESS == code) {
        SNode*pNew = NULL;
        code = nodesCloneNode(pTagCol, &pNew);
        if (TSDB_CODE_SUCCESS == code)
          code = nodesListMakeStrictAppend(&info.pTags, pNew);
      }
    }

    // sub table
    if (code == TSDB_CODE_SUCCESS) {
      SFunctionNode* pSubTable = NULL;
      pTagCol = NULL;
      if (pTags && numOfTags > 0) {
        code = createColumnNodeWithName(pTags[numOfTags - 1].name, &pTagCol);
      }
      if (code == TSDB_CODE_SUCCESS) {
        code = buildTSMAAstStreamSubTable(pStmt, pReq, pStmt->pOptions->recursiveTsma ? pTagCol : (SNode*)pTbnameFunc,
                                          (SNode**)&pSubTable);
        info.pSubTable = (SNode*)pSubTable;
      }
      if (code == TSDB_CODE_SUCCESS) {
        if (pStmt->pOptions->recursiveTsma) {
          code = nodesListMakeStrictAppend(&info.pTags, pTagCol);
        } else {
          SNode* pNew = NULL;
          code = nodesCloneNode((SNode*)pTbnameFunc, &pNew);
          if (TSDB_CODE_SUCCESS == code) code = nodesListMakeStrictAppend(&info.pTags, pNew);
        }
      }
    }
  }

  if (code == TSDB_CODE_SUCCESS && !pStmt->pOptions->recursiveTsma) code = fmCreateStateFuncs(info.pFuncs);

  if (code == TSDB_CODE_SUCCESS) {
    int32_t pProjectionTotalLen = 0;
    code = buildSampleAst(pCxt, &info, &pReq->ast, &pReq->astLen, &pReq->expr, &pReq->exprLen, &pProjectionTotalLen);
    if (code == TSDB_CODE_SUCCESS && pProjectionTotalLen > TSDB_MAX_BYTES_PER_ROW) {
      code = TSDB_CODE_PAR_INVALID_ROW_LENGTH;
    }
  }
  clearSampleAstInfo(&info);
  return code;
}

static int32_t createColumnBySchema(const SSchema* pSchema, SColumnNode** ppCol) {
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)ppCol);
  if (!*ppCol) return code;

  (*ppCol)->colId = pSchema->colId;
  (*ppCol)->node.resType.type = pSchema->type;
  (*ppCol)->node.resType.bytes = pSchema->bytes;
  strcpy((*ppCol)->colName, pSchema->name);
  return TSDB_CODE_SUCCESS;
}

static int32_t rewriteTSMAFuncs(STranslateContext* pCxt, SCreateTSMAStmt* pStmt, int32_t columnNum,
                                const SSchema* pCols) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SNode*         pNode;
  SFunctionNode* pFunc = NULL;
  SColumnNode*   pCol = NULL;
  if (pStmt->pOptions->recursiveTsma) {
    int32_t i = 0;
    FOREACH(pNode, pStmt->pOptions->pFuncs) {
      // rewrite all func parameters with tsma dest tb cols
      pFunc = (SFunctionNode*)pNode;
      const SSchema* pSchema = pCols + i;
      code = createColumnBySchema(pSchema, &pCol);
      if (TSDB_CODE_SUCCESS != code) break;
      (void)nodesListErase(pFunc->pParameterList, pFunc->pParameterList->pHead);
      code = nodesListPushFront(pFunc->pParameterList, (SNode*)pCol);
      if (TSDB_CODE_SUCCESS != code) break;
      snprintf(pFunc->node.userAlias, TSDB_COL_NAME_LEN, "%s", pSchema->name);
      // for first or last, the second param will be pk ts col, here we should remove it
      if (fmIsImplicitTsFunc(pFunc->funcId) && LIST_LENGTH(pFunc->pParameterList) == 2) {
        (void)nodesListErase(pFunc->pParameterList, pFunc->pParameterList->pTail);
      }
      ++i;
    }
    // recursive tsma, create func list from base tsma
    if (TSDB_CODE_SUCCESS == code) {
      code = fmCreateStateMergeFuncs(pStmt->pOptions->pFuncs);
    }
  } else {
    FOREACH(pNode, pStmt->pOptions->pFuncs) {
      pFunc = (SFunctionNode*)pNode;
      if (!pFunc->pParameterList || LIST_LENGTH(pFunc->pParameterList) != 1 ||
          nodeType(pFunc->pParameterList->pHead->pNode) != QUERY_NODE_COLUMN) {
        code = TSDB_CODE_TSMA_INVALID_FUNC_PARAM;
        break;
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
        code = TSDB_CODE_TSMA_INVALID_FUNC_PARAM;
        break;
      }
      code = fmGetFuncInfo(pFunc, NULL, 0);
      if (TSDB_CODE_SUCCESS != code) break;
      if (!fmIsTSMASupportedFunc(pFunc->funcId)) {
        code = TSDB_CODE_TSMA_UNSUPPORTED_FUNC;
        break;
      }

      pCol = (SColumnNode*)pFunc->pParameterList->pHead->pNode;
      snprintf(pFunc->node.userAlias, TSDB_COL_NAME_LEN, "%s(%s)", pFunc->functionName, pCol->colName);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesSortList(&pStmt->pOptions->pFuncs, compareTsmaFuncWithFuncAndColId);
    deduplicateTsmaFuncs(pStmt->pOptions->pFuncs);
  }
  return code;
}

static int32_t buildCreateTSMAReq(STranslateContext* pCxt, SCreateTSMAStmt* pStmt, SMCreateSmaReq* pReq,
                                  SName* useTbName) {
  SName      name = {0};
  SDbCfgInfo pDbInfo = {0};
  int32_t    code = TSDB_CODE_SUCCESS;
  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tsmaName, &name);
  code = tNameExtractFullName(&name, pReq->name);
  if (TSDB_CODE_SUCCESS == code) {
    memset(&name, 0, sizeof(SName));
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, useTbName);
    code = tNameExtractFullName(useTbName, pReq->stb);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pReq->igExists = pStmt->ignoreExists;
    code = getDBCfg(pCxt, pStmt->dbName, &pDbInfo);
  }
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pStmt->precision = pDbInfo.precision;
  code = translateValue(pCxt, (SValueNode*)pStmt->pOptions->pInterval);
  if (code == DEAL_RES_ERROR) {
    return code;
  }
  pReq->interval = ((SValueNode*)pStmt->pOptions->pInterval)->datum.i;
  pReq->intervalUnit = ((SValueNode*)pStmt->pOptions->pInterval)->unit;

#define TSMA_MIN_INTERVAL_MS 1000 * 60         // 1m
#define TSMA_MAX_INTERVAL_MS (60UL * 60UL * 1000UL * 24UL * 365UL)  // 1y

  if (!IS_CALENDAR_TIME_DURATION(pReq->intervalUnit)) {
    int64_t factor = TSDB_TICK_PER_SECOND(pDbInfo.precision) / TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_MILLI);
    if (pReq->interval > TSMA_MAX_INTERVAL_MS * factor || pReq->interval < TSMA_MIN_INTERVAL_MS * factor) {
      return TSDB_CODE_TSMA_INVALID_INTERVAL;
    }
  } else {
    if (pReq->intervalUnit == TIME_UNIT_MONTH && (pReq->interval < 1 || pReq->interval > 12))
      return TSDB_CODE_TSMA_INVALID_INTERVAL;
    if (pReq->intervalUnit == TIME_UNIT_YEAR && (pReq->interval != 1))
      return TSDB_CODE_TSMA_INVALID_INTERVAL;
  }

  STableMeta*     pTableMeta = NULL;
  STableTSMAInfo* pRecursiveTsma = NULL;
  int32_t         numOfCols = 0, numOfTags = 0;
  SSchema *       pCols = NULL, *pTags = NULL;
  if (pStmt->pOptions->recursiveTsma) {
    // useTbName is base tsma name
    code = getTsma(pCxt, useTbName, &pRecursiveTsma);
    if (code == TSDB_CODE_SUCCESS) {
      pReq->recursiveTsma = true;
      code = tNameExtractFullName(useTbName, pReq->baseTsmaName);
      if (TSDB_CODE_SUCCESS == code) {
        SValueNode* pInterval = (SValueNode*)pStmt->pOptions->pInterval;
        if (checkRecursiveTsmaInterval(pRecursiveTsma->interval, pRecursiveTsma->unit, pInterval->datum.i,
              pInterval->unit, pDbInfo.precision, true)) {
        } else {
          code = TSDB_CODE_TSMA_INVALID_RECURSIVE_INTERVAL;
        }
      }
    }
    if (code == TSDB_CODE_SUCCESS) {
      SNode* pNode;
      if (TSDB_CODE_SUCCESS != nodesStringToNode(pRecursiveTsma->ast, &pNode)) {
        return TSDB_CODE_TSMA_INVALID_STAT;
      }
      SSelectStmt* pSelect = (SSelectStmt*)pNode;
      FOREACH(pNode, pSelect->pProjectionList) {
        SFunctionNode* pFuncNode = (SFunctionNode*)pNode;
        if (!fmIsTSMASupportedFunc(pFuncNode->funcId)) continue;
        SNode* pNew = NULL;
        code = nodesCloneNode(pNode, &pNew);
        if (TSDB_CODE_SUCCESS != code) break;
        code = nodesListMakeStrictAppend(&pStmt->pOptions->pFuncs, pNew);
        if (TSDB_CODE_SUCCESS != code) {
          break;
        }
      }
      nodesDestroyNode((SNode*)pSelect);
      if (TSDB_CODE_SUCCESS == code) {
        memset(useTbName, 0, sizeof(SName));
        memcpy(pStmt->originalTbName, pRecursiveTsma->tb, TSDB_TABLE_NAME_LEN);
        toName(pCxt->pParseCxt->acctId, pStmt->dbName, pRecursiveTsma->tb, useTbName);
        code = tNameExtractFullName(useTbName, pReq->stb);
      }
      if (TSDB_CODE_SUCCESS == code) {
        numOfCols = pRecursiveTsma->pUsedCols->size;
        numOfTags = pRecursiveTsma->pTags ? pRecursiveTsma->pTags->size : 0;
        pCols = pRecursiveTsma->pUsedCols->pData;
        pTags = pRecursiveTsma->pTags ? pRecursiveTsma->pTags->pData : NULL;
        code = getTableMeta(pCxt, pStmt->dbName, pRecursiveTsma->targetTb, &pTableMeta);
      }
    }
  } else {
    code = getTableMeta(pCxt, pStmt->dbName, pStmt->tableName, &pTableMeta);
    if (TSDB_CODE_SUCCESS == code) {
      numOfCols = pTableMeta->tableInfo.numOfColumns;
      numOfTags = pTableMeta->tableInfo.numOfTags;
      pCols = pTableMeta->schema;
      pTags = pTableMeta->schema + numOfCols;
      if (pTableMeta->tableType == TSDB_NORMAL_TABLE) {
        pReq->normSourceTbUid = pTableMeta->uid;
      } else if (pTableMeta->tableType == TSDB_CHILD_TABLE) {
        code = TSDB_CODE_TSMA_INVALID_TB;
      }
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    pReq->deleteMark =
        convertTimePrecision(tsmaDataDeleteMark, TSDB_TIME_PRECISION_MILLI, pTableMeta->tableInfo.precision);
    code = getSmaIndexSql(pCxt, &pReq->sql, &pReq->sqlLen);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteTSMAFuncs(pCxt, pStmt, numOfCols, pCols);
  }
  if (TSDB_CODE_SUCCESS == code && !pStmt->pOptions->recursiveTsma) {
    if (LIST_LENGTH(pStmt->pOptions->pFuncs) + numOfTags + TSMA_RES_STB_EXTRA_COLUMN_NUM > TSDB_MAX_COLUMNS) {
      code = TSDB_CODE_PAR_TOO_MANY_COLUMNS;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildTSMAAst(pCxt, pStmt, pReq, pStmt->pOptions->recursiveTsma ? pRecursiveTsma->targetTb : pStmt->tableName,
                        numOfTags, pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    const char* pkColName = pTableMeta->schema[0].name;
    const char* tbName = pStmt->pOptions->recursiveTsma ? pRecursiveTsma->targetTb : pStmt->tableName;
    code = createLastTsSelectStmt(pStmt->dbName, tbName, pkColName, &pStmt->pPrevQuery);
  }

  taosMemoryFreeClear(pTableMeta);

  return code;
}

static int32_t translateCreateTSMA(STranslateContext* pCxt, SCreateTSMAStmt* pStmt) {
  pCxt->pCurrStmt = (SNode*)pStmt;
  int32_t code = TSDB_CODE_SUCCESS;

  SName useTbName = {0};
  if (code == TSDB_CODE_SUCCESS) {
    pStmt->pReq = taosMemoryCalloc(1, sizeof(SMCreateSmaReq));
    if (!pStmt->pReq) return terrno;
  }
  if (code == TSDB_CODE_SUCCESS) {
    code = buildCreateTSMAReq(pCxt, pStmt, pStmt->pReq, &useTbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(&useTbName, pCxt->pTargetTables);
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (!pStmt->pPrevQuery) {
      code = buildCmdMsg(pCxt, TDMT_MND_CREATE_TSMA, (FSerializeFunc)tSerializeSMCreateSmaReq, pStmt->pReq);
    } else {
      TSWAP(pCxt->pPrevRoot, pStmt->pPrevQuery);
    }
  }
  return code;
}

static int32_t buildIntervalForCreateTSMA(SCreateTSMAStmt* pStmt, SInterval* pInterval) {
  int32_t code = TSDB_CODE_SUCCESS;
  pInterval->interval = ((SValueNode*)pStmt->pOptions->pInterval)->datum.i;
  pInterval->intervalUnit = ((SValueNode*)pStmt->pOptions->pInterval)->unit;
  pInterval->offset = 0;
  pInterval->sliding = pInterval->interval;
  pInterval->slidingUnit = pInterval->intervalUnit;
  pInterval->precision = pStmt->pOptions->tsPrecision;
  return code;
}

int32_t translatePostCreateTSMA(SParseContext* pParseCxt, SQuery* pQuery, SSDataBlock* pBlock) {
  SCreateTSMAStmt*  pStmt = (SCreateTSMAStmt*)pQuery->pRoot;
  STranslateContext cxt = {0};
  SInterval         interval = {0};
  int64_t           lastTs = 0;

  int32_t code = initTranslateContext(pParseCxt, NULL, &cxt);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildIntervalForCreateTSMA(pStmt, &interval);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = createStreamReqVersionInfo(pBlock, &pStmt->pReq->pVgroupVerList, &lastTs, &interval);
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (interval.interval > 0) {
      pStmt->pReq->lastTs = taosTimeAdd(taosTimeTruncate(lastTs, &interval), interval.interval, interval.intervalUnit,
                                        interval.precision);
    } else {
      pStmt->pReq->lastTs = lastTs + 1;  // start key of the next time window
    }
    code = buildCmdMsg(&cxt, TDMT_MND_CREATE_TSMA, (FSerializeFunc)tSerializeSMCreateSmaReq, pStmt->pReq);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setQuery(&cxt, pQuery);
  }

  if (TSDB_CODE_SUCCESS == code) {
    SName name = {0};
    toName(pParseCxt->acctId, pStmt->dbName, pStmt->originalTbName, &name);
    code = collectUseTable(&name, cxt.pTargetTables);
  }

  int32_t tmpCode = setRefreshMeta(&cxt, pQuery);
  if (TSDB_CODE_SUCCESS == code) code = tmpCode;
  destroyTranslateContext(&cxt);

  tFreeSMCreateSmaReq(pStmt->pReq);
  taosMemoryFreeClear(pStmt->pReq);

  return code;
}

static int32_t translateDropTSMA(STranslateContext* pCxt, SDropTSMAStmt* pStmt) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SMDropSmaReq    dropReq = {0};
  SName           name = {0};
  STableTSMAInfo* pTsma = NULL;
  toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tsmaName, &name);
  code = tNameExtractFullName(&name, dropReq.name);
  if (TSDB_CODE_SUCCESS == code) {
    dropReq.igNotExists = pStmt->ignoreNotExists;
    code = getTsma(pCxt, &name, &pTsma);
  }
  if (code == TSDB_CODE_SUCCESS) {
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pTsma->tb, &name);
    code = collectUseTable(&name, pCxt->pTargetTables);
  }
  if (TSDB_CODE_SUCCESS == code)
    code = buildCmdMsg(pCxt, TDMT_MND_DROP_TSMA, (FSerializeFunc)tSerializeSMDropSmaReq, &dropReq);
  return code;
}

static int32_t translateQuery(STranslateContext* pCxt, SNode* pNode) {
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
    case QUERY_NODE_S3MIGRATE_DATABASE_STMT:
      code = translateS3MigrateDatabase(pCxt, (SS3MigrateDatabaseStmt*)pNode);
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
    case QUERY_NODE_CREATE_INDEX_STMT:
      code = translateCreateIndex(pCxt, (SCreateIndexStmt*)pNode);
      break;
    case QUERY_NODE_DROP_INDEX_STMT:
      code = translateDropIndex(pCxt, (SDropIndexStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_QNODE_STMT:
    case QUERY_NODE_CREATE_BNODE_STMT:
    case QUERY_NODE_CREATE_SNODE_STMT:
    case QUERY_NODE_CREATE_MNODE_STMT:
      code = translateCreateComponentNode(pCxt, (SCreateComponentNodeStmt*)pNode);
      break;
    case QUERY_NODE_DROP_QNODE_STMT:
    case QUERY_NODE_DROP_BNODE_STMT:
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
      code = translateCompact(pCxt, (SCompactDatabaseStmt*)pNode);
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
    case QUERY_NODE_KILL_QUERY_STMT:
      code = translateKillQuery(pCxt, (SKillQueryStmt*)pNode);
      break;
    case QUERY_NODE_KILL_TRANSACTION_STMT:
      code = translateKillTransaction(pCxt, (SKillStmt*)pNode);
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
    case QUERY_NODE_BALANCE_VGROUP_LEADER_STMT:
      code = translateBalanceVgroupLeader(pCxt, (SBalanceVgroupLeaderStmt*)pNode);
      break;
    case QUERY_NODE_BALANCE_VGROUP_LEADER_DATABASE_STMT:
      code = translateBalanceVgroupLeader(pCxt, (SBalanceVgroupLeaderStmt*)pNode);
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
      code = translateShowCreateTable(pCxt, (SShowCreateTableStmt*)pNode);
      break;
    case QUERY_NODE_SHOW_CREATE_VIEW_STMT:
      code = translateShowCreateView(pCxt, (SShowCreateViewStmt*)pNode);
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

static int32_t extractQueryResultSchema(const SNodeList* pProjections, int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = LIST_LENGTH(pProjections);
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
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
    }
    (*pSchema)[index].colId = index + 1;
    if ('\0' != pExpr->userAlias[0]) {
      strcpy((*pSchema)[index].name, pExpr->userAlias);
    } else {
      strcpy((*pSchema)[index].name, pExpr->aliasName);
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
  strcpy((*pSchema)[0].name, TSDB_EXPLAIN_RESULT_COLUMN_NAME);
  return TSDB_CODE_SUCCESS;
}

static int32_t extractDescribeResultSchema(STableMeta* pMeta, int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = DESCRIBE_RESULT_COLS;
  if (pMeta && useCompress(pMeta->tableType)) *numOfCols = DESCRIBE_RESULT_COLS_COMPRESS;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return terrno;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = DESCRIBE_RESULT_FIELD_LEN;
  strcpy((*pSchema)[0].name, "field");

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = DESCRIBE_RESULT_TYPE_LEN;
  strcpy((*pSchema)[1].name, "type");

  (*pSchema)[2].type = TSDB_DATA_TYPE_INT;
  (*pSchema)[2].bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  strcpy((*pSchema)[2].name, "length");

  (*pSchema)[3].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[3].bytes = DESCRIBE_RESULT_NOTE_LEN;
  strcpy((*pSchema)[3].name, "note");

  if (pMeta && useCompress(pMeta->tableType)) {
    (*pSchema)[4].type = TSDB_DATA_TYPE_BINARY;
    (*pSchema)[4].bytes = DESCRIBE_RESULT_COPRESS_OPTION_LEN;
    strcpy((*pSchema)[4].name, "encode");

    (*pSchema)[5].type = TSDB_DATA_TYPE_BINARY;
    (*pSchema)[5].bytes = DESCRIBE_RESULT_COPRESS_OPTION_LEN;
    strcpy((*pSchema)[5].name, "compress");

    (*pSchema)[6].type = TSDB_DATA_TYPE_BINARY;
    (*pSchema)[6].bytes = DESCRIBE_RESULT_COPRESS_OPTION_LEN;
    strcpy((*pSchema)[6].name, "level");
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
  strcpy((*pSchema)[0].name, "Database");

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = TSDB_MAX_BINARY_LEN;
  strcpy((*pSchema)[1].name, "Create Database");

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
  strcpy((*pSchema)[0].name, "Table");

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = SHOW_CREATE_TB_RESULT_FIELD2_LEN;
  strcpy((*pSchema)[1].name, "Create Table");

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
  strcpy((*pSchema)[0].name, "View");

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = SHOW_CREATE_VIEW_RESULT_FIELD2_LEN;
  strcpy((*pSchema)[1].name, "Create View");

  return TSDB_CODE_SUCCESS;
}

static int32_t extractShowVariablesResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = 3;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return terrno;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = TSDB_CONFIG_OPTION_LEN;
  strcpy((*pSchema)[0].name, "name");

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = TSDB_CONFIG_VALUE_LEN;
  strcpy((*pSchema)[1].name, "value");

  (*pSchema)[2].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[2].bytes = TSDB_CONFIG_SCOPE_LEN;
  strcpy((*pSchema)[2].name, "scope");

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
  strcpy((*pSchema)[0].name, "result");

  (*pSchema)[1].type = TSDB_DATA_TYPE_INT;
  (*pSchema)[1].bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  strcpy((*pSchema)[1].name, "id");

  (*pSchema)[2].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[2].bytes = COMPACT_DB_RESULT_FIELD3_LEN;
  strcpy((*pSchema)[2].name, "reason");

  return TSDB_CODE_SUCCESS;
}

int32_t extractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema) {
  if (NULL == pRoot) {
    return TSDB_CODE_SUCCESS;
  }

  switch (nodeType(pRoot)) {
    case QUERY_NODE_SELECT_STMT:
    case QUERY_NODE_SET_OPERATOR:
      return extractQueryResultSchema(getProjectList(pRoot), numOfCols, pSchema);
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
    case QUERY_NODE_SHOW_CREATE_VIEW_STMT:
      return extractShowCreateViewResultSchema(numOfCols, pSchema);
    case QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT:
    case QUERY_NODE_SHOW_VARIABLES_STMT:
      return extractShowVariablesResultSchema(numOfCols, pSchema);
    case QUERY_NODE_COMPACT_DATABASE_STMT:
      return extractCompactDbResultSchema(numOfCols, pSchema);
    default:
      break;
  }

  return TSDB_CODE_FAILED;
}

static int32_t createStarCol(SNode** ppNode) {
  SColumnNode* pCol = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return code;
  }
  strcpy(pCol->colName, "*");
  *ppNode = (SNode*)pCol;
  return code;
}

static int32_t createProjectCol(const char* pProjCol, SNode** ppNode) {
  SColumnNode* pCol = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
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
  int32_t code = nodesMakeNode(QUERY_NODE_SELECT_STMT, (SNode**)&pSelect);
  if (NULL == pSelect) {
    return code;
  }
  sprintf(pSelect->stmtName, "%p", pSelect);

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
  SNodeList* pProjectionList = NULL;
  if (numOfProjs >= 0) {
    int32_t code = createProjectCols(numOfProjs, pProjCol, &pProjectionList);
    if (TSDB_CODE_SUCCESS != code) return code;
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

static int32_t createOperatorNode(EOperatorType opType, const char* pColName, const SNode* pRight, SNode** pOp) {
  if (NULL == pRight) {
    return TSDB_CODE_SUCCESS;
  }

  SOperatorNode* pOper = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOper);
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
  int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOper);
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
  strcpy(((SColumnNode*)pOper->pLeft)->colName, pLeftCol);
  strcpy(((SColumnNode*)pOper->pRight)->colName, pRightCol);

  *ppResOp = (SNode*)pOper;
  return TSDB_CODE_SUCCESS;
}

static int32_t createIsOperatorNode(EOperatorType opType, const char* pColName, SNode** pOp) {
  SOperatorNode* pOper = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOper);
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
  SValueNode* pValNode1 = NULL, *pValNode2 = NULL;
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
  if (TSDB_CODE_SUCCESS == code)
    code = createLogicCondNode(&pNameCond1, &pNameCond2, &pNameCond, LOGIC_COND_TYPE_AND);

  if (TSDB_CODE_SUCCESS == code)
    code = insertCondIntoSelectStmt(pSelect, &pNameCond);

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
  SValueNode* pValNode1 = NULL, * pValNode2 = NULL;
  SNode* pNameCond = NULL;
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

  if (TSDB_CODE_SUCCESS == code)
    code = insertCondIntoSelectStmt(pSelect, &pNameCond);

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNameCond1);
    nodesDestroyNode(pNameCond2);
    nodesDestroyNode(pNameCond);
  }
  return code;
}

static int32_t addShowNormalTablesCond(SSelectStmt* pSelect) {
  SNode*      pTypeCond = NULL;
  int32_t     code = TSDB_CODE_SUCCESS;
  SValueNode* pValNode1 = NULL;
  code = nodesMakeValueNodeFromString("NORMAL_TABLE", &pValNode1);

  if (TSDB_CODE_SUCCESS == code)
    code = createOperatorNode(OP_TYPE_EQUAL, "type", (SNode*)pValNode1, &pTypeCond);

  nodesDestroyNode((SNode*)pValNode1);

  if (TSDB_CODE_SUCCESS == code) code = insertCondIntoSelectStmt(pSelect, &pTypeCond);
  if (TSDB_CODE_SUCCESS != code) nodesDestroyNode(pTypeCond);
  return code;
}

static int32_t addShowChildTablesCond(SSelectStmt* pSelect) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SNode*      pTypeCond = NULL;
  SValueNode* pValNode1 = NULL;
  code = nodesMakeValueNodeFromString("CHILD_TABLE", &pValNode1);

  if (TSDB_CODE_SUCCESS == code)
    code = createOperatorNode(OP_TYPE_EQUAL, "type", (SNode*)pValNode1, &pTypeCond);

  nodesDestroyNode((SNode*)pValNode1);

  if (TSDB_CODE_SUCCESS == code) code = insertCondIntoSelectStmt(pSelect, &pTypeCond);
  if (TSDB_CODE_SUCCESS != code) nodesDestroyNode(pTypeCond);
  return code;
}

static int32_t addShowKindCond(const SShowStmt* pShow, SSelectStmt* pSelect) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pShow->type != QUERY_NODE_SHOW_DATABASES_STMT && pShow->type != QUERY_NODE_SHOW_TABLES_STMT ||
      pShow->showKind == SHOW_KIND_ALL) {
    return TSDB_CODE_SUCCESS;
  }
  if (pShow->type == QUERY_NODE_SHOW_DATABASES_STMT) {
    if (pShow->showKind == SHOW_KIND_DATABASES_USER) {
      code = addShowUserDatabasesCond(pSelect);
    } else if (pShow->showKind == SHOW_KIND_DATABASES_SYSTEM) {
      code = addShowSystemDatabasesCond(pSelect);
    }
  } else if (pShow->type == QUERY_NODE_SHOW_TABLES_STMT) {
    if (pShow->showKind == SHOW_KIND_TABLES_NORMAL) {
      code = addShowNormalTablesCond(pSelect);
    } else if (pShow->showKind == SHOW_KIND_TABLES_CHILD) {
      code = addShowChildTablesCond(pSelect);
    }
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
    strcpy(((SRealTableNode*)pSelect->pFromTable)->qualDbName, ((SValueNode*)pShow->pDbName)->literal);
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
  toName(pCxt->pParseCxt->acctId, ((SValueNode*)pShow->pDbName)->literal, ((SValueNode*)pShow->pTbName)->literal, &name);
  code = getTargetMeta(pCxt, &name, &pTableMeta, true);
  if (TSDB_CODE_SUCCESS != code) {
    code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GET_META_ERROR, tstrerror(code));
    goto _exit;
  }
  if (TSDB_SUPER_TABLE != pTableMeta->tableType && TSDB_CHILD_TABLE != pTableMeta->tableType) {
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
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }
  strcpy(pFunc->functionName, "_tags");
  *ppNode = pFunc;
  return code;
}

static int32_t createShowTableTagsProjections(SNodeList** pProjections, SNodeList** pTags) {
  if (NULL != *pTags) {
    TSWAP(*pProjections, *pTags);
    return TSDB_CODE_SUCCESS;
  }
  SFunctionNode* pTbNameFunc = NULL;
  int32_t code = createTbnameFunction(&pTbNameFunc);
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
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }

  strcpy(pFunc->functionName, "_block_dist_info");
  strcpy(pFunc->node.aliasName, "_block_dist_info");
  *ppNode = pFunc;
  return code;
}

static int32_t createBlockDistFunc(SFunctionNode** ppNode) {
  SFunctionNode* pFunc = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }

  strcpy(pFunc->functionName, "_block_dist");
  strcpy(pFunc->node.aliasName, "_block_dist");
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
    if (TSDB_CODE_SUCCESS == code)
      code = nodesListMakeStrictAppend(&pStmt->pProjectionList, (SNode*)pFuncNew);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->showRewrite = true;
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

static int32_t buildNormalTableBatchReq(int32_t acctId, const SCreateTableStmt* pStmt, const SVgroupInfo* pVgroupInfo,
                                        SVgroupCreateTableBatch* pBatch) {
  char  dbFName[TSDB_DB_FNAME_LEN] = {0};
  SName name = {.type = TSDB_DB_NAME_T, .acctId = acctId};
  (void)strcpy(name.dbname, pStmt->dbName);
  (void)tNameGetFullDbName(&name, dbFName);

  SVCreateTbReq req = {0};
  req.type = TD_NORMAL_TABLE;
  req.name = taosStrdup(pStmt->tableName);
  req.ttl = pStmt->pOptions->ttl;
  if (pStmt->pOptions->commentNull == false) {
    req.comment = taosStrdup(pStmt->pOptions->comment);
    if (NULL == req.comment) {
      tdDestroySVCreateTbReq(&req);
      return TSDB_CODE_OUT_OF_MEMORY;
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
  int32_t code = tInitDefaultSColCmprWrapperByCols(&req.colCmpr, req.ntb.schemaRow.nCols);
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
      int32_t code = setColCompressByOption(
          pScheam->type, columnEncodeVal(((SColumnOptions*)pColDef->pOptions)->encode),
          columnCompressVal(((SColumnOptions*)pColDef->pOptions)->compress),
          columnLevelVal(((SColumnOptions*)pColDef->pOptions)->compressLevel), true, &req.colCmpr.pColCmpr[index].alg);
      if (code != TSDB_CODE_SUCCESS) {
        tdDestroySVCreateTbReq(&req);
        return code;
      }
    }
    ++index;
  }
  pBatch->info = *pVgroupInfo;
  (void)strcpy(pBatch->dbName, pStmt->dbName);
  pBatch->req.pArray = taosArrayInit(1, sizeof(struct SVCreateTbReq));
  if (NULL == pBatch->req.pArray) {
    tdDestroySVCreateTbReq(&req);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (NULL == taosArrayPush(pBatch->req.pArray, &req)) {
    tdDestroySVCreateTbReq(&req);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t serializeVgroupCreateTableBatch(SVgroupCreateTableBatch* pTbBatch, SArray* pBufArray) {
  int      tlen;
  SEncoder coder = {0};

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
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  ((SMsgHead*)buf)->vgId = htonl(pTbBatch->info.vgId);
  ((SMsgHead*)buf)->contLen = htonl(tlen);
  void* pBuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  tEncoderInit(&coder, pBuf, tlen - sizeof(SMsgHead));
  ret = tEncodeSVCreateTbBatchReq(&coder, &pTbBatch->req);
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
    ret = TSDB_CODE_OUT_OF_MEMORY;
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
  int32_t code = nodesMakeNode(QUERY_NODE_VNODE_MODIFY_STMT, (SNode**)&pNewStmt);
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
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SVgroupCreateTableBatch tbatch = {0};
  int32_t                 code = buildNormalTableBatchReq(acctId, pStmt, pInfo, &tbatch);
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
    strcpy(tBatch.dbName, dbName);

    tBatch.req.pArray = taosArrayInit(4, sizeof(struct SVCreateTbReq));
    if (!tBatch.req.pArray) {
      code = terrno;
    } else if (NULL == taosArrayPush(tBatch.req.pArray, &req)) {
      taosArrayDestroy(tBatch.req.pArray);
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      code = taosHashPut(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId), &tBatch, sizeof(tBatch));
      if (TSDB_CODE_SUCCESS != code) {
        taosArrayDestroy(tBatch.req.pArray);
      }
    }
  } else {  // add to the correct vgroup
    if (NULL == taosArrayPush(pTableBatch->req.pArray, &req)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS != code) {
    tdDestroySVCreateTbReq(&req);
  }

  return code;
}

static int32_t createCastFuncForTag(STranslateContext* pCxt, SNode* pNode, SDataType dt, SNode** pCast) {
  SNode* pExpr = NULL;
  int32_t code = nodesCloneNode(pNode, (SNode**)&pExpr);
  if (NULL == pExpr) {
    return code;
  }
  code = translateExpr(pCxt, &pExpr);
  if (TSDB_CODE_SUCCESS == code) {
    code = createCastFunc(pCxt, pExpr, dt, pCast);
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pExpr);
  }
  return code;
}

static int32_t buildKVRowForBindTags(STranslateContext* pCxt, SCreateSubTableClause* pStmt, STableMeta* pSuperTableMeta,
                                     STag** ppTag, SArray* tagName) {
  int32_t numOfTags = getNumOfTags(pSuperTableMeta);
  if (LIST_LENGTH(pStmt->pValsOfTags) != LIST_LENGTH(pStmt->pSpecificTags) ||
      numOfTags < LIST_LENGTH(pStmt->pValsOfTags)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TAGS_NOT_MATCHED);
  }

  SArray* pTagArray = taosArrayInit(LIST_LENGTH(pStmt->pValsOfTags), sizeof(STagVal));
  if (NULL == pTagArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  bool        isJson = false;
  SNode *     pTagNode = NULL, *pNode = NULL;
  uint8_t     precision = pSuperTableMeta->tableInfo.precision;
  SToken      token;
  char        tokenBuf[TSDB_MAX_TAGS_LEN];
  const char* tagStr = NULL;
  FORBOTH(pTagNode, pStmt->pSpecificTags, pNode, pStmt->pValsOfTags) {
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
      code = parseTagValue(&pCxt->msgBuf, &tagStr, precision, pSchema, &token, tagName, pTagArray, ppTag);
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

static int32_t buildKVRowForAllTags(STranslateContext* pCxt, SCreateSubTableClause* pStmt, STableMeta* pSuperTableMeta,
                                    STag** ppTag, SArray* tagName) {
  if (getNumOfTags(pSuperTableMeta) != LIST_LENGTH(pStmt->pValsOfTags)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TAGS_NOT_MATCHED);
  }

  SArray* pTagArray = taosArrayInit(LIST_LENGTH(pStmt->pValsOfTags), sizeof(STagVal));
  if (NULL == pTagArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  bool        isJson = false;
  SNode*      pNode;
  uint8_t     precision = pSuperTableMeta->tableInfo.precision;
  SSchema*    pTagSchema = getTableTagSchema(pSuperTableMeta);
  SToken      token;
  char        tokenBuf[TSDB_MAX_TAGS_LEN];
  const char* tagStr = NULL;
  FOREACH(pNode, pStmt->pValsOfTags) {
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
      code = parseTagValue(&pCxt->msgBuf, &tagStr, precision, pTagSchema, &token, tagName, pTagArray, ppTag);
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
  return TSDB_CODE_SUCCESS;
}
static int32_t rewriteCreateSubTable(STranslateContext* pCxt, SCreateSubTableClause* pStmt, SHashObj* pVgroupHashmap) {
  int32_t code = checkCreateSubTable(pCxt, pStmt);

  STableMeta* pSuperTableMeta = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getTableMeta(pCxt, pStmt->useDbName, pStmt->useTableName, &pSuperTableMeta);
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
      code = buildKVRowForBindTags(pCxt, pStmt, pSuperTableMeta, &pTag, tagName);
    } else {
      code = buildKVRowForAllTags(pCxt, pStmt, pSuperTableMeta, &pTag, tagName);
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
    return TSDB_CODE_OUT_OF_MEMORY;
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
      code = TSDB_CODE_OUT_OF_MEMORY;
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
    parserError("0x%" PRIx64 " catalogGetTableHashVgroup error, code:%s, dbName:%s, tbName:%s", pParseCxt->requestId,
                tstrerror(code), pName->dbname, pName->tname);
  }

  return code;
}

static int32_t parseOneStbRow(SMsgBuf* pMsgBuf, SParseFileContext* pParFileCxt) {
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
                             pParFileCxt->aTagVals, &pParFileCxt->pTag);
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

    code = parseOneStbRow(pMsgBuf, pParFileCxt);

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
        code = TSDB_CODE_OUT_OF_MEMORY;
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
  pParFileCxt->pStbMeta = pSuperTableMeta;
  pParFileCxt->tagNameFilled = false;
  pParFileCxt->pTag = NULL;
  pParFileCxt->ctbName.type = TSDB_TABLE_NAME_T;
  pParFileCxt->ctbName.acctId = acctId;
  strcpy(pParFileCxt->ctbName.dbname, pStmt->useDbName);

  if (NULL == pParFileCxt->aTagNames) {
    pParFileCxt->aTagNames = taosArrayInit(8, TSDB_COL_NAME_LEN);
    if (NULL == pParFileCxt->aTagNames) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _ERR;
    }
  }

  if (NULL == pParFileCxt->aCreateTbData) {
    pParFileCxt->aCreateTbData = taosArrayInit(16, sizeof(SCreateTableData));
    if (NULL == pParFileCxt->aCreateTbData) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _ERR;
    }
  }

  if (NULL == pParFileCxt->aTagIndexs) {
    pParFileCxt->aTagIndexs = taosArrayInit(pStmt->pSpecificTags->length, sizeof(int16_t));
    if (!pParFileCxt->aTagIndexs) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _ERR;
    }
  }

  if (NULL == pParFileCxt->aTagVals) {
    pParFileCxt->aTagVals = taosArrayInit(8, sizeof(STagVal));
    if (!pParFileCxt->aTagVals) {
      code = TSDB_CODE_OUT_OF_MEMORY;
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
    strncpy(pCreateInfo->useDbName, pCreateStmt->useDbName, TSDB_DB_NAME_LEN);
    strncpy(pCreateInfo->useTableName, pCreateStmt->useTableName, TSDB_TABLE_NAME_LEN);
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
  taosCloseFile(&fp);
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
      pModifyStmt->fileProcessing = (sz == tsMaxInsertBatchRows);
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
      qError("failed to serialize create table batch msg, since:%s", tstrerror(code));
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
    return TSDB_CODE_OUT_OF_MEMORY;
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
  int32_t code = nodesMakeNode(QUERY_NODE_VNODE_MODIFY_STMT, (SNode**)&pModifyStmt);
  if (pModifyStmt == NULL) {
    return code;
  }
  pModifyStmt->sqlNodeType = nodeType(pQuery->pRoot);
  pModifyStmt->fileProcessing = false;
  pModifyStmt->destroyParseFileCxt = destructParseFileContext;
  pModifyStmt->pVgroupsHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (NULL == pModifyStmt->pVgroupsHashObj) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
      qError("maxInsertBatchRows may need to be reduced, current:%d", tsMaxInsertBatchRows);
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
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    code = taosHashPut(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId), &tBatch, sizeof(tBatch));
    if (TSDB_CODE_SUCCESS != code) {
      taosArrayDestroy(tBatch.req.pArray);
      tBatch.req.pArray = NULL;
      return code;
    }
  } else {  // add to the correct vgroup
    if (NULL == taosArrayPush(pTableBatch->req.pArray, pReq)) {
      return TSDB_CODE_OUT_OF_MEMORY;
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
    req.name = pClause->tableName;
    code = addDropTbReqIntoVgroup(pVgroupHashmap, &info, &req);
  }

over:
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
    return TSDB_CODE_OUT_OF_MEMORY;
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
    ret = TSDB_CODE_OUT_OF_MEMORY;
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

static int32_t rewriteDropTable(STranslateContext* pCxt, SQuery* pQuery) {
  SDropTableStmt* pStmt = (SDropTableStmt*)pQuery->pRoot;
  int8_t          tableType;
  SNode*          pNode;
  SArray*         pTsmas = NULL;

  SHashObj* pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pVgroupHashmap) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DROP_STABLE);
    }
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
          code = TSDB_CODE_OUT_OF_MEMORY;
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

  SSchema* pSchema = getTagSchema(pTableMeta, pStmt->colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE, "Invalid tag name: %s",
                                   pStmt->colName);
  }
  pReq->tagName = taosStrdup(pStmt->colName);
  if (NULL == pReq->tagName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pReq->pTagArray = taosArrayInit(1, sizeof(STagVal));
  if (NULL == pReq->pTagArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
                         pReq->pTagArray, &pTag);
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

static int32_t buildAddColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                              SVAlterTbReq* pReq) {
  if (NULL != getColSchema(pTableMeta, pStmt->colName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DUPLICATED_COLUMN);
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

  // only super and normal support
  if (pStmt->pColOptions != NULL && TSDB_CHILD_TABLE == pTableMeta->tableType) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }

  pReq->colName = taosStrdup(pStmt->colName);
  if (NULL == pReq->colName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pReq->type = pStmt->dataType.type;
  pReq->flags = COL_SMA_ON;
  pReq->bytes = calcTypeBytes(pStmt->dataType);
  if (pStmt->pColOptions != NULL) {
    if (!checkColumnEncodeOrSetDefault(pReq->type, pStmt->pColOptions->encode)) return TSDB_CODE_TSC_ENCODE_PARAM_ERROR;
    if (!checkColumnCompressOrSetDefault(pReq->type, pStmt->pColOptions->compress))
      return TSDB_CODE_TSC_COMPRESS_PARAM_ERROR;
    if (!checkColumnLevelOrSetDefault(pReq->type, pStmt->pColOptions->compressLevel))
      return TSDB_CODE_TSC_COMPRESS_LEVEL_ERROR;
    int8_t code = setColCompressByOption(pReq->type, columnEncodeVal(pStmt->pColOptions->encode),
                                         columnCompressVal(pStmt->pColOptions->compress),
                                         columnLevelVal(pStmt->pColOptions->compressLevel), true, &pReq->compress);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t buildDropColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, const STableMeta* pTableMeta,
                               SVAlterTbReq* pReq) {
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
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pReq->colId = pSchema->colId;

  return TSDB_CODE_SUCCESS;
}

static int32_t buildUpdateColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, const STableMeta* pTableMeta,
                                 SVAlterTbReq* pReq) {
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

  pReq->colName = taosStrdup(pStmt->colName);
  if (NULL == pReq->colName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pReq->colId = pSchema->colId;

  return TSDB_CODE_SUCCESS;
}

static int32_t buildRenameColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                 SVAlterTbReq* pReq) {
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
    return TSDB_CODE_OUT_OF_MEMORY;
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
        code = TSDB_CODE_OUT_OF_MEMORY;
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
  const SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pStmt->colName);
  }

  pReq->colName = taosStrdup(pStmt->colName);
  pReq->colId = pSchema->colId;
  if (NULL == pReq->colName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (!checkColumnEncode(pStmt->pColOptions->encode)) return TSDB_CODE_TSC_ENCODE_PARAM_ERROR;
  if (!checkColumnCompress(pStmt->pColOptions->compress)) return TSDB_CODE_TSC_COMPRESS_PARAM_ERROR;
  if (!checkColumnLevel(pStmt->pColOptions->compressLevel)) return TSDB_CODE_TSC_COMPRESS_LEVEL_ERROR;
  int8_t code = setColCompressByOption(pSchema->type, columnEncodeVal(pStmt->pColOptions->encode),
                                       columnCompressVal(pStmt->pColOptions->compress),
                                       columnLevelVal(pStmt->pColOptions->compressLevel), true, &pReq->compress);
  return code;
}

static int32_t buildAlterTbReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                               SVAlterTbReq* pReq) {
  pReq->tbName = taosStrdup(pStmt->tableName);
  if (NULL == pReq->tbName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pReq->action = pStmt->alterType;

  switch (pStmt->alterType) {
    case TSDB_ALTER_TABLE_ADD_TAG:
    case TSDB_ALTER_TABLE_DROP_TAG:
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL:
      return buildUpdateTagValReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_ADD_COLUMN:
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION:
      return buildAddColReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      return buildDropColReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      return buildUpdateColReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      return buildUpdateOptionsReq(pCxt, pStmt, pReq);
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      if (TSDB_CHILD_TABLE == pTableMeta->tableType) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
      } else {
        return buildRenameColReq(pCxt, pStmt, pTableMeta, pReq);
      }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS:
      if (TSDB_CHILD_TABLE == pTableMeta->tableType) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
      } else {
        return buildAlterTableColumnCompress(pCxt, pStmt, pTableMeta, pReq);
      }
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
      return TSDB_CODE_OUT_OF_MEMORY;
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
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return code;
}

static int32_t buildModifyVnodeArray(STranslateContext* pCxt, SAlterTableStmt* pStmt, SVAlterTbReq* pReq,
                                     SArray** pArray) {
  SArray* pTmpArray = taosArrayInit(1, sizeof(void*));
  if (NULL == pTmpArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
  for (int i = 0; i < taosArrayGetSize(pReq->pTagArray); ++i) {
    STagVal* p = (STagVal*)taosArrayGet(pReq->pTagArray, i);
    if (IS_VAR_DATA_TYPE(p->type)) {
      taosMemoryFreeClear(p->pData);
    }
  }
  taosArrayDestroy(pReq->pTagArray);
  if (pReq->tagFree) tTagFree((STag*)pReq->pTagVal);
}

static int32_t rewriteAlterTableImpl(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                     SQuery* pQuery) {
  if (TSDB_SUPER_TABLE == pTableMeta->tableType) {
    return TSDB_CODE_SUCCESS;
  } else if (TSDB_CHILD_TABLE != pTableMeta->tableType && TSDB_NORMAL_TABLE != pTableMeta->tableType) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
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

static int32_t rewriteAlterTable(STranslateContext* pCxt, SQuery* pQuery) {
  SAlterTableStmt* pStmt = (SAlterTableStmt*)pQuery->pRoot;

  if (pStmt->dataType.type == TSDB_DATA_TYPE_JSON && pStmt->alterType == TSDB_ALTER_TABLE_ADD_COLUMN) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COL_JSON);
  }

  STableMeta* pTableMeta = NULL;
  int32_t     code = getTableMeta(pCxt, pStmt->dbName, pStmt->tableName, &pTableMeta);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteAlterTableImpl(pCxt, pStmt, pTableMeta, pQuery);
  }
  taosMemoryFree(pTableMeta);
  return code;
}

static int32_t serializeFlushVgroup(SVgroupInfo* pVg, SArray* pBufArray) {
  int32_t len = sizeof(SMsgHead);
  void*   buf = taosMemoryMalloc(len);
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == buf) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  return code;
}

static int32_t serializeFlushDb(SArray* pVgs, SArray** pOutput) {
  int32_t numOfVgs = taosArrayGetSize(pVgs);

  SArray* pBufArray = taosArrayInit(numOfVgs, sizeof(void*));
  if (NULL == pBufArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
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

static int32_t rewriteShowCompactDetailsStmt(STranslateContext* pCxt, SQuery* pQuery) {
  SShowCompactDetailsStmt* pShow = (SShowCompactDetailsStmt*)(pQuery->pRoot);
  SSelectStmt*             pStmt = NULL;
  int32_t                  code = createSelectStmtForShow(QUERY_NODE_SHOW_COMPACT_DETAILS_STMT, &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pShow->pCompactId) {
      code = createOperatorNode(OP_TYPE_EQUAL, "compact_id", pShow->pCompactId, &pStmt->pWhere);
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
  int32_t code = nodesMakeNode(QUERY_NODE_WHEN_THEN, (SNode**)&pWThen);
  if (TSDB_CODE_SUCCESS != code) return code;

  pWThen->pWhen = pWhen;
  pWThen->pThen = pThen;
  *ppResWhenThen = (SNode*)pWThen;
  return TSDB_CODE_SUCCESS;
}

static int32_t createParCaseWhenNode(SNode* pCase, SNodeList* pWhenThenList, SNode* pElse, const char* pAias,
                                     SNode** ppResCaseWhen) {
  SCaseWhenNode* pCaseWhen = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_CASE_WHEN, (SNode**)&pCaseWhen);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  pCaseWhen->pCase = pCase;
  pCaseWhen->pWhenThenList = pWhenThenList;
  pCaseWhen->pElse = pElse;
  if (pAias) {
    strcpy(pCaseWhen->node.aliasName, pAias);
    strcpy(pCaseWhen->node.userAlias, pAias);
  }
  *ppResCaseWhen = (SNode*)pCaseWhen;
  return TSDB_CODE_SUCCESS;
}

static int32_t createParFunctionNode(const char* pFunName, const char* pAias, SNodeList* pParameterList,
                                     SNode** ppResFunc) {
  SFunctionNode* pFunc = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  strcpy(pFunc->functionName, pFunName);
  strcpy(pFunc->node.aliasName, pAias);
  strcpy(pFunc->node.userAlias, pAias);
  pFunc->pParameterList = pParameterList;
  *ppResFunc = (SNode*)pFunc;
  return TSDB_CODE_SUCCESS;
}

static int32_t createParListNode(SNode* pItem, SNodeList** ppResList) {
  SNodeList* pList = NULL;
  int32_t code = nodesMakeList(&pList);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  CHECK_RES_OUT_OF_MEM(nodesListAppend(pList, pItem));
  *ppResList = pList;
  return TSDB_CODE_SUCCESS;
}

static int32_t createParTempTableNode(SSelectStmt* pSubquery, SNode** ppResTempTable) {
  STempTableNode* pTempTable = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_TEMP_TABLE, (SNode**)&pTempTable);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  pTempTable->pSubquery = (SNode*)pSubquery;
  taosRandStr(pTempTable->table.tableAlias, 8);
  strcpy(pSubquery->stmtName, pTempTable->table.tableAlias);
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
  if (TSDB_CODE_SUCCESS == code)
    code = createOperatorNode(OP_TYPE_EQUAL, "v2_status", (SNode*)pValNode, &pCond2);
  if (TSDB_CODE_SUCCESS == code)
    code = createOperatorNode(OP_TYPE_EQUAL, "v3_status", (SNode*)pValNode, &pCond3);
  if (TSDB_CODE_SUCCESS == code)
    code = createOperatorNode(OP_TYPE_EQUAL, "v4_status", (SNode*)pValNode, &pCond4);
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
  if (TSDB_CODE_SUCCESS == code)
    code = createLogicCondNode(&pTemp1, &pCond3, &pTemp2, LOGIC_COND_TYPE_OR);
  if (TSDB_CODE_SUCCESS == code)
    code = createLogicCondNode(&pTemp2, &pCond4, &pFullCond, LOGIC_COND_TYPE_OR);
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

  // case when leader_col = count_col and leader_col > 0 then 1 when leader_col < count_col and leader_col > 0 then 2 else
  // 0 end as status
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
  sprintf(pStmt->stmtName, "%p", pStmt);

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
    case QUERY_NODE_SHOW_STREAMS_STMT:
    case QUERY_NODE_SHOW_BNODES_STMT:
    case QUERY_NODE_SHOW_SNODES_STMT:
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
      code = rewriteShow(pCxt, pQuery);
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
    case QUERY_NODE_CREATE_MULTI_TABLES_STMT:
      code = rewriteCreateMultiTable(pCxt, pQuery);
      break;
    case QUERY_NODE_CREATE_SUBTABLE_FROM_FILE_CLAUSE:
      code = rewriteCreateTableFromFile(pCxt, pQuery);
      break;
    case QUERY_NODE_DROP_TABLE_STMT:
      code = rewriteDropTable(pCxt, pQuery);
      break;
    case QUERY_NODE_ALTER_TABLE_STMT:
      code = rewriteAlterTable(pCxt, pQuery);
      break;
    case QUERY_NODE_FLUSH_DATABASE_STMT:
      code = rewriteFlushDatabase(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_COMPACTS_STMT:
      code = rewriteShowCompacts(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_COMPACT_DETAILS_STMT:
      code = rewriteShowCompactDetailsStmt(pCxt, pQuery);
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
      return TDMT_VND_CREATE_TABLE;
    case QUERY_NODE_ALTER_TABLE_STMT:
      return TDMT_VND_ALTER_TABLE;
    case QUERY_NODE_DROP_TABLE_STMT:
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
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    SFullDatabaseName* pDb = taosHashIterate(pCxt->pDbs, NULL);
    while (NULL != pDb) {
      if (NULL == taosArrayPush(pQuery->pDbList, pDb->fullDbName)) {
        taosHashCancelIterate(pCxt->pDbs, pDb);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      pDb = taosHashIterate(pCxt->pDbs, pDb);
    }
  }

  if (NULL != pCxt->pTables) {
    taosArrayDestroy(pQuery->pTableList);
    pQuery->pTableList = taosArrayInit(taosHashGetSize(pCxt->pTables), sizeof(SName));
    if (NULL == pQuery->pTableList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    SName* pTable = taosHashIterate(pCxt->pTables, NULL);
    while (NULL != pTable) {
      if (NULL == taosArrayPush(pQuery->pTableList, pTable)) {
        taosHashCancelIterate(pCxt->pTables, pTable);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      pTable = taosHashIterate(pCxt->pTables, pTable);
    }
  }

  if (NULL != pCxt->pTargetTables) {
    taosArrayDestroy(pQuery->pTargetTableList);
    pQuery->pTargetTableList = taosArrayInit(taosHashGetSize(pCxt->pTargetTables), sizeof(SName));
    if (NULL == pQuery->pTargetTableList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    SName* pTable = taosHashIterate(pCxt->pTargetTables, NULL);
    while (NULL != pTable) {
      if (NULL == taosArrayPush(pQuery->pTargetTableList, pTable)) {
        taosHashCancelIterate(pCxt->pTargetTables, pTable);
        return TSDB_CODE_OUT_OF_MEMORY;
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
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_VIEW_STMT:
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
    if (TSDB_CODE_SUCCESS != extractResultSchema(pQuery->pRoot, &pQuery->numOfResCols, &pQuery->pResSchema)) {
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
