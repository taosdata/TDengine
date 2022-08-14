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

#include "parInt.h"

#include "catalog.h"
#include "cmdnodes.h"
#include "filter.h"
#include "functionMgt.h"
#include "parUtil.h"
#include "scalar.h"
#include "systable.h"
#include "tglobal.h"
#include "ttime.h"

#define generateDealNodeErrMsg(pCxt, code, ...) \
  (pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, code, ##__VA_ARGS__), DEAL_RES_ERROR)

#define SYSTABLE_SHOW_TYPE_OFFSET QUERY_NODE_SHOW_DNODES_STMT

typedef struct STranslateContext {
  SParseContext*   pParseCxt;
  int32_t          errCode;
  SMsgBuf          msgBuf;
  SArray*          pNsLevel;  // element is SArray*, the element of this subarray is STableNode*
  int32_t          currLevel;
  int32_t          levelNo;
  ESqlClause       currClause;
  SNode*           pCurrStmt;
  SCmdMsgInfo*     pCmdMsg;
  SHashObj*        pDbs;
  SHashObj*        pTables;
  SHashObj*        pTargetTables;
  SExplainOptions* pExplainOpt;
  SParseMetaCache* pMetaCache;
  bool             createStream;
  bool             stableQuery;
} STranslateContext;

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
    .pDbName = TSDB_PERFORMANCE_SCHEMA_DB,
    .pTableName = TSDB_PERFS_TABLE_STREAMS,
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
    .pDbName = TSDB_PERFORMANCE_SCHEMA_DB,
    .pTableName = TSDB_PERFS_TABLE_TOPICS,
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
    .pDbName = TSDB_PERFORMANCE_SCHEMA_DB,
    .pTableName = TSDB_PERFS_TABLE_SUBSCRIPTIONS,
    .numOfShowCols = 1,
    .pShowCols = {"*"}
  },
};
// clang-format on

static int32_t  translateSubquery(STranslateContext* pCxt, SNode* pNode);
static int32_t  translateQuery(STranslateContext* pCxt, SNode* pNode);
static EDealRes translateValue(STranslateContext* pCxt, SValueNode* pVal);

static bool afterGroupBy(ESqlClause clause) { return clause > SQL_CLAUSE_GROUP_BY; }

static bool beforeHaving(ESqlClause clause) { return clause < SQL_CLAUSE_HAVING; }

static bool afterHaving(ESqlClause clause) { return clause > SQL_CLAUSE_HAVING; }

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
  size_t currTotalLevel = taosArrayGetSize(pCxt->pNsLevel);
  if (currTotalLevel > pCxt->currLevel) {
    SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
    taosArrayPush(pTables, &pTable);
    if (hasSameTableAlias(pTables)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_UNIQUE_TABLE_ALIAS,
                                     "Not unique table/alias: '%s'", ((STableNode*)pTable)->tableAlias);
    }
  } else {
    do {
      SArray* pTables = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
      if (pCxt->currLevel == currTotalLevel) {
        taosArrayPush(pTables, &pTable);
        if (hasSameTableAlias(pTables)) {
          return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_UNIQUE_TABLE_ALIAS,
                                         "Not unique table/alias: '%s'", ((STableNode*)pTable)->tableAlias);
        }
      }
      taosArrayPush(pCxt->pNsLevel, &pTables);
      ++currTotalLevel;
    } while (currTotalLevel <= pCxt->currLevel);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectUseDatabaseImpl(const char* pFullDbName, SHashObj* pDbs) {
  SFullDatabaseName name = {0};
  strcpy(name.fullDbName, pFullDbName);
  return taosHashPut(pDbs, pFullDbName, strlen(pFullDbName), &name, sizeof(SFullDatabaseName));
}

static int32_t collectUseDatabase(const SName* pName, SHashObj* pDbs) {
  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pName, dbFName);
  return collectUseDatabaseImpl(dbFName, pDbs);
}

static int32_t collectUseTable(const SName* pName, SHashObj* pTable) {
  char fullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pName, fullName);
  return taosHashPut(pTable, fullName, strlen(fullName), pName, sizeof(SName));
}

static int32_t getTableMetaImpl(STranslateContext* pCxt, const SName* pName, STableMeta** pMeta) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = collectUseDatabase(pName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(pName, pCxt->pTables);
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (pParCxt->async) {
      code = getTableMetaFromCache(pCxt->pMetaCache, pName, pMeta);
    } else {
      SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                               .requestId = pParCxt->requestId,
                               .requestObjRefId = pParCxt->requestRid,
                               .mgmtEps = pParCxt->mgmtEpSet};
      code = catalogGetTableMeta(pParCxt->pCatalog, &conn, pName, pMeta);
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("0x%" PRIx64 " catalogGetTableMeta error, code:%s, dbName:%s, tbName:%s", pCxt->pParseCxt->requestId,
                tstrerror(code), pName->dbname, pName->tname);
  }
  return code;
}

static int32_t getTableMeta(STranslateContext* pCxt, const char* pDbName, const char* pTableName, STableMeta** pMeta) {
  SName name;
  return getTableMetaImpl(pCxt, toName(pCxt->pParseCxt->acctId, pDbName, pTableName, &name), pMeta);
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
  SName          name;
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
  tNameGetFullDbName(pName, fullDbName);
  int32_t code = collectUseDatabaseImpl(fullDbName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    if (pParCxt->async) {
      code = getDbVgInfoFromCache(pCxt->pMetaCache, fullDbName, pVgInfo);
    } else {
      SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                               .requestId = pParCxt->requestId,
                               .requestObjRefId = pParCxt->requestRid,
                               .mgmtEps = pParCxt->mgmtEpSet};
      code = catalogGetDBVgInfo(pParCxt->pCatalog, &conn, fullDbName, pVgInfo);
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("0x%" PRIx64 " catalogGetDBVgInfo error, code:%s, dbFName:%s", pCxt->pParseCxt->requestId,
                tstrerror(code), fullDbName);
  }
  return code;
}

static int32_t getDBVgInfo(STranslateContext* pCxt, const char* pDbName, SArray** pVgInfo) {
  SName name;
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pDbName, strlen(pDbName));
  char dbFname[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, dbFname);
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
  SName name;
  return getTableHashVgroupImpl(pCxt, toName(pCxt->pParseCxt->acctId, pDbName, pTableName, &name), pInfo);
}

static int32_t getDBVgVersion(STranslateContext* pCxt, const char* pDbFName, int32_t* pVersion, int64_t* pDbId,
                              int32_t* pTableNum) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = collectUseDatabaseImpl(pDbFName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    if (pParCxt->async) {
      code = getDbVgVersionFromCache(pCxt->pMetaCache, pDbFName, pVersion, pDbId, pTableNum);
    } else {
      code = catalogGetDBVgVersion(pParCxt->pCatalog, pDbFName, pVersion, pDbId, pTableNum);
    }
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("0x%" PRIx64 " catalogGetDBVgVersion error, code:%s, dbFName:%s", pCxt->pParseCxt->requestId,
                tstrerror(code), pDbFName);
  }
  return code;
}

static int32_t getDBCfg(STranslateContext* pCxt, const char* pDbName, SDbCfgInfo* pInfo) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  SName          name;
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pDbName, strlen(pDbName));
  char dbFname[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, dbFname);
  int32_t code = collectUseDatabaseImpl(dbFname, pCxt->pDbs);
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
  if (pParCxt->async) {
    code = getTableIndexFromCache(pCxt->pMetaCache, pName, pIndexes);
  } else {
    SRequestConnInfo conn = {.pTrans = pParCxt->pTransporter,
                             .requestId = pParCxt->requestId,
                             .requestObjRefId = pParCxt->requestRid,
                             .mgmtEps = pParCxt->mgmtEpSet};

    code = catalogGetTableIndex(pParCxt->pCatalog, &conn, pName, pIndexes);
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
  return defaultVal;
}

static bool isAliasColumn(const SNode* pNode) {
  return (QUERY_NODE_COLUMN == nodeType(pNode) && ('\0' == ((SColumnNode*)pNode)->tableAlias[0]));
}

static bool isAggFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsAggFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isSelectFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsSelectFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isTimelineFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsTimelineFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isImplicitTsFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsImplicitTsFunc(((SFunctionNode*)pNode)->funcId));
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

static bool isTimeLineQuery(SNode* pStmt) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    return ((SSelectStmt*)pStmt)->isTimeLineResult;
  } else {
    return false;
  }
}

static bool isPrimaryKeyImpl(SNode* pExpr) {
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    return (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pExpr)->colId);
  } else if (QUERY_NODE_FUNCTION == nodeType(pExpr)) {
    SFunctionNode* pFunc = (SFunctionNode*)pExpr;
    if (FUNCTION_TYPE_SELECT_VALUE == pFunc->funcType) {
      return isPrimaryKeyImpl(nodesListGetNode(pFunc->pParameterList, 0));
    } else if (FUNCTION_TYPE_WSTART == pFunc->funcType || FUNCTION_TYPE_WEND == pFunc->funcType) {
      return true;
    }
  }
  return false;
}

static bool isPrimaryKey(STempTableNode* pTable, SNode* pExpr) {
  if (!isTimeLineQuery(pTable->pSubquery)) {
    return false;
  }
  return isPrimaryKeyImpl(pExpr);
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
  pCol->hasIndex = (0 == tagFlag);
  pCol->node.resType.type = pColSchema->type;
  pCol->node.resType.bytes = pColSchema->bytes;
  if (TSDB_DATA_TYPE_TIMESTAMP == pCol->node.resType.type) {
    pCol->node.resType.precision = pTable->pMeta->tableInfo.precision;
  }
}

static void setColumnInfoByExpr(STempTableNode* pTable, SExprNode* pExpr, SColumnNode** pColRef) {
  SColumnNode* pCol = *pColRef;

  // pCol->pProjectRef = (SNode*)pExpr;
  if (NULL == pExpr->pAssociation) {
    pExpr->pAssociation = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  }
  taosArrayPush(pExpr->pAssociation, &pColRef);
  strcpy(pCol->tableAlias, pTable->table.tableAlias);
  pCol->colId = isPrimaryKey(pTable, (SNode*)pExpr) ? PRIMARYKEY_TIMESTAMP_COL_ID : 0;
  strcpy(pCol->colName, pExpr->aliasName);
  if ('\0' == pCol->node.aliasName[0]) {
    strcpy(pCol->node.aliasName, pCol->colName);
  }
  if ('\0' == pCol->node.userAlias[0]) {
    strcpy(pCol->node.userAlias, pCol->colName);
  }
  pCol->node.resType = pExpr->resType;
}

static int32_t createColumnsByTable(STranslateContext* pCxt, const STableNode* pTable, bool igTags, SNodeList* pList) {
  if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    const STableMeta* pMeta = ((SRealTableNode*)pTable)->pMeta;
    int32_t           nums = pMeta->tableInfo.numOfColumns +
                   (igTags ? 0 : ((TSDB_SUPER_TABLE == pMeta->tableType) ? pMeta->tableInfo.numOfTags : 0));
    for (int32_t i = 0; i < nums; ++i) {
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      if (NULL == pCol) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_OUT_OF_MEMORY);
      }
      setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema + i, (i - pMeta->tableInfo.numOfColumns), pCol);
      nodesListAppend(pList, (SNode*)pCol);
    }
  } else {
    STempTableNode* pTempTable = (STempTableNode*)pTable;
    SNodeList*      pProjectList = getProjectList(pTempTable->pSubquery);
    SNode*          pNode;
    FOREACH(pNode, pProjectList) {
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      if (NULL == pCol) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_OUT_OF_MEMORY);
      }
      nodesListAppend(pList, (SNode*)pCol);
      SListCell* pCell = nodesListGetCell(pList, LIST_LENGTH(pList) - 1);
      setColumnInfoByExpr(pTempTable, (SExprNode*)pNode, (SColumnNode**)&pCell->pNode);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool isInternalPrimaryKey(const SColumnNode* pCol) {
  return PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId &&
         (0 == strcmp(pCol->colName, ROWTS_PSEUDO_COLUMN_NAME) || 0 == strcmp(pCol->colName, C0_PSEUDO_COLUMN_NAME));
}

static int32_t findAndSetColumn(STranslateContext* pCxt, SColumnNode** pColRef, const STableNode* pTable,
                                bool* pFound) {
  SColumnNode* pCol = *pColRef;
  *pFound = false;
  if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    const STableMeta* pMeta = ((SRealTableNode*)pTable)->pMeta;
    if (isInternalPrimaryKey(pCol)) {
      setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema, -1, pCol);
      *pFound = true;
      return TSDB_CODE_SUCCESS;
    }
    int32_t nums = pMeta->tableInfo.numOfTags + pMeta->tableInfo.numOfColumns;
    for (int32_t i = 0; i < nums; ++i) {
      if (0 == strcmp(pCol->colName, pMeta->schema[i].name)) {
        setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema + i, (i - pMeta->tableInfo.numOfColumns), pCol);
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
        setColumnInfoByExpr(pTempTable, pExpr, pColRef);
        *pFound = true;
      } else if (isPrimaryKey(pTempTable, pNode) && isInternalPrimaryKey(pCol)) {
        setColumnInfoByExpr(pTempTable, pExpr, pColRef);
        *pFound = true;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
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
      pCxt->errCode = findAndSetColumn(pCxt, pCol, pTable, &foundCol);
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
    pCxt->errCode = findAndSetColumn(pCxt, pCol, pTable, &foundCol);
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

static EDealRes translateColumnUseAlias(STranslateContext* pCxt, SColumnNode** pCol, bool* pFound) {
  SNodeList* pProjectionList = getProjectListFromCurrStmt(pCxt->pCurrStmt);
  SNode*     pNode;
  FOREACH(pNode, pProjectionList) {
    SExprNode* pExpr = (SExprNode*)pNode;
    if (0 == strcmp((*pCol)->colName, pExpr->userAlias)) {
      SColumnRefNode* pColRef = (SColumnRefNode*)nodesMakeNode(QUERY_NODE_COLUMN_REF);
      if (NULL == pColRef) {
        pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
        return DEAL_RES_ERROR;
      }
      strcpy(pColRef->colName, pExpr->aliasName);
      nodesDestroyNode(*(SNode**)pCol);
      *(SNode**)pCol = (SNode*)pColRef;
      *pFound = true;
      return DEAL_RES_CONTINUE;
    }
  }
  *pFound = false;
  return DEAL_RES_CONTINUE;
}

static EDealRes translateColumn(STranslateContext* pCxt, SColumnNode** pCol) {
  if (NULL == pCxt->pCurrStmt || isSelectStmt(pCxt->pCurrStmt) && NULL == ((SSelectStmt*)pCxt->pCurrStmt)->pFromTable) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_COLUMN, (*pCol)->colName);
  }

  // count(*)/first(*)/last(*) and so on
  if (0 == strcmp((*pCol)->colName, "*")) {
    return DEAL_RES_CONTINUE;
  }

  EDealRes res = DEAL_RES_CONTINUE;
  if ('\0' != (*pCol)->tableAlias[0]) {
    res = translateColumnWithPrefix(pCxt, pCol);
  } else {
    bool found = false;
    if (SQL_CLAUSE_ORDER_BY == pCxt->currClause) {
      res = translateColumnUseAlias(pCxt, pCol, &found);
    }
    if (DEAL_RES_ERROR != res && !found) {
      if (isSetOperator(pCxt->pCurrStmt)) {
        res = generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_COLUMN, (*pCol)->colName);
      } else {
        res = translateColumnWithoutPrefix(pCxt, pCol);
      }
    }
  }
  return res;
}

static int32_t parseTimeFromValueNode(STranslateContext* pCxt, SValueNode* pVal) {
  if (IS_NUMERIC_TYPE(pVal->node.resType.type) || TSDB_DATA_TYPE_BOOL == pVal->node.resType.type) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
    if (IS_UNSIGNED_NUMERIC_TYPE(pVal->node.resType.type)) {
      pVal->datum.i = pVal->datum.u;
    } else if (IS_FLOAT_TYPE(pVal->node.resType.type)) {
      pVal->datum.i = pVal->datum.d;
    } else if (TSDB_DATA_TYPE_BOOL == pVal->node.resType.type) {
      pVal->datum.i = pVal->datum.b;
    }
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
                          pVal->node.resType.precision) != TSDB_CODE_SUCCESS) {
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
      if (strict && (TSDB_CODE_SUCCESS != code || !IS_VALID_BIGINT(pVal->datum.i))) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(int64_t*)&pVal->typeData = pVal->datum.i;
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      code = toUInteger(pVal->literal, strlen(pVal->literal), 10, &pVal->datum.u);
      if (strict && (TSDB_CODE_SUCCESS != code || !IS_VALID_UTINYINT(pVal->datum.u))) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(uint8_t*)&pVal->typeData = pVal->datum.u;
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      code = toUInteger(pVal->literal, strlen(pVal->literal), 10, &pVal->datum.u);
      if (strict && (TSDB_CODE_SUCCESS != code || !IS_VALID_USMALLINT(pVal->datum.u))) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(uint16_t*)&pVal->typeData = pVal->datum.u;
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      code = toUInteger(pVal->literal, strlen(pVal->literal), 10, &pVal->datum.u);
      if (strict && (TSDB_CODE_SUCCESS != code || !IS_VALID_UINT(pVal->datum.u))) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      *(uint32_t*)&pVal->typeData = pVal->datum.u;
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      code = toUInteger(pVal->literal, strlen(pVal->literal), 10, &pVal->datum.u);
      if (strict && (TSDB_CODE_SUCCESS != code || !IS_VALID_UBIGINT(pVal->datum.u))) {
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
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY: {
      if (strict && (pVal->node.resType.bytes > targetDt.bytes - VARSTR_HEADER_SIZE)) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      }
      pVal->datum.p = taosMemoryCalloc(1, targetDt.bytes + 1);
      if (NULL == pVal->datum.p) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
      }
      int32_t len = TMIN(targetDt.bytes - VARSTR_HEADER_SIZE, pVal->node.resType.bytes);
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
  if (pVal->isDuration) {
    res = translateDurationValue(pCxt, pVal);
  } else {
    res = translateNormalValue(pCxt, pVal, targetDt, strict);
  }
  pVal->node.resType = targetDt;
  pVal->node.resType.scale = pVal->unit;
  pVal->translate = true;
  if (!strict && TSDB_DATA_TYPE_UBIGINT == pVal->node.resType.type && pVal->datum.u <= INT64_MAX) {
    pVal->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  }
  return res;
}

static int32_t calcTypeBytes(SDataType dt) {
  if (TSDB_DATA_TYPE_BINARY == dt.type) {
    return dt.bytes + VARSTR_HEADER_SIZE;
  } else if (TSDB_DATA_TYPE_NCHAR == dt.type) {
    return dt.bytes * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
  } else {
    return dt.bytes;
  }
}

static EDealRes translateValue(STranslateContext* pCxt, SValueNode* pVal) {
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

static EDealRes translateOperator(STranslateContext* pCxt, SOperatorNode* pOp) {
  if (isMultiResFunc(pOp->pLeft)) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pLeft))->aliasName);
  }
  if (isMultiResFunc(pOp->pRight)) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
  }

  if (TSDB_CODE_SUCCESS != scalarGetOperatorResultType(pOp)) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pOp->node.aliasName);
  }

  return DEAL_RES_CONTINUE;
}

static EDealRes haveVectorFunction(SNode* pNode, void* pContext) {
  if (isAggFunc(pNode)) {
    *((bool*)pContext) = true;
    return DEAL_RES_END;
  } else if (isIndefiniteRowsFunc(pNode)) {
    *((bool*)pContext) = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t findTable(STranslateContext* pCxt, const char* pTableAlias, STableNode** pOutput) {
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

// count(*) is rewritten as count(ts) for scannning optimization
static int32_t rewriteCountStar(STranslateContext* pCxt, SFunctionNode* pCount) {
  SColumnNode* pCol = (SColumnNode*)nodesListGetNode(pCount->pParameterList, 0);
  STableNode*  pTable = NULL;
  int32_t      code = findTable(pCxt, ('\0' == pCol->tableAlias[0] ? NULL : pCol->tableAlias), &pTable);
  if (TSDB_CODE_SUCCESS == code && QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    setColumnInfoBySchema((SRealTableNode*)pTable, ((SRealTableNode*)pTable)->pMeta->schema, -1, pCol);
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
  return TSDB_CODE_SUCCESS;
}

static int32_t translateScanPseudoColumnFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsScanPseudoColumnFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (0 == LIST_LENGTH(pFunc->pParameterList)) {
    if (!isSelectStmt(pCxt->pCurrStmt) || NULL == ((SSelectStmt*)pCxt->pCurrStmt)->pFromTable ||
        QUERY_NODE_REAL_TABLE != nodeType(((SSelectStmt*)pCxt->pCurrStmt)->pFromTable)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TBNAME);
    }
  } else {
    SValueNode* pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 0);
    STableNode* pTable = NULL;
    pCxt->errCode = findTable(pCxt, pVal->literal, &pTable);
    if (TSDB_CODE_SUCCESS == pCxt->errCode && (NULL == pTable || QUERY_NODE_REAL_TABLE != nodeType(pTable))) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TBNAME);
    }
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
  if (pSelect->hasAggFuncs || pSelect->hasMultiRowsFunc ||
      (pSelect->hasIndefiniteRowsFunc &&
       (FUNC_RETURN_ROWS_INDEFINITE == pSelect->returnRows || pSelect->returnRows != fmGetFuncReturnRows(pFunc)))) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
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
  if (pSelect->hasAggFuncs || pSelect->hasMultiRowsFunc || pSelect->hasIndefiniteRowsFunc) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
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

static int32_t translateTimelineFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsTimelineFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!isSelectStmt(pCxt->pCurrStmt)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s function must be used in select statements", pFunc->functionName);
  }
  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  if (QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
      !isTimeLineQuery(((STempTableNode*)pSelect->pFromTable)->pSubquery)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                   "%s function requires valid time series input", pFunc->functionName);
  }
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

static int32_t translateWindowPseudoColumnFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsWindowPseudoColumnFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!isSelectStmt(pCxt->pCurrStmt) || NULL == ((SSelectStmt*)pCxt->pCurrStmt)->pWindow) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_WINDOW_PC);
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
                           TSDB_NORMAL_TABLE != ((SRealTableNode*)pTable)->pMeta->tableType))) ||
      NULL != pSelect->pPartitionByList) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_ONLY_SUPPORT_SINGLE_TABLE,
                                   "%s is only supported in single table query", pFunc->functionName);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateForbidSuperTableFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsForbidSuperTableFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!isSelectStmt(pCxt->pCurrStmt)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_ONLY_SUPPORT_SINGLE_TABLE,
                                   "%s is only supported in single table query", pFunc->functionName);
  }
  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pCurrStmt;
  SNode*       pTable = pSelect->pFromTable;
  if ((NULL != pTable && (QUERY_NODE_REAL_TABLE != nodeType(pTable) ||
                          (TSDB_CHILD_TABLE != ((SRealTableNode*)pTable)->pMeta->tableType &&
                           TSDB_NORMAL_TABLE != ((SRealTableNode*)pTable)->pMeta->tableType)))) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_ONLY_SUPPORT_SINGLE_TABLE,
                                   "%s is only supported in single table query", pFunc->functionName);
  }
  return TSDB_CODE_SUCCESS;
}

static bool isStar(SNode* pNode) {
  return (QUERY_NODE_COLUMN == nodeType(pNode)) && ('\0' == ((SColumnNode*)pNode)->tableAlias[0]) &&
         (0 == strcmp(((SColumnNode*)pNode)->colName, "*"));
}

static bool isTableStar(SNode* pNode) {
  return (QUERY_NODE_COLUMN == nodeType(pNode)) && ('\0' != ((SColumnNode*)pNode)->tableAlias[0]) &&
         (0 == strcmp(((SColumnNode*)pNode)->colName, "*"));
}

static int32_t translateMultiResFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (!fmIsMultiResFunc(pFunc->funcId)) {
    return TSDB_CODE_SUCCESS;
  }
  if (SQL_CLAUSE_SELECT != pCxt->currClause) {
    SNode* pPara = nodesListGetNode(pFunc->pParameterList, 0);
    if (isStar(pPara) || isTableStar(pPara)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                                     "%s(*) is only supported in SELECTed list", pFunc->functionName);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getMultiResFuncNum(SNodeList* pParameterList) {
  if (1 == LIST_LENGTH(pParameterList)) {
    return isStar(nodesListGetNode(pParameterList, 0)) ? 2 : 1;
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

static void setFuncClassification(SNode* pCurrStmt, SFunctionNode* pFunc) {
  if (NULL != pCurrStmt && QUERY_NODE_SELECT_STMT == nodeType(pCurrStmt)) {
    SSelectStmt* pSelect = (SSelectStmt*)pCurrStmt;
    pSelect->hasAggFuncs = pSelect->hasAggFuncs ? true : fmIsAggFunc(pFunc->funcId);
    pSelect->hasRepeatScanFuncs = pSelect->hasRepeatScanFuncs ? true : fmIsRepeatScanFunc(pFunc->funcId);
    if (fmIsIndefiniteRowsFunc(pFunc->funcId)) {
      pSelect->hasIndefiniteRowsFunc = true;
      pSelect->returnRows = fmGetFuncReturnRows(pFunc);
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
    pSelect->hasLastRowFunc = pSelect->hasLastRowFunc ? true : (FUNCTION_TYPE_LAST_ROW == pFunc->funcType);
    pSelect->hasTimeLineFunc = pSelect->hasTimeLineFunc ? true : fmIsTimelineFunc(pFunc->funcId);
    pSelect->hasUdaf = pSelect->hasUdaf ? true : fmIsUserDefinedFunc(pFunc->funcId) && fmIsAggFunc(pFunc->funcId);
    pSelect->onlyHasKeepOrderFunc = pSelect->onlyHasKeepOrderFunc ? fmIsKeepOrderFunc(pFunc->funcId) : false;
  }
}

static int32_t rewriteFuncToValue(STranslateContext* pCxt, char* pLiteral, SNode** pNode) {
  SValueNode* pVal = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == pVal) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pVal->node.aliasName, ((SExprNode*)*pNode)->aliasName);
  pVal->node.resType = ((SExprNode*)*pNode)->resType;
  if (NULL == pLiteral) {
    pVal->isNull = true;
  } else {
    pVal->literal = pLiteral;
    if (IS_VAR_DATA_TYPE(pVal->node.resType.type)) {
      pVal->node.resType.bytes = strlen(pLiteral);
    }
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
    pCurrDb = taosMemoryStrDup((void*)pCxt->pParseCxt->db);
    if (NULL == pCurrDb) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return rewriteFuncToValue(pCxt, pCurrDb, pNode);
}

static int32_t rewriteClentVersionFunc(STranslateContext* pCxt, SNode** pNode) {
  char* pVer = taosMemoryStrDup((void*)version);
  if (NULL == pVer) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return rewriteFuncToValue(pCxt, pVer, pNode);
}

static int32_t rewriteServerVersionFunc(STranslateContext* pCxt, SNode** pNode) {
  char* pVer = taosMemoryStrDup((void*)pCxt->pParseCxt->svrVer);
  if (NULL == pVer) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return rewriteFuncToValue(pCxt, pVer, pNode);
}

static int32_t rewriteServerStatusFunc(STranslateContext* pCxt, SNode** pNode) {
  if (pCxt->pParseCxt->nodeOffline) {
    return TSDB_CODE_RPC_NETWORK_UNAVAIL;
  }
  char* pStatus = taosMemoryStrDup((void*)"1");
  return rewriteFuncToValue(pCxt, pStatus, pNode);
}

static int32_t rewriteUserFunc(STranslateContext* pCxt, SNode** pNode) {
  char    userConn[TSDB_USER_LEN + 1 + TSDB_FQDN_LEN] = {0};  // format 'user@host'
  int32_t len = snprintf(userConn, sizeof(userConn), "%s@", pCxt->pParseCxt->pUser);
  taosGetFqdn(userConn + len);
  char* pUserConn = taosMemoryStrDup((void*)userConn);
  if (NULL == pUserConn) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return rewriteFuncToValue(pCxt, pUserConn, pNode);
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

static int32_t translateNoramlFunction(STranslateContext* pCxt, SFunctionNode* pFunc) {
  int32_t code = translateAggFunc(pCxt, pFunc);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateForbidSuperTableFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateScanPseudoColumnFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateIndefiniteRowsFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateForbidFillFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateWindowPseudoColumnFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateForbidStreamFunc(pCxt, pFunc);
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
    code = translateTimelineFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    setFuncClassification(pCxt->pCurrStmt, pFunc);
  }
  return code;
}

static int32_t rewriteQueryTimeFunc(STranslateContext* pCxt, int64_t val, SNode** pNode) {
  if (INT64_MIN == val || INT64_MAX == val) {
    return rewriteFuncToValue(pCxt, NULL, pNode);
  }

  char* pStr = taosMemoryCalloc(1, 20);
  if (NULL == pStr) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  snprintf(pStr, 20, "%" PRId64 "", val);
  return rewriteFuncToValue(pCxt, pStr, pNode);
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
  return translateNoramlFunction(pCxt, *pFunc);
}

static EDealRes translateFunction(STranslateContext* pCxt, SFunctionNode** pFunc) {
  SNode* pParam = NULL;
  FOREACH(pParam, (*pFunc)->pParameterList) {
    if (isMultiResFunc(pParam)) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)pParam)->aliasName);
    }
  }

  pCxt->errCode = getFuncInfo(pCxt, *pFunc);
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
  return TSDB_CODE_PAR_NO_VALID_FUNC_IN_WIN;
}

static EDealRes rewriteColToSelectValFunc(STranslateContext* pCxt, SNode** pNode) {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
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
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
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
    if (nodesEqualNode(getGroupByNode(pGroupNode), *pNode)) {
      return DEAL_RES_IGNORE_CHILD;
    }
  }
  SNode* pPartKey = NULL;
  FOREACH(pPartKey, pSelect->pPartitionByList) {
    if (nodesEqualNode(pPartKey, *pNode)) {
      return rewriteExprToGroupKeyFunc(pCxt, pNode);
    }
  }
  if (isScanPseudoColumnFunc(*pNode) || QUERY_NODE_COLUMN == nodeType(*pNode)) {
    if (pSelect->selectFuncNum > 1 || pSelect->hasOtherVectorFunc || !pSelect->hasSelectFunc) {
      return generateDealNodeErrMsg(pCxt, getGroupByErrorCode(pCxt));
    } else {
      return rewriteColToSelectValFunc(pCxt, pNode);
    }
  }
  if (isVectorFunc(*pNode) && isDistinctOrderBy(pCxt)) {
    return generateDealNodeErrMsg(pCxt, getGroupByErrorCode(pCxt));
  }
  return DEAL_RES_CONTINUE;
}

static int32_t checkExprForGroupBy(STranslateContext* pCxt, SNode** pNode) {
  nodesRewriteExpr(pNode, doCheckExprForGroupBy, pCxt);
  return pCxt->errCode;
}

static int32_t checkExprListForGroupBy(STranslateContext* pCxt, SSelectStmt* pSelect, SNodeList* pList) {
  if (NULL == getGroupByList(pCxt) && NULL == pSelect->pWindow) {
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
} CheckAggColCoexistCxt;

static EDealRes doCheckAggColCoexist(SNode** pNode, void* pContext) {
  CheckAggColCoexistCxt* pCxt = (CheckAggColCoexistCxt*)pContext;
  if (isVectorFunc(*pNode)) {
    return DEAL_RES_IGNORE_CHILD;
  }
  SNode* pPartKey = NULL;
  FOREACH(pPartKey, ((SSelectStmt*)pCxt->pTranslateCxt->pCurrStmt)->pPartitionByList) {
    if (nodesEqualNode(pPartKey, *pNode)) {
      return rewriteExprToGroupKeyFunc(pCxt->pTranslateCxt, pNode);
    }
  }
  if (isScanPseudoColumnFunc(*pNode) || QUERY_NODE_COLUMN == nodeType(*pNode)) {
    pCxt->existCol = true;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t checkAggColCoexist(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL != pSelect->pGroupByList || NULL != pSelect->pWindow ||
      (!pSelect->hasAggFuncs && !pSelect->hasIndefiniteRowsFunc && !pSelect->hasInterpFunc)) {
    return TSDB_CODE_SUCCESS;
  }
  if (!pSelect->onlyHasKeepOrderFunc) {
    pSelect->isTimeLineResult = false;
  }
  CheckAggColCoexistCxt cxt = {.pTranslateCxt = pCxt, .existCol = false};
  nodesRewriteExprs(pSelect->pProjectionList, doCheckAggColCoexist, &cxt);
  if (!pSelect->isDistinct) {
    nodesRewriteExprs(pSelect->pOrderByList, doCheckAggColCoexist, &cxt);
  }
  if (1 == pSelect->selectFuncNum && !pSelect->hasOtherVectorFunc) {
    return rewriteColsToSelectValFunc(pCxt, pSelect);
  }
  if (cxt.existCol) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_SINGLE_GROUP);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkWindowFuncCoexist(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pWindow) {
    return TSDB_CODE_SUCCESS;
  }
  if (NULL != pSelect->pWindow && !pSelect->hasAggFuncs) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NO_VALID_FUNC_IN_WIN);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t toVgroupsInfo(SArray* pVgs, SVgroupsInfo** pVgsInfo) {
  size_t vgroupNum = taosArrayGetSize(pVgs);
  *pVgsInfo = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo) * vgroupNum);
  if (NULL == *pVgsInfo) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
  taosArrayPush(*pVgroupList, &vg);
  return TSDB_CODE_SUCCESS;
}

static int32_t dnodeToVgroupsInfo(SArray* pDnodes, SVgroupsInfo** pVgsInfo) {
  size_t ndnode = taosArrayGetSize(pDnodes);
  *pVgsInfo = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo) * ndnode);
  if (NULL == *pVgsInfo) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*pVgsInfo)->numOfVgroups = ndnode;
  for (int32_t i = 0; i < ndnode; ++i) {
    memcpy(&((*pVgsInfo)->vgroups[i].epSet), taosArrayGet(pDnodes, i), sizeof(SEpSet));
  }
  return TSDB_CODE_SUCCESS;
}

static bool sysTableFromVnode(const char* pTable) {
  return (0 == strcmp(pTable, TSDB_INS_TABLE_TABLES)) ||
         (0 == strcmp(pTable, TSDB_INS_TABLE_TABLE_DISTRIBUTED) || (0 == strcmp(pTable, TSDB_INS_TABLE_TAGS)));
}

static bool sysTableFromDnode(const char* pTable) { return 0 == strcmp(pTable, TSDB_INS_TABLE_DNODE_VARIABLES); }

static int32_t setVnodeSysTableVgroupList(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  int32_t code = TSDB_CODE_SUCCESS;
  SArray* vgroupList = NULL;
  if ('\0' != pRealTable->qualDbName[0]) {
    if (0 != strcmp(pRealTable->qualDbName, TSDB_INFORMATION_SCHEMA_DB)) {
      code = getDBVgInfo(pCxt, pRealTable->qualDbName, &vgroupList);
    }
  } else {
    code = getDBVgInfoImpl(pCxt, pName, &vgroupList);
  }

  if (TSDB_CODE_SUCCESS == code &&
      0 == strcmp(pRealTable->table.dbName, TSDB_INFORMATION_SCHEMA_DB) &&
      0 == strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_TAGS) &&
      isSelectStmt(pCxt->pCurrStmt) &&
      0 == taosArrayGetSize(vgroupList)) {
    ((SSelectStmt*)pCxt->pCurrStmt)->isEmptyResult = true;
  }

  if (TSDB_CODE_SUCCESS == code &&
      0 == strcmp(pRealTable->table.dbName, TSDB_INFORMATION_SCHEMA_DB) &&
      0 == strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_TABLES)) {
    code = addMnodeToVgroupList(&pCxt->pParseCxt->mgmtEpSet, &vgroupList);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = toVgroupsInfo(vgroupList, &pRealTable->pVgroupList);
  }
  taosArrayDestroy(vgroupList);

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

static int32_t setTableVgroupList(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  if (pCxt->pParseCxt->topicQuery) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (TSDB_SUPER_TABLE == pRealTable->pMeta->tableType) {
    SArray* vgroupList = NULL;
    code = getDBVgInfoImpl(pCxt, pName, &vgroupList);
    if (TSDB_CODE_SUCCESS == code) {
      code = toVgroupsInfo(vgroupList, &pRealTable->pVgroupList);
    }
    taosArrayDestroy(vgroupList);
  } else if (TSDB_SYSTEM_TABLE == pRealTable->pMeta->tableType) {
    code = setSysTableVgroupList(pCxt, pName, pRealTable);
  } else {
    pRealTable->pVgroupList = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo));
    if (NULL == pRealTable->pVgroupList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pRealTable->pVgroupList->numOfVgroups = 1;
    code = getTableHashVgroupImpl(pCxt, pName, pRealTable->pVgroupList->vgroups);
  }
  return code;
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
           0 != strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_TABLE_DISTRIBUTED) &&
           0 != strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_TAGS);
  }
  return (TSDB_CHILD_TABLE == tableType || TSDB_NORMAL_TABLE == tableType);
}

static int32_t setTableIndex(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  if (pCxt->createStream || QUERY_SMA_OPTIMIZE_DISABLE == tsQuerySmaOptimize) {
    return TSDB_CODE_SUCCESS;
  }
  if (isSelectStmt(pCxt->pCurrStmt) && NULL != ((SSelectStmt*)pCxt->pCurrStmt)->pWindow &&
      QUERY_NODE_INTERVAL_WINDOW == nodeType(((SSelectStmt*)pCxt->pCurrStmt)->pWindow)) {
    return getTableIndex(pCxt, pName, &pRealTable->pSmaIndexes);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t setTableCacheLastMode(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  if (TSDB_SYSTEM_TABLE == pRealTable->pMeta->tableType) {
    return TSDB_CODE_SUCCESS;
  }

  SDbCfgInfo dbCfg = {0};
  int32_t    code = getDBCfg(pCxt, pRealTable->table.dbName, &dbCfg);
  if (TSDB_CODE_SUCCESS == code) {
    pRealTable->cacheLastMode = dbCfg.cacheLast;
  }
  return code;
}

static int32_t translateTable(STranslateContext* pCxt, SNode* pTable) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pTable)) {
    case QUERY_NODE_REAL_TABLE: {
      SRealTableNode* pRealTable = (SRealTableNode*)pTable;
      pRealTable->ratio = (NULL != pCxt->pExplainOpt ? pCxt->pExplainOpt->ratio : 1.0);
      // The SRealTableNode created through ROLLUP already has STableMeta.
      if (NULL == pRealTable->pMeta) {
        SName name;
        code = getTableMetaImpl(
            pCxt, toName(pCxt->pParseCxt->acctId, pRealTable->table.dbName, pRealTable->table.tableName, &name),
            &(pRealTable->pMeta));
        if (TSDB_CODE_SUCCESS != code) {
          return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GET_META_ERROR, tstrerror(code));
        }
        code = setTableVgroupList(pCxt, &name, pRealTable);
        if (TSDB_CODE_SUCCESS == code) {
          code = setTableIndex(pCxt, &name, pRealTable);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = setTableCacheLastMode(pCxt, &name, pRealTable);
        }
      }
      pRealTable->table.precision = pRealTable->pMeta->tableInfo.precision;
      pRealTable->table.singleTable = isSingleTable(pRealTable);
      if (TSDB_CODE_SUCCESS == code) {
        code = addNamespace(pCxt, pRealTable);
      }
      if (TSDB_SUPER_TABLE == pRealTable->pMeta->tableType) {
        pCxt->stableQuery = true;
      }
      break;
    }
    case QUERY_NODE_TEMP_TABLE: {
      STempTableNode* pTempTable = (STempTableNode*)pTable;
      code = translateSubquery(pCxt, pTempTable->pSubquery);
      if (TSDB_CODE_SUCCESS == code) {
        pTempTable->table.precision = getStmtPrecision(pTempTable->pSubquery);
        pTempTable->table.singleTable = stmtIsSingleTable(pTempTable->pSubquery);
        code = addNamespace(pCxt, pTempTable);
      }
      break;
    }
    case QUERY_NODE_JOIN_TABLE: {
      SJoinTableNode* pJoinTable = (SJoinTableNode*)pTable;
      code = translateTable(pCxt, pJoinTable->pLeft);
      if (TSDB_CODE_SUCCESS == code) {
        code = translateTable(pCxt, pJoinTable->pRight);
      }
      if (TSDB_CODE_SUCCESS == code) {
        pJoinTable->table.precision = calcJoinTablePrecision(pJoinTable);
        pJoinTable->table.singleTable = joinTableIsSingleTable(pJoinTable);
        code = translateExpr(pCxt, &pJoinTable->pOnCond);
      }
      break;
    }
    default:
      break;
  }
  return code;
}

static int32_t createAllColumns(STranslateContext* pCxt, bool igTags, SNodeList** pCols) {
  *pCols = nodesMakeList();
  if (NULL == *pCols) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_OUT_OF_MEMORY);
  }
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t  nums = taosArrayGetSize(pTables);
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    int32_t     code = createColumnsByTable(pCxt, pTable, igTags, *pCols);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static SNode* createMultiResFunc(SFunctionNode* pSrcFunc, SExprNode* pExpr) {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return NULL;
  }
  pFunc->pParameterList = nodesMakeList();
  if (NULL == pFunc->pParameterList ||
      TSDB_CODE_SUCCESS != nodesListStrictAppend(pFunc->pParameterList, nodesCloneNode((SNode*)pExpr))) {
    nodesDestroyNode((SNode*)pFunc);
    return NULL;
  }

  pFunc->node.resType = pExpr->resType;
  pFunc->funcId = pSrcFunc->funcId;
  pFunc->funcType = pSrcFunc->funcType;
  strcpy(pFunc->functionName, pSrcFunc->functionName);
  char    buf[TSDB_FUNC_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
  int32_t len = 0;
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    SColumnNode* pCol = (SColumnNode*)pExpr;
    len = snprintf(buf, sizeof(buf), "%s(%s.%s)", pSrcFunc->functionName, pCol->tableAlias, pCol->colName);
  } else {
    len = snprintf(buf, sizeof(buf), "%s(%s)", pSrcFunc->functionName, pExpr->aliasName);
  }
  strncpy(pFunc->node.aliasName, buf, TMIN(len, sizeof(pFunc->node.aliasName) - 1));

  return (SNode*)pFunc;
}

static int32_t createTableAllCols(STranslateContext* pCxt, SColumnNode* pCol, bool igTags, SNodeList** pOutput) {
  STableNode* pTable = NULL;
  int32_t     code = findTable(pCxt, pCol->tableAlias, &pTable);
  if (TSDB_CODE_SUCCESS == code && NULL == *pOutput) {
    *pOutput = nodesMakeList();
    if (NULL == *pOutput) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_OUT_OF_MEMORY);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnsByTable(pCxt, pTable, igTags, *pOutput);
  }
  return code;
}

static int32_t createMultiResFuncsParas(STranslateContext* pCxt, SNodeList* pSrcParas, SNodeList** pOutput) {
  int32_t code = TSDB_CODE_SUCCESS;

  SNodeList* pExprs = NULL;
  SNode*     pPara = NULL;
  FOREACH(pPara, pSrcParas) {
    if (isStar(pPara)) {
      code = createAllColumns(pCxt, true, &pExprs);
    } else if (isTableStar(pPara)) {
      code = createTableAllCols(pCxt, (SColumnNode*)pPara, true, &pExprs);
    } else {
      code = nodesListMakeStrictAppend(&pExprs, nodesCloneNode(pPara));
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
  SNodeList* pFuncs = nodesMakeList();
  if (NULL == pFuncs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pExpr = NULL;
  FOREACH(pExpr, pExprs) {
    code = nodesListStrictAppend(pFuncs, createMultiResFunc(pSrcFunc, (SExprNode*)pExpr));
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

static int32_t translateStar(STranslateContext* pCxt, SSelectStmt* pSelect) {
  SNode* pNode = NULL;
  WHERE_EACH(pNode, pSelect->pProjectionList) {
    int32_t code = TSDB_CODE_SUCCESS;
    if (isStar(pNode)) {
      SNodeList* pCols = NULL;
      code = createAllColumns(pCxt, false, &pCols);
      if (TSDB_CODE_SUCCESS == code) {
        INSERT_LIST(pSelect->pProjectionList, pCols);
        ERASE_NODE(pSelect->pProjectionList);
        continue;
      }
    } else if (isMultiResFunc(pNode)) {
      SNodeList* pFuncs = NULL;
      code = createMultiResFuncsFromStar(pCxt, (SFunctionNode*)pNode, &pFuncs);
      if (TSDB_CODE_SUCCESS == code) {
        INSERT_LIST(pSelect->pProjectionList, pFuncs);
        ERASE_NODE(pSelect->pProjectionList);
        continue;
      }
    } else if (isTableStar(pNode)) {
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
      return -1;
    case TSDB_DATA_TYPE_BOOL:
      return (pVal->datum.b ? 1 : 0);
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
      return pVal->datum.i;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      return pVal->datum.d;
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

static int32_t translateOrderByPosition(STranslateContext* pCxt, SNodeList* pProjectionList, SNodeList* pOrderByList,
                                        bool* pOther) {
  *pOther = false;
  SNode* pNode = NULL;
  WHERE_EACH(pNode, pOrderByList) {
    SNode* pExpr = ((SOrderByExprNode*)pNode)->pExpr;
    if (QUERY_NODE_VALUE == nodeType(pExpr)) {
      SValueNode* pVal = (SValueNode*)pExpr;
      if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
        return pCxt->errCode;
      }
      int32_t pos = getPositionValue(pVal);
      if (pos < 0) {
        ERASE_NODE(pOrderByList);
        continue;
      } else if (0 == pos || pos > LIST_LENGTH(pProjectionList)) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT);
      } else {
        SColumnRefNode* pCol = (SColumnRefNode*)nodesMakeNode(QUERY_NODE_COLUMN_REF);
        if (NULL == pCol) {
          return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_OUT_OF_MEMORY);
        }
        strcpy(pCol->colName, ((SExprNode*)nodesListGetNode(pProjectionList, pos - 1))->aliasName);
        ((SOrderByExprNode*)pNode)->pExpr = (SNode*)pCol;
        nodesDestroyNode(pExpr);
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
  int32_t code = translateOrderByPosition(pCxt, pSelect->pProjectionList, pSelect->pOrderByList, &other);
  if (TSDB_CODE_SUCCESS == code) {
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

static int32_t translateFillValues(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pWindow || QUERY_NODE_INTERVAL_WINDOW != nodeType(pSelect->pWindow) ||
      NULL == ((SIntervalWindowNode*)pSelect->pWindow)->pFill) {
    return TSDB_CODE_SUCCESS;
  }
  SFillNode* pFill = (SFillNode*)((SIntervalWindowNode*)pSelect->pWindow)->pFill;
  return TSDB_CODE_SUCCESS;
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

static int32_t checkProjectAlias(STranslateContext* pCxt, SNodeList* pProjectionList) {
  SHashObj* pUserAliasSet = taosHashInit(LIST_LENGTH(pProjectionList),
                                         taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  SNode*    pProject = NULL;
  FOREACH(pProject, pProjectionList) {
    SExprNode* pExpr = (SExprNode*)pProject;
    if (NULL != taosHashGet(pUserAliasSet, pExpr->userAlias, strlen(pExpr->userAlias))) {
      taosHashCleanup(pUserAliasSet);
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_AMBIGUOUS_COLUMN, pExpr->userAlias);
    }
    taosHashPut(pUserAliasSet, pExpr->userAlias, strlen(pExpr->userAlias), &pExpr, POINTER_BYTES);
  }
  taosHashCleanup(pUserAliasSet);
  return TSDB_CODE_SUCCESS;
}

static int32_t translateProjectionList(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (pSelect->isSubquery) {
    return checkProjectAlias(pCxt, pSelect->pProjectionList);
  }
  return rewriteProjectAlias(pSelect->pProjectionList);
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
  return code;
}

static int32_t translateHaving(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pGroupByList && NULL != pSelect->pHaving) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);
  }
  pCxt->currClause = SQL_CLAUSE_HAVING;
  int32_t code = translateExpr(pCxt, &pSelect->pHaving);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkExprForGroupBy(pCxt, &pSelect->pHaving);
  }
  return code;
}

static int32_t translateGroupBy(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pGroupByList) {
    return TSDB_CODE_SUCCESS;
  }
  if (NULL != pSelect->pWindow) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GROUPBY_WINDOW_COEXIST);
  }
  pCxt->currClause = SQL_CLAUSE_GROUP_BY;
  pSelect->isTimeLineResult = false;
  return translateExprList(pCxt, pSelect->pGroupByList);
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

  SNode* pCond = nodesCloneNode(pWhere);
  if (NULL == pCond) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNode* pPrimaryKeyCond = NULL;
  filterPartitionCond(&pCond, &pPrimaryKeyCond, NULL, NULL, NULL);

  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL != pPrimaryKeyCond) {
    bool isStrict = false;
    code = getTimeRange(&pPrimaryKeyCond, pTimeRange, &isStrict);
  } else {
    *pTimeRange = TSWINDOW_INITIALIZER;
  }
  nodesDestroyNode(pCond);
  nodesDestroyNode(pPrimaryKeyCond);
  return code;
}

static int32_t checkFill(STranslateContext* pCxt, SFillNode* pFill, SValueNode* pInterval) {
  if (FILL_MODE_NONE == pFill->mode) {
    return TSDB_CODE_SUCCESS;
  }

  if (TSWINDOW_IS_EQUAL(pFill->timeRange, TSWINDOW_INITIALIZER) ||
      TSWINDOW_IS_EQUAL(pFill->timeRange, TSWINDOW_DESC_INITIALIZER)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE);
  }

  // interp FILL clause
  if (NULL == pInterval) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t timeRange = TABS(pFill->timeRange.skey - pFill->timeRange.ekey);
  int64_t intervalRange = 0;
  if (TIME_IS_VAR_DURATION(pInterval->unit)) {
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
  if ((timeRange == 0) || (timeRange / intervalRange) >= MAX_INTERVAL_TIME_WINDOW) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateFill(STranslateContext* pCxt, SSelectStmt* pSelect, SIntervalWindowNode* pInterval) {
  if (NULL == pInterval->pFill) {
    return TSDB_CODE_SUCCESS;
  }

  ((SFillNode*)pInterval->pFill)->timeRange = pSelect->timeRange;
  return checkFill(pCxt, (SFillNode*)pInterval->pFill, (SValueNode*)pInterval->pInterval);
}

static int64_t getMonthsFromTimeVal(int64_t val, int32_t fromPrecision, char unit) {
  int64_t days = convertTimeFromPrecisionToUnit(val, fromPrecision, 'd');
  switch (unit) {
    case 'b':
    case 'u':
    case 'a':
    case 's':
    case 'm':
    case 'h':
    case 'd':
    case 'w':
      return days / 28;
    case 'n':
      return val;
    case 'y':
      return val * 12;
    default:
      break;
  }
  return -1;
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

static int32_t checkIntervalWindow(STranslateContext* pCxt, SIntervalWindowNode* pInterval) {
  uint8_t precision = ((SColumnNode*)pInterval->pCol)->node.resType.precision;

  SValueNode* pInter = (SValueNode*)pInterval->pInterval;
  bool        valInter = TIME_IS_VAR_DURATION(pInter->unit);
  if (pInter->datum.i <= 0 || (!valInter && pInter->datum.i < tsMinIntervalTime)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_VALUE_TOO_SMALL, tsMinIntervalTime,
                                getPrecisionStr(precision));
  }

  if (NULL != pInterval->pOffset) {
    SValueNode* pOffset = (SValueNode*)pInterval->pOffset;
    if (pOffset->datum.i <= 0) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_NEGATIVE);
    }
    if (pInter->unit == 'n' && pOffset->unit == 'y') {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_UNIT);
    }
    bool fixed = !TIME_IS_VAR_DURATION(pOffset->unit) && !valInter;
    if ((fixed && pOffset->datum.i >= pInter->datum.i) ||
        (!fixed && getMonthsFromTimeVal(pOffset->datum.i, precision, pOffset->unit) >=
                       getMonthsFromTimeVal(pInter->datum.i, precision, pInter->unit))) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG);
    }
  }

  if (NULL != pInterval->pSliding) {
    const static int32_t INTERVAL_SLIDING_FACTOR = 100;

    SValueNode* pSliding = (SValueNode*)pInterval->pSliding;
    if (TIME_IS_VAR_DURATION(pSliding->unit)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SLIDING_UNIT);
    }
    if ((pSliding->datum.i < convertTimePrecision(tsMinSlidingTime, TSDB_TIME_PRECISION_MILLI, precision)) ||
        (pInter->datum.i / pSliding->datum.i > INTERVAL_SLIDING_FACTOR)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SLIDING_TOO_SMALL);
    }
    if (pSliding->datum.i > pInter->datum.i) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SLIDING_TOO_BIG);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateIntervalWindow(STranslateContext* pCxt, SSelectStmt* pSelect, SIntervalWindowNode* pInterval) {
  int32_t code = checkIntervalWindow(pCxt, pInterval);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateFill(pCxt, pSelect, pInterval);
  }
  return code;
}

static EDealRes checkStateExpr(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    STranslateContext* pCxt = pContext;
    SColumnNode*       pCol = (SColumnNode*)pNode;

    int32_t type = pCol->node.resType.type;
    if (!IS_INTEGER_TYPE(type) && type != TSDB_DATA_TYPE_BOOL && !IS_VAR_DATA_TYPE(type)) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_STATE_WIN_TYPE);
    }
    if (COLUMN_TYPE_TAG == pCol->colType) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_STATE_WIN_COL);
    }
  }
  return DEAL_RES_CONTINUE;
}

static bool isPartitionByTbname(SNodeList* pPartitionByList) {
  if (1 != LIST_LENGTH(pPartitionByList)) {
    return false;
  }
  SNode* pPartKey = nodesListGetNode(pPartitionByList, 0);
  return QUERY_NODE_FUNCTION == nodeType(pPartKey) && FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pPartKey)->funcType;
}

static int32_t checkStateWindowForStream(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (!pCxt->createStream) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_SUPER_TABLE == ((SRealTableNode*)pSelect->pFromTable)->pMeta->tableType &&
      !isPartitionByTbname(pSelect->pPartitionByList)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY, "Unsupported stream query");
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateStateWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  SStateWindowNode* pState = (SStateWindowNode*)pSelect->pWindow;
  nodesWalkExprPostOrder(pState->pExpr, checkStateExpr, pCxt);
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pCxt->errCode = checkStateWindowForStream(pCxt, pSelect);
  }
  return pCxt->errCode;
}

static int32_t translateSessionWindow(STranslateContext* pCxt, SSessionWindowNode* pSession) {
  if ('y' == pSession->pGap->unit || 'n' == pSession->pGap->unit || 0 == pSession->pGap->datum.i) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SESSION_GAP);
  }
  if (PRIMARYKEY_TIMESTAMP_COL_ID != pSession->pCol->colId) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SESSION_COL);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSpecificWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  switch (nodeType(pSelect->pWindow)) {
    case QUERY_NODE_STATE_WINDOW:
      return translateStateWindow(pCxt, pSelect);
    case QUERY_NODE_SESSION_WINDOW:
      return translateSessionWindow(pCxt, (SSessionWindowNode*)pSelect->pWindow);
    case QUERY_NODE_INTERVAL_WINDOW:
      return translateIntervalWindow(pCxt, pSelect, (SIntervalWindowNode*)pSelect->pWindow);
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pWindow) {
    return TSDB_CODE_SUCCESS;
  }
  pCxt->currClause = SQL_CLAUSE_WINDOW;
  int32_t code = translateExpr(pCxt, &pSelect->pWindow);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateSpecificWindow(pCxt, pSelect);
  }
  return code;
}

static int32_t createDefaultFillNode(STranslateContext* pCxt, SNode** pOutput) {
  SFillNode* pFill = (SFillNode*)nodesMakeNode(QUERY_NODE_FILL);
  if (NULL == pFill) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pFill->mode = FILL_MODE_NONE;

  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    nodesDestroyNode((SNode*)pFill);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  strcpy(pCol->colName, ROWTS_PSEUDO_COLUMN_NAME);
  pFill->pWStartTs = (SNode*)pCol;

  *pOutput = (SNode*)pFill;
  return TSDB_CODE_SUCCESS;
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
    code = checkFill(pCxt, (SFillNode*)pSelect->pFill, (SValueNode*)pSelect->pEvery);
  }

  return code;
}

static int32_t translateInterp(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (!pSelect->hasInterpFunc) {
    if (NULL != pSelect->pRange || NULL != pSelect->pEvery || NULL != pSelect->pFill) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_INTERP_CLAUSE);
    }
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pSelect->pRange || NULL == pSelect->pEvery || NULL == pSelect->pFill) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_INTERP_CLAUSE,
                                   "Missing RANGE clause, EVERY clause or FILL clause");
  }

  int32_t code = translateExpr(pCxt, &pSelect->pRange);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExpr(pCxt, &pSelect->pEvery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateInterpFill(pCxt, pSelect);
  }
  return code;
}

static int32_t translatePartitionBy(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pPartitionByList) {
    return TSDB_CODE_SUCCESS;
  }
  pCxt->currClause = SQL_CLAUSE_PARTITION_BY;
  return translateExprList(pCxt, pSelect->pPartitionByList);
}

static int32_t translateWhere(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->currClause = SQL_CLAUSE_WHERE;
  int32_t code = translateExpr(pCxt, &pSelect->pWhere);
  if (TSDB_CODE_SUCCESS == code) {
    code = getQueryTimeRange(pCxt, pSelect->pWhere, &pSelect->timeRange);
  }
  return code;
}

static int32_t translateFrom(STranslateContext* pCxt, SNode* pTable) {
  pCxt->currClause = SQL_CLAUSE_FROM;
  return translateTable(pCxt, pTable);
}

static int32_t checkLimit(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if ((NULL != pSelect->pLimit && pSelect->pLimit->offset < 0) ||
      (NULL != pSelect->pSlimit && pSelect->pSlimit->offset < 0)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OFFSET_LESS_ZERO);
  }

  if (NULL != pSelect->pSlimit && NULL == pSelect->pPartitionByList) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SLIMIT_LEAK_PARTITION_BY);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t createPrimaryKeyColByTable(STranslateContext* pCxt, STableNode* pTable, SNode** pPrimaryKey) {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  strcpy(pCol->colName, ROWTS_PSEUDO_COLUMN_NAME);
  bool    found = false;
  int32_t code = findAndSetColumn(pCxt, &pCol, pTable, &found);
  if (TSDB_CODE_SUCCESS != code || !found) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TIMELINE_FUNC);
  }
  *pPrimaryKey = (SNode*)pCol;
  return TSDB_CODE_SUCCESS;
}

static int32_t createPrimaryKeyCol(STranslateContext* pCxt, SNode** pPrimaryKey) {
  STableNode* pTable = NULL;
  int32_t     code = findTable(pCxt, NULL, &pTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = createPrimaryKeyColByTable(pCxt, pTable, pPrimaryKey);
  }
  return code;
}

static EDealRes appendTsForImplicitTsFuncImpl(SNode* pNode, void* pContext) {
  STranslateContext* pCxt = pContext;
  if (isImplicitTsFunc(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    SNode*         pPrimaryKey = NULL;
    pCxt->errCode = createPrimaryKeyCol(pCxt, &pPrimaryKey);
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

typedef struct SReplaceOrderByAliasCxt {
  STranslateContext* pTranslateCxt;
  SNodeList*         pProjectionList;
} SReplaceOrderByAliasCxt;

static EDealRes replaceOrderByAliasImpl(SNode** pNode, void* pContext) {
  SReplaceOrderByAliasCxt* pCxt = pContext;
  if (QUERY_NODE_COLUMN_REF == nodeType(*pNode)) {
    SNodeList* pProjectionList = pCxt->pProjectionList;
    SNode*     pProject = NULL;
    FOREACH(pProject, pProjectionList) {
      SExprNode* pExpr = (SExprNode*)pProject;
      if (0 == strcmp(((SColumnRefNode*)*pNode)->colName, pExpr->aliasName)) {
        SNode* pNew = nodesCloneNode(pProject);
        if (NULL == pNew) {
          pCxt->pTranslateCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
          return DEAL_RES_ERROR;
        }
        ((SExprNode*)pNew)->orderAlias = true;
        nodesDestroyNode(*pNode);
        *pNode = pNew;
        return DEAL_RES_CONTINUE;
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t replaceOrderByAlias(STranslateContext* pCxt, SNodeList* pProjectionList, SNodeList* pOrderByList) {
  if (NULL == pOrderByList) {
    return TSDB_CODE_SUCCESS;
  }
  SReplaceOrderByAliasCxt cxt = {.pTranslateCxt = pCxt, .pProjectionList = pProjectionList};
  nodesRewriteExprsPostOrder(pOrderByList, replaceOrderByAliasImpl, &cxt);
  return pCxt->errCode;
}

static void resetResultTimeline(SSelectStmt* pSelect) {
  if (NULL == pSelect->pOrderByList) {
    return;
  }
  SNode* pOrder = ((SOrderByExprNode*)nodesListGetNode(pSelect->pOrderByList, 0))->pExpr;
  if ((QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) &&
       isPrimaryKey((STempTableNode*)pSelect->pFromTable, pOrder)) ||
      (QUERY_NODE_TEMP_TABLE != nodeType(pSelect->pFromTable) && isPrimaryKeyImpl(pOrder))) {
    pSelect->isTimeLineResult = true;
  } else {
    pSelect->isTimeLineResult = false;
  }
}

static int32_t replaceOrderByAliasForSelect(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = replaceOrderByAlias(pCxt, pSelect->pProjectionList, pSelect->pOrderByList);
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    resetResultTimeline(pSelect);
  }
  return code;
}

static int32_t translateSelectWithoutFrom(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->pCurrStmt = (SNode*)pSelect;
  pCxt->currClause = SQL_CLAUSE_SELECT;
  return translateExprList(pCxt, pSelect->pProjectionList);
}

static int32_t translateSelectFrom(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->pCurrStmt = (SNode*)pSelect;
  int32_t code = translateFrom(pCxt, pSelect->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    pSelect->precision = ((STableNode*)pSelect->pFromTable)->precision;
    code = translateWhere(pCxt, pSelect);
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
  if (TSDB_CODE_SUCCESS == code) {
    code = translateSelectList(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateOrderBy(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkAggColCoexist(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkWindowFuncCoexist(pCxt, pSelect);
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
    code = replaceOrderByAliasForSelect(pCxt, pSelect);
  }
  return code;
}

static int32_t translateSelect(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pFromTable) {
    return translateSelectWithoutFrom(pCxt, pSelect);
  } else {
    return translateSelectFrom(pCxt, pSelect);
  }
}

static SNode* createSetOperProject(const char* pTableAlias, SNode* pNode) {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  pCol->node.resType = ((SExprNode*)pNode)->resType;
  strcpy(pCol->tableAlias, pTableAlias);
  strcpy(pCol->colName, ((SExprNode*)pNode)->aliasName);
  strcpy(pCol->node.aliasName, pCol->colName);
  strcpy(pCol->node.userAlias, ((SExprNode*)pNode)->userAlias);
  return (SNode*)pCol;
}

static int32_t createCastFunc(STranslateContext* pCxt, SNode* pExpr, SDataType dt, SNode** pCast) {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pFunc->functionName, "cast");
  pFunc->node.resType = dt;
  if (TSDB_CODE_SUCCESS != nodesListMakeAppend(&pFunc->pParameterList, pExpr)) {
    nodesDestroyNode((SNode*)pFunc);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (TSDB_CODE_SUCCESS != getFuncInfo(pCxt, pFunc)) {
    nodesClearList(pFunc->pParameterList);
    pFunc->pParameterList = NULL;
    nodesDestroyNode((SNode*)pFunc);
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)pExpr)->aliasName);
  }
  *pCast = (SNode*)pFunc;
  return TSDB_CODE_SUCCESS;
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
    if (!dataTypeEqual(&pLeftExpr->resType, &pRightExpr->resType)) {
      SNode*  pRightFunc = NULL;
      int32_t code = createCastFunc(pCxt, pRight, pLeftExpr->resType, &pRightFunc);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      REPLACE_LIST2_NODE(pRightFunc);
      pRightExpr = (SExprNode*)pRightFunc;
    }
    strcpy(pRightExpr->aliasName, pLeftExpr->aliasName);
    pRightExpr->aliasName[strlen(pLeftExpr->aliasName)] = '\0';
    if (TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pSetOperator->pProjectionList,
                                                       createSetOperProject(pSetOperator->stmtName, pLeft))) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static uint8_t calcSetOperatorPrecision(SSetOperator* pSetOperator) {
  return calcPrecision(getStmtPrecision(pSetOperator->pLeft), getStmtPrecision(pSetOperator->pRight));
}

static int32_t translateSetOperOrderBy(STranslateContext* pCxt, SSetOperator* pSetOperator) {
  bool    other;
  int32_t code = translateOrderByPosition(pCxt, pSetOperator->pProjectionList, pSetOperator->pOrderByList, &other);
  if (TSDB_CODE_SUCCESS == code) {
    if (other) {
      pCxt->currClause = SQL_CLAUSE_ORDER_BY;
      pCxt->pCurrStmt = (SNode*)pSetOperator;
      code = translateExprList(pCxt, pSetOperator->pOrderByList);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = replaceOrderByAlias(pCxt, pSetOperator->pProjectionList, pSetOperator->pOrderByList);
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
  int32_t code = translateQuery(pCxt, pSetOperator->pLeft);
  if (TSDB_CODE_SUCCESS == code) {
    code = resetTranslateNamespace(pCxt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateQuery(pCxt, pSetOperator->pRight);
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
  int32_t code = translateFrom(pCxt, pDelete->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateDeleteWhere(pCxt, pDelete);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCxt->currClause = SQL_CLAUSE_SELECT;
    code = translateExpr(pCxt, &pDelete->pCountFunc);
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
  SOrderByExprNode* pOrderByExpr = (SOrderByExprNode*)nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
  if (NULL == pOrderByExpr) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pOrderByExpr->nullOrder = NULL_ORDER_FIRST;
  pOrderByExpr->order = ORDER_ASC;
  pOrderByExpr->pExpr = nodesCloneNode(pPrimaryKeyExpr);
  if (NULL == pOrderByExpr->pExpr) {
    nodesDestroyNode((SNode*)pOrderByExpr);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  ((SExprNode*)pOrderByExpr->pExpr)->orderAlias = true;
  NODES_DESTORY_LIST(*pOrderByList);
  return nodesListMakeStrictAppend(pOrderByList, (SNode*)pOrderByExpr);
}

static int32_t addOrderByPrimaryKeyToQuery(STranslateContext* pCxt, SNode* pPrimaryKeyExpr, SNode* pStmt) {
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

  SNode* pPrimaryKeyExpr = NULL;
  SNode* pBoundCol = NULL;
  SNode* pProj = NULL;
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
      pPrimaryKeyExpr = pProj;
    }
  }

  if (NULL == pPrimaryKeyExpr) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM,
                                   "Primary timestamp column can not be null");
  }

  return addOrderByPrimaryKeyToQuery(pCxt, pPrimaryKeyExpr, pInsert->pQuery);
}

static int32_t translateInsert(STranslateContext* pCxt, SInsertStmt* pInsert) {
  pCxt->pCurrStmt = (SNode*)pInsert;
  int32_t code = translateFrom(pCxt, pInsert->pTable);
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
  if (pVal->isDuration) {
    return pVal->datum.i / getUnitPerMinute(pVal->node.resType.precision);
  }
  return pVal->datum.i;
}

static int32_t buildCreateDbRetentions(const SNodeList* pRetentions, SCreateDbReq* pReq) {
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
      taosArrayPush(pReq->pRetensions, &retention);
    }
    pReq->numOfRetensions = taosArrayGetSize(pReq->pRetensions);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildCreateDbReq(STranslateContext* pCxt, SCreateDatabaseStmt* pStmt, SCreateDbReq* pReq) {
  SName name = {0};
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  tNameGetFullDbName(&name, pReq->db);
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
  pReq->ignoreExist = pStmt->ignoreExists;
  return buildCreateDbRetentions(pStmt->pOptions->pRetentions, pReq);
}

static int32_t checkRangeOption(STranslateContext* pCxt, int32_t code, const char* pName, int32_t val, int32_t minVal,
                                int32_t maxVal) {
  if (val >= 0 && (val < minVal || val > maxVal)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, code, "Invalid option %s: %" PRId64 " valid range: [%d, %d]", pName,
                                   val, minVal, maxVal);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbRangeOption(STranslateContext* pCxt, const char* pName, int32_t val, int32_t minVal,
                                  int32_t maxVal) {
  return checkRangeOption(pCxt, TSDB_CODE_PAR_INVALID_DB_OPTION, pName, val, minVal, maxVal);
}

static int32_t checkTableRangeOption(STranslateContext* pCxt, const char* pName, int32_t val, int32_t minVal,
                                     int32_t maxVal) {
  return checkRangeOption(pCxt, TSDB_CODE_PAR_INVALID_TABLE_OPTION, pName, val, minVal, maxVal);
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
    if (pVal->isDuration && TIME_UNIT_MINUTE != pVal->unit && TIME_UNIT_HOUR != pVal->unit &&
        TIME_UNIT_DAY != pVal->unit) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                     "Invalid option keep unit: %c, only m, h, d allowed", pVal->unit);
    }
    if (!pVal->isDuration) {
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

  if (pOptions->keep[0] < TSDB_MIN_KEEP || pOptions->keep[1] < TSDB_MIN_KEEP || pOptions->keep[2] < TSDB_MIN_KEEP ||
      pOptions->keep[0] > TSDB_MAX_KEEP || pOptions->keep[1] > TSDB_MAX_KEEP || pOptions->keep[2] > TSDB_MAX_KEEP) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid option keep: %" PRId64 ", %" PRId64 ", %" PRId64 " valid range: [%dm, %dm]",
                                   pOptions->keep[0], pOptions->keep[1], pOptions->keep[2], TSDB_MIN_KEEP,
                                   TSDB_MAX_KEEP);
  }

  if (!((pOptions->keep[0] <= pOptions->keep[1]) && (pOptions->keep[1] <= pOptions->keep[2]))) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid keep value, should be keep0 <= keep1 <= keep2");
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
                                   "Invalid option %s: %" PRId64 ", only %d, %d allowed", pName, val, v1, v2);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbRetentionsOption(STranslateContext* pCxt, SNodeList* pRetentions) {
  if (NULL == pRetentions) {
    return TSDB_CODE_SUCCESS;
  }

  if (LIST_LENGTH(pRetentions) > 3) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION, "Invalid option retentions");
  }

  SValueNode* pPrevFreq = NULL;
  SValueNode* pPrevKeep = NULL;
  SNode*      pRetention = NULL;
  FOREACH(pRetention, pRetentions) {
    SNode* pNode = NULL;
    FOREACH(pNode, ((SNodeListNode*)pRetention)->pNodeList) {
      SValueNode* pVal = (SValueNode*)pNode;
      if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
        return pCxt->errCode;
      }
    }

    SValueNode* pFreq = (SValueNode*)nodesListGetNode(((SNodeListNode*)pRetention)->pNodeList, 0);
    SValueNode* pKeep = (SValueNode*)nodesListGetNode(((SNodeListNode*)pRetention)->pNodeList, 1);
    if (pFreq->datum.i <= 0 || 'n' == pFreq->unit || 'y' == pFreq->unit || pFreq->datum.i >= pKeep->datum.i ||
        (NULL != pPrevFreq && pPrevFreq->datum.i >= pFreq->datum.i) ||
        (NULL != pPrevKeep && pPrevKeep->datum.i > pKeep->datum.i)) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION, "Invalid option retentions");
    }
    pPrevFreq = pFreq;
    pPrevKeep = pKeep;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkOptionsDependency(STranslateContext* pCxt, const char* pDbName, SDatabaseOptions* pOptions) {
  int32_t daysPerFile = pOptions->daysPerFile;
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
  if (daysPerFile > daysToKeep0) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DB_OPTION,
                                   "Invalid duration value, should be keep2 >= keep1 >= keep0 >= duration");
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
    code = checkDbKeepOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "pages", pOptions->pages, TSDB_MIN_PAGES_PER_VNODE, TSDB_MAX_PAGES_PER_VNODE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "pagesize", pOptions->pagesize, TSDB_MIN_PAGESIZE_PER_VNODE,
                              TSDB_MAX_PAGESIZE_PER_VNODE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbPrecisionOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "replications", pOptions->replica, TSDB_MIN_DB_REPLICA, TSDB_MAX_DB_REPLICA);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbStrictOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "walLevel", pOptions->walLevel, TSDB_MIN_WAL_LEVEL, TSDB_MAX_WAL_LEVEL);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRangeOption(pCxt, "vgroups", pOptions->numOfVgroups, TSDB_MIN_VNODES_PER_DB, TSDB_MAX_VNODES_PER_DB);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "singleStable", pOptions->singleStable, TSDB_DB_SINGLE_STABLE_ON,
                             TSDB_DB_SINGLE_STABLE_OFF);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRetentionsOption(pCxt, pOptions->pRetentions);
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
    code = checkOptionsDependency(pCxt, pDbName, pOptions);
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

typedef int32_t (*FSerializeFunc)(void* pBuf, int32_t bufLen, void* pReq);

static int32_t buildCmdMsg(STranslateContext* pCxt, int16_t msgType, FSerializeFunc func, void* pReq) {
  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = msgType;
  pCxt->pCmdMsg->msgLen = func(NULL, 0, pReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  func(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, pReq);

  return TSDB_CODE_SUCCESS;
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
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  tNameGetFullDbName(&name, dropReq.db);
  dropReq.ignoreNotExists = pStmt->ignoreNotExists;

  return buildCmdMsg(pCxt, TDMT_MND_DROP_DB, (FSerializeFunc)tSerializeSDropDbReq, &dropReq);
}

static void buildAlterDbReq(STranslateContext* pCxt, SAlterDatabaseStmt* pStmt, SAlterDbReq* pReq) {
  SName name = {0};
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  tNameGetFullDbName(&name, pReq->db);
  pReq->buffer = pStmt->pOptions->buffer;
  pReq->pageSize = -1;
  pReq->pages = pStmt->pOptions->pages;
  pReq->daysPerFile = -1;
  pReq->daysToKeep0 = pStmt->pOptions->keep[0];
  pReq->daysToKeep1 = pStmt->pOptions->keep[1];
  pReq->daysToKeep2 = pStmt->pOptions->keep[2];
  pReq->walFsyncPeriod = pStmt->pOptions->fsyncPeriod;
  pReq->walLevel = pStmt->pOptions->walLevel;
  pReq->strict = pStmt->pOptions->strict;
  pReq->cacheLast = pStmt->pOptions->cacheModel;
  pReq->cacheLastSize = pStmt->pOptions->cacheLastSize;
  pReq->replications = pStmt->pOptions->replica;
  return;
}

static int32_t translateAlterDatabase(STranslateContext* pCxt, SAlterDatabaseStmt* pStmt) {
  int32_t code = checkDatabaseOptions(pCxt, pStmt->dbName, pStmt->pOptions);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  SAlterDbReq alterReq = {0};
  buildAlterDbReq(pCxt, pStmt, &alterReq);

  return buildCmdMsg(pCxt, TDMT_MND_ALTER_DB, (FSerializeFunc)tSerializeSAlterDbReq, &alterReq);
}

static int32_t translateTrimDatabase(STranslateContext* pCxt, STrimDatabaseStmt* pStmt) {
  STrimDbReq req = {0};
  SName      name = {0};
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  tNameGetFullDbName(&name, req.db);
  return buildCmdMsg(pCxt, TDMT_MND_TRIM_DB, (FSerializeFunc)tSerializeSTrimDbReq, &req);
}

static int32_t columnDefNodeToField(SNodeList* pList, SArray** pArray) {
  *pArray = taosArrayInit(LIST_LENGTH(pList), sizeof(SField));
  SNode* pNode;
  FOREACH(pNode, pList) {
    SColumnDefNode* pCol = (SColumnDefNode*)pNode;
    SField          field = {.type = pCol->dataType.type, .bytes = calcTypeBytes(pCol->dataType)};
    strcpy(field.name, pCol->colName);
    if (pCol->sma) {
      field.flags |= COL_SMA_ON;
    }
    taosArrayPush(*pArray, &field);
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
      if ((TSDB_DATA_TYPE_VARCHAR == pTag->dataType.type && calcTypeBytes(pTag->dataType) > TSDB_MAX_BINARY_LEN) ||
          (TSDB_DATA_TYPE_NCHAR == pTag->dataType.type && calcTypeBytes(pTag->dataType) > TSDB_MAX_NCHAR_LEN)) {
        code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = taosHashPut(pHash, pTag->colName, len, &pTag, POINTER_BYTES);
    }
    if (TSDB_CODE_SUCCESS == code) {
      tagsSize += pTag->dataType.bytes;
    } else {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code && tagsSize > TSDB_MAX_TAGS_LEN) {
    code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAGS_LENGTH, TSDB_MAX_TAGS_LEN);
  }

  return code;
}

static int32_t checkTableColsSchema(STranslateContext* pCxt, SHashObj* pHash, int32_t ntags, SNodeList* pCols) {
  int32_t ncols = LIST_LENGTH(pCols);
  if (ncols < TSDB_MIN_COLUMNS) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
  } else if (ncols + ntags > TSDB_MAX_COLUMNS) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TOO_MANY_COLUMNS);
  }

  int32_t code = TSDB_CODE_SUCCESS;

  bool    first = true;
  int32_t rowSize = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pCols) {
    SColumnDefNode* pCol = (SColumnDefNode*)pNode;
    if (first) {
      first = false;
      if (TSDB_DATA_TYPE_TIMESTAMP != pCol->dataType.type) {
        code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FIRST_COLUMN);
      }
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
          (TSDB_DATA_TYPE_NCHAR == pCol->dataType.type && calcTypeBytes(pCol->dataType) > TSDB_MAX_NCHAR_LEN)) {
        code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = taosHashPut(pHash, pCol->colName, len, &pCol, POINTER_BYTES);
    }
    if (TSDB_CODE_SUCCESS == code) {
      rowSize += pCol->dataType.bytes;
    } else {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code && rowSize > TSDB_MAX_BYTES_PER_ROW) {
    code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ROW_LENGTH, TSDB_MAX_BYTES_PER_ROW);
  }

  return code;
}

static int32_t checkTableSchema(STranslateContext* pCxt, SCreateTableStmt* pStmt) {
  SHashObj* pHash = taosHashInit(LIST_LENGTH(pStmt->pTags) + LIST_LENGTH(pStmt->pCols),
                                 taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == pHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = checkTableTagsSchema(pCxt, pHash, pStmt->pTags);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableColsSchema(pCxt, pHash, LIST_LENGTH(pStmt->pTags), pStmt->pCols);
  }

  taosHashCleanup(pHash);
  return code;
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
    code = checkTableRollupOption(pCxt, pStmt->pOptions->pRollupFuncs, createStable, &dbCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableSmaOption(pCxt, pStmt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableSchema(pCxt, pStmt);
  }
  return code;
}

static void toSchema(const SColumnDefNode* pCol, col_id_t colId, SSchema* pSchema) {
  int8_t flags = 0;
  if (pCol->sma) {
    flags |= COL_SMA_ON;
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
} SSampleAstInfo;

static int32_t buildSampleAst(STranslateContext* pCxt, SSampleAstInfo* pInfo, char** pAst, int32_t* pLen, char** pExpr,
                              int32_t* pExprLen) {
  SSelectStmt* pSelect = (SSelectStmt*)nodesMakeNode(QUERY_NODE_SELECT_STMT);
  if (NULL == pSelect) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  sprintf(pSelect->stmtName, "%p", pSelect);

  SRealTableNode* pTable = (SRealTableNode*)nodesMakeNode(QUERY_NODE_REAL_TABLE);
  if (NULL == pTable) {
    nodesDestroyNode((SNode*)pSelect);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pTable->table.dbName, pInfo->pDbName);
  strcpy(pTable->table.tableName, pInfo->pTableName);
  TSWAP(pTable->pMeta, pInfo->pRollupTableMeta);
  pSelect->pFromTable = (SNode*)pTable;

  TSWAP(pSelect->pProjectionList, pInfo->pFuncs);
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pSelect->pProjectionList || NULL == pFunc) {
    nodesDestroyNode((SNode*)pSelect);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pFunc->functionName, "_wstart");
  nodesListPushFront(pSelect->pProjectionList, (SNode*)pFunc);
  SNode* pProject = NULL;
  FOREACH(pProject, pSelect->pProjectionList) { sprintf(((SExprNode*)pProject)->aliasName, "#%p", pProject); }

  TSWAP(pSelect->pPartitionByList, pInfo->pPartitionByList);

  SIntervalWindowNode* pInterval = (SIntervalWindowNode*)nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW);
  if (NULL == pInterval) {
    nodesDestroyNode((SNode*)pSelect);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pSelect->pWindow = (SNode*)pInterval;
  TSWAP(pInterval->pInterval, pInfo->pInterval);
  TSWAP(pInterval->pOffset, pInfo->pOffset);
  TSWAP(pInterval->pSliding, pInfo->pSliding);
  pInterval->pCol = nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pInterval->pCol) {
    nodesDestroyNode((SNode*)pSelect);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  ((SColumnNode*)pInterval->pCol)->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  strcpy(((SColumnNode*)pInterval->pCol)->colName, ROWTS_PSEUDO_COLUMN_NAME);

  pCxt->createStream = true;
  int32_t code = translateQuery(pCxt, (SNode*)pSelect);
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
}

static SNode* makeIntervalVal(SRetention* pRetension, int8_t precision) {
  SValueNode* pVal = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == pVal) {
    return NULL;
  }
  int64_t timeVal = convertTimeFromPrecisionToUnit(pRetension->freq, precision, pRetension->freqUnit);
  char    buf[20] = {0};
  int32_t len = snprintf(buf, sizeof(buf), "%" PRId64 "%c", timeVal, pRetension->freqUnit);
  pVal->literal = strndup(buf, len);
  if (NULL == pVal->literal) {
    nodesDestroyNode((SNode*)pVal);
    return NULL;
  }
  pVal->isDuration = true;
  pVal->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pVal->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  pVal->node.resType.precision = precision;
  return (SNode*)pVal;
}

static SNode* createColumnFromDef(SColumnDefNode* pDef) {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  strcpy(pCol->colName, pDef->colName);
  return (SNode*)pCol;
}

static SNode* createRollupFunc(SNode* pSrcFunc, SColumnDefNode* pColDef) {
  SFunctionNode* pFunc = (SFunctionNode*)nodesCloneNode(pSrcFunc);
  if (NULL == pFunc) {
    return NULL;
  }
  if (TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pFunc->pParameterList, createColumnFromDef(pColDef))) {
    nodesDestroyNode((SNode*)pFunc);
    return NULL;
  }
  return (SNode*)pFunc;
}

static SNodeList* createRollupFuncs(SCreateTableStmt* pStmt) {
  SNodeList* pFuncs = nodesMakeList();
  if (NULL == pFuncs) {
    return NULL;
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
      if (TSDB_CODE_SUCCESS != nodesListStrictAppend(pFuncs, createRollupFunc(pFunc, (SColumnDefNode*)pCol))) {
        nodesDestroyList(pFuncs);
        return NULL;
      }
    }
  }

  return pFuncs;
}

static STableMeta* createRollupTableMeta(SCreateTableStmt* pStmt, int8_t precision) {
  int32_t     numOfField = LIST_LENGTH(pStmt->pCols) + LIST_LENGTH(pStmt->pTags);
  STableMeta* pMeta = taosMemoryCalloc(1, sizeof(STableMeta) + numOfField * sizeof(SSchema));
  if (NULL == pMeta) {
    return NULL;
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

  return pMeta;
}

static SNode* createTbnameFunction() {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return NULL;
  }
  strcpy(pFunc->functionName, "tbname");
  return (SNode*)pFunc;
}

static int32_t buildSampleAstInfoByTable(STranslateContext* pCxt, SCreateTableStmt* pStmt, SRetention* pRetension,
                                         int8_t precision, SSampleAstInfo* pInfo) {
  pInfo->pDbName = pStmt->dbName;
  pInfo->pTableName = pStmt->tableName;
  pInfo->pFuncs = createRollupFuncs(pStmt);
  pInfo->pInterval = makeIntervalVal(pRetension, precision);
  pInfo->pRollupTableMeta = createRollupTableMeta(pStmt, precision);
  if (NULL == pInfo->pFuncs || NULL == pInfo->pInterval || NULL == pInfo->pRollupTableMeta) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return nodesListMakeStrictAppend(&pInfo->pPartitionByList, createTbnameFunction());
}

static int32_t getRollupAst(STranslateContext* pCxt, SCreateTableStmt* pStmt, SRetention* pRetension, int8_t precision,
                            char** pAst, int32_t* pLen) {
  SSampleAstInfo info = {0};
  int32_t        code = buildSampleAstInfoByTable(pCxt, pStmt, pRetension, precision, &info);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildSampleAst(pCxt, &info, pAst, pLen, NULL, NULL);
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
    initTranslateContext(pCxt->pParseCxt, pCxt->pMetaCache, &cxt);
    code = getRollupAst(&cxt, pStmt, pRetension, dbCfg.precision, 1 == i ? &pReq->pAst1 : &pReq->pAst2,
                        1 == i ? &pReq->ast1Len : &pReq->ast2Len);
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
  SNode* pNode;
  FOREACH(pNode, pFuncs) { taosArrayPush(*pArray, ((SFunctionNode*)pNode)->functionName); }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildCreateStbReq(STranslateContext* pCxt, SCreateTableStmt* pStmt, SMCreateStbReq* pReq) {
  pReq->igExists = pStmt->ignoreExists;
  pReq->delay1 = pStmt->pOptions->maxDelay1;
  pReq->delay2 = pStmt->pOptions->maxDelay2;
  pReq->watermark1 = pStmt->pOptions->watermark1;
  pReq->watermark2 = pStmt->pOptions->watermark2;
  pReq->colVer = 1;
  pReq->tagVer = 1;
  pReq->source = TD_REQ_FROM_APP;
  columnDefNodeToField(pStmt->pCols, &pReq->pColumns);
  columnDefNodeToField(pStmt->pTags, &pReq->pTags);
  pReq->numOfColumns = LIST_LENGTH(pStmt->pCols);
  pReq->numOfTags = LIST_LENGTH(pStmt->pTags);
  if (pStmt->pOptions->commentNull == false) {
    pReq->pComment = strdup(pStmt->pOptions->comment);
    if (NULL == pReq->pComment) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pReq->commentLen = strlen(pStmt->pOptions->comment);
  } else {
    pReq->commentLen = -1;
  }
  buildRollupFuncs(pStmt->pOptions->pRollupFuncs, &pReq->pFuncs);
  pReq->numOfFuncs = taosArrayGetSize(pReq->pFuncs);

  SName tableName;
  tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tableName), pReq->name);
  int32_t code = collectUseTable(&tableName, pCxt->pTables);
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
    tNameExtractFullName(pTableName, dropReq.name);
    dropReq.igNotExists = ignoreNotExists;
    code = buildCmdMsg(pCxt, TDMT_MND_DROP_STB, (FSerializeFunc)tSerializeSMDropStbReq, &dropReq);
  }
  return code;
}

static int32_t translateDropTable(STranslateContext* pCxt, SDropTableStmt* pStmt) {
  SDropTableClause* pClause = (SDropTableClause*)nodesListGetNode(pStmt->pTables, 0);
  SName             tableName;
  return doTranslateDropSuperTable(
      pCxt, toName(pCxt->pParseCxt->acctId, pClause->dbName, pClause->tableName, &tableName), pClause->ignoreNotExists);
}

static int32_t translateDropSuperTable(STranslateContext* pCxt, SDropSuperTableStmt* pStmt) {
  SName tableName;
  return doTranslateDropSuperTable(pCxt, toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tableName),
                                   pStmt->ignoreNotExists);
}

static int32_t buildAlterSuperTableReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, SMAlterStbReq* pAlterReq) {
  SName tableName;
  tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tableName), pAlterReq->name);
  pAlterReq->alterType = pStmt->alterType;

  if (TSDB_ALTER_TABLE_UPDATE_OPTIONS == pStmt->alterType) {
    //    pAlterReq->ttl = pStmt->pOptions->ttl;
    if (pStmt->pOptions->commentNull == false) {
      pAlterReq->comment = strdup(pStmt->pOptions->comment);
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
    case TSDB_ALTER_TABLE_ADD_TAG:
    case TSDB_ALTER_TABLE_DROP_TAG:
    case TSDB_ALTER_TABLE_ADD_COLUMN:
    case TSDB_ALTER_TABLE_DROP_COLUMN:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES: {
      TAOS_FIELD field = {.type = pStmt->dataType.type, .bytes = calcTypeBytes(pStmt->dataType)};
      strcpy(field.name, pStmt->colName);
      taosArrayPush(pAlterReq->pFields, &field);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME: {
      TAOS_FIELD oldField = {0};
      strcpy(oldField.name, pStmt->colName);
      taosArrayPush(pAlterReq->pFields, &oldField);
      TAOS_FIELD newField = {0};
      strcpy(newField.name, pStmt->newColName);
      taosArrayPush(pAlterReq->pFields, &newField);
      break;
    }
    default:
      break;
  }

  pAlterReq->numOfFields = taosArrayGetSize(pAlterReq->pFields);
  return TSDB_CODE_SUCCESS;
}

static SSchema* getColSchema(STableMeta* pTableMeta, const char* pColName) {
  int32_t numOfFields = getNumOfTags(pTableMeta) + getNumOfColumns(pTableMeta);
  for (int32_t i = 0; i < numOfFields; ++i) {
    SSchema* pSchema = pTableMeta->schema + i;
    if (0 == strcmp(pColName, pSchema->name)) {
      return pSchema;
    }
  }
  return NULL;
}

static SSchema* getTagSchema(STableMeta* pTableMeta, const char* pTagName) {
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

static int32_t checkAlterSuperTableBySchema(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta) {
  SSchema* pTagsSchema = getTableTagSchema(pTableMeta);
  if (getNumOfTags(pTableMeta) == 1 && pTagsSchema->type == TSDB_DATA_TYPE_JSON &&
      (pStmt->alterType == TSDB_ALTER_TABLE_ADD_TAG || pStmt->alterType == TSDB_ALTER_TABLE_DROP_TAG ||
       pStmt->alterType == TSDB_ALTER_TABLE_UPDATE_TAG_BYTES)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ONLY_ONE_JSON_TAG);
  }

  if (TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES == pStmt->alterType ||
      TSDB_ALTER_TABLE_UPDATE_TAG_BYTES == pStmt->alterType) {
    if (TSDB_SUPER_TABLE != pTableMeta->tableType) {
      return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE, "Table is not super table");
    }

    SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
    if (NULL == pSchema) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pStmt->colName);
    } else if (!IS_VAR_DATA_TYPE(pSchema->type) || pSchema->type != pStmt->dataType.type ||
               pSchema->bytes >= calcTypeBytes(pStmt->dataType)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_MODIFY_COL);
    }
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
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  tNameExtractFullName(&name, usedbReq.db);
  int32_t code = getDBVgVersion(pCxt, usedbReq.db, &usedbReq.vgVersion, &usedbReq.dbId, &usedbReq.numOfTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_USE_DB, (FSerializeFunc)tSerializeSUseDbReq, &usedbReq);
  }
  return code;
}

static int32_t translateCreateUser(STranslateContext* pCxt, SCreateUserStmt* pStmt) {
  SCreateUserReq createReq = {0};
  strcpy(createReq.user, pStmt->useName);
  createReq.createType = 0;
  createReq.superUser = 0;
  createReq.sysInfo = pStmt->sysinfo;
  createReq.enable = 1;
  strcpy(createReq.pass, pStmt->password);

  return buildCmdMsg(pCxt, TDMT_MND_CREATE_USER, (FSerializeFunc)tSerializeSCreateUserReq, &createReq);
}

static int32_t translateAlterUser(STranslateContext* pCxt, SAlterUserStmt* pStmt) {
  SAlterUserReq alterReq = {0};
  strcpy(alterReq.user, pStmt->useName);
  alterReq.alterType = pStmt->alterType;
  alterReq.superUser = 0;
  alterReq.enable = pStmt->enable;
  alterReq.sysInfo = pStmt->sysinfo;
  strcpy(alterReq.pass, pStmt->password);
  if (NULL != pCxt->pParseCxt->db) {
    strcpy(alterReq.dbname, pCxt->pParseCxt->db);
  }

  return buildCmdMsg(pCxt, TDMT_MND_ALTER_USER, (FSerializeFunc)tSerializeSAlterUserReq, &alterReq);
}

static int32_t translateDropUser(STranslateContext* pCxt, SDropUserStmt* pStmt) {
  SDropUserReq dropReq = {0};
  strcpy(dropReq.user, pStmt->useName);

  return buildCmdMsg(pCxt, TDMT_MND_DROP_USER, (FSerializeFunc)tSerializeSDropUserReq, &dropReq);
}

static int32_t translateCreateDnode(STranslateContext* pCxt, SCreateDnodeStmt* pStmt) {
  SCreateDnodeReq createReq = {0};
  strcpy(createReq.fqdn, pStmt->fqdn);
  createReq.port = pStmt->port;

  return buildCmdMsg(pCxt, TDMT_MND_CREATE_DNODE, (FSerializeFunc)tSerializeSCreateDnodeReq, &createReq);
}

static int32_t translateDropDnode(STranslateContext* pCxt, SDropDnodeStmt* pStmt) {
  SDropDnodeReq dropReq = {0};
  dropReq.dnodeId = pStmt->dnodeId;
  strcpy(dropReq.fqdn, pStmt->fqdn);
  dropReq.port = pStmt->port;

  return buildCmdMsg(pCxt, TDMT_MND_DROP_DNODE, (FSerializeFunc)tSerializeSDropDnodeReq, &dropReq);
}

static int32_t translateAlterDnode(STranslateContext* pCxt, SAlterDnodeStmt* pStmt) {
  SMCfgDnodeReq cfgReq = {0};
  cfgReq.dnodeId = pStmt->dnodeId;
  strcpy(cfgReq.config, pStmt->config);
  strcpy(cfgReq.value, pStmt->value);

  return buildCmdMsg(pCxt, TDMT_MND_CONFIG_DNODE, (FSerializeFunc)tSerializeSMCfgDnodeReq, &cfgReq);
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
  *pSql = strdup(pCxt->pParseCxt->pSql);
  if (NULL == *pSql) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pLen = pCxt->pParseCxt->sqlLen + 1;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildSampleAstInfoByIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt, SSampleAstInfo* pInfo) {
  pInfo->pDbName = pStmt->dbName;
  pInfo->pTableName = pStmt->tableName;
  pInfo->pFuncs = nodesCloneList(pStmt->pOptions->pFuncs);
  pInfo->pInterval = nodesCloneNode(pStmt->pOptions->pInterval);
  pInfo->pOffset = nodesCloneNode(pStmt->pOptions->pOffset);
  pInfo->pSliding = nodesCloneNode(pStmt->pOptions->pSliding);
  if (NULL == pInfo->pFuncs || NULL == pInfo->pInterval ||
      (NULL != pStmt->pOptions->pOffset && NULL == pInfo->pOffset) ||
      (NULL != pStmt->pOptions->pSliding && NULL == pInfo->pSliding)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getSmaIndexAst(STranslateContext* pCxt, SCreateIndexStmt* pStmt, char** pAst, int32_t* pLen,
                              char** pExpr, int32_t* pExprLen) {
  SSampleAstInfo info = {0};
  int32_t        code = buildSampleAstInfoByIndex(pCxt, pStmt, &info);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildSampleAst(pCxt, &info, pAst, pLen, pExpr, pExprLen);
  }
  clearSampleAstInfo(&info);
  return code;
}

static int32_t buildCreateSmaReq(STranslateContext* pCxt, SCreateIndexStmt* pStmt, SMCreateSmaReq* pReq) {
  SName name;
  tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pStmt->indexDbName, pStmt->indexName, &name), pReq->name);
  memset(&name, 0, sizeof(SName));
  tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name), pReq->stb);
  pReq->igExists = pStmt->ignoreExists;
  pReq->interval = ((SValueNode*)pStmt->pOptions->pInterval)->datum.i;
  pReq->intervalUnit = ((SValueNode*)pStmt->pOptions->pInterval)->unit;
  pReq->offset = (NULL != pStmt->pOptions->pOffset ? ((SValueNode*)pStmt->pOptions->pOffset)->datum.i : 0);
  pReq->sliding =
      (NULL != pStmt->pOptions->pSliding ? ((SValueNode*)pStmt->pOptions->pSliding)->datum.i : pReq->interval);
  pReq->slidingUnit =
      (NULL != pStmt->pOptions->pSliding ? ((SValueNode*)pStmt->pOptions->pSliding)->unit : pReq->intervalUnit);
  if (NULL != pStmt->pOptions->pStreamOptions) {
    SStreamOptions* pStreamOpt = (SStreamOptions*)pStmt->pOptions->pStreamOptions;
    pReq->maxDelay = (NULL != pStreamOpt->pDelay ? ((SValueNode*)pStreamOpt->pDelay)->datum.i : -1);
    pReq->watermark = (NULL != pStreamOpt->pWatermark ? ((SValueNode*)pStreamOpt->pWatermark)->datum.i
                                                      : TSDB_DEFAULT_ROLLUP_WATERMARK);
    if (pReq->watermark < TSDB_MIN_ROLLUP_WATERMARK) {
      pReq->watermark = TSDB_MIN_ROLLUP_WATERMARK;
    }
    if (pReq->watermark > TSDB_MAX_ROLLUP_WATERMARK) {
      pReq->watermark = TSDB_MAX_ROLLUP_WATERMARK;
    }
  }

  int32_t code = getSmaIndexDstVgId(pCxt, pStmt->dbName, pStmt->tableName, &pReq->dstVgId);
  if (TSDB_CODE_SUCCESS == code) {
    code = getSmaIndexSql(pCxt, &pReq->sql, &pReq->sqlLen);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = getSmaIndexAst(pCxt, pStmt, &pReq->ast, &pReq->astLen, &pReq->expr, &pReq->exprLen);
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

  if (TSDB_CODE_SUCCESS == code && NULL != pStmt->pOptions->pStreamOptions) {
    SStreamOptions* pStreamOpt = (SStreamOptions*)pStmt->pOptions->pStreamOptions;
    if (NULL != pStreamOpt->pWatermark) {
      code = doTranslateValue(pCxt, (SValueNode*)pStreamOpt->pWatermark);
    }
    if (TSDB_CODE_SUCCESS == code && NULL != pStreamOpt->pDelay) {
      code = doTranslateValue(pCxt, (SValueNode*)pStreamOpt->pDelay);
    }
  }

  return code;
}

static int32_t translateCreateSmaIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt) {
  SMCreateSmaReq createSmaReq = {0};
  int32_t        code = checkCreateSmaIndex(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateSmaReq(pCxt, pStmt, &createSmaReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_SMA, (FSerializeFunc)tSerializeSMCreateSmaReq, &createSmaReq);
  }
  tFreeSMCreateSmaReq(&createSmaReq);
  return code;
}

static int32_t buildCreateFullTextReq(STranslateContext* pCxt, SCreateIndexStmt* pStmt, SMCreateFullTextReq* pReq) {
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

static int32_t translateCreateIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt) {
  if (INDEX_TYPE_FULLTEXT == pStmt->indexType) {
    return translateCreateFullTextIndex(pCxt, pStmt);
  }
  return translateCreateSmaIndex(pCxt, pStmt);
}

static int32_t translateDropIndex(STranslateContext* pCxt, SDropIndexStmt* pStmt) {
  SMDropSmaReq dropSmaReq = {0};
  SName        name;
  tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pCxt->pParseCxt->db, pStmt->indexName, &name), dropSmaReq.name);
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
  return buildCmdMsg(pCxt, getCreateComponentNodeMsgType(nodeType(pStmt)),
                     (FSerializeFunc)tSerializeSCreateDropMQSBNodeReq, &createReq);
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
  return buildCmdMsg(pCxt, getDropComponentNodeMsgType(nodeType(pStmt)),
                     (FSerializeFunc)tSerializeSCreateDropMQSBNodeReq, &dropReq);
}

static int32_t buildCreateTopicReq(STranslateContext* pCxt, SCreateTopicStmt* pStmt, SCMCreateTopicReq* pReq) {
  SName name;
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->topicName, strlen(pStmt->topicName));
  tNameGetFullDbName(&name, pReq->name);
  pReq->igExists = pStmt->ignoreExists;
  pReq->withMeta = pStmt->withMeta;

  pReq->sql = strdup(pCxt->pParseCxt->pSql);
  if (NULL == pReq->sql) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  if ('\0' != pStmt->subSTbName[0]) {
    pReq->subType = TOPIC_SUB_TYPE__TABLE;
    toName(pCxt->pParseCxt->acctId, pStmt->subDbName, pStmt->subSTbName, &name);
    tNameGetFullDbName(&name, pReq->subDbName);
    tNameExtractFullName(&name, pReq->subStbName);
  } else if ('\0' != pStmt->subDbName[0]) {
    pReq->subType = TOPIC_SUB_TYPE__DB;
    tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->subDbName, strlen(pStmt->subDbName));
    tNameGetFullDbName(&name, pReq->subDbName);
  } else {
    pReq->subType = TOPIC_SUB_TYPE__COLUMN;
    char* dbName = ((SRealTableNode*)(((SSelectStmt*)pStmt->pQuery)->pFromTable))->table.dbName;
    tNameSetDbName(&name, pCxt->pParseCxt->acctId, dbName, strlen(dbName));
    tNameGetFullDbName(&name, pReq->subDbName);
    pCxt->pParseCxt->topicQuery = true;
    code = translateQuery(pCxt, pStmt->pQuery);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesNodeToString(pStmt->pQuery, false, &pReq->ast, NULL);
    }
  }

  return code;
}

static int32_t checkCreateTopic(STranslateContext* pCxt, SCreateTopicStmt* pStmt) {
  if (NULL == pStmt->pQuery) {
    return TSDB_CODE_SUCCESS;
  }

  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt->pQuery)) {
    SSelectStmt* pSelect = (SSelectStmt*)pStmt->pQuery;
    if (!pSelect->isDistinct && QUERY_NODE_REAL_TABLE == nodeType(pSelect->pFromTable) &&
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
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_TOPIC, (FSerializeFunc)tSerializeSCMCreateTopicReq, &createReq);
  }
  tFreeSCMCreateTopicReq(&createReq);
  return code;
}

static int32_t translateDropTopic(STranslateContext* pCxt, SDropTopicStmt* pStmt) {
  SMDropTopicReq dropReq = {0};

  SName name;
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->topicName, strlen(pStmt->topicName));
  tNameGetFullDbName(&name, dropReq.name);
  dropReq.igNotExists = pStmt->ignoreNotExists;

  return buildCmdMsg(pCxt, TDMT_MND_DROP_TOPIC, (FSerializeFunc)tSerializeSMDropTopicReq, &dropReq);
}

static int32_t translateDropCGroup(STranslateContext* pCxt, SDropCGroupStmt* pStmt) {
  SMDropCgroupReq dropReq = {0};

  SName name;
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->topicName, strlen(pStmt->topicName));
  tNameGetFullDbName(&name, dropReq.topic);
  dropReq.igNotExists = pStmt->ignoreNotExists;
  strcpy(dropReq.cgroup, pStmt->cgroup);

  return buildCmdMsg(pCxt, TDMT_MND_MQ_DROP_CGROUP, (FSerializeFunc)tSerializeSMDropCgroupReq, &dropReq);
}

static int32_t translateAlterLocal(STranslateContext* pCxt, SAlterLocalStmt* pStmt) {
  // todo
  return TSDB_CODE_SUCCESS;
}

static int32_t translateExplain(STranslateContext* pCxt, SExplainStmt* pStmt) {
  if (pStmt->analyze) {
    pCxt->pExplainOpt = pStmt->pOptions;
  }
  return translateQuery(pCxt, pStmt->pQuery);
}

static int32_t translateDescribe(STranslateContext* pCxt, SDescribeStmt* pStmt) {
  return refreshGetTableMeta(pCxt, pStmt->dbName, pStmt->tableName, &pStmt->pMeta);
}

static int32_t translateKillConnection(STranslateContext* pCxt, SKillStmt* pStmt) {
  SKillConnReq killReq = {0};
  killReq.connId = pStmt->targetId;
  return buildCmdMsg(pCxt, TDMT_MND_KILL_CONN, (FSerializeFunc)tSerializeSKillConnReq, &killReq);
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
         !isPartitionByTbname(pSelect->pPartitionByList);
}

static bool crossTableWithUdaf(SSelectStmt* pSelect) {
  return pSelect->hasUdaf && TSDB_SUPER_TABLE == ((SRealTableNode*)pSelect->pFromTable)->pMeta->tableType &&
         !isPartitionByTbname(pSelect->pPartitionByList);
}

static int32_t checkCreateStream(STranslateContext* pCxt, SCreateStreamStmt* pStmt) {
  if (NULL != pStmt->pOptions->pWatermark &&
      (DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pStmt->pOptions->pWatermark))) {
    return pCxt->errCode;
  }

  if (NULL != pStmt->pOptions->pDelay &&
      (DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pStmt->pOptions->pDelay))) {
    return pCxt->errCode;
  }

  if (NULL == pStmt->pQuery) {
    return TSDB_CODE_SUCCESS;
  }

  if (QUERY_NODE_SELECT_STMT != nodeType(pStmt->pQuery) ||
      QUERY_NODE_REAL_TABLE != nodeType(((SSelectStmt*)pStmt->pQuery)->pFromTable)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY, "Unsupported stream query");
  }

  return TSDB_CODE_SUCCESS;
}

static void getSourceDatabase(SNode* pStmt, int32_t acctId, char* pDbFName) {
  SName name = {.type = TSDB_DB_NAME_T, .acctId = acctId};
  strcpy(name.dbname, ((SRealTableNode*)(((SSelectStmt*)pStmt)->pFromTable))->table.dbName);
  tNameGetFullDbName(&name, pDbFName);
}

static int32_t addWstartTsToCreateStreamQuery(SNode* pStmt) {
  SSelectStmt* pSelect = (SSelectStmt*)pStmt;
  SNode*       pProj = nodesListGetNode(pSelect->pProjectionList, 0);
  if (NULL == pSelect->pWindow ||
      (QUERY_NODE_FUNCTION == nodeType(pProj) && 0 == strcmp("_wstart", ((SFunctionNode*)pProj)->functionName))) {
    return TSDB_CODE_SUCCESS;
  }
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pFunc->functionName, "_wstart");
  strcpy(pFunc->node.aliasName, pFunc->functionName);
  int32_t code = nodesListPushFront(pSelect->pProjectionList, (SNode*)pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pFunc);
  }
  return code;
}

static int32_t checkStreamQuery(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (TSDB_DATA_TYPE_TIMESTAMP != ((SExprNode*)nodesListGetNode(pSelect->pProjectionList, 0))->resType.type ||
      !pSelect->isTimeLineResult || crossTableWithoutAggOper(pSelect) || NULL != pSelect->pOrderByList ||
      crossTableWithUdaf(pSelect)) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY, "Unsupported stream query");
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildCreateStreamQuery(STranslateContext* pCxt, SNode* pStmt, SCMCreateStreamReq* pReq) {
  pCxt->createStream = true;
  int32_t code = addWstartTsToCreateStreamQuery(pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateQuery(pCxt, pStmt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkStreamQuery(pCxt, (SSelectStmt*)pStmt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    getSourceDatabase(pStmt, pCxt->pParseCxt->acctId, pReq->sourceDB);
    code = nodesNodeToString(pStmt, false, &pReq->ast, NULL);
  }
  return code;
}

static int32_t buildCreateStreamReq(STranslateContext* pCxt, SCreateStreamStmt* pStmt, SCMCreateStreamReq* pReq) {
  pReq->igExists = pStmt->ignoreExists;

  SName name;
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->streamName, strlen(pStmt->streamName));
  tNameGetFullDbName(&name, pReq->name);

  if ('\0' != pStmt->targetTabName[0]) {
    strcpy(name.dbname, pStmt->targetDbName);
    strcpy(name.tname, pStmt->targetTabName);
    tNameExtractFullName(&name, pReq->targetStbFullName);
  }

  int32_t code = buildCreateStreamQuery(pCxt, pStmt->pQuery, pReq);
  if (TSDB_CODE_SUCCESS == code) {
    pReq->sql = strdup(pCxt->pParseCxt->pSql);
    if (NULL == pReq->sql) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    pReq->triggerType = pStmt->pOptions->triggerType;
    pReq->maxDelay = (NULL != pStmt->pOptions->pDelay ? ((SValueNode*)pStmt->pOptions->pDelay)->datum.i : 0);
    pReq->watermark = (NULL != pStmt->pOptions->pWatermark ? ((SValueNode*)pStmt->pOptions->pWatermark)->datum.i : 0);
    pReq->igExpired = pStmt->pOptions->ignoreExpired;
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
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_STREAM, (FSerializeFunc)tSerializeSCMCreateStreamReq, &createReq);
  }

  tFreeSCMCreateStreamReq(&createReq);
  return code;
}

static int32_t translateDropStream(STranslateContext* pCxt, SDropStreamStmt* pStmt) {
  SMDropStreamReq dropReq = {0};
  SName           name;
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->streamName, strlen(pStmt->streamName));
  tNameGetFullDbName(&name, dropReq.name);
  dropReq.igNotExists = pStmt->ignoreNotExists;
  return buildCmdMsg(pCxt, TDMT_MND_DROP_STREAM, (FSerializeFunc)tSerializeSMDropStreamReq, &dropReq);
}

static int32_t readFromFile(char* pName, int32_t* len, char** buf) {
  int64_t filesize = 0;
  if (taosStatFile(pName, &filesize, NULL) < 0) {
    return TAOS_SYSTEM_ERROR(errno);
  }

  *len = filesize;

  if (*len <= 0) {
    return TSDB_CODE_TSC_FILE_EMPTY;
  }

  *buf = taosMemoryCalloc(1, *len);
  if (*buf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  TdFilePtr tfile = taosOpenFile(pName, O_RDONLY | O_BINARY);
  if (NULL == tfile) {
    taosMemoryFreeClear(*buf);
    return TAOS_SYSTEM_ERROR(errno);
  }

  int64_t s = taosReadFile(tfile, *buf, *len);
  if (s != *len) {
    taosCloseFile(&tfile);
    taosMemoryFreeClear(*buf);
    return TSDB_CODE_TSC_APP_ERROR;
  }
  taosCloseFile(&tfile);
  return TSDB_CODE_SUCCESS;
}

static int32_t translateCreateFunction(STranslateContext* pCxt, SCreateFunctionStmt* pStmt) {
  if (fmIsBuiltinFunc(pStmt->funcName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FUNCTION_NAME);
  }
  SCreateFuncReq req = {0};
  strcpy(req.name, pStmt->funcName);
  req.igExists = pStmt->ignoreExists;
  req.funcType = pStmt->isAgg ? TSDB_FUNC_TYPE_AGGREGATE : TSDB_FUNC_TYPE_SCALAR;
  req.scriptType = TSDB_FUNC_SCRIPT_BIN_LIB;
  req.outputType = pStmt->outputDt.type;
  req.outputLen = pStmt->outputDt.bytes;
  req.bufSize = pStmt->bufSize;
  int32_t code = readFromFile(pStmt->libraryPath, &req.codeLen, &req.pCode);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_FUNC, (FSerializeFunc)tSerializeSCreateFuncReq, &req);
  }
  return code;
}

static int32_t translateDropFunction(STranslateContext* pCxt, SDropFunctionStmt* pStmt) {
  SDropFuncReq req = {0};
  strcpy(req.name, pStmt->funcName);
  req.igNotExists = pStmt->ignoreNotExists;
  return buildCmdMsg(pCxt, TDMT_MND_DROP_FUNC, (FSerializeFunc)tSerializeSDropFuncReq, &req);
}

static int32_t translateGrant(STranslateContext* pCxt, SGrantStmt* pStmt) {
  SAlterUserReq req = {0};
  if (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_ALL) ||
      (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_READ) &&
       PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_WRITE))) {
    req.alterType = TSDB_ALTER_USER_ADD_ALL_DB;
  } else if (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_READ)) {
    req.alterType = TSDB_ALTER_USER_ADD_READ_DB;
  } else if (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_WRITE)) {
    req.alterType = TSDB_ALTER_USER_ADD_WRITE_DB;
  }
  strcpy(req.user, pStmt->userName);
  sprintf(req.dbname, "%d.%s", pCxt->pParseCxt->acctId, pStmt->dbName);
  return buildCmdMsg(pCxt, TDMT_MND_ALTER_USER, (FSerializeFunc)tSerializeSAlterUserReq, &req);
}

static int32_t translateRevoke(STranslateContext* pCxt, SRevokeStmt* pStmt) {
  SAlterUserReq req = {0};
  if (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_ALL) ||
      (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_READ) &&
       PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_WRITE))) {
    req.alterType = TSDB_ALTER_USER_REMOVE_ALL_DB;
  } else if (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_READ)) {
    req.alterType = TSDB_ALTER_USER_REMOVE_READ_DB;
  } else if (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_WRITE)) {
    req.alterType = TSDB_ALTER_USER_REMOVE_WRITE_DB;
  }
  strcpy(req.user, pStmt->userName);
  sprintf(req.dbname, "%d.%s", pCxt->pParseCxt->acctId, pStmt->dbName);
  return buildCmdMsg(pCxt, TDMT_MND_ALTER_USER, (FSerializeFunc)tSerializeSAlterUserReq, &req);
}

static int32_t translateBalanceVgroup(STranslateContext* pCxt, SBalanceVgroupStmt* pStmt) {
  SBalanceVgroupReq req = {0};
  return buildCmdMsg(pCxt, TDMT_MND_BALANCE_VGROUP, (FSerializeFunc)tSerializeSBalanceVgroupReq, &req);
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
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return getDBCfg(pCxt, pStmt->dbName, (SDbCfgInfo*)pStmt->pCfg);
}

static int32_t translateShowCreateTable(STranslateContext* pCxt, SShowCreateTableStmt* pStmt) {
  pStmt->pDbCfg = taosMemoryCalloc(1, sizeof(SDbCfgInfo));
  if (NULL == pStmt->pDbCfg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = getDBCfg(pCxt, pStmt->dbName, (SDbCfgInfo*)pStmt->pDbCfg);
  if (TSDB_CODE_SUCCESS == code) {
    SName name;
    toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &name);
    code = getTableCfg(pCxt, &name, (STableCfg**)&pStmt->pTableCfg);
  }
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
    case QUERY_NODE_KILL_CONNECTION_STMT:
      code = translateKillConnection(pCxt, (SKillStmt*)pNode);
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
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNode*  pNode;
  int32_t index = 0;
  FOREACH(pNode, pProjections) {
    SExprNode* pExpr = (SExprNode*)pNode;
    if (TSDB_DATA_TYPE_NULL == pExpr->resType.type) {
      (*pSchema)[index].type = TSDB_DATA_TYPE_VARCHAR;
      (*pSchema)[index].bytes = 0;
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
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = TSDB_EXPLAIN_RESULT_ROW_SIZE;
  strcpy((*pSchema)[0].name, TSDB_EXPLAIN_RESULT_COLUMN_NAME);
  return TSDB_CODE_SUCCESS;
}

static int32_t extractDescribeResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = DESCRIBE_RESULT_COLS;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return TSDB_CODE_OUT_OF_MEMORY;
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

  return TSDB_CODE_SUCCESS;
}

static int32_t extractShowCreateDatabaseResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = 2;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = TSDB_TABLE_NAME_LEN;
  strcpy((*pSchema)[0].name, "Table");

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = TSDB_MAX_BINARY_LEN;
  strcpy((*pSchema)[1].name, "Create Table");

  return TSDB_CODE_SUCCESS;
}

static int32_t extractShowVariablesResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = 2;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = TSDB_CONFIG_OPTION_LEN;
  strcpy((*pSchema)[0].name, "name");

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = TSDB_CONFIG_VALUE_LEN;
  strcpy((*pSchema)[1].name, "value");

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
    case QUERY_NODE_DESCRIBE_STMT:
      return extractDescribeResultSchema(numOfCols, pSchema);
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
      return extractShowCreateDatabaseResultSchema(numOfCols, pSchema);
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      return extractShowCreateTableResultSchema(numOfCols, pSchema);
    case QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT:
    case QUERY_NODE_SHOW_VARIABLES_STMT:
      return extractShowVariablesResultSchema(numOfCols, pSchema);
    default:
      break;
  }

  return TSDB_CODE_FAILED;
}

static SNode* createStarCol() {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  strcpy(pCol->colName, "*");
  return (SNode*)pCol;
}

static SNode* createProjectCol(const char* pProjCol) {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  strcpy(pCol->colName, pProjCol);
  return (SNode*)pCol;
}

static SNodeList* createProjectCols(int32_t ncols, const char* const pCols[]) {
  SNodeList* pProjections = NULL;
  if (ncols <= 0) {
    nodesListMakeStrictAppend(&pProjections, createStarCol());
    return pProjections;
  }
  for (int32_t i = 0; i < ncols; ++i) {
    int32_t code = nodesListMakeStrictAppend(&pProjections, createProjectCol(pCols[i]));
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(pProjections);
      return NULL;
    }
  }
  return pProjections;
}

static int32_t createSimpleSelectStmt(const char* pDb, const char* pTable, int32_t numOfProjs,
                                      const char* const pProjCol[], SSelectStmt** pStmt) {
  SSelectStmt* pSelect = (SSelectStmt*)nodesMakeNode(QUERY_NODE_SELECT_STMT);
  if (NULL == pSelect) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  sprintf(pSelect->stmtName, "%p", pSelect);

  SRealTableNode* pRealTable = (SRealTableNode*)nodesMakeNode(QUERY_NODE_REAL_TABLE);
  if (NULL == pRealTable) {
    nodesDestroyNode((SNode*)pSelect);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pRealTable->table.dbName, pDb);
  strcpy(pRealTable->table.tableName, pTable);
  strcpy(pRealTable->table.tableAlias, pTable);
  pSelect->pFromTable = (SNode*)pRealTable;

  pSelect->pProjectionList = createProjectCols(numOfProjs, pProjCol);
  if (NULL == pSelect->pProjectionList) {
    nodesDestroyNode((SNode*)pSelect);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pStmt = pSelect;

  return TSDB_CODE_SUCCESS;
}

static int32_t createSelectStmtForShow(ENodeType showType, SSelectStmt** pStmt) {
  const SSysTableShowAdapter* pShow = &sysTableShowAdapter[showType - SYSTABLE_SHOW_TYPE_OFFSET];
  return createSimpleSelectStmt(pShow->pDbName, pShow->pTableName, pShow->numOfShowCols, pShow->pShowCols, pStmt);
}

static int32_t createSelectStmtForShowTableDist(SShowTableDistributedStmt* pStmt, SSelectStmt** pOutput) {
  return createSimpleSelectStmt(pStmt->dbName, pStmt->tableName, 0, NULL, pOutput);
}

static int32_t createOperatorNode(EOperatorType opType, const char* pColName, SNode* pRight, SNode** pOp) {
  if (NULL == pRight) {
    return TSDB_CODE_SUCCESS;
  }

  SOperatorNode* pOper = (SOperatorNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
  if (NULL == pOper) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pOper->opType = opType;
  pOper->pLeft = nodesMakeNode(QUERY_NODE_COLUMN);
  pOper->pRight = nodesCloneNode(pRight);
  if (NULL == pOper->pLeft || NULL == pOper->pRight) {
    nodesDestroyNode((SNode*)pOper);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(((SColumnNode*)pOper->pLeft)->colName, pColName);

  *pOp = (SNode*)pOper;
  return TSDB_CODE_SUCCESS;
}

static const char* getTbNameColName(ENodeType type) {
  return (QUERY_NODE_SHOW_STABLES_STMT == type ? "stable_name" : "table_name");
}

static int32_t createLogicCondNode(SNode* pCond1, SNode* pCond2, SNode** pCond) {
  SLogicConditionNode* pCondition = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  if (NULL == pCondition) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCondition->condType = LOGIC_COND_TYPE_AND;
  pCondition->pParameterList = nodesMakeList();
  if (NULL == pCondition->pParameterList) {
    nodesDestroyNode((SNode*)pCondition);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (TSDB_CODE_SUCCESS != nodesListAppend(pCondition->pParameterList, pCond1) ||
      TSDB_CODE_SUCCESS != nodesListAppend(pCondition->pParameterList, pCond2)) {
    nodesDestroyNode((SNode*)pCondition);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pCond = (SNode*)pCondition;
  return TSDB_CODE_SUCCESS;
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
    if (TSDB_CODE_SUCCESS != createLogicCondNode(pDbCond, pTbCond, &pSelect->pWhere)) {
      nodesDestroyNode(pDbCond);
      nodesDestroyNode(pTbCond);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    pSelect->pWhere = (NULL == pDbCond ? pTbCond : pDbCond);
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
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  }
  return code;
}

static int32_t rewriteShowDnodeVariables(STranslateContext* pCxt, SQuery* pQuery) {
  SSelectStmt* pStmt = NULL;
  int32_t      code = createSelectStmtForShow(nodeType(pQuery->pRoot), &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = createOperatorNode(OP_TYPE_EQUAL, "dnode_id", ((SShowDnodeVariablesStmt*)pQuery->pRoot)->pDnodeId,
                              &pStmt->pWhere);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  }
  return code;
}

static SNode* createBlockDistInfoFunc() {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return NULL;
  }

  strcpy(pFunc->functionName, "_block_dist_info");
  strcpy(pFunc->node.aliasName, "_block_dist_info");
  return (SNode*)pFunc;
}

static SNode* createBlockDistFunc() {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return NULL;
  }

  strcpy(pFunc->functionName, "_block_dist");
  strcpy(pFunc->node.aliasName, "_block_dist");
  if (TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pFunc->pParameterList, createBlockDistInfoFunc())) {
    nodesDestroyNode((SNode*)pFunc);
    return NULL;
  }
  return (SNode*)pFunc;
}

static int32_t rewriteShowTableDist(STranslateContext* pCxt, SQuery* pQuery) {
  SSelectStmt* pStmt = NULL;
  int32_t      code = createSelectStmtForShowTableDist((SShowTableDistributedStmt*)pQuery->pRoot, &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    NODES_DESTORY_LIST(pStmt->pProjectionList);
    code = nodesListMakeStrictAppend(&pStmt->pProjectionList, createBlockDistFunc());
  }
  if (TSDB_CODE_SUCCESS == code) {
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

static void destroyCreateTbReq(SVCreateTbReq* pReq) {
  taosMemoryFreeClear(pReq->name);
  taosMemoryFreeClear(pReq->comment);
  taosMemoryFreeClear(pReq->ntb.schemaRow.pSchema);
}

static int32_t buildNormalTableBatchReq(int32_t acctId, const SCreateTableStmt* pStmt, const SVgroupInfo* pVgroupInfo,
                                        SVgroupCreateTableBatch* pBatch) {
  char  dbFName[TSDB_DB_FNAME_LEN] = {0};
  SName name = {.type = TSDB_DB_NAME_T, .acctId = acctId};
  strcpy(name.dbname, pStmt->dbName);
  tNameGetFullDbName(&name, dbFName);

  SVCreateTbReq req = {0};
  req.type = TD_NORMAL_TABLE;
  req.name = strdup(pStmt->tableName);
  req.ttl = pStmt->pOptions->ttl;
  if (pStmt->pOptions->commentNull == false) {
    req.comment = strdup(pStmt->pOptions->comment);
    if (NULL == req.comment) {
      destroyCreateTbReq(&req);
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
    destroyCreateTbReq(&req);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (pStmt->ignoreExists) {
    req.flags |= TD_CREATE_IF_NOT_EXISTS;
  }
  SNode*   pCol;
  col_id_t index = 0;
  FOREACH(pCol, pStmt->pCols) {
    toSchema((SColumnDefNode*)pCol, index + 1, req.ntb.schemaRow.pSchema + index);
    ++index;
  }
  pBatch->info = *pVgroupInfo;
  strcpy(pBatch->dbName, pStmt->dbName);
  pBatch->req.pArray = taosArrayInit(1, sizeof(struct SVCreateTbReq));
  if (NULL == pBatch->req.pArray) {
    destroyCreateTbReq(&req);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosArrayPush(pBatch->req.pArray, &req);

  return TSDB_CODE_SUCCESS;
}

static int32_t serializeVgroupCreateTableBatch(SVgroupCreateTableBatch* pTbBatch, SArray* pBufArray) {
  int      tlen;
  SEncoder coder = {0};

  int32_t ret = 0;
  tEncodeSize(tEncodeSVCreateTbBatchReq, &pTbBatch->req, tlen, ret);
  tlen += sizeof(SMsgHead);
  void* buf = taosMemoryMalloc(tlen);
  if (NULL == buf) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  ((SMsgHead*)buf)->vgId = htonl(pTbBatch->info.vgId);
  ((SMsgHead*)buf)->contLen = htonl(tlen);
  void* pBuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  tEncoderInit(&coder, pBuf, tlen - sizeof(SMsgHead));
  tEncodeSVCreateTbBatchReq(&coder, &pTbBatch->req);
  tEncoderClear(&coder);

  SVgDataBlocks* pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
  if (NULL == pVgData) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pVgData->vg = pTbBatch->info;
  pVgData->pData = buf;
  pVgData->size = tlen;
  pVgData->numOfTables = (int32_t)taosArrayGetSize(pTbBatch->req.pArray);
  taosArrayPush(pBufArray, &pVgData);

  return TSDB_CODE_SUCCESS;
}

static void destroyCreateTbReqBatch(void* data) {
  SVgroupCreateTableBatch* pTbBatch = (SVgroupCreateTableBatch*)data;
  size_t                   size = taosArrayGetSize(pTbBatch->req.pArray);
  for (int32_t i = 0; i < size; ++i) {
    SVCreateTbReq* pTableReq = taosArrayGet(pTbBatch->req.pArray, i);
    taosMemoryFreeClear(pTableReq->name);
    taosMemoryFreeClear(pTableReq->comment);

    if (pTableReq->type == TSDB_NORMAL_TABLE) {
      taosMemoryFreeClear(pTableReq->ntb.schemaRow.pSchema);
    } else if (pTableReq->type == TSDB_CHILD_TABLE) {
      taosMemoryFreeClear(pTableReq->ctb.pTag);
      taosMemoryFreeClear(pTableReq->ctb.name);
      taosArrayDestroy(pTableReq->ctb.tagName);
    }
  }

  taosArrayDestroy(pTbBatch->req.pArray);
}

int32_t rewriteToVnodeModifyOpStmt(SQuery* pQuery, SArray* pBufArray) {
  SVnodeModifOpStmt* pNewStmt = (SVnodeModifOpStmt*)nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT);
  if (pNewStmt == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
  SName       name;
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

static void addCreateTbReqIntoVgroup(int32_t acctId, SHashObj* pVgroupHashmap, SCreateSubTableClause* pStmt,
                                     const STag* pTag, uint64_t suid, const char* sTableNmae, SVgroupInfo* pVgInfo,
                                     SArray* tagName, uint8_t tagNum) {
  //  char  dbFName[TSDB_DB_FNAME_LEN] = {0};
  //  SName name = {.type = TSDB_DB_NAME_T, .acctId = acctId};
  //  strcpy(name.dbname, pStmt->dbName);
  //  tNameGetFullDbName(&name, dbFName);

  struct SVCreateTbReq req = {0};
  req.type = TD_CHILD_TABLE;
  req.name = strdup(pStmt->tableName);
  req.ttl = pStmt->pOptions->ttl;
  if (pStmt->pOptions->commentNull == false) {
    req.comment = strdup(pStmt->pOptions->comment);
    req.commentLen = strlen(pStmt->pOptions->comment);
  } else {
    req.commentLen = -1;
  }
  req.ctb.suid = suid;
  req.ctb.tagNum = tagNum;
  req.ctb.name = strdup(sTableNmae);
  req.ctb.pTag = (uint8_t*)pTag;
  req.ctb.tagName = taosArrayDup(tagName);
  if (pStmt->ignoreExists) {
    req.flags |= TD_CREATE_IF_NOT_EXISTS;
  }

  SVgroupCreateTableBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId));
  if (pTableBatch == NULL) {
    SVgroupCreateTableBatch tBatch = {0};
    tBatch.info = *pVgInfo;
    strcpy(tBatch.dbName, pStmt->dbName);

    tBatch.req.pArray = taosArrayInit(4, sizeof(struct SVCreateTbReq));
    taosArrayPush(tBatch.req.pArray, &req);

    taosHashPut(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId), &tBatch, sizeof(tBatch));
  } else {  // add to the correct vgroup
    taosArrayPush(pTableBatch->req.pArray, &req);
  }
}

static SDataType schemaToDataType(uint8_t precision, SSchema* pSchema) {
  SDataType dt = {.type = pSchema->type, .bytes = pSchema->bytes, .precision = precision, .scale = 0};
  return dt;
}

static int32_t createCastFuncForTag(STranslateContext* pCxt, SNode* pNode, SDataType dt, SNode** pCast) {
  SNode* pExpr = nodesCloneNode(pNode);
  if (NULL == pExpr) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = translateExpr(pCxt, &pExpr);
  if (TSDB_CODE_SUCCESS == code) {
    code = createCastFunc(pCxt, pExpr, dt, pCast);
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pExpr);
  }
  return code;
}

static int32_t createTagValFromExpr(STranslateContext* pCxt, SDataType targetDt, SNode* pNode, SValueNode** pVal) {
  SNode*  pCast = NULL;
  int32_t code = createCastFuncForTag(pCxt, pNode, targetDt, &pCast);
  SNode*  pNew = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = scalarCalculateConstants(pCast, &pNew);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pCast = pNew;
    if (QUERY_NODE_VALUE != nodeType(pCast)) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)pNode)->aliasName);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pVal = (SValueNode*)pCast;
  } else {
    nodesDestroyNode(pCast);
  }
  return code;
}

static int32_t createTagValFromVal(STranslateContext* pCxt, SDataType targetDt, SNode* pNode, SValueNode** pVal) {
  SValueNode* pTempVal = (SValueNode*)nodesCloneNode(pNode);
  if (NULL == pTempVal) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (DEAL_RES_ERROR == translateValueImpl(pCxt, pTempVal, targetDt, true)) {
    nodesDestroyNode((SNode*)pTempVal);
    return pCxt->errCode;
  }
  *pVal = pTempVal;
  return TSDB_CODE_SUCCESS;
}

static int32_t createTagVal(STranslateContext* pCxt, uint8_t precision, SSchema* pSchema, SNode* pNode,
                            SValueNode** pVal) {
  if (QUERY_NODE_VALUE == nodeType(pNode)) {
    return createTagValFromVal(pCxt, schemaToDataType(precision, pSchema), pNode, pVal);
  } else {
    return createTagValFromExpr(pCxt, schemaToDataType(precision, pSchema), pNode, pVal);
  }
}

static int32_t buildJsonTagVal(STranslateContext* pCxt, SSchema* pTagSchema, SValueNode* pVal, SArray* pTagArray,
                               STag** ppTag) {
  if (pVal->literal && strlen(pVal->literal) > (TSDB_MAX_JSON_TAG_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
    return buildSyntaxErrMsg(&pCxt->msgBuf, "json string too long than 4095", pVal->literal);
  }

  return parseJsontoTagData(pVal->literal, pTagArray, ppTag, &pCxt->msgBuf);
}

static int32_t buildNormalTagVal(STranslateContext* pCxt, SSchema* pTagSchema, SValueNode* pVal, SArray* pTagArray) {
  if (pVal->node.resType.type != TSDB_DATA_TYPE_NULL) {
    void*   nodeVal = nodesGetValueFromNode(pVal);
    STagVal val = {.cid = pTagSchema->colId, .type = pTagSchema->type};
    //    strcpy(val.colName, pTagSchema->name);
    if (IS_VAR_DATA_TYPE(pTagSchema->type)) {
      val.pData = varDataVal(nodeVal);
      val.nData = varDataLen(nodeVal);
    } else {
      memcpy(&val.i64, nodeVal, pTagSchema->bytes);
    }
    taosArrayPush(pTagArray, &val);
  }
  return TSDB_CODE_SUCCESS;
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

  bool       isJson = false;
  SNodeList* pVals = NULL;
  SNode *    pTag = NULL, *pNode = NULL;
  FORBOTH(pTag, pStmt->pSpecificTags, pNode, pStmt->pValsOfTags) {
    SColumnNode* pCol = (SColumnNode*)pTag;
    SSchema*     pSchema = getTagSchema(pSuperTableMeta, pCol->colName);
    if (NULL == pSchema) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAG_NAME, pCol->colName);
    }
    SValueNode* pVal = NULL;
    if (TSDB_CODE_SUCCESS == code) {
      code = createTagVal(pCxt, pSuperTableMeta->tableInfo.precision, pSchema, pNode, &pVal);
    }
    if (TSDB_CODE_SUCCESS == code) {
      if (pSchema->type == TSDB_DATA_TYPE_JSON) {
        isJson = true;
        code = buildJsonTagVal(pCxt, pSchema, pVal, pTagArray, ppTag);
        taosArrayPush(tagName, pCol->colName);
      } else if (pVal->node.resType.type != TSDB_DATA_TYPE_NULL) {
        code = buildNormalTagVal(pCxt, pSchema, pVal, pTagArray);
        taosArrayPush(tagName, pCol->colName);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeAppend(&pVals, (SNode*)pVal);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code && !isJson) {
    code = tTagNew(pTagArray, 1, false, ppTag);
  }

  nodesDestroyList(pVals);
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

  bool       isJson = false;
  int32_t    index = 0;
  SSchema*   pTagSchemas = getTableTagSchema(pSuperTableMeta);
  SNodeList* pVals = NULL;
  SNode*     pNode;
  FOREACH(pNode, pStmt->pValsOfTags) {
    SValueNode* pVal = NULL;
    SSchema*    pTagSchema = pTagSchemas + index;
    code = createTagVal(pCxt, pSuperTableMeta->tableInfo.precision, pTagSchema, pNode, &pVal);
    if (TSDB_CODE_SUCCESS == code) {
      if (pTagSchema->type == TSDB_DATA_TYPE_JSON) {
        isJson = true;
        code = buildJsonTagVal(pCxt, pTagSchema, pVal, pTagArray, ppTag);
        taosArrayPush(tagName, pTagSchema->name);
      } else if (pVal->node.resType.type != TSDB_DATA_TYPE_NULL && !pVal->isNull) {
        char*   tmpVal = nodesGetValueFromNode(pVal);
        STagVal val = {.cid = pTagSchema->colId, .type = pTagSchema->type};
        //        strcpy(val.colName, pTagSchema->name);
        if (IS_VAR_DATA_TYPE(pTagSchema->type)) {
          val.pData = varDataVal(tmpVal);
          val.nData = varDataLen(tmpVal);
        } else {
          memcpy(&val.i64, tmpVal, pTagSchema->bytes);
        }
        taosArrayPush(pTagArray, &val);
        taosArrayPush(tagName, pTagSchema->name);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeAppend(&pVals, (SNode*)pVal);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    ++index;
  }

  if (TSDB_CODE_SUCCESS == code && !isJson) {
    code = tTagNew(pTagArray, 1, false, ppTag);
  }

  nodesDestroyList(pVals);
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
    SName name;
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
    addCreateTbReqIntoVgroup(pCxt->pParseCxt->acctId, pVgroupHashmap, pStmt, pTag, pSuperTableMeta->uid,
                             pStmt->useTableName, &info, tagName, pSuperTableMeta->tableInfo.numOfTags);
  }

  taosArrayDestroy(tagName);
  taosMemoryFreeClear(pSuperTableMeta);
  return code;
}

SArray* serializeVgroupsCreateTableBatch(SHashObj* pVgroupHashmap) {
  SArray* pBufArray = taosArrayInit(taosHashGetSize(pVgroupHashmap), sizeof(void*));
  if (NULL == pBufArray) {
    return NULL;
  }

  int32_t                  code = TSDB_CODE_SUCCESS;
  SVgroupCreateTableBatch* pTbBatch = NULL;
  do {
    pTbBatch = taosHashIterate(pVgroupHashmap, pTbBatch);
    if (pTbBatch == NULL) {
      break;
    }

    serializeVgroupCreateTableBatch(pTbBatch, pBufArray);
  } while (true);

  return pBufArray;
}

static int32_t rewriteCreateMultiTable(STranslateContext* pCxt, SQuery* pQuery) {
  SCreateMultiTableStmt* pStmt = (SCreateMultiTableStmt*)pQuery->pRoot;

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

  SArray* pBufArray = serializeVgroupsCreateTableBatch(pVgroupHashmap);
  taosHashCleanup(pVgroupHashmap);
  if (NULL == pBufArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
}

typedef struct SVgroupDropTableBatch {
  SVDropTbBatchReq req;
  SVgroupInfo      info;
  char             dbName[TSDB_DB_NAME_LEN];
} SVgroupDropTableBatch;

static void addDropTbReqIntoVgroup(SHashObj* pVgroupHashmap, SDropTableClause* pClause, SVgroupInfo* pVgInfo) {
  SVDropTbReq            req = {.name = pClause->tableName, .igNotExists = pClause->ignoreNotExists};
  SVgroupDropTableBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId));
  if (NULL == pTableBatch) {
    SVgroupDropTableBatch tBatch = {0};
    tBatch.info = *pVgInfo;
    tBatch.req.pArray = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SVDropTbReq));
    taosArrayPush(tBatch.req.pArray, &req);

    taosHashPut(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId), &tBatch, sizeof(tBatch));
  } else {  // add to the correct vgroup
    taosArrayPush(pTableBatch->req.pArray, &req);
  }
}

static int32_t buildDropTableVgroupHashmap(STranslateContext* pCxt, SDropTableClause* pClause, bool* pIsSuperTable,
                                           SHashObj* pVgroupHashmap) {
  SName name;
  toName(pCxt->pParseCxt->acctId, pClause->dbName, pClause->tableName, &name);
  STableMeta* pTableMeta = NULL;
  int32_t     code = getTableMetaImpl(pCxt, &name, &pTableMeta);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(&name, pCxt->pTargetTables);
  }

  if (TSDB_CODE_SUCCESS == code && TSDB_SUPER_TABLE == pTableMeta->tableType) {
    *pIsSuperTable = true;
    goto over;
  }

  if (TSDB_CODE_PAR_TABLE_NOT_EXIST == code && pClause->ignoreNotExists) {
    code = TSDB_CODE_SUCCESS;
    goto over;
  }

  *pIsSuperTable = false;

  SVgroupInfo info = {0};
  if (TSDB_CODE_SUCCESS == code) {
    code = getTableHashVgroup(pCxt, pClause->dbName, pClause->tableName, &info);
  }
  if (TSDB_CODE_SUCCESS == code) {
    addDropTbReqIntoVgroup(pVgroupHashmap, pClause, &info);
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
  tlen += sizeof(SMsgHead);
  void* buf = taosMemoryMalloc(tlen);
  if (NULL == buf) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  ((SMsgHead*)buf)->vgId = htonl(pTbBatch->info.vgId);
  ((SMsgHead*)buf)->contLen = htonl(tlen);
  void* pBuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  tEncoderInit(&coder, pBuf, tlen - sizeof(SMsgHead));
  tEncodeSVDropTbBatchReq(&coder, &pTbBatch->req);
  tEncoderClear(&coder);

  SVgDataBlocks* pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
  if (NULL == pVgData) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pVgData->vg = pTbBatch->info;
  pVgData->pData = buf;
  pVgData->size = tlen;
  pVgData->numOfTables = (int32_t)taosArrayGetSize(pTbBatch->req.pArray);
  taosArrayPush(pBufArray, &pVgData);

  return TSDB_CODE_SUCCESS;
}

SArray* serializeVgroupsDropTableBatch(SHashObj* pVgroupHashmap) {
  SArray* pBufArray = taosArrayInit(taosHashGetSize(pVgroupHashmap), sizeof(void*));
  if (NULL == pBufArray) {
    return NULL;
  }

  int32_t                code = TSDB_CODE_SUCCESS;
  SVgroupDropTableBatch* pTbBatch = NULL;
  do {
    pTbBatch = taosHashIterate(pVgroupHashmap, pTbBatch);
    if (pTbBatch == NULL) {
      break;
    }

    serializeVgroupDropTableBatch(pTbBatch, pBufArray);
  } while (true);

  return pBufArray;
}

static int32_t rewriteDropTable(STranslateContext* pCxt, SQuery* pQuery) {
  SDropTableStmt* pStmt = (SDropTableStmt*)pQuery->pRoot;

  SHashObj* pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pVgroupHashmap) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  taosHashSetFreeFp(pVgroupHashmap, destroyDropTbReqBatch);
  bool   isSuperTable = false;
  SNode* pNode;
  FOREACH(pNode, pStmt->pTables) {
    int32_t code = buildDropTableVgroupHashmap(pCxt, (SDropTableClause*)pNode, &isSuperTable, pVgroupHashmap);
    if (TSDB_CODE_SUCCESS != code) {
      taosHashCleanup(pVgroupHashmap);
      return code;
    }
    if (isSuperTable && LIST_LENGTH(pStmt->pTables) > 1) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DROP_STABLE);
    }
  }

  if (isSuperTable || 0 == taosHashGetSize(pVgroupHashmap)) {
    taosHashCleanup(pVgroupHashmap);
    return TSDB_CODE_SUCCESS;
  }

  SArray* pBufArray = serializeVgroupsDropTableBatch(pVgroupHashmap);
  taosHashCleanup(pVgroupHashmap);
  if (NULL == pBufArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
}

static int32_t buildUpdateTagValReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                    SVAlterTbReq* pReq) {
  SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }

  pReq->tagName = strdup(pStmt->colName);
  if (NULL == pReq->tagName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pReq->colId = pSchema->colId;

  SDataType targetDt = schemaToDataType(pTableMeta->tableInfo.precision, pSchema);
  if (DEAL_RES_ERROR == translateValueImpl(pCxt, pStmt->pVal, targetDt, true)) {
    return pCxt->errCode;
  }

  pReq->tagType = targetDt.type;
  if (targetDt.type == TSDB_DATA_TYPE_JSON) {
    if (pStmt->pVal->literal &&
        strlen(pStmt->pVal->literal) > (TSDB_MAX_JSON_TAG_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
      return buildSyntaxErrMsg(&pCxt->msgBuf, "json string too long than 4095", pStmt->pVal->literal);
    }
    SArray* pTagVals = taosArrayInit(1, sizeof(STagVal));
    int32_t code = TSDB_CODE_SUCCESS;
    STag*   pTag = NULL;
    do {
      code = parseJsontoTagData(pStmt->pVal->literal, pTagVals, &pTag, &pCxt->msgBuf);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    } while (0);
    for (int i = 0; i < taosArrayGetSize(pTagVals); ++i) {
      STagVal* p = (STagVal*)taosArrayGet(pTagVals, i);
      if (IS_VAR_DATA_TYPE(p->type)) {
        taosMemoryFree(p->pData);
      }
    }
    taosArrayDestroy(pTagVals);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pReq->nTagVal = pTag->len;
    pReq->pTagVal = (uint8_t*)pTag;
    pStmt->pVal->datum.p = (char*)pTag;  // for free
  } else {
    pReq->isNull = pStmt->pVal->isNull;
    pReq->nTagVal = pStmt->pVal->node.resType.bytes;
    pReq->pTagVal = nodesGetValueFromNode(pStmt->pVal);

    // data and length are seperated for new tag format STagVal
    if (IS_VAR_DATA_TYPE(pStmt->pVal->node.resType.type)) {
      pReq->nTagVal = varDataLen(pReq->pTagVal);
      pReq->pTagVal = varDataVal(pReq->pTagVal);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t buildAddColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                              SVAlterTbReq* pReq) {
  if (NULL != getColSchema(pTableMeta, pStmt->colName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DUPLICATED_COLUMN);
  }

  pReq->colName = strdup(pStmt->colName);
  if (NULL == pReq->colName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pReq->type = pStmt->dataType.type;
  pReq->flags = COL_SMA_ON;
  // pReq->bytes = pStmt->dataType.bytes;
  pReq->bytes = calcTypeBytes(pStmt->dataType);
  return TSDB_CODE_SUCCESS;
}

static int32_t buildDropColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                               SVAlterTbReq* pReq) {
  if (2 == getNumOfColumns(pTableMeta)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DROP_COL);
  }
  SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pStmt->colName);
  } else if (PRIMARYKEY_TIMESTAMP_COL_ID == pSchema->colId) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_CANNOT_DROP_PRIMARY_KEY);
  }

  pReq->colName = strdup(pStmt->colName);
  if (NULL == pReq->colName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pReq->colId = pSchema->colId;

  return TSDB_CODE_SUCCESS;
}

static int32_t buildUpdateColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                 SVAlterTbReq* pReq) {
  pReq->colModBytes = calcTypeBytes(pStmt->dataType);
  pReq->colModType = pStmt->dataType.type;
  SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pStmt->colName);
  } else if (!IS_VAR_DATA_TYPE(pSchema->type) || pSchema->type != pStmt->dataType.type ||
             pSchema->bytes >= pReq->colModBytes) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_MODIFY_COL);
  }

  pReq->colName = strdup(pStmt->colName);
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

  pReq->colName = strdup(pStmt->colName);
  pReq->colNewName = strdup(pStmt->newColName);
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
      pReq->newComment = strdup(pStmt->pOptions->comment);
      if (NULL == pReq->newComment) {
        code = TSDB_CODE_OUT_OF_MEMORY;
      }
      pReq->newCommentLen = strlen(pReq->newComment);
    } else {
      pReq->newCommentLen = -1;
    }
  }

  return code;
}

static int32_t buildAlterTbReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                               SVAlterTbReq* pReq) {
  pReq->tbName = strdup(pStmt->tableName);
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
    tEncodeSVAlterTbReq(&coder, pReq);
    tEncoderClear(&coder);

    SVgDataBlocks* pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
    if (NULL == pVgData) {
      taosMemoryFree(pMsg);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pVgData->vg = vg;
    pVgData->pData = pMsg;
    pVgData->size = tlen;
    pVgData->numOfTables = 1;
    taosArrayPush(pArray, &pVgData);
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
}

static int32_t rewriteAlterTableImpl(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                     SQuery* pQuery) {
  if (getNumOfTags(pTableMeta) == 1 && pStmt->alterType == TSDB_ALTER_TABLE_DROP_TAG) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }

  if (TSDB_SUPER_TABLE == pTableMeta->tableType) {
    return TSDB_CODE_SUCCESS;
  } else if (TSDB_CHILD_TABLE != pTableMeta->tableType && TSDB_NORMAL_TABLE != pTableMeta->tableType) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
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
  if (NULL == buf) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  ((SMsgHead*)buf)->vgId = htonl(pVg->vgId);
  ((SMsgHead*)buf)->contLen = htonl(len);

  SVgDataBlocks* pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
  if (NULL == pVgData) {
    taosMemoryFree(buf);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pVgData->vg = *pVg;
  pVgData->pData = buf;
  pVgData->size = len;
  taosArrayPush(pBufArray, &pVgData);

  return TSDB_CODE_SUCCESS;
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

static int32_t rewriteQuery(STranslateContext* pCxt, SQuery* pQuery) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pQuery->pRoot)) {
    case QUERY_NODE_SHOW_LICENCES_STMT:
    case QUERY_NODE_SHOW_DATABASES_STMT:
    case QUERY_NODE_SHOW_TABLES_STMT:
    case QUERY_NODE_SHOW_STABLES_STMT:
    case QUERY_NODE_SHOW_USERS_STMT:
    case QUERY_NODE_SHOW_DNODES_STMT:
    case QUERY_NODE_SHOW_VGROUPS_STMT:
    case QUERY_NODE_SHOW_MNODES_STMT:
    case QUERY_NODE_SHOW_MODULES_STMT:
    case QUERY_NODE_SHOW_QNODES_STMT:
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
    case QUERY_NODE_SHOW_TAGS_STMT:
      code = rewriteShow(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_DNODE_VARIABLES_STMT:
      code = rewriteShowDnodeVariables(pCxt, pQuery);
      break;
    case QUERY_NODE_SHOW_TABLE_DISTRIBUTED_STMT:
      code = rewriteShowTableDist(pCxt, pQuery);
      break;
    case QUERY_NODE_CREATE_TABLE_STMT:
      if (NULL == ((SCreateTableStmt*)pQuery->pRoot)->pTags) {
        code = rewriteCreateTable(pCxt, pQuery);
      }
      break;
    case QUERY_NODE_CREATE_MULTI_TABLE_STMT:
      code = rewriteCreateMultiTable(pCxt, pQuery);
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

static int32_t setRefreshMate(STranslateContext* pCxt, SQuery* pQuery) {
  if (NULL != pCxt->pDbs) {
    taosArrayDestroy(pQuery->pDbList);
    pQuery->pDbList = taosArrayInit(taosHashGetSize(pCxt->pDbs), TSDB_DB_FNAME_LEN);
    if (NULL == pQuery->pDbList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    SFullDatabaseName* pDb = taosHashIterate(pCxt->pDbs, NULL);
    while (NULL != pDb) {
      taosArrayPush(pQuery->pDbList, pDb->fullDbName);
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
      taosArrayPush(pQuery->pTableList, pTable);
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
      taosArrayPush(pQuery->pTargetTableList, pTable);
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
    case QUERY_NODE_VNODE_MODIF_STMT:
      pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
      pQuery->msgType = toMsgType(((SVnodeModifOpStmt*)pQuery->pRoot)->sqlNodeType);
      break;
    case QUERY_NODE_DESCRIBE_STMT:
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
    case QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT:
      pQuery->execMode = QUERY_EXEC_MODE_LOCAL;
      pQuery->haveResultSet = true;
      break;
    case QUERY_NODE_RESET_QUERY_CACHE_STMT:
    case QUERY_NODE_ALTER_LOCAL_STMT:
      pQuery->execMode = QUERY_EXEC_MODE_LOCAL;
      break;
    case QUERY_NODE_SHOW_VARIABLES_STMT:
      pQuery->haveResultSet = true;
      pQuery->execMode = QUERY_EXEC_MODE_RPC;
      if (NULL != pCxt->pCmdMsg) {
        TSWAP(pQuery->pCmdMsg, pCxt->pCmdMsg);
        pQuery->msgType = pQuery->pCmdMsg->msgType;
      }
      break;
    default:
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
  if (TSDB_CODE_SUCCESS == code) {
    code = setQuery(&cxt, pQuery);
  }
  setRefreshMate(&cxt, pQuery);
  destroyTranslateContext(&cxt);
  return code;
}
